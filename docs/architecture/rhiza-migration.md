# Rhiza KV MetaStore Migration (0.19.0)

Status: the embedded Rhiza KV transport, bounded root/page snapshot path, and
all 35 `RhizaKvMetaStore` methods are implemented. The runtime/meta service is
wired to open the operator-managed store; production migration remains
unverified. This is not a production-readiness or end-to-end success claim.

The library user enables `rhiza-backend`, opens a `rhizadb::Config` with its
local working `DataDir`, and constructs `RhizaKvMetaStore::new(kv)`.
Local restart tests retain this directory; no-PVC deployments instead need
verified recovery from shared object storage. In operator-managed mode the
meta service opens `RhizaKvMetaStore` from the native `RHIZA_*` environment and
starts the recovery listener on that same database handle.

Local verification for this KV increment: 14 library tests, 5 SQL transport
tests, 5 KV/snapshot tests, and 3 MetaStore integration tests passed. The latter
cover catalog/reservation/commit recovery, concurrent clients on one native
node, and stale-token rejection after takeover with no failed-publication root
change. These are not a three-node QuePaxa proof. Workspace and Rhiza-feature
clippy pass with `-D warnings`; the runtime regression suites pass 54 unit and
232 materialized-view tests.

## Verified published artifact

The crate pins `rhizadb = "=0.19.0"`; the Rhiza KV implementation targets
that version's API.

Usable Rust surface:

- `Config::new(path)` plus `node_id`, `cluster_id`, `bind_addr`, `peer_addr`,
  and `set_option` builder methods.
- `Db::open(Config)`, `close(&mut self)`, `Drop`, generic `call`/
  `call_timeout`, `execute`, `execute_returning`, `query`, and
  `request_status`.
- Mutations return `MutationReceipt`; callers must invoke
  `require_committed()`. `Db` is `Send + Sync`, not `Clone`; share it with
  `Arc<Db>`.
- Convenience SQL/KV/graph reads force local consistency. A linearizable read
  is possible only through generic `call`, passing
  `{"consistency":"linearizable"}` in the native operation request.

`call("execute", {"request_id", "statements": [...]})` accepts an atomic
multi-statement SQL transaction. Each statement can carry `args`,
`want_rows`, `expected_rows_affected`, `expected_returned_rows`, and output
references. `INSERT OR REPLACE`, guarded updates, and `PRAGMA table_info`
were exercised successfully in an isolated create/write/read/close/reopen
probe. The probe used no server.

The SQL transport is separate from the KV-backed metadata store: it does not
implement `MetaStore`, and Rhiza SQL does not provide an engine-assigned
authority timestamp. The KV metadata path instead stores proposer-sampled time
with each root-CAS transition; that does not provide bounded wall-clock
failover.

The adapter's small async boundary delegates blocking SDK calls to Tokio's
blocking pool; the underlying SDK remains synchronous:

```rust
let store = RhizaSqlStore::open(data_dir, "node-1").await?;
store.execute_atomic("request-1", "INSERT INTO t VALUES ($1)", json!([1])).await?;
let result = store.query_linearizable("SELECT * FROM t", json!([])).await?;
```

Final local verification also passed 54 `velorix-runtime` unit tests, 232
`materialized_view_runtime` integration tests, workspace clippy with
`-D warnings`, the Rhiza-feature clippy check, and workflow `actionlint`.

## KV root-CAS safety boundary

Rhiza's KV root CAS is proposer-independent: any proposer can submit the
conditional mutation, and the replicated state machine decides whether the
expected root matches. This does not require forcing a single authority
leader. A fixed, caller-supplied timestamp can provide TTL liveness, but it is
not an authority clock and cannot replace the atomic epoch-safety primitive;
expiry should therefore be treated as a liveness hint until an engine-sampled
consensus timestamp is available.

The KV snapshot stores its root manifest at `velorix/snapshot/root` and pages
under `velorix/snapshot/page/<sha256-hex-digest>`. Pages are at most 64 KiB;
the complete KV snapshot is at most 8 MiB (128 pages), and the root manifest
is capped at 16 KiB. The snapshot codec is canonical JSON and independently
caps its encoded snapshot at 16 MiB. Page writes precede root publication, and
there is no page garbage collection: failed or superseded writes can leave
orphan pages, a current storage limitation.

## Evidence and remaining gates

The three-node embedded probe was attempted with an ephemeral S3-compatible
object store and concurrent startup, but nodes remained `not_ready` with
`QuePaxa quorum unavailable`; no distributed CAS or cross-node linearizable
read claim is made. No migration digest, no-PVC object-store recovery test,
or production rollout has been completed. The implementation is constrained
to the local Rust/Go/C build and GitHub Actions; no CloudBuild path or private
cluster identifier is part of this evidence.

The review also found that a recurring check-only run is insufficient for
acceptance. Acceptance requires native regression tests covering malformed
receipts, request-id replay/conflict, root-token round trips, page/full-state
digest validation, and concurrent proposer CAS behavior.

## Build evidence and limits

The crate's `build.rs` requires a host-target macOS or Linux GNU build, a C
compiler, and Go 1.27+; it builds a static `librhiza_ffi.a` with
`-buildmode=c-archive`. Local Rust 1.98.1 and Apple Clang 17 were available;
the installed Go was 1.26.1. `GOTOOLCHAIN=auto` transparently selected Go
1.27 and the isolated probe ran. With `GOTOOLCHAIN=local`, the build is
expected to fail because the bundled `native/go.mod` declares `go 1.27.0`.

## Runtime configuration evidence (not deployment evidence)

The native `rhiza.Config` accepts `DataDir`, `NodeID`, `ClusterID`, `BindAddr`,
`PeerAddr`, and object-store fields including `ObjStoreProvider`,
`ObjStoreDir`, `ObjStoreEndpoint`, `ObjStoreBucket`, `ObjStorePrefix`,
credentials, and `ObjStoreDurability`. The Rust SDK reaches these fields via
`Config::set_option("GoFieldName", value)`. Defaults include `BindAddr` and
`PeerAddr` of `127.0.0.1:0`, `ClusterID` of `cluster-a`, and async object-store
durability. `ObjStoreDurability="before-ack"` requires configured object
storage; multi-node operation requires shared S3/GCS/Azure storage with
conditional writes. A filesystem object store is not accepted for a
multi-node shared configuration.

For a no-PVC deployment, `DataDir` is still required for each node's local
WAL/SQLite/LatticeDB working state; object storage is the durable shared
checkpoint/archive authority, not a claim that the process has no local disk.
The source opens the object store and loads checkpoint/archive metadata during
cold start, then validates certified checkpoint seals before restore/replay.
This source inspection establishes configuration and startup ordering only; no
remote object-store cold-start, loss, or recovery test was run here.

## KV time and fencing semantics

The KV metadata path does not require an engine-assigned timestamp or a
single-process leader. Each proposer samples Unix time once for its attempted
transition; that proposed value, the evaluated domain result, and the complete
snapshot are published together by the root CAS. A competing proposer must
reload the winning root and re-evaluate. Reusing a request ID is permitted only
with the identical payload; an uncertain attempt is resolved with
`request_status` before another attempt is made.

Owner/authority epoch and token predicates are the safety boundary: a stale
writer cannot publish after a newer root has fenced it. The proposer timestamp
is only a TTL liveness input. Clock skew or rollback can make expiry early or
late, so this path must not claim bounded wall-clock failover. This is a
different limitation from the atomic root-CAS safety guarantee and is not an
upstream API blocker.

## Recovery and migration scope

No-PVC object-store recovery behavior for this SDK was not established by the
local probe and requires a separately designed, evidence-backed checkpoint and
restore test. Likewise, compatibility/migration remains offline analysis:
produce a digest over the 18-table adapter scope, compare schema/constraint/
index/trigger semantics, and verify representative data and transaction
outcomes before considering migration. No production or E2E claims follow from
the isolated SDK probe.
