# Rhiza SDK Optional SQL Transport (0.12.0)

Status: the optional Rhiza SQL transport is implemented and verified: 6 unit
tests and 5 integration tests pass with the `rhiza-backend` feature. This is
SQL transport only; the full Velorix `MetaStore` contract is not available
through Rhiza yet. This remains an additive adapter direction, not a
replacement, production-readiness, or end-to-end success claim.

## Verified published artifact

The published dependency is `rhizadb = "0.12.0"`. The source was inspected
from Cargo's registry cache and cross-checked against the [official v0.12.0
README](https://github.com/mrchypark/rhiza/blob/v0.12.0/sdk/rust/README.md),
[embedded example](https://raw.githubusercontent.com/mrchypark/rhiza/v0.12.0/sdk/rust/examples/embedded.rs),
and source commit
[`3b5a5a07aaf83b5ed279f75b920395bda7179577`](https://github.com/mrchypark/rhiza/commit/3b5a5a07aaf83b5ed279f75b920395bda7179577).

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

## Authority timestamp blocker

The SDK/native source exposes no engine-assigned transaction timestamp API.
The native SQL validator rejects nondeterministic `current_timestamp`,
`current_date`, `current_time`, and `random` expressions. `consistency:
linearizable` is a read barrier, not an authority wall-clock value. Therefore
Rhiza cannot currently replace the 14 Velorix call sites using
`txn_with_raft_serialized_timestamp`, which bind a single Raft-authoritative
Unix timestamp into fencing and expiry SQL. Host-clock injection, a Raft log
index, or a metrics timestamp is not equivalent and must not be substituted.

The minimal upstream addition should be one native operation that:

1. samples an authority Unix timestamp once before replication, under the
   leader/authority's command-serialization path;
2. persists that sampled value in the replicated command, injects it into
   bound SQL parameters (including conditional-update predicates), and returns
   it with the committed receipt;
3. applies the persisted value deterministically during follower/restart
   replay, never re-sampling wall-clock time; and
4. preserves the same `request_id`/fingerprint idempotency and
   `request_status` retry semantics.

Until that API exists, any Rhiza adapter must be explicitly marked
`authoritative_backend_time=false` and cannot claim the current Velorix
wall-clock failover/fencing contract.

## Recovery and migration scope

No-PVC object-store recovery behavior for this SDK was not established by the
local probe and requires a separately designed, evidence-backed checkpoint and
restore test. Likewise, compatibility/migration remains offline analysis:
produce a digest over the 18-table adapter scope, compare schema/constraint/
index/trigger semantics, and verify representative data and transaction
outcomes before considering migration. No production or E2E claims follow from
the isolated SDK probe.
