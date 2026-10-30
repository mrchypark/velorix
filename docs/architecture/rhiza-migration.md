# Rhiza KV MetaStore Upgrade and Local-init guidance

Current source configuration is parsed in
[main.rs](../../crates/velorix-meta/src/main.rs); KV metadata snapshot/root-CAS
behavior is in [rhiza_meta.rs](../../crates/velorix-meta/src/rhiza_meta.rs),
[rhiza_kv_snapshot.rs](../../crates/velorix-meta/src/rhiza_kv_snapshot.rs),
and [rhiza_snapshot.rs](../../crates/velorix-meta/src/rhiza_snapshot.rs).
Historical release and test records below remain historical and are not
re-verified by this source reconciliation. Run-local `target/` evidence is not
a public repository artifact or provider/deployment qualification.

## Current upstream maintenance-release assessment: v0.21.2

The official v0.21.2 tag resolves to the exact source commit
[`8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`](https://github.com/mrchypark/rhiza/commit/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd).
The maintenance-release fix settles an errored release only when bounded,
stable remote readback proves the exact expected free control at a changed
object version. Work errors, cancellation, ambiguous or unreadable state, and
held controls remain fail-closed. This is not an automatic/manual unlock API
for an orphaned held token. It changes no archive format, identity, receipt,
or public API, and does not change the release upload timeout.

The following official runs were independently checked to have this exact
head SHA and successful conclusions: [CI 40840202108](https://github.com/mrchypark/rhiza/actions/runs/40840202108),
[Performance 40840202329](https://github.com/mrchypark/rhiza/actions/runs/40840202329),
[Native archives 40841322381](https://github.com/mrchypark/rhiza/actions/runs/40841322381),
[Release sync 40840963425](https://github.com/mrchypark/rhiza/actions/runs/40840963425),
and [Operator image 40841322597](https://github.com/mrchypark/rhiza/actions/runs/40841322597).
These workflow results do not establish independent live-cloud qualification.
The upstream operator image is not a Velorix dependency: Velorix embeds the Go
SDK and owns its application image and init-local command.

The local project pins `rhizadb` v0.21.2 in Cargo.toml/Cargo.lock with SHA-256
`c913bfcb9a6c6c6b5340a92c320d08c4941ae07d985e6174de7d59ad12f03db0`. Artifact
readiness is recorded below. The crate omits optional `.cargo_vcs_info.json`;
this provenance gap does not block Velorix runtime verification, which checks
version, release hash, and relevant code. The v0.21.2 runtime evidence is
summarized below.

| Required artifact/evidence | Status |
| --- | --- |
| Published `rhizadb` v0.21.2 crate | Verified; SHA-256 matches Cargo.lock |
| Packaged `local.go` maintenance fix | Verified against exact tag source; SHA-256 prefix `dfcf564` |
| Native sidecar, Mac ARM64 | Verified against release |
| Native sidecar, Linux AMD64 | Verified against release |
| Docker Go toolchain tarball | Go 1.27.3 pin available and verified |
| Upstream operator image | Not required; Velorix embeds the Go SDK |
| `.cargo_vcs_info.json` | Absent; optional provenance gap, not a runtime requirement |
| Meta Rhiza-feature tests | Passed: 196; 4 fixture-only tests ignored |
| Native Local recovery probe | Passed against pinned local VersityGW; no Kubernetes |
| Split-process Local service recovery | Passed; local fixture, no Kubernetes or live cloud |
| Required workspace pre-push checks | Passed: fmt, workspace clippy, runtime lib 86, materialized-view runtime 263 |

Evidence is retained under `target/rhiza-0.21.2-integration-evidence/`,
`target/rhiza-recovery-evidence/20261029T111251Z-89574/`, and
`target/rhiza-service-recovery-evidence/20261029t111346z-89570/`. The Local
results cover native-process recovery only; they do not establish production
cloud or Kubernetes qualification. The release-level S3 benchmark remains
unavailable and required by the repository's release policy.

## Historical v0.21.1 documentation and evidence

Historical v0.21.1 evidence used `rhizadb` with Cargo.lock
checksum
`34f191b81110873c181806596715a04d9f4dacc504ebce07beb4750e39449c2f`. The
previous v0.21.0 crate checksum was
`a040f5b0ea6faa8b345fbdb2e7020dc72cbbe253ddb3bfc1fcdf89978ef4f5c4`; this is
distinct from the v0.21.0 macOS ARM64 native archive checksum, which begins
`b315`; do not label the crate checksum as a native archive checksum. The
archive format is shared by filesystem, S3, GCS, and Azure storage. It does not
automigrate existing databases or archives: unsupported older formats must be
rejected without modifying their data. Do not point this release at existing
user data; upgrade verification must use newly created data directories and
archive prefixes. The crate checksum is recorded in dependency governance.
Compatibility with older provider/archive data remains unverified. Velorix
does not require every Rhiza internal test. The scoped Velorix feature and
fresh-data harness results are recorded below. Provider coverage is not
implied by an S3-compatible run.

The official v0.21.1 Local init-container guide was added by upstream commit
`66e2416b52e3d64aa86134b7b0c8b70c5ace88be`, the peeled commit of annotated
tag object `86a2a33732a1a84ade45e76c7e1a3d6cae836611`. Official metadata,
guide, and Rust README snapshots are preserved in
`target/rhiza-0.21.1-contract-evidence/`. The crates.io API request returned
403 in the recorded verification; that run did not independently verify the
crate through the API. The cargo index check was pending with the dependency
worker when this documentation was updated. The release also records a failed
provider job; it is not characterized as all-green. These sources clarify
Local deployment behavior and do not change the separate v0.21.0 runtime test
evidence below. The published SDK's native Go toolchain declaration remains
Go 1.27.3; this deployment/docs work does not change it.

The library user enables `rhiza-backend`, opens a `rhizadb::Config` with its
local working `DataDir`, and constructs `RhizaKvMetaStore::new(kv)`. Upgrade
checks use a new local directory and a new archive prefix. Existing v0.20.0
database or archive data is not migrated in place; format rejection must leave
it byte-for-byte untouched. In operator-managed mode the meta service opens
`RhizaKvMetaStore` from the native `RHIZA_*` environment and starts the
recovery listener on that same database handle.

Historical Rhiza 0.19.0 verification for this KV increment: 14 library tests,
5 SQL transport tests, 5 KV/snapshot tests, and 3 MetaStore integration tests
passed. The MetaStore tests cover catalog/reservation/commit recovery,
concurrent clients on one native node, and stale-token rejection after takeover
with no failed-publication root change. These are not a three-node QuePaxa
proof. Workspace and Rhiza-feature clippy passed with `-D warnings`; the runtime
regression suites passed 54 unit and 249 materialized-view tests.

## Artifact provenance and verification status

The previous evidence run used `rhizadb = "=0.21.0"`; the then-current pin was
`rhizadb = "=0.21.1"` with checksum recorded above. The crates.io API request
for v0.21.1 returned 403, so no API proof is claimed. The official published
macOS ARM64
native archive was downloaded and its distinct checksum (prefix `b315`)
independently verified; it was not built locally. The vendored Rhiza 0.19
operator artifact is separate and is not mechanically upgraded by this crate
change.

The v0.21.0 Velorix Meta feature run passed 190 tests with 3 ignored; its
recorded rerun exit code is 0 in
`target/rhiza-0.21-native-evidence/feature-meta-tests-rerun1.exit`. The
single-node fresh-archive drill passed 1 test in 2.13 seconds, and the split
process recovery harness passed using a fresh data directory and archive
prefix. The split-process drill exercised S3 through the pinned local
VersityGW fixture on macOS ARM64. Filesystem, GCS, Azure, native archive image
variants, and rejection/non-modification of older archive formats remain
unverified. No three-node Kubernetes operator gate or HA deployment is part
of this upgrade evidence. The official v0.21.1 init guidance describes
application-owned Local recovery, not HA. The Velorix Kubernetes manifest has
not been run on a live cluster. Formatting, workspace clippy, Meta/Control
feature clippy with `-D warnings`, runtime tests (86 unit and 263 integration),
and API tests (458 passed, 1 ignored) all passed. Both live harnesses exited 0.
No Rhiza defect was reproduced.

The first feature wrapper could not capture the native exit status because its
status destination was read-only. The original feature log records 190
passing tests and 3 ignored; a rerun of the same suite recorded exit code 0. This wrapper
incident remains documented and is not attributed to Rhiza.

The first single-node fixture attempt received HTTP 400 while creating an
uppercase bucket; its log was retained. The corrected rerun passed. This
fixture issue was not reproduced as a Rhiza failure and is not counted as a
product defect.

## v0.21.0 split-process recovery evidence

The local evidence at
`target/rhiza-service-recovery-evidence/20261027t111801z-12147/result.json`
records one Rhiza 0.21.0 Meta process with separate API and worker processes,
fresh local metadata directories after restarts, a new archive prefix, and a
pinned local VersityGW S3 fixture. Initial sum/count results were `2.5/1`; they
remained `2.5/1` after the first empty-directory restore. A new input produced
`4/2`; its duplicate retry remained `4/2` with an unchanged descriptor, and a
second empty-directory restore retained `4/2`. Both restored views read their
checkpoint bodies from S3. Incremental query preparation read the new source
batch once and the old batch zero times. The checkpoint codec identity was
`velorix-arrow-ipc-zstd3-v1`.

The run verified logical fencing with a 30000 logical-tick lease parameter.
It explicitly reported `production_bounded_failover_safe=false` and
`production_multi_writer_safe=false`. This is local S3 recovery evidence, not
provider-wide compatibility or production safety evidence.

## Historical 0.20.0 split-process recovery evidence

The 2026-10-27 local functional run used separate API and worker processes, one
Rhiza 0.20 Meta process, fresh metadata state and read caches after each
restart, and manually pinned VersityGW commit
`fd04bc1df2656298577b82667a4195c77f8c7563`. Sum/count results were `2.5/1`
before and after the first empty-directory Meta restore. One post-restore
ingest returned `4/2`; its duplicate retry preserved offsets `1..2`, and a
second empty-directory restore retained `4/2`. S3 logs recorded checkpoint
body GETs for both views. The first-restore startup/query window recorded zero
GETs for the original source batch. Incremental processing recorded one GET
for the new batch and zero for the original batch. The query `logical_epoch`
matched for initial-to-first-restore and incremental-to-second-restore pairs.

This proves local logical-fencing functionality only. It does not establish a
production deployment, production multi-writer safety, or bounded wall-clock
failover. Recovery from a preexisting Rhiza 0.19.0 data directory and the
ignored three-node recovery gate remain untested.

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

Historical Rhiza 0.19.0 local verification passed 54 `velorix-runtime` unit
tests, 249 `materialized_view_runtime` integration tests, workspace clippy with
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

## Rhiza 0.21 local mode

Velorix accepts `VELORIX_RHIZA_MODE=local` for an explicit standalone Rhiza
process and maps it to the published Rust SDK's `Config::local()` builder. The
mode requires `VELORIX_RHIZA_DATA_DIR`, `VELORIX_RHIZA_NODE_ID`, and a stable
`VELORIX_RHIZA_CLUSTER_ID`. It does not construct a `Members` list, peer
listeners, peer token, or admin token. Runtime `RHIZA_*` config variables are
rejected in this explicit mode to prevent native environment settings from
shadowing it; build-time `RHIZA_NATIVE_LIB_DIR` is allowed. Local mode with
object storage uses the native archive namespace
`path.Join(ObjStorePrefix, ClusterID, "local/v1")`; keep the cluster ID and
prefix stable across restarts. The behavior is present in the v0.21.0 native
source revision `e4c6339f9ce2092ac329e9a24ccd583c6a219bc8`: `Local` startup
calls `prepareLocalBackup` when object storage is configured, and an empty
data directory is restored from that archive. Production mode requires an
explicit S3/GCS/Azure provider and
`VELORIX_RHIZA_OBJECT_STORE_DURABILITY=before-ack`, in addition to the normal
Meta bearer authentication and native mutual TLS.

The serving process opens the SDK Local database, whose native startup path
reconciles the Local archive and restores an empty data directory. This is a
single-writer archive recovery path, not peer replication or multi-writer
fencing. `velorix-meta check-config` only validates the supplied environment;
it does not open the SDK, inspect the archive, parse TLS PEM contents, or test
object-store connectivity. Rhiza's test-helper staging behavior is not treated
as a production archive handoff API.

## KV time and fencing semantics

Rhiza 0.20 uses `root_cas_logical_clock` and
`operation_driven_logical` lease expiry, replacing the earlier process-clock
TTL semantics. The clock advances with root-CAS state transitions; it does
not measure elapsed time. Capabilities report
`authoritative_backend_time=false`, `multi_writer_fencing_safe=true`,
`bounded_wall_clock_failover=false`, `production_bounded_failover_safe=false`,
and `production_multi_writer_safe=false`. Logical owner and epoch CAS checks
are a functional fencing capability, not production multi-writer safety.

The API setting `VELORIX_STANDING_RUNTIME_OWNER_TTL_MS` supplies the TTL
parameter; with this backend its configured value is a logical-tick budget,
not milliseconds of wall time. The final recovery run used `30000` logical
ticks. Do not interpret this value as a wall-clock failover bound.

A restarted API process explicitly calls `ReacquireStandingRuntimeOwner` to
advance the durable owner epoch and fence its prior process-incarnation token.
Older Meta servers do not implement this RPC and return `UNIMPLEMENTED`; the
new API fails closed. Cutover must quiesce metadata writers and upgrade Meta,
API, and worker processes to compatible versions together. Mixed old/new
metadata writers are unsupported.

Root CAS still serializes competing proposals: a proposer reloads the winning
root and re-evaluates after a CAS conflict. Reusing a request ID is permitted
only with the identical payload; an uncertain attempt is resolved with
`request_status` before retrying.

## Recovery and migration scope

The local split-process run establishes one single-Rhiza 0.20 recovery path;
it is historical and does not validate the v0.21.0 archive format. Multi-node
and production no-PVC recovery remain unverified. The upgrade is greenfield:
there is no in-place migration from older database or archive formats. The
18-table adapter digest and schema/constraint/index/trigger comparison remain
separate compatibility analysis and do not authorize modifying old user data.
The local run does not establish production safety.
