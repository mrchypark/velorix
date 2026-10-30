# Meta Service

## Current Source Contract (2026-10-30)

Meta is not one checkpoint snapshot. `MetaStore` exposes separate relation
catalog, ingest reservation/publication, active-view registry/bootstrap,
standing-runtime owner/checkpoint pointer, and partition-authority APIs. The
gRPC methods and request shapes are defined in
[meta.proto](../../crates/velorix-meta/proto/velorix/meta/v1/meta.proto); the
backend implementations are in
[lib.rs](../../crates/velorix-meta/src/lib.rs) and
[rhiza_meta.rs](../../crates/velorix-meta/src/rhiza_meta.rs). Rhiza's KV
implementation stores Velorix metadata through a bounded snapshot-root/page
protocol with CAS; the separate Rhiza Local archive is database recovery
storage, not the runtime checkpoint body store.

`VELORIX_META_MODE` and `VELORIX_META_BACKEND` are explicit configuration
inputs. Development permits memory, OSS, or Rhiza KV. Production rejects
memory; OSS requires durable object storage, and Rhiza KV requires exactly
three voters, explicit S3/GCS/Azure object storage, and `before-ack` archive
durability. Rhiza `local` mode is a separate single-writer mode and is not a
consensus cluster or multiwriter claim. Config admission is in
[main.rs](../../crates/velorix-meta/src/main.rs); the `production_rhiza_requires_three_voters_before_ack_and_object_storage`
and production-config tests exercise these gates. This source inspection is
not a live provider or deployment qualification.

Standing-runtime and partition-authority state have distinct capability
reports. Current Rhiza KV metadata uses proposer-sampled root-CAS logical
ticks, not authoritative elapsed wall-clock time; source presence or successful
local recovery does not establish bounded failover or production
multiwriter safety. Runtime checkpoint object bodies remain in the object-store
state/output namespaces; metadata stores pointer/identity/reference fields.

The metadata service owns hot control-plane metadata. Object storage owns large
artifacts and checkpoint payloads; Rhiza Local's own archive is a distinct
durability path with its own bucket/prefix configuration. API/worker checkpoint
objects do not configure or qualify the Rhiza archive. Foyer remains a
non-authoritative local cache only.

The product path is the embedded Rhiza KV backend, pinned locally to
`rhizadb = "=0.21.2"` with Cargo.lock checksum
`c913bfcb9a6c6c6b5340a92c320d08c4941ae07d985e6174de7d59ad12f03db0`.
The current release is Rhiza v0.21.2, whose exact official source is
[`8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`](https://github.com/mrchypark/rhiza/commit/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd).
The downloaded crate SHA matches the lockfile checksum. The package's
`.cargo_vcs_info.json` is absent, an optional provenance gap. Velorix verifies
the crate version, release checksum, and relevant source; this missing file is
not a runtime artifact requirement. The packaged `local.go` used for the
maintenance fix matches the exact tag source (SHA-256 prefix `dfcf564`). Native
Mac ARM64 and Linux AMD64 sidecars match the release, and the Docker Go 1.27.3
tarball pin is available and verified. The v0.21.2 Meta Rhiza-feature suite
passed 196 tests, with 4 fixture-only tests ignored. The standalone native Local
recovery probe and split-process service recovery also passed against a pinned
local VersityGW fixture. These are local fixture results, not live-cloud or
Kubernetes qualification. The workspace pre-push checks passed: formatting,
clippy with `-D warnings`, runtime library (86 tests), and
`materialized_view_runtime` (263 tests). Evidence is recorded under
`target/rhiza-0.21.2-integration-evidence/`,
`target/rhiza-recovery-evidence/20261029T111251Z-89574/`, and
`target/rhiza-service-recovery-evidence/20261029t111346z-89570/`.
The v0.21.0 release changes the archive format shared
by filesystem, S3, GCS, and Azure storage. This is a greenfield format: startup
must reject unsupported older database/archive formats without modifying
them. Do not upgrade existing user data in place; use a new local data
directory and a new archive prefix for verification. Historical crate
checksums remain recorded alongside the current v0.21.2 pin in
`dependency-governance.json`.

Rhiza v0.21.2 reconciles an errored Local maintenance release only after
bounded, stable remote readback proves the exact expected free control at a
changed object version. Work errors, cancellation, unreadable or uncertain
state, and unresolved held controls remain fail-closed. This is not a manual
unlock or orphaned-token clearing API. It makes no archive format, identity,
receipt, or public API change; the release upload timeout is unchanged. The
exact-source [CI](https://github.com/mrchypark/rhiza/actions/runs/40840202108),
[Performance](https://github.com/mrchypark/rhiza/actions/runs/40840202329),
[Native archives](https://github.com/mrchypark/rhiza/actions/runs/40841322381),
[Release sync](https://github.com/mrchypark/rhiza/actions/runs/40840963425),
and [Operator image](https://github.com/mrchypark/rhiza/actions/runs/40841322597)
runs were verified successful at that commit. CI includes pinned emulator SDK
contracts and explicit skips; these runs do not establish independent
live-cloud qualification. The local Cargo pin and required native artifact
download/pins are verified at v0.21.2. External exclusion of
overlapping writers remains required.

The v0.20 logical-fencing evidence below is historical. For v0.21.0, the
Velorix Meta feature suite passed 190 tests with 3 ignored, and the fresh-data
single-node archive drill passed 1 test in 2.13 seconds. A split-process
recovery run passed against S3 via the pinned local VersityGW fixture on macOS
ARM64; its recorded results are summarized below. Filesystem, GCS, Azure,
native archive image variants, and older-format rejection/non-modification
remain unverified. S3 evidence does not pass those cases. Formatting, workspace
clippy, Meta/Control feature clippy with `-D warnings`, runtime tests (86 unit
and 263 integration), and API tests (458 passed, 1 ignored) passed. Both live
harnesses exited 0. No Rhiza defect was reproduced.

The application-owned `init-local` deployment contract follows the official
Rhiza v0.21.1 [Local init-container recovery guide](https://raw.githubusercontent.com/mrchypark/rhiza/v0.21.1/docs/local-init-container-recovery.md),
introduced at upstream commit
[`66e2416b52e3d64aa86134b7b0c8b70c5ace88be`](https://github.com/mrchypark/rhiza/commit/66e2416b52e3d64aa86134b7b0c8b70c5ace88be).
`init-local` opens Rhiza with the same S3 configuration, identity, and mounted
`DataDir` used by `serve`, waits for native recovery/reconciliation, then
explicitly closes the database and surfaces close errors. It may perform
Rhiza's native archive operations; it must not start application SQL or other
listeners. Its successful close is required before the init container
acknowledges completion. `serve` must then use the same configuration and
`DataDir`, drains work, and explicitly closes Rhiza on shutdown, also
surfacing close errors. This does not imply a Rhiza-provided CLI or image, and
does not relabel the historical v0.21.0 tests above as v0.21.2 results. Init
sequencing alone does not exclude a second external writer;
single-writer exclusion remains an operational requirement. The all-in-one
image's entrypoint accepts `meta init-local` and forwards
those arguments to `velorix-meta`; run the image again with `meta serve` and the
same volume and environment. `velorix-all` is the image name, not a native CLI.
Its `all` mode already opens Rhiza as
part of service startup and does not need a separate init container. This
manifest explicitly selects Local mode and does not opt legacy clustered
deployments into it. The checked-in
Kubernetes contract is static and offline; no Kubernetes deployment or
container image qualification has been performed. Local mode remains
single-writer and does not establish HA or production failover safety. The
v0.21.2 maintenance release-acknowledgement fix addresses the previously
recorded S3 release-control issue. External quiescence and exclusion of
overlapping writers remain required for maintenance and any second writer.

The previously observed Rhiza 0.20 capability reports
`authoritative_backend_time=false`,
`backend_time_source_kind=root_cas_logical_clock`,
`lease_authority_kind=root_cas_logical_clock`, and
`lease_expiry_semantics=operation_driven_logical`. Its logical owner-lease
capability reports `multi_writer_fencing_safe=true`, while
`bounded_wall_clock_failover=false`,
`production_bounded_failover_safe=false`, and
`production_multi_writer_safe=false`. The logical fencing capability describes
root-CAS owner and epoch checks; it is not a production safety claim and does
not establish bounded failover.

## Logical fencing and version cutover

Under Rhiza's root-CAS clock, the `VELORIX_STANDING_RUNTIME_OWNER_TTL_MS`
parameter is a logical-tick budget, not elapsed milliseconds. The functional
recovery run used `30000`; that value does not imply a 30-second expiry or a
wall-clock recovery bound. A restarted process uses the explicit
`ReacquireStandingRuntimeOwner` operation to acquire the next owner epoch; the
old epoch remains fenced. The new RPC is not implemented by older Meta servers,
which return `UNIMPLEMENTED`; API callers fail closed.

Cutover requires quiescing metadata writers and moving Meta, API, and worker
processes to compatible versions together. Do not run old and new metadata
writers concurrently or use a mixed-version rollout for this protocol change.

The operator-managed runtime opens `RhizaKvMetaStore` from the native
`RHIZA_*` environment and starts the operator recovery listener on the same
database handle. The separate Rhiza SQL transport does not implement `MetaStore`
and Rhiza SQL does not supply an engine-assigned authority timestamp; metadata
uses the KV root-CAS path, whose proposer-sampled clock does not establish
bounded failover.

## First Meta API Slice

`crates/velorix-meta` defines the first typed gRPC boundary:

- `StoreRelationCatalog`
- `ReadRelationCatalog`
- `ReserveIngestRange`

The current implementation has:

- an in-memory backend for contract tests and local API work
- a `GrpcMetaStore` client used by `velorix-api`
- an `OssMetaStore` backend for standalone or low-cost deployments
- a `RhizaKvMetaStore` backend behind the `rhiza-backend` feature

The in-memory backend is not durable and must not be used as the production
meta authority. The object-store backend is durable and simple, but it keeps
metadata on the same object-store primitives as the existing replay path, so it
is best for small deployments, standalone E2E, and cost-optimized operation.

For local contract work, the gRPC service can be built with:

```bash
cargo run -p velorix-meta
```

## Rhiza KV service mode

The metadata binary also supports the embedded Rhiza KV backend behind the
`rhiza-backend` feature. Membership is fixed explicitly with either
`VELORIX_RHIZA_MEMBERS_JSON` or a secret-mounted
`VELORIX_RHIZA_MEMBERS_FILE` (never both). The value is a JSON array whose
members carry exactly `node_id`, `url`, `peer_url` (`quic://...`), and
`public_key` fields. Under the Rhiza 0.20.0 contract `public_key` is required
public material: standard padded base64 of the 32 raw Ed25519 public key
bytes, never the URL-safe alphabet. Any other field, including the legacy
per-member `token`, is a hard unknown-field error rather than an ignored key.
`public_key` is empty only for a single-node cluster that authenticates no
peers. The local `VELORIX_RHIZA_NODE_ID` must be one of the members. A
multi-node cluster also requires this node's own private peer token through
exactly one of `VELORIX_RHIZA_PEER_TOKEN` or
`VELORIX_RHIZA_PEER_TOKEN_FILE` (never both), and that token must differ from
`VELORIX_RHIZA_ADMIN_TOKEN`. Private peer tokens and derived keys never enter
the membership document, a log line, or diagnostics.
`VELORIX_RHIZA_PEER_ADDR` is the native bind address (`host:port`); the
`quic://` scheme belongs only in the member `peer_url`. For the native
S3-compatible provider, `VELORIX_RHIZA_OBJECT_STORE_ENDPOINT` is also a host
and port (for example `versitygw:9000`), with
`VELORIX_RHIZA_OBJECT_STORE_INSECURE=1` selecting HTTP.

This contract is greenfield. A run starts from a fresh empty
`VELORIX_RHIZA_DATA_DIR` with newly generated identities, and there is no
migration path from a pre-0.19.0 membership document; such a document is not a
valid input.

Development Rhiza cluster mode is explicitly single-node and defaults to
asynchronous object-store durability. Production cluster mode requires exactly
three voters, explicit S3/GCS/Azure object storage, and
`VELORIX_RHIZA_OBJECT_STORE_DURABILITY=before-ack`; no local filesystem object
store is accepted as production durability. The service performs a
linearizable metadata read before opening its gRPC listener, so an unavailable
Rhiza quorum fails startup. Rhiza's production fencing capability remains
fail-closed until the separately recorded three-node recovery evidence exists.

`VELORIX_RHIZA_MODE=local` explicitly selects Rhiza 0.21's native
`Config::local()` mode. It is a single-writer local database, not a one-voter
consensus cluster. It requires `VELORIX_RHIZA_DATA_DIR`,
`VELORIX_RHIZA_NODE_ID`, and a stable `VELORIX_RHIZA_CLUSTER_ID`. When an
object store is configured, Rhiza stores Local archives at
`path.Join(ObjStorePrefix, ClusterID, "local/v1")`; keep the cluster ID and
prefix stable across restarts. Production Local mode
requires explicit S3/GCS/Azure object storage and
`VELORIX_RHIZA_OBJECT_STORE_DURABILITY=before-ack`. It rejects peer listeners,
membership, peer identity secrets, admin credentials, and runtime `RHIZA_*`
configuration variables that could shadow the explicit settings. Build-time
`RHIZA_NATIVE_LIB_DIR` remains allowed. Local mode is not a multi-writer
fencing claim.

`velorix-meta check-config` applies the same production environment admission
as `serve` without opening Rhiza, accessing the archive, or binding a listener.
It validates settings as supplied; it does not parse TLS PEM files or contact
the object store. The serving process opens `Config::local()` and Rhiza performs
its native Local archive recovery when the data directory is empty. The
official test-helper staging path is not used as a production archive handoff.

Production transport may use native mutual TLS with
`VELORIX_META_TRANSPORT_SECURITY=native-mtls` and the server files
`VELORIX_META_TLS_CERT_FILE`, `VELORIX_META_TLS_KEY_FILE`, and
`VELORIX_META_TLS_CLIENT_CA_FILE`. Every client must present an identity signed
by the configured CA. `service-mesh-mtls` remains an operator-attested
external boundary and does not enable native TLS in this binary.

The read-only smoke probes accept the same trust material through
`VELORIX_META_TLS_CA_FILE`, `VELORIX_META_TLS_CLIENT_CERT_FILE`,
`VELORIX_META_TLS_CLIENT_KEY_FILE`, and optional
`VELORIX_META_TLS_DOMAIN_NAME`. Use `smoke --capabilities-only` for an
authenticated, non-mutating linearizable readiness check. Use
`smoke --probe-unauthenticated` with a trusted client identity to verify that
omitting the bearer token is rejected; this probe never falls back to a
plaintext or insecure TLS channel.

Example development configuration. Development Rhiza mode is single-node, so
the one membership entry publishes no key and needs no peer token, and
`VELORIX_RHIZA_DATA_DIR` must be a fresh empty directory:

```bash
VELORIX_META_MODE=development \
VELORIX_META_BIND=127.0.0.1:9090 \
VELORIX_META_BACKEND=rhiza-kv \
VELORIX_RHIZA_DATA_DIR=/tmp/velorix-rhiza \
VELORIX_RHIZA_NODE_ID=dev-1 \
VELORIX_RHIZA_CLUSTER_ID=velorix-dev \
VELORIX_RHIZA_BIND_ADDR=127.0.0.1:8100 \
VELORIX_RHIZA_PEER_ADDR=127.0.0.1:8200 \
VELORIX_RHIZA_MEMBERS_JSON='[{"node_id":"dev-1","url":"http://127.0.0.1:8100","peer_url":"quic://127.0.0.1:8200","public_key":""}]' \
cargo run -p velorix-meta --features rhiza-backend
```

Production startup is fail-closed: it requires an explicit mode, bind address,
durable backend, bearer token, and native mTLS certificate files. Unverified
service-mesh attestation is not accepted by this binary. It also requires exactly
three Rhiza voters plus before-ack object storage for `rhiza-kv`.
This backend can be used for catalog/admission durability work, but it must not
be used to satisfy `VELORIX_STANDING_RUNTIME_FENCING=required`.

For the isolated three-node no-PVC operator recovery gate, see
[development/rhiza-kv-k8s.md](../development/rhiza-kv-k8s.md). It is a validation
artifact with production trust disabled, not a durability attestation.

Run the object-store backend with the same S3-compatible settings used by
`velorix-api`:

```bash
VELORIX_META_MODE=development \
VELORIX_META_BACKEND=oss \
VELORIX_META_BIND=127.0.0.1:9090 \
VELORIX_S3_COMPAT=1 \
AWS_ENDPOINT_URL=http://127.0.0.1:9000 \
AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
VELORIX_S3_BUCKET=velorix \
VELORIX_S3_PREFIX=meta \
cargo run -p velorix-meta
```

For non-durable local API work, memory mode must also be explicit:

```bash
VELORIX_META_MODE=development VELORIX_META_BACKEND=memory cargo run -p velorix-meta
```

Point `velorix-api` at the meta service with:

```bash
VELORIX_META_GRPC_ENDPOINT=http://velorix-meta:9090 cargo run -p velorix-api
```

For a native-mTLS metadata endpoint, set the complete client bundle on the API
as well: `VELORIX_META_TLS_CA_FILE`,
`VELORIX_META_TLS_CLIENT_CERT_FILE`,
`VELORIX_META_TLS_CLIENT_KEY_FILE`, and `VELORIX_META_TLS_DOMAIN_NAME`, with an
`https://` endpoint. Partial bundles and HTTPS without verification material
fail closed.

or containerized with:

```bash
docker build -f Dockerfile.meta -t velorix-meta:dev .
```

## Deployment Shape

Velorix product evidence must not depend on PVC-created local disk. Rhiza nodes
keep their working state on ephemeral `emptyDir` volumes and rely on object
storage for the durable archive, so a cluster loss is recovered by generation
replacement rather than by restoring a volume.

## Required Product Semantics

The meta service is the hot admission authority. In `rhiza-kv` and `oss` modes,
object storage still receives recovery evidence and batch payloads so the
existing replay/query path can validate committed batches after restart.

Append admission order is:

1. Reserve the relation ingest range through the gRPC meta service.
2. Materialize Velorix recovery evidence to object storage.
3. Append the ingest batch payload to object storage with create-only semantics.
4. Periodically back up metadata to object storage.

If a range reservation succeeds but the payload append fails, an identical retry
is admitted as a duplicate metadata reservation and can complete the missing
object-store payload write.

## v0.21.0 local S3 logical-fencing recovery evidence

The recorded run at
`target/rhiza-service-recovery-evidence/20261027t111801z-12147/result.json`
used one Rhiza 0.21.0 Meta process and separate API and worker processes. It
started with fresh metadata directories and a new archive prefix. Initial
sum/count outputs were `2.5/1` and remained so after the first empty-directory
restore. A new input produced `4/2`; the duplicate retry kept `4/2` and the
descriptor unchanged. A second empty-directory restore also returned `4/2`.
Both restored views read checkpoint bodies from S3. The incremental query read
the new source batch once and the old batch zero times; checkpoint bodies used
`velorix-arrow-ipc-zstd3-v1`.

The run verified logical fencing with a `30000` logical-tick owner-lease
parameter. It reported `production_bounded_failover_safe=false` and
`production_multi_writer_safe=false`. This is local S3 evidence, not production
safety or multi-provider archive compatibility evidence.

## Historical 0.20 local logical-fencing recovery evidence

The 2026-10-27 historical split-process functional run used one Rhiza 0.20 Meta process,
separate API and worker processes, fresh metadata state and read caches after
each restart, and manually pinned VersityGW source commit
`fd04bc1df2656298577b82667a4195c77f8c7563`. Initial sum/count results were
`2.5/1`; they remained `2.5/1` after the first empty-directory Meta restore.
After one post-restore input, results were `4/2`; a duplicate retry preserved
the `1..2` ingest offsets, and a second empty-directory restore retained `4/2`.
Both restored views returned checkpoint-body S3 GETs. The first-restore
startup/query audit recorded zero GETs for the original source batch; the
incremental query recorded one GET for the new batch and zero for the original
batch. The query `logical_epoch` stayed equal across both recorded restart
pairs. The run used a 30000 logical-tick owner TTL parameter.

This is historical local logical-fencing functional evidence only. It does not
establish v0.21.0 archive compatibility, provider coverage, production bounded
failover, production multi-writer safety, or a production deployment. Existing
older data is not migrated by v0.21.0 and must remain unchanged when rejected;
that rejection behavior remains unverified.
