# Velorix 1.0 Production Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish Velorix 1.0 production readiness so docs, code, tests, CI-facing gates, and operational surfaces demonstrate an object-storage-authoritative, Kubernetes-native database runtime.

**Architecture:** Keep object storage as the durable authority and keep compute stateless. Kubernetes coordinates ownership and lifecycle, but every authoritative input, state, output, checkpoint, ownership, catalog, table, artifact, and benchmark decision is backed by Velorix-owned object-store records. Production APIs fail closed when bootstrap-only raw JSON, raw object URLs, local-only stores, missing ownership epochs, missing capability profiles, or incomplete resource policies are used.

**Tech Stack:** Rust workspace, `object_store`, Apache DataFusion, Arrow/Parquet, SlateDB, Foyer, Feldera artifact contracts, `kube`/`k8s-openapi` for Kubernetes control-plane integration, Tokio, serde, thiserror, proptest, criterion-style benchmark harnesses through existing custom JSON output.

---

## Current Baseline

The branch `feature/velorix-bootstrap` already has these production-readiness slices committed:

- Relation schema/fingerprint catalog contracts.
- Relation-aware ingest envelope/admission/replay path.
- Durable ownership epoch records and production fenced state/output/manifest publication.
- Checkpoint latest-candidate marker.
- Production persisted table spec that rejects raw URLs and resolves through a storage registry.
- Benchmark gate JSON validation and bootstrap benchmark output.
- Raw state ref rejection in production manifest publication.
- Production leased checkpoint publisher.
- Approximate object-request preflight for object-backed table scans.

The remaining work is not cosmetic. It is the difference between "strong bootstrap with fail-closed slices" and "1.0 production-ready database evidence."

## Non-Negotiable Contracts

- Object storage is the database authority.
- Kubernetes and etcd are coordination surfaces only.
- Velorix-owned manifests are authoritative for input, state, output, ownership, checkpoint, relation, table, artifact, and benchmark metadata.
- Iceberg is optional import/export/interoperability only after an adoption RFC.
- Foyer is a non-authoritative cache only.
- SlateDB owns durable state substrate internals; Velorix GC must never prefix-walk SlateDB internal objects.
- DataFusion is the ad hoc SQL query engine and must run under an enforceable resource policy in production.
- Feldera/DBSP is the standing-view semantic direction; direct runtime execution remains gated until artifact, state, resource, and recovery contracts are proven.
- Bootstrap/dev paths may remain only behind explicit APIs and must not be callable from production paths.

## File Structure Map

### Existing Files To Extend

- `Cargo.toml`: workspace dependencies and optional integration-test feature flags.
- `crates/velorix-core/src/query.rs`: query policy model and typed resource errors.
- `crates/velorix-core/tests/query.rs`: pure policy validation behavior.
- `crates/velorix-runtime/src/query.rs`: DataFusion execution enforcement.
- `crates/velorix-runtime/tests/query.rs`: object-backed and recovered-query enforcement tests.
- `crates/velorix-runtime/src/persisted_table.rs`: production table and policy lookup integration.
- `crates/velorix-runtime/tests/persisted_table.rs`: registry-backed table query tests.
- `crates/velorix-runtime/src/benchmark_gate.rs`: benchmark schema, baseline comparison, gate-level validation.
- `crates/velorix-runtime/tests/benchmark_gate.rs`: JSON gate and baseline validation.
- `crates/velorix-runtime/src/storage_registry.rs`: shared object-store registry identity and capabilities.
- `crates/velorix-storage/src/capability.rs`: backend capability profiles.
- `crates/velorix-storage/src/state.rs`: production publication, checkpoint lifecycle, retention checks.
- `crates/velorix-storage/src/state_store.rs`: SlateDB state reference behavior.
- `crates/velorix-storage/src/checkpoint_index.rs`: advisory latest marker validation and fallback.
- `crates/velorix-storage/src/object_key.rs`: new object-key layouts.
- `crates/velorix-storage/tests/checkpoint_publish.rs`: publication and lifecycle behavior.
- `crates/velorix-control/src/lease.rs`: partition lease client trait and in-memory implementation.
- `crates/velorix-control/tests/lease.rs`: lease behavior.
- `crates/velorix-cli/src/main.rs`: readiness, benchmark, and admin inspection commands.
- `benches/local_incremental.rs`: benchmark JSON output and backend selection.
- `docs/architecture/*.md`: status updates only after implementation and verification.
- `docs/superpowers/plans/2026-05-05-velorix-1-0-production-readiness.md`: this execution plan.

### New Files To Create

- `crates/velorix-core/src/resource_policy.rs`: normalized `QueryExecutionPolicyV1` type and parse/validation helpers.
- `crates/velorix-core/tests/resource_policy.rs`: policy validation unit tests.
- `crates/velorix-runtime/src/query_runtime.rs`: runtime limiter, timeout, byte counting, and DataFusion session construction.
- `crates/velorix-runtime/src/object_meter.rs`: exact object-store request/byte metering wrapper.
- `crates/velorix-runtime/tests/object_meter.rs`: exact request and byte accounting tests.
- `crates/velorix-runtime/src/query_policy_catalog.rs`: object-store-backed policy catalog.
- `crates/velorix-runtime/tests/query_policy_catalog.rs`: policy catalog tests.
- `crates/velorix-runtime/src/readiness.rs`: production readiness report assembly.
- `crates/velorix-runtime/tests/readiness.rs`: fail-closed readiness report tests.
- `crates/velorix-storage/src/checkpoint_lifecycle.rs`: checkpoint status records, retention policy, and validation.
- `crates/velorix-storage/src/gc.rs`: manifest-aware garbage collector planning and execution.
- `crates/velorix-storage/tests/checkpoint_lifecycle.rs`: checkpoint status transitions.
- `crates/velorix-storage/tests/gc.rs`: GC mark/sweep behavior.
- `crates/velorix-storage/tests/s3_compat.rs`: env-gated S3-compatible object-store tests.
- `crates/velorix-control/src/kubernetes.rs`: Kubernetes Lease-backed implementation.
- `crates/velorix-control/tests/kubernetes_lease.rs`: env-gated Kubernetes Lease tests.
- `crates/velorix-k8s/Cargo.toml`: Kubernetes operator/CRD crate.
- `crates/velorix-k8s/src/lib.rs`: CRD type exports.
- `crates/velorix-k8s/src/crd.rs`: CRD specs and status types.
- `crates/velorix-k8s/src/controller.rs`: reconciliation skeleton and status writer.
- `crates/velorix-k8s/tests/crd_schema.rs`: CRD schema tests.
- `crates/velorix-k8s/tests/reconcile.rs`: pure reconcile-core tests.
- `crates/velorix-runtime/src/feldera_registry.rs`: persisted Feldera artifact registry.
- `crates/velorix-runtime/tests/feldera_registry.rs`: artifact registry and fingerprint tests.
- `baselines/benchmark/local/pr-smoke.json`: local PR-smoke baseline.
- `baselines/benchmark/s3/nightly.json`: S3-compatible nightly baseline.
- `baselines/benchmark/s3/release.json`: S3-compatible release baseline.
- `.github/workflows/ci.yml`: PR smoke CI.
- `.github/workflows/nightly.yml`: S3-compatible nightly gate.
- `.github/workflows/release-gate.yml`: release benchmark and S3-compatible evidence gate.
- `deny.toml`: dependency/license/advisory governance.

## Execution Discipline

- [ ] Start every implementation batch with `git fetch origin main`.
- [ ] Use a new short-lived branch only if the current branch is not `feature/velorix-bootstrap`.
- [ ] Each task must start with failing behavior-focused tests.
- [ ] Each task must end with `cargo fmt --check`, `git diff --check`, focused tests, and a coherent commit.
- [ ] Do not push partial failing work.
- [ ] Update Yeoul memory after each verified production-readiness slice.
- [ ] Update docs only after code and tests prove the contract.

## Phase 0: Re-Establish the Gate Before More Code

### Task 0.1: Add a Production Readiness Status Matrix

**Purpose:** Make it impossible to confuse bootstrap-ready with 1.0-ready.

**Files:**
- Create: `docs/architecture/production-readiness-status.md`
- Modify: `docs/architecture/package-review-index.md`

- [ ] Create a status table with these rows: ingest, relation catalog, object-store capability, ownership, checkpoint lifecycle, state substrate, DataFusion policy, table registry, Feldera artifact registry, benchmark gate, S3-compatible tests, Kubernetes operator, GC, dependency governance.
- [ ] Give each row these columns: `Contract`, `Current Evidence`, `1.0 Required Evidence`, `Status`, `Blocking Tasks`.
- [ ] Set statuses to `complete`, `partial`, or `missing`; do not use percentage estimates.
- [ ] Run `rg -n "production-ready|1.0|bootstrap" docs/architecture/production-readiness-status.md docs/architecture/package-review-index.md`.
- [ ] Commit: `docs: add production readiness status matrix`.

### Task 0.2: Add CI Skeleton Before Runtime Changes

**Purpose:** Lock in the commands that all later slices must keep passing.

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `deny.toml`
- Modify: `Cargo.toml`

- [ ] Add a PR workflow with jobs for `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, and benchmark JSON smoke validation.
- [ ] Add `cargo-deny` configuration with explicit allowed licenses for current dependencies.
- [ ] Add a local verification script only if duplicated workflow commands become hard to keep aligned; otherwise keep commands directly in workflow YAML.
- [ ] Run `cargo fmt --check`.
- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] Run `cargo test --workspace`.
- [ ] Commit: `ci: add production readiness smoke gate`.

## Phase 1: Complete DataFusion QueryExecutionPolicyV1

### Task 1.1: Split Core Query Policy Into a Stable V1 Contract

**Purpose:** Move from bootstrap caps to a named production resource contract.

**Files:**
- Create: `crates/velorix-core/src/resource_policy.rs`
- Modify: `crates/velorix-core/src/lib.rs`
- Modify: `crates/velorix-core/src/query.rs`
- Create: `crates/velorix-core/tests/resource_policy.rs`

**Required model:**

```rust
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryExecutionPolicyV1 {
    pub max_sql_bytes: Option<usize>,
    pub planning_timeout_ms: Option<u64>,
    pub execution_timeout_ms: Option<u64>,
    pub max_output_rows: Option<usize>,
    pub max_output_bytes: Option<u64>,
    pub max_scan_files: Option<usize>,
    pub max_scan_bytes: Option<u64>,
    pub max_object_requests: Option<usize>,
    pub max_concurrent_queries: Option<usize>,
    pub memory_limit_bytes: Option<u64>,
    pub spill_limit_bytes: Option<u64>,
    pub batch_size: Option<std::num::NonZeroUsize>,
    pub target_partitions: Option<std::num::NonZeroUsize>,
}
```

- [ ] Keep `QueryPolicy` as a type alias or compatibility wrapper for bootstrap callers.
- [ ] Add typed errors for invalid zero timeout, invalid zero concurrency, invalid zero memory/spill budget, and output byte overrun.
- [ ] Add tests that unknown JSON fields fail, zero-valued invalid fields fail, and old `QueryPolicy` callers still compile.
- [ ] Run `cargo test -p velorix-core --test resource_policy`.
- [ ] Commit: `feat: add query execution policy v1`.

### Task 1.2: Enforce Planning and Execution Timeouts

**Purpose:** Production queries must not run indefinitely.

**Files:**
- Create: `crates/velorix-runtime/src/query_runtime.rs`
- Modify: `crates/velorix-runtime/src/query.rs`
- Modify: `crates/velorix-runtime/src/lib.rs`
- Modify: `crates/velorix-runtime/tests/query.rs`

**Required behavior:**

- Planning timeout wraps `context.sql(sql)` and `dataframe.into_optimized_plan()`.
- Execution timeout wraps `collect()`.
- Timeout errors are typed as `PlanningTimeout { timeout_ms }` and `ExecutionTimeout { timeout_ms }`.
- `LIMIT 1` cannot bypass planning timeout, scan byte limit, or object request limit.

- [ ] Write tests with a deliberately tiny timeout and a deterministic long-running query path. Prefer a DataFusion UDF sleep only if it is fully local and deterministic; otherwise use an injected test hook in `QueryRuntimeLimits`.
- [ ] Implement `QueryRuntimeLimits::run_planning` and `QueryRuntimeLimits::run_execution` using `tokio::time::timeout`.
- [ ] Run `cargo test -p velorix-runtime --test query planning_timeout`.
- [ ] Run `cargo test -p velorix-runtime --test query execution_timeout`.
- [ ] Commit: `feat: enforce query planning and execution timeouts`.

### Task 1.3: Enforce Exact Output Byte Limits

**Purpose:** Row caps are not enough; large rows can exhaust memory or egress.

**Files:**
- Modify: `crates/velorix-core/src/resource_policy.rs`
- Modify: `crates/velorix-runtime/src/query_runtime.rs`
- Modify: `crates/velorix-runtime/src/query.rs`
- Modify: `crates/velorix-runtime/tests/query.rs`

- [ ] Add `QueryPolicyError::OutputBytesExceeded { observed_bytes, max_bytes }`.
- [ ] Count output bytes from collected `RecordBatch` buffers using Arrow array memory size APIs.
- [ ] Stop after the first batch that exceeds the limit and return the typed error.
- [ ] Add tests where one row exceeds bytes while row count is under the row cap.
- [ ] Run `cargo test -p velorix-runtime --test query output_bytes`.
- [ ] Commit: `feat: enforce query output byte limits`.

### Task 1.4: Add Query Concurrency Pool

**Purpose:** Tenant/global concurrency must be bounded independently of DataFusion internals.

**Files:**
- Modify: `crates/velorix-runtime/src/query_runtime.rs`
- Modify: `crates/velorix-runtime/src/query.rs`
- Create: `crates/velorix-runtime/tests/query_concurrency.rs`

**Required model:**

```rust
#[derive(Clone, Debug)]
pub struct QueryExecutionLimiter {
    permits: Arc<tokio::sync::Semaphore>,
}
```

- [ ] Add a constructor from `QueryExecutionPolicyV1::max_concurrent_queries`.
- [ ] Return `ConcurrencyLimitExceeded { max_concurrent_queries }` if `try_acquire_owned` fails.
- [ ] Thread an optional shared limiter through object-backed and recovered query entry points.
- [ ] Add a test that holds one permit and proves the second query fails immediately.
- [ ] Run `cargo test -p velorix-runtime --test query_concurrency`.
- [ ] Commit: `feat: enforce query concurrency limits`.

### Task 1.5: Add Memory and Spill Configuration Boundary

**Purpose:** Production policy must include explicit memory and spill semantics even where DataFusion enforcement is version-dependent.

**Files:**
- Modify: `crates/velorix-runtime/src/query_runtime.rs`
- Modify: `crates/velorix-runtime/tests/query.rs`
- Modify: `docs/architecture/datafusion-resource-policy-v1.md`

- [ ] Create `DataFusionSessionFactory` that builds every production `SessionContext`.
- [ ] Configure batch size and target partitions only through this factory.
- [ ] Add `memory_limit_bytes` and `spill_limit_bytes` to the factory contract.
- [ ] If DataFusion 53 exposes a stable memory pool/spill manager API in this workspace, wire it directly and test failure when allocation exceeds the pool.
- [ ] If the stable API is not exposed, fail closed in production when `memory_limit_bytes` or `spill_limit_bytes` is unset and document the exact unsupported enforcement as `partial` in `production-readiness-status.md`.
- [ ] Run `cargo test -p velorix-runtime --test query`.
- [ ] Commit: `feat: centralize datafusion production session policy`.

## Phase 2: Exact Object-Store Metering and S3-Compatible Evidence

### Task 2.1: Replace Approximate Object Request Preflight With a Metered Store

**Purpose:** Current object request counting is a preflight estimate. 1.0 needs exact accounting around DataFusion object-store operations.

**Files:**
- Create: `crates/velorix-runtime/src/object_meter.rs`
- Modify: `crates/velorix-runtime/src/query.rs`
- Modify: `crates/velorix-runtime/src/benchmark_gate.rs`
- Create: `crates/velorix-runtime/tests/object_meter.rs`
- Modify: `crates/velorix-runtime/tests/query.rs`
- Modify: `crates/velorix-runtime/tests/persisted_table.rs`

**Required behavior:**

- Count every `get`, `get_opts`, `head`, `list`, `list_with_offset`, `put`, `put_opts`, `delete`, `copy`, and multipart initiation when the trait exposes the operation.
- Return `ObjectRequestsExceeded` before delegating when the next operation would exceed the policy.
- Track bytes returned to query execution and expose them to benchmark output.
- Keep current preflight only as an optional fast rejection before DataFusion runs.

- [ ] Add tests for exact request count on list plus file reads.
- [ ] Add a test where preflight passes but runtime metering fails during execution.
- [ ] Add a test where `LIMIT 1` still triggers request accounting for underlying file access.
- [ ] Run `cargo test -p velorix-runtime --test object_meter`.
- [ ] Run `cargo test -p velorix-runtime --test persisted_table`.
- [ ] Commit: `feat: meter object store requests during query execution`.

### Task 2.2: Add S3-Compatible Object Store Test Harness

**Purpose:** Local memory object stores are not production interoperability evidence.

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/velorix-storage/Cargo.toml`
- Modify: `crates/velorix-runtime/Cargo.toml`
- Create: `crates/velorix-storage/tests/s3_compat.rs`
- Create: `crates/velorix-runtime/tests/s3_compat_query.rs`
- Create: `docs/architecture/s3-compatible-test-harness.md`

**Environment contract:**

- `VELORIX_S3_COMPAT=1`
- `AWS_ENDPOINT_URL`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `VELORIX_S3_BUCKET`
- `VELORIX_S3_PREFIX`

- [ ] Tests must skip with a clear message unless `VELORIX_S3_COMPAT=1`.
- [ ] Storage tests must prove create-only put conflict, read-after-write, list-after-write, range read, and delete behavior.
- [ ] Runtime tests must write Parquet under the S3 prefix and query through the production table registry.
- [ ] Capability tests must reject a profile where conditional create or list-after-write is false.
- [ ] Run without env and confirm tests skip cleanly.
- [ ] Run with MinIO or an S3-compatible endpoint and confirm tests pass.
- [ ] Commit: `test: add s3-compatible production storage harness`.

### Task 2.3: Make Object Store Capabilities Namespace-Aware

**Purpose:** One backend profile is too coarse for authoritative input/state/output/checkpoint/table/artifact namespaces.

**Files:**
- Modify: `crates/velorix-storage/src/capability.rs`
- Modify: `crates/velorix-runtime/src/storage_registry.rs`
- Modify: `crates/velorix-storage/tests/object_store_capability.rs`
- Modify: `crates/velorix-runtime/tests/persisted_table.rs`

**Required model:**

```rust
pub enum AuthoritativeNamespace {
    Ingest,
    State,
    Output,
    Checkpoint,
    Ownership,
    TableCatalog,
    ArtifactCatalog,
    BenchmarkEvidence,
}
```

- [ ] Add `AuthoritativeObjectStoreCapabilitiesV1` with a `BTreeMap<AuthoritativeNamespace, ObjectStoreCapabilityProfile>`.
- [ ] Startup validation must require every namespace.
- [ ] Production table registry must require a registered store identity with capabilities, not only a DataFusion object store pointer.
- [ ] Add tests for missing namespace, weak namespace, and all-namespaces-valid startup.
- [ ] Commit: `feat: validate object store capabilities by namespace`.

## Phase 3: Checkpoint Lifecycle, Recovery, and GC

### Task 3.1: Add Checkpoint Lifecycle Status Records

**Purpose:** Checkpoints need observable lifecycle state, not only manifests and latest-candidate markers.

**Files:**
- Create: `crates/velorix-storage/src/checkpoint_lifecycle.rs`
- Modify: `crates/velorix-storage/src/lib.rs`
- Modify: `crates/velorix-storage/src/object_key.rs`
- Modify: `crates/velorix-storage/src/state.rs`
- Create: `crates/velorix-storage/tests/checkpoint_lifecycle.rs`

**Required statuses:**

- `Publishing`
- `Published`
- `Superseded`
- `GcEligible`
- `GcRunning`
- `GcCompleted`
- `Quarantined`

- [ ] Status objects must be create-only or monotonic transition records; do not mutate prior records in place.
- [ ] `Published` requires manifest digest verification and current ownership epoch.
- [ ] `Superseded` requires a child checkpoint whose parent is the current checkpoint.
- [ ] `Quarantined` records the validation error and blocks latest-candidate use.
- [ ] Add tests for valid transitions and invalid regressions.
- [ ] Commit: `feat: add checkpoint lifecycle records`.

### Task 3.2: Harden Latest-Candidate Recovery

**Purpose:** Advisory latest markers are useful only when every referenced object is revalidated.

**Files:**
- Modify: `crates/velorix-storage/src/checkpoint_index.rs`
- Modify: `crates/velorix-storage/src/state.rs`
- Modify: `crates/velorix-storage/tests/checkpoint_publish.rs`
- Create: `crates/velorix-storage/tests/checkpoint_recovery_index.rs`

**Required behavior:**

- If marker JSON is malformed, fallback to manifest scan.
- If marker schema version is unsupported, fallback to manifest scan.
- If marker digest mismatches manifest bytes, fallback to manifest scan and quarantine marker.
- If marker parent pointer does not match manifest parent, fallback to manifest scan and quarantine marker.
- If marker references a manifest whose state/output refs are missing, fallback to scan and quarantine marker.
- A valid marker may select only a manifest that passes body/key/digest/parent/input/state/output validation.

- [ ] Add table-driven tests for every corruption mode.
- [ ] Add one large-manifest-count test that validates marker lookup latency stays bounded.
- [ ] Commit: `feat: harden checkpoint latest recovery`.

### Task 3.3: Implement Manifest-Aware Garbage Collection

**Purpose:** Production cannot accumulate unbounded garbage, and GC must not delete authoritative or SlateDB-owned objects incorrectly.

**Files:**
- Create: `crates/velorix-storage/src/gc.rs`
- Modify: `crates/velorix-storage/src/lib.rs`
- Modify: `crates/velorix-storage/src/object_key.rs`
- Create: `crates/velorix-storage/tests/gc.rs`
- Modify: `docs/architecture/storage-contract.md`
- Modify: `docs/architecture/state-substrate-contract.md`

**Required behavior:**

- GC builds a mark set from published manifests, checkpoint lifecycle records, ownership records, relation catalog, table catalog, artifact registry, and benchmark evidence.
- GC can delete only Velorix-owned raw state/output/temp objects that are unreferenced and older than a retention window.
- GC must never delete under SlateDB internal prefixes.
- GC must never delete current or parent-chain checkpoint manifests.
- GC produces a `GcPlanV1` JSON evidence object before execution.
- GC execution writes `GcRunV1` evidence with deleted keys, skipped keys, errors, and policy.

- [ ] Add tests for unreferenced raw output deletion.
- [ ] Add tests for referenced output retention.
- [ ] Add tests proving SlateDB prefixes are skipped even when unreferenced.
- [ ] Add tests for dry-run plan and executed run evidence.
- [ ] Commit: `feat: add manifest-aware production gc`.

### Task 3.4: Deepen SlateDB State References

**Purpose:** Current tagged SlateDB refs are a boundary. 1.0 needs stable recovery evidence and GC-safe state root semantics.

**Files:**
- Modify: `crates/velorix-storage/src/state_store.rs`
- Modify: `crates/velorix-storage/src/manifest.rs`
- Modify: `crates/velorix-storage/tests/checkpoint_publish.rs`
- Create: `crates/velorix-storage/tests/slatedb_state.rs`
- Modify: `docs/architecture/state-substrate-contract.md`

**Required behavior:**

- `SlateDbCheckpointRefV1` must include `db_path`, `state_key`, `state_digest`, `state_bytes`, and `created_by_checkpoint_version`.
- Reading a SlateDB ref through a raw object store fails closed.
- Reading a raw ref through a production SlateDB-only recovery path fails closed.
- State digest is verified on read.
- Publication validates the same-transaction SlateDB marker and payload-key
  readability without hashing the full state payload on the manifest publish
  path.
- Closing/reopening the SlateDB store can recover the written state.
- GC retains SlateDB state by retaining root references, not by deleting internal objects.

- [x] Add reopen recovery tests using a real temp object store.
- [x] Add digest mismatch tests.
- [x] Add marker mismatch and marker-plus-payload existence tests.
- [ ] Commit: `feat: add recoverable slatedb checkpoint refs`.

## Phase 4: Kubernetes-Native Control Plane

### Task 4.1: Add Kubernetes Lease Client

**Purpose:** In-memory leases are useful tests, but production writer ownership needs a Kubernetes-native client.

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/velorix-k8s/Cargo.toml`
- Create: `crates/velorix-k8s/src/lib.rs`
- Create: `crates/velorix-k8s/src/lease.rs`
- Create: `crates/velorix-k8s/tests/kubernetes_lease.rs`

**Dependencies:**

- `kube`
- `k8s-openapi`
- `schemars`

**Required behavior:**

- Keep `crates/velorix-control` permanently kube-free; live Kubernetes
  dependencies belong in `crates/velorix-k8s`.
- Acquire or renew a Kubernetes `Lease`.
- Convert the Kubernetes lease identity into a storage `OwnershipEpochRecord`.
- Fail closed when the Kubernetes lease holder identity does not match the worker.
- Fail closed when lease renew fails.
- Preserve the rule that Kubernetes lease alone is not enough; durable epoch record is still required before production publication.

- [ ] Unit tests use a fake Kubernetes client or serialized Lease objects.
- [ ] Integration tests run only when `VELORIX_K8S_INTEGRATION=1`.
- [ ] Commit: `feat: add kubernetes lease client`.

### Task 4.2a: Add Kube-Free Control-Plane Contract Skeleton

**Purpose:** Fix the `missing` Kubernetes direction with a minimal contract
skeleton without pretending that a live operator exists.

**Files:**
- Modify: `crates/velorix-control/Cargo.toml`
- Modify: `crates/velorix-control/src/lib.rs`
- Create: `crates/velorix-control/src/control_plane_contract.rs`
- Create: `crates/velorix-control/src/reconcile_plan.rs`
- Create: `crates/velorix-control/tests/control_plane_contract.rs`
- Modify: `docs/architecture/production-readiness-status.md`

**Required behavior:**

- Keep `velorix-control` free of `kube`, `k8s-openapi`, `schemars`, and live
  client dependencies.
- Use plain serde wire contracts with explicit `api_version`, `kind`,
  `spec_version`, minimal metadata, and `deny_unknown_fields`.
- Keep status observed-only; status must not become checkpoint or ownership
  authority.
- Reconcile planning is side-effect-free and requires matching durable epoch
  record evidence before worker start.
- Lease-only ownership, status-only progress, conflicting lease owners, and
  conflicting epoch records fail closed.

- [x] Add stable serde contract and unknown-field tests.
- [x] Add side-effect-free reconcile plan tests for status-only progress,
  lease-only ownership, missing durable epoch record, matching epoch record,
  stale worker replacement, lease-owner conflict, and epoch-record conflict.
- [x] Keep production readiness status honest: contract skeleton exists, but
  live Kubernetes operator/client evidence remains missing.
- [ ] Commit: `feat: add kubernetes control-plane contract skeleton`.

### Task 4.2: Add Kubernetes CRD Crate

**Purpose:** 1.0 must expose operator-managed lifecycle surfaces, not only library APIs.

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/velorix-k8s/Cargo.toml`
- Create: `crates/velorix-k8s/src/lib.rs`
- Create: `crates/velorix-k8s/src/crd.rs`
- Create: `crates/velorix-k8s/tests/crd_schema.rs`

**CRDs:**

- `VelorixDatabase`
- `VelorixStream`
- `VelorixTable`
- `VelorixWorkerShard`
- `VelorixCheckpointPolicy`
- `VelorixBenchmarkGate`

**Required status fields:**

- observed generation.
- last accepted relation schema fingerprint.
- current owner epoch per shard.
- latest published checkpoint.
- latest benchmark gate result.
- readiness condition with `True`, `False`, `Unknown`.
- reason and message fields for fail-closed status.

- [x] Generate OpenAPI schemas through `kube::CustomResource`.
- [x] Add schema tests that required spec fields are present.
- [x] Add serde round-trip tests for all CRDs.
- [ ] Commit: `feat: add velorix kubernetes crds`.

### Task 4.3: Add Controller Reconciliation Skeleton

**Purpose:** Reconciliation should wire CRD desired state to object-store-backed authority without making Kubernetes authoritative.

**Files:**
- Create: `crates/velorix-k8s/src/controller.rs`
- Create: `crates/velorix-k8s/tests/reconcile.rs`
- Modify: `crates/velorix-k8s/src/lib.rs`

**Required behavior:**

- Reconcile validates object-store capability profile references.
- Reconcile validates relation catalog fingerprint references.
- Reconcile writes status only after object-store-backed validation passes.
- Reconcile never writes checkpoint manifests directly from Kubernetes state.
- Missing object-store authority produces `Ready=False` with reason `MissingAuthorityRecord`.

- [x] Add pure reconcile-core tests for missing authority, relation mismatch, stale checkpoint status, ready stream, and cross-authority evidence isolation.
- [ ] Commit: `feat: add kubernetes controller readiness reconciliation`.

## Phase 5: Production Catalogs for Query Policy, Tables, and Feldera Artifacts

### Task 5.1: Add Query Policy Catalog

**Purpose:** Production table specs reference `query_policy_id`, so policy records must be durable and validated.

**Files:**
- Create: `crates/velorix-runtime/src/query_policy_catalog.rs`
- Modify: `crates/velorix-runtime/src/lib.rs`
- Modify: `crates/velorix-runtime/src/persisted_table.rs`
- Create: `crates/velorix-runtime/tests/query_policy_catalog.rs`
- Modify: `crates/velorix-runtime/tests/persisted_table.rs`

**Required behavior:**

- Create-only policy object key: `v1/query-policy/{tenant_id}/{query_policy_id}.json`.
- Policy body includes `schema_version`, `tenant_id`, `query_policy_id`, and `QueryExecutionPolicyV1`.
- Production table query resolves policy from the catalog, not from an ad hoc caller parameter.
- Missing policy fails closed.
- Cross-tenant policy use fails closed.

- [ ] Add tests for create, duplicate, missing, cross-tenant, and successful table query.
- [ ] Commit: `feat: add durable query policy catalog`.

### Task 5.2: Validate Table Schema Fingerprint Against Relation Catalog

**Purpose:** A production table spec cannot merely carry a fingerprint string; it must match the relation catalog.

**Files:**
- Modify: `crates/velorix-runtime/src/persisted_table.rs`
- Modify: `crates/velorix-runtime/tests/persisted_table.rs`
- Modify: `crates/velorix-core/src/relation.rs`

**Required behavior:**

- Production table creation accepts a relation catalog reference.
- The table spec `schema_fingerprint` must equal the relation catalog fingerprint.
- Querying a table whose relation catalog fingerprint changed fails closed until a new table snapshot/spec is created.

- [ ] Add tests for matching fingerprint success.
- [ ] Add tests for mismatch rejection at create.
- [ ] Add tests for stale catalog rejection at query time.
- [ ] Commit: `feat: bind production tables to relation catalog fingerprints`.

### Task 5.3: Add Feldera Artifact Registry

**Purpose:** Feldera compile artifacts must be durable, relation-bound, and recoverable before direct runtime adoption.

**Files:**
- Create: `crates/velorix-runtime/src/feldera_registry.rs`
- Modify: `crates/velorix-runtime/src/lib.rs`
- Create: `crates/velorix-runtime/tests/feldera_registry.rs`
- Modify: `docs/architecture/feldera-artifact-contract.md`

**Required behavior:**

- Create-only artifact object key remains the storage-owned `v1/feldera-artifacts/{artifact_id}/sha256/{artifact_hash_hex}.artifact.json`; tenant/artifact-id lookup is deferred to a separate create-only index only if product semantics require it.
- Artifact body includes relation ids, schema fingerprints, artifact digest, compile timestamp, compiler identity, and accepted runtime ABI.
- Artifact registry validates through `velorix-core::feldera_artifact`.
- Fingerprint mismatch fails closed.
- Unknown generated Rust ABI fails closed.
- Direct runtime execution remains disabled until a separate DBSP runtime adoption gate passes.

- [x] Add tests using existing Feldera fixture JSON.
- [x] Add tests for mismatch, duplicate, unknown ABI, valid artifact retrieval, and select-time catalog drift rejection.
- [x] Commit: `feat: add durable feldera artifact registry`.

## Phase 6: Benchmark Gates and Release Evidence

### Task 6.1: Add Baseline Files and Comparator CLI

**Purpose:** Benchmarks become gates only when they compare machine-readable output against committed baselines.

**Files:**
- Create: `baselines/benchmark/local/pr-smoke.json`
- Create: `baselines/benchmark/s3/nightly.json`
- Create: `baselines/benchmark/s3/release.json`
- Modify: `crates/velorix-cli/src/main.rs`
- Modify: `crates/velorix-runtime/src/benchmark_gate.rs`
- Modify: `crates/velorix-runtime/tests/benchmark_gate.rs`

**CLI command:**

```bash
cargo run -p velorix-cli -- benchmark-gate \
  --gate-level pr-smoke \
  --backend local \
  --baseline baselines/benchmark/local/pr-smoke.json \
  --result target/velorix-bench/local-pr-smoke.json
```

- [x] CLI exits 0 when result is within baseline.
- [x] CLI exits non-zero when backend mismatches baseline.
- [x] CLI exits non-zero when object request metrics are missing.
- [x] CLI exits non-zero when any budget regression exceeds threshold.
- [x] Commit: `feat: add benchmark gate comparator cli`.

### Task 6.2: Split PR, Nightly, and Release Benchmark Workflows

**Purpose:** Local smoke, S3-compatible nightly, and release gates must not be mixed.

**Files:**
- Modify: `.github/workflows/ci.yml`
- Create: `.github/workflows/nightly.yml`
- Create: `.github/workflows/release-gate.yml`
- Modify: `docs/architecture/benchmark-gate-v1.md`

**Required behavior:**

- PR smoke runs local backend only.
- Nightly runs S3-compatible backend only when secrets are configured.
- Release gate requires S3-compatible result artifact and matching S3 baseline.
- Local and S3 baselines are not interchangeable.

- [x] Add artifact upload for benchmark JSON results.
- [x] Add explicit workflow failure when release gate has no S3 result artifact.
- [x] Commit: `ci: split benchmark smoke nightly and release gates`.

### Task 6.3: Extend Benchmark Workloads

**Purpose:** A single local incremental benchmark is not representative 1.0 evidence.

**Files:**
- Modify: `benches/local_incremental.rs`
- Create: `benches/s3_incremental.rs`
- Modify: `crates/velorix-runtime/src/benchmark_gate.rs`
- Modify: `crates/velorix-runtime/tests/benchmark_gate.rs`

**Required workloads:**

- ingest envelope validation throughput.
- checkpoint publish latency.
- checkpoint recovery latency.
- DataFusion table scan latency and object requests.
- SlateDB state write/read/reopen latency.
- GC dry-run planning latency.

- [ ] Each workload emits p50 and p95 latency.
- [ ] Each workload emits object requests and scan bytes where object storage is used.
- [x] S3-compatible benchmark requires `VELORIX_S3_COMPAT=1`.
- [ ] Commit: `bench: add production readiness workloads`.

Progress note: this task now has strict V1 workload detail schema and real
local `local_incremental` workload details for ingest envelope validation,
checkpoint publication, checkpoint recovery, and bounded DataFusion Parquet
table scan instrumentation. This slice adds real local SlateDB state
write/read/reopen instrumentation through `SlateDbStateStore` without exposing
state-store internals; object request counts come from the benchmark harness
metered object-store wrapper. This slice also adds local GC dry-run planning
instrumentation through `CheckpointPublisher::plan_garbage_collection`; GC
execution, listing-consistency failure modes, and S3-compatible evidence remain
pending.
`s3_incremental` is fail-closed and does not emit benchmark JSON without a live
implementation.

## Phase 7: Admin Readiness and Product Surface

### Task 7.1: Add Readiness Report API and CLI

**Purpose:** Operators need one machine-readable answer for whether the deployment is production-ready.

**Files:**
- Create: `crates/velorix-runtime/src/readiness.rs`
- Modify: `crates/velorix-runtime/src/lib.rs`
- Modify: `crates/velorix-cli/src/main.rs`
- Create: `crates/velorix-runtime/tests/readiness.rs`

**Report fields:**

- `schema_version`
- `deployment_id`
- `authority_store_id`
- `capability_status`
- `ownership_status`
- `checkpoint_status`
- `state_status`
- `query_policy_status`
- `table_catalog_status`
- `feldera_artifact_status`
- `benchmark_gate_status`
- `kubernetes_status`
- `production_ready`
- `blocking_reasons`

- [ ] Missing S3-compatible evidence sets `production_ready=false`.
- [ ] Missing Kubernetes Lease client evidence sets `production_ready=false`.
- [ ] Bootstrap raw state path sets `production_ready=false`.
- [ ] Valid evidence across all gates sets `production_ready=true`.
- [ ] CLI prints JSON only when `--json` is passed.
- [ ] Commit: `feat: add production readiness report`.

### Task 7.2: Add Admin Inspection Commands

**Purpose:** Debugging production state should not require ad hoc object-store browsing.

**Files:**
- Modify: `crates/velorix-cli/src/main.rs`
- Create: `crates/velorix-cli/tests/cli_readiness.rs`

**Commands:**

- `velorix-cli readiness --json`
- `velorix-cli checkpoint inspect --stream-id <id>`
- `velorix-cli gc plan --stream-id <id> --retention-checkpoints <n>`
- `velorix-cli table inspect --tenant-id <id> --table-id <id>`
- `velorix-cli benchmark-gate --gate-level <level> --backend <backend>`

- [ ] Commands read from object storage only.
- [ ] Commands never repair or mutate state unless the subcommand name is explicitly mutating.
- [ ] JSON output uses stable `schema_version`.
- [ ] Commit: `feat: add production admin inspection commands`.

## Phase 8: Dependency Governance and Release Cut

### Task 8.1: Lock Dependency Governance

**Purpose:** 1.0 must have repeatable dependency review, especially around DataFusion, SlateDB, Foyer, Feldera-related artifacts, object-store, and Kubernetes crates.

**Files:**
- Modify: `deny.toml`
- Create: `docs/architecture/dependency-governance.md`
- Modify: `.github/workflows/ci.yml`

- [ ] `cargo deny check` runs in CI.
- [ ] Duplicate versions for core crates are reviewed and justified.
- [ ] Licenses are explicitly allowed.
- [ ] Advisory failures block PRs except documented ignores with owner and expiry.
- [ ] Commit: `chore: add dependency governance gate`.

### Task 8.2: Add Release Candidate Checklist

**Purpose:** The repo needs a deterministic rule for calling 1.0 ready.

**Files:**
- Create: `docs/release/1.0-readiness-checklist.md`
- Modify: `docs/architecture/production-readiness-status.md`

**Required checklist:**

- `cargo fmt --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- S3-compatible storage harness pass.
- S3-compatible query harness pass.
- Kubernetes Lease integration pass.
- CRD schema generation pass.
- Benchmark PR smoke pass.
- Benchmark nightly S3 pass.
- Benchmark release S3 pass.
- GC dry-run and execution evidence pass.
- Readiness CLI returns `production_ready=true`.
- No architecture status row is `missing` or `partial`.

- [ ] Commit: `docs: add velorix 1.0 release readiness checklist`.

### Task 8.3: Final Release Verification Commit

**Purpose:** Produce the final evidence commit for 1.0 readiness.

**Files:**
- Modify: `docs/architecture/production-readiness-status.md`
- Modify: `docs/release/1.0-readiness-checklist.md`

- [ ] Run local full gate:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p velorix-cli -- readiness --json
```

- [ ] Run S3-compatible gate with required env:

```bash
VELORIX_S3_COMPAT=1 cargo test --workspace s3_compat
VELORIX_S3_COMPAT=1 cargo bench -p velorix-runtime --bench s3_incremental
```

- [ ] Run Kubernetes gate with required env:

```bash
VELORIX_K8S_INTEGRATION=1 cargo test -p velorix-control --test kubernetes_lease
cargo test -p velorix-k8s
```

- [ ] Update checklist with exact command outputs and artifact paths.
- [ ] Commit: `docs: record velorix 1.0 production readiness evidence`.

## Subagent Orchestration Plan

Use one implementation agent and one review agent per bounded slice. The orchestrator owns sequencing, verification, staging, commits, pushes, and final PR preparation.

1. Agent A: DataFusion policy and runtime enforcement.
2. Review A: Policy fail-closed review, timeout/byte/concurrency tests, bootstrap compatibility review.
3. Agent B: Exact object metering and S3-compatible harness.
4. Review B: Object-store authority and S3 evidence review.
5. Agent C: Checkpoint lifecycle, latest recovery, and GC.
6. Review C: Manifest validation, GC safety, SlateDB boundary review.
7. Agent D: Kubernetes Lease and CRD/operator skeleton.
8. Review D: Kubernetes-as-coordinator-only and durable authority review.
9. Agent E: Durable query policy/table/Feldera artifact catalogs.
10. Review E: Fingerprint mismatch, tenant isolation, artifact ABI review.
11. Agent F: Benchmark gates, workflows, readiness CLI.
12. Review F: Release evidence, CI command accuracy, machine-readable output review.

## Required Final Evidence

Velorix is 1.0 production-ready only when all of these are true:

- Working tree clean.
- Branch pushed.
- PR contains only coherent production-readiness commits.
- `docs/architecture/production-readiness-status.md` has no `missing` or `partial` rows.
- `docs/release/1.0-readiness-checklist.md` records successful local, S3-compatible, Kubernetes, benchmark, GC, and readiness CLI evidence.
- `cargo fmt --check` passes.
- `cargo clippy --workspace --all-targets -- -D warnings` passes.
- `cargo test --workspace` passes.
- Env-gated S3-compatible tests pass with recorded endpoint class.
- Env-gated Kubernetes tests pass with recorded cluster class.
- Release benchmark gate passes against S3-compatible baseline.
- Readiness CLI emits `production_ready=true`.

## First Three Commits To Execute Next

1. `docs: add production readiness status matrix`
2. `ci: add production readiness smoke gate`
3. `feat: add query execution policy v1`

These three commits create the visible gate, keep every later implementation honest, and start with the highest-impact remaining runtime risk: untrusted DataFusion resource use.
