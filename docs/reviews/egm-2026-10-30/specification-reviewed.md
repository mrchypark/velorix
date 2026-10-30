# Velorix product specification — current implementation draft

**Implementation baseline:** `c6222abd054858109c90a63a061db698fa9fc9e8` (2026-10-30).  
**Status:** Source-backed draft for product and architecture review. This is not a release qualification, deployment guide, or completed EGM review. Source presence and cited tests are evidence pointers; this reconciliation did not execute tests.

This document separates the implemented public contract from experimental/internal capabilities, historical proposals, and known gaps. “Supported” below means the implementation path and admission boundary exist at this baseline; it does not claim production-scale qualification.

**Status key:** **Supported** means the source path and admission boundary exist. **Default** means the capability is reachable under the configured public default policy; it does not mean eager materialization. **Experimental** means public admission requires a feature gate. **Internal** means no equivalent public route is established. **Planned** means a design proposal, not current behavior. **Unverified** means the required deployment or qualification evidence is absent.

## Purpose, goals, and non-goals

**Business motivation:** not specified in the authoritative requirements available for this draft. **Product purpose:** provide a jarless runtime for registering schema-bound relations, admitting supported views, incrementally maintaining materialized outputs, serving those outputs, and recovering progress from durable metadata and storage.

Goals are to validate relation schemas and ingested rows, admit only supported SQL families into the native runtime, update materialized outputs through the configured eager or demand mode, serve reads from published output, and recover from durable metadata/checkpoints.

Non-goals are user-supplied view code execution, external compilation/package-loading or image-based view execution, arbitrary SQL admission, and source-recomputation fallback for unsupported views.

## Product shape

Velorix is a jarless materialized-view database/runtime. Callers register relations with explicit schemas, ingest schema-bound records, declare supported views, and query published materialized output. View definitions are lowered to Velorix typed plans and executed by the native incremental runtime. Query-time DataFusion serves bounded SQL over published output; it does not create views or recompute them from source relations. Unsupported view SQL fails admission with an error. There is no user compiler, runtime build/deploy, package-loading, or image-based view execution path.

Configured API startup defaults authoritative relation ingest on. New views in that mode are demand-managed. `StandingRuntime` names the execution mode and does not promise eager materialization. Explicit Legacy mode retains eligible eager behavior. Existing checkpoints without demand metadata retain their eager behavior when authoritative ingest is later enabled; enabling the mode does not migrate old data/checkpoints.

## Deployment roles and images

The `velorix-api` binary supports `all`, `api`, and `worker` service roles. The `api` and `worker` Docker targets package that same binary with different default role settings; they are not separate executables. Split roles require API bearer authentication and shared Meta connectivity. The API role also requires a configured list of worker endpoints. Workers accept authenticated internal compute RPCs and reject public ingest. `/readyz` reports service/ingest mode.

The all-in-one image packages API, Meta, and ingest-writer binaries. Its default `all` entrypoint launches Meta and API; it does not automatically launch ingest-writer. Separate image build definitions do not prove that images were built, published, or deployed. See [Dockerfile.api](../Dockerfile.api), [Dockerfile.all-in-one](../Dockerfile.all-in-one), [entrypoint](../scripts/velorix-all-in-one-entrypoint.sh), and [worker role validation](../crates/velorix-api/src/worker_role.rs).

## Relations, schemas, and ingest

A relation has a registered versioned schema, key/weight semantics, schema fingerprint, DataFusion registration, and declared allowed operations. Public row input is JSON; catalog-bound preparation validates the schema, normalizes allowed operation envelopes into signed rows, constructs typed Arrow batches, and persists an Arrow IPC ingest envelope. The envelope binds relation identity/version, schema fingerprint, stream, partition, range, digest, optional event-time watermark, and payload. See [relation schema](../crates/velorix-core/src/relation.rs), [operation normalization](../crates/velorix-api/src/lib.rs), [ingest preparation](../crates/velorix-api/src/ingest_epoch.rs), and [envelope codec](../crates/velorix-storage/src/ingest_envelope.rs).

### Input deletion requirement and implementation gap

The product requirement carried into this specification excludes public input DELETE operations. It retains retractions/replacements produced inside derived views when an accepted source change alters an aggregate or other result. This is a product requirement, not a description of all current implementation capability.

At this baseline, public ingest code can normalize `insert`, `delete`, `update`, and `upsert` operation envelopes when the relation catalog allows each operation. Delete maps to a negative weight; update retracts the old row and inserts the new row; upsert may retract `before` and inserts `row`/`after`. The DELETE capability conflicts with the product requirement. Catalog-gated UPDATE/UPSERT are as-built capabilities; this draft neither approves nor prohibits their public product policy. Signed input weights and derived output retractions/replacements remain distinct: derived retractions are required when accepted source changes alter a view result. See `normalize_ingest_operation_envelopes` and `require_relation_operation` in [lib.rs](../crates/velorix-api/src/lib.rs), and [derived output coverage](../crates/velorix-api/src/generic_derived_backfill_tests.rs).

### Admission, identity, deduplication, and acknowledgement

Configured API startup defaults `VELORIX_API_AUTHORITATIVE_RELATION_INGEST` to true and requires a stable, nonempty `VELORIX_RELATION_INGEST_OWNER_ID` plus a compatible Meta authority. Set the flag to `0` only to explicitly select Legacy behavior. Low-level `ApiState` constructors start in Legacy until configured; they are not the configured server startup path. See [API config parsing](../crates/velorix-api/src/lib.rs) and [generic pipeline modes](development/generic-view-pipeline.md).

Authoritative ingestion uses relation/partition ownership, reserves a range, stages the immutable input, then publishes the relation source reference through Meta. The admitted range is bound to a digest: identical retries are idempotent; conflicting digests and overlapping active ranges fail. Authoritative mode accepts one relation batch per request; atomic multi-batch publication is unsupported. Do not infer deployed multiwriter safety from local coordinator or metadata source code. Kubernetes Lease-to-durable-fence integration remains unverified.

The ingest response currently reports the fixed wire label `ack_mode: materialized`; callers cannot negotiate an alternate ACK mode. This label alone does not mean every affected demand-managed view has materialized. In configured authoritative mode, ingestion publishes source input and skips views identified as demand-managed; existing eager checkpoints may still be advanced. The API-only split role returns `materialization.status: deferred` when it delegates compute. The local pass can report `materialization.status: completed` after it has skipped demand-managed views. Neither that status label nor the ACK label is a per-view freshness certificate; use the affected view's checkpoint/progress coverage. Legacy/eager execution applies eligible views and drains published dependencies before its materialization pass returns. Source pointers: [ingest epoch](../crates/velorix-api/src/ingest_epoch.rs), [demand classification](../crates/velorix-api/src/ingest_epoch.rs), and [split role behavior](../crates/velorix-api/src/worker_role.rs).

## Views, demand work, and dependencies

`POST /v1/views` resolves registered relation or published-view schemas, validates public feature policy, lowers supported SQL to a typed logical plan, derives output schema, and admits a native runtime binding. A declaration in authoritative mode is deferred even for empty input and carries a `backfill_required` lifecycle reason. This is progress/lifecycle metadata: a query can advance materialization without an explicit backfill request. Declaration does not replay all historical input automatically.

For new authoritative views, ingest persists source updates without eagerly applying demand-managed views. A query or explicit backfill advances bounded work. Startup restores durable progress but does not automatically replay all history or drain the entire active dependency graph. Requests can materialize ancestor dependencies as required; arbitrary DAG shapes, full closure performance, and production-scale scheduling are not qualified here.

The default demand budget is **one batch unit, 4,000 rows, 64 MiB, and 1,000 ms** per step, as defined by `DemandBudget::default` in [demand_materialization.rs](../crates/velorix-api/src/demand_materialization.rs). These are cooperative step limits, not hard RSS or CPU caps. Explicit backfill accepts request budgets and scope; a batch-limit compatibility alias exists. Pause/resume updates persisted control state and does not start a background worker. Reverse order and other controls require authoritative demand mode.

Demand responses use `202 Accepted` while required work remains, the view is paused, or work yields to foreground demand. `200 OK` means the requested step or scope is complete at its declared cut. A scoped 200 does not prove the whole view or unrelated scopes are complete. Full-view completion requires full-closure progress. These statuses do not imply that a source ingest has already appeared in every demand-managed output. See [query demand handling](../crates/velorix-api/src/query_serving.rs), [demand progress](../crates/velorix-api/src/demand_materialization.rs), and [focused API cases](../crates/velorix-api/src/generic_demand_materialization_tests.rs).

View-on-view edges bind immutable producer generations and durable cursors. Published deltas carry signed changes; consumers advance persisted cursors. A focused three-level filter → aggregate → Top-K test covers insert/retract/restart for that shape. It does not establish arbitrary SQL composition, all DAG interleavings, or bounded whole-graph work. Bootstrap admission records a source cut and generation; promotion checks activation cut and checkpoint coverage. Cross-relation ingest is not an atomic transaction. See [dependency handling](../crates/velorix-api/src/view_dependencies.rs), [DAG tests](../crates/velorix-api/src/view_dependency_dag_tests.rs), and `rest_three_level_filter_aggregate_topk_chain_exact` in [API tests](../crates/velorix-api/src/tests.rs).

Event-time windows support bounded correction semantics in their admitted shapes. Late rows may retract/replace published aggregate output while retained/cold state permits correction; this is not an output TTL or a universal late-data rule. Watermarks are explicit and monotonic under the defined partition/global checks. No automatic wall-clock watermark advance is promised. See [event-time runtime](../crates/velorix-runtime/src/materialized_view_runtime/event_time_window.rs) and [supported SQL constraints](architecture/supported-sql.md).

## SQL surfaces

There are three distinct SQL surfaces:

1. **Public view admission:** Velorix validators admit specific SQL families into typed plans and the native runtime. Unsupported shapes fail closed.
2. **Public read-time SQL:** query SQL runs against a selected published view output under query policy. It is not a general relation-source scan or view creation path.
3. **Internal/development table scans:** persisted-table and object-backed DataFusion paths have separate registration and policy contracts; they are not implied by public policy-catalog endpoints.

The current public family matrix and exact grammar are maintained in [supported-sql.md](architecture/supported-sql.md). The matrix covers filters/projections, group-by with sum/count/min/max/avg, bounded joins and subqueries, Top-K, event-time windows, and listed narrow Phase 8 families. Advanced admission is policy-gated separately: event-time windows are enabled by default; analytic `OVER` families are experimental by default. `VELORIX_EXPERIMENTAL_ADVANCED_VIEW_FEATURES=true` enables both policy families, but does not widen their validators to arbitrary SQL. Runtime presence, parser acceptance, and read-time DataFusion acceptance are not public view admission.

Known narrow shapes include bounded ROWS navigation frames, grouped Int64 percentile/median forms, CROSS JOIN, strict interval-overlap INNER JOIN, temporal/as-of joins, compiled-in deterministic scalar UDFs, and positive recursive fixpoints. Each has exclusions; see [Phase 8 designs](architecture/advanced-capabilities-design-v1.md) and the matrix. User-defined code, general recursive SQL, unrestricted windows, arbitrary subqueries, and unsupported joins are not implied.

## Query output, formats, limits, and caches

Public relation input is JSON. Durable source ingest uses the versioned Arrow IPC envelope. Runtime checkpoint bodies use typed Arrow IPC with ZSTD compression; small manifests, catalogs, and control metadata use JSON. Query responses default to Arrow IPC; `Accept: application/json` selects JSON rows. The response format and query path are separate from view execution. See [query response encoding](../crates/velorix-api/src/query_response.rs), [checkpoint records](../crates/velorix-api/src/checkpoint_record.rs), and [envelope codec](../crates/velorix-storage/src/ingest_envelope.rs).

Defaults below are recorded only where the current source defines them:

| Boundary | Source-defined default | Qualification |
| --- | --- | --- |
| Ingest rows per request | 10,000 | API request guard; configured elsewhere through API config. |
| Demand step | 1 batch, 4,000 rows, 64 MiB, 1,000 ms | Cooperative between atomic steps; does not cap process CPU/RSS. |
| Query SQL | 1 MiB | `QueryExecutionPolicyV1::default`. |
| Query planning/execution | 5,000 ms / 30,000 ms | Path/policy scoped timeouts. |
| Query output | 10,000 rows / 64 MiB | Response-output bounds, not scan byte bounds. |
| Query scan | 100 files / 1 GiB / 1,000 object requests | Applies on configured table-scan/query paths; not a universal ingestion or runtime limit. |
| DataFusion memory/spill | 512 MiB / 1 GiB | Policy/session configuration, not a verified hard process RSS cap or current-version enforcement qualification. |
| Native standing-runtime checkpoint state | 8 MiB | Default encoded state payload budget; separate from decoded expansion and RSS. |
| Public Top-K | 1,000 rows | Admission limit for supported Top-K forms. |

The table is not an exhaustive limit inventory. Request body, output-page expansion, query concurrency, cache capacities, deadlines, and operator-specific limits vary by path/configuration. `max_concurrent_queries` has no default cap in `QueryExecutionPolicyV1`; production table scan policy requires explicit scan/output/time bounds. No listed limit is a global hard RSS guarantee. See [resource policy](../crates/velorix-core/src/resource_policy.rs), [DataFusion session wiring](../crates/velorix-runtime/src/runtime_contract.rs), and [API state limits](../crates/velorix-api/src/lib.rs).

Materialized output is stored in Arrow IPC pages with an index and optional per-page null/min/max statistics. Supported predicates may prune a page only when stats prove it cannot match; residual SQL evaluates the complete selected weighted input. This is not Parquet-style column projection. Foyer caches are disposable: misses/errors use authoritative reads; cache hits do not bypass validation or budget checks. Cache capacity is not process memory accounting. See [materialized output pages](../crates/velorix-api/src/materialized_output_pages.rs) and [read cache](../crates/velorix-api/src/materialized_read_cache.rs).

## Durable authorities and recovery

The protocols use different records and publication authorities; the table summarizes each separately.

| Protocol | Body/reference | Authority and meaning |
| --- | --- | --- |
| Storage checkpoint | JSON `CheckpointManifest` and referenced state/output objects | Published validated manifest and payload refs are storage checkpoint authority. Latest-candidate marker is an advisory acceleration; valid candidates are checked and listing fallback applies when the marker is stale/corrupt. |
| API standing-runtime checkpoint | Immutable Arrow checkpoint body/object plus `StandingRuntimeCheckpointPointer` | Meta CAS pointer selects the published head and binds epoch/hash/coverage/lineage. Object existence alone is not publication authority. |
| Rhiza KV metadata snapshot | Root-CAS JSON manifest plus immutable digest-bound pages | Rhiza KV root selects a serialized Meta state snapshot; this is not the runtime checkpoint body and the current implementation serializes state into paged snapshots. Do not assume per-key/transaction semantics beyond source contract. |
| Rhiza Local archive | Rhiza Local database archive and recovery protocol | Separate single-writer archive/recovery path; not a consensus cluster or the API checkpoint pointer. |

Object storage holds immutable source/checkpoint bodies; Meta stores mutable catalogs, ingest reservation/publication/source-cut state, active-view lifecycle/generations, runtime owners and checkpoint pointers, and partition authority/pointers. Backend-specific snapshots may carry metadata state, but that does not make them interchangeable with output/checkpoint payloads. See [storage manifest](../crates/velorix-storage/src/manifest.rs), [checkpoint index](../crates/velorix-storage/src/checkpoint_index.rs), [API pointer publication](../crates/velorix-api/src/checkpoint_publication.rs), [Meta RPC contract](../crates/velorix-meta/proto/velorix/meta/v1/meta.proto), [Rhiza KV snapshot](../crates/velorix-meta/src/rhiza_kv_snapshot.rs), and [Rhiza Local snapshot](../crates/velorix-meta/src/rhiza_snapshot.rs).

Meta requires explicit `VELORIX_META_MODE` and `VELORIX_META_BACKEND`. Development supports memory, OSS, and feature-enabled Rhiza KV. Production rejects memory and validates authentication, transport, and backend durability configuration. Production Rhiza KV requires three voters and object-store durability `before-ack`; Rhiza Local is single-writer archive/recovery. Rhiza's current source reports `bounded_wall_clock_failover=false` and `production_multi_writer_safe=false`. These facts prevent us from claiming bounded wall-clock failover or production multiwriter safety from the current source alone. See [Meta startup validation](../crates/velorix-meta/src/main.rs) and [Rhiza capability report](../crates/velorix-meta/src/rhiza_meta.rs).

Recovery restores from authoritative pointers and immutable bodies, validates identities/coverage, and restores demand progress. Startup does not eagerly replay the full active DAG. GC safety, object-provider parity, deployment fencing, live recovery, and release qualification need their own artifact-backed gates. The opt-in Versity Gateway compatibility harness is not the representative S3 release benchmark.

## Capability status and unresolved gaps

- **Default — event-time:** supported `TUMBLE`, `HOP`, and `SESSION` shapes are policy-enabled by default, subject to the validators in [supported SQL](architecture/supported-sql.md).
- **Experimental — analytic `OVER`:** admission requires `VELORIX_EXPERIMENTAL_ADVANCED_VIEW_FEATURES=true` and remains limited to validated shapes. The flag also enables event-time policy, which is already enabled by default.
- **Internal/development:** persisted-table object scans and DataFusion tooling have no equivalent public source-scan route.
- **Planned:** broader SQL families and their semantic/resource contracts remain proposals unless the admission matrix identifies an implemented subset.
- **Unverified qualification:** production deployment/failover, Kubernetes fencing integration, provider parity, release benchmark/GC, arbitrary whole-DAG behavior, and DataFusion memory/spill enforcement remain open; see [release readiness](architecture/production-readiness-status.md).

The following index keeps unresolved requirements visible; their scope and evidence are described above or in the linked references.

1. **Input DELETE:** the product requirement excludes public input DELETE, while current catalog-gated ingest accepts it when allowed. UPDATE/UPSERT policy remains unselected; see [input semantics](#input-deletion-requirement-and-implementation-gap).
2. **SQL and dependency breadth:** only matrix-listed SQL shapes are supported; arbitrary SQL and general DAG qualification are not implied. See [supported SQL](architecture/supported-sql.md) and [dependency semantics](development/generic-view-pipeline.md).
3. **Demand and deployment:** work is query/backfill driven, and production fencing, provider parity, GC, recovery, and release benchmarks require separate evidence. See [demand behavior](#views-demand-work-and-dependencies) and [release readiness](architecture/production-readiness-status.md).
4. **Resource and test qualification:** policy limits are not global hard RSS guarantees, and this reconciliation did not run cited tests. The linked CI baseline is not release qualification; see [resource policy](architecture/datafusion-resource-policy-v1.md) and [readiness status](architecture/production-readiness-status.md).

## Open owner decisions for the next design

This draft records current implementation behavior; future architecture choices remain open and are not selected here:

- What public-input policy should apply to catalog-gated UPDATE/UPSERT? The recorded product requirement excludes DELETE; this draft does not decide UPDATE/UPSERT policy.
- Which DAG breadth/depth and SQL family/type combinations should be public, experimental, or unsupported?
- What release evidence will establish production fencing, provider behavior, S3 benchmark/GC, recovery, and rollback?
- Which limits should be public product guarantees versus configurable implementation bounds, especially memory/spill, cache capacity, and demand fairness?
- What migration compatibility contract applies when switching existing installations from Legacy/eager to authoritative demand mode?

Alternatives analysis for future architecture is out of scope for this as-built draft. A separate design decision artifact must compare alternatives and record reasons for rejecting them before any future architecture choice is selected. This draft does not reconstruct historical rationale or choose an implementation plan.

## Source and test entry points

- [API routes and configuration](../crates/velorix-api/src/lib.rs)
- [View admission](../crates/velorix-api/src/view_admission.rs)
- [Supported SQL matrix](architecture/supported-sql.md)
- [Demand materialization](../crates/velorix-api/src/demand_materialization.rs)
- [Three-level API test](../crates/velorix-api/src/tests.rs)
- [Ingest envelope tests](../crates/velorix-storage/tests/ingest_envelope.rs)
- [Checkpoint publication tests](../crates/velorix-storage/tests/checkpoint_publish.rs)
- [S3 compatibility harness](../crates/velorix-storage/tests/s3_compat.rs)
- [Release readiness gate](../.github/workflows/release-gate.yml)
