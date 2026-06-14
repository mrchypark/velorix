# Feldera Package-First Runtime Design

Date: 2026-05-27
Status: draft, pending implementation plan
Reviewed Feldera package versions: `dbsp 0.299.0`, `feldera-sqllib 0.299.0`,
`feldera-ir 0.299.0`, `feldera-types 0.299.0`,
`feldera-rest-api 0.299.0`, `feldera-adapterlib 0.299.0`,
`feldera-storage 0.299.0`.

## Decision

Velorix should treat Feldera as the owner of incremental SQL semantics and DBSP
runtime representation. Velorix should not grow a Velorix-owned SQL
parser/planner, SQL shape validator, or hand-written DBSP operator library for
standing views.

The primary Velorix direction is package-first integration:

- Reuse public Feldera Rust package layers where they fit the Velorix runtime
  boundary.
- Keep the Feldera server/pipeline-manager as a comparison and validation
  adapter, not the core runtime architecture.
- Demote release-image generated artifact activation to a static,
  release-bound side path.
- Keep descriptor/executable/checkpoint identity in the first compatibility
  proof. Only release-image artifact activation is demoted; provenance for a
  dynamic compiled descriptor or executable runtime remains required.

## Why The Current Path Is Misaligned

The current Velorix code has useful scaffolding, but it still encodes too much
incremental SQL behavior locally:

- `crates/velorix-core/src/dbsp_view_plan.rs` parses SQL through DataFusion and
  admits only one hand-selected `sum/count` shape.
- `crates/velorix-core/src/dbsp_engine.rs` manually builds a DBSP circuit for
  that shape.
- `crates/velorix-runtime/src/recovery.rs` selects `Prototype` or `Dbsp` as a
  catalog-specific sum/count backend, not as a general Feldera-backed standing
  view runtime.
- `crates/velorix-runtime/src/feldera_registry.rs` can now select a
  release-image package, but that makes view activation build-time static and
  does not match REST-defined product views.

Those pieces are acceptable as spikes and regression fixtures. They should not
become the product path.

## Package Findings

Current public package inspection shows these candidate layers:

| Package | Current role for Velorix | Notes |
| --- | --- | --- |
| `dbsp` | Primary candidate for in-process incremental runtime representation | Public continuous streaming analytics engine. It exposes circuits, streams, Z-sets, batches, traces, input handles, output handles, and persistent trace/storage concepts. It is low-level and currently requires Rust `1.93.1`, so adoption must be gated behind MSRV/toolchain policy. |
| `feldera-sqllib` | Candidate SQL runtime type layer | Provides Feldera SQL runtime types and aliases such as SQL decimal, strings, date/time, intervals, arrays/maps, `Weight`, `WSet`, and `IndexedWSet`. This should replace Velorix-specific JSON-only value conventions for standing-view runtime boundaries where possible. |
| `feldera-ir` | Candidate compiled-program descriptor layer | Provides HIR/LIR/MIR/dataflow graph structures and diff behavior. Use as a representation of compiled Feldera programs, not as a reason to write a Velorix SQL planner. |
| `feldera-types` | Candidate control/status/schema interop layer | Contains program config, program IR, runtime status, checkpoint, coordination, connector config, and API type definitions. Useful for comparison adapter and for borrowing lifecycle concepts. Avoid letting it override Velorix's object-storage authority without an explicit state/storage RFC. |
| `feldera-rest-api` | Validation/comparison adapter | Useful if Velorix runs a Feldera server as an external reference implementation. Not the core runtime dependency. |
| `feldera-adapterlib` | Later connector candidate | Useful after Velorix stabilizes relation catalog, ingest admission, and status contracts. Not part of the first runtime compatibility proof. |
| `feldera-storage` | Research-only until state authority is clarified | Potentially useful for DBSP persistent state, but high risk because Velorix currently wants object storage, SlateDB, and explicit manifests to be durable authority. |

The Feldera package family is moving quickly and current `0.299.0` crates use
Rust edition 2024 and Rust `1.93.1`. A package-first runtime plan must include
an explicit toolchain/MSRV gate instead of silently pulling these packages into
the release path.

`feldera-ir` is not an execution API. It can be logged, compared, or wrapped as
opaque compiler metadata, but Velorix production code must not interpret
`feldera_ir::Op`, `MirNode`, `LirNode`, or Calcite structures to construct DBSP
operators. Doing so would recreate the Feldera planner/lowering stack inside
Velorix.

## New Primary Runtime Boundary

The current `IncrementalEngine` trait is too narrow:

```rust
fn push_changes(epoch, signed_input_changes: &DeltaBatch) -> DeltaBatch
fn materialized_state() -> DeltaBatch
fn checkpoint_state() -> EngineCheckpoint
```

It assumes one generic `DeltaBatch`, one materialized output shape, and
JSON-encoded `key/value/weight` rows. That was enough for a bootstrap sum/count
engine, but it does not describe Feldera programs, which can have multiple
input tables, multiple output views, typed SQL values, and view-specific output
schemas.

The new primary boundary should be a standing-program runtime, not a
single-aggregate engine:

```rust
trait StandingProgramRuntime {
    fn program_identity(&self) -> StandingProgramIdentity;
    fn input_schemas(&self) -> Vec<RelationSchema>;
    fn output_schemas(&self) -> Vec<RelationSchema>;

    fn apply_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, RuntimeError>;

    fn materialized_view(
        &self,
        view: ScopedViewId,
        epoch: CommittedEpoch,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, RuntimeError>;

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, RuntimeError>;
    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, RuntimeError>
    where
        Self: Sized;
}
```

The concrete type names can change during implementation, but the semantics
should not:

- Inputs are relation-scoped, not one untyped batch.
- Outputs are view-scoped, not one generic table.
- `apply_changes` is an atomic epoch transaction over all input relations and
  all output views. It must define behavior for already-applied epochs,
  rejected input, deterministic execution failure, transient resource failure,
  and post-apply publication failure.
- Batches carry typed Arrow/Feldera SQL values, not only JSON payload strings.
- Logical epoch remains a Velorix progress boundary and must stay distinct from
  any DBSP internal timestamp, checkpoint UUID, or Feldera coordination step.
- Checkpoints return a Velorix-owned durable reference or codec envelope, not
  arbitrary local process state.
- Materialized reads are paginated or manifest/cursor-backed at a committed
  epoch. They must not imply full in-memory snapshots for large views.

## Compiler Boundary Options

The first compatibility proof must not assume that the SQL compiler is already
an in-process Rust library. It should evaluate these options in order:

1. Use public Feldera program descriptor crates (`feldera-ir`,
   `feldera-types`) for descriptor validation and schema mapping, while
   obtaining descriptor fixtures from Feldera tooling outside the hot path.
2. Run a Feldera compiler or pipeline-manager sidecar in development and live
   comparison tests to produce compiled descriptors.
3. Only after the descriptor/runtime boundary is proven, decide whether a
   production compiler service belongs in Velorix's control plane.

The product runtime must not call Java/Maven compilation synchronously inside a
REST request. `POST /v1/views` may create a pending compile job and expose
compile status, but activation must wait for a validated compiled descriptor.

The descriptor is not enough. Before implementation, Velorix must identify the
Feldera-owned executable boundary that turns a `StandingViewSpec` into a
runtime:

- statically linked generated Rust,
- dynamically compiled artifact,
- JIT artifact,
- Feldera server/pipeline-manager reference runtime, or
- a stable public Feldera runtime factory if one exists.

Velorix must not lower Feldera IR into DBSP operators itself.

Current implementation note: Velorix now builds deterministic Feldera
`program_code` from catalog-owned `CREATE TABLE` declarations and passes that
program to the configured compiler backend request. The default
`source_kind: standing_view` keeps the original product shortcut by wrapping the
user SQL body as `CREATE MATERIALIZED VIEW "{view_id}" AS ...`.
`source_kind: feldera_program` instead treats the user SQL as Feldera program
body and does not add a view wrapper, so a request can declare multiple
`CREATE VIEW` or `CREATE MATERIALIZED VIEW` outputs. This is the Feldera-owned
SQL semantics boundary. Velorix must not replace it with SQL-shape matching or
Velorix-owned lowering of Feldera plans. Product regression coverage now checks
that Feldera program bodies containing CTEs, `HAVING`, and `UNION ALL` pass to
the pipeline-manager compiler request without being wrapped as a Velorix-owned
single materialized view.

Current state-model note: a compiler backend can now return a resolved
`StandingViewSpec` without an executable artifact. Velorix records that as
`execution_mode: feldera_compile_pending`,
`lifecycle.compile_status: success`, and
`lifecycle.deployment_status: not_deployed`; query remains disabled. If the
backend also returns an executable artifact that maps to a registered runtime
factory, the existing activation path can still promote the view to
`standing_runtime`.

Current Feldera pipeline-manager adapter note: when
`VELORIX_FELDERA_PIPELINE_MANAGER_URL` is configured, `velorix-api` can submit
the deterministic `program_code` to Feldera's `PUT /v0/pipelines/{name}` REST
API, poll `GET /v0/pipelines/{name}`, and map
`program_info.schema.outputs` into Velorix `RelationSchema` output relations.
Compiler-only validation can return at `program_status: "SqlCompiled"` or a
later `"CompilingRust"` status with `program_info` because the output schema is
already available. Pipeline-manager runtime activation still waits for
`program_status: "Success"` because ingest/query requires the Feldera
executable.
When a pipeline-manager URL is configured, the local/default mode is now
`pipeline_manager_local_volatile`: it records that Velorix is exercising the
external Feldera runtime through the product API without claiming production
durability. With `VELORIX_STANDING_RUNTIME_FENCING=required`, the default is
compile/schema validation only, and `pipeline_manager_local_volatile` is
rejected even when `VELORIX_FELDERA_PIPELINE_MANAGER_RUNTIME_PRODUCTION_ENABLE=1`
is set. Set `VELORIX_FELDERA_PIPELINE_MANAGER_RUNTIME_MODE=compiler_only` for
compile/schema validation only; that records `compile_status: success` /
`deployment_status: not_deployed` and leaves queries disabled. Set
`VELORIX_FELDERA_PIPELINE_MANAGER_RUNTIME_MODE=pipeline_manager_external_managed`
when an externally operated Feldera pipeline-manager should remain the runtime
execution plane; with required fencing, that mode also requires
`VELORIX_FELDERA_PIPELINE_MANAGER_RUNTIME_PRODUCTION_ENABLE=1` as an explicit
operator acknowledgement. In local volatile or external-managed runtime mode,
the same backend can return a pipeline-manager runtime deployment and register
a `StandingProgramRuntimeFactory`. Those modes start the Feldera pipeline,
forward relation batches to Feldera ingress as JSON `insert_delete` events,
and read Feldera query output through the product view APIs. Both runtime modes
can activate multi-input Feldera programs and route each input relation batch
to the matching Feldera ingress table. Velorix treats
the catalog weight column as ingest/change metadata in this path: it is excluded
from Feldera `CREATE TABLE` input schemas, stripped from row payloads, mapped
from `1` to `insert`, and mapped from `-1` to `delete` only when the relation
declares `Delete`. It is useful for local boundary validation, but it is not
product-complete until live Feldera durable-state restore, exactly-once behavior
after transaction-started partial HTTP failure, update/upsert semantics, and
live Feldera tests are solved. Multi-input runtime apply now uses Feldera's
`start_transaction` / `commit_transaction` API and waits for
`global_metrics.transaction_status=NoTransaction` before publishing the Velorix
checkpoint, so successful join-style ingest epochs commit their related table
updates through Feldera as one transaction. On a Feldera ingress failure, the
pipeline-manager runtime now marks itself poisoned and rejects subsequent
apply/query/checkpoint attempts; this prevents serving a known-uncertain
external state, but it is not a substitute for durable rebuild/replay evidence
when a transaction was started and Feldera cannot roll it back. Transaction-started
apply failures now also persist a per-epoch/view runtime failure marker before
the API returns the error; future automatic retry checks that marker before
runtime restore/replay and fails closed until an operator-driven rebuild or
repair handles the uncertain external pipeline state. The admin-only
`POST /v1/standing-runtime/ingest-epoch-failures/repair` endpoint removes that
marker only when the request confirms that the external runtime was rebuilt or
cleared, validates the active standing-runtime identity, evicts the in-process
runtime cache, and lets the next retry converge from replayed ingest-log
checkpoints instead of applying the same epoch twice.
Pipeline-manager runtime checkpoints now use
`feldera-pipeline-manager-state-v2`, record the runtime deployment mode, and
reject restore when the checkpoint mode differs from the configured runtime
mode; this prevents an external-managed pipeline checkpoint from being reopened
under local volatile cleanup semantics. The local restore path now
rehydrates the pipeline-manager runtime from the published Velorix checkpoint
payload, preserves logical epoch, idempotency keys, and relation-aware replay
frontiers, and does not replay ingest already covered by that checkpoint. When
multiple input relations share a stream/partition, recovery replays from the
earliest relation frontier and skips only batches covered by the matching
relation/stream/partition checkpoint. Velorix now
preserves multiple Feldera `program_info.schema.outputs` as
`output_relations` even when the admission request did not predeclare
`output_relation_ids`, includes those output ids in
`StandingProgramIdentity.view_ids`, and exposes
`/v1/views/{view_id}/outputs/{output_id}/query` for explicit output reads.
Promoted `/v1/api/*` routes can bind to a concrete output through
`outputRelationId`/`output_relation_id`; multi-output views with `urlPath`
must set that binding. Linked query policies are applied to standing-runtime
reads with the same DataFusion table-scan policy path used by templated view
APIs; non-templated reads push row limits into the runtime page request before
validating final output rows and bytes.
Promoted `GET /v1/api/*` routes backed by the pipeline-manager runtime push
rendered `sql_template` SQL to Feldera `/query` after request validators pass.
Template and caller SQL placeholders are compiled into Feldera's request-local
ad hoc `PREPARE velorix_query AS ...; EXECUTE velorix_query(...)` form so the
query body keeps `$1`, `$2`, ... markers and values appear only as `EXECUTE`
arguments. Parameterized caller/template SQL must be a single statement because
one Feldera `PREPARE` binds one statement; raw caller SQL without Velorix
parameters is passed through unchanged. JSON request parameters are validated
and canonicalized, then rendered as SQL string literals for the current
pipeline-manager `/query` path. This is intentionally narrower than request-time
`VARIANT` bind support: direct probes against the pinned Feldera `/query`
surface reject `parse_json(...)` and `CAST(... AS VARIANT)` in ad hoc SQL, so
Velorix must not advertise generic `VARIANT` parameter binding until Feldera
exposes a stable expression for it. View API admission therefore rejects
`type: "variant"` request fields and `is_variant` template filters with an
explicit `/query` limitation rather than lowering them to JSON string binds.
For direct non-templated output reads,
`max_rows` is pushed to Feldera by wrapping the generated output query in a
bounded subquery with `LIMIT max_rows + 1`, and
`page_token=offset:<row_offset>` adds the corresponding `OFFSET`. Velorix trims
the extra fetched row before returning the API response and emits the next
offset token only when more rows are visible through the bounded query.
Caller/template SQL supports `max_rows` and `page_token=offset:<row_offset>` by
wrapping the query body with Feldera `LIMIT <max_rows + 1>` and optional
`OFFSET <row_offset>` before submission. Parameterized caller/template SQL is
wrapped inside the request-local Feldera `PREPARE`, so values remain only in
`EXECUTE` arguments. Velorix
keeps the older snapshot/DataFusion templated path only for linked
generated/local runtimes, and that path still rejects both `page_token` and
`max_rows`.
The current query response conversion covers scalar Feldera JSON outputs for
booleans, signed/unsigned integers and aliases, floats and aliases, decimal,
text/char, UUID, VARIANT, GEOMETRY as Feldera JSON text, hex binary including
`BINARY VARYING`, date, time, and timestamp/`DATETIME`. It also maps `ARRAY`,
`ROW`/`STRUCT`, and `MAP` JSON results through Arrow nested arrays, preserving
null map values; `INTERVAL` is preserved as Feldera JSON text at the API
boundary. The pipeline-manager path accepts `GEOMETRY`, but the pinned
`feldera-types` descriptor adapter fails closed for it until that package
exposes a geometry schema variant; the checked 0.299.0 dependency and the
0.306.0 crate surface both lack `SqlType::Geometry`. This is still
not a live Feldera compatibility guarantee until the same coverage is exercised
against a running Feldera pipeline-manager.
The volatile runtime must fail closed for duplicate operation capabilities,
delete-only operation sets, nullable or non-Int64 weight columns, or weight
columns in primary keys. Relation catalogs may declare `Update` or `Upsert`
capabilities without blocking activation, but the current runtime still maps
ingest rows through signed Feldera insert/delete events. `Update` and `Upsert`
therefore authorize the internal delete event needed for their before images
without authorizing callers to submit a direct `delete` envelope unless the
relation also declares `Delete`. The REST ingest
contract now accepts explicit operation envelopes for `insert`, `delete`,
`update`, and `upsert`, then normalizes them before durable admission:
`update` becomes delete-before plus insert-after, while `upsert` becomes
delete-before when supplied plus insert-row/after. This keeps Feldera ingress on
the documented `insert_delete` format and keeps Velorix's durable log in one
canonical signed-row representation. Key-only native `Update` or stateful
`Upsert` request semantics still need a separate ingest-envelope contract before
they can be enabled without corrupting Feldera row identity.

## Identity And Provenance Requirements

Every activated standing program must carry a `StandingProgramIdentity` with at
least:

- SQL/program hash,
- input relation catalog hash,
- output schema hash,
- Feldera compiler identity,
- Feldera runtime package names and versions,
- package feature set,
- DBSP/runtime ABI or compatibility identifier,
- checkpoint codec identity,
- native-code policy,
- tenant/program/view identity.

Activation and restore must fail closed on any mismatch unless an explicit
compatibility record exists. This is separate from the release-image generated
artifact path; dynamic package-first runtime still needs identity and
provenance from the first compatibility proof.

## SQL Program Feature Policy

The first product path must not accept every Feldera-compilable program. Initial
REST-defined views should allow only:

- Velorix-owned input tables derived from the relation catalog,
- Feldera SQL views/materialized views,
- deterministic Feldera built-ins covered by schema/type tests.

The first product path should reject:

- external Rust crates or native code,
- preprocessors,
- connector definitions,
- non-deterministic functions,
- any descriptor with `native_code=true`, non-empty external dependencies, or
  connector configuration.

Inline Rust-backed Feldera UDFs and Rust UDAs are admitted only through
`source_kind: feldera_program`, carried as `udf_rust` plus optional empty
`udf_toml`, and verified by live pipeline-manager runtime tests. SQL-only UDFs
can be considered after the initial proof if the compiled descriptor can prove
determinism and dependencies.

## View API Flow

`POST /v1/views` should move toward this flow:

1. Read the Velorix relation catalog from the metadata service.
2. Build and persist a `FelderaCompileRequestV1` in a pending state. Its hash
   must not depend on compiler-inferred output relations when the output
   contract is `Infer`.
3. Submit the SQL/table/view program to the Feldera-backed compiler boundary
   asynchronously.
4. Receive a compiled program descriptor containing input schemas, output
   schemas, program IR/schema, and runtime package/DBSP compatibility metadata.
5. Build the resolved `StandingViewSpec` from compiler-validated output schemas,
   then register an active view or standing program only if the compiled
   descriptor matches the Velorix relation catalog and output schema contract.
6. Start or update the `StandingProgramRuntime`.
7. Route ingest replay and live ingest changes into the standing runtime.
8. Serve view queries from materialized view snapshots or output change streams.

The product API should not validate view SQL by recognizing a few AST shapes.
If Feldera cannot compile the program, the view definition fails. If Feldera can
compile it but the compiled descriptor cannot be mapped to Velorix schemas,
state, or resource policy, the view definition fails closed.

For multi-input views, descriptor/artifact validation must check every input
catalog, not just the first relation. Catalog-derived input schemas must match
exactly by stable relation identity and schema fingerprint; a stale second
relation in a join must fail before activation.

## Feldera Server Adapter Scope

The Feldera server/pipeline-manager path is now a guarded external runtime
adapter as well as a reference:

- Verify that a Velorix `StandingViewSpec` can be expressed as a Feldera SQL
  program.
- In volatile demo mode, activate Feldera-compiled views without requiring
  Velorix to compile or load generated Rust in the API process.
- Compare output changes for the same input changes against any future
  in-process package-first runtime.
- Borrow lifecycle/status vocabulary for compile, runtime, coordination, and
  checkpoint states.
- Use `feldera-rest-api` or Feldera HTTP API in integration tests behind an
  explicit environment flag.

It is not a license for Velorix to adopt Feldera server state as the durable
authority. Velorix still owns relation/view metadata, admission, object-store
evidence, fencing, and the public API contract. Any move that gives Feldera
server storage or lifecycle state durable-authority status needs a separate
product decision.

## Demotions

These existing pieces should be reclassified:

- `DbspSingleKeySumCountEngine`: spike/reference test for DBSP boundary, not
  product runtime.
- `validate_supported_dbsp_view_sql`: bootstrap guard only. New product view
  definitions should rely on Feldera compilation and descriptor validation.
- `RuntimeFelderaArtifactRegistry` direct execution package matching:
  release-bound static artifact path only. It is useful for pinned release
  builds and provenance experiments, not the main dynamic REST view path.
- Feldera artifact hash/provenance readiness gates: keep, but move after
  package/runtime compatibility proof in the roadmap.

## Non-Goals

- No Velorix-owned general SQL parser, planner, or optimizer.
- No Velorix-owned clone of Feldera SQL runtime type semantics.
- No Velorix execution by matching on Feldera IR nodes or Calcite plan nodes.
- No dynamic Rust loading from object storage.
- No synchronous SQL-to-Rust compilation inside request handlers.
- No adoption of Feldera server state as Velorix durable authority without a
  separate storage/control-plane RFC.

## First Compatibility Proof

The first implementation plan should prove only this vertical slice:

1. Compile or obtain a Feldera-compatible program descriptor for one
   `StandingViewSpec`.
2. Identify the executable Feldera-owned runtime mechanism for that descriptor.
3. Map Velorix relation catalog schemas to Feldera SQL runtime schemas without
   JSON-only `key_json/value_json` assumptions.
4. Feed a relation-scoped batch into a Feldera/dbsp-backed runtime adapter.
5. Read one view-scoped output batch with typed columns.
6. Checkpoint and restore the runtime while preserving Velorix logical epoch,
   input frontiers, output frontiers, and program identity.
7. Compare outputs against either a Feldera server reference adapter or an
   existing known-correct fixture.

This proof should deliberately include a SQL shape that the current
`validate_supported_dbsp_view_sql` rejects, such as a filter or join, so the
test proves that Velorix is no longer the SQL planner.

## Testing Gates

Before productizing this design:

- Unit test schema mapping from `VelorixRelationCatalogV1` to Feldera SQL
  runtime types and back, including decimal precision/scale, nullability,
  timestamps/time zones, strings, binary, arrays/maps if supported, weights,
  deletes, and update-as-delete-plus-insert semantics.
- Integration test a Feldera/dbsp-backed `StandingProgramRuntime` through
  `apply_changes`, `materialized_view`, checkpoint, and restore.
- Regression test that the product view path does not call
  `validate_supported_dbsp_view_sql`.
- Architectural source checks are executable through
  `cargo test -p velorix-api --test source_audit`. They assert that the Feldera
  compiler backend path is chosen before linked/generated DBSP fixtures, DBSP
  SQL shape validators remain quarantined to linked fixture helpers, and
  `velorix-api` does not import DataFusion SQL planner, Feldera IR, or DBSP
  execution internals directly.
- Comparison tests against a Feldera server/pipeline-manager adapter behind
  `LIVE_FELDERA=1`. The repeatable local entry point is
  `scripts/run-live-feldera-pipeline-manager.sh`; with
  `LIVE_FELDERA_RUNTIME=1` it runs the runtime SQL-family cases one by one and
  can clear stale Feldera compiled binaries between cases to avoid exhausting a
  smaller local compiler cache. The default local runner stores that cache in
  `target/feldera-compiler-cache.ext4` and mounts it as an ext4 loop filesystem
  inside the Colima VM, so cleanup keeps the backing store under repo `target`
  while Feldera sees Linux filesystem semantics. Loop-mode cleanup runs
  `fstrim` after deleting stale compiler outputs so APFS can reclaim freed
  sparse-image blocks. A Docker named volume remains a legacy override. Current
  coverage includes
  `cargo test -p velorix-api --test live_feldera_pipeline_manager`, which
  submits Velorix-generated standing-view SQL to a configured pipeline-manager
  and validates the compiler-resolved output schema for grouped `sum`/`count`,
  projection plus filter, grouped `min`/`max`/`avg`, PIVOT aggregates,
  UNPIVOT table expressions, `JOIN ... USING`, and a two-table join through
  the same compiler-backed path. It also verifies that
  invalid Feldera SQL and Feldera-documented unsupported SQL families
  (`INTERSECT ALL`, `EXCEPT ALL`, `MATCH_RECOGNIZE`, `NTILE`, and `ROWS`
  window frames) fail closed through the compiler backend instead of activating
  a fallback fixture. Runtime live coverage is gated by
  `LIVE_FELDERA_RUNTIME=1`; those tests start the volatile pipeline-manager
  runtime, ingest signed relation batches through Feldera `insert_delete`, query
  the materialized outputs through the Velorix runtime adapter, checkpoint and
  restore the Velorix runtime adapter, query the output again, and verify
  `max_rows` plus `page_token=offset:<row_offset>` cursor pagination for
  materialized output reads, view-scoped SQL-pushdown reads, and promoted
  `GET /v1/api/*` template reads. The live `scores` fixture uses a
  distinct `event_id` primary key so duplicate `user_id` score events retain
  multiset aggregation semantics in Feldera. Runtime SQL-family coverage now
  includes CTE/`HAVING`/`UNION ALL`, PIVOT, UNPIVOT, and `JOIN ... USING` SQL
  through compile, ingest, and query, plus a raw `source_kind: feldera_program` multi-output
  program through compile, ingest, and explicit output queries. The raw Feldera
  program coverage also includes `CREATE FUNCTION`, Rust-backed
  `CREATE LINEAR AGGREGATE`, `CREATE TYPE` record aliases, and `CREATE INDEX`
  on an output materialized view, keeping those program-level statements on the
  Feldera-owned compiler/runtime path. The Rust extension payload is carried in
  the durable compile request as `udf_rust` plus optional `udf_toml`, participates
  in the compile-request hash, and is forwarded to pipeline-manager `PUT
  /v0/pipelines`. REST
  product-path live smokes cover `POST /v1/relations`, `POST /v1/views` with
  `input_relation_refs`, `POST /v1/view-compile-deploy/run-once`,
  `POST /v1/ingest`, and promoted `GET /v1/api/*` query for a two-relation join
  view against a real pipeline-manager runtime; they also cover scalar,
  typed-array, and JSON query parameters for promoted templates, a raw Feldera program body with
  multiple outputs, promoted API binding to one output, explicit output-query
  routing to another, and a separate no-hint path where
  `output_relation_ids` is omitted and Velorix adopts the compiler-discovered
  output schemas. Runtime coverage also exercises Feldera-native array
  literals, named `ROW` output values, `MAP(SELECT ...)`, `VARIANT`, `UUID`,
  binary literals, `CASE`, and `COALESCE` through the live
  compiler/runtime/query conversion path. The full runtime suite compiles real
  Rust pipelines inside Feldera and needs a large target-backed compiler cache
  image or periodic stale binary cleanup. External Feldera durable-state
  restart evidence remains required before this can be treated as production
  durability evidence. Relation catalogs and REST ingest now cover expanded
  Feldera scalar input columns for signed/unsigned integer widths, `REAL`,
  `DOUBLE`, fixed `CHAR`, fixed and variable binary hex strings, `TIME`,
  `UUID`, and the existing boolean, decimal, text, date, timestamp, and JSON types. They also
  cover nested `ARRAY`, `MAP`, and `ROW` input columns as non-key payload
  columns through relation catalog DDL generation and REST JSON-to-Arrow ingest
  conversion. Live compile-only tests verify that expanded scalar and nested
  input catalogs are accepted as Feldera input DDL. `INTERVAL` remains
  output-preservation-only because the pinned Feldera compiler rejects
  `INTERVAL` as a `CREATE TABLE` column type. Nested primary keys remain
  fail-closed until a stable nested key serialization contract exists.
  Pipeline-manager runtime mode uses a longer default compiler timeout than
  compiler-only/schema mode: schema-only validation defaults to 120 seconds
  because Velorix only needs `program_info.schema`, while runtime
  mode defaults to one hour because a cold Feldera open-source image may compile
  the internal Rust workspace before the first runnable pipeline executable is
  available.
- Resource test for memory, spill, and object-store write amplification before
  enabling production scale-out.
- Toolchain gate that refuses to promote Feldera `0.299.x` packages into the
  release path until the Rust `1.93.1` requirement is accepted or a compatible
  pinned version is chosen.

## Open Questions

- Can Velorix obtain a compiled Feldera program descriptor from public packages
  without running the full Java/Calcite compiler service in the request path?
- Which Feldera-owned mechanism creates an executable in-process runtime from a
  descriptor or SQL program without Velorix lowering IR to DBSP operators?
- Is `feldera-ir::Dataflow` stable enough to use as a durable or semi-durable
  descriptor, or should it be treated as volatile compiler output?
- Which DBSP persistent trace/storage APIs can produce a Velorix-owned
  checkpoint reference without letting Feldera storage become the database
  authority?
- Does Velorix want one standing runtime per view, or one program per relation
  group with multiple output views?
- What is the minimum Rust toolchain policy change acceptable for Feldera
  package adoption?

## Immediate Next Steps

1. Add a small `feldera-package-compat` crate or feature-gated module that
   depends on candidate Feldera packages outside the default release path.
2. Write compile-only and behavior tests for schema/type mapping using
   `feldera-sqllib`.
3. Prove executable runtime creation through a Feldera-owned compiler/runtime
   boundary, not descriptor parsing alone.
4. Define the `StandingProgramRuntime` trait beside, not inside, the current
   `IncrementalEngine` trait so existing bootstrap tests keep working.
5. Move current generated artifact activation docs into a "static release
   artifact path" section.
6. Replace the current view API roadmap so Feldera compilation/descriptor
   validation is the primary path and SQL shape validation is legacy.
