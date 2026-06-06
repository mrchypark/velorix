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

- Rust UDFs and Rust UDAs,
- external Rust crates or native code,
- preprocessors,
- connector definitions,
- non-deterministic functions,
- any descriptor with `native_code=true`, non-empty external dependencies, or
  connector configuration.

SQL-only UDFs can be considered after the initial proof if the compiled
descriptor can prove determinism and dependencies.

## View API Flow

`POST /v1/views` should move toward this flow:

1. Read the Velorix relation catalog from the metadata service.
2. Build and persist `StandingViewSpec` in a pending state.
3. Submit the SQL/table/view program to the Feldera-backed compiler boundary
   asynchronously.
4. Receive a compiled program descriptor containing input schemas, output
   schemas, program IR/schema, and runtime package/DBSP compatibility metadata.
5. Register an active view or standing program only if the compiled descriptor
   matches the Velorix relation catalog and output schema contract.
6. Start or update the `StandingProgramRuntime`.
7. Route ingest replay and live ingest changes into the standing runtime.
8. Serve view queries from materialized view snapshots or output change streams.

The product API should not validate view SQL by recognizing a few AST shapes.
If Feldera cannot compile the program, the view definition fails. If Feldera can
compile it but the compiled descriptor cannot be mapped to Velorix schemas,
state, or resource policy, the view definition fails closed.

## Feldera Server Adapter Scope

The Feldera server/pipeline-manager path remains valuable as a reference:

- Verify that a Velorix `StandingViewSpec` can be expressed as a Feldera SQL
  program.
- Compare output changes for the same input changes against the in-process
  package-first runtime.
- Borrow lifecycle/status vocabulary for compile, runtime, coordination, and
  checkpoint states.
- Use `feldera-rest-api` or Feldera HTTP API in integration tests behind an
  explicit environment flag.

It should not become the default Velorix runtime until there is an explicit
product decision to make Velorix a Feldera control-plane wrapper. That would
change ownership of storage, meta, lifecycle, and scale-out behavior.

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
- Architectural source checks that production standing-view code does not import
  DataFusion SQL planner modules, does not interpret Feldera IR nodes for
  execution, and does not construct DBSP relational operators outside a
  quarantined spike/adapter module.
- Comparison test against a Feldera server/pipeline-manager adapter behind
  `LIVE_FELDERA=1`.
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
