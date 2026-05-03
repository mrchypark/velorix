# Third-Party-First Architecture

Velorix should avoid direct hand-written implementations where mature packages
already fit the job. The project should integrate proven substrates and keep
Velorix-specific code focused on object-storage authority, checkpoint manifests,
stateless recovery, resource/cost policy, and package boundaries.

This note distinguishes current implementation from target direction. The
runtime object cache is currently Foyer-backed after the recent cache work,
SQL/query planning and execution currently use DataFusion for the minimal
`DeltaBatch` query boundary, and SlateDB now backs a minimal experimental
checkpoint-versioned state-store path. Incremental execution now has a
DBSP-shaped `IncrementalEngine` boundary backed by prototype operators. Direct
Feldera DBSP/dbsp integration remains a planned migration direction unless
matching code exists in the repository.

## Package Ownership

| Area | Status | Preferred owner | Velorix-owned boundary |
| --- | --- | --- | --- |
| Durable LSM/SST/state substrate | Current minimal experimental state-store implementation | SlateDB | Object key policy, stream progress, exactly-once manifests, recovery orchestration |
| Runtime object-store fetch-through cache | Current implementation | Foyer | Object-store authority checks, cache namespace policy, cache-as-non-durable invariant |
| SQL/DataFrame/query planning and Arrow execution | Current minimal implementation | Apache DataFusion | Runtime integration, checkpoint-aware inputs/outputs, cost/resource policy |
| Incremental algebra, operators, and circuit semantics | Current adapter boundary; direct DBSP crate integration remains gated | Feldera project semantics and/or Rust `dbsp` crate | `IncrementalEngine` adapter, object-backed persistence, moderate-performance cost optimizations |

## Cache Boundary

Velorix runtime code uses a Foyer wrapper for local memory/disk caching of
object-store fetch-through reads. The cache is never durable authority; object
storage and checkpoint manifests remain authoritative for recovery and progress.

SlateDB may use Foyer internally for its own block or object cache as the
SlateDB integration grows. That cache belongs to SlateDB's state substrate
internals. Velorix should keep the runtime object cache policy separate from any
SlateDB-internal cache policy to avoid duplicate eviction, durability, or
authority rules.

## Query Boundary

Velorix core currently exposes a minimal DataFusion-backed query boundary for
SQL over `DeltaBatch` input. Delta records are converted into an in-memory
Arrow/DataFusion table named `input`, with stable `key_json`, `value_json`, and
`weight` columns. DataFusion owns SQL parsing, query planning, physical
execution, and Arrow `RecordBatch` output.

This is not yet the full query service. Object-backed inputs, checkpoint-aware
query reads, persisted view access, and runtime cost/resource policy remain
future integration work.

## DBSP Adoption Gate

Feldera DBSP can mean the Feldera project and its DBSP model, or the Rust
`dbsp` crate as a direct dependency. Velorix should not treat direct crate
integration as already complete. Adoption is gated on:

- Embedded API fit for a stateless object-storage-first runtime.
- Rust and toolchain compatibility with the Velorix workspace.
- Checkpoint, state, and recovery integration with object-backed manifests.
- Cost and resource impact relative to Velorix's moderate-performance,
  low-cost goal.

Before direct `dbsp` crate integration, Velorix uses DBSP semantics as the
reference model through the current `IncrementalEngine` adapter boundary.

## Migration Sequence

1. Keep current hand-written delta/operator logic as prototype scaffolding only.
2. Route runtime incremental execution through the current `IncrementalEngine`
   boundary so operator internals can be swapped without changing storage,
   manifests, or runtime recovery.
3. Keep runtime object-store fetch-through caching behind the current Foyer
   wrapper while preserving object storage as the only source of durable truth.
4. Continue moving durable state layout and compaction responsibilities to
   SlateDB, leaving Velorix manifests responsible for stream progress and
   exactly-once commits.
5. Keep SQL/query surfaces routed through DataFusion instead of creating a
   custom planner or expression engine. The current implementation covers
   in-memory `DeltaBatch` input; object-backed and checkpoint-aware query
   service integration remains future work.
6. Use Feldera DBSP semantics as the reference model for incremental operators
   and circuit semantics. Consider direct Rust `dbsp` crate integration only
   after the adoption gates are satisfied; otherwise keep an adapter boundary.

## Non-Goals

- Do not build a bespoke query planner or expression engine when DataFusion fits.
- Do not build a separate durable LSM/compaction engine when SlateDB fits.
- Do not build custom memory/disk cache internals when Foyer fits.
- Do not keep expanding prototype delta/operator code as the long-term execution
  engine when Feldera DBSP semantics, adapters, or a gated `dbsp` crate
  integration can own the model.
