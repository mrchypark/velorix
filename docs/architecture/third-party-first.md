# Third-Party-First Architecture

Velorix should avoid direct hand-written implementations where mature packages
already fit the job. The project should integrate proven substrates and keep
Velorix-specific code focused on object-storage authority, checkpoint manifests,
stateless recovery, resource/cost policy, and package boundaries.

## Package Ownership

| Area | Preferred owner | Velorix-owned boundary |
| --- | --- | --- |
| Durable LSM/SST/state substrate | SlateDB | Object key policy, stream progress, exactly-once manifests, recovery orchestration |
| Local memory/disk cache | Foyer | Object-store authority checks, cache namespace policy, cache-as-non-durable invariant |
| SQL/DataFrame/query planning and Arrow execution | Apache DataFusion | Runtime integration, checkpoint-aware inputs/outputs, cost/resource policy |
| Incremental algebra, operators, and circuit semantics | Feldera DBSP | `IncrementalEngine` adapter, object-backed persistence, moderate-performance cost optimizations |

## Migration Sequence

1. Keep current hand-written delta/operator logic as prototype scaffolding only.
2. Introduce an `IncrementalEngine` boundary so operator internals can be swapped
   without changing storage, manifests, or runtime recovery.
3. Move local cache internals behind Foyer while preserving object storage as the
   only source of durable truth.
4. Move durable state layout and compaction responsibilities to SlateDB, leaving
   Velorix manifests responsible for stream progress and exactly-once commits.
5. Route SQL/DataFrame/query surfaces through DataFusion instead of creating a
   custom planner or expression engine.
6. Use Feldera DBSP as the reference model for incremental operators and circuit
   semantics, while allowing Velorix to optimize more aggressively for lower
   resource cost and accept moderate performance where that improves economics.

## Non-Goals

- Do not build a bespoke query planner or expression engine when DataFusion fits.
- Do not build a separate durable LSM/compaction engine when SlateDB fits.
- Do not build custom memory/disk cache internals when Foyer fits.
- Do not keep expanding prototype delta/operator code as the long-term execution
  engine when Feldera DBSP semantics or adapters can own the model.
