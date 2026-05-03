# Velorix

Velorix is an ultra-lightweight, object-storage-first streaming database.

It combines mature third-party data systems, stateless execution, and object
storage as the primary source of truth. All compute nodes are disposable,
scaling horizontally without state migration.

## Designed For

- Cost-efficient large-scale streaming workloads
- Real-time materialized views
- Cloud-native, stateless architectures

## Core Principles

- Object storage is the database
- Compute is stateless and ephemeral
- Prefer mature packages over direct hand-written implementations
- Everything is incremental through DBSP-shaped execution

## Key Features

- Incremental streaming engine shaped by Feldera DBSP semantics
- Object-storage-backed log, checkpoint, and state authority
- SlateDB-backed durable LSM/SST/state substrate as the target direction
- DataFusion-backed SQL/DataFrame planning and Arrow execution where
  batch/query semantics apply
- Foyer-backed hybrid local memory/disk cache as the target cache substrate
- Exactly-once processing through checkpointed manifests

## Architecture Direction

Velorix treats object storage as the durable system of record. Compute workers
read immutable log and state objects, process input as deltas, publish new
objects, and atomically advance checkpoint manifests. A worker can be replaced at
any time because durable progress is captured in object storage, not in local
process state.

Velorix is third-party-first. It should not grow bespoke engines where mature
packages already fit the problem:

- **SlateDB** should own the durable object-storage-first LSM/SST/compaction
  and state substrate. Velorix owns stream progress, exactly-once manifest
  semantics, and runtime recovery orchestration around that substrate.
- **Foyer** should own local memory/disk cache internals. Velorix should wrap it
  only to enforce object-store authority, cache invalidation boundaries, and the
  `ObjectKey` policy.
- **Apache DataFusion** should own SQL, DataFrame, query planning, expression
  handling, and Arrow execution wherever Velorix has batch/query semantics.
- **Feldera DBSP** should own or strongly shape incremental algebra, operators,
  circuit semantics, and long-term execution design. Existing hand-written delta
  and operator code is prototype scaffolding and should migrate behind an
  `IncrementalEngine` adapter or DBSP-backed engine over time.

Velorix-specific code remains valuable at the integration boundaries: object
storage authority, deterministic object keys, checkpoint manifests,
exactly-once publication, stateless recovery, resource and cost policy, and the
glue that composes these packages into one object-storage-first runtime.

The intended system shape is:

1. **Ingest log:** append-only object-backed batches of input deltas.
2. **Incremental engine:** an `IncrementalEngine` boundary hides prototype
   operators today and allows DBSP/Feldera-shaped execution to replace them.
3. **Query execution:** DataFusion handles SQL/DataFrame planning and Arrow
   execution for batch/query surfaces.
4. **State layout:** SlateDB owns the durable LSM/SST/state substrate over object
   storage, while manifests describe Velorix progress and publication state.
5. **Checkpoint protocol:** exactly-once progress is represented by versioned
   manifests that bind input offsets, state objects, and output commits.
6. **Disposable compute:** workers recover by loading the latest manifest and
   warming a Foyer-backed local cache from object storage.

## Goal Plan

The immediate goal is to prove that Velorix can run an end-to-end incremental
streaming workload while keeping object storage as the only durable database
and avoiding custom implementations where SlateDB, Foyer, DataFusion, or
Feldera/DBSP already provide the right substrate.

1. **Define the storage contract:** specify object keys, immutable batch files,
   state files, manifest schema, and atomic publication rules.
2. **Define package ownership:** document where SlateDB, Foyer, DataFusion, and
   Feldera/DBSP own behavior, and keep Velorix code at the integration boundary.
3. **Build the minimal object store layer:** start with a local filesystem
   adapter that behaves like object storage, then add S3-compatible storage.
4. **Implement the ingest log:** persist ordered input delta batches and expose
   replay from a checkpoint.
5. **Add an incremental engine boundary:** keep current map/filter/join/aggregate
   work as prototype scaffolding behind an `IncrementalEngine` adapter, then
   migrate toward DBSP/Feldera-shaped operators.
6. **Persist materialized state through SlateDB:** avoid building a separate
   durable LSM/compaction engine in Velorix.
7. **Add checkpointed manifests:** make recovery deterministic by binding input
   progress, state files, and output commits into one manifest version.
8. **Validate exactly-once behavior:** test crashes before, during, and after
   manifest publication.
9. **Integrate Foyer for hybrid local cache:** use Foyer for memory/disk cache
   internals while preserving object storage as the authority.
10. **Use DataFusion for query surfaces:** route SQL/DataFrame/query planning and
    Arrow execution through DataFusion instead of building a custom planner.
11. **Scale out workers:** partition streams and views so additional disposable
   workers increase throughput without state migration.
12. **Benchmark and harden:** measure cost, recovery time, throughput, and view
    freshness on representative object-storage-backed workloads.

See the detailed implementation plan in
[`docs/superpowers/plans/2026-05-03-velorix-bootstrap.md`](docs/superpowers/plans/2026-05-03-velorix-bootstrap.md).
See also
[`docs/architecture/third-party-first.md`](docs/architecture/third-party-first.md)
for the package ownership note.
