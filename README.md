# Velorix

Velorix is an ultra-lightweight, object-storage-first streaming database.

It is being built around mature third-party data systems, stateless execution,
and object storage as the primary source of truth. All compute nodes should be
disposable, scaling horizontally without state migration as the runtime matures.

## Designed For

- Cost-efficient large-scale streaming workloads
- Real-time materialized views
- Cloud-native, stateless architectures

## Core Principles

- Object storage is the database
- Compute is stateless and ephemeral
- Prefer mature packages over direct hand-written implementations
- Keep execution incremental and DBSP-shaped as the engine evolves

## Key Features

- Incremental streaming engine currently backed by Velorix prototype operators
  behind an `IncrementalEngine` boundary
- Object-storage-backed log, checkpoint, and state authority
- Foyer-backed hybrid local memory/disk runtime object cache for object-store
  fetch-through
- Planned SlateDB durable LSM/SST/state substrate where it fits
- Planned DataFusion SQL/DataFrame planning and Arrow execution where
  batch/query semantics apply
- Planned Feldera DBSP/dbsp-shaped execution path after integration gates are
  satisfied
- Exactly-once processing through checkpointed manifests

## Architecture Direction

Velorix treats object storage as the durable system of record. Compute workers
read immutable log and state objects, process input as deltas, publish new
objects, and atomically advance checkpoint manifests. A worker can be replaced at
any time because durable progress is captured in object storage, not in local
process state.

Velorix is third-party-first. It should not grow bespoke engines where mature
packages already fit the problem:

- **Foyer** currently owns the runtime local memory/disk object cache through a
  Velorix wrapper that enforces object-store authority, cache invalidation
  boundaries, and the `ObjectKey` policy.
- **SlateDB** is the planned owner for durable object-storage-first
  LSM/SST/compaction and state substrate behavior where the integration fits.
  Velorix owns stream progress, exactly-once manifest semantics, and runtime
  recovery orchestration around that substrate.
- **Apache DataFusion** is the planned owner for SQL, DataFrame, query planning,
  expression handling, and Arrow execution wherever Velorix has batch/query
  semantics.
- **Feldera DBSP** is the target direction for incremental algebra, operators,
  circuit semantics, and long-term execution design. This includes the Feldera
  project and the Rust `dbsp` crate, but direct crate integration is gated on
  embedded API fit, Rust/toolchain compatibility, checkpoint and state
  integration, and cost/resource impact. Velorix may first use DBSP semantics as
  a reference model or keep an adapter boundary before adopting the crate
  directly.

Foyer and SlateDB have separate cache boundaries. Velorix uses its Foyer wrapper
for runtime object-store fetch-through. SlateDB may use Foyer internally for its
own block or object cache after SlateDB is integrated; Velorix should keep those
policies separate instead of layering duplicate cache ownership.

Velorix-specific code remains valuable at the integration boundaries: object
storage authority, deterministic object keys, checkpoint manifests,
exactly-once publication, stateless recovery, resource and cost policy, and the
glue that composes these packages into one object-storage-first runtime.

The intended system shape is:

1. **Ingest log:** append-only object-backed batches of input deltas.
2. **Incremental engine:** an `IncrementalEngine` boundary hides prototype
   operators today and allows DBSP/Feldera-shaped execution to replace them
   after the integration gates are cleared.
3. **Query execution:** DataFusion is the planned path for SQL/DataFrame
   planning and Arrow execution for batch/query surfaces.
4. **State layout:** SlateDB is the planned durable LSM/SST/state substrate over
   object storage, while manifests describe Velorix progress and publication
   state.
5. **Checkpoint protocol:** exactly-once progress is represented by versioned
   manifests that bind input offsets, state objects, and output commits.
6. **Disposable compute:** workers recover by loading the latest manifest and
   warming the current Foyer-backed runtime object cache from object storage.

## Goal Plan

The immediate goal is to prove that Velorix can run an end-to-end incremental
streaming workload while keeping object storage as the only durable database
and avoiding custom implementations where Foyer already provides the runtime
cache substrate or where SlateDB, DataFusion, and Feldera/DBSP are planned to
provide the right substrate.

1. **Define the storage contract:** specify object keys, immutable batch files,
   state files, manifest schema, and atomic publication rules.
2. **Define package ownership:** document where Foyer owns current cache
   behavior, where SlateDB, DataFusion, and Feldera/DBSP are planned to own
   behavior, and keep Velorix code at the integration boundary.
3. **Build the minimal object store layer:** start with a local filesystem
   adapter that behaves like object storage, then add S3-compatible storage.
4. **Implement the ingest log:** persist ordered input delta batches and expose
   replay from a checkpoint.
5. **Add an incremental engine boundary:** keep current map/filter/join/aggregate
   work as prototype scaffolding behind an `IncrementalEngine` adapter, then
   migrate toward DBSP/Feldera-shaped operators.
6. **Plan materialized state persistence through SlateDB:** avoid building a
   separate durable LSM/compaction engine in Velorix.
7. **Add checkpointed manifests:** make recovery deterministic by binding input
   progress, state files, and output commits into one manifest version.
8. **Validate exactly-once behavior:** test crashes before, during, and after
   manifest publication.
9. **Keep the Foyer-backed hybrid local cache boundary current:** use Foyer for
   runtime object-cache memory/disk internals while preserving object storage as
   the authority.
10. **Use DataFusion for query surfaces:** route SQL/DataFrame/query planning and
    Arrow execution through DataFusion instead of building a custom planner once
    that integration work exists.
11. **Scale out workers:** partition streams and views so additional disposable
   workers increase throughput without state migration.
12. **Benchmark and harden:** measure cost, recovery time, throughput, and view
    freshness on representative object-storage-backed workloads.

See the detailed implementation plan in
[`docs/superpowers/plans/2026-05-03-velorix-bootstrap.md`](docs/superpowers/plans/2026-05-03-velorix-bootstrap.md).
See also
[`docs/architecture/third-party-first.md`](docs/architecture/third-party-first.md)
for the package ownership note.
