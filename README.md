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
- Experimental SlateDB-backed state store for checkpoint-versioned state
  payloads, with Velorix manifests still owning publication and progress
- Minimal DataFusion SQL/query planning and execution over in-memory Arrow
  batches for current `DeltaBatch` query input
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
- **SlateDB** backs the current minimal experimental state-store path for
  checkpoint-versioned state payloads. SlateDB owns the package-backed key/value
  persistence for that path, while Velorix owns stream progress, exactly-once
  manifest semantics, and runtime recovery orchestration around the substrate.
  Broader durable state layout, LSM/SST policy, compaction tuning, and state
  lifecycle design remain future work.
- **Apache DataFusion** currently owns the minimal SQL/query planning and
  execution boundary. Velorix converts `DeltaBatch` records into an in-memory
  Arrow/DataFusion table and returns Arrow `RecordBatch` output. Object-backed
  and checkpoint-aware query service integration remains future work.
- **Feldera DBSP** is the target direction for incremental algebra, operators,
  circuit semantics, and long-term execution design. This includes the Feldera
  project and the Rust `dbsp` crate, but direct crate integration is gated on
  embedded API fit, Rust/toolchain compatibility, checkpoint and state
  integration, and cost/resource impact. Velorix currently uses a DBSP-shaped
  `IncrementalEngine` adapter boundary backed by prototype operators before any
  direct crate adoption.

Foyer and SlateDB have separate cache boundaries. Velorix uses its Foyer wrapper
for runtime object-store fetch-through. SlateDB may use Foyer internally for its
own block or object cache as its state-store integration broadens; Velorix
should keep those policies separate instead of layering duplicate cache
ownership.

Velorix-specific code remains valuable at the integration boundaries: object
storage authority, deterministic object keys, checkpoint manifests,
exactly-once publication, stateless recovery, resource and cost policy, and the
glue that composes these packages into one object-storage-first runtime.

The intended system shape is:

1. **Ingest log:** append-only object-backed batches of input deltas.
2. **Incremental engine:** an `IncrementalEngine` boundary hides prototype
   operators today and allows DBSP/Feldera-shaped execution to replace them
   after the integration gates are cleared.
3. **Query execution:** DataFusion currently plans and executes SQL over an
   in-memory Arrow table built from `DeltaBatch` input and returns Arrow
   `RecordBatch` output. Object-backed and checkpoint-aware query services are
   still future work.
4. **State store:** SlateDB is the current minimal experimental durable
   state-store path over object storage for checkpoint-versioned payloads, while
   manifests describe Velorix progress and publication state. Broader state
   layout, LSM/SST, compaction, and lifecycle policy remain future work.
5. **Checkpoint protocol:** exactly-once progress is represented by versioned
   manifests that bind input offsets, state objects, and output commits. Engine
   checkpoint state objects use a versioned payload that carries the engine
   logical epoch separately from the manifest publication version; legacy raw
   `DeltaBatch` payloads are read with the manifest version as their best-effort
   epoch fallback.
6. **Disposable compute:** workers recover by loading the latest manifest and
   warming the current Foyer-backed runtime object cache from object storage.

## Goal Plan

The immediate goal is to prove that Velorix can run an end-to-end incremental
streaming workload while keeping object storage as the only durable database
and avoiding custom implementations where Foyer already provides the runtime
cache substrate, DataFusion already owns the minimal SQL/query boundary, SlateDB
already owns the minimal experimental checkpoint-versioned state-store path, or
the current DBSP-shaped adapter boundary keeps prototype operators replaceable.
Future direct Feldera DBSP/dbsp integration and broader SlateDB layout,
compaction, and lifecycle work remain gated follow-on work.

1. **Define the storage contract:** specify object keys, immutable batch files,
   state files, manifest schema, and atomic publication rules.
2. **Define package ownership:** document where Foyer owns current cache
   behavior, where DataFusion currently owns minimal SQL/query behavior, where
   SlateDB currently owns the minimal experimental state-store path, where the
   current DBSP-shaped adapter boundary contains prototype operators, and which
   direct Feldera DBSP/dbsp and broader SlateDB ownership remains future gated
   work. Keep Velorix code at the integration boundary.
3. **Build the minimal object store layer:** start with a local filesystem
   adapter that behaves like object storage, then add S3-compatible storage.
4. **Implement the ingest log:** persist ordered input delta batches and expose
   replay from a checkpoint.
5. **Keep the incremental engine boundary current:** keep current
   map/filter/join/aggregate work as prototype scaffolding behind the
   `IncrementalEngine` adapter, then migrate toward DBSP/Feldera-shaped
   operators after direct integration gates are satisfied.
6. **Extend the SlateDB state-store boundary:** keep the current minimal
   checkpoint-versioned path package-backed, and defer broader durable state
   layout, LSM/SST, compaction, and lifecycle work until the integration shape is
   clearer.
7. **Add checkpointed manifests:** make recovery deterministic by binding input
   progress, state files, and output commits into one manifest version.
8. **Validate exactly-once behavior:** test crashes before, during, and after
   manifest publication.
9. **Keep the Foyer-backed hybrid local cache boundary current:** use Foyer for
   runtime object-cache memory/disk internals while preserving object storage as
   the authority.
10. **Use DataFusion for query surfaces:** keep SQL/query planning and Arrow
    execution routed through DataFusion. The current implementation runs SQL
    over an in-memory `MemTable` built from `DeltaBatch` input; future work is
    to make that service object-backed and checkpoint-aware.
11. **Scale out workers:** partition streams and views so additional disposable
   workers increase throughput without state migration.
12. **Benchmark and harden:** measure cost, recovery time, throughput, and view
    freshness on representative object-storage-backed workloads.

See the detailed implementation plan in
[`docs/superpowers/plans/2026-05-03-velorix-bootstrap.md`](docs/superpowers/plans/2026-05-03-velorix-bootstrap.md).
See also
[`docs/architecture/third-party-first.md`](docs/architecture/third-party-first.md)
for the package ownership note.
