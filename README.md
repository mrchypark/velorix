# Velorix

Velorix is an ultra-lightweight, object-storage-first streaming database.

It combines incremental computation inspired by DBSP, stateless execution, and
object storage as the primary source of truth. All compute nodes are disposable,
scaling horizontally without state migration.

## Designed For

- Cost-efficient large-scale streaming workloads
- Real-time materialized views
- Cloud-native, stateless architectures

## Core Principles

- Object storage is the database
- Compute is stateless and ephemeral
- Everything is incremental through delta-based execution

## Key Features

- Incremental streaming engine inspired by DBSP
- Object-storage-backed log and state with an LSM-style layout
- Stateless horizontal scaling with fast recovery
- Hybrid local cache using memory and disk for performance
- Exactly-once processing through checkpointed manifests

## Architecture Direction

Velorix treats object storage as the durable system of record. Compute workers
read immutable log and state objects, process input as deltas, publish new
objects, and atomically advance checkpoint manifests. A worker can be replaced at
any time because durable progress is captured in object storage, not in local
process state.

The intended system shape is:

1. **Ingest log:** append-only object-backed batches of input deltas.
2. **Incremental engine:** DBSP-style operators maintain materialized views by
   applying deltas instead of recomputing full results.
3. **State layout:** LSM-style immutable state files plus manifests describe the
   latest consistent view of each stream and materialized view.
4. **Checkpoint protocol:** exactly-once progress is represented by versioned
   manifests that bind input offsets, state objects, and output commits.
5. **Disposable compute:** workers recover by loading the latest manifest and
   warming local memory and disk cache from object storage.

## Goal Plan

The immediate goal is to prove that Velorix can run an end-to-end incremental
streaming workload while keeping object storage as the only durable database.

1. **Define the storage contract:** specify object keys, immutable batch files,
   state files, manifest schema, and atomic publication rules.
2. **Build the minimal object store layer:** start with a local filesystem
   adapter that behaves like object storage, then add S3-compatible storage.
3. **Implement the ingest log:** persist ordered input delta batches and expose
   replay from a checkpoint.
4. **Implement the first incremental operators:** support map, filter, join, and
   aggregate over signed deltas.
5. **Persist materialized state:** write LSM-style state objects and compact them
   without requiring stateful workers.
6. **Add checkpointed manifests:** make recovery deterministic by binding input
   progress, state files, and output commits into one manifest version.
7. **Validate exactly-once behavior:** test crashes before, during, and after
   manifest publication.
8. **Add hybrid local cache:** cache hot objects in memory and spill warm objects
   to disk without treating cache as durable state.
9. **Scale out workers:** partition streams and views so additional disposable
   workers increase throughput without state migration.
10. **Benchmark and harden:** measure cost, recovery time, throughput, and view
    freshness on representative object-storage-backed workloads.

See the detailed implementation plan in
[`docs/superpowers/plans/2026-05-03-velorix-bootstrap.md`](docs/superpowers/plans/2026-05-03-velorix-bootstrap.md).
