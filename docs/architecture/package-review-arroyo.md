# Arroyo Package Review

Reviewed source:
[ArroyoSystems/arroyo](https://github.com/ArroyoSystems/arroyo) at
`6bb7c8c9f66ef173eab2b34ef45bbfbe9742d7c5` (2026-04-23).

## Fit Summary

Arroyo is a distributed stream processing engine in Rust with SQL streaming
pipelines, stateful windows and joins, checkpointing, connectors, Kubernetes
deployment assets, API crates, a web UI, and controller/worker separation.

Arroyo is valuable to Velorix as a reference for product surface and control
plane design. It should not be embedded as Velorix's execution engine. Its
workspace is a full stream-processing platform, and it patches DataFusion,
Arrow, sqlparser, cornucopia, and related crates. Pulling it into Velorix would
likely fight Velorix's simpler object-storage-first database boundary.

## Strong Advantages for Velorix

### Product Control Plane Shape

Arroyo's crate layout separates API, controller, planner, operator, state,
storage, worker, metrics, RPC, and web UI concerns. Velorix does not need all of
that now, but production readiness requires the same kinds of boundaries.

Velorix follow-up shape:

- `velorix-control`: future catalog, job/view lifecycle, checkpoint status, and
  scheduling decisions.
- `velorix-runtime`: disposable worker execution, recovery, query execution,
  cache, and object-store interaction.
- `velorix-storage`: object keys, manifests, state store, log replay, GC.
- `velorix-core`: algebra, specs, query contracts, and artifact validation.

The existing Velorix package split is broadly compatible with this direction.

### Streaming SQL Planner Lessons

Arroyo uses DataFusion and sqlparser heavily, then rewrites logical plans into
streaming-specific operators for windows, joins, sources, sinks, metadata,
watermarks, and async UDFs.

Velorix should learn from this without copying it:

- Ad hoc SQL should remain DataFusion-owned.
- Standing views should remain Feldera SQL-to-DBSP-owned.
- Velorix should avoid building its own streaming SQL planner unless Feldera
  integration is rejected by evidence.
- If Velorix eventually needs query catalog DDL, it should model source/table
  specs explicitly instead of treating raw SQL as the whole contract.

### Checkpoint Lifecycle

Arroyo has explicit checkpoint metadata states and a checkpoint metadata store
with create, update, finish, compacting, compacted, and cleanup operations.
It also has recovery state transitions that tear down workers/leaders and
reschedule after backoff.

Velorix already has exactly-once checkpoint manifests, but not a product-grade
checkpoint lifecycle. The missing piece is not only data correctness; it is
operability:

- checkpoint status inspection
- failed checkpoint diagnosis
- compaction status
- cleanup retention
- restart and retry policy
- view/job state transitions

Velorix should encode this around object-backed manifests and catalog objects,
not copy Arroyo's database-backed controller store.

Arroyo also highlights a gap Velorix must not hide: worker cleanup and recovery
need fencing. If an old worker resumes after a new worker owns the partition,
the old worker must fail state writes, output commits, and manifest publication.
That means future manifests and state writes should carry
`partition_id`, `owner_id`, and `owner_epoch`. For production, Kubernetes
`Lease` or an equivalent K8s-native lease primitive is the preferred owner for
that epoch; Postgres, raw etcd, OpenRaft, or object-store CAS leases are
fallback/RFC options.

### Connector and Deployment Surface

Arroyo treats connectors and Kubernetes/serverless deployment as primary
product surfaces. This matters because Velorix cannot be a production database
with only local filesystem tests and ad hoc object URLs.

Velorix should add:

- connector specs with stable ids, formats, offsets, credentials references,
  and error status
- pause/resume and end-of-input states where applicable
- health and metrics endpoints
- deployment manifests only after storage authority and recovery contracts are
  stable

### State and Commit Semantics

Arroyo has explicit commit data for operators and subtasks. This is useful as a
reminder that exactly-once output is not only about input replay. For Velorix,
future checkpoint manifests should bind:

- input ranges
- engine state refs
- output object refs
- connector/output commit metadata
- artifact ids for standing-view execution

## Risks and Non-Fit

- Arroyo is not object-storage-first in the Velorix sense. It is a distributed
  stream processing platform with its own controller, metadata database, worker
  orchestration, and checkpoint backend.
- Arroyo's patched DataFusion/Arrow/sqlparser stack is a strong signal that
  streaming SQL planning requires deep engine ownership. Velorix should avoid
  this path while Feldera SQL-to-DBSP remains viable.
- Embedding Arroyo would create overlapping ownership with Feldera for streaming
  SQL and with Velorix for checkpoints/recovery.
- Arroyo's operational assumptions are larger than Velorix's near-term goal:
  web UI, API service, controller, scheduler, worker fleet, connectors, and
  deployment stack.
- Arroyo is still a reference for job-state transitions, not proof that Velorix
  can defer its own fencing, checkpoint status, or connector status semantics.

## Recommendation

Use Arroyo as a production surface checklist, not as a dependency:

1. Add checkpoint lifecycle docs with states beyond "latest valid manifest".
2. Add connector/catalog status docs before implementing more table/query
   surfaces.
3. Add Kubernetes-native partition-owner fencing and stale-worker rejection
   tests before distributed writes.
4. Keep the current DataFusion path narrow and ad hoc.
5. Keep Feldera as the standing-view SQL owner instead of replicating Arroyo's
   custom streaming planner.
6. Revisit Arroyo only through a narrow adoption RFC if Velorix needs a full
   external streaming job runner rather than an embedded object-storage-first
   database runtime.
