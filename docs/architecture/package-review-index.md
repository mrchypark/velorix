# Package Review Index

Reviewed on 2026-05-04 for the Velorix production-readiness track.

This document links the package review notes that govern Velorix's
third-party-first direction. The reviews are evidence for package ownership and
integration sequencing; they are not claims that Velorix is production-ready
today.

## Reviewed Sources

| Source | Reviewed revision | Role in this review |
| --- | --- | --- |
| [OpenData](https://github.com/opendata-oss/opendata) | `0961492a` on 2026-04-30 | Object-native database fleet patterns, SlateDB-centered storage, stateless buffer, write coordination, common operational tooling |
| [Arroyo](https://github.com/ArroyoSystems/arroyo) | `6bb7c8c9` on 2026-04-23 | Distributed streaming SQL control plane, planner/operator/state organization, checkpoint lifecycle, connectors, Kubernetes/serverless operations |
| [Feldera](https://github.com/feldera/feldera) | `7ae331bf` on 2026-05-03 | SQL-to-DBSP compilation, DBSP semantics, pipeline artifact lifecycle, ad hoc query split, connector/checkpoint test strategy |
| Current Velorix workspace | `feature/velorix-bootstrap` | Existing object-store authority, checkpoint manifests, Foyer cache, SlateDB state-store v0, DataFusion query surfaces, Feldera artifact contract |

## Bottom Line

Velorix should stay third-party-first, but not all reviewed systems should
become dependencies.

| Candidate | Recommendation | Reason |
| --- | --- | --- |
| Feldera SQL-to-DBSP / DBSP | Adopt through the existing artifact and adapter gates; do not vendor the platform or compile generated Rust at runtime | Best fit for standing-view incremental semantics, but the full platform brings Java/Maven/compiler and runtime assumptions that should stay outside the Velorix hot path |
| Apache DataFusion | Keep as the ad hoc SQL and Arrow execution owner | Already aligned with Velorix's Parquet object-backed scan path; avoids a custom planner |
| SlateDB | Deepen as the durable state substrate | Strong fit for object-store-native state, LSM/SST, snapshot, compaction, and cache-aware storage responsibilities |
| Foyer | Keep as local runtime cache owner | Good fit for disposable local memory/disk cache, but must remain non-authoritative |
| `object_store` | Promote to first-class storage API owner | Object storage is the database authority; adapter capability checks cannot be implicit |
| Apache Iceberg | Treat as optional interoperability/export/import candidate | Velorix-owned object-storage manifests are the default internal table/state authority; Iceberg should own only a specific external table surface after an adoption RFC |
| OpenData | Use as a design and pattern reference only unless a narrow adoption RFC proves ownership boundaries | Shares Velorix's object-native direction, but it is a database fleet/project, not a narrow library dependency |
| Arroyo | Use as a reference for control plane, scheduling, connectors, and checkpoint lifecycle; avoid embedding the engine unless a narrow adoption RFC proves ownership boundaries | Mature streaming SQL surface, but it assumes a full distributed stream processing platform and carries patched Arrow/DataFusion/sqlparser dependencies |

## Product Direction Clarification

Velorix should be Kubernetes-native for production control plane and
orchestration. The production shape should use CRDs and an operator for
catalog/view lifecycle, status, scheduling intent, and worker orchestration.
Kubernetes `Lease` objects, or an equivalent K8s-native lease primitive, are the
preferred first owner for partition fencing and `owner_epoch` assignment.

Kubernetes and etcd are not the database authority. They may coordinate
control-plane intent and leases, but durable database state remains in object
storage. Velorix-owned manifests for input batches, engine state refs,
checkpoint publication, and materialized output objects are the default
internal database/table state.

The design is Databend-like in being object-storage-first with disposable
stateless compute, but Velorix is streaming/incremental-first. The internal
model is DBSP-shaped standing views, checkpoint manifests, and manifest-backed
materialized outputs, not a batch warehouse table format as the core authority.

## Production-Readiness Gaps These Reviews Add

- A write-path buffer should exist before high-throughput ingest is called
  product-ready. OpenData Buffer is the best design reference: producers flush
  immutable batches to object storage and a manifest-backed queue coordinates
  consumers.
- A write coordinator should own batching, backpressure, serial order, and
  durability watermarks. OpenData's write-coordination RFC is directly relevant
  to Velorix's future ingestion and view update path.
- A production checkpoint lifecycle needs explicit statuses, metadata rows,
  compaction, cleanup, and recovery transitions. Arroyo's controller/state split
  is a useful model, but Velorix should encode those states in object-backed
  manifests rather than copying Arroyo's database-backed controller state.
- Connectors need first-class status, error accounting, pause/resume semantics,
  and end-of-input handling. Feldera and Arroyo both show that this is product
  surface, not incidental runtime code.
- Benchmarks should be integration-level, object-store-backed, machine-readable,
  and CI/regression friendly. OpenData's bencher RFC provides a practical shape.
- Direct engine adoption must remain gated. Feldera/DBSP is the standing-view
  target, while DataFusion remains the ad hoc query target; conflating those two
  surfaces would make Velorix harder to reason about.
- Production storage adapters must fail closed when they cannot provide the
  manifest contract: conditional create/CAS behavior, metadata/ETag reads,
  range reads, listing semantics, multipart abort, bulk delete, timeout/retry
  policy, and telemetry.
- Raw Parquet URL catalog objects remain phase-0/dev table specs. Product-grade
  external table surfaces need explicit snapshot, schema, partition, compaction,
  and time-travel semantics. Apache Iceberg is an optional interoperability,
  export, or import candidate for those surfaces, not the default internal
  Velorix table/state format.
- Distributed writer fencing is now a first-class Kubernetes-native production
  decision. Object-storage manifests alone do not prove that only one worker
  owns a stream partition, so writes and manifests must carry `owner_epoch`
  once distributed execution is enabled.
- Dependency governance is part of database readiness. Package reviews must
  cover version pinning, MSRV, license, advisories, feature flags, audit status,
  replacement plan, and upgrade tests.

## Document Set

- [OpenData Package Review](package-review-opendata.md)
- [Arroyo Package Review](package-review-arroyo.md)
- [Feldera and Core Package Review](package-review-feldera-and-core-packages.md)
