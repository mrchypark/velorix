# Feldera and Core Package Review

Reviewed sources:

- [feldera/feldera](https://github.com/feldera/feldera) at
  `7ae331bf92f4272da4804ec2ea66bc7c0d71cf31` (2026-05-03)
- Current Velorix workspace on `feature/velorix-bootstrap`
- Existing package directives in [Third-Party-First Architecture](third-party-first.md)

## Decision Summary

Velorix should keep the current package ownership model and make it sharper:

- Feldera owns standing-view SQL-to-DBSP semantics and compile artifacts.
- DataFusion owns ad hoc SQL, Arrow execution, and direct Parquet scans.
- SlateDB owns the durable LSM/SST/state substrate as that path matures.
- Foyer owns disposable local cache internals.
- `object_store` owns the shared object storage API boundary. Arrow, Parquet,
  Tokio, serde, and proptest remain shared foundation crates.
- Velorix-owned object-storage manifests are the default internal table/state
  authority for inputs, checkpointed state refs, and materialized outputs.
- Apache Iceberg is an optional interoperability/export/import candidate, not
  the default internal table format. Raw Parquet URLs stay phase-0/dev only.
- Velorix owns object keys, manifests, exactly-once publication, stateless
  recovery, resource policy, product catalog contracts, and integration glue.

## Feldera

2026-05-27 realignment: Velorix's primary Feldera direction is now
package-first runtime integration, not release-image generated artifact
activation. See
[Feldera Package-First Runtime Design](../superpowers/specs/2026-05-27-feldera-package-first-runtime-design.md).
The static generated artifact path remains useful for release-bound fixtures
and provenance experiments, but it is not the main product path for REST-defined
views. Descriptor/executable/checkpoint identity is still required in the first
package-first proof; only release-image artifact activation is demoted.

Feldera is the best match for standing-view incremental execution. Its README
describes pipelines as SQL tables and views that process inserts, updates, and
deletes incrementally. It also documents full SQL support, DBSP theory,
connectors, fault tolerance, and ad hoc queries evaluated through DataFusion.

### What Velorix Should Reuse

- SQL-to-DBSP compilation as the standing-view compiler.
- DBSP semantics as the correctness model for incremental view maintenance.
- The Rust `dbsp` crate as the first in-process runtime representation to test,
  behind an explicit toolchain/MSRV gate.
- `feldera-sqllib` for SQL runtime types where standing-view inputs and outputs
  need typed SQL values instead of Velorix JSON-only bootstrap rows.
- `feldera-ir` and `feldera-types` as compiled-program descriptor, lifecycle,
  coordination, checkpoint, and comparison-adapter vocabulary where they fit
  without taking over Velorix durable authority.
- Opaque Feldera program descriptors for validation and diagnostics. Velorix
  must not execute by interpreting Feldera IR or Calcite plan nodes.
- The distinction between incremental pipeline execution and ad hoc DataFusion
  queries.
- Connector/checkpoint test ideas from Feldera's Python test suite, especially
  restart, checkpoint, transaction, connector status, and object-store sync
  scenarios.

### What Velorix Should Avoid

- Vendoring the full Feldera platform into the runtime.
- Invoking Java/Maven SQL compiler builds in the hot path.
- Loading generated Rust from object storage at runtime.
- Treating Feldera REST pipeline management as Velorix's internal runtime
  contract before an explicit product decision.
- Adopting Feldera's current workspace toolchain wholesale. The reviewed
  workspace uses a newer Rust requirement and a broad platform dependency set.

### Required Next Gates

Before direct runtime Feldera or `dbsp` adoption, Velorix needs:

1. A concrete package-first `StandingProgramRuntime` compatibility proof.
2. A proven Feldera-owned executable runtime mechanism; a compiled descriptor
   alone is not enough.
3. A schema/type mapping from `VelorixRelationCatalogV1` to Feldera SQL runtime
   values and view output schemas.
4. A state codec mapping from DBSP/Feldera state to object-backed Velorix state
   refs.
5. Recovery tests proving manifest checkpoint version and engine logical epoch
   remain distinct.
6. Resource tests for memory, spill, CPU, and object-store write amplification.
7. A release artifact registry mapping Velorix `artifact_id` and hash to a
   trusted binary/package only after the runtime compatibility proof passes.

The existing `feldera_artifact` phase-0 contract remains the right boundary.

## Apache DataFusion

DataFusion should continue to own ad hoc SQL and Arrow execution. Velorix
already uses it for:

- Bootstrap SQL over in-memory `DeltaBatch` input, scheduled to give way to
  cataloged typed Arrow relations.
- generated standing-runtime view page query surfaces.
- direct object-backed Parquet scans.
- persisted query validation and execution.

Production-readiness improvements should deepen this boundary rather than
replace it:

- Add tenant-scoped `SessionConfig`, shared memory pool, bounded disk spill
  manager, and spill directory quotas.
- Add registry-backed table/provider layout beyond single Parquet object URLs.
- Add query cancellation, timeout policy, scan-byte limits, object-store request
  limits, and concurrency limits.
- Add metrics around planning time, scan bytes, output rows, and spill.
- Keep SQL validation behavior-focused and backed by DataFusion, not custom
  parsing.
- Treat ad hoc SQL as untrusted code in production. Output row caps are not
  enough because expensive planning, scans, joins, sorts, and aggregation can
  consume memory, CPU, disk spill, and object-store egress before output rows
  are produced.

## SlateDB

SlateDB is the preferred durable state substrate because it is object-store
native and matches the Velorix rule that local compute is disposable. OpenData's
use of SlateDB across multiple database products strengthens this choice.

Velorix should next define:

- state key encodings and lexicographic ordering rules
- state table/column-family layout, if applicable through SlateDB APIs
- compaction and lifecycle policy
- snapshot/read isolation expectations
- garbage collection interaction between Velorix manifests and SlateDB objects
- S3 conditional-write assumptions and fallback behavior

Velorix must not duplicate SlateDB's LSM, SST, compaction, or cache internals.
Velorix garbage collection must never delete SlateDB-internal objects by prefix
walking; it may retain or release SlateDB state only through a SlateDB-owned
checkpoint/root handle or API. Internal materialized outputs should be
Velorix-manifest-backed object-storage objects under the `v1/outputs` namespace
by default, while `v1/ingest` remains committed input-only. Parquet and Iceberg
surfaces are external scan, import, export, or interoperability contracts unless
a future adoption RFC assigns one of them a narrower authority boundary.

## Foyer

Foyer remains the local runtime cache owner. It should continue to be treated as
non-authoritative fetch-through cache only.

Production-readiness work:

- expose cache metrics
- make namespace/tenant/view isolation explicit
- define restart behavior and cache invalidation policy
- prevent cached object presence from proving durability
- keep Velorix's runtime object cache separate from any SlateDB-internal cache
- make cache keys include tenant, store, object key, object version or ETag,
  byte range, and content encoding
- never store authorization decisions in cache entries

## Object Storage API

The `object_store` crate should be promoted from a supporting dependency to a
first-class package boundary. Object storage is the durable authority, so
adapter capabilities must be explicit.

Production startup should reject authoritative storage adapters that cannot
provide the manifest contract:

- conditional create or equivalent CAS for manifest publication
- metadata or ETag reads
- range reads
- list semantics documented per backend
- multipart abort
- bulk delete or safe delete iteration
- timeout, retry, and telemetry hooks

Velorix should use one shared object-store registry for storage, DataFusion
scans, SlateDB integration, and Foyer fetch-through. Separate clients with
different credentials, retries, endpoint allowlists, or telemetry would make
production behavior incoherent.

## `object_store`, Arrow, and Parquet

Arrow and Parquet are foundation crates, not optional conveniences. The raw
Parquet URL catalog is still only a phase-0 scan surface.

| Package | Velorix role |
| --- | --- |
| Arrow | In-memory columnar interchange between query/runtime boundaries |
| Parquet | First object-backed file format for controlled ad hoc scans and persisted phase-0 table specs |
| Apache Iceberg | Optional interoperability/export/import table surface candidate after a narrow adoption RFC |

Next work:

- Add S3-compatible integration tests behind an environment flag.
- Define object URL normalization and credential handling.
- Move beyond single Parquet object URLs for external table surfaces before
  claiming production interoperability.
- Keep Arrow schemas in explicit contracts rather than deriving them from
  JSON-only bootstrap state forever.

External table-surface tests should cover concurrent append, compaction, schema
add/drop/rename, partition evolution, and snapshot-time query reproducibility
when an interoperability format is adopted.

## Kubernetes-Native Control Plane and Fencing

Velorix should be Kubernetes-native for production control plane and
orchestration. Production lifecycle should be expressed through CRDs and an
operator for catalog/view specs, status, scheduling intent, worker ownership,
and recovery handoff.

Kubernetes and etcd are not the database authority. They coordinate
control-plane intent and leases, while object storage remains the durable
authority for Velorix-owned input manifests, checkpoint manifests, state refs,
and materialized output manifests.

Stateless compute does not by itself prevent two workers from writing the same
stream partition. The preferred production direction is Kubernetes `Lease`, or
an equivalent K8s-native lease primitive, as the first owner for partition
fencing and `owner_epoch` assignment. Postgres, raw etcd usage, OpenRaft, or an
object-store CAS lease should be treated as fallback or future alternatives only
after an explicit RFC.

Every state write and checkpoint manifest should eventually include
`partition_id`, `owner_id`, and `owner_epoch`. A stale worker must be unable to
write state objects, output objects, or manifests after losing the lease.

This makes Velorix Databend-like in the object-storage-first/stateless-compute
sense, but not in workload shape. Velorix is streaming/incremental-first:
standing views are DBSP-shaped, progress is checkpoint-manifest-shaped, and
internal materialized outputs are manifest-backed object-storage objects.

## Tokio, serde, thiserror, proptest

These are supporting packages that should remain boring and explicit:

- Tokio owns async execution primitives, but not runtime scheduling policy.
- serde owns wire format serialization, but every durable JSON object should
  remain fail-closed on unknown/missing fields.
- thiserror keeps boundary errors typed and testable.
- proptest should expand around object-key parsing, binary encoding, replay
  invariants, and manifest/state transitions.

## Dependency Governance

Database package adoption requires explicit governance:

- role and owner boundary
- direct dependency version and feature flags
- MSRV compatibility
- license policy
- RustSec advisory status
- `cargo deny` policy
- `cargo vet` or equivalent audit status
- transitive dependency review for risky crates
- replacement plan
- upgrade test suite

This is part of production readiness because DataFusion, SlateDB, Foyer,
Feldera, Arrow, object-store adapters, and generated Rust artifact tooling all
affect correctness and supply-chain risk.

## Package Review Matrix

| Area | Preferred package or source | Adopt now? | Notes |
| --- | --- | --- | --- |
| Standing-view SQL compilation | Feldera SQL-to-DBSP | Design now | Primary path should validate compiled Feldera descriptors, not Velorix-owned SQL shape parsing |
| Incremental semantics | Feldera DBSP / `dbsp` | Gate with compatibility proof | First prove a package-first `StandingProgramRuntime`; current sum/count DBSP code is a spike |
| SQL runtime values | `feldera-sqllib` | Gate with schema/type tests | Prefer Feldera SQL runtime types over JSON-only standing-view rows |
| Feldera program descriptors | `feldera-ir` / `feldera-types` | Research now | Use for compiled-program metadata and comparison adapters; do not make them durable authority without an RFC |
| Ad hoc SQL | DataFusion | Yes | Keep narrow, add resource policy |
| Object-backed table scans | DataFusion + Parquet + `object_store` | Yes | Keep raw URL specs dev-only; production needs registry-backed table layout |
| Internal materialized table/state authority | Velorix object-storage manifests | Yes | Default for input/state/output manifests and checkpoint publication |
| External table interoperability | Apache Iceberg or another table format | RFC only | Import/export/interop candidate, not core internal state |
| Durable state substrate | SlateDB | Yes, narrow | Define layout, compaction, GC, S3 assumptions |
| Local cache | Foyer | Yes | Non-authoritative only |
| Object storage API | `object_store` | Yes, first-class | Production adapters must fail closed without required capabilities |
| Write buffer | OpenData Buffer pattern | Design now | Possible dependency later, only after sequence/checkpoint mapping |
| Write coordination | OpenData RFC pattern | Design now | Needed for batching, backpressure, durability watermarks |
| Partition fencing | Kubernetes Lease or equivalent K8s-native lease primitive | Design now | Required to assign `owner_epoch` and reject stale workers; etcd is not database authority |
| Distributed control plane | Arroyo pattern | Reference | Avoid dependency; model states/status/scheduler boundaries |
| Connectors | Feldera/Arroyo patterns | Reference now | Implement only after catalog/status contracts |
| Benchmark framework | OpenData bencher pattern | Design now | CI-friendly object-store integration benchmarks |
| Dependency governance | `cargo deny` + `cargo vet` | Add | Required for license/advisory/audit policy |

## Resulting Product-Readiness Sequence

1. Keep current PR scope as a verified bootstrap, not a product-ready database.
2. Write the write-buffer and write-coordinator design before adding more ingest
   behavior.
3. Add checkpoint lifecycle/status and GC design before claiming operational
   readiness.
4. Add S3-compatible tests and object-store failure-mode tests.
5. Add partition-owner fencing and stale-worker rejection tests.
6. Add DataFusion resource controls and table layout.
7. Write any Iceberg or external table-format adoption RFC only for a specific
   interoperability/export/import surface.
8. Add connector catalog/status contracts.
9. Add dependency governance CI.
10. Evaluate package-first Feldera/DBSP runtime execution with a small
   `StandingProgramRuntime` vertical slice.
11. Only after that, promote release-bound artifact hash/provenance gates for
   static generated package builds.
