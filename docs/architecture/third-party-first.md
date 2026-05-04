# Third-Party-First Architecture

Velorix should avoid direct hand-written implementations where mature packages
already fit the job. The project should integrate proven substrates and keep
Velorix-specific code focused on object-storage authority, checkpoint manifests,
stateless recovery, resource/cost policy, and package boundaries.

Production Velorix should be Kubernetes-native for control plane and
orchestration, while remaining object-storage-authoritative for database state.
Kubernetes CRDs, an operator, and `Lease` objects may coordinate lifecycle,
scheduling, worker ownership, and `owner_epoch`, but Kubernetes and etcd are not
the database authority. Velorix-owned object-storage manifests for input
batches, checkpointed state refs, and materialized output objects are the
default internal table/state authority. The design is Databend-like in being
object-storage-first with disposable stateless compute, but Velorix is
streaming/incremental-first with DBSP-shaped standing views and checkpoint
manifests. Iceberg is optional external interoperability, export, or import
work only after a narrow adoption RFC.

This note distinguishes current implementation from target direction. The
runtime object cache is currently Foyer-backed, ad hoc SQL/query planning and
execution currently use DataFusion for the minimal `DeltaBatch`, validation,
and recovered materialized-state query boundaries, and persisted query service
v0 stores validated JSON query specs in object storage. Persisted table catalog
v0 stores Parquet scan URL specs for the direct object-backed scan path.
Persisted view access v0 composes stored query specs with stored object-backed
Parquet table specs. SlateDB now backs a minimal experimental
checkpoint-versioned state-store path.
Incremental execution now has a DBSP-shaped `IncrementalEngine` boundary backed
by prototype operators.
Feldera SQL-to-DBSP standing-view compilation now has a phase-0 artifact
metadata/spec contract. Direct runtime Feldera DBSP/dbsp integration remains
gated unless matching code exists in the repository.
The current `velorix-control` crate defines a pure lease-domain boundary for
partition ownership. It issues storage-compatible `owner_id`/`owner_epoch`
claims through explicit-clock domain types and an in-memory fake/test client,
without a Kubernetes dependency. A real Kubernetes `Lease` adapter, CRDs,
operator reconciliation, and any production linearizable fencing or commit
protocol remain future work. Object storage remains the durable database
authority; Kubernetes/control leases coordinate ownership epochs only.

See the package review notes for the current production-readiness package
strategy:

- [Package Review Index](package-review-index.md)
- [OpenData Package Review](package-review-opendata.md)
- [Arroyo Package Review](package-review-arroyo.md)
- [Feldera and Core Package Review](package-review-feldera-and-core-packages.md)

## Package Ownership

| Area | Status | Preferred owner | Velorix-owned boundary |
| --- | --- | --- | --- |
| Object storage API and adapter capabilities | Current implementation dependency; production capability gates still needed | Apache `object_store` crate plus backend-specific adapters | Backend allowlists, conditional-create/CAS requirements, credentials policy, shared registry, telemetry, fail-closed startup |
| Durable LSM/SST/state substrate | Current minimal experimental state-store implementation | SlateDB | Object key policy, stream progress, exactly-once manifests, recovery orchestration |
| Runtime object-store fetch-through cache | Current implementation | Foyer | Object-store authority checks, cache namespace policy, cache-as-non-durable invariant |
| Ad hoc SQL/DataFrame/query planning, validation, and Arrow execution | Current minimal implementation | Apache DataFusion | Runtime integration, checkpoint-aware inputs/outputs, persisted query catalog specs, cost/resource policy |
| Internal materialized table/state authority | Target direction; current manifests and checkpoint refs already define the recovery boundary | Velorix-owned object-storage manifests and objects | Input/state/output manifest schemas, exactly-once publication, checkpoint binding, recovery, GC |
| External table interoperability | Future RFC only; raw Parquet URL specs are phase-0 only | Apache Iceberg or another table format, only for import/export/interop surfaces | Table/catalog authorization, snapshot selection, table lifecycle policy, manifest binding when crossing the internal boundary |
| Standing-view SQL-to-DBSP compilation | Current phase-0 artifact contract | Feldera SQL-to-DBSP compiler and pipeline tooling | Spec/artifact validation, release artifact selection, object-backed state and manifests |
| Incremental algebra, operators, and circuit semantics | Current adapter boundary; direct DBSP crate integration remains gated | Feldera project semantics and/or Rust `dbsp` crate | `IncrementalEngine` adapter, object-backed persistence, moderate-performance cost optimizations |
| Kubernetes-native control plane/orchestration | Current pure `velorix-control` lease-domain boundary; target production Kubernetes adapter/operator direction | Current Velorix domain crate and in-memory fake; future Kubernetes CRDs, operator, and Lease or equivalent K8s-native lease primitive | Storage-compatible owner claims, owner epoch in writes/manifests, stale-worker rejection, recovery handoff rules; future catalog/view lifecycle intent, status, scheduling policy, and real Kubernetes acquisition |

## Cache Boundary

Velorix runtime code uses a Foyer wrapper for local memory/disk caching of
object-store fetch-through reads. The cache is never durable authority; object
storage and checkpoint manifests remain authoritative for recovery and progress.

SlateDB may use Foyer internally for its own block or object cache as the
SlateDB integration grows. That cache belongs to SlateDB's state substrate
internals. Velorix should keep the runtime object cache policy separate from any
SlateDB-internal cache policy to avoid duplicate eviction, durability, or
authority rules.

## DataFusion Query Boundary

Velorix core currently exposes a minimal DataFusion-backed query boundary for
SQL over `DeltaBatch` input. Delta records are converted into an in-memory
Arrow/DataFusion table named `input`, with stable `key_json`, `value_json`, and
`weight` columns. DataFusion owns SQL parsing, query planning, physical
execution, and Arrow `RecordBatch` output.

Velorix runtime now adds the smallest object-backed, checkpoint-aware query
boundary by first running stateless recovery against object storage and then
querying the recovered materialized `DeltaBatch` through that same DataFusion
path. This keeps object storage as durable authority and avoids a custom query
planner.

Velorix runtime also exposes the first direct object-backed DataFusion scan
boundary for Parquet input. The runtime registers caller-provided object storage
with DataFusion, registers the Parquet object URL as the `input` table, applies
the existing `QueryPolicy`, and returns Arrow `RecordBatch` output. DataFusion
owns SQL parsing, planning, execution, and Parquet object scanning.

Persisted query service v0 stores `PersistedQuerySpec` JSON objects under
deterministic `v1/queries/{query_id}.query.json` keys. Query ids use the shared
`ObjectKey` segment rules, SQL is validated by DataFusion against an empty
`input` table before a create-only catalog write, and execution loads the stored
SQL/policy before calling the recovered materialized-state query boundary.

Persisted table catalog v0 stores `PersistedTableSpec` JSON objects under
deterministic `v1/tables/{table_id}.table.json` keys for the direct Parquet scan
path. Table ids use the shared `ObjectKey` segment rules, create validates the
URL shape and format enum, and execution loads the stored URL before delegating
to the existing direct DataFusion Parquet scan helper. It does not scan or list
table contents at create time.

Persisted view access v0 is composition only: it loads one `PersistedQuerySpec`
and one `PersistedTableSpec` from object storage, then executes the stored SQL
over the stored object-backed Parquet table URL through the existing DataFusion
scan helper. Velorix does not add custom scanning, table listing, scheduling,
versioning, permissions, or Feldera execution in this boundary.

The query boundary also exposes a minimal Velorix-owned policy for SQL text
size, output row caps, DataFusion batch size, and target partitions. Output row
caps are enforced by applying a DataFusion `DataFrame` limit before collection,
not by parsing or planning SQL inside Velorix.

This is an ad hoc SQL/query surface, not standing-view compilation. Direct
Parquet object-backed scans and a small object-backed table catalog now exist as
a minimal boundary, with persisted view access v0 limited to a stored query over
a stored object-backed Parquet table. Broader table layout, query
scheduling/versioning, permissions, and memory-pool/disk-spill runtime resource
policy remain future integration work.

Apache Iceberg is not the default internal Velorix table format. It is an
optional candidate for future import, export, or interoperability surfaces if an
adoption RFC proves a narrow ownership boundary. Internal materialized outputs
remain Velorix manifest-backed object-storage objects by default.

## Feldera Standing-View Compile Contract

Feldera owns SQL-to-DBSP compilation for standing views. Velorix now defines a
phase-0 `feldera_artifact` contract in `velorix-core` that validates a
`StandingViewSpec` against `FelderaCompileArtifactMetadata`. The metadata
records compiler identity, generated Rust ABI identity, artifact id/hash,
typed input/output relation schemas, state codec, state schema version, and
epoch policy.

The v1 spec hash is
`velorix-feldera-spec-sha256-v1:<hex>`, where `<hex>` is SHA-256 over the
canonical compact serde JSON bytes for the typed `StandingViewSpec`.

Validation fails closed on unsupported metadata versions, blank identity fields,
missing schemas, malformed or unknown-field JSON, unsupported state codec or
epoch policy, mismatched view id or spec hash, schema mismatch, and unsupported
multi-input or multi-output shapes. Phase 0 supports one input relation and one
output relation.

Generated Rust is trusted only at build/release time. Object storage manifests
may reference a previously built artifact id/hash, but manifests cannot load
arbitrary generated code into a running process.

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
Checkpoint state for that boundary is serialized as a versioned engine payload
containing `schema_version`, `logical_epoch`, and `state`. The manifest
checkpoint version remains the publication/progress version and is not the
engine logical epoch. Runtime recovery still accepts legacy raw `DeltaBatch`
state objects by treating the manifest checkpoint version as the only available
epoch fallback for those old payloads.

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
   custom planner or expression engine. This DataFusion path is for ad hoc SQL
   over in-memory `DeltaBatch` input, runtime-recovered materialized state, and
   minimal direct Parquet object-backed `input` scans; persisted query service
   v0 catalogs validated SQL/policy specs in object storage, and persisted table
   catalog v0 catalogs Parquet scan URLs for the direct scan path. Persisted
   view access v0 composes those specs as stored query over stored Parquet
   input. Broader table layout, scheduling, versioning, and permissions remain
   future work.
6. Make the production control plane Kubernetes-native: model catalog/view
   lifecycle with CRDs and an operator, and use Kubernetes `Lease` or an
   equivalent K8s-native primitive for partition ownership and `owner_epoch`.
   Keep object storage, not Kubernetes or etcd, as the durable database
   authority. The current `velorix-control` slice is only a pure lease-domain
   API plus in-memory fake that issues storage-compatible owner claims; it has
   no Kubernetes dependency. The current storage slice carries and verifies
   plain `owner_id`/`owner_epoch` claim metadata on fenced state writes and
   checkpoint manifest publication, but those checks are non-atomic stale-owner
   detection and structurally unauthorized progress rejection, not production
   linearizable fencing. Actual Kubernetes Lease acquisition and any production
   fencing or marker-index commit protocol remain future control-plane/storage
   design work.
7. Use Feldera's SQL-to-DBSP compiler for standing-view SQL through validated
   external compiler artifacts. Do not hand-build Velorix circuits for standing
   views in this phase.
8. Use Feldera DBSP semantics as the reference model for incremental operators
   and circuit semantics. Consider direct Rust `dbsp` crate integration only
   after the adoption gates are satisfied; otherwise keep an adapter boundary.
9. Treat Iceberg as optional external table interoperability, export, or import
   work until a future RFC assigns it a specific surface.

## Non-Goals

- Do not build a bespoke query planner or expression engine when DataFusion fits.
- Do not build a separate durable LSM/compaction engine when SlateDB fits.
- Do not build custom memory/disk cache internals when Foyer fits.
- Do not keep expanding prototype delta/operator code as the long-term execution
  engine when Feldera DBSP semantics, adapters, or a gated `dbsp` crate
  integration can own the model.
- Do not vendor Feldera, add Java/Maven compiler builds, or load generated Rust
  from object storage at runtime as part of the phase-0 artifact contract.
- Do not treat Kubernetes, etcd, Iceberg, OpenData, or Arroyo as the durable
  internal database authority without a narrow adoption RFC. Object storage and
  Velorix manifests remain authoritative by default.
