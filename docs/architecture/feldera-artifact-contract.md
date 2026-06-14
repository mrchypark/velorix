# Feldera Compile Artifact Contract

This contract now describes the static release-bound artifact path, not the
primary Velorix product runtime path. The primary direction is package-first
Feldera runtime integration: reuse Feldera public Rust package layers such as
`dbsp`, `feldera-sqllib`, `feldera-ir`, and `feldera-types` where they fit the
Velorix standing-program boundary. See
[Feldera Package-First Runtime Design](../superpowers/specs/2026-05-27-feldera-package-first-runtime-design.md).

Velorix does not compile standing-view SQL through a Velorix-owned
shape-by-shape parser. This static artifact contract documents the older
Feldera Java/Calcite SQL compiler handoff used by generated Rust artifacts and
pipeline-manager compatibility checks. It is not the product backend target.
The product target is a jarless Feldera package path that reuses public Feldera
Rust packages where they expose stable descriptor/runtime boundaries and fails
closed when a requested SQL family still requires the Java SQL compiler.
Phase 0 records the contract Velorix expects from the legacy compilation step
without vendoring Feldera, invoking Java/Maven builds in `velorix-api`, or
loading generated Rust at runtime.

## Contract Shape

`velorix-core::feldera_artifact` defines three serde-backed documents:

- `StandingViewSpec`: the Velorix view contract, including `view_id`, SQL text,
  Feldera SQL dialect/source kind, typed input relation schemas, typed output
  relation schemas, and current shape flags.
- `FelderaCompileArtifactMetadata`: the compiler artifact identity, including
  metadata version, `view_id`, resolved spec hash, compile request hash,
  artifact id/hash, Feldera compiler identity, generated Rust ABI identity,
  input/output schemas, state codec, state schema version, and epoch policy.
- `FelderaReleaseArtifactProvenanceV1`: the release/build provenance identity,
  including release id/version, build id/builder id, artifact id/hash, spec
  hash, generated Rust ABI/crate identity, source repository/revision, and
  compiler name/version.

The v1 `spec_hash` format is
`velorix-feldera-spec-sha256-v1:<hex>`. The digest is SHA-256 over the
canonical serde JSON bytes produced by compact `serde_json` serialization of
the typed `StandingViewSpec` after deserialization. Struct field order is the
Rust wire-struct declaration order, enum names use the documented serde names,
arrays keep their declared order, and there is no pretty-printing or trailing
newline. This keeps the hash independent of source-file whitespace while
pinning the exact spec contract Velorix validated.

Dynamic compiler-backed views also carry a separate pending request identity:
`velorix-feldera-compile-request-sha256-v1:<hex>`. The digest is computed over
`FelderaCompileRequestV1`: view id, SQL, dialect/source kind, input relation
schemas, output schema contract, and shape. When the output contract is
`Infer`, compiler-inferred output relations are deliberately excluded from the
compile request hash and the pending compiler request stores an empty
`output_relations` snapshot. Activation still resolves the actual output schemas
into a `StandingViewSpec` and validates the artifact against that resolved
`spec_hash`.
`source_kind: standing_view` means the SQL is a single view body that Velorix
wraps as `CREATE MATERIALIZED VIEW "{view_id}" AS ...`.
`source_kind: feldera_program` means the SQL is already a Feldera program body;
Velorix prepends catalog-owned input `CREATE TABLE` declarations but does not
wrap the SQL. When `source_kind` is omitted, create-view admission also treats
SQL starting with `CREATE` after leading whitespace or SQL comments as a Feldera
program body so program SQL is not wrapped accidentally. Program requests may
declare `output_relation_ids` as an admission-time hint, but they no longer need
to predeclare output ids for compiler-backed discovery: the compile request asks
Feldera to infer all program outputs and the resolved spec is normalized from
`program_info.schema.outputs`.
When submitting SQL to Feldera pipeline-manager, Velorix strips the relation
weight column from generated input `CREATE TABLE` declarations. The weight
column remains part of the Velorix catalog identity, standing-runtime input
schema, and ingest envelope, but the pipeline-manager path treats it as change
metadata, not Feldera row data. Runtime ingestion uses that column to choose
Feldera `insert_delete` polarity and strips it before posting rows to Feldera
ingress. Relation capabilities that can produce a before image (`Delete`,
`Update`, or `Upsert`) allow Velorix to emit internal Feldera delete events;
direct user `delete` envelopes still require the relation's explicit `Delete`
capability.
Promoted Data API metadata may bind to one resolved output relation through
`outputRelationId`/`output_relation_id`; this binding is separate from the
program-level compile request identity.
For artifacts whose execution path is `feldera_pipeline_manager`, promoted API
`sql_template` admission validates template placeholders, declared parameters,
and the selected output binding only. Velorix does not require the template SQL
body to locally reference that output relation, and does not parse or execute
those templates through DataFusion before activation; rendered query SQL is
submitted to Feldera `/query` without Velorix legacy snapshot SQL normalization.
Template placeholders are recognized only in normal SQL text outside SQL
strings, quoted identifiers, and comments, so Feldera owns SQL grammar
acceptance for that runtime path. Static
linked/generated runtimes still use local DataFusion validation because their
templated reads execute over local Arrow snapshots.
For the same pipeline-manager execution path, view-scoped POST query endpoints
may carry caller-supplied `sql`. Velorix submits that SQL to Feldera `/query`
without local SQL parsing after rendering optional `{{ context.params.<name> |
... }}` placeholders from the request `parameters` map into escaped Feldera SQL
literals. Unreferenced parameters are rejected. When parameters are present,
only `{{ ... }}` expressions starting with `context.params.` in normal SQL text
outside SQL strings, quoted identifiers, and comments are treated as Velorix
placeholders; other brace blocks remain Feldera-owned SQL text. If `parameters`
is empty or omitted, SQL text is not scanned for placeholders, so Feldera-owned
syntax or string literals containing `{{`/`}}` pass through unchanged. This
remains scoped to an active view/output endpoint and does not reintroduce the
removed generic `/v1/query` surface; static linked/generated runtimes reject
caller-supplied SQL.
Artifact metadata v2 must carry both identities: `spec_hash` proves the
resolved schema-bearing view contract, while `compile_request_hash` proves the
pending compiler request that produced the artifact. Metadata v1 remains
readable only as a legacy compatibility format; if it declares
`compile_request_hash`, Velorix still validates the hash format.
When the compiler inferred output schemas after admission, the completion
request must provide the resolved `StandingViewSpec` alongside the artifact.
Velorix persists that resolved spec under its own `spec_hash` and atomically
moves the active view record from the pending compile request to the resolved
standing-runtime identity.

Validation is intentionally fail-closed. Velorix rejects unsupported metadata
versions, missing or blank required identity fields, missing schemas, unknown
JSON fields, malformed JSON, unknown state codecs, unsupported epoch policies,
generated Rust ABI versions outside the phase-0 contract, view id or spec hash
mismatches, metadata v2 compile request hash mismatches, schema mismatches, and
standing-view shape flags that disagree with the declared relation counts. The
current contract accepts multiple input relations and multiple materialized
output relations when the `multi_input`/`multi_output` flags match the schemas,
but execution remains gated by the runtime package and compiler boundary.
Required wire fields do not receive serde defaults; JSON must declare them
explicitly.

The relation schemas are SQL-facing contracts. They describe relation ids,
relation names, typed columns, nullability, and primary keys. They are not the
DataFusion `DeltaBatch` ad hoc query table with `key_json`, `value_json`, and
`weight`. Every Feldera input/output relation schema must carry the same
`relation_id`, `relation_version`, and `schema_fingerprint` used by ingest and
DataFusion registration. Artifact activation fails closed if the artifact
relation fingerprint differs from the cataloged input relation fingerprint. See
[Relation Contract V1](relation-contract-v1.md) and
[Schema Fingerprint V1](schema-fingerprint-v1.md).

## Trust Boundary

Generated Rust is trusted only as a build/release artifact. The release
pipeline compiles Feldera-generated Rust into a Velorix engine package after
reviewing the metadata, pinning compiler identity, and verifying the artifact
hash and the SHA-256 `spec_hash`. The running Velorix process selects among
already-built, release-trusted artifacts; it does not compile or dynamically
load arbitrary generated source from object storage.
`velorix-core::feldera_artifact` exposes artifact hash verification helpers
that reuse the metadata/spec validation and compare supplied artifact bytes
against `FelderaCompileArtifactMetadata.artifact_hash`; they do not invoke
Feldera, DBSP, Java/Maven, dynamic loading, or generated Rust execution. This is
byte-integrity evidence, not proof that a trusted release pipeline produced the
artifact.

Release provenance is a separate fail-closed readiness slice. The provenance
document uses serde `deny_unknown_fields`, requires every release/build/source
identity string, and must match `FelderaCompileArtifactMetadata` for
`artifact_id`, `artifact_hash`, `spec_hash`,
`generated_rust.abi_version`, and `generated_rust.crate_name`.
`velorix-cli feldera-artifact-provenance-verify --metadata <metadata-json>
--provenance <provenance-json> --json` emits stable
`feldera_artifact_release_provenance` readiness evidence after those checks
pass. It only validates JSON identity; it does not load, compile, or execute a
Feldera/DBSP artifact and it does not create a global artifact-id index.

Object storage manifests remain durable data and progress authority. They may
reference a validated artifact id/hash, but they cannot make code executable by
themselves. This prevents a checkpoint or manifest write from becoming a code
loading path.

## Artifact Registry

Velorix persists accepted phase-0 artifact metadata in the object store under
`v1/feldera-artifacts/{artifact_id}/sha256/{artifact_hash_hex}.artifact.json`.
Registration is create-only. Re-registering the exact same metadata is
idempotent, while reusing the same object key for different metadata fails
closed. The registry identity is the `(artifact_id, artifact_hash)` pair; if a
future product contract requires globally unique artifact ids, it must use a
separate create-only index object rather than a prefix scan.

The storage registry only stores and retrieves
`FelderaCompileArtifactMetadata`. Every registration validates the metadata
against the supplied `StandingViewSpec` with
`validate_feldera_compile_artifact`, so spec hash, view id, relation schemas,
schema fingerprints, state codec, epoch policy, and generated Rust ABI checks
remain owned by `velorix-core`. Reads deserialize with unknown-field rejection
from the core wire type and reject a stored body whose artifact identity does
not match the requested key. This registry does not compile, load, or
dynamically execute Feldera/DBSP artifacts.

The runtime facade in `velorix-runtime::feldera_registry` delegates durable
storage to that registry and does not define another object-key scheme.
Registering or selecting an artifact validates the metadata against the
provided `VelorixRelationCatalogV1` with
`validate_feldera_compile_artifact_for_catalog`, then checks whether the
generated Rust package identity is registered with the running Velorix binary.
API registration also binds metadata v2 artifacts to the recomputed
`FelderaCompileRequestV1` hash for the active view before activating a standing
runtime. Completion of a pending compile/deploy job therefore requires the
pending job request, the completion request, and the artifact metadata to agree
on the same compile request identity. If the compiler returns a resolved spec,
Velorix also requires the resolved spec to keep the same view id, SQL,
dialect/source kind, input relations, and shape as the pending request, while
allowing the compiler-inferred output relations to replace the transitional
placeholder output schema.
If the package is present, selection returns `DirectExecutionEnabled`; if it is
missing, selection returns `DirectExecutionDisabled` and product APIs fail
closed before creating an artifact-backed view. A tenant/artifact-id lookup
index remains deferred; if product semantics later require one, it should be a
separate create-only index object rather than replacing the artifact id/hash
registry key.
Runtime hash-verified registration can require matching artifact bytes before
persisting metadata, and still requires a package match before direct execution
can be enabled.

## Runtime Direction

DataFusion remains the ad hoc SQL/query engine, but the accepted target input is
cataloged typed Arrow relations, not durable JSON `DeltaBatch`. Any remaining
`DeltaBatch` query path is bootstrap-only and must not define Feldera artifact
compatibility.

Feldera owns standing-view SQL compilation. The artifact contract gives
Velorix a stable handoff point for `FelderaPipelineEngine` work:

1. Velorix records a `StandingViewSpec` for the view.
2. External Feldera tooling compiles that spec into a DBSP/Rust artifact.
3. Build/release automation verifies `FelderaCompileArtifactMetadata`.
4. The runtime maps the trusted artifact id/hash to a compiled execution
   package and persists state using the declared codec, schema version, and
   epoch policy.

The first runtime DBSP slice is deliberately narrower than generated artifact
loading. `velorix-core::dbsp_engine::DbspSingleKeySumCountEngine` remains
quarantined behind the internal `dbsp-spike` compilation boundary, but
`velorix-runtime` enables its `dbsp-runtime` integration by default. The public
runtime backend name is `Dbsp`, not `FelderaDbsp`. Default recovery uses DBSP
for the single `Utf8` primary-key plus `Int64` sum/count materialized-view shape
and falls back to the prototype engine for relation shapes this DBSP slice does
not yet support. Explicit `IncrementalEngineBackend::Dbsp` or
`VELORIX_INCREMENTAL_ENGINE=dbsp` requests fail closed if the catalog is outside
that supported DBSP shape.

Resolved materialized view definitions are durable `StandingViewSpec` records stored by
the materialized-view registry under
`v1/views/{view_id}/spec-sha256/{spec_hash}.view.json`. The record
is create-only and content-addressed by the canonical Feldera spec hash, so a
view definition can be registered and recovered independently of generated Rust
artifact release provenance.

Pending dynamic compiler requests should not use placeholder output relations as
the final durable identity. They are tracked by compile request hash until the
compiler returns output schemas and runtime metadata; only then should Velorix
register or activate the resolved `StandingViewSpec`. When the output contract
is `Infer`, a pending compiler request with a non-empty `output_relations`
snapshot is rejected as identity drift.
Compile/deploy job records for pending dynamic views are keyed primarily by the
compile request hash under
`v1/view-compile-deploy-jobs/{view_id}/compile-request-sha256/{hash}.job.json`.
The older `spec-sha256` job key remains a legacy fallback during migration
because current active pending views still carry a transitional placeholder
`StandingViewSpec`.

`POST /v1/views` can attach a generated artifact by supplying
`artifact.metadata`. In that mode Velorix builds the `StandingViewSpec` using
the artifact output schema, validates the artifact against the catalog/spec,
registers the metadata, requires an executable package match, and stores the
selected artifact identity on the active view record. Artifact-backed views are
therefore allowed to use Feldera SQL outside the hand-coded single-relation
sum/count DBSP SQL validator. The durable active record includes artifact
id/hash, generated Rust crate name, state codec/schema version, and execution
status.

Feldera REST API usage, Java/Maven compiler invocation, dynamic generated Rust
loading, and arbitrary artifact execution from object storage remain outside
this static artifact slice. The runtime DBSP backend is a package-backed spike
for the bootstrap `IncrementalEngine` boundary. It must be superseded by a
standing-program runtime boundary before Velorix claims broad Feldera-backed
materialized view support.
