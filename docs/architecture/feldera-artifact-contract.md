# Feldera Compile Artifact Contract

This contract now describes the static release-bound artifact path, not the
primary Velorix product runtime path. The primary direction is package-first
Feldera runtime integration: reuse Feldera public Rust package layers such as
`dbsp`, `feldera-sqllib`, `feldera-ir`, and `feldera-types` where they fit the
Velorix standing-program boundary. See
[Feldera Package-First Runtime Design](../superpowers/specs/2026-05-27-feldera-package-first-runtime-design.md).

Velorix does not compile standing-view SQL itself. Feldera owns SQL-to-DBSP
compilation through the Java/Calcite SQL compiler in
`sql-to-dbsp-compiler/SQL-compiler` and the surrounding Feldera pipeline
tooling. Phase 0 records the contract Velorix expects from that compilation
step without vendoring Feldera, invoking Java/Maven builds, or loading
generated Rust at runtime.

## Contract Shape

`velorix-core::feldera_artifact` defines three serde-backed documents:

- `StandingViewSpec`: the Velorix view contract, including `view_id`, SQL text,
  Feldera SQL dialect/source kind, typed input relation schemas, typed output
  relation schemas, and current shape flags.
- `FelderaCompileArtifactMetadata`: the compiler artifact identity, including
  metadata version, `view_id`, spec hash, artifact id/hash, Feldera compiler
  identity, generated Rust ABI identity, input/output schemas, state codec,
  state schema version, and epoch policy.
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

Validation is intentionally fail-closed. Velorix rejects unsupported metadata
versions, missing or blank required identity fields, missing schemas, unknown
JSON fields, malformed JSON, unknown state codecs, unsupported epoch policies,
generated Rust ABI versions outside the phase-0 contract, view id or spec hash
mismatches, schema mismatches, and multi-input or multi-output standing-view
shapes. Phase 0 supports one input relation and one output relation only.
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

Materialized view definitions are durable `StandingViewSpec` records stored by
the materialized-view registry under
`v1/views/{view_id}/spec-sha256/{spec_hash}.view.json`. The record
is create-only and content-addressed by the canonical Feldera spec hash, so a
view definition can be registered and recovered independently of generated Rust
artifact release provenance.

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
