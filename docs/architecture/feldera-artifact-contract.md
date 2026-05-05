# Feldera Compile Artifact Contract

Velorix does not compile standing-view SQL itself. Feldera owns SQL-to-DBSP
compilation through the Java/Calcite SQL compiler in
`sql-to-dbsp-compiler/SQL-compiler` and the surrounding Feldera pipeline
tooling. Phase 0 records the contract Velorix expects from that compilation
step without vendoring Feldera, invoking Java/Maven builds, or loading
generated Rust at runtime.

## Contract Shape

`velorix-core::feldera_artifact` defines two serde-backed documents:

- `StandingViewSpec`: the Velorix view contract, including `view_id`, SQL text,
  Feldera SQL dialect/source kind, typed input relation schemas, typed output
  relation schemas, and current shape flags.
- `FelderaCompileArtifactMetadata`: the compiler artifact identity, including
  metadata version, `view_id`, spec hash, artifact id/hash, Feldera compiler
  identity, generated Rust ABI identity, input/output schemas, state codec,
  state schema version, and epoch policy.

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

Generated Rust is trusted only as a build/release artifact. A future release
pipeline can compile Feldera-generated Rust into a Velorix engine package after
reviewing the metadata, pinning compiler identity, and verifying the artifact
hash and the SHA-256 `spec_hash`. The running Velorix process should select
among already-built, release-trusted artifacts; it should not compile or
dynamically load arbitrary generated source from object storage.

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
not match the requested key. This registry does not compile, load, or execute
Feldera/DBSP artifacts.

## Runtime Direction

DataFusion remains the ad hoc SQL/query engine, but the accepted target input is
cataloged typed Arrow relations, not durable JSON `DeltaBatch`. Any remaining
`DeltaBatch` query path is bootstrap-only and must not define Feldera artifact
compatibility.

Feldera owns standing-view SQL compilation. The phase-0 artifact contract gives
Velorix a stable handoff point for future `FelderaPipelineEngine` work:

1. Velorix records a `StandingViewSpec` for the view.
2. External Feldera tooling compiles that spec into a DBSP/Rust artifact.
3. Build/release automation verifies `FelderaCompileArtifactMetadata`.
4. A future engine maps the trusted artifact id/hash to a compiled execution
   package and persists state using the declared codec, schema version, and
   epoch policy.

Direct runtime Feldera integration, direct `dbsp` crate adoption, Feldera REST
API usage, Java/Maven compiler invocation, and generated Rust compilation are
all outside this phase.
