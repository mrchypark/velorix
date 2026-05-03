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

Validation is intentionally fail-closed. Velorix rejects unsupported metadata
versions, blank identity fields, missing schemas, unknown state codecs,
unsupported epoch policies, generated Rust ABI versions outside the phase-0
contract, view id or spec hash mismatches, schema mismatches, and multi-input or
multi-output standing-view shapes. Phase 0 supports one input relation and one
output relation only.

The relation schemas are SQL-facing contracts. They describe relation ids,
relation names, typed columns, nullability, and primary keys. They are not the
DataFusion `DeltaBatch` ad hoc query table with `key_json`, `value_json`, and
`weight`.

## Trust Boundary

Generated Rust is trusted only as a build/release artifact. A future release
pipeline can compile Feldera-generated Rust into a Velorix engine package after
reviewing the metadata, pinning compiler identity, and verifying the artifact
hash. The running Velorix process should select among already-built,
release-trusted artifacts; it should not compile or dynamically load arbitrary
generated source from object storage.

Object storage manifests remain durable data and progress authority. They may
reference a validated artifact id/hash, but they cannot make code executable by
themselves. This prevents a checkpoint or manifest write from becoming a code
loading path.

## Runtime Direction

DataFusion remains the current ad hoc SQL/query engine over Arrow-backed
`DeltaBatch` input. It is for query surfaces where callers submit SQL against
the current in-memory `input` table.

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
