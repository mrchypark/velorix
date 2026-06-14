use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::relation::{
    ArrowPhysicalTypeV1, RelationColumnV1, RelationOperationV1, RelationSchemaError,
    RelationSemanticRoleV1, VelorixLogicalTypeV1, VelorixRelationCatalogV1,
};

pub const FELDERA_ARTIFACT_METADATA_VERSION_V1: u32 = 1;
pub const FELDERA_ARTIFACT_METADATA_VERSION: u32 = 2;
pub const FELDERA_RELEASE_ARTIFACT_PROVENANCE_VERSION: u16 = 1;
pub const FELDERA_SPEC_HASH_PREFIX: &str = "velorix-feldera-spec-sha256-v1";
pub const FELDERA_COMPILE_REQUEST_HASH_PREFIX: &str = "velorix-feldera-compile-request-sha256-v1";
pub const SUPPORTED_STATE_CODEC: &str = "feldera-dbsp-state-v1";
pub const SUPPORTED_EPOCH_POLICY: &str = "monotonic-logical-epoch-v1";
pub const SUPPORTED_GENERATED_RUST_ABI_VERSION: &str = "feldera-generated-rust-abi-v1";
pub const MAX_RELATION_COLUMNS: usize = 1024;
pub const MAX_SQL_TYPE_NESTING_DEPTH: usize = 16;
pub const MAX_SQL_TYPE_NODES: usize = 4096;
pub const MAX_SQL_STRUCT_FIELDS: usize = 256;
pub const MAX_SQL_STRUCT_FIELD_NAME_BYTES: usize = 128;
pub const MAX_SQL_TIMEZONE_BYTES: usize = 128;
pub const MAX_FELDERA_UDF_RUST_BYTES: usize = 1_048_576;
pub const MAX_FELDERA_UDF_TOML_BYTES: usize = 65_536;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StandingViewSpec {
    pub view_id: String,
    pub sql: String,
    pub dialect: SqlDialect,
    pub source_kind: SqlSourceKind,
    #[serde(default, skip_serializing_if = "FelderaRustExtensionV1::is_empty")]
    pub rust_extension: FelderaRustExtensionV1,
    pub input_relations: Vec<RelationSchema>,
    pub output_relations: Vec<RelationSchema>,
    pub shape: StandingViewShape,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaCompileRequestV1 {
    pub view_id: String,
    pub sql: String,
    pub dialect: SqlDialect,
    pub source_kind: SqlSourceKind,
    #[serde(default, skip_serializing_if = "FelderaRustExtensionV1::is_empty")]
    pub rust_extension: FelderaRustExtensionV1,
    pub input_relations: Vec<RelationSchema>,
    pub output_contract: OutputSchemaContract,
    pub shape: StandingViewShape,
}

impl FelderaCompileRequestV1 {
    pub fn infer_output_from_standing_view_spec(spec: &StandingViewSpec) -> Self {
        let mut shape = spec.shape.clone();
        if spec.source_kind == SqlSourceKind::FelderaProgram {
            shape.multi_output = true;
        }
        Self {
            view_id: spec.view_id.clone(),
            sql: spec.sql.clone(),
            dialect: spec.dialect.clone(),
            source_kind: spec.source_kind.clone(),
            rust_extension: spec.rust_extension.clone(),
            input_relations: spec.input_relations.clone(),
            output_contract: OutputSchemaContract::Infer,
            shape,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaRustExtensionV1 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub udf_rust: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub udf_toml: Option<String>,
}

impl FelderaRustExtensionV1 {
    pub fn is_empty(&self) -> bool {
        self.udf_rust.is_none() && self.udf_toml.is_none()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputSchemaContract {
    Infer,
    MustMatch {
        output_relations: Vec<RelationSchema>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum SqlDialect {
    FelderaSql,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum SqlSourceKind {
    StandingView,
    FelderaProgram,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StandingViewShape {
    pub is_materialized: bool,
    pub multi_input: bool,
    pub multi_output: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationSchema {
    pub relation_id: String,
    pub relation_name: String,
    pub relation_version: String,
    pub schema_fingerprint: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: SqlDataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SqlDataType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Decimal {
        precision: u8,
        scale: u8,
    },
    Char {
        length: Option<u32>,
    },
    Utf8,
    Binary {
        length: u32,
    },
    Varbinary,
    Time,
    Date,
    Timestamp {
        timezone: Option<String>,
    },
    Interval {
        unit: SqlIntervalUnit,
    },
    Array {
        element_type: Box<SqlDataType>,
    },
    Struct {
        fields: Vec<SqlStructField>,
    },
    Map {
        key_type: Box<SqlDataType>,
        value_type: Box<SqlDataType>,
    },
    Null,
    Uuid,
    Json,
    Geometry,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum SqlIntervalUnit {
    Day,
    DayToHour,
    DayToMinute,
    DayToSecond,
    Hour,
    HourToMinute,
    HourToSecond,
    Minute,
    MinuteToSecond,
    Month,
    Second,
    Year,
    YearToMonth,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlStructField {
    pub name: String,
    pub data_type: SqlDataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaCompileArtifactMetadata {
    pub metadata_version: u32,
    pub view_id: String,
    pub spec_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compile_request_hash: Option<String>,
    pub artifact_id: String,
    pub artifact_hash: String,
    pub compiler: FelderaCompilerIdentity,
    pub generated_rust: GeneratedRustIdentity,
    pub input_schemas: Vec<RelationSchema>,
    pub output_schemas: Vec<RelationSchema>,
    pub state_codec: String,
    pub state_schema_version: u32,
    pub epoch_policy: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaPipelineManagerRuntimeDeployment {
    pub pipeline_name: String,
    pub mode: FelderaPipelineManagerRuntimeDeploymentMode,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FelderaPipelineManagerRuntimeDeploymentMode {
    LocalVolatile,
    ExternalManaged,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaCompilerIdentity {
    pub name: String,
    pub version: String,
    pub source: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedRustIdentity {
    pub abi_version: String,
    pub crate_name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaReleaseArtifactProvenanceV1 {
    pub schema_version: u16,
    pub release: FelderaReleaseIdentityV1,
    pub build: FelderaReleaseBuildIdentityV1,
    pub provenance: FelderaReleaseProvenanceIdentityV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaReleaseIdentityV1 {
    pub release_id: String,
    pub release_version: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaReleaseBuildIdentityV1 {
    pub build_id: String,
    pub builder_id: String,
    pub artifact_id: String,
    pub artifact_hash: String,
    pub spec_hash: String,
    pub generated_rust: GeneratedRustIdentity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaReleaseProvenanceIdentityV1 {
    pub source_repository: String,
    pub source_revision: String,
    pub compiler_name: String,
    pub compiler_version: String,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum FelderaArtifactError {
    #[error("unsupported Feldera artifact metadata version: {version}")]
    UnsupportedMetadataVersion { version: u32 },
    #[error("unsupported Feldera release artifact provenance version: {version}")]
    UnsupportedReleaseProvenanceVersion { version: u16 },
    #[error("missing Feldera artifact identity field: {field}")]
    MissingIdentityField { field: &'static str },
    #[error("missing Feldera release artifact provenance field: {field}")]
    MissingReleaseProvenanceField { field: &'static str },
    #[error("Feldera release artifact provenance mismatch: {field}")]
    MismatchedReleaseProvenanceField { field: &'static str },
    #[error("missing Feldera artifact schema field: {field}")]
    MissingSchema { field: &'static str },
    #[error("unsupported Feldera artifact state codec: {codec}")]
    UnsupportedStateCodec { codec: String },
    #[error("unsupported Feldera artifact epoch policy: {epoch_policy}")]
    UnsupportedEpochPolicy { epoch_policy: String },
    #[error("unsupported Feldera generated Rust ABI version: {abi_version}")]
    UnsupportedGeneratedRustAbi { abi_version: String },
    #[error("Feldera artifact view id mismatch: spec={spec_view_id}, artifact={artifact_view_id}")]
    MismatchedViewId {
        spec_view_id: String,
        artifact_view_id: String,
    },
    #[error("Feldera artifact spec hash mismatch: expected={expected}, actual={actual}")]
    MismatchedSpecHash { expected: String, actual: String },
    #[error(
        "Feldera artifact compile request hash mismatch: expected={expected}, actual={actual}"
    )]
    MismatchedCompileRequestHash { expected: String, actual: String },
    #[error("Feldera artifact bytes hash mismatch: expected={expected}, actual={actual}")]
    MismatchedArtifactHash { expected: String, actual: String },
    #[error("unsupported Feldera standing view shape: {shape}")]
    UnsupportedShape { shape: &'static str },
    #[error("Feldera artifact schema mismatch: {field}")]
    SchemaMismatch { field: &'static str },
    #[error("Feldera artifact schema fingerprint mismatch: {field}")]
    SchemaFingerprintMismatch { field: &'static str },
    #[error("invalid Feldera relation schema: {field}")]
    InvalidRelationSchema { field: &'static str },
    #[error("unsupported Feldera SQL table schema type at {field}: {data_type}")]
    UnsupportedTableSchemaType {
        field: &'static str,
        data_type: String,
    },
    #[error("invalid Feldera artifact hash: {field}")]
    InvalidArtifactHash { field: &'static str },
    #[error("invalid Feldera program info: {reason}")]
    InvalidProgramInfo { reason: String },
    #[error(transparent)]
    Serialization(#[from] SerdeJsonError),
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct SerdeJsonError(#[from] serde_json::Error);

impl PartialEq for SerdeJsonError {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_string() == other.0.to_string()
    }
}

impl Eq for SerdeJsonError {}

pub fn validate_feldera_compile_artifact(
    spec: &StandingViewSpec,
    artifact: &FelderaCompileArtifactMetadata,
) -> Result<(), FelderaArtifactError> {
    if artifact.metadata_version != FELDERA_ARTIFACT_METADATA_VERSION_V1
        && artifact.metadata_version != FELDERA_ARTIFACT_METADATA_VERSION
    {
        return Err(FelderaArtifactError::UnsupportedMetadataVersion {
            version: artifact.metadata_version,
        });
    }

    require_non_empty("view_id", &artifact.view_id)?;
    require_non_empty("spec_hash", &artifact.spec_hash)?;
    if artifact.metadata_version == FELDERA_ARTIFACT_METADATA_VERSION {
        let Some(compile_request_hash) = &artifact.compile_request_hash else {
            return Err(FelderaArtifactError::MissingIdentityField {
                field: "compile_request_hash",
            });
        };
        require_non_empty("compile_request_hash", compile_request_hash)?;
        validate_compile_request_hash("compile_request_hash", compile_request_hash)?;
    } else if let Some(compile_request_hash) = &artifact.compile_request_hash {
        require_non_empty("compile_request_hash", compile_request_hash)?;
        validate_compile_request_hash("compile_request_hash", compile_request_hash)?;
    }
    require_non_empty("artifact_id", &artifact.artifact_id)?;
    require_non_empty("artifact_hash", &artifact.artifact_hash)?;
    validate_artifact_hash("artifact_hash", &artifact.artifact_hash)?;
    require_non_empty("compiler.name", &artifact.compiler.name)?;
    require_non_empty("compiler.version", &artifact.compiler.version)?;
    require_non_empty("compiler.source", &artifact.compiler.source)?;
    require_non_empty(
        "generated_rust.abi_version",
        &artifact.generated_rust.abi_version,
    )?;
    require_non_empty(
        "generated_rust.crate_name",
        &artifact.generated_rust.crate_name,
    )?;
    if artifact.state_schema_version == 0 {
        return Err(FelderaArtifactError::MissingIdentityField {
            field: "state_schema_version",
        });
    }

    validate_materialized_standing_view_spec(spec)?;
    if artifact.input_schemas.is_empty() {
        return Err(FelderaArtifactError::MissingSchema {
            field: "input_schemas",
        });
    }
    if artifact.output_schemas.is_empty() {
        return Err(FelderaArtifactError::MissingSchema {
            field: "output_schemas",
        });
    }

    if spec.view_id != artifact.view_id {
        return Err(FelderaArtifactError::MismatchedViewId {
            spec_view_id: spec.view_id.clone(),
            artifact_view_id: artifact.view_id.clone(),
        });
    }

    let expected_spec_hash = feldera_spec_hash(spec)?;
    if expected_spec_hash != artifact.spec_hash {
        return Err(FelderaArtifactError::MismatchedSpecHash {
            expected: expected_spec_hash,
            actual: artifact.spec_hash.clone(),
        });
    }

    if artifact.state_codec != SUPPORTED_STATE_CODEC {
        return Err(FelderaArtifactError::UnsupportedStateCodec {
            codec: artifact.state_codec.clone(),
        });
    }
    if artifact.epoch_policy != SUPPORTED_EPOCH_POLICY {
        return Err(FelderaArtifactError::UnsupportedEpochPolicy {
            epoch_policy: artifact.epoch_policy.clone(),
        });
    }
    if artifact.generated_rust.abi_version != SUPPORTED_GENERATED_RUST_ABI_VERSION {
        return Err(FelderaArtifactError::UnsupportedGeneratedRustAbi {
            abi_version: artifact.generated_rust.abi_version.clone(),
        });
    }

    validate_relation_schemas(&spec.input_relations)?;
    validate_relation_schemas(&spec.output_relations)?;
    validate_relation_schemas(&artifact.input_schemas)?;
    validate_relation_schemas(&artifact.output_schemas)?;

    validate_relation_identity_matches(
        "input_schemas",
        &spec.input_relations[0],
        &artifact.input_schemas[0],
    )?;
    if spec.shape.multi_output != (spec.output_relations.len() > 1) {
        return Err(FelderaArtifactError::UnsupportedShape {
            shape: "spec.shape.multi_output",
        });
    }

    if spec.input_relations != artifact.input_schemas {
        return Err(FelderaArtifactError::SchemaMismatch {
            field: "input_schemas",
        });
    }
    if spec.output_relations != artifact.output_schemas {
        return Err(FelderaArtifactError::SchemaMismatch {
            field: "output_schemas",
        });
    }

    Ok(())
}

pub fn validate_feldera_compile_artifact_for_compile_request(
    spec: &StandingViewSpec,
    artifact: &FelderaCompileArtifactMetadata,
    compile_request_hash: &str,
) -> Result<(), FelderaArtifactError> {
    validate_feldera_compile_artifact(spec, artifact)?;
    validate_compile_request_hash("compile_request_hash", compile_request_hash)?;
    match &artifact.compile_request_hash {
        Some(actual) if actual == compile_request_hash => Ok(()),
        Some(actual) => Err(FelderaArtifactError::MismatchedCompileRequestHash {
            expected: compile_request_hash.to_string(),
            actual: actual.clone(),
        }),
        None => Err(FelderaArtifactError::MissingIdentityField {
            field: "compile_request_hash",
        }),
    }
}

pub fn validate_materialized_standing_view_spec(
    spec: &StandingViewSpec,
) -> Result<(), FelderaArtifactError> {
    require_non_empty("spec.view_id", &spec.view_id)?;
    require_non_empty("spec.sql", &spec.sql)?;

    if spec.input_relations.is_empty() {
        return Err(FelderaArtifactError::MissingSchema {
            field: "spec.input_relations",
        });
    }
    if spec.output_relations.is_empty() {
        return Err(FelderaArtifactError::MissingSchema {
            field: "spec.output_relations",
        });
    }
    if !spec.shape.is_materialized {
        return Err(FelderaArtifactError::UnsupportedShape {
            shape: "spec.shape.is_materialized",
        });
    }
    if spec.shape.multi_output != (spec.output_relations.len() > 1) {
        return Err(FelderaArtifactError::UnsupportedShape {
            shape: "spec.shape.multi_output",
        });
    }
    validate_feldera_rust_extension(
        "spec.rust_extension",
        &spec.source_kind,
        &spec.rust_extension,
    )?;

    validate_relation_schemas(&spec.input_relations)?;
    validate_relation_schemas(&spec.output_relations)?;

    Ok(())
}

pub fn validate_feldera_compile_request(
    request: &FelderaCompileRequestV1,
) -> Result<(), FelderaArtifactError> {
    require_non_empty("compile_request.view_id", &request.view_id)?;
    require_non_empty("compile_request.sql", &request.sql)?;
    if request.input_relations.is_empty() {
        return Err(FelderaArtifactError::MissingSchema {
            field: "compile_request.input_relations",
        });
    }
    if !request.shape.is_materialized {
        return Err(FelderaArtifactError::UnsupportedShape {
            shape: "compile_request.shape.is_materialized",
        });
    }
    if request.shape.multi_input != (request.input_relations.len() > 1) {
        return Err(FelderaArtifactError::UnsupportedShape {
            shape: "compile_request.shape.multi_input",
        });
    }
    validate_feldera_rust_extension(
        "compile_request.rust_extension",
        &request.source_kind,
        &request.rust_extension,
    )?;

    validate_relation_schemas(&request.input_relations)?;
    if let OutputSchemaContract::MustMatch { output_relations } = &request.output_contract {
        if output_relations.is_empty() {
            return Err(FelderaArtifactError::MissingSchema {
                field: "compile_request.output_contract.output_relations",
            });
        }
        if request.shape.multi_output != (output_relations.len() > 1) {
            return Err(FelderaArtifactError::UnsupportedShape {
                shape: "compile_request.shape.multi_output",
            });
        }
        validate_relation_schemas(output_relations)?;
    }

    Ok(())
}

fn validate_feldera_rust_extension(
    field_prefix: &'static str,
    source_kind: &SqlSourceKind,
    rust_extension: &FelderaRustExtensionV1,
) -> Result<(), FelderaArtifactError> {
    if rust_extension.is_empty() {
        return Ok(());
    }
    if source_kind != &SqlSourceKind::FelderaProgram {
        return Err(FelderaArtifactError::UnsupportedShape {
            shape: "rust_extension.source_kind",
        });
    }
    if let Some(udf_rust) = &rust_extension.udf_rust {
        validate_feldera_extension_payload(
            field_prefix,
            "udf_rust",
            udf_rust,
            MAX_FELDERA_UDF_RUST_BYTES,
        )?;
    }
    if let Some(udf_toml) = &rust_extension.udf_toml {
        validate_feldera_extension_payload(
            field_prefix,
            "udf_toml",
            udf_toml,
            MAX_FELDERA_UDF_TOML_BYTES,
        )?;
        validate_feldera_udf_toml_has_no_external_dependencies(udf_toml)?;
    }
    Ok(())
}

fn validate_feldera_udf_toml_has_no_external_dependencies(
    udf_toml: &str,
) -> Result<(), FelderaArtifactError> {
    for line in udf_toml.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed == "[dependencies]" {
            continue;
        }
        return Err(FelderaArtifactError::UnsupportedShape {
            shape: "rust_extension.udf_toml.external_dependencies",
        });
    }
    Ok(())
}

fn validate_feldera_extension_payload(
    field_prefix: &'static str,
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), FelderaArtifactError> {
    if value.trim().is_empty() {
        return Err(FelderaArtifactError::MissingIdentityField {
            field: if field == "udf_rust" {
                "rust_extension.udf_rust"
            } else {
                "rust_extension.udf_toml"
            },
        });
    }
    if value.len() > max_bytes {
        return Err(FelderaArtifactError::InvalidRelationSchema {
            field: if field == "udf_rust" {
                "rust_extension.udf_rust"
            } else {
                "rust_extension.udf_toml"
            },
        });
    }
    let _ = field_prefix;
    Ok(())
}

pub fn feldera_sql_program_for_compile_request(
    request: &FelderaCompileRequestV1,
) -> Result<String, FelderaArtifactError> {
    validate_feldera_compile_request(request)?;

    let mut statements = Vec::with_capacity(request.input_relations.len() + 1);
    for relation in &request.input_relations {
        statements.push(feldera_create_table_statement(relation)?);
    }
    match &request.source_kind {
        SqlSourceKind::StandingView => {
            statements.push(feldera_create_materialized_view_statement(
                &request.view_id,
                &request.sql,
            )?);
        }
        SqlSourceKind::FelderaProgram => {
            statements.push(request.sql.trim().to_string());
        }
    }

    Ok(statements.join("\n\n"))
}

pub fn standing_view_spec_for_compile_request(
    request: &FelderaCompileRequestV1,
) -> StandingViewSpec {
    StandingViewSpec {
        view_id: request.view_id.clone(),
        sql: request.sql.clone(),
        dialect: request.dialect.clone(),
        source_kind: request.source_kind.clone(),
        rust_extension: request.rust_extension.clone(),
        input_relations: request.input_relations.clone(),
        output_relations: match &request.output_contract {
            OutputSchemaContract::Infer => Vec::new(),
            OutputSchemaContract::MustMatch { output_relations } => output_relations.clone(),
        },
        shape: request.shape.clone(),
    }
}

pub const FELDERA_PIPELINE_NAME_MAX_CHARS: usize = 63;

pub fn feldera_pipeline_name_for_parts(view_id: &str, compile_request_hash: &str) -> String {
    let hash_tail = compile_request_hash
        .rsplit_once(':')
        .map(|(_, tail)| tail)
        .unwrap_or(compile_request_hash);
    let hash_tail = hash_tail.chars().take(16).collect::<String>();
    let view = view_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    let view = if view.is_empty() {
        "view".to_string()
    } else {
        view
    };
    let max_view_chars =
        FELDERA_PIPELINE_NAME_MAX_CHARS.saturating_sub("velorix--".len() + hash_tail.len());
    let view = view.chars().take(max_view_chars).collect::<String>();
    format!("velorix-{view}-{hash_tail}")
}

pub fn feldera_pipeline_manager_sql_compile_request(
    request: &FelderaCompileRequestV1,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<FelderaCompileRequestV1, FelderaArtifactError> {
    let mut weight_column_names = BTreeMap::new();
    for catalog in catalogs {
        catalog.validate().map_err(catalog_relation_error)?;
        let relation = &catalog.relation_schema;
        let weight_column = relation
            .columns
            .iter()
            .find(|column| column.column_id == relation.weight_column_id)
            .ok_or(FelderaArtifactError::InvalidRelationSchema {
                field: "weight_column_id",
            })?;
        if weight_column.semantic_role != RelationSemanticRoleV1::Weight {
            return Err(FelderaArtifactError::InvalidRelationSchema {
                field: "feldera_pipeline_manager.weight_column.semantic_role",
            });
        }
        if !matches!(weight_column.logical_type, VelorixLogicalTypeV1::Int64)
            || !matches!(
                weight_column.physical_arrow_type,
                ArrowPhysicalTypeV1::Int64
            )
        {
            return Err(FelderaArtifactError::InvalidRelationSchema {
                field: "feldera_pipeline_manager.weight_column.type",
            });
        }
        if weight_column.nullable {
            return Err(FelderaArtifactError::InvalidRelationSchema {
                field: "feldera_pipeline_manager.weight_column.nullable",
            });
        }
        if relation
            .primary_key_column_ids
            .iter()
            .any(|column_id| column_id == &relation.weight_column_id)
        {
            return Err(FelderaArtifactError::InvalidRelationSchema {
                field: "feldera_pipeline_manager.weight_column.primary_key",
            });
        }
        if !relation
            .allowed_operations
            .iter()
            .any(|operation| operation == &RelationOperationV1::Insert)
        {
            return Err(FelderaArtifactError::InvalidRelationSchema {
                field: "feldera_pipeline_manager.allowed_operations.insert",
            });
        }
        weight_column_names.insert(relation.relation_id.clone(), weight_column.name.clone());
    }

    let mut request = request.clone();
    for input in &mut request.input_relations {
        let weight_column = weight_column_names.get(&input.relation_id).ok_or_else(|| {
            FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "missing Feldera weight column metadata for input relation `{}`",
                    input.relation_id
                ),
            }
        })?;
        if input
            .primary_key
            .iter()
            .any(|column| column == weight_column)
        {
            return Err(FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "Feldera pipeline-manager compile request does not allow weight column `{weight_column}` in primary key for relation `{}`",
                    input.relation_id
                ),
            });
        }
        input.columns.retain(|column| column.name != *weight_column);
        if input.columns.is_empty() {
            return Err(FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "Feldera pipeline-manager compile request has no data columns after stripping weight column `{weight_column}` for relation `{}`",
                    input.relation_id
                ),
            });
        }
    }
    Ok(request)
}

pub fn feldera_output_schemas_from_program_info(
    view_id: &str,
    program_version: u64,
    program_info: Option<&Value>,
    multi_output: bool,
) -> Result<Vec<RelationSchema>, FelderaArtifactError> {
    let program_info = program_info.ok_or_else(|| FelderaArtifactError::InvalidProgramInfo {
        reason: "Feldera compiled response is missing program_info".to_string(),
    })?;
    let outputs = program_info
        .pointer("/schema/outputs")
        .and_then(Value::as_array)
        .ok_or_else(|| FelderaArtifactError::InvalidProgramInfo {
            reason: "Feldera program_info is missing schema.outputs".to_string(),
        })?;
    if outputs.is_empty() {
        return Err(FelderaArtifactError::InvalidProgramInfo {
            reason: "Feldera compiled program does not contain output views".to_string(),
        });
    }
    let mut output_names = BTreeSet::new();
    for output in outputs {
        let output_name = feldera_relation_name(output).ok_or_else(|| {
            FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "Feldera compiled program for `{view_id}` contains an output without a name"
                ),
            }
        })?;
        if !output_names.insert(feldera_relation_name_key(output_name, output)) {
            return Err(FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "Feldera compiled program contains duplicate output view `{output_name}`"
                ),
            });
        }
    }
    if !multi_output {
        let output = outputs
            .iter()
            .find(|output| feldera_relation_name_matches(view_id, output))
            .ok_or_else(|| FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "Feldera compiled program does not contain output view `{view_id}`"
                ),
            })?;
        return Ok(vec![feldera_output_schema_from_program_output(
            view_id,
            program_version,
            output,
        )?]);
    }
    let materialized_outputs = outputs
        .iter()
        .filter(|output| feldera_relation_is_materialized(output))
        .map(|output| feldera_output_schema_from_program_output(view_id, program_version, output))
        .collect::<Result<Vec<_>, _>>()?;
    if materialized_outputs.is_empty() {
        return Err(FelderaArtifactError::InvalidProgramInfo {
            reason: "Feldera compiled program does not contain materialized output views"
                .to_string(),
        });
    }
    Ok(materialized_outputs)
}

pub fn catalog_input_relation_schema(
    catalog: &VelorixRelationCatalogV1,
) -> Result<RelationSchema, FelderaArtifactError> {
    catalog.validate().map_err(catalog_relation_error)?;

    Ok(RelationSchema {
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_name: catalog.relation_schema.relation_name.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        columns: catalog
            .relation_schema
            .columns
            .iter()
            .map(catalog_column_schema)
            .collect::<Result<Vec<_>, FelderaArtifactError>>()?,
        primary_key: catalog_primary_key_columns(catalog)?,
    })
}

fn feldera_output_schema_from_program_output(
    view_id: &str,
    program_version: u64,
    output: &Value,
) -> Result<RelationSchema, FelderaArtifactError> {
    let output_name =
        feldera_relation_name(output).ok_or_else(|| FelderaArtifactError::InvalidProgramInfo {
            reason: format!(
                "Feldera compiled program for `{view_id}` contains an output without a name"
            ),
        })?;
    if !feldera_relation_is_materialized(output) {
        return Err(FelderaArtifactError::InvalidProgramInfo {
            reason: format!("Feldera output view `{output_name}` is not materialized"),
        });
    }
    let unmanaged_properties = feldera_relation_unmanaged_io_properties(output);
    if !unmanaged_properties.is_empty() {
        return Err(FelderaArtifactError::InvalidProgramInfo {
            reason: format!(
                "Feldera output view `{output_name}` contains unmanaged connector/external IO properties: {}",
                unmanaged_properties.join(", ")
            ),
        });
    }
    let fields = output
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| FelderaArtifactError::InvalidProgramInfo {
            reason: format!("Feldera output view `{output_name}` is missing fields"),
        })?;
    let columns = fields
        .iter()
        .enumerate()
        .map(|(index, field)| feldera_column_schema_from_field(output_name, index, field))
        .collect::<Result<Vec<_>, _>>()?;
    if columns.is_empty() {
        return Err(FelderaArtifactError::InvalidProgramInfo {
            reason: format!("Feldera output view `{output_name}` has no columns"),
        });
    }
    validate_feldera_program_relation_column_names(
        "output view",
        output_name,
        output,
        fields,
        &columns,
    )?;
    let primary_key = feldera_program_relation_primary_key_columns(
        "output view",
        output_name,
        output,
        fields,
        &columns,
    )?
    .unwrap_or_default();
    let schema_fingerprint = feldera_compiled_output_schema_fingerprint(
        output_name,
        program_version,
        &columns,
        &primary_key,
    )?;
    Ok(RelationSchema {
        relation_id: output_name.to_string(),
        relation_name: output_name.to_string(),
        relation_version: format!("feldera-program-v{program_version}"),
        schema_fingerprint,
        columns,
        primary_key,
    })
}

fn feldera_relation_unmanaged_io_properties(relation: &Value) -> Vec<String> {
    let mut properties = Vec::new();
    for key in [
        "connector",
        "connectors",
        "connector_config",
        "input_connectors",
        "output_connectors",
        "transport",
        "format",
    ] {
        if relation
            .get(key)
            .is_some_and(|value| !value.is_null() && value != &json!([]) && value != &json!({}))
        {
            properties.push(key.to_string());
        }
    }
    if let Some(object) = relation.get("properties").and_then(Value::as_object) {
        properties.extend(
            object
                .keys()
                .map(|key| format!("properties.{key}"))
                .collect::<Vec<_>>(),
        );
    }
    properties
}

fn feldera_relation_is_materialized(relation: &Value) -> bool {
    !relation
        .get("materialized")
        .and_then(Value::as_bool)
        .is_some_and(|materialized| !materialized)
}

fn validate_feldera_program_relation_column_names(
    relation_kind: &str,
    output_name: &str,
    output: &Value,
    fields: &[Value],
    columns: &[ColumnSchema],
) -> Result<(), FelderaArtifactError> {
    let mut seen: Vec<(&str, bool)> = Vec::new();
    for (field, column) in fields.iter().zip(columns) {
        if column.name.trim().is_empty() {
            return Err(FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "Feldera {relation_kind} `{output_name}` contains a blank field name"
                ),
            });
        }
        let case_insensitive = feldera_identifier_case_insensitive(output, field);
        if seen.iter().any(|(seen_name, seen_case_insensitive)| {
            feldera_identifiers_conflict(
                seen_name,
                *seen_case_insensitive,
                &column.name,
                case_insensitive,
            )
        }) {
            return Err(FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "Feldera {relation_kind} `{output_name}` contains duplicate field `{}`",
                    column.name
                ),
            });
        }
        seen.push((&column.name, case_insensitive));
    }
    Ok(())
}

fn feldera_program_relation_primary_key_columns(
    relation_kind: &str,
    output_name: &str,
    output: &Value,
    fields: &[Value],
    columns: &[ColumnSchema],
) -> Result<Option<Vec<String>>, FelderaArtifactError> {
    let Some(primary_key) = output.get("primary_key") else {
        return Ok(None);
    };
    let primary_key =
        primary_key
            .as_array()
            .ok_or_else(|| FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "Feldera {relation_kind} `{output_name}` primary_key must be an array"
                ),
            })?;
    let mut keys = Vec::with_capacity(primary_key.len());
    let mut seen: Vec<(&str, bool)> = Vec::new();
    let key_case_insensitive = feldera_relation_identifier_case_insensitive(output);
    for (index, key) in primary_key.iter().enumerate() {
        let key = key
            .as_str()
            .ok_or_else(|| FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "Feldera {relation_kind} `{output_name}` primary_key entry {index} must be a string"
                ),
            })?;
        if key.trim().is_empty() {
            return Err(FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "Feldera {relation_kind} `{output_name}` contains a blank primary_key entry"
                ),
            });
        }
        if seen.iter().any(|(seen_key, seen_case_insensitive)| {
            feldera_identifiers_conflict(
                seen_key,
                *seen_case_insensitive,
                key,
                key_case_insensitive,
            )
        }) {
            return Err(FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "Feldera {relation_kind} `{output_name}` contains duplicate primary_key entry `{key}`"
                ),
            });
        }
        let matching_column = fields
            .iter()
            .zip(columns)
            .find(|(field, column)| {
                feldera_identifiers_conflict(
                    key,
                    key_case_insensitive,
                    &column.name,
                    feldera_identifier_case_insensitive(output, field),
                )
            })
            .map(|(_, column)| column.name.clone())
            .ok_or_else(|| FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "Feldera {relation_kind} `{output_name}` primary_key entry `{key}` does not reference a field"
                ),
            })?;
        keys.push(matching_column);
        seen.push((key, key_case_insensitive));
    }
    Ok(Some(keys))
}

fn feldera_relation_identifier_case_insensitive(relation: &Value) -> bool {
    relation
        .get("case_sensitive")
        .and_then(Value::as_bool)
        .is_some_and(|case_sensitive| !case_sensitive)
}

fn feldera_identifier_case_insensitive(relation: &Value, identifier: &Value) -> bool {
    identifier
        .get("case_sensitive")
        .and_then(Value::as_bool)
        .map(|case_sensitive| !case_sensitive)
        .unwrap_or_else(|| feldera_relation_identifier_case_insensitive(relation))
}

fn feldera_identifiers_conflict(
    left: &str,
    left_case_insensitive: bool,
    right: &str,
    right_case_insensitive: bool,
) -> bool {
    if left_case_insensitive || right_case_insensitive {
        left.eq_ignore_ascii_case(right)
    } else {
        left == right
    }
}

fn feldera_relation_name(relation: &Value) -> Option<&str> {
    relation.get("name").and_then(Value::as_str)
}

fn feldera_relation_name_key(name: &str, relation: &Value) -> String {
    if feldera_relation_identifier_case_insensitive(relation) {
        name.to_ascii_lowercase()
    } else {
        name.to_string()
    }
}

fn feldera_relation_name_matches(expected: &str, relation: &Value) -> bool {
    let Some(actual) = feldera_relation_name(relation) else {
        return false;
    };
    if feldera_relation_identifier_case_insensitive(relation) {
        actual.eq_ignore_ascii_case(expected)
    } else {
        actual == expected
    }
}

fn feldera_column_schema_from_field(
    view_id: &str,
    index: usize,
    field: &Value,
) -> Result<ColumnSchema, FelderaArtifactError> {
    let name = field
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| FelderaArtifactError::InvalidProgramInfo {
            reason: format!("Feldera output view `{view_id}` field {index} is missing name"),
        })?
        .to_string();
    let columntype = field.get("columntype").unwrap_or(field);
    let (data_type, nullable) =
        feldera_sql_data_type_from_column_type(columntype).map_err(|error| {
            FelderaArtifactError::InvalidProgramInfo {
                reason: format!(
                    "Feldera output view `{view_id}` field `{name}` has unsupported type: {error}"
                ),
            }
        })?;
    Ok(ColumnSchema {
        name,
        data_type,
        nullable,
    })
}

fn feldera_sql_data_type_from_column_type(value: &Value) -> Result<(SqlDataType, bool), String> {
    match value {
        Value::String(name) => Ok((
            feldera_sql_data_type_from_name(name, None, None, value)?,
            false,
        )),
        Value::Object(object) => {
            let nullable = object
                .get("nullable")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let type_name = object
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_else(|| {
                    if object.get("fields").is_some() {
                        "STRUCT"
                    } else {
                        ""
                    }
                });
            if type_name.is_empty() {
                return Err("missing type".to_string());
            }
            let precision = object.get("precision").and_then(Value::as_i64);
            let scale = object.get("scale").and_then(Value::as_i64);
            Ok((
                feldera_sql_data_type_from_name(type_name, precision, scale, value)?,
                nullable,
            ))
        }
        _ => Err("column type must be an object or string".to_string()),
    }
}

fn feldera_sql_data_type_from_name(
    raw_name: &str,
    precision: Option<i64>,
    scale: Option<i64>,
    value: &Value,
) -> Result<SqlDataType, String> {
    let name = raw_name.trim().to_ascii_uppercase();
    match name.as_str() {
        "BOOLEAN" | "BOOL" => Ok(SqlDataType::Bool),
        "TINYINT" => Ok(SqlDataType::Int8),
        "SMALLINT" | "INT2" => Ok(SqlDataType::Int16),
        "INTEGER" | "INT" | "SIGNED" | "INT4" => Ok(SqlDataType::Int32),
        "BIGINT" | "INT8" | "INT64" => Ok(SqlDataType::Int64),
        "UTINYINT" | "TINYINT UNSIGNED" => Ok(SqlDataType::UInt8),
        "USMALLINT" | "SMALLINT UNSIGNED" => Ok(SqlDataType::UInt16),
        "UINTEGER" | "INTEGER UNSIGNED" | "INT UNSIGNED" | "UNSIGNED" => Ok(SqlDataType::UInt32),
        "UBIGINT" | "BIGINT UNSIGNED" => Ok(SqlDataType::UInt64),
        "REAL" | "FLOAT4" | "FLOAT32" => Ok(SqlDataType::Float32),
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" | "FLOAT64" => Ok(SqlDataType::Float64),
        "DECIMAL" | "DEC" | "NUMERIC" | "NUMBER" => {
            let precision = u8_from_i64("precision", precision.unwrap_or(38))?;
            let scale = u8_from_i64("scale", scale.unwrap_or(0))?;
            Ok(SqlDataType::Decimal { precision, scale })
        }
        "CHAR" | "CHARACTER" => {
            let length = match precision {
                Some(value) if value > 0 => Some(u32_from_i64("precision", value)?),
                _ => None,
            };
            Ok(SqlDataType::Char { length })
        }
        "VARCHAR" | "CHARACTER VARYING" | "STRING" | "TEXT" => Ok(SqlDataType::Utf8),
        "BINARY" => {
            let length = u32_from_i64("precision", precision.unwrap_or(1))?;
            Ok(SqlDataType::Binary { length })
        }
        "VARBINARY" | "BINARY VARYING" | "BYTEA" => Ok(SqlDataType::Varbinary),
        "TIME" => Ok(SqlDataType::Time),
        "DATE" => Ok(SqlDataType::Date),
        "TIMESTAMP" | "DATETIME" => Ok(SqlDataType::Timestamp { timezone: None }),
        "TIMESTAMP_TZ" => Ok(SqlDataType::Timestamp {
            timezone: Some("UTC".to_string()),
        }),
        "ARRAY" => {
            let component = value
                .get("component")
                .ok_or_else(|| "ARRAY is missing component".to_string())?;
            let (element_type, _) = feldera_sql_data_type_from_column_type(component)?;
            Ok(SqlDataType::Array {
                element_type: Box::new(element_type),
            })
        }
        "STRUCT" => {
            let fields = value
                .get("fields")
                .and_then(Value::as_array)
                .ok_or_else(|| "STRUCT is missing fields".to_string())?;
            let fields = fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let name = field
                        .get("name")
                        .and_then(Value::as_str)
                        .ok_or_else(|| format!("STRUCT field {index} is missing name"))?
                        .to_string();
                    let columntype = field.get("columntype").unwrap_or(field);
                    let (data_type, nullable) = feldera_sql_data_type_from_column_type(columntype)?;
                    Ok(SqlStructField {
                        name,
                        data_type,
                        nullable,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(SqlDataType::Struct { fields })
        }
        "MAP" => {
            let key = value
                .get("key")
                .ok_or_else(|| "MAP is missing key".to_string())?;
            let map_value = value
                .get("value")
                .ok_or_else(|| "MAP is missing value".to_string())?;
            let (key_type, _) = feldera_sql_data_type_from_column_type(key)?;
            let (value_type, _) = feldera_sql_data_type_from_column_type(map_value)?;
            Ok(SqlDataType::Map {
                key_type: Box::new(key_type),
                value_type: Box::new(value_type),
            })
        }
        "NULL" => Ok(SqlDataType::Null),
        "UUID" => Ok(SqlDataType::Uuid),
        "VARIANT" => Ok(SqlDataType::Json),
        "GEOMETRY" => Ok(SqlDataType::Geometry),
        "INTERVAL_DAY" => Ok(SqlDataType::Interval {
            unit: SqlIntervalUnit::Day,
        }),
        "INTERVAL_DAY_HOUR" => Ok(SqlDataType::Interval {
            unit: SqlIntervalUnit::DayToHour,
        }),
        "INTERVAL_DAY_MINUTE" => Ok(SqlDataType::Interval {
            unit: SqlIntervalUnit::DayToMinute,
        }),
        "INTERVAL_DAY_SECOND" => Ok(SqlDataType::Interval {
            unit: SqlIntervalUnit::DayToSecond,
        }),
        "INTERVAL_HOUR" => Ok(SqlDataType::Interval {
            unit: SqlIntervalUnit::Hour,
        }),
        "INTERVAL_HOUR_MINUTE" => Ok(SqlDataType::Interval {
            unit: SqlIntervalUnit::HourToMinute,
        }),
        "INTERVAL_HOUR_SECOND" => Ok(SqlDataType::Interval {
            unit: SqlIntervalUnit::HourToSecond,
        }),
        "INTERVAL_MINUTE" => Ok(SqlDataType::Interval {
            unit: SqlIntervalUnit::Minute,
        }),
        "INTERVAL_MINUTE_SECOND" => Ok(SqlDataType::Interval {
            unit: SqlIntervalUnit::MinuteToSecond,
        }),
        "INTERVAL_MONTH" => Ok(SqlDataType::Interval {
            unit: SqlIntervalUnit::Month,
        }),
        "INTERVAL_SECOND" => Ok(SqlDataType::Interval {
            unit: SqlIntervalUnit::Second,
        }),
        "INTERVAL_YEAR" => Ok(SqlDataType::Interval {
            unit: SqlIntervalUnit::Year,
        }),
        "INTERVAL_YEAR_MONTH" => Ok(SqlDataType::Interval {
            unit: SqlIntervalUnit::YearToMonth,
        }),
        _ => Err(format!("unknown Feldera SQL type `{raw_name}`")),
    }
}

fn u8_from_i64(field: &'static str, value: i64) -> Result<u8, String> {
    u8::try_from(value).map_err(|_| format!("{field} is outside u8 range"))
}

fn u32_from_i64(field: &'static str, value: i64) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| format!("{field} is outside u32 range"))
}

fn feldera_compiled_output_schema_fingerprint(
    view_id: &str,
    program_version: u64,
    columns: &[ColumnSchema],
    primary_key: &[String],
) -> Result<String, FelderaArtifactError> {
    let canonical = serde_json::to_vec(&json!({
        "domain": "velorix-feldera-compiled-output-schema-v1",
        "view_id": view_id,
        "program_version": program_version,
        "columns": columns,
        "primary_key": primary_key
    }))
    .map_err(SerdeJsonError)?;
    let mut hasher = Sha256::new();
    hasher.update(canonical);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

pub fn validate_feldera_compile_artifact_for_catalog(
    catalog: &VelorixRelationCatalogV1,
    spec: &StandingViewSpec,
    artifact: &FelderaCompileArtifactMetadata,
) -> Result<(), FelderaArtifactError> {
    validate_feldera_compile_artifact_for_catalogs(std::slice::from_ref(catalog), spec, artifact)
}

pub fn validate_feldera_compile_artifact_for_catalogs(
    catalogs: &[VelorixRelationCatalogV1],
    spec: &StandingViewSpec,
    artifact: &FelderaCompileArtifactMetadata,
) -> Result<(), FelderaArtifactError> {
    validate_spec_input_relations_match_catalogs(catalogs, spec)?;
    validate_feldera_compile_artifact(spec, artifact)
}

pub fn validate_feldera_compile_artifact_hash(
    spec: &StandingViewSpec,
    artifact: &FelderaCompileArtifactMetadata,
    artifact_bytes: &[u8],
) -> Result<(), FelderaArtifactError> {
    validate_feldera_compile_artifact(spec, artifact)?;
    validate_artifact_bytes_hash(artifact, artifact_bytes)
}

pub fn validate_feldera_compile_artifact_hash_for_catalog(
    catalog: &VelorixRelationCatalogV1,
    spec: &StandingViewSpec,
    artifact: &FelderaCompileArtifactMetadata,
    artifact_bytes: &[u8],
) -> Result<(), FelderaArtifactError> {
    validate_feldera_compile_artifact_for_catalog(catalog, spec, artifact)?;
    validate_artifact_bytes_hash(artifact, artifact_bytes)
}

pub fn validate_feldera_compile_artifact_hash_for_catalogs(
    catalogs: &[VelorixRelationCatalogV1],
    spec: &StandingViewSpec,
    artifact: &FelderaCompileArtifactMetadata,
    artifact_bytes: &[u8],
) -> Result<(), FelderaArtifactError> {
    validate_feldera_compile_artifact_for_catalogs(catalogs, spec, artifact)?;
    validate_artifact_bytes_hash(artifact, artifact_bytes)
}

pub fn validate_feldera_release_artifact_provenance(
    artifact: &FelderaCompileArtifactMetadata,
    provenance: &FelderaReleaseArtifactProvenanceV1,
) -> Result<(), FelderaArtifactError> {
    if artifact.metadata_version != FELDERA_ARTIFACT_METADATA_VERSION_V1
        && artifact.metadata_version != FELDERA_ARTIFACT_METADATA_VERSION
    {
        return Err(FelderaArtifactError::UnsupportedMetadataVersion {
            version: artifact.metadata_version,
        });
    }
    if provenance.schema_version != FELDERA_RELEASE_ARTIFACT_PROVENANCE_VERSION {
        return Err(FelderaArtifactError::UnsupportedReleaseProvenanceVersion {
            version: provenance.schema_version,
        });
    }

    require_non_empty("artifact_id", &artifact.artifact_id)?;
    require_non_empty("artifact_hash", &artifact.artifact_hash)?;
    validate_artifact_hash("artifact_hash", &artifact.artifact_hash)?;
    require_non_empty("spec_hash", &artifact.spec_hash)?;
    require_non_empty(
        "generated_rust.abi_version",
        &artifact.generated_rust.abi_version,
    )?;
    require_non_empty(
        "generated_rust.crate_name",
        &artifact.generated_rust.crate_name,
    )?;
    if artifact.generated_rust.abi_version != SUPPORTED_GENERATED_RUST_ABI_VERSION {
        return Err(FelderaArtifactError::UnsupportedGeneratedRustAbi {
            abi_version: artifact.generated_rust.abi_version.clone(),
        });
    }

    require_release_field("release.release_id", &provenance.release.release_id)?;
    require_release_field(
        "release.release_version",
        &provenance.release.release_version,
    )?;
    require_release_field("build.build_id", &provenance.build.build_id)?;
    require_release_field("build.builder_id", &provenance.build.builder_id)?;
    require_release_field("build.artifact_id", &provenance.build.artifact_id)?;
    require_release_field("build.artifact_hash", &provenance.build.artifact_hash)?;
    validate_artifact_hash("build.artifact_hash", &provenance.build.artifact_hash)?;
    require_release_field("build.spec_hash", &provenance.build.spec_hash)?;
    require_release_field(
        "build.generated_rust.abi_version",
        &provenance.build.generated_rust.abi_version,
    )?;
    require_release_field(
        "build.generated_rust.crate_name",
        &provenance.build.generated_rust.crate_name,
    )?;
    require_release_field(
        "provenance.source_repository",
        &provenance.provenance.source_repository,
    )?;
    require_release_field(
        "provenance.source_revision",
        &provenance.provenance.source_revision,
    )?;
    require_release_field(
        "provenance.compiler_name",
        &provenance.provenance.compiler_name,
    )?;
    require_release_field(
        "provenance.compiler_version",
        &provenance.provenance.compiler_version,
    )?;

    require_release_match(
        "build.artifact_id",
        &artifact.artifact_id,
        &provenance.build.artifact_id,
    )?;
    require_release_match(
        "build.artifact_hash",
        &artifact.artifact_hash,
        &provenance.build.artifact_hash,
    )?;
    require_release_match(
        "build.spec_hash",
        &artifact.spec_hash,
        &provenance.build.spec_hash,
    )?;
    require_release_match(
        "build.generated_rust.abi_version",
        &artifact.generated_rust.abi_version,
        &provenance.build.generated_rust.abi_version,
    )?;
    require_release_match(
        "build.generated_rust.crate_name",
        &artifact.generated_rust.crate_name,
        &provenance.build.generated_rust.crate_name,
    )?;
    require_release_match(
        "provenance.compiler_name",
        &artifact.compiler.name,
        &provenance.provenance.compiler_name,
    )?;
    require_release_match(
        "provenance.compiler_version",
        &artifact.compiler.version,
        &provenance.provenance.compiler_version,
    )?;

    Ok(())
}

/// Hashes the v1 standing-view spec contract as SHA-256 over the compact
/// `serde_json::to_vec` serialization of `StandingViewSpec`.
pub fn feldera_spec_hash(spec: &StandingViewSpec) -> Result<String, FelderaArtifactError> {
    let encoded = serde_json::to_vec(spec).map_err(SerdeJsonError)?;
    let digest = Sha256::digest(&encoded);
    Ok(format!("{FELDERA_SPEC_HASH_PREFIX}:{digest:x}"))
}

pub fn feldera_compile_request_hash(
    request: &FelderaCompileRequestV1,
) -> Result<String, FelderaArtifactError> {
    validate_feldera_compile_request(request)?;
    let encoded = serde_json::to_vec(request).map_err(SerdeJsonError)?;
    let digest = Sha256::digest(&encoded);
    Ok(format!("{FELDERA_COMPILE_REQUEST_HASH_PREFIX}:{digest:x}"))
}

pub fn feldera_artifact_bytes_hash(artifact_bytes: &[u8]) -> String {
    let digest = Sha256::digest(artifact_bytes);
    format!("sha256:{digest:x}")
}

fn validate_artifact_bytes_hash(
    artifact: &FelderaCompileArtifactMetadata,
    artifact_bytes: &[u8],
) -> Result<(), FelderaArtifactError> {
    let actual = feldera_artifact_bytes_hash(artifact_bytes);
    if actual == artifact.artifact_hash {
        Ok(())
    } else {
        Err(FelderaArtifactError::MismatchedArtifactHash {
            expected: artifact.artifact_hash.clone(),
            actual,
        })
    }
}

fn catalog_column_schema(column: &RelationColumnV1) -> Result<ColumnSchema, FelderaArtifactError> {
    Ok(ColumnSchema {
        name: column.name.clone(),
        data_type: sql_data_type_for_logical_type(&column.logical_type)?,
        nullable: column.nullable,
    })
}

fn catalog_primary_key_columns(
    catalog: &VelorixRelationCatalogV1,
) -> Result<Vec<String>, FelderaArtifactError> {
    catalog
        .relation_schema
        .primary_key_column_ids
        .iter()
        .map(|column_id| {
            catalog
                .relation_schema
                .columns
                .iter()
                .find(|column| &column.column_id == column_id)
                .map(|column| column.name.clone())
                .ok_or(FelderaArtifactError::InvalidRelationSchema {
                    field: "primary_key",
                })
        })
        .collect()
}

fn sql_data_type_for_logical_type(
    logical_type: &VelorixLogicalTypeV1,
) -> Result<SqlDataType, FelderaArtifactError> {
    Ok(match logical_type {
        VelorixLogicalTypeV1::Bool => SqlDataType::Bool,
        VelorixLogicalTypeV1::Int8 => SqlDataType::Int8,
        VelorixLogicalTypeV1::Int16 => SqlDataType::Int16,
        VelorixLogicalTypeV1::Int32 => SqlDataType::Int32,
        VelorixLogicalTypeV1::Int64 => SqlDataType::Int64,
        VelorixLogicalTypeV1::UInt8 => SqlDataType::UInt8,
        VelorixLogicalTypeV1::UInt16 => SqlDataType::UInt16,
        VelorixLogicalTypeV1::UInt32 => SqlDataType::UInt32,
        VelorixLogicalTypeV1::UInt64 => SqlDataType::UInt64,
        VelorixLogicalTypeV1::Float32 => SqlDataType::Float32,
        VelorixLogicalTypeV1::Float64 => SqlDataType::Float64,
        VelorixLogicalTypeV1::Decimal { precision, scale } => SqlDataType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        VelorixLogicalTypeV1::Char { length } => SqlDataType::Char { length: *length },
        VelorixLogicalTypeV1::Utf8 => SqlDataType::Utf8,
        VelorixLogicalTypeV1::Binary { length } => SqlDataType::Binary { length: *length },
        VelorixLogicalTypeV1::Varbinary => SqlDataType::Varbinary,
        VelorixLogicalTypeV1::Date => SqlDataType::Date,
        VelorixLogicalTypeV1::Time => SqlDataType::Time,
        VelorixLogicalTypeV1::Timestamp { timezone } => SqlDataType::Timestamp {
            timezone: timezone.clone(),
        },
        VelorixLogicalTypeV1::Uuid => SqlDataType::Uuid,
        VelorixLogicalTypeV1::Json => SqlDataType::Json,
        VelorixLogicalTypeV1::Array { element_type } => SqlDataType::Array {
            element_type: Box::new(sql_data_type_for_logical_type(element_type)?),
        },
        VelorixLogicalTypeV1::Struct { fields } => SqlDataType::Struct {
            fields: fields
                .iter()
                .map(|field| {
                    Ok(SqlStructField {
                        name: field.name.clone(),
                        data_type: sql_data_type_for_logical_type(&field.logical_type)?,
                        nullable: field.nullable,
                    })
                })
                .collect::<Result<Vec<_>, FelderaArtifactError>>()?,
        },
        VelorixLogicalTypeV1::Map {
            key_type,
            value_type,
        } => SqlDataType::Map {
            key_type: Box::new(sql_data_type_for_logical_type(key_type)?),
            value_type: Box::new(sql_data_type_for_logical_type(value_type)?),
        },
    })
}

fn catalog_relation_error(error: RelationSchemaError) -> FelderaArtifactError {
    match error {
        RelationSchemaError::SchemaFingerprintMismatch { .. } => {
            FelderaArtifactError::SchemaFingerprintMismatch { field: "catalog" }
        }
        _ => FelderaArtifactError::InvalidRelationSchema { field: "catalog" },
    }
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), FelderaArtifactError> {
    if value.trim().is_empty() {
        return Err(FelderaArtifactError::MissingIdentityField { field });
    }

    Ok(())
}

fn require_release_field(field: &'static str, value: &str) -> Result<(), FelderaArtifactError> {
    if value.trim().is_empty() {
        return Err(FelderaArtifactError::MissingReleaseProvenanceField { field });
    }

    Ok(())
}

fn require_release_match(
    field: &'static str,
    expected: &str,
    actual: &str,
) -> Result<(), FelderaArtifactError> {
    if expected == actual {
        Ok(())
    } else {
        Err(FelderaArtifactError::MismatchedReleaseProvenanceField { field })
    }
}

fn validate_relation_schemas(schemas: &[RelationSchema]) -> Result<(), FelderaArtifactError> {
    for schema in schemas {
        validate_relation_schema(schema)?;
    }

    Ok(())
}

fn validate_spec_input_relations_match_catalogs(
    catalogs: &[VelorixRelationCatalogV1],
    spec: &StandingViewSpec,
) -> Result<(), FelderaArtifactError> {
    if catalogs.is_empty() {
        return Err(FelderaArtifactError::MissingSchema { field: "catalogs" });
    }
    if spec.input_relations.is_empty() {
        return Err(FelderaArtifactError::MissingSchema {
            field: "spec.input_relations",
        });
    }
    if catalogs.len() != spec.input_relations.len() {
        return Err(FelderaArtifactError::SchemaMismatch {
            field: "spec.input_relations",
        });
    }

    let mut catalog_schemas_by_id = BTreeMap::new();
    let mut catalog_relation_names = BTreeSet::new();
    for catalog in catalogs {
        let schema = catalog_input_relation_schema(catalog)?;
        if !catalog_relation_names.insert(schema.relation_name.clone()) {
            return Err(FelderaArtifactError::InvalidRelationSchema {
                field: "duplicate_relation_name",
            });
        }
        if catalog_schemas_by_id
            .insert(schema.relation_id.clone(), schema)
            .is_some()
        {
            return Err(FelderaArtifactError::InvalidRelationSchema {
                field: "duplicate_relation_id",
            });
        }
    }

    let mut spec_relation_ids = BTreeSet::new();
    let mut spec_relation_names = BTreeSet::new();
    for spec_input in &spec.input_relations {
        if !spec_relation_ids.insert(spec_input.relation_id.as_str()) {
            return Err(FelderaArtifactError::InvalidRelationSchema {
                field: "duplicate_relation_id",
            });
        }
        if !spec_relation_names.insert(spec_input.relation_name.as_str()) {
            return Err(FelderaArtifactError::InvalidRelationSchema {
                field: "duplicate_relation_name",
            });
        }
        let Some(catalog_schema) = catalog_schemas_by_id.get(&spec_input.relation_id) else {
            return Err(FelderaArtifactError::SchemaMismatch {
                field: "spec.input_relations",
            });
        };
        validate_relation_identity_matches("spec.input_relations", catalog_schema, spec_input)?;
        if catalog_schema != spec_input {
            return Err(FelderaArtifactError::SchemaMismatch {
                field: "spec.input_relations",
            });
        }
    }

    Ok(())
}

fn validate_relation_schema(schema: &RelationSchema) -> Result<(), FelderaArtifactError> {
    if schema.relation_id.trim().is_empty() {
        return Err(FelderaArtifactError::InvalidRelationSchema {
            field: "relation_id",
        });
    }
    if schema.relation_name.trim().is_empty() {
        return Err(FelderaArtifactError::InvalidRelationSchema {
            field: "relation_name",
        });
    }
    if schema.relation_version.trim().is_empty() {
        return Err(FelderaArtifactError::InvalidRelationSchema {
            field: "relation_version",
        });
    }
    validate_schema_fingerprint("schema_fingerprint", &schema.schema_fingerprint)?;
    if schema.columns.is_empty() || schema.columns.len() > MAX_RELATION_COLUMNS {
        return Err(FelderaArtifactError::InvalidRelationSchema { field: "columns" });
    }

    let mut column_names = BTreeSet::new();
    for column in &schema.columns {
        if column.name.trim().is_empty() {
            return Err(FelderaArtifactError::InvalidRelationSchema {
                field: "column.name",
            });
        }
        if !column_names.insert(column.name.as_str()) {
            return Err(FelderaArtifactError::InvalidRelationSchema {
                field: "duplicate_column",
            });
        }
        validate_sql_data_type(&column.data_type)?;
    }

    for key_column in &schema.primary_key {
        if !column_names.contains(key_column.as_str()) {
            return Err(FelderaArtifactError::InvalidRelationSchema {
                field: "primary_key",
            });
        }
    }

    Ok(())
}

fn validate_relation_identity_matches(
    field: &'static str,
    spec_schema: &RelationSchema,
    artifact_schema: &RelationSchema,
) -> Result<(), FelderaArtifactError> {
    if spec_schema.relation_id != artifact_schema.relation_id
        || spec_schema.relation_version != artifact_schema.relation_version
    {
        return Err(FelderaArtifactError::SchemaMismatch { field });
    }
    if spec_schema.schema_fingerprint != artifact_schema.schema_fingerprint {
        return Err(FelderaArtifactError::SchemaFingerprintMismatch { field });
    }

    Ok(())
}

fn validate_schema_fingerprint(
    field: &'static str,
    fingerprint: &str,
) -> Result<(), FelderaArtifactError> {
    if fingerprint.trim().is_empty() {
        return Err(FelderaArtifactError::InvalidRelationSchema { field });
    }
    let Some(hex) = fingerprint.strip_prefix("sha256:") else {
        return Err(FelderaArtifactError::InvalidRelationSchema { field });
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(FelderaArtifactError::InvalidRelationSchema { field });
    }

    Ok(())
}

fn validate_artifact_hash(
    field: &'static str,
    artifact_hash: &str,
) -> Result<(), FelderaArtifactError> {
    let Some(hex) = artifact_hash.strip_prefix("sha256:") else {
        return Err(FelderaArtifactError::InvalidArtifactHash { field });
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(FelderaArtifactError::InvalidArtifactHash { field });
    }

    Ok(())
}

fn validate_compile_request_hash(
    field: &'static str,
    compile_request_hash: &str,
) -> Result<(), FelderaArtifactError> {
    let Some(hex) =
        compile_request_hash.strip_prefix(&format!("{FELDERA_COMPILE_REQUEST_HASH_PREFIX}:"))
    else {
        return Err(FelderaArtifactError::InvalidArtifactHash { field });
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(FelderaArtifactError::InvalidArtifactHash { field });
    }

    Ok(())
}

fn feldera_create_table_statement(
    relation: &RelationSchema,
) -> Result<String, FelderaArtifactError> {
    let mut declarations = relation
        .columns
        .iter()
        .map(|column| {
            Ok(format!(
                "    {} {}{}",
                quote_feldera_identifier(&column.name),
                feldera_sql_type_name(&column.data_type, "column.data_type")?,
                if column.nullable {
                    " NULL"
                } else {
                    " NOT NULL"
                }
            ))
        })
        .collect::<Result<Vec<_>, FelderaArtifactError>>()?;

    if !relation.primary_key.is_empty() {
        let primary_key = relation
            .primary_key
            .iter()
            .map(|column| quote_feldera_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        declarations.push(format!("    PRIMARY KEY ({primary_key})"));
    }

    Ok(format!(
        "CREATE TABLE {} (\n{}\n);",
        quote_feldera_identifier(&relation.relation_name),
        declarations.join(",\n")
    ))
}

fn feldera_create_materialized_view_statement(
    view_id: &str,
    sql: &str,
) -> Result<String, FelderaArtifactError> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err(FelderaArtifactError::MissingIdentityField {
            field: "compile_request.sql",
        });
    }
    let sql = sql.trim_end_matches(';').trim_end();

    Ok(format!(
        "CREATE MATERIALIZED VIEW {} AS\n{sql};",
        quote_feldera_identifier(view_id)
    ))
}

fn quote_feldera_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn feldera_sql_type_name(
    data_type: &SqlDataType,
    field: &'static str,
) -> Result<String, FelderaArtifactError> {
    Ok(match data_type {
        SqlDataType::Bool => "BOOLEAN".to_string(),
        SqlDataType::Int8 => "TINYINT".to_string(),
        SqlDataType::Int16 => "SMALLINT".to_string(),
        SqlDataType::Int32 => "INTEGER".to_string(),
        SqlDataType::Int64 => "BIGINT".to_string(),
        SqlDataType::UInt8 => "TINYINT UNSIGNED".to_string(),
        SqlDataType::UInt16 => "SMALLINT UNSIGNED".to_string(),
        SqlDataType::UInt32 => "INTEGER UNSIGNED".to_string(),
        SqlDataType::UInt64 => "BIGINT UNSIGNED".to_string(),
        SqlDataType::Float32 => "REAL".to_string(),
        SqlDataType::Float64 => "DOUBLE".to_string(),
        SqlDataType::Decimal { precision, scale } => format!("DECIMAL({precision}, {scale})"),
        SqlDataType::Char {
            length: Some(length),
        } => format!("CHAR({length})"),
        SqlDataType::Char { length: None } => "CHAR".to_string(),
        SqlDataType::Utf8 => "VARCHAR".to_string(),
        SqlDataType::Binary { length } => format!("BINARY({length})"),
        SqlDataType::Varbinary => "VARBINARY".to_string(),
        SqlDataType::Time => "TIME".to_string(),
        SqlDataType::Date => "DATE".to_string(),
        SqlDataType::Timestamp { timezone: None } => "TIMESTAMP".to_string(),
        SqlDataType::Timestamp {
            timezone: Some(timezone),
        } => {
            return Err(FelderaArtifactError::UnsupportedTableSchemaType {
                field,
                data_type: format!("TIMESTAMP WITH TIMEZONE {timezone}"),
            })
        }
        SqlDataType::Array { element_type } => {
            format!("{} ARRAY", feldera_sql_type_name(element_type, field)?)
        }
        SqlDataType::Struct { fields } => {
            let fields = fields
                .iter()
                .map(|struct_field| {
                    Ok(format!(
                        "{} {}{}",
                        quote_feldera_identifier(&struct_field.name),
                        feldera_sql_type_name(&struct_field.data_type, "struct.field.data_type")?,
                        if struct_field.nullable {
                            " NULL"
                        } else {
                            " NOT NULL"
                        }
                    ))
                })
                .collect::<Result<Vec<_>, FelderaArtifactError>>()?;
            format!("ROW({})", fields.join(", "))
        }
        SqlDataType::Map {
            key_type,
            value_type,
        } => format!(
            "MAP<{}, {}>",
            feldera_sql_type_name(key_type, field)?,
            feldera_sql_type_name(value_type, field)?
        ),
        SqlDataType::Null => {
            return Err(FelderaArtifactError::UnsupportedTableSchemaType {
                field,
                data_type: "NULL".to_string(),
            })
        }
        SqlDataType::Interval { unit } => {
            return Err(FelderaArtifactError::UnsupportedTableSchemaType {
                field,
                data_type: format!("INTERVAL {unit:?}"),
            })
        }
        SqlDataType::Uuid => "UUID".to_string(),
        SqlDataType::Json => "VARIANT".to_string(),
        SqlDataType::Geometry => "GEOMETRY".to_string(),
    })
}

fn validate_sql_data_type(data_type: &SqlDataType) -> Result<(), FelderaArtifactError> {
    let mut type_nodes = 0;
    validate_sql_data_type_with_limits(data_type, 0, &mut type_nodes)
}

fn validate_sql_data_type_with_limits(
    data_type: &SqlDataType,
    depth: usize,
    type_nodes: &mut usize,
) -> Result<(), FelderaArtifactError> {
    if depth > MAX_SQL_TYPE_NESTING_DEPTH {
        return Err(FelderaArtifactError::InvalidRelationSchema {
            field: "sql_type.depth",
        });
    }
    *type_nodes += 1;
    if *type_nodes > MAX_SQL_TYPE_NODES {
        return Err(FelderaArtifactError::InvalidRelationSchema {
            field: "sql_type.nodes",
        });
    }
    match data_type {
        SqlDataType::Decimal { precision, scale } => {
            if *precision == 0 || *precision > 38 || *scale > *precision {
                return Err(FelderaArtifactError::InvalidRelationSchema { field: "decimal" });
            }
        }
        SqlDataType::Char {
            length: Some(length),
        } => {
            if *length == 0 {
                return Err(FelderaArtifactError::InvalidRelationSchema {
                    field: "char.length",
                });
            }
        }
        SqlDataType::Binary { length } => {
            if *length == 0 {
                return Err(FelderaArtifactError::InvalidRelationSchema {
                    field: "binary.length",
                });
            }
        }
        SqlDataType::Timestamp { timezone } => {
            if let Some(timezone) = timezone.as_deref() {
                if timezone.trim().is_empty() || timezone.len() > MAX_SQL_TIMEZONE_BYTES {
                    return Err(FelderaArtifactError::InvalidRelationSchema {
                        field: "timestamp.timezone",
                    });
                }
            }
        }
        SqlDataType::Array { element_type } => {
            validate_sql_data_type_with_limits(element_type, depth + 1, type_nodes)?
        }
        SqlDataType::Struct { fields } => {
            if fields.is_empty() || fields.len() > MAX_SQL_STRUCT_FIELDS {
                return Err(FelderaArtifactError::InvalidRelationSchema {
                    field: "struct.fields",
                });
            }
            let mut names = BTreeSet::new();
            for field in fields {
                if field.name.trim().is_empty() {
                    return Err(FelderaArtifactError::InvalidRelationSchema {
                        field: "struct.field.name",
                    });
                }
                if field.name.len() > MAX_SQL_STRUCT_FIELD_NAME_BYTES {
                    return Err(FelderaArtifactError::InvalidRelationSchema {
                        field: "struct.field.name",
                    });
                }
                if !names.insert(field.name.as_str()) {
                    return Err(FelderaArtifactError::InvalidRelationSchema {
                        field: "struct.field.name",
                    });
                }
                validate_sql_data_type_with_limits(&field.data_type, depth + 1, type_nodes)?;
            }
        }
        SqlDataType::Map {
            key_type,
            value_type,
        } => {
            validate_sql_data_type_with_limits(key_type, depth + 1, type_nodes)?;
            validate_sql_data_type_with_limits(value_type, depth + 1, type_nodes)?;
        }
        SqlDataType::Bool
        | SqlDataType::Int8
        | SqlDataType::Int16
        | SqlDataType::Int32
        | SqlDataType::Int64
        | SqlDataType::UInt8
        | SqlDataType::UInt16
        | SqlDataType::UInt32
        | SqlDataType::UInt64
        | SqlDataType::Float32
        | SqlDataType::Float64
        | SqlDataType::Char { length: None }
        | SqlDataType::Utf8
        | SqlDataType::Varbinary
        | SqlDataType::Time
        | SqlDataType::Date
        | SqlDataType::Interval { .. }
        | SqlDataType::Null
        | SqlDataType::Uuid
        | SqlDataType::Json
        | SqlDataType::Geometry => {}
    }

    Ok(())
}
