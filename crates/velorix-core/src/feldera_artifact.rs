use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::relation::{
    RelationColumnV1, RelationSchemaError, VelorixLogicalTypeV1, VelorixRelationCatalogV1,
};

pub const FELDERA_ARTIFACT_METADATA_VERSION: u32 = 1;
pub const FELDERA_RELEASE_ARTIFACT_PROVENANCE_VERSION: u16 = 1;
pub const FELDERA_SPEC_HASH_PREFIX: &str = "velorix-feldera-spec-sha256-v1";
pub const SUPPORTED_STATE_CODEC: &str = "feldera-dbsp-state-v1";
pub const SUPPORTED_EPOCH_POLICY: &str = "monotonic-logical-epoch-v1";
pub const SUPPORTED_GENERATED_RUST_ABI_VERSION: &str = "feldera-generated-rust-abi-v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StandingViewSpec {
    pub view_id: String,
    pub sql: String,
    pub dialect: SqlDialect,
    pub source_kind: SqlSourceKind,
    pub input_relations: Vec<RelationSchema>,
    pub output_relations: Vec<RelationSchema>,
    pub shape: StandingViewShape,
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
    Int64,
    Float64,
    Decimal { precision: u8, scale: u8 },
    Utf8,
    Date,
    Timestamp { timezone: Option<String> },
    Json,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaCompileArtifactMetadata {
    pub metadata_version: u32,
    pub view_id: String,
    pub spec_hash: String,
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
    #[error("invalid Feldera artifact hash: {field}")]
    InvalidArtifactHash { field: &'static str },
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
    if artifact.metadata_version != FELDERA_ARTIFACT_METADATA_VERSION {
        return Err(FelderaArtifactError::UnsupportedMetadataVersion {
            version: artifact.metadata_version,
        });
    }

    require_non_empty("view_id", &artifact.view_id)?;
    require_non_empty("spec_hash", &artifact.spec_hash)?;
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

    if spec.shape.multi_input || spec.input_relations.len() > 1 || artifact.input_schemas.len() > 1
    {
        return Err(FelderaArtifactError::UnsupportedShape {
            shape: "multi_input",
        });
    }
    if spec.shape.multi_output
        || spec.output_relations.len() > 1
        || artifact.output_schemas.len() > 1
    {
        return Err(FelderaArtifactError::UnsupportedShape {
            shape: "multi_output",
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
    validate_relation_identity_matches(
        "output_schemas",
        &spec.output_relations[0],
        &artifact.output_schemas[0],
    )?;

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

pub fn validate_feldera_compile_artifact_for_catalog(
    catalog: &VelorixRelationCatalogV1,
    spec: &StandingViewSpec,
    artifact: &FelderaCompileArtifactMetadata,
) -> Result<(), FelderaArtifactError> {
    let catalog_schema = catalog_input_relation_schema(catalog)?;
    let Some(spec_input) = spec.input_relations.first() else {
        return Err(FelderaArtifactError::MissingSchema {
            field: "spec.input_relations",
        });
    };

    validate_relation_identity_matches("spec.input_relations", &catalog_schema, spec_input)?;
    if &catalog_schema != spec_input {
        return Err(FelderaArtifactError::SchemaMismatch {
            field: "spec.input_relations",
        });
    }

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

pub fn validate_feldera_release_artifact_provenance(
    artifact: &FelderaCompileArtifactMetadata,
    provenance: &FelderaReleaseArtifactProvenanceV1,
) -> Result<(), FelderaArtifactError> {
    if artifact.metadata_version != FELDERA_ARTIFACT_METADATA_VERSION {
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
        VelorixLogicalTypeV1::Int64 => SqlDataType::Int64,
        VelorixLogicalTypeV1::Float64 => SqlDataType::Float64,
        VelorixLogicalTypeV1::Decimal { precision, scale } => SqlDataType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        VelorixLogicalTypeV1::Utf8 => SqlDataType::Utf8,
        VelorixLogicalTypeV1::Date => SqlDataType::Date,
        VelorixLogicalTypeV1::Timestamp { timezone } => SqlDataType::Timestamp {
            timezone: timezone.clone(),
        },
        VelorixLogicalTypeV1::Json => SqlDataType::Json,
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
    if schema.columns.is_empty() {
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

fn validate_sql_data_type(data_type: &SqlDataType) -> Result<(), FelderaArtifactError> {
    match data_type {
        SqlDataType::Decimal { precision, scale } => {
            if *precision == 0 || *scale > *precision {
                return Err(FelderaArtifactError::InvalidRelationSchema { field: "decimal" });
            }
        }
        SqlDataType::Timestamp { timezone } => {
            if timezone
                .as_deref()
                .is_some_and(|timezone| timezone.trim().is_empty())
            {
                return Err(FelderaArtifactError::InvalidRelationSchema {
                    field: "timestamp.timezone",
                });
            }
        }
        SqlDataType::Bool
        | SqlDataType::Int64
        | SqlDataType::Float64
        | SqlDataType::Utf8
        | SqlDataType::Date
        | SqlDataType::Json => {}
    }

    Ok(())
}
