use thiserror::Error;

use crate::{
    feldera_artifact::{
        catalog_input_relation_schema, feldera_artifact_bytes_hash, feldera_compile_request_hash,
        feldera_spec_hash, FelderaArtifactError, FelderaCompileArtifactMetadata,
        FelderaCompileRequestV1, FelderaCompilerIdentity, FelderaRustExtensionV1,
        GeneratedRustIdentity, RelationSchema, SqlDialect, SqlSourceKind, StandingViewShape,
        StandingViewSpec, FELDERA_ARTIFACT_METADATA_VERSION, SUPPORTED_EPOCH_POLICY,
        SUPPORTED_STATE_CODEC,
    },
    relation::VelorixRelationCatalogV1,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedGeneratedViewDescriptor {
    pub view_id: String,
    pub input_relation_id: String,
    pub input_relation_version: String,
    pub sql: String,
    pub dynamic_view_binding: Option<DynamicGeneratedViewBinding>,
    pub artifact_id: String,
    pub artifact_identity_bytes: Vec<u8>,
    pub compiler: FelderaCompilerIdentity,
    pub generated_rust: GeneratedRustIdentity,
    pub output_schemas: Vec<RelationSchema>,
    pub state_schema_version: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamicGeneratedViewBinding {
    pub shape_id: String,
}

impl TrustedGeneratedViewDescriptor {
    pub fn matches_view_request(
        &self,
        view_id: &str,
        input_relation_id: &str,
        input_relation_version: &str,
        sql: &str,
    ) -> bool {
        self.view_id == view_id
            && self.matches_view_shape(input_relation_id, input_relation_version, sql)
    }

    pub fn matches_view_shape(
        &self,
        input_relation_id: &str,
        input_relation_version: &str,
        sql: &str,
    ) -> bool {
        self.input_relation_id == input_relation_id
            && self.input_relation_version == input_relation_version
            && normalize_sql_text(&self.sql) == normalize_sql_text(sql)
    }

    pub fn standing_view_spec(
        &self,
        catalog: &VelorixRelationCatalogV1,
    ) -> Result<StandingViewSpec, TrustedGeneratedViewDescriptorError> {
        self.validate_catalog(catalog)?;
        let input = catalog_input_relation_schema(catalog)?;
        Ok(StandingViewSpec {
            view_id: self.view_id.clone(),
            sql: self.sql.clone(),
            dialect: SqlDialect::FelderaSql,
            source_kind: SqlSourceKind::StandingView,
            rust_extension: FelderaRustExtensionV1::default(),
            input_relations: vec![input],
            output_relations: self.output_schemas.clone(),
            shape: StandingViewShape {
                is_materialized: true,
                multi_input: false,
                multi_output: self.output_schemas.len() > 1,
            },
        })
    }

    pub fn artifact_metadata(
        &self,
        catalog: &VelorixRelationCatalogV1,
    ) -> Result<FelderaCompileArtifactMetadata, TrustedGeneratedViewDescriptorError> {
        let spec = self.standing_view_spec(catalog)?;
        let input = spec
            .input_relations
            .first()
            .cloned()
            .ok_or(TrustedGeneratedViewDescriptorError::MissingInputSchema)?;

        Ok(FelderaCompileArtifactMetadata {
            metadata_version: FELDERA_ARTIFACT_METADATA_VERSION,
            view_id: self.view_id.clone(),
            spec_hash: feldera_spec_hash(&spec)?,
            compile_request_hash: Some(feldera_compile_request_hash(
                &FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec),
            )?),
            artifact_id: self.artifact_id.clone(),
            artifact_hash: feldera_artifact_bytes_hash(&self.artifact_identity_bytes),
            compiler: self.compiler.clone(),
            generated_rust: self.generated_rust.clone(),
            input_schemas: vec![input],
            output_schemas: self.output_schemas.clone(),
            state_codec: SUPPORTED_STATE_CODEC.to_string(),
            state_schema_version: self.state_schema_version,
            epoch_policy: SUPPORTED_EPOCH_POLICY.to_string(),
        })
    }

    fn validate_catalog(
        &self,
        catalog: &VelorixRelationCatalogV1,
    ) -> Result<(), TrustedGeneratedViewDescriptorError> {
        if catalog.relation_schema.relation_id != self.input_relation_id {
            return Err(TrustedGeneratedViewDescriptorError::InputRelationMismatch {
                expected: self.input_relation_id.clone(),
                actual: catalog.relation_schema.relation_id.clone(),
            });
        }
        if catalog.relation_schema.relation_version != self.input_relation_version {
            return Err(
                TrustedGeneratedViewDescriptorError::InputRelationVersionMismatch {
                    expected: self.input_relation_version.clone(),
                    actual: catalog.relation_schema.relation_version.clone(),
                },
            );
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum TrustedGeneratedViewDescriptorError {
    #[error(
        "trusted generated descriptor input relation mismatch: expected `{expected}`, actual `{actual}`"
    )]
    InputRelationMismatch { expected: String, actual: String },
    #[error(
        "trusted generated descriptor input relation version mismatch: expected `{expected}`, actual `{actual}`"
    )]
    InputRelationVersionMismatch { expected: String, actual: String },
    #[error("trusted generated descriptor did not produce an input schema")]
    MissingInputSchema,
    #[error(transparent)]
    Artifact(#[from] FelderaArtifactError),
}

fn normalize_sql_text(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}
