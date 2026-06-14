use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    feldera_artifact::{
        feldera_artifact_bytes_hash, feldera_compile_request_hash, feldera_spec_hash,
        validate_materialized_standing_view_spec, FelderaArtifactError, FelderaCompileRequestV1,
        RelationSchema, StandingViewSpec,
    },
    standing_program::{
        FelderaRuntimePackageIdentity, NativeCodePolicy, StandingProgramIdentity,
        StandingProgramRuntimeError,
    },
};

pub const FELDERA_PRODUCT_RUNTIME_DESCRIPTOR_VERSION: u32 = 1;
pub const FELDERA_PACKAGE_RUNTIME_EXECUTION_PATH: &str = "feldera_package_runtime";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaPackageRuntimeDescriptorV1 {
    pub descriptor_version: u32,
    pub view_id: String,
    pub spec_hash: String,
    pub compile_request_hash: String,
    pub backend: FelderaPackageBackendIdentity,
    pub runtime_factory: FelderaPackageRuntimeFactoryBinding,
    pub input_schemas: Vec<RelationSchema>,
    pub output_schemas: Vec<RelationSchema>,
    pub state_codec: String,
    pub state_schema_version: u32,
    pub standing_program_identity: StandingProgramIdentity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaPackageBackendIdentity {
    pub name: String,
    pub version: String,
    pub source: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaPackageRuntimeFactoryBinding {
    pub crate_name: String,
    pub crate_version: String,
    pub factory_symbol: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BuildFelderaPackageRuntimeDescriptorRequest {
    pub spec: StandingViewSpec,
    pub compile_request: FelderaCompileRequestV1,
    pub backend: FelderaPackageBackendIdentity,
    pub runtime_factory: FelderaPackageRuntimeFactoryBinding,
    pub state_codec: String,
    pub state_schema_version: u32,
}

#[derive(Debug, Error)]
pub enum FelderaProductRuntimeDescriptorError {
    #[error("unsupported Feldera product runtime descriptor version: {version}")]
    UnsupportedDescriptorVersion { version: u32 },
    #[error("missing Feldera product runtime descriptor field: {field}")]
    MissingField { field: &'static str },
    #[error("Feldera product runtime descriptor view id mismatch: spec={spec_view_id}, descriptor={descriptor_view_id}")]
    MismatchedViewId {
        spec_view_id: String,
        descriptor_view_id: String,
    },
    #[error("Feldera product runtime descriptor spec hash mismatch: expected={expected}, actual={actual}")]
    MismatchedSpecHash { expected: String, actual: String },
    #[error("Feldera product runtime descriptor compile request hash mismatch: expected={expected}, actual={actual}")]
    MismatchedCompileRequestHash { expected: String, actual: String },
    #[error("Feldera product runtime descriptor schema mismatch: {field}")]
    SchemaMismatch { field: &'static str },
    #[error("Feldera product runtime descriptor identity mismatch")]
    ProgramIdentityMismatch,
    #[error("Feldera product runtime descriptor native code policy must be disabled")]
    NativeCodePolicyNotDisabled,
    #[error(transparent)]
    Artifact(#[from] FelderaArtifactError),
    #[error(transparent)]
    StandingRuntime(#[from] StandingProgramRuntimeError),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

pub fn validate_feldera_package_runtime_descriptor(
    spec: &StandingViewSpec,
    compile_request: &FelderaCompileRequestV1,
    descriptor: &FelderaPackageRuntimeDescriptorV1,
) -> Result<(), FelderaProductRuntimeDescriptorError> {
    if descriptor.descriptor_version != FELDERA_PRODUCT_RUNTIME_DESCRIPTOR_VERSION {
        return Err(
            FelderaProductRuntimeDescriptorError::UnsupportedDescriptorVersion {
                version: descriptor.descriptor_version,
            },
        );
    }
    require_non_empty("view_id", &descriptor.view_id)?;
    require_non_empty("spec_hash", &descriptor.spec_hash)?;
    require_non_empty("compile_request_hash", &descriptor.compile_request_hash)?;
    require_non_empty("backend.name", &descriptor.backend.name)?;
    require_non_empty("backend.version", &descriptor.backend.version)?;
    require_non_empty("backend.source", &descriptor.backend.source)?;
    require_non_empty(
        "runtime_factory.crate_name",
        &descriptor.runtime_factory.crate_name,
    )?;
    require_non_empty(
        "runtime_factory.crate_version",
        &descriptor.runtime_factory.crate_version,
    )?;
    require_non_empty(
        "runtime_factory.factory_symbol",
        &descriptor.runtime_factory.factory_symbol,
    )?;
    require_non_empty("state_codec", &descriptor.state_codec)?;
    if descriptor.state_schema_version == 0 {
        return Err(FelderaProductRuntimeDescriptorError::MissingField {
            field: "state_schema_version",
        });
    }

    validate_materialized_standing_view_spec(spec)?;
    if spec.view_id != descriptor.view_id {
        return Err(FelderaProductRuntimeDescriptorError::MismatchedViewId {
            spec_view_id: spec.view_id.clone(),
            descriptor_view_id: descriptor.view_id.clone(),
        });
    }
    let expected_spec_hash = feldera_spec_hash(spec)?;
    if expected_spec_hash != descriptor.spec_hash {
        return Err(FelderaProductRuntimeDescriptorError::MismatchedSpecHash {
            expected: expected_spec_hash,
            actual: descriptor.spec_hash.clone(),
        });
    }
    let expected_compile_request_hash = feldera_compile_request_hash(compile_request)?;
    if expected_compile_request_hash != descriptor.compile_request_hash {
        return Err(
            FelderaProductRuntimeDescriptorError::MismatchedCompileRequestHash {
                expected: expected_compile_request_hash,
                actual: descriptor.compile_request_hash.clone(),
            },
        );
    }
    if spec.input_relations != descriptor.input_schemas {
        return Err(FelderaProductRuntimeDescriptorError::SchemaMismatch {
            field: "input_schemas",
        });
    }
    if spec.output_relations != descriptor.output_schemas {
        return Err(FelderaProductRuntimeDescriptorError::SchemaMismatch {
            field: "output_schemas",
        });
    }
    if descriptor.standing_program_identity.native_code_policy
        != NativeCodePolicy::DisabledNoExternalDependencies
    {
        return Err(FelderaProductRuntimeDescriptorError::NativeCodePolicyNotDisabled);
    }

    let expected_identity = feldera_package_runtime_identity_for_descriptor(spec, descriptor)?;
    if expected_identity != descriptor.standing_program_identity {
        return Err(FelderaProductRuntimeDescriptorError::ProgramIdentityMismatch);
    }
    descriptor.standing_program_identity.validate()?;
    Ok(())
}

pub fn build_feldera_package_runtime_descriptor(
    request: BuildFelderaPackageRuntimeDescriptorRequest,
) -> Result<FelderaPackageRuntimeDescriptorV1, FelderaProductRuntimeDescriptorError> {
    let mut descriptor = FelderaPackageRuntimeDescriptorV1 {
        descriptor_version: FELDERA_PRODUCT_RUNTIME_DESCRIPTOR_VERSION,
        view_id: request.spec.view_id.clone(),
        spec_hash: feldera_spec_hash(&request.spec)?,
        compile_request_hash: feldera_compile_request_hash(&request.compile_request)?,
        backend: request.backend,
        runtime_factory: request.runtime_factory,
        input_schemas: request.spec.input_relations.clone(),
        output_schemas: request.spec.output_relations.clone(),
        state_codec: request.state_codec,
        state_schema_version: request.state_schema_version,
        standing_program_identity: StandingProgramIdentity {
            tenant_id: "default".to_string(),
            program_id: "placeholder".to_string(),
            view_ids: vec!["placeholder".to_string()],
            sql_hash: feldera_artifact_bytes_hash(b"placeholder-sql"),
            input_catalog_hash: feldera_artifact_bytes_hash(b"placeholder-input"),
            output_schema_hash: feldera_artifact_bytes_hash(b"placeholder-output"),
            compiler_identity: "placeholder".to_string(),
            runtime_packages: vec![FelderaRuntimePackageIdentity {
                name: "placeholder".to_string(),
                version: "placeholder".to_string(),
            }],
            package_feature_set: vec!["placeholder".to_string()],
            dbsp_runtime_compatibility: "placeholder".to_string(),
            checkpoint_codec_identity: "placeholder".to_string(),
            native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
        },
    };
    descriptor.standing_program_identity =
        feldera_package_runtime_identity_for_descriptor(&request.spec, &descriptor)?;
    validate_feldera_package_runtime_descriptor(
        &request.spec,
        &request.compile_request,
        &descriptor,
    )?;
    Ok(descriptor)
}

pub fn feldera_package_runtime_identity_for_descriptor(
    spec: &StandingViewSpec,
    descriptor: &FelderaPackageRuntimeDescriptorV1,
) -> Result<StandingProgramIdentity, FelderaProductRuntimeDescriptorError> {
    let input_schema_bytes = serde_json::to_vec(&descriptor.input_schemas)?;
    let output_schema_bytes = serde_json::to_vec(&descriptor.output_schemas)?;
    let identity = StandingProgramIdentity {
        tenant_id: "default".to_string(),
        program_id: spec.view_id.clone(),
        view_ids: standing_program_view_ids_for_spec(spec),
        sql_hash: feldera_artifact_bytes_hash(spec.sql.as_bytes()),
        input_catalog_hash: feldera_artifact_bytes_hash(&input_schema_bytes),
        output_schema_hash: feldera_artifact_bytes_hash(&output_schema_bytes),
        compiler_identity: format!("{}:{}", descriptor.backend.name, descriptor.backend.version),
        runtime_packages: vec![FelderaRuntimePackageIdentity {
            name: descriptor.runtime_factory.crate_name.clone(),
            version: descriptor.runtime_factory.crate_version.clone(),
        }],
        package_feature_set: vec![FELDERA_PACKAGE_RUNTIME_EXECUTION_PATH.to_string()],
        dbsp_runtime_compatibility: descriptor.runtime_factory.crate_version.clone(),
        checkpoint_codec_identity: descriptor.state_codec.clone(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
    };
    identity.validate()?;
    Ok(identity)
}

fn standing_program_view_ids_for_spec(spec: &StandingViewSpec) -> Vec<String> {
    let mut view_ids = Vec::new();
    for view_id in std::iter::once(&spec.view_id).chain(
        spec.output_relations
            .iter()
            .map(|schema| &schema.relation_id),
    ) {
        if !view_ids.iter().any(|seen| seen == view_id) {
            view_ids.push(view_id.clone());
        }
    }
    view_ids
}

fn require_non_empty(
    field: &'static str,
    value: &str,
) -> Result<(), FelderaProductRuntimeDescriptorError> {
    if value.trim().is_empty() {
        Err(FelderaProductRuntimeDescriptorError::MissingField { field })
    } else {
        Ok(())
    }
}
