use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode, UpdateVersion};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use velorix_core::{
    standing_program::StandingProgramIdentity,
    view_contract::{
        validate_materialized_standing_view_spec, view_spec_hash, StandingViewSpec,
        ViewContractError,
    },
    view_plan::VelorixLogicalViewPlanV1,
};

use crate::{
    capability::{ObjectStoreCapabilityError, ObjectStoreCapabilityProfile},
    object_key::{ObjectKey, ObjectKeyError},
};

#[derive(Clone, Debug)]
pub struct MaterializedViewRegistry {
    store: Arc<dyn ObjectStore>,
    conditional_update_supported: bool,
}

const LEGACY_ACTIVE_MATERIALIZED_VIEW_SCHEMA_VERSION: u16 = 1;
const ACTIVE_MATERIALIZED_VIEW_SCHEMA_VERSION_V2: u16 = 2;
const ACTIVE_MATERIALIZED_VIEW_SCHEMA_VERSION: u16 = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterMaterializedViewOutcome {
    Created,
    Duplicate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivateMaterializedViewOutcome {
    Activated,
    Duplicate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpdateMaterializedViewLifecycleOutcome {
    Updated,
    Duplicate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum MaterializedViewExecutionMode {
    StandingRuntime,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum MaterializedViewCompileStatus {
    NotRequired,
    Pending,
    CompilingSql,
    SqlCompiled,
    CompilingRust,
    Success,
    SqlError,
    RustError,
    SystemError,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum MaterializedViewDeploymentStatus {
    NotRequired,
    NotDeployed,
    Deploying,
    Running,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedViewLifecycleStatus {
    pub compiler_backend: String,
    pub compile_status: MaterializedViewCompileStatus,
    pub deployment_status: MaterializedViewDeploymentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl MaterializedViewLifecycleStatus {
    pub fn standing_runtime() -> Self {
        Self {
            compiler_backend: "materialized_view_runtime".to_string(),
            compile_status: MaterializedViewCompileStatus::Success,
            deployment_status: MaterializedViewDeploymentStatus::Running,
            message: None,
        }
    }

    pub fn standing_runtime_deploying(message: Option<String>) -> Self {
        Self {
            compiler_backend: "materialized_view_runtime".to_string(),
            compile_status: MaterializedViewCompileStatus::Success,
            deployment_status: MaterializedViewDeploymentStatus::Deploying,
            message,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveMaterializedViewRecord {
    pub schema_version: u16,
    pub view_id: String,
    pub spec_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_mode: Option<MaterializedViewExecutionMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api: Option<MaterializedViewApiMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<MaterializedViewArtifactBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<MaterializedViewRuntimeBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<MaterializedViewLifecycleStatus>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveMaterializedView {
    pub spec_hash: String,
    pub spec: StandingViewSpec,
    pub execution_mode: MaterializedViewExecutionMode,
    pub api: Option<MaterializedViewApiMetadata>,
    pub artifact: Option<MaterializedViewArtifactBinding>,
    pub runtime: Option<MaterializedViewRuntimeBinding>,
    pub lifecycle: MaterializedViewLifecycleStatus,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedViewArtifactBinding {
    pub artifact_id: String,
    pub artifact_hash: String,
    pub runtime_crate_name: String,
    pub state_codec: String,
    pub state_schema_version: u32,
    pub execution_status: String,
    pub execution_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standing_program_identity: Option<StandingProgramIdentity>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedViewRuntimeBinding {
    pub runtime_kind: String,
    pub runtime_version: String,
    pub standing_program_identity: StandingProgramIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_plan: Option<VelorixLogicalViewPlanV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedViewApiPathIndexRecord {
    pub schema_version: u16,
    pub normalized_url_path: String,
    pub view_id: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedViewApiMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(
        default,
        rename = "urlPath",
        alias = "url_path",
        skip_serializing_if = "Option::is_none"
    )]
    pub url_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_relation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub request: Vec<MaterializedViewRequestFieldSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_schema: Option<MaterializedViewResponseSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_template: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub response_formats: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_policy_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedViewRequestFieldSpec {
    #[serde(rename = "fieldName", alias = "field_name")]
    pub field_name: String,
    #[serde(rename = "fieldIn", alias = "field_in")]
    pub field_in: String,
    #[serde(default = "default_request_field_type", rename = "type")]
    pub r#type: String,
    #[serde(
        default,
        rename = "defaultValue",
        alias = "default_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub default_value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub validators: Vec<String>,
}

fn default_request_field_type() -> String {
    "string".to_string()
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedViewResponseSchema {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<MaterializedViewResponseColumnSpec>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializedViewResponseColumnSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub r#type: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvalidExecutionModeReason {
    StandingRuntimeMissingArtifact,
    StandingRuntimeMissingIdentity,
    MissingExecutionModeForCurrentSchema { schema_version: u16 },
}

impl std::fmt::Display for InvalidExecutionModeReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StandingRuntimeMissingArtifact => {
                write!(f, "standing_runtime requires an artifact binding")
            }
            Self::StandingRuntimeMissingIdentity => {
                write!(f, "standing_runtime requires standing_program_identity")
            }
            Self::MissingExecutionModeForCurrentSchema { schema_version } => write!(
                f,
                "schema_version {schema_version} requires explicit execution_mode"
            ),
        }
    }
}

#[derive(Debug, Error)]
pub enum MaterializedViewRegistryError {
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    #[error(transparent)]
    Validation(#[from] ViewContractError),
    #[error("materialized view registry record conflict at `{object_key}`")]
    RecordConflict { object_key: ObjectKey },
    #[error("materialized view registry record `{object_key}` body identity does not match key")]
    RecordIdentityMismatch { object_key: ObjectKey },
    #[error("active materialized view record conflict at `{object_key}`")]
    ActiveRecordConflict { object_key: ObjectKey },
    #[error("active materialized view record `{object_key}` requires object-store conditional update support")]
    ActiveRecordConditionalUpdateUnsupported { object_key: ObjectKey },
    #[error("invalid active materialized view execution mode for `{view_id}`: {reason}")]
    InvalidExecutionMode {
        view_id: String,
        reason: InvalidExecutionModeReason,
    },
    #[error(
        "view API path `{normalized_url_path}` is already assigned to `{existing_view_id}`, cannot assign to `{requested_view_id}`"
    )]
    ApiPathConflict {
        normalized_url_path: String,
        existing_view_id: String,
        requested_view_id: String,
    },
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

impl MaterializedViewRegistry {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            conditional_update_supported: true,
        }
    }

    pub fn new_checked(
        store: Arc<dyn ObjectStore>,
        profile: &ObjectStoreCapabilityProfile,
    ) -> Result<Self, ObjectStoreCapabilityError> {
        profile.validate_for_velorix_durability()?;

        Ok(Self {
            store,
            conditional_update_supported: profile.conditional_update,
        })
    }

    pub fn object_key(
        &self,
        view_id: &str,
        spec_hash: &str,
    ) -> Result<ObjectKey, MaterializedViewRegistryError> {
        Ok(ObjectKey::materialized_view(view_id, spec_hash)?)
    }

    pub async fn register(
        &self,
        spec: &StandingViewSpec,
    ) -> Result<RegisterMaterializedViewOutcome, MaterializedViewRegistryError> {
        self.register_with_api_metadata(spec, None).await
    }

    pub async fn register_with_api_metadata(
        &self,
        spec: &StandingViewSpec,
        api: Option<MaterializedViewApiMetadata>,
    ) -> Result<RegisterMaterializedViewOutcome, MaterializedViewRegistryError> {
        self.register_with_api_metadata_and_artifact(spec, api, None)
            .await
    }

    pub async fn register_with_api_metadata_and_artifact(
        &self,
        spec: &StandingViewSpec,
        api: Option<MaterializedViewApiMetadata>,
        artifact: Option<MaterializedViewArtifactBinding>,
    ) -> Result<RegisterMaterializedViewOutcome, MaterializedViewRegistryError> {
        self.register_with_api_metadata_artifact_execution(spec, api, artifact, None, None)
            .await
    }

    pub async fn register_with_api_metadata_runtime_execution(
        &self,
        spec: &StandingViewSpec,
        api: Option<MaterializedViewApiMetadata>,
        runtime: MaterializedViewRuntimeBinding,
        lifecycle: Option<MaterializedViewLifecycleStatus>,
    ) -> Result<RegisterMaterializedViewOutcome, MaterializedViewRegistryError> {
        self.register_with_api_metadata_artifact_runtime_execution(
            spec,
            api,
            None,
            Some(runtime),
            Some(MaterializedViewExecutionMode::StandingRuntime),
            lifecycle,
        )
        .await
    }

    pub async fn register_with_api_metadata_artifact_execution(
        &self,
        spec: &StandingViewSpec,
        api: Option<MaterializedViewApiMetadata>,
        artifact: Option<MaterializedViewArtifactBinding>,
        execution_mode: Option<MaterializedViewExecutionMode>,
        lifecycle: Option<MaterializedViewLifecycleStatus>,
    ) -> Result<RegisterMaterializedViewOutcome, MaterializedViewRegistryError> {
        self.register_with_api_metadata_artifact_runtime_execution(
            spec,
            api,
            artifact,
            None,
            execution_mode,
            lifecycle,
        )
        .await
    }

    async fn register_with_api_metadata_artifact_runtime_execution(
        &self,
        spec: &StandingViewSpec,
        api: Option<MaterializedViewApiMetadata>,
        artifact: Option<MaterializedViewArtifactBinding>,
        runtime: Option<MaterializedViewRuntimeBinding>,
        execution_mode: Option<MaterializedViewExecutionMode>,
        lifecycle: Option<MaterializedViewLifecycleStatus>,
    ) -> Result<RegisterMaterializedViewOutcome, MaterializedViewRegistryError> {
        validate_materialized_standing_view_spec(spec)?;

        let spec_hash = view_spec_hash(spec)?;
        let object_key = self.object_key(&spec.view_id, &spec_hash)?;
        let bytes = serde_json::to_vec(spec)?;
        let result = self
            .store
            .put_opts(
                &Path::from(object_key.as_str()),
                Bytes::from(bytes).into(),
                PutMode::Create.into(),
            )
            .await;

        let outcome = match result {
            Ok(_) => Ok(RegisterMaterializedViewOutcome::Created),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self.read_object(&object_key).await?;
                if existing == *spec {
                    Ok(RegisterMaterializedViewOutcome::Duplicate)
                } else {
                    Err(MaterializedViewRegistryError::RecordConflict { object_key })
                }
            }
            Err(error) => Err(error.into()),
        }?;

        if let Some(api) = &api {
            if let Some(url_path) = &api.url_path {
                self.write_api_path_index(url_path, &spec.view_id).await?;
            }
        }

        self.write_active_record(
            spec.view_id.as_str(),
            spec_hash.as_str(),
            api,
            artifact,
            runtime,
            execution_mode,
            lifecycle,
        )
        .await?;

        Ok(outcome)
    }

    pub async fn read(
        &self,
        view_id: &str,
        spec_hash: &str,
    ) -> Result<StandingViewSpec, MaterializedViewRegistryError> {
        let object_key = self.object_key(view_id, spec_hash)?;
        let record = self.read_object(&object_key).await?;

        validate_materialized_standing_view_spec(&record)?;
        self.validate_record_identity(&object_key, &record)?;

        Ok(record)
    }

    pub async fn read_active(
        &self,
        view_id: &str,
    ) -> Result<ActiveMaterializedView, MaterializedViewRegistryError> {
        let object_key = self.active_object_key(view_id)?;
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;
        let record: ActiveMaterializedViewRecord = serde_json::from_slice(&bytes)?;
        self.validate_active_record_identity(view_id, &record, &object_key)?;
        let execution_mode = self.normalized_execution_mode(&record)?;
        let lifecycle = self.normalized_lifecycle(&record, &execution_mode);
        let spec = self.read(view_id, &record.spec_hash).await?;

        Ok(ActiveMaterializedView {
            spec_hash: record.spec_hash,
            spec,
            execution_mode,
            api: record.api,
            artifact: record.artifact,
            runtime: record.runtime,
            lifecycle,
        })
    }

    pub async fn list_active(
        &self,
    ) -> Result<Vec<ActiveMaterializedView>, MaterializedViewRegistryError> {
        let mut stream = self.store.list(Some(&Path::from("v1/views")));
        let mut active = Vec::new();

        while let Some(meta) = stream.try_next().await? {
            let location = meta.location.to_string();
            if !location.ends_with("/active.json") {
                continue;
            }
            let object_key = ObjectKey::parse(location)?;
            let bytes = self
                .store
                .get(&Path::from(object_key.as_str()))
                .await?
                .bytes()
                .await?;
            let record: ActiveMaterializedViewRecord = serde_json::from_slice(&bytes)?;
            self.validate_active_record_identity(&record.view_id, &record, &object_key)?;
            let execution_mode = self.normalized_execution_mode(&record)?;
            let lifecycle = self.normalized_lifecycle(&record, &execution_mode);
            let spec = self.read(&record.view_id, &record.spec_hash).await?;
            active.push(ActiveMaterializedView {
                spec_hash: record.spec_hash,
                spec,
                execution_mode,
                api: record.api,
                artifact: record.artifact,
                runtime: record.runtime,
                lifecycle,
            });
        }

        active.sort_by(|left, right| left.spec.view_id.cmp(&right.spec.view_id));
        Ok(active)
    }

    pub async fn update_standing_runtime_lifecycle(
        &self,
        view_id: &str,
        spec_hash: &str,
        lifecycle: MaterializedViewLifecycleStatus,
    ) -> Result<UpdateMaterializedViewLifecycleOutcome, MaterializedViewRegistryError> {
        let object_key = self.active_object_key(view_id)?;
        let get_result = self.store.get(&Path::from(object_key.as_str())).await?;
        let update_version = UpdateVersion {
            e_tag: get_result.meta.e_tag.clone(),
            version: get_result.meta.version.clone(),
        };
        let bytes = get_result.bytes().await?;
        let existing: ActiveMaterializedViewRecord = serde_json::from_slice(&bytes)?;
        self.validate_active_record_identity(view_id, &existing, &object_key)?;
        let existing_mode = self.normalized_execution_mode(&existing)?;
        if existing.spec_hash != spec_hash
            || existing_mode != MaterializedViewExecutionMode::StandingRuntime
            || (existing.artifact.is_none() && existing.runtime.is_none())
        {
            return Err(MaterializedViewRegistryError::ActiveRecordConflict { object_key });
        }

        let record = ActiveMaterializedViewRecord {
            schema_version: ACTIVE_MATERIALIZED_VIEW_SCHEMA_VERSION,
            view_id: existing.view_id.clone(),
            spec_hash: existing.spec_hash.clone(),
            execution_mode: Some(MaterializedViewExecutionMode::StandingRuntime),
            api: existing.api.clone(),
            artifact: existing.artifact.clone(),
            runtime: existing.runtime.clone(),
            lifecycle: Some(lifecycle),
        };
        self.validate_execution_mode(
            view_id,
            &MaterializedViewExecutionMode::StandingRuntime,
            &record.artifact,
            &record.runtime,
        )?;

        if existing == record {
            return Ok(UpdateMaterializedViewLifecycleOutcome::Duplicate);
        }

        self.put_active_record_update(&object_key, update_version, &record)
            .await?;

        Ok(UpdateMaterializedViewLifecycleOutcome::Updated)
    }

    async fn put_active_record_update(
        &self,
        object_key: &ObjectKey,
        update_version: UpdateVersion,
        record: &ActiveMaterializedViewRecord,
    ) -> Result<(), MaterializedViewRegistryError> {
        let path = Path::from(object_key.as_str());
        let new_bytes = Bytes::from(serde_json::to_vec(record)?);
        if !self.conditional_update_supported {
            return Err(
                MaterializedViewRegistryError::ActiveRecordConditionalUpdateUnsupported {
                    object_key: object_key.clone(),
                },
            );
        }
        if update_version.e_tag.is_none() && update_version.version.is_none() {
            return Err(
                MaterializedViewRegistryError::ActiveRecordConditionalUpdateUnsupported {
                    object_key: object_key.clone(),
                },
            );
        }

        let update_result = self
            .store
            .put_opts(
                &path,
                new_bytes.into(),
                PutMode::Update(update_version).into(),
            )
            .await;
        update_result
            .map_err(|error| active_update_error_to_registry(error, object_key.clone()))?;

        Ok(())
    }

    pub async fn list_api_path_indexes(
        &self,
    ) -> Result<Vec<MaterializedViewApiPathIndexRecord>, MaterializedViewRegistryError> {
        let mut stream = self
            .store
            .list(Some(&Path::from("v1/view-api-paths/sha256")));
        let mut indexes = Vec::new();

        while let Some(meta) = stream.try_next().await? {
            let location = meta.location.to_string();
            if !location.ends_with(".json") {
                continue;
            }
            let bytes = self.store.get(&meta.location).await?.bytes().await?;
            let record: MaterializedViewApiPathIndexRecord = serde_json::from_slice(&bytes)?;
            indexes.push(record);
        }

        indexes.sort_by(|left, right| left.normalized_url_path.cmp(&right.normalized_url_path));
        Ok(indexes)
    }

    async fn read_object(
        &self,
        object_key: &ObjectKey,
    ) -> Result<StandingViewSpec, MaterializedViewRegistryError> {
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;

        Ok(serde_json::from_slice(&bytes)?)
    }

    fn validate_record_identity(
        &self,
        object_key: &ObjectKey,
        record: &StandingViewSpec,
    ) -> Result<(), MaterializedViewRegistryError> {
        let spec_hash = view_spec_hash(record)?;
        if *object_key == self.object_key(&record.view_id, &spec_hash)? {
            Ok(())
        } else {
            Err(MaterializedViewRegistryError::RecordIdentityMismatch {
                object_key: object_key.clone(),
            })
        }
    }

    fn active_object_key(&self, view_id: &str) -> Result<ObjectKey, MaterializedViewRegistryError> {
        Ok(ObjectKey::active_materialized_view(view_id)?)
    }

    async fn write_active_record(
        &self,
        view_id: &str,
        spec_hash: &str,
        api: Option<MaterializedViewApiMetadata>,
        artifact: Option<MaterializedViewArtifactBinding>,
        runtime: Option<MaterializedViewRuntimeBinding>,
        execution_mode: Option<MaterializedViewExecutionMode>,
        lifecycle: Option<MaterializedViewLifecycleStatus>,
    ) -> Result<(), MaterializedViewRegistryError> {
        let object_key = self.active_object_key(view_id)?;
        let execution_mode = match execution_mode {
            Some(mode) => {
                self.validate_execution_mode(view_id, &mode, &artifact, &runtime)?;
                mode
            }
            None => self.execution_mode_for_new_record(view_id, &artifact, &runtime)?,
        };
        let lifecycle = lifecycle.unwrap_or_else(|| self.lifecycle_for_mode(&execution_mode));
        let record = ActiveMaterializedViewRecord {
            schema_version: ACTIVE_MATERIALIZED_VIEW_SCHEMA_VERSION,
            view_id: view_id.to_string(),
            spec_hash: spec_hash.to_string(),
            execution_mode: Some(execution_mode),
            api,
            artifact,
            runtime,
            lifecycle: Some(lifecycle),
        };
        let bytes = serde_json::to_vec(&record)?;
        let result = self
            .store
            .put_opts(
                &Path::from(object_key.as_str()),
                Bytes::from(bytes).into(),
                PutMode::Create.into(),
            )
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let bytes = self
                    .store
                    .get(&Path::from(object_key.as_str()))
                    .await?
                    .bytes()
                    .await?;
                let existing: ActiveMaterializedViewRecord = serde_json::from_slice(&bytes)?;
                if existing == record {
                    Ok(())
                } else {
                    Err(MaterializedViewRegistryError::ActiveRecordConflict { object_key })
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn write_api_path_index(
        &self,
        url_path: &str,
        view_id: &str,
    ) -> Result<(), MaterializedViewRegistryError> {
        let normalized_url_path = normalize_api_path(url_path);
        let path = api_path_index_object_path(&normalized_url_path);
        let record = MaterializedViewApiPathIndexRecord {
            schema_version: 1,
            normalized_url_path,
            view_id: view_id.to_string(),
        };
        let bytes = serde_json::to_vec(&record)?;
        let result = self
            .store
            .put_opts(&path, Bytes::from(bytes).into(), PutMode::Create.into())
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let bytes = self.store.get(&path).await?.bytes().await?;
                let existing: MaterializedViewApiPathIndexRecord = serde_json::from_slice(&bytes)?;
                if existing == record {
                    Ok(())
                } else {
                    Err(MaterializedViewRegistryError::ApiPathConflict {
                        normalized_url_path: record.normalized_url_path,
                        existing_view_id: existing.view_id,
                        requested_view_id: record.view_id,
                    })
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    fn validate_active_record_identity(
        &self,
        view_id: &str,
        record: &ActiveMaterializedViewRecord,
        object_key: &ObjectKey,
    ) -> Result<(), MaterializedViewRegistryError> {
        if matches!(
            record.schema_version,
            LEGACY_ACTIVE_MATERIALIZED_VIEW_SCHEMA_VERSION
                | ACTIVE_MATERIALIZED_VIEW_SCHEMA_VERSION_V2
                | ACTIVE_MATERIALIZED_VIEW_SCHEMA_VERSION
        ) && record.view_id == view_id
            && *object_key == self.active_object_key(&record.view_id)?
        {
            Ok(())
        } else {
            Err(MaterializedViewRegistryError::RecordIdentityMismatch {
                object_key: object_key.clone(),
            })
        }
    }

    fn execution_mode_for_new_record(
        &self,
        view_id: &str,
        artifact: &Option<MaterializedViewArtifactBinding>,
        runtime: &Option<MaterializedViewRuntimeBinding>,
    ) -> Result<MaterializedViewExecutionMode, MaterializedViewRegistryError> {
        let mode = MaterializedViewExecutionMode::StandingRuntime;
        self.validate_execution_mode(view_id, &mode, artifact, runtime)?;
        Ok(mode)
    }

    fn normalized_execution_mode(
        &self,
        record: &ActiveMaterializedViewRecord,
    ) -> Result<MaterializedViewExecutionMode, MaterializedViewRegistryError> {
        let mode = match &record.execution_mode {
            Some(mode) => mode.clone(),
            None if record.schema_version == LEGACY_ACTIVE_MATERIALIZED_VIEW_SCHEMA_VERSION => {
                MaterializedViewExecutionMode::StandingRuntime
            }
            None => {
                return Err(MaterializedViewRegistryError::InvalidExecutionMode {
                    view_id: record.view_id.clone(),
                    reason: InvalidExecutionModeReason::MissingExecutionModeForCurrentSchema {
                        schema_version: record.schema_version,
                    },
                });
            }
        };
        self.validate_execution_mode(&record.view_id, &mode, &record.artifact, &record.runtime)?;
        Ok(mode)
    }

    fn normalized_lifecycle(
        &self,
        record: &ActiveMaterializedViewRecord,
        mode: &MaterializedViewExecutionMode,
    ) -> MaterializedViewLifecycleStatus {
        record
            .lifecycle
            .clone()
            .unwrap_or_else(|| self.lifecycle_for_mode(mode))
    }

    fn lifecycle_for_mode(
        &self,
        mode: &MaterializedViewExecutionMode,
    ) -> MaterializedViewLifecycleStatus {
        match mode {
            MaterializedViewExecutionMode::StandingRuntime => {
                MaterializedViewLifecycleStatus::standing_runtime()
            }
        }
    }

    fn validate_execution_mode(
        &self,
        view_id: &str,
        mode: &MaterializedViewExecutionMode,
        artifact: &Option<MaterializedViewArtifactBinding>,
        runtime: &Option<MaterializedViewRuntimeBinding>,
    ) -> Result<(), MaterializedViewRegistryError> {
        let MaterializedViewExecutionMode::StandingRuntime = mode;
        let has_artifact_identity = artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
            .is_some();
        let has_runtime_identity = runtime.is_some();
        if !has_artifact_identity && !has_runtime_identity {
            return Err(MaterializedViewRegistryError::InvalidExecutionMode {
                view_id: view_id.to_string(),
                reason: InvalidExecutionModeReason::StandingRuntimeMissingArtifact,
            });
        }

        Ok(())
    }
}

fn normalize_api_path(path: &str) -> String {
    path.trim_matches('/').to_string()
}

fn active_update_error_to_registry(
    error: object_store::Error,
    object_key: ObjectKey,
) -> MaterializedViewRegistryError {
    match error {
        object_store::Error::Precondition { .. } => {
            MaterializedViewRegistryError::ActiveRecordConflict { object_key }
        }
        error => MaterializedViewRegistryError::ObjectStore(error),
    }
}

fn api_path_index_object_path(normalized_url_path: &str) -> Path {
    let digest = Sha256::digest(normalized_url_path.as_bytes());
    let hash = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Path::from(format!("v1/view-api-paths/sha256/{hash}.json"))
}
