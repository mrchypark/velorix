use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    materialized_view_registry::{
        MaterializedViewCompileStatus, MaterializedViewDeploymentStatus,
        MaterializedViewLifecycleStatus,
    },
    object_key::{ObjectKey, ObjectKeyError},
};
use velorix_core::feldera_artifact::{RelationSchema, StandingViewShape, StandingViewSpec};

#[derive(Clone, Debug)]
pub struct ViewCompileDeployJobRegistry {
    store: Arc<dyn ObjectStore>,
}

const VIEW_COMPILE_DEPLOY_JOB_SCHEMA_VERSION: u16 = 1;
const FELDERA_STANDING_VIEW_COMPILE_REQUEST_KIND: &str = "feldera_standing_view_compile_request_v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ViewCompileDeployJobRecord {
    pub schema_version: u16,
    pub job_id: String,
    pub view_id: String,
    pub spec_hash: String,
    pub compiler_backend: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compiler_request: Option<ViewCompileDeployCompilerRequestV1>,
    pub compile_status: MaterializedViewCompileStatus,
    pub deployment_status: MaterializedViewDeploymentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ViewCompileDeployCompilerRequestV1 {
    pub request_kind: String,
    pub view_id: String,
    pub spec_hash: String,
    pub sql: String,
    pub input_relations: Vec<RelationSchema>,
    pub output_relations: Vec<RelationSchema>,
    pub shape: StandingViewShape,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegisterViewCompileDeployJobOutcome {
    Created,
    Duplicate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompleteViewCompileDeployJobOutcome {
    Completed,
    Duplicate,
}

#[derive(Debug, Error)]
pub enum ViewCompileDeployJobRegistryError {
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    #[error("view compile/deploy job record conflict at `{object_key}`")]
    RecordConflict { object_key: ObjectKey },
    #[error("view compile/deploy job record `{object_key}` body identity does not match key")]
    RecordIdentityMismatch { object_key: ObjectKey },
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

impl ViewCompileDeployJobRegistry {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub fn object_key(
        &self,
        view_id: &str,
        spec_hash: &str,
    ) -> Result<ObjectKey, ViewCompileDeployJobRegistryError> {
        Ok(ObjectKey::view_compile_deploy_job(view_id, spec_hash)?)
    }

    pub async fn register_pending(
        &self,
        view_id: &str,
        spec_hash: &str,
        lifecycle: &MaterializedViewLifecycleStatus,
    ) -> Result<RegisterViewCompileDeployJobOutcome, ViewCompileDeployJobRegistryError> {
        let record = ViewCompileDeployJobRecord {
            schema_version: VIEW_COMPILE_DEPLOY_JOB_SCHEMA_VERSION,
            job_id: view_compile_deploy_job_id(view_id, spec_hash),
            view_id: view_id.to_string(),
            spec_hash: spec_hash.to_string(),
            compiler_backend: lifecycle.compiler_backend.clone(),
            compiler_request: None,
            compile_status: lifecycle.compile_status.clone(),
            deployment_status: lifecycle.deployment_status.clone(),
            message: lifecycle.message.clone(),
        };
        self.register_pending_record(record).await
    }

    pub async fn register_pending_for_spec(
        &self,
        spec: &StandingViewSpec,
        spec_hash: &str,
        lifecycle: &MaterializedViewLifecycleStatus,
    ) -> Result<RegisterViewCompileDeployJobOutcome, ViewCompileDeployJobRegistryError> {
        let record = ViewCompileDeployJobRecord {
            schema_version: VIEW_COMPILE_DEPLOY_JOB_SCHEMA_VERSION,
            job_id: view_compile_deploy_job_id(&spec.view_id, spec_hash),
            view_id: spec.view_id.clone(),
            spec_hash: spec_hash.to_string(),
            compiler_backend: lifecycle.compiler_backend.clone(),
            compiler_request: Some(ViewCompileDeployCompilerRequestV1 {
                request_kind: FELDERA_STANDING_VIEW_COMPILE_REQUEST_KIND.to_string(),
                view_id: spec.view_id.clone(),
                spec_hash: spec_hash.to_string(),
                sql: spec.sql.clone(),
                input_relations: spec.input_relations.clone(),
                output_relations: spec.output_relations.clone(),
                shape: spec.shape.clone(),
            }),
            compile_status: lifecycle.compile_status.clone(),
            deployment_status: lifecycle.deployment_status.clone(),
            message: lifecycle.message.clone(),
        };
        self.register_pending_record(record).await
    }

    async fn register_pending_record(
        &self,
        record: ViewCompileDeployJobRecord,
    ) -> Result<RegisterViewCompileDeployJobOutcome, ViewCompileDeployJobRegistryError> {
        let object_key = self.object_key(&record.view_id, &record.spec_hash)?;
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
            Ok(_) => Ok(RegisterViewCompileDeployJobOutcome::Created),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self.read(&record.view_id, &record.spec_hash).await?;
                if existing == record {
                    Ok(RegisterViewCompileDeployJobOutcome::Duplicate)
                } else {
                    Err(ViewCompileDeployJobRegistryError::RecordConflict { object_key })
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn read(
        &self,
        view_id: &str,
        spec_hash: &str,
    ) -> Result<ViewCompileDeployJobRecord, ViewCompileDeployJobRegistryError> {
        let object_key = self.object_key(view_id, spec_hash)?;
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;
        let record: ViewCompileDeployJobRecord = serde_json::from_slice(&bytes)?;
        validate_record_identity(&object_key, &record)?;
        Ok(record)
    }

    pub async fn list_pending(
        &self,
    ) -> Result<Vec<ViewCompileDeployJobRecord>, ViewCompileDeployJobRegistryError> {
        let mut stream = self
            .store
            .list(Some(&Path::from("v1/view-compile-deploy-jobs")));
        let mut records = Vec::new();

        while let Some(meta) = stream.try_next().await? {
            let location = meta.location.to_string();
            if !location.ends_with(".job.json") {
                continue;
            }
            let object_key = ObjectKey::parse(location)?;
            let bytes = self.store.get(&meta.location).await?.bytes().await?;
            let record: ViewCompileDeployJobRecord = serde_json::from_slice(&bytes)?;
            validate_record_identity(&object_key, &record)?;
            if record.compile_status == MaterializedViewCompileStatus::Pending
                && record.deployment_status == MaterializedViewDeploymentStatus::NotDeployed
            {
                records.push(record);
            }
        }

        records.sort_by(|left, right| left.job_id.cmp(&right.job_id));
        Ok(records)
    }

    pub async fn mark_running(
        &self,
        view_id: &str,
        spec_hash: &str,
        message: Option<String>,
    ) -> Result<CompleteViewCompileDeployJobOutcome, ViewCompileDeployJobRegistryError> {
        let object_key = self.object_key(view_id, spec_hash)?;
        let existing = self.read(view_id, spec_hash).await?;
        if existing.compile_status == MaterializedViewCompileStatus::Success
            && existing.deployment_status == MaterializedViewDeploymentStatus::Running
        {
            return Ok(CompleteViewCompileDeployJobOutcome::Duplicate);
        }
        if existing.compile_status != MaterializedViewCompileStatus::Pending
            || existing.deployment_status != MaterializedViewDeploymentStatus::NotDeployed
        {
            return Err(ViewCompileDeployJobRegistryError::RecordConflict { object_key });
        }
        let record = ViewCompileDeployJobRecord {
            schema_version: VIEW_COMPILE_DEPLOY_JOB_SCHEMA_VERSION,
            job_id: existing.job_id.clone(),
            view_id: existing.view_id.clone(),
            spec_hash: existing.spec_hash.clone(),
            compiler_backend: existing.compiler_backend.clone(),
            compiler_request: existing.compiler_request.clone(),
            compile_status: MaterializedViewCompileStatus::Success,
            deployment_status: MaterializedViewDeploymentStatus::Running,
            message,
        };
        self.store
            .put(
                &Path::from(object_key.as_str()),
                Bytes::from(serde_json::to_vec(&record)?).into(),
            )
            .await?;
        Ok(CompleteViewCompileDeployJobOutcome::Completed)
    }
}

pub fn view_compile_deploy_job_id(view_id: &str, spec_hash: &str) -> String {
    format!("{view_id}:{spec_hash}")
}

fn validate_record_identity(
    object_key: &ObjectKey,
    record: &ViewCompileDeployJobRecord,
) -> Result<(), ViewCompileDeployJobRegistryError> {
    if *object_key != ObjectKey::view_compile_deploy_job(&record.view_id, &record.spec_hash)?
        || record.job_id != view_compile_deploy_job_id(&record.view_id, &record.spec_hash)
        || record.schema_version != VIEW_COMPILE_DEPLOY_JOB_SCHEMA_VERSION
    {
        return Err(ViewCompileDeployJobRegistryError::RecordIdentityMismatch {
            object_key: object_key.clone(),
        });
    }

    if let Some(request) = &record.compiler_request {
        validate_compiler_request_identity(object_key, record, request)?;
    }

    Ok(())
}

fn validate_compiler_request_identity(
    object_key: &ObjectKey,
    record: &ViewCompileDeployJobRecord,
    request: &ViewCompileDeployCompilerRequestV1,
) -> Result<(), ViewCompileDeployJobRegistryError> {
    let request_matches_record = request.request_kind == FELDERA_STANDING_VIEW_COMPILE_REQUEST_KIND
        && request.view_id == record.view_id
        && request.spec_hash == record.spec_hash
        && !request.sql.trim().is_empty()
        && !request.input_relations.is_empty()
        && !request.output_relations.is_empty()
        && request.shape.is_materialized;
    if request_matches_record {
        Ok(())
    } else {
        Err(ViewCompileDeployJobRegistryError::RecordIdentityMismatch {
            object_key: object_key.clone(),
        })
    }
}
