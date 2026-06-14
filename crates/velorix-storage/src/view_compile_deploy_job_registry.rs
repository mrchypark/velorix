use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode, UpdateVersion};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    materialized_view_registry::{
        MaterializedViewCompileStatus, MaterializedViewDeploymentStatus,
        MaterializedViewLifecycleStatus,
    },
    object_key::{ObjectKey, ObjectKeyError},
};
use velorix_core::feldera_artifact::{
    feldera_compile_request_hash, FelderaArtifactError, FelderaCompileRequestV1,
    FelderaRustExtensionV1, OutputSchemaContract, RelationSchema, SqlDialect, SqlSourceKind,
    StandingViewShape, StandingViewSpec,
};

#[derive(Clone, Debug)]
pub struct ViewCompileDeployJobRegistry {
    store: Arc<dyn ObjectStore>,
}

const VIEW_COMPILE_DEPLOY_JOB_SCHEMA_VERSION: u16 = 1;
const VIEW_COMPILE_DEPLOY_JOB_CLAIM_SCHEMA_VERSION: u16 = 1;
const FELDERA_STANDING_VIEW_COMPILE_REQUEST_KIND: &str = "feldera_standing_view_compile_request_v1";

fn default_tenant_id() -> String {
    "default".to_string()
}

fn default_job_generation() -> u64 {
    1
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ViewCompileDeployJobRecord {
    pub schema_version: u16,
    pub job_id: String,
    #[serde(default = "default_tenant_id")]
    pub tenant_id: String,
    pub view_id: String,
    #[serde(default = "default_job_generation")]
    pub job_generation: u64,
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
    pub compile_request_hash: String,
    pub spec_hash: String,
    pub sql: String,
    pub dialect: SqlDialect,
    pub source_kind: SqlSourceKind,
    #[serde(default, skip_serializing_if = "FelderaRustExtensionV1::is_empty")]
    pub rust_extension: FelderaRustExtensionV1,
    pub input_relations: Vec<RelationSchema>,
    pub output_contract: OutputSchemaContract,
    pub output_relations: Vec<RelationSchema>,
    pub shape: StandingViewShape,
}

impl ViewCompileDeployCompilerRequestV1 {
    pub fn feldera_compile_request(&self) -> FelderaCompileRequestV1 {
        FelderaCompileRequestV1 {
            view_id: self.view_id.clone(),
            sql: self.sql.clone(),
            dialect: self.dialect.clone(),
            source_kind: self.source_kind.clone(),
            rust_extension: self.rust_extension.clone(),
            input_relations: self.input_relations.clone(),
            output_contract: self.output_contract.clone(),
            shape: self.shape.clone(),
        }
    }
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ViewCompileDeployJobClaimRecord {
    pub schema_version: u16,
    pub claim_id: String,
    pub job_id: String,
    pub tenant_id: String,
    pub view_id: String,
    pub job_generation: u64,
    pub compile_request_hash: String,
    pub worker_id: String,
    pub lease_id: String,
    pub fencing_token: u64,
    pub claimed_at_ms: u64,
    pub lease_duration_ms: u64,
    pub lease_expires_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ViewCompileDeployJobClaimOutcome {
    Claimed(ViewCompileDeployJobClaimRecord),
    Duplicate(ViewCompileDeployJobClaimRecord),
}

#[derive(Debug, Error)]
pub enum ViewCompileDeployJobRegistryError {
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    #[error("view compile/deploy job record conflict at `{object_key}`")]
    RecordConflict { object_key: ObjectKey },
    #[error(
        "view compile/deploy job claim conflict at `{object_key}`: active worker `{worker_id}` lease expires at {lease_expires_at_ms} ms"
    )]
    ActiveClaim {
        object_key: ObjectKey,
        worker_id: String,
        lease_expires_at_ms: u64,
    },
    #[error("view compile/deploy job record `{object_key}` body identity does not match key")]
    RecordIdentityMismatch { object_key: ObjectKey },
    #[error(transparent)]
    CompileRequest(#[from] FelderaArtifactError),
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

    pub fn object_key_for_compile_request(
        &self,
        view_id: &str,
        compile_request_hash: &str,
    ) -> Result<ObjectKey, ViewCompileDeployJobRegistryError> {
        Ok(ObjectKey::view_compile_deploy_job_for_compile_request(
            view_id,
            compile_request_hash,
        )?)
    }

    pub fn claim_object_key_for_compile_request(
        &self,
        view_id: &str,
        compile_request_hash: &str,
    ) -> Result<ObjectKey, ViewCompileDeployJobRegistryError> {
        Ok(
            ObjectKey::view_compile_deploy_job_claim_for_compile_request(
                view_id,
                compile_request_hash,
            )?,
        )
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
            tenant_id: default_tenant_id(),
            view_id: view_id.to_string(),
            job_generation: default_job_generation(),
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
        let compiler_request = compiler_request_for_spec(spec, spec_hash)?;
        let record = ViewCompileDeployJobRecord {
            schema_version: VIEW_COMPILE_DEPLOY_JOB_SCHEMA_VERSION,
            job_id: view_compile_deploy_compile_request_job_id(
                &spec.view_id,
                &compiler_request.compile_request_hash,
            ),
            tenant_id: default_tenant_id(),
            view_id: spec.view_id.clone(),
            job_generation: default_job_generation(),
            spec_hash: spec_hash.to_string(),
            compiler_backend: lifecycle.compiler_backend.clone(),
            compiler_request: Some(compiler_request),
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
        let object_key = self.object_key_for_record(&record)?;
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
                let existing = self.read_at_object_key(object_key.clone()).await?;
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
        self.read_at_object_key(object_key).await
    }

    pub async fn read_by_compile_request_hash(
        &self,
        view_id: &str,
        compile_request_hash: &str,
    ) -> Result<ViewCompileDeployJobRecord, ViewCompileDeployJobRegistryError> {
        let object_key = self.object_key_for_compile_request(view_id, compile_request_hash)?;
        self.read_at_object_key(object_key).await
    }

    async fn read_at_object_key(
        &self,
        object_key: ObjectKey,
    ) -> Result<ViewCompileDeployJobRecord, ViewCompileDeployJobRegistryError> {
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

        let mut deduped = BTreeMap::new();
        for record in records {
            let identity = record_dedup_identity(&record);
            deduped.entry(identity).or_insert(record);
        }
        let mut records = deduped.into_values().collect::<Vec<_>>();
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
        self.mark_running_at_object_key(object_key, message).await
    }

    pub async fn mark_running_for_compile_request_hash(
        &self,
        view_id: &str,
        compile_request_hash: &str,
        message: Option<String>,
    ) -> Result<CompleteViewCompileDeployJobOutcome, ViewCompileDeployJobRegistryError> {
        let object_key = self.object_key_for_compile_request(view_id, compile_request_hash)?;
        self.mark_running_at_object_key(object_key, message).await
    }

    pub async fn mark_compile_validated_for_compile_request_hash(
        &self,
        view_id: &str,
        compile_request_hash: &str,
        message: Option<String>,
    ) -> Result<CompleteViewCompileDeployJobOutcome, ViewCompileDeployJobRegistryError> {
        let object_key = self.object_key_for_compile_request(view_id, compile_request_hash)?;
        self.mark_compile_validated_at_object_key(object_key, message)
            .await
    }

    pub async fn claim_pending_for_compile_request_hash(
        &self,
        view_id: &str,
        compile_request_hash: &str,
        worker_id: &str,
        now_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<ViewCompileDeployJobClaimOutcome, ViewCompileDeployJobRegistryError> {
        validate_worker_id(worker_id)?;
        if lease_duration_ms == 0 {
            return Err(ViewCompileDeployJobRegistryError::RecordIdentityMismatch {
                object_key: self
                    .claim_object_key_for_compile_request(view_id, compile_request_hash)?,
            });
        }
        let job = self
            .read_by_compile_request_hash(view_id, compile_request_hash)
            .await?;
        if job.compile_status != MaterializedViewCompileStatus::Pending
            || job.deployment_status != MaterializedViewDeploymentStatus::NotDeployed
        {
            return Err(ViewCompileDeployJobRegistryError::RecordConflict {
                object_key: self.object_key_for_record(&job)?,
            });
        }
        let compiler_request = job.compiler_request.as_ref().ok_or_else(|| {
            ViewCompileDeployJobRegistryError::RecordIdentityMismatch {
                object_key: self.object_key_for_record(&job).unwrap_or_else(|_| {
                    ObjectKey::parse("v1/view-compile-deploy-jobs/invalid.job.json")
                        .expect("static object key is parseable")
                }),
            }
        })?;
        if compiler_request.compile_request_hash != compile_request_hash {
            return Err(ViewCompileDeployJobRegistryError::RecordIdentityMismatch {
                object_key: self.object_key_for_record(&job)?,
            });
        }
        let claim_object_key =
            self.claim_object_key_for_compile_request(view_id, compile_request_hash)?;
        let new_claim = claim_record_for_job(
            &job,
            compile_request_hash,
            worker_id,
            now_ms,
            lease_duration_ms,
            1,
        );
        let bytes = Bytes::from(serde_json::to_vec(&new_claim)?);
        let create_result = self
            .store
            .put_opts(
                &Path::from(claim_object_key.as_str()),
                bytes.into(),
                PutMode::Create.into(),
            )
            .await;
        match create_result {
            Ok(_) => Ok(ViewCompileDeployJobClaimOutcome::Claimed(new_claim)),
            Err(object_store::Error::AlreadyExists { .. }) => {
                self.replace_expired_or_read_active_claim(
                    claim_object_key,
                    &job,
                    compile_request_hash,
                    worker_id,
                    now_ms,
                    lease_duration_ms,
                )
                .await
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn read_claim_by_compile_request_hash(
        &self,
        view_id: &str,
        compile_request_hash: &str,
    ) -> Result<ViewCompileDeployJobClaimRecord, ViewCompileDeployJobRegistryError> {
        let object_key =
            self.claim_object_key_for_compile_request(view_id, compile_request_hash)?;
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;
        let record: ViewCompileDeployJobClaimRecord = serde_json::from_slice(&bytes)?;
        validate_claim_record_identity(&object_key, &record)?;
        Ok(record)
    }

    async fn replace_expired_or_read_active_claim(
        &self,
        claim_object_key: ObjectKey,
        job: &ViewCompileDeployJobRecord,
        compile_request_hash: &str,
        worker_id: &str,
        now_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<ViewCompileDeployJobClaimOutcome, ViewCompileDeployJobRegistryError> {
        let path = Path::from(claim_object_key.as_str());
        let get_result = self.store.get(&path).await?;
        let update_version = UpdateVersion {
            e_tag: get_result.meta.e_tag.clone(),
            version: get_result.meta.version.clone(),
        };
        let bytes = get_result.bytes().await?;
        let existing: ViewCompileDeployJobClaimRecord = serde_json::from_slice(&bytes)?;
        validate_claim_record_identity(&claim_object_key, &existing)?;
        if existing.lease_expires_at_ms > now_ms {
            if existing.worker_id == worker_id {
                return Ok(ViewCompileDeployJobClaimOutcome::Duplicate(existing));
            }
            return Err(ViewCompileDeployJobRegistryError::ActiveClaim {
                object_key: claim_object_key,
                worker_id: existing.worker_id,
                lease_expires_at_ms: existing.lease_expires_at_ms,
            });
        }
        let replacement = claim_record_for_job(
            job,
            compile_request_hash,
            worker_id,
            now_ms,
            lease_duration_ms,
            existing.fencing_token.saturating_add(1),
        );
        let bytes = Bytes::from(serde_json::to_vec(&replacement)?);
        if update_version.e_tag.is_some() || update_version.version.is_some() {
            let update_result = self
                .store
                .put_opts(&path, bytes.into(), PutMode::Update(update_version).into())
                .await;
            match update_result {
                Ok(_) => {}
                Err(object_store::Error::NotImplemented) => {
                    self.store
                        .put(&path, Bytes::from(serde_json::to_vec(&replacement)?).into())
                        .await?;
                }
                Err(error) => {
                    return Err(job_update_error_to_registry(
                        error,
                        claim_object_key.clone(),
                    ));
                }
            }
        } else {
            self.store.put(&path, bytes.into()).await?;
        }
        Ok(ViewCompileDeployJobClaimOutcome::Claimed(replacement))
    }

    async fn mark_compile_validated_at_object_key(
        &self,
        object_key: ObjectKey,
        message: Option<String>,
    ) -> Result<CompleteViewCompileDeployJobOutcome, ViewCompileDeployJobRegistryError> {
        let path = Path::from(object_key.as_str());
        let get_result = self.store.get(&path).await?;
        let update_version = UpdateVersion {
            e_tag: get_result.meta.e_tag.clone(),
            version: get_result.meta.version.clone(),
        };
        let bytes = get_result.bytes().await?;
        let existing: ViewCompileDeployJobRecord = serde_json::from_slice(&bytes)?;
        validate_record_identity(&object_key, &existing)?;
        if existing.compile_status == MaterializedViewCompileStatus::Success
            && existing.deployment_status == MaterializedViewDeploymentStatus::NotDeployed
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
            tenant_id: existing.tenant_id.clone(),
            view_id: existing.view_id.clone(),
            job_generation: existing.job_generation,
            spec_hash: existing.spec_hash.clone(),
            compiler_backend: existing.compiler_backend.clone(),
            compiler_request: existing.compiler_request.clone(),
            compile_status: MaterializedViewCompileStatus::Success,
            deployment_status: MaterializedViewDeploymentStatus::NotDeployed,
            message,
        };
        let bytes = Bytes::from(serde_json::to_vec(&record)?);
        if update_version.e_tag.is_some() || update_version.version.is_some() {
            let update_result = self
                .store
                .put_opts(&path, bytes.into(), PutMode::Update(update_version).into())
                .await;
            match update_result {
                Ok(_) => {}
                Err(object_store::Error::NotImplemented) => {
                    self.store
                        .put(&path, Bytes::from(serde_json::to_vec(&record)?).into())
                        .await?;
                }
                Err(error) => return Err(job_update_error_to_registry(error, object_key.clone())),
            }
        } else {
            self.store.put(&path, bytes.into()).await?;
        }
        Ok(CompleteViewCompileDeployJobOutcome::Completed)
    }

    async fn mark_running_at_object_key(
        &self,
        object_key: ObjectKey,
        message: Option<String>,
    ) -> Result<CompleteViewCompileDeployJobOutcome, ViewCompileDeployJobRegistryError> {
        let path = Path::from(object_key.as_str());
        let get_result = self.store.get(&path).await?;
        let update_version = UpdateVersion {
            e_tag: get_result.meta.e_tag.clone(),
            version: get_result.meta.version.clone(),
        };
        let bytes = get_result.bytes().await?;
        let existing: ViewCompileDeployJobRecord = serde_json::from_slice(&bytes)?;
        validate_record_identity(&object_key, &existing)?;
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
            tenant_id: existing.tenant_id.clone(),
            view_id: existing.view_id.clone(),
            job_generation: existing.job_generation,
            spec_hash: existing.spec_hash.clone(),
            compiler_backend: existing.compiler_backend.clone(),
            compiler_request: existing.compiler_request.clone(),
            compile_status: MaterializedViewCompileStatus::Success,
            deployment_status: MaterializedViewDeploymentStatus::Running,
            message,
        };
        let bytes = Bytes::from(serde_json::to_vec(&record)?);
        if update_version.e_tag.is_some() || update_version.version.is_some() {
            let update_result = self
                .store
                .put_opts(&path, bytes.into(), PutMode::Update(update_version).into())
                .await;
            match update_result {
                Ok(_) => {}
                Err(object_store::Error::NotImplemented) => {
                    self.store
                        .put(&path, Bytes::from(serde_json::to_vec(&record)?).into())
                        .await?;
                }
                Err(error) => return Err(job_update_error_to_registry(error, object_key.clone())),
            }
        } else {
            self.store.put(&path, bytes.into()).await?;
        }
        Ok(CompleteViewCompileDeployJobOutcome::Completed)
    }
}

impl ViewCompileDeployJobRegistry {
    fn object_key_for_record(
        &self,
        record: &ViewCompileDeployJobRecord,
    ) -> Result<ObjectKey, ViewCompileDeployJobRegistryError> {
        if let Some(request) = &record.compiler_request {
            self.object_key_for_compile_request(&record.view_id, &request.compile_request_hash)
        } else {
            self.object_key(&record.view_id, &record.spec_hash)
        }
    }
}

fn compiler_request_for_spec(
    spec: &StandingViewSpec,
    spec_hash: &str,
) -> Result<ViewCompileDeployCompilerRequestV1, ViewCompileDeployJobRegistryError> {
    let compile_request = FelderaCompileRequestV1::infer_output_from_standing_view_spec(spec);
    Ok(ViewCompileDeployCompilerRequestV1 {
        request_kind: FELDERA_STANDING_VIEW_COMPILE_REQUEST_KIND.to_string(),
        view_id: spec.view_id.clone(),
        compile_request_hash: feldera_compile_request_hash(&compile_request)?,
        spec_hash: spec_hash.to_string(),
        sql: spec.sql.clone(),
        dialect: spec.dialect.clone(),
        source_kind: spec.source_kind.clone(),
        rust_extension: spec.rust_extension.clone(),
        input_relations: spec.input_relations.clone(),
        output_contract: OutputSchemaContract::Infer,
        output_relations: Vec::new(),
        shape: compile_request.shape,
    })
}

pub fn view_compile_deploy_job_id(view_id: &str, spec_hash: &str) -> String {
    format!("{view_id}:{spec_hash}")
}

pub fn view_compile_deploy_compile_request_job_id(
    view_id: &str,
    compile_request_hash: &str,
) -> String {
    format!("{view_id}:{compile_request_hash}")
}

fn validate_record_identity(
    object_key: &ObjectKey,
    record: &ViewCompileDeployJobRecord,
) -> Result<(), ViewCompileDeployJobRegistryError> {
    if !record_key_matches(object_key, record)?
        || !record_job_id_matches(record)
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

fn record_key_matches(
    object_key: &ObjectKey,
    record: &ViewCompileDeployJobRecord,
) -> Result<bool, ViewCompileDeployJobRegistryError> {
    let legacy_key = ObjectKey::view_compile_deploy_job(&record.view_id, &record.spec_hash)?;
    if let Some(request) = &record.compiler_request {
        let compile_request_key = ObjectKey::view_compile_deploy_job_for_compile_request(
            &record.view_id,
            &request.compile_request_hash,
        )?;
        Ok(*object_key == compile_request_key || *object_key == legacy_key)
    } else {
        Ok(*object_key == legacy_key)
    }
}

fn record_job_id_matches(record: &ViewCompileDeployJobRecord) -> bool {
    if let Some(request) = &record.compiler_request {
        record.job_id
            == view_compile_deploy_compile_request_job_id(
                &record.view_id,
                &request.compile_request_hash,
            )
            || record.job_id == view_compile_deploy_job_id(&record.view_id, &record.spec_hash)
    } else {
        record.job_id == view_compile_deploy_job_id(&record.view_id, &record.spec_hash)
    }
}

fn record_dedup_identity(record: &ViewCompileDeployJobRecord) -> String {
    if let Some(request) = &record.compiler_request {
        format!(
            "{}:{}",
            record.view_id,
            request.compile_request_hash.as_str()
        )
    } else {
        format!("{}:{}", record.view_id, record.spec_hash.as_str())
    }
}

fn validate_worker_id(worker_id: &str) -> Result<(), ViewCompileDeployJobRegistryError> {
    let valid = !worker_id.trim().is_empty()
        && worker_id.len() <= 128
        && worker_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':'));
    if valid {
        Ok(())
    } else {
        let object_key =
            ObjectKey::parse("v1/view-compile-deploy-job-claims/invalid/invalid.claim.json")
                .expect("static object key is parseable");
        Err(ViewCompileDeployJobRegistryError::RecordIdentityMismatch { object_key })
    }
}

fn claim_record_for_job(
    job: &ViewCompileDeployJobRecord,
    compile_request_hash: &str,
    worker_id: &str,
    now_ms: u64,
    lease_duration_ms: u64,
    fencing_token: u64,
) -> ViewCompileDeployJobClaimRecord {
    let lease_expires_at_ms = now_ms.saturating_add(lease_duration_ms);
    let lease_id = view_compile_deploy_job_lease_id(
        &job.tenant_id,
        &job.view_id,
        job.job_generation,
        compile_request_hash,
        worker_id,
        fencing_token,
        now_ms,
        lease_expires_at_ms,
    );
    ViewCompileDeployJobClaimRecord {
        schema_version: VIEW_COMPILE_DEPLOY_JOB_CLAIM_SCHEMA_VERSION,
        claim_id: view_compile_deploy_job_claim_id(
            &job.tenant_id,
            &job.view_id,
            job.job_generation,
            compile_request_hash,
            fencing_token,
        ),
        job_id: job.job_id.clone(),
        tenant_id: job.tenant_id.clone(),
        view_id: job.view_id.clone(),
        job_generation: job.job_generation,
        compile_request_hash: compile_request_hash.to_string(),
        worker_id: worker_id.to_string(),
        lease_id,
        fencing_token,
        claimed_at_ms: now_ms,
        lease_duration_ms,
        lease_expires_at_ms,
    }
}

pub fn view_compile_deploy_job_claim_id(
    tenant_id: &str,
    view_id: &str,
    job_generation: u64,
    compile_request_hash: &str,
    fencing_token: u64,
) -> String {
    format!("{tenant_id}:{view_id}:generation:{job_generation}:{compile_request_hash}:claim:{fencing_token}")
}

fn view_compile_deploy_job_lease_id(
    tenant_id: &str,
    view_id: &str,
    job_generation: u64,
    compile_request_hash: &str,
    worker_id: &str,
    fencing_token: u64,
    claimed_at_ms: u64,
    lease_expires_at_ms: u64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(tenant_id.as_bytes());
    hasher.update([0]);
    hasher.update(view_id.as_bytes());
    hasher.update([0]);
    hasher.update(job_generation.to_be_bytes());
    hasher.update([0]);
    hasher.update(compile_request_hash.as_bytes());
    hasher.update([0]);
    hasher.update(worker_id.as_bytes());
    hasher.update([0]);
    hasher.update(fencing_token.to_be_bytes());
    hasher.update(claimed_at_ms.to_be_bytes());
    hasher.update(lease_expires_at_ms.to_be_bytes());
    format!(
        "velorix-feldera-compile-lease-sha256-v1:{:x}",
        hasher.finalize()
    )
}

fn validate_claim_record_identity(
    object_key: &ObjectKey,
    record: &ViewCompileDeployJobClaimRecord,
) -> Result<(), ViewCompileDeployJobRegistryError> {
    let expected_key = ObjectKey::view_compile_deploy_job_claim_for_compile_request(
        &record.view_id,
        &record.compile_request_hash,
    )?;
    let valid = *object_key == expected_key
        && record.schema_version == VIEW_COMPILE_DEPLOY_JOB_CLAIM_SCHEMA_VERSION
        && record.claim_id
            == view_compile_deploy_job_claim_id(
                &record.tenant_id,
                &record.view_id,
                record.job_generation,
                &record.compile_request_hash,
                record.fencing_token,
            )
        && record.job_id
            == view_compile_deploy_compile_request_job_id(
                &record.view_id,
                &record.compile_request_hash,
            )
        && record.fencing_token > 0
        && !record.tenant_id.trim().is_empty()
        && record.job_generation > 0
        && record.lease_duration_ms > 0
        && record.lease_expires_at_ms > record.claimed_at_ms
        && record.lease_expires_at_ms - record.claimed_at_ms == record.lease_duration_ms
        && validate_worker_id(record.worker_id.as_str()).is_ok();
    if valid {
        Ok(())
    } else {
        Err(ViewCompileDeployJobRegistryError::RecordIdentityMismatch {
            object_key: object_key.clone(),
        })
    }
}

fn job_update_error_to_registry(
    error: object_store::Error,
    object_key: ObjectKey,
) -> ViewCompileDeployJobRegistryError {
    match error {
        object_store::Error::Precondition { .. } => {
            ViewCompileDeployJobRegistryError::RecordConflict { object_key }
        }
        error => ViewCompileDeployJobRegistryError::ObjectStore(error),
    }
}

fn validate_compiler_request_identity(
    object_key: &ObjectKey,
    record: &ViewCompileDeployJobRecord,
    request: &ViewCompileDeployCompilerRequestV1,
) -> Result<(), ViewCompileDeployJobRegistryError> {
    let actual_compile_request_hash =
        feldera_compile_request_hash(&request.feldera_compile_request())?;
    let output_snapshot_matches_contract = match &request.output_contract {
        OutputSchemaContract::Infer => request.output_relations.is_empty(),
        OutputSchemaContract::MustMatch { output_relations } => {
            request.output_relations == *output_relations
        }
    };
    let request_matches_record = request.request_kind == FELDERA_STANDING_VIEW_COMPILE_REQUEST_KIND
        && request.view_id == record.view_id
        && request.compile_request_hash == actual_compile_request_hash
        && request.spec_hash == record.spec_hash
        && !request.sql.trim().is_empty()
        && !request.input_relations.is_empty()
        && output_snapshot_matches_contract
        && request.shape.is_materialized;
    if request_matches_record {
        Ok(())
    } else {
        Err(ViewCompileDeployJobRegistryError::RecordIdentityMismatch {
            object_key: object_key.clone(),
        })
    }
}
