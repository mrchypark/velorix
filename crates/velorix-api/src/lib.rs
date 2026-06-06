use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Context};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
        StringArray, StringDictionaryBuilder, TimestampNanosecondArray,
    },
    datatypes::{
        ArrowDictionaryKeyType, DataType, Field, Int16Type, Int32Type, Int64Type, Int8Type, Schema,
    },
    record_batch::RecordBatch,
};
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Path as AxumPath, Query, State},
    http::{header, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use axum_server::tls_rustls::RustlsConfig;
use futures::TryStreamExt;
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path as ObjectPath,
    prefix::PrefixStore,
    ObjectStore, PutMode,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use velorix_core::{
    dbsp_view_plan::validate_supported_dbsp_view_sql,
    feldera_artifact::{
        catalog_input_relation_schema, feldera_artifact_bytes_hash, feldera_spec_hash,
        ColumnSchema, FelderaCompileArtifactMetadata, FelderaCompilerIdentity,
        GeneratedRustIdentity, RelationSchema, SqlDataType, SqlDialect, SqlSourceKind,
        StandingViewShape, StandingViewSpec, SUPPORTED_GENERATED_RUST_ABI_VERSION,
    },
    generated_view_descriptor::TrustedGeneratedViewDescriptor,
    query::QueryPolicy,
    relation::{
        datafusion_schema_from_catalog, ArrowPhysicalTypeV1, DataFusionRegistrationModeV1,
        DataFusionRegistrationV1, DictionaryKeyTypeV1, FelderaRelationBindingV1,
        IncrementalAdapterBindingV1, RelationColumnV1, RelationOperationV1, RelationSemanticRoleV1,
        SchemaFingerprintV1, VelorixLogicalTypeV1, VelorixRelationCatalogV1,
        VelorixRelationSchemaV1, CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID,
        RELATION_SCHEMA_VERSION_V1,
    },
    standing_program::{
        EpochIdempotencyKey, FelderaRuntimePackageIdentity, MaterializedViewPage, NativeCodePolicy,
        RelationInputBatch, RuntimeCheckpoint, ScopedViewId, SnapshotPageRequest,
        StandingProgramIdentity, StandingProgramRuntime, StandingProgramRuntimeError,
    },
};
use velorix_k8s::{
    crd::ObjectStoreAuthorityRef,
    ingest_writer::DeployedIngestWriterRuntime,
    startup::{
        validate_operator_authority, OperatorAuthorityStartupComponents, ValidatedOperatorAuthority,
    },
};
use velorix_meta::{
    validate_bearer_token, AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest,
    GrpcMetaStore, IngestRangeReservation, MetaStore, MetaStoreError,
    PublishStandingRuntimeCheckpointOutcome, PublishStandingRuntimeCheckpointRequest,
    ReserveIngestRangeOutcome, StandingRuntimeCheckpointPointer, StandingRuntimeFencingCapability,
    StandingRuntimeOwnerClaim, StandingRuntimeOwnerToken, StoreRelationCatalogOutcome,
    STANDING_RUNTIME_BACKEND_TIME_SOURCE_RAFT_REPLICATED,
    STANDING_RUNTIME_FENCING_CAPABILITY_SCHEMA_VERSION,
    STANDING_RUNTIME_LEASE_AUTHORITY_KIND_HIQLITE_RAFT_SERIALIZED,
    STANDING_RUNTIME_LEASE_AUTHORITY_KIND_RAFT_REPLICATED_TIME,
    STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_BACKEND_WALL_CLOCK_TTL,
    STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_OPERATION_DRIVEN_LOGICAL,
    STANDING_RUNTIME_OWNER_SCOPE_KIND_TENANT_PROGRAM_VIEW,
};
use velorix_runtime::feldera_registry::{
    GeneratedRustArtifactPackage, RuntimeFelderaArtifactRegistry,
    RuntimeFelderaArtifactSelectionStatus,
};
use velorix_runtime::query::{
    query_production_recovered_materialized_view_table_with_bindings_and_policy_and_limiter,
    query_production_recovered_materialized_view_with_catalog_and_policy_and_limiter,
    query_production_recovered_materialized_view_with_policy_and_limiter,
    query_record_batches_table_with_bindings_and_policy_and_limiter,
    validate_record_batch_table_query_with_bindings_and_policy, ProductionQueryRuntime,
    QueryBindValue, QueryExecutionLimiter,
};
use velorix_runtime::query_policy_catalog::{
    QueryPolicyCatalogError, QueryPolicyCatalogRecord, QueryPolicyCatalogStore,
};
use velorix_storage::{
    capability::{AuthoritativeNamespace, ObjectStoreCapabilityProfile},
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::{AppendValidatedEnvelopeOutcome, IngestBatchDescriptor, IngestLog, ReplayCheckpoint},
    materialized_view_registry::{
        ActivateMaterializedViewOutcome, ActiveMaterializedView, InvalidExecutionModeReason,
        MaterializedViewApiMetadata, MaterializedViewArtifactBinding,
        MaterializedViewCompileStatus, MaterializedViewDeploymentStatus,
        MaterializedViewExecutionMode, MaterializedViewLifecycleStatus, MaterializedViewRegistry,
        MaterializedViewRegistryError, MaterializedViewRequestFieldSpec,
        MaterializedViewResponseColumnSpec, MaterializedViewResponseSchema,
        RegisterMaterializedViewOutcome, UpdateMaterializedViewLifecycleOutcome,
    },
    object_key::ObjectKey,
    relation_catalog_registry::{CreateRelationCatalogOutcome, RelationCatalogRegistry},
    view_compile_deploy_job_registry::{
        view_compile_deploy_job_id, ViewCompileDeployJobRecord, ViewCompileDeployJobRegistry,
        ViewCompileDeployJobRegistryError,
    },
};

#[derive(Clone)]
pub struct ApiState {
    store: Arc<dyn ObjectStore>,
    capabilities: Arc<velorix_storage::capability::AuthoritativeObjectStoreCapabilitiesV1>,
    ingest_writer: Arc<DeployedIngestWriterRuntime>,
    meta_store: Option<Arc<dyn MetaStore>>,
    meta_store_endpoint: Option<String>,
    owner_id: String,
    standing_runtime_owner_ttl_ms: u64,
    standing_runtime_fencing_required: bool,
    standing_runtime_fencing_mode: StandingRuntimeFencingMode,
    api_bearer_token: Option<Arc<str>>,
    admin_bearer_token: Option<Arc<str>>,
    max_request_body_bytes: usize,
    max_ingest_rows: usize,
    allow_legacy_recovered_sql_views: bool,
    enable_generic_query: bool,
    state_path: String,
    generated_artifact_packages: Arc<Vec<GeneratedRustArtifactPackage>>,
    trusted_generated_view_descriptors: Arc<Vec<TrustedGeneratedViewDescriptor>>,
    standing_runtimes: Arc<StandingRuntimeRegistry>,
    standing_runtime_factories: Arc<StandingRuntimeFactoryRegistry>,
    query_runtimes: Arc<Mutex<HashMap<String, ProductionQueryRuntime>>>,
}

type SharedStandingRuntime = Arc<Mutex<Box<dyn StandingProgramRuntime + Send>>>;

#[derive(Default)]
struct StandingRuntimeRegistry {
    runtimes: Mutex<HashMap<StandingRuntimeKey, SharedStandingRuntime>>,
    operation_locks: Mutex<HashMap<StandingRuntimeKey, Arc<AsyncMutex<()>>>>,
    local_state: Mutex<HashMap<StandingRuntimeKey, StandingRuntimeLocalState>>,
}

#[derive(Default)]
struct StandingRuntimeFactoryRegistry {
    factories: Mutex<HashMap<String, Arc<dyn StandingProgramRuntimeFactory>>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct StandingRuntimeKey {
    tenant_id: String,
    program_id: String,
    view_id: String,
}

#[derive(Clone, Debug, Default)]
struct StandingRuntimeLocalState {
    owner: Option<StandingRuntimeOwnerClaim>,
    committed_checkpoint: Option<StandingRuntimeCheckpointPointer>,
}

pub trait StandingProgramRuntimeFactory: Send + Sync + 'static {
    fn create(
        &self,
        identity: &StandingProgramIdentity,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String>;

    fn restore(
        &self,
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StandingRuntimeCheckpointRecord {
    schema_version: u16,
    record_kind: String,
    view_id: String,
    #[serde(default)]
    checkpoint_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous_checkpoint: Option<StandingRuntimeCheckpointPointer>,
    checkpoint: RuntimeCheckpoint,
    replay_checkpoints: Vec<ReplayCheckpoint>,
}

#[derive(Clone, Debug)]
struct GeneratedScoresByUserRuntimeFactory;

impl StandingProgramRuntimeFactory for GeneratedScoresByUserRuntimeFactory {
    fn create(
        &self,
        identity: &StandingProgramIdentity,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        velorix_generated_scores_by_user::create_standing_runtime(identity)
    }

    fn restore(
        &self,
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        velorix_generated_scores_by_user::restore_standing_runtime(checkpoint)
    }
}

impl ApiState {
    pub async fn from_validated_authority(
        validated: ValidatedOperatorAuthority,
        state_path: impl Into<String>,
        operator_id: impl Into<String>,
    ) -> Result<Self, ApiError> {
        Self::from_validated_authority_with_ingest_admission_startup(
            validated,
            state_path,
            operator_id,
            true,
        )
        .await
    }

    pub async fn from_validated_authority_with_ingest_admission_startup(
        validated: ValidatedOperatorAuthority,
        state_path: impl Into<String>,
        operator_id: impl Into<String>,
        reconstruct_ingest_admission: bool,
    ) -> Result<Self, ApiError> {
        let owner_id = process_incarnation_owner_id(operator_id.into())?;
        let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);
        let store = components.store();
        let capabilities = Arc::new(components.capabilities().clone());
        let ingest_writer = if reconstruct_ingest_admission {
            DeployedIngestWriterRuntime::from_startup_components(&components)
                .await
                .map_err(ApiError::internal)?
        } else {
            DeployedIngestWriterRuntime::from_startup_components_without_reconstruction(&components)
                .map_err(ApiError::internal)?
        };

        let state = Self {
            store,
            capabilities,
            ingest_writer: Arc::new(ingest_writer),
            meta_store: None,
            meta_store_endpoint: None,
            owner_id,
            standing_runtime_owner_ttl_ms: 30_000,
            standing_runtime_fencing_required: false,
            standing_runtime_fencing_mode: StandingRuntimeFencingMode::SingleWriter,
            api_bearer_token: None,
            admin_bearer_token: None,
            max_request_body_bytes: 1024 * 1024,
            max_ingest_rows: 10_000,
            allow_legacy_recovered_sql_views: false,
            enable_generic_query: false,
            state_path: state_path.into(),
            generated_artifact_packages: Arc::new(default_generated_artifact_packages()),
            trusted_generated_view_descriptors: Arc::new(
                default_trusted_generated_view_descriptors(),
            ),
            standing_runtimes: Arc::new(StandingRuntimeRegistry::default()),
            standing_runtime_factories: Arc::new(StandingRuntimeFactoryRegistry::default()),
            query_runtimes: Arc::new(Mutex::new(HashMap::new())),
        };
        state.register_builtin_standing_runtime_factories_for_generated_packages();

        Ok(state)
    }

    pub fn with_meta_store(mut self, meta_store: Arc<dyn MetaStore>) -> Self {
        self.meta_store = Some(meta_store);
        self
    }

    pub fn with_meta_store_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.meta_store_endpoint = Some(endpoint.into());
        self
    }

    pub fn with_standing_runtime_owner_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.standing_runtime_owner_ttl_ms = ttl_ms;
        self
    }

    pub fn with_standing_runtime_fencing_required(mut self, required: bool) -> Self {
        self.standing_runtime_fencing_required = required;
        self.standing_runtime_fencing_mode = if required {
            StandingRuntimeFencingMode::Required
        } else {
            StandingRuntimeFencingMode::SingleWriter
        };
        self
    }

    fn with_standing_runtime_fencing_mode(mut self, mode: StandingRuntimeFencingMode) -> Self {
        self.standing_runtime_fencing_required = mode.requires_metadata();
        self.standing_runtime_fencing_mode = mode;
        self
    }

    pub fn with_api_bearer_token(mut self, token: impl Into<String>) -> Result<Self, ApiError> {
        let token = token.into();
        validate_bearer_token(&token).map_err(ApiError::bad_request)?;
        self.api_bearer_token = Some(Arc::from(token));
        Ok(self)
    }

    pub fn with_admin_bearer_token(mut self, token: impl Into<String>) -> Result<Self, ApiError> {
        let token = token.into();
        validate_bearer_token(&token).map_err(ApiError::bad_request)?;
        self.admin_bearer_token = Some(Arc::from(token));
        Ok(self)
    }

    pub fn with_request_limits(mut self, max_body_bytes: usize, max_ingest_rows: usize) -> Self {
        self.max_request_body_bytes = max_body_bytes;
        self.max_ingest_rows = max_ingest_rows;
        self
    }

    pub fn with_legacy_recovered_sql_views_allowed(mut self, allowed: bool) -> Self {
        self.allow_legacy_recovered_sql_views = allowed;
        self
    }

    pub fn with_generic_query_enabled(mut self, enabled: bool) -> Self {
        self.enable_generic_query = enabled;
        self
    }

    pub fn with_generated_artifact_packages(
        mut self,
        crate_names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.generated_artifact_packages = Arc::new(
            crate_names
                .into_iter()
                .map(|crate_name| GeneratedRustArtifactPackage {
                    abi_version: SUPPORTED_GENERATED_RUST_ABI_VERSION.to_string(),
                    crate_name: crate_name.into(),
                })
                .collect(),
        );
        self.register_builtin_standing_runtime_factories_for_generated_packages();
        self
    }

    fn register_builtin_standing_runtime_factories_for_generated_packages(&self) {
        for package in self.generated_artifact_packages.iter() {
            register_builtin_standing_runtime_factory(self, &package.crate_name);
        }
    }

    pub fn register_standing_program_runtime(
        &self,
        view_id: impl Into<String>,
        runtime: impl StandingProgramRuntime + Send + 'static,
    ) -> Result<(), ApiError> {
        let view_id = view_id.into();
        let key = standing_runtime_key(runtime.program_identity(), &view_id);
        let mut runtimes = self
            .standing_runtimes
            .runtimes
            .lock()
            .map_err(|_| ApiError::internal("standing runtime registry lock poisoned"))?;
        runtimes.insert(key, Arc::new(Mutex::new(Box::new(runtime))));
        Ok(())
    }

    pub fn with_standing_program_runtime_factory(
        self,
        generated_rust_crate_name: impl Into<String>,
        factory: impl StandingProgramRuntimeFactory,
    ) -> Self {
        self.register_standing_program_runtime_factory(generated_rust_crate_name, factory);
        self
    }

    pub fn register_standing_program_runtime_factory(
        &self,
        generated_rust_crate_name: impl Into<String>,
        factory: impl StandingProgramRuntimeFactory,
    ) {
        let mut factories = self
            .standing_runtime_factories
            .factories
            .lock()
            .expect("standing runtime factory registry lock poisoned");
        factories.insert(generated_rust_crate_name.into(), Arc::new(factory));
    }

    fn standing_runtime(
        &self,
        identity: &StandingProgramIdentity,
        view_id: &str,
    ) -> Result<Option<SharedStandingRuntime>, ApiError> {
        let runtimes = self
            .standing_runtimes
            .runtimes
            .lock()
            .map_err(|_| ApiError::internal("standing runtime registry lock poisoned"))?;
        Ok(runtimes
            .get(&standing_runtime_key(identity, view_id))
            .cloned())
    }

    fn standing_runtime_operation_lock(
        &self,
        identity: &StandingProgramIdentity,
        view_id: &str,
    ) -> Result<Arc<AsyncMutex<()>>, ApiError> {
        let mut locks =
            self.standing_runtimes.operation_locks.lock().map_err(|_| {
                ApiError::internal("standing runtime operation lock registry poisoned")
            })?;
        Ok(locks
            .entry(standing_runtime_key(identity, view_id))
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone())
    }

    async fn acquire_standing_runtime_owner(
        &self,
        identity: &StandingProgramIdentity,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeOwnerToken>, ApiError> {
        let Some(meta_store) = &self.meta_store else {
            return Ok(None);
        };
        let outcome = meta_store
            .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
                tenant_id: identity.tenant_id.clone(),
                program_id: identity.program_id.clone(),
                view_id: view_id.to_string(),
                owner_id: self.owner_id.clone(),
                ttl_ms: self.standing_runtime_owner_ttl_ms,
            })
            .await
            .map_err(meta_error_to_api)?;
        match outcome {
            AcquireStandingRuntimeOwnerOutcome::Acquired(claim)
            | AcquireStandingRuntimeOwnerOutcome::Renewed(claim) => {
                self.set_standing_runtime_owner(identity, view_id, claim.clone())?;
                Ok(Some(standing_runtime_owner_token_from_claim(&claim)))
            }
            AcquireStandingRuntimeOwnerOutcome::Conflict(claim) => {
                self.remove_standing_runtime_with_state(identity, view_id)?;
                Err(ApiError::conflict(format!(
                    "standing runtime owner conflict for `{}/{}/{}`: current owner `{}` epoch {}",
                    identity.tenant_id,
                    identity.program_id,
                    view_id,
                    claim.owner_id,
                    claim.owner_epoch
                )))
            }
        }
    }

    async fn validate_standing_runtime_committed_for_query(
        &self,
        identity: &StandingProgramIdentity,
        view_id: &str,
        runtime_epoch: u64,
    ) -> Result<(), ApiError> {
        let Some(meta_store) = &self.meta_store else {
            return Ok(());
        };
        let key = standing_runtime_key(identity, view_id);
        let local = self.standing_runtime_local_state(&key)?;
        let latest = meta_store
            .read_standing_runtime_checkpoint(&identity.tenant_id, &identity.program_id, view_id)
            .await
            .map_err(meta_error_to_api)?;
        match (latest, local.committed_checkpoint) {
            (Some(latest), Some(local)) if latest == local => Ok(()),
            (None, None) if runtime_epoch == 0 => Ok(()),
            _ => {
                self.remove_standing_runtime_with_state(identity, view_id)?;
                Err(ApiError::service_unavailable(format!(
                    "standing runtime local state is not the committed checkpoint for artifact-backed view `{view_id}`"
                )))
            }
        }
    }

    fn set_standing_runtime_owner(
        &self,
        identity: &StandingProgramIdentity,
        view_id: &str,
        owner: StandingRuntimeOwnerClaim,
    ) -> Result<(), ApiError> {
        let key = standing_runtime_key(identity, view_id);
        let mut states = self
            .standing_runtimes
            .local_state
            .lock()
            .map_err(|_| ApiError::internal("standing runtime local state lock poisoned"))?;
        states.entry(key).or_default().owner = Some(owner);
        Ok(())
    }

    fn set_standing_runtime_committed_checkpoint(
        &self,
        identity: &StandingProgramIdentity,
        view_id: &str,
        pointer: Option<StandingRuntimeCheckpointPointer>,
    ) -> Result<(), ApiError> {
        let key = standing_runtime_key(identity, view_id);
        let mut states = self
            .standing_runtimes
            .local_state
            .lock()
            .map_err(|_| ApiError::internal("standing runtime local state lock poisoned"))?;
        states.entry(key).or_default().committed_checkpoint = pointer;
        Ok(())
    }

    fn standing_runtime_local_state(
        &self,
        key: &StandingRuntimeKey,
    ) -> Result<StandingRuntimeLocalState, ApiError> {
        let states = self
            .standing_runtimes
            .local_state
            .lock()
            .map_err(|_| ApiError::internal("standing runtime local state lock poisoned"))?;
        Ok(states.get(key).cloned().unwrap_or_default())
    }

    fn remove_standing_runtime_with_state(
        &self,
        identity: &StandingProgramIdentity,
        view_id: &str,
    ) -> Result<(), ApiError> {
        remove_standing_runtime(self, identity, view_id)
    }

    fn clear_standing_runtimes(&self) -> Result<(), ApiError> {
        self.standing_runtimes
            .runtimes
            .lock()
            .map_err(|_| ApiError::internal("standing runtime registry lock poisoned"))?
            .clear();
        self.standing_runtimes
            .local_state
            .lock()
            .map_err(|_| ApiError::internal("standing runtime local state lock poisoned"))?
            .clear();
        Ok(())
    }

    async fn validate_standing_runtime_fencing_or_evict(&self) -> Result<(), ApiError> {
        if !self.standing_runtime_fencing_mode.requires_metadata() {
            return Ok(());
        }
        let Some(meta_store) = self.meta_store.as_ref() else {
            self.clear_standing_runtimes()?;
            return Err(ApiError::service_unavailable(
                "standing runtime fencing is required but metadata store is not configured",
            ));
        };
        let capabilities = match meta_store.read_meta_store_capabilities().await {
            Ok(capabilities) => capabilities,
            Err(error) => {
                self.clear_standing_runtimes()?;
                return Err(meta_error_to_api(error));
            }
        };
        if let Err(error) = validate_standing_runtime_fencing_for_mode(
            &capabilities.standing_runtime_fencing,
            self.standing_runtime_fencing_mode,
        ) {
            self.clear_standing_runtimes()?;
            return Err(ApiError::service_unavailable(error));
        }
        Ok(())
    }

    fn standing_runtime_factory(
        &self,
        generated_rust_crate_name: &str,
    ) -> Result<Option<Arc<dyn StandingProgramRuntimeFactory>>, ApiError> {
        let factories = self
            .standing_runtime_factories
            .factories
            .lock()
            .map_err(|_| ApiError::internal("standing runtime factory registry lock poisoned"))?;
        Ok(factories.get(generated_rust_crate_name).cloned())
    }

    pub async fn restore_standing_program_runtimes_from_active_views(
        &self,
    ) -> Result<usize, ApiError> {
        self.validate_standing_runtime_fencing_or_evict().await?;
        let active_views = self
            .view_registry()?
            .list_active()
            .await
            .map_err(materialized_view_registry_error_to_api)?;
        let mut restored = 0;

        for active in active_views {
            let Some(artifact) = active.artifact.as_ref() else {
                continue;
            };
            if let Some(replay_checkpoints) = ensure_standing_runtime_for_artifact(
                self,
                &active.spec.view_id,
                artifact,
                &active.spec.input_relations,
                &active.spec.output_relations,
            )
            .await?
            {
                replay_committed_ingest_into_standing_runtime(self, &active, &replay_checkpoints)
                    .await?;
                restored += 1;
            }
        }

        Ok(restored)
    }

    pub async fn run_view_compile_deploy_worker_once(
        &self,
    ) -> Result<ViewCompileDeployWorkerReport, ApiError> {
        self.reconcile_missing_view_compile_deploy_jobs().await?;
        let jobs = self
            .view_compile_deploy_job_registry()?
            .list_pending()
            .await
            .map_err(view_compile_deploy_job_registry_error_to_api)?;
        let mut report = ViewCompileDeployWorkerReport {
            pending_jobs: jobs.len(),
            ..ViewCompileDeployWorkerReport::default()
        };

        for job in jobs {
            let job_id = job.job_id.clone();
            let view_id = job.view_id.clone();
            match self.run_view_compile_deploy_job(job).await {
                Ok(status) => match status {
                    ViewCompileDeployJobStatus::Activated => {
                        report.activated += 1;
                        report.outcomes.push(ViewCompileDeployWorkerJobOutcome {
                            job_id,
                            view_id,
                            status: "activated".to_string(),
                            reason: None,
                        });
                    }
                    ViewCompileDeployJobStatus::Duplicate => {
                        report.skipped += 1;
                        report.outcomes.push(ViewCompileDeployWorkerJobOutcome {
                            job_id,
                            view_id,
                            status: "duplicate".to_string(),
                            reason: None,
                        });
                    }
                    ViewCompileDeployJobStatus::Skipped(reason) => {
                        report.skipped += 1;
                        report.outcomes.push(ViewCompileDeployWorkerJobOutcome {
                            job_id,
                            view_id,
                            status: "skipped".to_string(),
                            reason: Some(reason),
                        });
                    }
                },
                Err(error) => {
                    report.failed += 1;
                    report.outcomes.push(ViewCompileDeployWorkerJobOutcome {
                        job_id,
                        view_id,
                        status: "failed".to_string(),
                        reason: Some(error.to_string()),
                    });
                }
            }
        }

        Ok(report)
    }

    async fn reconcile_missing_view_compile_deploy_jobs(&self) -> Result<(), ApiError> {
        let registry = self.view_compile_deploy_job_registry()?;
        let active_views = self
            .view_registry()?
            .list_active()
            .await
            .map_err(materialized_view_registry_error_to_api)?;
        for active in active_views {
            if active.execution_mode != MaterializedViewExecutionMode::FelderaCompilePending {
                continue;
            }
            match registry.read(&active.spec.view_id, &active.spec_hash).await {
                Ok(_) => continue,
                Err(ViewCompileDeployJobRegistryError::ObjectStore(
                    object_store::Error::NotFound { .. },
                )) => {}
                Err(error) => return Err(view_compile_deploy_job_registry_error_to_api(error)),
            }
            registry
                .register_pending_for_spec(&active.spec, &active.spec_hash, &active.lifecycle)
                .await
                .map_err(view_compile_deploy_job_registry_error_to_api)?;
        }

        Ok(())
    }

    async fn run_view_compile_deploy_job(
        &self,
        job: ViewCompileDeployJobRecord,
    ) -> Result<ViewCompileDeployJobStatus, ApiError> {
        let active = self
            .view_registry()?
            .read_active(&job.view_id)
            .await
            .map_err(materialized_view_registry_error_to_api)?;
        if active.spec_hash != job.spec_hash {
            return Ok(ViewCompileDeployJobStatus::Skipped(
                "active view spec hash no longer matches compile/deploy job".to_string(),
            ));
        }
        if active.execution_mode == MaterializedViewExecutionMode::FelderaCompilePending
            && !compile_job_request_matches_active_spec(&job, &active.spec)
        {
            return Ok(ViewCompileDeployJobStatus::Skipped(
                "compile/deploy job compiler_request does not match active view spec".to_string(),
            ));
        }
        let input = active
            .spec
            .input_relations
            .first()
            .ok_or_else(|| ApiError::bad_request("pending view has no input relation"))?;
        let catalog =
            read_relation_catalog(self, &input.relation_id, &input.relation_version).await?;
        let Some(descriptor) = trusted_generated_descriptor_for_spec(self, &catalog, &active.spec)?
        else {
            return Ok(ViewCompileDeployJobStatus::Skipped(
                "no trusted generated descriptor matches this pending view".to_string(),
            ));
        };
        if !state_has_generated_descriptor_package(self, &descriptor) {
            return Ok(ViewCompileDeployJobStatus::Skipped(format!(
                "generated Rust package `{}` is not registered with this Velorix binary",
                descriptor.generated_rust.crate_name
            )));
        }

        self.validate_standing_runtime_fencing_or_evict().await?;
        let artifact_metadata = generated_view_artifact_for_descriptor(&descriptor, &catalog)?;
        let (artifact, should_activate_deploying) = match active.execution_mode {
            MaterializedViewExecutionMode::FelderaCompilePending => (
                register_view_artifact(self, &catalog, &active.spec, &artifact_metadata).await?,
                true,
            ),
            MaterializedViewExecutionMode::StandingRuntime
                if active.lifecycle.deployment_status
                    == MaterializedViewDeploymentStatus::Deploying =>
            {
                let artifact = active
                    .artifact
                    .clone()
                    .ok_or_else(|| ApiError::conflict("deploying view is missing artifact"))?;
                (artifact, false)
            }
            MaterializedViewExecutionMode::StandingRuntime => {
                self.view_compile_deploy_job_registry()?
                    .mark_running(
                        &active.spec.view_id,
                        &active.spec_hash,
                        Some(
                            "standing runtime was already active; repaired compile/deploy job"
                                .to_string(),
                        ),
                    )
                    .await
                    .map_err(view_compile_deploy_job_registry_error_to_api)?;
                return Ok(ViewCompileDeployJobStatus::Duplicate);
            }
            MaterializedViewExecutionMode::LegacyRecoveredSql => {
                return Ok(ViewCompileDeployJobStatus::Skipped(
                    "active view is legacy_recovered_sql".to_string(),
                ));
            }
        };
        let identity = artifact
            .standing_program_identity
            .as_ref()
            .ok_or_else(|| ApiError::conflict("generated artifact is missing runtime identity"))?
            .clone();
        let replay_checkpoints = if let Some((runtime, replay_checkpoints)) =
            restore_or_build_standing_runtime_for_artifact(
                self,
                &active.spec.view_id,
                &artifact,
                &artifact_metadata.input_schemas,
                &artifact_metadata.output_schemas,
            )
            .await?
        {
            insert_standing_runtime(self, &active.spec.view_id, runtime)?;
            replay_checkpoints
        } else {
            read_latest_standing_runtime_checkpoint(self, &identity, &active.spec.view_id)
                .await?
                .map(|record| record.replay_checkpoints)
                .unwrap_or_default()
        };
        let deploying_lifecycle = MaterializedViewLifecycleStatus::standing_runtime_deploying(
            Some("catching up committed ingest before query activation".to_string()),
        );
        let activation = if should_activate_deploying {
            self.view_registry()?
                .activate_pending_with_artifact(
                    &active.spec.view_id,
                    &active.spec_hash,
                    artifact.clone(),
                    deploying_lifecycle.clone(),
                )
                .await
                .map_err(materialized_view_registry_error_to_api)?
        } else {
            ActivateMaterializedViewOutcome::Duplicate
        };
        let replay_active = ActiveMaterializedView {
            spec_hash: active.spec_hash.clone(),
            spec: active.spec.clone(),
            execution_mode: MaterializedViewExecutionMode::StandingRuntime,
            api: active.api.clone(),
            artifact: Some(artifact),
            lifecycle: deploying_lifecycle,
        };
        replay_committed_ingest_into_standing_runtime(self, &replay_active, &replay_checkpoints)
            .await?;
        let lifecycle = MaterializedViewLifecycleStatus::standing_runtime();
        let lifecycle_update = self
            .view_registry()?
            .update_standing_runtime_lifecycle(&active.spec.view_id, &active.spec_hash, lifecycle)
            .await
            .map_err(materialized_view_registry_error_to_api)?;
        self.view_compile_deploy_job_registry()?
            .mark_running(
                &active.spec.view_id,
                &active.spec_hash,
                Some("standing runtime activated from linked generated package".to_string()),
            )
            .await
            .map_err(view_compile_deploy_job_registry_error_to_api)?;

        Ok(match activation {
            ActivateMaterializedViewOutcome::Activated => ViewCompileDeployJobStatus::Activated,
            ActivateMaterializedViewOutcome::Duplicate => match lifecycle_update {
                UpdateMaterializedViewLifecycleOutcome::Updated => {
                    ViewCompileDeployJobStatus::Activated
                }
                UpdateMaterializedViewLifecycleOutcome::Duplicate => {
                    ViewCompileDeployJobStatus::Duplicate
                }
            },
        })
    }

    fn relation_registry(&self) -> Result<RelationCatalogRegistry, ApiError> {
        let profile = self
            .capabilities
            .validate_namespace(AuthoritativeNamespace::RelationCatalog)
            .map_err(ApiError::internal)?;
        RelationCatalogRegistry::new_checked(Arc::clone(&self.store), profile)
            .map_err(ApiError::internal)
    }

    fn view_registry(&self) -> Result<MaterializedViewRegistry, ApiError> {
        let profile = self
            .capabilities
            .validate_namespace(AuthoritativeNamespace::ArtifactCatalog)
            .map_err(ApiError::internal)?;
        MaterializedViewRegistry::new_checked(Arc::clone(&self.store), profile)
            .map_err(ApiError::internal)
    }

    fn view_compile_deploy_job_registry(&self) -> Result<ViewCompileDeployJobRegistry, ApiError> {
        self.capabilities
            .validate_namespace(AuthoritativeNamespace::ArtifactCatalog)
            .map_err(ApiError::internal)?;
        Ok(ViewCompileDeployJobRegistry::new(Arc::clone(&self.store)))
    }

    fn runtime_feldera_artifact_registry(
        &self,
    ) -> Result<RuntimeFelderaArtifactRegistry, ApiError> {
        RuntimeFelderaArtifactRegistry::new_with_startup_capabilities_and_generated_packages(
            Arc::clone(&self.store),
            &self.capabilities,
            self.generated_artifact_packages.iter().cloned(),
        )
        .map_err(ApiError::internal)
    }

    fn query_policy_catalog(&self) -> Result<QueryPolicyCatalogStore, ApiError> {
        QueryPolicyCatalogStore::new_checked(Arc::clone(&self.store), &self.capabilities)
            .map_err(query_policy_catalog_error_to_api)
    }

    fn query_limiter_for_policy(
        &self,
        query_policy_id: &str,
        policy: QueryPolicy,
    ) -> Result<Option<QueryExecutionLimiter>, ApiError> {
        let mut runtimes = self
            .query_runtimes
            .lock()
            .map_err(|_| ApiError::internal("query runtime registry lock poisoned"))?;
        let runtime = runtimes
            .entry(query_policy_id.to_string())
            .or_insert_with(|| ProductionQueryRuntime::from_policy(policy));
        runtime
            .compatible_limiter(policy)
            .map_err(ApiError::bad_request)
    }
}

pub fn app(state: ApiState) -> Router {
    let protected_routes = Router::new()
        .route("/v1/relations", post(create_relation))
        .route(
            "/v1/relations/orders-default",
            post(create_default_orders_relation),
        )
        .route(
            "/v1/relations/scores-default",
            post(create_default_scores_relation),
        )
        .route("/v1/ingest", post(ingest_rows))
        .route("/v1/query", post(query_rows))
        .route("/v1/query-policies", post(create_query_policy))
        .route(
            "/v1/query-policies/{query_policy_id}",
            get(get_query_policy),
        )
        .route("/v1/views", get(list_views).post(create_view))
        .route(
            "/v1/views/scores-positive-default",
            post(create_default_positive_scores_view),
        )
        .route("/v1/views/{view_id}", get(get_view))
        .route(
            "/v1/views/{view_id}/query",
            get(query_view_rows_get).post(query_view_rows_post),
        )
        .route("/v1/api/{*api_path}", get(query_view_api_get))
        .route("/v1/openapi.json", get(openapi_json))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_auth,
        ));
    let admin_routes = Router::new()
        .route(
            "/v1/view-compile-deploy/jobs",
            get(list_view_compile_deploy_jobs),
        )
        .route(
            "/v1/view-compile-deploy/run-once",
            post(run_view_compile_deploy_once),
        )
        .route(
            "/v1/standing-runtime/owners",
            get(get_standing_runtime_owners).post(acquire_standing_runtime_owners),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_auth,
        ));

    let max_request_body_bytes = state.max_request_body_bytes;
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .merge(protected_routes)
        .merge(admin_routes)
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .with_state(state)
}

async fn require_api_auth(
    State(state): State<ApiState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let Some(expected) = state.api_bearer_token.as_deref() else {
        return Ok(next.run(request).await);
    };
    require_bearer_token(&request, expected, "missing or invalid bearer token")?;
    Ok(next.run(request).await)
}

async fn require_admin_auth(
    State(state): State<ApiState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, ApiError> {
    if let Some(expected) = state.admin_bearer_token.as_deref() {
        require_bearer_token(&request, expected, "missing or invalid admin bearer token")?;
        return Ok(next.run(request).await);
    }
    if state.api_bearer_token.is_none() {
        return Ok(next.run(request).await);
    }
    Err(ApiError::service_unavailable(
        "admin_auth_required: configure VELORIX_ADMIN_BEARER_TOKEN for control-plane routes",
    ))
}

fn require_bearer_token(
    request: &Request<Body>,
    expected: &str,
    error: &'static str,
) -> Result<(), ApiError> {
    let actual = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if actual == Some(expected) {
        Ok(())
    } else {
        Err(ApiError::unauthorized(error))
    }
}

fn register_builtin_standing_runtime_factory(state: &ApiState, generated_rust_crate_name: &str) {
    if generated_rust_crate_name == velorix_generated_scores_by_user::CRATE_NAME {
        state.register_standing_program_runtime_factory(
            generated_rust_crate_name,
            GeneratedScoresByUserRuntimeFactory,
        );
    }
}

fn default_generated_artifact_packages() -> Vec<GeneratedRustArtifactPackage> {
    vec![GeneratedRustArtifactPackage {
        abi_version: SUPPORTED_GENERATED_RUST_ABI_VERSION.to_string(),
        crate_name: velorix_generated_scores_by_user::CRATE_NAME.to_string(),
    }]
}

fn default_trusted_generated_view_descriptors() -> Vec<TrustedGeneratedViewDescriptor> {
    vec![
        trusted_positive_scores_generated_descriptor(
            DEFAULT_POSITIVE_SCORES_VIEW_ID,
            "builtin-positive-scores-by-user",
            b"velorix-builtin-scores-by-user-generated-package".to_vec(),
        ),
        trusted_positive_scores_generated_descriptor(
            PENDING_SCORES_COMPILE_DEPLOY_VIEW_ID,
            "builtin-pending-positive-scores-by-user",
            b"velorix-builtin-scores-by-user-generated-package:pending_scores_by_user".to_vec(),
        ),
        trusted_positive_scores_generated_descriptor(
            MULTI_REPLICA_POSITIVE_SCORES_VIEW_ID,
            "builtin-multi-replica-positive-scores-by-user",
            b"velorix-builtin-scores-by-user-generated-package:multi_replica_positive_scores_by_user"
                .to_vec(),
        ),
    ]
}

const DEFAULT_SCORES_RELATION_ID: &str = "scores";
const DEFAULT_SCORES_RELATION_VERSION: &str = "2026-05-24.v1";
const DEFAULT_POSITIVE_SCORES_VIEW_ID: &str = "positive_scores_by_user";
const PENDING_SCORES_COMPILE_DEPLOY_VIEW_ID: &str = "pending_scores_by_user";
const MULTI_REPLICA_POSITIVE_SCORES_VIEW_ID: &str = "multi_replica_positive_scores_by_user";
const DEFAULT_POSITIVE_SCORES_SQL: &str = "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id";

fn trusted_positive_scores_generated_descriptor(
    view_id: &str,
    artifact_id: &str,
    artifact_identity_bytes: Vec<u8>,
) -> TrustedGeneratedViewDescriptor {
    TrustedGeneratedViewDescriptor {
        view_id: view_id.to_string(),
        input_relation_id: DEFAULT_SCORES_RELATION_ID.to_string(),
        input_relation_version: DEFAULT_SCORES_RELATION_VERSION.to_string(),
        sql: DEFAULT_POSITIVE_SCORES_SQL.to_string(),
        artifact_id: artifact_id.to_string(),
        artifact_identity_bytes,
        compiler: FelderaCompilerIdentity {
            name: "feldera-sql-compiler".to_string(),
            version: "builtin-default".to_string(),
            source: "velorix-linked-generated-package".to_string(),
        },
        generated_rust: GeneratedRustIdentity {
            abi_version: SUPPORTED_GENERATED_RUST_ABI_VERSION.to_string(),
            crate_name: velorix_generated_scores_by_user::CRATE_NAME.to_string(),
        },
        output_schemas: vec![positive_scores_output_schema(view_id, "")],
        state_schema_version: 1,
    }
}

fn default_scores_relation_catalog() -> Result<VelorixRelationCatalogV1, ApiError> {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: DEFAULT_SCORES_RELATION_ID.to_string(),
        relation_name: DEFAULT_SCORES_RELATION_ID.to_string(),
        relation_version: DEFAULT_SCORES_RELATION_VERSION.to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "user_id".to_string(),
                name: "user_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "score".to_string(),
                name: "score".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["user_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)
        .map_err(ApiError::bad_request)?;
    Ok(VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: DEFAULT_SCORES_RELATION_ID.to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: DEFAULT_SCORES_RELATION_ID.to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    })
}

fn default_positive_scores_view_request(
    _catalog: &VelorixRelationCatalogV1,
) -> Result<CreateViewRequest, ApiError> {
    Ok(CreateViewRequest {
        view_id: DEFAULT_POSITIVE_SCORES_VIEW_ID.to_string(),
        url_path: Some("/scores/positive".to_string()),
        input_relation_id: DEFAULT_SCORES_RELATION_ID.to_string(),
        input_relation_version: DEFAULT_SCORES_RELATION_VERSION.to_string(),
        sql: DEFAULT_POSITIVE_SCORES_SQL.to_string(),
        sql_template: None,
        description: Some("Positive score totals by user".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
        artifact: None,
    })
}

fn generated_view_artifact_for_descriptor(
    descriptor: &TrustedGeneratedViewDescriptor,
    catalog: &VelorixRelationCatalogV1,
) -> Result<FelderaCompileArtifactMetadata, ApiError> {
    descriptor
        .artifact_metadata(catalog)
        .map_err(ApiError::bad_request)
}

fn trusted_generated_descriptor_for_request(
    state: &ApiState,
    catalog: &VelorixRelationCatalogV1,
    request: &CreateViewRequest,
) -> Result<Option<TrustedGeneratedViewDescriptor>, ApiError> {
    if request.artifact.is_some() || state.allow_legacy_recovered_sql_views {
        return Ok(None);
    }
    Ok(trusted_generated_descriptor_for_shape(
        state,
        catalog,
        &request.view_id,
        &request.input_relation_id,
        &request.input_relation_version,
        &request.sql,
    ))
}

fn trusted_generated_descriptor_for_spec(
    state: &ApiState,
    catalog: &VelorixRelationCatalogV1,
    spec: &StandingViewSpec,
) -> Result<Option<TrustedGeneratedViewDescriptor>, ApiError> {
    let input = spec
        .input_relations
        .first()
        .ok_or_else(|| ApiError::bad_request("view has no input relation"))?;
    Ok(trusted_generated_descriptor_for_shape(
        state,
        catalog,
        &spec.view_id,
        &input.relation_id,
        &input.relation_version,
        &spec.sql,
    ))
}

fn trusted_generated_descriptor_for_shape(
    state: &ApiState,
    catalog: &VelorixRelationCatalogV1,
    view_id: &str,
    input_relation_id: &str,
    input_relation_version: &str,
    sql: &str,
) -> Option<TrustedGeneratedViewDescriptor> {
    state
        .trusted_generated_view_descriptors
        .iter()
        .find(|descriptor| {
            trusted_generated_descriptor_matches(
                descriptor,
                catalog,
                view_id,
                input_relation_id,
                input_relation_version,
                sql,
            )
        })
        .map(|descriptor| trusted_generated_descriptor_with_catalog_outputs(descriptor, catalog))
}

fn trusted_generated_descriptor_with_catalog_outputs(
    descriptor: &TrustedGeneratedViewDescriptor,
    catalog: &VelorixRelationCatalogV1,
) -> TrustedGeneratedViewDescriptor {
    let mut descriptor = descriptor.clone();
    if descriptor.generated_rust.crate_name == velorix_generated_scores_by_user::CRATE_NAME
        && descriptor.input_relation_id == DEFAULT_SCORES_RELATION_ID
        && descriptor.input_relation_version == DEFAULT_SCORES_RELATION_VERSION
        && descriptor.sql == DEFAULT_POSITIVE_SCORES_SQL
    {
        descriptor.output_schemas = vec![positive_scores_output_schema(
            &descriptor.view_id,
            catalog.schema_fingerprint.as_str(),
        )];
    }
    descriptor
}

fn trusted_generated_descriptor_matches(
    descriptor: &TrustedGeneratedViewDescriptor,
    catalog: &VelorixRelationCatalogV1,
    view_id: &str,
    input_relation_id: &str,
    input_relation_version: &str,
    sql: &str,
) -> bool {
    input_relation_id == descriptor.input_relation_id
        && input_relation_version == descriptor.input_relation_version
        && catalog.relation_schema.relation_id == descriptor.input_relation_id
        && catalog.relation_schema.relation_version == descriptor.input_relation_version
        && view_id == descriptor.view_id
        && descriptor.matches_view_request(view_id, input_relation_id, input_relation_version, sql)
}

fn state_has_generated_descriptor_package(
    state: &ApiState,
    descriptor: &TrustedGeneratedViewDescriptor,
) -> bool {
    state_has_generated_package(
        state,
        &GeneratedRustArtifactPackage {
            abi_version: descriptor.generated_rust.abi_version.clone(),
            crate_name: descriptor.generated_rust.crate_name.clone(),
        },
    )
}

fn state_has_generated_package(state: &ApiState, package: &GeneratedRustArtifactPackage) -> bool {
    state
        .generated_artifact_packages
        .iter()
        .any(|registered| registered == package)
}

fn positive_scores_output_schema(view_id: &str, schema_fingerprint: &str) -> RelationSchema {
    RelationSchema {
        relation_id: view_id.to_string(),
        relation_name: view_id.to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: schema_fingerprint.to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

pub async fn run_from_env() -> anyhow::Result<()> {
    let config = ApiConfig::from_env()?;
    let store = config.object_store()?;
    let validated = validate_operator_authority(
        ObjectStoreAuthorityRef {
            store_id: config.authority_store_id.clone(),
            namespace: config.authority_namespace.clone(),
        },
        store,
        config.backend_name.clone(),
        format!("v1/api-probes/{}", config.operator_id),
    )
    .await?;
    validated
        .capabilities()
        .validate_namespace(velorix_storage::capability::AuthoritativeNamespace::ArtifactCatalog)?
        .validate_for_conditional_update()
        .context(
            "velorix-api requires object-store conditional update support for active view CAS",
        )?;
    let meta_store = match config.meta_grpc_endpoint.as_ref() {
        Some(endpoint) => Some(match config.meta_bearer_token.as_ref() {
            Some(token) => {
                Arc::new(GrpcMetaStore::connect_with_bearer_token(endpoint, token).await?)
                    as Arc<dyn MetaStore>
            }
            None => Arc::new(GrpcMetaStore::connect(endpoint).await?) as Arc<dyn MetaStore>,
        }),
        None => None,
    };
    enforce_standing_runtime_fencing_startup(&config, meta_store.as_ref()).await?;
    let reconstruct_ingest_admission = config.meta_grpc_endpoint.is_none();
    let mut state = ApiState::from_validated_authority_with_ingest_admission_startup(
        validated,
        config.state_path,
        config.operator_id,
        reconstruct_ingest_admission,
    )
    .await?;
    state = state
        .with_request_limits(config.max_request_body_bytes, config.max_ingest_rows)
        .with_legacy_recovered_sql_views_allowed(config.allow_legacy_recovered_sql_views)
        .with_generic_query_enabled(config.enable_generic_query)
        .with_standing_runtime_fencing_mode(config.standing_runtime_fencing)
        .with_standing_runtime_owner_ttl_ms(config.standing_runtime_owner_ttl_ms);
    if let Some(token) = config.api_bearer_token {
        state = state
            .with_api_bearer_token(token)
            .map_err(|error| anyhow!("invalid VELORIX_API_BEARER_TOKEN: {error}"))?;
    }
    if let Some(token) = config.admin_bearer_token {
        state = state
            .with_admin_bearer_token(token)
            .map_err(|error| anyhow!("invalid VELORIX_ADMIN_BEARER_TOKEN: {error}"))?;
    }
    if let Some(meta_store) = meta_store {
        state = state.with_meta_store(meta_store);
        if let Some(endpoint) = config.meta_grpc_endpoint {
            state = state.with_meta_store_endpoint(endpoint);
        }
    }
    let generated_packages = generated_artifact_packages_from_env();
    if !generated_packages.is_empty() {
        state = state.with_generated_artifact_packages(generated_packages);
    }
    state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .map_err(|error| anyhow!(error.to_string()))?;
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    let app = app(state);
    if let Some(tls) = config.tls {
        let tls_config = RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path)
            .await
            .with_context(|| {
                format!(
                    "failed to load VELORIX_API_TLS_CERT_PATH/VELORIX_API_TLS_KEY_PATH from `{}` and `{}`",
                    tls.cert_path, tls.key_path
                )
            })?;
        let http_app = app.clone();
        let https_app = app;
        let http = async move {
            axum::serve(listener, http_app)
                .await
                .context("velorix-api HTTP listener stopped")
        };
        let https = async move {
            axum_server::tls_rustls::bind_rustls(tls.bind, tls_config)
                .serve(https_app.into_make_service())
                .await
                .context("velorix-api TLS listener stopped")
        };
        tokio::try_join!(http, https)?;
    } else {
        axum::serve(listener, app).await?;
    }
    Ok(())
}

fn generated_artifact_packages_from_env() -> Vec<String> {
    env::var("VELORIX_GENERATED_ARTIFACT_PACKAGES")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|package| !package.is_empty())
        .map(ToString::to_string)
        .collect()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CreateRelationRequest {
    #[serde(default)]
    pub catalog: Option<VelorixRelationCatalogV1>,
    #[serde(default)]
    pub default_orders_sum_count: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IngestRowsRequest {
    pub relation_id: String,
    pub relation_version: String,
    pub stream_id: String,
    pub partition_id: u32,
    pub start_offset_inclusive: u64,
    pub rows: Vec<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueryRequest {
    pub relation_id: String,
    pub relation_version: String,
    pub sql: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct StandingRuntimeOwnerReportResponse {
    pub local_owner_id: String,
    pub owners: Vec<StandingRuntimeOwnerViewResponse>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StandingRuntimeOwnerAcquireResponse {
    pub local_owner_id: String,
    pub outcomes: Vec<StandingRuntimeOwnerAcquireViewResponse>,
    pub owners: Vec<StandingRuntimeOwnerViewResponse>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StandingRuntimeOwnerAcquireViewResponse {
    pub tenant_id: String,
    pub program_id: String,
    pub view_id: String,
    pub outcome: String,
    pub current_owner: Option<StandingRuntimeOwnerClaim>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StandingRuntimeOwnerViewResponse {
    pub tenant_id: String,
    pub program_id: String,
    pub view_id: String,
    pub runtime_loaded: bool,
    pub local_owner: Option<StandingRuntimeOwnerClaim>,
    pub current_owner: Option<StandingRuntimeOwnerClaim>,
    pub current_owner_matches_local_process: bool,
    pub local_owner_matches_current_owner: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CreateQueryPolicyRequest {
    pub query_policy_id: String,
    pub policy: QueryPolicy,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CreateViewRequest {
    pub view_id: String,
    #[serde(default, rename = "urlPath", alias = "url_path")]
    pub url_path: Option<String>,
    pub input_relation_id: String,
    pub input_relation_version: String,
    pub sql: String,
    #[serde(default)]
    pub sql_template: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub request: Vec<MaterializedViewRequestFieldSpec>,
    #[serde(default)]
    pub response_schema: Option<MaterializedViewResponseSchema>,
    #[serde(default)]
    pub response_formats: Vec<String>,
    #[serde(default)]
    pub query_policy_id: Option<String>,
    #[serde(default)]
    pub artifact: Option<CreateViewArtifactRequest>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CreateViewArtifactRequest {
    pub metadata: FelderaCompileArtifactMetadata,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueryViewRequest {
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub parameters: BTreeMap<String, Value>,
    #[serde(default)]
    pub epoch: Option<u64>,
    #[serde(default)]
    pub page_token: Option<String>,
    #[serde(default)]
    pub max_rows: Option<usize>,
}

#[derive(Clone, Debug, Serialize)]
struct RelationResponse {
    relation_id: String,
    relation_version: String,
    schema_fingerprint: String,
    outcome: String,
}

#[derive(Clone, Debug, Serialize)]
struct IngestResponse {
    outcome: String,
    descriptor: IngestDescriptorResponse,
}

#[derive(Clone, Debug, Serialize)]
struct IngestDescriptorResponse {
    stream_id: String,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    object_key: String,
}

#[derive(Clone, Debug, Serialize)]
struct QueryPolicyResponse {
    tenant_id: String,
    query_policy_id: String,
    policy: QueryPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    outcome: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct QueryResponse {
    rows: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    logical_epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_page_token: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ViewResponse {
    view_id: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "urlPath")]
    url_path: Option<String>,
    input_relation_id: String,
    input_relation_version: String,
    spec_hash: String,
    execution_mode: MaterializedViewExecutionMode,
    lifecycle: MaterializedViewLifecycleStatus,
    query_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    disabled_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compile_job_id: Option<String>,
    query_endpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    request: Vec<MaterializedViewRequestFieldSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_schema: Option<MaterializedViewResponseSchema>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sql_template: Option<String>,
    response_formats: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_policy_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact: Option<MaterializedViewArtifactBinding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    outcome: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ViewCatalogResponse {
    views: Vec<ViewResponse>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ViewCompileDeployWorkerReport {
    pub pending_jobs: usize,
    pub activated: usize,
    pub skipped: usize,
    pub failed: usize,
    pub outcomes: Vec<ViewCompileDeployWorkerJobOutcome>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ViewCompileDeployJobCatalogResponse {
    pub pending_jobs: usize,
    pub jobs: Vec<ViewCompileDeployJobRecord>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ViewCompileDeployWorkerJobOutcome {
    pub job_id: String,
    pub view_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ViewCompileDeployJobStatus {
    Activated,
    Duplicate,
    Skipped(String),
}

async fn healthz() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn readyz(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    state.validate_standing_runtime_fencing_or_evict().await?;
    let metadata_store = if let Some(meta_store) = state.meta_store.as_ref() {
        let capabilities = meta_store
            .read_meta_store_capabilities()
            .await
            .map_err(meta_error_to_api)?;
        json!({
            "configured": true,
            "endpoint": state.meta_store_endpoint,
            "standing_runtime_fencing": standing_runtime_fencing_capability_json(
                &capabilities.standing_runtime_fencing
            )
        })
    } else {
        json!({ "configured": false })
    };
    Ok(Json(json!({
        "status": "ready",
        "standing_runtime_fencing_required": state.standing_runtime_fencing_required,
        "standing_runtime_fencing_mode": state.standing_runtime_fencing_mode.as_str(),
        "legacy_recovered_sql_views_allowed": state.allow_legacy_recovered_sql_views,
        "generic_query_enabled": state.enable_generic_query,
        "object_store": object_store_capabilities_json(state.capabilities.as_ref()),
        "api_auth": {
            "configured": state.api_bearer_token.is_some(),
            "mode": if state.api_bearer_token.is_some() { "bearer-token" } else { "unauthenticated-dev" },
            "max_request_body_bytes": state.max_request_body_bytes,
            "max_ingest_rows": state.max_ingest_rows,
        },
        "admin_auth": {
            "configured": state.admin_bearer_token.is_some(),
            "mode": if state.admin_bearer_token.is_some() { "bearer-token" } else { "unauthenticated-dev" },
        },
        "metadata_store": metadata_store
    })))
}

fn object_store_capabilities_json(
    capabilities: &velorix_storage::capability::AuthoritativeObjectStoreCapabilitiesV1,
) -> Value {
    let profiles = AuthoritativeNamespace::all()
        .into_iter()
        .filter_map(|namespace| {
            capabilities
                .profiles
                .get(&namespace)
                .map(|profile| (namespace.to_string(), object_store_profile_json(profile)))
        })
        .collect::<serde_json::Map<_, _>>();
    let artifact_catalog = capabilities
        .profiles
        .get(&AuthoritativeNamespace::ArtifactCatalog)
        .map(object_store_profile_json)
        .unwrap_or_else(|| json!({ "configured": false }));

    json!({
        "schema_version": 1,
        "authoritative_namespace_count": capabilities.profiles.len(),
        "artifact_catalog": artifact_catalog,
        "profiles": profiles,
    })
}

fn object_store_profile_json(profile: &ObjectStoreCapabilityProfile) -> Value {
    json!({
        "configured": true,
        "backend_name": profile.backend_name,
        "conditional_create": profile.conditional_create,
        "conditional_update": profile.conditional_update,
        "atomic_visibility": profile.atomic_visibility,
        "list_after_write": profile.list_after_write,
        "read_after_write": profile.read_after_write,
    })
}

fn standing_runtime_fencing_capability_json(
    capability: &StandingRuntimeFencingCapability,
) -> Value {
    json!({
        "capability_schema_version": capability.capability_schema_version,
        "backend_name": capability.backend_name,
        "owner_scope_kind": capability.owner_scope_kind,
        "linearizable_owner_lease": capability.linearizable_owner_lease,
        "durable_monotonic_owner_epoch": capability.durable_monotonic_owner_epoch,
        "authoritative_backend_time": capability.authoritative_backend_time,
        "owner_validated_checkpoint_publish": capability.owner_validated_checkpoint_publish,
        "publish_checks_owner_and_latest_atomically": capability.publish_checks_owner_and_latest_atomically,
        "publish_rejects_expired_owner": capability.publish_rejects_expired_owner,
        "latest_read_linearizable": capability.latest_read_linearizable,
        "publish_rejects_scope_mismatch": capability.publish_rejects_scope_mismatch,
        "max_owner_ttl_ms": capability.max_owner_ttl_ms,
        "control_plane_auth_enforced": capability.control_plane_auth_enforced,
        "production_multi_writer_safe": capability.production_multi_writer_safe,
        "backend_time_source_kind": capability.backend_time_source_kind,
        "backend_time_blocked_reason": capability.backend_time_blocked_reason,
        "lease_authority_kind": capability.lease_authority_kind,
        "lease_expiry_semantics": capability.lease_expiry_semantics,
        "bounded_wall_clock_failover": capability.bounded_wall_clock_failover,
        "failover_time_bound_ms": capability.failover_time_bound_ms,
        "multi_writer_fencing_safe": capability.multi_writer_fencing_safe,
        "production_bounded_failover_safe": capability.production_bounded_failover_safe,
    })
}

async fn create_default_orders_relation(
    State(state): State<ApiState>,
) -> Result<(StatusCode, Json<RelationResponse>), ApiError> {
    let catalog = velorix_runtime::recovery::orders_sum_count_relation_catalog()
        .map_err(ApiError::bad_request)?;
    create_relation_catalog(state, catalog).await
}

async fn create_default_scores_relation(
    State(state): State<ApiState>,
) -> Result<(StatusCode, Json<RelationResponse>), ApiError> {
    create_relation_catalog(state, default_scores_relation_catalog()?).await
}

async fn create_relation(
    State(state): State<ApiState>,
    Json(request): Json<CreateRelationRequest>,
) -> Result<(StatusCode, Json<RelationResponse>), ApiError> {
    let catalog = match (request.catalog, request.default_orders_sum_count) {
        (Some(catalog), false) | (Some(catalog), true) => catalog,
        (None, true) => velorix_runtime::recovery::orders_sum_count_relation_catalog()
            .map_err(ApiError::bad_request)?,
        (None, false) => {
            return Err(ApiError::bad_request(
                "request must include `catalog` or set `default_orders_sum_count`",
            ))
        }
    };
    create_relation_catalog(state, catalog).await
}

async fn create_relation_catalog(
    state: ApiState,
    catalog: VelorixRelationCatalogV1,
) -> Result<(StatusCode, Json<RelationResponse>), ApiError> {
    let outcome = if let Some(meta_store) = &state.meta_store {
        let outcome = meta_store
            .store_relation_catalog(catalog.clone())
            .await
            .map_err(meta_error_to_api)?;
        let _ = materialize_relation_catalog_to_object_store(&state, &catalog).await;
        match outcome {
            StoreRelationCatalogOutcome::Created => CreateRelationCatalogOutcome::Created,
            StoreRelationCatalogOutcome::Duplicate => CreateRelationCatalogOutcome::Duplicate,
        }
    } else {
        state
            .relation_registry()?
            .create(&catalog)
            .await
            .map_err(ApiError::internal)?
    };
    let (status, outcome_text) = match outcome {
        CreateRelationCatalogOutcome::Created => (StatusCode::CREATED, "created"),
        CreateRelationCatalogOutcome::Duplicate => (StatusCode::OK, "duplicate"),
    };
    Ok((
        status,
        Json(RelationResponse {
            relation_id: catalog.relation_schema.relation_id,
            relation_version: catalog.relation_schema.relation_version,
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            outcome: outcome_text.to_string(),
        }),
    ))
}

async fn materialize_relation_catalog_to_object_store(
    state: &ApiState,
    catalog: &VelorixRelationCatalogV1,
) -> Result<CreateRelationCatalogOutcome, ApiError> {
    state
        .relation_registry()?
        .create(catalog)
        .await
        .map_err(ApiError::internal)
}

async fn create_default_positive_scores_view(
    State(state): State<ApiState>,
) -> Result<(StatusCode, Json<ViewResponse>), ApiError> {
    let catalog = read_relation_catalog(
        &state,
        DEFAULT_SCORES_RELATION_ID,
        DEFAULT_SCORES_RELATION_VERSION,
    )
    .await?;
    let request = default_positive_scores_view_request(&catalog)?;

    create_view(State(state), Json(request)).await
}

async fn run_view_compile_deploy_once(
    State(state): State<ApiState>,
) -> Result<Json<ViewCompileDeployWorkerReport>, ApiError> {
    Ok(Json(state.run_view_compile_deploy_worker_once().await?))
}

async fn list_view_compile_deploy_jobs(
    State(state): State<ApiState>,
) -> Result<Json<ViewCompileDeployJobCatalogResponse>, ApiError> {
    let jobs = state
        .view_compile_deploy_job_registry()?
        .list_pending()
        .await
        .map_err(view_compile_deploy_job_registry_error_to_api)?;
    Ok(Json(ViewCompileDeployJobCatalogResponse {
        pending_jobs: jobs.len(),
        jobs,
    }))
}

async fn get_standing_runtime_owners(
    State(state): State<ApiState>,
) -> Result<Json<StandingRuntimeOwnerReportResponse>, ApiError> {
    standing_runtime_owner_report(&state).await.map(Json)
}

async fn acquire_standing_runtime_owners(
    State(state): State<ApiState>,
) -> Result<Json<StandingRuntimeOwnerAcquireResponse>, ApiError> {
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let mut outcomes = Vec::new();

    for active in active_views {
        let Some(identity) = active
            .artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
        else {
            continue;
        };
        let view_id = active.spec.view_id.clone();
        let Some(meta_store) = &state.meta_store else {
            outcomes.push(StandingRuntimeOwnerAcquireViewResponse {
                tenant_id: identity.tenant_id.clone(),
                program_id: identity.program_id.clone(),
                view_id,
                outcome: "no_meta_store".to_string(),
                current_owner: None,
            });
            continue;
        };

        let outcome = meta_store
            .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
                tenant_id: identity.tenant_id.clone(),
                program_id: identity.program_id.clone(),
                view_id: view_id.clone(),
                owner_id: state.owner_id.clone(),
                ttl_ms: state.standing_runtime_owner_ttl_ms,
            })
            .await
            .map_err(meta_error_to_api)?;
        let (outcome_name, current_owner) = match outcome {
            AcquireStandingRuntimeOwnerOutcome::Acquired(claim) => {
                state.set_standing_runtime_owner(identity, &view_id, claim.clone())?;
                ("acquired", Some(claim))
            }
            AcquireStandingRuntimeOwnerOutcome::Renewed(claim) => {
                state.set_standing_runtime_owner(identity, &view_id, claim.clone())?;
                ("renewed", Some(claim))
            }
            AcquireStandingRuntimeOwnerOutcome::Conflict(claim) => {
                state.remove_standing_runtime_with_state(identity, &view_id)?;
                ("conflict", Some(claim))
            }
        };
        outcomes.push(StandingRuntimeOwnerAcquireViewResponse {
            tenant_id: identity.tenant_id.clone(),
            program_id: identity.program_id.clone(),
            view_id,
            outcome: outcome_name.to_string(),
            current_owner,
        });
    }

    let report = standing_runtime_owner_report(&state).await?;
    Ok(Json(StandingRuntimeOwnerAcquireResponse {
        local_owner_id: report.local_owner_id,
        outcomes,
        owners: report.owners,
    }))
}

async fn standing_runtime_owner_report(
    state: &ApiState,
) -> Result<StandingRuntimeOwnerReportResponse, ApiError> {
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let mut owners = Vec::new();

    for active in active_views {
        let Some(identity) = active
            .artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
        else {
            continue;
        };
        let key = standing_runtime_key(identity, &active.spec.view_id);
        let local_state = state.standing_runtime_local_state(&key)?;
        let runtime_loaded = state
            .standing_runtime(identity, &active.spec.view_id)?
            .is_some();
        let current_owner = if let Some(meta_store) = &state.meta_store {
            meta_store
                .read_standing_runtime_owner(
                    &identity.tenant_id,
                    &identity.program_id,
                    &active.spec.view_id,
                )
                .await
                .map_err(meta_error_to_api)?
        } else {
            None
        };
        let current_owner_matches_local_process = current_owner
            .as_ref()
            .is_some_and(|owner| owner.owner_id == state.owner_id);
        let local_owner_matches_current_owner =
            match (local_state.owner.as_ref(), current_owner.as_ref()) {
                (Some(local), Some(current)) => {
                    local.owner_id == current.owner_id && local.owner_epoch == current.owner_epoch
                }
                (None, None) => true,
                _ => false,
            };

        owners.push(StandingRuntimeOwnerViewResponse {
            tenant_id: identity.tenant_id.clone(),
            program_id: identity.program_id.clone(),
            view_id: active.spec.view_id,
            runtime_loaded,
            local_owner: local_state.owner,
            current_owner,
            current_owner_matches_local_process,
            local_owner_matches_current_owner,
        });
    }

    Ok(StandingRuntimeOwnerReportResponse {
        local_owner_id: state.owner_id.clone(),
        owners,
    })
}

async fn create_view(
    State(state): State<ApiState>,
    Json(request): Json<CreateViewRequest>,
) -> Result<(StatusCode, Json<ViewResponse>), ApiError> {
    let catalog = read_relation_catalog(
        &state,
        &request.input_relation_id,
        &request.input_relation_version,
    )
    .await?;
    let trusted_generated_descriptor =
        trusted_generated_descriptor_for_request(&state, &catalog, &request)?;
    let trusted_generated_artifact = trusted_generated_descriptor
        .as_ref()
        .map(|descriptor| generated_view_artifact_for_descriptor(descriptor, &catalog))
        .transpose()?;
    let trusted_static_artifact = if trusted_generated_descriptor
        .as_ref()
        .is_some_and(|descriptor| state_has_generated_descriptor_package(&state, descriptor))
    {
        trusted_generated_artifact.clone()
    } else {
        None
    };
    let selected_artifact_metadata = request
        .artifact
        .as_ref()
        .map(|artifact_request| &artifact_request.metadata)
        .or(trusted_generated_artifact.as_ref());
    let spec = view_spec_from_request(&request, &catalog, selected_artifact_metadata)?;
    let artifact = if let Some(artifact_request) = &request.artifact {
        state.validate_standing_runtime_fencing_or_evict().await?;
        Some(register_view_artifact(&state, &catalog, &spec, &artifact_request.metadata).await?)
    } else if let Some(artifact_metadata) = &trusted_static_artifact {
        state.validate_standing_runtime_fencing_or_evict().await?;
        Some(register_view_artifact(&state, &catalog, &spec, artifact_metadata).await?)
    } else if state.allow_legacy_recovered_sql_views {
        validate_supported_dbsp_view_sql(&spec.sql, &catalog).map_err(|error| {
            ApiError::bad_request(format!(
                "legacy_recovered_sql DBSP bootstrap guard rejected view SQL: {error}. Product generated-package views can support wider SQL only when a trusted generated package is available"
            ))
        })?;
        None
    } else {
        None
    };
    let spec_hash = feldera_spec_hash(&spec).map_err(ApiError::bad_request)?;
    let api_metadata = api_metadata_from_create_view_request(&request);
    validate_view_api_metadata(&api_metadata)?;
    validate_query_policy_reference(&state, &api_metadata).await?;
    if artifact.is_some() {
        validate_standing_runtime_create_api_metadata(
            &spec.view_id,
            &api_metadata,
            &spec.output_relations,
        )
        .await?;
    }
    let pending_runtime = if let (Some(artifact), Some(artifact_metadata)) =
        (&artifact, selected_artifact_metadata)
    {
        build_standing_runtime_for_artifact(
            &state,
            &spec.view_id,
            artifact,
            &artifact_metadata.input_schemas,
            &artifact_metadata.output_schemas,
        )?
    } else {
        None
    };
    let execution_mode = if artifact.is_some() {
        MaterializedViewExecutionMode::StandingRuntime
    } else if state.allow_legacy_recovered_sql_views {
        MaterializedViewExecutionMode::LegacyRecoveredSql
    } else {
        MaterializedViewExecutionMode::FelderaCompilePending
    };
    let lifecycle = lifecycle_for_create_view_execution(&execution_mode);
    let outcome = if let Some(runtime) = pending_runtime {
        let operation_lock =
            state.standing_runtime_operation_lock(runtime.program_identity(), &spec.view_id)?;
        let _operation_guard = operation_lock.lock().await;
        let outcome = state
            .view_registry()?
            .register_with_api_metadata_artifact_execution(
                &spec,
                Some(api_metadata.clone()),
                artifact.clone(),
                Some(execution_mode.clone()),
                Some(lifecycle.clone()),
            )
            .await
            .map_err(materialized_view_registry_error_to_api)?;
        insert_standing_runtime(&state, &spec.view_id, runtime)?;
        outcome
    } else {
        state
            .view_registry()?
            .register_with_api_metadata_artifact_execution(
                &spec,
                Some(api_metadata.clone()),
                artifact.clone(),
                Some(execution_mode.clone()),
                Some(lifecycle.clone()),
            )
            .await
            .map_err(materialized_view_registry_error_to_api)?
    };
    if execution_mode == MaterializedViewExecutionMode::FelderaCompilePending {
        state
            .view_compile_deploy_job_registry()?
            .register_pending_for_spec(&spec, &spec_hash, &lifecycle)
            .await
            .map_err(view_compile_deploy_job_registry_error_to_api)?;
    }
    let (status, outcome_text) = match outcome {
        RegisterMaterializedViewOutcome::Created => {
            if execution_mode == MaterializedViewExecutionMode::FelderaCompilePending {
                (StatusCode::ACCEPTED, "compile_pending")
            } else {
                (StatusCode::CREATED, "created")
            }
        }
        RegisterMaterializedViewOutcome::Duplicate => (StatusCode::OK, "duplicate"),
    };

    Ok((
        status,
        Json(view_response(
            &spec,
            spec_hash,
            execution_mode,
            lifecycle,
            state.allow_legacy_recovered_sql_views,
            Some(api_metadata),
            artifact,
            Some(outcome_text),
        )?),
    ))
}

async fn register_view_artifact(
    state: &ApiState,
    catalog: &VelorixRelationCatalogV1,
    spec: &StandingViewSpec,
    artifact: &FelderaCompileArtifactMetadata,
) -> Result<MaterializedViewArtifactBinding, ApiError> {
    let registered = state
        .runtime_feldera_artifact_registry()?
        .register_trusted_artifact(catalog, spec, artifact)
        .await
        .map_err(ApiError::bad_request)?;
    let execution_status = runtime_artifact_status_text(&registered.status);
    if !matches!(
        registered.status,
        RuntimeFelderaArtifactSelectionStatus::DirectExecutionEnabled { .. }
    ) {
        return Err(ApiError::bad_request(format!(
            "generated Rust package `{}` is not registered with this Velorix binary",
            artifact.generated_rust.crate_name
        )));
    }

    Ok(MaterializedViewArtifactBinding {
        artifact_id: artifact.artifact_id.clone(),
        artifact_hash: artifact.artifact_hash.clone(),
        generated_rust_crate_name: artifact.generated_rust.crate_name.clone(),
        state_codec: artifact.state_codec.clone(),
        state_schema_version: artifact.state_schema_version,
        execution_status: execution_status.to_string(),
        execution_path: "static_release_artifact".to_string(),
        standing_program_identity: Some(standing_program_identity_from_artifact(
            catalog, spec, artifact,
        )?),
    })
}

fn standing_program_identity_from_artifact(
    catalog: &VelorixRelationCatalogV1,
    spec: &StandingViewSpec,
    artifact: &FelderaCompileArtifactMetadata,
) -> Result<StandingProgramIdentity, ApiError> {
    let output_schema_bytes = serde_json::to_vec(&artifact.output_schemas)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let identity = StandingProgramIdentity {
        tenant_id: "default".to_string(),
        program_id: spec.view_id.clone(),
        view_ids: vec![spec.view_id.clone()],
        sql_hash: feldera_artifact_bytes_hash(spec.sql.as_bytes()),
        input_catalog_hash: catalog.schema_fingerprint.as_str().to_string(),
        output_schema_hash: feldera_artifact_bytes_hash(&output_schema_bytes),
        compiler_identity: format!("{}:{}", artifact.compiler.name, artifact.compiler.version),
        runtime_packages: vec![FelderaRuntimePackageIdentity {
            name: artifact.generated_rust.crate_name.clone(),
            version: artifact.generated_rust.abi_version.clone(),
        }],
        package_feature_set: vec!["static_release_artifact".to_string()],
        dbsp_runtime_compatibility: artifact.generated_rust.abi_version.clone(),
        checkpoint_codec_identity: artifact.state_codec.clone(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
    };
    identity.validate().map_err(ApiError::bad_request)?;
    Ok(identity)
}

async fn ensure_standing_runtime_for_artifact(
    state: &ApiState,
    view_id: &str,
    artifact: &MaterializedViewArtifactBinding,
    expected_input_schemas: &[RelationSchema],
    expected_output_schemas: &[RelationSchema],
) -> Result<Option<Vec<ReplayCheckpoint>>, ApiError> {
    let Some((runtime, replay_checkpoints)) = restore_or_build_standing_runtime_for_artifact(
        state,
        view_id,
        artifact,
        expected_input_schemas,
        expected_output_schemas,
    )
    .await
    .map_err(|error| active_artifact_runtime_unavailable_error(view_id, artifact, error))?
    else {
        return Ok(None);
    };
    let committed_checkpoint =
        read_latest_standing_runtime_checkpoint(state, runtime.program_identity(), view_id)
            .await?
            .as_ref()
            .map(standing_runtime_checkpoint_pointer_from_record);
    state.set_standing_runtime_committed_checkpoint(
        runtime.program_identity(),
        view_id,
        committed_checkpoint,
    )?;
    insert_standing_runtime(state, view_id, runtime)?;
    Ok(Some(replay_checkpoints))
}

fn active_artifact_runtime_unavailable_error(
    view_id: &str,
    artifact: &MaterializedViewArtifactBinding,
    error: ApiError,
) -> ApiError {
    if error.status == StatusCode::BAD_REQUEST
        && error
            .message
            .contains("standing runtime factory is not registered")
    {
        return ApiError::service_unavailable(format!(
            "standing runtime is unavailable for active artifact-backed view `{}`: generated Rust crate `{}` is not linked in this Velorix binary",
            view_id, artifact.generated_rust_crate_name
        ));
    }
    error
}

fn build_standing_runtime_for_artifact(
    state: &ApiState,
    view_id: &str,
    artifact: &MaterializedViewArtifactBinding,
    expected_input_schemas: &[RelationSchema],
    expected_output_schemas: &[RelationSchema],
) -> Result<Option<Box<dyn StandingProgramRuntime + Send>>, ApiError> {
    let Some(identity) = artifact.standing_program_identity.as_ref() else {
        return Ok(None);
    };
    if state.standing_runtime(identity, view_id)?.is_some() {
        return Ok(None);
    }
    let Some(factory) = state.standing_runtime_factory(&artifact.generated_rust_crate_name)? else {
        return Err(ApiError::bad_request(format!(
            "standing runtime factory is not registered for generated Rust crate `{}`",
            artifact.generated_rust_crate_name
        )));
    };
    let runtime = factory.create(identity).map_err(ApiError::internal)?;
    if runtime.program_identity() != identity {
        return Err(ApiError::bad_request(
            StandingProgramRuntimeError::ProgramIdentityMismatch {
                expected_program_id: identity.program_id.clone(),
                actual_program_id: runtime.program_identity().program_id.clone(),
            },
        ));
    }
    validate_runtime_schemas(
        runtime.as_ref(),
        expected_input_schemas,
        expected_output_schemas,
    )?;
    Ok(Some(runtime))
}

async fn restore_or_build_standing_runtime_for_artifact(
    state: &ApiState,
    view_id: &str,
    artifact: &MaterializedViewArtifactBinding,
    expected_input_schemas: &[RelationSchema],
    expected_output_schemas: &[RelationSchema],
) -> Result<
    Option<(
        Box<dyn StandingProgramRuntime + Send>,
        Vec<ReplayCheckpoint>,
    )>,
    ApiError,
> {
    let Some(identity) = artifact.standing_program_identity.as_ref() else {
        return Ok(None);
    };
    if state.standing_runtime(identity, view_id)?.is_some() {
        return Ok(None);
    }
    let Some(factory) = state.standing_runtime_factory(&artifact.generated_rust_crate_name)? else {
        return Err(ApiError::bad_request(format!(
            "standing runtime factory is not registered for generated Rust crate `{}`",
            artifact.generated_rust_crate_name
        )));
    };

    let (runtime, replay_checkpoints) = if let Some(record) =
        read_latest_standing_runtime_checkpoint(state, identity, view_id).await?
    {
        record
            .checkpoint
            .validate_identity(identity)
            .map_err(ApiError::bad_request)?;
        if record.checkpoint.state_payload.is_some() {
            (
                factory
                    .restore(record.checkpoint)
                    .map_err(ApiError::internal)?,
                record.replay_checkpoints,
            )
        } else {
            (
                factory.create(identity).map_err(ApiError::internal)?,
                Vec::new(),
            )
        }
    } else {
        (
            factory.create(identity).map_err(ApiError::internal)?,
            Vec::new(),
        )
    };
    if runtime.program_identity() != identity {
        return Err(ApiError::bad_request(
            StandingProgramRuntimeError::ProgramIdentityMismatch {
                expected_program_id: identity.program_id.clone(),
                actual_program_id: runtime.program_identity().program_id.clone(),
            },
        ));
    }
    validate_runtime_schemas(
        runtime.as_ref(),
        expected_input_schemas,
        expected_output_schemas,
    )?;
    Ok(Some((runtime, replay_checkpoints)))
}

fn validate_runtime_schemas(
    runtime: &(dyn StandingProgramRuntime + Send),
    expected_input_schemas: &[RelationSchema],
    expected_output_schemas: &[RelationSchema],
) -> Result<(), ApiError> {
    let actual_input_schemas = runtime.input_schemas();
    if actual_input_schemas != expected_input_schemas {
        return Err(ApiError::bad_request(
            "standing runtime input schemas do not match artifact metadata",
        ));
    }
    let actual_output_schemas = runtime.output_schemas();
    if actual_output_schemas != expected_output_schemas {
        return Err(ApiError::bad_request(
            "standing runtime output schemas do not match artifact metadata",
        ));
    }

    Ok(())
}

fn insert_standing_runtime(
    state: &ApiState,
    view_id: &str,
    runtime: Box<dyn StandingProgramRuntime + Send>,
) -> Result<(), ApiError> {
    let key = standing_runtime_key(runtime.program_identity(), view_id);
    let mut runtimes = state
        .standing_runtimes
        .runtimes
        .lock()
        .map_err(|_| ApiError::internal("standing runtime registry lock poisoned"))?;
    runtimes.insert(key, Arc::new(Mutex::new(runtime)));
    Ok(())
}

fn remove_standing_runtime(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> Result<(), ApiError> {
    let mut runtimes = state
        .standing_runtimes
        .runtimes
        .lock()
        .map_err(|_| ApiError::internal("standing runtime registry lock poisoned"))?;
    let key = standing_runtime_key(identity, view_id);
    runtimes.remove(&key);
    let mut local_state = state
        .standing_runtimes
        .local_state
        .lock()
        .map_err(|_| ApiError::internal("standing runtime local state lock poisoned"))?;
    local_state.remove(&key);
    Ok(())
}

fn compile_job_request_matches_active_spec(
    job: &ViewCompileDeployJobRecord,
    spec: &StandingViewSpec,
) -> bool {
    let Some(request) = &job.compiler_request else {
        return false;
    };

    request.view_id == spec.view_id
        && request.spec_hash == job.spec_hash
        && request.sql == spec.sql
        && request.input_relations == spec.input_relations
        && request.output_relations == spec.output_relations
        && request.shape == spec.shape
}

fn standing_runtime_key(identity: &StandingProgramIdentity, view_id: &str) -> StandingRuntimeKey {
    StandingRuntimeKey {
        tenant_id: identity.tenant_id.clone(),
        program_id: identity.program_id.clone(),
        view_id: view_id.to_string(),
    }
}

fn standing_runtime_owner_token_from_claim(
    claim: &StandingRuntimeOwnerClaim,
) -> StandingRuntimeOwnerToken {
    StandingRuntimeOwnerToken {
        tenant_id: claim.tenant_id.clone(),
        program_id: claim.program_id.clone(),
        view_id: claim.view_id.clone(),
        owner_id: claim.owner_id.clone(),
        owner_epoch: claim.owner_epoch,
    }
}

fn process_incarnation_owner_id(operator_id: String) -> Result<String, ApiError> {
    let operator_id = operator_id.trim();
    if operator_id.is_empty() {
        return Err(ApiError::bad_request("operator_id must not be empty"));
    }
    let boot_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ApiError::internal("system clock is before unix epoch"))?
        .as_nanos();
    Ok(format!(
        "{operator_id}/pid-{}/boot-{boot_nanos}",
        std::process::id()
    ))
}

async fn list_views(State(state): State<ApiState>) -> Result<Json<ViewCatalogResponse>, ApiError> {
    let views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?
        .iter()
        .map(|view| active_view_response(&state, view, None))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Json(ViewCatalogResponse { views }))
}

async fn get_view(
    State(state): State<ApiState>,
    AxumPath(view_id): AxumPath<String>,
) -> Result<Json<ViewResponse>, ApiError> {
    let active = state
        .view_registry()?
        .read_active(&view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;

    Ok(Json(active_view_response(&state, &active, None)?))
}

async fn ingest_rows(
    State(state): State<ApiState>,
    Json(request): Json<IngestRowsRequest>,
) -> Result<(StatusCode, Json<IngestResponse>), ApiError> {
    if request.rows.len() > state.max_ingest_rows {
        return Err(ApiError::payload_too_large(format!(
            "ingest row count {} exceeds configured limit {}",
            request.rows.len(),
            state.max_ingest_rows
        )));
    }
    let catalog =
        read_relation_catalog(&state, &request.relation_id, &request.relation_version).await?;
    let batch = rows_to_record_batch(&catalog, &request.rows)?;
    let end_offset_exclusive = request
        .start_offset_inclusive
        .checked_add(request.rows.len() as u64)
        .ok_or_else(|| ApiError::bad_request("ingest offset range overflow"))?;
    let envelope = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: request.relation_id.clone(),
            relation_version: request.relation_version.clone(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: request.stream_id.clone(),
            partition_id: request.partition_id,
            start_offset_inclusive: request.start_offset_inclusive,
            end_offset_exclusive,
        },
        &[batch.clone()],
    )
    .map_err(ApiError::bad_request)?;
    ensure_standing_runtimes_for_ingest(&state, &request).await?;
    preacquire_standing_runtime_owners_for_ingest(&state, &request).await?;
    if state.meta_store.is_some() {
        reserve_ingest_range(&state, &request, &catalog, end_offset_exclusive, &envelope).await?;
    }
    let outcome = append_ingest_envelope(&state, envelope).await?;
    let (status, outcome, descriptor) = ingest_outcome_parts(outcome)?;
    if matches!(outcome, "appended" | "duplicate") {
        apply_standing_runtime_ingest(&state, &request).await?;
    }

    Ok((
        status,
        Json(IngestResponse {
            outcome: outcome.to_string(),
            descriptor: ingest_descriptor_response(&descriptor),
        }),
    ))
}

async fn ensure_standing_runtimes_for_ingest(
    state: &ApiState,
    request: &IngestRowsRequest,
) -> Result<(), ApiError> {
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let mut needs_restore = false;
    for active in &active_views {
        let Some(input) = active.spec.input_relations.first() else {
            continue;
        };
        let Some(identity) = active
            .artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
        else {
            continue;
        };
        if input.relation_id != request.relation_id
            || input.relation_version != request.relation_version
        {
            continue;
        }
        if state
            .standing_runtime(identity, &active.spec.view_id)?
            .is_none()
        {
            needs_restore = true;
            break;
        }
    }

    if needs_restore {
        state
            .restore_standing_program_runtimes_from_active_views()
            .await?;
    }

    for active in active_views {
        let Some(input) = active.spec.input_relations.first() else {
            continue;
        };
        let Some(identity) = active
            .artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
        else {
            continue;
        };
        if input.relation_id != request.relation_id
            || input.relation_version != request.relation_version
        {
            continue;
        }
        if state
            .standing_runtime(identity, &active.spec.view_id)?
            .is_none()
        {
            return Err(ApiError::service_unavailable(format!(
                "standing runtime is unavailable for active artifact-backed view `{}`",
                active.spec.view_id
            )));
        }
    }

    Ok(())
}

async fn preacquire_standing_runtime_owners_for_ingest(
    state: &ApiState,
    request: &IngestRowsRequest,
) -> Result<(), ApiError> {
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    for active in active_views {
        let Some(input) = active.spec.input_relations.first() else {
            continue;
        };
        let Some(identity) = active
            .artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
        else {
            continue;
        };
        if input.relation_id != request.relation_id
            || input.relation_version != request.relation_version
        {
            continue;
        }
        state
            .acquire_standing_runtime_owner(identity, &active.spec.view_id)
            .await?;
    }

    Ok(())
}

async fn apply_standing_runtime_ingest(
    state: &ApiState,
    request: &IngestRowsRequest,
) -> Result<(), ApiError> {
    state.validate_standing_runtime_fencing_or_evict().await?;
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    for active in active_views {
        let Some(input) = active.spec.input_relations.first() else {
            continue;
        };
        let Some(identity) = active
            .artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
        else {
            continue;
        };
        if input.relation_id != request.relation_id
            || input.relation_version != request.relation_version
        {
            continue;
        }
        let latest_checkpoint =
            read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id).await?;
        let replay_checkpoints = latest_checkpoint
            .as_ref()
            .map(|record| record.replay_checkpoints.as_slice())
            .unwrap_or(&[]);
        if let Err(error) =
            replay_committed_ingest_into_standing_runtime(state, &active, replay_checkpoints).await
        {
            return Err(error);
        }
    }

    Ok(())
}

fn next_standing_runtime_logical_epoch(
    runtime: &(dyn StandingProgramRuntime + Send),
    lower_bound: u64,
) -> Result<u64, ApiError> {
    let next = runtime
        .logical_epoch()
        .checked_add(1)
        .ok_or_else(|| ApiError::bad_request("standing runtime logical epoch overflow"))?;
    Ok(next.max(lower_bound))
}

async fn persist_standing_runtime_checkpoint(
    state: &ApiState,
    view_id: &str,
    checkpoint: &RuntimeCheckpoint,
    replay_checkpoint: ReplayCheckpoint,
    owner: Option<StandingRuntimeOwnerToken>,
) -> Result<(), ApiError> {
    if !checkpoint.identity.view_ids.iter().any(|id| id == view_id) {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint identity does not include view `{view_id}`"
        )));
    }
    let checkpoint_key = ObjectKey::standing_runtime_checkpoint(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
        checkpoint.logical_epoch,
        &checkpoint.state_root.content_hash,
    )
    .map_err(ApiError::bad_request)?;
    let previous_record =
        read_latest_standing_runtime_checkpoint(state, &checkpoint.identity, view_id).await?;
    let expected_previous = previous_record
        .as_ref()
        .map(standing_runtime_checkpoint_pointer_from_record);
    let candidate = standing_runtime_checkpoint_pointer_from_key(
        &checkpoint_key,
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
        checkpoint.logical_epoch,
        &checkpoint.state_root.content_hash,
    )?;
    let previous_checkpoint = if expected_previous.as_ref() == Some(&candidate) {
        previous_record
            .as_ref()
            .and_then(|record| record.previous_checkpoint.clone())
    } else {
        expected_previous.clone()
    };
    let latest_key = ObjectKey::standing_runtime_latest_checkpoint(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
    )
    .map_err(ApiError::bad_request)?;
    let replay_checkpoints =
        merged_standing_runtime_replay_checkpoints(previous_record.as_ref(), replay_checkpoint);
    let record = StandingRuntimeCheckpointRecord {
        schema_version: 1,
        record_kind: "standing_runtime_checkpoint_v1".to_string(),
        view_id: view_id.to_string(),
        checkpoint_key: checkpoint_key.as_str().to_string(),
        previous_checkpoint,
        checkpoint: checkpoint.clone(),
        replay_checkpoints,
    };
    let bytes =
        serde_json::to_vec(&record).map_err(|source| ApiError::internal(source.to_string()))?;
    let checkpoint_path = ObjectPath::from(checkpoint_key.as_str());
    let result = state
        .store
        .put_opts(
            &checkpoint_path,
            bytes::Bytes::from(bytes.clone()).into(),
            PutMode::Create.into(),
        )
        .await;
    match result {
        Ok(_) => {}
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = state
                .store
                .get(&checkpoint_path)
                .await
                .map_err(ApiError::internal)?
                .bytes()
                .await
                .map_err(ApiError::internal)?;
            if existing.as_ref() != bytes.as_slice() {
                return Err(ApiError::conflict(format!(
                    "standing runtime checkpoint conflict at {}",
                    checkpoint_key.as_str()
                )));
            }
        }
        Err(error) => return Err(ApiError::internal(error)),
    }
    publish_standing_runtime_checkpoint_pointer(state, expected_previous, candidate.clone(), owner)
        .await?;
    state.set_standing_runtime_committed_checkpoint(
        &checkpoint.identity,
        view_id,
        Some(candidate),
    )?;
    let latest_write = state
        .store
        .put(
            &ObjectPath::from(latest_key.as_str()),
            bytes::Bytes::from(bytes).into(),
        )
        .await;
    if let Err(error) = latest_write {
        if state.meta_store.is_none() {
            return Err(ApiError::internal(error));
        }
    }

    Ok(())
}

async fn publish_standing_runtime_checkpoint_pointer(
    state: &ApiState,
    expected_previous: Option<StandingRuntimeCheckpointPointer>,
    candidate: StandingRuntimeCheckpointPointer,
    owner: Option<StandingRuntimeOwnerToken>,
) -> Result<(), ApiError> {
    let Some(meta_store) = &state.meta_store else {
        return Ok(());
    };
    let owner = owner.ok_or_else(|| {
        ApiError::service_unavailable("standing runtime owner is required for checkpoint publish")
    })?;
    match meta_store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous,
            candidate: candidate.clone(),
            owner,
        })
        .await
        .map_err(meta_error_to_api)?
    {
        PublishStandingRuntimeCheckpointOutcome::Published
        | PublishStandingRuntimeCheckpointOutcome::Duplicate => Ok(()),
        PublishStandingRuntimeCheckpointOutcome::Conflict => Err(ApiError::conflict(format!(
            "standing runtime checkpoint publish conflict for `{}/{}/{}` at epoch {}",
            candidate.tenant_id, candidate.program_id, candidate.view_id, candidate.logical_epoch
        ))),
    }
}

fn standing_runtime_checkpoint_pointer_from_record(
    record: &StandingRuntimeCheckpointRecord,
) -> StandingRuntimeCheckpointPointer {
    StandingRuntimeCheckpointPointer {
        tenant_id: record.checkpoint.identity.tenant_id.clone(),
        program_id: record.checkpoint.identity.program_id.clone(),
        view_id: record.view_id.clone(),
        checkpoint_key: record.checkpoint_key.clone(),
        logical_epoch: record.checkpoint.logical_epoch,
        content_hash: record.checkpoint.state_root.content_hash.clone(),
    }
}

fn standing_runtime_checkpoint_pointer_from_key(
    checkpoint_key: &ObjectKey,
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
    logical_epoch: u64,
    content_hash: &str,
) -> Result<StandingRuntimeCheckpointPointer, ApiError> {
    let pointer = StandingRuntimeCheckpointPointer {
        tenant_id: tenant_id.to_string(),
        program_id: program_id.to_string(),
        view_id: view_id.to_string(),
        checkpoint_key: checkpoint_key.as_str().to_string(),
        logical_epoch,
        content_hash: content_hash.to_string(),
    };
    let (_, parts) = ObjectKey::parse_standing_runtime_checkpoint(pointer.checkpoint_key.clone())
        .map_err(ApiError::bad_request)?;
    if parts.tenant_id != pointer.tenant_id
        || parts.program_id != pointer.program_id
        || parts.view_id != pointer.view_id
        || parts.logical_epoch != pointer.logical_epoch
        || parts.content_hash != pointer.content_hash
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint pointer key/body mismatch for `{tenant_id}/{program_id}/{view_id}`"
        )));
    }
    Ok(pointer)
}

async fn read_latest_standing_runtime_checkpoint(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> Result<Option<StandingRuntimeCheckpointRecord>, ApiError> {
    ObjectKey::standing_runtime_latest_checkpoint(
        &identity.tenant_id,
        &identity.program_id,
        view_id,
    )
    .map_err(ApiError::bad_request)?;
    if let Some(meta_store) = &state.meta_store {
        let Some(pointer) = meta_store
            .read_standing_runtime_checkpoint(&identity.tenant_id, &identity.program_id, view_id)
            .await
            .map_err(meta_error_to_api)?
        else {
            return Ok(None);
        };
        return read_standing_runtime_checkpoint_record_from_pointer(
            state, identity, view_id, &pointer,
        )
        .await
        .map(Some);
    }
    let prefix = ObjectPath::from(format!(
        "v1/standing-runtime-checkpoints/{}/{}/{view_id}/epochs",
        identity.tenant_id, identity.program_id
    ));
    let mut stream = state.store.list(Some(&prefix));
    let mut latest_checkpoint: Option<(
        String,
        velorix_storage::object_key::StandingRuntimeCheckpointKeyParts,
    )> = None;
    while let Some(meta) = stream.try_next().await.map_err(ApiError::internal)? {
        let location = meta.location.to_string();
        if location.ends_with(".checkpoint.json") {
            let (_, parts) = ObjectKey::parse_standing_runtime_checkpoint(location.clone())
                .map_err(ApiError::bad_request)?;
            latest_checkpoint = Some(match latest_checkpoint {
                Some((current, current_parts))
                    if current_parts.logical_epoch > parts.logical_epoch =>
                {
                    (current, current_parts)
                }
                Some((_current, current_parts))
                    if current_parts.logical_epoch == parts.logical_epoch =>
                {
                    return Err(ApiError::bad_request(format!(
                        "multiple standing runtime checkpoints for `{}/{}/{view_id}` epoch {}",
                        identity.tenant_id, identity.program_id, parts.logical_epoch
                    )));
                }
                _ => (location, parts),
            });
        }
    }

    let Some((latest_checkpoint_path, checkpoint_key_parts)) = latest_checkpoint else {
        return Ok(None);
    };
    let bytes = state
        .store
        .get(&ObjectPath::from(latest_checkpoint_path.clone()))
        .await
        .map_err(ApiError::internal)?
        .bytes()
        .await
        .map_err(ApiError::internal)?;
    let mut record: StandingRuntimeCheckpointRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    if record.checkpoint_key.is_empty() {
        record.checkpoint_key = latest_checkpoint_path.clone();
    }
    if record.schema_version != 1
        || record.record_kind != "standing_runtime_checkpoint_v1"
        || record.view_id != view_id
        || record.checkpoint.identity != *identity
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint record identity mismatch for `{}/{}/{view_id}`",
            identity.tenant_id, identity.program_id
        )));
    }
    if checkpoint_key_parts.tenant_id != record.checkpoint.identity.tenant_id
        || checkpoint_key_parts.program_id != record.checkpoint.identity.program_id
        || checkpoint_key_parts.view_id != record.view_id
        || checkpoint_key_parts.logical_epoch != record.checkpoint.logical_epoch
        || checkpoint_key_parts.content_hash != record.checkpoint.state_root.content_hash
        || record.checkpoint_key != latest_checkpoint_path
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint object key/body mismatch for `{}/{}/{view_id}`",
            identity.tenant_id, identity.program_id
        )));
    }
    validate_standing_runtime_checkpoint_replay_frontiers(&record)?;

    Ok(Some(record))
}

async fn read_standing_runtime_checkpoint_record_from_pointer(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
    pointer: &StandingRuntimeCheckpointPointer,
) -> Result<StandingRuntimeCheckpointRecord, ApiError> {
    let bytes = state
        .store
        .get(&ObjectPath::from(pointer.checkpoint_key.clone()))
        .await
        .map_err(ApiError::internal)?
        .bytes()
        .await
        .map_err(ApiError::internal)?;
    let mut record: StandingRuntimeCheckpointRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    if record.checkpoint_key.is_empty() {
        record.checkpoint_key = pointer.checkpoint_key.clone();
    }
    validate_standing_runtime_checkpoint_record(identity, view_id, pointer, &record)?;
    validate_standing_runtime_checkpoint_replay_frontiers(&record)?;
    Ok(record)
}

fn validate_standing_runtime_checkpoint_record(
    identity: &StandingProgramIdentity,
    view_id: &str,
    pointer: &StandingRuntimeCheckpointPointer,
    record: &StandingRuntimeCheckpointRecord,
) -> Result<(), ApiError> {
    if record.schema_version != 1
        || record.record_kind != "standing_runtime_checkpoint_v1"
        || record.view_id != view_id
        || record.checkpoint.identity != *identity
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint record identity mismatch for `{}/{}/{view_id}`",
            identity.tenant_id, identity.program_id
        )));
    }
    if pointer.tenant_id != record.checkpoint.identity.tenant_id
        || pointer.program_id != record.checkpoint.identity.program_id
        || pointer.view_id != record.view_id
        || pointer.checkpoint_key != record.checkpoint_key
        || pointer.logical_epoch != record.checkpoint.logical_epoch
        || pointer.content_hash != record.checkpoint.state_root.content_hash
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint pointer/body mismatch for `{}/{}/{view_id}`",
            identity.tenant_id, identity.program_id
        )));
    }
    let (checkpoint_key, checkpoint_key_parts) =
        ObjectKey::parse_standing_runtime_checkpoint(pointer.checkpoint_key.clone())
            .map_err(ApiError::bad_request)?;
    if checkpoint_key_parts.tenant_id != pointer.tenant_id
        || checkpoint_key_parts.program_id != pointer.program_id
        || checkpoint_key_parts.view_id != pointer.view_id
        || checkpoint_key_parts.logical_epoch != pointer.logical_epoch
        || checkpoint_key_parts.content_hash != pointer.content_hash
        || checkpoint_key.as_str() != pointer.checkpoint_key
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint pointer key/body mismatch for `{}/{}/{view_id}`",
            identity.tenant_id, identity.program_id
        )));
    }
    Ok(())
}

fn validate_standing_runtime_checkpoint_replay_frontiers(
    record: &StandingRuntimeCheckpointRecord,
) -> Result<(), ApiError> {
    if record.checkpoint.state_payload.is_none() {
        return Ok(());
    }

    let checkpoint_input_frontier = record
        .checkpoint
        .input_frontiers
        .iter()
        .map(|frontier| frontier.committed_offset_exclusive)
        .max()
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "standing runtime checkpoint has no input frontier for view `{}`",
                record.view_id
            ))
        })?;
    let mut seen = BTreeSet::new();
    for replay in &record.replay_checkpoints {
        if !seen.insert((replay.stream_id.as_str(), replay.partition_id)) {
            return Err(ApiError::bad_request(format!(
                "duplicate standing runtime checkpoint replay frontier for view `{}` stream={} partition={}",
                record.view_id, replay.stream_id, replay.partition_id
            )));
        }
        if replay.end_offset_exclusive > checkpoint_input_frontier {
            return Err(ApiError::bad_request(format!(
                "standing runtime checkpoint replay frontier is ahead of checkpoint input frontier for view `{}` stream={} partition={} replay_end={} checkpoint_end={}",
                record.view_id,
                replay.stream_id,
                replay.partition_id,
                replay.end_offset_exclusive,
                checkpoint_input_frontier
            )));
        }
    }

    Ok(())
}

fn merged_standing_runtime_replay_checkpoints(
    previous_record: Option<&StandingRuntimeCheckpointRecord>,
    replay_checkpoint: ReplayCheckpoint,
) -> Vec<ReplayCheckpoint> {
    let mut replay_checkpoints = previous_record
        .map(|record| record.replay_checkpoints.clone())
        .unwrap_or_default();
    if let Some(existing) = replay_checkpoints.iter_mut().find(|existing| {
        existing.stream_id == replay_checkpoint.stream_id
            && existing.partition_id == replay_checkpoint.partition_id
    }) {
        existing.end_offset_exclusive = existing
            .end_offset_exclusive
            .max(replay_checkpoint.end_offset_exclusive);
    } else {
        replay_checkpoints.push(replay_checkpoint);
    }
    replay_checkpoints.sort_by(|left, right| {
        left.stream_id
            .cmp(&right.stream_id)
            .then(left.partition_id.cmp(&right.partition_id))
    });

    replay_checkpoints
}

async fn replay_committed_ingest_into_standing_runtime(
    state: &ApiState,
    active: &ActiveMaterializedView,
    replay_checkpoints: &[ReplayCheckpoint],
) -> Result<(), ApiError> {
    let Some(input) = active.spec.input_relations.first() else {
        return Ok(());
    };
    let Some(identity) = active
        .artifact
        .as_ref()
        .and_then(|artifact| artifact.standing_program_identity.as_ref())
    else {
        return Ok(());
    };
    let operation_lock = state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
    let _operation_guard = operation_lock.lock().await;
    let Some(runtime) = state.standing_runtime(identity, &active.spec.view_id)? else {
        return Ok(());
    };
    let ingest_log =
        IngestLog::new_catalog_checked(Arc::clone(&state.store), state.capabilities.as_ref())
            .map_err(ApiError::internal)?;
    let batches = ingest_log
        .replay_admitted_validated_envelopes_from(replay_checkpoints)
        .await
        .map_err(ApiError::internal)?;

    for batch in batches {
        let descriptor = batch.descriptor();
        let envelope =
            IngestEnvelope::decode(batch.payload().clone()).map_err(ApiError::bad_request)?;
        let header = envelope.header();
        if header.relation_id != input.relation_id
            || header.relation_version != input.relation_version
            || header.schema_fingerprint != input.schema_fingerprint
        {
            continue;
        }
        let owner = state
            .acquire_standing_runtime_owner(identity, &active.spec.view_id)
            .await?;
        let idempotency_key = EpochIdempotencyKey::new(format!(
            "{}:{}:{}-{}",
            descriptor.stream_id,
            descriptor.partition_id,
            descriptor.start_offset_inclusive,
            descriptor.end_offset_exclusive
        ))
        .map_err(ApiError::bad_request)?;
        let input_batch = RelationInputBatch {
            relation_id: header.relation_id.clone(),
            relation_version: header.relation_version.clone(),
            schema_fingerprint: header.schema_fingerprint.clone(),
            start_offset_inclusive: descriptor.start_offset_inclusive,
            end_offset_exclusive: descriptor.end_offset_exclusive,
            batches: envelope.record_batches().map_err(ApiError::bad_request)?,
        };
        let checkpoint = {
            let mut runtime = runtime
                .lock()
                .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?;
            let logical_epoch = next_standing_runtime_logical_epoch(
                runtime.as_ref(),
                descriptor.end_offset_exclusive,
            )?;
            runtime
                .apply_changes(logical_epoch, idempotency_key, vec![input_batch])
                .map_err(ApiError::bad_request)?;
            runtime.checkpoint().map_err(ApiError::bad_request)?
        };
        if let Err(error) = persist_standing_runtime_checkpoint(
            state,
            &active.spec.view_id,
            &checkpoint,
            ReplayCheckpoint::new(
                descriptor.stream_id.clone(),
                descriptor.partition_id,
                descriptor.end_offset_exclusive,
            ),
            owner,
        )
        .await
        {
            remove_standing_runtime(state, identity, &active.spec.view_id)?;
            return Err(error);
        }
    }

    Ok(())
}

async fn read_relation_catalog(
    state: &ApiState,
    relation_id: &str,
    relation_version: &str,
) -> Result<VelorixRelationCatalogV1, ApiError> {
    if let Some(meta_store) = &state.meta_store {
        meta_store
            .read_relation_catalog(relation_id, relation_version)
            .await
            .map_err(meta_error_to_api)
    } else {
        state
            .relation_registry()?
            .read(relation_id, relation_version)
            .await
            .map_err(ApiError::bad_request)
    }
}

async fn reserve_ingest_range(
    state: &ApiState,
    request: &IngestRowsRequest,
    catalog: &VelorixRelationCatalogV1,
    end_offset_exclusive: u64,
    envelope: &bytes::Bytes,
) -> Result<(), ApiError> {
    let meta_store = state
        .meta_store
        .as_ref()
        .ok_or_else(|| ApiError::internal("metadata store is not configured"))?;
    let header = IngestEnvelope::decode(envelope.clone())
        .map_err(ApiError::bad_request)?
        .header()
        .clone();
    let batch_key = ObjectKey::ingest_batch(
        &request.stream_id,
        request.partition_id,
        request.start_offset_inclusive,
        end_offset_exclusive,
    )
    .map_err(ApiError::bad_request)?;
    let outcome = meta_store
        .reserve_ingest_range(IngestRangeReservation {
            stream_id: request.stream_id.clone(),
            partition_id: request.partition_id,
            start_offset_inclusive: request.start_offset_inclusive,
            end_offset_exclusive,
            batch_key: batch_key.as_str().to_string(),
            payload_digest: header.payload_digest,
            relation_id: request.relation_id.clone(),
            relation_version: request.relation_version.clone(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            writer_epoch: 0,
        })
        .await
        .map_err(meta_error_to_api)?;

    match outcome {
        ReserveIngestRangeOutcome::Reserved | ReserveIngestRangeOutcome::Duplicate => Ok(()),
        ReserveIngestRangeOutcome::Conflict => Err(ApiError::conflict(format!(
            "ingest range conflict from metadata service for stream={} partition={} offsets={}-{}",
            request.stream_id,
            request.partition_id,
            request.start_offset_inclusive,
            end_offset_exclusive
        ))),
    }
}

async fn append_ingest_envelope(
    state: &ApiState,
    envelope: bytes::Bytes,
) -> Result<AppendValidatedEnvelopeOutcome, ApiError> {
    if state.meta_store.is_some() {
        state
            .ingest_writer
            .append_validated_envelope_after_external_admission(envelope)
            .await
            .map_err(ApiError::internal)
    } else {
        state
            .ingest_writer
            .append_catalog_validated_envelope(envelope)
            .await
            .map_err(ApiError::internal)
    }
}

const DEFAULT_TENANT_ID: &str = "default";

async fn create_query_policy(
    State(state): State<ApiState>,
    Json(request): Json<CreateQueryPolicyRequest>,
) -> Result<(StatusCode, Json<QueryPolicyResponse>), ApiError> {
    let record = state
        .query_policy_catalog()?
        .create_for_production_table_scan(
            DEFAULT_TENANT_ID,
            &request.query_policy_id,
            request.policy,
        )
        .await
        .map_err(query_policy_catalog_error_to_api)?;
    Ok((
        StatusCode::CREATED,
        Json(query_policy_response(record, Some("created"))),
    ))
}

async fn get_query_policy(
    State(state): State<ApiState>,
    AxumPath(query_policy_id): AxumPath<String>,
) -> Result<Json<QueryPolicyResponse>, ApiError> {
    let record = state
        .query_policy_catalog()?
        .get_for_production_table_scan(DEFAULT_TENANT_ID, &query_policy_id)
        .await
        .map_err(query_policy_catalog_error_to_api)?;
    Ok(Json(query_policy_response(record, None)))
}

fn query_policy_response(
    record: QueryPolicyCatalogRecord,
    outcome: Option<&str>,
) -> QueryPolicyResponse {
    QueryPolicyResponse {
        tenant_id: record.tenant_id,
        query_policy_id: record.query_policy_id,
        policy: record.policy,
        outcome: outcome.map(ToString::to_string),
    }
}

async fn query_rows(
    State(state): State<ApiState>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    if !state.enable_generic_query {
        return Err(generic_query_disabled_error());
    }
    let sql = normalize_product_query_sql(&request.sql);
    let batches = if state.meta_store.is_some() {
        let catalog =
            read_relation_catalog(&state, &request.relation_id, &request.relation_version).await?;
        query_production_recovered_materialized_view_with_catalog_and_policy_and_limiter(
            Arc::clone(&state.store),
            ObjectPath::from(state.state_path.clone()),
            catalog,
            &state.capabilities,
            &sql,
            QueryPolicy::default(),
            None,
        )
        .await
        .map_err(ApiError::bad_request)?
    } else {
        query_production_recovered_materialized_view_with_policy_and_limiter(
            Arc::clone(&state.store),
            ObjectPath::from(state.state_path.clone()),
            &request.relation_id,
            &request.relation_version,
            &state.capabilities,
            &sql,
            QueryPolicy::default(),
            None,
        )
        .await
        .map_err(ApiError::bad_request)?
    };
    Ok(Json(QueryResponse {
        rows: record_batches_to_json_rows(&batches)?,
        logical_epoch: None,
        next_page_token: None,
    }))
}

async fn query_view_rows_get(
    State(state): State<ApiState>,
    AxumPath(view_id): AxumPath<String>,
    Query(mut query): Query<BTreeMap<String, String>>,
) -> Result<Json<QueryResponse>, ApiError> {
    let page_request = extract_snapshot_page_request(&mut query)?;
    let parameters = query
        .into_iter()
        .map(|(name, value)| (name, Value::String(value)))
        .collect();
    query_view_rows_impl(state, view_id, None, parameters, page_request).await
}

fn extract_snapshot_page_request(
    query: &mut BTreeMap<String, String>,
) -> Result<SnapshotPageRequest, ApiError> {
    let committed_epoch = match query.remove("epoch") {
        Some(value) if value.trim().is_empty() => None,
        Some(value) => Some(value.parse::<u64>().map_err(|_| {
            ApiError::bad_request("pagination parameter `epoch` must be a non-negative integer")
        })?),
        None => None,
    };
    let page_token = query.remove("page_token").filter(|value| !value.is_empty());
    let max_rows = match query.remove("max_rows") {
        Some(value) if value.trim().is_empty() => None,
        Some(value) => {
            let parsed = value.parse::<usize>().map_err(|_| {
                ApiError::bad_request("pagination parameter `max_rows` must be a positive integer")
            })?;
            if parsed == 0 {
                return Err(ApiError::bad_request(
                    "pagination parameter `max_rows` must be a positive integer",
                ));
            }
            Some(parsed)
        }
        None => None,
    };
    Ok(SnapshotPageRequest {
        committed_epoch,
        page_token,
        max_rows,
    })
}

async fn query_view_api_get(
    State(state): State<ApiState>,
    AxumPath(api_path): AxumPath<String>,
    Query(mut query): Query<BTreeMap<String, String>>,
) -> Result<Json<QueryResponse>, ApiError> {
    let page_request = extract_snapshot_page_request(&mut query)?;
    let (active, mut parameters) = read_active_view_by_api_path(&state, &api_path).await?;
    ensure_view_execution_allowed(&state, &active)?;
    let api = active.api.clone().unwrap_or_default();
    for (name, value) in query
        .into_iter()
        .map(|(name, value)| (name, Value::String(value)))
    {
        if api
            .request
            .iter()
            .any(|field| field.field_name == name && field.field_in == "path")
        {
            return Err(ApiError::bad_request(format!(
                "parameter `{name}` must be supplied by the API path"
            )));
        }
        if let Some(existing) = parameters.insert(name.clone(), value.clone()) {
            if existing != value {
                return Err(ApiError::bad_request(format!(
                    "parameter `{name}` is provided by both path and query with different values"
                )));
            }
        }
    }
    query_active_view_rows_impl(state, active, None, parameters, page_request).await
}

async fn query_view_rows_post(
    State(state): State<ApiState>,
    AxumPath(view_id): AxumPath<String>,
    Json(request): Json<QueryViewRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    if request.sql.is_some() {
        return Err(ApiError::bad_request(
            "caller-supplied SQL is not allowed for view APIs",
        ));
    }
    let active = state
        .view_registry()?
        .read_active(&view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    ensure_view_execution_allowed(&state, &active)?;
    validate_direct_view_query_parameter_sources(&active, &request.parameters)?;
    query_active_view_rows_impl(
        state,
        active,
        request.sql,
        request.parameters,
        SnapshotPageRequest {
            committed_epoch: request.epoch,
            page_token: request.page_token,
            max_rows: request.max_rows,
        },
    )
    .await
}

async fn query_view_rows_impl(
    state: ApiState,
    view_id: String,
    request_sql: Option<String>,
    parameters: BTreeMap<String, Value>,
    page_request: SnapshotPageRequest,
) -> Result<Json<QueryResponse>, ApiError> {
    let active = state
        .view_registry()?
        .read_active(&view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    validate_direct_view_query_parameter_sources(&active, &parameters)?;
    query_active_view_rows_impl(state, active, request_sql, parameters, page_request).await
}

async fn read_active_view_by_api_path(
    state: &ApiState,
    api_path: &str,
) -> Result<(ActiveMaterializedView, BTreeMap<String, Value>), ApiError> {
    let normalized = normalize_api_path(api_path);
    let registry = state.view_registry()?;
    let indexes = registry
        .list_api_path_indexes()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let matched = indexes
        .into_iter()
        .find_map(|index| {
            match_api_path_pattern(&index.normalized_url_path, &normalized)
                .map(|parameters| (index.view_id, parameters))
        })
        .ok_or_else(|| ApiError::bad_request(format!("view API path `/{normalized}` not found")))?;
    let active = registry
        .read_active(&matched.0)
        .await
        .map_err(materialized_view_registry_error_to_api)?;

    Ok((active, matched.1))
}

async fn query_active_view_rows_impl(
    state: ApiState,
    active: ActiveMaterializedView,
    request_sql: Option<String>,
    parameters: BTreeMap<String, Value>,
    page_request: SnapshotPageRequest,
) -> Result<Json<QueryResponse>, ApiError> {
    ensure_view_execution_allowed(&state, &active)?;
    let view_id = active.spec.view_id.clone();
    let input = active
        .spec
        .input_relations
        .first()
        .ok_or_else(|| ApiError::bad_request("view has no input relation"))?;
    let api = active.api.clone().unwrap_or_default();
    let parameters = resolve_request_parameters(&api.request, &parameters)?;
    let query_policy = query_policy_for_view_api(&state, &api).await?;

    match active.execution_mode {
        MaterializedViewExecutionMode::LegacyRecoveredSql => {
            let bound_sql = if let Some(sql) = request_sql.as_deref() {
                render_view_sql_template(
                    &normalize_view_query_sql(sql, &view_id),
                    &api.request,
                    &parameters,
                )?
            } else if let Some(sql) = api.sql_template.as_deref() {
                render_view_sql_template(
                    &normalize_view_query_sql(sql, &view_id),
                    &api.request,
                    &parameters,
                )?
            } else {
                default_view_query_sql(&view_id)
            };
            let batches =
                query_production_recovered_materialized_view_table_with_bindings_and_policy_and_limiter(
                    Arc::clone(&state.store),
                    ObjectPath::from(state.state_path.clone()),
                    &input.relation_id,
                    &input.relation_version,
                    &view_id,
                    &state.capabilities,
                    &bound_sql.sql,
                    &bound_sql.bind_values,
                    query_policy.policy,
                    query_policy.limiter.clone(),
                )
                .await
                .map_err(ApiError::bad_request)?;
            let rows = record_batches_to_json_rows(&batches)?;
            let rows = match &api.response_schema {
                Some(response_schema) => materialized_rows_to_api_rows(&rows, response_schema)?,
                None => rows,
            };

            Ok(Json(QueryResponse {
                rows,
                logical_epoch: None,
                next_page_token: None,
            }))
        }
        MaterializedViewExecutionMode::StandingRuntime => {
            validate_standing_runtime_query_contract(
                &active.spec.view_id,
                request_sql.as_ref(),
                &api,
                &parameters,
                &page_request,
            )?;
            let (rows, logical_epoch, next_page_token) = if api.sql_template.is_some() {
                query_standing_runtime_rows_with_template(
                    &state,
                    &active,
                    &api,
                    &parameters,
                    page_request,
                    query_policy,
                )
                .await?
            } else {
                query_standing_runtime_rows(&state, &active, page_request).await?
            };
            let rows = match &api.response_schema {
                Some(response_schema) => materialized_rows_to_api_rows(&rows, response_schema)?,
                None => rows,
            };
            Ok(Json(QueryResponse {
                rows,
                logical_epoch: Some(logical_epoch),
                next_page_token,
            }))
        }
        MaterializedViewExecutionMode::FelderaCompilePending => Err(ApiError::service_unavailable(
            format!("feldera_compile_pending: view `{view_id}` is accepted but not deployed yet"),
        )),
    }
}

fn ensure_view_execution_allowed(
    state: &ApiState,
    active: &ActiveMaterializedView,
) -> Result<(), ApiError> {
    if active.execution_mode == MaterializedViewExecutionMode::FelderaCompilePending {
        return Err(ApiError::service_unavailable(format!(
            "feldera_compile_pending: view `{}` is accepted but not deployed yet",
            active.spec.view_id
        )));
    }
    if active.execution_mode == MaterializedViewExecutionMode::LegacyRecoveredSql
        && !state.allow_legacy_recovered_sql_views
    {
        return Err(legacy_recovered_sql_views_disabled_error());
    }
    Ok(())
}

async fn query_standing_runtime_rows_with_template(
    state: &ApiState,
    active: &ActiveMaterializedView,
    api: &MaterializedViewApiMetadata,
    parameters: &BTreeMap<String, Value>,
    page_request: SnapshotPageRequest,
    query_policy: ViewQueryPolicy,
) -> Result<(Vec<Value>, u64, Option<String>), ApiError> {
    let sql_template = api.sql_template.as_deref().ok_or_else(|| {
        ApiError::conflict(format!(
            "standing runtime view `{}` has request parameters but no sql_template",
            active.spec.view_id
        ))
    })?;
    let requested_epoch = page_request.committed_epoch;
    let bound_sql = render_view_sql_template(
        &normalize_view_query_sql(sql_template, &active.spec.view_id),
        &api.request,
        parameters,
    )?;
    let page = standing_runtime_page(state, active, page_request).await?;
    validate_standing_runtime_template_page(active, &page, requested_epoch)?;
    let batches = query_record_batches_table_with_bindings_and_policy_and_limiter(
        &active.spec.view_id,
        page.batches,
        &bound_sql.sql,
        &bound_sql.bind_values,
        query_policy.policy,
        query_policy.limiter,
    )
    .await
    .map_err(ApiError::bad_request)?;

    Ok((
        record_batches_to_json_rows(&batches)?,
        page.logical_epoch,
        None,
    ))
}

fn validate_standing_runtime_template_page(
    active: &ActiveMaterializedView,
    page: &MaterializedViewPage,
    requested_epoch: Option<u64>,
) -> Result<(), ApiError> {
    let artifact = active.artifact.as_ref().ok_or_else(|| {
        ApiError::conflict(format!(
            "standing runtime view `{}` is missing artifact binding",
            active.spec.view_id
        ))
    })?;
    let identity = artifact.standing_program_identity.as_ref().ok_or_else(|| {
        ApiError::conflict(format!(
            "artifact-backed view `{}` is missing standing runtime identity",
            active.spec.view_id
        ))
    })?;
    let expected_view = ScopedViewId {
        tenant_id: identity.tenant_id.clone(),
        program_id: identity.program_id.clone(),
        view_id: active.spec.view_id.clone(),
    };
    if page.view != expected_view {
        return Err(ApiError::conflict(format!(
            "standing runtime view `{}` returned a page for a different scoped view",
            active.spec.view_id
        )));
    }
    if let Some(epoch) = requested_epoch {
        if page.logical_epoch != epoch {
            return Err(ApiError::conflict(format!(
                "standing runtime view `{}` returned epoch {} for requested epoch {epoch}",
                active.spec.view_id, page.logical_epoch
            )));
        }
    }
    if page.next_page_token.is_some() {
        return Err(ApiError::conflict(format!(
            "full snapshot is unavailable for templated standing runtime view `{}`",
            active.spec.view_id
        )));
    }
    let output_schema = active
        .spec
        .output_relations
        .iter()
        .find(|schema| schema.relation_id == active.spec.view_id)
        .ok_or_else(|| {
            ApiError::conflict(format!(
                "standing runtime view `{}` has no matching output schema",
                active.spec.view_id
            ))
        })?;
    if page.schema_fingerprint != output_schema.schema_fingerprint {
        return Err(ApiError::conflict(format!(
            "standing runtime view `{}` returned schema fingerprint `{}` but active schema fingerprint is `{}`",
            active.spec.view_id, page.schema_fingerprint, output_schema.schema_fingerprint
        )));
    }
    let expected_arrow_schema = arrow_schema_from_feldera_relation_schema(output_schema)?;
    if page.batches.is_empty() {
        return Err(ApiError::conflict(format!(
            "standing runtime view `{}` returned no record batches",
            active.spec.view_id
        )));
    }
    for batch in &page.batches {
        if batch.schema().as_ref() != expected_arrow_schema.as_ref() {
            return Err(ApiError::conflict(format!(
                "standing runtime view `{}` returned batches that do not match the active output schema",
                active.spec.view_id
            )));
        }
    }

    Ok(())
}

async fn query_standing_runtime_rows(
    state: &ApiState,
    active: &ActiveMaterializedView,
    page_request: SnapshotPageRequest,
) -> Result<(Vec<Value>, u64, Option<String>), ApiError> {
    let page = standing_runtime_page(state, active, page_request).await?;

    Ok((
        record_batches_to_json_rows(&page.batches)?,
        page.logical_epoch,
        page.next_page_token,
    ))
}

async fn standing_runtime_page(
    state: &ApiState,
    active: &ActiveMaterializedView,
    page_request: SnapshotPageRequest,
) -> Result<MaterializedViewPage, ApiError> {
    state.validate_standing_runtime_fencing_or_evict().await?;
    let artifact = active.artifact.as_ref().ok_or_else(|| {
        ApiError::conflict(format!(
            "standing runtime view `{}` is missing artifact binding",
            active.spec.view_id
        ))
    })?;
    let identity = artifact.standing_program_identity.as_ref().ok_or_else(|| {
        ApiError::conflict(format!(
            "artifact-backed view `{}` is missing standing runtime identity",
            active.spec.view_id
        ))
    })?;
    if state
        .standing_runtime(identity, &active.spec.view_id)?
        .is_none()
    {
        let _ = ensure_standing_runtime_for_artifact(
            state,
            &active.spec.view_id,
            artifact,
            &active.spec.input_relations,
            &active.spec.output_relations,
        )
        .await?;
    }
    let operation_lock = state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
    let _operation_guard = operation_lock.lock().await;
    let runtime = state
        .standing_runtime(identity, &active.spec.view_id)?
        .ok_or_else(|| {
            ApiError::service_unavailable(format!(
                "standing runtime is unavailable for artifact-backed view `{}`",
                active.spec.view_id
            ))
        })?;
    let page = runtime
        .lock()
        .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?
        .materialized_view_page(
            ScopedViewId {
                tenant_id: identity.tenant_id.clone(),
                program_id: identity.program_id.clone(),
                view_id: active.spec.view_id.clone(),
            },
            page_request,
        )
        .map_err(ApiError::bad_request)?;
    state
        .validate_standing_runtime_committed_for_query(
            identity,
            &active.spec.view_id,
            page.logical_epoch,
        )
        .await?;

    Ok(page)
}

async fn openapi_json(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    let views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let mut paths = serde_json::Map::new();

    paths.insert(
        "/v1/relations".to_string(),
        json!({
            "post": {
                "summary": "Create a relation catalog",
                "responses": { "201": { "description": "Relation created" } }
            }
        }),
    );
    paths.insert(
        "/v1/ingest".to_string(),
        json!({
            "post": {
                "summary": "Ingest rows into a relation",
                "responses": { "201": { "description": "Rows ingested" } }
            }
        }),
    );
    paths.insert(
        "/v1/views".to_string(),
        json!({
            "get": {
                "summary": "List view APIs",
                "responses": { "200": { "description": "View catalog" } }
            },
            "post": {
                "summary": "Create a view API",
                "responses": { "201": { "description": "View created" } }
            }
        }),
    );

    for view in views {
        if !state.allow_legacy_recovered_sql_views
            && view.execution_mode == MaterializedViewExecutionMode::LegacyRecoveredSql
        {
            continue;
        }
        let response = active_view_response(&state, &view, None)?;
        if !response.query_enabled {
            continue;
        }
        paths.insert(
            openapi_path_from_query_endpoint(&response.query_endpoint),
            json!({
                "get": {
                    "summary": response.description.clone().unwrap_or_else(|| {
                        format!("Query {}", response.view_id)
                    }),
                    "x-velorix-view-id": response.view_id,
                    "x-velorix-url-path": response.url_path,
                    "x-velorix-input-relation-id": response.input_relation_id,
                    "x-velorix-input-relation-version": response.input_relation_version,
                    "x-velorix-spec-hash": response.spec_hash,
                    "x-velorix-request": response.request.clone(),
                    "x-velorix-response-schema": response.response_schema.clone(),
                    "x-velorix-sql-template": response.sql_template.clone(),
                    "x-velorix-query-policy-id": response.query_policy_id.clone(),
                    "parameters": openapi_view_query_parameters(
                        &response.request,
                        !(response.execution_mode == MaterializedViewExecutionMode::StandingRuntime
                            && response.sql_template.is_some())
                    ),
                    "responses": {
                        "200": {
                            "description": "View query result rows",
                            "content": {
                                "application/json": {
                                    "schema": openapi_query_response_schema(
                                        response.response_schema.as_ref()
                                    )
                                }
                            }
                        }
                    }
                }
            }),
        );
    }

    Ok(Json(json!({
        "openapi": "3.0.3",
        "info": {
            "title": "Velorix View APIs",
            "version": "0.1.0"
        },
        "paths": Value::Object(paths)
    })))
}

fn normalize_product_query_sql(sql: &str) -> String {
    match sql.trim().to_ascii_lowercase().as_str() {
        "select key, value, weight from input" => {
            "select key_json as key, value_json as value, weight from input".to_string()
        }
        "select key, value, weight from input order by key" => {
            "select key_json as key, value_json as value, weight from input order by key_json"
                .to_string()
        }
        _ => sql.to_string(),
    }
}

fn normalize_view_query_sql(sql: &str, view_id: &str) -> String {
    let normalized = sql.trim().to_ascii_lowercase();
    let compact_view_id = view_id.to_ascii_lowercase();
    if normalized == format!("select key, value, weight from {compact_view_id}") {
        format!("select key_json as key, value_json as value, weight from {view_id}")
    } else if normalized == format!("select key, value, weight from {compact_view_id} order by key")
    {
        format!(
            "select key_json as key, value_json as value, weight from {view_id} order by key_json"
        )
    } else {
        sql.to_string()
    }
}

fn sql_references_table(sql: &str, table_name: &str) -> bool {
    let tokens = sql
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    tokens.windows(2).any(|window| {
        matches!(window[0].to_ascii_lowercase().as_str(), "from" | "join")
            && window[1].eq_ignore_ascii_case(table_name)
    })
}

fn arrow_schema_from_feldera_relation_schema(
    schema: &RelationSchema,
) -> Result<Arc<Schema>, ApiError> {
    let fields = schema
        .columns
        .iter()
        .map(|column| {
            Ok(Field::new(
                column.name.as_str(),
                arrow_data_type_from_sql_data_type(&column.data_type)?,
                column.nullable,
            ))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn arrow_data_type_from_sql_data_type(data_type: &SqlDataType) -> Result<DataType, ApiError> {
    match data_type {
        SqlDataType::Bool => Ok(DataType::Boolean),
        SqlDataType::Int64 => Ok(DataType::Int64),
        SqlDataType::Float64 => Ok(DataType::Float64),
        SqlDataType::Decimal { precision, scale } => Ok(DataType::Decimal128(
            (*precision).try_into().map_err(|_| {
                ApiError::bad_request("decimal precision does not fit Arrow Decimal128")
            })?,
            (*scale).try_into().map_err(|_| {
                ApiError::bad_request("decimal scale does not fit Arrow Decimal128")
            })?,
        )),
        SqlDataType::Utf8 | SqlDataType::Json => Ok(DataType::Utf8),
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Timestamp { .. } => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            None,
        )),
    }
}

fn default_view_query_sql(view_id: &str) -> BoundViewSql {
    BoundViewSql {
        sql: format!("select key_json, value_json, weight from {view_id} order by key_json"),
        bind_values: Vec::new(),
    }
}

fn normalize_api_path(path: &str) -> String {
    path.trim_matches('/').to_string()
}

fn match_api_path_pattern(
    normalized_pattern: &str,
    normalized_path: &str,
) -> Option<BTreeMap<String, Value>> {
    let pattern_segments = normalized_pattern.split('/').collect::<Vec<_>>();
    let path_segments = normalized_path.split('/').collect::<Vec<_>>();
    if pattern_segments.len() != path_segments.len() {
        return None;
    }

    let mut parameters = BTreeMap::new();
    for (pattern, value) in pattern_segments.iter().zip(path_segments) {
        if let Some(name) = pattern.strip_prefix(':') {
            if name.is_empty() || value.is_empty() {
                return None;
            }
            parameters.insert(name.to_string(), Value::String(value.to_string()));
        } else if *pattern != value {
            return None;
        }
    }

    Some(parameters)
}

fn openapi_path_from_query_endpoint(endpoint: &str) -> String {
    endpoint
        .split('/')
        .map(|segment| {
            segment
                .strip_prefix(':')
                .map(|name| format!("{{{name}}}"))
                .unwrap_or_else(|| segment.to_string())
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn validate_view_api_metadata(api: &MaterializedViewApiMetadata) -> Result<(), ApiError> {
    let mut field_names = BTreeSet::new();
    if let Some(url_path) = &api.url_path {
        validate_url_path_request_contract(url_path, &api.request)?;
    }
    for field in &api.request {
        if !field_names.insert(field.field_name.as_str()) {
            return Err(ApiError::bad_request(format!(
                "request field `{}` is declared more than once",
                field.field_name
            )));
        }
        match field.field_in.as_str() {
            "query" | "path" => {}
            "header" => {
                return Err(ApiError::bad_request(
                    "header request fields are not enabled for view APIs yet",
                ));
            }
            "body" => {
                return Err(ApiError::bad_request(
                    "body request fields are not enabled for GET view APIs",
                ));
            }
            other => {
                return Err(ApiError::bad_request(format!(
                    "request field `{}` has unsupported fieldIn `{other}`",
                    field.field_name
                )));
            }
        }
        validate_request_field_contract(field)?;
    }
    if let Some(sql_template) = &api.sql_template {
        validate_sql_template_contract(sql_template, &api.request)?;
        validate_sql_template_parameter_coverage(sql_template, api)?;
    }
    Ok(())
}

async fn validate_query_policy_reference(
    state: &ApiState,
    api: &MaterializedViewApiMetadata,
) -> Result<(), ApiError> {
    if let Some(query_policy_id) = &api.query_policy_id {
        state
            .query_policy_catalog()?
            .get_for_production_table_scan(DEFAULT_TENANT_ID, query_policy_id)
            .await
            .map_err(query_policy_catalog_error_to_api)?;
    }
    Ok(())
}

struct ViewQueryPolicy {
    policy: QueryPolicy,
    limiter: Option<QueryExecutionLimiter>,
}

async fn query_policy_for_view_api(
    state: &ApiState,
    api: &MaterializedViewApiMetadata,
) -> Result<ViewQueryPolicy, ApiError> {
    if let Some(query_policy_id) = &api.query_policy_id {
        let record = state
            .query_policy_catalog()?
            .get_for_production_table_scan(DEFAULT_TENANT_ID, query_policy_id)
            .await
            .map_err(query_policy_catalog_error_to_api)?;
        let limiter = state.query_limiter_for_policy(query_policy_id, record.policy)?;
        Ok(ViewQueryPolicy {
            policy: record.policy,
            limiter,
        })
    } else {
        Ok(ViewQueryPolicy {
            policy: QueryPolicy::default(),
            limiter: None,
        })
    }
}

async fn validate_standing_runtime_create_api_metadata(
    view_id: &str,
    api: &MaterializedViewApiMetadata,
    output_schemas: &[RelationSchema],
) -> Result<(), ApiError> {
    let Some(sql_template) = api.sql_template.as_deref() else {
        if !api.request.is_empty() {
            return Err(ApiError::bad_request(format!(
                "standing runtime view `{view_id}` has request parameters but no sql_template"
            )));
        }
        return Ok(());
    };
    if !sql_references_table(sql_template, view_id) {
        return Err(ApiError::bad_request(format!(
            "standing runtime view `{view_id}` sql_template must reference table `{view_id}`"
        )));
    }
    let output_schema = output_schemas
        .iter()
        .find(|schema| schema.relation_id == view_id)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "standing runtime view `{view_id}` artifact metadata has no matching output schema"
            ))
        })?;
    let table_schema = arrow_schema_from_feldera_relation_schema(output_schema)?;
    let bound_sql = render_view_sql_template_for_validation(sql_template, &api.request)?;
    validate_record_batch_table_query_with_bindings_and_policy(
        view_id,
        table_schema,
        &normalize_view_query_sql(&bound_sql.sql, view_id),
        &bound_sql.bind_values,
        QueryPolicy::default(),
    )
    .await
    .map_err(ApiError::bad_request)?;
    Ok(())
}

fn validate_standing_runtime_query_contract(
    view_id: &str,
    request_sql: Option<&String>,
    api: &MaterializedViewApiMetadata,
    parameters: &BTreeMap<String, Value>,
    page_request: &SnapshotPageRequest,
) -> Result<(), ApiError> {
    if request_sql.is_some() {
        return Err(ApiError::bad_request(format!(
            "caller-supplied SQL is not supported for standing runtime view `{view_id}`"
        )));
    }
    if api.sql_template.is_none() && (!api.request.is_empty() || !parameters.is_empty()) {
        return Err(ApiError::conflict(format!(
            "standing runtime view `{view_id}` has request parameters but no sql_template"
        )));
    }
    if api.sql_template.is_some()
        && (page_request.page_token.is_some() || page_request.max_rows.is_some())
    {
        return Err(ApiError::bad_request(format!(
            "cursor pagination is not supported for templated standing runtime view `{view_id}`"
        )));
    }
    Ok(())
}

fn validate_direct_view_query_parameter_sources(
    active: &ActiveMaterializedView,
    parameters: &BTreeMap<String, Value>,
) -> Result<(), ApiError> {
    let api = active.api.clone().unwrap_or_default();
    for field in &api.request {
        if field.field_in == "path"
            && (parameters.contains_key(&field.field_name)
                || request_field_has_validator(field, "required"))
        {
            return Err(ApiError::bad_request(format!(
                "parameter `{}` must be supplied by the promoted API path",
                field.field_name
            )));
        }
    }
    Ok(())
}

fn validate_url_path_request_contract(
    url_path: &str,
    request: &[MaterializedViewRequestFieldSpec],
) -> Result<(), ApiError> {
    let normalized = normalize_api_path(url_path);
    let path_params = normalized
        .split('/')
        .filter_map(|segment| segment.strip_prefix(':'))
        .collect::<Vec<_>>();
    let mut unique_path_params = BTreeSet::new();
    for parameter in path_params {
        if parameter.is_empty() {
            return Err(ApiError::bad_request(
                "urlPath contains an empty path parameter",
            ));
        }
        if !unique_path_params.insert(parameter) {
            return Err(ApiError::bad_request(format!(
                "urlPath path parameter `{parameter}` is declared more than once"
            )));
        }
        let declared = request
            .iter()
            .any(|field| field.field_name == parameter && field.field_in == "path");
        if !declared {
            return Err(ApiError::bad_request(format!(
                "urlPath path parameter `{parameter}` must be declared with fieldIn `path`"
            )));
        }
    }
    for field in request.iter().filter(|field| field.field_in == "path") {
        if !unique_path_params.contains(field.field_name.as_str()) {
            return Err(ApiError::bad_request(format!(
                "request field `{}` is declared as path but is not present in urlPath",
                field.field_name
            )));
        }
    }
    Ok(())
}

fn validate_request_field_contract(
    field: &MaterializedViewRequestFieldSpec,
) -> Result<(), ApiError> {
    if matches!(
        field.field_name.as_str(),
        "epoch" | "page_token" | "max_rows"
    ) {
        return Err(ApiError::bad_request(format!(
            "request field `{}` is reserved for pagination",
            field.field_name
        )));
    }
    match field.r#type.as_str() {
        "string" | "int64" | "integer" | "float64" | "number" | "bool" | "boolean" | "json" => {}
        other => {
            return Err(ApiError::bad_request(format!(
                "request field `{}` declares unsupported type `{other}`",
                field.field_name
            )));
        }
    }
    if field.field_in == "path" && field.default_value.is_some() {
        return Err(ApiError::bad_request(format!(
            "request field `{}` cannot declare defaultValue for fieldIn `path`",
            field.field_name
        )));
    }
    for validator in &field.validators {
        validate_filter_contract(&field.field_name, &validator_filter_name(validator))?;
    }
    if let Some(default_value) = &field.default_value {
        validate_request_field_type(&field.field_name, field, default_value)?;
        for validator in &field.validators {
            apply_template_filter(
                &field.field_name,
                default_value,
                &validator_filter_name(validator),
            )?;
        }
    }
    Ok(())
}

fn validate_sql_template_contract(
    sql_template: &str,
    request: &[MaterializedViewRequestFieldSpec],
) -> Result<(), ApiError> {
    let fields = request
        .iter()
        .map(|field| (field.field_name.as_str(), field))
        .collect::<BTreeMap<_, _>>();
    let mut rest = sql_template;

    while let Some(start) = rest.find("{{") {
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find("}}") else {
            return Err(ApiError::bad_request(
                "sql template contains an unclosed parameter placeholder",
            ));
        };
        let expression = after_start[..end].trim();
        let (name, filters) = parse_template_parameter_expression(expression)?;
        let Some(field) = fields.get(name) else {
            return Err(ApiError::bad_request(format!(
                "sql template references undeclared parameter `{name}`"
            )));
        };
        for filter in filters {
            validate_filter_contract(name, &filter)?;
        }
        validate_request_field_contract(field)?;
        rest = &after_start[end + 2..];
    }

    if rest.contains("}}") {
        return Err(ApiError::bad_request(
            "sql template contains an unopened parameter placeholder",
        ));
    }
    Ok(())
}

fn validate_sql_template_parameter_coverage(
    sql_template: &str,
    api: &MaterializedViewApiMetadata,
) -> Result<(), ApiError> {
    let parameters = sql_template_parameter_names(sql_template)?;
    if let Some(url_path) = &api.url_path {
        for parameter in url_path
            .trim_matches('/')
            .split('/')
            .filter_map(|segment| segment.strip_prefix(':'))
        {
            if !parameters.contains(parameter) {
                return Err(ApiError::bad_request(format!(
                    "sql template must reference path parameter `{parameter}`"
                )));
            }
        }
    }
    for field in &api.request {
        if request_field_has_validator(field, "required") && !parameters.contains(&field.field_name)
        {
            return Err(ApiError::bad_request(format!(
                "sql template must reference required parameter `{}`",
                field.field_name
            )));
        }
    }
    Ok(())
}

fn sql_template_parameter_names(sql_template: &str) -> Result<BTreeSet<String>, ApiError> {
    let mut parameters = BTreeSet::new();
    let mut rest = sql_template;

    while let Some(start) = rest.find("{{") {
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find("}}") else {
            return Err(ApiError::bad_request(
                "sql template contains an unclosed parameter placeholder",
            ));
        };
        let expression = after_start[..end].trim();
        let (name, _) = parse_template_parameter_expression(expression)?;
        parameters.insert(name.to_string());
        rest = &after_start[end + 2..];
    }

    if rest.contains("}}") {
        return Err(ApiError::bad_request(
            "query template contains an unopened parameter placeholder",
        ));
    }
    Ok(parameters)
}

fn validate_filter_contract(name: &str, filter: &str) -> Result<(), ApiError> {
    match filter_name(filter) {
        "is_required" | "is_string" | "is_boolean" | "to_json" => {
            if !parse_filter_args(filter)?.is_empty() {
                return Err(ApiError::bad_request(format!(
                    "parameter `{name}` uses unsupported arguments for `{}`",
                    filter_name(filter)
                )));
            }
        }
        "is_integer" => {
            for (arg, raw) in parse_filter_args(filter)? {
                match arg.as_str() {
                    "min" | "max" | "greater" | "less" => {
                        raw.parse::<i64>().map_err(|_| {
                            ApiError::bad_request(format!(
                                "parameter `{name}` has invalid is_integer argument `{arg}`"
                            ))
                        })?;
                    }
                    _ => {
                        return Err(ApiError::bad_request(format!(
                            "parameter `{name}` uses unsupported is_integer argument `{arg}`"
                        )));
                    }
                }
            }
        }
        "is_number" => {
            for (arg, raw) in parse_filter_args(filter)? {
                match arg.as_str() {
                    "min" | "max" | "greater" | "less" => {
                        raw.parse::<f64>().map_err(|_| {
                            ApiError::bad_request(format!(
                                "parameter `{name}` has invalid is_number argument `{arg}`"
                            ))
                        })?;
                    }
                    _ => {
                        return Err(ApiError::bad_request(format!(
                            "parameter `{name}` uses unsupported is_number argument `{arg}`"
                        )));
                    }
                }
            }
        }
        other => {
            return Err(ApiError::bad_request(format!(
                "parameter `{name}` uses unsupported SQL template filter `{other}`"
            )));
        }
    }
    Ok(())
}

fn validate_request_parameters(
    fields: &[MaterializedViewRequestFieldSpec],
    values: &BTreeMap<String, Value>,
) -> Result<(), ApiError> {
    let specs = fields
        .iter()
        .map(|field| (field.field_name.as_str(), field))
        .collect::<BTreeMap<_, _>>();

    for name in values.keys() {
        if !specs.contains_key(name.as_str()) {
            return Err(ApiError::bad_request(format!(
                "parameter `{name}` is not declared by view API"
            )));
        }
    }

    for (name, spec) in specs {
        let Some(value) = values.get(name) else {
            if request_field_has_validator(spec, "required") {
                return Err(parameter_filter_error(name, "is_required"));
            }
            continue;
        };
        if value_is_empty(value) && request_field_has_validator(spec, "required") {
            return Err(parameter_filter_error(name, "is_required"));
        }
        validate_request_field_type(name, spec, value)?;
        for validator in &spec.validators {
            apply_template_filter(name, value, &validator_filter_name(validator))?;
        }
    }

    Ok(())
}

fn resolve_request_parameters(
    fields: &[MaterializedViewRequestFieldSpec],
    values: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>, ApiError> {
    let mut resolved = values.clone();
    for field in fields {
        if !resolved.contains_key(&field.field_name) {
            if let Some(default_value) = &field.default_value {
                resolved.insert(field.field_name.clone(), default_value.clone());
            }
        }
    }
    validate_request_parameters(fields, &resolved)?;
    Ok(resolved)
}

fn validate_request_field_type(
    name: &str,
    spec: &MaterializedViewRequestFieldSpec,
    value: &Value,
) -> Result<(), ApiError> {
    match spec.r#type.as_str() {
        "string" => value
            .as_str()
            .map(|_| ())
            .ok_or_else(|| parameter_filter_error(name, "is_string")),
        "int64" | "integer" => value
            .as_i64()
            .or_else(|| value.as_str().and_then(|value| value.parse::<i64>().ok()))
            .map(|_| ())
            .ok_or_else(|| parameter_filter_error(name, "is_integer")),
        "float64" | "number" => value
            .as_f64()
            .or_else(|| value.as_str().and_then(|value| value.parse::<f64>().ok()))
            .filter(|value| value.is_finite())
            .map(|_| ())
            .ok_or_else(|| parameter_filter_error(name, "is_number")),
        "bool" | "boolean" => value
            .as_bool()
            .or_else(|| value.as_str().and_then(parse_bool))
            .map(|_| ())
            .ok_or_else(|| parameter_filter_error(name, "is_boolean")),
        "json" => Ok(()),
        other => Err(ApiError::bad_request(format!(
            "parameter `{name}` declares unsupported type `{other}`"
        ))),
    }
}

#[derive(Clone, Debug, PartialEq)]
struct BoundViewSql {
    sql: String,
    bind_values: Vec<QueryBindValue>,
}

fn render_view_sql_template(
    template: &str,
    fields: &[MaterializedViewRequestFieldSpec],
    values: &BTreeMap<String, Value>,
) -> Result<BoundViewSql, ApiError> {
    let specs = fields
        .iter()
        .map(|field| (field.field_name.as_str(), field))
        .collect::<BTreeMap<_, _>>();
    let mut output = String::with_capacity(template.len());
    let mut bind_values = Vec::new();
    let mut rest = template;

    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find("}}") else {
            return Err(ApiError::bad_request(
                "query template contains an unclosed parameter placeholder",
            ));
        };
        let expression = after_start[..end].trim();
        let (name, filters) = parse_template_parameter_expression(expression)?;
        let spec = specs.get(name).ok_or_else(|| {
            ApiError::bad_request(format!(
                "sql template references undeclared parameter `{name}`"
            ))
        })?;
        match bind_value_for_template_parameter(name, spec, values.get(name), &filters)? {
            Some(value) => {
                bind_values.push(value);
                output.push_str(&format!("${}", bind_values.len()));
            }
            None => output.push_str("NULL"),
        }
        rest = &after_start[end + 2..];
    }

    if rest.contains("}}") {
        return Err(ApiError::bad_request(
            "query template contains an unopened parameter placeholder",
        ));
    }
    output.push_str(rest);

    Ok(BoundViewSql {
        sql: output,
        bind_values,
    })
}

fn render_view_sql_template_for_validation(
    template: &str,
    fields: &[MaterializedViewRequestFieldSpec],
) -> Result<BoundViewSql, ApiError> {
    let specs = fields
        .iter()
        .map(|field| (field.field_name.as_str(), field))
        .collect::<BTreeMap<_, _>>();
    let mut output = String::with_capacity(template.len());
    let mut bind_values = Vec::new();
    let mut rest = template;

    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find("}}") else {
            return Err(ApiError::bad_request(
                "query template contains an unclosed parameter placeholder",
            ));
        };
        let expression = after_start[..end].trim();
        let (name, filters) = parse_template_parameter_expression(expression)?;
        let spec = specs.get(name).ok_or_else(|| {
            ApiError::bad_request(format!(
                "sql template references undeclared parameter `{name}`"
            ))
        })?;
        for filter in filters {
            validate_filter_contract(name, &filter)?;
        }
        bind_values.push(dummy_bind_value_for_field(spec)?);
        output.push_str(&format!("${}", bind_values.len()));
        rest = &after_start[end + 2..];
    }

    if rest.contains("}}") {
        return Err(ApiError::bad_request(
            "query template contains an unopened parameter placeholder",
        ));
    }
    output.push_str(rest);

    Ok(BoundViewSql {
        sql: output,
        bind_values,
    })
}

fn dummy_bind_value_for_field(
    field: &MaterializedViewRequestFieldSpec,
) -> Result<QueryBindValue, ApiError> {
    match field.r#type.as_str() {
        "string" => Ok(QueryBindValue::Utf8("validation".to_string())),
        "int64" | "integer" => Ok(QueryBindValue::Int64(1_000_000)),
        "float64" | "number" => Ok(QueryBindValue::Float64(1_000_000.0)),
        "bool" | "boolean" => Ok(QueryBindValue::Boolean(true)),
        "json" => Ok(QueryBindValue::Utf8("null".to_string())),
        other => Err(ApiError::bad_request(format!(
            "request field `{}` declares unsupported type `{other}`",
            field.field_name
        ))),
    }
}

fn parse_template_parameter_expression(expression: &str) -> Result<(&str, Vec<String>), ApiError> {
    let mut parts = expression.split('|').map(str::trim);
    let parameter = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| {
            ApiError::bad_request("sql template contains an empty parameter expression")
        })?;
    let name = parameter.strip_prefix("context.params.").ok_or_else(|| {
        ApiError::bad_request(format!(
            "sql template parameter `{parameter}` must use context.params.<name>"
        ))
    })?;
    if name.is_empty() {
        return Err(ApiError::bad_request(
            "sql template contains an empty context.params parameter name",
        ));
    }
    Ok((name, parts.map(ToString::to_string).collect()))
}

fn bind_value_for_template_parameter(
    name: &str,
    spec: &MaterializedViewRequestFieldSpec,
    value: Option<&Value>,
    filters: &[String],
) -> Result<Option<QueryBindValue>, ApiError> {
    let required = request_field_has_validator(spec, "required")
        || filters
            .iter()
            .any(|filter| filter_name(filter) == "is_required");
    let Some(value) = value else {
        return if required {
            Err(parameter_filter_error(name, "is_required"))
        } else {
            Ok(None)
        };
    };
    if value_is_empty(value) && required {
        return Err(parameter_filter_error(name, "is_required"));
    }
    for filter in filters {
        apply_template_filter(name, value, filter)?;
    }

    if filters
        .iter()
        .any(|filter| filter_name(filter) == "to_json")
    {
        return serde_json::to_string(value)
            .map(|value| Some(QueryBindValue::Utf8(value)))
            .map_err(ApiError::bad_request);
    }

    bind_value_for_request_field(name, spec, value).map(Some)
}

fn apply_template_filter(name: &str, value: &Value, filter: &str) -> Result<(), ApiError> {
    match filter_name(filter) {
        "is_required" => {
            if value_is_empty(value) {
                Err(parameter_filter_error(name, "is_required"))
            } else {
                Ok(())
            }
        }
        "is_string" => value
            .as_str()
            .map(|_| ())
            .ok_or_else(|| parameter_filter_error(name, "is_string")),
        "is_integer" => value
            .as_i64()
            .or_else(|| value.as_str().and_then(|value| value.parse::<i64>().ok()))
            .ok_or_else(|| parameter_filter_error(name, "is_integer"))
            .and_then(|value| validate_integer_filter_args(name, value, filter)),
        "is_number" => value
            .as_f64()
            .or_else(|| value.as_str().and_then(|value| value.parse::<f64>().ok()))
            .filter(|value| value.is_finite())
            .ok_or_else(|| parameter_filter_error(name, "is_number"))
            .and_then(|value| validate_number_filter_args(name, value, filter)),
        "is_boolean" => value
            .as_bool()
            .or_else(|| value.as_str().and_then(parse_bool))
            .map(|_| ())
            .ok_or_else(|| parameter_filter_error(name, "is_boolean")),
        "to_json" => Ok(()),
        other => Err(ApiError::bad_request(format!(
            "parameter `{name}` uses unsupported SQL template filter `{other}`"
        ))),
    }
}

fn validator_filter_name(validator: &str) -> String {
    match filter_name(validator) {
        "required" => validator.replacen("required", "is_required", 1),
        "string" => validator.replacen("string", "is_string", 1),
        "integer" => validator.replacen("integer", "is_integer", 1),
        "number" => validator.replacen("number", "is_number", 1),
        "boolean" => validator.replacen("boolean", "is_boolean", 1),
        _ => validator.to_string(),
    }
}

fn validate_integer_filter_args(name: &str, value: i64, filter: &str) -> Result<(), ApiError> {
    let args = parse_filter_args(filter)?;
    for (arg, raw) in args {
        let limit = raw.parse::<i64>().map_err(|_| {
            ApiError::bad_request(format!(
                "parameter `{name}` has invalid is_integer argument `{arg}`"
            ))
        })?;
        match arg.as_str() {
            "min" if value < limit => return Err(parameter_filter_error(name, "is_integer")),
            "max" if value > limit => return Err(parameter_filter_error(name, "is_integer")),
            "greater" if value <= limit => return Err(parameter_filter_error(name, "is_integer")),
            "less" if value >= limit => return Err(parameter_filter_error(name, "is_integer")),
            "min" | "max" | "greater" | "less" => {}
            _ => {
                return Err(ApiError::bad_request(format!(
                    "parameter `{name}` uses unsupported is_integer argument `{arg}`"
                )));
            }
        }
    }
    Ok(())
}

fn validate_number_filter_args(name: &str, value: f64, filter: &str) -> Result<(), ApiError> {
    let args = parse_filter_args(filter)?;
    for (arg, raw) in args {
        let limit = raw.parse::<f64>().map_err(|_| {
            ApiError::bad_request(format!(
                "parameter `{name}` has invalid is_number argument `{arg}`"
            ))
        })?;
        match arg.as_str() {
            "min" if value < limit => return Err(parameter_filter_error(name, "is_number")),
            "max" if value > limit => return Err(parameter_filter_error(name, "is_number")),
            "greater" if value <= limit => return Err(parameter_filter_error(name, "is_number")),
            "less" if value >= limit => return Err(parameter_filter_error(name, "is_number")),
            "min" | "max" | "greater" | "less" => {}
            _ => {
                return Err(ApiError::bad_request(format!(
                    "parameter `{name}` uses unsupported is_number argument `{arg}`"
                )));
            }
        }
    }
    Ok(())
}

fn parse_filter_args(filter: &str) -> Result<BTreeMap<String, String>, ApiError> {
    let Some((_, rest)) = filter.split_once('(') else {
        return Ok(BTreeMap::new());
    };
    let args = rest
        .strip_suffix(')')
        .ok_or_else(|| ApiError::bad_request(format!("invalid validator syntax `{filter}`")))?;
    if args.trim().is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut parsed = BTreeMap::new();
    for item in args.split(',') {
        let (name, value) = item
            .split_once('=')
            .ok_or_else(|| ApiError::bad_request(format!("invalid validator argument `{item}`")))?;
        parsed.insert(
            name.trim().to_string(),
            value
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string(),
        );
    }
    Ok(parsed)
}

fn bind_value_for_request_field(
    name: &str,
    spec: &MaterializedViewRequestFieldSpec,
    value: &Value,
) -> Result<QueryBindValue, ApiError> {
    match spec.r#type.as_str() {
        "string" => value
            .as_str()
            .map(|value| QueryBindValue::Utf8(value.to_string()))
            .ok_or_else(|| parameter_filter_error(name, "is_string")),
        "int64" | "integer" => value
            .as_i64()
            .or_else(|| value.as_str().and_then(|value| value.parse::<i64>().ok()))
            .map(QueryBindValue::Int64)
            .ok_or_else(|| parameter_filter_error(name, "is_integer")),
        "float64" | "number" => value
            .as_f64()
            .or_else(|| value.as_str().and_then(|value| value.parse::<f64>().ok()))
            .filter(|value| value.is_finite())
            .map(QueryBindValue::Float64)
            .ok_or_else(|| parameter_filter_error(name, "is_number")),
        "bool" | "boolean" => value
            .as_bool()
            .or_else(|| value.as_str().and_then(parse_bool))
            .map(QueryBindValue::Boolean)
            .ok_or_else(|| parameter_filter_error(name, "is_boolean")),
        "json" => serde_json::to_string(value)
            .map(QueryBindValue::Utf8)
            .map_err(ApiError::bad_request),
        other => Err(ApiError::bad_request(format!(
            "parameter `{name}` declares unsupported type `{other}`"
        ))),
    }
}

fn request_field_has_validator(spec: &MaterializedViewRequestFieldSpec, validator: &str) -> bool {
    spec.validators.iter().any(|candidate| {
        filter_name(candidate) == validator || filter_name(candidate) == format!("is_{validator}")
    })
}

fn filter_name(filter: &str) -> &str {
    filter
        .split_once('(')
        .map(|(name, _)| name.trim())
        .unwrap_or_else(|| filter.trim())
}

fn value_is_empty(value: &Value) -> bool {
    value.is_null() || value.as_str().is_some_and(str::is_empty)
}

fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn parameter_filter_error(name: &str, filter: &str) -> ApiError {
    ApiError::bad_request(format!("parameter `{name}` must pass {filter}"))
}

fn materialized_rows_to_api_rows(
    rows: &[Value],
    response_schema: &MaterializedViewResponseSchema,
) -> Result<Vec<Value>, ApiError> {
    rows.iter()
        .map(|row| {
            let mut output = serde_json::Map::new();
            for column in &response_schema.columns {
                let value = response_column_value(row, column)?;
                output.insert(column.name.clone(), value);
            }
            Ok(Value::Object(output))
        })
        .collect()
}

fn response_column_value(
    row: &Value,
    column: &MaterializedViewResponseColumnSpec,
) -> Result<Value, ApiError> {
    let value = extract_materialized_source_value(row, &column.source)?;
    coerce_response_column_value(column, value)
}

fn extract_materialized_source_value(row: &Value, source: &str) -> Result<Value, ApiError> {
    let object = row
        .as_object()
        .ok_or_else(|| ApiError::internal("view query row must be a JSON object"))?;
    let mut parts = source.split('.');
    let root = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| ApiError::bad_request("response schema source must be non-empty"))?;
    let base = match root {
        "key" => object.get("key").or_else(|| object.get("key_json")),
        "key_json" => object.get("key_json").or_else(|| object.get("key")),
        "value" => object.get("value").or_else(|| object.get("value_json")),
        "value_json" => object.get("value_json").or_else(|| object.get("value")),
        "weight" => object.get("weight"),
        other => object.get(other),
    }
    .ok_or_else(|| ApiError::bad_request(format!("response source `{root}` is not available")))?;
    let mut value = match root {
        "key" | "key_json" | "value" | "value_json" => parse_json_encoded_source_value(base),
        _ => base.clone(),
    };

    for part in parts {
        value = value
            .as_object()
            .and_then(|object| object.get(part))
            .cloned()
            .ok_or_else(|| {
                ApiError::bad_request(format!("response source `{source}` is not available"))
            })?;
    }

    Ok(value)
}

fn parse_json_encoded_source_value(value: &Value) -> Value {
    match value {
        Value::String(raw) => {
            serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.clone()))
        }
        other => other.clone(),
    }
}

fn coerce_response_column_value(
    column: &MaterializedViewResponseColumnSpec,
    value: Value,
) -> Result<Value, ApiError> {
    match column.r#type.as_str() {
        "string" => match value {
            Value::String(_) => Ok(value),
            Value::Null => Ok(Value::Null),
            other => Ok(Value::String(other.to_string())),
        },
        "int64" | "integer" => value.as_i64().map(|value| json!(value)).ok_or_else(|| {
            ApiError::bad_request(format!(
                "response column `{}` from `{}` must be an integer",
                column.name, column.source
            ))
        }),
        "float64" | "number" => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(|value| json!(value))
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "response column `{}` from `{}` must be a finite number",
                    column.name, column.source
                ))
            }),
        "bool" | "boolean" => value.as_bool().map(|value| json!(value)).ok_or_else(|| {
            ApiError::bad_request(format!(
                "response column `{}` from `{}` must be a boolean",
                column.name, column.source
            ))
        }),
        "json" => Ok(value),
        other => Err(ApiError::bad_request(format!(
            "response column `{}` declares unsupported type `{other}`",
            column.name
        ))),
    }
}

fn openapi_view_query_parameters(
    request: &[MaterializedViewRequestFieldSpec],
    include_cursor_parameters: bool,
) -> Value {
    let mut parameters = request
        .iter()
        .map(|field| {
            json!({
                "name": field.field_name,
                "in": field.field_in,
                "required": request_field_has_validator(field, "required") || field.field_in == "path",
                "description": field.description,
                "schema": openapi_request_field_schema(field)
            })
        })
        .collect::<Vec<_>>();
    parameters.push(json!({
        "name": "epoch",
        "in": "query",
        "required": false,
        "description": "Committed logical epoch to read",
        "schema": { "type": "integer", "minimum": 0 }
    }));
    if include_cursor_parameters {
        parameters.push(json!({
            "name": "page_token",
            "in": "query",
            "required": false,
            "description": "Cursor returned by next_page_token",
            "schema": { "type": "string" }
        }));
        parameters.push(json!({
            "name": "max_rows",
            "in": "query",
            "required": false,
            "description": "Maximum materialized rows to return",
            "schema": { "type": "integer", "minimum": 1 }
        }));
    }
    Value::Array(parameters)
}

fn openapi_request_field_schema(field: &MaterializedViewRequestFieldSpec) -> Value {
    let mut schema = openapi_scalar_schema(&field.r#type);
    if let (Some(object), Some(default_value)) = (schema.as_object_mut(), &field.default_value) {
        object.insert("default".to_string(), default_value.clone());
    }
    schema
}

fn openapi_query_response_schema(
    response_schema: Option<&MaterializedViewResponseSchema>,
) -> Value {
    let mut row_properties = serde_json::Map::new();
    if let Some(response_schema) = response_schema {
        for column in &response_schema.columns {
            row_properties.insert(column.name.clone(), openapi_scalar_schema(&column.r#type));
        }
    } else {
        row_properties.insert("key".to_string(), openapi_scalar_schema("string"));
        row_properties.insert("value".to_string(), openapi_scalar_schema("string"));
        row_properties.insert("weight".to_string(), openapi_scalar_schema("int64"));
    }

    json!({
        "type": "object",
        "properties": {
            "rows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": Value::Object(row_properties)
                }
            },
            "logical_epoch": {
                "type": "integer",
                "format": "int64"
            },
            "next_page_token": {
                "type": "string"
            }
        }
    })
}

fn openapi_scalar_schema(type_name: &str) -> Value {
    match type_name {
        "string" => json!({ "type": "string" }),
        "int64" => json!({ "type": "integer", "format": "int64" }),
        "integer" => json!({ "type": "integer" }),
        "float64" => json!({ "type": "number", "format": "double" }),
        "number" => json!({ "type": "number" }),
        "bool" | "boolean" => json!({ "type": "boolean" }),
        "json" => json!({}),
        _ => json!({}),
    }
}

fn api_metadata_from_create_view_request(
    request: &CreateViewRequest,
) -> MaterializedViewApiMetadata {
    MaterializedViewApiMetadata {
        description: request.description.clone(),
        url_path: request.url_path.clone(),
        request: request.request.clone(),
        response_schema: request.response_schema.clone(),
        sql_template: request.sql_template.clone(),
        response_formats: if request.response_formats.is_empty() {
            vec!["json".to_string()]
        } else {
            request.response_formats.clone()
        },
        query_policy_id: request.query_policy_id.clone(),
    }
}

fn runtime_artifact_status_text(status: &RuntimeFelderaArtifactSelectionStatus) -> &'static str {
    match status {
        RuntimeFelderaArtifactSelectionStatus::DirectExecutionDisabled => {
            "direct_execution_disabled"
        }
        RuntimeFelderaArtifactSelectionStatus::DirectExecutionEnabled { .. } => {
            "direct_execution_enabled"
        }
    }
}

fn active_view_response(
    state: &ApiState,
    active: &ActiveMaterializedView,
    outcome: Option<&str>,
) -> Result<ViewResponse, ApiError> {
    view_response(
        &active.spec,
        active.spec_hash.clone(),
        active.execution_mode.clone(),
        active.lifecycle.clone(),
        state.allow_legacy_recovered_sql_views,
        active.api.clone(),
        active.artifact.clone(),
        outcome,
    )
}

fn view_response(
    spec: &StandingViewSpec,
    spec_hash: String,
    execution_mode: MaterializedViewExecutionMode,
    lifecycle: MaterializedViewLifecycleStatus,
    allow_legacy_recovered_sql_views: bool,
    api: Option<MaterializedViewApiMetadata>,
    artifact: Option<MaterializedViewArtifactBinding>,
    outcome: Option<&str>,
) -> Result<ViewResponse, ApiError> {
    let input = spec
        .input_relations
        .first()
        .ok_or_else(|| ApiError::bad_request("view has no input relation"))?;
    let api = api.unwrap_or_else(|| MaterializedViewApiMetadata {
        response_formats: vec!["json".to_string()],
        ..MaterializedViewApiMetadata::default()
    });

    let (query_enabled, disabled_reason) = view_query_availability(
        &execution_mode,
        &lifecycle,
        allow_legacy_recovered_sql_views,
    );
    let compile_job_id = if execution_mode == MaterializedViewExecutionMode::FelderaCompilePending {
        Some(view_compile_deploy_job_id(&spec.view_id, &spec_hash))
    } else {
        None
    };

    Ok(ViewResponse {
        view_id: spec.view_id.clone(),
        url_path: api.url_path.clone(),
        input_relation_id: input.relation_id.clone(),
        input_relation_version: input.relation_version.clone(),
        spec_hash,
        execution_mode,
        lifecycle,
        query_enabled,
        disabled_reason,
        compile_job_id,
        query_endpoint: api
            .url_path
            .as_deref()
            .map(|path| format!("/v1/api/{}", normalize_api_path(path)))
            .unwrap_or_else(|| format!("/v1/views/{}/query", spec.view_id)),
        description: api.description,
        request: api.request,
        response_schema: api.response_schema,
        sql_template: api.sql_template,
        response_formats: api.response_formats,
        query_policy_id: api.query_policy_id,
        artifact,
        outcome: outcome.map(ToString::to_string),
    })
}

fn lifecycle_for_create_view_execution(
    execution_mode: &MaterializedViewExecutionMode,
) -> MaterializedViewLifecycleStatus {
    match execution_mode {
        MaterializedViewExecutionMode::LegacyRecoveredSql => {
            MaterializedViewLifecycleStatus::legacy_recovered_sql()
        }
        MaterializedViewExecutionMode::StandingRuntime => {
            MaterializedViewLifecycleStatus::standing_runtime()
        }
        MaterializedViewExecutionMode::FelderaCompilePending => {
            MaterializedViewLifecycleStatus::feldera_compile_pending(Some(
                "view accepted; feldera compiler/deploy worker is not configured in this build"
                    .to_string(),
            ))
        }
    }
}

fn view_query_availability(
    execution_mode: &MaterializedViewExecutionMode,
    lifecycle: &MaterializedViewLifecycleStatus,
    allow_legacy_recovered_sql_views: bool,
) -> (bool, Option<String>) {
    match execution_mode {
        MaterializedViewExecutionMode::LegacyRecoveredSql => {
            if allow_legacy_recovered_sql_views {
                (true, None)
            } else {
                (
                    false,
                    Some("legacy_recovered_sql_views_disabled".to_string()),
                )
            }
        }
        MaterializedViewExecutionMode::FelderaCompilePending => {
            (false, Some("feldera_compile_pending".to_string()))
        }
        MaterializedViewExecutionMode::StandingRuntime => {
            if lifecycle.compile_status == MaterializedViewCompileStatus::Success
                && lifecycle.deployment_status == MaterializedViewDeploymentStatus::Running
            {
                (true, None)
            } else {
                (false, Some("standing_runtime_not_deployed".to_string()))
            }
        }
    }
}

fn view_spec_from_request(
    request: &CreateViewRequest,
    catalog: &VelorixRelationCatalogV1,
    artifact: Option<&FelderaCompileArtifactMetadata>,
) -> Result<StandingViewSpec, ApiError> {
    let input = catalog_input_relation_schema(catalog).map_err(ApiError::bad_request)?;
    Ok(StandingViewSpec {
        view_id: request.view_id.clone(),
        sql: request.sql.clone(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        input_relations: vec![input.clone()],
        output_relations: request
            .artifact
            .as_ref()
            .map(|artifact_request| artifact_request.metadata.output_schemas.clone())
            .or_else(|| artifact.map(|artifact| artifact.output_schemas.clone()))
            .unwrap_or_else(|| {
                vec![generic_materialized_view_output_schema(
                    request.view_id.as_str(),
                    input.schema_fingerprint.as_str(),
                )]
            }),
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    })
}

fn generic_materialized_view_output_schema(
    view_id: &str,
    schema_fingerprint: &str,
) -> RelationSchema {
    RelationSchema {
        relation_id: view_id.to_string(),
        relation_name: view_id.to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: schema_fingerprint.to_string(),
        columns: vec![
            ColumnSchema {
                name: "key_json".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "value_json".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "weight".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["key_json".to_string()],
    }
}

fn rows_to_record_batch(
    catalog: &VelorixRelationCatalogV1,
    rows: &[Value],
) -> Result<RecordBatch, ApiError> {
    if rows.is_empty() {
        return Err(ApiError::bad_request(
            "ingest request must contain at least one row",
        ));
    }

    catalog
        .validate_supported_incremental_adapter_scope()
        .map_err(ApiError::bad_request)?;
    let schema = datafusion_schema_from_catalog(catalog).map_err(ApiError::bad_request)?;
    let arrays = catalog
        .relation_schema
        .columns
        .iter()
        .map(|column| json_column_to_arrow_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema, arrays).map_err(ApiError::bad_request)
}

fn json_column_to_arrow_array(
    column: &RelationColumnV1,
    rows: &[Value],
) -> Result<ArrayRef, ApiError> {
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Boolean => Ok(Arc::new(BooleanArray::from(collect_column_values(
            column,
            rows,
            json_bool_value,
        )?))),
        ArrowPhysicalTypeV1::Int64 => Ok(Arc::new(Int64Array::from(collect_column_values(
            column,
            rows,
            json_i64_value,
        )?))),
        ArrowPhysicalTypeV1::Float64 => Ok(Arc::new(Float64Array::from(collect_column_values(
            column,
            rows,
            json_f64_value,
        )?))),
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => {
            let scale_i8 = i8::try_from(*scale)
                .map_err(|_| ApiError::bad_request("decimal scale is out of range"))?;
            let values = collect_column_values(column, rows, |column, value| {
                json_decimal128_value(column, value, *precision, *scale)
            })?;
            Ok(Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(*precision, scale_i8)
                    .map_err(ApiError::bad_request)?,
            ))
        }
        ArrowPhysicalTypeV1::Utf8 => Ok(Arc::new(StringArray::from(collect_column_values(
            column,
            rows,
            json_string_value,
        )?))),
        ArrowPhysicalTypeV1::Date32 => Ok(Arc::new(Date32Array::from(collect_column_values(
            column,
            rows,
            json_i32_value,
        )?))),
        ArrowPhysicalTypeV1::TimestampNanosecond { timezone } => {
            let array = TimestampNanosecondArray::from(collect_column_values(
                column,
                rows,
                json_i64_value,
            )?)
            .with_timezone_opt(timezone.clone());
            Ok(Arc::new(array))
        }
        ArrowPhysicalTypeV1::DictionaryUtf8 { key_type, .. } => {
            let values = collect_column_values(column, rows, json_string_value)?;
            dictionary_utf8_array(key_type, values)
        }
        ArrowPhysicalTypeV1::JsonUtf8 => Ok(Arc::new(StringArray::from(collect_column_values(
            column,
            rows,
            json_canonical_string_value,
        )?))),
    }
}

fn collect_column_values<T>(
    column: &RelationColumnV1,
    rows: &[Value],
    parse: impl Fn(&RelationColumnV1, &Value) -> Result<T, ApiError>,
) -> Result<Vec<Option<T>>, ApiError> {
    rows.iter()
        .map(|row| {
            let Some(value) = json_row_column_value(row, column)? else {
                return Ok(None);
            };
            parse(column, value).map(Some)
        })
        .collect()
}

fn json_row_column_value<'a>(
    row: &'a Value,
    column: &RelationColumnV1,
) -> Result<Option<&'a Value>, ApiError> {
    let object = row
        .as_object()
        .ok_or_else(|| ApiError::bad_request("each ingest row must be a JSON object"))?;
    let value = object.get(&column.name).ok_or_else(|| {
        ApiError::bad_request(format!(
            "row.{} is required by relation schema",
            column.name
        ))
    })?;
    if value.is_null() {
        if column.nullable {
            Ok(None)
        } else {
            Err(ApiError::bad_request(format!(
                "row.{} must be non-null",
                column.name
            )))
        }
    } else {
        Ok(Some(value))
    }
}

fn json_bool_value(column: &RelationColumnV1, value: &Value) -> Result<bool, ApiError> {
    value
        .as_bool()
        .ok_or_else(|| ApiError::bad_request(format!("row.{} must be a boolean", column.name)))
}

fn json_i64_value(column: &RelationColumnV1, value: &Value) -> Result<i64, ApiError> {
    value
        .as_i64()
        .ok_or_else(|| ApiError::bad_request(format!("row.{} must be an integer", column.name)))
}

fn json_i32_value(column: &RelationColumnV1, value: &Value) -> Result<i32, ApiError> {
    let value = json_i64_value(column, value)?;
    i32::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("row.{} is outside Int32 range", column.name)))
}

fn json_f64_value(column: &RelationColumnV1, value: &Value) -> Result<f64, ApiError> {
    let value = value.as_f64().ok_or_else(|| {
        ApiError::bad_request(format!("row.{} must be a finite number", column.name))
    })?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(ApiError::bad_request(format!(
            "row.{} must be a finite number",
            column.name
        )))
    }
}

fn json_string_value(column: &RelationColumnV1, value: &Value) -> Result<String, ApiError> {
    value
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| ApiError::bad_request(format!("row.{} must be a string", column.name)))
}

fn json_canonical_string_value(
    _column: &RelationColumnV1,
    value: &Value,
) -> Result<String, ApiError> {
    serde_json::to_string(value).map_err(ApiError::bad_request)
}

fn json_decimal128_value(
    column: &RelationColumnV1,
    value: &Value,
    precision: u8,
    scale: u8,
) -> Result<i128, ApiError> {
    let raw = match value {
        Value::Number(number) if scale == 0 => number
            .as_i64()
            .map(|value| value.to_string())
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "row.{} decimal number must be an integer",
                    column.name
                ))
            })?,
        Value::String(value) => value.clone(),
        _ => {
            return Err(ApiError::bad_request(format!(
                "row.{} must be a decimal string",
                column.name
            )));
        }
    };
    parse_decimal128(&raw, precision, scale).map_err(|reason| {
        ApiError::bad_request(format!("row.{} has invalid decimal: {reason}", column.name))
    })
}

fn parse_decimal128(raw: &str, precision: u8, scale: u8) -> Result<i128, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("empty value".to_string());
    }
    let (negative, unsigned) = trimmed
        .strip_prefix('-')
        .map(|value| (true, value))
        .unwrap_or((false, trimmed));
    if unsigned.is_empty() || unsigned.starts_with('+') {
        return Err("expected optional '-' followed by digits".to_string());
    }
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    if parts.next().is_some() {
        return Err("multiple decimal points".to_string());
    }
    if integer.is_empty() || !integer.chars().all(|character| character.is_ascii_digit()) {
        return Err("integer part must contain digits".to_string());
    }
    if !fraction.chars().all(|character| character.is_ascii_digit()) {
        return Err("fractional part must contain digits".to_string());
    }
    if fraction.len() > scale as usize {
        return Err(format!("scale exceeds declared scale {scale}"));
    }

    let significant_integer_digits = integer.trim_start_matches('0').len();
    let significant_fraction_digits = fraction.trim_end_matches('0').len();
    if significant_integer_digits + significant_fraction_digits > precision as usize {
        return Err(format!("precision exceeds declared precision {precision}"));
    }

    let mut digits = String::with_capacity(integer.len() + scale as usize);
    digits.push_str(integer);
    digits.push_str(fraction);
    for _ in fraction.len()..scale as usize {
        digits.push('0');
    }
    let magnitude = digits
        .parse::<i128>()
        .map_err(|_| "value exceeds Decimal128 range".to_string())?;
    Ok(if negative { -magnitude } else { magnitude })
}

fn dictionary_utf8_array(
    key_type: &DictionaryKeyTypeV1,
    values: Vec<Option<String>>,
) -> Result<ArrayRef, ApiError> {
    match key_type {
        DictionaryKeyTypeV1::Int8 => dictionary_utf8_array_with_key::<Int8Type>(values),
        DictionaryKeyTypeV1::Int16 => dictionary_utf8_array_with_key::<Int16Type>(values),
        DictionaryKeyTypeV1::Int32 => dictionary_utf8_array_with_key::<Int32Type>(values),
        DictionaryKeyTypeV1::Int64 => dictionary_utf8_array_with_key::<Int64Type>(values),
    }
}

fn dictionary_utf8_array_with_key<K>(values: Vec<Option<String>>) -> Result<ArrayRef, ApiError>
where
    K: ArrowDictionaryKeyType,
{
    let mut builder = StringDictionaryBuilder::<K>::new();
    for value in values {
        match value {
            Some(value) => {
                builder.append(value).map_err(ApiError::bad_request)?;
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn ingest_outcome_parts(
    outcome: AppendValidatedEnvelopeOutcome,
) -> Result<(StatusCode, &'static str, IngestBatchDescriptor), ApiError> {
    match outcome {
        AppendValidatedEnvelopeOutcome::Appended { descriptor } => {
            Ok((StatusCode::CREATED, "appended", descriptor))
        }
        AppendValidatedEnvelopeOutcome::Duplicate { descriptor } => {
            Ok((StatusCode::OK, "duplicate", descriptor))
        }
        AppendValidatedEnvelopeOutcome::Conflict {
            descriptor,
            object_key,
            reason,
        } => Err(ApiError::conflict(format!(
            "ingest conflict for stream={} partition={} offsets={}-{} object_key={} reason={}",
            descriptor.stream_id,
            descriptor.partition_id,
            descriptor.start_offset_inclusive,
            descriptor.end_offset_exclusive,
            object_key.as_str(),
            reason
        ))),
    }
}

fn ingest_descriptor_response(descriptor: &IngestBatchDescriptor) -> IngestDescriptorResponse {
    IngestDescriptorResponse {
        stream_id: descriptor.stream_id.clone(),
        partition_id: descriptor.partition_id,
        start_offset_inclusive: descriptor.start_offset_inclusive,
        end_offset_exclusive: descriptor.end_offset_exclusive,
        object_key: descriptor.object_key.as_str().to_string(),
    }
}

fn record_batches_to_json_rows(batches: &[RecordBatch]) -> Result<Vec<Value>, ApiError> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row_index in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();
            for (column_index, field) in schema.fields().iter().enumerate() {
                row.insert(
                    field.name().clone(),
                    arrow_value_to_json(batch.column(column_index), row_index)?,
                );
            }
            rows.push(Value::Object(row));
        }
    }
    Ok(rows)
}

fn arrow_value_to_json(column: &ArrayRef, row_index: usize) -> Result<Value, ApiError> {
    if column.is_null(row_index) {
        return Ok(Value::Null);
    }
    match column.data_type() {
        DataType::Utf8 => Ok(Value::String(
            column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ApiError::internal("invalid Utf8 Arrow column"))?
                .value(row_index)
                .to_string(),
        )),
        DataType::Int64 => Ok(json!(column
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| ApiError::internal("invalid Int64 Arrow column"))?
            .value(row_index))),
        DataType::Float64 => Ok(json!(column
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| ApiError::internal("invalid Float64 Arrow column"))?
            .value(row_index))),
        DataType::Boolean => Ok(json!(column
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| ApiError::internal("invalid Boolean Arrow column"))?
            .value(row_index))),
        other => Err(ApiError::internal(format!(
            "unsupported query result Arrow type {other:?}"
        ))),
    }
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: error.to_string(),
        }
    }

    fn unauthorized(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: error.to_string(),
        }
    }

    fn conflict(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: error.to_string(),
        }
    }

    fn payload_too_large(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: error.to_string(),
        }
    }

    fn service_unavailable(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: error.to_string(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    }
}

fn legacy_recovered_sql_views_disabled_error() -> ApiError {
    ApiError::conflict(
        "legacy_recovered_sql_views_disabled: legacy recovered SQL views require VELORIX_ALLOW_LEGACY_RECOVERED_SQL_VIEWS=1",
    )
}

fn generic_query_disabled_error() -> ApiError {
    ApiError::conflict(
        "generic_query_disabled: /v1/query requires VELORIX_ENABLE_GENERIC_QUERY=1 and is not part of the default product API surface",
    )
}

fn meta_error_to_api(error: MetaStoreError) -> ApiError {
    match error {
        MetaStoreError::RelationCatalogNotFound { .. }
        | MetaStoreError::RelationSchema(_)
        | MetaStoreError::EmptyIngestRange { .. }
        | MetaStoreError::EmptyField { .. }
        | MetaStoreError::InvalidBearerToken { .. }
        | MetaStoreError::InvalidDuration { .. }
        | MetaStoreError::IntegerOutOfRange { .. }
        | MetaStoreError::TimestampOverflow
        | MetaStoreError::Serialization(_)
        | MetaStoreError::StandingRuntimeCheckpointScopeMismatch
        | MetaStoreError::UnexpectedOutcome(_) => ApiError::bad_request(error),
        MetaStoreError::RelationCatalogConflict { .. }
        | MetaStoreError::StandingRuntimeOwnerMismatch => ApiError::conflict(error),
        MetaStoreError::UnsupportedCapability(_) => ApiError::service_unavailable(error),
        MetaStoreError::Remote(_) | MetaStoreError::Oss(_) | MetaStoreError::Hiqlite(_) => {
            ApiError::internal(error)
        }
    }
}

fn materialized_view_registry_error_to_api(error: MaterializedViewRegistryError) -> ApiError {
    match error {
        MaterializedViewRegistryError::InvalidExecutionMode { view_id, reason }
            if reason == InvalidExecutionModeReason::StandingRuntimeMissingIdentity =>
        {
            ApiError::conflict(format!(
                "artifact-backed view `{view_id}` is missing standing runtime identity"
            ))
        }
        MaterializedViewRegistryError::InvalidExecutionMode { view_id, reason }
            if reason == InvalidExecutionModeReason::StandingRuntimeMissingArtifact =>
        {
            ApiError::conflict(format!(
                "standing runtime view `{view_id}` is missing artifact binding"
            ))
        }
        error @ (MaterializedViewRegistryError::RecordConflict { .. }
        | MaterializedViewRegistryError::ActiveRecordConflict { .. }
        | MaterializedViewRegistryError::InvalidExecutionMode { .. }
        | MaterializedViewRegistryError::ApiPathConflict { .. }) => ApiError::conflict(error),
        error @ MaterializedViewRegistryError::ActiveRecordConditionalUpdateUnsupported {
            ..
        } => ApiError::service_unavailable(error),
        error => ApiError::bad_request(error),
    }
}

fn query_policy_catalog_error_to_api(error: QueryPolicyCatalogError) -> ApiError {
    match error {
        QueryPolicyCatalogError::ObjectStore(object_store::Error::AlreadyExists { .. }) => {
            ApiError::conflict(error)
        }
        QueryPolicyCatalogError::ObjectStore(object_store::Error::NotFound { .. }) => {
            ApiError::bad_request("query policy not found")
        }
        QueryPolicyCatalogError::ObjectKey(_)
        | QueryPolicyCatalogError::Json(_)
        | QueryPolicyCatalogError::Policy(_)
        | QueryPolicyCatalogError::UnsupportedSchemaVersion { .. }
        | QueryPolicyCatalogError::TenantIdMismatch { .. }
        | QueryPolicyCatalogError::QueryPolicyIdMismatch { .. } => ApiError::bad_request(error),
        QueryPolicyCatalogError::ObjectStoreCapabilities(_)
        | QueryPolicyCatalogError::MissingProductionAuthorityEvidence => {
            ApiError::service_unavailable(error)
        }
        QueryPolicyCatalogError::ObjectStore(_) => ApiError::internal(error),
    }
}

fn view_compile_deploy_job_registry_error_to_api(
    error: ViewCompileDeployJobRegistryError,
) -> ApiError {
    match error {
        ViewCompileDeployJobRegistryError::RecordConflict { .. }
        | ViewCompileDeployJobRegistryError::RecordIdentityMismatch { .. } => {
            ApiError::conflict(error)
        }
        ViewCompileDeployJobRegistryError::ObjectKey(_)
        | ViewCompileDeployJobRegistryError::Serde(_)
        | ViewCompileDeployJobRegistryError::ObjectStore(_) => ApiError::internal(error),
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ApiError {}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "error": self.message,
            })),
        )
            .into_response()
    }
}

#[derive(Clone, Debug)]
struct ApiConfig {
    bind: SocketAddr,
    tls: Option<ApiTlsConfig>,
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    region: String,
    bucket: String,
    prefix: String,
    force_path_style: bool,
    authority_store_id: String,
    authority_namespace: String,
    state_path: String,
    operator_id: String,
    backend_name: String,
    meta_grpc_endpoint: Option<String>,
    meta_bearer_token: Option<String>,
    api_bearer_token: Option<String>,
    admin_bearer_token: Option<String>,
    max_request_body_bytes: usize,
    max_ingest_rows: usize,
    allow_legacy_recovered_sql_views: bool,
    enable_generic_query: bool,
    standing_runtime_fencing: StandingRuntimeFencingMode,
    standing_runtime_owner_ttl_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ApiTlsConfig {
    bind: SocketAddr,
    cert_path: String,
    key_path: String,
}

impl ApiConfig {
    fn from_env() -> anyhow::Result<Self> {
        if std::env::var("VELORIX_S3_COMPAT").ok().as_deref() != Some("1") {
            return Err(anyhow!("velorix-api requires VELORIX_S3_COMPAT=1"));
        }
        let bind = std::env::var("VELORIX_API_BIND")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .context("invalid VELORIX_API_BIND")?;
        let tls = api_tls_config_from_values(
            std::env::var("VELORIX_API_TLS_CERT_PATH").ok(),
            std::env::var("VELORIX_API_TLS_KEY_PATH").ok(),
            std::env::var("VELORIX_API_TLS_BIND").ok(),
        )?;
        let endpoint = required_env("AWS_ENDPOINT_URL")?;
        let access_key_id = required_env("AWS_ACCESS_KEY_ID")?;
        let secret_access_key = required_env("AWS_SECRET_ACCESS_KEY")?;
        let session_token = optional_nonempty_env("AWS_SESSION_TOKEN");
        let region = required_env("AWS_REGION")?;
        let bucket = required_env("VELORIX_S3_BUCKET")?;
        let prefix = std::env::var("VELORIX_S3_PREFIX").unwrap_or_else(|_| "product".to_string());
        let force_path_style = parse_bool_env("VELORIX_S3_FORCE_PATH_STYLE", true)?;
        let authority_store_id = std::env::var("VELORIX_AUTHORITY_STORE_ID")
            .unwrap_or_else(|_| default_authority_store_id(&bucket, &prefix));
        let authority_namespace =
            std::env::var("VELORIX_AUTHORITY_NAMESPACE").unwrap_or_else(|_| "velorix".to_string());
        let state_path =
            std::env::var("VELORIX_STATE_PATH").unwrap_or_else(|_| "v1/state/slatedb".to_string());
        let operator_id =
            std::env::var("VELORIX_OPERATOR_ID").unwrap_or_else(|_| "velorix-api".to_string());
        let backend_name = std::env::var("VELORIX_OBJECT_STORE_BACKEND")
            .unwrap_or_else(|_| "s3-compatible".into());
        let meta_grpc_endpoint = std::env::var("VELORIX_META_GRPC_ENDPOINT")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let meta_bearer_token = optional_meta_bearer_token_from_env()?;
        let api_bearer_token = optional_api_bearer_token_from_env()?;
        let admin_bearer_token = optional_admin_bearer_token_from_env()?;
        let allow_unauthenticated_dev =
            parse_bool_env("VELORIX_API_ALLOW_UNAUTHENTICATED_DEV", false)?;
        if api_bearer_token.is_none() && !allow_unauthenticated_dev {
            return Err(anyhow!(
                "velorix-api requires VELORIX_API_BEARER_TOKEN or VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1"
            ));
        }
        if admin_bearer_token.is_none() && !allow_unauthenticated_dev {
            return Err(anyhow!(
                "velorix-api requires VELORIX_ADMIN_BEARER_TOKEN or VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1"
            ));
        }
        if api_bearer_token.is_some()
            && admin_bearer_token.is_some()
            && api_bearer_token == admin_bearer_token
        {
            return Err(anyhow!(
                "VELORIX_ADMIN_BEARER_TOKEN must be distinct from VELORIX_API_BEARER_TOKEN"
            ));
        }
        let max_request_body_bytes =
            parse_positive_usize_env("VELORIX_API_MAX_REQUEST_BODY_BYTES", 1024 * 1024)?;
        let max_ingest_rows = parse_positive_usize_env("VELORIX_API_MAX_INGEST_ROWS", 10_000)?;
        let allow_legacy_recovered_sql_views =
            parse_bool_env("VELORIX_ALLOW_LEGACY_RECOVERED_SQL_VIEWS", false)?;
        let enable_generic_query = parse_bool_env("VELORIX_ENABLE_GENERIC_QUERY", false)?;
        let api_replica_count =
            parse_api_replica_count(std::env::var("VELORIX_API_REPLICA_COUNT").ok().as_deref())?;
        let standing_runtime_fencing = StandingRuntimeFencingMode::from_env(
            std::env::var("VELORIX_STANDING_RUNTIME_FENCING")
                .ok()
                .as_deref(),
            api_replica_count,
        )?;
        let standing_runtime_owner_ttl_ms =
            parse_positive_u64_env("VELORIX_STANDING_RUNTIME_OWNER_TTL_MS", 30_000)?;

        Ok(Self {
            bind,
            tls,
            endpoint,
            access_key_id,
            secret_access_key,
            session_token,
            region,
            bucket,
            prefix,
            force_path_style,
            authority_store_id,
            authority_namespace,
            state_path,
            operator_id,
            backend_name,
            meta_grpc_endpoint,
            meta_bearer_token,
            api_bearer_token,
            admin_bearer_token,
            max_request_body_bytes,
            max_ingest_rows,
            allow_legacy_recovered_sql_views,
            enable_generic_query,
            standing_runtime_fencing,
            standing_runtime_owner_ttl_ms,
        })
    }

    fn object_store(&self) -> anyhow::Result<Arc<dyn ObjectStore>> {
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(&self.endpoint)
            .with_region(&self.region)
            .with_bucket_name(&self.bucket)
            .with_access_key_id(&self.access_key_id)
            .with_secret_access_key(&self.secret_access_key)
            .with_virtual_hosted_style_request(!self.force_path_style)
            .with_conditional_put(S3ConditionalPut::ETagMatch);
        if let Some(session_token) = &self.session_token {
            builder = builder.with_token(session_token);
        }
        if self.endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
        let store = builder.build()?;
        if self.prefix.trim().is_empty() {
            Ok(Arc::new(store))
        } else {
            Ok(Arc::new(PrefixStore::new(
                store,
                ObjectPath::from(self.prefix.trim_matches('/')),
            )))
        }
    }
}

fn default_authority_store_id(bucket: &str, prefix: &str) -> String {
    format!("s3://s3-compatible/{bucket}/{prefix}")
}

fn required_env(name: &str) -> anyhow::Result<String> {
    std::env::var(name).with_context(|| format!("{name} is required"))
}

fn optional_nonempty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn api_tls_config_from_values(
    cert_path: Option<String>,
    key_path: Option<String>,
    bind: Option<String>,
) -> anyhow::Result<Option<ApiTlsConfig>> {
    match (cert_path, key_path) {
        (None, None) => Ok(None),
        (Some(cert_path), Some(key_path)) => {
            if cert_path.trim().is_empty() {
                return Err(anyhow!("VELORIX_API_TLS_CERT_PATH must not be empty"));
            }
            if key_path.trim().is_empty() {
                return Err(anyhow!("VELORIX_API_TLS_KEY_PATH must not be empty"));
            }
            let bind = bind
                .unwrap_or_else(|| "0.0.0.0:8443".to_string())
                .parse()
                .context("invalid VELORIX_API_TLS_BIND")?;
            Ok(Some(ApiTlsConfig {
                bind,
                cert_path,
                key_path,
            }))
        }
        (Some(_), None) => Err(anyhow!(
            "VELORIX_API_TLS_KEY_PATH is required when VELORIX_API_TLS_CERT_PATH is set"
        )),
        (None, Some(_)) => Err(anyhow!(
            "VELORIX_API_TLS_CERT_PATH is required when VELORIX_API_TLS_KEY_PATH is set"
        )),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StandingRuntimeFencingMode {
    SingleWriter,
    LogicalFencing,
    Required,
}

impl StandingRuntimeFencingMode {
    fn from_env(value: Option<&str>, api_replica_count: u32) -> anyhow::Result<Self> {
        match value.map(str::trim).filter(|value| !value.is_empty()) {
            Some("required") | Some("production") | Some("multi-writer") => Ok(Self::Required),
            Some("logical-fencing") | Some("logical") | Some("operation-driven-logical") => {
                Ok(Self::LogicalFencing)
            }
            Some("unsafe-dev-only") => {
                if api_replica_count > 1 {
                    Err(anyhow!(
                        "VELORIX_STANDING_RUNTIME_FENCING=unsafe-dev-only is incompatible with VELORIX_API_REPLICA_COUNT={api_replica_count}"
                    ))
                } else {
                    Ok(Self::SingleWriter)
                }
            }
            Some(other) => Err(anyhow!(
                "unsupported VELORIX_STANDING_RUNTIME_FENCING `{other}`; expected `required`, `production`, `multi-writer`, `logical-fencing`, or `unsafe-dev-only`"
            )),
            None => Ok(Self::Required),
        }
    }

    fn requires_metadata(self) -> bool {
        !matches!(self, Self::SingleWriter)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::SingleWriter => "unsafe-dev-only",
            Self::LogicalFencing => "logical-fencing",
            Self::Required => "required",
        }
    }
}

fn parse_api_replica_count(value: Option<&str>) -> anyhow::Result<u32> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(1);
    };
    let parsed = value
        .parse::<u32>()
        .with_context(|| format!("invalid VELORIX_API_REPLICA_COUNT `{value}`"))?;
    if parsed == 0 {
        return Err(anyhow!(
            "VELORIX_API_REPLICA_COUNT must be greater than zero"
        ));
    }
    Ok(parsed)
}

fn optional_meta_bearer_token_from_env() -> anyhow::Result<Option<String>> {
    match env::var("VELORIX_META_BEARER_TOKEN") {
        Ok(value) => parse_optional_bearer_token(Some(value), "VELORIX_META_BEARER_TOKEN"),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(anyhow!("invalid VELORIX_META_BEARER_TOKEN: {error}")),
    }
}

fn optional_api_bearer_token_from_env() -> anyhow::Result<Option<String>> {
    match env::var("VELORIX_API_BEARER_TOKEN") {
        Ok(value) => parse_optional_bearer_token(Some(value), "VELORIX_API_BEARER_TOKEN"),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(anyhow!("invalid VELORIX_API_BEARER_TOKEN: {error}")),
    }
}

fn optional_admin_bearer_token_from_env() -> anyhow::Result<Option<String>> {
    match env::var("VELORIX_ADMIN_BEARER_TOKEN") {
        Ok(value) => parse_optional_bearer_token(Some(value), "VELORIX_ADMIN_BEARER_TOKEN"),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(anyhow!("invalid VELORIX_ADMIN_BEARER_TOKEN: {error}")),
    }
}

fn parse_optional_bearer_token(
    value: Option<String>,
    name: &str,
) -> anyhow::Result<Option<String>> {
    match value {
        Some(value) => {
            validate_bearer_token(&value).map_err(|error| anyhow!("invalid {name}: {error}"))?;
            Ok(Some(value))
        }
        None => Ok(None),
    }
}

fn parse_bool_env(name: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(name) {
        Ok(value) => match value.trim() {
            "1" | "true" => Ok(true),
            "0" | "false" => Ok(false),
            other => Err(anyhow!(
                "{name} must be 0, 1, true, or false; got `{other}`"
            )),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(anyhow!("invalid {name}: {error}")),
    }
}

fn parse_positive_usize_env(name: &str, default: usize) -> anyhow::Result<usize> {
    match env::var(name) {
        Ok(value) => {
            let value = value.trim();
            let parsed = value
                .parse::<usize>()
                .with_context(|| format!("invalid {name} `{value}`"))?;
            if parsed == 0 {
                Err(anyhow!("{name} must be greater than zero"))
            } else {
                Ok(parsed)
            }
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(anyhow!("invalid {name}: {error}")),
    }
}

fn parse_positive_u64_env(name: &str, default: u64) -> anyhow::Result<u64> {
    match env::var(name) {
        Ok(value) => {
            let value = value.trim();
            let parsed = value
                .parse::<u64>()
                .with_context(|| format!("invalid {name} `{value}`"))?;
            if parsed == 0 {
                Err(anyhow!("{name} must be greater than zero"))
            } else {
                Ok(parsed)
            }
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(anyhow!("invalid {name}: {error}")),
    }
}

async fn enforce_standing_runtime_fencing_startup(
    config: &ApiConfig,
    meta_store: Option<&Arc<dyn MetaStore>>,
) -> anyhow::Result<()> {
    if !config.standing_runtime_fencing.requires_metadata() {
        return Ok(());
    }
    let Some(meta_store) = meta_store else {
        return Err(anyhow!(
            "standing runtime fencing mode `{}` requires metadata; set VELORIX_META_GRPC_ENDPOINT to a compatible metadata service",
            config.standing_runtime_fencing.as_str()
        ));
    };
    let capabilities = meta_store
        .read_meta_store_capabilities()
        .await
        .map_err(|error| anyhow!("failed to read metadata service capabilities: {error}"))?;
    validate_standing_runtime_fencing_for_mode(
        &capabilities.standing_runtime_fencing,
        config.standing_runtime_fencing,
    )
}

fn validate_standing_runtime_fencing_for_mode(
    capability: &StandingRuntimeFencingCapability,
    mode: StandingRuntimeFencingMode,
) -> anyhow::Result<()> {
    match mode {
        StandingRuntimeFencingMode::SingleWriter => Ok(()),
        StandingRuntimeFencingMode::LogicalFencing => {
            validate_logical_standing_runtime_fencing(capability)
        }
        StandingRuntimeFencingMode::Required => {
            validate_production_standing_runtime_fencing(capability)
        }
    }
}

fn validate_logical_standing_runtime_fencing(
    capability: &StandingRuntimeFencingCapability,
) -> anyhow::Result<()> {
    let mut missing = Vec::new();
    if capability.capability_schema_version != STANDING_RUNTIME_FENCING_CAPABILITY_SCHEMA_VERSION {
        missing.push("supported_capability_schema_version");
    }
    if capability.owner_scope_kind != STANDING_RUNTIME_OWNER_SCOPE_KIND_TENANT_PROGRAM_VIEW {
        missing.push("tenant_program_view_owner_scope");
    }
    if !capability.linearizable_owner_lease {
        missing.push("linearizable_owner_lease");
    }
    if !capability.durable_monotonic_owner_epoch {
        missing.push("durable_monotonic_owner_epoch");
    }
    if !capability.owner_validated_checkpoint_publish {
        missing.push("owner_validated_checkpoint_publish");
    }
    if !capability.publish_checks_owner_and_latest_atomically {
        missing.push("publish_checks_owner_and_latest_atomically");
    }
    if !capability.publish_rejects_expired_owner {
        missing.push("publish_rejects_expired_owner");
    }
    if !capability.latest_read_linearizable {
        missing.push("latest_read_linearizable");
    }
    if !capability.publish_rejects_scope_mismatch {
        missing.push("publish_rejects_scope_mismatch");
    }
    if capability.max_owner_ttl_ms == 0 {
        missing.push("max_owner_ttl_ms");
    }
    if !capability.control_plane_auth_enforced {
        missing.push("control_plane_auth_enforced");
    }
    if !capability.multi_writer_fencing_safe {
        missing.push("multi_writer_fencing_safe");
    }
    if !matches!(
        capability.lease_authority_kind.as_str(),
        STANDING_RUNTIME_LEASE_AUTHORITY_KIND_HIQLITE_RAFT_SERIALIZED
            | STANDING_RUNTIME_LEASE_AUTHORITY_KIND_RAFT_REPLICATED_TIME
    ) {
        missing.push("recognized_lease_authority");
    }
    if !matches!(
        capability.lease_expiry_semantics.as_str(),
        STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_OPERATION_DRIVEN_LOGICAL
            | STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_BACKEND_WALL_CLOCK_TTL
    ) {
        missing.push("recognized_lease_expiry_semantics");
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "metadata backend `{}` is not safe for logical standing runtime multi-writer fencing; missing {}",
            capability.backend_name,
            missing.join(", ")
        ))
    }
}

fn validate_production_standing_runtime_fencing(
    capability: &StandingRuntimeFencingCapability,
) -> anyhow::Result<()> {
    let mut missing = Vec::new();
    if capability.capability_schema_version != STANDING_RUNTIME_FENCING_CAPABILITY_SCHEMA_VERSION {
        missing.push("supported_capability_schema_version");
    }
    if capability.owner_scope_kind != STANDING_RUNTIME_OWNER_SCOPE_KIND_TENANT_PROGRAM_VIEW {
        missing.push("tenant_program_view_owner_scope");
    }
    if !capability.linearizable_owner_lease {
        missing.push("linearizable_owner_lease");
    }
    if !capability.durable_monotonic_owner_epoch {
        missing.push("durable_monotonic_owner_epoch");
    }
    if !capability.authoritative_backend_time {
        missing.push("authoritative_backend_time");
    }
    if capability.backend_time_source_kind != STANDING_RUNTIME_BACKEND_TIME_SOURCE_RAFT_REPLICATED {
        missing.push("raft_replicated_authority_time_source");
    }
    if capability.lease_authority_kind != STANDING_RUNTIME_LEASE_AUTHORITY_KIND_RAFT_REPLICATED_TIME
    {
        missing.push("raft_replicated_time_lease_authority");
    }
    if capability.lease_expiry_semantics
        != STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_BACKEND_WALL_CLOCK_TTL
    {
        missing.push("backend_wall_clock_ttl_lease_expiry");
    }
    if !capability.owner_validated_checkpoint_publish {
        missing.push("owner_validated_checkpoint_publish");
    }
    if !capability.publish_checks_owner_and_latest_atomically {
        missing.push("publish_checks_owner_and_latest_atomically");
    }
    if !capability.publish_rejects_expired_owner {
        missing.push("publish_rejects_expired_owner");
    }
    if !capability.latest_read_linearizable {
        missing.push("latest_read_linearizable");
    }
    if !capability.publish_rejects_scope_mismatch {
        missing.push("publish_rejects_scope_mismatch");
    }
    if capability.max_owner_ttl_ms == 0 {
        missing.push("max_owner_ttl_ms");
    }
    if !capability.control_plane_auth_enforced {
        missing.push("control_plane_auth_enforced");
    }
    if !capability.multi_writer_fencing_safe {
        missing.push("multi_writer_fencing_safe");
    }
    if !capability.bounded_wall_clock_failover {
        missing.push("bounded_wall_clock_failover");
    }
    if capability.failover_time_bound_ms == 0 {
        missing.push("failover_time_bound_ms");
    }
    if !capability.production_bounded_failover_safe {
        missing.push("production_bounded_failover_safe");
    }
    if !capability.production_multi_writer_safe {
        missing.push("production_multi_writer_safe");
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "metadata backend `{}` is not production-safe for standing runtime multi-writer fencing; missing {}",
            capability.backend_name,
            missing.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standing_runtime_fencing_defaults_to_required() {
        assert_eq!(
            StandingRuntimeFencingMode::from_env(None, 2).unwrap(),
            StandingRuntimeFencingMode::Required
        );
        assert_eq!(
            StandingRuntimeFencingMode::from_env(None, 1).unwrap(),
            StandingRuntimeFencingMode::Required
        );
    }

    #[test]
    fn standing_runtime_fencing_allows_explicit_unsafe_dev_only_for_single_replica() {
        assert_eq!(
            StandingRuntimeFencingMode::from_env(Some("required"), 1).unwrap(),
            StandingRuntimeFencingMode::Required
        );
        assert_eq!(
            StandingRuntimeFencingMode::from_env(Some("logical-fencing"), 2).unwrap(),
            StandingRuntimeFencingMode::LogicalFencing
        );
        assert_eq!(
            StandingRuntimeFencingMode::from_env(Some("unsafe-dev-only"), 1).unwrap(),
            StandingRuntimeFencingMode::SingleWriter
        );
        assert!(StandingRuntimeFencingMode::from_env(Some("unsafe-dev-only"), 2).is_err());
        assert!(StandingRuntimeFencingMode::from_env(Some("maybe"), 1).is_err());
    }

    #[test]
    fn api_replica_count_rejects_zero_and_malformed_values() {
        assert_eq!(parse_api_replica_count(None).unwrap(), 1);
        assert_eq!(parse_api_replica_count(Some("3")).unwrap(), 3);
        assert!(parse_api_replica_count(Some("0")).is_err());
        assert!(parse_api_replica_count(Some("three")).is_err());
    }

    #[test]
    fn api_tls_config_requires_cert_and_key_together() {
        assert!(api_tls_config_from_values(None, None, None)
            .unwrap()
            .is_none());
        assert!(api_tls_config_from_values(Some("/cert.pem".to_string()), None, None).is_err());
        assert!(api_tls_config_from_values(None, Some("/key.pem".to_string()), None).is_err());
    }

    #[test]
    fn api_tls_config_defaults_and_parses_bind_address() {
        let config = api_tls_config_from_values(
            Some("/cert.pem".to_string()),
            Some("/key.pem".to_string()),
            None,
        )
        .unwrap()
        .unwrap();

        assert_eq!(config.bind.to_string(), "0.0.0.0:8443");
        assert_eq!(config.cert_path, "/cert.pem");
        assert_eq!(config.key_path, "/key.pem");

        let config = api_tls_config_from_values(
            Some("/cert.pem".to_string()),
            Some("/key.pem".to_string()),
            Some("127.0.0.1:9443".to_string()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(config.bind.to_string(), "127.0.0.1:9443");
    }

    #[test]
    fn default_authority_store_id_is_s3_compatible_not_local_rustfs() {
        assert_eq!(
            default_authority_store_id("velorix-product", "product/run-1"),
            "s3://s3-compatible/velorix-product/product/run-1"
        );
    }

    #[test]
    fn api_meta_bearer_token_parser_rejects_invalid_configured_tokens() {
        assert_eq!(
            parse_optional_bearer_token(None, "VELORIX_META_BEARER_TOKEN").unwrap(),
            None
        );
        assert_eq!(
            parse_optional_bearer_token(Some("secret".to_string()), "VELORIX_META_BEARER_TOKEN")
                .unwrap(),
            Some("secret".to_string())
        );
        assert!(
            parse_optional_bearer_token(Some(String::new()), "VELORIX_META_BEARER_TOKEN").is_err()
        );
        assert!(
            parse_optional_bearer_token(Some("   ".to_string()), "VELORIX_META_BEARER_TOKEN")
                .is_err()
        );
        assert!(parse_optional_bearer_token(
            Some("secret\n".to_string()),
            "VELORIX_META_BEARER_TOKEN"
        )
        .is_err());
    }

    #[test]
    fn production_standing_runtime_fencing_requires_every_capability_bit() {
        let unsafe_capability = StandingRuntimeFencingCapability {
            capability_schema_version: STANDING_RUNTIME_FENCING_CAPABILITY_SCHEMA_VERSION,
            backend_name: "in-memory".to_string(),
            owner_scope_kind: STANDING_RUNTIME_OWNER_SCOPE_KIND_TENANT_PROGRAM_VIEW.to_string(),
            linearizable_owner_lease: true,
            durable_monotonic_owner_epoch: false,
            authoritative_backend_time: false,
            owner_validated_checkpoint_publish: true,
            publish_checks_owner_and_latest_atomically: true,
            publish_rejects_expired_owner: true,
            latest_read_linearizable: true,
            publish_rejects_scope_mismatch: true,
            max_owner_ttl_ms: 300_000,
            control_plane_auth_enforced: false,
            production_multi_writer_safe: false,
            backend_time_source_kind: "process_clock".to_string(),
            backend_time_blocked_reason: "test_process_clock".to_string(),
            lease_authority_kind: "process_local".to_string(),
            lease_expiry_semantics: "process_clock_ttl".to_string(),
            bounded_wall_clock_failover: false,
            failover_time_bound_ms: 0,
            multi_writer_fencing_safe: false,
            production_bounded_failover_safe: false,
        };
        let error = validate_production_standing_runtime_fencing(&unsafe_capability).unwrap_err();
        assert!(error.to_string().contains("durable_monotonic_owner_epoch"));
        assert!(error.to_string().contains("authoritative_backend_time"));
        assert!(error
            .to_string()
            .contains("raft_replicated_authority_time_source"));
        assert!(error
            .to_string()
            .contains("raft_replicated_time_lease_authority"));
        assert!(error
            .to_string()
            .contains("backend_wall_clock_ttl_lease_expiry"));
        assert!(error.to_string().contains("control_plane_auth_enforced"));
        assert!(error.to_string().contains("multi_writer_fencing_safe"));
        assert!(error.to_string().contains("bounded_wall_clock_failover"));
        assert!(error.to_string().contains("failover_time_bound_ms"));
        assert!(error
            .to_string()
            .contains("production_bounded_failover_safe"));
        assert!(error.to_string().contains("production_multi_writer_safe"));

        let safe_capability = StandingRuntimeFencingCapability {
            capability_schema_version: STANDING_RUNTIME_FENCING_CAPABILITY_SCHEMA_VERSION,
            backend_name: "hiqlite".to_string(),
            owner_scope_kind: STANDING_RUNTIME_OWNER_SCOPE_KIND_TENANT_PROGRAM_VIEW.to_string(),
            linearizable_owner_lease: true,
            durable_monotonic_owner_epoch: true,
            authoritative_backend_time: true,
            owner_validated_checkpoint_publish: true,
            publish_checks_owner_and_latest_atomically: true,
            publish_rejects_expired_owner: true,
            latest_read_linearizable: true,
            publish_rejects_scope_mismatch: true,
            max_owner_ttl_ms: 300_000,
            control_plane_auth_enforced: true,
            production_multi_writer_safe: true,
            backend_time_source_kind: STANDING_RUNTIME_BACKEND_TIME_SOURCE_RAFT_REPLICATED
                .to_string(),
            backend_time_blocked_reason: String::new(),
            lease_authority_kind: "raft_replicated_time".to_string(),
            lease_expiry_semantics: "backend_wall_clock_ttl".to_string(),
            bounded_wall_clock_failover: true,
            failover_time_bound_ms: 300_000,
            multi_writer_fencing_safe: true,
            production_bounded_failover_safe: true,
        };
        validate_production_standing_runtime_fencing(&safe_capability).unwrap();
    }

    #[test]
    fn logical_standing_runtime_fencing_accepts_hiqlite_logical_clock_without_bounded_failover() {
        let capability = StandingRuntimeFencingCapability {
            capability_schema_version: STANDING_RUNTIME_FENCING_CAPABILITY_SCHEMA_VERSION,
            backend_name: "hiqlite".to_string(),
            owner_scope_kind: STANDING_RUNTIME_OWNER_SCOPE_KIND_TENANT_PROGRAM_VIEW.to_string(),
            linearizable_owner_lease: true,
            durable_monotonic_owner_epoch: true,
            authoritative_backend_time: false,
            owner_validated_checkpoint_publish: true,
            publish_checks_owner_and_latest_atomically: true,
            publish_rejects_expired_owner: true,
            latest_read_linearizable: true,
            publish_rejects_scope_mismatch: true,
            max_owner_ttl_ms: 300_000,
            control_plane_auth_enforced: true,
            production_multi_writer_safe: false,
            backend_time_source_kind: "unavailable".to_string(),
            backend_time_blocked_reason: "hiqlite_raft_replicated_authority_time_primitive_missing"
                .to_string(),
            lease_authority_kind: STANDING_RUNTIME_LEASE_AUTHORITY_KIND_HIQLITE_RAFT_SERIALIZED
                .to_string(),
            lease_expiry_semantics:
                STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_OPERATION_DRIVEN_LOGICAL.to_string(),
            bounded_wall_clock_failover: false,
            failover_time_bound_ms: 0,
            multi_writer_fencing_safe: true,
            production_bounded_failover_safe: false,
        };

        validate_logical_standing_runtime_fencing(&capability).unwrap();
        assert!(validate_production_standing_runtime_fencing(&capability).is_err());
    }
}
