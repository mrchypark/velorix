use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env,
    future::Future,
    io::Cursor,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, Context};
use arrow::{
    array::{
        new_empty_array, Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
        Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray,
        MapArray, NullArray, StringArray, StringDictionaryBuilder, StructArray,
        Time64NanosecondArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    },
    datatypes::{
        ArrowDictionaryKeyType, DataType, Field, Fields, Int16Type, Int32Type, Int64Type, Int8Type,
        Schema, TimeUnit,
    },
    record_batch::RecordBatch,
};
use async_trait::async_trait;
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
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS, NON_ALPHANUMERIC};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as AsyncMutex;
use velorix_core::{
    dbsp_view_plan::{
        validate_catalog_backed_sum_count_view_sql, validate_supported_dbsp_join_view_sql,
        SupportedDbspJoinViewPlan,
    },
    feldera_artifact::{
        catalog_input_relation_schema, feldera_artifact_bytes_hash, feldera_compile_request_hash,
        feldera_spec_hash, feldera_sql_program_for_compile_request,
        validate_feldera_compile_artifact_for_compile_request, validate_feldera_compile_request,
        ColumnSchema, FelderaCompileArtifactMetadata, FelderaCompileRequestV1,
        FelderaCompilerIdentity, FelderaRustExtensionV1, GeneratedRustIdentity,
        OutputSchemaContract, RelationSchema, SqlDataType, SqlDialect, SqlSourceKind,
        SqlStructField, StandingViewShape, StandingViewSpec, FELDERA_ARTIFACT_METADATA_VERSION,
        SUPPORTED_EPOCH_POLICY, SUPPORTED_GENERATED_RUST_ABI_VERSION, SUPPORTED_STATE_CODEC,
    },
    feldera_product_runtime::{
        validate_feldera_package_runtime_descriptor, FelderaPackageRuntimeDescriptorV1,
        FELDERA_PACKAGE_RUNTIME_EXECUTION_PATH,
    },
    generated_view_descriptor::{DynamicGeneratedViewBinding, TrustedGeneratedViewDescriptor},
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
        DurableStateRoot, EpochCommit, EpochIdempotencyKey, FelderaRuntimePackageIdentity,
        MaterializedViewPage, MaterializedViewSqlPage, NativeCodePolicy, RelationFrontier,
        RelationInputBatch, RuntimeCheckpoint, ScopedViewId, SnapshotPageRequest,
        StandingProgramIdentity, StandingProgramRuntime, StandingProgramRuntimeError, ViewFrontier,
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
        view_compile_deploy_compile_request_job_id, ViewCompileDeployJobClaimOutcome,
        ViewCompileDeployJobClaimRecord, ViewCompileDeployJobRecord, ViewCompileDeployJobRegistry,
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
    feldera_compiler_backend: Option<Arc<dyn FelderaCompilerBackend>>,
    generated_artifact_packages: Arc<Vec<GeneratedRustArtifactPackage>>,
    builtin_fixture_compile_worker_enabled: bool,
    trusted_generated_view_descriptors: Arc<Vec<TrustedGeneratedViewDescriptor>>,
    standing_runtimes: Arc<StandingRuntimeRegistry>,
    standing_runtime_factories: Arc<StandingRuntimeFactoryRegistry>,
    query_runtimes: Arc<Mutex<HashMap<String, ProductionQueryRuntime>>>,
}

type SharedStandingRuntime = Arc<Mutex<Box<dyn StandingProgramRuntime + Send>>>;

const API_PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'@')
    .add(b'&')
    .add(b'=')
    .add(b'+')
    .add(b'$')
    .add(b',');

#[derive(Clone, Default)]
struct StandingRuntimeReplayPlan {
    replay_checkpoints: Vec<ReplayCheckpoint>,
}

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

#[derive(Clone, Debug)]
pub struct FelderaCompilerBackendRequest {
    pub job_id: String,
    pub view_id: String,
    pub spec_hash: String,
    pub compile_request_hash: String,
    pub program_code: String,
    pub compiler_request: FelderaCompileRequestV1,
    pub catalogs: Vec<VelorixRelationCatalogV1>,
}

#[derive(Clone, Debug)]
pub struct FelderaCompilerBackendResponse {
    pub resolved_spec: StandingViewSpec,
    pub artifact: Option<FelderaCompileArtifactMetadata>,
    pub product_runtime: Option<FelderaPackageRuntimeDescriptorV1>,
    pub runtime_deployment: Option<FelderaPipelineManagerRuntimeDeployment>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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

impl FelderaPipelineManagerRuntimeDeploymentMode {
    fn as_checkpoint_str(self) -> &'static str {
        match self {
            FelderaPipelineManagerRuntimeDeploymentMode::LocalVolatile => "local_volatile",
            FelderaPipelineManagerRuntimeDeploymentMode::ExternalManaged => "external_managed",
        }
    }

    fn from_checkpoint_str(value: &str) -> Option<Self> {
        match value {
            "local_volatile" => Some(FelderaPipelineManagerRuntimeDeploymentMode::LocalVolatile),
            "external_managed" => {
                Some(FelderaPipelineManagerRuntimeDeploymentMode::ExternalManaged)
            }
            _ => None,
        }
    }
}

impl FelderaPipelineManagerRuntimeDeployment {
    fn supports_multi_input_activation(&self) -> bool {
        matches!(
            self.mode,
            FelderaPipelineManagerRuntimeDeploymentMode::LocalVolatile
                | FelderaPipelineManagerRuntimeDeploymentMode::ExternalManaged
        )
    }
}

#[async_trait]
pub trait FelderaCompilerBackend: Send + Sync + 'static {
    async fn compile(
        &self,
        request: FelderaCompilerBackendRequest,
    ) -> Result<FelderaCompilerBackendResponse, ApiError>;
}

#[derive(Clone)]
pub struct FelderaPipelineManagerCompilerBackend {
    client: reqwest::Client,
    base_url: String,
    bearer_token: Option<Arc<str>>,
    poll_interval: Duration,
    poll_timeout: Duration,
    program_profile: String,
    workers: u32,
    runtime_deployment_mode: Option<FelderaPipelineManagerRuntimeDeploymentMode>,
}

const FELDERA_PIPELINE_MANAGER_RUNTIME_PACKAGE_NAME: &str = "feldera-pipeline-manager-runtime";
const FELDERA_PIPELINE_MANAGER_RUNTIME_PACKAGE_VERSION: &str =
    "feldera-pipeline-manager-runtime-v1";
const FELDERA_PIPELINE_MANAGER_STATE_CODEC: &str = "feldera-pipeline-manager-state-v2";
const FELDERA_PIPELINE_MANAGER_LOCAL_CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
const FELDERA_COMPILER_SQL_COMPILED_STALL_TIMEOUT: Duration = Duration::from_secs(120);
const FELDERA_COMPILER_SCHEMA_TIMEOUT_DEFAULT_MS: u64 = 120_000;
const FELDERA_COMPILER_RUNTIME_TIMEOUT_DEFAULT_MS: u64 = 3_600_000;

impl FelderaPipelineManagerCompilerBackend {
    pub fn new(
        base_url: impl Into<String>,
        bearer_token: Option<String>,
        poll_interval: Duration,
        poll_timeout: Duration,
        program_profile: impl Into<String>,
        workers: u32,
    ) -> Result<Self, ApiError> {
        let base_url = base_url.into();
        let base_url = base_url.trim().trim_end_matches('/').to_string();
        if base_url.is_empty() {
            return Err(ApiError::bad_request("Feldera base URL must not be empty"));
        }
        if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
            return Err(ApiError::bad_request(
                "Feldera base URL must start with http:// or https://",
            ));
        }
        if poll_interval.is_zero() {
            return Err(ApiError::bad_request(
                "Feldera compiler poll interval must be greater than zero",
            ));
        }
        if poll_timeout < poll_interval {
            return Err(ApiError::bad_request(
                "Feldera compiler poll timeout must be greater than or equal to poll interval",
            ));
        }
        if workers == 0 {
            return Err(ApiError::bad_request(
                "Feldera compiler workers must be greater than zero",
            ));
        }
        let program_profile = program_profile.into();
        if program_profile.trim().is_empty() {
            return Err(ApiError::bad_request(
                "Feldera compiler profile must not be empty",
            ));
        }
        let bearer_token = match bearer_token {
            Some(token) if !token.trim().is_empty() => Some(Arc::<str>::from(token)),
            Some(_) => {
                return Err(ApiError::bad_request(
                    "Feldera bearer token must not be empty",
                ));
            }
            None => None,
        };
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(poll_timeout)
                .build()
                .map_err(ApiError::internal)?,
            base_url,
            bearer_token,
            poll_interval,
            poll_timeout,
            program_profile,
            workers,
            runtime_deployment_mode: None,
        })
    }

    pub fn with_volatile_runtime_deployment(mut self) -> Self {
        self.runtime_deployment_mode =
            Some(FelderaPipelineManagerRuntimeDeploymentMode::LocalVolatile);
        self
    }

    pub fn with_runtime_deployment_mode(
        mut self,
        mode: FelderaPipelineManagerRuntimeDeploymentMode,
    ) -> Self {
        self.runtime_deployment_mode = Some(mode);
        self
    }

    fn request(&self, method: reqwest::Method, url: String) -> reqwest::RequestBuilder {
        let request = self.client.request(method, url);
        match self.bearer_token.as_deref() {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    fn pipeline_url(&self, pipeline_name: &str) -> String {
        format!("{}/v0/pipelines/{}", self.base_url, pipeline_name)
    }
}

#[async_trait]
impl FelderaCompilerBackend for FelderaPipelineManagerCompilerBackend {
    async fn compile(
        &self,
        request: FelderaCompilerBackendRequest,
    ) -> Result<FelderaCompilerBackendResponse, ApiError> {
        let pipeline_name = feldera_pipeline_name_for_compile_request(&request);
        let pipeline_url = self.pipeline_url(&pipeline_name);
        let compiler_request = feldera_pipeline_manager_sql_compile_request(
            &request.compiler_request,
            &request.catalogs,
        )?;
        let program_code = feldera_sql_program_for_compile_request(&compiler_request)
            .map_err(|error| ApiError::bad_request(error.to_string()))?;
        let mut body = json!({
            "name": pipeline_name.clone(),
            "description": format!("Velorix compile validation for {}", request.view_id),
            "runtime_config": {
                "workers": self.workers
            },
            "program_config": {
                "profile": self.program_profile,
                "cache": true
            },
            "program_code": program_code
        });
        if let Some(udf_rust) = compiler_request.rust_extension.udf_rust.as_ref() {
            body["udf_rust"] = json!(udf_rust);
        }
        if let Some(udf_toml) = compiler_request.rust_extension.udf_toml.as_ref() {
            body["udf_toml"] = json!(udf_toml);
        }

        let response = self
            .request(reqwest::Method::PUT, pipeline_url.clone())
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                ApiError::service_unavailable(format!(
                    "Feldera pipeline create/update failed: {error}"
                ))
            })?;
        if !response.status().is_success() {
            return Err(feldera_http_error("Feldera pipeline create/update", response).await);
        }

        let deadline = tokio::time::Instant::now() + self.poll_timeout;
        let mut sql_compiled_stall_started_at = None;
        loop {
            let response = self
                .request(reqwest::Method::GET, pipeline_url.clone())
                .send()
                .await
                .map_err(|error| {
                    ApiError::service_unavailable(format!(
                        "Feldera pipeline status poll failed: {error}"
                    ))
                })?;
            if !response.status().is_success() {
                return Err(feldera_http_error("Feldera pipeline status poll", response).await);
            }
            let pipeline: FelderaPipelineStatusResponse =
                response.json().await.map_err(|error| {
                    ApiError::service_unavailable(format!(
                        "Feldera pipeline status response is not valid JSON: {error}"
                    ))
                })?;
            if let Some(warning) = feldera_semantic_warning_summary(pipeline.program_error.as_ref())
            {
                return Err(ApiError::bad_request(format!(
                    "Feldera compiler returned unsupported semantic warning for view `{}`: {warning}",
                    request.view_id
                )));
            }
            let now = tokio::time::Instant::now();
            if self.runtime_deployment_mode.is_some()
                && feldera_pipeline_sql_compiled_without_runtime_artifact(&pipeline)
            {
                let started_at = *sql_compiled_stall_started_at.get_or_insert(now);
                if now.duration_since(started_at)
                    >= feldera_sql_compiled_stall_timeout(self.poll_timeout)
                {
                    return Err(ApiError::service_unavailable(format!(
                        "Feldera compiler stalled after SQL compilation for view `{}`; runtime artifact was not produced and deployment_status={}",
                        request.view_id,
                        pipeline
                            .deployment_status
                            .as_deref()
                            .unwrap_or("unknown")
                    )));
                }
            } else {
                sql_compiled_stall_started_at = None;
            }
            match pipeline.program_status.as_str() {
                "Success" => {
                    return feldera_compiler_backend_response_from_pipeline(
                        &request,
                        &pipeline,
                        pipeline_name,
                        self.runtime_deployment_mode,
                    );
                }
                "SqlError" | "RustError" | "SystemError" => {
                    return Err(ApiError::bad_request(format!(
                        "Feldera compiler returned {} for view `{}`: {}",
                        pipeline.program_status,
                        request.view_id,
                        feldera_program_error_summary(pipeline.program_error.as_ref())
                    )));
                }
                "SqlCompiled" | "CompilingRust" if self.runtime_deployment_mode.is_none() => {
                    return feldera_compiler_backend_response_from_pipeline(
                        &request,
                        &pipeline,
                        pipeline_name,
                        None,
                    );
                }
                "Pending" | "CompilingSql" | "SqlCompiled" | "CompilingRust" => {
                    if now >= deadline {
                        return Err(ApiError::service_unavailable(format!(
                            "Feldera compiler timed out waiting for `{}` to compile; last program_status={}",
                            request.view_id, pipeline.program_status
                        )));
                    }
                    tokio::time::sleep(self.poll_interval).await;
                }
                other => {
                    return Err(ApiError::service_unavailable(format!(
                        "Feldera pipeline status response contains unknown program_status `{other}`"
                    )));
                }
            }
        }
    }
}

fn feldera_compiler_backend_response_from_pipeline(
    request: &FelderaCompilerBackendRequest,
    pipeline: &FelderaPipelineStatusResponse,
    pipeline_name: String,
    runtime_deployment_mode: Option<FelderaPipelineManagerRuntimeDeploymentMode>,
) -> Result<FelderaCompilerBackendResponse, ApiError> {
    if let Some(warning) = feldera_semantic_warning_summary(pipeline.program_error.as_ref()) {
        return Err(ApiError::bad_request(format!(
            "Feldera compiler returned unsupported semantic warning for view `{}`: {warning}",
            request.view_id
        )));
    }
    validate_feldera_program_info_admission(
        &request.compiler_request,
        pipeline.program_info.as_ref(),
    )?;
    let outputs = feldera_output_schemas_from_program_info(
        request.view_id.as_str(),
        pipeline.program_version,
        pipeline.program_info.as_ref(),
        request.compiler_request.shape.multi_output,
    )?;
    let mut resolved_spec = standing_view_spec_for_compile_request(&request.compiler_request);
    resolved_spec.output_relations = outputs;
    resolved_spec.shape.multi_output = resolved_spec.output_relations.len() > 1;
    let runtime_deployment =
        runtime_deployment_mode.map(|mode| FelderaPipelineManagerRuntimeDeployment {
            pipeline_name,
            mode,
        });
    Ok(FelderaCompilerBackendResponse {
        resolved_spec,
        artifact: None,
        product_runtime: None,
        runtime_deployment,
    })
}

impl StandingProgramRuntimeFactory for FelderaPipelineManagerCompilerBackend {
    fn create(
        &self,
        _identity: &StandingProgramIdentity,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Err("Feldera pipeline-manager runtime requires view spec and schemas".to_string())
    }

    fn create_with_catalogs_and_spec(
        &self,
        identity: &StandingProgramIdentity,
        catalogs: &[VelorixRelationCatalogV1],
        spec: &StandingViewSpec,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        let pipeline_name =
            feldera_pipeline_name_for_view_spec(spec).map_err(|error| error.to_string())?;
        let runtime_deployment_mode = self.runtime_deployment_mode.ok_or_else(|| {
            "Feldera pipeline-manager runtime deployment mode is not configured".to_string()
        })?;
        let runtime = FelderaPipelineManagerStandingRuntime::new(
            identity.clone(),
            input_schemas.to_vec(),
            output_schemas.to_vec(),
            catalogs.to_vec(),
            self.base_url.clone(),
            self.bearer_token.clone(),
            pipeline_name,
            runtime_deployment_mode,
            self.poll_timeout,
        )?;
        Ok(Box::new(runtime))
    }

    fn restore(
        &self,
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Err(format!(
            "Feldera pipeline-manager runtime restore requires active view metadata; checkpoint epoch={}",
            checkpoint.logical_epoch
        ))
    }

    fn restore_with_catalogs_and_spec(
        &self,
        checkpoint: RuntimeCheckpoint,
        catalogs: &[VelorixRelationCatalogV1],
        spec: &StandingViewSpec,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        let expected_pipeline_name =
            feldera_pipeline_name_for_view_spec(spec).map_err(|error| error.to_string())?;
        let runtime_deployment_mode = self.runtime_deployment_mode.ok_or_else(|| {
            "Feldera pipeline-manager runtime deployment mode is not configured".to_string()
        })?;
        let runtime = FelderaPipelineManagerStandingRuntime::restore_with_metadata(
            checkpoint,
            input_schemas.to_vec(),
            output_schemas.to_vec(),
            catalogs.to_vec(),
            self.base_url.clone(),
            self.bearer_token.clone(),
            expected_pipeline_name,
            runtime_deployment_mode,
            self.poll_timeout,
        )?;
        Ok(Box::new(runtime))
    }
}

#[derive(Debug, Deserialize)]
struct FelderaPipelineManagerCheckpointPayload {
    pipeline_name: String,
    logical_epoch: u64,
    deployment_mode: String,
    #[serde(default)]
    applied_idempotency: HashMap<String, u64>,
}

struct FelderaPipelineManagerStandingRuntime {
    identity: StandingProgramIdentity,
    input_schemas: Vec<RelationSchema>,
    output_schemas: Vec<RelationSchema>,
    input_relation_names: HashMap<String, String>,
    input_weight_column_names: HashMap<String, String>,
    input_delete_capable_relation_ids: BTreeSet<String>,
    input_catalogs: HashMap<String, VelorixRelationCatalogV1>,
    base_url: String,
    bearer_token: Option<Arc<str>>,
    pipeline_name: String,
    runtime_deployment_mode: FelderaPipelineManagerRuntimeDeploymentMode,
    logical_epoch: u64,
    applied_idempotency: HashMap<String, u64>,
    input_frontiers: BTreeMap<(String, String), u64>,
    poisoned_reason: Option<String>,
    timeout: Duration,
    cleanup_on_drop: bool,
}

impl FelderaPipelineManagerStandingRuntime {
    fn new(
        identity: StandingProgramIdentity,
        input_schemas: Vec<RelationSchema>,
        output_schemas: Vec<RelationSchema>,
        catalogs: Vec<VelorixRelationCatalogV1>,
        base_url: String,
        bearer_token: Option<Arc<str>>,
        pipeline_name: String,
        runtime_deployment_mode: FelderaPipelineManagerRuntimeDeploymentMode,
        timeout: Duration,
    ) -> Result<Self, String> {
        identity.validate().map_err(|error| error.to_string())?;
        let (input_relation_names, input_weight_column_names, input_delete_capable_relation_ids) =
            validate_feldera_pipeline_manager_runtime_catalogs(&catalogs)?;
        let runtime = Self {
            identity,
            input_schemas,
            output_schemas,
            input_relation_names,
            input_weight_column_names,
            input_delete_capable_relation_ids,
            input_catalogs: input_catalog_map(catalogs),
            base_url,
            bearer_token,
            pipeline_name,
            runtime_deployment_mode,
            logical_epoch: 0,
            applied_idempotency: HashMap::new(),
            input_frontiers: BTreeMap::new(),
            poisoned_reason: None,
            timeout,
            cleanup_on_drop: runtime_deployment_mode
                == FelderaPipelineManagerRuntimeDeploymentMode::LocalVolatile,
        };
        runtime.start_pipeline()?;
        Ok(runtime)
    }

    fn restore_with_metadata(
        checkpoint: RuntimeCheckpoint,
        input_schemas: Vec<RelationSchema>,
        output_schemas: Vec<RelationSchema>,
        catalogs: Vec<VelorixRelationCatalogV1>,
        base_url: String,
        bearer_token: Option<Arc<str>>,
        expected_pipeline_name: String,
        runtime_deployment_mode: FelderaPipelineManagerRuntimeDeploymentMode,
        timeout: Duration,
    ) -> Result<Self, String> {
        if checkpoint.checkpoint_codec_identity != FELDERA_PIPELINE_MANAGER_STATE_CODEC {
            return Err(format!(
                "Feldera pipeline-manager checkpoint codec mismatch: expected `{}`, found `{}`",
                FELDERA_PIPELINE_MANAGER_STATE_CODEC, checkpoint.checkpoint_codec_identity
            ));
        }
        let payload = checkpoint.state_payload.as_ref().ok_or_else(|| {
            "Feldera pipeline-manager checkpoint is missing state payload".to_string()
        })?;
        if payload.codec_identity != FELDERA_PIPELINE_MANAGER_STATE_CODEC {
            return Err(format!(
                "Feldera pipeline-manager checkpoint payload codec mismatch: expected `{}`, found `{}`",
                FELDERA_PIPELINE_MANAGER_STATE_CODEC, payload.codec_identity
            ));
        }
        let parsed: FelderaPipelineManagerCheckpointPayload =
            serde_json::from_str(&payload.payload).map_err(|error| {
                format!("Feldera pipeline-manager checkpoint payload is invalid: {error}")
            })?;
        if parsed.pipeline_name != expected_pipeline_name {
            return Err(format!(
                "Feldera pipeline-manager checkpoint pipeline mismatch: expected `{expected_pipeline_name}`, found `{}`",
                parsed.pipeline_name
            ));
        }
        if parsed.logical_epoch != checkpoint.logical_epoch {
            return Err(format!(
                "Feldera pipeline-manager checkpoint epoch mismatch: key/body epoch={} payload epoch={}",
                checkpoint.logical_epoch, parsed.logical_epoch
            ));
        }
        let checkpoint_mode = FelderaPipelineManagerRuntimeDeploymentMode::from_checkpoint_str(
            parsed.deployment_mode.as_str(),
        )
        .ok_or_else(|| {
            format!(
                "Feldera pipeline-manager checkpoint deployment_mode `{}` is not supported",
                parsed.deployment_mode
            )
        })?;
        if checkpoint_mode != runtime_deployment_mode {
            return Err(format!(
                "Feldera pipeline-manager checkpoint deployment mode mismatch: expected `{}`, found `{}`",
                runtime_deployment_mode.as_checkpoint_str(),
                parsed.deployment_mode
            ));
        }
        let (input_relation_names, input_weight_column_names, input_delete_capable_relation_ids) =
            validate_feldera_pipeline_manager_runtime_catalogs(&catalogs)?;
        let runtime = Self {
            identity: checkpoint.identity,
            input_schemas,
            output_schemas,
            input_relation_names,
            input_weight_column_names,
            input_delete_capable_relation_ids,
            input_catalogs: input_catalog_map(catalogs),
            base_url,
            bearer_token,
            pipeline_name: parsed.pipeline_name,
            runtime_deployment_mode,
            logical_epoch: checkpoint.logical_epoch,
            applied_idempotency: parsed.applied_idempotency,
            input_frontiers: checkpoint
                .input_frontiers
                .iter()
                .map(|frontier| {
                    (
                        (
                            frontier.relation_id.clone(),
                            frontier.relation_version.clone(),
                        ),
                        frontier.committed_offset_exclusive,
                    )
                })
                .collect(),
            poisoned_reason: None,
            timeout,
            cleanup_on_drop: runtime_deployment_mode
                == FelderaPipelineManagerRuntimeDeploymentMode::LocalVolatile,
        };
        runtime
            .identity
            .validate()
            .map_err(|error| error.to_string())?;
        runtime.start_pipeline()?;
        Ok(runtime)
    }

    fn pipeline_url(&self, suffix: &str) -> String {
        format!(
            "{}/v0/pipelines/{}{}",
            self.base_url, self.pipeline_name, suffix
        )
    }

    fn start_pipeline(&self) -> Result<(), String> {
        let start_url = self.pipeline_url("/start?initial=running");
        let status_url = self.pipeline_url("");
        let bearer_token = self.bearer_token.clone();
        let timeout = self.timeout;
        let pipeline_name = self.pipeline_name.clone();
        run_feldera_runtime_http(timeout, move |client| async move {
            let response = feldera_runtime_request(
                &client,
                bearer_token.as_deref(),
                reqwest::Method::POST,
                start_url,
            )
            .send()
            .await
            .map_err(|error| format!("Feldera pipeline start failed: {error}"))?;
            if !response.status().is_success() {
                return Err(feldera_runtime_http_error("Feldera pipeline start", response).await);
            }

            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                let response = feldera_runtime_request(
                    &client,
                    bearer_token.as_deref(),
                    reqwest::Method::GET,
                    status_url.clone(),
                )
                .send()
                .await
                .map_err(|error| format!("Feldera pipeline status poll failed: {error}"))?;
                if !response.status().is_success() {
                    return Err(feldera_runtime_http_error(
                        "Feldera pipeline status poll",
                        response,
                    )
                    .await);
                }
                let value = response.json::<Value>().await.map_err(|error| {
                    format!("Feldera pipeline status response is invalid: {error}")
                })?;
                let resources = value
                    .get("deployment_resources_status")
                    .and_then(Value::as_str);
                let deployment = value.get("deployment_status").and_then(Value::as_str);
                if resources.is_none()
                    || resources == Some("Provisioned")
                    || matches!(deployment, Some("Running" | "Paused"))
                {
                    return Ok(());
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!(
                        "Feldera pipeline `{pipeline_name}` did not become provisioned before timeout"
                    ));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    }

    fn start_transaction(&self) -> Result<(), String> {
        let transaction_url = self.pipeline_url("/start_transaction");
        let bearer_token = self.bearer_token.clone();
        run_feldera_runtime_http(self.timeout, move |client| async move {
            let response = feldera_runtime_request(
                &client,
                bearer_token.as_deref(),
                reqwest::Method::POST,
                transaction_url,
            )
            .send()
            .await
            .map_err(|error| format!("Feldera transaction start failed: {error}"))?;
            if !response.status().is_success() {
                return Err(
                    feldera_runtime_http_error("Feldera transaction start", response).await,
                );
            }
            Ok(())
        })
    }

    fn commit_transaction_and_wait(&self) -> Result<(), String> {
        let commit_url = self.pipeline_url("/commit_transaction");
        let stats_url = self.pipeline_url("/stats");
        let bearer_token = self.bearer_token.clone();
        let timeout = self.timeout;
        let pipeline_name = self.pipeline_name.clone();
        run_feldera_runtime_http(timeout, move |client| async move {
            let response = feldera_runtime_request(
                &client,
                bearer_token.as_deref(),
                reqwest::Method::POST,
                commit_url,
            )
            .send()
            .await
            .map_err(|error| format!("Feldera transaction commit failed: {error}"))?;
            if !response.status().is_success() {
                return Err(
                    feldera_runtime_http_error("Feldera transaction commit", response).await,
                );
            }

            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                let response = feldera_runtime_request(
                    &client,
                    bearer_token.as_deref(),
                    reqwest::Method::GET,
                    stats_url.clone(),
                )
                .send()
                .await
                .map_err(|error| format!("Feldera transaction status poll failed: {error}"))?;
                if !response.status().is_success() {
                    return Err(feldera_runtime_http_error(
                        "Feldera transaction status poll",
                        response,
                    )
                    .await);
                }
                let value = response.json::<Value>().await.map_err(|error| {
                    format!("Feldera transaction status response is invalid: {error}")
                })?;
                let status = value
                    .get("global_metrics")
                    .and_then(|metrics| metrics.get("transaction_status"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        "Feldera transaction status response is missing global_metrics.transaction_status"
                            .to_string()
                    })?;
                if status == "NoTransaction" {
                    return Ok(());
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!(
                        "Feldera pipeline `{pipeline_name}` transaction commit did not finish before timeout; last transaction_status={status}"
                    ));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    }

    fn ingest_relation_batch(&self, input: &RelationInputBatch) -> Result<(), String> {
        let table_name = self
            .input_relation_names
            .get(&input.relation_id)
            .ok_or_else(|| format!("unknown Feldera input relation `{}`", input.relation_id))?;
        let catalog = self.input_catalogs.get(&input.relation_id).ok_or_else(|| {
            format!(
                "missing catalog for Feldera input relation `{}`",
                input.relation_id
            )
        })?;
        let rows = record_batches_to_feldera_ingress_json_rows_for_catalog(catalog, &input.batches)
            .map_err(|error| error.to_string())?;
        let weight_column = self
            .input_weight_column_names
            .get(&input.relation_id)
            .ok_or_else(|| {
                format!(
                    "missing weight column for Feldera input relation `{}`",
                    input.relation_id
                )
            })?;
        let allow_delete = self
            .input_delete_capable_relation_ids
            .contains(&input.relation_id);
        let events =
            feldera_pipeline_manager_insert_delete_events(weight_column, allow_delete, rows)?;
        let table_name = utf8_percent_encode(table_name, NON_ALPHANUMERIC).to_string();
        let ingress_url = self.pipeline_url(&format!(
            "/ingress/{table_name}?format=json&update_format=insert_delete&array=true"
        ));
        let bearer_token = self.bearer_token.clone();
        run_feldera_runtime_http(self.timeout, move |client| async move {
            let response = feldera_runtime_request(
                &client,
                bearer_token.as_deref(),
                reqwest::Method::POST,
                ingress_url,
            )
            .json(&events)
            .send()
            .await
            .map_err(|error| format!("Feldera ingress failed: {error}"))?;
            if !response.status().is_success() {
                return Err(feldera_runtime_http_error("Feldera ingress", response).await);
            }
            Ok(())
        })
    }

    fn ensure_not_poisoned(&self) -> Result<(), StandingProgramRuntimeError> {
        if let Some(reason) = &self.poisoned_reason {
            return Err(StandingProgramRuntimeError::ExternalRuntime {
                reason: format!(
                    "Feldera pipeline-manager runtime is poisoned after a failed Feldera ingress; rebuild or replay the runtime before serving queries: {reason}"
                ),
            });
        }
        Ok(())
    }

    fn query_sql_rows(
        &self,
        sql: String,
        output_schema: Option<&RelationSchema>,
    ) -> Result<Vec<Value>, String> {
        let query_url = self.pipeline_url("/query");
        let bearer_token = self.bearer_token.clone();
        let output_column_names = output_schema.map(|schema| {
            schema
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<BTreeSet<_>>()
        });
        run_feldera_runtime_http(self.timeout, move |client| async move {
            let response = feldera_runtime_request(
                &client,
                bearer_token.as_deref(),
                reqwest::Method::GET,
                query_url,
            )
            .query(&[("sql", sql.as_str()), ("format", "json")])
            .send()
            .await
            .map_err(|error| format!("Feldera query failed: {error}"))?;
            if !response.status().is_success() {
                return Err(feldera_runtime_http_error("Feldera query", response).await);
            }
            let text = response
                .text()
                .await
                .map_err(|error| format!("Feldera query response read failed: {error}"))?;
            feldera_query_rows_from_text(&text, output_column_names.as_ref())
        })
    }

    fn cleanup_local_volatile_pipeline(&self) -> Result<(), String> {
        let pipeline_url = self.pipeline_url("");
        let stop_url = self.pipeline_url("/stop?force=true");
        let clear_url = self.pipeline_url("/clear");
        let bearer_token = self.bearer_token.clone();
        run_feldera_runtime_http(
            FELDERA_PIPELINE_MANAGER_LOCAL_CLEANUP_TIMEOUT,
            move |client| async move {
                let response = feldera_runtime_request(
                    &client,
                    bearer_token.as_deref(),
                    reqwest::Method::GET,
                    pipeline_url.clone(),
                )
                .send()
                .await
                .map_err(|error| format!("Feldera cleanup status poll failed: {error}"))?;
                if response.status() == reqwest::StatusCode::NOT_FOUND {
                    return Ok(());
                }
                if !response.status().is_success() {
                    return Err(feldera_runtime_http_error(
                        "Feldera cleanup status poll",
                        response,
                    )
                    .await);
                }
                let value = response.json::<Value>().await.map_err(|error| {
                    format!("Feldera cleanup status response is invalid: {error}")
                })?;
                let mut deployment = value
                    .get("deployment_status")
                    .and_then(Value::as_str)
                    .unwrap_or("Unknown")
                    .to_string();

                if deployment != "Stopped" {
                    let response = feldera_runtime_request(
                        &client,
                        bearer_token.as_deref(),
                        reqwest::Method::POST,
                        stop_url,
                    )
                    .send()
                    .await
                    .map_err(|error| format!("Feldera cleanup force-stop failed: {error}"))?;
                    if response.status() == reqwest::StatusCode::NOT_FOUND {
                        return Ok(());
                    }
                    if !response.status().is_success()
                        && response.status() != reqwest::StatusCode::SERVICE_UNAVAILABLE
                    {
                        return Err(feldera_runtime_http_error(
                            "Feldera cleanup force-stop",
                            response,
                        )
                        .await);
                    }
                }

                let deadline =
                    tokio::time::Instant::now() + FELDERA_PIPELINE_MANAGER_LOCAL_CLEANUP_TIMEOUT;
                while deployment != "Stopped" {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(format!(
                            "Feldera cleanup timed out waiting for pipeline to stop; last deployment_status={deployment}"
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let response = feldera_runtime_request(
                        &client,
                        bearer_token.as_deref(),
                        reqwest::Method::GET,
                        pipeline_url.clone(),
                    )
                    .send()
                    .await
                    .map_err(|error| format!("Feldera cleanup status poll failed: {error}"))?;
                    if response.status() == reqwest::StatusCode::NOT_FOUND {
                        return Ok(());
                    }
                    if !response.status().is_success() {
                        return Err(feldera_runtime_http_error(
                            "Feldera cleanup status poll",
                            response,
                        )
                        .await);
                    }
                    let value = response.json::<Value>().await.map_err(|error| {
                        format!("Feldera cleanup status response is invalid: {error}")
                    })?;
                    deployment = value
                        .get("deployment_status")
                        .and_then(Value::as_str)
                        .unwrap_or("Unknown")
                        .to_string();
                }

                let response = feldera_runtime_request(
                    &client,
                    bearer_token.as_deref(),
                    reqwest::Method::POST,
                    clear_url,
                )
                .send()
                .await
                .map_err(|error| format!("Feldera cleanup clear failed: {error}"))?;
                if response.status() == reqwest::StatusCode::NOT_FOUND {
                    return Ok(());
                }
                if !response.status().is_success() {
                    return Err(
                        feldera_runtime_http_error("Feldera cleanup clear", response).await,
                    );
                }

                let deadline =
                    tokio::time::Instant::now() + FELDERA_PIPELINE_MANAGER_LOCAL_CLEANUP_TIMEOUT;
                loop {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(
                            "Feldera cleanup timed out waiting for pipeline storage to clear"
                                .to_string(),
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let response = feldera_runtime_request(
                        &client,
                        bearer_token.as_deref(),
                        reqwest::Method::GET,
                        pipeline_url.clone(),
                    )
                    .send()
                    .await
                    .map_err(|error| format!("Feldera cleanup clear poll failed: {error}"))?;
                    if response.status() == reqwest::StatusCode::NOT_FOUND {
                        return Ok(());
                    }
                    if !response.status().is_success() {
                        return Err(feldera_runtime_http_error(
                            "Feldera cleanup clear poll",
                            response,
                        )
                        .await);
                    }
                    let value = response.json::<Value>().await.map_err(|error| {
                        format!("Feldera cleanup clear response is invalid: {error}")
                    })?;
                    if value
                        .get("storage_status")
                        .and_then(Value::as_str)
                        .is_some_and(|status| status == "Cleared")
                    {
                        break;
                    }
                }

                let response = feldera_runtime_request(
                    &client,
                    bearer_token.as_deref(),
                    reqwest::Method::DELETE,
                    pipeline_url,
                )
                .send()
                .await
                .map_err(|error| format!("Feldera cleanup delete failed: {error}"))?;
                if response.status() == reqwest::StatusCode::NOT_FOUND
                    || response.status().is_success()
                {
                    return Ok(());
                }
                Err(feldera_runtime_http_error("Feldera cleanup delete", response).await)
            },
        )
    }
}

impl Drop for FelderaPipelineManagerStandingRuntime {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            let _ = self.cleanup_local_volatile_pipeline();
        }
    }
}

fn validate_feldera_pipeline_manager_runtime_catalogs(
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<
    (
        HashMap<String, String>,
        HashMap<String, String>,
        BTreeSet<String>,
    ),
    String,
> {
    let mut input_relation_names = HashMap::new();
    let mut input_weight_column_names = HashMap::new();
    let mut input_delete_capable_relation_ids = BTreeSet::new();
    for catalog in catalogs {
        let delete_capable = validate_feldera_pipeline_manager_relation_operations(catalog)?;
        let weight_column = catalog
            .relation_schema
            .columns
            .iter()
            .find(|column| column.column_id == catalog.relation_schema.weight_column_id)
            .ok_or_else(|| {
                format!(
                    "relation `{}` weight column `{}` is missing",
                    catalog.relation_schema.relation_id, catalog.relation_schema.weight_column_id
                )
            })?;
        if !matches!(
            weight_column.physical_arrow_type,
            ArrowPhysicalTypeV1::Int64
        ) {
            return Err(format!(
                "Feldera pipeline-manager runtime requires Int64 weight column for relation `{}`",
                catalog.relation_schema.relation_id
            ));
        }
        if !matches!(weight_column.logical_type, VelorixLogicalTypeV1::Int64) {
            return Err(format!(
                "Feldera pipeline-manager runtime requires Int64 logical weight column for relation `{}`",
                catalog.relation_schema.relation_id
            ));
        }
        if weight_column.nullable {
            return Err(format!(
                "Feldera pipeline-manager runtime requires non-null weight column for relation `{}`",
                catalog.relation_schema.relation_id
            ));
        }
        if catalog
            .relation_schema
            .primary_key_column_ids
            .iter()
            .any(|column_id| column_id == &catalog.relation_schema.weight_column_id)
        {
            return Err(format!(
                "Feldera pipeline-manager runtime does not allow the weight column in the primary key for relation `{}`",
                catalog.relation_schema.relation_id
            ));
        }
        input_relation_names.insert(
            catalog.relation_schema.relation_id.clone(),
            catalog.relation_schema.relation_name.clone(),
        );
        input_weight_column_names.insert(
            catalog.relation_schema.relation_id.clone(),
            weight_column.name.clone(),
        );
        if delete_capable {
            input_delete_capable_relation_ids.insert(catalog.relation_schema.relation_id.clone());
        }
    }

    Ok((
        input_relation_names,
        input_weight_column_names,
        input_delete_capable_relation_ids,
    ))
}

fn input_catalog_map(
    catalogs: Vec<VelorixRelationCatalogV1>,
) -> HashMap<String, VelorixRelationCatalogV1> {
    catalogs
        .into_iter()
        .map(|catalog| (catalog.relation_schema.relation_id.clone(), catalog))
        .collect()
}

fn validate_feldera_pipeline_manager_relation_operations(
    catalog: &VelorixRelationCatalogV1,
) -> Result<bool, String> {
    let mut insert = false;
    let mut delete = false;
    let mut update = false;
    let mut upsert = false;
    for operation in &catalog.relation_schema.allowed_operations {
        match operation {
            RelationOperationV1::Insert if insert => {
                return Err(format!(
                    "Feldera pipeline-manager runtime rejects duplicate Insert relation operation; relation `{}` declares {:?}",
                    catalog.relation_schema.relation_id,
                    catalog.relation_schema.allowed_operations
                ));
            }
            RelationOperationV1::Insert => insert = true,
            RelationOperationV1::Delete if delete => {
                return Err(format!(
                    "Feldera pipeline-manager runtime rejects duplicate Delete relation operation; relation `{}` declares {:?}",
                    catalog.relation_schema.relation_id,
                    catalog.relation_schema.allowed_operations
                ));
            }
            RelationOperationV1::Delete => delete = true,
            RelationOperationV1::Update if update => {
                return Err(format!(
                    "Feldera pipeline-manager runtime rejects duplicate Update relation operation; relation `{}` declares {:?}",
                    catalog.relation_schema.relation_id,
                    catalog.relation_schema.allowed_operations
                ));
            }
            RelationOperationV1::Update => update = true,
            RelationOperationV1::Upsert if upsert => {
                return Err(format!(
                    "Feldera pipeline-manager runtime rejects duplicate Upsert relation operation; relation `{}` declares {:?}",
                    catalog.relation_schema.relation_id,
                    catalog.relation_schema.allowed_operations
                ));
            }
            RelationOperationV1::Upsert => upsert = true,
        }
    }
    if !insert {
        return Err(format!(
            "Feldera pipeline-manager runtime requires Insert relation operation; relation `{}` declares {:?}",
            catalog.relation_schema.relation_id,
            catalog.relation_schema.allowed_operations
        ));
    }
    Ok(delete || update || upsert)
}

fn feldera_pipeline_manager_insert_delete_events(
    weight_column: &str,
    allow_delete: bool,
    rows: Vec<Value>,
) -> Result<Vec<Value>, String> {
    let mut events = Vec::with_capacity(rows.len());
    for (index, row) in rows.into_iter().enumerate() {
        let mut data = row.as_object().cloned().ok_or_else(|| {
            format!("Feldera input row {index} must be a JSON object before ingress")
        })?;
        let weight = data
            .get(weight_column)
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                format!(
                    "Feldera input row {index} is missing Int64 weight column `{weight_column}`"
                )
            })?;
        data.remove(weight_column);
        let data = Value::Object(data);
        match weight {
            1 => events.push(json!({ "insert": data })),
            -1 if allow_delete => events.push(json!({ "delete": data })),
            -1 => {
                return Err(format!(
                    "Feldera pipeline-manager runtime received delete weight `{weight_column}` = -1 for an insert-only relation at row {index}"
                ));
            }
            _ => {
                return Err(format!(
                    "Feldera pipeline-manager runtime currently supports only signed unit weights with `{weight_column}` = 1 or -1; row {index} has {weight}"
                ));
            }
        }
    }

    Ok(events)
}

impl StandingProgramRuntime for FelderaPipelineManagerStandingRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        self.input_schemas.clone()
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        self.output_schemas.clone()
    }

    fn logical_epoch(&self) -> u64 {
        self.logical_epoch
    }

    fn apply_changes(
        &mut self,
        logical_epoch: u64,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        self.ensure_not_poisoned()?;
        if logical_epoch <= self.logical_epoch {
            return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                current: self.logical_epoch,
                attempted: logical_epoch,
            });
        }
        if let Some(first_epoch) = self.applied_idempotency.get(idempotency_key.as_str()) {
            return Err(StandingProgramRuntimeError::IdempotencyKeyConflict {
                idempotency_key: idempotency_key.as_str().to_string(),
                first_epoch: *first_epoch,
                attempted_epoch: logical_epoch,
            });
        }
        let transactional = input_changes.len() > 1;
        if transactional {
            if let Err(reason) = self.start_transaction() {
                let reason = format!(
                    "failed to start Feldera transaction for multi-input epoch {logical_epoch}: {reason}"
                );
                self.poisoned_reason = Some(reason.clone());
                return Err(StandingProgramRuntimeError::ExternalRuntime { reason });
            }
        }
        for input in &input_changes {
            if let Err(reason) = self.ingest_relation_batch(input) {
                let reason = format!(
                    "relation `{}` version `{}` failed at offsets {}..{}: {reason}",
                    input.relation_id,
                    input.relation_version,
                    input.start_offset_inclusive,
                    input.end_offset_exclusive
                );
                self.poisoned_reason = Some(reason.clone());
                return Err(StandingProgramRuntimeError::ExternalRuntime { reason });
            }
        }
        if transactional {
            if let Err(reason) = self.commit_transaction_and_wait() {
                let reason = format!(
                    "failed to commit Feldera transaction for multi-input epoch {logical_epoch}: {reason}"
                );
                self.poisoned_reason = Some(reason.clone());
                return Err(StandingProgramRuntimeError::ExternalRuntime { reason });
            }
        }
        self.logical_epoch = logical_epoch;
        self.applied_idempotency
            .insert(idempotency_key.as_str().to_string(), logical_epoch);
        let input_frontiers: Vec<RelationFrontier> = input_changes
            .into_iter()
            .map(|input| RelationFrontier {
                relation_id: input.relation_id,
                relation_version: input.relation_version,
                committed_offset_exclusive: input.end_offset_exclusive,
            })
            .collect();
        for frontier in &input_frontiers {
            self.input_frontiers.insert(
                (
                    frontier.relation_id.clone(),
                    frontier.relation_version.clone(),
                ),
                frontier.committed_offset_exclusive,
            );
        }
        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers,
            output_batches: Vec::new(),
        })
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        self.ensure_not_poisoned()?;
        if !self
            .identity
            .view_ids
            .iter()
            .any(|view_id| view_id == &view.view_id)
        {
            return Err(StandingProgramRuntimeError::UnknownView {
                view_id: view.view_id,
            });
        }
        if let Some(requested) = page.committed_epoch {
            if requested != self.logical_epoch {
                return Err(StandingProgramRuntimeError::UnavailableCommittedEpoch {
                    requested,
                    current: self.logical_epoch,
                });
            }
        }
        let output_schema = self
            .output_schemas
            .iter()
            .find(|schema| schema.relation_id == view.view_id)
            .ok_or_else(|| StandingProgramRuntimeError::UnknownView {
                view_id: view.view_id.clone(),
            })?;
        let sql = format!(
            "SELECT * FROM {}",
            feldera_sql_quoted_identifier(&output_schema.relation_name)
        );
        let (sql, page_offset, requested_rows) = feldera_sql_with_page(sql, &page)
            .map_err(|reason| StandingProgramRuntimeError::ExternalRuntime { reason })?;
        let rows = self
            .query_sql_rows(sql, Some(output_schema))
            .map_err(|reason| StandingProgramRuntimeError::ExternalRuntime { reason })?;
        let (rows, next_page_token) =
            feldera_apply_wrapped_page_bounds(rows, page_offset, requested_rows)
                .map_err(|reason| StandingProgramRuntimeError::ExternalRuntime { reason })?;
        let batch = feldera_rows_to_record_batch(output_schema, &rows)
            .map_err(|reason| StandingProgramRuntimeError::ExternalRuntime { reason })?;
        Ok(MaterializedViewPage {
            view,
            logical_epoch: self.logical_epoch,
            schema_fingerprint: output_schema.schema_fingerprint.clone(),
            batches: vec![batch],
            next_page_token,
        })
    }

    fn materialized_view_sql_page(
        &self,
        view: ScopedViewId,
        sql: String,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewSqlPage, StandingProgramRuntimeError> {
        self.ensure_not_poisoned()?;
        if !self
            .identity
            .view_ids
            .iter()
            .any(|view_id| view_id == &view.view_id)
        {
            return Err(StandingProgramRuntimeError::UnknownView {
                view_id: view.view_id,
            });
        }
        if let Some(requested) = page.committed_epoch {
            if requested != self.logical_epoch {
                return Err(StandingProgramRuntimeError::UnavailableCommittedEpoch {
                    requested,
                    current: self.logical_epoch,
                });
            }
        }
        let (sql, page_offset, requested_rows, page_mode) = feldera_sql_query_with_page(sql, &page)
            .map_err(|reason| StandingProgramRuntimeError::ExternalRuntime { reason })?;
        let rows = self
            .query_sql_rows(sql, None)
            .map_err(|reason| StandingProgramRuntimeError::ExternalRuntime { reason })?;
        let (rows, next_page_token) = match page_mode {
            FelderaSqlPageMode::Unwrapped => {
                feldera_apply_unwrapped_page_bounds(rows, page_offset, requested_rows)
            }
            FelderaSqlPageMode::Wrapped => {
                feldera_apply_wrapped_page_bounds(rows, page_offset, requested_rows)
            }
        }
        .map_err(|reason| StandingProgramRuntimeError::ExternalRuntime { reason })?;
        Ok(MaterializedViewSqlPage {
            view,
            logical_epoch: self.logical_epoch,
            rows,
            next_page_token,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        self.ensure_not_poisoned()?;
        let payload = json!({
            "pipeline_name": self.pipeline_name,
            "logical_epoch": self.logical_epoch,
            "deployment_mode": self.runtime_deployment_mode.as_checkpoint_str(),
            "applied_idempotency": self.applied_idempotency
        })
        .to_string();
        let content_hash = feldera_artifact_bytes_hash(payload.as_bytes());
        Ok(RuntimeCheckpoint {
            identity: self.identity.clone(),
            logical_epoch: self.logical_epoch,
            input_frontiers: self
                .input_frontiers
                .iter()
                .map(
                    |((relation_id, relation_version), committed_offset_exclusive)| {
                        RelationFrontier {
                            relation_id: relation_id.clone(),
                            relation_version: relation_version.clone(),
                            committed_offset_exclusive: *committed_offset_exclusive,
                        }
                    },
                )
                .collect(),
            output_frontiers: self
                .identity
                .view_ids
                .iter()
                .map(|view_id| ViewFrontier {
                    view_id: view_id.clone(),
                    committed_epoch: self.logical_epoch,
                })
                .collect(),
            checkpoint_codec_identity: FELDERA_PIPELINE_MANAGER_STATE_CODEC.to_string(),
            state_root: DurableStateRoot {
                object_key: format!("feldera-pipeline-manager://{}", self.pipeline_name),
                content_hash,
            },
            state_payload: Some(
                velorix_core::standing_program::RuntimeCheckpointStatePayload {
                    codec_identity: FELDERA_PIPELINE_MANAGER_STATE_CODEC.to_string(),
                    payload,
                },
            ),
            output_manifest_refs: Vec::new(),
            owner_epoch: None,
        })
    }

    fn restore(_checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError>
    where
        Self: Sized,
    {
        Err(StandingProgramRuntimeError::ExternalRuntime {
            reason: "Feldera pipeline-manager runtime restore requires active view metadata"
                .to_string(),
        })
    }
}

fn run_feldera_runtime_http<T, F, Fut>(timeout: Duration, operation: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(reqwest::Client) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("Feldera runtime HTTP executor failed to start: {error}"))?;
        runtime.block_on(async move {
            let client = reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .map_err(|error| format!("Feldera runtime HTTP client failed to build: {error}"))?;
            operation(client).await
        })
    })
    .join()
    .map_err(|_| "Feldera runtime HTTP executor panicked".to_string())?
}

fn feldera_runtime_request(
    client: &reqwest::Client,
    bearer_token: Option<&str>,
    method: reqwest::Method,
    url: String,
) -> reqwest::RequestBuilder {
    let request = client.request(method, url);
    match bearer_token {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}

async fn feldera_runtime_http_error(
    operation: &'static str,
    response: reqwest::Response,
) -> String {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if body.trim().is_empty() {
        format!("{operation} returned HTTP {status}")
    } else {
        format!("{operation} returned HTTP {status}: {body}")
    }
}

#[derive(Debug, Deserialize)]
struct FelderaPipelineStatusResponse {
    program_status: String,
    #[serde(default)]
    deployment_status: Option<String>,
    #[serde(default)]
    deployment_resources_status: Option<String>,
    #[serde(default)]
    program_version: u64,
    #[serde(default)]
    program_info: Option<Value>,
    #[serde(default)]
    program_error: Option<Value>,
}

fn feldera_pipeline_sql_compiled_without_runtime_artifact(
    pipeline: &FelderaPipelineStatusResponse,
) -> bool {
    pipeline.program_status == "SqlCompiled"
        && pipeline.deployment_status.as_deref() == Some("Stopped")
        && pipeline.deployment_resources_status.as_deref() == Some("Stopped")
        && pipeline
            .program_error
            .as_ref()
            .and_then(|error| error.pointer("/rust_compilation"))
            .is_none_or(Value::is_null)
}

fn feldera_sql_compiled_stall_timeout(poll_timeout: Duration) -> Duration {
    poll_timeout.min(FELDERA_COMPILER_SQL_COMPILED_STALL_TIMEOUT)
}

fn standing_view_spec_for_compile_request(request: &FelderaCompileRequestV1) -> StandingViewSpec {
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

fn feldera_pipeline_manager_sql_compile_request(
    request: &FelderaCompileRequestV1,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<FelderaCompileRequestV1, ApiError> {
    let (_, input_weight_column_names, _) =
        validate_feldera_pipeline_manager_runtime_catalogs(catalogs)
            .map_err(ApiError::bad_request)?;
    let mut request = request.clone();
    for input in &mut request.input_relations {
        let weight_column = input_weight_column_names
            .get(&input.relation_id)
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "missing Feldera weight column metadata for input relation `{}`",
                    input.relation_id
                ))
            })?;
        if input
            .primary_key
            .iter()
            .any(|column| column == weight_column)
        {
            return Err(ApiError::bad_request(format!(
                "Feldera pipeline-manager compile request does not allow weight column `{weight_column}` in primary key for relation `{}`",
                input.relation_id
            )));
        }
        input.columns.retain(|column| column.name != *weight_column);
        if input.columns.is_empty() {
            return Err(ApiError::bad_request(format!(
                "Feldera pipeline-manager compile request has no data columns after stripping weight column `{weight_column}` for relation `{}`",
                input.relation_id
            )));
        }
    }
    Ok(request)
}

fn feldera_pipeline_name_for_compile_request(request: &FelderaCompilerBackendRequest) -> String {
    feldera_pipeline_name_for_parts(&request.view_id, &request.compile_request_hash)
}

fn feldera_pipeline_name_for_view_spec(spec: &StandingViewSpec) -> Result<String, ApiError> {
    let compile_request_hash = compile_request_hash_for_spec(spec)?;
    Ok(feldera_pipeline_name_for_parts(
        &spec.view_id,
        &compile_request_hash,
    ))
}

const FELDERA_PIPELINE_NAME_MAX_CHARS: usize = 63;

fn feldera_pipeline_name_for_parts(view_id: &str, compile_request_hash: &str) -> String {
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

async fn feldera_http_error(operation: &'static str, response: reqwest::Response) -> ApiError {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let message = if body.trim().is_empty() {
        format!("{operation} returned HTTP {status}")
    } else {
        format!("{operation} returned HTTP {status}: {body}")
    };
    if status.is_client_error() {
        ApiError::bad_request(message)
    } else {
        ApiError::service_unavailable(message)
    }
}

fn feldera_program_error_summary(error: Option<&Value>) -> String {
    let Some(error) = error else {
        return "no program_error returned".to_string();
    };
    if let Some(message) = error.get("message").and_then(Value::as_str) {
        return message.to_string();
    }
    if let Some(messages) = error
        .pointer("/sql_compilation/messages")
        .and_then(Value::as_array)
    {
        let rendered = messages
            .iter()
            .filter_map(|message| message.get("message").and_then(Value::as_str))
            .take(3)
            .collect::<Vec<_>>()
            .join("; ");
        if !rendered.is_empty() {
            return rendered;
        }
    }
    serde_json::to_string(error).unwrap_or_else(|_| "unrenderable program_error".to_string())
}

fn feldera_semantic_warning_summary(error: Option<&Value>) -> Option<String> {
    let messages = error?
        .pointer("/sql_compilation/messages")
        .and_then(Value::as_array)?;
    messages.iter().find_map(|message| {
        let warning = message
            .get("warning")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !warning {
            return None;
        }
        let error_type = message
            .get("error_type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let text = message
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if error_type == "ORDER BY is ignored"
            || text.contains("ORDER BY clause is currently ignored")
        {
            Some(if text.is_empty() {
                error_type.to_string()
            } else {
                text.to_string()
            })
        } else {
            None
        }
    })
}

fn feldera_sql_quoted_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn api_path_segment(segment: &str) -> String {
    utf8_percent_encode(segment, API_PATH_SEGMENT_ENCODE_SET).to_string()
}

fn feldera_sql_with_page(
    sql: String,
    page: &SnapshotPageRequest,
) -> Result<(String, usize, Option<usize>), String> {
    let (offset, requested_rows) = feldera_sql_page_bounds(page)?;
    let fetch_rows = requested_rows
        .map(|requested_rows| {
            requested_rows.checked_add(1).ok_or_else(|| {
                "Feldera pipeline-manager max_rows is too large for pagination".to_string()
            })
        })
        .transpose()?;
    if fetch_rows.is_none() && offset == 0 {
        return Ok((sql, offset, requested_rows));
    }
    let sql = sql.trim().trim_end_matches(';').trim();
    let mut paged_sql = format!(
        "SELECT * FROM ({sql}) AS {}",
        feldera_sql_quoted_identifier("velorix_limited_query")
    );
    if let Some(fetch_rows) = fetch_rows {
        paged_sql.push_str(&format!(" LIMIT {fetch_rows}"));
    }
    if offset > 0 {
        paged_sql.push_str(&format!(" OFFSET {offset}"));
    }
    Ok((paged_sql, offset, requested_rows))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FelderaSqlPageMode {
    Unwrapped,
    Wrapped,
}

fn feldera_sql_query_with_page(
    sql: String,
    page: &SnapshotPageRequest,
) -> Result<(String, usize, Option<usize>, FelderaSqlPageMode), String> {
    let (offset, requested_rows) = feldera_sql_page_bounds(page)?;
    if requested_rows.is_none() && offset == 0 {
        return Ok((sql, offset, requested_rows, FelderaSqlPageMode::Unwrapped));
    }
    if let Some((query_sql, execute_sql)) = split_velorix_feldera_prepared_query_sql(&sql) {
        let (paged_query_sql, offset, requested_rows) =
            feldera_sql_with_page(query_sql.to_string(), page)?;
        return Ok((
            format!("PREPARE {FELDERA_PREPARED_QUERY_NAME} AS {paged_query_sql};\n{execute_sql}"),
            offset,
            requested_rows,
            FelderaSqlPageMode::Wrapped,
        ));
    }
    let query_sql = trim_feldera_prepared_statement_sql(&sql);
    if feldera_sql_has_statement_separator(query_sql) {
        return Err(
            "pagination for Feldera SQL query path requires a single SQL statement".to_string(),
        );
    }
    let (sql, offset, requested_rows) = feldera_sql_with_page(query_sql.to_string(), page)?;
    Ok((sql, offset, requested_rows, FelderaSqlPageMode::Wrapped))
}

fn split_velorix_feldera_prepared_query_sql(sql: &str) -> Option<(&str, String)> {
    let trimmed = sql.trim();
    let prefix = format!("PREPARE {FELDERA_PREPARED_QUERY_NAME} AS ");
    let rest = trimmed.strip_prefix(&prefix)?;
    let execute_prefix = format!("EXECUTE {FELDERA_PREPARED_QUERY_NAME}(");
    let separator = format!(";\n{execute_prefix}");
    let (query_sql, execute_args) = rest.split_once(&separator)?;
    if !execute_args.ends_with(");") {
        return None;
    }
    Some((query_sql.trim(), format!("{execute_prefix}{execute_args}")))
}

fn feldera_sql_page_bounds(page: &SnapshotPageRequest) -> Result<(usize, Option<usize>), String> {
    let offset = feldera_page_token_offset(page.page_token.as_deref())?;
    Ok((offset, page.max_rows))
}

fn feldera_page_token_offset(page_token: Option<&str>) -> Result<usize, String> {
    let Some(page_token) = page_token else {
        return Ok(0);
    };
    let Some(offset) = page_token.strip_prefix("offset:") else {
        return Err(format!(
            "invalid Feldera pipeline-manager page_token `{page_token}`; expected `offset:<row_offset>`"
        ));
    };
    let parsed = offset.parse::<usize>().map_err(|_| {
        format!(
            "invalid Feldera pipeline-manager page_token `{page_token}`; expected `offset:<row_offset>`"
        )
    })?;
    Ok(parsed)
}

fn feldera_apply_wrapped_page_bounds(
    mut rows: Vec<Value>,
    offset: usize,
    requested_rows: Option<usize>,
) -> Result<(Vec<Value>, Option<String>), String> {
    let Some(requested_rows) = requested_rows else {
        return Ok((rows, None));
    };
    if rows.len() <= requested_rows {
        return Ok((rows, None));
    }
    rows.truncate(requested_rows);
    let next_offset = offset.checked_add(requested_rows).ok_or_else(|| {
        "Feldera pipeline-manager page offset overflow while building next_page_token".to_string()
    })?;
    Ok((rows, Some(format!("offset:{next_offset}"))))
}

fn feldera_apply_unwrapped_page_bounds(
    mut rows: Vec<Value>,
    offset: usize,
    requested_rows: Option<usize>,
) -> Result<(Vec<Value>, Option<String>), String> {
    if offset > 0 {
        if offset >= rows.len() {
            return Ok((Vec::new(), None));
        }
        rows.drain(..offset);
    }
    feldera_apply_wrapped_page_bounds(rows, offset, requested_rows)
}

fn feldera_output_schemas_from_program_info(
    view_id: &str,
    program_version: u64,
    program_info: Option<&Value>,
    multi_output: bool,
) -> Result<Vec<RelationSchema>, ApiError> {
    let program_info = program_info.ok_or_else(|| {
        ApiError::service_unavailable("Feldera compiled response is missing program_info")
    })?;
    let outputs = program_info
        .pointer("/schema/outputs")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ApiError::service_unavailable("Feldera program_info is missing schema.outputs")
        })?;
    if outputs.is_empty() {
        return Err(ApiError::bad_request(
            "Feldera compiled program does not contain output views",
        ));
    }
    let mut output_names = BTreeSet::new();
    for output in outputs {
        let output_name = feldera_relation_name(output).ok_or_else(|| {
            ApiError::service_unavailable(format!(
                "Feldera compiled program for `{view_id}` contains an output without a name"
            ))
        })?;
        if !output_names.insert(feldera_relation_name_key(output_name, output)) {
            return Err(ApiError::bad_request(format!(
                "Feldera compiled program contains duplicate output view `{output_name}`"
            )));
        }
    }
    if !multi_output {
        let output = outputs
            .iter()
            .find(|output| feldera_relation_name_matches(view_id, output))
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "Feldera compiled program does not contain output view `{view_id}`"
                ))
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
        return Err(ApiError::bad_request(
            "Feldera compiled program does not contain materialized output views",
        ));
    }
    Ok(materialized_outputs)
}

fn validate_feldera_program_info_admission(
    request: &FelderaCompileRequestV1,
    program_info: Option<&Value>,
) -> Result<(), ApiError> {
    if request.source_kind != SqlSourceKind::FelderaProgram {
        return Ok(());
    }
    let program_info = program_info.ok_or_else(|| {
        ApiError::service_unavailable("Feldera compiled response is missing program_info")
    })?;
    let inputs = program_info
        .pointer("/schema/inputs")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ApiError::bad_request(
                "Feldera feldera_program compiled response is missing schema.inputs",
            )
        })?;
    let expected = request.input_relations.iter().collect::<Vec<_>>();
    let mut actual = BTreeSet::new();
    let mut matched_expected = BTreeSet::new();
    for input in inputs {
        let name = feldera_relation_name(input).ok_or_else(|| {
            ApiError::service_unavailable(
                "Feldera feldera_program compiled response contains an input without a name",
            )
        })?;
        if !actual.insert(feldera_relation_name_key(name, input)) {
            return Err(ApiError::bad_request(format!(
                "Feldera feldera_program compiled response contains duplicate input relation `{name}`"
            )));
        }
        let unmanaged_properties = feldera_relation_unmanaged_io_properties(input);
        if !unmanaged_properties.is_empty() {
            return Err(ApiError::bad_request(format!(
                "Feldera feldera_program input relation `{name}` contains unmanaged connector/external IO properties: {}",
                unmanaged_properties.join(", ")
            )));
        }
        let matching_expected = expected.iter().copied().find(|expected_schema| {
            feldera_relation_name_matches(&expected_schema.relation_name, input)
        });
        let Some(matching_expected) = matching_expected else {
            return Err(ApiError::bad_request(format!(
                "Feldera feldera_program compiled response contains unregistered input relation `{name}`"
            )));
        };
        validate_feldera_program_input_schema_matches(name, input, matching_expected)?;
        matched_expected.insert(matching_expected.relation_name.as_str());
    }
    for expected_name in expected {
        if !matched_expected.contains(expected_name.relation_name.as_str()) {
            return Err(ApiError::bad_request(format!(
                "Feldera feldera_program compiled response is missing registered input relation `{}`",
                expected_name.relation_name
            )));
        }
    }
    Ok(())
}

fn validate_feldera_program_input_schema_matches(
    input_name: &str,
    input: &Value,
    expected: &RelationSchema,
) -> Result<(), ApiError> {
    let fields = input
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "Feldera feldera_program input relation `{input_name}` is missing fields"
            ))
        })?;
    let columns = fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            feldera_column_schema_from_relation_field("input relation", input_name, index, field)
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_feldera_program_relation_column_names(
        "input relation",
        input_name,
        input,
        fields,
        &columns,
    )?;

    let mut expected_by_name = BTreeMap::new();
    for expected_column in &expected.columns {
        expected_by_name.insert(expected_column.name.to_ascii_lowercase(), expected_column);
    }

    let mut canonical_columns = Vec::with_capacity(columns.len());
    for (index, (field, column)) in fields.iter().zip(columns.iter()).enumerate() {
        let folded_name = column.name.to_ascii_lowercase();
        let Some(expected_column) = expected_by_name.get(&folded_name) else {
            return Err(ApiError::bad_request(format!(
                "Feldera feldera_program input relation `{input_name}` column {index} `{}` is not registered by relation `{}`",
                column.name, expected.relation_name
            )));
        };
        let case_insensitive = feldera_identifier_case_insensitive(input, field);
        let name_matches = if case_insensitive {
            expected_column
                .name
                .eq_ignore_ascii_case(column.name.as_str())
        } else {
            expected_column.name == column.name
        };
        if !name_matches {
            return Err(ApiError::bad_request(format!(
                "Feldera feldera_program input relation `{input_name}` column {index} `{}` does not match registered column `{}`",
                column.name, expected_column.name
            )));
        }
        if column.data_type != expected_column.data_type
            || column.nullable != expected_column.nullable
        {
            return Err(ApiError::bad_request(format!(
                "Feldera feldera_program input relation `{input_name}` column `{}` type does not match registered column `{}`",
                column.name, expected_column.name
            )));
        }
        canonical_columns.push((*expected_column).clone());
    }

    if let Some(primary_key) = feldera_program_relation_primary_key_columns(
        "input relation",
        input_name,
        input,
        fields,
        &canonical_columns,
    )? {
        if primary_key != expected.primary_key {
            return Err(ApiError::bad_request(format!(
                "Feldera feldera_program input relation `{input_name}` primary_key does not match registered relation `{}`",
                expected.relation_name
            )));
        }
    }

    Ok(())
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

fn feldera_output_schema_from_program_output(
    view_id: &str,
    program_version: u64,
    output: &Value,
) -> Result<RelationSchema, ApiError> {
    let output_name = feldera_relation_name(output).ok_or_else(|| {
        ApiError::service_unavailable(format!(
            "Feldera compiled program for `{view_id}` contains an output without a name"
        ))
    })?;
    if !feldera_relation_is_materialized(output) {
        return Err(ApiError::bad_request(format!(
            "Feldera output view `{output_name}` is not materialized"
        )));
    }
    let unmanaged_properties = feldera_relation_unmanaged_io_properties(output);
    if !unmanaged_properties.is_empty() {
        return Err(ApiError::bad_request(format!(
            "Feldera output view `{output_name}` contains unmanaged connector/external IO properties: {}",
            unmanaged_properties.join(", ")
        )));
    }
    let fields = output
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ApiError::service_unavailable(format!(
                "Feldera output view `{output_name}` is missing fields"
            ))
        })?;
    let columns = fields
        .iter()
        .enumerate()
        .map(|(index, field)| feldera_column_schema_from_field(output_name, index, field))
        .collect::<Result<Vec<_>, _>>()?;
    if columns.is_empty() {
        return Err(ApiError::bad_request(format!(
            "Feldera output view `{output_name}` has no columns"
        )));
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
    )
    .map_err(ApiError::service_unavailable)?;
    Ok(RelationSchema {
        relation_id: output_name.to_string(),
        relation_name: output_name.to_string(),
        relation_version: format!("feldera-program-v{program_version}"),
        schema_fingerprint,
        columns,
        primary_key,
    })
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
) -> Result<(), ApiError> {
    let mut seen: Vec<(&str, bool)> = Vec::new();
    for (field, column) in fields.iter().zip(columns) {
        if column.name.trim().is_empty() {
            return Err(ApiError::bad_request(format!(
                "Feldera {relation_kind} `{output_name}` contains a blank field name"
            )));
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
            return Err(ApiError::bad_request(format!(
                "Feldera {relation_kind} `{output_name}` contains duplicate field `{}`",
                column.name
            )));
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
) -> Result<Option<Vec<String>>, ApiError> {
    let Some(primary_key) = output.get("primary_key") else {
        return Ok(None);
    };
    let primary_key = primary_key.as_array().ok_or_else(|| {
        ApiError::bad_request(format!(
            "Feldera {relation_kind} `{output_name}` primary_key must be an array"
        ))
    })?;
    let mut keys = Vec::with_capacity(primary_key.len());
    let mut seen: Vec<(&str, bool)> = Vec::new();
    let key_case_insensitive = feldera_relation_identifier_case_insensitive(output);
    for (index, key) in primary_key.iter().enumerate() {
        let key = key.as_str().ok_or_else(|| {
            ApiError::bad_request(format!(
                "Feldera {relation_kind} `{output_name}` primary_key entry {index} must be a string"
            ))
        })?;
        if key.trim().is_empty() {
            return Err(ApiError::bad_request(format!(
                "Feldera {relation_kind} `{output_name}` contains a blank primary_key entry"
            )));
        }
        if seen.iter().any(|(seen_key, seen_case_insensitive)| {
            feldera_identifiers_conflict(
                seen_key,
                *seen_case_insensitive,
                key,
                key_case_insensitive,
            )
        }) {
            return Err(ApiError::bad_request(format!(
                "Feldera {relation_kind} `{output_name}` contains duplicate primary_key entry `{key}`"
            )));
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
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "Feldera {relation_kind} `{output_name}` primary_key entry `{key}` does not reference a field"
                ))
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
) -> Result<ColumnSchema, ApiError> {
    feldera_column_schema_from_relation_field("output view", view_id, index, field)
}

fn feldera_column_schema_from_relation_field(
    relation_kind: &str,
    relation_name: &str,
    index: usize,
    field: &Value,
) -> Result<ColumnSchema, ApiError> {
    let name = field
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ApiError::service_unavailable(format!(
                "Feldera {relation_kind} `{relation_name}` field {index} is missing name"
            ))
        })?
        .to_string();
    let columntype = field.get("columntype").unwrap_or(field);
    let (data_type, nullable) =
        feldera_sql_data_type_from_column_type(columntype).map_err(|error| {
            ApiError::bad_request(format!(
                "Feldera {relation_kind} `{relation_name}` field `{name}` has unsupported type: {error}"
            ))
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
            let precision = u8_from_optional_i64("precision", precision.unwrap_or(38))?;
            let scale = u8_from_optional_i64("scale", scale.unwrap_or(0))?;
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
                    Ok(velorix_core::feldera_artifact::SqlStructField {
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
            unit: velorix_core::feldera_artifact::SqlIntervalUnit::Day,
        }),
        "INTERVAL_DAY_HOUR" => Ok(SqlDataType::Interval {
            unit: velorix_core::feldera_artifact::SqlIntervalUnit::DayToHour,
        }),
        "INTERVAL_DAY_MINUTE" => Ok(SqlDataType::Interval {
            unit: velorix_core::feldera_artifact::SqlIntervalUnit::DayToMinute,
        }),
        "INTERVAL_DAY_SECOND" => Ok(SqlDataType::Interval {
            unit: velorix_core::feldera_artifact::SqlIntervalUnit::DayToSecond,
        }),
        "INTERVAL_HOUR" => Ok(SqlDataType::Interval {
            unit: velorix_core::feldera_artifact::SqlIntervalUnit::Hour,
        }),
        "INTERVAL_HOUR_MINUTE" => Ok(SqlDataType::Interval {
            unit: velorix_core::feldera_artifact::SqlIntervalUnit::HourToMinute,
        }),
        "INTERVAL_HOUR_SECOND" => Ok(SqlDataType::Interval {
            unit: velorix_core::feldera_artifact::SqlIntervalUnit::HourToSecond,
        }),
        "INTERVAL_MINUTE" => Ok(SqlDataType::Interval {
            unit: velorix_core::feldera_artifact::SqlIntervalUnit::Minute,
        }),
        "INTERVAL_MINUTE_SECOND" => Ok(SqlDataType::Interval {
            unit: velorix_core::feldera_artifact::SqlIntervalUnit::MinuteToSecond,
        }),
        "INTERVAL_MONTH" => Ok(SqlDataType::Interval {
            unit: velorix_core::feldera_artifact::SqlIntervalUnit::Month,
        }),
        "INTERVAL_SECOND" => Ok(SqlDataType::Interval {
            unit: velorix_core::feldera_artifact::SqlIntervalUnit::Second,
        }),
        "INTERVAL_YEAR" => Ok(SqlDataType::Interval {
            unit: velorix_core::feldera_artifact::SqlIntervalUnit::Year,
        }),
        "INTERVAL_YEAR_MONTH" => Ok(SqlDataType::Interval {
            unit: velorix_core::feldera_artifact::SqlIntervalUnit::YearToMonth,
        }),
        _ => Err(format!("unknown Feldera SQL type `{raw_name}`")),
    }
}

fn u8_from_optional_i64(field: &'static str, value: i64) -> Result<u8, String> {
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
) -> Result<String, serde_json::Error> {
    let canonical = serde_json::to_vec(&json!({
        "domain": "velorix-feldera-compiled-output-schema-v1",
        "view_id": view_id,
        "program_version": program_version,
        "columns": columns,
        "primary_key": primary_key
    }))?;
    let mut hasher = Sha256::new();
    hasher.update(canonical);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

pub trait StandingProgramRuntimeFactory: Send + Sync + 'static {
    fn output_schemas_for_view_request(
        &self,
        _view_id: &str,
        _sql: &str,
        _catalog: &VelorixRelationCatalogV1,
        _input_schema_fingerprint: &str,
    ) -> Result<Option<Vec<RelationSchema>>, ApiError> {
        Ok(None)
    }

    fn output_schemas_for_view_request_with_catalogs(
        &self,
        view_id: &str,
        sql: &str,
        catalogs: &[VelorixRelationCatalogV1],
        input_schema_fingerprint: &str,
    ) -> Result<Option<Vec<RelationSchema>>, ApiError> {
        let Some(catalog) = catalogs.first() else {
            return Ok(None);
        };
        self.output_schemas_for_view_request(view_id, sql, catalog, input_schema_fingerprint)
    }

    fn compile_artifact_for_spec(
        &self,
        _catalog: &VelorixRelationCatalogV1,
        _spec: &StandingViewSpec,
    ) -> Result<Option<FelderaCompileArtifactMetadata>, ApiError> {
        Ok(None)
    }

    fn compile_artifact_for_spec_with_catalogs(
        &self,
        catalogs: &[VelorixRelationCatalogV1],
        spec: &StandingViewSpec,
    ) -> Result<Option<FelderaCompileArtifactMetadata>, ApiError> {
        let Some(catalog) = catalogs.first() else {
            return Ok(None);
        };
        self.compile_artifact_for_spec(catalog, spec)
    }

    fn create(
        &self,
        identity: &StandingProgramIdentity,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String>;

    fn create_with_schemas(
        &self,
        identity: &StandingProgramIdentity,
        _input_schemas: &[RelationSchema],
        _output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        self.create(identity)
    }

    fn create_with_catalog(
        &self,
        identity: &StandingProgramIdentity,
        _catalog: &VelorixRelationCatalogV1,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        self.create_with_schemas(identity, input_schemas, output_schemas)
    }

    fn create_with_catalog_and_spec(
        &self,
        identity: &StandingProgramIdentity,
        catalog: &VelorixRelationCatalogV1,
        _spec: &StandingViewSpec,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        self.create_with_catalog(identity, catalog, input_schemas, output_schemas)
    }

    fn create_with_catalogs_and_spec(
        &self,
        identity: &StandingProgramIdentity,
        catalogs: &[VelorixRelationCatalogV1],
        spec: &StandingViewSpec,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        let Some(catalog) = catalogs.first() else {
            return Err("standing runtime requires at least one relation catalog".to_string());
        };
        self.create_with_catalog_and_spec(identity, catalog, spec, input_schemas, output_schemas)
    }

    fn restore(
        &self,
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String>;

    fn restore_with_catalogs_and_spec(
        &self,
        checkpoint: RuntimeCheckpoint,
        _catalogs: &[VelorixRelationCatalogV1],
        _spec: &StandingViewSpec,
        _input_schemas: &[RelationSchema],
        _output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        self.restore(checkpoint)
    }
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

#[derive(Clone, Debug)]
struct GeneratedSingleKeySumCountRuntimeFactory;

impl StandingProgramRuntimeFactory for GeneratedSingleKeySumCountRuntimeFactory {
    fn output_schemas_for_view_request(
        &self,
        view_id: &str,
        sql: &str,
        catalog: &VelorixRelationCatalogV1,
        _input_schema_fingerprint: &str,
    ) -> Result<Option<Vec<RelationSchema>>, ApiError> {
        if validate_catalog_backed_sum_count_view_sql(sql, catalog).is_err() {
            return Ok(None);
        }
        if validate_generic_single_key_sum_count_runtime_scope(catalog).is_err() {
            return Ok(None);
        }
        single_key_sum_count_output_schema(view_id, catalog).map(|schema| Some(vec![schema]))
    }

    fn compile_artifact_for_spec(
        &self,
        catalog: &VelorixRelationCatalogV1,
        spec: &StandingViewSpec,
    ) -> Result<Option<FelderaCompileArtifactMetadata>, ApiError> {
        generic_single_key_sum_count_artifact_for_spec(catalog, spec)
    }

    fn output_schemas_for_view_request_with_catalogs(
        &self,
        view_id: &str,
        sql: &str,
        catalogs: &[VelorixRelationCatalogV1],
        input_schema_fingerprint: &str,
    ) -> Result<Option<Vec<RelationSchema>>, ApiError> {
        if catalogs.len() == 2 {
            let Ok(plan) = validate_supported_dbsp_join_view_sql(sql, catalogs) else {
                return Ok(None);
            };
            validate_join_plan_catalog_order(&plan, catalogs)?;
            for catalog in catalogs {
                if validate_generic_single_key_sum_count_runtime_scope(catalog).is_err() {
                    return Ok(None);
                }
            }
            return join_sum_count_output_schema(view_id, catalogs)
                .map(|schema| Some(vec![schema]));
        }
        let Some(catalog) = catalogs.first() else {
            return Ok(None);
        };
        self.output_schemas_for_view_request(view_id, sql, catalog, input_schema_fingerprint)
    }

    fn compile_artifact_for_spec_with_catalogs(
        &self,
        catalogs: &[VelorixRelationCatalogV1],
        spec: &StandingViewSpec,
    ) -> Result<Option<FelderaCompileArtifactMetadata>, ApiError> {
        generic_single_key_sum_count_artifact_for_spec_with_catalogs(catalogs, spec)
    }

    fn create(
        &self,
        _identity: &StandingProgramIdentity,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Err("single-key sum/count runtime requires input/output schemas".to_string())
    }

    fn create_with_schemas(
        &self,
        identity: &StandingProgramIdentity,
        _input_schemas: &[RelationSchema],
        _output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        let _ = identity;
        Err("single-key sum/count runtime requires relation catalog".to_string())
    }

    fn create_with_catalog(
        &self,
        identity: &StandingProgramIdentity,
        catalog: &VelorixRelationCatalogV1,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        velorix_generated_single_key_sum_count::create_standing_runtime(
            identity,
            catalog,
            input_schemas,
            output_schemas,
        )
    }

    fn create_with_catalog_and_spec(
        &self,
        identity: &StandingProgramIdentity,
        catalog: &VelorixRelationCatalogV1,
        spec: &StandingViewSpec,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        velorix_generated_single_key_sum_count::create_standing_runtime_with_sql(
            identity,
            catalog,
            spec.sql.as_str(),
            input_schemas,
            output_schemas,
        )
    }

    fn create_with_catalogs_and_spec(
        &self,
        identity: &StandingProgramIdentity,
        catalogs: &[VelorixRelationCatalogV1],
        spec: &StandingViewSpec,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        velorix_generated_single_key_sum_count::create_standing_runtime_with_sql_and_catalogs(
            identity,
            catalogs,
            spec.sql.as_str(),
            input_schemas,
            output_schemas,
        )
    }

    fn restore(
        &self,
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        velorix_generated_single_key_sum_count::restore_standing_runtime(checkpoint)
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
        _state_path: impl Into<String>,
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
            feldera_compiler_backend: None,
            generated_artifact_packages: Arc::new(default_generated_artifact_packages()),
            builtin_fixture_compile_worker_enabled: false,
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

    pub fn with_builtin_fixture_compile_worker_enabled(mut self, enabled: bool) -> Self {
        self.builtin_fixture_compile_worker_enabled = enabled;
        self
    }

    pub fn with_feldera_compiler_backend(
        mut self,
        backend: Arc<dyn FelderaCompilerBackend>,
    ) -> Self {
        self.feldera_compiler_backend = Some(backend);
        self
    }

    pub fn with_feldera_pipeline_manager_backend(
        mut self,
        backend: Arc<FelderaPipelineManagerCompilerBackend>,
    ) -> Self {
        self.feldera_compiler_backend = Some(backend.clone());
        self.register_standing_program_runtime_factory(
            FELDERA_PIPELINE_MANAGER_RUNTIME_PACKAGE_NAME,
            backend.as_ref().clone(),
        );
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

    fn generated_package_output_schemas_for_view_request(
        &self,
        view_id: &str,
        sql: &str,
        catalogs: &[VelorixRelationCatalogV1],
        input_schema_fingerprint: &str,
    ) -> Result<Option<Vec<RelationSchema>>, ApiError> {
        if !self.builtin_fixture_compile_worker_enabled {
            return Ok(None);
        }
        let factories = self
            .standing_runtime_factories
            .factories
            .lock()
            .map_err(|_| ApiError::internal("standing runtime factory registry lock poisoned"))?;
        for package in self.generated_artifact_packages.iter() {
            if let Some(factory) = factories.get(&package.crate_name) {
                if let Some(output) = factory.output_schemas_for_view_request_with_catalogs(
                    view_id,
                    sql,
                    catalogs,
                    input_schema_fingerprint,
                )? {
                    return Ok(Some(output));
                }
            }
        }
        Ok(None)
    }

    fn generated_package_artifact_for_spec(
        &self,
        catalogs: &[VelorixRelationCatalogV1],
        spec: &StandingViewSpec,
    ) -> Result<Option<FelderaCompileArtifactMetadata>, ApiError> {
        if !self.builtin_fixture_compile_worker_enabled {
            return Ok(None);
        }
        let factories = self
            .standing_runtime_factories
            .factories
            .lock()
            .map_err(|_| ApiError::internal("standing runtime factory registry lock poisoned"))?;
        for package in self.generated_artifact_packages.iter() {
            if let Some(factory) = factories.get(&package.crate_name) {
                if let Some(artifact) =
                    factory.compile_artifact_for_spec_with_catalogs(catalogs, spec)?
                {
                    return Ok(Some(artifact));
                }
            }
        }
        Ok(None)
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
            if let Some(replay_plan) =
                ensure_standing_runtime_for_artifact(self, &active.spec, artifact).await?
            {
                replay_committed_ingest_into_standing_runtime(self, &active, &replay_plan).await?;
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
                    ViewCompileDeployJobStatus::CompileValidated => {
                        report.skipped += 1;
                        report.outcomes.push(ViewCompileDeployWorkerJobOutcome {
                            job_id,
                            view_id,
                            status: "compile_validated".to_string(),
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
            let compile_request_hash = compile_request_hash_for_spec(&active.spec)?;
            match registry
                .read_by_compile_request_hash(&active.spec.view_id, &compile_request_hash)
                .await
            {
                Ok(_) => continue,
                Err(ViewCompileDeployJobRegistryError::ObjectStore(
                    object_store::Error::NotFound { .. },
                )) => {}
                Err(error) => return Err(view_compile_deploy_job_registry_error_to_api(error)),
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
            if repair_compile_deploy_job_for_active_standing_runtime(
                self,
                &active,
                &job,
                "standing runtime was already active; repaired compile/deploy job",
            )
            .await?
            {
                return Ok(ViewCompileDeployJobStatus::Duplicate);
            }
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
        let catalogs = read_relation_catalogs_for_spec(self, &active.spec).await?;
        let catalog = catalogs
            .first()
            .ok_or_else(|| ApiError::bad_request("pending view has no input relation"))?;
        self.validate_standing_runtime_fencing_or_evict().await?;
        let active_compile_request_hash = compile_request_hash_for_spec(&active.spec)?;
        let resolution = if let Some(backend) = self.feldera_compiler_backend.as_ref() {
            let Some(compiler_request) = job.compiler_request.as_ref() else {
                return Ok(ViewCompileDeployJobStatus::Skipped(
                    "compile/deploy job is missing compiler_request".to_string(),
                ));
            };
            let compile_request_hash = compiler_request.compile_request_hash.clone();
            let compiler_request = compiler_request.feldera_compile_request();
            let program_code = feldera_sql_program_for_compile_request(&compiler_request)
                .map_err(|error| ApiError::bad_request(error.to_string()))?;
            let response = backend
                .compile(FelderaCompilerBackendRequest {
                    job_id: job.job_id.clone(),
                    view_id: job.view_id.clone(),
                    spec_hash: job.spec_hash.clone(),
                    compile_request_hash,
                    program_code,
                    compiler_request,
                    catalogs: catalogs.clone(),
                })
                .await?;
            let resolved_spec = resolved_compile_spec_with_pending_output_relation_ids(
                &active.spec,
                response.resolved_spec,
            );
            validate_resolved_compile_spec(
                &active.spec,
                &resolved_spec,
                &active_compile_request_hash,
            )?;
            ViewCompileDeployResolution {
                spec: resolved_spec,
                artifact: response.artifact,
                product_runtime: response.product_runtime,
                runtime_deployment: response.runtime_deployment,
                activation_message: "standing runtime activated from Feldera compiler backend",
            }
        } else if let Some(descriptor) =
            trusted_generated_descriptor_for_spec(self, catalog, &active.spec)?
        {
            if !state_has_generated_descriptor_package(self, &descriptor) {
                return Ok(ViewCompileDeployJobStatus::Skipped(format!(
                    "generated Rust package `{}` is not registered with this Velorix binary",
                    descriptor.generated_rust.crate_name
                )));
            }
            ViewCompileDeployResolution {
                spec: active.spec.clone(),
                artifact: Some(generated_view_artifact_for_descriptor(
                    &descriptor,
                    catalog,
                )?),
                product_runtime: None,
                runtime_deployment: None,
                activation_message: "standing runtime activated from linked generated package",
            }
        } else {
            match self.generated_package_artifact_for_spec(&catalogs, &active.spec) {
                Ok(Some(artifact)) => ViewCompileDeployResolution {
                    spec: active.spec.clone(),
                    artifact: Some(artifact),
                    product_runtime: None,
                    runtime_deployment: None,
                    activation_message: "standing runtime activated from linked generated package",
                },
                Ok(None) => {
                    return Ok(ViewCompileDeployJobStatus::Skipped(
                        "feldera compiler backend is not configured; builtin generated runtime fixtures are disabled for product compile/deploy jobs".to_string(),
                    ));
                }
                Err(error) => {
                    return Ok(ViewCompileDeployJobStatus::Skipped(error.to_string()));
                }
            }
        };
        let activation_spec_hash =
            feldera_spec_hash(&resolution.spec).map_err(ApiError::bad_request)?;
        let output_schemas = resolution.spec.output_relations.clone();
        let (artifact, should_activate_deploying) = match active.execution_mode {
            MaterializedViewExecutionMode::FelderaCompilePending => {
                if let Some(artifact_metadata) = resolution.artifact.as_ref() {
                    validate_feldera_compile_artifact_for_compile_request(
                        &resolution.spec,
                        artifact_metadata,
                        &active_compile_request_hash,
                    )
                    .map_err(ApiError::bad_request)?;
                    (
                        register_view_artifact(
                            self,
                            &catalogs,
                            &resolution.spec,
                            artifact_metadata,
                        )
                        .await?,
                        true,
                    )
                } else if let Some(product_runtime) = resolution.product_runtime.as_ref() {
                    let compile_request =
                        FelderaCompileRequestV1::infer_output_from_standing_view_spec(&active.spec);
                    validate_feldera_package_runtime_descriptor(
                        &resolution.spec,
                        &compile_request,
                        product_runtime,
                    )
                    .map_err(ApiError::bad_request)?;
                    (
                        feldera_package_runtime_artifact_binding(
                            &resolution.spec,
                            product_runtime,
                        )?,
                        true,
                    )
                } else if let Some(deployment) = resolution.runtime_deployment.as_ref() {
                    if resolution.spec.input_relations.len() > 1
                        && !deployment.supports_multi_input_activation()
                    {
                        let message =
                            "Feldera pipeline-manager runtime deployment is not activated for multi-input views in this runtime mode"
                                .to_string();
                        let lifecycle = MaterializedViewLifecycleStatus::feldera_compile_validated(
                            Some(message.clone()),
                        );
                        self.view_registry()?
                            .mark_pending_compile_validated_with_resolved_spec(
                                &active.spec.view_id,
                                &active.spec_hash,
                                &resolution.spec,
                                lifecycle,
                            )
                            .await
                            .map_err(materialized_view_registry_error_to_api)?;
                        self.view_compile_deploy_job_registry()?
                            .mark_compile_validated_for_compile_request_hash(
                                &active.spec.view_id,
                                &active_compile_request_hash,
                                Some(message),
                            )
                            .await
                            .map_err(view_compile_deploy_job_registry_error_to_api)?;
                        return Ok(ViewCompileDeployJobStatus::CompileValidated);
                    }
                    (
                        external_feldera_runtime_artifact_binding(
                            &catalogs,
                            &resolution.spec,
                            deployment,
                        )?,
                        true,
                    )
                } else {
                    let lifecycle =
                        MaterializedViewLifecycleStatus::feldera_compile_validated(Some(
                            "Feldera compiler resolved schemas; executable runtime is not deployed"
                                .to_string(),
                        ));
                    self.view_registry()?
                        .mark_pending_compile_validated_with_resolved_spec(
                            &active.spec.view_id,
                            &active.spec_hash,
                            &resolution.spec,
                            lifecycle,
                        )
                        .await
                        .map_err(materialized_view_registry_error_to_api)?;
                    self.view_compile_deploy_job_registry()?
                        .mark_compile_validated_for_compile_request_hash(
                            &active.spec.view_id,
                            &active_compile_request_hash,
                            Some(
                                "Feldera compiler resolved schemas; executable runtime is not deployed"
                                    .to_string(),
                            ),
                        )
                        .await
                        .map_err(view_compile_deploy_job_registry_error_to_api)?;
                    return Ok(ViewCompileDeployJobStatus::CompileValidated);
                }
            }
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
        };
        let identity = artifact
            .standing_program_identity
            .as_ref()
            .ok_or_else(|| ApiError::conflict("generated artifact is missing runtime identity"))?
            .clone();
        let replay_plan = if let Some((runtime, replay_plan)) =
            restore_or_build_standing_runtime_for_artifact(
                self,
                &resolution.spec,
                &artifact,
                &resolution.spec.input_relations,
                &output_schemas,
            )
            .await?
        {
            insert_standing_runtime(self, &resolution.spec.view_id, runtime)?;
            replay_plan
        } else {
            read_latest_standing_runtime_checkpoint(self, &identity, &resolution.spec.view_id)
                .await?
                .map(standing_runtime_replay_plan_from_record)
                .unwrap_or_default()
        };
        let deploying_lifecycle = MaterializedViewLifecycleStatus::standing_runtime_deploying(
            Some("catching up committed ingest before query activation".to_string()),
        );
        let activation = if should_activate_deploying {
            let api_metadata = active.api.clone().unwrap_or_default();
            validate_standing_runtime_create_api_metadata(
                &resolution.spec.view_id,
                &api_metadata,
                &output_schemas,
                sql_template_validation_mode_for_artifact(&artifact),
            )
            .await?;
            if activation_spec_hash == active.spec_hash {
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
                self.view_registry()?
                    .activate_pending_with_resolved_spec_artifact(
                        &active.spec.view_id,
                        &active.spec_hash,
                        &resolution.spec,
                        artifact.clone(),
                        deploying_lifecycle.clone(),
                    )
                    .await
                    .map_err(materialized_view_registry_error_to_api)?
            }
        } else {
            ActivateMaterializedViewOutcome::Duplicate
        };
        let replay_active = ActiveMaterializedView {
            spec_hash: activation_spec_hash.clone(),
            spec: resolution.spec.clone(),
            execution_mode: MaterializedViewExecutionMode::StandingRuntime,
            api: active.api.clone(),
            artifact: Some(artifact),
            lifecycle: deploying_lifecycle,
        };
        replay_committed_ingest_into_standing_runtime(self, &replay_active, &replay_plan).await?;
        let lifecycle = MaterializedViewLifecycleStatus::standing_runtime();
        let lifecycle_update = self
            .view_registry()?
            .update_standing_runtime_lifecycle(
                &resolution.spec.view_id,
                &activation_spec_hash,
                lifecycle,
            )
            .await
            .map_err(materialized_view_registry_error_to_api)?;
        mark_compile_deploy_job_running(
            self,
            &active.spec.view_id,
            &active.spec_hash,
            &active_compile_request_hash,
            resolution.activation_message.to_string(),
        )
        .await?;

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
        .route("/v1/ingest/epoch", post(ingest_epoch))
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
        .route(
            "/v1/views/{view_id}/outputs/{output_id}/query",
            get(query_view_output_rows_get).post(query_view_output_rows_post),
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
            "/v1/view-compile-deploy/jobs/{view_id}/claim",
            post(claim_view_compile_deploy_job),
        )
        .route(
            "/v1/view-compile-deploy/jobs/{view_id}/complete",
            post(complete_view_compile_deploy_job),
        )
        .route(
            "/v1/view-compile-deploy/run-once",
            post(run_view_compile_deploy_once),
        )
        .route(
            "/v1/standing-runtime/owners",
            get(get_standing_runtime_owners).post(acquire_standing_runtime_owners),
        )
        .route(
            "/v1/standing-runtime/ingest-epoch-failures/repair",
            post(repair_ingest_epoch_runtime_failure),
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
    } else if generated_rust_crate_name == velorix_generated_single_key_sum_count::CRATE_NAME {
        state.register_standing_program_runtime_factory(
            generated_rust_crate_name,
            GeneratedSingleKeySumCountRuntimeFactory,
        );
    }
}

fn default_generated_artifact_packages() -> Vec<GeneratedRustArtifactPackage> {
    vec![
        GeneratedRustArtifactPackage {
            abi_version: SUPPORTED_GENERATED_RUST_ABI_VERSION.to_string(),
            crate_name: velorix_generated_scores_by_user::CRATE_NAME.to_string(),
        },
        GeneratedRustArtifactPackage {
            abi_version: SUPPORTED_GENERATED_RUST_ABI_VERSION.to_string(),
            crate_name: velorix_generated_single_key_sum_count::CRATE_NAME.to_string(),
        },
    ]
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
const POSITIVE_SCORES_DYNAMIC_SHAPE_ID: &str = "scores.positive-scores-by-user.v1";

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
        dynamic_view_binding: Some(DynamicGeneratedViewBinding {
            shape_id: POSITIVE_SCORES_DYNAMIC_SHAPE_ID.to_string(),
        }),
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
        output_relation_id: None,
        input_relation_id: DEFAULT_SCORES_RELATION_ID.to_string(),
        input_relation_version: DEFAULT_SCORES_RELATION_VERSION.to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: DEFAULT_POSITIVE_SCORES_SQL.to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        udf_rust: None,
        udf_toml: None,
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

fn generic_single_key_sum_count_artifact_for_spec(
    catalog: &VelorixRelationCatalogV1,
    spec: &StandingViewSpec,
) -> Result<Option<FelderaCompileArtifactMetadata>, ApiError> {
    generic_single_key_sum_count_artifact_for_spec_with_catalogs(
        std::slice::from_ref(catalog),
        spec,
    )
}

fn generic_single_key_sum_count_artifact_for_spec_with_catalogs(
    catalogs: &[VelorixRelationCatalogV1],
    spec: &StandingViewSpec,
) -> Result<Option<FelderaCompileArtifactMetadata>, ApiError> {
    let Some(catalog) = catalogs.first() else {
        return Ok(None);
    };
    if catalogs.len() > 2 {
        return Ok(None);
    }
    if catalog.relation_schema.relation_id == DEFAULT_SCORES_RELATION_ID
        && catalog.relation_schema.relation_version == DEFAULT_SCORES_RELATION_VERSION
        && catalogs.len() == 1
    {
        return Ok(None);
    }
    if catalogs.len() == 2 {
        let plan = validate_supported_dbsp_join_view_sql(&spec.sql, catalogs)
            .map_err(ApiError::bad_request)?;
        validate_join_plan_catalog_order(&plan, catalogs)?;
        for catalog in catalogs {
            validate_generic_single_key_sum_count_runtime_scope(catalog)?;
        }
    } else {
        if let Err(error) = validate_catalog_backed_sum_count_view_sql(&spec.sql, catalog) {
            return Err(ApiError::bad_request(error));
        }
        validate_generic_single_key_sum_count_runtime_scope(catalog)?;
    }
    let spec_hash = feldera_spec_hash(spec).map_err(ApiError::bad_request)?;
    let spec_hash_segment = spec_hash
        .strip_prefix("velorix-feldera-spec-sha256-v1:")
        .unwrap_or(spec_hash.as_str());
    let artifact_id = format!(
        "builtin-single-key-sum-count-{}-{}",
        spec.view_id, spec_hash_segment
    );
    let artifact_identity_bytes = serde_json::to_vec(&json!({
        "runtime": velorix_generated_single_key_sum_count::CRATE_NAME,
        "view_id": spec.view_id,
        "spec_hash": spec_hash,
        "input_schemas": spec.input_relations,
        "output_schemas": spec.output_relations,
    }))
    .map_err(|source| ApiError::internal(source.to_string()))?;

    Ok(Some(FelderaCompileArtifactMetadata {
        metadata_version: FELDERA_ARTIFACT_METADATA_VERSION,
        view_id: spec.view_id.clone(),
        spec_hash,
        compile_request_hash: Some(compile_request_hash_for_spec(spec)?),
        artifact_id,
        artifact_hash: feldera_artifact_bytes_hash(&artifact_identity_bytes),
        compiler: FelderaCompilerIdentity {
            name: if catalogs.len() == 2 {
                "velorix-linked-fixture-two-input-join-sum-count".to_string()
            } else {
                "velorix-linked-fixture-single-key-sum-count".to_string()
            },
            version: "builtin-v1".to_string(),
            source: "velorix-relation-catalog".to_string(),
        },
        generated_rust: GeneratedRustIdentity {
            abi_version: SUPPORTED_GENERATED_RUST_ABI_VERSION.to_string(),
            crate_name: velorix_generated_single_key_sum_count::CRATE_NAME.to_string(),
        },
        input_schemas: spec.input_relations.clone(),
        output_schemas: spec.output_relations.clone(),
        state_codec: SUPPORTED_STATE_CODEC.to_string(),
        state_schema_version: 1,
        epoch_policy: SUPPORTED_EPOCH_POLICY.to_string(),
    }))
}

fn validate_user_supplied_generic_single_key_sum_count_artifact(
    state: &ApiState,
    catalogs: &[VelorixRelationCatalogV1],
    spec: &StandingViewSpec,
    artifact: &FelderaCompileArtifactMetadata,
) -> Result<(), ApiError> {
    if artifact.generated_rust.crate_name != velorix_generated_single_key_sum_count::CRATE_NAME {
        return Ok(());
    }
    let Some(expected) = state.generated_package_artifact_for_spec(catalogs, spec)? else {
        return Err(ApiError::bad_request(
            "generic single-key sum/count artifact is not supported for this view spec/catalog",
        ));
    };
    if artifact == &expected {
        Ok(())
    } else {
        Err(ApiError::bad_request(
            "generic single-key sum/count artifact metadata must exactly match the validated catalog-backed view spec",
        ))
    }
}

fn trusted_generated_descriptor_for_request(
    state: &ApiState,
    catalog: &VelorixRelationCatalogV1,
    request: &CreateViewRequest,
) -> Result<Option<TrustedGeneratedViewDescriptor>, ApiError> {
    if request.artifact.is_some()
        || !request.input_relation_refs.is_empty()
        || !request.input_relations.is_empty()
        || request.source_kind != SqlSourceKind::StandingView
        || !request.output_relation_ids.is_empty()
    {
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
    if spec.source_kind != SqlSourceKind::StandingView {
        return Ok(None);
    }
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
        .map(|descriptor| {
            trusted_generated_descriptor_for_request_view(descriptor, catalog, view_id)
        })
}

fn trusted_generated_descriptor_for_request_view(
    descriptor: &TrustedGeneratedViewDescriptor,
    catalog: &VelorixRelationCatalogV1,
    view_id: &str,
) -> TrustedGeneratedViewDescriptor {
    let mut descriptor = descriptor.clone();
    if descriptor.view_id != view_id {
        descriptor.view_id = view_id.to_string();
        descriptor.artifact_id = format!("{}-view-binding-{view_id}", descriptor.artifact_id);
    }
    if descriptor.generated_rust.crate_name == velorix_generated_scores_by_user::CRATE_NAME
        && descriptor.input_relation_id == DEFAULT_SCORES_RELATION_ID
        && descriptor.input_relation_version == DEFAULT_SCORES_RELATION_VERSION
        && descriptor.sql == DEFAULT_POSITIVE_SCORES_SQL
    {
        descriptor.output_schemas = vec![positive_scores_output_schema(
            view_id,
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
        && (view_id == descriptor.view_id || descriptor.dynamic_view_binding.is_some())
        && descriptor.matches_view_shape(input_relation_id, input_relation_version, sql)
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
    if let Some(base_url) = config.feldera_pipeline_manager_url {
        let mut backend = FelderaPipelineManagerCompilerBackend::new(
            base_url,
            config.feldera_bearer_token,
            Duration::from_millis(config.feldera_compiler_poll_interval_ms),
            Duration::from_millis(config.feldera_compiler_timeout_ms),
            config.feldera_compiler_profile,
            config.feldera_compiler_workers,
        )
        .map_err(|error| anyhow!(error.to_string()))?;
        if let Some(mode) = config.feldera_pipeline_manager_runtime_deployment_mode {
            backend = backend.with_runtime_deployment_mode(mode);
            state = state.with_feldera_pipeline_manager_backend(Arc::new(backend));
        } else {
            state = state.with_feldera_compiler_backend(Arc::new(backend));
        }
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
pub struct IngestEpochRequest {
    pub batches: Vec<IngestRowsRequest>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepairIngestEpochRuntimeFailureRequest {
    epoch_manifest_id: String,
    tenant_id: String,
    program_id: String,
    view_id: String,
    confirm_external_runtime_rebuilt: bool,
    repair_reason: String,
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
    #[serde(default, rename = "outputRelationId", alias = "output_relation_id")]
    pub output_relation_id: Option<String>,
    #[serde(default)]
    pub input_relation_id: String,
    #[serde(default)]
    pub input_relation_version: String,
    #[serde(default, rename = "inputRelationRefs", alias = "input_relation_refs")]
    pub input_relation_refs: Vec<InputRelationRef>,
    #[serde(default)]
    pub input_relations: Vec<RelationSchema>,
    pub sql: String,
    #[serde(default = "default_sql_source_kind")]
    pub source_kind: SqlSourceKind,
    #[serde(default)]
    pub output_relation_ids: Vec<String>,
    #[serde(default, rename = "udfRust", alias = "udf_rust")]
    pub udf_rust: Option<String>,
    #[serde(default, rename = "udfToml", alias = "udf_toml")]
    pub udf_toml: Option<String>,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputRelationRef {
    pub relation_id: String,
    pub relation_version: String,
}

fn default_sql_source_kind() -> SqlSourceKind {
    SqlSourceKind::StandingView
}

fn resolved_sql_source_kind_for_create_view(request: &CreateViewRequest) -> SqlSourceKind {
    if request.source_kind == SqlSourceKind::StandingView
        && looks_like_feldera_program_sql(request.sql.as_str())
    {
        SqlSourceKind::FelderaProgram
    } else {
        request.source_kind.clone()
    }
}

fn looks_like_feldera_program_sql(sql: &str) -> bool {
    let sql = trim_sql_leading_space_and_comments(sql);
    let Some(prefix) = sql.get(..6) else {
        return false;
    };
    if !prefix.eq_ignore_ascii_case("create") {
        return false;
    }
    sql.get(6..)
        .and_then(|rest| rest.chars().next())
        .map_or(true, is_sql_keyword_boundary)
}

fn trim_sql_leading_space_and_comments(mut sql: &str) -> &str {
    loop {
        let trimmed = sql.trim_start();
        if let Some(rest) = trimmed.strip_prefix("--") {
            sql = rest
                .find(['\n', '\r'])
                .map_or("", |line_end| &rest[line_end + 1..]);
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("/*") {
            let Some(comment_end) = rest.find("*/") else {
                return trimmed;
            };
            sql = &rest[comment_end + 2..];
            continue;
        }
        return trimmed;
    }
}

fn is_sql_keyword_boundary(ch: char) -> bool {
    !ch.is_ascii_alphanumeric() && ch != '_'
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
struct IngestEpochResponse {
    outcome: String,
    epoch_manifest_id: String,
    epoch_manifest_key: String,
    batches: Vec<IngestResponse>,
}

#[derive(Clone, Debug, Serialize)]
struct RepairIngestEpochRuntimeFailureResponse {
    outcome: String,
    marker_key: String,
    tenant_id: String,
    program_id: String,
    view_id: String,
    epoch_manifest_id: String,
    removed_runtime_cache: bool,
    failure_reason: String,
    repair_reason: String,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    output_relation_id: Option<String>,
    input_relation_id: String,
    input_relation_version: String,
    spec_hash: String,
    source_kind: SqlSourceKind,
    execution_mode: MaterializedViewExecutionMode,
    lifecycle: MaterializedViewLifecycleStatus,
    query_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    disabled_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compile_job_id: Option<String>,
    query_endpoint: String,
    output_query_endpoints: Vec<String>,
    output_relations: Vec<RelationSchema>,
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
    pub jobs: Vec<ViewCompileDeployJobResponse>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ViewCompileDeployJobResponse {
    #[serde(flatten)]
    pub job: ViewCompileDeployJobRecord,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_relation_catalogs: Vec<VelorixRelationCatalogV1>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimViewCompileDeployJobRequest {
    worker_id: String,
    #[serde(default = "default_view_compile_deploy_claim_lease_duration_ms")]
    lease_duration_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
struct ClaimViewCompileDeployJobResponse {
    claim_status: String,
    #[serde(flatten)]
    claim: ViewCompileDeployJobClaimRecord,
}

fn default_view_compile_deploy_claim_lease_duration_ms() -> u64 {
    300_000
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
    CompileValidated,
    Duplicate,
    Skipped(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlTemplateValidationMode {
    LocalDataFusion,
    ExternalFelderaRuntime,
}

struct ViewCompileDeployResolution {
    spec: StandingViewSpec,
    artifact: Option<FelderaCompileArtifactMetadata>,
    product_runtime: Option<FelderaPackageRuntimeDescriptorV1>,
    runtime_deployment: Option<FelderaPipelineManagerRuntimeDeployment>,
    activation_message: &'static str,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompleteViewCompileDeployRequest {
    #[serde(default)]
    spec_hash: Option<String>,
    #[serde(default)]
    compile_request_hash: Option<String>,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    job_generation: Option<u64>,
    #[serde(default)]
    worker_id: Option<String>,
    #[serde(default)]
    lease_id: Option<String>,
    #[serde(default)]
    fencing_token: Option<u64>,
    #[serde(default)]
    resolved_spec: Option<StandingViewSpec>,
    #[serde(default)]
    artifact: Option<FelderaCompileArtifactMetadata>,
    #[serde(default)]
    product_runtime: Option<FelderaPackageRuntimeDescriptorV1>,
    #[serde(default)]
    runtime_deployment: Option<FelderaPipelineManagerRuntimeDeployment>,
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
    let mut responses = Vec::with_capacity(jobs.len());
    for job in jobs {
        let input_relation_catalogs = if let Some(compiler_request) = &job.compiler_request {
            read_relation_catalogs_for_input_schemas(&state, &compiler_request.input_relations)
                .await?
        } else {
            Vec::new()
        };
        responses.push(ViewCompileDeployJobResponse {
            job,
            input_relation_catalogs,
        });
    }
    Ok(Json(ViewCompileDeployJobCatalogResponse {
        pending_jobs: responses.len(),
        jobs: responses,
    }))
}

async fn claim_view_compile_deploy_job(
    State(state): State<ApiState>,
    AxumPath(view_id): AxumPath<String>,
    Json(request): Json<ClaimViewCompileDeployJobRequest>,
) -> Result<Json<ClaimViewCompileDeployJobResponse>, ApiError> {
    if request.lease_duration_ms == 0 || request.lease_duration_ms > 3_600_000 {
        return Err(ApiError::bad_request(
            "claim lease_duration_ms must be between 1 and 3600000",
        ));
    }
    let active = state
        .view_registry()?
        .read_active(&view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    if active.execution_mode != MaterializedViewExecutionMode::FelderaCompilePending {
        return Err(ApiError::conflict(format!(
            "view `{view_id}` is not waiting for Feldera compile/deploy claim"
        )));
    }
    let compile_request_hash = compile_request_hash_for_spec(&active.spec)?;
    let job =
        read_pending_compile_deploy_job(&state, &view_id, &active.spec_hash, &compile_request_hash)
            .await?;
    if !compile_job_request_matches_active_spec(&job, &active.spec) {
        return Err(ApiError::conflict(
            "compile/deploy job compiler_request does not match active view spec",
        ));
    }
    let outcome = state
        .view_compile_deploy_job_registry()?
        .claim_pending_for_compile_request_hash(
            &view_id,
            &compile_request_hash,
            request.worker_id.as_str(),
            unix_epoch_millis()?,
            request.lease_duration_ms,
        )
        .await
        .map_err(view_compile_deploy_job_registry_error_to_api)?;
    let (claim_status, claim) = match outcome {
        ViewCompileDeployJobClaimOutcome::Claimed(claim) => ("claimed", claim),
        ViewCompileDeployJobClaimOutcome::Duplicate(claim) => ("duplicate", claim),
    };
    Ok(Json(ClaimViewCompileDeployJobResponse {
        claim_status: claim_status.to_string(),
        claim,
    }))
}

async fn complete_view_compile_deploy_job(
    State(state): State<ApiState>,
    AxumPath(view_id): AxumPath<String>,
    Json(request): Json<CompleteViewCompileDeployRequest>,
) -> Result<Json<ViewResponse>, ApiError> {
    let response = complete_pending_view_compile_deploy_job(
        &state,
        &view_id,
        request.spec_hash.as_deref(),
        request.compile_request_hash.as_deref(),
        request.tenant_id.as_deref(),
        request.job_generation,
        request.worker_id.as_deref(),
        request.lease_id.as_deref(),
        request.fencing_token,
        request.resolved_spec.as_ref(),
        request.artifact.as_ref(),
        request.product_runtime.as_ref(),
        request.runtime_deployment.as_ref(),
    )
    .await?;
    Ok(Json(response))
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
    let catalogs = read_relation_catalogs_for_view_request(&state, &request).await?;
    let catalog = catalogs
        .first()
        .ok_or_else(|| ApiError::bad_request("view has no input relation"))?;
    let trusted_generated_descriptor =
        trusted_generated_descriptor_for_request(&state, catalog, &request)?;
    let trusted_generated_artifact = trusted_generated_descriptor
        .as_ref()
        .map(|descriptor| generated_view_artifact_for_descriptor(descriptor, catalog))
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
    let spec = view_spec_from_request(&state, &request, &catalogs, selected_artifact_metadata)?;
    validate_feldera_runtime_spec_admission(&spec)?;
    let artifact = if let Some(artifact_request) = &request.artifact {
        state.validate_standing_runtime_fencing_or_evict().await?;
        validate_user_supplied_generic_single_key_sum_count_artifact(
            &state,
            &catalogs,
            &spec,
            &artifact_request.metadata,
        )?;
        Some(register_view_artifact(&state, &catalogs, &spec, &artifact_request.metadata).await?)
    } else if let Some(artifact_metadata) = &trusted_static_artifact {
        state.validate_standing_runtime_fencing_or_evict().await?;
        Some(register_view_artifact(&state, &catalogs, &spec, artifact_metadata).await?)
    } else {
        None
    };
    let spec_hash = feldera_spec_hash(&spec).map_err(ApiError::bad_request)?;
    let api_metadata = api_metadata_from_create_view_request(&request);
    validate_view_api_metadata(&api_metadata)?;
    validate_query_policy_reference(&state, &api_metadata).await?;
    validate_view_api_output_binding(&spec.view_id, &api_metadata, &spec.output_relations)?;
    if let Some(artifact_binding) = artifact.as_ref() {
        validate_standing_runtime_create_api_metadata(
            &spec.view_id,
            &api_metadata,
            &spec.output_relations,
            sql_template_validation_mode_for_artifact(artifact_binding),
        )
        .await?;
    }
    let pending_runtime = if let (Some(artifact), Some(artifact_metadata)) =
        (&artifact, selected_artifact_metadata)
    {
        build_standing_runtime_for_artifact(
            &state,
            &spec,
            artifact,
            &catalogs,
            &artifact_metadata.input_schemas,
            &artifact_metadata.output_schemas,
        )?
    } else {
        None
    };
    let execution_mode = if artifact.is_some() {
        MaterializedViewExecutionMode::StandingRuntime
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
            Some(api_metadata),
            artifact,
            Some(outcome_text),
        )?),
    ))
}

async fn register_view_artifact(
    state: &ApiState,
    catalogs: &[VelorixRelationCatalogV1],
    spec: &StandingViewSpec,
    artifact: &FelderaCompileArtifactMetadata,
) -> Result<MaterializedViewArtifactBinding, ApiError> {
    let catalog = catalogs
        .first()
        .ok_or_else(|| ApiError::bad_request("view artifact requires at least one catalog"))?;
    if artifact.compile_request_hash.is_some()
        || artifact.metadata_version == FELDERA_ARTIFACT_METADATA_VERSION
    {
        validate_feldera_compile_artifact_for_compile_request(
            spec,
            artifact,
            &compile_request_hash_for_spec(spec)?,
        )
        .map_err(ApiError::bad_request)?;
    }
    let registered = state
        .runtime_feldera_artifact_registry()?
        .register_trusted_artifact_for_catalogs(catalogs, spec, artifact)
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

fn external_feldera_runtime_artifact_binding(
    catalogs: &[VelorixRelationCatalogV1],
    spec: &StandingViewSpec,
    deployment: &FelderaPipelineManagerRuntimeDeployment,
) -> Result<MaterializedViewArtifactBinding, ApiError> {
    let identity = standing_program_identity_from_external_feldera_runtime(catalogs, spec)?;
    let artifact_hash = feldera_artifact_bytes_hash(
        serde_json::to_vec(&json!({
            "execution_path": "feldera_pipeline_manager",
            "pipeline_name": deployment.pipeline_name,
            "deployment_mode": format!("{:?}", deployment.mode),
            "spec": spec
        }))
        .map_err(ApiError::internal)?
        .as_slice(),
    );
    Ok(MaterializedViewArtifactBinding {
        artifact_id: format!("feldera-pipeline-manager:{}", deployment.pipeline_name),
        artifact_hash,
        generated_rust_crate_name: FELDERA_PIPELINE_MANAGER_RUNTIME_PACKAGE_NAME.to_string(),
        state_codec: FELDERA_PIPELINE_MANAGER_STATE_CODEC.to_string(),
        state_schema_version: 2,
        execution_status: "direct_execution_enabled".to_string(),
        execution_path: "feldera_pipeline_manager".to_string(),
        standing_program_identity: Some(identity),
    })
}

fn feldera_package_runtime_artifact_binding(
    _spec: &StandingViewSpec,
    descriptor: &FelderaPackageRuntimeDescriptorV1,
) -> Result<MaterializedViewArtifactBinding, ApiError> {
    let artifact_hash = feldera_artifact_bytes_hash(
        serde_json::to_vec(&descriptor)
            .map_err(ApiError::internal)?
            .as_slice(),
    );
    Ok(MaterializedViewArtifactBinding {
        artifact_id: format!(
            "{}:{}",
            FELDERA_PACKAGE_RUNTIME_EXECUTION_PATH, descriptor.view_id
        ),
        artifact_hash,
        generated_rust_crate_name: descriptor.runtime_factory.crate_name.clone(),
        state_codec: descriptor.state_codec.clone(),
        state_schema_version: descriptor.state_schema_version,
        execution_status: "direct_execution_enabled".to_string(),
        execution_path: FELDERA_PACKAGE_RUNTIME_EXECUTION_PATH.to_string(),
        standing_program_identity: Some(descriptor.standing_program_identity.clone()),
    })
}

fn validate_external_feldera_runtime_deployment_completion(
    spec: &StandingViewSpec,
    deployment: &FelderaPipelineManagerRuntimeDeployment,
) -> Result<(), ApiError> {
    if deployment.pipeline_name.trim().is_empty() {
        return Err(ApiError::bad_request(
            "runtime_deployment.pipeline_name must not be empty",
        ));
    }
    if deployment.pipeline_name.contains('/') || deployment.pipeline_name.contains('\\') {
        return Err(ApiError::bad_request(
            "runtime_deployment.pipeline_name must be a Feldera pipeline name, not a path",
        ));
    }
    let expected_pipeline_name = feldera_pipeline_name_for_view_spec(spec)?;
    if deployment.pipeline_name != expected_pipeline_name {
        return Err(ApiError::conflict(format!(
            "runtime_deployment.pipeline_name does not match pending view compile request: expected={}, request={}",
            expected_pipeline_name, deployment.pipeline_name
        )));
    }
    Ok(())
}

async fn validate_compile_deploy_claim_for_completion(
    state: &ApiState,
    view_id: &str,
    compile_request_hash: &str,
    request_tenant_id: Option<&str>,
    request_job_generation: Option<u64>,
    request_worker_id: Option<&str>,
    request_lease_id: Option<&str>,
    request_fencing_token: Option<u64>,
) -> Result<(), ApiError> {
    let registry = state.view_compile_deploy_job_registry()?;
    let claim = match registry
        .read_claim_by_compile_request_hash(view_id, compile_request_hash)
        .await
    {
        Ok(claim) => Some(claim),
        Err(ViewCompileDeployJobRegistryError::ObjectStore(object_store::Error::NotFound {
            ..
        })) => None,
        Err(error) => return Err(view_compile_deploy_job_registry_error_to_api(error)),
    };
    let Some(claim) = claim else {
        if request_tenant_id.is_some()
            || request_job_generation.is_some()
            || request_worker_id.is_some()
            || request_lease_id.is_some()
            || request_fencing_token.is_some()
        {
            return Err(ApiError::conflict(
                "completion request supplied claim proof but no active compile/deploy job claim exists",
            ));
        }
        return Ok(());
    };
    let Some(tenant_id) = request_tenant_id else {
        return Err(ApiError::conflict(
            "claimed compile/deploy job completion requires `tenant_id`",
        ));
    };
    let Some(job_generation) = request_job_generation else {
        return Err(ApiError::conflict(
            "claimed compile/deploy job completion requires `job_generation`",
        ));
    };
    let Some(worker_id) = request_worker_id else {
        return Err(ApiError::conflict(
            "claimed compile/deploy job completion requires `worker_id`",
        ));
    };
    let Some(lease_id) = request_lease_id else {
        return Err(ApiError::conflict(
            "claimed compile/deploy job completion requires `lease_id`",
        ));
    };
    let Some(fencing_token) = request_fencing_token else {
        return Err(ApiError::conflict(
            "claimed compile/deploy job completion requires `fencing_token`",
        ));
    };
    if claim.tenant_id != tenant_id
        || claim.job_generation != job_generation
        || claim.worker_id != worker_id
        || claim.lease_id != lease_id
        || claim.fencing_token != fencing_token
    {
        return Err(ApiError::conflict(
            "claimed compile/deploy job completion proof does not match the active claim",
        ));
    }
    if claim.lease_expires_at_ms <= unix_epoch_millis()? {
        return Err(ApiError::conflict(
            "claimed compile/deploy job lease has expired",
        ));
    }
    Ok(())
}

fn sql_template_validation_mode_for_artifact(
    artifact: &MaterializedViewArtifactBinding,
) -> SqlTemplateValidationMode {
    if artifact.execution_path == "feldera_pipeline_manager" {
        SqlTemplateValidationMode::ExternalFelderaRuntime
    } else {
        SqlTemplateValidationMode::LocalDataFusion
    }
}

async fn complete_pending_view_compile_deploy_job(
    state: &ApiState,
    view_id: &str,
    request_spec_hash: Option<&str>,
    request_compile_request_hash: Option<&str>,
    request_tenant_id: Option<&str>,
    request_job_generation: Option<u64>,
    request_worker_id: Option<&str>,
    request_lease_id: Option<&str>,
    request_fencing_token: Option<u64>,
    request_resolved_spec: Option<&StandingViewSpec>,
    artifact_metadata: Option<&FelderaCompileArtifactMetadata>,
    product_runtime: Option<&FelderaPackageRuntimeDescriptorV1>,
    runtime_deployment: Option<&FelderaPipelineManagerRuntimeDeployment>,
) -> Result<ViewResponse, ApiError> {
    if request_spec_hash.is_none() && request_compile_request_hash.is_none() {
        return Err(ApiError::bad_request(
            "completion request must include `compile_request_hash` or legacy `spec_hash`",
        ));
    }
    let completion_payloads = [
        artifact_metadata.is_some(),
        product_runtime.is_some(),
        runtime_deployment.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if completion_payloads != 1 {
        return Err(ApiError::bad_request(
            "completion request must include exactly one of `artifact`, `product_runtime`, or `runtime_deployment`",
        ));
    }
    let active = state
        .view_registry()?
        .read_active(view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let active_compile_request_hash = compile_request_hash_for_spec(&active.spec)?;
    if let Some(spec_hash) = request_spec_hash {
        if active.spec_hash != spec_hash {
            let modern_resolved_retry_matches = active.execution_mode
                == MaterializedViewExecutionMode::StandingRuntime
                && request_compile_request_hash == Some(active_compile_request_hash.as_str());
            if modern_resolved_retry_matches {
                // A resolved-spec activation intentionally moves the active view
                // away from the pending admission spec hash. The compile request
                // hash remains the durable identity for retry/repair.
            } else {
                return Err(ApiError::conflict(format!(
                "active view spec hash does not match completion request: active={}, request={}",
                active.spec_hash, spec_hash
            )));
            }
        }
    }
    if let Some(compile_request_hash) = request_compile_request_hash {
        if active_compile_request_hash != compile_request_hash {
            return Err(ApiError::conflict(format!(
                "active view compile request hash does not match completion request: active={}, request={}",
                active_compile_request_hash, compile_request_hash
            )));
        }
    }
    let activation_spec = if let Some(resolved_spec) = request_resolved_spec {
        validate_resolved_compile_spec(&active.spec, resolved_spec, &active_compile_request_hash)?;
        resolved_spec.clone()
    } else {
        active.spec.clone()
    };
    if let Some(deployment) = runtime_deployment {
        validate_external_feldera_runtime_deployment_completion(&activation_spec, deployment)?;
    }
    validate_compile_deploy_claim_for_completion(
        state,
        view_id,
        &active_compile_request_hash,
        request_tenant_id,
        request_job_generation,
        request_worker_id,
        request_lease_id,
        request_fencing_token,
    )
    .await?;
    let activation_spec_hash =
        feldera_spec_hash(&activation_spec).map_err(ApiError::bad_request)?;
    if let Some(artifact_metadata) = artifact_metadata {
        if activation_spec_hash != artifact_metadata.spec_hash {
            return Err(ApiError::conflict(format!(
                "resolved view spec hash does not match artifact metadata: resolved={}, artifact={}",
                activation_spec_hash, artifact_metadata.spec_hash
            )));
        }
        validate_feldera_compile_artifact_for_compile_request(
            &activation_spec,
            artifact_metadata,
            &active_compile_request_hash,
        )
        .map_err(ApiError::bad_request)?;
    }
    let active_compile_request =
        FelderaCompileRequestV1::infer_output_from_standing_view_spec(&active.spec);
    if let Some(product_runtime) = product_runtime {
        validate_feldera_package_runtime_descriptor(
            &activation_spec,
            &active_compile_request,
            product_runtime,
        )
        .map_err(ApiError::bad_request)?;
    }
    let catalogs = read_relation_catalogs_for_spec(state, &activation_spec).await?;
    let catalog = catalogs
        .first()
        .ok_or_else(|| ApiError::bad_request("pending view has no input relation"))?;
    if active.execution_mode == MaterializedViewExecutionMode::StandingRuntime
        && request_compile_request_hash.is_some()
        && request_resolved_spec.is_some()
    {
        if let Some(artifact_metadata) = artifact_metadata {
            validate_active_standing_runtime_artifact_matches_metadata(
                &active,
                catalog,
                &activation_spec,
                artifact_metadata,
            )?;
        } else if let Some(product_runtime) = product_runtime {
            validate_active_standing_runtime_artifact_matches_product_runtime(
                &active,
                &activation_spec,
                product_runtime,
            )?;
        } else {
            return Err(ApiError::bad_request(
                "duplicate standing-runtime completion repair requires `artifact` or `product_runtime`",
            ));
        }
        repair_matching_compile_deploy_job_for_active_standing_runtime(
            state,
            &active,
            request_spec_hash,
            request_compile_request_hash,
            "standing runtime was already active; repaired compile/deploy job",
        )
        .await?;
        let active = state
            .view_registry()?
            .read_active(view_id)
            .await
            .map_err(materialized_view_registry_error_to_api)?;
        return active_view_response(&active, Some("duplicate"));
    }
    if active.execution_mode != MaterializedViewExecutionMode::FelderaCompilePending {
        return Err(ApiError::conflict(format!(
            "view `{view_id}` is not waiting for Feldera compile/deploy completion"
        )));
    }
    let job = read_pending_compile_deploy_job(
        state,
        view_id,
        &active.spec_hash,
        &active_compile_request_hash,
    )
    .await?;
    if !compile_job_request_matches_active_spec(&job, &active.spec) {
        return Err(ApiError::conflict(
            "compile/deploy job compiler_request does not match active view spec",
        ));
    }

    state.validate_standing_runtime_fencing_or_evict().await?;
    let output_schemas = activation_spec.output_relations.clone();
    let (artifact, activation_message, template_validation_mode) = if let Some(artifact_metadata) =
        artifact_metadata
    {
        (
            register_view_artifact(state, &catalogs, &activation_spec, artifact_metadata).await?,
            "standing runtime activated from completed Feldera artifact",
            SqlTemplateValidationMode::LocalDataFusion,
        )
    } else if let Some(product_runtime) = product_runtime {
        (
            feldera_package_runtime_artifact_binding(&activation_spec, product_runtime)?,
            "standing runtime activated from completed jarless Feldera package runtime",
            SqlTemplateValidationMode::LocalDataFusion,
        )
    } else {
        let deployment = runtime_deployment.expect("runtime_deployment was validated above");
        (
            external_feldera_runtime_artifact_binding(&catalogs, &activation_spec, deployment)?,
            "standing runtime activated from completed Feldera runtime deployment",
            SqlTemplateValidationMode::ExternalFelderaRuntime,
        )
    };
    let identity = artifact
        .standing_program_identity
        .as_ref()
        .ok_or_else(|| ApiError::conflict("generated artifact is missing runtime identity"))?
        .clone();
    let api_metadata = active.api.clone().unwrap_or_default();
    validate_standing_runtime_create_api_metadata(
        &activation_spec.view_id,
        &api_metadata,
        &output_schemas,
        template_validation_mode,
    )
    .await?;
    let replay_plan = if let Some((runtime, replay_plan)) =
        restore_or_build_standing_runtime_for_artifact(
            state,
            &activation_spec,
            &artifact,
            &activation_spec.input_relations,
            &output_schemas,
        )
        .await?
    {
        insert_standing_runtime(state, &activation_spec.view_id, runtime)?;
        replay_plan
    } else {
        read_latest_standing_runtime_checkpoint(state, &identity, &activation_spec.view_id)
            .await?
            .map(standing_runtime_replay_plan_from_record)
            .unwrap_or_default()
    };
    let deploying_lifecycle = MaterializedViewLifecycleStatus::standing_runtime_deploying(Some(
        "catching up committed ingest before query activation".to_string(),
    ));
    let activation = if request_resolved_spec.is_some() {
        state
            .view_registry()?
            .activate_pending_with_resolved_spec_artifact(
                &active.spec.view_id,
                &active.spec_hash,
                &activation_spec,
                artifact.clone(),
                deploying_lifecycle.clone(),
            )
            .await
            .map_err(materialized_view_registry_error_to_api)?
    } else {
        state
            .view_registry()?
            .activate_pending_with_artifact(
                &active.spec.view_id,
                &active.spec_hash,
                artifact.clone(),
                deploying_lifecycle.clone(),
            )
            .await
            .map_err(materialized_view_registry_error_to_api)?
    };
    let replay_active = ActiveMaterializedView {
        spec_hash: activation_spec_hash.clone(),
        spec: activation_spec.clone(),
        execution_mode: MaterializedViewExecutionMode::StandingRuntime,
        api: active.api.clone(),
        artifact: Some(artifact),
        lifecycle: deploying_lifecycle,
    };
    replay_committed_ingest_into_standing_runtime(state, &replay_active, &replay_plan).await?;
    let lifecycle = MaterializedViewLifecycleStatus::standing_runtime();
    let lifecycle_update = state
        .view_registry()?
        .update_standing_runtime_lifecycle(
            &activation_spec.view_id,
            &activation_spec_hash,
            lifecycle,
        )
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    mark_compile_deploy_job_running(
        state,
        &active.spec.view_id,
        &active.spec_hash,
        &active_compile_request_hash,
        activation_message.to_string(),
    )
    .await?;
    let outcome = match (activation, lifecycle_update) {
        (ActivateMaterializedViewOutcome::Activated, _) => "activated",
        (
            ActivateMaterializedViewOutcome::Duplicate,
            UpdateMaterializedViewLifecycleOutcome::Updated,
        ) => "activated",
        (
            ActivateMaterializedViewOutcome::Duplicate,
            UpdateMaterializedViewLifecycleOutcome::Duplicate,
        ) => "duplicate",
    };
    let active = state
        .view_registry()?
        .read_active(&active.spec.view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    active_view_response(&active, Some(outcome))
}

async fn read_pending_compile_deploy_job(
    state: &ApiState,
    view_id: &str,
    spec_hash: &str,
    compile_request_hash: &str,
) -> Result<ViewCompileDeployJobRecord, ApiError> {
    let registry = state.view_compile_deploy_job_registry()?;
    match registry
        .read_by_compile_request_hash(view_id, compile_request_hash)
        .await
    {
        Ok(job) => Ok(job),
        Err(ViewCompileDeployJobRegistryError::ObjectStore(object_store::Error::NotFound {
            ..
        })) => match registry.read(view_id, spec_hash).await {
            Ok(job) => Ok(job),
            Err(ViewCompileDeployJobRegistryError::ObjectStore(object_store::Error::NotFound {
                ..
            })) => Err(ApiError::conflict(format!(
                "compile/deploy job does not exist for view `{view_id}`, compile request hash `{compile_request_hash}`, and legacy spec hash `{spec_hash}`"
            ))),
            Err(error) => Err(view_compile_deploy_job_registry_error_to_api(error)),
        },
        Err(error) => Err(view_compile_deploy_job_registry_error_to_api(error)),
    }
}

async fn mark_compile_deploy_job_running(
    state: &ApiState,
    view_id: &str,
    spec_hash: &str,
    compile_request_hash: &str,
    message: String,
) -> Result<(), ApiError> {
    let registry = state.view_compile_deploy_job_registry()?;
    match registry
        .mark_running_for_compile_request_hash(view_id, compile_request_hash, Some(message.clone()))
        .await
    {
        Ok(_) => Ok(()),
        Err(ViewCompileDeployJobRegistryError::ObjectStore(object_store::Error::NotFound {
            ..
        })) => registry
            .mark_running(view_id, spec_hash, Some(message))
            .await
            .map(|_| ())
            .map_err(view_compile_deploy_job_registry_error_to_api),
        Err(error) => Err(view_compile_deploy_job_registry_error_to_api(error)),
    }
}

async fn repair_compile_deploy_job_for_active_standing_runtime(
    state: &ApiState,
    active: &ActiveMaterializedView,
    job: &ViewCompileDeployJobRecord,
    message: &str,
) -> Result<bool, ApiError> {
    if active.execution_mode != MaterializedViewExecutionMode::StandingRuntime {
        return Ok(false);
    }
    if !compile_job_request_matches_active_spec(job, &active.spec) {
        return Ok(false);
    }
    let compile_request_hash = job
        .compiler_request
        .as_ref()
        .map(|request| request.compile_request_hash.as_str())
        .ok_or_else(|| ApiError::conflict("compile/deploy job is missing compiler_request"))?;
    mark_compile_deploy_job_running(
        state,
        &active.spec.view_id,
        &job.spec_hash,
        compile_request_hash,
        message.to_string(),
    )
    .await?;
    Ok(true)
}

async fn repair_matching_compile_deploy_job_for_active_standing_runtime(
    state: &ApiState,
    active: &ActiveMaterializedView,
    request_spec_hash: Option<&str>,
    request_compile_request_hash: Option<&str>,
    message: &str,
) -> Result<(), ApiError> {
    if active.execution_mode != MaterializedViewExecutionMode::StandingRuntime {
        return Ok(());
    }
    let registry = state.view_compile_deploy_job_registry()?;
    let job = if let Some(compile_request_hash) = request_compile_request_hash {
        match registry
            .read_by_compile_request_hash(&active.spec.view_id, compile_request_hash)
            .await
        {
            Ok(job) => Some(job),
            Err(ViewCompileDeployJobRegistryError::ObjectStore(
                object_store::Error::NotFound { .. },
            )) => None,
            Err(error) => return Err(view_compile_deploy_job_registry_error_to_api(error)),
        }
    } else if let Some(spec_hash) = request_spec_hash {
        match registry.read(&active.spec.view_id, spec_hash).await {
            Ok(job) => Some(job),
            Err(ViewCompileDeployJobRegistryError::ObjectStore(
                object_store::Error::NotFound { .. },
            )) => None,
            Err(error) => return Err(view_compile_deploy_job_registry_error_to_api(error)),
        }
    } else {
        None
    };

    if let Some(job) = job {
        if !repair_compile_deploy_job_for_active_standing_runtime(state, active, &job, message)
            .await?
        {
            return Err(ApiError::conflict(
                "compile/deploy job compiler_request does not match active view spec",
            ));
        }
    }
    Ok(())
}

fn validate_active_standing_runtime_artifact_matches_metadata(
    active: &ActiveMaterializedView,
    catalog: &VelorixRelationCatalogV1,
    spec: &StandingViewSpec,
    artifact_metadata: &FelderaCompileArtifactMetadata,
) -> Result<(), ApiError> {
    let active_artifact = active
        .artifact
        .as_ref()
        .ok_or_else(|| ApiError::conflict("active standing runtime is missing artifact"))?;
    let expected_identity =
        standing_program_identity_from_artifact(catalog, spec, artifact_metadata)?;
    let matches = active_artifact.artifact_id == artifact_metadata.artifact_id
        && active_artifact.artifact_hash == artifact_metadata.artifact_hash
        && active_artifact.generated_rust_crate_name == artifact_metadata.generated_rust.crate_name
        && active_artifact.state_codec == artifact_metadata.state_codec
        && active_artifact.state_schema_version == artifact_metadata.state_schema_version
        && active_artifact.standing_program_identity.as_ref() == Some(&expected_identity);
    if !matches {
        return Err(ApiError::conflict(
            "active standing runtime artifact does not match completion artifact metadata",
        ));
    }
    Ok(())
}

fn validate_active_standing_runtime_artifact_matches_product_runtime(
    active: &ActiveMaterializedView,
    spec: &StandingViewSpec,
    descriptor: &FelderaPackageRuntimeDescriptorV1,
) -> Result<(), ApiError> {
    let active_artifact = active
        .artifact
        .as_ref()
        .ok_or_else(|| ApiError::conflict("active standing runtime is missing artifact"))?;
    let expected = feldera_package_runtime_artifact_binding(spec, descriptor)?;
    let matches = active_artifact.artifact_id == expected.artifact_id
        && active_artifact.artifact_hash == expected.artifact_hash
        && active_artifact.generated_rust_crate_name == expected.generated_rust_crate_name
        && active_artifact.state_codec == expected.state_codec
        && active_artifact.state_schema_version == expected.state_schema_version
        && active_artifact.execution_path == expected.execution_path
        && active_artifact.standing_program_identity == expected.standing_program_identity;
    if !matches {
        return Err(ApiError::conflict(
            "active standing runtime artifact does not match product runtime descriptor",
        ));
    }
    Ok(())
}

fn standing_program_identity_from_artifact(
    catalog: &VelorixRelationCatalogV1,
    spec: &StandingViewSpec,
    artifact: &FelderaCompileArtifactMetadata,
) -> Result<StandingProgramIdentity, ApiError> {
    let input_schema_bytes = serde_json::to_vec(&artifact.input_schemas)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let output_schema_bytes = serde_json::to_vec(&artifact.output_schemas)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let input_catalog_hash = if artifact.input_schemas.len() == 1 {
        catalog.schema_fingerprint.as_str().to_string()
    } else {
        feldera_artifact_bytes_hash(&input_schema_bytes)
    };
    let identity = StandingProgramIdentity {
        tenant_id: "default".to_string(),
        program_id: spec.view_id.clone(),
        view_ids: standing_program_view_ids_for_spec(spec),
        sql_hash: feldera_artifact_bytes_hash(spec.sql.as_bytes()),
        input_catalog_hash,
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

fn standing_program_identity_from_external_feldera_runtime(
    catalogs: &[VelorixRelationCatalogV1],
    spec: &StandingViewSpec,
) -> Result<StandingProgramIdentity, ApiError> {
    let input_schema_bytes = serde_json::to_vec(&spec.input_relations)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let output_schema_bytes = serde_json::to_vec(&spec.output_relations)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let input_catalog_hash = if catalogs.len() == 1 {
        catalogs[0].schema_fingerprint.as_str().to_string()
    } else {
        feldera_artifact_bytes_hash(&input_schema_bytes)
    };
    let identity = StandingProgramIdentity {
        tenant_id: "default".to_string(),
        program_id: spec.view_id.clone(),
        view_ids: standing_program_view_ids_for_spec(spec),
        sql_hash: feldera_artifact_bytes_hash(spec.sql.as_bytes()),
        input_catalog_hash,
        output_schema_hash: feldera_artifact_bytes_hash(&output_schema_bytes),
        compiler_identity: "feldera-pipeline-manager".to_string(),
        runtime_packages: vec![FelderaRuntimePackageIdentity {
            name: FELDERA_PIPELINE_MANAGER_RUNTIME_PACKAGE_NAME.to_string(),
            version: FELDERA_PIPELINE_MANAGER_RUNTIME_PACKAGE_VERSION.to_string(),
        }],
        package_feature_set: vec!["feldera_pipeline_manager_runtime".to_string()],
        dbsp_runtime_compatibility: FELDERA_PIPELINE_MANAGER_RUNTIME_PACKAGE_VERSION.to_string(),
        checkpoint_codec_identity: FELDERA_PIPELINE_MANAGER_STATE_CODEC.to_string(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
    };
    identity.validate().map_err(ApiError::bad_request)?;
    Ok(identity)
}

fn standing_program_view_ids_for_spec(spec: &StandingViewSpec) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut view_ids = Vec::new();
    for view_id in std::iter::once(&spec.view_id).chain(
        spec.output_relations
            .iter()
            .map(|schema| &schema.relation_id),
    ) {
        if seen.insert(view_id.clone()) {
            view_ids.push(view_id.clone());
        }
    }
    view_ids
}

async fn ensure_standing_runtime_for_artifact(
    state: &ApiState,
    spec: &StandingViewSpec,
    artifact: &MaterializedViewArtifactBinding,
) -> Result<Option<StandingRuntimeReplayPlan>, ApiError> {
    let Some((runtime, replay_plan)) = restore_or_build_standing_runtime_for_artifact(
        state,
        spec,
        artifact,
        &spec.input_relations,
        &spec.output_relations,
    )
    .await
    .map_err(|error| active_artifact_runtime_unavailable_error(&spec.view_id, artifact, error))?
    else {
        return Ok(None);
    };
    let committed_checkpoint =
        read_latest_standing_runtime_checkpoint(state, runtime.program_identity(), &spec.view_id)
            .await?
            .as_ref()
            .map(standing_runtime_checkpoint_pointer_from_record);
    state.set_standing_runtime_committed_checkpoint(
        runtime.program_identity(),
        &spec.view_id,
        committed_checkpoint,
    )?;
    insert_standing_runtime(state, &spec.view_id, runtime)?;
    Ok(Some(replay_plan))
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
    spec: &StandingViewSpec,
    artifact: &MaterializedViewArtifactBinding,
    catalogs: &[VelorixRelationCatalogV1],
    expected_input_schemas: &[RelationSchema],
    expected_output_schemas: &[RelationSchema],
) -> Result<Option<Box<dyn StandingProgramRuntime + Send>>, ApiError> {
    let Some(identity) = artifact.standing_program_identity.as_ref() else {
        return Ok(None);
    };
    if state.standing_runtime(identity, &spec.view_id)?.is_some() {
        return Ok(None);
    }
    let Some(factory) = state.standing_runtime_factory(&artifact.generated_rust_crate_name)? else {
        return Err(ApiError::bad_request(format!(
            "standing runtime factory is not registered for generated Rust crate `{}`",
            artifact.generated_rust_crate_name
        )));
    };
    let runtime = factory
        .create_with_catalogs_and_spec(
            identity,
            catalogs,
            spec,
            expected_input_schemas,
            expected_output_schemas,
        )
        .map_err(ApiError::internal)?;
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
    spec: &StandingViewSpec,
    artifact: &MaterializedViewArtifactBinding,
    expected_input_schemas: &[RelationSchema],
    expected_output_schemas: &[RelationSchema],
) -> Result<
    Option<(
        Box<dyn StandingProgramRuntime + Send>,
        StandingRuntimeReplayPlan,
    )>,
    ApiError,
> {
    let Some(identity) = artifact.standing_program_identity.as_ref() else {
        return Ok(None);
    };
    if state.standing_runtime(identity, &spec.view_id)?.is_some() {
        return Ok(None);
    }
    let Some(factory) = state.standing_runtime_factory(&artifact.generated_rust_crate_name)? else {
        return Err(ApiError::bad_request(format!(
            "standing runtime factory is not registered for generated Rust crate `{}`",
            artifact.generated_rust_crate_name
        )));
    };
    let catalogs = read_relation_catalogs_for_input_schemas(state, expected_input_schemas).await?;

    let (runtime, replay_plan) = if let Some(record) =
        read_latest_standing_runtime_checkpoint(state, identity, &spec.view_id).await?
    {
        record
            .checkpoint
            .validate_identity(identity)
            .map_err(ApiError::bad_request)?;
        if record.checkpoint.state_payload.is_some() {
            let replay_plan = standing_runtime_replay_plan_from_record_ref(&record);
            let factory = Arc::clone(&factory);
            let catalogs = catalogs.clone();
            let spec = spec.clone();
            let expected_input_schemas = expected_input_schemas.to_vec();
            let expected_output_schemas = expected_output_schemas.to_vec();
            (
                tokio::task::spawn_blocking(move || {
                    factory.restore_with_catalogs_and_spec(
                        record.checkpoint,
                        &catalogs,
                        &spec,
                        &expected_input_schemas,
                        &expected_output_schemas,
                    )
                })
                .await
                .map_err(ApiError::internal)?
                .map_err(ApiError::internal)?,
                replay_plan,
            )
        } else {
            let factory = Arc::clone(&factory);
            let identity = identity.clone();
            let catalogs = catalogs.clone();
            let spec = spec.clone();
            let expected_input_schemas = expected_input_schemas.to_vec();
            let expected_output_schemas = expected_output_schemas.to_vec();
            (
                tokio::task::spawn_blocking(move || {
                    factory.create_with_catalogs_and_spec(
                        &identity,
                        &catalogs,
                        &spec,
                        &expected_input_schemas,
                        &expected_output_schemas,
                    )
                })
                .await
                .map_err(ApiError::internal)?
                .map_err(ApiError::internal)?,
                StandingRuntimeReplayPlan::default(),
            )
        }
    } else {
        let factory = Arc::clone(&factory);
        let identity = identity.clone();
        let catalogs = catalogs.clone();
        let spec = spec.clone();
        let expected_input_schemas = expected_input_schemas.to_vec();
        let expected_output_schemas = expected_output_schemas.to_vec();
        (
            tokio::task::spawn_blocking(move || {
                factory.create_with_catalogs_and_spec(
                    &identity,
                    &catalogs,
                    &spec,
                    &expected_input_schemas,
                    &expected_output_schemas,
                )
            })
            .await
            .map_err(ApiError::internal)?
            .map_err(ApiError::internal)?,
            StandingRuntimeReplayPlan::default(),
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
    Ok(Some((runtime, replay_plan)))
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
    remove_standing_runtime_if_present(state, identity, view_id).map(|_| ())
}

fn remove_standing_runtime_if_present(
    state: &ApiState,
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> Result<bool, ApiError> {
    let mut runtimes = state
        .standing_runtimes
        .runtimes
        .lock()
        .map_err(|_| ApiError::internal("standing runtime registry lock poisoned"))?;
    let key = standing_runtime_key(identity, view_id);
    let removed_runtime = runtimes.remove(&key).is_some();
    let mut local_state = state
        .standing_runtimes
        .local_state
        .lock()
        .map_err(|_| ApiError::internal("standing runtime local state lock poisoned"))?;
    let removed_local_state = local_state.remove(&key).is_some();
    Ok(removed_runtime || removed_local_state)
}

fn compile_job_request_matches_active_spec(
    job: &ViewCompileDeployJobRecord,
    spec: &StandingViewSpec,
) -> bool {
    let Some(request) = &job.compiler_request else {
        return false;
    };
    let expected = FelderaCompileRequestV1::infer_output_from_standing_view_spec(spec);

    request.view_id == spec.view_id
        && request.compile_request_hash
            == feldera_compile_request_hash(&expected).unwrap_or_default()
        && request.spec_hash == job.spec_hash
        && request.sql == spec.sql
        && request.dialect == spec.dialect
        && request.source_kind == spec.source_kind
        && request.rust_extension == spec.rust_extension
        && request.input_relations == spec.input_relations
        && request.output_contract == OutputSchemaContract::Infer
        && request.output_relations.is_empty()
        && request.shape == expected.shape
}

fn validate_resolved_compile_spec(
    pending_spec: &StandingViewSpec,
    resolved_spec: &StandingViewSpec,
    expected_compile_request_hash: &str,
) -> Result<(), ApiError> {
    if resolved_spec.view_id != pending_spec.view_id
        || resolved_spec.sql != pending_spec.sql
        || resolved_spec.dialect != pending_spec.dialect
        || resolved_spec.source_kind != pending_spec.source_kind
        || resolved_spec.rust_extension != pending_spec.rust_extension
        || resolved_spec.input_relations != pending_spec.input_relations
        || resolved_spec.shape.is_materialized != pending_spec.shape.is_materialized
        || resolved_spec.shape.multi_input != pending_spec.shape.multi_input
        || resolved_spec.shape.multi_output != (resolved_spec.output_relations.len() > 1)
    {
        return Err(ApiError::conflict(
            "resolved Feldera compile spec does not match pending compiler request identity",
        ));
    }
    if resolved_spec.output_relations.is_empty() {
        return Err(ApiError::bad_request(
            "resolved Feldera compile spec must include compiler-inferred output relations",
        ));
    }
    validate_feldera_program_output_hints(pending_spec, resolved_spec)?;
    validate_feldera_runtime_spec_admission(resolved_spec)?;
    let actual_compile_request_hash = compile_request_hash_for_spec(resolved_spec)?;
    if actual_compile_request_hash != expected_compile_request_hash {
        return Err(ApiError::conflict(format!(
            "resolved Feldera compile request hash does not match pending request: expected={}, actual={}",
            expected_compile_request_hash, actual_compile_request_hash
        )));
    }
    Ok(())
}

fn validate_feldera_program_output_hints(
    pending_spec: &StandingViewSpec,
    resolved_spec: &StandingViewSpec,
) -> Result<(), ApiError> {
    if !feldera_program_output_hints_are_present(pending_spec) {
        return Ok(());
    }
    let expected = pending_spec
        .output_relations
        .iter()
        .map(|schema| schema.relation_id.as_str())
        .collect::<BTreeSet<_>>();
    let actual = resolved_spec
        .output_relations
        .iter()
        .map(|schema| schema.relation_id.as_str())
        .collect::<BTreeSet<_>>();
    if !feldera_output_hint_relation_ids_match(&expected, &actual) {
        return Err(ApiError::bad_request(format!(
            "resolved Feldera program output relations do not match requested output_relation_ids: expected={expected:?}, actual={actual:?}"
        )));
    }
    Ok(())
}

fn resolved_compile_spec_with_pending_output_relation_ids(
    pending_spec: &StandingViewSpec,
    mut resolved_spec: StandingViewSpec,
) -> StandingViewSpec {
    if !feldera_program_output_hints_are_present(pending_spec) {
        return resolved_spec;
    }
    let expected = pending_spec
        .output_relations
        .iter()
        .map(|schema| schema.relation_id.as_str())
        .collect::<BTreeSet<_>>();
    let actual = resolved_spec
        .output_relations
        .iter()
        .map(|schema| schema.relation_id.as_str())
        .collect::<BTreeSet<_>>();
    if expected == actual || !feldera_output_hint_relation_ids_match(&expected, &actual) {
        return resolved_spec;
    }
    let Some(pending_ids_by_folded_id) =
        feldera_pending_output_relation_ids_by_folded_id(pending_spec)
    else {
        return resolved_spec;
    };
    for output_schema in &mut resolved_spec.output_relations {
        if let Some(pending_id) =
            pending_ids_by_folded_id.get(&output_schema.relation_id.to_ascii_lowercase())
        {
            output_schema.relation_id = pending_id.clone();
        }
    }
    resolved_spec
}

fn feldera_program_output_hints_are_present(spec: &StandingViewSpec) -> bool {
    spec.source_kind == SqlSourceKind::FelderaProgram
        && !spec.output_relations.is_empty()
        && !(spec.output_relations.len() == 1
            && spec.output_relations[0].relation_id == spec.view_id)
}

fn feldera_output_hint_relation_ids_match(
    expected: &BTreeSet<&str>,
    actual: &BTreeSet<&str>,
) -> bool {
    expected == actual
        || match (
            feldera_non_ambiguous_case_folded_relation_ids(expected),
            feldera_non_ambiguous_case_folded_relation_ids(actual),
        ) {
            (Some(expected), Some(actual)) => expected == actual,
            _ => false,
        }
}

fn feldera_non_ambiguous_case_folded_relation_ids(
    ids: &BTreeSet<&str>,
) -> Option<BTreeSet<String>> {
    let mut folded = BTreeSet::new();
    for id in ids {
        if !folded.insert(id.to_ascii_lowercase()) {
            return None;
        }
    }
    Some(folded)
}

fn feldera_pending_output_relation_ids_by_folded_id(
    spec: &StandingViewSpec,
) -> Option<BTreeMap<String, String>> {
    let mut ids = BTreeMap::new();
    for schema in &spec.output_relations {
        if ids
            .insert(
                schema.relation_id.to_ascii_lowercase(),
                schema.relation_id.clone(),
            )
            .is_some()
        {
            return None;
        }
    }
    Some(ids)
}

fn validate_feldera_runtime_spec_admission(spec: &StandingViewSpec) -> Result<(), ApiError> {
    validate_feldera_compile_request(
        &FelderaCompileRequestV1::infer_output_from_standing_view_spec(spec),
    )
    .map_err(ApiError::bad_request)?;
    validate_feldera_runtime_relation_schemas_admission(
        "spec.input_relations",
        &spec.input_relations,
    )?;
    validate_feldera_runtime_relation_schemas_admission(
        "spec.output_relations",
        &spec.output_relations,
    )?;
    Ok(())
}

fn validate_feldera_runtime_relation_schemas_admission(
    field: &str,
    schemas: &[RelationSchema],
) -> Result<(), ApiError> {
    for schema in schemas {
        for column in &schema.columns {
            validate_feldera_runtime_sql_type_admission(
                &format!("{field}.{}.{}", schema.relation_id, column.name),
                &column.data_type,
            )?;
        }
    }
    Ok(())
}

fn validate_feldera_runtime_sql_type_admission(
    field: &str,
    data_type: &SqlDataType,
) -> Result<(), ApiError> {
    match data_type {
        SqlDataType::Timestamp {
            timezone: Some(timezone),
        } => Err(ApiError::bad_request(format!(
            "Feldera runtime admission rejected `{field}`: timezone-bearing timestamps are not supported yet; timezone={timezone}"
        ))),
        SqlDataType::Array { element_type } => {
            validate_feldera_runtime_sql_type_admission(field, element_type)
        }
        SqlDataType::Struct { fields } => {
            for struct_field in fields {
                validate_feldera_runtime_sql_type_admission(
                    &format!("{field}.{}", struct_field.name),
                    &struct_field.data_type,
                )?;
            }
            Ok(())
        }
        SqlDataType::Map {
            key_type,
            value_type,
        } => {
            validate_feldera_runtime_sql_type_admission(&format!("{field}.key"), key_type)?;
            validate_feldera_runtime_sql_type_admission(&format!("{field}.value"), value_type)
        }
        other => {
            arrow_data_type_from_sql_data_type(other)?;
            Ok(())
        }
    }
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

fn unix_epoch_millis() -> Result<u64, ApiError> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ApiError::internal("system clock is before unix epoch"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| ApiError::internal("system clock millis overflowed u64"))
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
        .map(|view| active_view_response(view, None))
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

    Ok(Json(active_view_response(&active, None)?))
}

struct PreparedIngestBatch {
    request: IngestRowsRequest,
    catalog: VelorixRelationCatalogV1,
    record_batch: RecordBatch,
    end_offset_exclusive: u64,
    payload_digest: String,
    envelope: bytes::Bytes,
}

#[derive(Clone, Debug)]
struct PersistedIngestEpochManifest {
    epoch_manifest_id: String,
    epoch_manifest_key: String,
}

#[derive(Clone, Debug, Serialize)]
struct IngestEpochManifestRecord {
    schema_version: u16,
    record_kind: String,
    epoch_manifest_id: String,
    batches: Vec<IngestEpochManifestBatchRecord>,
}

#[derive(Clone, Debug, Serialize)]
struct IngestEpochManifestBatchRecord {
    relation_id: String,
    relation_version: String,
    schema_fingerprint: String,
    stream_id: String,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    payload_digest: String,
    batch_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct IngestEpochViewConvergenceRecord {
    schema_version: u16,
    record_kind: String,
    epoch_manifest_id: String,
    tenant_id: String,
    program_id: String,
    view_id: String,
    logical_epoch: u64,
    checkpoint_key: String,
    checkpoint_content_hash: String,
    replay_checkpoints: Vec<ReplayCheckpoint>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct IngestEpochViewRuntimeFailureRecord {
    schema_version: u16,
    record_kind: String,
    epoch_manifest_id: String,
    tenant_id: String,
    program_id: String,
    view_id: String,
    failure_reason: String,
    replay_checkpoints: Vec<ReplayCheckpoint>,
}

async fn ingest_rows(
    State(state): State<ApiState>,
    Json(request): Json<IngestRowsRequest>,
) -> Result<(StatusCode, Json<IngestResponse>), ApiError> {
    let prepared = prepare_ingest_batch(&state, request).await?;
    ensure_standing_runtimes_for_ingest(&state, &prepared.request).await?;
    preacquire_standing_runtime_owners_for_ingest(&state, &prepared.request).await?;
    if state.meta_store.is_some() {
        reserve_ingest_range(
            &state,
            &prepared.request,
            &prepared.catalog,
            prepared.end_offset_exclusive,
            &prepared.envelope,
        )
        .await?;
    }
    let outcome = append_ingest_envelope(&state, prepared.envelope).await?;
    let (status, outcome, descriptor) = ingest_outcome_parts(outcome)?;
    if matches!(outcome, "appended" | "duplicate") {
        apply_standing_runtime_ingest(&state, &prepared.request).await?;
    }

    Ok((
        status,
        Json(IngestResponse {
            outcome: outcome.to_string(),
            descriptor: ingest_descriptor_response(&descriptor),
        }),
    ))
}

async fn ingest_epoch(
    State(state): State<ApiState>,
    Json(request): Json<IngestEpochRequest>,
) -> Result<(StatusCode, Json<IngestEpochResponse>), ApiError> {
    if request.batches.is_empty() {
        return Err(ApiError::bad_request(
            "ingest epoch must contain at least one batch",
        ));
    }
    if request.batches.iter().any(|batch| batch.rows.is_empty()) {
        return Err(ApiError::bad_request(
            "ingest epoch batches must contain at least one row",
        ));
    }
    let total_rows = request
        .batches
        .iter()
        .try_fold(0usize, |total, batch| total.checked_add(batch.rows.len()))
        .ok_or_else(|| ApiError::bad_request("ingest epoch row count overflow"))?;
    if total_rows > state.max_ingest_rows {
        return Err(ApiError::payload_too_large(format!(
            "ingest epoch row count {total_rows} exceeds configured limit {}",
            state.max_ingest_rows
        )));
    }

    let mut prepared_batches = Vec::with_capacity(request.batches.len());
    for batch in request.batches {
        prepared_batches.push(prepare_ingest_batch(&state, batch).await?);
    }
    let canonical_total_rows = prepared_batches
        .iter()
        .try_fold(0usize, |total, batch| {
            total.checked_add(batch.request.rows.len())
        })
        .ok_or_else(|| ApiError::bad_request("canonical ingest epoch row count overflow"))?;
    if canonical_total_rows > state.max_ingest_rows {
        return Err(ApiError::payload_too_large(format!(
            "canonical ingest epoch row count {canonical_total_rows} exceeds configured limit {}",
            state.max_ingest_rows
        )));
    }
    validate_ingest_epoch_batch_ranges(&prepared_batches)?;
    let epoch_manifest = persist_ingest_epoch_manifest(&state, &prepared_batches).await?;
    ensure_no_ingest_epoch_view_runtime_failures(&state, &epoch_manifest, &prepared_batches)
        .await?;
    for prepared in &prepared_batches {
        ensure_standing_runtimes_for_ingest(&state, &prepared.request).await?;
    }
    for prepared in &prepared_batches {
        preacquire_standing_runtime_owners_for_ingest(&state, &prepared.request).await?;
    }
    if state.meta_store.is_some() {
        for prepared in &prepared_batches {
            reserve_ingest_range(
                &state,
                &prepared.request,
                &prepared.catalog,
                prepared.end_offset_exclusive,
                &prepared.envelope,
            )
            .await?;
        }
    }
    let epoch_requests = prepared_batches
        .iter()
        .map(|prepared| prepared.request.clone())
        .collect::<Vec<_>>();
    apply_standing_runtime_ingests_for_epoch_repair(
        &state,
        &epoch_manifest,
        &prepared_batches,
        &epoch_requests,
    )
    .await?;

    let mut responses = Vec::with_capacity(prepared_batches.len());
    let mut appended = 0usize;
    for prepared in &prepared_batches {
        let outcome = append_ingest_envelope(&state, prepared.envelope.clone()).await?;
        let (_status, outcome, descriptor) = ingest_outcome_parts(outcome)?;
        if outcome == "appended" {
            appended += 1;
        }
        responses.push(IngestResponse {
            outcome: outcome.to_string(),
            descriptor: ingest_descriptor_response(&descriptor),
        });
    }

    apply_standing_runtime_ingest_epoch(&state, &epoch_manifest, &prepared_batches).await?;

    let (status, outcome) = if appended > 0 {
        (StatusCode::CREATED, "appended")
    } else {
        (StatusCode::OK, "duplicate")
    };
    Ok((
        status,
        Json(IngestEpochResponse {
            outcome: outcome.to_string(),
            epoch_manifest_id: epoch_manifest.epoch_manifest_id,
            epoch_manifest_key: epoch_manifest.epoch_manifest_key,
            batches: responses,
        }),
    ))
}

async fn repair_ingest_epoch_runtime_failure(
    State(state): State<ApiState>,
    Json(request): Json<RepairIngestEpochRuntimeFailureRequest>,
) -> Result<Json<RepairIngestEpochRuntimeFailureResponse>, ApiError> {
    if !request.confirm_external_runtime_rebuilt {
        return Err(ApiError::bad_request(
            "confirm_external_runtime_rebuilt must be true after the external runtime has been rebuilt or cleared",
        ));
    }
    let repair_reason = request.repair_reason.trim();
    if repair_reason.is_empty() {
        return Err(ApiError::bad_request("repair_reason must not be empty"));
    }
    let active = state
        .view_registry()?
        .read_active(&request.view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let identity = active
        .artifact
        .as_ref()
        .and_then(|artifact| artifact.standing_program_identity.as_ref())
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "view `{}` is not backed by a standing runtime identity",
                request.view_id
            ))
        })?;
    if identity.tenant_id != request.tenant_id || identity.program_id != request.program_id {
        return Err(ApiError::bad_request(format!(
            "repair request identity does not match active view `{}`",
            request.view_id
        )));
    }
    let epoch_manifest = PersistedIngestEpochManifest {
        epoch_manifest_id: request.epoch_manifest_id.clone(),
        epoch_manifest_key: ObjectKey::ingest_epoch_manifest(&request.epoch_manifest_id)
            .map_err(ApiError::bad_request)?
            .as_str()
            .to_string(),
    };
    let marker_key = ObjectKey::ingest_epoch_view_runtime_failure(
        &request.epoch_manifest_id,
        &request.tenant_id,
        &request.program_id,
        &request.view_id,
    )
    .map_err(ApiError::bad_request)?;
    let failure =
        read_ingest_epoch_view_runtime_failure(&state, &epoch_manifest, identity, &request.view_id)
            .await?
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "ingest epoch runtime failure marker does not exist at {}",
                    marker_key.as_str()
                ))
            })?;
    state
        .store
        .delete(&ObjectPath::from(marker_key.as_str()))
        .await
        .map_err(ApiError::internal)?;
    let removed_runtime_cache =
        remove_standing_runtime_if_present(&state, identity, &request.view_id)?;

    Ok(Json(RepairIngestEpochRuntimeFailureResponse {
        outcome: "repaired".to_string(),
        marker_key: marker_key.as_str().to_string(),
        tenant_id: request.tenant_id,
        program_id: request.program_id,
        view_id: request.view_id,
        epoch_manifest_id: request.epoch_manifest_id,
        removed_runtime_cache,
        failure_reason: failure.failure_reason,
        repair_reason: repair_reason.to_string(),
    }))
}

async fn prepare_ingest_batch(
    state: &ApiState,
    mut request: IngestRowsRequest,
) -> Result<PreparedIngestBatch, ApiError> {
    if request.rows.len() > state.max_ingest_rows {
        return Err(ApiError::payload_too_large(format!(
            "ingest row count {} exceeds configured limit {}",
            request.rows.len(),
            state.max_ingest_rows
        )));
    }
    let catalog =
        read_relation_catalog(state, &request.relation_id, &request.relation_version).await?;
    request.rows = normalize_ingest_operation_envelopes(&catalog, &request.rows)?;
    if request.rows.len() > state.max_ingest_rows {
        return Err(ApiError::payload_too_large(format!(
            "canonical ingest row count {} exceeds configured limit {}",
            request.rows.len(),
            state.max_ingest_rows
        )));
    }
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
        std::slice::from_ref(&batch),
    )
    .map_err(ApiError::bad_request)?;
    let payload_digest = IngestEnvelope::decode(envelope.clone())
        .map_err(ApiError::bad_request)?
        .header()
        .payload_digest
        .clone();
    Ok(PreparedIngestBatch {
        request,
        catalog,
        record_batch: batch,
        end_offset_exclusive,
        payload_digest,
        envelope,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IngestEpochRange {
    relation_id: String,
    relation_version: String,
    schema_fingerprint: String,
    stream_id: String,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
}

fn validate_ingest_epoch_batch_ranges(
    prepared_batches: &[PreparedIngestBatch],
) -> Result<(), ApiError> {
    let mut ranges = prepared_batches
        .iter()
        .map(|prepared| IngestEpochRange {
            relation_id: prepared.request.relation_id.clone(),
            relation_version: prepared.request.relation_version.clone(),
            schema_fingerprint: prepared.catalog.schema_fingerprint.as_str().to_string(),
            stream_id: prepared.request.stream_id.clone(),
            partition_id: prepared.request.partition_id,
            start_offset_inclusive: prepared.request.start_offset_inclusive,
            end_offset_exclusive: prepared.end_offset_exclusive,
        })
        .collect::<Vec<_>>();
    ranges.sort_by(|left, right| {
        (
            left.relation_id.as_str(),
            left.relation_version.as_str(),
            left.schema_fingerprint.as_str(),
            left.stream_id.as_str(),
            left.partition_id,
            left.start_offset_inclusive,
            left.end_offset_exclusive,
        )
            .cmp(&(
                right.relation_id.as_str(),
                right.relation_version.as_str(),
                right.schema_fingerprint.as_str(),
                right.stream_id.as_str(),
                right.partition_id,
                right.start_offset_inclusive,
                right.end_offset_exclusive,
            ))
    });

    for pair in ranges.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if !same_ingest_epoch_source(previous, current) {
            continue;
        }
        if previous.start_offset_inclusive == current.start_offset_inclusive
            && previous.end_offset_exclusive == current.end_offset_exclusive
        {
            return Err(ApiError::bad_request(format!(
                "duplicate ingest epoch range for relation={} version={} stream={} partition={} offsets={}-{}",
                current.relation_id,
                current.relation_version,
                current.stream_id,
                current.partition_id,
                current.start_offset_inclusive,
                current.end_offset_exclusive
            )));
        }
        if current.start_offset_inclusive < previous.end_offset_exclusive {
            return Err(ApiError::bad_request(format!(
                "overlapping ingest epoch ranges for relation={} version={} stream={} partition={} previous_offsets={}-{} current_offsets={}-{}",
                current.relation_id,
                current.relation_version,
                current.stream_id,
                current.partition_id,
                previous.start_offset_inclusive,
                previous.end_offset_exclusive,
                current.start_offset_inclusive,
                current.end_offset_exclusive
            )));
        }
    }

    Ok(())
}

fn same_ingest_epoch_source(left: &IngestEpochRange, right: &IngestEpochRange) -> bool {
    left.relation_id == right.relation_id
        && left.relation_version == right.relation_version
        && left.schema_fingerprint == right.schema_fingerprint
        && left.stream_id == right.stream_id
        && left.partition_id == right.partition_id
}

async fn persist_ingest_epoch_manifest(
    state: &ApiState,
    prepared_batches: &[PreparedIngestBatch],
) -> Result<PersistedIngestEpochManifest, ApiError> {
    let mut batch_records = prepared_batches
        .iter()
        .map(ingest_epoch_manifest_batch_record)
        .collect::<Result<Vec<_>, _>>()?;
    batch_records.sort_by(|left, right| {
        (
            left.relation_id.as_str(),
            left.relation_version.as_str(),
            left.schema_fingerprint.as_str(),
            left.stream_id.as_str(),
            left.partition_id,
            left.start_offset_inclusive,
            left.end_offset_exclusive,
            left.payload_digest.as_str(),
        )
            .cmp(&(
                right.relation_id.as_str(),
                right.relation_version.as_str(),
                right.schema_fingerprint.as_str(),
                right.stream_id.as_str(),
                right.partition_id,
                right.start_offset_inclusive,
                right.end_offset_exclusive,
                right.payload_digest.as_str(),
            ))
    });
    let epoch_manifest_id = ingest_epoch_manifest_id(&batch_records)?;
    let record = IngestEpochManifestRecord {
        schema_version: 1,
        record_kind: "ingest_epoch_manifest_v1".to_string(),
        epoch_manifest_id: epoch_manifest_id.clone(),
        batches: batch_records,
    };
    let bytes =
        serde_json::to_vec(&record).map_err(|source| ApiError::internal(source.to_string()))?;
    let key =
        ObjectKey::ingest_epoch_manifest(&epoch_manifest_id).map_err(ApiError::bad_request)?;
    let path = ObjectPath::from(key.as_str());
    let result = state
        .store
        .put_opts(
            &path,
            bytes::Bytes::from(bytes.clone()).into(),
            PutMode::Create.into(),
        )
        .await;
    match result {
        Ok(_) => {}
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = state
                .store
                .get(&path)
                .await
                .map_err(ApiError::internal)?
                .bytes()
                .await
                .map_err(ApiError::internal)?;
            if existing.as_ref() != bytes.as_slice() {
                return Err(ApiError::conflict(format!(
                    "ingest epoch manifest conflict at {}",
                    key.as_str()
                )));
            }
        }
        Err(error) => return Err(ApiError::internal(error)),
    }
    Ok(PersistedIngestEpochManifest {
        epoch_manifest_id,
        epoch_manifest_key: key.as_str().to_string(),
    })
}

fn ingest_epoch_manifest_batch_record(
    prepared: &PreparedIngestBatch,
) -> Result<IngestEpochManifestBatchRecord, ApiError> {
    let batch_key = ObjectKey::ingest_batch(
        &prepared.request.stream_id,
        prepared.request.partition_id,
        prepared.request.start_offset_inclusive,
        prepared.end_offset_exclusive,
    )
    .map_err(ApiError::bad_request)?;
    Ok(IngestEpochManifestBatchRecord {
        relation_id: prepared.request.relation_id.clone(),
        relation_version: prepared.request.relation_version.clone(),
        schema_fingerprint: prepared.catalog.schema_fingerprint.as_str().to_string(),
        stream_id: prepared.request.stream_id.clone(),
        partition_id: prepared.request.partition_id,
        start_offset_inclusive: prepared.request.start_offset_inclusive,
        end_offset_exclusive: prepared.end_offset_exclusive,
        payload_digest: prepared.payload_digest.clone(),
        batch_key: batch_key.as_str().to_string(),
    })
}

fn ingest_epoch_manifest_id(
    batch_records: &[IngestEpochManifestBatchRecord],
) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(batch_records)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let mut hasher = Sha256::new();
    hasher.update(b"velorix.ingest-epoch.manifest.v1\0");
    hasher.update(&bytes);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

async fn persist_ingest_epoch_view_convergence(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    view_id: &str,
    checkpoint: &RuntimeCheckpoint,
    replay_checkpoints: Vec<ReplayCheckpoint>,
) -> Result<(), ApiError> {
    let key = ObjectKey::ingest_epoch_view_convergence(
        &epoch_manifest.epoch_manifest_id,
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
    )
    .map_err(ApiError::bad_request)?;
    let checkpoint_key = ObjectKey::standing_runtime_checkpoint(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
        checkpoint.logical_epoch,
        &checkpoint.state_root.content_hash,
    )
    .map_err(ApiError::bad_request)?;
    let record = IngestEpochViewConvergenceRecord {
        schema_version: 1,
        record_kind: "ingest_epoch_view_convergence_v1".to_string(),
        epoch_manifest_id: epoch_manifest.epoch_manifest_id.clone(),
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        view_id: view_id.to_string(),
        logical_epoch: checkpoint.logical_epoch,
        checkpoint_key: checkpoint_key.as_str().to_string(),
        checkpoint_content_hash: checkpoint.state_root.content_hash.clone(),
        replay_checkpoints,
    };
    let bytes =
        serde_json::to_vec(&record).map_err(|source| ApiError::internal(source.to_string()))?;
    let path = ObjectPath::from(key.as_str());
    let result = state
        .store
        .put_opts(
            &path,
            bytes::Bytes::from(bytes.clone()).into(),
            PutMode::Create.into(),
        )
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = state
                .store
                .get(&path)
                .await
                .map_err(ApiError::internal)?
                .bytes()
                .await
                .map_err(ApiError::internal)?;
            if existing.as_ref() == bytes.as_slice() {
                Ok(())
            } else {
                Err(ApiError::conflict(format!(
                    "ingest epoch view convergence conflict at {}",
                    key.as_str()
                )))
            }
        }
        Err(error) => Err(ApiError::internal(error)),
    }
}

async fn read_ingest_epoch_view_convergence(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> Result<Option<IngestEpochViewConvergenceRecord>, ApiError> {
    let key = ObjectKey::ingest_epoch_view_convergence(
        &epoch_manifest.epoch_manifest_id,
        &identity.tenant_id,
        &identity.program_id,
        view_id,
    )
    .map_err(ApiError::bad_request)?;
    let path = ObjectPath::from(key.as_str());
    let bytes = match state.store.get(&path).await {
        Ok(result) => result.bytes().await.map_err(ApiError::internal)?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(ApiError::internal(error)),
    };
    let record: IngestEpochViewConvergenceRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    validate_ingest_epoch_view_convergence_record(
        &record,
        epoch_manifest,
        identity,
        view_id,
        key.as_str(),
    )?;
    let pointer = StandingRuntimeCheckpointPointer {
        tenant_id: record.tenant_id.clone(),
        program_id: record.program_id.clone(),
        view_id: record.view_id.clone(),
        checkpoint_key: record.checkpoint_key.clone(),
        logical_epoch: record.logical_epoch,
        content_hash: record.checkpoint_content_hash.clone(),
    };
    let checkpoint =
        read_standing_runtime_checkpoint_record_from_pointer(state, identity, view_id, &pointer)
            .await?;
    if checkpoint.checkpoint.logical_epoch != record.logical_epoch
        || checkpoint.checkpoint.state_root.content_hash != record.checkpoint_content_hash
    {
        return Err(ApiError::bad_request(format!(
            "ingest epoch view convergence checkpoint mismatch at {}",
            key.as_str()
        )));
    }
    Ok(Some(record))
}

fn validate_ingest_epoch_view_convergence_record(
    record: &IngestEpochViewConvergenceRecord,
    epoch_manifest: &PersistedIngestEpochManifest,
    identity: &StandingProgramIdentity,
    view_id: &str,
    key: &str,
) -> Result<(), ApiError> {
    if record.schema_version != 1
        || record.record_kind != "ingest_epoch_view_convergence_v1"
        || record.epoch_manifest_id != epoch_manifest.epoch_manifest_id
        || record.tenant_id != identity.tenant_id
        || record.program_id != identity.program_id
        || record.view_id != view_id
    {
        return Err(ApiError::bad_request(format!(
            "ingest epoch view convergence body/key mismatch at {key}"
        )));
    }
    Ok(())
}

async fn persist_ingest_epoch_view_runtime_failure(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    identity: &StandingProgramIdentity,
    view_id: &str,
    failure_reason: String,
    replay_checkpoints: Vec<ReplayCheckpoint>,
) -> Result<(), ApiError> {
    let key = ObjectKey::ingest_epoch_view_runtime_failure(
        &epoch_manifest.epoch_manifest_id,
        &identity.tenant_id,
        &identity.program_id,
        view_id,
    )
    .map_err(ApiError::bad_request)?;
    let record = IngestEpochViewRuntimeFailureRecord {
        schema_version: 1,
        record_kind: "ingest_epoch_view_runtime_failure_v1".to_string(),
        epoch_manifest_id: epoch_manifest.epoch_manifest_id.clone(),
        tenant_id: identity.tenant_id.clone(),
        program_id: identity.program_id.clone(),
        view_id: view_id.to_string(),
        failure_reason,
        replay_checkpoints,
    };
    let bytes =
        serde_json::to_vec(&record).map_err(|source| ApiError::internal(source.to_string()))?;
    let path = ObjectPath::from(key.as_str());
    let result = state
        .store
        .put_opts(
            &path,
            bytes::Bytes::from(bytes.clone()).into(),
            PutMode::Create.into(),
        )
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = state
                .store
                .get(&path)
                .await
                .map_err(ApiError::internal)?
                .bytes()
                .await
                .map_err(ApiError::internal)?;
            if existing.as_ref() == bytes.as_slice() {
                Ok(())
            } else {
                Err(ApiError::conflict(format!(
                    "ingest epoch view runtime failure conflict at {}",
                    key.as_str()
                )))
            }
        }
        Err(error) => Err(ApiError::internal(error)),
    }
}

async fn read_ingest_epoch_view_runtime_failure(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    identity: &StandingProgramIdentity,
    view_id: &str,
) -> Result<Option<IngestEpochViewRuntimeFailureRecord>, ApiError> {
    let key = ObjectKey::ingest_epoch_view_runtime_failure(
        &epoch_manifest.epoch_manifest_id,
        &identity.tenant_id,
        &identity.program_id,
        view_id,
    )
    .map_err(ApiError::bad_request)?;
    let path = ObjectPath::from(key.as_str());
    let bytes = match state.store.get(&path).await {
        Ok(result) => result.bytes().await.map_err(ApiError::internal)?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(ApiError::internal(error)),
    };
    let record: IngestEpochViewRuntimeFailureRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    validate_ingest_epoch_view_runtime_failure_record(
        &record,
        epoch_manifest,
        identity,
        view_id,
        key.as_str(),
    )?;
    Ok(Some(record))
}

fn validate_ingest_epoch_view_runtime_failure_record(
    record: &IngestEpochViewRuntimeFailureRecord,
    epoch_manifest: &PersistedIngestEpochManifest,
    identity: &StandingProgramIdentity,
    view_id: &str,
    key: &str,
) -> Result<(), ApiError> {
    if record.schema_version != 1
        || record.record_kind != "ingest_epoch_view_runtime_failure_v1"
        || record.epoch_manifest_id != epoch_manifest.epoch_manifest_id
        || record.tenant_id != identity.tenant_id
        || record.program_id != identity.program_id
        || record.view_id != view_id
    {
        return Err(ApiError::bad_request(format!(
            "ingest epoch view runtime failure body/key mismatch at {key}"
        )));
    }
    Ok(())
}

fn ingest_epoch_view_runtime_failure_error(
    epoch_manifest: &PersistedIngestEpochManifest,
    failure: &IngestEpochViewRuntimeFailureRecord,
) -> ApiError {
    ApiError::service_unavailable(format!(
        "standing runtime ingest epoch `{}` for view `{}` has a durable runtime failure marker and will not be retried automatically; rebuild or repair the external runtime before replaying this epoch: {}",
        epoch_manifest.epoch_manifest_id, failure.view_id, failure.failure_reason
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
        let Some(identity) = active
            .artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
        else {
            continue;
        };
        if !view_uses_ingest_relation(active, request) {
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
        let Some(identity) = active
            .artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
        else {
            continue;
        };
        if !view_uses_ingest_relation(&active, request) {
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
        let Some(identity) = active
            .artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
        else {
            continue;
        };
        if !view_uses_ingest_relation(&active, request) {
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
    apply_standing_runtime_ingests(state, std::slice::from_ref(request)).await
}

async fn apply_standing_runtime_ingests(
    state: &ApiState,
    requests: &[IngestRowsRequest],
) -> Result<(), ApiError> {
    state.validate_standing_runtime_fencing_or_evict().await?;
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    for active in active_views {
        let Some(identity) = active
            .artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
        else {
            continue;
        };
        if !requests
            .iter()
            .any(|request| view_uses_ingest_relation(&active, request))
        {
            continue;
        }
        let latest_checkpoint =
            read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id).await?;
        let replay_plan = latest_checkpoint
            .as_ref()
            .map(standing_runtime_replay_plan_from_record_ref)
            .unwrap_or_default();
        replay_committed_ingest_into_standing_runtime(state, &active, &replay_plan).await?;
    }

    Ok(())
}

async fn apply_standing_runtime_ingests_for_epoch_repair(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    prepared_batches: &[PreparedIngestBatch],
    requests: &[IngestRowsRequest],
) -> Result<(), ApiError> {
    state.validate_standing_runtime_fencing_or_evict().await?;
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    for active in active_views {
        let Some(identity) = active
            .artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
        else {
            continue;
        };
        if !requests
            .iter()
            .any(|request| view_uses_ingest_relation(&active, request))
        {
            continue;
        }
        if prepared_batches
            .iter()
            .any(|prepared| view_uses_prepared_ingest_batch(&active, prepared))
        {
            if read_ingest_epoch_view_convergence(
                state,
                epoch_manifest,
                identity,
                &active.spec.view_id,
            )
            .await?
            .is_some()
            {
                continue;
            }
            if let Some(failure) = read_ingest_epoch_view_runtime_failure(
                state,
                epoch_manifest,
                identity,
                &active.spec.view_id,
            )
            .await?
            {
                return Err(ingest_epoch_view_runtime_failure_error(
                    epoch_manifest,
                    &failure,
                ));
            }
        }
        let latest_checkpoint =
            read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id).await?;
        let replay_plan = latest_checkpoint
            .as_ref()
            .map(standing_runtime_replay_plan_from_record_ref)
            .unwrap_or_default();
        replay_committed_ingest_into_standing_runtime(state, &active, &replay_plan).await?;
    }

    Ok(())
}

async fn ensure_no_ingest_epoch_view_runtime_failures(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    prepared_batches: &[PreparedIngestBatch],
) -> Result<(), ApiError> {
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    for active in active_views {
        let Some(identity) = active
            .artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
        else {
            continue;
        };
        if !prepared_batches
            .iter()
            .any(|prepared| view_uses_prepared_ingest_batch(&active, prepared))
        {
            continue;
        }
        if read_ingest_epoch_view_convergence(state, epoch_manifest, identity, &active.spec.view_id)
            .await?
            .is_some()
        {
            continue;
        }
        if let Some(failure) = read_ingest_epoch_view_runtime_failure(
            state,
            epoch_manifest,
            identity,
            &active.spec.view_id,
        )
        .await?
        {
            return Err(ingest_epoch_view_runtime_failure_error(
                epoch_manifest,
                &failure,
            ));
        }
    }

    Ok(())
}

async fn apply_standing_runtime_ingest_epoch(
    state: &ApiState,
    epoch_manifest: &PersistedIngestEpochManifest,
    prepared_batches: &[PreparedIngestBatch],
) -> Result<(), ApiError> {
    state.validate_standing_runtime_fencing_or_evict().await?;
    let active_views = state
        .view_registry()?
        .list_active()
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    for active in active_views {
        let Some(identity) = active
            .artifact
            .as_ref()
            .and_then(|artifact| artifact.standing_program_identity.as_ref())
        else {
            continue;
        };
        let matching_prepared_batches = prepared_batches
            .iter()
            .filter(|prepared| view_uses_prepared_ingest_batch(&active, prepared))
            .collect::<Vec<_>>();
        let input_batches = matching_prepared_batches
            .iter()
            .copied()
            .map(relation_input_batch_from_prepared_ingest)
            .collect::<Vec<_>>();
        if input_batches.is_empty() {
            continue;
        }
        if read_ingest_epoch_view_convergence(state, epoch_manifest, identity, &active.spec.view_id)
            .await?
            .is_some()
        {
            continue;
        }
        if let Some(failure) = read_ingest_epoch_view_runtime_failure(
            state,
            epoch_manifest,
            identity,
            &active.spec.view_id,
        )
        .await?
        {
            return Err(ingest_epoch_view_runtime_failure_error(
                epoch_manifest,
                &failure,
            ));
        }
        let replay_checkpoints = matching_prepared_batches
            .iter()
            .copied()
            .map(|prepared| {
                ReplayCheckpoint::for_relation(
                    prepared.request.relation_id.clone(),
                    prepared.request.relation_version.clone(),
                    prepared.request.stream_id.clone(),
                    prepared.request.partition_id,
                    prepared.end_offset_exclusive,
                )
            })
            .collect::<Vec<_>>();
        let operation_lock =
            state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
        let _operation_guard = operation_lock.lock().await;
        if let Some(latest_checkpoint) =
            read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id).await?
        {
            let replay_plan = standing_runtime_replay_plan_from_record_ref(&latest_checkpoint);
            if prepared_batches_are_covered_by_replay_checkpoints(
                &replay_plan.replay_checkpoints,
                &matching_prepared_batches,
            ) {
                persist_ingest_epoch_view_convergence(
                    state,
                    epoch_manifest,
                    &active.spec.view_id,
                    &latest_checkpoint.checkpoint,
                    replay_checkpoints,
                )
                .await?;
                continue;
            }
        }
        let runtime = state
            .standing_runtime(identity, &active.spec.view_id)?
            .ok_or_else(|| {
                ApiError::service_unavailable(format!(
                    "standing runtime disappeared for active artifact-backed view `{}`",
                    active.spec.view_id
                ))
            })?;
        let owner = state
            .acquire_standing_runtime_owner(identity, &active.spec.view_id)
            .await?;
        let idempotency_key = epoch_ingest_idempotency_key(
            &active.spec.view_id,
            matching_prepared_batches.iter().copied(),
        )
        .map_err(ApiError::bad_request)?;
        let checkpoint = apply_standing_runtime_changes_and_checkpoint_many(
            Arc::clone(&runtime),
            0,
            idempotency_key,
            input_batches,
        )
        .await;
        let checkpoint = match checkpoint {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                persist_ingest_epoch_view_runtime_failure(
                    state,
                    epoch_manifest,
                    identity,
                    &active.spec.view_id,
                    error.message.clone(),
                    replay_checkpoints.clone(),
                )
                .await?;
                remove_standing_runtime(state, identity, &active.spec.view_id)?;
                return Err(error);
            }
        };
        if let Err(error) = persist_standing_runtime_checkpoint(
            state,
            &active.spec.view_id,
            &checkpoint,
            replay_checkpoints.clone(),
            owner,
        )
        .await
        {
            remove_standing_runtime(state, identity, &active.spec.view_id)?;
            return Err(error);
        }
        persist_ingest_epoch_view_convergence(
            state,
            epoch_manifest,
            &active.spec.view_id,
            &checkpoint,
            replay_checkpoints,
        )
        .await?;
    }

    Ok(())
}

fn view_uses_ingest_relation(active: &ActiveMaterializedView, request: &IngestRowsRequest) -> bool {
    active.spec.input_relations.iter().any(|input| {
        input.relation_id == request.relation_id
            && input.relation_version == request.relation_version
    })
}

fn view_uses_prepared_ingest_batch(
    active: &ActiveMaterializedView,
    prepared: &PreparedIngestBatch,
) -> bool {
    active.spec.input_relations.iter().any(|input| {
        input.relation_id == prepared.request.relation_id
            && input.relation_version == prepared.request.relation_version
            && input.schema_fingerprint == prepared.catalog.schema_fingerprint.as_str()
    })
}

fn relation_input_batch_from_prepared_ingest(prepared: &PreparedIngestBatch) -> RelationInputBatch {
    RelationInputBatch {
        relation_id: prepared.request.relation_id.clone(),
        relation_version: prepared.request.relation_version.clone(),
        schema_fingerprint: prepared.catalog.schema_fingerprint.as_str().to_string(),
        start_offset_inclusive: prepared.request.start_offset_inclusive,
        end_offset_exclusive: prepared.end_offset_exclusive,
        batches: vec![prepared.record_batch.clone()],
    }
}

fn epoch_ingest_idempotency_key<'a>(
    view_id: &str,
    prepared_batches: impl IntoIterator<Item = &'a PreparedIngestBatch>,
) -> Result<EpochIdempotencyKey, StandingProgramRuntimeError> {
    let mut parts = prepared_batches
        .into_iter()
        .map(|prepared| {
            format!(
                "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
                prepared.request.relation_id,
                prepared.request.relation_version,
                prepared.catalog.schema_fingerprint.as_str(),
                prepared.request.stream_id,
                prepared.request.partition_id,
                prepared.request.start_offset_inclusive,
                prepared.end_offset_exclusive,
                prepared.payload_digest
            )
        })
        .collect::<Vec<_>>();
    parts.sort();
    let mut hasher = Sha256::new();
    hasher.update(b"velorix.ingest-epoch.runtime-idempotency.v1\0");
    hasher.update(view_id.as_bytes());
    for part in parts {
        hasher.update(b"\0");
        hasher.update(part.as_bytes());
    }
    EpochIdempotencyKey::new(format!("epoch:sha256:{:x}", hasher.finalize()))
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

async fn apply_standing_runtime_changes_and_checkpoint(
    runtime: SharedStandingRuntime,
    lower_bound_epoch: u64,
    idempotency_key: EpochIdempotencyKey,
    input_batch: RelationInputBatch,
) -> Result<RuntimeCheckpoint, ApiError> {
    apply_standing_runtime_changes_and_checkpoint_many(
        runtime,
        lower_bound_epoch,
        idempotency_key,
        vec![input_batch],
    )
    .await
}

async fn apply_standing_runtime_changes_and_checkpoint_many(
    runtime: SharedStandingRuntime,
    lower_bound_epoch: u64,
    idempotency_key: EpochIdempotencyKey,
    input_batches: Vec<RelationInputBatch>,
) -> Result<RuntimeCheckpoint, ApiError> {
    tokio::task::spawn_blocking(move || {
        let mut runtime = runtime
            .lock()
            .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?;
        let logical_epoch =
            next_standing_runtime_logical_epoch(runtime.as_ref(), lower_bound_epoch)?;
        runtime
            .apply_changes(logical_epoch, idempotency_key, input_batches)
            .map_err(ApiError::bad_request)?;
        runtime.checkpoint().map_err(ApiError::bad_request)
    })
    .await
    .map_err(ApiError::internal)?
}

async fn persist_standing_runtime_checkpoint(
    state: &ApiState,
    view_id: &str,
    checkpoint: &RuntimeCheckpoint,
    replay_checkpoints_to_merge: Vec<ReplayCheckpoint>,
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
    let replay_checkpoints = merged_standing_runtime_replay_checkpoints(
        previous_record.as_ref(),
        replay_checkpoints_to_merge,
    );
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

fn standing_runtime_replay_plan_from_record(
    record: StandingRuntimeCheckpointRecord,
) -> StandingRuntimeReplayPlan {
    StandingRuntimeReplayPlan {
        replay_checkpoints: record.replay_checkpoints,
    }
}

fn standing_runtime_replay_plan_from_record_ref(
    record: &StandingRuntimeCheckpointRecord,
) -> StandingRuntimeReplayPlan {
    StandingRuntimeReplayPlan {
        replay_checkpoints: record.replay_checkpoints.clone(),
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

    let legacy_checkpoint_input_frontier = record
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
    let mut input_frontiers_by_relation = BTreeMap::new();
    for frontier in &record.checkpoint.input_frontiers {
        let key = (
            frontier.relation_id.as_str(),
            frontier.relation_version.as_str(),
        );
        if input_frontiers_by_relation
            .insert(key, frontier.committed_offset_exclusive)
            .is_some()
        {
            return Err(ApiError::bad_request(format!(
                "duplicate standing runtime checkpoint input frontier for view `{}` relation={} version={}",
                record.view_id, frontier.relation_id, frontier.relation_version
            )));
        }
    }
    let mut seen = BTreeSet::new();
    for replay in &record.replay_checkpoints {
        if !seen.insert((
            replay.stream_id.as_str(),
            replay.partition_id,
            replay.relation_id.as_deref(),
            replay.relation_version.as_deref(),
        )) {
            return Err(ApiError::bad_request(format!(
                "duplicate standing runtime checkpoint replay frontier for view `{}` stream={} partition={}",
                record.view_id, replay.stream_id, replay.partition_id
            )));
        }
        match (
            replay.relation_id.as_deref(),
            replay.relation_version.as_deref(),
        ) {
            (Some(relation_id), Some(relation_version)) => {
                let Some(checkpoint_input_frontier) =
                    input_frontiers_by_relation.get(&(relation_id, relation_version))
                else {
                    return Err(ApiError::bad_request(format!(
                        "standing runtime checkpoint replay frontier has no matching input frontier for view `{}` relation={} version={} stream={} partition={}",
                        record.view_id,
                        relation_id,
                        relation_version,
                        replay.stream_id,
                        replay.partition_id
                    )));
                };
                if replay.end_offset_exclusive > *checkpoint_input_frontier {
                    return Err(ApiError::bad_request(format!(
                        "standing runtime checkpoint replay frontier is ahead of checkpoint input frontier for view `{}` relation={} version={} stream={} partition={} replay_end={} checkpoint_end={}",
                        record.view_id,
                        relation_id,
                        relation_version,
                        replay.stream_id,
                        replay.partition_id,
                        replay.end_offset_exclusive,
                        checkpoint_input_frontier
                    )));
                }
            }
            (None, None) if record.checkpoint.input_frontiers.len() == 1 => {
                if replay.end_offset_exclusive > legacy_checkpoint_input_frontier {
                    return Err(ApiError::bad_request(format!(
                        "standing runtime checkpoint replay frontier is ahead of checkpoint input frontier for view `{}` stream={} partition={} replay_end={} checkpoint_end={}",
                        record.view_id,
                        replay.stream_id,
                        replay.partition_id,
                        replay.end_offset_exclusive,
                        legacy_checkpoint_input_frontier
                    )));
                }
            }
            (None, None) => {
                return Err(ApiError::bad_request(format!(
                    "standing runtime checkpoint replay frontier lacks relation metadata for multi-relation view `{}` stream={} partition={}",
                    record.view_id, replay.stream_id, replay.partition_id
                )));
            }
            _ => {
                return Err(ApiError::bad_request(format!(
                    "standing runtime checkpoint replay frontier has partial relation metadata for view `{}` stream={} partition={}",
                    record.view_id, replay.stream_id, replay.partition_id
                )));
            }
        }
    }

    Ok(())
}

fn merged_standing_runtime_replay_checkpoints(
    previous_record: Option<&StandingRuntimeCheckpointRecord>,
    replay_checkpoints_to_merge: Vec<ReplayCheckpoint>,
) -> Vec<ReplayCheckpoint> {
    let mut replay_checkpoints = previous_record
        .map(|record| record.replay_checkpoints.clone())
        .unwrap_or_default();
    for replay_checkpoint in replay_checkpoints_to_merge {
        if let Some(existing) = replay_checkpoints.iter_mut().find(|existing| {
            existing.stream_id == replay_checkpoint.stream_id
                && existing.partition_id == replay_checkpoint.partition_id
                && existing.relation_id == replay_checkpoint.relation_id
                && existing.relation_version == replay_checkpoint.relation_version
        }) {
            existing.end_offset_exclusive = existing
                .end_offset_exclusive
                .max(replay_checkpoint.end_offset_exclusive);
        } else {
            replay_checkpoints.push(replay_checkpoint);
        }
    }
    replay_checkpoints.sort_by(|left, right| {
        left.stream_id
            .cmp(&right.stream_id)
            .then(left.partition_id.cmp(&right.partition_id))
            .then(left.relation_id.cmp(&right.relation_id))
            .then(left.relation_version.cmp(&right.relation_version))
    });

    replay_checkpoints
}

fn replay_checkpoints_cover_replayed_batch(
    replay_checkpoints: &[ReplayCheckpoint],
    relation_id: &str,
    relation_version: &str,
    stream_id: &str,
    partition_id: u32,
    batch_end_offset_exclusive: u64,
) -> bool {
    replay_checkpoints.iter().any(|checkpoint| {
        checkpoint.relation_id.as_deref() == Some(relation_id)
            && checkpoint.relation_version.as_deref() == Some(relation_version)
            && checkpoint.stream_id == stream_id
            && checkpoint.partition_id == partition_id
            && checkpoint.end_offset_exclusive >= batch_end_offset_exclusive
    })
}

fn prepared_batches_are_covered_by_replay_checkpoints(
    replay_checkpoints: &[ReplayCheckpoint],
    prepared_batches: &[&PreparedIngestBatch],
) -> bool {
    prepared_batches.iter().all(|prepared| {
        replay_checkpoints_cover_replayed_batch(
            replay_checkpoints,
            prepared.request.relation_id.as_str(),
            prepared.request.relation_version.as_str(),
            prepared.request.stream_id.as_str(),
            prepared.request.partition_id,
            prepared.end_offset_exclusive,
        )
    })
}

async fn replay_committed_ingest_into_standing_runtime(
    state: &ApiState,
    active: &ActiveMaterializedView,
    replay_plan: &StandingRuntimeReplayPlan,
) -> Result<(), ApiError> {
    if active.spec.input_relations.is_empty() {
        return Ok(());
    }
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
        .replay_admitted_validated_envelopes_from(&replay_plan.replay_checkpoints)
        .await
        .map_err(ApiError::internal)?;

    for batch in batches {
        let descriptor = batch.descriptor();
        let envelope =
            IngestEnvelope::decode(batch.payload().clone()).map_err(ApiError::bad_request)?;
        let header = envelope.header();
        if !active.spec.input_relations.iter().any(|input| {
            header.relation_id == input.relation_id
                && header.relation_version == input.relation_version
                && header.schema_fingerprint == input.schema_fingerprint
        }) {
            continue;
        }
        if replay_checkpoints_cover_replayed_batch(
            &replay_plan.replay_checkpoints,
            header.relation_id.as_str(),
            header.relation_version.as_str(),
            descriptor.stream_id.as_str(),
            descriptor.partition_id,
            descriptor.end_offset_exclusive,
        ) {
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
        let checkpoint = apply_standing_runtime_changes_and_checkpoint(
            Arc::clone(&runtime),
            descriptor.end_offset_exclusive,
            idempotency_key,
            input_batch,
        )
        .await?;
        if let Err(error) = persist_standing_runtime_checkpoint(
            state,
            &active.spec.view_id,
            &checkpoint,
            vec![ReplayCheckpoint::for_relation(
                header.relation_id.clone(),
                header.relation_version.clone(),
                descriptor.stream_id.clone(),
                descriptor.partition_id,
                descriptor.end_offset_exclusive,
            )],
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

async fn read_relation_catalogs_for_view_request(
    state: &ApiState,
    request: &CreateViewRequest,
) -> Result<Vec<VelorixRelationCatalogV1>, ApiError> {
    let has_single_ref = !request.input_relation_id.trim().is_empty()
        || !request.input_relation_version.trim().is_empty();
    if !request.input_relation_refs.is_empty() {
        if !request.input_relations.is_empty() || has_single_ref {
            return Err(ApiError::bad_request(
                "view must use only one input relation selector: input_relation_id/input_relation_version, input_relation_refs, or input_relations",
            ));
        }
        let mut catalogs = Vec::with_capacity(request.input_relation_refs.len());
        let mut seen = BTreeSet::new();
        for input in &request.input_relation_refs {
            if input.relation_id.trim().is_empty() || input.relation_version.trim().is_empty() {
                return Err(ApiError::bad_request(
                    "input_relation_refs must include non-empty relation_id and relation_version",
                ));
            }
            if !seen.insert((input.relation_id.as_str(), input.relation_version.as_str())) {
                return Err(ApiError::bad_request(format!(
                    "duplicate input_relation_refs entry for relation `{}` version `{}`",
                    input.relation_id, input.relation_version
                )));
            }
            catalogs.push(
                read_relation_catalog(state, &input.relation_id, &input.relation_version).await?,
            );
        }
        return Ok(catalogs);
    }
    if !request.input_relations.is_empty() {
        if has_single_ref {
            return Err(ApiError::bad_request(
                "view must use only one input relation selector: input_relation_id/input_relation_version, input_relation_refs, or input_relations",
            ));
        }
        return read_relation_catalogs_for_input_schemas(state, &request.input_relations).await;
    }
    if request.input_relation_id.trim().is_empty()
        || request.input_relation_version.trim().is_empty()
    {
        return Err(ApiError::bad_request(
            "view requires either input_relation_id/input_relation_version or input_relations",
        ));
    }
    read_relation_catalog(
        state,
        &request.input_relation_id,
        &request.input_relation_version,
    )
    .await
    .map(|catalog| vec![catalog])
}

async fn read_relation_catalogs_for_spec(
    state: &ApiState,
    spec: &StandingViewSpec,
) -> Result<Vec<VelorixRelationCatalogV1>, ApiError> {
    read_relation_catalogs_for_input_schemas(state, &spec.input_relations).await
}

async fn read_relation_catalogs_for_input_schemas(
    state: &ApiState,
    schemas: &[RelationSchema],
) -> Result<Vec<VelorixRelationCatalogV1>, ApiError> {
    if schemas.is_empty() {
        return Err(ApiError::bad_request("view has no input relation"));
    }
    let mut catalogs = Vec::with_capacity(schemas.len());
    for schema in schemas {
        let catalog =
            read_relation_catalog(state, &schema.relation_id, &schema.relation_version).await?;
        let expected = catalog_input_relation_schema(&catalog).map_err(ApiError::bad_request)?;
        if &expected != schema {
            return Err(ApiError::bad_request(format!(
                "input relation schema does not match registered relation `{}` version `{}`",
                schema.relation_id, schema.relation_version
            )));
        }
        catalogs.push(catalog);
    }
    Ok(catalogs)
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

async fn query_view_output_rows_get(
    State(state): State<ApiState>,
    AxumPath((view_id, output_id)): AxumPath<(String, String)>,
    Query(mut query): Query<BTreeMap<String, String>>,
) -> Result<Json<QueryResponse>, ApiError> {
    let page_request = extract_snapshot_page_request(&mut query)?;
    let parameters = query
        .into_iter()
        .map(|(name, value)| (name, Value::String(value)))
        .collect();
    query_view_output_rows_impl(state, view_id, output_id, None, parameters, page_request).await
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
    ensure_view_execution_allowed(&active)?;
    let api = active.api.clone().unwrap_or_default();
    for (name, raw_value) in query {
        let value = request_query_value_for_api_field(&api, &name, raw_value.as_str())?;
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
    query_active_view_output_rows_impl(
        state,
        active,
        api.output_relation_id.clone(),
        None,
        parameters,
        page_request,
        true,
    )
    .await
}

fn request_query_value_for_api_field(
    api: &MaterializedViewApiMetadata,
    name: &str,
    raw_value: &str,
) -> Result<Value, ApiError> {
    let Some(field) = api
        .request
        .iter()
        .find(|field| field.field_name == name && field.field_in == "query")
    else {
        return Ok(Value::String(raw_value.to_string()));
    };
    if field.r#type != "array" {
        return Ok(Value::String(raw_value.to_string()));
    }
    let value = serde_json::from_str::<Value>(raw_value).map_err(|error| {
        ApiError::bad_request(format!(
            "query parameter `{name}` with type `array` must be a JSON array: {error}"
        ))
    })?;
    if !value.is_array() {
        return Err(ApiError::bad_request(format!(
            "query parameter `{name}` with type `array` must be a JSON array"
        )));
    }
    Ok(value)
}

async fn query_view_rows_post(
    State(state): State<ApiState>,
    AxumPath(view_id): AxumPath<String>,
    Json(request): Json<QueryViewRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let active = state
        .view_registry()?
        .read_active(&view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    ensure_view_execution_allowed(&active)?;
    if request.sql.is_none() {
        validate_direct_view_query_parameter_sources(&active, &request.parameters)?;
    }
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

async fn query_view_output_rows_post(
    State(state): State<ApiState>,
    AxumPath((view_id, output_id)): AxumPath<(String, String)>,
    Json(request): Json<QueryViewRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let active = state
        .view_registry()?
        .read_active(&view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    ensure_view_execution_allowed(&active)?;
    query_active_view_output_rows_impl(
        state,
        active,
        Some(output_id),
        request.sql,
        request.parameters,
        SnapshotPageRequest {
            committed_epoch: request.epoch,
            page_token: request.page_token,
            max_rows: request.max_rows,
        },
        false,
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

async fn query_view_output_rows_impl(
    state: ApiState,
    view_id: String,
    output_id: String,
    request_sql: Option<String>,
    parameters: BTreeMap<String, Value>,
    page_request: SnapshotPageRequest,
) -> Result<Json<QueryResponse>, ApiError> {
    let active = state
        .view_registry()?
        .read_active(&view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    query_active_view_output_rows_impl(
        state,
        active,
        Some(output_id),
        request_sql,
        parameters,
        page_request,
        false,
    )
    .await
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
    query_active_view_output_rows_impl(
        state,
        active,
        None,
        request_sql,
        parameters,
        page_request,
        true,
    )
    .await
}

async fn query_active_view_output_rows_impl(
    state: ApiState,
    active: ActiveMaterializedView,
    requested_output_id: Option<String>,
    request_sql: Option<String>,
    parameters: BTreeMap<String, Value>,
    page_request: SnapshotPageRequest,
    use_view_api_metadata: bool,
) -> Result<Json<QueryResponse>, ApiError> {
    ensure_view_execution_allowed(&active)?;
    let view_id = active.spec.view_id.clone();
    let output_id = resolve_view_query_output_id(&active, requested_output_id.as_deref())?;
    let active_api = active.api.clone().unwrap_or_default();
    let raw_sql_query = request_sql.is_some();
    let use_view_api_metadata = use_view_api_metadata && !raw_sql_query;
    let api = if use_view_api_metadata {
        active_api.clone()
    } else {
        MaterializedViewApiMetadata::default()
    };
    let parameters = if raw_sql_query {
        parameters
    } else {
        resolve_request_parameters(&api.request, &parameters)?
    };
    let query_policy = query_policy_for_view_api(&state, &active_api).await?;

    match active.execution_mode {
        MaterializedViewExecutionMode::StandingRuntime => {
            let is_feldera_runtime = is_feldera_pipeline_manager_runtime(&active);
            validate_standing_runtime_query_contract(
                &active.spec.view_id,
                request_sql.as_ref(),
                &api,
                &parameters,
                &page_request,
                is_feldera_runtime,
            )?;
            let (rows, logical_epoch, next_page_token) = if let Some(sql) = request_sql {
                let requested_epoch = page_request.committed_epoch;
                let sql = render_caller_sql_as_feldera_sql(&sql, &parameters)?;
                let page_request =
                    page_request_with_query_policy_limit(page_request, query_policy.policy);
                let page =
                    standing_runtime_sql_page(&state, &active, &output_id, sql, page_request)
                        .await?;
                validate_standing_runtime_sql_page(&active, &output_id, &page, requested_epoch)?;
                (page.rows, page.logical_epoch, page.next_page_token)
            } else if api.sql_template.is_some() {
                query_standing_runtime_rows_with_template(
                    &state,
                    &active,
                    &output_id,
                    &api,
                    &parameters,
                    page_request,
                    query_policy,
                )
                .await?
            } else {
                query_standing_runtime_rows(&state, &active, &output_id, page_request, query_policy)
                    .await?
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

fn resolve_view_query_output_id(
    active: &ActiveMaterializedView,
    requested_output_id: Option<&str>,
) -> Result<String, ApiError> {
    if let Some(output_id) = requested_output_id {
        if active
            .spec
            .output_relations
            .iter()
            .any(|schema| schema.relation_id == output_id)
        {
            return Ok(output_id.to_string());
        }
        return Err(ApiError::bad_request(format!(
            "view `{}` has no output relation `{output_id}`",
            active.spec.view_id
        )));
    }
    if active.spec.output_relations.len() == 1 {
        return Ok(active.spec.output_relations[0].relation_id.clone());
    }
    if active
        .spec
        .output_relations
        .iter()
        .any(|schema| schema.relation_id == active.spec.view_id)
    {
        return Ok(active.spec.view_id.clone());
    }
    Err(ApiError::bad_request(format!(
        "view `{}` has multiple output relations; query one explicitly with `/v1/views/{}/outputs/{{output_id}}/query`",
        active.spec.view_id, active.spec.view_id
    )))
}

fn ensure_view_execution_allowed(active: &ActiveMaterializedView) -> Result<(), ApiError> {
    if active.execution_mode == MaterializedViewExecutionMode::FelderaCompilePending {
        return Err(ApiError::service_unavailable(format!(
            "feldera_compile_pending: view `{}` is accepted but not deployed yet",
            active.spec.view_id
        )));
    }
    if active.lifecycle.compile_status != MaterializedViewCompileStatus::Success
        || active.lifecycle.deployment_status != MaterializedViewDeploymentStatus::Running
    {
        return Err(ApiError::service_unavailable(format!(
            "standing_runtime_not_deployed: view `{}` is not running yet",
            active.spec.view_id
        )));
    }
    Ok(())
}

async fn query_standing_runtime_rows_with_template(
    state: &ApiState,
    active: &ActiveMaterializedView,
    output_id: &str,
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
    if is_feldera_pipeline_manager_runtime(active) {
        let feldera_sql =
            render_view_sql_template_as_feldera_sql(sql_template, &api.request, parameters)?;
        let page_request = page_request_with_query_policy_limit(page_request, query_policy.policy);
        let page =
            standing_runtime_sql_page(state, active, output_id, feldera_sql, page_request).await?;
        validate_standing_runtime_sql_page(active, output_id, &page, requested_epoch)?;
        return Ok((page.rows, page.logical_epoch, page.next_page_token));
    }
    let bound_sql = render_view_sql_template(
        &normalize_view_query_sql(sql_template, output_id),
        &api.request,
        parameters,
    )?;
    let page = standing_runtime_page(state, active, output_id, page_request).await?;
    validate_standing_runtime_template_page(active, output_id, &page, requested_epoch)?;
    let batches = query_record_batches_table_with_bindings_and_policy_and_limiter(
        output_id,
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

fn is_feldera_pipeline_manager_runtime(active: &ActiveMaterializedView) -> bool {
    active
        .artifact
        .as_ref()
        .is_some_and(|artifact| artifact.execution_path == "feldera_pipeline_manager")
}

fn validate_standing_runtime_sql_page(
    active: &ActiveMaterializedView,
    output_id: &str,
    page: &MaterializedViewSqlPage,
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
        view_id: output_id.to_string(),
    };
    if page.view != expected_view {
        return Err(ApiError::conflict(format!(
            "standing runtime view `{}` output `{output_id}` returned SQL rows for a different scoped view",
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
    Ok(())
}

fn validate_standing_runtime_template_page(
    active: &ActiveMaterializedView,
    output_id: &str,
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
        view_id: output_id.to_string(),
    };
    if page.view != expected_view {
        return Err(ApiError::conflict(format!(
            "standing runtime view `{}` output `{output_id}` returned a page for a different scoped view",
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
        .find(|schema| schema.relation_id == output_id)
        .ok_or_else(|| {
            ApiError::conflict(format!(
                "standing runtime view `{}` has no matching output schema for `{output_id}`",
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
    output_id: &str,
    page_request: SnapshotPageRequest,
    query_policy: ViewQueryPolicy,
) -> Result<(Vec<Value>, u64, Option<String>), ApiError> {
    let page_request = page_request_with_query_policy_limit(page_request, query_policy.policy);
    let page = standing_runtime_page(state, active, output_id, page_request).await?;
    let sql = format!("SELECT * FROM {}", feldera_sql_quoted_identifier(output_id));
    let batches = query_record_batches_table_with_bindings_and_policy_and_limiter(
        output_id,
        page.batches,
        &sql,
        &[],
        query_policy.policy,
        query_policy.limiter,
    )
    .await
    .map_err(ApiError::bad_request)?;
    let output_schema = active
        .spec
        .output_relations
        .iter()
        .find(|schema| schema.relation_id == output_id)
        .ok_or_else(|| {
            ApiError::conflict(format!(
                "standing runtime view `{}` has no matching output schema for `{output_id}`",
                active.spec.view_id
            ))
        })?;

    Ok((
        record_batches_to_json_rows_for_feldera_schema(output_schema, &batches)?,
        page.logical_epoch,
        page.next_page_token,
    ))
}

fn page_request_with_query_policy_limit(
    mut page_request: SnapshotPageRequest,
    policy: QueryPolicy,
) -> SnapshotPageRequest {
    let Some(policy_fetch_rows) = policy
        .max_output_rows
        .and_then(|max_rows| max_rows.checked_add(1))
    else {
        return page_request;
    };
    page_request.max_rows = Some(match page_request.max_rows {
        Some(requested_rows) => requested_rows.min(policy_fetch_rows),
        None => policy_fetch_rows,
    });
    page_request
}

async fn standing_runtime_page(
    state: &ApiState,
    active: &ActiveMaterializedView,
    output_id: &str,
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
        let _ = ensure_standing_runtime_for_artifact(state, &active.spec, artifact).await?;
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
    let scoped_view = ScopedViewId {
        tenant_id: identity.tenant_id.clone(),
        program_id: identity.program_id.clone(),
        view_id: output_id.to_string(),
    };
    let page = tokio::task::spawn_blocking(move || {
        runtime
            .lock()
            .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?
            .materialized_view_page(scoped_view, page_request)
            .map_err(ApiError::bad_request)
    })
    .await
    .map_err(ApiError::internal)??;
    state
        .validate_standing_runtime_committed_for_query(
            identity,
            &active.spec.view_id,
            page.logical_epoch,
        )
        .await?;

    Ok(page)
}

async fn standing_runtime_sql_page(
    state: &ApiState,
    active: &ActiveMaterializedView,
    output_id: &str,
    sql: String,
    page_request: SnapshotPageRequest,
) -> Result<MaterializedViewSqlPage, ApiError> {
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
        let _ = ensure_standing_runtime_for_artifact(state, &active.spec, artifact).await?;
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
    let scoped_view = ScopedViewId {
        tenant_id: identity.tenant_id.clone(),
        program_id: identity.program_id.clone(),
        view_id: output_id.to_string(),
    };
    let page = tokio::task::spawn_blocking(move || {
        runtime
            .lock()
            .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?
            .materialized_view_sql_page(scoped_view, sql, page_request)
            .map_err(ApiError::bad_request)
    })
    .await
    .map_err(ApiError::internal)??;
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
        "/v1/ingest/epoch".to_string(),
        json!({
            "post": {
                "summary": "Ingest one logical epoch across one or more relations",
                "responses": { "201": { "description": "Epoch batches ingested" } }
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
        let response = active_view_response(&view, None)?;
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
                    "x-velorix-output-relation-id": response.output_relation_id,
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
        for output in &response.output_relations {
            paths.insert(
                format!(
                    "/v1/views/{}/outputs/{}/query",
                    response.view_id, output.relation_id
                ),
                json!({
                    "get": {
                        "summary": format!("Query {} output {}", response.view_id, output.relation_id),
                        "x-velorix-view-id": response.view_id,
                        "x-velorix-output-relation-id": output.relation_id,
                        "x-velorix-output-schema-fingerprint": output.schema_fingerprint,
                        "parameters": openapi_view_query_parameters(
                            &response.request,
                            response.sql_template.is_some(),
                        ),
                        "responses": { "200": { "description": "Rows" } }
                    },
                    "post": {
                        "summary": format!("Query {} output {}", response.view_id, output.relation_id),
                        "x-velorix-view-id": response.view_id,
                        "x-velorix-output-relation-id": output.relation_id,
                        "x-velorix-output-schema-fingerprint": output.schema_fingerprint,
                        "responses": { "200": { "description": "Rows" } }
                    }
                }),
            );
        }
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
    sql_table_references(sql)
        .iter()
        .any(|reference| reference.eq_ignore_ascii_case(table_name))
}

fn sql_table_references(sql: &str) -> Vec<String> {
    let mut references = Vec::new();
    let mut rest = sql;
    while let Some((_keyword, after_keyword)) = next_from_or_join(rest) {
        if let Some((identifier, consumed)) = parse_sql_identifier(after_keyword) {
            references.push(identifier);
            rest = &after_keyword[consumed..];
        } else {
            rest = after_keyword;
        }
    }
    references
}

fn next_from_or_join(sql: &str) -> Option<(&str, &str)> {
    let mut index = 0;
    while index < sql.len() {
        let remaining = &sql[index..];
        let Some((token, start, end)) = next_unquoted_sql_word(remaining) else {
            return None;
        };
        let absolute_start = index + start;
        let absolute_end = index + end;
        if matches!(token.to_ascii_lowercase().as_str(), "from" | "join") {
            return Some((&sql[absolute_start..absolute_end], &sql[absolute_end..]));
        }
        index = absolute_end;
    }
    None
}

fn next_unquoted_sql_word(sql: &str) -> Option<(&str, usize, usize)> {
    let bytes = sql.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' => {
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'\'' {
                        if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                            index += 2;
                        } else {
                            index += 1;
                            break;
                        }
                    } else {
                        index += 1;
                    }
                }
            }
            b'"' => {
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == b'"' {
                        if index + 1 < bytes.len() && bytes[index + 1] == b'"' {
                            index += 2;
                        } else {
                            index += 1;
                            break;
                        }
                    } else {
                        index += 1;
                    }
                }
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                return Some((&sql[start..index], start, index));
            }
            _ => index += 1,
        }
    }
    None
}

fn parse_sql_identifier(sql: &str) -> Option<(String, usize)> {
    let trimmed_start = sql.len() - sql.trim_start().len();
    let sql = &sql[trimmed_start..];
    let bytes = sql.as_bytes();
    if bytes.first() == Some(&b'"') {
        let mut identifier = String::new();
        let mut index = 1;
        while index < bytes.len() {
            if bytes[index] == b'"' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'"' {
                    identifier.push('"');
                    index += 2;
                } else {
                    return Some((identifier, trimmed_start + index + 1));
                }
            } else {
                let character = sql[index..].chars().next()?;
                identifier.push(character);
                index += character.len_utf8();
            }
        }
        return None;
    }
    let mut index = 0;
    while index < bytes.len()
        && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_' || bytes[index] == b'.')
    {
        index += 1;
    }
    (index > 0).then(|| (sql[..index].to_string(), trimmed_start + index))
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
        SqlDataType::Int8 => Ok(DataType::Int8),
        SqlDataType::Int16 => Ok(DataType::Int16),
        SqlDataType::Int32 => Ok(DataType::Int32),
        SqlDataType::Int64 => Ok(DataType::Int64),
        SqlDataType::UInt8 => Ok(DataType::UInt8),
        SqlDataType::UInt16 => Ok(DataType::UInt16),
        SqlDataType::UInt32 => Ok(DataType::UInt32),
        SqlDataType::UInt64 => Ok(DataType::UInt64),
        SqlDataType::Float32 => Ok(DataType::Float32),
        SqlDataType::Float64 => Ok(DataType::Float64),
        SqlDataType::Decimal { precision, scale } => Ok(DataType::Decimal128(
            *precision,
            (*scale).try_into().map_err(|_| {
                ApiError::bad_request("decimal scale does not fit Arrow Decimal128")
            })?,
        )),
        SqlDataType::Char { .. }
        | SqlDataType::Utf8
        | SqlDataType::Json
        | SqlDataType::Uuid
        | SqlDataType::Geometry => Ok(DataType::Utf8),
        SqlDataType::Binary { .. } | SqlDataType::Varbinary => Ok(DataType::Binary),
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Time => Ok(DataType::Time64(TimeUnit::Nanosecond)),
        SqlDataType::Timestamp { .. } => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            None,
        )),
        SqlDataType::Interval { .. } => Ok(DataType::Utf8),
        SqlDataType::Array { element_type } => Ok(DataType::List(Arc::new(Field::new(
            "item",
            arrow_data_type_from_sql_data_type(element_type)?,
            true,
        )))),
        SqlDataType::Struct { fields } => Ok(DataType::Struct(Fields::from(
            fields
                .iter()
                .map(|field| {
                    Ok(Field::new(
                        field.name.as_str(),
                        arrow_data_type_from_sql_data_type(&field.data_type)?,
                        field.nullable,
                    ))
                })
                .collect::<Result<Vec<_>, ApiError>>()?,
        ))),
        SqlDataType::Map {
            key_type,
            value_type,
        } => Ok(DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("keys", arrow_data_type_from_sql_data_type(key_type)?, false),
                    Field::new(
                        "values",
                        arrow_data_type_from_sql_data_type(value_type)?,
                        true,
                    ),
                ])),
                false,
            )),
            false,
        )),
        SqlDataType::Null => Ok(DataType::Null),
    }
}

fn feldera_query_rows_from_text(
    text: &str,
    output_column_names: Option<&BTreeSet<String>>,
) -> Result<Vec<Value>, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return feldera_query_rows_from_value(value, output_column_names);
    }
    trimmed
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str::<Value>(line)
                .map_err(|error| format!("invalid Feldera JSON query row: {error}"))
                .and_then(|value| feldera_query_row_from_value(value, output_column_names))
        })
        .collect()
}

fn feldera_query_rows_from_value(
    value: Value,
    output_column_names: Option<&BTreeSet<String>>,
) -> Result<Vec<Value>, String> {
    match value {
        Value::Array(rows) => rows
            .into_iter()
            .map(|row| feldera_query_row_from_value(row, output_column_names))
            .collect::<Result<Vec<_>, _>>(),
        Value::Object(object) => Ok(vec![feldera_query_row_from_value(
            Value::Object(object),
            output_column_names,
        )?]),
        other => Err(format!(
            "Feldera JSON query result must be an object or array, got {other}"
        )),
    }
}

fn feldera_query_row_from_value(
    value: Value,
    output_column_names: Option<&BTreeSet<String>>,
) -> Result<Value, String> {
    match value {
        Value::Object(mut object) => {
            if object.len() == 1
                && object.contains_key("insert")
                && !output_column_names.is_some_and(|columns| columns.contains("insert"))
            {
                let insert = object.remove("insert").expect("insert key checked above");
                if insert.is_object() {
                    return feldera_query_row_from_value(insert, output_column_names);
                }
                return Ok(json!({ "insert": insert }));
            }
            if object.len() == 1
                && object.contains_key("delete")
                && !output_column_names.is_some_and(|columns| columns.contains("delete"))
            {
                let delete = object.remove("delete").expect("delete key checked above");
                if delete.is_object() {
                    return Err("Feldera query snapshot returned delete event".to_string());
                }
                return Ok(json!({ "delete": delete }));
            }
            Ok(Value::Object(object))
        }
        other => Err(format!("Feldera query row must be an object, got {other}")),
    }
}

fn feldera_rows_to_record_batch(
    schema: &RelationSchema,
    rows: &[Value],
) -> Result<RecordBatch, String> {
    let arrow_schema =
        arrow_schema_from_feldera_relation_schema(schema).map_err(|error| error.to_string())?;
    let arrays = schema
        .columns
        .iter()
        .map(|column| feldera_json_column_to_arrow_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(arrow_schema, arrays).map_err(|error| error.to_string())
}

fn feldera_json_column_to_arrow_array(
    column: &ColumnSchema,
    rows: &[Value],
) -> Result<ArrayRef, String> {
    match &column.data_type {
        SqlDataType::Bool => Ok(Arc::new(BooleanArray::from(collect_feldera_column_values(
            column,
            rows,
            |value| {
                value
                    .as_bool()
                    .ok_or_else(|| format!("column `{}` must be boolean", column.name))
            },
        )?))),
        SqlDataType::Int8 => Ok(Arc::new(Int8Array::from(collect_feldera_column_values(
            column,
            rows,
            |value| {
                let value = value
                    .as_i64()
                    .ok_or_else(|| format!("column `{}` must be int8", column.name))?;
                i8::try_from(value).map_err(|_| format!("column `{}` must fit int8", column.name))
            },
        )?))),
        SqlDataType::Int16 => Ok(Arc::new(Int16Array::from(collect_feldera_column_values(
            column,
            rows,
            |value| {
                let value = value
                    .as_i64()
                    .ok_or_else(|| format!("column `{}` must be int16", column.name))?;
                i16::try_from(value).map_err(|_| format!("column `{}` must fit int16", column.name))
            },
        )?))),
        SqlDataType::Int32 => Ok(Arc::new(Int32Array::from(collect_feldera_column_values(
            column,
            rows,
            |value| {
                let value = value
                    .as_i64()
                    .ok_or_else(|| format!("column `{}` must be int32", column.name))?;
                i32::try_from(value).map_err(|_| format!("column `{}` must fit int32", column.name))
            },
        )?))),
        SqlDataType::Int64 => Ok(Arc::new(Int64Array::from(collect_feldera_column_values(
            column,
            rows,
            |value| {
                value
                    .as_i64()
                    .ok_or_else(|| format!("column `{}` must be int64", column.name))
            },
        )?))),
        SqlDataType::UInt8 => Ok(Arc::new(UInt8Array::from(collect_feldera_column_values(
            column,
            rows,
            |value| {
                let value = value
                    .as_u64()
                    .ok_or_else(|| format!("column `{}` must be uint8", column.name))?;
                u8::try_from(value).map_err(|_| format!("column `{}` must fit uint8", column.name))
            },
        )?))),
        SqlDataType::UInt16 => Ok(Arc::new(UInt16Array::from(collect_feldera_column_values(
            column,
            rows,
            |value| {
                let value = value
                    .as_u64()
                    .ok_or_else(|| format!("column `{}` must be uint16", column.name))?;
                u16::try_from(value)
                    .map_err(|_| format!("column `{}` must fit uint16", column.name))
            },
        )?))),
        SqlDataType::UInt32 => Ok(Arc::new(UInt32Array::from(collect_feldera_column_values(
            column,
            rows,
            |value| {
                let value = value
                    .as_u64()
                    .ok_or_else(|| format!("column `{}` must be uint32", column.name))?;
                u32::try_from(value)
                    .map_err(|_| format!("column `{}` must fit uint32", column.name))
            },
        )?))),
        SqlDataType::UInt64 => Ok(Arc::new(UInt64Array::from(collect_feldera_column_values(
            column,
            rows,
            |value| {
                value
                    .as_u64()
                    .ok_or_else(|| format!("column `{}` must be uint64", column.name))
            },
        )?))),
        SqlDataType::Float32 => Ok(Arc::new(Float32Array::from(collect_feldera_column_values(
            column,
            rows,
            |value| {
                let value = value
                    .as_f64()
                    .filter(|value| value.is_finite())
                    .ok_or_else(|| format!("column `{}` must be finite float32", column.name))?;
                let value = value as f32;
                if value.is_finite() {
                    Ok(value)
                } else {
                    Err(format!("column `{}` must fit finite float32", column.name))
                }
            },
        )?))),
        SqlDataType::Float64 => Ok(Arc::new(Float64Array::from(collect_feldera_column_values(
            column,
            rows,
            |value| {
                value
                    .as_f64()
                    .filter(|value| value.is_finite())
                    .ok_or_else(|| format!("column `{}` must be finite float64", column.name))
            },
        )?))),
        SqlDataType::Char { .. }
        | SqlDataType::Utf8
        | SqlDataType::Uuid
        | SqlDataType::Geometry => Ok(Arc::new(StringArray::from(collect_feldera_column_values(
            column,
            rows,
            |value| match value {
                Value::String(value) => Ok(value.clone()),
                _ => Err(format!("column `{}` must be string", column.name)),
            },
        )?))),
        SqlDataType::Json => Ok(Arc::new(StringArray::from(collect_feldera_column_values(
            column,
            rows,
            |value| serde_json::to_string(value).map_err(|error| error.to_string()),
        )?))),
        SqlDataType::Binary { .. } | SqlDataType::Varbinary => {
            let values = collect_feldera_column_values(column, rows, |value| {
                let bytes = parse_feldera_output_binary_value(column, value)?;
                validate_sql_fixed_binary_length(
                    column.name.as_str(),
                    &column.data_type,
                    bytes.len(),
                )?;
                Ok(bytes)
            })?;
            Ok(Arc::new(BinaryArray::from_iter(
                values.iter().map(|value| value.as_deref()),
            )))
        }
        SqlDataType::Date => Ok(Arc::new(Date32Array::from(collect_feldera_column_values(
            column,
            rows,
            |value| parse_date32_value(column, value),
        )?))),
        SqlDataType::Time => Ok(Arc::new(Time64NanosecondArray::from(
            collect_feldera_column_values(column, rows, |value| {
                parse_time64_nanos_value(column, value)
            })?,
        ))),
        SqlDataType::Timestamp { .. } => Ok(Arc::new(TimestampNanosecondArray::from(
            collect_feldera_column_values(column, rows, |value| {
                parse_timestamp_nanos_value(column, value)
            })?,
        ))),
        SqlDataType::Decimal { precision, scale } => {
            let scale_i8 =
                i8::try_from(*scale).map_err(|_| "decimal scale is out of range".to_string())?;
            let values = collect_feldera_column_values(column, rows, |value| match value {
                Value::Number(number) => parse_decimal128(&number.to_string(), *precision, *scale)
                    .map_err(|reason| {
                        format!("column `{}` invalid decimal: {reason}", column.name)
                    }),
                Value::String(raw) => parse_decimal128(raw, *precision, *scale).map_err(|reason| {
                    format!("column `{}` invalid decimal: {reason}", column.name)
                }),
                _ => Err(format!(
                    "column `{}` must be decimal string or number",
                    column.name
                )),
            })?;
            Ok(Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(*precision, scale_i8)
                    .map_err(|error| error.to_string())?,
            ))
        }
        SqlDataType::Array { .. } | SqlDataType::Struct { .. } | SqlDataType::Map { .. } => {
            feldera_json_reader_column_to_arrow_array(column, rows)
        }
        SqlDataType::Interval { .. } => Ok(Arc::new(StringArray::from(
            collect_feldera_column_values(column, rows, |value| match value {
                Value::String(value) => Ok(value.clone()),
                other => serde_json::to_string(other).map_err(|error| error.to_string()),
            })?,
        ))),
        SqlDataType::Null => Ok(Arc::new(NullArray::new(rows.len()))),
    }
}

fn parse_feldera_output_binary_value(
    column: &ColumnSchema,
    value: &Value,
) -> Result<Vec<u8>, String> {
    match value {
        Value::String(value) => parse_hex_binary(value)
            .map_err(|reason| format!("column `{}` invalid binary: {reason}", column.name)),
        Value::Array(values) => values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let byte = value.as_u64().ok_or_else(|| {
                    format!("column `{}` byte {index} must be an integer", column.name)
                })?;
                u8::try_from(byte)
                    .map_err(|_| format!("column `{}` byte {index} must fit uint8", column.name))
            })
            .collect(),
        _ => Err(format!(
            "column `{}` must be binary hex string or byte array",
            column.name
        )),
    }
}

fn validate_sql_fixed_binary_length(
    column_name: &str,
    data_type: &SqlDataType,
    actual_len: usize,
) -> Result<(), String> {
    let SqlDataType::Binary { length } = data_type else {
        return Ok(());
    };
    if actual_len == usize::try_from(*length).unwrap_or(usize::MAX) {
        return Ok(());
    }
    Err(format!(
        "column `{column_name}` must contain exactly {length} bytes, got {actual_len}"
    ))
}

fn feldera_json_reader_column_to_arrow_array(
    column: &ColumnSchema,
    rows: &[Value],
) -> Result<ArrayRef, String> {
    let data_type =
        arrow_data_type_from_sql_data_type(&column.data_type).map_err(|error| error.to_string())?;
    if rows.is_empty() {
        return Ok(new_empty_array(&data_type));
    }
    let field = Arc::new(Field::new(
        column.name.as_str(),
        data_type.clone(),
        column.nullable,
    ));
    let mut lines = String::new();
    for row in rows {
        let value = feldera_row_column_value_for_json_reader(column, row)?;
        lines.push_str(&serde_json::to_string(&value).map_err(|error| error.to_string())?);
        lines.push('\n');
    }
    let mut reader = arrow::json::ReaderBuilder::new_with_field(field)
        .with_batch_size(rows.len())
        .build(Cursor::new(lines.into_bytes()))
        .map_err(|error| error.to_string())?;
    let batch = reader
        .next()
        .transpose()
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("column `{}` produced no Arrow JSON batch", column.name))?;
    if batch.num_rows() != rows.len() {
        return Err(format!(
            "column `{}` produced {} rows for {} Feldera rows",
            column.name,
            batch.num_rows(),
            rows.len()
        ));
    }
    Ok(batch.column(0).clone())
}

fn feldera_row_column_value_for_json_reader(
    column: &ColumnSchema,
    row: &Value,
) -> Result<Value, String> {
    let object = row
        .as_object()
        .ok_or_else(|| "Feldera query row must be an object".to_string())?;
    let Some(value) = object.get(&column.name) else {
        if column.nullable {
            return Ok(Value::Null);
        }
        return Err(format!("column `{}` is missing", column.name));
    };
    if value.is_null() && !column.nullable {
        return Err(format!("column `{}` must be non-null", column.name));
    }
    Ok(value.clone())
}

fn collect_feldera_column_values<T>(
    column: &ColumnSchema,
    rows: &[Value],
    parse: impl Fn(&Value) -> Result<T, String>,
) -> Result<Vec<Option<T>>, String> {
    rows.iter()
        .map(|row| {
            let object = row
                .as_object()
                .ok_or_else(|| "Feldera query row must be an object".to_string())?;
            let Some(value) = object.get(&column.name) else {
                if column.nullable {
                    return Ok(None);
                }
                return Err(format!("column `{}` is missing", column.name));
            };
            if value.is_null() {
                if column.nullable {
                    Ok(None)
                } else {
                    Err(format!("column `{}` must be non-null", column.name))
                }
            } else {
                parse(value).map(Some)
            }
        })
        .collect()
}

fn parse_hex_binary(raw: &str) -> Result<Vec<u8>, String> {
    let trimmed = raw.trim();
    let hex = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    if hex.len() % 2 != 0 {
        return Err("hex string must contain an even number of digits".to_string());
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks_exact(2) {
        let high = hex_digit_value(chunk[0])?;
        let low = hex_digit_value(chunk[1])?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn hex_digit_value(value: u8) -> Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err("hex string contains a non-hex digit".to_string()),
    }
}

fn parse_date32_value(column: &ColumnSchema, value: &Value) -> Result<i32, String> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .ok_or_else(|| format!("column `{}` must be date32 integer", column.name)),
        Value::String(raw) => {
            let days = parse_date_days(raw)
                .map_err(|reason| format!("column `{}` invalid date: {reason}", column.name))?;
            i32::try_from(days)
                .map_err(|_| format!("column `{}` date is outside Date32 range", column.name))
        }
        _ => Err(format!(
            "column `{}` must be date32 integer or date string",
            column.name
        )),
    }
}

fn parse_time64_nanos_value(column: &ColumnSchema, value: &Value) -> Result<i64, String> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| format!("column `{}` must be time nanos", column.name)),
        Value::String(raw) => parse_time_nanos(raw)
            .map_err(|reason| format!("column `{}` invalid time: {reason}", column.name)),
        _ => Err(format!(
            "column `{}` must be time nanos or time string",
            column.name
        )),
    }
}

fn parse_timestamp_nanos_value(column: &ColumnSchema, value: &Value) -> Result<i64, String> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .ok_or_else(|| format!("column `{}` must be timestamp nanos", column.name)),
        Value::String(raw) => parse_timestamp_nanos(raw)
            .map_err(|reason| format!("column `{}` invalid timestamp: {reason}", column.name)),
        _ => Err(format!(
            "column `{}` must be timestamp nanos or timestamp string",
            column.name
        )),
    }
}

fn parse_date_days(raw: &str) -> Result<i64, String> {
    let trimmed = raw.trim();
    let parts = trimmed.split('-').collect::<Vec<_>>();
    let [year, month, day] = parts.as_slice() else {
        return Err("expected YYYY-MM-DD".to_string());
    };
    let year = parse_ascii_i32(year, "year")?;
    let month = parse_ascii_u32(month, "month")?;
    let day = parse_ascii_u32(day, "day")?;
    days_from_civil(year, month, day)
}

fn parse_time_nanos(raw: &str) -> Result<i64, String> {
    let trimmed = raw.trim();
    let parts = trimmed.split(':').collect::<Vec<_>>();
    let [hour, minute, second] = parts.as_slice() else {
        return Err("expected HH:MM:SS[.fffffffff]".to_string());
    };
    let hour = parse_ascii_u32(hour, "hour")?;
    let minute = parse_ascii_u32(minute, "minute")?;
    let (second, fraction_nanos) = parse_second_and_fraction(second)?;
    if hour > 23 {
        return Err("hour must be 0-23".to_string());
    }
    if minute > 59 {
        return Err("minute must be 0-59".to_string());
    }
    if second > 59 {
        return Err("second must be 0-59".to_string());
    }
    let seconds = i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second);
    Ok(seconds * 1_000_000_000 + i64::from(fraction_nanos))
}

fn parse_timestamp_nanos(raw: &str) -> Result<i64, String> {
    let trimmed = raw.trim();
    let (date, time_with_zone) = if let Some((date, time)) = trimmed.split_once('T') {
        (date, time)
    } else if let Some((date, time)) = trimmed.split_once(' ') {
        (date, time)
    } else {
        return Err("expected YYYY-MM-DD HH:MM:SS[.fraction]".to_string());
    };
    let (time, offset_seconds) = split_time_and_timezone_offset(time_with_zone)?;
    let days = parse_date_days(date)?;
    let time_nanos = parse_time_nanos(time)?;
    let day_nanos = days
        .checked_mul(86_400_000_000_000)
        .ok_or_else(|| "timestamp day component is out of range".to_string())?;
    let local_nanos = day_nanos
        .checked_add(time_nanos)
        .ok_or_else(|| "timestamp time component is out of range".to_string())?;
    local_nanos
        .checked_sub(offset_seconds * 1_000_000_000)
        .ok_or_else(|| "timestamp timezone offset is out of range".to_string())
}

fn split_time_and_timezone_offset(raw: &str) -> Result<(&str, i64), String> {
    let trimmed = raw.trim();
    if let Some(time) = trimmed.strip_suffix('Z') {
        return Ok((time, 0));
    }
    let offset_start = trimmed
        .rfind('+')
        .or_else(|| trimmed.rfind('-'))
        .filter(|index| *index > 0);
    let Some(offset_start) = offset_start else {
        return Ok((trimmed, 0));
    };
    let (time, offset) = trimmed.split_at(offset_start);
    let sign = if offset.starts_with('-') { -1 } else { 1 };
    let offset = &offset[1..];
    let parts = offset.split(':').collect::<Vec<_>>();
    let [hour, minute] = parts.as_slice() else {
        return Err("timezone offset must be HH:MM".to_string());
    };
    let hour = parse_ascii_u32(hour, "timezone hour")?;
    let minute = parse_ascii_u32(minute, "timezone minute")?;
    if hour > 23 || minute > 59 {
        return Err("timezone offset is out of range".to_string());
    }
    Ok((time, sign * i64::from(hour * 3_600 + minute * 60)))
}

fn parse_second_and_fraction(raw: &str) -> Result<(u32, u32), String> {
    let (second, fraction) = raw
        .split_once('.')
        .map(|(second, fraction)| (second, Some(fraction)))
        .unwrap_or((raw, None));
    let second = parse_ascii_u32(second, "second")?;
    let Some(fraction) = fraction else {
        return Ok((second, 0));
    };
    if fraction.is_empty() {
        return Err("fraction must contain digits".to_string());
    }
    if fraction.len() > 9 || !fraction.bytes().all(|value| value.is_ascii_digit()) {
        return Err("fraction must contain 1-9 digits".to_string());
    }
    let mut nanos = fraction
        .parse::<u32>()
        .map_err(|_| "fraction is out of range".to_string())?;
    for _ in fraction.len()..9 {
        nanos *= 10;
    }
    Ok((second, nanos))
}

fn parse_ascii_i32(raw: &str, name: &str) -> Result<i32, String> {
    if raw.is_empty() || !raw.bytes().all(|value| value.is_ascii_digit()) {
        return Err(format!("{name} must contain digits"));
    }
    raw.parse::<i32>()
        .map_err(|_| format!("{name} is out of range"))
}

fn parse_ascii_u32(raw: &str, name: &str) -> Result<u32, String> {
    if raw.is_empty() || !raw.bytes().all(|value| value.is_ascii_digit()) {
        return Err(format!("{name} must contain digits"));
    }
    raw.parse::<u32>()
        .map_err(|_| format!("{name} is out of range"))
}

fn days_from_civil(mut year: i32, month: u32, day: u32) -> Result<i64, String> {
    if !(1..=12).contains(&month) {
        return Err("month must be 1-12".to_string());
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return Err(format!("day must be 1-{max_day} for month {month}"));
    }

    year -= i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let adjusted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * adjusted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = i64::from(year_of_era) * 365 + i64::from(year_of_era / 4)
        - i64::from(year_of_era / 100)
        + day_of_year;
    Ok(i64::from(era) * 146_097 + day_of_era - 719_468)
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
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
    if let Some(output_relation_id) = &api.output_relation_id {
        if output_relation_id.trim().is_empty() {
            return Err(ApiError::bad_request(
                "output_relation_id must not be blank when provided",
            ));
        }
    }
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
    validate_response_schema_contract(api.response_schema.as_ref())?;
    if let Some(sql_template) = &api.sql_template {
        validate_sql_template_contract(sql_template, &api.request)?;
        validate_sql_template_parameter_coverage(sql_template, api)?;
    }
    Ok(())
}

fn validate_response_schema_contract(
    response_schema: Option<&MaterializedViewResponseSchema>,
) -> Result<(), ApiError> {
    let Some(response_schema) = response_schema else {
        return Ok(());
    };
    let mut column_names = BTreeSet::new();
    for column in &response_schema.columns {
        if column.name.trim().is_empty() {
            return Err(ApiError::bad_request(
                "response schema column name must not be blank",
            ));
        }
        if !column_names.insert(column.name.as_str()) {
            return Err(ApiError::bad_request(format!(
                "response schema column `{}` is declared more than once",
                column.name
            )));
        }
        if column.source.trim().is_empty() {
            return Err(ApiError::bad_request(format!(
                "response schema column `{}` source must not be blank",
                column.name
            )));
        }
        if !response_column_type_supported(&column.r#type) {
            return Err(ApiError::bad_request(format!(
                "response schema column `{}` declares unsupported type `{}`",
                column.name, column.r#type
            )));
        }
    }
    Ok(())
}

fn response_column_type_supported(type_name: &str) -> bool {
    matches!(
        type_name,
        "string"
            | "int64"
            | "integer"
            | "float64"
            | "number"
            | "bool"
            | "boolean"
            | "date"
            | "time"
            | "timestamp"
            | "uuid"
            | "decimal"
            | "binary_hex"
            | "array"
            | "object"
            | "json"
    )
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
    sql_template_validation_mode: SqlTemplateValidationMode,
) -> Result<(), ApiError> {
    validate_view_api_output_binding(view_id, api, output_schemas)?;
    let output_id = api_output_binding_id(view_id, api, output_schemas)?;
    let Some(sql_template) = api.sql_template.as_deref() else {
        if !api.request.is_empty() {
            return Err(ApiError::bad_request(format!(
                "standing runtime view `{view_id}` has request parameters but no sql_template"
            )));
        }
        return Ok(());
    };
    if sql_template_validation_mode != SqlTemplateValidationMode::ExternalFelderaRuntime
        && !sql_references_table(sql_template, &output_id)
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime view `{view_id}` sql_template must reference table `{output_id}`"
        )));
    }
    let output_schema = output_schemas
        .iter()
        .find(|schema| schema.relation_id == output_id)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "standing runtime view `{view_id}` artifact metadata has no matching output schema `{output_id}`"
            ))
        })?;
    if sql_template_validation_mode == SqlTemplateValidationMode::ExternalFelderaRuntime {
        return Ok(());
    }
    let table_schema = arrow_schema_from_feldera_relation_schema(output_schema)?;
    let bound_sql = render_view_sql_template_for_validation(sql_template, &api.request)?;
    validate_record_batch_table_query_with_bindings_and_policy(
        &output_id,
        table_schema,
        &normalize_view_query_sql(&bound_sql.sql, &output_id),
        &bound_sql.bind_values,
        QueryPolicy::default(),
    )
    .await
    .map_err(ApiError::bad_request)?;
    Ok(())
}

fn validate_view_api_output_binding(
    view_id: &str,
    api: &MaterializedViewApiMetadata,
    output_schemas: &[RelationSchema],
) -> Result<(), ApiError> {
    if api.output_relation_id.is_none() && api.url_path.is_none() && api.sql_template.is_none() {
        return Ok(());
    }
    if api.url_path.is_some() && output_schemas.len() > 1 && api.output_relation_id.is_none() {
        return Err(ApiError::bad_request(format!(
            "view `{view_id}` has multiple outputs; promoted API routes must set output_relation_id"
        )));
    }
    if api.sql_template.is_some() && output_schemas.len() > 1 && api.output_relation_id.is_none() {
        return Err(ApiError::bad_request(format!(
            "view `{view_id}` has multiple outputs; templated APIs must set output_relation_id"
        )));
    }
    let output_id = api_output_binding_id(view_id, api, output_schemas)?;
    if !output_schemas
        .iter()
        .any(|schema| schema.relation_id == output_id.as_str())
    {
        return Err(ApiError::bad_request(format!(
            "view `{view_id}` has no output schema `{output_id}`"
        )));
    }
    Ok(())
}

fn api_output_binding_id(
    view_id: &str,
    api: &MaterializedViewApiMetadata,
    output_schemas: &[RelationSchema],
) -> Result<String, ApiError> {
    if let Some(output_id) = &api.output_relation_id {
        return Ok(output_id.clone());
    }
    if output_schemas.len() == 1 {
        return Ok(output_schemas[0].relation_id.clone());
    }
    if output_schemas
        .iter()
        .any(|schema| schema.relation_id == view_id)
    {
        return Ok(view_id.to_string());
    }
    Ok(view_id.to_string())
}

fn validate_standing_runtime_query_contract(
    view_id: &str,
    request_sql: Option<&String>,
    api: &MaterializedViewApiMetadata,
    parameters: &BTreeMap<String, Value>,
    page_request: &SnapshotPageRequest,
    allow_feldera_runtime_sql: bool,
) -> Result<(), ApiError> {
    if request_sql.is_some() && !allow_feldera_runtime_sql {
        return Err(ApiError::bad_request(format!(
            "caller-supplied SQL is not supported for standing runtime view `{view_id}`"
        )));
    }
    if request_sql.is_some() {
        return Ok(());
    }
    if api.sql_template.is_none() && (!api.request.is_empty() || !parameters.is_empty()) {
        return Err(ApiError::conflict(format!(
            "standing runtime view `{view_id}` has request parameters but no sql_template"
        )));
    }
    if api.sql_template.is_some() && page_request.page_token.is_some() && !allow_feldera_runtime_sql
    {
        return Err(ApiError::bad_request(format!(
            "cursor pagination is not supported for templated standing runtime view `{view_id}`"
        )));
    }
    if api.sql_template.is_some() && page_request.max_rows.is_some() && !allow_feldera_runtime_sql {
        return Err(ApiError::bad_request(format!(
            "row limits are not supported for templated standing runtime view `{view_id}`"
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
        "string" | "int64" | "integer" | "float64" | "number" | "bool" | "boolean" | "json"
        | "array" => {}
        other => {
            return Err(unsupported_request_field_type_error(
                &field.field_name,
                other,
            ));
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

fn unsupported_request_field_type_error(name: &str, field_type: &str) -> ApiError {
    if field_type.eq_ignore_ascii_case("variant") {
        ApiError::bad_request(format!(
            "request field `{name}` declares unsupported type `variant`: Feldera pipeline-manager /query does not support request-time VARIANT bind literals; use type `json` for canonical JSON text parameters or compute VARIANT inside a Feldera view"
        ))
    } else {
        ApiError::bad_request(format!(
            "request field `{name}` declares unsupported type `{field_type}`"
        ))
    }
}

struct SqlTemplatePlaceholder {
    start: usize,
    expression_start: usize,
    expression_end: usize,
    end: usize,
}

fn next_sql_template_placeholder(
    sql: &str,
    mut offset: usize,
) -> Result<Option<SqlTemplatePlaceholder>, ApiError> {
    while offset < sql.len() {
        let rest = &sql[offset..];
        if rest.starts_with("{{") {
            let expression_start = offset + 2;
            let after_start = &sql[expression_start..];
            let Some(end) = after_start.find("}}") else {
                return Err(ApiError::bad_request(
                    "query template contains an unclosed parameter placeholder",
                ));
            };
            let expression_end = expression_start + end;
            return Ok(Some(SqlTemplatePlaceholder {
                start: offset,
                expression_start,
                expression_end,
                end: expression_end + 2,
            }));
        }
        if rest.starts_with("}}") {
            return Err(ApiError::bad_request(
                "query template contains an unopened parameter placeholder",
            ));
        }
        if rest.starts_with('\'') {
            advance_sql_single_quoted_literal(sql, &mut offset);
            continue;
        }
        if rest.starts_with('"') {
            advance_sql_double_quoted_identifier(sql, &mut offset);
            continue;
        }
        if rest.starts_with("--") {
            advance_sql_line_comment(sql, &mut offset);
            continue;
        }
        if rest.starts_with("/*") {
            advance_sql_block_comment(sql, &mut offset);
            continue;
        }
        let ch = rest
            .chars()
            .next()
            .expect("non-empty SQL slice while scanning SQL template");
        offset += ch.len_utf8();
    }
    Ok(None)
}

fn advance_sql_single_quoted_literal(sql: &str, offset: &mut usize) {
    *offset += 1;
    while *offset < sql.len() {
        let rest = &sql[*offset..];
        if rest.starts_with("''") {
            *offset += 2;
            continue;
        }
        let ch = rest
            .chars()
            .next()
            .expect("non-empty SQL slice while skipping SQL string literal");
        *offset += ch.len_utf8();
        if ch == '\'' {
            break;
        }
    }
}

fn advance_sql_double_quoted_identifier(sql: &str, offset: &mut usize) {
    *offset += 1;
    while *offset < sql.len() {
        let rest = &sql[*offset..];
        if rest.starts_with("\"\"") {
            *offset += 2;
            continue;
        }
        let ch = rest
            .chars()
            .next()
            .expect("non-empty SQL slice while skipping SQL quoted identifier");
        *offset += ch.len_utf8();
        if ch == '"' {
            break;
        }
    }
}

fn advance_sql_line_comment(sql: &str, offset: &mut usize) {
    let rest = &sql[*offset..];
    if let Some(line_end) = rest.find('\n') {
        *offset += line_end;
    } else {
        *offset = sql.len();
    }
}

fn advance_sql_block_comment(sql: &str, offset: &mut usize) {
    let rest = &sql[*offset..];
    if let Some(comment_end) = rest[2..].find("*/") {
        *offset += 2 + comment_end + 2;
    } else {
        *offset = sql.len();
    }
}

fn validate_sql_template_contract(
    sql_template: &str,
    request: &[MaterializedViewRequestFieldSpec],
) -> Result<(), ApiError> {
    let fields = request
        .iter()
        .map(|field| (field.field_name.as_str(), field))
        .collect::<BTreeMap<_, _>>();
    let mut offset = 0;

    while let Some(placeholder) = next_sql_template_placeholder(sql_template, offset)? {
        let expression =
            sql_template[placeholder.expression_start..placeholder.expression_end].trim();
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
        offset = placeholder.end;
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
    let mut offset = 0;

    while let Some(placeholder) = next_sql_template_placeholder(sql_template, offset)? {
        let expression =
            sql_template[placeholder.expression_start..placeholder.expression_end].trim();
        let (name, _) = parse_template_parameter_expression(expression)?;
        parameters.insert(name.to_string());
        offset = placeholder.end;
    }

    Ok(parameters)
}

fn validate_filter_contract(name: &str, filter: &str) -> Result<(), ApiError> {
    match filter_name(filter) {
        "is_required" | "is_string" | "is_boolean" | "to_json" | "is_date" | "is_time"
        | "is_timestamp" | "is_uuid" | "is_decimal" | "is_binary_hex" | "is_json" => {
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
        "is_array" => validate_array_filter_contract(name, filter)?,
        "is_variant" => return Err(unsupported_variant_filter_error(name)),
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
        "json" => canonical_json_literal(name, value).map(|_| ()),
        "array" => value
            .as_array()
            .map(|_| ())
            .ok_or_else(|| parameter_filter_error(name, "is_array")),
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

const FELDERA_PREPARED_QUERY_NAME: &str = "velorix_query";

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
    let mut offset = 0;
    let mut copied_until = 0;

    while let Some(placeholder) = next_sql_template_placeholder(template, offset)? {
        output.push_str(&template[copied_until..placeholder.start]);
        let expression = template[placeholder.expression_start..placeholder.expression_end].trim();
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
        offset = placeholder.end;
        copied_until = placeholder.end;
    }
    output.push_str(&template[copied_until..]);

    Ok(BoundViewSql {
        sql: output,
        bind_values,
    })
}

fn render_view_sql_template_as_feldera_sql(
    template: &str,
    fields: &[MaterializedViewRequestFieldSpec],
    values: &BTreeMap<String, Value>,
) -> Result<String, ApiError> {
    let bound_sql = render_view_sql_template(template, fields, values)?;
    let bound_sql = rewrite_feldera_array_unnest_placeholders(bound_sql)?;
    feldera_prepared_query_sql(bound_sql)
}

fn render_caller_sql_as_feldera_sql(
    sql: &str,
    values: &BTreeMap<String, Value>,
) -> Result<String, ApiError> {
    let bound_sql = render_caller_sql_as_feldera_bound_sql(sql, values)?;
    let bound_sql = rewrite_feldera_array_unnest_placeholders(bound_sql)?;
    feldera_prepared_query_sql(bound_sql)
}

fn render_caller_sql_as_feldera_bound_sql(
    sql: &str,
    values: &BTreeMap<String, Value>,
) -> Result<BoundViewSql, ApiError> {
    if values.is_empty() {
        return Ok(BoundViewSql {
            sql: sql.to_string(),
            bind_values: Vec::new(),
        });
    }

    let mut output = String::with_capacity(sql.len());
    let mut bind_values = Vec::new();
    let mut offset = 0;
    let mut used = BTreeSet::new();

    while offset < sql.len() {
        let rest = &sql[offset..];
        if rest.starts_with("{{") {
            let after_start_offset = offset + 2;
            let after_start = &sql[after_start_offset..];
            let Some(end) = after_start.find("}}") else {
                output.push_str(rest);
                break;
            };
            let placeholder_end_offset = after_start_offset + end + 2;
            let expression = after_start[..end].trim();
            if !expression.starts_with("context.params.") {
                output.push_str(&sql[offset..placeholder_end_offset]);
                offset = placeholder_end_offset;
                continue;
            }
            let (name, filters) = parse_template_parameter_expression(expression)?;
            used.insert(name.to_string());
            match caller_sql_parameter_bind_value(name, values.get(name), &filters)? {
                Some(value) => {
                    bind_values.push(value);
                    output.push_str(&format!("${}", bind_values.len()));
                }
                None => output.push_str("NULL"),
            }
            offset = placeholder_end_offset;
            continue;
        }
        if rest.starts_with('\'') {
            copy_sql_single_quoted_literal(sql, &mut offset, &mut output);
            continue;
        }
        if rest.starts_with('"') {
            copy_sql_double_quoted_identifier(sql, &mut offset, &mut output);
            continue;
        }
        if rest.starts_with("--") {
            copy_sql_line_comment(sql, &mut offset, &mut output);
            continue;
        }
        if rest.starts_with("/*") {
            copy_sql_block_comment(sql, &mut offset, &mut output);
            continue;
        }

        let ch = rest
            .chars()
            .next()
            .expect("non-empty SQL slice while rendering caller SQL");
        output.push(ch);
        offset += ch.len_utf8();
    }

    if let Some(extra) = values.keys().find(|name| !used.contains(name.as_str())) {
        return Err(ApiError::bad_request(format!(
            "caller-supplied SQL parameter `{extra}` is not referenced by the SQL"
        )));
    }

    Ok(BoundViewSql {
        sql: output,
        bind_values,
    })
}

fn feldera_prepared_query_sql(bound_sql: BoundViewSql) -> Result<String, ApiError> {
    if bound_sql.bind_values.is_empty() {
        return Ok(bound_sql.sql);
    }
    let query_sql = trim_feldera_prepared_statement_sql(&bound_sql.sql);
    if query_sql.is_empty() {
        return Err(ApiError::bad_request(
            "Feldera prepared query SQL cannot be empty",
        ));
    }
    if feldera_sql_has_statement_separator(query_sql) {
        return Err(ApiError::bad_request(
            "Feldera prepared query parameters require a single SQL statement",
        ));
    }
    let args = bound_sql
        .bind_values
        .iter()
        .map(query_bind_value_to_feldera_sql_literal)
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    Ok(format!(
        "PREPARE {FELDERA_PREPARED_QUERY_NAME} AS {query_sql};\nEXECUTE {FELDERA_PREPARED_QUERY_NAME}({args});"
    ))
}

fn rewrite_feldera_array_unnest_placeholders(
    bound_sql: BoundViewSql,
) -> Result<BoundViewSql, ApiError> {
    if bound_sql.bind_values.is_empty() {
        return Ok(bound_sql);
    }

    let mut output = String::with_capacity(bound_sql.sql.len());
    let mut bind_values = Vec::new();
    let mut offset = 0;

    while offset < bound_sql.sql.len() {
        let rest = &bound_sql.sql[offset..];
        if rest.starts_with('\'') {
            copy_sql_single_quoted_literal(&bound_sql.sql, &mut offset, &mut output);
            continue;
        }
        if rest.starts_with('"') {
            copy_sql_double_quoted_identifier(&bound_sql.sql, &mut offset, &mut output);
            continue;
        }
        if rest.starts_with("--") {
            copy_sql_line_comment(&bound_sql.sql, &mut offset, &mut output);
            continue;
        }
        if rest.starts_with("/*") {
            copy_sql_block_comment(&bound_sql.sql, &mut offset, &mut output);
            continue;
        }
        if let Some((end, index)) = parse_feldera_in_unnest_placeholder(&bound_sql.sql, offset) {
            let value = bind_value_by_one_based_index(&bound_sql.bind_values, index)?;
            let Some(values) = array_bind_value_elements(value) else {
                return Err(ApiError::bad_request(
                    "Feldera IN UNNEST query parameter must be an array",
                ));
            };
            output.push_str("IN (");
            if values.is_empty() {
                output.push_str("NULL");
            } else {
                for (value_index, value) in values.into_iter().enumerate() {
                    if value_index > 0 {
                        output.push_str(", ");
                    }
                    bind_values.push(value);
                    output.push_str(&format!("${}", bind_values.len()));
                }
            }
            output.push(')');
            offset = end;
            continue;
        }
        if rest.starts_with('$') {
            if let Some((end, index)) = parse_feldera_placeholder(&bound_sql.sql, offset) {
                let value = bind_value_by_one_based_index(&bound_sql.bind_values, index)?;
                bind_values.push(value.clone());
                output.push_str(&format!("${}", bind_values.len()));
                offset = end;
                continue;
            }
        }

        let ch = rest
            .chars()
            .next()
            .expect("non-empty SQL slice while rewriting Feldera placeholders");
        output.push(ch);
        offset += ch.len_utf8();
    }

    Ok(BoundViewSql {
        sql: output,
        bind_values,
    })
}

fn bind_value_by_one_based_index(
    bind_values: &[QueryBindValue],
    index: usize,
) -> Result<&QueryBindValue, ApiError> {
    index
        .checked_sub(1)
        .and_then(|index| bind_values.get(index))
        .ok_or_else(|| ApiError::bad_request("Feldera query placeholder index is out of range"))
}

fn array_bind_value_elements(value: &QueryBindValue) -> Option<Vec<QueryBindValue>> {
    match value {
        QueryBindValue::Utf8Array(values) => {
            Some(values.iter().cloned().map(QueryBindValue::Utf8).collect())
        }
        QueryBindValue::JsonArray(values) => {
            Some(values.iter().cloned().map(QueryBindValue::Json).collect())
        }
        QueryBindValue::Int64Array(values) => {
            Some(values.iter().copied().map(QueryBindValue::Int64).collect())
        }
        QueryBindValue::Float64Array(values) => Some(
            values
                .iter()
                .copied()
                .map(QueryBindValue::Float64)
                .collect(),
        ),
        QueryBindValue::BooleanArray(values) => Some(
            values
                .iter()
                .copied()
                .map(QueryBindValue::Boolean)
                .collect(),
        ),
        QueryBindValue::DateArray(values) => {
            Some(values.iter().cloned().map(QueryBindValue::Date).collect())
        }
        QueryBindValue::TimeArray(values) => {
            Some(values.iter().cloned().map(QueryBindValue::Time).collect())
        }
        QueryBindValue::TimestampArray(values) => Some(
            values
                .iter()
                .cloned()
                .map(QueryBindValue::Timestamp)
                .collect(),
        ),
        QueryBindValue::UuidArray(values) => {
            Some(values.iter().cloned().map(QueryBindValue::Uuid).collect())
        }
        QueryBindValue::DecimalArray(values) => Some(
            values
                .iter()
                .cloned()
                .map(QueryBindValue::Decimal)
                .collect(),
        ),
        QueryBindValue::BinaryArray(values) => {
            Some(values.iter().cloned().map(QueryBindValue::Binary).collect())
        }
        QueryBindValue::Utf8(_)
        | QueryBindValue::Json(_)
        | QueryBindValue::Int64(_)
        | QueryBindValue::Float64(_)
        | QueryBindValue::Boolean(_)
        | QueryBindValue::Date(_)
        | QueryBindValue::Time(_)
        | QueryBindValue::Timestamp(_)
        | QueryBindValue::Uuid(_)
        | QueryBindValue::Decimal(_)
        | QueryBindValue::Binary(_) => None,
    }
}

fn parse_feldera_in_unnest_placeholder(sql: &str, offset: usize) -> Option<(usize, usize)> {
    let mut cursor = offset;
    cursor = parse_ascii_keyword(sql, cursor, "in")?;
    let after_in = skip_ascii_whitespace(sql, cursor);
    if after_in == cursor {
        return None;
    }
    cursor = parse_ascii_keyword(sql, after_in, "unnest")?;
    cursor = skip_ascii_whitespace(sql, cursor);
    cursor = consume_ascii_byte(sql, cursor, b'(')?;
    cursor = skip_ascii_whitespace(sql, cursor);
    let (after_placeholder, index) = parse_feldera_placeholder(sql, cursor)?;
    cursor = skip_ascii_whitespace(sql, after_placeholder);
    cursor = consume_ascii_byte(sql, cursor, b')')?;
    Some((cursor, index))
}

fn parse_feldera_placeholder(sql: &str, offset: usize) -> Option<(usize, usize)> {
    let bytes = sql.as_bytes();
    if bytes.get(offset).copied()? != b'$' {
        return None;
    }
    let mut cursor = offset + 1;
    let digits_start = cursor;
    while matches!(bytes.get(cursor), Some(b'0'..=b'9')) {
        cursor += 1;
    }
    if cursor == digits_start {
        return None;
    }
    let index = sql[digits_start..cursor].parse::<usize>().ok()?;
    Some((cursor, index))
}

fn parse_ascii_keyword(sql: &str, offset: usize, keyword: &str) -> Option<usize> {
    let end = offset.checked_add(keyword.len())?;
    let raw = sql.get(offset..end)?;
    if !raw.eq_ignore_ascii_case(keyword) {
        return None;
    }
    if sql
        .as_bytes()
        .get(end)
        .is_some_and(|value| value.is_ascii_alphanumeric() || *value == b'_')
    {
        return None;
    }
    Some(end)
}

fn skip_ascii_whitespace(sql: &str, mut offset: usize) -> usize {
    while sql
        .as_bytes()
        .get(offset)
        .is_some_and(|value| value.is_ascii_whitespace())
    {
        offset += 1;
    }
    offset
}

fn consume_ascii_byte(sql: &str, offset: usize, expected: u8) -> Option<usize> {
    if sql.as_bytes().get(offset).copied()? == expected {
        Some(offset + 1)
    } else {
        None
    }
}

fn trim_feldera_prepared_statement_sql(sql: &str) -> &str {
    sql.trim().trim_end_matches(';').trim_end()
}

fn feldera_sql_has_statement_separator(sql: &str) -> bool {
    let mut offset = 0;
    let mut copied = String::new();
    while offset < sql.len() {
        let rest = &sql[offset..];
        if rest.starts_with('\'') {
            copy_sql_single_quoted_literal(sql, &mut offset, &mut copied);
            continue;
        }
        if rest.starts_with('"') {
            copy_sql_double_quoted_identifier(sql, &mut offset, &mut copied);
            continue;
        }
        if rest.starts_with("--") {
            copy_sql_line_comment(sql, &mut offset, &mut copied);
            continue;
        }
        if rest.starts_with("/*") {
            copy_sql_block_comment(sql, &mut offset, &mut copied);
            continue;
        }
        let ch = rest
            .chars()
            .next()
            .expect("non-empty SQL slice while scanning Feldera SQL");
        if ch == ';' {
            return true;
        }
        offset += ch.len_utf8();
    }
    false
}

fn copy_sql_single_quoted_literal(sql: &str, offset: &mut usize, output: &mut String) {
    output.push('\'');
    *offset += 1;
    while *offset < sql.len() {
        let rest = &sql[*offset..];
        if rest.starts_with("''") {
            output.push_str("''");
            *offset += 2;
            continue;
        }
        let ch = rest
            .chars()
            .next()
            .expect("non-empty SQL slice while copying SQL string literal");
        output.push(ch);
        *offset += ch.len_utf8();
        if ch == '\'' {
            break;
        }
    }
}

fn copy_sql_double_quoted_identifier(sql: &str, offset: &mut usize, output: &mut String) {
    output.push('"');
    *offset += 1;
    while *offset < sql.len() {
        let rest = &sql[*offset..];
        if rest.starts_with("\"\"") {
            output.push_str("\"\"");
            *offset += 2;
            continue;
        }
        let ch = rest
            .chars()
            .next()
            .expect("non-empty SQL slice while copying SQL quoted identifier");
        output.push(ch);
        *offset += ch.len_utf8();
        if ch == '"' {
            break;
        }
    }
}

fn copy_sql_line_comment(sql: &str, offset: &mut usize, output: &mut String) {
    let rest = &sql[*offset..];
    if let Some(line_end) = rest.find('\n') {
        let end = *offset + line_end;
        output.push_str(&sql[*offset..end]);
        *offset = end;
    } else {
        output.push_str(rest);
        *offset = sql.len();
    }
}

fn copy_sql_block_comment(sql: &str, offset: &mut usize, output: &mut String) {
    let rest = &sql[*offset..];
    if let Some(comment_end) = rest[2..].find("*/") {
        let end = *offset + 2 + comment_end + 2;
        output.push_str(&sql[*offset..end]);
        *offset = end;
    } else {
        output.push_str(rest);
        *offset = sql.len();
    }
}

fn caller_sql_parameter_bind_value(
    name: &str,
    value: Option<&Value>,
    filters: &[String],
) -> Result<Option<QueryBindValue>, ApiError> {
    let required = filters
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

    if let Some(filter) = filters
        .iter()
        .find(|filter| filter_name(filter) == "is_array")
    {
        return query_array_bind_value_for_json_value(name, value, filter).map(Some);
    }
    if let Some(value) = typed_feldera_literal_bind_value_for_json_value(name, value, filters)? {
        return Ok(Some(value));
    }
    if filters
        .iter()
        .any(|filter| filter_name(filter) == "to_json")
    {
        return serde_json::to_string(value)
            .map(|value| QueryBindValue::Utf8(value))
            .map(Some)
            .map_err(ApiError::bad_request);
    }
    if filters
        .iter()
        .any(|filter| filter_name(filter) == "is_string")
    {
        let value = value
            .as_str()
            .ok_or_else(|| parameter_filter_error(name, "is_string"))?;
        return Ok(Some(QueryBindValue::Utf8(value.to_string())));
    }
    if filters
        .iter()
        .any(|filter| filter_name(filter) == "is_integer")
    {
        let value = value
            .as_i64()
            .or_else(|| value.as_str().and_then(|value| value.parse::<i64>().ok()))
            .ok_or_else(|| parameter_filter_error(name, "is_integer"))?;
        return Ok(Some(QueryBindValue::Int64(value)));
    }
    if filters
        .iter()
        .any(|filter| filter_name(filter) == "is_number")
    {
        let value = value
            .as_f64()
            .or_else(|| value.as_str().and_then(|value| value.parse::<f64>().ok()))
            .filter(|value| value.is_finite())
            .ok_or_else(|| parameter_filter_error(name, "is_number"))?;
        return Ok(Some(QueryBindValue::Float64(value)));
    }
    if filters
        .iter()
        .any(|filter| filter_name(filter) == "is_boolean")
    {
        let value = value
            .as_bool()
            .or_else(|| value.as_str().and_then(parse_bool))
            .ok_or_else(|| parameter_filter_error(name, "is_boolean"))?;
        return Ok(Some(QueryBindValue::Boolean(value)));
    }

    inferred_feldera_bind_value_for_json_value(name, value)
}

fn inferred_feldera_bind_value_for_json_value(
    name: &str,
    value: &Value,
) -> Result<Option<QueryBindValue>, ApiError> {
    match value {
        Value::Null => Ok(None),
        Value::Bool(value) => Ok(Some(QueryBindValue::Boolean(*value))),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                Ok(Some(QueryBindValue::Int64(value)))
            } else if let Some(value) = number.as_u64() {
                i64::try_from(value)
                    .map(QueryBindValue::Int64)
                    .map(Some)
                    .map_err(|_| parameter_filter_error(name, "is_integer"))
            } else if let Some(value) = number.as_f64().filter(|value| value.is_finite()) {
                Ok(Some(QueryBindValue::Float64(value)))
            } else {
                Err(parameter_filter_error(name, "is_number"))
            }
        }
        Value::String(value) => Ok(Some(QueryBindValue::Utf8(value.clone()))),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value)
            .map(QueryBindValue::Utf8)
            .map(Some)
            .map_err(ApiError::bad_request),
    }
}

fn query_bind_value_to_feldera_sql_literal(value: &QueryBindValue) -> Result<String, ApiError> {
    match value {
        QueryBindValue::Utf8(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
        QueryBindValue::Json(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
        QueryBindValue::Int64(value) => Ok(value.to_string()),
        QueryBindValue::Float64(value) if value.is_finite() => Ok(value.to_string()),
        QueryBindValue::Float64(_) => Err(ApiError::bad_request(
            "non-finite float query parameter cannot be rendered as Feldera SQL",
        )),
        QueryBindValue::Boolean(value) => Ok(if *value { "TRUE" } else { "FALSE" }.to_string()),
        QueryBindValue::Date(value)
        | QueryBindValue::Time(value)
        | QueryBindValue::Timestamp(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
        QueryBindValue::Uuid(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
        QueryBindValue::Decimal(value) => Ok(value.clone()),
        QueryBindValue::Binary(value) => Ok(format!(
            "x'{}'",
            format_hex_binary(value).trim_start_matches("0x")
        )),
        QueryBindValue::Utf8Array(values) => Ok(format!(
            "ARRAY[{}]",
            values
                .iter()
                .map(|value| format!("'{}'", value.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        QueryBindValue::JsonArray(values) => Ok(format!(
            "ARRAY[{}]",
            values
                .iter()
                .map(|value| format!("'{}'", value.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        QueryBindValue::Int64Array(values) => Ok(format!(
            "ARRAY[{}]",
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )),
        QueryBindValue::Float64Array(values) => {
            let values = values
                .iter()
                .map(|value| {
                    if value.is_finite() {
                        Ok(value.to_string())
                    } else {
                        Err(ApiError::bad_request(
                            "non-finite float query parameter cannot be rendered as Feldera SQL",
                        ))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("ARRAY[{}]", values.join(", ")))
        }
        QueryBindValue::BooleanArray(values) => Ok(format!(
            "ARRAY[{}]",
            values
                .iter()
                .map(|value| if *value { "TRUE" } else { "FALSE" }.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )),
        QueryBindValue::DateArray(values)
        | QueryBindValue::TimeArray(values)
        | QueryBindValue::TimestampArray(values) => Ok(format!(
            "ARRAY[{}]",
            values
                .iter()
                .map(|value| format!("'{}'", value.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        QueryBindValue::UuidArray(values) => Ok(format!(
            "ARRAY[{}]",
            values
                .iter()
                .map(|value| format!("'{}'", value.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        QueryBindValue::DecimalArray(values) => Ok(format!("ARRAY[{}]", values.join(", "))),
        QueryBindValue::BinaryArray(values) => Ok(format!(
            "ARRAY[{}]",
            values
                .iter()
                .map(|value| format!("x'{}'", format_hex_binary(value).trim_start_matches("0x")))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
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
    let mut offset = 0;
    let mut copied_until = 0;

    while let Some(placeholder) = next_sql_template_placeholder(template, offset)? {
        output.push_str(&template[copied_until..placeholder.start]);
        let expression = template[placeholder.expression_start..placeholder.expression_end].trim();
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
        offset = placeholder.end;
        copied_until = placeholder.end;
    }

    output.push_str(&template[copied_until..]);

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
        "json" => Ok(QueryBindValue::Json("null".to_string())),
        "array" => Ok(QueryBindValue::Int64Array(vec![1])),
        other => Err(unsupported_request_field_type_error(
            &field.field_name,
            other,
        )),
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

    if let Some(filter) = filters
        .iter()
        .find(|filter| filter_name(filter) == "is_array")
    {
        return query_array_bind_value_for_json_value(name, value, filter).map(Some);
    }
    if let Some(value) = typed_feldera_literal_bind_value_for_json_value(name, value, filters)? {
        return Ok(Some(value));
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
        "is_date" => canonical_date_literal(name, value).map(|_| ()),
        "is_time" => canonical_time_literal(name, value).map(|_| ()),
        "is_timestamp" => canonical_timestamp_literal(name, value).map(|_| ()),
        "is_uuid" => canonical_uuid_literal(name, value).map(|_| ()),
        "is_decimal" => canonical_decimal_literal(name, value).map(|_| ()),
        "is_binary_hex" => canonical_binary_hex_literal(name, value).map(|_| ()),
        "is_json" => canonical_json_literal(name, value).map(|_| ()),
        "is_array" => validate_array_filter_value(name, value, filter),
        "to_json" => Ok(()),
        "is_variant" => Err(unsupported_variant_filter_error(name)),
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
        "array" => validator.replacen("array", "is_array", 1),
        "date" => validator.replacen("date", "is_date", 1),
        "time" => validator.replacen("time", "is_time", 1),
        "timestamp" => validator.replacen("timestamp", "is_timestamp", 1),
        "uuid" => validator.replacen("uuid", "is_uuid", 1),
        "decimal" => validator.replacen("decimal", "is_decimal", 1),
        "binary_hex" => validator.replacen("binary_hex", "is_binary_hex", 1),
        "json" => validator.replacen("json", "is_json", 1),
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

fn validate_array_filter_contract(name: &str, filter: &str) -> Result<(), ApiError> {
    let args = parse_filter_args(filter)?;
    let Some(element) = args.get("element") else {
        return Err(ApiError::bad_request(format!(
            "parameter `{name}` uses is_array without required element argument"
        )));
    };
    if args.len() != 1 {
        return Err(ApiError::bad_request(format!(
            "parameter `{name}` uses unsupported is_array arguments"
        )));
    }
    match element.as_str() {
        "string" | "integer" | "number" | "boolean" | "date" | "time" | "timestamp" | "uuid"
        | "decimal" | "binary_hex" | "json" => Ok(()),
        other => Err(ApiError::bad_request(format!(
            "parameter `{name}` uses unsupported is_array element `{other}`"
        ))),
    }
}

fn validate_array_filter_value(name: &str, value: &Value, filter: &str) -> Result<(), ApiError> {
    query_array_bind_value_for_json_value(name, value, filter).map(|_| ())
}

fn query_array_bind_value_for_json_value(
    name: &str,
    value: &Value,
    filter: &str,
) -> Result<QueryBindValue, ApiError> {
    validate_array_filter_contract(name, filter)?;
    let args = parse_filter_args(filter)?;
    let element = args
        .get("element")
        .expect("validated is_array filter must have element argument");
    let values = value
        .as_array()
        .ok_or_else(|| parameter_filter_error(name, "is_array"))?;
    match element.as_str() {
        "string" => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(ToString::to_string)
                    .ok_or_else(|| parameter_filter_error(name, "is_array(element=string)"))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(QueryBindValue::Utf8Array),
        "integer" => values
            .iter()
            .map(|value| {
                value
                    .as_i64()
                    .or_else(|| value.as_str().and_then(|value| value.parse::<i64>().ok()))
                    .ok_or_else(|| parameter_filter_error(name, "is_array(element=integer)"))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(QueryBindValue::Int64Array),
        "number" => values
            .iter()
            .map(|value| {
                value
                    .as_f64()
                    .or_else(|| value.as_str().and_then(|value| value.parse::<f64>().ok()))
                    .filter(|value| value.is_finite())
                    .ok_or_else(|| parameter_filter_error(name, "is_array(element=number)"))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(QueryBindValue::Float64Array),
        "boolean" => values
            .iter()
            .map(|value| {
                value
                    .as_bool()
                    .or_else(|| value.as_str().and_then(parse_bool))
                    .ok_or_else(|| parameter_filter_error(name, "is_array(element=boolean)"))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(QueryBindValue::BooleanArray),
        "date" => values
            .iter()
            .map(|value| canonical_date_literal(name, value))
            .collect::<Result<Vec<_>, _>>()
            .map(QueryBindValue::DateArray),
        "time" => values
            .iter()
            .map(|value| canonical_time_literal(name, value))
            .collect::<Result<Vec<_>, _>>()
            .map(QueryBindValue::TimeArray),
        "timestamp" => values
            .iter()
            .map(|value| canonical_timestamp_literal(name, value))
            .collect::<Result<Vec<_>, _>>()
            .map(QueryBindValue::TimestampArray),
        "uuid" => values
            .iter()
            .map(|value| canonical_uuid_literal(name, value))
            .collect::<Result<Vec<_>, _>>()
            .map(QueryBindValue::UuidArray),
        "decimal" => values
            .iter()
            .map(|value| canonical_decimal_literal(name, value))
            .collect::<Result<Vec<_>, _>>()
            .map(QueryBindValue::DecimalArray),
        "binary_hex" => values
            .iter()
            .map(|value| canonical_binary_hex_literal(name, value))
            .collect::<Result<Vec<_>, _>>()
            .map(QueryBindValue::BinaryArray),
        "json" => values
            .iter()
            .map(|value| canonical_json_literal(name, value))
            .collect::<Result<Vec<_>, _>>()
            .map(QueryBindValue::JsonArray),
        _ => unreachable!("validated is_array element type"),
    }
}

fn typed_feldera_literal_bind_value_for_json_value(
    name: &str,
    value: &Value,
    filters: &[String],
) -> Result<Option<QueryBindValue>, ApiError> {
    for filter in filters {
        match filter_name(filter) {
            "is_date" => {
                return canonical_date_literal(name, value)
                    .map(QueryBindValue::Date)
                    .map(Some)
            }
            "is_time" => {
                return canonical_time_literal(name, value)
                    .map(QueryBindValue::Time)
                    .map(Some)
            }
            "is_timestamp" => {
                return canonical_timestamp_literal(name, value)
                    .map(QueryBindValue::Timestamp)
                    .map(Some);
            }
            "is_uuid" => {
                return canonical_uuid_literal(name, value)
                    .map(QueryBindValue::Uuid)
                    .map(Some)
            }
            "is_decimal" => {
                return canonical_decimal_literal(name, value)
                    .map(QueryBindValue::Decimal)
                    .map(Some);
            }
            "is_binary_hex" => {
                return canonical_binary_hex_literal(name, value)
                    .map(QueryBindValue::Binary)
                    .map(Some);
            }
            "is_json" => {
                return canonical_json_literal(name, value)
                    .map(QueryBindValue::Json)
                    .map(Some);
            }
            _ => {}
        }
    }
    Ok(None)
}

fn canonical_date_literal(name: &str, value: &Value) -> Result<String, ApiError> {
    let raw = value
        .as_str()
        .ok_or_else(|| parameter_filter_error(name, "is_date"))?
        .trim();
    if raw.len() != 10 {
        return Err(parameter_filter_error(name, "is_date"));
    }
    parse_date_days(raw).map_err(|_| parameter_filter_error(name, "is_date"))?;
    Ok(raw.to_string())
}

fn canonical_time_literal(name: &str, value: &Value) -> Result<String, ApiError> {
    let raw = value
        .as_str()
        .ok_or_else(|| parameter_filter_error(name, "is_time"))?
        .trim();
    parse_time_nanos(raw).map_err(|_| parameter_filter_error(name, "is_time"))?;
    Ok(raw.to_string())
}

fn canonical_timestamp_literal(name: &str, value: &Value) -> Result<String, ApiError> {
    let raw = value
        .as_str()
        .ok_or_else(|| parameter_filter_error(name, "is_timestamp"))?
        .trim();
    let (date, time) = raw
        .split_once('T')
        .or_else(|| raw.split_once(' '))
        .ok_or_else(|| parameter_filter_error(name, "is_timestamp"))?;
    if time.ends_with('Z') || time.rfind('+').is_some() || time.rfind('-').is_some() {
        return Err(parameter_filter_error(name, "is_timestamp"));
    }
    canonical_date_literal(name, &Value::String(date.to_string()))
        .map_err(|_| parameter_filter_error(name, "is_timestamp"))?;
    canonical_time_literal(name, &Value::String(time.to_string()))
        .map_err(|_| parameter_filter_error(name, "is_timestamp"))?;
    parse_timestamp_nanos(raw).map_err(|_| parameter_filter_error(name, "is_timestamp"))?;
    Ok(format!("{date} {time}"))
}

fn canonical_uuid_literal(name: &str, value: &Value) -> Result<String, ApiError> {
    let raw = value
        .as_str()
        .ok_or_else(|| parameter_filter_error(name, "is_uuid"))?
        .trim()
        .to_ascii_lowercase();
    if raw.len() != 36 {
        return Err(parameter_filter_error(name, "is_uuid"));
    }
    for (index, byte) in raw.bytes().enumerate() {
        let expected_hyphen = matches!(index, 8 | 13 | 18 | 23);
        if expected_hyphen {
            if byte != b'-' {
                return Err(parameter_filter_error(name, "is_uuid"));
            }
        } else if !byte.is_ascii_hexdigit() {
            return Err(parameter_filter_error(name, "is_uuid"));
        }
    }
    Ok(raw)
}

fn canonical_decimal_literal(name: &str, value: &Value) -> Result<String, ApiError> {
    let raw = match value {
        Value::Number(number) => number.to_string(),
        Value::String(value) => value.trim().to_string(),
        _ => return Err(parameter_filter_error(name, "is_decimal")),
    };
    let unsigned = raw
        .strip_prefix('-')
        .or_else(|| raw.strip_prefix('+'))
        .unwrap_or(&raw);
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next();
    if parts.next().is_some()
        || integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || fraction
            .is_some_and(|part| part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return Err(parameter_filter_error(name, "is_decimal"));
    }
    Ok(raw)
}

fn canonical_binary_hex_literal(name: &str, value: &Value) -> Result<Vec<u8>, ApiError> {
    let raw = value
        .as_str()
        .ok_or_else(|| parameter_filter_error(name, "is_binary_hex"))?;
    parse_hex_binary(raw).map_err(|_| parameter_filter_error(name, "is_binary_hex"))
}

fn canonical_json_literal(name: &str, value: &Value) -> Result<String, ApiError> {
    let parsed;
    let value = if let Some(raw) = value.as_str() {
        parsed = serde_json::from_str::<Value>(raw.trim())
            .map_err(|_| parameter_filter_error(name, "is_json"))?;
        &parsed
    } else {
        value
    };
    serde_json::to_string(value)
        .map_err(|_| ApiError::bad_request(format!("parameter `{name}` must be valid JSON")))
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
        "json" => canonical_json_literal(name, value).map(QueryBindValue::Json),
        "array" => Err(ApiError::bad_request(format!(
            "parameter `{name}` with type `array` requires an is_array(element=...) SQL template filter"
        ))),
        other => Err(unsupported_request_field_type_error(name, other)),
    }
}

fn unsupported_variant_filter_error(name: &str) -> ApiError {
    ApiError::bad_request(format!(
        "parameter `{name}` uses unsupported SQL template filter `is_variant`: Feldera pipeline-manager /query does not support request-time VARIANT bind literals; use `is_json` for canonical JSON text parameters or compute VARIANT inside a Feldera view"
    ))
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
    if value.is_null() {
        if response_column_type_supported(&column.r#type) {
            return Ok(Value::Null);
        }
        return Err(ApiError::bad_request(format!(
            "response column `{}` declares unsupported type `{}`",
            column.name, column.r#type
        )));
    }
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
        "date" => canonical_date_literal(&column.name, &value).map(Value::String),
        "time" => canonical_time_literal(&column.name, &value).map(Value::String),
        "timestamp" => canonical_timestamp_literal(&column.name, &value).map(Value::String),
        "uuid" => canonical_uuid_literal(&column.name, &value).map(Value::String),
        "decimal" => canonical_decimal_literal(&column.name, &value).map(Value::String),
        "binary_hex" => canonical_binary_hex_literal(&column.name, &value)
            .map(|bytes| Value::String(format_hex_binary(&bytes))),
        "array" => {
            let parsed = parse_json_encoded_source_value(&value);
            if parsed.is_array() {
                Ok(parsed)
            } else {
                Err(ApiError::bad_request(format!(
                    "response column `{}` from `{}` must be an array",
                    column.name, column.source
                )))
            }
        }
        "object" => {
            let parsed = parse_json_encoded_source_value(&value);
            if parsed.is_object() {
                Ok(parsed)
            } else {
                Err(ApiError::bad_request(format!(
                    "response column `{}` from `{}` must be an object",
                    column.name, column.source
                )))
            }
        }
        "json" => Ok(parse_json_encoded_source_value(&value)),
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
            row_properties.insert(
                column.name.clone(),
                openapi_response_column_schema(&column.r#type),
            );
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

fn openapi_response_column_schema(type_name: &str) -> Value {
    let mut schema = openapi_scalar_schema(type_name);
    if let Some(object) = schema.as_object_mut() {
        object.insert("nullable".to_string(), Value::Bool(true));
    }
    schema
}

fn openapi_scalar_schema(type_name: &str) -> Value {
    match type_name {
        "string" => json!({ "type": "string" }),
        "int64" => json!({ "type": "integer", "format": "int64" }),
        "integer" => json!({ "type": "integer" }),
        "float64" => json!({ "type": "number", "format": "double" }),
        "number" => json!({ "type": "number" }),
        "bool" | "boolean" => json!({ "type": "boolean" }),
        "array" => json!({ "type": "array", "items": {} }),
        "object" => json!({ "type": "object" }),
        "date" => json!({ "type": "string", "format": "date" }),
        "time" => json!({ "type": "string", "format": "time" }),
        "timestamp" => json!({ "type": "string", "format": "date-time" }),
        "uuid" => json!({ "type": "string", "format": "uuid" }),
        "decimal" => json!({ "type": "string", "format": "decimal" }),
        "binary_hex" => json!({ "type": "string", "pattern": "^(0[xX])?[0-9a-fA-F]*$" }),
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
        output_relation_id: request.output_relation_id.clone(),
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
    active: &ActiveMaterializedView,
    outcome: Option<&str>,
) -> Result<ViewResponse, ApiError> {
    view_response(
        &active.spec,
        active.spec_hash.clone(),
        active.execution_mode.clone(),
        active.lifecycle.clone(),
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

    let (query_enabled, disabled_reason) = view_query_availability(&execution_mode, &lifecycle);
    let compile_job_id = if execution_mode == MaterializedViewExecutionMode::FelderaCompilePending {
        Some(compile_job_id_for_spec(spec)?)
    } else {
        None
    };

    Ok(ViewResponse {
        view_id: spec.view_id.clone(),
        url_path: api.url_path.clone(),
        output_relation_id: api.output_relation_id.clone(),
        input_relation_id: input.relation_id.clone(),
        input_relation_version: input.relation_version.clone(),
        spec_hash,
        source_kind: spec.source_kind.clone(),
        execution_mode,
        lifecycle,
        query_enabled,
        disabled_reason,
        compile_job_id,
        query_endpoint: api
            .url_path
            .as_deref()
            .map(|path| format!("/v1/api/{}", normalize_api_path(path)))
            .unwrap_or_else(|| format!("/v1/views/{}/query", api_path_segment(&spec.view_id))),
        output_query_endpoints: spec
            .output_relations
            .iter()
            .map(|schema| {
                format!(
                    "/v1/views/{}/outputs/{}/query",
                    api_path_segment(&spec.view_id),
                    api_path_segment(&schema.relation_id)
                )
            })
            .collect(),
        output_relations: spec.output_relations.clone(),
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

fn compile_request_hash_for_spec(spec: &StandingViewSpec) -> Result<String, ApiError> {
    feldera_compile_request_hash(
        &FelderaCompileRequestV1::infer_output_from_standing_view_spec(spec),
    )
    .map_err(ApiError::bad_request)
}

fn compile_job_id_for_spec(spec: &StandingViewSpec) -> Result<String, ApiError> {
    Ok(view_compile_deploy_compile_request_job_id(
        &spec.view_id,
        &compile_request_hash_for_spec(spec)?,
    ))
}

fn lifecycle_for_create_view_execution(
    execution_mode: &MaterializedViewExecutionMode,
) -> MaterializedViewLifecycleStatus {
    match execution_mode {
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
) -> (bool, Option<String>) {
    match execution_mode {
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
    state: &ApiState,
    request: &CreateViewRequest,
    catalogs: &[VelorixRelationCatalogV1],
    artifact: Option<&FelderaCompileArtifactMetadata>,
) -> Result<StandingViewSpec, ApiError> {
    let input_relations = catalogs
        .iter()
        .map(|catalog| catalog_input_relation_schema(catalog).map_err(ApiError::bad_request))
        .collect::<Result<Vec<_>, _>>()?;
    let input = input_relations
        .first()
        .ok_or_else(|| ApiError::bad_request("view has no input relation"))?;
    validate_create_view_sql_source_contract(request)?;
    let source_kind = resolved_sql_source_kind_for_create_view(request);
    let output_relations = if let Some(artifact_request) = &request.artifact {
        artifact_request.metadata.output_schemas.clone()
    } else if let Some(artifact) = artifact {
        artifact.output_schemas.clone()
    } else if source_kind == SqlSourceKind::FelderaProgram
        && !request.output_relation_ids.is_empty()
    {
        generic_materialized_view_output_schemas_for_ids(
            &request.output_relation_ids,
            input.schema_fingerprint.as_str(),
        )?
    } else if source_kind == SqlSourceKind::StandingView {
        state
            .generated_package_output_schemas_for_view_request(
                request.view_id.as_str(),
                request.sql.as_str(),
                catalogs,
                input.schema_fingerprint.as_str(),
            )?
            .unwrap_or_else(|| {
                vec![generic_materialized_view_output_schema(
                    request.view_id.as_str(),
                    input.schema_fingerprint.as_str(),
                )]
            })
    } else {
        vec![generic_materialized_view_output_schema(
            request.view_id.as_str(),
            input.schema_fingerprint.as_str(),
        )]
    };
    let multi_output = output_relations.len() > 1;
    Ok(StandingViewSpec {
        view_id: request.view_id.clone(),
        sql: request.sql.clone(),
        dialect: SqlDialect::FelderaSql,
        source_kind,
        rust_extension: FelderaRustExtensionV1 {
            udf_rust: request.udf_rust.clone(),
            udf_toml: request.udf_toml.clone(),
        },
        input_relations,
        output_relations,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: catalogs.len() > 1,
            multi_output,
        },
    })
}

fn validate_create_view_sql_source_contract(request: &CreateViewRequest) -> Result<(), ApiError> {
    let source_kind = resolved_sql_source_kind_for_create_view(request);
    let mut output_ids = BTreeSet::new();
    for output_id in &request.output_relation_ids {
        let trimmed = output_id.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request(
                "output_relation_ids must not contain blank output ids",
            ));
        }
        if !output_ids.insert(trimmed) {
            return Err(ApiError::bad_request(format!(
                "duplicate output_relation_ids entry `{trimmed}`"
            )));
        }
    }
    if source_kind == SqlSourceKind::StandingView && !request.output_relation_ids.is_empty() {
        return Err(ApiError::bad_request(
            "output_relation_ids are only supported when source_kind is `feldera_program`",
        ));
    }
    Ok(())
}

fn generic_materialized_view_output_schemas_for_ids(
    output_relation_ids: &[String],
    input_schema_fingerprint: &str,
) -> Result<Vec<RelationSchema>, ApiError> {
    output_relation_ids
        .iter()
        .map(|output_id| {
            let output_id = output_id.trim();
            if output_id.is_empty() {
                return Err(ApiError::bad_request(
                    "output_relation_ids must not contain blank output ids",
                ));
            }
            let schema_fingerprint = feldera_artifact_bytes_hash(
                format!("velorix-compile-pending-output:{input_schema_fingerprint}:{output_id}")
                    .as_bytes(),
            );
            Ok(generic_materialized_view_output_schema(
                output_id,
                &schema_fingerprint,
            ))
        })
        .collect()
}

fn single_key_sum_count_output_schema(
    view_id: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<RelationSchema, ApiError> {
    let [primary_key_id] = catalog.relation_schema.primary_key_column_ids.as_slice() else {
        return Err(ApiError::bad_request(
            "single-key sum/count view requires exactly one primary key column",
        ));
    };
    let key_column = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| &column.column_id == primary_key_id)
        .ok_or_else(|| ApiError::bad_request("primary key column is missing from catalog"))?;
    let key_type = sql_type_from_catalog_column(key_column)?;
    let sum_type = generic_single_key_sum_count_sum_type(catalog)?;
    Ok(RelationSchema {
        relation_id: view_id.to_string(),
        relation_name: view_id.to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        columns: vec![
            ColumnSchema {
                name: key_column.name.clone(),
                data_type: key_type,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".to_string(),
                data_type: sum_type,
                nullable: false,
            },
            ColumnSchema {
                name: "count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec![key_column.name.clone()],
    })
}

fn join_sum_count_output_schema(
    view_id: &str,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<RelationSchema, ApiError> {
    let [left_catalog, right_catalog] = catalogs else {
        return Err(ApiError::bad_request(
            "join sum/count view requires exactly two input relations",
        ));
    };
    let [right_primary_key_id] = right_catalog
        .relation_schema
        .primary_key_column_ids
        .as_slice()
    else {
        return Err(ApiError::bad_request(
            "join sum/count view requires right input to have exactly one primary key column",
        ));
    };
    let key_column = right_catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| &column.column_id == right_primary_key_id)
        .ok_or_else(|| ApiError::bad_request("right primary key column is missing from catalog"))?;
    let key_type = sql_type_from_catalog_column(key_column)?;
    let sum_type = generic_single_key_sum_count_sum_type(left_catalog)?;
    let input_fingerprint = feldera_artifact_bytes_hash(
        serde_json::to_vec(&[
            left_catalog.schema_fingerprint.as_str(),
            right_catalog.schema_fingerprint.as_str(),
        ])
        .map_err(|source| ApiError::internal(source.to_string()))?
        .as_slice(),
    );
    Ok(RelationSchema {
        relation_id: view_id.to_string(),
        relation_name: view_id.to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: input_fingerprint,
        columns: vec![
            ColumnSchema {
                name: key_column.name.clone(),
                data_type: key_type,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".to_string(),
                data_type: sum_type,
                nullable: false,
            },
            ColumnSchema {
                name: "count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec![key_column.name.clone()],
    })
}

fn validate_join_plan_catalog_order(
    plan: &SupportedDbspJoinViewPlan,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<(), ApiError> {
    let [left, right] = catalogs else {
        return Err(ApiError::bad_request(
            "join sum/count view requires exactly two input relations",
        ));
    };
    if left.relation_schema.relation_id == plan.left_input_relation_id
        && right.relation_schema.relation_id == plan.right_input_relation_id
    {
        Ok(())
    } else {
        Err(ApiError::bad_request(
            "input_relations must be ordered to match the SQL JOIN left and right inputs",
        ))
    }
}

fn validate_generic_single_key_sum_count_runtime_scope(
    catalog: &VelorixRelationCatalogV1,
) -> Result<(), ApiError> {
    generic_single_key_sum_count_sum_type(catalog).map(|_| ())
}

fn generic_single_key_sum_count_sum_type(
    catalog: &VelorixRelationCatalogV1,
) -> Result<SqlDataType, ApiError> {
    let mut value_columns = catalog
        .relation_schema
        .columns
        .iter()
        .filter(|column| column.semantic_role == RelationSemanticRoleV1::Value);
    let value = value_columns.next().ok_or_else(|| {
        ApiError::bad_request("single-key sum/count view requires one value column")
    })?;
    if value_columns.next().is_some() {
        return Err(ApiError::bad_request(
            "single-key sum/count view supports exactly one value column",
        ));
    }
    match &value.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 => Ok(SqlDataType::Int64),
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => Ok(SqlDataType::Decimal {
            precision: *precision,
            scale: *scale,
        }),
        _ => Err(ApiError::bad_request(format!(
            "single-key sum/count generated runtime value column `{}` must be Int64 or Decimal128",
            value.name
        ))),
    }
}

fn sql_type_from_catalog_column(column: &RelationColumnV1) -> Result<SqlDataType, ApiError> {
    sql_type_from_logical_type(&column.logical_type)
}

fn sql_type_from_logical_type(
    logical_type: &VelorixLogicalTypeV1,
) -> Result<SqlDataType, ApiError> {
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
        VelorixLogicalTypeV1::Char { length } => SqlDataType::Char { length: *length },
        VelorixLogicalTypeV1::Utf8 => SqlDataType::Utf8,
        VelorixLogicalTypeV1::Binary { length } => SqlDataType::Binary { length: *length },
        VelorixLogicalTypeV1::Varbinary => SqlDataType::Varbinary,
        VelorixLogicalTypeV1::Json => SqlDataType::Json,
        VelorixLogicalTypeV1::Date => SqlDataType::Date,
        VelorixLogicalTypeV1::Time => SqlDataType::Time,
        VelorixLogicalTypeV1::Timestamp { timezone } => SqlDataType::Timestamp {
            timezone: timezone.clone(),
        },
        VelorixLogicalTypeV1::Uuid => SqlDataType::Uuid,
        VelorixLogicalTypeV1::Decimal { precision, scale } => SqlDataType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        VelorixLogicalTypeV1::Array { element_type } => SqlDataType::Array {
            element_type: Box::new(sql_type_from_logical_type(element_type)?),
        },
        VelorixLogicalTypeV1::Struct { fields } => SqlDataType::Struct {
            fields: fields
                .iter()
                .map(|field| {
                    Ok(SqlStructField {
                        name: field.name.clone(),
                        data_type: sql_type_from_logical_type(&field.logical_type)?,
                        nullable: field.nullable,
                    })
                })
                .collect::<Result<Vec<_>, ApiError>>()?,
        },
        VelorixLogicalTypeV1::Map {
            key_type,
            value_type,
        } => SqlDataType::Map {
            key_type: Box::new(sql_type_from_logical_type(key_type)?),
            value_type: Box::new(sql_type_from_logical_type(value_type)?),
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

fn normalize_ingest_operation_envelopes(
    catalog: &VelorixRelationCatalogV1,
    rows: &[Value],
) -> Result<Vec<Value>, ApiError> {
    let weight_column = relation_weight_column_name(catalog)?;
    let mut normalized = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let Some(object) = row.as_object() else {
            return Err(ApiError::bad_request(
                "each ingest row must be a JSON object",
            ));
        };
        let is_operation_envelope = object.contains_key("operation")
            && (object.contains_key("row")
                || object.contains_key("before")
                || object.contains_key("after"));
        if !is_operation_envelope {
            normalized.push(row.clone());
            continue;
        }

        let operation = object
            .get("operation")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "ingest operation envelope at row {index} requires string operation"
                ))
            })?
            .to_ascii_lowercase();
        match operation.as_str() {
            "insert" => {
                require_relation_operation(catalog, RelationOperationV1::Insert, index)?;
                let payload = envelope_payload(object, "row", index)
                    .or_else(|_| envelope_payload(object, "after", index))?;
                normalized.push(row_with_signed_weight(payload, weight_column, 1, index)?);
            }
            "delete" => {
                require_relation_operation(catalog, RelationOperationV1::Delete, index)?;
                let payload = envelope_payload(object, "row", index)
                    .or_else(|_| envelope_payload(object, "before", index))?;
                normalized.push(row_with_signed_weight(payload, weight_column, -1, index)?);
            }
            "update" => {
                require_relation_operation(catalog, RelationOperationV1::Update, index)?;
                let before = envelope_payload(object, "before", index)?;
                let after = envelope_payload(object, "after", index)?;
                normalized.push(row_with_signed_weight(before, weight_column, -1, index)?);
                normalized.push(row_with_signed_weight(after, weight_column, 1, index)?);
            }
            "upsert" => {
                require_relation_operation(catalog, RelationOperationV1::Upsert, index)?;
                if let Some(before) = object.get("before") {
                    normalized.push(row_with_signed_weight(before, weight_column, -1, index)?);
                }
                let payload = envelope_payload(object, "row", index)
                    .or_else(|_| envelope_payload(object, "after", index))?;
                normalized.push(row_with_signed_weight(payload, weight_column, 1, index)?);
            }
            other => {
                return Err(ApiError::bad_request(format!(
                    "unsupported ingest operation `{other}` at row {index}"
                )));
            }
        }
    }
    Ok(normalized)
}

fn relation_weight_column_name(catalog: &VelorixRelationCatalogV1) -> Result<&str, ApiError> {
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == catalog.relation_schema.weight_column_id)
        .map(|column| column.name.as_str())
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "relation `{}` weight column `{}` is missing",
                catalog.relation_schema.relation_id, catalog.relation_schema.weight_column_id
            ))
        })
}

fn require_relation_operation(
    catalog: &VelorixRelationCatalogV1,
    operation: RelationOperationV1,
    row_index: usize,
) -> Result<(), ApiError> {
    if catalog
        .relation_schema
        .allowed_operations
        .iter()
        .any(|allowed| allowed == &operation)
    {
        Ok(())
    } else {
        Err(ApiError::bad_request(format!(
            "relation `{}` does not allow {operation:?} operation at row {row_index}",
            catalog.relation_schema.relation_id
        )))
    }
}

fn envelope_payload<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
    row_index: usize,
) -> Result<&'a Value, ApiError> {
    object.get(field).ok_or_else(|| {
        ApiError::bad_request(format!(
            "ingest operation envelope at row {row_index} requires `{field}`"
        ))
    })
}

fn row_with_signed_weight(
    payload: &Value,
    weight_column: &str,
    weight: i64,
    row_index: usize,
) -> Result<Value, ApiError> {
    let mut object = payload.as_object().cloned().ok_or_else(|| {
        ApiError::bad_request(format!(
            "ingest operation envelope payload at row {row_index} must be a JSON object"
        ))
    })?;
    if object.contains_key(weight_column) {
        return Err(ApiError::bad_request(format!(
            "ingest operation envelope payload at row {row_index} must not include weight column `{weight_column}`"
        )));
    }
    object.insert(weight_column.to_string(), json!(weight));
    Ok(Value::Object(object))
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
        .validate_feldera_ingest_adapter_scope()
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
        ArrowPhysicalTypeV1::Int8 => Ok(Arc::new(Int8Array::from(collect_column_values(
            column,
            rows,
            json_i8_value,
        )?))),
        ArrowPhysicalTypeV1::Int16 => Ok(Arc::new(Int16Array::from(collect_column_values(
            column,
            rows,
            json_i16_value,
        )?))),
        ArrowPhysicalTypeV1::Int32 => Ok(Arc::new(Int32Array::from(collect_column_values(
            column,
            rows,
            json_i32_value,
        )?))),
        ArrowPhysicalTypeV1::Int64 => Ok(Arc::new(Int64Array::from(collect_column_values(
            column,
            rows,
            json_i64_value,
        )?))),
        ArrowPhysicalTypeV1::UInt8 => Ok(Arc::new(UInt8Array::from(collect_column_values(
            column,
            rows,
            json_u8_value,
        )?))),
        ArrowPhysicalTypeV1::UInt16 => Ok(Arc::new(UInt16Array::from(collect_column_values(
            column,
            rows,
            json_u16_value,
        )?))),
        ArrowPhysicalTypeV1::UInt32 => Ok(Arc::new(UInt32Array::from(collect_column_values(
            column,
            rows,
            json_u32_value,
        )?))),
        ArrowPhysicalTypeV1::UInt64 => Ok(Arc::new(UInt64Array::from(collect_column_values(
            column,
            rows,
            json_u64_value,
        )?))),
        ArrowPhysicalTypeV1::Float32 => Ok(Arc::new(Float32Array::from(collect_column_values(
            column,
            rows,
            json_f32_value,
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
        ArrowPhysicalTypeV1::Binary => {
            let values = collect_column_values(column, rows, json_binary_value)?;
            Ok(Arc::new(BinaryArray::from_iter(
                values.iter().map(|value| value.as_deref()),
            )))
        }
        ArrowPhysicalTypeV1::Date32 => Ok(Arc::new(Date32Array::from(collect_column_values(
            column,
            rows,
            json_date32_value,
        )?))),
        ArrowPhysicalTypeV1::Time64Nanosecond => Ok(Arc::new(Time64NanosecondArray::from(
            collect_column_values(column, rows, json_time64_nanos_value)?,
        ))),
        ArrowPhysicalTypeV1::TimestampNanosecond { timezone } => {
            let array = TimestampNanosecondArray::from(collect_column_values(
                column,
                rows,
                json_timestamp_nanos_value,
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
        ArrowPhysicalTypeV1::List { .. }
        | ArrowPhysicalTypeV1::Struct { .. }
        | ArrowPhysicalTypeV1::Map { .. } => {
            relation_json_reader_column_to_arrow_array(column, rows)
        }
    }
}

fn relation_json_reader_column_to_arrow_array(
    column: &RelationColumnV1,
    rows: &[Value],
) -> Result<ArrayRef, ApiError> {
    let data_type = sql_type_from_catalog_column(column)?;
    let schema = ColumnSchema {
        name: column.name.clone(),
        data_type,
        nullable: column.nullable,
    };
    feldera_json_reader_column_to_arrow_array(&schema, rows).map_err(ApiError::bad_request)
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

fn json_i8_value(column: &RelationColumnV1, value: &Value) -> Result<i8, ApiError> {
    let value = json_i64_value(column, value)?;
    i8::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("row.{} is outside Int8 range", column.name)))
}

fn json_i16_value(column: &RelationColumnV1, value: &Value) -> Result<i16, ApiError> {
    let value = json_i64_value(column, value)?;
    i16::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("row.{} is outside Int16 range", column.name)))
}

fn json_i32_value(column: &RelationColumnV1, value: &Value) -> Result<i32, ApiError> {
    let value = json_i64_value(column, value)?;
    i32::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("row.{} is outside Int32 range", column.name)))
}

fn json_u64_value(column: &RelationColumnV1, value: &Value) -> Result<u64, ApiError> {
    value.as_u64().ok_or_else(|| {
        ApiError::bad_request(format!("row.{} must be an unsigned integer", column.name))
    })
}

fn json_u8_value(column: &RelationColumnV1, value: &Value) -> Result<u8, ApiError> {
    let value = json_u64_value(column, value)?;
    u8::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("row.{} is outside UInt8 range", column.name)))
}

fn json_u16_value(column: &RelationColumnV1, value: &Value) -> Result<u16, ApiError> {
    let value = json_u64_value(column, value)?;
    u16::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("row.{} is outside UInt16 range", column.name)))
}

fn json_u32_value(column: &RelationColumnV1, value: &Value) -> Result<u32, ApiError> {
    let value = json_u64_value(column, value)?;
    u32::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("row.{} is outside UInt32 range", column.name)))
}

fn json_f32_value(column: &RelationColumnV1, value: &Value) -> Result<f32, ApiError> {
    let value = json_f64_value(column, value)?;
    let value = value as f32;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(ApiError::bad_request(format!(
            "row.{} is outside finite Float32 range",
            column.name
        )))
    }
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

fn json_binary_value(column: &RelationColumnV1, value: &Value) -> Result<Vec<u8>, ApiError> {
    let raw = value.as_str().ok_or_else(|| {
        ApiError::bad_request(format!("row.{} must be a hex string", column.name))
    })?;
    let bytes = parse_hex_binary(raw).map_err(|reason| {
        ApiError::bad_request(format!("row.{} invalid binary: {reason}", column.name))
    })?;
    validate_fixed_binary_length(
        "row",
        column.name.as_str(),
        &column.logical_type,
        bytes.len(),
    )?;
    Ok(bytes)
}

fn json_time64_nanos_value(column: &RelationColumnV1, value: &Value) -> Result<i64, ApiError> {
    match value {
        Value::Number(number) => number.as_i64().ok_or_else(|| {
            ApiError::bad_request(format!("row.{} must be a time64 integer", column.name))
        }),
        Value::String(raw) => parse_time_nanos(raw).map_err(|reason| {
            ApiError::bad_request(format!("row.{} invalid time: {reason}", column.name))
        }),
        _ => Err(ApiError::bad_request(format!(
            "row.{} must be a time string or time64 integer",
            column.name
        ))),
    }
}

fn json_date32_value(column: &RelationColumnV1, value: &Value) -> Result<i32, ApiError> {
    match value {
        Value::Number(_) => json_i32_value(column, value),
        Value::String(raw) => {
            let days = parse_date_days(raw).map_err(|reason| {
                ApiError::bad_request(format!("row.{} invalid date: {reason}", column.name))
            })?;
            i32::try_from(days).map_err(|_| {
                ApiError::bad_request(format!("row.{} date is outside Date32 range", column.name))
            })
        }
        _ => Err(ApiError::bad_request(format!(
            "row.{} must be a date string or date32 integer",
            column.name
        ))),
    }
}

fn json_timestamp_nanos_value(column: &RelationColumnV1, value: &Value) -> Result<i64, ApiError> {
    match value {
        Value::Number(_) => json_i64_value(column, value),
        Value::String(raw) => parse_timestamp_nanos(raw).map_err(|reason| {
            ApiError::bad_request(format!("row.{} invalid timestamp: {reason}", column.name))
        }),
        _ => Err(ApiError::bad_request(format!(
            "row.{} must be a timestamp string or timestamp nanos integer",
            column.name
        ))),
    }
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
        Value::Number(number) => number.to_string(),
        Value::String(value) => value.clone(),
        _ => {
            return Err(ApiError::bad_request(format!(
                "row.{} must be a decimal string or number",
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

fn record_batches_to_json_rows_for_feldera_schema(
    output_schema: &RelationSchema,
    batches: &[RecordBatch],
) -> Result<Vec<Value>, ApiError> {
    let json_columns = output_schema
        .columns
        .iter()
        .filter(|column| matches!(column.data_type, SqlDataType::Json))
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    let mut rows = record_batches_to_json_rows(batches)?;
    if json_columns.is_empty() {
        return Ok(rows);
    }
    for row in &mut rows {
        let object = row.as_object_mut().ok_or_else(|| {
            ApiError::internal("record batch JSON row must be an object before schema post-process")
        })?;
        for column in &json_columns {
            let Some(value) = object.get_mut(*column) else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            let raw = value.as_str().ok_or_else(|| {
                ApiError::internal(format!(
                    "Feldera JSON output column `{column}` must be stored as canonical JSON text"
                ))
            })?;
            *value = serde_json::from_str(raw).map_err(|error| {
                ApiError::internal(format!(
                    "Feldera JSON output column `{column}` contains invalid canonical JSON: {error}"
                ))
            })?;
        }
    }
    Ok(rows)
}

#[cfg(test)]
fn record_batches_to_feldera_ingress_json_rows(
    batches: &[RecordBatch],
) -> Result<Vec<Value>, ApiError> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row_index in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();
            for (column_index, field) in schema.fields().iter().enumerate() {
                row.insert(
                    field.name().clone(),
                    arrow_value_to_feldera_ingress_json(batch.column(column_index), row_index)?,
                );
            }
            rows.push(Value::Object(row));
        }
    }
    Ok(rows)
}

fn record_batches_to_feldera_ingress_json_rows_for_catalog(
    catalog: &VelorixRelationCatalogV1,
    batches: &[RecordBatch],
) -> Result<Vec<Value>, ApiError> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row_index in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();
            for column in &catalog.relation_schema.columns {
                let column_index = schema.index_of(column.name.as_str()).map_err(|error| {
                    ApiError::bad_request(format!(
                        "Feldera ingress batch for relation `{}` is missing column `{}`: {error}",
                        catalog.relation_schema.relation_id, column.name
                    ))
                })?;
                row.insert(
                    column.name.clone(),
                    arrow_value_to_feldera_ingress_json_for_catalog_column(
                        column,
                        batch.column(column_index),
                        row_index,
                    )?,
                );
            }
            rows.push(Value::Object(row));
        }
    }
    Ok(rows)
}

fn arrow_value_to_feldera_ingress_json_for_catalog_column(
    column: &RelationColumnV1,
    arrow_column: &ArrayRef,
    row_index: usize,
) -> Result<Value, ApiError> {
    if arrow_column.is_null(row_index) {
        return Ok(Value::Null);
    }
    if matches!(column.logical_type, VelorixLogicalTypeV1::Json)
        && matches!(column.physical_arrow_type, ArrowPhysicalTypeV1::JsonUtf8)
    {
        let json_text = arrow_column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "Feldera JSON ingress column `{}` must be Arrow Utf8",
                    column.name
                ))
            })?
            .value(row_index);
        return serde_json::from_str(json_text).map_err(|error| {
            ApiError::bad_request(format!(
                "Feldera JSON ingress column `{}` contains invalid JSON: {error}",
                column.name
            ))
        });
    }
    if matches!(column.logical_type, VelorixLogicalTypeV1::Binary { .. })
        && matches!(arrow_column.data_type(), DataType::Binary)
    {
        let values = arrow_column
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| ApiError::internal("invalid Binary Arrow column"))?
            .value(row_index);
        validate_fixed_binary_length(
            "Feldera ingress column",
            column.name.as_str(),
            &column.logical_type,
            values.len(),
        )?;
    }
    arrow_value_to_feldera_ingress_json(arrow_column, row_index)
}

fn validate_fixed_binary_length(
    scope: &str,
    column_name: &str,
    logical_type: &VelorixLogicalTypeV1,
    actual_len: usize,
) -> Result<(), ApiError> {
    let VelorixLogicalTypeV1::Binary { length } = logical_type else {
        return Ok(());
    };
    if actual_len == usize::try_from(*length).unwrap_or(usize::MAX) {
        return Ok(());
    }
    let field = if scope == "row" {
        format!("{scope}.{column_name}")
    } else {
        format!("{scope} `{column_name}`")
    };
    Err(ApiError::bad_request(format!(
        "{field} must contain exactly {length} bytes, got {actual_len}"
    )))
}

fn arrow_value_to_feldera_ingress_json(
    column: &ArrayRef,
    row_index: usize,
) -> Result<Value, ApiError> {
    if column.is_null(row_index) {
        return Ok(Value::Null);
    }
    match column.data_type() {
        DataType::Binary => {
            let values = column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| ApiError::internal("invalid Binary Arrow column"))?
                .value(row_index)
                .iter()
                .copied()
                .map(Value::from)
                .collect();
            Ok(Value::Array(values))
        }
        DataType::Date32 => Ok(Value::String(format_date32_for_feldera_ingress(
            column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| ApiError::internal("invalid Date32 Arrow column"))?
                .value(row_index),
        ))),
        DataType::Time64(TimeUnit::Nanosecond) => {
            Ok(Value::String(format_time_nanos_for_feldera_ingress(
                column
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .ok_or_else(|| ApiError::internal("invalid Time64Nanosecond Arrow column"))?
                    .value(row_index),
            )?))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, timezone) => {
            if timezone.is_some() {
                return Err(ApiError::bad_request(
                    "Feldera ingress for timezone-bearing TimestampNanosecond columns is not supported",
                ));
            }
            Ok(Value::String(format_timestamp_nanos_for_feldera_ingress(
                column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| ApiError::internal("invalid TimestampNanosecond Arrow column"))?
                    .value(row_index),
            )?))
        }
        DataType::List(_) => {
            let list = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| ApiError::internal("invalid List Arrow column"))?;
            let values = list.value(row_index);
            let mut json_values = Vec::with_capacity(values.len());
            for index in 0..values.len() {
                json_values.push(arrow_value_to_feldera_ingress_json(&values, index)?);
            }
            Ok(Value::Array(json_values))
        }
        DataType::Struct(fields) => {
            let struct_array = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| ApiError::internal("invalid Struct Arrow column"))?;
            let mut object = serde_json::Map::with_capacity(fields.len());
            for (field_index, field) in fields.iter().enumerate() {
                object.insert(
                    field.name().clone(),
                    arrow_value_to_feldera_ingress_json(
                        struct_array.column(field_index),
                        row_index,
                    )?,
                );
            }
            Ok(Value::Object(object))
        }
        DataType::Map(_, _) => {
            let map = column
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| ApiError::internal("invalid Map Arrow column"))?;
            let entries = map.value(row_index);
            let keys = entries.column(0);
            let values = entries.column(1);
            let mut object = serde_json::Map::with_capacity(entries.len());
            for index in 0..entries.len() {
                let key = arrow_value_to_feldera_ingress_json(keys, index)?;
                object.insert(
                    json_value_as_object_key(key),
                    arrow_value_to_feldera_ingress_json(values, index)?,
                );
            }
            Ok(Value::Object(object))
        }
        _ => arrow_value_to_json(column, row_index),
    }
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
        DataType::Int8 => Ok(json!(column
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| ApiError::internal("invalid Int8 Arrow column"))?
            .value(row_index))),
        DataType::Int16 => Ok(json!(column
            .as_any()
            .downcast_ref::<Int16Array>()
            .ok_or_else(|| ApiError::internal("invalid Int16 Arrow column"))?
            .value(row_index))),
        DataType::Int32 => Ok(json!(column
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| ApiError::internal("invalid Int32 Arrow column"))?
            .value(row_index))),
        DataType::Int64 => Ok(json!(column
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| ApiError::internal("invalid Int64 Arrow column"))?
            .value(row_index))),
        DataType::UInt8 => Ok(json!(column
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| ApiError::internal("invalid UInt8 Arrow column"))?
            .value(row_index))),
        DataType::UInt16 => Ok(json!(column
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| ApiError::internal("invalid UInt16 Arrow column"))?
            .value(row_index))),
        DataType::UInt32 => Ok(json!(column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ApiError::internal("invalid UInt32 Arrow column"))?
            .value(row_index))),
        DataType::UInt64 => Ok(json!(column
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| ApiError::internal("invalid UInt64 Arrow column"))?
            .value(row_index))),
        DataType::Float32 => Ok(json!(column
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| ApiError::internal("invalid Float32 Arrow column"))?
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
        DataType::Decimal128(_precision, scale) => {
            let value = column
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| ApiError::internal("invalid Decimal128 Arrow column"))?
                .value(row_index);
            Ok(Value::String(format_decimal128_for_json(value, *scale)))
        }
        DataType::Date32 => Ok(json!(column
            .as_any()
            .downcast_ref::<Date32Array>()
            .ok_or_else(|| ApiError::internal("invalid Date32 Arrow column"))?
            .value(row_index))),
        DataType::Time64(TimeUnit::Nanosecond) => Ok(json!(column
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .ok_or_else(|| ApiError::internal("invalid Time64Nanosecond Arrow column"))?
            .value(row_index))),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(json!(column
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| ApiError::internal("invalid TimestampNanosecond Arrow column"))?
            .value(row_index))),
        DataType::Binary => Ok(Value::String(format_hex_binary(
            column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| ApiError::internal("invalid Binary Arrow column"))?
                .value(row_index),
        ))),
        DataType::Null => Ok(Value::Null),
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _) => {
            arrow_nested_value_to_json(column, row_index)
        }
        other => Err(ApiError::internal(format!(
            "unsupported query result Arrow type {other:?}"
        ))),
    }
}

fn arrow_nested_value_to_json(column: &ArrayRef, row_index: usize) -> Result<Value, ApiError> {
    match column.data_type() {
        DataType::List(_) => {
            let list = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| ApiError::internal("invalid List Arrow column"))?;
            let values = list.value(row_index);
            let mut json_values = Vec::with_capacity(values.len());
            for index in 0..values.len() {
                json_values.push(arrow_value_to_json(&values, index)?);
            }
            Ok(Value::Array(json_values))
        }
        DataType::Struct(fields) => {
            let struct_array = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| ApiError::internal("invalid Struct Arrow column"))?;
            let mut object = serde_json::Map::with_capacity(fields.len());
            for (field_index, field) in fields.iter().enumerate() {
                object.insert(
                    field.name().clone(),
                    arrow_value_to_json(struct_array.column(field_index), row_index)?,
                );
            }
            Ok(Value::Object(object))
        }
        DataType::Map(_, _) => {
            let map = column
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| ApiError::internal("invalid Map Arrow column"))?;
            let entries = map.value(row_index);
            let keys = entries.column(0);
            let values = entries.column(1);
            let mut object = serde_json::Map::with_capacity(entries.len());
            for index in 0..entries.len() {
                let key = arrow_value_to_json(keys, index)?;
                object.insert(
                    json_value_as_object_key(key),
                    arrow_value_to_json(values, index)?,
                );
            }
            Ok(Value::Object(object))
        }
        other => Err(ApiError::internal(format!(
            "unsupported nested query result Arrow type {other:?}"
        ))),
    }
}

fn json_value_as_object_key(value: Value) -> String {
    match value {
        Value::String(value) => value,
        other => other.to_string(),
    }
}

fn format_hex_binary(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(2 + bytes.len() * 2);
    output.push_str("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn format_date32_for_feldera_ingress(days: i32) -> String {
    let (year, month, day) = civil_from_days(i64::from(days));
    format!("{year:04}-{month:02}-{day:02}")
}

fn format_time_nanos_for_feldera_ingress(nanos: i64) -> Result<String, ApiError> {
    const DAY_NANOS: i64 = 86_400_000_000_000;
    if !(0..DAY_NANOS).contains(&nanos) {
        return Err(ApiError::internal(format!(
            "time nanos value {nanos} is outside a single day"
        )));
    }
    Ok(format_time_of_day_nanos(nanos))
}

fn format_timestamp_nanos_for_feldera_ingress(nanos: i64) -> Result<String, ApiError> {
    const DAY_NANOS: i64 = 86_400_000_000_000;
    let days = nanos.div_euclid(DAY_NANOS);
    let time_nanos = nanos.rem_euclid(DAY_NANOS);
    let (year, month, day) = civil_from_days(days);
    Ok(format!(
        "{year:04}-{month:02}-{day:02} {}",
        format_time_of_day_nanos(time_nanos)
    ))
}

fn format_time_of_day_nanos(nanos: i64) -> String {
    let seconds = nanos / 1_000_000_000;
    let fraction = nanos % 1_000_000_000;
    let hour = seconds / 3_600;
    let minute = (seconds % 3_600) / 60;
    let second = seconds % 60;
    let mut output = format!("{hour:02}:{minute:02}:{second:02}");
    if fraction != 0 {
        let mut fraction = format!("{fraction:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        output.push('.');
        output.push_str(&fraction);
    }
    output
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year, month, day)
}

fn format_decimal128_for_json(value: i128, scale: i8) -> String {
    let magnitude = value.unsigned_abs();
    let mut digits = magnitude.to_string();
    let scale = usize::try_from(scale.max(0)).expect("non-negative i8 fits usize");
    let mut decimal = if scale == 0 {
        digits
    } else if digits.len() <= scale {
        let leading_zeroes = "0".repeat(scale - digits.len());
        format!("0.{leading_zeroes}{digits}")
    } else {
        let fractional = digits.split_off(digits.len() - scale);
        format!("{digits}.{fractional}")
    };
    if value.is_negative() {
        decimal.insert(0, '-');
    }
    decimal
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
        MaterializedViewRegistryError::InvalidExecutionMode {
            view_id,
            reason: InvalidExecutionModeReason::StandingRuntimeMissingIdentity,
        } => ApiError::conflict(format!(
            "artifact-backed view `{view_id}` is missing standing runtime identity"
        )),
        MaterializedViewRegistryError::InvalidExecutionMode {
            view_id,
            reason: InvalidExecutionModeReason::StandingRuntimeMissingArtifact,
        } => ApiError::conflict(format!(
            "standing runtime view `{view_id}` is missing artifact binding"
        )),
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
        | ViewCompileDeployJobRegistryError::ActiveClaim { .. }
        | ViewCompileDeployJobRegistryError::RecordIdentityMismatch { .. }
        | ViewCompileDeployJobRegistryError::CompileRequest(_) => ApiError::conflict(error),
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
    standing_runtime_fencing: StandingRuntimeFencingMode,
    standing_runtime_owner_ttl_ms: u64,
    feldera_pipeline_manager_url: Option<String>,
    feldera_bearer_token: Option<String>,
    feldera_compiler_poll_interval_ms: u64,
    feldera_compiler_timeout_ms: u64,
    feldera_compiler_profile: String,
    feldera_compiler_workers: u32,
    feldera_pipeline_manager_runtime_deployment_mode:
        Option<FelderaPipelineManagerRuntimeDeploymentMode>,
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
        let feldera_pipeline_manager_url = std::env::var("VELORIX_FELDERA_PIPELINE_MANAGER_URL")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let feldera_bearer_token = match std::env::var("VELORIX_FELDERA_BEARER_TOKEN") {
            Ok(value) if !value.trim().is_empty() => Some(value),
            Ok(_) => return Err(anyhow!("VELORIX_FELDERA_BEARER_TOKEN must not be empty")),
            Err(env::VarError::NotPresent) => None,
            Err(error) => return Err(anyhow!("invalid VELORIX_FELDERA_BEARER_TOKEN: {error}")),
        };
        let feldera_compiler_poll_interval_ms =
            parse_positive_u64_env("VELORIX_FELDERA_COMPILER_POLL_INTERVAL_MS", 1_000)?;
        let feldera_pipeline_manager_runtime_mode_raw =
            std::env::var("VELORIX_FELDERA_PIPELINE_MANAGER_RUNTIME_MODE").ok();
        let feldera_pipeline_manager_runtime_mode = parse_feldera_pipeline_manager_runtime_mode(
            feldera_pipeline_manager_runtime_mode_raw.as_deref(),
        )?;
        let feldera_pipeline_manager_runtime_production_enable = parse_bool_env(
            "VELORIX_FELDERA_PIPELINE_MANAGER_RUNTIME_PRODUCTION_ENABLE",
            false,
        )?;
        let feldera_pipeline_manager_runtime_deployment_mode =
            resolve_feldera_pipeline_manager_runtime_deployment_mode(
                feldera_pipeline_manager_runtime_mode,
                standing_runtime_fencing,
                feldera_pipeline_manager_runtime_production_enable,
                feldera_pipeline_manager_runtime_mode_raw.as_deref(),
            )?;
        let feldera_compiler_timeout_ms = parse_positive_u64_env(
            "VELORIX_FELDERA_COMPILER_TIMEOUT_MS",
            default_feldera_compiler_timeout_ms(
                feldera_pipeline_manager_runtime_deployment_mode.is_some(),
            ),
        )?;
        if feldera_compiler_timeout_ms < feldera_compiler_poll_interval_ms {
            return Err(anyhow!(
                "VELORIX_FELDERA_COMPILER_TIMEOUT_MS must be greater than or equal to VELORIX_FELDERA_COMPILER_POLL_INTERVAL_MS"
            ));
        }
        let feldera_compiler_profile =
            std::env::var("VELORIX_FELDERA_COMPILER_PROFILE").unwrap_or_else(|_| "dev".to_string());
        if feldera_compiler_profile.trim().is_empty() {
            return Err(anyhow!(
                "VELORIX_FELDERA_COMPILER_PROFILE must not be empty"
            ));
        }
        let feldera_compiler_workers =
            parse_positive_u32_env("VELORIX_FELDERA_COMPILER_WORKERS", 1)?;
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
            standing_runtime_fencing,
            standing_runtime_owner_ttl_ms,
            feldera_pipeline_manager_url,
            feldera_bearer_token,
            feldera_compiler_poll_interval_ms,
            feldera_compiler_timeout_ms,
            feldera_compiler_profile,
            feldera_compiler_workers,
            feldera_pipeline_manager_runtime_deployment_mode,
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

fn default_feldera_compiler_timeout_ms(runtime_enabled: bool) -> u64 {
    if runtime_enabled {
        FELDERA_COMPILER_RUNTIME_TIMEOUT_DEFAULT_MS
    } else {
        FELDERA_COMPILER_SCHEMA_TIMEOUT_DEFAULT_MS
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FelderaPipelineManagerRuntimeMode {
    Default,
    CompilerOnly,
    LocalVolatile,
    ExternalManaged,
}

fn parse_feldera_pipeline_manager_runtime_mode(
    value: Option<&str>,
) -> anyhow::Result<FelderaPipelineManagerRuntimeMode> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(FelderaPipelineManagerRuntimeMode::Default),
        Some("pipeline_manager_local_volatile") | Some("runtime") | Some("volatile_demo") => {
            Ok(FelderaPipelineManagerRuntimeMode::LocalVolatile)
        }
        Some("pipeline_manager_external_managed") | Some("external_managed") => {
            Ok(FelderaPipelineManagerRuntimeMode::ExternalManaged)
        }
        Some("compiler_only") => Ok(FelderaPipelineManagerRuntimeMode::CompilerOnly),
        Some(other) => Err(anyhow!(
            "VELORIX_FELDERA_PIPELINE_MANAGER_RUNTIME_MODE must be `pipeline_manager_local_volatile`, `runtime`, `volatile_demo`, `pipeline_manager_external_managed`, `external_managed`, or `compiler_only`, got `{other}`"
        )),
    }
}

fn resolve_feldera_pipeline_manager_runtime_deployment_mode(
    mode: FelderaPipelineManagerRuntimeMode,
    standing_runtime_fencing: StandingRuntimeFencingMode,
    production_enable: bool,
    raw_mode: Option<&str>,
) -> anyhow::Result<Option<FelderaPipelineManagerRuntimeDeploymentMode>> {
    match mode {
        FelderaPipelineManagerRuntimeMode::CompilerOnly => Ok(None),
        FelderaPipelineManagerRuntimeMode::Default => Ok((standing_runtime_fencing
            != StandingRuntimeFencingMode::Required)
            .then_some(FelderaPipelineManagerRuntimeDeploymentMode::LocalVolatile)),
        FelderaPipelineManagerRuntimeMode::LocalVolatile => {
            if standing_runtime_fencing == StandingRuntimeFencingMode::Required {
                return Err(anyhow!(
                    "VELORIX_FELDERA_PIPELINE_MANAGER_RUNTIME_MODE={} is local volatile and cannot be used when VELORIX_STANDING_RUNTIME_FENCING=required; use `pipeline_manager_external_managed` or `compiler_only`",
                    raw_mode.unwrap_or("pipeline_manager_local_volatile")
                ));
            }
            Ok(Some(
                FelderaPipelineManagerRuntimeDeploymentMode::LocalVolatile,
            ))
        }
        FelderaPipelineManagerRuntimeMode::ExternalManaged => {
            if standing_runtime_fencing == StandingRuntimeFencingMode::Required
                && !production_enable
            {
                return Err(anyhow!(
                    "VELORIX_FELDERA_PIPELINE_MANAGER_RUNTIME_MODE={} requires VELORIX_FELDERA_PIPELINE_MANAGER_RUNTIME_PRODUCTION_ENABLE=1 when VELORIX_STANDING_RUNTIME_FENCING=required",
                    raw_mode.unwrap_or("pipeline_manager_external_managed")
                ));
            }
            Ok(Some(
                FelderaPipelineManagerRuntimeDeploymentMode::ExternalManaged,
            ))
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

fn parse_positive_u32_env(name: &str, default: u32) -> anyhow::Result<u32> {
    match env::var(name) {
        Ok(value) => {
            let value = value.trim();
            let parsed = value
                .parse::<u32>()
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
    use velorix_core::relation::{ArrowStructFieldV1, VelorixStructFieldV1};

    #[test]
    fn feldera_runtime_compile_stall_detection_matches_sql_compiled_stopped_without_rust_artifact()
    {
        let stalled = FelderaPipelineStatusResponse {
            program_status: "SqlCompiled".to_string(),
            deployment_status: Some("Stopped".to_string()),
            deployment_resources_status: Some("Stopped".to_string()),
            program_version: 7,
            program_info: Some(json!({"schema": {"outputs": []}})),
            program_error: Some(json!({
                "sql_compilation": {
                    "exit_code": 0,
                    "messages": []
                },
                "rust_compilation": null
            })),
        };
        assert!(feldera_pipeline_sql_compiled_without_runtime_artifact(
            &stalled
        ));

        let compiling = FelderaPipelineStatusResponse {
            program_status: "CompilingRust".to_string(),
            deployment_status: Some("Stopped".to_string()),
            deployment_resources_status: Some("Stopped".to_string()),
            program_version: 7,
            program_info: None,
            program_error: None,
        };
        assert!(!feldera_pipeline_sql_compiled_without_runtime_artifact(
            &compiling
        ));
    }

    #[test]
    fn feldera_pipeline_name_stays_within_feldera_limit_when_view_id_is_long() {
        let name = feldera_pipeline_name_for_parts(
            "velorix_live_scores_by_user_15dc2_19ec1eae8bc_0",
            "velorix-feldera-compile-request-sha256-v1:df87b84c85c6ee61ffffffffffffffffffffffffffffffffffffffffffffffff",
        );

        assert!(
            name.len() <= FELDERA_PIPELINE_NAME_MAX_CHARS,
            "pipeline name `{name}` is too long"
        );
        assert!(name.starts_with("velorix-velorix_live_scores_by_user"));
        assert!(name.ends_with("-df87b84c85c6ee61"));
    }

    #[test]
    fn epoch_ingest_idempotency_key_is_order_independent_and_payload_bound() {
        let catalog = default_scores_relation_catalog().unwrap();
        let first = test_prepared_ingest_batch(
            &catalog,
            0,
            1,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        let second = test_prepared_ingest_batch(
            &catalog,
            1,
            2,
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        );
        let forward = epoch_ingest_idempotency_key("scores_by_user", [&first, &second]).unwrap();
        let reverse = epoch_ingest_idempotency_key("scores_by_user", [&second, &first]).unwrap();
        assert_eq!(forward.as_str(), reverse.as_str());

        let changed_payload = test_prepared_ingest_batch(
            &catalog,
            1,
            2,
            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        );
        let changed =
            epoch_ingest_idempotency_key("scores_by_user", [&first, &changed_payload]).unwrap();
        assert_ne!(forward.as_str(), changed.as_str());
    }

    fn test_prepared_ingest_batch(
        catalog: &VelorixRelationCatalogV1,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
        payload_digest: &str,
    ) -> PreparedIngestBatch {
        PreparedIngestBatch {
            request: IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive,
                rows: vec![json!({ "user_id": "u1", "score": 1, "delta": 1 })],
            },
            catalog: catalog.clone(),
            record_batch: RecordBatch::new_empty(Arc::new(Schema::empty())),
            end_offset_exclusive,
            payload_digest: payload_digest.to_string(),
            envelope: bytes::Bytes::new(),
        }
    }

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
    fn feldera_pipeline_manager_runtime_mode_defaults_to_local_runtime() {
        assert_eq!(
            parse_feldera_pipeline_manager_runtime_mode(None).unwrap(),
            FelderaPipelineManagerRuntimeMode::Default
        );
        assert_eq!(
            parse_feldera_pipeline_manager_runtime_mode(Some("pipeline_manager_local_volatile"))
                .unwrap(),
            FelderaPipelineManagerRuntimeMode::LocalVolatile
        );
        assert_eq!(
            parse_feldera_pipeline_manager_runtime_mode(Some("runtime")).unwrap(),
            FelderaPipelineManagerRuntimeMode::LocalVolatile
        );
        assert_eq!(
            parse_feldera_pipeline_manager_runtime_mode(Some("volatile_demo")).unwrap(),
            FelderaPipelineManagerRuntimeMode::LocalVolatile
        );
        assert_eq!(
            parse_feldera_pipeline_manager_runtime_mode(Some("pipeline_manager_external_managed"))
                .unwrap(),
            FelderaPipelineManagerRuntimeMode::ExternalManaged
        );
        assert_eq!(
            parse_feldera_pipeline_manager_runtime_mode(Some("external_managed")).unwrap(),
            FelderaPipelineManagerRuntimeMode::ExternalManaged
        );
        assert_eq!(
            parse_feldera_pipeline_manager_runtime_mode(Some("compiler_only")).unwrap(),
            FelderaPipelineManagerRuntimeMode::CompilerOnly
        );
        assert!(parse_feldera_pipeline_manager_runtime_mode(Some("running")).is_err());
    }

    #[test]
    fn feldera_compiler_timeout_default_is_runtime_sensitive() {
        assert_eq!(
            default_feldera_compiler_timeout_ms(false),
            FELDERA_COMPILER_SCHEMA_TIMEOUT_DEFAULT_MS
        );
        assert_eq!(
            default_feldera_compiler_timeout_ms(true),
            FELDERA_COMPILER_RUNTIME_TIMEOUT_DEFAULT_MS
        );
        assert!(
            FELDERA_COMPILER_RUNTIME_TIMEOUT_DEFAULT_MS
                > FELDERA_COMPILER_SCHEMA_TIMEOUT_DEFAULT_MS
        );
    }

    #[test]
    fn feldera_pipeline_manager_runtime_gate_resolves_deployment_modes() {
        assert_eq!(
            resolve_feldera_pipeline_manager_runtime_deployment_mode(
                FelderaPipelineManagerRuntimeMode::Default,
                StandingRuntimeFencingMode::SingleWriter,
                false,
                None,
            )
            .unwrap(),
            Some(FelderaPipelineManagerRuntimeDeploymentMode::LocalVolatile)
        );
        assert_eq!(
            resolve_feldera_pipeline_manager_runtime_deployment_mode(
                FelderaPipelineManagerRuntimeMode::Default,
                StandingRuntimeFencingMode::LogicalFencing,
                false,
                None,
            )
            .unwrap(),
            Some(FelderaPipelineManagerRuntimeDeploymentMode::LocalVolatile)
        );
        assert_eq!(
            resolve_feldera_pipeline_manager_runtime_deployment_mode(
                FelderaPipelineManagerRuntimeMode::Default,
                StandingRuntimeFencingMode::Required,
                false,
                None,
            )
            .unwrap(),
            None
        );
        assert_eq!(
            resolve_feldera_pipeline_manager_runtime_deployment_mode(
                FelderaPipelineManagerRuntimeMode::CompilerOnly,
                StandingRuntimeFencingMode::Required,
                true,
                Some("compiler_only"),
            )
            .unwrap(),
            None
        );
        assert!(resolve_feldera_pipeline_manager_runtime_deployment_mode(
            FelderaPipelineManagerRuntimeMode::LocalVolatile,
            StandingRuntimeFencingMode::Required,
            true,
            Some("runtime"),
        )
        .is_err());
        assert!(resolve_feldera_pipeline_manager_runtime_deployment_mode(
            FelderaPipelineManagerRuntimeMode::ExternalManaged,
            StandingRuntimeFencingMode::Required,
            false,
            Some("external_managed"),
        )
        .is_err());
        assert_eq!(
            resolve_feldera_pipeline_manager_runtime_deployment_mode(
                FelderaPipelineManagerRuntimeMode::ExternalManaged,
                StandingRuntimeFencingMode::Required,
                true,
                Some("external_managed"),
            )
            .unwrap(),
            Some(FelderaPipelineManagerRuntimeDeploymentMode::ExternalManaged)
        );
    }

    #[test]
    fn sql_references_table_accepts_quoted_feldera_output_identifiers() {
        assert!(sql_references_table(
            "select user_id, sum from \"scores-by-user\" where sum > 0",
            "scores-by-user"
        ));
        assert!(sql_references_table(
            "select region from \"sales.summary\" order by region",
            "sales.summary"
        ));
        assert!(sql_references_table(
            "select x from \"quoted\"\"output\"",
            "quoted\"output"
        ));
    }

    #[test]
    fn sql_references_table_ignores_literals_and_rejects_other_tables() {
        assert!(!sql_references_table(
            "select 'from scores-by-user' as message",
            "scores-by-user"
        ));
        assert!(!sql_references_table(
            "select 'from scores_by_user' as message",
            "scores_by_user"
        ));
        assert!(!sql_references_table(
            "select user_id from scores join raw_scores on scores.user_id = raw_scores.user_id",
            "scores-by-user"
        ));
        assert!(sql_references_table(
            "select user_id from scores_by_user where note = 'join raw_scores'",
            "scores_by_user"
        ));
    }

    #[test]
    fn feldera_pipeline_manager_insert_delete_events_strip_weight_metadata() {
        let events = feldera_pipeline_manager_insert_delete_events(
            "delta",
            true,
            vec![
                json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                json!({ "user_id": "u1", "score": 5, "delta": -1 }),
            ],
        )
        .unwrap();

        assert_eq!(
            events,
            vec![
                json!({ "insert": { "user_id": "u1", "score": 5 } }),
                json!({ "delete": { "user_id": "u1", "score": 5 } }),
            ]
        );
    }

    #[test]
    fn feldera_pipeline_manager_insert_delete_events_rejects_non_unit_weight() {
        let error = feldera_pipeline_manager_insert_delete_events(
            "delta",
            true,
            vec![json!({ "user_id": "u1", "score": 5, "delta": 2 })],
        )
        .unwrap_err();

        assert!(error.contains("supports only signed unit weights"));
    }

    #[test]
    fn feldera_pipeline_manager_insert_delete_events_rejects_delete_without_capability() {
        let error = feldera_pipeline_manager_insert_delete_events(
            "delta",
            false,
            vec![json!({ "user_id": "u1", "score": 5, "delta": -1 })],
        )
        .unwrap_err();

        assert!(error.contains("insert-only relation"));
    }

    #[test]
    fn feldera_pipeline_manager_relation_operations_accept_insert_delete_as_set() {
        let mut catalog = default_scores_relation_catalog().unwrap();
        catalog.relation_schema.allowed_operations =
            vec![RelationOperationV1::Delete, RelationOperationV1::Insert];

        let delete_capable =
            validate_feldera_pipeline_manager_relation_operations(&catalog).unwrap();

        assert!(delete_capable);
    }

    #[test]
    fn feldera_pipeline_manager_relation_operations_reject_delete_only() {
        let mut catalog = default_scores_relation_catalog().unwrap();
        catalog.relation_schema.allowed_operations = vec![RelationOperationV1::Delete];

        let error = validate_feldera_pipeline_manager_relation_operations(&catalog).unwrap_err();

        assert!(error.contains("requires Insert relation operation"));
    }

    #[test]
    fn feldera_pipeline_manager_relation_operations_reject_duplicates() {
        let mut catalog = default_scores_relation_catalog().unwrap();
        catalog.relation_schema.allowed_operations =
            vec![RelationOperationV1::Insert, RelationOperationV1::Insert];

        let error = validate_feldera_pipeline_manager_relation_operations(&catalog).unwrap_err();

        assert!(error.contains("duplicate Insert"));
    }

    #[test]
    fn feldera_pipeline_manager_relation_operations_reject_duplicate_update_and_upsert() {
        for (operation, expected) in [
            (RelationOperationV1::Update, "duplicate Update"),
            (RelationOperationV1::Upsert, "duplicate Upsert"),
        ] {
            let mut catalog = default_scores_relation_catalog().unwrap();
            catalog.relation_schema.allowed_operations = vec![
                RelationOperationV1::Insert,
                operation.clone(),
                operation.clone(),
            ];

            let error = validate_feldera_pipeline_manager_relation_operations(&catalog)
                .expect_err("duplicate operation should fail admission");

            assert!(
                error.contains(expected),
                "expected `{expected}`, got `{error}`"
            );
        }
    }

    #[test]
    fn feldera_pipeline_manager_relation_operations_treat_update_and_upsert_as_delete_event_capable(
    ) {
        for operation in [RelationOperationV1::Update, RelationOperationV1::Upsert] {
            let mut catalog = default_scores_relation_catalog().unwrap();
            catalog.relation_schema.allowed_operations =
                vec![RelationOperationV1::Insert, operation.clone()];

            let delete_capable =
                validate_feldera_pipeline_manager_relation_operations(&catalog).unwrap();

            assert!(
                delete_capable,
                "operation {operation:?} should allow internal delete events"
            );
        }
    }

    #[test]
    fn ingest_operation_envelope_normalizes_update_and_upsert_to_signed_rows() {
        let mut catalog = default_scores_relation_catalog().unwrap();
        catalog
            .relation_schema
            .allowed_operations
            .push(RelationOperationV1::Update);
        catalog
            .relation_schema
            .allowed_operations
            .push(RelationOperationV1::Upsert);

        let rows = normalize_ingest_operation_envelopes(
            &catalog,
            &[
                json!({
                    "operation": "update",
                    "before": { "user_id": "u1", "score": 5 },
                    "after": { "user_id": "u1", "score": 7 }
                }),
                json!({
                    "operation": "upsert",
                    "before": { "user_id": "u2", "score": 3 },
                    "row": { "user_id": "u2", "score": 9 }
                }),
            ],
        )
        .unwrap();

        assert_eq!(
            rows,
            vec![
                json!({ "user_id": "u1", "score": 5, "delta": -1 }),
                json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                json!({ "user_id": "u2", "score": 3, "delta": -1 }),
                json!({ "user_id": "u2", "score": 9, "delta": 1 }),
            ]
        );
    }

    #[test]
    fn ingest_operation_envelope_rejects_update_without_capability() {
        let catalog = default_scores_relation_catalog().unwrap();

        let error = normalize_ingest_operation_envelopes(
            &catalog,
            &[json!({
                "operation": "update",
                "before": { "user_id": "u1", "score": 5 },
                "after": { "user_id": "u1", "score": 7 }
            })],
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("does not allow Update"),
            "error: {error}"
        );
    }

    #[test]
    fn feldera_pipeline_manager_runtime_catalogs_reject_weight_column_primary_key() {
        let mut catalog = default_scores_relation_catalog().unwrap();
        catalog
            .relation_schema
            .primary_key_column_ids
            .push("delta".to_string());

        let error = validate_feldera_pipeline_manager_runtime_catalogs(&[catalog]).unwrap_err();

        assert!(error.contains("does not allow the weight column in the primary key"));
    }

    #[test]
    fn feldera_pipeline_manager_external_managed_mode_changes_artifact_identity() {
        let catalog = default_scores_relation_catalog().unwrap();
        let input = catalog_input_relation_schema(&catalog).unwrap();
        let output = single_key_sum_count_output_schema("scores_by_user", &catalog).unwrap();
        let spec = StandingViewSpec {
            view_id: "scores_by_user".to_string(),
            sql:
                "select user_id, sum(score) as sum, count(*) as count from scores group by user_id"
                    .to_string(),
            dialect: SqlDialect::FelderaSql,
            source_kind: SqlSourceKind::StandingView,
            rust_extension: Default::default(),
            input_relations: vec![input],
            output_relations: vec![output],
            shape: StandingViewShape {
                is_materialized: true,
                multi_input: false,
                multi_output: false,
            },
        };
        let local = external_feldera_runtime_artifact_binding(
            std::slice::from_ref(&catalog),
            &spec,
            &FelderaPipelineManagerRuntimeDeployment {
                pipeline_name: "velorix-scores".to_string(),
                mode: FelderaPipelineManagerRuntimeDeploymentMode::LocalVolatile,
            },
        )
        .unwrap();
        let external = external_feldera_runtime_artifact_binding(
            std::slice::from_ref(&catalog),
            &spec,
            &FelderaPipelineManagerRuntimeDeployment {
                pipeline_name: "velorix-scores".to_string(),
                mode: FelderaPipelineManagerRuntimeDeploymentMode::ExternalManaged,
            },
        )
        .unwrap();

        assert_eq!(local.execution_path, "feldera_pipeline_manager");
        assert_eq!(external.execution_path, "feldera_pipeline_manager");
        assert_ne!(local.artifact_hash, external.artifact_hash);
        assert_ne!(local.artifact_id, "");
        assert_eq!(local.artifact_id, external.artifact_id);
        assert_eq!(external.state_schema_version, 2);
        assert_eq!(external.state_codec, "feldera-pipeline-manager-state-v2");
    }

    #[test]
    fn feldera_pipeline_manager_checkpoint_records_external_managed_mode() {
        let catalog = default_scores_relation_catalog().unwrap();
        let input_schema = catalog_input_relation_schema(&catalog).unwrap();
        let output_schema = single_key_sum_count_output_schema("scores_by_user", &catalog).unwrap();
        let spec = StandingViewSpec {
            view_id: "scores_by_user".to_string(),
            sql:
                "select user_id, sum(score) as sum, count(*) as count from scores group by user_id"
                    .to_string(),
            dialect: SqlDialect::FelderaSql,
            source_kind: SqlSourceKind::StandingView,
            rust_extension: Default::default(),
            input_relations: vec![input_schema.clone()],
            output_relations: vec![output_schema.clone()],
            shape: StandingViewShape {
                is_materialized: true,
                multi_input: false,
                multi_output: false,
            },
        };
        let identity =
            standing_program_identity_from_external_feldera_runtime(&[catalog.clone()], &spec)
                .unwrap();
        let (input_relation_names, input_weight_column_names, input_delete_capable_relation_ids) =
            validate_feldera_pipeline_manager_runtime_catalogs(std::slice::from_ref(&catalog))
                .unwrap();
        let runtime = FelderaPipelineManagerStandingRuntime {
            identity,
            input_schemas: vec![input_schema],
            output_schemas: vec![output_schema],
            input_relation_names,
            input_weight_column_names,
            input_delete_capable_relation_ids,
            input_catalogs: input_catalog_map(vec![catalog]),
            base_url: "http://127.0.0.1:1".to_string(),
            bearer_token: None,
            pipeline_name: "velorix-scores".to_string(),
            runtime_deployment_mode: FelderaPipelineManagerRuntimeDeploymentMode::ExternalManaged,
            logical_epoch: 3,
            applied_idempotency: HashMap::from([("epoch-3".to_string(), 3)]),
            input_frontiers: BTreeMap::new(),
            poisoned_reason: None,
            timeout: Duration::from_millis(1),
            cleanup_on_drop: false,
        };

        let checkpoint = runtime.checkpoint().unwrap();
        let payload = checkpoint.state_payload.as_ref().unwrap();
        let payload: Value = serde_json::from_str(&payload.payload).unwrap();

        assert_eq!(
            checkpoint.checkpoint_codec_identity,
            FELDERA_PIPELINE_MANAGER_STATE_CODEC
        );
        assert_eq!(payload["pipeline_name"], "velorix-scores");
        assert_eq!(payload["logical_epoch"], 3);
        assert_eq!(payload["deployment_mode"], "external_managed");
        assert_eq!(payload["applied_idempotency"]["epoch-3"], 3);
    }

    #[test]
    fn feldera_pipeline_manager_restore_rejects_checkpoint_deployment_mode_mismatch() {
        let catalog = default_scores_relation_catalog().unwrap();
        let input_schema = catalog_input_relation_schema(&catalog).unwrap();
        let output_schema = single_key_sum_count_output_schema("scores_by_user", &catalog).unwrap();
        let spec = StandingViewSpec {
            view_id: "scores_by_user".to_string(),
            sql:
                "select user_id, sum(score) as sum, count(*) as count from scores group by user_id"
                    .to_string(),
            dialect: SqlDialect::FelderaSql,
            source_kind: SqlSourceKind::StandingView,
            rust_extension: Default::default(),
            input_relations: vec![input_schema.clone()],
            output_relations: vec![output_schema.clone()],
            shape: StandingViewShape {
                is_materialized: true,
                multi_input: false,
                multi_output: false,
            },
        };
        let identity =
            standing_program_identity_from_external_feldera_runtime(&[catalog.clone()], &spec)
                .unwrap();
        let payload = json!({
            "pipeline_name": "velorix-scores",
            "logical_epoch": 7,
            "deployment_mode": "local_volatile",
            "applied_idempotency": { "epoch-7": 7 }
        })
        .to_string();
        let checkpoint = RuntimeCheckpoint {
            identity,
            logical_epoch: 7,
            input_frontiers: Vec::new(),
            output_frontiers: Vec::new(),
            checkpoint_codec_identity: FELDERA_PIPELINE_MANAGER_STATE_CODEC.to_string(),
            state_root: DurableStateRoot {
                object_key: "feldera-pipeline-manager://velorix-scores".to_string(),
                content_hash: feldera_artifact_bytes_hash(payload.as_bytes()),
            },
            state_payload: Some(
                velorix_core::standing_program::RuntimeCheckpointStatePayload {
                    codec_identity: FELDERA_PIPELINE_MANAGER_STATE_CODEC.to_string(),
                    payload,
                },
            ),
            output_manifest_refs: Vec::new(),
            owner_epoch: None,
        };

        let error = match FelderaPipelineManagerStandingRuntime::restore_with_metadata(
            checkpoint,
            vec![input_schema],
            vec![output_schema],
            vec![catalog],
            "http://127.0.0.1:1".to_string(),
            None,
            "velorix-scores".to_string(),
            FelderaPipelineManagerRuntimeDeploymentMode::ExternalManaged,
            Duration::from_millis(1),
        ) {
            Ok(_) => panic!("restore unexpectedly accepted mismatched deployment mode"),
            Err(error) => error,
        };

        assert!(
            error.contains("checkpoint deployment mode mismatch"),
            "error: {error}"
        );
    }

    #[test]
    fn create_view_request_accepts_feldera_program_output_hints() {
        let request: CreateViewRequest = serde_json::from_value(json!({
            "view_id": "score_program",
            "input_relation_id": "scores",
            "input_relation_version": "v1",
            "source_kind": "feldera_program",
            "output_relation_ids": ["by_user", "by_region"],
            "sql": "CREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id; CREATE MATERIALIZED VIEW by_region AS SELECT region, COUNT(*) AS count FROM scores GROUP BY region;"
        }))
        .unwrap();

        validate_create_view_sql_source_contract(&request).unwrap();
        let outputs = generic_materialized_view_output_schemas_for_ids(
            &request.output_relation_ids,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();

        assert_eq!(request.source_kind, SqlSourceKind::FelderaProgram);
        assert_eq!(
            outputs
                .iter()
                .map(|schema| schema.relation_id.as_str())
                .collect::<Vec<_>>(),
            vec!["by_user", "by_region"]
        );
        assert!(outputs
            .iter()
            .all(|schema| schema.schema_fingerprint.starts_with("sha256:")));
    }

    #[test]
    fn create_view_request_treats_create_sql_as_feldera_program_when_source_kind_omitted() {
        let request: CreateViewRequest = serde_json::from_value(json!({
            "view_id": "score_program",
            "input_relation_id": "scores",
            "input_relation_version": "v1",
            "output_relation_ids": ["by_user", "by_region"],
            "sql": "  CREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id; CREATE MATERIALIZED VIEW by_region AS SELECT region, COUNT(*) AS count FROM scores GROUP BY region;"
        }))
        .unwrap();

        validate_create_view_sql_source_contract(&request).unwrap();

        assert_eq!(request.source_kind, SqlSourceKind::StandingView);
        assert_eq!(
            resolved_sql_source_kind_for_create_view(&request),
            SqlSourceKind::FelderaProgram
        );
    }

    #[test]
    fn create_view_request_treats_commented_create_sql_as_feldera_program() {
        let request: CreateViewRequest = serde_json::from_value(json!({
            "view_id": "score_program",
            "input_relation_id": "scores",
            "input_relation_version": "v1",
            "output_relation_ids": ["by_user"],
            "sql": "-- generated by app\n/* program outputs */\nCREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id"
        }))
        .unwrap();

        validate_create_view_sql_source_contract(&request).unwrap();

        assert_eq!(
            resolved_sql_source_kind_for_create_view(&request),
            SqlSourceKind::FelderaProgram
        );
    }

    #[test]
    fn create_view_request_does_not_treat_create_prefix_identifier_as_program() {
        let request: CreateViewRequest = serde_json::from_value(json!({
            "view_id": "score_program",
            "input_relation_id": "scores",
            "input_relation_version": "v1",
            "output_relation_ids": ["by_user"],
            "sql": "create_view_alias AS SELECT user_id FROM scores"
        }))
        .unwrap();

        assert!(validate_create_view_sql_source_contract(&request).is_err());
        assert_eq!(
            resolved_sql_source_kind_for_create_view(&request),
            SqlSourceKind::StandingView
        );
    }

    #[test]
    fn create_view_request_rejects_output_hints_for_standing_view_source() {
        let request: CreateViewRequest = serde_json::from_value(json!({
            "view_id": "score_program",
            "input_relation_id": "scores",
            "input_relation_version": "v1",
            "output_relation_ids": ["by_user"],
            "sql": "SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id"
        }))
        .unwrap();

        assert!(validate_create_view_sql_source_contract(&request).is_err());
    }

    #[test]
    fn caller_sql_renderer_compiles_typed_placeholders_to_feldera_prepare_execute() {
        let sql = "select * from scores where user_id = {{ context.params.user_id | is_required | is_string }} and score >= {{ context.params.min_score | is_integer(min=0) }} and active = {{ context.params.active | is_boolean }}";
        let rendered = render_caller_sql_as_feldera_sql(
            sql,
            &BTreeMap::from([
                ("user_id".to_string(), json!("u'1")),
                ("min_score".to_string(), json!("5")),
                ("active".to_string(), json!("true")),
            ]),
        )
        .unwrap();

        assert_eq!(
            rendered,
            "PREPARE velorix_query AS select * from scores where user_id = $1 and score >= $2 and active = $3;\nEXECUTE velorix_query('u''1', 5, TRUE);"
        );
    }

    #[test]
    fn caller_sql_renderer_compiles_array_placeholder_to_feldera_array_literal() {
        let sql =
            "select * from scores where user_id in unnest({{ context.params.user_ids | is_array(element=string) }})";
        let rendered = render_caller_sql_as_feldera_sql(
            sql,
            &BTreeMap::from([("user_ids".to_string(), json!(["u1", "u'2"]))]),
        )
        .unwrap();

        assert_eq!(
            rendered,
            "PREPARE velorix_query AS select * from scores where user_id IN ($1, $2);\nEXECUTE velorix_query('u1', 'u''2');"
        );
    }

    #[test]
    fn caller_sql_renderer_compiles_typed_array_placeholders_to_feldera_literals() {
        let sql = "select * from events where event_date in unnest({{ context.params.days | is_array(element=date) }}) and event_ts in unnest({{ context.params.timestamps | is_array(element=timestamp) }}) and event_uuid in unnest({{ context.params.ids | is_array(element=uuid) }}) and amount in unnest({{ context.params.amounts | is_array(element=decimal) }}) and raw in unnest({{ context.params.raw_values | is_array(element=binary_hex) }})";
        let rendered = render_caller_sql_as_feldera_sql(
            sql,
            &BTreeMap::from([
                ("days".to_string(), json!(["2026-06-10", "2026-06-11"])),
                (
                    "timestamps".to_string(),
                    json!(["2026-06-10T01:02:03", "2026-06-11 04:05:06"]),
                ),
                (
                    "ids".to_string(),
                    json!([
                        "018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa",
                        "018f4b6e-9cb5-7f5a-8027-2ce24be4d3ab"
                    ]),
                ),
                ("amounts".to_string(), json!(["123.45", "-7"])),
                ("raw_values".to_string(), json!(["0x0A0bff", "deadbeef"])),
            ]),
        )
        .unwrap();

        assert_eq!(
            rendered,
            "PREPARE velorix_query AS select * from events where event_date IN ($1, $2) and event_ts IN ($3, $4) and event_uuid IN ($5, $6) and amount IN ($7, $8) and raw IN ($9, $10);\nEXECUTE velorix_query('2026-06-10', '2026-06-11', '2026-06-10 01:02:03', '2026-06-11 04:05:06', '018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa', '018f4b6e-9cb5-7f5a-8027-2ce24be4d3ab', 123.45, -7, x'0a0bff', x'deadbeef');"
        );
    }

    #[test]
    fn caller_sql_renderer_rejects_invalid_typed_array_placeholder() {
        let error = render_caller_sql_as_feldera_sql(
            "select * from events where event_date in unnest({{ context.params.days | is_array(element=date) }})",
            &BTreeMap::from([("days".to_string(), json!(["2026-06-10", "2026-02-30"]))]),
        )
        .unwrap_err();

        assert!(error.to_string().contains("is_date"), "error: {error}");
    }

    #[test]
    fn caller_sql_renderer_compiles_typed_feldera_literal_placeholders() {
        let sql = "select * from events where event_date = {{ context.params.event_date | is_date }} and event_time = {{ context.params.event_time | is_time }} and event_ts = {{ context.params.event_ts | is_timestamp }} and event_uuid = {{ context.params.event_uuid | is_uuid }} and amount = {{ context.params.amount | is_decimal }} and raw = {{ context.params.raw | is_binary_hex }}";
        let rendered = render_caller_sql_as_feldera_sql(
            sql,
            &BTreeMap::from([
                ("event_date".to_string(), json!("2026-06-10")),
                ("event_time".to_string(), json!("01:02:03.004")),
                ("event_ts".to_string(), json!("2026-06-10T01:02:03.004")),
                (
                    "event_uuid".to_string(),
                    json!("018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa"),
                ),
                ("amount".to_string(), json!("123.4500")),
                ("raw".to_string(), json!("0x0A0bff")),
            ]),
        )
        .unwrap();

        assert_eq!(
            rendered,
            "PREPARE velorix_query AS select * from events where event_date = $1 and event_time = $2 and event_ts = $3 and event_uuid = $4 and amount = $5 and raw = $6;\nEXECUTE velorix_query('2026-06-10', '01:02:03.004', '2026-06-10 01:02:03.004', '018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa', 123.4500, x'0a0bff');"
        );
    }

    #[test]
    fn caller_sql_renderer_compiles_json_placeholder_to_canonical_feldera_string_literal() {
        let sql = "select * from events where raw_json = {{ context.params.payload | is_json }}";
        let rendered = render_caller_sql_as_feldera_sql(
            sql,
            &BTreeMap::from([(
                "payload".to_string(),
                json!({"name": "Ada", "quote": "it isn't magic"}),
            )]),
        )
        .unwrap();

        assert_eq!(
            rendered,
            "PREPARE velorix_query AS select * from events where raw_json = $1;\nEXECUTE velorix_query('{\"name\":\"Ada\",\"quote\":\"it isn''t magic\"}');"
        );
    }

    #[test]
    fn caller_sql_renderer_compiles_json_array_to_canonical_feldera_string_literals() {
        let sql =
            "select * from events where raw_json in unnest({{ context.params.payloads | is_array(element=json) }})";
        let rendered = render_caller_sql_as_feldera_sql(
            sql,
            &BTreeMap::from([(
                "payloads".to_string(),
                json!([{"kind": "a"}, {"kind": "b"}]),
            )]),
        )
        .unwrap();

        assert_eq!(
            rendered,
            "PREPARE velorix_query AS select * from events where raw_json IN ($1, $2);\nEXECUTE velorix_query('{\"kind\":\"a\"}', '{\"kind\":\"b\"}');"
        );
    }

    #[test]
    fn request_field_contract_rejects_variant_type_with_feldera_query_reason() {
        let field = MaterializedViewRequestFieldSpec {
            field_name: "payload".to_string(),
            field_in: "query".to_string(),
            r#type: "variant".to_string(),
            validators: vec!["required".to_string()],
            default_value: None,
            description: None,
        };

        let error = validate_request_field_contract(&field).unwrap_err();

        assert!(
            error.to_string().contains(
                "Feldera pipeline-manager /query does not support request-time VARIANT bind literals"
            ),
            "error: {error}"
        );
    }

    #[test]
    fn caller_sql_renderer_rejects_is_variant_filter_with_feldera_query_reason() {
        let error = render_caller_sql_as_feldera_sql(
            "select * from events where payload = {{ context.params.payload | is_variant }}",
            &BTreeMap::from([("payload".to_string(), json!({"name": "Ada"}))]),
        )
        .unwrap_err();

        assert!(
            error.to_string().contains(
                "Feldera pipeline-manager /query does not support request-time VARIANT bind literals"
            ),
            "error: {error}"
        );
    }

    #[test]
    fn caller_sql_renderer_rejects_invalid_typed_feldera_literal_placeholder() {
        let error = render_caller_sql_as_feldera_sql(
            "select * from events where event_date = {{ context.params.event_date | is_date }}",
            &BTreeMap::from([("event_date".to_string(), json!("2026-02-30"))]),
        )
        .unwrap_err();

        assert!(error.to_string().contains("is_date"), "error: {error}");
    }

    #[test]
    fn caller_sql_renderer_rejects_array_placeholder_with_wrong_element_type() {
        let error = render_caller_sql_as_feldera_sql(
            "select * from scores where user_id in unnest({{ context.params.user_ids | is_array(element=string) }})",
            &BTreeMap::from([("user_ids".to_string(), json!(["u1", 2]))]),
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("is_array(element=string)"),
            "error: {error}"
        );
    }

    #[test]
    fn caller_sql_renderer_rejects_unreferenced_parameters() {
        let error = render_caller_sql_as_feldera_sql(
            "select * from scores",
            &BTreeMap::from([("unused".to_string(), json!("u1"))]),
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("not referenced"),
            "error: {error}"
        );
    }

    #[test]
    fn caller_sql_renderer_rejects_multi_statement_sql_when_parameters_require_prepare() {
        let error = render_caller_sql_as_feldera_sql(
            "select * from scores where user_id = {{ context.params.user_id | is_string }}; select * from scores",
            &BTreeMap::from([("user_id".to_string(), json!("u1"))]),
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("single SQL statement"),
            "error: {error}"
        );
    }

    #[test]
    fn caller_sql_renderer_passes_through_braces_when_parameters_are_empty() {
        let sql = "select '{{ not_a_parameter }}' as literal from scores";

        let rendered = render_caller_sql_as_feldera_sql(sql, &BTreeMap::new()).unwrap();

        assert_eq!(rendered, sql);
    }

    #[test]
    fn caller_sql_renderer_only_replaces_context_params_placeholders() {
        let sql = "select '{{ feldera_owned }}' as literal, {{ context.params.user_id | is_string }} as user_id";
        let rendered = render_caller_sql_as_feldera_sql(
            sql,
            &BTreeMap::from([("user_id".to_string(), json!("u1"))]),
        )
        .unwrap();

        assert_eq!(
            rendered,
            "PREPARE velorix_query AS select '{{ feldera_owned }}' as literal, $1 as user_id;\nEXECUTE velorix_query('u1');"
        );
    }

    #[test]
    fn caller_sql_renderer_does_not_replace_placeholders_inside_sql_strings_or_comments() {
        let sql = "select 'it''s {{ context.params.literal }}' as literal, \"{{ context.params.ident }}\" as quoted_ident, {{ context.params.user_id | is_string }} as user_id -- {{ context.params.comment }}\n/* {{ context.params.block }} */";
        let rendered = render_caller_sql_as_feldera_sql(
            sql,
            &BTreeMap::from([("user_id".to_string(), json!("u1"))]),
        )
        .unwrap();

        assert_eq!(
            rendered,
            "PREPARE velorix_query AS select 'it''s {{ context.params.literal }}' as literal, \"{{ context.params.ident }}\" as quoted_ident, $1 as user_id -- {{ context.params.comment }}\n/* {{ context.params.block }} */;\nEXECUTE velorix_query('u1');"
        );
    }

    #[test]
    fn sql_template_renderer_does_not_replace_placeholders_inside_sql_strings_or_comments() {
        let field = MaterializedViewRequestFieldSpec {
            field_name: "user_id".to_string(),
            field_in: "query".to_string(),
            r#type: "string".to_string(),
            validators: vec!["required".to_string(), "string".to_string()],
            default_value: None,
            description: None,
        };
        let template = "select '{{ context.params.literal }}' as literal, \"{{ context.params.ident }}\" as ident, {{ context.params.user_id | is_required | is_string }} as user_id -- {{ context.params.comment }}\n/* {{ context.params.block }} */";
        let rendered = render_view_sql_template_as_feldera_sql(
            template,
            &[field],
            &BTreeMap::from([("user_id".to_string(), json!("u1"))]),
        )
        .unwrap();

        assert_eq!(
            rendered,
            "PREPARE velorix_query AS select '{{ context.params.literal }}' as literal, \"{{ context.params.ident }}\" as ident, $1 as user_id -- {{ context.params.comment }}\n/* {{ context.params.block }} */;\nEXECUTE velorix_query('u1');"
        );
    }

    #[test]
    fn response_schema_json_column_parses_canonical_json_text() {
        let rows = vec![json!({
            "payload_alias": "{\"name\":\"Ada\",\"scores\":[8,13],\"nested\":{\"active\":true}}",
            "label": "object"
        })];
        let response_schema = MaterializedViewResponseSchema {
            columns: vec![
                MaterializedViewResponseColumnSpec {
                    name: "payload".to_string(),
                    r#type: "json".to_string(),
                    source: "payload_alias".to_string(),
                    description: None,
                },
                MaterializedViewResponseColumnSpec {
                    name: "label".to_string(),
                    r#type: "string".to_string(),
                    source: "label".to_string(),
                    description: None,
                },
            ],
        };

        let rows = materialized_rows_to_api_rows(&rows, &response_schema).unwrap();

        assert_eq!(rows[0]["payload"]["name"], json!("Ada"));
        assert_eq!(rows[0]["payload"]["scores"], json!([8, 13]));
        assert_eq!(rows[0]["payload"]["nested"]["active"], json!(true));
        assert_eq!(rows[0]["label"], json!("object"));
    }

    #[test]
    fn response_schema_json_column_preserves_plain_string_values() {
        let rows = vec![json!({ "payload_alias": "plain-string" })];
        let response_schema = MaterializedViewResponseSchema {
            columns: vec![MaterializedViewResponseColumnSpec {
                name: "payload".to_string(),
                r#type: "json".to_string(),
                source: "payload_alias".to_string(),
                description: None,
            }],
        };

        let rows = materialized_rows_to_api_rows(&rows, &response_schema).unwrap();

        assert_eq!(rows[0]["payload"], json!("plain-string"));
    }

    #[test]
    fn response_schema_json_column_parses_canonical_json_string_scalar() {
        let rows = vec![json!({ "payload_alias": "\"plain-string\"" })];
        let response_schema = MaterializedViewResponseSchema {
            columns: vec![MaterializedViewResponseColumnSpec {
                name: "payload".to_string(),
                r#type: "json".to_string(),
                source: "payload_alias".to_string(),
                description: None,
            }],
        };

        let rows = materialized_rows_to_api_rows(&rows, &response_schema).unwrap();

        assert_eq!(rows[0]["payload"], json!("plain-string"));
    }

    #[test]
    fn response_schema_accepts_feldera_scalar_output_types() {
        let rows = vec![json!({
            "event_date": "2026-06-10",
            "event_time": "01:02:03.004005006",
            "event_ts": "2026-06-10T01:02:03.004005006",
            "event_uuid": "018F4B6E-9CB5-7F5A-8027-2CE24BE4D3AA",
            "amount": 12.34,
            "raw": "0A0BFF"
        })];
        let response_schema = MaterializedViewResponseSchema {
            columns: vec![
                MaterializedViewResponseColumnSpec {
                    name: "event_date".to_string(),
                    r#type: "date".to_string(),
                    source: "event_date".to_string(),
                    description: None,
                },
                MaterializedViewResponseColumnSpec {
                    name: "event_time".to_string(),
                    r#type: "time".to_string(),
                    source: "event_time".to_string(),
                    description: None,
                },
                MaterializedViewResponseColumnSpec {
                    name: "event_ts".to_string(),
                    r#type: "timestamp".to_string(),
                    source: "event_ts".to_string(),
                    description: None,
                },
                MaterializedViewResponseColumnSpec {
                    name: "event_uuid".to_string(),
                    r#type: "uuid".to_string(),
                    source: "event_uuid".to_string(),
                    description: None,
                },
                MaterializedViewResponseColumnSpec {
                    name: "amount".to_string(),
                    r#type: "decimal".to_string(),
                    source: "amount".to_string(),
                    description: None,
                },
                MaterializedViewResponseColumnSpec {
                    name: "raw".to_string(),
                    r#type: "binary_hex".to_string(),
                    source: "raw".to_string(),
                    description: None,
                },
            ],
        };

        let rows = materialized_rows_to_api_rows(&rows, &response_schema).unwrap();

        assert_eq!(rows[0]["event_date"], json!("2026-06-10"));
        assert_eq!(rows[0]["event_time"], json!("01:02:03.004005006"));
        assert_eq!(rows[0]["event_ts"], json!("2026-06-10 01:02:03.004005006"));
        assert_eq!(
            rows[0]["event_uuid"],
            json!("018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa")
        );
        assert_eq!(rows[0]["amount"], json!("12.34"));
        assert_eq!(rows[0]["raw"], json!("0x0a0bff"));
    }

    #[test]
    fn response_schema_accepts_feldera_complex_output_types() {
        let rows = vec![json!({
            "scores": [8, null, 13],
            "profile": { "name": "Ada", "tier": 2 },
            "attributes_json": "{\"critical\":9,\"batch\":null}",
            "tags_json": "[\"streaming\",\"sql\"]"
        })];
        let response_schema = MaterializedViewResponseSchema {
            columns: vec![
                MaterializedViewResponseColumnSpec {
                    name: "scores".to_string(),
                    r#type: "array".to_string(),
                    source: "scores".to_string(),
                    description: None,
                },
                MaterializedViewResponseColumnSpec {
                    name: "profile".to_string(),
                    r#type: "object".to_string(),
                    source: "profile".to_string(),
                    description: None,
                },
                MaterializedViewResponseColumnSpec {
                    name: "attributes".to_string(),
                    r#type: "object".to_string(),
                    source: "attributes_json".to_string(),
                    description: None,
                },
                MaterializedViewResponseColumnSpec {
                    name: "tags".to_string(),
                    r#type: "array".to_string(),
                    source: "tags_json".to_string(),
                    description: None,
                },
            ],
        };

        let rows = materialized_rows_to_api_rows(&rows, &response_schema).unwrap();

        assert_eq!(rows[0]["scores"], json!([8, null, 13]));
        assert_eq!(rows[0]["profile"], json!({ "name": "Ada", "tier": 2 }));
        assert_eq!(
            rows[0]["attributes"],
            json!({ "critical": 9, "batch": null })
        );
        assert_eq!(rows[0]["tags"], json!(["streaming", "sql"]));
    }

    #[test]
    fn response_schema_preserves_null_for_feldera_nullable_output_types() {
        let rows = vec![json!({
            "string_value": null,
            "int_value": null,
            "float_value": null,
            "bool_value": null,
            "date_value": null,
            "time_value": null,
            "timestamp_value": null,
            "uuid_value": null,
            "decimal_value": null,
            "binary_value": null,
            "array_value": null,
            "object_value": null,
            "json_value": null
        })];
        let response_schema = MaterializedViewResponseSchema {
            columns: vec![
                ("string_value", "string"),
                ("int_value", "int64"),
                ("float_value", "float64"),
                ("bool_value", "boolean"),
                ("date_value", "date"),
                ("time_value", "time"),
                ("timestamp_value", "timestamp"),
                ("uuid_value", "uuid"),
                ("decimal_value", "decimal"),
                ("binary_value", "binary_hex"),
                ("array_value", "array"),
                ("object_value", "object"),
                ("json_value", "json"),
            ]
            .into_iter()
            .map(|(name, type_name)| MaterializedViewResponseColumnSpec {
                name: name.to_string(),
                r#type: type_name.to_string(),
                source: name.to_string(),
                description: None,
            })
            .collect(),
        };

        let rows = materialized_rows_to_api_rows(&rows, &response_schema).unwrap();

        for column in response_schema.columns {
            assert_eq!(rows[0].get(&column.name), Some(&Value::Null));
        }
    }

    #[test]
    fn response_schema_rejects_unknown_type_during_admission() {
        let api = MaterializedViewApiMetadata {
            response_schema: Some(MaterializedViewResponseSchema {
                columns: vec![MaterializedViewResponseColumnSpec {
                    name: "payload".to_string(),
                    r#type: "xml".to_string(),
                    source: "payload".to_string(),
                    description: None,
                }],
            }),
            ..MaterializedViewApiMetadata::default()
        };

        let error = validate_view_api_metadata(&api).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("response schema column `payload` declares unsupported type `xml`"),
            "error: {error}"
        );
    }

    #[test]
    fn sql_template_renderer_compiles_array_parameter_to_feldera_array_literal() {
        let field = MaterializedViewRequestFieldSpec {
            field_name: "scores".to_string(),
            field_in: "query".to_string(),
            r#type: "array".to_string(),
            validators: vec!["array(element=integer)".to_string()],
            default_value: None,
            description: None,
        };
        validate_request_field_contract(&field).unwrap();
        let template =
            "select user_id from scores where score in unnest({{ context.params.scores | is_required | is_array(element=integer) }})";
        let rendered = render_view_sql_template_as_feldera_sql(
            template,
            &[field],
            &BTreeMap::from([("scores".to_string(), json!([5, "7"]))]),
        )
        .unwrap();

        assert_eq!(
            rendered,
            "PREPARE velorix_query AS select user_id from scores where score IN ($1, $2);\nEXECUTE velorix_query(5, 7);"
        );
    }

    #[test]
    fn sql_template_renderer_compiles_json_request_field_to_canonical_feldera_string_literal() {
        let field = MaterializedViewRequestFieldSpec {
            field_name: "payload".to_string(),
            field_in: "query".to_string(),
            r#type: "json".to_string(),
            validators: vec!["required".to_string(), "json".to_string()],
            default_value: None,
            description: None,
        };
        validate_request_field_contract(&field).unwrap();
        let template =
            "select id from events where raw_json = {{ context.params.payload | is_required | is_json }}";
        let rendered = render_view_sql_template_as_feldera_sql(
            template,
            &[field],
            &BTreeMap::from([("payload".to_string(), json!({"flag": true, "count": 3}))]),
        )
        .unwrap();

        assert_eq!(
            rendered,
            "PREPARE velorix_query AS select id from events where raw_json = $1;\nEXECUTE velorix_query('{\"count\":3,\"flag\":true}');"
        );
    }

    #[test]
    fn sql_template_coverage_ignores_placeholders_inside_sql_strings_or_comments() {
        let api = MaterializedViewApiMetadata {
            request: vec![MaterializedViewRequestFieldSpec {
                field_name: "user_id".to_string(),
                field_in: "query".to_string(),
                r#type: "string".to_string(),
                validators: vec!["required".to_string()],
                default_value: None,
                description: None,
            }],
            sql_template: Some(
                "select '{{ context.params.user_id }}' as literal -- {{ context.params.user_id }}"
                    .to_string(),
            ),
            ..MaterializedViewApiMetadata::default()
        };

        let error =
            validate_sql_template_parameter_coverage(api.sql_template.as_deref().unwrap(), &api)
                .unwrap_err();

        assert!(
            error.to_string().contains("required parameter `user_id`"),
            "error: {error}"
        );
    }

    #[test]
    fn promoted_api_output_binding_is_required_for_multi_output_views() {
        let outputs = vec![
            test_relation_schema(
                "by_user",
                vec![test_feldera_column("user_id", SqlDataType::Int64)],
            ),
            test_relation_schema(
                "by_region",
                vec![test_feldera_column("region", SqlDataType::Utf8)],
            ),
        ];
        let api_without_output = MaterializedViewApiMetadata {
            url_path: Some("/scores/by-user".to_string()),
            ..MaterializedViewApiMetadata::default()
        };
        let api_with_output = MaterializedViewApiMetadata {
            url_path: Some("/scores/by-user".to_string()),
            output_relation_id: Some("by_user".to_string()),
            ..MaterializedViewApiMetadata::default()
        };

        assert!(
            validate_view_api_output_binding("score_program", &api_without_output, &outputs)
                .is_err()
        );
        validate_view_api_output_binding("score_program", &api_with_output, &outputs).unwrap();
    }

    #[test]
    fn create_view_request_accepts_output_relation_binding_aliases() {
        let camel: CreateViewRequest = serde_json::from_value(json!({
            "view_id": "score_program",
            "input_relation_id": "scores",
            "input_relation_version": "v1",
            "outputRelationId": "by_user",
            "sql": "select * from scores"
        }))
        .unwrap();
        let snake: CreateViewRequest = serde_json::from_value(json!({
            "view_id": "score_program",
            "input_relation_id": "scores",
            "input_relation_version": "v1",
            "output_relation_id": "by_user",
            "sql": "select * from scores"
        }))
        .unwrap();

        assert_eq!(camel.output_relation_id.as_deref(), Some("by_user"));
        assert_eq!(snake.output_relation_id.as_deref(), Some("by_user"));
    }

    #[test]
    fn standing_runtime_page_request_uses_query_policy_output_row_bound() {
        let unbounded = SnapshotPageRequest {
            committed_epoch: Some(7),
            page_token: None,
            max_rows: None,
        };
        let policy = QueryPolicy {
            max_output_rows: Some(10),
            ..QueryPolicy::default()
        };
        let bounded = page_request_with_query_policy_limit(unbounded, policy);

        assert_eq!(bounded.committed_epoch, Some(7));
        assert_eq!(bounded.max_rows, Some(11));

        let caller_bounded = page_request_with_query_policy_limit(
            SnapshotPageRequest {
                committed_epoch: None,
                page_token: Some("next".to_string()),
                max_rows: Some(5),
            },
            policy,
        );
        assert_eq!(caller_bounded.page_token.as_deref(), Some("next"));
        assert_eq!(caller_bounded.max_rows, Some(5));
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_reads_all_outputs_when_multi_output() {
        let program_info = json!({
            "schema": {
                "outputs": [
                    {
                        "name": "by_user",
                        "fields": [
                            { "name": "user_id", "columntype": "BIGINT" },
                            { "name": "total_score", "columntype": "BIGINT" }
                        ],
                        "primary_key": ["user_id"]
                    },
                    {
                        "name": "by_region",
                        "fields": [
                            { "name": "region", "columntype": "VARCHAR" },
                            { "name": "event_count", "columntype": "BIGINT" }
                        ],
                        "primary_key": ["region"]
                    }
                ]
            }
        });

        let outputs =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap();

        assert_eq!(
            outputs
                .iter()
                .map(|schema| schema.relation_id.as_str())
                .collect::<Vec<_>>(),
            vec!["by_user", "by_region"]
        );
        assert_eq!(outputs[0].columns[0].data_type, SqlDataType::Int64);
        assert_eq!(outputs[1].columns[0].data_type, SqlDataType::Utf8);
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_keeps_single_output_contract() {
        let program_info = json!({
            "schema": {
                "outputs": [
                    {
                        "name": "by_user",
                        "fields": [{ "name": "user_id", "columntype": "BIGINT" }],
                        "primary_key": ["user_id"]
                    },
                    {
                        "name": "by_region",
                        "fields": [{ "name": "region", "columntype": "VARCHAR" }],
                        "primary_key": ["region"]
                    }
                ]
            }
        });

        let outputs =
            feldera_output_schemas_from_program_info("by_region", 7, Some(&program_info), false)
                .unwrap();

        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].relation_id, "by_region");
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_skip_non_materialized_multi_outputs() {
        let program_info = json!({
            "schema": {
                "outputs": [
                    {
                        "name": "error_view",
                        "materialized": false,
                        "fields": [{ "name": "message", "columntype": "VARCHAR" }],
                        "primary_key": []
                    },
                    {
                        "name": "by_user",
                        "materialized": true,
                        "fields": [{ "name": "user_id", "columntype": "BIGINT" }],
                        "primary_key": ["user_id"]
                    }
                ]
            }
        });

        let outputs =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap();

        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].relation_id, "by_user");
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_rejects_non_materialized_single_output() {
        let program_info = json!({
            "schema": {
                "outputs": [{
                    "name": "by_user",
                    "materialized": false,
                    "fields": [{ "name": "user_id", "columntype": "BIGINT" }],
                    "primary_key": ["user_id"]
                }]
            }
        });

        let error =
            feldera_output_schemas_from_program_info("by_user", 7, Some(&program_info), false)
                .unwrap_err();

        assert!(error
            .to_string()
            .contains("Feldera output view `by_user` is not materialized"));
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_rejects_connector_bearing_outputs() {
        let program_info = json!({
            "schema": {
                "outputs": [{
                    "name": "by_user",
                    "materialized": true,
                    "fields": [{ "name": "user_id", "columntype": "BIGINT" }],
                    "primary_key": ["user_id"],
                    "properties": {
                        "connectors": [{ "name": "sink" }]
                    }
                }]
            }
        });

        let error =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap_err();

        assert!(error.to_string().contains(
            "Feldera output view `by_user` contains unmanaged connector/external IO properties"
        ));
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_rejects_duplicate_output_names() {
        let program_info = json!({
            "schema": {
                "outputs": [
                    {
                        "name": "by_user",
                        "fields": [{ "name": "user_id", "columntype": "BIGINT" }],
                        "primary_key": ["user_id"]
                    },
                    {
                        "name": "by_user",
                        "fields": [{ "name": "region", "columntype": "VARCHAR" }],
                        "primary_key": ["region"]
                    }
                ]
            }
        });

        let error =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap_err();

        assert!(error
            .to_string()
            .contains("Feldera compiled program contains duplicate output view `by_user`"));
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_rejects_case_insensitive_duplicate_output_names() {
        let program_info = json!({
            "schema": {
                "outputs": [
                    {
                        "name": "by_user",
                        "case_sensitive": false,
                        "fields": [{ "name": "user_id", "columntype": "BIGINT" }],
                        "primary_key": ["user_id"]
                    },
                    {
                        "name": "BY_USER",
                        "case_sensitive": false,
                        "fields": [{ "name": "region", "columntype": "VARCHAR" }],
                        "primary_key": ["region"]
                    }
                ]
            }
        });

        let error =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap_err();

        assert!(error
            .to_string()
            .contains("Feldera compiled program contains duplicate output view `BY_USER`"));
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_matches_case_insensitive_single_output() {
        let program_info = json!({
            "schema": {
                "outputs": [{
                    "name": "BY_USER",
                    "case_sensitive": false,
                    "fields": [{ "name": "user_id", "columntype": "BIGINT" }],
                    "primary_key": ["user_id"]
                }]
            }
        });

        let outputs =
            feldera_output_schemas_from_program_info("by_user", 7, Some(&program_info), false)
                .unwrap();

        assert_eq!(outputs[0].relation_id, "BY_USER");
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_rejects_blank_output_field_name() {
        let program_info = json!({
            "schema": {
                "outputs": [{
                    "name": "by_user",
                    "fields": [{ "name": " ", "columntype": "BIGINT" }],
                    "primary_key": []
                }]
            }
        });

        let error =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap_err();

        assert!(error
            .to_string()
            .contains("Feldera output view `by_user` contains a blank field name"));
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_rejects_duplicate_output_field_names() {
        let program_info = json!({
            "schema": {
                "outputs": [{
                    "name": "by_user",
                    "fields": [
                        { "name": "user_id", "columntype": "BIGINT" },
                        { "name": "user_id", "columntype": "VARCHAR" }
                    ],
                    "primary_key": []
                }]
            }
        });

        let error =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap_err();

        assert!(error
            .to_string()
            .contains("Feldera output view `by_user` contains duplicate field `user_id`"));
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_rejects_case_insensitive_duplicate_output_field_names(
    ) {
        let program_info = json!({
            "schema": {
                "outputs": [{
                    "name": "by_user",
                    "case_sensitive": false,
                    "fields": [
                        { "name": "user_id", "columntype": "BIGINT" },
                        { "name": "USER_ID", "columntype": "VARCHAR" }
                    ],
                    "primary_key": []
                }]
            }
        });

        let error =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap_err();

        assert!(error
            .to_string()
            .contains("Feldera output view `by_user` contains duplicate field `USER_ID`"));
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_rejects_non_array_primary_key() {
        let program_info = json!({
            "schema": {
                "outputs": [{
                    "name": "by_user",
                    "fields": [{ "name": "user_id", "columntype": "BIGINT" }],
                    "primary_key": "user_id"
                }]
            }
        });

        let error =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap_err();

        assert!(error
            .to_string()
            .contains("Feldera output view `by_user` primary_key must be an array"));
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_rejects_non_string_primary_key_entry() {
        let program_info = json!({
            "schema": {
                "outputs": [{
                    "name": "by_user",
                    "fields": [{ "name": "user_id", "columntype": "BIGINT" }],
                    "primary_key": [7]
                }]
            }
        });

        let error =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap_err();

        assert!(error
            .to_string()
            .contains("Feldera output view `by_user` primary_key entry 0 must be a string"));
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_rejects_blank_primary_key_entry() {
        let program_info = json!({
            "schema": {
                "outputs": [{
                    "name": "by_user",
                    "fields": [{ "name": "user_id", "columntype": "BIGINT" }],
                    "primary_key": [" "]
                }]
            }
        });

        let error =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap_err();

        assert!(error
            .to_string()
            .contains("Feldera output view `by_user` contains a blank primary_key entry"));
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_rejects_unknown_primary_key_field() {
        let program_info = json!({
            "schema": {
                "outputs": [{
                    "name": "by_user",
                    "fields": [{ "name": "user_id", "columntype": "BIGINT" }],
                    "primary_key": ["missing_id"]
                }]
            }
        });

        let error =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap_err();

        assert!(error.to_string().contains(
            "Feldera output view `by_user` primary_key entry `missing_id` does not reference a field"
        ));
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_rejects_duplicate_primary_key_entry() {
        let program_info = json!({
            "schema": {
                "outputs": [{
                    "name": "by_user",
                    "case_sensitive": false,
                    "fields": [{ "name": "user_id", "columntype": "BIGINT" }],
                    "primary_key": ["user_id", "USER_ID"]
                }]
            }
        });

        let error =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap_err();

        assert!(error.to_string().contains(
            "Feldera output view `by_user` contains duplicate primary_key entry `USER_ID`"
        ));
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_canonicalizes_case_insensitive_primary_key() {
        let program_info = json!({
            "schema": {
                "outputs": [{
                    "name": "by_user",
                    "case_sensitive": false,
                    "fields": [{ "name": "user_id", "columntype": "BIGINT" }],
                    "primary_key": ["USER_ID"]
                }]
            }
        });

        let outputs =
            feldera_output_schemas_from_program_info("score_program", 7, Some(&program_info), true)
                .unwrap();

        assert_eq!(outputs[0].primary_key, vec!["user_id"]);
    }

    #[test]
    fn feldera_pipeline_manager_output_schemas_accept_feldera_type_aliases() {
        let program_info = json!({
            "schema": {
                "outputs": [{
                    "name": "wide_output",
                    "fields": [
                        { "name": "small_alias", "columntype": "INT2" },
                        { "name": "signed_alias", "columntype": "SIGNED" },
                        { "name": "big_alias", "columntype": "INT64" },
                        { "name": "real_alias", "columntype": "FLOAT32" },
                        { "name": "double_alias", "columntype": "FLOAT8" },
                        { "name": "bytes_alias", "columntype": "BINARY VARYING" },
                        { "name": "timestamp_alias", "columntype": "DATETIME" },
                        { "name": "shape", "columntype": { "type": "GEOMETRY", "nullable": true } }
                    ],
                    "primary_key": ["small_alias"]
                }]
            }
        });

        let outputs =
            feldera_output_schemas_from_program_info("wide_output", 11, Some(&program_info), false)
                .unwrap();
        let columns = &outputs[0].columns;

        assert_eq!(columns[0].data_type, SqlDataType::Int16);
        assert_eq!(columns[1].data_type, SqlDataType::Int32);
        assert_eq!(columns[2].data_type, SqlDataType::Int64);
        assert_eq!(columns[3].data_type, SqlDataType::Float32);
        assert_eq!(columns[4].data_type, SqlDataType::Float64);
        assert_eq!(columns[5].data_type, SqlDataType::Varbinary);
        assert_eq!(
            columns[6].data_type,
            SqlDataType::Timestamp { timezone: None }
        );
        assert_eq!(columns[7].data_type, SqlDataType::Geometry);
        assert!(columns[7].nullable);
    }

    #[test]
    fn feldera_program_info_admission_accepts_registered_inputs() {
        let request = test_feldera_program_compile_request(vec![test_relation_schema(
            "scores",
            vec![test_feldera_column("score", SqlDataType::Int64)],
        )]);
        let program_info = json!({
            "schema": {
                "inputs": [{
                    "name": "scores",
                    "fields": [{ "name": "score", "columntype": "BIGINT" }],
                    "primary_key": []
                }],
                "outputs": [{
                    "name": "scores_by_user",
                    "fields": [{ "name": "score", "columntype": "BIGINT" }],
                    "primary_key": []
                }]
            }
        });

        validate_feldera_program_info_admission(&request, Some(&program_info)).unwrap();
    }

    #[test]
    fn feldera_program_info_admission_accepts_case_insensitive_registered_input() {
        let request = test_feldera_program_compile_request(vec![test_relation_schema(
            "scores",
            vec![test_feldera_column("score", SqlDataType::Int64)],
        )]);
        let program_info = json!({
            "schema": {
                "inputs": [{
                    "name": "SCORES",
                    "case_sensitive": false,
                    "fields": [{ "name": "score", "columntype": "BIGINT" }],
                    "primary_key": []
                }],
                "outputs": [{
                    "name": "scores_by_user",
                    "fields": [{ "name": "score", "columntype": "BIGINT" }],
                    "primary_key": []
                }]
            }
        });

        validate_feldera_program_info_admission(&request, Some(&program_info)).unwrap();
    }

    #[test]
    fn feldera_program_info_admission_accepts_case_insensitive_registered_input_schema() {
        let request =
            test_feldera_program_compile_request(vec![test_relation_schema_with_primary_key(
                "scores",
                vec![
                    test_feldera_column("user_id", SqlDataType::Utf8),
                    test_feldera_column("score", SqlDataType::Int64),
                ],
                vec!["user_id"],
            )]);
        let program_info = json!({
            "schema": {
                "inputs": [{
                    "name": "SCORES",
                    "case_sensitive": false,
                    "fields": [
                        { "name": "USER_ID", "case_sensitive": false, "columntype": "VARCHAR" },
                        { "name": "SCORE", "case_sensitive": false, "columntype": "BIGINT" }
                    ],
                    "primary_key": ["USER_ID"]
                }],
                "outputs": [{
                    "name": "scores_by_user",
                    "fields": [{ "name": "score", "columntype": "BIGINT" }],
                    "primary_key": []
                }]
            }
        });

        validate_feldera_program_info_admission(&request, Some(&program_info)).unwrap();
    }

    #[test]
    fn feldera_program_info_admission_accepts_registered_input_column_projection() {
        let request = test_feldera_program_compile_request(vec![test_relation_schema(
            "scores",
            vec![
                test_feldera_column("user_id", SqlDataType::Utf8),
                test_feldera_column("score", SqlDataType::Int64),
            ],
        )]);
        let program_info = json!({
            "schema": {
                "inputs": [{
                    "name": "scores",
                    "fields": [{ "name": "score", "columntype": "BIGINT" }],
                    "primary_key": []
                }],
                "outputs": [{
                    "name": "scores_by_user",
                    "fields": [{ "name": "score", "columntype": "BIGINT" }],
                    "primary_key": []
                }]
            }
        });

        validate_feldera_program_info_admission(&request, Some(&program_info)).unwrap();
    }

    #[test]
    fn feldera_program_info_admission_rejects_input_column_type_mismatch() {
        let request = test_feldera_program_compile_request(vec![test_relation_schema(
            "scores",
            vec![test_feldera_column("score", SqlDataType::Int64)],
        )]);
        let program_info = json!({
            "schema": {
                "inputs": [{
                    "name": "scores",
                    "fields": [{ "name": "score", "columntype": "VARCHAR" }],
                    "primary_key": []
                }],
                "outputs": [{
                    "name": "scores_by_user",
                    "fields": [{ "name": "score", "columntype": "BIGINT" }],
                    "primary_key": []
                }]
            }
        });

        let error =
            validate_feldera_program_info_admission(&request, Some(&program_info)).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            error.message.contains("column `score` type does not match"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn feldera_program_info_admission_rejects_input_primary_key_mismatch() {
        let request =
            test_feldera_program_compile_request(vec![test_relation_schema_with_primary_key(
                "scores",
                vec![
                    test_feldera_column("user_id", SqlDataType::Utf8),
                    test_feldera_column("score", SqlDataType::Int64),
                ],
                vec!["user_id"],
            )]);
        let program_info = json!({
            "schema": {
                "inputs": [{
                    "name": "scores",
                    "fields": [
                        { "name": "user_id", "columntype": "VARCHAR" },
                        { "name": "score", "columntype": "BIGINT" }
                    ],
                    "primary_key": ["score"]
                }],
                "outputs": [{
                    "name": "scores_by_user",
                    "fields": [{ "name": "score", "columntype": "BIGINT" }],
                    "primary_key": []
                }]
            }
        });

        let error =
            validate_feldera_program_info_admission(&request, Some(&program_info)).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            error.message.contains("primary_key does not match"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn feldera_program_info_admission_rejects_case_insensitive_duplicate_input_relation() {
        let request = test_feldera_program_compile_request(vec![test_relation_schema(
            "scores",
            vec![test_feldera_column("score", SqlDataType::Int64)],
        )]);
        let program_info = json!({
            "schema": {
                "inputs": [
                    {
                        "name": "scores",
                        "case_sensitive": false,
                        "fields": [{ "name": "score", "columntype": "BIGINT" }],
                        "primary_key": []
                    },
                    {
                        "name": "SCORES",
                        "case_sensitive": false,
                        "fields": [{ "name": "score", "columntype": "BIGINT" }],
                        "primary_key": []
                    }
                ],
                "outputs": [{
                    "name": "scores_by_user",
                    "fields": [{ "name": "score", "columntype": "BIGINT" }],
                    "primary_key": []
                }]
            }
        });

        let error =
            validate_feldera_program_info_admission(&request, Some(&program_info)).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            error.message.contains("duplicate input relation `SCORES`"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn feldera_program_info_admission_rejects_unregistered_input_relation() {
        let request = test_feldera_program_compile_request(vec![test_relation_schema(
            "scores",
            vec![test_feldera_column("score", SqlDataType::Int64)],
        )]);
        let program_info = json!({
            "schema": {
                "inputs": [
                    {
                        "name": "scores",
                        "fields": [{ "name": "score", "columntype": "BIGINT" }],
                        "primary_key": []
                    },
                    {
                        "name": "external_scores",
                        "fields": [{ "name": "payload", "columntype": "VARCHAR" }],
                        "primary_key": []
                    }
                ],
                "outputs": [{
                    "name": "scores_by_user",
                    "fields": [{ "name": "score", "columntype": "BIGINT" }],
                    "primary_key": []
                }]
            }
        });

        let error =
            validate_feldera_program_info_admission(&request, Some(&program_info)).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            error
                .message
                .contains("unregistered input relation `external_scores`"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn feldera_program_info_admission_rejects_connector_bearing_input_relation() {
        let request = test_feldera_program_compile_request(vec![test_relation_schema(
            "scores",
            vec![test_feldera_column("score", SqlDataType::Int64)],
        )]);
        let program_info = json!({
            "schema": {
                "inputs": [{
                    "name": "scores",
                    "fields": [{ "name": "score", "columntype": "BIGINT" }],
                    "primary_key": [],
                    "properties": {
                        "connectors": "kafka://scores"
                    }
                }],
                "outputs": [{
                    "name": "scores_by_user",
                    "fields": [{ "name": "score", "columntype": "BIGINT" }],
                    "primary_key": []
                }]
            }
        });

        let error =
            validate_feldera_program_info_admission(&request, Some(&program_info)).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            error
                .message
                .contains("unmanaged connector/external IO properties"),
            "unexpected error message: {}",
            error.message
        );
        assert!(
            error.message.contains("properties.connectors"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn feldera_query_rows_preserves_row_fields_named_like_envelopes() {
        let rows = feldera_query_rows_from_text(
            r#"{"rows":[1,2],"data":{"nested":true},"records":"literal"}"#,
            None,
        )
        .unwrap();

        assert_eq!(
            rows,
            vec![json!({
                "rows": [1, 2],
                "data": { "nested": true },
                "records": "literal"
            })]
        );
    }

    #[test]
    fn feldera_query_rows_accepts_ndjson_insert_rows() {
        let rows = feldera_query_rows_from_text(
            "{\"insert\":{\"user_id\":\"u1\",\"score\":10}}\n{\"insert\":{\"user_id\":\"u2\",\"score\":20}}\n",
            None,
        )
        .unwrap();

        assert_eq!(
            rows,
            vec![
                json!({ "user_id": "u1", "score": 10 }),
                json!({ "user_id": "u2", "score": 20 })
            ]
        );
    }

    #[test]
    fn feldera_query_rows_preserves_insert_field_when_output_schema_declares_it() {
        let output_column_names = BTreeSet::from(["insert".to_string()]);
        let rows =
            feldera_query_rows_from_text(r#"[{"insert":"literal"}]"#, Some(&output_column_names))
                .unwrap();

        assert_eq!(rows, vec![json!({ "insert": "literal" })]);
    }

    #[test]
    fn feldera_query_rows_preserves_delete_field_when_output_schema_declares_it() {
        let output_column_names = BTreeSet::from(["delete".to_string()]);
        let rows =
            feldera_query_rows_from_text(r#"[{"delete":"literal"}]"#, Some(&output_column_names))
                .unwrap();

        assert_eq!(rows, vec![json!({ "delete": "literal" })]);
    }

    #[test]
    fn feldera_query_rows_preserves_insert_named_field_when_row_has_other_fields() {
        let rows =
            feldera_query_rows_from_text(r#"[{"insert":"literal","value":7}]"#, None).unwrap();

        assert_eq!(rows, vec![json!({ "insert": "literal", "value": 7 })]);
    }

    #[test]
    fn feldera_query_rows_preserves_schema_less_scalar_insert_and_delete_fields() {
        let rows =
            feldera_query_rows_from_text(r#"[{"insert":"literal"},{"delete":"literal"}]"#, None)
                .unwrap();

        assert_eq!(
            rows,
            vec![
                json!({ "insert": "literal" }),
                json!({ "delete": "literal" })
            ]
        );
    }

    #[test]
    fn view_query_output_resolution_requires_explicit_output_for_multi_output_without_default() {
        let active = test_active_view_with_outputs(
            "score_program",
            vec![
                test_relation_schema(
                    "by_user",
                    vec![test_feldera_column("user_id", SqlDataType::Int64)],
                ),
                test_relation_schema(
                    "by_region",
                    vec![test_feldera_column("region", SqlDataType::Utf8)],
                ),
            ],
        );

        assert!(resolve_view_query_output_id(&active, None).is_err());
        assert_eq!(
            resolve_view_query_output_id(&active, Some("by_region")).unwrap(),
            "by_region"
        );
        assert!(resolve_view_query_output_id(&active, Some("missing")).is_err());
    }

    #[test]
    fn standing_program_identity_includes_multi_output_relation_ids() {
        let spec = test_standing_view_spec_with_outputs(
            "score_program",
            vec![
                test_relation_schema(
                    "by_user",
                    vec![test_feldera_column("user_id", SqlDataType::Int64)],
                ),
                test_relation_schema(
                    "by_region",
                    vec![test_feldera_column("region", SqlDataType::Utf8)],
                ),
            ],
        );

        assert_eq!(
            standing_program_view_ids_for_spec(&spec),
            vec!["score_program", "by_user", "by_region"]
        );
    }

    #[test]
    fn resolved_compile_spec_rejects_timezone_bearing_output_timestamp() {
        let pending_spec = test_standing_view_spec_with_outputs(
            "tz_view",
            vec![test_relation_schema(
                "tz_view",
                vec![test_feldera_column("event_ts", SqlDataType::Utf8)],
            )],
        );
        let mut resolved_spec = pending_spec.clone();
        resolved_spec.output_relations = vec![test_relation_schema(
            "tz_view",
            vec![test_feldera_column(
                "event_ts",
                SqlDataType::Timestamp {
                    timezone: Some("UTC".to_string()),
                },
            )],
        )];
        resolved_spec.shape.multi_output = false;
        let compile_request_hash = compile_request_hash_for_spec(&pending_spec).unwrap();

        let error =
            validate_resolved_compile_spec(&pending_spec, &resolved_spec, &compile_request_hash)
                .unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            error
                .message
                .contains("timezone-bearing timestamps are not supported"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn resolved_compile_spec_rejects_feldera_program_output_hint_mismatch() {
        let mut pending_spec = test_standing_view_spec_with_outputs(
            "score_program",
            vec![
                test_relation_schema(
                    "by_user",
                    vec![test_feldera_column("user_id", SqlDataType::Utf8)],
                ),
                test_relation_schema(
                    "by_region",
                    vec![test_feldera_column("region", SqlDataType::Utf8)],
                ),
            ],
        );
        pending_spec.source_kind = SqlSourceKind::FelderaProgram;
        pending_spec.shape.multi_output = true;
        let mut resolved_spec = pending_spec.clone();
        resolved_spec.output_relations = vec![
            test_relation_schema(
                "by_user",
                vec![test_feldera_column("user_id", SqlDataType::Utf8)],
            ),
            test_relation_schema(
                "unexpected",
                vec![test_feldera_column("region", SqlDataType::Utf8)],
            ),
        ];
        let compile_request_hash = compile_request_hash_for_spec(&pending_spec).unwrap();

        let error =
            validate_resolved_compile_spec(&pending_spec, &resolved_spec, &compile_request_hash)
                .unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            error
                .message
                .contains("resolved Feldera program output relations do not match requested output_relation_ids"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn resolved_compile_spec_allows_case_insensitive_feldera_program_output_hints() {
        let mut pending_spec = test_standing_view_spec_with_outputs(
            "score_program",
            vec![
                test_relation_schema(
                    "by_user",
                    vec![test_feldera_column("user_id", SqlDataType::Utf8)],
                ),
                test_relation_schema(
                    "by_region",
                    vec![test_feldera_column("region", SqlDataType::Utf8)],
                ),
            ],
        );
        pending_spec.source_kind = SqlSourceKind::FelderaProgram;
        pending_spec.shape.multi_output = true;
        let mut resolved_spec = pending_spec.clone();
        resolved_spec.output_relations = vec![
            test_relation_schema(
                "BY_USER",
                vec![test_feldera_column("user_id", SqlDataType::Utf8)],
            ),
            test_relation_schema(
                "BY_REGION",
                vec![test_feldera_column("region", SqlDataType::Utf8)],
            ),
        ];
        let compile_request_hash = compile_request_hash_for_spec(&pending_spec).unwrap();

        validate_resolved_compile_spec(&pending_spec, &resolved_spec, &compile_request_hash)
            .unwrap();
    }

    #[test]
    fn resolved_compile_spec_preserves_pending_output_id_for_case_insensitive_hints() {
        let mut pending_spec = test_standing_view_spec_with_outputs(
            "score_program",
            vec![test_relation_schema(
                "by_user",
                vec![test_feldera_column("user_id", SqlDataType::Utf8)],
            )],
        );
        pending_spec.source_kind = SqlSourceKind::FelderaProgram;
        let mut resolved_spec = pending_spec.clone();
        resolved_spec.output_relations = vec![test_relation_schema(
            "BY_USER",
            vec![test_feldera_column("user_id", SqlDataType::Utf8)],
        )];
        resolved_spec.shape.multi_output = false;
        let resolved_spec =
            resolved_compile_spec_with_pending_output_relation_ids(&pending_spec, resolved_spec);
        let compile_request_hash = compile_request_hash_for_spec(&pending_spec).unwrap();

        validate_resolved_compile_spec(&pending_spec, &resolved_spec, &compile_request_hash)
            .unwrap();
        assert_eq!(resolved_spec.output_relations[0].relation_id, "by_user");
        assert_eq!(resolved_spec.output_relations[0].relation_name, "BY_USER");
    }

    #[test]
    fn feldera_program_output_hint_matching_rejects_case_folded_ambiguity() {
        let expected = BTreeSet::from(["by_user", "by_region"]);
        let actual = BTreeSet::from(["BY_USER", "by_user"]);

        assert!(!feldera_output_hint_relation_ids_match(&expected, &actual));
    }

    #[test]
    fn resolved_compile_spec_allows_feldera_program_output_discovery_without_hints() {
        let mut pending_spec = test_standing_view_spec_with_outputs(
            "score_program",
            vec![test_relation_schema(
                "score_program",
                vec![test_feldera_column("placeholder", SqlDataType::Utf8)],
            )],
        );
        pending_spec.source_kind = SqlSourceKind::FelderaProgram;
        pending_spec.shape.multi_output = false;
        let mut resolved_spec = pending_spec.clone();
        resolved_spec.output_relations = vec![
            test_relation_schema(
                "by_user",
                vec![test_feldera_column("user_id", SqlDataType::Utf8)],
            ),
            test_relation_schema(
                "by_region",
                vec![test_feldera_column("region", SqlDataType::Utf8)],
            ),
        ];
        resolved_spec.shape.multi_output = true;
        let compile_request_hash = compile_request_hash_for_spec(&pending_spec).unwrap();

        validate_resolved_compile_spec(&pending_spec, &resolved_spec, &compile_request_hash)
            .unwrap();
    }

    #[test]
    fn feldera_runtime_admission_rejects_nested_timezone_bearing_timestamps() {
        let spec = test_standing_view_spec_with_outputs(
            "nested_tz_view",
            vec![test_relation_schema(
                "nested_tz_view",
                vec![test_feldera_column(
                    "profile",
                    SqlDataType::Struct {
                        fields: vec![SqlStructField {
                            name: "created_at".to_string(),
                            data_type: SqlDataType::Timestamp {
                                timezone: Some("UTC".to_string()),
                            },
                            nullable: false,
                        }],
                    },
                )],
            )],
        );

        let error = validate_feldera_runtime_spec_admission(&spec).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            error
                .message
                .contains("spec.output_relations.nested_tz_view.profile.created_at"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn feldera_query_conversion_accepts_supported_scalar_result_types() {
        let schema = RelationSchema {
            relation_id: "scalar_view".to_string(),
            relation_name: "scalar_view".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: "scalar-fingerprint".to_string(),
            columns: vec![
                test_feldera_column("tiny", SqlDataType::Int8),
                test_feldera_column("small", SqlDataType::Int16),
                test_feldera_column("int_value", SqlDataType::Int32),
                test_feldera_column("big", SqlDataType::Int64),
                test_feldera_column("utiny", SqlDataType::UInt8),
                test_feldera_column("usmall", SqlDataType::UInt16),
                test_feldera_column("uint_value", SqlDataType::UInt32),
                test_feldera_column("ubig", SqlDataType::UInt64),
                test_feldera_column("real_value", SqlDataType::Float32),
                test_feldera_column("double_value", SqlDataType::Float64),
                test_feldera_column("char_value", SqlDataType::Char { length: Some(8) }),
                test_feldera_column("uuid_value", SqlDataType::Uuid),
                test_feldera_column("geometry_value", SqlDataType::Geometry),
                test_feldera_column("binary_value", SqlDataType::Binary { length: 3 }),
                test_feldera_column("varbinary_value", SqlDataType::Varbinary),
                test_feldera_column("date_value", SqlDataType::Date),
                test_feldera_column("time_value", SqlDataType::Time),
                test_feldera_column("timestamp_value", SqlDataType::Timestamp { timezone: None }),
            ],
            primary_key: vec!["tiny".to_string()],
        };
        let rows = vec![json!({
            "tiny": -8,
            "small": -32000,
            "int_value": -123456,
            "big": -1234567890123_i64,
            "utiny": 8,
            "usmall": 65000,
            "uint_value": 4_000_000_000_u64,
            "ubig": 9_000_000_000_u64,
            "real_value": 1.25,
            "double_value": 9.5,
            "char_value": "ABCDEFGH",
            "uuid_value": "018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa",
            "geometry_value": "POINT(1 2)",
            "binary_value": "0x0A0bff",
            "varbinary_value": "0x",
            "date_value": "1970-01-02",
            "time_value": "01:02:03.004005006",
            "timestamp_value": "1970-01-02T03:04:05.006+02:00"
        })];

        let batch = feldera_rows_to_record_batch(&schema, &rows).unwrap();
        let arrow_schema = batch.schema();
        assert_eq!(
            arrow_schema.field_with_name("tiny").unwrap().data_type(),
            &DataType::Int8
        );
        assert_eq!(
            arrow_schema.field_with_name("ubig").unwrap().data_type(),
            &DataType::UInt64
        );
        assert_eq!(
            arrow_schema
                .field_with_name("real_value")
                .unwrap()
                .data_type(),
            &DataType::Float32
        );
        assert_eq!(
            arrow_schema
                .field_with_name("binary_value")
                .unwrap()
                .data_type(),
            &DataType::Binary
        );
        assert_eq!(
            arrow_schema
                .field_with_name("time_value")
                .unwrap()
                .data_type(),
            &DataType::Time64(TimeUnit::Nanosecond)
        );

        let rows = record_batches_to_json_rows(&[batch.clone()]).unwrap();
        assert_eq!(rows[0]["tiny"], json!(-8));
        assert_eq!(rows[0]["uint_value"], json!(4_000_000_000_u64));
        assert_eq!(rows[0]["ubig"], json!(9_000_000_000_u64));
        assert_eq!(rows[0]["char_value"], json!("ABCDEFGH"));
        assert_eq!(
            rows[0]["uuid_value"],
            json!("018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa")
        );
        assert_eq!(rows[0]["geometry_value"], json!("POINT(1 2)"));
        assert_eq!(rows[0]["binary_value"], json!("0x0a0bff"));
        assert_eq!(rows[0]["varbinary_value"], json!("0x"));
        assert_eq!(rows[0]["date_value"], json!(1));
        assert_eq!(rows[0]["time_value"], json!(3_723_004_005_006_i64));
        assert_eq!(rows[0]["timestamp_value"], json!(90_245_006_000_000_i64));

        let feldera_ingress_rows = record_batches_to_feldera_ingress_json_rows(&[batch]).unwrap();
        assert_eq!(
            feldera_ingress_rows[0]["binary_value"],
            json!([10, 11, 255])
        );
        assert_eq!(feldera_ingress_rows[0]["varbinary_value"], json!([]));
        assert_eq!(feldera_ingress_rows[0]["date_value"], json!("1970-01-02"));
        assert_eq!(
            feldera_ingress_rows[0]["time_value"],
            json!("01:02:03.004005006")
        );
        assert_eq!(
            feldera_ingress_rows[0]["timestamp_value"],
            json!("1970-01-02 01:04:05.006")
        );
    }

    #[test]
    fn relation_ingest_conversion_rejects_fixed_binary_length_mismatch() {
        let catalog = test_expanded_scalar_catalog();
        let rows = vec![json!({
            "id": "row-1",
            "i8_value": -8,
            "i16_value": -32000,
            "i32_value": -123456,
            "u8_value": 8,
            "u16_value": 65000,
            "u32_value": 4_000_000_000_u64,
            "u64_value": 9_000_000_000_u64,
            "f32_value": 1.25,
            "raw": "0x0a0b",
            "bytes": "0xdeadbeef",
            "event_time": "01:02:03.004005006",
            "event_date": "1970-01-02",
            "event_ts": "1970-01-02 01:04:05.006",
            "uuid_value": "018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa",
            "amount": 10,
            "weight": 1
        })];

        let error = rows_to_record_batch(&catalog, &rows).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            error
                .message
                .contains("row.raw must contain exactly 3 bytes"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn feldera_query_conversion_rejects_fixed_binary_length_mismatch() {
        let schema = RelationSchema {
            relation_id: "binary_view".to_string(),
            relation_name: "binary_view".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: "binary-fingerprint".to_string(),
            columns: vec![
                test_feldera_column("raw", SqlDataType::Binary { length: 3 }),
                test_feldera_column("bytes", SqlDataType::Varbinary),
            ],
            primary_key: vec!["raw".to_string()],
        };
        let rows = vec![json!({
            "raw": "0x0a0b",
            "bytes": "0x0a0b"
        })];

        let error = feldera_rows_to_record_batch(&schema, &rows).unwrap_err();

        assert!(
            error.contains("column `raw` must contain exactly 3 bytes"),
            "unexpected error message: {error}"
        );
    }

    #[test]
    fn feldera_query_conversion_accepts_binary_byte_array_values() {
        let schema = RelationSchema {
            relation_id: "binary_view".to_string(),
            relation_name: "binary_view".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: "binary-fingerprint".to_string(),
            columns: vec![
                test_feldera_column("raw", SqlDataType::Binary { length: 3 }),
                test_feldera_column("bytes", SqlDataType::Varbinary),
            ],
            primary_key: vec!["raw".to_string()],
        };
        let rows = vec![json!({
            "raw": [10, 11, 255],
            "bytes": []
        })];

        let batch = feldera_rows_to_record_batch(&schema, &rows).unwrap();
        let rows = record_batches_to_json_rows(&[batch]).unwrap();

        assert_eq!(rows[0]["raw"], json!("0x0a0bff"));
        assert_eq!(rows[0]["bytes"], json!("0x"));
    }

    #[test]
    fn feldera_query_conversion_accepts_decimal_number_values() {
        let schema = RelationSchema {
            relation_id: "decimal_view".to_string(),
            relation_name: "decimal_view".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: "decimal-fingerprint".to_string(),
            columns: vec![test_feldera_column(
                "amount",
                SqlDataType::Decimal {
                    precision: 6,
                    scale: 2,
                },
            )],
            primary_key: vec!["amount".to_string()],
        };
        let rows = vec![json!({ "amount": 12.34 })];

        let batch = feldera_rows_to_record_batch(&schema, &rows).unwrap();
        let rows = record_batches_to_json_rows(&[batch]).unwrap();

        assert_eq!(rows[0]["amount"], json!("12.34"));
    }

    #[test]
    fn relation_ingest_conversion_accepts_decimal_number_values() {
        let relation_schema = VelorixRelationSchemaV1 {
            relation_id: "decimal_events".to_string(),
            relation_name: "decimal_events".to_string(),
            relation_version: "2026-06-12.v1".to_string(),
            columns: vec![
                test_relation_column(
                    "id",
                    VelorixLogicalTypeV1::Utf8,
                    ArrowPhysicalTypeV1::Utf8,
                    RelationSemanticRoleV1::PrimaryKey,
                    0,
                ),
                test_relation_column(
                    "amount",
                    VelorixLogicalTypeV1::Decimal {
                        precision: 6,
                        scale: 2,
                    },
                    ArrowPhysicalTypeV1::Decimal128 {
                        precision: 6,
                        scale: 2,
                    },
                    RelationSemanticRoleV1::Value,
                    1,
                ),
                test_relation_column(
                    "weight",
                    VelorixLogicalTypeV1::Int64,
                    ArrowPhysicalTypeV1::Int64,
                    RelationSemanticRoleV1::Weight,
                    2,
                ),
            ],
            primary_key_column_ids: vec!["id".to_string()],
            weight_column_id: "weight".to_string(),
            allowed_operations: vec![RelationOperationV1::Insert],
            event_time_column_id: None,
        };
        let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)
            .expect("decimal relation schema must fingerprint");
        let catalog = VelorixRelationCatalogV1 {
            schema_version: RELATION_SCHEMA_VERSION_V1,
            relation_schema,
            schema_fingerprint: schema_fingerprint.clone(),
            datafusion_registration: DataFusionRegistrationV1 {
                name: "decimal_events".to_string(),
                mode: DataFusionRegistrationModeV1::Table,
            },
            feldera_relation: FelderaRelationBindingV1 {
                relation_id: "decimal_events".to_string(),
                schema_fingerprint,
            },
            incremental_adapter: IncrementalAdapterBindingV1 {
                adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
            },
        };
        let rows = vec![json!({
            "id": "d1",
            "amount": 12.34,
            "weight": 1
        })];

        let batch = rows_to_record_batch(&catalog, &rows).unwrap();
        let rows = record_batches_to_json_rows(&[batch]).unwrap();

        assert_eq!(rows[0]["amount"], json!("12.34"));
    }

    #[test]
    fn feldera_query_conversion_preserves_json_variant_result_values() {
        let schema = RelationSchema {
            relation_id: "json_view".to_string(),
            relation_name: "json_view".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: "json-fingerprint".to_string(),
            columns: vec![
                test_feldera_column("payload", SqlDataType::Json),
                test_feldera_column("label", SqlDataType::Utf8),
            ],
            primary_key: vec!["label".to_string()],
        };
        let rows = vec![json!({
            "payload": {
                "name": "Ada",
                "scores": [8, 13],
                "nested": { "active": true }
            },
            "label": "object"
        })];

        let batch = feldera_rows_to_record_batch(&schema, &rows).unwrap();
        let rows = record_batches_to_json_rows_for_feldera_schema(&schema, &[batch]).unwrap();

        assert_eq!(rows[0]["payload"]["name"], json!("Ada"));
        assert_eq!(rows[0]["payload"]["scores"], json!([8, 13]));
        assert_eq!(rows[0]["payload"]["nested"]["active"], json!(true));
        assert_eq!(rows[0]["label"], json!("object"));
    }

    #[test]
    fn feldera_query_conversion_preserves_json_variant_string_values() {
        let schema = RelationSchema {
            relation_id: "json_view".to_string(),
            relation_name: "json_view".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: "json-fingerprint".to_string(),
            columns: vec![test_feldera_column("payload", SqlDataType::Json)],
            primary_key: vec!["payload".to_string()],
        };
        let rows = vec![json!({ "payload": "plain-string" })];

        let batch = feldera_rows_to_record_batch(&schema, &rows).unwrap();
        let rows = record_batches_to_json_rows_for_feldera_schema(&schema, &[batch]).unwrap();

        assert_eq!(rows[0]["payload"], json!("plain-string"));
    }

    #[test]
    fn feldera_ingress_conversion_rejects_fixed_binary_length_mismatch() {
        let catalog = test_expanded_scalar_catalog();
        let schema = datafusion_schema_from_catalog(&catalog).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["row-1"])) as ArrayRef,
                Arc::new(Int8Array::from(vec![-8])) as ArrayRef,
                Arc::new(Int16Array::from(vec![-32000])) as ArrayRef,
                Arc::new(Int32Array::from(vec![-123456])) as ArrayRef,
                Arc::new(UInt8Array::from(vec![8])) as ArrayRef,
                Arc::new(UInt16Array::from(vec![65000])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![4_000_000_000_u32])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![9_000_000_000_u64])) as ArrayRef,
                Arc::new(Float32Array::from(vec![1.25])) as ArrayRef,
                Arc::new(BinaryArray::from_vec(vec![b"\x0a\x0b"])) as ArrayRef,
                Arc::new(BinaryArray::from_vec(vec![b"\xde\xad\xbe\xef"])) as ArrayRef,
                Arc::new(Time64NanosecondArray::from(vec![3_723_004_005_006_i64])) as ArrayRef,
                Arc::new(Date32Array::from(vec![1])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![90_245_006_000_000_i64])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    "018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa",
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            ],
        )
        .unwrap();

        let error = record_batches_to_feldera_ingress_json_rows_for_catalog(&catalog, &[batch])
            .unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            error
                .message
                .contains("Feldera ingress column `raw` must contain exactly 3 bytes"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn feldera_ingress_conversion_rejects_timezone_bearing_timestamps() {
        let timezone = Some(Arc::<str>::from("UTC"));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "event_ts",
                DataType::Timestamp(TimeUnit::Nanosecond, timezone.clone()),
                false,
            )])),
            vec![Arc::new(
                TimestampNanosecondArray::from(vec![0]).with_timezone_opt(timezone),
            )],
        )
        .unwrap();

        let error = record_batches_to_feldera_ingress_json_rows(&[batch]).unwrap_err();
        assert_eq!(
            error.status,
            StatusCode::BAD_REQUEST,
            "timezone-bearing timestamps must fail closed at Feldera ingress"
        );
        assert!(
            error
                .message
                .contains("timezone-bearing TimestampNanosecond"),
            "unexpected error message: {}",
            error.message
        );
    }

    #[test]
    fn feldera_ingress_conversion_emits_json_utf8_as_raw_variant_values() {
        let catalog = test_json_events_catalog();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("payload", DataType::Utf8, false),
                Field::new("raw_json", DataType::Utf8, false),
                Field::new("weight", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["j1"])),
                Arc::new(StringArray::from(vec![
                    r#"{"name":"Ada","scores":[8,13],"nested":{"active":true}}"#,
                ])),
                Arc::new(StringArray::from(vec![r#"{"flag":true,"count":3}"#])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap();

        let rows =
            record_batches_to_feldera_ingress_json_rows_for_catalog(&catalog, &[batch]).unwrap();

        assert_eq!(rows[0]["payload"]["name"], json!("Ada"));
        assert_eq!(rows[0]["payload"]["scores"], json!([8, 13]));
        assert_eq!(rows[0]["raw_json"], json!(r#"{"flag":true,"count":3}"#));
        assert_eq!(rows[0]["weight"], json!(1));
    }

    #[test]
    fn feldera_query_conversion_preserves_supported_complex_result_types() {
        let schema = RelationSchema {
            relation_id: "complex_view".to_string(),
            relation_name: "complex_view".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: "complex-fingerprint".to_string(),
            columns: vec![
                test_feldera_column(
                    "scores",
                    SqlDataType::Array {
                        element_type: Box::new(SqlDataType::Int32),
                    },
                ),
                test_feldera_column(
                    "profile",
                    SqlDataType::Struct {
                        fields: vec![
                            velorix_core::feldera_artifact::SqlStructField {
                                name: "name".to_string(),
                                data_type: SqlDataType::Utf8,
                                nullable: false,
                            },
                            velorix_core::feldera_artifact::SqlStructField {
                                name: "active".to_string(),
                                data_type: SqlDataType::Bool,
                                nullable: true,
                            },
                        ],
                    },
                ),
                test_feldera_column(
                    "labels",
                    SqlDataType::Map {
                        key_type: Box::new(SqlDataType::Utf8),
                        value_type: Box::new(SqlDataType::Int64),
                    },
                ),
                test_feldera_column(
                    "int_labels",
                    SqlDataType::Map {
                        key_type: Box::new(SqlDataType::Int32),
                        value_type: Box::new(SqlDataType::Utf8),
                    },
                ),
                test_feldera_column(
                    "interval_value",
                    SqlDataType::Interval {
                        unit: velorix_core::feldera_artifact::SqlIntervalUnit::DayToSecond,
                    },
                ),
                test_feldera_column("null_value", SqlDataType::Null),
            ],
            primary_key: vec!["profile".to_string()],
        };
        let rows = vec![json!({
            "scores": [1, null, 3],
            "profile": { "name": "ada", "active": true },
            "labels": { "critical": 9, "batch": null },
            "int_labels": { "1": "one", "2": null },
            "interval_value": "1 02:03:04",
            "null_value": null
        })];

        let batch = feldera_rows_to_record_batch(&schema, &rows).unwrap();
        assert!(matches!(
            batch.schema().field(0).data_type(),
            DataType::List(_)
        ));
        assert!(matches!(
            batch.schema().field(1).data_type(),
            DataType::Struct(_)
        ));
        assert!(matches!(
            batch.schema().field(2).data_type(),
            DataType::Map(_, _)
        ));
        assert!(matches!(
            batch.schema().field(3).data_type(),
            DataType::Map(_, _)
        ));
        assert_eq!(batch.schema().field(4).data_type(), &DataType::Utf8);
        assert_eq!(batch.schema().field(5).data_type(), &DataType::Null);

        let rows = record_batches_to_json_rows(&[batch]).unwrap();
        assert_eq!(rows[0]["scores"], json!([1, null, 3]));
        assert_eq!(rows[0]["profile"], json!({ "name": "ada", "active": true }));
        assert_eq!(rows[0]["labels"], json!({ "critical": 9, "batch": null }));
        assert_eq!(rows[0]["int_labels"], json!({ "1": "one", "2": null }));
        assert_eq!(rows[0]["interval_value"], json!("1 02:03:04"));
        assert_eq!(rows[0]["null_value"], Value::Null);
    }

    #[test]
    fn relation_ingest_conversion_accepts_expanded_scalar_input_types() {
        let catalog = test_expanded_scalar_catalog();
        let rows = vec![json!({
            "id": "row-1",
            "i8_value": -8,
            "i16_value": -32000,
            "i32_value": -123456,
            "u8_value": 8,
            "u16_value": 65000,
            "u32_value": 4_000_000_000_u64,
            "u64_value": 9_000_000_000_u64,
            "f32_value": 1.25,
            "raw": "0x0A0BFF",
            "bytes": "0x",
            "event_time": "01:02:03.004005006",
            "event_date": "2026-06-10",
            "event_ts": "2026-06-10T01:02:03.004005006",
            "uuid_value": "018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa",
            "amount": 42,
            "weight": 1
        })];

        let batch = rows_to_record_batch(&catalog, &rows).unwrap();
        assert_eq!(
            batch
                .schema()
                .field_with_name("i8_value")
                .unwrap()
                .data_type(),
            &DataType::Int8
        );
        assert_eq!(
            batch
                .schema()
                .field_with_name("u64_value")
                .unwrap()
                .data_type(),
            &DataType::UInt64
        );
        assert_eq!(
            batch
                .schema()
                .field_with_name("f32_value")
                .unwrap()
                .data_type(),
            &DataType::Float32
        );
        assert_eq!(
            batch.schema().field_with_name("raw").unwrap().data_type(),
            &DataType::Binary
        );
        assert_eq!(
            batch
                .schema()
                .field_with_name("event_time")
                .unwrap()
                .data_type(),
            &DataType::Time64(TimeUnit::Nanosecond)
        );
        assert_eq!(
            batch
                .schema()
                .field_with_name("event_date")
                .unwrap()
                .data_type(),
            &DataType::Date32
        );
        assert_eq!(
            batch
                .schema()
                .field_with_name("event_ts")
                .unwrap()
                .data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        let rows = record_batches_to_json_rows(&[batch.clone()]).unwrap();
        assert_eq!(rows[0]["i8_value"], json!(-8));
        assert_eq!(rows[0]["u32_value"], json!(4_000_000_000_u64));
        assert_eq!(rows[0]["u64_value"], json!(9_000_000_000_u64));
        assert_eq!(rows[0]["f32_value"], json!(1.25));
        assert_eq!(rows[0]["raw"], json!("0x0a0bff"));
        assert_eq!(rows[0]["bytes"], json!("0x"));
        assert_eq!(rows[0]["event_time"], json!(3_723_004_005_006_i64));
        assert_eq!(rows[0]["event_date"], json!(20_614));
        assert_eq!(rows[0]["event_ts"], json!(1_781_053_323_004_005_006_i64));
        assert_eq!(
            rows[0]["uuid_value"],
            json!("018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa")
        );

        let feldera_rows = record_batches_to_feldera_ingress_json_rows(&[batch]).unwrap();
        assert_eq!(feldera_rows[0]["raw"], json!([10, 11, 255]));
        assert_eq!(feldera_rows[0]["bytes"], json!([]));
        assert_eq!(feldera_rows[0]["event_time"], json!("01:02:03.004005006"));
        assert_eq!(feldera_rows[0]["event_date"], json!("2026-06-10"));
        assert_eq!(
            feldera_rows[0]["event_ts"],
            json!("2026-06-10 01:02:03.004005006")
        );
    }

    #[test]
    fn relation_ingest_conversion_accepts_nested_input_types() {
        let catalog = test_nested_input_catalog();
        let rows = vec![json!({
            "id": "row-1",
            "scores": [10, null, 30],
            "attributes": { "critical": 9, "batch": null },
            "profile": { "name": "ada", "tier": 2 },
            "amount": 42,
            "weight": 1
        })];

        let batch = rows_to_record_batch(&catalog, &rows).unwrap();
        assert!(matches!(
            batch
                .schema()
                .field_with_name("scores")
                .unwrap()
                .data_type(),
            DataType::List(_)
        ));
        assert!(matches!(
            batch
                .schema()
                .field_with_name("attributes")
                .unwrap()
                .data_type(),
            DataType::Map(_, _)
        ));
        assert!(matches!(
            batch
                .schema()
                .field_with_name("profile")
                .unwrap()
                .data_type(),
            DataType::Struct(_)
        ));

        let rows = record_batches_to_json_rows(&[batch]).unwrap();
        assert_eq!(rows[0]["scores"], json!([10, null, 30]));
        assert_eq!(
            rows[0]["attributes"],
            json!({ "critical": 9, "batch": null })
        );
        assert_eq!(rows[0]["profile"], json!({ "name": "ada", "tier": 2 }));
    }

    fn test_feldera_column(name: &str, data_type: SqlDataType) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
            data_type,
            nullable: false,
        }
    }

    fn test_relation_schema(relation_id: &str, columns: Vec<ColumnSchema>) -> RelationSchema {
        test_relation_schema_with_primary_key(relation_id, columns, Vec::<&str>::new())
    }

    fn test_relation_schema_with_primary_key(
        relation_id: &str,
        columns: Vec<ColumnSchema>,
        primary_key: Vec<&str>,
    ) -> RelationSchema {
        RelationSchema {
            relation_id: relation_id.to_string(),
            relation_name: relation_id.to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: feldera_artifact_bytes_hash(relation_id.as_bytes()),
            columns,
            primary_key: primary_key
                .into_iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        }
    }

    fn test_feldera_program_compile_request(
        input_relations: Vec<RelationSchema>,
    ) -> FelderaCompileRequestV1 {
        FelderaCompileRequestV1 {
            view_id: "scores_program".to_string(),
            sql: "CREATE MATERIALIZED VIEW scores_by_user AS SELECT score FROM scores".to_string(),
            dialect: SqlDialect::FelderaSql,
            source_kind: SqlSourceKind::FelderaProgram,
            rust_extension: Default::default(),
            input_relations,
            output_contract: OutputSchemaContract::Infer,
            shape: StandingViewShape {
                is_materialized: true,
                multi_input: false,
                multi_output: true,
            },
        }
    }

    fn test_expanded_scalar_catalog() -> VelorixRelationCatalogV1 {
        let columns = vec![
            test_relation_column(
                "id",
                VelorixLogicalTypeV1::Utf8,
                ArrowPhysicalTypeV1::Utf8,
                RelationSemanticRoleV1::PrimaryKey,
                0,
            ),
            test_relation_column(
                "i8_value",
                VelorixLogicalTypeV1::Int8,
                ArrowPhysicalTypeV1::Int8,
                RelationSemanticRoleV1::Metadata,
                1,
            ),
            test_relation_column(
                "i16_value",
                VelorixLogicalTypeV1::Int16,
                ArrowPhysicalTypeV1::Int16,
                RelationSemanticRoleV1::Metadata,
                2,
            ),
            test_relation_column(
                "i32_value",
                VelorixLogicalTypeV1::Int32,
                ArrowPhysicalTypeV1::Int32,
                RelationSemanticRoleV1::Metadata,
                3,
            ),
            test_relation_column(
                "u8_value",
                VelorixLogicalTypeV1::UInt8,
                ArrowPhysicalTypeV1::UInt8,
                RelationSemanticRoleV1::Metadata,
                4,
            ),
            test_relation_column(
                "u16_value",
                VelorixLogicalTypeV1::UInt16,
                ArrowPhysicalTypeV1::UInt16,
                RelationSemanticRoleV1::Metadata,
                5,
            ),
            test_relation_column(
                "u32_value",
                VelorixLogicalTypeV1::UInt32,
                ArrowPhysicalTypeV1::UInt32,
                RelationSemanticRoleV1::Metadata,
                6,
            ),
            test_relation_column(
                "u64_value",
                VelorixLogicalTypeV1::UInt64,
                ArrowPhysicalTypeV1::UInt64,
                RelationSemanticRoleV1::Metadata,
                7,
            ),
            test_relation_column(
                "f32_value",
                VelorixLogicalTypeV1::Float32,
                ArrowPhysicalTypeV1::Float32,
                RelationSemanticRoleV1::Metadata,
                8,
            ),
            test_relation_column(
                "raw",
                VelorixLogicalTypeV1::Binary { length: 3 },
                ArrowPhysicalTypeV1::Binary,
                RelationSemanticRoleV1::Metadata,
                9,
            ),
            test_relation_column(
                "bytes",
                VelorixLogicalTypeV1::Varbinary,
                ArrowPhysicalTypeV1::Binary,
                RelationSemanticRoleV1::Metadata,
                10,
            ),
            test_relation_column(
                "event_time",
                VelorixLogicalTypeV1::Time,
                ArrowPhysicalTypeV1::Time64Nanosecond,
                RelationSemanticRoleV1::Metadata,
                11,
            ),
            test_relation_column(
                "event_date",
                VelorixLogicalTypeV1::Date,
                ArrowPhysicalTypeV1::Date32,
                RelationSemanticRoleV1::Metadata,
                12,
            ),
            test_relation_column(
                "event_ts",
                VelorixLogicalTypeV1::Timestamp { timezone: None },
                ArrowPhysicalTypeV1::TimestampNanosecond { timezone: None },
                RelationSemanticRoleV1::Metadata,
                13,
            ),
            test_relation_column(
                "uuid_value",
                VelorixLogicalTypeV1::Uuid,
                ArrowPhysicalTypeV1::Utf8,
                RelationSemanticRoleV1::Metadata,
                14,
            ),
            test_relation_column(
                "amount",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                RelationSemanticRoleV1::Value,
                15,
            ),
            test_relation_column(
                "weight",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                RelationSemanticRoleV1::Weight,
                16,
            ),
        ];
        let relation_schema = VelorixRelationSchemaV1 {
            relation_id: "expanded_scalars".to_string(),
            relation_name: "expanded_scalars".to_string(),
            relation_version: "2026-06-09.v1".to_string(),
            columns,
            primary_key_column_ids: vec!["id".to_string()],
            weight_column_id: "weight".to_string(),
            allowed_operations: vec![RelationOperationV1::Insert],
            event_time_column_id: None,
        };
        let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)
            .expect("expanded scalar relation schema must fingerprint");
        VelorixRelationCatalogV1 {
            schema_version: RELATION_SCHEMA_VERSION_V1,
            relation_schema,
            schema_fingerprint: schema_fingerprint.clone(),
            datafusion_registration: DataFusionRegistrationV1 {
                name: "expanded_scalars".to_string(),
                mode: DataFusionRegistrationModeV1::Table,
            },
            feldera_relation: FelderaRelationBindingV1 {
                relation_id: "expanded_scalars".to_string(),
                schema_fingerprint,
            },
            incremental_adapter: IncrementalAdapterBindingV1 {
                adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
            },
        }
    }

    fn test_nested_input_catalog() -> VelorixRelationCatalogV1 {
        let columns = vec![
            test_relation_column(
                "id",
                VelorixLogicalTypeV1::Utf8,
                ArrowPhysicalTypeV1::Utf8,
                RelationSemanticRoleV1::PrimaryKey,
                0,
            ),
            test_relation_column(
                "scores",
                VelorixLogicalTypeV1::Array {
                    element_type: Box::new(VelorixLogicalTypeV1::Int64),
                },
                ArrowPhysicalTypeV1::List {
                    element_type: Box::new(ArrowPhysicalTypeV1::Int64),
                },
                RelationSemanticRoleV1::Metadata,
                1,
            ),
            test_relation_column(
                "attributes",
                VelorixLogicalTypeV1::Map {
                    key_type: Box::new(VelorixLogicalTypeV1::Utf8),
                    value_type: Box::new(VelorixLogicalTypeV1::Int64),
                },
                ArrowPhysicalTypeV1::Map {
                    key_type: Box::new(ArrowPhysicalTypeV1::Utf8),
                    value_type: Box::new(ArrowPhysicalTypeV1::Int64),
                },
                RelationSemanticRoleV1::Metadata,
                2,
            ),
            test_relation_column(
                "profile",
                VelorixLogicalTypeV1::Struct {
                    fields: vec![
                        VelorixStructFieldV1 {
                            name: "name".to_string(),
                            logical_type: VelorixLogicalTypeV1::Utf8,
                            nullable: false,
                        },
                        VelorixStructFieldV1 {
                            name: "tier".to_string(),
                            logical_type: VelorixLogicalTypeV1::Int32,
                            nullable: true,
                        },
                    ],
                },
                ArrowPhysicalTypeV1::Struct {
                    fields: vec![
                        ArrowStructFieldV1 {
                            name: "name".to_string(),
                            physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                            nullable: false,
                        },
                        ArrowStructFieldV1 {
                            name: "tier".to_string(),
                            physical_arrow_type: ArrowPhysicalTypeV1::Int32,
                            nullable: true,
                        },
                    ],
                },
                RelationSemanticRoleV1::Metadata,
                3,
            ),
            test_relation_column(
                "amount",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                RelationSemanticRoleV1::Value,
                4,
            ),
            test_relation_column(
                "weight",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                RelationSemanticRoleV1::Weight,
                5,
            ),
        ];
        let relation_schema = VelorixRelationSchemaV1 {
            relation_id: "nested_inputs".to_string(),
            relation_name: "nested_inputs".to_string(),
            relation_version: "2026-06-10.v1".to_string(),
            columns,
            primary_key_column_ids: vec!["id".to_string()],
            weight_column_id: "weight".to_string(),
            allowed_operations: vec![RelationOperationV1::Insert],
            event_time_column_id: None,
        };
        let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)
            .expect("nested input relation schema must fingerprint");
        VelorixRelationCatalogV1 {
            schema_version: RELATION_SCHEMA_VERSION_V1,
            relation_schema,
            schema_fingerprint: schema_fingerprint.clone(),
            datafusion_registration: DataFusionRegistrationV1 {
                name: "nested_inputs".to_string(),
                mode: DataFusionRegistrationModeV1::Table,
            },
            feldera_relation: FelderaRelationBindingV1 {
                relation_id: "nested_inputs".to_string(),
                schema_fingerprint,
            },
            incremental_adapter: IncrementalAdapterBindingV1 {
                adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
            },
        }
    }

    fn test_json_events_catalog() -> VelorixRelationCatalogV1 {
        let columns = vec![
            test_relation_column(
                "id",
                VelorixLogicalTypeV1::Utf8,
                ArrowPhysicalTypeV1::Utf8,
                RelationSemanticRoleV1::PrimaryKey,
                0,
            ),
            test_relation_column(
                "payload",
                VelorixLogicalTypeV1::Json,
                ArrowPhysicalTypeV1::JsonUtf8,
                RelationSemanticRoleV1::Metadata,
                1,
            ),
            test_relation_column(
                "raw_json",
                VelorixLogicalTypeV1::Utf8,
                ArrowPhysicalTypeV1::Utf8,
                RelationSemanticRoleV1::Metadata,
                2,
            ),
            test_relation_column(
                "weight",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                RelationSemanticRoleV1::Weight,
                3,
            ),
        ];
        let relation_schema = VelorixRelationSchemaV1 {
            relation_id: "json_events".to_string(),
            relation_name: "json_events".to_string(),
            relation_version: "2026-06-11.v1".to_string(),
            columns,
            primary_key_column_ids: vec!["id".to_string()],
            weight_column_id: "weight".to_string(),
            allowed_operations: vec![RelationOperationV1::Insert],
            event_time_column_id: None,
        };
        let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)
            .expect("JSON event relation schema must fingerprint");
        VelorixRelationCatalogV1 {
            schema_version: RELATION_SCHEMA_VERSION_V1,
            relation_schema,
            schema_fingerprint: schema_fingerprint.clone(),
            datafusion_registration: DataFusionRegistrationV1 {
                name: "json_events".to_string(),
                mode: DataFusionRegistrationModeV1::Table,
            },
            feldera_relation: FelderaRelationBindingV1 {
                relation_id: "json_events".to_string(),
                schema_fingerprint,
            },
            incremental_adapter: IncrementalAdapterBindingV1 {
                adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
            },
        }
    }

    fn test_relation_column(
        name: &str,
        logical_type: VelorixLogicalTypeV1,
        physical_arrow_type: ArrowPhysicalTypeV1,
        semantic_role: RelationSemanticRoleV1,
        ordinal: u32,
    ) -> RelationColumnV1 {
        RelationColumnV1 {
            column_id: name.to_string(),
            name: name.to_string(),
            logical_type,
            physical_arrow_type,
            nullable: false,
            ordinal,
            semantic_role,
        }
    }

    fn test_standing_view_spec_with_outputs(
        view_id: &str,
        output_relations: Vec<RelationSchema>,
    ) -> StandingViewSpec {
        StandingViewSpec {
            view_id: view_id.to_string(),
            sql: "select 1".to_string(),
            dialect: SqlDialect::FelderaSql,
            source_kind: SqlSourceKind::StandingView,
            rust_extension: Default::default(),
            input_relations: vec![test_relation_schema(
                "scores",
                vec![test_feldera_column("score", SqlDataType::Int64)],
            )],
            shape: StandingViewShape {
                is_materialized: true,
                multi_input: false,
                multi_output: output_relations.len() > 1,
            },
            output_relations,
        }
    }

    fn test_active_view_with_outputs(
        view_id: &str,
        output_relations: Vec<RelationSchema>,
    ) -> ActiveMaterializedView {
        ActiveMaterializedView {
            spec_hash: "test-spec-hash".to_string(),
            spec: test_standing_view_spec_with_outputs(view_id, output_relations),
            execution_mode: MaterializedViewExecutionMode::StandingRuntime,
            api: None,
            artifact: None,
            lifecycle: MaterializedViewLifecycleStatus::standing_runtime(),
        }
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
