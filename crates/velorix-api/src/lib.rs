use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env,
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
        MapArray, StringArray, StringDictionaryBuilder, StructArray, Time64NanosecondArray,
        TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{
        ArrowDictionaryKeyType, DataType, Field, Fields, Int16Type, Int32Type, Int64Type, Int8Type,
        Schema, TimeUnit,
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
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as AsyncMutex;
use velorix_core::{
    delta::DeltaBatch,
    query::QueryPolicy,
    relation::{
        datafusion_schema_from_catalog, ArrowPhysicalTypeV1, DataFusionRegistrationModeV1,
        DataFusionRegistrationV1, DictionaryKeyTypeV1, IncrementalAdapterBindingV1,
        IncrementalRelationBindingV1, RelationColumnV1, RelationOperationV1,
        RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
    standing_program::{
        EpochIdempotencyKey, InputEventTimeWatermark, MaterializedViewPage,
        MaterializedViewSqlPage, NativeCodePolicy, RelationInputBatch, RuntimeCheckpoint,
        RuntimeCheckpointStatePayload, RuntimePackageIdentity, ScopedViewId, SnapshotPageRequest,
        StandingProgramIdentity, StandingProgramRuntime, StandingProgramRuntimeError,
        ViewOutputDelta,
    },
    view_contract::{
        catalog_input_relation_schema, stable_bytes_hash, validate_materialized_standing_view_spec,
        view_spec_hash, ColumnSchema, RelationSchema, SqlDataType, SqlDialect, SqlSourceKind,
        SqlStructField, StandingViewShape, StandingViewSpec,
    },
    view_plan::{
        lower_supported_sql_to_logical_plan, supported_view_plan_aggregate_outputs,
        validate_catalog_backed_sum_count_view_sql, validate_supported_join_view_sql,
        validate_supported_latest_by_key_sql, validate_supported_tumbling_window_sql,
        LogicalPlanAggregateFunctionV1, SupportedAggregateOutput, SupportedJoinViewPlan,
        SupportedLatestByKeyPlan, SupportedTumblingWindowPlan, SupportedViewPlan,
        VelorixLogicalViewPlanV1,
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
    STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX, STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX,
    STANDING_RUNTIME_OWNER_SCOPE_KIND_TENANT_PROGRAM_VIEW,
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
        ActiveMaterializedView, InvalidExecutionModeReason, MaterializedViewApiMetadata,
        MaterializedViewArtifactBinding, MaterializedViewCompileStatus,
        MaterializedViewDeploymentStatus, MaterializedViewExecutionMode,
        MaterializedViewLifecycleStatus, MaterializedViewRegistry, MaterializedViewRegistryError,
        MaterializedViewRequestFieldSpec, MaterializedViewResponseColumnSpec,
        MaterializedViewResponseSchema, MaterializedViewRuntimeBinding,
        RegisterMaterializedViewOutcome,
    },
    object_key::ObjectKey,
    relation_catalog_registry::{CreateRelationCatalogOutcome, RelationCatalogRegistry},
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

    fn create_with_catalogs_plan_and_spec(
        &self,
        identity: &StandingProgramIdentity,
        catalogs: &[VelorixRelationCatalogV1],
        logical_plan: &VelorixLogicalViewPlanV1,
        spec: &StandingViewSpec,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        let _ = logical_plan;
        self.create_with_catalogs_and_spec(identity, catalogs, spec, input_schemas, output_schemas)
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StandingRuntimeStatePayloadRecord {
    schema_version: u16,
    record_kind: String,
    tenant_id: String,
    program_id: String,
    view_id: String,
    logical_epoch: u64,
    checkpoint_codec_identity: String,
    state_content_hash: String,
    source_kind: String,
    payload: RuntimeCheckpointStatePayload,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StandingRuntimeOutputManifestRecord {
    schema_version: u16,
    record_kind: String,
    tenant_id: String,
    program_id: String,
    view_id: String,
    checkpoint_key: String,
    logical_epoch: u64,
    checkpoint_content_hash: String,
    output_content_hash: String,
    output_encoding: String,
    output_row_count: usize,
    source_kind: String,
    #[serde(default)]
    pages: Vec<StandingRuntimeOutputPageRef>,
    published_output: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StandingRuntimeOutputPageRef {
    page_index: u32,
    page_key: String,
    page_content_hash: String,
    row_count: usize,
    output_encoding: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StandingRuntimeOutputPageRecord {
    schema_version: u16,
    record_kind: String,
    tenant_id: String,
    program_id: String,
    view_id: String,
    logical_epoch: u64,
    output_content_hash: String,
    page_index: u32,
    page_content_hash: String,
    row_count: usize,
    output_encoding: String,
    source_kind: String,
    published_output: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StandingRuntimeOutputDeltaRecord {
    schema_version: u16,
    record_kind: String,
    tenant_id: String,
    program_id: String,
    view_id: String,
    logical_epoch: u64,
    schema_fingerprint: String,
    delta_content_hash: String,
    delta_encoding: String,
    delta_row_count: usize,
    source_kind: String,
    output_delta: Value,
}

#[derive(Clone, Debug)]
struct StandingRuntimeOutputPublication {
    manifest_key: ObjectKey,
    manifest_record: StandingRuntimeOutputManifestRecord,
    page_records: Vec<(ObjectKey, StandingRuntimeOutputPageRecord)>,
}

#[derive(Clone, Debug)]
struct StandingRuntimeDeltaPublication {
    delta_key: ObjectKey,
    delta_record: StandingRuntimeOutputDeltaRecord,
}

#[derive(Debug)]
struct StandingRuntimeApplyResult {
    checkpoint: RuntimeCheckpoint,
    output_deltas: Vec<ViewOutputDelta>,
}

#[derive(Clone, Debug)]
struct MaterializedViewRuntimeFactory;

impl StandingProgramRuntimeFactory for MaterializedViewRuntimeFactory {
    fn output_schemas_for_view_request(
        &self,
        view_id: &str,
        sql: &str,
        catalog: &VelorixRelationCatalogV1,
        _input_schema_fingerprint: &str,
    ) -> Result<Option<Vec<RelationSchema>>, ApiError> {
        let Ok(plan) = validate_catalog_backed_sum_count_view_sql(sql, catalog) else {
            let Ok(plan) = validate_supported_latest_by_key_sql(sql, catalog) else {
                let Ok(plan) = validate_supported_tumbling_window_sql(sql, catalog) else {
                    return Ok(None);
                };
                return tumbling_window_output_schema(view_id, catalog, &plan)
                    .map(|schema| Some(vec![schema]));
            };
            return latest_by_key_output_schema(view_id, catalog, &plan)
                .map(|schema| Some(vec![schema]));
        };
        single_key_sum_count_output_schema(view_id, catalog, &plan).map(|schema| Some(vec![schema]))
    }

    fn output_schemas_for_view_request_with_catalogs(
        &self,
        view_id: &str,
        sql: &str,
        catalogs: &[VelorixRelationCatalogV1],
        input_schema_fingerprint: &str,
    ) -> Result<Option<Vec<RelationSchema>>, ApiError> {
        if catalogs.len() == 2 {
            let Ok(plan) = validate_supported_join_view_sql(sql, catalogs) else {
                return Ok(None);
            };
            validate_join_plan_catalog_order(&plan, catalogs)?;
            return join_sum_count_output_schema(view_id, catalogs, &plan)
                .map(|schema| Some(vec![schema]));
        }
        let Some(catalog) = catalogs.first() else {
            return Ok(None);
        };
        self.output_schemas_for_view_request(view_id, sql, catalog, input_schema_fingerprint)
    }

    fn create(
        &self,
        _identity: &StandingProgramIdentity,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Err("materialized view runtime requires input/output schemas".to_string())
    }

    fn create_with_schemas(
        &self,
        identity: &StandingProgramIdentity,
        _input_schemas: &[RelationSchema],
        _output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        let _ = identity;
        Err("materialized view runtime requires relation catalog".to_string())
    }

    fn create_with_catalog(
        &self,
        identity: &StandingProgramIdentity,
        catalog: &VelorixRelationCatalogV1,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        velorix_runtime::materialized_view_runtime::create_standing_runtime(
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
        velorix_runtime::materialized_view_runtime::create_standing_runtime_with_sql(
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
        velorix_runtime::materialized_view_runtime::create_standing_runtime_with_sql_and_catalogs(
            identity,
            catalogs,
            spec.sql.as_str(),
            input_schemas,
            output_schemas,
        )
    }

    fn create_with_catalogs_plan_and_spec(
        &self,
        identity: &StandingProgramIdentity,
        catalogs: &[VelorixRelationCatalogV1],
        logical_plan: &VelorixLogicalViewPlanV1,
        _spec: &StandingViewSpec,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        velorix_runtime::materialized_view_runtime::create_standing_runtime_with_logical_plan_and_catalogs(
            identity,
            catalogs,
            logical_plan.clone(),
            input_schemas,
            output_schemas,
        )
    }

    fn restore(
        &self,
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        velorix_runtime::materialized_view_runtime::restore_standing_runtime(checkpoint)
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
            standing_runtimes: Arc::new(StandingRuntimeRegistry::default()),
            standing_runtime_factories: Arc::new(StandingRuntimeFactoryRegistry::default()),
            query_runtimes: Arc::new(Mutex::new(HashMap::new())),
        };
        state.register_standing_program_runtime_factory(
            velorix_runtime::materialized_view_runtime::CRATE_NAME,
            MaterializedViewRuntimeFactory,
        );

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
        runtime_kind: impl Into<String>,
        factory: impl StandingProgramRuntimeFactory,
    ) -> Self {
        self.register_standing_program_runtime_factory(runtime_kind, factory);
        self
    }

    pub fn register_standing_program_runtime_factory(
        &self,
        runtime_kind: impl Into<String>,
        factory: impl StandingProgramRuntimeFactory,
    ) {
        let mut factories = self
            .standing_runtime_factories
            .factories
            .lock()
            .expect("standing runtime factory registry lock poisoned");
        factories.insert(runtime_kind.into(), Arc::new(factory));
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
                    "standing runtime local state is not the committed checkpoint for view `{view_id}`"
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
        runtime_kind: &str,
    ) -> Result<Option<Arc<dyn StandingProgramRuntimeFactory>>, ApiError> {
        let factories = self
            .standing_runtime_factories
            .factories
            .lock()
            .map_err(|_| ApiError::internal("standing runtime factory registry lock poisoned"))?;
        Ok(factories.get(runtime_kind).cloned())
    }

    fn materialized_runtime_output_schemas_for_view_request(
        &self,
        view_id: &str,
        sql: &str,
        catalogs: &[VelorixRelationCatalogV1],
        input_schema_fingerprint: &str,
    ) -> Result<Option<Vec<RelationSchema>>, ApiError> {
        let factories = self
            .standing_runtime_factories
            .factories
            .lock()
            .map_err(|_| ApiError::internal("standing runtime factory registry lock poisoned"))?;
        let Some(factory) = factories.get(velorix_runtime::materialized_view_runtime::CRATE_NAME)
        else {
            return Ok(None);
        };
        factory.output_schemas_for_view_request_with_catalogs(
            view_id,
            sql,
            catalogs,
            input_schema_fingerprint,
        )
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
            if !standing_runtime_can_accept_incremental_ingest(&active) {
                continue;
            }
            if active_standing_runtime_identity(&active).is_none() {
                continue;
            }
            if let Some(replay_plan) =
                ensure_standing_runtime_for_active_view(self, &active).await?
            {
                replay_committed_ingest_into_standing_runtime(self, &active, &replay_plan).await?;
                restored += 1;
            }
        }

        Ok(restored)
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
            "/v1/views/{view_id}/backfill",
            get(get_view_backfill_status).post(run_view_backfill),
        )
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

const DEFAULT_SCORES_RELATION_ID: &str = "scores";
const DEFAULT_SCORES_RELATION_VERSION: &str = "2026-05-24.v1";
const DEFAULT_POSITIVE_SCORES_VIEW_ID: &str = "positive_scores_by_user";
const DEFAULT_POSITIVE_SCORES_SQL: &str = "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id";

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
        incremental_relation: IncrementalRelationBindingV1 {
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
        sql_template: None,
        description: Some("Positive score totals by user".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
    })
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_time_watermark: Option<IngestEventTimeWatermarkRequest>,
    pub rows: Vec<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IngestEventTimeWatermarkRequest {
    pub event_time_column_id: String,
    pub max_observed_event_time_ns: i64,
    pub watermark_ns: i64,
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
    request.source_kind.clone()
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

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackfillViewRequest {
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    batch_limit: Option<usize>,
    #[serde(default)]
    pause_ms: Option<u64>,
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
struct CoverageCapabilityResponse {
    status: String,
    reason: String,
}

#[derive(Clone, Debug, Serialize)]
struct MaterializationCoverageResponse {
    state: String,
    full_view: CoverageCapabilityResponse,
    request_scope: CoverageCapabilityResponse,
    range: CoverageCapabilityResponse,
    background_backfill: CoverageCapabilityResponse,
}

#[derive(Clone, Debug, Serialize)]
struct BackfillProgressResponse {
    processed_batches: usize,
    remaining_batches: usize,
    total_batches: usize,
    percent: f64,
}

#[derive(Clone, Debug, Serialize)]
struct BackfillViewResponse {
    view_id: String,
    outcome: String,
    mode: String,
    lifecycle: MaterializedViewLifecycleStatus,
    query_enabled: bool,
    coverage: MaterializationCoverageResponse,
    progress: BackfillProgressResponse,
    applied_batches: usize,
    remaining_batches: usize,
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
    coverage: MaterializationCoverageResponse,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlTemplateValidationMode {
    LocalDataFusion,
    ExternalSqlRuntime,
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

fn sql_quoted_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn api_path_segment(segment: &str) -> String {
    utf8_percent_encode(segment, API_PATH_SEGMENT_ENCODE_SET).to_string()
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
        if !standing_runtime_can_accept_incremental_ingest(&active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(&active) else {
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
        if !standing_runtime_can_accept_incremental_ingest(&active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(&active) else {
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
    let spec = view_spec_from_request(&state, &request, &catalogs)?;
    validate_materialized_runtime_spec_admission(&spec)?;
    state.validate_standing_runtime_fencing_or_evict().await?;
    let runtime_binding = materialized_view_runtime_binding_for_spec(&catalogs, &spec)?;
    let spec_hash = view_spec_hash(&spec).map_err(ApiError::bad_request)?;
    let api_metadata = api_metadata_from_create_view_request(&request);
    validate_view_api_metadata(&api_metadata)?;
    validate_query_policy_reference(&state, &api_metadata).await?;
    validate_view_api_output_binding(&spec.view_id, &api_metadata, &spec.output_relations)?;
    validate_standing_runtime_create_api_metadata(
        &spec.view_id,
        &api_metadata,
        &spec.output_relations,
        SqlTemplateValidationMode::LocalDataFusion,
    )
    .await?;
    let pending_runtime = build_standing_runtime_for_runtime_binding(
        &state,
        &spec,
        &runtime_binding,
        &catalogs,
        &spec.input_relations,
        &spec.output_relations,
    )?;
    let execution_mode = MaterializedViewExecutionMode::StandingRuntime;
    let requires_backfill = standing_runtime_create_requires_backfill(&state, &spec).await?;
    let lifecycle = lifecycle_for_create_view_execution(&execution_mode, requires_backfill);
    let outcome = if let Some(runtime) = pending_runtime {
        let operation_lock =
            state.standing_runtime_operation_lock(runtime.program_identity(), &spec.view_id)?;
        let _operation_guard = operation_lock.lock().await;
        let outcome = register_materialized_view_execution(
            &state,
            &spec,
            Some(api_metadata.clone()),
            None,
            Some(runtime_binding.clone()),
            Some(execution_mode.clone()),
            Some(lifecycle.clone()),
        )
        .await?;
        if view_query_availability(&lifecycle) {
            insert_standing_runtime(&state, &spec.view_id, runtime)?;
        }
        outcome
    } else {
        register_materialized_view_execution(
            &state,
            &spec,
            Some(api_metadata.clone()),
            None,
            Some(runtime_binding.clone()),
            Some(execution_mode.clone()),
            Some(lifecycle.clone()),
        )
        .await?
    };
    let (status, outcome_text) = match outcome {
        RegisterMaterializedViewOutcome::Created => (StatusCode::CREATED, "created"),
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
            None,
            Some(outcome_text),
        )?),
    ))
}

async fn register_materialized_view_execution(
    state: &ApiState,
    spec: &StandingViewSpec,
    api: Option<MaterializedViewApiMetadata>,
    artifact: Option<MaterializedViewArtifactBinding>,
    runtime: Option<MaterializedViewRuntimeBinding>,
    execution_mode: Option<MaterializedViewExecutionMode>,
    lifecycle: Option<MaterializedViewLifecycleStatus>,
) -> Result<RegisterMaterializedViewOutcome, ApiError> {
    if let Some(runtime) = runtime {
        state
            .view_registry()?
            .register_with_api_metadata_runtime_execution(spec, api, runtime, lifecycle)
            .await
            .map_err(materialized_view_registry_error_to_api)
    } else {
        state
            .view_registry()?
            .register_with_api_metadata_artifact_execution(
                spec,
                api,
                artifact,
                execution_mode,
                lifecycle,
            )
            .await
            .map_err(materialized_view_registry_error_to_api)
    }
}

fn materialized_view_runtime_binding_for_spec(
    catalogs: &[VelorixRelationCatalogV1],
    spec: &StandingViewSpec,
) -> Result<MaterializedViewRuntimeBinding, ApiError> {
    let identity = standing_program_identity_from_materialized_view_runtime(catalogs, spec)?;
    let output_schema = only_output_relation_for_runtime_binding(spec)?;
    let logical_plan =
        lower_supported_sql_to_logical_plan(spec.sql.as_str(), catalogs, output_schema)
            .map_err(ApiError::bad_request)?;
    Ok(MaterializedViewRuntimeBinding {
        runtime_kind: velorix_runtime::materialized_view_runtime::CRATE_NAME.to_string(),
        runtime_version: "builtin-v1".to_string(),
        standing_program_identity: identity,
        logical_plan: Some(logical_plan),
    })
}

fn only_output_relation_for_runtime_binding(
    spec: &StandingViewSpec,
) -> Result<&RelationSchema, ApiError> {
    let [output_schema] = spec.output_relations.as_slice() else {
        return Err(ApiError::bad_request(
            "materialized view runtime requires exactly one output relation",
        ));
    };
    Ok(output_schema)
}

fn standing_program_identity_from_materialized_view_runtime(
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
        stable_bytes_hash(&input_schema_bytes)
    };
    let identity = StandingProgramIdentity {
        tenant_id: "default".to_string(),
        program_id: spec.view_id.clone(),
        view_ids: standing_program_view_ids_for_spec(spec),
        sql_hash: stable_bytes_hash(spec.sql.as_bytes()),
        input_catalog_hash,
        output_schema_hash: stable_bytes_hash(&output_schema_bytes),
        compiler_identity: "velorix-materialized-view-runtime".to_string(),
        runtime_packages: vec![RuntimePackageIdentity {
            name: velorix_runtime::materialized_view_runtime::CRATE_NAME.to_string(),
            version: "builtin-v1".to_string(),
        }],
        package_feature_set: vec!["materialized_view_runtime".to_string()],
        runtime_compatibility: "velorix-materialized-view-runtime-v1".to_string(),
        checkpoint_codec_identity: "velorix-materialized-view-state-v1".to_string(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
    };
    identity.validate().map_err(ApiError::bad_request)?;
    Ok(identity)
}

fn active_standing_runtime_identity(
    active: &ActiveMaterializedView,
) -> Option<&StandingProgramIdentity> {
    active
        .runtime
        .as_ref()
        .map(|runtime| &runtime.standing_program_identity)
        .or_else(|| {
            active
                .artifact
                .as_ref()
                .and_then(|artifact| artifact.standing_program_identity.as_ref())
        })
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

async fn ensure_standing_runtime_for_active_view(
    state: &ApiState,
    active: &ActiveMaterializedView,
) -> Result<Option<StandingRuntimeReplayPlan>, ApiError> {
    let Some(runtime_binding) = active.runtime.as_ref() else {
        return Ok(None);
    };
    let Some((runtime, replay_plan)) = restore_or_build_standing_runtime_for_runtime_binding(
        state,
        &active.spec,
        runtime_binding,
        &active.spec.input_relations,
        &active.spec.output_relations,
    )
    .await?
    else {
        return Ok(None);
    };
    let committed_checkpoint = read_latest_standing_runtime_checkpoint(
        state,
        runtime.program_identity(),
        &active.spec.view_id,
    )
    .await?
    .as_ref()
    .map(standing_runtime_checkpoint_pointer_from_record);
    state.set_standing_runtime_committed_checkpoint(
        runtime.program_identity(),
        &active.spec.view_id,
        committed_checkpoint,
    )?;
    insert_standing_runtime(state, &active.spec.view_id, runtime)?;
    Ok(Some(replay_plan))
}

fn build_standing_runtime_for_runtime_binding(
    state: &ApiState,
    spec: &StandingViewSpec,
    runtime_binding: &MaterializedViewRuntimeBinding,
    catalogs: &[VelorixRelationCatalogV1],
    expected_input_schemas: &[RelationSchema],
    expected_output_schemas: &[RelationSchema],
) -> Result<Option<Box<dyn StandingProgramRuntime + Send>>, ApiError> {
    let identity = &runtime_binding.standing_program_identity;
    if state.standing_runtime(identity, &spec.view_id)?.is_some() {
        return Ok(None);
    }
    let Some(factory) = state.standing_runtime_factory(&runtime_binding.runtime_kind)? else {
        return Err(ApiError::bad_request(format!(
            "standing runtime factory is not registered for runtime `{}`",
            runtime_binding.runtime_kind
        )));
    };
    let logical_plan = runtime_binding
        .logical_plan
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("standing runtime binding is missing logical plan"))?;
    let runtime = factory
        .create_with_catalogs_plan_and_spec(
            identity,
            catalogs,
            logical_plan,
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

async fn restore_or_build_standing_runtime_for_runtime_binding(
    state: &ApiState,
    spec: &StandingViewSpec,
    runtime_binding: &MaterializedViewRuntimeBinding,
    expected_input_schemas: &[RelationSchema],
    expected_output_schemas: &[RelationSchema],
) -> Result<
    Option<(
        Box<dyn StandingProgramRuntime + Send>,
        StandingRuntimeReplayPlan,
    )>,
    ApiError,
> {
    let identity = &runtime_binding.standing_program_identity;
    if state.standing_runtime(identity, &spec.view_id)?.is_some() {
        return Ok(None);
    }
    let Some(factory) = state.standing_runtime_factory(&runtime_binding.runtime_kind)? else {
        return Err(ApiError::bad_request(format!(
            "standing runtime factory is not registered for runtime `{}`",
            runtime_binding.runtime_kind
        )));
    };
    let logical_plan = runtime_binding
        .logical_plan
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("standing runtime binding is missing logical plan"))?;
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
            let logical_plan = logical_plan.clone();
            let spec = spec.clone();
            let expected_input_schemas = expected_input_schemas.to_vec();
            let expected_output_schemas = expected_output_schemas.to_vec();
            (
                tokio::task::spawn_blocking(move || {
                    factory.create_with_catalogs_plan_and_spec(
                        &identity,
                        &catalogs,
                        &logical_plan,
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
        let logical_plan = logical_plan.clone();
        let spec = spec.clone();
        let expected_input_schemas = expected_input_schemas.to_vec();
        let expected_output_schemas = expected_output_schemas.to_vec();
        (
            tokio::task::spawn_blocking(move || {
                factory.create_with_catalogs_plan_and_spec(
                    &identity,
                    &catalogs,
                    &logical_plan,
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

fn validate_materialized_runtime_spec_admission(spec: &StandingViewSpec) -> Result<(), ApiError> {
    validate_materialized_standing_view_spec(spec).map_err(ApiError::bad_request)?;
    validate_materialized_runtime_relation_schemas_admission(
        "spec.input_relations",
        &spec.input_relations,
    )?;
    validate_materialized_runtime_relation_schemas_admission(
        "spec.output_relations",
        &spec.output_relations,
    )?;
    Ok(())
}

fn validate_materialized_runtime_relation_schemas_admission(
    field: &str,
    schemas: &[RelationSchema],
) -> Result<(), ApiError> {
    for schema in schemas {
        for column in &schema.columns {
            validate_materialized_runtime_sql_type_admission(
                &format!("{field}.{}.{}", schema.relation_id, column.name),
                &column.data_type,
            )?;
        }
    }
    Ok(())
}

fn validate_materialized_runtime_sql_type_admission(
    field: &str,
    data_type: &SqlDataType,
) -> Result<(), ApiError> {
    match data_type {
        SqlDataType::Timestamp {
            timezone: Some(timezone),
        } => Err(ApiError::bad_request(format!(
            "materialized runtime admission rejected `{field}`: timezone-bearing timestamps are not supported yet; timezone={timezone}"
        ))),
        SqlDataType::Array { element_type } => {
            validate_materialized_runtime_sql_type_admission(field, element_type)
        }
        SqlDataType::Struct { fields } => {
            for struct_field in fields {
                validate_materialized_runtime_sql_type_admission(
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
            validate_materialized_runtime_sql_type_admission(&format!("{field}.key"), key_type)?;
            validate_materialized_runtime_sql_type_admission(&format!("{field}.value"), value_type)
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

async fn get_view_backfill_status(
    State(state): State<ApiState>,
    AxumPath(view_id): AxumPath<String>,
) -> Result<Json<BackfillViewResponse>, ApiError> {
    let active = state
        .view_registry()?
        .read_active(&view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let progress = committed_backfill_progress(&state, &active).await?;
    Ok(Json(backfill_view_response(
        &active,
        "status",
        "status",
        0,
        progress.remaining_batches,
        progress,
    )))
}

async fn run_view_backfill(
    State(state): State<ApiState>,
    AxumPath(view_id): AxumPath<String>,
    Json(request): Json<BackfillViewRequest>,
) -> Result<(StatusCode, Json<BackfillViewResponse>), ApiError> {
    let mode = request.mode.as_deref().unwrap_or("sync");
    match mode {
        "sync" => {
            let outcome = run_view_backfill_step(&state, &view_id, request.batch_limit).await?;
            Ok((StatusCode::OK, Json(outcome)))
        }
        "background" => {
            let batch_limit = request.batch_limit.unwrap_or(1);
            if batch_limit == 0 {
                return Err(ApiError::bad_request(
                    "backfill batch_limit must be a positive integer",
                ));
            }
            let pause_ms = request.pause_ms.unwrap_or(100);
            let active = state
                .view_registry()?
                .read_active(&view_id)
                .await
                .map_err(materialized_view_registry_error_to_api)?;
            if view_query_availability(&active.lifecycle) {
                return Ok((
                    StatusCode::OK,
                    Json(backfill_view_response(
                        &active,
                        "already_running",
                        mode,
                        0,
                        0,
                        committed_backfill_progress(&state, &active).await?,
                    )),
                ));
            }
            if !view_backfill_is_query_triggerable(&active) {
                ensure_view_execution_allowed(&active)?;
            }
            spawn_background_view_backfill(state.clone(), view_id.clone(), batch_limit, pause_ms);
            let progress = committed_backfill_progress(&state, &active).await?;
            Ok((
                StatusCode::ACCEPTED,
                Json(backfill_view_response(
                    &active,
                    "scheduled",
                    mode,
                    0,
                    progress.remaining_batches,
                    progress,
                )),
            ))
        }
        other => Err(ApiError::bad_request(format!(
            "unsupported backfill mode `{other}`; expected `sync` or `background`"
        ))),
    }
}

struct PreparedIngestBatch {
    request: IngestRowsRequest,
    catalog: VelorixRelationCatalogV1,
    record_batch: RecordBatch,
    end_offset_exclusive: u64,
    event_time_watermark: Option<InputEventTimeWatermark>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_time_watermark: Option<InputEventTimeWatermark>,
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
    let identity = active_standing_runtime_identity(&active).ok_or_else(|| {
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
    let event_time_watermark = ingest_event_time_watermark(&catalog, &request, &batch)?;
    let envelope = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: request.relation_id.clone(),
            relation_version: request.relation_version.clone(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: request.stream_id.clone(),
            partition_id: request.partition_id,
            start_offset_inclusive: request.start_offset_inclusive,
            end_offset_exclusive,
            event_time_watermark: event_time_watermark.clone(),
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
        event_time_watermark,
        payload_digest,
        envelope,
    })
}

fn ingest_event_time_watermark(
    catalog: &VelorixRelationCatalogV1,
    request: &IngestRowsRequest,
    batch: &RecordBatch,
) -> Result<Option<InputEventTimeWatermark>, ApiError> {
    let Some(request_watermark) = &request.event_time_watermark else {
        return Ok(None);
    };
    let Some(event_time_column_id) = &catalog.relation_schema.event_time_column_id else {
        return Err(ApiError::bad_request(
            "event_time_watermark requires relation_schema.event_time_column_id",
        ));
    };
    if request_watermark.event_time_column_id != *event_time_column_id {
        return Err(ApiError::bad_request(format!(
            "event_time_watermark.event_time_column_id must match relation event_time_column_id `{event_time_column_id}`"
        )));
    }
    let column = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == *event_time_column_id)
        .ok_or_else(|| ApiError::bad_request("relation event_time_column_id column is missing"))?;
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Date32
        | ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {}
        _ => {
            return Err(ApiError::bad_request(
                "event_time_watermark currently supports Int64, Date32, or TimestampNanosecond event-time columns",
            ));
        }
    }
    if request_watermark.event_time_column_id.trim().is_empty() {
        return Err(ApiError::bad_request(
            "event_time_watermark.event_time_column_id must be nonempty",
        ));
    }
    if request_watermark.watermark_ns > request_watermark.max_observed_event_time_ns {
        return Err(ApiError::bad_request(
            "event_time_watermark.watermark_ns must be <= max_observed_event_time_ns",
        ));
    }
    let actual_max_observed = event_time_column_max_value(column, batch)?;
    if request_watermark.max_observed_event_time_ns < actual_max_observed {
        return Err(ApiError::bad_request(format!(
            "event_time_watermark.max_observed_event_time_ns must be >= actual max event-time value {actual_max_observed}"
        )));
    }
    Ok(Some(InputEventTimeWatermark {
        stream_id: request.stream_id.clone(),
        partition_id: request.partition_id,
        event_time_column_id: request_watermark.event_time_column_id.clone(),
        max_observed_event_time_ns: request_watermark.max_observed_event_time_ns,
        watermark_ns: request_watermark.watermark_ns,
    }))
}

fn event_time_column_max_value(
    column: &RelationColumnV1,
    batch: &RecordBatch,
) -> Result<i64, ApiError> {
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 => {
            let array = batch
                .column_by_name(&column.name)
                .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| ApiError::bad_request("event-time column must be Int64"))?;
            max_int64_array_value(&column.name, array)
        }
        ArrowPhysicalTypeV1::Date32 => {
            let array = batch
                .column_by_name(&column.name)
                .and_then(|column| column.as_any().downcast_ref::<Date32Array>())
                .ok_or_else(|| ApiError::bad_request("event-time column must be Date32"))?;
            max_i32_array_value(&column.name, array).map(i64::from)
        }
        ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {
            let array = batch
                .column_by_name(&column.name)
                .and_then(|column| {
                    column.as_any().downcast_ref::<TimestampNanosecondArray>()
                })
                .ok_or_else(|| {
                    ApiError::bad_request("event-time column must be TimestampNanosecond")
                })?;
            max_timestamp_array_value(&column.name, array)
        }
        _ => Err(ApiError::bad_request(
            "event_time_watermark currently supports Int64, Date32, or TimestampNanosecond event-time columns",
        )),
    }
}

fn max_int64_array_value(name: &str, array: &Int64Array) -> Result<i64, ApiError> {
    let mut max_value = None;
    for row in 0..array.len() {
        if !array.is_null(row) {
            max_value = Some(max_value.map_or(array.value(row), |current: i64| {
                current.max(array.value(row))
            }));
        }
    }
    max_value.ok_or_else(|| {
        ApiError::bad_request(format!(
            "event-time column `{name}` must contain at least one non-null value"
        ))
    })
}

fn max_timestamp_array_value(
    name: &str,
    array: &TimestampNanosecondArray,
) -> Result<i64, ApiError> {
    let mut max_value = None;
    for row in 0..array.len() {
        if !array.is_null(row) {
            max_value = Some(max_value.map_or(array.value(row), |current: i64| {
                current.max(array.value(row))
            }));
        }
    }
    max_value.ok_or_else(|| {
        ApiError::bad_request(format!(
            "event-time column `{name}` must contain at least one non-null value"
        ))
    })
}

fn max_i32_array_value(name: &str, array: &Date32Array) -> Result<i32, ApiError> {
    let mut max_value = None;
    for row in 0..array.len() {
        if !array.is_null(row) {
            max_value = Some(max_value.map_or(array.value(row), |current: i32| {
                current.max(array.value(row))
            }));
        }
    }
    max_value.ok_or_else(|| {
        ApiError::bad_request(format!(
            "event-time column `{name}` must contain at least one non-null value"
        ))
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
        event_time_watermark: prepared.event_time_watermark.clone(),
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
        output_manifest_refs: Vec::new(),
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

async fn standing_runtime_create_requires_backfill(
    state: &ApiState,
    spec: &StandingViewSpec,
) -> Result<bool, ApiError> {
    if spec.input_relations.is_empty() {
        return Ok(false);
    }
    let ingest_log =
        IngestLog::new_catalog_checked(Arc::clone(&state.store), state.capabilities.as_ref())
            .map_err(ApiError::internal)?;
    let batches = ingest_log
        .replay_admitted_validated_envelopes_from(&[])
        .await
        .map_err(ApiError::internal)?;
    for batch in batches {
        let envelope =
            IngestEnvelope::decode(batch.payload().clone()).map_err(ApiError::bad_request)?;
        let header = envelope.header();
        if spec.input_relations.iter().any(|input| {
            header.relation_id == input.relation_id
                && header.relation_version == input.relation_version
                && header.schema_fingerprint == input.schema_fingerprint
        }) {
            return Ok(true);
        }
    }

    Ok(false)
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
        if !standing_runtime_can_accept_incremental_ingest(active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(&active) else {
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
        if !standing_runtime_can_accept_incremental_ingest(&active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(&active) else {
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
                "standing runtime is unavailable for active view `{}`",
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
        if !standing_runtime_can_accept_incremental_ingest(&active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(&active) else {
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
        if !standing_runtime_can_accept_incremental_ingest(&active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(&active) else {
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
        if !standing_runtime_can_accept_incremental_ingest(&active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(&active) else {
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
        if !standing_runtime_can_accept_incremental_ingest(&active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(&active) else {
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
        if !standing_runtime_can_accept_incremental_ingest(&active) {
            continue;
        }
        let Some(identity) = active_standing_runtime_identity(&active) else {
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
                    "standing runtime disappeared for active view `{}`",
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
        let apply_result = apply_standing_runtime_changes_and_checkpoint_many(
            Arc::clone(&runtime),
            0,
            idempotency_key,
            input_batches,
        )
        .await;
        let apply_result = match apply_result {
            Ok(apply_result) => apply_result,
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
            &apply_result.checkpoint,
            &apply_result.output_deltas,
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
            &apply_result.checkpoint,
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
        event_time_watermark: prepared.event_time_watermark.clone(),
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
) -> Result<StandingRuntimeApplyResult, ApiError> {
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
) -> Result<StandingRuntimeApplyResult, ApiError> {
    tokio::task::spawn_blocking(move || {
        let mut runtime = runtime
            .lock()
            .map_err(|_| ApiError::internal("standing runtime lock poisoned"))?;
        let logical_epoch =
            next_standing_runtime_logical_epoch(runtime.as_ref(), lower_bound_epoch)?;
        let commit = runtime
            .apply_changes(logical_epoch, idempotency_key, input_batches)
            .map_err(ApiError::bad_request)?;
        let checkpoint = runtime.checkpoint().map_err(ApiError::bad_request)?;
        Ok(StandingRuntimeApplyResult {
            checkpoint,
            output_deltas: commit.output_deltas,
        })
    })
    .await
    .map_err(ApiError::internal)?
}

async fn persist_standing_runtime_checkpoint(
    state: &ApiState,
    view_id: &str,
    checkpoint: &RuntimeCheckpoint,
    output_deltas: &[ViewOutputDelta],
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
    let output_manifest = standing_runtime_output_manifest_record_for_checkpoint(
        checkpoint,
        view_id,
        &checkpoint_key,
    )?;
    if let Some(output_manifest) = &output_manifest {
        for (output_page_key, output_page_record) in &output_manifest.page_records {
            persist_standing_runtime_output_page(state, output_page_key, output_page_record)
                .await?;
        }
        persist_standing_runtime_output_manifest(
            state,
            &output_manifest.manifest_key,
            &output_manifest.manifest_record,
        )
        .await?;
    }
    let output_delta_publications =
        standing_runtime_output_delta_records_for_checkpoint(checkpoint, view_id, output_deltas)?;
    for publication in &output_delta_publications {
        persist_standing_runtime_output_delta(
            state,
            &publication.delta_key,
            &publication.delta_record,
        )
        .await?;
    }
    let (state_payload_key, state_payload_record) =
        standing_runtime_state_payload_record_for_checkpoint(checkpoint, view_id)?;
    persist_standing_runtime_state_payload(state, &state_payload_key, &state_payload_record)
        .await?;
    let checkpoint_for_record = standing_runtime_checkpoint_with_durable_publication_refs(
        checkpoint,
        output_manifest
            .as_ref()
            .map(|publication| &publication.manifest_key),
        output_delta_publications
            .iter()
            .map(|publication| &publication.delta_key)
            .collect::<Vec<_>>()
            .as_slice(),
        &state_payload_key,
    );
    let candidate = standing_runtime_checkpoint_pointer_from_key(
        &checkpoint_key,
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
        checkpoint.logical_epoch,
        &checkpoint.state_root.content_hash,
        checkpoint_for_record.output_manifest_refs.clone(),
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
        checkpoint: checkpoint_for_record,
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

fn standing_runtime_checkpoint_with_publication_output_refs(
    checkpoint: &RuntimeCheckpoint,
    output_manifest_key: Option<&ObjectKey>,
    output_delta_keys: &[&ObjectKey],
) -> RuntimeCheckpoint {
    let mut checkpoint = checkpoint.clone();
    checkpoint.output_manifest_refs.clear();
    if let Some(output_manifest_key) = output_manifest_key {
        checkpoint.output_manifest_refs.push(format!(
            "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
            output_manifest_key.as_str()
        ));
    }
    checkpoint
        .output_manifest_refs
        .extend(output_delta_keys.iter().map(|output_delta_key| {
            format!(
                "{STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX}{}",
                output_delta_key.as_str()
            )
        }));
    checkpoint
}

fn standing_runtime_checkpoint_with_durable_publication_refs(
    checkpoint: &RuntimeCheckpoint,
    output_manifest_key: Option<&ObjectKey>,
    output_delta_keys: &[&ObjectKey],
    state_payload_key: &ObjectKey,
) -> RuntimeCheckpoint {
    let mut checkpoint = standing_runtime_checkpoint_with_publication_output_refs(
        checkpoint,
        output_manifest_key,
        output_delta_keys,
    );
    checkpoint.state_root.object_key = state_payload_key.as_str().to_string();
    checkpoint.state_payload = None;
    checkpoint
}

fn standing_runtime_state_payload_record_for_checkpoint(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
) -> Result<(ObjectKey, StandingRuntimeStatePayloadRecord), ApiError> {
    let Some(payload) = checkpoint.state_payload.clone() else {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint for view `{view_id}` is missing state payload"
        )));
    };
    if payload.codec_identity != checkpoint.checkpoint_codec_identity {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint state payload codec mismatch for view `{view_id}`"
        )));
    }
    let actual_state_hash = stable_bytes_hash(payload.payload.as_bytes());
    if actual_state_hash != checkpoint.state_root.content_hash {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint state payload hash mismatch for view `{view_id}`"
        )));
    }
    let key = ObjectKey::standing_runtime_state_payload(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
        checkpoint.logical_epoch,
        &checkpoint.state_root.content_hash,
    )
    .map_err(ApiError::bad_request)?;
    let record = StandingRuntimeStatePayloadRecord {
        schema_version: 1,
        record_kind: "standing_runtime_state_payload_v1".to_string(),
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        view_id: view_id.to_string(),
        logical_epoch: checkpoint.logical_epoch,
        checkpoint_codec_identity: checkpoint.checkpoint_codec_identity.clone(),
        state_content_hash: checkpoint.state_root.content_hash.clone(),
        source_kind: "standing_runtime_checkpoint_state_payload".to_string(),
        payload,
    };
    validate_standing_runtime_state_payload_record(&key, &record)?;
    Ok((key, record))
}

async fn persist_standing_runtime_state_payload(
    state: &ApiState,
    state_payload_key: &ObjectKey,
    record: &StandingRuntimeStatePayloadRecord,
) -> Result<(), ApiError> {
    let bytes =
        serde_json::to_vec(record).map_err(|source| ApiError::internal(source.to_string()))?;
    let path = ObjectPath::from(state_payload_key.as_str());
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
                    "standing runtime state payload conflict at {}",
                    state_payload_key.as_str()
                )))
            }
        }
        Err(error) => Err(ApiError::internal(error)),
    }
}

fn standing_runtime_output_manifest_record_for_checkpoint(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
    checkpoint_key: &ObjectKey,
) -> Result<Option<StandingRuntimeOutputPublication>, ApiError> {
    let Some(published_output) = standing_runtime_checkpoint_published_output(checkpoint) else {
        return Ok(None);
    };
    let output_row_count = standing_runtime_published_output_row_count(&published_output)?;
    let output_bytes = serde_json::to_vec(&published_output)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let output_content_hash = stable_bytes_hash(&output_bytes);
    let output_page_key = ObjectKey::standing_runtime_output_page(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
        checkpoint.logical_epoch,
        0,
        &output_content_hash,
    )
    .map_err(ApiError::bad_request)?;
    let page_ref = StandingRuntimeOutputPageRef {
        page_index: 0,
        page_key: output_page_key.as_str().to_string(),
        page_content_hash: output_content_hash.clone(),
        row_count: output_row_count,
        output_encoding: "velorix-delta-batch-json-v1".to_string(),
    };
    let page_record = StandingRuntimeOutputPageRecord {
        schema_version: 1,
        record_kind: "standing_runtime_output_page_v1".to_string(),
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        view_id: view_id.to_string(),
        logical_epoch: checkpoint.logical_epoch,
        output_content_hash: output_content_hash.clone(),
        page_index: 0,
        page_content_hash: output_content_hash.clone(),
        row_count: output_row_count,
        output_encoding: "velorix-delta-batch-json-v1".to_string(),
        source_kind: "standing_runtime_checkpoint_published_output".to_string(),
        published_output: published_output.clone(),
    };
    let output_manifest_key = ObjectKey::standing_runtime_output_manifest(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        view_id,
        checkpoint.logical_epoch,
        &output_content_hash,
    )
    .map_err(ApiError::bad_request)?;
    let record = StandingRuntimeOutputManifestRecord {
        schema_version: 1,
        record_kind: "standing_runtime_output_manifest_v1".to_string(),
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        view_id: view_id.to_string(),
        checkpoint_key: checkpoint_key.as_str().to_string(),
        logical_epoch: checkpoint.logical_epoch,
        checkpoint_content_hash: checkpoint.state_root.content_hash.clone(),
        output_content_hash,
        output_encoding: "velorix-delta-batch-json-v1".to_string(),
        output_row_count,
        source_kind: "standing_runtime_checkpoint_published_output".to_string(),
        pages: vec![page_ref],
        published_output,
    };
    validate_standing_runtime_output_page_record(&output_page_key, &page_record)?;
    validate_standing_runtime_output_manifest_record(&output_manifest_key, &record)?;
    Ok(Some(StandingRuntimeOutputPublication {
        manifest_key: output_manifest_key,
        manifest_record: record,
        page_records: vec![(output_page_key, page_record)],
    }))
}

async fn persist_standing_runtime_output_page(
    state: &ApiState,
    output_page_key: &ObjectKey,
    record: &StandingRuntimeOutputPageRecord,
) -> Result<(), ApiError> {
    let bytes =
        serde_json::to_vec(record).map_err(|source| ApiError::internal(source.to_string()))?;
    let path = ObjectPath::from(output_page_key.as_str());
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
                    "standing runtime output page conflict at {}",
                    output_page_key.as_str()
                )))
            }
        }
        Err(error) => Err(ApiError::internal(error)),
    }
}

async fn persist_standing_runtime_output_manifest(
    state: &ApiState,
    output_manifest_key: &ObjectKey,
    record: &StandingRuntimeOutputManifestRecord,
) -> Result<(), ApiError> {
    let bytes =
        serde_json::to_vec(record).map_err(|source| ApiError::internal(source.to_string()))?;
    let path = ObjectPath::from(output_manifest_key.as_str());
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
                    "standing runtime output manifest conflict at {}",
                    output_manifest_key.as_str()
                )))
            }
        }
        Err(error) => Err(ApiError::internal(error)),
    }
}

fn standing_runtime_output_delta_records_for_checkpoint(
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
    output_deltas: &[ViewOutputDelta],
) -> Result<Vec<StandingRuntimeDeltaPublication>, ApiError> {
    let mut publications = Vec::new();
    for output_delta in output_deltas {
        if output_delta.view_id != view_id {
            return Err(ApiError::bad_request(format!(
                "standing runtime output delta identity does not match view `{view_id}`"
            )));
        }
        let output_delta_value = serde_json::to_value(&output_delta.delta)
            .map_err(|source| ApiError::internal(source.to_string()))?;
        let delta_bytes = serde_json::to_vec(&output_delta_value)
            .map_err(|source| ApiError::internal(source.to_string()))?;
        let delta_content_hash = stable_bytes_hash(&delta_bytes);
        let delta_key = ObjectKey::standing_runtime_output_delta(
            &checkpoint.identity.tenant_id,
            &checkpoint.identity.program_id,
            view_id,
            checkpoint.logical_epoch,
            &delta_content_hash,
        )
        .map_err(ApiError::bad_request)?;
        let delta_row_count = output_delta
            .delta
            .net_rows()
            .map_err(|_| ApiError::bad_request("standing runtime output delta is malformed"))?
            .len();
        let delta_record = StandingRuntimeOutputDeltaRecord {
            schema_version: 1,
            record_kind: "standing_runtime_output_delta_v1".to_string(),
            tenant_id: checkpoint.identity.tenant_id.clone(),
            program_id: checkpoint.identity.program_id.clone(),
            view_id: view_id.to_string(),
            logical_epoch: checkpoint.logical_epoch,
            schema_fingerprint: output_delta.schema_fingerprint.clone(),
            delta_content_hash,
            delta_encoding: "velorix-delta-batch-json-v1".to_string(),
            delta_row_count,
            source_kind: "standing_runtime_epoch_output_delta".to_string(),
            output_delta: output_delta_value,
        };
        validate_standing_runtime_output_delta_record(&delta_key, &delta_record)?;
        publications.push(StandingRuntimeDeltaPublication {
            delta_key,
            delta_record,
        });
    }
    Ok(publications)
}

async fn persist_standing_runtime_output_delta(
    state: &ApiState,
    output_delta_key: &ObjectKey,
    record: &StandingRuntimeOutputDeltaRecord,
) -> Result<(), ApiError> {
    let bytes =
        serde_json::to_vec(record).map_err(|source| ApiError::internal(source.to_string()))?;
    let path = ObjectPath::from(output_delta_key.as_str());
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
                    "standing runtime output delta conflict at {}",
                    output_delta_key.as_str()
                )))
            }
        }
        Err(error) => Err(ApiError::internal(error)),
    }
}

fn standing_runtime_checkpoint_published_output(checkpoint: &RuntimeCheckpoint) -> Option<Value> {
    let Some(state_payload) = &checkpoint.state_payload else {
        return None;
    };
    let Ok(payload) = serde_json::from_str::<Value>(&state_payload.payload) else {
        return None;
    };
    payload
        .get("published_output")
        .filter(|published_output| !published_output.is_null())
        .cloned()
}

fn standing_runtime_published_output_row_count(
    published_output: &Value,
) -> Result<usize, ApiError> {
    let output: DeltaBatch = serde_json::from_value(published_output.clone())
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    let rows = output
        .net_rows()
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    if rows.iter().any(|row| row.weight != 1) {
        return Err(ApiError::bad_request(
            "standing runtime published output contains non-materialized row weights",
        ));
    }
    Ok(rows.len())
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
        output_manifest_refs: record.checkpoint.output_manifest_refs.clone(),
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
    output_manifest_refs: Vec<String>,
) -> Result<StandingRuntimeCheckpointPointer, ApiError> {
    let pointer = StandingRuntimeCheckpointPointer {
        tenant_id: tenant_id.to_string(),
        program_id: program_id.to_string(),
        view_id: view_id.to_string(),
        checkpoint_key: checkpoint_key.as_str().to_string(),
        logical_epoch,
        content_hash: content_hash.to_string(),
        output_manifest_refs,
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
    let pointer = StandingRuntimeCheckpointPointer {
        tenant_id: checkpoint_key_parts.tenant_id,
        program_id: checkpoint_key_parts.program_id,
        view_id: checkpoint_key_parts.view_id,
        checkpoint_key: latest_checkpoint_path,
        logical_epoch: checkpoint_key_parts.logical_epoch,
        content_hash: checkpoint_key_parts.content_hash,
        output_manifest_refs: record.checkpoint.output_manifest_refs.clone(),
    };
    validate_standing_runtime_checkpoint_output_refs(&record, &pointer)?;
    hydrate_standing_runtime_checkpoint_state_payload(state, &mut record).await?;
    validate_standing_runtime_checkpoint_output_manifest_records(state, &record).await?;
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
    hydrate_standing_runtime_checkpoint_state_payload(state, &mut record).await?;
    validate_standing_runtime_checkpoint_output_manifest_records(state, &record).await?;
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
        || (!pointer.output_manifest_refs.is_empty()
            && pointer.output_manifest_refs != record.checkpoint.output_manifest_refs)
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
    validate_standing_runtime_checkpoint_output_refs(record, pointer)?;
    Ok(())
}

async fn hydrate_standing_runtime_checkpoint_state_payload(
    state: &ApiState,
    record: &mut StandingRuntimeCheckpointRecord,
) -> Result<(), ApiError> {
    let state_root_key = record.checkpoint.state_root.object_key.clone();
    let parsed_state_key = ObjectKey::parse_standing_runtime_state_payload(state_root_key.clone());
    let Ok((state_payload_key, parts)) = parsed_state_key else {
        if record.checkpoint.state_payload.is_some() {
            return Ok(());
        }
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint for view `{}` is missing durable state payload root",
            record.view_id
        )));
    };
    if parts.tenant_id != record.checkpoint.identity.tenant_id
        || parts.program_id != record.checkpoint.identity.program_id
        || parts.view_id != record.view_id
        || parts.logical_epoch != record.checkpoint.logical_epoch
        || parts.state_content_hash != record.checkpoint.state_root.content_hash
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint state payload key/body mismatch for `{}/{}/{}`",
            record.checkpoint.identity.tenant_id,
            record.checkpoint.identity.program_id,
            record.view_id
        )));
    }
    let bytes = state
        .store
        .get(&ObjectPath::from(state_payload_key.as_str()))
        .await
        .map_err(ApiError::internal)?
        .bytes()
        .await
        .map_err(ApiError::internal)?;
    let state_payload_record: StandingRuntimeStatePayloadRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    validate_standing_runtime_state_payload_record(&state_payload_key, &state_payload_record)?;
    if state_payload_record.tenant_id != record.checkpoint.identity.tenant_id
        || state_payload_record.program_id != record.checkpoint.identity.program_id
        || state_payload_record.view_id != record.view_id
        || state_payload_record.logical_epoch != record.checkpoint.logical_epoch
        || state_payload_record.checkpoint_codec_identity
            != record.checkpoint.checkpoint_codec_identity
        || state_payload_record.state_content_hash != record.checkpoint.state_root.content_hash
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime checkpoint state payload record mismatch for `{}/{}/{}`",
            record.checkpoint.identity.tenant_id,
            record.checkpoint.identity.program_id,
            record.view_id
        )));
    }
    if let Some(existing_payload) = &record.checkpoint.state_payload {
        if existing_payload != &state_payload_record.payload {
            return Err(ApiError::bad_request(format!(
                "standing runtime checkpoint embedded state payload mismatch for `{}/{}/{}`",
                record.checkpoint.identity.tenant_id,
                record.checkpoint.identity.program_id,
                record.view_id
            )));
        }
    }
    record.checkpoint.state_payload = Some(state_payload_record.payload);
    Ok(())
}

fn validate_standing_runtime_checkpoint_output_refs(
    record: &StandingRuntimeCheckpointRecord,
    pointer: &StandingRuntimeCheckpointPointer,
) -> Result<(), ApiError> {
    for output_ref in &record.checkpoint.output_manifest_refs {
        if let Some(output_manifest_key) =
            output_ref.strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX)
        {
            let (parsed_key, parts) =
                ObjectKey::parse_standing_runtime_output_manifest(output_manifest_key.to_string())
                    .map_err(ApiError::bad_request)?;
            if parsed_key.as_str() != output_manifest_key
                || parts.tenant_id != pointer.tenant_id
                || parts.program_id != pointer.program_id
                || parts.view_id != pointer.view_id
                || parts.logical_epoch != pointer.logical_epoch
            {
                return Err(ApiError::bad_request(format!(
                    "standing runtime checkpoint output manifest ref mismatch for `{}/{}/{}`",
                    pointer.tenant_id, pointer.program_id, pointer.view_id
                )));
            }
        } else if let Some(output_delta_key) =
            output_ref.strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX)
        {
            let (parsed_key, parts) =
                ObjectKey::parse_standing_runtime_output_delta(output_delta_key.to_string())
                    .map_err(ApiError::bad_request)?;
            if parsed_key.as_str() != output_delta_key
                || parts.tenant_id != pointer.tenant_id
                || parts.program_id != pointer.program_id
                || parts.view_id != pointer.view_id
                || parts.logical_epoch != pointer.logical_epoch
            {
                return Err(ApiError::bad_request(format!(
                    "standing runtime checkpoint output delta ref mismatch for `{}/{}/{}`",
                    pointer.tenant_id, pointer.program_id, pointer.view_id
                )));
            }
        } else {
            return Err(ApiError::bad_request(format!(
                "unsupported standing runtime checkpoint output ref for view `{}`",
                record.view_id
            )));
        }
    }
    Ok(())
}

async fn validate_standing_runtime_checkpoint_output_manifest_records(
    state: &ApiState,
    record: &StandingRuntimeCheckpointRecord,
) -> Result<(), ApiError> {
    for output_ref in &record.checkpoint.output_manifest_refs {
        if output_ref
            .strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX)
            .is_some()
        {
            let (_key, delta) =
                read_standing_runtime_output_delta_record(state, output_ref, &record.view_id)
                    .await?;
            if delta.tenant_id != record.checkpoint.identity.tenant_id
                || delta.program_id != record.checkpoint.identity.program_id
                || delta.view_id != record.view_id
                || delta.logical_epoch != record.checkpoint.logical_epoch
            {
                return Err(ApiError::bad_request(format!(
                    "standing runtime output delta body/checkpoint mismatch for `{}/{}/{}`",
                    record.checkpoint.identity.tenant_id,
                    record.checkpoint.identity.program_id,
                    record.view_id
                )));
            }
            continue;
        }
        let (_key, manifest) =
            read_standing_runtime_output_manifest_record(state, output_ref, &record.view_id)
                .await?;
        if manifest.tenant_id != record.checkpoint.identity.tenant_id
            || manifest.program_id != record.checkpoint.identity.program_id
            || manifest.view_id != record.view_id
            || manifest.checkpoint_key != record.checkpoint_key
            || manifest.logical_epoch != record.checkpoint.logical_epoch
            || manifest.checkpoint_content_hash != record.checkpoint.state_root.content_hash
        {
            return Err(ApiError::bad_request(format!(
                "standing runtime output manifest body/checkpoint mismatch for `{}/{}/{}`",
                record.checkpoint.identity.tenant_id,
                record.checkpoint.identity.program_id,
                record.view_id
            )));
        }
        for page in &manifest.pages {
            let (_page_key, page_record) =
                read_standing_runtime_output_page_record(state, page, &record.view_id).await?;
            if page_record.tenant_id != record.checkpoint.identity.tenant_id
                || page_record.program_id != record.checkpoint.identity.program_id
                || page_record.view_id != record.view_id
                || page_record.logical_epoch != record.checkpoint.logical_epoch
                || page_record.output_content_hash != manifest.output_content_hash
            {
                return Err(ApiError::bad_request(format!(
                    "standing runtime output page body/checkpoint mismatch for `{}/{}/{}`",
                    record.checkpoint.identity.tenant_id,
                    record.checkpoint.identity.program_id,
                    record.view_id
                )));
            }
        }
    }
    Ok(())
}

async fn read_standing_runtime_output_manifest_record(
    state: &ApiState,
    output_ref: &str,
    view_id: &str,
) -> Result<(ObjectKey, StandingRuntimeOutputManifestRecord), ApiError> {
    let output_manifest_key = output_ref
        .strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "unsupported standing runtime checkpoint output manifest ref for view `{view_id}`"
            ))
        })?;
    let bytes = state
        .store
        .get(&ObjectPath::from(output_manifest_key.to_string()))
        .await
        .map_err(ApiError::internal)?
        .bytes()
        .await
        .map_err(ApiError::internal)?;
    let manifest: StandingRuntimeOutputManifestRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    let (key, _) =
        ObjectKey::parse_standing_runtime_output_manifest(output_manifest_key.to_string())
            .map_err(ApiError::bad_request)?;
    validate_standing_runtime_output_manifest_record(&key, &manifest)?;
    Ok((key, manifest))
}

async fn read_standing_runtime_output_page_record(
    state: &ApiState,
    page: &StandingRuntimeOutputPageRef,
    view_id: &str,
) -> Result<(ObjectKey, StandingRuntimeOutputPageRecord), ApiError> {
    let bytes = state
        .store
        .get(&ObjectPath::from(page.page_key.clone()))
        .await
        .map_err(ApiError::internal)?
        .bytes()
        .await
        .map_err(ApiError::internal)?;
    let record: StandingRuntimeOutputPageRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    let (key, _) = ObjectKey::parse_standing_runtime_output_page(page.page_key.clone())
        .map_err(ApiError::bad_request)?;
    validate_standing_runtime_output_page_ref(page, view_id)?;
    validate_standing_runtime_output_page_record(&key, &record)?;
    if page.page_index != record.page_index
        || page.page_content_hash != record.page_content_hash
        || page.row_count != record.row_count
        || page.output_encoding != record.output_encoding
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output page ref/body mismatch for view `{view_id}`"
        )));
    }
    Ok((key, record))
}

async fn read_standing_runtime_output_delta_record(
    state: &ApiState,
    output_ref: &str,
    view_id: &str,
) -> Result<(ObjectKey, StandingRuntimeOutputDeltaRecord), ApiError> {
    let output_delta_key = output_ref
        .strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "unsupported standing runtime checkpoint output delta ref for view `{view_id}`"
            ))
        })?;
    let bytes = state
        .store
        .get(&ObjectPath::from(output_delta_key.to_string()))
        .await
        .map_err(ApiError::internal)?
        .bytes()
        .await
        .map_err(ApiError::internal)?;
    let record: StandingRuntimeOutputDeltaRecord = serde_json::from_slice(&bytes)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    let (key, _) = ObjectKey::parse_standing_runtime_output_delta(output_delta_key.to_string())
        .map_err(ApiError::bad_request)?;
    validate_standing_runtime_output_delta_record(&key, &record)?;
    Ok((key, record))
}

fn validate_standing_runtime_output_manifest_record(
    key: &ObjectKey,
    record: &StandingRuntimeOutputManifestRecord,
) -> Result<(), ApiError> {
    if record.schema_version != 1 || record.record_kind != "standing_runtime_output_manifest_v1" {
        return Err(ApiError::bad_request(format!(
            "standing runtime output manifest record identity mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    if record.output_encoding != "velorix-delta-batch-json-v1"
        || record.source_kind != "standing_runtime_checkpoint_published_output"
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output manifest codec/source mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    let (_, parts) = ObjectKey::parse_standing_runtime_output_manifest(key.as_str().to_string())
        .map_err(ApiError::bad_request)?;
    let output_bytes = serde_json::to_vec(&record.published_output)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let actual_output_hash = stable_bytes_hash(&output_bytes);
    let output_row_count = standing_runtime_published_output_row_count(&record.published_output)?;
    if record.pages.is_empty() {
        return Err(ApiError::bad_request(format!(
            "standing runtime output manifest has no page index for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    let mut page_indexes = BTreeSet::new();
    let mut indexed_row_count = 0usize;
    for page in &record.pages {
        validate_standing_runtime_output_page_ref(page, &record.view_id)?;
        if !page_indexes.insert(page.page_index) {
            return Err(ApiError::bad_request(format!(
                "duplicate standing runtime output page index for `{}/{}/{}`",
                record.tenant_id, record.program_id, record.view_id
            )));
        }
        let (_, page_parts) = ObjectKey::parse_standing_runtime_output_page(page.page_key.clone())
            .map_err(ApiError::bad_request)?;
        if page_parts.tenant_id != record.tenant_id
            || page_parts.program_id != record.program_id
            || page_parts.view_id != record.view_id
            || page_parts.logical_epoch != record.logical_epoch
            || page_parts.page_index != page.page_index
            || page_parts.page_content_hash != page.page_content_hash
        {
            return Err(ApiError::bad_request(format!(
                "standing runtime output page ref mismatch for `{}/{}/{}`",
                record.tenant_id, record.program_id, record.view_id
            )));
        }
        indexed_row_count = indexed_row_count
            .checked_add(page.row_count)
            .ok_or_else(|| ApiError::bad_request("standing runtime output page row overflow"))?;
    }
    if parts.tenant_id != record.tenant_id
        || parts.program_id != record.program_id
        || parts.view_id != record.view_id
        || parts.logical_epoch != record.logical_epoch
        || parts.output_content_hash != record.output_content_hash
        || record.output_content_hash != actual_output_hash
        || record.output_row_count != output_row_count
        || indexed_row_count != record.output_row_count
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output manifest key/body mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    Ok(())
}

fn validate_standing_runtime_output_delta_record(
    key: &ObjectKey,
    record: &StandingRuntimeOutputDeltaRecord,
) -> Result<(), ApiError> {
    if record.schema_version != 1 || record.record_kind != "standing_runtime_output_delta_v1" {
        return Err(ApiError::bad_request(format!(
            "standing runtime output delta record identity mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    if record.delta_encoding != "velorix-delta-batch-json-v1"
        || record.source_kind != "standing_runtime_epoch_output_delta"
        || record.schema_fingerprint.is_empty()
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output delta codec/source mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    let (_, parts) = ObjectKey::parse_standing_runtime_output_delta(key.as_str().to_string())
        .map_err(ApiError::bad_request)?;
    let delta_bytes = serde_json::to_vec(&record.output_delta)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let actual_delta_hash = stable_bytes_hash(&delta_bytes);
    let output_delta: DeltaBatch = serde_json::from_value(record.output_delta.clone())
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    let delta_row_count = output_delta
        .net_rows()
        .map_err(|_| ApiError::bad_request("standing runtime output delta is malformed"))?
        .len();
    if parts.tenant_id != record.tenant_id
        || parts.program_id != record.program_id
        || parts.view_id != record.view_id
        || parts.logical_epoch != record.logical_epoch
        || parts.delta_content_hash != record.delta_content_hash
        || actual_delta_hash != record.delta_content_hash
        || delta_row_count != record.delta_row_count
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output delta key/body mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    Ok(())
}

fn validate_standing_runtime_state_payload_record(
    key: &ObjectKey,
    record: &StandingRuntimeStatePayloadRecord,
) -> Result<(), ApiError> {
    if record.schema_version != 1 || record.record_kind != "standing_runtime_state_payload_v1" {
        return Err(ApiError::bad_request(format!(
            "standing runtime state payload record identity mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    if record.source_kind != "standing_runtime_checkpoint_state_payload"
        || record.payload.codec_identity != record.checkpoint_codec_identity
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime state payload codec/source mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    let (_, parts) = ObjectKey::parse_standing_runtime_state_payload(key.as_str().to_string())
        .map_err(ApiError::bad_request)?;
    let actual_state_hash = stable_bytes_hash(record.payload.payload.as_bytes());
    if parts.tenant_id != record.tenant_id
        || parts.program_id != record.program_id
        || parts.view_id != record.view_id
        || parts.logical_epoch != record.logical_epoch
        || parts.state_content_hash != record.state_content_hash
        || record.state_content_hash != actual_state_hash
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime state payload key/body mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    Ok(())
}

fn validate_standing_runtime_output_page_ref(
    page: &StandingRuntimeOutputPageRef,
    view_id: &str,
) -> Result<(), ApiError> {
    if page.output_encoding != "velorix-delta-batch-json-v1" {
        return Err(ApiError::bad_request(format!(
            "standing runtime output page codec mismatch for view `{view_id}`"
        )));
    }
    ObjectKey::parse_standing_runtime_output_page(page.page_key.clone())
        .map_err(ApiError::bad_request)?;
    Ok(())
}

fn validate_standing_runtime_output_page_record(
    key: &ObjectKey,
    record: &StandingRuntimeOutputPageRecord,
) -> Result<(), ApiError> {
    if record.schema_version != 1 || record.record_kind != "standing_runtime_output_page_v1" {
        return Err(ApiError::bad_request(format!(
            "standing runtime output page record identity mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    if record.output_encoding != "velorix-delta-batch-json-v1"
        || record.source_kind != "standing_runtime_checkpoint_published_output"
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output page codec/source mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
        )));
    }
    let (_, parts) = ObjectKey::parse_standing_runtime_output_page(key.as_str().to_string())
        .map_err(ApiError::bad_request)?;
    let page_bytes = serde_json::to_vec(&record.published_output)
        .map_err(|source| ApiError::internal(source.to_string()))?;
    let actual_page_hash = stable_bytes_hash(&page_bytes);
    let row_count = standing_runtime_published_output_row_count(&record.published_output)?;
    if parts.tenant_id != record.tenant_id
        || parts.program_id != record.program_id
        || parts.view_id != record.view_id
        || parts.logical_epoch != record.logical_epoch
        || parts.page_index != record.page_index
        || parts.page_content_hash != record.page_content_hash
        || record.page_content_hash != actual_page_hash
        || record.row_count != row_count
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output page key/body mismatch for `{}/{}/{}`",
            record.tenant_id, record.program_id, record.view_id
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

#[derive(Clone, Copy, Debug, Default)]
struct StandingRuntimeBackfillReplayOutcome {
    applied_batches: usize,
    remaining_batches: usize,
}

async fn replay_committed_ingest_into_standing_runtime(
    state: &ApiState,
    active: &ActiveMaterializedView,
    replay_plan: &StandingRuntimeReplayPlan,
) -> Result<(), ApiError> {
    replay_committed_ingest_into_standing_runtime_limited(state, active, replay_plan, None)
        .await
        .map(|_| ())
}

async fn replay_committed_ingest_into_standing_runtime_limited(
    state: &ApiState,
    active: &ActiveMaterializedView,
    replay_plan: &StandingRuntimeReplayPlan,
    batch_limit: Option<usize>,
) -> Result<StandingRuntimeBackfillReplayOutcome, ApiError> {
    if active.spec.input_relations.is_empty() {
        return Ok(StandingRuntimeBackfillReplayOutcome::default());
    }
    let Some(identity) = active_standing_runtime_identity(active) else {
        return Ok(StandingRuntimeBackfillReplayOutcome::default());
    };
    if batch_limit.is_some_and(|limit| limit == 0) {
        return Err(ApiError::bad_request(
            "backfill batch_limit must be a positive integer",
        ));
    }
    let operation_lock = state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
    let _operation_guard = operation_lock.lock().await;
    let Some(runtime) = state.standing_runtime(identity, &active.spec.view_id)? else {
        return Ok(StandingRuntimeBackfillReplayOutcome::default());
    };
    let ingest_log =
        IngestLog::new_catalog_checked(Arc::clone(&state.store), state.capabilities.as_ref())
            .map_err(ApiError::internal)?;
    let batches = ingest_log
        .replay_admitted_validated_envelopes_from(&replay_plan.replay_checkpoints)
        .await
        .map_err(ApiError::internal)?;

    let mut outcome = StandingRuntimeBackfillReplayOutcome::default();
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
        if batch_limit.is_some_and(|limit| outcome.applied_batches >= limit) {
            outcome.remaining_batches += 1;
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
            event_time_watermark: header.event_time_watermark.clone(),
            batches: envelope.record_batches().map_err(ApiError::bad_request)?,
        };
        let apply_result = apply_standing_runtime_changes_and_checkpoint(
            Arc::clone(&runtime),
            descriptor.end_offset_exclusive,
            idempotency_key,
            input_batch,
        )
        .await?;
        if let Err(error) = persist_standing_runtime_checkpoint(
            state,
            &active.spec.view_id,
            &apply_result.checkpoint,
            &apply_result.output_deltas,
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
        outcome.applied_batches += 1;
    }

    Ok(outcome)
}

async fn committed_backfill_progress(
    state: &ApiState,
    active: &ActiveMaterializedView,
) -> Result<BackfillProgressResponse, ApiError> {
    if active.spec.input_relations.is_empty() {
        return Ok(backfill_progress_response(0, 0));
    }
    let replay_plan = match active_standing_runtime_identity(active) {
        Some(identity) => {
            read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id)
                .await?
                .as_ref()
                .map(standing_runtime_replay_plan_from_record_ref)
                .unwrap_or_default()
        }
        None => StandingRuntimeReplayPlan::default(),
    };
    let ingest_log =
        IngestLog::new_catalog_checked(Arc::clone(&state.store), state.capabilities.as_ref())
            .map_err(ApiError::internal)?;
    let batches = ingest_log
        .replay_admitted_validated_envelopes_from(&[])
        .await
        .map_err(ApiError::internal)?;
    let mut total = 0usize;
    let mut remaining = 0usize;
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
        total += 1;
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
        remaining += 1;
    }
    Ok(backfill_progress_response(total, remaining))
}

fn backfill_progress_response(total: usize, remaining: usize) -> BackfillProgressResponse {
    let processed = total.saturating_sub(remaining);
    let percent = if total == 0 {
        100.0
    } else {
        (processed as f64 / total as f64) * 100.0
    };
    BackfillProgressResponse {
        processed_batches: processed,
        remaining_batches: remaining,
        total_batches: total,
        percent,
    }
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
    let active = ensure_view_query_ready(&state, active).await?;
    ensure_view_execution_allowed(&active)?;
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
            let is_materialized_runtime = is_external_sql_runtime(&active);
            validate_standing_runtime_query_contract(
                &active.spec.view_id,
                request_sql.as_ref(),
                &api,
                &parameters,
                &page_request,
                is_materialized_runtime,
            )?;
            let (rows, logical_epoch, next_page_token) = if let Some(sql) = request_sql {
                let requested_epoch = page_request.committed_epoch;
                let sql = render_caller_sql_as_bound_sql(&sql, &parameters)?;
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

async fn ensure_view_query_ready(
    state: &ApiState,
    active: ActiveMaterializedView,
) -> Result<ActiveMaterializedView, ApiError> {
    if view_query_availability(&active.lifecycle) {
        return Ok(active);
    }
    if !view_backfill_is_query_triggerable(&active) {
        ensure_view_execution_allowed(&active)?;
        return Ok(active);
    }

    let outcome = run_active_view_backfill_step(state, active, None).await?;
    let refreshed = outcome.active;
    ensure_view_execution_allowed(&refreshed)?;
    Ok(refreshed)
}

struct ActiveViewBackfillStepOutcome {
    active: ActiveMaterializedView,
    replay: StandingRuntimeBackfillReplayOutcome,
}

async fn run_view_backfill_step(
    state: &ApiState,
    view_id: &str,
    batch_limit: Option<usize>,
) -> Result<BackfillViewResponse, ApiError> {
    let active = state
        .view_registry()?
        .read_active(view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    let outcome = run_active_view_backfill_step(state, active, batch_limit).await?;
    let progress = committed_backfill_progress(state, &outcome.active).await?;
    Ok(backfill_view_response(
        &outcome.active,
        if outcome.replay.remaining_batches == 0 {
            "completed"
        } else {
            "advanced"
        },
        "sync",
        outcome.replay.applied_batches,
        outcome.replay.remaining_batches,
        progress,
    ))
}

async fn run_active_view_backfill_step(
    state: &ApiState,
    active: ActiveMaterializedView,
    batch_limit: Option<usize>,
) -> Result<ActiveViewBackfillStepOutcome, ApiError> {
    if view_query_availability(&active.lifecycle) {
        return Ok(ActiveViewBackfillStepOutcome {
            active,
            replay: StandingRuntimeBackfillReplayOutcome::default(),
        });
    }
    if !view_backfill_is_query_triggerable(&active) {
        ensure_view_execution_allowed(&active)?;
        return Ok(ActiveViewBackfillStepOutcome {
            active,
            replay: StandingRuntimeBackfillReplayOutcome::default(),
        });
    }
    let Some(identity) = active_standing_runtime_identity(&active) else {
        return Err(ApiError::service_unavailable(format!(
            "standing_runtime_not_deployed: view `{}` is backfill pending but has no runtime binding",
            active.spec.view_id
        )));
    };
    let replay_plan = if state
        .standing_runtime(identity, &active.spec.view_id)?
        .is_some()
    {
        read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id)
            .await?
            .as_ref()
            .map(standing_runtime_replay_plan_from_record_ref)
            .unwrap_or_default()
    } else {
        ensure_standing_runtime_for_active_view(state, &active)
            .await?
            .unwrap_or_default()
    };
    let replay = replay_committed_ingest_into_standing_runtime_limited(
        state,
        &active,
        &replay_plan,
        batch_limit,
    )
    .await?;
    if replay.remaining_batches == 0 {
        state
            .view_registry()?
            .update_standing_runtime_lifecycle(
                &active.spec.view_id,
                &active.spec_hash,
                MaterializedViewLifecycleStatus::standing_runtime(),
            )
            .await
            .map_err(materialized_view_registry_error_to_api)?;
    }

    let refreshed = state
        .view_registry()?
        .read_active(&active.spec.view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    Ok(ActiveViewBackfillStepOutcome {
        active: refreshed,
        replay,
    })
}

fn spawn_background_view_backfill(
    state: ApiState,
    view_id: String,
    batch_limit: usize,
    pause_ms: u64,
) {
    tokio::spawn(async move {
        loop {
            let outcome = run_view_backfill_step(&state, &view_id, Some(batch_limit)).await;
            match outcome {
                Ok(response) if response.remaining_batches == 0 => break,
                Ok(response) if response.applied_batches == 0 => break,
                Ok(_) => tokio::time::sleep(Duration::from_millis(pause_ms)).await,
                Err(_) => break,
            }
        }
    });
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
    if is_external_sql_runtime(active) {
        let sql = render_view_sql_template_as_bound_sql(sql_template, &api.request, parameters)?;
        let page_request = page_request_with_query_policy_limit(page_request, query_policy.policy);
        let page = standing_runtime_sql_page(state, active, output_id, sql, page_request).await?;
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

fn is_external_sql_runtime(active: &ActiveMaterializedView) -> bool {
    active
        .artifact
        .as_ref()
        .is_some_and(|artifact| artifact.execution_path == "external_sql_runtime")
}

fn validate_standing_runtime_sql_page(
    active: &ActiveMaterializedView,
    output_id: &str,
    page: &MaterializedViewSqlPage,
    requested_epoch: Option<u64>,
) -> Result<(), ApiError> {
    let identity = active_standing_runtime_identity(active).ok_or_else(|| {
        ApiError::conflict(format!(
            "standing runtime view `{}` is missing runtime identity",
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
    let identity = active_standing_runtime_identity(active).ok_or_else(|| {
        ApiError::conflict(format!(
            "standing runtime view `{}` is missing runtime identity",
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
    let expected_arrow_schema = arrow_schema_from_incremental_relation_schema(output_schema)?;
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
    let sql = format!("SELECT * FROM {}", sql_quoted_identifier(output_id));
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
        record_batches_to_json_rows_for_view_schema(output_schema, &batches)?,
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
    let identity = active_standing_runtime_identity(active).ok_or_else(|| {
        ApiError::conflict(format!(
            "standing runtime view `{}` is missing runtime identity",
            active.spec.view_id
        ))
    })?;
    standing_runtime_page_from_output_manifest(
        state,
        active,
        identity,
        output_id,
        page_request.clone(),
    )
    .await?
    .ok_or_else(|| {
        ApiError::service_unavailable(format!(
            "standing runtime output manifest is unavailable for view `{}` output `{output_id}`",
            active.spec.view_id
        ))
    })
}

async fn standing_runtime_page_from_output_manifest(
    state: &ApiState,
    active: &ActiveMaterializedView,
    identity: &StandingProgramIdentity,
    output_id: &str,
    page_request: SnapshotPageRequest,
) -> Result<Option<MaterializedViewPage>, ApiError> {
    let Some(record) =
        read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id).await?
    else {
        return Ok(None);
    };
    let Some(output_ref) = record
        .checkpoint
        .output_manifest_refs
        .iter()
        .find(|output_ref| {
            output_ref
                .strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX)
                .and_then(|key| {
                    ObjectKey::parse_standing_runtime_output_manifest(key.to_string())
                        .ok()
                        .map(|(_, parts)| parts)
                })
                .is_some_and(|parts| parts.view_id == output_id)
        })
    else {
        return Ok(None);
    };
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
    let (_key, manifest) =
        read_standing_runtime_output_manifest_record(state, output_ref, &active.spec.view_id)
            .await?;
    if manifest.checkpoint_key != record.checkpoint_key
        || manifest.logical_epoch != record.checkpoint.logical_epoch
        || manifest.checkpoint_content_hash != record.checkpoint.state_root.content_hash
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output manifest is not bound to the latest checkpoint for `{}/{}/{}`",
            identity.tenant_id, identity.program_id, active.spec.view_id
        )));
    }
    let published_output =
        standing_runtime_published_output_from_manifest_page(state, &manifest).await?;
    let aggregate_outputs =
        standing_runtime_output_aggregate_outputs_for_checkpoint(&record.checkpoint)?;
    let scoped_view = ScopedViewId {
        tenant_id: identity.tenant_id.clone(),
        program_id: identity.program_id.clone(),
        view_id: output_id.to_string(),
    };
    let page = velorix_runtime::materialized_view_runtime::materialized_delta_to_page(
        output_schema,
        &published_output,
        scoped_view,
        record.checkpoint.logical_epoch,
        page_request,
        aggregate_outputs.as_deref(),
    )
    .map_err(ApiError::bad_request)?;
    Ok(Some(page))
}

async fn standing_runtime_published_output_from_manifest_page(
    state: &ApiState,
    manifest: &StandingRuntimeOutputManifestRecord,
) -> Result<DeltaBatch, ApiError> {
    let Some(page) = manifest.pages.iter().find(|page| page.page_index == 0) else {
        return Err(ApiError::bad_request(format!(
            "standing runtime output manifest has no first page for `{}/{}/{}`",
            manifest.tenant_id, manifest.program_id, manifest.view_id
        )));
    };
    let (_key, page_record) =
        read_standing_runtime_output_page_record(state, page, &manifest.view_id).await?;
    if page_record.output_content_hash != manifest.output_content_hash
        || page_record.logical_epoch != manifest.logical_epoch
        || page_record.tenant_id != manifest.tenant_id
        || page_record.program_id != manifest.program_id
        || page_record.view_id != manifest.view_id
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime output page is not bound to manifest for `{}/{}/{}`",
            manifest.tenant_id, manifest.program_id, manifest.view_id
        )));
    }
    serde_json::from_value(page_record.published_output)
        .map_err(|source| ApiError::bad_request(source.to_string()))
}

fn standing_runtime_output_aggregate_outputs_for_checkpoint(
    checkpoint: &RuntimeCheckpoint,
) -> Result<Option<Vec<SupportedAggregateOutput>>, ApiError> {
    let Some(state_payload) = &checkpoint.state_payload else {
        return Ok(None);
    };
    let payload: Value = serde_json::from_str(&state_payload.payload)
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    if payload
        .get("runtime_kind")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "latest_by_key")
    {
        return Ok(None);
    }
    if payload
        .get("runtime_kind")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "two_input_join_sum_count")
    {
        return Ok(Some(default_standing_runtime_sum_count_outputs()));
    }
    if payload
        .get("runtime_kind")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "tumbling_event_time_aggregate")
    {
        let Some(plan) = payload.get("plan").filter(|plan| !plan.is_null()) else {
            return Ok(None);
        };
        let plan: SupportedTumblingWindowPlan = serde_json::from_value(plan.clone())
            .map_err(|source| ApiError::bad_request(source.to_string()))?;
        return Ok(Some(plan.aggregate_outputs));
    }
    let Some(plan) = payload.get("plan").filter(|plan| !plan.is_null()) else {
        return Ok(None);
    };
    let plan: SupportedViewPlan = serde_json::from_value(plan.clone())
        .map_err(|source| ApiError::bad_request(source.to_string()))?;
    Ok(Some(supported_view_plan_aggregate_outputs(&plan)))
}

fn default_standing_runtime_sum_count_outputs() -> Vec<SupportedAggregateOutput> {
    vec![
        SupportedAggregateOutput {
            function: LogicalPlanAggregateFunctionV1::Sum,
            input_column_id: None,
            output_column_id: "sum".to_string(),
        },
        SupportedAggregateOutput {
            function: LogicalPlanAggregateFunctionV1::Count,
            input_column_id: None,
            output_column_id: "count".to_string(),
        },
    ]
}

async fn standing_runtime_sql_page(
    state: &ApiState,
    active: &ActiveMaterializedView,
    output_id: &str,
    sql: String,
    page_request: SnapshotPageRequest,
) -> Result<MaterializedViewSqlPage, ApiError> {
    state.validate_standing_runtime_fencing_or_evict().await?;
    let identity = active_standing_runtime_identity(active).ok_or_else(|| {
        ApiError::conflict(format!(
            "standing runtime view `{}` is missing runtime identity",
            active.spec.view_id
        ))
    })?;
    if state
        .standing_runtime(identity, &active.spec.view_id)?
        .is_none()
    {
        let _ = ensure_standing_runtime_for_active_view(state, active).await?;
    }
    let operation_lock = state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
    let _operation_guard = operation_lock.lock().await;
    let runtime = state
        .standing_runtime(identity, &active.spec.view_id)?
        .ok_or_else(|| {
            ApiError::service_unavailable(format!(
                "standing runtime is unavailable for view `{}`",
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

fn arrow_schema_from_incremental_relation_schema(
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

fn json_reader_column_to_arrow_array(
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
        let value = row_column_value_for_json_reader(column, row)?;
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
            "column `{}` produced {} rows for {} JSON rows",
            column.name,
            batch.num_rows(),
            rows.len()
        ));
    }
    Ok(batch.column(0).clone())
}

fn row_column_value_for_json_reader(column: &ColumnSchema, row: &Value) -> Result<Value, String> {
    let object = row
        .as_object()
        .ok_or_else(|| "query row must be an object".to_string())?;
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
    if sql_template_validation_mode != SqlTemplateValidationMode::ExternalSqlRuntime
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
    if sql_template_validation_mode == SqlTemplateValidationMode::ExternalSqlRuntime {
        return Ok(());
    }
    let table_schema = arrow_schema_from_incremental_relation_schema(output_schema)?;
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
    allow_materialized_runtime_sql: bool,
) -> Result<(), ApiError> {
    if request_sql.is_some() && !allow_materialized_runtime_sql {
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
    if api.sql_template.is_some()
        && page_request.page_token.is_some()
        && !allow_materialized_runtime_sql
    {
        return Err(ApiError::bad_request(format!(
            "cursor pagination is not supported for templated standing runtime view `{view_id}`"
        )));
    }
    if api.sql_template.is_some()
        && page_request.max_rows.is_some()
        && !allow_materialized_runtime_sql
    {
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
            "request field `{name}` declares unsupported type `variant`: external SQL runtime /query does not support request-time VARIANT bind literals; use type `json` for canonical JSON text parameters or compute VARIANT inside a materialized runtime view"
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

const PREPARED_QUERY_NAME: &str = "velorix_query";

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

fn render_view_sql_template_as_bound_sql(
    template: &str,
    fields: &[MaterializedViewRequestFieldSpec],
    values: &BTreeMap<String, Value>,
) -> Result<String, ApiError> {
    let bound_sql = render_view_sql_template(template, fields, values)?;
    let bound_sql = rewrite_array_unnest_placeholders(bound_sql)?;
    prepared_query_sql(bound_sql)
}

fn render_caller_sql_as_bound_sql(
    sql: &str,
    values: &BTreeMap<String, Value>,
) -> Result<String, ApiError> {
    let bound_sql = render_caller_sql_as_parameterized_bound_sql(sql, values)?;
    let bound_sql = rewrite_array_unnest_placeholders(bound_sql)?;
    prepared_query_sql(bound_sql)
}

fn render_caller_sql_as_parameterized_bound_sql(
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

fn prepared_query_sql(bound_sql: BoundViewSql) -> Result<String, ApiError> {
    if bound_sql.bind_values.is_empty() {
        return Ok(bound_sql.sql);
    }
    let query_sql = trim_prepared_statement_sql(&bound_sql.sql);
    if query_sql.is_empty() {
        return Err(ApiError::bad_request(
            "materialized runtime prepared query SQL cannot be empty",
        ));
    }
    if sql_has_statement_separator(query_sql) {
        return Err(ApiError::bad_request(
            "materialized runtime prepared query parameters require a single SQL statement",
        ));
    }
    let args = bound_sql
        .bind_values
        .iter()
        .map(query_bind_value_to_sql_literal)
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    Ok(format!(
        "PREPARE {PREPARED_QUERY_NAME} AS {query_sql};\nEXECUTE {PREPARED_QUERY_NAME}({args});"
    ))
}

fn rewrite_array_unnest_placeholders(bound_sql: BoundViewSql) -> Result<BoundViewSql, ApiError> {
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
        if let Some((end, index)) = parse_in_unnest_placeholder(&bound_sql.sql, offset) {
            let value = bind_value_by_one_based_index(&bound_sql.bind_values, index)?;
            let Some(values) = array_bind_value_elements(value) else {
                return Err(ApiError::bad_request(
                    "materialized runtime IN UNNEST query parameter must be an array",
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
            if let Some((end, index)) = parse_numbered_placeholder(&bound_sql.sql, offset) {
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
            .expect("non-empty SQL slice while rewriting materialized runtime placeholders");
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
        .ok_or_else(|| ApiError::bad_request("query placeholder index is out of range"))
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

fn parse_in_unnest_placeholder(sql: &str, offset: usize) -> Option<(usize, usize)> {
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
    let (after_placeholder, index) = parse_numbered_placeholder(sql, cursor)?;
    cursor = skip_ascii_whitespace(sql, after_placeholder);
    cursor = consume_ascii_byte(sql, cursor, b')')?;
    Some((cursor, index))
}

fn parse_numbered_placeholder(sql: &str, offset: usize) -> Option<(usize, usize)> {
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

fn trim_prepared_statement_sql(sql: &str) -> &str {
    sql.trim().trim_end_matches(';').trim_end()
}

fn sql_has_statement_separator(sql: &str) -> bool {
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
            .expect("non-empty SQL slice while scanning SQL");
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
    if let Some(value) = typed_sql_literal_bind_value_for_json_value(name, value, filters)? {
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

    inferred_bind_value_for_json_value(name, value)
}

fn inferred_bind_value_for_json_value(
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

fn query_bind_value_to_sql_literal(value: &QueryBindValue) -> Result<String, ApiError> {
    match value {
        QueryBindValue::Utf8(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
        QueryBindValue::Json(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
        QueryBindValue::Int64(value) => Ok(value.to_string()),
        QueryBindValue::Float64(value) if value.is_finite() => Ok(value.to_string()),
        QueryBindValue::Float64(_) => Err(ApiError::bad_request(
            "non-finite float query parameter cannot be rendered as SQL",
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
                            "non-finite float query parameter cannot be rendered as SQL",
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
    if let Some(value) = typed_sql_literal_bind_value_for_json_value(name, value, filters)? {
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

fn typed_sql_literal_bind_value_for_json_value(
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
        "parameter `{name}` uses unsupported SQL template filter `is_variant`: external SQL runtime /query does not support request-time VARIANT bind literals; use `is_json` for canonical JSON text parameters or compute VARIANT inside a materialized runtime view"
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

    let query_enabled = view_query_availability(&lifecycle);
    let coverage = materialization_coverage_response(&lifecycle);

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
        coverage,
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

fn backfill_view_response(
    active: &ActiveMaterializedView,
    outcome: &str,
    mode: &str,
    applied_batches: usize,
    remaining_batches: usize,
    progress: BackfillProgressResponse,
) -> BackfillViewResponse {
    BackfillViewResponse {
        view_id: active.spec.view_id.clone(),
        outcome: outcome.to_string(),
        mode: mode.to_string(),
        lifecycle: active.lifecycle.clone(),
        query_enabled: view_query_availability(&active.lifecycle),
        coverage: materialization_coverage_response(&active.lifecycle),
        progress,
        applied_batches,
        remaining_batches,
    }
}

fn lifecycle_for_create_view_execution(
    execution_mode: &MaterializedViewExecutionMode,
    requires_backfill: bool,
) -> MaterializedViewLifecycleStatus {
    match execution_mode {
        MaterializedViewExecutionMode::StandingRuntime if requires_backfill => {
            MaterializedViewLifecycleStatus::standing_runtime_deploying(Some(
                "backfill_required: committed input data exists; first query will materialize the view before serving rows".to_string(),
            ))
        }
        MaterializedViewExecutionMode::StandingRuntime => MaterializedViewLifecycleStatus::standing_runtime(),
    }
}

fn view_query_availability(lifecycle: &MaterializedViewLifecycleStatus) -> bool {
    lifecycle.compile_status == MaterializedViewCompileStatus::Success
        && lifecycle.deployment_status == MaterializedViewDeploymentStatus::Running
}

fn materialization_coverage_response(
    lifecycle: &MaterializedViewLifecycleStatus,
) -> MaterializationCoverageResponse {
    let state = if view_query_availability(lifecycle) {
        "materialized"
    } else if lifecycle.deployment_status == MaterializedViewDeploymentStatus::Deploying
        && lifecycle
            .message
            .as_deref()
            .is_some_and(|message| message.contains("backfill_required"))
    {
        "backfill_required"
    } else if lifecycle.deployment_status == MaterializedViewDeploymentStatus::Failed {
        "failed"
    } else {
        "unavailable"
    };
    MaterializationCoverageResponse {
        state: state.to_string(),
        full_view: CoverageCapabilityResponse {
            status: "available".to_string(),
            reason: "full-view materialization is backed by committed ingest replay and durable standing-runtime checkpoints".to_string(),
        },
        request_scope: CoverageCapabilityResponse {
            status: "unsupported".to_string(),
            reason: "request-scope backfill needs a durable input scope index; current ingest logs are checkpoint-contiguous by stream/partition".to_string(),
        },
        range: CoverageCapabilityResponse {
            status: "unsupported".to_string(),
            reason: "arbitrary range backfill would violate contiguous input frontier semantics without a new range/index contract".to_string(),
        },
        background_backfill: CoverageCapabilityResponse {
            status: "available".to_string(),
            reason: "backfill can run in bounded committed-ingest batches through the view backfill API".to_string(),
        },
    }
}

fn view_backfill_is_query_triggerable(active: &ActiveMaterializedView) -> bool {
    active.lifecycle.compile_status == MaterializedViewCompileStatus::Success
        && active.lifecycle.deployment_status == MaterializedViewDeploymentStatus::Deploying
        && active
            .lifecycle
            .message
            .as_deref()
            .is_some_and(|message| message.contains("backfill_required"))
}

fn standing_runtime_can_accept_incremental_ingest(active: &ActiveMaterializedView) -> bool {
    view_query_availability(&active.lifecycle)
}

fn view_spec_from_request(
    state: &ApiState,
    request: &CreateViewRequest,
    catalogs: &[VelorixRelationCatalogV1],
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
    let output_relations = if source_kind == SqlSourceKind::StandingView {
        state
            .materialized_runtime_output_schemas_for_view_request(
                request.view_id.as_str(),
                request.sql.as_str(),
                catalogs,
                input.schema_fingerprint.as_str(),
            )?
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "unsupported view SQL for materialized runtime `{}`",
                    request.view_id
                ))
            })
    } else {
        return Err(ApiError::bad_request(
            "CREATE VIEW SQL requires a supported materialized view runtime; runtime fallback is disabled",
        ));
    }?;
    let multi_output = output_relations.len() > 1;
    Ok(StandingViewSpec {
        view_id: request.view_id.clone(),
        sql: request.sql.clone(),
        dialect: SqlDialect::VelorixSql,
        source_kind,
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
    if !request.output_relation_ids.is_empty() {
        return Err(ApiError::bad_request(
            "output_relation_ids are not supported by the local materialized runtime",
        ));
    }
    Ok(())
}

fn single_key_sum_count_output_schema(
    view_id: &str,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedViewPlan,
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
    let aggregate_outputs = supported_view_plan_aggregate_outputs(plan);
    let mut columns = Vec::with_capacity(1 + aggregate_outputs.len());
    columns.push(ColumnSchema {
        name: key_column.name.clone(),
        data_type: key_type,
        nullable: false,
    });
    for aggregate in &aggregate_outputs {
        columns.push(ColumnSchema {
            name: aggregate.output_column_id.clone(),
            data_type: single_key_aggregate_output_type(catalog, aggregate)?,
            nullable: false,
        });
    }
    let primary_key = vec![key_column.name.clone()];
    let schema_fingerprint =
        materialized_output_schema_fingerprint(view_id, "v1", &columns, &primary_key)?;
    Ok(RelationSchema {
        relation_id: view_id.to_string(),
        relation_name: view_id.to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint,
        columns,
        primary_key,
    })
}

fn latest_by_key_output_schema(
    view_id: &str,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedLatestByKeyPlan,
) -> Result<RelationSchema, ApiError> {
    let [primary_key_id] = catalog.relation_schema.primary_key_column_ids.as_slice() else {
        return Err(ApiError::bad_request(
            "latest-by-key view requires exactly one primary key column",
        ));
    };
    let key_column = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| &column.column_id == primary_key_id)
        .ok_or_else(|| ApiError::bad_request("primary key column is missing from catalog"))?;
    let value_column = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == plan.value_column_id)
        .ok_or_else(|| {
            ApiError::bad_request("latest-by-key value column is missing from catalog")
        })?;
    let columns = vec![
        ColumnSchema {
            name: key_column.name.clone(),
            data_type: sql_type_from_catalog_column(key_column)?,
            nullable: false,
        },
        ColumnSchema {
            name: plan.output_value_column_id.clone(),
            data_type: sql_type_from_catalog_column(value_column)?,
            nullable: false,
        },
    ];
    let primary_key = vec![key_column.name.clone()];
    let schema_fingerprint =
        materialized_output_schema_fingerprint(view_id, "v1", &columns, &primary_key)?;
    Ok(RelationSchema {
        relation_id: view_id.to_string(),
        relation_name: view_id.to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint,
        columns,
        primary_key,
    })
}

fn tumbling_window_output_schema(
    view_id: &str,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedTumblingWindowPlan,
) -> Result<RelationSchema, ApiError> {
    let [primary_key_id] = catalog.relation_schema.primary_key_column_ids.as_slice() else {
        return Err(ApiError::bad_request(
            "tumbling window view requires exactly one primary key column",
        ));
    };
    let key_column = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| &column.column_id == primary_key_id)
        .ok_or_else(|| ApiError::bad_request("primary key column is missing from catalog"))?;
    let mut columns = vec![
        ColumnSchema {
            name: key_column.name.clone(),
            data_type: sql_type_from_catalog_column(key_column)?,
            nullable: false,
        },
        ColumnSchema {
            name: plan.window_start_output_column_id.clone(),
            data_type: SqlDataType::Int64,
            nullable: false,
        },
        ColumnSchema {
            name: plan.window_end_output_column_id.clone(),
            data_type: SqlDataType::Int64,
            nullable: false,
        },
    ];
    for aggregate in &plan.aggregate_outputs {
        columns.push(ColumnSchema {
            name: aggregate.output_column_id.clone(),
            data_type: single_key_aggregate_output_type(catalog, aggregate)?,
            nullable: false,
        });
    }
    let primary_key = vec![
        key_column.name.clone(),
        plan.window_start_output_column_id.clone(),
        plan.window_end_output_column_id.clone(),
    ];
    let schema_fingerprint =
        materialized_output_schema_fingerprint(view_id, "v1", &columns, &primary_key)?;
    Ok(RelationSchema {
        relation_id: view_id.to_string(),
        relation_name: view_id.to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint,
        columns,
        primary_key,
    })
}

fn join_sum_count_output_schema(
    view_id: &str,
    catalogs: &[VelorixRelationCatalogV1],
    plan: &SupportedJoinViewPlan,
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
    let sum_type =
        generic_single_key_sum_count_sum_type_for_column(left_catalog, &plan.sum_value_column_id)?;
    let columns = vec![
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
    ];
    let primary_key = vec![key_column.name.clone()];
    let schema_fingerprint =
        materialized_output_schema_fingerprint(view_id, "v1", &columns, &primary_key)?;
    Ok(RelationSchema {
        relation_id: view_id.to_string(),
        relation_name: view_id.to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint,
        columns,
        primary_key,
    })
}

fn materialized_output_schema_fingerprint(
    relation_id: &str,
    relation_version: &str,
    columns: &[ColumnSchema],
    primary_key: &[String],
) -> Result<String, ApiError> {
    let canonical = json!({
        "relation_id": relation_id,
        "relation_name": relation_id,
        "relation_version": relation_version,
        "columns": columns,
        "primary_key": primary_key,
    });
    let bytes =
        serde_json::to_vec(&canonical).map_err(|source| ApiError::internal(source.to_string()))?;
    Ok(stable_bytes_hash(&bytes))
}

fn validate_join_plan_catalog_order(
    plan: &SupportedJoinViewPlan,
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

fn generic_single_key_sum_count_sum_type_for_column(
    catalog: &VelorixRelationCatalogV1,
    column_id: &str,
) -> Result<SqlDataType, ApiError> {
    let value = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == column_id)
        .ok_or_else(|| ApiError::bad_request("sum value column is missing from catalog"))?;
    match &value.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 => Ok(SqlDataType::Int64),
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => Ok(SqlDataType::Decimal {
            precision: *precision,
            scale: *scale,
        }),
        _ => Err(ApiError::bad_request(format!(
            "single-key sum/count materialized runtime value column `{}` must be Int64 or Decimal128",
            value.name
        ))),
    }
}

fn single_key_aggregate_output_type(
    catalog: &VelorixRelationCatalogV1,
    aggregate: &SupportedAggregateOutput,
) -> Result<SqlDataType, ApiError> {
    match aggregate.function {
        LogicalPlanAggregateFunctionV1::Sum => {
            let column_id = aggregate
                .input_column_id
                .as_ref()
                .ok_or_else(|| ApiError::bad_request("sum aggregate input column is missing"))?;
            generic_single_key_sum_count_sum_type_for_column(catalog, column_id)
        }
        LogicalPlanAggregateFunctionV1::Count => Ok(SqlDataType::Int64),
        LogicalPlanAggregateFunctionV1::Avg => {
            let column_id = aggregate
                .input_column_id
                .as_ref()
                .ok_or_else(|| ApiError::bad_request("avg aggregate input column is missing"))?;
            let value = catalog
                .relation_schema
                .columns
                .iter()
                .find(|column| &column.column_id == column_id)
                .ok_or_else(|| ApiError::bad_request("avg value column is missing from catalog"))?;
            match &value.physical_arrow_type {
                ArrowPhysicalTypeV1::Int64 => Ok(SqlDataType::Float64),
                _ => Err(ApiError::bad_request(format!(
                    "single-key materialized runtime avg column `{}` must be Int64",
                    value.name
                ))),
            }
        }
        LogicalPlanAggregateFunctionV1::Min | LogicalPlanAggregateFunctionV1::Max => {
            let column_id = aggregate.input_column_id.as_ref().ok_or_else(|| {
                ApiError::bad_request("min/max aggregate input column is missing")
            })?;
            generic_single_key_sum_count_sum_type_for_column(catalog, column_id)
        }
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
        .validate_ingest_adapter_scope()
        .map_err(ApiError::bad_request)?;
    let schema = datafusion_schema_from_catalog(catalog).map_err(ApiError::bad_request)?;
    let arrays = catalog
        .relation_schema
        .columns
        .iter()
        .map(|column| relation_json_column_to_arrow_array(column, rows))
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema, arrays).map_err(ApiError::bad_request)
}

fn relation_json_column_to_arrow_array(
    column: &RelationColumnV1,
    rows: &[Value],
) -> Result<ArrayRef, ApiError> {
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Boolean => Ok(Arc::new(BooleanArray::from(
            collect_relation_column_values(column, rows, json_bool_value)?,
        ))),
        ArrowPhysicalTypeV1::Int8 => Ok(Arc::new(Int8Array::from(collect_relation_column_values(
            column,
            rows,
            json_i8_value,
        )?))),
        ArrowPhysicalTypeV1::Int16 => Ok(Arc::new(Int16Array::from(
            collect_relation_column_values(column, rows, json_i16_value)?,
        ))),
        ArrowPhysicalTypeV1::Int32 => Ok(Arc::new(Int32Array::from(
            collect_relation_column_values(column, rows, json_i32_value)?,
        ))),
        ArrowPhysicalTypeV1::Int64 => Ok(Arc::new(Int64Array::from(
            collect_relation_column_values(column, rows, json_i64_value)?,
        ))),
        ArrowPhysicalTypeV1::UInt8 => Ok(Arc::new(UInt8Array::from(
            collect_relation_column_values(column, rows, json_u8_value)?,
        ))),
        ArrowPhysicalTypeV1::UInt16 => Ok(Arc::new(UInt16Array::from(
            collect_relation_column_values(column, rows, json_u16_value)?,
        ))),
        ArrowPhysicalTypeV1::UInt32 => Ok(Arc::new(UInt32Array::from(
            collect_relation_column_values(column, rows, json_u32_value)?,
        ))),
        ArrowPhysicalTypeV1::UInt64 => Ok(Arc::new(UInt64Array::from(
            collect_relation_column_values(column, rows, json_u64_value)?,
        ))),
        ArrowPhysicalTypeV1::Float32 => Ok(Arc::new(Float32Array::from(
            collect_relation_column_values(column, rows, json_f32_value)?,
        ))),
        ArrowPhysicalTypeV1::Float64 => Ok(Arc::new(Float64Array::from(
            collect_relation_column_values(column, rows, json_f64_value)?,
        ))),
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => {
            let scale_i8 = i8::try_from(*scale)
                .map_err(|_| ApiError::bad_request("decimal scale is out of range"))?;
            let values = collect_relation_column_values(column, rows, |column, value| {
                json_decimal128_value(column, value, *precision, *scale)
            })?;
            Ok(Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(*precision, scale_i8)
                    .map_err(ApiError::bad_request)?,
            ))
        }
        ArrowPhysicalTypeV1::Utf8 => Ok(Arc::new(StringArray::from(
            collect_relation_column_values(column, rows, json_string_value)?,
        ))),
        ArrowPhysicalTypeV1::Binary => {
            let values = collect_relation_column_values(column, rows, json_binary_value)?;
            Ok(Arc::new(BinaryArray::from_iter(
                values.iter().map(|value| value.as_deref()),
            )))
        }
        ArrowPhysicalTypeV1::Date32 => Ok(Arc::new(Date32Array::from(
            collect_relation_column_values(column, rows, json_date32_value)?,
        ))),
        ArrowPhysicalTypeV1::Time64Nanosecond => Ok(Arc::new(Time64NanosecondArray::from(
            collect_relation_column_values(column, rows, json_time64_nanos_value)?,
        ))),
        ArrowPhysicalTypeV1::TimestampNanosecond { timezone } => {
            let array = TimestampNanosecondArray::from(collect_relation_column_values(
                column,
                rows,
                json_timestamp_nanos_value,
            )?)
            .with_timezone_opt(timezone.clone());
            Ok(Arc::new(array))
        }
        ArrowPhysicalTypeV1::DictionaryUtf8 { key_type, .. } => {
            let values = collect_relation_column_values(column, rows, json_string_value)?;
            dictionary_utf8_array(key_type, values)
        }
        ArrowPhysicalTypeV1::JsonUtf8 => Ok(Arc::new(StringArray::from(
            collect_relation_column_values(column, rows, json_canonical_string_value)?,
        ))),
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
    json_reader_column_to_arrow_array(&schema, rows).map_err(ApiError::bad_request)
}

fn collect_relation_column_values<T>(
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

fn record_batches_to_json_rows_for_view_schema(
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
                    "query JSON output column `{column}` must be stored as canonical JSON text"
                ))
            })?;
            *value = serde_json::from_str(raw).map_err(|error| {
                ApiError::internal(format!(
                    "query JSON output column `{column}` contains invalid canonical JSON: {error}"
                ))
            })?;
        }
    }
    Ok(rows)
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
            "standing runtime view `{view_id}` is missing runtime identity"
        )),
        MaterializedViewRegistryError::InvalidExecutionMode {
            view_id,
            reason: InvalidExecutionModeReason::StandingRuntimeMissingArtifact,
        } => ApiError::conflict(format!(
            "standing runtime view `{view_id}` is missing artifact or runtime binding"
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
    use axum::http::Method;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use tower::ServiceExt as _;
    use velorix_core::{
        delta::{DeltaKey, DeltaRecord, DeltaValue},
        standing_program::{
            DurableStateRoot, RelationFrontier, RuntimeCheckpointStatePayload, ViewFrontier,
        },
    };
    use velorix_meta::InMemoryMetaStore;

    #[test]
    fn standing_runtime_checkpoint_publication_refs_output_manifest_when_payload_contains_published_output(
    ) {
        let checkpoint = test_runtime_checkpoint(Vec::new());
        let checkpoint_key = test_checkpoint_key(&checkpoint);
        let publication = standing_runtime_output_manifest_record_for_checkpoint(
            &checkpoint,
            "purchases_by_user",
            &checkpoint_key,
        )
        .unwrap()
        .unwrap();

        let published = standing_runtime_checkpoint_with_publication_output_refs(
            &checkpoint,
            Some(&publication.manifest_key),
            &[],
        );

        assert_eq!(
            published.output_manifest_refs,
            vec![format!(
                "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
                publication.manifest_key.as_str()
            )]
        );
    }

    #[test]
    fn standing_runtime_output_manifest_record_hashes_published_output_payload() {
        let checkpoint = test_runtime_checkpoint(Vec::new());
        let checkpoint_key = test_checkpoint_key(&checkpoint);

        let publication = standing_runtime_output_manifest_record_for_checkpoint(
            &checkpoint,
            "purchases_by_user",
            &checkpoint_key,
        )
        .unwrap()
        .unwrap();
        let record = &publication.manifest_record;

        validate_standing_runtime_output_manifest_record(&publication.manifest_key, record)
            .unwrap();
        assert_eq!(
            publication.manifest_key.as_str(),
            ObjectKey::standing_runtime_output_manifest(
                "tenant-a",
                "program-purchases",
                "purchases_by_user",
                7,
                &record.output_content_hash,
            )
            .unwrap()
            .as_str()
        );
        assert_eq!(record.checkpoint_key, checkpoint_key.as_str());
        assert_eq!(record.output_encoding, "velorix-delta-batch-json-v1");
        assert_eq!(
            record.source_kind,
            "standing_runtime_checkpoint_published_output"
        );
        assert_eq!(record.output_row_count, 0);
        assert_eq!(record.pages.len(), 1);
        assert_eq!(record.pages[0].page_index, 0);
        assert_eq!(record.pages[0].row_count, 0);
        assert_eq!(
            record.pages[0].page_content_hash,
            record.output_content_hash
        );
        assert_eq!(
            record.pages[0].page_key,
            ObjectKey::standing_runtime_output_page(
                "tenant-a",
                "program-purchases",
                "purchases_by_user",
                7,
                0,
                &record.output_content_hash,
            )
            .unwrap()
            .as_str()
        );
        assert_eq!(publication.page_records.len(), 1);
        validate_standing_runtime_output_page_record(
            &publication.page_records[0].0,
            &publication.page_records[0].1,
        )
        .unwrap();
        assert_eq!(
            publication.page_records[0].1.published_output,
            record.published_output
        );
    }

    #[tokio::test]
    async fn standing_runtime_checkpoint_persistence_writes_output_delta_manifest_ref() {
        let state = test_api_state().await;
        let checkpoint = test_runtime_checkpoint(Vec::new());
        let output_delta = ViewOutputDelta {
            view_id: "purchases_by_user".to_string(),
            schema_fingerprint:
                "sha256:0000000000000000000000000000000000000000000000000000000000000001"
                    .to_string(),
            delta: DeltaBatch::from_records([
                DeltaRecord::new(
                    DeltaKey::from_json(json!("alice")),
                    DeltaValue::from_json(json!({ "count": 2, "sum": 17 })),
                    -1,
                ),
                DeltaRecord::new(
                    DeltaKey::from_json(json!("alice")),
                    DeltaValue::from_json(json!({ "count": 3, "sum": 20 })),
                    1,
                ),
            ]),
        };

        persist_standing_runtime_checkpoint(
            &state,
            "purchases_by_user",
            &checkpoint,
            std::slice::from_ref(&output_delta),
            Vec::new(),
            None,
        )
        .await
        .unwrap();

        let checkpoint_key = test_checkpoint_key(&checkpoint);
        let checkpoint_bytes = state
            .store
            .get(&Path::from(checkpoint_key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let record: StandingRuntimeCheckpointRecord =
            serde_json::from_slice(&checkpoint_bytes).unwrap();
        let delta_ref = record
            .checkpoint
            .output_manifest_refs
            .iter()
            .find(|output_ref| output_ref.starts_with(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX))
            .unwrap();
        let delta_key = delta_ref
            .strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX)
            .unwrap();
        let delta_bytes = state
            .store
            .get(&Path::from(delta_key))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let delta_record: StandingRuntimeOutputDeltaRecord =
            serde_json::from_slice(&delta_bytes).unwrap();
        let (parsed_delta_key, _) =
            ObjectKey::parse_standing_runtime_output_delta(delta_key).unwrap();

        validate_standing_runtime_output_delta_record(&parsed_delta_key, &delta_record).unwrap();
        assert_eq!(
            delta_record.output_delta,
            serde_json::to_value(output_delta.delta).unwrap()
        );
        assert_eq!(delta_record.delta_row_count, 2);
        assert_eq!(
            delta_record.source_kind,
            "standing_runtime_epoch_output_delta"
        );

        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn standing_runtime_output_serving_reads_page_object_not_manifest_payload() {
        let state = test_api_state().await;
        let checkpoint = test_runtime_checkpoint(Vec::new());
        let checkpoint_key = test_checkpoint_key(&checkpoint);
        let mut publication = standing_runtime_output_manifest_record_for_checkpoint(
            &checkpoint,
            "purchases_by_user",
            &checkpoint_key,
        )
        .unwrap()
        .unwrap();
        publication.manifest_record.published_output = serde_json::json!({"records": [{"key": "poison", "value": {"sum": 999, "count": 999}, "weight": 1}]});
        let (page_key, page_record) = &publication.page_records[0];
        persist_standing_runtime_output_page(&state, page_key, page_record)
            .await
            .unwrap();

        let output = standing_runtime_published_output_from_manifest_page(
            &state,
            &publication.manifest_record,
        )
        .await
        .unwrap();

        let expected: DeltaBatch =
            serde_json::from_value(page_record.published_output.clone()).unwrap();
        assert_eq!(output, expected);
    }

    #[tokio::test]
    async fn standing_runtime_output_serving_fails_closed_when_page_object_is_missing() {
        let state = test_api_state().await;
        let checkpoint = test_runtime_checkpoint(Vec::new());
        let checkpoint_key = test_checkpoint_key(&checkpoint);
        let publication = standing_runtime_output_manifest_record_for_checkpoint(
            &checkpoint,
            "purchases_by_user",
            &checkpoint_key,
        )
        .unwrap()
        .unwrap();

        standing_runtime_published_output_from_manifest_page(&state, &publication.manifest_record)
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn standing_runtime_output_serving_fails_closed_when_page_object_is_corrupt() {
        let state = test_api_state().await;
        let checkpoint = test_runtime_checkpoint(Vec::new());
        let checkpoint_key = test_checkpoint_key(&checkpoint);
        let publication = standing_runtime_output_manifest_record_for_checkpoint(
            &checkpoint,
            "purchases_by_user",
            &checkpoint_key,
        )
        .unwrap()
        .unwrap();
        let (page_key, page_record) = &publication.page_records[0];
        let mut corrupt = page_record.clone();
        corrupt.published_output = serde_json::json!({"records": [{"key": "other", "value": {"sum": 1, "count": 1}, "weight": 1}]});
        let corrupt_bytes = serde_json::to_vec(&corrupt).unwrap();
        state
            .store
            .put(
                &Path::from(page_key.as_str()),
                bytes::Bytes::from(corrupt_bytes).into(),
            )
            .await
            .unwrap();

        let error = standing_runtime_published_output_from_manifest_page(
            &state,
            &publication.manifest_record,
        )
        .await
        .unwrap_err();

        assert!(format!("{error:?}").contains("standing runtime output page key/body mismatch"));
    }

    #[tokio::test]
    async fn standing_runtime_output_serving_fails_closed_when_manifest_object_is_missing() {
        let state = test_api_state().await;
        let checkpoint = test_runtime_checkpoint(Vec::new());
        let checkpoint_key = test_checkpoint_key(&checkpoint);
        let publication = standing_runtime_output_manifest_record_for_checkpoint(
            &checkpoint,
            "purchases_by_user",
            &checkpoint_key,
        )
        .unwrap()
        .unwrap();
        let output_ref = format!(
            "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
            publication.manifest_key.as_str()
        );

        let error = read_standing_runtime_output_manifest_record(
            &state,
            output_ref.as_str(),
            "purchases_by_user",
        )
        .await
        .unwrap_err();

        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn standing_runtime_output_serving_fails_closed_when_manifest_object_is_corrupt() {
        let state = test_api_state().await;
        let checkpoint = test_runtime_checkpoint(Vec::new());
        let checkpoint_key = test_checkpoint_key(&checkpoint);
        let publication = standing_runtime_output_manifest_record_for_checkpoint(
            &checkpoint,
            "purchases_by_user",
            &checkpoint_key,
        )
        .unwrap()
        .unwrap();
        state
            .store
            .put(
                &Path::from(publication.manifest_key.as_str()),
                bytes::Bytes::from_static(b"{not-json").into(),
            )
            .await
            .unwrap();
        let output_ref = format!(
            "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
            publication.manifest_key.as_str()
        );

        let error = read_standing_runtime_output_manifest_record(
            &state,
            output_ref.as_str(),
            "purchases_by_user",
        )
        .await
        .unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn standing_runtime_checkpoint_persistence_writes_state_object_and_strips_embedded_payload(
    ) {
        let state = test_api_state().await;
        let checkpoint = test_runtime_checkpoint(Vec::new());
        let expected_payload = checkpoint.state_payload.clone();
        let checkpoint_key = test_checkpoint_key(&checkpoint);

        persist_standing_runtime_checkpoint(
            &state,
            "purchases_by_user",
            &checkpoint,
            &[],
            Vec::new(),
            None,
        )
        .await
        .unwrap();

        let checkpoint_bytes = state
            .store
            .get(&Path::from(checkpoint_key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let raw_record: StandingRuntimeCheckpointRecord =
            serde_json::from_slice(&checkpoint_bytes).unwrap();
        assert!(raw_record.checkpoint.state_payload.is_none());
        let (state_key, state_key_parts) = ObjectKey::parse_standing_runtime_state_payload(
            raw_record.checkpoint.state_root.object_key.clone(),
        )
        .unwrap();
        assert_eq!(state_key_parts.tenant_id, "tenant-a");
        assert_eq!(state_key_parts.program_id, "program-purchases");
        assert_eq!(state_key_parts.view_id, "purchases_by_user");
        assert_eq!(state_key_parts.logical_epoch, checkpoint.logical_epoch);
        assert_eq!(
            state_key_parts.state_content_hash,
            checkpoint.state_root.content_hash
        );

        let state_bytes = state
            .store
            .get(&Path::from(state_key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let state_record: StandingRuntimeStatePayloadRecord =
            serde_json::from_slice(&state_bytes).unwrap();
        validate_standing_runtime_state_payload_record(&state_key, &state_record).unwrap();
        assert_eq!(Some(state_record.payload), expected_payload);
    }

    #[tokio::test]
    async fn standing_runtime_checkpoint_read_hydrates_state_payload_from_state_object() {
        let state = test_api_state().await;
        let checkpoint = test_runtime_checkpoint(Vec::new());
        let expected_payload = checkpoint.state_payload.clone();

        persist_standing_runtime_checkpoint(
            &state,
            "purchases_by_user",
            &checkpoint,
            &[],
            Vec::new(),
            None,
        )
        .await
        .unwrap();

        let record = read_latest_standing_runtime_checkpoint(
            &state,
            &checkpoint.identity,
            "purchases_by_user",
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(record.checkpoint.state_payload, expected_payload);
    }

    #[tokio::test]
    async fn standing_runtime_checkpoint_read_fails_closed_when_state_object_is_missing() {
        let state = test_api_state().await;
        let checkpoint = test_runtime_checkpoint(Vec::new());

        persist_standing_runtime_checkpoint(
            &state,
            "purchases_by_user",
            &checkpoint,
            &[],
            Vec::new(),
            None,
        )
        .await
        .unwrap();
        let checkpoint_key = test_checkpoint_key(&checkpoint);
        let checkpoint_bytes = state
            .store
            .get(&Path::from(checkpoint_key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let raw_record: StandingRuntimeCheckpointRecord =
            serde_json::from_slice(&checkpoint_bytes).unwrap();
        state
            .store
            .delete(&Path::from(raw_record.checkpoint.state_root.object_key))
            .await
            .unwrap();

        let error = read_latest_standing_runtime_checkpoint(
            &state,
            &checkpoint.identity,
            "purchases_by_user",
        )
        .await
        .unwrap_err();

        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn standing_runtime_checkpoint_read_fails_closed_when_checkpoint_object_is_corrupt() {
        let state = test_api_state().await;
        let checkpoint = test_runtime_checkpoint(Vec::new());
        let checkpoint_key = test_checkpoint_key(&checkpoint);
        state
            .store
            .put(
                &Path::from(checkpoint_key.as_str()),
                bytes::Bytes::from_static(b"{not-json").into(),
            )
            .await
            .unwrap();

        let error = read_latest_standing_runtime_checkpoint(
            &state,
            &checkpoint.identity,
            "purchases_by_user",
        )
        .await
        .unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn standing_runtime_checkpoint_read_fails_closed_when_meta_pointer_checkpoint_object_is_missing(
    ) {
        let meta_store = Arc::new(InMemoryMetaStore::default());
        let state = test_api_state().await.with_meta_store(meta_store.clone());
        let checkpoint = test_runtime_checkpoint(Vec::new());
        let checkpoint_key = test_checkpoint_key(&checkpoint);
        let owner = match meta_store
            .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
                tenant_id: checkpoint.identity.tenant_id.clone(),
                program_id: checkpoint.identity.program_id.clone(),
                view_id: "purchases_by_user".to_string(),
                owner_id: "api-test-missing-checkpoint-owner".to_string(),
                ttl_ms: 30_000,
            })
            .await
            .unwrap()
        {
            AcquireStandingRuntimeOwnerOutcome::Acquired(claim)
            | AcquireStandingRuntimeOwnerOutcome::Renewed(claim) => {
                standing_runtime_owner_token_from_claim(&claim)
            }
            AcquireStandingRuntimeOwnerOutcome::Conflict(claim) => {
                panic!("unexpected owner conflict: {claim:?}")
            }
        };

        persist_standing_runtime_checkpoint(
            &state,
            "purchases_by_user",
            &checkpoint,
            &[],
            Vec::new(),
            Some(owner),
        )
        .await
        .unwrap();
        state
            .store
            .delete(&Path::from(checkpoint_key.as_str()))
            .await
            .unwrap();

        let error = read_latest_standing_runtime_checkpoint(
            &state,
            &checkpoint.identity,
            "purchases_by_user",
        )
        .await
        .unwrap_err();

        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn standing_runtime_checkpoint_read_fails_closed_when_state_object_is_corrupt() {
        let state = test_api_state().await;
        let checkpoint = test_runtime_checkpoint(Vec::new());

        persist_standing_runtime_checkpoint(
            &state,
            "purchases_by_user",
            &checkpoint,
            &[],
            Vec::new(),
            None,
        )
        .await
        .unwrap();
        let checkpoint_key = test_checkpoint_key(&checkpoint);
        let checkpoint_bytes = state
            .store
            .get(&Path::from(checkpoint_key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let raw_record: StandingRuntimeCheckpointRecord =
            serde_json::from_slice(&checkpoint_bytes).unwrap();
        let state_key = ObjectKey::parse_standing_runtime_state_payload(
            raw_record.checkpoint.state_root.object_key.clone(),
        )
        .unwrap()
        .0;
        let mut corrupt_record =
            standing_runtime_state_payload_record_for_checkpoint(&checkpoint, "purchases_by_user")
                .unwrap()
                .1;
        corrupt_record.payload.payload = serde_json::json!({
            "schema_version": 1,
            "published_output": {
                "records": [{"key": "poison", "value": {"sum": 1, "count": 1}, "weight": 1}]
            }
        })
        .to_string();
        state
            .store
            .put(
                &Path::from(state_key.as_str()),
                bytes::Bytes::from(serde_json::to_vec(&corrupt_record).unwrap()).into(),
            )
            .await
            .unwrap();

        let error = read_latest_standing_runtime_checkpoint(
            &state,
            &checkpoint.identity,
            "purchases_by_user",
        )
        .await
        .unwrap_err();

        assert!(format!("{error:?}").contains("standing runtime state payload key/body mismatch"));
    }

    #[test]
    fn standing_runtime_checkpoint_output_ref_validation_rejects_untagged_ref() {
        let mut checkpoint = test_runtime_checkpoint(Vec::new());
        let checkpoint_key = test_checkpoint_key(&checkpoint);
        checkpoint
            .output_manifest_refs
            .push(checkpoint_key.as_str().to_string());
        let pointer = test_checkpoint_pointer(&checkpoint_key, &checkpoint);
        let record = test_checkpoint_record(&checkpoint_key, checkpoint);

        let error =
            validate_standing_runtime_checkpoint_output_refs(&record, &pointer).unwrap_err();

        assert!(format!("{error:?}").contains("unsupported standing runtime checkpoint output ref"));
    }

    #[test]
    fn standing_runtime_checkpoint_output_ref_validation_rejects_mismatched_output_manifest_key() {
        let mut checkpoint = test_runtime_checkpoint(Vec::new());
        let checkpoint_key = test_checkpoint_key(&checkpoint);
        let output_content_hash = format!("sha256:{}", "f".repeat(64));
        let mismatched_key = ObjectKey::standing_runtime_output_manifest(
            &checkpoint.identity.tenant_id,
            &checkpoint.identity.program_id,
            "purchases_by_user",
            checkpoint.logical_epoch + 1,
            &output_content_hash,
        )
        .unwrap();
        checkpoint.output_manifest_refs.push(format!(
            "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
            mismatched_key.as_str()
        ));
        let pointer = test_checkpoint_pointer(&checkpoint_key, &checkpoint);
        let record = test_checkpoint_record(&checkpoint_key, checkpoint);

        let error =
            validate_standing_runtime_checkpoint_output_refs(&record, &pointer).unwrap_err();

        assert!(format!("{error:?}")
            .contains("standing runtime checkpoint output manifest ref mismatch"));
    }

    #[test]
    fn single_key_output_schema_fingerprint_changes_with_aggregate_projection() {
        let catalog = test_purchases_catalog();
        let sum_count_plan = validate_catalog_backed_sum_count_view_sql(
            "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id",
            &catalog,
        )
        .unwrap();
        let avg_plan = validate_catalog_backed_sum_count_view_sql(
            "select user_id, sum(amount) as total, count(*) as events, avg(amount) as average from purchases group by user_id",
            &catalog,
        )
        .unwrap();

        let sum_count_schema =
            single_key_sum_count_output_schema("purchase_metrics", &catalog, &sum_count_plan)
                .unwrap();
        let avg_schema =
            single_key_sum_count_output_schema("purchase_metrics", &catalog, &avg_plan).unwrap();

        assert_ne!(
            sum_count_schema.schema_fingerprint,
            avg_schema.schema_fingerprint
        );
        assert_ne!(
            avg_schema.schema_fingerprint,
            catalog.schema_fingerprint.to_string()
        );
        assert_eq!(
            avg_schema
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["user_id", "total", "events", "average"]
        );
    }

    #[test]
    fn materialized_runtime_binding_persists_admitted_logical_plan() {
        let catalog = test_purchases_catalog();
        let sql =
            "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
        let plan = validate_catalog_backed_sum_count_view_sql(sql, &catalog).unwrap();
        let input_schema = catalog_input_relation_schema(&catalog).unwrap();
        let output_schema =
            single_key_sum_count_output_schema("purchases_by_user", &catalog, &plan).unwrap();
        let spec = StandingViewSpec {
            view_id: "purchases_by_user".to_string(),
            sql: sql.to_string(),
            dialect: SqlDialect::VelorixSql,
            source_kind: SqlSourceKind::StandingView,
            input_relations: vec![input_schema],
            output_relations: vec![output_schema],
            shape: StandingViewShape {
                is_materialized: true,
                multi_input: false,
                multi_output: false,
            },
        };

        let binding =
            materialized_view_runtime_binding_for_spec(std::slice::from_ref(&catalog), &spec)
                .unwrap();
        let logical_plan = binding.logical_plan.unwrap();

        assert_eq!(logical_plan.view_sql, spec.sql);
        assert_eq!(
            logical_plan.output_relation.relation_id,
            "purchases_by_user"
        );
        assert_eq!(
            logical_plan.input_relations[0].schema_fingerprint,
            catalog.schema_fingerprint.to_string()
        );
    }

    #[test]
    fn latest_by_key_output_schema_uses_arg_max_value_type() {
        let catalog = test_device_status_catalog();
        let plan = validate_supported_latest_by_key_sql(
            "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id",
            &catalog,
        )
        .unwrap();

        let schema = latest_by_key_output_schema("latest_device_status", &catalog, &plan).unwrap();

        assert_eq!(schema.relation_id, "latest_device_status");
        assert_eq!(
            schema
                .columns
                .iter()
                .map(|column| (column.name.as_str(), &column.data_type))
                .collect::<Vec<_>>(),
            vec![
                ("device_id", &SqlDataType::Utf8),
                ("enabled", &SqlDataType::Bool)
            ]
        );
        assert_eq!(schema.primary_key, vec!["device_id"]);
    }

    #[test]
    fn materialized_runtime_output_schema_supports_tumbling_event_time_window() {
        let factory = MaterializedViewRuntimeFactory;
        let catalog = test_purchases_event_time_catalog();

        let schemas = factory
            .output_schemas_for_view_request(
                "purchases_by_user_minute",
                "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count, min(amount) as minimum_amount, max(amount) as maximum_amount, avg(amount) as average_amount from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end",
                &catalog,
                catalog.schema_fingerprint.as_str(),
            )
            .unwrap()
            .unwrap();

        let schema = &schemas[0];
        assert_eq!(schema.relation_id, "purchases_by_user_minute");
        assert_eq!(
            schema
                .columns
                .iter()
                .map(|column| (column.name.as_str(), &column.data_type))
                .collect::<Vec<_>>(),
            vec![
                ("user_id", &SqlDataType::Utf8),
                ("window_start", &SqlDataType::Int64),
                ("window_end", &SqlDataType::Int64),
                ("total_amount", &SqlDataType::Int64),
                ("event_count", &SqlDataType::Int64),
                ("minimum_amount", &SqlDataType::Int64),
                ("maximum_amount", &SqlDataType::Int64),
                ("average_amount", &SqlDataType::Float64),
            ]
        );
        assert_eq!(
            schema.primary_key,
            vec!["user_id", "window_start", "window_end"]
        );
    }

    #[tokio::test]
    async fn rest_latest_bool_view_materialized_output_replays_later_ingest_after_restart() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = test_api_state_with_store(store.clone(), "api-test-owner-a", false).await;
        let router = app(state);

        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": test_device_status_catalog(),
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
        assert_eq!(relation_response.1["relation_id"], "device_status");

        let view_request = CreateViewRequest {
            view_id: "latest_device_status".to_string(),
            url_path: Some("/devices/latest-status".to_string()),
            output_relation_id: None,
            input_relation_id: "device_status".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("latest bool status by device".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
        let view_response =
            call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
        assert_eq!(
            view_response.0,
            StatusCode::CREATED,
            "view creation response: {}",
            view_response.1
        );
        assert_eq!(view_response.1["view_id"], "latest_device_status");
        assert_eq!(view_response.1["query_enabled"], true);

        let ingest_response = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({
                "relation_id": "device_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "device-status-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "event_time_watermark": {
                    "event_time_column_id": "event_time",
                    "max_observed_event_time_ns": 110,
                    "watermark_ns": 100
                },
                "rows": [
                    {"device_id": "device-a", "enabled": true, "event_time": 100, "delta": 1},
                    {"device_id": "device-a", "enabled": false, "event_time": 110, "delta": 1},
                    {"device_id": "device-b", "enabled": true, "event_time": 90, "delta": 1}
                ]
            }),
        )
        .await;
        assert_eq!(ingest_response.0, StatusCode::CREATED);
        assert_eq!(ingest_response.1["outcome"], "appended");

        let query_response = call_json(
            &router,
            Method::POST,
            "/v1/views/latest_device_status/query",
            json!({}),
        )
        .await;
        assert_eq!(
            query_response.0,
            StatusCode::OK,
            "join query response: {}",
            query_response.1
        );
        assert_latest_device_rows(&query_response.1, 3, true);

        append_committed_ingest_without_runtime_apply(
            store.clone(),
            IngestRowsRequest {
                relation_id: "device_status".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "device-status-stream".to_string(),
                partition_id: 0,
                start_offset_inclusive: 3,
                event_time_watermark: Some(IngestEventTimeWatermarkRequest {
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120,
                    watermark_ns: 115,
                }),
                rows: vec![json!({
                    "device_id": "device-b",
                    "enabled": false,
                    "event_time": 120,
                    "delta": 1
                })],
            },
        )
        .await;

        let restarted_state =
            test_api_state_with_store(store.clone(), "api-test-owner-b", true).await;
        let restored = restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap();
        assert_eq!(restored, 1);
        let restarted_app = app(restarted_state);
        let restarted_query_response = call_json(
            &restarted_app,
            Method::POST,
            "/v1/views/latest_device_status/query",
            json!({}),
        )
        .await;
        assert_eq!(restarted_query_response.0, StatusCode::OK);
        assert_latest_device_rows(&restarted_query_response.1, 4, false);
    }

    #[tokio::test]
    async fn rest_ingest_event_time_watermark_requires_declared_event_time_column() {
        let state = test_api_state().await;
        let router = app(state);

        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": test_scores_catalog(),
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);

        let ingest_response = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "event_time_watermark": {
                    "event_time_column_id": "event_time",
                    "max_observed_event_time_ns": 100,
                    "watermark_ns": 90
                },
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1}
                ]
            }),
        )
        .await;

        assert_eq!(ingest_response.0, StatusCode::BAD_REQUEST);
        assert!(
            ingest_response.1["error"]
                .as_str()
                .unwrap()
                .contains("event_time_watermark requires relation_schema.event_time_column_id"),
            "unexpected response: {}",
            ingest_response.1
        );
    }

    #[tokio::test]
    async fn rest_ingest_event_time_watermark_rejects_max_below_batch_event_time() {
        let state = test_api_state().await;
        let router = app(state);

        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": test_device_status_catalog(),
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);

        let ingest_response = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({
                "relation_id": "device_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "device-status-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "event_time_watermark": {
                    "event_time_column_id": "event_time",
                    "max_observed_event_time_ns": 109,
                    "watermark_ns": 100
                },
                "rows": [
                    {"device_id": "device-a", "enabled": true, "event_time": 100, "delta": 1},
                    {"device_id": "device-b", "enabled": false, "event_time": 110, "delta": 1}
                ]
            }),
        )
        .await;

        assert_eq!(ingest_response.0, StatusCode::BAD_REQUEST);
        assert!(
            ingest_response.1["error"]
                .as_str()
                .unwrap()
                .contains("max_observed_event_time_ns must be >= actual max event-time value 110"),
            "unexpected response: {}",
            ingest_response.1
        );
    }

    #[tokio::test]
    async fn rest_tumbling_window_view_materialized_output_replays_later_ingest_after_restart() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state =
            test_api_state_with_store(store.clone(), "api-test-window-owner-a", false).await;
        let router = app(state);

        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": test_purchases_event_time_catalog(),
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(
            relation_response.0,
            StatusCode::CREATED,
            "relation creation response: {}",
            relation_response.1
        );
        assert_eq!(relation_response.1["relation_id"], "purchases");

        let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
        let view_request = CreateViewRequest {
            view_id: "purchases_by_user_minute".to_string(),
            url_path: Some("/purchases/by-user-minute".to_string()),
            output_relation_id: None,
            input_relation_id: "purchases".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: sql.to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("purchase totals by user and event-time minute".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
        let view_response =
            call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
        assert_eq!(
            view_response.0,
            StatusCode::CREATED,
            "window view creation response: {}",
            view_response.1
        );
        assert_eq!(view_response.1["view_id"], "purchases_by_user_minute");
        assert_eq!(view_response.1["query_enabled"], true);

        let ingest_response = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({
                "relation_id": "purchases",
                "relation_version": "2026-05-24.v1",
                "stream_id": "purchases-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "event_time_watermark": {
                    "event_time_column_id": "event_time",
                    "max_observed_event_time_ns": 70_000_000_000i64,
                    "watermark_ns": 60_000_000_000i64
                },
                "rows": [
                    {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                    {"user_id": "bob", "amount": 5, "event_time": 30_000_000_000i64, "delta": 1},
                    {"user_id": "alice", "amount": 7, "event_time": 70_000_000_000i64, "delta": 1}
                ]
            }),
        )
        .await;
        assert_eq!(ingest_response.0, StatusCode::CREATED);
        assert_eq!(ingest_response.1["outcome"], "appended");

        let query_response = call_json(
            &router,
            Method::POST,
            "/v1/views/purchases_by_user_minute/query",
            json!({}),
        )
        .await;
        assert_eq!(
            query_response.0,
            StatusCode::OK,
            "window query response: {}",
            query_response.1
        );
        assert_window_rows(
            &query_response.1,
            3,
            json!([
                {"user_id": "alice", "window_start": 0, "window_end": 60_000_000_000i64, "total_amount": 10, "event_count": 1},
                {"user_id": "bob", "window_start": 0, "window_end": 60_000_000_000i64, "total_amount": 5, "event_count": 1}
            ]),
        );

        append_committed_ingest_without_runtime_apply(
            store.clone(),
            IngestRowsRequest {
                relation_id: "purchases".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "purchases-stream".to_string(),
                partition_id: 0,
                start_offset_inclusive: 3,
                event_time_watermark: Some(IngestEventTimeWatermarkRequest {
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                rows: vec![json!({
                    "user_id": "bob",
                    "amount": 11,
                    "event_time": 80_000_000_000i64,
                    "delta": 1
                })],
            },
        )
        .await;

        let restarted_state =
            test_api_state_with_store(store.clone(), "api-test-window-owner-b", true).await;
        let restored = restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap();
        assert_eq!(restored, 1);
        let restarted_app = app(restarted_state);
        let restarted_query_response = call_json(
            &restarted_app,
            Method::POST,
            "/v1/views/purchases_by_user_minute/query",
            json!({}),
        )
        .await;
        assert_eq!(
            restarted_query_response.0,
            StatusCode::OK,
            "restarted window query response: {}",
            restarted_query_response.1
        );
        assert_window_rows(
            &restarted_query_response.1,
            4,
            json!([
                {"user_id": "alice", "window_start": 0, "window_end": 60_000_000_000i64, "total_amount": 10, "event_count": 1},
                {"user_id": "alice", "window_start": 60_000_000_000i64, "window_end": 120_000_000_000i64, "total_amount": 7, "event_count": 1},
                {"user_id": "bob", "window_start": 0, "window_end": 60_000_000_000i64, "total_amount": 5, "event_count": 1},
                {"user_id": "bob", "window_start": 60_000_000_000i64, "window_end": 120_000_000_000i64, "total_amount": 11, "event_count": 1}
            ]),
        );
    }

    async fn call_json(
        app: &Router,
        method: Method,
        uri: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, value)
    }

    async fn append_committed_ingest_without_runtime_apply(
        store: Arc<dyn ObjectStore>,
        request: IngestRowsRequest,
    ) {
        let state = test_api_state_with_store(store, "api-test-crash-window-writer", false).await;
        let prepared = prepare_ingest_batch(&state, request).await.unwrap();
        let outcome = append_ingest_envelope(&state, prepared.envelope)
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            AppendValidatedEnvelopeOutcome::Appended { .. }
        ));
    }

    #[tokio::test]
    async fn rest_late_view_backfills_on_first_query_without_blocking_later_ingest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = test_api_state_with_store(store, "api-test-late-view-owner", false).await;
        let router = app(state);

        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": test_scores_catalog(),
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);

        let first_ingest = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "alice", "score": 3, "delta": 1},
                    {"user_id": "bob", "score": 4, "delta": 1}
                ]
            }),
        )
        .await;
        assert_eq!(first_ingest.0, StatusCode::CREATED);

        let view_request = CreateViewRequest {
            view_id: "late_scores_by_user".to_string(),
            url_path: Some("/scores/late-by-user".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("late-created score totals".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
        let view_response =
            call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
        assert_eq!(
            view_response.0,
            StatusCode::CREATED,
            "late view creation response: {}",
            view_response.1
        );
        assert_eq!(view_response.1["query_enabled"], false);
        assert_eq!(
            view_response.1["lifecycle"]["deployment_status"],
            "deploying"
        );
        assert!(
            view_response.1["lifecycle"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("backfill_required"),
            "late view lifecycle: {}",
            view_response.1
        );

        let second_ingest = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 3,
                "rows": [
                    {"user_id": "alice", "score": 2, "delta": 1}
                ]
            }),
        )
        .await;
        assert_eq!(
            second_ingest.0,
            StatusCode::CREATED,
            "late view must not block later ingest: {}",
            second_ingest.1
        );

        let query_response = call_json(
            &router,
            Method::POST,
            "/v1/views/late_scores_by_user/query",
            json!({}),
        )
        .await;
        assert_eq!(
            query_response.0,
            StatusCode::OK,
            "query-triggered backfill response: {}",
            query_response.1
        );
        assert_eq!(query_response.1["logical_epoch"], 4);
        assert_eq!(
            query_response.1["rows"],
            json!([
                {"user_id": "alice", "sum": 15, "count": 3},
                {"user_id": "bob", "sum": 4, "count": 1}
            ])
        );

        let refreshed_view = call_json(
            &router,
            Method::GET,
            "/v1/views/late_scores_by_user",
            json!({}),
        )
        .await;
        assert_eq!(refreshed_view.0, StatusCode::OK);
        assert_eq!(refreshed_view.1["query_enabled"], true);
        assert_eq!(
            refreshed_view.1["lifecycle"]["deployment_status"],
            "running"
        );
    }

    #[tokio::test]
    async fn rest_late_view_backfill_api_reports_coverage_and_runs_limited_steps() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = test_api_state_with_store(store, "api-test-backfill-api-owner", false).await;
        let router = app(state);

        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": test_scores_catalog(),
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);

        for (start, rows) in [
            (
                0,
                json!([
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "bob", "score": 4, "delta": 1}
                ]),
            ),
            (
                2,
                json!([
                    {"user_id": "alice", "score": 5, "delta": 1}
                ]),
            ),
        ] {
            let ingest = call_json(
                &router,
                Method::POST,
                "/v1/ingest",
                json!({
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": start,
                    "rows": rows
                }),
            )
            .await;
            assert_eq!(
                ingest.0,
                StatusCode::CREATED,
                "ingest response: {}",
                ingest.1
            );
        }

        let view_request = CreateViewRequest {
            view_id: "late_scores_backfill_api".to_string(),
            url_path: Some("/scores/backfill-api".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("late-created score totals with explicit backfill".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
        let view_response =
            call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
        assert_eq!(view_response.0, StatusCode::CREATED);
        assert_eq!(view_response.1["coverage"]["state"], "backfill_required");
        assert_eq!(
            view_response.1["coverage"]["request_scope"]["status"],
            "unsupported"
        );
        assert_eq!(
            view_response.1["coverage"]["background_backfill"]["status"],
            "available"
        );

        let status = call_json(
            &router,
            Method::GET,
            "/v1/views/late_scores_backfill_api/backfill",
            json!({}),
        )
        .await;
        assert_eq!(status.0, StatusCode::OK);
        assert_eq!(status.1["coverage"]["state"], "backfill_required");
        assert_eq!(status.1["progress"]["processed_batches"], 0);
        assert_eq!(status.1["progress"]["remaining_batches"], 2);
        assert_eq!(status.1["progress"]["total_batches"], 2);
        assert_eq!(status.1["progress"]["percent"], 0.0);

        let first_step = call_json(
            &router,
            Method::POST,
            "/v1/views/late_scores_backfill_api/backfill",
            json!({"mode": "sync", "batch_limit": 1}),
        )
        .await;
        assert_eq!(
            first_step.0,
            StatusCode::OK,
            "first backfill response: {}",
            first_step.1
        );
        assert_eq!(first_step.1["applied_batches"], 1);
        assert_eq!(first_step.1["remaining_batches"], 1);
        assert_eq!(first_step.1["query_enabled"], false);
        assert_eq!(first_step.1["progress"]["processed_batches"], 1);
        assert_eq!(first_step.1["progress"]["remaining_batches"], 1);
        assert_eq!(first_step.1["progress"]["total_batches"], 2);
        assert_eq!(first_step.1["progress"]["percent"], 50.0);

        let finish = call_json(
            &router,
            Method::POST,
            "/v1/views/late_scores_backfill_api/backfill",
            json!({"mode": "sync"}),
        )
        .await;
        assert_eq!(finish.0, StatusCode::OK, "finish response: {}", finish.1);
        assert_eq!(finish.1["remaining_batches"], 0);
        assert_eq!(finish.1["query_enabled"], true);
        assert_eq!(finish.1["progress"]["processed_batches"], 2);
        assert_eq!(finish.1["progress"]["remaining_batches"], 0);
        assert_eq!(finish.1["progress"]["total_batches"], 2);
        assert_eq!(finish.1["progress"]["percent"], 100.0);

        let query_response = call_json(
            &router,
            Method::POST,
            "/v1/views/late_scores_backfill_api/query",
            json!({}),
        )
        .await;
        assert_eq!(query_response.0, StatusCode::OK);
        assert_eq!(
            query_response.1["rows"],
            json!([
                {"user_id": "alice", "sum": 15, "count": 2},
                {"user_id": "bob", "sum": 4, "count": 1}
            ])
        );
    }

    #[tokio::test]
    async fn rest_late_view_background_backfill_scheduler_eventually_marks_running() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state =
            test_api_state_with_store(store, "api-test-background-backfill-owner", false).await;
        let router = app(state);

        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": test_scores_catalog(),
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);

        for (start, rows) in [
            (
                0,
                json!([
                    {"user_id": "alice", "score": 10, "delta": 1}
                ]),
            ),
            (
                1,
                json!([
                    {"user_id": "bob", "score": 4, "delta": 1}
                ]),
            ),
        ] {
            let ingest = call_json(
                &router,
                Method::POST,
                "/v1/ingest",
                json!({
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": start,
                    "rows": rows
                }),
            )
            .await;
            assert_eq!(ingest.0, StatusCode::CREATED);
        }

        let view_request = CreateViewRequest {
            view_id: "late_scores_background".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: None,
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
        let view_response =
            call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
        assert_eq!(view_response.0, StatusCode::CREATED);
        assert_eq!(view_response.1["query_enabled"], false);

        let scheduled = call_json(
            &router,
            Method::POST,
            "/v1/views/late_scores_background/backfill",
            json!({"mode": "background", "batch_limit": 1, "pause_ms": 1}),
        )
        .await;
        assert_eq!(
            scheduled.0,
            StatusCode::ACCEPTED,
            "scheduled response: {}",
            scheduled.1
        );
        assert_eq!(scheduled.1["outcome"], "scheduled");

        let mut latest = Value::Null;
        for _ in 0..50 {
            latest = call_json(
                &router,
                Method::GET,
                "/v1/views/late_scores_background",
                json!({}),
            )
            .await
            .1;
            if latest["query_enabled"] == true {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            latest["query_enabled"], true,
            "view did not become queryable: {latest}"
        );
        assert_eq!(latest["coverage"]["state"], "materialized");
    }

    #[tokio::test]
    async fn rest_view_query_fails_closed_without_published_output_manifest() {
        let state = test_api_state_with_store(
            Arc::new(InMemory::new()),
            "api-test-no-manifest-owner",
            false,
        )
        .await;
        let router = app(state);

        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": test_scores_catalog(),
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(
            relation_response.0,
            StatusCode::CREATED,
            "relation creation response: {}",
            relation_response.1
        );

        let view_request = CreateViewRequest {
            view_id: "scores_empty_until_ingest".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: None,
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
        let view_response =
            call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
        assert_eq!(
            view_response.0,
            StatusCode::CREATED,
            "view creation response: {}",
            view_response.1
        );
        assert_eq!(view_response.1["query_enabled"], true);

        let query_response = call_json(
            &router,
            Method::POST,
            "/v1/views/scores_empty_until_ingest/query",
            json!({}),
        )
        .await;
        assert_eq!(query_response.0, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            query_response.1["error"]
                .as_str()
                .unwrap_or_default()
                .contains("standing runtime output manifest is unavailable"),
            "query response: {}",
            query_response.1
        );
    }

    #[tokio::test]
    async fn rest_two_relation_join_view_materialized_output_survives_api_restart() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = test_api_state_with_store(store.clone(), "api-test-join-owner-a", false).await;
        let router = app(state);

        for catalog in [test_scores_catalog(), test_accounts_catalog()] {
            let relation_response = call_json(
                &router,
                Method::POST,
                "/v1/relations",
                json!({
                    "catalog": catalog,
                    "default_orders_sum_count": false
                }),
            )
            .await;
            assert_eq!(
                relation_response.0,
                StatusCode::CREATED,
                "relation creation response: {}",
                relation_response.1
            );
        }

        let view_request = CreateViewRequest {
            view_id: "scores_by_account".to_string(),
            url_path: Some("/scores/by-account".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("score metrics grouped by account join".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
        let view_response =
            call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
        assert_eq!(
            view_response.0,
            StatusCode::CREATED,
            "join view creation response: {}",
            view_response.1
        );
        assert_eq!(view_response.1["view_id"], "scores_by_account");
        assert_eq!(view_response.1["query_enabled"], true);

        let scores_ingest = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "alice", "score": 7, "delta": 1},
                    {"user_id": "bob", "score": 5, "delta": 1}
                ]
            }),
        )
        .await;
        assert_eq!(scores_ingest.0, StatusCode::CREATED);
        assert_eq!(scores_ingest.1["outcome"], "appended");

        let accounts_ingest = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({
                "relation_id": "accounts",
                "relation_version": "2026-05-24.v1",
                "stream_id": "accounts-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"account_id": "alice", "limit": 100, "delta": 1},
                    {"account_id": "bob", "limit": 50, "delta": 1}
                ]
            }),
        )
        .await;
        assert_eq!(accounts_ingest.0, StatusCode::CREATED);
        assert_eq!(accounts_ingest.1["outcome"], "appended");

        let query_response = call_json(
            &router,
            Method::POST,
            "/v1/views/scores_by_account/query",
            json!({}),
        )
        .await;
        assert_eq!(
            query_response.0,
            StatusCode::OK,
            "join query response: {}",
            query_response.1
        );
        assert_join_rows(&query_response.1, 4, 17);

        let restarted_state =
            test_api_state_with_store(store.clone(), "api-test-join-owner-b", true).await;
        let restored = restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap();
        assert_eq!(restored, 1);
        let restarted_router = app(restarted_state);
        let restarted_query = call_json(
            &restarted_router,
            Method::POST,
            "/v1/views/scores_by_account/query",
            json!({}),
        )
        .await;
        assert_eq!(
            restarted_query.0,
            StatusCode::OK,
            "restarted join query response: {}",
            restarted_query.1
        );
        assert_join_rows(&restarted_query.1, 4, 17);
    }

    fn assert_join_rows(response: &Value, expected_epoch: u64, expected_alice_sum: i64) {
        assert_eq!(response["logical_epoch"], expected_epoch);
        assert_eq!(
            response["rows"],
            json!([
                {"account_id": "alice", "sum": expected_alice_sum, "count": 2},
                {"account_id": "bob", "sum": 5, "count": 1}
            ])
        );
    }

    fn assert_latest_device_rows(response: &Value, expected_epoch: u64, expected_device_b: bool) {
        assert_eq!(response["logical_epoch"], expected_epoch);
        assert_eq!(
            response["rows"],
            json!([
                {"device_id": "device-a", "enabled": false},
                {"device_id": "device-b", "enabled": expected_device_b}
            ])
        );
    }

    fn assert_window_rows(response: &Value, expected_epoch: u64, expected_rows: Value) {
        assert_eq!(response["logical_epoch"], expected_epoch);
        assert_eq!(response["rows"], expected_rows);
    }

    fn test_runtime_checkpoint(output_manifest_refs: Vec<String>) -> RuntimeCheckpoint {
        let state_payload = serde_json::json!({
            "schema_version": 1,
            "published_output": {
                "records": []
            }
        })
        .to_string();
        let content_hash = stable_bytes_hash(state_payload.as_bytes());
        RuntimeCheckpoint {
            identity: StandingProgramIdentity {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_ids: vec!["purchases_by_user".to_string()],
                sql_hash: format!("sha256:{}", "a".repeat(64)),
                input_catalog_hash: format!("sha256:{}", "b".repeat(64)),
                output_schema_hash: format!("sha256:{}", "c".repeat(64)),
                compiler_identity: "velorix-logical-view-plan-v1".to_string(),
                runtime_packages: vec![RuntimePackageIdentity {
                    name: "velorix-runtime".to_string(),
                    version: "test".to_string(),
                }],
                package_feature_set: vec!["materialized-view-runtime".to_string()],
                runtime_compatibility: "velorix-materialized-view-runtime-v1".to_string(),
                checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
                native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
            },
            logical_epoch: 7,
            input_frontiers: vec![RelationFrontier {
                relation_id: "purchases".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                committed_offset_exclusive: 11,
            }],
            input_event_time_frontiers: Vec::new(),
            output_frontiers: vec![ViewFrontier {
                view_id: "purchases_by_user".to_string(),
                committed_epoch: 7,
            }],
            checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
            state_root: DurableStateRoot {
                object_key: "v1/state/materialized-view-runtime/program-purchases/checkpoint"
                    .to_string(),
                content_hash,
            },
            state_payload: Some(RuntimeCheckpointStatePayload {
                codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
                payload: state_payload,
            }),
            output_manifest_refs,
            owner_epoch: None,
        }
    }

    async fn test_api_state() -> ApiState {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        test_api_state_with_store(store, "api-test-owner", false).await
    }

    async fn test_api_state_with_store(
        store: Arc<dyn ObjectStore>,
        owner_id: &str,
        reconstruct_ingest_admission: bool,
    ) -> ApiState {
        let validated = validate_operator_authority(
            ObjectStoreAuthorityRef {
                store_id: "test".to_string(),
                namespace: "unit".to_string(),
            },
            store,
            "memory",
            "api-test",
        )
        .await
        .unwrap();
        ApiState::from_validated_authority_with_ingest_admission_startup(
            validated,
            "ignored",
            owner_id,
            reconstruct_ingest_admission,
        )
        .await
        .unwrap()
    }

    fn test_checkpoint_key(checkpoint: &RuntimeCheckpoint) -> ObjectKey {
        ObjectKey::standing_runtime_checkpoint(
            &checkpoint.identity.tenant_id,
            &checkpoint.identity.program_id,
            "purchases_by_user",
            checkpoint.logical_epoch,
            &checkpoint.state_root.content_hash,
        )
        .unwrap()
    }

    fn test_checkpoint_pointer(
        checkpoint_key: &ObjectKey,
        checkpoint: &RuntimeCheckpoint,
    ) -> StandingRuntimeCheckpointPointer {
        StandingRuntimeCheckpointPointer {
            tenant_id: checkpoint.identity.tenant_id.clone(),
            program_id: checkpoint.identity.program_id.clone(),
            view_id: "purchases_by_user".to_string(),
            checkpoint_key: checkpoint_key.as_str().to_string(),
            logical_epoch: checkpoint.logical_epoch,
            content_hash: checkpoint.state_root.content_hash.clone(),
            output_manifest_refs: checkpoint.output_manifest_refs.clone(),
        }
    }

    fn test_checkpoint_record(
        checkpoint_key: &ObjectKey,
        checkpoint: RuntimeCheckpoint,
    ) -> StandingRuntimeCheckpointRecord {
        StandingRuntimeCheckpointRecord {
            schema_version: 1,
            record_kind: "standing_runtime_checkpoint_v1".to_string(),
            view_id: "purchases_by_user".to_string(),
            checkpoint_key: checkpoint_key.as_str().to_string(),
            previous_checkpoint: None,
            checkpoint,
            replay_checkpoints: Vec::new(),
        }
    }

    fn test_purchases_catalog() -> VelorixRelationCatalogV1 {
        let relation_schema = VelorixRelationSchemaV1 {
            relation_id: "purchases".to_string(),
            relation_name: "purchases".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            columns: vec![
                RelationColumnV1 {
                    column_id: "user_id".to_string(),
                    name: "user_id".to_string(),
                    logical_type: VelorixLogicalTypeV1::Utf8,
                    physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                    nullable: false,
                    ordinal: 0,
                    semantic_role: RelationSemanticRoleV1::Metadata,
                },
                RelationColumnV1 {
                    column_id: "amount".to_string(),
                    name: "amount".to_string(),
                    logical_type: VelorixLogicalTypeV1::Int64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                    nullable: false,
                    ordinal: 1,
                    semantic_role: RelationSemanticRoleV1::Metadata,
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
            .expect("test relation schema should fingerprint");

        VelorixRelationCatalogV1 {
            schema_version: RELATION_SCHEMA_VERSION_V1,
            relation_schema,
            schema_fingerprint: schema_fingerprint.clone(),
            datafusion_registration: DataFusionRegistrationV1 {
                name: "purchases".to_string(),
                mode: DataFusionRegistrationModeV1::Table,
            },
            incremental_relation: IncrementalRelationBindingV1 {
                relation_id: "purchases".to_string(),
                schema_fingerprint,
            },
            incremental_adapter: IncrementalAdapterBindingV1 {
                adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
            },
        }
    }

    fn test_purchases_event_time_catalog() -> VelorixRelationCatalogV1 {
        let mut catalog = test_purchases_catalog();
        catalog.relation_schema.columns.insert(
            2,
            RelationColumnV1 {
                column_id: "event_time".to_string(),
                name: "event_time".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::EventTime,
            },
        );
        for (ordinal, column) in catalog.relation_schema.columns.iter_mut().enumerate() {
            column.ordinal = ordinal as u32;
        }
        catalog.relation_schema.event_time_column_id = Some("event_time".to_string());
        let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
            .expect("event-time purchases schema should fingerprint");
        catalog.schema_fingerprint = schema_fingerprint.clone();
        catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
        catalog
    }

    fn test_scores_catalog() -> VelorixRelationCatalogV1 {
        let relation_schema = VelorixRelationSchemaV1 {
            relation_id: "scores".to_string(),
            relation_name: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
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
                    semantic_role: RelationSemanticRoleV1::Metadata,
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
            .expect("scores catalog should fingerprint");
        VelorixRelationCatalogV1 {
            schema_version: RELATION_SCHEMA_VERSION_V1,
            relation_schema,
            schema_fingerprint: schema_fingerprint.clone(),
            datafusion_registration: DataFusionRegistrationV1 {
                name: "scores".to_string(),
                mode: DataFusionRegistrationModeV1::Table,
            },
            incremental_relation: IncrementalRelationBindingV1 {
                relation_id: "scores".to_string(),
                schema_fingerprint,
            },
            incremental_adapter: IncrementalAdapterBindingV1 {
                adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
            },
        }
    }

    fn test_accounts_catalog() -> VelorixRelationCatalogV1 {
        let relation_schema = VelorixRelationSchemaV1 {
            relation_id: "accounts".to_string(),
            relation_name: "accounts".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            columns: vec![
                RelationColumnV1 {
                    column_id: "account_id".to_string(),
                    name: "account_id".to_string(),
                    logical_type: VelorixLogicalTypeV1::Utf8,
                    physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                    nullable: false,
                    ordinal: 0,
                    semantic_role: RelationSemanticRoleV1::PrimaryKey,
                },
                RelationColumnV1 {
                    column_id: "limit".to_string(),
                    name: "limit".to_string(),
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
            primary_key_column_ids: vec!["account_id".to_string()],
            weight_column_id: "delta".to_string(),
            allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
            event_time_column_id: None,
        };
        let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)
            .expect("accounts catalog should fingerprint");
        VelorixRelationCatalogV1 {
            schema_version: RELATION_SCHEMA_VERSION_V1,
            relation_schema,
            schema_fingerprint: schema_fingerprint.clone(),
            datafusion_registration: DataFusionRegistrationV1 {
                name: "accounts".to_string(),
                mode: DataFusionRegistrationModeV1::Table,
            },
            incremental_relation: IncrementalRelationBindingV1 {
                relation_id: "accounts".to_string(),
                schema_fingerprint,
            },
            incremental_adapter: IncrementalAdapterBindingV1 {
                adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
            },
        }
    }

    fn test_device_status_catalog() -> VelorixRelationCatalogV1 {
        let relation_schema = VelorixRelationSchemaV1 {
            relation_id: "device_status".to_string(),
            relation_name: "device_status".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            columns: vec![
                RelationColumnV1 {
                    column_id: "device_id".to_string(),
                    name: "device_id".to_string(),
                    logical_type: VelorixLogicalTypeV1::Utf8,
                    physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                    nullable: false,
                    ordinal: 0,
                    semantic_role: RelationSemanticRoleV1::PrimaryKey,
                },
                RelationColumnV1 {
                    column_id: "enabled".to_string(),
                    name: "enabled".to_string(),
                    logical_type: VelorixLogicalTypeV1::Bool,
                    physical_arrow_type: ArrowPhysicalTypeV1::Boolean,
                    nullable: false,
                    ordinal: 1,
                    semantic_role: RelationSemanticRoleV1::Value,
                },
                RelationColumnV1 {
                    column_id: "event_time".to_string(),
                    name: "event_time".to_string(),
                    logical_type: VelorixLogicalTypeV1::Int64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                    nullable: false,
                    ordinal: 2,
                    semantic_role: RelationSemanticRoleV1::EventTime,
                },
                RelationColumnV1 {
                    column_id: "delta".to_string(),
                    name: "delta".to_string(),
                    logical_type: VelorixLogicalTypeV1::Int64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                    nullable: false,
                    ordinal: 3,
                    semantic_role: RelationSemanticRoleV1::Weight,
                },
            ],
            primary_key_column_ids: vec!["device_id".to_string()],
            weight_column_id: "delta".to_string(),
            allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
            event_time_column_id: Some("event_time".to_string()),
        };
        let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)
            .expect("device status catalog should fingerprint");
        VelorixRelationCatalogV1 {
            schema_version: RELATION_SCHEMA_VERSION_V1,
            relation_schema,
            schema_fingerprint: schema_fingerprint.clone(),
            datafusion_registration: DataFusionRegistrationV1 {
                name: "device_status".to_string(),
                mode: DataFusionRegistrationModeV1::Table,
            },
            incremental_relation: IncrementalRelationBindingV1 {
                relation_id: "device_status".to_string(),
                schema_fingerprint,
            },
            incremental_adapter: IncrementalAdapterBindingV1 {
                adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
            },
        }
    }
}
