use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env,
    io::Cursor,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Instant,
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
use futures::{StreamExt, TryStreamExt};
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path as ObjectPath,
    prefix::PrefixStore,
    ObjectStore, ObjectStoreExt, PutMode,
};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as AsyncMutex;
use velorix_control::{
    ingest_writer_runtime::DeployedIngestWriterRuntime,
    meta_admin::{
        validate_bearer_token, AcquireStandingRuntimeOwnerOutcome,
        AcquireStandingRuntimeOwnerRequest, BeginViewBootstrapOutcome, BeginViewBootstrapRequest,
        CaptureIngestSourceCutRequest, CommitIngestRangeOutcome,
        FixViewBootstrapActivationCutOutcome, FixViewBootstrapActivationCutRequest, GrpcMetaStore,
        IngestRangeReservation, IngestSourceRelationIdentityV1, MetaStore, MetaStoreError,
        PromoteViewBootstrapOutcome, PromoteViewBootstrapRequest,
        PublishStandingRuntimeCheckpointOutcome, PublishStandingRuntimeCheckpointRequest,
        ReserveIngestRangeOutcome, StandingRuntimeCheckpointPointer,
        StandingRuntimeFencingCapability, StandingRuntimeOwnerClaim, StandingRuntimeOwnerToken,
        StoreRelationCatalogOutcome, ViewBootstrapControlV1, ViewBootstrapLifecycleV1,
        INGEST_SOURCE_IDENTITY_GENERATION_V1, STANDING_RUNTIME_BACKEND_TIME_SOURCE_RAFT_REPLICATED,
        STANDING_RUNTIME_FENCING_CAPABILITY_SCHEMA_VERSION,
        STANDING_RUNTIME_LEASE_AUTHORITY_KIND_HIQLITE_RAFT_SERIALIZED,
        STANDING_RUNTIME_LEASE_AUTHORITY_KIND_RAFT_REPLICATED_TIME,
        STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_BACKEND_WALL_CLOCK_TTL,
        STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_OPERATION_DRIVEN_LOGICAL,
        STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX, STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX,
        STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX,
        STANDING_RUNTIME_OWNER_SCOPE_KIND_TENANT_PROGRAM_VIEW,
    },
    operator_authority::{
        validate_operator_authority, ObjectStoreAuthorityRef, OperatorAuthorityStartupComponents,
        ValidatedOperatorAuthority,
    },
    storage_admin::{
        ActiveMaterializedView, AppendValidatedEnvelopeOutcome, AuthoritativeNamespace,
        AuthoritativeObjectStoreCapabilitiesV1, CreateRelationCatalogOutcome,
        IngestBatchDescriptor, IngestEnvelope, IngestEnvelopeEncodeRequest, IngestEnvelopeHeader,
        IngestLog, InvalidExecutionModeReason, MaterializedViewAdmissionStatus,
        MaterializedViewApiMetadata, MaterializedViewDeploymentStatus,
        MaterializedViewExecutionMode, MaterializedViewLifecycleStatus, MaterializedViewRegistry,
        MaterializedViewRegistryError, MaterializedViewRequestFieldSpec,
        MaterializedViewResponseColumnSpec, MaterializedViewResponseSchema,
        MaterializedViewRuntimeBinding, ObjectKey, ObjectStoreCapabilityProfile,
        RegisterMaterializedViewOutcome, RelationCatalogRegistry, ReplayCheckpoint,
        StandingRuntimeCheckpointKeyParts,
    },
};
use velorix_core::{
    delta::DeltaBatch,
    query::{QueryBindValue, QueryPolicy},
    relation::{
        datafusion_schema_from_catalog, orders_sum_count_relation_catalog, ArrowPhysicalTypeV1,
        DataFusionRegistrationModeV1, DataFusionRegistrationV1, DictionaryKeyTypeV1,
        IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
    standing_program::{
        BuiltinRuntimeIdentity, CausalCutV1, CausalViewCursorV1, EpochIdempotencyKey,
        InputEventTimeWatermark, MaterializedViewPage, NativeCodePolicy, RelationFrontier,
        RelationInputBatch, RuntimeCheckpoint, RuntimeCheckpointInputCoverageV1,
        RuntimeCheckpointPartitionCoverageV1, RuntimeCheckpointRelationCoverageV1,
        RuntimeCheckpointStatePayload, ScopedViewId, SnapshotPageRequest, StandingInputChangeV1,
        StandingProgramIdentity, StandingProgramRuntime, StandingProgramRuntimeError,
        ViewOutputDelta, RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
    },
    view_contract::{
        catalog_input_relation_schema, published_relation_binding_v1,
        resolve_view_input_relation_v1, stable_bytes_hash,
        validate_materialized_standing_view_spec, validate_published_relation_binding_v1,
        view_spec_hash, ColumnSchema, PublishedRelationBindingV1, RelationSchema,
        ResolvedAdmissionInput, SourceInputBindingV1, SqlDataType, SqlDialect, SqlSourceKind,
        SqlStructField, StandingViewShape, StandingViewSpec, ViewDependencyEdgeBindingV1,
        PUBLISHED_RELATION_BINDING_SCHEMA_VERSION_V1,
    },
    view_plan::{
        lower_supported_analytic_row_number_sql_to_logical_plan,
        lower_supported_sql_to_logical_plan, supported_join_view_plan_aggregate_outputs,
        supported_join_view_plan_is_self_join, supported_join_view_plan_is_singleton,
        supported_view_plan_aggregate_outputs, supported_view_plan_group_keys,
        validate_catalog_backed_sum_count_view_sql, validate_supported_analytic_row_number_sql,
        validate_supported_filter_project_sql, validate_supported_join_view_sql,
        validate_supported_latest_by_key_sql, validate_supported_semi_anti_join_sql,
        validate_supported_three_input_inner_join_count_sql,
        validate_supported_tumbling_window_sql, LogicalPlanAggregateFunctionV1,
        SupportedAggregateInputRelationSide, SupportedAggregateOutput,
        SupportedAnalyticRowNumberPlan, SupportedFilterProjectPlan, SupportedJoinViewPlan,
        SupportedLatestByKeyPlan, SupportedProjectionExpr, SupportedThreeInputInnerJoinCountPlanV1,
        SupportedTumblingWindowPlan, SupportedViewPlan, VelorixLogicalViewExecutionV1,
        VelorixLogicalViewPlanV1, ViewPlanError, INCREMENTAL_BAG_SEMANTICS_VERSION_V1,
        INCREMENTAL_KEY_SEMANTICS_VERSION_V1, OUTPUT_PUBLICATION_PROTOCOL_VERSION_V1,
    },
};
use velorix_runtime::{
    query_policy_catalog::{
        QueryPolicyCatalogError, QueryPolicyCatalogRecord, QueryPolicyCatalogStore,
    },
    runtime_contract::{
        query_record_batches_table_with_bindings_and_policy_and_limiter,
        validate_record_batch_table_query_with_bindings_and_policy, ProductionQueryRuntime,
        QueryExecutionLimiter, MATERIALIZED_VIEW_RUNTIME_NAME,
    },
};

mod checkpoint_publication;
mod ingest_epoch;
mod openapi;
mod query_serving;
mod recovery;
mod view_admission;

use checkpoint_publication::*;
use ingest_epoch::*;
use openapi::openapi_json;
use query_serving::*;
use recovery::*;
use view_admission::*;

const PUBLIC_1_0_MAX_JOIN_INPUT_RELATIONS: usize = 3;
const PUBLIC_1_0_MAX_TOP_K_LIMIT: usize = 1_000;
const DEFAULT_MAX_STANDING_RUNTIME_OUTPUT_DELTA_RECORDS: usize = 100_000;
const DEFAULT_MAX_STANDING_RUNTIME_STATE_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone)]
pub struct ApiState {
    store: Arc<dyn ObjectStore>,
    capabilities: Arc<AuthoritativeObjectStoreCapabilitiesV1>,
    ingest_writer: Arc<DeployedIngestWriterRuntime<ObjectStoreAuthorityRef>>,
    meta_store: Option<Arc<dyn MetaStore>>,
    view_bootstrap_meta_store: Option<Arc<dyn MetaStore>>,
    meta_store_endpoint: Option<String>,
    owner_id: String,
    standing_runtime_owner_ttl_ms: u64,
    standing_runtime_fencing_required: bool,
    standing_runtime_fencing_mode: StandingRuntimeFencingMode,
    api_bearer_token: Option<Arc<str>>,
    admin_bearer_token: Option<Arc<str>>,
    max_request_body_bytes: usize,
    max_ingest_rows: usize,
    max_standing_runtime_output_delta_records: usize,
    max_standing_runtime_state_payload_bytes: usize,
    output_compaction_interval_epochs: u64,
    experimental_advanced_view_features: bool,
    experimental_view_on_view: bool,
    background_tasks: Arc<Mutex<BackgroundTaskStatus>>,
    background_compactions: Arc<Mutex<BTreeSet<String>>>,
    standing_runtimes: Arc<StandingRuntimeRegistry>,
    standing_runtime_factories: Arc<StandingRuntimeFactoryRegistry>,
    query_runtimes: Arc<Mutex<HashMap<String, ProductionQueryRuntime>>>,
}

type SharedStandingRuntime = Arc<Mutex<Box<dyn StandingProgramRuntime + Send>>>;

const MAX_CONCURRENT_EPOCH_APPENDS: usize = 16;

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
    input_frontiers: Vec<RelationFrontier>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct BackgroundTaskStatus {
    materialization_scheduled: u64,
    materialization_started: u64,
    materialization_succeeded: u64,
    materialization_failed: u64,
    last_materialization_elapsed_us: Option<u128>,
    last_materialization_error: Option<String>,
    compaction_scheduled: u64,
    compaction_already_running: u64,
    compaction_started: u64,
    compaction_succeeded: u64,
    compaction_failed: u64,
    last_compaction_elapsed_us: Option<u128>,
    last_compaction_error: Option<String>,
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

    /// Create a runtime bound to a published view output input.
    ///
    /// Default implementation fails closed: only factories that explicitly
    /// support view-on-view inputs override this.
    fn create_with_published_binding_plan_and_spec(
        &self,
        _identity: &StandingProgramIdentity,
        _binding: &PublishedRelationBindingV1,
        _logical_plan: &VelorixLogicalViewPlanV1,
        _spec: &StandingViewSpec,
        _input_schemas: &[RelationSchema],
        _output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Err("standing runtime factory does not support published-view inputs".to_string())
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
    #[serde(skip)]
    manifest_hash: String,
}

fn standing_runtime_checkpoint_record_from_slice(
    bytes: &[u8],
) -> Result<StandingRuntimeCheckpointRecord, serde_json::Error> {
    let mut value: Value = serde_json::from_slice(bytes)?;
    if let Some(identity) = value.pointer_mut("/checkpoint/identity") {
        normalize_legacy_standing_program_identity_value(identity);
    }
    let mut record: StandingRuntimeCheckpointRecord = serde_json::from_value(value)?;
    record.manifest_hash = stable_bytes_hash(bytes);
    Ok(record)
}

fn normalize_legacy_standing_program_identity_value(value: &mut Value) {
    let Some(identity) = value.as_object_mut() else {
        return;
    };
    move_legacy_key(identity, "compiler", "_identity", "planner_identity");
    move_legacy_key(
        identity,
        "runtime",
        "_packages",
        "builtin_runtime_identities",
    );
    move_legacy_key(identity, "package", "_feature_set", "runtime_capabilities");
}

fn move_legacy_key(
    object: &mut serde_json::Map<String, Value>,
    old_prefix: &str,
    old_suffix: &str,
    new_key: &str,
) {
    if object.contains_key(new_key) {
        return;
    }
    let old_key = format!("{old_prefix}{old_suffix}");
    if let Some(value) = object.remove(old_key.as_str()) {
        object.insert(new_key.to_string(), value);
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    producer_commit: Option<StandingRuntimeProducerCommitV1>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StandingRuntimeProducerCommitV1 {
    schema_version: u32,
    producer_view_generation: u64,
    producer_plan_hash: String,
    output_stream_id: String,
    output_schema_hash: String,
    key_descriptor_hash: String,
    delta_codec_identity: String,
    frontier_kind: String,
    checkpoint_key: String,
    checkpoint_content_hash: String,
    causal_cut_digest: String,
    producer_commit_digest: String,
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
                    let Ok(plan) = validate_supported_analytic_row_number_sql(sql, catalog) else {
                        let Ok(plan) = validate_supported_filter_project_sql(sql, catalog) else {
                            return Ok(None);
                        };
                        return filter_project_output_schema(view_id, catalog, &plan)
                            .map(|schema| Some(vec![schema]));
                    };
                    return analytic_row_number_output_schema(view_id, catalog, &plan)
                        .map(|schema| Some(vec![schema]));
                };
                return tumbling_window_output_schema(view_id, catalog, &plan)
                    .map(|schema| Some(vec![schema]));
            };
            return latest_by_key_output_schema(view_id, catalog, &plan)
                .map(|schema| Some(vec![schema]));
        };
        aggregate_output_schema(view_id, catalog, &plan).map(|schema| Some(vec![schema]))
    }

    fn output_schemas_for_view_request_with_catalogs(
        &self,
        view_id: &str,
        sql: &str,
        catalogs: &[VelorixRelationCatalogV1],
        input_schema_fingerprint: &str,
    ) -> Result<Option<Vec<RelationSchema>>, ApiError> {
        if catalogs.len() == 3 {
            if let Ok(plan) = validate_supported_three_input_inner_join_count_sql(sql, catalogs) {
                if catalogs
                    .iter()
                    .map(|catalog| &catalog.relation_schema.relation_id)
                    .ne(plan.ordered_input_relation_ids.iter())
                {
                    return Err(ApiError::bad_request(
                        "three-input JOIN catalogs must follow SQL join order",
                    ));
                }
                return three_input_join_count_output_schema(view_id, catalogs, &plan)
                    .map(|schema| Some(vec![schema]));
            }
            return Ok(None);
        }
        if matches!(catalogs.len(), 1 | 2) {
            if catalogs.len() == 2 {
                if let Ok(plan) = validate_supported_semi_anti_join_sql(sql, catalogs) {
                    let left_catalog = catalogs
                        .iter()
                        .find(|catalog| {
                            catalog.relation_schema.relation_id == plan.left_input_relation_id
                        })
                        .ok_or_else(|| {
                            ApiError::bad_request("semi/anti join left catalog is missing")
                        })?;
                    return filter_project_output_schema(view_id, left_catalog, &plan.projection)
                        .map(|schema| Some(vec![schema]));
                }
            }
            if let Ok(plan) = validate_supported_join_view_sql(sql, catalogs) {
                validate_join_plan_catalog_order(&plan, catalogs)?;
                return join_sum_count_output_schema(view_id, catalogs, &plan)
                    .map(|schema| Some(vec![schema]));
            }
            if catalogs.len() == 2 {
                return Ok(None);
            }
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

    fn create_with_published_binding_plan_and_spec(
        &self,
        identity: &StandingProgramIdentity,
        binding: &PublishedRelationBindingV1,
        logical_plan: &VelorixLogicalViewPlanV1,
        _spec: &StandingViewSpec,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        velorix_runtime::materialized_view_runtime::create_standing_runtime_with_logical_plan_and_published_binding(
            identity,
            binding,
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
            view_bootstrap_meta_store: None,
            meta_store_endpoint: None,
            owner_id,
            standing_runtime_owner_ttl_ms: 30_000,
            standing_runtime_fencing_required: false,
            standing_runtime_fencing_mode: StandingRuntimeFencingMode::SingleWriter,
            api_bearer_token: None,
            admin_bearer_token: None,
            max_request_body_bytes: 1024 * 1024,
            max_ingest_rows: 10_000,
            max_standing_runtime_output_delta_records:
                DEFAULT_MAX_STANDING_RUNTIME_OUTPUT_DELTA_RECORDS,
            max_standing_runtime_state_payload_bytes:
                DEFAULT_MAX_STANDING_RUNTIME_STATE_PAYLOAD_BYTES,
            output_compaction_interval_epochs: 0,
            experimental_advanced_view_features: false,
            experimental_view_on_view: false,
            background_tasks: Arc::new(Mutex::new(BackgroundTaskStatus::default())),
            background_compactions: Arc::new(Mutex::new(BTreeSet::new())),
            standing_runtimes: Arc::new(StandingRuntimeRegistry::default()),
            standing_runtime_factories: Arc::new(StandingRuntimeFactoryRegistry::default()),
            query_runtimes: Arc::new(Mutex::new(HashMap::new())),
        };
        state.register_standing_program_runtime_factory(
            MATERIALIZED_VIEW_RUNTIME_NAME,
            MaterializedViewRuntimeFactory,
        );

        Ok(state)
    }

    pub fn with_meta_store(mut self, meta_store: Arc<dyn MetaStore>) -> Self {
        self.view_bootstrap_meta_store = Some(Arc::clone(&meta_store));
        self.meta_store = Some(meta_store);
        self
    }

    #[cfg(test)]
    fn with_view_bootstrap_meta_store(mut self, meta_store: Arc<dyn MetaStore>) -> Self {
        self.view_bootstrap_meta_store = Some(meta_store);
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

    pub fn with_standing_runtime_budget_limits(
        mut self,
        max_output_delta_records: usize,
        max_state_payload_bytes: usize,
    ) -> Self {
        self.max_standing_runtime_output_delta_records = max_output_delta_records;
        self.max_standing_runtime_state_payload_bytes = max_state_payload_bytes;
        self
    }

    pub fn with_output_compaction_interval_epochs(mut self, interval_epochs: u64) -> Self {
        self.output_compaction_interval_epochs = interval_epochs;
        self
    }

    #[cfg(test)]
    pub fn with_experimental_advanced_view_features(mut self, enabled: bool) -> Self {
        self.experimental_advanced_view_features = enabled;
        self
    }

    #[cfg(test)]
    pub fn with_experimental_view_on_view(mut self, enabled: bool) -> Self {
        self.experimental_view_on_view = enabled;
        self
    }

    #[cfg(test)]
    fn background_task_status(&self) -> BackgroundTaskStatus {
        self.background_tasks
            .lock()
            .map(|status| status.clone())
            .unwrap_or_else(|_| BackgroundTaskStatus {
                last_materialization_error: Some(
                    "background task status lock poisoned".to_string(),
                ),
                last_compaction_error: Some("background task status lock poisoned".to_string()),
                ..BackgroundTaskStatus::default()
            })
    }

    fn record_background_compaction_scheduled(&self) {
        if let Ok(mut status) = self.background_tasks.lock() {
            status.compaction_scheduled += 1;
        }
    }

    fn record_background_compaction_already_running(&self) {
        if let Ok(mut status) = self.background_tasks.lock() {
            status.compaction_already_running += 1;
        }
    }

    fn try_start_background_compaction(&self, view_id: &str) -> bool {
        match self.background_compactions.lock() {
            Ok(mut active) => active.insert(view_id.to_string()),
            Err(_) => {
                self.record_background_compaction_error(
                    "background compaction registry lock poisoned",
                    0,
                );
                false
            }
        }
    }

    fn finish_background_compaction(&self, view_id: &str) {
        if let Ok(mut active) = self.background_compactions.lock() {
            active.remove(view_id);
        }
    }

    fn record_background_compaction_started(&self) {
        if let Ok(mut status) = self.background_tasks.lock() {
            status.compaction_started += 1;
        }
    }

    fn record_background_compaction_succeeded(&self, elapsed_us: u128) {
        if let Ok(mut status) = self.background_tasks.lock() {
            status.compaction_succeeded += 1;
            status.last_compaction_elapsed_us = Some(elapsed_us);
            status.last_compaction_error = None;
        }
    }

    fn record_background_compaction_error(&self, error: impl Into<String>, elapsed_us: u128) {
        match self.background_tasks.lock() {
            Ok(mut status) => {
                status.compaction_failed += 1;
                status.last_compaction_elapsed_us = Some(elapsed_us);
                status.last_compaction_error = Some(error.into());
            }
            Err(_) => eprintln!("background task status lock poisoned"),
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
        let Some(factory) = factories.get(MATERIALIZED_VIEW_RUNTIME_NAME) else {
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
        .route(
            "/v1/relations/{relation_id}/ingest",
            post(ingest_relation_rows),
        )
        .route("/v1/relations/ingest", post(ingest_epoch))
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
    #[cfg(test)]
    let protected_routes = protected_routes.route("/v1/ingest", post(ingest_rows_test_compat));
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
        .validate_namespace(AuthoritativeNamespace::ArtifactCatalog)?
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
        .with_standing_runtime_budget_limits(
            config.max_standing_runtime_output_delta_records,
            config.max_standing_runtime_state_payload_bytes,
        )
        .with_standing_runtime_fencing_mode(config.standing_runtime_fencing)
        .with_standing_runtime_owner_ttl_ms(config.standing_runtime_owner_ttl_ms)
        .with_output_compaction_interval_epochs(config.output_compaction_interval_epochs);
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct IngestRelationRowsRequest {
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
#[serde(deny_unknown_fields)]
pub struct IngestEpochRequest {
    pub batches: Vec<IngestRowsRequest>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestAckMode {
    #[default]
    Materialized,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepairIngestEpochRuntimeFailureRequest {
    epoch_manifest_id: String,
    tenant_id: String,
    program_id: String,
    view_id: String,
    confirm_standing_runtime_repaired: bool,
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
    /// Explicit input kind. Defaults to `Source`.
    ///
    /// A `View` input resolves against an active view's published output,
    /// never against a physical relation catalog. Admission rejects a `View`
    /// input when view-on-view is disabled.
    #[serde(default)]
    pub input_kind: InputRelationKind,
}

/// Explicit kind for a view input relation reference.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InputRelationKind {
    /// A registered physical ingest source.
    #[default]
    Source,
    /// An upstream materialized view output.
    View,
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
    batch_limit: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackfillRangeRequest {
    relation_id: String,
    relation_version: String,
    #[serde(default)]
    stream_id: Option<String>,
    #[serde(default)]
    partition_id: Option<u32>,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BackfillScopeRequest {
    #[serde(rename = "where")]
    where_clause: String,
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
    epoch_manifest_id: String,
    ingest_epoch: String,
    materialized_through: Option<u64>,
    ack_mode: IngestAckMode,
    materialization: IngestMaterializationResponse,
    timings: IngestTimingResponse,
}

#[derive(Clone, Debug, Serialize)]
struct IngestEpochResponse {
    outcome: String,
    epoch_manifest_id: String,
    epoch_manifest_key: String,
    ingest_epoch: String,
    materialized_through: Option<u64>,
    ack_mode: IngestAckMode,
    materialization: IngestMaterializationResponse,
    timings: IngestTimingResponse,
    batches: Vec<IngestResponse>,
}

#[derive(Clone, Debug, Serialize)]
struct IngestMaterializationResponse {
    status: String,
    active_views: usize,
    applied_batches: usize,
    materialized_through: Option<u64>,
    checkpoint_writes: usize,
    applied_batches_per_checkpoint_write: Option<usize>,
    output_delta_writes: usize,
    state_payload_writes: usize,
    checkpoint_record_writes: usize,
    checkpoint_pointer_writes: usize,
    latest_cache_writes: usize,
    checkpoint_publication_writes: usize,
}

#[derive(Clone, Debug, Serialize)]
struct IngestTimingResponse {
    total_ms: u128,
    total_us: u128,
    avg_batch_us: Option<u128>,
    avg_row_us: Option<u128>,
    rows_per_second: Option<u64>,
    batch_count: usize,
    row_count: usize,
}

#[derive(Debug)]
struct IngestTimer {
    started_at: Instant,
    last_stage_at: Instant,
    batch_count: usize,
    row_count: usize,
}

impl IngestTimer {
    fn start() -> Self {
        let now = Instant::now();
        Self {
            started_at: now,
            last_stage_at: now,
            batch_count: 0,
            row_count: 0,
        }
    }

    fn set_workload(&mut self, batch_count: usize, row_count: usize) {
        self.batch_count = batch_count;
        self.row_count = row_count;
    }

    fn mark(&mut self, stage: &str) {
        let now = Instant::now();
        let _ = stage;
        self.last_stage_at = now;
    }

    fn finish(self) -> IngestTimingResponse {
        let total = self.started_at.elapsed();
        let total_us = total.as_micros();
        IngestTimingResponse {
            total_ms: total.as_millis(),
            total_us,
            avg_batch_us: nonzero_div(total_us, self.batch_count),
            avg_row_us: nonzero_div(total_us, self.row_count),
            rows_per_second: rows_per_second(self.row_count, total_us),
            batch_count: self.batch_count,
            row_count: self.row_count,
        }
    }
}

fn nonzero_div(total: u128, count: usize) -> Option<u128> {
    total.checked_div(count as u128)
}

fn nonzero_div_usize(total: usize, count: usize) -> Option<usize> {
    total.checked_div(count)
}

fn rows_per_second(row_count: usize, total_us: u128) -> Option<u64> {
    if row_count == 0 || total_us == 0 {
        None
    } else {
        Some(((row_count as u128 * 1_000_000) / total_us) as u64)
    }
}

fn record_materialized_through(boundary: &mut Option<u64>, logical_epoch: u64) {
    *boundary = Some(boundary.map_or(logical_epoch, |current| current.min(logical_epoch)));
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
struct CompactViewResponse {
    view_id: String,
    outcome: String,
    mode: String,
    logical_epoch: Option<u64>,
    before_pages: usize,
    after_pages: usize,
    compacted_manifests: usize,
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
    outcome: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ViewCatalogResponse {
    views: Vec<ViewResponse>,
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
        "materialization_policy": {
            "preferred_ingest_path": "/v1/relations/{relation_id}/ingest",
            "multi_relation_ingest_path": "/v1/relations/ingest",
            "default_ack_mode": "materialized",
            "ack_modes": ["materialized"],
            "enforced_public_1_0_limits": {
                "max_request_body_bytes": state.max_request_body_bytes,
                "max_ingest_rows_per_request": state.max_ingest_rows,
                "max_join_input_relations": PUBLIC_1_0_MAX_JOIN_INPUT_RELATIONS,
                "max_top_k_limit": PUBLIC_1_0_MAX_TOP_K_LIMIT,
                "max_output_delta_records_per_commit": state.max_standing_runtime_output_delta_records,
                "max_state_payload_bytes_per_checkpoint": state.max_standing_runtime_state_payload_bytes
            },
            "checkpoint_coalescing": "one checkpoint publish per affected active view per committed epoch",
            "latency_diagnostics": "ingest responses include total_us, avg_batch_us, avg_row_us, rows_per_second, workload shape, and write coalescing counters; detailed stage timings belong in traces/metrics",
            "materialization_write_counters": ["output_delta_writes", "state_payload_writes", "checkpoint_record_writes", "checkpoint_pointer_writes", "latest_cache_writes", "checkpoint_publication_writes"]
        },
        "admin_auth": {
            "configured": state.admin_bearer_token.is_some(),
            "mode": if state.admin_bearer_token.is_some() { "bearer-token" } else { "unauthenticated-dev" },
        },
        "metadata_store": metadata_store
    })))
}

fn object_store_capabilities_json(capabilities: &AuthoritativeObjectStoreCapabilitiesV1) -> Value {
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
    let catalog = orders_sum_count_relation_catalog().map_err(ApiError::bad_request)?;
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
        (None, true) => orders_sum_count_relation_catalog().map_err(ApiError::bad_request)?,
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
        state.experimental_advanced_view_features,
    )))
}

async fn run_view_backfill(
    State(state): State<ApiState>,
    AxumPath(view_id): AxumPath<String>,
    Json(request): Json<BackfillViewRequest>,
) -> Result<(StatusCode, Json<BackfillViewResponse>), ApiError> {
    let outcome = run_view_backfill_step(&state, &view_id, request.batch_limit, None, None).await?;
    Ok((StatusCode::OK, Json(outcome)))
}

fn spawn_background_view_output_compaction(state: ApiState, view_id: String) -> bool {
    if !state.try_start_background_compaction(&view_id) {
        state.record_background_compaction_already_running();
        return false;
    }
    state.record_background_compaction_scheduled();
    tokio::spawn(async move {
        let started_at = Instant::now();
        state.record_background_compaction_started();
        if let Err(error) = compact_view_output_once(&state, &view_id, "background").await {
            state.record_background_compaction_error(
                error.message.clone(),
                started_at.elapsed().as_micros(),
            );
            eprintln!(
                "background view output compaction failed for `{view_id}`: {}",
                error.message
            );
        } else {
            state.record_background_compaction_succeeded(started_at.elapsed().as_micros());
        }
        state.finish_background_compaction(&view_id);
    });
    true
}

fn maybe_spawn_background_view_output_compaction_after_checkpoint(
    state: &ApiState,
    view_id: &str,
    logical_epoch: u64,
) -> bool {
    if !state.experimental_advanced_view_features {
        return false;
    }
    let interval = state.output_compaction_interval_epochs;
    if !should_schedule_background_output_compaction(interval, logical_epoch) {
        return false;
    }
    spawn_background_view_output_compaction(state.clone(), view_id.to_string())
}

fn should_schedule_background_output_compaction(interval_epochs: u64, logical_epoch: u64) -> bool {
    interval_epochs != 0 && logical_epoch != 0 && logical_epoch.is_multiple_of(interval_epochs)
}

async fn compact_view_output_once(
    state: &ApiState,
    view_id: &str,
    mode: &str,
) -> Result<CompactViewResponse, ApiError> {
    let active = state
        .view_registry()?
        .read_active(view_id)
        .await
        .map_err(materialized_view_registry_error_to_api)?;
    ensure_view_execution_allowed(&active)?;
    let identity = active_standing_runtime_identity(&active).ok_or_else(|| {
        ApiError::conflict(format!(
            "standing runtime view `{view_id}` is missing runtime identity"
        ))
    })?;
    let operation_lock = state.standing_runtime_operation_lock(identity, &active.spec.view_id)?;
    let _guard = operation_lock.lock().await;
    let owner = state
        .acquire_standing_runtime_owner(identity, &active.spec.view_id)
        .await?;
    let Some(record) =
        read_latest_standing_runtime_checkpoint(state, identity, &active.spec.view_id).await?
    else {
        return Err(ApiError::service_unavailable(format!(
            "standing runtime checkpoint is unavailable for view `{view_id}`"
        )));
    };
    let before_pages = record.checkpoint.output_manifest_refs.len();
    let checkpoint_key =
        ObjectKey::parse_standing_runtime_checkpoint(record.checkpoint_key.clone())
            .map_err(ApiError::bad_request)?
            .0;
    let publication = standing_runtime_output_manifest_record_for_checkpoint(
        &record.checkpoint,
        &record.view_id,
        &checkpoint_key,
    )?
    .ok_or_else(|| {
        ApiError::bad_request(format!(
            "standing runtime checkpoint for view `{}` cannot publish output without hydrated state payload",
            record.view_id
        ))
    })?;
    let output_manifest_refs = vec![format!(
        "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
        publication.manifest_key.as_str()
    )];
    let after_pages = publication.manifest_record.pages.len();
    let compacted_manifests =
        usize::from(record.checkpoint.output_manifest_refs != output_manifest_refs);

    publish_standing_runtime_output_snapshot_from_checkpoint(state, &record).await?;
    drop(owner);

    Ok(CompactViewResponse {
        view_id: active.spec.view_id,
        outcome: if compacted_manifests == 0 {
            "already_compacted".to_string()
        } else {
            "compacted".to_string()
        },
        mode: mode.to_string(),
        logical_epoch: Some(record.checkpoint.logical_epoch),
        before_pages,
        after_pages,
        compacted_manifests,
    })
}

#[cfg(test)]
async fn compact_standing_runtime_output_manifest(
    state: &ApiState,
    record: &StandingRuntimeCheckpointRecord,
    manifest: &StandingRuntimeOutputManifestRecord,
) -> Result<StandingRuntimeOutputPublication, ApiError> {
    let mut output = DeltaBatch::default();
    for page in &manifest.pages {
        let (_key, page_record) =
            read_standing_runtime_output_page_record(state, page, &manifest.view_id).await?;
        let page_output: DeltaBatch = serde_json::from_value(page_record.published_output)
            .map_err(|source| ApiError::bad_request(source.to_string()))?;
        output = output.combine(&page_output);
    }
    let compacted = DeltaBatch::from_records(
        output
            .net_rows()
            .map_err(|source| ApiError::bad_request(source.to_string()))?,
    );
    let published_output =
        serde_json::to_value(compacted).map_err(|source| ApiError::internal(source.to_string()))?;
    let checkpoint_key =
        ObjectKey::parse_standing_runtime_checkpoint(record.checkpoint_key.clone())
            .map_err(ApiError::bad_request)?
            .0;
    let publication = standing_runtime_output_publication(
        &record.checkpoint,
        &manifest.view_id,
        &checkpoint_key,
        published_output,
    )?;
    for (page_key, page_record) in &publication.page_records {
        put_standing_runtime_output_page(state, page_key, page_record).await?;
    }
    put_standing_runtime_output_manifest(
        state,
        &publication.manifest_key,
        &publication.manifest_record,
    )
    .await?;
    Ok(publication)
}

async fn publish_standing_runtime_output_snapshot_from_checkpoint(
    state: &ApiState,
    record: &StandingRuntimeCheckpointRecord,
) -> Result<StandingRuntimeOutputPublication, ApiError> {
    let checkpoint_key =
        ObjectKey::parse_standing_runtime_checkpoint(record.checkpoint_key.clone())
            .map_err(ApiError::bad_request)?
            .0;
    let publication = standing_runtime_output_manifest_record_for_checkpoint(
        &record.checkpoint,
        &record.view_id,
        &checkpoint_key,
    )?
    .ok_or_else(|| {
        ApiError::bad_request(format!(
            "standing runtime checkpoint for view `{}` cannot publish output without hydrated state payload",
            record.view_id
        ))
    })?;
    for (page_key, page_record) in &publication.page_records {
        persist_standing_runtime_output_page(state, page_key, page_record).await?;
    }
    persist_standing_runtime_output_manifest(
        state,
        &publication.manifest_key,
        &publication.manifest_record,
    )
    .await?;
    Ok(publication)
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
        let (token, start, end) = next_unquoted_sql_word(remaining)?;
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
    if !hex.len().is_multiple_of(2) {
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
    if !sql_references_table(sql_template, &output_id) {
        return Err(ApiError::bad_request(format!(
            "standing runtime view `{view_id}` sql_template must reference table `{output_id}`"
        )));
    }
    let output_schema = output_schemas
        .iter()
        .find(|schema| schema.relation_id == output_id)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "standing runtime view `{view_id}` runtime metadata has no matching output schema `{output_id}`"
            ))
        })?;
    let table_schema = arrow_schema_from_incremental_relation_schema(output_schema)?;
    let bound_sql = render_view_sql_template_for_validation(sql_template, &api.request)?;
    validate_record_batch_table_query_with_bindings_and_policy(
        &output_id,
        table_schema.clone(),
        &normalize_view_query_sql(&bound_sql.sql, &output_id),
        &bound_sql.bind_values,
        QueryPolicy::default(),
    )
    .await
    .map_err(ApiError::bad_request)?;
    validate_sql_template_response_schema_bindings(
        view_id,
        api.response_schema.as_ref(),
        &output_id,
        table_schema,
        &normalize_view_query_sql(&bound_sql.sql, &output_id),
        &bound_sql.bind_values,
    )
    .await?;
    Ok(())
}

/// Admits a response schema only when every direct source has one compatible
/// result column in the rendered template query.  The legacy `key.*` and
/// `value.*` JSON paths deliberately remain dynamic: their object members are
/// encoded inside a UTF-8 JSON result column and therefore have no planner
/// field to inspect at admission time.
async fn validate_sql_template_response_schema_bindings(
    view_id: &str,
    response_schema: Option<&MaterializedViewResponseSchema>,
    output_id: &str,
    table_schema: Arc<Schema>,
    sql: &str,
    bind_values: &[QueryBindValue],
) -> Result<(), ApiError> {
    let Some(response_schema) = response_schema else {
        return Ok(());
    };

    let batches = query_record_batches_table_with_bindings_and_policy_and_limiter(
        output_id,
        vec![RecordBatch::new_empty(table_schema)],
        sql,
        bind_values,
        QueryPolicy::default(),
        None,
    )
    .await
    .map_err(ApiError::bad_request)?;
    let result_schema = batches.first().map(RecordBatch::schema).ok_or_else(|| {
        ApiError::bad_request(format!(
            "standing runtime view `{view_id}` sql_template produced no result schema during admission"
        ))
    })?;
    if batches
        .iter()
        .any(|batch| batch.schema().as_ref() != result_schema.as_ref())
    {
        return Err(ApiError::bad_request(format!(
            "standing runtime view `{view_id}` sql_template produced inconsistent result schemas during admission"
        )));
    }

    for column in &response_schema.columns {
        validate_response_schema_template_column(view_id, column, result_schema.as_ref())?;
    }
    Ok(())
}

fn validate_response_schema_template_column(
    view_id: &str,
    column: &MaterializedViewResponseColumnSpec,
    result_schema: &Schema,
) -> Result<(), ApiError> {
    let mut source_parts = column.source.split('.');
    let root = source_parts.next().unwrap_or_default();
    let is_nested = source_parts.next().is_some();
    let matching_fields = result_schema
        .fields()
        .iter()
        .filter(|field| field.name() == root)
        .collect::<Vec<_>>();

    let [field] = matching_fields.as_slice() else {
        let detail = if matching_fields.is_empty() {
            "is not a template result column"
        } else {
            "is ambiguous in the template result"
        };
        return Err(ApiError::bad_request(format!(
            "standing runtime view `{view_id}` response schema column `{}` source `{}` {detail}",
            column.name, column.source
        )));
    };

    if is_nested && is_legacy_dynamic_response_source(root, field.data_type()) {
        return Ok(());
    }
    if is_nested {
        return Err(ApiError::bad_request(format!(
            "standing runtime view `{view_id}` response schema column `{}` source `{}` cannot address a nested template result column",
            column.name, column.source
        )));
    }
    if response_type_accepts_template_result(&column.r#type, field.data_type()) {
        return Ok(());
    }
    Err(ApiError::bad_request(format!(
        "standing runtime view `{view_id}` response schema column `{}` type `{}` is incompatible with template result source `{}` type `{:?}`",
        column.name, column.r#type, column.source, field.data_type()
    )))
}

fn is_legacy_dynamic_response_source(root: &str, data_type: &DataType) -> bool {
    matches!(root, "key" | "key_json" | "value" | "value_json")
        && matches!(data_type, DataType::Utf8)
}

fn response_type_accepts_template_result(response_type: &str, data_type: &DataType) -> bool {
    match response_type {
        "string" | "date" | "time" | "timestamp" | "uuid" => {
            matches!(data_type, DataType::Utf8)
        }
        "int64" | "integer" => matches!(
            data_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        ),
        "float64" | "number" => matches!(data_type, DataType::Float32 | DataType::Float64),
        "bool" | "boolean" => matches!(data_type, DataType::Boolean),
        "decimal" => matches!(data_type, DataType::Decimal128(_, _) | DataType::Utf8),
        "binary_hex" => matches!(data_type, DataType::Binary),
        "array" => matches!(data_type, DataType::List(_)),
        "object" => matches!(data_type, DataType::Struct(_) | DataType::Map(_, _)),
        // `json` deliberately accepts every Arrow value supported by the row
        // encoder, because the response coercion preserves JSON values.
        "json" => !matches!(data_type, DataType::Null),
        _ => false,
    }
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
) -> Result<(), ApiError> {
    if request_sql.is_some() {
        if page_request.page_token.is_some() {
            return Err(ApiError::bad_request(format!(
                "cursor pagination is not supported for raw SQL standing runtime view `{view_id}` because SQL must read a full materialized snapshot"
            )));
        }
        return Ok(());
    }
    if api.sql_template.is_none() && (!api.request.is_empty() || !parameters.is_empty()) {
        return Err(ApiError::conflict(format!(
            "standing runtime view `{view_id}` has request parameters but no sql_template"
        )));
    }
    if api.sql_template.is_some() && page_request.page_token.is_some() {
        return Err(ApiError::bad_request(format!(
            "cursor pagination is not supported for templated standing runtime view `{view_id}`"
        )));
    }
    if api.sql_template.is_some() && page_request.max_rows.is_some() {
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
            "request field `{name}` declares unsupported type `variant`: use type `json` for canonical JSON text parameters or compute VARIANT-equivalent values inside a materialized runtime view"
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
            .map(QueryBindValue::Utf8)
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
        "parameter `{name}` uses unsupported SQL template filter `is_variant`: use `is_json` for canonical JSON text parameters or compute VARIANT-equivalent values inside a materialized runtime view"
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
        outcome,
        false,
    )
}

fn view_response(
    spec: &StandingViewSpec,
    spec_hash: String,
    execution_mode: MaterializedViewExecutionMode,
    lifecycle: MaterializedViewLifecycleStatus,
    api: Option<MaterializedViewApiMetadata>,
    outcome: Option<&str>,
    experimental_advanced_view_features: bool,
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
    let coverage =
        materialization_coverage_response(&lifecycle, experimental_advanced_view_features);

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
    experimental_advanced_view_features: bool,
) -> BackfillViewResponse {
    BackfillViewResponse {
        view_id: active.spec.view_id.clone(),
        outcome: outcome.to_string(),
        mode: mode.to_string(),
        lifecycle: active.lifecycle.clone(),
        query_enabled: view_query_availability(&active.lifecycle),
        coverage: materialization_coverage_response(
            &active.lifecycle,
            experimental_advanced_view_features,
        ),
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
                "backfill_required: committed input data exists; run the view backfill API before querying materialized output".to_string(),
            ))
        }
        MaterializedViewExecutionMode::StandingRuntime => MaterializedViewLifecycleStatus::standing_runtime(),
    }
}

fn view_query_availability(lifecycle: &MaterializedViewLifecycleStatus) -> bool {
    lifecycle.admission_status == MaterializedViewAdmissionStatus::Admitted
        && lifecycle.deployment_status == MaterializedViewDeploymentStatus::Running
}

fn materialization_coverage_response(
    lifecycle: &MaterializedViewLifecycleStatus,
    _experimental_advanced_view_features: bool,
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
    }
}

fn view_has_backfill_required_lag(active: &ActiveMaterializedView) -> bool {
    active.lifecycle.admission_status == MaterializedViewAdmissionStatus::Admitted
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

fn aggregate_output_schema(
    view_id: &str,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedViewPlan,
) -> Result<RelationSchema, ApiError> {
    let group_keys = supported_view_plan_group_keys(plan);
    let aggregate_outputs = supported_view_plan_aggregate_outputs(plan);
    let mut columns = Vec::with_capacity(group_keys.len() + aggregate_outputs.len());
    for group_key in &group_keys {
        let (data_type, nullable) = match (&group_key.input_column_id, &group_key.expression) {
            (Some(column_id), None) => {
                let column = catalog
                    .relation_schema
                    .columns
                    .iter()
                    .find(|column| &column.column_id == column_id)
                    .ok_or_else(|| {
                        ApiError::bad_request(format!(
                            "group key column `{column_id}` is missing from catalog"
                        ))
                    })?;
                (sql_type_from_catalog_column(column)?, column.nullable)
            }
            (None, Some(expression)) => projection_expression_output_type(catalog, expression)?,
            _ => {
                return Err(ApiError::bad_request(
                    "group key must bind exactly one input column or expression",
                ));
            }
        };
        columns.push(ColumnSchema {
            name: group_key.output_column_id.clone(),
            data_type,
            nullable,
        });
    }
    for aggregate in &aggregate_outputs {
        columns.push(ColumnSchema {
            name: aggregate.output_column_id.clone(),
            data_type: single_key_aggregate_output_type(catalog, aggregate)?,
            nullable: false,
        });
    }
    let primary_key = group_keys
        .iter()
        .map(|group_key| group_key.output_column_id.clone())
        .collect::<Vec<_>>();
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

fn projection_expression_output_type(
    catalog: &VelorixRelationCatalogV1,
    expression: &SupportedProjectionExpr,
) -> Result<(SqlDataType, bool), ApiError> {
    match expression {
        SupportedProjectionExpr::Column { column_id } => {
            let column = catalog
                .relation_schema
                .columns
                .iter()
                .find(|column| &column.column_id == column_id)
                .ok_or_else(|| {
                    ApiError::bad_request(format!(
                        "group key expression column `{column_id}` is missing from catalog"
                    ))
                })?;
            Ok((sql_type_from_catalog_column(column)?, column.nullable))
        }
        SupportedProjectionExpr::LiteralInt64 { .. }
        | SupportedProjectionExpr::CoalesceInt64 { .. } => Ok((SqlDataType::Int64, false)),
        SupportedProjectionExpr::LiteralUtf8 { .. } => Ok((SqlDataType::Utf8, false)),
        SupportedProjectionExpr::BinaryInt64 { left, right, .. } => {
            let (_, left_nullable) = projection_expression_output_type(catalog, left)?;
            let (_, right_nullable) = projection_expression_output_type(catalog, right)?;
            Ok((SqlDataType::Int64, left_nullable || right_nullable))
        }
        SupportedProjectionExpr::AbsInt64 { expr } => {
            let (_, nullable) = projection_expression_output_type(catalog, expr)?;
            Ok((SqlDataType::Int64, nullable))
        }
        SupportedProjectionExpr::GreatestInt64 { exprs }
        | SupportedProjectionExpr::LeastInt64 { exprs } => {
            let mut nullable = false;
            for expr in exprs {
                nullable |= projection_expression_output_type(catalog, expr)?.1;
            }
            Ok((SqlDataType::Int64, nullable))
        }
        SupportedProjectionExpr::CaseInt64 {
            then_expr,
            else_expr,
            ..
        } => {
            let (_, then_nullable) = projection_expression_output_type(catalog, then_expr)?;
            let (_, else_nullable) = projection_expression_output_type(catalog, else_expr)?;
            Ok((SqlDataType::Int64, then_nullable || else_nullable))
        }
        SupportedProjectionExpr::LengthUtf8 { .. } => Ok((SqlDataType::Int64, false)),
        SupportedProjectionExpr::ConcatUtf8 { .. } => Ok((SqlDataType::Utf8, false)),
        SupportedProjectionExpr::SubstringUtf8 { .. } => Ok((SqlDataType::Utf8, false)),
        SupportedProjectionExpr::TrimUtf8 { .. } => Ok((SqlDataType::Utf8, false)),
    }
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
    let output_key_name = if plan.output_key_column_id.is_empty() {
        key_column.name.clone()
    } else {
        plan.output_key_column_id.clone()
    };
    let columns = vec![
        ColumnSchema {
            name: output_key_name.clone(),
            data_type: sql_type_from_catalog_column(key_column)?,
            nullable: false,
        },
        ColumnSchema {
            name: plan.output_value_column_id.clone(),
            data_type: sql_type_from_catalog_column(value_column)?,
            nullable: value_column.nullable,
        },
    ];
    let primary_key = vec![output_key_name];
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

fn filter_project_output_schema(
    view_id: &str,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedFilterProjectPlan,
) -> Result<RelationSchema, ApiError> {
    let [primary_key_id] = catalog.relation_schema.primary_key_column_ids.as_slice() else {
        return Err(ApiError::bad_request(
            "filter/project view requires exactly one primary key column",
        ));
    };
    let key_column = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| &column.column_id == primary_key_id)
        .ok_or_else(|| ApiError::bad_request("primary key column is missing from catalog"))?;
    let output_key_name = if plan.output_key_column_id.is_empty() {
        key_column.name.clone()
    } else {
        plan.output_key_column_id.clone()
    };
    let mut columns = vec![ColumnSchema {
        name: output_key_name.clone(),
        data_type: sql_type_from_catalog_column(key_column)?,
        nullable: false,
    }];
    for projection in &plan.value_columns {
        let input_column = catalog
            .relation_schema
            .columns
            .iter()
            .find(|column| column.column_id == projection.input_column_id)
            .ok_or_else(|| {
                ApiError::bad_request("filter/project input column is missing from catalog")
            })?;
        let data_type = if projection.expression.is_some() {
            SqlDataType::Int64
        } else {
            sql_type_from_catalog_column(input_column)?
        };
        columns.push(ColumnSchema {
            name: projection.output_column_id.clone(),
            data_type,
            nullable: projection.expression.is_none() && input_column.nullable,
        });
    }
    let primary_key = vec![output_key_name];
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

fn analytic_row_number_output_schema(
    view_id: &str,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedAnalyticRowNumberPlan,
) -> Result<RelationSchema, ApiError> {
    let [primary_key_id] = catalog.relation_schema.primary_key_column_ids.as_slice() else {
        return Err(ApiError::bad_request(
            "analytic row_number view requires exactly one primary key column",
        ));
    };
    let key_column = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| &column.column_id == primary_key_id)
        .ok_or_else(|| ApiError::bad_request("primary key column is missing from catalog"))?;
    let output_key_name = if plan.output_key_column_id.is_empty() {
        key_column.name.clone()
    } else {
        plan.output_key_column_id.clone()
    };
    let columns = vec![
        ColumnSchema {
            name: output_key_name.clone(),
            data_type: sql_type_from_catalog_column(key_column)?,
            nullable: false,
        },
        ColumnSchema {
            name: plan.output_row_number_column_id.clone(),
            data_type: SqlDataType::Int64,
            nullable: false,
        },
    ];
    let primary_key = vec![output_key_name];
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
    let output_key_name = if plan.output_key_column_id.is_empty() {
        key_column.name.clone()
    } else {
        plan.output_key_column_id.clone()
    };
    let mut columns = vec![
        ColumnSchema {
            name: output_key_name.clone(),
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
        output_key_name,
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

fn three_input_join_count_output_schema(
    view_id: &str,
    catalogs: &[VelorixRelationCatalogV1],
    plan: &SupportedThreeInputInnerJoinCountPlanV1,
) -> Result<RelationSchema, ApiError> {
    let [root, _, _] = catalogs else {
        return Err(ApiError::bad_request(
            "three-input JOIN requires exactly three catalogs",
        ));
    };
    if plan.output_key_column_ids.len() != plan.root_primary_key_column_ids.len() {
        return Err(ApiError::bad_request(
            "three-input JOIN output key mapping is invalid",
        ));
    }
    let mut columns = plan
        .root_primary_key_column_ids
        .iter()
        .zip(plan.output_key_column_ids.iter())
        .map(|(column_id, output_name)| {
            let column = root
                .relation_schema
                .columns
                .iter()
                .find(|column| &column.column_id == column_id)
                .ok_or_else(|| ApiError::bad_request("three-input JOIN root PK is missing"))?;
            Ok(ColumnSchema {
                name: output_name.clone(),
                data_type: sql_type_from_catalog_column(column)?,
                nullable: false,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    columns.push(ColumnSchema {
        name: plan.count_output_column_id.clone(),
        data_type: SqlDataType::Int64,
        nullable: false,
    });
    let primary_key = plan.output_key_column_ids.clone();
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
    let self_join = supported_join_view_plan_is_self_join(plan);
    if (self_join && catalogs.len() != 1) || (!self_join && catalogs.len() != 2) {
        return Err(ApiError::bad_request(
            "join sum/count view requires one self-joined or two distinct input relations",
        ));
    }
    let left_catalog = catalogs
        .iter()
        .find(|catalog| catalog.relation_schema.relation_id == plan.left_input_relation_id)
        .ok_or_else(|| ApiError::bad_request("join left relation is missing from catalog"))?;
    let right_catalog = catalogs
        .iter()
        .find(|catalog| catalog.relation_schema.relation_id == plan.right_input_relation_id)
        .ok_or_else(|| ApiError::bad_request("join right relation is missing from catalog"))?;
    let aggregate_outputs = supported_join_view_plan_aggregate_outputs(plan);
    let mut columns = Vec::new();
    let mut primary_key = Vec::new();
    if !supported_join_view_plan_is_singleton(plan) {
        let key_catalog = catalogs
            .iter()
            .find(|catalog| catalog.relation_schema.relation_id == plan.group_key_relation_id)
            .ok_or_else(|| {
                ApiError::bad_request("join group key relation is missing from catalog")
            })?;
        let key_column = key_catalog
            .relation_schema
            .columns
            .iter()
            .find(|column| column.column_id == plan.group_key_column_id)
            .ok_or_else(|| {
                ApiError::bad_request("join group key column is missing from catalog")
            })?;
        let output_key_name = if plan.output_key_column_id.is_empty() {
            key_column.name.clone()
        } else {
            plan.output_key_column_id.clone()
        };
        columns.push(ColumnSchema {
            name: output_key_name.clone(),
            data_type: sql_type_from_catalog_column(key_column)?,
            nullable: false,
        });
        primary_key.push(output_key_name);
    }
    for aggregate in aggregate_outputs {
        let data_type = join_aggregate_output_type(left_catalog, right_catalog, plan, &aggregate)?;
        columns.push(ColumnSchema {
            name: aggregate.output_column_id,
            data_type,
            nullable: false,
        });
    }
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

fn join_aggregate_output_type(
    left_catalog: &VelorixRelationCatalogV1,
    right_catalog: &VelorixRelationCatalogV1,
    plan: &SupportedJoinViewPlan,
    aggregate: &SupportedAggregateOutput,
) -> Result<SqlDataType, ApiError> {
    match aggregate.function {
        LogicalPlanAggregateFunctionV1::Count | LogicalPlanAggregateFunctionV1::CountDistinct => {
            if aggregate.input_column_id.is_some() {
                let _ = join_value_aggregate_column(
                    left_catalog,
                    right_catalog,
                    plan,
                    aggregate,
                    "count",
                )?;
            }
            Ok(SqlDataType::Int64)
        }
        LogicalPlanAggregateFunctionV1::Sum => {
            let column =
                join_value_aggregate_column(left_catalog, right_catalog, plan, aggregate, "sum")?;
            join_sum_min_max_output_type(column)
        }
        LogicalPlanAggregateFunctionV1::Min | LogicalPlanAggregateFunctionV1::Max => {
            let column = join_value_aggregate_column(
                left_catalog,
                right_catalog,
                plan,
                aggregate,
                "min/max",
            )?;
            join_sum_min_max_output_type(column)
        }
        LogicalPlanAggregateFunctionV1::Avg => {
            let column =
                join_value_aggregate_column(left_catalog, right_catalog, plan, aggregate, "avg")?;
            match &column.physical_arrow_type {
                ArrowPhysicalTypeV1::Int64 => Ok(SqlDataType::Float64),
                _ => Err(ApiError::bad_request(format!(
                    "join aggregate avg column `{}` must be Int64",
                    column.name
                ))),
            }
        }
    }
}

fn join_value_aggregate_column<'a>(
    left_catalog: &'a VelorixRelationCatalogV1,
    right_catalog: &'a VelorixRelationCatalogV1,
    plan: &SupportedJoinViewPlan,
    aggregate: &SupportedAggregateOutput,
    function_name: &str,
) -> Result<&'a RelationColumnV1, ApiError> {
    let column_id = aggregate.input_column_id.as_deref().ok_or_else(|| {
        ApiError::bad_request(format!(
            "join aggregate {function_name} input column is missing"
        ))
    })?;
    let side = aggregate
        .input_relation_side
        .unwrap_or(SupportedAggregateInputRelationSide::Left);
    if side == SupportedAggregateInputRelationSide::Left && column_id != plan.sum_value_column_id {
        return Err(ApiError::bad_request(format!(
            "join aggregate {function_name} currently supports the left input value column `{}`",
            plan.sum_value_column_id
        )));
    }
    let catalog = match side {
        SupportedAggregateInputRelationSide::Left => left_catalog,
        SupportedAggregateInputRelationSide::Right => right_catalog,
    };
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == column_id)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "join aggregate {function_name} value column is missing from input catalog"
            ))
        })
}

fn join_sum_min_max_output_type(column: &RelationColumnV1) -> Result<SqlDataType, ApiError> {
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 => Ok(SqlDataType::Int64),
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => Ok(SqlDataType::Decimal {
            precision: *precision,
            scale: *scale,
        }),
        _ => Err(ApiError::bad_request(format!(
            "join aggregate value column `{}` must be Int64 or Decimal128",
            column.name
        ))),
    }
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
    if supported_join_view_plan_is_self_join(plan) {
        let [catalog] = catalogs else {
            return Err(ApiError::bad_request(
                "self-join view requires exactly one physical input relation",
            ));
        };
        return if catalog.relation_schema.relation_id == plan.left_input_relation_id
            && plan.left_input_relation_id == plan.right_input_relation_id
        {
            Ok(())
        } else {
            Err(ApiError::bad_request(
                "input_relations must match the SQL self-JOIN input",
            ))
        };
    }
    let [left, right] = catalogs else {
        return Err(ApiError::bad_request(
            "join sum/count view requires exactly two input relations",
        ));
    };
    let requested = BTreeSet::from([
        left.relation_schema.relation_id.as_str(),
        right.relation_schema.relation_id.as_str(),
    ]);
    let planned = BTreeSet::from([
        plan.left_input_relation_id.as_str(),
        plan.right_input_relation_id.as_str(),
    ]);
    if requested == planned {
        Ok(())
    } else {
        Err(ApiError::bad_request(
            "input_relations must match the two SQL JOIN inputs",
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
        LogicalPlanAggregateFunctionV1::Count | LogicalPlanAggregateFunctionV1::CountDistinct => {
            Ok(SqlDataType::Int64)
        }
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
    details: Option<Value>,
}

impl ApiError {
    fn bad_request(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: error.to_string(),
            details: None,
        }
    }

    fn unauthorized(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: error.to_string(),
            details: None,
        }
    }

    fn conflict(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: error.to_string(),
            details: None,
        }
    }

    fn payload_too_large(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: error.to_string(),
            details: None,
        }
    }

    fn service_unavailable(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: error.to_string(),
            details: None,
        }
    }

    fn service_unavailable_with_details(error: impl std::fmt::Display, details: Value) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: error.to_string(),
            details: Some(details),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
            details: None,
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
        | MetaStoreError::DuplicateSourceCutRelation { .. }
        | MetaStoreError::OverlappingSourceCutRange { .. }
        | MetaStoreError::UnexpectedOutcome(_) => ApiError::bad_request(error),
        MetaStoreError::RelationCatalogConflict { .. }
        | MetaStoreError::NonMonotonicCheckpointEpoch { .. }
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
        let mut body = json!({
            "error": self.message,
        });
        if let Some(details) = self.details {
            body["details"] = details;
        }
        (self.status, Json(body)).into_response()
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
    max_standing_runtime_output_delta_records: usize,
    max_standing_runtime_state_payload_bytes: usize,
    output_compaction_interval_epochs: u64,
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
        let max_standing_runtime_output_delta_records = parse_positive_usize_env(
            "VELORIX_STANDING_RUNTIME_MAX_OUTPUT_DELTA_RECORDS",
            DEFAULT_MAX_STANDING_RUNTIME_OUTPUT_DELTA_RECORDS,
        )?;
        let max_standing_runtime_state_payload_bytes = parse_positive_usize_env(
            "VELORIX_STANDING_RUNTIME_MAX_STATE_PAYLOAD_BYTES",
            DEFAULT_MAX_STANDING_RUNTIME_STATE_PAYLOAD_BYTES,
        )?;
        let output_compaction_interval_epochs =
            parse_u64_env("VELORIX_OUTPUT_COMPACTION_INTERVAL_EPOCHS", 0)?;
        if output_compaction_interval_epochs != 0 {
            return Err(anyhow!(
                "VELORIX_OUTPUT_COMPACTION_INTERVAL_EPOCHS is experimental and disabled for the public 1.0 API"
            ));
        }
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
            max_standing_runtime_output_delta_records,
            max_standing_runtime_state_payload_bytes,
            output_compaction_interval_epochs,
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

fn parse_u64_env(name: &str, default: u64) -> anyhow::Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .with_context(|| format!("invalid {name} `{}`", value.trim())),
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
mod tests;
