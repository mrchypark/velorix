use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::{
    array::{
        ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
        StringArray, TimestampNanosecondArray,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use serde_json::{Number as JsonNumber, Value};
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::{
        AggregateValueMode, EngineCheckpointPayload, IncrementalEngine, LogicalEpoch,
        PrototypeIncrementalEngine,
    },
    operator::{KeyedEquiJoin, OperatorError},
    relation::{
        arrow_record_batches_to_key_latest_by_delta_batch,
        arrow_record_batches_to_key_value_delta_batch, ArrowPhysicalTypeV1, RelationSemanticRoleV1,
        VelorixRelationCatalogV1,
    },
    standing_program::{
        DurableStateRoot, EpochCommit, EpochIdempotencyKey, InputEventTimeFrontier,
        MaterializedViewPage, RelationFrontier, RelationInputBatch, RuntimeCheckpoint,
        RuntimeCheckpointStatePayload, ScopedViewId, SnapshotPageRequest, StandingProgramIdentity,
        StandingProgramRuntime, StandingProgramRuntimeError, ViewFrontier, ViewOutputBatch,
        ViewOutputDelta,
    },
    view_contract::{
        catalog_input_relation_schema, stable_bytes_hash, RelationSchema, SqlDataType,
        SqlStructField,
    },
    view_plan::{
        lower_supported_join_view_sql_to_logical_plan,
        lower_supported_latest_by_key_sql_to_logical_plan, lower_supported_sql_to_logical_plan,
        lower_supported_tumbling_window_sql_to_logical_plan,
        lower_supported_view_sql_to_logical_plan, supported_view_plan_aggregate_outputs,
        validate_logical_view_plan, validate_supported_join_view_sql,
        validate_supported_latest_by_key_sql, validate_supported_tumbling_window_sql,
        validate_supported_view_sql, LogicalPlanAggregateFunctionV1, PredicateOp, RowPredicate,
        SupportedAggregateOutput, SupportedJoinViewPlan, SupportedLatestByKeyPlan,
        SupportedTumblingWindowPlan, SupportedViewPlan, VelorixLogicalViewExecutionV1,
        VelorixLogicalViewPlanV1,
    },
};

pub const CRATE_NAME: &str = "velorix_materialized_view_runtime";

const CHECKPOINT_PAYLOAD_SCHEMA_VERSION: u32 = 1;
const JOIN_RUNTIME_KIND: &str = "two_input_join_sum_count";
const LATEST_BY_KEY_RUNTIME_KIND: &str = "latest_by_key";
const TUMBLING_WINDOW_RUNTIME_KIND: &str = "tumbling_event_time_aggregate";

type JoinOperator =
    KeyedEquiJoin<fn(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError>>;

pub fn create_standing_runtime(
    identity: &StandingProgramIdentity,
    _catalog: &VelorixRelationCatalogV1,
    _input_schemas: &[RelationSchema],
    _output_schemas: &[RelationSchema],
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    let _ = identity;
    Err("materialized view runtime requires admitted SQL and logical plan metadata".to_string())
}

pub fn create_standing_runtime_with_sql(
    identity: &StandingProgramIdentity,
    catalog: &VelorixRelationCatalogV1,
    sql: &str,
    input_schemas: &[RelationSchema],
    output_schemas: &[RelationSchema],
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    create_standing_runtime_with_sql_and_catalogs(
        identity,
        std::slice::from_ref(catalog),
        sql,
        input_schemas,
        output_schemas,
    )
}

pub fn create_standing_runtime_with_sql_and_catalogs(
    identity: &StandingProgramIdentity,
    catalogs: &[VelorixRelationCatalogV1],
    sql: &str,
    input_schemas: &[RelationSchema],
    output_schemas: &[RelationSchema],
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    let output_schema =
        only_schema(output_schemas, "output_schemas").map_err(|error| error.to_string())?;
    let logical_plan = lower_supported_sql_to_logical_plan(sql, catalogs, &output_schema)
        .map_err(|error| error.to_string())?;
    create_standing_runtime_with_logical_plan_and_catalogs(
        identity,
        catalogs,
        logical_plan,
        input_schemas,
        output_schemas,
    )
}

pub fn create_standing_runtime_with_logical_plan_and_catalogs(
    identity: &StandingProgramIdentity,
    catalogs: &[VelorixRelationCatalogV1],
    logical_plan: VelorixLogicalViewPlanV1,
    input_schemas: &[RelationSchema],
    output_schemas: &[RelationSchema],
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    let output_schema =
        only_schema(output_schemas, "output_schemas").map_err(|error| error.to_string())?;
    let sql = logical_plan.view_sql.clone();
    match &logical_plan.execution {
        VelorixLogicalViewExecutionV1::SingleKeySumCount { plan } => {
            let [catalog] = catalogs else {
                return Err(
                    "single-key sum/count runtime requires exactly one relation catalog"
                        .to_string(),
                );
            };
            SingleKeySumCountRuntime::new_with_logical_plan(
                identity.clone(),
                catalog.clone(),
                only_schema(input_schemas, "input_schemas").map_err(|error| error.to_string())?,
                output_schema.clone(),
                sql,
                plan.clone(),
                logical_plan,
            )
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string())
        }
        VelorixLogicalViewExecutionV1::LatestByKey { plan } => {
            let [catalog] = catalogs else {
                return Err(
                    "latest-by-key runtime requires exactly one relation catalog".to_string(),
                );
            };
            LatestByKeyRuntime::new_with_logical_plan(
                identity.clone(),
                catalog.clone(),
                only_schema(input_schemas, "input_schemas").map_err(|error| error.to_string())?,
                output_schema.clone(),
                sql,
                plan.clone(),
                logical_plan,
            )
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string())
        }
        VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan } => {
            TwoInputJoinRuntime::new_with_logical_plan(
                identity.clone(),
                catalogs.to_vec(),
                input_schemas.to_vec(),
                output_schema.clone(),
                sql,
                plan.clone(),
                logical_plan,
            )
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string())
        }
        VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan } => {
            let [catalog] = catalogs else {
                return Err(
                    "tumbling event-time runtime requires exactly one relation catalog".to_string(),
                );
            };
            TumblingEventTimeAggregateRuntime::new_with_logical_plan(
                identity.clone(),
                catalog.clone(),
                only_schema(input_schemas, "input_schemas").map_err(|error| error.to_string())?,
                output_schema.clone(),
                sql,
                plan.clone(),
                logical_plan,
            )
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string())
        }
    }
}

pub fn materialized_delta_to_page(
    output_schema: &RelationSchema,
    published_output: &DeltaBatch,
    scoped_view: ScopedViewId,
    logical_epoch: u64,
    page: SnapshotPageRequest,
    aggregate_outputs: Option<&[SupportedAggregateOutput]>,
) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
    let (batch, next_page_token) = if aggregate_outputs.is_none()
        && !looks_like_default_sum_count_output(output_schema)
    {
        materialized_generic_delta_page_batch(output_schema, published_output, logical_epoch, page)?
    } else if looks_like_tumbling_window_output(output_schema) {
        let Some(aggregate_outputs) = aggregate_outputs else {
            return Err(invalid_runtime_state());
        };
        materialized_tumbling_delta_page_batch(
            output_schema,
            published_output,
            aggregate_outputs,
            logical_epoch,
            page,
        )?
    } else {
        materialized_delta_page_batch(
            output_schema,
            published_output,
            logical_epoch,
            page,
            aggregate_outputs,
        )?
    };
    Ok(MaterializedViewPage {
        view: scoped_view,
        logical_epoch,
        schema_fingerprint: output_schema.schema_fingerprint.clone(),
        batches: vec![batch],
        next_page_token,
    })
}

fn looks_like_default_sum_count_output(output_schema: &RelationSchema) -> bool {
    matches!(
        output_schema.columns.as_slice(),
        [_, sum, count] if sum.name == "sum" && count.name == "count"
    )
}

fn looks_like_tumbling_window_output(output_schema: &RelationSchema) -> bool {
    matches!(
        output_schema.columns.as_slice(),
        [_, window_start, window_end, ..]
            if window_start.name == "window_start" && window_end.name == "window_end"
    )
}

pub fn restore_standing_runtime(
    checkpoint: RuntimeCheckpoint,
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    if checkpoint_has_tumbling_window_payload(&checkpoint) {
        return TumblingEventTimeAggregateRuntime::restore(checkpoint)
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string());
    }
    if checkpoint_has_latest_by_key_payload(&checkpoint) {
        return LatestByKeyRuntime::restore(checkpoint)
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string());
    }
    if checkpoint_has_join_payload(&checkpoint) {
        return TwoInputJoinRuntime::restore(checkpoint)
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string());
    }
    SingleKeySumCountRuntime::restore(checkpoint)
        .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
        .map_err(|error| error.to_string())
}

#[derive(Clone, Debug)]
pub struct SingleKeySumCountRuntime {
    identity: StandingProgramIdentity,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedViewPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    engine: PrototypeIncrementalEngine,
    published_output: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GenericCheckpointPayload {
    schema_version: u32,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedViewPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    input_frontiers: Vec<RelationFrontier>,
    #[serde(default)]
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    engine: EngineCheckpointPayload,
    published_output: DeltaBatch,
    applied_epochs: Vec<GenericAppliedEpoch>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GenericAppliedEpoch {
    idempotency_key: String,
    logical_epoch: LogicalEpoch,
}

pub struct LatestByKeyRuntime {
    identity: StandingProgramIdentity,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedLatestByKeyPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    latest_state: LatestByKeyState,
    published_output: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    logical_epoch: LogicalEpoch,
}

#[derive(Clone, Debug, Default)]
struct LatestByKeyState {
    rows: BTreeMap<String, LatestKeyRows>,
}

#[derive(Clone, Debug)]
struct LatestKeyRows {
    key: Value,
    values: BTreeMap<i128, BTreeMap<String, LatestValueCount>>,
}

#[derive(Clone, Debug)]
struct LatestValueCount {
    value: Value,
    weight: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LatestByKeyCheckpointPayload {
    schema_version: u32,
    runtime_kind: String,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedLatestByKeyPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    input_frontiers: Vec<RelationFrontier>,
    #[serde(default)]
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    latest_state: Vec<LatestByKeyCheckpointRow>,
    published_output: Option<DeltaBatch>,
    applied_epochs: Vec<GenericAppliedEpoch>,
    logical_epoch: LogicalEpoch,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LatestByKeyCheckpointRow {
    key: Value,
    ordering: i128,
    value: Value,
    weight: i64,
}

pub struct TumblingEventTimeAggregateRuntime {
    identity: StandingProgramIdentity,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedTumblingWindowPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    state: TumblingWindowState,
    published_output: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
    logical_epoch: LogicalEpoch,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TumblingWindowState {
    rows: BTreeMap<String, TumblingWindowStateRow>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TumblingWindowStateRow {
    group_key: Value,
    window_start_ns: i64,
    window_end_ns: i64,
    #[serde(default)]
    net_count: i64,
    #[serde(default)]
    avg_sums: BTreeMap<String, i64>,
    #[serde(default)]
    avg_counts: BTreeMap<String, i64>,
    #[serde(default)]
    extrema_values: BTreeMap<String, BTreeMap<i64, i64>>,
    values: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TumblingWindowCheckpointPayload {
    schema_version: u32,
    runtime_kind: String,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedTumblingWindowPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    input_frontiers: Vec<RelationFrontier>,
    #[serde(default)]
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    state: TumblingWindowState,
    published_output: DeltaBatch,
    applied_epochs: Vec<GenericAppliedEpoch>,
    logical_epoch: LogicalEpoch,
}

impl TumblingEventTimeAggregateRuntime {
    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedTumblingWindowPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_runtime_package(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_tumbling_window_view_plan",
            }
        })?;
        validate_tumbling_supported_schemas(&catalog, &input_schema, &output_schema, &plan)?;
        let compiled_plan = validate_supported_tumbling_window_sql(view_sql.as_str(), &catalog)
            .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "tumbling_window_view_plan",
            })?;
        if compiled_plan != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "tumbling_window_view_plan",
            });
        }
        let compiled_logical_plan = lower_supported_tumbling_window_sql_to_logical_plan(
            view_sql.as_str(),
            &catalog,
            &output_schema,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "logical_tumbling_window_view_plan",
        })?;
        if compiled_logical_plan != logical_plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_tumbling_window_view_plan",
            });
        }
        Ok(Self {
            identity,
            catalog,
            input_schema,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            state: TumblingWindowState::default(),
            published_output: DeltaBatch::default(),
            input_frontiers: Vec::new(),
            input_event_time_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
            logical_epoch: 0,
        })
    }

    fn output_schema_fingerprint(&self) -> String {
        self.output_schema.schema_fingerprint.clone()
    }

    fn materialized_batch(&self) -> Result<RecordBatch, StandingProgramRuntimeError> {
        materialized_tumbling_delta_to_record_batch(
            &self.output_schema,
            &self.published_output,
            &self.plan.aggregate_outputs,
        )
    }

    fn materialized_page_batch(
        &self,
        page: SnapshotPageRequest,
    ) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
        materialized_tumbling_delta_page_batch(
            &self.output_schema,
            &self.published_output,
            &self.plan.aggregate_outputs,
            self.logical_epoch,
            page,
        )
    }

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
        let payload = TumblingWindowCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: TUMBLING_WINDOW_RUNTIME_KIND.to_string(),
            catalog: self.catalog.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            state: self.state.clone(),
            published_output: self.published_output.clone(),
            applied_epochs: self
                .applied_epochs
                .iter()
                .map(|(idempotency_key, logical_epoch)| GenericAppliedEpoch {
                    idempotency_key: idempotency_key.clone(),
                    logical_epoch: *logical_epoch,
                })
                .collect(),
            logical_epoch: self.logical_epoch,
        };
        serde_json::to_string(&payload).map_err(|_| invalid_checkpoint())
    }

    fn restore_payload(
        checkpoint: &RuntimeCheckpoint,
    ) -> Result<TumblingWindowCheckpointPayload, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_checkpoint());
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: TumblingWindowCheckpointPayload =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != TUMBLING_WINDOW_RUNTIME_KIND
        {
            return Err(invalid_checkpoint());
        }
        validate_tumbling_supported_schemas(
            &payload.catalog,
            &payload.input_schema,
            &payload.output_schema,
            &payload.plan,
        )?;
        Ok(payload)
    }
}

impl StandingProgramRuntime for TumblingEventTimeAggregateRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        vec![self.input_schema.clone()]
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        vec![self.output_schema.clone()]
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        self.logical_epoch
    }

    fn apply_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        let idempotency_key_text = idempotency_key.as_str().to_string();
        if let Some(applied_epoch) = self.applied_epochs.get(&idempotency_key_text) {
            if *applied_epoch == logical_epoch {
                return Ok(EpochCommit {
                    logical_epoch,
                    idempotency_key,
                    input_frontiers: self.input_frontiers.clone(),
                    input_event_time_frontiers: self.input_event_time_frontiers.clone(),
                    output_deltas: Vec::new(),
                    output_batches: vec![ViewOutputBatch {
                        view_id: self.identity.view_ids[0].clone(),
                        schema_fingerprint: self.output_schema_fingerprint(),
                        batches: vec![self.materialized_batch()?],
                    }],
                });
            }
            return Err(StandingProgramRuntimeError::IdempotencyKeyConflict {
                idempotency_key: idempotency_key_text,
                first_epoch: *applied_epoch,
                attempted_epoch: logical_epoch,
            });
        }
        if logical_epoch <= self.logical_epoch {
            return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                current: self.logical_epoch,
                attempted: logical_epoch,
            });
        }

        let mut next_frontiers = self.input_frontiers.clone();
        let mut next_event_time_frontiers = self.input_event_time_frontiers.clone();
        for input in input_changes {
            validate_input_matches_schema(
                &input,
                &self.input_schema,
                "tumbling_event_time_input_relation",
            )?;
            apply_tumbling_input(
                &mut self.state,
                &self.catalog,
                &self.plan,
                &self.input_event_time_frontiers,
                &input,
            )?;
            advance_input_frontier(&mut next_frontiers, &input)?;
            advance_input_event_time_frontier(&mut next_event_time_frontiers, &input)?;
        }
        let previous_output = self.published_output.clone();
        self.published_output = self.state.closed_delta(
            &self.plan,
            min_event_time_watermark(&next_event_time_frontiers),
        );
        let output_delta = previous_output
            .inverse()
            .map_err(|_| invalid_runtime_state())?
            .combine(&self.published_output);
        self.input_frontiers = next_frontiers.clone();
        self.input_event_time_frontiers = next_event_time_frontiers.clone();
        self.applied_epochs
            .insert(idempotency_key_text, logical_epoch);
        self.logical_epoch = logical_epoch;

        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers: next_frontiers,
            input_event_time_frontiers: next_event_time_frontiers,
            output_deltas: vec![ViewOutputDelta {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                delta: output_delta,
            }],
            output_batches: vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![self.materialized_batch()?],
            }],
        })
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        if view.tenant_id != self.identity.tenant_id
            || view.program_id != self.identity.program_id
            || !self
                .identity
                .view_ids
                .iter()
                .any(|view_id| view_id == &view.view_id)
        {
            return Err(StandingProgramRuntimeError::UnknownView {
                view_id: view.view_id,
            });
        }

        let (batch, next_page_token) = self.materialized_page_batch(page)?;
        Ok(MaterializedViewPage {
            view,
            logical_epoch: self.logical_epoch,
            schema_fingerprint: self.output_schema_fingerprint(),
            batches: vec![batch],
            next_page_token,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        let payload = self.checkpoint_payload()?;
        let content_hash = stable_bytes_hash(payload.as_bytes());
        Ok(RuntimeCheckpoint {
            identity: self.identity.clone(),
            logical_epoch: self.logical_epoch,
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            output_frontiers: self
                .identity
                .view_ids
                .iter()
                .map(|view_id| ViewFrontier {
                    view_id: view_id.clone(),
                    committed_epoch: self.logical_epoch,
                })
                .collect(),
            checkpoint_codec_identity: self.identity.checkpoint_codec_identity.clone(),
            state_root: DurableStateRoot {
                object_key: format!(
                    "v1/state/materialized-view-runtime/{}/checkpoint",
                    self.identity.program_id
                ),
                content_hash,
            },
            state_payload: Some(RuntimeCheckpointStatePayload {
                codec_identity: self.identity.checkpoint_codec_identity.clone(),
                payload,
            }),
            output_manifest_refs: Vec::new(),
            owner_epoch: None,
        })
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        checkpoint.validate_identity(&checkpoint.identity)?;
        let payload = Self::restore_payload(&checkpoint)?;
        if payload.logical_epoch != checkpoint.logical_epoch
            || payload.input_frontiers != checkpoint.input_frontiers
            || payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers
        {
            return Err(invalid_checkpoint());
        }
        validate_checkpoint_frontiers_for_schemas(
            &checkpoint,
            std::slice::from_ref(&payload.input_schema),
        )?;
        validate_input_event_time_frontiers_for_catalogs(
            &checkpoint,
            std::slice::from_ref(&payload.catalog),
        )?;
        validate_view_sql_hash(&checkpoint.identity, payload.view_sql.as_str())?;
        let compiled_plan =
            validate_supported_tumbling_window_sql(payload.view_sql.as_str(), &payload.catalog)
                .map_err(|_| invalid_checkpoint())?;
        if compiled_plan != payload.plan {
            return Err(invalid_checkpoint());
        }
        validate_logical_view_plan(&payload.logical_plan).map_err(|_| invalid_checkpoint())?;
        let compiled_logical_plan = lower_supported_tumbling_window_sql_to_logical_plan(
            payload.view_sql.as_str(),
            &payload.catalog,
            &payload.output_schema,
        )
        .map_err(|_| invalid_checkpoint())?;
        if compiled_logical_plan != payload.logical_plan {
            return Err(invalid_checkpoint());
        }
        validate_published_output(&payload.published_output)?;
        Ok(Self {
            identity: checkpoint.identity,
            catalog: payload.catalog,
            input_schema: payload.input_schema,
            output_schema: payload.output_schema,
            view_sql: payload.view_sql,
            plan: payload.plan,
            logical_plan: payload.logical_plan,
            state: payload.state,
            published_output: payload.published_output,
            input_frontiers: checkpoint.input_frontiers,
            input_event_time_frontiers: checkpoint.input_event_time_frontiers,
            applied_epochs: payload
                .applied_epochs
                .into_iter()
                .map(|entry| (entry.idempotency_key, entry.logical_epoch))
                .collect(),
            logical_epoch: checkpoint.logical_epoch,
        })
    }
}

impl LatestByKeyRuntime {
    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedLatestByKeyPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_runtime_package(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_latest_by_key_view_plan",
            }
        })?;
        validate_latest_supported_schemas(&catalog, &input_schema, &output_schema, &plan)?;
        let compiled_plan = validate_supported_latest_by_key_sql(view_sql.as_str(), &catalog)
            .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "latest_by_key_view_plan",
            })?;
        if compiled_plan != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "latest_by_key_view_plan",
            });
        }
        let compiled_logical_plan = lower_supported_latest_by_key_sql_to_logical_plan(
            view_sql.as_str(),
            &catalog,
            &output_schema,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "logical_latest_by_key_view_plan",
        })?;
        if compiled_logical_plan != logical_plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_latest_by_key_view_plan",
            });
        }
        Ok(Self {
            identity,
            catalog,
            input_schema,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            latest_state: LatestByKeyState::default(),
            published_output: DeltaBatch::default(),
            input_frontiers: Vec::new(),
            input_event_time_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
            logical_epoch: 0,
        })
    }

    fn output_schema_fingerprint(&self) -> String {
        self.output_schema.schema_fingerprint.clone()
    }

    fn materialized_batch(&self) -> Result<RecordBatch, StandingProgramRuntimeError> {
        materialized_generic_delta_to_record_batch(&self.output_schema, &self.published_output)
    }

    fn materialized_page_batch(
        &self,
        page: SnapshotPageRequest,
    ) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
        materialized_generic_delta_page_batch(
            &self.output_schema,
            &self.published_output,
            self.logical_epoch,
            page,
        )
    }

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
        let payload = LatestByKeyCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: LATEST_BY_KEY_RUNTIME_KIND.to_string(),
            catalog: self.catalog.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            latest_state: self.latest_state.to_checkpoint_rows(),
            published_output: Some(self.published_output.clone()),
            applied_epochs: self
                .applied_epochs
                .iter()
                .map(|(idempotency_key, logical_epoch)| GenericAppliedEpoch {
                    idempotency_key: idempotency_key.clone(),
                    logical_epoch: *logical_epoch,
                })
                .collect(),
            logical_epoch: self.logical_epoch,
        };
        serde_json::to_string(&payload).map_err(|_| invalid_checkpoint())
    }

    fn restore_payload(
        checkpoint: &RuntimeCheckpoint,
    ) -> Result<LatestByKeyCheckpointPayload, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_checkpoint());
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: LatestByKeyCheckpointPayload =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != LATEST_BY_KEY_RUNTIME_KIND
        {
            return Err(invalid_checkpoint());
        }
        validate_latest_supported_schemas(
            &payload.catalog,
            &payload.input_schema,
            &payload.output_schema,
            &payload.plan,
        )?;
        Ok(payload)
    }
}

impl StandingProgramRuntime for LatestByKeyRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        vec![self.input_schema.clone()]
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        vec![self.output_schema.clone()]
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        self.logical_epoch
    }

    fn apply_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        let idempotency_key_text = idempotency_key.as_str().to_string();
        if let Some(applied_epoch) = self.applied_epochs.get(&idempotency_key_text) {
            if *applied_epoch == logical_epoch {
                return Ok(EpochCommit {
                    logical_epoch,
                    idempotency_key,
                    input_frontiers: self.input_frontiers.clone(),
                    input_event_time_frontiers: self.input_event_time_frontiers.clone(),
                    output_deltas: Vec::new(),
                    output_batches: vec![ViewOutputBatch {
                        view_id: self.identity.view_ids[0].clone(),
                        schema_fingerprint: self.output_schema_fingerprint(),
                        batches: vec![self.materialized_batch()?],
                    }],
                });
            }
            return Err(StandingProgramRuntimeError::IdempotencyKeyConflict {
                idempotency_key: idempotency_key_text,
                first_epoch: *applied_epoch,
                attempted_epoch: logical_epoch,
            });
        }
        let mut executor = LogicalPlanExecutor::LatestByKey {
            catalog: &self.catalog,
            input_schema: &self.input_schema,
            plan: &self.plan,
            latest_state: &mut self.latest_state,
            current_logical_epoch: self.logical_epoch,
        };
        let executor_commit = executor.apply_epoch(
            logical_epoch,
            &self.input_frontiers,
            &self.input_event_time_frontiers,
            input_changes,
        )?;
        self.published_output =
            apply_published_output_delta(&self.published_output, &executor_commit.output_delta)?;
        self.input_frontiers = executor_commit.input_frontiers.clone();
        self.input_event_time_frontiers = executor_commit.input_event_time_frontiers.clone();
        self.applied_epochs
            .insert(idempotency_key_text, logical_epoch);
        self.logical_epoch = logical_epoch;

        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers: executor_commit.input_frontiers,
            input_event_time_frontiers: executor_commit.input_event_time_frontiers,
            output_deltas: vec![ViewOutputDelta {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                delta: executor_commit.output_delta,
            }],
            output_batches: vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![self.materialized_batch()?],
            }],
        })
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        if view.tenant_id != self.identity.tenant_id
            || view.program_id != self.identity.program_id
            || !self
                .identity
                .view_ids
                .iter()
                .any(|view_id| view_id == &view.view_id)
        {
            return Err(StandingProgramRuntimeError::UnknownView {
                view_id: view.view_id,
            });
        }

        let (batch, next_page_token) = self.materialized_page_batch(page)?;
        Ok(MaterializedViewPage {
            view,
            logical_epoch: self.logical_epoch,
            schema_fingerprint: self.output_schema_fingerprint(),
            batches: vec![batch],
            next_page_token,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        let payload = self.checkpoint_payload()?;
        let content_hash = stable_bytes_hash(payload.as_bytes());
        Ok(RuntimeCheckpoint {
            identity: self.identity.clone(),
            logical_epoch: self.logical_epoch,
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            output_frontiers: self
                .identity
                .view_ids
                .iter()
                .map(|view_id| ViewFrontier {
                    view_id: view_id.clone(),
                    committed_epoch: self.logical_epoch,
                })
                .collect(),
            checkpoint_codec_identity: self.identity.checkpoint_codec_identity.clone(),
            state_root: DurableStateRoot {
                object_key: format!(
                    "v1/state/materialized-view-runtime/{}/checkpoint",
                    self.identity.program_id
                ),
                content_hash,
            },
            state_payload: Some(RuntimeCheckpointStatePayload {
                codec_identity: self.identity.checkpoint_codec_identity.clone(),
                payload,
            }),
            output_manifest_refs: Vec::new(),
            owner_epoch: None,
        })
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        checkpoint.validate_identity(&checkpoint.identity)?;
        let payload = Self::restore_payload(&checkpoint)?;
        if payload.logical_epoch != checkpoint.logical_epoch
            || payload.input_frontiers != checkpoint.input_frontiers
            || payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers
        {
            return Err(invalid_checkpoint());
        }
        validate_input_event_time_frontiers_for_catalogs(
            &checkpoint,
            std::slice::from_ref(&payload.catalog),
        )?;
        validate_checkpoint_frontiers_for_schemas(
            &checkpoint,
            std::slice::from_ref(&payload.input_schema),
        )?;
        validate_view_sql_hash(&checkpoint.identity, payload.view_sql.as_str())?;
        let compiled_plan =
            validate_supported_latest_by_key_sql(payload.view_sql.as_str(), &payload.catalog)
                .map_err(|_| invalid_checkpoint())?;
        if compiled_plan != payload.plan {
            return Err(invalid_checkpoint());
        }
        validate_logical_view_plan(&payload.logical_plan).map_err(|_| invalid_checkpoint())?;
        let compiled_logical_plan = lower_supported_latest_by_key_sql_to_logical_plan(
            payload.view_sql.as_str(),
            &payload.catalog,
            &payload.output_schema,
        )
        .map_err(|_| invalid_checkpoint())?;
        if compiled_logical_plan != payload.logical_plan {
            return Err(invalid_checkpoint());
        }
        let latest_state = LatestByKeyState::from_checkpoint_rows(payload.latest_state)?;
        let published_output = payload
            .published_output
            .clone()
            .unwrap_or_else(|| latest_state.materialized_delta(&payload.plan));
        validate_published_output(&published_output)?;
        Ok(Self {
            identity: checkpoint.identity,
            catalog: payload.catalog,
            input_schema: payload.input_schema,
            output_schema: payload.output_schema,
            view_sql: payload.view_sql,
            plan: payload.plan,
            logical_plan: payload.logical_plan,
            latest_state,
            published_output,
            input_frontiers: checkpoint.input_frontiers,
            input_event_time_frontiers: checkpoint.input_event_time_frontiers,
            applied_epochs: payload
                .applied_epochs
                .into_iter()
                .map(|entry| (entry.idempotency_key, entry.logical_epoch))
                .collect(),
            logical_epoch: checkpoint.logical_epoch,
        })
    }
}

pub struct TwoInputJoinRuntime {
    identity: StandingProgramIdentity,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedJoinViewPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    engine: PrototypeIncrementalEngine,
    join: JoinOperator,
    published_output: DeltaBatch,
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    applied_epochs: BTreeMap<String, LogicalEpoch>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct JoinCheckpointPayload {
    schema_version: u32,
    runtime_kind: String,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedJoinViewPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    input_frontiers: Vec<RelationFrontier>,
    #[serde(default)]
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    left_state: DeltaBatch,
    right_state: DeltaBatch,
    engine: EngineCheckpointPayload,
    #[serde(default)]
    published_output: Option<DeltaBatch>,
    applied_epochs: Vec<GenericAppliedEpoch>,
}

struct LogicalPlanExecutorCommit {
    input_frontiers: Vec<RelationFrontier>,
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    output_delta: DeltaBatch,
}

enum LogicalPlanExecutor<'a> {
    SingleKeyAggregate {
        catalog: &'a VelorixRelationCatalogV1,
        input_schema: &'a RelationSchema,
        plan: &'a SupportedViewPlan,
        engine: &'a mut PrototypeIncrementalEngine,
    },
    LatestByKey {
        catalog: &'a VelorixRelationCatalogV1,
        input_schema: &'a RelationSchema,
        plan: &'a SupportedLatestByKeyPlan,
        latest_state: &'a mut LatestByKeyState,
        current_logical_epoch: LogicalEpoch,
    },
    TwoInputJoin {
        catalogs: &'a [VelorixRelationCatalogV1],
        input_schemas: &'a [RelationSchema],
        plan: &'a SupportedJoinViewPlan,
        engine: &'a mut PrototypeIncrementalEngine,
        join: &'a mut JoinOperator,
    },
}

impl LogicalPlanExecutor<'_> {
    fn apply_epoch(
        &mut self,
        logical_epoch: LogicalEpoch,
        current_frontiers: &[RelationFrontier],
        current_event_time_frontiers: &[InputEventTimeFrontier],
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<LogicalPlanExecutorCommit, StandingProgramRuntimeError> {
        match self {
            Self::SingleKeyAggregate {
                catalog,
                input_schema,
                plan,
                engine,
            } => {
                if logical_epoch <= engine.logical_epoch() {
                    return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                        current: engine.logical_epoch(),
                        attempted: logical_epoch,
                    });
                }
                let mut combined = DeltaBatch::default();
                let mut input_frontiers = current_frontiers.to_vec();
                let mut input_event_time_frontiers = current_event_time_frontiers.to_vec();
                for input in input_changes {
                    validate_input_matches_schema(&input, input_schema, "generic_input_relation")?;
                    let delta = arrow_record_batches_to_key_value_delta_batch(
                        catalog,
                        &input.relation_id,
                        &input.relation_version,
                        &input.schema_fingerprint,
                        std::slice::from_ref(&plan.group_key_column_id),
                        &plan.sum_value_column_id,
                        &input.batches,
                    )
                    .map_err(|_| {
                        StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "generic_input_batch",
                        }
                    })?;
                    let delta = filter_delta_batch_for_plan(&delta, plan, catalog)?;
                    combined = combined.combine(&delta);
                    advance_input_frontier(&mut input_frontiers, &input)?;
                    advance_input_event_time_frontier(&mut input_event_time_frontiers, &input)?;
                }
                let output_delta = engine
                    .push_changes(logical_epoch, &combined)
                    .map_err(|_| invalid_runtime_state())?;
                Ok(LogicalPlanExecutorCommit {
                    input_frontiers,
                    input_event_time_frontiers,
                    output_delta,
                })
            }
            Self::LatestByKey {
                catalog,
                input_schema,
                plan,
                latest_state,
                current_logical_epoch,
            } => {
                if logical_epoch <= *current_logical_epoch {
                    return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                        current: *current_logical_epoch,
                        attempted: logical_epoch,
                    });
                }
                let mut combined = DeltaBatch::default();
                let mut input_frontiers = current_frontiers.to_vec();
                let mut input_event_time_frontiers = current_event_time_frontiers.to_vec();
                for input in input_changes {
                    validate_input_matches_schema(
                        &input,
                        input_schema,
                        "latest_by_key_input_relation",
                    )?;
                    let delta = arrow_record_batches_to_key_latest_by_delta_batch(
                        catalog,
                        &input.relation_id,
                        &input.relation_version,
                        &input.schema_fingerprint,
                        &plan.key_column_id,
                        &plan.value_column_id,
                        &plan.ordering_column_id,
                        &input.batches,
                    )
                    .map_err(|_| {
                        StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "latest_by_key_input_batch",
                        }
                    })?;
                    combined = combined.combine(&delta);
                    advance_input_frontier(&mut input_frontiers, &input)?;
                    advance_input_event_time_frontier(&mut input_event_time_frontiers, &input)?;
                }
                let output_delta = latest_state.apply_delta(&combined, plan)?;
                Ok(LogicalPlanExecutorCommit {
                    input_frontiers,
                    input_event_time_frontiers,
                    output_delta,
                })
            }
            Self::TwoInputJoin {
                catalogs,
                input_schemas,
                plan,
                engine,
                join,
            } => {
                if logical_epoch <= engine.logical_epoch() {
                    return Err(StandingProgramRuntimeError::NonMonotonicLogicalEpoch {
                        current: engine.logical_epoch(),
                        attempted: logical_epoch,
                    });
                }
                let mut joined_changes = DeltaBatch::default();
                let mut input_frontiers = current_frontiers.to_vec();
                let mut input_event_time_frontiers = current_event_time_frontiers.to_vec();
                for input in input_changes {
                    validate_input_matches_one_schema(
                        &input,
                        input_schemas,
                        "generic_join_input_relation",
                    )?;
                    let catalog = join_catalog_for_relation(catalogs, &input.relation_id)?;
                    if input.relation_id == plan.left_input_relation_id {
                        let delta = arrow_record_batches_to_key_value_delta_batch(
                            catalog,
                            &input.relation_id,
                            &input.relation_version,
                            &input.schema_fingerprint,
                            std::slice::from_ref(&plan.left_join_key_column_id),
                            &plan.sum_value_column_id,
                            &input.batches,
                        )
                        .map_err(|_| {
                            StandingProgramRuntimeError::InvalidProgramIdentity {
                                field: "generic_join_input_batch",
                            }
                        })?;
                        let joined = join
                            .apply_left(&delta)
                            .map_err(|_| invalid_runtime_state())?;
                        joined_changes = joined_changes.combine(&joined);
                    } else if input.relation_id == plan.right_input_relation_id {
                        let delta = arrow_record_batches_to_key_value_delta_batch(
                            catalog,
                            &input.relation_id,
                            &input.relation_version,
                            &input.schema_fingerprint,
                            std::slice::from_ref(&plan.right_join_key_column_id),
                            &plan.right_join_key_column_id,
                            &input.batches,
                        )
                        .map_err(|_| {
                            StandingProgramRuntimeError::InvalidProgramIdentity {
                                field: "generic_join_input_batch",
                            }
                        })?;
                        let joined = join
                            .apply_right(&delta)
                            .map_err(|_| invalid_runtime_state())?;
                        joined_changes = joined_changes.combine(&joined);
                    } else {
                        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "generic_join_input_relation",
                        });
                    }
                    advance_input_frontier(&mut input_frontiers, &input)?;
                    advance_input_event_time_frontier(&mut input_event_time_frontiers, &input)?;
                }

                let output_delta = engine
                    .push_changes(logical_epoch, &joined_changes)
                    .map_err(|_| invalid_runtime_state())?;
                Ok(LogicalPlanExecutorCommit {
                    input_frontiers,
                    input_event_time_frontiers,
                    output_delta,
                })
            }
        }
    }
}

fn validate_input_matches_schema(
    input: &RelationInputBatch,
    schema: &RelationSchema,
    field: &'static str,
) -> Result<(), StandingProgramRuntimeError> {
    if input.relation_id != schema.relation_id
        || input.relation_version != schema.relation_version
        || input.schema_fingerprint != schema.schema_fingerprint
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field });
    }
    Ok(())
}

fn validate_input_matches_one_schema(
    input: &RelationInputBatch,
    schemas: &[RelationSchema],
    field: &'static str,
) -> Result<(), StandingProgramRuntimeError> {
    if schemas
        .iter()
        .any(|schema| validate_input_matches_schema(input, schema, field).is_ok())
    {
        Ok(())
    } else {
        Err(StandingProgramRuntimeError::InvalidProgramIdentity { field })
    }
}

fn advance_input_frontier(
    frontiers: &mut Vec<RelationFrontier>,
    input: &RelationInputBatch,
) -> Result<(), StandingProgramRuntimeError> {
    if let Some(frontier) = frontiers.iter_mut().find(|frontier| {
        frontier.relation_id == input.relation_id
            && frontier.relation_version == input.relation_version
    }) {
        if input.start_offset_inclusive < frontier.committed_offset_exclusive {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "input_frontier.offset_range",
            });
        }
        frontier.committed_offset_exclusive = frontier
            .committed_offset_exclusive
            .max(input.end_offset_exclusive);
    } else {
        if input.start_offset_inclusive != 0 {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "input_frontier.offset_range",
            });
        }
        frontiers.push(RelationFrontier {
            relation_id: input.relation_id.clone(),
            relation_version: input.relation_version.clone(),
            committed_offset_exclusive: input.end_offset_exclusive,
        });
    }
    Ok(())
}

fn advance_input_event_time_frontier(
    frontiers: &mut Vec<InputEventTimeFrontier>,
    input: &RelationInputBatch,
) -> Result<(), StandingProgramRuntimeError> {
    let Some(watermark) = &input.event_time_watermark else {
        return Ok(());
    };
    if watermark.stream_id.trim().is_empty() || watermark.event_time_column_id.trim().is_empty() {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_event_time_watermark",
        });
    }
    if watermark.watermark_ns > watermark.max_observed_event_time_ns {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_event_time_watermark",
        });
    }

    if let Some(frontier) = frontiers.iter_mut().find(|frontier| {
        frontier.relation_id == input.relation_id
            && frontier.relation_version == input.relation_version
            && frontier.schema_fingerprint == input.schema_fingerprint
            && frontier.stream_id == watermark.stream_id
            && frontier.partition_id == watermark.partition_id
    }) {
        if frontier.event_time_column_id != watermark.event_time_column_id
            || watermark.max_observed_event_time_ns < frontier.max_observed_event_time_ns
            || watermark.watermark_ns < frontier.watermark_ns
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "input_event_time_watermark",
            });
        }
        frontier.max_observed_event_time_ns = watermark.max_observed_event_time_ns;
        frontier.watermark_ns = watermark.watermark_ns;
        return Ok(());
    }

    frontiers.push(InputEventTimeFrontier {
        relation_id: input.relation_id.clone(),
        relation_version: input.relation_version.clone(),
        schema_fingerprint: input.schema_fingerprint.clone(),
        stream_id: watermark.stream_id.clone(),
        partition_id: watermark.partition_id,
        event_time_column_id: watermark.event_time_column_id.clone(),
        max_observed_event_time_ns: watermark.max_observed_event_time_ns,
        watermark_ns: watermark.watermark_ns,
    });
    Ok(())
}

impl SingleKeySumCountRuntime {
    pub fn new_with_plan(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedViewPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        Self::new_with_logical_plan(
            identity,
            catalog,
            input_schema,
            output_schema,
            view_sql,
            plan,
            logical_plan,
        )
    }

    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalog: VelorixRelationCatalogV1,
        input_schema: RelationSchema,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedViewPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_runtime_package(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_view_plan",
            }
        })?;
        validate_supported_schemas(&catalog, &input_schema, &output_schema, &plan)?;
        let compiled_plan =
            validate_supported_view_sql(view_sql.as_str(), &catalog).map_err(|_| {
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan",
                }
            })?;
        if compiled_plan != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan",
            });
        }
        let compiled_logical_plan =
            lower_supported_view_sql_to_logical_plan(view_sql.as_str(), &catalog, &output_schema)
                .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_view_plan",
            })?;
        if compiled_logical_plan != logical_plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_view_plan",
            });
        }
        validate_plan_matches_catalog(&plan, &catalog)?;
        let value_mode = aggregate_value_mode_for_plan(&catalog, &plan)?;
        let track_extrema = plan_tracks_extrema(&plan);
        Ok(Self {
            identity,
            catalog,
            input_schema,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            engine: PrototypeIncrementalEngine::with_aggregate_value_mode_and_extrema(
                value_mode,
                track_extrema,
            ),
            published_output: DeltaBatch::default(),
            input_frontiers: Vec::new(),
            input_event_time_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
        })
    }

    fn output_schema_fingerprint(&self) -> String {
        self.output_schema.schema_fingerprint.clone()
    }

    fn materialized_batch(&self) -> Result<RecordBatch, StandingProgramRuntimeError> {
        materialized_delta_to_record_batch(
            &self.output_schema,
            &self.published_output,
            Some(&supported_view_plan_aggregate_outputs(&self.plan)),
        )
    }

    fn materialized_page_batch(
        &self,
        page: SnapshotPageRequest,
    ) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
        materialized_delta_page_batch(
            &self.output_schema,
            &self.published_output,
            self.engine.logical_epoch(),
            page,
            Some(&supported_view_plan_aggregate_outputs(&self.plan)),
        )
    }

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
        let payload = GenericCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            catalog: self.catalog.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            engine: self.engine.checkpoint_state().to_payload(),
            published_output: self.published_output.clone(),
            applied_epochs: self
                .applied_epochs
                .iter()
                .map(|(idempotency_key, logical_epoch)| GenericAppliedEpoch {
                    idempotency_key: idempotency_key.clone(),
                    logical_epoch: *logical_epoch,
                })
                .collect(),
        };
        serde_json::to_string(&payload).map_err(|_| invalid_checkpoint())
    }

    fn restore_payload(
        checkpoint: &RuntimeCheckpoint,
    ) -> Result<GenericCheckpointPayload, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_checkpoint());
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: GenericCheckpointPayload =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION {
            return Err(invalid_checkpoint());
        }
        validate_supported_schemas(
            &payload.catalog,
            &payload.input_schema,
            &payload.output_schema,
            &payload.plan,
        )?;
        Ok(payload)
    }
}

impl StandingProgramRuntime for SingleKeySumCountRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        vec![self.input_schema.clone()]
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        vec![self.output_schema.clone()]
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        self.engine.logical_epoch()
    }

    fn apply_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        let idempotency_key_text = idempotency_key.as_str().to_string();
        if let Some(applied_epoch) = self.applied_epochs.get(&idempotency_key_text) {
            if *applied_epoch == logical_epoch {
                return Ok(EpochCommit {
                    logical_epoch,
                    idempotency_key,
                    input_frontiers: self.input_frontiers.clone(),
                    input_event_time_frontiers: self.input_event_time_frontiers.clone(),
                    output_deltas: Vec::new(),
                    output_batches: vec![ViewOutputBatch {
                        view_id: self.identity.view_ids[0].clone(),
                        schema_fingerprint: self.output_schema_fingerprint(),
                        batches: vec![self.materialized_batch()?],
                    }],
                });
            }
            return Err(StandingProgramRuntimeError::IdempotencyKeyConflict {
                idempotency_key: idempotency_key_text,
                first_epoch: *applied_epoch,
                attempted_epoch: logical_epoch,
            });
        }
        let mut executor = LogicalPlanExecutor::SingleKeyAggregate {
            catalog: &self.catalog,
            input_schema: &self.input_schema,
            plan: &self.plan,
            engine: &mut self.engine,
        };
        let executor_commit = executor.apply_epoch(
            logical_epoch,
            &self.input_frontiers,
            &self.input_event_time_frontiers,
            input_changes,
        )?;
        self.published_output =
            apply_published_output_delta(&self.published_output, &executor_commit.output_delta)?;
        self.input_frontiers = executor_commit.input_frontiers.clone();
        self.input_event_time_frontiers = executor_commit.input_event_time_frontiers.clone();
        self.applied_epochs
            .insert(idempotency_key_text, logical_epoch);

        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers: executor_commit.input_frontiers,
            input_event_time_frontiers: executor_commit.input_event_time_frontiers,
            output_deltas: vec![ViewOutputDelta {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                delta: executor_commit.output_delta,
            }],
            output_batches: vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![self.materialized_batch()?],
            }],
        })
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        if view.tenant_id != self.identity.tenant_id
            || view.program_id != self.identity.program_id
            || !self
                .identity
                .view_ids
                .iter()
                .any(|view_id| view_id == &view.view_id)
        {
            return Err(StandingProgramRuntimeError::UnknownView {
                view_id: view.view_id,
            });
        }

        let (batch, next_page_token) = self.materialized_page_batch(page)?;
        Ok(MaterializedViewPage {
            view,
            logical_epoch: self.engine.logical_epoch(),
            schema_fingerprint: self.output_schema_fingerprint(),
            batches: vec![batch],
            next_page_token,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        let payload = self.checkpoint_payload()?;
        let content_hash = stable_bytes_hash(payload.as_bytes());
        Ok(RuntimeCheckpoint {
            identity: self.identity.clone(),
            logical_epoch: self.engine.logical_epoch(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            output_frontiers: self
                .identity
                .view_ids
                .iter()
                .map(|view_id| ViewFrontier {
                    view_id: view_id.clone(),
                    committed_epoch: self.engine.logical_epoch(),
                })
                .collect(),
            checkpoint_codec_identity: self.identity.checkpoint_codec_identity.clone(),
            state_root: DurableStateRoot {
                object_key: format!(
                    "v1/state/materialized-view-runtime/{}/checkpoint",
                    self.identity.program_id
                ),
                content_hash,
            },
            state_payload: Some(RuntimeCheckpointStatePayload {
                codec_identity: self.identity.checkpoint_codec_identity.clone(),
                payload,
            }),
            output_manifest_refs: Vec::new(),
            owner_epoch: None,
        })
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        checkpoint.validate_identity(&checkpoint.identity)?;
        let payload = Self::restore_payload(&checkpoint)?;
        validate_checkpoint_frontiers(&checkpoint, &payload)?;
        let engine_checkpoint = payload.engine.into_checkpoint();
        if engine_checkpoint.logical_epoch() != checkpoint.logical_epoch {
            return Err(invalid_checkpoint());
        }
        if payload.input_frontiers != checkpoint.input_frontiers {
            return Err(invalid_checkpoint());
        }
        if payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers {
            return Err(invalid_checkpoint());
        }
        validate_input_event_time_frontiers_for_catalogs(
            &checkpoint,
            std::slice::from_ref(&payload.catalog),
        )?;
        let view_sql = payload.view_sql;
        validate_view_sql_hash(&checkpoint.identity, view_sql.as_str())?;
        let plan = payload.plan;
        let compiled = validate_supported_view_sql(view_sql.as_str(), &payload.catalog)
            .map_err(|_| invalid_checkpoint())?;
        if compiled != plan {
            return Err(invalid_checkpoint());
        }
        validate_plan_matches_catalog(&plan, &payload.catalog)?;
        let value_mode = aggregate_value_mode_for_plan(&payload.catalog, &plan)?;
        let track_extrema = plan_tracks_extrema(&plan);
        let logical_plan = payload.logical_plan;
        validate_logical_view_plan(&logical_plan).map_err(|_| invalid_checkpoint())?;
        let compiled_logical_plan = lower_supported_view_sql_to_logical_plan(
            view_sql.as_str(),
            &payload.catalog,
            &payload.output_schema,
        )
        .map_err(|_| invalid_checkpoint())?;
        if compiled_logical_plan != logical_plan {
            return Err(invalid_checkpoint());
        }
        let engine =
            PrototypeIncrementalEngine::from_checkpoint_with_aggregate_value_mode_and_extrema(
                engine_checkpoint,
                value_mode,
                track_extrema,
            )
            .map_err(|_| invalid_checkpoint())?;
        let published_output = payload.published_output;
        validate_published_output(&published_output)?;
        Ok(Self {
            identity: checkpoint.identity,
            catalog: payload.catalog,
            input_schema: payload.input_schema,
            output_schema: payload.output_schema,
            view_sql,
            plan,
            logical_plan,
            engine,
            published_output,
            input_frontiers: checkpoint.input_frontiers,
            input_event_time_frontiers: checkpoint.input_event_time_frontiers,
            applied_epochs: payload
                .applied_epochs
                .into_iter()
                .map(|entry| (entry.idempotency_key, entry.logical_epoch))
                .collect(),
        })
    }
}

impl TwoInputJoinRuntime {
    pub fn new_with_plan(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedJoinViewPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        Self::new_with_logical_plan(
            identity,
            catalogs,
            input_schemas,
            output_schema,
            view_sql,
            plan,
            logical_plan,
        )
    }

    pub fn new_with_logical_plan(
        identity: StandingProgramIdentity,
        catalogs: Vec<VelorixRelationCatalogV1>,
        input_schemas: Vec<RelationSchema>,
        output_schema: RelationSchema,
        view_sql: String,
        plan: SupportedJoinViewPlan,
        logical_plan: VelorixLogicalViewPlanV1,
    ) -> Result<Self, StandingProgramRuntimeError> {
        identity.validate()?;
        validate_runtime_package(&identity)?;
        validate_view_sql_hash(&identity, view_sql.as_str())?;
        validate_logical_view_plan(&logical_plan).map_err(|_| {
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_join_view_plan",
            }
        })?;
        validate_join_supported_schemas(&catalogs, &input_schemas, &output_schema, &plan)?;
        let compiled_plan = validate_supported_join_view_sql(view_sql.as_str(), &catalogs)
            .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_view_plan",
            })?;
        if compiled_plan != plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_view_plan",
            });
        }
        let compiled_logical_plan = lower_supported_join_view_sql_to_logical_plan(
            view_sql.as_str(),
            &catalogs,
            &output_schema,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "logical_join_view_plan",
        })?;
        if compiled_logical_plan != logical_plan {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "logical_join_view_plan",
            });
        }
        validate_join_plan_matches_catalogs(&plan, &catalogs)?;
        let left_catalog = join_left_catalog(&plan, &catalogs)?;
        let value_mode =
            aggregate_value_mode_for_column_id(left_catalog, &plan.sum_value_column_id)?;
        Ok(Self {
            identity,
            catalogs,
            input_schemas,
            output_schema,
            view_sql,
            plan,
            logical_plan,
            engine: PrototypeIncrementalEngine::with_aggregate_value_mode(value_mode),
            join: new_join_operator(),
            published_output: DeltaBatch::default(),
            input_frontiers: Vec::new(),
            input_event_time_frontiers: Vec::new(),
            applied_epochs: BTreeMap::new(),
        })
    }

    fn output_schema_fingerprint(&self) -> String {
        self.output_schema.schema_fingerprint.clone()
    }

    fn materialized_batch(&self) -> Result<RecordBatch, StandingProgramRuntimeError> {
        materialized_delta_to_record_batch(&self.output_schema, &self.published_output, None)
    }

    fn materialized_page_batch(
        &self,
        page: SnapshotPageRequest,
    ) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
        materialized_delta_page_batch(
            &self.output_schema,
            &self.published_output,
            self.engine.logical_epoch(),
            page,
            None,
        )
    }

    fn checkpoint_payload(&self) -> Result<String, StandingProgramRuntimeError> {
        let payload = JoinCheckpointPayload {
            schema_version: CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
            runtime_kind: JOIN_RUNTIME_KIND.to_string(),
            catalogs: self.catalogs.clone(),
            input_schemas: self.input_schemas.clone(),
            output_schema: self.output_schema.clone(),
            view_sql: self.view_sql.clone(),
            plan: self.plan.clone(),
            logical_plan: self.logical_plan.clone(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            left_state: self.join.left_state(),
            right_state: self.join.right_state(),
            engine: self.engine.checkpoint_state().to_payload(),
            published_output: Some(self.published_output.clone()),
            applied_epochs: self
                .applied_epochs
                .iter()
                .map(|(idempotency_key, logical_epoch)| GenericAppliedEpoch {
                    idempotency_key: idempotency_key.clone(),
                    logical_epoch: *logical_epoch,
                })
                .collect(),
        };
        serde_json::to_string(&payload).map_err(|_| invalid_checkpoint())
    }

    fn restore_payload(
        checkpoint: &RuntimeCheckpoint,
    ) -> Result<JoinCheckpointPayload, StandingProgramRuntimeError> {
        let Some(state_payload) = &checkpoint.state_payload else {
            return Err(invalid_checkpoint());
        };
        if state_payload.codec_identity != checkpoint.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: checkpoint.checkpoint_codec_identity.clone(),
                actual: state_payload.codec_identity.clone(),
            });
        }
        let payload: JoinCheckpointPayload =
            serde_json::from_str(&state_payload.payload).map_err(|_| invalid_checkpoint())?;
        if payload.schema_version != CHECKPOINT_PAYLOAD_SCHEMA_VERSION
            || payload.runtime_kind != JOIN_RUNTIME_KIND
        {
            return Err(invalid_checkpoint());
        }
        validate_join_supported_schemas(
            &payload.catalogs,
            &payload.input_schemas,
            &payload.output_schema,
            &payload.plan,
        )?;
        Ok(payload)
    }
}

impl StandingProgramRuntime for TwoInputJoinRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        self.input_schemas.clone()
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        vec![self.output_schema.clone()]
    }

    fn logical_epoch(&self) -> LogicalEpoch {
        self.engine.logical_epoch()
    }

    fn apply_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        let idempotency_key_text = idempotency_key.as_str().to_string();
        if let Some(applied_epoch) = self.applied_epochs.get(&idempotency_key_text) {
            if *applied_epoch == logical_epoch {
                return Ok(EpochCommit {
                    logical_epoch,
                    idempotency_key,
                    input_frontiers: self.input_frontiers.clone(),
                    input_event_time_frontiers: self.input_event_time_frontiers.clone(),
                    output_deltas: Vec::new(),
                    output_batches: vec![ViewOutputBatch {
                        view_id: self.identity.view_ids[0].clone(),
                        schema_fingerprint: self.output_schema_fingerprint(),
                        batches: vec![self.materialized_batch()?],
                    }],
                });
            }
            return Err(StandingProgramRuntimeError::IdempotencyKeyConflict {
                idempotency_key: idempotency_key_text,
                first_epoch: *applied_epoch,
                attempted_epoch: logical_epoch,
            });
        }
        let mut executor = LogicalPlanExecutor::TwoInputJoin {
            catalogs: &self.catalogs,
            input_schemas: &self.input_schemas,
            plan: &self.plan,
            engine: &mut self.engine,
            join: &mut self.join,
        };
        let executor_commit = executor.apply_epoch(
            logical_epoch,
            &self.input_frontiers,
            &self.input_event_time_frontiers,
            input_changes,
        )?;
        self.published_output =
            apply_published_output_delta(&self.published_output, &executor_commit.output_delta)?;
        self.input_frontiers = executor_commit.input_frontiers.clone();
        self.input_event_time_frontiers = executor_commit.input_event_time_frontiers.clone();
        self.applied_epochs
            .insert(idempotency_key_text, logical_epoch);

        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers: executor_commit.input_frontiers,
            input_event_time_frontiers: executor_commit.input_event_time_frontiers,
            output_deltas: vec![ViewOutputDelta {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                delta: executor_commit.output_delta,
            }],
            output_batches: vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schema_fingerprint(),
                batches: vec![self.materialized_batch()?],
            }],
        })
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        if view.tenant_id != self.identity.tenant_id
            || view.program_id != self.identity.program_id
            || !self
                .identity
                .view_ids
                .iter()
                .any(|view_id| view_id == &view.view_id)
        {
            return Err(StandingProgramRuntimeError::UnknownView {
                view_id: view.view_id,
            });
        }

        let (batch, next_page_token) = self.materialized_page_batch(page)?;
        Ok(MaterializedViewPage {
            view,
            logical_epoch: self.engine.logical_epoch(),
            schema_fingerprint: self.output_schema_fingerprint(),
            batches: vec![batch],
            next_page_token,
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        let payload = self.checkpoint_payload()?;
        let content_hash = stable_bytes_hash(payload.as_bytes());
        Ok(RuntimeCheckpoint {
            identity: self.identity.clone(),
            logical_epoch: self.engine.logical_epoch(),
            input_frontiers: self.input_frontiers.clone(),
            input_event_time_frontiers: self.input_event_time_frontiers.clone(),
            output_frontiers: self
                .identity
                .view_ids
                .iter()
                .map(|view_id| ViewFrontier {
                    view_id: view_id.clone(),
                    committed_epoch: self.engine.logical_epoch(),
                })
                .collect(),
            checkpoint_codec_identity: self.identity.checkpoint_codec_identity.clone(),
            state_root: DurableStateRoot {
                object_key: format!(
                    "v1/state/materialized-view-runtime/{}/checkpoint",
                    self.identity.program_id
                ),
                content_hash,
            },
            state_payload: Some(RuntimeCheckpointStatePayload {
                codec_identity: self.identity.checkpoint_codec_identity.clone(),
                payload,
            }),
            output_manifest_refs: Vec::new(),
            owner_epoch: None,
        })
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        checkpoint.validate_identity(&checkpoint.identity)?;
        let payload = Self::restore_payload(&checkpoint)?;
        validate_join_checkpoint_frontiers(&checkpoint, &payload)?;
        let engine_checkpoint = payload.engine.into_checkpoint();
        if engine_checkpoint.logical_epoch() != checkpoint.logical_epoch
            || payload.input_frontiers != checkpoint.input_frontiers
            || payload.input_event_time_frontiers != checkpoint.input_event_time_frontiers
        {
            return Err(invalid_checkpoint());
        }
        validate_input_event_time_frontiers_for_catalogs(&checkpoint, &payload.catalogs)?;
        validate_view_sql_hash(&checkpoint.identity, payload.view_sql.as_str())?;
        let compiled =
            validate_supported_join_view_sql(payload.view_sql.as_str(), &payload.catalogs)
                .map_err(|_| invalid_checkpoint())?;
        if compiled != payload.plan {
            return Err(invalid_checkpoint());
        }
        validate_logical_view_plan(&payload.logical_plan).map_err(|_| invalid_checkpoint())?;
        let compiled_logical_plan = lower_supported_join_view_sql_to_logical_plan(
            payload.view_sql.as_str(),
            &payload.catalogs,
            &payload.output_schema,
        )
        .map_err(|_| invalid_checkpoint())?;
        if compiled_logical_plan != payload.logical_plan {
            return Err(invalid_checkpoint());
        }
        validate_join_plan_matches_catalogs(&payload.plan, &payload.catalogs)?;
        let left_catalog = join_left_catalog(&payload.plan, &payload.catalogs)?;
        let value_mode =
            aggregate_value_mode_for_column_id(left_catalog, &payload.plan.sum_value_column_id)?;
        let engine = PrototypeIncrementalEngine::from_checkpoint_with_aggregate_value_mode(
            engine_checkpoint,
            value_mode,
        )
        .map_err(|_| invalid_checkpoint())?;
        let published_output = payload
            .published_output
            .clone()
            .unwrap_or_else(|| engine.materialized_state());
        validate_published_output(&published_output)?;
        Ok(Self {
            identity: checkpoint.identity,
            catalogs: payload.catalogs,
            input_schemas: payload.input_schemas,
            output_schema: payload.output_schema,
            view_sql: payload.view_sql,
            plan: payload.plan,
            logical_plan: payload.logical_plan,
            engine,
            join: restore_join_operator(&payload.left_state, &payload.right_state)?,
            published_output,
            input_frontiers: checkpoint.input_frontiers,
            input_event_time_frontiers: checkpoint.input_event_time_frontiers,
            applied_epochs: payload
                .applied_epochs
                .into_iter()
                .map(|entry| (entry.idempotency_key, entry.logical_epoch))
                .collect(),
        })
    }
}

fn validate_checkpoint_frontiers(
    checkpoint: &RuntimeCheckpoint,
    payload: &GenericCheckpointPayload,
) -> Result<(), StandingProgramRuntimeError> {
    if checkpoint.input_frontiers.len() > 1 {
        return Err(invalid_checkpoint());
    }
    for frontier in &checkpoint.input_frontiers {
        if frontier.relation_id != payload.input_schema.relation_id
            || frontier.relation_version != payload.input_schema.relation_version
        {
            return Err(invalid_checkpoint());
        }
    }
    if checkpoint.output_frontiers.len() != checkpoint.identity.view_ids.len() {
        return Err(invalid_checkpoint());
    }
    for view_id in &checkpoint.identity.view_ids {
        let Some(frontier) = checkpoint
            .output_frontiers
            .iter()
            .find(|frontier| &frontier.view_id == view_id)
        else {
            return Err(invalid_checkpoint());
        };
        if frontier.committed_epoch != checkpoint.logical_epoch {
            return Err(invalid_checkpoint());
        }
    }
    Ok(())
}

fn validate_join_checkpoint_frontiers(
    checkpoint: &RuntimeCheckpoint,
    payload: &JoinCheckpointPayload,
) -> Result<(), StandingProgramRuntimeError> {
    if checkpoint.input_frontiers.len() > payload.input_schemas.len() {
        return Err(invalid_checkpoint());
    }
    for frontier in &checkpoint.input_frontiers {
        if !payload.input_schemas.iter().any(|schema| {
            schema.relation_id == frontier.relation_id
                && schema.relation_version == frontier.relation_version
        }) {
            return Err(invalid_checkpoint());
        }
    }
    if checkpoint.output_frontiers.len() != checkpoint.identity.view_ids.len() {
        return Err(invalid_checkpoint());
    }
    for view_id in &checkpoint.identity.view_ids {
        let Some(frontier) = checkpoint
            .output_frontiers
            .iter()
            .find(|frontier| &frontier.view_id == view_id)
        else {
            return Err(invalid_checkpoint());
        };
        if frontier.committed_epoch != checkpoint.logical_epoch {
            return Err(invalid_checkpoint());
        }
    }
    Ok(())
}

fn validate_checkpoint_frontiers_for_schemas(
    checkpoint: &RuntimeCheckpoint,
    input_schemas: &[RelationSchema],
) -> Result<(), StandingProgramRuntimeError> {
    if checkpoint.input_frontiers.len() > input_schemas.len() {
        return Err(invalid_checkpoint());
    }
    for frontier in &checkpoint.input_frontiers {
        if !input_schemas.iter().any(|schema| {
            schema.relation_id == frontier.relation_id
                && schema.relation_version == frontier.relation_version
        }) {
            return Err(invalid_checkpoint());
        }
    }
    if checkpoint.output_frontiers.len() != checkpoint.identity.view_ids.len() {
        return Err(invalid_checkpoint());
    }
    for view_id in &checkpoint.identity.view_ids {
        let Some(frontier) = checkpoint
            .output_frontiers
            .iter()
            .find(|frontier| &frontier.view_id == view_id)
        else {
            return Err(invalid_checkpoint());
        };
        if frontier.committed_epoch != checkpoint.logical_epoch {
            return Err(invalid_checkpoint());
        }
    }
    Ok(())
}

fn validate_input_event_time_frontiers_for_catalogs(
    checkpoint: &RuntimeCheckpoint,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<(), StandingProgramRuntimeError> {
    let mut seen = BTreeSet::new();
    for frontier in &checkpoint.input_event_time_frontiers {
        if frontier.stream_id.trim().is_empty()
            || frontier.event_time_column_id.trim().is_empty()
            || frontier.watermark_ns > frontier.max_observed_event_time_ns
        {
            return Err(invalid_checkpoint());
        }
        if !seen.insert((
            frontier.relation_id.as_str(),
            frontier.relation_version.as_str(),
            frontier.schema_fingerprint.as_str(),
            frontier.stream_id.as_str(),
            frontier.partition_id,
        )) {
            return Err(invalid_checkpoint());
        }
        let Some(catalog) = catalogs.iter().find(|catalog| {
            catalog.relation_schema.relation_id == frontier.relation_id
                && catalog.relation_schema.relation_version == frontier.relation_version
                && catalog.schema_fingerprint.as_str() == frontier.schema_fingerprint
        }) else {
            return Err(invalid_checkpoint());
        };
        let Some(event_time_column_id) = &catalog.relation_schema.event_time_column_id else {
            return Err(invalid_checkpoint());
        };
        if event_time_column_id != &frontier.event_time_column_id {
            return Err(invalid_checkpoint());
        }
        let Some(column) = catalog
            .relation_schema
            .columns
            .iter()
            .find(|column| column.column_id == *event_time_column_id)
        else {
            return Err(invalid_checkpoint());
        };
        match column.physical_arrow_type {
            ArrowPhysicalTypeV1::Int64
            | ArrowPhysicalTypeV1::Date32
            | ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {}
            _ => return Err(invalid_checkpoint()),
        }
    }
    Ok(())
}

fn validate_join_supported_schemas(
    catalogs: &[VelorixRelationCatalogV1],
    inputs: &[RelationSchema],
    output: &RelationSchema,
    plan: &SupportedJoinViewPlan,
) -> Result<(), StandingProgramRuntimeError> {
    let [left_catalog, right_catalog] = catalogs else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field: "catalogs" });
    };
    let expected_inputs = catalogs
        .iter()
        .map(|catalog| {
            catalog.validate().map_err(|_| {
                StandingProgramRuntimeError::InvalidProgramIdentity { field: "catalog" }
            })?;
            catalog_input_relation_schema(catalog).map_err(|_| {
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "input_schema",
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if expected_inputs != inputs {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_schemas",
        });
    }
    let [key, sum, count] = output.columns.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.columns",
        });
    };
    let right_key = catalog_primary_key_column(right_catalog)?;
    let expected_key_type = sql_type_from_catalog_column(right_key)?;
    let expected_sum_type =
        aggregate_sum_sql_type_for_column(left_catalog, &plan.sum_value_column_id)?;
    if output.primary_key != vec![key.name.clone()]
        || key.name != right_key.name
        || key.data_type != expected_key_type
        || sum.data_type != expected_sum_type
        || !matches!(count.data_type, SqlDataType::Int64)
        || sum.name != "sum"
        || count.name != "count"
        || key.nullable
        || sum.nullable
        || count.nullable
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema",
        });
    }
    Ok(())
}

fn validate_join_plan_matches_catalogs(
    plan: &SupportedJoinViewPlan,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<(), StandingProgramRuntimeError> {
    let left = join_left_catalog(plan, catalogs)?;
    let right = join_right_catalog(plan, catalogs)?;
    aggregate_value_mode_for_column_id(left, &plan.sum_value_column_id)?;
    if plan.left_join_key_column_id != catalog_primary_key_column(left)?.column_id
        || plan.right_join_key_column_id != catalog_primary_key_column(right)?.column_id
        || plan.group_key_relation_id != right.relation_schema.relation_id
        || plan.group_key_column_id != catalog_primary_key_column(right)?.column_id
        || plan.sum_value_relation_id != left.relation_schema.relation_id
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan",
        });
    }
    Ok(())
}

fn join_left_catalog<'a>(
    plan: &SupportedJoinViewPlan,
    catalogs: &'a [VelorixRelationCatalogV1],
) -> Result<&'a VelorixRelationCatalogV1, StandingProgramRuntimeError> {
    join_catalog_for_relation(catalogs, &plan.left_input_relation_id)
}

fn join_right_catalog<'a>(
    plan: &SupportedJoinViewPlan,
    catalogs: &'a [VelorixRelationCatalogV1],
) -> Result<&'a VelorixRelationCatalogV1, StandingProgramRuntimeError> {
    join_catalog_for_relation(catalogs, &plan.right_input_relation_id)
}

fn join_catalog_for_relation<'a>(
    catalogs: &'a [VelorixRelationCatalogV1],
    relation_id: &str,
) -> Result<&'a VelorixRelationCatalogV1, StandingProgramRuntimeError> {
    catalogs
        .iter()
        .find(|catalog| catalog.relation_schema.relation_id == relation_id)
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_catalog",
        })
}

fn new_join_operator() -> JoinOperator {
    KeyedEquiJoin::new(
        join_left_value as fn(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError>,
    )
}

fn restore_join_operator(
    left_state: &DeltaBatch,
    right_state: &DeltaBatch,
) -> Result<JoinOperator, StandingProgramRuntimeError> {
    let mut join = new_join_operator();
    join.apply_left(left_state)
        .map_err(|_| invalid_runtime_state())?;
    join.apply_right(right_state)
        .map_err(|_| invalid_runtime_state())?;
    Ok(join)
}

fn join_left_value(left: &DeltaValue, _right: &DeltaValue) -> Result<DeltaValue, OperatorError> {
    Ok(left.clone())
}

fn only_schema(
    schemas: &[RelationSchema],
    field: &'static str,
) -> Result<RelationSchema, StandingProgramRuntimeError> {
    let [schema] = schemas else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field });
    };
    Ok(schema.clone())
}

fn validate_runtime_package(
    identity: &StandingProgramIdentity,
) -> Result<(), StandingProgramRuntimeError> {
    if identity
        .runtime_packages
        .iter()
        .any(|package| package.name == CRATE_NAME)
    {
        Ok(())
    } else {
        Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "runtime_packages",
        })
    }
}

fn validate_supported_schemas(
    catalog: &VelorixRelationCatalogV1,
    input: &RelationSchema,
    output: &RelationSchema,
    plan: &SupportedViewPlan,
) -> Result<(), StandingProgramRuntimeError> {
    catalog
        .validate()
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity { field: "catalog" })?;
    let expected_input = catalog_input_relation_schema(catalog).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_schema",
        }
    })?;
    if &expected_input != input {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_schema",
        });
    }
    let [key, aggregate_columns @ ..] = output.columns.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.columns",
        });
    };
    let key_column = catalog_primary_key_column(catalog)?;
    let expected_key_type = sql_type_from_catalog_column(key_column)?;
    if output.primary_key != vec![key.name.clone()]
        || key.name != key_column.name
        || key.data_type != expected_key_type
        || key.nullable
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema",
        });
    }
    let aggregate_outputs = supported_view_plan_aggregate_outputs(plan);
    if aggregate_columns.len() != aggregate_outputs.len() {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.columns",
        });
    }
    for (column, aggregate) in aggregate_columns.iter().zip(aggregate_outputs.iter()) {
        if column.name != aggregate.output_column_id
            || column.data_type != aggregate_output_sql_type(catalog, aggregate)?
            || column.nullable
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "output_schema",
            });
        }
    }
    Ok(())
}

fn validate_latest_supported_schemas(
    catalog: &VelorixRelationCatalogV1,
    input: &RelationSchema,
    output: &RelationSchema,
    plan: &SupportedLatestByKeyPlan,
) -> Result<(), StandingProgramRuntimeError> {
    catalog
        .validate()
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity { field: "catalog" })?;
    let expected_input = catalog_input_relation_schema(catalog).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_schema",
        }
    })?;
    if &expected_input != input {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_schema",
        });
    }
    let [key, value] = output.columns.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.columns",
        });
    };
    let key_column = catalog_primary_key_column(catalog)?;
    let value_column = catalog_column_by_id(catalog, &plan.value_column_id)?;
    let ordering_column = catalog_column_by_id(catalog, &plan.ordering_column_id)?;
    if plan.input_relation_id != catalog.relation_schema.relation_id
        || plan.key_column_id != key_column.column_id
        || ordering_column.column_id == catalog.relation_schema.weight_column_id
        || value_column.column_id == catalog.relation_schema.weight_column_id
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "latest_by_key_plan",
        });
    }
    if !matches!(
        ordering_column.physical_arrow_type,
        ArrowPhysicalTypeV1::Int64
            | ArrowPhysicalTypeV1::Date32
            | ArrowPhysicalTypeV1::TimestampNanosecond { .. }
    ) {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "latest_by_key_plan.ordering_column",
        });
    }
    let expected_key_type = sql_type_from_catalog_column(key_column)?;
    let expected_value_type = sql_type_from_catalog_column(value_column)?;
    if output.primary_key != vec![key.name.clone()]
        || key.name != key_column.name
        || key.data_type != expected_key_type
        || key.nullable
        || value.name != plan.output_value_column_id
        || value.data_type != expected_value_type
        || value.nullable
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema",
        });
    }
    Ok(())
}

fn validate_tumbling_supported_schemas(
    catalog: &VelorixRelationCatalogV1,
    input: &RelationSchema,
    output: &RelationSchema,
    plan: &SupportedTumblingWindowPlan,
) -> Result<(), StandingProgramRuntimeError> {
    catalog
        .validate()
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity { field: "catalog" })?;
    let expected_input = catalog_input_relation_schema(catalog).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_schema",
        }
    })?;
    if &expected_input != input {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_schema",
        });
    }
    let [key, window_start, window_end, aggregate_columns @ ..] = output.columns.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.columns",
        });
    };
    let key_column = catalog_primary_key_column(catalog)?;
    let event_time_column = catalog_column_by_id(catalog, &plan.event_time_column_id)?;
    let value_column = catalog_column_by_id(catalog, &plan.sum_value_column_id)?;
    let Some(declared_event_time_column_id) = &catalog.relation_schema.event_time_column_id else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "tumbling_window_plan.event_time_column",
        });
    };
    if plan.input_relation_id != catalog.relation_schema.relation_id
        || plan.group_key_column_id != key_column.column_id
        || declared_event_time_column_id != &plan.event_time_column_id
        || event_time_column.column_id == catalog.relation_schema.weight_column_id
        || plan.window_size_ns <= 0
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "tumbling_window_plan",
        });
    }
    match event_time_column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Date32
        | ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {}
        _ => {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "tumbling_window_plan.event_time_column",
            });
        }
    }
    if !matches!(value_column.physical_arrow_type, ArrowPhysicalTypeV1::Int64) {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "tumbling_window_plan.value_column",
        });
    }
    let expected_key_type = sql_type_from_catalog_column(key_column)?;
    if output.primary_key
        != vec![
            key.name.clone(),
            window_start.name.clone(),
            window_end.name.clone(),
        ]
        || key.name != key_column.name
        || key.data_type != expected_key_type
        || key.nullable
        || window_start.name != plan.window_start_output_column_id
        || window_start.data_type != SqlDataType::Int64
        || window_start.nullable
        || window_end.name != plan.window_end_output_column_id
        || window_end.data_type != SqlDataType::Int64
        || window_end.nullable
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema",
        });
    }
    if aggregate_columns.len() != plan.aggregate_outputs.len() {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.columns",
        });
    }
    for (column, aggregate) in aggregate_columns.iter().zip(plan.aggregate_outputs.iter()) {
        if let Some(input_column_id) = &aggregate.input_column_id {
            if input_column_id != &plan.sum_value_column_id {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "tumbling_window_plan.aggregate_outputs",
                });
            }
        }
        if column.name != aggregate.output_column_id
            || column.data_type != aggregate_output_sql_type(catalog, aggregate)?
            || column.nullable
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "output_schema",
            });
        }
    }
    Ok(())
}

fn validate_plan_matches_catalog(
    plan: &SupportedViewPlan,
    catalog: &VelorixRelationCatalogV1,
) -> Result<(), StandingProgramRuntimeError> {
    if plan.input_relation_id != catalog.relation_schema.relation_id
        || plan.group_key_column_id != catalog_primary_key_column(catalog)?.column_id
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan",
        });
    }
    aggregate_value_mode_for_plan(catalog, plan)?;
    let aggregate_outputs = supported_view_plan_aggregate_outputs(plan);
    for output in &aggregate_outputs {
        aggregate_output_sql_type(catalog, output)?;
        if let Some(input_column_id) = &output.input_column_id {
            if input_column_id != &plan.sum_value_column_id {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.aggregate_outputs",
                });
            }
        }
    }
    if let Some(predicate) = &plan.predicate {
        let column = catalog_column(catalog, &predicate.column_id)?;
        if column.column_id != catalog_primary_key_column(catalog)?.column_id
            && column.column_id != plan.sum_value_column_id
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.predicate.column",
            });
        }
    }
    Ok(())
}

fn validate_view_sql_hash(
    identity: &StandingProgramIdentity,
    view_sql: &str,
) -> Result<(), StandingProgramRuntimeError> {
    let sql_hash = stable_bytes_hash(view_sql.as_bytes());
    if sql_hash == identity.sql_hash {
        Ok(())
    } else {
        Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_sql",
        })
    }
}

fn catalog_column<'a>(
    catalog: &'a VelorixRelationCatalogV1,
    column_id: &str,
) -> Result<&'a velorix_core::relation::RelationColumnV1, StandingProgramRuntimeError> {
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == column_id)
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.predicate",
        })
}

fn predicate_matches_record(
    predicate: &RowPredicate,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedViewPlan,
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    let column = catalog_column(catalog, &predicate.column_id)?;
    let actual = if column.column_id == catalog_primary_key_column(catalog)?.column_id {
        record.key.as_json()
    } else if column.column_id == plan.sum_value_column_id {
        record.value.as_json()
    } else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.predicate.column",
        });
    };
    compare_catalog_scalar(column, actual, predicate.op, &predicate.literal)
}

fn filter_delta_batch_for_plan(
    delta: &DeltaBatch,
    plan: &SupportedViewPlan,
    catalog: &VelorixRelationCatalogV1,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let Some(predicate) = &plan.predicate else {
        return Ok(delta.clone());
    };
    let mut records = Vec::new();
    for record in delta.records() {
        if predicate_matches_record(predicate, catalog, plan, record)? {
            records.push(record.clone());
        }
    }
    Ok(DeltaBatch::from_records(records))
}

impl TumblingWindowState {
    fn closed_delta(
        &self,
        plan: &SupportedTumblingWindowPlan,
        watermark: Option<i64>,
    ) -> DeltaBatch {
        let Some(watermark) = watermark else {
            return DeltaBatch::default();
        };
        DeltaBatch::from_records(
            self.rows
                .values()
                .filter(|row| row.window_end_ns <= watermark && tumbling_row_is_live(row, plan))
                .map(|row| {
                    let key = Value::Array(vec![
                        row.group_key.clone(),
                        Value::Number(JsonNumber::from(row.window_start_ns)),
                        Value::Number(JsonNumber::from(row.window_end_ns)),
                    ]);
                    let mut value = serde_json::Map::new();
                    for aggregate in &plan.aggregate_outputs {
                        value.insert(
                            aggregate.output_column_id.clone(),
                            tumbling_project_aggregate_value(row, aggregate),
                        );
                    }
                    DeltaRecord::new(
                        DeltaKey::from_json(key),
                        DeltaValue::from_json(Value::Object(value)),
                        1,
                    )
                })
                .collect::<Vec<_>>(),
        )
    }
}

fn tumbling_row_is_live(row: &TumblingWindowStateRow, plan: &SupportedTumblingWindowPlan) -> bool {
    if row.net_count > 0 {
        return true;
    }
    plan.aggregate_outputs
        .iter()
        .find(|aggregate| aggregate.function == LogicalPlanAggregateFunctionV1::Count)
        .and_then(|aggregate| row.values.get(&aggregate.output_column_id))
        .is_some_and(|count| *count > 0)
}

fn tumbling_project_aggregate_value(
    row: &TumblingWindowStateRow,
    aggregate: &SupportedAggregateOutput,
) -> Value {
    match aggregate.function {
        LogicalPlanAggregateFunctionV1::Sum | LogicalPlanAggregateFunctionV1::Count => {
            Value::Number(JsonNumber::from(
                row.values
                    .get(&aggregate.output_column_id)
                    .copied()
                    .unwrap_or_default(),
            ))
        }
        LogicalPlanAggregateFunctionV1::Avg => {
            let sum = row
                .avg_sums
                .get(&aggregate.output_column_id)
                .copied()
                .unwrap_or_default();
            let count = row
                .avg_counts
                .get(&aggregate.output_column_id)
                .copied()
                .unwrap_or_default();
            JsonNumber::from_f64(sum as f64 / count as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        LogicalPlanAggregateFunctionV1::Min => row
            .extrema_values
            .get(&aggregate.output_column_id)
            .and_then(|values| values.first_key_value().map(|(value, _)| *value))
            .map(JsonNumber::from)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        LogicalPlanAggregateFunctionV1::Max => row
            .extrema_values
            .get(&aggregate.output_column_id)
            .and_then(|values| values.last_key_value().map(|(value, _)| *value))
            .map(JsonNumber::from)
            .map(Value::Number)
            .unwrap_or(Value::Null),
    }
}

fn update_tumbling_extrema(
    row: &mut TumblingWindowStateRow,
    aggregate: &SupportedAggregateOutput,
    amount: i64,
    weight: i64,
) -> Result<(), StandingProgramRuntimeError> {
    let values = row
        .extrema_values
        .entry(aggregate.output_column_id.clone())
        .or_default();
    let entry = values.entry(amount).or_default();
    *entry = entry
        .checked_add(weight)
        .ok_or_else(invalid_runtime_state)?;
    if *entry < 0 {
        return Err(invalid_runtime_state());
    }
    if *entry == 0 {
        values.remove(&amount);
    }
    Ok(())
}

fn apply_tumbling_input(
    state: &mut TumblingWindowState,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedTumblingWindowPlan,
    current_event_time_frontiers: &[InputEventTimeFrontier],
    input: &RelationInputBatch,
) -> Result<(), StandingProgramRuntimeError> {
    let Some(watermark) = &input.event_time_watermark else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "tumbling_event_time_input_batch",
        });
    };
    if watermark.event_time_column_id != plan.event_time_column_id {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "tumbling_event_time_input_batch",
        });
    }
    let current_watermark = current_partition_watermark(current_event_time_frontiers, input);
    let key_column = catalog_primary_key_column(catalog)?;
    let value_column = catalog_column_by_id(catalog, &plan.sum_value_column_id)?;
    let event_time_column = catalog_column_by_id(catalog, &plan.event_time_column_id)?;
    let weight_column = catalog_column_by_id(catalog, &catalog.relation_schema.weight_column_id)?;

    for batch in &input.batches {
        let key_index = batch_column_index(batch, &key_column.name)?;
        let value_index = batch_column_index(batch, &value_column.name)?;
        let event_time_index = batch_column_index(batch, &event_time_column.name)?;
        let weight_index = batch_column_index(batch, &weight_column.name)?;
        for row_index in 0..batch.num_rows() {
            let event_time_ns = batch_event_time_ns(
                batch,
                event_time_index,
                &event_time_column.physical_arrow_type,
                row_index,
            )?;
            if current_watermark.is_some_and(|watermark| event_time_ns < watermark) {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "tumbling_event_time_input_batch",
                });
            }
            let group_key =
                batch_key_value(batch, key_index, &key_column.physical_arrow_type, row_index)?;
            let amount = batch_int64_value(batch, value_index, row_index)?;
            let weight = batch_int64_value(batch, weight_index, row_index)?;
            let window_start_ns =
                event_time_ns.div_euclid(plan.window_size_ns) * plan.window_size_ns;
            let window_end_ns = window_start_ns
                .checked_add(plan.window_size_ns)
                .ok_or_else(invalid_runtime_state)?;
            let state_key = canonical_json(&Value::Array(vec![
                group_key.clone(),
                Value::Number(JsonNumber::from(window_start_ns)),
                Value::Number(JsonNumber::from(window_end_ns)),
            ]));
            let row = state
                .rows
                .entry(state_key)
                .or_insert_with(|| TumblingWindowStateRow {
                    group_key,
                    window_start_ns,
                    window_end_ns,
                    net_count: 0,
                    avg_sums: BTreeMap::new(),
                    avg_counts: BTreeMap::new(),
                    extrema_values: BTreeMap::new(),
                    values: BTreeMap::new(),
                });
            row.net_count = row
                .net_count
                .checked_add(weight)
                .ok_or_else(invalid_runtime_state)?;
            for aggregate in &plan.aggregate_outputs {
                match aggregate.function {
                    LogicalPlanAggregateFunctionV1::Sum => {
                        let entry = row
                            .values
                            .entry(aggregate.output_column_id.clone())
                            .or_default();
                        *entry = entry
                            .checked_add(
                                amount
                                    .checked_mul(weight)
                                    .ok_or_else(invalid_runtime_state)?,
                            )
                            .ok_or_else(invalid_runtime_state)?;
                    }
                    LogicalPlanAggregateFunctionV1::Count => {
                        let entry = row
                            .values
                            .entry(aggregate.output_column_id.clone())
                            .or_default();
                        *entry = entry
                            .checked_add(weight)
                            .ok_or_else(invalid_runtime_state)?;
                    }
                    LogicalPlanAggregateFunctionV1::Avg => {
                        let weighted_amount = amount
                            .checked_mul(weight)
                            .ok_or_else(invalid_runtime_state)?;
                        let sum = row
                            .avg_sums
                            .entry(aggregate.output_column_id.clone())
                            .or_default();
                        *sum = sum
                            .checked_add(weighted_amount)
                            .ok_or_else(invalid_runtime_state)?;
                        let count = row
                            .avg_counts
                            .entry(aggregate.output_column_id.clone())
                            .or_default();
                        *count = count
                            .checked_add(weight)
                            .ok_or_else(invalid_runtime_state)?;
                    }
                    LogicalPlanAggregateFunctionV1::Min | LogicalPlanAggregateFunctionV1::Max => {
                        update_tumbling_extrema(row, aggregate, amount, weight)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn current_partition_watermark(
    frontiers: &[InputEventTimeFrontier],
    input: &RelationInputBatch,
) -> Option<i64> {
    let watermark = input.event_time_watermark.as_ref()?;
    frontiers
        .iter()
        .find(|frontier| {
            frontier.relation_id == input.relation_id
                && frontier.relation_version == input.relation_version
                && frontier.schema_fingerprint == input.schema_fingerprint
                && frontier.stream_id == watermark.stream_id
                && frontier.partition_id == watermark.partition_id
        })
        .map(|frontier| frontier.watermark_ns)
}

fn min_event_time_watermark(frontiers: &[InputEventTimeFrontier]) -> Option<i64> {
    frontiers.iter().map(|frontier| frontier.watermark_ns).min()
}

fn batch_column_index(
    batch: &RecordBatch,
    column_name: &str,
) -> Result<usize, StandingProgramRuntimeError> {
    batch
        .schema()
        .column_with_name(column_name)
        .map(|(index, _)| index)
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "tumbling_event_time_input_batch",
        })
}

fn batch_key_value(
    batch: &RecordBatch,
    column_index: usize,
    physical_type: &ArrowPhysicalTypeV1,
    row_index: usize,
) -> Result<Value, StandingProgramRuntimeError> {
    match physical_type {
        ArrowPhysicalTypeV1::Utf8 => Ok(Value::String(
            batch
                .column(column_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(invalid_runtime_state)?
                .value(row_index)
                .to_string(),
        )),
        ArrowPhysicalTypeV1::Int64 => Ok(Value::Number(JsonNumber::from(batch_int64_value(
            batch,
            column_index,
            row_index,
        )?))),
        ArrowPhysicalTypeV1::Boolean => Ok(Value::Bool(
            batch
                .column(column_index)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(invalid_runtime_state)?
                .value(row_index),
        )),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "tumbling_window_plan.group_key_column",
        }),
    }
}

fn batch_int64_value(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
) -> Result<i64, StandingProgramRuntimeError> {
    Ok(batch
        .column(column_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(invalid_runtime_state)?
        .value(row_index))
}

fn batch_event_time_ns(
    batch: &RecordBatch,
    column_index: usize,
    physical_type: &ArrowPhysicalTypeV1,
    row_index: usize,
) -> Result<i64, StandingProgramRuntimeError> {
    match physical_type {
        ArrowPhysicalTypeV1::Int64 => batch_int64_value(batch, column_index, row_index),
        ArrowPhysicalTypeV1::Date32 => {
            let days = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(invalid_runtime_state)?
                .value(row_index);
            Ok(i64::from(days) * 86_400_000_000_000)
        }
        ArrowPhysicalTypeV1::TimestampNanosecond { .. } => Ok(batch
            .column(column_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(invalid_runtime_state)?
            .value(row_index)),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "tumbling_window_plan.event_time_column",
        }),
    }
}

impl LatestByKeyState {
    fn apply_delta(
        &mut self,
        delta: &DeltaBatch,
        plan: &SupportedLatestByKeyPlan,
    ) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        let mut affected = BTreeMap::new();
        for record in delta.records() {
            let key_canonical = canonical_json(record.key.as_json());
            affected
                .entry(key_canonical.clone())
                .or_insert_with(|| self.current_record_for_key(&key_canonical, plan));
            self.apply_record(record)?;
        }

        let mut output = Vec::new();
        for (key_canonical, before) in affected {
            let after = self.current_record_for_key(&key_canonical, plan);
            if before != after {
                if let Some(before) = before {
                    output.push(before.inverse().map_err(|_| invalid_runtime_state())?);
                }
                if let Some(after) = after {
                    output.push(after);
                }
            }
        }
        Ok(DeltaBatch::from_records(output))
    }

    fn apply_record(&mut self, record: &DeltaRecord) -> Result<(), StandingProgramRuntimeError> {
        let key_canonical = canonical_json(record.key.as_json());
        let (ordering, value) = latest_delta_record_parts(record)?;
        let value_canonical = canonical_json(&value);
        let key_rows = self
            .rows
            .entry(key_canonical.clone())
            .or_insert_with(|| LatestKeyRows {
                key: record.key.as_json().clone(),
                values: BTreeMap::new(),
            });
        let values = key_rows.values.entry(ordering).or_default();
        let entry = values
            .entry(value_canonical.clone())
            .or_insert_with(|| LatestValueCount { value, weight: 0 });
        entry.weight = entry
            .weight
            .checked_add(record.weight)
            .ok_or_else(invalid_runtime_state)?;
        if entry.weight == 0 {
            values.remove(&value_canonical);
        }
        if values.is_empty() {
            key_rows.values.remove(&ordering);
        }
        if key_rows.values.is_empty() {
            self.rows.remove(&key_canonical);
        }
        Ok(())
    }

    fn current_record_for_key(
        &self,
        key_canonical: &str,
        plan: &SupportedLatestByKeyPlan,
    ) -> Option<DeltaRecord> {
        let key_rows = self.rows.get(key_canonical)?;
        let (_ordering, values) = key_rows.values.iter().next_back()?;
        let (_value_canonical, value) = values
            .iter()
            .filter(|(_, value)| value.weight > 0)
            .next_back()?;
        Some(DeltaRecord::new(
            velorix_core::delta::DeltaKey::from_json(key_rows.key.clone()),
            DeltaValue::from_json(serde_json::json!({
                plan.output_value_column_id.clone(): value.value.clone(),
            })),
            1,
        ))
    }

    fn materialized_delta(&self, plan: &SupportedLatestByKeyPlan) -> DeltaBatch {
        DeltaBatch::from_records(
            self.rows
                .keys()
                .filter_map(|key| self.current_record_for_key(key, plan)),
        )
    }

    fn to_checkpoint_rows(&self) -> Vec<LatestByKeyCheckpointRow> {
        let mut rows = Vec::new();
        for key_rows in self.rows.values() {
            for (ordering, values) in &key_rows.values {
                for value in values.values() {
                    if value.weight != 0 {
                        rows.push(LatestByKeyCheckpointRow {
                            key: key_rows.key.clone(),
                            ordering: *ordering,
                            value: value.value.clone(),
                            weight: value.weight,
                        });
                    }
                }
            }
        }
        rows
    }

    fn from_checkpoint_rows(
        rows: Vec<LatestByKeyCheckpointRow>,
    ) -> Result<Self, StandingProgramRuntimeError> {
        let mut state = Self::default();
        for row in rows {
            if row.weight == 0 {
                return Err(invalid_checkpoint());
            }
            let key_canonical = canonical_json(&row.key);
            let value_canonical = canonical_json(&row.value);
            let key_rows = state
                .rows
                .entry(key_canonical)
                .or_insert_with(|| LatestKeyRows {
                    key: row.key,
                    values: BTreeMap::new(),
                });
            let values = key_rows.values.entry(row.ordering).or_default();
            values.insert(
                value_canonical,
                LatestValueCount {
                    value: row.value,
                    weight: row.weight,
                },
            );
        }
        Ok(state)
    }
}

fn latest_delta_record_parts(
    record: &DeltaRecord,
) -> Result<(i128, Value), StandingProgramRuntimeError> {
    let object = record
        .value
        .as_json()
        .as_object()
        .ok_or_else(invalid_runtime_state)?;
    let ordering = object
        .get("ordering")
        .ok_or_else(invalid_runtime_state)
        .and_then(latest_ordering_value)?;
    let value = object
        .get("value")
        .cloned()
        .ok_or_else(invalid_runtime_state)?;
    Ok((ordering, value))
}

fn latest_ordering_value(value: &Value) -> Result<i128, StandingProgramRuntimeError> {
    value
        .as_i64()
        .map(i128::from)
        .ok_or_else(invalid_runtime_state)
}

fn compare_catalog_scalar(
    column: &velorix_core::relation::RelationColumnV1,
    actual: &Value,
    op: PredicateOp,
    literal: &Value,
) -> Result<bool, StandingProgramRuntimeError> {
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Date32
        | ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {
            return Ok(compare_ord(
                actual_i128(actual)?,
                op,
                literal_i128(literal)?,
            ));
        }
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => {
            let actual = decimal_value_i128(actual, *precision, *scale)?;
            let expected = decimal_value_i128(literal, *precision, *scale)?;
            return Ok(compare_ord(actual, op, expected));
        }
        ArrowPhysicalTypeV1::Float64 => {
            return Ok(compare_ord(actual_f64(actual)?, op, literal_f64(literal)?));
        }
        _ => {}
    }

    match (actual, literal) {
        (Value::Number(actual), Value::Number(expected)) => Ok(compare_ord(
            actual.as_i64().ok_or_else(invalid_runtime_state)?,
            op,
            expected
                .as_i64()
                .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                })?,
        )),
        (Value::String(actual), Value::String(expected)) => Ok(compare_ord(actual, op, expected)),
        (Value::Bool(actual), Value::Bool(expected)) => Ok(match op {
            PredicateOp::Eq => actual == expected,
            PredicateOp::NotEq => actual != expected,
            _ => {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.op",
                })
            }
        }),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.predicate.literal",
        }),
    }
}

fn actual_i128(value: &Value) -> Result<i128, StandingProgramRuntimeError> {
    value
        .as_i64()
        .map(i128::from)
        .ok_or_else(invalid_runtime_state)
}

fn literal_i128(value: &Value) -> Result<i128, StandingProgramRuntimeError> {
    match value {
        Value::Number(value) => value.as_i64().map(i128::from).ok_or(
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.predicate.literal",
            },
        ),
        Value::String(value) => {
            value
                .parse::<i128>()
                .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                })
        }
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.predicate.literal",
        }),
    }
}

fn actual_f64(value: &Value) -> Result<f64, StandingProgramRuntimeError> {
    value.as_f64().ok_or_else(invalid_runtime_state)
}

fn literal_f64(value: &Value) -> Result<f64, StandingProgramRuntimeError> {
    match value {
        Value::Number(value) => {
            value
                .as_f64()
                .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                })
        }
        Value::String(value) => {
            value
                .parse::<f64>()
                .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                })
        }
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.predicate.literal",
        }),
    }
}

fn decimal_value_i128(
    value: &Value,
    precision: u8,
    scale: u8,
) -> Result<i128, StandingProgramRuntimeError> {
    match value {
        Value::String(value) => parse_decimal128(value, precision, scale).ok_or(
            StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.predicate.literal",
            },
        ),
        Value::Number(value) => {
            let Some(value) = value.as_i64() else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                });
            };
            let factor = 10_i128.checked_pow(u32::from(scale)).ok_or(
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                },
            )?;
            i128::from(value).checked_mul(factor).ok_or(
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                },
            )
        }
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.predicate.literal",
        }),
    }
}

fn compare_ord<T: PartialOrd + PartialEq>(actual: T, op: PredicateOp, expected: T) -> bool {
    match op {
        PredicateOp::Eq => actual == expected,
        PredicateOp::NotEq => actual != expected,
        PredicateOp::Gt => actual > expected,
        PredicateOp::GtEq => actual >= expected,
        PredicateOp::Lt => actual < expected,
        PredicateOp::LtEq => actual <= expected,
    }
}

fn catalog_primary_key_column(
    catalog: &VelorixRelationCatalogV1,
) -> Result<&velorix_core::relation::RelationColumnV1, StandingProgramRuntimeError> {
    let [primary_key] = catalog.relation_schema.primary_key_column_ids.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "catalog.primary_key",
        });
    };
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| &column.column_id == primary_key)
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "catalog.primary_key",
        })
}

fn catalog_column_by_id<'a>(
    catalog: &'a VelorixRelationCatalogV1,
    column_id: &str,
) -> Result<&'a velorix_core::relation::RelationColumnV1, StandingProgramRuntimeError> {
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == column_id)
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "relation_column",
        })
}

fn aggregate_value_mode_for_plan(
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedViewPlan,
) -> Result<AggregateValueMode, StandingProgramRuntimeError> {
    aggregate_value_mode_for_column_id(catalog, &plan.sum_value_column_id)
}

fn plan_tracks_extrema(plan: &SupportedViewPlan) -> bool {
    supported_view_plan_aggregate_outputs(plan)
        .iter()
        .any(|output| {
            matches!(
                output.function,
                LogicalPlanAggregateFunctionV1::Min | LogicalPlanAggregateFunctionV1::Max
            )
        })
}

fn aggregate_value_mode_for_column_id(
    catalog: &VelorixRelationCatalogV1,
    column_id: &str,
) -> Result<AggregateValueMode, StandingProgramRuntimeError> {
    match &catalog_column(catalog, column_id)?.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 => Ok(AggregateValueMode::Integer),
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => {
            Ok(AggregateValueMode::Decimal128 {
                precision: *precision,
                scale: *scale,
            })
        }
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "catalog.value_column",
        }),
    }
}

fn aggregate_sum_sql_type_for_column(
    catalog: &VelorixRelationCatalogV1,
    column_id: &str,
) -> Result<SqlDataType, StandingProgramRuntimeError> {
    match &catalog_column(catalog, column_id)?.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 => Ok(SqlDataType::Int64),
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => Ok(SqlDataType::Decimal {
            precision: *precision,
            scale: *scale,
        }),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "catalog.value_column",
        }),
    }
}

fn aggregate_sum_sql_type_for_column_id(
    catalog: &VelorixRelationCatalogV1,
    column_id: &str,
) -> Result<SqlDataType, StandingProgramRuntimeError> {
    match &catalog_column(catalog, column_id)?.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 => Ok(SqlDataType::Int64),
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => Ok(SqlDataType::Decimal {
            precision: *precision,
            scale: *scale,
        }),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "catalog.value_column",
        }),
    }
}

fn aggregate_output_sql_type(
    catalog: &VelorixRelationCatalogV1,
    output: &SupportedAggregateOutput,
) -> Result<SqlDataType, StandingProgramRuntimeError> {
    match output.function {
        LogicalPlanAggregateFunctionV1::Sum => {
            let Some(column_id) = &output.input_column_id else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.aggregate_outputs.input_column_id",
                });
            };
            aggregate_sum_sql_type_for_column_id(catalog, column_id)
        }
        LogicalPlanAggregateFunctionV1::Count => Ok(SqlDataType::Int64),
        LogicalPlanAggregateFunctionV1::Avg => {
            let Some(column_id) = &output.input_column_id else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.aggregate_outputs.input_column_id",
                });
            };
            match &catalog_column(catalog, column_id)?.physical_arrow_type {
                ArrowPhysicalTypeV1::Int64 => Ok(SqlDataType::Float64),
                _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.aggregate_outputs.avg",
                }),
            }
        }
        LogicalPlanAggregateFunctionV1::Min | LogicalPlanAggregateFunctionV1::Max => {
            let Some(column_id) = &output.input_column_id else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.aggregate_outputs.input_column_id",
                });
            };
            aggregate_sum_sql_type_for_column_id(catalog, column_id)
        }
    }
}

fn sql_type_from_catalog_column(
    column: &velorix_core::relation::RelationColumnV1,
) -> Result<SqlDataType, StandingProgramRuntimeError> {
    Ok(match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Boolean => SqlDataType::Bool,
        ArrowPhysicalTypeV1::Int8 => SqlDataType::Int8,
        ArrowPhysicalTypeV1::Int16 => SqlDataType::Int16,
        ArrowPhysicalTypeV1::Int32 => SqlDataType::Int32,
        ArrowPhysicalTypeV1::Int64 => SqlDataType::Int64,
        ArrowPhysicalTypeV1::UInt8 => SqlDataType::UInt8,
        ArrowPhysicalTypeV1::UInt16 => SqlDataType::UInt16,
        ArrowPhysicalTypeV1::UInt32 => SqlDataType::UInt32,
        ArrowPhysicalTypeV1::UInt64 => SqlDataType::UInt64,
        ArrowPhysicalTypeV1::Float32 => SqlDataType::Float32,
        ArrowPhysicalTypeV1::Float64 => SqlDataType::Float64,
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => SqlDataType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        ArrowPhysicalTypeV1::Utf8 | ArrowPhysicalTypeV1::DictionaryUtf8 { .. } => SqlDataType::Utf8,
        ArrowPhysicalTypeV1::Binary => SqlDataType::Varbinary,
        ArrowPhysicalTypeV1::JsonUtf8 => SqlDataType::Json,
        ArrowPhysicalTypeV1::Date32 => SqlDataType::Date,
        ArrowPhysicalTypeV1::Time64Nanosecond => SqlDataType::Time,
        ArrowPhysicalTypeV1::TimestampNanosecond { timezone } => SqlDataType::Timestamp {
            timezone: timezone.clone(),
        },
        ArrowPhysicalTypeV1::List { element_type } => SqlDataType::Array {
            element_type: Box::new(sql_type_from_arrow_physical_type(element_type)?),
        },
        ArrowPhysicalTypeV1::Struct { fields } => SqlDataType::Struct {
            fields: fields
                .iter()
                .map(|field| {
                    Ok(SqlStructField {
                        name: field.name.clone(),
                        data_type: sql_type_from_arrow_physical_type(&field.physical_arrow_type)?,
                        nullable: field.nullable,
                    })
                })
                .collect::<Result<Vec<_>, StandingProgramRuntimeError>>()?,
        },
        ArrowPhysicalTypeV1::Map {
            key_type,
            value_type,
        } => SqlDataType::Map {
            key_type: Box::new(sql_type_from_arrow_physical_type(key_type)?),
            value_type: Box::new(sql_type_from_arrow_physical_type(value_type)?),
        },
    })
}

fn sql_type_from_arrow_physical_type(
    physical_type: &ArrowPhysicalTypeV1,
) -> Result<SqlDataType, StandingProgramRuntimeError> {
    let column = velorix_core::relation::RelationColumnV1 {
        column_id: "__type".to_string(),
        name: "__type".to_string(),
        logical_type: velorix_core::relation::VelorixLogicalTypeV1::Utf8,
        physical_arrow_type: physical_type.clone(),
        nullable: true,
        ordinal: 0,
        semantic_role: RelationSemanticRoleV1::Metadata,
    };
    sql_type_from_catalog_column(&column)
}

fn materialized_delta_to_record_batch(
    output_schema: &RelationSchema,
    state: &DeltaBatch,
    aggregate_outputs: Option<&[SupportedAggregateOutput]>,
) -> Result<RecordBatch, StandingProgramRuntimeError> {
    let [key_column, aggregate_columns @ ..] = output_schema.columns.as_slice() else {
        return Err(invalid_runtime_state());
    };
    let default_outputs;
    let aggregate_outputs = if let Some(aggregate_outputs) = aggregate_outputs {
        aggregate_outputs
    } else {
        default_outputs = fixed_sum_count_outputs();
        default_outputs.as_slice()
    };
    if aggregate_columns.len() != aggregate_outputs.len() {
        return Err(invalid_runtime_state());
    }
    let rows = state.net_rows().map_err(|_| invalid_runtime_state())?;
    let mut keys = Vec::new();
    let mut aggregate_values = vec![Vec::new(); aggregate_outputs.len()];
    for row in rows {
        if row.weight != 1 {
            return Err(invalid_runtime_state());
        }
        let value = row
            .value
            .as_json()
            .as_object()
            .ok_or_else(invalid_runtime_state)?;
        keys.push(row.key.as_json().clone());
        for (index, aggregate) in aggregate_outputs.iter().enumerate() {
            aggregate_values[index].push(project_aggregate_value(value, aggregate)?);
        }
    }

    let mut fields = Vec::with_capacity(output_schema.columns.len());
    fields.push(Field::new(
        key_column.name.as_str(),
        arrow_data_type(&key_column.data_type)?,
        false,
    ));
    for column in aggregate_columns {
        fields.push(Field::new(
            column.name.as_str(),
            arrow_data_type(&column.data_type)?,
            false,
        ));
    }
    let mut arrays = Vec::with_capacity(output_schema.columns.len());
    arrays.push(key_array(&key_column.data_type, &keys)?);
    for (column, values) in aggregate_columns.iter().zip(aggregate_values.iter()) {
        arrays.push(output_value_array(&column.data_type, values)?);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|_| invalid_runtime_state())
}

fn materialized_delta_page_batch(
    output_schema: &RelationSchema,
    published_output: &DeltaBatch,
    logical_epoch: u64,
    page: SnapshotPageRequest,
    aggregate_outputs: Option<&[SupportedAggregateOutput]>,
) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
    if let Some(requested) = page.committed_epoch {
        if requested != logical_epoch {
            return Err(StandingProgramRuntimeError::UnavailableCommittedEpoch {
                requested,
                current: logical_epoch,
            });
        }
    }
    let mut rows = published_output
        .net_rows()
        .map_err(|_| invalid_runtime_state())?;
    rows.sort_by(|left, right| {
        canonical_json(left.key.as_json()).cmp(&canonical_json(right.key.as_json()))
    });
    if let Some(page_token) = &page.page_token {
        rows.retain(|row| canonical_json(row.key.as_json()) > *page_token);
    }

    let limit = page.max_rows.unwrap_or(rows.len());
    if limit == 0 {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "snapshot_page.max_rows",
        });
    }
    let has_next = rows.len() > limit;
    if has_next {
        rows.truncate(limit);
    }
    let next_page_token = if has_next {
        rows.last().map(|row| canonical_json(row.key.as_json()))
    } else {
        None
    };
    materialized_delta_to_record_batch(
        output_schema,
        &DeltaBatch::from_records(rows),
        aggregate_outputs,
    )
    .map(|batch| (batch, next_page_token))
}

fn materialized_tumbling_delta_to_record_batch(
    output_schema: &RelationSchema,
    state: &DeltaBatch,
    aggregate_outputs: &[SupportedAggregateOutput],
) -> Result<RecordBatch, StandingProgramRuntimeError> {
    let [key_column, window_start_column, window_end_column, aggregate_columns @ ..] =
        output_schema.columns.as_slice()
    else {
        return Err(invalid_runtime_state());
    };
    if aggregate_columns.len() != aggregate_outputs.len() {
        return Err(invalid_runtime_state());
    }
    let rows = state.net_rows().map_err(|_| invalid_runtime_state())?;
    let mut group_keys = Vec::new();
    let mut window_starts = Vec::new();
    let mut window_ends = Vec::new();
    let mut aggregate_values = vec![Vec::new(); aggregate_columns.len()];
    for row in rows {
        if row.weight != 1 {
            return Err(invalid_runtime_state());
        }
        let key_values = row
            .key
            .as_json()
            .as_array()
            .ok_or_else(invalid_runtime_state)?;
        let [group_key, window_start, window_end] = key_values.as_slice() else {
            return Err(invalid_runtime_state());
        };
        let value = row
            .value
            .as_json()
            .as_object()
            .ok_or_else(invalid_runtime_state)?;
        group_keys.push(group_key.clone());
        window_starts.push(window_start.clone());
        window_ends.push(window_end.clone());
        for (index, aggregate) in aggregate_outputs.iter().enumerate() {
            aggregate_values[index].push(
                value
                    .get(aggregate.output_column_id.as_str())
                    .cloned()
                    .ok_or_else(invalid_runtime_state)?,
            );
        }
    }

    let mut fields = Vec::with_capacity(output_schema.columns.len());
    for column in [key_column, window_start_column, window_end_column] {
        fields.push(Field::new(
            column.name.as_str(),
            arrow_data_type(&column.data_type)?,
            false,
        ));
    }
    for column in aggregate_columns {
        fields.push(Field::new(
            column.name.as_str(),
            arrow_data_type(&column.data_type)?,
            false,
        ));
    }
    let mut arrays = Vec::with_capacity(output_schema.columns.len());
    arrays.push(key_array(&key_column.data_type, &group_keys)?);
    arrays.push(output_value_array(
        &window_start_column.data_type,
        &window_starts,
    )?);
    arrays.push(output_value_array(
        &window_end_column.data_type,
        &window_ends,
    )?);
    for (column, values) in aggregate_columns.iter().zip(aggregate_values.iter()) {
        arrays.push(output_value_array(&column.data_type, values)?);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|_| invalid_runtime_state())
}

fn materialized_tumbling_delta_page_batch(
    output_schema: &RelationSchema,
    published_output: &DeltaBatch,
    aggregate_outputs: &[SupportedAggregateOutput],
    logical_epoch: u64,
    page: SnapshotPageRequest,
) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
    if let Some(requested) = page.committed_epoch {
        if requested != logical_epoch {
            return Err(StandingProgramRuntimeError::UnavailableCommittedEpoch {
                requested,
                current: logical_epoch,
            });
        }
    }
    let mut rows = published_output
        .net_rows()
        .map_err(|_| invalid_runtime_state())?;
    rows.sort_by(|left, right| {
        canonical_json(left.key.as_json()).cmp(&canonical_json(right.key.as_json()))
    });
    if let Some(page_token) = &page.page_token {
        rows.retain(|row| canonical_json(row.key.as_json()) > *page_token);
    }

    let limit = page.max_rows.unwrap_or(rows.len());
    if limit == 0 {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "snapshot_page.max_rows",
        });
    }
    let has_next = rows.len() > limit;
    if has_next {
        rows.truncate(limit);
    }
    let next_page_token = if has_next {
        rows.last().map(|row| canonical_json(row.key.as_json()))
    } else {
        None
    };
    materialized_tumbling_delta_to_record_batch(
        output_schema,
        &DeltaBatch::from_records(rows),
        aggregate_outputs,
    )
    .map(|batch| (batch, next_page_token))
}

fn materialized_generic_delta_to_record_batch(
    output_schema: &RelationSchema,
    state: &DeltaBatch,
) -> Result<RecordBatch, StandingProgramRuntimeError> {
    let [key_column, value_columns @ ..] = output_schema.columns.as_slice() else {
        return Err(invalid_runtime_state());
    };
    if value_columns.is_empty() {
        return Err(invalid_runtime_state());
    }
    let rows = state.net_rows().map_err(|_| invalid_runtime_state())?;
    let mut keys = Vec::new();
    let mut column_values = vec![Vec::new(); value_columns.len()];
    for row in rows {
        if row.weight != 1 {
            return Err(invalid_runtime_state());
        }
        let value = row
            .value
            .as_json()
            .as_object()
            .ok_or_else(invalid_runtime_state)?;
        keys.push(row.key.as_json().clone());
        for (index, column) in value_columns.iter().enumerate() {
            column_values[index].push(
                value
                    .get(column.name.as_str())
                    .cloned()
                    .ok_or_else(invalid_runtime_state)?,
            );
        }
    }

    let mut fields = Vec::with_capacity(output_schema.columns.len());
    fields.push(Field::new(
        key_column.name.as_str(),
        arrow_data_type(&key_column.data_type)?,
        false,
    ));
    for column in value_columns {
        fields.push(Field::new(
            column.name.as_str(),
            arrow_data_type(&column.data_type)?,
            false,
        ));
    }
    let mut arrays = Vec::with_capacity(output_schema.columns.len());
    arrays.push(key_array(&key_column.data_type, &keys)?);
    for (column, values) in value_columns.iter().zip(column_values.iter()) {
        arrays.push(output_value_array(&column.data_type, values)?);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|_| invalid_runtime_state())
}

fn materialized_generic_delta_page_batch(
    output_schema: &RelationSchema,
    published_output: &DeltaBatch,
    logical_epoch: u64,
    page: SnapshotPageRequest,
) -> Result<(RecordBatch, Option<String>), StandingProgramRuntimeError> {
    if let Some(requested) = page.committed_epoch {
        if requested != logical_epoch {
            return Err(StandingProgramRuntimeError::UnavailableCommittedEpoch {
                requested,
                current: logical_epoch,
            });
        }
    }
    let mut rows = published_output
        .net_rows()
        .map_err(|_| invalid_runtime_state())?;
    rows.sort_by(|left, right| {
        canonical_json(left.key.as_json()).cmp(&canonical_json(right.key.as_json()))
    });
    if let Some(page_token) = &page.page_token {
        rows.retain(|row| canonical_json(row.key.as_json()) > *page_token);
    }

    let limit = page.max_rows.unwrap_or(rows.len());
    if limit == 0 {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "snapshot_page.max_rows",
        });
    }
    let has_next = rows.len() > limit;
    if has_next {
        rows.truncate(limit);
    }
    let next_page_token = if has_next {
        rows.last().map(|row| canonical_json(row.key.as_json()))
    } else {
        None
    };
    materialized_generic_delta_to_record_batch(output_schema, &DeltaBatch::from_records(rows))
        .map(|batch| (batch, next_page_token))
}

fn apply_published_output_delta(
    current: &DeltaBatch,
    output_delta: &DeltaBatch,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let rows = current
        .combine(output_delta)
        .net_rows()
        .map_err(|_| invalid_runtime_state())?;
    let published = DeltaBatch::from_records(rows);
    validate_published_output(&published)?;
    Ok(published)
}

fn validate_published_output(output: &DeltaBatch) -> Result<(), StandingProgramRuntimeError> {
    for row in output.net_rows().map_err(|_| invalid_runtime_state())? {
        if row.weight != 1 {
            return Err(invalid_runtime_state());
        }
    }
    Ok(())
}

fn fixed_sum_count_outputs() -> Vec<SupportedAggregateOutput> {
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

fn project_aggregate_value(
    value: &serde_json::Map<String, Value>,
    aggregate: &SupportedAggregateOutput,
) -> Result<Value, StandingProgramRuntimeError> {
    match aggregate.function {
        LogicalPlanAggregateFunctionV1::Sum => {
            value.get("sum").cloned().ok_or_else(invalid_runtime_state)
        }
        LogicalPlanAggregateFunctionV1::Count => value
            .get("count")
            .cloned()
            .ok_or_else(invalid_runtime_state),
        LogicalPlanAggregateFunctionV1::Avg => {
            let sum = value
                .get("sum")
                .and_then(Value::as_i64)
                .ok_or_else(invalid_runtime_state)?;
            let count = value
                .get("count")
                .and_then(Value::as_i64)
                .ok_or_else(invalid_runtime_state)?;
            if count == 0 {
                return Err(invalid_runtime_state());
            }
            let avg = sum as f64 / count as f64;
            JsonNumber::from_f64(avg)
                .map(Value::Number)
                .ok_or_else(invalid_runtime_state)
        }
        LogicalPlanAggregateFunctionV1::Min => {
            value.get("min").cloned().ok_or_else(invalid_runtime_state)
        }
        LogicalPlanAggregateFunctionV1::Max => {
            value.get("max").cloned().ok_or_else(invalid_runtime_state)
        }
    }
}

fn key_array(
    data_type: &SqlDataType,
    values: &[Value],
) -> Result<ArrayRef, StandingProgramRuntimeError> {
    match data_type {
        SqlDataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| value.as_str().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| value.as_i64().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Float64 => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| value.as_f64().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Bool => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| value.as_bool().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Decimal { precision, scale } => Ok(Arc::new(
            Decimal128Array::from(
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .and_then(|value| parse_decimal128(value, *precision, *scale))
                            .ok_or_else(invalid_runtime_state)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .with_precision_and_scale(
                *precision,
                i8::try_from(*scale).map_err(|_| invalid_runtime_state())?,
            )
            .map_err(|_| invalid_runtime_state())?,
        )),
        SqlDataType::Json => Ok(Arc::new(StringArray::from(
            values.iter().map(canonical_json).collect::<Vec<_>>(),
        ))),
        SqlDataType::Date => Ok(Arc::new(Date32Array::from(
            values
                .iter()
                .map(|value| {
                    value
                        .as_i64()
                        .and_then(|value| i32::try_from(value).ok())
                        .ok_or_else(invalid_runtime_state)
                })
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Timestamp { timezone } => Ok(Arc::new(
            TimestampNanosecondArray::from(
                values
                    .iter()
                    .map(|value| value.as_i64().ok_or_else(invalid_runtime_state))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .with_timezone_opt(timezone.clone()),
        )),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.key",
        }),
    }
}

fn output_value_array(
    data_type: &SqlDataType,
    values: &[Value],
) -> Result<ArrayRef, StandingProgramRuntimeError> {
    match data_type {
        SqlDataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| value.as_str().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| value.as_i64().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Bool => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| value.as_bool().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Float64 => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| value.as_f64().ok_or_else(invalid_runtime_state))
                .collect::<Result<Vec<_>, _>>()?,
        ))),
        SqlDataType::Decimal { precision, scale } => Ok(Arc::new(
            Decimal128Array::from(
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .and_then(|value| parse_decimal128(value, *precision, *scale))
                            .ok_or_else(invalid_runtime_state)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .with_precision_and_scale(
                *precision,
                i8::try_from(*scale).map_err(|_| invalid_runtime_state())?,
            )
            .map_err(|_| invalid_runtime_state())?,
        )),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.sum",
        }),
    }
}

fn arrow_data_type(data_type: &SqlDataType) -> Result<DataType, StandingProgramRuntimeError> {
    match data_type {
        SqlDataType::Utf8 => Ok(DataType::Utf8),
        SqlDataType::Int64 => Ok(DataType::Int64),
        SqlDataType::Float64 => Ok(DataType::Float64),
        SqlDataType::Bool => Ok(DataType::Boolean),
        SqlDataType::Decimal { precision, scale } => Ok(DataType::Decimal128(
            *precision,
            i8::try_from(*scale).map_err(|_| invalid_runtime_state())?,
        )),
        SqlDataType::Json => Ok(DataType::Utf8),
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Timestamp { timezone } => Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            timezone.clone().map(Into::into),
        )),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema",
        }),
    }
}

fn parse_decimal128(value: &str, precision: u8, scale: u8) -> Option<i128> {
    let (negative, digits) = match value.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, value),
    };
    let scale = usize::from(scale);
    let (whole, fractional) = match digits.split_once('.') {
        Some((whole, fractional)) => (whole, fractional),
        None if scale == 0 => (digits, ""),
        None => return None,
    };
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fractional.len() != scale
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let mut magnitude = whole.parse::<i128>().ok()?;
    let factor = 10_i128.checked_pow(scale.try_into().ok()?)?;
    magnitude = magnitude.checked_mul(factor)?;
    if scale > 0 {
        magnitude = magnitude.checked_add(fractional.parse::<i128>().ok()?)?;
    }
    if magnitude.unsigned_abs().to_string().len() > usize::from(precision) {
        return None;
    }
    if negative {
        magnitude.checked_neg()
    } else {
        Some(magnitude)
    }
}

fn invalid_checkpoint() -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "generic_checkpoint_payload",
    }
}

fn invalid_runtime_state() -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "generic_runtime_state",
    }
}

fn checkpoint_has_join_payload(checkpoint: &RuntimeCheckpoint) -> bool {
    checkpoint
        .state_payload
        .as_ref()
        .and_then(|payload| serde_json::from_str::<Value>(&payload.payload).ok())
        .and_then(|payload| {
            payload
                .get("runtime_kind")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .as_deref()
        == Some(JOIN_RUNTIME_KIND)
}

fn checkpoint_has_tumbling_window_payload(checkpoint: &RuntimeCheckpoint) -> bool {
    checkpoint
        .state_payload
        .as_ref()
        .and_then(|payload| serde_json::from_str::<Value>(&payload.payload).ok())
        .and_then(|payload| {
            payload
                .get("runtime_kind")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .as_deref()
        == Some(TUMBLING_WINDOW_RUNTIME_KIND)
}

fn checkpoint_has_latest_by_key_payload(checkpoint: &RuntimeCheckpoint) -> bool {
    checkpoint
        .state_payload
        .as_ref()
        .and_then(|payload| serde_json::from_str::<Value>(&payload.payload).ok())
        .and_then(|payload| {
            payload
                .get("runtime_kind")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .as_deref()
        == Some(LATEST_BY_KEY_RUNTIME_KIND)
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).expect("serializing JSON scalar cannot fail")
        }
        Value::Array(values) => {
            let items = values.iter().map(canonical_json).collect::<Vec<_>>();
            format!("[{}]", items.join(","))
        }
        Value::Object(object) => {
            let mut fields = object.iter().collect::<Vec<_>>();
            fields.sort_by(|left, right| left.0.cmp(right.0));
            let items = fields
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key)
                            .expect("serializing JSON object key cannot fail"),
                        canonical_json(value)
                    )
                })
                .collect::<Vec<_>>();
            format!("{{{}}}", items.join(","))
        }
    }
}
