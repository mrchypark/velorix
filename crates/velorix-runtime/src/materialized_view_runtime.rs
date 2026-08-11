use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use arrow::{
    array::{Array, BooleanArray, Date32Array, Int64Array, StringArray, TimestampNanosecondArray},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number as JsonNumber, Value};
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::{
        AggregateValueMode, EngineCheckpointPayload, IncrementalEngine, KeyedAggregateKernel,
        LogicalEpoch,
    },
    native_operator::{
        NativeAggregateOperator, NativeAntiJoinOperator, NativeBinaryJoinOperator,
        NativeDeltaOperator, NativeFullJoinOperator, NativeLeftJoinOperator,
        NativeOperatorCheckpointV1, NativeOperatorEdgeV1, NativeOperatorError, NativeOperatorGraph,
        NativeOperatorGraphCheckpointV1, NativeOperatorInputV1, NativeOperatorStateV1,
        NativeProjectOperator, NativeSemiJoinOperator,
    },
    operator::{KeyedEquiJoin, OperatorError},
    relation::{
        arrow_record_batches_to_key_latest_by_delta_batch,
        arrow_record_batches_to_key_multi_value_delta_batch,
        arrow_record_batches_to_key_nullable_value_delta_batch,
        arrow_record_batches_to_key_value_delta_batch,
        arrow_record_batches_to_key_value_delta_batch_skipping_null_values, ArrowPhysicalTypeV1,
        KeyLatestByDeltaBatchInput, RelationColumnV1, RelationSemanticRoleV1,
        VelorixRelationCatalogV1,
    },
    standing_program::{
        CausalViewCursorV1, DurableStateRoot, EpochCommit, EpochIdempotencyKey,
        InputEventTimeFrontier, MaterializedViewPage, RelationFrontier, RelationInputBatch,
        RuntimeCheckpoint, RuntimeCheckpointStatePayload, ScopedViewId, SnapshotPageRequest,
        StandingInputChangeV1, StandingProgramIdentity, StandingProgramRuntime,
        StandingProgramRuntimeError, ViewFrontier, ViewInputDeltaV1, ViewOutputBatch,
        ViewOutputDelta,
    },
    view_contract::{
        catalog_input_relation_schema, stable_bytes_hash, PublishedRelationBindingV1,
        RelationSchema, SqlDataType, SqlStructField,
    },
    view_plan::{
        lower_supported_analytic_row_number_sql_to_logical_plan,
        lower_supported_filter_project_sql_to_logical_plan,
        lower_supported_join_view_sql_to_logical_plan,
        lower_supported_latest_by_key_sql_to_logical_plan,
        lower_supported_semi_anti_join_sql_to_logical_plan, lower_supported_sql_to_logical_plan,
        lower_supported_three_input_inner_join_count_sql_to_logical_plan_with_policy,
        lower_supported_tumbling_window_sql_to_logical_plan,
        lower_supported_view_sql_to_logical_plan, supported_join_key_codec_id,
        supported_join_view_plan_aggregate_outputs, supported_join_view_plan_is_self_join,
        supported_join_view_plan_is_singleton, supported_join_view_plan_key_pairs,
        supported_join_view_plan_predicates, supported_join_view_plan_right_value_column_ids,
        supported_view_plan_aggregate_outputs, supported_view_plan_group_keys,
        supported_view_plan_is_singleton, validate_logical_view_plan,
        validate_supported_analytic_row_number_sql, validate_supported_filter_project_sql,
        validate_supported_join_view_sql, validate_supported_latest_by_key_sql,
        validate_supported_semi_anti_join_sql, validate_supported_tumbling_window_sql,
        validate_supported_view_sql, AggregateOutputPredicate, AggregateOutputPredicateExpr,
        JoinPredicateExpr, JoinRowPredicate, LogicalPlanAggregateFunctionV1,
        LogicalPlanExecutionImplementationV1, LogicalPlanLatestByKeyFunctionV1, PredicateOp,
        RowPredicate, RowPredicateExpr, SupportedAggregateInputRelationSide,
        SupportedAggregateOutput, SupportedAnalyticRowNumberPlan, SupportedAnalyticWindowFunction,
        SupportedEventTimeWindowKind, SupportedFilterProjectPlan, SupportedJoinKeyDomainV1,
        SupportedJoinKind, SupportedJoinViewPlan, SupportedLatestByKeyPlan,
        SupportedProjectionBinaryOp, SupportedProjectionExpr, SupportedSemiAntiJoinKindV1,
        SupportedSemiAntiJoinProjectPlanV1, SupportedThreeInputInnerJoinCountPlanV1,
        SupportedTopKPlan, SupportedTumblingWindowPlan, SupportedViewPlan,
        VelorixLogicalViewExecutionV1, VelorixLogicalViewPlanV1,
        COMPOSITE_PK_POSITIONAL_JSON_ARRAY_JOIN_KEY_CODEC_V1, LEFT_JOIN_INPUT_INSTANCE_ID_V1,
        RIGHT_JOIN_INPUT_INSTANCE_ID_V1, THREE_INPUT_LEGACY_SQL_ENCOUNTER_JOIN_ORDER_V1,
        THREE_INPUT_ROOT_FIXED_RIGHT_RELATION_ID_JOIN_ORDER_V1,
    },
};

pub use crate::runtime_contract::MATERIALIZED_VIEW_RUNTIME_NAME as CRATE_NAME;

mod analytic_row_number;
mod checkpoint_common;
mod event_time_window;
mod filter_project;
mod latest_by_key;
mod output;
mod semi_anti_join;
mod single_key_aggregate;
mod three_input_join;
mod two_input_join;

pub use analytic_row_number::AnalyticRowNumberRuntime;
pub use event_time_window::TumblingEventTimeAggregateRuntime;
pub use filter_project::FilterProjectRuntime;
pub use latest_by_key::LatestByKeyRuntime;
pub use semi_anti_join::TwoInputSemiAntiJoinRuntime;
pub use single_key_aggregate::{SingleKeyRuntimeInputV1, SingleKeySumCountRuntime};
pub use three_input_join::ThreeInputInnerJoinCountRuntime;
pub use two_input_join::TwoInputJoinRuntime;

use checkpoint_common::*;
use output::{
    materialized_delta_page_batch, materialized_delta_to_record_batch,
    materialized_generic_delta_page_batch, materialized_generic_delta_to_record_batch,
    materialized_tumbling_delta_page_batch, materialized_tumbling_delta_to_record_batch,
    parse_decimal128,
};

const CHECKPOINT_PAYLOAD_SCHEMA_VERSION: u32 = 1;
const FILTER_PROJECT_RUNTIME_KIND: &str = "filter_project";
const ANALYTIC_ROW_NUMBER_RUNTIME_KIND: &str = "analytic_row_number";
const JOIN_RUNTIME_KIND: &str = "two_input_join_sum_count";
const JOIN_COMMON_DAG_REFERENCE_RUNTIME_KIND: &str = "two_input_join_common_dag_reference_v1";
const LATEST_BY_KEY_RUNTIME_KIND: &str = "latest_by_key";
const TUMBLING_WINDOW_RUNTIME_KIND: &str = "tumbling_event_time_aggregate";
const JOIN_LEFT_VALUE_FIELD: &str = "__velorix_join_left_value";
const JOIN_RIGHT_VALUE_FIELD: &str = "__velorix_join_right_value";

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
        VelorixLogicalViewExecutionV1::FilterProject { plan } => {
            let [catalog] = catalogs else {
                return Err(
                    "filter/project runtime requires exactly one relation catalog".to_string(),
                );
            };
            FilterProjectRuntime::new_with_logical_plan(
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
        VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan } => {
            let [catalog] = catalogs else {
                return Err(
                    "analytic row-number runtime requires exactly one relation catalog".to_string(),
                );
            };
            AnalyticRowNumberRuntime::new_with_logical_plan(
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
                *plan.clone(),
                logical_plan,
            )
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string())
        }
        VelorixLogicalViewExecutionV1::ThreeInputInnerJoinCount { .. } => {
            ThreeInputInnerJoinCountRuntime::new_with_logical_plan(
                identity.clone(),
                catalogs.to_vec(),
                input_schemas.to_vec(),
                output_schema.clone(),
                logical_plan,
            )
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string())
        }
        VelorixLogicalViewExecutionV1::TwoInputSemiAntiJoinProject { .. } => {
            TwoInputSemiAntiJoinRuntime::new_with_logical_plan(
                identity.clone(),
                catalogs.to_vec(),
                input_schemas.to_vec(),
                output_schema.clone(),
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

/// Differential-test backend that binds an already admitted join DAG to the
/// generic native operator graph. It is intentionally not selected by the
/// public runtime factory or any SQL/API/configuration surface.
pub fn create_common_dag_reference_standing_runtime_with_logical_plan_and_catalogs(
    identity: &StandingProgramIdentity,
    catalogs: &[VelorixRelationCatalogV1],
    logical_plan: VelorixLogicalViewPlanV1,
    input_schemas: &[RelationSchema],
    output_schemas: &[RelationSchema],
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    let output_schema =
        only_schema(output_schemas, "output_schemas").map_err(|error| error.to_string())?;
    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan } =
        logical_plan.execution.clone()
    else {
        return Err(
            "common DAG reference backend supports admitted two-input joins only".to_string(),
        );
    };
    TwoInputJoinRuntime::new_common_dag_reference_with_logical_plan(
        identity.clone(),
        catalogs.to_vec(),
        input_schemas.to_vec(),
        output_schema,
        logical_plan.view_sql.clone(),
        *plan,
        logical_plan,
    )
    .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
    .map_err(|error| error.to_string())
}

pub fn restore_common_dag_reference_standing_runtime(
    checkpoint: RuntimeCheckpoint,
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    TwoInputJoinRuntime::restore_common_dag_reference(checkpoint)
        .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
        .map_err(|error| error.to_string())
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
    } else if looks_like_tumbling_window_output(output_schema, aggregate_outputs) {
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

/// Extract source-only input batches from a mixed standing input change list.
///
/// View inputs are rejected with a clear error. Runtimes that support
/// view-on-view dependencies implement their own view-input dispatch.
pub(super) fn source_input_batches(
    input_changes: Vec<StandingInputChangeV1>,
) -> Result<Vec<RelationInputBatch>, StandingProgramRuntimeError> {
    input_changes
        .into_iter()
        .map(|change| match change {
            StandingInputChangeV1::Source(batch) => Ok(batch),
            StandingInputChangeV1::View(_) => {
                Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "view delta input is not supported by this runtime",
                })
            }
        })
        .collect()
}

fn looks_like_tumbling_window_output(
    output_schema: &RelationSchema,
    aggregate_outputs: Option<&[SupportedAggregateOutput]>,
) -> bool {
    let Some(aggregate_outputs) = aggregate_outputs else {
        return false;
    };
    output_schema.columns.len() == aggregate_outputs.len() + 3
}

pub fn restore_standing_runtime(
    checkpoint: RuntimeCheckpoint,
) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
    if checkpoint_has_filter_project_payload(&checkpoint) {
        return FilterProjectRuntime::restore(checkpoint)
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string());
    }
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
    if checkpoint_has_analytic_row_number_payload(&checkpoint) {
        return AnalyticRowNumberRuntime::restore(checkpoint)
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string());
    }
    if checkpoint_has_common_dag_reference_join_payload(&checkpoint) {
        return TwoInputJoinRuntime::restore_common_dag_reference(checkpoint)
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string());
    }
    if checkpoint_has_join_payload(&checkpoint) {
        return TwoInputJoinRuntime::restore(checkpoint)
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string());
    }
    if checkpoint_has_three_input_join_payload(&checkpoint) {
        return ThreeInputInnerJoinCountRuntime::restore(checkpoint)
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string());
    }
    if checkpoint_has_semi_anti_join_payload(&checkpoint) {
        return TwoInputSemiAntiJoinRuntime::restore(checkpoint)
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string());
    }
    SingleKeySumCountRuntime::restore(checkpoint)
        .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
        .map_err(|error| error.to_string())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GenericCheckpointPayload {
    schema_version: u32,
    input: SingleKeyRuntimeInputV1,
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
    #[serde(default)]
    filtered_aggregate_state: DeltaBatch,
    applied_epochs: Vec<GenericAppliedEpoch>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GenericAppliedEpoch {
    idempotency_key: String,
    logical_epoch: LogicalEpoch,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FilterProjectCheckpointPayload {
    schema_version: u32,
    runtime_kind: String,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedFilterProjectPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    input_frontiers: Vec<RelationFrontier>,
    #[serde(default)]
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    full_output: Option<DeltaBatch>,
    published_output: DeltaBatch,
    applied_epochs: Vec<GenericAppliedEpoch>,
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AnalyticRowNumberState {
    rows: BTreeMap<String, AnalyticRowNumberStateRow>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AnalyticRowNumberStateRow {
    key: Value,
    partition_value: Value,
    order_value: Value,
    weight: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AnalyticRowNumberCheckpointPayload {
    schema_version: u32,
    runtime_kind: String,
    catalog: VelorixRelationCatalogV1,
    input_schema: RelationSchema,
    output_schema: RelationSchema,
    view_sql: String,
    plan: SupportedAnalyticRowNumberPlan,
    logical_plan: VelorixLogicalViewPlanV1,
    input_frontiers: Vec<RelationFrontier>,
    #[serde(default)]
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    state: AnalyticRowNumberState,
    published_output: DeltaBatch,
    applied_epochs: Vec<GenericAppliedEpoch>,
    logical_epoch: LogicalEpoch,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TumblingWindowState {
    rows: BTreeMap<String, TumblingWindowStateRow>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    session_events: BTreeMap<String, SessionWindowEvent>,
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
struct SessionWindowEvent {
    group_key: Value,
    event_time_ns: i64,
    amount: Option<i64>,
    weight: i64,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    join_key_codec_id: Option<String>,
    #[serde(default)]
    execution_binding: Option<JoinExecutionBindingV1>,
    input_frontiers: Vec<RelationFrontier>,
    #[serde(default)]
    input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    left_state: DeltaBatch,
    right_state: DeltaBatch,
    engine: EngineCheckpointPayload,
    #[serde(default)]
    published_output: Option<DeltaBatch>,
    #[serde(default)]
    filtered_aggregate_state: DeltaBatch,
    #[serde(default)]
    comparison_graph: Option<NativeOperatorGraphCheckpointV1>,
    applied_epochs: Vec<GenericAppliedEpoch>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinExecutionModeV1 {
    SelectedSpecialization,
    CommonDagReference,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JoinExecutionBindingV1 {
    pub mode: JoinExecutionModeV1,
    pub common_logical_dag_hash: String,
    pub implementation: LogicalPlanExecutionImplementationV1,
}

pub fn bind_join_execution_v1(
    logical_plan: &VelorixLogicalViewPlanV1,
    mode: JoinExecutionModeV1,
) -> Result<JoinExecutionBindingV1, String> {
    validate_logical_view_plan(logical_plan).map_err(|error| error.to_string())?;
    if !matches!(
        logical_plan.execution,
        VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { .. }
    ) {
        return Err("join execution binding requires an admitted two-input join plan".to_string());
    }
    let mut common_plan = logical_plan.clone();
    common_plan.plan_hash = None;
    common_plan.execution_implementation = None;
    let common_bytes = serde_json::to_vec(&common_plan).map_err(|error| error.to_string())?;
    let common_logical_dag_hash = format!(
        "velorix-common-logical-dag-sha256-v1:{}",
        stable_bytes_hash(&common_bytes)
    );
    let selected = logical_plan
        .execution_implementation
        .clone()
        .ok_or_else(|| "admitted join plan is missing its selected implementation".to_string())?;
    let implementation = match mode {
        JoinExecutionModeV1::SelectedSpecialization => selected,
        JoinExecutionModeV1::CommonDagReference => {
            let physical_bytes = serde_json::to_vec(&(
                &common_logical_dag_hash,
                "native_binary_or_left_join_v1",
                "native_planned_join_aggregate_v1",
                "native_planned_join_publisher_v1",
            ))
            .map_err(|error| error.to_string())?;
            LogicalPlanExecutionImplementationV1 {
                implementation_id: "velorix-common-dag-join-reference-v1".to_string(),
                state_codec_id: "velorix-native-operator-graph-checkpoint-v1".to_string(),
                physical_operator_dag_hash: format!(
                    "velorix-physical-operator-dag-sha256-v1:{}",
                    stable_bytes_hash(&physical_bytes)
                ),
                ..selected
            }
        }
    };
    Ok(JoinExecutionBindingV1 {
        mode,
        common_logical_dag_hash,
        implementation,
    })
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
        engine: &'a mut KeyedAggregateKernel,
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
        engine: &'a mut KeyedAggregateKernel,
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
                for input in &input_changes {
                    validate_input_matches_schema(input, input_schema, "generic_input_relation")?;
                    advance_input_frontier(&mut input_frontiers, input)?;
                    advance_input_event_time_frontier(&mut input_event_time_frontiers, input)?;
                }
                for input in input_changes {
                    let delta = single_key_input_delta_batch(catalog, plan, &input)?;
                    let delta = filter_delta_batch_for_plan(&delta, plan, catalog)?;
                    combined = combined.combine(&delta);
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
                for input in &input_changes {
                    validate_input_matches_schema(
                        input,
                        input_schema,
                        "latest_by_key_input_relation",
                    )?;
                    advance_input_frontier(&mut input_frontiers, input)?;
                    advance_input_event_time_frontier(&mut input_event_time_frontiers, input)?;
                }
                for input in input_changes {
                    let delta = arrow_record_batches_to_key_latest_by_delta_batch(
                        KeyLatestByDeltaBatchInput {
                            catalog,
                            relation_id: &input.relation_id,
                            relation_version: &input.relation_version,
                            schema_fingerprint: &input.schema_fingerprint,
                            key_column_id: &plan.key_column_id,
                            value_column_id: &plan.value_column_id,
                            ordering_column_id: &plan.ordering_column_id,
                            batches: &input.batches,
                        },
                    )
                    .map_err(|_| {
                        StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "latest_by_key_input_batch",
                        }
                    })?;
                    let delta = filter_delta_batch_for_latest_plan(&delta, plan, catalog)?;
                    combined = combined.combine(&delta);
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
                for input in &input_changes {
                    validate_input_matches_one_schema(
                        input,
                        input_schemas,
                        "generic_join_input_relation",
                    )?;
                    advance_input_frontier(&mut input_frontiers, input)?;
                    advance_input_event_time_frontier(&mut input_event_time_frontiers, input)?;
                }
                for input in input_changes {
                    let catalog = join_catalog_for_relation(catalogs, &input.relation_id)?;
                    if input.relation_id == plan.left_input_relation_id {
                        let delta = join_left_input_delta_batch(catalog, plan, &input)?;
                        let delta = prefilter_delta_batch_for_join_plan(&delta, plan, catalog)?;
                        let joined = match plan.join_kind {
                            SupportedJoinKind::Inner => join
                                .apply_left(&delta)
                                .map_err(|_| invalid_runtime_state())?,
                            SupportedJoinKind::Left => apply_left_join_left_delta(join, &delta)?,
                            SupportedJoinKind::Full => apply_full_join_left_delta(join, &delta)?,
                        };
                        joined_changes = joined_changes.combine(&joined);
                    } else if input.relation_id == plan.right_input_relation_id {
                        let delta = join_right_input_delta_batch(catalog, plan, &input)?;
                        let delta = prefilter_delta_batch_for_join_plan(&delta, plan, catalog)?;
                        let joined = match plan.join_kind {
                            SupportedJoinKind::Inner => join
                                .apply_right(&delta)
                                .map_err(|_| invalid_runtime_state())?,
                            SupportedJoinKind::Left => apply_left_join_right_delta(join, &delta)?,
                            SupportedJoinKind::Full => apply_full_join_right_delta(join, &delta)?,
                        };
                        joined_changes = joined_changes.combine(&joined);
                    } else {
                        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "generic_join_input_relation",
                        });
                    }
                }

                let joined_changes =
                    filter_joined_delta_batch_for_join_plan(&joined_changes, plan, catalogs)?;
                let joined_changes = project_joined_delta_batch_to_left_values(&joined_changes)?;
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

/// Isolated generic-DAG comparison target for retained aggregate-join
/// specializations. It never publishes output or replaces the selected runtime.
pub struct JoinSpecializationComparisonGraph {
    graph: NativeOperatorGraph,
    catalogs: Vec<VelorixRelationCatalogV1>,
    plan: SupportedJoinViewPlan,
}

impl JoinSpecializationComparisonGraph {
    pub fn new(
        catalogs: Vec<VelorixRelationCatalogV1>,
        plan: SupportedJoinViewPlan,
        output_schema: RelationSchema,
    ) -> Result<Self, String> {
        let mut graph = NativeOperatorGraph::new();
        match plan.join_kind {
            SupportedJoinKind::Inner => graph
                .add_operator(NativeBinaryJoinOperator::new(
                    "join",
                    join_output_value
                        as fn(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError>,
                ))
                .map_err(|error| error.to_string())?,
            SupportedJoinKind::Left => graph
                .add_operator(NativeLeftJoinOperator::new(
                    "join",
                    join_output_value
                        as fn(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError>,
                    unmatched_left_join_output_value
                        as fn(&DeltaValue) -> Result<DeltaValue, OperatorError>,
                ))
                .map_err(|error| error.to_string())?,
            SupportedJoinKind::Full => graph
                .add_operator(NativeFullJoinOperator::new(
                    "join",
                    join_output_value
                        as fn(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError>,
                    unmatched_left_join_output_value
                        as fn(&DeltaValue) -> Result<DeltaValue, OperatorError>,
                    unmatched_right_join_output_value
                        as fn(&DeltaValue) -> Result<DeltaValue, OperatorError>,
                ))
                .map_err(|error| error.to_string())?,
        }
        graph
            .add_operator(NativePlannedJoinAggregateOperator {
                node_id: "aggregate".to_string(),
                catalogs: catalogs.clone(),
                plan: plan.clone(),
                state: DeltaBatch::default(),
            })
            .map_err(|error| error.to_string())?;
        graph
            .add_operator(NativePlannedJoinPublisherOperator {
                node_id: "publish".to_string(),
                output_schema: output_schema.clone(),
                plan: plan.clone(),
                full_state: DeltaBatch::default(),
                published_state: DeltaBatch::default(),
            })
            .map_err(|error| error.to_string())?;
        graph.add_edge(NativeOperatorEdgeV1 {
            from_node_id: "join".to_string(),
            to_node_id: "aggregate".to_string(),
            to_port_id: "input".to_string(),
        });
        graph.add_edge(NativeOperatorEdgeV1 {
            from_node_id: "aggregate".to_string(),
            to_node_id: "publish".to_string(),
            to_port_id: "input".to_string(),
        });
        graph.validate().map_err(|error| error.to_string())?;
        Ok(Self {
            graph,
            catalogs,
            plan,
        })
    }

    pub fn apply_epoch(
        &mut self,
        logical_epoch: LogicalEpoch,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<DeltaBatch, String> {
        let mut inputs = Vec::new();
        for input in input_changes {
            let catalog = join_catalog_for_relation(&self.catalogs, &input.relation_id)
                .map_err(|error| error.to_string())?;
            let (port_id, delta) = if input.relation_id == self.plan.left_input_relation_id {
                (
                    "left",
                    join_left_input_delta_batch(catalog, &self.plan, &input)
                        .map_err(|error| error.to_string())?,
                )
            } else if input.relation_id == self.plan.right_input_relation_id {
                (
                    "right",
                    join_right_input_delta_batch(catalog, &self.plan, &input)
                        .map_err(|error| error.to_string())?,
                )
            } else {
                return Err("comparison input relation is not in the admitted plan".to_string());
            };
            let delta = prefilter_delta_batch_for_join_plan(&delta, &self.plan, catalog)
                .map_err(|error| error.to_string())?;
            inputs.push(NativeOperatorInputV1 {
                node_id: "join".to_string(),
                port_id: port_id.to_string(),
                batch: delta,
            });
        }
        self.graph
            .apply_epoch(logical_epoch, inputs)
            .map_err(|error| error.to_string())?
            .remove("publish")
            .ok_or_else(|| "comparison graph published output is missing".to_string())
    }

    pub fn checkpoint(&self) -> Result<NativeOperatorGraphCheckpointV1, String> {
        self.graph.checkpoint().map_err(|error| error.to_string())
    }

    pub fn restore(
        catalogs: Vec<VelorixRelationCatalogV1>,
        plan: SupportedJoinViewPlan,
        output_schema: RelationSchema,
        checkpoint: &NativeOperatorGraphCheckpointV1,
    ) -> Result<Self, String> {
        let mut comparison = Self::new(catalogs, plan, output_schema)?;
        comparison
            .graph
            .restore(checkpoint)
            .map_err(|error| error.to_string())?;
        Ok(comparison)
    }
}

struct NativePlannedJoinPublisherOperator {
    node_id: String,
    output_schema: RelationSchema,
    plan: SupportedJoinViewPlan,
    full_state: DeltaBatch,
    published_state: DeltaBatch,
}

impl NativeDeltaOperator for NativePlannedJoinPublisherOperator {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn input_ports(&self) -> &[&'static str] {
        &["input"]
    }

    fn apply(
        &mut self,
        port_id: &str,
        input: &DeltaBatch,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        if port_id != "input" {
            return Err(NativeOperatorError::InvalidGraph(
                "planned join publisher accepts only the input port".to_string(),
            ));
        }
        let next_full = apply_published_output_delta(&self.full_state, input)
            .map_err(|error| NativeOperatorError::InvalidGraph(error.to_string()))?;
        let aggregate_outputs = supported_join_view_plan_aggregate_outputs(&self.plan);
        let visible = filter_output_delta_for_having(
            &next_full,
            self.plan.having.as_ref(),
            self.plan.having_expr.as_ref(),
            &self.output_schema,
            Some(&aggregate_outputs),
        )
        .map_err(|error| NativeOperatorError::InvalidGraph(error.to_string()))?;
        let visible =
            apply_top_k_to_published_output(visible, self.plan.top_k.as_ref(), &aggregate_outputs)
                .map_err(|error| NativeOperatorError::InvalidGraph(error.to_string()))?;
        let delta = self.published_state.inverse()?.combine(&visible);
        self.full_state = next_full;
        self.published_state = visible;
        Ok(DeltaBatch::from_records(delta.net_rows()?))
    }

    fn checkpoint(&self) -> NativeOperatorCheckpointV1 {
        NativeOperatorCheckpointV1 {
            node_id: self.node_id.clone(),
            codec_id: "velorix-native-planned-join-publisher-v1".to_string(),
            codec_version: 1,
            state: NativeOperatorStateV1::Binary {
                left_state: self.full_state.clone(),
                right_state: self.published_state.clone(),
            },
        }
    }

    fn restore(
        &mut self,
        checkpoint: &NativeOperatorCheckpointV1,
    ) -> Result<(), NativeOperatorError> {
        if checkpoint.node_id != self.node_id
            || checkpoint.codec_id != "velorix-native-planned-join-publisher-v1"
            || checkpoint.codec_version != 1
        {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "planned join publisher checkpoint identity does not match".to_string(),
            ));
        }
        let NativeOperatorStateV1::Binary {
            left_state,
            right_state,
        } = &checkpoint.state
        else {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "planned join publisher checkpoint requires binary state".to_string(),
            ));
        };
        validate_published_output(left_state)
            .map_err(|error| NativeOperatorError::InvalidCheckpoint(error.to_string()))?;
        validate_published_output(right_state)
            .map_err(|error| NativeOperatorError::InvalidCheckpoint(error.to_string()))?;
        self.full_state = DeltaBatch::from_records(left_state.net_rows()?);
        self.published_state = DeltaBatch::from_records(right_state.net_rows()?);
        Ok(())
    }
}

struct NativePlannedJoinAggregateOperator {
    node_id: String,
    catalogs: Vec<VelorixRelationCatalogV1>,
    plan: SupportedJoinViewPlan,
    state: DeltaBatch,
}

impl NativeDeltaOperator for NativePlannedJoinAggregateOperator {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn input_ports(&self) -> &[&'static str] {
        &["input"]
    }

    fn apply(
        &mut self,
        port_id: &str,
        input: &DeltaBatch,
    ) -> Result<DeltaBatch, NativeOperatorError> {
        if port_id != "input" {
            return Err(NativeOperatorError::InvalidGraph(
                "planned join aggregate accepts only the input port".to_string(),
            ));
        }
        let filtered = filter_joined_delta_batch_for_join_plan(input, &self.plan, &self.catalogs)
            .map_err(|error| NativeOperatorError::InvalidGraph(error.to_string()))?;
        let (next, delta) =
            apply_filtered_join_aggregate_delta(&self.state, &filtered, &self.plan, &self.catalogs)
                .map_err(|error| NativeOperatorError::InvalidGraph(error.to_string()))?;
        self.state = next;
        Ok(delta)
    }

    fn checkpoint(&self) -> NativeOperatorCheckpointV1 {
        NativeOperatorCheckpointV1 {
            node_id: self.node_id.clone(),
            codec_id: "velorix-native-planned-join-aggregate-v1".to_string(),
            codec_version: 1,
            state: NativeOperatorStateV1::Unary {
                state: self.state.clone(),
            },
        }
    }

    fn restore(
        &mut self,
        checkpoint: &NativeOperatorCheckpointV1,
    ) -> Result<(), NativeOperatorError> {
        if checkpoint.node_id != self.node_id
            || checkpoint.codec_id != "velorix-native-planned-join-aggregate-v1"
            || checkpoint.codec_version != 1
        {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "planned join aggregate checkpoint identity does not match".to_string(),
            ));
        }
        let NativeOperatorStateV1::Unary { state } = &checkpoint.state else {
            return Err(NativeOperatorError::InvalidCheckpoint(
                "planned join aggregate checkpoint requires unary state".to_string(),
            ));
        };
        validate_published_output(state)
            .map_err(|error| NativeOperatorError::InvalidCheckpoint(error.to_string()))?;
        self.state = DeltaBatch::from_records(state.net_rows()?);
        Ok(())
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

fn single_key_input_delta_batch(
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedViewPlan,
    input: &RelationInputBatch,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if let Some(value_column_ids) = single_key_multi_input_column_ids(plan) {
        return arrow_record_batches_to_key_multi_value_delta_batch(
            catalog,
            &input.relation_id,
            &input.relation_version,
            &input.schema_fingerprint,
            std::slice::from_ref(&plan.group_key_column_id),
            &value_column_ids,
            &input.batches,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_input_batch",
        });
    }
    if let Some(count_column_id) = single_key_count_distinct_input_column(plan) {
        return arrow_record_batches_to_key_value_delta_batch_skipping_null_values(
            catalog,
            &input.relation_id,
            &input.relation_version,
            &input.schema_fingerprint,
            std::slice::from_ref(&plan.group_key_column_id),
            count_column_id,
            &input.batches,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_input_batch",
        });
    }
    if let Some(count_column_id) = single_key_count_only_input_column(plan) {
        return arrow_record_batches_to_key_nullable_count_delta_batch(
            catalog,
            &input.relation_id,
            &input.relation_version,
            &input.schema_fingerprint,
            &plan.group_key_column_id,
            count_column_id,
            &input.batches,
        );
    }
    if single_key_nullable_value_count_input_column(plan).is_some() {
        return arrow_record_batches_to_key_value_delta_batch_skipping_null_values(
            catalog,
            &input.relation_id,
            &input.relation_version,
            &input.schema_fingerprint,
            std::slice::from_ref(&plan.group_key_column_id),
            &plan.sum_value_column_id,
            &input.batches,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_input_batch",
        });
    }
    if single_key_sum_coalesce_fallback(plan).is_some() {
        return arrow_record_batches_to_key_nullable_value_delta_batch(
            catalog,
            &input.relation_id,
            &input.relation_version,
            &input.schema_fingerprint,
            std::slice::from_ref(&plan.group_key_column_id),
            &plan.sum_value_column_id,
            &input.batches,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_input_batch",
        });
    }
    if single_key_plan_uses_runtime_aggregate_state(plan)
        && catalog_column_by_id(catalog, &plan.sum_value_column_id)?.nullable
    {
        return arrow_record_batches_to_key_nullable_value_delta_batch(
            catalog,
            &input.relation_id,
            &input.relation_version,
            &input.schema_fingerprint,
            std::slice::from_ref(&plan.group_key_column_id),
            &plan.sum_value_column_id,
            &input.batches,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_input_batch",
        });
    }
    arrow_record_batches_to_key_value_delta_batch(
        catalog,
        &input.relation_id,
        &input.relation_version,
        &input.schema_fingerprint,
        std::slice::from_ref(&plan.group_key_column_id),
        &plan.sum_value_column_id,
        &input.batches,
    )
    .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "generic_input_batch",
    })
}

fn aggregate_group_input_delta_batch(
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedViewPlan,
    input: &RelationInputBatch,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if plan.aggregate_output_identity.is_none() {
        return single_key_input_delta_batch(catalog, plan, input);
    }
    let primary_key = catalog_primary_key_column(catalog)?.column_id.clone();
    let mut value_column_ids = supported_view_plan_aggregate_outputs(plan)
        .into_iter()
        .filter_map(|aggregate| aggregate.input_column_id)
        .collect::<BTreeSet<_>>();
    for key in supported_view_plan_group_keys(plan) {
        if let Some(column_id) = key.input_column_id {
            value_column_ids.insert(column_id);
        }
        if let Some(expression) = key.expression {
            value_column_ids.extend(projection_expr_column_ids(&expression));
        }
    }
    value_column_ids.remove(&primary_key);
    if value_column_ids.is_empty() {
        value_column_ids.insert(plan.sum_value_column_id.clone());
    }
    arrow_record_batches_to_key_multi_value_delta_batch(
        catalog,
        &input.relation_id,
        &input.relation_version,
        &input.schema_fingerprint,
        std::slice::from_ref(&primary_key),
        &value_column_ids.into_iter().collect::<Vec<_>>(),
        &input.batches,
    )
    .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "aggregate_group_input_batch",
    })
}

fn rekey_delta_batch_for_aggregate_group_with_primary_key(
    delta: &DeltaBatch,
    primary_key: &str,
    plan: &SupportedViewPlan,
    input_schema: &RelationSchema,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if plan.aggregate_output_identity.is_none() {
        return Ok(delta.clone());
    }
    let group_keys = supported_view_plan_group_keys(plan);
    let mut records = Vec::with_capacity(delta.records().len());
    for record in delta.records() {
        let mut input = record
            .value
            .as_json()
            .as_object()
            .cloned()
            .ok_or_else(invalid_runtime_state)?;
        input.insert(primary_key.to_string(), record.key.as_json().clone());
        let values = group_keys
            .iter()
            .map(|key| {
                let value = if let Some(column_id) = &key.input_column_id {
                    input
                        .get(column_id)
                        .cloned()
                        .ok_or_else(invalid_runtime_state)?
                } else if let Some(expression) = &key.expression {
                    Value::Number(JsonNumber::from(evaluate_projection_expr_for_schema(
                        expression,
                        &input,
                        input_schema,
                    )?))
                } else {
                    return Err(invalid_runtime_state());
                };
                Ok((key.output_column_id.clone(), value))
            })
            .collect::<Result<Vec<_>, StandingProgramRuntimeError>>()?;
        let key = if supported_view_plan_is_singleton(plan) {
            singleton_aggregate_key("state")
        } else if let [(_, value)] = values.as_slice() {
            value.clone()
        } else {
            DeltaValue::from_json(Value::Object(values.into_iter().collect()))
                .as_json()
                .clone()
        };
        records.push(DeltaRecord::new(
            DeltaKey::from_json(key),
            record.value.clone(),
            record.weight,
        ));
    }
    Ok(DeltaBatch::from_records(records))
}

fn rekey_delta_batch_for_aggregate_group(
    delta: &DeltaBatch,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedViewPlan,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if plan.aggregate_output_identity.is_none() {
        return Ok(delta.clone());
    }
    let primary_key = catalog_primary_key_column(catalog)?.column_id.clone();
    let group_keys = supported_view_plan_group_keys(plan);
    let mut records = Vec::with_capacity(delta.records().len());
    for record in delta.records() {
        let mut input = record
            .value
            .as_json()
            .as_object()
            .cloned()
            .ok_or_else(invalid_runtime_state)?;
        input.insert(primary_key.clone(), record.key.as_json().clone());
        let values = group_keys
            .iter()
            .map(|key| {
                let value = if let Some(column_id) = &key.input_column_id {
                    input
                        .get(column_id)
                        .cloned()
                        .ok_or_else(invalid_runtime_state)?
                } else if let Some(expression) = &key.expression {
                    Value::Number(JsonNumber::from(evaluate_projection_expr(
                        expression, &input, catalog,
                    )?))
                } else {
                    return Err(invalid_runtime_state());
                };
                Ok((key.output_column_id.clone(), value))
            })
            .collect::<Result<Vec<_>, StandingProgramRuntimeError>>()?;
        let key = if supported_view_plan_is_singleton(plan) {
            singleton_aggregate_key("state")
        } else if let [(_, value)] = values.as_slice() {
            value.clone()
        } else {
            DeltaValue::from_json(Value::Object(values.into_iter().collect()))
                .as_json()
                .clone()
        };
        records.push(DeltaRecord::new(
            DeltaKey::from_json(key),
            record.value.clone(),
            record.weight,
        ));
    }
    Ok(DeltaBatch::from_records(records))
}

fn singleton_aggregate_key(domain: &str) -> Value {
    serde_json::json!({
        "$velorix_internal_key": {
            "domain": format!("aggregate_singleton_{domain}"),
            "version": 1
        }
    })
}

fn publish_aggregate_state(
    state: &DeltaBatch,
    plan: &SupportedViewPlan,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if !supported_view_plan_is_singleton(plan) {
        return Ok(state.clone());
    }
    let rows = state.net_rows().map_err(|_| invalid_runtime_state())?;
    let value = match rows.as_slice() {
        [] => {
            let aggregate_outputs = supported_view_plan_aggregate_outputs(plan);
            let [count] = aggregate_outputs.as_slice() else {
                return Err(invalid_runtime_state());
            };
            let mut value = Map::new();
            value.insert(
                count.output_column_id.clone(),
                Value::Number(JsonNumber::from(0)),
            );
            DeltaValue::from_json(Value::Object(value))
        }
        [row] if row.weight == 1 => row.value.clone(),
        _ => return Err(invalid_runtime_state()),
    };
    Ok(DeltaBatch::from_records(vec![DeltaRecord::new(
        DeltaKey::from_json(singleton_aggregate_key("publication")),
        value,
        1,
    )]))
}

fn join_right_input_delta_batch(
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedJoinViewPlan,
    input: &RelationInputBatch,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let (_, right_join_key_column_ids) = join_key_column_ids(plan)?;
    let right_value_column_ids = supported_join_view_plan_right_value_column_ids(plan);
    if right_value_column_ids.is_empty() {
        let delta = arrow_record_batches_to_key_value_delta_batch(
            catalog,
            &input.relation_id,
            &input.relation_version,
            &input.schema_fingerprint,
            &right_join_key_column_ids,
            &plan.right_join_key_column_id,
            &input.batches,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_input_batch",
        })?;
        return normalize_composite_join_keys(delta, &right_join_key_column_ids);
    }
    if right_value_column_ids.len() == 1
        && !catalog_column_by_id(catalog, &right_value_column_ids[0])?.nullable
    {
        let delta = arrow_record_batches_to_key_value_delta_batch(
            catalog,
            &input.relation_id,
            &input.relation_version,
            &input.schema_fingerprint,
            &right_join_key_column_ids,
            &right_value_column_ids[0],
            &input.batches,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_input_batch",
        })?;
        return normalize_composite_join_keys(delta, &right_join_key_column_ids);
    }

    let delta = arrow_record_batches_to_key_multi_value_delta_batch(
        catalog,
        &input.relation_id,
        &input.relation_version,
        &input.schema_fingerprint,
        &right_join_key_column_ids,
        &right_value_column_ids,
        &input.batches,
    )
    .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "generic_join_input_batch",
    })?;
    normalize_composite_join_keys(delta, &right_join_key_column_ids)
}

fn join_left_input_delta_batch(
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedJoinViewPlan,
    input: &RelationInputBatch,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let (left_join_key_column_ids, _) = join_key_column_ids(plan)?;
    if join_plan_preserves_nullable_left_values(catalog, plan)? {
        let delta = arrow_record_batches_to_key_nullable_value_delta_batch(
            catalog,
            &input.relation_id,
            &input.relation_version,
            &input.schema_fingerprint,
            &left_join_key_column_ids,
            &plan.sum_value_column_id,
            &input.batches,
        )
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_input_batch",
        })?;
        return normalize_composite_join_keys(delta, &left_join_key_column_ids);
    }
    let convert = if join_nullable_value_count_input_column(catalog, plan)?.is_some() {
        arrow_record_batches_to_key_value_delta_batch_skipping_null_values
    } else {
        arrow_record_batches_to_key_value_delta_batch
    };
    let delta = convert(
        catalog,
        &input.relation_id,
        &input.relation_version,
        &input.schema_fingerprint,
        &left_join_key_column_ids,
        &plan.sum_value_column_id,
        &input.batches,
    )
    .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "generic_join_input_batch",
    })?;
    normalize_composite_join_keys(delta, &left_join_key_column_ids)
}

fn normalize_composite_join_keys(
    delta: DeltaBatch,
    key_column_ids: &[String],
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if key_column_ids.len() == 1 {
        return Ok(delta);
    }
    let records = delta
        .records()
        .iter()
        .map(|record| {
            let object = record.key.as_json().as_object().ok_or(
                StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_join_input_batch.composite_key",
                },
            )?;
            let values = key_column_ids
                .iter()
                .map(|column_id| {
                    object.get(column_id).cloned().ok_or(
                        StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "generic_join_input_batch.composite_key",
                        },
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DeltaRecord::new(
                DeltaKey::from_json(Value::Array(values)),
                record.value.clone(),
                record.weight,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DeltaBatch::from_records(records))
}

fn join_nullable_value_count_input_column<'a>(
    catalog: &VelorixRelationCatalogV1,
    plan: &'a SupportedJoinViewPlan,
) -> Result<Option<&'a str>, StandingProgramRuntimeError> {
    for output in supported_join_view_plan_aggregate_outputs(plan) {
        if matches!(
            output.function,
            LogicalPlanAggregateFunctionV1::Count | LogicalPlanAggregateFunctionV1::CountDistinct
        ) && output.input_column_id.as_deref() == Some(plan.sum_value_column_id.as_str())
            && catalog_column_by_id(catalog, &plan.sum_value_column_id)?.nullable
        {
            return Ok(Some(plan.sum_value_column_id.as_str()));
        }
    }
    Ok(None)
}

fn join_plan_preserves_nullable_left_values(
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedJoinViewPlan,
) -> Result<bool, StandingProgramRuntimeError> {
    Ok(join_plan_uses_runtime_aggregate_state(plan)
        && catalog_column_by_id(catalog, &plan.sum_value_column_id)?.nullable)
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
            && frontier.stream_id == input.stream_id
            && frontier.partition_id == input.partition_id
    }) {
        if input.start_offset_inclusive != frontier.committed_offset_exclusive {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "input_frontier.offset_range",
            });
        }
        frontier.committed_offset_exclusive = input.end_offset_exclusive;
    } else {
        frontiers.push(RelationFrontier {
            relation_id: input.relation_id.clone(),
            relation_version: input.relation_version.clone(),
            stream_id: input.stream_id.clone(),
            partition_id: input.partition_id,
            committed_offset_exclusive: input.end_offset_exclusive,
        });
    }
    frontiers.sort_by(|left, right| {
        (
            &left.relation_id,
            &left.relation_version,
            &left.stream_id,
            left.partition_id,
        )
            .cmp(&(
                &right.relation_id,
                &right.relation_version,
                &right.stream_id,
                right.partition_id,
            ))
    });
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
        frontiers.sort_by(|left, right| {
            (
                &left.relation_id,
                &left.relation_version,
                &left.schema_fingerprint,
                &left.stream_id,
                left.partition_id,
                &left.event_time_column_id,
            )
                .cmp(&(
                    &right.relation_id,
                    &right.relation_version,
                    &right.schema_fingerprint,
                    &right.stream_id,
                    right.partition_id,
                    &right.event_time_column_id,
                ))
        });
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
    frontiers.sort_by(|left, right| {
        (
            &left.relation_id,
            &left.relation_version,
            &left.schema_fingerprint,
            &left.stream_id,
            left.partition_id,
            &left.event_time_column_id,
        )
            .cmp(&(
                &right.relation_id,
                &right.relation_version,
                &right.schema_fingerprint,
                &right.stream_id,
                right.partition_id,
                &right.event_time_column_id,
            ))
    });
    Ok(())
}

fn validate_checkpoint_frontiers(
    checkpoint: &RuntimeCheckpoint,
    payload: &GenericCheckpointPayload,
) -> Result<(), StandingProgramRuntimeError> {
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
    let self_join = supported_join_view_plan_is_self_join(plan);
    if (self_join && catalogs.len() != 1) || (!self_join && catalogs.len() != 2) {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field: "catalogs" });
    }
    let left_catalog = join_left_catalog(plan, catalogs)?;
    let right_catalog = join_right_catalog(plan, catalogs)?;
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
    let aggregate_outputs = supported_join_view_plan_aggregate_outputs(plan);
    if supported_join_view_plan_is_singleton(plan) {
        if output.primary_key.is_empty()
            && output.columns.len() == aggregate_outputs.len()
            && output
                .columns
                .iter()
                .zip(aggregate_outputs.iter())
                .all(|(column, aggregate)| {
                    column.name == aggregate.output_column_id
                        && join_aggregate_output_sql_type(left_catalog, right_catalog, aggregate)
                            .is_ok_and(|data_type| column.data_type == data_type)
                        && !column.nullable
                })
        {
            return Ok(());
        }
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema",
        });
    }
    let [key, aggregate_columns @ ..] = output.columns.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.columns",
        });
    };
    if aggregate_columns.len() != aggregate_outputs.len() {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.columns",
        });
    }
    let group_key_catalog = join_catalog_for_relation(catalogs, &plan.group_key_relation_id)?;
    let group_key = catalog_column(group_key_catalog, &plan.group_key_column_id)?;
    let expected_key_type = sql_type_from_catalog_column(group_key)?;
    let expected_key_name = if plan.output_key_column_id.is_empty() {
        group_key.name.clone()
    } else {
        plan.output_key_column_id.clone()
    };
    if output.primary_key != vec![key.name.clone()]
        || key.name != expected_key_name
        || key.data_type != expected_key_type
        || key.nullable
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema",
        });
    }
    for (column, aggregate) in aggregate_columns.iter().zip(aggregate_outputs.iter()) {
        let expected_nullable = left_join_uses_extended_aggregate_state(plan)
            && !matches!(
                aggregate.function,
                LogicalPlanAggregateFunctionV1::Count
                    | LogicalPlanAggregateFunctionV1::CountDistinct
            );
        if column.name != aggregate.output_column_id
            || column.data_type
                != join_aggregate_output_sql_type(left_catalog, right_catalog, aggregate)?
            || column.nullable != expected_nullable
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "output_schema",
            });
        }
    }
    Ok(())
}

fn validate_join_plan_matches_catalogs(
    plan: &SupportedJoinViewPlan,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<(), StandingProgramRuntimeError> {
    let left = join_left_catalog(plan, catalogs)?;
    let right = join_right_catalog(plan, catalogs)?;
    let same_relation = plan.left_input_relation_id == plan.right_input_relation_id;
    let self_join = supported_join_view_plan_is_self_join(plan);
    if same_relation != self_join
        || (self_join
            && (plan.left_input_instance_id.as_deref() != Some(LEFT_JOIN_INPUT_INSTANCE_ID_V1)
                || plan.right_input_instance_id.as_deref()
                    != Some(RIGHT_JOIN_INPUT_INSTANCE_ID_V1)))
        || (!self_join
            && (plan.left_input_instance_id.is_some()
                || plan.right_input_instance_id.is_some()
                || supported_join_view_plan_is_singleton(plan)))
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan.input_instances",
        });
    }
    aggregate_value_mode_for_column_id(left, &plan.sum_value_column_id)?;
    let (left_join_key_column_ids, right_join_key_column_ids) = join_key_column_ids(plan)?;
    let left_join_key_set = left_join_key_column_ids.iter().collect::<BTreeSet<_>>();
    let right_join_key_set = right_join_key_column_ids.iter().collect::<BTreeSet<_>>();
    let left_primary_key_set = left
        .relation_schema
        .primary_key_column_ids
        .iter()
        .collect::<BTreeSet<_>>();
    let right_primary_key_set = right
        .relation_schema
        .primary_key_column_ids
        .iter()
        .collect::<BTreeSet<_>>();
    let covers_primary_keys =
        left_join_key_set == left_primary_key_set && right_join_key_set == right_primary_key_set;
    let non_primary_scalar = if left_join_key_column_ids.len() == 1 {
        let left_key = catalog_column(left, &left_join_key_column_ids[0])?;
        let right_key = catalog_column(right, &right_join_key_column_ids[0])?;
        plan.join_kind == SupportedJoinKind::Inner
            && !left_primary_key_set.contains(&left_key.column_id)
            && !right_primary_key_set.contains(&right_key.column_id)
            && left_key.column_id != left.relation_schema.weight_column_id
            && right_key.column_id != right.relation_schema.weight_column_id
            && !left_key.nullable
            && !right_key.nullable
            && supported_runtime_scalar_join_key_atom(&left_key.physical_arrow_type)
            && supported_runtime_scalar_join_key_atom(&right_key.physical_arrow_type)
    } else {
        false
    };
    let expected_join_key_domain =
        non_primary_scalar.then_some(SupportedJoinKeyDomainV1::NonPrimaryNonNullScalarV1);
    if (!covers_primary_keys && !non_primary_scalar)
        || plan.join_key_domain != expected_join_key_domain
        || plan.sum_value_relation_id != left.relation_schema.relation_id
        || (plan.join_kind != SupportedJoinKind::Inner && left_join_key_column_ids.len() > 1)
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan",
        });
    }
    for (left_key, right_key) in left_join_key_column_ids
        .iter()
        .zip(right_join_key_column_ids.iter())
    {
        if catalog_column(left, left_key)?.physical_arrow_type
            != catalog_column(right, right_key)?.physical_arrow_type
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_view_plan",
            });
        }
    }
    if !supported_join_view_plan_is_singleton(plan) {
        let group_key_catalog = join_catalog_for_relation(catalogs, &plan.group_key_relation_id)?;
        let group_key = catalog_column(group_key_catalog, &plan.group_key_column_id)?;
        let valid_group_key = (plan.group_key_relation_id == left.relation_schema.relation_id
            && left_join_key_column_ids.contains(&group_key.column_id))
            || (plan.group_key_relation_id == right.relation_schema.relation_id
                && right_join_key_column_ids.contains(&group_key.column_id));
        if !valid_group_key {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_view_plan.group_key",
            });
        }
    }
    let aggregate_outputs = supported_join_view_plan_aggregate_outputs(plan);
    if self_join
        && (!supported_join_view_plan_is_singleton(plan)
            || plan.join_kind != SupportedJoinKind::Inner
            || plan.composite_equality.is_some()
            || plan.join_key_domain != Some(SupportedJoinKeyDomainV1::NonPrimaryNonNullScalarV1)
            || !plan.aggregate_filter_exprs.is_empty()
            || plan.predicate.is_some()
            || !plan.predicates.is_empty()
            || plan.predicate_expr.is_some()
            || plan.having.is_some()
            || plan.having_expr.is_some()
            || plan.top_k.is_some()
            || !matches!(aggregate_outputs.as_slice(), [output]
                if output.function == LogicalPlanAggregateFunctionV1::Count
                    && output.input_column_id.is_none()
                    && output.input_expression.is_none()
                    && output.input_relation_side.is_none()))
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan.self_join",
        });
    }
    if plan.join_kind == SupportedJoinKind::Left
        && (plan.group_key_relation_id != plan.left_input_relation_id
            || plan.group_key_column_id != plan.left_join_key_column_id)
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan.left_join",
        });
    }
    let right_value_column_ids = supported_join_view_plan_right_value_column_ids(plan);
    for column_id in &right_value_column_ids {
        let column = catalog_column(right, column_id)?;
        if column.column_id == right.relation_schema.weight_column_id {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_view_plan.right_value_column",
            });
        }
    }
    let mut output_ids = BTreeSet::new();
    for output in &aggregate_outputs {
        if output.output_column_id.is_empty()
            || !output_ids.insert(output.output_column_id.to_ascii_lowercase())
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_view_plan.aggregate_outputs",
            });
        }
        if let Some(expression) = &output.input_expression {
            if !matches!(
                output.function,
                LogicalPlanAggregateFunctionV1::Sum
                    | LogicalPlanAggregateFunctionV1::Avg
                    | LogicalPlanAggregateFunctionV1::Min
                    | LogicalPlanAggregateFunctionV1::Max
            ) {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_join_view_plan.aggregate_outputs.input_expression",
                });
            }
            let Some(input_column_id) = output.input_column_id.as_deref() else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_join_view_plan.aggregate_outputs.input_expression",
                });
            };
            let columns = projection_expr_column_ids(expression);
            if columns.len() != 1 || columns[0] != input_column_id {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_join_view_plan.aggregate_outputs.input_expression",
                });
            }
            let catalog =
                if output.input_relation_side == Some(SupportedAggregateInputRelationSide::Right) {
                    right
                } else {
                    left
                };
            catalog_column(catalog, input_column_id)?;
        }
        match output.function {
            LogicalPlanAggregateFunctionV1::Sum
            | LogicalPlanAggregateFunctionV1::Avg
            | LogicalPlanAggregateFunctionV1::Min
            | LogicalPlanAggregateFunctionV1::Max
                if join_aggregate_uses_left_value(output, plan) => {}
            LogicalPlanAggregateFunctionV1::Sum
            | LogicalPlanAggregateFunctionV1::Avg
            | LogicalPlanAggregateFunctionV1::Min
            | LogicalPlanAggregateFunctionV1::Max
                if join_aggregate_uses_right_value(output, &right_value_column_ids) => {}
            LogicalPlanAggregateFunctionV1::Count if output.input_column_id.is_none() => {}
            LogicalPlanAggregateFunctionV1::Count
            | LogicalPlanAggregateFunctionV1::CountDistinct
                if join_aggregate_uses_left_value(output, plan) => {}
            LogicalPlanAggregateFunctionV1::Count
            | LogicalPlanAggregateFunctionV1::CountDistinct
                if join_aggregate_uses_right_value(output, &right_value_column_ids) => {}
            _ => {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_join_view_plan.aggregate_outputs",
                });
            }
        }
    }
    for (output_column_id, predicate_expr) in &plan.aggregate_filter_exprs {
        let Some(output) = aggregate_outputs
            .iter()
            .find(|output| output.output_column_id == *output_column_id)
        else {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_view_plan.aggregate_filter_exprs",
            });
        };
        if !matches!(
            output.function,
            LogicalPlanAggregateFunctionV1::Sum
                | LogicalPlanAggregateFunctionV1::Count
                | LogicalPlanAggregateFunctionV1::CountDistinct
                | LogicalPlanAggregateFunctionV1::Avg
                | LogicalPlanAggregateFunctionV1::Min
                | LogicalPlanAggregateFunctionV1::Max
        ) {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_view_plan.aggregate_filter_exprs",
            });
        }
        for predicate in predicate_expr.leaf_predicates() {
            let catalog = join_catalog_for_relation(catalogs, &predicate.relation_id)?;
            let column = catalog_column(catalog, &predicate.predicate.column_id)?;
            let valid_left = predicate.relation_id == plan.left_input_relation_id
                && (left_join_key_column_ids.contains(&column.column_id)
                    || column.column_id == plan.sum_value_column_id);
            let valid_right = predicate.relation_id == plan.right_input_relation_id
                && (right_join_key_column_ids.contains(&column.column_id)
                    || right_value_column_ids
                        .iter()
                        .any(|column_id| column_id == &column.column_id));
            if !valid_left && !valid_right {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_join_view_plan.aggregate_filter_exprs",
                });
            }
        }
        validate_join_predicate_expr_for_runtime(
            predicate_expr,
            plan,
            catalogs,
            &right_value_column_ids,
            "generic_join_view_plan.aggregate_filter_exprs",
        )?;
    }
    validate_having_predicates_for_outputs(
        plan.having.as_ref(),
        plan.having_expr.as_ref(),
        &aggregate_outputs,
        "generic_join_view_plan.having",
    )?;
    for predicate in supported_join_view_plan_predicates(plan) {
        let catalog = join_catalog_for_relation(catalogs, &predicate.relation_id)?;
        let column = catalog_column(catalog, &predicate.predicate.column_id)?;
        let valid_left = predicate.relation_id == plan.left_input_relation_id
            && (left_join_key_column_ids.contains(&column.column_id)
                || column.column_id == plan.sum_value_column_id);
        let valid_right = predicate.relation_id == plan.right_input_relation_id
            && (right_join_key_column_ids.contains(&column.column_id)
                || right_value_column_ids
                    .iter()
                    .any(|column_id| column_id == &column.column_id));
        if !valid_left && !valid_right {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_view_plan.predicate",
            });
        }
    }
    if let Some(predicate_expr) = &plan.predicate_expr {
        validate_join_predicate_expr_for_runtime(
            predicate_expr,
            plan,
            catalogs,
            &right_value_column_ids,
            "generic_join_view_plan.predicate",
        )?;
    }
    Ok(())
}

fn supported_runtime_scalar_join_key_atom(physical_type: &ArrowPhysicalTypeV1) -> bool {
    !matches!(
        physical_type,
        ArrowPhysicalTypeV1::List { .. }
            | ArrowPhysicalTypeV1::Struct { .. }
            | ArrowPhysicalTypeV1::Map { .. }
    )
}

fn join_key_column_ids(
    plan: &SupportedJoinViewPlan,
) -> Result<(Vec<String>, Vec<String>), StandingProgramRuntimeError> {
    let pairs = supported_join_view_plan_key_pairs(plan).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan.composite_equality",
        }
    })?;
    Ok((
        pairs
            .iter()
            .map(|pair| pair.left_column_id.clone())
            .collect(),
        pairs
            .iter()
            .map(|pair| pair.right_column_id.clone())
            .collect(),
    ))
}

fn validate_join_predicate_expr_for_runtime(
    predicate_expr: &JoinPredicateExpr,
    plan: &SupportedJoinViewPlan,
    catalogs: &[VelorixRelationCatalogV1],
    right_value_column_ids: &[String],
    field: &'static str,
) -> Result<(), StandingProgramRuntimeError> {
    match predicate_expr {
        JoinPredicateExpr::Atom { predicate } => validate_join_row_predicate_for_runtime(
            predicate,
            plan,
            catalogs,
            right_value_column_ids,
            field,
        ),
        JoinPredicateExpr::ScalarInt64Comparison {
            relation_id, left, ..
        } => validate_join_scalar_int64_predicate_for_runtime(
            relation_id,
            left,
            plan,
            catalogs,
            right_value_column_ids,
            field,
        ),
        JoinPredicateExpr::ScalarInt64ExpressionComparison {
            left_relation_id,
            left,
            right_relation_id,
            right,
            ..
        } => {
            if left_relation_id == right_relation_id {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field });
            }
            validate_join_scalar_int64_predicate_for_runtime(
                left_relation_id,
                left,
                plan,
                catalogs,
                right_value_column_ids,
                field,
            )?;
            validate_join_scalar_int64_predicate_for_runtime(
                right_relation_id,
                right,
                plan,
                catalogs,
                right_value_column_ids,
                field,
            )
        }
        JoinPredicateExpr::And { left, right } | JoinPredicateExpr::Or { left, right } => {
            validate_join_predicate_expr_for_runtime(
                left,
                plan,
                catalogs,
                right_value_column_ids,
                field,
            )?;
            validate_join_predicate_expr_for_runtime(
                right,
                plan,
                catalogs,
                right_value_column_ids,
                field,
            )
        }
    }
}

fn validate_join_row_predicate_for_runtime(
    predicate: &JoinRowPredicate,
    plan: &SupportedJoinViewPlan,
    catalogs: &[VelorixRelationCatalogV1],
    right_value_column_ids: &[String],
    field: &'static str,
) -> Result<(), StandingProgramRuntimeError> {
    let catalog = join_catalog_for_relation(catalogs, &predicate.relation_id)?;
    let column = catalog_column(catalog, &predicate.predicate.column_id)?;
    if join_predicate_column_is_runtime_visible(
        &predicate.relation_id,
        &column.column_id,
        plan,
        right_value_column_ids,
    ) {
        Ok(())
    } else {
        Err(StandingProgramRuntimeError::InvalidProgramIdentity { field })
    }
}

fn validate_join_scalar_int64_predicate_for_runtime(
    relation_id: &str,
    left: &SupportedProjectionExpr,
    plan: &SupportedJoinViewPlan,
    catalogs: &[VelorixRelationCatalogV1],
    right_value_column_ids: &[String],
    field: &'static str,
) -> Result<(), StandingProgramRuntimeError> {
    let catalog = join_catalog_for_relation(catalogs, relation_id)?;
    validate_filter_project_projection_expr(catalog, left)
        .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity { field })?;
    let column_ids = projection_expr_column_ids(left);
    let [column_id] = column_ids.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field });
    };
    if join_predicate_column_is_runtime_visible(
        relation_id,
        column_id,
        plan,
        right_value_column_ids,
    ) {
        Ok(())
    } else {
        Err(StandingProgramRuntimeError::InvalidProgramIdentity { field })
    }
}

fn join_predicate_column_is_runtime_visible(
    relation_id: &str,
    column_id: &str,
    plan: &SupportedJoinViewPlan,
    right_value_column_ids: &[String],
) -> bool {
    let pairs = supported_join_view_plan_key_pairs(plan).unwrap_or_default();
    let valid_left = relation_id == plan.left_input_relation_id
        && (pairs.iter().any(|pair| pair.left_column_id == column_id)
            || column_id == plan.sum_value_column_id);
    let valid_right = relation_id == plan.right_input_relation_id
        && (pairs.iter().any(|pair| pair.right_column_id == column_id)
            || right_value_column_ids
                .iter()
                .any(|right_column_id| right_column_id == column_id));
    valid_left || valid_right
}

fn join_aggregate_uses_left_value(
    output: &SupportedAggregateOutput,
    plan: &SupportedJoinViewPlan,
) -> bool {
    output.input_column_id.as_deref() == Some(plan.sum_value_column_id.as_str())
        && output
            .input_relation_side
            .unwrap_or(SupportedAggregateInputRelationSide::Left)
            == SupportedAggregateInputRelationSide::Left
}

fn join_aggregate_uses_right_value(
    output: &SupportedAggregateOutput,
    right_value_column_ids: &[String],
) -> bool {
    output.input_relation_side == Some(SupportedAggregateInputRelationSide::Right)
        && output.input_column_id.as_ref().is_some_and(|column_id| {
            right_value_column_ids
                .iter()
                .any(|right| right == column_id)
        })
}

fn validate_join_sql_or_logical_plan(
    view_sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
    plan: &SupportedJoinViewPlan,
    logical_plan: &VelorixLogicalViewPlanV1,
) -> Result<(), StandingProgramRuntimeError> {
    match validate_supported_join_view_sql(view_sql, catalogs) {
        Ok(mut compiled_plan) => {
            normalize_legacy_join_plan_input_relation_sides(&mut compiled_plan);
            if compiled_plan != *plan {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_join_view_plan",
                });
            }
            let compiled_logical_plan =
                lower_supported_join_view_sql_to_logical_plan(view_sql, catalogs, output_schema)
                    .map_err(|_| StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "logical_join_view_plan",
                    })?;
            if compiled_logical_plan != *logical_plan {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "logical_join_view_plan",
                });
            }
            Ok(())
        }
        Err(_) if join_plan_uses_runtime_aggregate_state(plan) => {
            if logical_plan.view_sql != view_sql
                || logical_plan.output_relation.relation_id != output_schema.relation_id
                || logical_plan.output_relation.relation_version != output_schema.relation_version
                || logical_plan.output_relation.schema_fingerprint
                    != output_schema.schema_fingerprint
            {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "logical_join_view_plan",
                });
            }
            match &logical_plan.execution {
                VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: logical_plan } => {
                    if logical_plan.as_ref() == plan {
                        Ok(())
                    } else {
                        Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "logical_join_view_plan",
                        })
                    }
                }
                _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "logical_join_view_plan",
                }),
            }
        }
        Err(_) => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan",
        }),
    }
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
        join_output_value as fn(&DeltaValue, &DeltaValue) -> Result<DeltaValue, OperatorError>,
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

fn join_output_value(left: &DeltaValue, right: &DeltaValue) -> Result<DeltaValue, OperatorError> {
    Ok(DeltaValue::from_json(serde_json::json!({
        JOIN_LEFT_VALUE_FIELD: left.as_json(),
        JOIN_RIGHT_VALUE_FIELD: right.as_json(),
    })))
}

fn unmatched_left_join_output_value(left: &DeltaValue) -> Result<DeltaValue, OperatorError> {
    join_output_value(left, &DeltaValue::from_json(Value::Null))
}

fn unmatched_right_join_output_value(right: &DeltaValue) -> Result<DeltaValue, OperatorError> {
    join_output_value(&DeltaValue::from_json(Value::Null), right)
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

fn validate_builtin_runtime_identity(
    identity: &StandingProgramIdentity,
) -> Result<(), StandingProgramRuntimeError> {
    if identity
        .builtin_runtime_identities
        .iter()
        .any(|runtime| runtime.name == CRATE_NAME)
    {
        Ok(())
    } else {
        Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "builtin_runtime_identities",
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
    let group_keys = supported_view_plan_group_keys(plan);
    if output.columns.len() < group_keys.len() {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.columns",
        });
    }
    let (key_columns, aggregate_columns) = output.columns.split_at(group_keys.len());
    if output.primary_key
        != key_columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema",
        });
    }
    for (output_key, group_key) in key_columns.iter().zip(group_keys.iter()) {
        let (expected_type, expected_nullable) =
            if let Some(input_column_id) = &group_key.input_column_id {
                let input_column = catalog_column(catalog, input_column_id)?;
                (
                    sql_type_from_catalog_column(input_column)?,
                    input_column.nullable,
                )
            } else if group_key.expression.is_some() {
                (SqlDataType::Int64, false)
            } else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "output_schema",
                });
            };
        if output_key.name != group_key.output_column_id
            || output_key.data_type != expected_type
            || output_key.nullable != expected_nullable
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "output_schema",
            });
        }
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

fn validate_filter_project_supported_schemas(
    catalog: &VelorixRelationCatalogV1,
    input: &RelationSchema,
    output: &RelationSchema,
    plan: &SupportedFilterProjectPlan,
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
    let [key, value_columns @ ..] = output.columns.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema.columns",
        });
    };
    let key_column = catalog_primary_key_column(catalog)?;
    let output_key_input_column = plan
        .output_key_input_column_id
        .as_deref()
        .map(|column_id| catalog_column_by_id(catalog, column_id))
        .transpose()?
        .unwrap_or(key_column);
    let expected_key_name = if plan.output_key_column_id.is_empty() {
        output_key_input_column.name.clone()
    } else {
        plan.output_key_column_id.clone()
    };
    if plan.input_relation_id != catalog.relation_schema.relation_id
        || plan.key_column_id != key_column.column_id
        || output.primary_key != vec![key.name.clone()]
        || key.name != expected_key_name
        || output_key_input_column.column_id == catalog.relation_schema.weight_column_id
        || output_key_input_column.nullable
        || key.data_type != sql_type_from_catalog_column(output_key_input_column)?
        || key.nullable
        || value_columns.len() != plan.value_columns.len()
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "filter_project_output_schema",
        });
    }
    let mut output_names = BTreeSet::from([key.name.clone()]);
    for (output_column, projection) in value_columns.iter().zip(plan.value_columns.iter()) {
        let input_column = catalog_column_by_id(catalog, &projection.input_column_id)?;
        if input_column.column_id == catalog.relation_schema.weight_column_id
            || output_column.name != projection.output_column_id
            || !output_names.insert(output_column.name.clone())
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "filter_project_output_schema",
            });
        }
        let expected_type = if let Some(expression) = &projection.expression {
            if input_column.nullable || output_column.nullable {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "filter_project_output_schema",
                });
            }
            validate_filter_project_projection_expr(catalog, expression)?;
            SqlDataType::Int64
        } else {
            if output_column.nullable != input_column.nullable {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "filter_project_output_schema",
                });
            }
            sql_type_from_catalog_column(input_column)?
        };
        if output_column.data_type != expected_type {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "filter_project_output_schema",
            });
        }
    }
    if let Some(predicate_expr) = &plan.predicate_expr {
        validate_filter_project_predicate_expr_column_references(
            predicate_expr,
            catalog,
            plan,
            key_column,
        )?;
    }
    if let Some(top_k) = &plan.top_k {
        let references_output_key = top_k.order_output_column_id == expected_key_name;
        let references_output_value = plan
            .value_columns
            .iter()
            .any(|column| column.output_column_id == top_k.order_output_column_id);
        let references_hidden_input = if let Some(column_id) = &top_k.order_input_column_id {
            let column = catalog_column_by_id(catalog, column_id)?;
            validate_filter_project_hidden_order_column(catalog, column).is_ok()
        } else {
            false
        };
        if top_k.limit == 0
            || (!references_output_key && !references_output_value && !references_hidden_input)
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "filter_project_top_k",
            });
        }
    }
    Ok(())
}

fn validate_filter_project_projection_expr(
    catalog: &VelorixRelationCatalogV1,
    expr: &SupportedProjectionExpr,
) -> Result<(), StandingProgramRuntimeError> {
    match expr {
        SupportedProjectionExpr::Column { column_id }
        | SupportedProjectionExpr::CoalesceInt64 { column_id, .. } => {
            validate_filter_project_int64_projection_column(catalog, column_id)
        }
        SupportedProjectionExpr::LiteralInt64 { .. } => Ok(()),
        SupportedProjectionExpr::LiteralUtf8 { .. } => Ok(()),
        SupportedProjectionExpr::BinaryInt64 { left, right, .. } => {
            validate_filter_project_projection_expr(catalog, left)?;
            validate_filter_project_projection_expr(catalog, right)
        }
        SupportedProjectionExpr::AbsInt64 { expr } => {
            validate_filter_project_projection_expr(catalog, expr)
        }
        SupportedProjectionExpr::GreatestInt64 { exprs }
        | SupportedProjectionExpr::LeastInt64 { exprs } => {
            for expr in exprs {
                validate_filter_project_projection_expr(catalog, expr)?;
            }
            Ok(())
        }
        SupportedProjectionExpr::CaseInt64 {
            predicate,
            then_expr,
            else_expr,
        } => {
            validate_filter_project_case_predicate_expr(catalog, predicate)?;
            validate_filter_project_projection_expr(catalog, then_expr)?;
            validate_filter_project_projection_expr(catalog, else_expr)
        }
        SupportedProjectionExpr::LengthUtf8 { expr } => {
            validate_filter_project_projection_expr(catalog, expr)
        }
        SupportedProjectionExpr::ConcatUtf8 { exprs } => {
            for expr in exprs {
                validate_filter_project_projection_expr(catalog, expr)?;
            }
            Ok(())
        }
        SupportedProjectionExpr::SubstringUtf8 {
            expr,
            start,
            length,
        } => {
            validate_filter_project_projection_expr(catalog, expr)?;
            validate_filter_project_projection_expr(catalog, start)?;
            if let Some(l) = length {
                validate_filter_project_projection_expr(catalog, l)?;
            }
            Ok(())
        }
        SupportedProjectionExpr::TrimUtf8 { expr } => {
            validate_filter_project_projection_expr(catalog, expr)
        }
    }
}

fn validate_filter_project_hidden_order_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
) -> Result<(), StandingProgramRuntimeError> {
    if column.column_id == catalog.relation_schema.weight_column_id
        || column.nullable
        || !matches!(
            column.physical_arrow_type,
            ArrowPhysicalTypeV1::Boolean
                | ArrowPhysicalTypeV1::Utf8
                | ArrowPhysicalTypeV1::Int64
                | ArrowPhysicalTypeV1::Float64
                | ArrowPhysicalTypeV1::Decimal128 { .. }
        )
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "filter_project_top_k",
        });
    }
    Ok(())
}

fn validate_filter_project_int64_projection_column(
    catalog: &VelorixRelationCatalogV1,
    column_id: &str,
) -> Result<(), StandingProgramRuntimeError> {
    let column = catalog_column_by_id(catalog, column_id)?;
    if column.column_id == catalog.relation_schema.weight_column_id
        || column.nullable
        || !matches!(column.physical_arrow_type, ArrowPhysicalTypeV1::Int64)
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "filter_project_projection_expr",
        });
    }
    Ok(())
}

fn validate_filter_project_case_predicate_expr(
    catalog: &VelorixRelationCatalogV1,
    predicate_expr: &RowPredicateExpr,
) -> Result<(), StandingProgramRuntimeError> {
    if row_predicate_expr_contains_scalar_int64_comparison(predicate_expr) {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "filter_project_projection_expr",
        });
    }
    for predicate in predicate_expr.leaf_predicates() {
        let column = catalog_column_by_id(catalog, &predicate.column_id)?;
        let boolean_column = matches!(column.physical_arrow_type, ArrowPhysicalTypeV1::Boolean);
        if column.column_id == catalog.relation_schema.weight_column_id
            || !matches!(
                column.physical_arrow_type,
                ArrowPhysicalTypeV1::Int64 | ArrowPhysicalTypeV1::Boolean
            )
            || (boolean_column && !matches!(predicate.op, PredicateOp::Eq | PredicateOp::NotEq))
            || (boolean_column && !predicate.literal.is_boolean())
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "filter_project_projection_expr",
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
        || ordering_column.nullable
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
    if let Some(predicate_expr) = &plan.predicate_expr {
        validate_row_predicate_expr_column_references(
            predicate_expr,
            false,
            "latest_by_key_plan.predicate.column",
            &mut |column_id| {
                if column_id != key_column.column_id
                    && column_id != value_column.column_id
                    && column_id != ordering_column.column_id
                {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "latest_by_key_plan.predicate.column",
                    });
                }
                Ok(())
            },
        )?;
    }
    let expected_key_type = sql_type_from_catalog_column(key_column)?;
    let expected_value_type = sql_type_from_catalog_column(value_column)?;
    let expected_key_name = if plan.output_key_column_id.is_empty() {
        key_column.name.clone()
    } else {
        plan.output_key_column_id.clone()
    };
    if output.primary_key != vec![key.name.clone()]
        || key.name != expected_key_name
        || key.data_type != expected_key_type
        || key.nullable
        || value.name != plan.output_value_column_id
        || value.data_type != expected_value_type
        || value.nullable != value_column.nullable
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "output_schema",
        });
    }
    Ok(())
}

fn validate_analytic_row_number_supported_schemas(
    catalog: &VelorixRelationCatalogV1,
    input: &RelationSchema,
    output: &RelationSchema,
    plan: &SupportedAnalyticRowNumberPlan,
) -> Result<(), StandingProgramRuntimeError> {
    let expected_input = catalog_input_relation_schema(catalog).map_err(|_| {
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "analytic_row_number_input_schema",
        }
    })?;
    if input != &expected_input || plan.input_relation_id != catalog.relation_schema.relation_id {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "analytic_row_number_input_schema",
        });
    }
    let key_column = catalog_primary_key_column(catalog)?;
    let partition_column = catalog_column_by_id(catalog, &plan.partition_column_id)?;
    let order_column = catalog_column_by_id(catalog, &plan.order_column_id)?;
    if plan.key_column_id != key_column.column_id
        || partition_column.column_id == catalog.relation_schema.weight_column_id
        || order_column.column_id == catalog.relation_schema.weight_column_id
        || partition_column.nullable
        || order_column.nullable
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "analytic_row_number_view_plan",
        });
    }
    if let Some(predicate_expr) = &plan.predicate_expr {
        validate_row_predicate_expr_column_references(
            predicate_expr,
            true,
            "analytic_row_number_predicate",
            &mut |column_id| {
                let column = catalog_column_by_id(catalog, column_id)?;
                if column.column_id == catalog.relation_schema.weight_column_id || column.nullable {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "analytic_row_number_predicate",
                    });
                }
                Ok(())
            },
        )?;
    }
    let [key, rank] = output.columns.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "analytic_row_number_output_schema",
        });
    };
    let output_key = if plan.output_key_column_id.is_empty() {
        key_column.name.as_str()
    } else {
        plan.output_key_column_id.as_str()
    };
    if key.name != output_key
        || key.data_type != sql_type_from_catalog_column(key_column)?
        || key.nullable
        || rank.name != plan.output_row_number_column_id
        || rank.data_type != SqlDataType::Int64
        || rank.nullable
        || output.primary_key != vec![key.name.clone()]
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "analytic_row_number_output_schema",
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
    match plan.window_kind {
        SupportedEventTimeWindowKind::Tumbling => {
            if plan.hop_slide_ns.is_some() || plan.session_gap_ns.is_some() {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "tumbling_window_plan.window_kind",
                });
            }
        }
        SupportedEventTimeWindowKind::Hopping => {
            let Some(slide_ns) = plan.hop_slide_ns else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "tumbling_window_plan.hop_slide_ns",
                });
            };
            if slide_ns <= 0
                || plan.window_size_ns < slide_ns
                || plan.window_size_ns % slide_ns != 0
            {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "tumbling_window_plan.hop_slide_ns",
                });
            }
            if plan.session_gap_ns.is_some() {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "tumbling_window_plan.session_gap_ns",
                });
            }
        }
        SupportedEventTimeWindowKind::Session => {
            if plan.session_gap_ns != Some(plan.window_size_ns) || plan.hop_slide_ns.is_some() {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "tumbling_window_plan.session_gap_ns",
                });
            }
        }
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
    if let Some(predicate_expr) = &plan.predicate_expr {
        validate_row_predicate_expr_column_references(
            predicate_expr,
            false,
            "tumbling_window_plan.predicate.column",
            &mut |column_id| {
                if column_id != key_column.column_id
                    && column_id != value_column.column_id
                    && column_id != event_time_column.column_id
                {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "tumbling_window_plan.predicate.column",
                    });
                }
                Ok(())
            },
        )?;
    }
    for (output_column_id, predicate_expr) in &plan.aggregate_filter_exprs {
        let Some(output) = plan
            .aggregate_outputs
            .iter()
            .find(|output| output.output_column_id == *output_column_id)
        else {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "tumbling_window_plan.aggregate_filter_exprs",
            });
        };
        if !matches!(
            output.function,
            LogicalPlanAggregateFunctionV1::Sum
                | LogicalPlanAggregateFunctionV1::Avg
                | LogicalPlanAggregateFunctionV1::Min
                | LogicalPlanAggregateFunctionV1::Max
                | LogicalPlanAggregateFunctionV1::Count
                | LogicalPlanAggregateFunctionV1::CountDistinct
        ) || output
            .input_column_id
            .as_ref()
            .is_some_and(|input_column_id| input_column_id != &plan.sum_value_column_id)
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "tumbling_window_plan.aggregate_filter_exprs",
            });
        }
        validate_row_predicate_expr_column_references(
            predicate_expr,
            false,
            "tumbling_window_plan.aggregate_filter_exprs",
            &mut |column_id| {
                if column_id != key_column.column_id
                    && column_id != value_column.column_id
                    && column_id != event_time_column.column_id
                {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "tumbling_window_plan.aggregate_filter_exprs",
                    });
                }
                Ok(())
            },
        )?;
    }
    let expected_key_type = sql_type_from_catalog_column(key_column)?;
    let expected_key_name = if plan.output_key_column_id.is_empty() {
        key_column.name.clone()
    } else {
        plan.output_key_column_id.clone()
    };
    if output.primary_key
        != vec![
            key.name.clone(),
            window_start.name.clone(),
            window_end.name.clone(),
        ]
        || key.name != expected_key_name
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
    validate_having_predicates_for_outputs(
        None,
        plan.having_expr.as_ref(),
        &plan.aggregate_outputs,
        "tumbling_window_plan.having",
    )?;
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
    let group_keys = supported_view_plan_group_keys(plan);
    if group_keys.is_empty() && !supported_view_plan_is_singleton(plan) {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.group_keys",
        });
    }
    if supported_view_plan_is_singleton(plan) {
        let aggregate_outputs = supported_view_plan_aggregate_outputs(plan);
        let [count] = aggregate_outputs.as_slice() else {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.singleton",
            });
        };
        if count.function != LogicalPlanAggregateFunctionV1::Count
            || count.input_column_id.is_some()
            || count.input_expression.is_some()
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.singleton",
            });
        }
    }
    let mut output_ids = BTreeSet::new();
    for key in &group_keys {
        if !output_ids.insert(key.output_column_id.as_str()) {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.group_keys",
            });
        }
        match (&key.input_column_id, &key.expression) {
            (Some(column_id), None) => {
                let column = catalog_column(catalog, column_id)?;
                if column.column_id == catalog.relation_schema.weight_column_id {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "generic_view_plan.group_keys",
                    });
                }
            }
            (None, Some(expression)) => {
                validate_filter_project_projection_expr(catalog, expression)?;
                let columns = projection_expr_column_ids(expression);
                if columns.is_empty()
                    || columns
                        .iter()
                        .any(|column_id| column_id == &catalog.relation_schema.weight_column_id)
                {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "generic_view_plan.group_keys",
                    });
                }
            }
            _ => {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.group_keys",
                });
            }
        }
    }
    aggregate_value_mode_for_plan(catalog, plan)?;
    let aggregate_outputs = supported_view_plan_aggregate_outputs(plan);
    let multi_input_column_ids = single_key_multi_input_column_ids(plan);
    for output in &aggregate_outputs {
        aggregate_output_sql_type(catalog, output)?;
        if let Some(expression) = &output.input_expression {
            if multi_input_column_ids.is_some() {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.aggregate_outputs.input_expression",
                });
            }
            if !matches!(
                output.function,
                LogicalPlanAggregateFunctionV1::Sum
                    | LogicalPlanAggregateFunctionV1::Avg
                    | LogicalPlanAggregateFunctionV1::Min
                    | LogicalPlanAggregateFunctionV1::Max
            ) {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.aggregate_outputs.input_expression",
                });
            }
            let columns = projection_expr_column_ids(expression);
            if columns.len() != 1 || columns[0] != plan.sum_value_column_id {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.aggregate_outputs.input_expression",
                });
            }
        }
        if let Some(input_column_id) = &output.input_column_id {
            if matches!(
                output.function,
                LogicalPlanAggregateFunctionV1::Count
                    | LogicalPlanAggregateFunctionV1::CountDistinct
            ) {
                let column = catalog_column(catalog, input_column_id)?;
                if single_key_count_only_input_column(plan).is_none()
                    && single_key_count_distinct_input_column(plan).is_none()
                    && input_column_id != &plan.sum_value_column_id
                {
                    let nullable_count_distinct = column.nullable
                        && output.function == LogicalPlanAggregateFunctionV1::CountDistinct;
                    if multi_input_column_ids.is_none()
                        || column.column_id == catalog.relation_schema.weight_column_id
                        || (column.nullable && !nullable_count_distinct)
                        || column.physical_arrow_type != ArrowPhysicalTypeV1::Int64
                    {
                        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                            field: "generic_view_plan.aggregate_outputs",
                        });
                    }
                }
            } else if input_column_id != &plan.sum_value_column_id {
                if multi_input_column_ids.is_none() {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "generic_view_plan.aggregate_outputs",
                    });
                }
                let column = catalog_column(catalog, input_column_id)?;
                if column.column_id == catalog.relation_schema.weight_column_id
                    || column.nullable
                    || column.physical_arrow_type != ArrowPhysicalTypeV1::Int64
                {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "generic_view_plan.aggregate_outputs",
                    });
                }
            }
        }
    }
    if let Some(predicate_expr) = &plan.predicate_expr {
        validate_row_predicate_expr_column_references(
            predicate_expr,
            true,
            "generic_view_plan.predicate.column",
            &mut |column_id| {
                let column = catalog_column(catalog, column_id)?;
                if column.column_id != catalog_primary_key_column(catalog)?.column_id
                    && column.column_id != plan.sum_value_column_id
                {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "generic_view_plan.predicate.column",
                    });
                }
                Ok(())
            },
        )?;
    } else if let Some(predicate) = &plan.predicate {
        let column = catalog_column(catalog, &predicate.column_id)?;
        if column.column_id != catalog_primary_key_column(catalog)?.column_id
            && column.column_id != plan.sum_value_column_id
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.predicate.column",
            });
        }
    }
    for (output_column_id, predicate_expr) in &plan.aggregate_filter_exprs {
        let Some(output) = aggregate_outputs
            .iter()
            .find(|output| output.output_column_id == *output_column_id)
        else {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.aggregate_filter_exprs",
            });
        };
        if !matches!(
            output.function,
            LogicalPlanAggregateFunctionV1::Sum
                | LogicalPlanAggregateFunctionV1::Count
                | LogicalPlanAggregateFunctionV1::CountDistinct
                | LogicalPlanAggregateFunctionV1::Avg
                | LogicalPlanAggregateFunctionV1::Min
                | LogicalPlanAggregateFunctionV1::Max
        ) || output
            .input_column_id
            .as_ref()
            .is_some_and(|input_column_id| input_column_id != &plan.sum_value_column_id)
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.aggregate_filter_exprs",
            });
        }
        validate_row_predicate_expr_column_references(
            predicate_expr,
            false,
            "generic_view_plan.aggregate_filter_exprs",
            &mut |column_id| {
                let column = catalog_column(catalog, column_id)?;
                if column.column_id != catalog_primary_key_column(catalog)?.column_id
                    && column.column_id != plan.sum_value_column_id
                {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "generic_view_plan.aggregate_filter_exprs",
                    });
                }
                Ok(())
            },
        )?;
    }
    validate_having_predicates_for_outputs(
        plan.having.as_ref(),
        plan.having_expr.as_ref(),
        &aggregate_outputs,
        "generic_view_plan.having",
    )?;
    Ok(())
}

fn single_key_count_only_input_column(plan: &SupportedViewPlan) -> Option<&str> {
    let [output] = plan.aggregate_outputs.as_slice() else {
        return None;
    };
    if output.function == LogicalPlanAggregateFunctionV1::Count {
        output.input_column_id.as_deref()
    } else {
        None
    }
}

fn single_key_count_distinct_input_column(plan: &SupportedViewPlan) -> Option<&str> {
    let [output] = plan.aggregate_outputs.as_slice() else {
        return None;
    };
    if output.function == LogicalPlanAggregateFunctionV1::CountDistinct {
        output.input_column_id.as_deref()
    } else {
        None
    }
}

fn single_key_multi_input_column_ids(plan: &SupportedViewPlan) -> Option<Vec<String>> {
    let column_ids = supported_view_plan_aggregate_outputs(plan)
        .iter()
        .filter_map(|output| output.input_column_id.clone())
        .collect::<BTreeSet<_>>();
    if column_ids.len() > 1 {
        Some(column_ids.into_iter().collect())
    } else {
        None
    }
}

fn single_key_nullable_value_count_input_column(plan: &SupportedViewPlan) -> Option<&str> {
    plan.aggregate_outputs.iter().find_map(|output| {
        if output.function == LogicalPlanAggregateFunctionV1::Count
            && output.input_column_id.as_deref() == Some(plan.sum_value_column_id.as_str())
        {
            output.input_column_id.as_deref()
        } else {
            None
        }
    })
}

fn single_key_sum_coalesce_fallback(plan: &SupportedViewPlan) -> Option<i64> {
    plan.aggregate_outputs.iter().find_map(|output| {
        if output.function != LogicalPlanAggregateFunctionV1::Sum
            || output.input_column_id.as_deref() != Some(plan.sum_value_column_id.as_str())
        {
            return None;
        }
        match output.input_expression.as_ref()? {
            SupportedProjectionExpr::CoalesceInt64 {
                column_id,
                fallback,
            } if column_id == &plan.sum_value_column_id => Some(*fallback),
            _ => None,
        }
    })
}

fn single_key_plan_uses_runtime_aggregate_state(plan: &SupportedViewPlan) -> bool {
    plan.aggregate_output_identity.is_some()
        || single_key_multi_input_column_ids(plan).is_some()
        || !plan.aggregate_filter_exprs.is_empty()
        || supported_view_plan_aggregate_outputs(plan)
            .iter()
            .any(|output| output.input_expression.is_some())
}

fn join_plan_uses_runtime_aggregate_state(plan: &SupportedJoinViewPlan) -> bool {
    supported_join_view_plan_is_singleton(plan)
        || plan.composite_equality.is_some()
        || matches!(
            plan.join_kind,
            SupportedJoinKind::Left | SupportedJoinKind::Full
        )
        || !plan.aggregate_filter_exprs.is_empty()
        || supported_join_view_plan_aggregate_outputs(plan)
            .iter()
            .any(|output| {
                output.input_expression.is_some()
                    || output.input_relation_side
                        == Some(SupportedAggregateInputRelationSide::Right)
                    || matches!(
                        output.function,
                        LogicalPlanAggregateFunctionV1::Avg
                            | LogicalPlanAggregateFunctionV1::Min
                            | LogicalPlanAggregateFunctionV1::Max
                    )
            })
}

fn validate_having_predicates_for_outputs(
    having: Option<&AggregateOutputPredicate>,
    having_expr: Option<&AggregateOutputPredicateExpr>,
    aggregate_outputs: &[SupportedAggregateOutput],
    field: &'static str,
) -> Result<(), StandingProgramRuntimeError> {
    let predicates = having_expr
        .map(AggregateOutputPredicateExpr::leaf_predicates)
        .or_else(|| having.cloned().map(|having| vec![having]))
        .unwrap_or_default();
    for predicate in predicates {
        if !aggregate_outputs
            .iter()
            .any(|output| output.output_column_id == predicate.output_column_id)
        {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field });
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
        input_record_column_value(record, &column.column_id)
    } else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.predicate.column",
        });
    };
    compare_catalog_scalar(column, actual, predicate.op, &predicate.literal)
}

fn input_record_column_value<'a>(record: &'a DeltaRecord, column_id: &str) -> &'a Value {
    let value = record.value.as_json();
    value
        .as_object()
        .and_then(|object| object.get(column_id))
        .unwrap_or(value)
}

fn predicate_expr_matches_record(
    predicate_expr: &RowPredicateExpr,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedViewPlan,
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    match predicate_expr {
        RowPredicateExpr::Atom { predicate } => {
            predicate_matches_record(predicate, catalog, plan, record)
        }
        RowPredicateExpr::ScalarInt64Comparison {
            left,
            comparison_op,
            literal,
        } => {
            let mut input = Map::new();
            input.insert(
                catalog_primary_key_column(catalog)?.column_id.clone(),
                record.key.as_json().clone(),
            );
            input.insert(
                plan.sum_value_column_id.clone(),
                input_record_column_value(record, &plan.sum_value_column_id).clone(),
            );
            scalar_int64_comparison_matches_input(left, *comparison_op, literal, &input, catalog)
        }
        RowPredicateExpr::ScalarInt64ExpressionComparison { .. } => {
            Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.predicate",
            })
        }
        RowPredicateExpr::And { left, right } => {
            Ok(predicate_expr_matches_record(left, catalog, plan, record)?
                && predicate_expr_matches_record(right, catalog, plan, record)?)
        }
        RowPredicateExpr::Or { left, right } => {
            Ok(predicate_expr_matches_record(left, catalog, plan, record)?
                || predicate_expr_matches_record(right, catalog, plan, record)?)
        }
    }
}

fn filter_delta_batch_for_plan(
    delta: &DeltaBatch,
    plan: &SupportedViewPlan,
    catalog: &VelorixRelationCatalogV1,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if let Some(predicate_expr) = &plan.predicate_expr {
        let mut records = Vec::new();
        for record in delta.records() {
            if predicate_expr_matches_record(predicate_expr, catalog, plan, record)? {
                records.push(record.clone());
            }
        }
        return Ok(DeltaBatch::from_records(records));
    }
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

fn apply_filtered_single_key_aggregate_delta(
    current: &DeltaBatch,
    input: &DeltaBatch,
    plan: &SupportedViewPlan,
    catalog: &VelorixRelationCatalogV1,
) -> Result<(DeltaBatch, DeltaBatch), StandingProgramRuntimeError> {
    let aggregate_outputs = supported_view_plan_aggregate_outputs(plan);
    let mut rows = BTreeMap::new();
    for row in current.net_rows().map_err(|_| invalid_runtime_state())? {
        if row.weight != 1 {
            return Err(invalid_runtime_state());
        }
        rows.insert(canonical_json(row.key.as_json()), row);
    }

    let mut before_rows: BTreeMap<String, Option<DeltaRecord>> = BTreeMap::new();
    for record in input.records() {
        let mut updates = Vec::new();
        for aggregate in &aggregate_outputs {
            let include = match plan
                .aggregate_filter_exprs
                .get(aggregate.output_column_id.as_str())
            {
                Some(predicate_expr) => {
                    predicate_expr_matches_record(predicate_expr, catalog, plan, record)?
                }
                None => true,
            };
            if !include {
                continue;
            }
            let update = match aggregate.function {
                LogicalPlanAggregateFunctionV1::Sum => FilteredAggregateUpdate::AddI64 {
                    aggregate: aggregate.clone(),
                    delta: aggregate_input_i64_value(
                        aggregate,
                        plan.sum_value_column_id.as_str(),
                        record,
                        catalog,
                    )?
                    .checked_mul(record.weight)
                    .ok_or_else(invalid_runtime_state)?,
                    qualifying_count_delta: None,
                },
                LogicalPlanAggregateFunctionV1::Avg => FilteredAggregateUpdate::Avg {
                    aggregate: aggregate.clone(),
                    amount: aggregate_input_f64_value(
                        aggregate,
                        plan.sum_value_column_id.as_str(),
                        record,
                        catalog,
                    )?,
                },
                LogicalPlanAggregateFunctionV1::Count => FilteredAggregateUpdate::AddI64 {
                    aggregate: aggregate.clone(),
                    delta: record.weight,
                    qualifying_count_delta: None,
                },
                LogicalPlanAggregateFunctionV1::CountDistinct => {
                    let input_value = aggregate_input_value(
                        aggregate,
                        plan.sum_value_column_id.as_str(),
                        record,
                        catalog,
                    )?;
                    if input_value.is_null() {
                        continue;
                    }
                    FilteredAggregateUpdate::Multiset {
                        aggregate: aggregate.clone(),
                        value: input_value,
                    }
                }
                LogicalPlanAggregateFunctionV1::Min | LogicalPlanAggregateFunctionV1::Max => {
                    FilteredAggregateUpdate::Multiset {
                        aggregate: aggregate.clone(),
                        value: Value::Number(JsonNumber::from(aggregate_input_i64_value(
                            aggregate,
                            plan.sum_value_column_id.as_str(),
                            record,
                            catalog,
                        )?)),
                    }
                }
            };
            updates.push(update);
        }
        if updates.is_empty() {
            continue;
        }

        let key = canonical_json(record.key.as_json());
        before_rows
            .entry(key.clone())
            .or_insert_with(|| rows.get(&key).cloned());
        let row = rows
            .entry(key.clone())
            .or_insert_with(|| zeroed_filtered_aggregate_record(record, &aggregate_outputs));
        let row_value = row
            .value
            .as_json()
            .as_object()
            .cloned()
            .ok_or_else(invalid_runtime_state)?;
        let mut row_value = row_value;
        for update in updates {
            match update {
                FilteredAggregateUpdate::Multiset {
                    aggregate,
                    value: multiset_value,
                } => {
                    update_filtered_multiset_value(
                        &mut row_value,
                        &aggregate.output_column_id,
                        &multiset_value,
                        record.weight,
                    )?;
                }
                FilteredAggregateUpdate::AddI64 {
                    aggregate,
                    delta,
                    qualifying_count_delta: _,
                } => {
                    let current = row_value
                        .get(&aggregate.output_column_id)
                        .map(json_i64_value)
                        .transpose()?
                        .unwrap_or_default();
                    let next = current
                        .checked_add(delta)
                        .ok_or_else(invalid_runtime_state)?;
                    row_value.insert(
                        aggregate.output_column_id,
                        Value::Number(JsonNumber::from(next)),
                    );
                }
                FilteredAggregateUpdate::Avg { aggregate, amount } => {
                    update_filtered_avg_value(
                        &mut row_value,
                        &aggregate.output_column_id,
                        amount,
                        record.weight,
                    )?;
                }
            }
        }
        row.value = DeltaValue::from_json(Value::Object(row_value));
        if !filtered_aggregate_record_is_live(row, &aggregate_outputs)? {
            rows.remove(&key);
        }
    }

    let mut output = Vec::new();
    for (key, before) in before_rows {
        let after = rows.get(&key).cloned();
        if before == after {
            continue;
        }
        if let Some(before) = before {
            output.push(before.inverse().map_err(|_| invalid_runtime_state())?);
        }
        if let Some(after) = after {
            output.push(after);
        }
    }
    let next = DeltaBatch::from_records(rows.into_values());
    validate_published_output(&next)?;
    Ok((next, DeltaBatch::from_records(output)))
}

enum FilteredAggregateUpdate {
    AddI64 {
        aggregate: SupportedAggregateOutput,
        delta: i64,
        qualifying_count_delta: Option<i64>,
    },
    Avg {
        aggregate: SupportedAggregateOutput,
        amount: f64,
    },
    Multiset {
        aggregate: SupportedAggregateOutput,
        value: Value,
    },
}

fn apply_filtered_join_aggregate_delta(
    current: &DeltaBatch,
    input: &DeltaBatch,
    plan: &SupportedJoinViewPlan,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<(DeltaBatch, DeltaBatch), StandingProgramRuntimeError> {
    let aggregate_outputs = supported_join_view_plan_aggregate_outputs(plan);
    let mut rows = BTreeMap::new();
    for row in current.net_rows().map_err(|_| invalid_runtime_state())? {
        if row.weight != 1 {
            return Err(invalid_runtime_state());
        }
        rows.insert(canonical_json(row.key.as_json()), row);
    }

    let mut before_rows: BTreeMap<String, Option<DeltaRecord>> = BTreeMap::new();
    for record in input.records() {
        let extended_left_state = left_join_uses_extended_aggregate_state(plan);
        let mut updates = Vec::new();
        for aggregate in &aggregate_outputs {
            let include = match plan
                .aggregate_filter_exprs
                .get(aggregate.output_column_id.as_str())
            {
                Some(predicate_expr) => join_predicate_expr_matches_joined_record(
                    predicate_expr,
                    plan,
                    catalogs,
                    record,
                )?,
                None => true,
            };
            if !include {
                continue;
            }
            let input_value = join_aggregate_input_value(aggregate, plan, catalogs, record)?;
            let update = match aggregate.function {
                LogicalPlanAggregateFunctionV1::Sum if input_value.is_null() => continue,
                LogicalPlanAggregateFunctionV1::Sum => FilteredAggregateUpdate::AddI64 {
                    aggregate: aggregate.clone(),
                    delta: json_i64_value(&input_value)?
                        .checked_mul(record.weight)
                        .ok_or_else(invalid_runtime_state)?,
                    qualifying_count_delta: extended_left_state.then_some(record.weight),
                },
                LogicalPlanAggregateFunctionV1::Count
                    if aggregate.input_column_id.is_some() && input_value.is_null() =>
                {
                    continue;
                }
                LogicalPlanAggregateFunctionV1::Count => FilteredAggregateUpdate::AddI64 {
                    aggregate: aggregate.clone(),
                    delta: record.weight,
                    qualifying_count_delta: None,
                },
                LogicalPlanAggregateFunctionV1::CountDistinct if input_value.is_null() => {
                    continue;
                }
                LogicalPlanAggregateFunctionV1::CountDistinct => {
                    FilteredAggregateUpdate::Multiset {
                        aggregate: aggregate.clone(),
                        value: input_value.clone(),
                    }
                }
                LogicalPlanAggregateFunctionV1::Avg if input_value.is_null() => continue,
                LogicalPlanAggregateFunctionV1::Avg => FilteredAggregateUpdate::Avg {
                    aggregate: aggregate.clone(),
                    amount: aggregate_sum_as_f64(&input_value)?,
                },
                LogicalPlanAggregateFunctionV1::Min | LogicalPlanAggregateFunctionV1::Max
                    if input_value.is_null() =>
                {
                    continue;
                }
                LogicalPlanAggregateFunctionV1::Min | LogicalPlanAggregateFunctionV1::Max => {
                    FilteredAggregateUpdate::Multiset {
                        aggregate: aggregate.clone(),
                        value: input_value.clone(),
                    }
                }
            };
            updates.push(update);
        }
        if updates.is_empty() && !extended_left_state {
            continue;
        }

        let aggregate_key = if supported_join_view_plan_is_singleton(plan) {
            DeltaKey::from_json(singleton_aggregate_key("join_state"))
        } else if plan.composite_equality.is_some() {
            let values = record
                .key
                .as_json()
                .as_array()
                .ok_or_else(invalid_runtime_state)?;
            DeltaKey::from_json(values.first().cloned().ok_or_else(invalid_runtime_state)?)
        } else {
            record.key.clone()
        };
        let key = canonical_json(aggregate_key.as_json());
        before_rows
            .entry(key.clone())
            .or_insert_with(|| rows.get(&key).cloned());
        let row = rows.entry(key.clone()).or_insert_with(|| {
            let zero = zeroed_filtered_aggregate_record(record, &aggregate_outputs);
            DeltaRecord::new(aggregate_key, zero.value, zero.weight)
        });
        let value = row
            .value
            .as_json()
            .as_object()
            .cloned()
            .ok_or_else(invalid_runtime_state)?;
        let mut value = value;
        if extended_left_state {
            update_hidden_count(&mut value, left_join_group_row_count_key(), record.weight)?;
        }
        for update in updates {
            match update {
                FilteredAggregateUpdate::Multiset {
                    aggregate,
                    value: multiset_value,
                } => {
                    update_filtered_multiset_value(
                        &mut value,
                        &aggregate.output_column_id,
                        &multiset_value,
                        record.weight,
                    )?;
                }
                FilteredAggregateUpdate::AddI64 {
                    aggregate,
                    delta,
                    qualifying_count_delta,
                } => {
                    let current = value
                        .get(&aggregate.output_column_id)
                        .map(json_i64_value)
                        .transpose()?
                        .unwrap_or_default();
                    let next = current
                        .checked_add(delta)
                        .ok_or_else(invalid_runtime_state)?;
                    value.insert(
                        aggregate.output_column_id.clone(),
                        Value::Number(JsonNumber::from(next)),
                    );
                    if let Some(count_delta) = qualifying_count_delta {
                        update_hidden_count(
                            &mut value,
                            &sum_qualifying_count_key(&aggregate.output_column_id),
                            count_delta,
                        )?;
                    }
                }
                FilteredAggregateUpdate::Avg { aggregate, amount } => {
                    update_filtered_avg_value(
                        &mut value,
                        &aggregate.output_column_id,
                        amount,
                        record.weight,
                    )?;
                }
            }
        }
        row.value = DeltaValue::from_json(Value::Object(value));
        if !filtered_aggregate_record_is_live(row, &aggregate_outputs)? {
            rows.remove(&key);
        }
    }

    let mut output = Vec::new();
    for (key, before) in before_rows {
        let after = rows.get(&key).cloned();
        if before == after {
            continue;
        }
        if let Some(before) = before {
            output.push(before.inverse().map_err(|_| invalid_runtime_state())?);
        }
        if let Some(after) = after {
            output.push(after);
        }
    }
    let next = DeltaBatch::from_records(rows.into_values());
    validate_published_output(&next)?;
    Ok((next, DeltaBatch::from_records(output)))
}

fn publish_join_aggregate_state(
    state: &DeltaBatch,
    plan: &SupportedJoinViewPlan,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if !supported_join_view_plan_is_singleton(plan) {
        return Ok(state.clone());
    }
    let rows = state.net_rows().map_err(|_| invalid_runtime_state())?;
    let value = match rows.as_slice() {
        [] => {
            let aggregate_outputs = supported_join_view_plan_aggregate_outputs(plan);
            let [count] = aggregate_outputs.as_slice() else {
                return Err(invalid_runtime_state());
            };
            let mut value = Map::new();
            value.insert(
                count.output_column_id.clone(),
                Value::Number(JsonNumber::from(0)),
            );
            DeltaValue::from_json(Value::Object(value))
        }
        [row] if row.weight == 1 => row.value.clone(),
        _ => return Err(invalid_runtime_state()),
    };
    Ok(DeltaBatch::from_records(vec![DeltaRecord::new(
        DeltaKey::from_json(singleton_aggregate_key("join_publication")),
        value,
        1,
    )]))
}

fn zeroed_filtered_aggregate_record(
    input: &DeltaRecord,
    aggregate_outputs: &[SupportedAggregateOutput],
) -> DeltaRecord {
    let mut value = serde_json::Map::new();
    for aggregate in aggregate_outputs {
        let initial = match aggregate.function {
            LogicalPlanAggregateFunctionV1::CountDistinct => Value::Array(Vec::new()),
            LogicalPlanAggregateFunctionV1::Avg => zeroed_filtered_avg_value(),
            LogicalPlanAggregateFunctionV1::Min | LogicalPlanAggregateFunctionV1::Max => {
                Value::Array(Vec::new())
            }
            _ => Value::Number(JsonNumber::from(0)),
        };
        value.insert(aggregate.output_column_id.clone(), initial);
    }
    DeltaRecord::new(
        input.key.clone(),
        DeltaValue::from_json(Value::Object(value)),
        1,
    )
}

fn filtered_aggregate_record_is_live(
    record: &DeltaRecord,
    aggregate_outputs: &[SupportedAggregateOutput],
) -> Result<bool, StandingProgramRuntimeError> {
    let value = record
        .value
        .as_json()
        .as_object()
        .ok_or_else(invalid_runtime_state)?;
    if let Some(group_row_count) = value.get(left_join_group_row_count_key()) {
        let group_row_count = group_row_count.as_i64().ok_or_else(invalid_runtime_state)?;
        if group_row_count < 0 {
            return Err(invalid_runtime_state());
        }
        return Ok(group_row_count > 0);
    }
    for aggregate in aggregate_outputs {
        match aggregate.function {
            LogicalPlanAggregateFunctionV1::CountDistinct => {
                if value
                    .get(&aggregate.output_column_id)
                    .and_then(Value::as_array)
                    .is_some_and(|values| !values.is_empty())
                {
                    return Ok(true);
                }
            }
            LogicalPlanAggregateFunctionV1::Avg => {
                if value
                    .get(&aggregate.output_column_id)
                    .and_then(Value::as_object)
                    .and_then(|value| value.get("count"))
                    .and_then(Value::as_i64)
                    .is_some_and(|count| count > 0)
                {
                    return Ok(true);
                }
            }
            LogicalPlanAggregateFunctionV1::Min | LogicalPlanAggregateFunctionV1::Max => {
                if value
                    .get(&aggregate.output_column_id)
                    .and_then(Value::as_array)
                    .is_some_and(|values| !values.is_empty())
                {
                    return Ok(true);
                }
            }
            _ => {
                if value
                    .get(&aggregate.output_column_id)
                    .map(json_i64_value)
                    .transpose()?
                    .unwrap_or_default()
                    != 0
                {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

fn left_join_uses_extended_aggregate_state(plan: &SupportedJoinViewPlan) -> bool {
    plan.join_kind == SupportedJoinKind::Full
        || (plan.join_kind == SupportedJoinKind::Left
            && (supported_join_view_plan_aggregate_outputs(plan)
                .iter()
                .any(|output| {
                    output.input_relation_side == Some(SupportedAggregateInputRelationSide::Right)
                })
                || plan.aggregate_filter_exprs.values().any(|predicate| {
                    join_predicate_expr_references_relation(
                        predicate,
                        &plan.right_input_relation_id,
                    )
                })
                || plan.predicate_expr.as_ref().is_some_and(|predicate| {
                    join_predicate_expr_references_relation(
                        predicate,
                        &plan.right_input_relation_id,
                    )
                })))
}

fn join_predicate_expr_references_relation(expr: &JoinPredicateExpr, relation_id: &str) -> bool {
    match expr {
        JoinPredicateExpr::Atom { predicate } => predicate.relation_id == relation_id,
        JoinPredicateExpr::ScalarInt64Comparison {
            relation_id: predicate_relation_id,
            ..
        } => predicate_relation_id == relation_id,
        JoinPredicateExpr::ScalarInt64ExpressionComparison {
            left_relation_id,
            right_relation_id,
            ..
        } => left_relation_id == relation_id || right_relation_id == relation_id,
        JoinPredicateExpr::And { left, right } | JoinPredicateExpr::Or { left, right } => {
            join_predicate_expr_references_relation(left, relation_id)
                || join_predicate_expr_references_relation(right, relation_id)
        }
    }
}

fn left_join_group_row_count_key() -> &'static str {
    "__velorix_left_join_group_row_count_v1"
}

fn sum_qualifying_count_key(output_column_id: &str) -> String {
    format!("__velorix_sum_qualifying_count_v1:{output_column_id}")
}

fn update_hidden_count(
    value: &mut serde_json::Map<String, Value>,
    key: &str,
    delta: i64,
) -> Result<(), StandingProgramRuntimeError> {
    let current = value.get(key).and_then(Value::as_i64).unwrap_or_default();
    let next = current
        .checked_add(delta)
        .ok_or_else(invalid_runtime_state)?;
    if next < 0 {
        return Err(invalid_runtime_state());
    }
    value.insert(key.to_string(), Value::Number(JsonNumber::from(next)));
    Ok(())
}

fn update_filtered_multiset_value(
    row: &mut serde_json::Map<String, Value>,
    output_column_id: &str,
    distinct_value: &Value,
    weight: i64,
) -> Result<(), StandingProgramRuntimeError> {
    let mut values = row
        .get(output_column_id)
        .and_then(Value::as_array)
        .ok_or_else(invalid_runtime_state)?
        .iter()
        .map(|entry| {
            let entry = entry.as_object().ok_or_else(invalid_runtime_state)?;
            let value = entry
                .get("value")
                .cloned()
                .ok_or_else(invalid_runtime_state)?;
            let weight = entry
                .get("weight")
                .and_then(Value::as_i64)
                .ok_or_else(invalid_runtime_state)?;
            Ok((canonical_json(&value), (value, weight)))
        })
        .collect::<Result<BTreeMap<_, _>, StandingProgramRuntimeError>>()?;
    let key = canonical_json(distinct_value);
    let next_weight = values
        .get(&key)
        .map_or(0, |(_, weight)| *weight)
        .checked_add(weight)
        .ok_or_else(invalid_runtime_state)?;
    if next_weight < 0 {
        return Err(invalid_runtime_state());
    }
    if next_weight == 0 {
        values.remove(&key);
    } else {
        values.insert(key, (distinct_value.clone(), next_weight));
    }
    row.insert(
        output_column_id.to_string(),
        Value::Array(
            values
                .into_values()
                .map(|(value, weight)| {
                    let mut entry = serde_json::Map::new();
                    entry.insert("value".to_string(), value);
                    entry.insert(
                        "weight".to_string(),
                        Value::Number(JsonNumber::from(weight)),
                    );
                    Value::Object(entry)
                })
                .collect(),
        ),
    );
    Ok(())
}

fn update_filtered_avg_value(
    row: &mut serde_json::Map<String, Value>,
    output_column_id: &str,
    amount: f64,
    weight: i64,
) -> Result<(), StandingProgramRuntimeError> {
    let current = row
        .get(output_column_id)
        .and_then(Value::as_object)
        .ok_or_else(invalid_runtime_state)?;
    let sum = current
        .get("sum")
        .and_then(Value::as_f64)
        .ok_or_else(invalid_runtime_state)?;
    let count = current
        .get("count")
        .and_then(Value::as_i64)
        .ok_or_else(invalid_runtime_state)?;
    let next_sum = sum + amount * weight as f64;
    if !next_sum.is_finite() {
        return Err(invalid_runtime_state());
    }
    let next_count = count
        .checked_add(weight)
        .ok_or_else(invalid_runtime_state)?;
    if next_count < 0 {
        return Err(invalid_runtime_state());
    }
    row.insert(
        output_column_id.to_string(),
        filtered_avg_value(next_sum, next_count)?,
    );
    Ok(())
}

fn zeroed_filtered_avg_value() -> Value {
    let mut value = serde_json::Map::new();
    value.insert("sum".to_string(), Value::Number(JsonNumber::from(0)));
    value.insert("count".to_string(), Value::Number(JsonNumber::from(0)));
    Value::Object(value)
}

fn filtered_avg_value(sum: f64, count: i64) -> Result<Value, StandingProgramRuntimeError> {
    let mut value = serde_json::Map::new();
    value.insert(
        "sum".to_string(),
        JsonNumber::from_f64(sum)
            .map(Value::Number)
            .ok_or_else(invalid_runtime_state)?,
    );
    value.insert("count".to_string(), Value::Number(JsonNumber::from(count)));
    Ok(Value::Object(value))
}

fn aggregate_input_record_value<'a>(
    aggregate: &SupportedAggregateOutput,
    value_column_id: &str,
    record: &'a DeltaRecord,
) -> &'a Value {
    let input_column_id = aggregate
        .input_column_id
        .as_deref()
        .unwrap_or(value_column_id);
    input_record_column_value(record, input_column_id)
}

fn aggregate_input_i64_value(
    aggregate: &SupportedAggregateOutput,
    value_column_id: &str,
    record: &DeltaRecord,
    catalog: &VelorixRelationCatalogV1,
) -> Result<i64, StandingProgramRuntimeError> {
    let Some(expression) = &aggregate.input_expression else {
        return json_i64_value(aggregate_input_record_value(
            aggregate,
            value_column_id,
            record,
        ));
    };
    let mut input = Map::new();
    input.insert(value_column_id.to_string(), record.value.as_json().clone());
    evaluate_projection_expr(expression, &input, catalog)
}

fn aggregate_input_value(
    aggregate: &SupportedAggregateOutput,
    value_column_id: &str,
    record: &DeltaRecord,
    catalog: &VelorixRelationCatalogV1,
) -> Result<Value, StandingProgramRuntimeError> {
    let Some(expression) = &aggregate.input_expression else {
        return Ok(aggregate_input_record_value(aggregate, value_column_id, record).clone());
    };
    let mut input = Map::new();
    input.insert(value_column_id.to_string(), record.value.as_json().clone());
    Ok(Value::Number(JsonNumber::from(evaluate_projection_expr(
        expression, &input, catalog,
    )?)))
}

fn aggregate_input_f64_value(
    aggregate: &SupportedAggregateOutput,
    value_column_id: &str,
    record: &DeltaRecord,
    catalog: &VelorixRelationCatalogV1,
) -> Result<f64, StandingProgramRuntimeError> {
    let Some(expression) = &aggregate.input_expression else {
        return aggregate_sum_as_f64(aggregate_input_record_value(
            aggregate,
            value_column_id,
            record,
        ));
    };
    let mut input = Map::new();
    input.insert(value_column_id.to_string(), record.value.as_json().clone());
    Ok(evaluate_projection_expr(expression, &input, catalog)? as f64)
}

fn json_i64_value(value: &Value) -> Result<i64, StandingProgramRuntimeError> {
    value.as_i64().ok_or_else(invalid_runtime_state)
}

fn filter_project_predicate_matches_record(
    predicate: &RowPredicate,
    catalog: &VelorixRelationCatalogV1,
    _plan: &SupportedFilterProjectPlan,
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    let column = catalog_column(catalog, &predicate.column_id)?;
    let actual = if column.column_id == catalog_primary_key_column(catalog)?.column_id {
        record.key.as_json()
    } else {
        record
            .value
            .as_json()
            .as_object()
            .and_then(|object| object.get(column.column_id.as_str()))
            .ok_or_else(invalid_runtime_state)?
    };
    compare_catalog_scalar(column, actual, predicate.op, &predicate.literal)
}

fn filter_project_record_projection_input(
    record: &DeltaRecord,
    catalog: &VelorixRelationCatalogV1,
) -> Result<Map<String, Value>, StandingProgramRuntimeError> {
    let mut input = record
        .value
        .as_json()
        .as_object()
        .cloned()
        .ok_or_else(invalid_runtime_state)?;
    input.insert(
        catalog_primary_key_column(catalog)?.column_id.clone(),
        record.key.as_json().clone(),
    );
    Ok(input)
}

fn filter_project_input_column_ids(plan: &SupportedFilterProjectPlan) -> Vec<String> {
    let mut columns = Vec::new();
    if let Some(output_key_input_column_id) = &plan.output_key_input_column_id {
        push_filter_project_input_column(&mut columns, plan, output_key_input_column_id.clone());
    }
    for projection in &plan.value_columns {
        push_filter_project_input_column(&mut columns, plan, projection.input_column_id.clone());
        if let Some(expression) = &projection.expression {
            for column_id in projection_expr_column_ids(expression) {
                push_filter_project_input_column(&mut columns, plan, column_id);
            }
        }
    }
    if let Some(predicate_expr) = &plan.predicate_expr {
        for column_id in row_predicate_expr_column_ids(predicate_expr) {
            push_filter_project_input_column(&mut columns, plan, column_id);
        }
    }
    if let Some(order_input_column_id) = plan
        .top_k
        .as_ref()
        .and_then(|top_k| top_k.order_input_column_id.clone())
    {
        push_filter_project_input_column(&mut columns, plan, order_input_column_id);
    }
    columns
}

fn push_filter_project_input_column(
    columns: &mut Vec<String>,
    plan: &SupportedFilterProjectPlan,
    column_id: String,
) {
    if column_id != plan.key_column_id && !columns.iter().any(|column| column == &column_id) {
        columns.push(column_id);
    }
}

fn projection_expr_column_ids(expr: &SupportedProjectionExpr) -> Vec<String> {
    let mut columns = Vec::new();
    collect_projection_expr_column_ids(expr, &mut columns);
    columns
}

fn row_predicate_expr_column_ids(expr: &RowPredicateExpr) -> Vec<String> {
    let mut columns = Vec::new();
    collect_row_predicate_expr_column_ids(expr, &mut columns);
    columns
}

fn collect_row_predicate_expr_column_ids(expr: &RowPredicateExpr, columns: &mut Vec<String>) {
    match expr {
        RowPredicateExpr::Atom { predicate } => {
            if !columns
                .iter()
                .any(|existing| existing == &predicate.column_id)
            {
                columns.push(predicate.column_id.clone());
            }
        }
        RowPredicateExpr::ScalarInt64Comparison { left, .. } => {
            for column_id in projection_expr_column_ids(left) {
                if !columns.iter().any(|existing| existing == &column_id) {
                    columns.push(column_id);
                }
            }
        }
        RowPredicateExpr::ScalarInt64ExpressionComparison { left, right, .. } => {
            for column_id in projection_expr_column_ids(left)
                .into_iter()
                .chain(projection_expr_column_ids(right))
            {
                if !columns.iter().any(|existing| existing == &column_id) {
                    columns.push(column_id);
                }
            }
        }
        RowPredicateExpr::And { left, right } | RowPredicateExpr::Or { left, right } => {
            collect_row_predicate_expr_column_ids(left, columns);
            collect_row_predicate_expr_column_ids(right, columns);
        }
    }
}

fn row_predicate_expr_contains_scalar_int64_comparison(expr: &RowPredicateExpr) -> bool {
    match expr {
        RowPredicateExpr::ScalarInt64Comparison { .. }
        | RowPredicateExpr::ScalarInt64ExpressionComparison { .. } => true,
        RowPredicateExpr::Atom { .. } => false,
        RowPredicateExpr::And { left, right } | RowPredicateExpr::Or { left, right } => {
            row_predicate_expr_contains_scalar_int64_comparison(left)
                || row_predicate_expr_contains_scalar_int64_comparison(right)
        }
    }
}

fn validate_row_predicate_expr_column_references<F>(
    expr: &RowPredicateExpr,
    allow_scalar_int64_comparison: bool,
    field: &'static str,
    validate_column: &mut F,
) -> Result<(), StandingProgramRuntimeError>
where
    F: FnMut(&str) -> Result<(), StandingProgramRuntimeError>,
{
    match expr {
        RowPredicateExpr::Atom { predicate } => validate_column(&predicate.column_id),
        RowPredicateExpr::ScalarInt64Comparison { left, .. } => {
            if !allow_scalar_int64_comparison {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field });
            }
            let column_ids = projection_expr_column_ids(left);
            let [column_id] = column_ids.as_slice() else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field });
            };
            validate_column(column_id)
        }
        RowPredicateExpr::ScalarInt64ExpressionComparison { left, right, .. } => {
            if !allow_scalar_int64_comparison {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field });
            }
            let column_ids = projection_expr_column_ids(left)
                .into_iter()
                .chain(projection_expr_column_ids(right));
            let mut saw_column = false;
            for column_id in column_ids {
                saw_column = true;
                validate_column(&column_id)?;
            }
            if !saw_column {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field });
            }
            Ok(())
        }
        RowPredicateExpr::And { left, right } | RowPredicateExpr::Or { left, right } => {
            validate_row_predicate_expr_column_references(
                left,
                allow_scalar_int64_comparison,
                field,
                validate_column,
            )?;
            validate_row_predicate_expr_column_references(
                right,
                allow_scalar_int64_comparison,
                field,
                validate_column,
            )
        }
    }
}

fn validate_filter_project_predicate_expr_column_references(
    expr: &RowPredicateExpr,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedFilterProjectPlan,
    key_column: &RelationColumnV1,
) -> Result<(), StandingProgramRuntimeError> {
    match expr {
        RowPredicateExpr::Atom { predicate } => validate_filter_project_predicate_column(
            catalog,
            plan,
            key_column,
            &predicate.column_id,
        ),
        RowPredicateExpr::ScalarInt64Comparison { left, .. } => {
            let column_ids = projection_expr_column_ids(left);
            let [column_id] = column_ids.as_slice() else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "filter_project_predicate",
                });
            };
            validate_filter_project_predicate_column(catalog, plan, key_column, column_id)
        }
        RowPredicateExpr::ScalarInt64ExpressionComparison { left, right, .. } => {
            let column_ids = projection_expr_column_ids(left)
                .into_iter()
                .chain(projection_expr_column_ids(right));
            let mut saw_column = false;
            for column_id in column_ids {
                saw_column = true;
                let column = catalog_column_by_id(catalog, &column_id)?;
                if column.column_id == catalog.relation_schema.weight_column_id
                    || column.nullable
                    || !matches!(column.physical_arrow_type, ArrowPhysicalTypeV1::Int64)
                {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "filter_project_predicate",
                    });
                }
            }
            if !saw_column {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "filter_project_predicate",
                });
            }
            Ok(())
        }
        RowPredicateExpr::And { left, right } | RowPredicateExpr::Or { left, right } => {
            validate_filter_project_predicate_expr_column_references(
                left, catalog, plan, key_column,
            )?;
            validate_filter_project_predicate_expr_column_references(
                right, catalog, plan, key_column,
            )
        }
    }
}

fn validate_filter_project_predicate_column(
    catalog: &VelorixRelationCatalogV1,
    _plan: &SupportedFilterProjectPlan,
    _key_column: &RelationColumnV1,
    column_id: &str,
) -> Result<(), StandingProgramRuntimeError> {
    let column = catalog_column_by_id(catalog, column_id)?;
    if column.column_id == catalog.relation_schema.weight_column_id {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "filter_project_predicate",
        });
    }
    Ok(())
}

fn collect_projection_expr_column_ids(expr: &SupportedProjectionExpr, columns: &mut Vec<String>) {
    match expr {
        SupportedProjectionExpr::Column { column_id } => {
            if !columns.iter().any(|existing| existing == column_id) {
                columns.push(column_id.clone());
            }
        }
        SupportedProjectionExpr::LiteralInt64 { .. } => {}
        SupportedProjectionExpr::LiteralUtf8 { .. } => {}
        SupportedProjectionExpr::BinaryInt64 { left, right, .. } => {
            collect_projection_expr_column_ids(left, columns);
            collect_projection_expr_column_ids(right, columns);
        }
        SupportedProjectionExpr::AbsInt64 { expr } => {
            collect_projection_expr_column_ids(expr, columns);
        }
        SupportedProjectionExpr::GreatestInt64 { exprs }
        | SupportedProjectionExpr::LeastInt64 { exprs } => {
            for expr in exprs {
                collect_projection_expr_column_ids(expr, columns);
            }
        }
        SupportedProjectionExpr::CoalesceInt64 { column_id, .. } => {
            if !columns.iter().any(|existing| existing == column_id) {
                columns.push(column_id.clone());
            }
        }
        SupportedProjectionExpr::CaseInt64 {
            predicate,
            then_expr,
            else_expr,
        } => {
            for column_id in row_predicate_expr_column_ids(predicate) {
                if !columns.iter().any(|existing| existing == &column_id) {
                    columns.push(column_id);
                }
            }
            collect_projection_expr_column_ids(then_expr, columns);
            collect_projection_expr_column_ids(else_expr, columns);
        }
        SupportedProjectionExpr::LengthUtf8 { expr } => {
            collect_projection_expr_column_ids(expr, columns);
        }
        SupportedProjectionExpr::ConcatUtf8 { exprs } => {
            for expr in exprs {
                collect_projection_expr_column_ids(expr, columns);
            }
        }
        SupportedProjectionExpr::SubstringUtf8 {
            expr,
            start,
            length,
        } => {
            collect_projection_expr_column_ids(expr, columns);
            collect_projection_expr_column_ids(start, columns);
            if let Some(l) = length {
                collect_projection_expr_column_ids(l, columns);
            }
        }
        SupportedProjectionExpr::TrimUtf8 { expr } => {
            collect_projection_expr_column_ids(expr, columns);
        }
    }
}

fn filter_project_predicate_expr_matches_record(
    predicate_expr: &RowPredicateExpr,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedFilterProjectPlan,
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    match predicate_expr {
        RowPredicateExpr::Atom { predicate } => {
            filter_project_predicate_matches_record(predicate, catalog, plan, record)
        }
        RowPredicateExpr::ScalarInt64Comparison {
            left,
            comparison_op,
            literal,
        } => {
            let input = filter_project_record_projection_input(record, catalog)?;
            scalar_int64_comparison_matches_input(left, *comparison_op, literal, &input, catalog)
        }
        RowPredicateExpr::ScalarInt64ExpressionComparison {
            left,
            comparison_op,
            right,
        } => {
            let input = filter_project_record_projection_input(record, catalog)?;
            scalar_int64_expression_comparison_matches_input(
                left,
                *comparison_op,
                right,
                &input,
                catalog,
            )
        }
        RowPredicateExpr::And { left, right } => Ok(filter_project_predicate_expr_matches_record(
            left, catalog, plan, record,
        )?
            && filter_project_predicate_expr_matches_record(right, catalog, plan, record)?),
        RowPredicateExpr::Or { left, right } => Ok(filter_project_predicate_expr_matches_record(
            left, catalog, plan, record,
        )?
            || filter_project_predicate_expr_matches_record(right, catalog, plan, record)?),
    }
}

fn filter_delta_batch_for_filter_project_plan(
    delta: &DeltaBatch,
    plan: &SupportedFilterProjectPlan,
    catalog: &VelorixRelationCatalogV1,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let Some(predicate_expr) = &plan.predicate_expr else {
        return Ok(delta.clone());
    };
    let mut records = Vec::new();
    for record in delta.records() {
        if filter_project_predicate_expr_matches_record(predicate_expr, catalog, plan, record)? {
            records.push(record.clone());
        }
    }
    Ok(DeltaBatch::from_records(records))
}

fn project_filter_project_delta_batch(
    delta: &DeltaBatch,
    plan: &SupportedFilterProjectPlan,
    catalog: &VelorixRelationCatalogV1,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let mut records = Vec::new();
    for record in delta.records() {
        let input = record
            .value
            .as_json()
            .as_object()
            .ok_or_else(invalid_runtime_state)?;
        let mut output = Map::new();
        for column in &plan.value_columns {
            let value = if let Some(expression) = &column.expression {
                Value::Number(JsonNumber::from(evaluate_projection_expr(
                    expression, input, catalog,
                )?))
            } else {
                input
                    .get(column.input_column_id.as_str())
                    .cloned()
                    .ok_or_else(invalid_runtime_state)?
            };
            output.insert(column.output_column_id.clone(), value);
        }
        if let Some(order_input_column_id) = plan
            .top_k
            .as_ref()
            .and_then(|top_k| top_k.order_input_column_id.as_ref())
        {
            let value = input
                .get(order_input_column_id.as_str())
                .cloned()
                .ok_or_else(invalid_runtime_state)?;
            output.insert(filter_project_hidden_order_value_key().to_string(), value);
        }
        let output_key = if let Some(output_key_input_column_id) = &plan.output_key_input_column_id
        {
            DeltaKey::from_json(
                input
                    .get(output_key_input_column_id.as_str())
                    .cloned()
                    .ok_or_else(invalid_runtime_state)?,
            )
        } else {
            record.key.clone()
        };
        records.push(DeltaRecord::new(
            output_key,
            DeltaValue::from_json(Value::Object(output)),
            record.weight,
        ));
    }
    Ok(DeltaBatch::from_records(records))
}

fn analytic_row_number_input_column_ids(plan: &SupportedAnalyticRowNumberPlan) -> Vec<String> {
    let mut columns = Vec::new();
    push_analytic_row_number_input_column(&mut columns, plan, plan.partition_column_id.clone());
    push_analytic_row_number_input_column(&mut columns, plan, plan.order_column_id.clone());
    if let Some(predicate_expr) = &plan.predicate_expr {
        for column_id in row_predicate_expr_column_ids(predicate_expr) {
            push_analytic_row_number_input_column(&mut columns, plan, column_id);
        }
    }
    if columns.is_empty() {
        columns.push(plan.key_column_id.clone());
    }
    columns
}

fn push_analytic_row_number_input_column(
    columns: &mut Vec<String>,
    plan: &SupportedAnalyticRowNumberPlan,
    column_id: String,
) {
    if column_id != plan.key_column_id && !columns.iter().any(|column| column == &column_id) {
        columns.push(column_id);
    }
}

fn filter_delta_batch_for_analytic_row_number_plan(
    delta: &DeltaBatch,
    plan: &SupportedAnalyticRowNumberPlan,
    catalog: &VelorixRelationCatalogV1,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let Some(predicate_expr) = &plan.predicate_expr else {
        return Ok(delta.clone());
    };
    let mut records = Vec::new();
    for record in delta.records() {
        if analytic_row_number_predicate_expr_matches_record(predicate_expr, catalog, record)? {
            records.push(record.clone());
        }
    }
    Ok(DeltaBatch::from_records(records))
}

fn analytic_row_number_predicate_expr_matches_record(
    predicate_expr: &RowPredicateExpr,
    catalog: &VelorixRelationCatalogV1,
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    match predicate_expr {
        RowPredicateExpr::Atom { predicate } => {
            let input = analytic_row_number_record_input(record, catalog)?;
            let actual = input
                .get(predicate.column_id.as_str())
                .ok_or_else(invalid_runtime_state)?;
            let column = catalog_column(catalog, &predicate.column_id)?;
            compare_catalog_scalar(column, actual, predicate.op, &predicate.literal)
        }
        RowPredicateExpr::ScalarInt64Comparison {
            left,
            comparison_op,
            literal,
        } => {
            let input = analytic_row_number_record_input(record, catalog)?;
            scalar_int64_comparison_matches_input(left, *comparison_op, literal, &input, catalog)
        }
        RowPredicateExpr::ScalarInt64ExpressionComparison {
            left,
            comparison_op,
            right,
        } => {
            let input = analytic_row_number_record_input(record, catalog)?;
            scalar_int64_expression_comparison_matches_input(
                left,
                *comparison_op,
                right,
                &input,
                catalog,
            )
        }
        RowPredicateExpr::And { left, right } => Ok(
            analytic_row_number_predicate_expr_matches_record(left, catalog, record)?
                && analytic_row_number_predicate_expr_matches_record(right, catalog, record)?,
        ),
        RowPredicateExpr::Or { left, right } => Ok(
            analytic_row_number_predicate_expr_matches_record(left, catalog, record)?
                || analytic_row_number_predicate_expr_matches_record(right, catalog, record)?,
        ),
    }
}

fn analytic_row_number_record_input(
    record: &DeltaRecord,
    catalog: &VelorixRelationCatalogV1,
) -> Result<Map<String, Value>, StandingProgramRuntimeError> {
    let mut input = record
        .value
        .as_json()
        .as_object()
        .cloned()
        .ok_or_else(invalid_runtime_state)?;
    input.insert(
        catalog_primary_key_column(catalog)?.column_id.clone(),
        record.key.as_json().clone(),
    );
    Ok(input)
}

fn evaluate_projection_expr(
    expr: &SupportedProjectionExpr,
    input: &Map<String, Value>,
    catalog: &VelorixRelationCatalogV1,
) -> Result<i64, StandingProgramRuntimeError> {
    match expr {
        SupportedProjectionExpr::Column { column_id } => input
            .get(column_id.as_str())
            .and_then(Value::as_i64)
            .ok_or_else(invalid_runtime_state),
        SupportedProjectionExpr::LiteralInt64 { value } => Ok(*value),
        SupportedProjectionExpr::LiteralUtf8 { .. } => {
            // String literals cannot be used in Int64 context
            Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "string_literal_in_int64_context",
            })
        }
        SupportedProjectionExpr::BinaryInt64 { op, left, right } => {
            let left = evaluate_projection_expr(left, input, catalog)?;
            let right = evaluate_projection_expr(right, input, catalog)?;
            match op {
                SupportedProjectionBinaryOp::Add => {
                    left.checked_add(right).ok_or_else(invalid_runtime_state)
                }
                SupportedProjectionBinaryOp::Subtract => {
                    left.checked_sub(right).ok_or_else(invalid_runtime_state)
                }
                SupportedProjectionBinaryOp::Multiply => {
                    left.checked_mul(right).ok_or_else(invalid_runtime_state)
                }
                SupportedProjectionBinaryOp::Divide => {
                    left.checked_div(right).ok_or_else(invalid_runtime_state)
                }
                SupportedProjectionBinaryOp::Modulo => {
                    left.checked_rem(right).ok_or_else(invalid_runtime_state)
                }
            }
        }
        SupportedProjectionExpr::AbsInt64 { expr } => {
            evaluate_projection_expr(expr, input, catalog)?
                .checked_abs()
                .ok_or_else(invalid_runtime_state)
        }
        SupportedProjectionExpr::GreatestInt64 { exprs } => exprs
            .iter()
            .map(|expr| evaluate_projection_expr(expr, input, catalog))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .max()
            .ok_or_else(invalid_runtime_state),
        SupportedProjectionExpr::LeastInt64 { exprs } => exprs
            .iter()
            .map(|expr| evaluate_projection_expr(expr, input, catalog))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .min()
            .ok_or_else(invalid_runtime_state),
        SupportedProjectionExpr::CoalesceInt64 {
            column_id,
            fallback,
        } => match input.get(column_id.as_str()) {
            Some(Value::Null) | None => Ok(*fallback),
            Some(value) => value.as_i64().ok_or_else(invalid_runtime_state),
        },
        SupportedProjectionExpr::CaseInt64 {
            predicate,
            then_expr,
            else_expr,
        } => {
            if projection_predicate_expr_matches_input(predicate, input, catalog)? {
                evaluate_projection_expr(then_expr, input, catalog)
            } else {
                evaluate_projection_expr(else_expr, input, catalog)
            }
        }
        // String expressions cannot be used in Int64 context
        SupportedProjectionExpr::LengthUtf8 { .. }
        | SupportedProjectionExpr::ConcatUtf8 { .. }
        | SupportedProjectionExpr::SubstringUtf8 { .. }
        | SupportedProjectionExpr::TrimUtf8 { .. } => {
            Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "string_expression_in_int64_context",
            })
        }
    }
}

/// Evaluate a projection expression against a published-view input schema.
///
/// This is the catalog-free analog of `evaluate_projection_expr` used by the
/// published-view aggregate path. The narrow Phase 4 slice forbids computed
/// group keys, so only direct columns and Int64 literals are reachable; any
/// wider expression fails closed.
fn evaluate_projection_expr_for_schema(
    expr: &SupportedProjectionExpr,
    input: &Map<String, Value>,
    _input_schema: &RelationSchema,
) -> Result<i64, StandingProgramRuntimeError> {
    match expr {
        SupportedProjectionExpr::Column { column_id } => input
            .get(column_id.as_str())
            .and_then(Value::as_i64)
            .ok_or_else(invalid_runtime_state),
        SupportedProjectionExpr::LiteralInt64 { value } => Ok(*value),
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "published_view_projection_expression_not_supported",
        }),
    }
}

/// Evaluate a string projection expression.
///
/// Returns the string result of evaluating the expression against the input record.
fn evaluate_string_projection_expr(
    expr: &SupportedProjectionExpr,
    input: &Map<String, Value>,
    catalog: &VelorixRelationCatalogV1,
) -> Result<String, StandingProgramRuntimeError> {
    match expr {
        SupportedProjectionExpr::Column { column_id } => input
            .get(column_id.as_str())
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .or_else(|| {
                // Try as Int64 and convert to string
                input
                    .get(column_id.as_str())
                    .and_then(|v| v.as_i64().map(|n| n.to_string()))
            })
            .ok_or_else(invalid_runtime_state),
        SupportedProjectionExpr::LiteralUtf8 { value } => Ok(value.clone()),
        SupportedProjectionExpr::LengthUtf8 { expr } => {
            let s = evaluate_string_projection_expr(expr, input, catalog)?;
            // LENGTH returns byte count (UTF-8)
            Ok(s.len().to_string())
        }
        SupportedProjectionExpr::ConcatUtf8 { exprs } => {
            let mut result = String::new();
            for expr in exprs {
                let s = evaluate_string_projection_expr(expr, input, catalog)?;
                result.push_str(&s);
            }
            Ok(result)
        }
        SupportedProjectionExpr::SubstringUtf8 {
            expr,
            start,
            length,
        } => {
            let s = evaluate_string_projection_expr(expr, input, catalog)?;
            let start_val = evaluate_projection_expr(start, input, catalog)? as usize;
            let len = match length {
                Some(l) => Some(evaluate_projection_expr(l, input, catalog)? as usize),
                None => None,
            };
            // SQL SUBSTRING is 1-indexed
            let start_idx = if start_val > 0 { start_val - 1 } else { 0 };
            let chars: Vec<char> = s.chars().collect();
            let result: String = match len {
                Some(l) => chars.iter().skip(start_idx).take(l).collect(),
                None => chars.iter().skip(start_idx).collect(),
            };
            Ok(result)
        }
        SupportedProjectionExpr::TrimUtf8 { expr } => {
            let s = evaluate_string_projection_expr(expr, input, catalog)?;
            Ok(s.trim().to_string())
        }
        // Int64 expressions cannot be used in string context
        SupportedProjectionExpr::LiteralInt64 { value } => Ok(value.to_string()),
        SupportedProjectionExpr::BinaryInt64 { .. }
        | SupportedProjectionExpr::AbsInt64 { .. }
        | SupportedProjectionExpr::GreatestInt64 { .. }
        | SupportedProjectionExpr::LeastInt64 { .. }
        | SupportedProjectionExpr::CoalesceInt64 { .. }
        | SupportedProjectionExpr::CaseInt64 { .. } => {
            Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "int64_expression_in_string_context",
            })
        }
    }
}

fn scalar_int64_comparison_matches_input(
    left: &SupportedProjectionExpr,
    comparison_op: PredicateOp,
    literal: &Value,
    input: &Map<String, Value>,
    catalog: &VelorixRelationCatalogV1,
) -> Result<bool, StandingProgramRuntimeError> {
    let column_ids = projection_expr_column_ids(left);
    let [column_id] = column_ids.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "scalar_int64_predicate_expression",
        });
    };
    let column = catalog_column_by_id(catalog, column_id)?;
    let actual = Value::Number(JsonNumber::from(evaluate_projection_expr(
        left, input, catalog,
    )?));
    compare_catalog_scalar(column, &actual, comparison_op, literal)
}

fn scalar_int64_expression_comparison_matches_input(
    left: &SupportedProjectionExpr,
    comparison_op: PredicateOp,
    right: &SupportedProjectionExpr,
    input: &Map<String, Value>,
    catalog: &VelorixRelationCatalogV1,
) -> Result<bool, StandingProgramRuntimeError> {
    let left_column_ids = projection_expr_column_ids(left);
    let right_column_ids = projection_expr_column_ids(right);
    if left_column_ids.is_empty() || right_column_ids.is_empty() {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "scalar_int64_predicate_expression",
        });
    }
    compare_ord(
        i128::from(evaluate_projection_expr(left, input, catalog)?),
        comparison_op,
        i128::from(evaluate_projection_expr(right, input, catalog)?),
    )
}

fn projection_predicate_expr_matches_input(
    predicate_expr: &RowPredicateExpr,
    input: &Map<String, Value>,
    catalog: &VelorixRelationCatalogV1,
) -> Result<bool, StandingProgramRuntimeError> {
    match predicate_expr {
        RowPredicateExpr::Atom { predicate } => {
            let column = catalog_column_by_id(catalog, &predicate.column_id)?;
            let actual = input
                .get(predicate.column_id.as_str())
                .cloned()
                .unwrap_or(Value::Null);
            compare_catalog_scalar(column, &actual, predicate.op, &predicate.literal)
        }
        RowPredicateExpr::ScalarInt64Comparison { .. }
        | RowPredicateExpr::ScalarInt64ExpressionComparison { .. } => {
            Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "filter_project_projection_expr.predicate",
            })
        }
        RowPredicateExpr::And { left, right } => Ok(projection_predicate_expr_matches_input(
            left, input, catalog,
        )? && projection_predicate_expr_matches_input(
            right, input, catalog,
        )?),
        RowPredicateExpr::Or { left, right } => Ok(projection_predicate_expr_matches_input(
            left, input, catalog,
        )? || projection_predicate_expr_matches_input(
            right, input, catalog,
        )?),
    }
}

fn latest_predicate_matches_record(
    predicate: &RowPredicate,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedLatestByKeyPlan,
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    let column = catalog_column(catalog, &predicate.column_id)?;
    let actual = if column.column_id == catalog_primary_key_column(catalog)?.column_id {
        record.key.as_json()
    } else {
        let object = record
            .value
            .as_json()
            .as_object()
            .ok_or_else(invalid_runtime_state)?;
        if column.column_id == plan.value_column_id {
            object.get("value").ok_or_else(invalid_runtime_state)?
        } else if column.column_id == plan.ordering_column_id {
            object.get("ordering").ok_or_else(invalid_runtime_state)?
        } else {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "latest_by_key_plan.predicate.column",
            });
        }
    };
    compare_catalog_scalar(column, actual, predicate.op, &predicate.literal)
}

fn latest_predicate_expr_matches_record(
    predicate_expr: &RowPredicateExpr,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedLatestByKeyPlan,
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    match predicate_expr {
        RowPredicateExpr::Atom { predicate } => {
            latest_predicate_matches_record(predicate, catalog, plan, record)
        }
        RowPredicateExpr::ScalarInt64Comparison { .. }
        | RowPredicateExpr::ScalarInt64ExpressionComparison { .. } => {
            Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "latest_by_key_plan.predicate",
            })
        }
        RowPredicateExpr::And { left, right } => Ok(latest_predicate_expr_matches_record(
            left, catalog, plan, record,
        )? && latest_predicate_expr_matches_record(
            right, catalog, plan, record,
        )?),
        RowPredicateExpr::Or { left, right } => Ok(latest_predicate_expr_matches_record(
            left, catalog, plan, record,
        )? || latest_predicate_expr_matches_record(
            right, catalog, plan, record,
        )?),
    }
}

fn filter_delta_batch_for_latest_plan(
    delta: &DeltaBatch,
    plan: &SupportedLatestByKeyPlan,
    catalog: &VelorixRelationCatalogV1,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let Some(predicate_expr) = &plan.predicate_expr else {
        return Ok(delta.clone());
    };
    let mut records = Vec::new();
    for record in delta.records() {
        if latest_predicate_expr_matches_record(predicate_expr, catalog, plan, record)? {
            records.push(record.clone());
        }
    }
    Ok(DeltaBatch::from_records(records))
}

fn tumbling_predicate_matches_row(
    predicate_expr: &Option<RowPredicateExpr>,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedTumblingWindowPlan,
    group_key: &Value,
    amount: Option<i64>,
    event_time_ns: i64,
) -> Result<bool, StandingProgramRuntimeError> {
    let Some(predicate_expr) = predicate_expr else {
        return Ok(true);
    };
    tumbling_predicate_expr_matches_row(
        predicate_expr,
        catalog,
        plan,
        group_key,
        amount,
        event_time_ns,
    )
}

fn tumbling_predicate_expr_matches_row(
    predicate_expr: &RowPredicateExpr,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedTumblingWindowPlan,
    group_key: &Value,
    amount: Option<i64>,
    event_time_ns: i64,
) -> Result<bool, StandingProgramRuntimeError> {
    match predicate_expr {
        RowPredicateExpr::Atom { predicate } => tumbling_predicate_atom_matches_row(
            predicate,
            catalog,
            plan,
            group_key,
            amount,
            event_time_ns,
        ),
        RowPredicateExpr::ScalarInt64Comparison { .. }
        | RowPredicateExpr::ScalarInt64ExpressionComparison { .. } => {
            Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "tumbling_window_plan.predicate",
            })
        }
        RowPredicateExpr::And { left, right } => Ok(tumbling_predicate_expr_matches_row(
            left,
            catalog,
            plan,
            group_key,
            amount,
            event_time_ns,
        )? && tumbling_predicate_expr_matches_row(
            right,
            catalog,
            plan,
            group_key,
            amount,
            event_time_ns,
        )?),
        RowPredicateExpr::Or { left, right } => Ok(tumbling_predicate_expr_matches_row(
            left,
            catalog,
            plan,
            group_key,
            amount,
            event_time_ns,
        )? || tumbling_predicate_expr_matches_row(
            right,
            catalog,
            plan,
            group_key,
            amount,
            event_time_ns,
        )?),
    }
}

fn tumbling_predicate_atom_matches_row(
    predicate: &RowPredicate,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedTumblingWindowPlan,
    group_key: &Value,
    amount: Option<i64>,
    event_time_ns: i64,
) -> Result<bool, StandingProgramRuntimeError> {
    let column = catalog_column(catalog, &predicate.column_id)?;
    let actual = if column.column_id == plan.group_key_column_id {
        group_key.clone()
    } else if column.column_id == plan.sum_value_column_id {
        amount
            .map(JsonNumber::from)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    } else if column.column_id == plan.event_time_column_id {
        Value::Number(JsonNumber::from(event_time_ns))
    } else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "tumbling_window_plan.predicate.column",
        });
    };
    compare_catalog_scalar(column, &actual, predicate.op, &predicate.literal)
}

fn join_predicate_matches_record(
    predicate: &JoinRowPredicate,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedJoinViewPlan,
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    let column = catalog_column(catalog, &predicate.predicate.column_id)?;
    let actual = if column.column_id == catalog_primary_key_column(catalog)?.column_id {
        record.key.as_json()
    } else if predicate.relation_id == plan.left_input_relation_id
        && column.column_id == plan.sum_value_column_id
    {
        record.value.as_json()
    } else if predicate.relation_id == plan.right_input_relation_id {
        join_right_predicate_value(plan, &column.column_id, record)?
    } else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan.predicate.column",
        });
    };
    compare_catalog_scalar(
        column,
        actual,
        predicate.predicate.op,
        &predicate.predicate.literal,
    )
}

fn join_scalar_int64_predicate_expr_matches_record(
    relation_id: &str,
    left: &SupportedProjectionExpr,
    comparison_op: PredicateOp,
    literal: &Value,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedJoinViewPlan,
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    if relation_id != catalog.relation_schema.relation_id {
        return Ok(true);
    }
    let column_ids = projection_expr_column_ids(left);
    let [column_id] = column_ids.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan.predicate",
        });
    };
    let actual = join_scalar_int64_record_value(relation_id, column_id, catalog, plan, record)?;
    let mut input = Map::new();
    input.insert(column_id.clone(), actual.clone());
    scalar_int64_comparison_matches_input(left, comparison_op, literal, &input, catalog)
}

fn join_scalar_int64_record_value<'a>(
    relation_id: &str,
    column_id: &str,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedJoinViewPlan,
    record: &'a DeltaRecord,
) -> Result<&'a Value, StandingProgramRuntimeError> {
    if column_id == catalog_primary_key_column(catalog)?.column_id {
        return Ok(record.key.as_json());
    }
    if relation_id == plan.left_input_relation_id && column_id == plan.sum_value_column_id {
        return Ok(record.value.as_json());
    }
    if relation_id == plan.right_input_relation_id {
        return join_right_predicate_value(plan, column_id, record);
    }
    Err(StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "generic_join_view_plan.predicate.column",
    })
}

fn join_right_predicate_value<'a>(
    plan: &SupportedJoinViewPlan,
    column_id: &str,
    record: &'a DeltaRecord,
) -> Result<&'a Value, StandingProgramRuntimeError> {
    let right_value_column_ids = supported_join_view_plan_right_value_column_ids(plan);
    if right_value_column_ids.len() == 1 && right_value_column_ids[0] == column_id {
        if let Some(value) = record
            .value
            .as_json()
            .as_object()
            .and_then(|value| value.get(column_id))
        {
            return Ok(value);
        }
        return Ok(record.value.as_json());
    }
    record
        .value
        .as_json()
        .as_object()
        .and_then(|value| value.get(column_id))
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan.predicate.column",
        })
}

fn prefilter_delta_batch_for_join_plan(
    delta: &DeltaBatch,
    plan: &SupportedJoinViewPlan,
    catalog: &VelorixRelationCatalogV1,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if plan.join_kind == SupportedJoinKind::Full
        || (plan.join_kind == SupportedJoinKind::Left
            && catalog.relation_schema.relation_id == plan.right_input_relation_id)
    {
        return Ok(delta.clone());
    }
    if let Some(predicate_expr) = &plan.predicate_expr {
        if predicate_expr.contains_or() {
            return Ok(delta.clone());
        }
        let mut records = Vec::new();
        for record in delta.records() {
            if join_predicate_expr_matches_record(predicate_expr, catalog, plan, record)? {
                records.push(record.clone());
            }
        }
        return Ok(DeltaBatch::from_records(records));
    }
    let predicates: Vec<_> = supported_join_view_plan_predicates(plan)
        .into_iter()
        .filter(|predicate| predicate.relation_id == catalog.relation_schema.relation_id)
        .collect();
    if predicates.is_empty() {
        return Ok(delta.clone());
    };
    let mut records = Vec::new();
    for record in delta.records() {
        let mut matches = true;
        for predicate in &predicates {
            if !join_predicate_matches_record(predicate, catalog, plan, record)? {
                matches = false;
                break;
            }
        }
        if matches {
            records.push(record.clone());
        }
    }
    Ok(DeltaBatch::from_records(records))
}

fn filter_joined_delta_batch_for_join_plan(
    delta: &DeltaBatch,
    plan: &SupportedJoinViewPlan,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let Some(predicate_expr) = &plan.predicate_expr else {
        return Ok(delta.clone());
    };
    let mut records = Vec::new();
    for record in delta.records() {
        if join_predicate_expr_matches_joined_record(predicate_expr, plan, catalogs, record)? {
            records.push(record.clone());
        }
    }
    Ok(DeltaBatch::from_records(records))
}

fn project_joined_delta_batch_to_left_values(
    delta: &DeltaBatch,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    delta
        .records()
        .iter()
        .map(|record| {
            Ok(DeltaRecord::new(
                record.key.clone(),
                DeltaValue::from_json(joined_left_value(record)?.clone()),
                record.weight,
            ))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(DeltaBatch::from_records)
}

fn left_join_match_count(
    state: &DeltaBatch,
    key: &DeltaKey,
) -> Result<i64, StandingProgramRuntimeError> {
    let mut count = 0_i64;
    for row in state
        .net_rows()
        .map_err(|_| invalid_runtime_state())?
        .into_iter()
        .filter(|row| row.key == *key)
    {
        if row.weight < 0 {
            return Err(invalid_runtime_state());
        }
        count = count
            .checked_add(row.weight)
            .ok_or_else(invalid_runtime_state)?;
    }
    Ok(count)
}

fn apply_left_join_left_delta(
    join: &mut JoinOperator,
    input: &DeltaBatch,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let right_state = join.right_state();
    let mut output = join
        .apply_left(input)
        .map_err(|_| invalid_runtime_state())?;
    for row in input.net_rows().map_err(|_| invalid_runtime_state())? {
        if left_join_match_count(&right_state, &row.key)? == 0 {
            output = output.combine(&DeltaBatch::from_records([DeltaRecord::new(
                row.key,
                unmatched_left_join_output_value(&row.value)
                    .map_err(|_| invalid_runtime_state())?,
                row.weight,
            )]));
        }
    }
    output
        .net_rows()
        .map(DeltaBatch::from_records)
        .map_err(|_| invalid_runtime_state())
}

fn apply_left_join_right_delta(
    join: &mut JoinOperator,
    input: &DeltaBatch,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let before = join.right_state();
    let mut output = join
        .apply_right(input)
        .map_err(|_| invalid_runtime_state())?;
    let after = join.right_state();
    let left = join.left_state();
    let touched_keys = input
        .records()
        .iter()
        .map(|row| (canonical_json(row.key.as_json()), row.key.clone()))
        .collect::<BTreeMap<_, _>>();
    for key in touched_keys.into_values() {
        let sign = match (
            left_join_match_count(&before, &key)? == 0,
            left_join_match_count(&after, &key)? == 0,
        ) {
            (true, false) => -1,
            (false, true) => 1,
            _ => continue,
        };
        for row in left
            .net_rows()
            .map_err(|_| invalid_runtime_state())?
            .into_iter()
            .filter(|row| row.key == key)
        {
            let weight = row
                .weight
                .checked_mul(sign)
                .ok_or_else(invalid_runtime_state)?;
            output = output.combine(&DeltaBatch::from_records([DeltaRecord::new(
                row.key,
                unmatched_left_join_output_value(&row.value)
                    .map_err(|_| invalid_runtime_state())?,
                weight,
            )]));
        }
    }
    output
        .net_rows()
        .map(DeltaBatch::from_records)
        .map_err(|_| invalid_runtime_state())
}

fn apply_full_join_left_delta(
    join: &mut JoinOperator,
    input: &DeltaBatch,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let before = join.left_state();
    let right = join.right_state();
    let mut output = join
        .apply_left(input)
        .map_err(|_| invalid_runtime_state())?;
    let after = join.left_state();
    for row in input.net_rows().map_err(|_| invalid_runtime_state())? {
        if left_join_match_count(&right, &row.key)? == 0 {
            output = output.combine(&DeltaBatch::from_records([DeltaRecord::new(
                row.key,
                unmatched_left_join_output_value(&row.value)
                    .map_err(|_| invalid_runtime_state())?,
                row.weight,
            )]));
        }
    }
    let touched_keys = input
        .records()
        .iter()
        .map(|row| (canonical_json(row.key.as_json()), row.key.clone()))
        .collect::<BTreeMap<_, _>>();
    for key in touched_keys.into_values() {
        let sign = match (
            left_join_match_count(&before, &key)? == 0,
            left_join_match_count(&after, &key)? == 0,
        ) {
            (true, false) => -1,
            (false, true) => 1,
            _ => continue,
        };
        for row in right
            .net_rows()
            .map_err(|_| invalid_runtime_state())?
            .into_iter()
            .filter(|row| row.key == key)
        {
            output = output.combine(&DeltaBatch::from_records([DeltaRecord::new(
                row.key,
                unmatched_right_join_output_value(&row.value)
                    .map_err(|_| invalid_runtime_state())?,
                row.weight
                    .checked_mul(sign)
                    .ok_or_else(invalid_runtime_state)?,
            )]));
        }
    }
    output
        .net_rows()
        .map(DeltaBatch::from_records)
        .map_err(|_| invalid_runtime_state())
}

fn apply_full_join_right_delta(
    join: &mut JoinOperator,
    input: &DeltaBatch,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let before = join.right_state();
    let left = join.left_state();
    let mut output = join
        .apply_right(input)
        .map_err(|_| invalid_runtime_state())?;
    let after = join.right_state();
    for row in input.net_rows().map_err(|_| invalid_runtime_state())? {
        if left_join_match_count(&left, &row.key)? == 0 {
            output = output.combine(&DeltaBatch::from_records([DeltaRecord::new(
                row.key,
                unmatched_right_join_output_value(&row.value)
                    .map_err(|_| invalid_runtime_state())?,
                row.weight,
            )]));
        }
    }
    let touched_keys = input
        .records()
        .iter()
        .map(|row| (canonical_json(row.key.as_json()), row.key.clone()))
        .collect::<BTreeMap<_, _>>();
    for key in touched_keys.into_values() {
        let sign = match (
            left_join_match_count(&before, &key)? == 0,
            left_join_match_count(&after, &key)? == 0,
        ) {
            (true, false) => -1,
            (false, true) => 1,
            _ => continue,
        };
        for row in left
            .net_rows()
            .map_err(|_| invalid_runtime_state())?
            .into_iter()
            .filter(|row| row.key == key)
        {
            output = output.combine(&DeltaBatch::from_records([DeltaRecord::new(
                row.key,
                unmatched_left_join_output_value(&row.value)
                    .map_err(|_| invalid_runtime_state())?,
                row.weight
                    .checked_mul(sign)
                    .ok_or_else(invalid_runtime_state)?,
            )]));
        }
    }
    output
        .net_rows()
        .map(DeltaBatch::from_records)
        .map_err(|_| invalid_runtime_state())
}

fn join_predicate_expr_matches_record(
    predicate_expr: &JoinPredicateExpr,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedJoinViewPlan,
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    match predicate_expr {
        JoinPredicateExpr::Atom { predicate } => {
            if predicate.relation_id != catalog.relation_schema.relation_id {
                Ok(true)
            } else {
                join_predicate_matches_record(predicate, catalog, plan, record)
            }
        }
        JoinPredicateExpr::ScalarInt64Comparison {
            relation_id,
            left,
            comparison_op,
            literal,
        } => join_scalar_int64_predicate_expr_matches_record(
            relation_id,
            left,
            *comparison_op,
            literal,
            catalog,
            plan,
            record,
        ),
        JoinPredicateExpr::ScalarInt64ExpressionComparison { .. } => Ok(true),
        JoinPredicateExpr::And { left, right } => Ok(join_predicate_expr_matches_record(
            left, catalog, plan, record,
        )? && join_predicate_expr_matches_record(
            right, catalog, plan, record,
        )?),
        JoinPredicateExpr::Or { left, right } => Ok(join_predicate_expr_matches_record(
            left, catalog, plan, record,
        )? || join_predicate_expr_matches_record(
            right, catalog, plan, record,
        )?),
    }
}

fn join_predicate_expr_matches_joined_record(
    predicate_expr: &JoinPredicateExpr,
    plan: &SupportedJoinViewPlan,
    catalogs: &[VelorixRelationCatalogV1],
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    match predicate_expr {
        JoinPredicateExpr::Atom { predicate } => {
            join_predicate_matches_joined_record(predicate, plan, catalogs, record)
        }
        JoinPredicateExpr::ScalarInt64Comparison {
            relation_id,
            left,
            comparison_op,
            literal,
        } => join_scalar_int64_predicate_expr_matches_joined_record(
            relation_id,
            left,
            *comparison_op,
            literal,
            plan,
            catalogs,
            record,
        ),
        JoinPredicateExpr::ScalarInt64ExpressionComparison {
            left_relation_id,
            left,
            comparison_op,
            right_relation_id,
            right,
        } => join_scalar_int64_expression_comparison_matches_joined_record(
            left_relation_id,
            left,
            *comparison_op,
            right_relation_id,
            right,
            plan,
            catalogs,
            record,
        ),
        JoinPredicateExpr::And { left, right } => Ok(join_predicate_expr_matches_joined_record(
            left, plan, catalogs, record,
        )?
            && join_predicate_expr_matches_joined_record(right, plan, catalogs, record)?),
        JoinPredicateExpr::Or { left, right } => Ok(join_predicate_expr_matches_joined_record(
            left, plan, catalogs, record,
        )?
            || join_predicate_expr_matches_joined_record(right, plan, catalogs, record)?),
    }
}

fn join_predicate_matches_joined_record(
    predicate: &JoinRowPredicate,
    plan: &SupportedJoinViewPlan,
    catalogs: &[VelorixRelationCatalogV1],
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    let catalog = join_catalog_for_relation(catalogs, &predicate.relation_id)?;
    let column = catalog_column(catalog, &predicate.predicate.column_id)?;
    let actual = if column.column_id == catalog_primary_key_column(catalog)?.column_id {
        record.key.as_json()
    } else if predicate.relation_id == plan.left_input_relation_id
        && column.column_id == plan.sum_value_column_id
    {
        joined_left_value(record)?
    } else if predicate.relation_id == plan.right_input_relation_id {
        joined_right_predicate_value(plan, &column.column_id, record)?
    } else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan.predicate.column",
        });
    };
    compare_catalog_scalar(
        column,
        actual,
        predicate.predicate.op,
        &predicate.predicate.literal,
    )
}

fn join_scalar_int64_predicate_expr_matches_joined_record(
    relation_id: &str,
    left: &SupportedProjectionExpr,
    comparison_op: PredicateOp,
    literal: &Value,
    plan: &SupportedJoinViewPlan,
    catalogs: &[VelorixRelationCatalogV1],
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    let catalog = join_catalog_for_relation(catalogs, relation_id)?;
    let column_ids = projection_expr_column_ids(left);
    let [column_id] = column_ids.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan.predicate",
        });
    };
    let actual =
        join_scalar_int64_joined_record_value(relation_id, column_id, plan, catalog, record)?;
    let mut input = Map::new();
    input.insert(column_id.clone(), actual.clone());
    scalar_int64_comparison_matches_input(left, comparison_op, literal, &input, catalog)
}

#[allow(clippy::too_many_arguments)]
fn join_scalar_int64_expression_comparison_matches_joined_record(
    left_relation_id: &str,
    left: &SupportedProjectionExpr,
    comparison_op: PredicateOp,
    right_relation_id: &str,
    right: &SupportedProjectionExpr,
    plan: &SupportedJoinViewPlan,
    catalogs: &[VelorixRelationCatalogV1],
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    let left_catalog = join_catalog_for_relation(catalogs, left_relation_id)?;
    let right_catalog = join_catalog_for_relation(catalogs, right_relation_id)?;
    let left_value = join_scalar_int64_joined_expression_value(
        left_relation_id,
        left,
        plan,
        left_catalog,
        record,
    )?;
    let right_value = join_scalar_int64_joined_expression_value(
        right_relation_id,
        right,
        plan,
        right_catalog,
        record,
    )?;
    let (Some(left_value), Some(right_value)) = (left_value, right_value) else {
        return Ok(false);
    };
    compare_ord(
        i128::from(left_value),
        comparison_op,
        i128::from(right_value),
    )
}

fn join_scalar_int64_joined_expression_value(
    relation_id: &str,
    expression: &SupportedProjectionExpr,
    plan: &SupportedJoinViewPlan,
    catalog: &VelorixRelationCatalogV1,
    record: &DeltaRecord,
) -> Result<Option<i64>, StandingProgramRuntimeError> {
    let column_ids = projection_expr_column_ids(expression);
    let [column_id] = column_ids.as_slice() else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan.predicate",
        });
    };
    let actual =
        join_scalar_int64_joined_record_value(relation_id, column_id, plan, catalog, record)?;
    if actual.is_null() {
        return Ok(None);
    }
    let mut input = Map::new();
    input.insert(column_id.clone(), actual.clone());
    evaluate_projection_expr(expression, &input, catalog).map(Some)
}

fn join_scalar_int64_joined_record_value<'a>(
    relation_id: &str,
    column_id: &str,
    plan: &SupportedJoinViewPlan,
    catalog: &VelorixRelationCatalogV1,
    record: &'a DeltaRecord,
) -> Result<&'a Value, StandingProgramRuntimeError> {
    if column_id == catalog_primary_key_column(catalog)?.column_id {
        return Ok(record.key.as_json());
    }
    if relation_id == plan.left_input_relation_id && column_id == plan.sum_value_column_id {
        return joined_left_value(record);
    }
    if relation_id == plan.right_input_relation_id {
        return joined_right_predicate_value(plan, column_id, record);
    }
    Err(StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "generic_join_view_plan.predicate.column",
    })
}

fn joined_left_value(record: &DeltaRecord) -> Result<&Value, StandingProgramRuntimeError> {
    record
        .value
        .as_json()
        .as_object()
        .and_then(|value| value.get(JOIN_LEFT_VALUE_FIELD))
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan.joined_value.left",
        })
}

fn join_aggregate_input_value<'a>(
    aggregate: &SupportedAggregateOutput,
    plan: &SupportedJoinViewPlan,
    catalogs: &'a [VelorixRelationCatalogV1],
    record: &'a DeltaRecord,
) -> Result<Value, StandingProgramRuntimeError> {
    let side = aggregate
        .input_relation_side
        .unwrap_or(SupportedAggregateInputRelationSide::Left);
    if let Some(expression) = &aggregate.input_expression {
        let Some(column_id) = aggregate.input_column_id.as_deref() else {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_join_view_plan.aggregate_outputs.input_column_id",
            });
        };
        let input_value = match side {
            SupportedAggregateInputRelationSide::Left => joined_left_value(record)?,
            SupportedAggregateInputRelationSide::Right => {
                joined_right_predicate_value(plan, column_id, record)?
            }
        };
        let mut input = Map::new();
        input.insert(column_id.to_string(), input_value.clone());
        let catalog = match side {
            SupportedAggregateInputRelationSide::Left => join_left_catalog(plan, catalogs)?,
            SupportedAggregateInputRelationSide::Right => join_right_catalog(plan, catalogs)?,
        };
        return Ok(Value::Number(JsonNumber::from(evaluate_projection_expr(
            expression, &input, catalog,
        )?)));
    }
    match side {
        SupportedAggregateInputRelationSide::Left => joined_left_value(record).cloned(),
        SupportedAggregateInputRelationSide::Right => {
            let Some(column_id) = aggregate.input_column_id.as_deref() else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_join_view_plan.aggregate_outputs.input_column_id",
                });
            };
            joined_right_predicate_value(plan, column_id, record).cloned()
        }
    }
}

fn joined_right_predicate_value<'a>(
    plan: &SupportedJoinViewPlan,
    column_id: &str,
    record: &'a DeltaRecord,
) -> Result<&'a Value, StandingProgramRuntimeError> {
    let right = record
        .value
        .as_json()
        .as_object()
        .and_then(|value| value.get(JOIN_RIGHT_VALUE_FIELD))
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan.joined_value.right",
        })?;
    let right_value_column_ids = supported_join_view_plan_right_value_column_ids(plan);
    if right_value_column_ids.len() == 1 && right_value_column_ids[0] == column_id {
        if let Some(value) = right.as_object().and_then(|value| value.get(column_id)) {
            return Ok(value);
        }
        return Ok(right);
    }
    right
        .as_object()
        .and_then(|value| value.get(column_id))
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_join_view_plan.predicate.column",
        })
}

fn filter_output_delta_for_having(
    delta: &DeltaBatch,
    having: Option<&AggregateOutputPredicate>,
    having_expr: Option<&AggregateOutputPredicateExpr>,
    output_schema: &RelationSchema,
    aggregate_outputs: Option<&[SupportedAggregateOutput]>,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if let Some(having_expr) = having_expr {
        let mut records = Vec::new();
        for record in delta.records() {
            if aggregate_output_matches_having_expr(
                having_expr,
                output_schema,
                aggregate_outputs,
                record,
            )? {
                records.push(record.clone());
            }
        }
        return Ok(DeltaBatch::from_records(records));
    }
    let Some(having) = having else {
        return Ok(delta.clone());
    };
    let mut records = Vec::new();
    for record in delta.records() {
        if aggregate_output_matches_having(having, output_schema, aggregate_outputs, record)? {
            records.push(record.clone());
        }
    }
    Ok(DeltaBatch::from_records(records))
}

fn aggregate_output_matches_having_expr(
    having_expr: &AggregateOutputPredicateExpr,
    output_schema: &RelationSchema,
    aggregate_outputs: Option<&[SupportedAggregateOutput]>,
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    match having_expr {
        AggregateOutputPredicateExpr::Atom { predicate } => {
            aggregate_output_matches_having(predicate, output_schema, aggregate_outputs, record)
        }
        AggregateOutputPredicateExpr::And { left, right } => Ok(
            aggregate_output_matches_having_expr(left, output_schema, aggregate_outputs, record)?
                && aggregate_output_matches_having_expr(
                    right,
                    output_schema,
                    aggregate_outputs,
                    record,
                )?,
        ),
        AggregateOutputPredicateExpr::Or { left, right } => Ok(
            aggregate_output_matches_having_expr(left, output_schema, aggregate_outputs, record)?
                || aggregate_output_matches_having_expr(
                    right,
                    output_schema,
                    aggregate_outputs,
                    record,
                )?,
        ),
    }
}

fn aggregate_output_matches_having(
    having: &AggregateOutputPredicate,
    output_schema: &RelationSchema,
    aggregate_outputs: Option<&[SupportedAggregateOutput]>,
    record: &DeltaRecord,
) -> Result<bool, StandingProgramRuntimeError> {
    let column = output_schema
        .columns
        .iter()
        .find(|column| column.name == having.output_column_id)
        .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.having.output_column_id",
        })?;
    let value = record.value.as_json();
    let value = value.as_object().ok_or_else(invalid_runtime_state)?;
    let actual = if let Some(aggregate) = aggregate_outputs.and_then(|outputs| {
        outputs
            .iter()
            .find(|output| output.output_column_id == having.output_column_id)
    }) {
        project_aggregate_value(value, aggregate)?
    } else {
        value
            .get(&having.output_column_id)
            .cloned()
            .ok_or_else(invalid_runtime_state)?
    };
    compare_output_scalar(&column.data_type, &actual, having.op, &having.literal)
}

fn compare_output_scalar(
    data_type: &SqlDataType,
    actual: &Value,
    mut op: PredicateOp,
    literal: &Value,
) -> Result<bool, StandingProgramRuntimeError> {
    if let Some(result) = compare_distinct_null_predicate(actual, literal, op) {
        return Ok(result);
    }
    op = non_null_predicate_op(op);
    if let Some(result) = compare_null_predicate(actual, op) {
        return Ok(result);
    }
    match data_type {
        SqlDataType::Int8
        | SqlDataType::Int16
        | SqlDataType::Int32
        | SqlDataType::Int64
        | SqlDataType::Date
        | SqlDataType::Timestamp { .. } => Ok(compare_ord(
            actual_i128(actual)?,
            op,
            literal_i128(literal)?,
        )?),
        SqlDataType::Float32 | SqlDataType::Float64 => {
            Ok(compare_ord(actual_f64(actual)?, op, literal_f64(literal)?)?)
        }
        SqlDataType::Decimal { precision, scale } => Ok(compare_ord(
            decimal_value_i128(actual, *precision, *scale)?,
            op,
            decimal_value_i128(literal, *precision, *scale)?,
        )?),
        SqlDataType::Utf8 | SqlDataType::Char { .. } => match (actual, literal) {
            (Value::String(actual), Value::String(expected)) => {
                compare_string_predicate(actual, op, expected)
            }
            _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.having.literal",
            }),
        },
        SqlDataType::Bool => match (actual, literal) {
            (Value::Bool(actual), Value::Bool(expected)) => Ok(match op {
                PredicateOp::Eq => actual == expected,
                PredicateOp::NotEq => actual != expected,
                _ => {
                    return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                        field: "generic_view_plan.having.op",
                    })
                }
            }),
            _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.having.literal",
            }),
        },
        _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_view_plan.having.output_column_id",
        }),
    }
}

impl TumblingWindowState {
    fn closed_delta(
        &self,
        plan: &SupportedTumblingWindowPlan,
        output_schema: &RelationSchema,
        watermark: Option<i64>,
    ) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        let Some(watermark) = watermark else {
            return Ok(DeltaBatch::default());
        };
        let mut records = Vec::new();
        for row in self.rows.values().filter(|row| {
            window_row_is_closed(row, plan, watermark) && tumbling_row_is_live(row, plan)
        }) {
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
            let record = DeltaRecord::new(
                DeltaKey::from_json(key),
                DeltaValue::from_json(Value::Object(value)),
                1,
            );
            let passes_having = match plan.having_expr.as_ref() {
                Some(having_expr) => aggregate_output_matches_having_expr(
                    having_expr,
                    output_schema,
                    Some(&plan.aggregate_outputs),
                    &record,
                )?,
                None => true,
            };
            if passes_having {
                records.push(record);
            }
        }
        Ok(DeltaBatch::from_records(records))
    }
}

fn window_row_is_closed(
    row: &TumblingWindowStateRow,
    plan: &SupportedTumblingWindowPlan,
    watermark: i64,
) -> bool {
    match plan.window_kind {
        SupportedEventTimeWindowKind::Tumbling | SupportedEventTimeWindowKind::Hopping => {
            row.window_end_ns <= watermark
        }
        SupportedEventTimeWindowKind::Session => row
            .window_end_ns
            .checked_add(plan.session_gap_ns.unwrap_or_default())
            .is_some_and(|close_time| close_time <= watermark),
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
        LogicalPlanAggregateFunctionV1::CountDistinct => {
            let count = row
                .extrema_values
                .get(&aggregate.output_column_id)
                .map(BTreeMap::len)
                .unwrap_or_default();
            Value::Number(JsonNumber::from(count as i64))
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
            let amount = batch_nullable_int64_value(batch, value_index, row_index)?;
            let weight = batch_int64_value(batch, weight_index, row_index)?;
            if !tumbling_predicate_matches_row(
                &plan.predicate_expr,
                catalog,
                plan,
                &group_key,
                amount,
                event_time_ns,
            )? {
                continue;
            }
            if !window_row_matches_any_aggregate_filter(
                catalog,
                plan,
                &group_key,
                amount,
                event_time_ns,
            )? {
                continue;
            }
            match plan.window_kind {
                SupportedEventTimeWindowKind::Tumbling | SupportedEventTimeWindowKind::Hopping => {
                    for (window_start_ns, window_end_ns) in
                        fixed_window_assignments(plan, event_time_ns)?
                    {
                        let row = window_state_row_mut(
                            state,
                            group_key.clone(),
                            window_start_ns,
                            window_end_ns,
                        );
                        apply_window_row_update(
                            row,
                            catalog,
                            plan,
                            &group_key,
                            amount,
                            event_time_ns,
                            weight,
                        )?;
                    }
                }
                SupportedEventTimeWindowKind::Session => {
                    apply_session_window_update(
                        state,
                        catalog,
                        plan,
                        group_key,
                        event_time_ns,
                        amount,
                        weight,
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn fixed_window_assignments(
    plan: &SupportedTumblingWindowPlan,
    event_time_ns: i64,
) -> Result<Vec<(i64, i64)>, StandingProgramRuntimeError> {
    match plan.window_kind {
        SupportedEventTimeWindowKind::Tumbling => {
            let window_start_ns =
                event_time_ns.div_euclid(plan.window_size_ns) * plan.window_size_ns;
            let window_end_ns = window_start_ns
                .checked_add(plan.window_size_ns)
                .ok_or_else(invalid_runtime_state)?;
            Ok(vec![(window_start_ns, window_end_ns)])
        }
        SupportedEventTimeWindowKind::Hopping => {
            let slide_ns = plan.hop_slide_ns.ok_or_else(invalid_runtime_state)?;
            let last_start = event_time_ns.div_euclid(slide_ns) * slide_ns;
            let window_count = plan.window_size_ns / slide_ns;
            let mut windows = Vec::with_capacity(window_count as usize);
            for index in 0..window_count {
                let window_start_ns = last_start
                    .checked_sub(
                        index
                            .checked_mul(slide_ns)
                            .ok_or_else(invalid_runtime_state)?,
                    )
                    .ok_or_else(invalid_runtime_state)?;
                let window_end_ns = window_start_ns
                    .checked_add(plan.window_size_ns)
                    .ok_or_else(invalid_runtime_state)?;
                if event_time_ns >= window_start_ns && event_time_ns < window_end_ns {
                    windows.push((window_start_ns, window_end_ns));
                }
            }
            windows.sort_unstable();
            Ok(windows)
        }
        SupportedEventTimeWindowKind::Session => Err(invalid_runtime_state()),
    }
}

fn window_state_row_mut(
    state: &mut TumblingWindowState,
    group_key: Value,
    window_start_ns: i64,
    window_end_ns: i64,
) -> &mut TumblingWindowStateRow {
    let state_key = window_state_key(&group_key, window_start_ns, window_end_ns);
    state
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
        })
}

fn window_state_key(group_key: &Value, window_start_ns: i64, window_end_ns: i64) -> String {
    canonical_json(&Value::Array(vec![
        group_key.clone(),
        Value::Number(JsonNumber::from(window_start_ns)),
        Value::Number(JsonNumber::from(window_end_ns)),
    ]))
}

fn apply_session_window_update(
    state: &mut TumblingWindowState,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedTumblingWindowPlan,
    group_key: Value,
    event_time_ns: i64,
    amount: Option<i64>,
    weight: i64,
) -> Result<(), StandingProgramRuntimeError> {
    update_session_event_index(state, group_key.clone(), event_time_ns, amount, weight)?;
    rebuild_session_windows_for_group(state, catalog, plan, &group_key)
}

fn update_session_event_index(
    state: &mut TumblingWindowState,
    group_key: Value,
    event_time_ns: i64,
    amount: Option<i64>,
    weight: i64,
) -> Result<(), StandingProgramRuntimeError> {
    let key = canonical_json(&Value::Array(vec![
        group_key.clone(),
        Value::Number(JsonNumber::from(event_time_ns)),
        amount
            .map(JsonNumber::from)
            .map(Value::Number)
            .unwrap_or(Value::Null),
    ]));
    let current_weight = state
        .session_events
        .get(&key)
        .map(|event| event.weight)
        .unwrap_or_default();
    let next_weight = current_weight
        .checked_add(weight)
        .ok_or_else(invalid_runtime_state)?;
    if next_weight < 0 {
        return Err(invalid_runtime_state());
    }
    if next_weight == 0 {
        state.session_events.remove(&key);
        return Ok(());
    }
    state.session_events.insert(
        key,
        SessionWindowEvent {
            group_key,
            event_time_ns,
            amount,
            weight: next_weight,
        },
    );
    Ok(())
}

fn rebuild_session_windows_for_group(
    state: &mut TumblingWindowState,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedTumblingWindowPlan,
    group_key: &Value,
) -> Result<(), StandingProgramRuntimeError> {
    let gap_ns = plan.session_gap_ns.ok_or_else(invalid_runtime_state)?;
    state.rows.retain(|_, row| row.group_key != *group_key);

    let mut events = state
        .session_events
        .values()
        .filter(|event| event.group_key == *group_key && event.weight > 0)
        .cloned()
        .collect::<Vec<_>>();
    events.sort_by(|left, right| {
        left.event_time_ns
            .cmp(&right.event_time_ns)
            .then_with(|| left.amount.cmp(&right.amount))
    });

    let mut current: Option<TumblingWindowStateRow> = None;
    for event in events {
        let starts_new_session = current.as_ref().is_some_and(|row| {
            event.event_time_ns > row.window_end_ns.checked_add(gap_ns).unwrap_or(i64::MAX)
        });
        if starts_new_session {
            insert_session_window_row(state, current.take().ok_or_else(invalid_runtime_state)?)?;
        }
        let row = current.get_or_insert_with(|| TumblingWindowStateRow {
            group_key: group_key.clone(),
            window_start_ns: event.event_time_ns,
            window_end_ns: event.event_time_ns,
            net_count: 0,
            avg_sums: BTreeMap::new(),
            avg_counts: BTreeMap::new(),
            extrema_values: BTreeMap::new(),
            values: BTreeMap::new(),
        });
        row.window_end_ns = row.window_end_ns.max(event.event_time_ns);
        apply_window_row_update(
            row,
            catalog,
            plan,
            group_key,
            event.amount,
            event.event_time_ns,
            event.weight,
        )?;
    }
    if let Some(row) = current {
        insert_session_window_row(state, row)?;
    }
    Ok(())
}

fn insert_session_window_row(
    state: &mut TumblingWindowState,
    row: TumblingWindowStateRow,
) -> Result<(), StandingProgramRuntimeError> {
    let state_key = window_state_key(&row.group_key, row.window_start_ns, row.window_end_ns);
    if state.rows.insert(state_key, row).is_some() {
        return Err(invalid_runtime_state());
    }
    Ok(())
}

fn apply_window_row_update(
    row: &mut TumblingWindowStateRow,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedTumblingWindowPlan,
    group_key: &Value,
    amount: Option<i64>,
    event_time_ns: i64,
    weight: i64,
) -> Result<(), StandingProgramRuntimeError> {
    let mut matched = false;
    for aggregate in &plan.aggregate_outputs {
        if !window_aggregate_filter_matches(
            catalog,
            plan,
            aggregate,
            group_key,
            amount,
            event_time_ns,
        )? {
            continue;
        }
        match aggregate.function {
            LogicalPlanAggregateFunctionV1::Sum => {
                let Some(amount) = window_aggregate_input_amount(
                    aggregate,
                    plan.sum_value_column_id.as_str(),
                    amount,
                    catalog,
                )?
                else {
                    continue;
                };
                matched = true;
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
                if aggregate.input_column_id.is_some() && amount.is_none() {
                    continue;
                }
                matched = true;
                let entry = row
                    .values
                    .entry(aggregate.output_column_id.clone())
                    .or_default();
                *entry = entry
                    .checked_add(weight)
                    .ok_or_else(invalid_runtime_state)?;
            }
            LogicalPlanAggregateFunctionV1::CountDistinct => {
                let Some(amount) = amount else {
                    continue;
                };
                matched = true;
                update_tumbling_extrema(row, aggregate, amount, weight)?;
            }
            LogicalPlanAggregateFunctionV1::Avg => {
                let Some(amount) = window_aggregate_input_amount(
                    aggregate,
                    plan.sum_value_column_id.as_str(),
                    amount,
                    catalog,
                )?
                else {
                    continue;
                };
                matched = true;
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
                let Some(amount) = window_aggregate_input_amount(
                    aggregate,
                    plan.sum_value_column_id.as_str(),
                    amount,
                    catalog,
                )?
                else {
                    continue;
                };
                matched = true;
                update_tumbling_extrema(row, aggregate, amount, weight)?;
            }
        }
    }
    if matched {
        row.net_count = row
            .net_count
            .checked_add(weight)
            .ok_or_else(invalid_runtime_state)?;
    }
    Ok(())
}

fn window_aggregate_input_amount(
    aggregate: &SupportedAggregateOutput,
    value_column_id: &str,
    amount: Option<i64>,
    catalog: &VelorixRelationCatalogV1,
) -> Result<Option<i64>, StandingProgramRuntimeError> {
    let Some(amount) = amount else {
        return Ok(None);
    };
    let Some(expression) = &aggregate.input_expression else {
        return Ok(Some(amount));
    };
    let mut input = Map::new();
    input.insert(
        value_column_id.to_string(),
        Value::Number(JsonNumber::from(amount)),
    );
    evaluate_projection_expr(expression, &input, catalog).map(Some)
}

fn window_row_matches_any_aggregate_filter(
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedTumblingWindowPlan,
    group_key: &Value,
    amount: Option<i64>,
    event_time_ns: i64,
) -> Result<bool, StandingProgramRuntimeError> {
    if plan.aggregate_filter_exprs.is_empty() {
        return Ok(true);
    }
    for aggregate in &plan.aggregate_outputs {
        if window_aggregate_filter_matches(
            catalog,
            plan,
            aggregate,
            group_key,
            amount,
            event_time_ns,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn window_aggregate_filter_matches(
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedTumblingWindowPlan,
    aggregate: &SupportedAggregateOutput,
    group_key: &Value,
    amount: Option<i64>,
    event_time_ns: i64,
) -> Result<bool, StandingProgramRuntimeError> {
    let Some(predicate_expr) = plan
        .aggregate_filter_exprs
        .get(aggregate.output_column_id.as_str())
    else {
        return Ok(true);
    };
    tumbling_predicate_expr_matches_row(
        predicate_expr,
        catalog,
        plan,
        group_key,
        amount,
        event_time_ns,
    )
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

fn arrow_record_batches_to_key_nullable_count_delta_batch(
    catalog: &VelorixRelationCatalogV1,
    relation_id: &str,
    relation_version: &str,
    schema_fingerprint: &str,
    key_column_id: &str,
    count_column_id: &str,
    batches: &[RecordBatch],
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if relation_id != catalog.relation_schema.relation_id
        || relation_version != catalog.relation_schema.relation_version
        || schema_fingerprint != catalog.schema_fingerprint.as_str()
    {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "generic_input_batch",
        });
    }
    let key_column = catalog_column_by_id(catalog, key_column_id)?;
    let count_column = catalog_column_by_id(catalog, count_column_id)?;
    let weight_column = catalog_column_by_id(catalog, &catalog.relation_schema.weight_column_id)?;
    let mut records = Vec::new();
    for batch in batches {
        let key_index = batch_column_index(batch, &key_column.name)?;
        let count_index = batch_column_index(batch, &count_column.name)?;
        let weight_index = batch_column_index(batch, &weight_column.name)?;
        for row_index in 0..batch.num_rows() {
            if batch.column(count_index).is_null(row_index) {
                continue;
            }
            let key =
                batch_key_value(batch, key_index, &key_column.physical_arrow_type, row_index)?;
            let weight = batch_int64_value(batch, weight_index, row_index)?;
            records.push(DeltaRecord::new(
                DeltaKey::from_json(key),
                DeltaValue::from_json(Value::Number(JsonNumber::from(0))),
                weight,
            ));
        }
    }
    Ok(DeltaBatch::from_records(records))
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

fn batch_nullable_int64_value(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
) -> Result<Option<i64>, StandingProgramRuntimeError> {
    let array = batch
        .column(column_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(invalid_runtime_state)?;
    if array.is_null(row_index) {
        Ok(None)
    } else {
        Ok(Some(array.value(row_index)))
    }
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
        let (_ordering, values) = match plan.function {
            LogicalPlanLatestByKeyFunctionV1::ArgMax => key_rows.values.iter().next_back()?,
            LogicalPlanLatestByKeyFunctionV1::ArgMin => key_rows.values.iter().next()?,
        };
        let (_value_canonical, value) = values.iter().rfind(|(_, value)| value.weight > 0)?;
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

impl AnalyticRowNumberState {
    fn published_output(
        &self,
        catalog: &VelorixRelationCatalogV1,
        plan: &SupportedAnalyticRowNumberPlan,
    ) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        let mut partitions = BTreeMap::<String, Value>::new();
        for row in self.rows.values() {
            if row.weight != 1 {
                return Err(invalid_checkpoint());
            }
            partitions
                .entry(canonical_json(&row.partition_value))
                .or_insert_with(|| row.partition_value.clone());
        }

        let mut output = DeltaBatch::default();
        for partition in partitions.values() {
            output = output.combine(&self.partition_output(catalog, plan, partition)?);
        }
        Ok(DeltaBatch::from_records(
            output.net_rows().map_err(|_| invalid_checkpoint())?,
        ))
    }

    fn apply_delta(
        &mut self,
        delta: &DeltaBatch,
        plan: &SupportedAnalyticRowNumberPlan,
        catalog: &VelorixRelationCatalogV1,
        affected_partitions: &mut BTreeMap<String, Value>,
    ) -> Result<(), StandingProgramRuntimeError> {
        for record in delta.records() {
            let key_canonical = canonical_json(record.key.as_json());
            if let Some(existing) = self.rows.get(&key_canonical) {
                affected_partitions
                    .entry(canonical_json(&existing.partition_value))
                    .or_insert_with(|| existing.partition_value.clone());
            }
            let input = analytic_row_number_record_input(record, catalog)?;
            let partition_value = input
                .get(plan.partition_column_id.as_str())
                .cloned()
                .ok_or_else(invalid_runtime_state)?;
            let order_value = input
                .get(plan.order_column_id.as_str())
                .cloned()
                .ok_or_else(invalid_runtime_state)?;
            affected_partitions
                .entry(canonical_json(&partition_value))
                .or_insert_with(|| partition_value.clone());
            self.apply_record(record, partition_value, order_value)?;
        }
        Ok(())
    }

    fn apply_record(
        &mut self,
        record: &DeltaRecord,
        partition_value: Value,
        order_value: Value,
    ) -> Result<(), StandingProgramRuntimeError> {
        let key_canonical = canonical_json(record.key.as_json());
        let next_weight = self
            .rows
            .get(&key_canonical)
            .map_or(0_i64, |row| row.weight)
            .checked_add(record.weight)
            .ok_or_else(invalid_runtime_state)?;
        if !(0..=1).contains(&next_weight) {
            return Err(invalid_runtime_state());
        }
        if next_weight == 0 {
            self.rows.remove(&key_canonical);
        } else {
            self.rows.insert(
                key_canonical,
                AnalyticRowNumberStateRow {
                    key: record.key.as_json().clone(),
                    partition_value,
                    order_value,
                    weight: next_weight,
                },
            );
        }
        Ok(())
    }

    fn partition_output(
        &self,
        catalog: &VelorixRelationCatalogV1,
        plan: &SupportedAnalyticRowNumberPlan,
        partition: &Value,
    ) -> Result<DeltaBatch, StandingProgramRuntimeError> {
        let key_column = catalog_primary_key_column(catalog)?;
        let order_column = catalog_column_by_id(catalog, &plan.order_column_id)?;
        let mut rows = self
            .rows
            .values()
            .filter(|row| row.partition_value == *partition)
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            let ordering =
                compare_row_number_values(order_column, &left.order_value, &right.order_value);
            let ordering = if plan.order_descending {
                ordering.reverse()
            } else {
                ordering
            };
            ordering.then_with(|| compare_row_number_values(key_column, &left.key, &right.key))
        });
        let mut previous_order_value: Option<&Value> = None;
        let mut previous_rank = 0_i64;
        let records = rows
            .into_iter()
            .enumerate()
            .filter_map(|(index, row)| {
                let rank = match plan.function {
                    SupportedAnalyticWindowFunction::RowNumber => (index + 1) as i64,
                    SupportedAnalyticWindowFunction::Rank => {
                        if previous_order_value == Some(&row.order_value) {
                            previous_rank
                        } else {
                            (index + 1) as i64
                        }
                    }
                    SupportedAnalyticWindowFunction::DenseRank => {
                        if previous_order_value == Some(&row.order_value) {
                            previous_rank
                        } else {
                            previous_rank + 1
                        }
                    }
                };
                previous_order_value = Some(&row.order_value);
                previous_rank = rank;
                if plan.rank_limit.is_some_and(|limit| rank > limit as i64) {
                    return None;
                }
                let mut value = Map::new();
                value.insert(
                    plan.output_row_number_column_id.clone(),
                    Value::Number(JsonNumber::from(rank)),
                );
                Some(DeltaRecord::new(
                    DeltaKey::from_json(row.key.clone()),
                    DeltaValue::from_json(Value::Object(value)),
                    1,
                ))
            })
            .collect::<Vec<_>>();
        Ok(DeltaBatch::from_records(records))
    }
}

fn validate_analytic_row_number_checkpoint_output(
    state: &AnalyticRowNumberState,
    published_output: &DeltaBatch,
    catalog: &VelorixRelationCatalogV1,
    plan: &SupportedAnalyticRowNumberPlan,
) -> Result<(), StandingProgramRuntimeError> {
    let state_output = state.published_output(catalog, plan)?;
    let state_rows = state_output.net_rows().map_err(|_| invalid_checkpoint())?;
    let published_rows = published_output
        .net_rows()
        .map_err(|_| invalid_checkpoint())?;
    if state_rows != published_rows {
        return Err(invalid_checkpoint());
    }
    Ok(())
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
    mut op: PredicateOp,
    literal: &Value,
) -> Result<bool, StandingProgramRuntimeError> {
    if let Some(result) = compare_distinct_null_predicate(actual, literal, op) {
        return Ok(result);
    }
    op = non_null_predicate_op(op);
    if let Some(result) = compare_null_predicate(actual, op) {
        return Ok(result);
    }
    if actual.is_null() {
        return Ok(false);
    }
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Date32
        | ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {
            return compare_ord(actual_i128(actual)?, op, literal_i128(literal)?);
        }
        ArrowPhysicalTypeV1::Decimal128 { precision, scale } => {
            let actual = decimal_value_i128(actual, *precision, *scale)?;
            let expected = decimal_value_i128(literal, *precision, *scale)?;
            return compare_ord(actual, op, expected);
        }
        ArrowPhysicalTypeV1::Float64 => {
            return compare_ord(actual_f64(actual)?, op, literal_f64(literal)?);
        }
        _ => {}
    }

    match (actual, literal) {
        (Value::Number(actual), Value::Number(expected)) => compare_ord(
            actual.as_i64().ok_or_else(invalid_runtime_state)?,
            op,
            expected
                .as_i64()
                .ok_or(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.predicate.literal",
                })?,
        ),
        (Value::String(actual), Value::String(expected)) => {
            compare_string_predicate(actual, op, expected)
        }
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

fn compare_null_predicate(actual: &Value, op: PredicateOp) -> Option<bool> {
    match op {
        PredicateOp::IsNull => Some(actual.is_null()),
        PredicateOp::IsNotNull => Some(!actual.is_null()),
        _ => None,
    }
}

fn compare_distinct_null_predicate(
    actual: &Value,
    literal: &Value,
    op: PredicateOp,
) -> Option<bool> {
    let null_involved = actual.is_null() || literal.is_null();
    match op {
        PredicateOp::IsDistinctFrom if null_involved => {
            Some(!(actual.is_null() && literal.is_null()))
        }
        PredicateOp::IsNotDistinctFrom if null_involved => {
            Some(actual.is_null() && literal.is_null())
        }
        _ => None,
    }
}

fn non_null_predicate_op(op: PredicateOp) -> PredicateOp {
    match op {
        PredicateOp::IsDistinctFrom => PredicateOp::NotEq,
        PredicateOp::IsNotDistinctFrom => PredicateOp::Eq,
        _ => op,
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

fn compare_ord<T: PartialOrd + PartialEq>(
    actual: T,
    op: PredicateOp,
    expected: T,
) -> Result<bool, StandingProgramRuntimeError> {
    Ok(match op {
        PredicateOp::Eq => actual == expected,
        PredicateOp::NotEq => actual != expected,
        PredicateOp::Gt => actual > expected,
        PredicateOp::GtEq => actual >= expected,
        PredicateOp::Lt => actual < expected,
        PredicateOp::LtEq => actual <= expected,
        PredicateOp::Like
        | PredicateOp::NotLike
        | PredicateOp::IsNull
        | PredicateOp::IsNotNull
        | PredicateOp::IsDistinctFrom
        | PredicateOp::IsNotDistinctFrom => {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.predicate.op",
            })
        }
    })
}

fn compare_string_predicate(
    actual: &str,
    op: PredicateOp,
    expected: &str,
) -> Result<bool, StandingProgramRuntimeError> {
    Ok(match op {
        PredicateOp::Eq => actual == expected,
        PredicateOp::NotEq => actual != expected,
        PredicateOp::Gt => actual > expected,
        PredicateOp::GtEq => actual >= expected,
        PredicateOp::Lt => actual < expected,
        PredicateOp::LtEq => actual <= expected,
        PredicateOp::Like => sql_like_matches(actual, expected),
        PredicateOp::NotLike => !sql_like_matches(actual, expected),
        PredicateOp::IsNull
        | PredicateOp::IsNotNull
        | PredicateOp::IsDistinctFrom
        | PredicateOp::IsNotDistinctFrom => {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "generic_view_plan.predicate.op",
            })
        }
    })
}

fn sql_like_matches(actual: &str, pattern: &str) -> bool {
    fn matches_at(actual: &[char], pattern: &[char]) -> bool {
        match pattern.split_first() {
            None => actual.is_empty(),
            Some(('%', rest)) => {
                matches_at(actual, rest)
                    || (!actual.is_empty() && matches_at(&actual[1..], pattern))
            }
            Some(('_', rest)) => !actual.is_empty() && matches_at(&actual[1..], rest),
            Some((expected, rest)) => actual.split_first().is_some_and(|(current, remaining)| {
                current == expected && matches_at(remaining, rest)
            }),
        }
    }
    matches_at(
        &actual.chars().collect::<Vec<_>>(),
        &pattern.chars().collect::<Vec<_>>(),
    )
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
                LogicalPlanAggregateFunctionV1::Min
                    | LogicalPlanAggregateFunctionV1::Max
                    | LogicalPlanAggregateFunctionV1::CountDistinct
            )
        })
}

fn join_plan_tracks_extrema(plan: &SupportedJoinViewPlan) -> bool {
    supported_join_view_plan_aggregate_outputs(plan)
        .iter()
        .any(|output| {
            matches!(
                output.function,
                LogicalPlanAggregateFunctionV1::Min
                    | LogicalPlanAggregateFunctionV1::Max
                    | LogicalPlanAggregateFunctionV1::CountDistinct
            )
        })
}

fn normalize_legacy_join_plan_input_relation_sides(plan: &mut SupportedJoinViewPlan) {
    for output in &mut plan.aggregate_outputs {
        if output.input_relation_side.is_none()
            && output
                .input_column_id
                .as_deref()
                .is_some_and(|column_id| column_id == plan.sum_value_column_id)
        {
            output.input_relation_side = Some(SupportedAggregateInputRelationSide::Left);
        }
    }
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
            if output.input_expression.is_some() {
                return Ok(SqlDataType::Int64);
            }
            let Some(column_id) = &output.input_column_id else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.aggregate_outputs.input_column_id",
                });
            };
            aggregate_sum_sql_type_for_column_id(catalog, column_id)
        }
        LogicalPlanAggregateFunctionV1::Count | LogicalPlanAggregateFunctionV1::CountDistinct => {
            Ok(SqlDataType::Int64)
        }
        LogicalPlanAggregateFunctionV1::Avg => {
            let Some(column_id) = &output.input_column_id else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.aggregate_outputs.input_column_id",
                });
            };
            match &catalog_column(catalog, column_id)?.physical_arrow_type {
                ArrowPhysicalTypeV1::Int64 | ArrowPhysicalTypeV1::Decimal128 { .. } => {
                    Ok(SqlDataType::Float64)
                }
                _ => Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.aggregate_outputs.avg",
                }),
            }
        }
        LogicalPlanAggregateFunctionV1::Min | LogicalPlanAggregateFunctionV1::Max => {
            if output.input_expression.is_some() {
                return Ok(SqlDataType::Int64);
            }
            let Some(column_id) = &output.input_column_id else {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "generic_view_plan.aggregate_outputs.input_column_id",
                });
            };
            aggregate_sum_sql_type_for_column_id(catalog, column_id)
        }
    }
}

fn join_aggregate_output_sql_type(
    left_catalog: &VelorixRelationCatalogV1,
    right_catalog: &VelorixRelationCatalogV1,
    output: &SupportedAggregateOutput,
) -> Result<SqlDataType, StandingProgramRuntimeError> {
    let catalog = if output.input_relation_side == Some(SupportedAggregateInputRelationSide::Right)
    {
        right_catalog
    } else {
        left_catalog
    };
    aggregate_output_sql_type(catalog, output)
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

fn apply_filter_project_full_output_delta(
    current: &DeltaBatch,
    output_delta: &DeltaBatch,
    plan: &SupportedFilterProjectPlan,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if plan.output_key_input_column_id.is_none() {
        return apply_published_output_delta(current, output_delta);
    }
    let rows = current
        .combine(output_delta)
        .net_rows()
        .map_err(|_| invalid_runtime_state())?;
    if rows.iter().any(|row| row.weight < 0) {
        return Err(invalid_runtime_state());
    }
    Ok(DeltaBatch::from_records(rows))
}

fn filter_project_published_output_from_full_output(
    full_output: DeltaBatch,
    plan: &SupportedFilterProjectPlan,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if plan.output_key_input_column_id.is_none() {
        validate_published_output(&full_output)?;
        return Ok(full_output);
    }
    let rows = full_output
        .net_rows()
        .map_err(|_| invalid_runtime_state())?
        .into_iter()
        .map(|mut row| {
            if row.weight < 0 {
                return Err(invalid_runtime_state());
            }
            row.weight = 1;
            Ok(row)
        })
        .collect::<Result<Vec<_>, StandingProgramRuntimeError>>()?;
    let published = DeltaBatch::from_records(rows);
    validate_published_output(&published)?;
    Ok(published)
}

fn apply_top_k_to_published_output(
    output: DeltaBatch,
    top_k: Option<&SupportedTopKPlan>,
    aggregate_outputs: &[SupportedAggregateOutput],
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let Some(top_k) = top_k else {
        return Ok(output);
    };
    let aggregate = aggregate_outputs
        .iter()
        .find(|aggregate| aggregate.output_column_id == top_k.order_output_column_id)
        .ok_or_else(invalid_runtime_state)?;
    let mut rows = output.net_rows().map_err(|_| invalid_runtime_state())?;
    rows.sort_by(|left, right| {
        let ordering = top_k_record_value(left, aggregate)
            .and_then(|left_value| {
                top_k_record_value(right, aggregate).map(|right_value| {
                    left_value
                        .partial_cmp(&right_value)
                        .unwrap_or(Ordering::Equal)
                })
            })
            .unwrap_or(Ordering::Equal);
        let ordering = if top_k.descending {
            ordering.reverse()
        } else {
            ordering
        };
        ordering.then_with(|| top_k_tie_breaker_ordering(left, right, top_k))
    });
    let rows: Vec<_> = rows
        .into_iter()
        .skip(top_k.offset)
        .take(top_k.limit)
        .collect();
    let published = DeltaBatch::from_records(rows);
    validate_published_output(&published)?;
    Ok(published)
}

fn apply_latest_top_k_to_published_output(
    output: DeltaBatch,
    top_k: Option<&SupportedTopKPlan>,
    plan: &SupportedLatestByKeyPlan,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let Some(top_k) = top_k else {
        return Ok(output);
    };
    let mut rows = output.net_rows().map_err(|_| invalid_runtime_state())?;
    rows.sort_by(|left, right| {
        let ordering = latest_top_k_record_value(left, top_k, plan)
            .and_then(|left_value| {
                latest_top_k_record_value(right, top_k, plan)
                    .map(|right_value| compare_json_values(&left_value, &right_value))
            })
            .unwrap_or(Ordering::Equal);
        let ordering = if top_k.descending {
            ordering.reverse()
        } else {
            ordering
        };
        ordering.then_with(|| top_k_tie_breaker_ordering(left, right, top_k))
    });
    let rows: Vec<_> = rows
        .into_iter()
        .skip(top_k.offset)
        .take(top_k.limit)
        .collect();
    let published = DeltaBatch::from_records(rows);
    validate_published_output(&published)?;
    Ok(published)
}

fn apply_filter_project_top_k_to_published_output(
    output: DeltaBatch,
    top_k: Option<&SupportedTopKPlan>,
    plan: &SupportedFilterProjectPlan,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    let Some(top_k) = top_k else {
        return Ok(output);
    };
    let mut rows = output.net_rows().map_err(|_| invalid_runtime_state())?;
    rows.sort_by(|left, right| {
        let ordering = filter_project_top_k_record_value(left, top_k, plan)
            .and_then(|left_value| {
                filter_project_top_k_record_value(right, top_k, plan)
                    .map(|right_value| compare_json_values(&left_value, &right_value))
            })
            .unwrap_or(Ordering::Equal);
        let ordering = if top_k.descending {
            ordering.reverse()
        } else {
            ordering
        };
        ordering.then_with(|| top_k_tie_breaker_ordering(left, right, top_k))
    });
    let rows: Vec<_> = rows
        .into_iter()
        .skip(top_k.offset)
        .take(top_k.limit)
        .collect();
    let published = DeltaBatch::from_records(rows);
    validate_published_output(&published)?;
    Ok(published)
}

fn top_k_tie_breaker_ordering(
    left: &DeltaRecord,
    right: &DeltaRecord,
    _top_k: &SupportedTopKPlan,
) -> Ordering {
    canonical_json(left.key.as_json()).cmp(&canonical_json(right.key.as_json()))
}

fn filter_project_top_k_record_value(
    record: &DeltaRecord,
    top_k: &SupportedTopKPlan,
    plan: &SupportedFilterProjectPlan,
) -> Result<Value, StandingProgramRuntimeError> {
    if top_k.order_input_column_id.is_some() {
        return record
            .value
            .as_json()
            .as_object()
            .and_then(|value| value.get(filter_project_hidden_order_value_key()))
            .cloned()
            .ok_or_else(invalid_runtime_state);
    }
    if top_k.order_output_column_id == plan.output_key_column_id {
        return Ok(record.key.as_json().clone());
    }
    if plan
        .value_columns
        .iter()
        .any(|column| column.output_column_id == top_k.order_output_column_id)
    {
        return record
            .value
            .as_json()
            .as_object()
            .and_then(|value| value.get(&top_k.order_output_column_id))
            .cloned()
            .ok_or_else(invalid_runtime_state);
    }
    Err(invalid_runtime_state())
}

fn strip_filter_project_hidden_order_value(
    output: DeltaBatch,
    plan: &SupportedFilterProjectPlan,
) -> Result<DeltaBatch, StandingProgramRuntimeError> {
    if !plan
        .top_k
        .as_ref()
        .is_some_and(|top_k| top_k.order_input_column_id.is_some())
    {
        return Ok(output);
    }
    let records = output
        .records()
        .iter()
        .map(|record| {
            let mut value = record
                .value
                .as_json()
                .as_object()
                .cloned()
                .ok_or_else(invalid_runtime_state)?;
            value.remove(filter_project_hidden_order_value_key());
            Ok(DeltaRecord::new(
                record.key.clone(),
                DeltaValue::from_json(Value::Object(value)),
                record.weight,
            ))
        })
        .collect::<Result<Vec<_>, StandingProgramRuntimeError>>()?;
    Ok(DeltaBatch::from_records(records))
}

fn filter_project_hidden_order_value_key() -> &'static str {
    "__velorix_hidden_order_value"
}

fn latest_top_k_record_value(
    record: &DeltaRecord,
    top_k: &SupportedTopKPlan,
    plan: &SupportedLatestByKeyPlan,
) -> Result<Value, StandingProgramRuntimeError> {
    if top_k.order_output_column_id == plan.output_key_column_id {
        return Ok(record.key.as_json().clone());
    }
    if top_k.order_output_column_id == plan.output_value_column_id {
        return record
            .value
            .as_json()
            .as_object()
            .and_then(|value| value.get(&plan.output_value_column_id))
            .cloned()
            .ok_or_else(invalid_runtime_state);
    }
    Err(invalid_runtime_state())
}

fn compare_json_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left
            .as_f64()
            .and_then(|left| right.as_f64().and_then(|right| left.partial_cmp(&right)))
            .unwrap_or(Ordering::Equal),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Null, Value::Null) => Ordering::Equal,
        _ => canonical_json(left).cmp(&canonical_json(right)),
    }
}

fn compare_row_number_values(column: &RelationColumnV1, left: &Value, right: &Value) -> Ordering {
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int8
        | ArrowPhysicalTypeV1::Int16
        | ArrowPhysicalTypeV1::Int32
        | ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Date32
        | ArrowPhysicalTypeV1::Time64Nanosecond
        | ArrowPhysicalTypeV1::TimestampNanosecond { .. } => left
            .as_i64()
            .and_then(|left| right.as_i64().map(|right| left.cmp(&right)))
            .unwrap_or(Ordering::Equal),
        ArrowPhysicalTypeV1::UInt8
        | ArrowPhysicalTypeV1::UInt16
        | ArrowPhysicalTypeV1::UInt32
        | ArrowPhysicalTypeV1::UInt64 => left
            .as_u64()
            .and_then(|left| right.as_u64().map(|right| left.cmp(&right)))
            .unwrap_or(Ordering::Equal),
        ArrowPhysicalTypeV1::Decimal128 { .. } => left
            .as_i64()
            .and_then(|left| right.as_i64().map(|right| left.cmp(&right)))
            .unwrap_or(Ordering::Equal),
        ArrowPhysicalTypeV1::Float32 | ArrowPhysicalTypeV1::Float64 => left
            .as_f64()
            .and_then(|left| right.as_f64().and_then(|right| left.partial_cmp(&right)))
            .unwrap_or(Ordering::Equal),
        ArrowPhysicalTypeV1::Utf8
        | ArrowPhysicalTypeV1::DictionaryUtf8 { .. }
        | ArrowPhysicalTypeV1::JsonUtf8 => left.as_str().cmp(&right.as_str()),
        ArrowPhysicalTypeV1::Boolean => left.as_bool().cmp(&right.as_bool()),
        ArrowPhysicalTypeV1::Binary
        | ArrowPhysicalTypeV1::List { .. }
        | ArrowPhysicalTypeV1::Struct { .. }
        | ArrowPhysicalTypeV1::Map { .. } => canonical_json(left).cmp(&canonical_json(right)),
    }
}

fn top_k_record_value(
    record: &DeltaRecord,
    aggregate: &SupportedAggregateOutput,
) -> Result<f64, StandingProgramRuntimeError> {
    let value = record
        .value
        .as_json()
        .as_object()
        .ok_or_else(invalid_runtime_state)?;
    let projected = project_aggregate_value(value, aggregate)?;
    aggregate_sum_as_f64(&projected)
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
            input_relation_side: None,
            input_expression: None,
            output_column_id: "sum".to_string(),
        },
        SupportedAggregateOutput {
            function: LogicalPlanAggregateFunctionV1::Count,
            input_column_id: None,
            input_relation_side: None,
            input_expression: None,
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
            if value.contains_key(left_join_group_row_count_key())
                && value
                    .get(&sum_qualifying_count_key(&aggregate.output_column_id))
                    .and_then(Value::as_i64)
                    .unwrap_or_default()
                    == 0
            {
                return Ok(Value::Null);
            }
            value
                .get(aggregate.output_column_id.as_str())
                .or_else(|| value.get("sum"))
                .cloned()
                .ok_or_else(invalid_runtime_state)
        }
        LogicalPlanAggregateFunctionV1::Count => value
            .get(aggregate.output_column_id.as_str())
            .or_else(|| value.get("count"))
            .cloned()
            .ok_or_else(invalid_runtime_state),
        LogicalPlanAggregateFunctionV1::CountDistinct => {
            if let Some(values) = value
                .get(aggregate.output_column_id.as_str())
                .and_then(Value::as_array)
            {
                return Ok(Value::Number(JsonNumber::from(values.len() as i64)));
            }
            if let Some(values) = value.get("values").and_then(Value::as_array) {
                return Ok(Value::Number(JsonNumber::from(values.len() as i64)));
            }
            value
                .get(aggregate.output_column_id.as_str())
                .cloned()
                .ok_or_else(invalid_runtime_state)
        }
        LogicalPlanAggregateFunctionV1::Avg => {
            if let Some(projected) = value.get(aggregate.output_column_id.as_str()) {
                if let Some(avg) = project_filtered_avg_value(projected)? {
                    return Ok(avg);
                }
                return Ok(projected.clone());
            }
            let sum = aggregate_sum_as_f64(value.get("sum").ok_or_else(invalid_runtime_state)?)?;
            let count = value
                .get("count")
                .and_then(Value::as_i64)
                .ok_or_else(invalid_runtime_state)?;
            if count == 0 {
                return Ok(Value::Null);
            }
            let avg = sum / count as f64;
            JsonNumber::from_f64(avg)
                .map(Value::Number)
                .ok_or_else(invalid_runtime_state)
        }
        LogicalPlanAggregateFunctionV1::Min => {
            if let Some(values) = value
                .get(aggregate.output_column_id.as_str())
                .and_then(Value::as_array)
            {
                return multiset_i64_extreme(values, false);
            }
            value
                .get(aggregate.output_column_id.as_str())
                .or_else(|| value.get("min"))
                .cloned()
                .ok_or_else(invalid_runtime_state)
        }
        LogicalPlanAggregateFunctionV1::Max => {
            if let Some(values) = value
                .get(aggregate.output_column_id.as_str())
                .and_then(Value::as_array)
            {
                return multiset_i64_extreme(values, true);
            }
            value
                .get(aggregate.output_column_id.as_str())
                .or_else(|| value.get("max"))
                .cloned()
                .ok_or_else(invalid_runtime_state)
        }
    }
}

fn project_filtered_avg_value(value: &Value) -> Result<Option<Value>, StandingProgramRuntimeError> {
    let Some(value) = value.as_object() else {
        return Ok(None);
    };
    let Some(sum) = value.get("sum").and_then(Value::as_f64) else {
        return Ok(None);
    };
    let count = value
        .get("count")
        .and_then(Value::as_i64)
        .ok_or_else(invalid_runtime_state)?;
    if count == 0 {
        return Ok(Some(Value::Null));
    }
    JsonNumber::from_f64(sum / count as f64)
        .map(Value::Number)
        .map(Some)
        .ok_or_else(invalid_runtime_state)
}

fn multiset_i64_extreme(values: &[Value], max: bool) -> Result<Value, StandingProgramRuntimeError> {
    let mut selected: Option<i64> = None;
    for entry in values {
        let entry = entry.as_object().ok_or_else(invalid_runtime_state)?;
        let weight = entry
            .get("weight")
            .and_then(Value::as_i64)
            .ok_or_else(invalid_runtime_state)?;
        if weight <= 0 {
            continue;
        }
        let candidate = entry
            .get("value")
            .and_then(Value::as_i64)
            .ok_or_else(invalid_runtime_state)?;
        selected = Some(match selected {
            Some(current) if max => current.max(candidate),
            Some(current) => current.min(candidate),
            None => candidate,
        });
    }
    Ok(selected
        .map(JsonNumber::from)
        .map(Value::Number)
        .unwrap_or(Value::Null))
}

fn aggregate_sum_as_f64(value: &Value) -> Result<f64, StandingProgramRuntimeError> {
    if let Some(value) = value.as_i64() {
        return Ok(value as f64);
    }
    value
        .as_str()
        .and_then(|value| value.parse::<f64>().ok())
        .ok_or_else(invalid_runtime_state)
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
    matches!(
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
            .as_deref(),
        Some(JOIN_RUNTIME_KIND | JOIN_COMMON_DAG_REFERENCE_RUNTIME_KIND)
    )
}

fn checkpoint_has_three_input_join_payload(checkpoint: &RuntimeCheckpoint) -> bool {
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
        == Some(three_input_join::THREE_INPUT_JOIN_RUNTIME_KIND)
}

fn checkpoint_has_semi_anti_join_payload(checkpoint: &RuntimeCheckpoint) -> bool {
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
        == Some(semi_anti_join::SEMI_ANTI_JOIN_RUNTIME_KIND)
}

fn checkpoint_has_common_dag_reference_join_payload(checkpoint: &RuntimeCheckpoint) -> bool {
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
        == Some(JOIN_COMMON_DAG_REFERENCE_RUNTIME_KIND)
}

fn checkpoint_has_filter_project_payload(checkpoint: &RuntimeCheckpoint) -> bool {
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
        == Some(FILTER_PROJECT_RUNTIME_KIND)
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

fn checkpoint_has_analytic_row_number_payload(checkpoint: &RuntimeCheckpoint) -> bool {
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
        == Some(ANALYTIC_ROW_NUMBER_RUNTIME_KIND)
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
