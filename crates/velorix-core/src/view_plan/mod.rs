//! Materialized view SQL admission and logical plan contracts.
//!
//! SQL is only the user-facing input. The runtime contract is the versioned
//! logical plan produced by this module.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Number as JsonNumber, Value as JsonValue};
use sqlparser::{
    ast::{
        BinaryOperator, CastKind, CeilFloorKind, DataType, DateTimeField, Distinct,
        DuplicateTreatment, Expr, Fetch, Function, FunctionArg, FunctionArgExpr,
        FunctionArgumentList, FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator,
        LimitClause, ObjectName, OrderByExpr, OrderByKind, Query, Select, SelectItem,
        SelectItemQualifiedWildcardKind, SetExpr, SetOperator, SetQuantifier,
        Statement as SqlStatement, TableAlias, TableFactor, TableSampleKind, TrimWhereField,
        UnaryOperator, Value as SqlValue, ValueWithSpan, WindowType,
    },
    dialect::GenericDialect,
    parser::{Parser, ParserError},
};
use thiserror::Error;

use crate::{
    operator_contract::{
        AcceptedChangelogV1, CandidateKeyV1, ChangelogModeV1, CheckpointCodecIdentityV1,
        DeterminismGuaranteeV1, DeterminismRequirementV1, InputPortContractV1, InputPortRefV1,
        KeyEqualityV1, NullabilityV1, OperatorContractV1, OperatorDagContractV1, OperatorEdgeV1,
        OperatorKindIdentityV1, OutputPortContractV1, OutputPortRefV1, PortColumnV1,
        ProcessingFrontierGuaranteeV1, ProcessingFrontierRequirementV1, ProgressGuaranteeV1,
        ProgressRequirementV1, RowSchemaV1, StateBoundednessV1, StateContractV1,
        StateRetentionContractV1, UniquenessGuaranteeV1, WatermarkGuaranteeV1,
        WatermarkRequirementV1, OPERATOR_DAG_CONTRACT_VERSION_V1,
    },
    relation::{
        ArrowPhysicalTypeV1, RelationColumnV1, RelationSchemaError,
        SupportedIncrementalAdapterSpec, VelorixRelationCatalogV1,
    },
    view_contract::{
        catalog_input_relation_schema, stable_bytes_hash, ColumnSchema, RelationSchema, SqlDataType,
    },
};

mod typed_expr;

pub use typed_expr::{
    builtin_udf_identity_for_name, builtin_udf_spec, validate_typed_expr_node,
    BuiltinScalarFunctionV1, BuiltinUdfIdentityV1, CanonicalI128V1, RuntimeScalarTypeV1,
    ScalarLiteralV1, TypedExprError, TypedExprKindV1, TypedExprNodeV1, TypedExprProgramV1,
    TYPED_EXPR_PROGRAM_SCHEMA_VERSION_V1,
};

pub const LOGICAL_VIEW_PLAN_VERSION_V1: u32 = 1;
pub const LOGICAL_VIEW_PLAN_VERSION_V2: u32 = 2;
pub const LOGICAL_VIEW_PLAN_HASH_PREFIX: &str = "velorix-logical-view-plan-sha256-v1";
pub const LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V1: &str = "velorix-logical-view-capabilities-v1";
pub const LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2: &str = "velorix-logical-view-capabilities-v2";
pub const LOGICAL_VIEW_STATE_CODEC_VERSION_V1: &str = "velorix-logical-view-state-v1";
/// Durable physical encoding for composite primary-key join keys.
///
/// The equality pairs are sorted canonically, every pair must use the same
/// exact Arrow physical type on both sides, and each row key is encoded as a
/// positional JSON array with one canonical JSON atom per pair. Primary-key
/// admission makes every component non-null.
pub const COMPOSITE_PK_POSITIONAL_JSON_ARRAY_JOIN_KEY_CODEC_V1: &str =
    "velorix-composite-pk-positional-json-array-join-key-v1";
pub const NON_PRIMARY_NON_NULL_SCALAR_JOIN_KEY_CODEC_V1: &str =
    "velorix-non-primary-non-null-scalar-join-key-v1";
pub const SCALAR_PK_JSON_JOIN_KEY_CODEC_V1: &str = "velorix-scalar-pk-json-join-key-v1";
pub const SELF_JOIN_ATOMIC_FANOUT_PROTOCOL_V1: &str =
    "velorix-self-join-left-then-right-atomic-fanout-v1";
pub const THREE_INPUT_LEGACY_SQL_ENCOUNTER_JOIN_ORDER_V1: &str =
    "velorix-three-input-legacy-sql-encounter-order-v1";
pub const THREE_INPUT_ROOT_FIXED_RIGHT_RELATION_ID_JOIN_ORDER_V1: &str =
    "velorix-three-input-root-fixed-right-relation-id-order-v1";
pub const LEFT_JOIN_INPUT_INSTANCE_ID_V1: &str = "scan_left";
pub const RIGHT_JOIN_INPUT_INSTANCE_ID_V1: &str = "scan_right";
pub const LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1: &str = "velorix-materialized-output-v1";
pub const INCREMENTAL_KEY_SEMANTICS_VERSION_V1: &str = "velorix-incremental-key-semantics-v1";
pub const INCREMENTAL_BAG_SEMANTICS_VERSION_V1: &str = "velorix-incremental-bag-semantics-v1";
pub const EXECUTION_IMPLEMENTATION_CONTRACT_VERSION_V2: u32 = 2;
pub const CHECKPOINT_MANIFEST_VERSION_V1: u32 = 1;
pub const OUTPUT_PUBLICATION_PROTOCOL_VERSION_V1: &str = "velorix-durable-output-publication-v1";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VelorixLogicalViewPlanV1 {
    pub plan_version: u32,
    pub plan_hash: Option<String>,
    pub view_sql: String,
    pub capability_version: String,
    pub key_semantics_version: String,
    pub bag_semantics_version: String,
    pub input_relations: Vec<LogicalPlanRelationRef>,
    pub output_relation: LogicalPlanRelationRef,
    pub nodes: Vec<VelorixLogicalViewPlanNodeV1>,
    pub operator_dag_contract: OperatorDagContractV1,
    pub state_requirements: Vec<LogicalPlanStateRequirementV1>,
    pub output_codec_version: String,
    pub execution_implementation: Option<LogicalPlanExecutionImplementationV1>,
    pub execution: VelorixLogicalViewExecutionV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanExecutionImplementationV1 {
    pub contract_version: u32,
    pub implementation_id: String,
    pub implementation_version: u32,
    pub state_codec_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_key_codec_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_fanout_protocol_id: Option<String>,
    pub checkpoint_manifest_version: u32,
    pub output_codec_id: String,
    pub output_publication_protocol_id: String,
    pub physical_operator_dag_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanRelationRef {
    pub relation_id: String,
    pub relation_name: String,
    pub relation_version: String,
    pub schema_fingerprint: String,
}

/// Schema-only relation input used by the SQL planner.
///
/// The planner only needs the relation schema (columns, key, types) to resolve
/// SQL names and expressions. Physical source catalogs carry extra ingestion
/// contract fields (weight column, incremental adapter) that do not apply to
/// published view outputs. `PlannerRelationInput` decouples those concerns so a
/// consumer view can plan against a producer's `PublishedRelationBindingV1`
/// without fabricating a physical `VelorixRelationCatalogV1`.
#[derive(Clone, Debug)]
pub struct PlannerRelationInput {
    /// The relation schema the planner resolves SQL against.
    pub relation: RelationSchema,
    /// The catalog schema fingerprint this input binds to.
    pub schema_fingerprint: String,
    /// The weight column id, if this input is a physical source with one.
    pub weight_column_id: Option<String>,
    /// The declared event-time column id, if this input has one.
    pub event_time_column_id: Option<String>,
    /// How this input's changes are encoded at runtime.
    pub change_encoding: PlannerChangeEncoding,
}

/// How an input's signed changes are delivered to the runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlannerChangeEncoding {
    /// A registered physical ingest source.
    PhysicalSource,
    /// A published view output delivered as a signed delta batch.
    PublishedDelta {
        delta_codec_identity: String,
        frontier_kind: String,
    },
}

impl PlannerRelationInput {
    /// Builds a planner input from a physical source catalog.
    pub fn from_source_catalog(catalog: &VelorixRelationCatalogV1) -> Result<Self, ViewPlanError> {
        let relation = catalog_input_relation_schema(catalog).map_err(|error| {
            ViewPlanError::UnsupportedShape {
                reason: format!("relation catalog does not yield an input schema: {error}"),
            }
        })?;
        Ok(Self {
            schema_fingerprint: catalog.schema_fingerprint.to_string(),
            weight_column_id: Some(catalog.relation_schema.weight_column_id.clone()),
            event_time_column_id: catalog.relation_schema.event_time_column_id.clone(),
            relation,
            change_encoding: PlannerChangeEncoding::PhysicalSource,
        })
    }

    /// Builds a planner input from a published view output binding.
    pub fn from_published_binding(
        relation: RelationSchema,
        delta_codec_identity: String,
        frontier_kind: String,
    ) -> Self {
        Self {
            schema_fingerprint: relation.schema_fingerprint.clone(),
            weight_column_id: None,
            event_time_column_id: None,
            relation,
            change_encoding: PlannerChangeEncoding::PublishedDelta {
                delta_codec_identity,
                frontier_kind,
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VelorixLogicalViewPlanNodeV1 {
    RelationScan {
        node_id: String,
        relation: LogicalPlanRelationRef,
    },
    Filter {
        node_id: String,
        input: String,
        predicate: LogicalPlanPredicateV1,
    },
    Project {
        node_id: String,
        input: String,
        columns: Vec<LogicalPlanColumnRef>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        computed_columns: Vec<LogicalPlanComputedColumnV1>,
    },
    Aggregate {
        node_id: String,
        input: String,
        group_keys: Vec<LogicalPlanColumnRef>,
        accumulators: Vec<LogicalPlanAggregateAccumulatorV1>,
    },
    TopK {
        node_id: String,
        input: String,
        order_by: LogicalPlanColumnRef,
        descending: bool,
        limit: usize,
        #[serde(default, skip_serializing_if = "usize_is_zero")]
        offset: usize,
    },
    InnerEquiJoin {
        node_id: String,
        left: String,
        right: String,
        left_key: LogicalPlanColumnRef,
        right_key: LogicalPlanColumnRef,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        composite_equality: Option<LogicalPlanCompositeJoinEqualityV1>,
    },
    LeftEquiJoin {
        node_id: String,
        left: String,
        right: String,
        left_key: LogicalPlanColumnRef,
        right_key: LogicalPlanColumnRef,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        composite_equality: Option<LogicalPlanCompositeJoinEqualityV1>,
    },
    SemiEquiJoin {
        node_id: String,
        left: String,
        right: String,
        left_key: LogicalPlanColumnRef,
        right_key: LogicalPlanColumnRef,
    },
    AntiEquiJoin {
        node_id: String,
        left: String,
        right: String,
        left_key: LogicalPlanColumnRef,
        right_key: LogicalPlanColumnRef,
    },
    FullEquiJoin {
        node_id: String,
        left: String,
        right: String,
        left_key: LogicalPlanColumnRef,
        right_key: LogicalPlanColumnRef,
        output_key: LogicalPlanColumnRef,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        composite_equality: Option<LogicalPlanCompositeJoinEqualityV1>,
    },
    TumblingWindow {
        node_id: String,
        input: String,
        event_time_column: LogicalPlanColumnRef,
        window_size_ns: i64,
    },
    LatestByKey {
        node_id: String,
        input: String,
        key_columns: Vec<LogicalPlanColumnRef>,
        ordering_column: LogicalPlanColumnRef,
    },
    RowNumber {
        node_id: String,
        input: String,
        partition_column: LogicalPlanColumnRef,
        order_column: LogicalPlanColumnRef,
        descending: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        rank_limit: Option<usize>,
    },
    Output {
        node_id: String,
        input: String,
        relation: LogicalPlanRelationRef,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalPlanBinaryJoinStepV1 {
    pub node_id: String,
    pub right_input: String,
    pub left_key: LogicalPlanColumnRef,
    pub right_key: LogicalPlanColumnRef,
    pub composite_equality: Option<LogicalPlanCompositeJoinEqualityV1>,
    pub join_kind: SupportedJoinKind,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanJoinKeyPairV1 {
    pub left_key: LogicalPlanColumnRef,
    pub right_key: LogicalPlanColumnRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanCompositeJoinEqualityV1 {
    pub schema_version: u32,
    pub additional_pairs: Vec<LogicalPlanJoinKeyPairV1>,
}

/// Lowers an ordered N-input join chain to ordinary binary logical nodes.
///
/// SQL admission remains responsible for proving that the chosen order is
/// semantically valid. This helper deliberately creates no N-way operator.
pub fn lower_join_chain_to_binary_dag(
    first_input: &str,
    steps: &[LogicalPlanBinaryJoinStepV1],
) -> Result<(Vec<VelorixLogicalViewPlanNodeV1>, String), ViewPlanError> {
    if first_input.trim().is_empty() {
        return invalid_logical_plan("binary join chain first input must be non-empty");
    }
    if steps.is_empty() {
        return invalid_logical_plan("binary join chain requires at least two inputs");
    }
    let mut current_left = first_input.to_string();
    let mut node_ids = BTreeSet::new();
    let mut nodes = Vec::with_capacity(steps.len());
    for step in steps {
        if step.node_id.trim().is_empty() || step.right_input.trim().is_empty() {
            return invalid_logical_plan("binary join chain identities must be non-empty");
        }
        if !node_ids.insert(step.node_id.clone()) {
            return invalid_logical_plan("binary join chain node ids must be unique");
        }
        if step.node_id == current_left || step.node_id == step.right_input {
            return invalid_logical_plan("binary join chain node cannot consume itself");
        }
        let node = match step.join_kind {
            SupportedJoinKind::Inner => VelorixLogicalViewPlanNodeV1::InnerEquiJoin {
                node_id: step.node_id.clone(),
                left: current_left,
                right: step.right_input.clone(),
                left_key: step.left_key.clone(),
                right_key: step.right_key.clone(),
                composite_equality: step.composite_equality.clone(),
            },
            SupportedJoinKind::Left => VelorixLogicalViewPlanNodeV1::LeftEquiJoin {
                node_id: step.node_id.clone(),
                left: current_left,
                right: step.right_input.clone(),
                left_key: step.left_key.clone(),
                right_key: step.right_key.clone(),
                composite_equality: step.composite_equality.clone(),
            },
            SupportedJoinKind::Full => {
                return invalid_logical_plan(
                    "full join requires a dedicated coalesced output-key lowering",
                )
            }
        };
        validate_logical_join_key_contracts(std::slice::from_ref(&node))?;
        current_left = step.node_id.clone();
        nodes.push(node);
    }
    Ok((nodes, current_left))
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanColumnRef {
    pub relation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_instance_id: Option<String>,
    pub column_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanComputedColumnV1 {
    pub output: LogicalPlanColumnRef,
    pub input_relation_id: String,
    pub expression: SupportedProjectionExpr,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanPredicateV1 {
    pub column: LogicalPlanColumnRef,
    pub op: PredicateOp,
    pub literal: JsonValue,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanAggregateAccumulatorV1 {
    pub function: LogicalPlanAggregateFunctionV1,
    pub input: Option<LogicalPlanColumnRef>,
    pub output_column_id: String,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalPlanAggregateFunctionV1 {
    Sum,
    Count,
    CountDistinct,
    Min,
    Max,
    Avg,
    /// Phase 8.2: exact discrete percentile. `percentile` is a
    /// compile-time literal in [0, 1]; the result is an input value at the
    /// discrete rank, exact across retractions and restart.
    PercentileDisc {
        percentile: f64,
    },
    /// Phase 8.2: exact continuous percentile (linear interpolation).
    PercentileCont {
        percentile: f64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanStateRequirementV1 {
    pub node_id: String,
    pub state_kind: LogicalPlanStateKindV1,
    pub key_columns: Vec<LogicalPlanColumnRef>,
    pub codec_version: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalPlanStateKindV1 {
    Aggregate,
    JoinIndex,
    TumblingWindowAggregate,
    LatestByKey,
    Projection,
    RowNumber,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VelorixLogicalViewExecutionV1 {
    SingleKeySumCount {
        plan: SupportedViewPlan,
    },
    FilterProject {
        plan: SupportedFilterProjectPlan,
    },
    LatestByKey {
        plan: SupportedLatestByKeyPlan,
    },
    AnalyticRowNumber {
        plan: SupportedAnalyticRowNumberPlan,
    },
    TwoInputJoinSumCount {
        plan: Box<SupportedJoinViewPlan>,
    },
    ThreeInputInnerJoinCount {
        plan: SupportedThreeInputInnerJoinCountPlanV1,
    },
    TwoInputSemiAntiJoinProject {
        plan: SupportedSemiAntiJoinProjectPlanV1,
    },
    TumblingEventTimeAggregate {
        plan: SupportedTumblingWindowPlan,
    },
    /// Phase 7.2: `WHERE outer_col <op> (SELECT <agg>(col) FROM inner)`
    /// for an uncorrelated scalar aggregate subquery over the second
    /// relation. The scalar is recomputed per epoch from the inner
    /// aggregate state and the outer predicate is evaluated against it;
    /// when the scalar changes the full outer bag is re-evaluated.
    ScalarAggregateFilter {
        plan: Box<SupportedScalarAggregateFilterPlanV1>,
    },
    /// Phase 8.1: bounded ROWS window frames with navigation functions
    /// (LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE). Exact-only; the frame
    /// is `ROWS BETWEEN k PRECEDING AND k FOLLOWING` with constant k.
    AnalyticWindowFrames {
        plan: Box<SupportedAnalyticWindowFramePlanV1>,
    },
    /// Phase 8.3: exact interval overlap inner join
    /// `left.start < right.end AND right.start < left.end` with non-null
    /// timestamp endpoints and a maximum interval duration.
    IntervalJoin {
        plan: Box<SupportedIntervalJoinPlanV1>,
    },
    /// Phase 8.5: `WITH RECURSIVE` positive fixpoint (UNION DISTINCT only).
    /// Every epoch recomputes the closure from the updated base multiset
    /// and diffs against the previous derived set (exact retractions).
    RecursiveFixpointV1 {
        plan: Box<SupportedRecursiveFixpointPlanV1>,
    },
    /// Phase 8.3: exact CROSS JOIN over two registered relations. The
    /// output is the full projected row per left/right pair; every epoch
    /// recomputes the pair set and diffs (exact retractions).
    CrossJoin {
        plan: Box<SupportedCrossJoinPlanV1>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedIntervalJoinPlanV1 {
    pub schema_version: u32,
    pub left_input_relation_id: String,
    pub right_input_relation_id: String,
    pub left_key_column_id: String,
    pub right_key_column_id: String,
    pub left_start_column_id: String,
    pub left_end_column_id: String,
    pub right_start_column_id: String,
    pub right_end_column_id: String,
    pub max_interval_duration_ns: i64,
    /// Output row projection over the left relation plus the right key:
    /// every output row is keyed by its full projected content, so the
    /// output schema primary key must cover all output columns.
    pub output_columns: Vec<IntervalJoinOutputColumnV1>,
    /// Output name of the right key column (part of every output row).
    pub right_key_output_name: String,
    pub resource_contract: IntervalJoinResourceContractV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IntervalJoinOutputColumnV1 {
    /// Left relation column id (or the left key) projected into the output.
    pub left_column_id: String,
    /// Output column name (also the DeltaKey object field).
    pub output_name: String,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IntervalJoinResourceContractV1 {
    pub max_intervals_per_side: u64,
    pub max_matches_per_epoch: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedAnalyticWindowFramePlanV1 {
    pub schema_version: u32,
    pub input_relation_id: String,
    pub key_column_id: String,
    pub output_key_column_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_key_input_column_id: Option<String>,
    pub partition_column_id: String,
    pub order_column_id: String,
    pub order_descending: bool,
    /// Constant ROWS frame bound (rows before and after the current row).
    pub frame_preceding: u64,
    pub frame_following: u64,
    pub function: WindowNavigationFunctionV1,
    pub output_column_id: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WindowNavigationFunctionV1 {
    Lag {
        value_column_id: String,
        offset: u64,
    },
    Lead {
        value_column_id: String,
        offset: u64,
    },
    FirstValue {
        value_column_id: String,
    },
    LastValue {
        value_column_id: String,
    },
    NthValue {
        value_column_id: String,
        n: u64,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedScalarAggregateFilterPlanV1 {
    pub schema_version: u32,
    pub outer_input_relation_id: String,
    pub scalar_input_relation_id: String,
    pub outer_key_column_id: String,
    /// The scalar aggregate over the inner relation (no GROUP BY).
    pub scalar_aggregate: SupportedAggregateOutput,
    /// The outer comparison: outer column op scalar-slot.
    pub outer_comparison_column_id: String,
    pub comparison_op: ScalarSubqueryComparisonOp,
    /// Public output projection over the outer relation.
    pub projection: SupportedFilterProjectPlan,
    pub resource_contract: ScalarAggregateResourceContractV1,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarSubqueryComparisonOp {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScalarAggregateResourceContractV1 {
    pub max_outer_rows: u64,
    pub max_recomputed_rows_per_epoch: u64,
    pub max_output_delta_rows: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedViewPlan {
    pub input_relation_id: String,
    pub group_key_column_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate_output_identity: Option<SupportedAggregateOutputIdentity>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub output_key_column_id: String,
    pub sum_value_column_id: String,
    #[serde(default)]
    pub aggregate_outputs: Vec<SupportedAggregateOutput>,
    pub predicate: Option<RowPredicate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate_expr: Option<RowPredicateExpr>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub aggregate_filter_exprs: BTreeMap<String, RowPredicateExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub having: Option<AggregateOutputPredicate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub having_expr: Option<AggregateOutputPredicateExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_k: Option<SupportedTopKPlan>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SupportedAggregateOutputIdentity {
    Singleton,
    GroupKey { group_keys: Vec<SupportedGroupKey> },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedGroupKey {
    pub output_column_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_column_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expression: Option<SupportedProjectionExpr>,
}

pub fn supported_view_plan_group_keys(plan: &SupportedViewPlan) -> Vec<SupportedGroupKey> {
    match &plan.aggregate_output_identity {
        Some(SupportedAggregateOutputIdentity::Singleton) => return Vec::new(),
        Some(SupportedAggregateOutputIdentity::GroupKey { group_keys }) => {
            return group_keys.clone();
        }
        None => {}
    }
    vec![SupportedGroupKey {
        output_column_id: if plan.output_key_column_id.is_empty() {
            plan.group_key_column_id.clone()
        } else {
            plan.output_key_column_id.clone()
        },
        input_column_id: Some(plan.group_key_column_id.clone()),
        expression: None,
    }]
}

pub fn supported_view_plan_is_singleton(plan: &SupportedViewPlan) -> bool {
    matches!(
        plan.aggregate_output_identity,
        Some(SupportedAggregateOutputIdentity::Singleton)
    )
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedTopKPlan {
    pub order_output_column_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_input_column_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tie_breaker_output_column_id: Option<String>,
    pub descending: bool,
    pub limit: usize,
    #[serde(default, skip_serializing_if = "usize_is_zero")]
    pub offset: usize,
}

fn usize_is_zero(value: &usize) -> bool {
    *value == 0
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedAggregateOutput {
    pub function: LogicalPlanAggregateFunctionV1,
    pub input_column_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_relation_side: Option<SupportedAggregateInputRelationSide>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_expression: Option<SupportedProjectionExpr>,
    pub output_column_id: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportedAggregateInputRelationSide {
    Left,
    Right,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedProjectionColumn {
    pub input_column_id: String,
    pub output_column_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expression: Option<SupportedProjectionExpr>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SupportedProjectionExpr {
    Column {
        column_id: String,
    },
    LiteralInt64 {
        value: i64,
    },
    BinaryInt64 {
        op: SupportedProjectionBinaryOp,
        left: Box<SupportedProjectionExpr>,
        right: Box<SupportedProjectionExpr>,
    },
    AbsInt64 {
        expr: Box<SupportedProjectionExpr>,
    },
    GreatestInt64 {
        exprs: Vec<SupportedProjectionExpr>,
    },
    LeastInt64 {
        exprs: Vec<SupportedProjectionExpr>,
    },
    CoalesceInt64 {
        column_id: String,
        fallback: i64,
    },
    CaseInt64 {
        predicate: RowPredicateExpr,
        then_expr: Box<SupportedProjectionExpr>,
        else_expr: Box<SupportedProjectionExpr>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportedProjectionBinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedFilterProjectPlan {
    pub input_relation_id: String,
    pub key_column_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub output_key_column_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_key_input_column_id: Option<String>,
    pub value_columns: Vec<SupportedProjectionColumn>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub typed_value_columns: Vec<TypedProjectionColumn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate_expr: Option<RowPredicateExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_k: Option<SupportedTopKPlan>,
}

/// A value column produced by a typed expression program (Phase 6 string,
/// temporal, and float families). Kept separate from the legacy Int64-only
/// `SupportedProjectionExpr` so persisted V1 plans stay byte-stable.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TypedProjectionColumn {
    pub output_column_id: String,
    pub program: TypedExprProgramV1,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportedSemiAntiJoinKindV1 {
    Semi,
    Anti,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedSemiAntiJoinProjectPlanV1 {
    pub schema_version: u32,
    pub join_kind: SupportedSemiAntiJoinKindV1,
    pub left_input_relation_id: String,
    pub right_input_relation_id: String,
    pub left_join_key_column_id: String,
    pub right_join_key_column_id: String,
    pub projection: SupportedFilterProjectPlan,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedLatestByKeyPlan {
    pub input_relation_id: String,
    pub key_column_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub output_key_column_id: String,
    pub value_column_id: String,
    pub ordering_column_id: String,
    pub output_value_column_id: String,
    #[serde(default)]
    pub function: LogicalPlanLatestByKeyFunctionV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate_expr: Option<RowPredicateExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_k: Option<SupportedTopKPlan>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedAnalyticRowNumberPlan {
    pub input_relation_id: String,
    pub key_column_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub output_key_column_id: String,
    #[serde(default)]
    pub function: SupportedAnalyticWindowFunction,
    pub partition_column_id: String,
    pub order_column_id: String,
    pub order_descending: bool,
    pub output_row_number_column_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate_expr: Option<RowPredicateExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank_limit: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportedAnalyticWindowFunction {
    #[default]
    RowNumber,
    Rank,
    DenseRank,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalPlanLatestByKeyFunctionV1 {
    #[default]
    ArgMax,
    ArgMin,
}

/// Public late-row handling policy for event-time windows. The policy is
/// part of the admitted plan and the checkpoint payload, so retractions and
/// restart replay the same decisions deterministically.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LateRowPolicy {
    /// Reject the whole input batch when any row is late (event time below
    /// the current watermark). The admission fails closed with a clear error.
    #[default]
    Reject,
    /// Drop late rows and record evidence (count per epoch) instead of
    /// failing. The view output is exact for everything admitted.
    DropWithEvidence,
    /// Admit rows whose event time is at least `allowance_ns` behind the
    /// watermark, up to a configured bound. Rows beyond the bound are
    /// dropped with evidence.
    AdmitWithinAllowance {
        /// Maximum allowed lateness in nanoseconds.
        allowance_ns: i64,
    },
}

impl LateRowPolicy {
    pub fn validate(&self) -> Result<(), ViewPlanError> {
        if let LateRowPolicy::AdmitWithinAllowance { allowance_ns } = self {
            if *allowance_ns < 0 {
                return Err(ViewPlanError::UnsupportedShape {
                    reason: "late-row allowance must be non-negative".to_string(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedTumblingWindowPlan {
    pub input_relation_id: String,
    pub group_key_column_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub output_key_column_id: String,
    pub event_time_column_id: String,
    #[serde(default)]
    pub window_kind: SupportedEventTimeWindowKind,
    pub window_size_ns: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hop_slide_ns: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_gap_ns: Option<i64>,
    pub sum_value_column_id: String,
    pub aggregate_outputs: Vec<SupportedAggregateOutput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate_expr: Option<RowPredicateExpr>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub aggregate_filter_exprs: BTreeMap<String, RowPredicateExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub having_expr: Option<AggregateOutputPredicateExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_k: Option<SupportedTopKPlan>,
    pub window_start_output_column_id: String,
    pub window_end_output_column_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub late_row_policy: Option<LateRowPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_contract: Option<StateRetentionContractV1>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportedEventTimeWindowKind {
    #[default]
    Tumbling,
    Hopping,
    Session,
}

pub fn supported_view_plan_aggregate_outputs(
    plan: &SupportedViewPlan,
) -> Vec<SupportedAggregateOutput> {
    if plan.aggregate_outputs.is_empty() {
        vec![
            SupportedAggregateOutput {
                function: LogicalPlanAggregateFunctionV1::Sum,
                input_column_id: Some(plan.sum_value_column_id.clone()),
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
    } else {
        plan.aggregate_outputs.clone()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedJoinViewPlan {
    pub left_input_relation_id: String,
    pub right_input_relation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left_input_instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right_input_instance_id: Option<String>,
    #[serde(default)]
    pub join_kind: SupportedJoinKind,
    pub left_join_key_column_id: String,
    pub right_join_key_column_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub composite_equality: Option<SupportedCompositeJoinEqualityV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_key_domain: Option<SupportedJoinKeyDomainV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate_output_identity: Option<SupportedAggregateOutputIdentity>,
    pub group_key_relation_id: String,
    pub group_key_column_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub output_key_column_id: String,
    pub sum_value_relation_id: String,
    pub sum_value_column_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right_value_column_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub right_value_column_ids: Vec<String>,
    #[serde(default)]
    pub aggregate_outputs: Vec<SupportedAggregateOutput>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub aggregate_filter_exprs: BTreeMap<String, JoinPredicateExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<JoinRowPredicate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub predicates: Vec<JoinRowPredicate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate_expr: Option<JoinPredicateExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub having: Option<AggregateOutputPredicate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub having_expr: Option<AggregateOutputPredicateExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_k: Option<SupportedTopKPlan>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedThreeInputInnerJoinCountPlanV1 {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub join_order_policy_id: String,
    pub ordered_input_relation_ids: Vec<String>,
    pub root_primary_key_column_ids: Vec<String>,
    pub output_key_column_ids: Vec<String>,
    pub count_output_column_id: String,
    pub join_key_codec_id: String,
    pub root_to_input_pk_permutations: Vec<Vec<usize>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedJoinKeyPairV1 {
    pub left_column_id: String,
    pub right_column_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedCompositeJoinEqualityV1 {
    pub schema_version: u32,
    pub additional_pairs: Vec<SupportedJoinKeyPairV1>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportedJoinKeyDomainV1 {
    NonPrimaryNonNullScalarV1,
}

pub fn supported_join_view_plan_key_pairs(
    plan: &SupportedJoinViewPlan,
) -> Result<Vec<SupportedJoinKeyPairV1>, ViewPlanError> {
    let first = SupportedJoinKeyPairV1 {
        left_column_id: plan.left_join_key_column_id.clone(),
        right_column_id: plan.right_join_key_column_id.clone(),
    };
    let mut pairs = vec![first];
    if let Some(composite) = &plan.composite_equality {
        if composite.schema_version != 1 {
            return unsupported("unsupported composite join equality schema version");
        }
        if composite.additional_pairs.is_empty() {
            return unsupported("composite join equality requires at least two key pairs");
        }
        pairs.extend(composite.additional_pairs.clone());
    }
    validate_ordered_join_key_pairs(
        pairs
            .iter()
            .map(|pair| (&pair.left_column_id, &pair.right_column_id)),
    )?;
    Ok(pairs)
}

pub fn supported_join_key_codec_id(plan: &SupportedJoinViewPlan) -> Option<&'static str> {
    match plan.join_key_domain {
        Some(SupportedJoinKeyDomainV1::NonPrimaryNonNullScalarV1) => {
            Some(NON_PRIMARY_NON_NULL_SCALAR_JOIN_KEY_CODEC_V1)
        }
        None if plan.composite_equality.is_some() => {
            Some(COMPOSITE_PK_POSITIONAL_JSON_ARRAY_JOIN_KEY_CODEC_V1)
        }
        None => None,
    }
}

pub fn supported_join_view_plan_is_singleton(plan: &SupportedJoinViewPlan) -> bool {
    matches!(
        plan.aggregate_output_identity,
        Some(SupportedAggregateOutputIdentity::Singleton)
    )
}

pub fn supported_join_view_plan_is_self_join(plan: &SupportedJoinViewPlan) -> bool {
    plan.left_input_relation_id == plan.right_input_relation_id
        && plan.left_input_instance_id.as_deref() == Some(LEFT_JOIN_INPUT_INSTANCE_ID_V1)
        && plan.right_input_instance_id.as_deref() == Some(RIGHT_JOIN_INPUT_INSTANCE_ID_V1)
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportedJoinKind {
    #[default]
    Inner,
    Left,
    Full,
}

pub fn supported_join_view_plan_aggregate_outputs(
    plan: &SupportedJoinViewPlan,
) -> Vec<SupportedAggregateOutput> {
    if plan.aggregate_outputs.is_empty() {
        vec![
            SupportedAggregateOutput {
                function: LogicalPlanAggregateFunctionV1::Sum,
                input_column_id: Some(plan.sum_value_column_id.clone()),
                input_relation_side: Some(SupportedAggregateInputRelationSide::Left),
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
    } else {
        plan.aggregate_outputs.clone()
    }
}

pub fn supported_join_view_plan_predicates(plan: &SupportedJoinViewPlan) -> Vec<JoinRowPredicate> {
    if let Some(predicate_expr) = &plan.predicate_expr {
        return predicate_expr.leaf_predicates();
    }
    plan.predicate
        .clone()
        .into_iter()
        .chain(plan.predicates.clone())
        .collect()
}

pub fn supported_join_view_plan_right_value_column_id(plan: &SupportedJoinViewPlan) -> &str {
    plan.right_value_column_id
        .as_deref()
        .unwrap_or(&plan.right_join_key_column_id)
}

pub fn supported_join_view_plan_right_value_column_ids(
    plan: &SupportedJoinViewPlan,
) -> Vec<String> {
    if !plan.right_value_column_ids.is_empty() {
        plan.right_value_column_ids.clone()
    } else {
        vec![supported_join_view_plan_right_value_column_id(plan).to_string()]
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RowPredicate {
    pub column_id: String,
    pub op: PredicateOp,
    pub literal: JsonValue,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum RowPredicateExpr {
    Atom {
        predicate: RowPredicate,
    },
    ScalarInt64Comparison {
        left: Box<SupportedProjectionExpr>,
        comparison_op: PredicateOp,
        literal: JsonValue,
    },
    ScalarInt64ExpressionComparison {
        left: Box<SupportedProjectionExpr>,
        comparison_op: PredicateOp,
        right: Box<SupportedProjectionExpr>,
    },
    And {
        left: Box<RowPredicateExpr>,
        right: Box<RowPredicateExpr>,
    },
    Or {
        left: Box<RowPredicateExpr>,
        right: Box<RowPredicateExpr>,
    },
}

impl RowPredicateExpr {
    pub fn leaf_predicates(&self) -> Vec<RowPredicate> {
        match self {
            Self::Atom { predicate } => vec![predicate.clone()],
            Self::ScalarInt64Comparison { .. } | Self::ScalarInt64ExpressionComparison { .. } => {
                Vec::new()
            }
            Self::And { left, right } | Self::Or { left, right } => {
                let mut predicates = left.leaf_predicates();
                predicates.extend(right.leaf_predicates());
                predicates
            }
        }
    }

    pub fn contains_or(&self) -> bool {
        match self {
            Self::Atom { .. }
            | Self::ScalarInt64Comparison { .. }
            | Self::ScalarInt64ExpressionComparison { .. } => false,
            Self::And { left, right } => left.contains_or() || right.contains_or(),
            Self::Or { .. } => true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JoinRowPredicate {
    pub relation_id: String,
    pub predicate: RowPredicate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum JoinPredicateExpr {
    Atom {
        predicate: JoinRowPredicate,
    },
    ScalarInt64Comparison {
        relation_id: String,
        left: Box<SupportedProjectionExpr>,
        comparison_op: PredicateOp,
        literal: JsonValue,
    },
    ScalarInt64ExpressionComparison {
        left_relation_id: String,
        left: Box<SupportedProjectionExpr>,
        comparison_op: PredicateOp,
        right_relation_id: String,
        right: Box<SupportedProjectionExpr>,
    },
    And {
        left: Box<JoinPredicateExpr>,
        right: Box<JoinPredicateExpr>,
    },
    Or {
        left: Box<JoinPredicateExpr>,
        right: Box<JoinPredicateExpr>,
    },
}

impl JoinPredicateExpr {
    pub fn leaf_predicates(&self) -> Vec<JoinRowPredicate> {
        match self {
            Self::Atom { predicate } => vec![predicate.clone()],
            Self::ScalarInt64Comparison { .. } | Self::ScalarInt64ExpressionComparison { .. } => {
                Vec::new()
            }
            Self::And { left, right } | Self::Or { left, right } => {
                let mut predicates = left.leaf_predicates();
                predicates.extend(right.leaf_predicates());
                predicates
            }
        }
    }

    pub fn contains_or(&self) -> bool {
        match self {
            Self::Atom { .. }
            | Self::ScalarInt64Comparison { .. }
            | Self::ScalarInt64ExpressionComparison { .. } => false,
            Self::And { left, right } => left.contains_or() || right.contains_or(),
            Self::Or { .. } => true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateOutputPredicate {
    pub output_column_id: String,
    pub op: PredicateOp,
    pub literal: JsonValue,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum AggregateOutputPredicateExpr {
    Atom {
        predicate: AggregateOutputPredicate,
    },
    And {
        left: Box<AggregateOutputPredicateExpr>,
        right: Box<AggregateOutputPredicateExpr>,
    },
    Or {
        left: Box<AggregateOutputPredicateExpr>,
        right: Box<AggregateOutputPredicateExpr>,
    },
}

impl AggregateOutputPredicateExpr {
    pub fn leaf_predicates(&self) -> Vec<AggregateOutputPredicate> {
        match self {
            Self::Atom { predicate } => vec![predicate.clone()],
            Self::And { left, right } | Self::Or { left, right } => {
                let mut predicates = left.leaf_predicates();
                predicates.extend(right.leaf_predicates());
                predicates
            }
        }
    }

    pub fn contains_or(&self) -> bool {
        match self {
            Self::Atom { .. } => false,
            Self::And { left, right } => left.contains_or() || right.contains_or(),
            Self::Or { .. } => true,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PredicateOp {
    Eq,
    NotEq,
    Like,
    NotLike,
    IsNull,
    IsNotNull,
    Gt,
    GtEq,
    Lt,
    LtEq,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

#[derive(Debug, Error)]
pub enum ViewPlanError {
    #[error(transparent)]
    Relation(#[from] RelationSchemaError),
    #[error("view SQL parse error: {0}")]
    Parse(#[from] ParserError),
    #[error("view SQL is outside the supported materialization scope: {reason}")]
    UnsupportedShape { reason: String },
    #[error("invalid logical view plan: {reason}")]
    InvalidLogicalPlan { reason: String },
}

fn parse_single_query(sql: &str) -> Result<Box<Query>, ViewPlanError> {
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)?;
    if statements.len() != 1 {
        return unsupported("expected exactly one SELECT statement");
    }
    let statement = statements
        .pop()
        .expect("validated statement count must be one");
    let SqlStatement::Query(query) = statement else {
        return unsupported("expected a SELECT statement");
    };

    Ok(query)
}

pub fn lower_supported_sql_to_logical_plan(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    if let [catalog] = catalogs {
        if let Ok(query) = parse_single_query(sql) {
            if query.with.as_ref().is_some_and(|with| with.recursive) {
                return lower_supported_recursive_cte_sql_to_logical_plan(
                    sql,
                    catalog,
                    output_schema,
                );
            }
        }
    }
    match catalogs {
        [catalog] => match lower_supported_join_view_sql_to_logical_plan(sql, catalogs, output_schema)
        {
            Ok(plan) => Ok(plan),
            Err(ViewPlanError::UnsupportedShape { .. }) => {
                match lower_supported_view_sql_to_logical_plan(sql, catalog, output_schema) {
                    Ok(plan) => Ok(plan),
                    Err(ViewPlanError::UnsupportedShape { .. }) => {
                match lower_supported_latest_by_key_sql_to_logical_plan(sql, catalog, output_schema)
                {
                    Ok(plan) => Ok(plan),
                    Err(ViewPlanError::UnsupportedShape { .. }) => {
                        match lower_supported_tumbling_window_sql_to_logical_plan(
                            sql,
                            catalog,
                            output_schema,
                        ) {
                            Ok(plan) => Ok(plan),
                            Err(ViewPlanError::UnsupportedShape { .. }) => {
                                match lower_supported_analytic_row_number_sql_to_logical_plan(
                                    sql,
                                    catalog,
                                    output_schema,
                                ) {
                                    Ok(plan) => Ok(plan),
                                    Err(ViewPlanError::UnsupportedShape { .. }) => {
                                        match lower_supported_analytic_window_frame_sql_to_logical_plan(
                                            sql,
                                            catalog,
                                            output_schema,
                                        ) {
                                            Ok(plan) => Ok(plan),
                                            Err(ViewPlanError::UnsupportedShape { .. }) => {
                                                lower_supported_filter_project_sql_to_logical_plan(
                                                    sql,
                                                    catalog,
                                                    output_schema,
                                                )
                                            }
                                            Err(error) => Err(error),
                                        }
                                    }
                                    Err(error) => Err(error),
                                }
                            }
                            Err(error) => Err(error),
                        }
                    }
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        },
        [_, _] => {
            match lower_supported_scalar_aggregate_filter_sql_to_logical_plan(
                sql,
                catalogs,
                output_schema,
            ) {
                Ok(plan) => Ok(plan),
                Err(ViewPlanError::UnsupportedShape { .. }) => {
                    match lower_supported_semi_anti_join_sql_to_logical_plan(
                        sql,
                        catalogs,
                        output_schema,
                    ) {
                        Ok(plan) => Ok(plan),
                        Err(ViewPlanError::UnsupportedShape { .. }) => {
                            match lower_supported_cross_join_sql_to_logical_plan(
                                sql, catalogs, output_schema,
                            ) {
                                Ok(plan) => Ok(plan),
                                Err(ViewPlanError::UnsupportedShape { .. }) => {
                                    match lower_supported_interval_join_sql_to_logical_plan(
                                        sql, catalogs, output_schema,
                                    ) {
                                        Ok(plan) => Ok(plan),
                                        Err(ViewPlanError::UnsupportedShape { .. }) => {
                                            lower_supported_join_view_sql_to_logical_plan(
                                                sql, catalogs, output_schema,
                                            )
                                        }
                                        Err(error) => Err(error),
                                    }
                                }
                                Err(error) => Err(error),
                            }
                        }
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(error),
            }
        }
        [_, _, _] => {
            lower_supported_three_input_inner_join_count_sql_to_logical_plan(
                sql,
                catalogs,
                output_schema,
            )
        }
        _ => unsupported("view SQL admission currently supports one, two, or exactly three input relations"),
    }
}

pub fn lower_supported_view_sql_to_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_view_sql(sql, catalog)?;
    finalize_logical_plan(single_key_sum_count_logical_plan(
        sql,
        catalog,
        output_schema,
        supported,
    ))
}

pub fn lower_supported_join_view_sql_to_logical_plan(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_join_view_sql(sql, catalogs)?;
    finalize_logical_plan(two_input_join_sum_count_logical_plan(
        sql,
        catalogs,
        output_schema,
        supported,
    )?)
}

/// Lower a published-view-output single-key sum/count view.
///
/// This is the dedicated Phase 4 view-on-view admission entry point. It does NOT
/// fall back to other plan families. It verifies the input is a published delta,
/// that the planner input fingerprint matches the relation schema, and that the
/// `PublishedDelta` codec/frontier match the resolved edge, then lowers only the
/// `SingleKeySumCount` family.
///
/// `expected_codec` and `expected_frontier` come from the resolved
/// `ViewDependencyEdgeBindingV1` (the consumer's persisted dependency edge), so
/// a stale generation or mismatched codec is rejected before planning.
pub fn lower_published_single_key_sum_count_sql(
    sql: &str,
    input: &PlannerRelationInput,
    output_schema: &RelationSchema,
    expected_codec: &str,
    expected_frontier: &str,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let PlannerChangeEncoding::PublishedDelta {
        delta_codec_identity,
        frontier_kind,
    } = &input.change_encoding
    else {
        return unsupported("published view input must use PublishedDelta change encoding");
    };
    if delta_codec_identity != expected_codec || frontier_kind != expected_frontier {
        return unsupported(
            "published view input codec or frontier does not match the admitted dependency edge",
        );
    }
    if input.schema_fingerprint != input.relation.schema_fingerprint {
        return unsupported(
            "published view input schema fingerprint does not match its relation schema",
        );
    }
    if input.weight_column_id.is_some() || input.event_time_column_id.is_some() {
        return unsupported(
            "published view input must not carry a physical weight or event-time column",
        );
    }
    let key_column = relation_primary_key_column(&input.relation)?;
    let supported = validate_supported_view_sql_with_input(sql, input, None)?;
    finalize_logical_plan(single_key_sum_count_logical_plan_from_input(
        sql,
        &input.relation,
        key_column,
        output_schema,
        supported,
    )?)
}

/// Infer the output schema of a published single-key sum/count view from its
/// SQL projection.
///
/// This is the catalog-free analog of the catalog-backed output-schema
/// inference used by `output_schemas_for_view_request`. It reads the projection
/// and group key from SQL and the published input schema, so create_view can
/// derive the consumer output schema before lowering. The resulting schema must
/// be reused identically for the plan, `ViewSpec`, published binding, runtime
/// factory, and create response.
pub fn infer_single_key_sum_count_output_schema(
    sql: &str,
    input: &PlannerRelationInput,
    view_id: &str,
) -> Result<RelationSchema, ViewPlanError> {
    let [key_column_id] = input.relation.primary_key.as_slice() else {
        return unsupported("published view SQL requires exactly one primary key column");
    };
    let key_column = input
        .relation
        .columns
        .iter()
        .find(|column| &column.name == key_column_id)
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "published view primary key column is missing".to_string(),
        })?;
    let query = parse_single_query(sql)?;
    let select = match query.body.as_ref() {
        sqlparser::ast::SetExpr::Select(select) => select.as_ref(),
        _ => return unsupported("published view SQL requires a plain SELECT"),
    };
    let mut columns = vec![ColumnSchema {
        name: key_column.name.clone(),
        data_type: key_column.data_type.clone(),
        nullable: false,
    }];
    let primary_key = vec![key_column.name.clone()];
    for item in select.projection.iter().skip(1) {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
            _ => {
                return unsupported(
                    "published view aggregate projections must be scalar expressions",
                )
            }
        };
        let Expr::Function(function) = expr else {
            return unsupported("published view aggregate projections must be aggregate functions");
        };
        let function_name = single_object_name_identifier(&function.name).ok_or_else(|| {
            ViewPlanError::UnsupportedShape {
                reason: "published view aggregate function name is not recognized".to_string(),
            }
        })?;
        let output_name = alias.unwrap_or(function_name.as_str()).to_string();
        let upper = function_name.to_ascii_uppercase();
        let data_type = match upper.as_str() {
            "SUM" => SqlDataType::Int64,
            "COUNT" => SqlDataType::Int64,
            _ => {
                return unsupported(
                    "published view aggregate only supports SUM and COUNT in this slice",
                );
            }
        };
        columns.push(ColumnSchema {
            name: output_name,
            data_type,
            nullable: false,
        });
    }
    if columns.len() == 1 {
        return unsupported("published view SQL requires at least one aggregate projection");
    }
    let relation = RelationSchema {
        relation_id: format!("{view_id}_output"),
        relation_name: format!("{view_id}_output"),
        relation_version: "v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "0".repeat(64)),
        columns,
        primary_key,
    };
    Ok(relation)
}

pub fn lower_supported_semi_anti_join_sql_to_logical_plan(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_semi_anti_join_sql(sql, catalogs)?;
    finalize_logical_plan(semi_anti_join_project_logical_plan(
        sql,
        catalogs,
        output_schema,
        supported,
    )?)
}

pub fn lower_supported_three_input_inner_join_count_sql_to_logical_plan(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    lower_supported_three_input_inner_join_count_sql_to_logical_plan_with_policy(
        sql,
        catalogs,
        output_schema,
        THREE_INPUT_ROOT_FIXED_RIGHT_RELATION_ID_JOIN_ORDER_V1,
    )
}

pub fn lower_supported_three_input_inner_join_count_sql_to_logical_plan_with_policy(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
    join_order_policy_id: &str,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_three_input_inner_join_count_sql_with_policy(
        sql,
        catalogs,
        join_order_policy_id,
    )?;
    finalize_logical_plan(three_input_inner_join_count_logical_plan(
        sql,
        catalogs,
        output_schema,
        supported,
    )?)
}

pub fn validate_logical_view_plan(plan: &VelorixLogicalViewPlanV1) -> Result<(), ViewPlanError> {
    if plan.plan_version != LOGICAL_VIEW_PLAN_VERSION_V2 {
        return invalid_logical_plan("unsupported logical view plan version");
    }
    if plan.capability_version != LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2 {
        return invalid_logical_plan("unsupported logical view capability version");
    }
    if plan.key_semantics_version != INCREMENTAL_KEY_SEMANTICS_VERSION_V1 {
        return invalid_logical_plan("unsupported incremental key semantics version");
    }
    if plan.bag_semantics_version != INCREMENTAL_BAG_SEMANTICS_VERSION_V1 {
        return invalid_logical_plan("unsupported incremental bag semantics version");
    }
    if plan.output_codec_version != LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1 {
        return invalid_logical_plan("unsupported materialized output codec version");
    }
    if plan.input_relations.is_empty() {
        return invalid_logical_plan("logical view plan must have input relations");
    }
    if plan.nodes.is_empty() {
        return invalid_logical_plan("logical view plan must have nodes");
    }
    validate_logical_join_key_contracts(&plan.nodes)?;
    if let VelorixLogicalViewExecutionV1::ThreeInputInnerJoinCount { plan: supported } =
        &plan.execution
    {
        validate_three_input_inner_join_logical_contract(plan, supported)?;
    }
    plan.operator_dag_contract
        .validate()
        .map_err(|error| ViewPlanError::InvalidLogicalPlan {
            reason: format!("invalid operator DAG contract: {error}"),
        })?;
    let expected_operator_contract = derive_operator_dag_contract(plan)?;
    if plan.operator_dag_contract != expected_operator_contract {
        return invalid_logical_plan(
            "operator DAG contract does not match the admitted logical operators",
        );
    }
    let Some(execution_implementation) = &plan.execution_implementation else {
        return invalid_logical_plan("execution implementation identity is missing");
    };
    let expected_execution_implementation = derive_execution_implementation(plan)?;
    if execution_implementation != &expected_execution_implementation {
        return invalid_logical_plan(
            "execution implementation identity does not match the admitted physical DAG",
        );
    }
    if plan
        .nodes
        .iter()
        .filter(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Output { .. }))
        .count()
        != 1
    {
        return invalid_logical_plan("logical view plan must have exactly one output node");
    }
    let Some(plan_hash) = &plan.plan_hash else {
        return invalid_logical_plan("logical view plan hash is missing");
    };
    let expected = logical_view_plan_hash(plan)?;
    if plan_hash != &expected {
        return invalid_logical_plan("logical view plan hash mismatch");
    }
    Ok(())
}

fn validate_three_input_inner_join_logical_contract(
    logical_plan: &VelorixLogicalViewPlanV1,
    plan: &SupportedThreeInputInnerJoinCountPlanV1,
) -> Result<(), ViewPlanError> {
    if !three_input_join_order_policy_is_valid(plan)
        || plan.ordered_input_relation_ids.len() != 3
        || plan
            .ordered_input_relation_ids
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            != 3
        || plan.root_primary_key_column_ids.len() < 2
        || plan
            .root_primary_key_column_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || plan.output_key_column_ids.len() != plan.root_primary_key_column_ids.len()
        || plan.root_to_input_pk_permutations.len() != 3
        || plan.join_key_codec_id != COMPOSITE_PK_POSITIONAL_JSON_ARRAY_JOIN_KEY_CODEC_V1
        || logical_plan.input_relations.len() != 3
        || logical_plan
            .input_relations
            .iter()
            .map(|relation| &relation.relation_id)
            .ne(plan.ordered_input_relation_ids.iter())
    {
        return invalid_logical_plan("invalid three-input join plan identity");
    }
    let arity = plan.root_primary_key_column_ids.len();
    for permutation in &plan.root_to_input_pk_permutations {
        if permutation.len() != arity
            || permutation.iter().copied().collect::<BTreeSet<_>>().len() != arity
            || permutation.iter().any(|position| *position >= arity)
        {
            return invalid_logical_plan("three-input join PK mapping must be a bijection");
        }
    }
    if plan.root_to_input_pk_permutations[0] != (0..arity).collect::<Vec<_>>() {
        return invalid_logical_plan("three-input join root PK mapping must preserve identity");
    }
    let scans = logical_plan
        .nodes
        .iter()
        .filter_map(|node| match node {
            VelorixLogicalViewPlanNodeV1::RelationScan { node_id, relation } => {
                Some((node_id.as_str(), relation.relation_id.as_str()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if scans
        != vec![
            ("scan_0", plan.ordered_input_relation_ids[0].as_str()),
            ("scan_1", plan.ordered_input_relation_ids[1].as_str()),
            ("scan_2", plan.ordered_input_relation_ids[2].as_str()),
        ]
    {
        return invalid_logical_plan("three-input join scans do not match ordered inputs");
    }
    let joins = logical_plan
        .nodes
        .iter()
        .filter_map(|node| match node {
            VelorixLogicalViewPlanNodeV1::InnerEquiJoin {
                node_id,
                left,
                right,
                left_key,
                right_key,
                composite_equality,
            } => Some((
                node_id,
                left,
                right,
                left_key,
                right_key,
                composite_equality,
            )),
            VelorixLogicalViewPlanNodeV1::LeftEquiJoin { .. } => None,
            _ => None,
        })
        .collect::<Vec<_>>();
    if joins.len() != 2 {
        return invalid_logical_plan("three-input join requires exactly two inner joins");
    }
    for (index, (node_id, left, right, left_key, right_key, composite)) in
        joins.into_iter().enumerate()
    {
        let step = index + 1;
        let expected_left = if step == 1 { "scan_0" } else { "join_1" };
        if node_id != &format!("join_{step}")
            || left != expected_left
            || right != &format!("scan_{step}")
            || left_key.relation_id != plan.ordered_input_relation_ids[0]
            || right_key.relation_id != plan.ordered_input_relation_ids[step]
        {
            return invalid_logical_plan("three-input join topology must be left-deep");
        }
        let mut left_columns = vec![left_key.column_id.as_str()];
        let mut right_columns = vec![right_key.column_id.as_str()];
        let Some(composite) = composite else {
            return invalid_logical_plan("three-input join requires composite equality");
        };
        left_columns.extend(
            composite
                .additional_pairs
                .iter()
                .map(|pair| pair.left_key.column_id.as_str()),
        );
        right_columns.extend(
            composite
                .additional_pairs
                .iter()
                .map(|pair| pair.right_key.column_id.as_str()),
        );
        if left_columns
            != plan
                .root_primary_key_column_ids
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
            || right_columns.len() != arity
            || right_columns.iter().collect::<BTreeSet<_>>().len() != arity
        {
            return invalid_logical_plan("three-input join key lineage does not preserve root PK");
        }
    }
    let has_exact_tail = logical_plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Project { node_id, input, .. }
            if node_id == "project_three_input_count" && input == "join_2"
    )) && logical_plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Aggregate { node_id, input, group_keys, accumulators }
            if node_id == "aggregate_three_input_count"
                && input == "project_three_input_count"
                && group_keys.iter().map(|key| key.column_id.as_str()).eq(plan.root_primary_key_column_ids.iter().map(String::as_str))
                && matches!(accumulators.as_slice(), [LogicalPlanAggregateAccumulatorV1 { function: LogicalPlanAggregateFunctionV1::Count, input: None, output_column_id }] if output_column_id == &plan.count_output_column_id)
    )) && logical_plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Output { input, relation, .. }
            if input == "aggregate_three_input_count" && relation == &logical_plan.output_relation
    ));
    if !has_exact_tail {
        return invalid_logical_plan("three-input join aggregate/output lineage is invalid");
    }
    Ok(())
}

fn three_input_join_order_policy_is_valid(plan: &SupportedThreeInputInnerJoinCountPlanV1) -> bool {
    matches!(
        (plan.schema_version, plan.join_order_policy_id.as_str()),
        (1, "") | (2, THREE_INPUT_ROOT_FIXED_RIGHT_RELATION_ID_JOIN_ORDER_V1)
    )
}

fn validate_logical_join_key_contracts(
    nodes: &[VelorixLogicalViewPlanNodeV1],
) -> Result<(), ViewPlanError> {
    let scan_relations = nodes
        .iter()
        .filter_map(|node| match node {
            VelorixLogicalViewPlanNodeV1::RelationScan { node_id, relation } => {
                Some((node_id.as_str(), relation.relation_id.as_str()))
            }
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    for node in nodes {
        let (left_key, right_key, composite_equality) = match node {
            VelorixLogicalViewPlanNodeV1::InnerEquiJoin {
                left_key,
                right_key,
                composite_equality,
                ..
            }
            | VelorixLogicalViewPlanNodeV1::LeftEquiJoin {
                left_key,
                right_key,
                composite_equality,
                ..
            } => (left_key, right_key, composite_equality),
            VelorixLogicalViewPlanNodeV1::SemiEquiJoin {
                left_key,
                right_key,
                ..
            }
            | VelorixLogicalViewPlanNodeV1::AntiEquiJoin {
                left_key,
                right_key,
                ..
            } => (left_key, right_key, &None),
            _ => continue,
        };
        let mut pairs = vec![(left_key, right_key)];
        match (
            left_key.input_instance_id.as_deref(),
            right_key.input_instance_id.as_deref(),
        ) {
            (None, None) if left_key.relation_id != right_key.relation_id => {}
            (Some(left_instance), Some(right_instance))
                if left_instance != right_instance
                    && (scan_relations.is_empty()
                        || (scan_relations.get(left_instance).copied()
                            == Some(left_key.relation_id.as_str())
                            && scan_relations.get(right_instance).copied()
                                == Some(right_key.relation_id.as_str()))) => {}
            _ => {
                return invalid_logical_plan(
                    "duplicated join relations require distinct canonical scan instance identities",
                )
            }
        }
        if let Some(composite) = composite_equality {
            if composite.schema_version != 1 {
                return invalid_logical_plan(
                    "unsupported logical composite join equality schema version",
                );
            }
            if composite.additional_pairs.is_empty() {
                return invalid_logical_plan(
                    "logical composite join equality requires at least two key pairs",
                );
            }
            pairs.extend(
                composite
                    .additional_pairs
                    .iter()
                    .map(|pair| (&pair.left_key, &pair.right_key)),
            );
        }
        if pairs.iter().any(|(left, right)| {
            left.relation_id != left_key.relation_id
                || left.input_instance_id != left_key.input_instance_id
                || right.relation_id != right_key.relation_id
                || right.input_instance_id != right_key.input_instance_id
        }) {
            return invalid_logical_plan(
                "logical composite join keys must preserve left and right relation direction",
            );
        }
        validate_ordered_join_key_pairs(
            pairs
                .iter()
                .map(|(left, right)| (&left.column_id, &right.column_id)),
        )?;
    }
    Ok(())
}

fn validate_ordered_join_key_pairs<'a>(
    pairs: impl IntoIterator<Item = (&'a String, &'a String)>,
) -> Result<(), ViewPlanError> {
    let mut previous: Option<(&str, &str)> = None;
    let mut left_columns = BTreeSet::new();
    let mut right_columns = BTreeSet::new();
    for (left, right) in pairs {
        if left.trim().is_empty() || right.trim().is_empty() {
            return invalid_logical_plan("join key column identities must be non-empty");
        }
        let pair = (left.as_str(), right.as_str());
        if previous.is_some_and(|previous| previous >= pair) {
            return invalid_logical_plan(
                "join key pairs must be unique and ordered lexicographically",
            );
        }
        if !left_columns.insert(left.as_str()) || !right_columns.insert(right.as_str()) {
            return invalid_logical_plan("join key columns must be unique on each input side");
        }
        previous = Some(pair);
    }
    Ok(())
}

pub fn logical_view_plan_hash(plan: &VelorixLogicalViewPlanV1) -> Result<String, ViewPlanError> {
    let mut canonical = plan.clone();
    canonical.plan_hash = None;
    let bytes =
        serde_json::to_vec(&canonical).map_err(|source| ViewPlanError::InvalidLogicalPlan {
            reason: format!("could not serialize logical plan: {source}"),
        })?;
    Ok(format!(
        "{LOGICAL_VIEW_PLAN_HASH_PREFIX}:{}",
        stable_bytes_hash(&bytes)
    ))
}

pub fn lower_supported_latest_by_key_sql_to_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_latest_by_key_sql(sql, catalog)?;
    finalize_logical_plan(latest_by_key_logical_plan(
        sql,
        catalog,
        output_schema,
        supported,
    ))
}

pub fn lower_supported_analytic_row_number_sql_to_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_analytic_row_number_sql(sql, catalog)?;
    finalize_logical_plan(analytic_row_number_logical_plan(
        sql,
        catalog,
        output_schema,
        supported,
    ))
}

pub fn lower_supported_tumbling_window_sql_to_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_tumbling_window_sql(sql, catalog)?;
    finalize_logical_plan(tumbling_window_logical_plan(
        sql,
        catalog,
        output_schema,
        supported,
    ))
}

/// Lowering variant with an explicit late-row policy. The policy becomes part
/// of the admitted plan (and therefore the checkpoint payload), so retractions
/// and restart replay identical decisions. `None` means the default
/// `LateRowPolicy::Reject`.
pub fn lower_supported_tumbling_window_sql_to_logical_plan_with_policy(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
    late_row_policy: Option<LateRowPolicy>,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported =
        validate_supported_tumbling_window_sql_with_policy(sql, catalog, late_row_policy)?;
    finalize_logical_plan(tumbling_window_logical_plan(
        sql,
        catalog,
        output_schema,
        supported,
    ))
}

pub fn lower_supported_filter_project_sql_to_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_filter_project_sql(sql, catalog)?;
    validate_filter_project_output_schema_shape(catalog, output_schema, &supported)?;
    finalize_logical_plan(filter_project_logical_plan(
        sql,
        catalog,
        output_schema,
        supported,
    ))
}

fn validate_filter_project_output_schema_shape(
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
    plan: &SupportedFilterProjectPlan,
) -> Result<(), ViewPlanError> {
    if plan.output_key_input_column_id.is_none() {
        return Ok(());
    }
    let [key, value_columns @ ..] = output_schema.columns.as_slice() else {
        return unsupported("filter/project output schema requires a primary key column");
    };
    let output_key_input_column = plan
        .output_key_input_column_id
        .as_deref()
        .map(|column_id| catalog_column_by_id(catalog, column_id))
        .transpose()?
        .unwrap_or(catalog_primary_key_column(catalog)?);
    let expected_key_name = if plan.output_key_column_id.is_empty() {
        output_key_input_column.name.as_str()
    } else {
        plan.output_key_column_id.as_str()
    };
    if output_schema.primary_key != vec![key.name.clone()]
        || !identifier_eq(key.name.as_str(), expected_key_name)
        || value_columns.len() != plan.value_columns.len()
    {
        return unsupported(
            "filter/project output schema primary key must match the projected output key",
        );
    }
    let mut output_names = BTreeSet::from([key.name.clone()]);
    for (output_column, projection) in value_columns.iter().zip(plan.value_columns.iter()) {
        if !identifier_eq(
            output_column.name.as_str(),
            projection.output_column_id.as_str(),
        ) || !output_names.insert(output_column.name.clone())
        {
            return unsupported("filter/project output schema columns must match projection");
        }
    }
    Ok(())
}

pub fn validate_supported_view_sql(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedViewPlan, ViewPlanError> {
    catalog.validate()?;
    let adapter = crate::relation::supported_incremental_adapter_spec(
        &catalog.incremental_adapter.adapter_id,
    )
    .ok_or(RelationSchemaError::InvalidRelationSchema {
        field: "incremental_adapter.adapter_id",
    })?;
    if !matches!(
        adapter,
        SupportedIncrementalAdapterSpec::ScalarSumCount | SupportedIncrementalAdapterSpec::Generic
    ) {
        return unsupported("view SQL currently supports scalar single-key sum/count views");
    }
    let key_column = catalog_primary_key_column(catalog)?;
    let query = parse_single_query(sql)?;
    // Phase 7.1: an aggregate CTE followed by an identity/filter projection
    // over the CTE is inlined into a plain aggregate query (the outer
    // WHERE merges into the inner WHERE/HAVING). The runtime and the stored
    // plan still see the original SQL text, so identity hashing is stable.
    let query = if let Some(merged) = inline_aggregate_cte(query.clone(), catalog)? {
        Box::new(merged)
    } else {
        query
    };
    let (select, cte_source) =
        supported_plain_select_allow_identity_cte_and_top_k(&query, catalog)?;

    let from_source = validate_from_relation_allow_having_and_distinct_on_with_cte(
        select,
        catalog,
        cte_source.as_ref().map(|source| source.alias.as_str()),
    )?;
    let relation_alias = from_source.alias.as_deref();
    let source_projection = cte_source
        .as_ref()
        .and_then(|source| source.projection.as_ref())
        .or(from_source.projection.as_ref());
    let group_keys =
        validate_aggregate_group_keys(select, catalog, relation_alias, source_projection)?;
    let projection = validate_projection(
        select,
        catalog,
        key_column,
        relation_alias,
        source_projection,
        &group_keys,
    )?;
    if group_keys.is_empty() {
        let [count] = projection.aggregate_outputs.as_slice() else {
            return unsupported("global aggregate currently supports exactly count(*)");
        };
        if count.function != LogicalPlanAggregateFunctionV1::Count
            || count.input_column_id.is_some()
            || count.input_expression.is_some()
            || select.distinct.is_some()
            || !projection.aggregate_filter_exprs.is_empty()
            || projection.aggregate_filter_expr.is_some()
            || select.having.is_some()
        {
            return unsupported("global aggregate currently supports exactly count(*)");
        }
    }
    validate_aggregate_source_projection(
        source_projection,
        &group_keys,
        &projection.aggregate_outputs,
    )?;
    validate_distinct_on_group_key(
        select,
        catalog,
        key_column,
        relation_alias,
        &projection.output_key_column_id,
    )?;
    let predicate_expr = combine_row_predicate_exprs(
        validate_selection_with_cte_source(
            select,
            cte_source.as_ref(),
            from_source.source_selection.as_ref(),
            catalog,
            key_column,
            projection.value_column,
            relation_alias,
        )?,
        projection.aggregate_filter_expr.clone(),
    );
    let predicates = predicate_expr
        .as_ref()
        .map(RowPredicateExpr::leaf_predicates)
        .unwrap_or_default();
    let having_expr = validate_having(
        select,
        catalog,
        key_column,
        projection.value_column,
        &projection.aggregate_outputs,
        projection.aggregate_filter_expr.as_ref(),
        &projection.aggregate_filter_exprs,
        relation_alias,
    )?;
    let having = having_expr
        .as_ref()
        .and_then(|expr| expr.leaf_predicates().into_iter().next());
    let top_k = validate_aggregate_top_k(
        &query,
        &projection.aggregate_outputs,
        Some(AggregateTopKBindingContext::Single {
            catalog,
            key_column,
            value_column: projection.value_column,
            relation_alias,
            aggregate_filter_expr: projection.aggregate_filter_expr.as_ref(),
            aggregate_filter_exprs: &projection.aggregate_filter_exprs,
        }),
        &[
            projection.output_key_column_id.as_str(),
            key_column.column_id.as_str(),
        ],
        true,
    )?;
    if group_keys.is_empty() && top_k.is_some() {
        return unsupported("global aggregate does not support Top-K clauses");
    }
    Ok(SupportedViewPlan {
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        group_key_column_id: key_column.column_id.clone(),
        aggregate_output_identity: if group_keys.len() == 1
            && group_keys[0].input_column_id.as_deref() == Some(key_column.column_id.as_str())
            && group_keys[0].expression.is_none()
        {
            None
        } else if group_keys.is_empty() {
            Some(SupportedAggregateOutputIdentity::Singleton)
        } else {
            Some(SupportedAggregateOutputIdentity::GroupKey { group_keys })
        },
        output_key_column_id: projection.output_key_column_id,
        sum_value_column_id: projection.value_column.column_id.clone(),
        aggregate_outputs: projection.aggregate_outputs,
        predicate: predicates.first().cloned(),
        predicate_expr,
        aggregate_filter_exprs: projection.aggregate_filter_exprs,
        having,
        having_expr,
        top_k,
    })
}

pub fn validate_supported_tumbling_window_sql(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedTumblingWindowPlan, ViewPlanError> {
    catalog.validate()?;
    let adapter = crate::relation::supported_incremental_adapter_spec(
        &catalog.incremental_adapter.adapter_id,
    )
    .ok_or(RelationSchemaError::InvalidRelationSchema {
        field: "incremental_adapter.adapter_id",
    })?;
    if !matches!(
        adapter,
        SupportedIncrementalAdapterSpec::ScalarSumCount | SupportedIncrementalAdapterSpec::Generic
    ) {
        return unsupported("tumbling window SQL currently supports scalar or generic inputs");
    }
    let key_column = catalog_primary_key_column(catalog)?;
    let query = parse_single_query(sql)?;
    let (select, cte_source) =
        supported_plain_select_allow_identity_cte_and_top_k(&query, catalog)?;
    validate_plain_select_clauses_allow_having(select)?;
    let (event_time_column, from_window, from_source) =
        match validate_event_time_window_from_relation_with_cte(
            select,
            catalog,
            cte_source.as_ref().map(|source| source.alias.as_str()),
        ) {
            Ok((event_time_column, window, relation_alias)) => {
                (event_time_column, Some(window), relation_alias)
            }
            Err(error)
                if error
                    .to_string()
                    .contains("FROM must use TUMBLE/HOP/SESSION") =>
            {
                let from_source = validate_from_relation_after_clause_check_with_cte(
                    select,
                    catalog,
                    cte_source.as_ref().map(|source| source.alias.as_str()),
                )?;
                (
                    declared_tumbling_event_time_column(catalog)?,
                    None,
                    from_source,
                )
            }
            Err(error) => return Err(error),
        };
    let relation_alias = from_source.alias.as_deref();
    let projection = validate_tumbling_projection(
        select,
        catalog,
        key_column,
        event_time_column,
        relation_alias,
    )?;
    let predicate_expr = combine_row_predicate_exprs(
        combine_row_predicate_exprs(
            validate_tumbling_cte_selection(
                cte_source.as_ref(),
                from_source.source_selection.as_ref(),
                catalog,
                key_column,
                projection.value_column,
                event_time_column,
            )?,
            validate_tumbling_selection(
                select,
                catalog,
                key_column,
                projection.value_column,
                event_time_column,
                relation_alias,
            )?,
        ),
        projection.aggregate_filter_expr.clone(),
    );
    let group_by_window = validate_tumbling_group_by(
        select,
        catalog,
        key_column,
        relation_alias,
        &projection.output_key_column_id,
    )?;
    let having_expr = validate_having(
        select,
        catalog,
        key_column,
        projection.value_column,
        &projection.aggregate_outputs,
        projection.aggregate_filter_expr.as_ref(),
        &projection.aggregate_filter_exprs,
        relation_alias,
    )?;
    let top_k = validate_aggregate_top_k(
        &query,
        &projection.aggregate_outputs,
        Some(AggregateTopKBindingContext::Single {
            catalog,
            key_column,
            value_column: projection.value_column,
            relation_alias,
            aggregate_filter_expr: projection.aggregate_filter_expr.as_ref(),
            aggregate_filter_exprs: &projection.aggregate_filter_exprs,
        }),
        &[
            projection.output_key_column_id.as_str(),
            key_column.column_id.as_str(),
        ],
        true,
    )?;
    let window = match (from_window.as_ref(), group_by_window.as_ref()) {
        (Some(from_window), None) => from_window.clone(),
        (None, Some(group_by_window)) => group_by_window.clone(),
        (Some(from_window), Some(group_by_window)) if from_window == group_by_window => {
            from_window.clone()
        }
        (Some(_), Some(_)) => {
            return unsupported("event-time windows in FROM and GROUP BY must match");
        }
        (None, None) => {
            return unsupported(
                "event-time window SQL requires either FROM TUMBLE/HOP/SESSION(...) or GROUP BY TUMBLE/HOP/SESSION(...)",
            );
        }
    };

    Ok(SupportedTumblingWindowPlan {
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        group_key_column_id: key_column.column_id.clone(),
        output_key_column_id: projection.output_key_column_id,
        event_time_column_id: event_time_column.column_id.clone(),
        window_kind: window.kind,
        window_size_ns: window.window_size_ns,
        hop_slide_ns: window.hop_slide_ns,
        session_gap_ns: window.session_gap_ns,
        sum_value_column_id: projection.value_column.column_id.clone(),
        aggregate_outputs: projection.aggregate_outputs,
        predicate_expr,
        aggregate_filter_exprs: projection.aggregate_filter_exprs,
        having_expr,
        top_k,
        window_start_output_column_id: projection.window_start_output_column_id,
        window_end_output_column_id: projection.window_end_output_column_id,
        late_row_policy: None,
        retention_contract: None,
    })
}

/// Validates window SQL and attaches an explicit late-row policy to the
/// compiled plan. The base validator keeps returning `None` so existing
/// callers are byte-stable.
pub fn validate_supported_tumbling_window_sql_with_policy(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    late_row_policy: Option<LateRowPolicy>,
) -> Result<SupportedTumblingWindowPlan, ViewPlanError> {
    if let Some(policy) = &late_row_policy {
        policy.validate()?;
    }
    let mut plan = validate_supported_tumbling_window_sql(sql, catalog)?;
    plan.late_row_policy = late_row_policy;
    Ok(plan)
}

pub fn validate_supported_filter_project_sql(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedFilterProjectPlan, ViewPlanError> {
    catalog.validate()?;
    let adapter = crate::relation::supported_incremental_adapter_spec(
        &catalog.incremental_adapter.adapter_id,
    )
    .ok_or(RelationSchemaError::InvalidRelationSchema {
        field: "incremental_adapter.adapter_id",
    })?;
    if !matches!(
        adapter,
        SupportedIncrementalAdapterSpec::ScalarSumCount | SupportedIncrementalAdapterSpec::Generic
    ) {
        return unsupported("filter/project SQL currently supports scalar or generic inputs");
    }
    let key_column = catalog_primary_key_column(catalog)?;
    let query = parse_single_query(sql)?;
    if matches!(query.body.as_ref(), SetExpr::SetOperation { .. }) {
        return validate_filter_project_set_query(&query, catalog, key_column);
    }
    let cte_source = validate_cte_source(&query, catalog)?;
    validate_query_level_clauses_with_options(&query, cte_source.is_some(), true)?;
    let select = supported_plain_select_body(&query)?;
    validate_plain_select_clauses_allow_qualify(select)?;
    let from_source = validate_from_relation_after_clause_check_with_cte(
        select,
        catalog,
        cte_source.as_ref().map(|source| source.alias.as_str()),
    )?;
    let relation_alias = from_source.alias.as_deref();
    if !group_by_is_empty(&select.group_by) {
        return unsupported("filter/project materialized views must not use GROUP BY");
    }
    if matches!(select.projection.as_slice(), [SelectItem::Wildcard(_)])
        && (cte_source
            .as_ref()
            .and_then(|source| source.projected_column_ids.as_ref())
            .or(from_source.projected_column_ids.as_ref())
            .is_some())
    {
        return unsupported(
            "SELECT * filter/project views over CTE/derived sources require identity SELECT * source",
        );
    }
    let source_projection = cte_source
        .as_ref()
        .and_then(|source| source.projection.as_ref())
        .or(from_source.projection.as_ref());
    let projection = validate_filter_project_projection(
        select,
        catalog,
        key_column,
        relation_alias,
        source_projection,
    )?;
    validate_filter_project_plain_distinct(select, key_column, &projection)?;
    let top_k =
        validate_filter_project_top_k(&query, &projection, catalog, key_column, relation_alias)?;
    if projection.value_columns.is_empty()
        && projection.typed_value_columns.is_empty()
        && projection.output_key_input_column_id.is_none()
        && top_k.is_none()
    {
        return unsupported("filter/project materialized views require at least one value column");
    }
    let predicate_expr = combine_row_predicate_exprs(
        validate_filter_project_cte_selection(
            cte_source.as_ref(),
            from_source.source_selection.as_ref(),
            catalog,
            key_column,
            &projection.value_columns,
        )?,
        validate_filter_project_selection(
            select,
            catalog,
            key_column,
            &projection.value_columns,
            relation_alias,
            source_projection,
        )?,
    );

    Ok(SupportedFilterProjectPlan {
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        key_column_id: key_column.column_id.clone(),
        output_key_column_id: projection.output_key_column_id,
        output_key_input_column_id: projection.output_key_input_column_id,
        value_columns: projection
            .value_columns
            .into_iter()
            .map(|column| SupportedProjectionColumn {
                input_column_id: column.input_column_id,
                output_column_id: column.output_column_id,
                expression: column.expression,
            })
            .collect(),
        typed_value_columns: projection.typed_value_columns,
        predicate_expr,
        top_k,
    })
}

fn validate_filter_project_plain_distinct(
    select: &Select,
    key_column: &RelationColumnV1,
    projection: &ValidatedFilterProjectProjection,
) -> Result<(), ViewPlanError> {
    if !matches!(select.distinct, Some(Distinct::Distinct)) {
        return Ok(());
    }
    if projection.output_key_input_column_id.is_some() {
        if projection.value_columns.is_empty() {
            return Ok(());
        }
        return unsupported(
            "plain SELECT DISTINCT filter/project projected-key views must project only the output primary key",
        );
    }
    for column in &projection.value_columns {
        if column.input_column_id == key_column.column_id
            || column.expression.as_ref().is_some_and(|expression| {
                supported_projection_expr_column_ids(expression)
                    .iter()
                    .any(|column_id| column_id == &key_column.column_id)
            })
        {
            return unsupported(
                "plain SELECT DISTINCT filter/project views must project the primary key exactly once",
            );
        }
    }
    Ok(())
}

fn validate_filter_project_set_query(
    query: &Query,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
) -> Result<SupportedFilterProjectPlan, ViewPlanError> {
    validate_query_level_clauses(query, false)?;
    let SetExpr::SetOperation {
        op,
        set_quantifier,
        left,
        right,
    } = query.body.as_ref()
    else {
        return unsupported("expected a filter/project set operation");
    };
    let is_distinct = matches!(
        set_quantifier,
        SetQuantifier::None | SetQuantifier::Distinct
    );
    if !is_distinct
        || !matches!(
            op,
            SetOperator::Union | SetOperator::Intersect | SetOperator::Except
        )
    {
        return unsupported(
            "filter/project set views currently support UNION DISTINCT, INTERSECT DISTINCT, or same-relation EXCEPT DISTINCT only",
        );
    }
    let mut left = validate_filter_project_set_operand(left, catalog, key_column)?;
    let right = validate_filter_project_set_operand(right, catalog, key_column)?;
    if left.input_relation_id != right.input_relation_id
        || left.key_column_id != right.key_column_id
        || left.output_key_column_id != right.output_key_column_id
        || left.value_columns != right.value_columns
    {
        return unsupported(
            "filter/project set operands must use the same relation and projection",
        );
    }
    left.predicate_expr = match op {
        SetOperator::Union => match (left.predicate_expr, right.predicate_expr) {
            (None, _) | (_, None) => None,
            (Some(left), Some(right)) => Some(RowPredicateExpr::Or {
                left: Box::new(left),
                right: Box::new(right),
            }),
        },
        SetOperator::Intersect => match (left.predicate_expr, right.predicate_expr) {
            (Some(left), Some(right)) => Some(RowPredicateExpr::And {
                left: Box::new(left),
                right: Box::new(right),
            }),
            (None, _) | (_, None) => {
                return unsupported(
                    "filter/project INTERSECT DISTINCT operands must both have filters",
                );
            }
        },
        SetOperator::Except => match (left.predicate_expr, right.predicate_expr) {
            (left, Some(right)) => {
                let not_right = negate_row_predicate_expr(right);
                match left {
                    Some(left) => Some(RowPredicateExpr::And {
                        left: Box::new(left),
                        right: Box::new(not_right),
                    }),
                    None => Some(not_right),
                }
            }
            (_, None) => {
                return unsupported(
                    "filter/project EXCEPT DISTINCT right operand must have a filter",
                );
            }
        },
        _ => unreachable!("unsupported set operator checked above"),
    };
    Ok(left)
}

fn validate_filter_project_set_operand(
    set_expr: &SetExpr,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
) -> Result<SupportedFilterProjectPlan, ViewPlanError> {
    let select = match set_expr {
        SetExpr::Select(select) => select,
        SetExpr::Query(query) => {
            validate_query_level_clauses(query, false)?;
            supported_plain_select_body(query)?
        }
        _ => return unsupported("filter/project set operands must be SELECT queries"),
    };
    validate_plain_select_clauses(select)?;
    let from_source = validate_from_relation_after_clause_check(select, catalog)?;
    let relation_alias = from_source.as_deref();
    if !group_by_is_empty(&select.group_by) {
        return unsupported("filter/project set operands must not use GROUP BY");
    }
    let projection =
        validate_filter_project_projection(select, catalog, key_column, relation_alias, None)?;
    let predicate_expr = validate_filter_project_selection(
        select,
        catalog,
        key_column,
        &projection.value_columns,
        relation_alias,
        None,
    )?;
    Ok(SupportedFilterProjectPlan {
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        key_column_id: key_column.column_id.clone(),
        output_key_column_id: projection.output_key_column_id,
        output_key_input_column_id: projection.output_key_input_column_id,
        value_columns: projection
            .value_columns
            .into_iter()
            .map(|column| SupportedProjectionColumn {
                input_column_id: column.input_column_id,
                output_column_id: column.output_column_id,
                expression: column.expression,
            })
            .collect(),
        typed_value_columns: Vec::new(),
        predicate_expr,
        top_k: None,
    })
}

fn validate_filter_project_cte_selection(
    cte_source: Option<&CteSource>,
    derived_source_selection: Option<&Expr>,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_columns: &[ValidatedProjectionColumn],
) -> Result<Option<RowPredicateExpr>, ViewPlanError> {
    let cte_predicate = match cte_source.and_then(|source| source.selection.as_ref()) {
        Some(selection) => validate_filter_project_predicate_expr(
            selection,
            catalog,
            key_column,
            value_columns,
            None,
            None,
        )
        .map(Some)?,
        None => None,
    };
    let derived_predicate = match derived_source_selection {
        Some(selection) => validate_filter_project_predicate_expr(
            selection,
            catalog,
            key_column,
            value_columns,
            None,
            None,
        )
        .map(Some)?,
        None => None,
    };
    Ok(combine_row_predicate_exprs(
        cte_predicate,
        derived_predicate,
    ))
}

pub fn validate_catalog_backed_sum_count_view_sql(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedViewPlan, ViewPlanError> {
    validate_supported_view_sql(sql, catalog)
}

pub fn validate_supported_latest_by_key_sql(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedLatestByKeyPlan, ViewPlanError> {
    catalog.validate()?;
    let adapter = crate::relation::supported_incremental_adapter_spec(
        &catalog.incremental_adapter.adapter_id,
    )
    .ok_or(RelationSchemaError::InvalidRelationSchema {
        field: "incremental_adapter.adapter_id",
    })?;
    if !matches!(
        adapter,
        SupportedIncrementalAdapterSpec::ScalarSumCount | SupportedIncrementalAdapterSpec::Generic
    ) {
        return unsupported("latest-by-key SQL currently supports scalar or generic inputs");
    }
    let key_column = catalog_primary_key_column(catalog)?;
    let query = parse_single_query(sql)?;
    let cte_source = validate_cte_source(&query, catalog)?;
    validate_query_level_clauses_with_options(&query, cte_source.is_some(), true)?;
    let select = supported_plain_select_body(&query)?;
    validate_plain_select_clauses(select)?;
    let from_source = validate_from_relation_after_clause_check_with_cte(
        select,
        catalog,
        cte_source.as_ref().map(|source| source.alias.as_str()),
    )?;
    let relation_alias = from_source.alias.as_deref();
    let latest = validate_latest_by_key_projection(select, catalog, key_column, relation_alias)?;
    let top_k = validate_latest_top_k(&query, catalog, &latest, relation_alias)?;
    let predicate_expr = combine_row_predicate_exprs(
        combine_row_predicate_exprs(
            validate_latest_cte_selection(
                cte_source.as_ref(),
                from_source.source_selection.as_ref(),
                catalog,
                key_column,
                latest.value_column,
                latest.ordering_column,
            )?,
            validate_latest_selection(
                select,
                catalog,
                key_column,
                latest.value_column,
                latest.ordering_column,
                relation_alias,
            )?,
        ),
        latest.aggregate_filter_expr.clone(),
    );
    validate_group_by_key(
        select,
        catalog,
        key_column,
        relation_alias,
        &latest.output_key_column_id,
        None,
    )?;

    Ok(SupportedLatestByKeyPlan {
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        key_column_id: key_column.column_id.clone(),
        output_key_column_id: latest.output_key_column_id,
        value_column_id: latest.value_column.column_id.clone(),
        ordering_column_id: latest.ordering_column.column_id.clone(),
        output_value_column_id: latest.output_value_column_id,
        function: latest.function,
        predicate_expr,
        top_k,
    })
}

pub fn validate_supported_analytic_row_number_sql(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedAnalyticRowNumberPlan, ViewPlanError> {
    catalog.validate()?;
    let adapter = crate::relation::supported_incremental_adapter_spec(
        &catalog.incremental_adapter.adapter_id,
    )
    .ok_or(RelationSchemaError::InvalidRelationSchema {
        field: "incremental_adapter.adapter_id",
    })?;
    if !matches!(
        adapter,
        SupportedIncrementalAdapterSpec::ScalarSumCount | SupportedIncrementalAdapterSpec::Generic
    ) {
        return unsupported("ROW_NUMBER SQL currently supports scalar or generic inputs");
    }
    let key_column = catalog_primary_key_column(catalog)?;
    let query = parse_single_query(sql)?;
    let wrapped_top_n = validate_row_number_top_n_wrapper(&query)?;
    let query = wrapped_top_n
        .as_ref()
        .map(|top_n| top_n.inner_query)
        .unwrap_or(query.as_ref());
    let cte_source = validate_cte_source(query, catalog)?;
    validate_query_level_clauses_with_options(query, cte_source.is_some(), false)?;
    if query.order_by.is_some() {
        return unsupported("ROW_NUMBER materialized views do not support query ORDER BY");
    }
    let select = supported_plain_select_body(query)?;
    validate_plain_select_clauses_allow_qualify(select)?;
    let from_source = validate_from_relation_after_clause_check_with_cte(
        select,
        catalog,
        cte_source.as_ref().map(|source| source.alias.as_str()),
    )?;
    let relation_alias = from_source.alias.as_deref();
    if !group_by_is_empty(&select.group_by) {
        return unsupported("ROW_NUMBER materialized views must not use GROUP BY");
    }
    let row_number = validate_row_number_projection(select, catalog, key_column, relation_alias)?;
    if let Some(top_n) = &wrapped_top_n {
        top_n.validate_projection(
            row_number.output_key_column_id.as_str(),
            row_number.output_row_number_column_id.as_str(),
        )?;
    }
    let source_predicate_expr = validate_latest_cte_selection(
        cte_source.as_ref(),
        from_source.source_selection.as_ref(),
        catalog,
        key_column,
        row_number.partition_column,
        row_number.order_column,
    )?;
    let predicate_expr = combine_row_predicate_exprs(
        source_predicate_expr,
        validate_row_number_selection(
            select,
            catalog,
            key_column,
            row_number.partition_column,
            row_number.order_column,
            relation_alias,
        )?,
    );
    let rank_limit =
        validate_row_number_qualify(select, row_number.output_row_number_column_id.as_str())?
            .or_else(|| wrapped_top_n.as_ref().map(|top_n| top_n.rank_limit));
    if row_number.function == SupportedAnalyticWindowFunction::RowNumber
        && row_number.implicit_primary_key_tie_breaker
        && rank_limit.is_none()
    {
        return unsupported(
            "ROW_NUMBER implicit primary key tie-breaker requires rank <= <positive integer> or rank = 1",
        );
    }

    Ok(SupportedAnalyticRowNumberPlan {
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        key_column_id: key_column.column_id.clone(),
        output_key_column_id: row_number.output_key_column_id,
        function: row_number.function,
        partition_column_id: row_number.partition_column.column_id.clone(),
        order_column_id: row_number.order_column.column_id.clone(),
        order_descending: row_number.order_descending,
        output_row_number_column_id: row_number.output_row_number_column_id,
        predicate_expr,
        rank_limit,
    })
}

struct RowNumberTopNWrapper<'a> {
    inner_query: &'a Query,
    output_key_column_id: String,
    output_row_number_column_id: String,
    rank_limit: usize,
}

impl RowNumberTopNWrapper<'_> {
    fn validate_projection(
        &self,
        output_key_column_id: &str,
        output_row_number_column_id: &str,
    ) -> Result<(), ViewPlanError> {
        if !identifier_eq(self.output_key_column_id.as_str(), output_key_column_id)
            || !identifier_eq(
                self.output_row_number_column_id.as_str(),
                output_row_number_column_id,
            )
        {
            return unsupported(
                "ROW_NUMBER Top-N wrapper projection must match the inner key and rank outputs",
            );
        }
        Ok(())
    }
}

fn validate_row_number_top_n_wrapper(
    query: &Query,
) -> Result<Option<RowNumberTopNWrapper<'_>>, ViewPlanError> {
    let select = supported_plain_select_body(query)?;
    let [table] = select.from.as_slice() else {
        return Ok(None);
    };
    let TableFactor::Derived {
        lateral,
        subquery,
        alias,
        sample,
    } = &table.relation
    else {
        return Ok(None);
    };
    if select_projection_contains_row_number_function(&select.projection) {
        return Ok(None);
    }
    let inner_select = supported_plain_select_body(subquery)?;
    if !select_projection_contains_row_number_function(&inner_select.projection) {
        return Ok(None);
    }
    if *lateral
        || sample.is_some()
        || alias
            .as_ref()
            .is_some_and(|alias| !alias.columns.is_empty())
    {
        return unsupported(
            "ROW_NUMBER Top-N wrapper must use one unqualified derived row-number subquery",
        );
    }
    validate_query_level_clauses(query, false)?;
    validate_plain_select_clauses(select)?;
    if !table.joins.is_empty() || !group_by_is_empty(&select.group_by) {
        return unsupported(
            "ROW_NUMBER Top-N wrapper must only filter the derived row-number output",
        );
    }
    let [key, rank] = select.projection.as_slice() else {
        return unsupported("ROW_NUMBER Top-N wrapper projection must be key, rank");
    };
    let output_key_column_id = row_number_top_n_projection_identifier(key)?;
    let output_row_number_column_id = row_number_top_n_projection_identifier(rank)?;
    let Some(selection) = &select.selection else {
        return unsupported("ROW_NUMBER Top-N wrapper requires WHERE rank <= <positive integer>");
    };
    let rank_limit =
        validate_row_number_top_n_selection(selection, output_row_number_column_id.as_str())?;
    Ok(Some(RowNumberTopNWrapper {
        inner_query: subquery.as_ref(),
        output_key_column_id,
        output_row_number_column_id,
        rank_limit,
    }))
}

fn row_number_top_n_projection_identifier(item: &SelectItem) -> Result<String, ViewPlanError> {
    let SelectItem::UnnamedExpr(expr) = item else {
        return unsupported("ROW_NUMBER Top-N wrapper projection must be unaliased key, rank");
    };
    expression_identifier(expr)
        .map(str::to_string)
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "ROW_NUMBER Top-N wrapper projection must be unaliased key, rank".to_string(),
        })
}

fn validate_row_number_top_n_selection(
    selection: &Expr,
    output_row_number_column_id: &str,
) -> Result<usize, ViewPlanError> {
    let Expr::BinaryOp { left, op, right } = selection else {
        return unsupported(
            "ROW_NUMBER Top-N wrapper WHERE must be rank <= <positive integer> or rank = 1",
        );
    };
    if !expression_identifier(left)
        .is_some_and(|identifier| identifier_eq(identifier, output_row_number_column_id))
    {
        return unsupported(
            "ROW_NUMBER Top-N wrapper WHERE must be rank <= <positive integer> or rank = 1",
        );
    }
    validate_row_number_rank_limit_predicate(op, right)
}

fn select_projection_contains_row_number_function(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return false,
        };
        matches!(expr, Expr::Function(function) if function_name_eq(&function.name, "row_number"))
    })
}

fn validate_latest_cte_selection(
    cte_source: Option<&CteSource>,
    derived_source_selection: Option<&Expr>,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
    ordering_column: &RelationColumnV1,
) -> Result<Option<RowPredicateExpr>, ViewPlanError> {
    let cte_predicate = match cte_source.and_then(|source| source.selection.as_ref()) {
        Some(selection) => validate_latest_predicate_expr(
            selection,
            catalog,
            key_column,
            value_column,
            ordering_column,
            None,
        )
        .map(Some)?,
        None => None,
    };
    let derived_predicate = match derived_source_selection {
        Some(selection) => validate_latest_predicate_expr(
            selection,
            catalog,
            key_column,
            value_column,
            ordering_column,
            None,
        )
        .map(Some)?,
        None => None,
    };
    Ok(combine_row_predicate_exprs(
        cte_predicate,
        derived_predicate,
    ))
}

fn finalize_logical_plan(
    mut plan: VelorixLogicalViewPlanV1,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    plan.plan_hash = None;
    plan.operator_dag_contract = derive_operator_dag_contract(&plan)?;
    plan.execution_implementation = Some(derive_execution_implementation(&plan)?);
    plan.plan_hash = Some(logical_view_plan_hash(&plan)?);
    validate_logical_view_plan(&plan)?;
    Ok(plan)
}

fn derive_execution_implementation(
    plan: &VelorixLogicalViewPlanV1,
) -> Result<LogicalPlanExecutionImplementationV1, ViewPlanError> {
    let implementation_id = match &plan.execution {
        VelorixLogicalViewExecutionV1::SingleKeySumCount { .. } => {
            "velorix-single-key-aggregate-specialization-v1"
        }
        VelorixLogicalViewExecutionV1::FilterProject { .. } => {
            "velorix-filter-project-specialization-v1"
        }
        VelorixLogicalViewExecutionV1::LatestByKey { .. } => {
            "velorix-latest-by-key-specialization-v1"
        }
        VelorixLogicalViewExecutionV1::AnalyticRowNumber { .. } => {
            "velorix-row-number-specialization-v1"
        }
        VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { .. } => {
            "velorix-tumbling-window-specialization-v1"
        }
        VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan } => {
            if supported_join_view_plan_is_self_join(plan) {
                "velorix-self-join-global-count-specialization-v1"
            } else if plan.join_kind == SupportedJoinKind::Full {
                "velorix-full-join-specialization-v1"
            } else if plan.join_kind == SupportedJoinKind::Left {
                "velorix-narrow-left-join-specialization-v1"
            } else if join_plan_requires_general_aggregate_specialization(plan) {
                "velorix-general-aggregate-join-specialization-v1"
            } else {
                "velorix-keyed-aggregate-join-specialization-v1"
            }
        }
        VelorixLogicalViewExecutionV1::ThreeInputInnerJoinCount { .. } => {
            "velorix-native-three-input-inner-join-dag-v1"
        }
        VelorixLogicalViewExecutionV1::TwoInputSemiAntiJoinProject { plan } => match plan.join_kind
        {
            SupportedSemiAntiJoinKindV1::Semi => "velorix-native-semi-join-project-dag-v1",
            SupportedSemiAntiJoinKindV1::Anti => "velorix-native-anti-join-project-dag-v1",
        },
        VelorixLogicalViewExecutionV1::ScalarAggregateFilter { .. } => {
            "velorix-scalar-aggregate-filter-specialization-v1"
        }
        VelorixLogicalViewExecutionV1::AnalyticWindowFrames { .. } => {
            "velorix-analytic-window-frames-specialization-v1"
        }
        VelorixLogicalViewExecutionV1::IntervalJoin { .. } => {
            "velorix-interval-join-specialization-v1"
        }
        VelorixLogicalViewExecutionV1::RecursiveFixpointV1 { .. } => {
            "velorix-recursive-fixpoint-specialization-v1"
        }
        VelorixLogicalViewExecutionV1::CrossJoin { .. } => "velorix-cross-join-specialization-v1",
    };
    let physical_bytes = serde_json::to_vec(&(
        &plan.nodes,
        &plan.operator_dag_contract,
        &plan.state_requirements,
        &plan.output_codec_version,
        &plan.execution,
        OUTPUT_PUBLICATION_PROTOCOL_VERSION_V1,
    ))
    .map_err(|source| ViewPlanError::InvalidLogicalPlan {
        reason: format!("could not serialize physical operator DAG: {source}"),
    })?;
    Ok(LogicalPlanExecutionImplementationV1 {
        contract_version: EXECUTION_IMPLEMENTATION_CONTRACT_VERSION_V2,
        implementation_id: implementation_id.to_string(),
        implementation_version: 1,
        state_codec_id: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
        join_key_codec_id: match &plan.execution {
            VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan } => {
                supported_join_key_codec_id(plan).map(str::to_string)
            }
            VelorixLogicalViewExecutionV1::ThreeInputInnerJoinCount { plan } => {
                Some(plan.join_key_codec_id.clone())
            }
            VelorixLogicalViewExecutionV1::TwoInputSemiAntiJoinProject { .. } => {
                Some(SCALAR_PK_JSON_JOIN_KEY_CODEC_V1.to_string())
            }
            _ => None,
        },
        input_fanout_protocol_id: match &plan.execution {
            VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan }
                if supported_join_view_plan_is_self_join(plan) =>
            {
                Some(SELF_JOIN_ATOMIC_FANOUT_PROTOCOL_V1.to_string())
            }
            _ => None,
        },
        checkpoint_manifest_version: CHECKPOINT_MANIFEST_VERSION_V1,
        output_codec_id: plan.output_codec_version.clone(),
        output_publication_protocol_id: OUTPUT_PUBLICATION_PROTOCOL_VERSION_V1.to_string(),
        physical_operator_dag_hash: format!(
            "velorix-physical-operator-dag-sha256-v1:{}",
            stable_bytes_hash(&physical_bytes)
        ),
    })
}

fn join_plan_requires_general_aggregate_specialization(plan: &SupportedJoinViewPlan) -> bool {
    !plan.aggregate_filter_exprs.is_empty()
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

fn empty_operator_dag_contract() -> OperatorDagContractV1 {
    OperatorDagContractV1 {
        contract_version: OPERATOR_DAG_CONTRACT_VERSION_V1.to_string(),
        operators: Vec::new(),
        edges: Vec::new(),
    }
}

fn derive_operator_dag_contract(
    plan: &VelorixLogicalViewPlanV1,
) -> Result<OperatorDagContractV1, ViewPlanError> {
    let referenced_columns = referenced_columns_by_relation(&plan.nodes);
    let window_watermarks = window_watermarks_by_relation(&plan.nodes);
    let mut outputs = BTreeMap::<String, OutputPortContractV1>::new();
    let mut operators = Vec::with_capacity(plan.nodes.len());
    let mut edges = Vec::new();

    for node in &plan.nodes {
        let node_id = logical_node_id(node).to_string();
        if outputs.contains_key(&node_id) {
            return invalid_logical_plan("logical operator node ids must be unique");
        }
        let input_bindings = logical_node_inputs(node);
        let mut inputs = Vec::with_capacity(input_bindings.len());
        let mut upstream = Vec::with_capacity(input_bindings.len());
        for (port_id, producer_id) in input_bindings {
            let producer = outputs.get(producer_id).ok_or_else(|| {
                ViewPlanError::InvalidLogicalPlan {
                    reason: format!(
                        "operator {node_id} input {port_id} references a missing or non-prior node {producer_id}"
                    ),
                }
            })?;
            let requirement = input_requirement(node, port_id);
            inputs.push(requirement);
            upstream.push(producer.clone());
            edges.push(OperatorEdgeV1 {
                from: OutputPortRefV1 {
                    node_id: producer_id.to_string(),
                    port_id: "output".to_string(),
                },
                to: InputPortRefV1 {
                    node_id: node_id.clone(),
                    port_id: port_id.to_string(),
                },
            });
        }

        let output = derive_node_output(
            node,
            &upstream,
            plan,
            &referenced_columns,
            &window_watermarks,
        )?;
        let state = derive_node_state(node);
        operators.push(OperatorContractV1 {
            node_id: node_id.clone(),
            operator: OperatorKindIdentityV1 {
                kind: logical_node_kind(node).to_string(),
                version: 1,
            },
            inputs,
            outputs: vec![output.clone()],
            state,
        });
        outputs.insert(node_id, output);
    }

    validate_all_nodes_reach_output(&plan.nodes, &edges)?;
    let contract = OperatorDagContractV1 {
        contract_version: OPERATOR_DAG_CONTRACT_VERSION_V1.to_string(),
        operators,
        edges,
    };
    contract
        .validate()
        .map_err(|error| ViewPlanError::InvalidLogicalPlan {
            reason: format!("invalid derived operator DAG contract: {error}"),
        })?;
    Ok(contract)
}

fn logical_node_id(node: &VelorixLogicalViewPlanNodeV1) -> &str {
    match node {
        VelorixLogicalViewPlanNodeV1::RelationScan { node_id, .. }
        | VelorixLogicalViewPlanNodeV1::Filter { node_id, .. }
        | VelorixLogicalViewPlanNodeV1::Project { node_id, .. }
        | VelorixLogicalViewPlanNodeV1::Aggregate { node_id, .. }
        | VelorixLogicalViewPlanNodeV1::TopK { node_id, .. }
        | VelorixLogicalViewPlanNodeV1::InnerEquiJoin { node_id, .. }
        | VelorixLogicalViewPlanNodeV1::LeftEquiJoin { node_id, .. }
        | VelorixLogicalViewPlanNodeV1::SemiEquiJoin { node_id, .. }
        | VelorixLogicalViewPlanNodeV1::AntiEquiJoin { node_id, .. }
        | VelorixLogicalViewPlanNodeV1::FullEquiJoin { node_id, .. }
        | VelorixLogicalViewPlanNodeV1::TumblingWindow { node_id, .. }
        | VelorixLogicalViewPlanNodeV1::LatestByKey { node_id, .. }
        | VelorixLogicalViewPlanNodeV1::RowNumber { node_id, .. }
        | VelorixLogicalViewPlanNodeV1::Output { node_id, .. } => node_id,
    }
}

fn logical_node_kind(node: &VelorixLogicalViewPlanNodeV1) -> &'static str {
    match node {
        VelorixLogicalViewPlanNodeV1::RelationScan { .. } => "relation_scan",
        VelorixLogicalViewPlanNodeV1::Filter { .. } => "filter",
        VelorixLogicalViewPlanNodeV1::Project { .. } => "project",
        VelorixLogicalViewPlanNodeV1::Aggregate { .. } => "aggregate",
        VelorixLogicalViewPlanNodeV1::TopK { .. } => "top_k",
        VelorixLogicalViewPlanNodeV1::InnerEquiJoin { .. } => "inner_equi_join",
        VelorixLogicalViewPlanNodeV1::LeftEquiJoin { .. } => "left_equi_join",
        VelorixLogicalViewPlanNodeV1::SemiEquiJoin { .. } => "semi_equi_join",
        VelorixLogicalViewPlanNodeV1::AntiEquiJoin { .. } => "anti_equi_join",
        VelorixLogicalViewPlanNodeV1::FullEquiJoin { .. } => "full_equi_join",
        VelorixLogicalViewPlanNodeV1::TumblingWindow { .. } => "tumbling_window",
        VelorixLogicalViewPlanNodeV1::LatestByKey { .. } => "latest_by_key",
        VelorixLogicalViewPlanNodeV1::RowNumber { .. } => "row_number",
        VelorixLogicalViewPlanNodeV1::Output { .. } => "output",
    }
}

fn logical_node_inputs(node: &VelorixLogicalViewPlanNodeV1) -> Vec<(&'static str, &str)> {
    match node {
        VelorixLogicalViewPlanNodeV1::RelationScan { .. } => Vec::new(),
        VelorixLogicalViewPlanNodeV1::Filter { input, .. }
        | VelorixLogicalViewPlanNodeV1::Project { input, .. }
        | VelorixLogicalViewPlanNodeV1::Aggregate { input, .. }
        | VelorixLogicalViewPlanNodeV1::TopK { input, .. }
        | VelorixLogicalViewPlanNodeV1::TumblingWindow { input, .. }
        | VelorixLogicalViewPlanNodeV1::LatestByKey { input, .. }
        | VelorixLogicalViewPlanNodeV1::RowNumber { input, .. }
        | VelorixLogicalViewPlanNodeV1::Output { input, .. } => vec![("input", input)],
        VelorixLogicalViewPlanNodeV1::InnerEquiJoin { left, right, .. }
        | VelorixLogicalViewPlanNodeV1::LeftEquiJoin { left, right, .. }
        | VelorixLogicalViewPlanNodeV1::SemiEquiJoin { left, right, .. }
        | VelorixLogicalViewPlanNodeV1::AntiEquiJoin { left, right, .. }
        | VelorixLogicalViewPlanNodeV1::FullEquiJoin { left, right, .. } => {
            vec![("left", left), ("right", right)]
        }
    }
}

fn input_requirement(node: &VelorixLogicalViewPlanNodeV1, port_id: &str) -> InputPortContractV1 {
    let required_columns = required_columns_for_node(node, port_id)
        .into_iter()
        .map(|column| crate::operator_contract::RequiredColumnV1 {
            column_id: global_column_id(&column),
            nullability: NullabilityV1::Nullable,
        })
        .collect();
    let watermark = match node {
        VelorixLogicalViewPlanNodeV1::TumblingWindow {
            event_time_column, ..
        } => WatermarkRequirementV1::Monotonic {
            event_time_column_id: global_column_id(event_time_column),
        },
        _ => WatermarkRequirementV1::None,
    };
    InputPortContractV1 {
        port_id: port_id.to_string(),
        accepted_changelog: AcceptedChangelogV1::GeneralRetract,
        required_columns,
        required_keys: Vec::new(),
        required_determinism: DeterminismRequirementV1::ReplayDeterministic,
        required_progress: ProgressRequirementV1 {
            processing: ProcessingFrontierRequirementV1::PerInputCheckpointed,
            watermark,
        },
    }
}

fn required_columns_for_node(
    node: &VelorixLogicalViewPlanNodeV1,
    port_id: &str,
) -> Vec<LogicalPlanColumnRef> {
    match node {
        VelorixLogicalViewPlanNodeV1::Filter { predicate, .. } => {
            vec![predicate.column.clone()]
        }
        VelorixLogicalViewPlanNodeV1::Project {
            columns,
            computed_columns,
            ..
        } => {
            let mut required = columns.clone();
            for computed in computed_columns {
                required.extend(
                    supported_projection_expr_column_ids(&computed.expression)
                        .into_iter()
                        .map(|column_id| {
                            column_ref(computed.input_relation_id.as_str(), column_id.as_str())
                        }),
                );
            }
            required
        }
        VelorixLogicalViewPlanNodeV1::Aggregate {
            group_keys,
            accumulators,
            ..
        } => {
            let mut columns = group_keys.clone();
            columns.extend(
                accumulators
                    .iter()
                    .filter_map(|accumulator| accumulator.input.clone()),
            );
            columns
        }
        VelorixLogicalViewPlanNodeV1::TopK { order_by, .. } => vec![order_by.clone()],
        VelorixLogicalViewPlanNodeV1::InnerEquiJoin {
            left_key,
            right_key,
            composite_equality,
            ..
        }
        | VelorixLogicalViewPlanNodeV1::LeftEquiJoin {
            left_key,
            right_key,
            composite_equality,
            ..
        }
        | VelorixLogicalViewPlanNodeV1::FullEquiJoin {
            left_key,
            right_key,
            composite_equality,
            ..
        } => {
            let mut columns = if port_id == "left" {
                vec![left_key.clone()]
            } else {
                vec![right_key.clone()]
            };
            if let Some(composite) = composite_equality {
                columns.extend(composite.additional_pairs.iter().map(|pair| {
                    if port_id == "left" {
                        pair.left_key.clone()
                    } else {
                        pair.right_key.clone()
                    }
                }));
            }
            columns
        }
        VelorixLogicalViewPlanNodeV1::SemiEquiJoin {
            left_key,
            right_key,
            ..
        }
        | VelorixLogicalViewPlanNodeV1::AntiEquiJoin {
            left_key,
            right_key,
            ..
        } => {
            if port_id == "left" {
                vec![left_key.clone()]
            } else {
                vec![right_key.clone()]
            }
        }
        VelorixLogicalViewPlanNodeV1::TumblingWindow {
            event_time_column, ..
        } => vec![event_time_column.clone()],
        VelorixLogicalViewPlanNodeV1::LatestByKey {
            key_columns,
            ordering_column,
            ..
        } => {
            let mut columns = key_columns.clone();
            columns.push(ordering_column.clone());
            columns
        }
        VelorixLogicalViewPlanNodeV1::RowNumber {
            partition_column,
            order_column,
            ..
        } => vec![partition_column.clone(), order_column.clone()],
        VelorixLogicalViewPlanNodeV1::RelationScan { .. }
        | VelorixLogicalViewPlanNodeV1::Output { .. } => Vec::new(),
    }
}

fn derive_node_output(
    node: &VelorixLogicalViewPlanNodeV1,
    upstream: &[OutputPortContractV1],
    plan: &VelorixLogicalViewPlanV1,
    referenced_columns: &BTreeMap<String, BTreeSet<String>>,
    window_watermarks: &BTreeMap<String, BTreeSet<String>>,
) -> Result<OutputPortContractV1, ViewPlanError> {
    let mut schema = match node {
        VelorixLogicalViewPlanNodeV1::RelationScan { relation, .. } => {
            let mut columns = referenced_columns
                .get(&relation.relation_id)
                .into_iter()
                .flatten()
                .map(|column_id| PortColumnV1 {
                    column_id: format!("{}.{}", relation.relation_id, column_id),
                    logical_type: format!("schema-bound:{}", relation.schema_fingerprint),
                    nullability: NullabilityV1::Nullable,
                })
                .collect::<Vec<_>>();
            if columns.is_empty() {
                columns.push(PortColumnV1 {
                    column_id: format!("{}.__row_presence_v1", relation.relation_id),
                    logical_type: "logical-row-presence-v1".to_string(),
                    nullability: NullabilityV1::NonNull,
                });
            }
            RowSchemaV1 { columns }
        }
        VelorixLogicalViewPlanNodeV1::Project {
            columns,
            computed_columns,
            ..
        } => {
            let mut projected = columns.iter().map(derived_port_column).collect::<Vec<_>>();
            projected.extend(computed_columns.iter().map(|computed| PortColumnV1 {
                column_id: global_column_id(&computed.output),
                logical_type: "logical-scalar-int64-v1".to_string(),
                nullability: NullabilityV1::Nullable,
            }));
            RowSchemaV1 { columns: projected }
        }
        VelorixLogicalViewPlanNodeV1::Aggregate {
            group_keys,
            accumulators,
            ..
        } => {
            let mut columns = group_keys
                .iter()
                .map(derived_port_column)
                .collect::<Vec<_>>();
            columns.extend(accumulators.iter().map(|accumulator| PortColumnV1 {
                column_id: format!(
                    "{}.{}",
                    plan.output_relation.relation_id, accumulator.output_column_id
                ),
                logical_type: "logical-aggregate-output-v1".to_string(),
                nullability: if matches!(
                    accumulator.function,
                    LogicalPlanAggregateFunctionV1::Count
                        | LogicalPlanAggregateFunctionV1::CountDistinct
                ) {
                    NullabilityV1::NonNull
                } else {
                    NullabilityV1::Nullable
                },
            }));
            RowSchemaV1 { columns }
        }
        VelorixLogicalViewPlanNodeV1::InnerEquiJoin { .. } => RowSchemaV1 {
            columns: upstream
                .iter()
                .flat_map(|output| output.schema.columns.clone())
                .collect(),
        },
        VelorixLogicalViewPlanNodeV1::SemiEquiJoin { .. }
        | VelorixLogicalViewPlanNodeV1::AntiEquiJoin { .. } => RowSchemaV1 {
            columns: upstream
                .first()
                .map(|output| output.schema.columns.clone())
                .unwrap_or_default(),
        },
        VelorixLogicalViewPlanNodeV1::LeftEquiJoin { .. } => RowSchemaV1 {
            columns: upstream
                .iter()
                .enumerate()
                .flat_map(|(input_index, output)| {
                    output
                        .schema
                        .columns
                        .iter()
                        .cloned()
                        .map(move |mut column| {
                            if input_index == 1 {
                                column.nullability = NullabilityV1::Nullable;
                            }
                            column
                        })
                })
                .collect(),
        },
        VelorixLogicalViewPlanNodeV1::FullEquiJoin {
            left_key,
            output_key,
            ..
        } => {
            let logical_type = upstream
                .first()
                .and_then(|output| {
                    output
                        .schema
                        .columns
                        .iter()
                        .find(|column| column.column_id == global_column_id(left_key))
                })
                .map(|column| column.logical_type.clone())
                .unwrap_or_else(|| "logical-coalesced-join-key-v1".to_string());
            let mut columns = upstream
                .iter()
                .flat_map(|output| output.schema.columns.iter().cloned())
                .map(|mut column| {
                    column.nullability = NullabilityV1::Nullable;
                    column
                })
                .collect::<Vec<_>>();
            columns.push(PortColumnV1 {
                column_id: global_column_id(output_key),
                logical_type,
                nullability: NullabilityV1::NonNull,
            });
            RowSchemaV1 { columns }
        }
        _ => upstream
            .first()
            .map(|output| output.schema.clone())
            .ok_or_else(|| ViewPlanError::InvalidLogicalPlan {
                reason: format!("operator {} requires an input", logical_node_id(node)),
            })?,
    };
    augment_execution_output_aliases(node, plan, &mut schema);
    canonicalize_port_columns(&mut schema.columns);
    if schema.columns.is_empty() {
        return invalid_logical_plan(format!(
            "operator {} produced an empty capability schema",
            logical_node_id(node)
        ));
    }

    let candidate_keys = match node {
        VelorixLogicalViewPlanNodeV1::Filter { .. }
        | VelorixLogicalViewPlanNodeV1::TopK { .. }
        | VelorixLogicalViewPlanNodeV1::SemiEquiJoin { .. }
        | VelorixLogicalViewPlanNodeV1::AntiEquiJoin { .. }
        | VelorixLogicalViewPlanNodeV1::Output { .. } => upstream
            .first()
            .map(|output| output.candidate_keys.clone())
            .unwrap_or_default(),
        VelorixLogicalViewPlanNodeV1::Project { .. } => upstream
            .first()
            .map(|output| {
                output
                    .candidate_keys
                    .iter()
                    .filter(|key| {
                        key.columns.iter().all(|column| {
                            schema
                                .columns
                                .iter()
                                .any(|candidate| &candidate.column_id == column)
                        })
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default(),
        VelorixLogicalViewPlanNodeV1::Aggregate { group_keys, .. } => {
            if group_keys.is_empty() {
                Vec::new()
            } else {
                vec![CandidateKeyV1 {
                    columns: group_keys.iter().map(global_column_id).collect(),
                    equality: KeyEqualityV1::SqlNotDistinct,
                }]
            }
        }
        VelorixLogicalViewPlanNodeV1::LatestByKey { key_columns, .. } => {
            vec![CandidateKeyV1 {
                columns: key_columns.iter().map(global_column_id).collect(),
                equality: KeyEqualityV1::SqlNotDistinct,
            }]
        }
        _ => Vec::new(),
    };
    let uniqueness = if matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Aggregate { group_keys, .. } if group_keys.is_empty()
    ) || (matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Filter { .. }
            | VelorixLogicalViewPlanNodeV1::Project { .. }
            | VelorixLogicalViewPlanNodeV1::TopK { .. }
            | VelorixLogicalViewPlanNodeV1::Output { .. }
    ) && upstream
        .first()
        .is_some_and(|output| output.uniqueness == UniquenessGuaranteeV1::Singleton))
    {
        UniquenessGuaranteeV1::Singleton
    } else if candidate_keys.is_empty() {
        UniquenessGuaranteeV1::NotGuaranteed
    } else {
        UniquenessGuaranteeV1::CandidateKeys
    };
    let watermark = match node {
        VelorixLogicalViewPlanNodeV1::RelationScan { relation, .. } => window_watermarks
            .get(&relation.relation_id)
            .and_then(|columns| columns.iter().next())
            .map(|column_id| WatermarkGuaranteeV1::Monotonic {
                event_time_column_id: format!("{}.{}", relation.relation_id, column_id),
            })
            .unwrap_or(WatermarkGuaranteeV1::None),
        VelorixLogicalViewPlanNodeV1::Filter { .. }
        | VelorixLogicalViewPlanNodeV1::Project { .. }
        | VelorixLogicalViewPlanNodeV1::TumblingWindow { .. }
        | VelorixLogicalViewPlanNodeV1::LatestByKey { .. }
        | VelorixLogicalViewPlanNodeV1::RowNumber { .. }
        | VelorixLogicalViewPlanNodeV1::TopK { .. }
        | VelorixLogicalViewPlanNodeV1::SemiEquiJoin { .. }
        | VelorixLogicalViewPlanNodeV1::AntiEquiJoin { .. }
        | VelorixLogicalViewPlanNodeV1::Output { .. } => upstream
            .first()
            .map(|output| output.progress.watermark.clone())
            .unwrap_or(WatermarkGuaranteeV1::None),
        _ => WatermarkGuaranteeV1::None,
    };
    Ok(OutputPortContractV1 {
        port_id: "output".to_string(),
        schema,
        changelog: ChangelogModeV1::GeneralRetract,
        candidate_keys,
        uniqueness,
        determinism: DeterminismGuaranteeV1::ReplayDeterministic,
        progress: ProgressGuaranteeV1 {
            processing: ProcessingFrontierGuaranteeV1::PerInputCheckpointed,
            watermark,
        },
    })
}

fn derive_node_state(node: &VelorixLogicalViewPlanNodeV1) -> Option<StateContractV1> {
    let boundedness = match node {
        VelorixLogicalViewPlanNodeV1::Project { .. }
        | VelorixLogicalViewPlanNodeV1::Aggregate { .. }
        | VelorixLogicalViewPlanNodeV1::TopK { .. }
        | VelorixLogicalViewPlanNodeV1::InnerEquiJoin { .. }
        | VelorixLogicalViewPlanNodeV1::LeftEquiJoin { .. }
        | VelorixLogicalViewPlanNodeV1::SemiEquiJoin { .. }
        | VelorixLogicalViewPlanNodeV1::AntiEquiJoin { .. }
        | VelorixLogicalViewPlanNodeV1::FullEquiJoin { .. }
        | VelorixLogicalViewPlanNodeV1::LatestByKey { .. }
        | VelorixLogicalViewPlanNodeV1::RowNumber { .. } => StateBoundednessV1::Unbounded,
        VelorixLogicalViewPlanNodeV1::TumblingWindow {
            event_time_column, ..
        } => StateBoundednessV1::WatermarkBounded {
            event_time_column_id: global_column_id(event_time_column),
            allowed_lateness_ns: 0,
        },
        _ => return None,
    };
    Some(StateContractV1 {
        boundedness,
        checkpoint_codec: CheckpointCodecIdentityV1 {
            codec_id: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
            codec_version: 1,
        },
    })
}

fn augment_execution_output_aliases(
    node: &VelorixLogicalViewPlanNodeV1,
    plan: &VelorixLogicalViewPlanV1,
    schema: &mut RowSchemaV1,
) {
    let output_relation_id = &plan.output_relation.relation_id;
    let mut aliases = Vec::new();
    match (&plan.execution, node) {
        (
            VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported },
            VelorixLogicalViewPlanNodeV1::Aggregate { .. },
        ) => aliases.extend(
            supported_view_plan_group_keys(supported)
                .into_iter()
                .map(|key| key.output_column_id),
        ),
        (
            VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported },
            VelorixLogicalViewPlanNodeV1::Aggregate { .. },
        ) => aliases.push(supported.output_key_column_id.clone()),
        (
            VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported },
            VelorixLogicalViewPlanNodeV1::Aggregate { .. },
        ) => {
            aliases.extend([
                supported.output_key_column_id.clone(),
                supported.window_start_output_column_id.clone(),
                supported.window_end_output_column_id.clone(),
            ]);
        }
        (
            VelorixLogicalViewExecutionV1::FilterProject { plan: supported },
            VelorixLogicalViewPlanNodeV1::Project { .. },
        ) => {
            aliases.push(supported.output_key_column_id.clone());
            aliases.extend(
                supported
                    .value_columns
                    .iter()
                    .map(|column| column.output_column_id.clone()),
            );
            if let Some(column_id) = supported
                .top_k
                .as_ref()
                .and_then(|top_k| top_k.order_input_column_id.as_ref())
            {
                schema.columns.push(PortColumnV1 {
                    column_id: format!("{output_relation_id}.{column_id}"),
                    logical_type: "logical-hidden-order-input-v1".to_string(),
                    nullability: NullabilityV1::Nullable,
                });
            }
        }
        (
            VelorixLogicalViewExecutionV1::TwoInputSemiAntiJoinProject { plan: supported },
            VelorixLogicalViewPlanNodeV1::Project { .. },
        ) => {
            aliases.push(supported.projection.output_key_column_id.clone());
            aliases.extend(
                supported
                    .projection
                    .value_columns
                    .iter()
                    .map(|column| column.output_column_id.clone()),
            );
        }
        (
            VelorixLogicalViewExecutionV1::LatestByKey { plan: supported },
            VelorixLogicalViewPlanNodeV1::Project { .. },
        ) => aliases.extend([
            supported.output_key_column_id.clone(),
            supported.output_value_column_id.clone(),
        ]),
        (
            VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan: supported },
            VelorixLogicalViewPlanNodeV1::RowNumber { .. },
        ) => aliases.extend([
            supported.output_key_column_id.clone(),
            supported.output_row_number_column_id.clone(),
        ]),
        _ => {}
    }
    for alias in aliases.into_iter().filter(|alias| !alias.is_empty()) {
        schema.columns.push(PortColumnV1 {
            column_id: format!("{output_relation_id}.{alias}"),
            logical_type: "logical-output-alias-v1".to_string(),
            nullability: NullabilityV1::Nullable,
        });
    }
}

fn referenced_columns_by_relation(
    nodes: &[VelorixLogicalViewPlanNodeV1],
) -> BTreeMap<String, BTreeSet<String>> {
    let mut by_relation = BTreeMap::new();
    for node in nodes {
        for column in required_columns_for_node(node, "left")
            .into_iter()
            .chain(required_columns_for_node(node, "right"))
        {
            by_relation
                .entry(column.relation_id)
                .or_insert_with(BTreeSet::new)
                .insert(column.column_id);
        }
    }
    by_relation
}

fn window_watermarks_by_relation(
    nodes: &[VelorixLogicalViewPlanNodeV1],
) -> BTreeMap<String, BTreeSet<String>> {
    let mut by_relation = BTreeMap::new();
    for node in nodes {
        if let VelorixLogicalViewPlanNodeV1::TumblingWindow {
            event_time_column, ..
        } = node
        {
            by_relation
                .entry(event_time_column.relation_id.clone())
                .or_insert_with(BTreeSet::new)
                .insert(event_time_column.column_id.clone());
        }
    }
    by_relation
}

fn derived_port_column(column: &LogicalPlanColumnRef) -> PortColumnV1 {
    PortColumnV1 {
        column_id: global_column_id(column),
        logical_type: "logical-plan-derived-v1".to_string(),
        nullability: NullabilityV1::Nullable,
    }
}

fn global_column_id(column: &LogicalPlanColumnRef) -> String {
    format!("{}.{}", column.relation_id, column.column_id)
}

fn canonicalize_port_columns(columns: &mut Vec<PortColumnV1>) {
    columns.sort_by(|left, right| left.column_id.cmp(&right.column_id));
    columns.dedup_by(|left, right| left.column_id == right.column_id);
}

fn validate_all_nodes_reach_output(
    nodes: &[VelorixLogicalViewPlanNodeV1],
    edges: &[OperatorEdgeV1],
) -> Result<(), ViewPlanError> {
    let output_id = nodes
        .iter()
        .find_map(|node| match node {
            VelorixLogicalViewPlanNodeV1::Output { node_id, .. } => Some(node_id.as_str()),
            _ => None,
        })
        .ok_or_else(|| ViewPlanError::InvalidLogicalPlan {
            reason: "logical view plan output node is missing".to_string(),
        })?;
    let mut reachable = BTreeSet::from([output_id.to_string()]);
    let mut changed = true;
    while changed {
        changed = false;
        for edge in edges {
            if reachable.contains(&edge.to.node_id) && reachable.insert(edge.from.node_id.clone()) {
                changed = true;
            }
        }
    }
    if nodes
        .iter()
        .any(|node| !reachable.contains(logical_node_id(node)))
    {
        return invalid_logical_plan(
            "logical operator DAG contains a node outside the output path",
        );
    }
    Ok(())
}

fn single_key_sum_count_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
    supported: SupportedViewPlan,
) -> VelorixLogicalViewPlanV1 {
    let input_relation = logical_relation_from_catalog(catalog);
    let output_relation = logical_relation_from_schema(output_schema);
    let group_key_specs = supported_view_plan_group_keys(&supported);
    let group_keys = group_key_specs
        .iter()
        .map(|key| {
            key.input_column_id
                .as_deref()
                .map(|column_id| column_ref(&supported.input_relation_id, column_id))
                .unwrap_or_else(|| column_ref(&output_relation.relation_id, &key.output_column_id))
        })
        .collect::<Vec<_>>();
    let accumulators = supported_view_plan_aggregate_outputs(&supported)
        .iter()
        .map(|output| LogicalPlanAggregateAccumulatorV1 {
            function: output.function,
            input: output
                .input_column_id
                .as_ref()
                .map(|column_id| column_ref(&supported.input_relation_id, column_id)),
            output_column_id: output.output_column_id.clone(),
        })
        .collect();
    let scan_node = "scan_input".to_string();
    let mut current_node = scan_node.clone();
    let mut nodes = vec![VelorixLogicalViewPlanNodeV1::RelationScan {
        node_id: scan_node,
        relation: input_relation.clone(),
    }];
    if !supported
        .predicate_expr
        .as_ref()
        .is_some_and(RowPredicateExpr::contains_or)
    {
        let predicates = supported
            .predicate_expr
            .as_ref()
            .map(RowPredicateExpr::leaf_predicates)
            .or_else(|| supported.predicate.clone().map(|predicate| vec![predicate]))
            .unwrap_or_default();
        for (index, predicate) in predicates.iter().enumerate() {
            let filter_node = if index == 0 {
                "filter_input".to_string()
            } else {
                format!("filter_input_{index}")
            };
            nodes.push(VelorixLogicalViewPlanNodeV1::Filter {
                node_id: filter_node.clone(),
                input: current_node,
                predicate: LogicalPlanPredicateV1 {
                    column: column_ref(&supported.input_relation_id, &predicate.column_id),
                    op: predicate.op,
                    literal: predicate.literal.clone(),
                },
            });
            current_node = filter_node;
        }
    }
    let computed_columns = group_key_specs
        .iter()
        .filter_map(|key| {
            key.expression
                .clone()
                .map(|expression| LogicalPlanComputedColumnV1 {
                    output: column_ref(&output_relation.relation_id, &key.output_column_id),
                    input_relation_id: supported.input_relation_id.clone(),
                    expression,
                })
        })
        .collect::<Vec<_>>();
    if !computed_columns.is_empty() {
        let project_node = "project_group_expressions".to_string();
        let mut passthrough_ids = group_key_specs
            .iter()
            .filter_map(|key| key.input_column_id.clone())
            .collect::<BTreeSet<_>>();
        for aggregate in supported_view_plan_aggregate_outputs(&supported) {
            if let Some(column_id) = aggregate.input_column_id {
                passthrough_ids.insert(column_id);
            }
        }
        for computed in &computed_columns {
            passthrough_ids.extend(supported_projection_expr_column_ids(&computed.expression));
        }
        nodes.push(VelorixLogicalViewPlanNodeV1::Project {
            node_id: project_node.clone(),
            input: current_node,
            columns: passthrough_ids
                .into_iter()
                .map(|column_id| column_ref(&supported.input_relation_id, &column_id))
                .collect(),
            computed_columns,
        });
        current_node = project_node;
    }
    let aggregate_node = "aggregate_sum_count".to_string();
    nodes.push(VelorixLogicalViewPlanNodeV1::Aggregate {
        node_id: aggregate_node.clone(),
        input: current_node,
        group_keys: group_keys.clone(),
        accumulators,
    });
    current_node = aggregate_node.clone();
    if !supported
        .having_expr
        .as_ref()
        .is_some_and(AggregateOutputPredicateExpr::contains_or)
    {
        let predicates = supported
            .having_expr
            .as_ref()
            .map(AggregateOutputPredicateExpr::leaf_predicates)
            .or_else(|| supported.having.clone().map(|having| vec![having]))
            .unwrap_or_default();
        for (index, having) in predicates.iter().enumerate() {
            let filter_node = if index == 0 {
                "filter_aggregate".to_string()
            } else {
                format!("filter_aggregate_{index}")
            };
            nodes.push(VelorixLogicalViewPlanNodeV1::Filter {
                node_id: filter_node.clone(),
                input: current_node,
                predicate: LogicalPlanPredicateV1 {
                    column: column_ref(&output_relation.relation_id, &having.output_column_id),
                    op: having.op,
                    literal: having.literal.clone(),
                },
            });
            current_node = filter_node;
        }
    }
    if let Some(top_k) = &supported.top_k {
        let top_k_node = "top_k_materialized_output".to_string();
        nodes.push(VelorixLogicalViewPlanNodeV1::TopK {
            node_id: top_k_node.clone(),
            input: current_node,
            order_by: column_ref(&output_relation.relation_id, &top_k.order_output_column_id),
            descending: top_k.descending,
            limit: top_k.limit,
            offset: top_k.offset,
        });
        current_node = top_k_node;
    }
    nodes.push(VelorixLogicalViewPlanNodeV1::Output {
        node_id: "output_materialized_view".to_string(),
        input: current_node,
        relation: output_relation.clone(),
    });
    VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations: vec![input_relation],
        output_relation,
        nodes,
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: aggregate_node,
            state_kind: LogicalPlanStateKindV1::Aggregate,
            key_columns: group_keys,
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported },
    }
}

/// Build a single-key sum/count logical plan from a published-view input schema.
///
/// Mirror of `single_key_sum_count_logical_plan` that plans against a persisted
/// `RelationSchema` instead of a physical `VelorixRelationCatalogV1`. The
/// relation's logical ref comes from `logical_relation_from_schema`, and the
/// aggregate key/accumulator columns bind to the published input relation id.
fn single_key_sum_count_logical_plan_from_input(
    sql: &str,
    input_relation: &RelationSchema,
    _key_column: String,
    output_schema: &RelationSchema,
    supported: SupportedViewPlan,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    if supported.input_relation_id != input_relation.relation_id {
        return unsupported(
            "published view plan input relation id does not match the resolved relation",
        );
    }
    let input_relation_ref = logical_relation_from_schema(input_relation);
    let output_relation = logical_relation_from_schema(output_schema);
    let group_key_specs = supported_view_plan_group_keys(&supported);
    let group_keys = group_key_specs
        .iter()
        .map(|key| {
            key.input_column_id
                .as_deref()
                .map(|column_id| column_ref(&supported.input_relation_id, column_id))
                .unwrap_or_else(|| column_ref(&output_relation.relation_id, &key.output_column_id))
        })
        .collect::<Vec<_>>();
    let accumulators = supported_view_plan_aggregate_outputs(&supported)
        .iter()
        .map(|output| LogicalPlanAggregateAccumulatorV1 {
            function: output.function,
            input: output
                .input_column_id
                .as_ref()
                .map(|column_id| column_ref(&supported.input_relation_id, column_id)),
            output_column_id: output.output_column_id.clone(),
        })
        .collect();
    let scan_node = "scan_input".to_string();
    let mut current_node = scan_node.clone();
    let mut nodes = vec![VelorixLogicalViewPlanNodeV1::RelationScan {
        node_id: scan_node,
        relation: input_relation_ref.clone(),
    }];
    if !supported
        .predicate_expr
        .as_ref()
        .is_some_and(RowPredicateExpr::contains_or)
    {
        let predicates = supported
            .predicate_expr
            .as_ref()
            .map(RowPredicateExpr::leaf_predicates)
            .or_else(|| supported.predicate.clone().map(|predicate| vec![predicate]))
            .unwrap_or_default();
        for (index, predicate) in predicates.iter().enumerate() {
            let filter_node = if index == 0 {
                "filter_input".to_string()
            } else {
                format!("filter_input_{index}")
            };
            nodes.push(VelorixLogicalViewPlanNodeV1::Filter {
                node_id: filter_node.clone(),
                input: current_node,
                predicate: LogicalPlanPredicateV1 {
                    column: column_ref(&supported.input_relation_id, &predicate.column_id),
                    op: predicate.op,
                    literal: predicate.literal.clone(),
                },
            });
            current_node = filter_node;
        }
    }
    let aggregate_node = "aggregate_input".to_string();
    nodes.push(VelorixLogicalViewPlanNodeV1::Aggregate {
        node_id: aggregate_node.clone(),
        input: current_node,
        group_keys: group_keys.clone(),
        accumulators,
    });
    let current_node = aggregate_node.clone();
    nodes.push(VelorixLogicalViewPlanNodeV1::Output {
        node_id: "output_materialized_view".to_string(),
        input: current_node,
        relation: output_relation.clone(),
    });
    Ok(VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations: vec![input_relation_ref],
        output_relation,
        nodes,
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: aggregate_node,
            state_kind: LogicalPlanStateKindV1::Aggregate,
            key_columns: group_keys,
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported },
    })
}

/// Validate a published-view single-key sum/count SQL shape against a planner
/// relation input.
///
/// This is the catalog-free core of `validate_supported_view_sql`. It does not
/// perform physical catalog or adapter validation. The first Phase 4 slice
/// admits only the narrow direct-PK single-key sum/count family:
///
/// - exactly one published delta input
/// - direct single primary-key grouping
/// - `SUM(non-null Int64 column)` and/or `COUNT(*)` / `COUNT(column)`
/// - no Top-K, window, CTE/derived input, DISTINCT, HAVING, aggregate FILTER,
///   or computed group key
///
/// Any other shape fails closed; there is no family fallback.
fn validate_supported_view_sql_with_input(
    sql: &str,
    input: &PlannerRelationInput,
    _registration_name: Option<&str>,
) -> Result<SupportedViewPlan, ViewPlanError> {
    if input.weight_column_id.is_some() {
        return unsupported("published view input must not carry a physical weight column");
    }
    if input.event_time_column_id.is_some() {
        return unsupported("published view input must not carry a physical event-time column");
    }
    let [key_column_id] = input.relation.primary_key.as_slice() else {
        return unsupported("published view SQL requires exactly one primary key column");
    };
    let key_column_id = key_column_id.as_str();
    let query = parse_single_query(sql)?;
    if query.limit_clause.is_some() || query.fetch.is_some() {
        return unsupported("published view aggregate Top-K is not supported in this slice");
    }
    let select = match query.body.as_ref() {
        sqlparser::ast::SetExpr::Select(select) => select.as_ref(),
        _ => return unsupported("published view SQL requires a plain SELECT"),
    };
    if select.distinct.is_some() {
        return unsupported("published view aggregate DISTINCT is not supported in this slice");
    }
    if select.having.is_some() {
        return unsupported("published view aggregate HAVING is not supported in this slice");
    }
    let (group_exprs, modifiers) = match &select.group_by {
        GroupByExpr::All(modifiers) => (Vec::new(), modifiers),
        GroupByExpr::Expressions(exprs, modifiers) => (exprs.clone(), modifiers),
    };
    if !modifiers.is_empty() {
        return unsupported("published view GROUP BY modifiers are not supported");
    }
    if group_exprs.len() != 1 {
        return unsupported("published view SQL requires exactly one group key");
    }
    let group_expr = &group_exprs[0];
    if let Some(column) = expression_catalog_column_against_relation(group_expr, input) {
        if column.name != key_column_id {
            return unsupported("published view group key must be the single primary key column");
        }
    } else {
        return unsupported("published view group key must reference the primary key column");
    }

    let mut aggregate_outputs = Vec::new();
    let mut sum_value_column_id: Option<String> = None;
    let mut seen = BTreeSet::new();
    for item in select.projection.iter().skip(1) {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
            _ => {
                return unsupported(
                    "published view aggregate projections must be scalar expressions",
                )
            }
        };
        let Expr::Function(function) = expr else {
            return unsupported("published view aggregate projections must be aggregate functions");
        };
        let FunctionArguments::List(arguments) = &function.args else {
            return unsupported("published view aggregate functions require an argument list");
        };
        let function_name = single_object_name_identifier(&function.name).ok_or_else(|| {
            ViewPlanError::UnsupportedShape {
                reason: "published view aggregate function name is not recognized".to_string(),
            }
        })?;
        let upper = function_name.to_ascii_uppercase();
        let alias = alias.unwrap_or(function_name.as_str());
        if !seen.insert(alias.to_string()) {
            return unsupported("published view aggregate output aliases must be unique");
        }
        match upper.as_str() {
            "SUM" => {
                if arguments.args.len() != 1 {
                    return unsupported("SUM requires exactly one argument");
                }
                let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) = &arguments.args[0]
                else {
                    return unsupported("SUM argument must be a scalar expression");
                };
                let Some(column) = expression_catalog_column_against_relation(arg_expr, input)
                else {
                    return unsupported("SUM argument must reference a registered column");
                };
                if column.name == key_column_id {
                    return unsupported("SUM of the group key column is not supported");
                }
                sum_value_column_id = Some(column.name.clone());
                aggregate_outputs.push(SupportedAggregateOutput {
                    function: LogicalPlanAggregateFunctionV1::Sum,
                    output_column_id: alias.to_string(),
                    input_column_id: Some(column.name.clone()),
                    input_relation_side: None,
                    input_expression: None,
                });
            }
            "COUNT" => match arguments.args.as_slice() {
                [] | [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] => {
                    aggregate_outputs.push(SupportedAggregateOutput {
                        function: LogicalPlanAggregateFunctionV1::Count,
                        output_column_id: alias.to_string(),
                        input_column_id: None,
                        input_relation_side: None,
                        input_expression: None,
                    });
                }
                [FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))] => {
                    if let Expr::Value(v) = arg_expr {
                        if let SqlValue::Number(n, _) = &v.value {
                            if n == "1" {
                                aggregate_outputs.push(SupportedAggregateOutput {
                                    function: LogicalPlanAggregateFunctionV1::Count,
                                    output_column_id: alias.to_string(),
                                    input_column_id: None,
                                    input_relation_side: None,
                                    input_expression: None,
                                });
                                continue;
                            }
                        }
                    }
                    if let Some(column) =
                        expression_catalog_column_against_relation(arg_expr, input)
                    {
                        aggregate_outputs.push(SupportedAggregateOutput {
                            function: LogicalPlanAggregateFunctionV1::Count,
                            output_column_id: alias.to_string(),
                            input_column_id: Some(column.name.clone()),
                            input_relation_side: None,
                            input_expression: None,
                        });
                    } else {
                        return unsupported("COUNT argument must reference a registered column");
                    }
                }
                _ => return unsupported("COUNT supports zero or one argument in this slice"),
            },
            _ => {
                return unsupported(
                    "published view aggregate only supports SUM and COUNT in this slice",
                );
            }
        }
    }
    if aggregate_outputs.is_empty() {
        return unsupported("published view SQL requires at least one aggregate projection");
    }

    Ok(SupportedViewPlan {
        input_relation_id: input.relation.relation_id.clone(),
        group_key_column_id: key_column_id.to_string(),
        aggregate_output_identity: Some(SupportedAggregateOutputIdentity::GroupKey {
            group_keys: vec![SupportedGroupKey {
                output_column_id: key_column_id.to_string(),
                input_column_id: Some(key_column_id.to_string()),
                expression: None,
            }],
        }),
        output_key_column_id: key_column_id.to_string(),
        sum_value_column_id: sum_value_column_id.unwrap_or_else(|| key_column_id.to_string()),
        aggregate_outputs,
        predicate: None,
        predicate_expr: None,
        aggregate_filter_exprs: BTreeMap::new(),
        having: None,
        having_expr: None,
        top_k: None,
    })
}

/// Resolve a column reference in a published-view relation schema.
///
/// Returns the matching `ColumnSchema` when the expression is an unqualified or
/// alias-qualified identifier resolving to a column of the published relation.
fn expression_catalog_column_against_relation<'a>(
    expr: &'a Expr,
    input: &'a PlannerRelationInput,
) -> Option<&'a ColumnSchema> {
    let column_name = match expr {
        Expr::Identifier(ident) => ident.value.as_str(),
        Expr::CompoundIdentifier(idents) if idents.len() == 2 => idents[1].value.as_str(),
        _ => return None,
    };
    input
        .relation
        .columns
        .iter()
        .find(|column| column.name == column_name)
}

fn three_input_inner_join_count_logical_plan(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
    supported: SupportedThreeInputInnerJoinCountPlanV1,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    if !three_input_join_order_policy_is_valid(&supported)
        || supported.ordered_input_relation_ids.len() != 3
        || supported.root_to_input_pk_permutations.len() != 3
        || supported.root_primary_key_column_ids.len() < 2
        || supported.output_key_column_ids.len() != supported.root_primary_key_column_ids.len()
        || supported.join_key_codec_id != COMPOSITE_PK_POSITIONAL_JSON_ARRAY_JOIN_KEY_CODEC_V1
    {
        return invalid_logical_plan("invalid three-input join execution plan");
    }
    let ordered_catalogs = supported
        .ordered_input_relation_ids
        .iter()
        .map(|relation_id| {
            catalogs
                .iter()
                .find(|catalog| &catalog.relation_schema.relation_id == relation_id)
                .ok_or_else(|| ViewPlanError::InvalidLogicalPlan {
                    reason: "three-input join catalog binding is missing".to_string(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if ordered_catalogs.len() != catalogs.len() {
        return invalid_logical_plan("three-input join catalog binding is not bijective");
    }
    let expected_columns = supported
        .output_key_column_ids
        .iter()
        .cloned()
        .chain(std::iter::once(supported.count_output_column_id.clone()))
        .collect::<Vec<_>>();
    if output_schema
        .columns
        .iter()
        .map(|column| &column.name)
        .ne(expected_columns.iter())
        || output_schema.primary_key != supported.output_key_column_ids
    {
        return invalid_logical_plan(
            "three-input join output schema does not match SQL projection",
        );
    }

    let input_relations = ordered_catalogs
        .iter()
        .map(|catalog| logical_relation_from_catalog(catalog))
        .collect::<Vec<_>>();
    let output_relation = logical_relation_from_schema(output_schema);
    let scan_ids = ["scan_0", "scan_1", "scan_2"];
    let mut nodes = ordered_catalogs
        .iter()
        .zip(scan_ids)
        .map(
            |(catalog, node_id)| VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: node_id.to_string(),
                relation: logical_relation_from_catalog(catalog),
            },
        )
        .collect::<Vec<_>>();
    let root_relation_id = &supported.ordered_input_relation_ids[0];
    let mut steps = Vec::new();
    let mut state_requirements = Vec::new();
    for step_index in 1..3 {
        let right_catalog = ordered_catalogs[step_index];
        let permutation = &supported.root_to_input_pk_permutations[step_index];
        if permutation.len() != supported.root_primary_key_column_ids.len() {
            return invalid_logical_plan("three-input join PK permutation has the wrong arity");
        }
        let pairs = supported
            .root_primary_key_column_ids
            .iter()
            .enumerate()
            .map(|(position, root_column_id)| {
                let right_position = *permutation.get(position).ok_or_else(|| {
                    ViewPlanError::InvalidLogicalPlan {
                        reason: "three-input join PK permutation is incomplete".to_string(),
                    }
                })?;
                let right_column_id = right_catalog
                    .relation_schema
                    .primary_key_column_ids
                    .get(right_position)
                    .ok_or_else(|| ViewPlanError::InvalidLogicalPlan {
                        reason: "three-input join PK permutation is out of range".to_string(),
                    })?;
                Ok((
                    column_ref(root_relation_id, root_column_id),
                    column_ref(&right_catalog.relation_schema.relation_id, right_column_id),
                ))
            })
            .collect::<Result<Vec<_>, ViewPlanError>>()?;
        let (left_key, right_key) = pairs[0].clone();
        let composite_equality = LogicalPlanCompositeJoinEqualityV1 {
            schema_version: 1,
            additional_pairs: pairs
                .iter()
                .skip(1)
                .map(|(left_key, right_key)| LogicalPlanJoinKeyPairV1 {
                    left_key: left_key.clone(),
                    right_key: right_key.clone(),
                })
                .collect(),
        };
        let node_id = format!("join_{step_index}");
        steps.push(LogicalPlanBinaryJoinStepV1 {
            node_id: node_id.clone(),
            right_input: scan_ids[step_index].to_string(),
            left_key: left_key.clone(),
            right_key: right_key.clone(),
            composite_equality: Some(composite_equality.clone()),
            join_kind: SupportedJoinKind::Inner,
        });
        state_requirements.push(LogicalPlanStateRequirementV1 {
            node_id,
            state_kind: LogicalPlanStateKindV1::JoinIndex,
            key_columns: std::iter::once(left_key)
                .chain(std::iter::once(right_key))
                .chain(
                    composite_equality
                        .additional_pairs
                        .iter()
                        .flat_map(|pair| [pair.left_key.clone(), pair.right_key.clone()]),
                )
                .collect(),
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
        });
    }
    let (join_nodes, final_join) = lower_join_chain_to_binary_dag(scan_ids[0], &steps)?;
    nodes.extend(join_nodes);
    nodes.push(VelorixLogicalViewPlanNodeV1::Project {
        node_id: "project_three_input_count".to_string(),
        input: final_join,
        columns: supported
            .root_primary_key_column_ids
            .iter()
            .map(|column_id| column_ref(root_relation_id, column_id))
            .collect(),
        computed_columns: vec![LogicalPlanComputedColumnV1 {
            output: column_ref(&output_relation.relation_id, "__velorix_count_one"),
            input_relation_id: root_relation_id.clone(),
            expression: SupportedProjectionExpr::LiteralInt64 { value: 1 },
        }],
    });
    let aggregate_node = "aggregate_three_input_count".to_string();
    let group_keys = supported
        .root_primary_key_column_ids
        .iter()
        .map(|column_id| column_ref(root_relation_id, column_id))
        .collect::<Vec<_>>();
    nodes.push(VelorixLogicalViewPlanNodeV1::Aggregate {
        node_id: aggregate_node.clone(),
        input: "project_three_input_count".to_string(),
        group_keys: group_keys.clone(),
        accumulators: vec![LogicalPlanAggregateAccumulatorV1 {
            function: LogicalPlanAggregateFunctionV1::Count,
            input: None,
            output_column_id: supported.count_output_column_id.clone(),
        }],
    });
    nodes.push(VelorixLogicalViewPlanNodeV1::Output {
        node_id: "output_materialized_view".to_string(),
        input: aggregate_node.clone(),
        relation: output_relation.clone(),
    });
    state_requirements.push(LogicalPlanStateRequirementV1 {
        node_id: aggregate_node,
        state_kind: LogicalPlanStateKindV1::Aggregate,
        key_columns: group_keys,
        codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
    });
    Ok(VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations,
        output_relation,
        nodes,
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements,
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::ThreeInputInnerJoinCount { plan: supported },
    })
}

fn two_input_join_sum_count_logical_plan(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
    supported: SupportedJoinViewPlan,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let left_catalog = catalogs
        .iter()
        .find(|catalog| catalog.relation_schema.relation_id == supported.left_input_relation_id)
        .ok_or_else(|| ViewPlanError::InvalidLogicalPlan {
            reason: "left join relation is missing from catalogs".to_string(),
        })?;
    let right_catalog = catalogs
        .iter()
        .find(|catalog| catalog.relation_schema.relation_id == supported.right_input_relation_id)
        .ok_or_else(|| ViewPlanError::InvalidLogicalPlan {
            reason: "right join relation is missing from catalogs".to_string(),
        })?;
    let left_relation = logical_relation_from_catalog(left_catalog);
    let right_relation = logical_relation_from_catalog(right_catalog);
    let output_relation = logical_relation_from_schema(output_schema);
    let left_scan = LEFT_JOIN_INPUT_INSTANCE_ID_V1.to_string();
    let right_scan = RIGHT_JOIN_INPUT_INSTANCE_ID_V1.to_string();
    let mut left_join_input = left_scan.clone();
    let mut right_join_input = right_scan.clone();
    let join_node = match supported.join_kind {
        SupportedJoinKind::Inner => "inner_equi_join".to_string(),
        SupportedJoinKind::Left => "left_equi_join".to_string(),
        SupportedJoinKind::Full => "full_equi_join".to_string(),
    };
    let aggregate_node = "aggregate_join_sum_count".to_string();
    let mut current_node = aggregate_node.clone();
    let left_key = match supported.left_input_instance_id.as_deref() {
        Some(instance_id) => instance_column_ref(
            &supported.left_input_relation_id,
            instance_id,
            &supported.left_join_key_column_id,
        ),
        None => column_ref(
            &supported.left_input_relation_id,
            &supported.left_join_key_column_id,
        ),
    };
    let right_key = match supported.right_input_instance_id.as_deref() {
        Some(instance_id) => instance_column_ref(
            &supported.right_input_relation_id,
            instance_id,
            &supported.right_join_key_column_id,
        ),
        None => column_ref(
            &supported.right_input_relation_id,
            &supported.right_join_key_column_id,
        ),
    };
    let logical_composite_equality =
        supported
            .composite_equality
            .as_ref()
            .map(|composite| LogicalPlanCompositeJoinEqualityV1 {
                schema_version: composite.schema_version,
                additional_pairs: composite
                    .additional_pairs
                    .iter()
                    .map(|pair| LogicalPlanJoinKeyPairV1 {
                        left_key: supported
                            .left_input_instance_id
                            .as_deref()
                            .map(|instance_id| {
                                instance_column_ref(
                                    &supported.left_input_relation_id,
                                    instance_id,
                                    &pair.left_column_id,
                                )
                            })
                            .unwrap_or_else(|| {
                                column_ref(&supported.left_input_relation_id, &pair.left_column_id)
                            }),
                        right_key: supported
                            .right_input_instance_id
                            .as_deref()
                            .map(|instance_id| {
                                instance_column_ref(
                                    &supported.right_input_relation_id,
                                    instance_id,
                                    &pair.right_column_id,
                                )
                            })
                            .unwrap_or_else(|| {
                                column_ref(
                                    &supported.right_input_relation_id,
                                    &pair.right_column_id,
                                )
                            }),
                    })
                    .collect(),
            });
    let group_key = if supported.join_kind == SupportedJoinKind::Full {
        column_ref(
            &output_relation.relation_id,
            &supported.output_key_column_id,
        )
    } else {
        column_ref(
            &supported.group_key_relation_id,
            &supported.group_key_column_id,
        )
    };
    let group_keys = if supported_join_view_plan_is_singleton(&supported) {
        Vec::new()
    } else {
        vec![group_key.clone()]
    };
    let accumulators = supported_join_view_plan_aggregate_outputs(&supported)
        .iter()
        .map(|output| LogicalPlanAggregateAccumulatorV1 {
            function: output.function,
            input: output.input_column_id.as_ref().map(|column_id| {
                column_ref(
                    join_aggregate_input_relation_id(&supported, output),
                    column_id,
                )
            }),
            output_column_id: output.output_column_id.clone(),
        })
        .collect();
    let mut nodes = vec![
        VelorixLogicalViewPlanNodeV1::RelationScan {
            node_id: left_scan.clone(),
            relation: left_relation.clone(),
        },
        VelorixLogicalViewPlanNodeV1::RelationScan {
            node_id: right_scan.clone(),
            relation: right_relation.clone(),
        },
    ];
    if !supported
        .predicate_expr
        .as_ref()
        .is_some_and(JoinPredicateExpr::contains_or)
    {
        for (index, predicate) in supported_join_view_plan_predicates(&supported)
            .iter()
            .enumerate()
        {
            if supported.join_kind == SupportedJoinKind::Full
                || (supported.join_kind == SupportedJoinKind::Left
                    && predicate.relation_id == supported.right_input_relation_id)
            {
                continue;
            }
            let (input, relation_id, next_input, filter_node) =
                if predicate.relation_id == supported.left_input_relation_id {
                    (
                        left_join_input.clone(),
                        supported.left_input_relation_id.as_str(),
                        &mut left_join_input,
                        if index == 0 {
                            "filter_join_left".to_string()
                        } else {
                            format!("filter_join_left_{index}")
                        },
                    )
                } else {
                    (
                        right_join_input.clone(),
                        supported.right_input_relation_id.as_str(),
                        &mut right_join_input,
                        if index == 0 {
                            "filter_join_right".to_string()
                        } else {
                            format!("filter_join_right_{index}")
                        },
                    )
                };
            nodes.push(VelorixLogicalViewPlanNodeV1::Filter {
                node_id: filter_node.clone(),
                input,
                predicate: LogicalPlanPredicateV1 {
                    column: column_ref(relation_id, &predicate.predicate.column_id),
                    op: predicate.predicate.op,
                    literal: predicate.predicate.literal.clone(),
                },
            });
            *next_input = filter_node;
        }
    }
    if supported.join_kind == SupportedJoinKind::Full {
        nodes.push(VelorixLogicalViewPlanNodeV1::FullEquiJoin {
            node_id: join_node.clone(),
            left: left_join_input,
            right: right_join_input,
            left_key: left_key.clone(),
            right_key: right_key.clone(),
            output_key: group_key.clone(),
            composite_equality: logical_composite_equality.clone(),
        });
    } else {
        let (join_nodes, lowered_join_node) = lower_join_chain_to_binary_dag(
            &left_join_input,
            &[LogicalPlanBinaryJoinStepV1 {
                node_id: join_node.clone(),
                right_input: right_join_input,
                left_key: left_key.clone(),
                right_key: right_key.clone(),
                composite_equality: logical_composite_equality.clone(),
                join_kind: supported.join_kind,
            }],
        )?;
        debug_assert_eq!(lowered_join_node, join_node);
        nodes.extend(join_nodes);
    }
    let mut aggregate_input = join_node.clone();
    if matches!(
        supported.join_kind,
        SupportedJoinKind::Left | SupportedJoinKind::Full
    ) && !supported
        .predicate_expr
        .as_ref()
        .is_some_and(JoinPredicateExpr::contains_or)
    {
        for (index, predicate) in supported_join_view_plan_predicates(&supported)
            .iter()
            .filter(|predicate| {
                supported.join_kind == SupportedJoinKind::Full
                    || predicate.relation_id == supported.right_input_relation_id
            })
            .enumerate()
        {
            let prefix = if supported.join_kind == SupportedJoinKind::Left {
                "filter_left_join_post_right"
            } else {
                "filter_full_join_post"
            };
            let filter_node = if index == 0 {
                prefix.to_string()
            } else {
                format!("{prefix}_{index}")
            };
            nodes.push(VelorixLogicalViewPlanNodeV1::Filter {
                node_id: filter_node.clone(),
                input: aggregate_input,
                predicate: LogicalPlanPredicateV1 {
                    column: column_ref(&predicate.relation_id, &predicate.predicate.column_id),
                    op: predicate.predicate.op,
                    literal: predicate.predicate.literal.clone(),
                },
            });
            aggregate_input = filter_node;
        }
    }
    nodes.push(VelorixLogicalViewPlanNodeV1::Aggregate {
        node_id: aggregate_node.clone(),
        input: aggregate_input,
        group_keys,
        accumulators,
    });
    if !supported
        .having_expr
        .as_ref()
        .is_some_and(AggregateOutputPredicateExpr::contains_or)
    {
        let predicates = supported
            .having_expr
            .as_ref()
            .map(AggregateOutputPredicateExpr::leaf_predicates)
            .or_else(|| supported.having.clone().map(|having| vec![having]))
            .unwrap_or_default();
        for (index, having) in predicates.iter().enumerate() {
            let filter_node = if index == 0 {
                "filter_join_aggregate".to_string()
            } else {
                format!("filter_join_aggregate_{index}")
            };
            nodes.push(VelorixLogicalViewPlanNodeV1::Filter {
                node_id: filter_node.clone(),
                input: current_node,
                predicate: LogicalPlanPredicateV1 {
                    column: column_ref(&output_relation.relation_id, &having.output_column_id),
                    op: having.op,
                    literal: having.literal.clone(),
                },
            });
            current_node = filter_node;
        }
    }
    if let Some(top_k) = &supported.top_k {
        let top_k_node = "top_k_join_materialized_output".to_string();
        nodes.push(VelorixLogicalViewPlanNodeV1::TopK {
            node_id: top_k_node.clone(),
            input: current_node,
            order_by: column_ref(&output_relation.relation_id, &top_k.order_output_column_id),
            descending: top_k.descending,
            limit: top_k.limit,
            offset: top_k.offset,
        });
        current_node = top_k_node;
    }
    nodes.push(VelorixLogicalViewPlanNodeV1::Output {
        node_id: "output_materialized_view".to_string(),
        input: current_node,
        relation: output_relation.clone(),
    });

    Ok(VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations: if supported_join_view_plan_is_self_join(&supported) {
            vec![left_relation.clone()]
        } else {
            vec![left_relation.clone(), right_relation.clone()]
        },
        output_relation: output_relation.clone(),
        nodes,
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: vec![
            LogicalPlanStateRequirementV1 {
                node_id: join_node,
                state_kind: LogicalPlanStateKindV1::JoinIndex,
                key_columns: std::iter::once(left_key)
                    .chain(std::iter::once(right_key))
                    .chain(logical_composite_equality.iter().flat_map(|composite| {
                        composite
                            .additional_pairs
                            .iter()
                            .flat_map(|pair| [pair.left_key.clone(), pair.right_key.clone()])
                    }))
                    .collect(),
                codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
            },
            LogicalPlanStateRequirementV1 {
                node_id: aggregate_node,
                state_kind: LogicalPlanStateKindV1::Aggregate,
                key_columns: if supported_join_view_plan_is_singleton(&supported) {
                    Vec::new()
                } else {
                    vec![group_key]
                },
                codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
            },
        ],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::TwoInputJoinSumCount {
            plan: Box::new(supported),
        },
    })
}

fn latest_by_key_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
    supported: SupportedLatestByKeyPlan,
) -> VelorixLogicalViewPlanV1 {
    let input_relation = logical_relation_from_catalog(catalog);
    let output_relation = logical_relation_from_schema(output_schema);
    let key = column_ref(&supported.input_relation_id, &supported.key_column_id);
    let ordering = column_ref(&supported.input_relation_id, &supported.ordering_column_id);
    let scan_node = "scan_input".to_string();
    let mut latest_input = scan_node.clone();
    let latest_node = "latest_by_key".to_string();
    let project_node = "project_latest_value".to_string();
    let mut nodes = vec![VelorixLogicalViewPlanNodeV1::RelationScan {
        node_id: scan_node.clone(),
        relation: input_relation.clone(),
    }];
    if !supported
        .predicate_expr
        .as_ref()
        .is_some_and(RowPredicateExpr::contains_or)
    {
        let predicates = supported
            .predicate_expr
            .as_ref()
            .map(RowPredicateExpr::leaf_predicates)
            .unwrap_or_default();
        for (index, predicate) in predicates.iter().enumerate() {
            let filter_node = if index == 0 {
                "filter_latest_input".to_string()
            } else {
                format!("filter_latest_input_{index}")
            };
            nodes.push(VelorixLogicalViewPlanNodeV1::Filter {
                node_id: filter_node.clone(),
                input: latest_input,
                predicate: LogicalPlanPredicateV1 {
                    column: column_ref(&supported.input_relation_id, &predicate.column_id),
                    op: predicate.op,
                    literal: predicate.literal.clone(),
                },
            });
            latest_input = filter_node;
        }
    }
    nodes.extend([
        VelorixLogicalViewPlanNodeV1::LatestByKey {
            node_id: latest_node.clone(),
            input: latest_input,
            key_columns: vec![key.clone()],
            ordering_column: ordering,
        },
        VelorixLogicalViewPlanNodeV1::Project {
            node_id: project_node.clone(),
            input: latest_node.clone(),
            columns: vec![
                key.clone(),
                column_ref(&supported.input_relation_id, &supported.value_column_id),
            ],
            computed_columns: Vec::new(),
        },
    ]);
    let mut output_input = project_node;
    if let Some(top_k) = &supported.top_k {
        let top_k_node = "top_k_latest_materialized_output".to_string();
        nodes.push(VelorixLogicalViewPlanNodeV1::TopK {
            node_id: top_k_node.clone(),
            input: output_input,
            order_by: column_ref(&output_relation.relation_id, &top_k.order_output_column_id),
            descending: top_k.descending,
            limit: top_k.limit,
            offset: top_k.offset,
        });
        output_input = top_k_node;
    }
    nodes.push(VelorixLogicalViewPlanNodeV1::Output {
        node_id: "output_materialized_view".to_string(),
        input: output_input,
        relation: output_relation.clone(),
    });
    VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations: vec![input_relation.clone()],
        output_relation: output_relation.clone(),
        nodes,
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: latest_node,
            state_kind: LogicalPlanStateKindV1::LatestByKey,
            key_columns: vec![key],
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::LatestByKey { plan: supported },
    }
}

fn analytic_row_number_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
    supported: SupportedAnalyticRowNumberPlan,
) -> VelorixLogicalViewPlanV1 {
    let input_relation = logical_relation_from_catalog(catalog);
    let output_relation = logical_relation_from_schema(output_schema);
    let key = column_ref(&supported.input_relation_id, &supported.key_column_id);
    let partition = column_ref(&supported.input_relation_id, &supported.partition_column_id);
    let order = column_ref(&supported.input_relation_id, &supported.order_column_id);
    let scan_node = "scan_input".to_string();
    let mut current_node = scan_node.clone();
    let row_number_node = "analytic_row_number".to_string();
    let mut nodes = vec![VelorixLogicalViewPlanNodeV1::RelationScan {
        node_id: scan_node,
        relation: input_relation.clone(),
    }];
    if !supported
        .predicate_expr
        .as_ref()
        .is_some_and(RowPredicateExpr::contains_or)
    {
        let predicates = supported
            .predicate_expr
            .as_ref()
            .map(RowPredicateExpr::leaf_predicates)
            .unwrap_or_default();
        for (index, predicate) in predicates.iter().enumerate() {
            let filter_node = if index == 0 {
                "filter_row_number_input".to_string()
            } else {
                format!("filter_row_number_input_{index}")
            };
            nodes.push(VelorixLogicalViewPlanNodeV1::Filter {
                node_id: filter_node.clone(),
                input: current_node,
                predicate: LogicalPlanPredicateV1 {
                    column: column_ref(&supported.input_relation_id, &predicate.column_id),
                    op: predicate.op,
                    literal: predicate.literal.clone(),
                },
            });
            current_node = filter_node;
        }
    }
    nodes.extend([
        VelorixLogicalViewPlanNodeV1::RowNumber {
            node_id: row_number_node.clone(),
            input: current_node,
            partition_column: partition.clone(),
            order_column: order.clone(),
            descending: supported.order_descending,
            rank_limit: supported.rank_limit,
        },
        VelorixLogicalViewPlanNodeV1::Output {
            node_id: "output_materialized_view".to_string(),
            input: row_number_node.clone(),
            relation: output_relation.clone(),
        },
    ]);
    VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations: vec![input_relation],
        output_relation,
        nodes,
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: row_number_node,
            state_kind: LogicalPlanStateKindV1::RowNumber,
            key_columns: vec![partition, order, key],
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan: supported },
    }
}

fn tumbling_window_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
    supported: SupportedTumblingWindowPlan,
) -> VelorixLogicalViewPlanV1 {
    let input_relation = logical_relation_from_catalog(catalog);
    let output_relation = logical_relation_from_schema(output_schema);
    let group_key = column_ref(&supported.input_relation_id, &supported.group_key_column_id);
    let event_time = column_ref(
        &supported.input_relation_id,
        &supported.event_time_column_id,
    );
    let accumulators = supported
        .aggregate_outputs
        .iter()
        .map(|output| LogicalPlanAggregateAccumulatorV1 {
            function: output.function,
            input: output
                .input_column_id
                .as_ref()
                .map(|column_id| column_ref(&supported.input_relation_id, column_id)),
            output_column_id: output.output_column_id.clone(),
        })
        .collect();
    let scan_node = "scan_input".to_string();
    let mut window_input = scan_node.clone();
    let window_node = "tumbling_event_time_window".to_string();
    let aggregate_node = "aggregate_tumbling_window".to_string();
    let mut output_input = aggregate_node.clone();
    let mut nodes = vec![VelorixLogicalViewPlanNodeV1::RelationScan {
        node_id: scan_node.clone(),
        relation: input_relation.clone(),
    }];
    if !supported
        .predicate_expr
        .as_ref()
        .is_some_and(RowPredicateExpr::contains_or)
    {
        let predicates = supported
            .predicate_expr
            .as_ref()
            .map(RowPredicateExpr::leaf_predicates)
            .unwrap_or_default();
        for (index, predicate) in predicates.iter().enumerate() {
            let filter_node = if index == 0 {
                "filter_window_input".to_string()
            } else {
                format!("filter_window_input_{index}")
            };
            nodes.push(VelorixLogicalViewPlanNodeV1::Filter {
                node_id: filter_node.clone(),
                input: window_input,
                predicate: LogicalPlanPredicateV1 {
                    column: column_ref(&supported.input_relation_id, &predicate.column_id),
                    op: predicate.op,
                    literal: predicate.literal.clone(),
                },
            });
            window_input = filter_node;
        }
    }
    nodes.extend([
        VelorixLogicalViewPlanNodeV1::TumblingWindow {
            node_id: window_node.clone(),
            input: window_input,
            event_time_column: event_time.clone(),
            window_size_ns: supported.window_size_ns,
        },
        VelorixLogicalViewPlanNodeV1::Aggregate {
            node_id: aggregate_node.clone(),
            input: window_node,
            group_keys: vec![group_key.clone(), event_time],
            accumulators,
        },
    ]);
    if !supported
        .having_expr
        .as_ref()
        .is_some_and(AggregateOutputPredicateExpr::contains_or)
    {
        let predicates = supported
            .having_expr
            .as_ref()
            .map(AggregateOutputPredicateExpr::leaf_predicates)
            .unwrap_or_default();
        for (index, having) in predicates.iter().enumerate() {
            let filter_node = if index == 0 {
                "filter_window_aggregate".to_string()
            } else {
                format!("filter_window_aggregate_{index}")
            };
            nodes.push(VelorixLogicalViewPlanNodeV1::Filter {
                node_id: filter_node.clone(),
                input: output_input,
                predicate: LogicalPlanPredicateV1 {
                    column: column_ref(&output_relation.relation_id, &having.output_column_id),
                    op: having.op,
                    literal: having.literal.clone(),
                },
            });
            output_input = filter_node;
        }
    }
    if let Some(top_k) = &supported.top_k {
        let top_k_node = "top_k_window_materialized_output".to_string();
        nodes.push(VelorixLogicalViewPlanNodeV1::TopK {
            node_id: top_k_node.clone(),
            input: output_input,
            order_by: column_ref(&output_relation.relation_id, &top_k.order_output_column_id),
            descending: top_k.descending,
            limit: top_k.limit,
            offset: top_k.offset,
        });
        output_input = top_k_node;
    }
    nodes.push(VelorixLogicalViewPlanNodeV1::Output {
        node_id: "output_materialized_view".to_string(),
        input: output_input,
        relation: output_relation.clone(),
    });
    VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations: vec![input_relation.clone()],
        output_relation: output_relation.clone(),
        nodes,
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: aggregate_node,
            state_kind: LogicalPlanStateKindV1::TumblingWindowAggregate,
            key_columns: vec![group_key],
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported },
    }
}

fn filter_project_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
    supported: SupportedFilterProjectPlan,
) -> VelorixLogicalViewPlanV1 {
    let input_relation = logical_relation_from_catalog(catalog);
    let output_relation = logical_relation_from_schema(output_schema);
    let key = column_ref(
        &supported.input_relation_id,
        supported
            .output_key_input_column_id
            .as_deref()
            .unwrap_or(&supported.key_column_id),
    );
    let scan_node = "scan_input".to_string();
    let mut current_node = scan_node.clone();
    let mut nodes = vec![VelorixLogicalViewPlanNodeV1::RelationScan {
        node_id: scan_node,
        relation: input_relation.clone(),
    }];
    if !supported
        .predicate_expr
        .as_ref()
        .is_some_and(RowPredicateExpr::contains_or)
    {
        let predicates = supported
            .predicate_expr
            .as_ref()
            .map(RowPredicateExpr::leaf_predicates)
            .unwrap_or_default();
        for (index, predicate) in predicates.iter().enumerate() {
            let filter_node = if index == 0 {
                "filter_project_input".to_string()
            } else {
                format!("filter_project_input_{index}")
            };
            nodes.push(VelorixLogicalViewPlanNodeV1::Filter {
                node_id: filter_node.clone(),
                input: current_node,
                predicate: LogicalPlanPredicateV1 {
                    column: column_ref(&supported.input_relation_id, &predicate.column_id),
                    op: predicate.op,
                    literal: predicate.literal.clone(),
                },
            });
            current_node = filter_node;
        }
    }
    let project_node = "project_materialized_output".to_string();
    let mut columns = vec![key.clone()];
    columns.extend(
        supported
            .value_columns
            .iter()
            .map(|column| column_ref(&supported.input_relation_id, &column.input_column_id)),
    );
    nodes.push(VelorixLogicalViewPlanNodeV1::Project {
        node_id: project_node.clone(),
        input: current_node,
        columns,
        computed_columns: Vec::new(),
    });
    let mut output_input = project_node.clone();
    if let Some(top_k) = &supported.top_k {
        let top_k_node = "top_k_filter_project_materialized_output".to_string();
        nodes.push(VelorixLogicalViewPlanNodeV1::TopK {
            node_id: top_k_node.clone(),
            input: output_input,
            order_by: column_ref(&output_relation.relation_id, &top_k.order_output_column_id),
            descending: top_k.descending,
            limit: top_k.limit,
            offset: top_k.offset,
        });
        output_input = top_k_node;
    }
    nodes.push(VelorixLogicalViewPlanNodeV1::Output {
        node_id: "output_materialized_view".to_string(),
        input: output_input,
        relation: output_relation.clone(),
    });
    VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations: vec![input_relation],
        output_relation,
        nodes,
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: project_node,
            state_kind: LogicalPlanStateKindV1::Projection,
            key_columns: vec![key],
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::FilterProject { plan: supported },
    }
}

fn semi_anti_join_project_logical_plan(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
    supported: SupportedSemiAntiJoinProjectPlanV1,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let left_catalog = catalogs
        .iter()
        .find(|catalog| catalog.relation_schema.relation_id == supported.left_input_relation_id)
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "semi/anti join left input catalog is missing".to_string(),
        })?;
    let right_catalog = catalogs
        .iter()
        .find(|catalog| catalog.relation_schema.relation_id == supported.right_input_relation_id)
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "semi/anti join right input catalog is missing".to_string(),
        })?;
    let left_relation = logical_relation_from_catalog(left_catalog);
    let right_relation = logical_relation_from_catalog(right_catalog);
    let output_relation = logical_relation_from_schema(output_schema);
    let left_key = column_ref(
        &supported.left_input_relation_id,
        &supported.left_join_key_column_id,
    );
    let right_key = column_ref(
        &supported.right_input_relation_id,
        &supported.right_join_key_column_id,
    );
    let join_node_id = "semi_anti_join".to_string();
    let mut nodes = vec![
        VelorixLogicalViewPlanNodeV1::RelationScan {
            node_id: "scan_left".to_string(),
            relation: left_relation.clone(),
        },
        VelorixLogicalViewPlanNodeV1::RelationScan {
            node_id: "scan_right".to_string(),
            relation: right_relation.clone(),
        },
    ];
    nodes.push(match supported.join_kind {
        SupportedSemiAntiJoinKindV1::Semi => VelorixLogicalViewPlanNodeV1::SemiEquiJoin {
            node_id: join_node_id.clone(),
            left: "scan_left".to_string(),
            right: "scan_right".to_string(),
            left_key: left_key.clone(),
            right_key: right_key.clone(),
        },
        SupportedSemiAntiJoinKindV1::Anti => VelorixLogicalViewPlanNodeV1::AntiEquiJoin {
            node_id: join_node_id.clone(),
            left: "scan_left".to_string(),
            right: "scan_right".to_string(),
            left_key: left_key.clone(),
            right_key: right_key.clone(),
        },
    });
    let project_node_id = "project_materialized_output".to_string();
    let mut columns = vec![left_key.clone()];
    columns.extend(
        supported
            .projection
            .value_columns
            .iter()
            .map(|column| column_ref(&supported.left_input_relation_id, &column.input_column_id)),
    );
    nodes.push(VelorixLogicalViewPlanNodeV1::Project {
        node_id: project_node_id.clone(),
        input: join_node_id.clone(),
        columns,
        computed_columns: Vec::new(),
    });
    nodes.push(VelorixLogicalViewPlanNodeV1::Output {
        node_id: "output_materialized_view".to_string(),
        input: project_node_id,
        relation: output_relation.clone(),
    });
    Ok(VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations: vec![left_relation, right_relation],
        output_relation,
        nodes,
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: join_node_id,
            state_kind: LogicalPlanStateKindV1::JoinIndex,
            key_columns: vec![left_key, right_key],
            codec_version: match supported.join_kind {
                SupportedSemiAntiJoinKindV1::Semi => "velorix-native-semi-join-v1",
                SupportedSemiAntiJoinKindV1::Anti => "velorix-native-anti-join-v1",
            }
            .to_string(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::TwoInputSemiAntiJoinProject { plan: supported },
    })
}

fn logical_relation_from_catalog(catalog: &VelorixRelationCatalogV1) -> LogicalPlanRelationRef {
    LogicalPlanRelationRef {
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_name: catalog.relation_schema.relation_name.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
    }
}

fn logical_relation_from_schema(schema: &RelationSchema) -> LogicalPlanRelationRef {
    LogicalPlanRelationRef {
        relation_id: schema.relation_id.clone(),
        relation_name: schema.relation_name.clone(),
        relation_version: schema.relation_version.clone(),
        schema_fingerprint: schema.schema_fingerprint.clone(),
    }
}
fn column_ref(relation_id: &str, column_id: &str) -> LogicalPlanColumnRef {
    LogicalPlanColumnRef {
        relation_id: relation_id.to_string(),
        input_instance_id: None,
        column_id: column_id.to_string(),
    }
}

fn instance_column_ref(
    relation_id: &str,
    input_instance_id: &str,
    column_id: &str,
) -> LogicalPlanColumnRef {
    LogicalPlanColumnRef {
        relation_id: relation_id.to_string(),
        input_instance_id: Some(input_instance_id.to_string()),
        column_id: column_id.to_string(),
    }
}

fn invalid_logical_plan<T>(reason: impl Into<String>) -> Result<T, ViewPlanError> {
    Err(ViewPlanError::InvalidLogicalPlan {
        reason: reason.into(),
    })
}

pub fn validate_supported_three_input_inner_join_count_sql(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<SupportedThreeInputInnerJoinCountPlanV1, ViewPlanError> {
    validate_supported_three_input_inner_join_count_sql_with_policy(
        sql,
        catalogs,
        THREE_INPUT_ROOT_FIXED_RIGHT_RELATION_ID_JOIN_ORDER_V1,
    )
}

pub fn validate_supported_three_input_inner_join_count_sql_with_policy(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    join_order_policy_id: &str,
) -> Result<SupportedThreeInputInnerJoinCountPlanV1, ViewPlanError> {
    let (schema_version, persisted_join_order_policy_id) = match join_order_policy_id {
        THREE_INPUT_LEGACY_SQL_ENCOUNTER_JOIN_ORDER_V1 => (1, String::new()),
        THREE_INPUT_ROOT_FIXED_RIGHT_RELATION_ID_JOIN_ORDER_V1 => {
            (2, join_order_policy_id.to_string())
        }
        _ => return unsupported("three-input JOIN order policy is not supported"),
    };
    let [_, _, _] = catalogs else {
        return unsupported("three-input JOIN requires exactly three registered relations");
    };
    let mut relation_ids = BTreeSet::new();
    for catalog in catalogs {
        catalog.validate()?;
        if !relation_ids.insert(catalog.relation_schema.relation_id.as_str()) {
            return unsupported("three-input JOIN requires three distinct relations");
        }
        let adapter = crate::relation::supported_incremental_adapter_spec(
            &catalog.incremental_adapter.adapter_id,
        )
        .ok_or(RelationSchemaError::InvalidRelationSchema {
            field: "incremental_adapter.adapter_id",
        })?;
        if !matches!(
            adapter,
            SupportedIncrementalAdapterSpec::ScalarSumCount
                | SupportedIncrementalAdapterSpec::Generic
        ) {
            return unsupported("three-input JOIN requires a generic incremental input adapter");
        }
        if catalog.relation_schema.primary_key_column_ids.len() < 2 {
            return unsupported("three-input JOIN requires a composite primary key on every input");
        }
    }

    let query = parse_single_query(sql)?;
    validate_query_level_clauses(&query, false)?;
    let select = supported_plain_select_body(&query)?;
    validate_plain_select_clauses(select)?;
    if select.selection.is_some() || select.distinct.is_some() {
        return unsupported("three-input JOIN does not support WHERE or DISTINCT");
    }
    let [table] = select.from.as_slice() else {
        return unsupported("three-input JOIN requires one joined table expression");
    };
    let [first_join, second_join] = table.joins.as_slice() else {
        return unsupported("three-input JOIN requires exactly two binary join steps");
    };
    let root_table = n_way_registered_table_ref(&table.relation, "root")?;
    let root_catalog = n_way_catalog_for_table(&root_table, catalogs)?;
    let root_primary_key_column_ids = {
        let mut ids = root_catalog.relation_schema.primary_key_column_ids.clone();
        ids.sort();
        ids
    };
    let mut right_bindings = Vec::with_capacity(2);
    let mut aliases = BTreeSet::from([root_table.alias.to_ascii_lowercase()]);

    for (index, join) in [first_join, second_join].into_iter().enumerate() {
        if join.global {
            return unsupported("GLOBAL JOIN is not supported");
        }
        let constraint = match &join.join_operator {
            JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => constraint,
            _ => return unsupported("three-input JOIN supports INNER JOIN only"),
        };
        let right_table = n_way_registered_table_ref(&join.relation, "right")?;
        if !aliases.insert(right_table.alias.to_ascii_lowercase()) {
            return unsupported("three-input JOIN aliases must be distinct");
        }
        let right_catalog = n_way_catalog_for_table(&right_table, catalogs)?;
        if right_bindings
            .iter()
            .any(|(relation_id, _)| relation_id == &right_catalog.relation_schema.relation_id)
        {
            return unsupported("three-input JOIN must add exactly one new relation per step");
        }
        let JoinConstraint::On(on) = constraint else {
            return unsupported("three-input JOIN requires an ON equality for every step");
        };
        let mut pairs = Vec::new();
        for conjunct in join_on_conjuncts(on) {
            let Some((root_ref, right_ref)) =
                join_on_equality_refs(conjunct, &root_table.alias, &right_table.alias)?
            else {
                return unsupported("three-input JOIN does not support residual ON predicates");
            };
            let root_column = qualified_ref_catalog_column(&root_ref, root_catalog)?;
            let right_column = qualified_ref_catalog_column(&right_ref, right_catalog)?;
            pairs.push((root_column, right_column));
        }
        pairs.sort_by(|(left_a, _), (left_b, _)| left_a.column_id.cmp(&left_b.column_id));
        if pairs.len() != root_primary_key_column_ids.len()
            || pairs
                .iter()
                .map(|(left, _)| left.column_id.as_str())
                .ne(root_primary_key_column_ids.iter().map(String::as_str))
        {
            return unsupported("three-input JOIN must cover every root primary-key position once");
        }
        let right_pk = &right_catalog.relation_schema.primary_key_column_ids;
        let mut seen_right = BTreeSet::new();
        let mut permutation = Vec::with_capacity(pairs.len());
        for (root_column, right_column) in &pairs {
            if root_column.nullable
                || right_column.nullable
                || root_column.physical_arrow_type != right_column.physical_arrow_type
            {
                return unsupported(
                    "three-input JOIN key pairs must be non-null and have exact Arrow types",
                );
            }
            let Some(position) = right_pk
                .iter()
                .position(|column_id| column_id == &right_column.column_id)
            else {
                return unsupported("three-input JOIN must use the complete right primary key");
            };
            if !seen_right.insert(position) {
                return unsupported("three-input JOIN primary-key mapping must be bijective");
            }
            permutation.push(position);
        }
        if seen_right.len() != right_pk.len() || right_pk.len() != root_primary_key_column_ids.len()
        {
            return unsupported("three-input JOIN primary-key mapping must be bijective");
        }
        right_bindings.push((
            right_catalog.relation_schema.relation_id.clone(),
            permutation,
        ));
        debug_assert_eq!(right_bindings.len(), index + 1);
    }
    if join_order_policy_id == THREE_INPUT_ROOT_FIXED_RIGHT_RELATION_ID_JOIN_ORDER_V1 {
        right_bindings.sort_by(|left, right| left.0.cmp(&right.0));
    }
    let mut ordered_input_relation_ids = vec![root_catalog.relation_schema.relation_id.clone()];
    let mut root_to_input_pk_permutations = vec![(0..root_primary_key_column_ids.len()).collect()];
    for (relation_id, permutation) in right_bindings {
        ordered_input_relation_ids.push(relation_id);
        root_to_input_pk_permutations.push(permutation);
    }
    if ordered_input_relation_ids.len() != 3
        || catalogs.iter().any(|catalog| {
            !ordered_input_relation_ids
                .iter()
                .any(|id| id == &catalog.relation_schema.relation_id)
        })
    {
        return unsupported("three-input JOIN must use every registered relation exactly once");
    }

    let expected_projection_len = root_primary_key_column_ids.len() + 1;
    if select.projection.len() != expected_projection_len {
        return unsupported("three-input JOIN must project the root primary key and count(*)");
    }
    let mut output_key_column_ids = Vec::with_capacity(root_primary_key_column_ids.len());
    for (item, column_id) in select
        .projection
        .iter()
        .zip(root_primary_key_column_ids.iter())
    {
        let column = catalog_column_by_id(root_catalog, column_id)?;
        if !select_item_references_qualified_column(item, &root_table.alias, column) {
            return unsupported(
                "three-input JOIN key projection must follow canonical root PK order",
            );
        }
        output_key_column_ids.push(select_item_alias_or_default(item, &column.name)?);
    }
    let count_item = select.projection.last().expect("projection is non-empty");
    let count = validate_join_count_select_item(
        count_item,
        &root_table.alias,
        root_catalog,
        "",
        root_catalog,
    )?;
    if count.function != LogicalPlanAggregateFunctionV1::Count
        || count.input_column_id.is_some()
        || count.input_expression.is_some()
        || count.input_relation_side.is_some()
        || select_item_function_filter(count_item).is_some()
    {
        return unsupported("three-input JOIN supports exactly count(*)");
    }
    let GroupByExpr::Expressions(group_by, modifiers) = &select.group_by else {
        return unsupported("three-input JOIN requires explicit GROUP BY root primary key");
    };
    if !modifiers.is_empty() || group_by.len() != root_primary_key_column_ids.len() {
        return unsupported("three-input JOIN must group by every root primary-key column");
    }
    for (expression, column_id) in group_by.iter().zip(root_primary_key_column_ids.iter()) {
        let reference = qualified_column_ref(expression)?;
        let column = qualified_ref_catalog_column(&reference, root_catalog)?;
        if !identifier_eq(&reference.qualifier, &root_table.alias) || &column.column_id != column_id
        {
            return unsupported("three-input JOIN GROUP BY must follow canonical root PK order");
        }
    }

    Ok(SupportedThreeInputInnerJoinCountPlanV1 {
        schema_version,
        join_order_policy_id: persisted_join_order_policy_id,
        ordered_input_relation_ids,
        root_primary_key_column_ids,
        output_key_column_ids,
        count_output_column_id: count.output_column_id,
        join_key_codec_id: COMPOSITE_PK_POSITIONAL_JSON_ARRAY_JOIN_KEY_CODEC_V1.to_string(),
        root_to_input_pk_permutations,
    })
}

fn n_way_registered_table_ref(
    factor: &TableFactor,
    side: &'static str,
) -> Result<SqlTableRef, ViewPlanError> {
    let TableFactor::Table { alias: Some(_), .. } = factor else {
        return unsupported("three-input JOIN requires an explicit alias for every relation");
    };
    registered_table_ref(factor, side)
}

fn n_way_catalog_for_table<'a>(
    table: &SqlTableRef,
    catalogs: &'a [VelorixRelationCatalogV1],
) -> Result<&'a VelorixRelationCatalogV1, ViewPlanError> {
    let matches = catalogs
        .iter()
        .filter(|catalog| {
            identifier_eq(&table.name, &catalog.relation_schema.relation_id)
                || identifier_eq(&table.name, &catalog.relation_schema.relation_name)
        })
        .collect::<Vec<_>>();
    let [catalog] = matches.as_slice() else {
        return unsupported("three-input JOIN table must resolve to one registered relation");
    };
    Ok(*catalog)
}

/// Admits the bounded V1 lowering of correlated EXISTS/NOT EXISTS to ordinary
/// binary semi/anti joins. The persisted plan intentionally contains no
/// subquery-specific runtime node.
pub fn validate_supported_semi_anti_join_sql(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<SupportedSemiAntiJoinProjectPlanV1, ViewPlanError> {
    let [first_catalog, second_catalog] = catalogs else {
        return unsupported("semi/anti join SQL requires exactly two input relations");
    };
    for catalog in catalogs {
        catalog.validate()?;
        let adapter = crate::relation::supported_incremental_adapter_spec(
            &catalog.incremental_adapter.adapter_id,
        )
        .ok_or(RelationSchemaError::InvalidRelationSchema {
            field: "incremental_adapter.adapter_id",
        })?;
        if !matches!(
            adapter,
            SupportedIncrementalAdapterSpec::ScalarSumCount
                | SupportedIncrementalAdapterSpec::Generic
        ) {
            return unsupported("semi/anti join SQL requires scalar or generic inputs");
        }
    }

    let query = parse_single_query(sql)?;
    validate_query_level_clauses(&query, false)?;
    let select = supported_plain_select_body(&query)?;
    validate_plain_select_clauses(select)?;
    if select.distinct.is_some() || !group_by_is_empty(&select.group_by) {
        return unsupported("semi/anti join V1 does not support DISTINCT or GROUP BY");
    }
    let [outer_from] = select.from.as_slice() else {
        return unsupported("semi/anti join outer query requires one registered relation");
    };
    if !outer_from.joins.is_empty() {
        return unsupported("semi/anti join outer query must not contain an explicit JOIN");
    }
    let outer_table = registered_table_ref(&outer_from.relation, "outer")?;
    let left_catalog = catalog_for_table(&outer_table, first_catalog, second_catalog)?;
    let right_catalog =
        if left_catalog.relation_schema.relation_id == first_catalog.relation_schema.relation_id {
            second_catalog
        } else {
            first_catalog
        };
    if left_catalog.relation_schema.relation_id == right_catalog.relation_schema.relation_id {
        return unsupported("semi/anti join V1 requires two distinct registered relations");
    }

    // Phase 7.3: `WHERE x IN (SELECT y FROM r)` is decorrelated into the
    // correlated EXISTS form `WHERE EXISTS (SELECT y FROM r WHERE r.y = x)`
    // and reuses the semi/anti join machinery. Only the WHERE-only boolean
    // context is admitted; IN inside SELECT/CASE/OR fails closed.
    let mut in_form = false;
    let mut in_negated = false;
    let select = if let Some(Expr::InSubquery {
        expr,
        subquery,
        negated,
    }) = select.selection.as_ref()
    {
        if !matches!(
            expr.as_ref(),
            Expr::Identifier(_) | Expr::CompoundIdentifier(_)
        ) {
            return unsupported(
                "IN-subquery decorrelation requires a direct column probe expression",
            );
        }
        let Some(inner_select) = decorrelated_in_subquery_select(expr, subquery)? else {
            return unsupported(
                "IN-subquery decorrelation requires a single-column subquery over one registered relation",
            );
        };
        in_form = true;
        in_negated = *negated;
        let mut rewritten = (*select).clone();
        rewritten.selection = Some(Expr::Exists {
            subquery: inner_select,
            negated: *negated,
        });
        rewritten
    } else {
        (*select).clone()
    };
    let Some(Expr::Exists { subquery, negated }) = select.selection.as_ref() else {
        return unsupported(
            "semi/anti join V1 requires one correlated EXISTS or NOT EXISTS predicate",
        );
    };
    validate_query_level_clauses(subquery, false)?;
    let inner = supported_plain_select_body(subquery)?;
    validate_plain_select_clauses(inner)?;
    if inner.distinct.is_some() || !group_by_is_empty(&inner.group_by) {
        return unsupported("EXISTS/NOT EXISTS V1 subquery does not support DISTINCT or GROUP BY");
    }
    if in_form {
        if !matches!(
            inner.projection.as_slice(),
            [SelectItem::UnnamedExpr(Expr::Identifier(_))
                | SelectItem::UnnamedExpr(Expr::CompoundIdentifier(_))]
        ) {
            return unsupported(
                "IN-subquery decorrelation requires exactly one inner column projection",
            );
        }
    } else if !matches!(
        inner.projection.as_slice(),
        [SelectItem::UnnamedExpr(Expr::Value(value))]
            if !matches!(value.value, SqlValue::Null)
    ) {
        return unsupported("EXISTS/NOT EXISTS V1 subquery must project one non-null literal");
    }
    let [inner_from] = inner.from.as_slice() else {
        return unsupported("EXISTS/NOT EXISTS V1 subquery requires one registered relation");
    };
    if !inner_from.joins.is_empty() {
        return unsupported("EXISTS/NOT EXISTS V1 subquery must not contain a JOIN");
    }
    let inner_table = registered_table_ref(&inner_from.relation, "inner")?;
    let resolved_inner_catalog = catalog_for_table(&inner_table, first_catalog, second_catalog)?;
    if resolved_inner_catalog.relation_schema.relation_id
        != right_catalog.relation_schema.relation_id
    {
        return unsupported(
            "EXISTS/NOT EXISTS subquery must reference the other registered relation",
        );
    }
    let Some(Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    }) = inner.selection.as_ref()
    else {
        return unsupported("EXISTS/NOT EXISTS V1 requires one correlated equality predicate");
    };
    let left_ref = qualified_column_ref(left)?;
    let right_ref = qualified_column_ref(right)?;
    let (outer_ref, inner_ref) =
        orient_join_refs(left_ref, right_ref, &outer_table.alias, &inner_table.alias)?;
    let left_key = qualified_ref_catalog_column(&outer_ref, left_catalog)?;
    let right_key = qualified_ref_catalog_column(&inner_ref, right_catalog)?;
    if in_negated && (left_key.nullable || right_key.nullable) {
        return unsupported(
            "EXISTS/NOT EXISTS and NOT IN correlation must equate identical non-null scalar columns; nullable NOT IN is rejected until null-aware anti-join semantics exist",
        );
    }
    if !supported_scalar_join_key_atom(&left_key.physical_arrow_type)
        || !supported_scalar_join_key_atom(&right_key.physical_arrow_type)
        || left_key.physical_arrow_type != right_key.physical_arrow_type
        || left_key.logical_type != right_key.logical_type
    {
        return unsupported(
            "EXISTS/NOT EXISTS correlation must equate identical non-null scalar columns",
        );
    }

    let left_pk = catalog_primary_key_column(left_catalog)?;
    let projection = validate_filter_project_projection(
        &select,
        left_catalog,
        left_pk,
        Some(&outer_table.alias),
        None,
    )?;
    if projection.value_columns.is_empty() {
        return unsupported(
            "semi/anti join materialized output requires at least one value column",
        );
    }
    Ok(SupportedSemiAntiJoinProjectPlanV1 {
        schema_version: 1,
        join_kind: if *negated {
            SupportedSemiAntiJoinKindV1::Anti
        } else {
            SupportedSemiAntiJoinKindV1::Semi
        },
        left_input_relation_id: left_catalog.relation_schema.relation_id.clone(),
        right_input_relation_id: right_catalog.relation_schema.relation_id.clone(),
        left_join_key_column_id: left_key.column_id.clone(),
        right_join_key_column_id: right_key.column_id.clone(),
        projection: SupportedFilterProjectPlan {
            typed_value_columns: Vec::new(),
            input_relation_id: left_catalog.relation_schema.relation_id.clone(),
            key_column_id: left_pk.column_id.clone(),
            output_key_column_id: projection.output_key_column_id,
            // Phase 7.4: when the correlation key is not the left primary
            // key, the projected output key must be derived from the left
            // PK value carried by the left delta.
            output_key_input_column_id: projection.output_key_input_column_id.or_else(|| {
                (left_key.column_id != left_pk.column_id).then(|| left_pk.column_id.clone())
            }),
            value_columns: projection
                .value_columns
                .into_iter()
                .map(|column| SupportedProjectionColumn {
                    input_column_id: column.input_column_id,
                    output_column_id: column.output_column_id,
                    expression: column.expression,
                })
                .collect(),
            predicate_expr: None,
            top_k: None,
        },
    })
}

pub fn validate_supported_join_view_sql(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<SupportedJoinViewPlan, ViewPlanError> {
    let (left_catalog, right_catalog) =
        match catalogs {
            [catalog] => (catalog, catalog),
            [left_catalog, right_catalog] => (left_catalog, right_catalog),
            _ => return unsupported(
                "join view SQL currently requires one self-joined or two distinct input relations",
            ),
        };
    for catalog in [left_catalog, right_catalog] {
        catalog.validate()?;
        let adapter = crate::relation::supported_incremental_adapter_spec(
            &catalog.incremental_adapter.adapter_id,
        )
        .ok_or(RelationSchemaError::InvalidRelationSchema {
            field: "incremental_adapter.adapter_id",
        })?;
        if !matches!(
            adapter,
            SupportedIncrementalAdapterSpec::ScalarSumCount
                | SupportedIncrementalAdapterSpec::Generic
        ) {
            return unsupported(
                "join view SQL currently supports scalar sum/count or generic inputs",
            );
        }
    }

    let query = parse_single_query(sql)?;
    let cte_sources = validate_join_cte_sources(&query, left_catalog, right_catalog)?;
    validate_query_level_clauses_with_options(&query, !cte_sources.is_empty(), true)?;
    let select = supported_plain_select_body(&query)?;
    validate_plain_select_clauses_allow_having(select)?;

    let JoinSqlBindings {
        left_catalog,
        right_catalog,
        join_kind,
        left_alias,
        right_alias,
        left_source_selection,
        right_source_selection,
        on_residual_selection,
        join_key_pairs,
    } = validate_two_input_join(select, left_catalog, right_catalog, &cte_sources)?;
    let join_key_domain = validate_join_key_pairs_for_incremental_state(
        left_catalog,
        right_catalog,
        join_kind,
        &join_key_pairs,
    )?;
    let Some(&(left_key, right_key)) = join_key_pairs.first() else {
        return unsupported("JOIN ON must contain a key equality");
    };
    let composite_equality = (join_key_pairs.len() > 1).then(|| SupportedCompositeJoinEqualityV1 {
        schema_version: 1,
        additional_pairs: join_key_pairs
            .iter()
            .skip(1)
            .map(|(left, right)| SupportedJoinKeyPairV1 {
                left_column_id: left.column_id.clone(),
                right_column_id: right.column_id.clone(),
            })
            .collect(),
    });
    if left_catalog.relation_schema.relation_id == right_catalog.relation_schema.relation_id {
        return validate_supported_self_join_global_count(
            &query,
            select,
            left_catalog,
            join_kind,
            &left_alias,
            &right_alias,
            left_source_selection.as_ref(),
            right_source_selection.as_ref(),
            on_residual_selection.as_ref(),
            left_key,
            right_key,
            composite_equality,
            join_key_domain,
            &cte_sources,
        );
    }
    let projection = validate_join_projection(
        select,
        join_kind,
        &right_alias,
        right_catalog,
        right_key,
        &left_alias,
        left_catalog,
        left_key,
    )?;
    let selection_context = JoinSelectionContext {
        select,
        left_alias: &left_alias,
        left_catalog,
        left_key,
        left_value: projection.sum_value_column,
        right_alias: &right_alias,
        right_catalog,
    };
    let on_residual_predicate =
        validate_join_on_residual_selection(on_residual_selection.as_ref(), &selection_context)?;
    let predicate_expr = combine_join_predicate_exprs(
        combine_join_predicate_exprs(
            validate_join_cte_selection(&cte_sources, &selection_context)?,
            combine_join_predicate_exprs(
                validate_join_derived_table_selection(
                    left_source_selection.as_ref(),
                    right_source_selection.as_ref(),
                    &selection_context,
                )?,
                combine_join_predicate_exprs(
                    on_residual_predicate.clone(),
                    validate_join_selection(&selection_context)?,
                ),
            ),
        ),
        projection.shared_aggregate_filter_expr.clone(),
    );
    let predicates = predicate_expr
        .as_ref()
        .map(JoinPredicateExpr::leaf_predicates)
        .unwrap_or_default();
    let mut right_value_predicates = predicates.clone();
    for predicate_expr in projection.aggregate_filter_exprs.values() {
        right_value_predicates.extend(predicate_expr.leaf_predicates());
    }
    let mut right_value_column_ids =
        join_right_value_column_ids(&right_value_predicates, right_catalog, right_key)?;
    let mut seen_right_values = right_value_column_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    for aggregate in &projection.aggregate_outputs {
        if aggregate.input_relation_side == Some(SupportedAggregateInputRelationSide::Right) {
            let Some(column_id) = &aggregate.input_column_id else {
                return unsupported("JOIN right aggregate input column is missing");
            };
            if seen_right_values.insert(column_id.clone()) {
                right_value_column_ids.push(column_id.clone());
            }
        }
    }
    for predicate_expr in predicate_expr
        .iter()
        .chain(projection.aggregate_filter_exprs.values())
    {
        for column_id in join_scalar_int64_predicate_expr_column_ids(
            predicate_expr,
            &right_catalog.relation_schema.relation_id,
        ) {
            if column_id != right_key.column_id && seen_right_values.insert(column_id.clone()) {
                right_value_column_ids.push(column_id);
            }
        }
    }
    let right_value_column_id = right_value_column_ids.first().cloned();
    validate_join_group_by_key(
        select,
        &left_alias,
        left_catalog,
        &right_alias,
        right_catalog,
        &projection.group_key_relation_id,
        &projection.group_key_column_id,
        &projection.output_key_column_id,
        projection.output_key_catalog,
        projection.coalesced_full_join_key,
        left_key,
        right_key,
    )?;
    let having_context = JoinHavingBindingContext {
        select,
        left_alias: &left_alias,
        left_catalog,
        left_key,
        left_value: projection.sum_value_column,
        right_alias: &right_alias,
        right_catalog,
        aggregate_outputs: &projection.aggregate_outputs,
        shared_aggregate_filter_expr: projection.shared_aggregate_filter_expr.as_ref(),
        aggregate_filter_exprs: &projection.aggregate_filter_exprs,
    };
    let having_expr = validate_join_having(select, &having_context)?;
    let having = having_expr
        .as_ref()
        .and_then(|expr| expr.leaf_predicates().into_iter().next());
    let top_k = validate_aggregate_top_k(
        &query,
        &projection.aggregate_outputs,
        Some(AggregateTopKBindingContext::Join {
            select,
            left_alias: &left_alias,
            left_catalog,
            left_key,
            left_value: projection.sum_value_column,
            right_alias: &right_alias,
            right_catalog,
            shared_aggregate_filter_expr: projection.shared_aggregate_filter_expr.as_ref(),
            aggregate_filter_exprs: &projection.aggregate_filter_exprs,
        }),
        &[
            projection.output_key_column_id.as_str(),
            projection.group_key_column_id.as_str(),
        ],
        false,
    )?;
    validate_left_join_scope(
        join_kind,
        &projection,
        left_catalog,
        left_key,
        &on_residual_predicate,
        left_source_selection.is_some()
            || cte_sources.iter().any(|source| {
                source.relation_id == left_catalog.relation_schema.relation_id
                    && source.selection.is_some()
            }),
        right_source_selection.is_some()
            || cte_sources.iter().any(|source| {
                source.relation_id == right_catalog.relation_schema.relation_id
                    && source.selection.is_some()
            }),
    )?;

    Ok(SupportedJoinViewPlan {
        left_input_relation_id: left_catalog.relation_schema.relation_id.clone(),
        right_input_relation_id: right_catalog.relation_schema.relation_id.clone(),
        left_input_instance_id: None,
        right_input_instance_id: None,
        join_kind,
        left_join_key_column_id: left_key.column_id.clone(),
        right_join_key_column_id: right_key.column_id.clone(),
        composite_equality,
        join_key_domain,
        aggregate_output_identity: None,
        group_key_relation_id: projection.group_key_relation_id,
        group_key_column_id: projection.group_key_column_id,
        output_key_column_id: projection.output_key_column_id,
        sum_value_relation_id: left_catalog.relation_schema.relation_id.clone(),
        sum_value_column_id: projection.sum_value_column.column_id.clone(),
        right_value_column_id,
        right_value_column_ids,
        aggregate_outputs: projection.aggregate_outputs,
        aggregate_filter_exprs: projection.aggregate_filter_exprs,
        predicate: predicates.first().cloned(),
        predicates: predicates.into_iter().skip(1).collect(),
        predicate_expr,
        having,
        having_expr,
        top_k,
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_supported_self_join_global_count(
    query: &Query,
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    join_kind: SupportedJoinKind,
    left_alias: &str,
    right_alias: &str,
    left_source_selection: Option<&Expr>,
    right_source_selection: Option<&Expr>,
    on_residual_selection: Option<&Expr>,
    left_key: &RelationColumnV1,
    right_key: &RelationColumnV1,
    composite_equality: Option<SupportedCompositeJoinEqualityV1>,
    join_key_domain: Option<SupportedJoinKeyDomainV1>,
    cte_sources: &[CteSource],
) -> Result<SupportedJoinViewPlan, ViewPlanError> {
    if join_kind != SupportedJoinKind::Inner
        || composite_equality.is_some()
        || join_key_domain != Some(SupportedJoinKeyDomainV1::NonPrimaryNonNullScalarV1)
        || !cte_sources.is_empty()
        || left_source_selection.is_some()
        || right_source_selection.is_some()
        || on_residual_selection.is_some()
        || select.selection.is_some()
        || select.distinct.is_some()
        || !group_by_is_empty(&select.group_by)
        || select.having.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
    {
        return unsupported(
            "self-JOIN currently supports only one non-primary non-null scalar equality and global count(*) without predicates, grouping, or Top-K",
        );
    }
    let [count_item] = select.projection.as_slice() else {
        return unsupported("self-JOIN currently supports exactly global count(*)");
    };
    let count =
        validate_join_count_select_item(count_item, left_alias, catalog, right_alias, catalog)?;
    if count.function != LogicalPlanAggregateFunctionV1::Count
        || count.input_column_id.is_some()
        || count.input_expression.is_some()
        || count.input_relation_side.is_some()
        || select_item_function_filter(count_item).is_some()
    {
        return unsupported("self-JOIN currently supports exactly global count(*)");
    }
    let sum_value_column = count_only_runtime_value_column(catalog, std::slice::from_ref(&count))?;
    Ok(SupportedJoinViewPlan {
        left_input_relation_id: catalog.relation_schema.relation_id.clone(),
        right_input_relation_id: catalog.relation_schema.relation_id.clone(),
        left_input_instance_id: Some(LEFT_JOIN_INPUT_INSTANCE_ID_V1.to_string()),
        right_input_instance_id: Some(RIGHT_JOIN_INPUT_INSTANCE_ID_V1.to_string()),
        join_kind,
        left_join_key_column_id: left_key.column_id.clone(),
        right_join_key_column_id: right_key.column_id.clone(),
        composite_equality: None,
        join_key_domain,
        aggregate_output_identity: Some(SupportedAggregateOutputIdentity::Singleton),
        group_key_relation_id: catalog.relation_schema.relation_id.clone(),
        group_key_column_id: left_key.column_id.clone(),
        output_key_column_id: String::new(),
        sum_value_relation_id: catalog.relation_schema.relation_id.clone(),
        sum_value_column_id: sum_value_column.column_id.clone(),
        right_value_column_id: None,
        right_value_column_ids: Vec::new(),
        aggregate_outputs: vec![count],
        aggregate_filter_exprs: BTreeMap::new(),
        predicate: None,
        predicates: Vec::new(),
        predicate_expr: None,
        having: None,
        having_expr: None,
        top_k: None,
    })
}

fn supported_plain_select_allow_identity_cte_and_top_k<'a>(
    query: &'a Query,
    catalog: &VelorixRelationCatalogV1,
) -> Result<(&'a Select, Option<CteSource>), ViewPlanError> {
    let cte_source = validate_cte_source(query, catalog)?;
    validate_query_level_clauses_with_options(query, cte_source.is_some(), true)?;
    Ok((supported_plain_select_body(query)?, cte_source))
}

fn validate_query_level_clauses(query: &Query, allow_with: bool) -> Result<(), ViewPlanError> {
    validate_query_level_clauses_with_options(query, allow_with, false)
}

fn validate_query_level_clauses_with_options(
    query: &Query,
    allow_with: bool,
    allow_limit: bool,
) -> Result<(), ViewPlanError> {
    if (!allow_with && query.with.is_some())
        || (!allow_limit && (query.limit_clause.is_some() || query.fetch.is_some()))
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return unsupported("query-level clauses are not supported for materialized view planning");
    }
    Ok(())
}

fn supported_plain_select_body(query: &Query) -> Result<&Select, ViewPlanError> {
    match query.body.as_ref() {
        SetExpr::Select(select) => Ok(select),
        SetExpr::Query(inner) => {
            validate_query_level_clauses(inner, false)?;
            supported_plain_select_body(inner)
        }
        _ => unsupported("set operations, VALUES, and nested queries are not supported"),
    }
}

#[derive(Clone, Debug)]
struct CteSource {
    alias: String,
    relation_id: String,
    selection: Option<Expr>,
    projected_column_ids: Option<BTreeSet<String>>,
    projection: Option<SourceProjection>,
}

fn validate_cte_source(
    query: &Query,
    catalog: &VelorixRelationCatalogV1,
) -> Result<Option<CteSource>, ViewPlanError> {
    let Some(with) = &query.with else {
        return Ok(None);
    };
    if with.recursive || with.cte_tables.len() != 1 {
        return unsupported("only one non-recursive identity CTE is supported");
    }
    let cte = &with.cte_tables[0];
    if !cte.alias.columns.is_empty() || cte.from.is_some() || cte.materialized.is_some() {
        return unsupported(
            "identity CTE column aliases and materialization hints are unsupported",
        );
    }
    validate_query_level_clauses(&cte.query, false)?;
    let cte_select = supported_plain_select_body(&cte.query)?;
    let source_alias = validate_from_relation(cte_select, catalog)?;
    let projection = validate_simple_source_projection(
        &cte_select.projection,
        catalog,
        source_alias.as_deref(),
    )?;
    if !group_by_is_empty(&cte_select.group_by) {
        return unsupported("CTE source must not aggregate");
    }
    Ok(Some(CteSource {
        alias: cte.alias.name.value.clone(),
        relation_id: catalog.relation_schema.relation_id.clone(),
        selection: normalize_source_selection(
            cte_select.selection.as_ref(),
            source_alias.as_deref(),
        ),
        projected_column_ids: projection
            .as_ref()
            .map(|projection| projection.projected_column_ids.clone()),
        projection,
    }))
}

fn validate_join_cte_sources(
    query: &Query,
    left_catalog: &VelorixRelationCatalogV1,
    right_catalog: &VelorixRelationCatalogV1,
) -> Result<Vec<CteSource>, ViewPlanError> {
    let Some(with) = &query.with else {
        return Ok(Vec::new());
    };
    if with.recursive || with.cte_tables.is_empty() || with.cte_tables.len() > 2 {
        return unsupported("one or two non-recursive identity CTEs are supported");
    }
    let mut aliases = BTreeSet::new();
    let mut sources = Vec::with_capacity(with.cte_tables.len());
    for cte in &with.cte_tables {
        if !aliases.insert(cte.alias.name.value.to_ascii_lowercase()) {
            return unsupported("identity CTE aliases must be unique");
        }
        if !cte.alias.columns.is_empty() || cte.from.is_some() || cte.materialized.is_some() {
            return unsupported(
                "identity CTE column aliases and materialization hints are unsupported",
            );
        }
        validate_query_level_clauses(&cte.query, false)?;
        let cte_select = supported_plain_select_body(&cte.query)?;
        let table = validate_join_cte_from_relation(cte_select, left_catalog, right_catalog)?;
        let catalog = catalog_for_table(&table, left_catalog, right_catalog)?;
        if !cte_projection_is_identity(&cte_select.projection, catalog)
            || !group_by_is_empty(&cte_select.group_by)
        {
            return unsupported(
                "CTE source must be SELECT * or all registered columns FROM the registered relation",
            );
        }
        sources.push(CteSource {
            alias: cte.alias.name.value.clone(),
            relation_id: catalog.relation_schema.relation_id.clone(),
            selection: normalize_source_selection(
                cte_select.selection.as_ref(),
                Some(&table.alias),
            ),
            projected_column_ids: None,
            projection: None,
        });
    }
    Ok(sources)
}

fn validate_join_cte_from_relation(
    select: &Select,
    left_catalog: &VelorixRelationCatalogV1,
    right_catalog: &VelorixRelationCatalogV1,
) -> Result<SqlTableRef, ViewPlanError> {
    let [table] = select.from.as_slice() else {
        return unsupported("CTE source must reference exactly one registered relation");
    };
    if !table.joins.is_empty() {
        return unsupported("joins are not supported inside identity CTE sources");
    }
    let table = table_ref(&table.relation, "CTE source")?;
    catalog_for_table(&table, left_catalog, right_catalog)?;
    Ok(table)
}

fn cte_projection_is_identity(
    projection: &[SelectItem],
    catalog: &VelorixRelationCatalogV1,
) -> bool {
    if matches!(projection, [SelectItem::Wildcard(_)]) {
        return true;
    }
    let columns = &catalog.relation_schema.columns;
    if projection.len() != columns.len() {
        return false;
    }
    projection
        .iter()
        .zip(columns)
        .all(|(item, column)| match item {
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                identifier_eq(ident.value.as_str(), column.name.as_str())
                    || identifier_eq(ident.value.as_str(), column.column_id.as_str())
            }
            _ => false,
        })
}

#[derive(Clone, Debug)]
struct SourceProjection {
    projected_column_ids: BTreeSet<String>,
    columns: Vec<SourceProjectionColumn>,
}

#[derive(Clone, Debug)]
struct SourceProjectionColumn {
    output_name: String,
    input_column_id: String,
}

fn validate_simple_source_projection(
    projection: &[SelectItem],
    catalog: &VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
) -> Result<Option<SourceProjection>, ViewPlanError> {
    if matches!(projection, [SelectItem::Wildcard(_)]) {
        return Ok(None);
    }
    let mut projected_column_ids = BTreeSet::new();
    let mut output_names = BTreeSet::new();
    let mut columns = Vec::with_capacity(projection.len());
    for item in projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
            _ => return unsupported("source projection must use direct registered columns"),
        };
        let Some(column) = expression_column(expr, catalog, relation_alias) else {
            if alias.is_some() {
                return unsupported(
                    "source projection aliases must map directly to registered columns",
                );
            }
            return unsupported("source projection must use direct registered columns");
        };
        let output_name = alias.unwrap_or(column.name.as_str()).to_string();
        if let Some(alias) = alias {
            if !column_identifier_eq(column, alias)
                && catalog
                    .relation_schema
                    .columns
                    .iter()
                    .any(|catalog_column| column_identifier_eq(catalog_column, alias))
            {
                return unsupported(
                    "source projection aliases must not shadow another registered column",
                );
            }
        }
        if !output_names.insert(output_name.to_ascii_lowercase()) {
            return unsupported("source projection output names must be unique");
        }
        if !projected_column_ids.insert(column.column_id.clone()) {
            return unsupported("source projection column ids must be unique");
        }
        columns.push(SourceProjectionColumn {
            output_name,
            input_column_id: column.column_id.clone(),
        });
    }
    Ok(Some(SourceProjection {
        projected_column_ids,
        columns,
    }))
}

fn validate_aggregate_source_projection(
    projection: Option<&SourceProjection>,
    group_keys: &[SupportedGroupKey],
    aggregate_outputs: &[SupportedAggregateOutput],
) -> Result<(), ViewPlanError> {
    let Some(projection) = projection else {
        return Ok(());
    };
    let projected_column_ids = &projection.projected_column_ids;
    for group_key in group_keys {
        let referenced_columns = group_key.input_column_id.iter().cloned().chain(
            group_key
                .expression
                .as_ref()
                .into_iter()
                .flat_map(supported_projection_expr_column_ids),
        );
        if referenced_columns
            .into_iter()
            .any(|column_id| !projected_column_ids.contains(&column_id))
        {
            return unsupported(if group_keys.len() == 1 {
                "aggregate source projection must include the group key column"
            } else {
                "aggregate source projection must include group key columns"
            });
        }
    }
    for output in aggregate_outputs {
        if let Some(input_column_id) = &output.input_column_id {
            if !projected_column_ids.contains(input_column_id) {
                return unsupported(
                    "aggregate source projection must include aggregate input columns",
                );
            }
        }
    }
    Ok(())
}

fn group_by_is_empty(group_by: &GroupByExpr) -> bool {
    matches!(group_by, GroupByExpr::Expressions(expressions, modifiers) if expressions.is_empty() && modifiers.is_empty())
}

fn validate_plain_select_clauses(select: &Select) -> Result<(), ViewPlanError> {
    validate_plain_select_clauses_with_having(select, false)
}

fn validate_plain_select_clauses_allow_qualify(select: &Select) -> Result<(), ViewPlanError> {
    validate_plain_select_clauses_with_options(select, false, false, true)
}

fn validate_plain_select_clauses_allow_having(select: &Select) -> Result<(), ViewPlanError> {
    validate_plain_select_clauses_with_having(select, true)
}

fn validate_plain_select_clauses_allow_having_and_distinct_on(
    select: &Select,
) -> Result<(), ViewPlanError> {
    validate_plain_select_clauses_with_options(select, true, true, false)
}

fn validate_plain_select_clauses_with_having(
    select: &Select,
    allow_having: bool,
) -> Result<(), ViewPlanError> {
    validate_plain_select_clauses_with_options(select, allow_having, false, false)
}

fn validate_plain_select_clauses_with_options(
    select: &Select,
    allow_having: bool,
    allow_distinct_on: bool,
    allow_qualify: bool,
) -> Result<(), ViewPlanError> {
    if (!allow_distinct_on && matches!(select.distinct, Some(Distinct::On(_))))
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || (!allow_having && select.having.is_some())
        || !select.named_window.is_empty()
        || (!allow_qualify && select.qualify.is_some())
        || select.value_table_mode.is_some()
    {
        return unsupported("only plain SELECT/FROM/GROUP BY sum/count views are supported");
    }
    Ok(())
}

fn validate_distinct_on_group_key(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    relation_alias: Option<&str>,
    output_key_column_id: &str,
) -> Result<(), ViewPlanError> {
    let Some(Distinct::On(expressions)) = &select.distinct else {
        return Ok(());
    };
    let [expression] = expressions.as_slice() else {
        return unsupported("DISTINCT ON must reference exactly the group key");
    };
    if expression_is_first_select_projection_ordinal(expression)
        || expression_references_column(expression, key_column, relation_alias)
        || expression_references_unambiguous_output_alias(expression, catalog, output_key_column_id)
    {
        Ok(())
    } else {
        unsupported("DISTINCT ON must reference the group key")
    }
}

fn validate_from_relation(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
) -> Result<Option<String>, ViewPlanError> {
    validate_plain_select_clauses(select)?;
    validate_from_relation_after_clause_check(select, catalog)
}

#[derive(Clone, Debug)]
struct SingleInputFromSource {
    alias: Option<String>,
    source_selection: Option<Expr>,
    projected_column_ids: Option<BTreeSet<String>>,
    projection: Option<SourceProjection>,
}

fn validate_from_relation_allow_having_and_distinct_on_with_cte(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    identity_cte_alias: Option<&str>,
) -> Result<SingleInputFromSource, ViewPlanError> {
    validate_plain_select_clauses_allow_having_and_distinct_on(select)?;
    validate_from_relation_after_clause_check_with_cte(select, catalog, identity_cte_alias)
}

fn validate_from_relation_after_clause_check(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
) -> Result<Option<String>, ViewPlanError> {
    validate_from_relation_after_clause_check_with_options(select, catalog, None, false)
        .map(|source| source.alias)
}

fn validate_from_relation_after_clause_check_with_cte(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    identity_cte_alias: Option<&str>,
) -> Result<SingleInputFromSource, ViewPlanError> {
    validate_from_relation_after_clause_check_with_options(
        select,
        catalog,
        identity_cte_alias,
        true,
    )
}

fn validate_from_relation_after_clause_check_with_options(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    identity_cte_alias: Option<&str>,
    allow_derived_source: bool,
) -> Result<SingleInputFromSource, ViewPlanError> {
    let [table] = select.from.as_slice() else {
        return unsupported("expected exactly one input relation");
    };
    if !table.joins.is_empty() {
        return unsupported("joins are not supported for single-input materialized view planning");
    }
    if let TableFactor::Derived {
        lateral,
        subquery,
        alias,
        sample,
    } = &table.relation
    {
        if !allow_derived_source {
            return unsupported("nested derived input relations are unsupported");
        }
        return validate_single_input_derived_from_relation(
            *lateral,
            subquery,
            alias.as_ref(),
            sample.as_ref(),
            catalog,
        );
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = &table.relation
    else {
        return unsupported("FROM must reference a registered relation table");
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return unsupported(
            "table functions, hints, versions, samples, and partitions are unsupported",
        );
    }

    let Some(table_name) = single_object_name_identifier(name) else {
        return unsupported("relation name must be an unqualified identifier");
    };
    let accepted = [
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_name.as_str(),
        catalog.datafusion_registration.name.as_str(),
    ];
    let matched_catalog_relation = accepted
        .iter()
        .any(|candidate| identifier_eq(candidate, table_name.as_str()));
    let matched_identity_cte =
        identity_cte_alias.is_some_and(|alias| identifier_eq(alias, table_name.as_str()));
    if !matched_catalog_relation && !matched_identity_cte {
        return unsupported("FROM relation does not match the view input relation catalog");
    }
    let Some(alias) = alias else {
        return Ok(SingleInputFromSource {
            alias: matched_identity_cte.then(|| table_name.to_string()),
            source_selection: None,
            projected_column_ids: None,
            projection: None,
        });
    };
    if !alias.columns.is_empty() {
        return unsupported("single-input relation column aliases are unsupported");
    }
    Ok(SingleInputFromSource {
        alias: Some(alias.name.value.clone()),
        source_selection: None,
        projected_column_ids: None,
        projection: None,
    })
}

fn validate_single_input_derived_from_relation(
    lateral: bool,
    subquery: &Query,
    alias: Option<&TableAlias>,
    sample: Option<&TableSampleKind>,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SingleInputFromSource, ViewPlanError> {
    if lateral || sample.is_some() {
        return unsupported("LATERAL and TABLESAMPLE are unsupported for derived input relations");
    }
    let Some(alias) = alias else {
        return unsupported("derived input relation must have an alias");
    };
    if !alias.columns.is_empty() {
        return unsupported("derived input relation column aliases are unsupported");
    }
    validate_query_level_clauses(subquery, false)?;
    let select = supported_plain_select_body(subquery)?;
    validate_plain_select_clauses(select)?;
    if !group_by_is_empty(&select.group_by) {
        return unsupported("derived input relation must not aggregate");
    }
    let source_alias = validate_from_relation_after_clause_check(select, catalog)?;
    let projection =
        validate_simple_source_projection(&select.projection, catalog, source_alias.as_deref())?;
    Ok(SingleInputFromSource {
        alias: Some(alias.name.value.clone()),
        source_selection: normalize_source_selection(
            select.selection.as_ref(),
            source_alias.as_deref(),
        ),
        projected_column_ids: projection
            .as_ref()
            .map(|projection| projection.projected_column_ids.clone()),
        projection,
    })
}

fn validate_event_time_window_from_relation_with_cte<'a>(
    select: &Select,
    catalog: &'a VelorixRelationCatalogV1,
    identity_cte_alias: Option<&str>,
) -> Result<
    (
        &'a RelationColumnV1,
        EventTimeWindowGroupBySpec,
        SingleInputFromSource,
    ),
    ViewPlanError,
> {
    let declared_event_time_column = declared_tumbling_event_time_column(catalog)?;
    let [table] = select.from.as_slice() else {
        return unsupported("expected exactly one event-time window input relation");
    };
    if !table.joins.is_empty() {
        return unsupported("joins are not supported inside event-time window materialization");
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = &table.relation
    else {
        return unsupported(
            "FROM must use TUMBLE/HOP/SESSION(relation, event_time, interval...) for window views",
        );
    };
    if !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return unsupported("event-time window table hints, versions, and samples are unsupported");
    }
    let Some(function_name) = single_object_name_identifier(name) else {
        return unsupported("FROM must use TUMBLE/HOP/SESSION(relation, event_time, interval...)");
    };
    let function_name = function_name.to_ascii_lowercase();
    if !matches!(function_name.as_str(), "tumble" | "hop" | "session") {
        return unsupported("FROM must use TUMBLE/HOP/SESSION(relation, event_time, interval...)");
    }
    let Some(args) = args else {
        return unsupported("event-time window table function requires arguments");
    };
    if args.settings.is_some() {
        return unsupported("event-time window table function settings are not supported");
    }
    let (relation_arg, event_time_arg, window) = match function_name.as_str() {
        "tumble" => {
            let [relation_arg, event_time_arg, interval_arg] = args.args.as_slice() else {
                return unsupported("tumble requires relation, event_time, and interval arguments");
            };
            (
                relation_arg,
                event_time_arg,
                EventTimeWindowGroupBySpec {
                    kind: SupportedEventTimeWindowKind::Tumbling,
                    window_size_ns: positive_window_interval_ns(interval_arg, "TUMBLE")?,
                    hop_slide_ns: None,
                    session_gap_ns: None,
                },
            )
        }
        "hop" => {
            let [relation_arg, event_time_arg, slide_arg, size_arg] = args.args.as_slice() else {
                return unsupported("hop requires relation, event_time, slide, and size arguments");
            };
            let hop_slide_ns = positive_window_interval_ns(slide_arg, "HOP slide")?;
            let window_size_ns = positive_window_interval_ns(size_arg, "HOP size")?;
            if window_size_ns < hop_slide_ns || window_size_ns % hop_slide_ns != 0 {
                return unsupported("HOP requires size to be a positive multiple of slide");
            }
            (
                relation_arg,
                event_time_arg,
                EventTimeWindowGroupBySpec {
                    kind: SupportedEventTimeWindowKind::Hopping,
                    window_size_ns,
                    hop_slide_ns: Some(hop_slide_ns),
                    session_gap_ns: None,
                },
            )
        }
        "session" => {
            let [relation_arg, event_time_arg, gap_arg] = args.args.as_slice() else {
                return unsupported("session requires relation, event_time, and gap arguments");
            };
            let session_gap_ns = positive_window_interval_ns(gap_arg, "SESSION gap")?;
            (
                relation_arg,
                event_time_arg,
                EventTimeWindowGroupBySpec {
                    kind: SupportedEventTimeWindowKind::Session,
                    window_size_ns: session_gap_ns,
                    hop_slide_ns: None,
                    session_gap_ns: Some(session_gap_ns),
                },
            )
        }
        _ => unreachable!("validated event-time window table function"),
    };
    let relation_name = table_function_identifier_arg(relation_arg)?;
    let accepted = [
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_name.as_str(),
        catalog.datafusion_registration.name.as_str(),
    ];
    let matched_catalog_relation = accepted
        .iter()
        .any(|candidate| identifier_eq(candidate, relation_name.as_str()));
    let matched_identity_cte =
        identity_cte_alias.is_some_and(|alias| identifier_eq(alias, relation_name.as_str()));
    if !matched_catalog_relation && !matched_identity_cte {
        return unsupported("tumble relation does not match the view input relation catalog");
    }
    let event_time_name = table_function_identifier_arg(event_time_arg)?;
    let Some(event_time_column) = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column_identifier_eq(column, event_time_name.as_str()))
    else {
        return unsupported("event-time window argument must reference a registered column");
    };
    if event_time_column.column_id != declared_event_time_column.column_id {
        return unsupported("event-time window column must match relation event-time column");
    }
    let relation_alias = if let Some(alias) = alias {
        if !alias.columns.is_empty() {
            return unsupported("event-time window column aliases are unsupported");
        }
        Some(alias.name.value.clone())
    } else if matched_identity_cte {
        Some(relation_name)
    } else {
        None
    };
    Ok((
        event_time_column,
        window,
        SingleInputFromSource {
            alias: relation_alias,
            source_selection: None,
            projected_column_ids: None,
            projection: None,
        },
    ))
}

fn declared_tumbling_event_time_column(
    catalog: &VelorixRelationCatalogV1,
) -> Result<&RelationColumnV1, ViewPlanError> {
    let Some(declared_event_time_column_id) = &catalog.relation_schema.event_time_column_id else {
        return unsupported("tumbling window SQL requires a declared relation event-time column");
    };
    let Some(event_time_column) = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| &column.column_id == declared_event_time_column_id)
    else {
        return unsupported("relation event-time column is missing from the catalog");
    };
    match event_time_column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Date32
        | ArrowPhysicalTypeV1::TimestampNanosecond { .. } => Ok(event_time_column),
        _ => unsupported(
            "tumble event-time column currently supports Int64, Date32, or TimestampNanosecond",
        ),
    }
}

fn table_function_identifier_arg(arg: &FunctionArg) -> Result<String, ViewPlanError> {
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(identifier))) = arg else {
        return unsupported("tumble relation and event-time arguments must be identifiers");
    };
    Ok(identifier.value.clone())
}

fn table_function_interval_ns_arg(arg: &FunctionArg) -> Result<i64, ViewPlanError> {
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Interval(interval))) = arg else {
        return unsupported("tumble interval argument must be an INTERVAL literal");
    };
    let Expr::Value(value) = interval.value.as_ref() else {
        return unsupported("tumble interval value must be a string or numeric literal");
    };
    let (quantity, unit) =
        match &value.value {
            SqlValue::SingleQuotedString(text)
            | SqlValue::DoubleQuotedString(text)
            | SqlValue::NationalStringLiteral(text) => {
                parse_interval_text(text, interval.leading_field.clone())?
            }
            SqlValue::Number(text, _) => {
                let unit = interval.leading_field.clone().ok_or_else(|| {
                    ViewPlanError::UnsupportedShape {
                        reason: "tumble interval numeric literal requires a time unit".to_string(),
                    }
                })?;
                (parse_positive_interval_quantity(text)?, unit)
            }
            _ => return unsupported("tumble interval value must be a string or numeric literal"),
        };
    interval_quantity_to_ns(quantity, unit)
}

fn parse_interval_text(
    text: &str,
    leading_field: Option<DateTimeField>,
) -> Result<(i64, DateTimeField), ViewPlanError> {
    if let Some(unit) = leading_field {
        return Ok((parse_positive_interval_quantity(text)?, unit));
    }
    let mut parts = text.split_whitespace();
    let Some(quantity) = parts.next() else {
        return unsupported("tumble interval literal is empty");
    };
    let Some(unit) = parts.next() else {
        return unsupported("tumble interval literal must include a unit");
    };
    if parts.next().is_some() {
        return unsupported("tumble interval literal has too many parts");
    }
    Ok((
        parse_positive_interval_quantity(quantity)?,
        parse_interval_unit(unit)?,
    ))
}

fn parse_positive_interval_quantity(text: &str) -> Result<i64, ViewPlanError> {
    let quantity = text
        .parse::<i64>()
        .map_err(|_| ViewPlanError::UnsupportedShape {
            reason: "tumble interval quantity must be a positive integer".to_string(),
        })?;
    if quantity <= 0 {
        return unsupported("tumble interval quantity must be positive");
    }
    Ok(quantity)
}

fn parse_interval_unit(unit: &str) -> Result<DateTimeField, ViewPlanError> {
    match unit.to_ascii_lowercase().as_str() {
        "nanosecond" | "nanoseconds" | "ns" => Ok(DateTimeField::Nanosecond),
        "microsecond" | "microseconds" | "us" => Ok(DateTimeField::Microsecond),
        "millisecond" | "milliseconds" | "ms" => Ok(DateTimeField::Millisecond),
        "second" | "seconds" | "s" => Ok(DateTimeField::Second),
        "minute" | "minutes" | "m" => Ok(DateTimeField::Minute),
        "hour" | "hours" | "h" => Ok(DateTimeField::Hour),
        "day" | "days" | "d" => Ok(DateTimeField::Day),
        "week" | "weeks" | "w" => Ok(DateTimeField::Week(None)),
        _ => unsupported("tumble interval unit is not supported"),
    }
}

fn interval_quantity_to_ns(quantity: i64, unit: DateTimeField) -> Result<i64, ViewPlanError> {
    let multiplier = match unit {
        DateTimeField::Nanosecond | DateTimeField::Nanoseconds => 1_i64,
        DateTimeField::Microsecond | DateTimeField::Microseconds => 1_000_i64,
        DateTimeField::Millisecond | DateTimeField::Milliseconds => 1_000_000_i64,
        DateTimeField::Second | DateTimeField::Seconds => 1_000_000_000_i64,
        DateTimeField::Minute | DateTimeField::Minutes => 60_000_000_000_i64,
        DateTimeField::Hour | DateTimeField::Hours => 3_600_000_000_000_i64,
        DateTimeField::Day | DateTimeField::Days => 86_400_000_000_000_i64,
        DateTimeField::Week(None) | DateTimeField::Weeks => 604_800_000_000_000_i64,
        _ => return unsupported("tumble interval unit is not supported"),
    };
    quantity
        .checked_mul(multiplier)
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "tumble interval is too large".to_string(),
        })
}

struct JoinSqlBindings<'a> {
    left_catalog: &'a VelorixRelationCatalogV1,
    right_catalog: &'a VelorixRelationCatalogV1,
    join_kind: SupportedJoinKind,
    left_alias: String,
    right_alias: String,
    left_source_selection: Option<Expr>,
    right_source_selection: Option<Expr>,
    on_residual_selection: Option<Expr>,
    join_key_pairs: Vec<(&'a RelationColumnV1, &'a RelationColumnV1)>,
}

fn validate_two_input_join<'a>(
    select: &Select,
    first_catalog: &'a VelorixRelationCatalogV1,
    second_catalog: &'a VelorixRelationCatalogV1,
    cte_sources: &[CteSource],
) -> Result<JoinSqlBindings<'a>, ViewPlanError> {
    let [table] = select.from.as_slice() else {
        return unsupported("expected exactly one joined table expression");
    };
    let [join] = table.joins.as_slice() else {
        return unsupported("expected exactly one INNER JOIN input relation");
    };
    if join.global {
        return unsupported("GLOBAL JOIN is not supported");
    }
    let sql_left_table = table_ref(&table.relation, "left")?;
    let sql_right_table = table_ref(&join.relation, "right")?;
    let sql_left_catalog =
        catalog_for_table_with_ctes(&sql_left_table, first_catalog, second_catalog, cte_sources)?;
    let sql_right_catalog =
        catalog_for_table_with_ctes(&sql_right_table, first_catalog, second_catalog, cte_sources)?;
    validate_derived_source_projection(&sql_left_table, sql_left_catalog)?;
    validate_derived_source_projection(&sql_right_table, sql_right_catalog)?;
    validate_join_cte_sources_are_used(cte_sources, &sql_left_table, &sql_right_table)?;
    if sql_left_catalog.relation_schema.relation_id == sql_right_catalog.relation_schema.relation_id
        && identifier_eq(&sql_left_table.alias, &sql_right_table.alias)
    {
        return unsupported("self-JOIN inputs require two distinct SQL aliases");
    }
    let (join_kind, constraint, swap_operands) = match &join.join_operator {
        JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
            (SupportedJoinKind::Inner, constraint, false)
        }
        JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
            (SupportedJoinKind::Left, constraint, false)
        }
        JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
            (SupportedJoinKind::Left, constraint, true)
        }
        JoinOperator::FullOuter(constraint) => (SupportedJoinKind::Full, constraint, false),
        _ => {
            return unsupported(
                "only INNER or narrow LEFT/RIGHT JOIN is supported for join materialization",
            )
        }
    };
    let (sql_join_key_pairs, on_residual_selection) = match constraint {
        JoinConstraint::On(expr) => {
            let mut join_key_pairs: Vec<(&RelationColumnV1, &RelationColumnV1)> = Vec::new();
            let mut residuals = Vec::new();
            for conjunct in join_on_conjuncts(expr) {
                if let Some((left_join_ref, right_join_ref)) =
                    join_on_equality_refs(conjunct, &sql_left_table.alias, &sql_right_table.alias)?
                {
                    let next_left_column =
                        qualified_ref_catalog_column(&left_join_ref, sql_left_catalog)?;
                    let next_right_column =
                        qualified_ref_catalog_column(&right_join_ref, sql_right_catalog)?;
                    if join_key_pairs.iter().any(|(left_column, right_column)| {
                        left_column.column_id == next_left_column.column_id
                            && right_column.column_id == next_right_column.column_id
                    }) {
                        continue;
                    }
                    join_key_pairs.push((next_left_column, next_right_column));
                } else {
                    residuals.push(conjunct.clone());
                }
            }
            if join_key_pairs.is_empty() {
                return unsupported("JOIN ON must contain exactly one key equality");
            }
            (join_key_pairs, combine_join_on_residuals(residuals))
        }
        JoinConstraint::Using(columns) => {
            if columns.is_empty() {
                return unsupported("JOIN USING must reference at least one column");
            }
            let mut pairs = Vec::with_capacity(columns.len());
            for column in columns {
                let Some(column_name) = single_object_name_identifier(column) else {
                    return unsupported("JOIN USING column must be an unqualified identifier");
                };
                pairs.push((
                    catalog_column_by_identifier(sql_left_catalog, column_name.as_str())?,
                    catalog_column_by_identifier(sql_right_catalog, column_name.as_str())?,
                ));
            }
            (pairs, None)
        }
        _ => return unsupported("JOIN must use one ON equality predicate or USING column"),
    };
    let (left_catalog, right_catalog, left_table, right_table, mut join_key_pairs) =
        if swap_operands {
            (
                sql_right_catalog,
                sql_left_catalog,
                sql_right_table,
                sql_left_table,
                sql_join_key_pairs
                    .into_iter()
                    .map(|(left, right)| (right, left))
                    .collect(),
            )
        } else {
            (
                sql_left_catalog,
                sql_right_catalog,
                sql_left_table,
                sql_right_table,
                sql_join_key_pairs,
            )
        };
    join_key_pairs.sort_by(|(left_a, right_a), (left_b, right_b)| {
        (&left_a.column_id, &right_a.column_id).cmp(&(&left_b.column_id, &right_b.column_id))
    });
    Ok(JoinSqlBindings {
        left_catalog,
        right_catalog,
        join_kind,
        left_alias: left_table.alias,
        right_alias: right_table.alias,
        left_source_selection: left_table.source_selection,
        right_source_selection: right_table.source_selection,
        on_residual_selection,
        join_key_pairs,
    })
}

fn join_on_conjuncts(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Nested(inner) => join_on_conjuncts(inner),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut conjuncts = join_on_conjuncts(left);
            conjuncts.extend(join_on_conjuncts(right));
            conjuncts
        }
        _ => vec![expr],
    }
}

fn join_on_equality_refs(
    expr: &Expr,
    left_alias: &str,
    right_alias: &str,
) -> Result<Option<(QualifiedColumnRef, QualifiedColumnRef)>, ViewPlanError> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    else {
        return Ok(None);
    };
    let Some(left_ref) = maybe_qualified_column_ref(left) else {
        return Ok(None);
    };
    let Some(right_ref) = maybe_qualified_column_ref(right) else {
        return Ok(None);
    };
    orient_join_refs(left_ref, right_ref, left_alias, right_alias).map(Some)
}

fn maybe_qualified_column_ref(expr: &Expr) -> Option<QualifiedColumnRef> {
    let Expr::CompoundIdentifier(parts) = expr else {
        return None;
    };
    let [qualifier, column] = parts.as_slice() else {
        return None;
    };
    Some(QualifiedColumnRef {
        qualifier: qualifier.value.clone(),
        column: column.value.clone(),
    })
}

fn combine_join_on_residuals(residuals: Vec<Expr>) -> Option<Expr> {
    residuals.into_iter().reduce(|left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    })
}

fn catalog_column_by_identifier<'a>(
    catalog: &'a VelorixRelationCatalogV1,
    column_name: &str,
) -> Result<&'a RelationColumnV1, ViewPlanError> {
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column_identifier_eq(column, column_name))
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "JOIN USING column must exist in both input relations".to_string(),
        })
}

struct SqlTableRef {
    name: String,
    alias: String,
    derived_projection: Option<Vec<SelectItem>>,
    source_selection: Option<Expr>,
}

fn table_ref(factor: &TableFactor, side: &'static str) -> Result<SqlTableRef, ViewPlanError> {
    match factor {
        TableFactor::Table { .. } => registered_table_ref(factor, side),
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } => derived_table_ref(*lateral, subquery, alias.as_ref(), sample.as_ref(), side),
        _ => unsupported(format!(
            "{side} JOIN input must be a registered relation table"
        )),
    }
}

fn registered_table_ref(
    factor: &TableFactor,
    side: &'static str,
) -> Result<SqlTableRef, ViewPlanError> {
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = factor
    else {
        return unsupported(format!(
            "{side} JOIN input must be a registered relation table"
        ));
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return unsupported(
            "table functions, hints, versions, samples, and partitions are unsupported",
        );
    }
    let Some(name) = single_object_name_identifier(name) else {
        return unsupported("relation name must be an unqualified identifier");
    };
    let alias = alias
        .as_ref()
        .map(|alias| alias.name.value.clone())
        .unwrap_or_else(|| name.clone());
    Ok(SqlTableRef {
        name,
        alias,
        derived_projection: None,
        source_selection: None,
    })
}

fn derived_table_ref(
    lateral: bool,
    subquery: &Query,
    alias: Option<&TableAlias>,
    sample: Option<&TableSampleKind>,
    side: &'static str,
) -> Result<SqlTableRef, ViewPlanError> {
    if lateral || sample.is_some() {
        return unsupported("LATERAL and TABLESAMPLE are unsupported for derived JOIN inputs");
    }
    let Some(alias) = alias else {
        return unsupported("derived JOIN input must have an alias");
    };
    if !alias.columns.is_empty() {
        return unsupported("derived JOIN input column aliases are unsupported");
    }
    validate_query_level_clauses(subquery, false)?;
    let select = supported_plain_select_body(subquery)?;
    let [table] = select.from.as_slice() else {
        return unsupported("derived JOIN input must read exactly one registered relation");
    };
    if !table.joins.is_empty() {
        return unsupported("joins are not supported inside derived JOIN inputs");
    }
    if !group_by_is_empty(&select.group_by) {
        return unsupported("derived JOIN input must not aggregate");
    }
    let source = registered_table_ref(&table.relation, side)?;
    Ok(SqlTableRef {
        name: source.name,
        alias: alias.name.value.clone(),
        derived_projection: Some(select.projection.clone()),
        source_selection: normalize_source_selection(
            select.selection.as_ref(),
            Some(&source.alias),
        ),
    })
}

fn validate_derived_source_projection(
    table: &SqlTableRef,
    catalog: &VelorixRelationCatalogV1,
) -> Result<(), ViewPlanError> {
    let Some(projection) = &table.derived_projection else {
        return Ok(());
    };
    if cte_projection_is_identity(projection, catalog) {
        Ok(())
    } else {
        unsupported(
            "derived JOIN input must be SELECT * or all registered columns FROM the registered relation",
        )
    }
}

fn catalog_for_table<'a>(
    table: &SqlTableRef,
    first: &'a VelorixRelationCatalogV1,
    second: &'a VelorixRelationCatalogV1,
) -> Result<&'a VelorixRelationCatalogV1, ViewPlanError> {
    [first, second]
        .into_iter()
        .find(|catalog| relation_identifier_matches(catalog, table.name.as_str()))
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "JOIN relation does not match a view input relation catalog".to_string(),
        })
}

fn catalog_for_table_with_ctes<'a>(
    table: &SqlTableRef,
    first: &'a VelorixRelationCatalogV1,
    second: &'a VelorixRelationCatalogV1,
    cte_sources: &[CteSource],
) -> Result<&'a VelorixRelationCatalogV1, ViewPlanError> {
    for cte_source in cte_sources {
        if identifier_eq(table.name.as_str(), &cte_source.alias) {
            return [first, second]
                .into_iter()
                .find(|catalog| catalog.relation_schema.relation_id == cte_source.relation_id)
                .ok_or_else(|| ViewPlanError::UnsupportedShape {
                    reason: "JOIN CTE source relation does not match a view input relation catalog"
                        .to_string(),
                });
        }
    }
    catalog_for_table(table, first, second)
}

fn validate_join_cte_sources_are_used(
    cte_sources: &[CteSource],
    left_table: &SqlTableRef,
    right_table: &SqlTableRef,
) -> Result<(), ViewPlanError> {
    for cte_source in cte_sources {
        if !identifier_eq(&cte_source.alias, left_table.name.as_str())
            && !identifier_eq(&cte_source.alias, right_table.name.as_str())
        {
            return unsupported("identity CTE source must be used by a JOIN input");
        }
    }
    Ok(())
}

fn relation_identifier_matches(catalog: &VelorixRelationCatalogV1, table_name: &str) -> bool {
    [
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_name.as_str(),
        catalog.datafusion_registration.name.as_str(),
    ]
    .iter()
    .any(|candidate| identifier_eq(candidate, table_name))
}

#[derive(Clone)]
struct QualifiedColumnRef {
    qualifier: String,
    column: String,
}

fn qualified_column_ref(expr: &Expr) -> Result<QualifiedColumnRef, ViewPlanError> {
    let Expr::CompoundIdentifier(parts) = expr else {
        return unsupported("JOIN view columns must use qualified table aliases");
    };
    let [qualifier, column] = parts.as_slice() else {
        return unsupported("JOIN view columns must use one table alias and one column name");
    };
    Ok(QualifiedColumnRef {
        qualifier: qualifier.value.clone(),
        column: column.value.clone(),
    })
}

fn orient_join_refs(
    left_expr: QualifiedColumnRef,
    right_expr: QualifiedColumnRef,
    left_alias: &str,
    right_alias: &str,
) -> Result<(QualifiedColumnRef, QualifiedColumnRef), ViewPlanError> {
    if identifier_eq(left_expr.qualifier.as_str(), left_alias)
        && identifier_eq(right_expr.qualifier.as_str(), right_alias)
    {
        Ok((left_expr, right_expr))
    } else if identifier_eq(left_expr.qualifier.as_str(), right_alias)
        && identifier_eq(right_expr.qualifier.as_str(), left_alias)
    {
        Ok((right_expr, left_expr))
    } else {
        unsupported("JOIN ON columns must reference the two joined table aliases")
    }
}

fn qualified_ref_catalog_column<'a>(
    reference: &QualifiedColumnRef,
    catalog: &'a VelorixRelationCatalogV1,
) -> Result<&'a RelationColumnV1, ViewPlanError> {
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column_identifier_eq(column, reference.column.as_str()))
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "qualified column must reference a registered relation column".to_string(),
        })
}

fn catalog_column_by_id<'a>(
    catalog: &'a VelorixRelationCatalogV1,
    column_id: &str,
) -> Result<&'a RelationColumnV1, ViewPlanError> {
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == column_id)
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "column id must reference a registered relation column".to_string(),
        })
}

#[allow(clippy::too_many_arguments)]
fn validate_join_group_by_key(
    select: &Select,
    left_alias: &str,
    left_catalog: &VelorixRelationCatalogV1,
    right_alias: &str,
    right_catalog: &VelorixRelationCatalogV1,
    selected_key_relation_id: &str,
    selected_key_column_id: &str,
    output_key_column_id: &str,
    output_key_catalog: &VelorixRelationCatalogV1,
    coalesced_full_join_key: bool,
    left_key: &RelationColumnV1,
    right_key: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    let (expressions, modifiers) = match &select.group_by {
        GroupByExpr::All(modifiers) if modifiers.is_empty() => return Ok(()),
        GroupByExpr::All(_) => return unsupported("GROUP BY ALL modifiers are not supported"),
        GroupByExpr::Expressions(expressions, modifiers) => (expressions, modifiers),
    };
    if !modifiers.is_empty() {
        return unsupported("GROUP BY modifiers are not supported");
    }
    let [group_key] = expressions.as_slice() else {
        return unsupported("expected exactly one GROUP BY key");
    };
    if expression_is_first_select_projection_ordinal(group_key) {
        return Ok(());
    }
    if expression_references_unambiguous_output_alias(
        group_key,
        output_key_catalog,
        output_key_column_id,
    ) {
        return Ok(());
    }
    if coalesced_full_join_key {
        return if join_key_coalesce_expr(group_key, left_alias, left_key, right_alias, right_key) {
            Ok(())
        } else {
            unsupported(
                "FULL JOIN GROUP BY must match COALESCE(left_key, right_key) or its output alias",
            )
        };
    }
    let reference = qualified_column_ref(group_key)?;
    let (relation_id, column) = if identifier_eq(reference.qualifier.as_str(), left_alias) {
        let column = qualified_ref_catalog_column(&reference, left_catalog)?;
        (&left_catalog.relation_schema.relation_id, column)
    } else if identifier_eq(reference.qualifier.as_str(), right_alias) {
        let column = qualified_ref_catalog_column(&reference, right_catalog)?;
        (&right_catalog.relation_schema.relation_id, column)
    } else {
        return unsupported("GROUP BY key must reference a joined table alias");
    };
    if relation_id == selected_key_relation_id && column.column_id == selected_key_column_id {
        Ok(())
    } else {
        unsupported("GROUP BY key must match the projected join key")
    }
}

#[allow(clippy::too_many_arguments)]
fn join_projection_key<'a>(
    item: &SelectItem,
    join_kind: SupportedJoinKind,
    left_alias: &str,
    left_catalog: &'a VelorixRelationCatalogV1,
    left_key: &'a RelationColumnV1,
    right_alias: &str,
    right_catalog: &'a VelorixRelationCatalogV1,
    right_key: &'a RelationColumnV1,
) -> Result<(&'a VelorixRelationCatalogV1, &'a RelationColumnV1, bool), ViewPlanError> {
    if join_kind == SupportedJoinKind::Full {
        if select_item_is_join_key_coalesce(item, left_alias, left_key, right_alias, right_key) {
            return Ok((left_catalog, left_key, true));
        }
        return unsupported("FULL JOIN first projection must be COALESCE(left_key, right_key)");
    }
    if select_item_references_qualified_column(item, left_alias, left_key) {
        Ok((left_catalog, left_key, false))
    } else if select_item_references_qualified_column(item, right_alias, right_key) {
        Ok((right_catalog, right_key, false))
    } else {
        unsupported("first projection must be one of the joined primary key columns")
    }
}

fn select_item_is_join_key_coalesce(
    item: &SelectItem,
    left_alias: &str,
    left_key: &RelationColumnV1,
    right_alias: &str,
    right_key: &RelationColumnV1,
) -> bool {
    let expression = match item {
        SelectItem::UnnamedExpr(expression)
        | SelectItem::ExprWithAlias {
            expr: expression, ..
        } => expression,
        _ => return false,
    };
    join_key_coalesce_expr(expression, left_alias, left_key, right_alias, right_key)
}

fn join_key_coalesce_expr(
    expression: &Expr,
    left_alias: &str,
    left_key: &RelationColumnV1,
    right_alias: &str,
    right_key: &RelationColumnV1,
) -> bool {
    let Expr::Function(function) = expression else {
        return false;
    };
    if !function_name_eq(&function.name, "coalesce")
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return false;
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return false;
    };
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(left)), FunctionArg::Unnamed(FunctionArgExpr::Expr(right))] =
        arguments.args.as_slice()
    else {
        return false;
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return false;
    }
    let Ok(left_reference) = qualified_column_ref(left) else {
        return false;
    };
    let Ok(right_reference) = qualified_column_ref(right) else {
        return false;
    };
    identifier_eq(&left_reference.qualifier, left_alias)
        && column_identifier_eq(left_key, &left_reference.column)
        && identifier_eq(&right_reference.qualifier, right_alias)
        && column_identifier_eq(right_key, &right_reference.column)
}

#[allow(clippy::too_many_arguments)]
fn join_aggregate_input_relation_id<'a>(
    supported: &'a SupportedJoinViewPlan,
    output: &SupportedAggregateOutput,
) -> &'a str {
    match output
        .input_relation_side
        .unwrap_or(SupportedAggregateInputRelationSide::Left)
    {
        SupportedAggregateInputRelationSide::Left => &supported.sum_value_relation_id,
        SupportedAggregateInputRelationSide::Right => &supported.right_input_relation_id,
    }
}
struct ValidatedJoinProjection<'a> {
    group_key_relation_id: String,
    group_key_column_id: String,
    output_key_column_id: String,
    output_key_catalog: &'a VelorixRelationCatalogV1,
    coalesced_full_join_key: bool,
    sum_value_column: &'a RelationColumnV1,
    aggregate_outputs: Vec<SupportedAggregateOutput>,
    shared_aggregate_filter_expr: Option<JoinPredicateExpr>,
    aggregate_filter_exprs: BTreeMap<String, JoinPredicateExpr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct JoinCountArgument {
    column_id: String,
    relation_side: SupportedAggregateInputRelationSide,
}

#[allow(clippy::too_many_arguments)]
fn validate_join_projection<'a>(
    select: &Select,
    join_kind: SupportedJoinKind,
    right_alias: &str,
    right_catalog: &'a VelorixRelationCatalogV1,
    right_key: &'a RelationColumnV1,
    left_alias: &str,
    left_catalog: &'a VelorixRelationCatalogV1,
    left_key: &'a RelationColumnV1,
) -> Result<ValidatedJoinProjection<'a>, ViewPlanError> {
    let [key, aggregates @ ..] = select.projection.as_slice() else {
        return unsupported("expected projection: key, aggregate...");
    };
    if aggregates.is_empty() {
        return unsupported("expected projection: key, aggregate...");
    }
    let (output_key_catalog, output_key_column, coalesced_full_join_key) = join_projection_key(
        key,
        join_kind,
        left_alias,
        left_catalog,
        left_key,
        right_alias,
        right_catalog,
        right_key,
    )?;
    let output_key_column_id = select_item_alias_or_default(key, output_key_column.name.as_str())?;
    let mut left_value: Option<&RelationColumnV1> = None;
    let mut aggregate_outputs = Vec::with_capacity(aggregates.len());
    for item in aggregates {
        let output = if select_item_is_function(item, "count") {
            validate_join_count_select_item(
                item,
                left_alias,
                left_catalog,
                right_alias,
                right_catalog,
            )?
        } else {
            let (output, side, column) = validate_join_value_aggregate_select_item(
                item,
                left_alias,
                left_catalog,
                right_alias,
                right_catalog,
            )?;
            if side == SupportedAggregateInputRelationSide::Left {
                if let Some(left_value) = left_value {
                    if left_value.column_id != column.column_id {
                        return unsupported(
                            "JOIN value aggregates must use the same left input value column",
                        );
                    }
                } else {
                    left_value = Some(column);
                }
            }
            output
        };
        aggregate_outputs.push(output);
    }
    let left_value = match left_value {
        Some(left_value) => left_value,
        None if aggregate_outputs.iter().all(|output| {
            matches!(
                output.function,
                LogicalPlanAggregateFunctionV1::Count
                    | LogicalPlanAggregateFunctionV1::CountDistinct
            )
        }) =>
        {
            count_only_runtime_value_column(left_catalog, &aggregate_outputs)?
        }
        None => return unsupported("JOIN aggregate projection requires one left value aggregate"),
    };
    for output in &aggregate_outputs {
        match output.function {
            LogicalPlanAggregateFunctionV1::CountDistinct
                if output.input_relation_side
                    == Some(SupportedAggregateInputRelationSide::Left)
                    && output.input_column_id.as_deref() != Some(left_value.column_id.as_str()) =>
            {
                return unsupported(
                    "JOIN count(DISTINCT ...) currently supports the left sum value column or a right input column",
                );
            }
            LogicalPlanAggregateFunctionV1::CountDistinct
                if output.input_relation_side
                    != Some(SupportedAggregateInputRelationSide::Left)
                    && output.input_relation_side
                        != Some(SupportedAggregateInputRelationSide::Right) =>
            {
                return unsupported(
                    "JOIN count(DISTINCT ...) must reference a joined input column",
                );
            }
            LogicalPlanAggregateFunctionV1::Count
                if output.input_relation_side
                    == Some(SupportedAggregateInputRelationSide::Left)
                    && output.input_column_id.as_deref() != Some(left_value.column_id.as_str()) =>
            {
                return unsupported(
                    "JOIN count(nullable_column) currently supports the left sum value column or a right input column",
                );
            }
            _ => {}
        }
    }
    let filter_context = JoinSelectionContext {
        select,
        left_alias,
        left_catalog,
        left_key,
        left_value,
        right_alias,
        right_catalog,
    };
    let mut aggregate_filter_exprs = Vec::new();
    for (item, output) in aggregates.iter().zip(&aggregate_outputs) {
        if let Some(filter) = select_item_function_filter(item) {
            aggregate_filter_exprs.push((
                output.output_column_id.clone(),
                validate_join_predicate_expr(filter, &filter_context)?,
            ));
        }
    }
    let (shared_aggregate_filter_expr, per_output_filter_exprs) =
        match aggregate_filter_exprs.as_slice() {
            [] => (None, BTreeMap::new()),
            [(first_output, first_expr), rest @ ..]
                if aggregate_filter_exprs.len() == aggregate_outputs.len()
                    && rest.iter().all(|(_, expr)| expr == first_expr)
                    && aggregate_outputs
                        .iter()
                        .any(|output| output.output_column_id == *first_output)
                    && rest.iter().all(|(output_column_id, _)| {
                        aggregate_outputs
                            .iter()
                            .any(|output| output.output_column_id == *output_column_id)
                    }) =>
            {
                (Some(first_expr.clone()), BTreeMap::new())
            }
            _ => {
                for (output_column_id, _) in &aggregate_filter_exprs {
                    let Some(output) = aggregate_outputs
                        .iter()
                        .find(|output| output.output_column_id == *output_column_id)
                    else {
                        return unsupported("JOIN aggregate FILTER output is not projected");
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
                        return unsupported("JOIN aggregate FILTER output is not supported");
                    }
                }
                (
                    None,
                    aggregate_filter_exprs
                        .into_iter()
                        .collect::<BTreeMap<_, _>>(),
                )
            }
        };
    let mut output_ids = BTreeSet::new();
    for output in &aggregate_outputs {
        if !output_ids.insert(output.output_column_id.to_ascii_lowercase()) {
            return unsupported("JOIN aggregate output column ids must be unique");
        }
    }
    Ok(ValidatedJoinProjection {
        group_key_relation_id: output_key_catalog.relation_schema.relation_id.clone(),
        group_key_column_id: output_key_column.column_id.clone(),
        output_key_column_id,
        output_key_catalog,
        coalesced_full_join_key,
        sum_value_column: left_value,
        aggregate_outputs,
        shared_aggregate_filter_expr,
        aggregate_filter_exprs: per_output_filter_exprs,
    })
}

fn validate_left_join_scope(
    join_kind: SupportedJoinKind,
    projection: &ValidatedJoinProjection<'_>,
    left_catalog: &VelorixRelationCatalogV1,
    left_key: &RelationColumnV1,
    on_residual_predicate: &Option<JoinPredicateExpr>,
    has_left_source_filter: bool,
    has_right_source_filter: bool,
) -> Result<(), ViewPlanError> {
    if join_kind == SupportedJoinKind::Inner {
        return Ok(());
    }
    if join_kind == SupportedJoinKind::Left
        && (projection.group_key_relation_id != left_catalog.relation_schema.relation_id
            || projection.group_key_column_id != left_key.column_id)
    {
        return unsupported("LEFT JOIN materialization must GROUP BY the left primary key");
    }
    if join_kind == SupportedJoinKind::Full && !projection.coalesced_full_join_key {
        return unsupported(
            "FULL JOIN materialization must project and GROUP BY COALESCE(left_key, right_key)",
        );
    }
    if on_residual_predicate.is_some() {
        return unsupported("outer join materialization does not support ON residual predicates");
    }
    if projection.shared_aggregate_filter_expr.is_some() {
        return unsupported(
            "outer join materialization does not support shared aggregate FILTER clauses",
        );
    }
    if has_right_source_filter || (join_kind == SupportedJoinKind::Full && has_left_source_filter) {
        return unsupported("outer join materialization does not support CTE or derived-source filters on a null-extending input");
    }
    Ok(())
}

fn validate_join_having(
    select: &Select,
    context: &JoinHavingBindingContext<'_>,
) -> Result<Option<AggregateOutputPredicateExpr>, ViewPlanError> {
    let Some(expr) = &select.having else {
        return Ok(None);
    };
    validate_join_having_expr(expr, context).map(Some)
}

struct JoinHavingBindingContext<'a> {
    select: &'a Select,
    left_alias: &'a str,
    left_catalog: &'a VelorixRelationCatalogV1,
    left_key: &'a RelationColumnV1,
    left_value: &'a RelationColumnV1,
    right_alias: &'a str,
    right_catalog: &'a VelorixRelationCatalogV1,
    aggregate_outputs: &'a [SupportedAggregateOutput],
    shared_aggregate_filter_expr: Option<&'a JoinPredicateExpr>,
    aggregate_filter_exprs: &'a BTreeMap<String, JoinPredicateExpr>,
}

fn validate_join_having_expr(
    expr: &Expr,
    context: &JoinHavingBindingContext<'_>,
) -> Result<AggregateOutputPredicateExpr, ViewPlanError> {
    if let Expr::Nested(inner) = expr {
        return validate_join_having_expr(inner, context);
    }
    if let Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr,
    } = expr
    {
        return Ok(negate_aggregate_predicate_expr(validate_join_having_expr(
            expr, context,
        )?));
    }
    if let Expr::Between {
        expr,
        negated,
        low,
        high,
    } = expr
    {
        let output_column_id = join_having_output_column_id(expr, context)?;
        return aggregate_between_predicate_expr(output_column_id, low, high, *negated);
    }
    if let Expr::InList {
        expr,
        list,
        negated,
    } = expr
    {
        let output_column_id = join_having_output_column_id(expr, context)?;
        return aggregate_in_list_predicate_expr(output_column_id, list, *negated);
    }
    if let Expr::IsNull(inner) = expr {
        let output_column_id = join_having_output_column_id(inner, context)?;
        return Ok(aggregate_null_predicate_expr(output_column_id, false));
    }
    if let Expr::IsNotNull(inner) = expr {
        let output_column_id = join_having_output_column_id(inner, context)?;
        return Ok(aggregate_null_predicate_expr(output_column_id, true));
    }
    if let Some((left, right, op)) = distinct_predicate_parts(expr) {
        let (output_expr, literal_expr) = if expression_is_literal(right) {
            (left, right)
        } else if expression_is_literal(left) {
            (right, left)
        } else {
            return unsupported(
                "JOIN HAVING IS DISTINCT FROM must compare an aggregate output to a literal",
            );
        };
        return Ok(AggregateOutputPredicateExpr::Atom {
            predicate: AggregateOutputPredicate {
                output_column_id: join_having_output_column_id(output_expr, context)?,
                op,
                literal: predicate_literal(literal_expr)?,
            },
        });
    }
    let Expr::BinaryOp { left, op, right } = expr else {
        return unsupported("JOIN HAVING currently supports one aggregate-output comparison");
    };
    if *op == BinaryOperator::And {
        return Ok(AggregateOutputPredicateExpr::And {
            left: Box::new(validate_join_having_expr(left, context)?),
            right: Box::new(validate_join_having_expr(right, context)?),
        });
    }
    if *op == BinaryOperator::Or {
        return Ok(AggregateOutputPredicateExpr::Or {
            left: Box::new(validate_join_having_expr(left, context)?),
            right: Box::new(validate_join_having_expr(right, context)?),
        });
    }
    let (output_expr, literal_expr, op) = if expression_is_literal(right) {
        (left.as_ref(), right.as_ref(), op.clone())
    } else if expression_is_literal(left) {
        let Some(op) = reverse_predicate_op(op.clone()) else {
            return unsupported("JOIN HAVING comparison operator is not supported");
        };
        (right.as_ref(), left.as_ref(), op)
    } else {
        return unsupported("JOIN HAVING must compare an aggregate output to a literal");
    };
    let Some(op) = predicate_op(op) else {
        return unsupported("JOIN HAVING comparison operator is not supported");
    };
    Ok(AggregateOutputPredicateExpr::Atom {
        predicate: AggregateOutputPredicate {
            output_column_id: join_having_output_column_id(output_expr, context)?,
            op,
            literal: predicate_literal(literal_expr)?,
        },
    })
}

fn join_having_output_column_id(
    expr: &Expr,
    context: &JoinHavingBindingContext<'_>,
) -> Result<String, ViewPlanError> {
    if let Some(identifier) = expression_identifier(expr) {
        if let Some(output) = context
            .aggregate_outputs
            .iter()
            .find(|output| identifier_eq(&output.output_column_id, identifier))
        {
            return Ok(output.output_column_id.clone());
        }
        return unsupported("JOIN HAVING identifier must reference a projected aggregate output");
    }
    let Expr::Function(function) = expr else {
        return unsupported("JOIN HAVING expression must reference a projected aggregate output");
    };
    let Some(function_name) = single_object_name_identifier(&function.name) else {
        return unsupported("JOIN HAVING aggregate function name must be unqualified");
    };
    if !matches!(function.parameters, FunctionArguments::None)
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported("JOIN HAVING aggregate modifiers are not supported");
    }
    let filter_context = JoinSelectionContext {
        select: context.select,
        left_alias: context.left_alias,
        left_catalog: context.left_catalog,
        left_key: context.left_key,
        left_value: context.left_value,
        right_alias: context.right_alias,
        right_catalog: context.right_catalog,
    };
    let having_filter_expr = function
        .filter
        .as_deref()
        .map(|filter| validate_join_predicate_expr(filter, &filter_context))
        .transpose()?;
    let FunctionArguments::List(arguments) = &function.args else {
        return unsupported("JOIN HAVING aggregate arguments must be a simple argument list");
    };
    if !arguments.clauses.is_empty() {
        return unsupported("JOIN HAVING aggregate argument clauses are not supported");
    }
    match function_name.to_ascii_lowercase().as_str() {
        "count" => {
            let is_distinct = matches!(
                arguments.duplicate_treatment,
                Some(DuplicateTreatment::Distinct)
            );
            let argument = validate_join_count_argument(
                arguments,
                context.left_alias,
                context.left_catalog,
                context.right_alias,
                context.right_catalog,
            )?;
            let function_matches = if is_distinct {
                let Some(argument) = argument.as_ref() else {
                    return unsupported(
                        "JOIN HAVING count(DISTINCT ...) must reference one column",
                    );
                };
                join_aggregate_outputs_for_side(
                    context.aggregate_outputs,
                    LogicalPlanAggregateFunctionV1::CountDistinct,
                    Some(argument.column_id.as_str()),
                    argument.relation_side,
                    None,
                )
            } else if let Some(argument) = argument.as_ref() {
                join_aggregate_outputs_for_side(
                    context.aggregate_outputs,
                    LogicalPlanAggregateFunctionV1::Count,
                    Some(argument.column_id.as_str()),
                    argument.relation_side,
                    None,
                )
            } else {
                join_aggregate_outputs(
                    context.aggregate_outputs,
                    LogicalPlanAggregateFunctionV1::Count,
                    None,
                )
            };
            join_filtered_having_output_column_id(
                function_matches,
                having_filter_expr.as_ref(),
                context,
            )
        }
        "sum" | "avg" | "min" | "max" => {
            let aggregate_function = match function_name.to_ascii_lowercase().as_str() {
                "sum" => LogicalPlanAggregateFunctionV1::Sum,
                "avg" => LogicalPlanAggregateFunctionV1::Avg,
                "min" => LogicalPlanAggregateFunctionV1::Min,
                "max" => LogicalPlanAggregateFunctionV1::Max,
                _ => unreachable!("validated JOIN HAVING aggregate function"),
            };
            if arguments.duplicate_treatment.is_some() {
                return unsupported("JOIN HAVING DISTINCT value aggregates are not supported");
            }
            let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice()
            else {
                return unsupported(
                    "JOIN HAVING value aggregate requires one qualified column or expression argument",
                );
            };
            let (input_side, _catalog, column, input_expression) =
                validate_join_value_aggregate_argument(
                    argument,
                    context.left_alias,
                    context.left_catalog,
                    context.right_alias,
                    context.right_catalog,
                )?;
            let function_matches = join_aggregate_outputs_for_side(
                context.aggregate_outputs,
                aggregate_function,
                Some(&column.column_id),
                input_side,
                input_expression.as_ref(),
            );
            join_filtered_having_output_column_id(
                function_matches,
                having_filter_expr.as_ref(),
                context,
            )
        }
        _ => unsupported("JOIN HAVING aggregate function is not supported"),
    }
}

fn join_filtered_having_output_column_id(
    function_matches: Vec<&SupportedAggregateOutput>,
    having_filter_expr: Option<&JoinPredicateExpr>,
    context: &JoinHavingBindingContext<'_>,
) -> Result<String, ViewPlanError> {
    let matches = function_matches
        .iter()
        .copied()
        .filter(|output| {
            aggregate_output_filter_matches(
                having_filter_expr,
                &output.output_column_id,
                context.shared_aggregate_filter_expr,
                context.aggregate_filter_exprs,
            )
        })
        .collect::<Vec<_>>();
    let [output] = matches.as_slice() else {
        if having_filter_expr.is_none()
            && function_matches.iter().any(|output| {
                context.shared_aggregate_filter_expr.is_some()
                    || context
                        .aggregate_filter_exprs
                        .contains_key(&output.output_column_id)
            })
        {
            return unsupported(
                "JOIN HAVING aggregate function must reference one unfiltered projected aggregate output",
            );
        }
        return unsupported(
            "JOIN HAVING aggregate expression must match a projected aggregate output",
        );
    };
    Ok(output.output_column_id.clone())
}

fn join_aggregate_outputs<'a>(
    aggregate_outputs: &'a [SupportedAggregateOutput],
    function: LogicalPlanAggregateFunctionV1,
    input_column_id: Option<&str>,
) -> Vec<&'a SupportedAggregateOutput> {
    aggregate_outputs
        .iter()
        .filter(|output| {
            output.function == function && output.input_column_id.as_deref() == input_column_id
        })
        .collect()
}

fn join_aggregate_outputs_for_side<'a>(
    aggregate_outputs: &'a [SupportedAggregateOutput],
    function: LogicalPlanAggregateFunctionV1,
    input_column_id: Option<&str>,
    input_side: SupportedAggregateInputRelationSide,
    input_expression: Option<&SupportedProjectionExpr>,
) -> Vec<&'a SupportedAggregateOutput> {
    aggregate_outputs
        .iter()
        .filter(|output| {
            output.function == function
                && output.input_column_id.as_deref() == input_column_id
                && output.input_expression.as_ref() == input_expression
                && output
                    .input_relation_side
                    .unwrap_or(SupportedAggregateInputRelationSide::Left)
                    == input_side
        })
        .collect()
}

struct JoinSelectionContext<'a> {
    select: &'a Select,
    left_alias: &'a str,
    left_catalog: &'a VelorixRelationCatalogV1,
    left_key: &'a RelationColumnV1,
    left_value: &'a RelationColumnV1,
    right_alias: &'a str,
    right_catalog: &'a VelorixRelationCatalogV1,
}

fn validate_join_selection(
    context: &JoinSelectionContext<'_>,
) -> Result<Option<JoinPredicateExpr>, ViewPlanError> {
    let Some(selection) = &context.select.selection else {
        return Ok(None);
    };
    validate_join_predicate_expr(selection, context).map(Some)
}

fn validate_join_on_residual_selection(
    selection: Option<&Expr>,
    context: &JoinSelectionContext<'_>,
) -> Result<Option<JoinPredicateExpr>, ViewPlanError> {
    let Some(selection) = selection else {
        return Ok(None);
    };
    let predicate = validate_join_predicate_expr(selection, context)?;
    if predicate.contains_or() {
        return unsupported("JOIN ON residual predicates must be AND-conjoined simple predicates");
    }
    Ok(Some(predicate))
}

fn validate_join_predicate_expr(
    selection: &Expr,
    context: &JoinSelectionContext<'_>,
) -> Result<JoinPredicateExpr, ViewPlanError> {
    if let Expr::Nested(inner) = selection {
        return validate_join_predicate_expr(inner, context);
    }
    if let Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr,
    } = selection
    {
        return Ok(negate_join_predicate_expr(validate_join_predicate_expr(
            expr, context,
        )?));
    }
    if let Expr::Between {
        expr,
        negated,
        low,
        high,
    } = selection
    {
        let reference = qualified_column_ref(expr)?;
        let (relation_id, column) =
            if identifier_eq(reference.qualifier.as_str(), context.left_alias) {
                let column = qualified_ref_catalog_column(&reference, context.left_catalog)?;
                if column.column_id != context.left_key.column_id
                    && column.column_id != context.left_value.column_id
                {
                    return unsupported(
                        "JOIN WHERE left predicate must reference the join key or sum input column",
                    );
                }
                (
                    context.left_catalog.relation_schema.relation_id.clone(),
                    column,
                )
            } else if identifier_eq(reference.qualifier.as_str(), context.right_alias) {
                let column = qualified_ref_catalog_column(&reference, context.right_catalog)?;
                if column.column_id == context.right_catalog.relation_schema.weight_column_id {
                    return unsupported(
                        "JOIN WHERE right predicate must not reference the weight column",
                    );
                }
                (
                    context.right_catalog.relation_schema.relation_id.clone(),
                    column,
                )
            } else {
                return unsupported("JOIN WHERE column must reference a joined table alias");
            };
        return join_between_predicate_expr(
            relation_id,
            column.column_id.clone(),
            low,
            high,
            *negated,
        );
    }
    if let Expr::InList {
        expr,
        list,
        negated,
    } = selection
    {
        let reference = qualified_column_ref(expr)?;
        let (relation_id, column) =
            if identifier_eq(reference.qualifier.as_str(), context.left_alias) {
                let column = qualified_ref_catalog_column(&reference, context.left_catalog)?;
                if column.column_id != context.left_key.column_id
                    && column.column_id != context.left_value.column_id
                {
                    return unsupported(
                        "JOIN WHERE left predicate must reference the join key or sum input column",
                    );
                }
                (
                    context.left_catalog.relation_schema.relation_id.clone(),
                    column,
                )
            } else if identifier_eq(reference.qualifier.as_str(), context.right_alias) {
                let column = qualified_ref_catalog_column(&reference, context.right_catalog)?;
                if column.column_id == context.right_catalog.relation_schema.weight_column_id {
                    return unsupported(
                        "JOIN WHERE right predicate must not reference the weight column",
                    );
                }
                (
                    context.right_catalog.relation_schema.relation_id.clone(),
                    column,
                )
            } else {
                return unsupported("JOIN WHERE column must reference a joined table alias");
            };
        return join_in_list_predicate_expr(
            relation_id,
            column.column_id.clone(),
            column.nullable,
            list,
            *negated,
        );
    }
    if let Expr::Like {
        negated,
        any,
        expr,
        pattern,
        escape_char,
    } = selection
    {
        if *any || escape_char.is_some() {
            return unsupported("JOIN WHERE LIKE ANY and ESCAPE are not supported");
        }
        let reference = qualified_column_ref(expr)?;
        let (relation_id, column) =
            if identifier_eq(reference.qualifier.as_str(), context.left_alias) {
                let column = qualified_ref_catalog_column(&reference, context.left_catalog)?;
                if column.column_id != context.left_key.column_id
                    && column.column_id != context.left_value.column_id
                {
                    return unsupported(
                        "JOIN WHERE left predicate must reference the join key or sum input column",
                    );
                }
                (
                    context.left_catalog.relation_schema.relation_id.clone(),
                    column,
                )
            } else if identifier_eq(reference.qualifier.as_str(), context.right_alias) {
                let column = qualified_ref_catalog_column(&reference, context.right_catalog)?;
                if column.column_id == context.right_catalog.relation_schema.weight_column_id {
                    return unsupported(
                        "JOIN WHERE right predicate must not reference the weight column",
                    );
                }
                (
                    context.right_catalog.relation_schema.relation_id.clone(),
                    column,
                )
            } else {
                return unsupported("JOIN WHERE column must reference a joined table alias");
            };
        if !predicate_column_supports_like(column) {
            return unsupported("JOIN WHERE LIKE column must be text-like");
        }
        return join_like_predicate_expr(relation_id, column.column_id.clone(), pattern, *negated);
    }
    if let Expr::IsNull(expr) = selection {
        let (relation_id, column) = join_predicate_target_column(expr, context)?;
        return Ok(join_null_predicate_expr(
            relation_id,
            column.column_id.clone(),
            false,
        ));
    }
    if let Expr::IsNotNull(expr) = selection {
        let (relation_id, column) = join_predicate_target_column(expr, context)?;
        return Ok(join_null_predicate_expr(
            relation_id,
            column.column_id.clone(),
            true,
        ));
    }
    if let Some((left, right, op)) = distinct_predicate_parts(selection) {
        let (column_expr, literal_expr) = if expression_is_literal(right) {
            (left, right)
        } else if expression_is_literal(left) {
            (right, left)
        } else {
            return unsupported(
                "JOIN WHERE IS DISTINCT FROM must compare a qualified column to a literal",
            );
        };
        let (relation_id, column) = join_predicate_target_column(column_expr, context)?;
        return Ok(JoinPredicateExpr::Atom {
            predicate: JoinRowPredicate {
                relation_id,
                predicate: RowPredicate {
                    column_id: column.column_id.clone(),
                    op,
                    literal: predicate_literal(literal_expr)?,
                },
            },
        });
    }
    let Expr::BinaryOp { left, op, right } = selection else {
        return unsupported(
            "JOIN WHERE currently supports one qualified column/literal comparison",
        );
    };
    if *op == BinaryOperator::And {
        return Ok(JoinPredicateExpr::And {
            left: Box::new(validate_join_predicate_expr(left, context)?),
            right: Box::new(validate_join_predicate_expr(right, context)?),
        });
    }
    if *op == BinaryOperator::Or {
        let left = validate_join_predicate_expr(left, context)?;
        let right = validate_join_predicate_expr(right, context)?;
        return Ok(JoinPredicateExpr::Or {
            left: Box::new(left),
            right: Box::new(right),
        });
    }
    if !expression_is_literal(left)
        && !expression_is_literal(right)
        && (expr_references_qualified_alias(left, context.left_alias)
            || expr_references_qualified_alias(left, context.right_alias))
        && (expr_references_qualified_alias(right, context.left_alias)
            || expr_references_qualified_alias(right, context.right_alias))
    {
        return validate_join_scalar_int64_expression_comparison_predicate_expr(
            left,
            right,
            op.clone(),
            context,
        );
    }
    if expression_is_literal(right) && join_predicate_side_is_scalar_expression(left, context) {
        return validate_join_scalar_int64_comparison_predicate_expr(
            left,
            right,
            op.clone(),
            context,
        );
    }
    if expression_is_literal(left) && join_predicate_side_is_scalar_expression(right, context) {
        let Some(op) = reverse_predicate_op(op.clone()) else {
            return unsupported("JOIN WHERE scalar Int64 comparison operator is not supported");
        };
        return validate_join_scalar_int64_comparison_predicate_expr(right, left, op, context);
    }
    if join_predicate_side_is_scalar_expression(left, context)
        || join_predicate_side_is_scalar_expression(right, context)
    {
        return unsupported(
            "JOIN scalar Int64 predicate expressions must reference exactly one joined relation side and compare to a literal",
        );
    }
    let (column_expr, literal_expr, op) = if expression_is_literal(right) {
        (left.as_ref(), right.as_ref(), op.clone())
    } else if expression_is_literal(left) {
        let Some(op) = reverse_predicate_op(op.clone()) else {
            return unsupported("JOIN WHERE comparison operator is not supported");
        };
        (right.as_ref(), left.as_ref(), op)
    } else {
        return unsupported("JOIN WHERE must compare a qualified column to a literal");
    };
    let reference = qualified_column_ref(column_expr)?;
    let (relation_id, column) = if identifier_eq(reference.qualifier.as_str(), context.left_alias) {
        let column = qualified_ref_catalog_column(&reference, context.left_catalog)?;
        if column.column_id != context.left_key.column_id
            && column.column_id != context.left_value.column_id
        {
            return unsupported(
                "JOIN WHERE left predicate must reference the join key or sum input column",
            );
        }
        (
            context.left_catalog.relation_schema.relation_id.clone(),
            column,
        )
    } else if identifier_eq(reference.qualifier.as_str(), context.right_alias) {
        let column = qualified_ref_catalog_column(&reference, context.right_catalog)?;
        if column.column_id == context.right_catalog.relation_schema.weight_column_id {
            return unsupported("JOIN WHERE right predicate must not reference the weight column");
        }
        (
            context.right_catalog.relation_schema.relation_id.clone(),
            column,
        )
    } else {
        return unsupported("JOIN WHERE column must reference a joined table alias");
    };
    let Some(op) = predicate_op(op) else {
        return unsupported("JOIN WHERE comparison operator is not supported");
    };
    Ok(JoinPredicateExpr::Atom {
        predicate: JoinRowPredicate {
            relation_id,
            predicate: RowPredicate {
                column_id: column.column_id.clone(),
                op,
                literal: predicate_literal(literal_expr)?,
            },
        },
    })
}

fn validate_join_scalar_int64_expression_comparison_predicate_expr(
    left_expr: &Expr,
    right_expr: &Expr,
    op: BinaryOperator,
    context: &JoinSelectionContext<'_>,
) -> Result<JoinPredicateExpr, ViewPlanError> {
    let Some(comparison_op) = predicate_op(op) else {
        return unsupported("JOIN WHERE scalar Int64 comparison operator is not supported");
    };
    let left = join_scalar_int64_expression_side(left_expr, context)?;
    let right = join_scalar_int64_expression_side(right_expr, context)?;
    if left.0 == right.0 {
        return unsupported(
            "JOIN scalar Int64 predicate expressions must reference opposite joined relation sides",
        );
    }
    Ok(JoinPredicateExpr::ScalarInt64ExpressionComparison {
        left_relation_id: left.0,
        left: Box::new(left.1),
        comparison_op,
        right_relation_id: right.0,
        right: Box::new(right.1),
    })
}

fn join_scalar_int64_expression_side(
    expression: &Expr,
    context: &JoinSelectionContext<'_>,
) -> Result<(String, SupportedProjectionExpr), ViewPlanError> {
    let left = join_scalar_int64_expression_match(
        expression,
        context.left_alias,
        context.left_catalog,
        &context.left_catalog.relation_schema.relation_id,
        |column| {
            column.column_id == context.left_key.column_id
                || column.column_id == context.left_value.column_id
        },
    )?;
    let right = join_scalar_int64_expression_match(
        expression,
        context.right_alias,
        context.right_catalog,
        &context.right_catalog.relation_schema.relation_id,
        |column| column.column_id != context.right_catalog.relation_schema.weight_column_id,
    )?;
    match (left, right) {
        (Some(matched), None) | (None, Some(matched)) => Ok(matched),
        _ => unsupported(
            "JOIN scalar Int64 predicate expressions must reference exactly one joined relation side",
        ),
    }
}

fn join_predicate_side_is_scalar_expression(
    expr: &Expr,
    context: &JoinSelectionContext<'_>,
) -> bool {
    !expression_is_literal(expr)
        && maybe_qualified_column_ref(expr).is_none()
        && (expr_references_qualified_alias(expr, context.left_alias)
            || expr_references_qualified_alias(expr, context.right_alias))
}

fn validate_join_scalar_int64_comparison_predicate_expr(
    expression: &Expr,
    literal_expr: &Expr,
    op: BinaryOperator,
    context: &JoinSelectionContext<'_>,
) -> Result<JoinPredicateExpr, ViewPlanError> {
    let Some(comparison_op) = predicate_op(op) else {
        return unsupported("JOIN WHERE scalar Int64 comparison operator is not supported");
    };
    let left = join_scalar_int64_expression_match(
        expression,
        context.left_alias,
        context.left_catalog,
        &context.left_catalog.relation_schema.relation_id,
        |column| {
            column.column_id == context.left_key.column_id
                || column.column_id == context.left_value.column_id
        },
    )?;
    let right = join_scalar_int64_expression_match(
        expression,
        context.right_alias,
        context.right_catalog,
        &context.right_catalog.relation_schema.relation_id,
        |column| column.column_id != context.right_catalog.relation_schema.weight_column_id,
    )?;
    let (relation_id, left) = match (left, right) {
        (Some(matched), None) | (None, Some(matched)) => matched,
        _ => {
            return unsupported(
                "JOIN scalar Int64 predicate expressions must reference exactly one joined relation side and compare to a literal",
            )
        }
    };
    Ok(JoinPredicateExpr::ScalarInt64Comparison {
        relation_id,
        left: Box::new(left),
        comparison_op,
        literal: predicate_literal(literal_expr)?,
    })
}

fn join_scalar_int64_expression_match<F>(
    expression: &Expr,
    relation_alias: &str,
    catalog: &VelorixRelationCatalogV1,
    relation_id: &str,
    column_is_runtime_visible: F,
) -> Result<Option<(String, SupportedProjectionExpr)>, ViewPlanError>
where
    F: Fn(&RelationColumnV1) -> bool,
{
    let Ok(expression) =
        supported_filter_project_projection_expr(expression, catalog, Some(relation_alias))
    else {
        return Ok(None);
    };
    let column_ids = supported_projection_expr_column_ids(&expression);
    let [column_id] = column_ids.as_slice() else {
        return unsupported(
            "JOIN scalar Int64 predicate expressions must reference exactly one joined relation side and compare to a literal",
        );
    };
    let Some(column) = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == *column_id)
    else {
        return unsupported(
            "JOIN scalar Int64 predicate expressions must reference exactly one joined relation side and compare to a literal",
        );
    };
    if !column_is_runtime_visible(column) {
        return unsupported(
            "JOIN scalar Int64 predicate expression references a column that is not carried by the join runtime",
        );
    }
    Ok(Some((relation_id.to_string(), expression)))
}

fn join_predicate_target_column<'a>(
    expr: &Expr,
    context: &'a JoinSelectionContext<'_>,
) -> Result<(String, &'a RelationColumnV1), ViewPlanError> {
    let reference = qualified_column_ref(expr)?;
    if identifier_eq(reference.qualifier.as_str(), context.left_alias) {
        let column = qualified_ref_catalog_column(&reference, context.left_catalog)?;
        if column.column_id != context.left_key.column_id
            && column.column_id != context.left_value.column_id
        {
            return unsupported(
                "JOIN WHERE left predicate must reference the join key or sum input column",
            );
        }
        Ok((
            context.left_catalog.relation_schema.relation_id.clone(),
            column,
        ))
    } else if identifier_eq(reference.qualifier.as_str(), context.right_alias) {
        let column = qualified_ref_catalog_column(&reference, context.right_catalog)?;
        if column.column_id == context.right_catalog.relation_schema.weight_column_id {
            return unsupported("JOIN WHERE right predicate must not reference the weight column");
        }
        Ok((
            context.right_catalog.relation_schema.relation_id.clone(),
            column,
        ))
    } else {
        unsupported("JOIN WHERE column must reference a joined table alias")
    }
}

fn join_right_value_column_ids(
    predicates: &[JoinRowPredicate],
    right_catalog: &VelorixRelationCatalogV1,
    right_key: &RelationColumnV1,
) -> Result<Vec<String>, ViewPlanError> {
    let mut seen = BTreeSet::new();
    let mut right_value_column_ids = Vec::new();
    for predicate in predicates
        .iter()
        .filter(|predicate| predicate.relation_id == right_catalog.relation_schema.relation_id)
    {
        if predicate.predicate.column_id == right_key.column_id {
            continue;
        }
        let column = catalog_column_by_id(right_catalog, &predicate.predicate.column_id)?;
        if column.column_id == right_catalog.relation_schema.weight_column_id {
            return unsupported("JOIN WHERE right predicate must not reference the weight column");
        }
        if seen.insert(column.column_id.clone()) {
            right_value_column_ids.push(column.column_id.clone());
        }
    }
    Ok(right_value_column_ids)
}

fn join_scalar_int64_predicate_expr_column_ids(
    predicate_expr: &JoinPredicateExpr,
    relation_id: &str,
) -> Vec<String> {
    match predicate_expr {
        JoinPredicateExpr::Atom { .. } => Vec::new(),
        JoinPredicateExpr::ScalarInt64Comparison {
            relation_id: predicate_relation_id,
            left,
            ..
        } if predicate_relation_id == relation_id => supported_projection_expr_column_ids(left),
        JoinPredicateExpr::ScalarInt64Comparison { .. } => Vec::new(),
        JoinPredicateExpr::ScalarInt64ExpressionComparison {
            left_relation_id,
            left,
            right_relation_id,
            right,
            ..
        } => {
            let mut columns = Vec::new();
            if left_relation_id == relation_id {
                columns.extend(supported_projection_expr_column_ids(left));
            }
            if right_relation_id == relation_id {
                columns.extend(supported_projection_expr_column_ids(right));
            }
            columns
        }
        JoinPredicateExpr::And { left, right } | JoinPredicateExpr::Or { left, right } => {
            let mut columns = join_scalar_int64_predicate_expr_column_ids(left, relation_id);
            for column_id in join_scalar_int64_predicate_expr_column_ids(right, relation_id) {
                if !columns.iter().any(|existing| existing == &column_id) {
                    columns.push(column_id);
                }
            }
            columns
        }
    }
}

fn validate_selection_with_cte_source(
    select: &Select,
    cte_source: Option<&CteSource>,
    derived_source_selection: Option<&Expr>,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
    relation_alias: Option<&str>,
) -> Result<Option<RowPredicateExpr>, ViewPlanError> {
    let cte_predicate = match cte_source.and_then(|source| source.selection.as_ref()) {
        Some(selection) => {
            validate_row_predicate_expr(selection, catalog, key_column, value_column, None, true)
                .map(Some)?
        }
        None => None,
    };
    let derived_predicate = match derived_source_selection {
        Some(selection) => {
            validate_row_predicate_expr(selection, catalog, key_column, value_column, None, true)
                .map(Some)?
        }
        None => None,
    };
    let select_predicate = match &select.selection {
        Some(selection) => validate_row_predicate_expr(
            selection,
            catalog,
            key_column,
            value_column,
            relation_alias,
            true,
        )
        .map(Some)?,
        None => None,
    };
    Ok(combine_row_predicate_exprs(
        combine_row_predicate_exprs(cte_predicate, derived_predicate),
        select_predicate,
    ))
}

fn combine_row_predicate_exprs(
    left: Option<RowPredicateExpr>,
    right: Option<RowPredicateExpr>,
) -> Option<RowPredicateExpr> {
    match (left, right) {
        (Some(left), Some(right)) => Some(RowPredicateExpr::And {
            left: Box::new(left),
            right: Box::new(right),
        }),
        (Some(expr), None) | (None, Some(expr)) => Some(expr),
        (None, None) => None,
    }
}

fn combine_join_predicate_exprs(
    left: Option<JoinPredicateExpr>,
    right: Option<JoinPredicateExpr>,
) -> Option<JoinPredicateExpr> {
    match (left, right) {
        (Some(left), Some(right)) => Some(JoinPredicateExpr::And {
            left: Box::new(left),
            right: Box::new(right),
        }),
        (Some(expr), None) | (None, Some(expr)) => Some(expr),
        (None, None) => None,
    }
}

fn validate_join_cte_selection(
    cte_sources: &[CteSource],
    context: &JoinSelectionContext<'_>,
) -> Result<Option<JoinPredicateExpr>, ViewPlanError> {
    let mut combined = None;
    for cte_source in cte_sources {
        let Some(selection) = &cte_source.selection else {
            continue;
        };
        let alias = if cte_source.relation_id == context.left_catalog.relation_schema.relation_id {
            context.left_alias
        } else if cte_source.relation_id == context.right_catalog.relation_schema.relation_id {
            context.right_alias
        } else {
            return unsupported("JOIN identity CTE source relation does not match a joined input");
        };
        let predicate =
            validate_join_predicate_expr(&qualify_cte_predicate_expr(selection, alias), context)?;
        combined = combine_join_predicate_exprs(combined, Some(predicate));
    }
    Ok(combined)
}

fn validate_join_derived_table_selection(
    left_selection: Option<&Expr>,
    right_selection: Option<&Expr>,
    context: &JoinSelectionContext<'_>,
) -> Result<Option<JoinPredicateExpr>, ViewPlanError> {
    let mut combined = None;
    if let Some(selection) = left_selection {
        let predicate = validate_join_predicate_expr(
            &qualify_cte_predicate_expr(selection, context.left_alias),
            context,
        )?;
        combined = combine_join_predicate_exprs(combined, Some(predicate));
    }
    if let Some(selection) = right_selection {
        let predicate = validate_join_predicate_expr(
            &qualify_cte_predicate_expr(selection, context.right_alias),
            context,
        )?;
        combined = combine_join_predicate_exprs(combined, Some(predicate));
    }
    Ok(combined)
}

fn qualify_cte_predicate_expr(expr: &Expr, alias: &str) -> Expr {
    match expr {
        Expr::Identifier(identifier) => {
            Expr::CompoundIdentifier(vec![Ident::new(alias), identifier.clone()])
        }
        Expr::Nested(inner) => Expr::Nested(Box::new(qualify_cte_predicate_expr(inner, alias))),
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(qualify_cte_predicate_expr(expr, alias)),
        },
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(qualify_cte_predicate_expr(expr, alias)),
            negated: *negated,
            low: low.clone(),
            high: high.clone(),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(qualify_cte_predicate_expr(expr, alias)),
            list: list.clone(),
            negated: *negated,
        },
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => Expr::Like {
            negated: *negated,
            any: *any,
            expr: Box::new(qualify_cte_predicate_expr(expr, alias)),
            pattern: pattern.clone(),
            escape_char: escape_char.clone(),
        },
        Expr::IsNull(expr) => Expr::IsNull(Box::new(qualify_cte_predicate_expr(expr, alias))),
        Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(qualify_cte_predicate_expr(expr, alias))),
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(qualify_cte_predicate_expr(left, alias)),
            op: op.clone(),
            right: Box::new(qualify_cte_predicate_expr(right, alias)),
        },
        _ => expr.clone(),
    }
}

fn normalize_source_selection(
    selection: Option<&Expr>,
    source_alias: Option<&str>,
) -> Option<Expr> {
    selection.map(|expr| normalize_source_predicate_expr(expr, source_alias))
}

fn normalize_source_predicate_expr(expr: &Expr, source_alias: Option<&str>) -> Expr {
    match expr {
        Expr::CompoundIdentifier(parts) => {
            if let (Some(alias), [qualifier, column]) = (source_alias, parts.as_slice()) {
                if identifier_eq(qualifier.value.as_str(), alias) {
                    return Expr::Identifier(column.clone());
                }
            }
            expr.clone()
        }
        Expr::Nested(inner) => Expr::Nested(Box::new(normalize_source_predicate_expr(
            inner,
            source_alias,
        ))),
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(normalize_source_predicate_expr(expr, source_alias)),
        },
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(normalize_source_predicate_expr(expr, source_alias)),
            negated: *negated,
            low: Box::new(normalize_source_predicate_expr(low, source_alias)),
            high: Box::new(normalize_source_predicate_expr(high, source_alias)),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(normalize_source_predicate_expr(expr, source_alias)),
            list: list
                .iter()
                .map(|item| normalize_source_predicate_expr(item, source_alias))
                .collect(),
            negated: *negated,
        },
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => Expr::Like {
            negated: *negated,
            any: *any,
            expr: Box::new(normalize_source_predicate_expr(expr, source_alias)),
            pattern: Box::new(normalize_source_predicate_expr(pattern, source_alias)),
            escape_char: escape_char.clone(),
        },
        Expr::IsNull(expr) => Expr::IsNull(Box::new(normalize_source_predicate_expr(
            expr,
            source_alias,
        ))),
        Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(normalize_source_predicate_expr(
            expr,
            source_alias,
        ))),
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(normalize_source_predicate_expr(left, source_alias)),
            op: op.clone(),
            right: Box::new(normalize_source_predicate_expr(right, source_alias)),
        },
        _ => expr.clone(),
    }
}

fn validate_row_predicate_expr(
    selection: &Expr,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
    relation_alias: Option<&str>,
    allow_scalar_int64_comparison: bool,
) -> Result<RowPredicateExpr, ViewPlanError> {
    if let Expr::Nested(inner) = selection {
        return validate_row_predicate_expr(
            inner,
            catalog,
            key_column,
            value_column,
            relation_alias,
            allow_scalar_int64_comparison,
        );
    }
    if let Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr,
    } = selection
    {
        return Ok(negate_row_predicate_expr(validate_row_predicate_expr(
            expr,
            catalog,
            key_column,
            value_column,
            relation_alias,
            allow_scalar_int64_comparison,
        )?));
    }
    if let Expr::Between {
        expr,
        negated,
        low,
        high,
    } = selection
    {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported("WHERE column must reference a registered relation column");
        };
        if !predicate_column_is_runtime_visible(column, key_column, value_column) {
            return unsupported(
                "WHERE column must be the primary key or value column for this materialized runtime",
            );
        }
        return row_between_predicate_expr(column.column_id.clone(), low, high, *negated);
    }
    if let Expr::InList {
        expr,
        list,
        negated,
    } = selection
    {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported("WHERE column must reference a registered relation column");
        };
        if !predicate_column_is_runtime_visible(column, key_column, value_column) {
            return unsupported(
                "WHERE column must be the primary key or value column for this materialized runtime",
            );
        }
        return row_in_list_predicate_expr(
            column.column_id.clone(),
            column.nullable,
            list,
            *negated,
        );
    }
    if let Expr::Like {
        negated,
        any,
        expr,
        pattern,
        escape_char,
    } = selection
    {
        if *any || escape_char.is_some() {
            return unsupported("WHERE LIKE ANY and ESCAPE are not supported");
        }
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported("WHERE column must reference a registered relation column");
        };
        if !predicate_column_is_runtime_visible(column, key_column, value_column) {
            return unsupported(
                "WHERE column must be the primary key or value column for this materialized runtime",
            );
        }
        if !predicate_column_supports_like(column) {
            return unsupported("WHERE LIKE column must be text-like");
        }
        return row_like_predicate_expr(column.column_id.clone(), pattern, *negated);
    }
    if let Expr::IsNull(expr) = selection {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported("WHERE column must reference a registered relation column");
        };
        if !predicate_column_is_runtime_visible(column, key_column, value_column) {
            return unsupported(
                "WHERE column must be the primary key or value column for this materialized runtime",
            );
        }
        return Ok(row_null_predicate_expr(column.column_id.clone(), false));
    }
    if let Expr::IsNotNull(expr) = selection {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported("WHERE column must reference a registered relation column");
        };
        if !predicate_column_is_runtime_visible(column, key_column, value_column) {
            return unsupported(
                "WHERE column must be the primary key or value column for this materialized runtime",
            );
        }
        return Ok(row_null_predicate_expr(column.column_id.clone(), true));
    }
    if let Some((left, right, op)) = distinct_predicate_parts(selection) {
        let (column, literal_expr) = distinct_column_literal(
            left,
            right,
            catalog,
            relation_alias,
            "WHERE column must reference a registered relation column",
            "WHERE IS DISTINCT FROM must compare a catalog column to a literal",
        )?;
        if !predicate_column_is_runtime_visible(column, key_column, value_column) {
            return unsupported(
                "WHERE column must be the primary key or value column for this materialized runtime",
            );
        }
        return Ok(RowPredicateExpr::Atom {
            predicate: RowPredicate {
                column_id: column.column_id.clone(),
                op,
                literal: predicate_literal(literal_expr)?,
            },
        });
    }
    let Expr::BinaryOp { left, op, right } = selection else {
        return unsupported("WHERE currently supports one column/literal comparison");
    };
    if *op == BinaryOperator::And {
        return Ok(RowPredicateExpr::And {
            left: Box::new(validate_row_predicate_expr(
                left,
                catalog,
                key_column,
                value_column,
                relation_alias,
                allow_scalar_int64_comparison,
            )?),
            right: Box::new(validate_row_predicate_expr(
                right,
                catalog,
                key_column,
                value_column,
                relation_alias,
                allow_scalar_int64_comparison,
            )?),
        });
    }
    if *op == BinaryOperator::Or {
        return Ok(RowPredicateExpr::Or {
            left: Box::new(validate_row_predicate_expr(
                left,
                catalog,
                key_column,
                value_column,
                relation_alias,
                allow_scalar_int64_comparison,
            )?),
            right: Box::new(validate_row_predicate_expr(
                right,
                catalog,
                key_column,
                value_column,
                relation_alias,
                allow_scalar_int64_comparison,
            )?),
        });
    }
    let (column_expr, literal_expr, sql_op) = if expression_is_literal(right) {
        (left.as_ref(), right.as_ref(), op.clone())
    } else if expression_is_literal(left) {
        let Some(op) = reverse_predicate_op(op.clone()) else {
            return unsupported("WHERE comparison operator is not supported");
        };
        (right.as_ref(), left.as_ref(), op)
    } else {
        return unsupported("WHERE comparison must compare a catalog column to a literal");
    };
    if let Some(column) = expression_catalog_column(column_expr, catalog, relation_alias) {
        let Some(op) = predicate_op(sql_op.clone()) else {
            return unsupported("WHERE comparison operator is not supported");
        };
        let literal = predicate_literal(literal_expr)?;
        if !predicate_column_is_runtime_visible(column, key_column, value_column) {
            return unsupported(
                "WHERE column must be the primary key or value column for this materialized runtime",
            );
        }
        return Ok(RowPredicateExpr::Atom {
            predicate: RowPredicate {
                column_id: column.column_id.clone(),
                op,
                literal,
            },
        });
    }
    if !allow_scalar_int64_comparison {
        return unsupported("WHERE scalar Int64 predicate expressions are not supported here");
    }
    validate_scalar_int64_comparison_predicate_expr(
        column_expr,
        literal_expr,
        sql_op,
        catalog,
        relation_alias,
        None,
        |column| predicate_column_is_runtime_visible(column, key_column, value_column),
    )
}

fn validate_filter_project_selection(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_columns: &[ValidatedProjectionColumn],
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
) -> Result<Option<RowPredicateExpr>, ViewPlanError> {
    let Some(selection) = &select.selection else {
        return Ok(None);
    };
    validate_filter_project_predicate_expr(
        selection,
        catalog,
        key_column,
        value_columns,
        relation_alias,
        source_projection,
    )
    .map(Some)
}

#[allow(clippy::only_used_in_recursion)]
fn validate_filter_project_predicate_expr(
    selection: &Expr,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_columns: &[ValidatedProjectionColumn],
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
) -> Result<RowPredicateExpr, ViewPlanError> {
    if let Expr::Nested(inner) = selection {
        return validate_filter_project_predicate_expr(
            inner,
            catalog,
            key_column,
            value_columns,
            relation_alias,
            source_projection,
        );
    }
    if let Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr,
    } = selection
    {
        return Ok(negate_row_predicate_expr(
            validate_filter_project_predicate_expr(
                expr,
                catalog,
                key_column,
                value_columns,
                relation_alias,
                source_projection,
            )?,
        ));
    }
    if let Expr::Between {
        expr,
        negated,
        low,
        high,
    } = selection
    {
        let Some(column) =
            expression_filter_project_column(expr, catalog, relation_alias, source_projection)
        else {
            return unsupported(
                "filter/project WHERE column must reference a registered relation column",
            );
        };
        validate_filter_project_predicate_column(catalog, column)?;
        return row_between_predicate_expr(column.column_id.clone(), low, high, *negated);
    }
    if let Expr::InList {
        expr,
        list,
        negated,
    } = selection
    {
        let Some(column) =
            expression_filter_project_column(expr, catalog, relation_alias, source_projection)
        else {
            return unsupported(
                "filter/project WHERE column must reference a registered relation column",
            );
        };
        validate_filter_project_predicate_column(catalog, column)?;
        return row_in_list_predicate_expr(
            column.column_id.clone(),
            column.nullable,
            list,
            *negated,
        );
    }
    if let Expr::Like {
        negated,
        any,
        expr,
        pattern,
        escape_char,
    } = selection
    {
        if *any || escape_char.is_some() {
            return unsupported("filter/project WHERE LIKE ANY and ESCAPE are not supported");
        }
        let Some(column) =
            expression_filter_project_column(expr, catalog, relation_alias, source_projection)
        else {
            return unsupported(
                "filter/project WHERE column must reference a registered relation column",
            );
        };
        validate_filter_project_predicate_column(catalog, column)?;
        if !predicate_column_supports_like(column) {
            return unsupported("filter/project WHERE LIKE column must be text-like");
        }
        return row_like_predicate_expr(column.column_id.clone(), pattern, *negated);
    }
    if let Expr::IsNull(expr) = selection {
        let Some(column) =
            expression_filter_project_column(expr, catalog, relation_alias, source_projection)
        else {
            return unsupported(
                "filter/project WHERE column must reference a registered relation column",
            );
        };
        validate_filter_project_predicate_column(catalog, column)?;
        return Ok(row_null_predicate_expr(column.column_id.clone(), false));
    }
    if let Expr::IsNotNull(expr) = selection {
        let Some(column) =
            expression_filter_project_column(expr, catalog, relation_alias, source_projection)
        else {
            return unsupported(
                "filter/project WHERE column must reference a registered relation column",
            );
        };
        validate_filter_project_predicate_column(catalog, column)?;
        return Ok(row_null_predicate_expr(column.column_id.clone(), true));
    }
    if let Some((left, right, op)) = distinct_predicate_parts(selection) {
        let (column, literal_expr) = distinct_filter_project_column_literal(
            left,
            right,
            catalog,
            relation_alias,
            source_projection,
            "filter/project WHERE column must reference a registered relation column",
            "filter/project WHERE IS DISTINCT FROM must compare a catalog column to a literal",
        )?;
        validate_filter_project_predicate_column(catalog, column)?;
        return Ok(RowPredicateExpr::Atom {
            predicate: RowPredicate {
                column_id: column.column_id.clone(),
                op,
                literal: predicate_literal(literal_expr)?,
            },
        });
    }
    let Expr::BinaryOp { left, op, right } = selection else {
        return unsupported("filter/project WHERE currently supports column/literal comparisons");
    };
    if *op == BinaryOperator::And {
        return Ok(RowPredicateExpr::And {
            left: Box::new(validate_filter_project_predicate_expr(
                left,
                catalog,
                key_column,
                value_columns,
                relation_alias,
                source_projection,
            )?),
            right: Box::new(validate_filter_project_predicate_expr(
                right,
                catalog,
                key_column,
                value_columns,
                relation_alias,
                source_projection,
            )?),
        });
    }
    if *op == BinaryOperator::Or {
        return Ok(RowPredicateExpr::Or {
            left: Box::new(validate_filter_project_predicate_expr(
                left,
                catalog,
                key_column,
                value_columns,
                relation_alias,
                source_projection,
            )?),
            right: Box::new(validate_filter_project_predicate_expr(
                right,
                catalog,
                key_column,
                value_columns,
                relation_alias,
                source_projection,
            )?),
        });
    }
    let (column_expr, literal_expr, sql_op) = if expression_is_literal(right) {
        (left.as_ref(), right.as_ref(), op.clone())
    } else if expression_is_literal(left) {
        let Some(op) = reverse_predicate_op(op.clone()) else {
            return unsupported("filter/project WHERE comparison operator is not supported");
        };
        (right.as_ref(), left.as_ref(), op)
    } else {
        return validate_scalar_int64_expression_comparison_predicate_expr(
            left,
            right,
            op.clone(),
            catalog,
            relation_alias,
            source_projection,
            |column| column.column_id != catalog.relation_schema.weight_column_id,
        );
    };
    if let Some(column) =
        expression_filter_project_column(column_expr, catalog, relation_alias, source_projection)
    {
        validate_filter_project_predicate_column(catalog, column)?;
        let Some(op) = predicate_op(sql_op.clone()) else {
            return unsupported("filter/project WHERE comparison operator is not supported");
        };
        return Ok(RowPredicateExpr::Atom {
            predicate: RowPredicate {
                column_id: column.column_id.clone(),
                op,
                literal: predicate_literal(literal_expr)?,
            },
        });
    }
    validate_scalar_int64_comparison_predicate_expr(
        column_expr,
        literal_expr,
        sql_op,
        catalog,
        relation_alias,
        source_projection,
        |column| column.column_id != catalog.relation_schema.weight_column_id,
    )
}

fn validate_filter_project_predicate_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    if column.column_id == catalog.relation_schema.weight_column_id {
        return unsupported("filter/project WHERE must not reference the weight column");
    }
    Ok(())
}

fn validate_latest_selection(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
    ordering_column: &RelationColumnV1,
    relation_alias: Option<&str>,
) -> Result<Option<RowPredicateExpr>, ViewPlanError> {
    let Some(selection) = &select.selection else {
        return Ok(None);
    };
    validate_latest_predicate_expr(
        selection,
        catalog,
        key_column,
        value_column,
        ordering_column,
        relation_alias,
    )
    .map(Some)
}

fn validate_row_number_selection(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    partition_column: &RelationColumnV1,
    order_column: &RelationColumnV1,
    relation_alias: Option<&str>,
) -> Result<Option<RowPredicateExpr>, ViewPlanError> {
    validate_latest_selection(
        select,
        catalog,
        key_column,
        partition_column,
        order_column,
        relation_alias,
    )
}

fn validate_row_number_qualify(
    select: &Select,
    output_row_number_column_id: &str,
) -> Result<Option<usize>, ViewPlanError> {
    let Some(qualify) = &select.qualify else {
        return Ok(None);
    };
    let Expr::BinaryOp { left, op, right } = qualify else {
        return unsupported("ROW_NUMBER QUALIFY must be rank <= <positive integer> or rank = 1");
    };
    if !expression_identifier(left)
        .is_some_and(|identifier| identifier_eq(identifier, output_row_number_column_id))
    {
        return unsupported("ROW_NUMBER QUALIFY must be rank <= <positive integer> or rank = 1");
    }
    Ok(Some(validate_row_number_rank_limit_predicate(op, right)?))
}

fn validate_row_number_rank_limit_predicate(
    op: &BinaryOperator,
    literal: &Expr,
) -> Result<usize, ViewPlanError> {
    let limit = validate_row_number_rank_limit_literal(literal)?;
    match op {
        BinaryOperator::LtEq => Ok(limit),
        BinaryOperator::Eq if limit == 1 => Ok(1),
        _ => {
            unsupported("ROW_NUMBER rank predicate must be rank <= <positive integer> or rank = 1")
        }
    }
}

fn validate_row_number_rank_limit_literal(expr: &Expr) -> Result<usize, ViewPlanError> {
    let Expr::Value(value) = expr else {
        return unsupported("ROW_NUMBER QUALIFY rank limit must be a positive integer literal");
    };
    let SqlValue::Number(text, false) = &value.value else {
        return unsupported("ROW_NUMBER QUALIFY rank limit must be a positive integer literal");
    };
    let limit = text
        .parse::<usize>()
        .map_err(|_| ViewPlanError::UnsupportedShape {
            reason: "ROW_NUMBER QUALIFY rank limit must be a positive integer literal".to_string(),
        })?;
    if limit == 0 {
        return unsupported("ROW_NUMBER QUALIFY rank limit must be greater than zero");
    }
    Ok(limit)
}

fn validate_tumbling_selection(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
    event_time_column: &RelationColumnV1,
    relation_alias: Option<&str>,
) -> Result<Option<RowPredicateExpr>, ViewPlanError> {
    let Some(selection) = &select.selection else {
        return Ok(None);
    };
    validate_tumbling_predicate_expr(
        selection,
        catalog,
        key_column,
        value_column,
        event_time_column,
        relation_alias,
    )
    .map(Some)
}

fn validate_tumbling_cte_selection(
    cte_source: Option<&CteSource>,
    derived_source_selection: Option<&Expr>,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
    event_time_column: &RelationColumnV1,
) -> Result<Option<RowPredicateExpr>, ViewPlanError> {
    let cte_predicate = match cte_source.and_then(|source| source.selection.as_ref()) {
        Some(selection) => validate_tumbling_predicate_expr(
            selection,
            catalog,
            key_column,
            value_column,
            event_time_column,
            None,
        )
        .map(Some)?,
        None => None,
    };
    let derived_predicate = match derived_source_selection {
        Some(selection) => validate_tumbling_predicate_expr(
            selection,
            catalog,
            key_column,
            value_column,
            event_time_column,
            None,
        )
        .map(Some)?,
        None => None,
    };
    Ok(combine_row_predicate_exprs(
        cte_predicate,
        derived_predicate,
    ))
}

fn validate_tumbling_predicate_expr(
    selection: &Expr,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
    event_time_column: &RelationColumnV1,
    relation_alias: Option<&str>,
) -> Result<RowPredicateExpr, ViewPlanError> {
    if let Expr::Nested(inner) = selection {
        return validate_tumbling_predicate_expr(
            inner,
            catalog,
            key_column,
            value_column,
            event_time_column,
            relation_alias,
        );
    }
    if let Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr,
    } = selection
    {
        return Ok(negate_row_predicate_expr(validate_tumbling_predicate_expr(
            expr,
            catalog,
            key_column,
            value_column,
            event_time_column,
            relation_alias,
        )?));
    }
    if let Expr::Between {
        expr,
        negated,
        low,
        high,
    } = selection
    {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported("window WHERE column must reference a registered relation column");
        };
        if column.column_id != key_column.column_id
            && column.column_id != value_column.column_id
            && column.column_id != event_time_column.column_id
        {
            return unsupported("window WHERE column must be the key, value, or event-time column");
        }
        return row_between_predicate_expr(column.column_id.clone(), low, high, *negated);
    }
    if let Expr::InList {
        expr,
        list,
        negated,
    } = selection
    {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported("window WHERE column must reference a registered relation column");
        };
        if column.column_id != key_column.column_id
            && column.column_id != value_column.column_id
            && column.column_id != event_time_column.column_id
        {
            return unsupported("window WHERE column must be the key, value, or event-time column");
        }
        return row_in_list_predicate_expr(
            column.column_id.clone(),
            column.nullable,
            list,
            *negated,
        );
    }
    if let Expr::Like {
        negated,
        any,
        expr,
        pattern,
        escape_char,
    } = selection
    {
        if *any || escape_char.is_some() {
            return unsupported("window WHERE LIKE ANY and ESCAPE are not supported");
        }
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported("window WHERE column must reference a registered relation column");
        };
        if column.column_id != key_column.column_id
            && column.column_id != value_column.column_id
            && column.column_id != event_time_column.column_id
        {
            return unsupported("window WHERE column must be the key, value, or event-time column");
        }
        if !predicate_column_supports_like(column) {
            return unsupported("window WHERE LIKE column must be text-like");
        }
        return row_like_predicate_expr(column.column_id.clone(), pattern, *negated);
    }
    if let Expr::IsNull(expr) = selection {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported("window WHERE column must reference a registered relation column");
        };
        if column.column_id != key_column.column_id
            && column.column_id != value_column.column_id
            && column.column_id != event_time_column.column_id
        {
            return unsupported("window WHERE column must be the key, value, or event-time column");
        }
        return Ok(row_null_predicate_expr(column.column_id.clone(), false));
    }
    if let Expr::IsNotNull(expr) = selection {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported("window WHERE column must reference a registered relation column");
        };
        if column.column_id != key_column.column_id
            && column.column_id != value_column.column_id
            && column.column_id != event_time_column.column_id
        {
            return unsupported("window WHERE column must be the key, value, or event-time column");
        }
        return Ok(row_null_predicate_expr(column.column_id.clone(), true));
    }
    if let Some((left, right, op)) = distinct_predicate_parts(selection) {
        let (column, literal_expr) = distinct_column_literal(
            left,
            right,
            catalog,
            relation_alias,
            "window WHERE column must reference a registered relation column",
            "window WHERE IS DISTINCT FROM must compare a catalog column to a literal",
        )?;
        if column.column_id != key_column.column_id
            && column.column_id != value_column.column_id
            && column.column_id != event_time_column.column_id
        {
            return unsupported("window WHERE column must be the key, value, or event-time column");
        }
        return Ok(RowPredicateExpr::Atom {
            predicate: RowPredicate {
                column_id: column.column_id.clone(),
                op,
                literal: predicate_literal(literal_expr)?,
            },
        });
    }
    let Expr::BinaryOp { left, op, right } = selection else {
        return unsupported("window WHERE currently supports column/literal comparisons");
    };
    if *op == BinaryOperator::And {
        return Ok(RowPredicateExpr::And {
            left: Box::new(validate_tumbling_predicate_expr(
                left,
                catalog,
                key_column,
                value_column,
                event_time_column,
                relation_alias,
            )?),
            right: Box::new(validate_tumbling_predicate_expr(
                right,
                catalog,
                key_column,
                value_column,
                event_time_column,
                relation_alias,
            )?),
        });
    }
    if *op == BinaryOperator::Or {
        return Ok(RowPredicateExpr::Or {
            left: Box::new(validate_tumbling_predicate_expr(
                left,
                catalog,
                key_column,
                value_column,
                event_time_column,
                relation_alias,
            )?),
            right: Box::new(validate_tumbling_predicate_expr(
                right,
                catalog,
                key_column,
                value_column,
                event_time_column,
                relation_alias,
            )?),
        });
    }
    let (column_expr, literal_expr, op) = if expression_is_literal(right) {
        (left.as_ref(), right.as_ref(), op.clone())
    } else if expression_is_literal(left) {
        let Some(op) = reverse_predicate_op(op.clone()) else {
            return unsupported("window WHERE comparison operator is not supported");
        };
        (right.as_ref(), left.as_ref(), op)
    } else {
        return unsupported("window WHERE must compare a catalog column to a literal");
    };
    let Some(column) = expression_catalog_column(column_expr, catalog, relation_alias) else {
        return unsupported("window WHERE column must reference a registered relation column");
    };
    if column.column_id != key_column.column_id
        && column.column_id != value_column.column_id
        && column.column_id != event_time_column.column_id
    {
        return unsupported("window WHERE column must be the key, value, or event-time column");
    }
    let Some(op) = predicate_op(op) else {
        return unsupported("window WHERE comparison operator is not supported");
    };
    Ok(RowPredicateExpr::Atom {
        predicate: RowPredicate {
            column_id: column.column_id.clone(),
            op,
            literal: predicate_literal(literal_expr)?,
        },
    })
}

fn validate_latest_predicate_expr(
    selection: &Expr,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
    ordering_column: &RelationColumnV1,
    relation_alias: Option<&str>,
) -> Result<RowPredicateExpr, ViewPlanError> {
    if let Expr::Nested(inner) = selection {
        return validate_latest_predicate_expr(
            inner,
            catalog,
            key_column,
            value_column,
            ordering_column,
            relation_alias,
        );
    }
    if let Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr,
    } = selection
    {
        return Ok(negate_row_predicate_expr(validate_latest_predicate_expr(
            expr,
            catalog,
            key_column,
            value_column,
            ordering_column,
            relation_alias,
        )?));
    }
    if let Expr::Between {
        expr,
        negated,
        low,
        high,
    } = selection
    {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported(
                "latest-by-key WHERE column must reference a registered relation column",
            );
        };
        if column.column_id != key_column.column_id
            && column.column_id != value_column.column_id
            && column.column_id != ordering_column.column_id
        {
            return unsupported(
                "latest-by-key WHERE column must be the key, value, or ordering column",
            );
        }
        return row_between_predicate_expr(column.column_id.clone(), low, high, *negated);
    }
    if let Expr::InList {
        expr,
        list,
        negated,
    } = selection
    {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported(
                "latest-by-key WHERE column must reference a registered relation column",
            );
        };
        if column.column_id != key_column.column_id
            && column.column_id != value_column.column_id
            && column.column_id != ordering_column.column_id
        {
            return unsupported(
                "latest-by-key WHERE column must be the key, value, or ordering column",
            );
        }
        return row_in_list_predicate_expr(
            column.column_id.clone(),
            column.nullable,
            list,
            *negated,
        );
    }
    if let Expr::Like {
        negated,
        any,
        expr,
        pattern,
        escape_char,
    } = selection
    {
        if *any || escape_char.is_some() {
            return unsupported("latest-by-key WHERE LIKE ANY and ESCAPE are not supported");
        }
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported(
                "latest-by-key WHERE column must reference a registered relation column",
            );
        };
        if column.column_id != key_column.column_id
            && column.column_id != value_column.column_id
            && column.column_id != ordering_column.column_id
        {
            return unsupported(
                "latest-by-key WHERE column must be the key, value, or ordering column",
            );
        }
        if !predicate_column_supports_like(column) {
            return unsupported("latest-by-key WHERE LIKE column must be text-like");
        }
        return row_like_predicate_expr(column.column_id.clone(), pattern, *negated);
    }
    if let Expr::IsNull(expr) = selection {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported(
                "latest-by-key WHERE column must reference a registered relation column",
            );
        };
        if column.column_id != key_column.column_id
            && column.column_id != value_column.column_id
            && column.column_id != ordering_column.column_id
        {
            return unsupported(
                "latest-by-key WHERE column must be the key, value, or ordering column",
            );
        }
        return Ok(row_null_predicate_expr(column.column_id.clone(), false));
    }
    if let Expr::IsNotNull(expr) = selection {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported(
                "latest-by-key WHERE column must reference a registered relation column",
            );
        };
        if column.column_id != key_column.column_id
            && column.column_id != value_column.column_id
            && column.column_id != ordering_column.column_id
        {
            return unsupported(
                "latest-by-key WHERE column must be the key, value, or ordering column",
            );
        }
        return Ok(row_null_predicate_expr(column.column_id.clone(), true));
    }
    if let Some((left, right, op)) = distinct_predicate_parts(selection) {
        let (column, literal_expr) = distinct_column_literal(
            left,
            right,
            catalog,
            relation_alias,
            "latest-by-key WHERE column must reference a registered relation column",
            "latest-by-key WHERE IS DISTINCT FROM must compare a catalog column to a literal",
        )?;
        if column.column_id != key_column.column_id
            && column.column_id != value_column.column_id
            && column.column_id != ordering_column.column_id
        {
            return unsupported(
                "latest-by-key WHERE column must be the key, value, or ordering column",
            );
        }
        return Ok(RowPredicateExpr::Atom {
            predicate: RowPredicate {
                column_id: column.column_id.clone(),
                op,
                literal: predicate_literal(literal_expr)?,
            },
        });
    }
    let Expr::BinaryOp { left, op, right } = selection else {
        return unsupported("latest-by-key WHERE currently supports column/literal comparisons");
    };
    if *op == BinaryOperator::And {
        return Ok(RowPredicateExpr::And {
            left: Box::new(validate_latest_predicate_expr(
                left,
                catalog,
                key_column,
                value_column,
                ordering_column,
                relation_alias,
            )?),
            right: Box::new(validate_latest_predicate_expr(
                right,
                catalog,
                key_column,
                value_column,
                ordering_column,
                relation_alias,
            )?),
        });
    }
    if *op == BinaryOperator::Or {
        return Ok(RowPredicateExpr::Or {
            left: Box::new(validate_latest_predicate_expr(
                left,
                catalog,
                key_column,
                value_column,
                ordering_column,
                relation_alias,
            )?),
            right: Box::new(validate_latest_predicate_expr(
                right,
                catalog,
                key_column,
                value_column,
                ordering_column,
                relation_alias,
            )?),
        });
    }
    let (column_expr, literal_expr, op) = if expression_is_literal(right) {
        (left.as_ref(), right.as_ref(), op.clone())
    } else if expression_is_literal(left) {
        let Some(op) = reverse_predicate_op(op.clone()) else {
            return unsupported("latest-by-key WHERE comparison operator is not supported");
        };
        (right.as_ref(), left.as_ref(), op)
    } else {
        return unsupported("latest-by-key WHERE must compare a catalog column to a literal");
    };
    let Some(column) = expression_catalog_column(column_expr, catalog, relation_alias) else {
        return unsupported(
            "latest-by-key WHERE column must reference a registered relation column",
        );
    };
    if column.column_id != key_column.column_id
        && column.column_id != value_column.column_id
        && column.column_id != ordering_column.column_id
    {
        return unsupported(
            "latest-by-key WHERE column must be the key, value, or ordering column",
        );
    }
    let Some(op) = predicate_op(op) else {
        return unsupported("latest-by-key WHERE comparison operator is not supported");
    };
    Ok(RowPredicateExpr::Atom {
        predicate: RowPredicate {
            column_id: column.column_id.clone(),
            op,
            literal: predicate_literal(literal_expr)?,
        },
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_having(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
    aggregate_outputs: &[SupportedAggregateOutput],
    aggregate_filter_expr: Option<&RowPredicateExpr>,
    aggregate_filter_exprs: &BTreeMap<String, RowPredicateExpr>,
    relation_alias: Option<&str>,
) -> Result<Option<AggregateOutputPredicateExpr>, ViewPlanError> {
    let Some(expr) = &select.having else {
        return Ok(None);
    };
    let context = HavingBindingContext {
        catalog,
        key_column,
        value_column,
        aggregate_outputs,
        aggregate_filter_expr,
        aggregate_filter_exprs,
        relation_alias,
    };
    validate_having_expr(expr, &context).map(Some)
}

struct HavingBindingContext<'a> {
    catalog: &'a VelorixRelationCatalogV1,
    key_column: &'a RelationColumnV1,
    value_column: &'a RelationColumnV1,
    aggregate_outputs: &'a [SupportedAggregateOutput],
    aggregate_filter_expr: Option<&'a RowPredicateExpr>,
    aggregate_filter_exprs: &'a BTreeMap<String, RowPredicateExpr>,
    relation_alias: Option<&'a str>,
}

fn validate_having_expr(
    expr: &Expr,
    context: &HavingBindingContext<'_>,
) -> Result<AggregateOutputPredicateExpr, ViewPlanError> {
    if let Expr::Nested(inner) = expr {
        return validate_having_expr(inner, context);
    }
    if let Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr,
    } = expr
    {
        return Ok(negate_aggregate_predicate_expr(validate_having_expr(
            expr, context,
        )?));
    }
    if let Expr::Between {
        expr,
        negated,
        low,
        high,
    } = expr
    {
        let output_column_id = having_output_column_id(expr, context)?;
        return aggregate_between_predicate_expr(output_column_id, low, high, *negated);
    }
    if let Expr::InList {
        expr,
        list,
        negated,
    } = expr
    {
        let output_column_id = having_output_column_id(expr, context)?;
        return aggregate_in_list_predicate_expr(output_column_id, list, *negated);
    }
    if let Expr::IsNull(expr) = expr {
        let output_column_id = having_output_column_id(expr, context)?;
        return Ok(aggregate_null_predicate_expr(output_column_id, false));
    }
    if let Expr::IsNotNull(expr) = expr {
        let output_column_id = having_output_column_id(expr, context)?;
        return Ok(aggregate_null_predicate_expr(output_column_id, true));
    }
    if let Some((left, right, op)) = distinct_predicate_parts(expr) {
        let (output_expr, literal_expr) = if expression_is_literal(right) {
            (left, right)
        } else if expression_is_literal(left) {
            (right, left)
        } else {
            return unsupported(
                "HAVING IS DISTINCT FROM must compare an aggregate output to a literal",
            );
        };
        return Ok(AggregateOutputPredicateExpr::Atom {
            predicate: AggregateOutputPredicate {
                output_column_id: having_output_column_id(output_expr, context)?,
                op,
                literal: predicate_literal(literal_expr)?,
            },
        });
    }
    let Expr::BinaryOp { left, op, right } = expr else {
        return unsupported("HAVING currently supports one aggregate-output comparison");
    };
    if *op == BinaryOperator::And {
        return Ok(AggregateOutputPredicateExpr::And {
            left: Box::new(validate_having_expr(left, context)?),
            right: Box::new(validate_having_expr(right, context)?),
        });
    }
    if *op == BinaryOperator::Or {
        return Ok(AggregateOutputPredicateExpr::Or {
            left: Box::new(validate_having_expr(left, context)?),
            right: Box::new(validate_having_expr(right, context)?),
        });
    }
    let (output_expr, literal_expr, op) = if expression_is_literal(right) {
        (left.as_ref(), right.as_ref(), op.clone())
    } else if expression_is_literal(left) {
        let Some(op) = reverse_predicate_op(op.clone()) else {
            return unsupported("HAVING comparison operator is not supported");
        };
        (right.as_ref(), left.as_ref(), op)
    } else {
        return unsupported("HAVING comparison must compare an aggregate output to a literal");
    };
    let Some(op) = predicate_op(op) else {
        return unsupported("HAVING comparison operator is not supported");
    };
    let output_column_id = having_output_column_id(output_expr, context)?;
    Ok(AggregateOutputPredicateExpr::Atom {
        predicate: AggregateOutputPredicate {
            output_column_id,
            op,
            literal: predicate_literal(literal_expr)?,
        },
    })
}

fn having_output_column_id(
    expr: &Expr,
    context: &HavingBindingContext<'_>,
) -> Result<String, ViewPlanError> {
    if let Some(identifier) = expression_identifier(expr) {
        if context
            .aggregate_outputs
            .iter()
            .any(|output| output.output_column_id == identifier)
        {
            return Ok(identifier.to_string());
        }
        return unsupported("HAVING identifier must reference a projected aggregate output");
    }
    let Expr::Function(function) = expr else {
        return unsupported("HAVING expression must reference a projected aggregate output");
    };
    let Some(function_name) = single_object_name_identifier(&function.name) else {
        return unsupported("HAVING aggregate function name must be unqualified");
    };
    let canonical_function = function_name.to_ascii_lowercase();
    let mut aggregate_function = match canonical_function.as_str() {
        "sum" => LogicalPlanAggregateFunctionV1::Sum,
        "count" => LogicalPlanAggregateFunctionV1::Count,
        "min" => LogicalPlanAggregateFunctionV1::Min,
        "max" => LogicalPlanAggregateFunctionV1::Max,
        "avg" => LogicalPlanAggregateFunctionV1::Avg,
        _ => return unsupported("HAVING aggregate function is not supported"),
    };
    if !matches!(function.parameters, FunctionArguments::None)
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported("HAVING aggregate modifiers are not supported");
    }
    let having_filter_expr = function
        .filter
        .as_deref()
        .map(|filter| {
            validate_row_predicate_expr(
                filter,
                context.catalog,
                context.key_column,
                context.value_column,
                context.relation_alias,
                false,
            )
        })
        .transpose()?;
    let (input_column_id, input_expression) = match aggregate_function {
        LogicalPlanAggregateFunctionV1::Count => {
            let FunctionArguments::List(arguments) = &function.args else {
                return unsupported("HAVING count arguments must be a simple argument list");
            };
            if matches!(
                arguments.duplicate_treatment,
                Some(DuplicateTreatment::Distinct)
            ) {
                aggregate_function = LogicalPlanAggregateFunctionV1::CountDistinct;
            }
            (
                validate_count_argument(arguments, context.catalog, context.relation_alias, None)?
                    .map(|column| column.column_id.clone()),
                None,
            )
        }
        _ => {
            let FunctionArguments::List(arguments) = &function.args else {
                return unsupported("HAVING aggregate arguments must be a simple argument list");
            };
            if !arguments.clauses.is_empty() {
                return unsupported("HAVING aggregate argument clauses are not supported");
            }
            if arguments.duplicate_treatment.is_some() {
                return unsupported("HAVING DISTINCT value aggregates are not supported");
            }
            let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice()
            else {
                return unsupported("HAVING aggregate value functions require one column argument");
            };
            if let Some(column) =
                expression_column(argument, context.catalog, context.relation_alias)
            {
                (Some(column.column_id.clone()), None)
            } else {
                let expression = supported_filter_project_projection_expr(
                    argument,
                    context.catalog,
                    context.relation_alias,
                )?;
                let column_ids = supported_projection_expr_column_ids(&expression);
                let [column_id] = column_ids.as_slice() else {
                    return unsupported(
                        "HAVING aggregate input expressions must reference exactly one Int64 value column",
                    );
                };
                (Some(column_id.clone()), Some(expression))
            }
        }
    };
    let function_matches = context
        .aggregate_outputs
        .iter()
        .filter(|output| {
            output.function == aggregate_function
                && output.input_column_id == input_column_id
                && output.input_expression == input_expression
        })
        .collect::<Vec<_>>();
    let matches = function_matches
        .iter()
        .copied()
        .filter(|output| {
            aggregate_output_filter_matches(
                having_filter_expr.as_ref(),
                &output.output_column_id,
                context.aggregate_filter_expr,
                context.aggregate_filter_exprs,
            )
        })
        .collect::<Vec<_>>();
    let [output] = matches.as_slice() else {
        if having_filter_expr.is_none()
            && function_matches.iter().any(|output| {
                context.aggregate_filter_expr.is_some()
                    || context
                        .aggregate_filter_exprs
                        .contains_key(&output.output_column_id)
            })
        {
            return unsupported(
                "HAVING aggregate function must reference one unfiltered projected aggregate output",
            );
        }
        return Err(ViewPlanError::UnsupportedShape {
            reason: "HAVING expression must reference a projected aggregate output".to_string(),
        });
    };
    Ok(output.output_column_id.clone())
}

fn expression_catalog_column<'a>(
    expr: &Expr,
    catalog: &'a VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
) -> Option<&'a RelationColumnV1> {
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| expression_references_catalog_column(expr, catalog, column, relation_alias))
}

fn expression_filter_project_column<'a>(
    expr: &Expr,
    catalog: &'a VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
) -> Option<&'a RelationColumnV1> {
    match source_projection {
        Some(projection) => {
            source_projection_expression_column(expr, catalog, relation_alias, projection)
        }
        None => expression_catalog_column(expr, catalog, relation_alias),
    }
}

fn expression_is_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Value(_)
            | Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: _
            }
    )
}

fn expression_identifier(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(identifier) => Some(identifier.value.as_str()),
        _ => None,
    }
}

fn predicate_op(op: BinaryOperator) -> Option<PredicateOp> {
    match op {
        BinaryOperator::Eq => Some(PredicateOp::Eq),
        BinaryOperator::NotEq => Some(PredicateOp::NotEq),
        BinaryOperator::Gt => Some(PredicateOp::Gt),
        BinaryOperator::GtEq => Some(PredicateOp::GtEq),
        BinaryOperator::Lt => Some(PredicateOp::Lt),
        BinaryOperator::LtEq => Some(PredicateOp::LtEq),
        _ => None,
    }
}

fn distinct_predicate_parts(expr: &Expr) -> Option<(&Expr, &Expr, PredicateOp)> {
    match expr {
        Expr::IsDistinctFrom(left, right) => {
            Some((left.as_ref(), right.as_ref(), PredicateOp::IsDistinctFrom))
        }
        Expr::IsNotDistinctFrom(left, right) => Some((
            left.as_ref(),
            right.as_ref(),
            PredicateOp::IsNotDistinctFrom,
        )),
        _ => None,
    }
}

fn distinct_column_literal<'a>(
    left: &'a Expr,
    right: &'a Expr,
    catalog: &'a VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
    column_error: &str,
    comparison_error: &str,
) -> Result<(&'a RelationColumnV1, &'a Expr), ViewPlanError> {
    if expression_is_literal(right) {
        let Some(column) = expression_catalog_column(left, catalog, relation_alias) else {
            return unsupported(column_error);
        };
        return Ok((column, right));
    }
    if expression_is_literal(left) {
        let Some(column) = expression_catalog_column(right, catalog, relation_alias) else {
            return unsupported(column_error);
        };
        return Ok((column, left));
    }
    unsupported(comparison_error)
}

fn distinct_filter_project_column_literal<'a>(
    left: &'a Expr,
    right: &'a Expr,
    catalog: &'a VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
    column_error: &str,
    comparison_error: &str,
) -> Result<(&'a RelationColumnV1, &'a Expr), ViewPlanError> {
    if expression_is_literal(right) {
        let Some(column) =
            expression_filter_project_column(left, catalog, relation_alias, source_projection)
        else {
            return unsupported(column_error);
        };
        return Ok((column, right));
    }
    if expression_is_literal(left) {
        let Some(column) =
            expression_filter_project_column(right, catalog, relation_alias, source_projection)
        else {
            return unsupported(column_error);
        };
        return Ok((column, left));
    }
    unsupported(comparison_error)
}

fn negate_predicate_op(op: PredicateOp) -> PredicateOp {
    match op {
        PredicateOp::Eq => PredicateOp::NotEq,
        PredicateOp::NotEq => PredicateOp::Eq,
        PredicateOp::IsDistinctFrom => PredicateOp::IsNotDistinctFrom,
        PredicateOp::IsNotDistinctFrom => PredicateOp::IsDistinctFrom,
        PredicateOp::Like => PredicateOp::NotLike,
        PredicateOp::NotLike => PredicateOp::Like,
        PredicateOp::Gt => PredicateOp::LtEq,
        PredicateOp::GtEq => PredicateOp::Lt,
        PredicateOp::Lt => PredicateOp::GtEq,
        PredicateOp::LtEq => PredicateOp::Gt,
        PredicateOp::IsNull => PredicateOp::IsNotNull,
        PredicateOp::IsNotNull => PredicateOp::IsNull,
    }
}

fn negate_row_predicate_expr(expr: RowPredicateExpr) -> RowPredicateExpr {
    match expr {
        RowPredicateExpr::Atom { mut predicate } => {
            predicate.op = negate_predicate_op(predicate.op);
            RowPredicateExpr::Atom { predicate }
        }
        RowPredicateExpr::ScalarInt64Comparison {
            left,
            comparison_op,
            literal,
        } => RowPredicateExpr::ScalarInt64Comparison {
            left,
            comparison_op: negate_predicate_op(comparison_op),
            literal,
        },
        RowPredicateExpr::ScalarInt64ExpressionComparison {
            left,
            comparison_op,
            right,
        } => RowPredicateExpr::ScalarInt64ExpressionComparison {
            left,
            comparison_op: negate_predicate_op(comparison_op),
            right,
        },
        RowPredicateExpr::And { left, right } => RowPredicateExpr::Or {
            left: Box::new(negate_row_predicate_expr(*left)),
            right: Box::new(negate_row_predicate_expr(*right)),
        },
        RowPredicateExpr::Or { left, right } => RowPredicateExpr::And {
            left: Box::new(negate_row_predicate_expr(*left)),
            right: Box::new(negate_row_predicate_expr(*right)),
        },
    }
}

fn row_between_predicate_expr(
    column_id: String,
    low: &Expr,
    high: &Expr,
    negated: bool,
) -> Result<RowPredicateExpr, ViewPlanError> {
    let expr = RowPredicateExpr::And {
        left: Box::new(RowPredicateExpr::Atom {
            predicate: RowPredicate {
                column_id: column_id.clone(),
                op: PredicateOp::GtEq,
                literal: predicate_literal(low)?,
            },
        }),
        right: Box::new(RowPredicateExpr::Atom {
            predicate: RowPredicate {
                column_id,
                op: PredicateOp::LtEq,
                literal: predicate_literal(high)?,
            },
        }),
    };
    Ok(if negated {
        negate_row_predicate_expr(expr)
    } else {
        expr
    })
}

fn row_in_list_predicate_expr(
    column_id: String,
    column_nullable: bool,
    list: &[Expr],
    negated: bool,
) -> Result<RowPredicateExpr, ViewPlanError> {
    validate_in_list_nullability(column_nullable, list)?;
    let mut items = list.iter();
    let Some(first) = items.next() else {
        return unsupported("IN list must contain at least one literal");
    };
    let op = if negated {
        PredicateOp::NotEq
    } else {
        PredicateOp::Eq
    };
    let mut expr = RowPredicateExpr::Atom {
        predicate: RowPredicate {
            column_id: column_id.clone(),
            op,
            literal: predicate_literal(first)?,
        },
    };
    for item in items {
        let atom = RowPredicateExpr::Atom {
            predicate: RowPredicate {
                column_id: column_id.clone(),
                op,
                literal: predicate_literal(item)?,
            },
        };
        expr = if negated {
            RowPredicateExpr::And {
                left: Box::new(expr),
                right: Box::new(atom),
            }
        } else {
            RowPredicateExpr::Or {
                left: Box::new(expr),
                right: Box::new(atom),
            }
        };
    }
    Ok(expr)
}

fn validate_in_list_nullability(column_nullable: bool, list: &[Expr]) -> Result<(), ViewPlanError> {
    if column_nullable {
        return unsupported(
            "IN/NOT IN on a nullable expression requires null-aware predicate semantics",
        );
    }
    if list
        .iter()
        .any(|expr| matches!(expr, Expr::Value(value) if matches!(value.value, SqlValue::Null)))
    {
        return unsupported(
            "IN/NOT IN with a NULL list item requires null-aware predicate semantics",
        );
    }
    Ok(())
}

fn row_like_predicate_expr(
    column_id: String,
    pattern: &Expr,
    negated: bool,
) -> Result<RowPredicateExpr, ViewPlanError> {
    Ok(RowPredicateExpr::Atom {
        predicate: RowPredicate {
            column_id,
            op: if negated {
                PredicateOp::NotLike
            } else {
                PredicateOp::Like
            },
            literal: predicate_like_literal(pattern)?,
        },
    })
}

fn row_null_predicate_expr(column_id: String, is_not_null: bool) -> RowPredicateExpr {
    RowPredicateExpr::Atom {
        predicate: RowPredicate {
            column_id,
            op: if is_not_null {
                PredicateOp::IsNotNull
            } else {
                PredicateOp::IsNull
            },
            literal: JsonValue::Null,
        },
    }
}

fn negate_join_predicate_expr(expr: JoinPredicateExpr) -> JoinPredicateExpr {
    match expr {
        JoinPredicateExpr::Atom { mut predicate } => {
            predicate.predicate.op = negate_predicate_op(predicate.predicate.op);
            JoinPredicateExpr::Atom { predicate }
        }
        JoinPredicateExpr::ScalarInt64Comparison {
            relation_id,
            left,
            comparison_op,
            literal,
        } => JoinPredicateExpr::ScalarInt64Comparison {
            relation_id,
            left,
            comparison_op: negate_predicate_op(comparison_op),
            literal,
        },
        JoinPredicateExpr::ScalarInt64ExpressionComparison {
            left_relation_id,
            left,
            comparison_op,
            right_relation_id,
            right,
        } => JoinPredicateExpr::ScalarInt64ExpressionComparison {
            left_relation_id,
            left,
            comparison_op: negate_predicate_op(comparison_op),
            right_relation_id,
            right,
        },
        JoinPredicateExpr::And { left, right } => JoinPredicateExpr::Or {
            left: Box::new(negate_join_predicate_expr(*left)),
            right: Box::new(negate_join_predicate_expr(*right)),
        },
        JoinPredicateExpr::Or { left, right } => JoinPredicateExpr::And {
            left: Box::new(negate_join_predicate_expr(*left)),
            right: Box::new(negate_join_predicate_expr(*right)),
        },
    }
}

fn join_between_predicate_expr(
    relation_id: String,
    column_id: String,
    low: &Expr,
    high: &Expr,
    negated: bool,
) -> Result<JoinPredicateExpr, ViewPlanError> {
    let expr = JoinPredicateExpr::And {
        left: Box::new(JoinPredicateExpr::Atom {
            predicate: JoinRowPredicate {
                relation_id: relation_id.clone(),
                predicate: RowPredicate {
                    column_id: column_id.clone(),
                    op: PredicateOp::GtEq,
                    literal: predicate_literal(low)?,
                },
            },
        }),
        right: Box::new(JoinPredicateExpr::Atom {
            predicate: JoinRowPredicate {
                relation_id,
                predicate: RowPredicate {
                    column_id,
                    op: PredicateOp::LtEq,
                    literal: predicate_literal(high)?,
                },
            },
        }),
    };
    Ok(if negated {
        negate_join_predicate_expr(expr)
    } else {
        expr
    })
}

fn join_in_list_predicate_expr(
    relation_id: String,
    column_id: String,
    column_nullable: bool,
    list: &[Expr],
    negated: bool,
) -> Result<JoinPredicateExpr, ViewPlanError> {
    validate_in_list_nullability(column_nullable, list)?;
    let mut items = list.iter();
    let Some(first) = items.next() else {
        return unsupported("JOIN WHERE IN list must contain at least one literal");
    };
    let op = if negated {
        PredicateOp::NotEq
    } else {
        PredicateOp::Eq
    };
    let mut expr = JoinPredicateExpr::Atom {
        predicate: JoinRowPredicate {
            relation_id: relation_id.clone(),
            predicate: RowPredicate {
                column_id: column_id.clone(),
                op,
                literal: predicate_literal(first)?,
            },
        },
    };
    for item in items {
        let atom = JoinPredicateExpr::Atom {
            predicate: JoinRowPredicate {
                relation_id: relation_id.clone(),
                predicate: RowPredicate {
                    column_id: column_id.clone(),
                    op,
                    literal: predicate_literal(item)?,
                },
            },
        };
        expr = if negated {
            JoinPredicateExpr::And {
                left: Box::new(expr),
                right: Box::new(atom),
            }
        } else {
            JoinPredicateExpr::Or {
                left: Box::new(expr),
                right: Box::new(atom),
            }
        };
    }
    Ok(expr)
}

fn join_like_predicate_expr(
    relation_id: String,
    column_id: String,
    pattern: &Expr,
    negated: bool,
) -> Result<JoinPredicateExpr, ViewPlanError> {
    Ok(JoinPredicateExpr::Atom {
        predicate: JoinRowPredicate {
            relation_id,
            predicate: RowPredicate {
                column_id,
                op: if negated {
                    PredicateOp::NotLike
                } else {
                    PredicateOp::Like
                },
                literal: predicate_like_literal(pattern)?,
            },
        },
    })
}

fn join_null_predicate_expr(
    relation_id: String,
    column_id: String,
    is_not_null: bool,
) -> JoinPredicateExpr {
    JoinPredicateExpr::Atom {
        predicate: JoinRowPredicate {
            relation_id,
            predicate: RowPredicate {
                column_id,
                op: if is_not_null {
                    PredicateOp::IsNotNull
                } else {
                    PredicateOp::IsNull
                },
                literal: JsonValue::Null,
            },
        },
    }
}

fn negate_aggregate_predicate_expr(
    expr: AggregateOutputPredicateExpr,
) -> AggregateOutputPredicateExpr {
    match expr {
        AggregateOutputPredicateExpr::Atom { mut predicate } => {
            predicate.op = negate_predicate_op(predicate.op);
            AggregateOutputPredicateExpr::Atom { predicate }
        }
        AggregateOutputPredicateExpr::And { left, right } => AggregateOutputPredicateExpr::Or {
            left: Box::new(negate_aggregate_predicate_expr(*left)),
            right: Box::new(negate_aggregate_predicate_expr(*right)),
        },
        AggregateOutputPredicateExpr::Or { left, right } => AggregateOutputPredicateExpr::And {
            left: Box::new(negate_aggregate_predicate_expr(*left)),
            right: Box::new(negate_aggregate_predicate_expr(*right)),
        },
    }
}

fn aggregate_between_predicate_expr(
    output_column_id: String,
    low: &Expr,
    high: &Expr,
    negated: bool,
) -> Result<AggregateOutputPredicateExpr, ViewPlanError> {
    let expr = AggregateOutputPredicateExpr::And {
        left: Box::new(AggregateOutputPredicateExpr::Atom {
            predicate: AggregateOutputPredicate {
                output_column_id: output_column_id.clone(),
                op: PredicateOp::GtEq,
                literal: predicate_literal(low)?,
            },
        }),
        right: Box::new(AggregateOutputPredicateExpr::Atom {
            predicate: AggregateOutputPredicate {
                output_column_id,
                op: PredicateOp::LtEq,
                literal: predicate_literal(high)?,
            },
        }),
    };
    Ok(if negated {
        negate_aggregate_predicate_expr(expr)
    } else {
        expr
    })
}

fn aggregate_in_list_predicate_expr(
    output_column_id: String,
    list: &[Expr],
    negated: bool,
) -> Result<AggregateOutputPredicateExpr, ViewPlanError> {
    validate_in_list_nullability(false, list)?;
    let mut items = list.iter();
    let Some(first) = items.next() else {
        return unsupported("HAVING IN list must contain at least one literal");
    };
    let op = if negated {
        PredicateOp::NotEq
    } else {
        PredicateOp::Eq
    };
    let mut expr = AggregateOutputPredicateExpr::Atom {
        predicate: AggregateOutputPredicate {
            output_column_id: output_column_id.clone(),
            op,
            literal: predicate_literal(first)?,
        },
    };
    for item in items {
        let atom = AggregateOutputPredicateExpr::Atom {
            predicate: AggregateOutputPredicate {
                output_column_id: output_column_id.clone(),
                op,
                literal: predicate_literal(item)?,
            },
        };
        expr = if negated {
            AggregateOutputPredicateExpr::And {
                left: Box::new(expr),
                right: Box::new(atom),
            }
        } else {
            AggregateOutputPredicateExpr::Or {
                left: Box::new(expr),
                right: Box::new(atom),
            }
        };
    }
    Ok(expr)
}

fn aggregate_null_predicate_expr(
    output_column_id: String,
    is_not_null: bool,
) -> AggregateOutputPredicateExpr {
    AggregateOutputPredicateExpr::Atom {
        predicate: AggregateOutputPredicate {
            output_column_id,
            op: if is_not_null {
                PredicateOp::IsNotNull
            } else {
                PredicateOp::IsNull
            },
            literal: JsonValue::Null,
        },
    }
}

fn reverse_predicate_op(op: BinaryOperator) -> Option<BinaryOperator> {
    match op {
        BinaryOperator::Eq => Some(BinaryOperator::Eq),
        BinaryOperator::NotEq => Some(BinaryOperator::NotEq),
        BinaryOperator::Gt => Some(BinaryOperator::Lt),
        BinaryOperator::GtEq => Some(BinaryOperator::LtEq),
        BinaryOperator::Lt => Some(BinaryOperator::Gt),
        BinaryOperator::LtEq => Some(BinaryOperator::GtEq),
        _ => None,
    }
}

fn predicate_literal(expr: &Expr) -> Result<JsonValue, ViewPlanError> {
    match expr {
        Expr::Value(value) => predicate_literal_value(&value.value, false),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let Expr::Value(value) = expr.as_ref() else {
                return unsupported("WHERE comparison literal is not supported");
            };
            predicate_literal_value(&value.value, true)
        }
        _ => unsupported("WHERE comparison literal is not supported"),
    }
}

fn predicate_like_literal(expr: &Expr) -> Result<JsonValue, ViewPlanError> {
    let literal = predicate_literal(expr)?;
    if literal.is_string() {
        Ok(literal)
    } else {
        unsupported("LIKE pattern must be a string literal")
    }
}

fn predicate_column_supports_like(column: &RelationColumnV1) -> bool {
    matches!(
        column.physical_arrow_type,
        ArrowPhysicalTypeV1::Utf8 | ArrowPhysicalTypeV1::JsonUtf8
    )
}

fn predicate_literal_value(value: &SqlValue, negative: bool) -> Result<JsonValue, ViewPlanError> {
    match value {
        SqlValue::Number(value, _) => {
            let value = if negative {
                format!("-{value}")
            } else {
                value.clone()
            };
            if value.contains('.') {
                return Ok(JsonValue::String(value));
            }
            let number = JsonNumber::from(value.parse::<i64>().map_err(|_| {
                ViewPlanError::UnsupportedShape {
                    reason: "WHERE numeric literal is not supported".to_string(),
                }
            })?);
            Ok(JsonValue::Number(number))
        }
        SqlValue::SingleQuotedString(value)
        | SqlValue::DoubleQuotedString(value)
        | SqlValue::NationalStringLiteral(value) => Ok(JsonValue::String(value.clone())),
        SqlValue::Boolean(value) => Ok(JsonValue::Bool(*value)),
        SqlValue::Null => Ok(JsonValue::Null),
        _ => unsupported("WHERE comparison literal is not supported"),
    }
}

fn predicate_column_is_runtime_visible(
    column: &RelationColumnV1,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
) -> bool {
    column.column_id == key_column.column_id || column.column_id == value_column.column_id
}

fn validate_scalar_int64_comparison_predicate_expr<F>(
    expression: &Expr,
    literal_expr: &Expr,
    op: BinaryOperator,
    catalog: &VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
    column_is_runtime_visible: F,
) -> Result<RowPredicateExpr, ViewPlanError>
where
    F: Fn(&RelationColumnV1) -> bool,
{
    let Some(comparison_op) = predicate_op(op) else {
        return unsupported("scalar Int64 predicate comparison operator is not supported");
    };
    let left = supported_filter_project_bound_projection_expr(
        expression,
        catalog,
        relation_alias,
        source_projection,
    )?;
    let column_ids = supported_projection_expr_column_ids(&left);
    if source_projection.is_some() && column_ids.len() > 1 {
        validate_scalar_int64_expression_columns(&left, catalog, &column_is_runtime_visible)?;
        return Ok(RowPredicateExpr::ScalarInt64ExpressionComparison {
            left: Box::new(left),
            comparison_op,
            right: Box::new(supported_projection_literal_expr(literal_expr)?),
        });
    }
    let [column_id] = column_ids.as_slice() else {
        return unsupported(
            "scalar Int64 predicate expression must reference exactly one runtime-visible column",
        );
    };
    let Some(column) = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == *column_id)
    else {
        return unsupported(
            "scalar Int64 predicate expression must reference exactly one runtime-visible column",
        );
    };
    if !column_is_runtime_visible(column) {
        return unsupported(
            "scalar Int64 predicate expression must reference exactly one runtime-visible column",
        );
    }
    Ok(RowPredicateExpr::ScalarInt64Comparison {
        left: Box::new(left),
        comparison_op,
        literal: predicate_literal(literal_expr)?,
    })
}

fn validate_scalar_int64_expression_comparison_predicate_expr<F>(
    left_expr: &Expr,
    right_expr: &Expr,
    op: BinaryOperator,
    catalog: &VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
    column_is_runtime_visible: F,
) -> Result<RowPredicateExpr, ViewPlanError>
where
    F: Fn(&RelationColumnV1) -> bool,
{
    let Some(comparison_op) = predicate_op(op) else {
        return unsupported("scalar Int64 predicate comparison operator is not supported");
    };
    let left = supported_filter_project_bound_projection_expr(
        left_expr,
        catalog,
        relation_alias,
        source_projection,
    )?;
    let right = supported_filter_project_bound_projection_expr(
        right_expr,
        catalog,
        relation_alias,
        source_projection,
    )?;
    validate_scalar_int64_expression_columns(&left, catalog, &column_is_runtime_visible)?;
    validate_scalar_int64_expression_columns(&right, catalog, &column_is_runtime_visible)?;
    Ok(RowPredicateExpr::ScalarInt64ExpressionComparison {
        left: Box::new(left),
        comparison_op,
        right: Box::new(right),
    })
}

fn validate_scalar_int64_expression_columns<F>(
    expression: &SupportedProjectionExpr,
    catalog: &VelorixRelationCatalogV1,
    column_is_runtime_visible: &F,
) -> Result<(), ViewPlanError>
where
    F: Fn(&RelationColumnV1) -> bool,
{
    let column_ids = supported_projection_expr_column_ids(expression);
    if column_ids.is_empty() {
        return unsupported(
            "scalar Int64 predicate expression must reference at least one runtime-visible column",
        );
    }
    for column_id in column_ids {
        let Some(column) = catalog
            .relation_schema
            .columns
            .iter()
            .find(|column| column.column_id == column_id)
        else {
            return unsupported(
                "scalar Int64 predicate expression must reference at least one runtime-visible column",
            );
        };
        if !column_is_runtime_visible(column) {
            return unsupported(
                "scalar Int64 predicate expression must reference at least one runtime-visible column",
            );
        }
    }
    Ok(())
}

fn validate_aggregate_group_keys(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
) -> Result<Vec<SupportedGroupKey>, ViewPlanError> {
    let (expressions, modifiers, group_by_all) = match &select.group_by {
        GroupByExpr::All(modifiers) => (&[][..], modifiers, true),
        GroupByExpr::Expressions(expressions, modifiers) => {
            (expressions.as_slice(), modifiers, false)
        }
    };
    if !modifiers.is_empty() {
        return unsupported("GROUP BY modifiers are not supported");
    }
    if expressions.is_empty() && !group_by_all {
        return Ok(Vec::new());
    }
    let group_key_count = if group_by_all { 1 } else { expressions.len() };
    if select.projection.len() <= group_key_count {
        return unsupported("expected grouping projections followed by aggregate projections");
    }

    let mut output_ids = BTreeSet::new();
    let mut group_keys: Vec<SupportedGroupKey> = Vec::with_capacity(group_key_count);
    for (index, item) in select.projection.iter().take(group_key_count).enumerate() {
        let group_by = expressions.get(index);
        let (projection_expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
            _ => return unsupported("grouping projections must be scalar expressions"),
        };
        let bound_direct_column = expression_filter_project_column(
            projection_expr,
            catalog,
            relation_alias,
            source_projection,
        );
        let fallback_direct_column =
            expression_catalog_column(projection_expr, catalog, relation_alias);
        if bound_direct_column.is_none()
            && source_projection.is_some_and(|projection| {
                fallback_direct_column.is_some_and(|column| {
                    projection
                        .projected_column_ids
                        .contains(column.column_id.as_str())
                })
            })
        {
            return unsupported("grouping projection must reference a visible source column");
        }
        let direct_column = bound_direct_column.or(fallback_direct_column);
        let output_column_id = match (alias, direct_column) {
            (Some(alias), _) => alias.to_string(),
            (None, Some(column)) => select_item_alias_or_source_default(
                item,
                column.name.as_str(),
                relation_alias,
                source_projection,
            )?,
            (None, None) => {
                return unsupported("computed grouping projections require an explicit alias")
            }
        };
        if !output_ids.insert(output_column_id.to_ascii_lowercase()) {
            return unsupported("grouping output column ids must be unique");
        }
        let ordinal_matches = group_by.is_some_and(|group_by| matches!(
            group_by,
            Expr::Value(value)
                if matches!(&value.value, SqlValue::Number(text, _) if text == &(index + 1).to_string())
        ));
        let alias_matches = group_by.is_some_and(|group_by| {
            expression_references_unambiguous_output_alias(
                group_by,
                catalog,
                output_column_id.as_str(),
            )
        });
        let direct_binding_matches = group_by.is_some_and(|group_by| {
            direct_column.is_some_and(|projection_column| {
                expression_filter_project_column(
                    group_by,
                    catalog,
                    relation_alias,
                    source_projection,
                )
                .or_else(|| expression_catalog_column(group_by, catalog, relation_alias))
                .is_some_and(|group_column| group_column.column_id == projection_column.column_id)
            })
        });
        if !group_by_all
            && !ordinal_matches
            && !alias_matches
            && !direct_binding_matches
            && group_by != Some(projection_expr)
        {
            return unsupported(
                "GROUP BY expressions must match grouping projections by expression, alias, or ordinal",
            );
        }

        let group_key = if let Some(column) = direct_column {
            if column.column_id == catalog.relation_schema.weight_column_id {
                return unsupported("GROUP BY may not reference the changelog weight column");
            }
            SupportedGroupKey {
                output_column_id,
                input_column_id: Some(column.column_id.clone()),
                expression: None,
            }
        } else {
            if catalog
                .relation_schema
                .columns
                .iter()
                .any(|column| column_identifier_eq(column, output_column_id.as_str()))
            {
                return unsupported("computed grouping aliases must not shadow registered columns");
            }
            let expression = supported_filter_project_bound_projection_expr(
                projection_expr,
                catalog,
                relation_alias,
                source_projection,
            )?;
            let column_ids = supported_projection_expr_column_ids(&expression);
            if column_ids.is_empty()
                || column_ids
                    .iter()
                    .any(|column_id| column_id == &catalog.relation_schema.weight_column_id)
            {
                return unsupported(
                    "computed grouping expressions must reference registered non-weight columns",
                );
            }
            SupportedGroupKey {
                output_column_id,
                input_column_id: None,
                expression: Some(expression),
            }
        };
        if group_keys.iter().any(|existing| {
            existing.input_column_id == group_key.input_column_id
                && existing.expression == group_key.expression
        }) {
            return unsupported("duplicate GROUP BY expressions are not supported");
        }
        group_keys.push(group_key);
    }
    Ok(group_keys)
}

fn validate_group_by_key(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    relation_alias: Option<&str>,
    output_key_column_id: &str,
    source_projection: Option<&SourceProjection>,
) -> Result<(), ViewPlanError> {
    let (expressions, modifiers) = match &select.group_by {
        GroupByExpr::All(modifiers) if modifiers.is_empty() => return Ok(()),
        GroupByExpr::All(_) => return unsupported("GROUP BY ALL modifiers are not supported"),
        GroupByExpr::Expressions(expressions, modifiers) => (expressions, modifiers),
    };
    if !modifiers.is_empty() {
        return unsupported("GROUP BY modifiers are not supported");
    }
    let [group_key] = expressions.as_slice() else {
        return unsupported("expected exactly one GROUP BY key");
    };
    if expression_is_first_select_projection_ordinal(group_key)
        || expression_references_bound_column(
            group_key,
            catalog,
            key_column,
            relation_alias,
            source_projection,
        )
        || expression_references_unambiguous_output_alias(group_key, catalog, output_key_column_id)
    {
        Ok(())
    } else {
        unsupported("GROUP BY key must be the catalog primary key column")
    }
}

fn validate_projection<'a>(
    select: &Select,
    catalog: &'a VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
    group_keys: &[SupportedGroupKey],
) -> Result<ValidatedAggregateProjection<'a>, ViewPlanError> {
    let aggregates = select.projection.get(group_keys.len()..).ok_or_else(|| {
        ViewPlanError::UnsupportedShape {
            reason: "expected grouping projections followed by aggregate projections".to_string(),
        }
    })?;
    let output_key_column_id = group_keys
        .first()
        .map(|key| key.output_column_id.clone())
        .unwrap_or_default();

    if aggregates.is_empty() {
        return unsupported("expected at least one aggregate projection");
    }

    let mut output_ids = group_keys
        .iter()
        .map(|key| key.output_column_id.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let mut value_column: Option<&RelationColumnV1> = None;
    let mut aggregate_outputs = Vec::with_capacity(aggregates.len());
    let mut aggregate_filter_exprs = Vec::new();

    for item in aggregates {
        let aggregate =
            select_item_aggregate(item, catalog, relation_alias, true, source_projection)?;
        if !output_ids.insert(aggregate.output.output_column_id.clone()) {
            return unsupported("aggregate output column ids must be unique");
        }
        if let Some(filter_expr) = aggregate.filter_expr {
            aggregate_filter_exprs.push((aggregate.output.output_column_id.clone(), filter_expr));
        }
        if let Some(column) = aggregate.input_column {
            validate_numeric_sum_column(catalog, column)?;
            if aggregate.output.function == LogicalPlanAggregateFunctionV1::Avg {
                validate_numeric_avg_column(column)?;
            }
            if value_column.is_none() {
                value_column = Some(column);
            }
        }
        aggregate_outputs.push(aggregate.output);
    }

    let value_column = match value_column {
        Some(value_column) => value_column,
        None if aggregate_outputs.iter().all(|output| {
            matches!(
                output.function,
                LogicalPlanAggregateFunctionV1::Count
                    | LogicalPlanAggregateFunctionV1::CountDistinct
            )
        }) =>
        {
            count_only_runtime_value_column(catalog, &aggregate_outputs)?
        }
        None => {
            return unsupported(
                "single-key aggregate runtime currently requires sum(value), avg(value), or count-only aggregates",
            );
        }
    };
    let aggregate_input_column_ids = aggregate_input_column_ids(&aggregate_outputs);
    if aggregate_input_column_ids.len() > 1 {
        if !aggregate_filter_exprs.is_empty() {
            return unsupported(
                "multi-input single-key aggregate FILTER clauses are not supported",
            );
        }
        validate_raw_int64_multi_input_aggregates(catalog, &aggregate_outputs)?;
    }
    for output in &aggregate_outputs {
        if matches!(
            output.function,
            LogicalPlanAggregateFunctionV1::Count | LogicalPlanAggregateFunctionV1::CountDistinct
        ) && output
            .input_column_id
            .as_ref()
            .is_some_and(|input_column_id| {
                aggregate_input_column_ids.len() <= 1
                    && aggregate_outputs.len() != 1
                    && input_column_id != &value_column.column_id
            })
        {
            return unsupported(
                "mixed count(nullable_column) aggregates must use the same input column as value aggregates",
            );
        }
    }
    let shared_filter_expr = aggregate_filter_exprs
        .first()
        .and_then(|(_, first_filter)| {
            (aggregate_filter_exprs.len() == aggregate_outputs.len()
                && aggregate_filter_exprs
                    .iter()
                    .all(|(_, filter_expr)| filter_expr == first_filter))
            .then_some(first_filter)
        });
    let aggregate_filter_expr = shared_filter_expr
        .map(|filter_expr| {
            validate_row_predicate_expr(
                filter_expr,
                catalog,
                key_column,
                value_column,
                relation_alias,
                false,
            )
        })
        .transpose()?;
    let per_output_filter_exprs = if aggregate_filter_expr.is_none()
        && !aggregate_filter_exprs.is_empty()
    {
        if value_column.physical_arrow_type != ArrowPhysicalTypeV1::Int64 {
            return unsupported(
                "mixed aggregate FILTER currently supports Int64 value aggregate inputs",
            );
        }
        for output in &aggregate_outputs {
            match output.function {
                LogicalPlanAggregateFunctionV1::Sum
                | LogicalPlanAggregateFunctionV1::Min
                | LogicalPlanAggregateFunctionV1::Max
                | LogicalPlanAggregateFunctionV1::Avg => {}
                LogicalPlanAggregateFunctionV1::Count if output.input_column_id.is_none() => {}
                LogicalPlanAggregateFunctionV1::CountDistinct
                    if output.input_column_id.as_deref()
                        == Some(value_column.column_id.as_str()) => {}
                _ => {
                    return unsupported(
                        "mixed aggregate FILTER currently supports value aggregates, count(*), and count(distinct value) outputs",
                    );
                }
            }
        }
        aggregate_filter_exprs
            .iter()
            .map(|(output_column_id, filter_expr)| {
                Ok((
                    output_column_id.clone(),
                    validate_row_predicate_expr(
                        filter_expr,
                        catalog,
                        key_column,
                        value_column,
                        relation_alias,
                        false,
                    )?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, ViewPlanError>>()?
    } else {
        BTreeMap::new()
    };

    Ok(ValidatedAggregateProjection {
        output_key_column_id,
        window_start_output_column_id: String::new(),
        window_end_output_column_id: String::new(),
        value_column,
        aggregate_outputs,
        aggregate_filter_expr,
        aggregate_filter_exprs: per_output_filter_exprs,
    })
}

fn aggregate_input_column_ids(aggregate_outputs: &[SupportedAggregateOutput]) -> BTreeSet<String> {
    aggregate_outputs
        .iter()
        .filter_map(|output| output.input_column_id.clone())
        .collect()
}

fn validate_raw_int64_multi_input_aggregates(
    catalog: &VelorixRelationCatalogV1,
    aggregate_outputs: &[SupportedAggregateOutput],
) -> Result<(), ViewPlanError> {
    for output in aggregate_outputs {
        if output.input_expression.is_some() {
            return unsupported(
                "multi-input single-key aggregates currently support raw input columns only",
            );
        }
        match output.function {
            LogicalPlanAggregateFunctionV1::Sum
            | LogicalPlanAggregateFunctionV1::Min
            | LogicalPlanAggregateFunctionV1::Max
            | LogicalPlanAggregateFunctionV1::Avg
            | LogicalPlanAggregateFunctionV1::Count
            | LogicalPlanAggregateFunctionV1::CountDistinct
            | LogicalPlanAggregateFunctionV1::PercentileDisc { .. }
            | LogicalPlanAggregateFunctionV1::PercentileCont { .. } => {}
        }
        let Some(input_column_id) = output.input_column_id.as_deref() else {
            continue;
        };
        let column = catalog
            .relation_schema
            .columns
            .iter()
            .find(|column| column.column_id == input_column_id)
            .ok_or_else(|| ViewPlanError::UnsupportedShape {
                reason: "multi-input aggregate input column is missing from relation catalog"
                    .to_string(),
            })?;
        let nullable_count_distinct =
            column.nullable && output.function == LogicalPlanAggregateFunctionV1::CountDistinct;
        if column.column_id == catalog.relation_schema.weight_column_id
            || (column.nullable && !nullable_count_distinct)
            || column.physical_arrow_type != ArrowPhysicalTypeV1::Int64
        {
            return unsupported(
                "multi-input single-key aggregates currently support non-null Int64 inputs and nullable Int64 count(DISTINCT ...) inputs",
            );
        }
    }
    Ok(())
}

fn validate_aggregate_top_k(
    query: &Query,
    aggregate_outputs: &[SupportedAggregateOutput],
    binding_context: Option<AggregateTopKBindingContext<'_>>,
    tie_breaker_key_column_ids: &[&str],
    allow_offset: bool,
) -> Result<Option<SupportedTopKPlan>, ViewPlanError> {
    let Some(top_k_bounds) = validate_top_k_limit(query, allow_offset)? else {
        return Ok(None);
    };
    let Some(order_by) = &query.order_by else {
        return unsupported("LIMIT materialized views require ORDER BY");
    };
    if order_by.interpolate.is_some() {
        return unsupported("ORDER BY INTERPOLATE is not supported for materialized top-k views");
    }
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return unsupported("ORDER BY ALL is not supported for materialized top-k views");
    };
    let (order, tie_breaker_output_column_id) = match expressions.as_slice() {
        [order] => (order, None),
        [order, tie_breaker] => {
            validate_top_k_key_tie_breaker(tie_breaker, tie_breaker_key_column_ids)?;
            let Some(output_key) = tie_breaker_key_column_ids.first() else {
                return unsupported("materialized top-k key tie-breaker is not available");
            };
            (order, Some((*output_key).to_string()))
        }
        _ => {
            return unsupported(
                "materialized top-k views require one ORDER BY expression or metric plus key tie-breaker",
            );
        }
    };
    if order.with_fill.is_some() || order.options.nulls_first.is_some() {
        return unsupported(
            "ORDER BY NULLS/WITH FILL is not supported for materialized top-k views",
        );
    }
    let output_column_id = match &order.expr {
        Expr::Identifier(identifier) => aggregate_outputs
            .iter()
            .find(|output| {
                identifier_eq(identifier.value.as_str(), output.output_column_id.as_str())
            })
            .map(|output| output.output_column_id.clone())
            .ok_or_else(|| ViewPlanError::UnsupportedShape {
                reason: "materialized top-k ORDER BY must reference an aggregate output alias"
                    .to_string(),
            })?,
        Expr::Function(_) => {
            let Some(context) = binding_context else {
                return unsupported(
                    "materialized top-k ORDER BY aggregate function binding is not supported here",
                );
            };
            order_by_aggregate_function_output_column_id(&order.expr, aggregate_outputs, context)?
        }
        _ => {
            return unsupported(
                "materialized top-k ORDER BY must reference an aggregate output alias or function",
            );
        }
    };
    let output = aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == output_column_id)
        .expect("ORDER BY output id should come from aggregate outputs");
    if output.input_column_id.is_none()
        && output.function != LogicalPlanAggregateFunctionV1::Count
        && output.function != LogicalPlanAggregateFunctionV1::CountDistinct
    {
        return unsupported(
            "materialized top-k ORDER BY must reference a numeric aggregate output",
        );
    }
    Ok(Some(SupportedTopKPlan {
        order_output_column_id: output_column_id,
        order_input_column_id: None,
        tie_breaker_output_column_id,
        descending: order.options.asc != Some(true),
        limit: top_k_bounds.limit,
        offset: top_k_bounds.offset,
    }))
}

fn validate_top_k_key_tie_breaker(
    tie_breaker: &OrderByExpr,
    accepted_key_column_ids: &[&str],
) -> Result<(), ViewPlanError> {
    if tie_breaker.with_fill.is_some() || tie_breaker.options.nulls_first.is_some() {
        return unsupported(
            "ORDER BY NULLS/WITH FILL is not supported for materialized top-k views",
        );
    }
    if tie_breaker.options.asc == Some(false) {
        return unsupported("materialized top-k key tie-breaker must be ASC");
    }
    let Expr::Identifier(identifier) = &tie_breaker.expr else {
        return unsupported("materialized top-k key tie-breaker must reference the output key");
    };
    if !accepted_key_column_ids
        .iter()
        .any(|column_id| identifier_eq(identifier.value.as_str(), column_id))
    {
        return unsupported("materialized top-k key tie-breaker must reference the output key");
    }
    Ok(())
}

enum AggregateTopKBindingContext<'a> {
    Single {
        catalog: &'a VelorixRelationCatalogV1,
        key_column: &'a RelationColumnV1,
        value_column: &'a RelationColumnV1,
        relation_alias: Option<&'a str>,
        aggregate_filter_expr: Option<&'a RowPredicateExpr>,
        aggregate_filter_exprs: &'a BTreeMap<String, RowPredicateExpr>,
    },
    Join {
        select: &'a Select,
        left_alias: &'a str,
        left_catalog: &'a VelorixRelationCatalogV1,
        left_key: &'a RelationColumnV1,
        left_value: &'a RelationColumnV1,
        right_alias: &'a str,
        right_catalog: &'a VelorixRelationCatalogV1,
        shared_aggregate_filter_expr: Option<&'a JoinPredicateExpr>,
        aggregate_filter_exprs: &'a BTreeMap<String, JoinPredicateExpr>,
    },
}

fn order_by_aggregate_function_output_column_id(
    expr: &Expr,
    aggregate_outputs: &[SupportedAggregateOutput],
    context: AggregateTopKBindingContext<'_>,
) -> Result<String, ViewPlanError> {
    match context {
        AggregateTopKBindingContext::Single {
            catalog,
            key_column,
            value_column,
            relation_alias,
            aggregate_filter_expr,
            aggregate_filter_exprs,
        } => order_by_single_aggregate_function_output_column_id(
            expr,
            aggregate_outputs,
            catalog,
            key_column,
            value_column,
            relation_alias,
            aggregate_filter_expr,
            aggregate_filter_exprs,
        ),
        AggregateTopKBindingContext::Join {
            select,
            left_alias,
            left_catalog,
            left_key,
            left_value,
            right_alias,
            right_catalog,
            shared_aggregate_filter_expr,
            aggregate_filter_exprs,
        } => {
            let context = JoinAggregateTopKFunctionContext {
                select,
                left_alias,
                left_catalog,
                left_key,
                left_value,
                right_alias,
                right_catalog,
                shared_aggregate_filter_expr,
                aggregate_filter_exprs,
            };
            order_by_join_aggregate_function_output_column_id(expr, aggregate_outputs, context)
        }
    }
}

struct JoinAggregateTopKFunctionContext<'a> {
    select: &'a Select,
    left_alias: &'a str,
    left_catalog: &'a VelorixRelationCatalogV1,
    left_key: &'a RelationColumnV1,
    left_value: &'a RelationColumnV1,
    right_alias: &'a str,
    right_catalog: &'a VelorixRelationCatalogV1,
    shared_aggregate_filter_expr: Option<&'a JoinPredicateExpr>,
    aggregate_filter_exprs: &'a BTreeMap<String, JoinPredicateExpr>,
}

impl<'a> JoinAggregateTopKFunctionContext<'a> {
    fn count_argument(
        &self,
        arguments: &FunctionArgumentList,
    ) -> Result<Option<JoinCountArgument>, ViewPlanError> {
        validate_join_order_by_count_argument(
            arguments,
            self.left_alias,
            self.left_catalog,
            self.right_alias,
            self.right_catalog,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn order_by_single_aggregate_function_output_column_id(
    expr: &Expr,
    aggregate_outputs: &[SupportedAggregateOutput],
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
    relation_alias: Option<&str>,
    aggregate_filter_expr: Option<&RowPredicateExpr>,
    aggregate_filter_exprs: &BTreeMap<String, RowPredicateExpr>,
) -> Result<String, ViewPlanError> {
    let Expr::Function(function) = expr else {
        return unsupported("materialized top-k ORDER BY must reference an aggregate function");
    };
    let Some(function_name) = single_object_name_identifier(&function.name) else {
        return unsupported(
            "materialized top-k ORDER BY aggregate function name must be unqualified",
        );
    };
    let canonical_function = function_name.to_ascii_lowercase();
    let mut aggregate_function = match canonical_function.as_str() {
        "sum" => LogicalPlanAggregateFunctionV1::Sum,
        "count" => LogicalPlanAggregateFunctionV1::Count,
        "min" => LogicalPlanAggregateFunctionV1::Min,
        "max" => LogicalPlanAggregateFunctionV1::Max,
        "avg" => LogicalPlanAggregateFunctionV1::Avg,
        _ => return unsupported("materialized top-k ORDER BY aggregate function is not supported"),
    };
    if !matches!(function.parameters, FunctionArguments::None)
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported("materialized top-k ORDER BY aggregate modifiers are not supported");
    }
    let order_filter_expr = function
        .filter
        .as_deref()
        .map(|filter| {
            validate_row_predicate_expr(
                filter,
                catalog,
                key_column,
                value_column,
                relation_alias,
                false,
            )
        })
        .transpose()?;
    let (input_column_id, input_expression) = match aggregate_function {
        LogicalPlanAggregateFunctionV1::Count => {
            let FunctionArguments::List(arguments) = &function.args else {
                return unsupported(
                    "materialized top-k ORDER BY count arguments must be a simple argument list",
                );
            };
            if matches!(
                arguments.duplicate_treatment,
                Some(DuplicateTreatment::Distinct)
            ) {
                aggregate_function = LogicalPlanAggregateFunctionV1::CountDistinct;
            }
            (
                validate_count_argument(arguments, catalog, relation_alias, None)?
                    .map(|column| column.column_id.clone()),
                None,
            )
        }
        _ => {
            let FunctionArguments::List(arguments) = &function.args else {
                return unsupported(
                    "materialized top-k ORDER BY aggregate arguments must be a simple argument list",
                );
            };
            if !arguments.clauses.is_empty() {
                return unsupported(
                    "materialized top-k ORDER BY aggregate argument clauses are not supported",
                );
            }
            if arguments.duplicate_treatment.is_some() {
                return unsupported(
                    "materialized top-k ORDER BY DISTINCT value aggregate arguments are not supported",
                );
            }
            let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice()
            else {
                return unsupported(
                    "materialized top-k ORDER BY aggregate value functions require one column argument",
                );
            };
            if let Some(column) = expression_column(argument, catalog, relation_alias) {
                (Some(column.column_id.clone()), None)
            } else {
                let expression =
                    supported_filter_project_projection_expr(argument, catalog, relation_alias)?;
                let column_ids = supported_projection_expr_column_ids(&expression);
                let [column_id] = column_ids.as_slice() else {
                    return unsupported(
                        "materialized top-k ORDER BY aggregate input expressions must reference exactly one Int64 value column",
                    );
                };
                let column = catalog
                    .relation_schema
                    .columns
                    .iter()
                    .find(|column| column.column_id == *column_id)
                    .ok_or_else(|| ViewPlanError::UnsupportedShape {
                        reason:
                            "materialized top-k ORDER BY aggregate input expression column is missing from relation catalog"
                                .to_string(),
                    })?;
                (Some(column.column_id.clone()), Some(expression))
            }
        }
    };
    let function_matches = aggregate_outputs
        .iter()
        .filter(|output| {
            output.function == aggregate_function
                && output.input_column_id == input_column_id
                && output.input_expression == input_expression
        })
        .collect::<Vec<_>>();
    let matches = function_matches
        .iter()
        .copied()
        .filter(|output| {
            aggregate_output_filter_matches(
                order_filter_expr.as_ref(),
                &output.output_column_id,
                aggregate_filter_expr,
                aggregate_filter_exprs,
            )
        })
        .collect::<Vec<_>>();
    let [output] = matches.as_slice() else {
        if order_filter_expr.is_none()
            && function_matches.iter().any(|output| {
                aggregate_filter_expr.is_some()
                    || aggregate_filter_exprs.contains_key(&output.output_column_id)
            })
        {
            return unsupported(
                "materialized top-k ORDER BY aggregate function must reference one unfiltered projected aggregate output",
            );
        }
        return unsupported(
            "materialized top-k ORDER BY aggregate function must reference one projected aggregate output",
        );
    };
    Ok(output.output_column_id.clone())
}

fn order_by_join_aggregate_function_output_column_id(
    expr: &Expr,
    aggregate_outputs: &[SupportedAggregateOutput],
    context: JoinAggregateTopKFunctionContext<'_>,
) -> Result<String, ViewPlanError> {
    let Expr::Function(function) = expr else {
        return unsupported("materialized top-k ORDER BY must reference an aggregate function");
    };
    let Some(function_name) = single_object_name_identifier(&function.name) else {
        return unsupported(
            "materialized top-k ORDER BY aggregate function name must be unqualified",
        );
    };
    let canonical_function = function_name.to_ascii_lowercase();
    let value_aggregate_function = match canonical_function.as_str() {
        "sum" => Some(LogicalPlanAggregateFunctionV1::Sum),
        "avg" => Some(LogicalPlanAggregateFunctionV1::Avg),
        "min" => Some(LogicalPlanAggregateFunctionV1::Min),
        "max" => Some(LogicalPlanAggregateFunctionV1::Max),
        "count" => None,
        _ => return unsupported("materialized top-k ORDER BY aggregate function is not supported"),
    };
    if !matches!(function.parameters, FunctionArguments::None)
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported("materialized top-k ORDER BY aggregate modifiers are not supported");
    }
    let filter_context = JoinSelectionContext {
        select: context.select,
        left_alias: context.left_alias,
        left_catalog: context.left_catalog,
        left_key: context.left_key,
        left_value: context.left_value,
        right_alias: context.right_alias,
        right_catalog: context.right_catalog,
    };
    let order_filter_expr = function
        .filter
        .as_deref()
        .map(|filter| validate_join_predicate_expr(filter, &filter_context))
        .transpose()?;
    let FunctionArguments::List(arguments) = &function.args else {
        return unsupported(
            "materialized top-k ORDER BY aggregate arguments must be a simple argument list",
        );
    };
    let is_count = canonical_function == "count";
    let (aggregate_function, input_column_id, input_relation_side, input_expression) = if is_count {
        let is_distinct = matches!(
            arguments.duplicate_treatment,
            Some(DuplicateTreatment::Distinct)
        );
        let argument = context.count_argument(arguments)?;
        if is_distinct && argument.is_none() {
            return unsupported(
                "materialized top-k ORDER BY count(DISTINCT ...) must reference one column",
            );
        }
        let input_relation_side = argument.as_ref().map(|argument| argument.relation_side);
        let input_column_id = argument.map(|argument| argument.column_id);
        (
            if is_distinct {
                LogicalPlanAggregateFunctionV1::CountDistinct
            } else {
                LogicalPlanAggregateFunctionV1::Count
            },
            input_column_id,
            input_relation_side,
            None,
        )
    } else {
        let aggregate_function =
            value_aggregate_function.expect("validated non-count aggregate function");
        if !arguments.clauses.is_empty() {
            return unsupported(
                "materialized top-k ORDER BY aggregate argument clauses are not supported",
            );
        }
        if arguments.duplicate_treatment.is_some() {
            return unsupported(
                "materialized top-k ORDER BY DISTINCT value aggregate arguments are not supported",
            );
        }
        let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice()
        else {
            return unsupported(
                "materialized top-k ORDER BY aggregate value functions require one column argument",
            );
        };
        let (input_side, _catalog, column, input_expression) =
            validate_join_value_aggregate_argument(
                argument,
                context.left_alias,
                context.left_catalog,
                context.right_alias,
                context.right_catalog,
            )?;
        (
            aggregate_function,
            Some(column.column_id.clone()),
            Some(input_side),
            input_expression,
        )
    };
    let function_matches = aggregate_outputs
        .iter()
        .filter(|output| {
            output.function == aggregate_function
                && output.input_column_id == input_column_id
                && output.input_relation_side == input_relation_side
                && output.input_expression == input_expression
        })
        .collect::<Vec<_>>();
    let matches = function_matches
        .iter()
        .copied()
        .filter(|output| {
            aggregate_output_filter_matches(
                order_filter_expr.as_ref(),
                &output.output_column_id,
                context.shared_aggregate_filter_expr,
                context.aggregate_filter_exprs,
            )
        })
        .collect::<Vec<_>>();
    let [output] = matches.as_slice() else {
        if order_filter_expr.is_none()
            && function_matches.iter().any(|output| {
                context.shared_aggregate_filter_expr.is_some()
                    || context
                        .aggregate_filter_exprs
                        .contains_key(&output.output_column_id)
            })
        {
            return unsupported(
                "materialized top-k ORDER BY aggregate function must reference one unfiltered projected aggregate output",
            );
        }
        return unsupported(
            "materialized top-k ORDER BY aggregate function must reference one projected aggregate output",
        );
    };
    Ok(output.output_column_id.clone())
}

fn aggregate_output_filter_matches<T: PartialEq>(
    order_filter_expr: Option<&T>,
    output_column_id: &str,
    shared_filter_expr: Option<&T>,
    aggregate_filter_exprs: &BTreeMap<String, T>,
) -> bool {
    let projected_filter_expr =
        shared_filter_expr.or_else(|| aggregate_filter_exprs.get(output_column_id));
    projected_filter_expr == order_filter_expr
}

fn validate_latest_top_k(
    query: &Query,
    catalog: &VelorixRelationCatalogV1,
    latest: &ValidatedLatestByKeyProjection<'_>,
    relation_alias: Option<&str>,
) -> Result<Option<SupportedTopKPlan>, ViewPlanError> {
    let Some(top_k_bounds) = validate_top_k_limit(query, true)? else {
        return Ok(None);
    };
    let Some(order_by) = &query.order_by else {
        return unsupported("LIMIT materialized views require ORDER BY");
    };
    if order_by.interpolate.is_some() {
        return unsupported("ORDER BY INTERPOLATE is not supported for materialized top-k views");
    }
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return unsupported("ORDER BY ALL is not supported for materialized top-k views");
    };
    let [order] = expressions.as_slice() else {
        return unsupported("materialized top-k views require exactly one ORDER BY expression");
    };
    if order.with_fill.is_some() || order.options.nulls_first.is_some() {
        return unsupported(
            "ORDER BY NULLS/WITH FILL is not supported for materialized top-k views",
        );
    }
    let order_output_column_id = match &order.expr {
        Expr::Identifier(identifier)
            if identifier_eq(
                identifier.value.as_str(),
                latest.output_key_column_id.as_str(),
            ) =>
        {
            latest.output_key_column_id.clone()
        }
        Expr::Identifier(identifier)
            if identifier_eq(
                identifier.value.as_str(),
                latest.output_value_column_id.as_str(),
            ) =>
        {
            latest.output_value_column_id.clone()
        }
        Expr::Function(_) => {
            order_by_latest_function_output_column_id(&order.expr, catalog, latest, relation_alias)?
        }
        _ => {
            return unsupported(
                "latest-by-key top-k ORDER BY must reference an output column alias or matching arg_max/arg_min function",
            );
        }
    };
    Ok(Some(SupportedTopKPlan {
        order_output_column_id,
        order_input_column_id: None,
        tie_breaker_output_column_id: None,
        descending: order.options.asc != Some(true),
        limit: top_k_bounds.limit,
        offset: top_k_bounds.offset,
    }))
}

fn order_by_latest_function_output_column_id(
    expr: &Expr,
    catalog: &VelorixRelationCatalogV1,
    latest: &ValidatedLatestByKeyProjection<'_>,
    relation_alias: Option<&str>,
) -> Result<String, ViewPlanError> {
    let Expr::Function(function) = expr else {
        return unsupported(
            "latest-by-key top-k ORDER BY must reference an arg_max/arg_min function",
        );
    };
    let Some(function_name) = single_object_name_identifier(&function.name) else {
        return unsupported(
            "latest-by-key top-k ORDER BY arg_max/arg_min function name must be unqualified",
        );
    };
    let order_function = if identifier_eq(&function_name, "arg_max") {
        LogicalPlanLatestByKeyFunctionV1::ArgMax
    } else if identifier_eq(&function_name, "arg_min") {
        LogicalPlanLatestByKeyFunctionV1::ArgMin
    } else {
        return unsupported("latest-by-key top-k ORDER BY function must be arg_max or arg_min");
    };
    if order_function != latest.function {
        return unsupported(
            "latest-by-key top-k ORDER BY function must match the projected latest function",
        );
    }
    if !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported(
            "latest-by-key top-k ORDER BY arg_max/arg_min modifiers are not supported",
        );
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return unsupported(
            "latest-by-key top-k ORDER BY arg_max/arg_min arguments must be a simple list",
        );
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return unsupported(
            "latest-by-key top-k ORDER BY DISTINCT arg_max/arg_min arguments and clauses are not supported",
        );
    }
    if latest.aggregate_filter_expr.is_some() {
        return unsupported(
            "latest-by-key top-k ORDER BY arg_max/arg_min function must reference an unfiltered projected latest output",
        );
    }
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(value)), FunctionArg::Unnamed(FunctionArgExpr::Expr(ordering))] =
        arguments.args.as_slice()
    else {
        return unsupported(
            "latest-by-key top-k ORDER BY arg_max/arg_min requires value and ordering columns",
        );
    };
    let Some(value_column) = expression_column(value, catalog, relation_alias) else {
        return unsupported(
            "latest-by-key top-k ORDER BY arg_max/arg_min value must reference the projected latest value column",
        );
    };
    if value_column.column_id != latest.value_column.column_id {
        return unsupported(
            "latest-by-key top-k ORDER BY arg_max/arg_min value must match the projected latest value column",
        );
    }
    let Some(ordering_column) = expression_column(ordering, catalog, relation_alias) else {
        return unsupported(
            "latest-by-key top-k ORDER BY arg_max/arg_min ordering must reference the projected latest ordering column",
        );
    };
    if ordering_column.column_id != latest.ordering_column.column_id {
        return unsupported(
            "latest-by-key top-k ORDER BY arg_max/arg_min ordering must match the projected latest ordering column",
        );
    }
    Ok(latest.output_value_column_id.clone())
}

struct TopKBounds {
    limit: usize,
    offset: usize,
}

fn validate_top_k_limit(
    query: &Query,
    allow_offset: bool,
) -> Result<Option<TopKBounds>, ViewPlanError> {
    match (&query.limit_clause, &query.fetch) {
        (None, None) => Ok(None),
        (Some(limit_clause), None) => Ok(Some(validate_literal_limit(limit_clause, allow_offset)?)),
        (None, Some(fetch)) => Ok(Some(TopKBounds {
            limit: validate_literal_fetch(fetch)?,
            offset: 0,
        })),
        (Some(_), Some(_)) => {
            unsupported("materialized top-k views support LIMIT or FETCH FIRST, not both")
        }
    }
}

fn validate_literal_limit(
    limit_clause: &LimitClause,
    allow_offset: bool,
) -> Result<TopKBounds, ViewPlanError> {
    let LimitClause::LimitOffset {
        limit: Some(limit),
        offset,
        limit_by,
    } = limit_clause
    else {
        return unsupported("materialized top-k views support LIMIT <positive integer> only");
    };
    if !limit_by.is_empty() {
        return unsupported("LIMIT BY is not supported for materialized top-k views");
    }
    if offset.is_some() && !allow_offset {
        return unsupported("materialized top-k views support LIMIT <positive integer> only");
    }
    let limit = validate_positive_integer_literal(limit, "LIMIT")?;
    let offset = offset
        .as_ref()
        .map(|offset| validate_non_negative_integer_literal(&offset.value, "OFFSET"))
        .transpose()?
        .unwrap_or(0);
    Ok(TopKBounds { limit, offset })
}

fn validate_literal_fetch(fetch: &Fetch) -> Result<usize, ViewPlanError> {
    if fetch.with_ties {
        return unsupported("FETCH FIRST WITH TIES is not supported for materialized top-k views");
    }
    if fetch.percent {
        return unsupported("FETCH FIRST PERCENT is not supported for materialized top-k views");
    }
    let Some(quantity) = &fetch.quantity else {
        return unsupported(
            "materialized top-k FETCH FIRST must include a positive integer literal",
        );
    };
    validate_positive_integer_literal(quantity, "FETCH FIRST")
}

fn validate_positive_integer_literal(expr: &Expr, clause: &str) -> Result<usize, ViewPlanError> {
    let Expr::Value(value) = expr else {
        return unsupported(format!(
            "materialized top-k {clause} must be a positive integer literal"
        ));
    };
    let SqlValue::Number(text, false) = &value.value else {
        return unsupported(format!(
            "materialized top-k {clause} must be a positive integer literal"
        ));
    };
    let limit = text
        .parse::<usize>()
        .map_err(|_| ViewPlanError::UnsupportedShape {
            reason: format!("materialized top-k {clause} must be a positive integer literal"),
        })?;
    if limit == 0 {
        return unsupported(format!(
            "materialized top-k {clause} must be greater than zero"
        ));
    }
    Ok(limit)
}

fn validate_non_negative_integer_literal(
    expr: &Expr,
    clause: &str,
) -> Result<usize, ViewPlanError> {
    let Expr::Value(value) = expr else {
        return unsupported(format!(
            "materialized top-k {clause} must be a non-negative integer literal"
        ));
    };
    let SqlValue::Number(text, false) = &value.value else {
        return unsupported(format!(
            "materialized top-k {clause} must be a non-negative integer literal"
        ));
    };
    text.parse::<usize>()
        .map_err(|_| ViewPlanError::UnsupportedShape {
            reason: format!("materialized top-k {clause} must be a non-negative integer literal"),
        })
}

fn validate_filter_project_top_k(
    query: &Query,
    projection: &ValidatedFilterProjectProjection,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    relation_alias: Option<&str>,
) -> Result<Option<SupportedTopKPlan>, ViewPlanError> {
    let Some(top_k_bounds) = validate_top_k_limit(query, true)? else {
        return Ok(None);
    };
    let Some(order_by) = &query.order_by else {
        return unsupported("LIMIT materialized views require ORDER BY");
    };
    if order_by.interpolate.is_some() {
        return unsupported("ORDER BY INTERPOLATE is not supported for materialized top-k views");
    }
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return unsupported("ORDER BY ALL is not supported for materialized top-k views");
    };
    let (order, tie_breaker_output_column_id) = match expressions.as_slice() {
        [order] => (order, None),
        [order, tie_breaker] => {
            validate_top_k_key_tie_breaker(tie_breaker, &[&projection.output_key_column_id])?;
            (order, Some(projection.output_key_column_id.clone()))
        }
        _ => {
            return unsupported(
                "materialized top-k views require one ORDER BY expression or metric plus key tie-breaker",
            );
        }
    };
    if order.with_fill.is_some() || order.options.nulls_first.is_some() {
        return unsupported(
            "ORDER BY NULLS/WITH FILL is not supported for materialized top-k views",
        );
    }
    let (output_column_id, order_input_column_id) = if let Expr::Identifier(identifier) =
        &order.expr
    {
        if identifier_eq(
            identifier.value.as_str(),
            projection.output_key_column_id.as_str(),
        ) {
            (projection.output_key_column_id.clone(), None)
        } else {
            let projected_output_column_id = projection
                .value_columns
                .iter()
                .find(|column| {
                    identifier_eq(identifier.value.as_str(), column.output_column_id.as_str())
                })
                .map(|column| column.output_column_id.clone());
            if let Some(output_column_id) = projected_output_column_id {
                (output_column_id, None)
            } else if let Some(column) = expression_column(&order.expr, catalog, relation_alias) {
                if column.column_id == key_column.column_id {
                    (projection.output_key_column_id.clone(), None)
                } else {
                    validate_filter_project_hidden_order_column(catalog, column)?;
                    (column.column_id.clone(), Some(column.column_id.clone()))
                }
            } else {
                return unsupported(
                    "filter/project top-k ORDER BY must reference a projected output alias or registered input column",
                );
            }
        }
    } else if let Some(column) = expression_column(&order.expr, catalog, relation_alias) {
        if column.column_id == key_column.column_id {
            (projection.output_key_column_id.clone(), None)
        } else if let Some(projected) = projection.value_columns.iter().find(|projected| {
            projected.expression.is_none() && projected.input_column_id == column.column_id
        }) {
            (projected.output_column_id.clone(), None)
        } else {
            validate_filter_project_hidden_order_column(catalog, column)?;
            (column.column_id.clone(), Some(column.column_id.clone()))
        }
    } else {
        let order_expression =
            supported_filter_project_projection_expr(&order.expr, catalog, relation_alias)?;
        let mut matches = projection.value_columns.iter().filter(|column| {
            column
                .expression
                .as_ref()
                .is_some_and(|expression| expression == &order_expression)
        });
        let Some(column) = matches.next() else {
            return unsupported(
                "filter/project top-k ORDER BY computed expression must exactly match one projected computed expression",
            );
        };
        if matches.next().is_some() {
            return unsupported(
                "filter/project top-k ORDER BY computed expression must exactly match one projected computed expression",
            );
        }
        (column.output_column_id.clone(), None)
    };
    Ok(Some(SupportedTopKPlan {
        order_output_column_id: output_column_id,
        order_input_column_id,
        tie_breaker_output_column_id,
        descending: order.options.asc != Some(true),
        limit: top_k_bounds.limit,
        offset: top_k_bounds.offset,
    }))
}

fn validate_filter_project_hidden_order_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    validate_filter_project_value_column(catalog, column)?;
    if column.nullable {
        return unsupported("filter/project top-k hidden ORDER BY column must be non-null");
    }
    Ok(())
}

fn validate_tumbling_projection<'a>(
    select: &Select,
    catalog: &'a VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    event_time_column: &RelationColumnV1,
    relation_alias: Option<&str>,
) -> Result<ValidatedAggregateProjection<'a>, ViewPlanError> {
    let [key, window_start, window_end, aggregates @ ..] = select.projection.as_slice() else {
        return unsupported("expected projection: key, window_start, window_end, aggregate...");
    };
    if !select_item_references_column(key, key_column, relation_alias) {
        return unsupported("first tumbling projection must be the primary key column");
    }
    let output_key_column_id = select_item_alias_or_default(key, key_column.name.as_str())?;
    let Some(window_start_output_column_id) =
        select_item_identifier_alias_or_default(window_start, "window_start")
    else {
        return unsupported("tumbling projection must include window_start");
    };
    let Some(window_end_output_column_id) =
        select_item_identifier_alias_or_default(window_end, "window_end")
    else {
        return unsupported("tumbling projection must include window_end");
    };
    if aggregates.is_empty() {
        return unsupported("expected at least one tumbling aggregate projection");
    }

    let mut output_ids = BTreeSet::new();
    let mut value_column: Option<&RelationColumnV1> = None;
    let mut aggregate_outputs = Vec::with_capacity(aggregates.len());
    let mut aggregate_filter_exprs = Vec::new();

    for item in aggregates {
        let aggregate = select_item_aggregate(item, catalog, relation_alias, true, None)?;
        if !output_ids.insert(aggregate.output.output_column_id.clone()) {
            return unsupported("aggregate output column ids must be unique");
        }
        if let Some(filter_expr) = aggregate.filter_expr {
            aggregate_filter_exprs.push((aggregate.output.output_column_id.clone(), filter_expr));
        }
        if let Some(column) = aggregate.count_input_column {
            validate_numeric_sum_column(catalog, column)?;
            if let Some(existing) = value_column {
                if existing.column_id != column.column_id {
                    return unsupported(
                        "tumbling aggregate runtime currently requires count(value) to use the same input column as value aggregates",
                    );
                }
            } else {
                value_column = Some(column);
            }
        }
        if let Some(column) = aggregate.input_column {
            validate_numeric_sum_column(catalog, column)?;
            if aggregate.output.function == LogicalPlanAggregateFunctionV1::Avg {
                validate_numeric_avg_column(column)?;
            }
            if let Some(existing) = value_column {
                if existing.column_id != column.column_id {
                    return unsupported(
                        "tumbling aggregate runtime currently requires all value aggregates to use the same input column",
                    );
                }
            } else {
                value_column = Some(column);
            }
        }
        aggregate_outputs.push(aggregate.output);
    }

    let value_column = match value_column {
        Some(value_column) => value_column,
        None if aggregate_outputs.iter().all(|output| {
            matches!(
                output.function,
                LogicalPlanAggregateFunctionV1::Count
                    | LogicalPlanAggregateFunctionV1::CountDistinct
            )
        }) =>
        {
            count_only_runtime_value_column(catalog, &aggregate_outputs)?
        }
        None => {
            return unsupported(
                "tumbling aggregate runtime currently requires sum(value), avg(value), or count-only aggregates",
            );
        }
    };
    let shared_filter_expr = aggregate_filter_exprs
        .first()
        .and_then(|(_, first_filter)| {
            (aggregate_filter_exprs.len() == aggregate_outputs.len()
                && aggregate_filter_exprs
                    .iter()
                    .all(|(_, filter_expr)| filter_expr == first_filter))
            .then_some(first_filter)
        });
    let aggregate_filter_expr = shared_filter_expr
        .map(|filter_expr| {
            validate_tumbling_predicate_expr(
                filter_expr,
                catalog,
                key_column,
                value_column,
                event_time_column,
                relation_alias,
            )
        })
        .transpose()?;
    let per_output_filter_exprs = if aggregate_filter_expr.is_none()
        && !aggregate_filter_exprs.is_empty()
    {
        if value_column.physical_arrow_type != ArrowPhysicalTypeV1::Int64 {
            return unsupported(
                "mixed window aggregate FILTER currently supports Int64 sum(value) inputs",
            );
        }
        for output in &aggregate_outputs {
            match output.function {
                LogicalPlanAggregateFunctionV1::Sum => {}
                LogicalPlanAggregateFunctionV1::Count
                    if output.input_column_id.is_none()
                        || output.input_column_id.as_deref()
                            == Some(value_column.column_id.as_str()) => {}
                LogicalPlanAggregateFunctionV1::CountDistinct
                    if output.input_column_id.as_deref()
                        == Some(value_column.column_id.as_str()) => {}
                _ => {
                    return unsupported(
                        "mixed window aggregate FILTER currently supports sum(value), count(*), count(value), and count(distinct value) outputs",
                    );
                }
            }
        }
        aggregate_filter_exprs
            .iter()
            .map(|(output_column_id, filter_expr)| {
                Ok((
                    output_column_id.clone(),
                    validate_tumbling_predicate_expr(
                        filter_expr,
                        catalog,
                        key_column,
                        value_column,
                        event_time_column,
                        relation_alias,
                    )?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, ViewPlanError>>()?
    } else {
        BTreeMap::new()
    };

    Ok(ValidatedAggregateProjection {
        output_key_column_id,
        window_start_output_column_id,
        window_end_output_column_id,
        value_column,
        aggregate_outputs,
        aggregate_filter_expr,
        aggregate_filter_exprs: per_output_filter_exprs,
    })
}

struct ValidatedAggregateProjection<'a> {
    output_key_column_id: String,
    window_start_output_column_id: String,
    window_end_output_column_id: String,
    value_column: &'a RelationColumnV1,
    aggregate_outputs: Vec<SupportedAggregateOutput>,
    aggregate_filter_expr: Option<RowPredicateExpr>,
    aggregate_filter_exprs: BTreeMap<String, RowPredicateExpr>,
}

struct ValidatedFilterProjectProjection {
    output_key_column_id: String,
    output_key_input_column_id: Option<String>,
    value_columns: Vec<ValidatedProjectionColumn>,
    typed_value_columns: Vec<TypedProjectionColumn>,
}

struct ValidatedProjectionColumn {
    input_column_id: String,
    output_column_id: String,
    expression: Option<SupportedProjectionExpr>,
}

struct ParsedAggregateProjection<'a> {
    output: SupportedAggregateOutput,
    input_column: Option<&'a RelationColumnV1>,
    count_input_column: Option<&'a RelationColumnV1>,
    filter_expr: Option<Expr>,
}

fn count_only_runtime_value_column<'a>(
    catalog: &'a VelorixRelationCatalogV1,
    aggregate_outputs: &[SupportedAggregateOutput],
) -> Result<&'a RelationColumnV1, ViewPlanError> {
    if let [output] = aggregate_outputs {
        if let Some(input_column_id) = &output.input_column_id {
            return catalog
                .relation_schema
                .columns
                .iter()
                .find(|column| column.column_id == *input_column_id)
                .ok_or_else(|| ViewPlanError::UnsupportedShape {
                    reason: "count input column is missing from relation catalog".to_string(),
                });
        }
    }
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| {
            column.column_id != catalog.relation_schema.weight_column_id
                && !column.nullable
                && matches!(
                    column.physical_arrow_type,
                    ArrowPhysicalTypeV1::Int64 | ArrowPhysicalTypeV1::Decimal128 { .. }
                )
        })
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "count-only aggregate runtime requires one non-null Int64 or Decimal128 non-weight column".to_string(),
        })
}

struct ValidatedLatestByKeyProjection<'a> {
    output_key_column_id: String,
    value_column: &'a RelationColumnV1,
    ordering_column: &'a RelationColumnV1,
    output_value_column_id: String,
    function: LogicalPlanLatestByKeyFunctionV1,
    aggregate_filter_expr: Option<RowPredicateExpr>,
}

struct ValidatedRowNumberProjection<'a> {
    output_key_column_id: String,
    function: SupportedAnalyticWindowFunction,
    partition_column: &'a RelationColumnV1,
    order_column: &'a RelationColumnV1,
    order_descending: bool,
    output_row_number_column_id: String,
    implicit_primary_key_tie_breaker: bool,
}

fn validate_row_number_projection<'a>(
    select: &Select,
    catalog: &'a VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    relation_alias: Option<&str>,
) -> Result<ValidatedRowNumberProjection<'a>, ViewPlanError> {
    let [key, row_number] = select.projection.as_slice() else {
        return unsupported("expected projection: key, row_number() OVER (...)");
    };
    if !select_item_references_column(key, key_column, relation_alias) {
        return unsupported("first ROW_NUMBER projection must be the primary key column");
    }
    let output_key_column_id = select_item_alias_or_default(key, key_column.name.as_str())?;
    let SelectItem::ExprWithAlias { expr, alias } = row_number else {
        return unsupported("ROW_NUMBER projection requires an alias");
    };
    if identifier_eq(output_key_column_id.as_str(), alias.value.as_str()) {
        return unsupported("ROW_NUMBER output columns must be unique");
    }
    let Expr::Function(function) = expr else {
        return unsupported("ROW_NUMBER projection must use row_number()");
    };
    let function_kind = analytic_window_function(&function.name)?;
    if !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
        || function.uses_odbc_syntax
    {
        return unsupported(
            "analytic window projection must use rank() or row_number() OVER (...)",
        );
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return unsupported("ROW_NUMBER arguments must be an empty argument list");
    };
    if arguments.duplicate_treatment.is_some()
        || !arguments.args.is_empty()
        || !arguments.clauses.is_empty()
    {
        return unsupported("ROW_NUMBER arguments must be empty");
    }
    let Some(WindowType::WindowSpec(window)) = function.over.as_ref() else {
        return unsupported("ROW_NUMBER requires an inline OVER window specification");
    };
    if window.window_name.is_some() || window.window_frame.is_some() {
        return unsupported("ROW_NUMBER named windows and frames are not supported");
    }
    let [partition_expr] = window.partition_by.as_slice() else {
        return unsupported("ROW_NUMBER requires exactly one PARTITION BY column");
    };
    let Some(partition_column) = expression_catalog_column(partition_expr, catalog, relation_alias)
    else {
        return unsupported("ROW_NUMBER PARTITION BY must reference a registered relation column");
    };
    validate_row_number_partition_column(catalog, partition_column)?;
    let (order, implicit_primary_key_tie_breaker) = match window.order_by.as_slice() {
        [order] => (order, true),
        [order, tie_breaker] => {
            if function_kind != SupportedAnalyticWindowFunction::RowNumber {
                return unsupported("RANK and DENSE_RANK require exactly one ORDER BY column");
            }
            if tie_breaker.with_fill.is_some() || tie_breaker.options.nulls_first.is_some() {
                return unsupported("ROW_NUMBER tie-breaker NULLS/WITH FILL is not supported");
            }
            if tie_breaker.options.asc == Some(false) {
                return unsupported("ROW_NUMBER primary key tie-breaker must be ASC");
            }
            if !expression_references_column(&tie_breaker.expr, key_column, relation_alias) {
                return unsupported(
                    "ROW_NUMBER ORDER BY must include the primary key ASC tie-breaker",
                );
            }
            (order, false)
        }
        _ => return unsupported("ROW_NUMBER requires ORDER BY sortable column, primary key ASC"),
    };
    let Some(order_column) = expression_catalog_column(&order.expr, catalog, relation_alias) else {
        return unsupported("ROW_NUMBER ORDER BY must reference a registered relation column");
    };
    if order_column.nullable {
        return unsupported("ROW_NUMBER ORDER BY column must be non-nullable");
    }
    validate_latest_ordering_column(catalog, order_column)?;
    if order.with_fill.is_some() || order.options.nulls_first.is_some() {
        return unsupported("ROW_NUMBER ORDER BY NULLS/WITH FILL is not supported");
    }
    Ok(ValidatedRowNumberProjection {
        output_key_column_id,
        function: function_kind,
        partition_column,
        order_column,
        order_descending: order.options.asc == Some(false),
        output_row_number_column_id: alias.value.clone(),
        implicit_primary_key_tie_breaker,
    })
}

fn analytic_window_function(
    name: &ObjectName,
) -> Result<SupportedAnalyticWindowFunction, ViewPlanError> {
    if function_name_eq(name, "row_number") {
        Ok(SupportedAnalyticWindowFunction::RowNumber)
    } else if function_name_eq(name, "rank") {
        Ok(SupportedAnalyticWindowFunction::Rank)
    } else if function_name_eq(name, "dense_rank") {
        Ok(SupportedAnalyticWindowFunction::DenseRank)
    } else {
        unsupported("analytic window projection must use dense_rank(), rank(), or row_number()")
    }
}

fn validate_latest_by_key_projection<'a>(
    select: &Select,
    catalog: &'a VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    relation_alias: Option<&str>,
) -> Result<ValidatedLatestByKeyProjection<'a>, ViewPlanError> {
    let [key, latest_value] = select.projection.as_slice() else {
        return unsupported(
            "expected projection: key, arg_max(value, ordering) or arg_min(value, ordering)",
        );
    };
    if !select_item_references_column(key, key_column, relation_alias) {
        return unsupported("first projection must be the primary key column");
    }
    let output_key_column_id = select_item_alias_or_default(key, key_column.name.as_str())?;
    let (expr, alias) = match latest_value {
        SelectItem::UnnamedExpr(expr) => (expr, None),
        SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
        _ => return unsupported("latest-by-key projection must be an expression"),
    };
    let Expr::Function(function) = expr else {
        return unsupported("latest-by-key projection must use arg_max(value, ordering) or arg_min(value, ordering)");
    };
    let latest_function = latest_by_key_function(&function.name)?;
    if !matches!(function.parameters, FunctionArguments::None)
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported("latest-by-key projection must use arg_max(value, ordering) or arg_min(value, ordering)");
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return unsupported("latest-by-key arguments must be a simple argument list");
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return unsupported(
            "DISTINCT latest-by-key arguments and aggregate clauses are not supported",
        );
    }
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(value)), FunctionArg::Unnamed(FunctionArgExpr::Expr(ordering))] =
        arguments.args.as_slice()
    else {
        return unsupported("latest-by-key aggregate requires value and ordering column arguments");
    };
    let Some(value_column) = expression_column(value, catalog, relation_alias) else {
        return unsupported("latest-by-key value must reference a registered relation column");
    };
    let Some(ordering_column) = expression_column(ordering, catalog, relation_alias) else {
        return unsupported("latest-by-key ordering must reference a registered relation column");
    };
    validate_latest_value_column(catalog, value_column)?;
    validate_latest_ordering_column(catalog, ordering_column)?;
    let aggregate_filter_expr = function
        .filter
        .as_deref()
        .map(|filter| {
            validate_latest_predicate_expr(
                filter,
                catalog,
                key_column,
                value_column,
                ordering_column,
                relation_alias,
            )
        })
        .transpose()?;
    let output_value_column_id = alias.unwrap_or(value_column.name.as_str()).to_string();
    Ok(ValidatedLatestByKeyProjection {
        output_key_column_id,
        value_column,
        ordering_column,
        output_value_column_id,
        function: latest_function,
        aggregate_filter_expr,
    })
}

fn latest_by_key_function(
    name: &ObjectName,
) -> Result<LogicalPlanLatestByKeyFunctionV1, ViewPlanError> {
    if function_name_eq(name, "arg_max") {
        Ok(LogicalPlanLatestByKeyFunctionV1::ArgMax)
    } else if function_name_eq(name, "arg_min") {
        Ok(LogicalPlanLatestByKeyFunctionV1::ArgMin)
    } else {
        unsupported(
            "latest-by-key projection must use arg_max(value, ordering) or arg_min(value, ordering)",
        )
    }
}

fn validate_filter_project_projection(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
) -> Result<ValidatedFilterProjectProjection, ViewPlanError> {
    let expand_wildcard = match select.projection.as_slice() {
        [SelectItem::Wildcard(_)] => true,
        [SelectItem::QualifiedWildcard(
            SelectItemQualifiedWildcardKind::ObjectName(qualifier),
            _,
        )] => {
            if !filter_project_wildcard_qualifier_matches(qualifier, catalog, relation_alias) {
                return unsupported(
                    "qualified filter/project wildcard must reference the input relation",
                );
            }
            true
        }
        [SelectItem::QualifiedWildcard(_, _)] => {
            return unsupported(
                "qualified filter/project wildcard must reference the input relation",
            )
        }
        _ => false,
    };
    if expand_wildcard {
        let mut value_columns = Vec::new();
        for column in &catalog.relation_schema.columns {
            if column.column_id == key_column.column_id
                || column.column_id == catalog.relation_schema.weight_column_id
            {
                continue;
            }
            validate_filter_project_value_column(catalog, column)?;
            value_columns.push(ValidatedProjectionColumn {
                input_column_id: column.column_id.clone(),
                output_column_id: column.name.clone(),
                expression: None,
            });
        }
        if value_columns.is_empty() {
            return unsupported(
                "filter/project materialized views require at least one value column",
            );
        }
        return Ok(ValidatedFilterProjectProjection {
            output_key_column_id: key_column.name.clone(),
            output_key_input_column_id: None,
            value_columns,
            typed_value_columns: Vec::new(),
        });
    }
    let [key, values @ ..] = select.projection.as_slice() else {
        return unsupported("expected projection: key, value...");
    };
    let plain_distinct = matches!(select.distinct, Some(Distinct::Distinct));
    let (output_key_column_id, output_key_input_column_id) = if select_item_references_bound_column(
        key,
        catalog,
        key_column,
        relation_alias,
        source_projection,
    ) {
        (
            select_item_alias_or_source_default(
                key,
                key_column.name.as_str(),
                relation_alias,
                source_projection,
            )?,
            None,
        )
    } else if plain_distinct && values.is_empty() {
        let (expr, alias) = match key {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
            _ => {
                return unsupported(
                    "plain SELECT DISTINCT filter/project output key must be a direct column",
                )
            }
        };
        let Some(column) =
            expression_filter_project_column(expr, catalog, relation_alias, source_projection)
        else {
            return unsupported(
                "plain SELECT DISTINCT filter/project output key must be a registered relation column",
            );
        };
        validate_filter_project_value_column(catalog, column)?;
        if column.nullable {
            return unsupported(
                "plain SELECT DISTINCT filter/project output key column must be non-null",
            );
        }
        let default_name = source_projection
            .and_then(|projection| source_projection_output_name(expr, relation_alias, projection))
            .unwrap_or(column.name.as_str());
        (
            alias.unwrap_or(default_name).to_string(),
            Some(column.column_id.clone()),
        )
    } else {
        return unsupported("first filter/project projection must be the primary key column");
    };
    let mut output_ids = BTreeSet::from([output_key_column_id.clone()]);
    let mut value_columns = Vec::with_capacity(values.len());
    let mut typed_value_columns = Vec::new();
    for item in values {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
            _ => return unsupported("filter/project projection items must be direct columns"),
        };
        let (input_column_id, expression, default_name) = if let Some(column) =
            expression_filter_project_column(expr, catalog, relation_alias, source_projection)
        {
            validate_filter_project_value_column(catalog, column)?;
            let default_name = source_projection
                .and_then(|projection| {
                    source_projection_output_name(expr, relation_alias, projection)
                })
                .unwrap_or(column.name.as_str());
            (column.column_id.clone(), None, default_name)
        } else {
            if source_projection.is_some() {
                return unsupported(
                    "filter/project source projection references must use projected column names",
                );
            }
            let Some(alias) = alias else {
                return unsupported("computed filter/project projections require an alias");
            };
            let Some(typed_column) =
                supported_filter_project_typed_projection(expr, catalog, relation_alias)?
            else {
                let expression =
                    supported_filter_project_projection_expr(expr, catalog, relation_alias)?;
                let input_column_id =
                    first_supported_projection_expr_column_id(&expression).ok_or_else(|| {
                        ViewPlanError::UnsupportedShape {
                            reason: "computed filter/project projections must reference at least one registered column".to_string(),
                        }
                    })?;
                let output_column_id = alias.to_string();
                if !output_ids.insert(output_column_id.clone()) {
                    return unsupported("filter/project output column ids must be unique");
                }
                value_columns.push(ValidatedProjectionColumn {
                    input_column_id,
                    output_column_id,
                    expression: Some(expression),
                });
                continue;
            };
            let output_column_id = alias.to_string();
            if !output_ids.insert(output_column_id.clone()) {
                return unsupported("filter/project output column ids must be unique");
            }
            typed_value_columns.push(TypedProjectionColumn {
                output_column_id,
                program: typed_column,
            });
            continue;
        };
        let output_column_id = alias.unwrap_or(default_name).to_string();
        if !output_ids.insert(output_column_id.clone()) {
            return unsupported("filter/project output column ids must be unique");
        }
        value_columns.push(ValidatedProjectionColumn {
            input_column_id,
            output_column_id,
            expression,
        });
    }
    Ok(ValidatedFilterProjectProjection {
        output_key_column_id,
        output_key_input_column_id,
        value_columns,
        typed_value_columns,
    })
}

fn filter_project_wildcard_qualifier_matches(
    qualifier: &ObjectName,
    catalog: &VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
) -> bool {
    let Some(qualifier) = single_object_name_identifier(qualifier) else {
        return false;
    };
    if let Some(alias) = relation_alias {
        return identifier_eq(qualifier.as_str(), alias);
    }
    [
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_name.as_str(),
        catalog.datafusion_registration.name.as_str(),
    ]
    .iter()
    .any(|candidate| identifier_eq(qualifier.as_str(), candidate))
}

fn supported_filter_project_projection_expr(
    expr: &Expr,
    catalog: &VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
) -> Result<SupportedProjectionExpr, ViewPlanError> {
    supported_filter_project_bound_projection_expr(expr, catalog, relation_alias, None)
}

fn supported_filter_project_bound_projection_expr(
    expr: &Expr,
    catalog: &VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
) -> Result<SupportedProjectionExpr, ViewPlanError> {
    match expr {
        Expr::Nested(inner) => supported_filter_project_bound_projection_expr(
            inner,
            catalog,
            relation_alias,
            source_projection,
        ),
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let Some(column) =
                expression_filter_project_column(expr, catalog, relation_alias, source_projection)
            else {
                return unsupported("computed projection column is not registered");
            };
            validate_filter_project_int64_expr_column(catalog, column)?;
            Ok(SupportedProjectionExpr::Column {
                column_id: column.column_id.clone(),
            })
        }
        Expr::Value(value) => supported_projection_literal(&value.value, false),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            if let Expr::Value(value) = expr.as_ref() {
                return supported_projection_literal(&value.value, true);
            }
            Ok(SupportedProjectionExpr::BinaryInt64 {
                op: SupportedProjectionBinaryOp::Subtract,
                left: Box::new(SupportedProjectionExpr::LiteralInt64 { value: 0 }),
                right: Box::new(supported_filter_project_bound_projection_expr(
                    expr,
                    catalog,
                    relation_alias,
                    source_projection,
                )?),
            })
        }
        Expr::BinaryOp { left, op, right } => {
            let op = match op {
                BinaryOperator::Plus => SupportedProjectionBinaryOp::Add,
                BinaryOperator::Minus => SupportedProjectionBinaryOp::Subtract,
                BinaryOperator::Multiply => SupportedProjectionBinaryOp::Multiply,
                BinaryOperator::Divide => SupportedProjectionBinaryOp::Divide,
                BinaryOperator::Modulo => SupportedProjectionBinaryOp::Modulo,
                _ => return unsupported("computed projection operator is not supported"),
            };
            Ok(SupportedProjectionExpr::BinaryInt64 {
                op,
                left: Box::new(supported_filter_project_bound_projection_expr(
                    left,
                    catalog,
                    relation_alias,
                    source_projection,
                )?),
                right: Box::new(supported_filter_project_bound_projection_expr(
                    right,
                    catalog,
                    relation_alias,
                    source_projection,
                )?),
            })
        }
        Expr::Cast {
            kind,
            expr,
            data_type,
            array,
            format,
        } => {
            if !matches!(
                kind,
                CastKind::Cast | CastKind::DoubleColon | CastKind::TryCast | CastKind::SafeCast
            ) || *array
                || format.is_some()
            {
                return unsupported("computed projection CAST form is not supported");
            }
            if !is_supported_int64_cast_type(data_type) {
                return unsupported("computed projection CAST target must be BIGINT or INT64");
            }
            supported_filter_project_bound_projection_expr(
                expr,
                catalog,
                relation_alias,
                source_projection,
            )
        }
        Expr::Function(function) => {
            if !matches!(function.parameters, FunctionArguments::None)
                || function.filter.is_some()
                || function.over.is_some()
                || !function.within_group.is_empty()
            {
                return unsupported("computed projection function is not supported");
            }
            let FunctionArguments::List(arguments) = &function.args else {
                return unsupported("computed projection function requires an argument list");
            };
            if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
                return unsupported(
                    "computed projection function DISTINCT arguments and clauses are not supported",
                );
            }
            if function_name_eq(&function.name, "abs") {
                let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] =
                    arguments.args.as_slice()
                else {
                    return unsupported("ABS requires one expression argument");
                };
                return Ok(SupportedProjectionExpr::AbsInt64 {
                    expr: Box::new(supported_filter_project_bound_projection_expr(
                        argument,
                        catalog,
                        relation_alias,
                        source_projection,
                    )?),
                });
            }
            if function_name_eq(&function.name, "coalesce") {
                let [FunctionArg::Unnamed(FunctionArgExpr::Expr(column_expr)), FunctionArg::Unnamed(FunctionArgExpr::Expr(fallback_expr))] =
                    arguments.args.as_slice()
                else {
                    return unsupported("COALESCE requires a column and fallback literal");
                };
                let Some(column) = expression_filter_project_column(
                    column_expr,
                    catalog,
                    relation_alias,
                    source_projection,
                ) else {
                    return unsupported("COALESCE column is not registered");
                };
                validate_coalesce_int64_column(catalog, column)?;
                let SupportedProjectionExpr::LiteralInt64 { value } =
                    supported_projection_literal_expr(fallback_expr)?
                else {
                    return unsupported("COALESCE fallback must be an Int64 literal");
                };
                return Ok(SupportedProjectionExpr::CoalesceInt64 {
                    column_id: column.column_id.clone(),
                    fallback: value,
                });
            }
            if function_name_eq(&function.name, "if") {
                if source_projection.is_some() {
                    return unsupported("IF over projected sources is not supported");
                }
                let [FunctionArg::Unnamed(FunctionArgExpr::Expr(predicate)), FunctionArg::Unnamed(FunctionArgExpr::Expr(then_expr)), FunctionArg::Unnamed(FunctionArgExpr::Expr(else_expr))] =
                    arguments.args.as_slice()
                else {
                    return unsupported(
                        "IF requires predicate, then, and else expression arguments",
                    );
                };
                return Ok(SupportedProjectionExpr::CaseInt64 {
                    predicate: validate_case_projection_predicate_expr(
                        predicate,
                        catalog,
                        relation_alias,
                    )?,
                    then_expr: Box::new(supported_filter_project_bound_projection_expr(
                        then_expr,
                        catalog,
                        relation_alias,
                        source_projection,
                    )?),
                    else_expr: Box::new(supported_filter_project_bound_projection_expr(
                        else_expr,
                        catalog,
                        relation_alias,
                        source_projection,
                    )?),
                });
            }
            if function_name_eq(&function.name, "greatest")
                || function_name_eq(&function.name, "least")
            {
                if arguments.args.len() < 2 {
                    return unsupported("GREATEST/LEAST require at least two expression arguments");
                }
                let exprs = arguments
                    .args
                    .iter()
                    .map(|argument| {
                        let FunctionArg::Unnamed(FunctionArgExpr::Expr(argument)) = argument else {
                            return unsupported("GREATEST/LEAST only support expression arguments");
                        };
                        supported_filter_project_bound_projection_expr(
                            argument,
                            catalog,
                            relation_alias,
                            source_projection,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if function_name_eq(&function.name, "greatest") {
                    return Ok(SupportedProjectionExpr::GreatestInt64 { exprs });
                }
                return Ok(SupportedProjectionExpr::LeastInt64 { exprs });
            }
            // String expression functions are handled by the typed
            // projection surface; the legacy Int64 expression surface
            // rejects them with a pointer to the supported path.
            if function_name_eq(&function.name, "length")
                || function_name_eq(&function.name, "char_length")
                || function_name_eq(&function.name, "character_length")
                || function_name_eq(&function.name, "concat")
                || function_name_eq(&function.name, "substring")
                || function_name_eq(&function.name, "substr")
                || function_name_eq(&function.name, "trim")
                || function_name_eq(&function.name, "upper")
                || function_name_eq(&function.name, "lower")
            {
                return unsupported(
                    "string expressions are supported through typed projections; the legacy Int64 expression surface does not admit string functions",
                );
            }
            unsupported("computed projection function is not supported")
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if source_projection.is_some() {
                return unsupported("CASE expressions over projected sources are not supported");
            }
            if conditions.is_empty() {
                return unsupported("computed CASE expressions require WHEN branches");
            }
            let else_result =
                else_result
                    .as_ref()
                    .ok_or_else(|| ViewPlanError::UnsupportedShape {
                        reason: "computed CASE expressions require ELSE".to_string(),
                    })?;
            let mut expression = supported_filter_project_bound_projection_expr(
                else_result,
                catalog,
                relation_alias,
                source_projection,
            )?;
            for branch in conditions.iter().rev() {
                let predicate = if let Some(operand) = operand.as_ref() {
                    validate_simple_case_projection_predicate_expr(
                        operand,
                        &branch.condition,
                        catalog,
                        relation_alias,
                    )?
                } else {
                    validate_case_projection_predicate_expr(
                        &branch.condition,
                        catalog,
                        relation_alias,
                    )?
                };
                expression = SupportedProjectionExpr::CaseInt64 {
                    predicate,
                    then_expr: Box::new(supported_filter_project_bound_projection_expr(
                        &branch.result,
                        catalog,
                        relation_alias,
                        source_projection,
                    )?),
                    else_expr: Box::new(expression),
                };
            }
            Ok(expression)
        }
        Expr::Substring { .. } | Expr::Trim { .. } => unsupported(
            "string expressions are supported through typed projections; the legacy Int64 expression surface does not admit string functions",
        ),
        _ => unsupported("computed projection expression is not supported"),
    }
}

fn validate_simple_case_projection_predicate_expr(
    operand: &Expr,
    when_expr: &Expr,
    catalog: &VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
) -> Result<RowPredicateExpr, ViewPlanError> {
    let Some(column) = expression_catalog_column(operand, catalog, relation_alias) else {
        return unsupported(
            "computed simple CASE operand must reference a registered relation column",
        );
    };
    validate_filter_project_int64_expr_column(catalog, column)?;
    if !expression_is_literal(when_expr) {
        return unsupported("computed simple CASE WHEN value must be a literal");
    }
    Ok(RowPredicateExpr::Atom {
        predicate: RowPredicate {
            column_id: column.column_id.clone(),
            op: PredicateOp::Eq,
            literal: predicate_literal(when_expr)?,
        },
    })
}

fn validate_case_projection_predicate_expr(
    selection: &Expr,
    catalog: &VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
) -> Result<RowPredicateExpr, ViewPlanError> {
    if let Expr::Nested(inner) = selection {
        return validate_case_projection_predicate_expr(inner, catalog, relation_alias);
    }
    if let Expr::UnaryOp {
        op: UnaryOperator::Not,
        expr,
    } = selection
    {
        return Ok(negate_row_predicate_expr(
            validate_case_projection_predicate_expr(expr, catalog, relation_alias)?,
        ));
    }
    if let Expr::Between {
        expr,
        negated,
        low,
        high,
    } = selection
    {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported(
                "computed CASE WHEN column must reference a registered relation column",
            );
        };
        validate_case_projection_int64_predicate_column(catalog, column)?;
        return row_between_predicate_expr(column.column_id.clone(), low, high, *negated);
    }
    if let Expr::InList {
        expr,
        list,
        negated,
    } = selection
    {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported(
                "computed CASE WHEN column must reference a registered relation column",
            );
        };
        validate_case_projection_int64_predicate_column(catalog, column)?;
        return row_in_list_predicate_expr(
            column.column_id.clone(),
            column.nullable,
            list,
            *negated,
        );
    }
    if let Expr::IsNull(expr) = selection {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported(
                "computed CASE WHEN column must reference a registered relation column",
            );
        };
        validate_case_projection_int64_predicate_column(catalog, column)?;
        return Ok(row_null_predicate_expr(column.column_id.clone(), false));
    }
    if let Expr::IsNotNull(expr) = selection {
        let Some(column) = expression_catalog_column(expr, catalog, relation_alias) else {
            return unsupported(
                "computed CASE WHEN column must reference a registered relation column",
            );
        };
        validate_case_projection_int64_predicate_column(catalog, column)?;
        return Ok(row_null_predicate_expr(column.column_id.clone(), true));
    }
    if let Some((left, right, op)) = distinct_predicate_parts(selection) {
        let (column, literal_expr) = distinct_column_literal(
            left,
            right,
            catalog,
            relation_alias,
            "computed CASE WHEN column must reference a registered relation column",
            "computed CASE WHEN IS DISTINCT FROM must compare a catalog column to a literal",
        )?;
        validate_case_projection_int64_predicate_column(catalog, column)?;
        return Ok(RowPredicateExpr::Atom {
            predicate: RowPredicate {
                column_id: column.column_id.clone(),
                op,
                literal: predicate_literal(literal_expr)?,
            },
        });
    }
    let Expr::BinaryOp { left, op, right } = selection else {
        return unsupported("computed CASE WHEN supports column/literal comparisons");
    };
    if *op == BinaryOperator::And {
        return Ok(RowPredicateExpr::And {
            left: Box::new(validate_case_projection_predicate_expr(
                left,
                catalog,
                relation_alias,
            )?),
            right: Box::new(validate_case_projection_predicate_expr(
                right,
                catalog,
                relation_alias,
            )?),
        });
    }
    if *op == BinaryOperator::Or {
        return Ok(RowPredicateExpr::Or {
            left: Box::new(validate_case_projection_predicate_expr(
                left,
                catalog,
                relation_alias,
            )?),
            right: Box::new(validate_case_projection_predicate_expr(
                right,
                catalog,
                relation_alias,
            )?),
        });
    }
    let (column_expr, literal_expr, op) = if expression_is_literal(right) {
        (left.as_ref(), right.as_ref(), op.clone())
    } else if expression_is_literal(left) {
        let Some(op) = reverse_predicate_op(op.clone()) else {
            return unsupported("computed CASE WHEN comparison operator is not supported");
        };
        (right.as_ref(), left.as_ref(), op)
    } else {
        return unsupported("computed CASE WHEN must compare a catalog column to a literal");
    };
    let Some(column) = expression_catalog_column(column_expr, catalog, relation_alias) else {
        return unsupported(
            "computed CASE WHEN column must reference a registered relation column",
        );
    };
    let Some(op) = predicate_op(op) else {
        return unsupported("computed CASE WHEN comparison operator is not supported");
    };
    let literal = predicate_literal(literal_expr)?;
    validate_case_projection_comparison_predicate_column(catalog, column, op, &literal)?;
    Ok(RowPredicateExpr::Atom {
        predicate: RowPredicate {
            column_id: column.column_id.clone(),
            op,
            literal,
        },
    })
}

fn validate_case_projection_int64_predicate_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    if column.column_id == catalog.relation_schema.weight_column_id {
        return unsupported("computed CASE WHEN must not reference weight");
    }
    if !matches!(column.physical_arrow_type, ArrowPhysicalTypeV1::Int64) {
        return unsupported("computed CASE WHEN predicate requires an Int64 column");
    }
    Ok(())
}

fn validate_case_projection_comparison_predicate_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
    op: PredicateOp,
    literal: &JsonValue,
) -> Result<(), ViewPlanError> {
    if column.column_id == catalog.relation_schema.weight_column_id {
        return unsupported("computed CASE WHEN must not reference weight");
    }
    if matches!(column.physical_arrow_type, ArrowPhysicalTypeV1::Boolean) {
        if !matches!(op, PredicateOp::Eq | PredicateOp::NotEq) {
            return unsupported("computed CASE WHEN Boolean predicates support = and <> only");
        }
        if !literal.is_boolean() {
            return unsupported("Boolean CASE predicates require Boolean literals");
        }
    }
    if !matches!(
        column.physical_arrow_type,
        ArrowPhysicalTypeV1::Int64 | ArrowPhysicalTypeV1::Boolean
    ) {
        return unsupported("computed CASE WHEN predicate requires an Int64 or Boolean column");
    }
    Ok(())
}

fn supported_projection_literal(
    value: &SqlValue,
    negative: bool,
) -> Result<SupportedProjectionExpr, ViewPlanError> {
    let SqlValue::Number(value, _) = value else {
        return unsupported("computed projection literal must be an Int64 number");
    };
    if value.contains('.') {
        return unsupported("computed projection literal must be an Int64 number");
    }
    let value = if negative {
        format!("-{value}")
    } else {
        value.clone()
    };
    Ok(SupportedProjectionExpr::LiteralInt64 {
        value: value
            .parse::<i64>()
            .map_err(|_| ViewPlanError::UnsupportedShape {
                reason: "computed projection literal must be an Int64 number".to_string(),
            })?,
    })
}

fn supported_projection_literal_expr(
    expr: &Expr,
) -> Result<SupportedProjectionExpr, ViewPlanError> {
    match expr {
        Expr::Value(value) => supported_projection_literal(&value.value, false),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let Expr::Value(value) = expr.as_ref() else {
                return unsupported("computed projection literal must be an Int64 number");
            };
            supported_projection_literal(&value.value, true)
        }
        _ => unsupported("computed projection literal must be an Int64 number"),
    }
}

fn is_supported_int64_cast_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::BigInt(_) | DataType::Int8(_) | DataType::Int64
    )
}

fn validate_coalesce_int64_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    if column.column_id == catalog.relation_schema.weight_column_id {
        return unsupported("COALESCE expressions must not reference weight");
    }
    if !matches!(column.physical_arrow_type, ArrowPhysicalTypeV1::Int64) {
        return unsupported("COALESCE expressions require Int64 columns");
    }
    Ok(())
}

fn validate_filter_project_int64_expr_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    if column.column_id == catalog.relation_schema.weight_column_id {
        return unsupported("computed filter/project expressions must not reference weight");
    }
    if column.nullable || !matches!(column.physical_arrow_type, ArrowPhysicalTypeV1::Int64) {
        return unsupported("computed filter/project expressions require non-null Int64 columns");
    }
    Ok(())
}

fn first_supported_projection_expr_column_id(expr: &SupportedProjectionExpr) -> Option<String> {
    match expr {
        SupportedProjectionExpr::Column { column_id } => Some(column_id.clone()),
        SupportedProjectionExpr::LiteralInt64 { .. } => None,
        SupportedProjectionExpr::BinaryInt64 { left, right, .. } => {
            first_supported_projection_expr_column_id(left)
                .or_else(|| first_supported_projection_expr_column_id(right))
        }
        SupportedProjectionExpr::AbsInt64 { expr } => {
            first_supported_projection_expr_column_id(expr)
        }
        SupportedProjectionExpr::GreatestInt64 { exprs }
        | SupportedProjectionExpr::LeastInt64 { exprs } => exprs
            .iter()
            .find_map(first_supported_projection_expr_column_id),
        SupportedProjectionExpr::CoalesceInt64 { column_id, .. } => Some(column_id.clone()),
        SupportedProjectionExpr::CaseInt64 {
            predicate,
            then_expr,
            else_expr,
        } => predicate
            .leaf_predicates()
            .into_iter()
            .map(|predicate| predicate.column_id)
            .next()
            .or_else(|| first_supported_projection_expr_column_id(then_expr))
            .or_else(|| first_supported_projection_expr_column_id(else_expr)),
    }
}

fn supported_projection_expr_column_ids(expr: &SupportedProjectionExpr) -> Vec<String> {
    let mut columns = Vec::new();
    collect_supported_projection_expr_column_ids(expr, &mut columns);
    columns
}

fn collect_supported_projection_expr_column_ids(
    expr: &SupportedProjectionExpr,
    columns: &mut Vec<String>,
) {
    match expr {
        SupportedProjectionExpr::Column { column_id } => {
            if !columns.iter().any(|existing| existing == column_id) {
                columns.push(column_id.clone());
            }
        }
        SupportedProjectionExpr::LiteralInt64 { .. } => {}
        SupportedProjectionExpr::BinaryInt64 { left, right, .. } => {
            collect_supported_projection_expr_column_ids(left, columns);
            collect_supported_projection_expr_column_ids(right, columns);
        }
        SupportedProjectionExpr::AbsInt64 { expr } => {
            collect_supported_projection_expr_column_ids(expr, columns);
        }
        SupportedProjectionExpr::GreatestInt64 { exprs }
        | SupportedProjectionExpr::LeastInt64 { exprs } => {
            for expr in exprs {
                collect_supported_projection_expr_column_ids(expr, columns);
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
            for predicate in predicate.leaf_predicates() {
                if !columns
                    .iter()
                    .any(|existing| existing == &predicate.column_id)
                {
                    columns.push(predicate.column_id);
                }
            }
            collect_supported_projection_expr_column_ids(then_expr, columns);
            collect_supported_projection_expr_column_ids(else_expr, columns);
        }
    }
}

fn validate_filter_project_value_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    if column.column_id == catalog.relation_schema.weight_column_id {
        return unsupported("filter/project value columns must not reference the weight column");
    }
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Boolean
        | ArrowPhysicalTypeV1::Utf8
        | ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Float64
        | ArrowPhysicalTypeV1::Decimal128 { .. } => Ok(()),
        _ => unsupported("filter/project value column type is not supported by the runtime"),
    }
}

fn select_item_references_column(
    item: &SelectItem,
    column: &RelationColumnV1,
    relation_alias: Option<&str>,
) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) => expression_references_column(expr, column, relation_alias),
        SelectItem::ExprWithAlias { expr, .. } => {
            expression_references_column(expr, column, relation_alias)
        }
        _ => false,
    }
}

fn select_item_references_bound_column(
    item: &SelectItem,
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expression_references_bound_column(
                expr,
                catalog,
                column,
                relation_alias,
                source_projection,
            )
        }
        _ => false,
    }
}

fn select_item_identifier_alias_or_default(item: &SelectItem, expected: &str) -> Option<String> {
    match item {
        SelectItem::UnnamedExpr(expr) if expression_references_identifier(expr, expected) => {
            Some(expected.to_string())
        }
        SelectItem::ExprWithAlias { expr, alias }
            if expression_references_identifier(expr, expected) =>
        {
            Some(alias.value.clone())
        }
        _ => None,
    }
}

fn select_item_references_qualified_column(
    item: &SelectItem,
    qualifier: &str,
    column: &RelationColumnV1,
) -> bool {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return false,
    };
    expression_references_qualified_column(expr, qualifier, column)
}

fn select_item_alias_or_default(item: &SelectItem, default: &str) -> Result<String, ViewPlanError> {
    match item {
        SelectItem::UnnamedExpr(_) => Ok(default.to_string()),
        SelectItem::ExprWithAlias { alias, .. } => Ok(alias.value.clone()),
        _ => unsupported("projection item must be an expression"),
    }
}

fn select_item_alias_or_source_default(
    item: &SelectItem,
    default: &str,
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
) -> Result<String, ViewPlanError> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Ok(alias.value.clone()),
        SelectItem::UnnamedExpr(expr) => {
            if let Some(source_projection) = source_projection {
                if let Some(output_name) =
                    source_projection_output_name(expr, relation_alias, source_projection)
                {
                    return Ok(output_name.to_string());
                }
            }
            Ok(default.to_string())
        }
        _ => unsupported("projection item must be an expression"),
    }
}

fn validate_tumbling_group_by(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    relation_alias: Option<&str>,
    output_key_column_id: &str,
) -> Result<Option<EventTimeWindowGroupBySpec>, ViewPlanError> {
    let (expressions, modifiers) = match &select.group_by {
        GroupByExpr::All(modifiers) if modifiers.is_empty() => return Ok(None),
        GroupByExpr::All(_) => return unsupported("GROUP BY ALL modifiers are not supported"),
        GroupByExpr::Expressions(expressions, modifiers) => (expressions, modifiers),
    };
    if !modifiers.is_empty() {
        return unsupported("GROUP BY modifiers are not supported");
    }
    match expressions.as_slice() {
        [group_key, window_start, window_end]
            if (expression_is_first_select_projection_ordinal(group_key)
                || expression_references_group_key(
                group_key,
                catalog,
                key_column,
                relation_alias,
                output_key_column_id,
            ))
                && expression_references_identifier(window_start, "window_start")
                && expression_references_identifier(window_end, "window_end") =>
        {
            Ok(None)
        }
        [group_key, window]
            if (expression_is_first_select_projection_ordinal(group_key)
                || expression_references_group_key(
                group_key,
                catalog,
                key_column,
                relation_alias,
                output_key_column_id,
            ))
                && expression_is_event_time_window_function(window) =>
        {
            Ok(Some(event_time_window_group_by_spec(window)?))
        }
        [group_key, ..]
            if !(expression_is_first_select_projection_ordinal(group_key)
                || expression_references_group_key(
                    group_key,
                    catalog,
                    key_column,
                    relation_alias,
                    output_key_column_id,
                )) =>
        {
            unsupported("tumbling GROUP BY first expression must be the catalog primary key column")
        }
        _ => unsupported(
            "expected GROUP BY key, window_start, window_end or GROUP BY key, TUMBLE/HOP/SESSION(interval...)",
        ),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EventTimeWindowGroupBySpec {
    kind: SupportedEventTimeWindowKind,
    window_size_ns: i64,
    hop_slide_ns: Option<i64>,
    session_gap_ns: Option<i64>,
}

fn expression_is_event_time_window_function(expr: &Expr) -> bool {
    let Expr::Function(function) = expr else {
        return false;
    };
    let is_window = function_name_eq(&function.name, "tumble")
        || function_name_eq(&function.name, "hop")
        || function_name_eq(&function.name, "session");
    is_window
        && matches!(function.parameters, FunctionArguments::None)
        && function.filter.is_none()
        && function.over.is_none()
        && function.within_group.is_empty()
}

fn event_time_window_group_by_spec(
    expr: &Expr,
) -> Result<EventTimeWindowGroupBySpec, ViewPlanError> {
    let Expr::Function(function) = expr else {
        return unsupported("GROUP BY window expression must be a function call");
    };
    if !expression_is_event_time_window_function(expr) {
        return unsupported(
            "GROUP BY window functions do not support parameters, FILTER, OVER, or WITHIN GROUP",
        );
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return unsupported("GROUP BY window function requires interval arguments");
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return unsupported("GROUP BY window DISTINCT and clauses are not supported");
    }
    let Some(function_name) = single_object_name_identifier(&function.name) else {
        return unsupported("window function name must be unqualified");
    };
    match function_name.to_ascii_lowercase().as_str() {
        "tumble" => {
            let [interval_arg] = arguments.args.as_slice() else {
                return unsupported("GROUP BY TUMBLE requires exactly one interval argument");
            };
            let window_size_ns = positive_window_interval_ns(interval_arg, "TUMBLE")?;
            Ok(EventTimeWindowGroupBySpec {
                kind: SupportedEventTimeWindowKind::Tumbling,
                window_size_ns,
                hop_slide_ns: None,
                session_gap_ns: None,
            })
        }
        "hop" => {
            let [slide_arg, size_arg] = arguments.args.as_slice() else {
                return unsupported("GROUP BY HOP requires slide and size interval arguments");
            };
            let hop_slide_ns = positive_window_interval_ns(slide_arg, "HOP slide")?;
            let window_size_ns = positive_window_interval_ns(size_arg, "HOP size")?;
            if window_size_ns < hop_slide_ns || window_size_ns % hop_slide_ns != 0 {
                return unsupported(
                    "GROUP BY HOP requires size to be a positive multiple of slide",
                );
            }
            Ok(EventTimeWindowGroupBySpec {
                kind: SupportedEventTimeWindowKind::Hopping,
                window_size_ns,
                hop_slide_ns: Some(hop_slide_ns),
                session_gap_ns: None,
            })
        }
        "session" => {
            let [gap_arg] = arguments.args.as_slice() else {
                return unsupported("GROUP BY SESSION requires exactly one gap interval argument");
            };
            let session_gap_ns = positive_window_interval_ns(gap_arg, "SESSION gap")?;
            Ok(EventTimeWindowGroupBySpec {
                kind: SupportedEventTimeWindowKind::Session,
                window_size_ns: session_gap_ns,
                hop_slide_ns: None,
                session_gap_ns: Some(session_gap_ns),
            })
        }
        _ => unsupported("unsupported event-time window function"),
    }
}

fn positive_window_interval_ns(arg: &FunctionArg, label: &str) -> Result<i64, ViewPlanError> {
    let interval_ns = table_function_interval_ns_arg(arg)?;
    if interval_ns <= 0 {
        return unsupported(format!("GROUP BY {label} interval must be positive"));
    }
    Ok(interval_ns)
}

fn select_item_aggregate<'a>(
    item: &SelectItem,
    catalog: &'a VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
    allow_filter: bool,
    source_projection: Option<&SourceProjection>,
) -> Result<ParsedAggregateProjection<'a>, ViewPlanError> {
    let (expr, alias) = match item {
        SelectItem::UnnamedExpr(expr) => (expr, None),
        SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
        _ => return unsupported("aggregate projection must be an expression"),
    };
    let Expr::Function(function) = expr else {
        return unsupported("aggregate projection must be a supported aggregate function");
    };
    if !matches!(function.parameters, FunctionArguments::None)
        || (!allow_filter && function.filter.is_some())
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported(
            "aggregate parameters, FILTER, OVER, and WITHIN GROUP are not supported",
        );
    }
    let Some(function_name) = single_object_name_identifier(&function.name) else {
        return unsupported("aggregate function name must be unqualified");
    };
    let canonical_function = function_name.to_ascii_lowercase();
    let output_column_id = alias.unwrap_or(canonical_function.as_str()).to_string();
    let filter_expr = function.filter.as_deref().cloned();

    let FunctionArguments::List(arguments) = &function.args else {
        return unsupported("aggregate arguments must be a simple argument list");
    };
    if !arguments.clauses.is_empty() {
        return unsupported("aggregate argument clauses are not supported");
    }

    match canonical_function.as_str() {
        "sum" | "avg" | "min" | "max" => {
            if arguments.duplicate_treatment.is_some() {
                return unsupported("DISTINCT value aggregate arguments are not supported");
            }
            let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice()
            else {
                return unsupported("aggregate value functions require one column argument");
            };
            let function = match canonical_function.as_str() {
                "sum" => LogicalPlanAggregateFunctionV1::Sum,
                "avg" => LogicalPlanAggregateFunctionV1::Avg,
                "min" => LogicalPlanAggregateFunctionV1::Min,
                "max" => LogicalPlanAggregateFunctionV1::Max,
                _ => unreachable!("validated aggregate function"),
            };
            let (column, input_expression) = if let Some(column) = match source_projection {
                Some(projection) => source_projection_expression_column(
                    argument,
                    catalog,
                    relation_alias,
                    projection,
                ),
                None => expression_column(argument, catalog, relation_alias),
            } {
                (column, None)
            } else if matches!(
                function,
                LogicalPlanAggregateFunctionV1::Sum
                    | LogicalPlanAggregateFunctionV1::Avg
                    | LogicalPlanAggregateFunctionV1::Min
                    | LogicalPlanAggregateFunctionV1::Max
            ) {
                if let Some(projection) = source_projection {
                    let Some(column) = expression_column(argument, catalog, relation_alias) else {
                        return unsupported(
                            "aggregate input must reference a projected source column",
                        );
                    };
                    if source_projection_projects_column(projection, column.column_id.as_str()) {
                        return unsupported(
                            "aggregate input must reference a projected source column",
                        );
                    }
                    (column, None)
                } else {
                    let expression = supported_filter_project_projection_expr(
                        argument,
                        catalog,
                        relation_alias,
                    )?;
                    let column_ids = supported_projection_expr_column_ids(&expression);
                    let [column_id] = column_ids.as_slice() else {
                        return unsupported(
                            "aggregate input expressions must reference exactly one Int64 value column",
                        );
                    };
                    let column = catalog
                        .relation_schema
                        .columns
                        .iter()
                        .find(|column| column.column_id == *column_id)
                        .ok_or_else(|| ViewPlanError::UnsupportedShape {
                            reason:
                                "aggregate input expression column is missing from relation catalog"
                                    .to_string(),
                        })?;
                    (column, Some(expression))
                }
            } else {
                return unsupported("aggregate input must reference a registered relation column");
            };
            Ok(ParsedAggregateProjection {
                output: SupportedAggregateOutput {
                    function,
                    input_column_id: Some(column.column_id.clone()),
                    input_relation_side: None,
                    input_expression,
                    output_column_id,
                },
                input_column: Some(column),
                count_input_column: None,
                filter_expr: filter_expr.clone(),
            })
        }
        "median" => {
            if arguments.duplicate_treatment.is_some() {
                return unsupported("DISTINCT MEDIAN is not supported");
            }
            let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice()
            else {
                return unsupported("MEDIAN requires one column argument");
            };
            let column = match source_projection {
                Some(projection) => source_projection_expression_column(
                    argument,
                    catalog,
                    relation_alias,
                    projection,
                ),
                None => expression_column(argument, catalog, relation_alias),
            }
            .ok_or_else(|| ViewPlanError::UnsupportedShape {
                reason: "MEDIAN requires a direct column argument".to_string(),
            })?;
            let input_expression: Option<SupportedProjectionExpr> = None;
            if input_expression.is_some() {
                return unsupported("MEDIAN does not support computed input expressions");
            }
            if !matches!(
                column.physical_arrow_type,
                ArrowPhysicalTypeV1::Int64 | ArrowPhysicalTypeV1::Decimal128 { .. }
            ) {
                return unsupported("MEDIAN currently supports Int64 or Decimal128 input columns");
            }
            Ok(ParsedAggregateProjection {
                output: SupportedAggregateOutput {
                    function: LogicalPlanAggregateFunctionV1::PercentileCont { percentile: 0.5 },
                    input_column_id: Some(column.column_id.clone()),
                    input_relation_side: None,
                    input_expression: None,
                    output_column_id,
                },
                input_column: Some(column),
                count_input_column: None,
                filter_expr: filter_expr.clone(),
            })
        }
        name @ ("percentile_disc" | "percentile_cont") => {
            if arguments.duplicate_treatment.is_some() {
                return unsupported("DISTINCT percentile arguments are not supported");
            }
            let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument)), FunctionArg::Unnamed(FunctionArgExpr::Expr(percentile_expr))] =
                arguments.args.as_slice()
            else {
                return unsupported(format!(
                    "{name} requires exactly (column, percentile) arguments"
                ));
            };
            let percentile = match percentile_expr {
                Expr::Value(ValueWithSpan {
                    value: SqlValue::Number(text, _),
                    ..
                }) => text.parse::<f64>().ok(),
                _ => None,
            };
            let Some(percentile) = percentile else {
                return unsupported(format!("{name} percentile must be a numeric literal"));
            };
            if !(0.0..=1.0).contains(&percentile) {
                return unsupported(format!("{name} percentile must be in [0, 1]"));
            }
            let column = match source_projection {
                Some(projection) => source_projection_expression_column(
                    argument,
                    catalog,
                    relation_alias,
                    projection,
                ),
                None => expression_column(argument, catalog, relation_alias),
            }
            .ok_or_else(|| ViewPlanError::UnsupportedShape {
                reason: format!("{name} requires a direct column argument"),
            })?;
            let input_expression: Option<SupportedProjectionExpr> = None;
            if input_expression.is_some() {
                return unsupported(format!(
                    "{name} does not support computed input expressions"
                ));
            }
            if !matches!(
                column.physical_arrow_type,
                ArrowPhysicalTypeV1::Int64 | ArrowPhysicalTypeV1::Decimal128 { .. }
            ) {
                return unsupported(format!(
                    "{name} currently supports Int64 or Decimal128 input columns"
                ));
            }
            Ok(ParsedAggregateProjection {
                output: SupportedAggregateOutput {
                    function: if name == "percentile_disc" {
                        LogicalPlanAggregateFunctionV1::PercentileDisc { percentile }
                    } else {
                        LogicalPlanAggregateFunctionV1::PercentileCont { percentile }
                    },
                    input_column_id: Some(column.column_id.clone()),
                    input_relation_side: None,
                    input_expression: None,
                    output_column_id,
                },
                input_column: Some(column),
                count_input_column: None,
                filter_expr: filter_expr.clone(),
            })
        }
        "count" => {
            let is_distinct = matches!(
                arguments.duplicate_treatment,
                Some(DuplicateTreatment::Distinct)
            );
            let count_input_column =
                validate_count_argument(arguments, catalog, relation_alias, source_projection)?;
            Ok(ParsedAggregateProjection {
                output: SupportedAggregateOutput {
                    function: if is_distinct {
                        LogicalPlanAggregateFunctionV1::CountDistinct
                    } else {
                        LogicalPlanAggregateFunctionV1::Count
                    },
                    input_column_id: count_input_column.map(|column| column.column_id.clone()),
                    input_relation_side: None,
                    input_expression: None,
                    output_column_id,
                },
                input_column: None,
                count_input_column,
                filter_expr,
            })
        }
        _ => unsupported("aggregate function is not supported by this materialized runtime"),
    }
}

fn select_item_is_function(item: &SelectItem, expected: &str) -> bool {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return false,
    };
    let Expr::Function(function) = expr else {
        return false;
    };
    function_name_eq(&function.name, expected)
}

fn validate_join_value_aggregate_select_item<'a>(
    item: &SelectItem,
    left_alias: &str,
    left_catalog: &'a VelorixRelationCatalogV1,
    right_alias: &str,
    right_catalog: &'a VelorixRelationCatalogV1,
) -> Result<
    (
        SupportedAggregateOutput,
        SupportedAggregateInputRelationSide,
        &'a RelationColumnV1,
    ),
    ViewPlanError,
> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return unsupported("JOIN aggregate projection must be an expression"),
    };
    let Expr::Function(function) = expr else {
        return unsupported("JOIN aggregate projection must be a supported aggregate function");
    };
    if !matches!(function.parameters, FunctionArguments::None)
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported("JOIN aggregate modifiers, OVER, and WITHIN GROUP are not supported");
    }
    let Some(function_name) = single_object_name_identifier(&function.name) else {
        return unsupported("JOIN aggregate function name must be unqualified");
    };
    let canonical_function = function_name.to_ascii_lowercase();
    let aggregate_function = match canonical_function.as_str() {
        "sum" => LogicalPlanAggregateFunctionV1::Sum,
        "avg" => LogicalPlanAggregateFunctionV1::Avg,
        "min" => LogicalPlanAggregateFunctionV1::Min,
        "max" => LogicalPlanAggregateFunctionV1::Max,
        _ => return unsupported("JOIN aggregate function is not supported by this runtime"),
    };
    let FunctionArguments::List(arguments) = &function.args else {
        return unsupported("JOIN aggregate arguments must be a simple argument list");
    };
    if !arguments.clauses.is_empty() {
        return unsupported("JOIN aggregate argument clauses are not supported");
    }
    if arguments.duplicate_treatment.is_some() {
        return unsupported("JOIN DISTINCT value aggregate arguments are not supported");
    }
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice() else {
        return unsupported("JOIN aggregate value functions require one qualified column argument");
    };
    let (side, catalog, column, input_expression) = validate_join_value_aggregate_argument(
        argument,
        left_alias,
        left_catalog,
        right_alias,
        right_catalog,
    )?;
    if side == SupportedAggregateInputRelationSide::Right
        && column.column_id == right_catalog.relation_schema.weight_column_id
    {
        return unsupported(
            "JOIN value aggregate input must not reference the right weight column",
        );
    }
    match aggregate_function {
        LogicalPlanAggregateFunctionV1::Avg => validate_numeric_avg_column(column)?,
        LogicalPlanAggregateFunctionV1::Sum
        | LogicalPlanAggregateFunctionV1::Min
        | LogicalPlanAggregateFunctionV1::Max => validate_numeric_sum_column(catalog, column)?,
        LogicalPlanAggregateFunctionV1::PercentileDisc { .. }
        | LogicalPlanAggregateFunctionV1::PercentileCont { .. } => {
            if !matches!(
                column.physical_arrow_type,
                ArrowPhysicalTypeV1::Int64 | ArrowPhysicalTypeV1::Decimal128 { .. }
            ) {
                return unsupported(
                    "percentile aggregates currently support Int64 or Decimal128 input columns",
                );
            }
        }
        LogicalPlanAggregateFunctionV1::Count | LogicalPlanAggregateFunctionV1::CountDistinct => {
            unreachable!("validated value aggregate function")
        }
    }
    Ok((
        SupportedAggregateOutput {
            function: aggregate_function,
            input_column_id: Some(column.column_id.clone()),
            input_relation_side: Some(side),
            input_expression,
            output_column_id: select_item_alias_or_default(item, canonical_function.as_str())?,
        },
        side,
        column,
    ))
}

fn validate_join_value_aggregate_argument<'a>(
    argument: &Expr,
    left_alias: &str,
    left_catalog: &'a VelorixRelationCatalogV1,
    right_alias: &str,
    right_catalog: &'a VelorixRelationCatalogV1,
) -> Result<
    (
        SupportedAggregateInputRelationSide,
        &'a VelorixRelationCatalogV1,
        &'a RelationColumnV1,
        Option<SupportedProjectionExpr>,
    ),
    ViewPlanError,
> {
    if let Ok(reference) = qualified_column_ref(argument) {
        let (side, catalog) = if identifier_eq(reference.qualifier.as_str(), left_alias) {
            (SupportedAggregateInputRelationSide::Left, left_catalog)
        } else if identifier_eq(reference.qualifier.as_str(), right_alias) {
            (SupportedAggregateInputRelationSide::Right, right_catalog)
        } else {
            return unsupported("JOIN value aggregate input must reference a joined table alias");
        };
        return Ok((
            side,
            catalog,
            qualified_ref_catalog_column(&reference, catalog)?,
            None,
        ));
    }

    let left = join_value_aggregate_expression_match(
        argument,
        left_alias,
        left_catalog,
        SupportedAggregateInputRelationSide::Left,
    )?;
    let right = join_value_aggregate_expression_match(
        argument,
        right_alias,
        right_catalog,
        SupportedAggregateInputRelationSide::Right,
    )?;
    match (left, right) {
        (Some(matched), None) | (None, Some(matched)) => Ok(matched),
        (Some(_), Some(_)) => unsupported(
            "JOIN aggregate input expressions must reference exactly one joined relation side",
        ),
        (None, None)
            if expr_references_qualified_alias(argument, left_alias)
                && expr_references_qualified_alias(argument, right_alias) =>
        {
            unsupported(
                "JOIN aggregate input expressions must reference exactly one joined relation side",
            )
        }
        (None, None) => {
            unsupported("JOIN aggregate input expression must reference one joined table alias")
        }
    }
}

type JoinValueAggregateExpressionMatch<'a> = (
    SupportedAggregateInputRelationSide,
    &'a VelorixRelationCatalogV1,
    &'a RelationColumnV1,
    Option<SupportedProjectionExpr>,
);

fn join_value_aggregate_expression_match<'a>(
    argument: &Expr,
    relation_alias: &str,
    catalog: &'a VelorixRelationCatalogV1,
    side: SupportedAggregateInputRelationSide,
) -> Result<Option<JoinValueAggregateExpressionMatch<'a>>, ViewPlanError> {
    let Ok(expression) =
        supported_filter_project_projection_expr(argument, catalog, Some(relation_alias))
    else {
        return Ok(None);
    };
    let column_ids = supported_projection_expr_column_ids(&expression);
    let [column_id] = column_ids.as_slice() else {
        return unsupported(
            "JOIN aggregate input expressions must reference exactly one Int64 value column",
        );
    };
    let column = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == *column_id)
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "JOIN aggregate input expression column is missing from relation catalog"
                .to_string(),
        })?;
    Ok(Some((side, catalog, column, Some(expression))))
}

fn expr_references_qualified_alias(expr: &Expr, alias: &str) -> bool {
    match expr {
        Expr::CompoundIdentifier(parts) => {
            let [qualifier, _] = parts.as_slice() else {
                return false;
            };
            identifier_eq(qualifier.value.as_str(), alias)
        }
        Expr::Nested(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::Cast { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_references_qualified_alias(inner, alias),
        Expr::BinaryOp { left, right, .. } => {
            expr_references_qualified_alias(left, alias)
                || expr_references_qualified_alias(right, alias)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_qualified_alias(expr, alias)
                || expr_references_qualified_alias(low, alias)
                || expr_references_qualified_alias(high, alias)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_qualified_alias(expr, alias)
                || list
                    .iter()
                    .any(|expr| expr_references_qualified_alias(expr, alias))
        }
        Expr::Function(function) => match &function.args {
            FunctionArguments::List(arguments) => arguments
                .args
                .iter()
                .any(|argument| function_arg_references_qualified_alias(argument, alias)),
            _ => false,
        },
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_references_qualified_alias(expr, alias))
                || conditions.iter().any(|branch| {
                    expr_references_qualified_alias(&branch.condition, alias)
                        || expr_references_qualified_alias(&branch.result, alias)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|expr| expr_references_qualified_alias(expr, alias))
        }
        _ => false,
    }
}

fn function_arg_references_qualified_alias(argument: &FunctionArg, alias: &str) -> bool {
    match argument {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
            expr_references_qualified_alias(expr, alias)
        }
        _ => false,
    }
}

fn select_item_function_filter(item: &SelectItem) -> Option<&Expr> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return None,
    };
    let Expr::Function(function) = expr else {
        return None;
    };
    function.filter.as_deref()
}

fn validate_join_count_select_item(
    item: &SelectItem,
    left_alias: &str,
    left_catalog: &VelorixRelationCatalogV1,
    right_alias: &str,
    right_catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedAggregateOutput, ViewPlanError> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return unsupported("third projection must be a count aggregate"),
    };
    let output_column_id = select_item_alias_or_default(item, "count")?;
    let Expr::Function(function) = expr else {
        return unsupported("third projection must be a count aggregate");
    };
    if !function_name_eq(&function.name, "count")
        || !matches!(function.parameters, FunctionArguments::None)
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported("third projection must be a count aggregate");
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return unsupported("count arguments must be a simple argument list");
    };
    let is_distinct = matches!(
        arguments.duplicate_treatment,
        Some(DuplicateTreatment::Distinct)
    );
    let argument = validate_join_count_argument(
        arguments,
        left_alias,
        left_catalog,
        right_alias,
        right_catalog,
    )?;
    Ok(SupportedAggregateOutput {
        function: if is_distinct {
            LogicalPlanAggregateFunctionV1::CountDistinct
        } else {
            LogicalPlanAggregateFunctionV1::Count
        },
        input_relation_side: argument.as_ref().map(|argument| argument.relation_side),
        input_column_id: argument.map(|argument| argument.column_id),
        input_expression: None,
        output_column_id,
    })
}

fn validate_count_argument<'a>(
    arguments: &FunctionArgumentList,
    catalog: &'a VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
) -> Result<Option<&'a RelationColumnV1>, ViewPlanError> {
    if !arguments.clauses.is_empty() {
        return unsupported("count argument clauses are not supported");
    }
    let is_distinct = matches!(
        arguments.duplicate_treatment,
        Some(DuplicateTreatment::Distinct)
    );
    if matches!(
        arguments.args.as_slice(),
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
    ) {
        if is_distinct {
            return unsupported("count(DISTINCT *) is not supported");
        }
        return Ok(None);
    }
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice() else {
        return unsupported(
            "count argument must be *, one non-null literal, or one non-null relation column",
        );
    };
    if expression_is_non_null_literal(argument) {
        if is_distinct {
            return unsupported("count(DISTINCT literal) is not supported");
        }
        return Ok(None);
    }
    let Some(column) = source_projection
        .and_then(|projection| {
            source_projection_expression_column(argument, catalog, relation_alias, projection)
                .or_else(|| {
                    let column = expression_column(argument, catalog, relation_alias)?;
                    (!source_projection_projects_column(projection, column.column_id.as_str()))
                        .then_some(column)
                })
        })
        .or_else(|| {
            source_projection
                .is_none()
                .then(|| expression_column(argument, catalog, relation_alias))
                .flatten()
        })
    else {
        return unsupported(
            "count argument must be *, one non-null literal, or one non-null relation column",
        );
    };
    if is_distinct || column.nullable {
        return Ok(Some(column));
    }
    Ok(None)
}

fn validate_join_count_argument(
    arguments: &FunctionArgumentList,
    left_alias: &str,
    left_catalog: &VelorixRelationCatalogV1,
    right_alias: &str,
    right_catalog: &VelorixRelationCatalogV1,
) -> Result<Option<JoinCountArgument>, ViewPlanError> {
    if !arguments.clauses.is_empty() {
        return unsupported("JOIN count argument clauses are not supported");
    }
    let is_distinct = matches!(
        arguments.duplicate_treatment,
        Some(DuplicateTreatment::Distinct)
    );
    if matches!(
        arguments.args.as_slice(),
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
    ) {
        if is_distinct {
            return unsupported("JOIN count(DISTINCT *) is not supported");
        }
        return Ok(None);
    }
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice() else {
        return unsupported(
            "JOIN count argument must be *, one non-null literal, or one non-null qualified column",
        );
    };
    if expression_is_non_null_literal(argument) {
        if is_distinct {
            return unsupported("JOIN count(DISTINCT literal) is not supported");
        }
        return Ok(None);
    }
    let reference = qualified_column_ref(argument)?;
    let (relation_side, column) = if identifier_eq(reference.qualifier.as_str(), left_alias) {
        (
            SupportedAggregateInputRelationSide::Left,
            qualified_ref_catalog_column(&reference, left_catalog)?,
        )
    } else if !right_alias.is_empty() && identifier_eq(reference.qualifier.as_str(), right_alias) {
        (
            SupportedAggregateInputRelationSide::Right,
            qualified_ref_catalog_column(&reference, right_catalog)?,
        )
    } else {
        return unsupported("JOIN count argument must reference a joined table alias");
    };
    if relation_side == SupportedAggregateInputRelationSide::Right
        && column.column_id == right_catalog.relation_schema.weight_column_id
    {
        return unsupported("JOIN count input must not reference the right weight column");
    }
    Ok(Some(JoinCountArgument {
        column_id: column.column_id.clone(),
        relation_side,
    }))
}

fn validate_join_order_by_count_argument(
    arguments: &FunctionArgumentList,
    left_alias: &str,
    left_catalog: &VelorixRelationCatalogV1,
    right_alias: &str,
    right_catalog: &VelorixRelationCatalogV1,
) -> Result<Option<JoinCountArgument>, ViewPlanError> {
    if !arguments.clauses.is_empty() {
        return unsupported("JOIN count argument clauses are not supported");
    }
    let is_distinct = matches!(
        arguments.duplicate_treatment,
        Some(DuplicateTreatment::Distinct)
    );
    if matches!(
        arguments.args.as_slice(),
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
    ) {
        if is_distinct {
            return unsupported("JOIN count(DISTINCT *) is not supported");
        }
        return Ok(None);
    }
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice() else {
        return unsupported(
            "JOIN count argument must be *, one non-null literal, or one non-null qualified column",
        );
    };
    if expression_is_non_null_literal(argument) {
        if is_distinct {
            return unsupported("JOIN count(DISTINCT literal) is not supported");
        }
        return Ok(None);
    }
    let reference = qualified_column_ref(argument)?;
    let (relation_side, column) = if identifier_eq(reference.qualifier.as_str(), left_alias) {
        (
            SupportedAggregateInputRelationSide::Left,
            qualified_ref_catalog_column(&reference, left_catalog)?,
        )
    } else if !right_alias.is_empty() && identifier_eq(reference.qualifier.as_str(), right_alias) {
        (
            SupportedAggregateInputRelationSide::Right,
            qualified_ref_catalog_column(&reference, right_catalog)?,
        )
    } else {
        return unsupported("JOIN count argument must reference a joined table alias");
    };
    Ok(Some(JoinCountArgument {
        column_id: column.column_id.clone(),
        relation_side,
    }))
}

fn expression_is_non_null_literal(expr: &Expr) -> bool {
    predicate_literal(expr).is_ok()
}

fn expression_references_column(
    expr: &Expr,
    column: &RelationColumnV1,
    relation_alias: Option<&str>,
) -> bool {
    match expr {
        Expr::Identifier(ident) => column_identifier_eq(column, ident.value.as_str()),
        Expr::CompoundIdentifier(parts) => {
            let [qualifier, name] = parts.as_slice() else {
                return false;
            };
            relation_alias.is_some_and(|alias| identifier_eq(alias, qualifier.value.as_str()))
                && column_identifier_eq(column, name.value.as_str())
        }
        _ => false,
    }
}

fn expression_references_group_key(
    expr: &Expr,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    relation_alias: Option<&str>,
    output_key_column_id: &str,
) -> bool {
    expression_references_column(expr, key_column, relation_alias)
        || expression_references_unambiguous_output_alias(expr, catalog, output_key_column_id)
}

fn expression_is_first_select_projection_ordinal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Value(value) if matches!(value.value, SqlValue::Number(ref text, _) if text == "1")
    )
}

fn expression_references_unambiguous_output_alias(
    expr: &Expr,
    catalog: &VelorixRelationCatalogV1,
    output_column_id: &str,
) -> bool {
    let Expr::Identifier(ident) = expr else {
        return false;
    };
    if !identifier_eq(ident.value.as_str(), output_column_id) {
        return false;
    }
    !catalog
        .relation_schema
        .columns
        .iter()
        .any(|column| column_identifier_eq(column, output_column_id))
}

fn expression_references_catalog_column(
    expr: &Expr,
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
    relation_alias: Option<&str>,
) -> bool {
    if expression_references_column(expr, column, relation_alias) {
        return true;
    }
    let Expr::CompoundIdentifier(parts) = expr else {
        return false;
    };
    let [qualifier, name] = parts.as_slice() else {
        return false;
    };
    if !column_identifier_eq(column, name.value.as_str()) {
        return false;
    }
    if relation_alias.is_some() {
        return false;
    }
    let accepted = [
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_name.as_str(),
        catalog.datafusion_registration.name.as_str(),
    ];
    accepted
        .iter()
        .any(|candidate| identifier_eq(candidate, qualifier.value.as_str()))
}

fn expression_references_identifier(expr: &Expr, expected: &str) -> bool {
    matches!(expr, Expr::Identifier(ident) if identifier_eq(ident.value.as_str(), expected))
}

fn expression_references_bound_column(
    expr: &Expr,
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
    relation_alias: Option<&str>,
    source_projection: Option<&SourceProjection>,
) -> bool {
    let Some(source_projection) = source_projection else {
        return expression_references_column(expr, column, relation_alias);
    };
    source_projection_expression_column(expr, catalog, relation_alias, source_projection)
        .is_some_and(|bound_column| bound_column.column_id == column.column_id)
}

fn expression_column<'a>(
    expr: &Expr,
    catalog: &'a VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
) -> Option<&'a RelationColumnV1> {
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| expression_references_catalog_column(expr, catalog, column, relation_alias))
}

fn source_projection_expression_column<'a>(
    expr: &Expr,
    catalog: &'a VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
    source_projection: &SourceProjection,
) -> Option<&'a RelationColumnV1> {
    let output_name = source_projection_output_name(expr, relation_alias, source_projection)?;
    let projected = source_projection
        .columns
        .iter()
        .find(|column| identifier_eq(column.output_name.as_str(), output_name))?;
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == projected.input_column_id)
}

fn source_projection_output_name<'a>(
    expr: &Expr,
    relation_alias: Option<&str>,
    source_projection: &'a SourceProjection,
) -> Option<&'a str> {
    let output_name = match expr {
        Expr::Identifier(ident) => ident.value.as_str(),
        Expr::CompoundIdentifier(parts) => {
            let [qualifier, name] = parts.as_slice() else {
                return None;
            };
            if !relation_alias.is_some_and(|alias| identifier_eq(alias, qualifier.value.as_str())) {
                return None;
            }
            name.value.as_str()
        }
        _ => return None,
    };
    source_projection
        .columns
        .iter()
        .find(|column| identifier_eq(column.output_name.as_str(), output_name))
        .map(|column| column.output_name.as_str())
}

fn source_projection_projects_column(
    source_projection: &SourceProjection,
    column_id: &str,
) -> bool {
    source_projection
        .columns
        .iter()
        .any(|column| column.input_column_id == column_id)
}

fn expression_references_qualified_column(
    expr: &Expr,
    qualifier: &str,
    column: &RelationColumnV1,
) -> bool {
    let Ok(reference) = qualified_column_ref(expr) else {
        return false;
    };
    identifier_eq(reference.qualifier.as_str(), qualifier)
        && column_identifier_eq(column, reference.column.as_str())
}

fn catalog_primary_key_column(
    catalog: &VelorixRelationCatalogV1,
) -> Result<&RelationColumnV1, ViewPlanError> {
    let [column_id] = catalog.relation_schema.primary_key_column_ids.as_slice() else {
        return unsupported("view SQL currently requires exactly one primary key column");
    };
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| &column.column_id == column_id)
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "relation catalog primary key column is missing".to_string(),
        })
}

/// Find the primary key column name for a published-view relation schema.
///
/// A published `RelationSchema` carries its key as `primary_key: Vec<String>`.
/// The single-key sum/count family requires exactly one primary key column.
fn relation_primary_key_column(relation: &RelationSchema) -> Result<String, ViewPlanError> {
    let [column_id] = relation.primary_key.as_slice() else {
        return unsupported("published view SQL currently requires exactly one primary key column");
    };
    if !relation
        .columns
        .iter()
        .any(|column| &column.name == column_id)
    {
        return unsupported(
            "published view primary key column is missing from the relation schema",
        );
    }
    Ok(column_id.clone())
}

fn validate_join_key_pairs_for_incremental_state(
    left_catalog: &VelorixRelationCatalogV1,
    right_catalog: &VelorixRelationCatalogV1,
    join_kind: SupportedJoinKind,
    pairs: &[(&RelationColumnV1, &RelationColumnV1)],
) -> Result<Option<SupportedJoinKeyDomainV1>, ViewPlanError> {
    if join_kind != SupportedJoinKind::Inner && pairs.len() > 1 {
        return unsupported("composite equality is currently supported only for INNER JOIN");
    }
    let left_primary_keys = &left_catalog.relation_schema.primary_key_column_ids;
    let right_primary_keys = &right_catalog.relation_schema.primary_key_column_ids;
    if left_primary_keys.len() == 1 && right_primary_keys.len() == 1 && pairs.len() != 1 {
        return unsupported("JOIN ON must contain exactly one key equality");
    }
    let left_pair_columns = pairs
        .iter()
        .map(|(left, _)| left.column_id.as_str())
        .collect::<BTreeSet<_>>();
    let right_pair_columns = pairs
        .iter()
        .map(|(_, right)| right.column_id.as_str())
        .collect::<BTreeSet<_>>();
    let left_primary_keys = left_primary_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let right_primary_keys = right_primary_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let covers_primary_keys =
        left_pair_columns == left_primary_keys && right_pair_columns == right_primary_keys;
    if !covers_primary_keys {
        let Some((left, right)) = pairs.first().copied().filter(|_| pairs.len() == 1) else {
            return unsupported(
                "JOIN equality columns must cover every primary key column of both inputs exactly once; non-primary joins currently require exactly one equality",
            );
        };
        if join_kind != SupportedJoinKind::Inner {
            return unsupported("non-primary equality is currently supported only for INNER JOIN");
        }
        if left_primary_keys.contains(left.column_id.as_str())
            || right_primary_keys.contains(right.column_id.as_str())
        {
            return unsupported(
                "partial primary-key equality is not supported; use one non-primary equality or both complete primary keys",
            );
        }
        if left.column_id == left_catalog.relation_schema.weight_column_id
            || right.column_id == right_catalog.relation_schema.weight_column_id
        {
            return unsupported("JOIN equality must not reference a weight column");
        }
        if left.nullable || right.nullable {
            return unsupported(
                "non-primary JOIN equality columns must be non-null until SQL NULL key semantics are implemented",
            );
        }
        if !supported_scalar_join_key_atom(&left.physical_arrow_type)
            || !supported_scalar_join_key_atom(&right.physical_arrow_type)
        {
            return unsupported(
                "non-primary JOIN equality currently supports only existing scalar primary-key atom types",
            );
        }
    }
    if pairs
        .iter()
        .any(|(left, right)| left.physical_arrow_type != right.physical_arrow_type)
    {
        return if pairs.len() == 1 {
            unsupported("JOIN ON primary key columns must have identical physical Arrow types")
        } else {
            unsupported(
                "corresponding JOIN primary key columns must have identical physical Arrow types",
            )
        };
    }
    Ok((!covers_primary_keys).then_some(SupportedJoinKeyDomainV1::NonPrimaryNonNullScalarV1))
}

fn supported_scalar_join_key_atom(physical_type: &ArrowPhysicalTypeV1) -> bool {
    !matches!(
        physical_type,
        ArrowPhysicalTypeV1::List { .. }
            | ArrowPhysicalTypeV1::Struct { .. }
            | ArrowPhysicalTypeV1::Map { .. }
    )
}

fn validate_numeric_sum_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    if column.column_id == catalog.relation_schema.weight_column_id {
        return unsupported("sum(value_column) must not reference the weight column");
    }
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 | ArrowPhysicalTypeV1::Decimal128 { .. } => Ok(()),
        _ => unsupported("sum(value_column) currently supports Int64 or Decimal128 columns"),
    }
}

fn validate_numeric_avg_column(column: &RelationColumnV1) -> Result<(), ViewPlanError> {
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 | ArrowPhysicalTypeV1::Decimal128 { .. } => Ok(()),
        _ => unsupported("avg(value_column) currently supports Int64 or Decimal128 columns"),
    }
}

fn validate_latest_value_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    if column.column_id == catalog.relation_schema.weight_column_id {
        return unsupported("latest-by-key value must not reference the weight column");
    }
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Boolean
        | ArrowPhysicalTypeV1::Utf8
        | ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Float64
        | ArrowPhysicalTypeV1::Decimal128 { .. } => Ok(()),
        _ => unsupported("latest-by-key value column type is not supported by runtime"),
    }
}

fn validate_latest_ordering_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    if column.column_id == catalog.relation_schema.weight_column_id {
        return unsupported("latest-by-key ordering must not reference the weight column");
    }
    if column.nullable {
        return unsupported("latest-by-key ordering column must be non-nullable");
    }
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Date32
        | ArrowPhysicalTypeV1::TimestampNanosecond { .. } => Ok(()),
        _ => unsupported(
            "latest-by-key ordering currently supports Int64, Date32, or TimestampNanosecond",
        ),
    }
}

fn validate_row_number_partition_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    if column.column_id == catalog.relation_schema.weight_column_id {
        return unsupported("ROW_NUMBER partition must not reference the weight column");
    }
    if column.nullable {
        return unsupported("ROW_NUMBER partition column must be non-nullable");
    }
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Boolean
        | ArrowPhysicalTypeV1::Utf8
        | ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Float64
        | ArrowPhysicalTypeV1::Decimal128 { .. }
        | ArrowPhysicalTypeV1::Date32
        | ArrowPhysicalTypeV1::TimestampNanosecond { .. } => Ok(()),
        _ => unsupported("ROW_NUMBER partition column type is not supported by runtime"),
    }
}

fn single_object_name_identifier(name: &ObjectName) -> Option<String> {
    let [part] = name.0.as_slice() else {
        return None;
    };
    part.as_ident().map(|ident| ident.value.clone())
}

fn function_name_eq(name: &ObjectName, expected: &str) -> bool {
    single_object_name_identifier(name)
        .as_deref()
        .is_some_and(|name| identifier_eq(name, expected))
}

fn column_identifier_eq(column: &RelationColumnV1, candidate: &str) -> bool {
    identifier_eq(column.column_id.as_str(), candidate)
        || identifier_eq(column.name.as_str(), candidate)
}

fn identifier_eq(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn unsupported<T>(reason: impl Into<String>) -> Result<T, ViewPlanError> {
    Err(ViewPlanError::UnsupportedShape {
        reason: reason.into(),
    })
}

/// Parses a computed filter/project projection into the typed expression IR
/// (Phase 6 string/temporal/float families). Returns `None` for expressions
/// that belong to the legacy Int64-only `SupportedProjectionExpr` surface and
/// `Err` for typed-family expressions that fail type checking.
fn supported_filter_project_typed_projection(
    expr: &Expr,
    catalog: &VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
) -> Result<Option<TypedExprProgramV1>, ViewPlanError> {
    let root = match typed_expr_node_from_expr(expr, catalog, relation_alias)? {
        Some(node) => node,
        None => return Ok(None),
    };
    let program = TypedExprProgramV1 {
        encoding_version: TYPED_EXPR_PROGRAM_SCHEMA_VERSION_V1,
        root,
    };
    program
        .validate()
        .map_err(|error| ViewPlanError::UnsupportedShape {
            reason: format!("typed projection is invalid: {error}"),
        })?;
    Ok(Some(program))
}

fn typed_expr_node_from_expr(
    expr: &Expr,
    catalog: &VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
) -> Result<Option<TypedExprNodeV1>, ViewPlanError> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let Some(column) =
                expression_filter_project_column(expr, catalog, relation_alias, None)
            else {
                return unsupported("typed projections must reference registered relation columns");
            };
            let result_type = typed_scalar_type_for_column(column)?;
            // The public contract binds event-time columns as Int64
            // nanoseconds; typed temporal functions consume them as
            // timestamps.
            let result_type = if catalog
                .relation_schema
                .event_time_column_id
                .as_ref()
                .is_some_and(|event_time_column_id| event_time_column_id == &column.column_id)
            {
                RuntimeScalarTypeV1::TimestampNanosecond
            } else {
                result_type
            };
            Ok(Some(TypedExprNodeV1 {
                result_type,
                nullable: column.nullable,
                kind: TypedExprKindV1::Column {
                    column_id: column.column_id.clone(),
                },
            }))
        }
        Expr::Value(value) => typed_literal_node(value),
        Expr::Function(Function {
            name: function_name,
            args,
            ..
        }) => {
            let name = function_name.to_string();
            // Phase 8.4: compiled-in UDF names always route to the typed
            // surface with their pinned identity.
            if let Some(identity) = builtin_udf_identity_for_name(name.as_str()) {
                let FunctionArguments::List(argument_list) = args else {
                    return unsupported("builtin UDFs require a normal argument list");
                };
                let mut arg_nodes = Vec::new();
                for argument in &argument_list.args {
                    let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) = argument else {
                        return unsupported("builtin UDFs only accept expression arguments");
                    };
                    let Some(node) = typed_expr_node_from_expr(arg_expr, catalog, relation_alias)?
                    else {
                        return unsupported(
                            "builtin UDF arguments must be columns, literals, or typed functions",
                        );
                    };
                    arg_nodes.push(node);
                }
                let Some((arity, _, result_type)) = builtin_udf_spec(&identity) else {
                    return unsupported(format!("builtin UDF `{name}` has no type specification"));
                };
                if arg_nodes.len() != arity {
                    return unsupported(format!(
                        "builtin UDF `{name}` expects {arity} arguments, got {}",
                        arg_nodes.len()
                    ));
                }
                let nullable = arg_nodes.iter().any(|node| node.nullable);
                return Ok(Some(TypedExprNodeV1 {
                    result_type,
                    nullable,
                    kind: TypedExprKindV1::UdfCall {
                        identity,
                        args: arg_nodes,
                    },
                }));
            }
            if !is_typed_function_name(&name) {
                return Ok(None);
            }
            let function =
                typed_function_for_name(&name).ok_or_else(|| ViewPlanError::UnsupportedShape {
                    reason: format!("unsupported typed function `{name}`"),
                })?;
            let FunctionArguments::List(argument_list) = args else {
                return unsupported("typed functions require a normal argument list");
            };
            let mut arg_nodes = Vec::new();
            for argument in &argument_list.args {
                let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) = argument else {
                    return unsupported("typed functions only accept expression arguments");
                };
                let Some(node) = typed_expr_node_from_expr(arg_expr, catalog, relation_alias)?
                else {
                    return unsupported(
                        "typed function arguments must be columns, literals, or typed functions",
                    );
                };
                arg_nodes.push(node);
            }
            let function = typed_function_with_field(name.as_str(), function, &arg_nodes)?;
            let result_type = typed_call_result_type(function, &arg_nodes)?;
            Ok(Some(TypedExprNodeV1 {
                result_type,
                nullable: arg_nodes.iter().any(|node| node.nullable),
                kind: TypedExprKindV1::Call {
                    function,
                    args: arg_nodes,
                },
            }))
        }
        Expr::BinaryOp { left, op, right } => {
            // Interval literals are the only binary operand forms the typed
            // surface owns on the right side (timestamp/date arithmetic).
            // Everything else is either a float-typed arithmetic pair or
            // legacy Int64 arithmetic.
            let mut interval_left = None;
            let mut interval_right = None;
            for (index, arg_expr) in [&**left, &**right].into_iter().enumerate() {
                if let Expr::Interval(interval) = arg_expr {
                    let Some(Some(ns)) = typed_interval_nanoseconds(interval) else {
                        return unsupported(
                            "typed arithmetic intervals must have a fixed duration",
                        );
                    };
                    let node = TypedExprNodeV1 {
                        result_type: RuntimeScalarTypeV1::Int64,
                        nullable: false,
                        kind: TypedExprKindV1::Literal {
                            value: ScalarLiteralV1::Int64(ns),
                        },
                    };
                    if index == 0 {
                        interval_left = Some(node);
                    } else {
                        interval_right = Some(node);
                    }
                    continue;
                }
                let Some(node) = typed_expr_node_from_expr(arg_expr, catalog, relation_alias)?
                else {
                    return Ok(None);
                };
                if index == 0 {
                    interval_left = Some(node);
                } else {
                    interval_right = Some(node);
                }
            }
            let left_node = interval_left.ok_or_else(|| ViewPlanError::UnsupportedShape {
                reason: "typed binary expressions require a left operand".to_string(),
            })?;
            let right_node = interval_right.ok_or_else(|| ViewPlanError::UnsupportedShape {
                reason: "typed binary expressions require a right operand".to_string(),
            })?;
            let function = if matches!(right_node.kind, TypedExprKindV1::Literal { .. })
                && left_node.result_type == RuntimeScalarTypeV1::TimestampNanosecond
                && right_node.result_type == RuntimeScalarTypeV1::Int64
            {
                match op {
                    BinaryOperator::Plus => BuiltinScalarFunctionV1::TimestampAddNanoseconds,
                    BinaryOperator::Minus => BuiltinScalarFunctionV1::TimestampSubtractNanoseconds,
                    _ => return Ok(None),
                }
            } else if left_node.result_type == RuntimeScalarTypeV1::Date32
                && right_node.result_type == RuntimeScalarTypeV1::Int64
                && op == &BinaryOperator::Plus
            {
                BuiltinScalarFunctionV1::DateAddDays
            } else if (left_node.result_type == RuntimeScalarTypeV1::Float64
                || left_node.result_type == RuntimeScalarTypeV1::Int64)
                && (right_node.result_type == RuntimeScalarTypeV1::Float64
                    || right_node.result_type == RuntimeScalarTypeV1::Int64)
                && (left_node.result_type == RuntimeScalarTypeV1::Float64
                    || right_node.result_type == RuntimeScalarTypeV1::Float64)
            {
                match op {
                    BinaryOperator::Plus => BuiltinScalarFunctionV1::AddFloat64,
                    BinaryOperator::Minus => BuiltinScalarFunctionV1::SubtractFloat64,
                    BinaryOperator::Multiply => BuiltinScalarFunctionV1::MultiplyFloat64,
                    BinaryOperator::Divide => BuiltinScalarFunctionV1::DivideFloat64,
                    _ => return Ok(None),
                }
            } else {
                return Ok(None);
            };
            let arg_nodes = vec![left_node, right_node];
            let result_type = typed_call_result_type(function, &arg_nodes)?;
            Ok(Some(TypedExprNodeV1 {
                result_type,
                nullable: arg_nodes.iter().any(|node| node.nullable),
                kind: TypedExprKindV1::Call {
                    function,
                    args: arg_nodes,
                },
            }))
        }
        Expr::Extract { field, expr, .. } => {
            let function = match field {
                DateTimeField::Year | DateTimeField::Years => BuiltinScalarFunctionV1::ExtractYear,
                DateTimeField::Month | DateTimeField::Months => {
                    BuiltinScalarFunctionV1::ExtractMonth
                }
                DateTimeField::Day | DateTimeField::Days => BuiltinScalarFunctionV1::ExtractDay,
                DateTimeField::Hour | DateTimeField::Hours => BuiltinScalarFunctionV1::ExtractHour,
                DateTimeField::Minute | DateTimeField::Minutes => {
                    BuiltinScalarFunctionV1::ExtractMinute
                }
                DateTimeField::Second | DateTimeField::Seconds => {
                    BuiltinScalarFunctionV1::ExtractSecond
                }
                _ => {
                    return unsupported(format!(
                        "EXTRACT field `{field}` is not supported by the typed runtime"
                    ))
                }
            };
            let Some(node) = typed_expr_node_from_expr(expr, catalog, relation_alias)? else {
                return unsupported(
                    "EXTRACT argument must be a column, literal, or typed function",
                );
            };
            let result_type = typed_call_result_type(function, std::slice::from_ref(&node))?;
            Ok(Some(TypedExprNodeV1 {
                result_type,
                nullable: node.nullable,
                kind: TypedExprKindV1::Call {
                    function,
                    args: vec![node],
                },
            }))
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let mut arg_nodes = Vec::new();
            for arg_expr in std::iter::once(&**expr)
                .chain(substring_from.iter().map(|arg| &**arg))
                .chain(substring_for.iter().map(|arg| &**arg))
            {
                let Some(node) = typed_expr_node_from_expr(arg_expr, catalog, relation_alias)?
                else {
                    return unsupported(
                        "SUBSTRING arguments must be columns, literals, or typed functions",
                    );
                };
                arg_nodes.push(node);
            }
            if arg_nodes.len() < 2 {
                return unsupported("SUBSTRING requires a start position");
            }
            for arg in arg_nodes.iter().skip(1) {
                if arg.result_type != RuntimeScalarTypeV1::Int64 {
                    return unsupported("SUBSTRING start/length must be Int64");
                }
            }
            let function = BuiltinScalarFunctionV1::Substring;
            let result_type = typed_call_result_type(function, &arg_nodes)?;
            Ok(Some(TypedExprNodeV1 {
                result_type,
                nullable: arg_nodes.iter().any(|node| node.nullable),
                kind: TypedExprKindV1::Call {
                    function,
                    args: arg_nodes,
                },
            }))
        }
        Expr::Trim {
            trim_where,
            trim_what,
            expr,
            trim_characters,
        } => {
            if matches!(
                trim_where,
                Some(TrimWhereField::Leading) | Some(TrimWhereField::Trailing)
            ) {
                return unsupported(
                    "TRIM with LEADING/TRAILING is not supported; use TRIM(x) or TRIM(chars FROM x)",
                );
            }
            if trim_where.is_some() && trim_what.is_none() {
                return unsupported("TRIM with BOTH requires a FROM character expression");
            }
            if trim_characters
                .as_ref()
                .is_some_and(|characters| !characters.is_empty())
            {
                return unsupported("TRIM comma-separated characters are not supported");
            }
            let mut arg_nodes = Vec::new();
            for arg_expr in std::iter::once(&**expr).chain(trim_what.iter().map(|arg| &**arg)) {
                let Some(node) = typed_expr_node_from_expr(arg_expr, catalog, relation_alias)?
                else {
                    return unsupported(
                        "TRIM arguments must be columns, literals, or typed functions",
                    );
                };
                arg_nodes.push(node);
            }
            for arg in arg_nodes.iter().skip(1) {
                if arg.result_type != RuntimeScalarTypeV1::Utf8 {
                    return unsupported("TRIM characters must be Utf8");
                }
            }
            let function = BuiltinScalarFunctionV1::Trim;
            let result_type = typed_call_result_type(function, &arg_nodes)?;
            Ok(Some(TypedExprNodeV1 {
                result_type,
                nullable: arg_nodes.iter().any(|node| node.nullable),
                kind: TypedExprKindV1::Call {
                    function,
                    args: arg_nodes,
                },
            }))
        }
        Expr::Ceil { expr, field } => {
            typed_ceil_floor_call(expr, field, true, catalog, relation_alias)
        }
        Expr::Floor { expr, field } => {
            typed_ceil_floor_call(expr, field, false, catalog, relation_alias)
        }
        _ => Ok(None),
    }
}

fn typed_ceil_floor_call(
    expr: &Expr,
    field: &CeilFloorKind,
    is_ceil: bool,
    catalog: &VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
) -> Result<Option<TypedExprNodeV1>, ViewPlanError> {
    if !matches!(
        field,
        CeilFloorKind::DateTimeField(DateTimeField::NoDateTime)
    ) {
        return unsupported("CEIL/FLOOR with a TO field or scale argument is not supported");
    }
    let Some(node) = typed_expr_node_from_expr(expr, catalog, relation_alias)? else {
        return unsupported("CEIL/FLOOR arguments must be columns, literals, or typed functions");
    };
    if node.result_type != RuntimeScalarTypeV1::Float64 {
        return unsupported("CEIL/FLOOR require a float64 argument");
    }
    let function = if is_ceil {
        BuiltinScalarFunctionV1::CeilFloat64
    } else {
        BuiltinScalarFunctionV1::FloorFloat64
    };
    let result_type = typed_call_result_type(function, std::slice::from_ref(&node))?;
    Ok(Some(TypedExprNodeV1 {
        result_type,
        nullable: node.nullable,
        kind: TypedExprKindV1::Call {
            function,
            args: vec![node],
        },
    }))
}

fn typed_literal_node(value: &ValueWithSpan) -> Result<Option<TypedExprNodeV1>, ViewPlanError> {
    let node = match &value.value {
        SqlValue::SingleQuotedString(text) => TypedExprNodeV1 {
            result_type: RuntimeScalarTypeV1::Utf8,
            nullable: false,
            kind: TypedExprKindV1::Literal {
                value: ScalarLiteralV1::Utf8 {
                    value: text.clone(),
                },
            },
        },
        SqlValue::Number(text, _) => {
            if text.contains('.') || text.contains('e') || text.contains('E') {
                let number = text
                    .parse::<f64>()
                    .map_err(|_| ViewPlanError::UnsupportedShape {
                        reason: "typed float literal is not a valid number".to_string(),
                    })?;
                TypedExprNodeV1 {
                    result_type: RuntimeScalarTypeV1::Float64,
                    nullable: false,
                    kind: TypedExprKindV1::Literal {
                        value: ScalarLiteralV1::Float64 {
                            canonical_bits: number.to_bits(),
                        },
                    },
                }
            } else {
                let number = text
                    .parse::<i64>()
                    .map_err(|_| ViewPlanError::UnsupportedShape {
                        reason: "typed integer literal is not a valid number".to_string(),
                    })?;
                TypedExprNodeV1 {
                    result_type: RuntimeScalarTypeV1::Int64,
                    nullable: false,
                    kind: TypedExprKindV1::Literal {
                        value: ScalarLiteralV1::Int64(number),
                    },
                }
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(node))
}

fn typed_scalar_type_for_column(
    column: &RelationColumnV1,
) -> Result<RuntimeScalarTypeV1, ViewPlanError> {
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Boolean => Ok(RuntimeScalarTypeV1::Boolean),
        ArrowPhysicalTypeV1::Int64 => Ok(RuntimeScalarTypeV1::Int64),
        ArrowPhysicalTypeV1::Float64 => Ok(RuntimeScalarTypeV1::Float64),
        ArrowPhysicalTypeV1::Utf8 => Ok(RuntimeScalarTypeV1::Utf8),
        ArrowPhysicalTypeV1::Date32 => Ok(RuntimeScalarTypeV1::Date32),
        ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {
            Ok(RuntimeScalarTypeV1::TimestampNanosecond)
        }
        other => unsupported(format!(
            "typed projections do not support input physical type {other:?}"
        )),
    }
}

fn is_typed_function_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "concat"
            | "substring"
            | "substr"
            | "upper"
            | "ucase"
            | "lower"
            | "lcase"
            | "trim"
            | "btrim"
            | "ltrim"
            | "rtrim"
            | "length"
            | "char_length"
            | "character_length"
            | "extract"
            | "date_trunc"
            | "abs"
            | "ceil"
            | "ceiling"
            | "floor"
            | "round"
            | "greatest"
            | "least"
            | "age_days"
    )
}

fn typed_function_for_name(name: &str) -> Option<BuiltinScalarFunctionV1> {
    match name.to_ascii_lowercase().as_str() {
        "concat" => Some(BuiltinScalarFunctionV1::Concat),
        "substring" | "substr" => Some(BuiltinScalarFunctionV1::Substring),
        "upper" | "ucase" => Some(BuiltinScalarFunctionV1::Upper),
        "lower" | "lcase" => Some(BuiltinScalarFunctionV1::Lower),
        "trim" | "btrim" | "ltrim" | "rtrim" => Some(BuiltinScalarFunctionV1::Trim),
        "length" | "char_length" | "character_length" => Some(BuiltinScalarFunctionV1::Length),
        "extract" => Some(BuiltinScalarFunctionV1::ExtractYear),
        "date_trunc" => Some(BuiltinScalarFunctionV1::DateTruncDay),
        "abs" => Some(BuiltinScalarFunctionV1::AbsFloat64),
        "ceil" | "ceiling" => Some(BuiltinScalarFunctionV1::CeilFloat64),
        "floor" => Some(BuiltinScalarFunctionV1::FloorFloat64),
        "round" => Some(BuiltinScalarFunctionV1::RoundFloat64),
        "greatest" => Some(BuiltinScalarFunctionV1::GreatestFloat64),
        "least" => Some(BuiltinScalarFunctionV1::LeastFloat64),
        "age_days" => Some(BuiltinScalarFunctionV1::AgeDays),
        _ => None,
    }
}

fn typed_call_result_type(
    function: BuiltinScalarFunctionV1,
    args: &[TypedExprNodeV1],
) -> Result<RuntimeScalarTypeV1, ViewPlanError> {
    match function {
        BuiltinScalarFunctionV1::Concat
        | BuiltinScalarFunctionV1::Substring
        | BuiltinScalarFunctionV1::Upper
        | BuiltinScalarFunctionV1::Lower
        | BuiltinScalarFunctionV1::Trim => {
            if !args
                .first()
                .is_some_and(|arg| arg.result_type == RuntimeScalarTypeV1::Utf8)
            {
                return unsupported(format!("{function:?} requires a utf8 first argument"));
            }
            Ok(RuntimeScalarTypeV1::Utf8)
        }
        BuiltinScalarFunctionV1::Length => {
            if !args
                .first()
                .is_some_and(|arg| arg.result_type == RuntimeScalarTypeV1::Utf8)
            {
                return unsupported("LENGTH requires a utf8 argument");
            }
            Ok(RuntimeScalarTypeV1::Int64)
        }
        BuiltinScalarFunctionV1::ExtractYear
        | BuiltinScalarFunctionV1::ExtractMonth
        | BuiltinScalarFunctionV1::ExtractDay
        | BuiltinScalarFunctionV1::ExtractHour
        | BuiltinScalarFunctionV1::ExtractMinute
        | BuiltinScalarFunctionV1::ExtractSecond
        | BuiltinScalarFunctionV1::DateTruncDay
        | BuiltinScalarFunctionV1::DateTruncHour
        | BuiltinScalarFunctionV1::DateTruncMinute
        | BuiltinScalarFunctionV1::DateTruncSecond => {
            // The timestamp expression is either the only argument
            // (`EXTRACT(field FROM ts)` AST form) or the second argument
            // (`extract('field', ts)` function form with a leading literal).
            if !args
                .iter()
                .any(|arg| arg.result_type == RuntimeScalarTypeV1::TimestampNanosecond)
            {
                return unsupported(format!(
                    "{function:?} requires a timestamp_nanosecond argument"
                ));
            }
            match function {
                BuiltinScalarFunctionV1::ExtractYear
                | BuiltinScalarFunctionV1::ExtractMonth
                | BuiltinScalarFunctionV1::ExtractDay
                | BuiltinScalarFunctionV1::ExtractHour
                | BuiltinScalarFunctionV1::ExtractMinute
                | BuiltinScalarFunctionV1::ExtractSecond => Ok(RuntimeScalarTypeV1::Int64),
                _ => Ok(RuntimeScalarTypeV1::TimestampNanosecond),
            }
        }
        BuiltinScalarFunctionV1::TimestampAddNanoseconds
        | BuiltinScalarFunctionV1::TimestampSubtractNanoseconds => {
            if !args
                .first()
                .is_some_and(|arg| arg.result_type == RuntimeScalarTypeV1::TimestampNanosecond)
                || !args
                    .get(1)
                    .is_some_and(|arg| arg.result_type == RuntimeScalarTypeV1::Int64)
            {
                return unsupported(
                    "timestamp arithmetic requires (timestamp_nanosecond, int64 nanoseconds)",
                );
            }
            Ok(RuntimeScalarTypeV1::TimestampNanosecond)
        }
        BuiltinScalarFunctionV1::DateAddDays => {
            if !args
                .first()
                .is_some_and(|arg| arg.result_type == RuntimeScalarTypeV1::Date32)
                || !args
                    .get(1)
                    .is_some_and(|arg| arg.result_type == RuntimeScalarTypeV1::Int64)
            {
                return unsupported("DATE + days requires (date32, int64 days)");
            }
            Ok(RuntimeScalarTypeV1::Date32)
        }
        BuiltinScalarFunctionV1::AbsFloat64
        | BuiltinScalarFunctionV1::CeilFloat64
        | BuiltinScalarFunctionV1::FloorFloat64
        | BuiltinScalarFunctionV1::RoundFloat64 => {
            if !args
                .first()
                .is_some_and(|arg| arg.result_type == RuntimeScalarTypeV1::Float64)
            {
                return unsupported(format!("{function:?} requires a float64 argument"));
            }
            Ok(RuntimeScalarTypeV1::Float64)
        }
        BuiltinScalarFunctionV1::GreatestFloat64 | BuiltinScalarFunctionV1::LeastFloat64 => {
            if !args
                .iter()
                .all(|arg| arg.result_type == RuntimeScalarTypeV1::Float64)
            {
                return unsupported(format!("{function:?} requires float64 arguments"));
            }
            Ok(RuntimeScalarTypeV1::Float64)
        }
        BuiltinScalarFunctionV1::AddFloat64
        | BuiltinScalarFunctionV1::SubtractFloat64
        | BuiltinScalarFunctionV1::MultiplyFloat64
        | BuiltinScalarFunctionV1::DivideFloat64 => {
            if !args.iter().all(|arg| {
                arg.result_type == RuntimeScalarTypeV1::Float64
                    || arg.result_type == RuntimeScalarTypeV1::Int64
            }) {
                return unsupported(format!(
                    "{function:?} requires float64 arguments (int64 is coerced exactly)"
                ));
            }
            Ok(RuntimeScalarTypeV1::Float64)
        }
        BuiltinScalarFunctionV1::AgeDays => {
            if !args.iter().all(|arg| {
                arg.result_type == RuntimeScalarTypeV1::Int64
                    || arg.result_type == RuntimeScalarTypeV1::TimestampNanosecond
            }) {
                return unsupported("AGE_DAYS requires int64 or timestamp_nanosecond arguments");
            }
            Ok(RuntimeScalarTypeV1::Int64)
        }
    }
}

/// Converts a sqlparser interval with a fixed-duration leading field into
/// nanoseconds. Returns `None` for calendar units (month/year) and for
/// non-literal interval values.
fn typed_interval_nanoseconds(interval: &sqlparser::ast::Interval) -> Option<Option<i64>> {
    use sqlparser::ast::DateTimeField;
    let (text, field) = match &*interval.value {
        Expr::Value(ValueWithSpan {
            value: SqlValue::SingleQuotedString(text),
            ..
        }) => {
            // `INTERVAL '1 hour'` carries the unit inside the string when no
            // leading field is present; `INTERVAL '1' HOUR` uses the field.
            let parts = text.split_whitespace().collect::<Vec<_>>();
            let field = match (parts.as_slice(), interval.leading_field.as_ref()) {
                ([number, unit, ..], None) => {
                    let unit: &str = unit;
                    let parsed_unit = match unit.to_ascii_lowercase().as_str() {
                        "nanosecond" | "nanoseconds" => DateTimeField::Nanosecond,
                        "microsecond" | "microseconds" => DateTimeField::Microsecond,
                        "millisecond" | "milliseconds" => DateTimeField::Millisecond,
                        "second" | "seconds" => DateTimeField::Second,
                        "minute" | "minutes" => DateTimeField::Minute,
                        "hour" | "hours" => DateTimeField::Hour,
                        "day" | "days" => DateTimeField::Day,
                        _ => return None,
                    };
                    (*number, parsed_unit)
                }
                ([number, ..], Some(field)) => (*number, field.clone()),
                ([number], None) => (*number, DateTimeField::Second),
                _ => return None,
            };
            let number = field.0.parse::<i64>().ok()?;
            (number, field.1)
        }
        Expr::Value(ValueWithSpan {
            value: SqlValue::Number(text, _),
            ..
        }) => (
            text.parse::<i64>().ok()?,
            interval
                .leading_field
                .as_ref()
                .cloned()
                .unwrap_or(DateTimeField::Second),
        ),
        _ => return None,
    };
    let value = text;
    let multiplier = match &field {
        DateTimeField::Nanosecond => Some(1),
        DateTimeField::Microsecond => Some(1_000),
        DateTimeField::Millisecond => Some(1_000_000),
        DateTimeField::Second | DateTimeField::Seconds => Some(1_000_000_000),
        DateTimeField::Minute | DateTimeField::Minutes => Some(60_000_000_000),
        DateTimeField::Hour | DateTimeField::Hours => Some(3_600_000_000_000),
        DateTimeField::Day | DateTimeField::Days => Some(86_400_000_000_000),
        _ => None,
    }?;
    Some(Some(value.checked_mul(multiplier)?))
}

fn typed_function_with_field(
    name: &str,
    function: BuiltinScalarFunctionV1,
    args: &[TypedExprNodeV1],
) -> Result<BuiltinScalarFunctionV1, ViewPlanError> {
    let name = name.to_ascii_lowercase();
    if name == "extract" {
        let field = match args.first().map(|arg| &arg.kind) {
            Some(TypedExprKindV1::Literal {
                value: ScalarLiteralV1::Utf8 { value: field },
            }) => field.to_ascii_lowercase(),
            _ => {
                return unsupported("EXTRACT requires a string field literal as its first argument")
            }
        };
        return match field.as_str() {
            "year" => Ok(BuiltinScalarFunctionV1::ExtractYear),
            "month" => Ok(BuiltinScalarFunctionV1::ExtractMonth),
            "day" => Ok(BuiltinScalarFunctionV1::ExtractDay),
            "hour" => Ok(BuiltinScalarFunctionV1::ExtractHour),
            "minute" => Ok(BuiltinScalarFunctionV1::ExtractMinute),
            "second" => Ok(BuiltinScalarFunctionV1::ExtractSecond),
            _ => unsupported(format!("EXTRACT field `{field}` is not supported")),
        };
    }
    if name == "date_trunc" {
        let unit = match args.first().map(|arg| &arg.kind) {
            Some(TypedExprKindV1::Literal {
                value: ScalarLiteralV1::Utf8 { value: unit },
            }) => unit.to_ascii_lowercase(),
            _ => {
                return unsupported(
                    "DATE_TRUNC requires a string unit literal as its first argument",
                )
            }
        };
        return match unit.as_str() {
            "day" => Ok(BuiltinScalarFunctionV1::DateTruncDay),
            "hour" => Ok(BuiltinScalarFunctionV1::DateTruncHour),
            "minute" => Ok(BuiltinScalarFunctionV1::DateTruncMinute),
            "second" => Ok(BuiltinScalarFunctionV1::DateTruncSecond),
            _ => unsupported(format!("DATE_TRUNC unit `{unit}` is not supported")),
        };
    }
    Ok(function)
}

/// Phase 7.1: inlines a single aggregate CTE into the outer query.
///
/// Pattern:
/// ```sql
/// WITH x AS (SELECT k, sum(v) AS total FROM t GROUP BY k)
/// SELECT k, total FROM x WHERE <pred>
/// ```
/// The outer query must be a single-relation SELECT over the CTE alias
/// whose projections reference only the CTE group key and aggregate output
/// columns. Outer WHERE conjuncts on the group key merge into the inner
/// WHERE; conjuncts on aggregate outputs merge into the inner HAVING (with
/// the output reference rebuilt as the aggregate call). Anything else fails
/// closed through the existing identity-CTE path. Returns `None` when the
/// pattern does not match (the caller keeps the existing validation).
fn inline_aggregate_cte(
    query: Box<Query>,
    catalog: &VelorixRelationCatalogV1,
) -> Result<Option<Query>, ViewPlanError> {
    let Some(with) = &query.with else {
        return Ok(None);
    };
    let [cte] = with.cte_tables.as_slice() else {
        return Ok(None);
    };
    if !cte.alias.columns.is_empty() || cte.from.is_some() || cte.materialized.is_some() {
        return Ok(None);
    }
    let SetExpr::Select(cte_select) = cte.query.body.as_ref() else {
        return Ok(None);
    };
    let has_group_by = matches!(
        &cte_select.group_by,
        GroupByExpr::Expressions(expressions, modifiers)
            if !expressions.is_empty() && modifiers.is_empty()
    );
    if !has_group_by {
        return Ok(None);
    }
    let SetExpr::Select(outer_select) = &*query.body else {
        return Ok(None);
    };
    if outer_select.distinct.is_some() || outer_select.from.len() != 1 {
        return Ok(None);
    }
    let cte_alias = cte.alias.name.value.as_str();
    let [outer_from] = outer_select.from.as_slice() else {
        return Ok(None);
    };
    let TableFactor::Table { name, alias, .. } = &outer_from.relation else {
        return Ok(None);
    };
    let from_alias = alias.as_ref().map(|alias| alias.name.value.as_str());
    let from_name = name.to_string();
    let from_is_cte = from_alias == Some(cte_alias)
        || (from_alias.is_none() && from_name.eq_ignore_ascii_case(cte_alias));
    if !from_is_cte || !outer_from.joins.is_empty() {
        return Ok(None);
    }

    // The CTE itself must be a supported single-key aggregate query.
    let cte_query = Query {
        with: None,
        body: Box::new(SetExpr::Select((*cte_select).clone())),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };
    let cte_plan = validate_supported_view_sql(&cte_query.to_string(), catalog)?;
    if cte_plan.aggregate_outputs.is_empty() {
        return Ok(None);
    }
    let key_output_name = if cte_plan.output_key_column_id.is_empty() {
        catalog_column_by_id(catalog, &cte_plan.group_key_column_id)
            .ok()
            .map(|column| column.name.clone())
    } else {
        Some(cte_plan.output_key_column_id.clone())
    };
    let Some(key_output_name) = key_output_name else {
        return Ok(None);
    };
    let base_key_name = catalog_primary_key_column(catalog)?.name.clone();
    // CTE output name -> rebuilt inner expression.
    let mut output_exprs: BTreeMap<String, Expr> = BTreeMap::new();
    output_exprs.insert(
        key_output_name.clone(),
        Expr::Identifier(Ident::new(base_key_name.as_str())),
    );
    for aggregate in &cte_plan.aggregate_outputs {
        let Some(call) = rebuilt_aggregate_call(aggregate, catalog)? else {
            return Ok(None);
        };
        output_exprs.insert(aggregate.output_column_id.clone(), call);
    }

    // Outer projection: every item must reference a CTE output column.
    let mut merged_projection = Vec::with_capacity(outer_select.projection.len());
    for item in &outer_select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
            _ => return Ok(None),
        };
        let column_name = cte_output_column_reference(expr, cte_alias, from_alias);
        let Some(column_name) = column_name else {
            return Ok(None);
        };
        let Some(inner) = output_exprs.get(&column_name) else {
            return Ok(None);
        };
        if let Some(alias) = alias {
            if alias != column_name {
                // Renaming an aggregate CTE output is only safe when the
                // name still matches the canonical output id; keep the
                // alias only for the group key.
                if !column_name.eq_ignore_ascii_case(key_output_name.as_str()) {
                    return Ok(None);
                }
            }
            merged_projection.push(SelectItem::ExprWithAlias {
                expr: inner.clone(),
                alias: Ident::new(alias),
            });
        } else {
            merged_projection.push(SelectItem::UnnamedExpr(inner.clone()));
        }
    }

    // Outer WHERE: classify conjuncts by the CTE output columns they touch.
    let mut key_filters = Vec::new();
    let mut having_filters = Vec::new();
    if let Some(selection) = &outer_select.selection {
        for conjunct in split_and_conjuncts(selection) {
            let referenced = cte_output_references(&conjunct, cte_alias, from_alias);
            if referenced.is_empty() {
                return Ok(None);
            }
            let mut uses_key = false;
            let mut uses_aggregate = false;
            for name in &referenced {
                if *name == key_output_name {
                    uses_key = true;
                } else if output_exprs.contains_key(name) {
                    uses_aggregate = true;
                } else {
                    return Ok(None);
                }
            }
            if uses_key && uses_aggregate {
                return Ok(None);
            }
            if uses_key {
                key_filters.push(rewrite_cte_references(
                    conjunct.clone(),
                    cte_alias,
                    from_alias,
                    &output_exprs,
                )?);
            } else {
                having_filters.push(rewrite_cte_references(
                    conjunct.clone(),
                    cte_alias,
                    from_alias,
                    &output_exprs,
                )?);
            }
        }
    }

    let mut merged = (*cte_select).clone();
    merged.projection = merged_projection;
    merged.selection = combine_and_exprs(
        cte_select.selection.as_ref(),
        combine_and_exprs_opt(&key_filters),
    );
    merged.having = combine_and_exprs(
        cte_select.having.as_ref(),
        combine_and_exprs_opt(&having_filters),
    );
    merged.distinct = None;
    let merged_query = Query {
        with: None,
        body: Box::new(SetExpr::Select(merged)),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };
    // The merged query must itself be a supported aggregate query; any
    // mismatch fails closed here rather than in the runtime.
    validate_supported_view_sql(&merged_query.to_string(), catalog)?;
    Ok(Some(merged_query))
}

/// Rebuilds the SQL aggregate call for a validated aggregate output so an
/// outer HAVING/projection reference can be replaced by the inner call.
fn rebuilt_aggregate_call(
    aggregate: &SupportedAggregateOutput,
    catalog: &VelorixRelationCatalogV1,
) -> Result<Option<Expr>, ViewPlanError> {
    if aggregate.input_expression.is_some() {
        return Ok(None);
    }
    let input_name = match &aggregate.input_column_id {
        Some(column_id) => Some(catalog_column_by_id(catalog, column_id)?.name.clone()),
        None => None,
    };
    let function_name = match aggregate.function {
        LogicalPlanAggregateFunctionV1::Sum => "sum",
        LogicalPlanAggregateFunctionV1::Count => "count",
        LogicalPlanAggregateFunctionV1::CountDistinct => "count",
        LogicalPlanAggregateFunctionV1::Min => "min",
        LogicalPlanAggregateFunctionV1::Max => "max",
        LogicalPlanAggregateFunctionV1::PercentileDisc { .. } => "percentile_disc",
        LogicalPlanAggregateFunctionV1::PercentileCont { .. } => "percentile_cont",
        LogicalPlanAggregateFunctionV1::Avg => "avg",
    };
    let mut arguments = FunctionArguments::List(FunctionArgumentList {
        duplicate_treatment: if aggregate.function == LogicalPlanAggregateFunctionV1::CountDistinct
        {
            Some(DuplicateTreatment::Distinct)
        } else {
            None
        },
        args: Vec::new(),
        clauses: Vec::new(),
    });
    match (&aggregate.function, input_name) {
        (LogicalPlanAggregateFunctionV1::Count, None) => {
            if let FunctionArguments::List(list) = &mut arguments {
                list.args
                    .push(FunctionArg::Unnamed(FunctionArgExpr::Wildcard));
            }
        }
        (_, Some(input_name)) => {
            if let FunctionArguments::List(list) = &mut arguments {
                list.args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::Identifier(Ident::new(input_name)),
                )));
            }
        }
        _ => return Ok(None),
    }
    Ok(Some(Expr::Function(Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new(function_name),
        )]),
        args: arguments,
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
        parameters: FunctionArguments::None,
        uses_odbc_syntax: false,
    })))
}

/// Returns the CTE output column name referenced by a simple column
/// expression, if it is a (possibly CTE-qualified) reference.
fn cte_output_column_reference(
    expr: &Expr,
    cte_alias: &str,
    from_alias: Option<&str>,
) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => {
            let [qualifier, name] = parts.as_slice() else {
                return None;
            };
            let qualifier_matches = from_alias
                .map(|alias| identifier_eq(alias, qualifier.value.as_str()))
                .unwrap_or_else(|| identifier_eq(cte_alias, qualifier.value.as_str()));
            qualifier_matches.then(|| name.value.clone())
        }
        _ => None,
    }
}

/// Collects the CTE output column names referenced anywhere in an
/// expression.
fn cte_output_references(
    expr: &Expr,
    cte_alias: &str,
    from_alias: Option<&str>,
) -> BTreeSet<String> {
    let mut references = BTreeSet::new();
    collect_cte_references(expr, cte_alias, from_alias, &mut references);
    references
}

fn collect_cte_references(
    expr: &Expr,
    cte_alias: &str,
    from_alias: Option<&str>,
    references: &mut BTreeSet<String>,
) {
    match expr {
        Expr::Identifier(ident) => {
            references.insert(ident.value.clone());
        }
        Expr::CompoundIdentifier(parts) => {
            if let [qualifier, name] = parts.as_slice() {
                let qualifier_matches = from_alias
                    .map(|alias| identifier_eq(alias, qualifier.value.as_str()))
                    .unwrap_or_else(|| identifier_eq(cte_alias, qualifier.value.as_str()));
                if qualifier_matches {
                    references.insert(name.value.clone());
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_cte_references(left, cte_alias, from_alias, references);
            collect_cte_references(right, cte_alias, from_alias, references);
        }
        Expr::Nested(inner) => collect_cte_references(inner, cte_alias, from_alias, references),
        Expr::UnaryOp { expr, .. } => {
            collect_cte_references(expr, cte_alias, from_alias, references)
        }
        Expr::Value(_) => {}
        _ => {}
    }
}

/// Rewrites CTE output column references to the rebuilt inner expressions.
fn rewrite_cte_references(
    expr: Expr,
    cte_alias: &str,
    from_alias: Option<&str>,
    output_exprs: &BTreeMap<String, Expr>,
) -> Result<Expr, ViewPlanError> {
    match expr {
        Expr::Identifier(ident) => {
            if let Some(inner) = output_exprs.get(&ident.value) {
                Ok(inner.clone())
            } else {
                Ok(Expr::Identifier(ident))
            }
        }
        Expr::CompoundIdentifier(parts) => {
            if let [qualifier, name] = parts.as_slice() {
                let qualifier_matches = from_alias
                    .map(|alias| identifier_eq(alias, qualifier.value.as_str()))
                    .unwrap_or_else(|| identifier_eq(cte_alias, qualifier.value.as_str()));
                if qualifier_matches {
                    if let Some(inner) = output_exprs.get(&name.value) {
                        return Ok(inner.clone());
                    }
                }
            }
            Ok(Expr::CompoundIdentifier(parts))
        }
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(rewrite_cte_references(
                *left,
                cte_alias,
                from_alias,
                output_exprs,
            )?),
            op,
            right: Box::new(rewrite_cte_references(
                *right,
                cte_alias,
                from_alias,
                output_exprs,
            )?),
        }),
        Expr::Nested(inner) => Ok(Expr::Nested(Box::new(rewrite_cte_references(
            *inner,
            cte_alias,
            from_alias,
            output_exprs,
        )?))),
        Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op,
            expr: Box::new(rewrite_cte_references(
                *expr,
                cte_alias,
                from_alias,
                output_exprs,
            )?),
        }),
        other => Ok(other),
    }
}

fn split_and_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            op: BinaryOperator::And,
            left,
            right,
        } => {
            let mut conjuncts = split_and_conjuncts(left);
            conjuncts.extend(split_and_conjuncts(right));
            conjuncts
        }
        other => vec![other.clone()],
    }
}

fn combine_and_exprs_opt(exprs: &[Expr]) -> Option<Expr> {
    match exprs {
        [] => None,
        [single] => Some(single.clone()),
        [head, tail @ ..] => Some(
            tail.iter()
                .fold(head.clone(), |acc, conjunct| Expr::BinaryOp {
                    left: Box::new(acc),
                    op: BinaryOperator::And,
                    right: Box::new(conjunct.clone()),
                }),
        ),
    }
}

fn combine_and_exprs(left: Option<&Expr>, right: Option<Expr>) -> Option<Expr> {
    match (left, right) {
        (None, None) => None,
        (Some(left), None) => Some(left.clone()),
        (None, Some(right)) => Some(right),
        (Some(left), Some(right)) => Some(Expr::BinaryOp {
            left: Box::new(left.clone()),
            op: BinaryOperator::And,
            right: Box::new(right),
        }),
    }
}

/// Builds the decorrelated EXISTS subquery for `WHERE x IN (SELECT y FROM r)`:
/// `EXISTS (SELECT y FROM r WHERE r.y = x)`. Returns `None` for unsupported
/// subquery shapes (multi-column projections, joins, DISTINCT/GROUP BY,
/// inner selections, set operations).
fn decorrelated_in_subquery_select(
    probe: &Expr,
    subquery: &Query,
) -> Result<Option<Box<Query>>, ViewPlanError> {
    validate_query_level_clauses(subquery, false)?;
    let SetExpr::Select(inner_select) = subquery.body.as_ref() else {
        return Ok(None);
    };
    if inner_select.distinct.is_some() || !group_by_is_empty(&inner_select.group_by) {
        return Ok(None);
    }
    let [SelectItem::UnnamedExpr(projected)] = inner_select.projection.as_slice() else {
        return Ok(None);
    };
    let inner_column = match projected {
        Expr::Identifier(inner_column) => inner_column.clone(),
        Expr::CompoundIdentifier(parts) => match parts.last() {
            Some(inner_column) => inner_column.clone(),
            None => return Ok(None),
        },
        _ => return Ok(None),
    };
    let [inner_from] = inner_select.from.as_slice() else {
        return Ok(None);
    };
    if !inner_from.joins.is_empty() || inner_select.selection.is_some() {
        return Ok(None);
    }
    let inner_alias = match &inner_from.relation {
        TableFactor::Table { name, alias, .. } => alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .unwrap_or_else(|| name.to_string()),
        _ => String::new(),
    };
    if inner_alias.is_empty() {
        return Ok(None);
    }
    let inner_ref = Expr::CompoundIdentifier(vec![Ident::new(inner_alias), inner_column.clone()]);
    let equality = Expr::BinaryOp {
        left: Box::new(inner_ref),
        op: BinaryOperator::Eq,
        right: Box::new(probe.clone()),
    };
    let mut synthesized = (*inner_select).clone();
    synthesized.projection = vec![SelectItem::UnnamedExpr(Expr::Identifier(inner_column))];
    synthesized.selection = Some(equality);
    Ok(Some(Box::new(Query {
        with: None,
        body: Box::new(SetExpr::Select(synthesized)),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    })))
}

/// Phase 7.2: validates `WHERE outer_col <op> (SELECT <agg>(col) FROM inner)`
/// for an uncorrelated scalar aggregate subquery over the second relation.
/// MVP scope: exactly one comparison forming the whole outer WHERE, a
/// direct outer column probe, one global aggregate (SUM/COUNT/MIN/MAX/AVG,
/// or COUNT(*)) over the inner relation with no filters, and a
/// filter/project outer projection.
pub fn validate_supported_scalar_aggregate_filter_sql(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<SupportedScalarAggregateFilterPlanV1, ViewPlanError> {
    let [outer_catalog, scalar_catalog] = catalogs else {
        return unsupported("scalar aggregate filter SQL requires exactly two input relations");
    };
    for catalog in catalogs {
        catalog.validate()?;
        let adapter = crate::relation::supported_incremental_adapter_spec(
            &catalog.incremental_adapter.adapter_id,
        )
        .ok_or(RelationSchemaError::InvalidRelationSchema {
            field: "incremental_adapter.adapter_id",
        })?;
        if !matches!(
            adapter,
            SupportedIncrementalAdapterSpec::ScalarSumCount
                | SupportedIncrementalAdapterSpec::Generic
        ) {
            return unsupported("scalar aggregate filter SQL requires scalar or generic inputs");
        }
    }
    let query = parse_single_query(sql)?;
    validate_query_level_clauses(&query, false)?;
    let SetExpr::Select(select) = query.body.as_ref() else {
        return unsupported("scalar aggregate filter requires a single SELECT");
    };
    validate_plain_select_clauses(select)?;
    if select.distinct.is_some() || !group_by_is_empty(&select.group_by) || select.having.is_some()
    {
        return unsupported("scalar aggregate filter does not support DISTINCT or GROUP BY");
    }
    let [outer_from] = select.from.as_slice() else {
        return unsupported("scalar aggregate filter outer query requires one relation");
    };
    if !outer_from.joins.is_empty() {
        return unsupported("scalar aggregate filter outer query must not contain a JOIN");
    }
    let outer_table = registered_table_ref(&outer_from.relation, "outer")?;
    if outer_table.name != outer_catalog.relation_schema.relation_id {
        return unsupported(
            "scalar aggregate filter outer query must reference the first registered relation",
        );
    }
    let outer_alias = Some(outer_table.alias.as_str());
    let Some(Expr::BinaryOp { left, op, right }) = select.selection.as_ref() else {
        return unsupported(
            "scalar aggregate filter requires one comparison predicate over a scalar subquery",
        );
    };
    let comparison_op = match op {
        BinaryOperator::Eq => ScalarSubqueryComparisonOp::Eq,
        BinaryOperator::NotEq => ScalarSubqueryComparisonOp::NotEq,
        BinaryOperator::Gt => ScalarSubqueryComparisonOp::Gt,
        BinaryOperator::GtEq => ScalarSubqueryComparisonOp::GtEq,
        BinaryOperator::Lt => ScalarSubqueryComparisonOp::Lt,
        BinaryOperator::LtEq => ScalarSubqueryComparisonOp::LtEq,
        _ => return unsupported("scalar aggregate filter comparison operator is not supported"),
    };
    let outer_ref = qualified_column_ref(left)?;
    let outer_column = qualified_ref_catalog_column(&outer_ref, outer_catalog)?;
    if outer_column.column_id == outer_catalog.relation_schema.weight_column_id {
        return unsupported("scalar aggregate filter must not compare the weight column");
    }
    let Expr::Subquery(scalar_subquery) = right.as_ref() else {
        return unsupported("scalar aggregate filter requires a scalar subquery on the right side");
    };
    validate_query_level_clauses(scalar_subquery, false)?;
    let SetExpr::Select(inner_select) = scalar_subquery.body.as_ref() else {
        return unsupported("scalar subquery requires a single SELECT");
    };
    validate_plain_select_clauses(inner_select)?;
    if inner_select.distinct.is_some()
        || !group_by_is_empty(&inner_select.group_by)
        || inner_select.having.is_some()
        || inner_select.selection.is_some()
    {
        return unsupported(
            "scalar subquery does not support DISTINCT, GROUP BY, HAVING, WHERE, or LIMIT",
        );
    }
    let [inner_from] = inner_select.from.as_slice() else {
        return unsupported("scalar subquery requires one relation");
    };
    let inner_table = registered_table_ref(&inner_from.relation, "inner")?;
    if inner_table.name != scalar_catalog.relation_schema.relation_id {
        return unsupported("scalar subquery must reference the second registered relation");
    }
    let inner_alias = Some(inner_table.alias.as_str());
    let [SelectItem::UnnamedExpr(Expr::Function(Function { name, args, .. }))] =
        inner_select.projection.as_slice()
    else {
        return unsupported("scalar subquery must project exactly one aggregate call");
    };
    let function_name = name.to_string();
    let scalar_aggregate = scalar_subquery_aggregate_output(
        function_name.as_str(),
        args,
        scalar_catalog,
        inner_alias,
    )?;
    let key_column = catalog_primary_key_column(outer_catalog)?;
    let projection = validate_filter_project_projection(
        &select.clone(),
        outer_catalog,
        key_column,
        outer_alias,
        None,
    )?;
    if projection.value_columns.is_empty() {
        return unsupported(
            "scalar aggregate filter materialized output requires at least one value column",
        );
    }
    Ok(SupportedScalarAggregateFilterPlanV1 {
        schema_version: 1,
        outer_input_relation_id: outer_catalog.relation_schema.relation_id.clone(),
        scalar_input_relation_id: scalar_catalog.relation_schema.relation_id.clone(),
        outer_key_column_id: key_column.column_id.clone(),
        scalar_aggregate,
        outer_comparison_column_id: outer_column.column_id.clone(),
        comparison_op,
        projection: SupportedFilterProjectPlan {
            typed_value_columns: Vec::new(),
            input_relation_id: outer_catalog.relation_schema.relation_id.clone(),
            key_column_id: key_column.column_id.clone(),
            output_key_column_id: projection.output_key_column_id,
            output_key_input_column_id: projection.output_key_input_column_id,
            value_columns: projection
                .value_columns
                .into_iter()
                .map(|column| SupportedProjectionColumn {
                    input_column_id: column.input_column_id,
                    output_column_id: column.output_column_id,
                    expression: column.expression,
                })
                .collect(),
            predicate_expr: None,
            top_k: None,
        },
        resource_contract: ScalarAggregateResourceContractV1 {
            max_outer_rows: 1_000_000,
            max_recomputed_rows_per_epoch: 1_000_000,
            max_output_delta_rows: 1_000_000,
        },
    })
}

fn scalar_subquery_aggregate_output(
    function_name: &str,
    args: &FunctionArguments,
    catalog: &VelorixRelationCatalogV1,
    _inner_alias: Option<&str>,
) -> Result<SupportedAggregateOutput, ViewPlanError> {
    let FunctionArguments::List(argument_list) = args else {
        return unsupported("scalar subquery aggregate requires a normal argument list");
    };
    let aggregate = match function_name.to_ascii_lowercase().as_str() {
        "count" if argument_list.args.is_empty() => SupportedAggregateOutput {
            function: LogicalPlanAggregateFunctionV1::Count,
            input_column_id: None,
            input_relation_side: None,
            input_expression: None,
            output_column_id: "count".to_string(),
        },
        "count" => {
            let [FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))] = argument_list.args.as_slice()
            else {
                return unsupported("COUNT requires one column argument");
            };
            let inner_ref = qualified_column_ref(expr)?;
            let column = qualified_ref_catalog_column(&inner_ref, catalog)?;
            SupportedAggregateOutput {
                function: LogicalPlanAggregateFunctionV1::Count,
                input_column_id: Some(column.column_id.clone()),
                input_relation_side: None,
                input_expression: None,
                output_column_id: "count".to_string(),
            }
        }
        name @ ("sum" | "min" | "max" | "avg") => {
            let [FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))] = argument_list.args.as_slice()
            else {
                return unsupported(format!("{name} requires one column argument"));
            };
            let inner_ref = qualified_column_ref(expr)?;
            let column = qualified_ref_catalog_column(&inner_ref, catalog)?;
            let function = match name {
                "sum" => LogicalPlanAggregateFunctionV1::Sum,
                "min" => LogicalPlanAggregateFunctionV1::Min,
                "max" => LogicalPlanAggregateFunctionV1::Max,
                "avg" => LogicalPlanAggregateFunctionV1::Avg,
                _ => unreachable!(),
            };
            match function {
                LogicalPlanAggregateFunctionV1::Sum | LogicalPlanAggregateFunctionV1::Avg => {
                    validate_numeric_sum_column(catalog, column)?;
                }
                _ => {}
            }
            SupportedAggregateOutput {
                function,
                input_column_id: Some(column.column_id.clone()),
                input_relation_side: None,
                input_expression: None,
                output_column_id: name.to_string(),
            }
        }
        other => {
            return unsupported(format!(
                "scalar subquery aggregate `{other}` is not supported"
            ))
        }
    };
    Ok(aggregate)
}

pub fn lower_supported_scalar_aggregate_filter_sql_to_logical_plan(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_scalar_aggregate_filter_sql(sql, catalogs)?;
    finalize_logical_plan(scalar_aggregate_filter_logical_plan(
        sql,
        catalogs,
        output_schema,
        supported,
    )?)
}

fn scalar_aggregate_filter_logical_plan(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
    supported: SupportedScalarAggregateFilterPlanV1,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let outer_catalog =
        catalog_for_relation_in_slice(catalogs, &supported.outer_input_relation_id)?;
    let output_relation = logical_relation_from_schema(output_schema);
    let outer_scan = logical_relation_from_catalog(outer_catalog);
    let outer_key = column_ref(
        &supported.outer_input_relation_id,
        &supported.outer_key_column_id,
    );
    let outer_comparison = column_ref(
        &supported.outer_input_relation_id,
        &supported.outer_comparison_column_id,
    );
    let filter_node_id = "scalar_aggregate_filter".to_string();
    let project_node_id = "scalar_aggregate_project".to_string();
    let nodes = vec![
        VelorixLogicalViewPlanNodeV1::RelationScan {
            node_id: "outer_relation_scan".to_string(),
            relation: outer_scan,
        },
        VelorixLogicalViewPlanNodeV1::Filter {
            node_id: filter_node_id.clone(),
            input: "outer_relation_scan".to_string(),
            predicate: LogicalPlanPredicateV1 {
                column: outer_comparison,
                op: PredicateOp::Gt,
                literal: JsonValue::Null,
            },
        },
        VelorixLogicalViewPlanNodeV1::Project {
            node_id: project_node_id.clone(),
            input: filter_node_id,
            columns: vec![
                outer_key,
                column_ref(&supported.outer_input_relation_id, "score"),
            ],
            computed_columns: Vec::new(),
        },
        VelorixLogicalViewPlanNodeV1::Output {
            node_id: "output_materialized_view".to_string(),
            input: project_node_id,
            relation: output_relation.clone(),
        },
    ];
    let mut plan = VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations: catalogs.iter().map(logical_relation_from_catalog).collect(),
        output_relation,
        nodes,
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: Vec::new(),
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::ScalarAggregateFilter {
            plan: Box::new(supported),
        },
    };
    plan.execution_implementation = Some(derive_execution_implementation(&plan)?);
    Ok(plan)
}

fn catalog_for_relation_in_slice<'a>(
    catalogs: &'a [VelorixRelationCatalogV1],
    relation_id: &str,
) -> Result<&'a VelorixRelationCatalogV1, ViewPlanError> {
    catalogs
        .iter()
        .find(|catalog| catalog.relation_schema.relation_id == relation_id)
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "scalar aggregate filter input catalog is missing".to_string(),
        })
}

/// Phase 8.1: validates bounded ROWS window frames with navigation
/// functions: `LAG/LEAD(expr, k)`, `FIRST_VALUE(expr)`, `LAST_VALUE(expr)`,
/// `NTH_VALUE(expr, n)` OVER (PARTITION BY col ORDER BY col ROWS BETWEEN
/// k PRECEDING AND k FOLLOWING). One partition column, one non-null
/// sortable order column, constant bounded frame; RANGE/GROUPS/UNBOUNDED/
/// EXCLUDE/named windows fail closed.
pub fn validate_supported_analytic_window_frame_sql(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedAnalyticWindowFramePlanV1, ViewPlanError> {
    catalog.validate()?;
    let adapter = crate::relation::supported_incremental_adapter_spec(
        &catalog.incremental_adapter.adapter_id,
    )
    .ok_or(RelationSchemaError::InvalidRelationSchema {
        field: "incremental_adapter.adapter_id",
    })?;
    if !matches!(
        adapter,
        SupportedIncrementalAdapterSpec::ScalarSumCount | SupportedIncrementalAdapterSpec::Generic
    ) {
        return unsupported("window frame SQL currently supports scalar or generic inputs");
    }
    let key_column = catalog_primary_key_column(catalog)?;
    let query = parse_single_query(sql)?;
    validate_query_level_clauses(&query, false)?;
    let SetExpr::Select(select) = query.body.as_ref() else {
        return unsupported("window frame requires a single SELECT");
    };
    validate_plain_select_clauses_allow_qualify(select)?;
    if select.distinct.is_some() || !group_by_is_empty(&select.group_by) || select.having.is_some()
    {
        return unsupported("window frame does not support DISTINCT, GROUP BY, or HAVING");
    }
    let [from] = select.from.as_slice() else {
        return unsupported("window frame requires one relation");
    };
    if !from.joins.is_empty() {
        return unsupported("window frame must not contain a JOIN");
    }
    let table = registered_table_ref(&from.relation, "frame")?;
    if table.name != catalog.relation_schema.relation_id {
        return unsupported("window frame must reference the registered relation");
    }
    let relation_alias = Some(table.alias.as_str());
    let [key_item, frame_item] = select.projection.as_slice() else {
        return unsupported("window frame requires the primary key and exactly one window column");
    };
    let (output_key_column_id, output_key_input_column_id) =
        if select_item_references_bound_column(key_item, catalog, key_column, relation_alias, None)
        {
            (
                select_item_alias_or_source_default(
                    key_item,
                    key_column.name.as_str(),
                    relation_alias,
                    None,
                )?,
                None,
            )
        } else {
            return unsupported("first window frame projection must be the primary key column");
        };
    let (window_expr, output_column_id) = match frame_item {
        SelectItem::UnnamedExpr(expr) => (expr, None),
        SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
        _ => return unsupported("window frame projection must be a window function"),
    };
    let Expr::Function(function) = window_expr else {
        return unsupported("window frame projection must be a window function");
    };
    let function_name = function.name.to_string();
    let Some(WindowType::WindowSpec(window)) = function.over.as_ref() else {
        return unsupported("window frame requires an inline OVER window specification");
    };
    if window.window_name.is_some() {
        return unsupported("named windows are not supported");
    }
    let [partition_expr] = window.partition_by.as_slice() else {
        return unsupported("window frame requires exactly one PARTITION BY column");
    };
    let Some(partition_column) = expression_catalog_column(partition_expr, catalog, relation_alias)
    else {
        return unsupported("PARTITION BY must reference a registered relation column");
    };
    validate_row_number_partition_column(catalog, partition_column)?;
    let [order] = window.order_by.as_slice() else {
        return unsupported("window frame requires exactly one ORDER BY column");
    };
    let Some(order_column) = expression_catalog_column(&order.expr, catalog, relation_alias) else {
        return unsupported("ORDER BY must reference a registered relation column");
    };
    if order_column.nullable {
        return unsupported("window frame ORDER BY column must be non-nullable");
    }
    validate_latest_ordering_column(catalog, order_column)?;
    if order.with_fill.is_some() || order.options.nulls_first.is_some() {
        return unsupported("window frame ORDER BY NULLS/WITH FILL is not supported");
    }
    let (frame_preceding, frame_following) = match &window.window_frame {
        None => (0, 0),
        Some(frame) => {
            if frame.units != sqlparser::ast::WindowFrameUnits::Rows {
                return unsupported("window frame supports only ROWS units");
            }
            let bound = |bound: &sqlparser::ast::WindowFrameBound| -> Result<u64, ViewPlanError> {
                match bound {
                    sqlparser::ast::WindowFrameBound::Preceding(Some(expr)) => {
                        const_frame_bound(expr, "PRECEDING")
                    }
                    sqlparser::ast::WindowFrameBound::Following(Some(expr)) => {
                        const_frame_bound(expr, "FOLLOWING")
                    }
                    sqlparser::ast::WindowFrameBound::CurrentRow => Ok(0),
                    _ => unsupported("window frame bounds must be bounded constants"),
                }
            };
            let end = frame
                .end_bound
                .as_ref()
                .map(bound)
                .transpose()?
                .unwrap_or(0);
            (bound(&frame.start_bound)?, end)
        }
    };
    let FunctionArguments::List(argument_list) = &function.args else {
        return unsupported("window frame function requires a normal argument list");
    };
    let function_spec = parse_window_navigation_function(
        function_name.as_str(),
        &argument_list.args,
        catalog,
        relation_alias,
    )?;
    let output_column_id = output_column_id.unwrap_or(function_name.as_str());
    Ok(SupportedAnalyticWindowFramePlanV1 {
        schema_version: 1,
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        key_column_id: key_column.column_id.clone(),
        output_key_column_id: output_key_column_id.clone(),
        output_key_input_column_id,
        partition_column_id: partition_column.column_id.clone(),
        order_column_id: order_column.column_id.clone(),
        order_descending: order.options.asc == Some(false),
        frame_preceding,
        frame_following,
        function: function_spec,
        output_column_id: output_column_id.to_string(),
    })
}

fn const_frame_bound(expr: &Expr, label: &str) -> Result<u64, ViewPlanError> {
    let Expr::Value(ValueWithSpan {
        value: SqlValue::Number(text, _),
        ..
    }) = expr
    else {
        return unsupported(format!("window frame {label} bound must be a constant"));
    };
    text.parse::<u64>()
        .map_err(|_| ViewPlanError::UnsupportedShape {
            reason: format!("window frame {label} bound must be a non-negative constant"),
        })
}

fn parse_window_navigation_function(
    name: &str,
    args: &[FunctionArg],
    catalog: &VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
) -> Result<WindowNavigationFunctionV1, ViewPlanError> {
    fn column_arg(arg: &FunctionArg) -> Result<&Expr, ViewPlanError> {
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
            return unsupported("window frame value argument must be an expression");
        };
        Ok(expr)
    }
    let const_arg = |arg: &FunctionArg| -> Result<u64, ViewPlanError> {
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
            return unsupported("window frame offset argument must be a constant");
        };
        const_frame_bound(expr, "offset")
    };
    match name.to_ascii_lowercase().as_str() {
        "lag" | "lead" => {
            let [value_arg, offset_arg] = args else {
                return unsupported(format!("{name} requires (value_column, constant_offset)"));
            };
            let column = catalog_column_from_expr(column_arg(value_arg)?, catalog, relation_alias)?;
            let offset = const_arg(offset_arg)?;
            if offset == 0 {
                return unsupported(format!("{name} offset must be positive"));
            }
            Ok(if name.eq_ignore_ascii_case("lag") {
                WindowNavigationFunctionV1::Lag {
                    value_column_id: column.column_id.clone(),
                    offset,
                }
            } else {
                WindowNavigationFunctionV1::Lead {
                    value_column_id: column.column_id.clone(),
                    offset,
                }
            })
        }
        "first_value" | "last_value" => {
            let [value_arg] = args else {
                return unsupported(format!("{name} requires exactly one value column"));
            };
            let column = catalog_column_from_expr(column_arg(value_arg)?, catalog, relation_alias)?;
            Ok(if name.eq_ignore_ascii_case("first_value") {
                WindowNavigationFunctionV1::FirstValue {
                    value_column_id: column.column_id.clone(),
                }
            } else {
                WindowNavigationFunctionV1::LastValue {
                    value_column_id: column.column_id.clone(),
                }
            })
        }
        "nth_value" => {
            let [value_arg, n_arg] = args else {
                return unsupported("nth_value requires (value_column, constant_n)");
            };
            let column = catalog_column_from_expr(column_arg(value_arg)?, catalog, relation_alias)?;
            let n = const_arg(n_arg)?;
            if n == 0 {
                return unsupported("nth_value n must be positive");
            }
            Ok(WindowNavigationFunctionV1::NthValue {
                value_column_id: column.column_id.clone(),
                n,
            })
        }
        other => unsupported(format!(
            "window navigation function `{other}` is not supported"
        )),
    }
}

fn catalog_column_from_expr<'a>(
    expr: &Expr,
    catalog: &'a VelorixRelationCatalogV1,
    relation_alias: Option<&str>,
) -> Result<&'a RelationColumnV1, ViewPlanError> {
    expression_catalog_column(expr, catalog, relation_alias).ok_or_else(|| {
        ViewPlanError::UnsupportedShape {
            reason: "window frame value argument must reference a registered relation column"
                .to_string(),
        }
    })
}

pub fn lower_supported_analytic_window_frame_sql_to_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_analytic_window_frame_sql(sql, catalog)?;
    let input_relation = logical_relation_from_catalog(catalog);
    let output_relation = logical_relation_from_schema(output_schema);
    let partition = column_ref(&supported.input_relation_id, &supported.partition_column_id);
    let order = column_ref(&supported.input_relation_id, &supported.order_column_id);
    finalize_logical_plan(VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations: vec![input_relation],
        output_relation: output_relation.clone(),
        nodes: vec![
            VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: "frame_scan".to_string(),
                relation: logical_relation_from_catalog(catalog),
            },
            VelorixLogicalViewPlanNodeV1::RowNumber {
                node_id: "frame_partition".to_string(),
                input: "frame_scan".to_string(),
                partition_column: partition,
                order_column: order,
                descending: supported.order_descending,
                rank_limit: None,
            },
            VelorixLogicalViewPlanNodeV1::Output {
                node_id: "output_materialized_view".to_string(),
                input: "frame_partition".to_string(),
                relation: output_relation,
            },
        ],
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: Vec::new(),
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::AnalyticWindowFrames {
            plan: Box::new(supported),
        },
    })
}

/// Phase 8.3: validates the exact interval overlap inner join
/// `left.start < right.end AND right.start < left.end` over two relations
/// with non-null timestamp endpoints. Admission requires: INNER JOIN only,
/// exactly the two overlap conjuncts, `start < end` per side, a maximum
/// interval duration, and a filter/project outer projection.
pub fn validate_supported_interval_join_sql(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<SupportedIntervalJoinPlanV1, ViewPlanError> {
    let [left_catalog, right_catalog] = catalogs else {
        return unsupported("interval join SQL requires exactly two input relations");
    };
    for catalog in catalogs {
        catalog.validate()?;
        let adapter = crate::relation::supported_incremental_adapter_spec(
            &catalog.incremental_adapter.adapter_id,
        )
        .ok_or(RelationSchemaError::InvalidRelationSchema {
            field: "incremental_adapter.adapter_id",
        })?;
        if !matches!(adapter, SupportedIncrementalAdapterSpec::Generic) {
            return unsupported("interval join SQL requires generic (+-1 weight) inputs; scalar sum/count adapter weights are not supported yet");
        }
    }
    let query = parse_single_query(sql)?;
    validate_query_level_clauses(&query, false)?;
    let SetExpr::Select(select) = query.body.as_ref() else {
        return unsupported("interval join requires a single SELECT");
    };
    validate_plain_select_clauses(select)?;
    if select.distinct.is_some() || !group_by_is_empty(&select.group_by) || select.having.is_some()
    {
        return unsupported("interval join does not support DISTINCT, GROUP BY, or HAVING");
    }
    let [left_from] = select.from.as_slice() else {
        return unsupported("interval join requires exactly one FROM join");
    };
    let left_table = registered_table_ref(&left_from.relation, "left")?;
    if left_table.name != left_catalog.relation_schema.relation_id {
        return unsupported("interval join left relation must be the first registered relation");
    }
    let [right_join] = left_from.joins.as_slice() else {
        return unsupported("interval join requires exactly one JOIN");
    };
    if !matches!(
        right_join.join_operator,
        JoinOperator::Inner(_) | JoinOperator::Join(_)
    ) {
        return unsupported("interval join currently supports INNER JOIN only");
    }
    let right_table = registered_table_ref(&right_join.relation, "right")?;
    if right_table.name != right_catalog.relation_schema.relation_id {
        return unsupported("interval join right relation must be the second registered relation");
    }
    let constraint = match &right_join.join_operator {
        JoinOperator::Inner(constraint) | JoinOperator::Join(constraint) => constraint,
        _ => return unsupported("interval join requires an INNER JOIN"),
    };
    let JoinConstraint::On(on_expr) = constraint else {
        return unsupported("interval join requires an ON predicate");
    };
    let on_expr: &Expr = on_expr;
    let conjuncts = split_and_conjuncts(on_expr);
    let mut left_start = None;
    let mut left_end = None;
    let mut right_start = None;
    let mut right_end = None;
    for conjunct in conjuncts {
        let Expr::BinaryOp { left, op, right } = conjunct else {
            return unsupported("interval join ON predicates must be strict overlap comparisons");
        };
        if op != BinaryOperator::Lt {
            return unsupported("interval join requires `start < end` overlap comparisons");
        }
        let left_ref = qualified_column_ref(left.as_ref())?;
        let right_ref = qualified_column_ref(right.as_ref())?;
        let (left_side, right_side) = orient_join_refs(
            left_ref,
            right_ref,
            left_table.alias.as_str(),
            right_table.alias.as_str(),
        )?;
        let left_column = qualified_ref_catalog_column(&left_side, left_catalog)?;
        let right_column = qualified_ref_catalog_column(&right_side, right_catalog)?;
        if left_column.column_id == right_catalog.relation_schema.weight_column_id
            || right_column.column_id == left_catalog.relation_schema.weight_column_id
        {
            return unsupported("interval join must not reference weight columns");
        }
        // left.start < right.end and right.start < left.end
        if left_column.column_id.ends_with("_start") || right_column.column_id.ends_with("_end") {
            left_start = Some(left_column.column_id.clone());
            right_end = Some(right_column.column_id.clone());
        } else {
            right_start = Some(right_column.column_id.clone());
            left_end = Some(left_column.column_id.clone());
        }
    }
    let (Some(left_start), Some(left_end), Some(right_start), Some(right_end)) =
        (left_start, left_end, right_start, right_end)
    else {
        return unsupported(
            "interval join requires exactly left.start < right.end and right.start < left.end",
        );
    };
    for column_id in [&left_start, &left_end, &right_start, &right_end] {
        let catalog = if *column_id == left_start || *column_id == left_end {
            left_catalog
        } else {
            right_catalog
        };
        let column = catalog_column_by_id(catalog, column_id)?;
        if column.nullable {
            return unsupported("interval join endpoint columns must be non-null");
        }
        if !matches!(
            column.physical_arrow_type,
            ArrowPhysicalTypeV1::Int64 | ArrowPhysicalTypeV1::TimestampNanosecond { .. }
        ) {
            return unsupported(
                "interval join endpoint columns must be Int64 nanoseconds or TimestampNanosecond",
            );
        }
    }
    // `start < end` per side is enforced at runtime per row; the maximum
    // interval duration is a static admission contract for eviction proofs.
    let max_interval_duration_ns = i64::MAX / 4;
    let left_key = catalog_primary_key_column(left_catalog)?;
    let right_key = catalog_primary_key_column(right_catalog)?;
    let projection = validate_filter_project_projection(
        select,
        left_catalog,
        left_key,
        Some(left_table.alias.as_str()),
        None,
    )?;
    if !projection.typed_value_columns.is_empty() {
        return unsupported("interval join output must not use typed projections");
    }
    if projection.value_columns.is_empty() {
        return unsupported("interval join output requires at least one value column");
    }
    let mut output_columns = Vec::new();
    output_columns.push(IntervalJoinOutputColumnV1 {
        left_column_id: left_key.column_id.clone(),
        output_name: projection.output_key_column_id.clone(),
    });
    for column in &projection.value_columns {
        output_columns.push(IntervalJoinOutputColumnV1 {
            left_column_id: column.input_column_id.clone(),
            output_name: column.output_column_id.clone(),
        });
    }
    Ok(SupportedIntervalJoinPlanV1 {
        schema_version: 1,
        left_input_relation_id: left_catalog.relation_schema.relation_id.clone(),
        right_input_relation_id: right_catalog.relation_schema.relation_id.clone(),
        left_key_column_id: left_key.column_id.clone(),
        right_key_column_id: right_key.column_id.clone(),
        left_start_column_id: left_start,
        left_end_column_id: left_end,
        right_start_column_id: right_start,
        right_end_column_id: right_end,
        max_interval_duration_ns,
        output_columns,
        right_key_output_name: right_key.name.clone(),
        resource_contract: IntervalJoinResourceContractV1 {
            max_intervals_per_side: 1_000_000,
            max_matches_per_epoch: 1_000_000,
        },
    })
}

pub fn lower_supported_interval_join_sql_to_logical_plan(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_interval_join_sql(sql, catalogs)?;
    let output_relation = logical_relation_from_schema(output_schema);
    let left_catalog = catalog_for_relation_in_slice(catalogs, &supported.left_input_relation_id)?;
    let right_catalog =
        catalog_for_relation_in_slice(catalogs, &supported.right_input_relation_id)?;
    finalize_logical_plan(VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations: catalogs.iter().map(logical_relation_from_catalog).collect(),
        output_relation: output_relation.clone(),
        nodes: vec![
            VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: "interval_left_scan".to_string(),
                relation: logical_relation_from_catalog(left_catalog),
            },
            VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: "interval_right_scan".to_string(),
                relation: logical_relation_from_catalog(right_catalog),
            },
            VelorixLogicalViewPlanNodeV1::InnerEquiJoin {
                node_id: "interval_join".to_string(),
                left: "interval_left_scan".to_string(),
                right: "interval_right_scan".to_string(),
                left_key: column_ref(
                    &supported.left_input_relation_id,
                    &supported.left_key_column_id,
                ),
                right_key: column_ref(
                    &supported.right_input_relation_id,
                    &supported.right_key_column_id,
                ),
                composite_equality: None,
            },
            VelorixLogicalViewPlanNodeV1::Output {
                node_id: "output_materialized_view".to_string(),
                input: "interval_join".to_string(),
                relation: output_relation,
            },
        ],
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: Vec::new(),
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::IntervalJoin {
            plan: Box::new(supported),
        },
    })
}

/// Phase 8.5: recursive CTE plan. The derived fixpoint is a set (UNION
/// DISTINCT) over the anchor unioned with the positive recursive term;
/// every epoch recomputes the closure from the updated base multiset and
/// diffs against the previous derived set (exact retractions).
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedRecursiveFixpointPlanV1 {
    pub schema_version: u32,
    pub input_relation_id: String,
    /// Recursion relation column names, positional (anchor order). These
    /// are also the composite materialized output key columns.
    pub recursion_column_names: Vec<String>,
    /// Anchor projection: base column ids, positional.
    pub anchor_projection: Vec<String>,
    /// Recursive term projection: source per position.
    pub recursive_projection: Vec<RecursiveProjectionItemV1>,
    /// The single equi-join between the recursion relation and the base.
    pub recursive_join: RecursiveEquiJoinV1,
    /// Conjunctive base-row predicates in the recursive term (AND-combined).
    #[serde(default)]
    pub recursive_base_predicate: Vec<RecursiveBasePredicateV1>,
    /// Conjunctive base-row predicates in the anchor (AND-combined).
    #[serde(default)]
    pub anchor_base_predicate: Vec<RecursiveBasePredicateV1>,
    pub resource_contract: RecursiveFixpointContractV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecursiveProjectionItemV1 {
    Recursive { column_id: String },
    Base { column_id: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecursiveEquiJoinV1 {
    pub recursive_column_id: String,
    pub base_column_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecursiveBasePredicateV1 {
    pub base_column_id: String,
    pub op: PredicateOp,
    pub literal: JsonValue,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecursiveFixpointContractV1 {
    pub max_iterations: u64,
    pub max_derived_rows: u64,
    pub max_work_units_per_epoch: u64,
}

/// Phase 8.5: validates `WITH RECURSIVE r AS (anchor UNION DISTINCT term)
/// SELECT ... FROM r ...`. Admission requires exactly one self-reference, a
/// positive anchor and recursive term (direct column projections, one
/// equi-join between r and the registered base relation, optional
/// conjunctive base-column predicates), and no aggregation, windows,
/// negation, outer joins, or UNION ALL.
pub fn validate_supported_recursive_cte_sql(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedRecursiveFixpointPlanV1, ViewPlanError> {
    catalog.validate()?;
    let adapter = crate::relation::supported_incremental_adapter_spec(
        &catalog.incremental_adapter.adapter_id,
    )
    .ok_or(RelationSchemaError::InvalidRelationSchema {
        field: "incremental_adapter.adapter_id",
    })?;
    if !matches!(adapter, SupportedIncrementalAdapterSpec::Generic) {
        return unsupported("recursive CTE SQL requires generic (+-1 weight) inputs; scalar sum/count adapter weights are not supported yet");
    }
    let query = parse_single_query(sql)?;
    validate_query_level_clauses_with_options(&query, true, false)?;
    let Some(with) = &query.with else {
        return unsupported("recursive CTE requires WITH RECURSIVE");
    };
    if !with.recursive {
        return unsupported("recursive CTE requires WITH RECURSIVE");
    }
    let [cte] = with.cte_tables.as_slice() else {
        return unsupported("recursive CTE requires exactly one CTE");
    };
    if cte.from.is_some() || cte.materialized.is_some() {
        return unsupported("recursive CTE must not declare a FROM or materialization hint");
    }
    if !cte.alias.columns.is_empty() {
        return unsupported("recursive CTE column aliases are not supported");
    }
    let recursion_alias = cte.alias.name.value.clone();
    validate_query_level_clauses_with_options(&cte.query, false, false)?;
    let SetExpr::SetOperation {
        op: SetOperator::Union,
        set_quantifier,
        left,
        right,
    } = cte.query.body.as_ref()
    else {
        return unsupported("recursive CTE body must be anchor UNION DISTINCT recursive term");
    };
    if !matches!(
        set_quantifier,
        SetQuantifier::Distinct | SetQuantifier::None
    ) {
        return unsupported("recursive CTE requires UNION DISTINCT (UNION ALL is not supported)");
    }
    let SetExpr::Select(anchor_select) = left.as_ref() else {
        return unsupported("recursive CTE anchor must be a plain SELECT");
    };
    let SetExpr::Select(recursive_select) = right.as_ref() else {
        return unsupported("recursive CTE recursive term must be a plain SELECT");
    };
    validate_plain_select_clauses(anchor_select)?;
    validate_plain_select_clauses(recursive_select)?;
    if anchor_select.distinct.is_some() || !group_by_is_empty(&anchor_select.group_by) {
        return unsupported("recursive CTE anchor must not use DISTINCT or GROUP BY");
    }
    if recursive_select.distinct.is_some() || !group_by_is_empty(&recursive_select.group_by) {
        return unsupported("recursive CTE recursive term must not use DISTINCT or GROUP BY");
    }

    // Anchor: single registered base relation, direct column projections.
    let [anchor_from] = anchor_select.from.as_slice() else {
        return unsupported("recursive CTE anchor requires exactly one FROM table");
    };
    if !anchor_from.joins.is_empty() {
        return unsupported("recursive CTE anchor must not contain joins");
    }
    let anchor_table = registered_table_ref(&anchor_from.relation, "anchor")?;
    if !identifier_eq(
        anchor_table.name.as_str(),
        catalog.relation_schema.relation_id.as_str(),
    ) {
        return unsupported("recursive CTE anchor must reference the registered relation");
    }
    let anchor_alias = anchor_table.alias.as_str();
    let mut recursion_column_names = Vec::new();
    let mut anchor_projection = Vec::new();
    for item in &anchor_select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
            _ => return unsupported("recursive CTE anchor projections must be direct columns"),
        };
        let Some(column) =
            expression_filter_project_column(expr, catalog, Some(anchor_alias), None)
        else {
            return unsupported(
                "recursive CTE anchor projections must be direct base relation columns",
            );
        };
        if column.column_id == catalog.relation_schema.weight_column_id {
            return unsupported(
                "recursive CTE anchor projections must not reference the weight column",
            );
        }
        let name = alias.unwrap_or(column.name.as_str()).to_string();
        if recursion_column_names.contains(&name) {
            return unsupported("recursive CTE column names must be unique");
        }
        recursion_column_names.push(name);
        anchor_projection.push(column.column_id.clone());
    }
    if recursion_column_names.is_empty() {
        return unsupported("recursive CTE requires at least one projected column");
    }
    let anchor_base_predicate = validate_recursive_base_predicates(
        anchor_select.selection.as_ref(),
        catalog,
        anchor_alias,
    )?;

    // Recursive term: r joined with the base relation on one equality.
    let [recursive_from] = recursive_select.from.as_slice() else {
        return unsupported("recursive term requires exactly one FROM join");
    };
    let [join] = recursive_from.joins.as_slice() else {
        return unsupported("recursive term requires exactly one JOIN");
    };
    if !matches!(
        join.join_operator,
        JoinOperator::Inner(_) | JoinOperator::Join(_)
    ) {
        return unsupported("recursive term currently supports INNER JOIN only");
    }
    let left_table = registered_table_ref(&recursive_from.relation, "recursive")?;
    let right_table = registered_table_ref(&join.relation, "recursive")?;
    let left_is_recursive = identifier_eq(left_table.name.as_str(), recursion_alias.as_str());
    let right_is_recursive = identifier_eq(right_table.name.as_str(), recursion_alias.as_str());
    if left_is_recursive == right_is_recursive {
        return unsupported(
            "recursive term must reference the recursion relation exactly once and the base relation once",
        );
    }
    let (recursive_table, base_table) = if left_is_recursive {
        (left_table, right_table)
    } else {
        (right_table, left_table)
    };
    if !identifier_eq(
        base_table.name.as_str(),
        catalog.relation_schema.relation_id.as_str(),
    ) {
        return unsupported(
            "recursive term must join the recursion relation with the registered relation",
        );
    }
    let constraint = match &join.join_operator {
        JoinOperator::Inner(constraint) | JoinOperator::Join(constraint) => constraint,
        _ => return unsupported("recursive term requires an INNER JOIN"),
    };
    let JoinConstraint::On(on_expr) = constraint else {
        return unsupported("recursive term requires an ON predicate");
    };
    let on_expr: &Expr = on_expr;
    let conjuncts = split_and_conjuncts(on_expr);
    let [equality] = conjuncts.as_slice() else {
        return unsupported("recursive term ON predicate must be a single equality");
    };
    let Expr::BinaryOp { left, op, right } = equality else {
        return unsupported("recursive term ON predicate must be a single equality");
    };
    if op != &BinaryOperator::Eq {
        return unsupported("recursive term ON predicate must be an equality");
    }
    let left_ref = qualified_column_ref(left.as_ref())?;
    let right_ref = qualified_column_ref(right.as_ref())?;
    let (recursive_column, base_column) = if identifier_eq(
        left_ref.qualifier.as_str(),
        recursive_table.alias.as_str(),
    ) && identifier_eq(
        right_ref.qualifier.as_str(),
        base_table.alias.as_str(),
    ) {
        (left_ref.column.as_str(), right_ref.column.as_str())
    } else if identifier_eq(left_ref.qualifier.as_str(), base_table.alias.as_str())
        && identifier_eq(right_ref.qualifier.as_str(), recursive_table.alias.as_str())
    {
        (right_ref.column.as_str(), left_ref.column.as_str())
    } else {
        return unsupported(
            "recursive term ON must compare the recursion relation column with a base relation column",
        );
    };
    let base_join_column = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column_identifier_eq(column, base_column))
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "recursive term join base column is not registered".to_string(),
        })?;
    if base_join_column.column_id == catalog.relation_schema.weight_column_id {
        return unsupported("recursive term join must not reference the weight column");
    }
    if !recursion_column_names
        .iter()
        .any(|name| identifier_eq(name, recursive_column))
    {
        return unsupported(
            "recursive term ON must reference a recursion relation column from the anchor",
        );
    }
    // Type compatibility: the recursion column's type comes from its anchor
    // base column; it must match the base join column type so JSON equality
    // in the fixpoint join is semantically exact.
    let anchor_index = recursion_column_names
        .iter()
        .position(|name| identifier_eq(name, recursive_column))
        .ok_or_else(|| ViewPlanError::UnsupportedShape {
            reason: "recursive term ON recursion column is not in the anchor".to_string(),
        })?;
    let anchor_base_column = catalog_column_by_id(catalog, &anchor_projection[anchor_index])?;
    if anchor_base_column.physical_arrow_type != base_join_column.physical_arrow_type
        || anchor_base_column.nullable != base_join_column.nullable
    {
        return unsupported(
            "recursive term join columns must have identical physical types and nullability",
        );
    }

    // Recursive term projection: positional names must match the anchor.
    let mut recursive_projection = Vec::new();
    for (index, item) in recursive_select.projection.iter().enumerate() {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
            _ => return unsupported("recursive term projections must be direct columns"),
        };
        let (source, column_name) = if let Some(column) =
            expression_filter_project_column(expr, catalog, Some(base_table.alias.as_str()), None)
        {
            if column.column_id == catalog.relation_schema.weight_column_id {
                return unsupported(
                    "recursive term projections must not reference the weight column",
                );
            }
            (
                RecursiveProjectionItemV1::Base {
                    column_id: column.column_id.clone(),
                },
                column.name.clone(),
            )
        } else {
            let reference = qualified_column_ref(expr)?;
            if !identifier_eq(reference.qualifier.as_str(), recursive_table.alias.as_str()) {
                return unsupported(
                    "recursive term projections must reference the recursion relation or the base relation",
                );
            }
            (
                RecursiveProjectionItemV1::Recursive {
                    column_id: reference.column.clone(),
                },
                reference.column.clone(),
            )
        };
        let expected_name =
            recursion_column_names
                .get(index)
                .ok_or_else(|| ViewPlanError::UnsupportedShape {
                    reason: "recursive term projection arity must match the anchor".to_string(),
                })?;
        if !identifier_eq(
            alias.unwrap_or(column_name.as_str()),
            expected_name.as_str(),
        ) {
            return unsupported(
                "recursive term projection column names must positionally match the anchor",
            );
        }
        recursive_projection.push(source);
    }
    if recursive_projection.len() != recursion_column_names.len() {
        return unsupported("recursive term projection arity must match the anchor");
    }
    let recursive_base_predicate = validate_recursive_base_predicates(
        recursive_select.selection.as_ref(),
        catalog,
        base_table.alias.as_str(),
    )?;

    // Outer query: plain projection over r with exactly the r columns.
    let SetExpr::Select(outer_select) = query.body.as_ref() else {
        return unsupported("recursive CTE outer query must be a plain SELECT");
    };
    validate_plain_select_clauses(outer_select)?;
    let [outer_from] = outer_select.from.as_slice() else {
        return unsupported("recursive CTE outer query requires exactly one FROM table");
    };
    if !outer_from.joins.is_empty() {
        return unsupported("recursive CTE outer query must not contain joins");
    }
    let outer_table = registered_table_ref(&outer_from.relation, "recursive")?;
    if !identifier_eq(outer_table.name.as_str(), recursion_alias.as_str()) {
        return unsupported("recursive CTE outer query must reference the recursion relation");
    }
    if !group_by_is_empty(&outer_select.group_by) {
        return unsupported("recursive CTE outer query must not aggregate");
    }
    if outer_select.selection.is_some() {
        return unsupported("recursive CTE outer query WHERE is not supported yet");
    }
    let mut projected_r_columns = Vec::new();
    for item in &outer_select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) => expr,
            SelectItem::ExprWithAlias { .. } => {
                return unsupported("recursive CTE outer projections must not use aliases")
            }
            _ => return unsupported("recursive CTE outer projections must be direct columns"),
        };
        let column_name = match expr {
            Expr::Identifier(identifier) => identifier.value.clone(),
            Expr::CompoundIdentifier(_) => {
                let reference = qualified_column_ref(expr)?;
                if !identifier_eq(reference.qualifier.as_str(), outer_table.alias.as_str()) {
                    return unsupported(
                        "recursive CTE outer projections must reference the recursion relation",
                    );
                }
                reference.column.clone()
            }
            _ => return unsupported("recursive CTE outer projections must be direct columns"),
        };
        projected_r_columns.push(column_name);
    }
    if projected_r_columns.len() != recursion_column_names.len()
        || projected_r_columns
            .iter()
            .zip(recursion_column_names.iter())
            .any(|(actual, expected)| !identifier_eq(actual, expected))
    {
        return unsupported(
            "recursive CTE outer projection must select every recursion column in anchor order",
        );
    }

    Ok(SupportedRecursiveFixpointPlanV1 {
        schema_version: 1,
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        recursion_column_names,
        anchor_projection,
        recursive_projection,
        recursive_join: RecursiveEquiJoinV1 {
            recursive_column_id: recursive_column.to_string(),
            base_column_id: base_join_column.column_id.clone(),
        },
        recursive_base_predicate,
        anchor_base_predicate,
        resource_contract: RecursiveFixpointContractV1 {
            max_iterations: 100_000,
            max_derived_rows: 1_000_000,
            max_work_units_per_epoch: 10_000_000,
        },
    })
}

fn validate_recursive_base_predicates(
    selection: Option<&Expr>,
    catalog: &VelorixRelationCatalogV1,
    alias: &str,
) -> Result<Vec<RecursiveBasePredicateV1>, ViewPlanError> {
    let Some(selection) = selection else {
        return Ok(Vec::new());
    };
    let conjuncts = split_and_conjuncts(selection);
    let mut predicates = Vec::new();
    for conjunct in conjuncts {
        let Expr::BinaryOp { left, op, right } = conjunct else {
            return unsupported(
                "recursive CTE WHERE clauses must be conjunctive base-column comparisons",
            );
        };
        let comparison_op = match op {
            BinaryOperator::Eq => PredicateOp::Eq,
            BinaryOperator::NotEq => PredicateOp::NotEq,
            BinaryOperator::Lt => PredicateOp::Lt,
            BinaryOperator::LtEq => PredicateOp::LtEq,
            BinaryOperator::Gt => PredicateOp::Gt,
            BinaryOperator::GtEq => PredicateOp::GtEq,
            _ => return unsupported("recursive CTE WHERE clauses must use comparison predicates"),
        };
        let (column_expr, literal_expr) = if let Some(column) =
            expression_filter_project_column(left.as_ref(), catalog, Some(alias), None)
        {
            (Some(column), right)
        } else {
            (
                expression_filter_project_column(right.as_ref(), catalog, Some(alias), None),
                left,
            )
        };
        let Some(column) = column_expr else {
            return unsupported(
                "recursive CTE WHERE comparisons must reference a base relation column",
            );
        };
        if column.column_id == catalog.relation_schema.weight_column_id {
            return unsupported("recursive CTE WHERE must not reference the weight column");
        }
        let Expr::Value(value) = literal_expr.as_ref() else {
            return unsupported("recursive CTE WHERE comparisons require a literal");
        };
        let literal = match &value.value {
            SqlValue::Number(text, _) => {
                let integer = text
                    .parse::<i64>()
                    .map_err(|_| ViewPlanError::UnsupportedShape {
                        reason: "recursive CTE WHERE literal must be an integer".to_string(),
                    })?;
                if !matches!(column.physical_arrow_type, ArrowPhysicalTypeV1::Int64) {
                    return unsupported(
                        "recursive CTE WHERE integer literals require an Int64 column",
                    );
                }
                JsonValue::Number(integer.into())
            }
            SqlValue::SingleQuotedString(text) => {
                if !matches!(column.physical_arrow_type, ArrowPhysicalTypeV1::Utf8) {
                    return unsupported(
                        "recursive CTE WHERE string literals require a Utf8 column",
                    );
                }
                JsonValue::String(text.clone())
            }
            _ => return unsupported("recursive CTE WHERE literals must be integers or strings"),
        };
        predicates.push(RecursiveBasePredicateV1 {
            base_column_id: column.column_id.clone(),
            op: comparison_op,
            literal,
        });
    }
    Ok(predicates)
}

/// Phase 8.5: lowers an admitted recursive CTE to a logical view plan.
pub fn lower_supported_recursive_cte_sql_to_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_recursive_cte_sql(sql, catalog)?;
    let output_relation = logical_relation_from_schema(output_schema);
    finalize_logical_plan(VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations: vec![logical_relation_from_catalog(catalog)],
        output_relation: output_relation.clone(),
        nodes: vec![
            VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: "recursive_base_scan".to_string(),
                relation: logical_relation_from_catalog(catalog),
            },
            VelorixLogicalViewPlanNodeV1::Project {
                node_id: "recursive_projection".to_string(),
                input: "recursive_base_scan".to_string(),
                columns: supported
                    .anchor_projection
                    .iter()
                    .map(|column_id| column_ref(&supported.input_relation_id, column_id.as_str()))
                    .collect(),
                computed_columns: Vec::new(),
            },
            VelorixLogicalViewPlanNodeV1::Output {
                node_id: "output_materialized_view".to_string(),
                input: "recursive_projection".to_string(),
                relation: output_relation,
            },
        ],
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: Vec::new(),
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::RecursiveFixpointV1 {
            plan: Box::new(supported),
        },
    })
}

/// Phase 8.3: CROSS JOIN plan. The output is the full projected row per
/// left/right pair, keyed by itself; the projection must include both
/// primary keys so output rows are unique per pair (bag semantics).
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedCrossJoinPlanV1 {
    pub schema_version: u32,
    pub left_input_relation_id: String,
    pub right_input_relation_id: String,
    pub projection: Vec<CrossJoinProjectionItemV1>,
    pub resource_contract: CrossJoinResourceContractV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CrossJoinProjectionItemV1 {
    pub side: CrossJoinSideV1,
    pub column_id: String,
    pub output_name: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossJoinSideV1 {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CrossJoinResourceContractV1 {
    pub max_rows_per_side: u64,
    pub max_pairs_per_epoch: u64,
}

/// Phase 8.3: validates `SELECT ... FROM left CROSS JOIN right` over two
/// registered relations. Admission requires a bare CROSS JOIN (no ON/USING),
/// a plain projection of direct columns that includes both primary keys,
/// and no WHERE, GROUP BY, DISTINCT, or aggregates. State and per-epoch
/// output are bounded by `CrossJoinResourceContractV1`.
pub fn validate_supported_cross_join_sql(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<SupportedCrossJoinPlanV1, ViewPlanError> {
    let [left_catalog, right_catalog] = catalogs else {
        return unsupported("cross join SQL requires exactly two input relations");
    };
    for catalog in catalogs {
        catalog.validate()?;
        let adapter = crate::relation::supported_incremental_adapter_spec(
            &catalog.incremental_adapter.adapter_id,
        )
        .ok_or(RelationSchemaError::InvalidRelationSchema {
            field: "incremental_adapter.adapter_id",
        })?;
        if !matches!(adapter, SupportedIncrementalAdapterSpec::Generic) {
            return unsupported("cross join SQL requires generic (+-1 weight) inputs; scalar sum/count adapter weights are not supported yet");
        }
    }
    let query = parse_single_query(sql)?;
    validate_query_level_clauses(&query, false)?;
    let SetExpr::Select(select) = query.body.as_ref() else {
        return unsupported("cross join requires a single SELECT");
    };
    validate_plain_select_clauses(select)?;
    if select.distinct.is_some() || !group_by_is_empty(&select.group_by) {
        return unsupported("cross join does not support DISTINCT or GROUP BY");
    }
    let [left_from] = select.from.as_slice() else {
        return unsupported("cross join requires exactly one FROM join");
    };
    let left_table = registered_table_ref(&left_from.relation, "left")?;
    if !identifier_eq(
        left_table.name.as_str(),
        left_catalog.relation_schema.relation_id.as_str(),
    ) {
        return unsupported("cross join left relation must be the first registered relation");
    }
    let [right_join] = left_from.joins.as_slice() else {
        return unsupported("cross join requires exactly one JOIN");
    };
    if !matches!(
        right_join.join_operator,
        JoinOperator::CrossJoin(JoinConstraint::None)
    ) {
        return unsupported("cross join requires a bare CROSS JOIN without ON or USING");
    }
    let right_table = registered_table_ref(&right_join.relation, "right")?;
    if !identifier_eq(
        right_table.name.as_str(),
        right_catalog.relation_schema.relation_id.as_str(),
    ) {
        return unsupported("cross join right relation must be the second registered relation");
    }
    if select.selection.is_some() {
        return unsupported("cross join WHERE clauses are not supported yet");
    }
    let left_key = catalog_primary_key_column(left_catalog)?;
    let right_key = catalog_primary_key_column(right_catalog)?;
    let mut projection = Vec::new();
    let mut output_names = BTreeSet::new();
    let mut has_left_key = false;
    let mut has_right_key = false;
    for item in &select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
            _ => return unsupported("cross join projections must be direct columns"),
        };
        let reference = qualified_column_ref(expr)?;
        let (side, catalog, alias_hint) =
            if identifier_eq(reference.qualifier.as_str(), left_table.alias.as_str()) {
                (
                    CrossJoinSideV1::Left,
                    left_catalog,
                    left_table.alias.as_str(),
                )
            } else if identifier_eq(reference.qualifier.as_str(), right_table.alias.as_str()) {
                (
                    CrossJoinSideV1::Right,
                    right_catalog,
                    right_table.alias.as_str(),
                )
            } else {
                return unsupported(
                    "cross join projections must reference one of the two joined table aliases",
                );
            };
        let column = qualified_ref_catalog_column(&reference, catalog)?;
        let output_name = alias.unwrap_or(column.name.as_str()).to_string();
        if !output_names.insert(output_name.clone()) {
            return unsupported("cross join output column names must be unique");
        }
        if column.column_id == catalog.relation_schema.weight_column_id {
            return unsupported("cross join projections must not reference weight columns");
        }
        if side == CrossJoinSideV1::Left && column.column_id == left_key.column_id {
            has_left_key = true;
        }
        if side == CrossJoinSideV1::Right && column.column_id == right_key.column_id {
            has_right_key = true;
        }
        let _ = alias_hint;
        projection.push(CrossJoinProjectionItemV1 {
            side,
            column_id: column.column_id.clone(),
            output_name,
        });
    }
    if !has_left_key || !has_right_key {
        return unsupported(
            "cross join output must include both the left and right primary key columns",
        );
    }
    Ok(SupportedCrossJoinPlanV1 {
        schema_version: 1,
        left_input_relation_id: left_catalog.relation_schema.relation_id.clone(),
        right_input_relation_id: right_catalog.relation_schema.relation_id.clone(),
        projection,
        resource_contract: CrossJoinResourceContractV1 {
            max_rows_per_side: 1_000_000,
            max_pairs_per_epoch: 1_000_000,
        },
    })
}

/// Phase 8.3: lowers an admitted CROSS JOIN to a logical view plan.
pub fn lower_supported_cross_join_sql_to_logical_plan(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_cross_join_sql(sql, catalogs)?;
    let output_relation = logical_relation_from_schema(output_schema);
    let left_catalog = catalog_for_relation_in_slice(catalogs, &supported.left_input_relation_id)?;
    let right_catalog =
        catalog_for_relation_in_slice(catalogs, &supported.right_input_relation_id)?;
    finalize_logical_plan(VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.to_string(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.to_string(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.to_string(),
        input_relations: catalogs.iter().map(logical_relation_from_catalog).collect(),
        output_relation: output_relation.clone(),
        nodes: vec![
            VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: "cross_left_scan".to_string(),
                relation: logical_relation_from_catalog(left_catalog),
            },
            VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: "cross_right_scan".to_string(),
                relation: logical_relation_from_catalog(right_catalog),
            },
            VelorixLogicalViewPlanNodeV1::InnerEquiJoin {
                node_id: "cross_join".to_string(),
                left: "cross_left_scan".to_string(),
                right: "cross_right_scan".to_string(),
                left_key: column_ref(&supported.left_input_relation_id, "cross_key_left"),
                right_key: column_ref(&supported.right_input_relation_id, "cross_key_right"),
                composite_equality: None,
            },
            VelorixLogicalViewPlanNodeV1::Output {
                node_id: "output_materialized_view".to_string(),
                input: "cross_join".to_string(),
                relation: output_relation,
            },
        ],
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: Vec::new(),
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::CrossJoin {
            plan: Box::new(supported),
        },
    })
}
