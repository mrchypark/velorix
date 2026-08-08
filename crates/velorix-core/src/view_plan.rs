//! Materialized view SQL admission and logical plan contracts.
//!
//! SQL is only the user-facing input. The runtime contract is the versioned
//! logical plan produced by this module.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Number as JsonNumber, Value as JsonValue};
use sqlparser::{
    ast::{
        BinaryOperator, CastKind, DataType, DateTimeField, Distinct, DuplicateTreatment, Expr,
        Fetch, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, GroupByExpr,
        Ident, JoinConstraint, JoinOperator, LimitClause, ObjectName, OrderByExpr, OrderByKind,
        Query, Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, SetOperator,
        SetQuantifier, Statement as SqlStatement, TableAlias, TableFactor, TableSampleKind,
        UnaryOperator, Value as SqlValue, WindowType,
    },
    dialect::GenericDialect,
    parser::{Parser, ParserError},
};
use thiserror::Error;

use crate::{
    relation::{
        ArrowPhysicalTypeV1, RelationColumnV1, RelationSchemaError,
        SupportedIncrementalAdapterSpec, VelorixRelationCatalogV1,
    },
    view_contract::{stable_bytes_hash, RelationSchema},
};

pub const LOGICAL_VIEW_PLAN_VERSION_V1: u32 = 1;
pub const LOGICAL_VIEW_PLAN_HASH_PREFIX: &str = "velorix-logical-view-plan-sha256-v1";
pub const LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V1: &str = "velorix-logical-view-capabilities-v1";
pub const LOGICAL_VIEW_STATE_CODEC_VERSION_V1: &str = "velorix-logical-view-state-v1";
pub const LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1: &str = "velorix-materialized-output-v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VelorixLogicalViewPlanV1 {
    pub plan_version: u32,
    pub plan_hash: Option<String>,
    pub view_sql: String,
    pub capability_version: String,
    pub input_relations: Vec<LogicalPlanRelationRef>,
    pub output_relation: LogicalPlanRelationRef,
    pub nodes: Vec<VelorixLogicalViewPlanNodeV1>,
    pub state_requirements: Vec<LogicalPlanStateRequirementV1>,
    pub output_codec_version: String,
    pub execution: VelorixLogicalViewExecutionV1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanRelationRef {
    pub relation_id: String,
    pub relation_name: String,
    pub relation_version: String,
    pub schema_fingerprint: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
    },
    LeftEquiJoin {
        node_id: String,
        left: String,
        right: String,
        left_key: LogicalPlanColumnRef,
        right_key: LogicalPlanColumnRef,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanColumnRef {
    pub relation_id: String,
    pub column_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanPredicateV1 {
    pub column: LogicalPlanColumnRef,
    pub op: PredicateOp,
    pub literal: JsonValue,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPlanAggregateAccumulatorV1 {
    pub function: LogicalPlanAggregateFunctionV1,
    pub input: Option<LogicalPlanColumnRef>,
    pub output_column_id: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalPlanAggregateFunctionV1 {
    Sum,
    Count,
    CountDistinct,
    Min,
    Max,
    Avg,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
    TumblingEventTimeAggregate {
        plan: SupportedTumblingWindowPlan,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedViewPlan {
    pub input_relation_id: String,
    pub group_key_column_id: String,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate_expr: Option<RowPredicateExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_k: Option<SupportedTopKPlan>,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedJoinViewPlan {
    pub left_input_relation_id: String,
    pub right_input_relation_id: String,
    #[serde(default)]
    pub join_kind: SupportedJoinKind,
    pub left_join_key_column_id: String,
    pub right_join_key_column_id: String,
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

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportedJoinKind {
    #[default]
    Inner,
    Left,
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
    match catalogs {
        [catalog] => match lower_supported_view_sql_to_logical_plan(sql, catalog, output_schema) {
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
        },
        [_, _] => lower_supported_join_view_sql_to_logical_plan(sql, catalogs, output_schema),
        _ => unsupported("view SQL admission currently supports one or two input relations"),
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

pub fn validate_logical_view_plan(plan: &VelorixLogicalViewPlanV1) -> Result<(), ViewPlanError> {
    if plan.plan_version != LOGICAL_VIEW_PLAN_VERSION_V1 {
        return invalid_logical_plan("unsupported logical view plan version");
    }
    if plan.capability_version != LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V1 {
        return invalid_logical_plan("unsupported logical view capability version");
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
    let projection = validate_projection(
        select,
        catalog,
        key_column,
        relation_alias,
        source_projection,
    )?;
    validate_aggregate_source_projection(
        source_projection,
        key_column,
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
    validate_group_by_key(
        select,
        catalog,
        key_column,
        relation_alias,
        &projection.output_key_column_id,
        source_projection,
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
    Ok(SupportedViewPlan {
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        group_key_column_id: key_column.column_id.clone(),
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
    })
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
    plan.plan_hash = Some(logical_view_plan_hash(&plan)?);
    validate_logical_view_plan(&plan)?;
    Ok(plan)
}

fn single_key_sum_count_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
    supported: SupportedViewPlan,
) -> VelorixLogicalViewPlanV1 {
    let input_relation = logical_relation_from_catalog(catalog);
    let output_relation = logical_relation_from_schema(output_schema);
    let group_key = column_ref(&supported.input_relation_id, &supported.group_key_column_id);
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
    let aggregate_node = "aggregate_sum_count".to_string();
    nodes.push(VelorixLogicalViewPlanNodeV1::Aggregate {
        node_id: aggregate_node.clone(),
        input: current_node,
        group_keys: vec![group_key.clone()],
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
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V1,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V1.to_string(),
        input_relations: vec![input_relation],
        output_relation,
        nodes,
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: aggregate_node,
            state_kind: LogicalPlanStateKindV1::Aggregate,
            key_columns: vec![group_key],
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution: VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported },
    }
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
    let left_scan = "scan_left".to_string();
    let right_scan = "scan_right".to_string();
    let mut left_join_input = left_scan.clone();
    let mut right_join_input = right_scan.clone();
    let join_node = match supported.join_kind {
        SupportedJoinKind::Inner => "inner_equi_join".to_string(),
        SupportedJoinKind::Left => "left_equi_join".to_string(),
    };
    let aggregate_node = "aggregate_join_sum_count".to_string();
    let mut current_node = aggregate_node.clone();
    let left_key = column_ref(
        &supported.left_input_relation_id,
        &supported.left_join_key_column_id,
    );
    let right_key = column_ref(
        &supported.right_input_relation_id,
        &supported.right_join_key_column_id,
    );
    let group_key = column_ref(
        &supported.group_key_relation_id,
        &supported.group_key_column_id,
    );
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
    match supported.join_kind {
        SupportedJoinKind::Inner => nodes.push(VelorixLogicalViewPlanNodeV1::InnerEquiJoin {
            node_id: join_node.clone(),
            left: left_join_input,
            right: right_join_input,
            left_key: left_key.clone(),
            right_key: right_key.clone(),
        }),
        SupportedJoinKind::Left => nodes.push(VelorixLogicalViewPlanNodeV1::LeftEquiJoin {
            node_id: join_node.clone(),
            left: left_join_input,
            right: right_join_input,
            left_key: left_key.clone(),
            right_key: right_key.clone(),
        }),
    };
    nodes.push(VelorixLogicalViewPlanNodeV1::Aggregate {
        node_id: aggregate_node.clone(),
        input: join_node.clone(),
        group_keys: vec![group_key.clone()],
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
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V1,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V1.to_string(),
        input_relations: vec![left_relation.clone(), right_relation.clone()],
        output_relation: output_relation.clone(),
        nodes,
        state_requirements: vec![
            LogicalPlanStateRequirementV1 {
                node_id: join_node,
                state_kind: LogicalPlanStateKindV1::JoinIndex,
                key_columns: vec![left_key, right_key],
                codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
            },
            LogicalPlanStateRequirementV1 {
                node_id: aggregate_node,
                state_kind: LogicalPlanStateKindV1::Aggregate,
                key_columns: vec![group_key],
                codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
            },
        ],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
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
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V1,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V1.to_string(),
        input_relations: vec![input_relation.clone()],
        output_relation: output_relation.clone(),
        nodes,
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: latest_node,
            state_kind: LogicalPlanStateKindV1::LatestByKey,
            key_columns: vec![key],
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
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
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V1,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V1.to_string(),
        input_relations: vec![input_relation],
        output_relation,
        nodes,
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: row_number_node,
            state_kind: LogicalPlanStateKindV1::RowNumber,
            key_columns: vec![partition, order, key],
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
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
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V1,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V1.to_string(),
        input_relations: vec![input_relation.clone()],
        output_relation: output_relation.clone(),
        nodes,
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: aggregate_node,
            state_kind: LogicalPlanStateKindV1::TumblingWindowAggregate,
            key_columns: vec![group_key],
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
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
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V1,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V1.to_string(),
        input_relations: vec![input_relation],
        output_relation,
        nodes,
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: project_node,
            state_kind: LogicalPlanStateKindV1::Projection,
            key_columns: vec![key],
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.to_string(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.to_string(),
        execution: VelorixLogicalViewExecutionV1::FilterProject { plan: supported },
    }
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
        column_id: column_id.to_string(),
    }
}

fn invalid_logical_plan<T>(reason: impl Into<String>) -> Result<T, ViewPlanError> {
    Err(ViewPlanError::InvalidLogicalPlan {
        reason: reason.into(),
    })
}

pub fn validate_supported_join_view_sql(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<SupportedJoinViewPlan, ViewPlanError> {
    let [left_catalog, right_catalog] = catalogs else {
        return unsupported("join view SQL currently requires exactly two input relations");
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
        left_join_column,
        right_join_column,
    } = validate_two_input_join(select, left_catalog, right_catalog, &cte_sources)?;
    let left_key = catalog_primary_key_column(left_catalog)?;
    let right_key = catalog_primary_key_column(right_catalog)?;
    if left_join_column.column_id != left_key.column_id
        || right_join_column.column_id != right_key.column_id
    {
        return unsupported("JOIN ON must compare the primary key columns of both inputs");
    }
    if left_join_column.physical_arrow_type != right_join_column.physical_arrow_type {
        return unsupported("JOIN ON primary key columns must have identical physical Arrow types");
    }
    let projection = validate_join_projection(
        select,
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
        &predicate_expr,
    )?;

    Ok(SupportedJoinViewPlan {
        left_input_relation_id: left_catalog.relation_schema.relation_id.clone(),
        right_input_relation_id: right_catalog.relation_schema.relation_id.clone(),
        join_kind,
        left_join_key_column_id: left_key.column_id.clone(),
        right_join_key_column_id: right_key.column_id.clone(),
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
    key_column: &RelationColumnV1,
    aggregate_outputs: &[SupportedAggregateOutput],
) -> Result<(), ViewPlanError> {
    let Some(projection) = projection else {
        return Ok(());
    };
    let projected_column_ids = &projection.projected_column_ids;
    if !projected_column_ids.contains(&key_column.column_id) {
        return unsupported("aggregate source projection must include the group key column");
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
    left_join_column: &'a RelationColumnV1,
    right_join_column: &'a RelationColumnV1,
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
    let left_table = table_ref(&table.relation, "left")?;
    let right_table = table_ref(&join.relation, "right")?;
    let left_catalog =
        catalog_for_table_with_ctes(&left_table, first_catalog, second_catalog, cte_sources)?;
    let right_catalog =
        catalog_for_table_with_ctes(&right_table, first_catalog, second_catalog, cte_sources)?;
    validate_derived_source_projection(&left_table, left_catalog)?;
    validate_derived_source_projection(&right_table, right_catalog)?;
    validate_join_cte_sources_are_used(cte_sources, &left_table, &right_table)?;
    if left_catalog.relation_schema.relation_id == right_catalog.relation_schema.relation_id {
        return unsupported("JOIN inputs must be distinct relations");
    }
    let (join_kind, constraint) = match &join.join_operator {
        JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
            (SupportedJoinKind::Inner, constraint)
        }
        JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
            (SupportedJoinKind::Left, constraint)
        }
        _ => {
            return unsupported(
                "only INNER or narrow LEFT JOIN is supported for join materialization",
            )
        }
    };
    let (left_join_column, right_join_column, on_residual_selection) = match constraint {
        JoinConstraint::On(expr) => {
            let mut left_join_column: Option<&RelationColumnV1> = None;
            let mut right_join_column: Option<&RelationColumnV1> = None;
            let mut residuals = Vec::new();
            for conjunct in join_on_conjuncts(expr) {
                if let Some((left_join_ref, right_join_ref)) =
                    join_on_equality_refs(conjunct, &left_table.alias, &right_table.alias)?
                {
                    let next_left_column =
                        qualified_ref_catalog_column(&left_join_ref, left_catalog)?;
                    let next_right_column =
                        qualified_ref_catalog_column(&right_join_ref, right_catalog)?;
                    if let (Some(left_column), Some(right_column)) =
                        (left_join_column, right_join_column)
                    {
                        if left_column.column_id == next_left_column.column_id
                            && right_column.column_id == next_right_column.column_id
                        {
                            continue;
                        }
                        return unsupported("JOIN ON must contain exactly one key equality");
                    }
                    left_join_column = Some(next_left_column);
                    right_join_column = Some(next_right_column);
                } else {
                    residuals.push(conjunct.clone());
                }
            }
            let Some(left_join_column) = left_join_column else {
                return unsupported("JOIN ON must contain exactly one key equality");
            };
            let Some(right_join_column) = right_join_column else {
                return unsupported("JOIN ON must contain exactly one key equality");
            };
            (
                left_join_column,
                right_join_column,
                combine_join_on_residuals(residuals),
            )
        }
        JoinConstraint::Using(columns) => {
            let [column] = columns.as_slice() else {
                return unsupported("JOIN USING must reference exactly one column");
            };
            let Some(column_name) = single_object_name_identifier(column) else {
                return unsupported("JOIN USING column must be an unqualified identifier");
            };
            (
                catalog_column_by_identifier(left_catalog, column_name.as_str())?,
                catalog_column_by_identifier(right_catalog, column_name.as_str())?,
                None,
            )
        }
        _ => return unsupported("JOIN must use one ON equality predicate or USING column"),
    };
    Ok(JoinSqlBindings {
        left_catalog,
        right_catalog,
        join_kind,
        left_alias: left_table.alias,
        right_alias: right_table.alias,
        left_source_selection: left_table.source_selection,
        right_source_selection: right_table.source_selection,
        on_residual_selection,
        left_join_column,
        right_join_column,
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

fn join_projection_key<'a>(
    item: &SelectItem,
    left_alias: &str,
    left_catalog: &'a VelorixRelationCatalogV1,
    left_key: &'a RelationColumnV1,
    right_alias: &str,
    right_catalog: &'a VelorixRelationCatalogV1,
    right_key: &'a RelationColumnV1,
) -> Result<(&'a VelorixRelationCatalogV1, &'a RelationColumnV1), ViewPlanError> {
    if select_item_references_qualified_column(item, left_alias, left_key) {
        Ok((left_catalog, left_key))
    } else if select_item_references_qualified_column(item, right_alias, right_key) {
        Ok((right_catalog, right_key))
    } else {
        unsupported("first projection must be one of the joined primary key columns")
    }
}

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

fn validate_join_projection<'a>(
    select: &Select,
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
    let (output_key_catalog, output_key_column) = join_projection_key(
        key,
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
    predicate_expr: &Option<JoinPredicateExpr>,
) -> Result<(), ViewPlanError> {
    if join_kind != SupportedJoinKind::Left {
        return Ok(());
    }
    if projection.group_key_relation_id != left_catalog.relation_schema.relation_id
        || projection.group_key_column_id != left_key.column_id
    {
        return unsupported("LEFT JOIN materialization must GROUP BY the left primary key");
    }
    if on_residual_predicate.is_some() {
        return unsupported("LEFT JOIN materialization does not support ON residual predicates");
    }
    if projection.shared_aggregate_filter_expr.is_some() {
        return unsupported(
            "LEFT JOIN materialization does not support shared aggregate FILTER clauses",
        );
    }
    if projection
        .aggregate_filter_exprs
        .values()
        .any(|expr| !join_predicate_expr_is_left_only(expr, left_catalog))
    {
        return unsupported(
            "LEFT JOIN materialization only supports left-side aggregate FILTER predicates",
        );
    }
    if projection.aggregate_outputs.iter().any(|aggregate| {
        aggregate.input_relation_side == Some(SupportedAggregateInputRelationSide::Right)
    }) {
        return unsupported(
            "LEFT JOIN materialization does not support right-side aggregate inputs",
        );
    }
    if predicate_expr
        .as_ref()
        .into_iter()
        .flat_map(JoinPredicateExpr::leaf_predicates)
        .any(|predicate| predicate.relation_id != left_catalog.relation_schema.relation_id)
    {
        return unsupported("LEFT JOIN materialization does not support right-side predicates");
    }
    Ok(())
}

fn join_predicate_expr_is_left_only(
    predicate_expr: &JoinPredicateExpr,
    left_catalog: &VelorixRelationCatalogV1,
) -> bool {
    let left_relation_id = &left_catalog.relation_schema.relation_id;
    match predicate_expr {
        JoinPredicateExpr::Atom { predicate } => predicate.relation_id == *left_relation_id,
        JoinPredicateExpr::ScalarInt64Comparison { relation_id, .. } => {
            relation_id == left_relation_id
        }
        JoinPredicateExpr::ScalarInt64ExpressionComparison {
            left_relation_id: expr_left_relation_id,
            right_relation_id,
            ..
        } => expr_left_relation_id == left_relation_id && right_relation_id == left_relation_id,
        JoinPredicateExpr::And { left, right } | JoinPredicateExpr::Or { left, right } => {
            join_predicate_expr_is_left_only(left, left_catalog)
                && join_predicate_expr_is_left_only(right, left_catalog)
        }
    }
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
        return join_in_list_predicate_expr(relation_id, column.column_id.clone(), list, *negated);
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
        return row_in_list_predicate_expr(column.column_id.clone(), list, *negated);
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
        return row_in_list_predicate_expr(column.column_id.clone(), list, *negated);
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
        return row_in_list_predicate_expr(column.column_id.clone(), list, *negated);
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
        return row_in_list_predicate_expr(column.column_id.clone(), list, *negated);
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
    list: &[Expr],
    negated: bool,
) -> Result<RowPredicateExpr, ViewPlanError> {
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
    list: &[Expr],
    negated: bool,
) -> Result<JoinPredicateExpr, ViewPlanError> {
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
) -> Result<ValidatedAggregateProjection<'a>, ViewPlanError> {
    let [key, aggregates @ ..] = select.projection.as_slice() else {
        return unsupported("expected projection: key, aggregate...");
    };
    let key_is_bound = select_item_references_bound_column(
        key,
        catalog,
        key_column,
        relation_alias,
        source_projection,
    );
    let key_is_missing_from_source_projection = source_projection.is_some_and(|projection| {
        !source_projection_projects_column(projection, key_column.column_id.as_str())
            && select_item_references_column(key, key_column, relation_alias)
    });
    if !key_is_bound && !key_is_missing_from_source_projection {
        return unsupported("first projection must be the primary key column");
    }
    let output_key_column_id = select_item_alias_or_source_default(
        key,
        key_column.name.as_str(),
        relation_alias,
        source_projection,
    )?;

    if aggregates.is_empty() {
        return unsupported("expected at least one aggregate projection");
    }

    let mut output_ids = BTreeSet::new();
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
            | LogicalPlanAggregateFunctionV1::CountDistinct => {}
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
            let expression =
                supported_filter_project_projection_expr(expr, catalog, relation_alias)?;
            let input_column_id =
                    first_supported_projection_expr_column_id(&expression).ok_or_else(|| {
                        ViewPlanError::UnsupportedShape {
                            reason: "computed filter/project projections must reference at least one registered column".to_string(),
                        }
                    })?;
            (input_column_id, Some(expression), alias)
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
        return row_in_list_predicate_expr(column.column_id.clone(), list, *negated);
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
