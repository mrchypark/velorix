//! Materialized view SQL admission and logical plan contracts.
//!
//! SQL is only the user-facing input. The runtime contract is the versioned
//! logical plan produced by this module.

use std::collections::BTreeSet;

use datafusion::sql::{
    parser::{DFParser, Statement as DataFusionStatement},
    sqlparser::ast::{
        BinaryOperator, DateTimeField, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
        GroupByExpr, JoinConstraint, JoinOperator, ObjectName, Query, Select, SelectItem, SetExpr,
        Statement as SqlStatement, TableFactor, UnaryOperator, Value as SqlValue,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Number as JsonNumber, Value as JsonValue};
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
    InnerEquiJoin {
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
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VelorixLogicalViewExecutionV1 {
    SingleKeySumCount { plan: SupportedViewPlan },
    LatestByKey { plan: SupportedLatestByKeyPlan },
    TwoInputJoinSumCount { plan: SupportedJoinViewPlan },
    TumblingEventTimeAggregate { plan: SupportedTumblingWindowPlan },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedViewPlan {
    pub input_relation_id: String,
    pub group_key_column_id: String,
    pub sum_value_column_id: String,
    #[serde(default)]
    pub aggregate_outputs: Vec<SupportedAggregateOutput>,
    pub predicate: Option<RowPredicate>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedAggregateOutput {
    pub function: LogicalPlanAggregateFunctionV1,
    pub input_column_id: Option<String>,
    pub output_column_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedLatestByKeyPlan {
    pub input_relation_id: String,
    pub key_column_id: String,
    pub value_column_id: String,
    pub ordering_column_id: String,
    pub output_value_column_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedTumblingWindowPlan {
    pub input_relation_id: String,
    pub group_key_column_id: String,
    pub event_time_column_id: String,
    pub window_size_ns: i64,
    pub sum_value_column_id: String,
    pub aggregate_outputs: Vec<SupportedAggregateOutput>,
    pub window_start_output_column_id: String,
    pub window_end_output_column_id: String,
}

pub fn supported_view_plan_aggregate_outputs(
    plan: &SupportedViewPlan,
) -> Vec<SupportedAggregateOutput> {
    if plan.aggregate_outputs.is_empty() {
        vec![
            SupportedAggregateOutput {
                function: LogicalPlanAggregateFunctionV1::Sum,
                input_column_id: Some(plan.sum_value_column_id.clone()),
                output_column_id: "sum".to_string(),
            },
            SupportedAggregateOutput {
                function: LogicalPlanAggregateFunctionV1::Count,
                input_column_id: None,
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
    pub left_join_key_column_id: String,
    pub right_join_key_column_id: String,
    pub group_key_relation_id: String,
    pub group_key_column_id: String,
    pub sum_value_relation_id: String,
    pub sum_value_column_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RowPredicate {
    pub column_id: String,
    pub op: PredicateOp,
    pub literal: JsonValue,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PredicateOp {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

#[derive(Debug, Error)]
pub enum ViewPlanError {
    #[error(transparent)]
    Relation(#[from] RelationSchemaError),
    #[error("view SQL parse error: {0}")]
    Parse(#[from] datafusion::error::DataFusionError),
    #[error("view SQL is outside the supported materialization scope: {reason}")]
    UnsupportedShape { reason: String },
    #[error("invalid logical view plan: {reason}")]
    InvalidLogicalPlan { reason: String },
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
                        lower_supported_tumbling_window_sql_to_logical_plan(
                            sql,
                            catalog,
                            output_schema,
                        )
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
    let mut statements = DFParser::parse_sql(sql)?;
    if statements.len() != 1 {
        return unsupported("expected exactly one SELECT statement");
    }
    let statement = statements
        .pop_front()
        .expect("validated statement count must be one");
    let DataFusionStatement::Statement(statement) = statement else {
        return unsupported("expected a SQL SELECT statement");
    };
    let SqlStatement::Query(query) = *statement else {
        return unsupported("expected a SELECT statement");
    };
    let select = supported_plain_select(&query)?;

    validate_from_relation(select, catalog)?;
    let projection = validate_projection(select, catalog, key_column)?;
    let predicate = validate_selection(select, catalog, key_column, projection.value_column)?;
    validate_group_by_key(select, key_column)?;

    Ok(SupportedViewPlan {
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        group_key_column_id: key_column.column_id.clone(),
        sum_value_column_id: projection.value_column.column_id.clone(),
        aggregate_outputs: projection.aggregate_outputs,
        predicate,
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
    let mut statements = DFParser::parse_sql(sql)?;
    if statements.len() != 1 {
        return unsupported("expected exactly one SELECT statement");
    }
    let statement = statements
        .pop_front()
        .expect("validated statement count must be one");
    let DataFusionStatement::Statement(statement) = statement else {
        return unsupported("expected a SQL SELECT statement");
    };
    let SqlStatement::Query(query) = *statement else {
        return unsupported("expected a SELECT statement");
    };
    let select = supported_plain_select(&query)?;
    validate_plain_select_clauses(select)?;
    if select.selection.is_some() {
        return unsupported("WHERE is not supported for tumbling window materialization yet");
    }
    let (event_time_column, window_size_ns) = validate_tumble_from_relation(select, catalog)?;
    let projection = validate_tumbling_projection(select, catalog, key_column)?;
    validate_tumbling_group_by(select, key_column)?;

    Ok(SupportedTumblingWindowPlan {
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        group_key_column_id: key_column.column_id.clone(),
        event_time_column_id: event_time_column.column_id.clone(),
        window_size_ns,
        sum_value_column_id: projection.value_column.column_id.clone(),
        aggregate_outputs: projection.aggregate_outputs,
        window_start_output_column_id: "window_start".to_string(),
        window_end_output_column_id: "window_end".to_string(),
    })
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
    let mut statements = DFParser::parse_sql(sql)?;
    if statements.len() != 1 {
        return unsupported("expected exactly one SELECT statement");
    }
    let statement = statements
        .pop_front()
        .expect("validated statement count must be one");
    let DataFusionStatement::Statement(statement) = statement else {
        return unsupported("expected a SQL SELECT statement");
    };
    let SqlStatement::Query(query) = *statement else {
        return unsupported("expected a SELECT statement");
    };
    let select = supported_plain_select(&query)?;

    validate_from_relation(select, catalog)?;
    if select.selection.is_some() {
        return unsupported("WHERE is not supported for latest-by-key materialization yet");
    }
    let latest = validate_latest_by_key_projection(select, catalog, key_column)?;
    validate_group_by_key(select, key_column)?;

    Ok(SupportedLatestByKeyPlan {
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        key_column_id: key_column.column_id.clone(),
        value_column_id: latest.value_column.column_id.clone(),
        ordering_column_id: latest.ordering_column.column_id.clone(),
        output_value_column_id: latest.output_value_column_id,
    })
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
    if let Some(predicate) = &supported.predicate {
        let filter_node = "filter_input".to_string();
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
    let aggregate_node = "aggregate_sum_count".to_string();
    nodes.push(VelorixLogicalViewPlanNodeV1::Aggregate {
        node_id: aggregate_node.clone(),
        input: current_node,
        group_keys: vec![group_key.clone()],
        accumulators,
    });
    nodes.push(VelorixLogicalViewPlanNodeV1::Output {
        node_id: "output_materialized_view".to_string(),
        input: aggregate_node.clone(),
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
    let join_node = "inner_equi_join".to_string();
    let aggregate_node = "aggregate_join_sum_count".to_string();
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
    let sum_value = column_ref(
        &supported.sum_value_relation_id,
        &supported.sum_value_column_id,
    );
    Ok(VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V1,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V1.to_string(),
        input_relations: vec![left_relation.clone(), right_relation.clone()],
        output_relation: output_relation.clone(),
        nodes: vec![
            VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: left_scan.clone(),
                relation: left_relation,
            },
            VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: right_scan.clone(),
                relation: right_relation,
            },
            VelorixLogicalViewPlanNodeV1::InnerEquiJoin {
                node_id: join_node.clone(),
                left: left_scan,
                right: right_scan,
                left_key: left_key.clone(),
                right_key: right_key.clone(),
            },
            VelorixLogicalViewPlanNodeV1::Aggregate {
                node_id: aggregate_node.clone(),
                input: join_node.clone(),
                group_keys: vec![group_key.clone()],
                accumulators: vec![
                    LogicalPlanAggregateAccumulatorV1 {
                        function: LogicalPlanAggregateFunctionV1::Sum,
                        input: Some(sum_value),
                        output_column_id: "sum".to_string(),
                    },
                    LogicalPlanAggregateAccumulatorV1 {
                        function: LogicalPlanAggregateFunctionV1::Count,
                        input: None,
                        output_column_id: "count".to_string(),
                    },
                ],
            },
            VelorixLogicalViewPlanNodeV1::Output {
                node_id: "output_materialized_view".to_string(),
                input: aggregate_node.clone(),
                relation: output_relation,
            },
        ],
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
        execution: VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported },
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
    let latest_node = "latest_by_key".to_string();
    let project_node = "project_latest_value".to_string();
    VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V1,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V1.to_string(),
        input_relations: vec![input_relation.clone()],
        output_relation: output_relation.clone(),
        nodes: vec![
            VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: scan_node.clone(),
                relation: input_relation,
            },
            VelorixLogicalViewPlanNodeV1::LatestByKey {
                node_id: latest_node.clone(),
                input: scan_node,
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
            VelorixLogicalViewPlanNodeV1::Output {
                node_id: "output_materialized_view".to_string(),
                input: project_node,
                relation: output_relation,
            },
        ],
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
    let window_node = "tumbling_event_time_window".to_string();
    let aggregate_node = "aggregate_tumbling_window".to_string();
    VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V1,
        plan_hash: None,
        view_sql: sql.to_string(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V1.to_string(),
        input_relations: vec![input_relation.clone()],
        output_relation: output_relation.clone(),
        nodes: vec![
            VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: scan_node.clone(),
                relation: input_relation,
            },
            VelorixLogicalViewPlanNodeV1::TumblingWindow {
                node_id: window_node.clone(),
                input: scan_node,
                event_time_column: event_time.clone(),
                window_size_ns: supported.window_size_ns,
            },
            VelorixLogicalViewPlanNodeV1::Aggregate {
                node_id: aggregate_node.clone(),
                input: window_node,
                group_keys: vec![group_key.clone(), event_time],
                accumulators,
            },
            VelorixLogicalViewPlanNodeV1::Output {
                node_id: "output_materialized_view".to_string(),
                input: aggregate_node.clone(),
                relation: output_relation,
            },
        ],
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
        if adapter != SupportedIncrementalAdapterSpec::ScalarSumCount {
            return unsupported("join view SQL currently supports scalar sum/count inputs");
        }
    }

    let mut statements = DFParser::parse_sql(sql)?;
    if statements.len() != 1 {
        return unsupported("expected exactly one SELECT statement");
    }
    let statement = statements
        .pop_front()
        .expect("validated statement count must be one");
    let DataFusionStatement::Statement(statement) = statement else {
        return unsupported("expected a SQL SELECT statement");
    };
    let SqlStatement::Query(query) = *statement else {
        return unsupported("expected a SELECT statement");
    };
    let select = supported_plain_select(&query)?;
    validate_plain_select_clauses(select)?;
    if select.selection.is_some() {
        return unsupported("WHERE is not supported for join materialization yet");
    }

    let JoinSqlBindings {
        left_catalog,
        right_catalog,
        left_alias,
        right_alias,
        left_join_column,
        right_join_column,
    } = validate_two_input_join(select, left_catalog, right_catalog)?;
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
    let left_value =
        validate_join_projection(select, &right_alias, right_key, &left_alias, left_catalog)?;
    validate_join_group_by_key(select, &right_alias, right_catalog, right_key)?;

    Ok(SupportedJoinViewPlan {
        left_input_relation_id: left_catalog.relation_schema.relation_id.clone(),
        right_input_relation_id: right_catalog.relation_schema.relation_id.clone(),
        left_join_key_column_id: left_key.column_id.clone(),
        right_join_key_column_id: right_key.column_id.clone(),
        group_key_relation_id: right_catalog.relation_schema.relation_id.clone(),
        group_key_column_id: right_key.column_id.clone(),
        sum_value_relation_id: left_catalog.relation_schema.relation_id.clone(),
        sum_value_column_id: left_value.column_id.clone(),
    })
}

fn supported_plain_select(query: &Query) -> Result<&Select, ViewPlanError> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return unsupported("query-level clauses are not supported for materialized view planning");
    }
    match query.body.as_ref() {
        SetExpr::Select(select) => Ok(select),
        _ => unsupported("set operations, VALUES, and nested queries are not supported"),
    }
}

fn validate_plain_select_clauses(select: &Select) -> Result<(), ViewPlanError> {
    if select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return unsupported("only plain SELECT/FROM/GROUP BY sum/count views are supported");
    }
    Ok(())
}

fn validate_from_relation(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
) -> Result<(), ViewPlanError> {
    validate_plain_select_clauses(select)?;

    let [table] = select.from.as_slice() else {
        return unsupported("expected exactly one input relation");
    };
    if !table.joins.is_empty() {
        return unsupported("joins are not supported for single-input materialized view planning");
    }
    let TableFactor::Table {
        name,
        alias: _,
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
    if accepted
        .iter()
        .any(|candidate| identifier_eq(candidate, table_name.as_str()))
    {
        Ok(())
    } else {
        unsupported("FROM relation does not match the view input relation catalog")
    }
}

fn validate_tumble_from_relation<'a>(
    select: &Select,
    catalog: &'a VelorixRelationCatalogV1,
) -> Result<(&'a RelationColumnV1, i64), ViewPlanError> {
    let Some(declared_event_time_column_id) = &catalog.relation_schema.event_time_column_id else {
        return unsupported("tumbling window SQL requires a declared relation event-time column");
    };
    let [table] = select.from.as_slice() else {
        return unsupported("expected exactly one tumbling input relation");
    };
    if !table.joins.is_empty() {
        return unsupported("joins are not supported inside tumbling window materialization");
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
            "FROM must use tumble(relation, event_time, interval) for window views",
        );
    };
    if alias.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return unsupported(
            "tumbling window table aliases, hints, versions, and samples are unsupported",
        );
    }
    if !single_object_name_identifier(name)
        .as_deref()
        .is_some_and(|name| identifier_eq(name, "tumble"))
    {
        return unsupported("FROM must use tumble(relation, event_time, interval)");
    }
    let Some(args) = args else {
        return unsupported("tumble requires relation, event_time, and interval arguments");
    };
    if args.settings.is_some() {
        return unsupported("tumble settings are not supported");
    }
    let [relation_arg, event_time_arg, interval_arg] = args.args.as_slice() else {
        return unsupported("tumble requires relation, event_time, and interval arguments");
    };
    let relation_name = table_function_identifier_arg(relation_arg)?;
    let accepted = [
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_name.as_str(),
        catalog.datafusion_registration.name.as_str(),
    ];
    if !accepted
        .iter()
        .any(|candidate| identifier_eq(candidate, relation_name.as_str()))
    {
        return unsupported("tumble relation does not match the view input relation catalog");
    }
    let event_time_name = table_function_identifier_arg(event_time_arg)?;
    let Some(event_time_column) = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column_identifier_eq(column, event_time_name.as_str()))
    else {
        return unsupported("tumble event-time argument must reference a registered column");
    };
    if &event_time_column.column_id != declared_event_time_column_id {
        return unsupported("tumble event-time column must match relation event-time column");
    }
    match event_time_column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Date32
        | ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {}
        _ => {
            return unsupported(
                "tumble event-time column currently supports Int64, Date32, or TimestampNanosecond",
            )
        }
    }
    let window_size_ns = table_function_interval_ns_arg(interval_arg)?;
    if window_size_ns <= 0 {
        return unsupported("tumble interval must be positive");
    }
    Ok((event_time_column, window_size_ns))
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
        "second" | "seconds" => Ok(DateTimeField::Second),
        "minute" | "minutes" => Ok(DateTimeField::Minute),
        "hour" | "hours" => Ok(DateTimeField::Hour),
        "day" | "days" => Ok(DateTimeField::Day),
        _ => unsupported("tumble interval currently supports seconds, minutes, hours, or days"),
    }
}

fn interval_quantity_to_ns(quantity: i64, unit: DateTimeField) -> Result<i64, ViewPlanError> {
    let multiplier = match unit {
        DateTimeField::Second => 1_000_000_000_i64,
        DateTimeField::Minute => 60_000_000_000_i64,
        DateTimeField::Hour => 3_600_000_000_000_i64,
        DateTimeField::Day => 86_400_000_000_000_i64,
        _ => {
            return unsupported(
                "tumble interval currently supports seconds, minutes, hours, or days",
            )
        }
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
    left_alias: String,
    right_alias: String,
    left_join_column: &'a RelationColumnV1,
    right_join_column: &'a RelationColumnV1,
}

fn validate_two_input_join<'a>(
    select: &Select,
    first_catalog: &'a VelorixRelationCatalogV1,
    second_catalog: &'a VelorixRelationCatalogV1,
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
    let left_catalog = catalog_for_table(&left_table, first_catalog, second_catalog)?;
    let right_catalog = catalog_for_table(&right_table, first_catalog, second_catalog)?;
    if left_catalog.relation_schema.relation_id == right_catalog.relation_schema.relation_id {
        return unsupported("JOIN inputs must be distinct relations");
    }
    let constraint = match &join.join_operator {
        JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => constraint,
        _ => return unsupported("only INNER JOIN is supported for join materialization"),
    };
    let JoinConstraint::On(Expr::BinaryOp { left, op, right }) = constraint else {
        return unsupported("JOIN must use one ON equality predicate");
    };
    if !matches!(op, BinaryOperator::Eq) {
        return unsupported("JOIN ON must use equality");
    }
    let left_ref = qualified_column_ref(left)?;
    let right_ref = qualified_column_ref(right)?;
    let (left_join_ref, right_join_ref) =
        orient_join_refs(left_ref, right_ref, &left_table.alias, &right_table.alias)?;
    let left_join_column = qualified_ref_catalog_column(&left_join_ref, left_catalog)?;
    let right_join_column = qualified_ref_catalog_column(&right_join_ref, right_catalog)?;
    Ok(JoinSqlBindings {
        left_catalog,
        right_catalog,
        left_alias: left_table.alias,
        right_alias: right_table.alias,
        left_join_column,
        right_join_column,
    })
}

struct SqlTableRef {
    name: String,
    alias: String,
}

fn table_ref(factor: &TableFactor, side: &'static str) -> Result<SqlTableRef, ViewPlanError> {
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
    Ok(SqlTableRef { name, alias })
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

fn validate_join_group_by_key(
    select: &Select,
    right_alias: &str,
    right_catalog: &VelorixRelationCatalogV1,
    right_key: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    let GroupByExpr::Expressions(expressions, modifiers) = &select.group_by else {
        return unsupported("GROUP BY ALL is not supported");
    };
    if !modifiers.is_empty() {
        return unsupported("GROUP BY modifiers are not supported");
    }
    let [group_key] = expressions.as_slice() else {
        return unsupported("expected exactly one GROUP BY key");
    };
    let reference = qualified_column_ref(group_key)?;
    if !identifier_eq(reference.qualifier.as_str(), right_alias) {
        return unsupported("GROUP BY key must reference the right input table alias");
    }
    let column = qualified_ref_catalog_column(&reference, right_catalog)?;
    if column.column_id == right_key.column_id {
        Ok(())
    } else {
        unsupported("GROUP BY key must be the right input primary key column")
    }
}

fn validate_join_projection<'a>(
    select: &Select,
    right_alias: &str,
    right_key: &RelationColumnV1,
    left_alias: &str,
    left_catalog: &'a VelorixRelationCatalogV1,
) -> Result<&'a RelationColumnV1, ViewPlanError> {
    let [key, sum, count] = select.projection.as_slice() else {
        return unsupported("expected projection: key, sum(value), count(*)");
    };
    if !select_item_references_qualified_column(key, right_alias, right_key) {
        return unsupported("first projection must be the right input primary key column");
    }
    let left_value =
        select_item_sum_qualified_column(sum, left_alias, left_catalog).ok_or_else(|| {
            ViewPlanError::UnsupportedShape {
                reason: "second projection must be sum(left_value_column)".to_string(),
            }
        })?;
    validate_numeric_sum_column(left_catalog, left_value)?;
    if !select_item_is_count_star(count) {
        return unsupported("third projection must be count(*)");
    }
    Ok(left_value)
}

fn validate_selection(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
) -> Result<Option<RowPredicate>, ViewPlanError> {
    let Some(selection) = &select.selection else {
        return Ok(None);
    };
    let Expr::BinaryOp { left, op, right } = selection else {
        return unsupported("WHERE currently supports one column/literal comparison");
    };
    let (column_expr, literal_expr, op) = if expression_is_literal(right) {
        (left.as_ref(), right.as_ref(), op.clone())
    } else if expression_is_literal(left) {
        let Some(op) = reverse_predicate_op(op.clone()) else {
            return unsupported("WHERE comparison operator is not supported");
        };
        (right.as_ref(), left.as_ref(), op)
    } else {
        return unsupported("WHERE comparison must compare a catalog column to a literal");
    };
    let Some(column) = expression_catalog_column(column_expr, catalog) else {
        return unsupported("WHERE column must reference a registered relation column");
    };
    if !predicate_column_is_runtime_visible(column, key_column, value_column) {
        return unsupported(
            "WHERE column must be the primary key or value column for this materialized runtime",
        );
    }
    let Some(op) = predicate_op(op) else {
        return unsupported("WHERE comparison operator is not supported");
    };
    let literal = predicate_literal(literal_expr)?;
    Ok(Some(RowPredicate {
        column_id: column.column_id.clone(),
        op,
        literal,
    }))
}

fn expression_catalog_column<'a>(
    expr: &Expr,
    catalog: &'a VelorixRelationCatalogV1,
) -> Option<&'a RelationColumnV1> {
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| expression_references_column(expr, column))
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

fn validate_group_by_key(
    select: &Select,
    key_column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    let GroupByExpr::Expressions(expressions, modifiers) = &select.group_by else {
        return unsupported("GROUP BY ALL is not supported");
    };
    if !modifiers.is_empty() {
        return unsupported("GROUP BY modifiers are not supported");
    }
    let [group_key] = expressions.as_slice() else {
        return unsupported("expected exactly one GROUP BY key");
    };
    if expression_references_column(group_key, key_column) {
        Ok(())
    } else {
        unsupported("GROUP BY key must be the catalog primary key column")
    }
}

fn validate_projection<'a>(
    select: &Select,
    catalog: &'a VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
) -> Result<ValidatedAggregateProjection<'a>, ViewPlanError> {
    let [key, aggregates @ ..] = select.projection.as_slice() else {
        return unsupported("expected projection: key, aggregate...");
    };
    if !select_item_references_column(key, key_column) {
        return unsupported("first projection must be the primary key column");
    }

    if aggregates.is_empty() {
        return unsupported("expected at least one aggregate projection");
    }

    let mut output_ids = BTreeSet::new();
    let mut value_column: Option<&RelationColumnV1> = None;
    let mut aggregate_outputs = Vec::with_capacity(aggregates.len());

    for item in aggregates {
        let aggregate = select_item_aggregate(item, catalog)?;
        if !output_ids.insert(aggregate.output.output_column_id.clone()) {
            return unsupported("aggregate output column ids must be unique");
        }
        if let Some(column) = aggregate.input_column {
            validate_numeric_sum_column(catalog, column)?;
            if !matches!(column.physical_arrow_type, ArrowPhysicalTypeV1::Int64) {
                return unsupported(
                    "tumbling aggregate value column currently supports Int64 only",
                );
            }
            if aggregate.output.function == LogicalPlanAggregateFunctionV1::Avg {
                validate_numeric_avg_column(column)?;
            }
            if let Some(existing) = value_column {
                if existing.column_id != column.column_id {
                    return unsupported(
                        "single-key aggregate runtime currently requires all value aggregates to use the same input column",
                    );
                }
            } else {
                value_column = Some(column);
            }
        }
        aggregate_outputs.push(aggregate.output);
    }

    let Some(value_column) = value_column else {
        return unsupported(
            "single-key aggregate runtime currently requires sum(value) or avg(value)",
        );
    };

    Ok(ValidatedAggregateProjection {
        value_column,
        aggregate_outputs,
    })
}

fn validate_tumbling_projection<'a>(
    select: &Select,
    catalog: &'a VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
) -> Result<ValidatedAggregateProjection<'a>, ViewPlanError> {
    let [key, window_start, window_end, aggregates @ ..] = select.projection.as_slice() else {
        return unsupported("expected projection: key, window_start, window_end, aggregate...");
    };
    if !select_item_references_column(key, key_column) {
        return unsupported("first tumbling projection must be the primary key column");
    }
    if !select_item_references_identifier(window_start, "window_start")
        || !select_item_references_identifier(window_end, "window_end")
    {
        return unsupported("tumbling projection must include window_start and window_end");
    }
    if aggregates.is_empty() {
        return unsupported("expected at least one tumbling aggregate projection");
    }

    let mut output_ids = BTreeSet::new();
    let mut value_column: Option<&RelationColumnV1> = None;
    let mut aggregate_outputs = Vec::with_capacity(aggregates.len());

    for item in aggregates {
        let aggregate = select_item_aggregate(item, catalog)?;
        if !output_ids.insert(aggregate.output.output_column_id.clone()) {
            return unsupported("aggregate output column ids must be unique");
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

    let Some(value_column) = value_column else {
        return unsupported(
            "tumbling aggregate runtime currently requires sum(value) or avg(value)",
        );
    };

    Ok(ValidatedAggregateProjection {
        value_column,
        aggregate_outputs,
    })
}

struct ValidatedAggregateProjection<'a> {
    value_column: &'a RelationColumnV1,
    aggregate_outputs: Vec<SupportedAggregateOutput>,
}

struct ParsedAggregateProjection<'a> {
    output: SupportedAggregateOutput,
    input_column: Option<&'a RelationColumnV1>,
}

struct ValidatedLatestByKeyProjection<'a> {
    value_column: &'a RelationColumnV1,
    ordering_column: &'a RelationColumnV1,
    output_value_column_id: String,
}

fn validate_latest_by_key_projection<'a>(
    select: &Select,
    catalog: &'a VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
) -> Result<ValidatedLatestByKeyProjection<'a>, ViewPlanError> {
    let [key, latest_value] = select.projection.as_slice() else {
        return unsupported("expected projection: key, arg_max(value, ordering)");
    };
    if !select_item_references_column(key, key_column) {
        return unsupported("first projection must be the primary key column");
    }
    let (expr, alias) = match latest_value {
        SelectItem::UnnamedExpr(expr) => (expr, None),
        SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.as_str())),
        _ => return unsupported("latest-by-key projection must be an expression"),
    };
    let Expr::Function(function) = expr else {
        return unsupported("latest-by-key projection must use arg_max(value, ordering)");
    };
    if !function_name_eq(&function.name, "arg_max")
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return unsupported("latest-by-key projection must use arg_max(value, ordering)");
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return unsupported("arg_max arguments must be a simple argument list");
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return unsupported("DISTINCT arg_max arguments and aggregate clauses are not supported");
    }
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(value)), FunctionArg::Unnamed(FunctionArgExpr::Expr(ordering))] =
        arguments.args.as_slice()
    else {
        return unsupported("arg_max requires value and ordering column arguments");
    };
    let Some(value_column) = expression_column(value, catalog) else {
        return unsupported("arg_max value must reference a registered relation column");
    };
    let Some(ordering_column) = expression_column(ordering, catalog) else {
        return unsupported("arg_max ordering must reference a registered relation column");
    };
    validate_latest_value_column(catalog, value_column)?;
    validate_latest_ordering_column(catalog, ordering_column)?;
    let output_value_column_id = alias.unwrap_or(value_column.name.as_str()).to_string();
    Ok(ValidatedLatestByKeyProjection {
        value_column,
        ordering_column,
        output_value_column_id,
    })
}

fn select_item_references_column(item: &SelectItem, column: &RelationColumnV1) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) => expression_references_column(expr, column),
        SelectItem::ExprWithAlias { expr, .. } => expression_references_column(expr, column),
        _ => false,
    }
}

fn select_item_references_identifier(item: &SelectItem, expected: &str) -> bool {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) => expr,
        SelectItem::ExprWithAlias { expr, alias } => {
            if !identifier_eq(alias.value.as_str(), expected) {
                return false;
            }
            expr
        }
        _ => return false,
    };
    expression_references_identifier(expr, expected)
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

fn validate_tumbling_group_by(
    select: &Select,
    key_column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    let GroupByExpr::Expressions(expressions, modifiers) = &select.group_by else {
        return unsupported("GROUP BY ALL is not supported");
    };
    if !modifiers.is_empty() {
        return unsupported("GROUP BY modifiers are not supported");
    }
    let [group_key, window_start, window_end] = expressions.as_slice() else {
        return unsupported("expected GROUP BY key, window_start, window_end");
    };
    if expression_references_column(group_key, key_column)
        && expression_references_identifier(window_start, "window_start")
        && expression_references_identifier(window_end, "window_end")
    {
        Ok(())
    } else {
        unsupported("tumbling GROUP BY must be key, window_start, window_end")
    }
}

fn select_item_aggregate<'a>(
    item: &SelectItem,
    catalog: &'a VelorixRelationCatalogV1,
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
        || function.filter.is_some()
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

    let FunctionArguments::List(arguments) = &function.args else {
        return unsupported("aggregate arguments must be a simple argument list");
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return unsupported("DISTINCT aggregate arguments and aggregate clauses are not supported");
    }

    match canonical_function.as_str() {
        "sum" | "avg" | "min" | "max" => {
            let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice()
            else {
                return unsupported("aggregate value functions require one column argument");
            };
            let Some(column) = expression_column(argument, catalog) else {
                return unsupported("aggregate input must reference a registered relation column");
            };
            let function = match canonical_function.as_str() {
                "sum" => LogicalPlanAggregateFunctionV1::Sum,
                "avg" => LogicalPlanAggregateFunctionV1::Avg,
                "min" => LogicalPlanAggregateFunctionV1::Min,
                "max" => LogicalPlanAggregateFunctionV1::Max,
                _ => unreachable!("validated aggregate function"),
            };
            Ok(ParsedAggregateProjection {
                output: SupportedAggregateOutput {
                    function,
                    input_column_id: Some(column.column_id.clone()),
                    output_column_id,
                },
                input_column: Some(column),
            })
        }
        "count" => {
            if !matches!(
                arguments.args.as_slice(),
                [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
            ) {
                return unsupported("count currently supports only count(*)");
            }
            Ok(ParsedAggregateProjection {
                output: SupportedAggregateOutput {
                    function: LogicalPlanAggregateFunctionV1::Count,
                    input_column_id: None,
                    output_column_id,
                },
                input_column: None,
            })
        }
        _ => unsupported("aggregate function is not supported by this materialized runtime"),
    }
}

fn select_item_sum_qualified_column<'a>(
    item: &SelectItem,
    qualifier: &str,
    catalog: &'a VelorixRelationCatalogV1,
) -> Option<&'a RelationColumnV1> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return None,
    };
    let Expr::Function(function) = expr else {
        return None;
    };
    if !function_name_eq(&function.name, "sum")
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return None;
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return None;
    };
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return None;
    }
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice() else {
        return None;
    };
    expression_qualified_column(argument, qualifier, catalog)
}

fn select_item_is_count_star(item: &SelectItem) -> bool {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return false,
    };
    let Expr::Function(function) = expr else {
        return false;
    };
    if !function_name_eq(&function.name, "count")
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
    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
        return false;
    }
    matches!(
        arguments.args.as_slice(),
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
    )
}

fn expression_references_column(expr: &Expr, column: &RelationColumnV1) -> bool {
    match expr {
        Expr::Identifier(ident) => column_identifier_eq(column, ident.value.as_str()),
        _ => false,
    }
}

fn expression_references_identifier(expr: &Expr, expected: &str) -> bool {
    matches!(expr, Expr::Identifier(ident) if identifier_eq(ident.value.as_str(), expected))
}

fn expression_column<'a>(
    expr: &Expr,
    catalog: &'a VelorixRelationCatalogV1,
) -> Option<&'a RelationColumnV1> {
    let Expr::Identifier(ident) = expr else {
        return None;
    };
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column_identifier_eq(column, ident.value.as_str()))
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

fn expression_qualified_column<'a>(
    expr: &Expr,
    qualifier: &str,
    catalog: &'a VelorixRelationCatalogV1,
) -> Option<&'a RelationColumnV1> {
    let Ok(reference) = qualified_column_ref(expr) else {
        return None;
    };
    if !identifier_eq(reference.qualifier.as_str(), qualifier) {
        return None;
    }
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column_identifier_eq(column, reference.column.as_str()))
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
        ArrowPhysicalTypeV1::Int64 => Ok(()),
        _ => unsupported("avg(value_column) currently supports Int64 columns"),
    }
}

fn validate_latest_value_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    if column.column_id == catalog.relation_schema.weight_column_id {
        return unsupported("arg_max value must not reference the weight column");
    }
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Boolean
        | ArrowPhysicalTypeV1::Utf8
        | ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Float64
        | ArrowPhysicalTypeV1::Decimal128 { .. } => Ok(()),
        _ => unsupported("arg_max value column type is not supported by latest-by-key runtime"),
    }
}

fn validate_latest_ordering_column(
    catalog: &VelorixRelationCatalogV1,
    column: &RelationColumnV1,
) -> Result<(), ViewPlanError> {
    if column.column_id == catalog.relation_schema.weight_column_id {
        return unsupported("arg_max ordering must not reference the weight column");
    }
    match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64
        | ArrowPhysicalTypeV1::Date32
        | ArrowPhysicalTypeV1::TimestampNanosecond { .. } => Ok(()),
        _ => {
            unsupported("arg_max ordering currently supports Int64, Date32, or TimestampNanosecond")
        }
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
