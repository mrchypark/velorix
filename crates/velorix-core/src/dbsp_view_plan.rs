//! Temporary SQL-shape recognizers for linked fixture runtimes.
//!
//! This module is not a Feldera SQL compiler, a generic DBSP planner, or a
//! product-complete materialized-view admission path. Product-defined views
//! should move through the Feldera compiler-backed artifact/runtime boundary.

use datafusion::sql::{
    parser::{DFParser, Statement as DataFusionStatement},
    sqlparser::ast::{
        BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
        JoinConstraint, JoinOperator, ObjectName, Query, Select, SelectItem, SetExpr,
        Statement as SqlStatement, TableFactor, UnaryOperator, Value as SqlValue,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Number as JsonNumber, Value as JsonValue};
use thiserror::Error;

use crate::relation::{
    RelationColumnV1, RelationSchemaError, RelationSemanticRoleV1, SupportedIncrementalAdapterSpec,
    VelorixRelationCatalogV1,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedDbspViewPlan {
    pub input_relation_id: String,
    pub group_key_column_id: String,
    pub sum_value_column_id: String,
    pub predicate: Option<DbspRowPredicate>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedDbspJoinViewPlan {
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
pub struct DbspRowPredicate {
    pub column_id: String,
    pub op: DbspPredicateOp,
    pub literal: JsonValue,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DbspPredicateOp {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

#[derive(Debug, Error)]
pub enum DbspViewPlanError {
    #[error(transparent)]
    Relation(#[from] RelationSchemaError),
    #[error("DBSP view SQL parse error: {0}")]
    Parse(#[from] datafusion::error::DataFusionError),
    #[error("DBSP view SQL is outside the supported materialization scope: {reason}")]
    UnsupportedShape { reason: String },
}

pub fn validate_supported_dbsp_view_sql(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedDbspViewPlan, DbspViewPlanError> {
    let adapter = catalog.validate_supported_incremental_adapter_scope()?;
    if adapter != SupportedIncrementalAdapterSpec::ScalarSumCount {
        return unsupported("DBSP view SQL currently supports scalar single-key sum/count views");
    }
    let key_column = single_semantic_column(catalog, RelationSemanticRoleV1::PrimaryKey)?;
    let value_column = single_semantic_column(catalog, RelationSemanticRoleV1::Value)?;
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
    let predicate = validate_selection(select, catalog, key_column, value_column)?;
    validate_group_by_key(select, key_column)?;
    validate_projection(select, key_column, value_column)?;

    Ok(SupportedDbspViewPlan {
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        group_key_column_id: key_column.column_id.clone(),
        sum_value_column_id: value_column.column_id.clone(),
        predicate,
    })
}

pub fn validate_catalog_backed_sum_count_view_sql(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedDbspViewPlan, DbspViewPlanError> {
    validate_supported_dbsp_view_sql(sql, catalog)
}

pub fn validate_supported_dbsp_join_view_sql(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<SupportedDbspJoinViewPlan, DbspViewPlanError> {
    let [left_catalog, right_catalog] = catalogs else {
        return unsupported("DBSP join view SQL currently requires exactly two input relations");
    };
    for catalog in [left_catalog, right_catalog] {
        let adapter = catalog.validate_supported_incremental_adapter_scope()?;
        if adapter != SupportedIncrementalAdapterSpec::ScalarSumCount {
            return unsupported("DBSP join view SQL currently supports scalar sum/count inputs");
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
        return unsupported("WHERE is not supported for DBSP join materialization yet");
    }

    let JoinSqlBindings {
        left_catalog,
        right_catalog,
        left_alias,
        right_alias,
        left_join_column,
        right_join_column,
    } = validate_two_input_join(select, left_catalog, right_catalog)?;
    let left_key = single_semantic_column(left_catalog, RelationSemanticRoleV1::PrimaryKey)?;
    let right_key = single_semantic_column(right_catalog, RelationSemanticRoleV1::PrimaryKey)?;
    if left_join_column.column_id != left_key.column_id
        || right_join_column.column_id != right_key.column_id
    {
        return unsupported("JOIN ON must compare the primary key columns of both inputs");
    }
    if left_join_column.physical_arrow_type != right_join_column.physical_arrow_type {
        return unsupported("JOIN ON primary key columns must have identical physical Arrow types");
    }
    let left_value = single_semantic_column(left_catalog, RelationSemanticRoleV1::Value)?;
    validate_join_group_by_key(select, &right_alias, right_catalog, right_key)?;
    validate_join_projection(select, &right_alias, right_key, &left_alias, left_value)?;

    Ok(SupportedDbspJoinViewPlan {
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

fn supported_plain_select(query: &Query) -> Result<&Select, DbspViewPlanError> {
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
        return unsupported("query-level clauses are not supported for DBSP materialization");
    }
    match query.body.as_ref() {
        SetExpr::Select(select) => Ok(select),
        _ => unsupported("set operations, VALUES, and nested queries are not supported"),
    }
}

fn validate_plain_select_clauses(select: &Select) -> Result<(), DbspViewPlanError> {
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
) -> Result<(), DbspViewPlanError> {
    validate_plain_select_clauses(select)?;

    let [table] = select.from.as_slice() else {
        return unsupported("expected exactly one input relation");
    };
    if !table.joins.is_empty() {
        return unsupported("joins are not supported for DBSP materialization yet");
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
) -> Result<JoinSqlBindings<'a>, DbspViewPlanError> {
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
        _ => return unsupported("only INNER JOIN is supported for DBSP join materialization"),
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

fn table_ref(factor: &TableFactor, side: &'static str) -> Result<SqlTableRef, DbspViewPlanError> {
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
) -> Result<&'a VelorixRelationCatalogV1, DbspViewPlanError> {
    [first, second]
        .into_iter()
        .find(|catalog| relation_identifier_matches(catalog, table.name.as_str()))
        .ok_or_else(|| DbspViewPlanError::UnsupportedShape {
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

fn qualified_column_ref(expr: &Expr) -> Result<QualifiedColumnRef, DbspViewPlanError> {
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
) -> Result<(QualifiedColumnRef, QualifiedColumnRef), DbspViewPlanError> {
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
) -> Result<&'a RelationColumnV1, DbspViewPlanError> {
    catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column_identifier_eq(column, reference.column.as_str()))
        .ok_or_else(|| DbspViewPlanError::UnsupportedShape {
            reason: "qualified column must reference a registered relation column".to_string(),
        })
}

fn validate_join_group_by_key(
    select: &Select,
    right_alias: &str,
    right_catalog: &VelorixRelationCatalogV1,
    right_key: &RelationColumnV1,
) -> Result<(), DbspViewPlanError> {
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

fn validate_join_projection(
    select: &Select,
    right_alias: &str,
    right_key: &RelationColumnV1,
    left_alias: &str,
    left_value: &RelationColumnV1,
) -> Result<(), DbspViewPlanError> {
    let [key, sum, count] = select.projection.as_slice() else {
        return unsupported("expected projection: key, sum(value), count(*)");
    };
    if !select_item_references_qualified_column(key, right_alias, right_key) {
        return unsupported("first projection must be the right input primary key column");
    }
    if !select_item_is_sum_of_qualified_column(sum, left_alias, left_value) {
        return unsupported("second projection must be sum(left_value_column)");
    }
    if !select_item_is_count_star(count) {
        return unsupported("third projection must be count(*)");
    }
    Ok(())
}

fn validate_selection(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
) -> Result<Option<DbspRowPredicate>, DbspViewPlanError> {
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
            "WHERE column must be the primary key or value column for this generated runtime",
        );
    }
    let Some(op) = predicate_op(op) else {
        return unsupported("WHERE comparison operator is not supported");
    };
    let literal = predicate_literal(literal_expr)?;
    Ok(Some(DbspRowPredicate {
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

fn predicate_op(op: BinaryOperator) -> Option<DbspPredicateOp> {
    match op {
        BinaryOperator::Eq => Some(DbspPredicateOp::Eq),
        BinaryOperator::NotEq => Some(DbspPredicateOp::NotEq),
        BinaryOperator::Gt => Some(DbspPredicateOp::Gt),
        BinaryOperator::GtEq => Some(DbspPredicateOp::GtEq),
        BinaryOperator::Lt => Some(DbspPredicateOp::Lt),
        BinaryOperator::LtEq => Some(DbspPredicateOp::LtEq),
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

fn predicate_literal(expr: &Expr) -> Result<JsonValue, DbspViewPlanError> {
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

fn predicate_literal_value(
    value: &SqlValue,
    negative: bool,
) -> Result<JsonValue, DbspViewPlanError> {
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
                DbspViewPlanError::UnsupportedShape {
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
) -> Result<(), DbspViewPlanError> {
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

fn validate_projection(
    select: &Select,
    key_column: &RelationColumnV1,
    value_column: &RelationColumnV1,
) -> Result<(), DbspViewPlanError> {
    let [key, sum, count] = select.projection.as_slice() else {
        return unsupported("expected projection: key, sum(value), count(*)");
    };
    if !select_item_references_column(key, key_column) {
        return unsupported("first projection must be the primary key column");
    }
    if !select_item_is_sum_of_column(sum, value_column) {
        return unsupported("second projection must be sum(value_column)");
    }
    if !select_item_is_count_star(count) {
        return unsupported("third projection must be count(*)");
    }
    Ok(())
}

fn select_item_references_column(item: &SelectItem, column: &RelationColumnV1) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) => expression_references_column(expr, column),
        SelectItem::ExprWithAlias { expr, .. } => expression_references_column(expr, column),
        _ => false,
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

fn select_item_is_sum_of_column(item: &SelectItem, column: &RelationColumnV1) -> bool {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return false,
    };
    let Expr::Function(function) = expr else {
        return false;
    };
    if !function_name_eq(&function.name, "sum")
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
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice() else {
        return false;
    };
    expression_references_column(argument, column)
}

fn select_item_is_sum_of_qualified_column(
    item: &SelectItem,
    qualifier: &str,
    column: &RelationColumnV1,
) -> bool {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return false,
    };
    let Expr::Function(function) = expr else {
        return false;
    };
    if !function_name_eq(&function.name, "sum")
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
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice() else {
        return false;
    };
    expression_references_qualified_column(argument, qualifier, column)
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

fn single_semantic_column(
    catalog: &VelorixRelationCatalogV1,
    role: RelationSemanticRoleV1,
) -> Result<&RelationColumnV1, DbspViewPlanError> {
    let mut columns = catalog
        .relation_schema
        .columns
        .iter()
        .filter(|column| column.semantic_role == role);
    let Some(column) = columns.next() else {
        return unsupported(format!("relation catalog is missing {role:?} column"));
    };
    if columns.next().is_some() {
        return unsupported(format!(
            "relation catalog has multiple {role:?} columns, which is unsupported"
        ));
    }
    Ok(column)
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

fn unsupported<T>(reason: impl Into<String>) -> Result<T, DbspViewPlanError> {
    Err(DbspViewPlanError::UnsupportedShape {
        reason: reason.into(),
    })
}
