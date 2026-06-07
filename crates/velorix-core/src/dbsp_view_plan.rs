use datafusion::sql::{
    parser::{DFParser, Statement as DataFusionStatement},
    sqlparser::ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, ObjectName, Query,
        Select, SelectItem, SetExpr, Statement as SqlStatement, TableFactor,
    },
};
use thiserror::Error;

use crate::relation::{
    RelationColumnV1, RelationSchemaError, RelationSemanticRoleV1, SupportedIncrementalAdapterSpec,
    VelorixRelationCatalogV1,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupportedDbspViewPlan {
    pub input_relation_id: String,
    pub group_key_column_id: String,
    pub sum_value_column_id: String,
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
    validate_group_by_key(select, key_column)?;
    validate_projection(select, key_column, value_column)?;

    Ok(SupportedDbspViewPlan {
        input_relation_id: catalog.relation_schema.relation_id.clone(),
        group_key_column_id: key_column.column_id.clone(),
        sum_value_column_id: value_column.column_id.clone(),
    })
}

pub fn validate_catalog_backed_sum_count_view_sql(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedDbspViewPlan, DbspViewPlanError> {
    validate_supported_dbsp_view_sql(sql, catalog)
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

fn validate_from_relation(
    select: &Select,
    catalog: &VelorixRelationCatalogV1,
) -> Result<(), DbspViewPlanError> {
    if select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || select.selection.is_some()
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
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|ident| column_identifier_eq(column, ident.value.as_str())),
        _ => false,
    }
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
