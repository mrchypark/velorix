//! Schema-bound series expansion and bag-preserving UNION ALL stages.

use super::*;

/// Admission default; callers may choose a smaller or larger explicit budget.
pub const DEFAULT_SERIES_MAX_EMITTED_ROWS_PER_INPUT: u64 = 100_000;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedSeriesPlanV1 {
    pub schema_version: u32,
    pub input_schema: RelationSchema,
    /// Catalog column ID to public schema name; excludes the physical weight.
    pub input_column_names: BTreeMap<String, String>,
    pub start: TypedExprProgramV1,
    pub end: TypedExprProgramV1,
    /// Int64 units for integer series, fixed-duration nanoseconds for timestamps.
    pub step: TypedExprProgramV1,
    pub generated_column_id: String,
    pub projection: Vec<SeriesProjectionV1>,
    pub output_columns: Vec<ColumnSchema>,
    pub max_emitted_rows_per_input: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SeriesProjectionV1 {
    pub output_name: String,
    /// None selects the generated value; Some selects a registered source column.
    pub input_column_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedUnionAllPlanV1 {
    pub schema_version: u32,
    /// SQL encounter order is the branch identity, including repeated relations.
    pub branches: Vec<UnionAllBranchV1>,
    pub output_columns: Vec<ColumnSchema>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UnionAllBranchV1 {
    pub input_schema: RelationSchema,
    pub input_column_names: BTreeMap<String, String>,
    /// Positional expressions; names come exclusively from the first branch.
    pub projection: Vec<TypedExprProgramV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SetOperationInputV1 {
    pub port_id: String,
    pub node_id: String,
    pub required_columns: Vec<LogicalPlanColumnRef>,
}

pub fn validate_supported_series_sql(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedSeriesPlanV1, ViewPlanError> {
    validate_supported_series_sql_with_budget(
        sql,
        catalog,
        DEFAULT_SERIES_MAX_EMITTED_ROWS_PER_INPUT,
    )
}

pub fn validate_supported_series_sql_with_budget(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    max_emitted_rows_per_input: u64,
) -> Result<SupportedSeriesPlanV1, ViewPlanError> {
    catalog.validate()?;
    if max_emitted_rows_per_input == 0 {
        return unsupported("series expansion budget must be positive");
    }
    let query = parse_single_query(sql)?;
    validate_stage_query(&query)?;
    let SetExpr::Select(select) = query.body.as_ref() else {
        return unsupported("series requires a SELECT stage");
    };
    validate_stage_select(select)?;
    let (source, series) = match select.from.as_slice() {
        [source] if source.joins.len() == 1 => {
            let join = &source.joins[0];
            if join.global
                || !matches!(
                    join.join_operator,
                    JoinOperator::CrossJoin(JoinConstraint::None)
                )
            {
                return unsupported("series requires CROSS JOIN LATERAL");
            }
            (&source.relation, &join.relation)
        }
        [source, series] if source.joins.is_empty() && series.joins.is_empty() => {
            (&source.relation, &series.relation)
        }
        _ => return unsupported("series requires one source and one lateral generate_series"),
    };
    let table = stage_table(source)?;
    if table.name != catalog.relation_schema.relation_id {
        return unsupported("series source is not the bound registered relation");
    }
    let TableFactor::Function {
        lateral: true,
        name,
        args,
        with_ordinality: false,
        alias: Some(alias),
    } = series
    else {
        return unsupported("series requires LATERAL generate_series(...) AS alias(column)");
    };
    if !single_object_name_identifier(name)
        .is_some_and(|name| name.eq_ignore_ascii_case("generate_series"))
        || alias.columns.len() != 1
        || alias.columns[0].data_type.is_some()
        || alias.at.is_some()
        || alias.name.value.eq_ignore_ascii_case(&table.alias)
    {
        return unsupported("invalid generate_series function or alias");
    }
    let [start, end, step] = args.as_slice() else {
        return unsupported("generate_series requires start, end and step");
    };
    let start = stage_expr(function_arg(start)?, catalog, &table.alias)?;
    let end = stage_expr(function_arg(end)?, catalog, &table.alias)?;
    if !matches!(
        start.root.result_type,
        RuntimeScalarTypeV1::Int64 | RuntimeScalarTypeV1::TimestampNanosecond
    ) || start.root.result_type != end.root.result_type
    {
        return unsupported("series bounds must have the same Int64 or TimestampNanosecond type");
    }
    let step_expr = function_arg(step)?;
    let step = if let Expr::Interval(interval) = step_expr {
        if start.root.result_type != RuntimeScalarTypeV1::TimestampNanosecond
            || interval.last_field.is_some()
            || interval.leading_precision.is_some()
            || interval.fractional_seconds_precision.is_some()
        {
            return unsupported("timestamp series requires a fixed-duration interval");
        }
        // Reject extra tokens instead of letting the shared interval helper truncate them.
        if let Expr::Value(ValueWithSpan {
            value: SqlValue::SingleQuotedString(text),
            ..
        }) = interval.value.as_ref()
        {
            let count = text.split_whitespace().count();
            if count == 0
                || count
                    > if interval.leading_field.is_some() {
                        1
                    } else {
                        2
                    }
            {
                return unsupported("series interval must contain one number and one fixed unit");
            }
        }
        let Some(Some(value)) = typed_interval_nanoseconds(interval) else {
            return unsupported("series interval is invalid or overflows nanoseconds");
        };
        TypedExprProgramV1 {
            encoding_version: TYPED_EXPR_PROGRAM_SCHEMA_VERSION_V1,
            root: TypedExprNodeV1 {
                result_type: RuntimeScalarTypeV1::Int64,
                nullable: false,
                kind: TypedExprKindV1::Literal {
                    value: ScalarLiteralV1::Int64(value),
                },
            },
        }
    } else {
        stage_expr(step_expr, catalog, &table.alias)?
    };
    if step.root.result_type != RuntimeScalarTypeV1::Int64
        || matches!(
            step.root.kind,
            TypedExprKindV1::Literal {
                value: ScalarLiteralV1::Int64(0)
            }
        )
    {
        return unsupported("series step must be a nonzero Int64 (nanoseconds for timestamps)");
    }
    let input_schema = stage_input_schema(catalog)?;
    let generated_column_id = alias.columns[0].name.value.clone();
    let mut projection = Vec::new();
    let mut output_columns = Vec::new();
    for item in &select.projection {
        let (expr, output_name) = projection_item(item)?;
        let generated = match expr {
            Expr::Identifier(id) if id.value == generated_column_id => {
                if input_schema
                    .columns
                    .iter()
                    .any(|column| column.name == id.value)
                {
                    return unsupported("ambiguous generated column; qualify it with its alias");
                }
                true
            }
            Expr::CompoundIdentifier(ids) if ids.len() == 2 => {
                ids[0].value == alias.name.value && ids[1].value == generated_column_id
            }
            _ => false,
        };
        let (input_column_id, data_type, nullable) = if generated {
            (None, scalar_sql_type(&start.root.result_type)?, false)
        } else {
            let column = catalog_column_from_expr(expr, catalog, Some(&table.alias))?;
            if column.column_id == catalog.relation_schema.weight_column_id {
                return unsupported("series projections must not reference the physical weight");
            }
            let source = input_schema
                .columns
                .iter()
                .find(|source| source.name == column.name)
                .ok_or_else(|| stage_error("series projection must be a public source column"))?;
            (
                Some(column.column_id.clone()),
                source.data_type.clone(),
                source.nullable,
            )
        };
        projection.push(SeriesProjectionV1 {
            output_name: output_name.clone(),
            input_column_id,
        });
        output_columns.push(ColumnSchema {
            name: output_name,
            data_type,
            nullable,
        });
    }
    validate_output_columns(&output_columns)?;
    let plan = SupportedSeriesPlanV1 {
        schema_version: 1,
        input_schema,
        input_column_names: input_column_names(catalog),
        start,
        end,
        step,
        generated_column_id,
        projection,
        output_columns,
        max_emitted_rows_per_input,
    };
    validate_series_plan(&plan)?;
    Ok(plan)
}

pub fn validate_supported_union_all_sql(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<SupportedUnionAllPlanV1, ViewPlanError> {
    let mut catalog_ids = BTreeSet::new();
    for catalog in catalogs {
        catalog.validate()?;
        if !catalog_ids.insert(catalog.relation_schema.relation_id.as_str()) {
            return unsupported("UNION ALL catalogs must have unique relation identities");
        }
    }
    let query = parse_single_query(sql)?;
    validate_stage_query(&query)?;
    let mut selects = Vec::new();
    union_selects(&query.body, &mut selects)?;
    if selects.len() < 2 {
        return unsupported("UNION ALL requires at least two SELECT branches");
    }
    let mut branches = Vec::new();
    let mut output_columns: Vec<ColumnSchema> = Vec::new();
    for select in selects {
        validate_stage_select(select)?;
        let [from] = select.from.as_slice() else {
            return unsupported("each UNION ALL branch requires one registered relation");
        };
        if !from.joins.is_empty() {
            return unsupported("join inputs must be declared as separate stages before UNION ALL");
        }
        let table = stage_table(&from.relation)?;
        let catalog = catalogs
            .iter()
            .find(|catalog| catalog.relation_schema.relation_id == table.name)
            .ok_or_else(|| stage_error("UNION ALL branch relation is not registered"))?;
        let mut projection = Vec::new();
        let mut columns = Vec::new();
        for item in &select.projection {
            let (expr, name) = projection_item(item)?;
            let program = stage_expr(expr, catalog, &table.alias)?;
            columns.push(ColumnSchema {
                name,
                data_type: scalar_sql_type(&program.root.result_type)?,
                nullable: program.root.nullable,
            });
            projection.push(program);
        }
        if branches.is_empty() {
            validate_output_columns(&columns)?;
            output_columns = columns;
        } else {
            if columns.len() != output_columns.len() {
                return unsupported("UNION ALL branches must have equal projection arity");
            }
            for (output, column) in output_columns.iter_mut().zip(columns) {
                if output.data_type != column.data_type {
                    return unsupported("UNION ALL positional types must match; use explicit casts in a preceding stage");
                }
                output.nullable |= column.nullable;
            }
        }
        branches.push(UnionAllBranchV1 {
            input_schema: stage_input_schema(catalog)?,
            input_column_names: input_column_names(catalog),
            projection,
        });
    }
    if branches
        .iter()
        .map(|branch| branch.input_schema.relation_id.as_str())
        .collect::<BTreeSet<_>>()
        != catalog_ids
    {
        return unsupported("UNION ALL catalogs must match the referenced input relations exactly");
    }
    let plan = SupportedUnionAllPlanV1 {
        schema_version: 1,
        branches,
        output_columns,
    };
    validate_union_plan(&plan)?;
    Ok(plan)
}

pub fn supported_series_output_columns(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<Vec<ColumnSchema>, ViewPlanError> {
    Ok(validate_supported_series_sql(sql, catalog)?.output_columns)
}

pub fn supported_union_all_output_columns(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
) -> Result<Vec<ColumnSchema>, ViewPlanError> {
    Ok(validate_supported_union_all_sql(sql, catalogs)?.output_columns)
}

fn stage_error(reason: &str) -> ViewPlanError {
    ViewPlanError::UnsupportedShape {
        reason: reason.into(),
    }
}

fn validate_stage_query(query: &Query) -> Result<(), ViewPlanError> {
    validate_query_level_clauses(query, false)?;
    if query.order_by.is_some() {
        return unsupported("set/series stage ordering is unsupported");
    }
    Ok(())
}

fn validate_stage_select(select: &Select) -> Result<(), ViewPlanError> {
    validate_plain_select_clauses(select)?;
    if select.selection.is_some()
        || select.distinct.is_some()
        || !group_by_is_empty(&select.group_by)
        || select.exclude.is_some()
        || !select.optimizer_hints.is_empty()
    {
        return unsupported("set/series stages require plain projections; declare filters and aggregates as separate views");
    }
    Ok(())
}

fn stage_table(factor: &TableFactor) -> Result<SqlTableRef, ViewPlanError> {
    if let TableFactor::Table {
        alias: Some(alias), ..
    } = factor
    {
        if !alias.columns.is_empty() || alias.at.is_some() {
            return unsupported("source table column aliases are unsupported");
        }
    }
    registered_table_ref(factor, "stage")
}

fn stage_input_schema(catalog: &VelorixRelationCatalogV1) -> Result<RelationSchema, ViewPlanError> {
    catalog_input_relation_schema(catalog).map_err(|error| stage_error(&error.to_string()))
}

fn input_column_names(catalog: &VelorixRelationCatalogV1) -> BTreeMap<String, String> {
    catalog
        .relation_schema
        .columns
        .iter()
        .filter(|column| column.column_id != catalog.relation_schema.weight_column_id)
        .map(|column| (column.column_id.clone(), column.name.clone()))
        .collect()
}

fn stage_expr(
    expr: &Expr,
    catalog: &VelorixRelationCatalogV1,
    alias: &str,
) -> Result<TypedExprProgramV1, ViewPlanError> {
    if let Expr::UnaryOp {
        op: UnaryOperator::Minus,
        expr,
    } = expr
    {
        if let Expr::Value(ValueWithSpan {
            value: SqlValue::Number(number, _),
            ..
        }) = expr.as_ref()
        {
            if let Ok(value) = format!("-{number}").parse::<i64>() {
                return Ok(TypedExprProgramV1 {
                    encoding_version: TYPED_EXPR_PROGRAM_SCHEMA_VERSION_V1,
                    root: TypedExprNodeV1 {
                        result_type: RuntimeScalarTypeV1::Int64,
                        nullable: false,
                        kind: TypedExprKindV1::Literal {
                            value: ScalarLiteralV1::Int64(value),
                        },
                    },
                });
            }
        }
    }
    let root = typed_expr_node_from_expr_in_context(expr, catalog, Some(alias), false)?
        .ok_or_else(|| stage_error("stage expression is outside the typed scalar contract"))?;
    let program = TypedExprProgramV1 {
        encoding_version: TYPED_EXPR_PROGRAM_SCHEMA_VERSION_V1,
        root,
    };
    validate_bound_program(
        &program,
        &stage_input_schema(catalog)?,
        &input_column_names(catalog),
    )?;
    Ok(program)
}

fn function_arg(arg: &FunctionArg) -> Result<&Expr, ViewPlanError> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
        _ => unsupported("series arguments must be positional scalar expressions"),
    }
}

fn projection_item(item: &SelectItem) -> Result<(&Expr, String), ViewPlanError> {
    match item {
        SelectItem::ExprWithAlias { expr, alias } => Ok((expr, alias.value.clone())),
        SelectItem::UnnamedExpr(expr @ Expr::Identifier(id)) => Ok((expr, id.value.clone())),
        SelectItem::UnnamedExpr(expr @ Expr::CompoundIdentifier(ids)) if !ids.is_empty() => {
            Ok((expr, ids.last().unwrap().value.clone()))
        }
        _ => unsupported("stage projections require explicit columns or aliased expressions"),
    }
}

fn union_selects<'a>(body: &'a SetExpr, out: &mut Vec<&'a Select>) -> Result<(), ViewPlanError> {
    match body {
        SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: SetQuantifier::All,
            left,
            right,
        } => {
            union_selects(left, out)?;
            union_selects(right, out)
        }
        SetExpr::Select(select) => {
            out.push(select);
            Ok(())
        }
        SetExpr::Query(query) => {
            validate_stage_query(query)?;
            union_selects(&query.body, out)
        }
        _ => unsupported("only positional UNION ALL of SELECT branches is supported"),
    }
}

pub(super) fn scalar_sql_type(value: &RuntimeScalarTypeV1) -> Result<SqlDataType, ViewPlanError> {
    Ok(match value {
        RuntimeScalarTypeV1::Boolean => SqlDataType::Bool,
        RuntimeScalarTypeV1::Int64 => SqlDataType::Int64,
        RuntimeScalarTypeV1::Float64 => SqlDataType::Float64,
        RuntimeScalarTypeV1::Decimal128 { precision, scale } => SqlDataType::Decimal {
            precision: *precision,
            scale: u8::try_from(*scale)
                .map_err(|_| stage_error("negative decimal scale is unsupported"))?,
        },
        RuntimeScalarTypeV1::Utf8 => SqlDataType::Utf8,
        RuntimeScalarTypeV1::Date32 => SqlDataType::Date,
        RuntimeScalarTypeV1::TimestampNanosecond => SqlDataType::Timestamp { timezone: None },
    })
}

fn validate_output_columns(columns: &[ColumnSchema]) -> Result<(), ViewPlanError> {
    let mut names = BTreeSet::new();
    if columns.is_empty()
        || columns
            .iter()
            .any(|column| column.name.trim().is_empty() || !names.insert(&column.name))
    {
        return unsupported("stage output requires nonempty unique column names");
    }
    Ok(())
}

pub fn lower_supported_series_sql_to_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    lower_supported_series_sql_to_logical_plan_with_budget(
        sql,
        catalog,
        output_schema,
        DEFAULT_SERIES_MAX_EMITTED_ROWS_PER_INPUT,
    )
}

pub fn lower_supported_series_sql_to_logical_plan_with_budget(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
    max_emitted_rows_per_input: u64,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported =
        validate_supported_series_sql_with_budget(sql, catalog, max_emitted_rows_per_input)?;
    validate_stage_output(output_schema, &supported.output_columns)?;
    finalize_logical_plan(stage_logical_plan(
        sql,
        output_schema,
        VelorixLogicalViewExecutionV1::Series {
            plan: Box::new(supported),
        },
    )?)
}

pub fn lower_supported_union_all_sql_to_logical_plan(
    sql: &str,
    catalogs: &[VelorixRelationCatalogV1],
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let supported = validate_supported_union_all_sql(sql, catalogs)?;
    validate_stage_output(output_schema, &supported.output_columns)?;
    finalize_logical_plan(stage_logical_plan(
        sql,
        output_schema,
        VelorixLogicalViewExecutionV1::UnionAll {
            plan: Box::new(supported),
        },
    )?)
}

pub(super) fn validate_stage_output(
    schema: &RelationSchema,
    columns: &[ColumnSchema],
) -> Result<(), ViewPlanError> {
    if schema.columns != columns
        || !schema.primary_key.is_empty()
        || schema.relation_id.trim().is_empty()
        || schema.relation_name.trim().is_empty()
        || schema.relation_version.trim().is_empty()
        || schema.schema_fingerprint.trim().is_empty()
    {
        return unsupported(
            "stage output must match inferred columns and bag semantics (empty primary key)",
        );
    }
    Ok(())
}

pub(super) fn schema_columns(schema: &RelationSchema) -> Vec<LogicalPlanColumnRef> {
    schema
        .columns
        .iter()
        .map(|column| column_ref(&schema.relation_id, &column.name))
        .collect()
}

pub(super) fn stage_logical_plan(
    sql: &str,
    output_schema: &RelationSchema,
    execution: VelorixLogicalViewExecutionV1,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let output_relation = logical_relation_from_schema(output_schema);
    let mut nodes = Vec::new();
    let mut input_relations = Vec::new();
    let stage_id = match &execution {
        VelorixLogicalViewExecutionV1::TypedMap { plan } => {
            let relation = logical_relation_from_schema(&plan.input_schema);
            input_relations.push(relation.clone());
            nodes.push(VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: "scan_input".into(),
                relation,
            });
            nodes.push(VelorixLogicalViewPlanNodeV1::TypedMap {
                node_id: "typed_map".into(),
                input: "scan_input".into(),
                plan: plan.clone(),
                output_schema: output_schema.clone(),
            });
            "typed_map"
        }
        VelorixLogicalViewExecutionV1::Series { plan } => {
            let relation = logical_relation_from_schema(&plan.input_schema);
            input_relations.push(relation.clone());
            nodes.push(VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: "scan_input".into(),
                relation,
            });
            nodes.push(VelorixLogicalViewPlanNodeV1::Series {
                node_id: "series".into(),
                input: "scan_input".into(),
                plan: plan.clone(),
                output_schema: output_schema.clone(),
            });
            "series"
        }
        VelorixLogicalViewExecutionV1::UnionAll { plan } => {
            let mut inputs = Vec::new();
            for (index, branch) in plan.branches.iter().enumerate() {
                let relation = logical_relation_from_schema(&branch.input_schema);
                if !input_relations.contains(&relation) {
                    input_relations.push(relation.clone());
                }
                let node_id = format!("scan_{index}");
                nodes.push(VelorixLogicalViewPlanNodeV1::RelationScan {
                    node_id: node_id.clone(),
                    relation,
                });
                inputs.push(SetOperationInputV1 {
                    port_id: format!("branch_{index}"),
                    node_id,
                    required_columns: schema_columns(&branch.input_schema),
                });
            }
            nodes.push(VelorixLogicalViewPlanNodeV1::UnionAll {
                node_id: "union_all".into(),
                inputs,
                plan: plan.clone(),
                output_schema: output_schema.clone(),
            });
            "union_all"
        }
        _ => return invalid_logical_plan("expected typed-map, series or UNION ALL execution"),
    };
    nodes.push(VelorixLogicalViewPlanNodeV1::Output {
        node_id: "output_materialized_view".into(),
        input: stage_id.into(),
        relation: output_relation.clone(),
    });
    Ok(VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.into(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.into(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.into(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.into(),
        input_relations,
        output_relation,
        nodes,
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: stage_id.into(),
            state_kind: LogicalPlanStateKindV1::Projection,
            key_columns: Vec::new(),
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.into(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.into(),
        execution_implementation: None,
        execution,
    })
}

fn validate_program(program: &TypedExprProgramV1) -> Result<(), ViewPlanError> {
    program
        .validate()
        .map_err(|error| stage_error(&error.to_string()))
}

fn bound_column<'a>(
    id: &str,
    schema: &'a RelationSchema,
    names: &BTreeMap<String, String>,
) -> Result<&'a ColumnSchema, ViewPlanError> {
    names
        .get(id)
        .and_then(|name| schema.columns.iter().find(|column| &column.name == name))
        .ok_or_else(|| stage_error("set/series expression references an unbound input column"))
}

fn validate_input_bindings(
    schema: &RelationSchema,
    names: &BTreeMap<String, String>,
) -> Result<(), ViewPlanError> {
    validate_output_columns(&schema.columns)?;
    if schema.relation_id.trim().is_empty()
        || schema.relation_name.trim().is_empty()
        || schema.relation_version.trim().is_empty()
        || schema.schema_fingerprint.trim().is_empty()
        || names.is_empty()
        || names.values().collect::<BTreeSet<_>>().len() != names.len()
        || names.iter().any(|(id, name)| {
            id.trim().is_empty() || !schema.columns.iter().any(|column| &column.name == name)
        })
    {
        return unsupported("invalid set/series input schema bindings");
    }
    Ok(())
}

fn validate_bound_program(
    program: &TypedExprProgramV1,
    schema: &RelationSchema,
    names: &BTreeMap<String, String>,
) -> Result<(), ViewPlanError> {
    validate_program(program)?;
    fn check(
        node: &TypedExprNodeV1,
        schema: &RelationSchema,
        names: &BTreeMap<String, String>,
    ) -> Result<(), ViewPlanError> {
        match &node.kind {
            TypedExprKindV1::Column { column_id } => {
                let column = bound_column(column_id, schema, names)?;
                if column.data_type != scalar_sql_type(&node.result_type)?
                    || column.nullable != node.nullable
                {
                    return unsupported(
                        "set/series expression type or nullability differs from its input schema",
                    );
                }
            }
            TypedExprKindV1::Call { args, .. } | TypedExprKindV1::UdfCall { args, .. } => {
                for arg in args {
                    check(arg, schema, names)?;
                }
            }
            TypedExprKindV1::Literal { .. } => {}
        }
        Ok(())
    }
    check(&program.root, schema, names)
}

fn validate_series_plan(plan: &SupportedSeriesPlanV1) -> Result<(), ViewPlanError> {
    validate_input_bindings(&plan.input_schema, &plan.input_column_names)?;
    validate_output_columns(&plan.output_columns)?;
    if plan.schema_version != 1
        || plan.max_emitted_rows_per_input == 0
        || plan.generated_column_id.trim().is_empty()
        || plan.projection.len() != plan.output_columns.len()
        || !matches!(
            plan.start.root.result_type,
            RuntimeScalarTypeV1::Int64 | RuntimeScalarTypeV1::TimestampNanosecond
        )
        || plan.start.root.result_type != plan.end.root.result_type
        || plan.step.root.result_type != RuntimeScalarTypeV1::Int64
    {
        return unsupported("invalid series plan contract");
    }
    for program in [&plan.start, &plan.end, &plan.step] {
        validate_bound_program(program, &plan.input_schema, &plan.input_column_names)?;
        if matches!(
            program.root.kind,
            TypedExprKindV1::Literal {
                value: ScalarLiteralV1::Null { .. }
            }
        ) {
            return unsupported("series bounds and step cannot be NULL");
        }
    }
    if matches!(
        plan.step.root.kind,
        TypedExprKindV1::Literal {
            value: ScalarLiteralV1::Int64(0)
        }
    ) {
        return unsupported("series step cannot be zero");
    }
    if let (Some(start), Some(end), Some(step)) = (
        series_literal(&plan.start),
        series_literal(&plan.end),
        series_literal(&plan.step),
    ) {
        let distance = i128::from(end) - i128::from(start);
        let count = if (step > 0 && distance < 0) || (step < 0 && distance > 0) {
            0
        } else {
            distance.abs() / i128::from(step).abs() + 1
        };
        if count > i128::from(plan.max_emitted_rows_per_input) {
            return unsupported("literal series exceeds max_emitted_rows_per_input");
        }
    }
    for (projection, output) in plan.projection.iter().zip(&plan.output_columns) {
        if projection.output_name != output.name {
            return unsupported("series projection names differ from output schema");
        }
        if let Some(id) = &projection.input_column_id {
            let source = bound_column(id, &plan.input_schema, &plan.input_column_names)?;
            if source.data_type != output.data_type || source.nullable != output.nullable {
                return unsupported(
                    "series source projection type or nullability differs from its input schema",
                );
            }
        }
        if projection.input_column_id.is_none()
            && (output.nullable
                || output.data_type != scalar_sql_type(&plan.start.root.result_type)?)
        {
            return unsupported("generated series output has incorrect type or nullability");
        }
    }
    Ok(())
}

fn series_literal(program: &TypedExprProgramV1) -> Option<i64> {
    match program.root.kind {
        TypedExprKindV1::Literal {
            value: ScalarLiteralV1::Int64(value) | ScalarLiteralV1::TimestampNanosecond(value),
        } => Some(value),
        _ => None,
    }
}

fn validate_union_plan(plan: &SupportedUnionAllPlanV1) -> Result<(), ViewPlanError> {
    validate_output_columns(&plan.output_columns)?;
    if plan.schema_version != 1 || plan.branches.len() < 2 {
        return unsupported("invalid UNION ALL plan contract");
    }
    let mut schemas = BTreeMap::new();
    let mut nullable = vec![false; plan.output_columns.len()];
    for branch in &plan.branches {
        validate_input_bindings(&branch.input_schema, &branch.input_column_names)?;
        if schemas
            .insert(
                &branch.input_schema.relation_id,
                (&branch.input_schema, &branch.input_column_names),
            )
            .is_some_and(|previous| previous != (&branch.input_schema, &branch.input_column_names))
            || branch.projection.len() != plan.output_columns.len()
        {
            return unsupported("UNION ALL input bindings or arity are inconsistent");
        }
        for (index, (program, output)) in branch
            .projection
            .iter()
            .zip(&plan.output_columns)
            .enumerate()
        {
            validate_bound_program(program, &branch.input_schema, &branch.input_column_names)?;
            if scalar_sql_type(&program.root.result_type)? != output.data_type {
                return unsupported("UNION ALL expression type differs from output schema");
            }
            nullable[index] |= program.root.nullable;
        }
    }
    if plan
        .output_columns
        .iter()
        .zip(nullable)
        .any(|(column, nullable)| column.nullable != nullable)
    {
        return unsupported("UNION ALL output nullability must include every branch");
    }
    Ok(())
}

pub(super) fn validate_set_operation_binding(
    logical: &VelorixLogicalViewPlanV1,
) -> Result<(), ViewPlanError> {
    let expected_columns = match &logical.execution {
        VelorixLogicalViewExecutionV1::TypedMap { plan } => {
            validate_supported_typed_map_plan(plan)?;
            &plan.output_columns
        }
        VelorixLogicalViewExecutionV1::Series { plan } => {
            validate_series_plan(plan)?;
            &plan.output_columns
        }
        VelorixLogicalViewExecutionV1::UnionAll { plan } => {
            validate_union_plan(plan)?;
            &plan.output_columns
        }
        _ => {
            if logical.nodes.iter().any(|node| {
                matches!(
                    node,
                    VelorixLogicalViewPlanNodeV1::TypedMap { .. }
                        | VelorixLogicalViewPlanNodeV1::Series { .. }
                        | VelorixLogicalViewPlanNodeV1::UnionAll { .. }
                )
            }) {
                return invalid_logical_plan("set operator node requires matching execution");
            }
            return Ok(());
        }
    };
    let output_schema = logical
        .nodes
        .iter()
        .find_map(|node| match node {
            VelorixLogicalViewPlanNodeV1::TypedMap { output_schema, .. }
            | VelorixLogicalViewPlanNodeV1::Series { output_schema, .. }
            | VelorixLogicalViewPlanNodeV1::UnionAll { output_schema, .. } => Some(output_schema),
            _ => None,
        })
        .ok_or_else(|| stage_error("set execution requires its logical operator"))?;
    validate_stage_output(output_schema, expected_columns)?;
    let expected = stage_logical_plan(&logical.view_sql, output_schema, logical.execution.clone())?;
    if logical.nodes != expected.nodes
        || logical.input_relations != expected.input_relations
        || logical.output_relation != expected.output_relation
        || logical.state_requirements != expected.state_requirements
    {
        return invalid_logical_plan(
            "set/series nodes and input/output bindings differ from execution contract",
        );
    }
    Ok(())
}

pub(super) fn is_union_all_sql(sql: &str) -> Result<bool, ViewPlanError> {
    let query = parse_single_query(sql)?;
    fn is_union(body: &SetExpr) -> bool {
        match body {
            SetExpr::SetOperation {
                op: SetOperator::Union,
                set_quantifier: SetQuantifier::All,
                ..
            } => true,
            SetExpr::Query(query) => is_union(&query.body),
            _ => false,
        }
    }
    Ok(is_union(&query.body))
}

pub(super) fn is_series_sql(sql: &str) -> Result<bool, ViewPlanError> {
    let query = parse_single_query(sql)?;
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(false);
    };
    Ok(select
        .from
        .iter()
        .flat_map(|from| {
            std::iter::once(&from.relation).chain(from.joins.iter().map(|join| &join.relation))
        })
        .any(|factor| match factor {
            TableFactor::Function { name, .. }
            | TableFactor::Table {
                name,
                args: Some(_),
                ..
            } => single_object_name_identifier(name)
                .is_some_and(|name| name.eq_ignore_ascii_case("generate_series")),
            _ => false,
        }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::{
        RelationOperationV1, RelationSemanticRoleV1, VelorixLogicalTypeV1, VelorixRelationSchemaV1,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID,
    };

    #[test]
    fn removed_specialized_plan_tags_fail_closed() {
        for kind in ["native_projection", "native_windowed_fact", "native_rollup"] {
            let payload = serde_json::json!({ "kind": kind, "plan": {} });
            let execution =
                serde_json::from_value::<VelorixLogicalViewExecutionV1>(payload.clone())
                    .unwrap_err();
            let node = serde_json::from_value::<VelorixLogicalViewPlanNodeV1>(payload).unwrap_err();
            assert!(execution.to_string().contains("unknown variant"));
            assert!(node.to_string().contains("unknown variant"));
        }
    }

    fn catalog(name: &str, nullable: bool, timestamp: bool) -> VelorixRelationCatalogV1 {
        let column = |name: &str, ordinal, role, nullable| RelationColumnV1 {
            column_id: name.into(),
            name: name.into(),
            logical_type: VelorixLogicalTypeV1::Int64,
            physical_arrow_type: ArrowPhysicalTypeV1::Int64,
            nullable,
            ordinal,
            semantic_role: role,
        };
        let mut columns = vec![
            column("id", 0, RelationSemanticRoleV1::PrimaryKey, false),
            column("lo", 1, RelationSemanticRoleV1::Value, nullable),
            column("hi", 2, RelationSemanticRoleV1::Value, false),
            column("weight", 3, RelationSemanticRoleV1::Weight, false),
        ];
        if timestamp {
            for column in &mut columns[1..3] {
                column.logical_type = VelorixLogicalTypeV1::Timestamp { timezone: None };
                column.physical_arrow_type =
                    ArrowPhysicalTypeV1::TimestampNanosecond { timezone: None };
            }
        }
        VelorixRelationCatalogV1::from_relation_schema(
            VelorixRelationSchemaV1 {
                relation_id: name.into(),
                relation_name: name.into(),
                relation_version: "v1".into(),
                columns,
                primary_key_column_ids: vec!["id".into()],
                weight_column_id: "weight".into(),
                allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
                event_time_column_id: None,
            },
            CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
        )
        .unwrap()
    }

    fn output(columns: Vec<ColumnSchema>) -> RelationSchema {
        RelationSchema {
            relation_id: "result".into(),
            relation_name: "result".into(),
            relation_version: "v1".into(),
            schema_fingerprint: stable_bytes_hash(b"output"),
            columns,
            primary_key: Vec::new(),
        }
    }

    #[test]
    fn series_types_budget_and_contract_are_bound() {
        let catalog = catalog("input_a", false, false);
        let sql = "SELECT s.id, g.n AS value FROM input_a AS s CROSS JOIN LATERAL generate_series(s.lo, s.hi, 1) AS g(n)";
        let supported = validate_supported_series_sql(sql, &catalog).unwrap();
        assert_eq!(supported.start.root.result_type, RuntimeScalarTypeV1::Int64);
        assert_eq!(supported.projection[1].input_column_id, None);
        let schema = output(supported.output_columns.clone());
        let logical =
            lower_supported_sql_to_logical_plan(sql, std::slice::from_ref(&catalog), &schema)
                .unwrap();
        validate_logical_view_plan(&logical).unwrap();
        assert_eq!(
            logical.operator_dag_contract.operators[1].operator.kind,
            "series_flatmap"
        );
        let mut changed = logical;
        if let VelorixLogicalViewExecutionV1::Series { plan } = &mut changed.execution {
            plan.max_emitted_rows_per_input += 1;
        }
        assert!(finalize_logical_plan(changed).is_err());
        let literals = "SELECT n FROM input_a, LATERAL generate_series(3, 1, -1) AS g(n)";
        assert!(validate_supported_series_sql_with_budget(literals, &catalog, 3).is_ok());
        assert!(validate_supported_series_sql_with_budget(literals, &catalog, 2).is_err());
        for args in ["1, 2, 0", "1, 2, NULL", "NULL, 2, 1"] {
            let sql = format!("SELECT n FROM input_a, LATERAL generate_series({args}) AS g(n)");
            assert!(validate_supported_series_sql(&sql, &catalog).is_err());
        }
    }

    #[test]
    fn declared_event_time_type_and_distinct_catalog_ids_are_preserved() {
        let mut schema = catalog("input_a", false, false).relation_schema;
        schema.columns[1].column_id = "lower_bound_id".into();
        schema.event_time_column_id = Some("lower_bound_id".into());
        let left = VelorixRelationCatalogV1::from_relation_schema(
            schema,
            CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
        )
        .unwrap();
        let right = catalog("input_b", true, false);
        let sql = "SELECT lo AS value FROM input_a UNION ALL SELECT lo FROM input_b";
        let plan = validate_supported_union_all_sql(sql, &[left.clone(), right]).unwrap();
        assert_eq!(plan.output_columns[0].data_type, SqlDataType::Int64);
        assert!(plan.output_columns[0].nullable);
        assert_eq!(plan.branches[0].input_column_names["lower_bound_id"], "lo");
        assert!(matches!(&plan.branches[0].projection[0].root.kind,
            TypedExprKindV1::Column { column_id } if column_id == "lower_bound_id"));
        let series = validate_supported_series_sql(
            "SELECT s.lo, g.n FROM input_a s CROSS JOIN LATERAL generate_series(s.lo, s.hi, 1) AS g(n)",
            &left,
        ).unwrap();
        assert_eq!(series.start.root.result_type, RuntimeScalarTypeV1::Int64);
        assert_eq!(
            series.projection[0].input_column_id.as_deref(),
            Some("lower_bound_id")
        );
        validate_series_plan(&series).unwrap();
    }

    #[test]
    fn rehashed_series_rejects_unbound_or_mistyped_inputs_and_projections() {
        let sql = "SELECT s.lo, g.n FROM input_a s CROSS JOIN LATERAL generate_series(s.lo, s.hi, 1) AS g(n)";
        let valid = validate_supported_series_sql(sql, &catalog("input_a", true, false)).unwrap();
        for corruption in 0..6 {
            let mut plan = valid.clone();
            match corruption {
                0 => {
                    plan.start.root.kind = TypedExprKindV1::Column {
                        column_id: "missing".into(),
                    }
                }
                1 => plan.start.root.nullable = false,
                2 => plan.projection[0].input_column_id = Some("missing".into()),
                3 => plan.output_columns[0].nullable = false,
                4 => plan.output_columns[0].data_type = SqlDataType::Float64,
                _ => {
                    plan.input_column_names
                        .insert("lo".into(), "missing".into());
                }
            }
            let output = output(plan.output_columns.clone());
            let logical = stage_logical_plan(
                sql,
                &output,
                VelorixLogicalViewExecutionV1::Series {
                    plan: Box::new(plan),
                },
            )
            .unwrap();
            assert!(
                finalize_logical_plan(logical).is_err(),
                "corruption {corruption}"
            );
        }
    }

    #[test]
    fn rehashed_union_rejects_invalid_expression_leaves() {
        let sql = "SELECT lo AS value FROM input_a UNION ALL SELECT hi FROM input_a";
        let valid =
            validate_supported_union_all_sql(sql, &[catalog("input_a", true, false)]).unwrap();
        for corruption in 0..4 {
            let mut plan = valid.clone();
            match corruption {
                0 => {
                    plan.branches[0].projection[0].root.kind = TypedExprKindV1::Column {
                        column_id: "missing".into(),
                    }
                }
                1 => plan.branches[0].projection[0].root.nullable = false,
                2 => {
                    for branch in &mut plan.branches {
                        branch.projection[0].root.result_type = RuntimeScalarTypeV1::Float64;
                    }
                    plan.output_columns[0].data_type = SqlDataType::Float64;
                }
                _ => {
                    plan.branches[0].input_column_names.remove("lo");
                }
            }
            let output = output(plan.output_columns.clone());
            let logical = stage_logical_plan(
                sql,
                &output,
                VelorixLogicalViewExecutionV1::UnionAll {
                    plan: Box::new(plan),
                },
            )
            .unwrap();
            assert!(
                finalize_logical_plan(logical).is_err(),
                "corruption {corruption}"
            );
        }
    }

    #[test]
    fn timestamp_series_uses_checked_fixed_nanoseconds() {
        let catalog = catalog("input_b", true, true);
        let sql = "SELECT g.t FROM input_b AS s CROSS JOIN LATERAL generate_series(s.lo, s.hi, INTERVAL '2 seconds') AS g(t)";
        let plan = validate_supported_series_sql(sql, &catalog).unwrap();
        assert_eq!(
            plan.start.root.result_type,
            RuntimeScalarTypeV1::TimestampNanosecond
        );
        assert_eq!(series_literal(&plan.step), Some(2_000_000_000));
        assert_eq!(
            plan.output_columns[0].data_type,
            SqlDataType::Timestamp { timezone: None }
        );
        for interval in [
            "0 seconds",
            "1 month",
            "1 second garbage",
            "9223372036854775807 days",
        ] {
            let sql = sql.replace("2 seconds", interval);
            assert!(validate_supported_series_sql(&sql, &catalog).is_err());
        }
    }

    #[test]
    fn union_preserves_branch_order_repetition_and_positional_nullability() {
        let catalogs = vec![
            catalog("input_a", false, false),
            catalog("input_b", true, false),
        ];
        let sql = "SELECT lo AS value FROM input_a UNION ALL SELECT lo AS other FROM input_b UNION ALL SELECT hi FROM input_a";
        let supported = validate_supported_union_all_sql(sql, &catalogs).unwrap();
        assert_eq!(supported.branches.len(), 3);
        assert_eq!(
            supported.branches[0].input_schema,
            supported.branches[2].input_schema
        );
        assert_eq!(supported.output_columns[0].name, "value");
        assert!(supported.output_columns[0].nullable);
        let schema = output(supported.output_columns);
        let logical = lower_supported_sql_to_logical_plan(sql, &catalogs, &schema).unwrap();
        validate_logical_view_plan(&logical).unwrap();
        assert_eq!(logical.input_relations.len(), 2);
        assert_eq!(logical.operator_dag_contract.operators[3].inputs.len(), 3);
        assert_eq!(
            logical.operator_dag_contract.operators[3].outputs[0].uniqueness,
            UniquenessGuaranteeV1::NotGuaranteed
        );
    }

    #[test]
    fn union_fails_closed_for_non_bag_or_incompatible_shapes() {
        let catalogs = vec![catalog("input_a", false, false)];
        for sql in [
            "SELECT lo FROM input_a UNION SELECT hi FROM input_a",
            "SELECT lo FROM input_a UNION ALL SELECT id, hi FROM input_a",
            "SELECT lo FROM input_a UNION ALL SELECT 'x' AS hi FROM input_a",
            "SELECT lo FROM input_a UNION ALL SELECT hi FROM input_a WHERE id > 1",
            "SELECT lo FROM input_a UNION ALL SELECT hi FROM input_a ORDER BY 1",
        ] {
            assert!(
                validate_supported_union_all_sql(sql, &catalogs).is_err(),
                "{sql}"
            );
        }
        let sql = "SELECT lo FROM input_a UNION ALL SELECT hi FROM input_a";
        let mut schema = output(supported_union_all_output_columns(sql, &catalogs).unwrap());
        schema.primary_key = vec!["lo".into()];
        assert!(lower_supported_union_all_sql_to_logical_plan(sql, &catalogs, &schema).is_err());
    }
}
