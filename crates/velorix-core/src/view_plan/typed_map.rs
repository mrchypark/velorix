//! Typed scalar map/filter over signed row bags, without an observable key requirement.

use super::*;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportedTypedMapPlanV1 {
    pub schema_version: u32,
    pub input_schema: RelationSchema,
    pub projection: Vec<TypedExprProgramV1>,
    pub output_columns: Vec<ColumnSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<TypedExprProgramV1>,
}

pub fn validate_supported_typed_map_sql(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
) -> Result<SupportedTypedMapPlanV1, ViewPlanError> {
    catalog.validate()?;
    let query = parse_single_query(sql)?;
    validate_query_level_clauses(&query, false)?;
    if query.order_by.is_some() {
        return unsupported("typed map does not support output ordering");
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return unsupported("typed map requires a single SELECT");
    };
    validate_plain_select_clauses(select)?;
    if select.distinct.is_some()
        || !group_by_is_empty(&select.group_by)
        || select.exclude.is_some()
        || !select.optimizer_hints.is_empty()
    {
        return unsupported("typed map supports scalar SELECT and WHERE only");
    }
    let [from] = select.from.as_slice() else {
        return unsupported("typed map requires one registered relation");
    };
    if !from.joins.is_empty() {
        return unsupported("typed map requires a separate join stage");
    }
    if let TableFactor::Table {
        alias: Some(alias), ..
    } = &from.relation
    {
        if !alias.columns.is_empty() || alias.at.is_some() {
            return unsupported("typed map does not support table column aliases");
        }
    }
    let table = registered_table_ref(&from.relation, "typed map")?;
    if table.name != catalog.relation_schema.relation_id {
        return unsupported("typed map source must match its registered relation");
    }
    let input_schema =
        catalog_input_relation_schema(catalog).map_err(|error| map_error(error.to_string()))?;
    let mut plan = SupportedTypedMapPlanV1 {
        schema_version: 1,
        input_schema,
        projection: Vec::new(),
        output_columns: Vec::new(),
        predicate: select
            .selection
            .as_ref()
            .map(|expr| scalar_program(expr, catalog, &table.alias))
            .transpose()?,
    };
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(options)
                if options.opt_ilike.is_none()
                    && options.opt_exclude.is_none()
                    && options.opt_except.is_none()
                    && options.opt_replace.is_none()
                    && options.opt_rename.is_none()
                    && options.opt_alias.is_none() =>
            {
                for column in &catalog.relation_schema.columns {
                    if column.column_id != catalog.relation_schema.weight_column_id {
                        let expr = Expr::Identifier(sqlparser::ast::Ident::new(&column.name));
                        push_projection(&mut plan, &expr, &column.name, catalog, &table.alias)?;
                    }
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                push_projection(&mut plan, expr, &alias.value, catalog, &table.alias)?;
            }
            SelectItem::UnnamedExpr(expr @ Expr::Identifier(id)) => {
                push_projection(&mut plan, expr, &id.value, catalog, &table.alias)?;
            }
            SelectItem::UnnamedExpr(expr @ Expr::CompoundIdentifier(ids)) if !ids.is_empty() => {
                push_projection(
                    &mut plan,
                    expr,
                    &ids.last().unwrap().value,
                    catalog,
                    &table.alias,
                )?;
            }
            _ => {
                return unsupported(
                    "typed map requires plain * or explicitly named scalar projections",
                )
            }
        }
    }
    validate_supported_typed_map_plan(&plan)?;
    Ok(plan)
}

fn push_projection(
    plan: &mut SupportedTypedMapPlanV1,
    expr: &Expr,
    name: &str,
    catalog: &VelorixRelationCatalogV1,
    alias: &str,
) -> Result<(), ViewPlanError> {
    let program = scalar_program(expr, catalog, alias)?;
    plan.output_columns.push(ColumnSchema {
        name: name.into(),
        data_type: scalar_sql_type(program.root.result_type)?,
        nullable: program.root.nullable,
    });
    plan.projection.push(program);
    Ok(())
}

fn scalar_program(
    expr: &Expr,
    catalog: &VelorixRelationCatalogV1,
    alias: &str,
) -> Result<TypedExprProgramV1, ViewPlanError> {
    let mut root = typed_expr_node_from_expr_in_context(expr, catalog, Some(alias), false)?
        .ok_or_else(|| map_error("typed map expression is outside the scalar contract"))?;
    fn check(
        node: &mut TypedExprNodeV1,
        catalog: &VelorixRelationCatalogV1,
    ) -> Result<(), ViewPlanError> {
        match &mut node.kind {
            TypedExprKindV1::Column { column_id } => {
                if column_id == &catalog.relation_schema.weight_column_id {
                    return unsupported(
                        "typed map expressions must not reference the weight column",
                    );
                }
                let column = catalog
                    .relation_schema
                    .columns
                    .iter()
                    .find(|column| column.column_id == *column_id)
                    .ok_or_else(|| map_error("typed map input column is not registered"))?;
                *column_id = column.name.clone();
            }
            TypedExprKindV1::Call { args, .. } | TypedExprKindV1::UdfCall { args, .. } => {
                for arg in args {
                    check(arg, catalog)?;
                }
            }
            TypedExprKindV1::Literal { .. } => {}
        }
        Ok(())
    }
    check(&mut root, catalog)?;
    let program = TypedExprProgramV1 {
        encoding_version: TYPED_EXPR_PROGRAM_SCHEMA_VERSION_V1,
        root,
    };
    program
        .validate()
        .map_err(|error| map_error(error.to_string()))?;
    Ok(program)
}

fn map_error(reason: impl Into<String>) -> ViewPlanError {
    ViewPlanError::UnsupportedShape {
        reason: reason.into(),
    }
}

fn scalar_sql_type(value: RuntimeScalarTypeV1) -> Result<SqlDataType, ViewPlanError> {
    Ok(match value {
        RuntimeScalarTypeV1::Boolean => SqlDataType::Bool,
        RuntimeScalarTypeV1::Int64 => SqlDataType::Int64,
        RuntimeScalarTypeV1::Float64 => SqlDataType::Float64,
        RuntimeScalarTypeV1::Utf8 => SqlDataType::Utf8,
        RuntimeScalarTypeV1::Date32 => SqlDataType::Date,
        RuntimeScalarTypeV1::TimestampNanosecond => SqlDataType::Timestamp { timezone: None },
        RuntimeScalarTypeV1::Decimal128 { precision, scale } => SqlDataType::Decimal {
            precision,
            scale: u8::try_from(scale)
                .map_err(|_| map_error("negative decimal scale is unsupported"))?,
        },
    })
}

pub fn supported_typed_map_output_columns(plan: &SupportedTypedMapPlanV1) -> Vec<ColumnSchema> {
    plan.output_columns.clone()
}

pub fn validate_supported_typed_map_plan(
    plan: &SupportedTypedMapPlanV1,
) -> Result<(), ViewPlanError> {
    if plan.schema_version != 1
        || plan.projection.is_empty()
        || plan.projection.len() != plan.output_columns.len()
    {
        return unsupported("invalid typed map plan version or projection arity");
    }
    let mut names = BTreeSet::new();
    for (program, column) in plan.projection.iter().zip(&plan.output_columns) {
        validate_bound_program(program, &plan.input_schema)?;
        if column.name.trim().is_empty()
            || !names.insert(column.name.to_ascii_lowercase())
            || column.data_type != scalar_sql_type(program.root.result_type)?
            || column.nullable != program.root.nullable
        {
            return unsupported(
                "typed map output names, types or nullability differ from projections",
            );
        }
    }
    if let Some(predicate) = &plan.predicate {
        validate_bound_program(predicate, &plan.input_schema)?;
        if predicate.root.result_type != RuntimeScalarTypeV1::Boolean {
            return unsupported("typed map WHERE must produce Boolean; only TRUE passes");
        }
    }
    Ok(())
}

fn validate_bound_program(
    program: &TypedExprProgramV1,
    schema: &RelationSchema,
) -> Result<(), ViewPlanError> {
    program
        .validate()
        .map_err(|error| map_error(error.to_string()))?;
    fn check(node: &TypedExprNodeV1, schema: &RelationSchema) -> Result<(), ViewPlanError> {
        match &node.kind {
            TypedExprKindV1::Column { column_id } => {
                let column = schema
                    .columns
                    .iter()
                    .find(|column| column.name == *column_id)
                    .ok_or_else(|| {
                        map_error("typed map expression references an unbound input column")
                    })?;
                if column.data_type != scalar_sql_type(node.result_type)?
                    || column.nullable != node.nullable
                {
                    return unsupported(
                        "typed map expression column type differs from its input schema",
                    );
                }
            }
            TypedExprKindV1::Call { args, .. } | TypedExprKindV1::UdfCall { args, .. } => {
                for arg in args {
                    check(arg, schema)?;
                }
            }
            TypedExprKindV1::Literal { .. } => {}
        }
        Ok(())
    }
    check(&program.root, schema)
}

pub fn lower_supported_typed_map_sql_to_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    let plan = validate_supported_typed_map_sql(sql, catalog)?;
    finalize_logical_plan(logical_plan(sql, plan, output_schema)?)
}

fn logical_plan(
    sql: &str,
    plan: SupportedTypedMapPlanV1,
    output_schema: &RelationSchema,
) -> Result<VelorixLogicalViewPlanV1, ViewPlanError> {
    validate_supported_typed_map_plan(&plan)?;
    if output_schema.columns != plan.output_columns
        || !output_schema.primary_key.is_empty()
        || output_schema.relation_id.trim().is_empty()
        || output_schema.relation_name.trim().is_empty()
        || output_schema.relation_version.trim().is_empty()
        || output_schema.schema_fingerprint.trim().is_empty()
    {
        return unsupported(
            "typed map output must match inferred columns with an empty primary key",
        );
    }
    let input_relation = logical_relation_from_schema(&plan.input_schema);
    let output_relation = logical_relation_from_schema(output_schema);
    Ok(VelorixLogicalViewPlanV1 {
        plan_version: LOGICAL_VIEW_PLAN_VERSION_V2,
        plan_hash: None,
        view_sql: sql.into(),
        capability_version: LOGICAL_VIEW_PLAN_CAPABILITY_VERSION_V2.into(),
        key_semantics_version: INCREMENTAL_KEY_SEMANTICS_VERSION_V1.into(),
        bag_semantics_version: INCREMENTAL_BAG_SEMANTICS_VERSION_V1.into(),
        input_relations: vec![input_relation.clone()],
        output_relation: output_relation.clone(),
        nodes: vec![
            VelorixLogicalViewPlanNodeV1::RelationScan {
                node_id: "scan_input".into(),
                relation: input_relation,
            },
            VelorixLogicalViewPlanNodeV1::TypedMap {
                node_id: "typed_map".into(),
                input: "scan_input".into(),
                plan: Box::new(plan.clone()),
                output_schema: output_schema.clone(),
            },
            VelorixLogicalViewPlanNodeV1::Output {
                node_id: "output_materialized_view".into(),
                input: "typed_map".into(),
                relation: output_relation,
            },
        ],
        operator_dag_contract: empty_operator_dag_contract(),
        state_requirements: vec![LogicalPlanStateRequirementV1 {
            node_id: "typed_map".into(),
            state_kind: LogicalPlanStateKindV1::Projection,
            key_columns: Vec::new(),
            codec_version: LOGICAL_VIEW_STATE_CODEC_VERSION_V1.into(),
        }],
        output_codec_version: LOGICAL_VIEW_OUTPUT_CODEC_VERSION_V1.into(),
        execution_implementation: None,
        execution: VelorixLogicalViewExecutionV1::TypedMap {
            plan: Box::new(plan),
        },
    })
}

pub fn validate_typed_map_binding(logical: &VelorixLogicalViewPlanV1) -> Result<(), ViewPlanError> {
    let VelorixLogicalViewExecutionV1::TypedMap { plan } = &logical.execution else {
        if logical
            .nodes
            .iter()
            .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::TypedMap { .. }))
        {
            return invalid_logical_plan("typed map node requires matching execution");
        }
        return Ok(());
    };
    let output_schema = logical
        .nodes
        .iter()
        .find_map(|node| match node {
            VelorixLogicalViewPlanNodeV1::TypedMap { output_schema, .. } => Some(output_schema),
            _ => None,
        })
        .ok_or_else(|| map_error("typed map execution requires its logical node"))?;
    let expected = logical_plan(&logical.view_sql, *plan.clone(), output_schema)?;
    if logical.nodes != expected.nodes
        || logical.input_relations != expected.input_relations
        || logical.output_relation != expected.output_relation
        || logical.state_requirements != expected.state_requirements
    {
        return invalid_logical_plan("typed map nodes or bindings differ from execution contract");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relation::{
        RelationOperationV1, RelationSemanticRoleV1, VelorixLogicalTypeV1, VelorixRelationSchemaV1,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID,
    };

    fn catalog() -> VelorixRelationCatalogV1 {
        let columns = [
            (
                "id",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                false,
            ),
            (
                "epoch_ns",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                true,
            ),
            (
                "metric",
                VelorixLogicalTypeV1::Float64,
                ArrowPhysicalTypeV1::Float64,
                true,
            ),
            (
                "weight",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                false,
            ),
        ]
        .into_iter()
        .enumerate()
        .map(
            |(ordinal, (name, logical_type, physical_arrow_type, nullable))| RelationColumnV1 {
                column_id: name.into(),
                name: name.into(),
                ordinal: ordinal as u32,
                logical_type,
                physical_arrow_type,
                nullable,
                semantic_role: match name {
                    "id" => RelationSemanticRoleV1::PrimaryKey,
                    "weight" => RelationSemanticRoleV1::Weight,
                    _ => RelationSemanticRoleV1::Value,
                },
            },
        )
        .collect();
        VelorixRelationCatalogV1::from_relation_schema(
            VelorixRelationSchemaV1 {
                relation_id: "events".into(),
                relation_name: "events".into(),
                relation_version: "v1".into(),
                columns,
                primary_key_column_ids: vec!["id".into()],
                weight_column_id: "weight".into(),
                allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
                event_time_column_id: Some("epoch_ns".into()),
            },
            CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
        )
        .unwrap()
    }

    #[test]
    fn wildcard_where_and_key_free_projection_preserve_declared_types() {
        let catalog = catalog();
        let plan = validate_supported_typed_map_sql(
            "SELECT * FROM events WHERE epoch_ns IS NOT NULL AND metric IS NOT NULL",
            &catalog,
        )
        .unwrap();
        assert_eq!(
            plan.output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "epoch_ns", "metric"]
        );
        assert_eq!(plan.output_columns[1].data_type, SqlDataType::Int64);
        let plan = validate_supported_typed_map_sql("SELECT to_timestamp(epoch_ns / 1e9) AS instant, metric AS value FROM events WHERE metric > 2.5 OR metric IS NULL", &catalog).unwrap();
        assert_eq!(plan.output_columns.len(), 2);
        assert_eq!(
            plan.predicate.as_ref().unwrap().root.result_type,
            RuntimeScalarTypeV1::Boolean
        );
        assert!(plan.output_columns.iter().all(|column| column.nullable));
        let restored = serde_json::from_str(&serde_json::to_string(&plan).unwrap()).unwrap();
        assert_eq!(plan, restored);
        validate_supported_typed_map_plan(&restored).unwrap();
    }

    #[test]
    fn scalar_only_admission_rejects_unimplemented_shapes() {
        for sql in [
            "SELECT SUM(metric) AS total FROM events",
            "SELECT round(metric) OVER () AS total FROM events",
            "SELECT round(DISTINCT metric) AS total FROM events",
            "SELECT metric FROM events WHERE metric",
            "SELECT metric FROM events GROUP BY metric",
            "SELECT DISTINCT metric FROM events",
            "SELECT metric FROM events ORDER BY metric",
            "SELECT metric FROM events LIMIT 1",
            "SELECT metric FROM events OFFSET 1",
            "SELECT metric FROM events FETCH FIRST 1 ROW ONLY",
            "SELECT metric FROM events FOR UPDATE",
            "SELECT metric FROM events HAVING TRUE",
            "SELECT metric FROM events QUALIFY TRUE",
            "SELECT metric FROM events AS e(x)",
            "WITH e AS (SELECT * FROM events) SELECT metric FROM e",
            "SELECT a.metric FROM events a JOIN events b ON a.id = b.id",
            "SELECT weight FROM events",
            "SELECT metric AS result, metric AS result FROM events",
        ] {
            assert!(
                validate_supported_typed_map_sql(sql, &catalog()).is_err(),
                "{sql}"
            );
        }
    }

    #[test]
    fn logical_binding_rejects_tampered_bag_schema_and_predicate() {
        let catalog = catalog();
        let sql = "SELECT metric FROM events WHERE metric IS NOT NULL";
        let plan = validate_supported_typed_map_sql(sql, &catalog).unwrap();
        let output = RelationSchema {
            relation_id: "result".into(),
            relation_name: "result".into(),
            relation_version: "v1".into(),
            schema_fingerprint: stable_bytes_hash(b"typed-map-test"),
            columns: plan.output_columns.clone(),
            primary_key: Vec::new(),
        };
        let logical = logical_plan(sql, plan.clone(), &output).unwrap();
        validate_typed_map_binding(&logical).unwrap();
        let mut keyed = output;
        keyed.primary_key = vec!["metric".into()];
        assert!(logical_plan(sql, plan.clone(), &keyed).is_err());
        let mut invalid = plan;
        invalid.predicate = Some(invalid.projection[0].clone());
        assert!(validate_supported_typed_map_plan(&invalid).is_err());
    }

    #[test]
    fn central_validation_rejects_matching_malformed_node_and_execution() {
        let sql = "SELECT metric FROM events WHERE metric IS NOT NULL";
        let supported = validate_supported_typed_map_sql(sql, &catalog()).unwrap();
        let output = RelationSchema {
            relation_id: "result".into(),
            relation_name: "result".into(),
            relation_version: "v1".into(),
            schema_fingerprint: stable_bytes_hash(b"typed-map-test"),
            columns: supported.output_columns.clone(),
            primary_key: Vec::new(),
        };
        let logical = logical_plan(sql, supported.clone(), &output).unwrap();
        for corruption in 0..4 {
            let mut malformed = supported.clone();
            match corruption {
                0 => malformed.schema_version = 99,
                1 => {
                    malformed.projection[0].root.kind = TypedExprKindV1::Column {
                        column_id: "missing".into(),
                    }
                }
                2 => {
                    malformed
                        .input_schema
                        .columns
                        .iter_mut()
                        .find(|column| column.name == "metric")
                        .unwrap()
                        .nullable = false
                }
                _ => malformed.predicate = Some(malformed.projection[0].clone()),
            }
            let mut forged = logical.clone();
            forged.execution = VelorixLogicalViewExecutionV1::TypedMap {
                plan: Box::new(malformed.clone()),
            };
            if let VelorixLogicalViewPlanNodeV1::TypedMap { plan, .. } = &mut forged.nodes[1] {
                **plan = malformed;
            }
            assert!(
                finalize_logical_plan(forged).is_err(),
                "corruption {corruption}"
            );
        }
    }
}
