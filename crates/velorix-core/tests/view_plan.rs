use serde_json::json;
use velorix_core::{
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
    view_contract::{ColumnSchema, RelationSchema, SqlDataType},
    view_plan::{
        lower_supported_join_view_sql_to_logical_plan, lower_supported_sql_to_logical_plan,
        lower_supported_view_sql_to_logical_plan, validate_logical_view_plan,
        validate_supported_join_view_sql, validate_supported_latest_by_key_sql,
        validate_supported_tumbling_window_sql, validate_supported_view_sql,
        LogicalPlanAggregateFunctionV1, PredicateOp, VelorixLogicalViewExecutionV1,
        VelorixLogicalViewPlanNodeV1, ViewPlanError, LOGICAL_VIEW_PLAN_HASH_PREFIX,
        LOGICAL_VIEW_PLAN_VERSION_V1,
    },
};

#[test]
fn single_key_sum_count_sql_lowers_to_hashed_logical_view_plan() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    assert_eq!(plan.plan_version, LOGICAL_VIEW_PLAN_VERSION_V1);
    assert!(plan
        .plan_hash
        .as_ref()
        .is_some_and(|hash| hash.starts_with(LOGICAL_VIEW_PLAN_HASH_PREFIX)));
    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::SingleKeySumCount { .. }
    ));
    assert!(plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::RelationScan { .. })));
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Aggregate { accumulators, .. }
            if accumulators.iter().any(|acc| acc.function == LogicalPlanAggregateFunctionV1::Sum)
                && accumulators
                    .iter()
                    .any(|acc| acc.function == LogicalPlanAggregateFunctionV1::Count)
    )));
    assert!(plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Output { .. })));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filtered_single_key_sum_count_sql_lowers_to_plan_with_filter_node() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores where score > -1 group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    assert!(plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { .. })));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filtered_projected_single_key_aggregate_sql_lowers_to_projected_accumulators() {
    let catalog = scores_catalog();
    let output_schema = scores_projected_stats_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as total_score, count(*) as score_events, avg(score) as average_score from scores where score > 0 group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    assert!(plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { .. })));
    let aggregate = plan
        .nodes
        .iter()
        .find_map(|node| match node {
            VelorixLogicalViewPlanNodeV1::Aggregate { accumulators, .. } => Some(accumulators),
            _ => None,
        })
        .expect("filtered aggregate plan should include aggregate node");
    assert!(aggregate.iter().any(|acc| {
        acc.function == LogicalPlanAggregateFunctionV1::Sum && acc.output_column_id == "total_score"
    }));
    assert!(aggregate.iter().any(|acc| {
        acc.function == LogicalPlanAggregateFunctionV1::Count
            && acc.output_column_id == "score_events"
    }));
    assert!(aggregate.iter().any(|acc| {
        acc.function == LogicalPlanAggregateFunctionV1::Avg
            && acc.output_column_id == "average_score"
    }));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_lowers_to_hashed_logical_view_plan() {
    let orders = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[orders, accounts],
        &output_schema,
    )
    .unwrap();

    assert_eq!(plan.plan_version, LOGICAL_VIEW_PLAN_VERSION_V1);
    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { .. }
    ));
    assert!(plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::InnerEquiJoin { .. })));
    assert!(plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Aggregate { .. })));
    assert!(plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Output { .. })));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn latest_by_key_sql_lowers_to_project_and_latest_nodes() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    assert_eq!(plan.plan_version, LOGICAL_VIEW_PLAN_VERSION_V1);
    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    assert_eq!(supported.key_column_id, "device_id");
    assert_eq!(supported.value_column_id, "enabled");
    assert_eq!(supported.ordering_column_id, "event_time");
    assert!(plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::LatestByKey { .. })));
    assert!(plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Project { .. })));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn latest_by_key_fixture_sql_accepts_arg_max_shape() {
    let catalog = device_status_catalog();

    let plan = validate_supported_latest_by_key_sql(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id",
        &catalog,
    )
    .unwrap();

    assert_eq!(plan.input_relation_id, "device_status");
    assert_eq!(plan.key_column_id, "device_id");
    assert_eq!(plan.value_column_id, "enabled");
    assert_eq!(plan.ordering_column_id, "event_time");
    assert_eq!(plan.output_value_column_id, "enabled");
}

#[test]
fn latest_by_key_sql_uses_arg_max_columns_without_value_role_cardinality() {
    let mut catalog = device_status_catalog();
    catalog.relation_schema.columns.push(RelationColumnV1 {
        column_id: "debug_score".to_string(),
        name: "debug_score".to_string(),
        logical_type: VelorixLogicalTypeV1::Int64,
        physical_arrow_type: ArrowPhysicalTypeV1::Int64,
        nullable: false,
        ordinal: 4,
        semantic_role: RelationSemanticRoleV1::Value,
    });
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();

    let plan = validate_supported_latest_by_key_sql(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id",
        &catalog,
    )
    .unwrap();

    assert_eq!(plan.value_column_id, "enabled");
    assert_eq!(plan.ordering_column_id, "event_time");
}

#[test]
fn logical_view_plan_validation_rejects_tampered_hash() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();
    let mut plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    plan.plan_hash = Some(format!("{LOGICAL_VIEW_PLAN_HASH_PREFIX}:sha256:bad"));

    let error = validate_logical_view_plan(&plan).unwrap_err();
    assert!(matches!(error, ViewPlanError::InvalidLogicalPlan { .. }));
    assert!(error.to_string().contains("hash mismatch"));
}

#[test]
fn single_key_sum_count_fixture_sql_accepts_catalog_backed_shape() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
    )
    .unwrap();

    assert_eq!(plan.input_relation_id, "scores");
    assert_eq!(plan.group_key_column_id, "user_id");
    assert_eq!(plan.sum_value_column_id, "score");
    assert_eq!(plan.predicate, None);
}

#[test]
fn single_key_sum_count_sql_uses_projected_value_column_without_value_semantic_role() {
    let catalog = purchases_catalog_without_value_role();
    let output_schema = purchases_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(supported.group_key_column_id, "user_id");
    assert_eq!(supported.sum_value_column_id, "amount");
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Aggregate { accumulators, .. }
            if accumulators.iter().any(|acc| {
                acc.function == LogicalPlanAggregateFunctionV1::Sum
                    && acc
                        .input
                        .as_ref()
                        .is_some_and(|input| input.column_id == "amount")
            })
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_lowers_avg_with_sum_count_accumulators() {
    let catalog = purchases_catalog_without_value_role();
    let output_schema = purchases_avg_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(amount) as total, count(*) as events, avg(amount) as average from purchases group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Aggregate { accumulators, .. }
            if accumulators.iter().any(|acc| {
                acc.function == LogicalPlanAggregateFunctionV1::Sum
                    && acc.output_column_id == "total"
            })
                && accumulators.iter().any(|acc| {
                    acc.function == LogicalPlanAggregateFunctionV1::Count
                        && acc.output_column_id == "events"
                })
                && accumulators.iter().any(|acc| {
                    acc.function == LogicalPlanAggregateFunctionV1::Avg
                        && acc.output_column_id == "average"
                })
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_lowers_min_max_with_multiset_state_requirement() {
    let catalog = purchases_catalog_without_value_role();
    let output_schema = purchases_min_max_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, min(amount) as smallest, max(amount) as largest from purchases group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Aggregate { accumulators, .. }
            if accumulators.iter().any(|acc| {
                acc.function == LogicalPlanAggregateFunctionV1::Min
                    && acc.output_column_id == "smallest"
            })
                && accumulators.iter().any(|acc| {
                    acc.function == LogicalPlanAggregateFunctionV1::Max
                        && acc.output_column_id == "largest"
                })
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_rejects_avg_decimal_until_decimal_policy_exists() {
    let mut catalog = purchases_catalog_without_value_role();
    let amount = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "amount")
        .unwrap();
    amount.logical_type = VelorixLogicalTypeV1::Decimal {
        precision: 12,
        scale: 2,
    };
    amount.physical_arrow_type = ArrowPhysicalTypeV1::Decimal128 {
        precision: 12,
        scale: 2,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("decimal catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;

    let error = validate_supported_view_sql(
        "select user_id, avg(amount) as average from purchases group by user_id",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(error.to_string().contains("Int64"));
}

#[test]
fn tumbling_event_time_aggregate_sql_lowers_to_hashed_logical_view_plan() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_sql_to_logical_plan(sql, std::slice::from_ref(&catalog), &output_schema)
            .unwrap();

    assert_eq!(plan.plan_version, LOGICAL_VIEW_PLAN_VERSION_V1);
    assert!(plan
        .plan_hash
        .as_ref()
        .is_some_and(|hash| hash.starts_with(LOGICAL_VIEW_PLAN_HASH_PREFIX)));
    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert_eq!(supported.input_relation_id, "purchases");
    assert_eq!(supported.group_key_column_id, "user_id");
    assert_eq!(supported.event_time_column_id, "event_time");
    assert_eq!(supported.window_size_ns, 60_000_000_000);
    assert!(plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::TumblingWindow { .. })));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_lowers_min_max_avg_outputs() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_stats_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count, min(amount) as minimum_amount, max(amount) as maximum_amount, avg(amount) as average_amount from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_sql_to_logical_plan(sql, std::slice::from_ref(&catalog), &output_schema)
            .unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert_eq!(supported.sum_value_column_id, "amount");
    assert_eq!(supported.aggregate_outputs.len(), 5);
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Sum
            && aggregate.input_column_id.as_deref() == Some("amount")
            && aggregate.output_column_id == "total_amount"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Count
            && aggregate.input_column_id.is_none()
            && aggregate.output_column_id == "event_count"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Min
            && aggregate.input_column_id.as_deref() == Some("amount")
            && aggregate.output_column_id == "minimum_amount"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Max
            && aggregate.input_column_id.as_deref() == Some("amount")
            && aggregate.output_column_id == "maximum_amount"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Avg
            && aggregate.input_column_id.as_deref() == Some("amount")
            && aggregate.output_column_id == "average_amount"
    }));
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Aggregate { accumulators, .. }
            if accumulators
                .iter()
                .any(|acc| acc.function == LogicalPlanAggregateFunctionV1::Min)
                && accumulators
                    .iter()
                    .any(|acc| acc.function == LogicalPlanAggregateFunctionV1::Max)
                && accumulators
                    .iter()
                    .any(|acc| acc.function == LogicalPlanAggregateFunctionV1::Avg)
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_requires_declared_event_time_column() {
    let catalog = purchases_catalog_without_value_role();
    let error = validate_supported_tumbling_window_sql(
        "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(error.to_string().contains("event-time column"));
}

fn scores_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_user".to_string(),
        relation_name: "scores_by_user".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn scores_projected_stats_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_user".to_string(),
        relation_name: "scores_by_user".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000007".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "total_score".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "score_events".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "average_score".to_string(),
                data_type: SqlDataType::Float64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn purchases_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "purchases_by_user".to_string(),
        relation_name: "purchases_by_user".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000003".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn purchases_avg_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "purchase_metrics_by_user".to_string(),
        relation_name: "purchase_metrics_by_user".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000004".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "total".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "events".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "average".to_string(),
                data_type: SqlDataType::Float64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn purchases_min_max_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "purchase_extrema_by_user".to_string(),
        relation_name: "purchase_extrema_by_user".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000005".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "smallest".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "largest".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn purchases_window_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "purchases_by_user_minute".to_string(),
        relation_name: "purchases_by_user_minute".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000008".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "window_start".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "window_end".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "total_amount".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "event_count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec![
            "user_id".to_string(),
            "window_start".to_string(),
            "window_end".to_string(),
        ],
    }
}

fn purchases_window_stats_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "purchases_by_user_minute_stats".to_string(),
        relation_name: "purchases_by_user_minute_stats".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000009".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "window_start".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "window_end".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "total_amount".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "event_count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "minimum_amount".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "maximum_amount".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "average_amount".to_string(),
                data_type: SqlDataType::Float64,
                nullable: false,
            },
        ],
        primary_key: vec![
            "user_id".to_string(),
            "window_start".to_string(),
            "window_end".to_string(),
        ],
    }
}

fn latest_device_status_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "latest_device_status".to_string(),
        relation_name: "latest_device_status".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000007".to_string(),
        columns: vec![
            ColumnSchema {
                name: "device_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "enabled".to_string(),
                data_type: SqlDataType::Bool,
                nullable: false,
            },
        ],
        primary_key: vec!["device_id".to_string()],
    }
}

fn join_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_account".to_string(),
        relation_name: "scores_by_account".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000002".to_string(),
        columns: vec![
            ColumnSchema {
                name: "account_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["account_id".to_string()],
    }
}

#[test]
fn single_key_sum_count_fixture_sql_accepts_single_literal_comparison_filter() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, sum(score) as sum, count(*) as count from scores where score > -1 group by user_id",
        &catalog,
    )
    .unwrap();

    let predicate = plan.predicate.unwrap();
    assert_eq!(predicate.column_id, "score");
    assert_eq!(predicate.op, PredicateOp::Gt);
    assert_eq!(predicate.literal, json!(-1));
}

#[test]
fn two_input_pk_join_sum_count_fixture_sql_accepts_inner_equi_join_shape() {
    let orders = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[orders, accounts],
    )
    .unwrap();

    assert_eq!(plan.left_input_relation_id, "scores");
    assert_eq!(plan.right_input_relation_id, "accounts");
    assert_eq!(plan.left_join_key_column_id, "user_id");
    assert_eq!(plan.right_join_key_column_id, "account_id");
    assert_eq!(plan.group_key_relation_id, "accounts");
    assert_eq!(plan.group_key_column_id, "account_id");
    assert_eq!(plan.sum_value_relation_id, "scores");
    assert_eq!(plan.sum_value_column_id, "score");
}

#[test]
fn two_input_join_sum_count_uses_sql_sum_column_without_value_semantic_role() {
    let mut scores = scores_catalog();
    scores.relation_schema.columns[1].semantic_role = RelationSemanticRoleV1::Metadata;
    scores.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&scores.relation_schema).unwrap();
    scores.incremental_relation.schema_fingerprint = scores.schema_fingerprint.clone();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(plan.sum_value_column_id, "score");
}

#[test]
fn two_input_pk_join_sum_count_fixture_sql_rejects_mismatched_join_key_types() {
    let orders = scores_catalog();
    let mut accounts = accounts_catalog();
    accounts.relation_schema.columns[0].logical_type = VelorixLogicalTypeV1::Int64;
    accounts.relation_schema.columns[0].physical_arrow_type = ArrowPhysicalTypeV1::Int64;
    accounts.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&accounts.relation_schema).unwrap();
    accounts.incremental_relation.schema_fingerprint = accounts.schema_fingerprint.clone();

    let error = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[orders, accounts],
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(error
        .to_string()
        .contains("JOIN ON primary key columns must have identical physical Arrow types"));
}

#[test]
fn single_key_sum_count_fixture_sql_rejects_filter_on_non_runtime_visible_column() {
    let mut catalog = scores_catalog();
    catalog.relation_schema.columns.push(RelationColumnV1 {
        column_id: "status".to_string(),
        name: "status".to_string(),
        logical_type: VelorixLogicalTypeV1::Utf8,
        physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
        nullable: false,
        ordinal: 3,
        semantic_role: RelationSemanticRoleV1::Metadata,
    });
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();

    let error = validate_supported_view_sql(
        "select user_id, sum(score) as sum, count(*) as count from scores where status = 'paid' group by user_id",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(error
        .to_string()
        .contains("WHERE column must be the primary key or value column"));
}

#[test]
fn single_key_sum_count_fixture_sql_rejects_filter_on_weight_column() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select user_id, sum(score) as sum, count(*) as count from scores where delta = 1 group by user_id",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(error
        .to_string()
        .contains("WHERE column must be the primary key or value column"));
}

#[test]
fn single_key_sum_count_fixture_sql_rejects_qualified_column_references_for_now() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select s.user_id, sum(s.score) as sum, count(*) as count from scores as s group by s.user_id",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn unsupported_single_input_sql_families_fail_closed_without_logical_plan_fallback() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();
    let cases = [
        "select user_id, count(distinct score) as count from scores group by user_id",
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id having sum(score) > 0",
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id order by user_id",
        "with base as (select * from scores) select user_id, sum(score) as sum, count(*) as count from base group by user_id",
        "select user_id, sum(score) over (partition by user_id) as sum, count(*) as count from scores group by user_id",
    ];

    for sql in cases {
        let error = lower_supported_sql_to_logical_plan(
            sql,
            std::slice::from_ref(&catalog),
            &output_schema,
        )
        .unwrap_err();

        assert!(
            matches!(error, ViewPlanError::UnsupportedShape { .. }),
            "expected fail-closed unsupported admission for SQL `{sql}`, got `{error}`"
        );
    }
}

#[test]
fn unsupported_join_sql_families_fail_closed_without_logical_plan_fallback() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];
    let cases = [
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id group by a.account_id",
            "only INNER JOIN",
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where s.score > 0 group by a.account_id",
            "WHERE is not supported",
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id <> a.account_id group by a.account_id",
            "JOIN ON must use equality",
        ),
    ];

    for (sql, expected_reason) in cases {
        let error =
            lower_supported_sql_to_logical_plan(sql, &catalogs, &output_schema).unwrap_err();

        assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
        assert!(
            error.to_string().contains(expected_reason),
            "expected `{expected_reason}` in `{error}` for SQL: {sql}"
        );
    }
}

fn scores_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "scores".to_string(),
        relation_name: "scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
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
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "scores".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "scores".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn purchases_catalog_without_value_role() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "purchases".to_string(),
        relation_name: "purchases".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "user_id".to_string(),
                name: "user_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::Metadata,
            },
            RelationColumnV1 {
                column_id: "amount".to_string(),
                name: "amount".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Metadata,
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
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "purchases".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "purchases".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn purchases_event_time_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = purchases_catalog_without_value_role();
    catalog.relation_schema.columns.insert(
        2,
        RelationColumnV1 {
            column_id: "event_time".to_string(),
            name: "event_time".to_string(),
            logical_type: VelorixLogicalTypeV1::Int64,
            physical_arrow_type: ArrowPhysicalTypeV1::Int64,
            nullable: false,
            ordinal: 2,
            semantic_role: RelationSemanticRoleV1::EventTime,
        },
    );
    for (ordinal, column) in catalog.relation_schema.columns.iter_mut().enumerate() {
        column.ordinal = ordinal as u32;
    }
    catalog.relation_schema.event_time_column_id = Some("event_time".to_string());
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("event-time purchases schema should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
}

fn accounts_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "accounts".to_string(),
        relation_name: "accounts".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "limit".to_string(),
                name: "limit".to_string(),
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
        primary_key_column_ids: vec!["account_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "accounts".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "accounts".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn device_status_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "device_status".to_string(),
        relation_name: "device_status".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "device_id".to_string(),
                name: "device_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "enabled".to_string(),
                name: "enabled".to_string(),
                logical_type: VelorixLogicalTypeV1::Bool,
                physical_arrow_type: ArrowPhysicalTypeV1::Boolean,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "event_time".to_string(),
                name: "event_time".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::EventTime,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 3,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["device_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: Some("event_time".to_string()),
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "device_status".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "device_status".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}
