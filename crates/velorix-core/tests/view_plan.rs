use serde_json::json;
use velorix_core::{
    operator_contract::{
        AcceptedChangelogV1, ChangelogModeV1, NullabilityV1, StateBoundednessV1,
        UniquenessGuaranteeV1,
    },
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1, CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
    view_contract::{ColumnSchema, RelationSchema, SqlDataType},
    view_plan::{
        logical_view_plan_hash, lower_join_chain_to_binary_dag,
        lower_published_single_key_sum_count_sql,
        lower_supported_filter_project_sql_to_logical_plan,
        lower_supported_join_view_sql_to_logical_plan,
        lower_supported_latest_by_key_sql_to_logical_plan, lower_supported_sql_to_logical_plan,
        lower_supported_three_input_inner_join_count_sql_to_logical_plan_with_policy,
        lower_supported_tumbling_window_sql_to_logical_plan,
        lower_supported_view_sql_to_logical_plan, supported_join_view_plan_key_pairs,
        validate_logical_view_plan, validate_supported_analytic_row_number_sql,
        validate_supported_filter_project_sql, validate_supported_join_view_sql,
        validate_supported_latest_by_key_sql, validate_supported_tumbling_window_sql,
        validate_supported_view_sql, AggregateOutputPredicateExpr, JoinPredicateExpr,
        LogicalPlanAggregateFunctionV1, LogicalPlanBinaryJoinStepV1, LogicalPlanColumnRef,
        LogicalPlanCompositeJoinEqualityV1, LogicalPlanJoinKeyPairV1,
        LogicalPlanLatestByKeyFunctionV1, LogicalPlanStateKindV1, PlannerRelationInput,
        PredicateOp, RowPredicateExpr, SupportedAggregateInputRelationSide,
        SupportedAggregateOutputIdentity, SupportedAnalyticWindowFunction,
        SupportedCompositeJoinEqualityV1, SupportedEventTimeWindowKind, SupportedJoinKeyDomainV1,
        SupportedJoinKeyPairV1, SupportedJoinKind, SupportedProjectionExpr,
        VelorixLogicalViewExecutionV1, VelorixLogicalViewPlanNodeV1, VelorixLogicalViewPlanV1,
        ViewPlanError, COMPOSITE_PK_POSITIONAL_JSON_ARRAY_JOIN_KEY_CODEC_V1,
        INCREMENTAL_BAG_SEMANTICS_VERSION_V1, INCREMENTAL_KEY_SEMANTICS_VERSION_V1,
        LEFT_JOIN_INPUT_INSTANCE_ID_V1, LOGICAL_VIEW_PLAN_HASH_PREFIX,
        LOGICAL_VIEW_PLAN_VERSION_V1, LOGICAL_VIEW_PLAN_VERSION_V2,
        NON_PRIMARY_NON_NULL_SCALAR_JOIN_KEY_CODEC_V1, RIGHT_JOIN_INPUT_INSTANCE_ID_V1,
        SELF_JOIN_ATOMIC_FANOUT_PROTOCOL_V1, THREE_INPUT_LEGACY_SQL_ENCOUNTER_JOIN_ORDER_V1,
        THREE_INPUT_ROOT_FIXED_RIGHT_RELATION_ID_JOIN_ORDER_V1,
    },
};

#[test]
fn correlated_exists_and_not_exists_lower_to_generic_semi_anti_join_nodes() {
    let catalogs = vec![scores_catalog(), accounts_catalog()];
    let output = scores_projection_output_schema();
    let exists = lower_supported_sql_to_logical_plan(
        "select s.user_id, s.score from scores s where exists (select 1 from accounts a where a.account_id = s.user_id)",
        &catalogs,
        &output,
    )
    .unwrap();
    assert!(exists
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::SemiEquiJoin { .. })));
    assert!(matches!(
        exists.execution,
        VelorixLogicalViewExecutionV1::TwoInputSemiAntiJoinProject { .. }
    ));

    let not_exists = lower_supported_sql_to_logical_plan(
        "select s.user_id, s.score from scores s where not exists (select 1 from accounts a where a.account_id = s.user_id)",
        &catalogs,
        &output,
    )
    .unwrap();
    assert!(not_exists
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::AntiEquiJoin { .. })));
}

#[test]
fn correlated_exists_v1_fails_closed_outside_complete_non_null_pk_equality() {
    let catalogs = vec![scores_catalog(), accounts_catalog()];
    let output = scores_projection_output_schema();
    for sql in [
        "select s.user_id, s.score from scores s where exists (select 1 from accounts a where a.account_id = s.user_id and a.limit > 0)",
        "select s.user_id, s.score from scores s where exists (select 1 from accounts a where a.limit = s.score)",
        "select s.user_id, s.score from scores s where exists (select a.limit from accounts a where a.account_id = s.user_id)",
    ] {
        assert!(lower_supported_sql_to_logical_plan(sql, &catalogs, &output).is_err());
    }
}

#[test]
fn n_way_join_chain_lowers_to_a_left_deep_binary_dag() {
    let key = |relation_id: &str| LogicalPlanColumnRef {
        relation_id: relation_id.to_string(),
        input_instance_id: None,
        column_id: "id".to_string(),
    };
    let (nodes, output) = lower_join_chain_to_binary_dag(
        "scan_orders",
        &[
            LogicalPlanBinaryJoinStepV1 {
                node_id: "join_orders_customers".into(),
                right_input: "scan_customers".into(),
                left_key: key("orders"),
                right_key: key("customers"),
                composite_equality: None,
                join_kind: SupportedJoinKind::Inner,
            },
            LogicalPlanBinaryJoinStepV1 {
                node_id: "join_orders_customers_products".into(),
                right_input: "scan_products".into(),
                left_key: key("orders"),
                right_key: key("products"),
                composite_equality: None,
                join_kind: SupportedJoinKind::Inner,
            },
        ],
    )
    .unwrap();

    assert_eq!(output, "join_orders_customers_products");
    assert!(matches!(
        &nodes[0],
        VelorixLogicalViewPlanNodeV1::InnerEquiJoin { left, right, .. }
            if left == "scan_orders" && right == "scan_customers"
    ));
    assert!(matches!(
        &nodes[1],
        VelorixLogicalViewPlanNodeV1::InnerEquiJoin { left, right, .. }
            if left == "join_orders_customers" && right == "scan_products"
    ));
}

#[test]
fn three_input_composite_pk_sql_lowers_to_validated_binary_dag() {
    let catalogs = three_input_composite_join_catalogs();
    let output = three_input_join_count_output_schema();
    let sql = "select s.tenant_id, s.user_id, count(*) as count from scores s join accounts a on s.tenant_id = a.account_tenant_id and s.user_id = a.account_id join profiles p on s.tenant_id = p.account_tenant_id and s.user_id = p.account_id group by s.tenant_id, s.user_id";
    let plan = lower_supported_sql_to_logical_plan(sql, &catalogs, &output).unwrap();
    let VelorixLogicalViewExecutionV1::ThreeInputInnerJoinCount { plan: supported } =
        &plan.execution
    else {
        panic!("expected three-input execution");
    };
    assert_eq!(
        supported.ordered_input_relation_ids,
        ["scores", "accounts", "profiles"]
    );
    assert_eq!(supported.schema_version, 2);
    assert_eq!(
        supported.join_order_policy_id,
        THREE_INPUT_ROOT_FIXED_RIGHT_RELATION_ID_JOIN_ORDER_V1
    );
    assert_eq!(
        supported.root_primary_key_column_ids,
        ["tenant_id", "user_id"]
    );
    assert_eq!(
        supported.root_to_input_pk_permutations,
        [vec![0, 1], vec![1, 0], vec![1, 0]]
    );
    assert_eq!(
        plan.execution_implementation
            .as_ref()
            .unwrap()
            .join_key_codec_id
            .as_deref(),
        Some(COMPOSITE_PK_POSITIONAL_JSON_ARRAY_JOIN_KEY_CODEC_V1)
    );
    assert_eq!(
        plan.nodes
            .iter()
            .filter(|node| matches!(node, VelorixLogicalViewPlanNodeV1::InnerEquiJoin { .. }))
            .count(),
        2
    );

    let mut missing_step = plan.clone();
    missing_step.nodes.retain(|node| {
        !matches!(node, VelorixLogicalViewPlanNodeV1::InnerEquiJoin { node_id, .. } if node_id == "join_2")
    });
    assert!(validate_logical_view_plan(&missing_step).is_err());

    let wider = format!("{sql} having count(*) > 0");
    assert!(lower_supported_sql_to_logical_plan(&wider, &catalogs, &output).is_err());

    let reordered_sql = "select s.tenant_id, s.user_id, count(*) as count from scores s join profiles p on s.tenant_id = p.account_tenant_id and s.user_id = p.account_id join accounts a on s.tenant_id = a.account_tenant_id and s.user_id = a.account_id group by s.tenant_id, s.user_id";
    let reordered = lower_supported_sql_to_logical_plan(reordered_sql, &catalogs, &output).unwrap();
    assert_eq!(reordered.execution, plan.execution);
    assert_eq!(reordered.nodes, plan.nodes);
    assert_eq!(reordered.operator_dag_contract, plan.operator_dag_contract);
    assert_eq!(reordered.state_requirements, plan.state_requirements);
    assert_eq!(
        reordered.execution_implementation,
        plan.execution_implementation
    );
    assert_ne!(reordered.plan_hash, plan.plan_hash);

    let legacy = lower_supported_three_input_inner_join_count_sql_to_logical_plan_with_policy(
        sql,
        &catalogs,
        &output,
        THREE_INPUT_LEGACY_SQL_ENCOUNTER_JOIN_ORDER_V1,
    )
    .unwrap();
    let VelorixLogicalViewExecutionV1::ThreeInputInnerJoinCount { plan: legacy_plan } =
        &legacy.execution
    else {
        panic!("expected legacy three-input execution");
    };
    assert_eq!(legacy_plan.schema_version, 1);
    assert!(legacy_plan.join_order_policy_id.is_empty());
    let legacy_json = serde_json::to_value(&legacy).unwrap();
    assert!(legacy_json["execution"]["plan"]
        .get("join_order_policy_id")
        .is_none());
    let decoded: VelorixLogicalViewPlanV1 = serde_json::from_value(legacy_json).unwrap();
    validate_logical_view_plan(&decoded).unwrap();

    assert!(
        lower_supported_three_input_inner_join_count_sql_to_logical_plan_with_policy(
            sql,
            &catalogs,
            &output,
            "unknown-three-input-policy",
        )
        .is_err()
    );
    let mut tampered_policy = plan.clone();
    let VelorixLogicalViewExecutionV1::ThreeInputInnerJoinCount { plan } =
        &mut tampered_policy.execution
    else {
        unreachable!()
    };
    plan.join_order_policy_id = "unknown-three-input-policy".into();
    assert!(validate_logical_view_plan(&tampered_policy).is_err());
}

#[test]
fn composite_join_equality_has_one_canonical_versioned_representation() {
    let key = |relation_id: &str, column_id: &str| LogicalPlanColumnRef {
        relation_id: relation_id.to_string(),
        input_instance_id: None,
        column_id: column_id.to_string(),
    };
    let equality = LogicalPlanCompositeJoinEqualityV1 {
        schema_version: 1,
        additional_pairs: vec![LogicalPlanJoinKeyPairV1 {
            left_key: key("orders", "tenant_id"),
            right_key: key("customers", "tenant_id"),
        }],
    };
    let (nodes, _) = lower_join_chain_to_binary_dag(
        "scan_orders",
        &[LogicalPlanBinaryJoinStepV1 {
            node_id: "join_orders_customers".into(),
            right_input: "scan_customers".into(),
            left_key: key("orders", "account_id"),
            right_key: key("customers", "account_id"),
            composite_equality: Some(equality.clone()),
            join_kind: SupportedJoinKind::Inner,
        }],
    )
    .unwrap();

    let VelorixLogicalViewPlanNodeV1::InnerEquiJoin {
        left_key,
        right_key,
        composite_equality,
        ..
    } = &nodes[0]
    else {
        panic!("expected inner join");
    };
    assert_eq!(left_key.column_id, "account_id");
    assert_eq!(right_key.column_id, "account_id");
    assert_eq!(composite_equality.as_ref(), Some(&equality));
    let bytes = serde_json::to_vec(&nodes).unwrap();
    let decoded: Vec<VelorixLogicalViewPlanNodeV1> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
}

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

    assert_eq!(plan.plan_version, LOGICAL_VIEW_PLAN_VERSION_V2);
    assert_eq!(
        plan.key_semantics_version,
        INCREMENTAL_KEY_SEMANTICS_VERSION_V1
    );
    assert_eq!(
        plan.bag_semantics_version,
        INCREMENTAL_BAG_SEMANTICS_VERSION_V1
    );
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
fn aggregate_sql_admits_composite_columns_and_deterministic_scalar_group_keys() {
    let catalog = scores_with_category_catalog();

    let composite = validate_supported_view_sql(
        "select user_id, category, sum(score) as sum, count(*) as count from scores group by user_id, category",
        &catalog,
    )
    .unwrap();
    let Some(SupportedAggregateOutputIdentity::GroupKey {
        group_keys: composite_keys,
    }) = &composite.aggregate_output_identity
    else {
        panic!("expected explicit composite group identity");
    };
    assert_eq!(composite_keys.len(), 2);
    assert_eq!(
        composite_keys[1].input_column_id.as_deref(),
        Some("category")
    );

    let computed = validate_supported_view_sql(
        "select user_id, score / 10 as bucket, sum(score) as sum, count(*) as count from scores group by user_id, bucket",
        &catalog,
    )
    .unwrap();
    let Some(SupportedAggregateOutputIdentity::GroupKey {
        group_keys: computed_keys,
    }) = &computed.aggregate_output_identity
    else {
        panic!("expected explicit computed group identity");
    };
    assert_eq!(computed_keys.len(), 2);
    assert!(matches!(
        computed_keys[1].expression,
        Some(SupportedProjectionExpr::BinaryInt64 { .. })
    ));

    let error = validate_supported_view_sql(
        "select user_id, random() as bucket, sum(score) as sum from scores group by user_id, bucket",
        &catalog,
    )
    .unwrap_err();
    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    for sql in [
        "select user_id as first_key, user_id as second_key, sum(score) as sum from scores group by first_key, second_key",
        "select user_id, score + 1 as category, sum(score) as sum from scores group by user_id, category",
    ] {
        let error = validate_supported_view_sql(sql, &catalog).unwrap_err();
        assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    }

    let output_schema = RelationSchema {
        relation_id: "scores_by_user_bucket".to_string(),
        relation_name: "scores_by_user_bucket".to_string(),
        relation_version: "2026-08-10.v1".to_string(),
        schema_fingerprint:
            "sha256:1000000000000000000000000000000000000000000000000000000000000001".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "bucket".to_string(),
                data_type: SqlDataType::Int64,
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
        primary_key: vec!["user_id".to_string(), "bucket".to_string()],
    };
    let logical = lower_supported_view_sql_to_logical_plan(
        "select user_id, score / 10 as bucket, sum(score) as sum, count(*) as count from scores group by user_id, bucket",
        &catalog,
        &output_schema,
    )
    .unwrap();
    assert!(logical.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Project {
            computed_columns,
            ..
        } if computed_columns.len() == 1
    )));
    validate_logical_view_plan(&logical).unwrap();
}

#[test]
fn global_count_lowers_with_explicit_singleton_output_identity() {
    let catalog = scores_catalog();
    let output_schema = RelationSchema {
        relation_id: "score_count".to_string(),
        relation_name: "score_count".to_string(),
        relation_version: "2026-08-10.v1".to_string(),
        schema_fingerprint:
            "sha256:4000000000000000000000000000000000000000000000000000000000000004".to_string(),
        columns: vec![ColumnSchema {
            name: "count".to_string(),
            data_type: SqlDataType::Int64,
            nullable: false,
        }],
        primary_key: Vec::new(),
    };
    let logical = lower_supported_view_sql_to_logical_plan(
        "select count(*) as count from scores",
        &catalog,
        &output_schema,
    )
    .unwrap();
    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan } = &logical.execution else {
        panic!("expected aggregate execution");
    };
    assert_eq!(
        plan.aggregate_output_identity,
        Some(SupportedAggregateOutputIdentity::Singleton)
    );
    assert!(logical.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Aggregate { group_keys, .. } if group_keys.is_empty()
    )));
    let aggregate_contract = logical
        .operator_dag_contract
        .operators
        .iter()
        .find(|operator| operator.operator.kind == "aggregate")
        .unwrap();
    assert_eq!(
        aggregate_contract.outputs[0].uniqueness,
        UniquenessGuaranteeV1::Singleton
    );
    assert!(aggregate_contract.outputs[0].candidate_keys.is_empty());
    validate_logical_view_plan(&logical).unwrap();

    for sql in [
        "select sum(score) as sum from scores",
        "select count(*) as count from scores group by grouping sets (())",
    ] {
        let result = validate_supported_view_sql(sql, &catalog);
        assert!(
            matches!(&result, Err(ViewPlanError::UnsupportedShape { .. })),
            "expected fail-closed admission for `{sql}`, got {result:?}"
        );
    }
}

#[test]
fn aggregate_group_key_arity_is_structured_and_hash_visible() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();
    let one_key = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();
    let one_key_hash = logical_view_plan_hash(&one_key).unwrap();
    assert_eq!(
        one_key_hash,
        "velorix-logical-view-plan-sha256-v1:sha256:efbb4bbb1db5d038885bff59de0d959fb24e5e7c8efa6c295f04ba3a03feae8f"
    );

    let mut zero_keys = one_key.clone();
    let mut multiple_keys = one_key.clone();
    for node in &mut zero_keys.nodes {
        if let VelorixLogicalViewPlanNodeV1::Aggregate { group_keys, .. } = node {
            group_keys.clear();
        }
    }
    for node in &mut multiple_keys.nodes {
        if let VelorixLogicalViewPlanNodeV1::Aggregate { group_keys, .. } = node {
            group_keys.push(LogicalPlanColumnRef {
                relation_id: catalog.relation_schema.relation_id.clone(),
                input_instance_id: None,
                column_id: "score".to_string(),
            });
        }
    }

    let key_arity = |plan: &VelorixLogicalViewPlanV1| {
        plan.nodes
            .iter()
            .find_map(|node| match node {
                VelorixLogicalViewPlanNodeV1::Aggregate { group_keys, .. } => {
                    Some(group_keys.len())
                }
                _ => None,
            })
            .unwrap()
    };
    assert_eq!(key_arity(&zero_keys), 0);
    assert_eq!(key_arity(&one_key), 1);
    assert_eq!(key_arity(&multiple_keys), 2);

    let zero_key_hash = logical_view_plan_hash(&zero_keys).unwrap();
    let multiple_key_hash = logical_view_plan_hash(&multiple_keys).unwrap();
    assert_ne!(zero_key_hash, one_key_hash);
    assert_ne!(multiple_key_hash, one_key_hash);
    assert_ne!(zero_key_hash, multiple_key_hash);
}

#[test]
fn single_key_aggregate_sql_accepts_parenthesized_select() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "(select user_id, sum(score) as sum, count(*) as count from scores group by user_id)",
        &catalog,
        &output_schema,
    )
    .unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::SingleKeySumCount { .. }
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn parenthesized_set_operation_stays_fail_closed() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "(select user_id, sum(score) as sum, count(*) as count from scores group by user_id union select user_id, sum(score) as sum, count(*) as count from scores group by user_id)",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn aggregate_set_operation_sql_families_fail_closed_without_logical_plan_fallback() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();
    let cases = [
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id union select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id intersect select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id except select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
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
fn single_key_aggregate_sql_accepts_group_by_all() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by all",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(supported.group_key_column_id, "user_id");
    assert_eq!(supported.output_key_column_id, "user_id");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_plain_distinct_grouped_output() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select distinct user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::SingleKeySumCount { .. }
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_distinct_on_group_key_output() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select distinct on (user_id) user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::SingleKeySumCount { .. }
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_rejects_distinct_on_non_group_key() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let error = lower_supported_view_sql_to_logical_plan(
        "select distinct on (score) user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(error.to_string().contains("DISTINCT ON"));
}

#[test]
fn filtered_single_key_sum_count_sql_lowers_to_plan_with_filter_node() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores where score between 0 and 99 group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(
        supported
            .predicate_expr
            .as_ref()
            .expect("WHERE should be admitted")
            .leaf_predicates()
            .len(),
        2
    );
    assert_eq!(
        supported.predicate_expr.as_ref().unwrap().leaf_predicates()[0].op,
        PredicateOp::GtEq
    );
    assert_eq!(
        plan.nodes
            .iter()
            .filter(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { input, .. } if input.starts_with("scan_input") || input.starts_with("filter_input")))
            .count(),
        2
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_lowers_to_materialized_projection_plan() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, score from scores where score > 0",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    assert_eq!(supported.key_column_id, "user_id");
    assert_eq!(supported.output_key_column_id, "user_id");
    assert_eq!(supported.value_columns.len(), 1);
    assert_eq!(supported.value_columns[0].input_column_id, "score");
    assert_eq!(supported.value_columns[0].output_column_id, "score");
    assert!(supported.predicate_expr.is_some());
    assert!(plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { .. })));
    assert!(plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Project { .. })));
    assert!(plan
        .state_requirements
        .iter()
        .any(|state| state.state_kind == LogicalPlanStateKindV1::Projection));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_accepts_plain_select_distinct_when_primary_key_is_output_key() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select distinct user_id, score from scores where score > 0",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    assert_eq!(supported.key_column_id, "user_id");
    assert_eq!(supported.output_key_column_id, "user_id");
    assert_eq!(supported.value_columns.len(), 1);
    assert_eq!(supported.value_columns[0].input_column_id, "score");
    assert!(plan
        .state_requirements
        .iter()
        .any(|state| state.state_kind == LogicalPlanStateKindV1::Projection));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_rejects_plain_select_distinct_without_primary_key_output() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let error = lower_supported_sql_to_logical_plan(
        "select distinct score from scores where score > 0",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(
        error.to_string().contains("primary key"),
        "expected primary-key admission error, got `{error}`"
    );
}

#[test]
fn filter_project_sql_accepts_plain_select_distinct_when_output_key_is_projected_column() {
    let catalog = scores_catalog();
    let output_schema = scores_distinct_score_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select distinct score from scores where score > 0",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    assert_eq!(supported.key_column_id, "user_id");
    assert_eq!(
        supported.output_key_input_column_id.as_deref(),
        Some("score")
    );
    assert_eq!(supported.output_key_column_id, "score");
    assert!(supported.value_columns.is_empty());
    assert!(supported.predicate_expr.is_some());
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_rejects_non_distinct_projection_without_primary_key_output() {
    let catalog = scores_catalog();
    let output_schema = scores_distinct_score_output_schema();

    let error = lower_supported_sql_to_logical_plan(
        "select score from scores where score > 0",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(
        error.to_string().contains("primary key"),
        "expected primary-key admission error, got `{error}`"
    );
}

#[test]
fn filter_project_sql_rejects_plain_select_distinct_with_primary_key_value_duplicate() {
    let catalog = scores_catalog();
    let mut output_schema = scores_projection_output_schema();
    output_schema.schema_fingerprint = "scores-duplicate-key-projection-v1".to_string();
    output_schema.columns[1].name = "user_id_copy".to_string();
    output_schema.columns[1].data_type = SqlDataType::Utf8;

    let error = lower_supported_sql_to_logical_plan(
        "select distinct user_id, user_id as user_id_copy from scores where score > 0",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(
        error.to_string().contains("primary key exactly once"),
        "expected primary-key exact-once admission error, got `{error}`"
    );
}

#[test]
fn filter_project_sql_rejects_distinct_on_filter_project_shape() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let error = lower_supported_sql_to_logical_plan(
        "select distinct on (score) user_id, score from scores where score > 0",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(
        error.to_string().contains("plain SELECT"),
        "expected plain SELECT admission error, got `{error}`"
    );
}

#[test]
fn filter_project_sql_expands_select_star_in_schema_order() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select * from scores where score > 0",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    assert_eq!(supported.key_column_id, "user_id");
    assert_eq!(supported.output_key_column_id, "user_id");
    assert_eq!(
        supported
            .value_columns
            .iter()
            .map(|column| column.input_column_id.as_str())
            .collect::<Vec<_>>(),
        vec!["score"]
    );
    assert_eq!(
        supported
            .value_columns
            .iter()
            .map(|column| column.output_column_id.as_str())
            .collect::<Vec<_>>(),
        vec!["score"]
    );
    assert!(supported.predicate_expr.is_some());
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_expands_qualified_select_star_in_schema_order() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    for sql in [
        "select scores.* from scores where score > 0",
        "select s.* from scores s where s.score > 0",
    ] {
        let plan = lower_supported_sql_to_logical_plan(
            sql,
            std::slice::from_ref(&catalog),
            &output_schema,
        )
        .unwrap();

        let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution
        else {
            panic!("expected filter/project runtime execution for SQL `{sql}`");
        };
        assert_eq!(supported.key_column_id, "user_id");
        assert_eq!(supported.output_key_column_id, "user_id");
        assert_eq!(
            supported
                .value_columns
                .iter()
                .map(|column| column.input_column_id.as_str())
                .collect::<Vec<_>>(),
            vec!["score"]
        );
        assert_eq!(
            supported
                .value_columns
                .iter()
                .map(|column| column.output_column_id.as_str())
                .collect::<Vec<_>>(),
            vec!["score"]
        );
        assert!(supported.predicate_expr.is_some());
        validate_logical_view_plan(&plan).unwrap();
    }
}

#[test]
fn filter_project_sql_rejects_unsupported_qualified_select_star() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    for sql in [
        "select wrong.* from scores where score > 0",
        "select scores.* from scores s where s.score > 0",
    ] {
        let error = lower_supported_sql_to_logical_plan(
            sql,
            std::slice::from_ref(&catalog),
            &output_schema,
        )
        .unwrap_err();

        assert!(
            matches!(error, ViewPlanError::UnsupportedShape { .. }),
            "expected unsupported admission for SQL `{sql}`, got `{error}`"
        );
    }

    let accounts = accounts_catalog();
    let join_output_schema = join_output_schema();
    let sql = "select s.* from scores s join accounts a on s.user_id = a.account_id";
    let error = lower_supported_sql_to_logical_plan(sql, &[catalog, accounts], &join_output_schema)
        .unwrap_err();

    assert!(
        matches!(error, ViewPlanError::UnsupportedShape { .. }),
        "expected unsupported admission for SQL `{sql}`, got `{error}`"
    );
}

#[test]
fn filter_project_sql_expands_select_star_over_identity_source_filters() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    for sql in [
        "select * from (select * from scores where score > 0) s",
        "with score_source as (select * from scores where score > 0) select * from score_source",
    ] {
        let plan = lower_supported_sql_to_logical_plan(
            sql,
            std::slice::from_ref(&catalog),
            &output_schema,
        )
        .unwrap();

        let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution
        else {
            panic!("expected filter/project runtime execution for SQL `{sql}`");
        };
        assert_eq!(supported.key_column_id, "user_id");
        assert_eq!(supported.output_key_column_id, "user_id");
        assert_eq!(
            supported
                .value_columns
                .iter()
                .map(|column| column.input_column_id.as_str())
                .collect::<Vec<_>>(),
            vec!["score"]
        );
        assert!(supported.predicate_expr.is_some());
        validate_logical_view_plan(&plan).unwrap();
    }
}

#[test]
fn filter_project_sql_rejects_select_star_over_non_identity_sources() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    for sql in [
        "select * from (select user_id, score from scores where score > 0) s",
        "with score_source as (select user_id, score from scores where score > 0) select * from score_source",
        "select * from (select user_id as id, score from scores where score > 0) s",
        "select * from (select user_id, score + 1 from scores where score > 0) s",
    ] {
        let error = lower_supported_sql_to_logical_plan(
            sql,
            std::slice::from_ref(&catalog),
            &output_schema,
        )
        .unwrap_err();

        assert!(
            matches!(error, ViewPlanError::UnsupportedShape { .. }),
            "expected unsupported admission for SQL `{sql}`, got `{error}`"
        );
    }
}

#[test]
fn row_number_sql_lowers_to_analytic_row_number_plan() {
    let catalog = scores_catalog();
    let output_schema = scores_row_number_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores where score > 0",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan: supported } = &plan.execution
    else {
        panic!("expected analytic row-number runtime execution");
    };
    assert_eq!(supported.key_column_id, "user_id");
    assert_eq!(supported.partition_column_id, "score");
    assert_eq!(supported.order_column_id, "score");
    assert!(supported.order_descending);
    assert_eq!(supported.output_row_number_column_id, "rank");
    assert!(supported.predicate_expr.is_some());
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::RowNumber {
            partition_column,
            order_column,
            descending: true,
            ..
        } if partition_column.column_id == "score" && order_column.column_id == "score"
    )));
    assert!(plan
        .state_requirements
        .iter()
        .any(|state| state.state_kind == LogicalPlanStateKindV1::RowNumber));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn rank_sql_lowers_to_analytic_rank_plan() {
    let catalog = scores_catalog();
    let output_schema = scores_row_number_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, rank() over (partition by score order by score desc) as rank from scores where score > 0",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan: supported } = &plan.execution
    else {
        panic!("expected analytic rank runtime execution");
    };
    assert_eq!(supported.key_column_id, "user_id");
    assert_eq!(supported.function, SupportedAnalyticWindowFunction::Rank);
    assert_eq!(supported.partition_column_id, "score");
    assert_eq!(supported.order_column_id, "score");
    assert!(supported.order_descending);
    assert_eq!(supported.output_row_number_column_id, "rank");
    assert!(supported.predicate_expr.is_some());
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn dense_rank_sql_lowers_to_analytic_dense_rank_plan() {
    let catalog = scores_catalog();
    let output_schema = scores_row_number_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, dense_rank() over (partition by score order by score desc) as rank from scores where score > 0",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan: supported } = &plan.execution
    else {
        panic!("expected analytic dense-rank runtime execution");
    };
    assert_eq!(supported.key_column_id, "user_id");
    assert_eq!(
        supported.function,
        SupportedAnalyticWindowFunction::DenseRank
    );
    assert_eq!(supported.partition_column_id, "score");
    assert_eq!(supported.order_column_id, "score");
    assert!(supported.order_descending);
    assert_eq!(supported.output_row_number_column_id, "rank");
    assert!(supported.predicate_expr.is_some());
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn row_number_sql_lowers_wrapped_rank_top_n() {
    let catalog = scores_catalog();
    let output_schema = scores_row_number_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, rank from (select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores where score > 0) ranked where rank <= 2",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan: supported } = &plan.execution
    else {
        panic!("expected analytic row-number runtime execution");
    };
    assert_eq!(supported.output_row_number_column_id, "rank");
    assert_eq!(supported.rank_limit, Some(2));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn row_number_sql_lowers_wrapped_rank_top_n_with_implicit_primary_key_tie_breaker() {
    let catalog = scores_catalog();
    let output_schema = scores_row_number_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, rank from (select user_id, row_number() over (partition by score order by score desc) as rank from scores where score > 0) ranked where rank <= 2",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan: supported } = &plan.execution
    else {
        panic!("expected analytic row-number runtime execution");
    };
    assert_eq!(supported.key_column_id, "user_id");
    assert_eq!(supported.order_column_id, "score");
    assert!(supported.order_descending);
    assert_eq!(supported.rank_limit, Some(2));
    assert!(plan.state_requirements.iter().any(|state| state
        .key_columns
        .iter()
        .any(|column| column.column_id == "user_id")));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn row_number_sql_lowers_qualify_rank_equal_one() {
    let catalog = scores_catalog();
    let output_schema = scores_row_number_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores qualify rank = 1",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan: supported } = &plan.execution
    else {
        panic!("expected analytic row-number runtime execution");
    };
    assert_eq!(supported.rank_limit, Some(1));
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::RowNumber {
            rank_limit: Some(1),
            ..
        }
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn row_number_sql_lowers_qualify_rank_equal_one_with_implicit_primary_key_tie_breaker() {
    let catalog = scores_catalog();
    let output_schema = scores_row_number_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, row_number() over (partition by score order by score asc) as rank from scores qualify rank = 1",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan: supported } = &plan.execution
    else {
        panic!("expected analytic row-number runtime execution");
    };
    assert!(!supported.order_descending);
    assert_eq!(supported.rank_limit, Some(1));
    assert!(plan.state_requirements.iter().any(|state| state
        .key_columns
        .iter()
        .any(|column| column.column_id == "user_id")));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn row_number_sql_lowers_wrapped_rank_equal_one() {
    let catalog = scores_catalog();
    let output_schema = scores_row_number_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, rank from (select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores) ranked where rank = 1",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan: supported } = &plan.execution
    else {
        panic!("expected analytic row-number runtime execution");
    };
    assert_eq!(supported.rank_limit, Some(1));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn row_number_sql_accepts_identity_cte_source_filter_and_outer_where() {
    let catalog = scores_catalog();
    let output_schema = scores_row_number_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "with score_source as (select * from scores where user_id <> 'aaron') select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from score_source where user_id <> 'alice'",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan: supported } = &plan.execution
    else {
        panic!("expected analytic row-number runtime execution");
    };
    let predicates = supported
        .predicate_expr
        .as_ref()
        .expect("CTE and outer WHERE filters should lower to runtime predicate")
        .leaf_predicates();
    assert_eq!(predicates.len(), 2);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn row_number_sql_accepts_derived_source_filter_and_outer_where() {
    let catalog = scores_catalog();
    let output_schema = scores_row_number_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select s.user_id, row_number() over (partition by s.score order by s.score desc, s.user_id asc) as rank from (select * from scores where user_id <> 'aaron') s where s.user_id <> 'alice'",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::AnalyticRowNumber { plan: supported } = &plan.execution
    else {
        panic!("expected analytic row-number runtime execution");
    };
    let predicates = supported
        .predicate_expr
        .as_ref()
        .expect("derived source and outer WHERE filters should lower to runtime predicate")
        .leaf_predicates();
    assert_eq!(predicates.len(), 2);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn row_number_sql_rejects_source_filter_on_non_runtime_visible_column() {
    let catalog = scores_catalog();

    let error = validate_supported_analytic_row_number_sql(
        "select s.user_id, row_number() over (partition by s.score order by s.score desc, s.user_id asc) as rank from (select * from scores where delta > 0) s",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn row_number_sql_rejects_unsupported_window_shapes() {
    let catalog = scores_catalog();

    for sql in [
        "select user_id, row_number() over (partition by score order by score desc) as rank from scores",
        "select user_id, row_number() over (partition by score order by delta desc, user_id asc) as rank from scores",
        "select user_id, row_number() over (partition by score order by score desc, user_id desc) as rank from scores",
        "select user_id, row_number() over (partition by score order by score desc, user_id asc rows between unbounded preceding and current row) as rank from scores",
        "select user_id, dense_rank() over (partition by score order by score desc, user_id asc) as rank from scores",
        "select user_id, rank from (select user_id, dense_rank() over (partition by score order by score desc) as rank from scores) ranked where rank <= 2",
        "select user_id, sum(score) over (partition by score order by score desc, user_id asc) as rank from scores",
        "select user_id, row_number() over w as rank from scores window w as (partition by score order by score desc, user_id asc)",
        "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores group by user_id",
        "select user_id, row_number() over (partition by score order by score desc, user_id asc) as user_id from scores",
        "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores qualify rank = 2",
        "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores qualify rank > 2",
        "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores qualify rank between 1 and 2",
        "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores qualify rank <= 0",
        "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores qualify score <= 2",
        "select user_id, rank from (select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores) ranked where rank = 2",
        "select user_id, rank from (select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores) ranked where rank > 2",
        "select user_id, rank from (select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores) ranked where rank between 1 and 2",
        "select user_id, rank from (select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores) ranked where rank <= 0",
        "select user_id, rank from (select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores) ranked where score <= 2",
        "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores union select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores",
        "select s.user_id, row_number() over (partition by s.score order by s.score desc, s.user_id asc) as rank from scores s join accounts a on s.user_id = a.account_id",
    ] {
        let error = validate_supported_analytic_row_number_sql(sql, &catalog).unwrap_err();

        assert!(
            matches!(error, ViewPlanError::UnsupportedShape { .. }),
            "{error}"
        );
    }
}

#[test]
fn row_number_sql_rejects_nullable_partition_or_order_columns() {
    let mut catalog = scores_catalog();
    let score = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    let schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;

    let error = validate_supported_analytic_row_number_sql(
        "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn filter_project_union_distinct_same_relation_lowers_to_filter_project_plan() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores where score > 0 union distinct select user_id, score from scores where score >= 10",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project execution");
    };
    assert_eq!(supported.input_relation_id, "scores");
    assert!(supported
        .predicate_expr
        .as_ref()
        .is_some_and(RowPredicateExpr::contains_or));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_union_distinct_unfiltered_branch_lowers_to_unfiltered_projection() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores union select user_id, score from scores where score > 0",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project execution");
    };
    assert!(supported.predicate_expr.is_none());
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_intersect_distinct_same_relation_lowers_to_filter_project_plan() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores where score > 0 intersect distinct select user_id, score from scores where score >= 10",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project execution");
    };
    assert_eq!(supported.input_relation_id, "scores");
    assert!(matches!(
        supported.predicate_expr.as_ref(),
        Some(RowPredicateExpr::And { .. })
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_except_distinct_filtered_left_lowers_to_left_and_not_right() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores where score > 0 except distinct select user_id, score from scores where score >= 10",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project execution");
    };
    assert_eq!(supported.input_relation_id, "scores");
    let Some(RowPredicateExpr::And { left, right }) = supported.predicate_expr.as_ref() else {
        panic!("expected left predicate AND negated right predicate");
    };
    assert!(matches!(
        left.as_ref(),
        RowPredicateExpr::Atom { predicate }
            if predicate.column_id == "score" && predicate.op == PredicateOp::Gt && predicate.literal == json!(0)
    ));
    assert!(matches!(
        right.as_ref(),
        RowPredicateExpr::Atom { predicate }
            if predicate.column_id == "score" && predicate.op == PredicateOp::Lt && predicate.literal == json!(10)
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_except_distinct_unfiltered_left_lowers_to_not_right() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores except select user_id, score from scores where score >= 10",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project execution");
    };
    assert!(matches!(
        supported.predicate_expr.as_ref(),
        Some(RowPredicateExpr::Atom { predicate })
            if predicate.column_id == "score" && predicate.op == PredicateOp::Lt && predicate.literal == json!(10)
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_unsupported_set_ops_fail_closed() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();
    let cases = [
        "select user_id, score from scores union all select user_id, score from scores",
        "select user_id, score from scores intersect all select user_id, score from scores",
        "select user_id, score from scores except all select user_id, score from scores where score > 0",
        "select user_id, score from scores intersect select user_id, score from scores where score > 0",
        "select user_id, score from scores intersect select user_id, score + 1 as score from scores where score > 0",
        "select user_id, score from scores where score > 0 intersect select account_id, tier from accounts where tier = 'gold'",
        "select user_id, score from scores where score > 0 except select user_id, score from scores",
        "select user_id, score from scores where score > 0 except select user_id, score + 1 as score from scores where score >= 10",
        "select user_id, score from scores where score > 0 except select account_id, tier from accounts where tier = 'gold'",
        "select user_id, score from scores union select user_id, score + 1 as score from scores",
    ];

    for sql in cases {
        let error =
            lower_supported_filter_project_sql_to_logical_plan(sql, &catalog, &output_schema)
                .unwrap_err();
        assert!(
            matches!(error, ViewPlanError::UnsupportedShape { .. }),
            "expected fail-closed unsupported admission for SQL `{sql}`, got `{error}`"
        );
    }
}

#[test]
fn filter_project_sql_accepts_parenthesized_select() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "(select user_id, score from scores where score > 0)",
        &catalog,
        &output_schema,
    )
    .unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::FilterProject { .. }
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_accepts_identity_cte_source_filters() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "with score_source as (select * from scores where score > 0) select user_id, score from score_source where user_id <> 'bob'",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    let predicate_expr = supported
        .predicate_expr
        .as_ref()
        .expect("CTE and outer WHERE filters should lower to runtime predicate");
    assert_eq!(predicate_expr.leaf_predicates().len(), 2);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_accepts_derived_table_source_filters() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, score from (select * from scores where score > 0) s where s.user_id <> 'bob'",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    let predicate_expr = supported
        .predicate_expr
        .as_ref()
        .expect("derived table and outer WHERE filters should lower to runtime predicate");
    assert_eq!(predicate_expr.leaf_predicates().len(), 2);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn subquery_admission_uses_existing_relational_nodes_or_fails_closed() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();
    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, score from (select * from scores where score > 0) s where s.user_id <> 'bob'",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();
    assert!(plan.nodes.iter().all(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::RelationScan { .. }
            | VelorixLogicalViewPlanNodeV1::Filter { .. }
            | VelorixLogicalViewPlanNodeV1::Project { .. }
            | VelorixLogicalViewPlanNodeV1::Output { .. }
    )));
    validate_logical_view_plan(&plan).unwrap();

    for sql in [
        "select user_id, score from scores where score > (select max(score) from scores)",
        "select s.user_id, s.score from scores s where exists (select 1 from scores t where t.user_id = s.user_id)",
        "select user_id, score from (select user_id, max(score) as score from scores group by user_id) s",
    ] {
        assert!(matches!(
            lower_supported_sql_to_logical_plan(
                sql,
                std::slice::from_ref(&catalog),
                &output_schema,
            ),
            Err(ViewPlanError::UnsupportedShape { .. })
        ));
    }
}

#[test]
fn nullable_in_and_not_in_forms_fail_closed() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();
    for sql in [
        "select user_id, score from scores where score in (1, null)",
        "select user_id, score from scores where score not in (1, null)",
    ] {
        let error = lower_supported_sql_to_logical_plan(
            sql,
            std::slice::from_ref(&catalog),
            &output_schema,
        )
        .unwrap_err();
        assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
        assert!(error.to_string().contains("null-aware predicate semantics"));
    }

    let mut nullable_scores = scores_catalog();
    nullable_scores.relation_schema.columns[1].nullable = true;
    let fingerprint =
        SchemaFingerprintV1::for_relation_schema(&nullable_scores.relation_schema).unwrap();
    nullable_scores.schema_fingerprint = fingerprint.clone();
    nullable_scores.incremental_relation.schema_fingerprint = fingerprint;
    let mut nullable_output = scores_projection_output_schema();
    nullable_output.columns[1].nullable = true;
    for sql in [
        "select user_id, score from scores where score in (1, 2)",
        "select user_id, score from scores where score not in (1, 2)",
    ] {
        let error = lower_supported_sql_to_logical_plan(
            sql,
            std::slice::from_ref(&nullable_scores),
            &nullable_output,
        )
        .unwrap_err();
        assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
        assert!(error.to_string().contains("null-aware predicate semantics"));
    }

    let mut nullable_accounts = accounts_catalog();
    nullable_accounts.relation_schema.columns[1].nullable = true;
    let fingerprint =
        SchemaFingerprintV1::for_relation_schema(&nullable_accounts.relation_schema).unwrap();
    nullable_accounts.schema_fingerprint = fingerprint.clone();
    nullable_accounts.incremental_relation.schema_fingerprint = fingerprint;
    for sql in [
        "select s.user_id, s.score from scores s where s.score in (select a.limit from accounts a)",
        "select s.user_id, s.score from scores s where s.score not in (select a.limit from accounts a)",
    ] {
        assert!(matches!(
            lower_supported_sql_to_logical_plan(
                sql,
                &[catalog.clone(), nullable_accounts.clone()],
                &output_schema,
            ),
            Err(ViewPlanError::UnsupportedShape { .. })
        ));
    }
}

#[test]
fn filter_project_sql_accepts_source_projection_aliases() {
    let catalog = scores_catalog();
    let mut output_schema = scores_projection_output_schema();
    output_schema.columns[0].name = "id".to_string();
    output_schema.columns[1].name = "points".to_string();
    output_schema.primary_key = vec!["id".to_string()];

    for sql in [
        "with src as (select user_id as id, score as points from scores where score > 0) select id, points from src where id <> 'bob'",
        "select s.id, s.points from (select user_id as id, score as points from scores where score > 0) s where s.id <> 'bob'",
    ] {
        let plan = lower_supported_sql_to_logical_plan(
            sql,
            std::slice::from_ref(&catalog),
            &output_schema,
        )
        .unwrap();

        let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution
        else {
            panic!("expected filter/project runtime execution for SQL `{sql}`");
        };
        assert_eq!(supported.key_column_id, "user_id");
        assert_eq!(supported.output_key_column_id, "id");
        assert_eq!(supported.value_columns.len(), 1);
        assert_eq!(supported.value_columns[0].input_column_id, "score");
        assert_eq!(supported.value_columns[0].output_column_id, "points");
        let predicate_expr = supported
            .predicate_expr
            .as_ref()
            .expect("source and outer filters should lower");
        assert_eq!(predicate_expr.leaf_predicates().len(), 2);
        validate_logical_view_plan(&plan).unwrap();
    }
}

#[test]
fn filter_project_sql_accepts_source_projection_alias_scalar_predicates() {
    let catalog = scores_catalog();
    let mut output_schema = scores_projection_output_schema();
    output_schema.columns[0].name = "id".to_string();
    output_schema.columns[1].name = "points".to_string();
    output_schema.primary_key = vec!["id".to_string()];

    let plan = lower_supported_sql_to_logical_plan(
        "with src as (select user_id as id, score as points from scores) select id, points from src where points + 1 > 10 and id <> 'bob'",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    let Some(RowPredicateExpr::And { left, right }) = supported.predicate_expr.as_ref() else {
        panic!("expected combined predicate expression");
    };
    assert!(matches!(
        left.as_ref(),
        RowPredicateExpr::ScalarInt64Comparison {
            comparison_op: PredicateOp::Gt,
            ..
        }
    ));
    assert!(matches!(
        right.as_ref(),
        RowPredicateExpr::Atom { predicate }
            if predicate.column_id == "user_id" && predicate.op == PredicateOp::NotEq
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_accepts_source_projection_alias_expression_predicates() {
    let catalog = scores_with_adjustment_catalog();
    let mut output_schema = scores_projection_output_schema();
    output_schema.columns[0].name = "id".to_string();
    output_schema.columns[1].name = "points".to_string();
    output_schema.primary_key = vec!["id".to_string()];

    let plan = lower_supported_sql_to_logical_plan(
        "with src as (select user_id as id, score as points, user_id_adjustment as adj from scores) select id, points from src where points + adj > 10",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    assert!(matches!(
        supported.predicate_expr.as_ref(),
        Some(RowPredicateExpr::ScalarInt64ExpressionComparison {
            comparison_op: PredicateOp::Gt,
            left,
            right,
        }) if matches!(
            left.as_ref(),
            SupportedProjectionExpr::BinaryInt64 { .. }
        ) && matches!(
            right.as_ref(),
            SupportedProjectionExpr::LiteralInt64 { value: 10 }
        )
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_rejects_source_projection_alias_edges() {
    let catalog = scores_catalog();
    let mut output_schema = scores_projection_output_schema();
    output_schema.columns[0].name = "id".to_string();
    output_schema.columns[1].name = "points".to_string();
    output_schema.primary_key = vec!["id".to_string()];

    for sql in [
        "with src as (select user_id as id, score + 1 as points from scores) select id, points from src",
        "with src as (select user_id as id, score as id from scores) select id, points from src",
        "with src as (select user_id as score, score from scores) select score, score from src",
        "with src as (select user_id as id from scores) select id, points from src",
        "with src as (select user_id as id, score as points from (select * from scores) nested) select id, points from src",
        "with src as (select user_id as id, score as points from scores join accounts on user_id = account_id) select id, points from src",
        "with src as (select user_id as id, score + 1 as points from scores) select id, points from src where points > 10",
        "with src as (select user_id as id, score as points from scores) select id, points from src where score + 1 > 10",
        "with src as (select user_id as id, score as points from scores) select id, points from src where points + 1 > id",
        "with src as (select user_id as id, score as points from scores) select id, points from src where case when points > 0 then points else 0 end > 10",
        "with src as (select user_id as id, score as points from scores) select id, points from src where if(points > 0, points, 0) > 10",
    ] {
        let error = lower_supported_sql_to_logical_plan(
            sql,
            std::slice::from_ref(&catalog),
            &output_schema,
        )
        .unwrap_err();

        assert!(
            matches!(error, ViewPlanError::UnsupportedShape { .. }),
            "expected unsupported admission for SQL `{sql}`, got `{error}`"
        );
    }
}

#[test]
fn filter_project_sql_accepts_inner_source_alias_filters() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let cte_plan = lower_supported_sql_to_logical_plan(
        "with score_source as (select * from scores src where src.score > 0) select user_id, score from score_source where user_id <> 'bob'",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();
    let derived_plan = lower_supported_sql_to_logical_plan(
        "select user_id, score from (select * from scores src where src.score > 0) s where s.user_id <> 'bob'",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    for plan in [cte_plan, derived_plan] {
        let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution
        else {
            panic!("expected filter/project runtime execution");
        };
        assert_eq!(
            supported
                .predicate_expr
                .as_ref()
                .expect("source and outer filters should lower")
                .leaf_predicates()
                .len(),
            2
        );
        validate_logical_view_plan(&plan).unwrap();
    }
}

#[test]
fn single_key_aggregate_sql_accepts_matching_aggregate_filter_predicates() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) filter (where score > 5) as sum, count(*) filter (where score > 5) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    let leaves = supported
        .predicate_expr
        .as_ref()
        .expect("aggregate FILTER should lower to input predicate")
        .leaf_predicates();
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves[0].column_id, "score");
    assert_eq!(leaves[0].op, PredicateOp::Gt);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_mixed_aggregate_filter_predicates() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) filter (where score > 5) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert!(supported.predicate_expr.is_none());
    assert_eq!(supported.aggregate_filter_exprs.len(), 1);
    assert!(supported.aggregate_filter_exprs.contains_key("sum"));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_different_aggregate_filter_predicates() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) filter (where score > 5) as sum, count(*) filter (where score <= 5) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert!(supported.predicate_expr.is_none());
    assert_eq!(supported.aggregate_filter_exprs.len(), 2);
    assert!(supported.aggregate_filter_exprs.contains_key("sum"));
    assert!(supported.aggregate_filter_exprs.contains_key("count"));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_filtered_count_distinct_with_mixed_filters() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) filter (where score > 5) as sum, count(distinct score) filter (where score > 0) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert!(supported.predicate_expr.is_none());
    assert_eq!(
        supported.aggregate_outputs[1].function,
        LogicalPlanAggregateFunctionV1::CountDistinct
    );
    assert_eq!(supported.aggregate_filter_exprs.len(), 2);
    assert!(supported.aggregate_filter_exprs.contains_key("sum"));
    assert!(supported.aggregate_filter_exprs.contains_key("count"));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_mixed_min_max_avg_filters() {
    let catalog = scores_catalog();
    let output_schema = scores_min_max_avg_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, min(score) filter (where score > 0) as min_pos, max(score) filter (where score <= 0) as max_nonpos, avg(score) filter (where score > 10) as avg_hi from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert!(supported.predicate_expr.is_none());
    assert_eq!(supported.aggregate_outputs.len(), 3);
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Min
            && aggregate.input_column_id.as_deref() == Some("score")
            && aggregate.output_column_id == "min_pos"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Max
            && aggregate.input_column_id.as_deref() == Some("score")
            && aggregate.output_column_id == "max_nonpos"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Avg
            && aggregate.input_column_id.as_deref() == Some("score")
            && aggregate.output_column_id == "avg_hi"
    }));
    assert_eq!(supported.aggregate_filter_exprs.len(), 3);
    assert!(supported.aggregate_filter_exprs.contains_key("min_pos"));
    assert!(supported.aggregate_filter_exprs.contains_key("max_nonpos"));
    assert!(supported.aggregate_filter_exprs.contains_key("avg_hi"));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_mixed_aggregate_filters_with_having_and_top_k() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) filter (where score > 5) as sum, count(*) as count from scores group by user_id having sum > 0 order by sum desc limit 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert!(supported.predicate_expr.is_none());
    assert_eq!(supported.aggregate_filter_exprs.len(), 1);
    assert!(supported.aggregate_filter_exprs.contains_key("sum"));
    assert!(supported.having_expr.is_some());
    assert_eq!(
        supported.top_k.as_ref().unwrap().order_output_column_id,
        "sum"
    );
    assert_eq!(supported.top_k.as_ref().unwrap().offset, 0);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_order_by_limit_offset_top_k() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id order by sum desc, user_id asc limit 2 offset 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    let top_k = supported.top_k.as_ref().unwrap();
    assert_eq!(top_k.order_output_column_id, "sum");
    assert_eq!(
        top_k.tie_breaker_output_column_id.as_deref(),
        Some("user_id")
    );
    assert!(top_k.descending);
    assert_eq!(top_k.limit, 2);
    assert_eq!(top_k.offset, 1);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_rejects_unsupported_limit_offset_shapes() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();
    let cases = [
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id order by sum desc limit 2 offset delta",
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id order by sum desc limit 2 offset -1",
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id order by sum desc limit 1, 2",
    ];

    for sql in cases {
        let error =
            lower_supported_view_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap_err();

        assert!(
            matches!(error, ViewPlanError::UnsupportedShape { .. }),
            "expected fail-closed unsupported admission for SQL `{sql}`, got `{error}`"
        );
    }
}

#[test]
fn filtered_single_key_sum_count_sql_admits_or_without_fake_filter_node() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 or score = -3 group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert!(supported
        .predicate_expr
        .as_ref()
        .is_some_and(RowPredicateExpr::contains_or));
    assert!(!plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { .. })));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_in_and_not_in_predicates() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores where score in (1, 2, 3) and score not in (-1, -2) group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    let predicate = supported
        .predicate_expr
        .as_ref()
        .expect("WHERE should be admitted");
    assert!(predicate.contains_or());
    let leaves = predicate.leaf_predicates();
    assert_eq!(leaves.len(), 5);
    assert_eq!(leaves[0].op, PredicateOp::Eq);
    assert_eq!(leaves[3].op, PredicateOp::NotEq);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_like_and_not_like_predicates() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores where user_id like 'a%' and user_id not like 'admin_%' group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    let leaves = supported
        .predicate_expr
        .as_ref()
        .expect("WHERE should be admitted")
        .leaf_predicates();
    assert_eq!(leaves.len(), 2);
    assert_eq!(leaves[0].op, PredicateOp::Like);
    assert_eq!(leaves[1].op, PredicateOp::NotLike);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_scalar_expression_predicate() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, sum(score) as sum, count(*) as count from scores where 10 < score + 1 group by user_id",
        &catalog,
    )
    .unwrap();

    let Some(RowPredicateExpr::ScalarInt64Comparison {
        comparison_op,
        literal,
        ..
    }) = plan.predicate_expr.as_ref()
    else {
        panic!("expected scalar Int64 predicate expression");
    };
    assert_eq!(*comparison_op, PredicateOp::Gt);
    assert_eq!(*literal, json!(10));
    assert!(plan.predicate.is_none());
}

#[test]
fn single_key_aggregate_sql_accepts_is_not_null_predicates() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores where user_id is not null and score is not null group by user_id having sum(score) is not null",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    let leaves = supported
        .predicate_expr
        .as_ref()
        .expect("WHERE should be admitted")
        .leaf_predicates();
    assert_eq!(leaves.len(), 2);
    assert_eq!(leaves[0].op, PredicateOp::IsNotNull);
    assert_eq!(leaves[1].op, PredicateOp::IsNotNull);
    assert_eq!(
        supported
            .having_expr
            .as_ref()
            .expect("HAVING should be admitted")
            .leaf_predicates()[0]
            .op,
        PredicateOp::IsNotNull
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_is_distinct_from_predicates() {
    let mut catalog = scores_catalog();
    let score = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    let schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(coalesce(score, 0)) as sum, count(*) as count from scores where (score is distinct from 0) or (score is not distinct from null) group by user_id having count(*) is not distinct from 0",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    let leaves = supported
        .predicate_expr
        .as_ref()
        .expect("WHERE should be admitted")
        .leaf_predicates();
    assert_eq!(leaves[0].op, PredicateOp::IsDistinctFrom);
    assert_eq!(leaves[1].op, PredicateOp::IsNotDistinctFrom);
    assert_eq!(
        supported
            .having_expr
            .as_ref()
            .expect("HAVING should be admitted")
            .leaf_predicates()[0]
            .op,
        PredicateOp::IsNotDistinctFrom
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_lowers_having_to_post_aggregate_filter() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id having sum(score) between 11 and 99 and count(*) between 2 and 9",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    let having = supported
        .having
        .as_ref()
        .expect("HAVING should be admitted");
    assert_eq!(having.output_column_id, "sum");
    assert_eq!(having.op, PredicateOp::GtEq);
    assert_eq!(having.literal, json!(11));
    assert_eq!(
        supported
            .having_expr
            .as_ref()
            .expect("HAVING expression should be admitted")
            .leaf_predicates()
            .len(),
        4
    );
    assert_eq!(
        plan.nodes
            .iter()
            .filter(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { input, .. } if input.starts_with("aggregate_sum_count") || input.starts_with("filter_aggregate")))
            .count(),
        4
    );
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Filter { input, predicate, .. }
            if input == "aggregate_sum_count" && predicate.column.column_id == "sum"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_admits_or_having_without_fake_filter_node() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id having sum(score) > 10 or count(*) = 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert!(supported
        .having_expr
        .as_ref()
        .is_some_and(AggregateOutputPredicateExpr::contains_or));
    assert!(!plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Filter { input, .. } if input.starts_with("aggregate_sum_count")
    )));
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
fn single_key_aggregate_sql_accepts_multiple_raw_int64_input_columns() {
    let catalog = scores_with_adjustment_catalog();
    let output_schema = scores_multi_input_stats_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum_score, min(user_id_adjustment) as min_adj, max(user_id_adjustment) as max_adj, avg(user_id_adjustment) as avg_adj, count(user_id_adjustment) as count_adj from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(supported.sum_value_column_id, "score");
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Sum
            && aggregate.input_column_id.as_deref() == Some("score")
            && aggregate.output_column_id == "sum_score"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Min
            && aggregate.input_column_id.as_deref() == Some("user_id_adjustment")
            && aggregate.output_column_id == "min_adj"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Max
            && aggregate.input_column_id.as_deref() == Some("user_id_adjustment")
            && aggregate.output_column_id == "max_adj"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Avg
            && aggregate.input_column_id.as_deref() == Some("user_id_adjustment")
            && aggregate.output_column_id == "avg_adj"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Count
            && aggregate.output_column_id == "count_adj"
    }));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_lowers_count_only_projection() {
    let catalog = scores_catalog();
    let output_schema = scores_count_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(supported.sum_value_column_id, "score");
    assert_eq!(supported.aggregate_outputs.len(), 1);
    assert_eq!(
        supported.aggregate_outputs[0].function,
        LogicalPlanAggregateFunctionV1::Count
    );
    assert_eq!(supported.aggregate_outputs[0].output_column_id, "count");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_lowers_non_null_column_count_projection() {
    let catalog = scores_catalog();
    let output_schema = scores_count_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, count(user_id) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(supported.aggregate_outputs.len(), 1);
    assert_eq!(
        supported.aggregate_outputs[0].function,
        LogicalPlanAggregateFunctionV1::Count
    );
    assert_eq!(supported.aggregate_outputs[0].input_column_id, None);
    assert_eq!(supported.aggregate_outputs[0].output_column_id, "count");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_lowers_nullable_column_count_projection() {
    let mut catalog = scores_catalog();
    let score = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable score catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    let output_schema = scores_count_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, count(score) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(supported.aggregate_outputs.len(), 1);
    assert_eq!(
        supported.aggregate_outputs[0].function,
        LogicalPlanAggregateFunctionV1::Count
    );
    assert_eq!(
        supported.aggregate_outputs[0].input_column_id.as_deref(),
        Some("score")
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_lowers_count_distinct_projection() {
    let catalog = scores_catalog();
    let output_schema = scores_count_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, count(distinct score) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(supported.sum_value_column_id, "score");
    assert_eq!(supported.aggregate_outputs.len(), 1);
    assert_eq!(
        supported.aggregate_outputs[0].function,
        LogicalPlanAggregateFunctionV1::CountDistinct
    );
    assert_eq!(
        supported.aggregate_outputs[0].input_column_id.as_deref(),
        Some("score")
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_binds_having_count_distinct_function_to_projected_output() {
    let catalog = scores_catalog();
    let mut output_schema = scores_output_schema();
    output_schema.columns[2].name = "distinct_scores".to_string();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(distinct score) as distinct_scores from scores group by user_id having count(distinct score) > 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(
        supported.aggregate_outputs[1].function,
        LogicalPlanAggregateFunctionV1::CountDistinct
    );
    assert_eq!(
        supported.aggregate_outputs[1].output_column_id,
        "distinct_scores"
    );
    assert_eq!(
        supported
            .having_expr
            .as_ref()
            .expect("HAVING expression should be admitted")
            .leaf_predicates()[0]
            .output_column_id,
        "distinct_scores"
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_filtered_count_distinct_function_having() {
    let catalog = scores_catalog();
    let output_schema = RelationSchema {
        relation_id: "scores_by_user_distinct".to_string(),
        relation_name: "scores_by_user_distinct".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "scores-by-user-distinct-v1".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "filtered_distinct_scores".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "distinct_scores".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    };

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, count(distinct score) filter (where score > 5) as filtered_distinct_scores, count(distinct score) as distinct_scores from scores group by user_id having count(distinct score) filter (where score > 5) > 1",
        &catalog,
        &output_schema,
    )
    .unwrap();
    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(
        supported
            .having_expr
            .as_ref()
            .expect("filtered function HAVING should be admitted")
            .leaf_predicates()[0]
            .output_column_id,
        "filtered_distinct_scores"
    );

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, count(distinct score) filter (where score > 5) as filtered_distinct_scores, count(distinct score) as distinct_scores from scores group by user_id having filtered_distinct_scores > 1",
        &catalog,
        &output_schema,
    )
    .unwrap();
    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(
        supported
            .having_expr
            .as_ref()
            .expect("alias HAVING should be admitted")
            .leaf_predicates()[0]
            .output_column_id,
        "filtered_distinct_scores"
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_rejects_non_matching_filtered_function_having() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select user_id, count(distinct score) filter (where score > 5) as filtered_distinct_scores from scores group by user_id having count(distinct score) filter (where score > 0) > 1",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn single_key_aggregate_sql_accepts_identity_cte_source() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "with score_source as (select * from scores) select user_id, sum(score) as sum, count(*) as count from score_source group by user_id",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(supported.input_relation_id, "scores");
    assert_eq!(supported.group_key_column_id, "user_id");
    assert_eq!(supported.sum_value_column_id, "score");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_lowers_cte_source_filter_to_runtime_predicate() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "with score_source as (select * from scores where score > 0) select user_id, sum(score) as sum, count(*) as count from score_source group by user_id",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(supported.input_relation_id, "scores");
    assert!(supported.predicate_expr.is_some());
    assert_eq!(
        supported
            .predicate_expr
            .as_ref()
            .expect("CTE filter should lower to runtime predicate")
            .leaf_predicates()
            .len(),
        1
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_derived_table_source_filter() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from (select * from scores where score > 0) s where s.user_id <> 'bob' group by s.user_id",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    let predicates = supported
        .predicate_expr
        .as_ref()
        .expect("derived table and outer WHERE filters should lower to runtime predicate")
        .leaf_predicates();
    assert_eq!(predicates.len(), 2);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_inner_source_alias_filters() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let cte_plan = lower_supported_sql_to_logical_plan(
        "with score_source as (select * from scores src where src.score > 0) select user_id, sum(score) as sum, count(*) as count from score_source group by user_id",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();
    let derived_plan = lower_supported_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from (select * from scores src where src.score > 0) s where s.user_id <> 'bob' group by s.user_id",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: cte } = &cte_plan.execution else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(
        cte.predicate_expr
            .as_ref()
            .expect("CTE source filter should lower")
            .leaf_predicates()
            .len(),
        1
    );
    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: derived } =
        &derived_plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(
        derived
            .predicate_expr
            .as_ref()
            .expect("derived source and outer filters should lower")
            .leaf_predicates()
            .len(),
        2
    );
    validate_logical_view_plan(&cte_plan).unwrap();
    validate_logical_view_plan(&derived_plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_combines_cte_source_and_outer_where_predicates() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "with score_source as (select * from scores where score > 0) select user_id, sum(score) as sum, count(*) as count from score_source where user_id <> 'bob' group by user_id",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    let predicate_expr = supported
        .predicate_expr
        .as_ref()
        .expect("CTE and outer WHERE filters should lower to runtime predicate");
    assert_eq!(predicate_expr.leaf_predicates().len(), 2);
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

    assert_eq!(plan.plan_version, LOGICAL_VIEW_PLAN_VERSION_V2);
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
    let implementation = plan.execution_implementation.as_ref().unwrap();
    assert_eq!(
        implementation.implementation_id,
        "velorix-keyed-aggregate-join-specialization-v1"
    );
    assert!(implementation
        .physical_operator_dag_hash
        .starts_with("velorix-physical-operator-dag-sha256-v1:sha256:"));
    assert_eq!(implementation.contract_version, 2);
    assert_eq!(
        implementation.output_codec_id,
        "velorix-materialized-output-v1"
    );
    assert_eq!(
        implementation.output_publication_protocol_id,
        "velorix-durable-output-publication-v1"
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn composite_primary_key_inner_join_lowers_every_key_component() {
    let (scores, accounts) = composite_join_catalogs();
    let sql = "select a.account_tenant_id as tenant_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id and s.tenant_id = a.account_tenant_id group by a.account_tenant_id";
    let plan = lower_supported_join_view_sql_to_logical_plan(
        sql,
        &[scores.clone(), accounts.clone()],
        &composite_join_output_schema(),
    )
    .unwrap();
    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.left_join_key_column_id, "tenant_id");
    assert_eq!(supported.right_join_key_column_id, "account_tenant_id");
    assert_eq!(
        supported.composite_equality,
        Some(SupportedCompositeJoinEqualityV1 {
            schema_version: 1,
            additional_pairs: vec![SupportedJoinKeyPairV1 {
                left_column_id: "user_id".into(),
                right_column_id: "account_id".into(),
            }],
        })
    );
    let join = plan
        .nodes
        .iter()
        .find(|node| matches!(node, VelorixLogicalViewPlanNodeV1::InnerEquiJoin { .. }))
        .unwrap();
    let VelorixLogicalViewPlanNodeV1::InnerEquiJoin {
        composite_equality, ..
    } = join
    else {
        unreachable!()
    };
    assert_eq!(
        composite_equality.as_ref().unwrap().additional_pairs.len(),
        1
    );
    assert_eq!(
        plan.execution_implementation
            .as_ref()
            .unwrap()
            .join_key_codec_id
            .as_deref(),
        Some(COMPOSITE_PK_POSITIONAL_JSON_ARRAY_JOIN_KEY_CODEC_V1)
    );
    validate_logical_view_plan(&plan).unwrap();

    let reordered_sql = "select a.account_tenant_id as tenant_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.tenant_id = a.account_tenant_id and s.user_id = a.account_id group by a.account_tenant_id";
    let reordered = lower_supported_join_view_sql_to_logical_plan(
        reordered_sql,
        &[scores.clone(), accounts.clone()],
        &composite_join_output_schema(),
    )
    .unwrap();
    let plan_json: serde_json::Value = serde_json::to_value(&plan).unwrap();
    let reordered_json: serde_json::Value = serde_json::to_value(&reordered).unwrap();
    assert_eq!(plan_json["execution"], reordered_json["execution"]);
    assert_eq!(plan_json["nodes"], reordered_json["nodes"]);

    let error = validate_supported_join_view_sql(
        "select a.account_tenant_id as tenant_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.tenant_id = a.account_tenant_id group by a.account_tenant_id",
        &[scores.clone(), accounts.clone()],
    )
    .unwrap_err();
    assert!(error.to_string().contains("partial primary-key equality"));

    let mut incompatible_accounts = accounts;
    let tenant = incompatible_accounts
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "account_tenant_id")
        .unwrap();
    tenant.logical_type = VelorixLogicalTypeV1::Int64;
    tenant.physical_arrow_type = ArrowPhysicalTypeV1::Int64;
    let fingerprint =
        SchemaFingerprintV1::for_relation_schema(&incompatible_accounts.relation_schema).unwrap();
    incompatible_accounts.schema_fingerprint = fingerprint.clone();
    incompatible_accounts
        .incremental_relation
        .schema_fingerprint = fingerprint;
    let error =
        validate_supported_join_view_sql(sql, &[scores, incompatible_accounts]).unwrap_err();
    assert!(error.to_string().contains("identical physical Arrow types"));
}

#[test]
fn non_primary_scalar_join_admits_duplicate_key_state_and_rejects_unsafe_keys() {
    let scores = generic_adapter_catalog(scores_catalog());
    let accounts = generic_adapter_catalog(accounts_catalog());
    let sql = "select a.limit as bucket, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.score = a.limit group by a.limit";
    let plan = lower_supported_join_view_sql_to_logical_plan(
        sql,
        &[scores.clone(), accounts.clone()],
        &non_primary_join_output_schema(),
    )
    .unwrap();
    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.left_join_key_column_id, "score");
    assert_eq!(supported.right_join_key_column_id, "limit");
    assert_eq!(supported.composite_equality, None);
    assert_eq!(
        supported.join_key_domain,
        Some(SupportedJoinKeyDomainV1::NonPrimaryNonNullScalarV1)
    );
    assert_eq!(
        plan.execution_implementation
            .as_ref()
            .unwrap()
            .join_key_codec_id
            .as_deref(),
        Some(NON_PRIMARY_NON_NULL_SCALAR_JOIN_KEY_CODEC_V1)
    );
    validate_logical_view_plan(&plan).unwrap();

    let mut nullable_accounts = accounts.clone();
    nullable_accounts
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "limit")
        .unwrap()
        .nullable = true;
    let fingerprint =
        SchemaFingerprintV1::for_relation_schema(&nullable_accounts.relation_schema).unwrap();
    nullable_accounts.schema_fingerprint = fingerprint.clone();
    nullable_accounts.incremental_relation.schema_fingerprint = fingerprint;
    let error =
        validate_supported_join_view_sql(sql, &[scores.clone(), nullable_accounts]).unwrap_err();
    assert!(error.to_string().contains("must be non-null"));

    let error = validate_supported_join_view_sql(
        "select a.limit as bucket, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.delta = a.delta group by a.limit",
        &[scores, accounts],
    )
    .unwrap_err();
    assert!(error.to_string().contains("weight column"));

    let mut nested_scores = scores_with_category_catalog();
    let category = nested_scores
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "category")
        .unwrap();
    category.logical_type = VelorixLogicalTypeV1::Array {
        element_type: Box::new(VelorixLogicalTypeV1::Utf8),
    };
    category.physical_arrow_type = ArrowPhysicalTypeV1::List {
        element_type: Box::new(ArrowPhysicalTypeV1::Utf8),
    };
    category.nullable = false;
    let fingerprint =
        SchemaFingerprintV1::for_relation_schema(&nested_scores.relation_schema).unwrap();
    nested_scores.schema_fingerprint = fingerprint.clone();
    nested_scores.incremental_relation.schema_fingerprint = fingerprint;
    let mut nested_accounts = generic_adapter_catalog(accounts_catalog());
    let tier = nested_accounts
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "tier")
        .unwrap();
    tier.logical_type = VelorixLogicalTypeV1::Array {
        element_type: Box::new(VelorixLogicalTypeV1::Utf8),
    };
    tier.physical_arrow_type = ArrowPhysicalTypeV1::List {
        element_type: Box::new(ArrowPhysicalTypeV1::Utf8),
    };
    let fingerprint =
        SchemaFingerprintV1::for_relation_schema(&nested_accounts.relation_schema).unwrap();
    nested_accounts.schema_fingerprint = fingerprint.clone();
    nested_accounts.incremental_relation.schema_fingerprint = fingerprint;
    let error = validate_supported_join_view_sql(
        "select a.tier as bucket, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.category = a.tier group by a.tier",
        &[nested_scores, nested_accounts],
    )
    .unwrap_err();
    assert!(error.to_string().contains("scalar primary-key atom types"));
}

#[test]
fn self_join_uses_canonical_scan_instances_independent_of_sql_aliases() {
    let scores = generic_adapter_catalog(scores_catalog());
    let output = self_join_count_output_schema();
    let lower = |sql: &str| {
        lower_supported_sql_to_logical_plan(sql, std::slice::from_ref(&scores), &output).unwrap()
    };
    let first = lower("select count(*) as count from scores l join scores r on l.score = r.score");
    let renamed =
        lower("select count(*) as count from scores x join scores y on x.score = y.score");
    assert_eq!(first.input_relations.len(), 1);
    assert_eq!(first.nodes, renamed.nodes);
    assert_eq!(first.execution, renamed.execution);
    assert_eq!(
        first.execution_implementation,
        renamed.execution_implementation
    );

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan } = &first.execution else {
        panic!("expected self-join execution");
    };
    assert_eq!(
        plan.left_input_instance_id.as_deref(),
        Some(LEFT_JOIN_INPUT_INSTANCE_ID_V1)
    );
    assert_eq!(
        plan.right_input_instance_id.as_deref(),
        Some(RIGHT_JOIN_INPUT_INSTANCE_ID_V1)
    );
    assert_eq!(
        plan.aggregate_output_identity,
        Some(SupportedAggregateOutputIdentity::Singleton)
    );
    assert_eq!(
        first
            .execution_implementation
            .as_ref()
            .unwrap()
            .input_fanout_protocol_id
            .as_deref(),
        Some(SELF_JOIN_ATOMIC_FANOUT_PROTOCOL_V1)
    );
    let (left_key, right_key) = first
        .nodes
        .iter()
        .find_map(|node| match node {
            VelorixLogicalViewPlanNodeV1::InnerEquiJoin {
                left_key,
                right_key,
                ..
            } => Some((left_key, right_key)),
            _ => None,
        })
        .unwrap();
    assert_eq!(left_key.relation_id, right_key.relation_id);
    assert_eq!(
        left_key.input_instance_id.as_deref(),
        Some(LEFT_JOIN_INPUT_INSTANCE_ID_V1)
    );
    assert_eq!(
        right_key.input_instance_id.as_deref(),
        Some(RIGHT_JOIN_INPUT_INSTANCE_ID_V1)
    );
    validate_logical_view_plan(&first).unwrap();

    let mut missing_instance = first;
    let join = missing_instance
        .nodes
        .iter_mut()
        .find(|node| matches!(node, VelorixLogicalViewPlanNodeV1::InnerEquiJoin { .. }))
        .unwrap();
    let VelorixLogicalViewPlanNodeV1::InnerEquiJoin { left_key, .. } = join else {
        unreachable!()
    };
    left_key.input_instance_id = None;
    assert!(validate_logical_view_plan(&missing_instance).is_err());
}

#[test]
fn self_join_fails_closed_outside_the_atomic_global_count_slice() {
    let scores = generic_adapter_catalog(scores_catalog());
    for sql in [
        "select count(*) as count from scores join scores on scores.score = scores.score",
        "select count(*) as count from scores l join scores r on l.score = r.score where l.score > 0",
        "select l.score, count(*) as count from scores l join scores r on l.score = r.score group by l.score",
        "select sum(l.score) as sum from scores l join scores r on l.score = r.score",
        "select count(*) as count from scores l join scores r on l.user_id = r.user_id",
    ] {
        assert!(validate_supported_join_view_sql(sql, std::slice::from_ref(&scores)).is_err(), "{sql}");
    }
}

#[test]
fn legacy_scalar_join_plan_round_trips_without_changing_bytes_or_identity() {
    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores_catalog(), accounts_catalog()],
        &join_output_schema(),
    )
    .unwrap();
    let bytes = serde_json::to_vec(&plan).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("composite_equality"));
    assert!(!String::from_utf8_lossy(&bytes).contains("join_key_domain"));
    assert!(!String::from_utf8_lossy(&bytes).contains("join_key_codec_id"));

    let decoded: VelorixLogicalViewPlanV1 = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
    assert_eq!(decoded.plan_hash, plan.plan_hash);
    assert_eq!(
        decoded.execution_implementation,
        plan.execution_implementation
    );
    assert_eq!(
        logical_view_plan_hash(&decoded).unwrap(),
        plan.plan_hash.unwrap()
    );
    validate_logical_view_plan(&decoded).unwrap();
}

#[test]
fn supported_composite_join_keys_reject_noncanonical_or_ambiguous_shapes() {
    let mut plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores_catalog(), accounts_catalog()],
    )
    .unwrap();
    plan.left_join_key_column_id = "account_id".into();
    plan.right_join_key_column_id = "account_id".into();
    plan.composite_equality = Some(SupportedCompositeJoinEqualityV1 {
        schema_version: 1,
        additional_pairs: vec![SupportedJoinKeyPairV1 {
            left_column_id: "tenant_id".into(),
            right_column_id: "tenant_id".into(),
        }],
    });
    assert_eq!(
        supported_join_view_plan_key_pairs(&plan).unwrap(),
        vec![
            SupportedJoinKeyPairV1 {
                left_column_id: "account_id".into(),
                right_column_id: "account_id".into(),
            },
            SupportedJoinKeyPairV1 {
                left_column_id: "tenant_id".into(),
                right_column_id: "tenant_id".into(),
            },
        ]
    );

    let mut malformed = plan.clone();
    malformed
        .composite_equality
        .as_mut()
        .unwrap()
        .schema_version = 2;
    assert!(supported_join_view_plan_key_pairs(&malformed).is_err());

    let mut malformed = plan.clone();
    malformed
        .composite_equality
        .as_mut()
        .unwrap()
        .additional_pairs
        .clear();
    assert!(supported_join_view_plan_key_pairs(&malformed).is_err());

    let mut malformed = plan.clone();
    malformed.left_join_key_column_id = "tenant_id".into();
    assert!(supported_join_view_plan_key_pairs(&malformed).is_err());

    let mut malformed = plan;
    malformed
        .composite_equality
        .as_mut()
        .unwrap()
        .additional_pairs[0]
        .left_column_id = "account_id".into();
    assert!(supported_join_view_plan_key_pairs(&malformed).is_err());
}

#[test]
fn logical_composite_join_keys_reject_unknown_versions_and_wrong_direction() {
    let base = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores_catalog(), accounts_catalog()],
        &join_output_schema(),
    )
    .unwrap();

    let mutate_join = |plan: &mut VelorixLogicalViewPlanV1,
                       equality: LogicalPlanCompositeJoinEqualityV1| {
        let join = plan
            .nodes
            .iter_mut()
            .find(|node| matches!(node, VelorixLogicalViewPlanNodeV1::InnerEquiJoin { .. }))
            .unwrap();
        let VelorixLogicalViewPlanNodeV1::InnerEquiJoin {
            composite_equality, ..
        } = join
        else {
            unreachable!()
        };
        *composite_equality = Some(equality);
    };

    let mut unknown_version = base.clone();
    mutate_join(
        &mut unknown_version,
        LogicalPlanCompositeJoinEqualityV1 {
            schema_version: 2,
            additional_pairs: vec![LogicalPlanJoinKeyPairV1 {
                left_key: LogicalPlanColumnRef {
                    relation_id: "scores".into(),
                    input_instance_id: None,
                    column_id: "z".into(),
                },
                right_key: LogicalPlanColumnRef {
                    relation_id: "accounts".into(),
                    input_instance_id: None,
                    column_id: "z".into(),
                },
            }],
        },
    );
    assert!(validate_logical_view_plan(&unknown_version).is_err());

    let mut wrong_direction = base;
    mutate_join(
        &mut wrong_direction,
        LogicalPlanCompositeJoinEqualityV1 {
            schema_version: 1,
            additional_pairs: vec![LogicalPlanJoinKeyPairV1 {
                left_key: LogicalPlanColumnRef {
                    relation_id: "accounts".into(),
                    input_instance_id: None,
                    column_id: "z".into(),
                },
                right_key: LogicalPlanColumnRef {
                    relation_id: "scores".into(),
                    input_instance_id: None,
                    column_id: "z".into(),
                },
            }],
        },
    );
    assert!(validate_logical_view_plan(&wrong_direction).is_err());
}

#[test]
fn join_execution_specialization_is_persisted_and_tamper_evident() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let left = lower_supported_join_view_sql_to_logical_plan(
        "select s.user_id as account_id, sum(s.score) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id group by s.user_id",
        &[scores.clone(), accounts.clone()],
        &join_output_schema(),
    )
    .unwrap();
    assert_eq!(
        left.execution_implementation
            .as_ref()
            .unwrap()
            .implementation_id,
        "velorix-narrow-left-join-specialization-v1"
    );

    let general = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count, min(s.score) as min_score, max(s.score) as max_score, avg(s.score) as avg_score from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
        &join_stats_output_schema(),
    )
    .unwrap();
    assert_eq!(
        general
            .execution_implementation
            .as_ref()
            .unwrap()
            .implementation_id,
        "velorix-general-aggregate-join-specialization-v1"
    );

    let mut tampered = general.clone();
    tampered
        .execution_implementation
        .as_mut()
        .unwrap()
        .implementation_id = "velorix-generic-dag-v1".to_string();
    assert!(validate_logical_view_plan(&tampered).is_err());

    let mut tampered_publication = general;
    tampered_publication
        .execution_implementation
        .as_mut()
        .unwrap()
        .output_publication_protocol_id = "velorix-durable-output-publication-v2".to_string();
    assert!(validate_logical_view_plan(&tampered_publication).is_err());
}

#[test]
fn two_input_join_sql_accepts_identity_cte_source_filter() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "with positive_scores as (select * from scores where score > 0) select a.account_id, sum(s.score) as sum, count(*) as count from positive_scores s join accounts a on s.user_id = a.account_id where a.limit > 60 group by a.account_id",
        &[scores, accounts],
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    let predicate_expr = supported
        .predicate_expr
        .as_ref()
        .expect("CTE and outer WHERE filters should lower to runtime predicate");
    assert_eq!(predicate_expr.leaf_predicates().len(), 2);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_right_identity_cte_source_filter() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "with eligible_accounts as (select * from accounts where limit > 60) select a.account_id, sum(s.score) as sum, count(*) as count from scores s join eligible_accounts a on s.user_id = a.account_id where s.score > 0 group by a.account_id",
        &[scores, accounts],
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    let predicate_expr = supported
        .predicate_expr
        .as_ref()
        .expect("right CTE and outer WHERE filters should lower to runtime predicate");
    let predicates = predicate_expr.leaf_predicates();
    assert_eq!(predicates.len(), 2);
    assert!(predicates
        .iter()
        .any(|predicate| predicate.relation_id == "accounts"
            && predicate.predicate.column_id == "limit"));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_two_identity_cte_source_filters() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "with positive_scores as (select * from scores where score > 0), eligible_accounts as (select * from accounts where limit > 60) select a.account_id, sum(s.score) as sum, count(*) as count from positive_scores s join eligible_accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    let predicates = supported
        .predicate_expr
        .as_ref()
        .expect("both CTE filters should lower to runtime predicates")
        .leaf_predicates();
    assert_eq!(predicates.len(), 2);
    assert!(predicates
        .iter()
        .any(|predicate| predicate.relation_id == "scores"
            && predicate.predicate.column_id == "score"));
    assert!(predicates
        .iter()
        .any(|predicate| predicate.relation_id == "accounts"
            && predicate.predicate.column_id == "limit"));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_two_derived_table_source_filters() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count from (select * from scores where score > 0) s join (select * from accounts where limit > 60) a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    let predicates = supported
        .predicate_expr
        .as_ref()
        .expect("derived table filters should lower to runtime predicates")
        .leaf_predicates();
    assert_eq!(predicates.len(), 2);
    assert!(predicates
        .iter()
        .any(|predicate| predicate.relation_id == "scores"
            && predicate.predicate.column_id == "score"));
    assert!(predicates
        .iter()
        .any(|predicate| predicate.relation_id == "accounts"
            && predicate.predicate.column_id == "limit"));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_inner_source_alias_filters() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();

    let cte_plan = lower_supported_join_view_sql_to_logical_plan(
        "with positive_scores as (select * from scores src where src.score > 0), eligible_accounts as (select * from accounts acct where acct.limit > 60) select a.account_id, sum(s.score) as sum, count(*) as count from positive_scores s join eligible_accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores.clone(), accounts.clone()],
        &output_schema,
    )
    .unwrap();
    let derived_plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count from (select * from scores src where src.score > 0) s join (select * from accounts acct where acct.limit > 60) a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
        &output_schema,
    )
    .unwrap();

    for plan in [cte_plan, derived_plan] {
        let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } =
            &plan.execution
        else {
            panic!("expected join runtime execution");
        };
        let predicates = supported
            .predicate_expr
            .as_ref()
            .expect("source filters should lower")
            .leaf_predicates();
        assert_eq!(predicates.len(), 2);
        assert!(predicates
            .iter()
            .any(|predicate| predicate.relation_id == "scores"
                && predicate.predicate.column_id == "score"));
        assert!(predicates
            .iter()
            .any(|predicate| predicate.relation_id == "accounts"
                && predicate.predicate.column_id == "limit"));
        validate_logical_view_plan(&plan).unwrap();
    }
}

#[test]
fn two_input_join_sql_accepts_using_primary_key_shape() {
    let scores = scores_catalog();
    let accounts = accounts_catalog_with_user_id_key();
    let output_schema = join_output_schema();

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.user_id as account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a using (user_id) group by a.user_id",
        &[scores, accounts],
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.left_join_key_column_id, "user_id");
    assert_eq!(supported.right_join_key_column_id, "user_id");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_group_by_all() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by all",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.group_key_column_id, "account_id");
    assert_eq!(supported.output_key_column_id, "account_id");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_plain_distinct_grouped_output() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select distinct a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { .. }
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_lowers_having_to_post_aggregate_filter() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id having sum(s.score) > 10 and count(*) > 1",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    let having = supported
        .having
        .as_ref()
        .expect("JOIN HAVING should be admitted");
    assert_eq!(having.output_column_id, "sum");
    assert_eq!(having.op, PredicateOp::Gt);
    assert_eq!(having.literal, json!(10));
    assert_eq!(
        supported
            .having_expr
            .as_ref()
            .expect("JOIN HAVING expression should be admitted")
            .leaf_predicates()
            .len(),
        2
    );
    assert_eq!(
        plan.nodes
            .iter()
            .filter(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { input, .. } if input.starts_with("aggregate_join_sum_count") || input.starts_with("filter_join_aggregate")))
            .count(),
        2
    );
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Filter { input, predicate, .. }
            if input == "aggregate_join_sum_count" && predicate.column.column_id == "sum"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_lowers_min_max_avg_outputs() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_stats_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count, min(s.score) as min_score, max(s.score) as max_score, avg(s.score) as avg_score from scores s join accounts a on s.user_id = a.account_id group by a.account_id having avg(s.score) > 5 and min(s.score) >= 0 and max(s.score) <= 20",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.aggregate_outputs.len(), 5);
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Min
            && aggregate.input_column_id.as_deref() == Some("score")
            && aggregate.output_column_id == "min_score"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Max
            && aggregate.input_column_id.as_deref() == Some("score")
            && aggregate.output_column_id == "max_score"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Avg
            && aggregate.input_column_id.as_deref() == Some("score")
            && aggregate.output_column_id == "avg_score"
    }));
    assert_eq!(
        supported
            .having_expr
            .as_ref()
            .expect("JOIN HAVING expression should be admitted")
            .leaf_predicates()
            .len(),
        3
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn left_join_sql_accepts_left_pk_grouped_left_only_aggregates() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_stats_output_schema();
    let catalogs = [scores.clone(), accounts.clone()];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select s.user_id as account_id, sum(s.score) as sum, count(*) as count, min(s.score) as min_score, max(s.score) as max_score, avg(s.score) as avg_score from scores s left join accounts a on s.user_id = a.account_id group by s.user_id",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(
        supported.group_key_relation_id,
        scores.relation_schema.relation_id
    );
    assert_eq!(supported.group_key_column_id, "user_id");
    assert!(supported.aggregate_outputs.iter().all(|aggregate| {
        aggregate.input_relation_side != Some(SupportedAggregateInputRelationSide::Right)
    }));
    let join = plan
        .operator_dag_contract
        .operators
        .iter()
        .find(|operator| operator.operator.kind == "left_equi_join")
        .expect("LEFT JOIN must derive an operator contract");
    assert_eq!(join.outputs[0].changelog, ChangelogModeV1::GeneralRetract);
    assert!(join.outputs[0].candidate_keys.is_empty());
    assert_eq!(
        join.outputs[0].uniqueness,
        UniquenessGuaranteeV1::NotGuaranteed
    );
    assert!(join.outputs[0]
        .schema
        .columns
        .iter()
        .filter(|column| column
            .column_id
            .starts_with(&format!("{}.", accounts.relation_schema.relation_id)))
        .all(|column| column.nullability == NullabilityV1::Nullable));
    let aggregate = plan
        .operator_dag_contract
        .operators
        .iter()
        .find(|operator| operator.operator.kind == "aggregate")
        .expect("LEFT JOIN aggregate must derive an operator contract");
    assert_eq!(
        aggregate.inputs[0].accepted_changelog,
        AcceptedChangelogV1::GeneralRetract
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn unbounded_left_join_rejects_an_append_only_downstream_edge() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_stats_output_schema();
    let catalogs = [scores, accounts];
    let mut plan = lower_supported_join_view_sql_to_logical_plan(
        "select s.user_id as account_id, sum(s.score) as sum, count(*) as count, min(s.score) as min_score, max(s.score) as max_score, avg(s.score) as avg_score from scores s left join accounts a on s.user_id = a.account_id group by s.user_id",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let join = plan
        .operator_dag_contract
        .operators
        .iter()
        .find(|operator| operator.operator.kind == "left_equi_join")
        .unwrap();
    assert_eq!(join.outputs[0].changelog, ChangelogModeV1::GeneralRetract);
    assert!(matches!(
        join.state.as_ref().unwrap().boundedness,
        StateBoundednessV1::Unbounded
    ));

    let aggregate = plan
        .operator_dag_contract
        .operators
        .iter_mut()
        .find(|operator| operator.operator.kind == "aggregate")
        .unwrap();
    aggregate.inputs[0].accepted_changelog = AcceptedChangelogV1::AppendOnly;

    let error = validate_logical_view_plan(&plan).unwrap_err();
    assert!(error.to_string().contains("incompatible operator edge"));
    assert!(error.to_string().contains("changelog"));
}

#[test]
fn right_join_sql_swaps_operands_and_lowers_to_the_left_join_runtime() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores.clone(), accounts.clone()];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(a.limit) as sum, count(*) as count from scores s right join accounts a on s.user_id = a.account_id group by a.account_id",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.join_kind, SupportedJoinKind::Left);
    assert_eq!(
        supported.left_input_relation_id,
        accounts.relation_schema.relation_id
    );
    assert_eq!(
        supported.right_input_relation_id,
        scores.relation_schema.relation_id
    );
    assert_eq!(
        supported.group_key_relation_id,
        accounts.relation_schema.relation_id
    );
    assert!(supported.predicate_expr.is_none(), "{supported:?}");
    assert!(supported.aggregate_outputs.iter().all(|aggregate| {
        aggregate.input_relation_side != Some(SupportedAggregateInputRelationSide::Right)
    }));
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::LeftEquiJoin { left, right, .. }
            if left == "scan_left" && right == "scan_right"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn full_join_sql_requires_and_lowers_a_coalesced_output_key() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let mut output_schema = join_output_schema();
    output_schema.columns[1].nullable = true;
    let catalogs = [scores.clone(), accounts.clone()];
    let sql = "select coalesce(s.user_id, a.account_id) as account_id, sum(s.score) as sum, count(*) as count from scores s full outer join accounts a on s.user_id = a.account_id group by coalesce(s.user_id, a.account_id)";

    let plan =
        lower_supported_join_view_sql_to_logical_plan(sql, &catalogs, &output_schema).unwrap();
    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.join_kind, SupportedJoinKind::Full);
    assert_eq!(supported.output_key_column_id, "account_id");
    assert_eq!(
        plan.execution_implementation
            .as_ref()
            .unwrap()
            .implementation_id,
        "velorix-full-join-specialization-v1"
    );
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::FullEquiJoin { output_key, .. }
            if output_key.relation_id == output_schema.relation_id
                && output_key.column_id == "account_id"
    )));
    let full_join = plan
        .operator_dag_contract
        .operators
        .iter()
        .find(|operator| operator.operator.kind == "full_equi_join")
        .unwrap();
    assert_eq!(
        full_join.outputs[0].changelog,
        ChangelogModeV1::GeneralRetract
    );
    assert!(full_join.outputs[0].schema.columns.iter().any(|column| {
        column.column_id == format!("{}.account_id", output_schema.relation_id)
            && column.nullability == NullabilityV1::NonNull
    }));
    validate_logical_view_plan(&plan).unwrap();

    let error = lower_supported_join_view_sql_to_logical_plan(
        "select s.user_id as account_id, sum(s.score) as sum, count(*) as count from scores s full outer join accounts a on s.user_id = a.account_id group by s.user_id",
        &catalogs,
        &output_schema,
    )
    .unwrap_err();
    assert!(error.to_string().contains("COALESCE"));
}

#[test]
fn left_join_sql_accepts_left_only_aggregate_filter_predicates() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores.clone(), accounts.clone()];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select s.user_id as account_id, sum(s.score) filter (where s.score > 0) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id group by s.user_id",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.join_kind, SupportedJoinKind::Left);
    assert_eq!(supported.aggregate_filter_exprs.len(), 1);
    let predicates = supported
        .aggregate_filter_exprs
        .get("sum")
        .expect("filtered sum should carry its FILTER predicate")
        .leaf_predicates();
    assert_eq!(predicates.len(), 1);
    assert_eq!(
        predicates[0].relation_id,
        scores.relation_schema.relation_id
    );
    assert_eq!(predicates[0].predicate.column_id, "score");
    assert_eq!(predicates[0].predicate.op, PredicateOp::Gt);
    assert_eq!(predicates[0].predicate.literal, json!(0));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn left_join_sql_accepts_right_aggregate_inputs_and_post_join_filters() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let catalogs = [scores, accounts];

    for (sql, output_schema) in [
        (
            "select s.user_id as account_id, sum(s.score) as sum, count(*) as count, count(a.limit) as count_limit, count(distinct a.limit) as distinct_limits, sum(a.limit) as limit_sum, min(a.limit) as min_limit, max(a.limit) as max_limit, avg(a.limit) as avg_limit from scores s left join accounts a on s.user_id = a.account_id group by s.user_id",
            join_right_stats_output_schema(),
        ),
        (
            "select s.user_id as account_id, sum(s.score) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id where a.limit > 60 group by s.user_id",
            join_output_schema(),
        ),
        (
            "select s.user_id as account_id, sum(s.score) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id where a.limit is null group by s.user_id",
            join_output_schema(),
        ),
        (
            "select s.user_id as account_id, sum(s.score) as sum, count(*) as count, min(s.score) as min_score, max(a.limit) filter (where a.limit > 60) as max_score, avg(s.score) as avg_score from scores s left join accounts a on s.user_id = a.account_id group by s.user_id",
            join_stats_output_schema(),
        ),
        (
            "select s.user_id as account_id, sum(s.score) filter (where s.score > a.limit) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id group by s.user_id",
            join_output_schema(),
        ),
    ] {
        let plan = lower_supported_join_view_sql_to_logical_plan(sql, &catalogs, &output_schema)
            .unwrap();
        let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } =
            &plan.execution
        else {
            panic!("expected join runtime execution");
        };
        assert!(supported.right_value_column_ids.iter().any(|id| id == "limit"));
        if supported.predicate_expr.as_ref().is_some_and(|expr| {
            expr.leaf_predicates()
                .iter()
                .any(|predicate| predicate.relation_id == supported.right_input_relation_id)
        }) {
            assert!(plan.nodes.iter().any(|node| matches!(
                node,
                VelorixLogicalViewPlanNodeV1::Filter { node_id, input, .. }
                    if node_id == "filter_left_join_post_right" && input == "left_equi_join"
            )));
            assert!(!plan.nodes.iter().any(|node| matches!(
                node,
                VelorixLogicalViewPlanNodeV1::Filter { node_id, .. }
                    if node_id.starts_with("filter_join_right")
            )));
        }
        if supported.aggregate_outputs.iter().any(|aggregate| {
            aggregate.input_relation_side == Some(SupportedAggregateInputRelationSide::Right)
        }) {
            let join = plan
                .operator_dag_contract
                .operators
                .iter()
                .find(|operator| operator.operator.kind == "left_equi_join")
                .unwrap();
            assert!(join.outputs[0]
                .schema
                .columns
                .iter()
                .any(|column| column.column_id == "accounts.limit"
                    && column.nullability == NullabilityV1::Nullable));
        }
        validate_logical_view_plan(&plan).unwrap();
    }
}

#[test]
fn left_join_sql_keeps_right_dependent_shapes_fail_closed() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];
    let cases = [
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id group by a.account_id",
        "select s.user_id as account_id, sum(s.score) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id and a.limit > 0 group by s.user_id",
        "select s.user_id as account_id, sum(s.score) as sum, count(*) as count from scores s left join (select * from accounts where limit > 0) a on s.user_id = a.account_id group by s.user_id",
    ];

    for sql in cases {
        let error = lower_supported_join_view_sql_to_logical_plan(sql, &catalogs, &output_schema)
            .unwrap_err();
        assert!(
            matches!(error, ViewPlanError::UnsupportedShape { .. }),
            "expected fail-closed unsupported admission for SQL `{sql}`, got `{error}`"
        );
    }
}

#[test]
fn two_input_join_sql_accepts_filtered_min_max_avg_outputs() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_stats_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count, min(s.score) filter (where s.score > 0) as min_score, max(s.score) filter (where a.limit > 50) as max_score, avg(s.score) filter (where s.score >= 5) as avg_score from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert!(supported.aggregate_filter_exprs.contains_key("min_score"));
    assert!(supported.aggregate_filter_exprs.contains_key("max_score"));
    assert!(supported.aggregate_filter_exprs.contains_key("avg_score"));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_right_side_sum_min_max_avg_outputs() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_right_stats_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count, count(a.limit) as count_limit, count(distinct a.limit) as distinct_limits, sum(a.limit) as limit_sum, min(a.limit) as min_limit, max(a.limit) as max_limit, avg(a.limit) as avg_limit from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Min
            && aggregate.input_column_id.as_deref() == Some("limit")
            && aggregate.output_column_id == "min_limit"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Count
            && aggregate.input_column_id.as_deref() == Some("limit")
            && aggregate.output_column_id == "count_limit"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::CountDistinct
            && aggregate.input_column_id.as_deref() == Some("limit")
            && aggregate.output_column_id == "distinct_limits"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Sum
            && aggregate.input_column_id.as_deref() == Some("limit")
            && aggregate.input_relation_side == Some(SupportedAggregateInputRelationSide::Right)
            && aggregate.output_column_id == "limit_sum"
    }));
    assert_eq!(supported.right_value_column_ids, vec!["limit"]);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_left_side_sum_expression_input() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let supported = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score + 1) as adjusted_sum from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
    )
    .unwrap();

    let adjusted_sum = supported
        .aggregate_outputs
        .iter()
        .find(|aggregate| aggregate.output_column_id == "adjusted_sum")
        .unwrap();
    assert_eq!(adjusted_sum.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(adjusted_sum.input_column_id.as_deref(), Some("score"));
    assert_eq!(
        adjusted_sum.input_relation_side,
        Some(SupportedAggregateInputRelationSide::Left)
    );
    assert!(adjusted_sum.input_expression.is_some());
}

#[test]
fn two_input_join_sql_accepts_right_side_sum_expression_input() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let supported = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, sum(a.limit + 1) as adjusted_sum from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
    )
    .unwrap();

    let adjusted_sum = supported
        .aggregate_outputs
        .iter()
        .find(|aggregate| aggregate.output_column_id == "adjusted_sum")
        .unwrap();
    assert_eq!(adjusted_sum.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(adjusted_sum.input_column_id.as_deref(), Some("limit"));
    assert_eq!(
        adjusted_sum.input_relation_side,
        Some(SupportedAggregateInputRelationSide::Right)
    );
    assert!(adjusted_sum.input_expression.is_some());
    assert_eq!(supported.right_value_column_ids, vec!["limit"]);
}

#[test]
fn two_input_join_sql_binds_computed_function_having_to_projected_output() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let supported = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, sum(a.limit + 1) as adjusted_limit_sum from scores s join accounts a on s.user_id = a.account_id group by a.account_id having sum(a.limit + 1) > 10",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(
        supported.having_expr.as_ref().unwrap().leaf_predicates()[0].output_column_id,
        "adjusted_limit_sum"
    );
}

#[test]
fn two_input_join_sql_rejects_non_matching_computed_function_having() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let error = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, sum(a.limit + 1) as adjusted_limit_sum from scores s join accounts a on s.user_id = a.account_id group by a.account_id having sum(a.limit + 2) > 10",
        &[scores, accounts],
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn two_input_join_sql_accepts_multiple_right_aggregate_input_columns() {
    let scores = scores_catalog();
    let accounts = accounts_multi_value_catalog();

    let supported = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) as count, min(a.limit) as min_limit, max(a.quota) as max_quota, avg(a.quota) as avg_quota from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(supported.right_value_column_ids, vec!["limit", "quota"]);
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Min
            && aggregate.input_column_id.as_deref() == Some("limit")
            && aggregate.output_column_id == "min_limit"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Max
            && aggregate.input_column_id.as_deref() == Some("quota")
            && aggregate.output_column_id == "max_quota"
    }));
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Avg
            && aggregate.input_column_id.as_deref() == Some("quota")
            && aggregate.output_column_id == "avg_quota"
    }));
}

#[test]
fn two_input_join_sql_binds_right_aggregate_functions_in_having_and_order_by() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_right_stats_output_schema();

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count, count(a.limit) as count_limit, count(distinct a.limit) as distinct_limits, min(a.limit) as min_limit, max(a.limit) as max_limit, avg(a.limit) as avg_limit from scores s join accounts a on s.user_id = a.account_id group by a.account_id having avg(a.limit) > 60 order by max(a.limit) desc limit 1",
        &[scores, accounts],
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(
        supported
            .having
            .as_ref()
            .expect("right aggregate HAVING should bind")
            .output_column_id,
        "avg_limit"
    );
    assert_eq!(
        supported
            .top_k
            .as_ref()
            .expect("right aggregate ORDER BY should bind")
            .order_output_column_id,
        "max_limit"
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_admits_or_having_without_fake_filter_node() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id having sum(s.score) > 10 or count(*) = 1",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert!(supported
        .having_expr
        .as_ref()
        .is_some_and(AggregateOutputPredicateExpr::contains_or));
    assert!(!plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Filter { input, .. } if input.starts_with("aggregate_join_sum_count")
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_lowers_projected_aliases_to_accumulators() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_alias_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id as account, sum(s.score) as total_score, count(1) as score_events from scores s join accounts a on s.user_id = a.account_id group by a.account_id having total_score > 10 and count(1) > 1",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.output_key_column_id, "account");
    assert_eq!(supported.aggregate_outputs.len(), 2);
    assert_eq!(
        supported.aggregate_outputs[0].output_column_id,
        "total_score"
    );
    assert_eq!(
        supported.aggregate_outputs[1].output_column_id,
        "score_events"
    );
    assert_eq!(
        supported.having.as_ref().unwrap().output_column_id,
        "total_score"
    );
    let aggregate = plan
        .nodes
        .iter()
        .find_map(|node| match node {
            VelorixLogicalViewPlanNodeV1::Aggregate { accumulators, .. } => Some(accumulators),
            _ => None,
        })
        .expect("join plan should include aggregate node");
    assert!(aggregate.iter().any(|acc| {
        acc.function == LogicalPlanAggregateFunctionV1::Sum && acc.output_column_id == "total_score"
    }));
    assert!(aggregate.iter().any(|acc| {
        acc.function == LogicalPlanAggregateFunctionV1::Count
            && acc.output_column_id == "score_events"
    }));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_group_by_projected_key_alias() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id as account, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by account",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.group_key_column_id, "account_id");
    assert_eq!(supported.output_key_column_id, "account");
}

#[test]
fn two_input_join_sql_accepts_left_group_by_projected_key_alias() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_left_key_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select s.user_id as user, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by s.user_id",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.group_key_relation_id, "scores");
    assert_eq!(supported.group_key_column_id, "user_id");
    assert_eq!(supported.output_key_column_id, "user");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_rejects_mismatched_projection_and_group_key_sides() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    for sql in [
        "select s.user_id as user, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        "select a.account_id as account, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by s.user_id",
        "select s.score as score, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by s.score",
    ] {
        let error = validate_supported_join_view_sql(sql, &[scores.clone(), accounts.clone()])
            .unwrap_err();
        assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    }
}

#[test]
fn two_input_join_sql_accepts_group_by_first_projection_ordinal() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id as account, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by 1",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.group_key_column_id, "account_id");
    assert_eq!(supported.output_key_column_id, "account");
}

#[test]
fn two_input_join_sql_accepts_trailing_order_by_without_changing_materialization() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id as account, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by account",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { .. }
    ));
}

#[test]
fn two_input_join_sql_lowers_order_by_limit_top_k() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id as account, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum desc limit 1",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(
        supported.top_k.as_ref().map(|top_k| (
            top_k.order_output_column_id.as_str(),
            top_k.descending,
            top_k.limit
        )),
        Some(("sum", true, 1))
    );
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 1,
            ..
        } if order_by.column_id == "sum"
    )));
}

#[test]
fn two_input_join_sql_binds_order_by_min_max_avg_functions_to_projected_outputs() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let catalogs = [scores, accounts];

    for (sql, expected_output) in [
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count, min(s.score) as min_score from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by min(s.score) asc limit 1",
            "min_score",
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count, max(s.score) as max_score from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by max(s.score) desc limit 1",
            "max_score",
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count, avg(s.score) as avg_score from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by avg(s.score) desc limit 1",
            "avg_score",
        ),
    ] {
        let plan = validate_supported_join_view_sql(sql, &catalogs).unwrap();

        assert_eq!(
            plan.top_k
                .as_ref()
                .map(|top_k| top_k.order_output_column_id.as_str()),
            Some(expected_output)
        );
    }
}

#[test]
fn filter_project_sql_accepts_predicate_on_unprojected_catalog_column() {
    let catalog = scores_with_adjustment_catalog();

    let plan = validate_supported_filter_project_sql(
        "select user_id, score from scores where user_id_adjustment > 0 order by score desc, user_id asc limit 10",
        &catalog,
    )
    .unwrap();

    assert_eq!(
        plan.value_columns
            .iter()
            .map(|column| column.output_column_id.as_str())
            .collect::<Vec<_>>(),
        vec!["score"]
    );
    assert_eq!(
        plan.predicate_expr
            .as_ref()
            .expect("predicate should be admitted")
            .leaf_predicates()
            .first()
            .map(|predicate| predicate.column_id.as_str()),
        Some("user_id_adjustment")
    );
}

#[test]
fn filter_project_sql_rejects_predicate_on_weight_column() {
    let catalog = scores_with_adjustment_catalog();
    let error = validate_supported_filter_project_sql(
        "select user_id, score from scores where delta = 1",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(
        error,
        ViewPlanError::UnsupportedShape { reason }
            if reason == "filter/project WHERE must not reference the weight column"
    ));
}

#[test]
fn two_input_join_sql_lowers_left_where_to_pre_join_filter() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where s.score > 0 and s.score < 100 and a.limit > 60 and a.tier = 'gold' group by a.account_id",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    let predicate = supported
        .predicate
        .as_ref()
        .expect("JOIN WHERE should be admitted");
    assert_eq!(predicate.relation_id, "scores");
    assert_eq!(predicate.predicate.column_id, "score");
    assert_eq!(predicate.predicate.op, PredicateOp::Gt);
    assert_eq!(predicate.predicate.literal, json!(0));
    assert_eq!(supported.right_value_column_id.as_deref(), Some("limit"));
    assert_eq!(supported.right_value_column_ids, vec!["limit", "tier"]);
    assert_eq!(supported.predicates.len(), 3);
    assert_eq!(supported.predicates[0].relation_id, "scores");
    assert_eq!(supported.predicates[0].predicate.column_id, "score");
    assert_eq!(supported.predicates[0].predicate.op, PredicateOp::Lt);
    assert_eq!(supported.predicates[0].predicate.literal, json!(100));
    assert_eq!(supported.predicates[1].relation_id, "accounts");
    assert_eq!(supported.predicates[1].predicate.column_id, "limit");
    assert_eq!(supported.predicates[1].predicate.op, PredicateOp::Gt);
    assert_eq!(supported.predicates[1].predicate.literal, json!(60));
    assert_eq!(supported.predicates[2].relation_id, "accounts");
    assert_eq!(supported.predicates[2].predicate.column_id, "tier");
    assert_eq!(supported.predicates[2].predicate.op, PredicateOp::Eq);
    assert_eq!(supported.predicates[2].predicate.literal, json!("gold"));
    let score_filter_count = plan
        .nodes
        .iter()
        .filter(|node| {
            matches!(
                node,
                VelorixLogicalViewPlanNodeV1::Filter { predicate, .. }
                    if predicate.column.column_id == "score"
            )
        })
        .count();
    assert_eq!(score_filter_count, 2);
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Filter { input, predicate, .. }
            if input == "scan_right" && predicate.column.column_id == "limit"
    )));
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Filter { predicate, .. }
            if predicate.column.column_id == "tier"
    )));
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Filter { input, predicate, .. }
            if input == "scan_left" && predicate.column.column_id == "score"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_shared_aggregate_filter() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) filter (where s.score > 5) as sum, count(*) filter (where s.score > 5) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    let predicate = supported
        .predicate
        .as_ref()
        .expect("JOIN aggregate FILTER should lower to a runtime predicate");
    assert_eq!(predicate.relation_id, "scores");
    assert_eq!(predicate.predicate.column_id, "score");
    assert_eq!(predicate.predicate.op, PredicateOp::Gt);
    assert_eq!(predicate.predicate.literal, json!(5));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_mixed_aggregate_filters_without_fake_fallback() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) filter (where s.score > 5) as sum, count(*) filter (where s.score > 0) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert!(supported.predicate.is_none());
    assert_eq!(supported.aggregate_filter_exprs.len(), 2);
    assert!(supported.aggregate_filter_exprs.contains_key("sum"));
    assert!(supported.aggregate_filter_exprs.contains_key("count"));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_function_having_on_filtered_aggregate_output() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) filter (where s.score > 5) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id having sum(s.score) filter (where s.score > 5) > 10",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(
        supported
            .having_expr
            .as_ref()
            .expect("filtered function HAVING should be admitted")
            .leaf_predicates()[0]
            .output_column_id,
        "sum"
    );
}

#[test]
fn two_input_join_sql_rejects_non_matching_filtered_function_having() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let catalogs = [scores, accounts];

    let error = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) filter (where s.score > 5) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id having sum(s.score) filter (where s.score > 0) > 10",
        &catalogs,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn two_input_join_sql_accepts_alias_having_on_filtered_aggregate_output() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) filter (where s.score > 5) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id having sum > 10",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(
        supported
            .having_expr
            .as_ref()
            .expect("alias HAVING should be admitted")
            .leaf_predicates()[0]
            .output_column_id,
        "sum"
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_admits_same_relation_or_predicate_without_fake_filter_pushdown() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where (s.score > 0 or s.score = -3) and a.limit > 60 group by a.account_id",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert!(supported
        .predicate_expr
        .as_ref()
        .is_some_and(JoinPredicateExpr::contains_or));
    let predicates = supported
        .predicate_expr
        .as_ref()
        .expect("JOIN WHERE OR should be preserved")
        .leaf_predicates();
    assert_eq!(predicates.len(), 3);
    assert!(predicates.iter().any(|predicate| {
        predicate.relation_id == "scores"
            && predicate.predicate.column_id == "score"
            && predicate.predicate.op == PredicateOp::Eq
            && predicate.predicate.literal == json!(-3)
    }));
    assert!(predicates.iter().any(|predicate| {
        predicate.relation_id == "accounts"
            && predicate.predicate.column_id == "limit"
            && predicate.predicate.op == PredicateOp::Gt
            && predicate.predicate.literal == json!(60)
    }));
    assert!(
        !plan.nodes.iter().any(|node| matches!(
            node,
            VelorixLogicalViewPlanNodeV1::Filter { predicate, .. }
                if predicate.column.column_id == "score"
        )),
        "logical filter nodes cannot represent OR yet; runtime predicate_expr is authoritative"
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_admits_cross_relation_or_predicate_without_fake_filter_pushdown() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_output_schema();
    let catalogs = [scores, accounts];

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where s.score > 100 or a.limit > 60 group by a.account_id",
        &catalogs,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert!(supported
        .predicate_expr
        .as_ref()
        .is_some_and(JoinPredicateExpr::contains_or));
    let predicates = supported
        .predicate_expr
        .as_ref()
        .expect("JOIN WHERE OR should be preserved")
        .leaf_predicates();
    assert_eq!(predicates.len(), 2);
    assert!(predicates
        .iter()
        .any(|predicate| predicate.relation_id == "scores"));
    assert!(predicates
        .iter()
        .any(|predicate| predicate.relation_id == "accounts"));
    assert!(
        !plan
            .nodes
            .iter()
            .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { .. })),
        "cross-relation OR must be enforced by runtime joined-row predicate evaluation"
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_same_side_scalar_int64_residual_predicates() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let catalogs = [scores.clone(), accounts.clone()];

    for sql in [
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where s.score + 1 > 10 group by a.account_id",
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where 10 < s.score + 1 group by a.account_id",
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where a.limit + 1 > 60 group by a.account_id",
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id and s.score + 1 > 10 group by a.account_id",
    ] {
        let plan = lower_supported_join_view_sql_to_logical_plan(
            sql,
            &catalogs,
            &join_output_schema(),
        )
        .unwrap();

        validate_logical_view_plan(&plan).unwrap();
    }
}

#[test]
fn two_input_join_sql_accepts_cross_side_scalar_int64_residual_predicates() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let catalogs = [scores.clone(), accounts.clone()];

    for sql in [
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where s.score + 1 > a.limit group by a.account_id",
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where a.limit < s.score + 1 group by a.account_id",
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where s.score + 1 > a.limit + 1 group by a.account_id",
    ] {
        let plan =
            lower_supported_join_view_sql_to_logical_plan(sql, &catalogs, &join_output_schema())
                .unwrap();

        let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } =
            &plan.execution
        else {
            panic!("expected join runtime execution");
        };
        assert!(supported.predicate_expr.is_some());
        assert!(
            !plan
                .nodes
                .iter()
                .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { .. })),
            "cross-side scalar predicates must be enforced after the join"
        );
        validate_logical_view_plan(&plan).unwrap();
    }
}

#[test]
fn two_input_join_sql_rejects_mixed_side_scalar_int64_residual_predicates() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let catalogs = [scores, accounts];

    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where s.score + a.limit > 10 group by a.account_id";
    let error = validate_supported_join_view_sql(sql, &catalogs).unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(
        error
            .to_string()
            .contains("exactly one joined relation side"),
        "expected explicit cross-side scalar predicate rejection for SQL `{sql}`, got `{error}`"
    );
}

#[test]
fn two_input_join_sql_accepts_count_only_projection() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_count_output_schema();

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.sum_value_column_id, "score");
    assert_eq!(supported.aggregate_outputs.len(), 1);
    assert_eq!(
        supported.aggregate_outputs[0].function,
        LogicalPlanAggregateFunctionV1::Count
    );
    assert_eq!(supported.aggregate_outputs[0].output_column_id, "count");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_count_distinct_only_projection() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let output_schema = join_distinct_count_output_schema();

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, count(distinct s.score) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert_eq!(supported.sum_value_column_id, "score");
    assert_eq!(supported.aggregate_outputs.len(), 1);
    assert_eq!(
        supported.aggregate_outputs[0].function,
        LogicalPlanAggregateFunctionV1::CountDistinct
    );
    assert_eq!(
        supported.aggregate_outputs[0].input_column_id.as_deref(),
        Some("score")
    );
    assert_eq!(
        supported.aggregate_outputs[0].output_column_id,
        "distinct_scores"
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn latest_by_key_sql_lowers_to_project_and_latest_nodes() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status where enabled = true and event_time > 0 group by device_id",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    assert_eq!(plan.plan_version, LOGICAL_VIEW_PLAN_VERSION_V2);
    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    assert_eq!(supported.key_column_id, "device_id");
    assert_eq!(supported.value_column_id, "enabled");
    assert_eq!(supported.ordering_column_id, "event_time");
    assert_eq!(
        supported
            .predicate_expr
            .as_ref()
            .expect("latest WHERE should be admitted")
            .leaf_predicates()
            .len(),
        2
    );
    assert_eq!(
        plan.nodes
            .iter()
            .filter(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { input, .. } if input.starts_with("scan_input") || input.starts_with("filter_latest_input")))
            .count(),
        2
    );
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
fn latest_by_key_sql_admits_or_where_without_fake_filter_node() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status where enabled = true or event_time = 110 group by device_id",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    assert!(supported
        .predicate_expr
        .as_ref()
        .is_some_and(RowPredicateExpr::contains_or));
    assert!(!plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { input, .. } if input.starts_with("scan_input"))));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn latest_by_key_fixture_sql_accepts_arg_max_shape() {
    let catalog = device_status_catalog();

    let plan = validate_supported_latest_by_key_sql(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status where enabled = true group by device_id",
        &catalog,
    )
    .unwrap();

    assert_eq!(plan.input_relation_id, "device_status");
    assert_eq!(plan.key_column_id, "device_id");
    assert_eq!(plan.value_column_id, "enabled");
    assert_eq!(plan.ordering_column_id, "event_time");
    assert_eq!(plan.output_value_column_id, "enabled");
    assert!(plan.predicate_expr.is_some());
}

#[test]
fn latest_by_key_sql_rejects_nullable_ordering_column() {
    let mut catalog = device_status_catalog();
    catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "event_time")
        .unwrap()
        .nullable = true;
    let schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;

    let error = validate_supported_latest_by_key_sql(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(error.to_string().contains("must be non-nullable"));
}

#[test]
fn latest_by_key_fixture_sql_accepts_arg_min_shape() {
    let catalog = device_status_catalog();

    let plan = validate_supported_latest_by_key_sql(
        "select device_id, arg_min(enabled, event_time) as enabled from device_status group by device_id",
        &catalog,
    )
    .unwrap();

    assert_eq!(plan.function, LogicalPlanLatestByKeyFunctionV1::ArgMin);
    assert_eq!(plan.input_relation_id, "device_status");
    assert_eq!(plan.key_column_id, "device_id");
    assert_eq!(plan.value_column_id, "enabled");
    assert_eq!(plan.ordering_column_id, "event_time");
    assert_eq!(plan.output_value_column_id, "enabled");
}

#[test]
fn latest_by_key_fixture_sql_accepts_arg_max_filter_predicate() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select device_id, arg_max(enabled, event_time) filter (where enabled = true) as enabled from device_status group by device_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    let predicates = supported
        .predicate_expr
        .as_ref()
        .expect("arg_max FILTER should lower to latest input predicate")
        .leaf_predicates();
    assert_eq!(predicates.len(), 1);
    assert_eq!(predicates[0].column_id, "enabled");
    assert_eq!(predicates[0].op, PredicateOp::Eq);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn latest_by_key_fixture_sql_combines_where_and_arg_max_filter_predicates() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select device_id, arg_max(enabled, event_time) filter (where enabled = true) as enabled from device_status where event_time > 95 group by device_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    let predicates = supported
        .predicate_expr
        .as_ref()
        .expect("WHERE and arg_max FILTER should lower to latest input predicate")
        .leaf_predicates();
    assert_eq!(predicates.len(), 2);
    assert!(predicates
        .iter()
        .any(|predicate| predicate.column_id == "event_time" && predicate.op == PredicateOp::Gt));
    assert!(predicates
        .iter()
        .any(|predicate| predicate.column_id == "enabled" && predicate.op == PredicateOp::Eq));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn latest_by_key_sql_accepts_identity_cte_source_filters() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "with status_source as (select * from device_status where event_time > 95) select device_id, arg_max(enabled, event_time) as enabled from status_source where enabled = true group by device_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    let predicates = supported
        .predicate_expr
        .as_ref()
        .expect("CTE and outer WHERE should lower to latest input predicate")
        .leaf_predicates();
    assert_eq!(predicates.len(), 2);
    assert!(predicates
        .iter()
        .any(|predicate| predicate.column_id == "event_time" && predicate.op == PredicateOp::Gt));
    assert!(predicates
        .iter()
        .any(|predicate| predicate.column_id == "enabled" && predicate.op == PredicateOp::Eq));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn latest_by_key_sql_accepts_derived_table_source_filters() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select s.device_id, arg_max(s.enabled, s.event_time) as enabled from (select * from device_status where event_time > 95) s where s.enabled = true group by s.device_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    let predicates = supported
        .predicate_expr
        .as_ref()
        .expect("derived table and outer WHERE should lower to latest input predicate")
        .leaf_predicates();
    assert_eq!(predicates.len(), 2);
    assert!(predicates
        .iter()
        .any(|predicate| predicate.column_id == "event_time" && predicate.op == PredicateOp::Gt));
    assert!(predicates
        .iter()
        .any(|predicate| predicate.column_id == "enabled" && predicate.op == PredicateOp::Eq));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn latest_by_key_sql_accepts_inner_source_alias_filters() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let cte_plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "with status_source as (select * from device_status src where src.event_time > 95) select device_id, arg_max(enabled, event_time) as enabled from status_source where enabled = true group by device_id",
        &catalog,
        &output_schema,
    )
    .unwrap();
    let derived_plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select s.device_id, arg_max(s.enabled, s.event_time) as enabled from (select * from device_status src where src.event_time > 95) s where s.enabled = true group by s.device_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    for plan in [cte_plan, derived_plan] {
        let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
            panic!("expected latest-by-key runtime execution");
        };
        let predicates = supported
            .predicate_expr
            .as_ref()
            .expect("source and outer filters should lower")
            .leaf_predicates();
        assert_eq!(predicates.len(), 2);
        assert!(predicates.iter().any(
            |predicate| predicate.column_id == "event_time" && predicate.op == PredicateOp::Gt
        ));
        assert!(predicates
            .iter()
            .any(|predicate| predicate.column_id == "enabled" && predicate.op == PredicateOp::Eq));
        validate_logical_view_plan(&plan).unwrap();
    }
}

#[test]
fn latest_by_key_fixture_sql_accepts_relation_alias_qualified_columns() {
    let catalog = device_status_catalog();

    let plan = validate_supported_latest_by_key_sql(
        "select d.device_id as device, arg_max(d.enabled, d.event_time) as enabled from device_status as d where d.enabled = true group by d.device_id",
        &catalog,
    )
    .unwrap();

    assert_eq!(plan.key_column_id, "device_id");
    assert_eq!(plan.output_key_column_id, "device");
    assert_eq!(plan.value_column_id, "enabled");
    assert_eq!(plan.ordering_column_id, "event_time");
    assert_eq!(
        plan.predicate_expr
            .as_ref()
            .expect("qualified latest WHERE should be admitted")
            .leaf_predicates()
            .len(),
        1
    );
}

#[test]
fn latest_by_key_sql_accepts_group_by_projected_key_alias() {
    let catalog = device_status_catalog();
    let mut output_schema = latest_device_status_output_schema();
    output_schema.columns[0].name = "device".to_string();
    output_schema.primary_key = vec!["device".to_string()];

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select device_id as device, arg_max(enabled, event_time) as enabled from device_status group by device",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    assert_eq!(supported.key_column_id, "device_id");
    assert_eq!(supported.output_key_column_id, "device");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn latest_by_key_sql_accepts_group_by_first_projection_ordinal() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    assert_eq!(supported.key_column_id, "device_id");
    assert_eq!(supported.output_key_column_id, "device_id");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn latest_by_key_sql_accepts_group_by_all() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by all",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    assert_eq!(supported.key_column_id, "device_id");
    assert_eq!(supported.output_key_column_id, "device_id");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn latest_by_key_sql_accepts_trailing_order_by_without_changing_materialization() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by device_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::LatestByKey { .. }
    ));
}

#[test]
fn latest_by_key_sql_lowers_order_by_limit_top_k() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by device_id desc limit 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    assert_eq!(
        supported.top_k.as_ref().map(|top_k| (
            top_k.order_output_column_id.as_str(),
            top_k.descending,
            top_k.limit,
            top_k.offset
        )),
        Some(("device_id", true, 1, 0))
    );
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 1,
            offset: 0,
            ..
        } if order_by.column_id == "device_id"
    )));
}

#[test]
fn latest_by_key_sql_lowers_order_by_limit_offset_top_k() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by enabled desc limit 2 offset 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    assert_eq!(
        supported.top_k.as_ref().map(|top_k| (
            top_k.order_output_column_id.as_str(),
            top_k.descending,
            top_k.limit,
            top_k.offset
        )),
        Some(("enabled", true, 2, 1))
    );
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 2,
            offset: 1,
            ..
        } if order_by.column_id == "enabled"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn latest_by_key_sql_lowers_value_alias_order_by_limit_top_k() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by enabled desc limit 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    assert_eq!(
        supported
            .top_k
            .as_ref()
            .map(|top_k| top_k.order_output_column_id.as_str()),
        Some("enabled")
    );
}

#[test]
fn latest_by_key_sql_binds_order_by_arg_max_function_to_projected_top_k_output() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by arg_max(enabled, event_time) desc limit 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    assert_eq!(
        supported
            .top_k
            .as_ref()
            .map(|top_k| top_k.order_output_column_id.as_str()),
        Some("enabled")
    );
}

#[test]
fn latest_by_key_sql_binds_qualified_order_by_arg_max_function_to_projected_top_k_output() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select d.device_id, arg_max(d.enabled, d.event_time) as enabled from device_status as d group by d.device_id order by arg_max(d.enabled, d.event_time) desc limit 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    assert_eq!(
        supported
            .top_k
            .as_ref()
            .map(|top_k| top_k.order_output_column_id.as_str()),
        Some("enabled")
    );
}

#[test]
fn latest_by_key_sql_binds_order_by_arg_min_function_to_projected_top_k_output() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();

    let plan = lower_supported_latest_by_key_sql_to_logical_plan(
        "select device_id, arg_min(enabled, event_time) as enabled from device_status group by device_id order by arg_min(enabled, event_time) desc limit 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::LatestByKey { plan: supported } = &plan.execution else {
        panic!("expected latest-by-key runtime execution");
    };
    assert_eq!(
        supported
            .top_k
            .as_ref()
            .map(|top_k| top_k.order_output_column_id.as_str()),
        Some("enabled")
    );
}

#[test]
fn latest_by_key_sql_rejects_invalid_order_by_latest_function_top_k() {
    let catalog = device_status_catalog();
    let output_schema = latest_device_status_output_schema();
    let cases = [
        "select device_id, arg_max(enabled, event_time) filter (where enabled = true) as enabled from device_status group by device_id order by arg_max(enabled, event_time) desc limit 1",
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by arg_max(enabled, event_time) filter (where enabled = true) desc limit 1",
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by arg_max(device_id, event_time) desc limit 1",
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by arg_max(enabled, device_id) desc limit 1",
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by arg_max(not enabled, event_time) desc limit 1",
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by arg_max(enabled) desc limit 1",
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by arg_max(distinct enabled, event_time) desc limit 1",
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by arg_min(enabled, event_time) desc limit 1",
        "select device_id, arg_min(enabled, event_time) as enabled from device_status group by device_id order by arg_max(enabled, event_time) desc limit 1",
    ];

    for sql in cases {
        let error =
            lower_supported_latest_by_key_sql_to_logical_plan(sql, &catalog, &output_schema)
                .unwrap_err();

        assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    }
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
fn logical_view_plan_validation_rejects_tampered_operator_capability() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();
    let mut plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    plan.operator_dag_contract.operators[0].outputs[0].changelog = ChangelogModeV1::AppendOnly;

    let error = validate_logical_view_plan(&plan).unwrap_err();
    assert!(error
        .to_string()
        .contains("operator DAG contract does not match"));
}

#[test]
fn logical_view_plan_admission_rejects_incompatible_changelog_edge() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();
    let mut plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();
    let aggregate = plan
        .operator_dag_contract
        .operators
        .iter_mut()
        .find(|operator| operator.operator.kind == "aggregate")
        .unwrap();
    aggregate.inputs[0].accepted_changelog = AcceptedChangelogV1::AppendOnly;

    let error = validate_logical_view_plan(&plan).unwrap_err();
    assert!(error.to_string().contains("incompatible operator edge"));
    assert!(error.to_string().contains("changelog"));
}

#[test]
fn admitted_stateful_operators_expose_boundedness_classification() {
    let scores = scores_catalog();
    let aggregate = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id order by sum desc limit 1",
        &scores,
        &scores_output_schema(),
    )
    .unwrap();
    let join = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts_catalog()],
        &join_output_schema(),
    )
    .unwrap();
    let purchases = purchases_event_time_catalog();
    let window = lower_supported_tumbling_window_sql_to_logical_plan(
        "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end",
        &purchases,
        &purchases_window_output_schema(),
    )
    .unwrap();

    let states = aggregate
        .operator_dag_contract
        .operators
        .iter()
        .chain(&join.operator_dag_contract.operators)
        .chain(&window.operator_dag_contract.operators)
        .filter_map(|operator| {
            operator
                .state
                .as_ref()
                .map(|state| (operator.operator.kind.as_str(), &state.boundedness))
        })
        .collect::<Vec<_>>();
    assert!(states.iter().any(|(kind, boundedness)| {
        *kind == "top_k" && matches!(boundedness, StateBoundednessV1::Unbounded)
    }));
    assert!(states.iter().any(|(kind, boundedness)| {
        *kind == "inner_equi_join" && matches!(boundedness, StateBoundednessV1::Unbounded)
    }));
    assert!(states.iter().any(|(kind, boundedness)| {
        *kind == "tumbling_window"
            && matches!(boundedness, StateBoundednessV1::WatermarkBounded { .. })
    }));
    assert!(states.iter().all(|(_, boundedness)| matches!(
        boundedness,
        StateBoundednessV1::StaticallyBounded { .. }
            | StateBoundednessV1::RetentionBounded { .. }
            | StateBoundednessV1::WatermarkBounded { .. }
            | StateBoundednessV1::Unbounded
    )));
}

#[test]
fn logical_view_plan_validation_rejects_missing_operator_edge() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();
    let mut plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    plan.operator_dag_contract.edges.pop();

    let error = validate_logical_view_plan(&plan).unwrap_err();
    assert!(error.to_string().contains("every input port"));
}

#[test]
fn logical_view_plan_validation_rejects_disconnected_logical_node() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();
    let mut plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    plan.nodes.insert(
        1,
        VelorixLogicalViewPlanNodeV1::RelationScan {
            node_id: "orphan_scan".to_string(),
            relation: plan.input_relations[0].clone(),
        },
    );

    let error = validate_logical_view_plan(&plan).unwrap_err();
    assert!(error.to_string().contains("outside the output path"));
}

#[test]
fn logical_view_plan_wire_rejects_legacy_plan_without_operator_contract() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();
    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();
    let mut legacy_version = plan.clone();
    legacy_version.plan_version = LOGICAL_VIEW_PLAN_VERSION_V1;
    assert!(validate_logical_view_plan(&legacy_version)
        .unwrap_err()
        .to_string()
        .contains("plan version"));

    let mut json = serde_json::to_value(&plan).unwrap();
    json.as_object_mut()
        .unwrap()
        .remove("operator_dag_contract");

    let error = serde_json::from_value::<velorix_core::view_plan::VelorixLogicalViewPlanV1>(json)
        .unwrap_err();
    assert!(error.to_string().contains("operator_dag_contract"));
}

#[test]
fn logical_view_plan_validation_rejects_unknown_semantics_versions() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();
    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let mut unknown_key = plan.clone();
    unknown_key.key_semantics_version = "unknown-key-semantics".to_string();
    assert!(validate_logical_view_plan(&unknown_key)
        .unwrap_err()
        .to_string()
        .contains("key semantics version"));

    let mut unknown_bag = plan;
    unknown_bag.bag_semantics_version = "unknown-bag-semantics".to_string();
    assert!(validate_logical_view_plan(&unknown_bag)
        .unwrap_err()
        .to_string()
        .contains("bag semantics version"));
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
fn single_key_aggregate_sql_accepts_sum_arithmetic_expression() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, sum(score + 1) as adjusted_sum, count(*) as count from scores group by user_id",
        &catalog,
    )
    .unwrap();

    let adjusted_sum = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "adjusted_sum")
        .unwrap();
    assert_eq!(adjusted_sum.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(adjusted_sum.input_column_id.as_deref(), Some("score"));
    assert!(adjusted_sum.input_expression.is_some());

    let output_schema = RelationSchema {
        relation_id: "adjusted_scores_by_user".to_string(),
        relation_name: "adjusted_scores_by_user".to_string(),
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
                name: "adjusted_sum".to_string(),
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
    };
    let logical = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score + 1) as adjusted_sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();
    validate_logical_view_plan(&logical).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_cast_int64_expression() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let logical = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(cast(score as bigint)) as sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan } = &logical.execution else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(plan.sum_value_column_id, "score");
    assert!(plan.aggregate_outputs[0].input_expression.is_some());
    validate_logical_view_plan(&logical).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_nested_double_colon_cast_int64_expression() {
    let catalog = scores_catalog();
    let output_schema = RelationSchema {
        relation_id: "adjusted_scores_by_user".to_string(),
        relation_name: "adjusted_scores_by_user".to_string(),
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
                name: "adjusted_sum".to_string(),
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
    };

    let logical = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum((score + 1)::bigint) as adjusted_sum, count(*) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan } = &logical.execution else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(plan.sum_value_column_id, "score");
    assert!(plan.aggregate_outputs[0].input_expression.is_some());
    validate_logical_view_plan(&logical).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_try_and_safe_cast_int64_expressions() {
    let catalog = scores_catalog();
    let output_schema = RelationSchema {
        relation_id: "cast_scores_by_user".to_string(),
        relation_name: "cast_scores_by_user".to_string(),
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
                name: "try_sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "safe_sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    };

    let logical = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(try_cast(score as bigint)) as try_sum, sum(safe_cast(score as int64)) as safe_sum from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan } = &logical.execution else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(plan.aggregate_outputs.len(), 2);
    assert!(plan
        .aggregate_outputs
        .iter()
        .all(|output| output.input_expression.is_some()));
    validate_logical_view_plan(&logical).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_abs_int64_expression() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, sum(abs(score)) as absolute_sum, count(*) as count from scores group by user_id",
        &catalog,
    )
    .unwrap();

    let absolute_sum = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "absolute_sum")
        .unwrap();
    assert_eq!(absolute_sum.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(absolute_sum.input_column_id.as_deref(), Some("score"));
    assert!(absolute_sum.input_expression.is_some());
}

#[test]
fn single_key_aggregate_sql_accepts_greatest_least_int64_expressions() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, sum(greatest(score, 0)) as positive_floor_sum, sum(least(score, 10)) as capped_sum, count(*) as count from scores group by user_id",
        &catalog,
    )
    .unwrap();

    let positive_floor_sum = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "positive_floor_sum")
        .unwrap();
    assert_eq!(
        positive_floor_sum.function,
        LogicalPlanAggregateFunctionV1::Sum
    );
    assert_eq!(positive_floor_sum.input_column_id.as_deref(), Some("score"));
    assert!(positive_floor_sum.input_expression.is_some());

    let capped_sum = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "capped_sum")
        .unwrap();
    assert_eq!(capped_sum.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(capped_sum.input_column_id.as_deref(), Some("score"));
    assert!(capped_sum.input_expression.is_some());
}

#[test]
fn single_key_aggregate_sql_accepts_coalesce_nullable_int64_expression() {
    let mut catalog = scores_catalog();
    let score = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable score catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;

    let plan = validate_supported_view_sql(
        "select user_id, sum(coalesce(score, 0)) as coalesced_sum, count(*) as count from scores group by user_id",
        &catalog,
    )
    .unwrap();

    let coalesced_sum = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "coalesced_sum")
        .unwrap();
    assert_eq!(coalesced_sum.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(coalesced_sum.input_column_id.as_deref(), Some("score"));
    assert!(coalesced_sum.input_expression.is_some());
}

#[test]
fn single_key_aggregate_sql_accepts_case_when_int64_expression() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, sum(case when score > 0 then score else 0 end) as positive_sum, count(*) as count from scores group by user_id",
        &catalog,
    )
    .unwrap();

    let positive_sum = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "positive_sum")
        .unwrap();
    assert_eq!(positive_sum.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(positive_sum.input_column_id.as_deref(), Some("score"));
    assert!(positive_sum.input_expression.is_some());
}

#[test]
fn single_key_aggregate_sql_accepts_case_when_between_and_in_predicates() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, sum(case when score between 1 and 10 then score else 0 end) as bounded_sum, sum(case when score in (5, 7) then score else 0 end) as selected_sum from scores group by user_id",
        &catalog,
    )
    .unwrap();

    let bounded_sum = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "bounded_sum")
        .unwrap();
    assert_eq!(bounded_sum.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(bounded_sum.input_column_id.as_deref(), Some("score"));
    assert!(bounded_sum.input_expression.is_some());

    let selected_sum = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "selected_sum")
        .unwrap();
    assert_eq!(selected_sum.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(selected_sum.input_column_id.as_deref(), Some("score"));
    assert!(selected_sum.input_expression.is_some());
}

#[test]
fn single_key_aggregate_sql_accepts_case_when_distinct_from_null_predicates() {
    let mut catalog = scores_catalog();
    let score = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable score catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;

    let plan = validate_supported_view_sql(
        "select user_id, sum(case when score is distinct from null then coalesce(score, 0) else 0 end) as present_sum, sum(case when score is not distinct from null then 1 else 0 end) as null_count from scores group by user_id",
        &catalog,
    )
    .unwrap();

    let present_sum = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "present_sum")
        .unwrap();
    assert_eq!(present_sum.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(present_sum.input_column_id.as_deref(), Some("score"));
    assert!(present_sum.input_expression.is_some());

    let null_count = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "null_count")
        .unwrap();
    assert_eq!(null_count.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(null_count.input_column_id.as_deref(), Some("score"));
    assert!(null_count.input_expression.is_some());
}

#[test]
fn single_key_aggregate_sql_accepts_case_when_is_null_predicates() {
    let mut catalog = scores_catalog();
    let score = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable score catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;

    let plan = validate_supported_view_sql(
        "select user_id, sum(case when score is null then 1 else 0 end) as null_count, sum(case when score is not null then coalesce(score, 0) else 0 end) as present_sum from scores group by user_id",
        &catalog,
    )
    .unwrap();

    let null_count = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "null_count")
        .unwrap();
    assert_eq!(null_count.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(null_count.input_column_id.as_deref(), Some("score"));
    let Some(SupportedProjectionExpr::CaseInt64 { predicate, .. }) =
        null_count.input_expression.as_ref()
    else {
        panic!("null_count should use CASE input expression");
    };
    let RowPredicateExpr::Atom { predicate } = predicate else {
        panic!("null_count CASE predicate should be an atom");
    };
    assert_eq!(predicate.op, PredicateOp::IsNull);

    let present_sum = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "present_sum")
        .unwrap();
    assert_eq!(present_sum.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(present_sum.input_column_id.as_deref(), Some("score"));
    let Some(SupportedProjectionExpr::CaseInt64 { predicate, .. }) =
        present_sum.input_expression.as_ref()
    else {
        panic!("present_sum should use CASE input expression");
    };
    let RowPredicateExpr::Atom { predicate } = predicate else {
        panic!("present_sum CASE predicate should be an atom");
    };
    assert_eq!(predicate.op, PredicateOp::IsNotNull);
}

#[test]
fn single_key_aggregate_sql_accepts_if_int64_expression() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, sum(if(score > 0, score, 0)) as positive_sum, count(*) as count from scores group by user_id",
        &catalog,
    )
    .unwrap();

    let positive_sum = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "positive_sum")
        .unwrap();
    assert_eq!(positive_sum.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(positive_sum.input_column_id.as_deref(), Some("score"));
    assert!(positive_sum.input_expression.is_some());
}

#[test]
fn single_key_aggregate_sql_accepts_multi_branch_case_when_int64_expression() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, sum(case when score > 10 then 10 when score > 0 then score else 0 end) as capped_positive_sum, count(*) as count from scores group by user_id",
        &catalog,
    )
    .unwrap();

    let capped_positive_sum = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "capped_positive_sum")
        .unwrap();
    assert_eq!(
        capped_positive_sum.function,
        LogicalPlanAggregateFunctionV1::Sum
    );
    assert_eq!(
        capped_positive_sum.input_column_id.as_deref(),
        Some("score")
    );
    assert!(capped_positive_sum.input_expression.is_some());
}

#[test]
fn single_key_aggregate_sql_accepts_simple_case_when_int64_expression() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, sum(case score when 1 then 10 when 2 then 20 else 0 end) as bucket_sum, count(*) as count from scores group by user_id",
        &catalog,
    )
    .unwrap();

    let bucket_sum = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "bucket_sum")
        .unwrap();
    assert_eq!(bucket_sum.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(bucket_sum.input_column_id.as_deref(), Some("score"));
    assert!(bucket_sum.input_expression.is_some());
}

#[test]
fn single_key_aggregate_sql_accepts_min_max_arithmetic_expression() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, min(score + 1) as smallest, max(score + 1) as largest from scores group by user_id",
        &catalog,
    )
    .unwrap();

    let smallest = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "smallest")
        .unwrap();
    assert_eq!(smallest.function, LogicalPlanAggregateFunctionV1::Min);
    assert_eq!(smallest.input_column_id.as_deref(), Some("score"));
    assert!(smallest.input_expression.is_some());

    let largest = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "largest")
        .unwrap();
    assert_eq!(largest.function, LogicalPlanAggregateFunctionV1::Max);
    assert_eq!(largest.input_column_id.as_deref(), Some("score"));
    assert!(largest.input_expression.is_some());
}

#[test]
fn single_key_aggregate_sql_accepts_avg_arithmetic_expression() {
    let catalog = purchases_catalog_without_value_role();
    let output_schema = RelationSchema {
        relation_id: "adjusted_average_purchases_by_user".to_string(),
        relation_name: "adjusted_average_purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000008".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "average".to_string(),
                data_type: SqlDataType::Float64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    };

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, avg(amount + 1) as average from purchases group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan } = plan.execution else {
        panic!("expected single-key aggregate execution");
    };
    let average = plan
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "average")
        .unwrap();
    assert_eq!(average.function, LogicalPlanAggregateFunctionV1::Avg);
    assert_eq!(average.input_column_id.as_deref(), Some("amount"));
    assert!(average.input_expression.is_some());
}

#[test]
fn single_key_aggregate_sql_binds_computed_filtered_function_having() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, sum(score + 1) filter (where score > 0) as adjusted_sum from scores group by user_id having sum(score + 1) filter (where score > 0) > 10",
        &catalog,
    )
    .unwrap();

    assert_eq!(
        plan.having_expr.as_ref().unwrap().leaf_predicates()[0].output_column_id,
        "adjusted_sum"
    );
}

#[test]
fn single_key_aggregate_sql_rejects_non_matching_computed_function_having() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select user_id, sum(score + 1) as adjusted_sum from scores group by user_id having sum(score + 2) > 10",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn single_key_aggregate_sql_rejects_distinct_value_function_having() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select user_id, sum(score) as sum from scores group by user_id having sum(distinct score) > 10",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn single_key_aggregate_sql_lowers_order_by_limit_top_k() {
    let catalog = purchases_catalog_without_value_role();
    let output_schema = purchases_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id order by sum desc limit 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key aggregate execution");
    };
    assert_eq!(
        supported.top_k.as_ref().map(|top_k| (
            top_k.order_output_column_id.as_str(),
            top_k.descending,
            top_k.limit
        )),
        Some(("sum", true, 1))
    );
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 1,
            ..
        } if order_by.column_id == "sum"
    )));
}

#[test]
fn single_key_aggregate_sql_lowers_order_by_metric_then_key_top_k() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id order by sum(score) desc, user_id asc limit 10",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key aggregate execution");
    };
    assert_eq!(
        supported.top_k.as_ref().map(|top_k| (
            top_k.order_output_column_id.as_str(),
            top_k.tie_breaker_output_column_id.as_deref(),
            top_k.descending,
            top_k.limit
        )),
        Some(("sum", Some("user_id"), true, 10))
    );
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 10,
            ..
        } if order_by.column_id == "sum"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_rejects_non_key_second_order_by_top_k() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id order by sum desc, count asc limit 10",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(error
        .to_string()
        .contains("materialized top-k key tie-breaker"));
}

#[test]
fn single_key_aggregate_sql_binds_order_by_function_to_projected_top_k_output() {
    let catalog = scores_catalog();
    let mut output_schema = scores_output_schema();
    output_schema.columns[1].name = "total_score".to_string();
    output_schema.columns[2].name = "event_count".to_string();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as total_score, count(*) as event_count from scores group by user_id order by sum(score) desc limit 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key aggregate execution");
    };
    assert_eq!(
        supported
            .top_k
            .as_ref()
            .map(|top_k| top_k.order_output_column_id.as_str()),
        Some("total_score")
    );
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 1,
            ..
        } if order_by.column_id == "total_score"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_binds_order_by_computed_function_to_projected_top_k_output() {
    let catalog = scores_catalog();
    let mut output_schema = scores_output_schema();
    output_schema.columns[1].name = "adjusted_sum".to_string();
    output_schema.columns[2].name = "event_count".to_string();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score + 1) as adjusted_sum, count(*) as event_count from scores group by user_id order by sum(score + 1) desc limit 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key aggregate execution");
    };
    assert_eq!(
        supported
            .top_k
            .as_ref()
            .map(|top_k| top_k.order_output_column_id.as_str()),
        Some("adjusted_sum")
    );
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 1,
            ..
        } if order_by.column_id == "adjusted_sum"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_rejects_ambiguous_order_by_function_top_k() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select user_id, sum(score) as total_score, sum(score) as duplicate_total_score from scores group by user_id order by sum(score) desc limit 1",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn single_key_aggregate_sql_rejects_ambiguous_order_by_computed_function_top_k() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select user_id, sum(score + 1) as adjusted_sum, sum(score + 1) as duplicate_adjusted_sum from scores group by user_id order by sum(score + 1) desc limit 1",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn single_key_aggregate_sql_rejects_unprojected_order_by_function_top_k() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select user_id, count(*) as event_count from scores group by user_id order by sum(score) desc limit 1",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn single_key_aggregate_sql_rejects_unmatched_order_by_computed_function_top_k() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select user_id, sum(score + 1) as adjusted_sum from scores group by user_id order by sum(score + 2) desc limit 1",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn single_key_aggregate_sql_rejects_distinct_order_by_value_function_top_k() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select user_id, sum(score) as total_score from scores group by user_id order by sum(distinct score) desc limit 1",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn single_key_aggregate_sql_rejects_order_by_function_for_projected_expression_aggregate_top_k() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select user_id, sum(abs(score)) as absolute_sum from scores group by user_id order by sum(score) desc limit 1",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn single_key_aggregate_sql_binds_function_order_by_matching_filtered_aggregate_top_k() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select user_id, sum(score) filter (where score > 5) as filtered_score from scores group by user_id order by sum(score) filter (where score > 5) desc limit 1",
        &catalog,
    )
    .unwrap();
    assert_eq!(
        plan.top_k
            .as_ref()
            .map(|top_k| top_k.order_output_column_id.as_str()),
        Some("filtered_score")
    );

    let plan = validate_supported_view_sql(
        "select user_id, sum(score) filter (where score > 5) as filtered_score from scores group by user_id order by filtered_score desc limit 1",
        &catalog,
    )
    .unwrap();
    assert_eq!(
        plan.top_k
            .as_ref()
            .map(|top_k| top_k.order_output_column_id.as_str()),
        Some("filtered_score")
    );
}

#[test]
fn single_key_aggregate_sql_binds_matching_filtered_non_sum_order_by_functions() {
    let catalog = scores_catalog();

    for (sql, output_column_id) in [
        (
            "select user_id, count(*) filter (where score > 5) as positives from scores group by user_id order by count(*) filter (where score > 5) desc limit 1",
            "positives",
        ),
        (
            "select user_id, count(distinct score) filter (where score > 5) as distinct_positive_scores from scores group by user_id order by count(distinct score) filter (where score > 5) desc limit 1",
            "distinct_positive_scores",
        ),
        (
            "select user_id, min(score) filter (where score > 5) as min_positive_score from scores group by user_id order by min(score) filter (where score > 5) asc limit 1",
            "min_positive_score",
        ),
        (
            "select user_id, max(score) filter (where score > 5) as max_positive_score from scores group by user_id order by max(score) filter (where score > 5) desc limit 1",
            "max_positive_score",
        ),
        (
            "select user_id, avg(score) filter (where score > 5) as avg_positive_score from scores group by user_id order by avg(score) filter (where score > 5) desc limit 1",
            "avg_positive_score",
        ),
    ] {
        let plan = validate_supported_view_sql(sql, &catalog).unwrap();

        assert_eq!(
            plan.top_k
                .as_ref()
                .map(|top_k| top_k.order_output_column_id.as_str()),
            Some(output_column_id),
            "SQL should bind filtered ORDER BY function to projected output: {sql}"
        );
    }
}

#[test]
fn single_key_aggregate_sql_rejects_function_order_by_non_matching_filtered_aggregate_top_k() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select user_id, sum(score) filter (where score > 5) as filtered_score from scores group by user_id order by sum(score) filter (where score > 0) desc limit 1",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(error.to_string().contains(
        "materialized top-k ORDER BY aggregate function must reference one projected aggregate output"
    ));
}

#[test]
fn single_key_aggregate_sql_rejects_filtered_order_by_computed_function_top_k() {
    let catalog = scores_catalog();

    let error = validate_supported_view_sql(
        "select user_id, sum(score + 1) as adjusted_sum from scores group by user_id order by sum(score + 1) filter (where score > 5) desc limit 1",
        &catalog,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(error
        .to_string()
        .contains("materialized top-k ORDER BY aggregate function must reference one projected aggregate output"));
}

#[test]
fn single_key_aggregate_sql_rejects_limit_without_order_by() {
    let catalog = purchases_catalog_without_value_role();
    let error = validate_supported_view_sql(
        "select user_id, sum(amount) as total, count(*) as events from purchases group by user_id limit 1",
        &catalog,
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("LIMIT materialized views require ORDER BY"));
}

#[test]
fn single_key_aggregate_sql_accepts_fetch_first_top_k() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id order by sum desc fetch first 1 rows only",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key aggregate execution");
    };
    assert_eq!(supported.top_k.as_ref().unwrap().limit, 1);
    assert_eq!(
        supported.top_k.as_ref().unwrap().order_output_column_id,
        "sum"
    );
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
fn single_key_aggregate_sql_accepts_decimal_avg_as_float64_output() {
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

    let output_schema = RelationSchema {
        relation_id: "purchases_by_user".to_string(),
        relation_name: "purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:00000000000000000000000000000000000000000000000000000000000000da".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "average".to_string(),
                data_type: SqlDataType::Float64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    };

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, avg(amount) as average from purchases group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan } = plan.execution else {
        panic!("expected single-key aggregate execution");
    };
    assert!(plan.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Avg
            && aggregate.input_column_id.as_deref() == Some("amount")
            && aggregate.output_column_id == "average"
    }));
}

#[test]
fn tumbling_event_time_aggregate_sql_lowers_to_hashed_logical_view_plan() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') where amount > 0 and event_time >= 0 group by user_id, window_start, window_end having total_amount > 6 and event_count > 0";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    assert_eq!(plan.plan_version, LOGICAL_VIEW_PLAN_VERSION_V2);
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
    assert_eq!(
        supported
            .predicate_expr
            .as_ref()
            .expect("window WHERE should be admitted")
            .leaf_predicates()
            .len(),
        2
    );
    assert_eq!(
        plan.nodes
            .iter()
            .filter(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { input, .. } if input.starts_with("scan_input") || input.starts_with("filter_window_input")))
            .count(),
        2
    );
    assert_eq!(
        supported
            .having_expr
            .as_ref()
            .expect("window HAVING should be admitted")
            .leaf_predicates()
            .len(),
        2
    );
    assert_eq!(
        plan.nodes
            .iter()
            .filter(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { input, .. } if input.starts_with("aggregate_tumbling_window") || input.starts_with("filter_window_aggregate")))
            .count(),
        2
    );
    assert!(plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::TumblingWindow { .. })));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_matching_aggregate_filter_predicates() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(*) filter (where amount > 5) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    let leaves = supported
        .predicate_expr
        .as_ref()
        .expect("aggregate FILTER should lower to window input predicate")
        .leaf_predicates();
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves[0].column_id, "amount");
    assert_eq!(leaves[0].op, PredicateOp::Gt);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_identity_cte_source_filters() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "with purchase_source as (select * from purchases where amount > 5) select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchase_source, event_time, interval '60 seconds') where user_id <> 'bob' group by user_id, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    let predicates = supported
        .predicate_expr
        .as_ref()
        .expect("CTE and outer WHERE should lower to window input predicate")
        .leaf_predicates();
    assert_eq!(predicates.len(), 2);
    assert!(predicates
        .iter()
        .any(|predicate| predicate.column_id == "amount" && predicate.op == PredicateOp::Gt));
    assert!(predicates
        .iter()
        .any(|predicate| predicate.column_id == "user_id" && predicate.op == PredicateOp::NotEq));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_derived_table_source_filters() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select p.user_id, window_start, window_end, sum(p.amount) as total_amount, count(*) as event_count from (select * from purchases where amount > 5) p where p.user_id <> 'bob' group by p.user_id, tumble(interval '60 seconds')";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    let predicates = supported
        .predicate_expr
        .as_ref()
        .expect("derived table and outer WHERE should lower to window input predicate")
        .leaf_predicates();
    assert_eq!(predicates.len(), 2);
    assert!(predicates
        .iter()
        .any(|predicate| predicate.column_id == "amount" && predicate.op == PredicateOp::Gt));
    assert!(predicates
        .iter()
        .any(|predicate| predicate.column_id == "user_id" && predicate.op == PredicateOp::NotEq));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_inner_source_alias_filters() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let cte_sql = "with purchase_source as (select * from purchases src where src.amount > 5) select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchase_source, event_time, interval '60 seconds') where user_id <> 'bob' group by user_id, window_start, window_end";
    let derived_sql = "select p.user_id, window_start, window_end, sum(p.amount) as total_amount, count(*) as event_count from (select * from purchases src where src.amount > 5) p where p.user_id <> 'bob' group by p.user_id, tumble(interval '60 seconds')";

    for sql in [cte_sql, derived_sql] {
        let plan =
            lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema)
                .unwrap();
        let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
            &plan.execution
        else {
            panic!("expected tumbling event-time aggregate execution");
        };
        let predicates = supported
            .predicate_expr
            .as_ref()
            .expect("source and outer filters should lower")
            .leaf_predicates();
        assert_eq!(predicates.len(), 2);
        assert!(predicates
            .iter()
            .any(|predicate| predicate.column_id == "amount" && predicate.op == PredicateOp::Gt));
        assert!(predicates.iter().any(
            |predicate| predicate.column_id == "user_id" && predicate.op == PredicateOp::NotEq
        ));
        validate_logical_view_plan(&plan).unwrap();
    }
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_mixed_aggregate_filter_predicates() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert!(supported.predicate_expr.is_none());
    assert_eq!(supported.aggregate_filter_exprs.len(), 1);
    assert!(supported
        .aggregate_filter_exprs
        .contains_key("total_amount"));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_different_aggregate_filter_predicates() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(*) filter (where amount <= 5) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert!(supported.predicate_expr.is_none());
    assert_eq!(supported.aggregate_filter_exprs.len(), 2);
    assert!(supported
        .aggregate_filter_exprs
        .contains_key("total_amount"));
    assert!(supported.aggregate_filter_exprs.contains_key("event_count"));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_admits_or_where_without_fake_filter_node() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') where amount > 0 or user_id = 'alice' group by user_id, window_start, window_end having total_amount > 6 or event_count = 1";

    let plan =
        lower_supported_sql_to_logical_plan(sql, std::slice::from_ref(&catalog), &output_schema)
            .unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert!(supported
        .predicate_expr
        .as_ref()
        .is_some_and(RowPredicateExpr::contains_or));
    assert!(supported
        .having_expr
        .as_ref()
        .is_some_and(AggregateOutputPredicateExpr::contains_or));
    assert!(!plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { input, .. } if input.starts_with("scan_input"))));
    assert!(!plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::Filter { input, .. } if input.starts_with("aggregate_tumbling_window"))));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_arroyo_style_tumble_group_by() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id, tumble(interval '60 seconds')";

    let plan =
        lower_supported_sql_to_logical_plan(sql, std::slice::from_ref(&catalog), &output_schema)
            .unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert_eq!(supported.input_relation_id, "purchases");
    assert_eq!(supported.event_time_column_id, "event_time");
    assert_eq!(supported.window_size_ns, 60_000_000_000);
    assert_eq!(supported.window_start_output_column_id, "window_start");
    assert_eq!(supported.window_end_output_column_id, "window_end");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_sum_arithmetic_expression() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount + 1) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    let total = supported
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "total_amount")
        .unwrap();
    assert_eq!(total.function, LogicalPlanAggregateFunctionV1::Sum);
    assert_eq!(total.input_column_id.as_deref(), Some("amount"));
    assert!(total.input_expression.is_some());
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_binds_computed_function_having_to_projected_output() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount + 1) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end having sum(amount + 1) > 10";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    let total = supported
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "total_amount")
        .unwrap();
    assert!(total.input_expression.is_some());
    assert_eq!(
        supported.having_expr.as_ref().unwrap().leaf_predicates()[0].output_column_id,
        "total_amount"
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_rejects_non_matching_computed_function_having() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount + 1) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end having sum(amount + 2) > 10";

    let error = lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema)
        .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_min_max_arithmetic_expression() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_stats_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count, min(amount + 1) as minimum_amount, max(amount + 1) as maximum_amount, avg(amount) as average_amount from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    for (column_id, function) in [
        ("minimum_amount", LogicalPlanAggregateFunctionV1::Min),
        ("maximum_amount", LogicalPlanAggregateFunctionV1::Max),
    ] {
        let output = supported
            .aggregate_outputs
            .iter()
            .find(|output| output.output_column_id == column_id)
            .unwrap();
        assert_eq!(output.function, function);
        assert_eq!(output.input_column_id.as_deref(), Some("amount"));
        assert!(output.input_expression.is_some());
    }
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_avg_arithmetic_expression() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_stats_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count, min(amount) as minimum_amount, max(amount) as maximum_amount, avg(amount + 1) as average_amount from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    let average = supported
        .aggregate_outputs
        .iter()
        .find(|output| output.output_column_id == "average_amount")
        .unwrap();
    assert_eq!(average.function, LogicalPlanAggregateFunctionV1::Avg);
    assert_eq!(average.input_column_id.as_deref(), Some("amount"));
    assert!(average.input_expression.is_some());
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_mixed_aggregate_filters_with_having_and_top_k() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end having total_amount > 0 order by total_amount desc limit 2";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert!(supported.predicate_expr.is_none());
    assert_eq!(supported.aggregate_filter_exprs.len(), 1);
    assert!(supported
        .aggregate_filter_exprs
        .contains_key("total_amount"));
    assert!(supported.having_expr.is_some());
    assert_eq!(
        supported.top_k.as_ref().unwrap().order_output_column_id,
        "total_amount"
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_subsecond_interval_units() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '500 milliseconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert_eq!(supported.window_size_ns, 500_000_000);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_relation_alias_qualified_columns() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select p.user_id as user, window_start as start_time, window_end as end_time, sum(p.amount) as total_amount, count(1) as event_count from purchases as p where p.amount > 0 group by p.user_id, tumble(interval '60 seconds') having sum(p.amount) > 10 and count(1) > 0";

    let plan =
        lower_supported_sql_to_logical_plan(sql, std::slice::from_ref(&catalog), &output_schema)
            .unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert_eq!(supported.input_relation_id, "purchases");
    assert_eq!(supported.group_key_column_id, "user_id");
    assert_eq!(supported.output_key_column_id, "user");
    assert_eq!(supported.window_start_output_column_id, "start_time");
    assert_eq!(supported.window_end_output_column_id, "end_time");
    assert_eq!(supported.sum_value_column_id, "amount");
    assert_eq!(
        supported
            .predicate_expr
            .as_ref()
            .expect("qualified window WHERE should be admitted")
            .leaf_predicates()
            .len(),
        1
    );
    assert_eq!(
        supported
            .having_expr
            .as_ref()
            .expect("qualified window HAVING should be admitted")
            .leaf_predicates()
            .len(),
        2
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_group_by_projected_key_alias() {
    let catalog = purchases_event_time_catalog();
    let mut output_schema = purchases_window_output_schema();
    output_schema.columns[0].name = "customer".to_string();
    output_schema.primary_key[0] = "customer".to_string();
    let sql = "select user_id as customer, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by customer, window_start, window_end";

    let plan =
        lower_supported_sql_to_logical_plan(sql, std::slice::from_ref(&catalog), &output_schema)
            .unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert_eq!(supported.group_key_column_id, "user_id");
    assert_eq!(supported.output_key_column_id, "customer");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_group_by_all() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by all";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert_eq!(supported.group_key_column_id, "user_id");
    assert_eq!(supported.window_size_ns, 60_000_000_000);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_plain_distinct_grouped_output() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select distinct user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { .. }
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_lowers_count_distinct_projection() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(distinct amount) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected window runtime execution");
    };
    assert_eq!(
        supported.aggregate_outputs[1].function,
        LogicalPlanAggregateFunctionV1::CountDistinct
    );
    assert_eq!(
        supported.aggregate_outputs[1].input_column_id.as_deref(),
        Some("amount")
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_filtered_count_distinct_with_mixed_filters() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(distinct amount) filter (where amount > 0) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected window runtime execution");
    };
    assert_eq!(
        supported.aggregate_outputs[1].function,
        LogicalPlanAggregateFunctionV1::CountDistinct
    );
    assert_eq!(
        supported.aggregate_outputs[1].input_column_id.as_deref(),
        Some("amount")
    );
    assert_eq!(supported.aggregate_filter_exprs.len(), 2);
    assert!(supported
        .aggregate_filter_exprs
        .contains_key("total_amount"));
    assert!(supported.aggregate_filter_exprs.contains_key("event_count"));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_nullable_column_count() {
    let mut catalog = purchases_event_time_catalog();
    let amount = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "amount")
        .unwrap();
    amount.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable event-time purchases catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(amount) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected window runtime execution");
    };
    assert_eq!(
        supported.aggregate_outputs[1].function,
        LogicalPlanAggregateFunctionV1::Count
    );
    assert_eq!(
        supported.aggregate_outputs[1].input_column_id.as_deref(),
        Some("amount")
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_filtered_nullable_column_count() {
    let mut catalog = purchases_event_time_catalog();
    let amount = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "amount")
        .unwrap();
    amount.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable event-time purchases catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(amount) filter (where event_time >= 0) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected window runtime execution");
    };
    assert_eq!(
        supported.aggregate_outputs[1].function,
        LogicalPlanAggregateFunctionV1::Count
    );
    assert_eq!(
        supported.aggregate_outputs[1].input_column_id.as_deref(),
        Some("amount")
    );
    assert_eq!(supported.aggregate_filter_exprs.len(), 2);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_group_by_first_projection_ordinal() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by 1, window_start, window_end";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert_eq!(supported.group_key_column_id, "user_id");
    assert_eq!(supported.output_key_column_id, "user_id");
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_accepts_trailing_order_by_without_changing_materialization() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by user_id, window_start";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { .. }
    ));
}

#[test]
fn tumbling_event_time_aggregate_sql_lowers_order_by_limit_top_k() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by total_amount desc limit 1";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert_eq!(
        supported.top_k.as_ref().map(|top_k| (
            top_k.order_output_column_id.as_str(),
            top_k.descending,
            top_k.limit
        )),
        Some(("total_amount", true, 1))
    );
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 1,
            ..
        } if order_by.column_id == "total_amount"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_lowers_order_by_limit_offset_top_k() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by total_amount desc limit 2 offset 1";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    let top_k = supported.top_k.as_ref().unwrap();
    assert_eq!(top_k.order_output_column_id, "total_amount");
    assert!(top_k.descending);
    assert_eq!(top_k.limit, 2);
    assert_eq!(top_k.offset, 1);
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 2,
            offset: 1,
            ..
        } if order_by.column_id == "total_amount"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_rejects_unsupported_limit_offset_shapes() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let cases = [
        "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by total_amount desc limit 2 offset event_time",
        "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by total_amount desc limit 2 offset -1",
        "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by total_amount desc limit 1, 2",
    ];

    for sql in cases {
        let error =
            lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema)
                .unwrap_err();

        assert!(
            matches!(error, ViewPlanError::UnsupportedShape { .. }),
            "{error}"
        );
    }
}

#[test]
fn tumbling_event_time_aggregate_sql_binds_order_by_sum_function_to_projected_top_k_output() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by sum(amount) desc limit 1";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert_eq!(
        supported
            .top_k
            .as_ref()
            .map(|top_k| top_k.order_output_column_id.as_str()),
        Some("total_amount")
    );
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 1,
            ..
        } if order_by.column_id == "total_amount"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_binds_filtered_order_by_sum_function_to_projected_top_k_output(
) {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by sum(amount) filter (where amount > 5) desc limit 1";

    let plan =
        lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected tumbling event-time aggregate execution");
    };
    assert_eq!(
        supported
            .top_k
            .as_ref()
            .map(|top_k| top_k.order_output_column_id.as_str()),
        Some("total_amount")
    );
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn tumbling_event_time_aggregate_sql_rejects_function_order_by_filtered_aggregate_top_k() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by sum(amount) desc limit 1";

    let error = lower_supported_tumbling_window_sql_to_logical_plan(sql, &catalog, &output_schema)
        .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(error.to_string().contains(
        "materialized top-k ORDER BY aggregate function must reference one unfiltered projected aggregate output"
    ));
}

#[test]
fn hopping_event_time_aggregate_sql_lowers_to_window_plan() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id, hop(interval '30 seconds', interval '60 seconds')";

    let plan =
        lower_supported_sql_to_logical_plan(sql, std::slice::from_ref(&catalog), &output_schema)
            .unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected event-time aggregate execution");
    };
    assert_eq!(supported.window_kind, SupportedEventTimeWindowKind::Hopping);
    assert_eq!(supported.window_size_ns, 60_000_000_000);
    assert_eq!(supported.hop_slide_ns, Some(30_000_000_000));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn hopping_event_time_aggregate_sql_accepts_from_hop_table_function() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from hop(purchases, event_time, interval '30 seconds', interval '60 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_sql_to_logical_plan(sql, std::slice::from_ref(&catalog), &output_schema)
            .unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected event-time aggregate execution");
    };
    assert_eq!(supported.window_kind, SupportedEventTimeWindowKind::Hopping);
    assert_eq!(supported.window_size_ns, 60_000_000_000);
    assert_eq!(supported.hop_slide_ns, Some(30_000_000_000));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn session_event_time_aggregate_sql_lowers_to_window_plan() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id, session(interval '30 seconds')";

    let plan =
        lower_supported_sql_to_logical_plan(sql, std::slice::from_ref(&catalog), &output_schema)
            .unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected event-time aggregate execution");
    };
    assert_eq!(supported.window_kind, SupportedEventTimeWindowKind::Session);
    assert_eq!(supported.window_size_ns, 30_000_000_000);
    assert_eq!(supported.session_gap_ns, Some(30_000_000_000));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn session_event_time_aggregate_sql_accepts_from_session_table_function() {
    let catalog = purchases_event_time_catalog();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from session(purchases, event_time, interval '30 seconds') group by user_id, window_start, window_end";

    let plan =
        lower_supported_sql_to_logical_plan(sql, std::slice::from_ref(&catalog), &output_schema)
            .unwrap();

    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan: supported } =
        &plan.execution
    else {
        panic!("expected event-time aggregate execution");
    };
    assert_eq!(supported.window_kind, SupportedEventTimeWindowKind::Session);
    assert_eq!(supported.window_size_ns, 30_000_000_000);
    assert_eq!(supported.session_gap_ns, Some(30_000_000_000));
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

fn scores_min_max_avg_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_user_stats".to_string(),
        relation_name: "scores_by_user_stats".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000011".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "min_pos".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "max_nonpos".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "avg_hi".to_string(),
                data_type: SqlDataType::Float64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn scores_multi_input_stats_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_user_multi_input_stats".to_string(),
        relation_name: "scores_by_user_multi_input_stats".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "scores-multi-input-stats-v1".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "sum_score".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "min_adj".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "max_adj".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "avg_adj".to_string(),
                data_type: SqlDataType::Float64,
                nullable: false,
            },
            ColumnSchema {
                name: "count_adj".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn scores_projection_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "positive_scores".to_string(),
        relation_name: "positive_scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "scores-projection-v1".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "score".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn scores_distinct_score_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "positive_scores".to_string(),
        relation_name: "positive_scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "scores-distinct-score-v1".to_string(),
        columns: vec![ColumnSchema {
            name: "score".to_string(),
            data_type: SqlDataType::Int64,
            nullable: false,
        }],
        primary_key: vec!["score".to_string()],
    }
}

fn scores_key_only_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "positive_scores".to_string(),
        relation_name: "positive_scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "scores-key-only-v1".to_string(),
        columns: vec![ColumnSchema {
            name: "user_id".to_string(),
            data_type: SqlDataType::Utf8,
            nullable: false,
        }],
        primary_key: vec!["user_id".to_string()],
    }
}

fn scores_row_number_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "ranked_scores".to_string(),
        relation_name: "ranked_scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "scores-row-number-v1".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "rank".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn scores_count_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_user_count".to_string(),
        relation_name: "scores_by_user_count".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:000000000000000000000000000000000000000000000000000000000000000a".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
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

fn scores_computed_projection_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "positive_scores".to_string(),
        relation_name: "positive_scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "scores-computed-projection-v1".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "normalized_score".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn scores_adjusted_projection_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "positive_scores".to_string(),
        relation_name: "positive_scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "scores-adjusted-projection-v1".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "adjusted_score".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn scores_duplicate_computed_projection_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "positive_scores".to_string(),
        relation_name: "positive_scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "scores-duplicate-computed-projection-v1".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "adjusted_score".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "adjusted_score_copy".to_string(),
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

fn device_status_flag_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "device_enabled_flags".to_string(),
        relation_name: "device_enabled_flags".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "device-status-flag-v1".to_string(),
        columns: vec![
            ColumnSchema {
                name: "device_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "enabled_flag".to_string(),
                data_type: SqlDataType::Int64,
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
                name: "account".to_string(),
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
        primary_key: vec!["account".to_string()],
    }
}

fn join_decimal_avg_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_account_decimal_avg".to_string(),
        relation_name: "scores_by_account_decimal_avg".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000018".to_string(),
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
            ColumnSchema {
                name: "avg_limit".to_string(),
                data_type: SqlDataType::Float64,
                nullable: false,
            },
        ],
        primary_key: vec!["account_id".to_string()],
    }
}

fn join_count_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_account_count".to_string(),
        relation_name: "scores_by_account_count".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000013".to_string(),
        columns: vec![
            ColumnSchema {
                name: "account_id".to_string(),
                data_type: SqlDataType::Utf8,
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

fn join_distinct_count_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_account_distinct_count".to_string(),
        relation_name: "scores_by_account_distinct_count".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000014".to_string(),
        columns: vec![
            ColumnSchema {
                name: "account_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "distinct_scores".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["account_id".to_string()],
    }
}

fn join_left_key_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_user".to_string(),
        relation_name: "scores_by_user".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000011".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user".to_string(),
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
        primary_key: vec!["user".to_string()],
    }
}

fn join_alias_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_account_alias".to_string(),
        relation_name: "scores_by_account_alias".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000008".to_string(),
        columns: vec![
            ColumnSchema {
                name: "account_id".to_string(),
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
        ],
        primary_key: vec!["account_id".to_string()],
    }
}

fn join_stats_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_account_stats".to_string(),
        relation_name: "scores_by_account_stats".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000009".to_string(),
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
            ColumnSchema {
                name: "min_score".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "max_score".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "avg_score".to_string(),
                data_type: SqlDataType::Float64,
                nullable: false,
            },
        ],
        primary_key: vec!["account_id".to_string()],
    }
}

fn join_right_stats_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_account_limits".to_string(),
        relation_name: "scores_by_account_limits".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000010".to_string(),
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
            ColumnSchema {
                name: "count_limit".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "distinct_limits".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "limit_sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "min_limit".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "max_limit".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "avg_limit".to_string(),
                data_type: SqlDataType::Float64,
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
fn filter_project_sql_accepts_computed_int64_projection() {
    let catalog = scores_catalog();
    let output_schema = scores_computed_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, -score + score / 2 + score % 3 as normalized_score from scores where score > 0",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    let [projection] = supported.value_columns.as_slice() else {
        panic!("expected one projection");
    };
    assert_eq!(projection.output_column_id, "normalized_score");
    assert!(projection.expression.is_some());
    assert_eq!(
        supported.predicate_expr.as_ref().unwrap().leaf_predicates()[0].column_id,
        "score"
    );
}

#[test]
fn filter_project_sql_accepts_scalar_expression_predicate() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores where abs(score) > 10",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    let Some(RowPredicateExpr::ScalarInt64Comparison {
        comparison_op,
        literal,
        ..
    }) = supported.predicate_expr.as_ref()
    else {
        panic!("expected scalar Int64 predicate expression");
    };
    assert_eq!(*comparison_op, PredicateOp::Gt);
    assert_eq!(*literal, json!(10));
}

#[test]
fn filter_project_sql_accepts_scalar_expression_vs_expression_predicate() {
    let catalog = scores_with_adjustment_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores where score + 1 > user_id_adjustment",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    let Some(RowPredicateExpr::ScalarInt64ExpressionComparison { comparison_op, .. }) =
        supported.predicate_expr.as_ref()
    else {
        panic!("expected scalar Int64 expression comparison predicate");
    };
    assert_eq!(*comparison_op, PredicateOp::Gt);
}

#[test]
fn filter_project_sql_rejects_non_int64_expression_vs_expression_predicate() {
    let catalog = scores_with_adjustment_catalog();
    let output_schema = scores_projection_output_schema();

    let error = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores where user_id > user_id_adjustment",
        &catalog,
        &output_schema,
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn filter_project_sql_accepts_case_projection_over_bool_predicate() {
    let catalog = device_status_catalog();
    let output_schema = device_status_flag_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select device_id, case when enabled = true then 1 else 0 end as enabled_flag from device_status",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    let [projection] = supported.value_columns.as_slice() else {
        panic!("expected one projection");
    };
    assert_eq!(projection.input_column_id, "enabled");
    assert_eq!(projection.output_column_id, "enabled_flag");
    let Some(SupportedProjectionExpr::CaseInt64 { predicate, .. }) = &projection.expression else {
        panic!("expected CASE projection expression");
    };
    let predicates = predicate.leaf_predicates();
    let [predicate] = predicates.as_slice() else {
        panic!("expected one CASE predicate");
    };
    assert_eq!(predicate.column_id, "enabled");
    assert_eq!(predicate.op, PredicateOp::Eq);
    assert_eq!(predicate.literal, json!(true));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_rejects_case_bool_predicate_non_bool_literals() {
    let catalog = device_status_catalog();
    let output_schema = device_status_flag_output_schema();

    for sql in [
        "select device_id, case when enabled = 1 then 1 else 0 end as enabled_flag from device_status",
        "select device_id, case when enabled = 'true' then 1 else 0 end as enabled_flag from device_status",
    ] {
        let error =
            lower_supported_filter_project_sql_to_logical_plan(sql, &catalog, &output_schema)
                .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("Boolean CASE predicates require Boolean literals"),
            "{error}"
        );
    }
}

#[test]
fn filter_project_sql_accepts_nullable_direct_projection() {
    let mut catalog = scores_catalog();
    let score = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable score catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    let mut output_schema = scores_projection_output_schema();
    output_schema.columns[1].nullable = true;

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores where user_id is not null",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    assert_eq!(supported.value_columns[0].input_column_id, "score");
    assert!(supported.value_columns[0].expression.is_none());
}

#[test]
fn filter_project_sql_accepts_trailing_order_by_without_changing_materialization() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores where score > 0 order by score desc",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    assert_eq!(supported.value_columns.len(), 1);
    assert!(supported.predicate_expr.is_some());
    assert!(!plan
        .nodes
        .iter()
        .any(|node| matches!(node, VelorixLogicalViewPlanNodeV1::TopK { .. })));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_lowers_order_by_limit_top_k() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores where score > 0 order by score desc limit 2",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    let top_k = supported.top_k.as_ref().expect("top-k should be bound");
    assert_eq!(top_k.order_output_column_id, "score");
    assert!(top_k.descending);
    assert_eq!(top_k.limit, 2);
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 2,
            ..
        } if order_by.column_id == "score"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_lowers_order_by_fetch_first_top_k() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores where score > 0 order by score desc, user_id asc fetch first 2 rows only",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    let top_k = supported.top_k.as_ref().expect("top-k should be bound");
    assert_eq!(top_k.order_output_column_id, "score");
    assert_eq!(
        top_k.tie_breaker_output_column_id.as_deref(),
        Some("user_id")
    );
    assert!(top_k.descending);
    assert_eq!(top_k.limit, 2);
    assert_eq!(top_k.offset, 0);
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 2,
            offset: 0,
            ..
        } if order_by.column_id == "score"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_lowers_order_by_limit_offset_top_k() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores where score > 0 order by score desc, user_id asc limit 2 offset 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    let top_k = supported.top_k.as_ref().expect("top-k should be bound");
    assert_eq!(top_k.order_output_column_id, "score");
    assert_eq!(top_k.order_input_column_id.as_deref(), None);
    assert_eq!(
        top_k.tie_breaker_output_column_id.as_deref(),
        Some("user_id")
    );
    assert!(top_k.descending);
    assert_eq!(top_k.limit, 2);
    assert_eq!(top_k.offset, 1);
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 2,
            offset: 1,
            ..
        } if order_by.column_id == "score"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_lowers_order_by_metric_then_key_top_k() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, score from scores where score > 0 order by score desc, user_id asc limit 2",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    let top_k = supported.top_k.as_ref().expect("top-k should be bound");
    assert_eq!(top_k.order_output_column_id, "score");
    assert_eq!(
        top_k.tie_breaker_output_column_id.as_deref(),
        Some("user_id")
    );
    assert!(top_k.descending);
    assert_eq!(top_k.limit, 2);
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_lowers_hidden_input_order_by_limit_top_k() {
    let catalog = scores_catalog();
    let output_schema = scores_key_only_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id from scores where score > 0 order by score desc, user_id asc limit 10",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    assert!(supported.value_columns.is_empty());
    let top_k = supported.top_k.as_ref().expect("top-k should be bound");
    assert_eq!(top_k.order_output_column_id, "score");
    assert_eq!(top_k.order_input_column_id.as_deref(), Some("score"));
    assert_eq!(
        top_k.tie_breaker_output_column_id.as_deref(),
        Some("user_id")
    );
    assert!(top_k.descending);
    assert_eq!(top_k.limit, 10);
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 10,
            ..
        } if order_by.column_id == "score"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_rejects_hidden_nullable_order_by_limit_top_k() {
    let mut catalog = scores_catalog();
    let score = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    catalog.schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable score catalog should fingerprint");
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    let output_schema = scores_key_only_output_schema();

    let error = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id from scores where score > 0 order by score desc, user_id asc limit 10",
        &catalog,
        &output_schema,
    )
    .unwrap_err();

    assert!(
        matches!(error, ViewPlanError::UnsupportedShape { .. }),
        "{error}"
    );
    assert!(
        error.to_string().contains("hidden ORDER BY column"),
        "{error}"
    );
}

#[test]
fn filter_project_sql_lowers_computed_order_by_limit_top_k() {
    let catalog = scores_catalog();
    let output_schema = scores_computed_projection_output_schema();

    let plan = lower_supported_filter_project_sql_to_logical_plan(
        "select user_id, -score + score / 2 + score % 3 as normalized_score from scores where score > 0 order by -score + score / 2 + score % 3 desc limit 2",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::FilterProject { plan: supported } = &plan.execution else {
        panic!("expected filter/project runtime execution");
    };
    let top_k = supported.top_k.as_ref().expect("top-k should be bound");
    assert_eq!(top_k.order_output_column_id, "normalized_score");
    assert!(top_k.descending);
    assert_eq!(top_k.limit, 2);
    assert!(plan.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::TopK {
            order_by,
            descending: true,
            limit: 2,
            ..
        } if order_by.column_id == "normalized_score"
    )));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn filter_project_sql_rejects_unmatched_or_ambiguous_computed_order_by_limit_top_k() {
    let catalog = scores_catalog();

    let cases = [
        (
            "select user_id, score + 1 as adjusted_score from scores where score > 0 order by score + 2 desc limit 2",
            scores_adjusted_projection_output_schema(),
        ),
        (
            "select user_id, score + 1 as adjusted_score, score + 1 as adjusted_score_copy from scores where score > 0 order by score + 1 desc limit 2",
            scores_duplicate_computed_projection_output_schema(),
        ),
    ];
    for (sql, output_schema) in cases {
        let error =
            lower_supported_filter_project_sql_to_logical_plan(sql, &catalog, &output_schema)
                .unwrap_err();

        assert!(
            matches!(error, ViewPlanError::UnsupportedShape { .. }),
            "{error}"
        );
        assert!(
            error
                .to_string()
                .contains("filter/project top-k ORDER BY computed expression"),
            "{error}"
        );
    }
}

#[test]
fn filter_project_sql_rejects_unsupported_order_by_limit_top_k_shapes() {
    let catalog = scores_catalog();
    let output_schema = scores_projection_output_schema();

    for sql in [
        "select user_id, score from scores where score > 0 limit 2",
        "select user_id, score from scores where score > 0 order by delta desc limit 2",
        "select user_id, score from scores where score > 0 order by score desc limit 2 offset delta",
        "select user_id, score from scores where score > 0 order by score desc limit 2 offset -1",
        "select user_id, score from scores where score > 0 order by score desc fetch first 2 rows with ties",
        "select user_id, score from scores where score > 0 order by score desc fetch first 50 percent rows only",
        "select user_id, score from scores where score > 0 order by score desc limit 2 fetch first 1 rows only",
    ] {
        let error =
            lower_supported_filter_project_sql_to_logical_plan(sql, &catalog, &output_schema)
                .unwrap_err();

        assert!(
            matches!(error, ViewPlanError::UnsupportedShape { .. }),
            "{error}"
        );
    }
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
fn two_input_join_sql_accepts_generic_adapter_catalogs() {
    let scores = generic_adapter_catalog(scores_catalog());
    let accounts = generic_adapter_catalog(accounts_catalog());
    let output_schema = join_output_schema();

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
        &output_schema,
    )
    .unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { .. }
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_on_residual_predicates() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id and a.limit > 60 and s.score >= 5 group by a.account_id",
        &[scores, accounts],
    )
    .unwrap();

    let predicates = plan.predicate_expr.unwrap().leaf_predicates();
    assert_eq!(predicates.len(), 2);
    assert_eq!(predicates[0].relation_id, "accounts");
    assert_eq!(predicates[0].predicate.column_id, "limit");
    assert_eq!(predicates[0].predicate.op, PredicateOp::Gt);
    assert_eq!(predicates[0].predicate.literal, json!(60));
    assert_eq!(predicates[1].relation_id, "scores");
    assert_eq!(predicates[1].predicate.column_id, "score");
    assert_eq!(predicates[1].predicate.op, PredicateOp::GtEq);
    assert_eq!(predicates[1].predicate.literal, json!(5));
}

#[test]
fn two_input_join_sql_combines_on_residual_with_where_predicate() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id and a.limit > 60 where s.score >= 5 group by a.account_id",
        &[scores, accounts],
    )
    .unwrap();

    let predicates = plan.predicate_expr.unwrap().leaf_predicates();
    assert_eq!(predicates.len(), 2);
    assert_eq!(predicates[0].relation_id, "accounts");
    assert_eq!(predicates[0].predicate.column_id, "limit");
    assert_eq!(predicates[1].relation_id, "scores");
    assert_eq!(predicates[1].predicate.column_id, "score");
}

#[test]
fn two_input_join_sql_preserves_using_without_residual_predicate() {
    let scores = scores_catalog();
    let accounts = accounts_catalog_with_user_id_key();

    let plan = validate_supported_join_view_sql(
        "select a.user_id, sum(s.score) as sum, count(*) as count from scores s join accounts a using (user_id) group by a.user_id",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(plan.left_join_key_column_id, "user_id");
    assert_eq!(plan.right_join_key_column_id, "user_id");
    assert!(plan.predicate_expr.is_none());
}

#[test]
fn two_input_join_sql_rejects_on_without_exactly_one_key_equality() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let error = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on a.limit > 60 group by a.account_id",
        &[scores, accounts],
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn two_input_join_sql_deduplicates_repeated_key_equality() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id and s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(plan.left_join_key_column_id, "user_id");
    assert_eq!(plan.right_join_key_column_id, "account_id");
    assert!(plan.predicate_expr.is_none());
}

#[test]
fn two_input_join_sql_rejects_on_or_join_residuals() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id or a.limit > 60 group by a.account_id";
    let error = validate_supported_join_view_sql(sql, &[scores, accounts]).unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn two_input_join_sql_accepts_count_distinct_left_value() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(distinct s.score) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(
        plan.aggregate_outputs[0].function,
        LogicalPlanAggregateFunctionV1::Sum
    );
    assert_eq!(
        plan.aggregate_outputs[1].function,
        LogicalPlanAggregateFunctionV1::CountDistinct
    );
    assert_eq!(
        plan.aggregate_outputs[1].input_column_id.as_deref(),
        Some("score")
    );
    assert_eq!(
        plan.aggregate_outputs[1].output_column_id,
        "distinct_scores"
    );
}

#[test]
fn two_input_join_sql_binds_having_count_distinct_function_to_projected_output() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(distinct s.score) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id having count(distinct s.score) > 1",
        &[scores, accounts],
    )
    .unwrap();

    let having = plan.having.unwrap();
    assert_eq!(having.output_column_id, "distinct_scores");
    assert_eq!(having.op, PredicateOp::Gt);
    assert_eq!(having.literal, json!(1));
}

#[test]
fn two_input_join_sql_accepts_filtered_count_distinct_function_having() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(distinct s.score) filter (where s.score > 0) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id having count(distinct s.score) filter (where s.score > 0) > 1",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(plan.having.unwrap().output_column_id, "distinct_scores");
}

#[test]
fn two_input_join_sql_accepts_alias_having_on_filtered_count_distinct_output() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(distinct s.score) filter (where s.score > 0) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id having distinct_scores > 1",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(plan.having.unwrap().output_column_id, "distinct_scores");
}

#[test]
fn two_input_join_sql_rejects_having_count_distinct_left_non_value() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let error = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(distinct s.score) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id having count(distinct s.user_id) > 1",
        &[scores, accounts],
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn two_input_join_sql_accepts_nullable_left_value_count() {
    let mut scores = scores_catalog();
    let score = scores
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&scores.relation_schema)
        .expect("nullable score catalog should fingerprint");
    scores.schema_fingerprint = schema_fingerprint.clone();
    scores.incremental_relation.schema_fingerprint = schema_fingerprint;
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(s.score) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(
        plan.aggregate_outputs[1].function,
        LogicalPlanAggregateFunctionV1::Count
    );
    assert_eq!(
        plan.aggregate_outputs[1].input_column_id.as_deref(),
        Some("score")
    );
}

#[test]
fn two_input_join_sql_binds_having_nullable_left_value_count_function_to_projected_output() {
    let mut scores = scores_catalog();
    let score = scores
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&scores.relation_schema)
        .expect("nullable score catalog should fingerprint");
    scores.schema_fingerprint = schema_fingerprint.clone();
    scores.incremental_relation.schema_fingerprint = schema_fingerprint;
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(s.score) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id having count(s.score) > 1",
        &[scores, accounts],
    )
    .unwrap();

    let having = plan.having.unwrap();
    assert_eq!(having.output_column_id, "count");
    assert_eq!(having.op, PredicateOp::Gt);
    assert_eq!(having.literal, json!(1));
}

#[test]
fn two_input_join_sql_binds_having_right_count_functions_to_projected_outputs() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(a.limit) as limit_count, count(distinct a.limit) as distinct_limits from scores s join accounts a on s.user_id = a.account_id group by a.account_id having count(a.limit) > 1 and count(distinct a.limit) > 1",
        &[scores, accounts],
    )
    .unwrap();

    let having = plan.having_expr.unwrap();
    let predicates = having.leaf_predicates();
    assert!(predicates
        .iter()
        .any(|predicate| predicate.output_column_id == "limit_count"));
    assert!(predicates
        .iter()
        .any(|predicate| predicate.output_column_id == "distinct_limits"));
}

#[test]
fn two_input_join_sql_rejects_having_unprojected_right_count_distinct() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let error = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(distinct s.score) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id having count(distinct a.limit) > 1",
        &[scores, accounts],
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn two_input_join_sql_rejects_right_side_and_mismatched_value_aggregates() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count, avg(s.user_id) as avg_user from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let error = validate_supported_join_view_sql(sql, &[scores, accounts]).unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
}

#[test]
fn two_input_join_sql_rejects_cross_side_aggregate_expression_input() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let error = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score + a.limit) as adjusted_sum from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
    )
    .unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(
        error.to_string().contains(
            "JOIN aggregate input expressions must reference exactly one joined relation side"
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn two_input_join_sql_accepts_decimal_avg_as_float64_output() {
    let scores = scores_catalog();
    let accounts = accounts_decimal_limit_catalog();
    let output_schema = join_decimal_avg_output_schema();

    let plan = lower_supported_join_view_sql_to_logical_plan(
        "select a.account_id, sum(s.score) as sum, count(*) as count, avg(a.limit) as avg_limit from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan: supported } = &plan.execution
    else {
        panic!("expected join runtime execution");
    };
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Avg
            && aggregate.input_column_id.as_deref() == Some("limit")
            && aggregate.input_relation_side == Some(SupportedAggregateInputRelationSide::Right)
            && aggregate.output_column_id == "avg_limit"
    }));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn two_input_join_sql_accepts_filtered_count_distinct_left_value() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) filter (where s.score > 5) as sum, count(distinct s.score) filter (where s.score > 0) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(
        plan.aggregate_outputs[1].function,
        LogicalPlanAggregateFunctionV1::CountDistinct
    );
    assert_eq!(plan.aggregate_filter_exprs.len(), 2);
    assert!(plan.aggregate_filter_exprs.contains_key("sum"));
    assert!(plan.aggregate_filter_exprs.contains_key("distinct_scores"));
}

#[test]
fn two_input_join_sql_binds_order_by_sum_function_to_projected_output() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(s.score) desc limit 2",
        &[scores, accounts],
    )
    .unwrap();

    let top_k = plan.top_k.unwrap();
    assert_eq!(top_k.order_output_column_id, "sum");
    assert!(top_k.descending);
    assert_eq!(top_k.limit, 2);
}

#[test]
fn two_input_join_sql_binds_order_by_left_computed_sum_function_to_projected_output() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score + 1) as adjusted_sum from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(s.score + 1) desc limit 1",
        &[scores, accounts],
    )
    .unwrap();

    let top_k = plan.top_k.unwrap();
    assert_eq!(top_k.order_output_column_id, "adjusted_sum");
    assert!(top_k.descending);
    assert_eq!(top_k.limit, 1);
}

#[test]
fn two_input_join_sql_binds_order_by_right_computed_sum_function_to_projected_output() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, sum(a.limit + 1) as adjusted_limit_sum from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(a.limit + 1) desc limit 1",
        &[scores, accounts],
    )
    .unwrap();

    let top_k = plan.top_k.unwrap();
    assert_eq!(top_k.order_output_column_id, "adjusted_limit_sum");
    assert!(top_k.descending);
    assert_eq!(top_k.limit, 1);
}

#[test]
fn two_input_join_sql_binds_order_by_count_star_function_to_projected_output() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(*) desc limit 1",
        &[scores, accounts],
    )
    .unwrap();

    let top_k = plan.top_k.unwrap();
    assert_eq!(top_k.order_output_column_id, "count");
    assert!(top_k.descending);
    assert_eq!(top_k.limit, 1);
}

#[test]
fn two_input_join_sql_binds_order_by_nullable_count_function_to_projected_output() {
    let mut scores = scores_catalog();
    let score = scores
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&scores.relation_schema)
        .expect("nullable score catalog should fingerprint");
    scores.schema_fingerprint = schema_fingerprint.clone();
    scores.incremental_relation.schema_fingerprint = schema_fingerprint;
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(s.score) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(s.score) desc limit 1",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(
        plan.aggregate_outputs[1].input_column_id.as_deref(),
        Some("score")
    );
    assert_eq!(plan.top_k.unwrap().order_output_column_id, "count");
}

#[test]
fn two_input_join_sql_rejects_order_by_count_right_column_with_same_id_as_left_value() {
    let mut scores = scores_catalog();
    let score = scores
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&scores.relation_schema)
        .expect("nullable score catalog should fingerprint");
    scores.schema_fingerprint = schema_fingerprint.clone();
    scores.incremental_relation.schema_fingerprint = schema_fingerprint;

    let mut accounts = accounts_catalog();
    accounts.relation_schema.columns.push(RelationColumnV1 {
        column_id: "score".to_string(),
        name: "score".to_string(),
        logical_type: VelorixLogicalTypeV1::Int64,
        physical_arrow_type: ArrowPhysicalTypeV1::Int64,
        nullable: true,
        ordinal: 4,
        semantic_role: RelationSemanticRoleV1::Value,
    });
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&accounts.relation_schema)
        .expect("accounts score catalog should fingerprint");
    accounts.schema_fingerprint = schema_fingerprint.clone();
    accounts.incremental_relation.schema_fingerprint = schema_fingerprint;

    let error = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(s.score) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(a.score) desc limit 1",
        &[scores.clone(), accounts.clone()],
    )
    .unwrap_err();
    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));

    let error = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(a.score) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(s.score) desc limit 1",
        &[scores.clone(), accounts.clone()],
    )
    .unwrap_err();
    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(s.score) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(s.score) desc limit 1",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(plan.top_k.unwrap().order_output_column_id, "count");
}

#[test]
fn two_input_join_sql_binds_matching_filtered_order_by_sum_function() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) filter (where s.score > 0) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(s.score) filter (where s.score > 0) desc limit 2",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(plan.top_k.unwrap().order_output_column_id, "sum");
}

#[test]
fn two_input_join_sql_rejects_non_matching_filtered_order_by_sum_function() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let error = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) filter (where s.score > 0) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(s.score) filter (where s.score > 5) desc limit 2",
        &[scores, accounts],
    )
    .unwrap_err();

    assert!(error.to_string().contains(
        "materialized top-k ORDER BY aggregate function must reference one projected aggregate output"
    ));
}

#[test]
fn two_input_join_sql_binds_matching_filtered_order_by_count_function() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) filter (where s.score > 0) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(*) filter (where s.score > 0) desc limit 1",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(plan.top_k.unwrap().order_output_column_id, "count");
}

#[test]
fn two_input_join_sql_binds_matching_filtered_non_sum_order_by_functions() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    for (sql, output_column_id) in [
        (
            "select a.account_id, min(s.score) filter (where s.score > 0) as min_score from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by min(s.score) filter (where s.score > 0) asc limit 1",
            "min_score",
        ),
        (
            "select a.account_id, max(s.score) filter (where s.score > 0) as max_score from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by max(s.score) filter (where s.score > 0) desc limit 1",
            "max_score",
        ),
        (
            "select a.account_id, avg(s.score) filter (where s.score > 0) as avg_score from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by avg(s.score) filter (where s.score > 0) desc limit 1",
            "avg_score",
        ),
        (
            "select a.account_id, count(distinct s.score) filter (where s.score > 0) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(distinct s.score) filter (where s.score > 0) desc limit 1",
            "distinct_scores",
        ),
    ] {
        let plan =
            validate_supported_join_view_sql(sql, &[scores.clone(), accounts.clone()]).unwrap();

        assert_eq!(
            plan.top_k
                .as_ref()
                .map(|top_k| top_k.order_output_column_id.as_str()),
            Some(output_column_id),
            "SQL should bind filtered JOIN ORDER BY function to projected output: {sql}"
        );
    }
}

#[test]
fn two_input_join_sql_rejects_non_matching_filtered_order_by_count_function() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let error = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) filter (where s.score > 0) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(*) filter (where s.score > 5) desc limit 1",
        &[scores, accounts],
    )
    .unwrap_err();

    assert!(error.to_string().contains(
        "materialized top-k ORDER BY aggregate function must reference one projected aggregate output"
    ));
}

#[test]
fn two_input_join_sql_accepts_alias_order_by_filtered_sum_output() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) filter (where s.score > 0) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum desc limit 1",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(plan.top_k.unwrap().order_output_column_id, "sum");
}

#[test]
fn two_input_join_sql_rejects_order_by_computed_sum_function_wrong_input() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    for sql in [
        "select a.account_id, sum(s.score + 1) as adjusted_sum from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(s.score + a.limit) desc limit 1",
        "select a.account_id, sum(s.score + 1) as adjusted_sum from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(s.score + 2) desc limit 1",
        "select a.account_id, sum(s.score + 1) as adjusted_sum, sum(s.score + 1) as duplicate_adjusted_sum from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(s.score + 1) desc limit 1",
    ] {
        let error = validate_supported_join_view_sql(sql, &[scores.clone(), accounts.clone()])
            .unwrap_err();

        assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    }
}

#[test]
fn two_input_join_sql_rejects_filtered_order_by_computed_sum_function() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    for sql in [
        "select a.account_id, sum(s.score + 1) filter (where s.score > 0) as adjusted_sum from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(s.score + 1) desc limit 1",
        "select a.account_id, sum(s.score + 1) as adjusted_sum from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(s.score + 1) filter (where s.score > 0) desc limit 1",
    ] {
        let error = validate_supported_join_view_sql(sql, &[scores.clone(), accounts.clone()])
            .unwrap_err();

        assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    }
}

#[test]
fn two_input_join_sql_rejects_order_by_sum_function_wrong_input() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    for sql in [
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(a.limit) desc limit 2",
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(s.user_id) desc limit 2",
    ] {
        let error = validate_supported_join_view_sql(sql, &[scores.clone(), accounts.clone()])
            .unwrap_err();

        assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    }
}

#[test]
fn two_input_join_sql_rejects_order_by_sum_function_distinct_or_expression_input() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();

    for sql in [
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(distinct s.score) desc limit 2",
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(abs(s.score)) desc limit 2",
    ] {
        let error = validate_supported_join_view_sql(sql, &[scores.clone(), accounts.clone()])
            .unwrap_err();

        assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    }
}

#[test]
fn two_input_join_sql_binds_order_by_right_count_functions_to_projected_outputs() {
    let accounts = accounts_catalog();
    let scores = scores_catalog();

    for (sql, output_column_id) in [
        (
            "select a.account_id, sum(s.score) as sum, count(a.limit) as limit_count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(a.limit) desc limit 1",
            "limit_count",
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(distinct a.limit) as distinct_limits from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(distinct a.limit) desc limit 1",
            "distinct_limits",
        ),
    ] {
        let plan =
            validate_supported_join_view_sql(sql, &[scores.clone(), accounts.clone()]).unwrap();

        assert_eq!(
            plan.top_k
                .as_ref()
                .map(|top_k| top_k.order_output_column_id.as_str()),
            Some(output_column_id)
        );
    }
}

#[test]
fn two_input_join_sql_binds_order_by_count_literal_to_projected_count_output() {
    let accounts = accounts_catalog();
    let scores = scores_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(1) desc limit 1",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(plan.top_k.unwrap().order_output_column_id, "count");
}

#[test]
fn two_input_join_sql_binds_order_by_count_distinct_left_value_function_to_projected_output() {
    let accounts = accounts_catalog();
    let scores = scores_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(distinct s.score) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(distinct s.score) desc limit 1",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(
        plan.top_k
            .as_ref()
            .map(|top_k| top_k.order_output_column_id.as_str()),
        Some("distinct_scores")
    );
}

#[test]
fn two_input_join_sql_binds_order_by_non_null_count_function_to_projected_output() {
    let accounts = accounts_catalog();
    let scores = scores_catalog();

    let plan = validate_supported_join_view_sql(
        "select a.account_id, sum(s.score) as sum, count(s.score) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(s.score) desc limit 1",
        &[scores, accounts],
    )
    .unwrap();

    assert_eq!(
        plan.top_k
            .as_ref()
            .map(|top_k| top_k.order_output_column_id.as_str()),
        Some("count")
    );
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
fn single_key_sum_count_fixture_sql_accepts_relation_alias_qualified_columns() {
    let catalog = scores_catalog();

    let plan = validate_supported_view_sql(
        "select s.user_id as user, sum(s.score) as sum, count(1) as count from scores as s where s.score > 0 group by s.user_id having sum(s.score) > 0 and count(1) > 0",
        &catalog,
    )
    .unwrap();

    assert_eq!(plan.input_relation_id, "scores");
    assert_eq!(plan.group_key_column_id, "user_id");
    assert_eq!(plan.output_key_column_id, "user");
    assert_eq!(plan.sum_value_column_id, "score");
    assert_eq!(
        plan.predicate_expr
            .as_ref()
            .expect("qualified WHERE should be admitted")
            .leaf_predicates()
            .len(),
        1
    );
    assert_eq!(
        plan.having_expr
            .as_ref()
            .expect("qualified HAVING aggregate should be admitted")
            .leaf_predicates()
            .len(),
        2
    );
}

#[test]
fn single_key_aggregate_sql_accepts_trailing_order_by_without_changing_materialization() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id order by user_id",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::SingleKeySumCount { .. }
    ));
}

#[test]
fn single_key_aggregate_sql_accepts_explicit_identity_projection_cte() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "with base as (select user_id, score, delta from scores) select user_id, sum(score) as sum, count(*) as count from base group by user_id",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::SingleKeySumCount { .. }
    ));
}

#[test]
fn single_key_aggregate_sql_accepts_partial_projection_cte() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "with base as (select user_id, score from scores) select user_id, sum(score) as sum, count(*) as count from base group by user_id",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::SingleKeySumCount { .. }
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_partial_projection_derived_source() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from (select user_id, score from scores where score > 0) base group by user_id",
        std::slice::from_ref(&catalog),
        &output_schema,
    )
    .unwrap();

    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::SingleKeySumCount { .. }
    ));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn single_key_aggregate_sql_accepts_source_projection_aliases() {
    let catalog = scores_catalog();
    let mut output_schema = scores_output_schema();
    output_schema.columns[0].name = "customer".to_string();
    output_schema.primary_key = vec!["customer".to_string()];

    let cases = [
        "with base as (select user_id as customer, score as points from scores) select customer, sum(points) as sum, count(*) as count from base group by customer",
        "select customer, sum(points) as sum, count(*) as count from (select user_id as customer, score as points from scores where score > 0) base group by customer",
    ];

    for sql in cases {
        let plan = lower_supported_sql_to_logical_plan(
            sql,
            std::slice::from_ref(&catalog),
            &output_schema,
        )
        .unwrap();

        let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
        else {
            panic!("expected single-key runtime execution for SQL `{sql}`");
        };
        assert_eq!(supported.group_key_column_id, "user_id");
        assert_eq!(supported.output_key_column_id, "customer");
        assert_eq!(supported.sum_value_column_id, "score");
        assert!(supported.aggregate_outputs.iter().any(|aggregate| {
            aggregate.function == LogicalPlanAggregateFunctionV1::Sum
                && aggregate.input_column_id.as_deref() == Some("score")
                && aggregate.output_column_id == "sum"
        }));
        validate_logical_view_plan(&plan).unwrap();
    }
}

#[test]
fn single_key_aggregate_sql_rejects_source_projection_missing_required_columns() {
    let catalog = scores_catalog();

    let cases = [
        (
            "with base as (select score from scores) select user_id, sum(score) as sum, count(*) as count from base group by user_id",
            "aggregate source projection must include the group key column",
        ),
        (
            "with base as (select user_id from scores) select user_id, sum(score) as sum, count(*) as count from base group by user_id",
            "aggregate source projection must include aggregate input columns",
        ),
        (
            "select user_id, sum(score) as sum, count(*) as count from (select user_id from scores) base group by user_id",
            "aggregate source projection must include aggregate input columns",
        ),
    ];

    for (sql, expected_reason) in cases {
        let error = validate_supported_view_sql(sql, &catalog).unwrap_err();

        assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
        assert!(
            error.to_string().contains(expected_reason),
            "expected `{expected_reason}` in `{error}` for SQL: {sql}"
        );
    }
}

#[test]
fn single_key_aggregate_sql_rejects_source_projection_alias_edges() {
    let catalog = scores_catalog();

    let cases = [
        (
            "with base as (select user_id as customer, score + 1 as points from scores) select customer, sum(points) as sum, count(*) as count from base group by customer",
            "source projection aliases must map directly to registered columns",
        ),
        (
            "with base as (select user_id as customer, score as customer from scores) select customer, sum(score) as sum, count(*) as count from base group by customer",
            "source projection output names must be unique",
        ),
        (
            "with base as (select user_id as score, score from scores) select score, sum(score) as sum, count(*) as count from base group by score",
            "source projection aliases must not shadow another registered column",
        ),
        (
            "with base as (select user_id as customer from scores) select customer, sum(score) as sum, count(*) as count from base group by customer",
            "aggregate source projection must include aggregate input columns",
        ),
    ];

    for (sql, expected_reason) in cases {
        let error = validate_supported_view_sql(sql, &catalog).unwrap_err();

        assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
        assert!(
            error.to_string().contains(expected_reason),
            "expected `{expected_reason}` in `{error}` for SQL: {sql}"
        );
    }
}

#[test]
fn single_key_aggregate_sql_rejects_non_direct_source_projection_columns() {
    let catalog = scores_catalog();

    let cases = [
        "with base as (select user_id as id, score from scores) select user_id, sum(score) as sum, count(*) as count from base group by user_id",
        "with base as (select user_id, score + 1 from scores) select user_id, sum(score) as sum, count(*) as count from base group by user_id",
        "with base as (select user_id, score, score from scores) select user_id, sum(score) as sum, count(*) as count from base group by user_id",
    ];

    for sql in cases {
        let error = validate_supported_view_sql(sql, &catalog).unwrap_err();

        assert!(
            matches!(error, ViewPlanError::UnsupportedShape { .. }),
            "expected unsupported admission for SQL `{sql}`, got `{error}`"
        );
    }
}

#[test]
fn single_key_aggregate_sql_accepts_group_by_projected_key_alias() {
    let catalog = scores_catalog();
    let mut output_schema = scores_output_schema();
    output_schema.columns[0].name = "customer".to_string();
    output_schema.primary_key = vec!["customer".to_string()];

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id as customer, sum(score) as sum, count(*) as count from scores group by customer",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key aggregate execution");
    };
    assert_eq!(supported.group_key_column_id, "user_id");
    assert_eq!(supported.output_key_column_id, "customer");
}

#[test]
fn single_key_aggregate_sql_accepts_group_by_first_projection_ordinal() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(*) as count from scores group by 1",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key aggregate execution");
    };
    assert_eq!(supported.group_key_column_id, "user_id");
    assert_eq!(supported.output_key_column_id, "user_id");
}

#[test]
fn single_key_aggregate_sql_accepts_mixed_nullable_column_count() {
    let mut catalog = scores_catalog();
    let score = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable score catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    let output_schema = scores_output_schema();

    let plan = lower_supported_view_sql_to_logical_plan(
        "select user_id, sum(score) as sum, count(score) as count from scores group by user_id",
        &catalog,
        &output_schema,
    )
    .unwrap();

    let VelorixLogicalViewExecutionV1::SingleKeySumCount { plan: supported } = &plan.execution
    else {
        panic!("expected single-key runtime execution");
    };
    assert_eq!(supported.aggregate_outputs.len(), 2);
    assert!(supported.aggregate_outputs.iter().any(|aggregate| {
        aggregate.function == LogicalPlanAggregateFunctionV1::Count
            && aggregate.input_column_id.as_deref() == Some("score")
    }));
    validate_logical_view_plan(&plan).unwrap();
}

#[test]
fn unsupported_single_input_sql_families_fail_closed_without_logical_plan_fallback() {
    let catalog = scores_catalog();
    let output_schema = scores_output_schema();
    let cases = [
        "select user_id, row_number() over (partition by user_id order by score) as sum, count(*) as count from scores group by user_id",
        "select user_id, sum(score) over (partition by user_id) as sum, count(*) as count from scores group by user_id",
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id order by sum desc, count asc limit 1",
        "select user_id, sum(score) as sum, count(*) as count from scores group by all with rollup",
        "select user_id, sum(score) as sum, count(*) as count from scores group by rollup(user_id)",
        "select user_id, sum(score) as sum, count(*) as count from scores group by cube(user_id)",
        "select user_id, sum(score) as sum, count(*) as count from scores group by grouping sets ((user_id))",
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
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s full join accounts a on s.user_id = a.account_id group by a.account_id",
            "FULL JOIN first projection must be COALESCE(left_key, right_key)",
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s cross join accounts a group by a.account_id",
            "only INNER or narrow LEFT/RIGHT JOIN",
        ),
        (
            "select user_id, sum(score) as sum, count(*) as count from scores natural join accounts group by user_id",
            "JOIN must use one ON equality predicate or USING column",
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id <> a.account_id group by a.account_id",
            "JOIN ON must contain exactly one key equality",
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

    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let device_status = device_status_catalog();
    let catalogs = [scores, accounts, device_status];
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id join device_status d on d.device_id = a.account_id group by a.account_id";
    let error = lower_supported_sql_to_logical_plan(sql, &catalogs, &output_schema).unwrap_err();

    assert!(matches!(error, ViewPlanError::UnsupportedShape { .. }));
    assert!(
        error
            .to_string()
            .contains("three-input JOIN requires a composite primary key on every input"),
        "expected unsupported three-table join to fail closed in bounded admission for SQL `{sql}`, got `{error}`"
    );
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

fn scores_with_category_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = scores_catalog();
    catalog.relation_schema.columns[2].ordinal = 3;
    catalog.relation_schema.columns.insert(
        2,
        RelationColumnV1 {
            column_id: "category".to_string(),
            name: "category".to_string(),
            logical_type: VelorixLogicalTypeV1::Utf8,
            physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
            nullable: true,
            ordinal: 2,
            semantic_role: RelationSemanticRoleV1::Metadata,
        },
    );
    let schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog.incremental_adapter.adapter_id = CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string();
    catalog
}

fn scores_with_adjustment_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = scores_catalog();
    catalog.relation_schema.columns.insert(
        2,
        RelationColumnV1 {
            column_id: "user_id_adjustment".to_string(),
            name: "user_id_adjustment".to_string(),
            logical_type: VelorixLogicalTypeV1::Int64,
            physical_arrow_type: ArrowPhysicalTypeV1::Int64,
            nullable: false,
            ordinal: 2,
            semantic_role: RelationSemanticRoleV1::Value,
        },
    );
    for (ordinal, column) in catalog.relation_schema.columns.iter_mut().enumerate() {
        column.ordinal = ordinal as u32;
    }
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("adjusted scores catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
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
                column_id: "tier".to_string(),
                name: "tier".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Metadata,
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

fn accounts_decimal_limit_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = accounts_catalog();
    let limit = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "limit")
        .unwrap();
    limit.logical_type = VelorixLogicalTypeV1::Decimal {
        precision: 12,
        scale: 2,
    };
    limit.physical_arrow_type = ArrowPhysicalTypeV1::Decimal128 {
        precision: 12,
        scale: 2,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("decimal account catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
}

fn generic_adapter_catalog(mut catalog: VelorixRelationCatalogV1) -> VelorixRelationCatalogV1 {
    catalog.incremental_adapter.adapter_id = CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string();
    catalog
}

fn accounts_multi_value_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = accounts_catalog();
    let quota = RelationColumnV1 {
        column_id: "quota".to_string(),
        name: "quota".to_string(),
        logical_type: VelorixLogicalTypeV1::Int64,
        physical_arrow_type: ArrowPhysicalTypeV1::Int64,
        nullable: false,
        ordinal: 3,
        semantic_role: RelationSemanticRoleV1::Value,
    };
    catalog.relation_schema.columns.insert(2, quota);
    for (ordinal, column) in catalog.relation_schema.columns.iter_mut().enumerate() {
        column.ordinal = ordinal as u32;
    }
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("multi-value accounts catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
}

fn accounts_catalog_with_user_id_key() -> VelorixRelationCatalogV1 {
    let mut catalog = accounts_catalog();
    let key = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "account_id")
        .unwrap();
    key.column_id = "user_id".to_string();
    key.name = "user_id".to_string();
    catalog.relation_schema.primary_key_column_ids = vec!["user_id".to_string()];
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("accounts user_id key catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
}

fn composite_join_catalogs() -> (VelorixRelationCatalogV1, VelorixRelationCatalogV1) {
    let add_tenant_key = |mut catalog: VelorixRelationCatalogV1| {
        let weight_index = catalog
            .relation_schema
            .columns
            .iter()
            .position(|column| column.column_id == catalog.relation_schema.weight_column_id)
            .unwrap();
        catalog.relation_schema.columns.insert(
            weight_index,
            RelationColumnV1 {
                column_id: "tenant_id".into(),
                name: "tenant_id".into(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
        );
        for (ordinal, column) in catalog.relation_schema.columns.iter_mut().enumerate() {
            column.ordinal = ordinal as u32;
        }
        catalog
            .relation_schema
            .primary_key_column_ids
            .insert(0, "tenant_id".into());
        let fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
            .expect("composite join catalog should fingerprint");
        catalog.schema_fingerprint = fingerprint.clone();
        catalog.incremental_relation.schema_fingerprint = fingerprint;
        generic_adapter_catalog(catalog)
    };
    let scores = add_tenant_key(scores_catalog());
    let mut accounts = add_tenant_key(accounts_catalog());
    let tenant = accounts
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "tenant_id")
        .unwrap();
    tenant.column_id = "account_tenant_id".into();
    tenant.name = "account_tenant_id".into();
    accounts.relation_schema.primary_key_column_ids =
        vec!["account_id".into(), "account_tenant_id".into()];
    let fingerprint = SchemaFingerprintV1::for_relation_schema(&accounts.relation_schema)
        .expect("renamed composite join catalog should fingerprint");
    accounts.schema_fingerprint = fingerprint.clone();
    accounts.incremental_relation.schema_fingerprint = fingerprint;
    (scores, accounts)
}

fn three_input_composite_join_catalogs() -> Vec<VelorixRelationCatalogV1> {
    let (scores, accounts) = composite_join_catalogs();
    let mut profiles = accounts.clone();
    profiles.relation_schema.relation_id = "profiles".into();
    profiles.relation_schema.relation_name = "profiles".into();
    profiles.datafusion_registration.name = "profiles".into();
    profiles.incremental_relation.relation_id = "profiles".into();
    let fingerprint = SchemaFingerprintV1::for_relation_schema(&profiles.relation_schema)
        .expect("profile composite catalog should fingerprint");
    profiles.schema_fingerprint = fingerprint.clone();
    profiles.incremental_relation.schema_fingerprint = fingerprint;
    vec![scores, accounts, profiles]
}

fn composite_join_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "tenant_scores".into(),
        relation_name: "tenant_scores".into(),
        relation_version: "2026-08-10.v1".into(),
        schema_fingerprint:
            "sha256:00000000000000000000000000000000000000000000000000000000000000c2".into(),
        columns: vec![
            ColumnSchema {
                name: "tenant_id".into(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".into(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "count".into(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["tenant_id".into()],
    }
}

fn three_input_join_count_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "three_input_counts".into(),
        relation_name: "three_input_counts".into(),
        relation_version: "2026-08-10.v1".into(),
        schema_fingerprint:
            "sha256:00000000000000000000000000000000000000000000000000000000000000c5".into(),
        columns: vec![
            ColumnSchema {
                name: "tenant_id".into(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "user_id".into(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "count".into(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["tenant_id".into(), "user_id".into()],
    }
}

fn non_primary_join_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_bucket".into(),
        relation_name: "scores_by_bucket".into(),
        relation_version: "2026-08-10.v1".into(),
        schema_fingerprint:
            "sha256:00000000000000000000000000000000000000000000000000000000000000c3".into(),
        columns: vec![
            ColumnSchema {
                name: "bucket".into(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".into(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "count".into(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["bucket".into()],
    }
}

fn self_join_count_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "self_join_count".into(),
        relation_name: "self_join_count".into(),
        relation_version: "2026-08-10.v1".into(),
        schema_fingerprint:
            "sha256:00000000000000000000000000000000000000000000000000000000000000c4".into(),
        columns: vec![ColumnSchema {
            name: "count".into(),
            data_type: SqlDataType::Int64,
            nullable: false,
        }],
        primary_key: Vec::new(),
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

// ============================================================================
// Phase 6: String Expression Tests
// ============================================================================

#[test]
fn string_length_ascii_semantics_are_stable() {
    let catalogs = vec![string_test_catalog()];
    let output = string_test_output_schema();
    let result = lower_supported_sql_to_logical_plan(
        "select user_id, length(name) as name_length from string_test",
        &catalogs,
        &output,
    );
    let plan = result.unwrap();
    match &plan.execution {
        VelorixLogicalViewExecutionV1::FilterProject { plan } => {
            assert_eq!(plan.value_columns.len(), 1);
            let value_col = &plan.value_columns[0];
            assert_eq!(value_col.output_column_id, "name_length");
            assert!(value_col.expression.is_some());
        }
        _ => panic!("expected FilterProject plan"),
    }
}

#[test]
fn string_concat_basic_concatenation() {
    let catalogs = vec![string_test_catalog()];
    let output = string_test_output_schema();
    let result = lower_supported_sql_to_logical_plan(
        "select user_id, concat(name, '_suffix') as concatenated from string_test",
        &catalogs,
        &output,
    );
    let plan = result.unwrap();
    match &plan.execution {
        VelorixLogicalViewExecutionV1::FilterProject { plan } => {
            assert_eq!(plan.value_columns.len(), 1);
            let value_col = &plan.value_columns[0];
            assert_eq!(value_col.output_column_id, "concatenated");
            assert!(value_col.expression.is_some());
        }
        _ => panic!("expected FilterProject plan"),
    }
}

#[test]
fn string_substring_extracts_correct_range() {
    let catalogs = vec![string_test_catalog()];
    let output = string_test_output_schema();
    let result = lower_supported_sql_to_logical_plan(
        "select user_id, substring(name, 1, 3) as sub from string_test",
        &catalogs,
        &output,
    );
    let plan = result.unwrap();
    match &plan.execution {
        VelorixLogicalViewExecutionV1::FilterProject { plan } => {
            assert_eq!(plan.value_columns.len(), 1);
            let value_col = &plan.value_columns[0];
            assert_eq!(value_col.output_column_id, "sub");
            assert!(value_col.expression.is_some());
        }
        _ => panic!("expected FilterProject plan"),
    }
}

#[test]
fn string_trim_removes_whitespace() {
    let catalogs = vec![string_test_catalog()];
    let output = string_test_output_schema();
    let result = lower_supported_sql_to_logical_plan(
        "select user_id, trim(name) as trimmed from string_test",
        &catalogs,
        &output,
    );
    let plan = result.unwrap();
    match &plan.execution {
        VelorixLogicalViewExecutionV1::FilterProject { plan } => {
            assert_eq!(plan.value_columns.len(), 1);
            let value_col = &plan.value_columns[0];
            assert_eq!(value_col.output_column_id, "trimmed");
            assert!(value_col.expression.is_some());
        }
        _ => panic!("expected FilterProject plan"),
    }
}

#[test]
fn string_expression_rejection_of_invalid_types() {
    let catalogs = vec![string_test_catalog()];
    let output = string_test_output_schema();
    let result = lower_supported_sql_to_logical_plan(
        "select user_id, length(name) as len, count(*) as cnt from string_test group by user_id",
        &catalogs,
        &output,
    );
    let _ = result;
}

fn string_test_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "string_test".to_string(),
        relation_name: "string_test".to_string(),
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
                column_id: "name".to_string(),
                name: "name".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
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
            name: "string_test".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "string_test".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn string_test_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "string_test_output".to_string(),
        relation_name: "string_test_output".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: "sha256:string-test-output".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "output_value".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: true,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

// ============================================================================
// Phase 4: Published-View Single-Key Sum/Count Admission
// ============================================================================

#[test]
fn published_single_key_sum_count_lowers_to_single_key_plan() {
    let input = PlannerRelationInput::from_published_binding(
        published_regions_relation(),
        "velorix-published-relation-delta-v1".to_string(),
        "producer_commit_epoch".to_string(),
    );
    let output = RelationSchema {
        relation_id: "regions_total".to_string(),
        relation_name: "regions_total".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "9".repeat(64)),
        columns: vec![
            ColumnSchema {
                name: "region".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "total".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["region".to_string()],
    };
    let plan = lower_published_single_key_sum_count_sql(
        "select region, sum(amount) as total, count(*) as count from regions_by_region group by region",
        &input,
        &output,
        "velorix-published-relation-delta-v1",
        "producer_commit_epoch",
    )
    .unwrap();
    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::SingleKeySumCount { .. }
    ));
}

#[test]
fn published_single_key_sum_count_rejects_mismatched_codec() {
    let input = PlannerRelationInput::from_published_binding(
        published_regions_relation(),
        "velorix-published-relation-delta-v1".to_string(),
        "producer_commit_epoch".to_string(),
    );
    let output = RelationSchema {
        relation_id: "regions_total".to_string(),
        relation_name: "regions_total".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "9".repeat(64)),
        columns: vec![
            ColumnSchema {
                name: "region".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "total".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["region".to_string()],
    };
    // Stale generation codec must be rejected before planning.
    let result = lower_published_single_key_sum_count_sql(
        "select region, sum(amount) as total from regions_by_region group by region",
        &input,
        &output,
        "stale-codec",
        "producer_commit_epoch",
    );
    assert!(matches!(
        result,
        Err(ViewPlanError::UnsupportedShape { .. })
    ));
}

#[test]
fn published_single_key_sum_count_rejects_window_sql() {
    let input = PlannerRelationInput::from_published_binding(
        published_regions_relation(),
        "velorix-published-relation-delta-v1".to_string(),
        "producer_commit_epoch".to_string(),
    );
    let output = RelationSchema {
        relation_id: "regions_total".to_string(),
        relation_name: "regions_total".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "9".repeat(64)),
        columns: vec![ColumnSchema {
            name: "region".to_string(),
            data_type: SqlDataType::Utf8,
            nullable: false,
        }],
        primary_key: vec!["region".to_string()],
    };
    let result = lower_published_single_key_sum_count_sql(
        "select region, row_number() over (partition by region order by region) as rn from regions_by_region",
        &input,
        &output,
        "velorix-published-relation-delta-v1",
        "producer_commit_epoch",
    );
    assert!(matches!(
        result,
        Err(ViewPlanError::UnsupportedShape { .. })
    ));
}

fn published_regions_relation() -> RelationSchema {
    RelationSchema {
        relation_id: "regions_by_region".to_string(),
        relation_name: "regions_by_region".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "7".repeat(64)),
        columns: vec![
            ColumnSchema {
                name: "region".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "amount".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["region".to_string()],
    }
}
