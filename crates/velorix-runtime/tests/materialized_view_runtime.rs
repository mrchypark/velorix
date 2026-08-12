use std::sync::Arc;

use arrow::{
    array::{Array, BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use serde_json::{json, Value};
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    native_operator::{
        NativeAggregateOperator, NativeBinaryJoinOperator, NativeLeftJoinOperator,
        NativeOperatorEdgeV1, NativeOperatorGraph, NativeOperatorGraphCheckpointV1,
        NativeOperatorInputV1, NativeOperatorStateV1,
    },
    operator::AggregateValueMode,
    operator_contract::ChangelogModeV1,
    relation::{
        arrow_record_batches_to_key_value_delta_batch, ArrowPhysicalTypeV1,
        DataFusionRegistrationModeV1, DataFusionRegistrationV1, IncrementalAdapterBindingV1,
        IncrementalRelationBindingV1, RelationColumnV1, RelationOperationV1,
        RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1, VelorixRelationSourceV1,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
    standing_program::{
        BuiltinRuntimeIdentity, EpochIdempotencyKey, InputEventTimeWatermark, NativeCodePolicy,
        RelationFrontier, RelationInputBatch, RelationInputEncodingV1, ScopedViewId,
        SnapshotPageRequest, StandingProgramIdentity, StandingProgramRuntime,
        StandingProgramRuntimeError,
    },
    view_contract::{
        catalog_from_published_relation_binding, catalog_input_relation_schema,
        published_relation_binding_v1, stable_bytes_hash, ColumnSchema, RelationSchema,
        SqlDataType, PUBLISHED_DELTA_WEIGHT_FIELD_V1, PUBLISHED_RELATION_DELTA_CODEC_V1,
    },
    view_plan::{
        logical_view_plan_hash, lower_supported_analytic_row_number_sql_to_logical_plan,
        lower_supported_filter_project_sql_to_logical_plan,
        lower_supported_join_view_sql_to_logical_plan,
        lower_supported_three_input_inner_join_count_sql_to_logical_plan_with_policy,
        lower_supported_tumbling_window_sql_to_logical_plan_with_policy, LateRowPolicy,
        LogicalPlanAggregateFunctionV1, RowPredicateExpr, SupportedAggregateOutput,
        SupportedProjectionExpr, VelorixLogicalViewExecutionV1, VelorixLogicalViewPlanV1,
        COMPOSITE_PK_POSITIONAL_JSON_ARRAY_JOIN_KEY_CODEC_V1,
        THREE_INPUT_LEGACY_SQL_ENCOUNTER_JOIN_ORDER_V1,
    },
};
use velorix_runtime::materialized_view_runtime::{
    advance_input_event_time_frontier, bind_join_execution_v1, combine_multi_input_watermarks,
    create_common_dag_reference_standing_runtime_with_logical_plan_and_catalogs,
    create_standing_runtime, create_standing_runtime_with_logical_plan_and_catalogs,
    create_standing_runtime_with_sql_and_catalogs, materialized_delta_to_page,
    restore_common_dag_reference_standing_runtime, restore_standing_runtime, JoinExecutionModeV1,
    JoinSpecializationComparisonGraph, TwoInputJoinRuntime, CRATE_NAME,
};

#[test]
fn public_exists_and_not_exists_materialize_through_restart_with_duplicate_matches() {
    for (label, negated) in [("semi", false), ("anti", true)] {
        let scores = scores_catalog();
        let accounts = accounts_catalog();
        let catalogs = vec![scores.clone(), accounts.clone()];
        let input_schemas = catalogs
            .iter()
            .map(|catalog| catalog_input_relation_schema(catalog).unwrap())
            .collect::<Vec<_>>();
        let output = scores_projection_output_schema();
        let sql = format!(
            "select s.user_id, s.score from scores s where {}exists (select 1 from accounts a where a.account_id = s.user_id)",
            if negated { "not " } else { "" }
        );
        let identity = standing_identity_with_view(&sql, "positive_scores");
        let mut runtime = create_standing_runtime_with_sql_and_catalogs(
            &identity,
            &catalogs,
            &sql,
            &input_schemas,
            std::slice::from_ref(&output),
        )
        .unwrap();

        runtime
            .apply_changes(
                1,
                EpochIdempotencyKey::new(format!("{label}-epoch-1")).unwrap(),
                vec![
                    relation_input(
                        &scores,
                        &format!("{label}-scores"),
                        0,
                        2,
                        scores_rows_batch(&[("alice", 10, 1), ("bob", 5, 1)]),
                    ),
                    relation_input(
                        &accounts,
                        &format!("{label}-accounts"),
                        0,
                        1,
                        accounts_rows_batch(&[("alice", 100, "gold", 1)]),
                    ),
                ],
            )
            .unwrap();
        assert_projected_scores_page(
            runtime.as_ref(),
            1,
            if negated {
                &[("bob", 5)]
            } else {
                &[("alice", 10)]
            },
        );

        let checkpoint = runtime.checkpoint().unwrap();
        let payload = checkpoint.state_payload.as_ref().unwrap().payload.as_str();
        assert!(payload.contains(if negated {
            "velorix-native-anti-join-v1"
        } else {
            "velorix-native-semi-join-v1"
        }));
        let mut restored = restore_standing_runtime(checkpoint).unwrap();
        restored
            .apply_changes(
                2,
                EpochIdempotencyKey::new(format!("{label}-epoch-2")).unwrap(),
                vec![relation_input(
                    &accounts,
                    &format!("{label}-accounts"),
                    1,
                    4,
                    accounts_rows_batch(&[
                        ("alice", 100, "gold", -1),
                        ("bob", 50, "gold", 1),
                        ("bob", 75, "silver", 1),
                    ]),
                )],
            )
            .unwrap();
        assert_projected_scores_page(
            restored.as_ref(),
            2,
            if negated {
                &[("alice", 10)]
            } else {
                &[("bob", 5)]
            },
        );

        restored
            .apply_changes(
                3,
                EpochIdempotencyKey::new(format!("{label}-epoch-3")).unwrap(),
                vec![
                    relation_input(
                        &scores,
                        &format!("{label}-scores"),
                        2,
                        4,
                        scores_rows_batch(&[("alice", 10, -1), ("alice", 12, 1)]),
                    ),
                    relation_input(
                        &accounts,
                        &format!("{label}-accounts"),
                        4,
                        5,
                        accounts_rows_batch(&[("bob", 50, "gold", -1)]),
                    ),
                ],
            )
            .unwrap();
        assert_projected_scores_page(
            restored.as_ref(),
            3,
            if negated {
                &[("alice", 12)]
            } else {
                &[("bob", 5)]
            },
        );

        let mut restored = restore_standing_runtime(restored.checkpoint().unwrap()).unwrap();
        restored
            .apply_changes(
                4,
                EpochIdempotencyKey::new(format!("{label}-epoch-4")).unwrap(),
                vec![relation_input(
                    &accounts,
                    &format!("{label}-accounts"),
                    5,
                    6,
                    accounts_rows_batch(&[("bob", 75, "silver", -1)]),
                )],
            )
            .unwrap();
        assert_projected_scores_page(
            restored.as_ref(),
            4,
            if negated {
                &[("alice", 12), ("bob", 5)]
            } else {
                &[]
            },
        );
    }
}

#[test]
fn runtime_materializes_global_count_empty_input_and_final_retract_across_restore() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = global_count_output_schema();
    let sql = "select count(*) as count from scores";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let empty = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: identity.tenant_id.clone(),
                program_id: identity.program_id.clone(),
                view_id: identity.view_ids[0].clone(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(0),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    assert_global_count_batch(&empty.batches[0], 0);

    let inserted = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("global-count-epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "global-count-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", 5, 1),
                    ("alice", 7, 1),
                ])],
            }],
        )
        .unwrap();
    assert_global_count_batch(&inserted.output_batches[0].batches[0], 3);

    let checkpoint = runtime.checkpoint().unwrap();
    let payload = checkpoint
        .state_payload
        .as_ref()
        .expect("global aggregate checkpoint payload")
        .payload
        .as_str();
    assert!(payload.contains("aggregate_singleton_state"));
    assert!(payload.contains("aggregate_singleton_publication"));
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    let deleted = restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("global-count-epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "global-count-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 6,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, -1),
                    ("bob", 5, -1),
                    ("alice", 7, -1),
                ])],
            }],
        )
        .unwrap();
    assert_global_count_batch(&deleted.output_batches[0].batches[0], 0);
}

#[test]
fn runtime_materializes_composite_computed_group_keys() {
    let catalog = scores_with_category_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = composite_bucket_output_schema();
    let sql = "select user_id, score / 10 as bucket, sum(score) as sum, count(*) as count from scores group by user_id, bucket";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let commit = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("composite-group-epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "composite-group-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_with_category_batch()],
            }],
        )
        .unwrap();

    let batch = &commit.output_batches[0].batches[0];
    assert_eq!(batch.num_rows(), 2);
    let users = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let buckets = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sums = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        users.iter().collect::<Vec<_>>(),
        vec![Some("u1"), Some("u1")]
    );
    assert_eq!(buckets.values(), &[0, 1]);
    assert_eq!(sums.values(), &[12, 15]);
    assert_eq!(counts.values(), &[2, 1]);
}

#[test]
fn runtime_materializes_registered_composite_group_keys_with_null() {
    let catalog = scores_with_category_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = composite_category_output_schema();
    let sql = "select user_id, category, sum(score) as sum, count(*) as count from scores group by user_id, category";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();
    let commit = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("registered-composite-group-epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "registered-composite-group-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_with_category_batch()],
            }],
        )
        .unwrap();

    let batch = &commit.output_batches[0].batches[0];
    let categories = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut rows = (0..batch.num_rows())
        .map(|index| {
            (
                (!categories.is_null(index)).then(|| categories.value(index).to_string()),
                sums.value(index),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(rows, vec![(None, 15), (Some("a".to_string()), 12)]);

    let checkpoint = runtime.checkpoint().unwrap();
    assert!(checkpoint
        .state_payload
        .as_ref()
        .unwrap()
        .payload
        .contains("\"category\":null"));
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    let retracted = restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("registered-composite-group-epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "registered-composite-group-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_with_category_rows_batch(&[("u1", 15, None, -1)])],
            }],
        )
        .unwrap();
    let batch = &retracted.output_batches[0].batches[0];
    assert_eq!(batch.num_rows(), 1);
    let categories = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(categories.value(0), "a");
    let removed_group = retracted.output_deltas[0]
        .delta
        .records()
        .iter()
        .find(|record| {
            record.weight == -1
                && record
                    .key
                    .as_json()
                    .as_object()
                    .and_then(|key| key.get("category"))
                    == Some(&Value::Null)
        });
    assert!(removed_group.is_some());
}

#[test]
fn runtime_materializes_sum_count_for_relation_without_value_semantic_role() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 17);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(sums.value(1), 5);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_repeated_epoch_is_idempotent_before_and_after_restore() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();
    let input = RelationInputBatch {
        encoding: RelationInputEncodingV1::SourceRelationV1,
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        stream_id: "idempotent-stream".to_string(),
        partition_id: 0,
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        start_offset_inclusive: 0,
        end_offset_exclusive: 3,
        event_time_watermark: None,
        batches: vec![purchases_batch()],
    };
    let key = EpochIdempotencyKey::new("repeated-epoch").unwrap();
    runtime
        .apply_changes(1, key.clone(), vec![input.clone()])
        .unwrap();
    let checkpoint = runtime.checkpoint().unwrap();
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let duplicate = runtime
        .apply_changes(1, key.clone(), vec![input.clone()])
        .unwrap();
    assert!(duplicate.output_deltas.is_empty());
    assert_eq!(runtime.checkpoint().unwrap(), checkpoint);
    assert_eq!(
        runtime
            .materialized_view_page(
                ScopedViewId {
                    tenant_id: "tenant-a".to_string(),
                    program_id: "program-purchases".to_string(),
                    view_id: "purchases_by_user".to_string(),
                },
                SnapshotPageRequest {
                    committed_epoch: Some(1),
                    page_token: None,
                    max_rows: None,
                },
            )
            .unwrap()
            .batches,
        page.batches
    );

    let mut restored = restore_standing_runtime(checkpoint.clone()).unwrap();
    let restored_duplicate = restored.apply_changes(1, key, vec![input]).unwrap();
    assert!(restored_duplicate.output_deltas.is_empty());
    assert_eq!(restored.checkpoint().unwrap(), checkpoint);
}

#[test]
fn runtime_same_epoch_input_permutations_have_identical_state_output_and_restore() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let inputs = [(0, "alice", 10), (1, "bob", 5), (2, "alice", 7)].map(
        |(partition_id, user_id, amount)| RelationInputBatch {
            encoding: RelationInputEncodingV1::SourceRelationV1,
            relation_id: catalog.relation_schema.relation_id.clone(),
            relation_version: catalog.relation_schema.relation_version.clone(),
            stream_id: "permutation-stream".to_string(),
            partition_id,
            schema_fingerprint: catalog.schema_fingerprint.to_string(),
            start_offset_inclusive: 0,
            end_offset_exclusive: 1,
            event_time_watermark: None,
            batches: vec![purchases_rows_batch(&[(user_id, amount, 1)])],
        },
    );
    let mut baseline = None;
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let mut runtime = create_standing_runtime_with_sql_and_catalogs(
            &identity,
            std::slice::from_ref(&catalog),
            sql,
            std::slice::from_ref(&input_schema),
            std::slice::from_ref(&output_schema),
        )
        .unwrap();
        runtime
            .apply_changes(
                1,
                EpochIdempotencyKey::new("permuted-epoch").unwrap(),
                order.iter().map(|index| inputs[*index].clone()).collect(),
            )
            .unwrap();
        let checkpoint = runtime.checkpoint().unwrap();
        let page = runtime
            .materialized_view_page(
                ScopedViewId {
                    tenant_id: "tenant-a".to_string(),
                    program_id: "program-purchases".to_string(),
                    view_id: "purchases_by_user".to_string(),
                },
                SnapshotPageRequest {
                    committed_epoch: Some(1),
                    page_token: None,
                    max_rows: None,
                },
            )
            .unwrap();
        let restored = restore_standing_runtime(checkpoint.clone()).unwrap();
        let restored_page = restored
            .materialized_view_page(
                ScopedViewId {
                    tenant_id: "tenant-a".to_string(),
                    program_id: "program-purchases".to_string(),
                    view_id: "purchases_by_user".to_string(),
                },
                SnapshotPageRequest {
                    committed_epoch: Some(1),
                    page_token: None,
                    max_rows: None,
                },
            )
            .unwrap();
        assert_eq!(restored_page.batches, page.batches);
        let evidence = (
            checkpoint.state_root.content_hash,
            checkpoint.state_payload.unwrap().payload,
            page.batches,
        );
        match &baseline {
            Some(baseline) => assert_eq!(&evidence, baseline),
            None => baseline = Some(evidence),
        }
    }
}

#[test]
fn runtime_bounds_restored_idempotency_history_and_preserves_recent_duplicates() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(1, EpochIdempotencyKey::new("seed").unwrap(), Vec::new())
        .unwrap();

    let mut checkpoint = runtime.checkpoint().unwrap();
    let state_payload = checkpoint.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    payload["applied_epochs"] = Value::Array(
        (1..=1_025)
            .map(|logical_epoch| {
                json!({
                    "idempotency_key": format!("epoch-{logical_epoch}"),
                    "logical_epoch": logical_epoch,
                })
            })
            .collect(),
    );
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    checkpoint.state_root.content_hash = stable_bytes_hash(state_payload.payload.as_bytes());

    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    let checkpoint = restored.checkpoint().unwrap();
    let payload: Value = serde_json::from_str(&checkpoint.state_payload.unwrap().payload).unwrap();
    let applied_epochs = payload["applied_epochs"].as_array().unwrap();
    assert_eq!(applied_epochs.len(), 1_024);
    assert!(applied_epochs
        .iter()
        .any(|entry| entry["idempotency_key"] == "epoch-1025"));
    assert!(!applied_epochs
        .iter()
        .any(|entry| entry["idempotency_key"] == "epoch-1"));

    let duplicate = restored
        .apply_changes(
            1_025,
            EpochIdempotencyKey::new("epoch-1025").unwrap(),
            Vec::new(),
        )
        .unwrap();
    assert!(duplicate.output_deltas.is_empty());
    assert!(matches!(
        restored.apply_changes(
            1_026,
            EpochIdempotencyKey::new("epoch-1025").unwrap(),
            Vec::new(),
        ),
        Err(StandingProgramRuntimeError::IdempotencyKeyConflict {
            first_epoch: 1_025,
            attempted_epoch: 1_026,
            ..
        })
    ));
}

#[test]
fn runtime_materializes_sum_arithmetic_expression() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "adjusted_purchases_by_user".to_string(),
        relation_name: "adjusted_purchases_by_user".to_string(),
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
    let sql =
        "select user_id, sum(amount + 1) as adjusted_sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "adjusted_purchases_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "adjusted_purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let adjusted_sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(adjusted_sums.value(0), 19);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(adjusted_sums.value(1), 6);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_materializes_cast_int64_aggregate_expression() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "select user_id, sum(cast(amount as bigint)) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 17);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(sums.value(1), 5);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_materializes_nested_double_colon_cast_int64_aggregate_expression() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "adjusted_purchases_by_user".to_string(),
        relation_name: "adjusted_purchases_by_user".to_string(),
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
    let sql = "select user_id, sum((amount + 1)::bigint) as adjusted_sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "adjusted_purchases_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "adjusted_purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let adjusted_sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(adjusted_sums.value(0), 19);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(adjusted_sums.value(1), 6);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_materializes_try_and_safe_cast_int64_aggregate_expressions() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "cast_purchases_by_user".to_string(),
        relation_name: "cast_purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
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
    let sql = "select user_id, sum(try_cast(amount as bigint)) as try_sum, sum(safe_cast(amount as int64)) as safe_sum from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "cast_purchases_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "cast_purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let try_sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let safe_sums = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(try_sums.value(0), 17);
    assert_eq!(safe_sums.value(0), 17);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(try_sums.value(1), 5);
    assert_eq!(safe_sums.value(1), 5);
}

#[test]
fn runtime_materializes_abs_int64_aggregate_expression() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "absolute_purchases_by_user".to_string(),
        relation_name: "absolute_purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000020".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "absolute_sum".to_string(),
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
    let sql = "select user_id, sum(abs(amount)) as absolute_sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "absolute_purchases_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_rows_batch(&[
                    ("alice", -10, 1),
                    ("bob", 5, 1),
                    ("alice", -7, 1),
                ])],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "absolute_purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 17);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(sums.value(1), 5);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_materializes_greatest_least_int64_aggregate_expressions() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "bounded_purchases_by_user".to_string(),
        relation_name: "bounded_purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000022".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "positive_floor_sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "capped_sum".to_string(),
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
    let sql = "select user_id, sum(greatest(amount, 0)) as positive_floor_sum, sum(least(amount, 10)) as capped_sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "bounded_purchases_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_rows_batch(&[
                    ("alice", -10, 1),
                    ("alice", 17, 1),
                    ("bob", 5, 1),
                ])],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "bounded_purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let positive_floor_sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let capped_sums = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(positive_floor_sums.value(0), 17);
    assert_eq!(capped_sums.value(0), 0);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(positive_floor_sums.value(1), 5);
    assert_eq!(capped_sums.value(1), 5);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_materializes_coalesce_nullable_int64_aggregate_expression() {
    let catalog = scores_catalog_with_nullable_score();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "coalesced_scores_by_user".to_string(),
        relation_name: "coalesced_scores_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000021".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "coalesced_sum".to_string(),
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
    let sql = "select user_id, sum(coalesce(score, 0)) as coalesced_sum, count(*) as count from scores group by user_id";
    let identity = standing_identity_with_view(sql, "coalesced_scores_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_nullable_rows_batch(&[
                    ("alice", Some(10), 1),
                    ("alice", None, 1),
                    ("alice", Some(7), 1),
                    ("bob", None, 1),
                ])],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "coalesced_scores_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 17);
    assert_eq!(counts.value(0), 3);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(sums.value(1), 0);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_materializes_is_not_distinct_from_null_predicate() {
    let catalog = scores_catalog_with_nullable_score();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "null_scores_by_user".to_string(),
        relation_name: "null_scores_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000022".to_string(),
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
    };
    let sql = "select user_id, sum(coalesce(score, 0)) as sum, count(*) as count from scores where score is not distinct from null group by user_id";
    let identity = standing_identity_with_view(sql, "null_scores_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_nullable_rows_batch(&[
                    ("alice", Some(10), 1),
                    ("alice", None, 1),
                    ("bob", None, 1),
                    ("carol", Some(3), 1),
                ])],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "null_scores_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 0);
    assert_eq!(counts.value(0), 1);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(sums.value(1), 0);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_materializes_is_distinct_from_predicate() {
    let catalog = scores_catalog_with_nullable_score();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "distinct_scores_by_user".to_string(),
        relation_name: "distinct_scores_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000023".to_string(),
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
    };
    let sql = "select user_id, sum(coalesce(score, 0)) as sum, count(*) as count from scores where score is distinct from 0 group by user_id";
    let identity = standing_identity_with_view(sql, "distinct_scores_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_nullable_rows_batch(&[
                    ("alice", Some(10), 1),
                    ("alice", None, 1),
                    ("bob", None, 1),
                    ("carol", Some(0), 1),
                ])],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "distinct_scores_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 10);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(sums.value(1), 0);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_materializes_case_when_distinct_from_null_predicates() {
    let catalog = scores_catalog_with_nullable_score();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "case_null_safe_scores_by_user".to_string(),
        relation_name: "case_null_safe_scores_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000025".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "present_sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "null_count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    };
    let sql = "select user_id, sum(case when score is distinct from null then coalesce(score, 0) else 0 end) as present_sum, sum(case when score is not distinct from null then 1 else 0 end) as null_count from scores group by user_id";
    let identity = standing_identity_with_view(sql, "case_null_safe_scores_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_nullable_rows_batch(&[
                    ("alice", Some(10), 1),
                    ("alice", None, 1),
                    ("alice", Some(7), 1),
                    ("bob", None, 1),
                ])],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "case_null_safe_scores_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let present_sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let null_counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(present_sums.value(0), 17);
    assert_eq!(null_counts.value(0), 1);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(present_sums.value(1), 0);
    assert_eq!(null_counts.value(1), 1);
}

#[test]
fn runtime_materializes_case_when_is_null_predicates() {
    let catalog = scores_catalog_with_nullable_score();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "case_null_predicate_scores_by_user".to_string(),
        relation_name: "case_null_predicate_scores_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000026".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "null_count".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "present_sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    };
    let sql = "select user_id, sum(case when score is null then 1 else 0 end) as null_count, sum(case when score is not null then coalesce(score, 0) else 0 end) as present_sum from scores group by user_id";
    let identity = standing_identity_with_view(sql, "case_null_predicate_scores_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_nullable_rows_batch(&[
                    ("alice", Some(10), 1),
                    ("alice", None, 1),
                    ("alice", Some(7), 1),
                    ("bob", None, 1),
                ])],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "case_null_predicate_scores_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let null_counts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let present_sums = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(null_counts.value(0), 1);
    assert_eq!(present_sums.value(0), 17);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(null_counts.value(1), 1);
    assert_eq!(present_sums.value(1), 0);
}

#[test]
fn runtime_materializes_case_when_int64_aggregate_expression() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "positive_purchases_by_user".to_string(),
        relation_name: "positive_purchases_by_user".to_string(),
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
                name: "positive_sum".to_string(),
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
    let sql = "select user_id, sum(case when amount > 6 then amount else 0 end) as positive_sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "positive_purchases_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "positive_purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 17);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(sums.value(1), 0);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_materializes_case_when_between_and_in_predicates() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "case_predicate_purchases_by_user".to_string(),
        relation_name: "case_predicate_purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000024".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "bounded_sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "selected_sum".to_string(),
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
    let sql = "select user_id, sum(case when amount between 6 and 10 then amount else 0 end) as bounded_sum, sum(case when amount in (5, 7) then amount else 0 end) as selected_sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "case_predicate_purchases_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "case_predicate_purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let bounded_sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let selected_sums = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(bounded_sums.value(0), 17);
    assert_eq!(selected_sums.value(0), 7);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(bounded_sums.value(1), 0);
    assert_eq!(selected_sums.value(1), 5);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_materializes_if_int64_aggregate_expression() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "positive_if_purchases_by_user".to_string(),
        relation_name: "positive_if_purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000023".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "positive_sum".to_string(),
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
    let sql = "select user_id, sum(if(amount > 6, amount, 0)) as positive_sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "positive_if_purchases_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "positive_if_purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 17);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(sums.value(1), 0);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_materializes_multi_branch_case_when_int64_aggregate_expression() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "capped_purchases_by_user".to_string(),
        relation_name: "capped_purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000018".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "capped_sum".to_string(),
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
    let sql = "select user_id, sum(case when amount > 8 then 8 when amount > 0 then amount else 0 end) as capped_sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "capped_purchases_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "capped_purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 15);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(sums.value(1), 5);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_materializes_simple_case_when_int64_aggregate_expression() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "bucketed_purchases_by_user".to_string(),
        relation_name: "bucketed_purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000019".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "bucket_sum".to_string(),
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
    let sql = "select user_id, sum(case amount when 10 then 100 when 7 then 70 else 0 end) as bucket_sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "bucketed_purchases_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "bucketed_purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 170);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(sums.value(1), 0);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_materializes_count_only_aggregate_and_restores_state() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_count_output_schema();
    let sql = "select user_id, count(*) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "purchases_by_user_count");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_count_page(runtime.as_ref(), 1, &[("alice", 2), ("bob", 1)]);

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("user_id", DataType::Utf8, false),
                        Field::new("amount", DataType::Int64, false),
                        Field::new("delta", DataType::Int64, false),
                    ])),
                    vec![
                        Arc::new(StringArray::from(vec!["alice"])) as _,
                        Arc::new(Int64Array::from(vec![11])) as _,
                        Arc::new(Int64Array::from(vec![1])) as _,
                    ],
                )
                .unwrap()],
            }],
        )
        .unwrap();

    assert_count_page(restored.as_ref(), 2, &[("alice", 3), ("bob", 1)]);
}

#[test]
fn runtime_materializes_filter_project_view_and_restores_state() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "select user_id, score from scores where score > 0 order by score desc";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, 1), ("bob", -3, 1)])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 1, &[("alice", 10)]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, -1), ("carol", 7, 1)])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(restored.as_ref(), 2, &[("carol", 7)]);
}

#[test]
fn runtime_materializes_filter_project_view_from_published_relation_delta_input() {
    let binding = published_relation_binding_v1(
        "filtered_scores",
        7,
        "velorix-logical-view-plan-sha256-v1:plan",
        &filtered_scores_published_relation(),
    )
    .unwrap();
    let catalog = catalog_from_published_relation_binding(&binding).unwrap();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    assert_eq!(input_schema, binding.relation);
    let output_schema = filtered_scores_projection_output_schema();
    let sql = "select user_id, score from filtered_scores where score > 0 order by score desc";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let published_encoding = RelationInputEncodingV1::PublishedRelationDeltaV1 {
        delta_codec_identity: PUBLISHED_RELATION_DELTA_CODEC_V1.to_string(),
        output_schema_hash: binding.output_schema_hash.clone(),
        weight_field_name: PUBLISHED_DELTA_WEIGHT_FIELD_V1.to_string(),
        weight_field_index: binding.relation.columns.len(),
    };
    let input_batch = |start: u64, end: u64, rows: &[(&str, i64, i64)]| RelationInputBatch {
        encoding: published_encoding.clone(),
        relation_id: binding.relation.relation_id.clone(),
        relation_version: binding.relation.relation_version.clone(),
        stream_id: binding.output_stream_id.clone(),
        partition_id: 0,
        schema_fingerprint: binding.relation.schema_fingerprint.clone(),
        start_offset_inclusive: start,
        end_offset_exclusive: end,
        event_time_watermark: None,
        batches: vec![filtered_scores_published_batch(rows)],
    };

    runtime
        .apply_changes(
            10,
            EpochIdempotencyKey::new("published-epoch-10").unwrap(),
            vec![input_batch(0, 10, &[("alice", 10, 1), ("carol", 3, 1)])],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 10, &[("alice", 10), ("carol", 3)]);

    runtime
        .apply_changes(
            25,
            EpochIdempotencyKey::new("published-epoch-25").unwrap(),
            vec![input_batch(
                10,
                25,
                &[
                    ("alice", 10, -1),
                    ("bob", -4, 1),
                    ("carol", 3, -1),
                    ("dave", 8, 1),
                ],
            )],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 25, &[("dave", 8)]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            100,
            EpochIdempotencyKey::new("published-epoch-100").unwrap(),
            vec![RelationInputBatch {
                encoding: published_encoding,
                relation_id: binding.relation.relation_id.clone(),
                relation_version: binding.relation.relation_version.clone(),
                stream_id: binding.output_stream_id.clone(),
                partition_id: 0,
                schema_fingerprint: binding.relation.schema_fingerprint.clone(),
                start_offset_inclusive: 25,
                end_offset_exclusive: 100,
                event_time_watermark: None,
                batches: vec![filtered_scores_published_empty_batch()],
            }],
        )
        .unwrap();
    assert_projected_scores_page(restored.as_ref(), 100, &[("dave", 8)]);
}

#[test]
fn runtime_rejects_published_relation_delta_input_with_wrong_codec_or_weight_layout() {
    let binding = published_relation_binding_v1(
        "filtered_scores",
        7,
        "velorix-logical-view-plan-sha256-v1:plan",
        &filtered_scores_published_relation(),
    )
    .unwrap();
    let catalog = catalog_from_published_relation_binding(&binding).unwrap();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = filtered_scores_projection_output_schema();
    let sql = "select user_id, score from filtered_scores where score > 0";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let wrong_codec = RelationInputEncodingV1::PublishedRelationDeltaV1 {
        delta_codec_identity: "not-the-published-codec".to_string(),
        output_schema_hash: binding.output_schema_hash.clone(),
        weight_field_name: PUBLISHED_DELTA_WEIGHT_FIELD_V1.to_string(),
        weight_field_index: binding.relation.columns.len(),
    };
    let result = runtime.apply_changes(
        1,
        EpochIdempotencyKey::new("published-epoch-1").unwrap(),
        vec![RelationInputBatch {
            encoding: wrong_codec,
            relation_id: binding.relation.relation_id.clone(),
            relation_version: binding.relation.relation_version.clone(),
            stream_id: binding.output_stream_id.clone(),
            partition_id: 0,
            schema_fingerprint: binding.relation.schema_fingerprint.clone(),
            start_offset_inclusive: 0,
            end_offset_exclusive: 1,
            event_time_watermark: None,
            batches: vec![filtered_scores_published_batch(&[("alice", 10, 1)])],
        }],
    );
    assert!(matches!(
        result,
        Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "published_input.delta_codec_identity"
        })
    ));

    let wrong_weight_index = RelationInputEncodingV1::PublishedRelationDeltaV1 {
        delta_codec_identity: PUBLISHED_RELATION_DELTA_CODEC_V1.to_string(),
        output_schema_hash: binding.output_schema_hash.clone(),
        weight_field_name: PUBLISHED_DELTA_WEIGHT_FIELD_V1.to_string(),
        weight_field_index: 0,
    };
    let result = runtime.apply_changes(
        1,
        EpochIdempotencyKey::new("published-epoch-1").unwrap(),
        vec![RelationInputBatch {
            encoding: wrong_weight_index,
            relation_id: binding.relation.relation_id.clone(),
            relation_version: binding.relation.relation_version.clone(),
            stream_id: binding.output_stream_id.clone(),
            partition_id: 0,
            schema_fingerprint: binding.relation.schema_fingerprint.clone(),
            start_offset_inclusive: 0,
            end_offset_exclusive: 1,
            event_time_watermark: None,
            batches: vec![filtered_scores_published_batch(&[("alice", 10, 1)])],
        }],
    );
    assert!(matches!(
        result,
        Err(StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "published_input.weight_field_index"
        })
    ));
}

fn filtered_scores_published_relation() -> RelationSchema {
    RelationSchema {
        relation_id: "filtered_scores".to_string(),
        relation_name: "filtered_scores".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
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

fn filtered_scores_projection_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "positive_scores".to_string(),
        relation_name: "positive_scores".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "3".repeat(64)),
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

fn filtered_scores_published_batch(rows: &[(&str, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new(PUBLISHED_DELTA_WEIGHT_FIELD_V1, DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn filtered_scores_published_empty_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new(PUBLISHED_DELTA_WEIGHT_FIELD_V1, DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ],
    )
    .unwrap()
}

#[test]
fn runtime_materializes_plain_select_distinct_filter_project_when_primary_key_is_output_key() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "select distinct user_id, score from scores where score > 0";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, 1), ("bob", -3, 1)])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 1, &[("alice", 10)]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, -1), ("carol", 7, 1)])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(restored.as_ref(), 2, &[("carol", 7)]);
}

#[test]
fn runtime_rejects_plain_select_distinct_filter_project_without_primary_key_output() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "select distinct score from scores where score > 0";
    let identity = standing_identity_with_view(sql, "positive_scores");

    let error = match create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    ) {
        Ok(_) => panic!("SELECT DISTINCT without primary key output should fail closed"),
        Err(error) => error,
    };

    assert!(
        error.contains("primary key"),
        "expected primary-key admission error, got `{error}`"
    );
}

#[test]
fn runtime_materializes_plain_select_distinct_filter_project_by_projected_output_key() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_distinct_score_output_schema();
    let sql = "select distinct score from scores where score > 0";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", 10, 1),
                    ("carol", 7, 1),
                ])],
            }],
        )
        .unwrap();
    assert_projected_distinct_scores_page(runtime.as_ref(), 1, &[10, 7]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, -1)])],
            }],
        )
        .unwrap();
    assert_projected_distinct_scores_page(restored.as_ref(), 2, &[10, 7]);

    restored
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("bob", 10, -1)])],
            }],
        )
        .unwrap();
    assert_projected_distinct_scores_page(restored.as_ref(), 3, &[7]);
}

#[test]
fn runtime_materializes_row_number_and_restores_rerank_state() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_row_number_output_schema();
    let sql = "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores where score > 0";
    let identity = standing_identity_with_view(sql, "scores_ranked");
    let logical_plan =
        lower_supported_analytic_row_number_sql_to_logical_plan(sql, &catalog, &output_schema)
            .unwrap();
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let first = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", 10, 1),
                    ("carol", -1, 1),
                ])],
            }],
        )
        .unwrap();
    assert_row_number_delta(
        &first.output_deltas[0].delta,
        &[("alice", 1, 1), ("bob", 2, 1)],
    );
    assert_row_number_page(runtime.as_ref(), 1, &[("alice", 1), ("bob", 2)]);

    let second = runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("aaron", 10, 1)])],
            }],
        )
        .unwrap();
    assert_row_number_delta(
        &second.output_deltas[0].delta,
        &[
            ("aaron", 1, 1),
            ("alice", 1, -1),
            ("alice", 2, 1),
            ("bob", 2, -1),
            ("bob", 3, 1),
        ],
    );
    assert_row_number_page(
        runtime.as_ref(),
        2,
        &[("aaron", 1), ("alice", 2), ("bob", 3)],
    );

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("aaron", 10, -1)])],
            }],
        )
        .unwrap();

    assert_row_number_page(restored.as_ref(), 3, &[("alice", 1), ("bob", 2)]);
}

#[test]
fn runtime_materializes_rank_with_sql_order_ties() {
    let catalog = scores_with_adjustment_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_row_number_output_schema();
    let sql = "select user_id, rank() over (partition by user_id_adjustment order by score desc) as rank from scores where score > 0";
    let identity = standing_identity_with_view(sql, "scores_ranked");
    let logical_plan =
        lower_supported_analytic_row_number_sql_to_logical_plan(sql, &catalog, &output_schema)
            .unwrap();
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let first = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_with_adjustment_rows_batch(&[
                    ("alice", 10, 1, 1),
                    ("bob", 10, 1, 1),
                    ("carol", 5, 1, 1),
                    ("dana", 7, 2, 1),
                ])],
            }],
        )
        .unwrap();

    assert_row_number_delta(
        &first.output_deltas[0].delta,
        &[
            ("alice", 1, 1),
            ("bob", 1, 1),
            ("carol", 3, 1),
            ("dana", 1, 1),
        ],
    );
    assert_row_number_page(
        runtime.as_ref(),
        1,
        &[("alice", 1), ("bob", 1), ("carol", 3), ("dana", 1)],
    );
}

#[test]
fn runtime_materializes_dense_rank_with_sql_order_ties() {
    let catalog = scores_with_adjustment_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_row_number_output_schema();
    let sql = "select user_id, dense_rank() over (partition by user_id_adjustment order by score desc) as rank from scores where score > 0";
    let identity = standing_identity_with_view(sql, "scores_ranked");
    let logical_plan =
        lower_supported_analytic_row_number_sql_to_logical_plan(sql, &catalog, &output_schema)
            .unwrap();
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let first = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_with_adjustment_rows_batch(&[
                    ("bob", 10, 1, 1),
                    ("alice", 10, 1, 1),
                    ("carol", 5, 1, 1),
                    ("dana", 7, 2, 1),
                ])],
            }],
        )
        .unwrap();

    assert_row_number_delta(
        &first.output_deltas[0].delta,
        &[
            ("alice", 1, 1),
            ("bob", 1, 1),
            ("carol", 2, 1),
            ("dana", 1, 1),
        ],
    );
    assert_row_number_page(
        runtime.as_ref(),
        1,
        &[("alice", 1), ("bob", 1), ("carol", 2), ("dana", 1)],
    );
}

#[test]
fn runtime_materializes_wrapped_row_number_top_n() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_row_number_output_schema();
    let sql = "select user_id, rank from (select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores where score > 0) ranked where rank <= 2";
    let identity = standing_identity_with_view(sql, "scores_ranked");
    let logical_plan =
        lower_supported_analytic_row_number_sql_to_logical_plan(sql, &catalog, &output_schema)
            .unwrap();
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let first = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", 10, 1),
                    ("carol", 10, 1),
                ])],
            }],
        )
        .unwrap();
    assert_row_number_delta(
        &first.output_deltas[0].delta,
        &[("alice", 1, 1), ("bob", 2, 1)],
    );
    assert_row_number_page(runtime.as_ref(), 1, &[("alice", 1), ("bob", 2)]);

    let second = runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, -1)])],
            }],
        )
        .unwrap();
    assert_row_number_delta(
        &second.output_deltas[0].delta,
        &[
            ("alice", 1, -1),
            ("bob", 2, -1),
            ("bob", 1, 1),
            ("carol", 2, 1),
        ],
    );
    assert_row_number_page(runtime.as_ref(), 2, &[("bob", 1), ("carol", 2)]);
}

#[test]
fn runtime_materializes_wrapped_row_number_top_one_and_promotes_after_delete() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_row_number_output_schema();
    let sql = "select user_id, rank from (select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores where score > 0) ranked where rank = 1";
    let identity = standing_identity_with_view(sql, "scores_ranked");
    let logical_plan =
        lower_supported_analytic_row_number_sql_to_logical_plan(sql, &catalog, &output_schema)
            .unwrap();
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let first = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", 10, 1),
                    ("carol", 10, 1),
                    ("dana", 5, 1),
                    ("erin", 5, 1),
                ])],
            }],
        )
        .unwrap();
    assert_row_number_delta(
        &first.output_deltas[0].delta,
        &[("alice", 1, 1), ("dana", 1, 1)],
    );
    assert_row_number_page(runtime.as_ref(), 1, &[("alice", 1), ("dana", 1)]);

    let second = runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 5,
                end_offset_exclusive: 6,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, -1)])],
            }],
        )
        .unwrap();
    assert_row_number_delta(
        &second.output_deltas[0].delta,
        &[("alice", 1, -1), ("bob", 1, 1)],
    );
    assert_row_number_page(runtime.as_ref(), 2, &[("bob", 1), ("dana", 1)]);
}

#[test]
fn runtime_materializes_row_number_with_source_and_outer_predicates_before_ranking() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_row_number_output_schema();
    let sql = "select s.user_id, row_number() over (partition by s.score order by s.score desc, s.user_id asc) as rank from (select * from scores where user_id <> 'aaron') s where s.user_id <> 'alice'";
    let identity = standing_identity_with_view(sql, "scores_ranked");
    let logical_plan =
        lower_supported_analytic_row_number_sql_to_logical_plan(sql, &catalog, &output_schema)
            .unwrap();
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("aaron", 10, 1),
                    ("alice", 10, 1),
                    ("bob", 10, 1),
                    ("carol", 10, 1),
                ])],
            }],
        )
        .unwrap();

    assert_row_number_page(runtime.as_ref(), 1, &[("bob", 1), ("carol", 2)]);
}

#[test]
fn runtime_restore_rejects_row_number_checkpoint_when_state_mismatches_published_output() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_row_number_output_schema();
    let sql = "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores where score > 0";
    let identity = standing_identity_with_view(sql, "scores_ranked");
    let logical_plan =
        lower_supported_analytic_row_number_sql_to_logical_plan(sql, &catalog, &output_schema)
            .unwrap();
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, 1), ("bob", 10, 1)])],
            }],
        )
        .unwrap();

    let mut checkpoint = runtime.checkpoint().unwrap();
    let state_payload = checkpoint.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    payload["state"]["rows"] = json!({});
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    checkpoint.state_root.content_hash = stable_bytes_hash(state_payload.payload.as_bytes());

    let err = match restore_standing_runtime(checkpoint) {
        Ok(_) => panic!("restore unexpectedly accepted mismatched row-number checkpoint state"),
        Err(err) => err,
    };

    assert!(
        err.contains("generic_checkpoint_payload"),
        "unexpected restore error: {err}"
    );
}

#[test]
fn runtime_rejects_row_number_logical_plan_that_does_not_match_sql() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_row_number_output_schema();
    let sql = "select user_id, sum(score) as sum, count(*) as count from scores group by user_id";
    let identity = standing_identity_with_view(sql, "scores_ranked");
    let logical_plan = row_number_logical_plan(sql, &catalog, &output_schema);

    let error = match create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    ) {
        Ok(_) => panic!("row-number runtime accepted a mismatched SQL/logical plan"),
        Err(error) => error,
    };

    assert!(error.contains("analytic_row_number_view_plan"));
}

#[test]
fn runtime_rejects_tampered_operator_contract_before_construction() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_row_number_output_schema();
    let sql = "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores where score > 0";
    let identity = standing_identity_with_view(sql, "scores_ranked");
    let mut logical_plan =
        lower_supported_analytic_row_number_sql_to_logical_plan(sql, &catalog, &output_schema)
            .unwrap();
    logical_plan.operator_dag_contract.operators[0].outputs[0].changelog =
        ChangelogModeV1::AppendOnly;

    let error = match create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    ) {
        Ok(_) => panic!("runtime accepted a tampered operator contract"),
        Err(error) => error,
    };

    assert!(error.contains("analytic_row_number_view_plan"));
}

#[test]
fn runtime_rejects_tampered_execution_implementation_before_construction() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_row_number_output_schema();
    let sql = "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores where score > 0";
    let identity = standing_identity_with_view(sql, "scores_ranked");
    let mut logical_plan =
        lower_supported_analytic_row_number_sql_to_logical_plan(sql, &catalog, &output_schema)
            .unwrap();
    logical_plan
        .execution_implementation
        .as_mut()
        .unwrap()
        .implementation_id = "velorix-generic-dag-v1".to_string();
    logical_plan.plan_hash = Some(logical_view_plan_hash(&logical_plan).unwrap());

    let error = match create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    ) {
        Ok(_) => panic!("runtime accepted a tampered execution implementation"),
        Err(error) => error,
    };

    assert!(error.contains("analytic_row_number_view_plan"));
}

#[test]
fn runtime_materializes_row_number_with_precise_large_int64_ordering() {
    let catalog = scores_with_adjustment_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_row_number_output_schema();
    let sql = "select user_id, row_number() over (partition by user_id_adjustment order by score desc, user_id asc) as rank from scores where score > 0";
    let identity = standing_identity_with_view(sql, "scores_ranked");
    let logical_plan =
        lower_supported_analytic_row_number_sql_to_logical_plan(sql, &catalog, &output_schema)
            .unwrap();
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: None,
                batches: vec![scores_with_adjustment_rows_batch(&[
                    ("alice", 9_007_199_254_740_992, 1, 1),
                    ("bob", 9_007_199_254_740_993, 1, 1),
                ])],
            }],
        )
        .unwrap();

    assert_row_number_page(runtime.as_ref(), 1, &[("alice", 2), ("bob", 1)]);
}

#[test]
fn runtime_materializes_filter_project_union_distinct_overlapping_branch_rows_once() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "select user_id, score from scores where score > 0 union distinct select user_id, score from scores where score >= 10";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", -3, 1),
                    ("carol", 7, 1),
                ])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 1, &[("alice", 10), ("carol", 7)]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("carol", 7, -1), ("dave", 12, 1)])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(restored.as_ref(), 2, &[("alice", 10), ("dave", 12)]);
}

#[test]
fn runtime_materializes_filter_project_intersect_distinct_overlapping_branch_rows_once() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "select user_id, score from scores where score > 0 intersect distinct select user_id, score from scores where score >= 10";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", -3, 1),
                    ("carol", 7, 1),
                ])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 1, &[("alice", 10)]);

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_projected_scores_page(restored.as_ref(), 1, &[("alice", 10)]);
}

#[test]
fn runtime_materializes_filter_project_except_distinct_left_minus_right_and_restores_state() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "select user_id, score from scores where score > 0 except distinct select user_id, score from scores where score >= 10";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", -3, 1),
                    ("carol", 7, 1),
                ])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 1, &[("carol", 7)]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("dave", 12, 1), ("erin", 6, 1)])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(restored.as_ref(), 2, &[("carol", 7), ("erin", 6)]);
}

#[test]
fn runtime_materializes_filter_project_with_scalar_expression_predicate() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "select user_id, score from scores where score + 1 > 10 order by score desc";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", 9, 1),
                    ("carol", 11, 1),
                ])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 1, &[("alice", 10), ("carol", 11)]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, -1), ("dave", 12, 1)])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(restored.as_ref(), 2, &[("carol", 11), ("dave", 12)]);
}

#[test]
fn runtime_materializes_filter_project_with_expression_vs_expression_predicate() {
    let catalog = scores_with_adjustment_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "select user_id, score from scores where score + 1 > user_id_adjustment order by score desc";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_with_adjustment_rows_batch(&[
                    ("alice", 10, 10, 1),
                    ("bob", 9, 10, 1),
                    ("carol", 12, 11, 1),
                ])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 1, &[("alice", 10), ("carol", 12)]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_with_adjustment_rows_batch(&[
                    ("alice", 10, 10, -1),
                    ("dave", 11, 9, 1),
                ])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(restored.as_ref(), 2, &[("carol", 12), ("dave", 11)]);
}

#[test]
fn runtime_materializes_filter_project_with_unprojected_predicate_column() {
    let catalog = scores_with_adjustment_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "select user_id, score from scores where user_id_adjustment > 0 order by score desc, user_id asc limit 10";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_with_adjustment_rows_batch(&[
                    ("alice", 10, 1, 1),
                    ("bob", 12, 0, 1),
                    ("carol", 7, 2, 1),
                ])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 1, &[("alice", 10), ("carol", 7)]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_with_adjustment_rows_batch(&[
                    ("alice", 10, 1, -1),
                    ("dave", 11, 3, 1),
                ])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(restored.as_ref(), 2, &[("carol", 7), ("dave", 11)]);
}

#[test]
fn runtime_materializes_single_key_aggregates_over_multiple_raw_int64_input_columns() {
    let catalog = scores_with_adjustment_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_multi_input_stats_output_schema();
    let sql = "select user_id, sum(score) as sum_score, min(user_id_adjustment) as min_adj, max(user_id_adjustment) as max_adj, avg(user_id_adjustment) as avg_adj, count(user_id_adjustment) as count_adj from scores group by user_id";
    let identity = standing_identity_with_view(sql, "scores_by_user_multi_input_stats");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_with_adjustment_rows_batch(&[
                    ("alice", 10, 3, 1),
                    ("alice", 7, -2, 1),
                    ("bob", 5, 8, 1),
                ])],
            }],
        )
        .unwrap();
    assert_scores_multi_input_stats_page(
        runtime.as_ref(),
        1,
        &[("alice", 17, -2, 3, 0.5, 2), ("bob", 5, 8, 8, 8.0, 1)],
    );

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_with_adjustment_rows_batch(&[
                    ("alice", 10, 3, -1),
                    ("bob", 2, -4, 1),
                ])],
            }],
        )
        .unwrap();
    assert_scores_multi_input_stats_page(
        runtime.as_ref(),
        2,
        &[("alice", 7, -2, -2, -2.0, 1), ("bob", 7, -4, 8, 2.0, 2)],
    );
}

#[test]
fn runtime_materializes_multi_input_count_distinct_by_selected_value_across_restore_and_retractions(
) {
    let mut catalog = scores_with_adjustment_catalog();
    let adjustment = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "user_id_adjustment")
        .unwrap();
    adjustment.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable adjustment catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;

    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_multi_input_distinct_output_schema();
    let sql = "select user_id, sum(score) as sum_score, count(distinct user_id_adjustment) as distinct_adjustments from scores group by user_id";
    let identity = standing_identity_with_view(sql, "scores_by_user_distinct_adjustment");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_with_nullable_adjustment_rows_batch(&[
                    ("alice", 10, Some(3), 1),
                    ("alice", 7, Some(3), 1),
                    ("alice", 5, Some(5), 1),
                    ("alice", 2, None, 1),
                ])],
            }],
        )
        .unwrap();
    assert_sum_count_page(
        runtime.as_ref(),
        "scores_by_user_distinct_adjustment",
        1,
        &[("alice", 24, 2)],
    );

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_with_nullable_adjustment_rows_batch(&[(
                    "alice",
                    10,
                    Some(3),
                    -1,
                )])],
            }],
        )
        .unwrap();
    assert_sum_count_page(
        restored.as_ref(),
        "scores_by_user_distinct_adjustment",
        2,
        &[("alice", 14, 2)],
    );

    restored
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 5,
                end_offset_exclusive: 6,
                event_time_watermark: None,
                batches: vec![scores_with_nullable_adjustment_rows_batch(&[(
                    "alice",
                    7,
                    Some(3),
                    -1,
                )])],
            }],
        )
        .unwrap();
    assert_sum_count_page(
        restored.as_ref(),
        "scores_by_user_distinct_adjustment",
        3,
        &[("alice", 7, 1)],
    );
}

#[test]
fn runtime_materializes_filter_project_order_by_limit_top_k_and_restores_full_state() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "select user_id, score from scores where score > 0 order by score desc limit 2";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", 8, 1),
                    ("carol", 6, 1),
                ])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 1, &[("alice", 10), ("bob", 8)]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, -1)])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(restored.as_ref(), 2, &[("bob", 8), ("carol", 6)]);
}

#[test]
fn runtime_materializes_filter_project_order_by_limit_offset_top_k() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "select user_id, score from scores where score > 0 order by score desc, user_id asc limit 2 offset 1";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", 8, 1),
                    ("carol", 6, 1),
                    ("dave", 4, 1),
                ])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 1, &[("bob", 8), ("carol", 6)]);

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("erin", 12, 1)])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 2, &[("alice", 10), ("bob", 8)]);
}

#[test]
fn runtime_materializes_filter_project_order_by_fetch_first_top_k() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "select user_id, score from scores where score > 0 order by score desc, user_id asc fetch first 2 rows only";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("bob", 10, 1),
                    ("alice", 10, 1),
                    ("carol", 8, 1),
                    ("dave", -1, 1),
                ])],
            }],
        )
        .unwrap();

    assert_projected_scores_page(runtime.as_ref(), 1, &[("alice", 10), ("bob", 10)]);
}

#[test]
fn runtime_materializes_filter_project_hidden_input_order_by_top_k() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_key_only_output_schema();
    let sql = "select user_id from scores where score > 0 order by score desc, user_id asc limit 2";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", 8, 1),
                    ("carol", 6, 1),
                ])],
            }],
        )
        .unwrap();
    assert_projected_user_ids_page(runtime.as_ref(), 1, &["alice", "bob"]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, -1)])],
            }],
        )
        .unwrap();
    assert_projected_user_ids_page(restored.as_ref(), 2, &["bob", "carol"]);
}

#[test]
fn runtime_materializes_filter_project_cte_source_filters() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "with score_source as (select * from scores where score > 0) select user_id, score from score_source where user_id <> 'bob'";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", 8, 1),
                    ("carol", -2, 1),
                ])],
            }],
        )
        .unwrap();

    assert_projected_scores_page(runtime.as_ref(), 1, &[("alice", 10)]);
}

#[test]
fn runtime_materializes_filter_project_derived_table_source_filters() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_projection_output_schema();
    let sql = "select s.user_id, s.score from (select * from scores where score > 0) s where s.user_id <> 'bob'";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", 8, 1),
                    ("carol", -2, 1),
                ])],
            }],
        )
        .unwrap();

    assert_projected_scores_page(runtime.as_ref(), 1, &[("alice", 10)]);
}

#[test]
fn runtime_materializes_filter_project_nullable_value_and_restores_state() {
    let catalog = scores_catalog_with_nullable_score();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let mut output_schema = scores_projection_output_schema();
    output_schema.columns[1].nullable = true;
    let sql = "select user_id, score from scores where user_id is not null";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: None,
                batches: vec![scores_nullable_rows_batch(&[
                    ("alice", Some(10), 1),
                    ("bob", None, 1),
                ])],
            }],
        )
        .unwrap();
    assert_projected_nullable_scores_page(
        runtime.as_ref(),
        1,
        &[("alice", Some(10)), ("bob", None)],
    );

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_projected_nullable_scores_page(
        restored.as_ref(),
        1,
        &[("alice", Some(10)), ("bob", None)],
    );
}

#[test]
fn runtime_materializes_computed_filter_project_view() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_computed_projection_output_schema();
    let sql =
        "select user_id, -score + score / 2 + score % 3 as normalized_score from scores where score > 0";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, 1), ("bob", -3, 1)])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 1, &[("alice", -4)]);

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, -1), ("carol", 7, 1)])],
            }],
        )
        .unwrap();
    assert_projected_scores_page(runtime.as_ref(), 2, &[("carol", -3)]);
}

#[test]
fn runtime_materializes_filter_project_case_over_bool_predicate() {
    let catalog = device_status_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = device_status_flag_output_schema();
    let sql = "select device_id, case when enabled = true then 1 else 0 end as enabled_flag from device_status";
    let identity = standing_identity_with_view(sql, "device_enabled_flags");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "device-status-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[
                    ("device-a", true, 100, 1),
                    ("device-b", false, 101, 1),
                ])],
            }],
        )
        .unwrap();

    assert_device_enabled_flags_page(runtime.as_ref(), 1, &[("device-a", 1), ("device-b", 0)]);
}

#[test]
fn runtime_rejects_filter_project_case_bool_predicate_non_bool_literal() {
    let catalog = device_status_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = device_status_flag_output_schema();
    let sql = "select device_id, case when enabled = true then 1 else 0 end as enabled_flag from device_status";
    let identity = standing_identity_with_view(sql, "device_enabled_flags");
    let mut logical_plan =
        lower_supported_filter_project_sql_to_logical_plan(sql, &catalog, &output_schema).unwrap();
    {
        let VelorixLogicalViewExecutionV1::FilterProject { plan } = &mut logical_plan.execution
        else {
            panic!("expected filter/project runtime execution");
        };
        let expression = plan.value_columns[0]
            .expression
            .as_mut()
            .expect("expected CASE projection expression");
        let SupportedProjectionExpr::CaseInt64 { predicate, .. } = expression else {
            panic!("expected CASE projection expression");
        };
        let RowPredicateExpr::Atom { predicate } = predicate else {
            panic!("expected CASE predicate atom");
        };
        predicate.literal = json!(1);
    }
    logical_plan.plan_hash = Some(logical_view_plan_hash(&logical_plan).unwrap());

    let error = match create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    ) {
        Ok(_) => panic!("crafted plan should be rejected"),
        Err(error) => error,
    };

    assert!(
        error.contains("logical_filter_project_view_plan"),
        "the implementation-bound physical DAG must reject the crafted plan before execution: {error}"
    );
}

#[test]
fn runtime_materializes_count_distinct_aggregate_and_restores_state() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_count_output_schema();
    let sql = "select user_id, count(distinct amount) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "purchases_by_user_count");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![purchases_rows_batch(&[
                    ("alice", 10, 1),
                    ("alice", 10, 1),
                    ("alice", 7, 1),
                    ("bob", 5, 1),
                ])],
            }],
        )
        .unwrap();

    assert_count_page(runtime.as_ref(), 1, &[("alice", 2), ("bob", 1)]);

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![purchases_rows_batch(&[("alice", 7, -1)])],
            }],
        )
        .unwrap();

    assert_count_page(restored.as_ref(), 2, &[("alice", 1), ("bob", 1)]);
}

#[test]
fn runtime_materializes_non_null_column_count_aggregate() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_count_output_schema();
    let sql = "select user_id, count(user_id) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "purchases_by_user_count");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_count_page(runtime.as_ref(), 1, &[("alice", 2), ("bob", 1)]);
}

#[test]
fn runtime_materializes_nullable_column_count_aggregate() {
    let catalog = purchases_catalog_with_nullable_amount();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_count_output_schema();
    let sql = "select user_id, count(amount) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "purchases_by_user_count");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_nullable_amount_batch()],
            }],
        )
        .unwrap();

    assert_count_page(runtime.as_ref(), 1, &[("alice", 1), ("bob", 1)]);
}

#[test]
fn runtime_materializes_mixed_nullable_column_count_aggregate() {
    let catalog = purchases_catalog_with_nullable_amount();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "select user_id, sum(amount) as sum, count(amount) as count from purchases group by user_id";
    let identity = standing_identity_with_view(sql, "purchases_by_user_nullable_count");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_nullable_amount_batch()],
            }],
        )
        .unwrap();

    assert_sum_count_page(
        runtime.as_ref(),
        "purchases_by_user_nullable_count",
        1,
        &[("alice", 10, 1), ("bob", 5, 1)],
    );
}

#[test]
fn runtime_materializes_identity_cte_single_relation_aggregate() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "with purchase_source as (select user_id, amount, delta from purchases) select user_id, sum(amount) as sum, count(*) as count from purchase_source group by user_id";
    let identity = standing_identity_with_view(sql, "purchases_by_user_cte");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_sum_count_page(
        runtime.as_ref(),
        "purchases_by_user_cte",
        1,
        &[("alice", 17, 2), ("bob", 5, 1)],
    );
}

#[test]
fn runtime_materializes_cte_source_filter_single_relation_aggregate() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "with purchase_source as (select * from purchases where amount > 5) select user_id, sum(amount) as sum, count(*) as count from purchase_source group by user_id";
    let identity = standing_identity_with_view(sql, "purchases_by_user_cte_filter");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_sum_count_page(
        runtime.as_ref(),
        "purchases_by_user_cte_filter",
        1,
        &[("alice", 17, 2)],
    );
}

#[test]
fn runtime_materializes_derived_table_source_filter_single_relation_aggregate() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "select p.user_id, sum(p.amount) as sum, count(*) as count from (select * from purchases where amount > 5) p group by p.user_id";
    let identity = standing_identity_with_view(sql, "purchases_by_user_derived_filter");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_sum_count_page(
        runtime.as_ref(),
        "purchases_by_user_derived_filter",
        1,
        &[("alice", 17, 2)],
    );
}

#[test]
fn runtime_materializes_cte_source_and_outer_where_filters() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "with purchase_source as (select * from purchases where amount > 5) select user_id, sum(amount) as sum, count(*) as count from purchase_source where user_id <> 'bob' group by user_id";
    let identity = standing_identity_with_view(sql, "purchases_by_user_cte_filter");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", 7, 1),
                    ("alice", 4, 1),
                ])],
            }],
        )
        .unwrap();

    assert_sum_count_page(
        runtime.as_ref(),
        "purchases_by_user_cte_filter",
        1,
        &[("alice", 10, 1)],
    );
}

#[test]
fn runtime_commit_publishes_materialized_output_batch_after_ingest() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let commit = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_eq!(commit.output_batches.len(), 1);
    let output = &commit.output_batches[0];
    assert_eq!(output.view_id, "purchases_by_user");
    assert_eq!(output.schema_fingerprint, output_schema.schema_fingerprint);
    assert_eq!(output.batches.len(), 1);

    let batch = &output.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 17);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(sums.value(1), 5);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_commit_publishes_signed_output_delta_for_changed_keys_only() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let commit = runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![purchases_rows_batch(&[("alice", 3, 1)])],
            }],
        )
        .unwrap();

    assert_eq!(commit.output_deltas.len(), 1);
    let output_delta = &commit.output_deltas[0];
    assert_eq!(output_delta.view_id, "purchases_by_user");
    assert_eq!(
        output_delta.schema_fingerprint,
        output_schema.schema_fingerprint
    );
    assert_eq!(
        output_delta.delta.net_rows().unwrap(),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!("alice")),
                DeltaValue::from_json(json!({ "count": 2, "sum": 17 })),
                -1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("alice")),
                DeltaValue::from_json(json!({ "count": 3, "sum": 20 })),
                1,
            ),
        ]
    );
}

#[test]
fn runtime_materializes_filtered_single_relation_aggregate_view() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "select user_id, sum(amount) as sum, count(*) as count from purchases where amount in (5, 7, 10) and user_id like 'a%' and user_id is not null and amount is not null group by user_id having sum(amount) is not null";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 17);
    assert_eq!(counts.value(0), 2);
}

#[test]
fn runtime_materializes_single_relation_aggregate_with_scalar_expression_predicate() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_total_score_output_schema();
    let sql = "select user_id, sum(score) as total_score, count(*) as event_count from scores where score + 1 > 10 group by user_id";
    let identity = standing_identity_with_view(sql, "scores_by_user_expr_predicate");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("alice", 8, 1),
                    ("bob", 20, 1),
                ])],
            }],
        )
        .unwrap();
    assert_sum_count_page(
        runtime.as_ref(),
        "scores_by_user_expr_predicate",
        1,
        &[("alice", 10, 1), ("bob", 20, 1)],
    );

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("alice", 10, -1), ("carol", 11, 1)])],
            }],
        )
        .unwrap();
    assert_sum_count_page(
        runtime.as_ref(),
        "scores_by_user_expr_predicate",
        2,
        &[("bob", 20, 1), ("carol", 11, 1)],
    );
}

#[test]
fn runtime_materializes_single_relation_aggregate_with_between_predicates() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "select user_id, sum(amount) as sum, count(*) as count from purchases where amount between 6 and 20 group by user_id having sum(amount) between 10 and 20";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![purchases_rows_batch(&[
                    ("alice", 10, 1),
                    ("bob", 5, 1),
                    ("alice", 7, 1),
                    ("carol", 21, 1),
                ])],
            }],
        )
        .unwrap();

    assert_top_purchase_user(runtime.as_ref(), 1, "alice", 17, 2);
}

#[test]
fn runtime_materializes_single_relation_aggregate_with_matching_filters() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "select user_id, sum(amount) filter (where amount > 5) as sum, count(*) filter (where amount > 5) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_sum_count_page(
        runtime.as_ref(),
        "purchases_by_user",
        1,
        &[("alice", 17, 2)],
    );
}

#[test]
fn runtime_materializes_single_relation_aggregate_with_mixed_filters() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "select user_id, sum(amount) filter (where amount > 5) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_sum_count_page(
        runtime.as_ref(),
        "purchases_by_user",
        1,
        &[("alice", 17, 2), ("bob", 0, 1)],
    );

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![purchases_rows_batch(&[("alice", 10, -1)])],
            }],
        )
        .unwrap();

    assert_sum_count_page(
        runtime.as_ref(),
        "purchases_by_user",
        2,
        &[("alice", 7, 1), ("bob", 0, 1)],
    );

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_sum_count_page(
        restored.as_ref(),
        "purchases_by_user",
        2,
        &[("alice", 7, 1), ("bob", 0, 1)],
    );
}

#[test]
fn runtime_materializes_single_relation_filtered_count_distinct_with_mixed_filters() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "select user_id, sum(amount) filter (where amount > 5) as sum, count(distinct amount) filter (where amount > 0) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![purchases_rows_batch(&[
                    ("alice", 10, 1),
                    ("alice", 10, 1),
                    ("alice", 7, 1),
                    ("bob", 5, 1),
                ])],
            }],
        )
        .unwrap();

    assert_sum_count_page(
        runtime.as_ref(),
        "purchases_by_user",
        1,
        &[("alice", 27, 2), ("bob", 0, 1)],
    );

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_sum_count_page(
        restored.as_ref(),
        "purchases_by_user",
        1,
        &[("alice", 27, 2), ("bob", 0, 1)],
    );
}

#[test]
fn runtime_materializes_single_relation_mixed_min_max_avg_filters() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_min_max_avg_output_schema();
    let sql = "select user_id, min(score) filter (where score > 0) as min_pos, max(score) filter (where score <= 0) as max_nonpos, avg(score) filter (where score > 10) as avg_hi from scores group by user_id";
    let identity = standing_identity_with_view(sql, "scores_by_user_stats");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 6,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 5, 1),
                    ("alice", 15, 1),
                    ("alice", -2, 1),
                    ("bob", 12, 1),
                    ("bob", -1, 1),
                    ("bob", 20, 1),
                ])],
            }],
        )
        .unwrap();

    assert_scores_min_max_avg_page(
        runtime.as_ref(),
        1,
        &[("alice", 5, -2, 15.0), ("bob", 12, -1, 16.0)],
    );

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_scores_min_max_avg_page(
        restored.as_ref(),
        1,
        &[("alice", 5, -2, 15.0), ("bob", 12, -1, 16.0)],
    );
}

#[test]
fn runtime_materializes_single_relation_mixed_filter_having_top_k() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "select user_id, sum(amount) filter (where amount > 5) as sum, count(*) as count from purchases group by user_id having sum > 0 order by sum desc limit 1";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_sum_count_page(
        runtime.as_ref(),
        "purchases_by_user",
        1,
        &[("alice", 17, 2)],
    );

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![purchases_rows_batch(&[("bob", 20, 1)])],
            }],
        )
        .unwrap();

    assert_sum_count_page(runtime.as_ref(), "purchases_by_user", 2, &[("bob", 20, 2)]);

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_sum_count_page(restored.as_ref(), "purchases_by_user", 2, &[("bob", 20, 2)]);
}

#[test]
fn runtime_materializes_single_relation_aggregate_with_different_filters() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "select user_id, sum(amount) filter (where amount > 5) as sum, count(*) filter (where amount <= 5) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_sum_count_page(
        runtime.as_ref(),
        "purchases_by_user",
        1,
        &[("alice", 17, 0), ("bob", 0, 1)],
    );
}

#[test]
fn runtime_materializes_single_relation_aggregate_having_view() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql = "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id having sum(amount) in (17) or count(*) not in (0, 2)";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let commit = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_eq!(
        commit.output_deltas[0].delta.net_rows().unwrap(),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!("alice")),
                DeltaValue::from_json(json!({ "count": 2, "sum": 17 })),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("bob")),
                DeltaValue::from_json(json!({ "count": 1, "sum": 5 })),
                1,
            ),
        ]
    );
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 17);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(sums.value(1), 5);
}

#[test]
fn runtime_materializes_single_relation_having_count_distinct_function_view() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = RelationSchema {
        relation_id: "scores_distinct_having_by_user".to_string(),
        relation_name: "scores_distinct_having_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000024".to_string(),
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
                name: "distinct_scores".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    };
    let sql = "select user_id, sum(score) as sum, count(distinct score) as distinct_scores from scores group by user_id having count(distinct score) > 1";
    let identity = standing_identity_with_view(sql, "scores_distinct_having_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("alice", 10, 1),
                    ("alice", 7, 1),
                    ("bob", 5, 1),
                    ("bob", 5, 1),
                ])],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_distinct_having_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let distinct_scores = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 27);
    assert_eq!(distinct_scores.value(0), 2);
}

#[test]
fn runtime_rejects_non_contiguous_input_offsets_without_advancing_frontier() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let first_commit = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_eq!(first_commit.input_frontiers.len(), 1);
    assert_eq!(
        first_commit.input_frontiers[0].committed_offset_exclusive,
        3
    );
    let checkpoint_before_gap = runtime.checkpoint().unwrap();

    let sparse_error = runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 5,
                end_offset_exclusive: 6,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap_err();

    assert!(matches!(
        sparse_error,
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_frontier.offset_range"
        }
    ));
    assert_eq!(runtime.checkpoint().unwrap(), checkpoint_before_gap);

    runtime
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let err = runtime
        .apply_changes(
            4,
            EpochIdempotencyKey::new("epoch-4").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 7,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap_err();

    let checkpoint = runtime.checkpoint().unwrap();
    assert_eq!(checkpoint.input_frontiers.len(), 1);
    let frontier = &checkpoint.input_frontiers[0];
    assert_eq!(frontier.relation_id, catalog.relation_schema.relation_id);
    assert_eq!(
        frontier.relation_version,
        catalog.relation_schema.relation_version
    );
    assert_eq!(frontier.committed_offset_exclusive, 5);

    assert!(matches!(
        err,
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_frontier.offset_range"
        }
    ));
}

#[test]
fn runtime_tracks_input_frontiers_by_relation_stream_and_partition() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    for (epoch, stream_id) in [(1, "stream-a"), (2, "stream-b")] {
        runtime
            .apply_changes(
                epoch,
                EpochIdempotencyKey::new(format!("epoch-{epoch}")).unwrap(),
                vec![RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: catalog.relation_schema.relation_id.clone(),
                    relation_version: catalog.relation_schema.relation_version.clone(),
                    stream_id: stream_id.to_string(),
                    partition_id: 0,
                    schema_fingerprint: catalog.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 1,
                    event_time_watermark: None,
                    batches: vec![purchases_batch()],
                }],
            )
            .unwrap();
    }

    let checkpoint = runtime.checkpoint().unwrap();
    assert_eq!(checkpoint.input_frontiers.len(), 2);
    assert!(checkpoint.input_frontiers.iter().any(|frontier| {
        frontier.stream_id == "stream-a"
            && frontier.partition_id == 0
            && frontier.committed_offset_exclusive == 1
    }));
    assert!(checkpoint.input_frontiers.iter().any(|frontier| {
        frontier.stream_id == "stream-b"
            && frontier.partition_id == 0
            && frontier.committed_offset_exclusive == 1
    }));
}

#[test]
fn runtime_accepts_sparse_first_offset_for_new_stream_frontier() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let commit = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "sparse-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 42,
                end_offset_exclusive: 43,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_eq!(commit.input_frontiers.len(), 1);
    let frontier = &commit.input_frontiers[0];
    assert_eq!(frontier.stream_id, "sparse-stream");
    assert_eq!(frontier.partition_id, 0);
    assert_eq!(frontier.committed_offset_exclusive, 43);
}

#[test]
fn runtime_checkpoints_event_time_frontiers_by_source_partition() {
    let catalog = purchases_catalog_with_amount_event_time();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 7,
                    event_time_column_id: "amount".to_string(),
                    max_observed_event_time_ns: 1_700_000_000_000_000_100,
                    watermark_ns: 1_700_000_000_000_000_000,
                }),
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let commit = runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 6,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 7,
                    event_time_column_id: "amount".to_string(),
                    max_observed_event_time_ns: 1_700_000_000_000_000_300,
                    watermark_ns: 1_700_000_000_000_000_200,
                }),
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_eq!(commit.input_event_time_frontiers.len(), 1);
    let frontier = &commit.input_event_time_frontiers[0];
    assert_eq!(frontier.relation_id, "purchases");
    assert_eq!(frontier.stream_id, "purchases-stream");
    assert_eq!(frontier.partition_id, 7);
    assert_eq!(
        frontier.max_observed_event_time_ns,
        1_700_000_000_000_000_300
    );
    assert_eq!(frontier.watermark_ns, 1_700_000_000_000_000_200);

    let checkpoint = runtime.checkpoint().unwrap();
    assert_eq!(
        checkpoint.input_event_time_frontiers,
        commit.input_event_time_frontiers
    );
    let restored = restore_standing_runtime(checkpoint).unwrap();
    let restored_checkpoint = restored.checkpoint().unwrap();

    assert_eq!(
        restored_checkpoint.input_event_time_frontiers,
        commit.input_event_time_frontiers
    );
}

#[test]
fn runtime_rejects_non_monotonic_event_time_watermark_for_source_partition() {
    let catalog = purchases_catalog_with_amount_event_time();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 7,
                    event_time_column_id: "amount".to_string(),
                    max_observed_event_time_ns: 300,
                    watermark_ns: 200,
                }),
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let err = runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 6,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 7,
                    event_time_column_id: "amount".to_string(),
                    max_observed_event_time_ns: 350,
                    watermark_ns: 199,
                }),
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_event_time_watermark"
        }
    ));
}

#[test]
fn runtime_restore_rejects_malformed_event_time_frontier_even_when_payload_matches() {
    let catalog = purchases_catalog_with_amount_event_time();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 7,
                    event_time_column_id: "amount".to_string(),
                    max_observed_event_time_ns: 300,
                    watermark_ns: 200,
                }),
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let mut checkpoint = runtime.checkpoint().unwrap();
    checkpoint.input_event_time_frontiers[0].watermark_ns = 400;
    let state_payload = checkpoint.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    payload["input_event_time_frontiers"][0]["watermark_ns"] = json!(400);
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    checkpoint.state_root.content_hash = stable_bytes_hash(state_payload.payload.as_bytes());

    let err = match restore_standing_runtime(checkpoint) {
        Ok(_) => panic!("restore unexpectedly accepted malformed event-time frontier"),
        Err(error) => error,
    };

    assert!(
        err.contains("generic_checkpoint_payload"),
        "unexpected restore error: {err}"
    );
}

#[test]
fn runtime_creation_without_admitted_sql_fails_closed() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let identity = standing_identity(
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id",
    );

    let error = match create_standing_runtime(
        &identity,
        &catalog,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    ) {
        Ok(_) => panic!("runtime creation without admitted SQL unexpectedly succeeded"),
        Err(error) => error,
    };

    assert!(error.contains("requires admitted SQL and logical plan metadata"));
}

#[test]
fn runtime_materializes_avg_projection_aliases_for_relation_without_value_semantic_role() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_avg_output_schema();
    let sql = "select user_id, sum(amount) as total, count(*) as events, avg(amount) as average from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let totals = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let events = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let averages = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(1).name(), "total");
    assert_eq!(batch.schema().field(2).name(), "events");
    assert_eq!(batch.schema().field(3).name(), "average");
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(totals.value(0), 17);
    assert_eq!(events.value(0), 2);
    assert_eq!(averages.value(0), 8.5);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(totals.value(1), 5);
    assert_eq!(events.value(1), 1);
    assert_eq!(averages.value(1), 5.0);
}

#[test]
fn runtime_materializes_single_key_order_by_limit_top_k_and_restores_state() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id order by sum desc limit 1";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        std::slice::from_ref(&input_schema),
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();
    assert_top_purchase_user(runtime.as_ref(), 1, "alice", 17, 2);

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![purchases_rows_batch(&[("bob", 20, 1)])],
            }],
        )
        .unwrap();
    assert_top_purchase_user(runtime.as_ref(), 2, "bob", 25, 2);

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_top_purchase_user(restored.as_ref(), 2, "bob", 25, 2);
}

#[test]
fn runtime_materializes_single_key_order_by_limit_offset_top_k() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id order by sum desc limit 1 offset 1";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        std::slice::from_ref(&input_schema),
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();
    assert_top_purchase_user(runtime.as_ref(), 1, "bob", 5, 1);

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![purchases_rows_batch(&[("bob", 20, 1)])],
            }],
        )
        .unwrap();
    assert_top_purchase_user(runtime.as_ref(), 2, "alice", 17, 2);
}

#[test]
fn runtime_materializes_single_key_order_by_function_top_k() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_total_score_output_schema();
    let sql = "select user_id, sum(score) as total_score, count(*) as event_count from scores group by user_id order by sum(score) desc limit 1";
    let identity = standing_identity_with_view(sql, "scores_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        std::slice::from_ref(&input_schema),
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-top-k-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("alice", 7, 1),
                    ("bob", 5, 1),
                    ("carol", 20, 1),
                ])],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let totals = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let events = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(batch.schema().field(1).name(), "total_score");
    assert_eq!(batch.schema().field(2).name(), "event_count");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(user_ids.value(0), "carol");
    assert_eq!(totals.value(0), 20);
    assert_eq!(events.value(0), 1);
}

#[test]
fn runtime_materializes_single_key_order_by_metric_then_key_top_k() {
    let catalog = scores_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = scores_total_score_output_schema();
    let sql = "select user_id, sum(score) as total_score, count(*) as event_count from scores group by user_id order by sum(score) desc, user_id asc limit 1";
    let identity = standing_identity_with_view(sql, "scores_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        std::slice::from_ref(&input_schema),
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "scores-top-k-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("bob", 7, 1),
                    ("alice", 10, 1),
                    ("bob", 3, 1),
                    ("alice", 0, 1),
                    ("carol", 9, 1),
                ])],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let totals = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let events = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(totals.value(0), 10);
    assert_eq!(events.value(0), 2);
}

#[test]
fn runtime_materializes_decimal_avg_as_float64_output() {
    let catalog = purchases_decimal_amount_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_decimal_avg_output_schema();
    let sql = "select user_id, avg(amount) as average from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_decimal_amount_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let averages = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(1).name(), "average");
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(averages.value(0), 8.5);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(averages.value(1), 5.0);
}

#[test]
fn runtime_materializes_avg_arithmetic_expression() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_avg_output_schema();
    let sql = "select user_id, sum(amount) as total, count(*) as events, avg(amount + 1) as average from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let averages = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(averages.value(0), 9.5);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(averages.value(1), 6.0);
}

#[test]
fn runtime_materializes_filtered_projected_aggregate_view() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_avg_output_schema();
    let sql = "select user_id, sum(amount) as total, count(*) as events, avg(amount) as average from purchases where amount > 5 group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let totals = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let events = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let averages = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(1).name(), "total");
    assert_eq!(batch.schema().field(2).name(), "events");
    assert_eq!(batch.schema().field(3).name(), "average");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(totals.value(0), 17);
    assert_eq!(events.value(0), 2);
    assert_eq!(averages.value(0), 8.5);
}

#[test]
fn materialized_delta_to_page_paginates_manifest_output_when_serving_checkpoint_snapshot() {
    let output_schema = purchases_avg_output_schema();
    let published_output = DeltaBatch::from_records([
        DeltaRecord::new(
            DeltaKey::from_json(json!("bob")),
            DeltaValue::from_json(json!({"sum": 5, "count": 1})),
            1,
        ),
        DeltaRecord::new(
            DeltaKey::from_json(json!("alice")),
            DeltaValue::from_json(json!({"sum": 17, "count": 2})),
            1,
        ),
    ]);
    let aggregate_outputs = vec![
        SupportedAggregateOutput {
            function: LogicalPlanAggregateFunctionV1::Sum,
            input_column_id: Some("amount".to_string()),
            input_relation_side: None,
            input_expression: None,
            output_column_id: "total".to_string(),
        },
        SupportedAggregateOutput {
            function: LogicalPlanAggregateFunctionV1::Count,
            input_column_id: None,
            input_relation_side: None,
            input_expression: None,
            output_column_id: "events".to_string(),
        },
        SupportedAggregateOutput {
            function: LogicalPlanAggregateFunctionV1::Avg,
            input_column_id: Some("amount".to_string()),
            input_relation_side: None,
            input_expression: None,
            output_column_id: "average".to_string(),
        },
    ];
    let view = ScopedViewId {
        tenant_id: "tenant-a".to_string(),
        program_id: "program-purchases".to_string(),
        view_id: "purchases_by_user".to_string(),
    };

    let first_page = materialized_delta_to_page(
        &output_schema,
        &published_output,
        view.clone(),
        7,
        SnapshotPageRequest {
            committed_epoch: Some(7),
            page_token: None,
            max_rows: Some(1),
        },
        Some(&aggregate_outputs),
    )
    .unwrap();

    let first_batch = &first_page.batches[0];
    let first_user_ids = first_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let first_averages = first_batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(first_page.view, view);
    assert_eq!(first_page.logical_epoch, 7);
    assert_eq!(
        first_page.schema_fingerprint,
        output_schema.schema_fingerprint
    );
    assert_eq!(first_batch.num_rows(), 1);
    assert_eq!(first_user_ids.value(0), "alice");
    assert_eq!(first_averages.value(0), 8.5);
    assert_eq!(first_page.next_page_token.as_deref(), Some("\"alice\""));

    let second_page = materialized_delta_to_page(
        &output_schema,
        &published_output,
        view,
        7,
        SnapshotPageRequest {
            committed_epoch: Some(7),
            page_token: first_page.next_page_token,
            max_rows: Some(1),
        },
        Some(&aggregate_outputs),
    )
    .unwrap();
    let second_batch = &second_page.batches[0];
    let second_user_ids = second_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(second_batch.num_rows(), 1);
    assert_eq!(second_user_ids.value(0), "bob");
    assert_eq!(second_page.next_page_token, None);
}

#[test]
fn runtime_materializes_min_max_and_recomputes_after_extreme_delete() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_min_max_output_schema();
    let sql = "select user_id, min(amount) as smallest, max(amount) as largest from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();
    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![purchase_delete_batch("alice", 10)],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(2),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let smallest = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let largest = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(1).name(), "smallest");
    assert_eq!(batch.schema().field(2).name(), "largest");
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(smallest.value(0), 7);
    assert_eq!(largest.value(0), 7);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(smallest.value(1), 5);
    assert_eq!(largest.value(1), 5);
}

#[test]
fn runtime_materializes_min_max_arithmetic_expression() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_min_max_output_schema();
    let sql = "select user_id, min(amount + 1) as smallest, max(amount + 1) as largest from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let smallest = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let largest = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(smallest.value(0), 8);
    assert_eq!(largest.value(0), 11);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(smallest.value(1), 6);
    assert_eq!(largest.value(1), 6);
}

#[test]
fn runtime_restores_min_max_multiset_checkpoint_before_extreme_delete() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_min_max_output_schema();
    let sql = "select user_id, min(amount) as smallest, max(amount) as largest from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![purchase_delete_batch("alice", 10)],
            }],
        )
        .unwrap();

    let page = restored
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(2),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let smallest = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let largest = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(smallest.value(0), 7);
    assert_eq!(largest.value(0), 7);
}

#[test]
fn runtime_restored_query_reads_published_output_not_engine_state() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let mut checkpoint = runtime.checkpoint().unwrap();
    assert!(checkpoint.output_manifest_refs.is_empty());
    let state_payload = checkpoint.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    assert!(payload
        .get("published_output")
        .is_some_and(|published_output| !published_output.is_null()));
    payload["engine"]["state"]["records"] = json!([]);
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    checkpoint.state_root.content_hash = stable_bytes_hash(state_payload.payload.as_bytes());
    let restored = restore_standing_runtime(checkpoint).unwrap();

    let page = restored
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(user_ids.value(0), "alice");
    assert_eq!(sums.value(0), 17);
    assert_eq!(counts.value(0), 2);
    assert_eq!(user_ids.value(1), "bob");
    assert_eq!(sums.value(1), 5);
    assert_eq!(counts.value(1), 1);
}

#[test]
fn runtime_restore_without_admitted_plan_metadata_fails_closed() {
    let catalog = purchases_catalog_without_value_role();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_output_schema();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let identity = standing_identity(sql);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    let mut checkpoint = runtime.checkpoint().unwrap();
    let state_payload = checkpoint.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    payload.as_object_mut().unwrap().remove("plan");
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    checkpoint.state_root.content_hash = stable_bytes_hash(state_payload.payload.as_bytes());

    assert!(restore_standing_runtime(checkpoint).is_err());
}

#[test]
fn runtime_materializes_two_relation_join_and_restores_epoch_consistent_state() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let first_commit = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 5, 1),
                        ("alice", 7, 1),
                        ("charlie", 30, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2), ("bob", 5, 1)]);

    let initial_frontiers = vec![
        RelationFrontier {
            relation_id: accounts.relation_schema.relation_id.clone(),
            relation_version: accounts.relation_schema.relation_version.clone(),
            stream_id: "test-stream".to_string(),
            partition_id: 0,
            committed_offset_exclusive: 2,
        },
        RelationFrontier {
            relation_id: scores.relation_schema.relation_id.clone(),
            relation_version: scores.relation_schema.relation_version.clone(),
            stream_id: "test-stream".to_string(),
            partition_id: 0,
            committed_offset_exclusive: 4,
        },
    ];
    assert_eq!(first_commit.input_frontiers, initial_frontiers);

    let checkpoint = runtime.checkpoint().unwrap();
    assert_eq!(checkpoint.input_frontiers, initial_frontiers);
    let scalar_payload: Value =
        serde_json::from_str(&checkpoint.state_payload.as_ref().unwrap().payload).unwrap();
    assert!(scalar_payload.get("join_key_codec_id").is_none());
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    assert_eq!(
        restored.checkpoint().unwrap().input_frontiers,
        initial_frontiers
    );

    let second_commit = restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: scores.relation_schema.relation_id.clone(),
                relation_version: scores.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: scores.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![score_append_batch("alice", 3)],
            }],
        )
        .unwrap();

    assert_join_page(restored.as_ref(), 2, &[("alice", 20, 3), ("bob", 5, 1)]);

    let advanced_frontiers = vec![
        RelationFrontier {
            relation_id: accounts.relation_schema.relation_id.clone(),
            relation_version: accounts.relation_schema.relation_version.clone(),
            stream_id: "test-stream".to_string(),
            partition_id: 0,
            committed_offset_exclusive: 2,
        },
        RelationFrontier {
            relation_id: scores.relation_schema.relation_id.clone(),
            relation_version: scores.relation_schema.relation_version.clone(),
            stream_id: "test-stream".to_string(),
            partition_id: 0,
            committed_offset_exclusive: 5,
        },
    ];
    assert_eq!(second_commit.input_frontiers, advanced_frontiers);
    assert_eq!(
        restored.checkpoint().unwrap().input_frontiers,
        advanced_frontiers
    );
}

#[test]
fn runtime_materializes_composite_primary_key_join_across_retract_and_restart() {
    let (scores, accounts) = composite_join_catalogs();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = composite_join_output_schema();
    let sql = "select a.account_tenant_id as tenant_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id and s.tenant_id = a.account_tenant_id group by a.account_tenant_id";
    let identity = standing_identity_with_view(sql, "scores_by_tenant");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("composite-join-epoch-1").unwrap(),
            vec![
                relation_input(
                    &scores,
                    "composite-join-scores",
                    0,
                    3,
                    composite_scores_rows_batch(&[
                        ("alice", 10, "t1", 2),
                        ("bob", 5, "t1", 1),
                        ("alice", 7, "t2", 1),
                    ]),
                ),
                relation_input(
                    &accounts,
                    "composite-join-accounts",
                    0,
                    2,
                    composite_accounts_rows_batch(&[
                        ("alice", 100, "gold", "t1", 3),
                        ("bob", 50, "gold", "t1", 1),
                    ]),
                ),
            ],
        )
        .unwrap();
    assert_join_page_for_view(runtime.as_ref(), "scores_by_tenant", 1, &[("t1", 65, 7)]);

    let checkpoint = runtime.checkpoint().unwrap();
    let checkpoint_payload: Value =
        serde_json::from_str(&checkpoint.state_payload.as_ref().unwrap().payload).unwrap();
    assert_eq!(
        checkpoint_payload["join_key_codec_id"],
        "velorix-composite-pk-positional-json-array-join-key-v1"
    );
    for replacement in [None, Some("unknown-composite-key-codec-v9")] {
        let mut tampered = checkpoint.clone();
        let state_payload = tampered.state_payload.as_mut().unwrap();
        let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
        match replacement {
            Some(codec) => {
                payload["join_key_codec_id"] = Value::String(codec.into());
            }
            None => {
                payload.as_object_mut().unwrap().remove("join_key_codec_id");
            }
        }
        state_payload.payload = serde_json::to_string(&payload).unwrap();
        tampered.state_root.content_hash = stable_bytes_hash(state_payload.payload.as_bytes());
        assert!(restore_standing_runtime(tampered).is_err());
    }
    let mut tampered_binding = checkpoint.clone();
    let state_payload = tampered_binding.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    payload["execution_binding"]["implementation"]["join_key_codec_id"] =
        Value::String("unknown-composite-key-codec-v9".into());
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    tampered_binding.state_root.content_hash = stable_bytes_hash(state_payload.payload.as_bytes());
    assert!(restore_standing_runtime(tampered_binding).is_err());

    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    assert_join_page_for_view(restored.as_ref(), "scores_by_tenant", 1, &[("t1", 65, 7)]);

    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("composite-join-epoch-2").unwrap(),
            vec![
                relation_input(
                    &scores,
                    "composite-join-scores",
                    3,
                    4,
                    composite_scores_rows_batch(&[("alice", 10, "t1", -1)]),
                ),
                relation_input(
                    &accounts,
                    "composite-join-accounts",
                    2,
                    3,
                    composite_accounts_rows_batch(&[("alice", 100, "gold", "t2", 2)]),
                ),
            ],
        )
        .unwrap();
    assert_join_page_for_view(
        restored.as_ref(),
        "scores_by_tenant",
        2,
        &[("t1", 35, 4), ("t2", 14, 2)],
    );

    restored
        .apply_changes(
            3,
            EpochIdempotencyKey::new("composite-join-epoch-3").unwrap(),
            vec![relation_input(
                &accounts,
                "composite-join-accounts",
                3,
                4,
                composite_accounts_rows_batch(&[("alice", 100, "gold", "t1", -3)]),
            )],
        )
        .unwrap();
    assert_join_page_for_view(
        restored.as_ref(),
        "scores_by_tenant",
        3,
        &[("t1", 5, 1), ("t2", 14, 2)],
    );
}

#[test]
fn runtime_materializes_three_input_composite_pk_join_through_binary_dag() {
    let [scores, accounts, profiles] = three_input_composite_join_catalogs();
    let catalogs = vec![scores.clone(), accounts.clone(), profiles.clone()];
    let input_schemas = catalogs
        .iter()
        .map(|catalog| catalog_input_relation_schema(catalog).unwrap())
        .collect::<Vec<_>>();
    let output_schema = three_input_join_count_output_schema();
    let sql = "select s.tenant_id, s.user_id, count(*) as count from scores s join accounts a on s.tenant_id = a.account_tenant_id and s.user_id = a.account_id join profiles p on s.tenant_id = p.account_tenant_id and s.user_id = p.account_id group by s.tenant_id, s.user_id";
    let identity = standing_identity_with_view(sql, "three_input_counts");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &catalogs,
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("three-input-epoch-1").unwrap(),
            vec![
                relation_input(
                    &scores,
                    "three-input-scores",
                    0,
                    2,
                    composite_scores_rows_batch(&[("alice", 10, "t1", 2), ("bob", 5, "t1", 1)]),
                ),
                relation_input(
                    &accounts,
                    "three-input-accounts",
                    0,
                    2,
                    composite_accounts_rows_batch(&[
                        ("alice", 100, "gold", "t1", 3),
                        ("bob", 50, "silver", "t1", 1),
                    ]),
                ),
                relation_input(
                    &profiles,
                    "three-input-profiles",
                    0,
                    1,
                    composite_accounts_rows_batch(&[("alice", 0, "active", "t1", 4)]),
                ),
            ],
        )
        .unwrap();
    assert_three_input_count_page(runtime.as_ref(), 1, &[("t1", "alice", 24)]);

    let checkpoint = runtime.checkpoint().unwrap();
    assert_eq!(checkpoint.input_frontiers.len(), 3);
    let payload: Value =
        serde_json::from_str(&checkpoint.state_payload.as_ref().unwrap().payload).unwrap();
    assert_eq!(
        payload["logical_plan"]["execution_implementation"]["join_key_codec_id"],
        COMPOSITE_PK_POSITIONAL_JSON_ARRAY_JOIN_KEY_CODEC_V1
    );
    assert_eq!(payload["graph"]["operators"].as_array().unwrap().len(), 4);

    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    assert_three_input_count_page(restored.as_ref(), 1, &[("t1", "alice", 24)]);
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("three-input-epoch-2").unwrap(),
            vec![relation_input(
                &scores,
                "three-input-scores",
                2,
                3,
                composite_scores_rows_batch(&[("alice", 10, "t1", -1)]),
            )],
        )
        .unwrap();
    assert_three_input_count_page(restored.as_ref(), 2, &[("t1", "alice", 12)]);
    restored
        .apply_changes(
            3,
            EpochIdempotencyKey::new("three-input-epoch-3").unwrap(),
            vec![relation_input(
                &accounts,
                "three-input-accounts",
                2,
                3,
                composite_accounts_rows_batch(&[("alice", 100, "gold", "t1", -1)]),
            )],
        )
        .unwrap();
    assert_three_input_count_page(restored.as_ref(), 3, &[("t1", "alice", 8)]);
    restored
        .apply_changes(
            4,
            EpochIdempotencyKey::new("three-input-epoch-4").unwrap(),
            vec![relation_input(
                &profiles,
                "three-input-profiles",
                1,
                2,
                composite_accounts_rows_batch(&[("alice", 0, "active", "t1", 1)]),
            )],
        )
        .unwrap();
    assert_three_input_count_page(restored.as_ref(), 4, &[("t1", "alice", 10)]);
}

#[test]
fn three_input_join_order_policy_preserves_results_state_and_legacy_restore() {
    let [scores, accounts, profiles] = three_input_composite_join_catalogs();
    let catalogs = vec![scores.clone(), accounts.clone(), profiles.clone()];
    let input_schemas = catalogs
        .iter()
        .map(|catalog| catalog_input_relation_schema(catalog).unwrap())
        .collect::<Vec<_>>();
    let output_schema = three_input_join_count_output_schema();
    let sql = "select s.tenant_id, s.user_id, count(*) as count from scores s join accounts a on s.tenant_id = a.account_tenant_id and s.user_id = a.account_id join profiles p on s.tenant_id = p.account_tenant_id and s.user_id = p.account_id group by s.tenant_id, s.user_id";
    let reordered_sql = "select s.tenant_id, s.user_id, count(*) as count from scores s join profiles p on s.tenant_id = p.account_tenant_id and s.user_id = p.account_id join accounts a on s.tenant_id = a.account_tenant_id and s.user_id = a.account_id group by s.tenant_id, s.user_id";
    let changes = || {
        vec![
            relation_input(
                &scores,
                "join-order-scores",
                0,
                2,
                composite_scores_rows_batch(&[("alice", 10, "t1", 2), ("bob", 5, "t1", 1)]),
            ),
            relation_input(
                &accounts,
                "join-order-accounts",
                0,
                2,
                composite_accounts_rows_batch(&[
                    ("alice", 100, "gold", "t1", 3),
                    ("bob", 50, "silver", "t1", 1),
                ]),
            ),
            relation_input(
                &profiles,
                "join-order-profiles",
                0,
                1,
                composite_accounts_rows_batch(&[("alice", 0, "active", "t1", 4)]),
            ),
        ]
    };

    let mut canonical = create_standing_runtime_with_sql_and_catalogs(
        &standing_identity_with_view(sql, "three_input_counts"),
        &catalogs,
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();
    let mut reordered = create_standing_runtime_with_sql_and_catalogs(
        &standing_identity_with_view(reordered_sql, "three_input_counts"),
        &catalogs,
        reordered_sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();
    canonical
        .apply_changes(
            1,
            EpochIdempotencyKey::new("join-order-canonical").unwrap(),
            changes(),
        )
        .unwrap();
    reordered
        .apply_changes(
            1,
            EpochIdempotencyKey::new("join-order-reordered").unwrap(),
            changes(),
        )
        .unwrap();
    assert_three_input_count_page(canonical.as_ref(), 1, &[("t1", "alice", 24)]);
    assert_three_input_count_page(reordered.as_ref(), 1, &[("t1", "alice", 24)]);
    let canonical_checkpoint = canonical.checkpoint().unwrap();
    let reordered_checkpoint = reordered.checkpoint().unwrap();
    let canonical_payload: Value =
        serde_json::from_str(&canonical_checkpoint.state_payload.as_ref().unwrap().payload)
            .unwrap();
    let reordered_payload: Value =
        serde_json::from_str(&reordered_checkpoint.state_payload.as_ref().unwrap().payload)
            .unwrap();
    assert_eq!(canonical_payload["graph"], reordered_payload["graph"]);
    assert_eq!(
        canonical_payload["published_output"],
        reordered_payload["published_output"]
    );
    assert_three_input_count_page(
        restore_standing_runtime(canonical_checkpoint)
            .unwrap()
            .as_ref(),
        1,
        &[("t1", "alice", 24)],
    );
    assert_three_input_count_page(
        restore_standing_runtime(reordered_checkpoint)
            .unwrap()
            .as_ref(),
        1,
        &[("t1", "alice", 24)],
    );

    let legacy_plan = lower_supported_three_input_inner_join_count_sql_to_logical_plan_with_policy(
        sql,
        &catalogs,
        &output_schema,
        THREE_INPUT_LEGACY_SQL_ENCOUNTER_JOIN_ORDER_V1,
    )
    .unwrap();
    let legacy_identity = standing_identity_with_view(sql, "three_input_counts");
    let mut legacy = create_standing_runtime_with_logical_plan_and_catalogs(
        &legacy_identity,
        &catalogs,
        legacy_plan,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();
    legacy
        .apply_changes(
            1,
            EpochIdempotencyKey::new("join-order-legacy").unwrap(),
            changes(),
        )
        .unwrap();
    let restored_legacy = restore_standing_runtime(legacy.checkpoint().unwrap()).unwrap();
    assert_three_input_count_page(restored_legacy.as_ref(), 1, &[("t1", "alice", 24)]);
}

#[test]
fn three_input_join_epoch_rolls_back_on_overflow_and_restore_rejects_torn_checkpoint() {
    let [scores, accounts, profiles] = three_input_composite_join_catalogs();
    let catalogs = vec![scores.clone(), accounts.clone(), profiles.clone()];
    let input_schemas = catalogs
        .iter()
        .map(|catalog| catalog_input_relation_schema(catalog).unwrap())
        .collect::<Vec<_>>();
    let output_schema = three_input_join_count_output_schema();
    let sql = "select s.tenant_id, s.user_id, count(*) as count from scores s join accounts a on s.tenant_id = a.account_tenant_id and s.user_id = a.account_id join profiles p on s.tenant_id = p.account_tenant_id and s.user_id = p.account_id group by s.tenant_id, s.user_id";
    let identity = standing_identity_with_view(sql, "three_input_counts");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &catalogs,
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();
    let empty = runtime.checkpoint().unwrap();

    let overflow = runtime.apply_changes(
        1,
        EpochIdempotencyKey::new("three-input-overflow").unwrap(),
        vec![
            relation_input(
                &scores,
                "three-input-scores",
                0,
                1,
                composite_scores_rows_batch(&[("alice", 10, "t1", i64::MAX)]),
            ),
            relation_input(
                &accounts,
                "three-input-accounts",
                0,
                1,
                composite_accounts_rows_batch(&[("alice", 100, "gold", "t1", 1)]),
            ),
            relation_input(
                &profiles,
                "three-input-profiles",
                0,
                1,
                composite_accounts_rows_batch(&[("alice", 0, "active", "t1", 2)]),
            ),
        ],
    );
    assert!(overflow.is_err());
    assert_eq!(runtime.checkpoint().unwrap(), empty);

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("three-input-safe").unwrap(),
            vec![
                relation_input(
                    &scores,
                    "three-input-scores",
                    0,
                    1,
                    composite_scores_rows_batch(&[("alice", 10, "t1", 1)]),
                ),
                relation_input(
                    &accounts,
                    "three-input-accounts",
                    0,
                    1,
                    composite_accounts_rows_batch(&[("alice", 100, "gold", "t1", 1)]),
                ),
                relation_input(
                    &profiles,
                    "three-input-profiles",
                    0,
                    1,
                    composite_accounts_rows_batch(&[("alice", 0, "active", "t1", 1)]),
                ),
            ],
        )
        .unwrap();
    assert_three_input_count_page(runtime.as_ref(), 1, &[("t1", "alice", 1)]);

    let committed = runtime.checkpoint().unwrap();
    let gap = runtime.apply_changes(
        2,
        EpochIdempotencyKey::new("three-input-gap").unwrap(),
        vec![relation_input(
            &profiles,
            "three-input-profiles",
            2,
            3,
            composite_accounts_rows_batch(&[("alice", 0, "active", "t1", 1)]),
        )],
    );
    assert!(gap.is_err());
    assert_eq!(runtime.checkpoint().unwrap(), committed);

    let mut torn_output = committed.clone();
    let state_payload = torn_output.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    payload["published_output"] = json!({"records": []});
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    assert!(restore_standing_runtime(torn_output).is_err());

    let mut torn_epoch = committed;
    let state_payload = torn_epoch.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    payload["graph"]["logical_epoch"] = json!(2);
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    assert!(restore_standing_runtime(torn_epoch).is_err());

    let mut unknown_policy = runtime.checkpoint().unwrap();
    let state_payload = unknown_policy.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    payload["logical_plan"]["execution"]["plan"]["join_order_policy_id"] =
        json!("unknown-three-input-policy");
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    assert!(restore_standing_runtime(unknown_policy).is_err());
}

fn assert_three_input_count_page(
    runtime: &(dyn StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, &str, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".into(),
                program_id: "program-purchases".into(),
                view_id: "three_input_counts".into(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let tenants = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let users = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let actual = (0..batch.num_rows())
        .map(|index| {
            (
                tenants.value(index),
                users.value(index),
                counts.value(index),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn runtime_materializes_non_primary_duplicate_join_across_retract_and_restart() {
    let scores = generic_adapter_catalog(scores_catalog());
    let accounts = generic_adapter_catalog(accounts_catalog());
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = non_primary_join_output_schema();
    let sql = "select a.limit as bucket, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.score = a.limit group by a.limit";
    let identity = standing_identity_with_view(sql, "scores_by_bucket");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("non-primary-join-epoch-1").unwrap(),
            vec![
                relation_input(
                    &scores,
                    "non-primary-join-scores",
                    0,
                    3,
                    scores_rows_batch(&[("left-a", 10, 2), ("left-b", 10, 1), ("left-c", 5, 1)]),
                ),
                relation_input(
                    &accounts,
                    "non-primary-join-accounts",
                    0,
                    3,
                    accounts_rows_batch(&[
                        ("right-a", 10, "gold", 3),
                        ("right-b", 10, "silver", 1),
                        ("right-c", 5, "gold", 1),
                    ]),
                ),
            ],
        )
        .unwrap();
    assert_int_join_page_for_view(
        runtime.as_ref(),
        "scores_by_bucket",
        1,
        &[(10, 120, 12), (5, 5, 1)],
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let payload: Value =
        serde_json::from_str(&checkpoint.state_payload.as_ref().unwrap().payload).unwrap();
    assert_eq!(
        payload["join_key_codec_id"],
        "velorix-non-primary-non-null-scalar-join-key-v1"
    );
    let mut wrong_domain = checkpoint.clone();
    let state_payload = wrong_domain.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    payload["join_key_codec_id"] =
        Value::String("velorix-composite-pk-positional-json-array-join-key-v1".into());
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    wrong_domain.state_root.content_hash = stable_bytes_hash(state_payload.payload.as_bytes());
    assert!(restore_standing_runtime(wrong_domain).is_err());

    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    assert_int_join_page_for_view(
        restored.as_ref(),
        "scores_by_bucket",
        1,
        &[(10, 120, 12), (5, 5, 1)],
    );
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("non-primary-join-epoch-2").unwrap(),
            vec![
                relation_input(
                    &scores,
                    "non-primary-join-scores",
                    3,
                    4,
                    scores_rows_batch(&[("left-a", 10, -1)]),
                ),
                relation_input(
                    &accounts,
                    "non-primary-join-accounts",
                    3,
                    6,
                    accounts_rows_batch(&[
                        ("right-a", 10, "gold", -2),
                        ("right-d", 5, "bronze", 2),
                    ]),
                ),
            ],
        )
        .unwrap();
    assert_int_join_page_for_view(
        restored.as_ref(),
        "scores_by_bucket",
        2,
        &[(10, 40, 4), (5, 15, 3)],
    );
}

#[test]
fn runtime_materializes_atomic_self_join_fanout_across_retract_and_restart() {
    let scores = generic_adapter_catalog(scores_catalog());
    let input_schema = catalog_input_relation_schema(&scores).unwrap();
    let output_schema = global_count_output_schema();
    let sql = "select count(*) as count from scores l join scores r on l.score = r.score";
    let identity = standing_identity_with_view(sql, "score_self_join_count");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&scores),
        sql,
        std::slice::from_ref(&input_schema),
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let empty = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: identity.tenant_id.clone(),
                program_id: identity.program_id.clone(),
                view_id: identity.view_ids[0].clone(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(0),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    assert_global_count_batch(&empty.batches[0], 0);

    let before_failed_fanout = runtime.checkpoint().unwrap();
    assert!(runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("self-join-failed-fanout").unwrap(),
            vec![relation_input(
                &scores,
                "self-join-scores",
                0,
                1,
                scores_rows_batch(&[("overflow", 99, i64::MAX)]),
            )],
        )
        .is_err());
    let after_failed_fanout = runtime.checkpoint().unwrap();
    assert_eq!(
        before_failed_fanout.state_payload,
        after_failed_fanout.state_payload
    );
    assert_eq!(
        before_failed_fanout.input_frontiers,
        after_failed_fanout.input_frontiers
    );
    assert_eq!(runtime.logical_epoch(), 0);

    let inserted = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("self-join-epoch-1").unwrap(),
            vec![relation_input(
                &scores,
                "self-join-scores",
                0,
                4,
                scores_rows_batch(&[("left-a", 10, 2), ("left-b", 10, 1), ("left-c", 5, 1)]),
            )],
        )
        .unwrap();
    assert_global_count_batch(&inserted.output_batches[0].batches[0], 10);
    assert_eq!(inserted.input_frontiers.len(), 1);

    let checkpoint = runtime.checkpoint().unwrap();
    let payload: Value =
        serde_json::from_str(&checkpoint.state_payload.as_ref().unwrap().payload).unwrap();
    assert_eq!(payload["input_schemas"].as_array().unwrap().len(), 1);
    assert_eq!(
        payload["execution_binding"]["implementation"]["input_fanout_protocol_id"],
        "velorix-self-join-left-then-right-atomic-fanout-v1"
    );
    assert!(!payload["left_state"]["records"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(!payload["right_state"]["records"]
        .as_array()
        .unwrap()
        .is_empty());
    let mut restored = restore_standing_runtime(checkpoint).unwrap();

    let retracted = restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("self-join-epoch-2").unwrap(),
            vec![relation_input(
                &scores,
                "self-join-scores",
                4,
                5,
                scores_rows_batch(&[("left-a", 10, -1)]),
            )],
        )
        .unwrap();
    assert_global_count_batch(&retracted.output_batches[0].batches[0], 5);

    let emptied = restored
        .apply_changes(
            3,
            EpochIdempotencyKey::new("self-join-epoch-3").unwrap(),
            vec![relation_input(
                &scores,
                "self-join-scores",
                5,
                8,
                scores_rows_batch(&[("left-a", 10, -1), ("left-b", 10, -1), ("left-c", 5, -1)]),
            )],
        )
        .unwrap();
    assert_global_count_batch(&emptied.output_batches[0].batches[0], 0);
}

#[test]
fn inner_aggregate_join_specialization_matches_native_dag_delta_state_and_restart() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut uninterrupted_runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();
    let mut uninterrupted_graph = native_inner_join_aggregate_graph();

    let first_scores = scores_rows_batch(&[
        ("alice", 10, 1),
        ("alice", 7, 1),
        ("bob", 5, 1),
        ("charlie", 30, 1),
    ]);
    let first_accounts = accounts_alice_bob_batch();
    let specialized = uninterrupted_runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("equivalence-inner-1").unwrap(),
            vec![
                join_relation_input(&scores, 0, 4, vec![first_scores.clone()]),
                join_relation_input(&accounts, 0, 2, vec![first_accounts.clone()]),
            ],
        )
        .unwrap();
    let generic = apply_native_join_epoch(
        &mut uninterrupted_graph,
        1,
        native_join_input(&scores, "user_id", "score", &[first_scores]),
        native_join_input(&accounts, "account_id", "limit", &[first_accounts]),
    );
    assert!(
        specialization_delta_difference(&specialized.output_deltas[0].delta, &generic).is_none()
    );
    assert_inner_join_specialization_state_equivalent(
        uninterrupted_runtime.as_ref(),
        &uninterrupted_graph,
    );

    let mut restored_runtime =
        restore_standing_runtime(uninterrupted_runtime.checkpoint().unwrap()).unwrap();
    let graph_checkpoint = uninterrupted_graph.checkpoint().unwrap();
    let mut restored_graph = native_inner_join_aggregate_graph();
    restored_graph.restore(&graph_checkpoint).unwrap();

    let changed_scores = scores_rows_batch(&[("alice", 7, -1), ("charlie", 30, -1), ("bob", 2, 1)]);
    let changed_accounts =
        accounts_rows_batch(&[("charlie", 90, "silver", 1), ("bob", 50, "gold", -1)]);
    let runtime_tail = vec![
        join_relation_input(&scores, 4, 7, vec![changed_scores.clone()]),
        join_relation_input(&accounts, 2, 4, vec![changed_accounts.clone()]),
    ];
    let graph_left = native_join_input(&scores, "user_id", "score", &[changed_scores]);
    let graph_right = native_join_input(&accounts, "account_id", "limit", &[changed_accounts]);
    let uninterrupted_specialized = uninterrupted_runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("equivalence-inner-2").unwrap(),
            runtime_tail.clone(),
        )
        .unwrap();
    let restored_specialized = restored_runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("equivalence-inner-2").unwrap(),
            runtime_tail,
        )
        .unwrap();
    let uninterrupted_generic = apply_native_join_epoch(
        &mut uninterrupted_graph,
        2,
        graph_left.clone(),
        graph_right.clone(),
    );
    let restored_generic = apply_native_join_epoch(&mut restored_graph, 2, graph_left, graph_right);

    for delta in [
        &restored_specialized.output_deltas[0].delta,
        &uninterrupted_generic,
        &restored_generic,
    ] {
        assert!(specialization_delta_difference(
            &uninterrupted_specialized.output_deltas[0].delta,
            delta,
        )
        .is_none());
    }
    assert_eq!(
        restored_runtime.checkpoint().unwrap(),
        uninterrupted_runtime.checkpoint().unwrap()
    );
    assert_eq!(
        restored_graph.checkpoint().unwrap(),
        uninterrupted_graph.checkpoint().unwrap()
    );
    assert_inner_join_specialization_state_equivalent(
        uninterrupted_runtime.as_ref(),
        &uninterrupted_graph,
    );
    assert_inner_join_specialization_state_equivalent(restored_runtime.as_ref(), &restored_graph);
}

#[test]
fn narrow_left_join_specialization_matches_native_dag_delta_state_and_restart() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select s.user_id as account_id, sum(s.score) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id group by s.user_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut uninterrupted_runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();
    let mut uninterrupted_graph = native_left_join_aggregate_graph();

    let first_scores = scores_rows_batch(&[
        ("alice", 10, 1),
        ("alice", 7, 1),
        ("bob", 5, 1),
        ("charlie", 30, 1),
    ]);
    let first_accounts = accounts_alice_bob_batch();
    let specialized = uninterrupted_runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("equivalence-left-1").unwrap(),
            vec![
                join_relation_input(&scores, 0, 4, vec![first_scores.clone()]),
                join_relation_input(&accounts, 0, 2, vec![first_accounts.clone()]),
            ],
        )
        .unwrap();
    let generic = apply_native_join_epoch(
        &mut uninterrupted_graph,
        1,
        native_join_input(&scores, "user_id", "score", &[first_scores]),
        native_join_input(&accounts, "account_id", "limit", &[first_accounts]),
    );
    assert!(
        specialization_delta_difference(&specialized.output_deltas[0].delta, &generic).is_none()
    );
    assert_left_join_specialization_state_equivalent(
        uninterrupted_runtime.as_ref(),
        &uninterrupted_graph,
    );

    let mut restored_runtime =
        restore_standing_runtime(uninterrupted_runtime.checkpoint().unwrap()).unwrap();
    let graph_checkpoint = uninterrupted_graph.checkpoint().unwrap();
    let mut restored_graph = native_left_join_aggregate_graph();
    restored_graph.restore(&graph_checkpoint).unwrap();

    let changed_scores = scores_rows_batch(&[
        ("alice", 7, -1),
        ("bob", 5, -1),
        ("bob", 8, 1),
        ("dora", 4, 1),
    ]);
    let changed_accounts =
        accounts_rows_batch(&[("charlie", 90, "silver", 1), ("alice", 100, "gold", -1)]);
    let runtime_tail = vec![
        join_relation_input(&scores, 4, 8, vec![changed_scores.clone()]),
        join_relation_input(&accounts, 2, 4, vec![changed_accounts.clone()]),
    ];
    let graph_left = native_join_input(&scores, "user_id", "score", &[changed_scores]);
    let graph_right = native_join_input(&accounts, "account_id", "limit", &[changed_accounts]);
    let uninterrupted_specialized = uninterrupted_runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("equivalence-left-2").unwrap(),
            runtime_tail.clone(),
        )
        .unwrap();
    let restored_specialized = restored_runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("equivalence-left-2").unwrap(),
            runtime_tail,
        )
        .unwrap();
    let uninterrupted_generic = apply_native_join_epoch(
        &mut uninterrupted_graph,
        2,
        graph_left.clone(),
        graph_right.clone(),
    );
    let restored_generic = apply_native_join_epoch(&mut restored_graph, 2, graph_left, graph_right);

    for delta in [
        &restored_specialized.output_deltas[0].delta,
        &uninterrupted_generic,
        &restored_generic,
    ] {
        assert!(specialization_delta_difference(
            &uninterrupted_specialized.output_deltas[0].delta,
            delta,
        )
        .is_none());
    }
    assert_eq!(
        restored_runtime.checkpoint().unwrap(),
        uninterrupted_runtime.checkpoint().unwrap()
    );
    assert_eq!(
        restored_graph.checkpoint().unwrap(),
        uninterrupted_graph.checkpoint().unwrap()
    );
    assert_left_join_specialization_state_equivalent(
        uninterrupted_runtime.as_ref(),
        &uninterrupted_graph,
    );
    assert_left_join_specialization_state_equivalent(restored_runtime.as_ref(), &restored_graph);
}

#[test]
fn specialization_equivalence_harness_detects_a_mutated_delta() {
    let correct = DeltaBatch::from_records([DeltaRecord::new(
        DeltaKey::from_json(json!("alice")),
        DeltaValue::from_json(json!({ "sum": 10, "count": 1 })),
        1,
    )]);
    let mutant = DeltaBatch::from_records([DeltaRecord::new(
        DeltaKey::from_json(json!("alice")),
        DeltaValue::from_json(json!({ "sum": 11, "count": 1 })),
        1,
    )]);
    assert!(specialization_delta_difference(&correct, &mutant).is_some());
}

#[test]
fn retained_join_specializations_toggle_to_common_dag_with_equivalent_recovery() {
    let mut full_join_output_schema = join_output_schema();
    full_join_output_schema.columns[1].nullable = true;
    let cases = [
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
            join_output_schema(),
            "scores_by_account",
        ),
        (
            "select s.user_id as account_id, sum(s.score) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id group by s.user_id",
            join_output_schema(),
            "scores_by_account_left",
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count, count(a.limit) as count_limit, count(distinct a.limit) as distinct_limits, sum(a.limit) as limit_sum, min(a.limit) as min_limit, max(a.limit) as max_limit, avg(a.limit) as avg_limit from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
            join_right_stats_output_schema(),
            "scores_by_account_limits",
        ),
        (
            "select coalesce(s.user_id, a.account_id) as account_id, sum(s.score) as sum, count(*) as count from scores s full outer join accounts a on s.user_id = a.account_id group by coalesce(s.user_id, a.account_id)",
            full_join_output_schema,
            "scores_by_account",
        ),
    ];

    for (sql, output_schema, view_id) in cases {
        let scores = scores_catalog();
        let accounts = accounts_catalog();
        let catalogs = vec![scores.clone(), accounts.clone()];
        let input_schemas = vec![
            catalog_input_relation_schema(&scores).unwrap(),
            catalog_input_relation_schema(&accounts).unwrap(),
        ];
        let identity = standing_identity_with_view(sql, view_id);
        let logical_plan =
            lower_supported_join_view_sql_to_logical_plan(sql, &catalogs, &output_schema).unwrap();
        let selected_binding =
            bind_join_execution_v1(&logical_plan, JoinExecutionModeV1::SelectedSpecialization)
                .unwrap();
        let reference_binding =
            bind_join_execution_v1(&logical_plan, JoinExecutionModeV1::CommonDagReference).unwrap();
        assert_eq!(
            selected_binding.common_logical_dag_hash,
            reference_binding.common_logical_dag_hash
        );
        assert_ne!(
            selected_binding.implementation.implementation_id,
            reference_binding.implementation.implementation_id
        );
        assert_ne!(
            selected_binding.implementation.physical_operator_dag_hash,
            reference_binding.implementation.physical_operator_dag_hash
        );
        assert_eq!(
            selected_binding.implementation.output_codec_id,
            reference_binding.implementation.output_codec_id
        );
        assert_eq!(
            selected_binding
                .implementation
                .output_publication_protocol_id,
            reference_binding
                .implementation
                .output_publication_protocol_id
        );

        let mut selected = create_standing_runtime_with_logical_plan_and_catalogs(
            &identity,
            &catalogs,
            logical_plan.clone(),
            &input_schemas,
            std::slice::from_ref(&output_schema),
        )
        .unwrap();
        let mut reference =
            create_common_dag_reference_standing_runtime_with_logical_plan_and_catalogs(
                &identity,
                &catalogs,
                logical_plan,
                &input_schemas,
                std::slice::from_ref(&output_schema),
            )
            .unwrap();

        let first = vec![
            join_relation_input(
                &scores,
                0,
                4,
                vec![scores_rows_batch(&[
                    ("alice", 10, 1),
                    ("alice", 7, 1),
                    ("bob", 5, 1),
                    ("charlie", 30, 1),
                ])],
            ),
            join_relation_input(&accounts, 0, 2, vec![accounts_alice_bob_batch()]),
        ];
        assert_join_runtime_epoch_equivalent(&mut selected, &mut reference, 1, first);

        let changed = vec![
            join_relation_input(
                &scores,
                4,
                7,
                vec![scores_rows_batch(&[
                    ("alice", 7, -1),
                    ("bob", 5, -1),
                    ("bob", 8, 1),
                ])],
            ),
            join_relation_input(
                &accounts,
                2,
                5,
                vec![accounts_rows_batch(&[
                    ("alice", 100, "gold", -1),
                    ("alice", 80, "silver", 1),
                    ("charlie", 90, "silver", 1),
                ])],
            ),
        ];
        assert_join_runtime_epoch_equivalent(&mut selected, &mut reference, 2, changed);

        let selected_checkpoint = selected.checkpoint().unwrap();
        let reference_checkpoint = reference.checkpoint().unwrap();
        assert_join_checkpoint_canonical_equivalent(&selected_checkpoint, &reference_checkpoint);
        assert!(TwoInputJoinRuntime::restore(reference_checkpoint.clone()).is_err());
        assert!(
            restore_common_dag_reference_standing_runtime(selected_checkpoint.clone()).is_err()
        );
        let automatically_restored_reference =
            restore_standing_runtime(reference_checkpoint.clone()).unwrap();
        assert_join_checkpoint_canonical_equivalent(
            &selected_checkpoint,
            &automatically_restored_reference.checkpoint().unwrap(),
        );
        drop(selected);
        drop(reference);
        let mut selected = restore_standing_runtime(selected_checkpoint).unwrap();
        let mut reference =
            restore_common_dag_reference_standing_runtime(reference_checkpoint).unwrap();
        assert_join_checkpoint_canonical_equivalent(
            &selected.checkpoint().unwrap(),
            &reference.checkpoint().unwrap(),
        );

        let tail = vec![
            join_relation_input(
                &scores,
                7,
                9,
                vec![scores_rows_batch(&[("charlie", 30, -1), ("dora", 4, 1)])],
            ),
            join_relation_input(
                &accounts,
                5,
                7,
                vec![accounts_rows_batch(&[
                    ("charlie", 90, "silver", -1),
                    ("bob", 50, "gold", -1),
                ])],
            ),
        ];
        assert_join_runtime_epoch_equivalent(&mut selected, &mut reference, 3, tail);
        assert_join_checkpoint_canonical_equivalent(
            &selected.checkpoint().unwrap(),
            &reference.checkpoint().unwrap(),
        );
    }
}

#[test]
fn general_aggregate_join_specialization_matches_native_dag_state_and_restart() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let catalogs = vec![scores.clone(), accounts.clone()];
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_right_stats_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count, count(a.limit) as count_limit, count(distinct a.limit) as distinct_limits, sum(a.limit) as limit_sum, min(a.limit) as min_limit, max(a.limit) as max_limit, avg(a.limit) as avg_limit from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account_limits");
    let logical_plan =
        lower_supported_join_view_sql_to_logical_plan(sql, &catalogs, &output_schema).unwrap();
    let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan } = &logical_plan.execution
    else {
        panic!("expected join execution");
    };
    let supported_plan = plan.as_ref().clone();
    let mut uninterrupted_runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        &catalogs,
        logical_plan,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();
    let mut uninterrupted_graph = JoinSpecializationComparisonGraph::new(
        catalogs.clone(),
        supported_plan.clone(),
        output_schema.clone(),
    )
    .unwrap();

    let first_inputs = vec![
        join_relation_input(
            &scores,
            0,
            3,
            vec![scores_rows_batch(&[
                ("alice", 10, 1),
                ("bob", 5, 1),
                ("alice", 7, 1),
            ])],
        ),
        join_relation_input(&accounts, 0, 2, vec![accounts_alice_bob_batch()]),
    ];
    let specialized = uninterrupted_runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("equivalence-general-1").unwrap(),
            first_inputs.clone(),
        )
        .unwrap();
    let generic = uninterrupted_graph.apply_epoch(1, first_inputs).unwrap();
    assert!(
        specialization_delta_difference(&specialized.output_deltas[0].delta, &generic).is_none()
    );
    assert_general_join_specialization_state_equivalent(
        uninterrupted_runtime.as_ref(),
        &uninterrupted_graph,
    );

    let mut restored_runtime =
        restore_standing_runtime(uninterrupted_runtime.checkpoint().unwrap()).unwrap();
    let graph_checkpoint = uninterrupted_graph.checkpoint().unwrap();
    let mut restored_graph = JoinSpecializationComparisonGraph::restore(
        catalogs,
        supported_plan,
        output_schema,
        &graph_checkpoint,
    )
    .unwrap();
    let tail = vec![join_relation_input(
        &accounts,
        2,
        4,
        vec![accounts_rows_batch(&[
            ("alice", 100, "gold", -1),
            ("alice", 80, "gold", 1),
        ])],
    )];
    let live = uninterrupted_runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("equivalence-general-2").unwrap(),
            tail.clone(),
        )
        .unwrap();
    let restored = restored_runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("equivalence-general-2").unwrap(),
            tail.clone(),
        )
        .unwrap();
    let generic_live = uninterrupted_graph.apply_epoch(2, tail.clone()).unwrap();
    let generic_restored = restored_graph.apply_epoch(2, tail).unwrap();
    for delta in [
        &restored.output_deltas[0].delta,
        &generic_live,
        &generic_restored,
    ] {
        assert!(specialization_delta_difference(&live.output_deltas[0].delta, delta).is_none());
    }
    assert_eq!(
        restored_runtime.checkpoint().unwrap(),
        uninterrupted_runtime.checkpoint().unwrap()
    );
    assert_eq!(
        restored_graph.checkpoint().unwrap(),
        uninterrupted_graph.checkpoint().unwrap()
    );
    assert_general_join_specialization_state_equivalent(
        uninterrupted_runtime.as_ref(),
        &uninterrupted_graph,
    );
    assert_general_join_specialization_state_equivalent(restored_runtime.as_ref(), &restored_graph);
}

#[test]
fn general_aggregate_join_comparison_covers_filters_and_expression_inputs() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let catalogs = vec![scores.clone(), accounts.clone()];
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let cases = vec![
        (
            "select a.account_id, sum(s.score) filter (where s.score > 5) as sum, count(*) filter (where s.score > 0) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
            join_output_schema(),
            "scores_by_account",
        ),
        (
            "select a.account_id, sum(s.score + 1) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
            join_output_schema(),
            "scores_by_account",
        ),
        (
            "select a.account_id, sum(s.score) as sum, sum(a.limit + 1) as adjusted_sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
            join_adjusted_sum_output_schema(),
            "scores_by_account_adjusted",
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id having sum(s.score) > 5 order by sum desc limit 1",
            join_output_schema(),
            "scores_by_account",
        ),
    ];

    for (case_index, (sql, output_schema, view_id)) in cases.into_iter().enumerate() {
        let identity = standing_identity_with_view(sql, view_id);
        let logical_plan =
            lower_supported_join_view_sql_to_logical_plan(sql, &catalogs, &output_schema).unwrap();
        let VelorixLogicalViewExecutionV1::TwoInputJoinSumCount { plan } = &logical_plan.execution
        else {
            panic!("expected join execution");
        };
        let mut comparison = JoinSpecializationComparisonGraph::new(
            catalogs.clone(),
            plan.as_ref().clone(),
            output_schema.clone(),
        )
        .unwrap();
        let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
            &identity,
            &catalogs,
            logical_plan,
            &input_schemas,
            std::slice::from_ref(&output_schema),
        )
        .unwrap();
        let inputs = vec![
            join_relation_input(&scores, 0, 3, vec![scores_batch()]),
            join_relation_input(&accounts, 0, 2, vec![accounts_alice_bob_batch()]),
        ];
        let specialized = runtime
            .apply_changes(
                1,
                EpochIdempotencyKey::new(format!("equivalence-general-case-{case_index}")).unwrap(),
                inputs.clone(),
            )
            .unwrap();
        let generic = comparison.apply_epoch(1, inputs).unwrap();
        assert!(
            specialization_delta_difference(&specialized.output_deltas[0].delta, &generic,)
                .is_none()
        );
        assert_general_join_specialization_state_equivalent(runtime.as_ref(), &comparison);

        let tail = vec![join_relation_input(
            &scores,
            3,
            6,
            vec![scores_rows_batch(&[
                ("alice", 10, -1),
                ("alice", 7, -1),
                ("bob", 20, 1),
            ])],
        )];
        let specialized = runtime
            .apply_changes(
                2,
                EpochIdempotencyKey::new(format!("equivalence-general-case-{case_index}-tail"))
                    .unwrap(),
                tail.clone(),
            )
            .unwrap();
        let generic = comparison.apply_epoch(2, tail).unwrap();
        assert!(
            specialization_delta_difference(&specialized.output_deltas[0].delta, &generic,)
                .is_none()
        );
        assert_general_join_specialization_state_equivalent(runtime.as_ref(), &comparison);
    }
}

#[test]
fn runtime_materializes_left_join_left_only_aggregates_for_unmatched_left_rows() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_stats_output_schema();
    let sql = "select s.user_id as account_id, sum(s.score) as sum, count(*) as count, min(s.score) as min_score, max(s.score) as max_score, avg(s.score) as avg_score from scores s left join accounts a on s.user_id = a.account_id group by s.user_id";
    let identity = standing_identity_with_view(sql, "scores_by_account_stats");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 5, 1),
                        ("alice", 7, 1),
                        ("charlie", 30, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_stats_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 17, 2, 7, 10, 8.5),
            ("bob", 5, 1, 5, 5, 5.0),
            ("charlie", 30, 1, 30, 30, 30.0),
        ],
    );

    // The right relation is keyed by account_id, so an unmatched left row and
    // its later single match each contribute exactly once. Matching and then
    // retracting the right row after recovery must not duplicate or remove the
    // left-only aggregate.
    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: accounts.relation_schema.relation_id.clone(),
                relation_version: accounts.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: accounts.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![accounts_rows_batch(&[("charlie", 100, "gold", 1)])],
            }],
        )
        .unwrap();
    assert_join_stats_page(
        restored.as_ref(),
        2,
        &[
            ("alice", 17, 2, 7, 10, 8.5),
            ("bob", 5, 1, 5, 5, 5.0),
            ("charlie", 30, 1, 30, 30, 30.0),
        ],
    );

    restored
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: accounts.relation_schema.relation_id.clone(),
                relation_version: accounts.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: accounts.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![accounts_rows_batch(&[("charlie", 100, "gold", -1)])],
            }],
        )
        .unwrap();
    assert_join_stats_page(
        restored.as_ref(),
        3,
        &[
            ("alice", 17, 2, 7, 10, 8.5),
            ("bob", 5, 1, 5, 5, 5.0),
            ("charlie", 30, 1, 30, 30, 30.0),
        ],
    );
}

#[test]
fn runtime_materializes_full_join_symmetric_transitions_and_restores_state() {
    let scores = scores_catalog();
    let accounts = accounts_nullable_limit_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let mut output_schema = join_output_schema();
    output_schema.columns[1].nullable = true;
    let sql = "select coalesce(s.user_id, a.account_id) as account_id, sum(s.score) as sum, count(*) as count from scores s full outer join accounts a on s.user_id = a.account_id group by coalesce(s.user_id, a.account_id)";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("full-join-epoch-1").unwrap(),
            vec![
                join_relation_input(
                    &scores,
                    0,
                    2,
                    vec![scores_rows_batch(&[("alice", 10, 1), ("charlie", 30, 1)])],
                ),
                join_relation_input(
                    &accounts,
                    0,
                    2,
                    vec![accounts_nullable_limit_rows_batch(&[
                        ("alice", Some(100), "gold", 1),
                        ("bob", None, "gold", 1),
                    ])],
                ),
            ],
        )
        .unwrap();
    assert_nullable_join_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", Some(10), 1),
            ("bob", None, 1),
            ("charlie", Some(30), 1),
        ],
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let payload: Value =
        serde_json::from_str(&checkpoint.state_payload.as_ref().unwrap().payload).unwrap();
    assert_eq!(payload["plan"]["join_kind"], "full");
    let mut restored = restore_standing_runtime(checkpoint).unwrap();

    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("full-join-epoch-2").unwrap(),
            vec![
                join_relation_input(&scores, 2, 3, vec![scores_rows_batch(&[("bob", 5, 1)])]),
                join_relation_input(
                    &accounts,
                    2,
                    3,
                    vec![accounts_nullable_limit_rows_batch(&[(
                        "charlie",
                        Some(90),
                        "silver",
                        1,
                    )])],
                ),
            ],
        )
        .unwrap();
    assert_nullable_join_page(
        restored.as_ref(),
        2,
        &[
            ("alice", Some(10), 1),
            ("bob", Some(5), 1),
            ("charlie", Some(30), 1),
        ],
    );

    restored
        .apply_changes(
            3,
            EpochIdempotencyKey::new("full-join-epoch-3").unwrap(),
            vec![
                join_relation_input(&scores, 3, 4, vec![scores_rows_batch(&[("alice", 7, 1)])]),
                join_relation_input(
                    &accounts,
                    3,
                    4,
                    vec![accounts_nullable_limit_rows_batch(&[(
                        "alice",
                        Some(100),
                        "gold",
                        1,
                    )])],
                ),
            ],
        )
        .unwrap();
    assert_nullable_join_page(
        restored.as_ref(),
        3,
        &[
            ("alice", Some(34), 4),
            ("bob", Some(5), 1),
            ("charlie", Some(30), 1),
        ],
    );

    let mut restored = restore_standing_runtime(restored.checkpoint().unwrap()).unwrap();
    for (epoch, start_offset, expected_sum, expected_count) in
        [(4, 4, Some(17), 2), (5, 5, Some(17), 2)]
    {
        restored
            .apply_changes(
                epoch,
                EpochIdempotencyKey::new(format!("full-join-epoch-{epoch}")).unwrap(),
                vec![join_relation_input(
                    &accounts,
                    start_offset,
                    start_offset + 1,
                    vec![accounts_nullable_limit_rows_batch(&[(
                        "alice",
                        Some(100),
                        "gold",
                        -1,
                    )])],
                )],
            )
            .unwrap();
        assert_nullable_join_page(
            restored.as_ref(),
            epoch,
            &[
                ("alice", expected_sum, expected_count),
                ("bob", Some(5), 1),
                ("charlie", Some(30), 1),
            ],
        );
    }

    restored
        .apply_changes(
            6,
            EpochIdempotencyKey::new("full-join-epoch-6").unwrap(),
            vec![join_relation_input(
                &scores,
                4,
                6,
                vec![scores_rows_batch(&[("alice", 10, -1), ("alice", 7, -1)])],
            )],
        )
        .unwrap();
    assert_nullable_join_page(
        restored.as_ref(),
        6,
        &[("bob", Some(5), 1), ("charlie", Some(30), 1)],
    );

    restored
        .apply_changes(
            7,
            EpochIdempotencyKey::new("full-join-epoch-7").unwrap(),
            vec![
                join_relation_input(&scores, 6, 7, vec![scores_rows_batch(&[("bob", 5, -1)])]),
                join_relation_input(
                    &accounts,
                    6,
                    7,
                    vec![accounts_nullable_limit_rows_batch(&[(
                        "charlie",
                        Some(90),
                        "silver",
                        -1,
                    )])],
                ),
            ],
        )
        .unwrap();
    assert_nullable_join_page(
        restored.as_ref(),
        7,
        &[("bob", None, 1), ("charlie", Some(30), 1)],
    );

    restored
        .apply_changes(
            8,
            EpochIdempotencyKey::new("full-join-epoch-8").unwrap(),
            vec![join_relation_input(
                &scores,
                7,
                9,
                vec![scores_rows_batch(&[("charlie", 30, -1), ("bob", 30, 1)])],
            )],
        )
        .unwrap();
    assert_nullable_join_page(restored.as_ref(), 8, &[("bob", Some(30), 1)]);
}

#[test]
fn runtime_materializes_left_join_right_aggregate_filter_and_restores_state() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = left_join_right_sum_output_schema();
    let sql = "select s.user_id as account_id, sum(s.score) as sum, sum(a.limit) filter (where a.limit > 60) as limit_sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id group by s.user_id";
    let identity = standing_identity_with_view(sql, "left_join_right_sum");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                join_relation_input(
                    &scores,
                    0,
                    4,
                    vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("alice", 7, 1),
                        ("bob", 5, 1),
                        ("charlie", 30, 1),
                    ])],
                ),
                join_relation_input(&accounts, 0, 2, vec![accounts_alice_bob_batch()]),
            ],
        )
        .unwrap();

    assert_left_join_right_sum_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 17, Some(200), 2),
            ("bob", 5, None, 1),
            ("charlie", 30, None, 1),
        ],
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let mut tampered = checkpoint.clone();
    let state_payload = tampered.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    payload["plan"]["right_value_column_ids"] = json!(["missing_right_value"]);
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    tampered.state_root.content_hash = stable_bytes_hash(state_payload.payload.as_bytes());
    assert!(restore_standing_runtime(tampered).is_err());

    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![join_relation_input(
                &accounts,
                2,
                3,
                vec![accounts_rows_batch(&[("alice", 100, "gold", -1)])],
            )],
        )
        .unwrap();
    assert_left_join_right_sum_page(
        restored.as_ref(),
        2,
        &[
            ("alice", 17, None, 2),
            ("bob", 5, None, 1),
            ("charlie", 30, None, 1),
        ],
    );
}

#[test]
fn runtime_materializes_left_join_empty_right_aggregates_as_sql_null() {
    let scores = scores_catalog();
    let accounts = accounts_nullable_limit_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let mut output_schema = join_right_stats_output_schema();
    for index in [1, 5, 6, 7, 8] {
        output_schema.columns[index].nullable = true;
    }
    let sql = "select s.user_id as account_id, sum(s.score) as sum, count(*) as count, count(a.limit) as count_limit, count(distinct a.limit) as distinct_limits, sum(a.limit) as limit_sum, min(a.limit) as min_limit, max(a.limit) as max_limit, avg(a.limit) as avg_limit from scores s left join accounts a on s.user_id = a.account_id group by s.user_id";
    let identity = standing_identity_with_view(sql, "scores_by_account_limits");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                join_relation_input(
                    &scores,
                    0,
                    3,
                    vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("alice", 7, 1),
                        ("bob", 5, 1),
                    ])],
                ),
                join_relation_input(
                    &accounts,
                    0,
                    1,
                    vec![accounts_nullable_limit_rows_batch(&[(
                        "alice", None, "gold", 1,
                    )])],
                ),
            ],
        )
        .unwrap();
    assert_left_join_empty_right_stats_page(runtime.as_ref(), 1, &["alice", "bob"]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![join_relation_input(
                &accounts,
                1,
                4,
                vec![accounts_nullable_limit_rows_batch(&[
                    ("alice", None, "gold", -1),
                    ("alice", Some(80), "gold", 1),
                    ("bob", Some(50), "gold", 1),
                ])],
            )],
        )
        .unwrap();
    assert_join_right_stats_page(
        restored.as_ref(),
        2,
        &[
            ("alice", 17, 2, 2, 1, 160, 80, 80, 80.0),
            ("bob", 5, 1, 1, 1, 50, 50, 50, 50.0),
        ],
    );
}

#[test]
fn runtime_materializes_left_join_post_join_right_filter() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let mut output_schema = join_output_schema();
    output_schema.columns[1].nullable = true;
    let sql = "select s.user_id as account_id, sum(s.score) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id where a.limit > 60 group by s.user_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                join_relation_input(
                    &scores,
                    0,
                    4,
                    vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("alice", 7, 1),
                        ("bob", 5, 1),
                        ("charlie", 30, 1),
                    ])],
                ),
                join_relation_input(&accounts, 0, 2, vec![accounts_alice_bob_batch()]),
            ],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2)]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![join_relation_input(
                &accounts,
                2,
                3,
                vec![accounts_rows_batch(&[("alice", 100, "gold", -1)])],
            )],
        )
        .unwrap();
    assert_join_page(restored.as_ref(), 2, &[]);
}

#[test]
fn runtime_materializes_null_accepting_left_join_where_after_the_join() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let mut output_schema = join_output_schema();
    output_schema.columns[1].nullable = true;
    let sql = "select s.user_id as account_id, sum(s.score) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id where a.limit is null group by s.user_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                join_relation_input(
                    &scores,
                    0,
                    4,
                    vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("alice", 7, 1),
                        ("bob", 5, 1),
                        ("charlie", 30, 1),
                    ])],
                ),
                join_relation_input(&accounts, 0, 2, vec![accounts_alice_bob_batch()]),
            ],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 1, &[("charlie", 30, 1)]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![join_relation_input(
                &accounts,
                2,
                3,
                vec![accounts_rows_batch(&[("alice", 100, "gold", -1)])],
            )],
        )
        .unwrap();
    assert_join_page(
        restored.as_ref(),
        2,
        &[("alice", 17, 2), ("charlie", 30, 1)],
    );
}

#[test]
fn runtime_materializes_right_join_by_swapping_to_left_join_state() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(a.limit) as sum, count(*) as count from scores s right join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 1,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[("alice", 10, 1)])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 1, &[("alice", 100, 1), ("bob", 50, 1)]);

    let checkpoint = runtime.checkpoint().unwrap();
    let payload: Value =
        serde_json::from_str(&checkpoint.state_payload.as_ref().unwrap().payload).unwrap();
    assert_eq!(payload["plan"]["join_kind"], "left");
    assert_eq!(
        payload["plan"]["left_input_relation_id"],
        accounts.relation_schema.relation_id
    );
    assert_eq!(
        payload["plan"]["right_input_relation_id"],
        scores.relation_schema.relation_id
    );

    let restored = restore_standing_runtime(checkpoint).unwrap();
    assert_join_page(restored.as_ref(), 1, &[("alice", 100, 1), ("bob", 50, 1)]);
}

#[test]
fn runtime_materializes_left_join_with_left_only_aggregate_filter_for_unmatched_left_rows() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select s.user_id as account_id, sum(s.score) filter (where s.score > 0) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id group by s.user_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 5,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("alice", -7, 1),
                        ("bob", -5, 1),
                        ("charlie", 30, 1),
                        ("charlie", -1, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_page(
        runtime.as_ref(),
        1,
        &[("alice", 10, 2), ("bob", 0, 1), ("charlie", 30, 2)],
    );
}

#[test]
fn runtime_materializes_two_relation_join_with_generic_adapter_catalogs() {
    let scores = generic_adapter_catalog(scores_catalog());
    let accounts = generic_adapter_catalog(accounts_catalog());
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 5, 1),
                        ("alice", 7, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2), ("bob", 5, 1)]);
}

#[test]
fn runtime_materializes_two_relation_join_sum_expression_inputs() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let left_sql = "select a.account_id, sum(s.score + 1) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let left_identity = standing_identity_with_view(left_sql, "scores_by_account");
    let mut left_runtime = create_standing_runtime_with_sql_and_catalogs(
        &left_identity,
        &[scores.clone(), accounts.clone()],
        left_sql,
        &input_schemas,
        std::slice::from_ref(&join_output_schema()),
    )
    .unwrap();

    apply_join_expression_fixture(left_runtime.as_mut(), &scores, &accounts);
    assert_join_page(left_runtime.as_ref(), 1, &[("alice", 19, 2), ("bob", 6, 1)]);

    let right_sql = "select a.account_id, sum(s.score) as sum, sum(a.limit + 1) as adjusted_sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let right_identity = standing_identity_with_view(right_sql, "scores_by_account_adjusted");
    let mut right_runtime = create_standing_runtime_with_sql_and_catalogs(
        &right_identity,
        &[scores.clone(), accounts.clone()],
        right_sql,
        &input_schemas,
        std::slice::from_ref(&join_adjusted_sum_output_schema()),
    )
    .unwrap();

    apply_join_expression_fixture(right_runtime.as_mut(), &scores, &accounts);
    assert_join_adjusted_sum_page(
        right_runtime.as_ref(),
        1,
        &[("alice", 17, 202, 2), ("bob", 5, 51, 1)],
    );
}

#[test]
fn runtime_materializes_two_relation_join_scalar_int64_residual_predicates() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];

    for (sql, expected) in [
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where s.score + 1 > 10 group by a.account_id",
            vec![("alice", 10, 1)],
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where 10 < s.score + 1 group by a.account_id",
            vec![("alice", 10, 1)],
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where a.limit + 1 > 60 group by a.account_id",
            vec![("alice", 17, 2)],
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id and s.score + 1 > 10 group by a.account_id",
            vec![("alice", 10, 1)],
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where s.score + 46 > a.limit group by a.account_id",
            vec![("bob", 5, 1)],
        ),
        (
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where s.score + 47 > a.limit + 1 group by a.account_id",
            vec![("bob", 5, 1)],
        ),
    ] {
        let identity = standing_identity_with_view(sql, "scores_by_account");
        let mut runtime = create_standing_runtime_with_sql_and_catalogs(
            &identity,
            &[scores.clone(), accounts.clone()],
            sql,
            &input_schemas,
            std::slice::from_ref(&join_output_schema()),
        )
        .unwrap();

        apply_join_expression_fixture(runtime.as_mut(), &scores, &accounts);
        assert_join_page(runtime.as_ref(), 1, &expected);
    }
}

fn apply_join_expression_fixture(
    runtime: &mut (dyn StandingProgramRuntime + Send),
    scores: &VelorixRelationCatalogV1,
    accounts: &VelorixRelationCatalogV1,
) {
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 5, 1),
                        ("alice", 7, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();
}

#[test]
fn runtime_materializes_two_relation_join_count_only_and_restores_state() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_count_output_schema();
    let sql = "select a.account_id, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account_count");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 5, 1),
                        ("alice", 7, 1),
                        ("charlie", 30, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_count_page(runtime.as_ref(), 1, &[("alice", 2), ("bob", 1)]);

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: scores.relation_schema.relation_id.clone(),
                relation_version: scores.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: scores.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![score_append_batch("alice", 3)],
            }],
        )
        .unwrap();

    assert_join_count_page(restored.as_ref(), 2, &[("alice", 3), ("bob", 1)]);
}

#[test]
fn runtime_materializes_two_relation_join_count_distinct_only_and_restores_state() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_distinct_count_output_schema();
    let sql = "select a.account_id, count(distinct s.score) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account_distinct_count");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("alice", 10, 1),
                        ("alice", 7, 1),
                        ("bob", 5, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_distinct_count_page(runtime.as_ref(), 1, &[("alice", 2), ("bob", 1)]);

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: scores.relation_schema.relation_id.clone(),
                relation_version: scores.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: scores.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![score_append_batch("bob", 20)],
            }],
        )
        .unwrap();

    assert_join_distinct_count_page(restored.as_ref(), 2, &[("alice", 2), ("bob", 2)]);
}

#[test]
fn runtime_materializes_two_relation_join_grouped_by_left_key_projection() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_left_key_output_schema();
    let sql = "select s.user_id as user, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by s.user_id";
    let identity = standing_identity_with_view(sql, "scores_by_user");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 5, 1),
                        ("alice", 7, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_left_key_page(runtime.as_ref(), 1, &[("alice", 17, 2), ("bob", 5, 1)]);

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: scores.relation_schema.relation_id.clone(),
                relation_version: scores.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: scores.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![score_append_batch("alice", 3)],
            }],
        )
        .unwrap();

    assert_join_left_key_page(restored.as_ref(), 2, &[("alice", 20, 3), ("bob", 5, 1)]);
}

#[test]
fn runtime_materializes_two_relation_join_stats_and_restores_incremental_state() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_stats_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count, min(s.score) as min_score, max(s.score) as max_score, avg(s.score) as avg_score from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account_stats");
    let logical_plan = join_stats_logical_plan(sql, &scores, &accounts, &output_schema);
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        logical_plan,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 5, 1),
                        ("alice", 7, 1),
                        ("charlie", 30, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_stats_page(
        runtime.as_ref(),
        1,
        &[("alice", 17, 2, 7, 10, 8.5), ("bob", 5, 1, 5, 5, 5.0)],
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: scores.relation_schema.relation_id.clone(),
                relation_version: scores.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: scores.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 7,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[
                    ("alice", 7, -1),
                    ("alice", 13, 1),
                    ("bob", 20, 1),
                ])],
            }],
        )
        .unwrap();

    assert_join_stats_page(
        restored.as_ref(),
        2,
        &[("alice", 23, 2, 10, 13, 11.5), ("bob", 25, 2, 5, 20, 12.5)],
    );
}

#[test]
fn runtime_materializes_two_relation_join_right_side_stats_and_restores_state() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_right_stats_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count, count(a.limit) as count_limit, count(distinct a.limit) as distinct_limits, sum(a.limit) as limit_sum, min(a.limit) as min_limit, max(a.limit) as max_limit, avg(a.limit) as avg_limit from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account_limits");
    let logical_plan = lower_supported_join_view_sql_to_logical_plan(
        sql,
        &[scores.clone(), accounts.clone()],
        &output_schema,
    )
    .unwrap();
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        logical_plan,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 5, 1),
                        ("alice", 7, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_right_stats_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 17, 2, 2, 1, 200, 100, 100, 100.0),
            ("bob", 5, 1, 1, 1, 50, 50, 50, 50.0),
        ],
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: accounts.relation_schema.relation_id.clone(),
                relation_version: accounts.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: accounts.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![accounts_rows_batch(&[
                    ("alice", 100, "gold", -1),
                    ("alice", 80, "gold", 1),
                ])],
            }],
        )
        .unwrap();

    assert_join_right_stats_page(
        restored.as_ref(),
        2,
        &[
            ("alice", 17, 2, 2, 1, 160, 80, 80, 80.0),
            ("bob", 5, 1, 1, 1, 50, 50, 50, 50.0),
        ],
    );
}

#[test]
fn runtime_materializes_two_relation_join_decimal_avg_as_float64_output() {
    let scores = scores_catalog();
    let accounts = accounts_decimal_limit_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_decimal_avg_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count, avg(a.limit) as avg_limit from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account_decimal_avg");
    let logical_plan = lower_supported_join_view_sql_to_logical_plan(
        sql,
        &[scores.clone(), accounts.clone()],
        &output_schema,
    )
    .unwrap();
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        logical_plan,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 5, 1),
                        ("alice", 7, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_decimal_limit_rows_batch(&[
                        ("alice", 1000, "gold", 1),
                        ("bob", 505, "gold", 1),
                    ])],
                },
            ],
        )
        .unwrap();

    assert_join_decimal_avg_page(
        runtime.as_ref(),
        1,
        &[("alice", 17, 2, 10.0), ("bob", 5, 1, 5.05)],
    );
}

#[test]
fn runtime_materializes_two_relation_join_nullable_right_value_count() {
    let scores = scores_catalog();
    let accounts = accounts_nullable_limit_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_right_nullable_count_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count, count(a.limit) as count_limit, count(distinct a.limit) as distinct_limits from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_nullable_account_limits");
    let logical_plan = lower_supported_join_view_sql_to_logical_plan(
        sql,
        &[scores.clone(), accounts.clone()],
        &output_schema,
    )
    .unwrap();
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        logical_plan,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 5, 1),
                        ("alice", 7, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_nullable_limit_rows_batch(&[
                        ("alice", None, "gold", 1),
                        ("bob", Some(50), "gold", 1),
                    ])],
                },
            ],
        )
        .unwrap();

    assert_join_right_nullable_count_page(
        runtime.as_ref(),
        1,
        &[("alice", 17, 2, 0, 0), ("bob", 5, 1, 1, 1)],
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: accounts.relation_schema.relation_id.clone(),
                relation_version: accounts.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: accounts.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![accounts_nullable_limit_rows_batch(&[
                    ("alice", None, "gold", -1),
                    ("alice", Some(80), "gold", 1),
                ])],
            }],
        )
        .unwrap();

    assert_join_right_nullable_count_page(
        restored.as_ref(),
        2,
        &[("alice", 17, 2, 2, 1), ("bob", 5, 1, 1, 1)],
    );
}

#[test]
fn runtime_materializes_two_relation_join_multiple_right_aggregate_input_columns() {
    let scores = scores_catalog();
    let accounts = accounts_multi_value_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_right_multi_value_stats_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count, min(a.limit) as min_limit, max(a.quota) as max_quota, avg(a.quota) as avg_quota from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account_quotas");
    let logical_plan = lower_supported_join_view_sql_to_logical_plan(
        sql,
        &[scores.clone(), accounts.clone()],
        &output_schema,
    )
    .unwrap();
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        logical_plan,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 5, 1),
                        ("alice", 7, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_multi_value_rows_batch(&[
                        ("alice", 100, 1000, "gold", 1),
                        ("bob", 50, 500, "gold", 1),
                    ])],
                },
            ],
        )
        .unwrap();

    assert_join_right_multi_value_stats_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 17, 2, 100, 1000, 1000.0),
            ("bob", 5, 1, 50, 500, 500.0),
        ],
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: accounts.relation_schema.relation_id.clone(),
                relation_version: accounts.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: accounts.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![accounts_multi_value_rows_batch(&[
                    ("alice", 100, 1000, "gold", -1),
                    ("alice", 80, 800, "gold", 1),
                ])],
            }],
        )
        .unwrap();

    assert_join_right_multi_value_stats_page(
        restored.as_ref(),
        2,
        &[
            ("alice", 17, 2, 80, 800, 800.0),
            ("bob", 5, 1, 50, 500, 500.0),
        ],
    );
}

#[test]
fn runtime_materializes_two_relation_join_right_aggregate_having_order_by_top_k() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_right_stats_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count, count(a.limit) as count_limit, count(distinct a.limit) as distinct_limits, sum(a.limit) as limit_sum, min(a.limit) as min_limit, max(a.limit) as max_limit, avg(a.limit) as avg_limit from scores s join accounts a on s.user_id = a.account_id group by a.account_id having avg(a.limit) > 60 order by max(a.limit) desc limit 1";
    let identity = standing_identity_with_view(sql, "scores_by_account_limits");
    let logical_plan = lower_supported_join_view_sql_to_logical_plan(
        sql,
        &[scores.clone(), accounts.clone()],
        &output_schema,
    )
    .unwrap();
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        logical_plan,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 5, 1),
                        ("alice", 7, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_right_stats_page(
        runtime.as_ref(),
        1,
        &[("alice", 17, 2, 2, 1, 200, 100, 100, 100.0)],
    );
}

#[test]
fn runtime_restores_legacy_join_checkpoint_without_aggregate_input_relation_side() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(s.score) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[("alice", 10, 1), ("bob", 5, 1)])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();

    let mut checkpoint = runtime.checkpoint().unwrap();
    let state_payload = checkpoint.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    let aggregate_outputs = payload
        .pointer_mut("/plan/aggregate_outputs")
        .and_then(Value::as_array_mut)
        .unwrap();
    for output in aggregate_outputs {
        output
            .as_object_mut()
            .unwrap()
            .remove("input_relation_side");
    }
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    checkpoint.state_root.content_hash = stable_bytes_hash(state_payload.payload.as_bytes());

    let restored = restore_standing_runtime(checkpoint).unwrap();
    assert_join_page(restored.as_ref(), 1, &[("alice", 10, 1), ("bob", 5, 1)]);
}

#[test]
fn runtime_rejects_join_checkpoint_without_published_output() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[("alice", 10, 1), ("bob", 5, 1)])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_alice_bob_batch()],
                },
            ],
        )
        .unwrap();

    let mut checkpoint = runtime.checkpoint().unwrap();
    assert_eq!(checkpoint.input_frontiers.len(), 2);
    assert!(checkpoint.input_frontiers.iter().any(|frontier| {
        frontier.relation_id == scores.relation_schema.relation_id
            && frontier.committed_offset_exclusive == 2
    }));
    assert!(checkpoint.input_frontiers.iter().any(|frontier| {
        frontier.relation_id == accounts.relation_schema.relation_id
            && frontier.committed_offset_exclusive == 2
    }));
    let state_payload = checkpoint.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    payload.as_object_mut().unwrap().remove("published_output");
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    checkpoint.state_root.content_hash = stable_bytes_hash(state_payload.payload.as_bytes());

    let err = match restore_standing_runtime(checkpoint) {
        Ok(_) => panic!("restore unexpectedly rebuilt missing join published output"),
        Err(error) => error,
    };
    assert!(err.contains("published_output"), "{err}");
}

#[test]
fn runtime_materializes_two_relation_join_using_primary_key() {
    let scores = scores_catalog();
    let accounts = accounts_catalog_with_user_id_key();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.user_id as account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a using (user_id) group by a.user_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_batch()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_using_user_id_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2), ("bob", 5, 1)]);
}

#[test]
fn runtime_materializes_two_relation_join_order_by_limit_top_k_and_restores_state() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum desc limit 1";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_batch()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2)]);

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: scores.relation_schema.relation_id.clone(),
                relation_version: scores.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: scores.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![score_append_batch("bob", 20)],
            }],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 2, &[("bob", 25, 2)]);

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_join_page(restored.as_ref(), 2, &[("bob", 25, 2)]);
}

#[test]
fn runtime_materializes_two_relation_join_order_by_sum_function_top_k() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum(s.score) desc limit 1";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_batch()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2)]);

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: scores.relation_schema.relation_id.clone(),
                relation_version: scores.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: scores.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![score_append_batch("bob", 20)],
            }],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 2, &[("bob", 25, 2)]);
}

#[test]
fn runtime_materializes_two_relation_join_order_by_count_star_function_top_k() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(*) desc limit 1";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_batch()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2)]);
}

#[test]
fn runtime_materializes_two_relation_join_order_by_count_distinct_function_top_k() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_distinct_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(distinct s.score) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by count(distinct s.score) desc limit 1";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_batch()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2)]);
}

#[test]
fn runtime_materializes_two_relation_join_having_view() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id having sum(s.score) > 10 or count(*) = 1";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_batch()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2), ("bob", 5, 1)]);
}

#[test]
fn runtime_materializes_two_relation_join_projected_aliases() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_alias_output_schema();
    let sql = "select a.account_id, sum(s.score) as total_score, count(*) as score_events from scores s join accounts a on s.user_id = a.account_id group by a.account_id having total_score > 10";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_batch()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap_or_else(|error| panic!("[SCALAR-APPLY1] {error}"));

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_account".to_string(),
            },
            SnapshotPageRequest::default(),
        )
        .unwrap();
    let batch = &page.batches[0];
    assert_eq!(batch.schema().field(1).name(), "total_score");
    assert_eq!(batch.schema().field(2).name(), "score_events");
    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2)]);
}

#[test]
fn runtime_materializes_two_relation_join_shared_aggregate_filter() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) filter (where s.score > 5) as sum, count(*) filter (where s.score > 5) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_batch()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2)]);

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: scores.relation_schema.relation_id.clone(),
                relation_version: scores.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: scores.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![score_append_batch("bob", 20)],
            }],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 2, &[("alice", 17, 2), ("bob", 20, 1)]);

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_join_page(restored.as_ref(), 2, &[("alice", 17, 2), ("bob", 20, 1)]);
}

#[test]
fn runtime_materializes_two_relation_join_mixed_aggregate_filters() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) filter (where s.score > 5) as sum, count(*) filter (where s.score > 0) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_batch()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2), ("bob", 0, 1)]);

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: scores.relation_schema.relation_id.clone(),
                relation_version: scores.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: scores.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![score_append_batch("bob", 20)],
            }],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 2, &[("alice", 17, 2), ("bob", 20, 2)]);

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_join_page(restored.as_ref(), 2, &[("alice", 17, 2), ("bob", 20, 2)]);
}

#[test]
fn runtime_materializes_two_relation_join_filtered_count_distinct() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_distinct_output_schema();
    let sql = "select a.account_id, sum(s.score) filter (where s.score > 5) as sum, count(distinct s.score) filter (where s.score > 0) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("alice", 10, 1),
                        ("alice", 7, 1),
                        ("bob", 5, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 1, &[("alice", 27, 2), ("bob", 0, 1)]);

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_join_page(restored.as_ref(), 1, &[("alice", 27, 2), ("bob", 0, 1)]);
}

#[test]
fn runtime_materializes_two_relation_join_having_count_distinct_function() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_distinct_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(distinct s.score) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id having count(distinct s.score) > 1";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("alice", 10, 1),
                        ("alice", 7, 1),
                        ("bob", 5, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 1, &[("alice", 27, 2)]);

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: scores.relation_schema.relation_id.clone(),
                relation_version: scores.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: scores.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("bob", 20, 1)])],
            }],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 2, &[("alice", 27, 2), ("bob", 25, 2)]);
}

#[test]
fn runtime_materializes_two_relation_join_count_distinct_skips_null_left_values() {
    let scores = scores_catalog_with_nullable_score();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_distinct_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(distinct s.score) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![scores_nullable_batch()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2), ("bob", 5, 1)]);
}

#[test]
fn runtime_materializes_two_relation_join_nullable_left_value_count() {
    let scores = scores_catalog_with_nullable_score();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(s.score) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![scores_nullable_batch()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();
    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2), ("bob", 5, 1)]);
}

#[test]
fn runtime_materializes_two_relation_join_with_left_where_filter() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where (s.score > 0 or s.score = -3) and s.score < 100 and a.limit > 60 and a.tier = 'gold' group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 6,
                    event_time_watermark: None,
                    batches: vec![RecordBatch::try_new(
                        Arc::new(Schema::new(vec![
                            Field::new("user_id", DataType::Utf8, false),
                            Field::new("score", DataType::Int64, false),
                            Field::new("delta", DataType::Int64, false),
                        ])),
                        vec![
                            Arc::new(StringArray::from(vec![
                                "alice", "alice", "bob", "alice", "charlie", "alice",
                            ])),
                            Arc::new(Int64Array::from(vec![10, 7, 5, 150, 8, -3])),
                            Arc::new(Int64Array::from(vec![1, 1, 1, 1, 1, 1])),
                        ],
                    )
                    .unwrap()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_page(runtime.as_ref(), 1, &[("alice", 14, 3)]);

    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 6,
                    end_offset_exclusive: 9,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 20, 1),  // passes the left predicate
                        ("alice", 200, 1), // fails `score < 100`
                        ("alice", -3, -1), // retracts a passing row
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 3,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![accounts_rows_batch(&[("charlie", 100, "gold", 1)])],
                },
            ],
        )
        .unwrap();
    assert_join_page(restored.as_ref(), 2, &[("alice", 37, 3), ("charlie", 8, 1)]);

    restored
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 9,
                    end_offset_exclusive: 10,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[("alice", 20, -1)])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 4,
                    end_offset_exclusive: 5,
                    event_time_watermark: None,
                    batches: vec![accounts_rows_batch(&[("charlie", 100, "gold", -1)])],
                },
            ],
        )
        .unwrap();
    assert_join_page(restored.as_ref(), 3, &[("alice", 17, 2)]);
}

#[test]
fn runtime_materializes_two_relation_join_with_cte_source_filter() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "with positive_scores as (select * from scores where score > 0) select a.account_id, sum(s.score) as sum, count(*) as count from positive_scores s join accounts a on s.user_id = a.account_id where a.limit > 60 group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![RecordBatch::try_new(
                        Arc::new(Schema::new(vec![
                            Field::new("user_id", DataType::Utf8, false),
                            Field::new("score", DataType::Int64, false),
                            Field::new("delta", DataType::Int64, false),
                        ])),
                        vec![
                            Arc::new(StringArray::from(vec!["alice", "alice", "bob", "alice"])),
                            Arc::new(Int64Array::from(vec![10, -3, 5, 7])),
                            Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
                        ],
                    )
                    .unwrap()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2)]);
}

#[test]
fn runtime_materializes_two_relation_join_with_right_cte_source_filter() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "with eligible_accounts as (select * from accounts where limit > 60) select a.account_id, sum(s.score) as sum, count(*) as count from scores s join eligible_accounts a on s.user_id = a.account_id where s.score > 0 group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![RecordBatch::try_new(
                        Arc::new(Schema::new(vec![
                            Field::new("user_id", DataType::Utf8, false),
                            Field::new("score", DataType::Int64, false),
                            Field::new("delta", DataType::Int64, false),
                        ])),
                        vec![
                            Arc::new(StringArray::from(vec!["alice", "alice", "bob", "alice"])),
                            Arc::new(Int64Array::from(vec![10, -3, 5, 7])),
                            Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
                        ],
                    )
                    .unwrap()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2)]);
}

#[test]
fn runtime_materializes_two_relation_join_with_two_cte_source_filters() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "with positive_scores as (select * from scores where score > 0), eligible_accounts as (select * from accounts where limit > 60) select a.account_id, sum(s.score) as sum, count(*) as count from positive_scores s join eligible_accounts a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![RecordBatch::try_new(
                        Arc::new(Schema::new(vec![
                            Field::new("user_id", DataType::Utf8, false),
                            Field::new("score", DataType::Int64, false),
                            Field::new("delta", DataType::Int64, false),
                        ])),
                        vec![
                            Arc::new(StringArray::from(vec!["alice", "alice", "bob", "alice"])),
                            Arc::new(Int64Array::from(vec![10, -3, 5, 7])),
                            Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
                        ],
                    )
                    .unwrap()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2)]);
}

#[test]
fn runtime_materializes_two_relation_join_with_two_derived_table_source_filters() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from (select * from scores where score > 0) s join (select * from accounts where limit > 60) a on s.user_id = a.account_id group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 4,
                    event_time_watermark: None,
                    batches: vec![RecordBatch::try_new(
                        Arc::new(Schema::new(vec![
                            Field::new("user_id", DataType::Utf8, false),
                            Field::new("score", DataType::Int64, false),
                            Field::new("delta", DataType::Int64, false),
                        ])),
                        vec![
                            Arc::new(StringArray::from(vec!["alice", "alice", "bob", "alice"])),
                            Arc::new(Int64Array::from(vec![10, -3, 5, 7])),
                            Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
                        ],
                    )
                    .unwrap()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2)]);
}

#[test]
fn runtime_materializes_two_relation_join_with_cross_relation_or_where_filter() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let input_schemas = vec![
        catalog_input_relation_schema(&scores).unwrap(),
        catalog_input_relation_schema(&accounts).unwrap(),
    ];
    let output_schema = join_output_schema();
    let sql = "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where s.score > 100 or a.limit > 60 group by a.account_id";
    let identity = standing_identity_with_view(sql, "scores_by_account");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &[scores.clone(), accounts.clone()],
        sql,
        &input_schemas,
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_batch()],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "test-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![accounts_batch()],
                },
            ],
        )
        .unwrap();

    assert_join_page(runtime.as_ref(), 1, &[("alice", 17, 2)]);
}

#[test]
fn runtime_materializes_latest_bool_by_key_and_restores_state() {
    let catalog = device_status_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = latest_device_status_output_schema();
    let sql = "select device_id, arg_max(enabled, event_time) as enabled from device_status where enabled = true group by all";
    let identity = standing_identity_with_view(sql, "latest_device_status");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[
                    ("device-a", true, 100, 1),
                    ("device-a", false, 110, 1),
                    ("device-b", true, 90, 1),
                ])],
            }],
        )
        .unwrap();

    assert_latest_status_page(
        runtime.as_ref(),
        1,
        &[("device-a", true), ("device-b", true)],
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[
                    ("device-a", true, 105, 1),
                    ("device-b", false, 120, 1),
                ])],
            }],
        )
        .unwrap();

    assert_latest_status_page(
        restored.as_ref(),
        2,
        &[("device-a", true), ("device-b", true)],
    );
}

#[test]
fn runtime_rejects_latest_by_key_checkpoint_without_published_output() {
    let catalog = device_status_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = latest_device_status_output_schema();
    let sql = "select device_id, arg_max(enabled, event_time) as enabled from device_status where enabled = true group by all";
    let identity = standing_identity_with_view(sql, "latest_device_status");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[
                    ("device-a", true, 100, 1),
                    ("device-b", true, 90, 1),
                ])],
            }],
        )
        .unwrap();

    let mut checkpoint = runtime.checkpoint().unwrap();
    let state_payload = checkpoint.state_payload.as_mut().unwrap();
    let mut payload: Value = serde_json::from_str(&state_payload.payload).unwrap();
    payload.as_object_mut().unwrap().remove("published_output");
    state_payload.payload = serde_json::to_string(&payload).unwrap();
    checkpoint.state_root.content_hash = stable_bytes_hash(state_payload.payload.as_bytes());

    let err = match restore_standing_runtime(checkpoint) {
        Ok(_) => panic!("restore unexpectedly rebuilt missing latest-by-key published output"),
        Err(error) => error,
    };
    assert!(err.contains("published_output"), "{err}");
}

#[test]
fn runtime_materializes_earliest_bool_by_key_with_arg_min() {
    let catalog = device_status_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = latest_device_status_output_schema();
    let sql = "select device_id, arg_min(enabled, event_time) as enabled from device_status group by device_id";
    let identity = standing_identity_with_view(sql, "latest_device_status");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[
                    ("device-a", true, 100, 1),
                    ("device-a", false, 110, 1),
                    ("device-b", false, 80, 1),
                    ("device-b", true, 70, 1),
                ])],
            }],
        )
        .unwrap();

    assert_latest_status_page(
        runtime.as_ref(),
        1,
        &[("device-a", true), ("device-b", true)],
    );

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 6,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[
                    ("device-a", false, 90, 1),
                    ("device-b", false, 120, 1),
                ])],
            }],
        )
        .unwrap();

    assert_latest_status_page(
        runtime.as_ref(),
        2,
        &[("device-a", false), ("device-b", true)],
    );
}

#[test]
fn runtime_materializes_latest_by_key_with_arg_max_filter_predicate() {
    let catalog = device_status_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = latest_device_status_output_schema();
    let sql = "select device_id, arg_max(enabled, event_time) filter (where enabled = true) as enabled from device_status group by device_id";
    let identity = standing_identity_with_view(sql, "latest_device_status");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[
                    ("device-a", false, 110, 1),
                    ("device-a", true, 100, 1),
                    ("device-b", false, 120, 1),
                    ("device-b", true, 90, 1),
                ])],
            }],
        )
        .unwrap();

    assert_latest_status_page(
        runtime.as_ref(),
        1,
        &[("device-a", true), ("device-b", true)],
    );
}

#[test]
fn runtime_materializes_latest_by_key_with_where_and_arg_max_filter_predicates() {
    let catalog = device_status_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = latest_device_status_output_schema();
    let sql = "select device_id, arg_max(enabled, event_time) filter (where enabled = true) as enabled from device_status where event_time > 95 group by device_id";
    let identity = standing_identity_with_view(sql, "latest_device_status");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 5,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[
                    ("device-a", false, 110, 1),
                    ("device-a", true, 100, 1),
                    ("device-b", true, 90, 1),
                    ("device-c", false, 120, 1),
                    ("device-c", true, 80, 1),
                ])],
            }],
        )
        .unwrap();

    assert_latest_status_page(runtime.as_ref(), 1, &[("device-a", true)]);
}

#[test]
fn runtime_materializes_latest_by_key_cte_source_filters() {
    let catalog = device_status_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = latest_device_status_output_schema();
    let sql = "with status_source as (select * from device_status where event_time > 95) select device_id, arg_max(enabled, event_time) as enabled from status_source where enabled = true group by device_id";
    let identity = standing_identity_with_view(sql, "latest_device_status");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[
                    ("device-a", true, 100, 1),
                    ("device-a", false, 110, 1),
                    ("device-b", true, 90, 1),
                ])],
            }],
        )
        .unwrap();

    assert_latest_status_page(runtime.as_ref(), 1, &[("device-a", true)]);
}

#[test]
fn runtime_materializes_latest_by_key_derived_table_source_filters() {
    let catalog = device_status_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = latest_device_status_output_schema();
    let sql = "select s.device_id, arg_max(s.enabled, s.event_time) as enabled from (select * from device_status where event_time > 95) s where s.enabled = true group by s.device_id";
    let identity = standing_identity_with_view(sql, "latest_device_status");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[
                    ("device-a", true, 100, 1),
                    ("device-a", false, 110, 1),
                    ("device-b", true, 90, 1),
                ])],
            }],
        )
        .unwrap();

    assert_latest_status_page(runtime.as_ref(), 1, &[("device-a", true)]);
}

#[test]
fn runtime_materializes_latest_by_key_order_by_limit_top_k_and_restores_state() {
    let catalog = device_status_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = latest_device_status_output_schema();
    let sql = "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by device_id desc limit 1";
    let identity = standing_identity_with_view(sql, "latest_device_status");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[
                    ("device-a", true, 100, 1),
                    ("device-b", false, 110, 1),
                ])],
            }],
        )
        .unwrap();

    assert_latest_status_page(runtime.as_ref(), 1, &[("device-b", false)]);

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[("device-c", true, 120, 1)])],
            }],
        )
        .unwrap();

    assert_latest_status_page(restored.as_ref(), 2, &[("device-c", true)]);
}

#[test]
fn runtime_materializes_latest_by_key_order_by_limit_offset_top_k() {
    let catalog = device_status_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = latest_device_status_output_schema();
    let sql = "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by device_id desc limit 1 offset 1";
    let identity = standing_identity_with_view(sql, "latest_device_status");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[
                    ("device-a", true, 100, 1),
                    ("device-b", false, 110, 1),
                    ("device-c", true, 120, 1),
                ])],
            }],
        )
        .unwrap();
    assert_latest_status_page(runtime.as_ref(), 1, &[("device-b", false)]);

    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[("device-d", false, 130, 1)])],
            }],
        )
        .unwrap();
    assert_latest_status_page(runtime.as_ref(), 2, &[("device-c", true)]);
}

#[test]
fn runtime_materializes_latest_by_key_order_by_arg_max_function_top_k() {
    let catalog = device_status_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = latest_device_status_output_schema();
    let sql = "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id order by arg_max(enabled, event_time) desc limit 1";
    let identity = standing_identity_with_view(sql, "latest_device_status");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: None,
                batches: vec![device_status_batch(&[
                    ("device-a", false, 100, 1),
                    ("device-b", true, 110, 1),
                ])],
            }],
        )
        .unwrap();

    assert_latest_status_page(runtime.as_ref(), 1, &[("device-b", true)]);
}

#[test]
fn runtime_materializes_tumbling_event_time_windows_and_restores_state() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') where amount > 0 group by user_id, window_start, window_end having total_amount > 6";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 70_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("bob", 5, 30_000_000_000, 1),
                    ("bob", -50, 35_000_000_000, 1),
                    ("alice", 7, 70_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(runtime.as_ref(), 1, &[("alice", 0, 60_000_000_000, 10, 1)]);

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[(
                    "bob",
                    11,
                    80_000_000_000,
                    1,
                )])],
            }],
        )
        .unwrap();

    assert_window_page(
        restored.as_ref(),
        2,
        &[
            ("alice", 0, 60_000_000_000, 10, 1),
            ("alice", 60_000_000_000, 120_000_000_000, 7, 1),
            ("bob", 60_000_000_000, 120_000_000_000, 11, 1),
        ],
    );
}

#[test]
fn runtime_materializes_tumbling_event_time_aggregate_with_matching_filters() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(*) filter (where amount > 5) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("bob", 5, 30_000_000_000, 1),
                    ("alice", 7, 70_000_000_000, 1),
                    ("bob", 11, 80_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 10, 1),
            ("alice", 60_000_000_000, 120_000_000_000, 7, 1),
            ("bob", 60_000_000_000, 120_000_000_000, 11, 1),
        ],
    );
}

#[test]
fn runtime_materializes_tumbling_event_time_aggregate_with_mixed_filters() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("bob", 5, 30_000_000_000, 1),
                    ("alice", 7, 70_000_000_000, 1),
                    ("bob", 11, 80_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 10, 1),
            ("alice", 60_000_000_000, 120_000_000_000, 7, 1),
            ("bob", 0, 60_000_000_000, 0, 1),
            ("bob", 60_000_000_000, 120_000_000_000, 11, 1),
        ],
    );

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_window_page(
        restored.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 10, 1),
            ("alice", 60_000_000_000, 120_000_000_000, 7, 1),
            ("bob", 0, 60_000_000_000, 0, 1),
            ("bob", 60_000_000_000, 120_000_000_000, 11, 1),
        ],
    );
}

#[test]
fn runtime_materializes_tumbling_event_time_filtered_count_distinct_with_mixed_filters() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(distinct amount) filter (where amount > 0) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 5,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("alice", 10, 20_000_000_000, 1),
                    ("bob", 5, 30_000_000_000, 1),
                    ("alice", 7, 70_000_000_000, 1),
                    ("bob", 11, 80_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 20, 1),
            ("alice", 60_000_000_000, 120_000_000_000, 7, 1),
            ("bob", 0, 60_000_000_000, 0, 1),
            ("bob", 60_000_000_000, 120_000_000_000, 11, 1),
        ],
    );

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_window_page(
        restored.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 20, 1),
            ("alice", 60_000_000_000, 120_000_000_000, 7, 1),
            ("bob", 0, 60_000_000_000, 0, 1),
            ("bob", 60_000_000_000, 120_000_000_000, 11, 1),
        ],
    );
}

#[test]
fn runtime_materializes_tumbling_event_time_nullable_column_count() {
    let catalog = purchases_event_time_catalog_with_nullable_amount();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(amount) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_nullable_amount_batch(&[
                    ("alice", Some(10), 10_000_000_000, 1),
                    ("alice", None, 20_000_000_000, 1),
                    ("bob", None, 30_000_000_000, 1),
                    ("alice", Some(7), 70_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 10, 1),
            ("alice", 60_000_000_000, 120_000_000_000, 7, 1),
        ],
    );

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_window_page(
        restored.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 10, 1),
            ("alice", 60_000_000_000, 120_000_000_000, 7, 1),
        ],
    );
}

#[test]
fn runtime_materializes_tumbling_event_time_filtered_nullable_column_count() {
    let catalog = purchases_event_time_catalog_with_nullable_amount();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(amount) filter (where event_time >= 0) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_nullable_amount_batch(&[
                    ("alice", Some(10), 10_000_000_000, 1),
                    ("alice", None, 20_000_000_000, 1),
                    ("bob", None, 30_000_000_000, 1),
                    ("alice", Some(7), 70_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 10, 1),
            ("alice", 60_000_000_000, 120_000_000_000, 7, 1),
        ],
    );
}

#[test]
fn runtime_materializes_tumbling_event_time_mixed_filter_having_top_k() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end having total_amount > 0 order by total_amount desc limit 1";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("bob", 5, 30_000_000_000, 1),
                    ("alice", 7, 70_000_000_000, 1),
                    ("bob", 11, 80_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        1,
        &[("bob", 60_000_000_000, 120_000_000_000, 11, 1)],
    );

    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_window_page(
        restored.as_ref(),
        1,
        &[("bob", 60_000_000_000, 120_000_000_000, 11, 1)],
    );
}

#[test]
fn runtime_materializes_tumbling_event_time_aggregate_with_different_filters() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(*) filter (where amount <= 5) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("bob", 5, 30_000_000_000, 1),
                    ("alice", 7, 70_000_000_000, 1),
                    ("bob", 11, 80_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 10, 0),
            ("alice", 60_000_000_000, 120_000_000_000, 7, 0),
            ("bob", 0, 60_000_000_000, 0, 1),
            ("bob", 60_000_000_000, 120_000_000_000, 11, 0),
        ],
    );
}

#[test]
fn runtime_materializes_subsecond_tumbling_event_time_windows() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '500 milliseconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 1_000_000_000,
                    watermark_ns: 1_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 3, 100_000_000, 1),
                    ("bob", 5, 300_000_000, 1),
                    ("alice", 4, 700_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 0, 500_000_000, 3, 1),
            ("alice", 500_000_000, 1_000_000_000, 4, 1),
            ("bob", 0, 500_000_000, 5, 1),
        ],
    );
}

#[test]
fn runtime_materializes_tumbling_event_time_order_by_limit_top_k_and_restores_state() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by total_amount desc limit 1";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 70_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("bob", 5, 30_000_000_000, 1),
                    ("alice", 7, 70_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(runtime.as_ref(), 1, &[("alice", 0, 60_000_000_000, 10, 1)]);

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[(
                    "bob",
                    11,
                    80_000_000_000,
                    1,
                )])],
            }],
        )
        .unwrap();

    assert_window_page(
        restored.as_ref(),
        2,
        &[("bob", 60_000_000_000, 120_000_000_000, 11, 1)],
    );
}

#[test]
fn runtime_materializes_tumbling_event_time_order_by_limit_offset_top_k() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by total_amount desc limit 2 offset 1";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 70_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 30, 10_000_000_000, 1),
                    ("bob", 20, 20_000_000_000, 1),
                    ("carol", 10, 30_000_000_000, 1),
                    ("dave", 5, 40_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        1,
        &[
            ("bob", 0, 60_000_000_000, 20, 1),
            ("carol", 0, 60_000_000_000, 10, 1),
        ],
    );
}

#[test]
fn runtime_materializes_tumbling_event_time_order_by_sum_function_top_k() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by sum(amount) desc limit 1";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 70_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("bob", 12, 30_000_000_000, 1),
                    ("alice", 50, 70_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(runtime.as_ref(), 1, &[("bob", 0, 60_000_000_000, 12, 1)]);
}

#[test]
fn runtime_materializes_tumbling_event_time_count_distinct_and_restores_state() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(distinct amount) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 70_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("alice", 10, 20_000_000_000, 1),
                    ("alice", 7, 30_000_000_000, 1),
                    ("bob", 5, 30_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 27, 2),
            ("bob", 0, 60_000_000_000, 5, 1),
        ],
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let restored = restore_standing_runtime(checkpoint).unwrap();
    assert_window_page(
        restored.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 27, 2),
            ("bob", 0, 60_000_000_000, 5, 1),
        ],
    );
}

#[test]
fn runtime_materializes_hopping_event_time_windows() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id, hop(interval '30 seconds', interval '60 seconds')";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 90_000_000_000,
                    watermark_ns: 90_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("alice", 7, 40_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", -30_000_000_000, 30_000_000_000, 10, 1),
            ("alice", 0, 60_000_000_000, 17, 2),
            ("alice", 30_000_000_000, 90_000_000_000, 7, 1),
        ],
    );
}

#[test]
fn runtime_materializes_hopping_event_time_advanced_aggregates_having_top_k() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_stats_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(distinct amount) as event_count, min(amount) as minimum_amount, max(amount) as maximum_amount, avg(amount) as average_amount from purchases group by user_id, hop(interval '30 seconds', interval '60 seconds') having avg(amount) > 11 order by sum(amount) desc limit 1";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute_stats");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 90_000_000_000,
                    watermark_ns: 90_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("alice", 14, 20_000_000_000, 1),
                    ("alice", 10, 40_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_stats_page(
        runtime.as_ref(),
        1,
        &[("alice", 0, 60_000_000_000, 34, 2, 10, 14, 34.0 / 3.0)],
    );
}

#[test]
fn runtime_materializes_session_event_time_windows() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id, session(interval '30 seconds')";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("alice", 7, 25_000_000_000, 1),
                    ("alice", 11, 80_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 10_000_000_000, 25_000_000_000, 17, 2),
            ("alice", 80_000_000_000, 80_000_000_000, 11, 1),
        ],
    );
}

#[test]
fn runtime_materializes_session_event_time_advanced_aggregates_after_bridge_retract() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_stats_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(distinct amount) as event_count, min(amount) as minimum_amount, max(amount) as maximum_amount, avg(amount) as average_amount from purchases group by user_id, session(interval '30 seconds') having event_count > 1 order by average_amount desc limit 1";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute_stats");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 7,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 0, 1),
                    ("alice", 20, 60_000_000_000, 1),
                    ("alice", 10, 30_000_000_000, 1),
                    ("alice", 10, 30_000_000_000, -1),
                    ("carol", 8, 5_000_000_000, 1),
                    ("carol", 12, 20_000_000_000, 1),
                    ("bob", 50, 10_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_stats_page(
        runtime.as_ref(),
        1,
        &[("carol", 5_000_000_000, 20_000_000_000, 20, 2, 8, 12, 10.0)],
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let restored = restore_standing_runtime(checkpoint).unwrap();
    assert_window_stats_page(
        restored.as_ref(),
        1,
        &[("carol", 5_000_000_000, 20_000_000_000, 20, 2, 8, 12, 10.0)],
    );
}

#[test]
fn runtime_rejects_missing_session_event_retract_without_mutating_state() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id, session(interval '30 seconds')";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let err = runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 1,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[(
                    "alice",
                    10,
                    10_000_000_000,
                    -1,
                )])],
            }],
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StandingProgramRuntimeError::InvalidProgramIdentity { .. }
    ));
    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 1,
                end_offset_exclusive: 2,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[(
                    "alice",
                    10,
                    10_000_000_000,
                    1,
                )])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        2,
        &[("alice", 10_000_000_000, 10_000_000_000, 10, 1)],
    );
}

#[test]
fn runtime_materializes_tumbling_event_time_cte_source_filters() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "with purchase_source as (select * from purchases where amount > 5) select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchase_source, event_time, interval '60 seconds') where user_id <> 'bob' group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("bob", 8, 20_000_000_000, 1),
                    ("alice", 4, 30_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(runtime.as_ref(), 1, &[("alice", 0, 60_000_000_000, 10, 1)]);
}

#[test]
fn runtime_materializes_tumbling_event_time_derived_table_source_filters() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select p.user_id, window_start, window_end, sum(p.amount) as total_amount, count(*) as event_count from (select * from purchases where amount > 5) p where p.user_id <> 'bob' group by p.user_id, tumble(interval '60 seconds')";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("bob", 8, 20_000_000_000, 1),
                    ("alice", 4, 30_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(runtime.as_ref(), 1, &[("alice", 0, 60_000_000_000, 10, 1)]);
}

#[test]
fn runtime_materializes_tumbling_event_time_min_max_avg_and_restores_state() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_stats_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count, min(amount) as minimum_amount, max(amount) as maximum_amount, avg(amount) as average_amount from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute_stats");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 70_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("alice", 14, 20_000_000_000, 1),
                    ("bob", 5, 30_000_000_000, 1),
                    ("alice", 7, 70_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_stats_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 24, 2, 10, 14, 12.0),
            ("bob", 0, 60_000_000_000, 5, 1, 5, 5, 5.0),
        ],
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 6,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 120_000_000_000,
                    watermark_ns: 120_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 9, 80_000_000_000, 1),
                    ("bob", 11, 90_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_stats_page(
        restored.as_ref(),
        2,
        &[
            ("alice", 0, 60_000_000_000, 24, 2, 10, 14, 12.0),
            ("alice", 60_000_000_000, 120_000_000_000, 16, 2, 7, 9, 8.0),
            ("bob", 0, 60_000_000_000, 5, 1, 5, 5, 5.0),
            ("bob", 60_000_000_000, 120_000_000_000, 11, 1, 11, 11, 11.0),
        ],
    );
}

#[test]
fn runtime_materializes_tumbling_min_max_arithmetic_expression() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_stats_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count, min(amount + 1) as minimum_amount, max(amount + 1) as maximum_amount, avg(amount) as average_amount from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute_stats");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("alice", 7, 20_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_stats_page(
        runtime.as_ref(),
        1,
        &[("alice", 0, 60_000_000_000, 17, 2, 8, 11, 8.5)],
    );
}

#[test]
fn runtime_materializes_tumbling_avg_arithmetic_expression() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_stats_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count, min(amount) as minimum_amount, max(amount) as maximum_amount, avg(amount + 1) as average_amount from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute_stats");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("alice", 7, 20_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_stats_page(
        runtime.as_ref(),
        1,
        &[("alice", 0, 60_000_000_000, 17, 2, 7, 10, 9.5)],
    );
}

#[test]
fn runtime_materializes_tumbling_sum_arithmetic_expression() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount + 1) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("alice", 7, 20_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    assert_window_page(runtime.as_ref(), 1, &[("alice", 0, 60_000_000_000, 19, 2)]);
}

#[test]
fn runtime_rejects_late_rows_for_already_closed_tumbling_window() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 1,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[(
                    "alice",
                    10,
                    10_000_000_000,
                    1,
                )])],
            }],
        )
        .unwrap();

    let err = runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 1,
                end_offset_exclusive: 2,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[("bob", 9, 30_000_000_000, 1)])],
            }],
        )
        .unwrap_err();

    assert!(matches!(
        err,
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "tumbling_event_time_input_batch"
        }
    ));
}

fn standing_identity(sql: &str) -> StandingProgramIdentity {
    standing_identity_with_view(sql, "purchases_by_user")
}

fn standing_identity_with_view(sql: &str, view_id: &str) -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "tenant-a".to_string(),
        program_id: "program-purchases".to_string(),
        view_ids: vec![view_id.to_string()],
        sql_hash: stable_bytes_hash(sql.as_bytes()),
        input_catalog_hash: format!("sha256:{}", "1".repeat(64)),
        output_schema_hash: format!("sha256:{}", "2".repeat(64)),
        planner_identity: "velorix-logical-view-planner@1".to_string(),
        builtin_runtime_identities: vec![BuiltinRuntimeIdentity {
            name: CRATE_NAME.to_string(),
            version: "0.1.0".to_string(),
        }],
        runtime_capabilities: vec!["materialized-view-runtime-v1".to_string()],
        runtime_compatibility: "velorix-materialized-view-runtime-v1".to_string(),
        checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
    }
}

fn native_inner_join_aggregate_graph() -> NativeOperatorGraph {
    let mut graph = NativeOperatorGraph::new();
    graph
        .add_operator(NativeBinaryJoinOperator::new("join", |left, _right| {
            Ok(left.clone())
        }))
        .unwrap();
    graph
        .add_operator(NativeAggregateOperator::new(
            "aggregate",
            AggregateValueMode::Integer,
            false,
        ))
        .unwrap();
    graph.add_edge(NativeOperatorEdgeV1 {
        from_node_id: "join".to_string(),
        to_node_id: "aggregate".to_string(),
        to_port_id: "input".to_string(),
    });
    graph
}

fn native_left_join_aggregate_graph() -> NativeOperatorGraph {
    let mut graph = NativeOperatorGraph::new();
    graph
        .add_operator(NativeLeftJoinOperator::new(
            "join",
            |left, _right| Ok(left.clone()),
            |left| Ok(left.clone()),
        ))
        .unwrap();
    graph
        .add_operator(NativeAggregateOperator::new(
            "aggregate",
            AggregateValueMode::Integer,
            false,
        ))
        .unwrap();
    graph.add_edge(NativeOperatorEdgeV1 {
        from_node_id: "join".to_string(),
        to_node_id: "aggregate".to_string(),
        to_port_id: "input".to_string(),
    });
    graph
}

fn native_join_input(
    catalog: &VelorixRelationCatalogV1,
    key_column_id: &str,
    value_column_id: &str,
    batches: &[RecordBatch],
) -> DeltaBatch {
    arrow_record_batches_to_key_value_delta_batch(
        catalog,
        &catalog.relation_schema.relation_id,
        &catalog.relation_schema.relation_version,
        catalog.schema_fingerprint.as_str(),
        &[key_column_id.to_string()],
        value_column_id,
        batches,
    )
    .unwrap()
}

fn join_relation_input(
    catalog: &VelorixRelationCatalogV1,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    batches: Vec<RecordBatch>,
) -> RelationInputBatch {
    RelationInputBatch {
        encoding: RelationInputEncodingV1::SourceRelationV1,
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        stream_id: "specialization-equivalence".to_string(),
        partition_id: 0,
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        start_offset_inclusive,
        end_offset_exclusive,
        event_time_watermark: None,
        batches,
    }
}

fn apply_native_join_epoch(
    graph: &mut NativeOperatorGraph,
    epoch: u64,
    left: DeltaBatch,
    right: DeltaBatch,
) -> DeltaBatch {
    graph
        .apply_epoch(
            epoch,
            vec![
                NativeOperatorInputV1 {
                    node_id: "join".to_string(),
                    port_id: "left".to_string(),
                    batch: left,
                },
                NativeOperatorInputV1 {
                    node_id: "join".to_string(),
                    port_id: "right".to_string(),
                    batch: right,
                },
            ],
        )
        .unwrap()
        .remove("aggregate")
        .unwrap()
}

fn canonical_delta(batch: &DeltaBatch) -> DeltaBatch {
    DeltaBatch::from_records(batch.net_rows().unwrap())
}

fn assert_join_runtime_epoch_equivalent(
    selected: &mut Box<dyn StandingProgramRuntime + Send>,
    reference: &mut Box<dyn StandingProgramRuntime + Send>,
    logical_epoch: u64,
    input_changes: Vec<RelationInputBatch>,
) {
    let selected_commit = selected
        .apply_changes(
            logical_epoch,
            EpochIdempotencyKey::new(format!("selected-reference-{logical_epoch}")).unwrap(),
            input_changes.clone(),
        )
        .unwrap();
    let reference_commit = reference
        .apply_changes(
            logical_epoch,
            EpochIdempotencyKey::new(format!("selected-reference-{logical_epoch}")).unwrap(),
            input_changes,
        )
        .unwrap();
    assert_eq!(
        selected_commit.input_frontiers,
        reference_commit.input_frontiers
    );
    assert_eq!(
        selected_commit.input_event_time_frontiers,
        reference_commit.input_event_time_frontiers
    );
    assert_eq!(selected_commit.output_deltas.len(), 1);
    assert_eq!(reference_commit.output_deltas.len(), 1);
    assert!(specialization_delta_difference(
        &selected_commit.output_deltas[0].delta,
        &reference_commit.output_deltas[0].delta,
    )
    .is_none());
}

fn assert_join_checkpoint_canonical_equivalent(
    selected: &velorix_core::standing_program::RuntimeCheckpoint,
    reference: &velorix_core::standing_program::RuntimeCheckpoint,
) {
    assert_eq!(selected.logical_epoch, reference.logical_epoch);
    assert_eq!(selected.input_frontiers, reference.input_frontiers);
    assert_eq!(
        selected.input_event_time_frontiers,
        reference.input_event_time_frontiers
    );
    assert_eq!(selected.output_frontiers, reference.output_frontiers);
    let selected_payload: Value =
        serde_json::from_str(&selected.state_payload.as_ref().unwrap().payload).unwrap();
    let reference_payload: Value =
        serde_json::from_str(&reference.state_payload.as_ref().unwrap().payload).unwrap();
    assert_eq!(
        selected_payload.get("published_output"),
        reference_payload.get("published_output")
    );
    assert_eq!(
        selected_payload.get("applied_epochs"),
        reference_payload.get("applied_epochs")
    );
}

fn canonical_key_multiplicity(batch: &DeltaBatch) -> DeltaBatch {
    DeltaBatch::from_records(batch.net_rows().unwrap().into_iter().map(|row| {
        DeltaRecord::new(
            row.key.clone(),
            DeltaValue::from_json(row.key.as_json().clone()),
            row.weight,
        )
    }))
}

fn specialization_delta_difference(
    specialized: &DeltaBatch,
    generic: &DeltaBatch,
) -> Option<(DeltaBatch, DeltaBatch)> {
    let specialized = canonical_delta(specialized);
    let generic = canonical_delta(generic);
    (specialized != generic).then_some((specialized, generic))
}

fn native_checkpoint_state(
    checkpoint: &NativeOperatorGraphCheckpointV1,
    node_id: &str,
) -> NativeOperatorStateV1 {
    checkpoint
        .operators
        .iter()
        .find(|operator| operator.node_id == node_id)
        .unwrap()
        .state
        .clone()
}

fn runtime_join_checkpoint_state(
    runtime: &(dyn StandingProgramRuntime + Send),
) -> (
    u64,
    DeltaBatch,
    DeltaBatch,
    DeltaBatch,
    DeltaBatch,
    DeltaBatch,
) {
    let checkpoint = runtime.checkpoint().unwrap();
    let payload: Value = serde_json::from_str(
        &checkpoint
            .state_payload
            .as_ref()
            .expect("join checkpoint payload")
            .payload,
    )
    .unwrap();
    (
        checkpoint.logical_epoch,
        serde_json::from_value(payload["left_state"].clone()).unwrap(),
        serde_json::from_value(payload["right_state"].clone()).unwrap(),
        serde_json::from_value(payload["engine"]["state"].clone()).unwrap(),
        serde_json::from_value(payload["filtered_aggregate_state"].clone()).unwrap(),
        serde_json::from_value(payload["published_output"].clone()).unwrap(),
    )
}

fn assert_inner_join_specialization_state_equivalent(
    runtime: &(dyn StandingProgramRuntime + Send),
    graph: &NativeOperatorGraph,
) {
    let runtime_state = runtime_join_checkpoint_state(runtime);
    let graph_checkpoint = graph.checkpoint().unwrap();
    assert_eq!(runtime_state.0, graph_checkpoint.logical_epoch);
    let NativeOperatorStateV1::Binary {
        left_state,
        right_state,
    } = native_checkpoint_state(&graph_checkpoint, "join")
    else {
        panic!("join must have binary state");
    };
    let NativeOperatorStateV1::Unary { state: aggregate } =
        native_checkpoint_state(&graph_checkpoint, "aggregate")
    else {
        panic!("aggregate must have unary state");
    };
    assert_eq!(
        canonical_delta(&runtime_state.1),
        canonical_delta(&left_state)
    );
    assert_eq!(
        canonical_key_multiplicity(&runtime_state.2),
        canonical_key_multiplicity(&right_state)
    );
    assert_eq!(
        canonical_delta(&runtime_state.3),
        canonical_delta(&aggregate)
    );
    assert_eq!(
        canonical_delta(&runtime_state.5),
        canonical_delta(&aggregate)
    );
}

fn assert_left_join_specialization_state_equivalent(
    runtime: &(dyn StandingProgramRuntime + Send),
    graph: &NativeOperatorGraph,
) {
    let runtime_state = runtime_join_checkpoint_state(runtime);
    let graph_checkpoint = graph.checkpoint().unwrap();
    assert_eq!(runtime_state.0, graph_checkpoint.logical_epoch);
    let NativeOperatorStateV1::Unary { state: aggregate } =
        native_checkpoint_state(&graph_checkpoint, "aggregate")
    else {
        panic!("aggregate must have unary state");
    };
    // The narrow specialization deliberately omits generic join-side state.
    // Its canonical logical state is the left-only aggregate and published bag.
    assert_eq!(
        canonical_delta(&runtime_state.4),
        canonical_delta(&aggregate)
    );
    assert_eq!(
        canonical_delta(&runtime_state.5),
        canonical_delta(&aggregate)
    );
}

fn assert_general_join_specialization_state_equivalent(
    runtime: &(dyn StandingProgramRuntime + Send),
    graph: &JoinSpecializationComparisonGraph,
) {
    let runtime_state = runtime_join_checkpoint_state(runtime);
    let graph_checkpoint = graph.checkpoint().unwrap();
    assert_eq!(runtime_state.0, graph_checkpoint.logical_epoch);
    let NativeOperatorStateV1::Unary { state: aggregate } =
        native_checkpoint_state(&graph_checkpoint, "aggregate")
    else {
        panic!("aggregate must have unary state");
    };
    let NativeOperatorStateV1::Binary {
        left_state: publisher_full,
        right_state: publisher_visible,
    } = native_checkpoint_state(&graph_checkpoint, "publish")
    else {
        panic!("publisher must have binary state");
    };
    let runtime_aggregate = if runtime_state.4.records().is_empty() {
        &runtime_state.3
    } else {
        &runtime_state.4
    };
    assert_eq!(
        canonical_delta(runtime_aggregate),
        canonical_delta(&aggregate)
    );
    assert_eq!(
        canonical_delta(runtime_aggregate),
        canonical_delta(&publisher_full)
    );
    assert_eq!(
        canonical_delta(&runtime_state.5),
        canonical_delta(&publisher_visible)
    );
}

fn assert_join_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64, i64)],
) {
    assert_join_page_for_view(runtime, "scores_by_account", epoch, expected);
}

fn assert_nullable_join_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, Option<i64>, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_account".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let account_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), expected.len());
    for (index, (account_id, sum, count)) in expected.iter().enumerate() {
        assert_eq!(account_ids.value(index), *account_id);
        match sum {
            Some(sum) => assert_eq!(sums.value(index), *sum),
            None => assert!(sums.is_null(index)),
        }
        assert_eq!(counts.value(index), *count);
    }
}

fn assert_join_page_for_view(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    view_id: &str,
    epoch: u64,
    expected: &[(&str, i64, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: view_id.to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let account_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), expected.len());
    for (index, (account_id, sum, count)) in expected.iter().enumerate() {
        assert_eq!(account_ids.value(index), *account_id);
        assert_eq!(sums.value(index), *sum);
        assert_eq!(counts.value(index), *count);
    }
}

fn assert_int_join_page_for_view(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    view_id: &str,
    epoch: u64,
    expected: &[(i64, i64, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: view_id.to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (key, sum, count)) in expected.iter().enumerate() {
        assert_eq!(keys.value(index), *key);
        assert_eq!(sums.value(index), *sum);
        assert_eq!(counts.value(index), *count);
    }
}

fn assert_join_adjusted_sum_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64, i64, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_account_adjusted".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let account_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let adjusted_sums = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(2).name(), "adjusted_sum");
    assert_eq!(batch.num_rows(), expected.len());
    for (row, (account_id, sum, adjusted_sum, count)) in expected.iter().enumerate() {
        assert_eq!(account_ids.value(row), *account_id);
        assert_eq!(sums.value(row), *sum);
        assert_eq!(adjusted_sums.value(row), *adjusted_sum);
        assert_eq!(counts.value(row), *count);
    }
}

fn assert_join_count_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_account_count".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let account_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(0).name(), "account_id");
    assert_eq!(batch.schema().field(1).name(), "count");
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (account_id, count)) in expected.iter().enumerate() {
        assert_eq!(account_ids.value(index), *account_id);
        assert_eq!(counts.value(index), *count);
    }
}

fn assert_row_number_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_ranked".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ranks = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(0).name(), "user_id");
    assert_eq!(batch.schema().field(1).name(), "rank");
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (user_id, rank)) in expected.iter().enumerate() {
        assert_eq!(user_ids.value(index), *user_id);
        assert_eq!(ranks.value(index), *rank);
    }
}

fn assert_row_number_delta(delta: &DeltaBatch, expected: &[(&str, i64, i64)]) {
    let mut rows = delta
        .net_rows()
        .unwrap()
        .into_iter()
        .map(|row| {
            let user_id = row.key.as_json().as_str().unwrap().to_string();
            let rank = row
                .value
                .as_json()
                .as_object()
                .unwrap()
                .get("rank")
                .and_then(Value::as_i64)
                .unwrap();
            (user_id, rank, row.weight)
        })
        .collect::<Vec<_>>();
    rows.sort();
    let mut expected = expected
        .iter()
        .map(|(user_id, rank, weight)| ((*user_id).to_string(), *rank, *weight))
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(rows, expected);
}

fn assert_join_distinct_count_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_account_distinct_count".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let account_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(0).name(), "account_id");
    assert_eq!(batch.schema().field(1).name(), "distinct_scores");
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (account_id, count)) in expected.iter().enumerate() {
        assert_eq!(account_ids.value(index), *account_id);
        assert_eq!(counts.value(index), *count);
    }
}

fn assert_join_left_key_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let users = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(0).name(), "user");
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (user, sum, count)) in expected.iter().enumerate() {
        assert_eq!(users.value(index), *user);
        assert_eq!(sums.value(index), *sum);
        assert_eq!(counts.value(index), *count);
    }
}

type JoinStatsRow<'a> = (&'a str, i64, i64, i64, i64, f64);
type JoinRightStatsRow<'a> = (&'a str, i64, i64, i64, i64, i64, i64, i64, f64);

fn assert_join_stats_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[JoinStatsRow<'_>],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_account_stats".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let account_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let minimums = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let maximums = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let averages = batch
        .column(5)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(1).name(), "sum");
    assert_eq!(batch.schema().field(2).name(), "count");
    assert_eq!(batch.schema().field(3).name(), "min_score");
    assert_eq!(batch.schema().field(4).name(), "max_score");
    assert_eq!(batch.schema().field(5).name(), "avg_score");
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (account_id, sum, count, minimum, maximum, average)) in expected.iter().enumerate()
    {
        assert_eq!(account_ids.value(index), *account_id);
        assert_eq!(sums.value(index), *sum);
        assert_eq!(counts.value(index), *count);
        assert_eq!(minimums.value(index), *minimum);
        assert_eq!(maximums.value(index), *maximum);
        assert_eq!(averages.value(index), *average);
    }
}

fn assert_join_right_stats_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[JoinRightStatsRow<'_>],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_account_limits".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let account_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let limit_counts = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let distinct_limits = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let limit_sums = batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let minimums = batch
        .column(6)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let maximums = batch
        .column(7)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let averages = batch
        .column(8)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(3).name(), "count_limit");
    assert_eq!(batch.schema().field(4).name(), "distinct_limits");
    assert_eq!(batch.schema().field(5).name(), "limit_sum");
    assert_eq!(batch.schema().field(6).name(), "min_limit");
    assert_eq!(batch.schema().field(7).name(), "max_limit");
    assert_eq!(batch.schema().field(8).name(), "avg_limit");
    assert_eq!(batch.num_rows(), expected.len());
    for (
        index,
        (account_id, sum, count, limit_count, distinct_limit, limit_sum, minimum, maximum, average),
    ) in expected.iter().enumerate()
    {
        assert_eq!(account_ids.value(index), *account_id);
        assert_eq!(sums.value(index), *sum);
        assert_eq!(counts.value(index), *count);
        assert_eq!(limit_counts.value(index), *limit_count);
        assert_eq!(distinct_limits.value(index), *distinct_limit);
        assert_eq!(limit_sums.value(index), *limit_sum);
        assert_eq!(minimums.value(index), *minimum);
        assert_eq!(maximums.value(index), *maximum);
        assert_eq!(averages.value(index), *average);
    }
}

fn assert_left_join_right_sum_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64, Option<i64>, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "left_join_right_sum".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let limit_sums = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (key, sum, limit_sum, count)) in expected.iter().enumerate() {
        assert_eq!(keys.value(index), *key);
        assert_eq!(sums.value(index), *sum);
        assert_eq!(
            (!limit_sums.is_null(index)).then(|| limit_sums.value(index)),
            *limit_sum
        );
        assert_eq!(counts.value(index), *count);
    }
}

fn assert_left_join_empty_right_stats_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected_keys: &[&str],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_account_limits".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let count_limits = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let distinct_limits = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(batch.num_rows(), expected_keys.len());
    for (row, expected_key) in expected_keys.iter().enumerate() {
        assert_eq!(keys.value(row), *expected_key);
        assert_eq!(count_limits.value(row), 0);
        assert_eq!(distinct_limits.value(row), 0);
        for column in 5..=8 {
            assert!(batch.column(column).is_null(row));
        }
    }
}

fn assert_join_decimal_avg_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64, i64, f64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_account_decimal_avg".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let account_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let averages = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(3).name(), "avg_limit");
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (account_id, sum, count, average)) in expected.iter().enumerate() {
        assert_eq!(account_ids.value(index), *account_id);
        assert_eq!(sums.value(index), *sum);
        assert_eq!(counts.value(index), *count);
        assert_eq!(averages.value(index), *average);
    }
}

fn assert_join_right_nullable_count_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64, i64, i64, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_nullable_account_limits".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let account_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let limit_counts = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let distinct_limits = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(3).name(), "count_limit");
    assert_eq!(batch.schema().field(4).name(), "distinct_limits");
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (account_id, sum, count, limit_count, distinct_limit)) in
        expected.iter().enumerate()
    {
        assert_eq!(account_ids.value(index), *account_id);
        assert_eq!(sums.value(index), *sum);
        assert_eq!(counts.value(index), *count);
        assert_eq!(limit_counts.value(index), *limit_count);
        assert_eq!(distinct_limits.value(index), *distinct_limit);
    }
}

fn assert_join_right_multi_value_stats_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64, i64, i64, i64, f64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_account_quotas".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let account_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let min_limits = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let max_quotas = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let avg_quotas = batch
        .column(5)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(3).name(), "min_limit");
    assert_eq!(batch.schema().field(4).name(), "max_quota");
    assert_eq!(batch.schema().field(5).name(), "avg_quota");
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (account_id, sum, count, min_limit, max_quota, avg_quota)) in
        expected.iter().enumerate()
    {
        assert_eq!(account_ids.value(index), *account_id);
        assert_eq!(sums.value(index), *sum);
        assert_eq!(counts.value(index), *count);
        assert_eq!(min_limits.value(index), *min_limit);
        assert_eq!(max_quotas.value(index), *max_quota);
        assert_eq!(avg_quotas.value(index), *avg_quota);
    }
}

fn assert_count_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user_count".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), expected.len());
    for (index, (user_id, count)) in expected.iter().enumerate() {
        assert_eq!(user_ids.value(index), *user_id);
        assert_eq!(counts.value(index), *count);
    }
}

fn assert_projected_scores_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "positive_scores".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let scores = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), expected.len());
    for (index, (user_id, score)) in expected.iter().enumerate() {
        assert_eq!(user_ids.value(index), *user_id);
        assert_eq!(scores.value(index), *score);
    }
}

fn assert_projected_user_ids_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[&str],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "positive_scores".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "user_id");
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(batch.num_rows(), expected.len());
    for (index, user_id) in expected.iter().enumerate() {
        assert_eq!(user_ids.value(index), *user_id);
    }
}

fn assert_projected_distinct_scores_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[i64],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "positive_scores".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "score");
    let scores = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), expected.len());
    for (index, score) in expected.iter().enumerate() {
        assert_eq!(scores.value(index), *score);
    }
}

fn assert_projected_nullable_scores_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, Option<i64>)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "positive_scores".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let scores = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), expected.len());
    for (index, (user_id, score)) in expected.iter().enumerate() {
        assert_eq!(user_ids.value(index), *user_id);
        match score {
            Some(score) => assert_eq!(scores.value(index), *score),
            None => assert!(scores.is_null(index)),
        }
    }
}

fn assert_sum_count_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    view_id: &str,
    epoch: u64,
    expected: &[(&str, i64, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: view_id.to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), expected.len());
    for (index, (user_id, sum, count)) in expected.iter().enumerate() {
        assert_eq!(user_ids.value(index), *user_id);
        assert_eq!(sums.value(index), *sum);
        assert_eq!(counts.value(index), *count);
    }
}

fn assert_latest_status_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, bool)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "latest_device_status".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let device_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let enabled = batch
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();

    assert_eq!(batch.num_rows(), expected.len());
    for (index, (device_id, is_enabled)) in expected.iter().enumerate() {
        assert_eq!(device_ids.value(index), *device_id);
        assert_eq!(enabled.value(index), *is_enabled);
    }
}

fn assert_device_enabled_flags_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "device_enabled_flags".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let device_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let flags = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), expected.len());
    for (index, (device_id, enabled_flag)) in expected.iter().enumerate() {
        assert_eq!(device_ids.value(index), *device_id);
        assert_eq!(flags.value(index), *enabled_flag);
    }
}

fn assert_window_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64, i64, i64, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user_minute".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let window_starts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let window_ends = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let totals = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), expected.len());
    for (index, (user_id, window_start, window_end, total, count)) in expected.iter().enumerate() {
        assert_eq!(user_ids.value(index), *user_id);
        assert_eq!(window_starts.value(index), *window_start);
        assert_eq!(window_ends.value(index), *window_end);
        assert_eq!(totals.value(index), *total);
        assert_eq!(counts.value(index), *count);
    }
}

type WindowStatsRow<'a> = (&'a str, i64, i64, i64, i64, i64, i64, f64);

fn assert_window_stats_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[WindowStatsRow<'_>],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user_minute_stats".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let window_starts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let window_ends = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let totals = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let minimums = batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let maximums = batch
        .column(6)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let averages = batch
        .column(7)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(3).name(), "total_amount");
    assert_eq!(batch.schema().field(4).name(), "event_count");
    assert_eq!(batch.schema().field(5).name(), "minimum_amount");
    assert_eq!(batch.schema().field(6).name(), "maximum_amount");
    assert_eq!(batch.schema().field(7).name(), "average_amount");
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (user_id, window_start, window_end, total, count, minimum, maximum, average)) in
        expected.iter().enumerate()
    {
        assert_eq!(user_ids.value(index), *user_id);
        assert_eq!(window_starts.value(index), *window_start);
        assert_eq!(window_ends.value(index), *window_end);
        assert_eq!(totals.value(index), *total);
        assert_eq!(counts.value(index), *count);
        assert_eq!(minimums.value(index), *minimum);
        assert_eq!(maximums.value(index), *maximum);
        assert_eq!(averages.value(index), *average);
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
        relation_source: VelorixRelationSourceV1::SourceRelation,
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

fn generic_adapter_catalog(mut catalog: VelorixRelationCatalogV1) -> VelorixRelationCatalogV1 {
    catalog.incremental_adapter.adapter_id = CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string();
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

fn three_input_composite_join_catalogs() -> [VelorixRelationCatalogV1; 3] {
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
    [scores, accounts, profiles]
}

fn scores_with_category_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = scores_catalog();
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
    for (ordinal, column) in catalog.relation_schema.columns.iter_mut().enumerate() {
        column.ordinal = ordinal as u32;
    }
    let schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    generic_adapter_catalog(catalog)
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

fn scores_catalog_with_nullable_score() -> VelorixRelationCatalogV1 {
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
        relation_source: VelorixRelationSourceV1::SourceRelation,
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

fn accounts_nullable_limit_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = accounts_catalog();
    let limit = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "limit")
        .unwrap();
    limit.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable account limit catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
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
        .expect("decimal account limit catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
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
        relation_source: VelorixRelationSourceV1::SourceRelation,
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
        relation_source: VelorixRelationSourceV1::SourceRelation,
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

fn purchases_catalog_with_nullable_amount() -> VelorixRelationCatalogV1 {
    let mut catalog = purchases_catalog_without_value_role();
    let amount = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "amount")
        .unwrap();
    amount.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable amount catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
}

fn purchases_decimal_amount_catalog() -> VelorixRelationCatalogV1 {
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
        .expect("decimal amount catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
}

fn purchases_catalog_with_amount_event_time() -> VelorixRelationCatalogV1 {
    let mut catalog = purchases_catalog_without_value_role();
    catalog.relation_schema.event_time_column_id = Some("amount".to_string());
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("event-time purchases schema should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
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

fn purchases_event_time_catalog_with_nullable_amount() -> VelorixRelationCatalogV1 {
    let mut catalog = purchases_event_time_catalog();
    let amount = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "amount")
        .unwrap();
    amount.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable event-time purchases schema should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
}

fn purchases_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "purchases_by_user".to_string(),
        relation_name: "purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
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

fn composite_bucket_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_user_bucket".to_string(),
        relation_name: "scores_by_user_bucket".to_string(),
        relation_version: "2026-08-10.v1".to_string(),
        schema_fingerprint:
            "sha256:2000000000000000000000000000000000000000000000000000000000000002".to_string(),
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
    }
}

fn composite_join_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_tenant".into(),
        relation_name: "scores_by_tenant".into(),
        relation_version: "2026-08-10.v1".into(),
        schema_fingerprint:
            "sha256:6000000000000000000000000000000000000000000000000000000000000006".into(),
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
            "sha256:7000000000000000000000000000000000000000000000000000000000000007".into(),
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

fn global_count_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "score_count".to_string(),
        relation_name: "score_count".to_string(),
        relation_version: "2026-08-10.v1".to_string(),
        schema_fingerprint:
            "sha256:5000000000000000000000000000000000000000000000000000000000000005".to_string(),
        columns: vec![ColumnSchema {
            name: "count".to_string(),
            data_type: SqlDataType::Int64,
            nullable: false,
        }],
        primary_key: Vec::new(),
    }
}

fn assert_global_count_batch(batch: &RecordBatch, expected: i64) {
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 1);
    let count = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), expected);
}

fn composite_category_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_user_category".to_string(),
        relation_name: "scores_by_user_category".to_string(),
        relation_version: "2026-08-10.v1".to_string(),
        schema_fingerprint:
            "sha256:3000000000000000000000000000000000000000000000000000000000000003".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "category".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: true,
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
        primary_key: vec!["user_id".to_string(), "category".to_string()],
    }
}

fn scores_total_score_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_user".to_string(),
        relation_name: "scores_by_user".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "scores-total-score-v1".to_string(),
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
                name: "event_count".to_string(),
                data_type: SqlDataType::Int64,
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

fn scores_multi_input_distinct_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_user_distinct_adjustment".to_string(),
        relation_name: "scores_by_user_distinct_adjustment".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "scores-multi-input-distinct-v1".to_string(),
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
                name: "distinct_adjustments".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn purchases_count_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "purchases_by_user_count".to_string(),
        relation_name: "purchases_by_user_count".to_string(),
        relation_version: "v1".to_string(),
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

fn assert_top_purchase_user(
    runtime: &(dyn StandingProgramRuntime + Send),
    epoch: u64,
    expected_user: &str,
    expected_total: i64,
    expected_events: i64,
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "purchases_by_user".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let totals = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let events = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(user_ids.value(0), expected_user);
    assert_eq!(totals.value(0), expected_total);
    assert_eq!(events.value(0), expected_events);
}

fn assert_scores_min_max_avg_page(
    runtime: &(dyn StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64, i64, f64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_user_stats".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let minimums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let maximums = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let averages = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(1).name(), "min_pos");
    assert_eq!(batch.schema().field(2).name(), "max_nonpos");
    assert_eq!(batch.schema().field(3).name(), "avg_hi");
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (user_id, minimum, maximum, average)) in expected.iter().enumerate() {
        assert_eq!(user_ids.value(index), *user_id);
        assert_eq!(minimums.value(index), *minimum);
        assert_eq!(maximums.value(index), *maximum);
        assert_eq!(averages.value(index), *average);
    }
}

fn assert_scores_multi_input_stats_page(
    runtime: &(dyn StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64, i64, i64, f64, i64)],
) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "scores_by_user_multi_input_stats".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(epoch),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();

    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let minimums = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let maximums = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let averages = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let counts = batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.schema().field(1).name(), "sum_score");
    assert_eq!(batch.schema().field(2).name(), "min_adj");
    assert_eq!(batch.schema().field(3).name(), "max_adj");
    assert_eq!(batch.schema().field(4).name(), "avg_adj");
    assert_eq!(batch.schema().field(5).name(), "count_adj");
    assert_eq!(batch.num_rows(), expected.len());
    for (index, (user_id, sum, minimum, maximum, average, count)) in expected.iter().enumerate() {
        assert_eq!(user_ids.value(index), *user_id);
        assert_eq!(sums.value(index), *sum);
        assert_eq!(minimums.value(index), *minimum);
        assert_eq!(maximums.value(index), *maximum);
        assert_eq!(averages.value(index), *average);
        assert_eq!(counts.value(index), *count);
    }
}

fn typed_expressions_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "typed_projection_view".to_string(),
        relation_name: "typed_projection_view".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "typed-projection-v1".to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "user_upper".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: true,
            },
            ColumnSchema {
                name: "user_tag".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: true,
            },
            ColumnSchema {
                name: "event_year".to_string(),
                data_type: SqlDataType::Int64,
                nullable: true,
            },
            ColumnSchema {
                name: "hour_trunc".to_string(),
                data_type: SqlDataType::Timestamp { timezone: None },
                nullable: true,
            },
            ColumnSchema {
                name: "plus_hour".to_string(),
                data_type: SqlDataType::Timestamp { timezone: None },
                nullable: true,
            },
            ColumnSchema {
                name: "amount_f64".to_string(),
                data_type: SqlDataType::Float64,
                nullable: true,
            },
            ColumnSchema {
                name: "amount_abs".to_string(),
                data_type: SqlDataType::Float64,
                nullable: true,
            },
            ColumnSchema {
                name: "amount_round".to_string(),
                data_type: SqlDataType::Float64,
                nullable: true,
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
        relation_id: "scores_ranked".to_string(),
        relation_name: "scores_ranked".to_string(),
        relation_version: "v1".to_string(),
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

fn purchases_avg_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "purchases_by_user".to_string(),
        relation_name: "purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
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

fn purchases_decimal_avg_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "purchases_by_user".to_string(),
        relation_name: "purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:000000000000000000000000000000000000000000000000000000000000000b".to_string(),
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
    }
}

fn purchases_min_max_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "purchases_by_user".to_string(),
        relation_name: "purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
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
        relation_version: "v1".to_string(),
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
        relation_version: "v1".to_string(),
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
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000006".to_string(),
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

fn join_adjusted_sum_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_account_adjusted".to_string(),
        relation_name: "scores_by_account_adjusted".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000029".to_string(),
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
        primary_key: vec!["account_id".to_string()],
    }
}

fn join_count_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_account_count".to_string(),
        relation_name: "scores_by_account_count".to_string(),
        relation_version: "v1".to_string(),
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
        relation_version: "v1".to_string(),
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
        relation_version: "v1".to_string(),
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

fn join_distinct_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_account".to_string(),
        relation_name: "scores_by_account".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000007".to_string(),
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
                name: "distinct_scores".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["account_id".to_string()],
    }
}

fn join_alias_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_account".to_string(),
        relation_name: "scores_by_account".to_string(),
        relation_version: "v1".to_string(),
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
        relation_version: "v1".to_string(),
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
        relation_version: "v1".to_string(),
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

fn left_join_right_sum_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "left_join_right_sum".to_string(),
        relation_name: "left_join_right_sum".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000022".to_string(),
        columns: vec![
            ColumnSchema {
                name: "account_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: true,
            },
            ColumnSchema {
                name: "limit_sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: true,
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

fn join_decimal_avg_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_account_decimal_avg".to_string(),
        relation_name: "scores_by_account_decimal_avg".to_string(),
        relation_version: "v1".to_string(),
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

fn join_right_nullable_count_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_nullable_account_limits".to_string(),
        relation_name: "scores_by_nullable_account_limits".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000011".to_string(),
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
        ],
        primary_key: vec!["account_id".to_string()],
    }
}

fn join_right_multi_value_stats_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "scores_by_account_quotas".to_string(),
        relation_name: "scores_by_account_quotas".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000012".to_string(),
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
                name: "min_limit".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "max_quota".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "avg_quota".to_string(),
                data_type: SqlDataType::Float64,
                nullable: false,
            },
        ],
        primary_key: vec!["account_id".to_string()],
    }
}

fn join_stats_logical_plan(
    sql: &str,
    scores: &VelorixRelationCatalogV1,
    accounts: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
) -> VelorixLogicalViewPlanV1 {
    lower_supported_join_view_sql_to_logical_plan(
        sql,
        &[scores.clone(), accounts.clone()],
        output_schema,
    )
    .unwrap()
}

fn row_number_logical_plan(
    sql: &str,
    catalog: &VelorixRelationCatalogV1,
    output_schema: &RelationSchema,
) -> VelorixLogicalViewPlanV1 {
    let admitted_sql = "select user_id, row_number() over (partition by score order by score desc, user_id asc) as rank from scores where score > 0";
    let mut logical_plan = lower_supported_analytic_row_number_sql_to_logical_plan(
        admitted_sql,
        catalog,
        output_schema,
    )
    .unwrap();
    logical_plan.view_sql = sql.to_string();
    logical_plan.plan_hash = None;
    logical_plan.plan_hash = Some(logical_view_plan_hash(&logical_plan).unwrap());
    logical_plan
}

fn device_status_batch(rows: &[(&str, bool, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("device_id", DataType::Utf8, false),
            Field::new("enabled", DataType::Boolean, false),
            Field::new("event_time", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(device_id, _, _, _)| *device_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|(_, enabled, _, _)| *enabled)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, event_time, _)| *event_time)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, _, delta)| *delta)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn purchases_batch() -> RecordBatch {
    purchases_rows_batch(&[("alice", 10, 1), ("bob", 5, 1), ("alice", 7, 1)])
}

fn purchases_nullable_amount_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob", "alice"])) as _,
            Arc::new(Int64Array::from(vec![Some(10), Some(5), None])) as _,
            Arc::new(Int64Array::from(vec![1, 1, 1])) as _,
        ],
    )
    .unwrap()
}

fn purchases_decimal_amount_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("amount", DataType::Decimal128(12, 2), false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob", "alice"])) as _,
            Arc::new(
                Decimal128Array::from(vec![1000, 500, 700])
                    .with_precision_and_scale(12, 2)
                    .unwrap(),
            ) as _,
            Arc::new(Int64Array::from(vec![1, 1, 1])) as _,
        ],
    )
    .unwrap()
}

fn purchases_rows_batch(rows: &[(&str, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(user_id, _, _)| *user_id)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, amount, _)| *amount)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, _, delta)| *delta).collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn purchases_event_time_batch(rows: &[(&str, i64, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("event_time", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(user_id, _, _, _)| *user_id)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, amount, _, _)| *amount)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, event_time, _)| *event_time)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, _, delta)| *delta)
                    .collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn purchases_event_time_nullable_amount_batch(
    rows: &[(&str, Option<i64>, i64, i64)],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
            Field::new("event_time", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(user_id, _, _, _)| *user_id)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, amount, _, _)| *amount)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, event_time, _)| *event_time)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, _, delta)| *delta)
                    .collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn purchase_delete_batch(user_id: &str, amount: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![user_id])) as _,
            Arc::new(Int64Array::from(vec![amount])) as _,
            Arc::new(Int64Array::from(vec![-1])) as _,
        ],
    )
    .unwrap()
}

fn scores_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob", "alice"])) as _,
            Arc::new(Int64Array::from(vec![10, 5, 7])) as _,
            Arc::new(Int64Array::from(vec![1, 1, 1])) as _,
        ],
    )
    .unwrap()
}

fn relation_input(
    catalog: &VelorixRelationCatalogV1,
    stream_id: &str,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    batch: RecordBatch,
) -> RelationInputBatch {
    RelationInputBatch {
        encoding: RelationInputEncodingV1::SourceRelationV1,
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        stream_id: stream_id.into(),
        partition_id: 0,
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        start_offset_inclusive,
        end_offset_exclusive,
        event_time_watermark: None,
        batches: vec![batch],
    }
}

fn composite_scores_rows_batch(rows: &[(&str, i64, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn composite_accounts_rows_batch(rows: &[(&str, i64, &str, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("limit", DataType::Int64, false),
            Field::new("tier", DataType::Utf8, false),
            Field::new("account_tenant_id", DataType::Utf8, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.4).collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn scores_with_category_batch() -> RecordBatch {
    scores_with_category_rows_batch(&[
        ("u1", 5, Some("a"), 1),
        ("u1", 7, Some("a"), 1),
        ("u1", 15, None, 1),
    ])
}

fn scores_with_category_rows_batch(rows: &[(&str, i64, Option<&str>, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new("category", DataType::Utf8, true),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(user_id, _, _, _)| *user_id)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, score, _, _)| *score)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(_, _, category, _)| *category)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, _, delta)| *delta)
                    .collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn scores_rows_batch(rows: &[(&str, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(user_id, _, _)| *user_id)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, score, _)| *score).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, _, delta)| *delta).collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn scores_with_adjustment_rows_batch(rows: &[(&str, i64, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new("user_id_adjustment", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(user_id, _, _, _)| *user_id)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, score, _, _)| *score)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, adjustment, _)| *adjustment)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, _, delta)| *delta)
                    .collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn scores_with_nullable_adjustment_rows_batch(
    rows: &[(&str, i64, Option<i64>, i64)],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new("user_id_adjustment", DataType::Int64, true),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(user_id, _, _, _)| *user_id)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, score, _, _)| *score)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, adjustment, _)| *adjustment)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, _, delta)| *delta)
                    .collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn scores_nullable_rows_batch(rows: &[(&str, Option<i64>, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, true),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(user_id, _, _)| *user_id)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, score, _)| *score).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, _, delta)| *delta).collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn scores_nullable_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, true),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["alice", "alice", "alice", "bob"])) as _,
            Arc::new(Int64Array::from(vec![Some(10), None, Some(7), Some(5)])) as _,
            Arc::new(Int64Array::from(vec![1, 1, 1, 1])) as _,
        ],
    )
    .unwrap()
}

fn accounts_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("limit", DataType::Int64, false),
            Field::new("tier", DataType::Utf8, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])) as _,
            Arc::new(Int64Array::from(vec![100, 50, 100])) as _,
            Arc::new(StringArray::from(vec!["gold", "gold", "silver"])) as _,
            Arc::new(Int64Array::from(vec![1, 1, 1])) as _,
        ],
    )
    .unwrap()
}

fn accounts_alice_bob_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("limit", DataType::Int64, false),
            Field::new("tier", DataType::Utf8, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])) as _,
            Arc::new(Int64Array::from(vec![100, 50])) as _,
            Arc::new(StringArray::from(vec!["gold", "gold"])) as _,
            Arc::new(Int64Array::from(vec![1, 1])) as _,
        ],
    )
    .unwrap()
}

fn accounts_rows_batch(rows: &[(&str, i64, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("limit", DataType::Int64, false),
            Field::new("tier", DataType::Utf8, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(account_id, _, _, _)| *account_id)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, limit, _, _)| *limit)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringArray::from(
                rows.iter().map(|(_, _, tier, _)| *tier).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, _, delta)| *delta)
                    .collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn accounts_decimal_limit_rows_batch(rows: &[(&str, i128, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("limit", DataType::Decimal128(12, 2), false),
            Field::new("tier", DataType::Utf8, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(account_id, _, _, _)| *account_id)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(
                Decimal128Array::from(
                    rows.iter()
                        .map(|(_, limit, _, _)| *limit)
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(12, 2)
                .unwrap(),
            ) as _,
            Arc::new(StringArray::from(
                rows.iter().map(|(_, _, tier, _)| *tier).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, _, delta)| *delta)
                    .collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn accounts_nullable_limit_rows_batch(rows: &[(&str, Option<i64>, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("limit", DataType::Int64, true),
            Field::new("tier", DataType::Utf8, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(account_id, _, _, _)| *account_id)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, limit, _, _)| *limit)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringArray::from(
                rows.iter().map(|(_, _, tier, _)| *tier).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, _, delta)| *delta)
                    .collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn accounts_multi_value_rows_batch(rows: &[(&str, i64, i64, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("limit", DataType::Int64, false),
            Field::new("quota", DataType::Int64, false),
            Field::new("tier", DataType::Utf8, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(account_id, _, _, _, _)| *account_id)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, limit, _, _, _)| *limit)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, quota, _, _)| *quota)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(_, _, _, tier, _)| *tier)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, _, _, _, delta)| *delta)
                    .collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

fn accounts_using_user_id_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("limit", DataType::Int64, false),
            Field::new("tier", DataType::Utf8, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])) as _,
            Arc::new(Int64Array::from(vec![100, 50, 100])) as _,
            Arc::new(StringArray::from(vec!["gold", "gold", "silver"])) as _,
            Arc::new(Int64Array::from(vec![1, 1, 1])) as _,
        ],
    )
    .unwrap()
}

fn score_append_batch(user_id: &str, score: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![user_id])) as _,
            Arc::new(Int64Array::from(vec![score])) as _,
            Arc::new(Int64Array::from(vec![1])) as _,
        ],
    )
    .unwrap()
}

#[test]
fn late_row_policy_drop_with_evidence_drops_late_rows_and_persists_evidence() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let logical_plan = lower_supported_tumbling_window_sql_to_logical_plan_with_policy(
        sql,
        &catalog,
        &output_schema,
        Some(LateRowPolicy::DropWithEvidence),
    )
    .unwrap();
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    // Epoch 1 establishes the watermark frontier (no lateness possible on the
    // very first batch of a partition).
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("bob", 45, 45_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();
    assert_window_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 10, 1),
            ("bob", 0, 60_000_000_000, 45, 1),
        ],
    );

    // Epoch 2: carol@20s is late (20 < 60 vs the established frontier) and
    // must be dropped with evidence instead of failing.
    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[(
                    "carol",
                    99,
                    20_000_000_000,
                    1,
                )])],
            }],
        )
        .unwrap();

    assert_window_page(
        runtime.as_ref(),
        2,
        &[
            ("alice", 0, 60_000_000_000, 10, 1),
            ("bob", 0, 60_000_000_000, 45, 1),
        ],
    );

    // Evidence is part of the durable checkpoint payload.
    let checkpoint = runtime.checkpoint().unwrap();
    let payload: serde_json::Value =
        serde_json::from_str(&checkpoint.state_payload.as_ref().unwrap().payload).unwrap();
    assert_eq!(payload["state"]["late_rows_dropped"], 1);

    // Restart replays the same evidence and stays exact.
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    // A late row after restart is still dropped with evidence (counter
    // accumulates deterministically across the checkpoint boundary).
    restored
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[(
                    "dave",
                    99,
                    30_000_000_000,
                    1,
                )])],
            }],
        )
        .unwrap();
    assert_window_page(
        restored.as_ref(),
        3,
        &[
            ("alice", 0, 60_000_000_000, 10, 1),
            ("bob", 0, 60_000_000_000, 45, 1),
        ],
    );
    let checkpoint = restored.checkpoint().unwrap();
    let payload: serde_json::Value =
        serde_json::from_str(&checkpoint.state_payload.as_ref().unwrap().payload).unwrap();
    assert_eq!(payload["state"]["late_rows_dropped"], 2);
}

#[test]
fn late_row_policy_admit_within_allowance_defers_finalization_until_frontier() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let logical_plan = lower_supported_tumbling_window_sql_to_logical_plan_with_policy(
        sql,
        &catalog,
        &output_schema,
        Some(LateRowPolicy::AdmitWithinAllowance {
            allowance_ns: 10_000_000_000,
        }),
    )
    .unwrap();
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    // Watermark 60s, allowance 10s -> finalization frontier 50s. bob@55s is
    // late relative to the watermark but still within allowance, so it is
    // admitted; the [0,60) window ends at 60 > 50, so it must NOT be
    // published yet (deferred finalization).
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 10_000_000_000, 1),
                    ("bob", 55, 55_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();
    assert_window_page(runtime.as_ref(), 1, &[]);

    // Watermark advances to 70s -> frontier 60s; the [0,60) window is now
    // final and publishes exactly once with both admitted rows.
    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 70_000_000_000,
                    watermark_ns: 70_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[(
                    "alice",
                    7,
                    70_000_000_000,
                    1,
                )])],
            }],
        )
        .unwrap();
    assert_window_page(
        runtime.as_ref(),
        2,
        &[
            ("alice", 0, 60_000_000_000, 10, 1),
            ("bob", 0, 60_000_000_000, 55, 1),
        ],
    );
}

#[test]
fn multi_input_watermark_combination_is_min_across_partitions_and_rejects_regression() {
    use velorix_core::standing_program::InputEventTimeFrontier;

    let frontiers = vec![
        InputEventTimeFrontier {
            relation_id: "a".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: "fp".to_string(),
            stream_id: "s".to_string(),
            partition_id: 0,
            event_time_column_id: "event_time".to_string(),
            max_observed_event_time_ns: 100,
            watermark_ns: 90,
        },
        InputEventTimeFrontier {
            relation_id: "a".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: "fp".to_string(),
            stream_id: "s".to_string(),
            partition_id: 1,
            event_time_column_id: "event_time".to_string(),
            max_observed_event_time_ns: 60,
            watermark_ns: 50,
        },
        InputEventTimeFrontier {
            relation_id: "b".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: "fp".to_string(),
            stream_id: "t".to_string(),
            partition_id: 0,
            event_time_column_id: "event_time".to_string(),
            max_observed_event_time_ns: 200,
            watermark_ns: 180,
        },
    ];
    // Effective operator watermark = min over all active partitions of all
    // inputs; no input can finalize windows while another is behind.
    assert_eq!(combine_multi_input_watermarks(&frontiers), Some(50));
    assert_eq!(combine_multi_input_watermarks(&[]), None);

    // A regressing watermark must fail the epoch closed.
    let mut state = frontiers.clone();
    let regression = [RelationInputBatch {
        encoding: RelationInputEncodingV1::SourceRelationV1,
        relation_id: "a".to_string(),
        relation_version: "v1".to_string(),
        stream_id: "s".to_string(),
        partition_id: 1,
        schema_fingerprint: "fp".to_string(),
        start_offset_inclusive: 0,
        end_offset_exclusive: 1,
        event_time_watermark: Some(InputEventTimeWatermark {
            stream_id: "s".to_string(),
            partition_id: 1,
            event_time_column_id: "event_time".to_string(),
            max_observed_event_time_ns: 55,
            watermark_ns: 45,
        }),
        batches: vec![],
    }];
    let error = advance_input_event_time_frontier(&mut state, &regression[0]).unwrap_err();
    assert!(
        error.to_string().contains("input_event_time_watermark"),
        "watermark regression must fail closed: {error}"
    );
}

#[test]
fn late_row_policy_default_strict_reject_fails_closed_on_late_row() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    // No policy on the plan: the legacy strict contract must be preserved.
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &standing_identity_with_view(sql, "purchases_by_user_minute"),
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 1,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[(
                    "alice",
                    10,
                    10_000_000_000,
                    1,
                )])],
            }],
        )
        .unwrap();

    let error = runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 1,
                end_offset_exclusive: 2,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[(
                    "carol",
                    99,
                    20_000_000_000,
                    1,
                )])],
            }],
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("tumbling_event_time_input_batch"),
        "strict default must fail closed on late rows: {error}"
    );
    assert_window_page(runtime.as_ref(), 1, &[("alice", 0, 60_000_000_000, 10, 1)]);
}

#[test]
fn session_window_retraction_splits_merged_session_exactly_and_survives_restart() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id, session(interval '10 seconds')";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    let watermark = |w: i64| InputEventTimeWatermark {
        stream_id: "purchases-stream".to_string(),
        partition_id: 0,
        event_time_column_id: "event_time".to_string(),
        max_observed_event_time_ns: w,
        watermark_ns: w,
    };
    // Epoch 1: 30s, 40s, 50s with a 10s gap merge into one session [30,50];
    // watermark 20s keeps every row on-time and nothing finalizes yet
    // (50 + 10 > 20).
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: Some(watermark(20_000_000_000)),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 30_000_000_000, 1),
                    ("alice", 5, 40_000_000_000, 1),
                    ("alice", 9, 50_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();
    assert_window_page(runtime.as_ref(), 1, &[]);

    // Epoch 2: retracting the bridge event (40s) must split the merged
    // session into [30] and [50] while nothing is published yet.
    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: Some(watermark(20_000_000_000)),
                batches: vec![purchases_event_time_batch(&[(
                    "alice",
                    5,
                    40_000_000_000,
                    -1,
                )])],
            }],
        )
        .unwrap();
    assert_window_page(runtime.as_ref(), 2, &[]);

    // Epoch 3: watermark 60s finalizes both split sessions exactly once.
    runtime
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: Some(watermark(60_000_000_000)),
                batches: vec![],
            }],
        )
        .unwrap();
    assert_window_page(
        runtime.as_ref(),
        3,
        &[
            ("alice", 30_000_000_000, 30_000_000_000, 10, 1),
            ("alice", 50_000_000_000, 50_000_000_000, 9, 1),
        ],
    );

    // Restart equivalence: the same suffix after a checkpoint yields the
    // identical split output.
    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: Some(watermark(60_000_000_000)),
                batches: vec![],
            }],
        )
        .unwrap();
    assert_window_page(
        restored.as_ref(),
        3,
        &[
            ("alice", 30_000_000_000, 30_000_000_000, 10, 1),
            ("alice", 50_000_000_000, 50_000_000_000, 9, 1),
        ],
    );
}

#[test]
fn tumbling_window_retraction_before_closure_is_exact_and_after_closure_fails_closed() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    // Epoch 1: rows at 30s and 35s, watermark 20s (all on-time), the
    // [0,60) window is not closed yet.
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 20_000_000_000,
                    watermark_ns: 20_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 30_000_000_000, 1),
                    ("alice", 7, 35_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();
    assert_window_page(runtime.as_ref(), 1, &[]);

    // Epoch 2: retract the 30s row before closure; the window keeps 35s only.
    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 3,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 20_000_000_000,
                    watermark_ns: 20_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[(
                    "alice",
                    10,
                    30_000_000_000,
                    -1,
                )])],
            }],
        )
        .unwrap();
    assert_window_page(runtime.as_ref(), 2, &[]);

    // Epoch 3: the window closes exactly once with the surviving row.
    runtime
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![],
            }],
        )
        .unwrap();
    assert_window_page(runtime.as_ref(), 3, &[("alice", 0, 60_000_000_000, 7, 1)]);

    // A retraction of a finalized-window row is late and the strict default
    // fails the epoch closed.
    let error = runtime
        .apply_changes(
            4,
            EpochIdempotencyKey::new("epoch-4").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 4,
                end_offset_exclusive: 5,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 60_000_000_000,
                    watermark_ns: 60_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[(
                    "alice",
                    -7,
                    35_000_000_000,
                    -1,
                )])],
            }],
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("tumbling_event_time_input_batch"),
        "post-finalization retraction must fail closed under strict policy: {error}"
    );
}

#[test]
fn hopping_window_retraction_updates_all_fanout_windows_exactly() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = purchases_window_output_schema();
    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from hop(purchases, event_time, interval '5 seconds', interval '10 seconds') group by user_id, window_start, window_end";
    let identity = standing_identity_with_view(sql, "purchases_by_user_minute");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    // Epoch 1: 10s and 12s rows (watermark 10s keeps both on-time); each
    // lands in the [5,15) and [10,20) windows; nothing is closed yet
    // (window ends 15/20 > frontier 10).
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 10_000_000_000,
                    watermark_ns: 10_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 5, 10_000_000_000, 1),
                    ("alice", 3, 12_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();
    assert_window_page(runtime.as_ref(), 1, &[]);

    // Epoch 2: retract the 10s row; both fanout windows must drop it.
    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 3,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 10_000_000_000,
                    watermark_ns: 10_000_000_000,
                }),
                batches: vec![purchases_event_time_batch(&[(
                    "alice",
                    5,
                    10_000_000_000,
                    -1,
                )])],
            }],
        )
        .unwrap();

    // Epoch 3: watermark 20s finalizes [5,15) and [10,20) with only the
    // 12s row surviving in both.
    runtime
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: Some(InputEventTimeWatermark {
                    stream_id: "purchases-stream".to_string(),
                    partition_id: 0,
                    event_time_column_id: "event_time".to_string(),
                    max_observed_event_time_ns: 20_000_000_000,
                    watermark_ns: 20_000_000_000,
                }),
                batches: vec![],
            }],
        )
        .unwrap();
    assert_window_page(
        runtime.as_ref(),
        3,
        &[
            ("alice", 10_000_000_000, 20_000_000_000, 3, 1),
            ("alice", 5_000_000_000, 15_000_000_000, 3, 1),
        ],
    );
}

#[test]
fn runtime_materializes_string_temporal_float_typed_projections_and_restores() {
    let catalog = purchases_event_time_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = typed_expressions_output_schema();
    let sql = "select user_id, upper(user_id) as user_upper, concat(user_id, '-x') as user_tag, extract(year from event_time) as event_year, date_trunc('hour', event_time) as hour_trunc, event_time + interval '1 hour' as plus_hour, amount * 1.0 as amount_f64, abs(amount * 1.0) as amount_abs, round(amount * 1.5) as amount_round from purchases";
    let identity = standing_identity_with_view(sql, "typed_projection_view");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                event_time_watermark: None,
                batches: vec![purchases_event_time_batch(&[
                    ("alice", 10, 1_735_689_600_000_000_000, 1),
                    ("bob", 4, 1_735_689_600_000_000_000, 1),
                ])],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "typed_projection_view".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_upper = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let user_tag = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(user_upper.value(0), "ALICE");
    assert_eq!(user_tag.value(0), "alice-x");

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "test-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![purchases_event_time_batch(&[(
                    "carol",
                    7,
                    1_735_689_600_000_000_000,
                    1,
                )])],
            }],
        )
        .unwrap();
    let page = restored
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "typed_projection_view".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(2),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_upper = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let user_tag = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let mut values = (0..batch.num_rows())
        .map(|index| {
            (
                user_upper.value(index).to_string(),
                user_tag.value(index).to_string(),
            )
        })
        .collect::<Vec<_>>();
    values.sort();
    assert_eq!(
        values,
        vec![
            ("ALICE".to_string(), "alice-x".to_string()),
            ("BOB".to_string(), "bob-x".to_string()),
            ("CAROL".to_string(), "carol-x".to_string()),
        ]
    );
}

#[test]
fn public_exists_on_non_primary_key_materializes_and_restores() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let catalogs = vec![scores.clone(), accounts.clone()];
    let input_schemas = catalogs
        .iter()
        .map(|catalog| catalog_input_relation_schema(catalog).unwrap())
        .collect::<Vec<_>>();
    let output = scores_projection_output_schema();
    // Phase 7.4: correlation on identical non-null scalar columns (score =
    // limit), not only the primary key.
    let sql = "select s.user_id, s.score from scores s where exists (select 1 from accounts a where a.limit = s.score)";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &catalogs,
        sql,
        &input_schemas,
        std::slice::from_ref(&output),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "scores-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 20, 1),
                        ("carol", 30, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "accounts-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_rows_batch(&[
                        ("a1", 10, "gold", 1),
                        ("a2", 40, "silver", 1),
                    ])],
                },
            ],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "positive_scores".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap_or_else(|error| panic!("[PAGE-ERR] {error}"));
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut users = (0..batch.num_rows())
        .map(|index| user_ids.value(index).to_string())
        .collect::<Vec<_>>();
    users.sort();
    assert_eq!(users, vec!["alice".to_string()]);

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: accounts.relation_schema.relation_id.clone(),
                relation_version: accounts.relation_schema.relation_version.clone(),
                stream_id: "accounts-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: accounts.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![accounts_rows_batch(&[("a3", 20, "gold", 1)])],
            }],
        )
        .unwrap();
    let page = restored
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "positive_scores".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(2),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut users = (0..batch.num_rows())
        .map(|index| user_ids.value(index).to_string())
        .collect::<Vec<_>>();
    users.sort();
    assert_eq!(users, vec!["alice".to_string(), "bob".to_string()]);
}

#[test]
fn runtime_materializes_decimal_group_key_aggregate_and_restores() {
    let catalog = decimal_key_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = decimal_key_output_schema();
    let sql = "select k, sum(amount) as total from decimal_events group by k";
    let identity = standing_identity_with_view(sql, "decimal_events_by_key");

    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        sql,
        &[input_schema],
        std::slice::from_ref(&output_schema),
    )
    .unwrap_or_else(|error| panic!("[DEC-ERR] {error}"));

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "decimal-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![decimal_events_batch(&[
                    ("1.25", 10, 1),
                    ("2.50", 7, 1),
                    ("1.25", -3, 1),
                ])],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "decimal_events_by_key".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let totals = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut rows = (0..batch.num_rows())
        .map(|index| (keys.value(index), totals.value(index)))
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(rows, vec![(125_i128, 7_i64), (250_i128, 7_i64)]);

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                stream_id: "decimal-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![decimal_events_batch(&[("1.25", 5, 1)])],
            }],
        )
        .unwrap();
    let page = restored
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "decimal_events_by_key".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(2),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let totals = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut rows = (0..batch.num_rows())
        .map(|index| (keys.value(index), totals.value(index)))
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(rows, vec![(125_i128, 12_i64), (250_i128, 7_i64)]);
}

fn decimal_key_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = purchases_catalog_without_value_role();
    catalog.relation_schema.relation_id = "decimal_events".to_string();
    catalog.relation_schema.relation_name = "decimal_events".to_string();
    let key = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "user_id")
        .unwrap();
    key.name = "k".to_string();
    key.column_id = "k".to_string();
    key.logical_type = VelorixLogicalTypeV1::Decimal {
        precision: 10,
        scale: 2,
    };
    key.physical_arrow_type = ArrowPhysicalTypeV1::Decimal128 {
        precision: 10,
        scale: 2,
    };
    catalog.relation_schema.primary_key_column_ids = vec!["k".to_string()];
    catalog.relation_schema.event_time_column_id = None;
    catalog.incremental_relation.relation_id = "decimal_events".to_string();

    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("decimal key schema should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
}

fn decimal_key_output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "decimal_events_by_key".to_string(),
        relation_name: "decimal_events_by_key".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "decimal-key-v1".to_string(),
        columns: vec![
            ColumnSchema {
                name: "k".to_string(),
                data_type: SqlDataType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                nullable: false,
            },
            ColumnSchema {
                name: "total".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["k".to_string()],
    }
}

fn decimal_events_batch(rows: &[(&str, i64, i64)]) -> RecordBatch {
    let keys = Decimal128Array::from_iter_values(
        rows.iter()
            .map(|(key, _, _)| key.replace('.', "").parse::<i128>().unwrap()),
    )
    .with_precision_and_scale(10, 2)
    .unwrap();
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Decimal128(10, 2), false),
            arrow::datatypes::Field::new("amount", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("delta", arrow::datatypes::DataType::Int64, false),
        ])),
        vec![
            Arc::new(keys) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, v, _)| *v).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, _, w)| *w).collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap()
}

#[test]
fn scalar_aggregate_filter_materializes_and_restores() {
    let scores = scores_catalog();
    let accounts = accounts_catalog();
    let catalogs = vec![scores.clone(), accounts.clone()];
    let input_schemas = catalogs
        .iter()
        .map(|catalog| catalog_input_relation_schema(catalog).unwrap())
        .collect::<Vec<_>>();
    let output = scores_projection_output_schema();
    let sql = "select s.user_id, s.score from scores s where s.score > (select avg(a.limit) from accounts a)";
    let identity = standing_identity_with_view(sql, "positive_scores");
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity,
        &catalogs,
        sql,
        &input_schemas,
        std::slice::from_ref(&output),
    )
    .unwrap();

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    stream_id: "scores-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_rows_batch(&[
                        ("alice", 10, 1),
                        ("bob", 20, 1),
                        ("carol", 30, 1),
                    ])],
                },
                RelationInputBatch {
                    encoding: RelationInputEncodingV1::SourceRelationV1,
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
                    stream_id: "accounts-stream".to_string(),
                    partition_id: 0,
                    schema_fingerprint: accounts.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                    event_time_watermark: None,
                    batches: vec![accounts_rows_batch(&[
                        ("a1", 10, "gold", 1),
                        ("a2", 30, "silver", 1),
                    ])],
                },
            ],
        )
        .unwrap();

    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "positive_scores".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut users = (0..batch.num_rows())
        .map(|index| user_ids.value(index).to_string())
        .collect::<Vec<_>>();
    users.sort();
    // avg(limit) = 20; score > 20 -> carol only.
    assert_eq!(users, vec!["carol".to_string()]);

    // Scalar changes to 40: bob and carol now pass.
    runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: accounts.relation_schema.relation_id.clone(),
                relation_version: accounts.relation_schema.relation_version.clone(),
                stream_id: "accounts-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: accounts.schema_fingerprint.to_string(),
                start_offset_inclusive: 2,
                end_offset_exclusive: 3,
                event_time_watermark: None,
                batches: vec![accounts_rows_batch(&[("a3", 80, "gold", 1)])],
            }],
        )
        .unwrap();
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "positive_scores".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(2),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut users = (0..batch.num_rows())
        .map(|index| user_ids.value(index).to_string())
        .collect::<Vec<_>>();
    users.sort();
    // avg now 40; no score exceeds 40.
    assert!(users.is_empty(), "no score exceeds avg 40: {users:?}");

    // Restart: checkpoint then restore, verify state and continued updates.
    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![RelationInputBatch {
                encoding: RelationInputEncodingV1::SourceRelationV1,
                relation_id: scores.relation_schema.relation_id.clone(),
                relation_version: scores.relation_schema.relation_version.clone(),
                stream_id: "scores-stream".to_string(),
                partition_id: 0,
                schema_fingerprint: scores.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![scores_rows_batch(&[("dave", 50, 1)])],
            }],
        )
        .unwrap();
    let page = restored
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".to_string(),
                program_id: "program-purchases".to_string(),
                view_id: "positive_scores".to_string(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(3),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut users = (0..batch.num_rows())
        .map(|index| user_ids.value(index).to_string())
        .collect::<Vec<_>>();
    users.sort();
    assert_eq!(users, vec!["dave".to_string()]);
}
