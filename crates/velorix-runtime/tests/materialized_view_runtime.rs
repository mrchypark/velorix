use std::sync::Arc;

use arrow::{
    array::{BooleanArray, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use serde_json::{json, Value};
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
    standing_program::{
        EpochIdempotencyKey, InputEventTimeWatermark, NativeCodePolicy, RelationInputBatch,
        RuntimePackageIdentity, ScopedViewId, SnapshotPageRequest, StandingProgramIdentity,
        StandingProgramRuntimeError,
    },
    view_contract::{
        catalog_input_relation_schema, stable_bytes_hash, ColumnSchema, RelationSchema, SqlDataType,
    },
    view_plan::{LogicalPlanAggregateFunctionV1, SupportedAggregateOutput},
};
use velorix_runtime::materialized_view_runtime::{
    create_standing_runtime, create_standing_runtime_with_sql_and_catalogs,
    materialized_delta_to_page, restore_standing_runtime, CRATE_NAME,
};

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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
    let sql = "select user_id, sum(amount) as sum, count(*) as count from purchases where amount > 5 group by user_id";
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
fn runtime_accepts_sparse_forward_offsets_and_rejects_overlapping_offsets() {
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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

    let sparse_commit = runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 5,
                end_offset_exclusive: 6,
                event_time_watermark: None,
                batches: vec![purchases_batch()],
            }],
        )
        .unwrap();

    assert_eq!(sparse_commit.input_frontiers.len(), 1);
    assert_eq!(
        sparse_commit.input_frontiers[0].committed_offset_exclusive,
        6
    );

    let err = runtime
        .apply_changes(
            3,
            EpochIdempotencyKey::new("epoch-3").unwrap(),
            vec![RelationInputBatch {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.to_string(),
                start_offset_inclusive: 5,
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
    assert_eq!(frontier.committed_offset_exclusive, 6);

    assert!(matches!(
        err,
        StandingProgramRuntimeError::InvalidProgramIdentity {
            field: "input_frontier.offset_range"
        }
    ));
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
            output_column_id: "total".to_string(),
        },
        SupportedAggregateOutput {
            function: LogicalPlanAggregateFunctionV1::Count,
            input_column_id: None,
            output_column_id: "events".to_string(),
        },
        SupportedAggregateOutput {
            function: LogicalPlanAggregateFunctionV1::Avg,
            input_column_id: Some("amount".to_string()),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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

    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("epoch-1").unwrap(),
            vec![
                RelationInputBatch {
                    relation_id: scores.relation_schema.relation_id.clone(),
                    relation_version: scores.relation_schema.relation_version.clone(),
                    schema_fingerprint: scores.schema_fingerprint.to_string(),
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 3,
                    event_time_watermark: None,
                    batches: vec![scores_batch()],
                },
                RelationInputBatch {
                    relation_id: accounts.relation_schema.relation_id.clone(),
                    relation_version: accounts.relation_schema.relation_version.clone(),
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

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                relation_id: scores.relation_schema.relation_id.clone(),
                relation_version: scores.relation_schema.relation_version.clone(),
                schema_fingerprint: scores.schema_fingerprint.to_string(),
                start_offset_inclusive: 3,
                end_offset_exclusive: 4,
                event_time_watermark: None,
                batches: vec![score_append_batch("alice", 3)],
            }],
        )
        .unwrap();

    assert_join_page(restored.as_ref(), 2, &[("alice", 20, 3), ("bob", 5, 1)]);
}

#[test]
fn runtime_materializes_latest_bool_by_key_and_restores_state() {
    let catalog = device_status_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = latest_device_status_output_schema();
    let sql =
        "select device_id, arg_max(enabled, event_time) as enabled from device_status group by device_id";
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
        &[("device-a", false), ("device-b", true)],
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
        &[("device-a", false), ("device-b", false)],
    );
}

#[test]
fn runtime_materializes_tumbling_event_time_windows_and_restores_state() {
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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

    assert_window_page(
        runtime.as_ref(),
        1,
        &[
            ("alice", 0, 60_000_000_000, 10, 1),
            ("bob", 0, 60_000_000_000, 5, 1),
        ],
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let mut restored = restore_standing_runtime(checkpoint).unwrap();
    restored
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![RelationInputBatch {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
        &[
            ("alice", 0, 60_000_000_000, 10, 1),
            ("alice", 60_000_000_000, 120_000_000_000, 7, 1),
            ("bob", 0, 60_000_000_000, 5, 1),
            ("bob", 60_000_000_000, 120_000_000_000, 11, 1),
        ],
    );
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
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
        compiler_identity: "velorix-materialized-runtime@1".to_string(),
        runtime_packages: vec![RuntimePackageIdentity {
            name: CRATE_NAME.to_string(),
            version: "0.1.0".to_string(),
        }],
        package_feature_set: vec!["materialized-view-runtime-v1".to_string()],
        runtime_compatibility: "velorix-materialized-view-runtime-v1".to_string(),
        checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
    }
}

fn assert_join_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64, i64)],
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

fn assert_window_stats_page(
    runtime: &(dyn velorix_core::standing_program::StandingProgramRuntime + Send),
    epoch: u64,
    expected: &[(&str, i64, i64, i64, i64, i64, i64, f64)],
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

fn accounts_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("limit", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])) as _,
            Arc::new(Int64Array::from(vec![100, 50])) as _,
            Arc::new(Int64Array::from(vec![1, 1])) as _,
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
