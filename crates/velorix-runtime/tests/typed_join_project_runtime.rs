use std::sync::Arc;

use arrow::{
    array::{Array, Float64Array, Int64Array, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use serde_json::{json, Value};
use velorix_core::{
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1, VelorixRelationSourceV1,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
    standing_program::{
        BuiltinRuntimeIdentity, EpochIdempotencyKey, NativeCodePolicy, RelationInputBatch,
        RelationInputEncodingV1, ScopedViewId, SnapshotPageRequest, StandingProgramIdentity,
        StandingProgramRuntime,
    },
    view_contract::{
        catalog_input_relation_schema, stable_bytes_hash, ColumnSchema, RelationSchema, SqlDataType,
    },
    view_plan::{
        lower_supported_typed_join_project_sql_to_logical_plan,
        validate_supported_typed_join_project_sql,
    },
};
use velorix_runtime::materialized_view_runtime::{
    create_standing_runtime_with_logical_plan_and_catalogs, restore_standing_runtime, CRATE_NAME,
};

type Row<'a> = (&'a str, i64, Option<f64>, Option<f64>, i64);

#[test]
fn bag_publication_validates_checkpoint_and_checkpoint_budget_is_atomic() {
    use velorix_runtime::materialized_view_runtime::{
        bag_checkpoint_published_output_with_limits, materialized_bag_canonical_batch,
        restore_standing_runtime_with_limits, RuntimeResourceLimits,
    };
    let (mut runtime, catalogs) = runtime("INNER");
    let empty = runtime.checkpoint().unwrap();
    let limits = RuntimeResourceLimits {
        max_checkpoint_bytes: empty.state_payload.as_ref().unwrap().payload.len(),
        ..RuntimeResourceLimits::default()
    };
    let mut bounded = restore_standing_runtime_with_limits(empty.clone(), limits).unwrap();
    let inputs = vec![
        input(
            &catalogs[0],
            false,
            0,
            &[
                ("same", 10, Some(2.0), None, 2),
                ("same", 10, Some(3.0), None, 1),
            ],
        ),
        input(&catalogs[1], true, 0, &[("same", 10, Some(4.0), None, 1)]),
    ];
    assert!(bounded.apply_changes(1, key(1), inputs.clone()).is_err());
    assert_eq!(bounded.checkpoint().unwrap(), empty);
    runtime.apply_changes(1, key(1), inputs).unwrap();
    let checkpoint = runtime.checkpoint().unwrap();
    assert!(restore_standing_runtime_with_limits(checkpoint.clone(), limits).is_err());
    let bag =
        bag_checkpoint_published_output_with_limits(&checkpoint, RuntimeResourceLimits::default())
            .unwrap()
            .unwrap();
    assert_eq!(bag.records().len(), 2);
    assert_eq!(bag.records().iter().map(|row| row.weight).sum::<i64>(), 3);
    assert_eq!(
        materialized_bag_canonical_batch(&output_schema(), &bag)
            .unwrap()
            .num_rows(),
        3
    );
    let mut corrupt = checkpoint;
    let payload = &mut corrupt.state_payload.as_mut().unwrap().payload;
    let mut state: Value = velorix_core::arrow_state::from_state(payload).unwrap();
    state["output"] =
        serde_json::to_value(velorix_core::delta::DeltaBatch::from_records([])).unwrap();
    *payload = velorix_core::arrow_state::to_state(&state).unwrap();
    corrupt.state_root.content_hash = stable_bytes_hash(payload.as_bytes());
    assert!(bag_checkpoint_published_output_with_limits(
        &corrupt,
        RuntimeResourceLimits::default()
    )
    .is_err());
}

#[test]
fn checkpoint_budget_exact_boundary_survives_signed_updates_and_restore() {
    use velorix_runtime::materialized_view_runtime::{
        restore_standing_runtime_with_limits, RuntimeResourceLimits,
    };
    let (runtime, catalogs) = runtime("INNER");
    let empty = runtime.checkpoint().unwrap();
    let inputs = vec![
        input(
            &catalogs[0],
            false,
            0,
            &[
                ("same", 10, Some(2.0), None, 2),
                ("same", 10, Some(3.0), None, 1),
            ],
        ),
        input(&catalogs[1], true, 0, &[("same", 10, Some(4.0), None, 1)]),
    ];
    let accepts = |max_checkpoint_bytes| {
        let limits = RuntimeResourceLimits {
            max_checkpoint_bytes,
            ..RuntimeResourceLimits::default()
        };
        let Ok(mut candidate) = restore_standing_runtime_with_limits(empty.clone(), limits) else {
            return false;
        };
        candidate.apply_changes(1, key(1), inputs.clone()).is_ok()
    };
    let mut low = empty.state_payload.as_ref().unwrap().payload.len();
    let mut high = RuntimeResourceLimits::default().max_checkpoint_bytes;
    assert!(accepts(high));
    while low < high {
        let middle = low + (high - low) / 2;
        if accepts(middle) {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    assert!(accepts(low));
    assert!(low > 0 && !accepts(low - 1));

    let exact_limits = RuntimeResourceLimits {
        max_checkpoint_bytes: low,
        ..RuntimeResourceLimits::default()
    };
    let mut bounded = restore_standing_runtime_with_limits(empty.clone(), exact_limits).unwrap();
    assert!(bounded.apply_changes(1, key(1), inputs.clone()).is_ok());
    let committed = bounded.checkpoint().unwrap();
    let restore_limits = RuntimeResourceLimits {
        max_checkpoint_bytes: low + 1024,
        ..RuntimeResourceLimits::default()
    };
    let mut restored =
        restore_standing_runtime_with_limits(committed.clone(), restore_limits).unwrap();
    let retract = input(&catalogs[0], false, 2, &[("same", 10, Some(2.0), None, -2)]);
    restored.apply_changes(2, key(2), vec![retract]).unwrap();
    let after_retract = restored.checkpoint().unwrap();
    assert_eq!(
        restore_standing_runtime_with_limits(after_retract.clone(), restore_limits)
            .unwrap()
            .checkpoint()
            .unwrap(),
        after_retract
    );

    let mut rejected = restore_standing_runtime_with_limits(
        empty.clone(),
        RuntimeResourceLimits {
            max_checkpoint_bytes: low - 1,
            ..RuntimeResourceLimits::default()
        },
    )
    .unwrap();
    assert!(rejected
        .apply_changes(1, key(1), inputs)
        .unwrap_err()
        .to_string()
        .contains("runtime_checkpoint_bytes"));
    assert_eq!(rejected.checkpoint().unwrap(), empty);
}

fn catalog(id: &str, right: bool) -> VelorixRelationCatalogV1 {
    let names = if right {
        ["key_r", "bucket_r", "metric", "aux", "delta"]
    } else {
        ["item", "bucket", "metric", "aux", "delta"]
    };
    let types = [
        (VelorixLogicalTypeV1::Utf8, ArrowPhysicalTypeV1::Utf8),
        (
            VelorixLogicalTypeV1::Timestamp { timezone: None },
            ArrowPhysicalTypeV1::TimestampNanosecond { timezone: None },
        ),
        (VelorixLogicalTypeV1::Float64, ArrowPhysicalTypeV1::Float64),
        (VelorixLogicalTypeV1::Float64, ArrowPhysicalTypeV1::Float64),
        (VelorixLogicalTypeV1::Int64, ArrowPhysicalTypeV1::Int64),
    ];
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: id.into(),
        relation_name: id.into(),
        relation_version: "v1".into(),
        columns: names
            .iter()
            .zip(types)
            .enumerate()
            .map(
                |(ordinal, (name, (logical_type, physical_arrow_type)))| RelationColumnV1 {
                    column_id: (*name).into(),
                    name: (*name).into(),
                    logical_type,
                    physical_arrow_type,
                    nullable: ordinal == 2 || ordinal == 3,
                    ordinal: ordinal as u32,
                    semantic_role: if ordinal == 0 {
                        RelationSemanticRoleV1::PrimaryKey
                    } else if ordinal == 4 {
                        RelationSemanticRoleV1::Weight
                    } else {
                        RelationSemanticRoleV1::Value
                    },
                },
            )
            .collect(),
        primary_key_column_ids: vec![names[0].into()],
        weight_column_id: "delta".into(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: fingerprint.clone(),
        relation_source: VelorixRelationSourceV1::SourceRelation,
        datafusion_registration: DataFusionRegistrationV1 {
            name: id.into(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: id.into(),
            schema_fingerprint: fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
        },
    }
}

fn batch(right: bool, rows: &[Row<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(if right { "key_r" } else { "item" }, DataType::Utf8, false),
            Field::new(
                if right { "bucket_r" } else { "bucket" },
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("metric", DataType::Float64, true),
            Field::new("aux", DataType::Float64, true),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.4).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn input(
    catalog: &VelorixRelationCatalogV1,
    right: bool,
    offset: u64,
    rows: &[Row<'_>],
) -> RelationInputBatch {
    RelationInputBatch {
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_version: "v1".into(),
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        stream_id: "rows".into(),
        partition_id: 0,
        start_offset_inclusive: offset,
        end_offset_exclusive: offset + rows.len() as u64,
        event_time_watermark: None,
        encoding: RelationInputEncodingV1::SourceRelationV1,
        batches: vec![batch(right, rows)],
    }
}

fn sql(kind: &str) -> String {
    let keys = if kind == "FULL OUTER" {
        "COALESCE(s.item, e.key_r) AS item, COALESCE(s.bucket, e.bucket_r) AS bucket"
    } else {
        "s.item, s.bucket"
    };
    format!("SELECT {keys}, s.metric AS mean_metric, s.aux AS mean_aux, e.metric AS measurement FROM samples s {kind} JOIN measurement e ON s.item = e.key_r AND s.bucket = e.bucket_r")
}

fn output_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "item_fact".into(),
        relation_name: "item_fact".into(),
        relation_version: "v1".into(),
        schema_fingerprint: format!("sha256:{}", "3".repeat(64)),
        primary_key: vec![],
        columns: vec![
            ColumnSchema {
                name: "item".into(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "bucket".into(),
                data_type: SqlDataType::Timestamp { timezone: None },
                nullable: false,
            },
            ColumnSchema {
                name: "mean_metric".into(),
                data_type: SqlDataType::Float64,
                nullable: true,
            },
            ColumnSchema {
                name: "mean_aux".into(),
                data_type: SqlDataType::Float64,
                nullable: true,
            },
            ColumnSchema {
                name: "measurement".into(),
                data_type: SqlDataType::Float64,
                nullable: true,
            },
        ],
    }
}

fn runtime(
    kind: &str,
) -> (
    Box<dyn StandingProgramRuntime + Send>,
    Vec<VelorixRelationCatalogV1>,
) {
    runtime_with_nullable_keys(kind, false)
}

fn runtime_with_nullable_keys(
    kind: &str,
    nullable: bool,
) -> (
    Box<dyn StandingProgramRuntime + Send>,
    Vec<VelorixRelationCatalogV1>,
) {
    let mut catalogs = vec![catalog("samples", false), catalog("measurement", true)];
    if nullable {
        for catalog in &mut catalogs {
            catalog.relation_schema.primary_key_column_ids.clear();
            for column in catalog.relation_schema.columns.iter_mut().take(2) {
                column.nullable = true;
                column.semantic_role = RelationSemanticRoleV1::Value;
            }
            let fingerprint =
                SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
            catalog.schema_fingerprint = fingerprint.clone();
            catalog.incremental_relation.schema_fingerprint = fingerprint;
        }
    }
    let sql = if kind == "FULL DIRECT" {
        sql("LEFT").replace(" LEFT JOIN ", " FULL OUTER JOIN ")
    } else {
        sql(kind)
    };
    let mut output = output_schema();
    if nullable {
        for column in output.columns.iter_mut().take(2) {
            column.nullable = true;
        }
    }
    runtime_for_sql(sql, output, catalogs)
}

fn runtime_for_sql(
    sql: String,
    output: RelationSchema,
    catalogs: Vec<VelorixRelationCatalogV1>,
) -> (
    Box<dyn StandingProgramRuntime + Send>,
    Vec<VelorixRelationCatalogV1>,
) {
    let logical =
        lower_supported_typed_join_project_sql_to_logical_plan(&sql, &catalogs, &output).unwrap();
    let identity = StandingProgramIdentity {
        tenant_id: "tenant".into(),
        program_id: "join".into(),
        view_ids: vec!["item_fact".into()],
        sql_hash: stable_bytes_hash(sql.as_bytes()),
        input_catalog_hash: format!("sha256:{}", "1".repeat(64)),
        output_schema_hash: format!("sha256:{}", "2".repeat(64)),
        planner_identity: "velorix-logical-view-planner@1".into(),
        builtin_runtime_identities: vec![BuiltinRuntimeIdentity {
            name: CRATE_NAME.into(),
            version: "0.1.0".into(),
        }],
        runtime_capabilities: vec!["materialized-view-runtime-v1".into()],
        runtime_compatibility: "velorix-materialized-view-runtime-v1".into(),
        checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".into(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
    };
    let schemas = catalogs
        .iter()
        .map(|catalog| catalog_input_relation_schema(catalog).unwrap())
        .collect::<Vec<_>>();
    let runtime = create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        &catalogs,
        logical,
        &schemas,
        &[output],
    )
    .unwrap();
    (runtime, catalogs)
}

fn key(epoch: u64) -> EpochIdempotencyKey {
    EpochIdempotencyKey::new(format!("epoch-{epoch}")).unwrap()
}

fn page(
    runtime: &dyn StandingProgramRuntime,
    limit: Option<usize>,
    token: Option<String>,
) -> (Vec<Value>, Option<String>) {
    let page = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant".into(),
                program_id: "join".into(),
                view_id: "item_fact".into(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(runtime.logical_epoch()),
                page_token: token,
                max_rows: limit,
            },
        )
        .unwrap();
    let batch = &page.batches[0];
    let items = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let buckets = batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let floats = (2..5)
        .map(|index| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let rows = (0..batch.num_rows())
        .map(|row| {
            json!([
                items.is_valid(row).then(|| items.value(row)),
                buckets.is_valid(row).then(|| buckets.value(row)),
                floats[0].is_valid(row).then(|| floats[0].value(row)),
                floats[1].is_valid(row).then(|| floats[1].value(row)),
                floats[2].is_valid(row).then(|| floats[2].value(row))
            ])
        })
        .collect();
    (rows, page.next_page_token)
}

#[test]
fn composite_left_join_float_bags_unmatched_transitions_and_restore() {
    let (mut runtime, catalogs) = runtime("LEFT");
    let left = input(
        &catalogs[0],
        false,
        0,
        &[
            ("item-a", 300, Some(10.5), None, 2),
            ("item-a", 600, Some(20.5), Some(2.0), 1),
        ],
    );
    let first = runtime
        .apply_changes_delta_only(1, key(1), vec![left])
        .unwrap();
    assert!(first.output_batches.is_empty());
    assert_eq!(
        first.output_deltas[0]
            .delta
            .records()
            .iter()
            .map(|row| row.weight)
            .sum::<i64>(),
        3
    );
    assert_eq!(
        page(runtime.as_ref(), None, None).0,
        vec![
            json!(["item-a", 300, 10.5, null, null]),
            json!(["item-a", 300, 10.5, null, null]),
            json!(["item-a", 600, 20.5, 2.0, null])
        ]
    );
    let second = runtime
        .apply_changes_delta_only(
            2,
            key(2),
            vec![input(
                &catalogs[1],
                true,
                0,
                &[
                    ("item-a", 300, Some(1.25), None, 3),
                    ("item-a", 900, Some(100.0), None, 1),
                ],
            )],
        )
        .unwrap();
    let changes = second.output_deltas[0].delta.records();
    assert!(changes
        .iter()
        .any(|row| row.key.as_json()["measurement"].is_null() && row.weight == -2));
    assert!(changes
        .iter()
        .any(|row| row.key.as_json()["measurement"] == json!(1.25) && row.weight == 6));
    assert_eq!(page(runtime.as_ref(), None, None).0.len(), 7);
    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    let last = restored
        .apply_changes_delta_only(
            3,
            key(3),
            vec![input(
                &catalogs[1],
                true,
                2,
                &[("item-a", 300, Some(1.25), None, -3)],
            )],
        )
        .unwrap();
    assert!(last.output_deltas[0]
        .delta
        .records()
        .iter()
        .any(|row| row.key.as_json()["measurement"].is_null() && row.weight == 2));
    assert_eq!(page(restored.as_ref(), None, None).0.len(), 3);
    let checkpoint = restored.checkpoint().unwrap();
    let retry = restored
        .apply_changes_delta_only(3, key(3), vec![])
        .unwrap();
    assert!(retry.output_deltas.is_empty());
    assert_eq!(restored.checkpoint().unwrap(), checkpoint);
}

#[test]
fn composite_full_join_coalesces_right_only_item_bucket_and_pages_bag_copies() {
    let (mut runtime, catalogs) = runtime("FULL OUTER");
    runtime
        .apply_changes_delta_only(
            1,
            key(1),
            vec![
                input(
                    &catalogs[0],
                    false,
                    0,
                    &[("item-a", 300, Some(10.0), Some(1.0), 1)],
                ),
                input(
                    &catalogs[1],
                    true,
                    0,
                    &[("item-a", 600, Some(2.5), None, 2)],
                ),
            ],
        )
        .unwrap();
    let expected = vec![
        json!(["item-a", 300, 10.0, 1.0, null]),
        json!(["item-a", 600, null, null, 2.5]),
        json!(["item-a", 600, null, null, 2.5]),
    ];
    assert_eq!(page(runtime.as_ref(), None, None).0, expected);
    let mut paged = Vec::new();
    let mut token = None;
    loop {
        let (rows, next) = page(runtime.as_ref(), Some(1), token);
        paged.extend(rows);
        if next.is_none() {
            break;
        }
        token = next;
    }
    assert_eq!(paged, expected);
    let mut restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    let matched = restored
        .apply_changes_delta_only(
            2,
            key(2),
            vec![input(
                &catalogs[0],
                false,
                1,
                &[("item-a", 600, Some(20.0), None, 3)],
            )],
        )
        .unwrap();
    assert!(matched.output_deltas[0]
        .delta
        .records()
        .iter()
        .any(|row| row.key.as_json()["mean_metric"].is_null() && row.weight == -2));
    assert!(matched.output_deltas[0]
        .delta
        .records()
        .iter()
        .any(|row| row.key.as_json()["mean_metric"] == json!(20.0) && row.weight == 6));
    restored
        .apply_changes_delta_only(
            3,
            key(3),
            vec![input(
                &catalogs[0],
                false,
                2,
                &[("item-a", 600, Some(20.0), None, -3)],
            )],
        )
        .unwrap();
    assert_eq!(page(restored.as_ref(), None, None).0, expected);
}

#[test]
fn inner_join_rejects_invalid_retractions_and_nonfinite_payload_atomically() {
    let (mut runtime, catalogs) = runtime("INNER");
    runtime
        .apply_changes(
            1,
            key(1),
            vec![
                input(
                    &catalogs[0],
                    false,
                    0,
                    &[("item-a", 300, Some(1.0), None, 1)],
                ),
                input(
                    &catalogs[1],
                    true,
                    0,
                    &[("item-a", 300, Some(2.0), None, 1)],
                ),
            ],
        )
        .unwrap();
    let checkpoint = runtime.checkpoint().unwrap();
    for rows in [
        vec![("item-a", 300, Some(2.0), None, -2)],
        vec![("item-a", 300, Some(f64::NAN), None, 1)],
    ] {
        assert!(runtime
            .apply_changes_delta_only(2, key(2), vec![input(&catalogs[1], true, 1, &rows)])
            .is_err());
        assert_eq!(runtime.checkpoint().unwrap(), checkpoint);
    }
    runtime
        .apply_changes_delta_only(
            2,
            key(2),
            vec![input(
                &catalogs[1],
                true,
                1,
                &[("item-a", 300, Some(2.0), None, -1)],
            )],
        )
        .unwrap();
    assert!(page(runtime.as_ref(), None, None).0.is_empty());
}

#[test]
fn checkpoint_rejects_corrupted_hash_codec_schema_and_torn_bag() {
    let (mut runtime, catalogs) = runtime("FULL OUTER");
    runtime
        .apply_changes_delta_only(
            1,
            key(1),
            vec![input(
                &catalogs[1],
                true,
                0,
                &[("item-a", 300, Some(2.0), None, 2)],
            )],
        )
        .unwrap();
    let checkpoint = runtime.checkpoint().unwrap();
    let mut corrupt = checkpoint.clone();
    corrupt.state_root.content_hash = format!("sha256:{}", "0".repeat(64));
    assert!(restore_standing_runtime(corrupt).is_err());
    for (field, value) in [
        ("schema_version", json!(2)),
        ("key_codec", json!("wrong")),
        ("output", json!({"records":[]})),
    ] {
        let mut corrupt = checkpoint.clone();
        let state = corrupt.state_payload.as_mut().unwrap();
        let mut payload: Value = velorix_core::arrow_state::from_state(&state.payload).unwrap();
        payload[field] = value;
        state.payload = velorix_core::arrow_state::to_state(&payload).unwrap();
        corrupt.state_root.content_hash = stable_bytes_hash(state.payload.as_bytes());
        assert!(
            restore_standing_runtime(corrupt).is_err(),
            "accepted {field} corruption"
        );
    }
}

#[test]
fn computed_join_projection_filter_retractions_and_restart_use_join_deltas() {
    use velorix_core::view_plan::VelorixLogicalViewPlanNodeV1;
    let catalogs = vec![catalog("samples", false), catalog("measurement", true)];
    let sql = "SELECT s.item, s.metric + e.metric AS combined FROM samples s LEFT JOIN measurement e ON s.item = e.key_r AND s.bucket = e.bucket_r WHERE e.metric IS NULL OR e.metric > 0.0".to_string();
    let mut output = output_schema();
    output.columns = vec![
        ColumnSchema {
            name: "item".into(),
            data_type: SqlDataType::Utf8,
            nullable: false,
        },
        ColumnSchema {
            name: "combined".into(),
            data_type: SqlDataType::Float64,
            nullable: true,
        },
    ];
    let logical =
        lower_supported_typed_join_project_sql_to_logical_plan(&sql, &catalogs, &output).unwrap();
    assert!(logical.nodes.iter().any(|node| matches!(
        node,
        VelorixLogicalViewPlanNodeV1::Project {
            typed_predicate: Some(_),
            ..
        }
    )));
    let (computed_columns, predicate) = logical
        .nodes
        .iter()
        .find_map(|node| match node {
            VelorixLogicalViewPlanNodeV1::Project {
                computed_columns,
                typed_predicate: Some(predicate),
                ..
            } => Some((computed_columns, predicate)),
            _ => None,
        })
        .unwrap();
    assert!(predicate.bindings.iter().all(|binding| {
        ["samples", "measurement"].contains(&binding.input.relation_id.as_str())
            && binding.internal_column_id.starts_with("__velorix_join_")
            && binding.input.column_id == "metric"
    }));
    assert_eq!(predicate.bindings.len(), 1);
    let typed_computed = computed_columns
        .iter()
        .find_map(|computed| match &computed.expression {
            velorix_core::view_plan::SupportedProjectionExpr::Typed { bindings, .. } => {
                Some(bindings)
            }
            _ => None,
        })
        .unwrap();
    assert!(typed_computed.iter().all(|binding| {
        ["samples", "measurement"].contains(&binding.input.relation_id.as_str())
            && binding.internal_column_id.starts_with("__velorix_join_")
            && binding.input.column_id == "metric"
    }));
    assert_eq!(typed_computed.len(), 2);
    let (mut runtime, _) = runtime_for_sql(sql, output, catalogs.clone());
    let first = runtime
        .apply_changes(
            1,
            key(1),
            vec![
                input(
                    &catalogs[0],
                    false,
                    0,
                    &[("item-a", 300, Some(2.0), None, 2)],
                ),
                input(
                    &catalogs[1],
                    true,
                    0,
                    &[("item-a", 300, Some(3.0), None, 1)],
                ),
            ],
        )
        .unwrap();
    assert_eq!(first.output_deltas[0].delta.records()[0].weight, 2);
    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    runtime = restored;
    let replacement = runtime
        .apply_changes(
            2,
            key(2),
            vec![input(
                &catalogs[1],
                true,
                1,
                &[("item-a", 300, Some(3.0), None, -1)],
            )],
        )
        .unwrap();
    let changes = replacement.output_deltas[0].delta.records();
    assert_eq!(changes.len(), 2);
    assert!(changes
        .iter()
        .any(|row| row.weight == -2 && row.key.as_json()["combined"] == json!(5.0)));
    assert!(changes
        .iter()
        .any(|row| row.weight == 2 && row.key.as_json()["combined"].is_null()));
}

#[test]
fn typed_join_rejects_ambiguous_columns_and_where_unknown_on_both_join_shapes() {
    let catalogs = vec![catalog("samples", false), catalog("measurement", true)];
    let ambiguous = "SELECT metric FROM samples s JOIN measurement e ON s.item = e.key_r";
    assert!(validate_supported_typed_join_project_sql(ambiguous, &catalogs).is_err());
    for sql in [
        "SELECT metric + 1.0 AS computed FROM samples s JOIN measurement e ON s.item = e.key_r",
        "SELECT s.item FROM samples s JOIN measurement e ON s.item = e.key_r WHERE metric > 0.0",
    ] {
        assert!(validate_supported_typed_join_project_sql(sql, &catalogs).is_err());
    }

    // USING disambiguates its own key only. A same-named non-key remains
    // ambiguous in expressions even when the join has a USING clause.
    let mut using_catalogs = catalogs.clone();
    let right_key = using_catalogs[1]
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "key_r")
        .unwrap();
    right_key.name = "item".into();
    let fingerprint =
        SchemaFingerprintV1::for_relation_schema(&using_catalogs[1].relation_schema).unwrap();
    using_catalogs[1].schema_fingerprint = fingerprint.clone();
    using_catalogs[1].incremental_relation.schema_fingerprint = fingerprint;
    assert!(validate_supported_typed_join_project_sql(
        "SELECT metric + 1.0 AS computed FROM samples s JOIN measurement e USING (item)",
        &using_catalogs,
    )
    .is_err());

    let sql = "SELECT s.item, e.metric AS measurement FROM samples s LEFT JOIN measurement e ON s.item = e.key_r AND s.bucket = e.bucket_r WHERE e.metric > 0.0".to_string();
    let mut output = output_schema();
    output.columns = vec![
        ColumnSchema {
            name: "item".into(),
            data_type: SqlDataType::Utf8,
            nullable: false,
        },
        ColumnSchema {
            name: "measurement".into(),
            data_type: SqlDataType::Float64,
            nullable: true,
        },
    ];
    let (mut runtime, _) = runtime_for_sql(sql, output, catalogs.clone());
    let result = runtime
        .apply_changes(
            1,
            key(1),
            vec![
                input(
                    &catalogs[0],
                    false,
                    0,
                    &[
                        ("unmatched", 10, Some(1.0), None, 1),
                        ("matched-null", 20, Some(2.0), None, 1),
                    ],
                ),
                input(
                    &catalogs[1],
                    true,
                    0,
                    &[("matched-null", 20, None, None, 1)],
                ),
            ],
        )
        .unwrap();
    assert!(result.output_deltas[0].delta.records().is_empty());
    let materialized = runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant".into(),
                program_id: "join".into(),
                view_id: "item_fact".into(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(1),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    let batch = &materialized.batches[0];
    assert_eq!(batch.schema().fields().len(), 2);
    assert_eq!(batch.schema().field(0).name(), "item");
    assert_eq!(batch.schema().field(1).name(), "measurement");
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn full_join_output_key_tracks_coalesced_key_after_computed_projection() {
    use velorix_core::view_plan::VelorixLogicalViewPlanNodeV1;
    let catalogs = vec![catalog("samples", false), catalog("measurement", true)];
    let sql = "SELECT s.metric + e.metric AS combined, COALESCE(s.item, e.key_r) AS join_key FROM samples s FULL OUTER JOIN measurement e ON s.item = e.key_r";
    let mut output = output_schema();
    output.columns = vec![
        ColumnSchema {
            name: "combined".into(),
            data_type: SqlDataType::Float64,
            nullable: true,
        },
        ColumnSchema {
            name: "join_key".into(),
            data_type: SqlDataType::Utf8,
            nullable: false,
        },
    ];
    let plan =
        lower_supported_typed_join_project_sql_to_logical_plan(sql, &catalogs, &output).unwrap();
    let join = plan
        .nodes
        .iter()
        .find_map(|node| match node {
            VelorixLogicalViewPlanNodeV1::FullEquiJoin { output_key, .. } => Some(output_key),
            _ => None,
        })
        .unwrap();
    assert_eq!(join.relation_id, "typed_join");
    assert_eq!(join.column_id, "__velorix_coalesced_join_key");
    let (mut runtime, _) = runtime_for_sql(sql.to_string(), output, catalogs.clone());
    runtime
        .apply_changes_delta_only(
            1,
            key(1),
            vec![input(
                &catalogs[1],
                true,
                0,
                &[("right-only", 1, Some(3.0), None, 2)],
            )],
        )
        .unwrap();
    assert_eq!(
        runtime
            .materialized_view_page(
                ScopedViewId {
                    tenant_id: "tenant".into(),
                    program_id: "join".into(),
                    view_id: "item_fact".into()
                },
                SnapshotPageRequest {
                    committed_epoch: Some(1),
                    page_token: None,
                    max_rows: None
                },
            )
            .unwrap()
            .batches[0]
            .num_rows(),
        2
    );
    let transition = runtime
        .apply_changes_delta_only(
            2,
            key(2),
            vec![input(
                &catalogs[0],
                false,
                0,
                &[("right-only", 1, Some(4.0), None, 1)],
            )],
        )
        .unwrap();
    let rows = transition.output_deltas[0].delta.records();
    assert!(rows
        .iter()
        .any(|row| row.weight == -2 && row.key.as_json()["join_key"] == json!("right-only")));
    assert!(rows
        .iter()
        .any(|row| row.weight == 2 && row.key.as_json()["combined"] == json!(7.0)));
    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_eq!(
        restored
            .materialized_view_page(
                ScopedViewId {
                    tenant_id: "tenant".into(),
                    program_id: "join".into(),
                    view_id: "item_fact".into()
                },
                SnapshotPageRequest {
                    committed_epoch: Some(2),
                    page_token: None,
                    max_rows: None
                },
            )
            .unwrap()
            .batches[0]
            .num_rows(),
        2
    );
}

#[test]
fn join_admission_accepts_nullable_keys_and_rejects_unimplemented_sql_shapes() {
    let catalogs = vec![catalog("samples", false), catalog("measurement", true)];
    for sql in [
        "SELECT s.item, e.metric FROM samples s LEFT JOIN measurement e ON s.bucket < e.bucket_r",
        "SELECT s.item, e.metric FROM samples s ASOF JOIN measurement e ON s.item = e.key_r",
    ] {
        assert!(validate_supported_typed_join_project_sql(sql, &catalogs).is_err());
    }
    let mut nullable = catalogs.clone();
    nullable[1].relation_schema.columns[1].nullable = true;
    let fingerprint =
        SchemaFingerprintV1::for_relation_schema(&nullable[1].relation_schema).unwrap();
    nullable[1].schema_fingerprint = fingerprint.clone();
    nullable[1].incremental_relation.schema_fingerprint = fingerprint;
    assert!(validate_supported_typed_join_project_sql(&sql("LEFT"), &nullable).is_ok());
}

type NullableKeyRow<'a> = (Option<&'a str>, Option<i64>, Option<f64>, i64);

fn nullable_key_input(
    catalog: &VelorixRelationCatalogV1,
    right: bool,
    offset: u64,
    rows: &[NullableKeyRow<'_>],
) -> RelationInputBatch {
    let converted = rows
        .iter()
        .map(|r| (r.0.unwrap_or(""), r.1.unwrap_or(0), r.2, None, r.3))
        .collect::<Vec<_>>();
    let mut input = input(catalog, right, offset, &converted);
    let base = &input.batches[0];
    let mut fields = base
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    fields[0] = fields[0].clone().with_nullable(true);
    fields[1] = fields[1].clone().with_nullable(true);
    let mut columns = base.columns().to_vec();
    columns[0] = Arc::new(StringArray::from(
        rows.iter().map(|r| r.0).collect::<Vec<_>>(),
    ));
    columns[1] = Arc::new(TimestampNanosecondArray::from(
        rows.iter().map(|r| r.1).collect::<Vec<_>>(),
    ));
    input.batches = vec![RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()];
    input
}

#[test]
fn nullable_composite_equality_keys_are_unmatched_bags_across_restore() {
    for kind in ["INNER", "LEFT", "FULL OUTER"] {
        let (mut runtime, catalogs) = runtime_with_nullable_keys(kind, true);
        let left_rows = [
            (None, Some(10), Some(2.0), 2),
            (Some("known"), None, Some(3.0), 1),
            (Some("known"), Some(10), Some(4.0), 1),
        ];
        let right_rows = [
            (None, Some(10), Some(5.0), 3),
            (Some("known"), None, Some(6.0), 1),
            (Some("known"), Some(10), Some(7.0), 2),
        ];
        runtime
            .apply_changes(
                1,
                key(1),
                vec![
                    nullable_key_input(&catalogs[0], false, 0, &left_rows),
                    nullable_key_input(&catalogs[1], true, 0, &right_rows),
                ],
            )
            .unwrap();
        let expected_count = match kind {
            "INNER" => 2,
            "LEFT" => 5,
            _ => 9,
        };
        let (before, _) = page(runtime.as_ref(), None, None);
        assert_eq!(before.len(), expected_count, "{kind}");
        // Only the fully non-null tuple produces matched payload columns.
        assert_eq!(
            before
                .iter()
                .filter(|row| !row[2].is_null() && !row[4].is_null())
                .count(),
            2,
            "{kind}"
        );
        if kind != "INNER" {
            assert_eq!(
                before
                    .iter()
                    .filter(|row| row[0].is_null() && row[2] == json!(2.0) && row[4].is_null())
                    .count(),
                2
            );
        }
        if kind == "FULL OUTER" {
            assert_eq!(
                before
                    .iter()
                    .filter(|row| row[0].is_null() && row[2].is_null() && row[4] == json!(5.0))
                    .count(),
                3
            );
        }
        let checkpoint = runtime.checkpoint().unwrap();
        let mut restored = restore_standing_runtime(checkpoint).unwrap();
        assert_eq!(page(restored.as_ref(), None, None).0, before);
        // Retract one null-key duplicate on each side and replace the second
        // left null component with a matching non-null component.
        let correction = vec![
            nullable_key_input(
                &catalogs[0],
                false,
                3,
                &[
                    (None, Some(10), Some(2.0), -1),
                    (Some("known"), None, Some(3.0), -1),
                    (Some("known"), Some(10), Some(3.0), 1),
                ],
            ),
            nullable_key_input(&catalogs[1], true, 3, &[(None, Some(10), Some(5.0), -1)]),
        ];
        restored
            .apply_changes(2, key(2), correction.clone())
            .unwrap();
        runtime.apply_changes(2, key(2), correction).unwrap();
        let (after, _) = page(restored.as_ref(), None, None);
        assert_eq!(after, page(runtime.as_ref(), None, None).0);
        assert_eq!(
            after.len(),
            match kind {
                "INNER" => 4,
                "LEFT" => 5,
                _ => 8,
            }
        );
        assert_eq!(
            after
                .iter()
                .filter(|row| !row[2].is_null() && !row[4].is_null())
                .count(),
            4
        );
        let again = restore_standing_runtime(restored.checkpoint().unwrap()).unwrap();
        assert_eq!(page(again.as_ref(), None, None).0, after);
        let checkpoint = restored.checkpoint().unwrap();
        assert!(restored
            .apply_changes(
                3,
                key(3),
                vec![nullable_key_input(
                    &catalogs[0],
                    false,
                    6,
                    &[(None, Some(10), Some(2.0), -2)]
                )]
            )
            .is_err());
        assert_eq!(restored.checkpoint().unwrap(), checkpoint);
    }
}

#[test]
fn full_join_payload_only_projection_preserves_values_and_multiplicity() {
    let catalogs = vec![catalog("samples", false), catalog("measurement", true)];
    let sql = "SELECT s.metric AS left_metric, e.metric AS right_metric FROM samples s FULL OUTER JOIN measurement e ON s.item = e.key_r".to_string();
    let mut output = output_schema();
    output.columns = vec![
        ColumnSchema {
            name: "left_metric".into(),
            data_type: SqlDataType::Float64,
            nullable: true,
        },
        ColumnSchema {
            name: "right_metric".into(),
            data_type: SqlDataType::Float64,
            nullable: true,
        },
    ];
    let (mut runtime, _) = runtime_for_sql(sql, output, catalogs.clone());
    let initial = runtime
        .apply_changes_delta_only(
            1,
            key(1),
            vec![input(
                &catalogs[1],
                true,
                0,
                &[("k", 1, Some(3.0), None, 2)],
            )],
        )
        .unwrap();
    assert!(initial.output_deltas[0].delta.records().iter().any(|row| {
        row.weight == 2
            && row.key.as_json()["left_metric"].is_null()
            && row.key.as_json()["right_metric"] == json!(3.0)
    }));
    let transition = runtime
        .apply_changes_delta_only(
            2,
            key(2),
            vec![input(
                &catalogs[0],
                false,
                0,
                &[("k", 1, Some(5.0), None, 1)],
            )],
        )
        .unwrap();
    let changes = transition.output_deltas[0].delta.records();
    assert!(changes.iter().any(|row| row.weight == -2
        && row.key.as_json()["left_metric"].is_null()
        && row.key.as_json()["right_metric"] == json!(3.0)));
    assert!(changes.iter().any(|row| row.weight == 2
        && row.key.as_json()["left_metric"] == json!(5.0)
        && row.key.as_json()["right_metric"] == json!(3.0)));
    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    let page = restored
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant".into(),
                program_id: "join".into(),
                view_id: "item_fact".into(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(2),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap();
    assert_eq!(page.batches[0].num_rows(), 2);
}

#[test]
fn full_join_typed_coalesce_defaults_nullable_payloads() {
    let catalogs = vec![catalog("samples", false), catalog("measurement", true)];
    let sql = "SELECT COALESCE(s.metric, 0.0) AS left_metric, COALESCE(e.metric, 0.0) AS right_metric FROM samples s FULL OUTER JOIN measurement e ON s.item = e.key_r".to_string();
    let mut output = output_schema();
    output.columns = vec![
        ColumnSchema {
            name: "left_metric".into(),
            data_type: SqlDataType::Float64,
            nullable: false,
        },
        ColumnSchema {
            name: "right_metric".into(),
            data_type: SqlDataType::Float64,
            nullable: false,
        },
    ];
    let (mut runtime, _) = runtime_for_sql(sql, output, catalogs.clone());

    let changes = runtime
        .apply_changes_delta_only(
            1,
            key(1),
            vec![
                input(&catalogs[0], false, 0, &[("left", 1, None, None, 1)]),
                input(&catalogs[1], true, 0, &[("right", 1, Some(4.5), None, 1)]),
            ],
        )
        .unwrap();
    let rows = changes.output_deltas[0].delta.records();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| {
        row.key.as_json()["left_metric"] == json!(0.0)
            && row.key.as_json()["right_metric"] == json!(0.0)
    }));
    assert!(rows.iter().any(|row| {
        row.key.as_json()["left_metric"] == json!(0.0)
            && row.key.as_json()["right_metric"] == json!(4.5)
    }));
    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_eq!(restored.logical_epoch(), 1);
}
