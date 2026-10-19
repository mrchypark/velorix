use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use serde_json::json;
use velorix_core::{
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1, VelorixRelationSourceV1,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
    standing_program::{
        BuiltinRuntimeIdentity, EpochCommit, EpochIdempotencyKey, NativeCodePolicy,
        RelationInputBatch, RelationInputEncodingV1, ScopedViewId, SnapshotPageRequest,
        StandingProgramIdentity, StandingProgramRuntime,
    },
    view_contract::{
        catalog_from_published_relation_binding, catalog_input_relation_schema,
        published_relation_binding_v1, stable_bytes_hash, ColumnSchema, RelationSchema,
        SqlDataType, PUBLISHED_DELTA_WEIGHT_FIELD_V1, PUBLISHED_RELATION_DELTA_CODEC_V1,
    },
    view_plan::validate_supported_ordered_window_sql,
};
use velorix_runtime::materialized_view_runtime::{
    create_standing_runtime_with_sql_and_catalogs, restore_standing_runtime, CRATE_NAME,
};

#[derive(Clone, Copy)]
struct Row<'a> {
    key: &'a str,
    group_id: Option<&'a str>,
    day: i64,
    ts: i64,
    metric: Option<f64>,
    auxiliary: Option<f64>,
    weight: i64,
}

fn row(key: &str, ts: i64, metric: Option<f64>) -> Row<'_> {
    Row {
        key,
        group_id: Some("group-a"),
        day: 1,
        ts,
        metric,
        auxiliary: Some(230.25),
        weight: 1,
    }
}

fn catalog() -> VelorixRelationCatalogV1 {
    let specs = [
        (
            "id",
            VelorixLogicalTypeV1::Utf8,
            ArrowPhysicalTypeV1::Utf8,
            false,
        ),
        (
            "group_id",
            VelorixLogicalTypeV1::Utf8,
            ArrowPhysicalTypeV1::Utf8,
            true,
        ),
        (
            "day",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            false,
        ),
        (
            "ts",
            VelorixLogicalTypeV1::Timestamp { timezone: None },
            ArrowPhysicalTypeV1::TimestampNanosecond { timezone: None },
            false,
        ),
        (
            "metric",
            VelorixLogicalTypeV1::Float64,
            ArrowPhysicalTypeV1::Float64,
            true,
        ),
        (
            "auxiliary",
            VelorixLogicalTypeV1::Float64,
            ArrowPhysicalTypeV1::Float64,
            true,
        ),
        (
            "delta",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            false,
        ),
    ];
    let schema = VelorixRelationSchemaV1 {
        relation_id: "events".into(),
        relation_name: "events".into(),
        relation_version: "v1".into(),
        columns: specs
            .into_iter()
            .enumerate()
            .map(
                |(ordinal, (name, logical_type, physical_arrow_type, nullable))| RelationColumnV1 {
                    column_id: name.into(),
                    name: name.into(),
                    logical_type,
                    physical_arrow_type,
                    nullable,
                    ordinal: ordinal as u32,
                    semantic_role: match name {
                        "id" => RelationSemanticRoleV1::PrimaryKey,
                        "delta" => RelationSemanticRoleV1::Weight,
                        _ => RelationSemanticRoleV1::Value,
                    },
                },
            )
            .collect(),
        primary_key_column_ids: vec!["id".into()],
        weight_column_id: "delta".into(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: Some("ts".into()),
    };
    let fingerprint = SchemaFingerprintV1::for_relation_schema(&schema).unwrap();
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema: schema,
        schema_fingerprint: fingerprint.clone(),
        relation_source: VelorixRelationSourceV1::SourceRelation,
        datafusion_registration: DataFusionRegistrationV1 {
            name: "events".into(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "events".into(),
            schema_fingerprint: fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
        },
    }
}

fn output(columns: &[(&str, SqlDataType, bool)]) -> RelationSchema {
    RelationSchema {
        relation_id: "ordered".into(),
        relation_name: "ordered".into(),
        relation_version: "v1".into(),
        schema_fingerprint: "ordered-v1".into(),
        columns: columns
            .iter()
            .map(|(name, data_type, nullable)| ColumnSchema {
                name: (*name).into(),
                data_type: data_type.clone(),
                nullable: *nullable,
            })
            .collect(),
        primary_key: vec!["id".into()],
    }
}

fn identity(sql: &str) -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "tenant-a".into(),
        program_id: "ordered-program".into(),
        view_ids: vec!["ordered".into()],
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
    }
}

fn runtime(sql: &str, output: RelationSchema) -> Box<dyn StandingProgramRuntime> {
    let catalog = catalog();
    create_standing_runtime_with_sql_and_catalogs(
        &identity(sql),
        std::slice::from_ref(&catalog),
        sql,
        &[catalog_input_relation_schema(&catalog).unwrap()],
        &[output],
    )
    .unwrap()
}

fn input(rows: &[Row<'_>], offset: u64) -> RelationInputBatch {
    let catalog = catalog();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.key).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.group_id).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.day).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.ts).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.metric).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.auxiliary).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.weight).collect::<Vec<_>>(),
        )),
    ];
    let fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("group_id", DataType::Utf8, true),
        Field::new("day", DataType::Int64, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("metric", DataType::Float64, true),
        Field::new("auxiliary", DataType::Float64, true),
        Field::new("delta", DataType::Int64, false),
    ];
    RelationInputBatch {
        encoding: RelationInputEncodingV1::SourceRelationV1,
        relation_id: "events".into(),
        relation_version: "v1".into(),
        stream_id: "stream".into(),
        partition_id: 0,
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        start_offset_inclusive: offset,
        end_offset_exclusive: offset + rows.len() as u64,
        event_time_watermark: None,
        batches: vec![RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()],
    }
}

fn apply(
    runtime: &mut dyn StandingProgramRuntime,
    epoch: u64,
    offset: u64,
    rows: &[Row<'_>],
) -> EpochCommit {
    runtime
        .apply_changes(
            epoch,
            EpochIdempotencyKey::new(format!("epoch-{epoch}")).unwrap(),
            vec![input(rows, offset)],
        )
        .unwrap()
}

fn snapshot(runtime: &dyn StandingProgramRuntime) -> RecordBatch {
    runtime
        .materialized_view_page(
            ScopedViewId {
                tenant_id: "tenant-a".into(),
                program_id: "ordered-program".into(),
                view_id: "ordered".into(),
            },
            SnapshotPageRequest {
                committed_epoch: Some(runtime.logical_epoch()),
                page_token: None,
                max_rows: None,
            },
        )
        .unwrap()
        .batches
        .remove(0)
}

fn float_values(batch: &RecordBatch, name: &str) -> BTreeMap<String, Option<f64>> {
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|i| {
            (
                ids.value(i).into(),
                (!values.is_null(i)).then(|| values.value(i)),
            )
        })
        .collect()
}

#[test]
fn wide_ordered_aliases_hit_expansion_budget_before_output_clones() {
    use velorix_runtime::materialized_view_runtime::{
        restore_standing_runtime_with_limits, RuntimeResourceLimits,
    };
    let wide = "x".repeat(512 * 1024);
    for lag in [false, true] {
        let aliases = (0..100)
            .map(|index| format!("alias_{index}"))
            .collect::<Vec<_>>();
        let mut expressions = vec!["id".to_string()];
        let mut schema = output(&[("id", SqlDataType::Utf8, false)]);
        for alias in &aliases {
            expressions.push(if lag {
                format!("lag(group_id) over (partition by day order by ts) as {alias}")
            } else {
                format!("group_id as {alias}")
            });
            schema.columns.push(ColumnSchema {
                name: alias.clone(),
                data_type: SqlDataType::Utf8,
                nullable: true,
            });
        }
        if !lag {
            expressions.push("lag(metric) over (partition by day order by ts) as previous".into());
            schema.columns.push(ColumnSchema {
                name: "previous".into(),
                data_type: SqlDataType::Float64,
                nullable: true,
            });
        }
        let sql = format!("select {} from events", expressions.join(", "));
        let initial = runtime(&sql, schema).checkpoint().unwrap();
        let limits = RuntimeResourceLimits {
            max_checkpoint_bytes: 64 * 1024 * 1024,
            max_snapshot_bytes: 64 * 1024 * 1024,
            ..RuntimeResourceLimits::default()
        };
        let mut runtime = restore_standing_runtime_with_limits(initial.clone(), limits).unwrap();
        let rows = [
            Row {
                group_id: Some(&wide),
                ..row("a", 10, Some(1.0))
            },
            Row {
                group_id: Some(&wide),
                ..row("b", 20, Some(2.0))
            },
        ];
        let error = runtime
            .apply_changes(
                1,
                EpochIdempotencyKey::new("retry").unwrap(),
                vec![input(&rows, 0)],
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("runtime_expansion_bytes"),
            "{error}"
        );
        assert_eq!(
            runtime.checkpoint().unwrap().state_root.content_hash,
            initial.state_root.content_hash
        );
        runtime
            .apply_changes(
                1,
                EpochIdempotencyKey::new("retry").unwrap(),
                vec![input(
                    &[row("a", 10, Some(1.0)), row("b", 20, Some(2.0))],
                    0,
                )],
            )
            .unwrap();
    }
}

#[test]
fn ordered_resource_limits_reject_atomically_and_restore_with_lower_limits() {
    use velorix_runtime::materialized_view_runtime::{
        restore_standing_runtime_with_limits, RuntimeResourceLimits,
    };
    let sql = "select id, lag(metric) over (partition by group_id, day order by ts) as previous from events";
    let schema = output(&[
        ("id", SqlDataType::Utf8, false),
        ("previous", SqlDataType::Float64, true),
    ]);
    let baseline = runtime(sql, schema);
    let initial = baseline.checkpoint().unwrap();
    let defaults = RuntimeResourceLimits::default();
    for limits in [
        RuntimeResourceLimits {
            max_checkpoint_bytes: initial.state_payload.as_ref().unwrap().payload.len(),
            ..defaults
        },
        RuntimeResourceLimits {
            max_expansion_bytes: 1,
            ..defaults
        },
        RuntimeResourceLimits {
            max_expansion_rows: 1,
            ..defaults
        },
        RuntimeResourceLimits {
            max_snapshot_bytes: 1,
            ..defaults
        },
    ] {
        let mut runtime = restore_standing_runtime_with_limits(initial.clone(), limits).unwrap();
        assert!(runtime
            .apply_changes(
                1,
                EpochIdempotencyKey::new("retry").unwrap(),
                vec![input(
                    &[row("a", 10, Some(1.0)), row("b", 20, Some(2.0))],
                    0
                )]
            )
            .is_err());
        assert_eq!(
            runtime.checkpoint().unwrap().state_root.content_hash,
            initial.state_root.content_hash
        );
    }
    let mut runtime = baseline;
    apply(
        runtime.as_mut(),
        1,
        0,
        &[row("a", 10, Some(1.0)), row("b", 20, Some(2.0))],
    );
    let checkpoint = runtime.checkpoint().unwrap();
    assert!(restore_standing_runtime_with_limits(
        checkpoint.clone(),
        RuntimeResourceLimits {
            max_checkpoint_bytes: checkpoint.state_payload.as_ref().unwrap().payload.len() - 1,
            ..defaults
        }
    )
    .is_err());
    assert!(restore_standing_runtime_with_limits(
        checkpoint,
        RuntimeResourceLimits {
            max_expansion_rows: 1,
            ..defaults
        }
    )
    .is_err());
}

#[test]
fn composite_range_peers_nulls_late_suffix_retractions_and_restore() {
    let sql = "select id, group_id, day, ts, metric, sum(metric) over (partition by group_id, day order by ts) as cumulative from events";
    let schema = output(&[
        ("id", SqlDataType::Utf8, false),
        ("group_id", SqlDataType::Utf8, true),
        ("day", SqlDataType::Int64, false),
        ("ts", SqlDataType::Timestamp { timezone: None }, false),
        ("metric", SqlDataType::Float64, true),
        ("cumulative", SqlDataType::Float64, true),
    ]);
    let mut runtime = runtime(sql, schema);
    let initial = [
        row("a", 10, None),
        row("b", 20, Some(2.25)),
        row("c", 20, Some(3.5)),
        row("d", 30, Some(1.0)),
        Row {
            day: 2,
            ..row("e", 10, Some(50.0))
        },
        Row {
            group_id: None,
            ..row("f", 10, None)
        },
        Row {
            group_id: None,
            ..row("g", 20, Some(7.0))
        },
    ];
    apply(runtime.as_mut(), 1, 0, &initial);
    assert_eq!(
        float_values(&snapshot(runtime.as_ref()), "cumulative"),
        BTreeMap::from([
            ("a".into(), None),
            ("b".into(), Some(5.75)),
            ("c".into(), Some(5.75)),
            ("d".into(), Some(6.75)),
            ("e".into(), Some(50.0)),
            ("f".into(), None),
            ("g".into(), Some(7.0))
        ])
    );
    let mut runtime = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    // This timestamp is arbitrarily older than all existing input: no age horizon.
    let commit = apply(
        runtime.as_mut(),
        2,
        7,
        &[row("late", i64::MIN + 1, Some(0.25))],
    );
    let net = commit.output_deltas[0].delta.net_rows().unwrap();
    assert_eq!(net.len(), 9); // four changed rows (-old,+new), one new row
    assert!(!net
        .iter()
        .any(|r| [json!("e"), json!("f"), json!("g")].contains(r.key.as_json())));
    assert_eq!(
        float_values(&snapshot(runtime.as_ref()), "cumulative")["d"],
        Some(7.0)
    );
    let removed = Row {
        weight: -1,
        ..initial[2]
    };
    let commit = apply(runtime.as_mut(), 3, 8, &[removed]);
    let net = commit.output_deltas[0].delta.net_rows().unwrap();
    assert_eq!(net.len(), 5); // c removed, b/d corrected
    assert!(net
        .iter()
        .any(|r| r.key.as_json() == &json!("c") && r.weight == -1));
    assert_eq!(
        float_values(&snapshot(runtime.as_ref()), "cumulative")["b"],
        Some(2.5)
    );
    assert_eq!(
        float_values(&snapshot(runtime.as_ref()), "cumulative")["d"],
        Some(3.5)
    );
    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_eq!(snapshot(runtime.as_ref()), snapshot(restored.as_ref()));
}

#[test]
fn rows_frame_uses_primary_key_ties_and_excludes_preceding_end() {
    let sql = "select id, sum(metric) over (partition by group_id, day order by ts rows between unbounded preceding and current row) as cumulative, sum(metric) over (partition by group_id, day order by ts rows between unbounded preceding and 1 preceding) as previous from events";
    let mut runtime = runtime(
        sql,
        output(&[
            ("id", SqlDataType::Utf8, false),
            ("cumulative", SqlDataType::Float64, true),
            ("previous", SqlDataType::Float64, true),
        ]),
    );
    apply(
        runtime.as_mut(),
        1,
        0,
        &[
            row("c", 10, Some(3.0)),
            row("a", 10, None),
            row("b", 10, Some(2.0)),
            row("d", 20, None),
        ],
    );
    let batch = snapshot(runtime.as_ref());
    assert_eq!(
        float_values(&batch, "cumulative"),
        BTreeMap::from([
            ("a".into(), None),
            ("b".into(), Some(2.0)),
            ("c".into(), Some(5.0)),
            ("d".into(), Some(5.0))
        ])
    );
    assert_eq!(
        float_values(&batch, "previous"),
        BTreeMap::from([
            ("a".into(), None),
            ("b".into(), None),
            ("c".into(), Some(2.0)),
            ("d".into(), Some(5.0))
        ])
    );
}

#[test]
fn multiple_lag_outputs_preserve_first_rows_types_nulls_and_retractions() {
    let sql = "select id, ts, metric, auxiliary, lag(ts) over (partition by group_id, day order by ts) as timestamauxiliary_last, lag(metric) over (partition by group_id, day order by ts) as metric_last, lag(auxiliary, 2) over (partition by group_id, day order by ts) as auxiliary_last from events";
    let schema = output(&[
        ("id", SqlDataType::Utf8, false),
        ("ts", SqlDataType::Timestamp { timezone: None }, false),
        ("metric", SqlDataType::Float64, true),
        ("auxiliary", SqlDataType::Float64, true),
        (
            "timestamauxiliary_last",
            SqlDataType::Timestamp { timezone: None },
            true,
        ),
        ("metric_last", SqlDataType::Float64, true),
        ("auxiliary_last", SqlDataType::Float64, true),
    ]);
    let mut runtime = runtime(sql, schema.clone());
    let rows = [
        row("a", 10, Some(1.0)),
        Row {
            auxiliary: None,
            ..row("b", 20, None)
        },
        row("c", 30, Some(3.0)),
    ];
    apply(runtime.as_mut(), 1, 0, &rows);
    let batch = snapshot(runtime.as_ref());
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
    );
    let times = batch
        .column_by_name("timestamauxiliary_last")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert!(times.is_null(0));
    assert_eq!((times.value(1), times.value(2)), (10, 20));
    assert_eq!(
        float_values(&batch, "metric_last"),
        BTreeMap::from([
            ("a".into(), None),
            ("b".into(), Some(1.0)),
            ("c".into(), None)
        ])
    );
    let mut runtime = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    apply(
        runtime.as_mut(),
        2,
        3,
        &[Row {
            weight: -1,
            ..rows[1]
        }],
    );
    let batch = snapshot(runtime.as_ref());
    assert_eq!(float_values(&batch, "metric_last")["c"], Some(1.0));
    assert_eq!(float_values(&batch, "auxiliary_last")["c"], None);
    assert!(batch.schema().field(4).is_nullable());
}

#[test]
fn invalid_retraction_or_later_batch_rolls_back_everything_and_retry_is_idempotent() {
    let sql = "select id, sum(metric) over (partition by group_id, day order by ts) as cumulative from events";
    let mut runtime = runtime(
        sql,
        output(&[
            ("id", SqlDataType::Utf8, false),
            ("cumulative", SqlDataType::Float64, true),
        ]),
    );
    apply(runtime.as_mut(), 1, 0, &[row("a", 10, Some(1.0))]);
    let before = runtime.checkpoint().unwrap();
    let valid = input(&[row("b", 20, Some(2.0))], 1);
    let mut invalid = input(&[row("c", 30, Some(3.0))], 2);
    invalid.schema_fingerprint = "wrong".into();
    assert!(runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![valid, invalid]
        )
        .is_err());
    assert_eq!(runtime.checkpoint().unwrap(), before);
    assert!(runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![input(
                &[Row {
                    weight: -1,
                    ..row("a", 10, Some(999.0))
                }],
                1
            )]
        )
        .is_err());
    assert_eq!(runtime.checkpoint().unwrap(), before);
    apply(runtime.as_mut(), 2, 1, &[row("b", 20, Some(2.0))]);
    let before = runtime.checkpoint().unwrap();
    let retry = runtime
        .apply_changes(2, EpochIdempotencyKey::new("epoch-2").unwrap(), Vec::new())
        .unwrap();
    assert!(retry.output_deltas.is_empty());
    assert_eq!(runtime.checkpoint().unwrap(), before);
}

#[test]
fn unsupported_window_shapes_fail_admission() {
    let catalog = catalog();
    for sql in [
        "select id, max(metric) over (partition by group_id order by ts) as x from events",
        "select id, sum(metric) over (partition by group_id order by ts rows between 1 preceding and current row) as x from events",
        "select id, sum(metric) over (partition by group_id order by ts range between unbounded preceding and 1 preceding) as x from events",
        "select id, lag(metric, 1, 0) over (partition by group_id order by ts) as x from events",
        "select id, sum(metric) over (partition by group_id order by ts, day) as x from events",
    ] {
        assert!(validate_supported_ordered_window_sql(sql, &catalog).is_err(), "unexpected admission: {sql}");
    }
}

#[test]
fn global_partition_preserves_select_order_and_descending_peers() {
    let sql =
        "select metric, sum(metric) over (order by ts desc) as cumulative, id, ts from events";
    let mut runtime = runtime(
        sql,
        output(&[
            ("metric", SqlDataType::Float64, true),
            ("cumulative", SqlDataType::Float64, true),
            ("id", SqlDataType::Utf8, false),
            ("ts", SqlDataType::Timestamp { timezone: None }, false),
        ]),
    );
    apply(
        runtime.as_mut(),
        1,
        0,
        &[
            row("a", 10, Some(1.0)),
            row("b", 20, Some(2.0)),
            Row {
                group_id: Some("other"),
                day: 2,
                ..row("c", 20, Some(3.0))
            },
        ],
    );
    let batch = snapshot(runtime.as_ref());
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["metric", "cumulative", "id", "ts"]
    );
    assert_eq!(
        float_values(&batch, "cumulative"),
        BTreeMap::from([
            ("a".into(), Some(6.0)),
            ("b".into(), Some(5.0)),
            ("c".into(), Some(5.0))
        ])
    );
    let mut runtime = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    apply(runtime.as_mut(), 2, 3, &[row("late", 0, Some(4.0))]);
    assert_eq!(
        float_values(&snapshot(runtime.as_ref()), "cumulative")["late"],
        Some(10.0)
    );
}

#[test]
fn key_update_moves_partitions_and_corrects_both_suffixes() {
    let sql = "select id, sum(metric) over (partition by group_id, day order by ts) as cumulative from events";
    let mut runtime = runtime(
        sql,
        output(&[
            ("id", SqlDataType::Utf8, false),
            ("cumulative", SqlDataType::Float64, true),
        ]),
    );
    let a = row("a", 10, Some(2.0));
    apply(
        runtime.as_mut(),
        1,
        0,
        &[
            a,
            row("b", 20, Some(3.0)),
            Row {
                day: 2,
                ..row("c", 20, Some(4.0))
            },
        ],
    );
    apply(
        runtime.as_mut(),
        2,
        3,
        &[Row { day: 2, ..a }, Row { weight: -1, ..a }],
    );
    assert_eq!(
        float_values(&snapshot(runtime.as_ref()), "cumulative"),
        BTreeMap::from([
            ("a".into(), Some(2.0)),
            ("b".into(), Some(3.0)),
            ("c".into(), Some(6.0))
        ])
    );
    let checkpoint = runtime.checkpoint().unwrap();
    let mut runtime = restore_standing_runtime(checkpoint).unwrap();
    apply(
        runtime.as_mut(),
        3,
        5,
        &[Row {
            day: 2,
            weight: -1,
            ..a
        }],
    );
    assert_eq!(
        float_values(&snapshot(runtime.as_ref()), "cumulative")["c"],
        Some(4.0)
    );
}

#[test]
fn item_fact_lags_timestamp_metric_sequence_and_late_row_corrects_next_sample() {
    // Bind timestamp, metric and sequence columns before testing lag corrections.
    let sql = "select ts as timestamp_ns, auxiliary as auxiliary_value, day as seqnum, id, lag(ts) over (partition by group_id order by ts) as timestamauxiliary_last, lag(auxiliary) over (partition by group_id order by ts) as auxiliary_last, lag(day) over (partition by group_id order by ts) as seqnum_last from events";
    let mut catalog = catalog();
    catalog.relation_schema.columns[3].logical_type = VelorixLogicalTypeV1::Int64;
    catalog.relation_schema.columns[3].physical_arrow_type = ArrowPhysicalTypeV1::Int64;
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    let schema = output(&[
        ("timestamp_ns", SqlDataType::Int64, false),
        ("auxiliary_value", SqlDataType::Float64, true),
        ("seqnum", SqlDataType::Int64, false),
        ("id", SqlDataType::Utf8, false),
        ("timestamauxiliary_last", SqlDataType::Int64, true),
        ("auxiliary_last", SqlDataType::Float64, true),
        ("seqnum_last", SqlDataType::Int64, true),
    ]);
    let mut runtime = create_standing_runtime_with_sql_and_catalogs(
        &identity(sql),
        std::slice::from_ref(&catalog),
        sql,
        &[catalog_input_relation_schema(&catalog).unwrap()],
        &[schema],
    )
    .unwrap();
    let apply_item =
        |runtime: &mut dyn StandingProgramRuntime, epoch: u64, offset: u64, rows: &[Row<'_>]| {
            let mut batch = input(rows, offset);
            batch.schema_fingerprint = catalog.schema_fingerprint.to_string();
            let original = batch.batches.remove(0);
            let mut fields = original
                .schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            fields[3] = Field::new("ts", DataType::Int64, false);
            let mut arrays = original.columns().to_vec();
            arrays[3] = Arc::new(Int64Array::from(
                rows.iter().map(|row| row.ts).collect::<Vec<_>>(),
            ));
            batch
                .batches
                .push(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap());
            runtime
                .apply_changes(
                    epoch,
                    EpochIdempotencyKey::new(format!("epoch-{epoch}")).unwrap(),
                    vec![batch],
                )
                .unwrap()
        };
    let first = Row {
        day: 100,
        auxiliary: Some(20.0),
        ..row("a", 1_000_000_001, None)
    };
    let last = Row {
        day: 102,
        auxiliary: Some(40.0),
        ..row("c", 3_000_000_003, None)
    };
    apply_item(runtime.as_mut(), 1, 0, &[first, last]);
    let batch = snapshot(runtime.as_ref());
    assert_eq!(float_values(&batch, "auxiliary_last")["c"], Some(20.0));
    let seq = batch
        .column_by_name("seqnum_last")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(seq.is_null(0));
    assert_eq!(seq.value(1), 100);
    let mut runtime = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    let late = Row {
        day: 101,
        auxiliary: Some(30.0),
        ..row("b", 2_000_000_002, None)
    };
    let commit = apply_item(runtime.as_mut(), 2, 2, &[late]);
    assert_eq!(commit.output_deltas[0].delta.net_rows().unwrap().len(), 3);
    let batch = snapshot(runtime.as_ref());
    assert_eq!(float_values(&batch, "auxiliary_last")["c"], Some(30.0));
    let times = batch
        .column_by_name("timestamauxiliary_last")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(times.value(2), 2_000_000_002);
    let seq = batch
        .column_by_name("seqnum_last")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(seq.value(2), 101);
    apply_item(runtime.as_mut(), 3, 3, &[Row { weight: -1, ..late }]);
    assert_eq!(
        float_values(&snapshot(runtime.as_ref()), "auxiliary_last")["c"],
        Some(20.0)
    );
}

#[test]
fn published_bucket_deltas_drive_grouped_and_global_prefixes_after_restart() {
    let mut producer = catalog_input_relation_schema(&catalog()).unwrap();
    producer.columns.retain(|column| column.name != "delta");
    let binding = published_relation_binding_v1(
        "item_buckets",
        1,
        &stable_bytes_hash(b"native-item-buckets"),
        &producer,
    )
    .unwrap();
    let catalog = catalog_from_published_relation_binding(&binding).unwrap();
    for partition in ["partition by group_id", ""] {
        let sql = format!(
            "select id, sum(metric) over ({partition} order by ts) as cumulative from events"
        );
        let mut runtime = create_standing_runtime_with_sql_and_catalogs(
            &identity(&sql),
            std::slice::from_ref(&catalog),
            &sql,
            &[catalog_input_relation_schema(&catalog).unwrap()],
            &[output(&[
                ("id", SqlDataType::Utf8, false),
                ("cumulative", SqlDataType::Float64, true),
            ])],
        )
        .unwrap();
        let apply_published = |runtime: &mut dyn StandingProgramRuntime,
                               epoch: u64,
                               offset: u64,
                               rows: &[Row<'_>]| {
            let mut input = input(rows, offset);
            input.stream_id = binding.output_stream_id.clone();
            input.schema_fingerprint = binding.relation.schema_fingerprint.clone();
            input.encoding = RelationInputEncodingV1::PublishedRelationDeltaV1 {
                delta_codec_identity: PUBLISHED_RELATION_DELTA_CODEC_V1.into(),
                output_schema_hash: binding.output_schema_hash.clone(),
                weight_field_name: PUBLISHED_DELTA_WEIGHT_FIELD_V1.into(),
                weight_field_index: binding.relation.columns.len(),
            };
            let original = input.batches.remove(0);
            let mut fields = original
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>();
            *fields.last_mut().unwrap() =
                Field::new(PUBLISHED_DELTA_WEIGHT_FIELD_V1, DataType::Int64, false);
            input.batches.push(
                RecordBatch::try_new(Arc::new(Schema::new(fields)), original.columns().to_vec())
                    .unwrap(),
            );
            runtime
                .apply_changes(
                    epoch,
                    EpochIdempotencyKey::new(format!("epoch-{epoch}")).unwrap(),
                    vec![input],
                )
                .unwrap()
        };
        let a = row("a", 10, Some(1.25));
        let b = row("b", 20, Some(2.5));
        let c = Row {
            group_id: Some("group-b"),
            ..row("c", 20, Some(4.0))
        };
        apply_published(runtime.as_mut(), 1, 0, &[a, b, c]);
        assert_eq!(
            float_values(&snapshot(runtime.as_ref()), "cumulative")["b"],
            Some(if partition.is_empty() { 7.75 } else { 3.75 })
        );
        let mut runtime = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
        // Published upstream corrections retract the old bucket and insert its new value.
        apply_published(
            runtime.as_mut(),
            2,
            3,
            &[
                Row { weight: -1, ..a },
                Row {
                    metric: Some(2.0),
                    ..a
                },
            ],
        );
        assert_eq!(
            float_values(&snapshot(runtime.as_ref()), "cumulative")["b"],
            Some(if partition.is_empty() { 8.5 } else { 4.5 })
        );
        let batch = snapshot(runtime.as_ref());
        let empty = apply_published(runtime.as_mut(), 3, 5, &[]);
        assert!(empty.output_deltas[0].delta.net_rows().unwrap().is_empty());
        assert_eq!(batch, snapshot(runtime.as_ref()));
    }
}

#[test]
fn overflow_and_duplicate_keys_are_atomic_and_deleted_partitions_restore_empty() {
    let sql =
        "select id, sum(metric) over (partition by group_id order by ts) as cumulative from events";
    let mut runtime = runtime(
        sql,
        output(&[
            ("id", SqlDataType::Utf8, false),
            ("cumulative", SqlDataType::Float64, true),
        ]),
    );
    let first = row("a", 10, Some(1e308));
    apply(runtime.as_mut(), 1, 0, &[first]);
    let before = runtime.checkpoint().unwrap();
    assert!(runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![input(&[row("b", 20, Some(1e308))], 1)]
        )
        .is_err());
    assert_eq!(runtime.checkpoint().unwrap(), before);
    assert!(runtime
        .apply_changes(
            2,
            EpochIdempotencyKey::new("epoch-2").unwrap(),
            vec![input(&[first], 1)]
        )
        .is_err());
    assert_eq!(runtime.checkpoint().unwrap(), before);
    apply(
        runtime.as_mut(),
        2,
        1,
        &[Row {
            weight: -1,
            ..first
        }],
    );
    let mut runtime = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    assert_eq!(snapshot(runtime.as_ref()).num_rows(), 0);
    apply(runtime.as_mut(), 3, 2, &[row("a", 0, Some(0.25))]);
    assert_eq!(
        float_values(&snapshot(runtime.as_ref()), "cumulative")["a"],
        Some(0.25)
    );
}

#[test]
fn restore_rejects_inconsistent_materialized_output_and_missing_retained_row_values() {
    let sql =
        "select id, sum(metric) over (partition by group_id order by ts) as cumulative from events";
    let mut runtime = runtime(
        sql,
        output(&[
            ("id", SqlDataType::Utf8, false),
            ("cumulative", SqlDataType::Float64, true),
        ]),
    );
    apply(runtime.as_mut(), 1, 0, &[row("a", 10, Some(2.0))]);
    let original = runtime.checkpoint().unwrap();
    for remove_values in [false, true] {
        let mut checkpoint = original.clone();
        let state = checkpoint.state_payload.as_mut().unwrap();
        let mut payload: serde_json::Value = serde_json::from_str(&state.payload).unwrap();
        if remove_values {
            let partitions = payload["partitions"].as_object_mut().unwrap();
            let partition = partitions
                .values_mut()
                .next()
                .unwrap()
                .as_object_mut()
                .unwrap();
            partition.values_mut().next().unwrap()["values"]
                .as_object_mut()
                .unwrap()
                .remove("auxiliary");
        } else {
            payload["published_output"] = json!({"records": []});
        }
        state.payload = serde_json::to_string(&payload).unwrap();
        checkpoint.state_root.content_hash = stable_bytes_hash(state.payload.as_bytes());
        assert!(restore_standing_runtime(checkpoint).is_err());
    }
}

#[test]
fn float_partition_groups_signed_zero_as_sql_equal() {
    let sql = "select id, sum(metric) over (partition by auxiliary order by ts) as cumulative from events";
    let mut runtime = runtime(
        sql,
        output(&[
            ("id", SqlDataType::Utf8, false),
            ("cumulative", SqlDataType::Float64, true),
        ]),
    );
    apply(
        runtime.as_mut(),
        1,
        0,
        &[
            Row {
                auxiliary: Some(-0.0),
                ..row("a", 10, Some(2.0))
            },
            Row {
                auxiliary: Some(0.0),
                ..row("b", 20, Some(3.0))
            },
        ],
    );
    assert_eq!(
        float_values(&snapshot(runtime.as_ref()), "cumulative")["b"],
        Some(5.0)
    );
    let mut runtime = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    apply(
        runtime.as_mut(),
        2,
        2,
        &[Row {
            auxiliary: Some(0.0),
            ..row("late", 0, Some(1.0))
        }],
    );
    assert_eq!(
        float_values(&snapshot(runtime.as_ref()), "cumulative")["b"],
        Some(6.0)
    );
}

#[test]
fn item_named_window_binds_same_plan_as_inline_and_restores_multiple_lags() {
    let named = "select id, lag(ts) over item_time as timestamauxiliary_last, lag(auxiliary) over item_time as auxiliary_last, lag(day) over item_time as seqnum_last from events window item_time as (partition by group_id order by ts)";
    let inline = "select id, lag(ts) over (partition by group_id order by ts) as timestamauxiliary_last, lag(auxiliary) over (partition by group_id order by ts) as auxiliary_last, lag(day) over (partition by group_id order by ts) as seqnum_last from events";
    assert_eq!(
        validate_supported_ordered_window_sql(named, &catalog()).unwrap(),
        validate_supported_ordered_window_sql(inline, &catalog()).unwrap()
    );
    let mut runtime = runtime(
        named,
        output(&[
            ("id", SqlDataType::Utf8, false),
            (
                "timestamauxiliary_last",
                SqlDataType::Timestamp { timezone: None },
                true,
            ),
            ("auxiliary_last", SqlDataType::Float64, true),
            ("seqnum_last", SqlDataType::Int64, true),
        ]),
    );
    apply(
        runtime.as_mut(),
        1,
        0,
        &[
            row("b", 20, None),
            Row {
                day: 100,
                ..row("a", 10, None)
            },
        ],
    );
    let restored = restore_standing_runtime(runtime.checkpoint().unwrap()).unwrap();
    let batch = snapshot(restored.as_ref());
    assert_eq!(batch.num_rows(), 2);
    let seq = batch
        .column_by_name("seqnum_last")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(seq.is_null(0));
    assert_eq!(seq.value(1), 100);
    assert!(validate_supported_ordered_window_sql(
        "select id, lag(auxiliary) over missing as auxiliary_last from events",
        &catalog()
    )
    .is_err());
}
