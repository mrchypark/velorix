use arrow::{
    array::{Array, ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray},
    record_batch::RecordBatch,
};
use std::sync::Arc;
use velorix_core::{
    relation::*,
    standing_program::*,
    view_contract::{catalog_input_relation_schema, stable_bytes_hash, RelationSchema},
    view_plan::{
        supported_ordered_window_output_columns, supported_ordered_window_output_primary_key,
        validate_supported_ordered_window_sql,
    },
};
use velorix_runtime::materialized_view_runtime::{
    create_standing_runtime_with_sql_and_catalogs, restore_standing_runtime, CRATE_NAME,
};

fn catalog(composite: bool) -> VelorixRelationCatalogV1 {
    let specs = [
        (
            "entity",
            VelorixLogicalTypeV1::Utf8,
            ArrowPhysicalTypeV1::Utf8,
        ),
        (
            "instant",
            VelorixLogicalTypeV1::Timestamp { timezone: None },
            ArrowPhysicalTypeV1::TimestampNanosecond { timezone: None },
        ),
        (
            "position",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
        ),
        (
            "amount",
            VelorixLogicalTypeV1::Float64,
            ArrowPhysicalTypeV1::Float64,
        ),
        (
            "weight",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
        ),
    ];
    let keys = if composite {
        vec!["entity".into(), "instant".into()]
    } else {
        vec!["instant".into()]
    };
    VelorixRelationCatalogV1::from_relation_schema(
        VelorixRelationSchemaV1 {
            relation_id: "observations".into(),
            relation_name: "observations".into(),
            relation_version: "v1".into(),
            columns: specs
                .into_iter()
                .enumerate()
                .map(
                    |(ordinal, (name, logical_type, physical_arrow_type))| RelationColumnV1 {
                        column_id: name.into(),
                        name: name.into(),
                        logical_type,
                        physical_arrow_type,
                        nullable: false,
                        ordinal: ordinal as u32,
                        semantic_role: if name == "weight" {
                            RelationSemanticRoleV1::Weight
                        } else if keys.iter().any(|id| id == name) {
                            RelationSemanticRoleV1::PrimaryKey
                        } else {
                            RelationSemanticRoleV1::Value
                        },
                    },
                )
                .collect(),
            primary_key_column_ids: keys,
            weight_column_id: "weight".into(),
            allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
            event_time_column_id: None,
        },
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap()
}

fn identity(sql: &str) -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "tenant".into(),
        program_id: "ordered-identities".into(),
        view_ids: vec!["result".into()],
        sql_hash: stable_bytes_hash(sql.as_bytes()),
        input_catalog_hash: stable_bytes_hash(b"inputs"),
        output_schema_hash: stable_bytes_hash(b"outputs"),
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

fn runtime(
    catalog: &VelorixRelationCatalogV1,
    sql: &str,
) -> Box<dyn StandingProgramRuntime + Send> {
    let plan = validate_supported_ordered_window_sql(sql, catalog).unwrap();
    assert_eq!(
        plan.schema_version,
        if catalog.relation_schema.primary_key_column_ids.is_empty() {
            3
        } else if catalog.relation_schema.primary_key_column_ids == ["entity"]
            && !catalog.relation_schema.columns[0].nullable
        {
            1
        } else {
            2
        }
    );
    let output = RelationSchema {
        relation_id: "result".into(),
        relation_name: "result".into(),
        relation_version: "v1".into(),
        schema_fingerprint: stable_bytes_hash(b"result"),
        columns: supported_ordered_window_output_columns(sql, catalog, &plan).unwrap(),
        primary_key: supported_ordered_window_output_primary_key(catalog, &plan).unwrap(),
    };
    create_standing_runtime_with_sql_and_catalogs(
        &identity(sql),
        std::slice::from_ref(catalog),
        sql,
        &[catalog_input_relation_schema(catalog).unwrap()],
        &[output],
    )
    .unwrap()
}

fn input(
    catalog: &VelorixRelationCatalogV1,
    offset: u64,
    rows: &[(&str, i64, i64, f64, i64)],
) -> RelationInputBatch {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.3).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.4).collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_from_iter(
        arrays
            .into_iter()
            .zip(["entity", "instant", "position", "amount", "weight"])
            .map(|(array, name)| (name, array)),
    )
    .unwrap();
    RelationInputBatch {
        relation_id: "observations".into(),
        relation_version: "v1".into(),
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        stream_id: "rows".into(),
        partition_id: 0,
        start_offset_inclusive: offset,
        end_offset_exclusive: offset + rows.len() as u64,
        event_time_watermark: None,
        encoding: RelationInputEncodingV1::SourceRelationV1,
        batches: vec![batch],
    }
}

fn apply(
    runtime: &mut dyn StandingProgramRuntime,
    catalog: &VelorixRelationCatalogV1,
    epoch: u64,
    offset: u64,
    rows: &[(&str, i64, i64, f64, i64)],
) -> EpochCommit {
    let commit = runtime
        .apply_changes(
            epoch,
            EpochIdempotencyKey::new(format!("epoch-{epoch}")).unwrap(),
            vec![input(catalog, offset, rows)],
        )
        .unwrap();
    assert_persisted_output(runtime, &commit);
    commit
}

fn assert_persisted_output(runtime: &dyn StandingProgramRuntime, commit: &EpochCommit) {
    use velorix_core::{
        delta::DeltaBatch,
        view_plan::{supported_view_plan_aggregate_outputs, SupportedViewPlan},
    };
    use velorix_runtime::materialized_view_runtime::materialized_delta_to_page_with_checkpoint;
    let checkpoint = runtime.checkpoint().unwrap();
    let payload: serde_json::Value =
        serde_json::from_str(&checkpoint.state_payload.as_ref().unwrap().payload).unwrap();
    let output: DeltaBatch = serde_json::from_value(payload["published_output"].clone()).unwrap();
    let aggregates = if payload["runtime_kind"] == "single_key_sum_count" {
        let plan: SupportedViewPlan = serde_json::from_value(payload["plan"].clone()).unwrap();
        Some(supported_view_plan_aggregate_outputs(&plan))
    } else {
        None
    };
    let schema = &runtime.output_schemas()[0];
    let view = ScopedViewId {
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        view_id: "result".into(),
    };
    let full = materialized_delta_to_page_with_checkpoint(
        schema,
        &output,
        view.clone(),
        checkpoint.logical_epoch,
        SnapshotPageRequest::default(),
        aggregates.as_deref(),
        &checkpoint,
    )
    .unwrap();
    assert_eq!(full.batches, commit.output_batches[0].batches);
    let mut token = None;
    let mut batches = Vec::new();
    for _ in 0..=full.batches[0].num_rows() {
        let page = materialized_delta_to_page_with_checkpoint(
            schema,
            &output,
            view.clone(),
            checkpoint.logical_epoch,
            SnapshotPageRequest {
                max_rows: Some(1),
                page_token: token,
                committed_epoch: Some(checkpoint.logical_epoch),
            },
            aggregates.as_deref(),
            &checkpoint,
        )
        .unwrap();
        batches.extend(page.batches);
        token = page.next_page_token;
        if token.is_none() {
            break;
        }
    }
    assert!(token.is_none());
    let joined = arrow::compute::concat_batches(&full.batches[0].schema(), &batches).unwrap();
    assert_eq!(joined, full.batches[0]);
}

#[test]
fn timestamp_identity_preserves_type_and_late_retraction_after_restart() {
    let catalog = catalog(false);
    let sql = "SELECT amount, instant AS observed_at, LAG(amount, 1) OVER (ORDER BY instant) AS previous FROM observations";
    let mut rt = runtime(&catalog, sql);
    apply(
        rt.as_mut(),
        &catalog,
        1,
        0,
        &[("a", 30, 0, 3.0, 1), ("a", 10, 0, 1.0, 1)],
    );
    rt = restore_standing_runtime(rt.checkpoint().unwrap()).unwrap();
    let commit = apply(rt.as_mut(), &catalog, 2, 2, &[("a", 20, 0, 2.0, 1)]);
    let batch = &commit.output_batches[0].batches[0];
    let times = batch
        .column_by_name("observed_at")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let previous = batch
        .column_by_name("previous")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let index = (0..times.len()).find(|&i| times.value(i) == 30).unwrap();
    assert_eq!(previous.value(index), 2.0);
    let commit = apply(rt.as_mut(), &catalog, 3, 3, &[("a", 20, 0, 2.0, -1)]);
    assert_eq!(commit.output_batches[0].batches[0].num_rows(), 2);
    rt = restore_standing_runtime(rt.checkpoint().unwrap()).unwrap();
    assert!(rt
        .apply_changes(3, EpochIdempotencyKey::new("epoch-3").unwrap(), vec![])
        .unwrap()
        .output_deltas
        .is_empty());
}

#[test]
fn composite_identity_uses_all_components_for_ties_and_atomic_corrections() {
    let catalog = catalog(true);
    let sql = "SELECT instant AS observed_at, entity AS subject, amount, LAG(amount, 1) OVER (ORDER BY position) AS previous FROM observations";
    let plan = validate_supported_ordered_window_sql(sql, &catalog).unwrap();
    assert_eq!(
        supported_ordered_window_output_primary_key(&catalog, &plan).unwrap(),
        ["subject", "observed_at"]
    );
    let mut rt = runtime(&catalog, sql);
    let commit = apply(
        rt.as_mut(),
        &catalog,
        1,
        0,
        &[
            ("a", 10, 0, 10.0, 1),
            ("a", 2, 0, 2.0, 1),
            ("b", 2, 0, 20.0, 1),
        ],
    );
    let batch = &commit.output_batches[0].batches[0];
    assert_eq!(batch.num_rows(), 3);
    let subjects = batch
        .column_by_name("subject")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let times = batch
        .column_by_name("observed_at")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let previous = batch
        .column_by_name("previous")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for i in 0..batch.num_rows() {
        match (subjects.value(i), times.value(i)) {
            ("a", 2) => assert!(previous.is_null(i)),
            ("a", 10) => assert_eq!(previous.value(i), 2.0),
            ("b", 2) => assert_eq!(previous.value(i), 10.0),
            _ => panic!("unexpected identity"),
        }
    }
    let checkpoint = rt.checkpoint().unwrap();
    assert!(rt
        .apply_changes(
            2,
            EpochIdempotencyKey::new("duplicate").unwrap(),
            vec![input(&catalog, 3, &[("a", 2, 0, 2.0, 1)])]
        )
        .is_err());
    assert!(rt
        .apply_changes(
            2,
            EpochIdempotencyKey::new("bad").unwrap(),
            vec![input(&catalog, 3, &[("a", 2, 0, 999.0, -1)])]
        )
        .is_err());
    assert_eq!(
        checkpoint.state_root.content_hash,
        rt.checkpoint().unwrap().state_root.content_hash
    );
    rt = restore_standing_runtime(checkpoint).unwrap();
    apply(
        rt.as_mut(),
        &catalog,
        2,
        3,
        &[("a", 2, 0, 2.0, -1), ("a", 2, 0, 4.0, 1)],
    );
    restore_standing_runtime(rt.checkpoint().unwrap()).unwrap();
    assert!(validate_supported_ordered_window_sql(
        "SELECT entity, LAG(amount, 1) OVER (ORDER BY position) AS previous FROM observations",
        &catalog
    )
    .is_err());
}

fn bag_catalog() -> VelorixRelationCatalogV1 {
    let mut schema = catalog(false).relation_schema;
    schema.primary_key_column_ids.clear();
    for column in &mut schema.columns {
        if column.semantic_role == RelationSemanticRoleV1::PrimaryKey {
            column.semantic_role = RelationSemanticRoleV1::Value;
        }
    }
    VelorixRelationCatalogV1::from_relation_schema(
        schema,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap()
}

#[test]
fn bag_lag_preserves_duplicate_occurrences_and_stable_retractions() {
    let catalog = bag_catalog();
    let sql = "SELECT position, amount, LAG(amount, 1) OVER (ORDER BY position) AS previous FROM observations";
    let mut rt = runtime(&catalog, sql);
    assert!(rt.output_schemas()[0].primary_key.is_empty());
    let commit = apply(
        rt.as_mut(),
        &catalog,
        1,
        0,
        &[("x", 1, 1, 4.0, 3), ("x", 2, 2, 8.0, 1)],
    );
    let batch = &commit.output_batches[0].batches[0];
    assert_eq!(batch.num_rows(), 4);
    let previous = batch
        .column_by_name("previous")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(previous.null_count(), 1);
    assert_eq!(
        (0..previous.len())
            .filter(|&i| !previous.is_null(i) && previous.value(i) == 4.0)
            .count(),
        3
    );
    let before = rt.checkpoint().unwrap();
    assert!(rt
        .apply_changes(
            2,
            EpochIdempotencyKey::new("underflow").unwrap(),
            vec![input(&catalog, 2, &[("x", 1, 1, 4.0, -4)])]
        )
        .is_err());
    assert_eq!(
        before.state_root.content_hash,
        rt.checkpoint().unwrap().state_root.content_hash
    );
    assert!(rt
        .apply_changes(
            2,
            EpochIdempotencyKey::new("expansion").unwrap(),
            vec![input(&catalog, 2, &[("x", 1, 1, 4.0, i64::MAX - 3)])]
        )
        .is_err());
    assert_eq!(
        before.state_root.content_hash,
        rt.checkpoint().unwrap().state_root.content_hash
    );
    let before_hash = before.state_root.content_hash.clone();
    rt = restore_standing_runtime(before).unwrap();
    assert_eq!(
        before_hash,
        rt.checkpoint().unwrap().state_root.content_hash
    );
    let commit = apply(rt.as_mut(), &catalog, 2, 2, &[("x", 1, 1, 4.0, -1)]);
    assert_eq!(commit.output_batches[0].batches[0].num_rows(), 3);
    rt = restore_standing_runtime(rt.checkpoint().unwrap()).unwrap();
    let commit = apply(rt.as_mut(), &catalog, 3, 3, &[("x", 1, 1, 4.0, -2)]);
    let batch = &commit.output_batches[0].batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert!(batch.column_by_name("previous").unwrap().is_null(0));
    assert!(rt
        .apply_changes(3, EpochIdempotencyKey::new("epoch-3").unwrap(), vec![])
        .unwrap()
        .output_deltas
        .is_empty());
}

#[test]
fn bag_rows_cumulative_and_range_peers_keep_multiplicity() {
    let catalog = bag_catalog();
    for (frame, expected) in [
        ("ROWS", vec![4.0, 8.0, 16.0]),
        ("RANGE", vec![8.0, 8.0, 16.0]),
    ] {
        let sql = format!("SELECT SUM(amount) OVER (ORDER BY position {frame} BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM observations");
        let mut rt = runtime(&catalog, &sql);
        let commit = apply(
            rt.as_mut(),
            &catalog,
            1,
            0,
            &[("x", 1, 1, 4.0, 2), ("x", 2, 2, 8.0, 1)],
        );
        let batch = &commit.output_batches[0].batches[0];
        assert_eq!(batch.num_columns(), 1);
        let running = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut actual = (0..running.len())
            .map(|i| running.value(i))
            .collect::<Vec<_>>();
        actual.sort_by(f64::total_cmp);
        assert_eq!(actual, expected);
        rt = restore_standing_runtime(rt.checkpoint().unwrap()).unwrap();
        let commit = apply(rt.as_mut(), &catalog, 2, 2, &[("x", 0, 0, 1.0, 1)]);
        assert_eq!(commit.output_batches[0].batches[0].num_rows(), 4);
    }
}

#[test]
fn lag_respects_nulls_and_rejects_null_treatment_modifiers() {
    let mut schema = bag_catalog().relation_schema;
    schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "amount")
        .unwrap()
        .nullable = true;
    let catalog = VelorixRelationCatalogV1::from_relation_schema(
        schema,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap();
    let sql =
        "SELECT position, LAG(amount, 1) OVER (ORDER BY position) AS previous FROM observations";
    let unsupported = sql.replace("LAG(amount, 1)", "LAG(amount, 1) IGNORE NULLS");
    let error = validate_supported_ordered_window_sql(&unsupported, &catalog).unwrap_err();
    assert!(error.to_string().contains("NULL treatment"), "{error}");
    let mut rt = runtime(&catalog, sql);
    let mut source = input(
        &catalog,
        0,
        &[
            ("x", 1, 1, 5.0, 1),
            ("x", 2, 2, 0.0, 1),
            ("x", 3, 3, 9.0, 1),
        ],
    );
    let batch = &source.batches[0];
    let fields = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| field.as_ref().clone().with_nullable(index == 3))
        .collect::<Vec<_>>();
    let mut arrays = batch.columns().to_vec();
    arrays[3] = Arc::new(Float64Array::from(vec![Some(5.0), None, Some(9.0)]));
    source.batches =
        vec![
            RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), arrays).unwrap(),
        ];
    let commit = rt
        .apply_changes(
            1,
            EpochIdempotencyKey::new("nullable").unwrap(),
            vec![source],
        )
        .unwrap();
    let batch = &commit.output_batches[0].batches[0];
    let positions = batch
        .column_by_name("position")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let previous = batch
        .column_by_name("previous")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for i in 0..batch.num_rows() {
        match positions.value(i) {
            1 | 3 => assert!(previous.is_null(i)),
            2 => assert_eq!(previous.value(i), 5.0),
            _ => panic!("unexpected position"),
        }
    }
}

#[test]
fn rehashed_ordered_nodes_cannot_disagree_with_execution() {
    use velorix_core::view_plan::{
        logical_view_plan_hash, lower_supported_ordered_window_sql_to_logical_plan,
        validate_logical_view_plan, OUTPUT_PUBLICATION_PROTOCOL_VERSION_V1,
    };
    use velorix_runtime::materialized_view_runtime::create_standing_runtime_with_logical_plan_and_catalogs;
    let catalog = bag_catalog();
    let sql = "SELECT position, LAG(amount, 1) OVER (PARTITION BY entity ORDER BY position) AS previous FROM observations";
    let output = runtime(&catalog, sql).output_schemas().remove(0);
    let original =
        lower_supported_ordered_window_sql_to_logical_plan(sql, &catalog, &output).unwrap();
    for altered_sql in [
        sql.replace("ORDER BY position", "ORDER BY position DESC"),
        sql.replace("PARTITION BY entity", "PARTITION BY instant"),
    ] {
        let mut changed =
            lower_supported_ordered_window_sql_to_logical_plan(&altered_sql, &catalog, &output)
                .unwrap();
        changed.view_sql = sql.into();
        changed.execution = original.execution.clone();
        let physical_bytes = serde_json::to_vec(&(
            &changed.nodes,
            &changed.operator_dag_contract,
            &changed.state_requirements,
            &changed.output_codec_version,
            &changed.execution,
            OUTPUT_PUBLICATION_PROTOCOL_VERSION_V1,
        ))
        .unwrap();
        changed
            .execution_implementation
            .as_mut()
            .unwrap()
            .physical_operator_dag_hash = format!(
            "velorix-physical-operator-dag-sha256-v1:{}",
            stable_bytes_hash(&physical_bytes)
        );
        changed.plan_hash = Some(logical_view_plan_hash(&changed).unwrap());
        // Prove rejection comes from SQL/node binding, not a stale hash or malformed DAG.
        validate_logical_view_plan(&changed).unwrap();
        assert!(create_standing_runtime_with_logical_plan_and_catalogs(
            &identity(sql),
            std::slice::from_ref(&catalog),
            changed,
            &[catalog_input_relation_schema(&catalog).unwrap()],
            std::slice::from_ref(&output)
        )
        .is_err());
    }
}

type NullableOrderRow<'a> = (Option<&'a str>, Option<i64>, i64, f64, i64);

fn nullable_order_input(
    catalog: &VelorixRelationCatalogV1,
    offset: u64,
    rows: &[NullableOrderRow<'_>],
) -> RelationInputBatch {
    let mut result = input(catalog, offset, &[]);
    result.end_offset_exclusive = offset + rows.len() as u64;
    result.batches = vec![RecordBatch::try_from_iter(vec![
        (
            "entity",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "instant",
            Arc::new(TimestampNanosecondArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "position",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "amount",
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "weight",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.4).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .unwrap()];
    // Nullability belongs to the registered schema, not to the values present
    // in this particular insert/retraction batch.
    result.batches[0] = RecordBatch::try_new(
        datafusion_schema_from_catalog(catalog).unwrap(),
        result.batches[0].columns().to_vec(),
    )
    .unwrap();
    validate_record_batch_matches_catalog(catalog, &result.batches[0]).unwrap();
    result
}

#[test]
fn nullable_order_and_nullable_tuple_identity_survive_restart_and_retraction() {
    for bag in [false, true] {
        let mut schema = catalog(true).relation_schema;
        for column in &mut schema.columns {
            if column.name == "entity" || column.name == "instant" {
                column.nullable = true;
            }
            if bag && column.semantic_role == RelationSemanticRoleV1::PrimaryKey {
                column.semantic_role = RelationSemanticRoleV1::Value;
            }
        }
        if bag {
            schema.primary_key_column_ids.clear();
        }
        let catalog = VelorixRelationCatalogV1::from_relation_schema(
            schema,
            CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
        )
        .unwrap();
        for (ordering, order) in [
            ("", vec![2usize, 3, 0, 1]),
            ("ASC", vec![2usize, 3, 0, 1]),
            ("ASC NULLS LAST", vec![2, 3, 0, 1]),
            ("ASC NULLS FIRST", vec![0, 1, 2, 3]),
            ("DESC", vec![0, 1, 3, 2]),
            ("DESC NULLS FIRST", vec![0, 1, 3, 2]),
            ("DESC NULLS LAST", vec![3, 2, 0, 1]),
        ] {
            let sql = format!("SELECT entity, instant, position, LAG(amount) OVER w AS previous, SUM(amount) OVER (ORDER BY instant {ordering} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rows, SUM(amount) OVER w AS running_range FROM observations WINDOW w AS (ORDER BY instant {ordering})");
            let mut rt = runtime(&catalog, &sql);
            assert!(rt.output_schemas()[0]
                .columns
                .iter()
                .filter(|c| c.name == "entity" || c.name == "instant")
                .all(|c| c.nullable));
            assert_eq!(rt.output_schemas()[0].primary_key.is_empty(), bag);
            let source = [
                (Some("a"), None, 0, 1.0, 1),
                (Some("b"), None, 1, 2.0, 1),
                (None, Some(1), 2, 4.0, 1),
                (Some("d"), Some(2), 3, 8.0, 1),
            ];
            let commit = rt
                .apply_changes(
                    1,
                    EpochIdempotencyKey::new("nullable-1").unwrap(),
                    vec![nullable_order_input(&catalog, 0, &source)],
                )
                .unwrap();
            let check = |commit: &EpochCommit, order: &[usize]| {
                let batch = &commit.output_batches[0].batches[0];
                let positions = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let previous = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let rows = batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let range = batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(batch.num_rows(), order.len());
                let mut sum = 0.0;
                for (i, &id) in order.iter().enumerate() {
                    sum += source[id].3;
                    let row = (0..positions.len())
                        .find(|&row| positions.value(row) == id as i64)
                        .unwrap();
                    if i == 0 {
                        assert!(previous.is_null(row));
                    } else {
                        assert_eq!(previous.value(row), source[order[i - 1]].3);
                    }
                    assert_eq!(rows.value(row), sum, "{ordering}, bag={bag}");
                    let peer_end = order
                        .iter()
                        .enumerate()
                        .skip(i + 1)
                        .take_while(|(_, next)| source[**next].1 == source[id].1)
                        .map(|(_, next)| source[*next].3)
                        .sum::<f64>();
                    assert_eq!(range.value(row), sum + peer_end, "{ordering}, bag={bag}");
                }
            };
            check(&commit, &order);
            assert_persisted_output(rt.as_ref(), &commit);
            let checkpoint = rt.checkpoint().unwrap();
            let mut tampered = checkpoint.clone();
            let payload = tampered.state_payload.as_mut().unwrap();
            let mut json: serde_json::Value = serde_json::from_str(&payload.payload).unwrap();
            let nulls_first = json["plan"]["order_nulls_first"].as_bool().unwrap();
            json["plan"]["order_nulls_first"] = serde_json::json!(!nulls_first);
            payload.payload = serde_json::to_string(&json).unwrap();
            tampered.state_root.content_hash = stable_bytes_hash(payload.payload.as_bytes());
            assert!(restore_standing_runtime(tampered).is_err());
            rt = restore_standing_runtime(checkpoint.clone()).unwrap();
            assert_eq!(
                checkpoint.state_root.content_hash,
                rt.checkpoint().unwrap().state_root.content_hash
            );
            if !bag {
                let before = rt.checkpoint().unwrap();
                assert!(rt
                    .apply_changes(
                        2,
                        EpochIdempotencyKey::new("duplicate-null-key").unwrap(),
                        vec![nullable_order_input(&catalog, 4, &[source[0]])]
                    )
                    .is_err());
                assert_eq!(
                    before.state_root.content_hash,
                    rt.checkpoint().unwrap().state_root.content_hash
                );
            }
            let mut retract = source[0];
            retract.4 = -1;
            let commit = rt
                .apply_changes(
                    2,
                    EpochIdempotencyKey::new("nullable-2").unwrap(),
                    vec![nullable_order_input(&catalog, 4, &[retract])],
                )
                .unwrap();
            check(
                &commit,
                &order.into_iter().filter(|id| *id != 0).collect::<Vec<_>>(),
            );
            assert_persisted_output(rt.as_ref(), &commit);
        }
    }
}

#[test]
fn persisted_legacy_ordered_key_is_reordered_to_select_position() {
    let mut schema = catalog(false).relation_schema;
    schema.primary_key_column_ids = vec!["entity".into()];
    schema.columns[0].semantic_role = RelationSemanticRoleV1::PrimaryKey;
    schema.columns[1].semantic_role = RelationSemanticRoleV1::Value;
    let catalog = VelorixRelationCatalogV1::from_relation_schema(
        schema,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap();
    let sql = "SELECT position, entity, LAG(amount) OVER (ORDER BY instant) AS previous FROM observations";
    let mut rt = runtime(&catalog, sql);
    apply(
        rt.as_mut(),
        &catalog,
        1,
        0,
        &[("a", 1, 7, 4.0, 1), ("b", 2, 9, 8.0, 1)],
    );
    rt = restore_standing_runtime(rt.checkpoint().unwrap()).unwrap();
    apply(rt.as_mut(), &catalog, 2, 2, &[("a", 1, 7, 4.0, -1)]);
}

#[test]
fn persisted_global_and_three_key_aggregates_do_not_use_window_decoder() {
    use velorix_core::view_contract::{ColumnSchema, SqlDataType};
    let catalog = bag_catalog();
    for grouped in [false, true] {
        let sql = if grouped {
            "SELECT entity, instant, position, COUNT(*) AS total FROM observations GROUP BY entity, instant, position"
        } else {
            "SELECT COUNT(*) AS total FROM observations"
        };
        let mut columns = if grouped {
            vec![
                ColumnSchema {
                    name: "entity".into(),
                    data_type: SqlDataType::Utf8,
                    nullable: false,
                },
                ColumnSchema {
                    name: "instant".into(),
                    data_type: SqlDataType::Timestamp { timezone: None },
                    nullable: false,
                },
                ColumnSchema {
                    name: "position".into(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                },
            ]
        } else {
            Vec::new()
        };
        columns.push(ColumnSchema {
            name: "total".into(),
            data_type: SqlDataType::Int64,
            nullable: false,
        });
        let output = RelationSchema {
            relation_id: "result".into(),
            relation_name: "result".into(),
            relation_version: "v1".into(),
            schema_fingerprint: stable_bytes_hash(b"aggregate-result"),
            columns,
            primary_key: if grouped {
                vec!["entity".into(), "instant".into(), "position".into()]
            } else {
                Vec::new()
            },
        };
        let mut rt = create_standing_runtime_with_sql_and_catalogs(
            &identity(sql),
            std::slice::from_ref(&catalog),
            sql,
            &[catalog_input_relation_schema(&catalog).unwrap()],
            &[output],
        )
        .unwrap();
        apply(
            rt.as_mut(),
            &catalog,
            1,
            0,
            &[("a", 1, 7, 4.0, 2), ("b", 2, 9, 8.0, 1)],
        );
        rt = restore_standing_runtime(rt.checkpoint().unwrap()).unwrap();
        apply(
            rt.as_mut(),
            &catalog,
            2,
            2,
            &[("a", 1, 7, 4.0, -2), ("b", 2, 9, 8.0, -1)],
        );
    }
}
