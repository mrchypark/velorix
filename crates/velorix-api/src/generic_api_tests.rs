use super::*;
use crate::example_fixtures::default_scores_relation_catalog;
use crate::example_fixtures::public_relation_registration;
use velorix_core::delta::{DeltaKey, DeltaRecord, DeltaValue};

#[tokio::test]
async fn global_aggregates_publish_empty_and_append_results() {
    use tower::ServiceExt;
    use velorix_core::relation::*;
    async fn post(router: &Router, path: &str, body: Value, status: StatusCode) -> Value {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::ACCEPT, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let actual = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(actual, status, "{path}: {value}");
        value
    }
    let state = tests::with_initialized_active_view_meta_store(
        tests::test_api_state_with_store(
            Arc::new(object_store::memory::InMemory::new()),
            "global-empty-owner",
            false,
        )
        .await,
        Arc::new(velorix_control::meta_admin::InMemoryMetaStore::default()),
    )
    .await;
    let router = app(state);
    let catalog = VelorixRelationCatalogV1::from_relation_schema(
        VelorixRelationSchemaV1 {
            relation_id: "samples".into(),
            relation_name: "samples".into(),
            relation_version: "v1".into(),
            columns: vec![
                RelationColumnV1 {
                    column_id: "reading".into(),
                    name: "reading".into(),
                    logical_type: VelorixLogicalTypeV1::Float64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Float64,
                    nullable: false,
                    ordinal: 0,
                    semantic_role: RelationSemanticRoleV1::Value,
                },
                RelationColumnV1 {
                    column_id: "delta".into(),
                    name: "delta".into(),
                    logical_type: VelorixLogicalTypeV1::Int64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                    nullable: false,
                    ordinal: 1,
                    semantic_role: RelationSemanticRoleV1::Weight,
                },
            ],
            primary_key_column_ids: Vec::new(),
            weight_column_id: "delta".into(),
            allowed_operations: vec![RelationOperationV1::Insert],
            event_time_column_id: None,
        },
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap();
    post(
        &router,
        "/v1/relations",
        public_relation_registration(&catalog),
        StatusCode::CREATED,
    )
    .await;
    for (view, aggregate) in [("sample_sum", "SUM(reading)"), ("sample_count", "COUNT(*)")] {
        post(&router, "/v1/views", json!({"view_id":view,"input_relation_id":"samples",
            "input_relation_version":"v1","sql":format!("SELECT {aggregate} AS result FROM samples"),
            "response_formats":["json"]}), StatusCode::CREATED).await;
        // Declaring the consumer publishes a validated empty source checkpoint.
        // Queries below read that materialization, not a source recomputation.
        post(&router, "/v1/views", json!({"view_id":format!("{view}_consumer"),"input_relation_id":view,
            "input_relation_version":"v1","sql":format!("SELECT * FROM {view} WHERE result IS NOT NULL"),
            "response_formats":["json"]}), StatusCode::CREATED).await;
        // Re-enter producer bootstrap with a published checkpoint: activation
        // reconciliation must be idempotent and must not apply another epoch.
        let before = post(
            &router,
            &format!("/v1/views/{view}/query"),
            json!({}),
            StatusCode::OK,
        )
        .await;
        post(&router, "/v1/views", json!({"view_id":format!("{view}_second_consumer"),"input_relation_id":view,
            "input_relation_version":"v1","sql":format!("SELECT * FROM {view} WHERE result IS NOT NULL"),
            "response_formats":["json"]}), StatusCode::CREATED).await;
        let after = post(
            &router,
            &format!("/v1/views/{view}/query"),
            json!({}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(before["rows"], after["rows"]);
    }
    for (offset, expected_sum, expected_count) in [(None, Value::Null, 0), (Some(0), json!(2.5), 1)]
    {
        if let Some(offset) = offset {
            post(
                &router,
                "/v1/relations/samples/ingest",
                json!({"relation_version":"v1",
                "stream_id":"samples-stream","partition_id":0,"start_offset_inclusive":offset,
                "rows":[{"reading":2.5}]}),
                StatusCode::CREATED,
            )
            .await;
        }
        let sum = post(
            &router,
            "/v1/views/sample_sum/query",
            json!({}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(sum["rows"], json!([{"result":expected_sum}]));
        let count = post(
            &router,
            "/v1/views/sample_count/query",
            json!({}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(count["rows"], json!([{"result":expected_count}]));
    }
}

#[test]
fn global_aggregate_inference_accepts_keyless_nonnumeric_count_and_float_sum() {
    for (relation, columns, sql, expected_type, expected_nullable) in [
        (
            "signal_labels",
            vec![
                ColumnSchema {
                    name: "observed_at".into(),
                    data_type: SqlDataType::Timestamp { timezone: None },
                    nullable: true,
                },
                ColumnSchema {
                    name: "label".into(),
                    data_type: SqlDataType::Utf8,
                    nullable: true,
                },
            ],
            "SELECT COUNT(*) AS result FROM signal_labels",
            SqlDataType::Int64,
            false,
        ),
        (
            "signal_values",
            vec![ColumnSchema {
                name: "reading".into(),
                data_type: SqlDataType::Float64,
                nullable: false,
            }],
            "SELECT SUM(reading) AS result FROM signal_values",
            SqlDataType::Float64,
            true,
        ),
    ] {
        let source = bag_stage_output_schema(relation, columns).unwrap();
        let binding = published_relation_binding_v1(
            relation,
            1,
            &stable_bytes_hash(relation.as_bytes()),
            &source,
        )
        .unwrap();
        let catalog = catalog_from_published_relation_binding(&binding).unwrap();
        assert!(catalog.relation_schema.primary_key_column_ids.is_empty());
        // Keep the core admission error visible instead of the API's final
        // unsupported-family fallback when this regression fails.
        let plan = validate_catalog_backed_sum_count_view_sql(sql, &catalog)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        let direct = aggregate_output_schema("signal_summary", &catalog, &plan).unwrap();
        let inferred = MaterializedViewRuntimeFactory
            .output_schemas_for_view_request_with_catalogs(
                "signal_summary",
                sql,
                std::slice::from_ref(&catalog),
                catalog.schema_fingerprint.as_str(),
            )
            .unwrap()
            .expect("aggregate API inference");
        assert_eq!(inferred, vec![direct]);
        assert!(inferred[0].primary_key.is_empty());
        assert_eq!(inferred[0].columns.len(), 1);
        assert_eq!(inferred[0].columns[0].name, "result");
        assert_eq!(inferred[0].columns[0].data_type, expected_type);
        assert_eq!(inferred[0].columns[0].nullable, expected_nullable);
        lower_materialized_view_runtime_sql_to_logical_plan(
            sql,
            std::slice::from_ref(&catalog),
            &inferred[0],
        )
        .unwrap();
    }
}

#[tokio::test]
async fn typed_join_chain_preserves_nonunique_values_and_null_key_semantics() {
    use tower::ServiceExt;
    use velorix_core::relation::*;

    async fn post(router: &Router, path: &str, body: Value, status: StatusCode) -> Value {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::ACCEPT, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let actual_status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(actual_status, status, "{path}: {value}");
        value
    }
    let state = tests::with_initialized_active_view_meta_store(
        tests::test_api_state_with_store(
            Arc::new(object_store::memory::InMemory::new()),
            "join-bag-chain",
            false,
        )
        .await,
        Arc::new(velorix_control::meta_admin::InMemoryMetaStore::default()),
    )
    .await;
    let router = app(state);
    for relation in ["grove_left", "grove_right"] {
        let catalog = VelorixRelationCatalogV1::from_relation_schema(
            VelorixRelationSchemaV1 {
                relation_id: relation.into(),
                relation_name: relation.into(),
                relation_version: "v1".into(),
                columns: ["seed", "reading", "delta"]
                    .into_iter()
                    .enumerate()
                    .map(|(ordinal, name)| RelationColumnV1 {
                        column_id: name.into(),
                        name: name.into(),
                        logical_type: VelorixLogicalTypeV1::Int64,
                        physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                        nullable: name != "delta",
                        ordinal: ordinal as u32,
                        semantic_role: if name == "delta" {
                            RelationSemanticRoleV1::Weight
                        } else {
                            RelationSemanticRoleV1::Value
                        },
                    })
                    .collect(),
                primary_key_column_ids: Vec::new(),
                weight_column_id: "delta".into(),
                allowed_operations: vec![RelationOperationV1::Insert],
                event_time_column_id: None,
            },
            CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
        )
        .unwrap();
        post(
            &router,
            "/v1/relations",
            public_relation_registration(&catalog),
            StatusCode::CREATED,
        )
        .await;
    }
    post(
        &router,
        "/v1/views",
        json!({"view_id":"grove_join", "input_relation_refs":[
        {"relation_id":"grove_left","relation_version":"v1"},
        {"relation_id":"grove_right","relation_version":"v1"}],
        "sql":"SELECT l.reading AS reading FROM grove_left l JOIN grove_right r ON l.seed = r.seed",
        "response_formats":["json"]}),
        StatusCode::CREATED,
    )
    .await;
    post(
        &router,
        "/v1/views",
        json!({"view_id":"grove_filtered", "input_relation_id":"grove_join",
        "input_relation_version":"v1", "sql":"SELECT * FROM grove_join WHERE reading IS NOT NULL",
        "response_formats":["json"]}),
        StatusCode::CREATED,
    )
    .await;
    post(&router, "/v1/views", json!({"view_id":"grove_outer", "input_relation_refs":[
        {"relation_id":"grove_left","relation_version":"v1"},
        {"relation_id":"grove_right","relation_version":"v1"}],
        "sql":"SELECT l.reading AS left_reading, r.reading AS right_reading FROM grove_left l FULL OUTER JOIN grove_right r ON l.seed = r.seed",
        "response_formats":["json"]}), StatusCode::CREATED).await;
    post(&router, "/v1/views", json!({"view_id":"grove_outer_consumer", "input_relation_id":"grove_outer",
        "input_relation_version":"v1", "sql":"SELECT * FROM grove_outer WHERE left_reading IS NOT NULL",
        "response_formats":["json"]}), StatusCode::CREATED).await;
    for (relation, rows) in [
        (
            "grove_left",
            json!([{"seed":7,"reading":11},{"seed":7,"reading":11},{"seed":7,"reading":null},{"seed":null,"reading":31}]),
        ),
        (
            "grove_right",
            json!([{"seed":7,"reading":21},{"seed":7,"reading":22},{"seed":null,"reading":41}]),
        ),
    ] {
        post(
            &router,
            &format!("/v1/relations/{relation}/ingest"),
            json!({"relation_version":"v1",
            "stream_id":relation,"partition_id":0,"start_offset_inclusive":0,"rows":rows}),
            StatusCode::CREATED,
        )
        .await;
    }
    let joined = post(
        &router,
        "/v1/views/grove_join/query",
        json!({}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(joined["rows"].as_array().unwrap().len(), 6);
    let outer = post(
        &router,
        "/v1/views/grove_outer/query",
        json!({}),
        StatusCode::OK,
    )
    .await;
    let outer_rows = outer["rows"].as_array().unwrap();
    assert_eq!(outer_rows.len(), 8);
    assert!(outer_rows.contains(&json!({"left_reading":31,"right_reading":null})));
    assert!(outer_rows.contains(&json!({"left_reading":null,"right_reading":41})));
    assert!(!outer_rows.contains(&json!({"left_reading":31,"right_reading":41})));
    let filtered = post(
        &router,
        "/v1/views/grove_filtered/query",
        json!({}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        filtered["rows"],
        json!([{"reading":11},{"reading":11},{"reading":11},{"reading":11}])
    );
    post(
        &router,
        "/v1/relations/grove_right/ingest",
        json!({"relation_version":"v1",
        "stream_id":"grove_right","partition_id":0,"start_offset_inclusive":3,
        "rows":[{"seed":7,"reading":23}]}),
        StatusCode::CREATED,
    )
    .await;
    let filtered = post(
        &router,
        "/v1/views/grove_filtered/query",
        json!({}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(
        filtered["rows"],
        json!([{"reading":11},{"reading":11},{"reading":11},{"reading":11},{"reading":11},{"reading":11}])
    );
}

#[tokio::test]
async fn typed_map_chain_preserves_live_bags_and_aggregate_updates() {
    use axum::http::Method;
    use tower::ServiceExt;
    use velorix_core::relation::*;

    async fn post(router: &Router, path: &str, body: Value, status: StatusCode) -> Value {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(path)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::ACCEPT, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let actual_status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(actual_status, status, "{path}: {value}");
        value
    }
    async fn records(router: &Router) -> Vec<i64> {
        let response = post(
            router,
            "/v1/views/orchard_final/query",
            json!({}),
            StatusCode::OK,
        )
        .await;
        let mut values = response["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["reading"].as_i64().unwrap())
            .collect::<Vec<_>>();
        values.sort();
        values
    }
    let store = Arc::new(object_store::memory::InMemory::new());
    let state = tests::with_initialized_active_view_meta_store(
        tests::test_api_state_with_store(store, "bag-chain-a", false).await,
        Arc::new(velorix_control::meta_admin::InMemoryMetaStore::default()),
    )
    .await;
    let router = app(state.clone());
    let catalog = VelorixRelationCatalogV1::from_relation_schema(
        VelorixRelationSchemaV1 {
            relation_id: "orchard_raw".into(),
            relation_name: "orchard_raw".into(),
            relation_version: "v1".into(),
            columns: ["seed", "reading", "delta"]
                .into_iter()
                .enumerate()
                .map(|(ordinal, name)| RelationColumnV1 {
                    column_id: name.into(),
                    name: name.into(),
                    logical_type: VelorixLogicalTypeV1::Int64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                    nullable: name != "delta",
                    ordinal: ordinal as u32,
                    semantic_role: if name == "delta" {
                        RelationSemanticRoleV1::Weight
                    } else {
                        RelationSemanticRoleV1::Value
                    },
                })
                .collect(),
            primary_key_column_ids: Vec::new(),
            weight_column_id: "delta".into(),
            allowed_operations: vec![RelationOperationV1::Insert],
            event_time_column_id: None,
        },
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap();
    post(
        &router,
        "/v1/relations",
        public_relation_registration(&catalog),
        StatusCode::CREATED,
    )
    .await;
    for (view, input, sql) in [
        (
            "orchard_middle",
            "orchard_raw",
            "SELECT * FROM orchard_raw WHERE seed IS NOT NULL",
        ),
        (
            "orchard_final",
            "orchard_middle",
            "SELECT reading FROM orchard_middle WHERE reading > 0",
        ),
        (
            "orchard_groups",
            "orchard_raw",
            "SELECT seed, COUNT(*) AS total FROM orchard_raw GROUP BY seed",
        ),
        (
            "orchard_group_map",
            "orchard_groups",
            "SELECT seed, total FROM orchard_groups",
        ),
    ] {
        post(
            &router,
            "/v1/views",
            json!({"view_id":view,"input_relation_id":input,
            "input_relation_version":"v1","sql":sql,"response_formats":["json"]}),
            StatusCode::CREATED,
        )
        .await;
    }
    assert!(records(&router).await.is_empty());
    let insert = json!({"relation_version":"v1","stream_id":"orchard-stream","partition_id":0,
    "start_offset_inclusive":0,"rows":[
        {"seed":7,"reading":11}, {"seed":7,"reading":11},
        {"seed":7,"reading":19}, {"seed":null,"reading":99}
    ]});
    post(
        &router,
        "/v1/relations/orchard_raw/ingest",
        insert,
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(records(&router).await, [11, 11, 19]);
    let producer = tests::read_test_active_view(&state, "orchard_middle")
        .await
        .unwrap();
    let binding = published_relation_binding_for_active_view(&producer)
        .unwrap()
        .unwrap();
    let identity = active_standing_runtime_identity(&producer).unwrap();
    let captured = capture_producer_head_cursor(
        &state,
        &identity.tenant_id,
        &producer,
        &binding,
        "snapshot-test-edge",
    )
    .await
    .unwrap();
    let (captured_record, _, captured_pages) =
        checkpoint_publication::read_authoritative_cursor_checkpoint_with_snapshot_pages(
            &state,
            &identity.tenant_id,
            &binding,
            &captured,
            true,
        )
        .await
        .unwrap();
    let captured_pages = captured_pages.expect("typed map snapshot pages");
    assert_eq!(captured_pages.schema, binding.relation);
    let restored = standing_runtime_checkpoint_published_batch_with_limits(
        &captured_record.checkpoint,
        state.runtime_resource_limits(),
    )
    .unwrap()
    .unwrap();
    let page_rows = DeltaBatch::from_records(
        captured_pages
            .pages
            .iter()
            .flat_map(|page| page.records().iter().cloned()),
    );
    assert_eq!(page_rows.net_rows().unwrap(), restored.net_rows().unwrap());
    assert_eq!(
        published_delta_to_record_batch(&binding, &page_rows).unwrap(),
        published_delta_to_record_batch(&binding, &restored).unwrap()
    );
    let groups = post(
        &router,
        "/v1/views/orchard_group_map/query",
        json!({}),
        StatusCode::OK,
    )
    .await;
    let rows = groups["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&json!({"seed":null,"total":1})));
    assert!(rows.contains(&json!({"seed":7,"total":3})));
    post(
        &router,
        "/v1/relations/orchard_raw/ingest",
        json!({"relation_version":"v1",
        "stream_id":"orchard-stream","partition_id":0,"start_offset_inclusive":4,
        "rows":[{"seed":7,"reading":11}]}),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(records(&router).await, [11, 11, 11, 19]);
    let groups = post(
        &router,
        "/v1/views/orchard_group_map/query",
        json!({}),
        StatusCode::OK,
    )
    .await;
    let rows = groups["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&json!({"seed":null,"total":1})));
    assert!(rows.contains(&json!({"seed":7,"total":4})));
    // Advancing the authoritative head must not substitute the latest output
    // for the immutable admission-time cursor.
    let (historical, _, historical_pages) =
        checkpoint_publication::read_authoritative_cursor_checkpoint_with_snapshot_pages(
            &state,
            &identity.tenant_id,
            &binding,
            &captured,
            true,
        )
        .await
        .unwrap();
    assert_eq!(historical.checkpoint.logical_epoch, captured.output_epoch);
    let latest = read_latest_standing_runtime_checkpoint(&state, identity, &producer.spec.view_id)
        .await
        .unwrap()
        .unwrap();
    assert!(latest.checkpoint.logical_epoch > captured.output_epoch);
    let historical_rows = DeltaBatch::from_records(
        historical_pages
            .unwrap()
            .pages
            .into_iter()
            .flat_map(|page| page.records().to_vec()),
    );
    assert_eq!(
        historical_rows.net_rows().unwrap(),
        restored.net_rows().unwrap()
    );
    let manifest_ref = historical
        .checkpoint
        .output_manifest_refs
        .iter()
        .find(|reference| reference.starts_with(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX))
        .unwrap();
    let (_, manifest) =
        read_standing_runtime_output_manifest_record(&state, manifest_ref, &producer.spec.view_id)
            .await
            .unwrap();
    state
        .store
        .put(
            &ObjectPath::from(manifest.pages[0].page_key.clone()),
            bytes::Bytes::from_static(b"tampered captured page").into(),
        )
        .await
        .unwrap();
    assert!(
        checkpoint_publication::read_authoritative_cursor_checkpoint_with_snapshot_pages(
            &state,
            &identity.tenant_id,
            &binding,
            &captured,
            true,
        )
        .await
        .is_err()
    );
}

#[test]
fn nullable_aggregate_group_projection_infers_typed_map_without_false_nullability() {
    let source = bag_stage_output_schema(
        "grove_samples",
        vec![
            ColumnSchema {
                name: "seed".into(),
                data_type: SqlDataType::Int64,
                nullable: true,
            },
            ColumnSchema {
                name: "reading".into(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
    )
    .unwrap();
    let binding = published_relation_binding_v1(
        "grove_samples",
        1,
        &stable_bytes_hash(b"grove-samples"),
        &source,
    )
    .unwrap();
    let source_catalog = catalog_from_published_relation_binding(&binding).unwrap();
    let factory = MaterializedViewRuntimeFactory;
    let grouped = factory
        .output_schemas_for_view_request(
            "grove_groups",
            "SELECT seed, COUNT(*) AS total FROM grove_samples GROUP BY seed",
            &source_catalog,
            source_catalog.schema_fingerprint.as_str(),
        )
        .unwrap()
        .unwrap()
        .remove(0);
    assert_eq!(grouped.primary_key, ["seed"]);
    assert!(grouped.columns[0].nullable);
    let binding = published_relation_binding_v1(
        "grove_groups",
        1,
        &stable_bytes_hash(b"grove-groups"),
        &grouped,
    )
    .unwrap();
    let catalog = catalog_from_published_relation_binding(&binding).unwrap();
    let sql = "SELECT seed, total FROM grove_groups";
    let output = factory
        .output_schemas_for_view_request(
            "grove_projection",
            sql,
            &catalog,
            catalog.schema_fingerprint.as_str(),
        )
        .unwrap()
        .unwrap()
        .remove(0);
    assert!(output.primary_key.is_empty());
    assert!(output.columns[0].nullable);
    assert!(!output.columns[1].nullable);
    let plan = lower_materialized_view_runtime_sql_to_logical_plan(
        sql,
        std::slice::from_ref(&catalog),
        &output,
    )
    .unwrap();
    assert!(matches!(
        plan.execution,
        VelorixLogicalViewExecutionV1::TypedMap { .. }
    ));
}

#[tokio::test]
async fn runtime_checkpoint_and_work_budgets_are_independent() {
    let state = tests::test_api_state_with_store(
        Arc::new(object_store::memory::InMemory::new()),
        "resource-limits",
        false,
    )
    .await
    .with_standing_runtime_budget_limits(123, 64 * 1024 * 1024)
    .with_standing_runtime_work_byte_limits(2 * 1024 * 1024, 3 * 1024 * 1024)
    .unwrap();
    let limits = state.runtime_resource_limits();
    assert_eq!(limits.max_checkpoint_bytes, 64 * 1024 * 1024);
    assert_eq!(limits.max_expansion_bytes, 2 * 1024 * 1024);
    assert_eq!(limits.max_expansion_rows, 123);
    assert_eq!(limits.max_snapshot_bytes, 3 * 1024 * 1024);
    assert_eq!(
        state.max_standing_runtime_state_fragment_bytes,
        4 * 1024 * 1024
    );
    assert!(state
        .clone()
        .with_standing_runtime_state_fragment_bytes(0)
        .is_err());
    assert!(state
        .clone()
        .with_standing_runtime_work_byte_limits(0, 1)
        .is_err());
    assert!(state
        .clone()
        .with_standing_runtime_work_byte_limits(1, 0)
        .is_err());
    assert!(state.with_standing_runtime_work_byte_limits(0, 0).is_err());
}

#[test]
fn generic_expansion_and_union_inference_do_not_claim_unique_output_keys() {
    let columns = vec![ColumnSchema {
        name: "seed".into(),
        data_type: SqlDataType::Int64,
        nullable: true,
    }];
    let primary_key = vec!["seed".into()];
    let source = RelationSchema {
        relation_id: "orchard_seeds".into(),
        relation_name: "orchard_seeds".into(),
        relation_version: "v1".into(),
        schema_fingerprint: materialized_output_schema_fingerprint(
            "orchard_seeds",
            "v1",
            &columns,
            &primary_key,
        )
        .unwrap(),
        columns,
        primary_key,
    };
    let binding = published_relation_binding_v1(
        "orchard_seeds",
        1,
        &stable_bytes_hash(b"synthetic-source"),
        &source,
    )
    .unwrap();
    let catalog = catalog_from_published_relation_binding(&binding).unwrap();
    for (sql, names) in [
        (
            "SELECT o.seed, s.tick FROM orchard_seeds o CROSS JOIN LATERAL generate_series(1, 3, 1) AS s(tick)",
            vec!["seed", "tick"],
        ),
        (
            "SELECT seed AS sprout FROM orchard_seeds UNION ALL SELECT seed FROM orchard_seeds",
            vec!["sprout"],
        ),
    ] {
        let output = MaterializedViewRuntimeFactory
            .output_schemas_for_view_request_with_catalogs(
                "orchard_stage", sql, std::slice::from_ref(&catalog),
                catalog.schema_fingerprint.as_str(),
            ).unwrap().unwrap();
        assert!(output[0].primary_key.is_empty());
        let stage_binding = published_relation_binding_v1(
            "orchard_stage", 1, &stable_bytes_hash(sql.as_bytes()), &output[0],
        ).unwrap();
        let stage_catalog = catalog_from_published_relation_binding(&stage_binding).unwrap();
        assert!(stage_catalog.relation_schema.primary_key_column_ids.is_empty());
        stage_catalog.validate_ingest_adapter_scope().unwrap();
        let map_sql = format!(
            "SELECT * FROM orchard_stage WHERE {} IS NOT NULL", names[0],
        );
        let mapped = MaterializedViewRuntimeFactory
            .output_schemas_for_view_request_with_catalogs(
                "orchard_filtered", &map_sql, std::slice::from_ref(&stage_catalog),
                stage_catalog.schema_fingerprint.as_str(),
            ).unwrap().unwrap();
        assert!(mapped[0].primary_key.is_empty());
        let map_plan = lower_materialized_view_runtime_sql_to_logical_plan(
            &map_sql, std::slice::from_ref(&stage_catalog), &mapped[0],
        ).unwrap();
        assert!(matches!(map_plan.execution, VelorixLogicalViewExecutionV1::TypedMap { .. }));
        let union_sql = format!(
            "SELECT {0} FROM orchard_stage UNION ALL SELECT {0} FROM orchard_stage",
            names[0],
        );
        let next_output = MaterializedViewRuntimeFactory
            .output_schemas_for_view_request_with_catalogs(
                "orchard_combined", &union_sql, std::slice::from_ref(&stage_catalog),
                stage_catalog.schema_fingerprint.as_str(),
            ).unwrap().unwrap();
        assert!(next_output[0].primary_key.is_empty());
        lower_materialized_view_runtime_sql_to_logical_plan(
            &union_sql, std::slice::from_ref(&stage_catalog), &next_output[0],
        ).unwrap();
        for adapter in [
            velorix_core::relation::CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID,
            velorix_core::relation::CATALOG_ROW_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID,
        ] {
            let mut keyed_catalog = stage_catalog.clone();
            keyed_catalog.incremental_adapter.adapter_id = adapter.into();
            assert!(keyed_catalog.validate_supported_incremental_adapter_scope().is_err());
        }
        assert_eq!(output[0].columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(), names);
        assert!(output[0].columns[0].nullable);
    }
}

#[test]
fn published_stage_preserves_nullable_repeated_keys_and_signed_multiplicity() {
    let columns = vec![
        ColumnSchema {
            name: "rack".into(),
            data_type: SqlDataType::Int64,
            nullable: true,
        },
        ColumnSchema {
            name: "reading".into(),
            data_type: SqlDataType::Int64,
            nullable: true,
        },
    ];
    let primary_key = vec!["rack".into()];
    let schema = RelationSchema {
        relation_id: "orchard_stage".into(),
        relation_name: "orchard_stage".into(),
        relation_version: "v1".into(),
        schema_fingerprint: materialized_output_schema_fingerprint(
            "orchard_stage",
            "v1",
            &columns,
            &primary_key,
        )
        .unwrap(),
        columns,
        primary_key,
    };
    let binding = published_relation_binding_v1(
        "orchard_stage",
        1,
        &stable_bytes_hash(b"synthetic-stage"),
        &schema,
    )
    .unwrap();
    let catalog = catalog_from_published_relation_binding(&binding).unwrap();
    assert!(catalog.relation_schema.columns[0].nullable);
    assert_eq!(catalog.relation_schema.primary_key_column_ids, ["rack"]);

    let delta = DeltaBatch::from_records([
        DeltaRecord::new(
            DeltaKey::from_json(json!(7)),
            DeltaValue::from_json(json!({"reading": 11})),
            2,
        ),
        DeltaRecord::new(
            DeltaKey::from_json(json!(7)),
            DeltaValue::from_json(json!({"reading": 19})),
            -1,
        ),
        DeltaRecord::new(
            DeltaKey::from_json(Value::Null),
            DeltaValue::from_json(json!({"reading": null})),
            3,
        ),
    ]);
    let batch = published_delta_to_record_batch(&binding, &delta).unwrap();
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let weights = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut rows = (0..batch.num_rows())
        .map(|index| {
            (
                (!keys.is_null(index)).then(|| keys.value(index)),
                (!values.is_null(index)).then(|| values.value(index)),
                weights.value(index),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (None, None, 3),
            (Some(7), Some(11), 2),
            (Some(7), Some(19), -1)
        ]
    );
}

#[test]
fn native_spec_startup_config_fails_closed_without_environment_mutation() {
    use std::ffi::OsStr;

    assert!(validate_native_family_spec_config(None).is_ok());
    assert!(validate_native_family_spec_config(Some(OsStr::new(""))).is_ok());
    for path in ["/operator/native-spec.json", "missing.json", " "] {
        let error = validate_native_family_spec_config(Some(OsStr::new(path)))
            .unwrap_err()
            .to_string();
        assert!(error.contains(NATIVE_FAMILY_SPEC_PATH_ENV));
        assert!(error.contains("unsupported"));
        assert!(error.contains("unset"));
    }
}

#[tokio::test]
async fn generic_api_admission_preserves_sql_paths_and_rejects_native_executions() {
    let state = tests::test_api_state_with_store(
        Arc::new(object_store::memory::InMemory::new()),
        "generic-admission-test",
        false,
    )
    .await;
    let catalog = default_scores_relation_catalog().unwrap();
    for sql in [
        "select user_id, score from scores where score > 0",
        "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
    ] {
        let schemas = MaterializedViewRuntimeFactory
            .output_schemas_for_view_request(
                "generic_output",
                sql,
                &catalog,
                catalog.schema_fingerprint.as_str(),
            )
            .unwrap()
            .unwrap();
        let plan = lower_materialized_view_runtime_sql_to_logical_plan(
            sql,
            std::slice::from_ref(&catalog),
            &schemas[0],
        )
        .unwrap();
        validate_public_runtime_plan_admission(&state, &plan).unwrap();
    }
    for kind in ["native_projection", "native_windowed_fact", "native_rollup"] {
        assert!(
            serde_json::from_value::<VelorixLogicalViewExecutionV1>(json!({
                "kind": kind, "plan": {}
            }))
            .is_err()
        );
    }
}
