use super::*;

fn append_only_scores_catalog() -> VelorixRelationCatalogV1 {
    let mut schema = test_scores_catalog().relation_schema;
    schema.primary_key_column_ids.clear();
    schema.columns[0].semantic_role = RelationSemanticRoleV1::Value;
    schema.allowed_operations = vec![RelationOperationV1::Insert];
    VelorixRelationCatalogV1::from_relation_schema(
        schema,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
    )
    .unwrap()
}

// Register as a child of tests.rs; reuse its authoritative metadata/store fixture.
async fn post(router: &Router, path: &str, body: Value, expected: StatusCode) -> Value {
    let (status, value) = call_json(router, Method::POST, path, body).await;
    assert_eq!(status, expected, "{path}: {value}");
    value
}

async fn declare(router: &Router, view: &str, input: &str, version: &str, sql: &str) {
    post(
        router,
        "/v1/views",
        json!({
            "view_id":view,"input_relation_id":input,"input_relation_version":version,
            "sql":sql,"response_formats":["json"]
        }),
        StatusCode::CREATED,
    )
    .await;
}

async fn ingest(router: &Router, offset: u64, rows: Value) {
    post(
        router,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version":"2026-05-24.v1","stream_id":"derived-stream",
            "partition_id":0,"start_offset_inclusive":offset,"rows":rows
        }),
        StatusCode::CREATED,
    )
    .await;
}

async fn query_rows(router: &Router, view: &str) -> Vec<Value> {
    for _ in 0..64 {
        let (status, value) = call_json(
            router,
            Method::POST,
            &format!("/v1/views/{view}/query"),
            json!({}),
        )
        .await;
        if status == StatusCode::OK {
            let mut rows = value["rows"].as_array().unwrap().clone();
            rows.sort_by_key(Value::to_string);
            return rows;
        }
        assert_eq!(status, StatusCode::ACCEPTED, "{view}: {value}");
        assert_eq!(value["materialization"]["complete"], false);
        assert!(
            value.get("rows").is_none(),
            "partial output escaped: {value}"
        );
    }
    panic!("bounded query did not complete for {view}");
}

async fn assert_pending(state: &ApiState) {
    // A query performs work; inspect durable progress without advancing it.
    let active = state
        .view_registry()
        .unwrap()
        .read_active("derived_copy")
        .await
        .unwrap();
    let record = read_latest_standing_runtime_checkpoint_header(
        state,
        active_standing_runtime_identity(&active).unwrap(),
        "derived_copy",
    )
    .await
    .unwrap();
    if let Some(record) = record {
        assert!(
            !record
                .demand_materialization
                .as_ref()
                .unwrap()
                .full_complete
        );
    }
}

async fn finish_backfill(router: &Router, view: &str) -> u64 {
    let mut processed_rows = 0;
    for _ in 0..64 {
        let (status, value) = call_json(
            router,
            Method::POST,
            &format!("/v1/views/{view}/backfill"),
            json!({"budget":{"batch_limit":1,"max_rows":512}}),
        )
        .await;
        assert!(
            matches!(status, StatusCode::OK | StatusCode::ACCEPTED),
            "{view}: {value}"
        );
        let progress = &value["materialization"];
        let used = progress["processed_rows"].as_u64().unwrap();
        assert!(used <= 512, "bounded step exceeded row budget: {value}");
        processed_rows += used;
        assert_eq!(progress["complete"], status == StatusCode::OK);
        if status == StatusCode::OK {
            assert_eq!(progress["remaining_rows"], 0);
            return processed_rows;
        }
        assert!(progress["remaining_rows"].as_u64().unwrap() > 0);
    }
    panic!("bounded backfill did not complete for {view}");
}

#[tokio::test]
async fn historical_derived_pages_pause_restart_and_resume_the_captured_cut() {
    let counts = ObjectStoreAccessCounts::default();
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(backing, counts.clone()));
    let meta: Arc<dyn MetaStore> =
        Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let state =
        test_authoritative_api_state_with_store(store.clone(), "derived-owner", meta.clone())
            .await
            .with_experimental_advanced_view_features(true);
    let router = app(state.clone());
    post(
        &router,
        "/v1/relations",
        json!({"catalog":append_only_scores_catalog(),"default_orders_sum_count":false}),
        StatusCode::CREATED,
    )
    .await;
    let input = (0..257)
        .map(|i| json!({"user_id":format!("item-{i:04}"),"score":i,"delta":1}))
        .collect::<Vec<_>>();
    ingest(&router, 0, json!(input)).await;
    declare(
        &router,
        "derived_source",
        "scores",
        "2026-05-24.v1",
        "SELECT user_id, SUM(score) AS score FROM scores GROUP BY user_id",
    )
    .await;
    post(
        &router,
        "/v1/views/derived_source/backfill",
        json!({"batch_limit":10}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(query_rows(&router, "derived_source").await.len(), 257);

    // Current aggregate snapshots are unpaged. UNION ALL uses the native
    // 512-row codec; repeated branches yield 514 rows with bag multiplicity.
    declare(
        &router,
        "derived_paged",
        "derived_source",
        "v1",
        "SELECT user_id, score FROM derived_source UNION ALL SELECT user_id, score FROM derived_source",
    )
    .await;

    post(
        &router,
        "/v1/views/derived_paged/backfill",
        json!({"batch_limit":10}),
        StatusCode::OK,
    )
    .await;
    let paged = state
        .view_registry()
        .unwrap()
        .read_active("derived_paged")
        .await
        .unwrap();
    assert!(
        matches!(
            &paged
                .runtime
                .as_ref()
                .unwrap()
                .logical_plan
                .as_ref()
                .unwrap()
                .execution,
            VelorixLogicalViewExecutionV1::UnionAll { .. }
        ),
        "fixture must admit a native paged family"
    );
    let paged_checkpoint = read_latest_standing_runtime_checkpoint(
        &state,
        active_standing_runtime_identity(&paged).unwrap(),
        "derived_paged",
    )
    .await
    .unwrap()
    .unwrap();
    let output_ref = paged_checkpoint
        .checkpoint
        .output_manifest_refs
        .iter()
        .find(|reference| reference.starts_with(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX))
        .unwrap();
    let (_, manifest) =
        read_standing_runtime_output_manifest_record(&state, output_ref, "derived_paged")
            .await
            .unwrap();
    assert!(materialized_output_pages::is_bag_codec(
        &manifest.output_encoding
    ));
    assert_eq!(
        manifest
            .pages
            .iter()
            .map(|page| page.row_count)
            .collect::<Vec<_>>(),
        vec![512, 2]
    );
    // 514 output occurrences exceed the existing 512-row page boundary.
    // Admission captures metadata only, not producer payload pages or consumer state.
    counts.clear();
    declare(
        &router,
        "derived_copy",
        "derived_paged",
        "v1",
        "SELECT user_id, score FROM derived_paged",
    )
    .await;
    assert!(
        counts.get_paths().iter().all(|path| !path
            .starts_with("v1/standing-runtime-output-pages/")
            && !path.starts_with("v1/standing-runtime-state-payloads/")
            && !path.starts_with("v1/standing-runtime-output-deltas/")),
        "declaration fetched output pages: {:?}",
        counts.get_paths()
    );
    let active = state
        .view_registry()
        .unwrap()
        .read_active("derived_copy")
        .await
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    assert!(
        read_latest_standing_runtime_checkpoint(&state, identity, "derived_copy")
            .await
            .unwrap()
            .is_none(),
        "declaration applied a baseline"
    );
    // Simulate an interrupted older generic declaration: registration was
    // Running, but automatic bootstrap never published a checkpoint.
    state
        .view_registry()
        .unwrap()
        .update_standing_runtime_lifecycle(
            "derived_copy",
            &active.spec_hash,
            MaterializedViewLifecycleStatus::standing_runtime(),
        )
        .await
        .unwrap();
    counts.clear();
    let duplicate = post(
        &router,
        "/v1/views",
        json!({"view_id":"derived_copy","input_relation_id":"derived_paged",
            "input_relation_version":"v1","sql":"SELECT user_id, score FROM derived_paged",
            "response_formats":["json"]}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(duplicate["outcome"], "duplicate");
    assert_eq!(duplicate["query_enabled"], false);
    let retried = state
        .view_registry()
        .unwrap()
        .read_active("derived_copy")
        .await
        .unwrap();
    assert_eq!(retried.spec_hash, active.spec_hash);
    // Includes the captured cursors, logical plan, and published generation.
    assert_eq!(retried.runtime, active.runtime);
    assert!(
        read_latest_standing_runtime_checkpoint_header(&state, identity, "derived_copy")
            .await
            .unwrap()
            .is_none()
    );
    assert!(counts.get_paths().iter().all(|path| {
        !path.starts_with("v1/standing-runtime-output-pages/")
            && !path.starts_with("v1/standing-runtime-state-payloads/")
            && !path.starts_with("v1/standing-runtime-output-deltas/")
    }));
    assert_pending(&state).await;

    counts.clear();
    let first_page = post(
        &router,
        "/v1/views/derived_copy/backfill",
        json!({"budget":{"batch_limit":1,"max_rows":512,"max_bytes":1048576,"max_duration_ms":10000}}),
        StatusCode::ACCEPTED,
    )
    .await;
    // Demand counts weighted records: 256 records of weight 2 fill this page.
    assert_eq!(first_page["materialization"]["processed_rows"], 256);
    assert_eq!(first_page["materialization"]["complete"], false);
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| path.starts_with(
                "v1/standing-runtime-output-pages/default/derived_paged/derived_paged/"
            ))
            .count(),
        1,
        "first bounded step must fetch only its selected producer page"
    );
    assert!(counts.get_paths().iter().all(|path| {
        !path.starts_with("v1/standing-runtime-state-payloads/default/derived_paged/derived_paged/")
    }));
    assert_pending(&state).await;
    // Ingest must keep working while
    // the consumer is partial, without draining post-cut deltas into its baseline.
    let partial = read_latest_standing_runtime_checkpoint(&state, identity, "derived_copy")
        .await
        .unwrap()
        .unwrap();
    let progress = partial.published_input_bootstrap.as_ref().unwrap();
    assert!(!progress.is_complete());
    assert_eq!(progress.inputs[0].total_pages, 2);
    assert_eq!(
        partial
            .demand_materialization
            .as_ref()
            .unwrap()
            .artifacts
            .values()
            .map(|artifact| artifact.applied.processed_len())
            .sum::<u64>(),
        256
    );
    assert!(partial.checkpoint.input_coverage.is_none());
    assert!(partial.checkpoint.causal_cut.is_none());
    let mut corrupt = partial.clone();
    corrupt.published_input_bootstrap.as_mut().unwrap().inputs[0].next_page_index = 3;
    let pointer = standing_runtime_checkpoint_pointer_from_record(&corrupt);
    assert!(validate_standing_runtime_checkpoint_record(
        identity,
        "derived_copy",
        &pointer,
        &corrupt
    )
    .is_err());

    // Corruption after a committed page must not advance the durable resume
    // position. Restore the immutable fixture bytes before continuing.
    let (_, manifest) = read_standing_runtime_output_manifest_record(
        &state,
        &progress.inputs[0].output_manifest_ref,
        "derived_paged",
    )
    .await
    .unwrap();
    let page_path = ObjectPath::from(manifest.pages[1].page_key.clone());
    let saved_page = store.get(&page_path).await.unwrap().bytes().await.unwrap();
    store
        .put(&page_path, bytes::Bytes::from_static(b"{}").into())
        .await
        .unwrap();
    let (status, _) = call_json(
        &router,
        Method::POST,
        "/v1/views/derived_copy/backfill",
        json!({"batch_limit":1}),
    )
    .await;
    assert!(
        status.is_client_error() || status.is_server_error(),
        "corruption accepted: {status}"
    );
    store.put(&page_path, saved_page.into()).await.unwrap();
    let unchanged = read_latest_standing_runtime_checkpoint(&state, identity, "derived_copy")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(partial.checkpoint_key, unchanged.checkpoint_key);
    assert_eq!(
        partial.published_input_bootstrap,
        unchanged.published_input_bootstrap
    );
    assert_eq!(
        partial.demand_materialization,
        unchanged.demand_materialization
    );
    let pause = post(
        &router,
        "/v1/views/derived_copy/backfill",
        json!({"action":"pause"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(pause["materialization"]["status"], "paused");
    let paused = read_latest_standing_runtime_checkpoint(&state, identity, "derived_copy")
        .await
        .unwrap()
        .unwrap();
    let blocked = post(
        &router,
        "/v1/views/derived_copy/backfill",
        json!({"batch_limit":1}),
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(blocked["materialization"]["status"], "paused");
    assert_eq!(
        read_latest_standing_runtime_checkpoint(&state, identity, "derived_copy")
            .await
            .unwrap()
            .unwrap()
            .checkpoint_key,
        paused.checkpoint_key
    );

    // A positive append replaces the aggregate's old output through derived
    // retraction/insertion; the public source remains an append-only bag.
    ingest(
        &router,
        257,
        json!([
            {"user_id":"item-0000","score":9000,"delta":1},
            {"user_id":"later","score":7000,"delta":1}
        ]),
    )
    .await;
    // Materialize the producer explicitly; authoritative ingest only records source rows.
    post(
        &router,
        "/v1/views/derived_paged/backfill",
        json!({"batch_limit":10}),
        StatusCode::OK,
    )
    .await;
    assert_pending(&state).await;
    let runtime_owner = state.owner_id.clone();
    drop(router);
    drop(state);

    let mut restored = test_authoritative_api_state_with_store(store, "derived-owner", meta)
        .await
        .with_experimental_advanced_view_features(true);
    restored.owner_id = runtime_owner;
    restored
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    let router = app(restored.clone());
    assert_pending(&restored).await;
    drain_published_view_dependencies(&restored).await.unwrap();
    assert_pending(&restored).await;
    let recovered = read_latest_standing_runtime_checkpoint(&restored, identity, "derived_copy")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.checkpoint_key, paused.checkpoint_key);
    assert_eq!(
        recovered.demand_materialization,
        paused.demand_materialization
    );
    let blocked = post(
        &router,
        "/v1/views/derived_copy/backfill",
        json!({"batch_limit":1}),
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(blocked["materialization"]["status"], "paused");
    let resume = post(
        &router,
        "/v1/views/derived_copy/backfill",
        json!({"action":"resume"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(resume["materialization"]["status"], "resumed");
    // The final baseline page consumes the entire request budget. The newer
    // producer commit must remain pending until a separate bounded request.
    counts.clear();
    let final_page = post(
        &router,
        "/v1/views/derived_copy/backfill",
        json!({"batch_limit":1}),
        StatusCode::ACCEPTED,
    )
    .await;
    assert_eq!(final_page["materialization"]["processed_rows"], 1);
    assert_eq!(final_page["materialization"]["complete"], false);
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| path.starts_with(
                "v1/standing-runtime-output-pages/default/derived_paged/derived_paged/"
            ))
            .count(),
        2,
        "resumed step reads its selected page plus one ordering boundary page"
    );
    assert!(counts.get_paths().iter().all(|path| {
        !path.starts_with("v1/standing-runtime-state-payloads/default/derived_paged/derived_paged/")
    }));
    assert_pending(&restored).await;
    drain_published_view_dependencies(&restored).await.unwrap();
    assert_pending(&restored).await;
    // Producer certification can publish empty commits as well as row deltas.
    assert!(finish_backfill(&router, "derived_copy").await > 0);
    let mut expected = (0..257)
        .map(|i| json!({"user_id":format!("item-{i:04}"),"score":if i == 0 {9000} else {i}}))
        .collect::<Vec<_>>();
    expected.push(json!({"user_id":"later","score":7000}));
    expected.extend(expected.clone());
    expected.sort_by_key(Value::to_string);
    assert_eq!(query_rows(&router, "derived_copy").await, expected);
    // Repeated manual backfill cannot add the baseline or catch-up twice.
    post(
        &router,
        "/v1/views/derived_copy/backfill",
        json!({"batch_limit":1}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(query_rows(&router, "derived_copy").await, expected);
}

#[tokio::test]
async fn running_legacy_derived_checkpoint_is_upgraded_without_reapplying_baseline() {
    legacy_derived_checkpoint_upgrade_case(false).await;
}

#[tokio::test]
async fn running_legacy_derived_checkpoint_upgrade_preserves_late_catchup() {
    legacy_derived_checkpoint_upgrade_case(true).await;
}

async fn legacy_derived_checkpoint_upgrade_case(late_catchup: bool) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta: Arc<dyn MetaStore> =
        Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let state =
        test_authoritative_api_state_with_store(store, "legacy-baseline-owner", meta.clone())
            .await
            .with_experimental_advanced_view_features(true);
    let router = app(state.clone());
    post(
        &router,
        "/v1/relations",
        json!({"catalog":append_only_scores_catalog(),
        "default_orders_sum_count":false}),
        StatusCode::CREATED,
    )
    .await;
    ingest(&router, 0, json!([{"user_id":"one","score":7,"delta":1}])).await;
    declare(
        &router,
        "baseline_source",
        "scores",
        "2026-05-24.v1",
        "SELECT user_id, score FROM scores WHERE score > 0",
    )
    .await;
    post(
        &router,
        "/v1/views/baseline_source/backfill",
        json!({}),
        StatusCode::OK,
    )
    .await;
    declare(
        &router,
        "baseline_copy",
        "baseline_source",
        "v1",
        "SELECT user_id, score FROM baseline_source",
    )
    .await;
    let active = state
        .view_registry()
        .unwrap()
        .read_active("baseline_copy")
        .await
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let input = persisted_consumer_bootstrap_inputs(&active)
        .unwrap()
        .remove(0);
    let (producer, _) = checkpoint_publication::read_authoritative_cursor_checkpoint(
        &state,
        &identity.tenant_id,
        &input.binding,
        &input.bootstrap_cursor,
    )
    .await
    .unwrap();
    let output = standing_runtime_checkpoint_published_batch_with_limits(
        &producer.checkpoint,
        state.runtime_resource_limits(),
    )
    .unwrap()
    .unwrap();
    let batch = published_delta_to_record_batch(&input.binding, &output).unwrap();
    ensure_standing_runtime_for_active_view(&state, &active)
        .await
        .unwrap();
    let runtime = state
        .standing_runtime(identity, "baseline_copy")
        .unwrap()
        .unwrap();
    let applied = apply_standing_runtime_changes_and_checkpoint(
        &state,
        runtime,
        1,
        EpochIdempotencyKey::new("legacy-baseline".to_string()).unwrap(),
        RelationInputBatch {
            relation_id: input.binding.relation.relation_id.clone(),
            relation_version: input.binding.relation.relation_version.clone(),
            stream_id: input.binding.output_stream_id.clone(),
            partition_id: 0,
            schema_fingerprint: input.binding.relation.schema_fingerprint.clone(),
            start_offset_inclusive: 0,
            end_offset_exclusive: input.bootstrap_cursor.output_epoch,
            event_time_watermark: None,
            encoding: RelationInputEncodingV1::PublishedRelationDeltaV1 {
                delta_codec_identity: input.binding.delta_codec_identity.clone(),
                output_schema_hash: input.binding.output_schema_hash.clone(),
                weight_field_name: PUBLISHED_DELTA_WEIGHT_FIELD_V1.to_string(),
                weight_field_index: input.binding.relation.columns.len(),
            },
            batches: vec![batch],
        },
        StandingRuntimeBudgetLimits::from_state(&state),
    )
    .await
    .unwrap();
    let owner = state
        .acquire_standing_runtime_owner(identity, "baseline_copy")
        .await
        .unwrap();
    persist_standing_runtime_checkpoint(
        &state,
        "baseline_copy",
        &applied.checkpoint,
        &applied.output_deltas,
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), owner)
            .with_published_relation(published_relation_binding_for_active_view(&active).unwrap())
            .with_direct_view_inputs(vec![StandingRuntimeDirectViewInputV1 {
                published_relation: input.binding,
                cursor: input.bootstrap_cursor,
            }]),
        None,
    )
    .await
    .unwrap();
    state
        .view_registry()
        .unwrap()
        .update_standing_runtime_lifecycle(
            "baseline_copy",
            &active.spec_hash,
            MaterializedViewLifecycleStatus::standing_runtime(),
        )
        .await
        .unwrap();
    let before = read_latest_standing_runtime_checkpoint(&state, identity, "baseline_copy")
        .await
        .unwrap()
        .unwrap();
    assert!(before.published_input_bootstrap.is_none());
    let mut immutable_before = Vec::new();
    let mut paths = vec![before.checkpoint_key.clone()];
    paths.extend(
        before
            .checkpoint
            .output_manifest_refs
            .iter()
            .filter_map(|reference| {
                reference
                    .strip_prefix(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX)
                    .or_else(|| reference.strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX))
                    .map(str::to_string)
            }),
    );
    for path in paths {
        let bytes = state
            .store
            .get(&ObjectPath::from(path.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        immutable_before.push((path, bytes));
    }
    // Advance the producer while the old baseline remains fixed. Restore the
    // old Running registry shape before exercising the backfill fast path.
    state
        .view_registry()
        .unwrap()
        .update_standing_runtime_lifecycle(
            "baseline_copy",
            &active.spec_hash,
            lifecycle_for_create_view_execution(
                &MaterializedViewExecutionMode::StandingRuntime,
                true,
            ),
        )
        .await
        .unwrap();
    if late_catchup {
        ingest(&router, 1, json!([{"user_id":"later","score":9,"delta":1}])).await;
        post(
            &router,
            "/v1/views/baseline_source/backfill",
            json!({"batch_limit":10}),
            StatusCode::OK,
        )
        .await;
    }
    state
        .view_registry()
        .unwrap()
        .update_standing_runtime_lifecycle(
            "baseline_copy",
            &active.spec_hash,
            MaterializedViewLifecycleStatus::standing_runtime(),
        )
        .await
        .unwrap();
    assert_eq!(
        meta.read_view_bootstrap(&identity.tenant_id, &identity.program_id, "baseline_copy")
            .await
            .unwrap()
            .unwrap()
            .lifecycle,
        ViewBootstrapLifecycleV1::Bootstrapping
    );
    // A checkpoint without demand progress retains the legacy backfill contract.
    let before_request = read_latest_standing_runtime_checkpoint(&state, identity, "baseline_copy")
        .await
        .unwrap()
        .unwrap();
    let current_input = consumer_direct_view_inputs(&active, &before_request)
        .unwrap()
        .remove(0);
    let producer = state
        .view_registry()
        .unwrap()
        .read_active("baseline_source")
        .await
        .unwrap();
    let producer_head = capture_producer_head_cursor(
        &state,
        &identity.tenant_id,
        &producer,
        &current_input.published_relation,
        &current_input.cursor.input_edge,
    )
    .await
    .unwrap();
    let initial_input = consumer_direct_view_inputs(&active, &before)
        .unwrap()
        .remove(0);
    assert_eq!(
        producer_head.output_epoch > initial_input.cursor.output_epoch,
        late_catchup,
        "late append must advance the published producer cursor"
    );
    let pending = read_validated_producer_commits_after_cursor(
        &state,
        &current_input.published_relation,
        &current_input.cursor,
    )
    .await
    .unwrap();
    assert!(
        pending.len() <= 1,
        "fixture catch-up exceeds its one-commit request budget"
    );
    if pending.is_empty() {
        assert_eq!(
            current_input.cursor, producer_head,
            "zero replay requires an already covered head"
        );
    }
    let response = post(
        &router,
        "/v1/views/baseline_copy/backfill",
        json!({"batch_limit":1}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(response["query_enabled"], true);
    assert_eq!(
        response["applied_batches"],
        pending.len(),
        "only commits not already covered before this request may be replayed"
    );
    let after = read_latest_standing_runtime_checkpoint(&state, identity, "baseline_copy")
        .await
        .unwrap()
        .unwrap();
    assert!(after
        .published_input_bootstrap
        .as_ref()
        .unwrap()
        .is_complete());
    assert!(after.checkpoint.logical_epoch > before.checkpoint.logical_epoch);
    assert_ne!(after.checkpoint_key, before.checkpoint_key);
    assert_eq!(
        consumer_direct_view_inputs(&active, &after)
            .unwrap()
            .remove(0)
            .cursor,
        producer_head
    );
    let mut expected = vec![json!({"user_id":"one","score":7})];
    if late_catchup {
        expected.push(json!({"user_id":"later","score":9}));
    }
    expected.sort_by_key(Value::to_string);
    assert_eq!(query_rows(&router, "baseline_copy").await, expected);
    assert_eq!(
        read_latest_standing_runtime_checkpoint(&state, identity, "baseline_copy")
            .await
            .unwrap()
            .unwrap()
            .checkpoint_key,
        after.checkpoint_key,
        "row verification must not repair missing catch-up work"
    );
    let retry = post(
        &router,
        "/v1/views/baseline_copy/backfill",
        json!({"batch_limit":1}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(retry["applied_batches"], 0);
    let retried = read_latest_standing_runtime_checkpoint(&state, identity, "baseline_copy")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retried.checkpoint_key, after.checkpoint_key);
    assert_eq!(query_rows(&router, "baseline_copy").await, expected);
    for (path, bytes) in immutable_before {
        assert_eq!(
            state
                .store
                .get(&ObjectPath::from(path))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            bytes
        );
    }
}

#[tokio::test]
async fn empty_root_global_aggregate_chain_bootstraps_and_updates_automatically() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta: Arc<dyn MetaStore> =
        Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let state =
        test_authoritative_api_state_with_store(store.clone(), "empty-derived-owner", meta.clone())
            .await;
    let router = app(state.clone());
    post(
        &router,
        "/v1/relations",
        json!({"catalog":append_only_scores_catalog(),"default_orders_sum_count":false}),
        StatusCode::CREATED,
    )
    .await;
    for (family, aggregate, empty) in [
        ("count", "COUNT(*)", json!(0)),
        ("sum", "SUM(score)", Value::Null),
    ] {
        let total = format!("empty_{family}_total");
        let copy = format!("empty_{family}_copy");
        let leaf = format!("empty_{family}_leaf");
        declare(
            &router,
            &total,
            "scores",
            "2026-05-24.v1",
            &format!("SELECT {aggregate} AS result FROM scores"),
        )
        .await;
        declare(
            &router,
            &copy,
            &total,
            "v1",
            &format!("SELECT result FROM {total}"),
        )
        .await;
        declare(
            &router,
            &leaf,
            &copy,
            "v1",
            &format!("SELECT result FROM {copy}"),
        )
        .await;
        for view in [&total, &copy, &leaf] {
            assert_eq!(
                query_rows(&router, view).await,
                vec![json!({"result":empty})]
            );
        }
    }
    let runtime_owner = state.owner_id.clone();
    drop(router);
    drop(state);
    let mut restored =
        test_authoritative_api_state_with_store(store, "empty-derived-owner", meta).await;
    restored.owner_id = runtime_owner;
    restored
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    let router = app(restored);
    for (family, empty) in [("count", json!(0)), ("sum", Value::Null)] {
        assert_eq!(
            query_rows(&router, &format!("empty_{family}_leaf")).await,
            vec![json!({"result":empty})]
        );
    }
    ingest(&router, 0, json!([{"user_id":"first","score":7,"delta":1}])).await;
    for (family, expected) in [("count", 1), ("sum", 7)] {
        for stage in ["total", "copy", "leaf"] {
            assert_eq!(
                query_rows(&router, &format!("empty_{family}_{stage}")).await,
                vec![json!({"result":expected})]
            );
        }
    }
    ingest(&router, 1, json!([{"user_id":"first","score":5,"delta":1}])).await;
    for (family, expected) in [("count", 2), ("sum", 12)] {
        for stage in ["total", "copy", "leaf"] {
            assert_eq!(
                query_rows(&router, &format!("empty_{family}_{stage}")).await,
                vec![json!({"result":expected})]
            );
        }
    }
}

#[tokio::test]
async fn shared_physical_root_history_defers_even_empty_published_branches() {
    shared_root_history_case(true).await;
}

#[tokio::test]
async fn legacy_ingest_eagerly_bootstraps_empty_historical_derived_baselines() {
    shared_root_history_case(false).await;
}

async fn shared_root_history_case(authoritative: bool) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta: Arc<dyn MetaStore> =
        Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let mut state = test_authoritative_api_state_with_store(store, "shared-root-owner", meta).await;
    if !authoritative {
        state.relation_ingest_mode = RelationIngestMode::Legacy;
    }
    let router = app(state.clone());
    post(
        &router,
        "/v1/relations",
        json!({"catalog":append_only_scores_catalog(),"default_orders_sum_count":false}),
        StatusCode::CREATED,
    )
    .await;
    ingest(&router, 0, json!([{"user_id":"a","score":7,"delta":1}])).await;
    for name in ["branch_one", "branch_two"] {
        declare(
            &router,
            name,
            "scores",
            "2026-05-24.v1",
            "SELECT user_id, score FROM scores WHERE score < 0",
        )
        .await;
        post(
            &router,
            &format!("/v1/views/{name}/backfill"),
            json!({"batch_limit":1}),
            StatusCode::OK,
        )
        .await;
        assert!(query_rows(&router, name).await.is_empty());
    }
    post(&router, "/v1/views", json!({"view_id":"empty_union", "input_relation_refs":[
        {"relation_id":"branch_one","relation_version":"v1"},
        {"relation_id":"branch_two","relation_version":"v1"}],
        "sql":"SELECT user_id, score FROM branch_one UNION ALL SELECT user_id, score FROM branch_two",
        "response_formats":["json"]}), StatusCode::CREATED).await;
    if !authoritative {
        let response = post(
            &router,
            "/v1/views/empty_union/query",
            json!({}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(response["rows"], json!([]));
    }
    let active = state
        .view_registry()
        .unwrap()
        .read_active("empty_union")
        .await
        .unwrap();
    let checkpoint = read_latest_standing_runtime_checkpoint(
        &state,
        active_standing_runtime_identity(&active).unwrap(),
        "empty_union",
    )
    .await
    .unwrap();
    assert_eq!(
        checkpoint.is_none(),
        authoritative,
        "only authoritative declaration defers materialization"
    );
    post(
        &router,
        "/v1/views/empty_union/backfill",
        json!({"batch_limit":10}),
        StatusCode::OK,
    )
    .await;
    assert!(query_rows(&router, "empty_union").await.is_empty());
    ingest(&router, 1, json!([{"user_id":"b","score":-2,"delta":1}])).await;
    assert_eq!(
        query_rows(&router, "empty_union").await,
        vec![
            json!({"user_id":"b","score":-2}),
            json!({"user_id":"b","score":-2})
        ]
    );
}
