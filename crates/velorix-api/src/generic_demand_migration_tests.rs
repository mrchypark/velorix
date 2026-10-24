//! Register as a child of tests.rs; integration owner runs these tests.
use super::*;

#[tokio::test]
async fn checkpoint_body_and_fragment_reads_share_cache_and_preserve_disabled_path() {
    let counts = ObjectStoreAccessCounts::default();
    let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        Arc::new(InMemory::new()),
        counts.clone(),
    ));
    let state = test_authoritative_api_state_with_store(
        store.clone(),
        "cache-test-owner",
        Arc::new(InMemoryMetaStore::default()),
    )
    .await;
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = materialized_read_cache::MaterializedReadCache::open(
        materialized_read_cache::MaterializedReadCacheConfig {
            directory: cache_dir.path().join("cache"),
            memory_bytes: 64 * 1024,
            disk_bytes: 8 * 1024 * 1024,
        },
    )
    .await
    .unwrap();
    let state = state
        .with_materialized_read_cache(Arc::new(cache))
        .with_standing_runtime_state_fragment_bytes(4096)
        .unwrap();
    let checkpoint_record = test_runtime_checkpoint(Vec::new());
    let checkpoint = test_checkpoint_key(&checkpoint_record);
    let body = checkpoint_record::encode(
        &test_checkpoint_record(&checkpoint, checkpoint_record),
        checkpoint_record::MAX_ENCODED_RECORD_BYTES,
    )
    .unwrap();
    let checkpoint_path = ObjectPath::from(checkpoint.as_str());
    store
        .put(&checkpoint_path, body.clone().into())
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            checkpoint_publication::read_object_bytes_bounded(&state, &checkpoint_path, None)
                .await
                .unwrap(),
            body
        );
    }
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|p| p.as_str() == checkpoint_path.as_ref())
            .count(),
        1
    );

    let checkpoint_state = test_runtime_checkpoint(Vec::new());
    let (arrow_key, arrow_record) = standing_runtime_state_payload_record_for_checkpoint(
        &checkpoint_state,
        "purchases_by_user",
    )
    .unwrap();
    persist_standing_runtime_state_payload(&state, &arrow_key, &arrow_record)
        .await
        .unwrap();
    counts.clear();
    for _ in 0..2 {
        let bytes = checkpoint_publication::read_object_bytes_bounded(
            &state,
            &ObjectPath::from(arrow_key.as_str()),
            None,
        )
        .await
        .unwrap();
        assert!(bytes.starts_with(&[255; 4]));
        assert_eq!(
            checkpoint_arrow::decode(&bytes, state.max_standing_runtime_state_payload_bytes)
                .unwrap(),
            arrow_record
        );
    }
    assert_eq!(
        counts.get_paths(),
        vec![arrow_key.as_str().to_string()],
        "warm Arrow state read must reuse verified bytes"
    );

    let fragment_body = b"fragment bytes".to_vec();
    let fragment_hash = stable_bytes_hash(&fragment_body);
    let state_root = ObjectKey::standing_runtime_state_payload(
        "tenant",
        "program",
        "cache-view",
        1,
        &fragment_hash,
    )
    .unwrap();
    let fragment = ObjectPath::from(format!(
        "{}.fragment-0-{fragment_hash}",
        state_root.as_str()
    ));
    store
        .put(&fragment, fragment_body.clone().into())
        .await
        .unwrap();
    counts.clear();
    for _ in 0..2 {
        assert_eq!(
            checkpoint_fragments::read_bounded(&state, &fragment, fragment_body.len())
                .await
                .unwrap(),
            fragment_body
        );
    }
    assert_eq!(counts.get_paths(), vec![fragment.to_string()]);

    let payload =
        velorix_core::arrow_state::to_state(&json!({"retained": synthetic_state_text(64 * 1024)}))
            .unwrap();
    let payload_hash = stable_bytes_hash(payload.as_bytes());
    let root = ObjectKey::standing_runtime_state_payload(
        "tenant",
        "program",
        "cache-view",
        1,
        &payload_hash,
    )
    .unwrap();
    let manifest_record = StandingRuntimeStatePayloadRecord {
        schema_version: 1,
        record_kind: "standing_runtime_state_payload_v1".into(),
        tenant_id: "tenant".into(),
        program_id: "program".into(),
        view_id: "cache-view".into(),
        logical_epoch: 1,
        checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".into(),
        state_content_hash: payload_hash,
        source_kind: "standing_runtime_checkpoint_state_payload".into(),
        payload: RuntimeCheckpointStatePayload {
            codec_identity: "velorix-standing-program-checkpoint-v1".into(),
            payload,
        },
    };
    assert!(
        checkpoint_fragments::persist_fragments(&state, &root, &manifest_record)
            .await
            .unwrap()
    );
    counts.clear();
    let root_path = ObjectPath::from(root.as_str());
    for _ in 0..2 {
        assert!(
            checkpoint_fragments::read_bounded(&state, &root_path, 64 * 1024)
                .await
                .unwrap()
                .starts_with(b"{\"schema_version\":2")
        );
    }
    assert_eq!(counts.get_paths(), vec![root_path.to_string()]);

    let disabled = test_authoritative_api_state_with_store(
        store.clone(),
        "cache-disabled-owner",
        Arc::new(InMemoryMetaStore::default()),
    )
    .await;
    counts.clear();
    for _ in 0..2 {
        assert_eq!(
            checkpoint_publication::read_object_bytes_bounded(&disabled, &checkpoint_path, None)
                .await
                .unwrap(),
            body
        );
    }
    assert_eq!(counts.get_paths().len(), 2);
}

async fn request(router: &Router, path: &str, body: Value) -> Value {
    let (status, value) = call_json(router, Method::POST, path, body).await;
    assert!(status.is_success(), "{path}: {status} {value}");
    value
}

async fn declare(router: &Router, view: &str, input: &str, version: &str) {
    request(
        router,
        "/v1/views",
        json!({"view_id":view,
        "input_relation_id":input,"input_relation_version":version,
        "sql":format!("SELECT user_id, score FROM {input}"),
        "response_formats":["json"]}),
    )
    .await;
}

async fn append(router: &Router, offset: u64, score: i64) {
    request(
        router,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version":"2026-05-24.v1","stream_id":"migration-stream",
            "partition_id":0,"start_offset_inclusive":offset,
            "rows":[{"user_id":"one","score":score,"delta":1}]
        }),
    )
    .await;
}

async fn finish(state: &ApiState, view: &str) -> u64 {
    let active = tests::read_test_active_view(state, view).await.unwrap();
    let request = demand_materialization::DemandRequest {
        scope: Some(demand_materialization::DemandScope {
            predicate: "score >= 7".into(),
        }),
        budget: demand_materialization::DemandBudget {
            batch_limit: 1,
            max_rows: 1,
            max_bytes: 16 * 1024 * 1024,
            max_duration_ms: 10000,
        },
    };
    let mut applied = 0;
    for _ in 0..32 {
        let outcome = demand_materialization::materialize_demand(
            state,
            &active,
            &request,
            demand_materialization::DemandOrder::Forward,
            true,
        )
        .await
        .unwrap();
        applied += outcome.processed_rows;
        if outcome.complete {
            return applied;
        }
    }
    panic!("migration did not finish");
}

async fn run_case(derived: bool) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta: Arc<dyn MetaStore> =
        Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let state =
        test_authoritative_api_state_with_store(store.clone(), "migration-owner", meta.clone())
            .await
            .with_experimental_advanced_view_features(true);
    let router = app(state.clone());
    let mut schema = test_scores_catalog().relation_schema;
    schema.primary_key_column_ids.clear();
    schema.columns[0].semantic_role = RelationSemanticRoleV1::Value;
    schema.allowed_operations = vec![RelationOperationV1::Insert];
    let catalog = VelorixRelationCatalogV1::from_relation_schema(
        schema,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap();
    request(&router, "/v1/relations", json!({"catalog":catalog})).await;
    declare(&router, "migration_source", "scores", "2026-05-24.v1").await;
    seed_legacy_eager_checkpoint_for_compatibility(&state, "migration_source").await;
    append(&router, 0, 7).await;
    append(&router, 1, 3).await;
    let target = if derived {
        declare(&router, "migration_copy", "migration_source", "v1").await;
        seed_legacy_eager_checkpoint_for_compatibility(&state, "migration_copy").await;
        "migration_copy"
    } else {
        "migration_source"
    };
    let active = tests::read_test_active_view(&state, target).await.unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let old = read_latest_standing_runtime_checkpoint(&state, identity, target)
        .await
        .unwrap()
        .unwrap();
    assert!(old.demand_materialization.is_none());
    if derived {
        let baseline_ref = old
            .published_input_bootstrap
            .as_ref()
            .and_then(|bootstrap| bootstrap.inputs.first())
            .map(|input| input.output_manifest_ref.as_str())
            .expect("legacy derived checkpoint retains its applied baseline identity");
        let baseline_key = baseline_ref
            .strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX)
            .expect("fixture baseline has a manifest reference");
        // Isolated in-memory fixture only: make the already-applied immutable
        // baseline unavailable so the migration proves it never reloads it.
        // Production objects and checkpoint pointers are never modified.
        store.delete(&ObjectPath::from(baseline_key)).await.unwrap();
    }
    let tiny = demand_materialization::DemandRequest {
        scope: None,
        budget: demand_materialization::DemandBudget {
            max_bytes: 1,
            ..Default::default()
        },
    };
    let error = demand_materialization::materialize_demand(
        &state,
        &active,
        &tiny,
        demand_materialization::DemandOrder::Forward,
        true,
    )
    .await
    .unwrap_err();
    assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        read_latest_standing_runtime_checkpoint(&state, identity, target)
            .await
            .unwrap()
            .unwrap()
            .checkpoint_key,
        old.checkpoint_key
    );
    let old_bytes = store
        .get(&ObjectPath::from(old.checkpoint_key.clone()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    // Imported physical prefixes must contribute no applied rows.
    assert_eq!(finish(&state, "migration_source").await, 0);
    append(&router, 2, 11).await;
    // A derived consumer remains at its eager baseline while its source is
    // demand-managed. Catch-up must traverse source -> consumer, not reapply it.
    let processed = finish(&state, target).await;
    assert_eq!(processed, if derived { 2 } else { 1 });
    let body = json!({"materialization":{"scope":{"where":"score >= 7"},
        "budget":{"batch_limit":4,"max_rows":10,"max_bytes":16777216,"max_duration_ms":10000}}});
    let result = request(&router, &format!("/v1/views/{target}/query"), body.clone()).await;
    let mut rows = result["rows"].as_array().unwrap().clone();
    rows.sort_by_key(Value::to_string);
    let mut expected = vec![
        json!({"user_id":"one","score":7}),
        json!({"user_id":"one","score":11}),
    ];
    expected.sort_by_key(Value::to_string);
    assert_eq!(rows, expected);
    assert_eq!(
        store
            .get(&ObjectPath::from(old.checkpoint_key))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        old_bytes
    );
    let restarted = state.clone();
    remove_standing_runtime(&restarted, identity, target).unwrap();
    if derived {
        remove_standing_runtime(&restarted, identity, "migration_source").unwrap();
    }
    assert_eq!(
        finish(&restarted, target).await,
        0,
        "restart replayed imported rows"
    );
    let result = request(&app(restarted), &format!("/v1/views/{target}/query"), body).await;
    let mut rows = result["rows"].as_array().unwrap().clone();
    rows.sort_by_key(Value::to_string);
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn bounded_fragment_reader_rejects_actual_size_before_buffering() {
    let counts = ObjectStoreAccessCounts::default();
    let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        Arc::new(InMemory::new()),
        counts.clone(),
    ));
    let state = test_api_state_with_store(store.clone(), "bounded-fragment", false).await;
    let path = ObjectPath::from("migration-test/oversized-fragment");
    store
        .put(&path, bytes::Bytes::from_static(b"0123456789").into())
        .await
        .unwrap();
    counts.clear();
    let error = checkpoint_fragments::read_bounded(&state, &path, 4)
        .await
        .unwrap_err();
    assert!(error.message.contains("declared byte limit"));
    assert!(
        counts.get_paths().is_empty(),
        "oversized fragment body was fetched"
    );
}

#[tokio::test]
async fn derived_pause_resume_preserves_cursors_without_physical_source_cut() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta: Arc<dyn MetaStore> =
        Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let state = test_authoritative_api_state_with_store(store, "control-owner", meta)
        .await
        .with_experimental_advanced_view_features(true);
    let router = app(state.clone());
    let mut schema = test_scores_catalog().relation_schema;
    schema.primary_key_column_ids.clear();
    schema.columns[0].semantic_role = RelationSemanticRoleV1::Value;
    schema.allowed_operations = vec![RelationOperationV1::Insert];
    let catalog = VelorixRelationCatalogV1::from_relation_schema(
        schema,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap();
    request(&router, "/v1/relations", json!({"catalog":catalog})).await;
    append(&router, 0, 9).await;
    declare(&router, "control_source", "scores", "2026-05-24.v1").await;
    finish(&state, "control_source").await;
    declare(&router, "control_copy", "control_source", "v1").await;
    finish(&state, "control_copy").await;
    let active = tests::read_test_active_view(&state, "control_copy")
        .await
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let before = read_latest_standing_runtime_checkpoint(&state, identity, "control_copy")
        .await
        .unwrap()
        .unwrap();
    let direct_before = consumer_direct_view_inputs(&active, &before).unwrap();
    demand_materialization::set_demand_control(
        &state,
        &active,
        true,
        demand_materialization::DemandOrder::Forward,
    )
    .await
    .unwrap();
    demand_materialization::set_demand_control(
        &state,
        &active,
        false,
        demand_materialization::DemandOrder::Reverse,
    )
    .await
    .unwrap();
    let after = read_latest_standing_runtime_checkpoint(&state, identity, "control_copy")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.published_input_bootstrap,
        before.published_input_bootstrap
    );
    let direct_after = consumer_direct_view_inputs(&active, &after).unwrap();
    assert_eq!(direct_after.len(), direct_before.len());
    for (after, before) in direct_after.iter().zip(&direct_before) {
        assert_eq!(after.cursor, before.cursor);
        assert_eq!(after.published_relation, before.published_relation);
    }
}

#[tokio::test]
async fn physical_eager_prefix_migrates_without_replay_and_filters_after_restart() {
    run_case(false).await;
}

#[tokio::test]
async fn derived_eager_cursor_catches_late_source_append_without_baseline_replay() {
    run_case(true).await;
}
