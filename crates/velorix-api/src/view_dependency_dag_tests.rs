use super::*;
use axum::http::Method;
use tower::ServiceExt;
use velorix_control::meta_admin::InMemoryMetaStore;
use velorix_core::relation::*;

async fn post(router: &Router, path: &str, body: Value, expected: StatusCode) -> Value {
    let (status, value) = post_unchecked(router, path, body).await;
    assert_eq!(status, expected, "{path}: {value}");
    value
}

async fn post_unchecked(router: &Router, path: &str, body: Value) -> (StatusCode, Value) {
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
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    (status, value)
}

fn catalog(name: &str) -> VelorixRelationCatalogV1 {
    VelorixRelationCatalogV1::from_relation_schema(
        VelorixRelationSchemaV1 {
            relation_id: name.into(),
            relation_name: name.into(),
            relation_version: "v1".into(),
            columns: ["item", "reading", "delta"]
                .into_iter()
                .enumerate()
                .map(|(ordinal, name)| RelationColumnV1 {
                    column_id: name.into(),
                    name: name.into(),
                    logical_type: VelorixLogicalTypeV1::Int64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                    nullable: name == "reading",
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
            allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
            event_time_column_id: None,
        },
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap()
}

async fn ingest(router: &Router, source: &str, offset: u64, rows: Value, status: StatusCode) {
    post(
        router,
        &format!("/v1/relations/{source}/ingest"),
        json!({
            "relation_version":"v1", "stream_id":format!("{source}-stream"), "partition_id":0,
            "start_offset_inclusive":offset,"rows":rows,
        }),
        status,
    )
    .await;
}

async fn checkpoint(
    state: &ApiState,
    view: &str,
) -> (ActiveMaterializedView, StandingRuntimeCheckpointRecord) {
    let active = tests::read_test_active_view(state, view).await.unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let record = read_latest_standing_runtime_checkpoint(state, identity, view)
        .await
        .unwrap()
        .unwrap();
    (active, record)
}

async fn assert_rows(router: &Router, mut expected: Vec<Value>) {
    let result = post(
        router,
        "/v1/views/combined/query",
        json!({}),
        StatusCode::OK,
    )
    .await;
    let mut actual = result["rows"].as_array().unwrap().clone();
    actual.sort_by_key(Value::to_string);
    expected.sort_by_key(Value::to_string);
    assert_eq!(actual, expected);
}

fn rows(join: bool, left: i64, right: &[i64]) -> Vec<Value> {
    if join {
        right
            .iter()
            .map(|r| json!({"item":1,"left_reading":left,"right_reading":r}))
            .collect()
    } else {
        std::iter::once(left)
            .chain(right.iter().copied())
            .map(|r| json!({"item":1,"reading":r}))
            .collect()
    }
}

#[tokio::test]
async fn active_view_read_snapshot_reuses_validated_records_and_refreshes_per_operation() {
    let counts = crate::tests::ObjectStoreAccessCounts::default();
    let store: Arc<dyn ObjectStore> = Arc::new(crate::tests::CountingObjectStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        counts.clone(),
    ));
    let cache_dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        materialized_read_cache::MaterializedReadCache::open(
            materialized_read_cache::MaterializedReadCacheConfig {
                directory: cache_dir.path().to_owned(),
                memory_bytes: 1024 * 1024,
                disk_bytes: 8 * 1024 * 1024,
            },
        )
        .await
        .unwrap(),
    );
    let state = crate::tests::test_api_state_with_store(store, "snapshot-owner", false).await;
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let state = state.with_meta_store(meta_store.clone());
    meta_store
        .import_active_view_registry(ImportActiveViewRegistryRequestV1 {
            scope: view_dependencies::active_view_registry_scope(&state),
            records: vec![],
        })
        .await
        .unwrap();
    let router = app(state.clone());
    post(
        &router,
        "/v1/relations",
        json!({"catalog":catalog("snapshot_source")}),
        StatusCode::CREATED,
    )
    .await;
    post(
        &router,
        "/v1/views",
        json!({
            "view_id":"snapshot_view",
            "input_relation_id":"snapshot_source",
            "input_relation_version":"v1",
            "sql":"SELECT item, reading FROM snapshot_source WHERE reading IS NOT NULL",
            "response_formats":["json"]
        }),
        StatusCode::CREATED,
    )
    .await;
    post(
        &router,
        "/v1/relations",
        json!({"catalog":catalog("unrelated_source")}),
        StatusCode::CREATED,
    )
    .await;
    post(
        &router,
        "/v1/views",
        json!({
            "view_id":"unrelated_view",
            "input_relation_id":"unrelated_source",
            "input_relation_version":"v1",
            "sql":"SELECT item, reading FROM unrelated_source WHERE reading IS NOT NULL",
            "response_formats":["json"]
        }),
        StatusCode::CREATED,
    )
    .await;
    let state = state.with_materialized_read_cache(cache.clone());

    counts.clear();
    let first = ActiveViewReadSnapshot::load(&state).await.unwrap();
    for _ in 0..3 {
        view_dependency_edges_from_snapshot(first.views()).unwrap();
    }
    assert!(counts.list_prefixes().is_empty());
    let paths = counts.get_paths();
    assert_eq!(
        paths
            .iter()
            .filter(|path| path.ends_with("active.json"))
            .count(),
        0,
        "mutable active pointers are served by one Meta snapshot"
    );
    assert_eq!(
        paths
            .iter()
            .filter(|path| path.contains("spec-sha256"))
            .count(),
        2
    );

    let active = first.views().first().unwrap();
    view_dependencies::update_active_view_lifecycle_admin(
        &state,
        "snapshot_view",
        &active.spec_hash,
        MaterializedViewLifecycleStatus::standing_runtime_deploying(None),
    )
    .await
    .unwrap();
    counts.clear();
    let second = ActiveViewReadSnapshot::load(&state).await.unwrap();
    assert_eq!(
        second.views()[0].lifecycle.deployment_status,
        MaterializedViewDeploymentStatus::Deploying
    );
    assert!(counts.list_prefixes().is_empty());
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| path.ends_with("active.json"))
            .count(),
        0,
        "next operation sees the external Meta update without an S3 pointer read"
    );
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| path.contains("spec-sha256"))
            .count(),
        0,
        "a second operation must reuse only the immutable spec bytes"
    );
    let error = demand_materialization::demand_root_from_snapshot(active, &second).unwrap_err();
    assert!(error.message.contains("changed after it was read"));

    let registry = state.view_registry().unwrap();
    let record = second.records.get("snapshot_view").unwrap().record.clone();
    let key =
        active_view_spec_cache_key(&state, &registry, &record.view_id, &record.spec_hash).unwrap();
    let mut other_authority = key.clone();
    other_authority.store_id.push_str("-other");
    let mut other_namespace = key.clone();
    other_namespace.namespace.push_str("-other");
    assert!(cache
        .get(&other_authority, |_| Ok::<(), ()>(()))
        .await
        .is_none());
    assert!(cache
        .get(&other_namespace, |_| Ok::<(), ()>(()))
        .await
        .is_none());

    // A corrupt cache entry is discarded and refetched from the authoritative
    // immutable object; it can never be returned as a spec.
    cache
        .insert(key, b"corrupt cached spec".to_vec(), |_| Ok::<(), ()>(()))
        .unwrap();
    counts.clear();
    let recovered = ActiveViewReadSnapshot::load(&state).await.unwrap();
    assert_eq!(recovered.views()[0].spec.view_id, "snapshot_view");
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| path.contains("spec-sha256"))
            .count(),
        1,
        "corrupt cached bytes must miss and reload the verified spec"
    );

    counts.clear();
    let closure = ActiveViewReadSnapshot::load_demand_closure(&state, "snapshot_view")
        .await
        .unwrap();
    assert_eq!(closure.views().len(), 1);
    assert_eq!(closure.views()[0].spec.view_id, "snapshot_view");
    assert!(counts.list_prefixes().is_empty());
    assert!(counts
        .get_paths()
        .iter()
        .all(|path| !path.ends_with("active.json")));
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| path.contains("spec-sha256"))
            .count(),
        0
    );

    // Query entry points take a new Meta snapshot while resolving unchanged
    // immutable specs through the validated Foyer cache.
    counts.clear();
    let queried = view_dependencies::read_active_cached(&state, "snapshot_view")
        .await
        .unwrap();
    assert_eq!(
        queried.lifecycle.deployment_status,
        MaterializedViewDeploymentStatus::Deploying
    );
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| path.ends_with("active.json"))
            .count(),
        0,
        "every request refreshes mutable records from Meta, not S3"
    );
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| path.contains("spec-sha256"))
            .count(),
        0,
        "an unchanged immutable spec should reuse the validated cache"
    );

    let mut refreshed_spec = second.views()[0].spec.clone();
    refreshed_spec.sql.push_str(" /* next request spec */");
    let refreshed_spec_hash = view_spec_hash(&refreshed_spec).unwrap();
    let versioned = second.records.get("snapshot_view").unwrap();
    let previous_runtime = versioned.record.runtime.as_ref().unwrap();
    let refreshed_runtime = view_admission::materialized_view_runtime_binding_for_spec(
        &[catalog("snapshot_source")],
        &refreshed_spec,
        &previous_runtime.input_bindings,
        None,
    )
    .unwrap();
    registry
        .register_with_api_metadata_runtime_execution(
            &refreshed_spec,
            versioned.record.api.clone(),
            refreshed_runtime,
            versioned.record.lifecycle.clone(),
        )
        .await
        .unwrap();
    let mut refreshed_record = versioned.record.clone();
    refreshed_record.spec_hash = refreshed_spec_hash.clone();
    assert!(matches!(
        meta_store
            .compare_exchange_active_view_record(CompareExchangeActiveViewRecordRequestV1 {
                scope: view_dependencies::active_view_registry_scope(&state),
                view_id: "snapshot_view".into(),
                expected_generation: versioned.generation,
                record: refreshed_record,
                owner: None,
            })
            .await
            .unwrap(),
        CompareExchangeActiveViewRecordOutcomeV1::Updated(_)
    ));
    counts.clear();
    let (refreshed, _) = ActiveViewReadSnapshot::load_one(&state, "snapshot_view")
        .await
        .unwrap();
    assert_eq!(refreshed.spec_hash, refreshed_spec_hash);
    assert_eq!(refreshed.spec.sql, refreshed_spec.sql);
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| path.contains("spec-sha256"))
            .count(),
        1,
        "the next request follows a changed Meta spec hash and loads its immutable spec"
    );
    cache.close().await.unwrap();
}

async fn two_producer_dag(join: bool) {
    // Keep ordinary DAG recovery work comfortably inside the owner lease even
    // when this integration test runs beside the full API suite. Expiry and
    // takeover remain covered below after the router and old state are dropped.
    const OWNER_TTL_MS: u64 = 5_000;
    let failing_store = Arc::new(crate::tests::ArmedPrefixFailingObjectStore::new(Arc::new(
        object_store::memory::InMemory::new(),
    )));
    let store: Arc<dyn ObjectStore> = failing_store.clone();
    let meta = Arc::new(crate::tests::DurableCapabilityMetaStore::new(
        velorix_control::meta_admin::InMemoryMetaStore::default(),
    ));
    let state = crate::tests::with_initialized_active_view_meta_store(
        crate::tests::test_api_state_with_store(store.clone(), "dag-owner-a", false).await,
        meta.clone(),
    )
    .await
    .with_standing_runtime_owner_ttl_ms(OWNER_TTL_MS);
    let router = app(state.clone());
    for (source, producer) in [
        ("left_source", "left_stage"),
        ("right_source", "right_stage"),
    ] {
        post(
            &router,
            "/v1/relations",
            json!({"catalog":catalog(source)}),
            StatusCode::CREATED,
        )
        .await;
        post(&router, "/v1/views", json!({"view_id":producer,"input_relation_id":source,
            "input_relation_version":"v1", "sql":format!("SELECT item, reading FROM {source} WHERE reading IS NOT NULL"),
            "response_formats":["json"]}), StatusCode::CREATED).await;
    }
    ingest(
        &router,
        "left_source",
        0,
        json!([{"item":1,"reading":10,"delta":1}]),
        StatusCode::CREATED,
    )
    .await;
    ingest(
        &router,
        "left_source",
        1,
        json!([{"item":1,"reading":10,"delta":-1},{"item":1,"reading":11,"delta":1}]),
        StatusCode::CREATED,
    )
    .await;
    ingest(
        &router,
        "left_source",
        3,
        json!([{"item":1,"reading":11,"delta":-1},{"item":1,"reading":12,"delta":1}]),
        StatusCode::CREATED,
    )
    .await;
    ingest(
        &router,
        "right_source",
        0,
        json!([{"item":1,"reading":20,"delta":1}]),
        StatusCode::CREATED,
    )
    .await;
    let (_, left_head) = checkpoint(&state, "left_stage").await;
    let (_, right_head) = checkpoint(&state, "right_stage").await;
    assert_ne!(
        left_head.checkpoint.logical_epoch,
        right_head.checkpoint.logical_epoch
    );
    let sql = if join {
        "SELECT l.item, l.reading AS left_reading, r.reading AS right_reading FROM left_stage l JOIN right_stage r ON l.item = r.item"
    } else {
        "SELECT item, reading FROM left_stage UNION ALL SELECT item, reading FROM right_stage"
    };
    post(&router,"/v1/views",json!({"view_id":"combined","input_relation_refs":[
        {"relation_id":"left_stage","relation_version":"v1"},{"relation_id":"right_stage","relation_version":"v1"}],
        "sql":sql,"response_formats":["json"]}),StatusCode::CREATED).await;
    post(
        &router,
        "/v1/views/combined/backfill",
        json!({"batch_limit":10}),
        StatusCode::OK,
    )
    .await;
    let (active, baseline) = checkpoint(&state, "combined").await;
    assert_eq!(baseline.checkpoint.logical_epoch, 1);
    let inputs = consumer_direct_view_inputs(&active, &baseline).unwrap();
    assert_eq!(inputs.len(), 2);
    let api_router = app(state
        .clone()
        .with_service_role(worker_role::ServiceRole::Api, Vec::new(), 1)
        .unwrap());
    meta.reset_active_view_snapshot_reads();
    assert_rows(&api_router, rows(join, 12, &[20])).await;
    assert_eq!(
        meta.active_view_snapshot_reads(),
        1,
        "API query uses its fresh root registry snapshot for producer bindings"
    );

    meta.reset_active_view_snapshot_reads();
    let (scope_status, _) = post_unchecked(
        &api_router,
        "/v1/views/combined/query",
        json!({"materialization":{"scope":{"where":"item = 1"}}}),
    )
    .await;
    assert_ne!(scope_status, StatusCode::OK);

    let producer_snapshot = ActiveViewReadSnapshot::load_demand_closure(&state, "combined")
        .await
        .unwrap();
    let mut mismatched_root =
        demand_materialization::demand_root_from_snapshot(&active, &producer_snapshot).unwrap();
    let runtime = mismatched_root.runtime.as_mut().unwrap();
    let published_binding = runtime
        .input_bindings
        .iter_mut()
        .find_map(|input| match input {
            StandingInputBindingV1::PublishedView {
                published_relation, ..
            } => Some(published_relation),
            _ => None,
        })
        .unwrap();
    published_binding.producer_plan_hash.push_str("-mismatch");
    let error = demand_materialization::demand_dependency_order_from_snapshot(
        &state,
        &mismatched_root,
        &producer_snapshot,
    )
    .await
    .unwrap_err();
    assert!(error
        .message
        .contains("producer generation or schema differs"));

    // Eviction is recoverable through targeted reload. Fail only this consumer's
    // checkpoint publication so source/producer commits succeed while its durable
    // cursor remains behind. Recovery must traverse both producer commits.
    let consumer_checkpoint_prefix = format!(
        "v1/standing-runtime-checkpoints/{}/{}/{}/epochs/",
        baseline.checkpoint.identity.tenant_id, baseline.checkpoint.identity.program_id, "combined",
    );
    assert!(baseline
        .checkpoint_key
        .starts_with(&consumer_checkpoint_prefix));
    remove_standing_runtime(
        &state,
        active_standing_runtime_identity(&active).unwrap(),
        "combined",
    )
    .unwrap();
    failing_store.arm(&consumer_checkpoint_prefix);
    ingest(
        &router,
        "left_source",
        5,
        json!([{"item":1,"reading":12,"delta":-1},{"item":1,"reading":13,"delta":1}]),
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    failing_store.arm(&consumer_checkpoint_prefix);
    ingest(
        &router,
        "left_source",
        7,
        json!([{"item":1,"reading":13,"delta":-1},{"item":1,"reading":14,"delta":1}]),
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert_eq!(
        checkpoint(&state, "combined").await.1.checkpoint_key,
        baseline.checkpoint_key,
        "failed consumer publication must not advance the durable baseline"
    );
    meta.reset_active_view_snapshot_reads();
    let (api_lag_status, _) =
        post_unchecked(&api_router, "/v1/views/combined/query", json!({})).await;
    assert_ne!(api_lag_status, StatusCode::OK);
    let lag = post(
        &router,
        "/v1/views/combined/query",
        json!({}),
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;
    assert_eq!(lag["details"]["code"], "MATERIALIZATION_LAG");
    ensure_standing_runtime_for_active_view(&state, &active)
        .await
        .unwrap();
    drain_published_view_dependencies(&state).await.unwrap();
    let (_, after_left) = checkpoint(&state, "combined").await;
    assert_eq!(after_left.checkpoint.logical_epoch, 3);
    let right_cursor = inputs
        .iter()
        .find(|input| input.cursor.producer_view_id == "right_stage")
        .unwrap();
    assert_eq!(
        consumer_edge_cursor(&after_left, &right_cursor.cursor.input_edge)
            .unwrap()
            .unwrap(),
        right_cursor.cursor
    );
    assert_rows(&router, rows(join, 14, &[20])).await;

    // Repeat an already consumed historical edge commit; consumer epoch and
    // output must not change, including when its local epoch is smaller.
    let left_input = inputs
        .iter()
        .find(|input| input.cursor.producer_view_id == "left_stage")
        .unwrap();
    let commits = read_validated_producer_commits_after_cursor(
        &state,
        &left_input.published_relation,
        &left_input.cursor,
    )
    .await
    .unwrap();
    assert_eq!(commits.len(), 2);
    apply_validated_producer_commit_to_consumer(
        &state,
        &active,
        &left_input.published_relation,
        &left_input.cursor.input_edge,
        &left_input.cursor,
        &commits[0],
    )
    .await
    .unwrap();
    assert_eq!(
        checkpoint(&state, "combined")
            .await
            .1
            .checkpoint
            .logical_epoch,
        3
    );
    ingest(
        &router,
        "right_source",
        1,
        json!([{"item":1,"reading":21,"delta":1}]),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(
        checkpoint(&state, "combined")
            .await
            .1
            .checkpoint
            .logical_epoch,
        4
    );
    assert_rows(&router, rows(join, 14, &[20, 21])).await;

    let mut previous_cursor = left_input.cursor.clone();
    previous_cursor.output_epoch = commits[0].logical_epoch;
    previous_cursor.commit_digest = commits[0].commit_digest.clone();
    let mut conflicting_commit = commits[1].clone();
    conflicting_commit.commit_digest = stable_bytes_hash(b"conflicting-commit");
    assert!(apply_validated_producer_commit_to_consumer(
        &state,
        &active,
        &left_input.published_relation,
        &left_input.cursor.input_edge,
        &previous_cursor,
        &conflicting_commit,
    )
    .await
    .is_err());

    // The final producer advances after the other edge is already current.
    // Fail consumer publication again: its complete-but-stale durable cut must
    // remain unavailable until restart drains it.
    remove_standing_runtime(
        &state,
        active_standing_runtime_identity(&active).unwrap(),
        "combined",
    )
    .unwrap();
    failing_store.arm(&consumer_checkpoint_prefix);
    ingest(
        &router,
        "right_source",
        2,
        json!([{"item":1,"reading":20,"delta":-1}]),
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert_eq!(
        checkpoint(&state, "combined")
            .await
            .1
            .checkpoint
            .logical_epoch,
        4,
        "failed publication must preserve the pre-restart consumer epoch"
    );
    let lag = post(
        &router,
        "/v1/views/combined/query",
        json!({}),
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;
    assert_eq!(lag["details"]["code"], "MATERIALIZATION_LAG");
    let combined_identity = active_standing_runtime_identity(&active).unwrap().clone();
    drop(router);
    drop(state);
    let expiry_wait = std::time::Duration::from_millis(OWNER_TTL_MS + OWNER_TTL_MS / 3 + 1_000);
    tokio::time::timeout(expiry_wait, async {
        loop {
            if meta
                .read_standing_runtime_owner(
                    &combined_identity.tenant_id,
                    &combined_identity.program_id,
                    "combined",
                )
                .await
                .expect("read previous owner while waiting for expiry")
                .is_none()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("previous owner must expire after its renewal worker stops");
    let restarted = crate::tests::with_initialized_active_view_meta_store(
        crate::tests::test_api_state_with_store(store, "dag-owner-b", true).await,
        meta,
    )
    .await
    .with_standing_runtime_owner_ttl_ms(OWNER_TTL_MS);
    assert_eq!(
        restarted
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        3
    );
    let restored_router = app(restarted.clone());
    assert_rows(&restored_router, rows(join, 14, &[21])).await;
    let (restored_active, restored_checkpoint) = checkpoint(&restarted, "combined").await;
    assert_eq!(
        consumer_direct_view_inputs(&restored_active, &restored_checkpoint)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        drain_published_view_dependencies(&restarted)
            .await
            .unwrap()
            .applied_commits,
        0
    );
    ingest(
        &restored_router,
        "left_source",
        9,
        json!([{"item":1,"reading":14,"delta":-1},{"item":1,"reading":15,"delta":1}]),
        StatusCode::CREATED,
    )
    .await;
    assert_rows(&restored_router, rows(join, 15, &[21])).await;
}

#[tokio::test]
async fn two_producer_union_preserves_complete_cut_through_replay_and_restart() {
    two_producer_dag(false).await;
}

#[tokio::test]
async fn two_producer_join_preserves_complete_cut_through_replay_and_restart() {
    two_producer_dag(true).await;
}
