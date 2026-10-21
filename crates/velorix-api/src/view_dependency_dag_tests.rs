use super::*;
use axum::http::Method;
use tower::ServiceExt;
use velorix_core::relation::*;

async fn post(router: &Router, path: &str, body: Value, expected: StatusCode) -> Value {
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
    assert_eq!(status, expected, "{path}: {value}");
    value
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
    let active = state
        .view_registry()
        .unwrap()
        .read_active(view)
        .await
        .unwrap();
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

async fn two_producer_dag(join: bool) {
    let failing_store = Arc::new(crate::tests::ArmedPrefixFailingObjectStore::new(Arc::new(
        object_store::memory::InMemory::new(),
    )));
    let store: Arc<dyn ObjectStore> = failing_store.clone();
    let meta = Arc::new(velorix_control::meta_admin::InMemoryMetaStore::default());
    let state = crate::tests::test_api_state_with_store(store.clone(), "dag-owner-a", false)
        .await
        .with_meta_store(meta.clone())
        .with_standing_runtime_owner_ttl_ms(1_000);
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
    assert_rows(&router, rows(join, 12, &[20])).await;

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
    drop(router);
    drop(state);
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let restarted = crate::tests::test_api_state_with_store(store, "dag-owner-b", true)
        .await
        .with_meta_store(meta)
        .with_standing_runtime_owner_ttl_ms(1_000);
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
