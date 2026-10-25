use super::{bounded_demand_requests::post, *};

// Child of tests.rs so the authoritative fixture is shared, not reimplemented.
#[tokio::test]
async fn authoritative_staged_families_backfill_and_restart_preserve_rows() {
    async fn rows(router: &Router, view: &str) -> Vec<Value> {
        let response = post(
            router,
            &format!("/v1/views/{view}/query"),
            json!({}),
            StatusCode::OK,
        )
        .await;
        let mut rows = response["rows"].as_array().unwrap().clone();
        rows.sort_by_key(Value::to_string);
        rows
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta: Arc<dyn MetaStore> =
        Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let state = test_authoritative_api_state_with_store(store.clone(), "stage-owner", meta.clone())
        .await
        .with_experimental_advanced_view_features(true);
    let router = app(state.clone());
    post(
        &router,
        "/v1/relations",
        json!({"catalog":test_scores_catalog()}),
        StatusCode::CREATED,
    )
    .await;
    post(
        &router,
        "/v1/relations/scores/ingest",
        json!({
        "relation_version":"2026-05-24.v1", "stream_id":"stage-stream", "partition_id":0,
        "start_offset_inclusive":0,"rows":[
            {"user_id":"a","score":2,"delta":1},
            {"user_id":"b","score":3,"delta":1},
            {"user_id":"c","score":5,"delta":1}
        ]}),
        StatusCode::CREATED,
    )
    .await;
    let stages = [
        (
            "stage_map",
            vec![("scores", "2026-05-24.v1")],
            "SELECT user_id, score, to_timestamp(score) AS ts FROM scores WHERE score IS NOT NULL",
            3,
        ),
        (
            "stage_ordered",
            vec![("stage_map", "v1")],
            "SELECT user_id, score, LAG(score) OVER (ORDER BY ts) AS previous FROM stage_map",
            3,
        ),
        (
            "stage_series",
            vec![("stage_map", "v1")],
            "SELECT m.user_id, m.score, s.tick FROM stage_map m CROSS JOIN LATERAL generate_series(1, 2, 1) AS s(tick)",
            6,
        ),
        (
            "stage_union",
            vec![("stage_map", "v1"), ("stage_series", "v1")],
            "SELECT user_id, score FROM stage_map UNION ALL SELECT user_id, score FROM stage_series",
            9,
        ),
        (
            "stage_groups",
            vec![("stage_union", "v1")],
            "SELECT user_id, SUM(score) AS total, COUNT(*) AS n FROM stage_union GROUP BY user_id",
            3,
        ),
        (
            "stage_join",
            vec![("stage_map", "v1"), ("stage_groups", "v1")],
            "SELECT m.user_id, m.score, g.total, g.n FROM stage_map m JOIN stage_groups g ON m.user_id = g.user_id",
            3,
        ),
    ];
    let mut snapshots = Vec::new();
    for (view, inputs, sql, expected_count) in &stages {
        let refs = inputs
            .iter()
            .map(|(id, version)| json!({"relation_id":id,"relation_version":version}))
            .collect::<Vec<_>>();
        post(&router, "/v1/views", json!({"view_id":view,"input_relation_refs":refs,"sql":sql,"response_formats":["json"]}), StatusCode::CREATED).await;
        let active = tests::read_test_active_view(&state, view).await.unwrap();
        assert!(
            read_latest_standing_runtime_checkpoint_header(
                &state,
                active_standing_runtime_identity(&active).unwrap(),
                view,
            )
            .await
            .unwrap()
            .is_none(),
            "declaration must not backfill {view}"
        );
        // Physical inputs need source cuts; published inputs need producer cursors.
        // Calling the same endpoint twice must neither confuse them nor replay rows.
        for _ in 0..2 {
            post(
                &router,
                &format!("/v1/views/{view}/backfill"),
                json!({"batch_limit":10}),
                StatusCode::OK,
            )
            .await;
        }
        let actual = rows(&router, view).await;
        assert_eq!(actual.len(), *expected_count, "{view}");
        snapshots.push(actual);
    }
    let mut expected_join = vec![
        json!({"user_id":"a","score":2,"total":6,"n":3}),
        json!({"user_id":"b","score":3,"total":9,"n":3}),
        json!({"user_id":"c","score":5,"total":15,"n":3}),
    ];
    expected_join.sort_by_key(Value::to_string);
    assert_eq!(snapshots[5], expected_join);
    let mut expected_ordered = vec![
        json!({"user_id":"a","score":2,"previous":null}),
        json!({"user_id":"b","score":3,"previous":2}),
        json!({"user_id":"c","score":5,"previous":3}),
    ];
    expected_ordered.sort_by_key(Value::to_string);
    assert_eq!(snapshots[1], expected_ordered);
    // This fixture restarts the same process; lease takeover has dedicated tests.
    let runtime_owner = state.owner_id.clone();
    drop(router);
    drop(state);
    let mut restored = test_authoritative_api_state_with_store(store, "stage-owner", meta)
        .await
        .with_experimental_advanced_view_features(true);
    restored.owner_id = runtime_owner;
    assert_eq!(
        restored
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        stages.len()
    );
    let router = app(restored);
    for ((view, _, _, _), expected) in stages.iter().zip(snapshots) {
        assert_eq!(rows(&router, view).await, expected, "restored {view}");
        post(
            &router,
            &format!("/v1/views/{view}/backfill"),
            json!({"batch_limit":10}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(rows(&router, view).await, expected, "backfilled {view}");
    }
}

#[tokio::test]
async fn authoritative_replay_skips_covered_payload_gets_but_rejects_uncovered_corruption() {
    let counts = ObjectStoreAccessCounts::default();
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> =
        Arc::new(CountingObjectStore::new(backing.clone(), counts.clone()));
    let meta: Arc<dyn MetaStore> =
        Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let state = test_authoritative_api_state_with_store(store, "skip-owner", meta.clone()).await;
    let router = app(state.clone());
    let catalog = test_scores_catalog();
    let response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog":catalog}),
    )
    .await;
    assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
    let response = call_json(&router, Method::POST, "/v1/views", json!({
        "view_id":"skip_total","input_relation_id":"scores","input_relation_version":"2026-05-24.v1",
        "sql":"SELECT user_id, SUM(score) AS total, COUNT(*) AS n FROM scores GROUP BY user_id"
    })).await;
    assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
    let mut first_plan = None;
    for offset in 0..2 {
        let response = call_json(
            &router,
            Method::POST,
            "/v1/relations/scores/ingest",
            json!({
                "relation_version":"2026-05-24.v1","stream_id":"skip-stream","partition_id":0,
                "start_offset_inclusive":offset,"rows":[{"user_id":"a","score":3,"delta":1}]
            }),
        )
        .await;
        assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
        let response = post(
            &router,
            "/v1/views/skip_total/backfill",
            json!({"batch_limit":10}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(response["materialization"]["complete"], true);
        if offset == 0 {
            let active = tests::read_test_active_view(&state, "skip_total")
                .await
                .unwrap();
            let identity = active_standing_runtime_identity(&active).unwrap();
            let record = read_latest_standing_runtime_checkpoint(&state, identity, "skip_total")
                .await
                .unwrap()
                .unwrap();
            first_plan = Some(standing_runtime_replay_plan_from_record_ref(&record));
        }
    }
    let active = tests::read_test_active_view(&state, "skip_total")
        .await
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let record = read_latest_standing_runtime_checkpoint(&state, identity, "skip_total")
        .await
        .unwrap()
        .unwrap();
    let latest_plan = standing_runtime_replay_plan_from_record_ref(&record);
    let cut = meta
        .capture_relation_ingest_source_cut(CaptureRelationIngestSourceCutRequest {
            authority: RelationPartitionAuthorityKey {
                namespace: "unit-authoritative".into(),
                relation_id: "scores".into(),
                stream_id: "skip-stream".into(),
                partition_id: 0,
            },
            relation_version: "2026-05-24.v1".into(),
            schema_fingerprint: catalog.schema_fingerprint.to_string(),
        })
        .await
        .unwrap();
    let publications = &cut.partitions[0].publications;
    assert_eq!(publications.len(), 2);
    counts.clear();
    let batches = recovery::read_replay_ingest_batches_with_coverage(
        &state,
        &active,
        &latest_plan,
        None,
        true,
    )
    .await
    .unwrap();
    assert!(batches.is_empty());
    assert!(!counts
        .get_paths()
        .iter()
        .any(|path| publications.iter().any(|p| p.object_key == *path)));
    counts.clear();
    let batches = recovery::read_replay_ingest_batches_with_coverage(
        &state,
        &active,
        first_plan.as_ref().unwrap(),
        None,
        true,
    )
    .await
    .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| **path == publications[0].object_key)
            .count(),
        0
    );
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| **path == publications[1].object_key)
            .count(),
        1
    );
    backing
        .put(
            &Path::from(publications[1].object_key.as_str()),
            bytes::Bytes::from_static(b"corrupt").into(),
        )
        .await
        .unwrap();
    counts.clear();
    let result = recovery::read_replay_ingest_batches_with_coverage(
        &state,
        &active,
        first_plan.as_ref().unwrap(),
        None,
        true,
    )
    .await;
    assert!(
        result.is_err(),
        "uncovered payload corruption must fail closed"
    );
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| **path == publications[1].object_key)
            .count(),
        1
    );
}
