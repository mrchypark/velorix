use super::*;

fn commit_gate() -> Arc<GuardRaceHook> {
    Arc::new(GuardRaceHook {
        reached: Notify::new(),
        release: Notify::new(),
        fire_once: AtomicBool::new(false),
        all_checkpoints: true,
    })
}

async fn wait_for_commit(gate: &GuardRaceHook) {
    tokio::time::timeout(Duration::from_secs(5), gate.reached.notified())
        .await
        .expect("owner must enter its atomic commit");
}

#[tokio::test]
async fn blocked_cold_owner_allows_warm_scope_and_other_view_to_complete() {
    let gate = commit_gate();
    let meta = Arc::new(
        DurableCapabilityMetaStore::new(InMemoryMetaStore::default()).with_guard_race(gate.clone()),
    );
    let (_state, router, _store, _meta, _counts) = fixture_with_meta(meta).await;
    ingest(
        &router,
        0,
        json!([
            {"user_id":"a","score":3,"delta":1},
            {"user_id":"b","score":5,"delta":1}
        ]),
    )
    .await;
    for view in ["scoped_output", "independent_output"] {
        declare(
            &router,
            view,
            &[("scores", "2026-05-24.v1")],
            "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
        )
        .await;
    }
    assert_eq!(
        demand(&router, "scoped_output", Some("user_id = 'a'"), 64).await,
        vec![json!({"user_id":"a","total":3})]
    );
    gate.fire_once.store(true, Ordering::SeqCst);
    let cold_router = router.clone();
    let cold = tokio::spawn(async move {
        post(
            &cold_router,
            "/v1/views/scoped_output/query",
            demand_body(Some("user_id = 'b'"), 64),
        )
        .await
    });
    wait_for_commit(&gate).await;

    let warm = tokio::time::timeout(
        Duration::from_secs(2),
        post(
            &router,
            "/v1/views/scoped_output/query",
            demand_body(Some("user_id = 'a'"), 64),
        ),
    )
    .await;
    let independent = tokio::time::timeout(
        Duration::from_secs(2),
        post(
            &router,
            "/v1/views/independent_output/query",
            demand_body(None, 64),
        ),
    )
    .await;
    // Release and join before asserting, including on regression failures.
    gate.release.notify_one();
    let cold = cold.await.unwrap();
    assert_eq!(cold.0, StatusCode::OK, "{cold:?}");
    let warm = warm.expect("warm scope waited for unrelated cold work");
    assert_eq!(warm.0, StatusCode::OK, "{warm:?}");
    assert_eq!(warm.1["rows"], json!([{"user_id":"a","total":3}]));
    let independent = independent.expect("different views share a calculation gate");
    assert_eq!(independent.0, StatusCode::OK, "{independent:?}");
}

#[tokio::test]
async fn duplicate_scope_prepares_once_and_short_wait_returns_pending() {
    let gate = commit_gate();
    let meta = Arc::new(
        DurableCapabilityMetaStore::new(InMemoryMetaStore::default()).with_guard_race(gate.clone()),
    );
    let (_state, router, _store, _meta, counts) = fixture_with_meta(meta).await;
    ingest(&router, 0, json!([{"user_id":"a","score":7,"delta":1}])).await;
    declare(
        &router,
        "bounded_output",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
    )
    .await;
    counts.clear();
    gate.fire_once.store(true, Ordering::SeqCst);
    let first_router = router.clone();
    let first = tokio::spawn(async move {
        post(
            &first_router,
            "/v1/views/bounded_output/query",
            demand_body(None, 64),
        )
        .await
    });
    wait_for_commit(&gate).await;
    let mut small = demand_body(None, 1);
    small["materialization"]["budget"]["max_duration_ms"] = json!(10);
    let pending = tokio::time::timeout(
        Duration::from_secs(2),
        post(&router, "/v1/views/bounded_output/query", small),
    )
    .await;
    let mut duplicate = Box::pin(post(
        &router,
        "/v1/views/bounded_output/query",
        demand_body(None, 64),
    ));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(duplicate.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    gate.release.notify_one();
    let (first, duplicate) = tokio::join!(first, duplicate);
    let first = first.unwrap();
    assert_eq!(first.0, StatusCode::OK, "{first:?}");
    assert_eq!(duplicate.0, StatusCode::OK, "{duplicate:?}");
    assert_eq!(duplicate.1["rows"], json!([{"user_id":"a","total":7}]));
    assert_pending(&pending.expect("short budget did not bound the wait"), 1);
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| path.starts_with("v1/ingest-staging/"))
            .count(),
        1,
        "duplicate requests prepared the same source twice"
    );
}

#[tokio::test]
async fn api_role_reads_completed_scope_without_a_worker_or_local_runtime() {
    let (state, router, store, meta, counts) = fixture().await;
    ingest(
        &router,
        0,
        json!([
            {"user_id":"a","score":3,"delta":1},
            {"user_id":"b","score":5,"delta":1}
        ]),
    )
    .await;
    declare(
        &router,
        "published_output",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
    )
    .await;
    demand(&router, "published_output", Some("user_id = 'a'"), 64).await;
    let api = test_authoritative_api_state_with_store(store, "query-replica", meta)
        .await
        .with_experimental_advanced_view_features(true)
        .with_service_role(
            worker_role::ServiceRole::Api,
            vec!["http://127.0.0.1:1".into()],
            1,
        )
        .unwrap();
    let active = tests::read_test_active_view(&state, "published_output")
        .await
        .unwrap();
    let request = demand_materialization::DemandRequest::default();
    let denied = demand_materialization::materialize_demand_step(
        &api,
        &active,
        &request,
        demand_materialization::DemandOrder::Forward,
        true,
        None,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(denied.status, StatusCode::ACCEPTED);
    counts.clear();
    let warm = post(
        &app(api.clone()),
        "/v1/views/published_output/query",
        demand_body(Some("user_id = 'a'"), 64),
    )
    .await;
    assert_eq!(warm.0, StatusCode::OK, "{warm:?}");
    assert_eq!(warm.1["rows"], json!([{"user_id":"a","total":3}]));
    assert!(api.standing_runtimes.runtimes.lock().unwrap().is_empty());
    assert!(counts
        .get_paths()
        .iter()
        .all(|path| !path.starts_with("v1/ingest-staging/")));
}

#[tokio::test]
async fn cancelled_demand_and_control_keep_worker_slot_through_atomic_commit() {
    for control in [false, true] {
        let gate = commit_gate();
        let meta = Arc::new(
            DurableCapabilityMetaStore::new(InMemoryMetaStore::default())
                .with_guard_race(gate.clone()),
        );
        let (state, router, _store, _meta, _counts) = fixture_with_meta(meta).await;
        ingest(&router, 0, json!([{"user_id":"a","score":7,"delta":1}])).await;
        declare(
            &router,
            "atomic_output",
            &[("scores", "2026-05-24.v1")],
            "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
        )
        .await;
        if control {
            demand(&router, "atomic_output", None, 64).await;
        }
        let worker = state
            .with_service_role(worker_role::ServiceRole::Worker, Vec::new(), 1)
            .unwrap();
        let active = tests::read_test_active_view(&worker, "atomic_output")
            .await
            .unwrap();
        let identity = active_standing_runtime_identity(&active).unwrap().clone();
        let operation = worker
            .standing_runtime_operation_lock(&identity, "atomic_output")
            .unwrap();
        let compute_slot = Some(worker.try_compute_slot().unwrap());
        gate.fire_once.store(true, Ordering::SeqCst);
        let task_worker = worker.clone();
        let task = tokio::spawn(async move {
            if control {
                demand_materialization::set_demand_control(
                    &task_worker,
                    &active,
                    true,
                    demand_materialization::DemandOrder::Forward,
                    compute_slot,
                )
                .await
            } else {
                demand_materialization::materialize_demand_step(
                    &task_worker,
                    &active,
                    &demand_materialization::DemandRequest::default(),
                    demand_materialization::DemandOrder::Forward,
                    true,
                    None,
                    compute_slot,
                )
                .await
                .map(|_| ())
            }
        });
        wait_for_commit(&gate).await;
        task.abort();
        let cancelled = task.await.unwrap_err().is_cancelled();
        let slot_retained = worker.try_compute_slot().is_err();
        gate.release.notify_one();
        let guard = tokio::time::timeout(Duration::from_secs(5), operation.lock_owned())
            .await
            .expect("detached commit did not finish");
        drop(guard);
        assert!(cancelled);
        assert!(
            slot_retained,
            "request cancellation released a running atomic unit's slot"
        );
        assert!(
            worker.try_compute_slot().is_ok(),
            "completed atomic unit leaked its worker slot"
        );
    }
}
