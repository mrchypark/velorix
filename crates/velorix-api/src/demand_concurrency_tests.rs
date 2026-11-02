use super::*;

struct DemandSourceGetGateStore {
    inner: Arc<dyn ObjectStore>,
    armed: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    source_gets: Arc<std::sync::atomic::AtomicUsize>,
}

impl std::fmt::Debug for DemandSourceGetGateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DemandSourceGetGateStore")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for DemandSourceGetGateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DemandSourceGetGateStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for DemandSourceGetGateStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        if !options.head && location.as_ref().starts_with("v1/ingest-staging/") {
            self.source_gets
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
        }
        self.inner.get_opts(location, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

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
            {"user_id":"a","score":3},
            {"user_id":"b","score":5}
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
    ingest(&router, 0, json!([{"user_id":"a","score":7}])).await;
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
async fn exhausted_compute_capacity_returns_pending_without_starting_materialization() {
    let (state, router, _store, _meta, counts) = fixture().await;
    ingest(&router, 0, json!([{"user_id":"a","score":7}])).await;
    declare(
        &router,
        "capacity_bounded_output",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
    )
    .await;
    let slots = (0..state.max_compute_jobs)
        .map(|_| state.try_compute_slot().unwrap())
        .collect::<Vec<_>>();
    counts.clear();

    let response = post(
        &router,
        "/v1/views/capacity_bounded_output/query",
        demand_body(None, 64),
    )
    .await;
    assert_pending(&response, 1);
    assert!(
        counts
            .get_paths()
            .iter()
            .all(|path| !path.starts_with("v1/ingest-staging/")),
        "capacity-rejected request started source preparation: {:?}",
        counts.get_paths()
    );
    assert_eq!(state.compute_slots.available_permits(), 0);

    drop(slots);
    assert_eq!(
        demand(&router, "capacity_bounded_output", None, 64).await,
        vec![json!({"user_id":"a","total":7})]
    );
}

#[tokio::test]
async fn timed_out_precommit_source_load_finishes_once_and_later_query_uses_checkpoint() {
    let armed = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let source_gets = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let store: Arc<dyn ObjectStore> = Arc::new(DemandSourceGetGateStore {
        inner: Arc::new(InMemory::new()),
        armed: armed.clone(),
        entered: entered.clone(),
        release: release.clone(),
        source_gets: source_gets.clone(),
    });
    let meta = Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let state = test_authoritative_api_state_with_store(store, "precommit-owner", meta).await;
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
    let registered = post(
        &router,
        "/v1/relations",
        public_relation_registration(&catalog),
    )
    .await;
    assert_eq!(registered.0, StatusCode::CREATED, "{registered:?}");
    ingest(&router, 0, json!([{"user_id":"a","score":7}])).await;
    declare(
        &router,
        "precommit_timeout_output",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
    )
    .await;
    let active = tests::read_test_active_view(&state, "precommit_timeout_output")
        .await
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap().clone();
    let operation = state
        .standing_runtime_operation_lock(&identity, "precommit_timeout_output")
        .unwrap();
    let mut body = demand_body(None, 64);
    body["materialization"]["budget"]["max_duration_ms"] = json!(10);
    armed.store(true, std::sync::atomic::Ordering::SeqCst);

    let first_router = router.clone();
    let first = tokio::spawn(async move {
        post(
            &first_router,
            "/v1/views/precommit_timeout_output/query",
            body,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("demand did not enter the pre-commit source load");
    let pending = tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .expect("HTTP request did not return at its deadline")
        .unwrap();
    assert_pending(&pending, 1);
    assert!(
        operation.clone().try_lock_owned().is_err(),
        "timed-out job released its view lock"
    );
    assert_eq!(
        state.compute_slots.available_permits(),
        state.max_compute_jobs - 1,
        "timed-out job released its compute slot"
    );

    release.notify_one();
    let completed_job = tokio::time::timeout(Duration::from_secs(5), operation.lock_owned())
        .await
        .expect("detached materialization did not release its view lock");
    drop(completed_job);
    tokio::time::timeout(Duration::from_secs(5), async {
        while state.compute_slots.available_permits() != state.max_compute_jobs {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("finished materialization did not release its compute slot");
    assert_eq!(
        demand(&router, "precommit_timeout_output", None, 64).await,
        vec![json!({"user_id":"a","total":7})]
    );
    let record = checkpoint(&state, "precommit_timeout_output")
        .await
        .expect("detached pre-commit job did not publish its checkpoint");
    assert!(record
        .demand_materialization
        .as_ref()
        .is_some_and(|progress| progress.full_complete));
    assert_eq!(source_gets.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        state.compute_slots.available_permits(),
        state.max_compute_jobs
    );
}

#[tokio::test]
async fn api_role_reads_completed_scope_without_a_worker_or_local_runtime() {
    let (state, router, store, meta, counts) = fixture().await;
    ingest(
        &router,
        0,
        json!([
            {"user_id":"a","score":3},
            {"user_id":"b","score":5}
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
        ingest(&router, 0, json!([{"user_id":"a","score":7}])).await;
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
