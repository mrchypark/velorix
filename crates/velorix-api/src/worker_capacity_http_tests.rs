use super::*;

const RPC_TOKEN: &str = "worker-capacity-http-test-token";

struct HttpServer {
    endpoint: String,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl HttpServer {
    fn start(listener: tokio::net::TcpListener, router: Router) -> Self {
        Self {
            endpoint: format!("http://{}", listener.local_addr().unwrap()),
            task: Some(tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            })),
        }
    }

    async fn stop(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn capacity_fixture() -> (ApiState, ApiState, HttpServer, HttpServer, String, u64) {
    let (_state, router, store, meta, _counts) = fixture().await;
    ingest(&router, 0, json!([{"user_id":"a","score":7,"delta":1}])).await;
    let view = "capacity_output";
    declare(
        &router,
        view,
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
    )
    .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let worker =
        test_authoritative_api_state_with_store(store.clone(), "capacity-worker", meta.clone())
            .await
            .with_experimental_advanced_view_features(true)
            .with_api_bearer_token(RPC_TOKEN)
            .unwrap()
            .with_owner_endpoint(format!("http://{}", listener.local_addr().unwrap()))
            .unwrap()
            .with_service_role(worker_role::ServiceRole::Worker, Vec::new(), 1)
            .unwrap();
    let active = tests::read_test_active_view(&worker, view).await.unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let owner = worker
        .acquire_standing_runtime_owner(identity, view)
        .await
        .unwrap()
        .unwrap();
    let worker_server = HttpServer::start(listener, app(worker.clone()));
    let api = test_authoritative_api_state_with_store(store, "capacity-api", meta)
        .await
        .with_experimental_advanced_view_features(true)
        .with_api_bearer_token(RPC_TOKEN)
        .unwrap()
        .with_service_role(
            worker_role::ServiceRole::Api,
            vec![worker_server.endpoint.clone()],
            1,
        )
        .unwrap();
    let api_server = HttpServer::start(
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        app(api.clone()),
    );
    (
        worker,
        api,
        worker_server,
        api_server,
        view.into(),
        owner.owner_epoch,
    )
}

#[tokio::test]
async fn worker_full_returns_public_scoped_query_pending_over_http() {
    let (worker, api, worker_server, api_server, view, _epoch) = capacity_fixture().await;
    let slot = worker.try_compute_slot().unwrap();
    let response = reqwest::Client::new()
        .post(format!("{}/v1/views/{view}/query", api_server.endpoint))
        .bearer_auth(RPC_TOKEN)
        .json(&demand_body(Some("user_id = 'a'"), 64))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.json::<Value>().await.unwrap();
    api_server.stop().await;
    worker_server.stop().await;
    drop(slot);
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["materialization"]["complete"], false, "{body}");
    assert!(body["materialization"]["status"].is_string(), "{body}");
    assert!(body.get("rows").is_none(), "{body}");
    assert!(api.standing_runtimes.runtimes.lock().unwrap().is_empty());
    assert!(worker.standing_runtimes.runtimes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn worker_full_preserves_each_pending_rpc_kind_and_validation_errors() {
    let (worker, _api, worker_server, api_server, view, epoch) = capacity_fixture().await;
    let slot = worker.try_compute_slot().unwrap();
    let active = tests::read_test_active_view(&worker, &view).await.unwrap();
    let mut results = Vec::new();
    for (operation, kind) in [
        (json!({"kind":"materialize"}), "materialization"),
        (json!({"kind":"control","paused":true}), "control"),
        (
            json!({"kind":"published_backfill","batch_limit":1}),
            "published_backfill",
        ),
    ] {
        let request = json!({
            "operation":operation,"scope":null,"budget":demand_budget(64),"order":"forward",
            "query_priority":false,"pinned_spec_hash":view_spec_hash(&active.spec).unwrap(),
            "expected_owner_epoch":epoch,"deadline_unix_ms":owner_routing::unix_now_ms()+10_000,
            "hop_count":1
        });
        let url = format!(
            "{}/internal/v1/views/{view}/demand-step",
            worker_server.endpoint
        );
        let response = reqwest::Client::new()
            .post(&url)
            .bearer_auth(RPC_TOKEN)
            .json(&request)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.json::<Value>().await.unwrap();
        results.push((status, body, kind));
    }
    let url = format!(
        "{}/internal/v1/views/{view}/demand-step",
        worker_server.endpoint
    );
    let malformed = reqwest::Client::new()
        .post(&url)
        .bearer_auth(RPC_TOKEN)
        .json(&json!({"unexpected":true}))
        .send()
        .await
        .unwrap()
        .status();
    let unauthenticated = reqwest::Client::new()
        .post(&url)
        .json(&json!({"unexpected":true}))
        .send()
        .await
        .unwrap()
        .status();
    api_server.stop().await;
    worker_server.stop().await;
    drop(slot);
    for (status, body, kind) in results {
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        assert_eq!(body["kind"], kind, "{body}");
    }
    assert_eq!(malformed, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(unauthenticated, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn malformed_success_rpc_is_not_hidden_as_public_pending() {
    assert_public_worker_error(StatusCode::ACCEPTED, StatusCode::BAD_GATEWAY).await;
}

#[tokio::test]
async fn worker_http_server_errors_are_preserved_by_public_query() {
    for status in [
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::GATEWAY_TIMEOUT,
    ] {
        assert_public_worker_error(status, status).await;
    }
}

async fn assert_public_worker_error(worker_status: StatusCode, expected_status: StatusCode) {
    let (worker, api, worker_server, api_server, view, _epoch) = capacity_fixture().await;
    api_server.stop().await;
    worker_server.stop().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let worker = worker.with_owner_endpoint(endpoint.clone()).unwrap();
    let active = tests::read_test_active_view(&worker, &view).await.unwrap();
    let _owner = worker
        .acquire_standing_runtime_owner(active_standing_runtime_identity(&active).unwrap(), &view)
        .await
        .unwrap();
    let corrupt = HttpServer::start(
        listener,
        Router::new().route(
            "/internal/v1/views/{view}/demand-step",
            axum::routing::post(move || async move {
                (worker_status, Json(json!({"error":"worker_error"})))
            }),
        ),
    );
    let api_server = HttpServer::start(
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        app(api),
    );
    let response = reqwest::Client::new()
        .post(format!("{}/v1/views/{view}/query", api_server.endpoint))
        .bearer_auth(RPC_TOKEN)
        .json(&demand_body(Some("user_id = 'a'"), 64))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.json::<Value>().await.unwrap();
    api_server.stop().await;
    corrupt.stop().await;
    assert_eq!(status, expected_status, "{body}");
}

#[tokio::test]
async fn restarted_endpoint_waits_for_foreign_lease_without_self_rpc_then_takes_over() {
    let durable = Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let (old, router, store, meta, counts) = fixture_with_meta(durable.clone()).await;
    ingest(&router, 0, json!([{"user_id":"a","score":7,"delta":1}])).await;
    let view = "restart_output";
    declare(
        &router,
        view,
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
    )
    .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let old = old.with_owner_endpoint(endpoint.clone()).unwrap();
    let active = tests::read_test_active_view(&old, view).await.unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let old_owner = old
        .acquire_standing_runtime_owner(identity, view)
        .await
        .unwrap()
        .unwrap();
    let worker =
        test_authoritative_api_state_with_store(store.clone(), "restart-worker", meta.clone())
            .await
            .with_experimental_advanced_view_features(true)
            .with_api_bearer_token(RPC_TOKEN)
            .unwrap()
            .with_owner_endpoint(endpoint.clone())
            .unwrap()
            .with_service_role(worker_role::ServiceRole::Worker, Vec::new(), 1)
            .unwrap();
    assert_ne!(worker.owner_id, old.owner_id);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = calls.clone();
    let worker_router = app(worker.clone()).layer(axum::middleware::from_fn(
        move |request: Request<Body>, next: axum::middleware::Next| {
            let counted = counted.clone();
            async move {
                if request.uri().path().ends_with("/demand-step") {
                    counted.fetch_add(1, Ordering::SeqCst);
                }
                next.run(request).await
            }
        },
    ));
    let worker_server = HttpServer::start(listener, worker_router);
    let api = test_authoritative_api_state_with_store(store, "restart-api", meta.clone())
        .await
        .with_experimental_advanced_view_features(true)
        .with_api_bearer_token(RPC_TOKEN)
        .unwrap()
        .with_service_role(worker_role::ServiceRole::Api, vec![endpoint.clone()], 1)
        .unwrap();
    let api_server = HttpServer::start(
        tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(),
        app(api),
    );
    let client = reqwest::Client::new();
    let backfill_url = format!("{}/v1/views/{view}/backfill", api_server.endpoint);
    counts.clear();
    for full in [false, true] {
        let slot = full.then(|| worker.try_compute_slot().unwrap());
        let before_calls = calls.load(Ordering::SeqCst);
        let response = client
            .post(&backfill_url)
            .bearer_auth(RPC_TOKEN)
            .json(&json!({"budget":demand_budget(64)}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.json::<Value>().await.unwrap();
        assert_eq!(status, StatusCode::ACCEPTED, "full={full}: {body}");
        assert_eq!(body["materialization"]["complete"], false, "{body}");
        assert_eq!(body["materialization"]["work_units"], 0, "{body}");
        assert_eq!(
            calls.load(Ordering::SeqCst) - before_calls,
            1,
            "worker RPC forwarded to itself"
        );
        // A genuinely stale internal epoch remains a conflict even when capacity is full.
        let stale = client
            .post(format!("{endpoint}/internal/v1/views/{view}/demand-step"))
            .bearer_auth(RPC_TOKEN)
            .json(&json!({
                "operation":{"kind":"materialize"},"scope":null,"budget":demand_budget(64),
                "order":"forward","query_priority":false,
                "pinned_spec_hash":view_spec_hash(&active.spec).unwrap(),
                "expected_owner_epoch":old_owner.owner_epoch+1,
                "deadline_unix_ms":owner_routing::unix_now_ms()+10_000,"hop_count":1
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        drop(slot);
    }
    assert!(
        counts
            .get_paths()
            .iter()
            .all(|path| !path.starts_with("v1/ingest-staging/")),
        "source preparation ran before lease expiry"
    );
    assert!(worker.standing_runtimes.runtimes.lock().unwrap().is_empty());
    assert!(meta
        .read_standing_runtime_checkpoint(&identity.tenant_id, &identity.program_id, view)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        standing_runtime_owner_token_from_claim(
            &meta
                .read_standing_runtime_owner(&identity.tenant_id, &identity.program_id, view)
                .await
                .unwrap()
                .unwrap()
        ),
        old_owner
    );
    durable
        .inner
        .expire_standing_runtime_owner_for_test(&identity.tenant_id, &identity.program_id, view)
        .await
        .unwrap();
    let response = client
        .post(&backfill_url)
        .bearer_auth(RPC_TOKEN)
        .json(&json!({"budget":demand_budget(64)}))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.json::<Value>().await.unwrap();
    let query = client
        .post(format!("{}/v1/views/{view}/query", api_server.endpoint))
        .header(axum::http::header::ACCEPT, "application/json")
        .bearer_auth(RPC_TOKEN)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let query_status = query.status();
    let rows = query.json::<Value>().await.unwrap();
    let owner = meta
        .read_standing_runtime_owner(&identity.tenant_id, &identity.program_id, view)
        .await
        .unwrap()
        .unwrap();
    api_server.stop().await;
    worker_server.stop().await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["materialization"]["complete"], true, "{body}");
    assert_eq!(owner.owner_id, worker.owner_id);
    assert!(owner.owner_epoch > old_owner.owner_epoch);
    assert_eq!(query_status, StatusCode::OK, "{rows}");
    assert_eq!(rows["rows"], json!([{"user_id":"a","total":7}]));
}
