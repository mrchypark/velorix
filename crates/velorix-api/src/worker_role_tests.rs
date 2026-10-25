use super::*;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use tower::ServiceExt;

const TOKEN: &str = "worker-role-test-authentication";

async fn state(role: ServiceRole) -> ApiState {
    crate::tests::test_api_state_with_store(Arc::new(InMemory::new()), "role-test", false)
        .await
        .with_api_bearer_token(TOKEN)
        .unwrap()
        .with_service_role(role, vec!["http://worker.invalid:8080".into()], 2)
        .unwrap()
}

fn available(slots: usize, owned: usize, resident: usize) -> WorkerStatus {
    WorkerStatus {
        role: ServiceRole::Worker,
        max_concurrent_jobs: 4,
        running_jobs: 4 - slots,
        available_slots: slots,
        resident_runtimes: resident,
        owned_views: owned,
        resident_limit: 32,
        available_runtime_capacity: 32usize.saturating_sub(resident),
    }
}

#[test]
fn roles_and_split_configuration_fail_closed() {
    assert_eq!(ServiceRole::parse(None).unwrap(), ServiceRole::All);
    assert_eq!(ServiceRole::parse(Some("api")).unwrap(), ServiceRole::Api);
    assert_eq!(
        ServiceRole::parse(Some("worker")).unwrap(),
        ServiceRole::Worker
    );
    assert!(ServiceRole::parse(Some("other")).is_err());
    let endpoints = parse_worker_endpoints(Some(
        "http://one.example:8080, https://two.example,http://one.example:8080",
    ))
    .unwrap();
    assert_eq!(endpoints.len(), 2);
    assert!(parse_worker_endpoints(Some("http://user@worker.example")).is_err());
    assert!(parse_worker_endpoints(Some(
        &vec!["http://worker.example"; 33]
            .into_iter()
            .enumerate()
            .map(|(index, _)| format!("http://worker-{index}.example"))
            .collect::<Vec<_>>()
            .join(",")
    ))
    .is_err());
    for role in [ServiceRole::Api, ServiceRole::Worker] {
        assert!(validate_split_config(role, &endpoints, true, true).is_ok());
        assert!(validate_split_config(role, &endpoints, false, true).is_err());
        assert!(validate_split_config(role, &endpoints, true, false).is_err());
    }
    assert!(validate_split_config(ServiceRole::Api, &[], true, true).is_err());
    assert!(validate_split_config(ServiceRole::All, &[], false, false).is_ok());
    assert_eq!(ServiceRole::Api.compute_replica_count(9, 2), 0);
    assert_eq!(ServiceRole::Worker.compute_replica_count(9, 2), 2);
    assert_eq!(ServiceRole::All.compute_replica_count(9, 2), 9);
}

#[test]
fn worker_capacity_ranks_slots_then_ownership_and_count_headroom() {
    let mut candidates = vec![
        ("first".into(), available(4, 10, 1)),
        ("second".into(), available(4, 1, 2)),
        ("busy".into(), available(1, 0, 0)),
    ];
    order_candidates(&mut candidates);
    assert_eq!(candidates[0].0, "second");
    assert_eq!(candidates[2].0, "busy");
    assert!(!valid_status(&available(0, 0, 0)));
    let mut forged = available(4, 0, 0);
    forged.running_jobs = 1;
    assert!(!valid_status(&forged));
    forged = available(4, 0, 0);
    forged.role = ServiceRole::Api;
    assert!(!valid_status(&forged));
    // Resident capacity is a count preference, not a claimed memory-byte limit.
    assert!(valid_status(&available(4, 0, 33)));
}

#[tokio::test]
async fn equal_idle_workers_rotate_for_sequential_declarations() {
    let state = state(ServiceRole::Api)
        .await
        .with_service_role(
            ServiceRole::Api,
            vec!["http://one.example".into(), "http://two.example".into()],
            2,
        )
        .unwrap();
    let first = rotated_endpoints(&state)[0].clone();
    let second = rotated_endpoints(&state)[0].clone();
    assert_ne!(first, second);
    assert_eq!(first, *rotated_endpoints(&state)[0]);
}

#[tokio::test]
async fn api_cannot_acquire_slots_or_compute_ownership() {
    let state = state(ServiceRole::Api).await;
    assert!(!state.allows_local_materialization());
    assert_eq!(
        state.try_compute_slot().unwrap_err().status,
        StatusCode::ACCEPTED
    );
    let checkpoint = crate::tests::test_runtime_checkpoint(Vec::new());
    let error = state
        .acquire_standing_runtime_owner(&checkpoint.identity, "view")
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::ACCEPTED);
    assert!(remote_owner(&state, &checkpoint.identity, "view")
        .await
        .unwrap()
        .is_none());
    assert!(state.standing_runtimes.runtimes.lock().unwrap().is_empty());
    assert!(state
        .standing_runtimes
        .local_state
        .lock()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn full_worker_rejects_claim_before_metadata_or_preparation() {
    let state = state(ServiceRole::Worker).await;
    let first = state.try_compute_slot().unwrap();
    let second = state.try_compute_slot().unwrap();
    let before = state.standing_runtimes.local_state.lock().unwrap().len();
    let error = claim(
        State(state.clone()),
        Json(ClaimRequest {
            view_id: "unregistered".into(),
            pinned_spec_hash: "unregistered".into(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.status, StatusCode::ACCEPTED);
    assert_eq!(
        before,
        state.standing_runtimes.local_state.lock().unwrap().len()
    );
    let Json(status) = status(State(state.clone())).await.unwrap();
    assert_eq!(status.running_jobs, 2);
    assert_eq!(status.available_slots, 0);
    drop((first, second));
    assert_eq!(state.compute_slots.available_permits(), 2);
}

#[tokio::test]
async fn explicit_slot_arc_outlives_cancelled_waiter() {
    let state = state(ServiceRole::Worker)
        .await
        .with_service_role(ServiceRole::Worker, Vec::new(), 1)
        .unwrap();
    let slot = state.try_compute_slot().unwrap();
    let (started, ready) = tokio::sync::oneshot::channel();
    let (finish, finished) = tokio::sync::oneshot::channel();
    let commit_slot = slot.clone();
    let commit = tokio::spawn(async move {
        let _slot = commit_slot;
        started.send(()).unwrap();
        finished.await.unwrap();
    });
    ready.await.unwrap();
    drop(slot);
    assert_eq!(
        state.try_compute_slot().unwrap_err().status,
        StatusCode::ACCEPTED
    );
    finish.send(()).unwrap();
    commit.await.unwrap();
    assert_eq!(state.compute_slots.available_permits(), 1);
}

#[tokio::test]
async fn internal_status_requires_auth_and_worker_public_api_is_closed() {
    let state = state(ServiceRole::Worker).await;
    let router = app(state);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/v1/worker/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/v1/worker/status")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/v1/views")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

async fn stored_objects(store: &Arc<dyn ObjectStore>) -> Vec<(String, u64)> {
    let mut objects = store
        .list(None)
        .map_ok(|meta| (meta.location.to_string(), meta.size))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    objects.sort();
    objects
}

async fn eager_fixture() -> (
    ApiState,
    Arc<dyn ObjectStore>,
    VelorixRelationCatalogV1,
    String,
) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = crate::tests::test_api_state_with_store(store.clone(), "eager-owner", false).await;
    let catalog = crate::example_fixtures::default_scores_relation_catalog().unwrap();
    let (status, _) = create_relation(
        State(state.clone()),
        Json(CreateRelationRequest {
            catalog: catalog.clone(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CREATED);
    let request = crate::example_fixtures::default_positive_scores_view_request(&catalog).unwrap();
    let view_id = request.view_id.clone();
    assert_eq!(
        create_view(State(state.clone()), Json(request))
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );
    let (_, Json(ack)) = ingest_epoch(State(state.clone()), Json(ingest_request(&catalog, 0)))
        .await
        .unwrap();
    assert_eq!(ack.ack_mode, IngestAckMode::Materialized);
    assert_eq!(ack.materialization.status, "completed");
    assert!(ack.materialized_through.is_some());
    (state, store, catalog, view_id)
}

fn ingest_request(catalog: &VelorixRelationCatalogV1, offset: u64) -> IngestEpochRequest {
    IngestEpochRequest {
        batches: vec![IngestRowsRequest {
            relation_id: catalog.relation_schema.relation_id.clone(),
            relation_version: catalog.relation_schema.relation_version.clone(),
            stream_id: "role-test".into(),
            partition_id: 0,
            start_offset_inclusive: offset,
            event_time_watermark: None,
            rows: vec![json!({"user_id":"item","score":7,"delta":1})],
        }],
    }
}

#[tokio::test]
async fn split_legacy_and_eager_refusal_precedes_every_durable_write() {
    let (original, store, catalog, view_id) = eager_fixture().await;
    let meta = original.meta_store.as_ref().unwrap().clone();
    let active = view_dependencies::read_active_from_meta(&original, &view_id)
        .await
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let before_pointer = meta
        .read_standing_runtime_checkpoint(&identity.tenant_id, &identity.program_id, &view_id)
        .await
        .unwrap();
    let cut_request = CaptureRelationIngestSourceCutsRequest {
        namespace: "unit".into(),
        relations: vec![IngestSourceRelationIdentityV1 {
            relation_id: catalog.relation_schema.relation_id.clone(),
            relation_version: catalog.relation_schema.relation_version.clone(),
            relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
            schema_fingerprint: catalog.schema_fingerprint.as_str().into(),
        }],
    };
    let before_cut = serde_json::to_value(
        meta.capture_relation_ingest_source_cuts(cut_request.clone())
            .await
            .unwrap(),
    )
    .unwrap();
    let before_objects = stored_objects(&store).await;
    for role in [ServiceRole::Api, ServiceRole::Worker] {
        for authoritative in [false, true] {
            let mut state = original
                .clone()
                .with_service_role(role, Vec::new(), 2)
                .unwrap();
            if authoritative {
                state = state
                    .with_authoritative_relation_ingest(
                        "input-owner",
                        "unit",
                        "test",
                        "memory",
                        "probe",
                        30_000,
                    )
                    .unwrap();
            }
            let state = state.with_api_bearer_token(TOKEN).unwrap();
            let response = app(state.clone())
                .oneshot(
                    Request::builder()
                        .method(axum::http::Method::POST)
                        .uri("/v1/relations/ingest")
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                        .body(Body::from(
                            serde_json::to_vec(&ingest_request(&catalog, 1)).unwrap(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{role:?} authoritative={authoritative}"
            );
            let error = ingest_epoch(State(state), Json(ingest_request(&catalog, 1)))
                .await
                .unwrap_err();
            assert_eq!(
                error.status,
                StatusCode::BAD_REQUEST,
                "{role:?} authoritative={authoritative}: {error}"
            );
            assert_eq!(stored_objects(&store).await, before_objects);
            assert_eq!(
                serde_json::to_value(
                    meta.capture_relation_ingest_source_cuts(cut_request.clone())
                        .await
                        .unwrap()
                )
                .unwrap(),
                before_cut
            );
            assert_eq!(
                meta.read_standing_runtime_checkpoint(
                    &identity.tenant_id,
                    &identity.program_id,
                    &view_id
                )
                .await
                .unwrap(),
                before_pointer
            );
        }
    }
}

#[tokio::test]
async fn api_restart_preserves_legacy_declarations_and_checkpoints_without_owners() {
    let (original, store, _catalog, view_id) = eager_fixture().await;
    let before_objects = stored_objects(&store).await;
    let meta = original.meta_store.as_ref().unwrap().clone();
    let before = serde_json::to_value(
        meta.read_active_view_registry_snapshot(ActiveViewRegistryReadRequestV1 {
            scope: view_dependencies::active_view_registry_scope(&original),
        })
        .await
        .unwrap(),
    )
    .unwrap();
    let state = crate::tests::test_api_state_with_store(store.clone(), "api-restart", false)
        .await
        .with_meta_store(meta.clone())
        .with_view_bootstrap_meta_store(meta.clone())
        .with_service_role(ServiceRole::Api, Vec::new(), 2)
        .unwrap();
    assert_eq!(
        state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    assert!(state.standing_runtimes.runtimes.lock().unwrap().is_empty());
    assert!(state
        .standing_runtimes
        .local_state
        .lock()
        .unwrap()
        .is_empty());
    assert_eq!(
        view_dependencies::read_active_from_meta(&state, &view_id)
            .await
            .unwrap()
            .spec_hash,
        view_dependencies::read_active_from_meta(&original, &view_id)
            .await
            .unwrap()
            .spec_hash
    );
    assert_eq!(stored_objects(&store).await, before_objects);
    assert_eq!(
        serde_json::to_value(
            meta.read_active_view_registry_snapshot(ActiveViewRegistryReadRequestV1 {
                scope: view_dependencies::active_view_registry_scope(&state),
            })
            .await
            .unwrap()
        )
        .unwrap(),
        before
    );
    let Json(readiness) = readyz(State(state)).await.unwrap();
    assert_eq!(readiness["local_materialization"], false);
    assert!(readiness["split_legacy_compute"]
        .as_str()
        .unwrap()
        .contains("unsupported"));
}

#[tokio::test]
async fn split_boot_accepts_an_uninitialized_registry_without_restoring_state() {
    for role in [ServiceRole::Api, ServiceRole::Worker] {
        let (state, _meta) = crate::tests::test_api_state_with_uninitialized_active_registry(
            Arc::new(InMemory::new()),
            "fresh-role",
        )
        .await;
        let state = state.with_service_role(role, Vec::new(), 1).unwrap();
        assert_eq!(
            state
                .restore_standing_program_runtimes_from_active_views()
                .await
                .unwrap(),
            0
        );
        assert!(state.standing_runtimes.runtimes.lock().unwrap().is_empty());
        assert!(state
            .standing_runtimes
            .local_state
            .lock()
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn all_role_eager_ack_is_independent_of_worker_rpc_capacity() {
    let (state, _store, catalog, view_id) = eager_fixture().await;
    let slot = state
        .compute_slots
        .clone()
        .acquire_many_owned(state.max_compute_jobs as u32)
        .await
        .unwrap();
    let (_, Json(ack)) = ingest_epoch(State(state.clone()), Json(ingest_request(&catalog, 1)))
        .await
        .unwrap();
    assert_eq!(ack.ack_mode, IngestAckMode::Materialized);
    assert_eq!(ack.materialization.status, "completed");
    let response = app(state)
        .oneshot(
            Request::builder()
                .method(axum::http::Method::POST)
                .uri(format!("/v1/views/{view_id}/query"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    drop(slot);
}

#[tokio::test]
async fn multiple_slow_worker_status_probes_share_one_deadline() {
    let mut handles = Vec::new();
    let mut endpoints = Vec::new();
    for _ in 0..8 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        endpoints.push(format!("http://{}", listener.local_addr().unwrap()));
        let router = Router::new().route(
            "/internal/v1/worker/status",
            get(|| async {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                Json(available(4, 0, 0))
            }),
        );
        handles.push(tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        }));
    }
    let state = state(ServiceRole::Api)
        .await
        .with_service_role(ServiceRole::Api, endpoints, 1)
        .unwrap();
    let started = std::time::Instant::now();
    let result = candidates(&state, owner_routing::unix_now_ms().saturating_add(40)).await;
    let elapsed = started.elapsed();
    for handle in handles {
        handle.abort();
        let _ = handle.await;
    }
    assert!(result.is_empty());
    // Allow executor jitter, but never the previous serial endpoint timeout sum.
    assert!(
        elapsed < std::time::Duration::from_millis(250),
        "{elapsed:?}"
    );
}
