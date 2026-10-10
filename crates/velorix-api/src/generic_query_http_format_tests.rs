use super::*;
use arrow::ipc::reader::StreamReader;
use std::io::Cursor;

async fn wire_query(
    router: &Router,
    method: Method,
    uri: &str,
    body: Value,
    accept: Option<&str>,
) -> (StatusCode, axum::http::HeaderMap, bytes::Bytes) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(accept) = accept {
        builder = builder.header(header::ACCEPT, accept);
    }
    let response = router
        .clone()
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, headers, bytes)
}

fn wire_rows(response: &(StatusCode, axum::http::HeaderMap, bytes::Bytes)) -> Vec<Value> {
    assert_eq!(
        response.0,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&response.2)
    );
    if response.1[header::CONTENT_TYPE] == "application/json" {
        serde_json::from_slice::<Value>(&response.2).unwrap()["rows"]
            .as_array()
            .unwrap()
            .clone()
    } else {
        assert_eq!(
            response.1[header::CONTENT_TYPE],
            "application/vnd.apache.arrow.stream"
        );
        let batches = StreamReader::try_new(Cursor::new(&response.2), None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        record_batches_to_json_rows(&batches).unwrap()
    }
}

async fn generic_fixture() -> (ApiState, Router, ObjectStoreAccessCounts) {
    let counts = ObjectStoreAccessCounts::default();
    let store = Arc::new(CountingObjectStore::new(
        Arc::new(InMemory::new()),
        counts.clone(),
    ));
    let state = test_api_state_with_store(store, "generic-format-owner", false)
        .await
        .with_meta_store(Arc::new(InMemoryMetaStore::default()));
    let policy = QueryPolicy {
        max_concurrent_queries: Some(1),
        ..QueryPolicy::default()
    };
    for (id, policy) in [
        ("format-policy", policy),
        (
            "format-page-policy",
            QueryPolicy {
                max_output_rows: Some(2),
                ..policy
            },
        ),
    ] {
        state
            .query_policy_catalog()
            .unwrap()
            .create_for_production_table_scan("default", id, policy)
            .await
            .unwrap();
    }
    let router = app(state.clone());
    for catalog in [
        test_relation_catalog_for_e2e(
            "orders",
            &[
                ("customer_id", VelorixLogicalTypeV1::Utf8, false),
                ("amount", VelorixLogicalTypeV1::Int64, false),
            ],
        ),
        test_relation_catalog_for_e2e(
            "inventory",
            &[
                ("customer_id", VelorixLogicalTypeV1::Utf8, false),
                ("quantity", VelorixLogicalTypeV1::Int64, false),
                ("category", VelorixLogicalTypeV1::Utf8, false),
            ],
        ),
    ] {
        let result = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({"catalog":catalog,"default_orders_sum_count":false}),
        )
        .await;
        assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
    }
    let group = "select customer_id, sum(amount) as sum, count(*) as count from orders where amount > 0 group by customer_id";
    for (view_id, sql) in [
        ("format_filter", "select customer_id, amount from orders where amount > 0"),
        ("format_group", group),
        ("format_template", group),
        ("format_capped", group),
        ("format_join", "select a.customer_id, sum(s.amount) as sum, count(*) as count from orders s join inventory a on s.customer_id = a.customer_id group by a.customer_id"),
    ] {
        let mut request = json!({"view_id":view_id,"sql":sql,
            "input_relation_id":"orders","input_relation_version":"2026-08-14.v1",
            "query_policy_id":if view_id=="format_capped" {"format-page-policy"} else {"format-policy"}});
        if view_id == "format_join" {
            request["input_relation_id"] = json!("");
            request["input_relation_version"] = json!("");
            request["input_relation_refs"] = json!([
                {"relation_id":"orders","relation_version":"2026-08-14.v1"},
                {"relation_id":"inventory","relation_version":"2026-08-14.v1"}]);
        }
        if view_id == "format_template" {
            request["sql_template"] = json!("SELECT * FROM format_template WHERE customer_id={{context.params.user}}");
            request["request"] = json!([{"fieldName":"user","fieldIn":"query","type":"string"}]);
        }
        let result = call_json(&router, Method::POST, "/v1/views", request).await;
        assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
        assert_eq!(result.1["response_formats"], json!(["arrow","json"]));
    }
    let result = call_json(&router, Method::POST, "/v1/relations/ingest", json!({"batches":[
        {"relation_id":"orders","relation_version":"2026-08-14.v1","stream_id":"format-orders",
         "partition_id":0,"start_offset_inclusive":0,"rows":[
             {"customer_id":"alice","amount":10,"delta":1},{"customer_id":"alice","amount":7,"delta":1},
             {"customer_id":"bob","amount":5,"delta":1},{"customer_id":"charlie","amount":2,"delta":1}]},
        {"relation_id":"inventory","relation_version":"2026-08-14.v1","stream_id":"format-inventory",
         "partition_id":0,"start_offset_inclusive":0,"rows":[
             {"customer_id":"alice","quantity":100,"category":"gold","delta":1},
             {"customer_id":"bob","quantity":50,"category":"silver","delta":1}]}
    ]})).await;
    assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
    for view in [
        "format_filter",
        "format_group",
        "format_join",
        "format_capped",
        "format_template",
    ] {
        activate_fixture_view(&router, view).await;
    }
    (state, router, counts)
}

async fn activate_fixture_view(router: &Router, view: &str) {
    let result = call_json(
        router,
        Method::POST,
        &format!("/v1/views/{view}/backfill"),
        json!({}),
    )
    .await;
    assert_eq!(result.0, StatusCode::OK, "{view}: {}", result.1);
}

#[tokio::test]
async fn generic_http_formats_cover_materialized_filter_group_join_raw_and_template() {
    let (_, router, _) = generic_fixture().await;
    for view in ["format_filter", "format_group", "format_join"] {
        let uri = format!("/v1/views/{view}/query");
        for method in [Method::GET, Method::POST] {
            let arrow = wire_query(&router, method.clone(), &uri, json!({}), None).await;
            let json = wire_query(
                &router,
                method.clone(),
                &uri,
                json!({}),
                Some("application/json"),
            )
            .await;
            let arrow_rows = wire_rows(&arrow);
            assert!(!arrow_rows.is_empty());
            assert_eq!(arrow_rows, wire_rows(&json));
            assert_eq!(
                arrow.1["x-velorix-logical-epoch"],
                json.1["x-velorix-logical-epoch"]
            );
            let wildcard = wire_query(&router, method, &uri, json!({}), Some("*/*")).await;
            assert_eq!(wire_rows(&wildcard), arrow_rows);
        }
    }
    for (uri, body) in [
        (
            "/v1/views/format_group/query",
            json!({"sql":"SELECT * FROM format_group WHERE customer_id='alice'"}),
        ),
        (
            "/v1/views/format_template/query",
            json!({"parameters":{"user":"alice"}}),
        ),
    ] {
        let arrow = wire_query(&router, Method::POST, uri, body.clone(), None).await;
        let json = wire_query(&router, Method::POST, uri, body, Some("application/json")).await;
        assert_eq!(
            wire_rows(&arrow),
            vec![json!({"customer_id":"alice","sum":17,"count":2})]
        );
        assert_eq!(wire_rows(&arrow), wire_rows(&json));
    }
    for uri in [
        "/v1/views/format_group/query?sql=SELECT%20customer_id%20FROM%20format_group%20WHERE%20customer_id%3D%27alice%27",
        "/v1/views/format_template/query?user=alice",
    ] {
        let arrow = wire_query(&router, Method::GET, uri, json!({}), None).await;
        let json = wire_query(&router, Method::GET, uri, json!({}), Some("application/json")).await;
        assert_eq!(wire_rows(&arrow).len(), 1);
        assert_eq!(wire_rows(&arrow), wire_rows(&json));
    }
    let empty = wire_query(&router, Method::POST, "/v1/views/format_group/query",
        json!({"sql":"SELECT customer_id AS renamed FROM format_group WHERE customer_id='missing'"}), None).await;
    assert!(wire_rows(&empty).is_empty());
    let reader = StreamReader::try_new(Cursor::new(&empty.2), None).unwrap();
    assert_eq!(reader.schema().field(0).name(), "renamed");
    assert_eq!(reader.schema().field(0).data_type(), &DataType::Utf8);
    for accept in [
        "text/plain",
        "application/json;q=0,application/vnd.apache.arrow.stream;q=0",
        "application/json;q=bad",
    ] {
        let result = wire_query(
            &router,
            Method::POST,
            "/v1/views/format_group/query",
            json!({}),
            Some(accept),
        )
        .await;
        assert_eq!(result.0, StatusCode::NOT_ACCEPTABLE);
    }
}

#[tokio::test]
async fn generic_direct_pagination_respects_output_cap_and_keeps_cursor_metadata() {
    let (_, router, _) = generic_fixture().await;
    let uri = "/v1/views/format_capped/outputs/format_capped/query";
    for method in [Method::GET, Method::POST] {
        for accept in [None, Some("application/json")] {
            let first = wire_query(&router, method.clone(), uri, json!({}), accept).await;
            let mut rows = wire_rows(&first);
            assert_eq!(rows.len(), 2);
            let token = first.1["x-velorix-next-page-token"].to_str().unwrap();
            let epoch = first.1["x-velorix-logical-epoch"]
                .to_str()
                .unwrap()
                .parse::<u64>()
                .unwrap();
            if accept.is_none() {
                let reader = StreamReader::try_new(Cursor::new(&first.2), None).unwrap();
                assert_eq!(reader.schema().metadata()["velorix.next_page_token"], token);
                assert_eq!(
                    reader.schema().metadata()["velorix.logical_epoch"],
                    epoch.to_string()
                );
            }
            let second = wire_query(
                &router,
                Method::POST,
                uri,
                json!({"page_token":token,"epoch":epoch}),
                accept,
            )
            .await;
            let rest = wire_rows(&second);
            assert_eq!(rest.len(), 1);
            assert!(!second.1.contains_key("x-velorix-next-page-token"));
            rows.extend(rest);
            assert_eq!(
                rows,
                vec![
                    json!({"customer_id":"alice","sum":17,"count":2}),
                    json!({"customer_id":"bob","sum":5,"count":1}),
                    json!({"customer_id":"charlie","sum":2,"count":1})
                ]
            );
        }
    }
}

#[tokio::test]
async fn generic_exhausted_query_permit_rejects_before_snapshot_reads() {
    let (state, router, counts) = generic_fixture().await;
    let policy = QueryPolicy {
        max_concurrent_queries: Some(1),
        ..QueryPolicy::default()
    };
    let limiter = state
        .query_limiter_for_policy("format-policy", policy)
        .unwrap()
        .unwrap();
    let permit =
        velorix_runtime::runtime_contract::acquire_query_permit(policy, Some(&limiter)).unwrap();
    for accept in [None, Some("application/json")] {
        counts.clear();
        let result = wire_query(
            &router,
            Method::POST,
            "/v1/views/format_group/query",
            json!({"sql":"SELECT * FROM format_group"}),
            accept,
        )
        .await;
        assert_eq!(result.0, StatusCode::BAD_REQUEST);
        assert!(String::from_utf8_lossy(&result.2).contains("concurrent"));
        assert!(!counts
            .get_paths()
            .iter()
            .any(|path| path.starts_with("v1/standing-runtime-")));
    }
    drop(permit);
    let result = wire_query(
        &router,
        Method::POST,
        "/v1/views/format_group/query",
        json!({}),
        None,
    )
    .await;
    assert_eq!(wire_rows(&result).len(), 3);
    assert!(
        velorix_runtime::runtime_contract::acquire_query_permit(policy, Some(&limiter)).is_ok()
    );
}

#[tokio::test]
async fn generic_sql_count_and_cte_read_all_materialized_rows_above_output_cap() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "large-orders-format-owner",
        false,
    )
    .await
    .with_meta_store(Arc::new(InMemoryMetaStore::default()));
    let router = app(state.clone());
    let catalog = test_relation_catalog_for_e2e(
        "orders",
        &[
            ("customer_id", VelorixLogicalTypeV1::Utf8, false),
            ("amount", VelorixLogicalTypeV1::Int64, false),
        ],
    );
    let result = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog":catalog,"default_orders_sum_count":false}),
    )
    .await;
    assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
    let result = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id":"large_orders","input_relation_id":"orders",
            "input_relation_version":"2026-08-14.v1",
            "sql":"SELECT customer_id, amount FROM orders WHERE amount > 0"
        }),
    )
    .await;
    assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
    let rows = (0..10017)
        .map(|index| {
            json!({
                "customer_id":format!("customer-{index:05}"),"amount":1,"delta":1,
            })
        })
        .collect::<Vec<_>>();
    for (index, chunk) in rows.chunks(5000).enumerate() {
        let result = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({"relation_id":"orders","relation_version":"2026-08-14.v1",
                "stream_id":"large-orders","partition_id":0,
                "start_offset_inclusive":index*5000,"rows":chunk}),
        )
        .await;
        assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
    }
    let uri = "/v1/views/large_orders/query";
    activate_fixture_view(&router, "large_orders").await;
    for sql in [
        "SELECT COUNT(*) AS n FROM large_orders",
        "WITH input_rows AS (SELECT * FROM large_orders) SELECT COUNT(*) AS n FROM input_rows",
    ] {
        for accept in [None, Some("application/json")] {
            let result = wire_query(&router, Method::POST, uri, json!({"sql":sql}), accept).await;
            assert_eq!(wire_rows(&result), vec![json!({"n":10017})]);
        }
    }
    for method in [Method::GET, Method::POST] {
        for accept in [None, Some("application/json")] {
            let first = wire_query(&router, method.clone(), uri, json!({}), accept).await;
            let mut rows = wire_rows(&first);
            assert_eq!(rows.len(), 10000);
            let second = wire_query(&router, Method::POST, uri, json!({
                "epoch":first.1["x-velorix-logical-epoch"].to_str().unwrap().parse::<u64>().unwrap(),
                "page_token":first.1["x-velorix-next-page-token"].to_str().unwrap()
            }), accept).await;
            let rest = wire_rows(&second);
            assert_eq!(rest.len(), 17);
            assert!(!second.1.contains_key("x-velorix-next-page-token"));
            rows.extend(rest);
            let ids = rows
                .iter()
                .map(|row| row["customer_id"].as_str().unwrap())
                .collect::<BTreeSet<_>>();
            assert_eq!(ids.len(), 10017);
        }
    }
    for checkpoint_only in [false, true] {
        let legacy = install_legacy_checkpoint(&state, "large_orders", checkpoint_only).await;
        let router = app(legacy);
        for sql in [
            "SELECT COUNT(*) AS n FROM large_orders",
            "WITH input_rows AS (SELECT * FROM large_orders) SELECT COUNT(*) AS n FROM input_rows",
        ] {
            for accept in [None, Some("application/json")] {
                let result =
                    wire_query(&router, Method::POST, uri, json!({"sql":sql}), accept).await;
                assert_eq!(
                    wire_rows(&result),
                    vec![json!({"n":10017})],
                    "checkpoint_only={checkpoint_only}"
                );
            }
        }
    }
}

#[tokio::test]
async fn generic_direct_http_source_budgets_reject_before_page_reads() {
    let (state, router, counts) = generic_fixture().await;
    let key = Path::from("v1/views/format_group/active.json");
    let original = state.store.get(&key).await.unwrap().bytes().await.unwrap();
    let policies = [
        QueryPolicy {
            max_scan_files: Some(0),
            ..QueryPolicy::default()
        },
        QueryPolicy {
            max_object_requests: Some(1),
            ..QueryPolicy::default()
        },
        QueryPolicy {
            max_scan_bytes: Some(1),
            ..QueryPolicy::default()
        },
        QueryPolicy {
            memory_limit_bytes: Some(32),
            ..QueryPolicy::default()
        },
    ];
    for (index, policy) in policies.into_iter().enumerate() {
        let id = format!("source-budget-{index}");
        state
            .query_policy_catalog()
            .unwrap()
            .create_for_production_table_scan("default", &id, policy)
            .await
            .unwrap();
        let mut record: Value = serde_json::from_slice(&original).unwrap();
        record["api"]["query_policy_id"] = json!(id);
        state
            .store
            .put(
                &key,
                bytes::Bytes::from(serde_json::to_vec(&record).unwrap()).into(),
            )
            .await
            .unwrap();
        for method in [Method::GET, Method::POST] {
            for accept in [None, Some("application/json")] {
                counts.clear();
                let result = wire_query(
                    &router,
                    method.clone(),
                    "/v1/views/format_group/query",
                    json!({}),
                    accept,
                )
                .await;
                assert_eq!(
                    result.0,
                    StatusCode::BAD_REQUEST,
                    "policy={index}: {}",
                    String::from_utf8_lossy(&result.2)
                );
                assert!(!counts
                    .get_paths()
                    .iter()
                    .any(|path| path.starts_with("v1/standing-runtime-output-pages/")));
                if index == 1 {
                    assert!(
                        counts
                            .get_paths()
                            .iter()
                            .filter(|path| path.starts_with("v1/standing-runtime-"))
                            .count()
                            <= 1
                    );
                }
            }
        }
    }
}

// Re-encode the real admitted runtime checkpoint in its supported JSON v1
// storage format. This is a storage compatibility fixture, not a SQL fallback.
async fn install_legacy_checkpoint(
    state: &ApiState,
    view: &str,
    checkpoint_only: bool,
) -> ApiState {
    let active = state
        .view_registry()
        .unwrap()
        .read_active(view)
        .await
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let mut record = read_latest_standing_runtime_checkpoint(state, identity, view)
        .await
        .unwrap()
        .unwrap();
    let previous = state
        .meta_store
        .as_ref()
        .unwrap()
        .read_standing_runtime_checkpoint(&identity.tenant_id, &identity.program_id, view)
        .await
        .unwrap()
        .unwrap();
    // Publish the same complete materialized state at a new epoch through the
    // normal metadata CAS. Retain the existing admission and activation state.
    record.checkpoint.logical_epoch += 1;
    for frontier in &mut record.checkpoint.output_frontiers {
        frontier.committed_epoch = record.checkpoint.logical_epoch;
    }
    let (state_key, state_record) =
        standing_runtime_state_payload_record_for_checkpoint(&record.checkpoint, view).unwrap();
    persist_standing_runtime_state_payload(state, &state_key, &state_record)
        .await
        .unwrap();
    record.checkpoint.state_root.object_key = state_key.as_str().into();
    let key = ObjectKey::standing_runtime_checkpoint(
        &identity.tenant_id,
        &identity.program_id,
        view,
        record.checkpoint.logical_epoch,
        &record.checkpoint.state_root.content_hash,
    )
    .unwrap();
    record.checkpoint_key = key.as_str().into();
    record.checkpoint.output_manifest_refs.clear();
    let publication =
        standing_runtime_output_manifest_record_for_checkpoint(&record.checkpoint, view, &key)
            .unwrap()
            .unwrap();
    if checkpoint_only {
        match state
            .store
            .delete(&Path::from(publication.manifest_key.as_str()))
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => panic!("delete synthetic legacy manifest: {error}"),
        }
    } else {
        assert_eq!(
            publication.manifest_record.output_encoding,
            "velorix-delta-batch-json-v1"
        );
        for (key, page) in &publication.page_records {
            put_standing_runtime_output_page(state, key, page)
                .await
                .unwrap();
        }
        put_standing_runtime_output_manifest(
            state,
            &publication.manifest_key,
            &publication.manifest_record,
        )
        .await
        .unwrap();
        record.checkpoint.output_manifest_refs.push(format!(
            "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
            publication.manifest_key.as_str()
        ));
    }
    record.previous_checkpoint = Some(previous.clone());
    record.manifest_hash.clear();
    state
        .store
        .put(
            &Path::from(record.checkpoint_key.as_str()),
            bytes::Bytes::from(serde_json::to_vec(&record).unwrap()).into(),
        )
        .await
        .unwrap();
    let legacy = state.clone();
    let owner = legacy
        .acquire_standing_runtime_owner(identity, view)
        .await
        .unwrap()
        .unwrap();
    publish_standing_runtime_checkpoint_pointer(
        &legacy,
        Some(previous),
        standing_runtime_checkpoint_pointer_from_record(&record),
        Some(owner),
        None,
    )
    .await
    .unwrap();
    legacy
}

#[tokio::test]
async fn legacy_http_source_budgets_apply_with_cache_disabled_and_warmed() {
    for checkpoint_only in [false, true] {
        let (original, _, counts) = generic_fixture().await;
        let legacy = install_legacy_checkpoint(&original, "format_group", checkpoint_only).await;
        let directory = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            crate::materialized_read_cache::MaterializedReadCache::open(
                crate::materialized_read_cache::MaterializedReadCacheConfig {
                    directory: directory.path().to_owned(),
                    memory_bytes: 1024 * 1024,
                    disk_bytes: 8 * 1024 * 1024,
                },
            )
            .await
            .unwrap(),
        );
        for cached in [false, true] {
            let mut state = legacy.clone();
            if cached {
                state = state.with_materialized_read_cache(cache.clone());
            }
            let key = Path::from("v1/views/format_group/active.json");
            let original = state.store.get(&key).await.unwrap().bytes().await.unwrap();
            // Two successful reads populate and hit the descriptor/manifest cache.
            let router = app(state.clone());
            for _ in 0..2 {
                assert_eq!(
                    wire_rows(
                        &wire_query(
                            &router,
                            Method::GET,
                            "/v1/views/format_group/query",
                            json!({}),
                            None
                        )
                        .await
                    )
                    .len(),
                    3
                );
            }
            for (index, policy) in [
                QueryPolicy {
                    max_scan_files: Some(0),
                    ..QueryPolicy::default()
                },
                QueryPolicy {
                    max_object_requests: Some(2),
                    ..QueryPolicy::default()
                },
                QueryPolicy {
                    max_scan_bytes: Some(1),
                    ..QueryPolicy::default()
                },
                QueryPolicy {
                    memory_limit_bytes: Some(32),
                    ..QueryPolicy::default()
                },
            ]
            .into_iter()
            .enumerate()
            {
                let id = format!("legacy-budget-{checkpoint_only}-{cached}-{index}");
                state
                    .query_policy_catalog()
                    .unwrap()
                    .create_for_production_table_scan("default", &id, policy)
                    .await
                    .unwrap();
                let mut active: Value = serde_json::from_slice(&original).unwrap();
                active["api"]["query_policy_id"] = json!(id);
                state
                    .store
                    .put(
                        &key,
                        bytes::Bytes::from(serde_json::to_vec(&active).unwrap()).into(),
                    )
                    .await
                    .unwrap();
                for method in [Method::GET, Method::POST] {
                    for accept in [None, Some("application/json")] {
                        counts.clear();
                        let result = wire_query(
                            &router,
                            method.clone(),
                            "/v1/views/format_group/query",
                            json!({}),
                            accept,
                        )
                        .await;
                        assert_eq!(
                            result.0,
                            StatusCode::BAD_REQUEST,
                            "checkpoint_only={checkpoint_only} cached={cached} policy={index}: {}",
                            String::from_utf8_lossy(&result.2)
                        );
                        assert!(!counts
                            .get_paths()
                            .iter()
                            .any(|p| p.starts_with("v1/standing-runtime-output-pages/")));
                    }
                }
            }
            state.store.put(&key, original.into()).await.unwrap();
        }
        cache.close().await.unwrap();
    }
}

#[tokio::test]
async fn legacy_no_meta_discovery_is_bounded_and_detects_concurrent_new_head() {
    let (original, _, counts) = generic_fixture().await;
    let legacy = install_legacy_checkpoint(&original, "format_group", true).await;
    let active = legacy
        .view_registry()
        .unwrap()
        .read_active("format_group")
        .await
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let schema = &active.spec.output_relations[0];
    let mut no_meta = legacy.clone();
    no_meta.meta_store = None;
    // The legacy helper must account for every discovery poll, not just LIST creation.
    for budget in [1, 2] {
        counts.clear();
        let error = crate::query_serving::legacy_materialized_page(
            &no_meta,
            "format_group",
            identity,
            &schema.relation_id,
            schema,
            SnapshotPageRequest::default(),
            false,
            QueryPolicy {
                max_object_requests: Some(budget),
                ..QueryPolicy::default()
            },
            0,
            0,
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(
            counts.get_paths().is_empty(),
            "discovery exceeded budget before descriptor GET"
        );
    }
    let page = crate::query_serving::legacy_materialized_page(
        &no_meta,
        "format_group",
        identity,
        &schema.relation_id,
        schema,
        SnapshotPageRequest::default(),
        false,
        QueryPolicy::default(),
        0,
        0,
    )
    .await
    .unwrap();
    assert_eq!(
        page.batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );

    // Pause between descriptor and state reads using the object-store test adapter.
    // The producer publishes through the existing owner/CAS path while the reader
    // is still consuming the previous immutable checkpoint.
    no_meta.store = Arc::new(object_store::throttle::ThrottledStore::new(
        legacy.store.clone(),
        object_store::throttle::ThrottleConfig {
            wait_get_per_call: Duration::from_millis(20),
            ..Default::default()
        },
    ));
    counts.clear();
    let reader = crate::query_serving::legacy_materialized_page(
        &no_meta,
        "format_group",
        identity,
        &schema.relation_id,
        schema,
        SnapshotPageRequest::default(),
        false,
        QueryPolicy::default(),
        0,
        0,
    );
    let producer = async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if counts
                    .get_paths()
                    .iter()
                    .any(|p| p.ends_with(".checkpoint.json"))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        install_legacy_checkpoint(&legacy, "format_group", true).await;
    };
    let (result, ()) = tokio::join!(reader, producer);
    assert_eq!(result.unwrap_err().status, StatusCode::CONFLICT);
}
