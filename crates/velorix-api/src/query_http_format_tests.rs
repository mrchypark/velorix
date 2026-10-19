use super::*;
use arrow::ipc::reader::StreamReader;
use axum::http::Method;
use tower::ServiceExt;

async fn request(
    router: &Router,
    method: Method,
    uri: &str,
    body: Value,
    accept: Option<&str>,
) -> (StatusCode, HeaderMap, bytes::Bytes) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(accept) = accept {
        request = request.header(header::ACCEPT, accept);
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, headers, bytes)
}

fn arrow_batches(response: &(StatusCode, HeaderMap, bytes::Bytes)) -> Vec<RecordBatch> {
    assert_eq!(
        response.0,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&response.2)
    );
    assert_eq!(response.1[header::CONTENT_TYPE], ARROW_STREAM);
    StreamReader::try_new(Cursor::new(response.2.clone()), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

#[tokio::test]
async fn http_native_count_without_pruning_reads_complete_input_for_both_codecs() {
    for ipc in [false, true] {
        let (state, _) = native_output_pages::tests::public_native_fixture_with_codec(ipc).await;
        let router = app(state);
        for sql in [
            "SELECT COUNT(*) AS n FROM purchases_by_user",
            "WITH source AS (SELECT * FROM purchases_by_user) SELECT COUNT(*) AS n FROM source",
        ] {
            for accept in [None, Some("application/json")] {
                let response = request(
                    &router,
                    Method::POST,
                    "/v1/views/purchases_by_user/query",
                    json!({"sql":sql}),
                    accept,
                )
                .await;
                assert_eq!(
                    response.0,
                    StatusCode::OK,
                    "ipc={ipc}, sql={sql}: {}",
                    String::from_utf8_lossy(&response.2)
                );
                let rows = if accept.is_none() {
                    record_batches_to_json_rows(&arrow_batches(&response)).unwrap()
                } else {
                    serde_json::from_slice::<Value>(&response.2).unwrap()["rows"]
                        .as_array()
                        .unwrap()
                        .clone()
                };
                assert_eq!(rows, vec![json!({"n":24020})]);
            }
        }
    }
}

#[tokio::test]
async fn http_default_arrow_and_explicit_json_cover_raw_template_empty_and_policy() {
    let (state, active) = native_output_pages::tests::public_native_fixture().await;
    assert_eq!(
        active_view_response(&active, None)
            .unwrap()
            .response_formats,
        vec!["arrow", "json"]
    );
    let router = app(state);
    let uri = "/v1/views/purchases_by_user/query";
    for body in [
        json!({"sql":"SELECT * FROM purchases_by_user WHERE id='m'"}),
        json!({"parameters":{"map_id":"m"}}),
    ] {
        let default = request(&router, Method::POST, uri, body.clone(), None).await;
        let batches = arrow_batches(&default);
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 20);
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Utf8);
        let wildcard = request(&router, Method::POST, uri, body.clone(), Some("*/*")).await;
        assert_eq!(
            record_batches_to_json_rows(&arrow_batches(&wildcard)).unwrap(),
            record_batches_to_json_rows(&batches).unwrap()
        );
        let json_response =
            request(&router, Method::POST, uri, body, Some("application/json")).await;
        assert_eq!(json_response.0, StatusCode::OK);
        let json: Value = serde_json::from_slice(&json_response.2).unwrap();
        assert_eq!(
            json["rows"],
            serde_json::to_value(record_batches_to_json_rows(&batches).unwrap()).unwrap()
        );
        assert_eq!(
            json["logical_epoch"].as_u64().unwrap().to_string(),
            default.1["x-velorix-logical-epoch"].to_str().unwrap()
        );
    }
    let empty = request(
        &router,
        Method::POST,
        uri,
        json!({"sql":"SELECT id AS renamed FROM purchases_by_user WHERE id='missing'"}),
        None,
    )
    .await;
    let reader = StreamReader::try_new(Cursor::new(empty.2.clone()), None).unwrap();
    assert_eq!(reader.schema().field(0).name(), "renamed");
    assert_eq!(reader.schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        arrow_batches(&empty)
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    for accept in [
        "text/plain",
        "application/json;q=0,application/vnd.apache.arrow.stream;q=0",
        "application/json;q=bad",
    ] {
        assert_eq!(
            request(&router, Method::POST, uri, json!({}), Some(accept))
                .await
                .0,
            StatusCode::NOT_ACCEPTABLE
        );
    }
    for accept in [None, Some("application/json")] {
        let response = request(
            &router,
            Method::POST,
            uri,
            json!({"sql":"SELECT * FROM purchases_by_user WHERE power>=1"}),
            accept,
        )
        .await;
        assert_eq!(
            response.0,
            StatusCode::BAD_REQUEST,
            "{}",
            String::from_utf8_lossy(&response.2)
        );
    }
}

#[tokio::test]
async fn http_direct_page_keeps_epoch_cursor_in_headers_and_arrow_schema() {
    let (state, _) = native_output_pages::tests::public_native_fixture().await;
    let router = app(state);
    let uri = "/v1/views/purchases_by_user/outputs/purchases_by_user/query";
    let first = request(
        &router,
        Method::GET,
        &format!("{uri}?max_rows=1"),
        json!({}),
        None,
    )
    .await;
    let batches = arrow_batches(&first);
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let epoch = first.1["x-velorix-logical-epoch"]
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let token = first.1["x-velorix-next-page-token"].to_str().unwrap();
    assert_eq!(
        batches[0].schema().metadata()["velorix.next_page_token"],
        token
    );
    let second = request(
        &router,
        Method::POST,
        uri,
        json!({"max_rows":1,"epoch":epoch,"page_token":token}),
        None,
    )
    .await;
    assert_eq!(
        arrow_batches(&second)
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert_eq!(
        first.1["x-velorix-logical-epoch"],
        second.1["x-velorix-logical-epoch"]
    );
}

#[tokio::test]
async fn direct_http_source_budgets_reject_before_page_reads_in_both_formats() {
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
        let (mut state, _) = native_output_pages::tests::public_native_fixture().await;
        let policy_id = format!("direct-budget-{index}");
        state
            .query_policy_catalog()
            .unwrap()
            .create_for_production_table_scan("default", &policy_id, policy)
            .await
            .unwrap();
        let key = object_store::path::Path::from("v1/views/purchases_by_user/active.json");
        let bytes = state.store.get(&key).await.unwrap().bytes().await.unwrap();
        let mut record: Value = serde_json::from_slice(&bytes).unwrap();
        record["api"]["query_policy_id"] = json!(policy_id);
        state
            .store
            .put(
                &key,
                bytes::Bytes::from(serde_json::to_vec(&record).unwrap()).into(),
            )
            .await
            .unwrap();
        let counts = crate::tests::ObjectStoreAccessCounts::default();
        state.store = Arc::new(crate::tests::CountingObjectStore::new(
            state.store.clone(),
            counts.clone(),
        ));
        let router = app(state);
        for method in [Method::GET, Method::POST] {
            for accept in [None, Some("application/json")] {
                counts.clear();
                let uri = if method == Method::GET {
                    "/v1/views/purchases_by_user/outputs/purchases_by_user/query?max_rows=1"
                } else {
                    "/v1/views/purchases_by_user/outputs/purchases_by_user/query"
                };
                let response =
                    request(&router, method.clone(), uri, json!({"max_rows":1}), accept).await;
                assert_eq!(
                    response.0,
                    StatusCode::BAD_REQUEST,
                    "policy={index}: {}",
                    String::from_utf8_lossy(&response.2)
                );
                assert!(
                    !counts
                        .get_paths()
                        .iter()
                        .any(|path| path.starts_with("v1/standing-runtime-output-pages/")),
                    "policy={index} fetched a page: {:?}",
                    counts.get_paths()
                );
                if index == 1 {
                    assert!(
                        !counts
                            .get_paths()
                            .iter()
                            .any(|path| path.starts_with("v1/standing-runtime-")),
                        "exhausted object budget fetched native metadata"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn default_direct_get_and_post_paginate_within_row_policy_in_both_formats() {
    let (state, _) = native_output_pages::tests::public_native_fixture().await;
    let router = app(state);
    let uri = "/v1/views/purchases_by_user/outputs/purchases_by_user/query";
    for (method, accept) in [
        (Method::GET, None),
        (Method::POST, None),
        (Method::GET, Some("application/json")),
        (Method::POST, Some("application/json")),
    ] {
        let mut page = request(&router, method, uri, json!({}), accept).await;
        let mut total = BTreeMap::<String, usize>::new();
        let mut epoch = None;
        for expected_count in [10000, 10000, 4020] {
            assert_eq!(
                page.0,
                StatusCode::OK,
                "{}",
                String::from_utf8_lossy(&page.2)
            );
            let current_epoch = page.1["x-velorix-logical-epoch"]
                .to_str()
                .unwrap()
                .parse::<u64>()
                .unwrap();
            assert_eq!(*epoch.get_or_insert(current_epoch), current_epoch);
            let rows = if accept.is_some() {
                let json: Value = serde_json::from_slice(&page.2).unwrap();
                json["rows"].as_array().unwrap().clone()
            } else {
                record_batches_to_json_rows(&arrow_batches(&page)).unwrap()
            };
            assert_eq!(rows.len(), expected_count);
            for row in rows {
                *total.entry(row["id"].as_str().unwrap().into()).or_default() += 1;
            }
            match page.1.get("x-velorix-next-page-token") {
                Some(token) => {
                    assert!(expected_count == 10000);
                    page = request(
                        &router,
                        Method::POST,
                        uri,
                        json!({"epoch":current_epoch,"page_token":token.to_str().unwrap()}),
                        accept,
                    )
                    .await;
                }
                None => assert_eq!(expected_count, 4020),
            }
        }
        assert_eq!(
            total,
            BTreeMap::from([("a".into(), 12000), ("m".into(), 20), ("z".into(), 12000)])
        );
    }
}

#[tokio::test]
async fn filtered_sql_rechecks_fencing_and_evicts_before_reading_cached_pages() {
    let (mut state, active) = native_output_pages::tests::public_native_fixture().await;
    let directory = tempfile::tempdir().unwrap();
    let cache =
        native_read_cache::NativeReadCache::open(native_read_cache::NativeReadCacheConfig {
            directory: directory.path().join("cache"),
            memory_bytes: 1024 * 1024,
            disk_bytes: 8 * 1024 * 1024,
        })
        .await
        .unwrap();
    state = state.with_native_read_cache(Arc::new(cache));
    let counts = crate::tests::ObjectStoreAccessCounts::default();
    state.store = Arc::new(crate::tests::CountingObjectStore::new(
        state.store.clone(),
        counts.clone(),
    ));
    let schema = &active.spec.output_relations[0];
    let predicate = velorix_core::query::materialized_page_predicate(
        "SELECT * FROM purchases_by_user WHERE id='m'",
        schema,
        &[],
    )
    .unwrap();
    query_serving::standing_runtime_sql_page(
        &state,
        &active,
        &schema.relation_id,
        SnapshotPageRequest::default(),
        Some(&predicate),
        QueryPolicy::default(),
    )
    .await
    .unwrap();
    counts.clear();
    query_serving::standing_runtime_sql_page(
        &state,
        &active,
        &schema.relation_id,
        SnapshotPageRequest::default(),
        Some(&predicate),
        QueryPolicy::default(),
    )
    .await
    .unwrap();
    assert!(!counts
        .get_paths()
        .iter()
        .any(|path| path.starts_with("v1/standing-runtime-output-pages/")));
    counts.clear();
    let state = state.with_standing_runtime_fencing_required(true);
    let identity = active_standing_runtime_identity(&active).unwrap();
    state.standing_runtimes.local_state.lock().unwrap().insert(
        standing_runtime_key(identity, &active.spec.view_id),
        StandingRuntimeLocalState::default(),
    );
    let error = query_serving::standing_runtime_sql_page(
        &state,
        &active,
        &schema.relation_id,
        SnapshotPageRequest::default(),
        Some(&predicate),
        QueryPolicy::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(state
        .standing_runtimes
        .local_state
        .lock()
        .unwrap()
        .is_empty());
    assert!(counts.get_paths().is_empty());
}
