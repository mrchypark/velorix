use super::*;
use arrow::ipc::reader::StreamReader;
use http_body_util::BodyExt;

#[test]
fn query_metadata_defaults_to_arrow_and_keeps_explicit_preferences() {
    let mut request: CreateViewRequest =
        serde_json::from_value(json!({"view_id":"result","sql":"SELECT * FROM input"})).unwrap();
    assert_eq!(
        api_metadata_from_create_view_request(&request).response_formats,
        vec!["arrow", "json"]
    );
    request.response_formats = vec!["json".into()];
    assert_eq!(
        api_metadata_from_create_view_request(&request).response_formats,
        vec!["json"]
    );
}

#[tokio::test]
async fn held_query_permit_cannot_bypass_a_stricter_concurrency_policy() {
    use velorix_runtime::runtime_contract::{
        acquire_query_permit, query_record_batches_table_with_bindings_and_policy_and_permit,
    };
    let permit = acquire_query_permit(QueryPolicy::default(), None).unwrap();
    let policy = QueryPolicy {
        max_concurrent_queries: Some(1),
        ..QueryPolicy::default()
    };
    let result = query_record_batches_table_with_bindings_and_policy_and_permit(
        "result",
        typed_response(false).batches,
        "SELECT * FROM result",
        &[],
        policy,
        &permit,
    )
    .await;
    assert!(result.unwrap_err().to_string().contains("limiter"));
}

#[tokio::test]
async fn empty_sql_projection_keeps_its_result_schema() {
    let response = typed_response(false);
    let batches = query_record_batches_table_with_bindings_and_policy_and_limiter(
        "result",
        response.batches,
        "SELECT id AS renamed FROM result WHERE id = -1",
        &[],
        QueryPolicy::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);
    assert_eq!(batches[0].schema().field(0).name(), "renamed");
    assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Int64);
}

#[tokio::test]
async fn api_response_projection_preserves_typed_batches_and_legacy_coercions() {
    let specification = MaterializedViewResponseSchema {
        columns: vec![
            MaterializedViewResponseColumnSpec {
                name: "renamed".into(),
                source: "id".into(),
                r#type: "int64".into(),
                description: None,
            },
            MaterializedViewResponseColumnSpec {
                name: "multiplicity".into(),
                source: "weight".into(),
                r#type: "string".into(),
                description: None,
            },
        ],
    };
    let response = apply_response_schema(typed_response(false), &specification).unwrap();
    assert_eq!(response.schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(response.schema.field(1).data_type(), &DataType::Utf8);
    let rows = record_batches_to_json_rows(&response.batches).unwrap();
    assert_eq!(rows[0]["renamed"], 9_007_199_254_740_993i64);
    assert_eq!(rows[0]["multiplicity"], "2");
    assert!(rows[1]["renamed"].is_null());
    assert_eq!(rows[1]["multiplicity"], "-1");
}

#[test]
fn accept_negotiation_honors_specificity_quality_and_exclusions() {
    for (accept, expected) in [
        (None, Some(QueryResponseFormat::Arrow)),
        (Some("*/*"), Some(QueryResponseFormat::Arrow)),
        (Some("application/json"), Some(QueryResponseFormat::Json)),
        (
            Some("application/json, */*"),
            Some(QueryResponseFormat::Json),
        ),
        (Some("APPLICATION/JSON"), Some(QueryResponseFormat::Json)),
        (
            Some("application/json;q=0.8, application/vnd.apache.arrow.stream;q=0.2"),
            Some(QueryResponseFormat::Json),
        ),
        (
            Some("application/vnd.apache.arrow.stream;q=0, */*;q=1"),
            Some(QueryResponseFormat::Json),
        ),
        (
            Some("application/json;q=0, application/*"),
            Some(QueryResponseFormat::Arrow),
        ),
        (Some("application/json;q=0, */*;q=0"), None),
        (Some("text/plain"), None),
        (Some(""), None),
        (Some("application/json;q=1.1"), None),
        (Some("application/json;q=NaN"), None),
        (Some("application/json;q=0.0001"), None),
        (Some("application/json;q=1;q=0"), None),
        (Some("*/json"), None),
    ] {
        let mut headers = HeaderMap::new();
        if let Some(accept) = accept {
            headers.insert(header::ACCEPT, HeaderValue::from_str(accept).unwrap());
        }
        match expected {
            Some(format) => assert_eq!(
                QueryResponseFormat::negotiate(&headers).unwrap(),
                format,
                "{accept:?}"
            ),
            None => assert_eq!(
                QueryResponseFormat::negotiate(&headers).unwrap_err().status,
                StatusCode::NOT_ACCEPTABLE,
                "{accept:?}"
            ),
        }
    }
    let mut headers = HeaderMap::new();
    headers.append(header::ACCEPT, HeaderValue::from_static("text/plain"));
    headers.append(header::ACCEPT, HeaderValue::from_static("application/json"));
    assert_eq!(
        QueryResponseFormat::negotiate(&headers).unwrap(),
        QueryResponseFormat::Json
    );
}

fn typed_response(empty: bool) -> QueryBatchResponse {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("weight", DataType::Int64, false),
    ]));
    let batches = if empty {
        Vec::new()
    } else {
        vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(9_007_199_254_740_993), None])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(123_456_789),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![2, -1])),
            ],
        )
        .unwrap()]
    };
    QueryBatchResponse {
        schema,
        batches,
        logical_epoch: 42,
        next_page_token: Some("page-2".into()),
    }
}

#[tokio::test]
async fn arrow_preserves_types_nulls_weights_empty_schema_and_metadata_without_json() {
    for empty in [false, true] {
        let typed = typed_response(empty);
        let expected = typed.batches.clone();
        let schema = typed.schema.clone();
        let response = typed
            .into_http_response(QueryResponseFormat::Arrow, QueryPolicy::default(), |_| {
                panic!("Arrow must not serialize JSON")
            })
            .unwrap();
        assert_eq!(response.headers()[header::CONTENT_TYPE], ARROW_STREAM);
        assert_eq!(response.headers()[header::VARY], "Accept");
        assert_eq!(response.headers()["x-velorix-logical-epoch"], "42");
        assert_eq!(response.headers()["x-velorix-next-page-token"], "page-2");
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
        assert_eq!(reader.schema().fields(), schema.fields());
        assert_eq!(reader.schema().metadata()["velorix.logical_epoch"], "42");
        assert_eq!(
            reader.schema().metadata()["velorix.next_page_token"],
            "page-2"
        );
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), expected.len());
        for (actual, expected) in batches.iter().zip(&expected) {
            assert_eq!(actual.columns(), expected.columns());
        }
    }
}

#[tokio::test]
async fn explicit_json_keeps_legacy_envelope_and_both_formats_obey_wire_limits() {
    let response = typed_response(false)
        .into_http_response(
            QueryResponseFormat::Json,
            QueryPolicy::default(),
            record_batches_to_json_rows,
        )
        .unwrap();
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["logical_epoch"], 42);
    assert_eq!(json["next_page_token"], "page-2");
    assert_eq!(json["rows"][0]["id"], 9_007_199_254_740_993i64);
    for format in [QueryResponseFormat::Arrow, QueryResponseFormat::Json] {
        let policy = QueryPolicy {
            max_output_bytes: Some(8),
            ..QueryPolicy::default()
        };
        let error = typed_response(false)
            .into_http_response(format, policy, record_batches_to_json_rows)
            .unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("max_output_bytes"));
        let policy = QueryPolicy {
            max_output_rows: Some(1),
            ..QueryPolicy::default()
        };
        assert!(typed_response(false)
            .into_http_response(format, policy, record_batches_to_json_rows)
            .unwrap_err()
            .message
            .contains("max_output_rows"));
    }
}
