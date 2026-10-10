//! Synthetic correctness and I/O checks for schema-bound materialized pages.
use super::*;
use crate::materialized_output_pages as pages;
use velorix_core::delta::{DeltaKey, DeltaRecord, DeltaValue};

fn page_fixture() -> (RuntimeCheckpoint, RelationSchema, ObjectKey) {
    let schema = RelationSchema {
        relation_id: "purchases_by_user".into(),
        relation_name: "purchases_by_user".into(),
        relation_version: "synthetic-v1".into(),
        schema_fingerprint: stable_bytes_hash(b"synthetic-inventory-floats"),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "amount".into(),
                data_type: SqlDataType::Float64,
                nullable: true,
            },
        ],
        primary_key: vec!["id".into()],
    };
    let mut checkpoint = test_runtime_checkpoint(Vec::new());
    let payload = json!({"output_schema":schema}).to_string();
    checkpoint.state_root.content_hash = stable_bytes_hash(payload.as_bytes());
    checkpoint.state_payload.as_mut().unwrap().payload = payload;
    let key = ObjectKey::standing_runtime_checkpoint(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        &schema.relation_id,
        checkpoint.logical_epoch,
        &checkpoint.state_root.content_hash,
    )
    .unwrap();
    (checkpoint, schema, key)
}

fn float_row(id: &str, value: Value, weight: i64) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(json!({"id":id})),
        DeltaValue::from_json(json!({"amount":value})),
        weight,
    )
}

#[test]
fn materialized_ipc_preserves_difficult_floats_weights_nulls_and_rejects_padding() {
    let (checkpoint, schema, key) = page_fixture();
    let values = [
        f64::from_bits(0x3fd5555555555555),
        f64::from_bits(0x3ff0000000000001),
        f64::from_bits(0x0010000000000001),
        9007199254740992.0,
        -1.2345678901234567e-200,
    ];
    for (i, value) in values.into_iter().enumerate() {
        let publication = pages::publication(
            &checkpoint,
            &schema.relation_id,
            &key,
            DeltaBatch::from_records([
                float_row(&format!("item-{i}"), json!(value), 601),
                float_row(&format!("item-{i}"), json!(value), -1),
                float_row("null", Value::Null, 3),
            ]),
        )
        .unwrap();
        assert_eq!(publication.manifest_record.output_row_count, 603);
        assert_eq!(publication.page_records.len(), 2);
        for (key, page) in &publication.page_records {
            let bytes = pages::encode_page(page).unwrap();
            assert_eq!(stable_bytes_hash(&bytes), page.page_content_hash);
            assert!(serde_json::from_slice::<Value>(&bytes).is_err());
            let restored = pages::decode_page(&bytes, pages::CODEC).unwrap();
            pages::validate_page(key, &restored).unwrap();
            assert_eq!(
                pages::validate_page_binding(&publication.manifest_record, &restored).unwrap(),
                pages::validate_page_binding(&publication.manifest_record, page).unwrap()
            );
            let mut padded = bytes;
            padded.push(0);
            assert!(pages::decode_page(&padded, pages::CODEC).is_err());
        }
    }
}

#[test]
fn materialized_page_binding_recomputes_stats_and_accepts_absent_legacy_stats() {
    let (checkpoint, schema, key) = page_fixture();
    let publication = pages::publication(
        &checkpoint,
        &schema.relation_id,
        &key,
        DeltaBatch::from_records([
            float_row("item", json!(7.5), 1),
            float_row("missing", Value::Null, 2),
        ]),
    )
    .unwrap();
    let page = &publication.page_records[0].1;
    let mut wrong = publication.manifest_record.clone();
    wrong.published_output["page_stats"][0][1]["nulls"] = json!(0);
    assert!(pages::validate_page_binding(&wrong, page).is_err());
    wrong
        .published_output
        .as_object_mut()
        .unwrap()
        .remove("page_stats");
    pages::validate_page_binding(&wrong, page).unwrap();
    let mut wrong_schema = publication.manifest_record;
    wrong_schema.published_output["schema"]["columns"][1]["nullable"] = json!(false);
    assert!(pages::validate_checkpoint_binding(&wrong_schema, &checkpoint).is_err());
}

async fn request_rows(router: &Router, view: &str, sql: &str) -> Vec<Value> {
    let result = call_json(
        router,
        Method::POST,
        &format!("/v1/views/{view}/query"),
        json!({"sql":sql}),
    )
    .await;
    assert_eq!(result.0, StatusCode::OK, "{}", result.1);
    result.1["rows"].as_array().unwrap().clone()
}

async fn register(router: &Router, relation: &str, columns: &[(&str, VelorixLogicalTypeV1, bool)]) {
    let result = call_json(router, Method::POST, "/v1/relations",
        json!({"catalog":test_relation_catalog_for_e2e(relation, columns),"default_orders_sum_count":false})).await;
    assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
}

async fn create(router: &Router, view: &str, sql: &str, join: bool) {
    let mut request = json!({"view_id":view,"sql":sql,"input_relation_id":"orders",
        "input_relation_version":"2026-08-14.v1","response_formats":["json"]});
    if join {
        request["input_relation_id"] = json!("");
        request["input_relation_version"] = json!("");
        request["input_relation_refs"] = json!([
            {"relation_id":"orders","relation_version":"2026-08-14.v1"},
            {"relation_id":"inventory","relation_version":"2026-08-14.v1"}]);
    }
    let result = call_json(router, Method::POST, "/v1/views", request).await;
    assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
}

async fn activate(router: &Router, view: &str) {
    let activated = call_json(
        router,
        Method::POST,
        &format!("/v1/views/{view}/backfill"),
        json!({}),
    )
    .await;
    assert_eq!(activated.0, StatusCode::OK, "{}", activated.1);
}

fn data_page_reads(counts: &ObjectStoreAccessCounts) -> usize {
    counts
        .get_paths()
        .iter()
        .filter(|key| key.starts_with("v1/standing-runtime-output-pages/"))
        .count()
}

#[tokio::test]
async fn materialized_public_runtime_normalizes_filter_projection_group_and_two_schema_join() {
    let state = test_api_state()
        .await
        .with_meta_store(Arc::new(InMemoryMetaStore::default()));
    let router = app(state);
    register(
        &router,
        "orders",
        &[
            ("customer_id", VelorixLogicalTypeV1::Utf8, false),
            ("amount", VelorixLogicalTypeV1::Int64, false),
        ],
    )
    .await;
    register(
        &router,
        "inventory",
        &[
            ("customer_id", VelorixLogicalTypeV1::Utf8, false),
            ("quantity", VelorixLogicalTypeV1::Int64, false),
        ],
    )
    .await;
    create(
        &router,
        "order_projection",
        "select customer_id, amount from orders where amount > 5",
        false,
    )
    .await;
    create(&router, "order_metrics", "select customer_id, sum(amount) as total, count(*) as count, min(amount) as minimum, max(amount) as maximum, avg(amount) as average from orders group by customer_id", false).await;
    create(&router, "order_join", "select i.customer_id, sum(o.amount) as sum, count(*) as count from orders o join inventory i on o.customer_id = i.customer_id group by i.customer_id", true).await;
    let result = call_json(&router, Method::POST, "/v1/relations/ingest", json!({"batches":[
        {"relation_id":"orders","relation_version":"2026-08-14.v1","stream_id":"orders-stream","partition_id":0,"start_offset_inclusive":0,
         "rows":[{"customer_id":"alice","amount":10,"delta":1},{"customer_id":"alice","amount":6,"delta":1},{"customer_id":"bob","amount":2,"delta":1}]},
        {"relation_id":"inventory","relation_version":"2026-08-14.v1","stream_id":"inventory-stream","partition_id":0,"start_offset_inclusive":0,
         "rows":[{"customer_id":"alice","quantity":3,"delta":1}]}
    ]})).await;
    assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
    for view in ["order_projection", "order_metrics", "order_join"] {
        activate(&router, view).await;
    }
    assert_eq!(
        request_rows(
            &router,
            "order_projection",
            "SELECT count(*) AS count FROM order_projection"
        )
        .await,
        vec![json!({"count":2})]
    );
    assert_eq!(
        request_rows(
            &router,
            "order_metrics",
            "SELECT * FROM order_metrics WHERE customer_id='alice'"
        )
        .await,
        vec![
            json!({"customer_id":"alice","total":16,"count":2,"minimum":6,"maximum":10,"average":8.0})
        ]
    );
    assert_eq!(
        request_rows(&router, "order_join", "SELECT * FROM order_join").await,
        vec![json!({"customer_id":"alice","sum":16,"count":2})]
    );
}

#[tokio::test]
async fn materialized_public_large_snapshot_prunes_and_keeps_count_cte_null_and_unknown_residuals_complete(
) {
    let counts = ObjectStoreAccessCounts::default();
    let store = Arc::new(CountingObjectStore::new(
        Arc::new(InMemory::new()),
        counts.clone(),
    ));
    let state = test_api_state_with_store(store, "materialized-page-owner", false)
        .await
        .with_meta_store(Arc::new(InMemoryMetaStore::default()));
    let router = app(state.clone());
    register(
        &router,
        "orders",
        &[
            ("customer_id", VelorixLogicalTypeV1::Utf8, false),
            ("amount", VelorixLogicalTypeV1::Int64, true),
        ],
    )
    .await;
    create(
        &router,
        "order_rows",
        "select customer_id, amount from orders",
        false,
    )
    .await;
    let rows = (0..10017)
        .map(|i| {
            json!({"customer_id":format!("customer-{i:05}"),
        "amount":if i==0 {Value::Null} else {json!(i)},"delta":1})
        })
        .collect::<Vec<_>>();
    for (index, chunk) in rows.chunks(5000).enumerate() {
        let result = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({"relation_id":"orders",
            "relation_version":"2026-08-14.v1","stream_id":"large-orders","partition_id":0,
            "start_offset_inclusive":index * 5000,"rows":chunk}),
        )
        .await;
        assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
    }
    activate(&router, "order_rows").await;
    let mut without_meta = state;
    without_meta.meta_store = None;
    let router_without_meta = app(without_meta);
    for router in [&router, &router_without_meta] {
        for sql in [
            "SELECT count(*) AS count FROM order_rows",
            "WITH selected AS (SELECT * FROM order_rows) SELECT count(*) AS count FROM selected",
        ] {
            counts.clear();
            assert_eq!(
                request_rows(router, "order_rows", sql).await,
                vec![json!({"count":10017})]
            );
            assert_eq!(data_page_reads(&counts), 20);
            assert!(!counts
                .get_paths()
                .iter()
                .any(|path| path.starts_with("v1/standing-runtime-state-payloads/")));
        }
        for (predicate, count, max_pages) in [
            ("customer_id='customer-10016'", 1, 2),
            ("customer_id='customer-10016' AND abs(amount)>0", 1, 2),
            ("customer_id='customer-10016' OR amount IS NULL", 2, 3),
            ("customer_id='missing' AND amount>0", 0, 0),
            ("amount IS NULL", 1, 1),
            ("customer_id='missing' OR abs(amount)>0", 10016, 20),
        ] {
            counts.clear();
            let sql = format!("SELECT count(*) AS count FROM order_rows WHERE {predicate}");
            assert_eq!(
                request_rows(router, "order_rows", &sql).await,
                vec![json!({"count":count})]
            );
            assert!(
                data_page_reads(&counts) <= max_pages,
                "{predicate}: {:?}",
                counts.get_paths()
            );
            assert!(!counts
                .get_paths()
                .iter()
                .any(|path| path.starts_with("v1/standing-runtime-state-payloads/")));
        }
    }
}

#[test]
fn materialized_ipc_rejects_extra_metadata_and_disagreement_between_typed_rows_and_bag() {
    let (checkpoint, schema, key) = page_fixture();
    let publication = pages::publication(
        &checkpoint,
        &schema.relation_id,
        &key,
        DeltaBatch::from_records([float_row("item", json!(1.2345678901234567), 1)]),
    )
    .unwrap();
    let bytes = pages::encode_page(&publication.page_records[0].1).unwrap();
    let batch = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&bytes), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    for extra_metadata in [false, true] {
        let mut metadata = batch.schema().metadata().clone();
        let mut columns = batch.columns().to_vec();
        if extra_metadata {
            metadata.insert("unbound".into(), "extra".into());
        } else {
            columns[1] = Arc::new(arrow::array::Float64Array::from(vec![9.0]));
        }
        let altered = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new_with_metadata(
                batch.schema().fields().clone(),
                metadata,
            )),
            columns,
        )
        .unwrap();
        let mut bytes = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut bytes, altered.schema().as_ref())
                    .unwrap();
            writer.write(&altered).unwrap();
            writer.finish().unwrap();
        }
        assert!(pages::decode_page(&bytes, pages::CODEC).is_err());
    }
}

#[tokio::test]
async fn materialized_float_ipc_cache_revalidates_warm_and_disk_reopened_bytes() {
    use crate::materialized_read_cache::{
        MaterializedReadCache, MaterializedReadCacheConfig, MaterializedReadCacheKey,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (checkpoint, schema, key) = page_fixture();
    let publication = pages::publication(
        &checkpoint,
        &schema.relation_id,
        &key,
        DeltaBatch::from_records([float_row(
            "item",
            json!(f64::from_bits(0x3ff0000000000001)),
            2,
        )]),
    )
    .unwrap();
    let (page_key, page) = &publication.page_records[0];
    let bytes = pages::encode_page(page).unwrap();
    let cache_key = MaterializedReadCacheKey {
        store_id: "synthetic-memory-store".into(),
        namespace: "synthetic".into(),
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        output_id: schema.relation_id.clone(),
        object_path: page_key.as_str().into(),
        content_hash: page.page_content_hash.clone(),
        codec: pages::CODEC.into(),
        checkpoint_epoch: checkpoint.logical_epoch,
        root_hash: publication.manifest_record.output_content_hash.clone(),
    };
    let directory = tempfile::tempdir().unwrap();
    let config = MaterializedReadCacheConfig {
        directory: directory.path().into(),
        memory_bytes: 64 * 1024,
        disk_bytes: 8 * 1024 * 1024,
    };
    let fetched = AtomicUsize::new(0);
    let validated = AtomicUsize::new(0);
    let validator = |bytes: &[u8]| {
        validated.fetch_add(1, Ordering::SeqCst);
        let restored = pages::decode_page(bytes, pages::CODEC)?;
        pages::validate_page(page_key, &restored)?;
        pages::validate_page_binding(&publication.manifest_record, &restored).map(|_| ())
    };
    let cache = MaterializedReadCache::open(config.clone()).await.unwrap();
    for _ in 0..2 {
        let restored = cache
            .read_through(
                cache_key.clone(),
                || async {
                    fetched.fetch_add(1, Ordering::SeqCst);
                    Ok(bytes.clone())
                },
                validator,
            )
            .await
            .unwrap();
        assert_eq!(restored, bytes);
    }
    assert_eq!(fetched.load(Ordering::SeqCst), 1);
    cache.close().await.unwrap();
    drop(cache);
    let cache = MaterializedReadCache::open(config).await.unwrap();
    assert_eq!(cache.get(&cache_key, validator).await.unwrap(), bytes);
    assert!(validated.load(Ordering::SeqCst) >= 3);
    cache.close().await.unwrap();
}

#[test]
fn materialized_ipc_reads_persisted_float_bytes_in_fresh_process() {
    const CHILD_PATH: &str = "VELORIX_SYNTHETIC_MATERIALIZED_IPC_CHILD";
    if let Ok(path) = std::env::var(CHILD_PATH) {
        let path = std::path::PathBuf::from(path);
        let bytes = std::fs::read(&path).unwrap();
        let record = pages::decode_page(&bytes, pages::CODEC).unwrap();
        assert_eq!(record.row_count, 2);
        std::fs::write(path.with_extension("verified"), stable_bytes_hash(&bytes)).unwrap();
        return;
    }
    let (checkpoint, schema, key) = page_fixture();
    let publication = pages::publication(
        &checkpoint,
        &schema.relation_id,
        &key,
        DeltaBatch::from_records([float_row(
            "item",
            json!(f64::from_bits(0x0010000000000001)),
            2,
        )]),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("inventory.arrow");
    std::fs::write(
        &path,
        pages::encode_page(&publication.page_records[0].1).unwrap(),
    )
    .unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact").arg("tests::materialized_output_pages_tests::materialized_ipc_reads_persisted_float_bytes_in_fresh_process")
        .arg("--nocapture").env(CHILD_PATH,&path).status().unwrap();
    assert!(status.success());
    assert_eq!(
        std::fs::read_to_string(path.with_extension("verified"))
            .expect("the selected child test must validate the persisted IPC page"),
        stable_bytes_hash(&std::fs::read(&path).unwrap())
    );
}

#[test]
fn materialized_predicate_ast_rejects_nested_reads_and_preserves_unknown_coercions() {
    use velorix_core::query::{materialized_page_predicate, PagePredicate, QueryBindValue};
    let (_, mut schema, _) = page_fixture();
    schema.columns[1].data_type = SqlDataType::Int64;
    for sql in [
        "WITH selected AS (SELECT * FROM purchases_by_user) SELECT * FROM selected WHERE id='item'",
        "SELECT (SELECT count(*) FROM purchases_by_user) FROM purchases_by_user WHERE id='item'",
        "SELECT * FROM purchases_by_user a JOIN purchases_by_user b ON a.id=b.id WHERE a.id='item'",
        "SELECT * FROM purchases_by_user WHERE id='item' UNION SELECT * FROM purchases_by_user",
        "SELECT * FROM (SELECT * FROM purchases_by_user) p WHERE id='item'",
    ] {
        assert!(
            materialized_page_predicate(sql, &schema, &[]).is_none(),
            "{sql}"
        );
    }
    for condition in [
        "amount BETWEEN 0.0 AND 9007199254740992",
        "amount='7'",
        "NOT(id='item')",
    ] {
        assert!(
            matches!(
                materialized_page_predicate(
                    &format!("SELECT * FROM purchases_by_user WHERE {condition}"),
                    &schema,
                    &[]
                ),
                Some(PagePredicate::Unknown)
            ),
            "{condition}"
        );
    }
    let filtered = materialized_page_predicate(
        "SELECT * FROM purchases_by_user WHERE id=$1 AND abs(amount)>0",
        &schema,
        &[QueryBindValue::Utf8("item".into())],
    )
    .unwrap();
    assert!(
        matches!(filtered,PagePredicate::And(_,unknown) if matches!(*unknown,PagePredicate::Unknown))
    );
    assert!(matches!(
        materialized_page_predicate(
            "SELECT * FROM purchases_by_user WHERE id=$1",
            &schema,
            &[QueryBindValue::Int64(7)]
        ),
        Some(PagePredicate::Unknown)
    ));
}

#[tokio::test]
async fn materialized_public_cursor_preserves_rows_and_rejects_old_epoch_after_signed_update() {
    let state = test_api_state()
        .await
        .with_meta_store(Arc::new(InMemoryMetaStore::default()));
    let router = app(state);
    register(
        &router,
        "orders",
        &[
            ("customer_id", VelorixLogicalTypeV1::Utf8, false),
            ("amount", VelorixLogicalTypeV1::Int64, false),
        ],
    )
    .await;
    create(
        &router,
        "order_copies",
        "select customer_id, amount from orders",
        false,
    )
    .await;
    let ingest = |offset, rows| {
        json!({"relation_id":"orders","relation_version":"2026-08-14.v1",
        "stream_id":"weighted-orders","partition_id":0,"start_offset_inclusive":offset,"rows":rows})
    };
    let result = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        ingest(
            0,
            json!([
        {"customer_id":"alice","amount":10,"delta":1},
        {"customer_id":"alice-other","amount":10,"delta":1},
        {"customer_id":"bob","amount":20,"delta":1}]),
        ),
    )
    .await;
    assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
    let uri = "/v1/views/order_copies/outputs/order_copies/query";
    activate(&router, "order_copies").await;
    let first = call_json(&router, Method::POST, uri, json!({"max_rows":1})).await;
    assert_eq!(first.0, StatusCode::OK, "{}", first.1);
    assert_eq!(
        first.1["rows"],
        json!([{"customer_id":"alice","amount":10}])
    );
    let request = json!({"max_rows":2,"page_token":first.1["next_page_token"],"committed_epoch":first.1["logical_epoch"]});
    let rest = call_json(&router, Method::POST, uri, request.clone()).await;
    assert_eq!(rest.0, StatusCode::OK, "{}", rest.1);
    assert_eq!(
        rest.1["rows"],
        json!([{"customer_id":"alice-other","amount":10},{"customer_id":"bob","amount":20}])
    );
    let result = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        ingest(
            3,
            json!([
        {"customer_id":"alice","amount":10,"delta":-1}]),
        ),
    )
    .await;
    assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
    assert_eq!(
        request_rows(
            &router,
            "order_copies",
            "SELECT count(*) AS count, sum(amount) AS sum FROM order_copies"
        )
        .await,
        vec![json!({"count":2,"sum":30})]
    );
    let stale = call_json(&router, Method::POST, uri, request).await;
    assert_eq!(stale.0, StatusCode::BAD_REQUEST, "{}", stale.1);
}
