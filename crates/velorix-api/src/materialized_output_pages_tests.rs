//! Synthetic correctness and I/O checks for schema-bound materialized pages.
use super::*;
use crate::materialized_output_pages as pages;
use velorix_core::delta::{DeltaKey, DeltaRecord, DeltaValue};

fn page_fixture() -> (RuntimeCheckpoint, RelationSchema, ObjectKey) {
    let schema = RelationSchema {
        relation_id: "left_table".into(),
        relation_name: "left_table".into(),
        relation_version: "synthetic-v1".into(),
        schema_fingerprint: stable_bytes_hash(b"synthetic-right_table-floats"),
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
    let payload = json!({"output_schema":schema});
    let payload = velorix_core::arrow_state::to_state(&payload).unwrap();
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
        json!({"catalog":test_relation_catalog_for_e2e(relation, columns),(concat!("default_", "orde", "rs_sum_count")):false})).await;
    assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
}

async fn create(router: &Router, view: &str, sql: &str, join: bool) {
    let mut request = json!({"view_id":view,"sql":sql,"input_relation_id":"left_table",
        "input_relation_version":"2026-08-14.v1","response_formats":["json"]});
    if join {
        request["input_relation_id"] = json!("");
        request["input_relation_version"] = json!("");
        request["input_relation_refs"] = json!([
            {"relation_id":"left_table","relation_version":"2026-08-14.v1"},
            {"relation_id":"right_table","relation_version":"2026-08-14.v1"}]);
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
        "left_table",
        &[
            ("key_col", VelorixLogicalTypeV1::Utf8, false),
            ("amount", VelorixLogicalTypeV1::Int64, false),
        ],
    )
    .await;
    register(
        &router,
        "right_table",
        &[
            ("key_col", VelorixLogicalTypeV1::Utf8, false),
            ("value_col", VelorixLogicalTypeV1::Int64, false),
        ],
    )
    .await;
    create(
        &router,
        "projected_output",
        "select key_col, amount from left_table where amount > 5",
        false,
    )
    .await;
    create(&router, "grouped_output", "select key_col, sum(amount) as total, count(*) as count, min(amount) as minimum, max(amount) as maximum, avg(amount) as average from left_table group by key_col", false).await;
    create(&router, "joined_output", "select i.key_col, sum(o.amount) as sum, count(*) as count from left_table o join right_table i on o.key_col = i.key_col group by i.key_col", true).await;
    let result = call_json(&router, Method::POST, "/v1/relations/ingest", json!({"batches":[
        {"relation_id":"left_table","relation_version":"2026-08-14.v1","stream_id":"left_table-stream","partition_id":0,"start_offset_inclusive":0,
         "rows":[{"key_col":"k1","amount":10,"delta":1},{"key_col":"k1","amount":6,"delta":1},{"key_col":"k2","amount":2,"delta":1}]},
        {"relation_id":"right_table","relation_version":"2026-08-14.v1","stream_id":"right_table-stream","partition_id":0,"start_offset_inclusive":0,
         "rows":[{"key_col":"k1","value_col":3,"delta":1}]}
    ]})).await;
    assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
    for view in ["projected_output", "grouped_output", "joined_output"] {
        activate(&router, view).await;
    }
    assert_eq!(
        request_rows(
            &router,
            "projected_output",
            "SELECT count(*) AS count FROM projected_output"
        )
        .await,
        vec![json!({"count":2})]
    );
    assert_eq!(
        request_rows(
            &router,
            "grouped_output",
            "SELECT * FROM grouped_output WHERE key_col='k1'"
        )
        .await,
        vec![json!({"key_col":"k1","total":16,"count":2,"minimum":6,"maximum":10,"average":8.0})]
    );
    assert_eq!(
        request_rows(&router, "joined_output", "SELECT * FROM joined_output").await,
        vec![json!({"key_col":"k1","sum":16,"count":2})]
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
        .with_standing_runtime_budget_limits(100_000, 16 * 1024 * 1024)
        .with_meta_store(Arc::new(InMemoryMetaStore::default()));
    let router = app(state.clone());
    register(
        &router,
        "left_table",
        &[
            ("key_col", VelorixLogicalTypeV1::Utf8, false),
            ("amount", VelorixLogicalTypeV1::Int64, true),
        ],
    )
    .await;
    create(
        &router,
        "row_output",
        "select key_col, amount from left_table",
        false,
    )
    .await;
    let rows = (0..10017)
        .map(|i| {
            json!({"key_col":format!("k{i:05}"),
        "amount":if i==0 {Value::Null} else {json!(i)},"delta":1})
        })
        .collect::<Vec<_>>();
    for (index, chunk) in rows.chunks(5000).enumerate() {
        let result = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({"relation_id":"left_table",
            "relation_version":"2026-08-14.v1","stream_id":"large-left_table","partition_id":0,
            "start_offset_inclusive":index * 5000,"rows":chunk}),
        )
        .await;
        assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
    }
    activate(&router, "row_output").await;
    let mut without_meta = state;
    without_meta.meta_store = None;
    let router_without_meta = app(without_meta);
    for router in [&router, &router_without_meta] {
        for sql in [
            "SELECT count(*) AS count FROM row_output",
            "WITH selected AS (SELECT * FROM row_output) SELECT count(*) AS count FROM selected",
        ] {
            counts.clear();
            assert_eq!(
                request_rows(router, "row_output", sql).await,
                vec![json!({"count":10017})]
            );
            assert_eq!(data_page_reads(&counts), 20);
            assert!(!counts
                .get_paths()
                .iter()
                .any(|path| path.starts_with("v1/standing-runtime-state-payloads/")));
        }
        for (predicate, count, max_pages) in [
            ("key_col='k10016'", 1, 2),
            ("key_col='k10016' AND abs(amount)>0", 1, 2),
            ("key_col='k10016' OR amount IS NULL", 2, 3),
            ("key_col='missing' AND amount>0", 0, 0),
            ("amount IS NULL", 1, 1),
            ("key_col='missing' OR abs(amount)>0", 10016, 20),
        ] {
            counts.clear();
            let sql = format!("SELECT count(*) AS count FROM row_output WHERE {predicate}");
            assert_eq!(
                request_rows(router, "row_output", &sql).await,
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
    let path = directory.path().join("right_table.arrow");
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
        "WITH selected AS (SELECT * FROM left_table) SELECT * FROM selected WHERE id='item'",
        "SELECT (SELECT count(*) FROM left_table) FROM left_table WHERE id='item'",
        "SELECT * FROM left_table a JOIN left_table b ON a.id=b.id WHERE a.id='item'",
        "SELECT * FROM left_table WHERE id='item' UNION SELECT * FROM left_table",
        "SELECT * FROM (SELECT * FROM left_table) p WHERE id='item'",
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
                    &format!("SELECT * FROM left_table WHERE {condition}"),
                    &schema,
                    &[]
                ),
                Some(PagePredicate::Unknown)
            ),
            "{condition}"
        );
    }
    let filtered = materialized_page_predicate(
        "SELECT * FROM left_table WHERE id=$1 AND abs(amount)>0",
        &schema,
        &[QueryBindValue::Utf8("item".into())],
    )
    .unwrap();
    assert!(
        matches!(filtered,PagePredicate::And(_,unknown) if matches!(*unknown,PagePredicate::Unknown))
    );
    assert!(matches!(
        materialized_page_predicate(
            "SELECT * FROM left_table WHERE id=$1",
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
        "left_table",
        &[
            ("key_col", VelorixLogicalTypeV1::Utf8, false),
            ("amount", VelorixLogicalTypeV1::Int64, false),
        ],
    )
    .await;
    create(
        &router,
        "copied_output",
        "select key_col, amount from left_table",
        false,
    )
    .await;
    let ingest = |offset, rows| {
        json!({"relation_id":"left_table","relation_version":"2026-08-14.v1",
        "stream_id":"weighted-left_table","partition_id":0,"start_offset_inclusive":offset,"rows":rows})
    };
    let result = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        ingest(
            0,
            json!([
        {"key_col":"k1","amount":10,"delta":1},
        {"key_col":"k1_other","amount":10,"delta":1},
        {"key_col":"k2","amount":20,"delta":1}]),
        ),
    )
    .await;
    assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
    let uri = "/v1/views/copied_output/outputs/copied_output/query";
    activate(&router, "copied_output").await;
    let first = call_json(&router, Method::POST, uri, json!({"max_rows":1})).await;
    assert_eq!(first.0, StatusCode::OK, "{}", first.1);
    assert_eq!(first.1["rows"], json!([{"key_col":"k1","amount":10}]));
    let request = json!({"max_rows":2,"page_token":first.1["next_page_token"],"committed_epoch":first.1["logical_epoch"]});
    let rest = call_json(&router, Method::POST, uri, request.clone()).await;
    assert_eq!(rest.0, StatusCode::OK, "{}", rest.1);
    assert_eq!(
        rest.1["rows"],
        json!([{"key_col":"k1_other","amount":10},{"key_col":"k2","amount":20}])
    );
    let result = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        ingest(
            3,
            json!([
        {"key_col":"k1","amount":10,"delta":-1}]),
        ),
    )
    .await;
    assert_eq!(result.0, StatusCode::CREATED, "{}", result.1);
    assert_eq!(
        request_rows(
            &router,
            "copied_output",
            "SELECT count(*) AS count, sum(amount) AS sum FROM copied_output"
        )
        .await,
        vec![json!({"count":2,"sum":30})]
    );
    let stale = call_json(&router, Method::POST, uri, request).await;
    assert_eq!(stale.0, StatusCode::BAD_REQUEST, "{}", stale.1);
}

// Standalone synthetic fixtures; these do not load external harnesses or workloads.
fn publication_checkpoint(
    key_type: SqlDataType,
    key_value: Value,
    weight: i64,
) -> RuntimeCheckpoint {
    use velorix_core::standing_program::DurableStateRoot;
    let schema = RelationSchema {
        relation_id: "synthetic_output".into(),
        relation_name: "synthetic_output".into(),
        relation_version: "synthetic-v1".into(),
        schema_fingerprint: stable_bytes_hash(format!("synthetic-{key_type:?}").as_bytes()),
        columns: vec![
            ColumnSchema {
                name: "k".into(),
                data_type: key_type,
                nullable: false,
            },
            ColumnSchema {
                name: "v".into(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["k".into()],
    };
    let output = if weight == 0 {
        DeltaBatch::default()
    } else {
        DeltaBatch::from_records([DeltaRecord::new(
            DeltaKey::from_json(key_value),
            DeltaValue::from_json(json!({"v":17})),
            weight,
        )])
    };
    let payload = json!({"schema_version":1,"output_schema":schema,"published_output":output});
    let payload = velorix_core::arrow_state::to_state(&payload).unwrap();
    RuntimeCheckpoint {
        identity: StandingProgramIdentity {
            tenant_id: "synthetic_tenant".into(),
            program_id: "synthetic_program".into(),
            view_ids: vec!["synthetic_output".into()],
            sql_hash: stable_bytes_hash(b"synthetic-plan"),
            input_catalog_hash: stable_bytes_hash(b"synthetic-catalog"),
            output_schema_hash: stable_bytes_hash(b"synthetic-schema"),
            planner_identity: "velorix-logical-view-planner-v1".into(),
            builtin_runtime_identities: vec![BuiltinRuntimeIdentity {
                name: "velorix-runtime".into(),
                version: "test".into(),
            }],
            runtime_capabilities: vec!["materialized-view-runtime".into()],
            runtime_compatibility: "velorix-materialized-view-runtime-v1".into(),
            checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".into(),
            native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
        },
        logical_epoch: 1,
        input_frontiers: vec![RelationFrontier {
            relation_id: "synthetic_input".into(),
            relation_version: "synthetic-v1".into(),
            stream_id: "synthetic_stream".into(),
            partition_id: 0,
            committed_offset_exclusive: 1,
        }],
        input_event_time_frontiers: Vec::new(),
        output_frontiers: vec![ViewFrontier {
            view_id: "synthetic_output".into(),
            committed_epoch: 1,
        }],
        checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".into(),
        state_root: DurableStateRoot {
            object_key: "synthetic-state".into(),
            content_hash: stable_bytes_hash(payload.as_bytes()),
        },
        state_payload: Some(RuntimeCheckpointStatePayload {
            codec_identity: "velorix-standing-program-checkpoint-v1".into(),
            payload,
        }),
        output_manifest_refs: Vec::new(),
        owner_epoch: None,
        input_coverage: None,
        causal_cut: None,
    }
}

fn publication_key(checkpoint: &RuntimeCheckpoint) -> ObjectKey {
    ObjectKey::standing_runtime_checkpoint(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        "synthetic_output",
        checkpoint.logical_epoch,
        &checkpoint.state_root.content_hash,
    )
    .unwrap()
}

async fn publication_state(store: Arc<dyn ObjectStore>, meta: Arc<InMemoryMetaStore>) -> ApiState {
    let authority = validate_operator_authority(
        ObjectStoreAuthorityRef {
            store_id: "synthetic_store".into(),
            namespace: "synthetic_namespace".into(),
        },
        store,
        "memory",
        "synthetic-publication",
    )
    .await
    .unwrap();
    ApiState::from_validated_authority_with_ingest_admission_startup(
        authority,
        "synthetic-owner",
        false,
    )
    .await
    .unwrap()
    .with_meta_store(meta)
}

fn publication_schema(checkpoint: &RuntimeCheckpoint) -> RelationSchema {
    let payload: Value =
        velorix_core::arrow_state::from_state(&checkpoint.state_payload.as_ref().unwrap().payload)
            .unwrap();
    serde_json::from_value(payload["output_schema"].clone()).unwrap()
}

#[tokio::test]
async fn publication_checkpoint_future_has_bounded_inline_size() {
    let checkpoint = publication_checkpoint(SqlDataType::Int32, json!(7), 1);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = publication_state(store, Arc::new(InMemoryMetaStore::default())).await;
    let future = persist_standing_runtime_checkpoint(
        &state,
        "synthetic_output",
        &checkpoint,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    );
    let bytes = std::mem::size_of_val(&future);
    eprintln!("checkpoint publication inline future: {bytes} bytes");
    // This future is embedded in ingest and recovery; reserve under 1% of a 2 MiB stack.
    assert!(
        bytes <= 19 * 1024,
        "checkpoint publication future uses {bytes} bytes"
    );
}

#[test]
fn publication_scalar_key_matrix_preserves_native_ipc_including_empty_snapshots() {
    for (key_type, value) in [
        (SqlDataType::Int8, json!(-128)),
        (SqlDataType::Int16, json!(-32768)),
        (SqlDataType::Int32, json!(i32::MIN)),
        (SqlDataType::Int64, json!(i64::MIN)),
        (SqlDataType::UInt8, json!(u8::MAX)),
        (SqlDataType::UInt16, json!(u16::MAX)),
        (SqlDataType::UInt32, json!(u32::MAX)),
        (SqlDataType::UInt64, json!(u64::MAX)),
        (SqlDataType::Float32, json!(1.25)),
        (
            SqlDataType::Float32,
            json!(f32::from_bits(0x3dcccccd) as f64),
        ),
        (SqlDataType::Float64, json!(1.25)),
        (SqlDataType::Bool, json!(true)),
        (SqlDataType::Utf8, json!("synthetic-key")),
        (SqlDataType::Char { length: Some(1) }, json!("x")),
        (
            SqlDataType::Uuid,
            json!("00000000-0000-0000-0000-000000000001"),
        ),
        (SqlDataType::Binary { length: 2 }, json!("0x0102")),
        (SqlDataType::Varbinary, json!("0x0102")),
        (SqlDataType::Date, json!(1)),
        (SqlDataType::Time, json!(42)),
        (SqlDataType::Timestamp { timezone: None }, json!(42)),
        (
            SqlDataType::Timestamp {
                timezone: Some("UTC".into()),
            },
            json!(42),
        ),
        (
            SqlDataType::Decimal {
                precision: 10,
                scale: 2,
            },
            json!("1.25"),
        ),
        (SqlDataType::Json, json!({"synthetic":7})),
        (SqlDataType::Json, json!("synthetic-key")),
    ] {
        for weight in [0, 1] {
            let checkpoint = publication_checkpoint(key_type.clone(), value.clone(), weight);
            let schema = publication_schema(&checkpoint);
            let encoded_schema =
                velorix_runtime::materialized_view_runtime::materialized_canonical_output_batch(
                    &schema,
                    &DeltaBatch::default(),
                )
                .unwrap()
                .schema();
            assert_eq!(
                arrow_schema_from_incremental_relation_schema(&schema).unwrap(),
                encoded_schema,
                "{key_type:?}"
            );
            let key = publication_key(&checkpoint);
            let publication = pages::checkpoint_publication(&checkpoint, "synthetic_output", &key)
                .unwrap_or_else(|error| panic!("{key_type:?}: {}", error.message))
                .unwrap();
            assert_eq!(publication.manifest_record.output_encoding, pages::CODEC);
            assert_eq!(
                publication.manifest_record.output_row_count,
                weight as usize
            );
            pages::validate_checkpoint_binding(&publication.manifest_record, &checkpoint).unwrap();
            for (key, page) in &publication.page_records {
                let bytes = pages::encode_page(page).unwrap();
                let restored = pages::decode_page(&bytes, pages::CODEC).unwrap_or_else(|error| {
                    panic!("{key_type:?}, weight {weight}: {}", error.message)
                });
                pages::validate_page(key, &restored).unwrap();
                assert_eq!(
                    pages::validate_page_binding(&publication.manifest_record, &restored).unwrap(),
                    pages::validate_page_binding(&publication.manifest_record, page).unwrap(),
                );
            }
        }
    }
}

#[test]
fn publication_rejects_invalid_typed_keys_and_negative_bags() {
    for (key_type, key) in [
        (SqlDataType::Int32, json!(i64::MAX)),
        (SqlDataType::UInt32, json!(-1)),
        (SqlDataType::Float32, json!(f64::MAX)),
        (SqlDataType::Binary { length: 2 }, json!("0x01")),
        (SqlDataType::Varbinary, json!("0xzz")),
        (SqlDataType::Int64, Value::Null),
    ] {
        for weight in [1, 8192 * 512 + 1] {
            let checkpoint = publication_checkpoint(key_type.clone(), key.clone(), weight);
            assert!(pages::checkpoint_publication(
                &checkpoint,
                "synthetic_output",
                &publication_key(&checkpoint)
            )
            .is_err());
        }
    }
    let checkpoint = publication_checkpoint(SqlDataType::Int64, json!(1), -1);
    assert!(pages::checkpoint_publication(
        &checkpoint,
        "synthetic_output",
        &publication_key(&checkpoint)
    )
    .is_err());
}

#[test]
fn publication_page_capacity_boundary_retains_eligible_index() {
    for count in [8192 * 512 - 1, 8192 * 512] {
        let checkpoint = publication_checkpoint(SqlDataType::Int64, json!(1), count);
        let publication = pages::checkpoint_publication(
            &checkpoint,
            "synthetic_output",
            &publication_key(&checkpoint),
        )
        .unwrap()
        .unwrap();
        assert_eq!(publication.manifest_record.output_row_count, count as usize);
        assert_eq!(publication.page_records.len(), 8192);
        pages::validate_checkpoint_binding(&publication.manifest_record, &checkpoint).unwrap();
    }
}

#[tokio::test]
async fn publication_above_page_capacity_preserves_bag_checkpoint_pointer_and_bounded_pages() {
    let meta = Arc::new(InMemoryMetaStore::default());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = publication_state(store.clone(), meta.clone()).await;
    let checkpoint = publication_checkpoint(SqlDataType::Int64, json!(1), 8192 * 512 + 1);
    assert!(pages::checkpoint_publication(
        &checkpoint,
        "synthetic_output",
        &publication_key(&checkpoint)
    )
    .unwrap()
    .is_none());
    let owner = state
        .acquire_standing_runtime_owner(&checkpoint.identity, "synthetic_output")
        .await
        .unwrap()
        .unwrap();
    persist_standing_runtime_checkpoint(
        &state,
        "synthetic_output",
        &checkpoint,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner)),
        None,
    )
    .await
    .unwrap();
    let pointer = meta
        .read_standing_runtime_checkpoint(
            "synthetic_tenant",
            "synthetic_program",
            "synthetic_output",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        pointer.checkpoint_key,
        publication_key(&checkpoint).as_str()
    );
    assert!(pointer.output_manifest_refs.is_empty());
    let restarted = publication_state(store, meta).await;
    let restored = read_latest_standing_runtime_checkpoint(
        &restarted,
        &checkpoint.identity,
        "synthetic_output",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(restored.checkpoint.state_payload, checkpoint.state_payload);
    let schema = publication_schema(&restored.checkpoint);
    let output: DeltaBatch = serde_json::from_value(
        standing_runtime_checkpoint_published_output(&restored.checkpoint).unwrap(),
    )
    .unwrap();
    assert_eq!(output.records().len(), 1);
    assert_eq!(output.records()[0].weight, 8192 * 512 + 1);
    let scoped = ScopedViewId {
        tenant_id: "synthetic_tenant".into(),
        program_id: "synthetic_program".into(),
        view_id: "synthetic_output".into(),
    };
    let first = velorix_runtime::materialized_view_runtime::materialized_delta_to_page(
        &schema,
        &output,
        scoped.clone(),
        1,
        SnapshotPageRequest {
            max_rows: Some(3),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    assert_eq!(first.batches[0].num_rows(), 3);
    let second = velorix_runtime::materialized_view_runtime::materialized_delta_to_page(
        &schema,
        &output,
        scoped.clone(),
        1,
        SnapshotPageRequest {
            max_rows: Some(3),
            page_token: first.next_page_token.clone(),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    assert_eq!(second.batches[0].num_rows(), 3);
    assert_ne!(first.next_page_token, second.next_page_token);
    assert!(
        velorix_runtime::materialized_view_runtime::materialized_delta_to_page(
            &schema,
            &output,
            scoped,
            2,
            SnapshotPageRequest {
                max_rows: Some(3),
                page_token: first.next_page_token,
                ..Default::default()
            },
            None,
        )
        .is_err()
    );
    let error = super::super::query_serving::legacy_materialized_page(
        &restarted,
        "synthetic_output",
        &checkpoint.identity,
        "synthetic_output",
        &schema,
        SnapshotPageRequest {
            max_rows: Some(3),
            ..Default::default()
        },
        false,
        QueryPolicy {
            memory_limit_bytes: Some(1024),
            ..Default::default()
        },
        0,
        0,
    )
    .await
    .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn publication_duplicate_and_orphan_reuse_original_descriptor_bytes() {
    for published in [false, true] {
        let meta = Arc::new(InMemoryMetaStore::default());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = publication_state(store.clone(), meta.clone()).await;
        let checkpoint = publication_checkpoint(SqlDataType::Int32, json!(7), 1);
        let owner = state
            .acquire_standing_runtime_owner(&checkpoint.identity, "synthetic_output")
            .await
            .unwrap()
            .unwrap();
        let (state_key, payload) =
            standing_runtime_state_payload_record_for_checkpoint(&checkpoint, "synthetic_output")
                .unwrap();
        persist_standing_runtime_state_payload(&state, &state_key, &payload)
            .await
            .unwrap();
        // This is the base-version publication: durable state, no native manifest ref.
        let record = StandingRuntimeCheckpointRecord {
            demand_materialization: None,
            published_input_bootstrap: None,
            schema_version: 1,
            record_kind: "standing_runtime_checkpoint_v1".into(),
            view_id: "synthetic_output".into(),
            checkpoint_key: publication_key(&checkpoint).as_str().into(),
            previous_checkpoint: None,
            checkpoint: standing_runtime_checkpoint_with_durable_publication_refs(
                &checkpoint,
                None,
                &[],
                &state_key,
            ),
            replay_checkpoints: Vec::new(),
            manifest_hash: String::new(),
        };
        let original =
            checkpoint_record::encode(&record, checkpoint_record::MAX_ENCODED_RECORD_BYTES)
                .unwrap();
        let path = Path::from(record.checkpoint_key.clone());
        store
            .put_opts(&path, original.clone().into(), PutMode::Create.into())
            .await
            .unwrap();
        if published {
            publish_standing_runtime_checkpoint_pointer(
                &state,
                None,
                standing_runtime_checkpoint_pointer_from_record(&record),
                Some(owner.clone()),
                None,
            )
            .await
            .unwrap();
        }
        persist_standing_runtime_checkpoint(
            &state,
            "synthetic_output",
            &checkpoint,
            &[],
            StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner.clone())),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            original.as_slice()
        );
        let pointer = meta
            .read_standing_runtime_checkpoint(
                "synthetic_tenant",
                "synthetic_program",
                "synthetic_output",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pointer.manifest_hash, stable_bytes_hash(&original));
        assert!(pointer.output_manifest_refs.is_empty());
        let restored = read_latest_standing_runtime_checkpoint(
            &state,
            &checkpoint.identity,
            "synthetic_output",
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(restored.checkpoint.state_payload, checkpoint.state_payload);
        let mut conflict = checkpoint.clone();
        conflict.owner_epoch = Some(9);
        let error = persist_standing_runtime_checkpoint(
            &state,
            "synthetic_output",
            &conflict,
            &[],
            StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner)),
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::CONFLICT);
        assert_eq!(
            store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            original.as_slice()
        );
        assert_eq!(
            meta.read_standing_runtime_checkpoint(
                "synthetic_tenant",
                "synthetic_program",
                "synthetic_output"
            )
            .await
            .unwrap()
            .unwrap(),
            pointer
        );
    }
}

fn publication_catalog(
    logical_type: VelorixLogicalTypeV1,
    physical_type: ArrowPhysicalTypeV1,
) -> VelorixRelationCatalogV1 {
    use velorix_core::relation::CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID;
    VelorixRelationCatalogV1::from_relation_schema(
        VelorixRelationSchemaV1 {
            relation_id: "synthetic_input".into(),
            relation_name: "synthetic_input".into(),
            relation_version: "synthetic-v1".into(),
            columns: vec![
                RelationColumnV1 {
                    column_id: "k".into(),
                    name: "k".into(),
                    logical_type,
                    physical_arrow_type: physical_type,
                    nullable: false,
                    ordinal: 0,
                    semantic_role: RelationSemanticRoleV1::PrimaryKey,
                },
                RelationColumnV1 {
                    column_id: "v".into(),
                    name: "v".into(),
                    logical_type: VelorixLogicalTypeV1::Int64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                    nullable: false,
                    ordinal: 1,
                    semantic_role: RelationSemanticRoleV1::Value,
                },
                RelationColumnV1 {
                    column_id: "weight".into(),
                    name: "weight".into(),
                    logical_type: VelorixLogicalTypeV1::Int64,
                    physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                    nullable: false,
                    ordinal: 2,
                    semantic_role: RelationSemanticRoleV1::Weight,
                },
            ],
            primary_key_column_ids: vec!["k".into()],
            weight_column_id: "weight".into(),
            allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
            event_time_column_id: None,
        },
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap()
}

#[test]
fn publication_admission_rejects_nested_keys_and_keeps_value_type_rules() {
    let catalog = publication_catalog(
        VelorixLogicalTypeV1::Array {
            element_type: Box::new(VelorixLogicalTypeV1::Int64),
        },
        ArrowPhysicalTypeV1::List {
            element_type: Box::new(ArrowPhysicalTypeV1::Int64),
        },
    );
    let error = validate_supported_filter_project_sql("select k,v from synthetic_input", &catalog)
        .unwrap_err();
    assert!(error.to_string().contains("primary key type"));
    let mut schema = publication_catalog(VelorixLogicalTypeV1::Int32, ArrowPhysicalTypeV1::Int32)
        .relation_schema;
    schema.columns[1].logical_type = VelorixLogicalTypeV1::Int32;
    schema.columns[1].physical_arrow_type = ArrowPhysicalTypeV1::Int32;
    let catalog = VelorixRelationCatalogV1::from_relation_schema(
        schema,
        velorix_core::relation::CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap();
    assert!(
        validate_supported_filter_project_sql("select k,v from synthetic_input", &catalog).is_err()
    );
}

#[tokio::test]
async fn publication_primitive_keys_admit_ingest_publish_and_restore() {
    for (logical, physical, value) in [
        (
            VelorixLogicalTypeV1::Int32,
            ArrowPhysicalTypeV1::Int32,
            json!(-7),
        ),
        (
            VelorixLogicalTypeV1::UInt32,
            ArrowPhysicalTypeV1::UInt32,
            json!(u32::MAX),
        ),
        (
            VelorixLogicalTypeV1::Float32,
            ArrowPhysicalTypeV1::Float32,
            json!(1.25),
        ),
        (
            VelorixLogicalTypeV1::Timestamp {
                timezone: Some("UTC".into()),
            },
            ArrowPhysicalTypeV1::TimestampNanosecond {
                timezone: Some("UTC".into()),
            },
            json!(42),
        ),
    ] {
        let timezone_key = matches!(
            &logical,
            VelorixLogicalTypeV1::Timestamp { timezone: Some(_) }
        );
        let meta = Arc::new(InMemoryMetaStore::default());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = publication_state(store.clone(), meta.clone()).await;
        let router = app(state.clone());
        let registered = call_json(&router, Method::POST, "/v1/relations",
            json!({"catalog":publication_catalog(logical,physical),(concat!("default_", "orde", "rs_sum_count")):false})).await;
        assert_eq!(registered.0, StatusCode::CREATED, "{}", registered.1);
        // A retained cancelling batch supplies the bootstrap frontier with empty output.
        let seeded = call_json(&router, Method::POST, "/v1/relations/ingest", json!({"batches":[{
            "relation_id":"synthetic_input","relation_version":"synthetic-v1","stream_id":"synthetic_stream",
            "partition_id":0,"start_offset_inclusive":0,"rows":[
                {"k":value,"v":17,"weight":1}, {"k":value,"v":17,"weight":-1}
            ]
        }]})).await;
        assert_eq!(seeded.0, StatusCode::CREATED, "{}", seeded.1);
        let created = call_json(&router, Method::POST, "/v1/views", json!({
            "view_id":"synthetic_output", "sql":"select k,v from synthetic_input",
            "input_relation_id":"synthetic_input", "input_relation_version":"synthetic-v1", "response_formats":["json"]
        })).await;
        if timezone_key {
            // Native IPC preserves UTC; SQL admission still rejects timezone-bearing inputs.
            assert_eq!(created.0, StatusCode::BAD_REQUEST, "{}", created.1);
            let error = created.1["error"].as_str().unwrap();
            assert!(error.contains("spec.input_relations.synthetic_input.k"));
            assert!(error.contains("timezone-bearing timestamps are not supported yet"));
            assert!(error.contains("timezone=UTC"));
            assert!(state
                .view_registry()
                .unwrap()
                .list_active()
                .await
                .unwrap()
                .is_empty());
            assert!(meta
                .read_view_bootstrap("default", "synthetic_output", "synthetic_output")
                .await
                .unwrap()
                .is_none());
            assert!(meta
                .read_standing_runtime_checkpoint("default", "synthetic_output", "synthetic_output")
                .await
                .unwrap()
                .is_none());
            let checkpoint_prefix = Path::from(
                "v1/standing-runtime-checkpoints/default/synthetic_output/synthetic_output/epochs",
            );
            assert!(store
                .list(Some(&checkpoint_prefix))
                .try_next()
                .await
                .unwrap()
                .is_none());
            continue;
        }
        assert_eq!(created.0, StatusCode::CREATED, "{}", created.1);
        let activated = call_json(
            &router,
            Method::POST,
            "/v1/views/synthetic_output/backfill",
            json!({}),
        )
        .await;
        assert_eq!(activated.0, StatusCode::OK, "{}", activated.1);
        let empty = call_json(
            &router,
            Method::POST,
            "/v1/views/synthetic_output/query",
            json!({"sql":"select * from synthetic_output"}),
        )
        .await;
        assert_eq!(empty.0, StatusCode::OK, "{}", empty.1);
        assert_eq!(empty.1["rows"], json!([]));
        let ingested = call_json(&router, Method::POST, "/v1/relations/ingest", json!({"batches":[{
            "relation_id":"synthetic_input","relation_version":"synthetic-v1","stream_id":"synthetic_stream",
            "partition_id":0,"start_offset_inclusive":2,"rows":[{"k":value,"v":17,"weight":1}]
        }]})).await;
        assert_eq!(ingested.0, StatusCode::CREATED, "{}", ingested.1);
        let rows = call_json(
            &router,
            Method::POST,
            "/v1/views/synthetic_output/query",
            json!({"sql":"select * from synthetic_output"}),
        )
        .await;
        assert_eq!(rows.0, StatusCode::OK, "{}", rows.1);
        assert_eq!(rows.1["rows"], json!([{"k":value,"v":17}]));
        let restarted = publication_state(store, meta).await;
        restarted
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap();
        let rows = call_json(
            &app(restarted),
            Method::POST,
            "/v1/views/synthetic_output/query",
            json!({"sql":"select * from synthetic_output"}),
        )
        .await;
        assert_eq!(rows.0, StatusCode::OK, "{}", rows.1);
        assert_eq!(rows.1["rows"], json!([{"k":value,"v":17}]));
    }
}

#[tokio::test]
async fn publication_descriptor_collector_bounds_actual_bytes_with_understated_metadata() {
    let store = InMemory::new();
    let path = Path::from("synthetic-descriptor");
    store
        .put_opts(
            &path,
            bytes::Bytes::from_static(b"1234").into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    let mut result = store.get(&path).await.unwrap();
    result.meta.size = 0;
    let error = collect_object_bytes_bounded(result, Some(3))
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    let result = store.get(&path).await.unwrap();
    assert_eq!(
        collect_object_bytes_bounded(result, Some(4)).await.unwrap(),
        b"1234"
    );
}
