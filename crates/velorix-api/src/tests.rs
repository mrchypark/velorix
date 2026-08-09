use super::*;
use axum::http::Method;
use futures::stream::BoxStream;
use object_store::{
    memory::InMemory, path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use std::{fmt, time::Duration};
use tower::ServiceExt as _;
use velorix_control::meta_admin::InMemoryMetaStore;
use velorix_core::{
    delta::{DeltaKey, DeltaRecord, DeltaValue},
    standing_program::{
        DurableStateRoot, RelationFrontier, RuntimeCheckpointStatePayload, ViewFrontier,
    },
};

fn template_result_schema() -> RelationSchema {
    RelationSchema {
        relation_id: "template_output".to_string(),
        relation_name: "template_output".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: "sha256:template-output".to_string(),
        columns: vec![
            ColumnSchema {
                name: "name".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "amount".to_string(),
                data_type: SqlDataType::Int64,
                nullable: true,
            },
            ColumnSchema {
                name: "value".to_string(),
                data_type: SqlDataType::Json,
                nullable: true,
            },
        ],
        primary_key: vec!["name".to_string()],
    }
}

fn response_column(name: &str, r#type: &str, source: &str) -> MaterializedViewResponseColumnSpec {
    MaterializedViewResponseColumnSpec {
        name: name.to_string(),
        r#type: r#type.to_string(),
        source: source.to_string(),
        description: None,
    }
}

#[tokio::test]
async fn sql_template_response_schema_sources_are_admitted_against_result_schema() {
    let output = template_result_schema();
    let table_schema = arrow_schema_from_incremental_relation_schema(&output).unwrap();
    let response_schema = MaterializedViewResponseSchema {
        columns: vec![
            response_column("display_name", "string", "name"),
            response_column("amount", "integer", "amount"),
            // Legacy views expose JSON payloads as `value`; planner fields
            // cannot describe their dynamic members at admission time.
            response_column("nested_name", "string", "value.profile.name"),
        ],
    };

    validate_sql_template_response_schema_bindings(
        "template_view",
        Some(&response_schema),
        "template_output",
        table_schema,
        "select name, amount, value from template_output",
        &[],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn sql_template_response_schema_rejects_unknown_wrong_type_and_ambiguous_sources() {
    let output = template_result_schema();
    let schema = arrow_schema_from_incremental_relation_schema(&output).unwrap();
    for (response_schema, sql, expected) in [
        (
            MaterializedViewResponseSchema {
                columns: vec![response_column("missing", "string", "missing")],
            },
            "select name, amount from template_output",
            "is not a template result column",
        ),
        (
            MaterializedViewResponseSchema {
                columns: vec![response_column("amount", "integer", "name")],
            },
            "select name, amount from template_output",
            "is incompatible",
        ),
        (
            MaterializedViewResponseSchema {
                columns: vec![response_column("duplicate", "string", "duplicate")],
            },
            "select name as duplicate, amount as duplicate from template_output",
            "same name",
        ),
    ] {
        let error = validate_sql_template_response_schema_bindings(
            "template_view",
            Some(&response_schema),
            "template_output",
            schema.clone(),
            sql,
            &[],
        )
        .await
        .unwrap_err();
        assert!(error.message.contains(expected), "{error:?}");
    }
}

fn legacy_key(prefix: &str, suffix: &str) -> String {
    format!("{prefix}{suffix}")
}

#[derive(Clone, Debug, Default)]
struct ObjectStoreAccessCounts {
    get_paths: Arc<Mutex<Vec<String>>>,
    list_prefixes: Arc<Mutex<Vec<String>>>,
    active_ingest_puts: Arc<std::sync::atomic::AtomicUsize>,
    max_concurrent_ingest_puts: Arc<std::sync::atomic::AtomicUsize>,
}

impl ObjectStoreAccessCounts {
    fn clear(&self) {
        self.get_paths.lock().unwrap().clear();
        self.list_prefixes.lock().unwrap().clear();
        self.active_ingest_puts
            .store(0, std::sync::atomic::Ordering::SeqCst);
        self.max_concurrent_ingest_puts
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }

    fn get_paths(&self) -> Vec<String> {
        self.get_paths.lock().unwrap().clone()
    }

    fn list_prefixes(&self) -> Vec<String> {
        self.list_prefixes.lock().unwrap().clone()
    }

    fn max_concurrent_ingest_puts(&self) -> usize {
        self.max_concurrent_ingest_puts
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn start_ingest_put(&self) {
        let current = self
            .active_ingest_puts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let mut observed = self
            .max_concurrent_ingest_puts
            .load(std::sync::atomic::Ordering::SeqCst);
        while current > observed {
            match self.max_concurrent_ingest_puts.compare_exchange(
                observed,
                current,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(next) => observed = next,
            }
        }
    }

    fn finish_ingest_put(&self) {
        self.active_ingest_puts
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counts: ObjectStoreAccessCounts,
    ingest_put_delay: Duration,
}

impl CountingObjectStore {
    fn new(inner: Arc<dyn ObjectStore>, counts: ObjectStoreAccessCounts) -> Self {
        Self {
            inner,
            counts,
            ingest_put_delay: Duration::ZERO,
        }
    }

    fn with_ingest_put_delay(
        inner: Arc<dyn ObjectStore>,
        counts: ObjectStoreAccessCounts,
        ingest_put_delay: Duration,
    ) -> Self {
        Self {
            inner,
            counts,
            ingest_put_delay,
        }
    }
}

impl fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let track_ingest_put = location.to_string().starts_with("v1/ingest/");
        if track_ingest_put {
            self.counts.start_ingest_put();
            if self.ingest_put_delay > Duration::ZERO {
                tokio::time::sleep(self.ingest_put_delay).await;
            }
        }
        let result = self.inner.put_opts(location, payload, opts).await;
        if track_ingest_put {
            self.counts.finish_ingest_put();
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.counts
            .get_paths
            .lock()
            .unwrap()
            .push(location.to_string());
        self.inner.get_opts(location, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.counts
            .list_prefixes
            .lock()
            .unwrap()
            .push(prefix.map(Path::to_string).unwrap_or_default());
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

#[test]
fn standing_runtime_checkpoint_publication_refs_output_manifest_when_payload_contains_published_output(
) {
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let publication = standing_runtime_output_manifest_record_for_checkpoint(
        &checkpoint,
        "purchases_by_user",
        &checkpoint_key,
    )
    .unwrap()
    .unwrap();

    let published = standing_runtime_checkpoint_with_publication_output_refs(
        &checkpoint,
        Some(&publication.manifest_key),
        &[],
    );

    assert_eq!(
        published.output_manifest_refs,
        vec![format!(
            "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
            publication.manifest_key.as_str()
        )]
    );
}

#[test]
fn standing_runtime_output_manifest_record_hashes_published_output_payload() {
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);

    let publication = standing_runtime_output_manifest_record_for_checkpoint(
        &checkpoint,
        "purchases_by_user",
        &checkpoint_key,
    )
    .unwrap()
    .unwrap();
    let record = &publication.manifest_record;

    validate_standing_runtime_output_manifest_record(&publication.manifest_key, record).unwrap();
    assert_eq!(
        publication.manifest_key.as_str(),
        ObjectKey::standing_runtime_output_manifest(
            "tenant-a",
            "program-purchases",
            "purchases_by_user",
            7,
            &record.output_content_hash,
        )
        .unwrap()
        .as_str()
    );
    assert_eq!(record.checkpoint_key, checkpoint_key.as_str());
    assert_eq!(record.output_encoding, "velorix-delta-batch-json-v1");
    assert_eq!(
        record.source_kind,
        "standing_runtime_checkpoint_published_output"
    );
    assert_eq!(record.output_row_count, 0);
    assert_eq!(record.pages.len(), 1);
    assert_eq!(record.pages[0].page_index, 0);
    assert_eq!(record.pages[0].row_count, 0);
    assert_eq!(
        record.pages[0].page_content_hash,
        record.output_content_hash
    );
    assert_eq!(
        record.pages[0].page_key,
        ObjectKey::standing_runtime_output_page(
            "tenant-a",
            "program-purchases",
            "purchases_by_user",
            7,
            0,
            &record.output_content_hash,
        )
        .unwrap()
        .as_str()
    );
    assert_eq!(publication.page_records.len(), 1);
    validate_standing_runtime_output_page_record(
        &publication.page_records[0].0,
        &publication.page_records[0].1,
    )
    .unwrap();
    assert_eq!(
        publication.page_records[0].1.published_output,
        record.published_output
    );
}

#[tokio::test]
async fn standing_runtime_checkpoint_persistence_writes_output_delta_manifest_ref() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let output_delta = ViewOutputDelta {
        view_id: "purchases_by_user".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        delta: DeltaBatch::from_records([
            DeltaRecord::new(
                DeltaKey::from_json(json!("alice")),
                DeltaValue::from_json(json!({ "count": 2, "sum": 17 })),
                -1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("alice")),
                DeltaValue::from_json(json!({ "count": 3, "sum": 20 })),
                1,
            ),
        ]),
    };

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        std::slice::from_ref(&output_delta),
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    )
    .await
    .unwrap();

    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let checkpoint_bytes = state
        .store
        .get(&Path::from(checkpoint_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let record: StandingRuntimeCheckpointRecord =
        serde_json::from_slice(&checkpoint_bytes).unwrap();
    assert!(
            record
                .checkpoint
                .output_manifest_refs
                .iter()
                .all(|output_ref| output_ref.starts_with(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX)),
            "ingest checkpoint should publish delta refs only; full output snapshots are compacted separately: {:?}",
            record.checkpoint.output_manifest_refs
        );
    let delta_ref = record
        .checkpoint
        .output_manifest_refs
        .iter()
        .find(|output_ref| output_ref.starts_with(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX))
        .unwrap();
    let delta_key = delta_ref
        .strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX)
        .unwrap();
    let delta_bytes = state
        .store
        .get(&Path::from(delta_key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let delta_record: StandingRuntimeOutputDeltaRecord =
        serde_json::from_slice(&delta_bytes).unwrap();
    let (parsed_delta_key, _) = ObjectKey::parse_standing_runtime_output_delta(delta_key).unwrap();

    validate_standing_runtime_output_delta_record(&parsed_delta_key, &delta_record).unwrap();
    assert_eq!(
        delta_record.output_delta,
        serde_json::to_value(output_delta.delta).unwrap()
    );
    assert_eq!(delta_record.delta_row_count, 2);
    assert_eq!(
        delta_record.source_kind,
        "standing_runtime_epoch_output_delta"
    );

    read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn standing_runtime_checkpoint_read_fails_closed_when_output_delta_object_is_missing() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let output_delta = ViewOutputDelta {
        view_id: "purchases_by_user".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        delta: DeltaBatch::from_records([DeltaRecord::new(
            DeltaKey::from_json(json!("alice")),
            DeltaValue::from_json(json!({ "count": 3, "sum": 20 })),
            1,
        )]),
    };

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        std::slice::from_ref(&output_delta),
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    )
    .await
    .unwrap();

    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let checkpoint_bytes = state
        .store
        .get(&Path::from(checkpoint_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let record: StandingRuntimeCheckpointRecord =
        serde_json::from_slice(&checkpoint_bytes).unwrap();
    let delta_ref = record
        .checkpoint
        .output_manifest_refs
        .iter()
        .find(|output_ref| output_ref.starts_with(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX))
        .unwrap();
    let delta_key = delta_ref
        .strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX)
        .unwrap();
    state.store.delete(&Path::from(delta_key)).await.unwrap();

    let error =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap_err();

    assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn standing_runtime_checkpoint_read_fails_closed_when_output_delta_object_is_corrupt() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let output_delta = ViewOutputDelta {
        view_id: "purchases_by_user".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        delta: DeltaBatch::from_records([DeltaRecord::new(
            DeltaKey::from_json(json!("alice")),
            DeltaValue::from_json(json!({ "count": 3, "sum": 20 })),
            1,
        )]),
    };

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        std::slice::from_ref(&output_delta),
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    )
    .await
    .unwrap();

    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let checkpoint_bytes = state
        .store
        .get(&Path::from(checkpoint_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let record: StandingRuntimeCheckpointRecord =
        serde_json::from_slice(&checkpoint_bytes).unwrap();
    let delta_ref = record
        .checkpoint
        .output_manifest_refs
        .iter()
        .find(|output_ref| output_ref.starts_with(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX))
        .unwrap();
    let delta_key = delta_ref
        .strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX)
        .unwrap();
    let delta_bytes = state
        .store
        .get(&Path::from(delta_key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut delta_record: StandingRuntimeOutputDeltaRecord =
        serde_json::from_slice(&delta_bytes).unwrap();
    delta_record.output_delta = json!({"records": []});
    state
        .store
        .put(
            &Path::from(delta_key),
            bytes::Bytes::from(serde_json::to_vec(&delta_record).unwrap()).into(),
        )
        .await
        .unwrap();

    let error =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(error.message.contains("output delta key/body mismatch"));
}

#[tokio::test]
async fn standing_runtime_output_serving_reads_page_object_not_manifest_payload() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let mut publication = standing_runtime_output_manifest_record_for_checkpoint(
        &checkpoint,
        "purchases_by_user",
        &checkpoint_key,
    )
    .unwrap()
    .unwrap();
    publication.manifest_record.published_output = serde_json::json!({"records": [{"key": "poison", "value": {"sum": 999, "count": 999}, "weight": 1}]});
    let (page_key, page_record) = &publication.page_records[0];
    persist_standing_runtime_output_page(&state, page_key, page_record)
        .await
        .unwrap();

    let output =
        standing_runtime_published_output_from_manifest_page(&state, &publication.manifest_record)
            .await
            .unwrap();

    let expected: DeltaBatch =
        serde_json::from_value(page_record.published_output.clone()).unwrap();
    assert_eq!(output, expected);
}

#[tokio::test]
async fn standing_runtime_output_compaction_rewrites_fragmented_pages_as_single_page() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let record = test_checkpoint_record(&checkpoint_key, checkpoint.clone());
    let page_outputs = [
        DeltaBatch::from_records([DeltaRecord::new(
            DeltaKey::from_json(json!("alice")),
            DeltaValue::from_json(json!({"sum": 10, "count": 1})),
            1,
        )]),
        DeltaBatch::from_records([DeltaRecord::new(
            DeltaKey::from_json(json!("bob")),
            DeltaValue::from_json(json!({"sum": 5, "count": 1})),
            1,
        )]),
    ];
    let full_output = page_outputs[0].combine(&page_outputs[1]);
    let full_value = serde_json::to_value(&full_output).unwrap();
    let full_hash = stable_bytes_hash(&serde_json::to_vec(&full_value).unwrap());
    let mut pages = Vec::new();
    for (index, output) in page_outputs.into_iter().enumerate() {
        let output_value = serde_json::to_value(output).unwrap();
        let page_hash = stable_bytes_hash(&serde_json::to_vec(&output_value).unwrap());
        let page_key = ObjectKey::standing_runtime_output_page(
            "tenant-a",
            "program-purchases",
            "purchases_by_user",
            checkpoint.logical_epoch,
            index as u32,
            &page_hash,
        )
        .unwrap();
        let page_ref = StandingRuntimeOutputPageRef {
            page_index: index as u32,
            page_key: page_key.as_str().to_string(),
            page_content_hash: page_hash.clone(),
            row_count: 1,
            output_encoding: "velorix-delta-batch-json-v1".to_string(),
        };
        let page_record = StandingRuntimeOutputPageRecord {
            schema_version: 1,
            record_kind: "standing_runtime_output_page_v1".to_string(),
            tenant_id: "tenant-a".to_string(),
            program_id: "program-purchases".to_string(),
            view_id: "purchases_by_user".to_string(),
            logical_epoch: checkpoint.logical_epoch,
            output_content_hash: full_hash.clone(),
            page_index: index as u32,
            page_content_hash: page_hash,
            row_count: 1,
            output_encoding: "velorix-delta-batch-json-v1".to_string(),
            source_kind: "standing_runtime_checkpoint_published_output".to_string(),
            published_output: output_value,
        };
        put_standing_runtime_output_page(&state, &page_key, &page_record)
            .await
            .unwrap();
        pages.push(page_ref);
    }
    let manifest = StandingRuntimeOutputManifestRecord {
        schema_version: 1,
        record_kind: "standing_runtime_output_manifest_v1".to_string(),
        tenant_id: "tenant-a".to_string(),
        program_id: "program-purchases".to_string(),
        view_id: "purchases_by_user".to_string(),
        checkpoint_key: checkpoint_key.as_str().to_string(),
        logical_epoch: checkpoint.logical_epoch,
        checkpoint_content_hash: checkpoint.state_root.content_hash.clone(),
        output_content_hash: full_hash,
        output_encoding: "velorix-delta-batch-json-v1".to_string(),
        output_row_count: 2,
        source_kind: "standing_runtime_checkpoint_published_output".to_string(),
        pages,
        published_output: full_value,
    };

    let compacted = compact_standing_runtime_output_manifest(&state, &record, &manifest)
        .await
        .unwrap();

    assert_eq!(compacted.manifest_record.pages.len(), 1);
    assert_eq!(compacted.manifest_record.output_row_count, 2);
    let (_page_key, page_record) = read_standing_runtime_output_page_record(
        &state,
        &compacted.manifest_record.pages[0],
        "purchases_by_user",
    )
    .await
    .unwrap();
    let compacted_output: DeltaBatch =
        serde_json::from_value(page_record.published_output).unwrap();
    assert_eq!(compacted_output.net_rows().unwrap().len(), 2);
}

#[tokio::test]
async fn standing_runtime_output_compaction_publishes_checkpoint_bound_snapshot_without_republishing_checkpoint(
) {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-compact-no-side-effect-owner",
        false,
    )
    .await;
    let router = app(state.clone());

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "scores_compact_no_side_effect".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("score totals for compaction conflict side-effect check".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-compact-no-side-effect-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest_response.0, StatusCode::CREATED);

    let active = state
        .view_registry()
        .unwrap()
        .read_active("scores_compact_no_side_effect")
        .await
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let record =
        read_latest_standing_runtime_checkpoint(&state, identity, "scores_compact_no_side_effect")
            .await
            .unwrap()
            .unwrap();
    assert!(record
        .checkpoint
        .output_manifest_refs
        .iter()
        .all(|output_ref| { output_ref.starts_with(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX) }));
    let checkpoint_key =
        ObjectKey::parse_standing_runtime_checkpoint(record.checkpoint_key.clone())
            .unwrap()
            .0;
    let publication = standing_runtime_output_manifest_record_for_checkpoint(
        &record.checkpoint,
        "scores_compact_no_side_effect",
        &checkpoint_key,
    )
    .unwrap()
    .unwrap();

    let response = compact_view_output_once(&state, "scores_compact_no_side_effect", "sync")
        .await
        .unwrap();

    assert_eq!(response.outcome, "compacted");
    assert_eq!(response.compacted_manifests, 1);
    assert!(state
        .store
        .get(&Path::from(publication.manifest_key.as_str()))
        .await
        .is_ok());
    assert!(state
        .store
        .get(&Path::from(publication.page_records[0].0.as_str()))
        .await
        .is_ok());
    let latest =
        read_latest_standing_runtime_checkpoint(&state, identity, "scores_compact_no_side_effect")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        latest.checkpoint.output_manifest_refs,
        record.checkpoint.output_manifest_refs
    );
}

#[tokio::test]
async fn standing_runtime_output_serving_fails_closed_when_page_object_is_missing() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let publication = standing_runtime_output_manifest_record_for_checkpoint(
        &checkpoint,
        "purchases_by_user",
        &checkpoint_key,
    )
    .unwrap()
    .unwrap();

    standing_runtime_published_output_from_manifest_page(&state, &publication.manifest_record)
        .await
        .unwrap_err();
}

#[tokio::test]
async fn standing_runtime_output_serving_fails_closed_when_page_object_is_corrupt() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let publication = standing_runtime_output_manifest_record_for_checkpoint(
        &checkpoint,
        "purchases_by_user",
        &checkpoint_key,
    )
    .unwrap()
    .unwrap();
    let (page_key, page_record) = &publication.page_records[0];
    let mut corrupt = page_record.clone();
    corrupt.published_output = serde_json::json!({"records": [{"key": "other", "value": {"sum": 1, "count": 1}, "weight": 1}]});
    let corrupt_bytes = serde_json::to_vec(&corrupt).unwrap();
    state
        .store
        .put(
            &Path::from(page_key.as_str()),
            bytes::Bytes::from(corrupt_bytes).into(),
        )
        .await
        .unwrap();

    let error =
        standing_runtime_published_output_from_manifest_page(&state, &publication.manifest_record)
            .await
            .unwrap_err();

    assert!(format!("{error:?}").contains("standing runtime output page key/body mismatch"));
}

#[tokio::test]
async fn standing_runtime_output_serving_fails_closed_when_manifest_object_is_missing() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let publication = standing_runtime_output_manifest_record_for_checkpoint(
        &checkpoint,
        "purchases_by_user",
        &checkpoint_key,
    )
    .unwrap()
    .unwrap();
    let output_ref = format!(
        "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
        publication.manifest_key.as_str()
    );

    let error = read_standing_runtime_output_manifest_record(
        &state,
        output_ref.as_str(),
        "purchases_by_user",
    )
    .await
    .unwrap_err();

    assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn standing_runtime_output_serving_fails_closed_when_manifest_object_is_corrupt() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let publication = standing_runtime_output_manifest_record_for_checkpoint(
        &checkpoint,
        "purchases_by_user",
        &checkpoint_key,
    )
    .unwrap()
    .unwrap();
    state
        .store
        .put(
            &Path::from(publication.manifest_key.as_str()),
            bytes::Bytes::from_static(b"{not-json").into(),
        )
        .await
        .unwrap();
    let output_ref = format!(
        "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
        publication.manifest_key.as_str()
    );

    let error = read_standing_runtime_output_manifest_record(
        &state,
        output_ref.as_str(),
        "purchases_by_user",
    )
    .await
    .unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn standing_runtime_checkpoint_persistence_writes_state_object_and_strips_embedded_payload() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let expected_payload = checkpoint.state_payload.clone();
    let checkpoint_key = test_checkpoint_key(&checkpoint);

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    )
    .await
    .unwrap();

    let checkpoint_bytes = state
        .store
        .get(&Path::from(checkpoint_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let raw_record: StandingRuntimeCheckpointRecord =
        serde_json::from_slice(&checkpoint_bytes).unwrap();
    assert!(raw_record.checkpoint.state_payload.is_none());
    let (state_key, state_key_parts) = ObjectKey::parse_standing_runtime_state_payload(
        raw_record.checkpoint.state_root.object_key.clone(),
    )
    .unwrap();
    assert_eq!(state_key_parts.tenant_id, "tenant-a");
    assert_eq!(state_key_parts.program_id, "program-purchases");
    assert_eq!(state_key_parts.view_id, "purchases_by_user");
    assert_eq!(state_key_parts.logical_epoch, checkpoint.logical_epoch);
    assert_eq!(
        state_key_parts.state_content_hash,
        checkpoint.state_root.content_hash
    );

    let state_bytes = state
        .store
        .get(&Path::from(state_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let state_record: StandingRuntimeStatePayloadRecord =
        serde_json::from_slice(&state_bytes).unwrap();
    validate_standing_runtime_state_payload_record(&state_key, &state_record).unwrap();
    assert_eq!(Some(state_record.payload), expected_payload);
}

#[tokio::test]
async fn standing_runtime_checkpoint_read_hydrates_state_payload_from_state_object() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let expected_payload = checkpoint.state_payload.clone();

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    )
    .await
    .unwrap();

    let record =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap()
            .unwrap();

    assert_eq!(record.checkpoint.state_payload, expected_payload);
}

#[tokio::test]
async fn standing_runtime_checkpoint_read_migrates_legacy_identity_keys() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    )
    .await
    .unwrap();
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let checkpoint_bytes = state
        .store
        .get(&Path::from(checkpoint_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut record: Value = serde_json::from_slice(&checkpoint_bytes).unwrap();
    let identity = record
        .pointer_mut("/checkpoint/identity")
        .and_then(Value::as_object_mut)
        .unwrap();
    let planner = identity.remove("planner_identity").unwrap();
    identity.insert(legacy_key("compiler", "_identity"), planner);
    let runtimes = identity.remove("builtin_runtime_identities").unwrap();
    identity.insert(legacy_key("runtime", "_packages"), runtimes);
    let capabilities = identity.remove("runtime_capabilities").unwrap();
    identity.insert(legacy_key("package", "_feature_set"), capabilities);
    object_store::ObjectStoreExt::put(
        &*state.store,
        &Path::from(checkpoint_key.as_str()),
        serde_json::to_vec(&record).unwrap().into(),
    )
    .await
    .unwrap();

    let restored =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap()
            .unwrap();

    assert_eq!(restored.checkpoint.identity, checkpoint.identity);
}

#[test]
fn standing_runtime_replay_plan_uses_input_frontier_when_replay_checkpoints_are_absent() {
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let record = StandingRuntimeCheckpointRecord {
        schema_version: 1,
        record_kind: "standing_runtime_checkpoint_v1".to_string(),
        view_id: "purchases_by_user".to_string(),
        checkpoint_key: test_checkpoint_key(&checkpoint).as_str().to_string(),
        previous_checkpoint: None,
        checkpoint,
        replay_checkpoints: Vec::new(),
        manifest_hash: String::new(),
    };

    let replay_plan = standing_runtime_replay_plan_from_record_ref(&record);

    assert!(replay_plan_covers_replayed_batch(
        &replay_plan,
        "purchases",
        "2026-05-24.v1",
        "test-stream",
        0,
        11,
    ));
    assert!(!replay_plan_covers_replayed_batch(
        &replay_plan,
        "purchases",
        "2026-05-24.v1",
        "other-stream",
        0,
        11,
    ));
    assert!(!replay_plan_covers_replayed_batch(
        &replay_plan,
        "purchases",
        "2026-05-24.v1",
        "test-stream",
        0,
        12,
    ));
}

#[test]
fn standing_runtime_checkpoint_validation_allows_historical_replay_without_active_frontier() {
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let record = StandingRuntimeCheckpointRecord {
        schema_version: 1,
        record_kind: "standing_runtime_checkpoint_v1".to_string(),
        view_id: "purchases_by_user".to_string(),
        checkpoint_key: test_checkpoint_key(&checkpoint).as_str().to_string(),
        previous_checkpoint: None,
        checkpoint,
        replay_checkpoints: vec![ReplayCheckpoint::for_relation(
            "purchases".to_string(),
            "2026-05-24.v1".to_string(),
            "historical-stream".to_string(),
            0,
            1,
        )],
        manifest_hash: String::new(),
    };

    validate_standing_runtime_checkpoint_replay_frontiers(&record).unwrap();
    let replay_plan = standing_runtime_replay_plan_from_record_ref(&record);
    assert!(replay_plan_covers_replayed_batch(
        &replay_plan,
        "purchases",
        "2026-05-24.v1",
        "historical-stream",
        0,
        1,
    ));
}

#[tokio::test]
async fn standing_runtime_checkpoint_read_fails_closed_when_state_object_is_missing() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    )
    .await
    .unwrap();
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let checkpoint_bytes = state
        .store
        .get(&Path::from(checkpoint_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let raw_record: StandingRuntimeCheckpointRecord =
        serde_json::from_slice(&checkpoint_bytes).unwrap();
    state
        .store
        .delete(&Path::from(raw_record.checkpoint.state_root.object_key))
        .await
        .unwrap();

    let error =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap_err();

    assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn standing_runtime_checkpoint_read_fails_closed_when_checkpoint_object_is_corrupt() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    state
        .store
        .put(
            &Path::from(checkpoint_key.as_str()),
            bytes::Bytes::from_static(b"{not-json").into(),
        )
        .await
        .unwrap();

    let error =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn relation_catalog_read_falls_back_to_object_store_when_meta_is_empty_after_recovery() {
    let state = test_api_state().await;
    let catalog = test_scores_catalog();
    materialize_relation_catalog_to_object_store(&state, &catalog)
        .await
        .unwrap();
    let state = state.with_meta_store(Arc::new(InMemoryMetaStore::default()));

    let restored = read_relation_catalog(&state, "scores", "2026-05-24.v1")
        .await
        .unwrap();

    assert_eq!(restored, catalog);
}

#[tokio::test]
async fn standing_runtime_checkpoint_read_ignores_object_store_when_meta_pointer_is_empty_after_recovery(
) {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    )
    .await
    .unwrap();
    let state = state.with_meta_store(Arc::new(InMemoryMetaStore::default()));

    let restored =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap();

    assert!(
            restored.is_none(),
            "meta-backed recovery must not treat object listing or latest cache as checkpoint authority"
        );
}

#[tokio::test]
async fn standing_runtime_checkpoint_read_uses_meta_pointer_when_latest_cache_is_stale() {
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let state = test_api_state().await.with_meta_store(meta_store);
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let owner = state
        .acquire_standing_runtime_owner(&checkpoint.identity, "purchases_by_user")
        .await
        .unwrap()
        .expect("test meta store should grant owner token");

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner.clone())),
        None,
    )
    .await
    .unwrap();
    let latest_key = ObjectKey::standing_runtime_latest_checkpoint(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        "purchases_by_user",
    )
    .unwrap();
    let stale_latest_bytes = state
        .store
        .get(&Path::from(latest_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    let mut next = test_runtime_checkpoint(Vec::new());
    next.logical_epoch = checkpoint.logical_epoch + 1;
    next.input_frontiers[0].committed_offset_exclusive += 1;
    next.output_frontiers[0].committed_epoch = next.logical_epoch;
    let next_payload = serde_json::json!({
        "schema_version": 1,
        "published_output": {
            "records": []
        },
        "meta_pointer_winner": true
    })
    .to_string();
    next.state_root.content_hash = stable_bytes_hash(next_payload.as_bytes());
    next.state_payload = Some(RuntimeCheckpointStatePayload {
        codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
        payload: next_payload,
    });

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &next,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner)),
        None,
    )
    .await
    .unwrap();
    state
        .store
        .put(
            &Path::from(latest_key.as_str()),
            stale_latest_bytes.clone().into(),
        )
        .await
        .unwrap();

    let restored =
        read_latest_standing_runtime_checkpoint(&state, &next.identity, "purchases_by_user")
            .await
            .unwrap()
            .expect("meta pointer should select the published checkpoint");

    assert_eq!(restored.checkpoint.logical_epoch, next.logical_epoch);
    assert_eq!(
        restored.checkpoint.state_root.content_hash,
        next.state_root.content_hash
    );
}

#[tokio::test]
async fn standing_runtime_checkpoint_read_keeps_old_meta_pointer_when_new_checkpoint_is_orphaned() {
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "api-test-orphan-meta", false)
        .await
        .with_meta_store(meta_store);
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let owner = state
        .acquire_standing_runtime_owner(&checkpoint.identity, "purchases_by_user")
        .await
        .unwrap()
        .expect("test meta store should grant owner token");

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner)),
        None,
    )
    .await
    .unwrap();

    let mut orphaned = test_runtime_checkpoint(Vec::new());
    orphaned.logical_epoch = checkpoint.logical_epoch + 1;
    orphaned.input_frontiers[0].committed_offset_exclusive += 1;
    orphaned.output_frontiers[0].committed_epoch = orphaned.logical_epoch;
    let orphaned_payload = serde_json::json!({
        "schema_version": 1,
        "published_output": {
            "records": []
        },
        "orphaned_checkpoint": true
    })
    .to_string();
    orphaned.state_root.content_hash = stable_bytes_hash(orphaned_payload.as_bytes());
    orphaned.state_payload = Some(RuntimeCheckpointStatePayload {
        codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
        payload: orphaned_payload,
    });

    let orphan_writer =
        test_api_state_with_store(store, "api-test-orphan-object-writer", false).await;
    persist_standing_runtime_checkpoint(
        &orphan_writer,
        "purchases_by_user",
        &orphaned,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    )
    .await
    .unwrap();

    let restored =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap()
            .expect("metadata pointer should remain authoritative");

    assert_eq!(restored.checkpoint.logical_epoch, checkpoint.logical_epoch);
    assert_eq!(
        restored.checkpoint.state_root.content_hash,
        checkpoint.state_root.content_hash
    );
}

#[tokio::test]
async fn standing_runtime_checkpoint_read_fails_closed_when_manifest_hash_mismatches_pointer() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    )
    .await
    .unwrap();
    let record =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap()
            .expect("checkpoint should be readable without meta authority");
    let mut pointer = standing_runtime_checkpoint_pointer_from_record(&record);
    pointer.manifest_hash =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_string();

    let error = read_standing_runtime_checkpoint_record_from_pointer(
        &state,
        &checkpoint.identity,
        "purchases_by_user",
        &pointer,
    )
    .await
    .unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(error.message.contains("manifest hash mismatch"));
}

#[tokio::test]
async fn standing_runtime_checkpoint_read_fails_closed_when_pointer_body_mismatches_checkpoint_object(
) {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    )
    .await
    .unwrap();
    let record =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap()
            .expect("checkpoint should be readable without meta authority");
    let mut pointer = standing_runtime_checkpoint_pointer_from_record(&record);
    pointer.logical_epoch += 1;

    let error = read_standing_runtime_checkpoint_record_from_pointer(
        &state,
        &checkpoint.identity,
        "purchases_by_user",
        &pointer,
    )
    .await
    .unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(error.message.contains("pointer/body mismatch"));
}

#[tokio::test]
async fn standing_runtime_checkpoint_publish_rehydrates_empty_meta_pointer_after_recovery() {
    let state = test_api_state().await;
    let previous = test_runtime_checkpoint(Vec::new());
    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &previous,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    )
    .await
    .unwrap();

    let meta_store = Arc::new(InMemoryMetaStore::default());
    let state = state.with_meta_store(meta_store.clone());
    let owner = match meta_store
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: previous.identity.tenant_id.clone(),
            program_id: previous.identity.program_id.clone(),
            view_id: "purchases_by_user".to_string(),
            owner_id: "api-test-recovery-owner".to_string(),
            ttl_ms: 30_000,
        })
        .await
        .unwrap()
    {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim)
        | AcquireStandingRuntimeOwnerOutcome::Renewed(claim) => {
            standing_runtime_owner_token_from_claim(&claim)
        }
        AcquireStandingRuntimeOwnerOutcome::Conflict(claim) => {
            panic!("unexpected owner conflict: {claim:?}")
        }
    };

    let mut next = test_runtime_checkpoint(Vec::new());
    next.logical_epoch = previous.logical_epoch + 1;
    next.input_frontiers[0].committed_offset_exclusive += 1;
    next.output_frontiers[0].committed_epoch = next.logical_epoch;
    let next_payload = serde_json::json!({
        "schema_version": 1,
        "published_output": {
            "records": []
        },
        "recovered_after_empty_meta": true
    })
    .to_string();
    next.state_root.content_hash = stable_bytes_hash(next_payload.as_bytes());
    next.state_payload = Some(RuntimeCheckpointStatePayload {
        codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
        payload: next_payload,
    });

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &next,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner)),
        None,
    )
    .await
    .unwrap();

    let published = meta_store
        .read_standing_runtime_checkpoint(
            &next.identity.tenant_id,
            &next.identity.program_id,
            "purchases_by_user",
        )
        .await
        .unwrap()
        .expect("meta pointer should be rehydrated and advanced");
    assert_eq!(published.logical_epoch, next.logical_epoch);
    assert_eq!(published.content_hash, next.state_root.content_hash);

    let restored =
        read_latest_standing_runtime_checkpoint(&state, &next.identity, "purchases_by_user")
            .await
            .unwrap()
            .expect("published checkpoint should be readable");
    assert_eq!(restored.checkpoint.logical_epoch, next.logical_epoch);
}

#[tokio::test]
async fn standing_runtime_checkpoint_read_fails_closed_when_meta_pointer_checkpoint_object_is_missing(
) {
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let state = test_api_state().await.with_meta_store(meta_store.clone());
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let owner = match meta_store
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: checkpoint.identity.tenant_id.clone(),
            program_id: checkpoint.identity.program_id.clone(),
            view_id: "purchases_by_user".to_string(),
            owner_id: "api-test-missing-checkpoint-owner".to_string(),
            ttl_ms: 30_000,
        })
        .await
        .unwrap()
    {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim)
        | AcquireStandingRuntimeOwnerOutcome::Renewed(claim) => {
            standing_runtime_owner_token_from_claim(&claim)
        }
        AcquireStandingRuntimeOwnerOutcome::Conflict(claim) => {
            panic!("unexpected owner conflict: {claim:?}")
        }
    };

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner)),
        None,
    )
    .await
    .unwrap();
    state
        .store
        .delete(&Path::from(checkpoint_key.as_str()))
        .await
        .unwrap();

    let error =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap_err();

    assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn standing_runtime_checkpoint_read_fails_closed_when_state_object_is_corrupt() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), None),
        None,
    )
    .await
    .unwrap();
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let checkpoint_bytes = state
        .store
        .get(&Path::from(checkpoint_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let raw_record: StandingRuntimeCheckpointRecord =
        serde_json::from_slice(&checkpoint_bytes).unwrap();
    let state_key = ObjectKey::parse_standing_runtime_state_payload(
        raw_record.checkpoint.state_root.object_key.clone(),
    )
    .unwrap()
    .0;
    let mut corrupt_record =
        standing_runtime_state_payload_record_for_checkpoint(&checkpoint, "purchases_by_user")
            .unwrap()
            .1;
    corrupt_record.payload.payload = serde_json::json!({
        "schema_version": 1,
        "published_output": {
            "records": [{"key": "poison", "value": {"sum": 1, "count": 1}, "weight": 1}]
        }
    })
    .to_string();
    state
        .store
        .put(
            &Path::from(state_key.as_str()),
            bytes::Bytes::from(serde_json::to_vec(&corrupt_record).unwrap()).into(),
        )
        .await
        .unwrap();

    let error =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap_err();

    assert!(format!("{error:?}").contains("standing runtime state payload key/body mismatch"));
}

#[test]
fn standing_runtime_checkpoint_output_ref_validation_rejects_untagged_ref() {
    let mut checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    checkpoint
        .output_manifest_refs
        .push(checkpoint_key.as_str().to_string());
    let pointer = test_checkpoint_pointer(&checkpoint_key, &checkpoint);
    let record = test_checkpoint_record(&checkpoint_key, checkpoint);

    let error = validate_standing_runtime_checkpoint_output_refs(&record, &pointer).unwrap_err();

    assert!(format!("{error:?}").contains("unsupported standing runtime checkpoint output ref"));
}

#[test]
fn standing_runtime_checkpoint_output_ref_validation_rejects_mismatched_output_manifest_key() {
    let mut checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let output_content_hash = format!("sha256:{}", "f".repeat(64));
    let mismatched_key = ObjectKey::standing_runtime_output_manifest(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        "purchases_by_user",
        checkpoint.logical_epoch + 1,
        &output_content_hash,
    )
    .unwrap();
    checkpoint.output_manifest_refs.push(format!(
        "{STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX}{}",
        mismatched_key.as_str()
    ));
    let pointer = test_checkpoint_pointer(&checkpoint_key, &checkpoint);
    let record = test_checkpoint_record(&checkpoint_key, checkpoint);

    let error = validate_standing_runtime_checkpoint_output_refs(&record, &pointer).unwrap_err();

    assert!(
        format!("{error:?}").contains("standing runtime checkpoint output manifest ref mismatch")
    );
}

#[test]
fn single_key_output_schema_fingerprint_changes_with_aggregate_projection() {
    let catalog = test_purchases_catalog();
    let sum_count_plan = validate_catalog_backed_sum_count_view_sql(
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id",
        &catalog,
    )
    .unwrap();
    let avg_plan = validate_catalog_backed_sum_count_view_sql(
            "select user_id as user, sum(amount) as total, count(*) as events, avg(amount) as average from purchases group by user_id",
            &catalog,
        )
        .unwrap();

    let sum_count_schema =
        single_key_sum_count_output_schema("purchase_metrics", &catalog, &sum_count_plan).unwrap();
    let avg_schema =
        single_key_sum_count_output_schema("purchase_metrics", &catalog, &avg_plan).unwrap();

    assert_ne!(
        sum_count_schema.schema_fingerprint,
        avg_schema.schema_fingerprint
    );
    assert_ne!(
        avg_schema.schema_fingerprint,
        catalog.schema_fingerprint.to_string()
    );
    assert_eq!(
        avg_schema
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["user", "total", "events", "average"]
    );
}

#[test]
fn materialized_runtime_binding_persists_admitted_logical_plan() {
    let catalog = test_purchases_catalog();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let plan = validate_catalog_backed_sum_count_view_sql(sql, &catalog).unwrap();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema =
        single_key_sum_count_output_schema("purchases_by_user", &catalog, &plan).unwrap();
    let spec = StandingViewSpec {
        view_id: "purchases_by_user".to_string(),
        sql: sql.to_string(),
        dialect: SqlDialect::VelorixSql,
        source_kind: SqlSourceKind::StandingView,
        input_relations: vec![input_schema],
        output_relations: vec![output_schema],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };

    let binding =
        materialized_view_runtime_binding_for_spec(std::slice::from_ref(&catalog), &spec).unwrap();
    let logical_plan = binding.logical_plan.unwrap();

    assert_eq!(logical_plan.view_sql, spec.sql);
    assert_eq!(
        logical_plan.output_relation.relation_id,
        "purchases_by_user"
    );
    assert_eq!(
        logical_plan.input_relations[0].schema_fingerprint,
        catalog.schema_fingerprint.to_string()
    );
}

#[test]
fn latest_by_key_output_schema_uses_arg_max_value_type() {
    let mut catalog = test_device_status_catalog();
    catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "enabled")
        .unwrap()
        .nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable latest-by-key input schema should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    let plan = validate_supported_latest_by_key_sql(
            "select device_id as device, arg_max(enabled, event_time) as enabled from device_status group by device_id",
            &catalog,
        )
        .unwrap();

    let schema = latest_by_key_output_schema("latest_device_status", &catalog, &plan).unwrap();

    assert_eq!(schema.relation_id, "latest_device_status");
    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("device", &SqlDataType::Utf8),
            ("enabled", &SqlDataType::Bool)
        ]
    );
    assert!(schema.columns[1].nullable);
    assert_eq!(schema.primary_key, vec!["device"]);
}

#[test]
fn materialized_runtime_output_schema_supports_analytic_row_number() {
    let factory = MaterializedViewRuntimeFactory;
    let catalog = test_scores_catalog();

    let schemas = factory
            .output_schemas_for_view_request(
                "ranked_scores",
                "select user_id, row_number() over (partition by user_id order by score desc, user_id asc) as score_rank from scores",
                &catalog,
                catalog.schema_fingerprint.as_str(),
            )
            .unwrap()
            .expect("ROW_NUMBER analytic view should be admitted");

    let schema = &schemas[0];
    assert_eq!(schema.relation_id, "ranked_scores");
    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("user_id", &SqlDataType::Utf8),
            ("score_rank", &SqlDataType::Int64)
        ]
    );
    assert_eq!(schema.primary_key, vec!["user_id"]);
}

#[test]
fn materialized_runtime_output_schema_supports_tumbling_event_time_window() {
    let factory = MaterializedViewRuntimeFactory;
    let catalog = test_purchases_event_time_catalog();

    let schemas = factory
            .output_schemas_for_view_request(
                "purchases_by_user_minute",
                "select user_id as user, window_start as start_time, window_end as end_time, sum(amount) as total_amount, count(*) as event_count, min(amount) as minimum_amount, max(amount) as maximum_amount, avg(amount) as average_amount from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end",
                &catalog,
                catalog.schema_fingerprint.as_str(),
            )
            .unwrap()
            .unwrap();

    let schema = &schemas[0];
    assert_eq!(schema.relation_id, "purchases_by_user_minute");
    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("user", &SqlDataType::Utf8),
            ("start_time", &SqlDataType::Int64),
            ("end_time", &SqlDataType::Int64),
            ("total_amount", &SqlDataType::Int64),
            ("event_count", &SqlDataType::Int64),
            ("minimum_amount", &SqlDataType::Int64),
            ("maximum_amount", &SqlDataType::Int64),
            ("average_amount", &SqlDataType::Float64),
        ]
    );
    assert_eq!(schema.primary_key, vec!["user", "start_time", "end_time"]);
}

#[test]
fn materialized_runtime_output_schema_supports_join_min_max_avg() {
    let catalogs = vec![test_scores_catalog(), test_accounts_catalog()];
    let plan = validate_supported_join_view_sql(
            "select a.account_id, sum(s.score) as total_score, count(*) as event_count, min(s.score) as min_score, max(s.score) as max_score, avg(s.score) as avg_score from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
            &catalogs,
        )
        .unwrap();

    let schema =
        join_sum_count_output_schema("score_extremes_by_account", &catalogs, &plan).unwrap();

    assert_eq!(schema.relation_id, "score_extremes_by_account");
    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("account_id", &SqlDataType::Utf8),
            ("total_score", &SqlDataType::Int64),
            ("event_count", &SqlDataType::Int64),
            ("min_score", &SqlDataType::Int64),
            ("max_score", &SqlDataType::Int64),
            ("avg_score", &SqlDataType::Float64),
        ]
    );
    assert_eq!(schema.primary_key, vec!["account_id"]);
}

#[test]
fn materialized_runtime_output_schema_supports_join_right_min_max_avg() {
    let catalogs = vec![test_scores_catalog(), test_accounts_catalog()];
    let plan = validate_supported_join_view_sql(
            "select a.account_id, sum(s.score) as total_score, count(*) as event_count, count(a.limit) as limit_count, count(distinct a.limit) as distinct_limits, min(a.limit) as min_limit, max(a.limit) as max_limit, avg(a.limit) as avg_limit from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
            &catalogs,
        )
        .unwrap();

    let schema = join_sum_count_output_schema("score_limits_by_account", &catalogs, &plan).unwrap();

    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("account_id", &SqlDataType::Utf8),
            ("total_score", &SqlDataType::Int64),
            ("event_count", &SqlDataType::Int64),
            ("limit_count", &SqlDataType::Int64),
            ("distinct_limits", &SqlDataType::Int64),
            ("min_limit", &SqlDataType::Int64),
            ("max_limit", &SqlDataType::Int64),
            ("avg_limit", &SqlDataType::Float64),
        ]
    );
}

#[test]
fn materialized_runtime_output_schema_admits_supported_rest_join_smoke_shape() {
    let mut readings = test_scores_catalog();
    readings.relation_schema.relation_id = "rest_join_readings_test".to_string();
    readings.relation_schema.relation_name = "rest_join_readings_test".to_string();
    readings.relation_schema.columns[0].column_id = "device_id".to_string();
    readings.relation_schema.columns[0].name = "device_id".to_string();
    readings.relation_schema.columns[1].column_id = "temperature_c".to_string();
    readings.relation_schema.columns[1].name = "temperature_c".to_string();
    readings.relation_schema.primary_key_column_ids = vec!["device_id".to_string()];
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&readings.relation_schema)
        .expect("readings schema should fingerprint");
    readings.schema_fingerprint = schema_fingerprint.clone();
    readings.incremental_relation.relation_id = "rest_join_readings_test".to_string();
    readings.incremental_relation.schema_fingerprint = schema_fingerprint;
    readings.datafusion_registration.name = "rest_join_readings_test".to_string();

    let mut devices = test_accounts_catalog();
    devices.relation_schema.relation_id = "rest_join_devices_test".to_string();
    devices.relation_schema.relation_name = "rest_join_devices_test".to_string();
    devices.relation_schema.columns[0].column_id = "device_id".to_string();
    devices.relation_schema.columns[0].name = "device_id".to_string();
    devices.relation_schema.columns[1].column_id = "calibration_offset".to_string();
    devices.relation_schema.columns[1].name = "calibration_offset".to_string();
    devices.relation_schema.columns[2].column_id = "site".to_string();
    devices.relation_schema.columns[2].name = "site".to_string();
    devices.relation_schema.primary_key_column_ids = vec!["device_id".to_string()];
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&devices.relation_schema)
        .expect("devices schema should fingerprint");
    devices.schema_fingerprint = schema_fingerprint.clone();
    devices.incremental_relation.relation_id = "rest_join_devices_test".to_string();
    devices.incremental_relation.schema_fingerprint = schema_fingerprint;
    devices.datafusion_registration.name = "rest_join_devices_test".to_string();

    let catalogs = vec![readings, devices];
    let schemas = MaterializedViewRuntimeFactory
            .output_schemas_for_view_request_with_catalogs(
                "rest_join_readings_by_device_test",
                "select d.device_id, sum(r.temperature_c) as total_temperature_c, count(*) as reading_count, min(r.temperature_c) as min_temperature_c, max(r.temperature_c) as max_temperature_c from rest_join_readings_test r join rest_join_devices_test d on r.device_id = d.device_id group by d.device_id",
                &catalogs,
                catalogs[0].schema_fingerprint.as_str(),
            )
            .unwrap()
            .expect("REST join smoke SQL should be admitted");
    let schema = &schemas[0];

    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("device_id", &SqlDataType::Utf8),
            ("total_temperature_c", &SqlDataType::Int64),
            ("reading_count", &SqlDataType::Int64),
            ("min_temperature_c", &SqlDataType::Int64),
            ("max_temperature_c", &SqlDataType::Int64),
        ]
    );
}

#[test]
fn materialized_runtime_output_schema_supports_filter_project_union_distinct() {
    let factory = MaterializedViewRuntimeFactory;
    let catalog = test_scores_catalog();

    let schemas = factory
            .output_schemas_for_view_request(
                "positive_scores",
                "select user_id, score from scores where score > 0 union distinct select user_id, score from scores where score >= 10",
                &catalog,
                catalog.schema_fingerprint.as_str(),
            )
            .unwrap()
            .unwrap();

    let schema = &schemas[0];
    assert_eq!(schema.relation_id, "positive_scores");
    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("user_id", &SqlDataType::Utf8),
            ("score", &SqlDataType::Int64)
        ]
    );
    assert_eq!(schema.primary_key, vec!["user_id"]);
}

#[test]
fn materialized_runtime_output_schema_supports_join_left_group_key_projection() {
    let catalogs = vec![test_scores_catalog(), test_accounts_catalog()];
    let plan = validate_supported_join_view_sql(
            "select s.user_id as user, sum(s.score) as total_score, count(*) as event_count from scores s join accounts a on s.user_id = a.account_id group by s.user_id",
            &catalogs,
        )
        .unwrap();

    let schema = join_sum_count_output_schema("scores_by_user", &catalogs, &plan).unwrap();

    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("user", &SqlDataType::Utf8),
            ("total_score", &SqlDataType::Int64),
            ("event_count", &SqlDataType::Int64),
        ]
    );
    assert_eq!(schema.primary_key, vec!["user"]);
}

#[tokio::test]
async fn rest_latest_by_key_cte_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-latest-cte", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_device_status_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "latest_device_status_cte".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "device_status".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "with status_source as (select * from device_status where event_time > 95) select device_id as device, arg_max(enabled, event_time) as enabled from status_source where enabled = true group by device_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("latest device status through CTE source".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "device_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "device-status-cte-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "event_time_watermark": {
                    "event_time_column_id": "event_time",
                    "max_observed_event_time_ns": 110,
                    "watermark_ns": 100
                },
                "rows": [
                    {"device_id": "device-a", "enabled": true, "event_time": 100, "delta": 1},
                    {"device_id": "device-a", "enabled": false, "event_time": 110, "delta": 1},
                    {"device_id": "device-b", "enabled": true, "event_time": 90, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );
    assert_eq!(ingest_response.1["materialization"]["status"], "completed");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/latest_device_status_cte/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"device": "device-a", "enabled": true}])
    );
}

#[tokio::test]
async fn rest_latest_by_key_nullable_value_survives_query_and_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "api-test-latest-nullable-a", false).await;
    let router = app(state);

    let mut catalog = test_device_status_catalog();
    catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "enabled")
        .expect("device status fixture has enabled")
        .nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable latest-by-key input schema should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": catalog,
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(
        relation_response.0,
        StatusCode::CREATED,
        "{relation_response:?}"
    );

    let view_response = call_json(
            &router,
            Method::POST,
            "/v1/views",
            json!({
                "view_id": "latest_nullable_device_status",
                "sql": "select device_id as device, arg_max(enabled, event_time) as enabled from device_status group by device_id",
                "input_relation_id": "device_status",
                "input_relation_version": "2026-05-24.v1",
                "source_kind": "standing_view"
            }),
        )
        .await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "device_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "nullable-device-status-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"device_id": "device-a", "enabled": true, "event_time": 100, "delta": 1},
                    {"device_id": "device-a", "enabled": null, "event_time": 110, "delta": 1},
                    {"device_id": "device-b", "enabled": false, "event_time": 120, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let expected_rows = json!([
        {"device": "device-a", "enabled": null},
        {"device": "device-b", "enabled": false}
    ]);
    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/latest_nullable_device_status/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"], expected_rows,
        "{query_response:?}"
    );

    let restarted_state =
        test_api_state_with_store(store, "api-test-latest-nullable-b", true).await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let restarted_router = app(restarted_state);
    let restarted_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/latest_nullable_device_status/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(
        restarted_query.1["rows"], expected_rows,
        "{restarted_query:?}"
    );

    let append_newer_value = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "device_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "nullable-device-status-stream",
                "partition_id": 0,
                "start_offset_inclusive": 3,
                "rows": [
                    {"device_id": "device-a", "enabled": false, "event_time": 120, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(
        append_newer_value.0,
        StatusCode::CREATED,
        "{append_newer_value:?}"
    );
    let newest_value_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/latest_nullable_device_status/query",
        json!({}),
    )
    .await;
    assert_eq!(
        newest_value_query.0,
        StatusCode::OK,
        "{newest_value_query:?}"
    );
    assert_eq!(
        newest_value_query.1["rows"],
        json!([
            {"device": "device-a", "enabled": false},
            {"device": "device-b", "enabled": false}
        ]),
        "{newest_value_query:?}"
    );

    let retract_newer_value = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "device_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "nullable-device-status-stream",
                "partition_id": 0,
                "start_offset_inclusive": 4,
                "rows": [
                    {"device_id": "device-a", "enabled": false, "event_time": 120, "delta": -1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(
        retract_newer_value.0,
        StatusCode::CREATED,
        "{retract_newer_value:?}"
    );
    let restored_latest_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/latest_nullable_device_status/query",
        json!({}),
    )
    .await;
    assert_eq!(
        restored_latest_query.0,
        StatusCode::OK,
        "{restored_latest_query:?}"
    );
    assert_eq!(
        restored_latest_query.1["rows"], expected_rows,
        "{restored_latest_query:?}"
    );
}

#[tokio::test]
async fn rest_latest_bool_view_materialized_output_replays_later_ingest_after_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "api-test-owner-a", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_device_status_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);
    assert_eq!(relation_response.1["relation_id"], "device_status");

    let view_request = CreateViewRequest {
            view_id: "latest_device_status".to_string(),
            url_path: Some("/devices/latest-status".to_string()),
            output_relation_id: None,
            input_relation_id: "device_status".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select d.device_id as device, arg_max(d.enabled, d.event_time) as enabled from device_status as d where d.enabled = true group by d.device_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("latest bool status by device".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(
        view_response.0,
        StatusCode::CREATED,
        "view creation response: {}",
        view_response.1
    );
    assert_eq!(view_response.1["view_id"], "latest_device_status");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "device_status",
            "relation_version": "2026-05-24.v1",
            "stream_id": "device-status-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "event_time_watermark": {
                "event_time_column_id": "event_time",
                "max_observed_event_time_ns": 110,
                "watermark_ns": 100
            },
            "rows": [
                {"device_id": "device-a", "enabled": true, "event_time": 100, "delta": 1},
                {"device_id": "device-a", "enabled": false, "event_time": 110, "delta": 1},
                {"device_id": "device-b", "enabled": true, "event_time": 90, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest_response.0, StatusCode::CREATED);
    assert_eq!(ingest_response.1["outcome"], "appended");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/latest_device_status/query",
        json!({}),
    )
    .await;
    assert_eq!(
        query_response.0,
        StatusCode::OK,
        "join query response: {}",
        query_response.1
    );
    assert_latest_device_rows(&query_response.1, 3, true, true);

    let get_sql_response = call_json(
            &router,
            Method::GET,
            "/v1/views/latest_device_status/query?sql=select%20device%2C%20enabled%20from%20latest_device_status%20where%20enabled%20%3D%20true%20order%20by%20device",
            Value::Null,
        )
        .await;
    assert_eq!(
        get_sql_response.0,
        StatusCode::OK,
        "GET SQL query response: {}",
        get_sql_response.1
    );
    assert_eq!(
        get_sql_response.1["rows"],
        json!([
            {"device": "device-a", "enabled": true},
            {"device": "device-b", "enabled": true}
        ])
    );

    append_admitted_ingest_without_runtime_apply(
        store.clone(),
        IngestRowsRequest {
            relation_id: "device_status".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            stream_id: "device-status-stream".to_string(),
            partition_id: 0,
            start_offset_inclusive: 3,
            event_time_watermark: Some(IngestEventTimeWatermarkRequest {
                event_time_column_id: "event_time".to_string(),
                max_observed_event_time_ns: 120,
                watermark_ns: 115,
            }),
            rows: vec![json!({
                "device_id": "device-b",
                "enabled": false,
                "event_time": 120,
                "delta": 1
            })],
        },
    )
    .await;

    let restarted_state = test_api_state_with_store(store.clone(), "api-test-owner-b", true).await;
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restarted_app = app(restarted_state);
    let restarted_query_response = call_json(
        &restarted_app,
        Method::POST,
        "/v1/views/latest_device_status/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query_response.0, StatusCode::OK);
    assert_latest_device_rows(&restarted_query_response.1, 4, true, true);
}

#[tokio::test]
async fn rest_latest_by_key_order_by_limit_top_k_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-latest-top-k", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_device_status_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "latest_device_status_top".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "device_status".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select device_id as device, arg_max(enabled, event_time) as enabled from device_status group by device_id order by device desc limit 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("top latest device status".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let openapi_response = call_json(&router, Method::GET, "/v1/openapi.json", Value::Null).await;
    assert_eq!(openapi_response.0, StatusCode::OK, "{openapi_response:?}");
    assert!(
        openapi_response.1["paths"]["/v1/relations/{relation_id}/ingest"].is_object(),
        "relation ingest path missing from OpenAPI: {}",
        openapi_response.1
    );
    assert_eq!(openapi_response.1["paths"]["/v1/ingest"], Value::Null);
    assert_eq!(openapi_response.1["paths"]["/v1/ingest/epoch"], Value::Null);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "device_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "device-status-top-k-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"device_id": "device-a", "enabled": true, "event_time": 100, "delta": 1},
                    {"device_id": "device-b", "enabled": false, "event_time": 110, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/latest_device_status_top/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"device": "device-b", "enabled": false}])
    );
}

#[tokio::test]
async fn rest_latest_by_key_order_by_limit_offset_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-latest-limit-offset",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_device_status_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "latest_device_status_offset".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "device_status".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select device_id as device, arg_max(enabled, event_time) as enabled from device_status group by device_id order by device desc limit 1 offset 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("second latest device status by device order".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "device_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "device-status-offset-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"device_id": "device-a", "enabled": true, "event_time": 100, "delta": 1},
                    {"device_id": "device-b", "enabled": false, "event_time": 110, "delta": 1},
                    {"device_id": "device-c", "enabled": true, "event_time": 120, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/latest_device_status_offset/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"device": "device-b", "enabled": false}])
    );
}

#[tokio::test]
async fn rest_ingest_event_time_watermark_requires_declared_event_time_column() {
    let state = test_api_state().await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "event_time_watermark": {
                "event_time_column_id": "event_time",
                "max_observed_event_time_ns": 100,
                "watermark_ns": 90
            },
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1}
            ]
        }),
    )
    .await;

    assert_eq!(ingest_response.0, StatusCode::BAD_REQUEST);
    assert!(
        ingest_response.1["error"]
            .as_str()
            .unwrap()
            .contains("event_time_watermark requires relation_schema.event_time_column_id"),
        "unexpected response: {}",
        ingest_response.1
    );
}

#[tokio::test]
async fn rest_ingest_event_time_watermark_rejects_max_below_batch_event_time() {
    let state = test_api_state().await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_device_status_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "device_status",
            "relation_version": "2026-05-24.v1",
            "stream_id": "device-status-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "event_time_watermark": {
                "event_time_column_id": "event_time",
                "max_observed_event_time_ns": 109,
                "watermark_ns": 100
            },
            "rows": [
                {"device_id": "device-a", "enabled": true, "event_time": 100, "delta": 1},
                {"device_id": "device-b", "enabled": false, "event_time": 110, "delta": 1}
            ]
        }),
    )
    .await;

    assert_eq!(ingest_response.0, StatusCode::BAD_REQUEST);
    assert!(
        ingest_response.1["error"]
            .as_str()
            .unwrap()
            .contains("max_observed_event_time_ns must be >= actual max event-time value 110"),
        "unexpected response: {}",
        ingest_response.1
    );
}

#[tokio::test]
async fn rest_tumbling_window_view_materialized_output_replays_later_ingest_after_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "api-test-window-owner-a", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(
        relation_response.0,
        StatusCode::CREATED,
        "relation creation response: {}",
        relation_response.1
    );
    assert_eq!(relation_response.1["relation_id"], "purchases");

    let sql = "select p.user_id as user, window_start as start_time, window_end as end_time, sum(p.amount) as total_amount, count(1) as event_count from tumble(purchases, event_time, interval '60 seconds') as p where p.amount > 0 group by p.user_id, window_start, window_end having sum(p.amount) > 6 and count(1) > 0";
    let view_request = CreateViewRequest {
        view_id: "purchases_by_user_minute".to_string(),
        url_path: Some("/purchases/by-user-minute".to_string()),
        output_relation_id: None,
        input_relation_id: "purchases".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: sql.to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("purchase totals by user and event-time minute".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(
        view_response.0,
        StatusCode::CREATED,
        "window view creation response: {}",
        view_response.1
    );
    assert_eq!(view_response.1["view_id"], "purchases_by_user_minute");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "purchases",
            "relation_version": "2026-05-24.v1",
            "stream_id": "purchases-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "event_time_watermark": {
                "event_time_column_id": "event_time",
                "max_observed_event_time_ns": 70_000_000_000i64,
                "watermark_ns": 60_000_000_000i64
            },
            "rows": [
                {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                {"user_id": "bob", "amount": 5, "event_time": 30_000_000_000i64, "delta": 1},
                {"user_id": "bob", "amount": -50, "event_time": 35_000_000_000i64, "delta": 1},
                {"user_id": "alice", "amount": 7, "event_time": 70_000_000_000i64, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest_response.0, StatusCode::CREATED);
    assert_eq!(ingest_response.1["outcome"], "appended");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/purchases_by_user_minute/query",
        json!({}),
    )
    .await;
    assert_eq!(
        query_response.0,
        StatusCode::OK,
        "window query response: {}",
        query_response.1
    );
    assert_window_rows(
        &query_response.1,
        4,
        json!([
            {"user": "alice", "start_time": 0, "end_time": 60_000_000_000i64, "total_amount": 10, "event_count": 1}
        ]),
    );

    append_admitted_ingest_without_runtime_apply(
        store.clone(),
        IngestRowsRequest {
            relation_id: "purchases".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            stream_id: "purchases-stream".to_string(),
            partition_id: 0,
            start_offset_inclusive: 4,
            event_time_watermark: Some(IngestEventTimeWatermarkRequest {
                event_time_column_id: "event_time".to_string(),
                max_observed_event_time_ns: 120_000_000_000,
                watermark_ns: 120_000_000_000,
            }),
            rows: vec![json!({
                "user_id": "bob",
                "amount": 11,
                "event_time": 80_000_000_000i64,
                "delta": 1
            })],
        },
    )
    .await;

    let restarted_state =
        test_api_state_with_store(store.clone(), "api-test-window-owner-b", true).await;
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restarted_app = app(restarted_state);
    let restarted_query_response = call_json(
        &restarted_app,
        Method::POST,
        "/v1/views/purchases_by_user_minute/query",
        json!({}),
    )
    .await;
    assert_eq!(
        restarted_query_response.0,
        StatusCode::OK,
        "restarted window query response: {}",
        restarted_query_response.1
    );
    assert_window_rows(
        &restarted_query_response.1,
        5,
        json!([
            {"user": "alice", "start_time": 0, "end_time": 60_000_000_000i64, "total_amount": 10, "event_count": 1},
            {"user": "alice", "start_time": 60_000_000_000i64, "end_time": 120_000_000_000i64, "total_amount": 7, "event_count": 1},
            {"user": "bob", "start_time": 60_000_000_000i64, "end_time": 120_000_000_000i64, "total_amount": 11, "event_count": 1}
        ]),
    );
}

#[tokio::test]
async fn rest_tumbling_window_cte_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-window-cte", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "purchases_by_user_minute_cte".to_string(),
            url_path: Some("/purchases/by-user-minute-cte".to_string()),
            output_relation_id: None,
            input_relation_id: "purchases".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "with purchase_source as (select * from purchases where amount > 5) select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchase_source, event_time, interval '60 seconds') where user_id <> 'bob' group by user_id, window_start, window_end".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("purchase window totals through CTE source".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "purchases",
                "relation_version": "2026-05-24.v1",
                "stream_id": "purchases-window-cte-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "event_time_watermark": {
                    "event_time_column_id": "event_time",
                    "max_observed_event_time_ns": 60_000_000_000i64,
                    "watermark_ns": 60_000_000_000i64
                },
                "rows": [
                    {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                    {"user_id": "bob", "amount": 8, "event_time": 20_000_000_000i64, "delta": 1},
                    {"user_id": "alice", "amount": 4, "event_time": 30_000_000_000i64, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );
    assert_eq!(ingest_response.1["materialization"]["status"], "completed");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/purchases_by_user_minute_cte/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_window_rows(
        &query_response.1,
        3,
        json!([
            {"user_id": "alice", "window_start": 0, "window_end": 60_000_000_000i64, "total_amount": 10, "event_count": 1}
        ]),
    );
}

#[tokio::test]
async fn rest_tumbling_window_order_by_limit_top_k_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-window-top-k-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "top_purchase_window".to_string(),
            url_path: Some("/purchases/top-window".to_string()),
            output_relation_id: None,
            input_relation_id: "purchases".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id as user, window_start as start_time, window_end as end_time, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by total_amount desc limit 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("top purchase window by materialized amount".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "purchases",
                "relation_version": "2026-05-24.v1",
                "stream_id": "top-window-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "event_time_watermark": {
                    "event_time_column_id": "event_time",
                    "max_observed_event_time_ns": 70_000_000_000i64,
                    "watermark_ns": 60_000_000_000i64
                },
                "rows": [
                    {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                    {"user_id": "bob", "amount": 12, "event_time": 30_000_000_000i64, "delta": 1},
                    {"user_id": "alice", "amount": 50, "event_time": 70_000_000_000i64, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let query_response = call_json(
        &router,
        Method::GET,
        "/v1/api/purchases/top-window",
        Value::Null,
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_window_rows(
        &query_response.1,
        3,
        json!([
            {"user": "bob", "start_time": 0, "end_time": 60_000_000_000i64, "total_amount": 12, "event_count": 1}
        ]),
    );
}

#[tokio::test]
async fn rest_tumbling_window_order_by_sum_function_top_k_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-window-function-top-k-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "top_purchase_window_by_sum_function".to_string(),
            url_path: Some("/purchases/top-window-by-sum".to_string()),
            output_relation_id: None,
            input_relation_id: "purchases".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id as user, window_start as start_time, window_end as end_time, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by sum(amount) desc limit 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("top purchase window by materialized amount".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "purchases",
                "relation_version": "2026-05-24.v1",
                "stream_id": "top-window-function-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "event_time_watermark": {
                    "event_time_column_id": "event_time",
                    "max_observed_event_time_ns": 70_000_000_000i64,
                    "watermark_ns": 60_000_000_000i64
                },
                "rows": [
                    {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                    {"user_id": "bob", "amount": 12, "event_time": 30_000_000_000i64, "delta": 1},
                    {"user_id": "alice", "amount": 50, "event_time": 70_000_000_000i64, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let query_response = call_json(
        &router,
        Method::GET,
        "/v1/api/purchases/top-window-by-sum",
        Value::Null,
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_window_rows(
        &query_response.1,
        3,
        json!([
            {"user": "bob", "start_time": 0, "end_time": 60_000_000_000i64, "total_amount": 12, "event_count": 1}
        ]),
    );
}

#[tokio::test]
async fn rest_tumbling_window_count_distinct_view_materializes_outputs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(
        store.clone(),
        "api-test-window-count-distinct-owner-a",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "purchases_distinct_amounts_by_user_minute".to_string(),
            url_path: Some("/purchases/distinct-amounts-by-user-minute".to_string()),
            output_relation_id: None,
            input_relation_id: "purchases".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, window_start, window_end, sum(amount) as total_amount, count(distinct amount) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("distinct purchase amounts by user and event-time minute".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "purchases",
                "relation_version": "2026-05-24.v1",
                "stream_id": "purchases-distinct-window-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "event_time_watermark": {
                    "event_time_column_id": "event_time",
                    "max_observed_event_time_ns": 70_000_000_000i64,
                    "watermark_ns": 60_000_000_000i64
                },
                "rows": [
                    {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                    {"user_id": "alice", "amount": 10, "event_time": 20_000_000_000i64, "delta": 1},
                    {"user_id": "alice", "amount": 7, "event_time": 30_000_000_000i64, "delta": 1},
                    {"user_id": "bob", "amount": 5, "event_time": 30_000_000_000i64, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/purchases_distinct_amounts_by_user_minute/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_window_rows(
        &query_response.1,
        4,
        json!([
            {"user_id": "alice", "window_start": 0, "window_end": 60_000_000_000i64, "total_amount": 27, "event_count": 2},
            {"user_id": "bob", "window_start": 0, "window_end": 60_000_000_000i64, "total_amount": 5, "event_count": 1}
        ]),
    );

    let restarted_state =
        test_api_state_with_store(store, "api-test-window-count-distinct-owner-b", true).await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let restarted_app = app(restarted_state);
    let restarted_query = call_json(
        &restarted_app,
        Method::POST,
        "/v1/views/purchases_distinct_amounts_by_user_minute/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(restarted_query.1["rows"], query_response.1["rows"]);
}

#[tokio::test]
async fn rest_tumbling_window_filtered_count_distinct_view_materializes_outputs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(
        store.clone(),
        "api-test-window-filtered-count-distinct-owner-a",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "purchases_filtered_distinct_amounts_by_user_minute".to_string(),
            url_path: Some("/purchases/filtered-distinct-amounts-by-user-minute".to_string()),
            output_relation_id: None,
            input_relation_id: "purchases".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(distinct amount) filter (where amount > 0) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("filtered distinct purchase amounts by user and event-time minute".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "purchases",
                "relation_version": "2026-05-24.v1",
                "stream_id": "purchases-filtered-distinct-window-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "event_time_watermark": {
                    "event_time_column_id": "event_time",
                    "max_observed_event_time_ns": 70_000_000_000i64,
                    "watermark_ns": 60_000_000_000i64
                },
                "rows": [
                    {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                    {"user_id": "alice", "amount": 10, "event_time": 20_000_000_000i64, "delta": 1},
                    {"user_id": "alice", "amount": 7, "event_time": 30_000_000_000i64, "delta": 1},
                    {"user_id": "bob", "amount": 5, "event_time": 30_000_000_000i64, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/purchases_filtered_distinct_amounts_by_user_minute/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_window_rows(
        &query_response.1,
        4,
        json!([
            {"user_id": "alice", "window_start": 0, "window_end": 60_000_000_000i64, "total_amount": 27, "event_count": 2},
            {"user_id": "bob", "window_start": 0, "window_end": 60_000_000_000i64, "total_amount": 0, "event_count": 1}
        ]),
    );

    let restarted_state = test_api_state_with_store(
        store,
        "api-test-window-filtered-count-distinct-owner-b",
        true,
    )
    .await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let restarted_app = app(restarted_state);
    let restarted_query = call_json(
        &restarted_app,
        Method::POST,
        "/v1/views/purchases_filtered_distinct_amounts_by_user_minute/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(restarted_query.1["rows"], query_response.1["rows"]);
}

#[tokio::test]
async fn rest_tumbling_window_nullable_column_count_view_materializes_outputs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(
        store.clone(),
        "api-test-window-nullable-count-owner-a",
        false,
    )
    .await;
    let router = app(state);
    let mut catalog = test_purchases_event_time_catalog();
    let amount = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "amount")
        .unwrap();
    amount.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable event-time purchases catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": catalog,
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "purchases_nullable_amounts_by_user_minute".to_string(),
            url_path: Some("/purchases/nullable-amounts-by-user-minute".to_string()),
            output_relation_id: None,
            input_relation_id: "purchases".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, window_start, window_end, sum(amount) as total_amount, count(amount) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("nullable purchase amount counts by event-time minute".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_response = call_json(
            &router,
            Method::POST,
            "/v1/relations/ingest",
            json!({
                "batches": [{
                    "relation_id": "purchases",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "purchases-nullable-window-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "event_time_watermark": {
                        "event_time_column_id": "event_time",
                        "max_observed_event_time_ns": 70_000_000_000i64,
                        "watermark_ns": 60_000_000_000i64
                    },
                    "rows": [
                        {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                        {"user_id": "alice", "amount": null, "event_time": 20_000_000_000i64, "delta": 1},
                        {"user_id": "alice", "amount": 7, "event_time": 30_000_000_000i64, "delta": 1},
                        {"user_id": "bob", "amount": null, "event_time": 30_000_000_000i64, "delta": 1}
                    ]
                }]
            }),
        )
        .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/purchases_nullable_amounts_by_user_minute/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_window_rows(
        &query_response.1,
        4,
        json!([
            {"user_id": "alice", "window_start": 0, "window_end": 60_000_000_000i64, "total_amount": 17, "event_count": 2}
        ]),
    );

    let restarted_state =
        test_api_state_with_store(store, "api-test-window-nullable-count-owner-b", true).await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let restarted_app = app(restarted_state);
    let restarted_query = call_json(
        &restarted_app,
        Method::POST,
        "/v1/views/purchases_nullable_amounts_by_user_minute/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(restarted_query.1["rows"], query_response.1["rows"]);
}

#[tokio::test]
async fn rest_tumbling_window_filtered_nullable_column_count_view_materializes_outputs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(
        store.clone(),
        "api-test-window-filtered-nullable-count-owner-a",
        false,
    )
    .await;
    let router = app(state);
    let mut catalog = test_purchases_event_time_catalog();
    let amount = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "amount")
        .unwrap();
    amount.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable event-time purchases catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": catalog,
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "purchases_filtered_nullable_amounts_by_user_minute".to_string(),
            url_path: Some("/purchases/filtered-nullable-amounts-by-user-minute".to_string()),
            output_relation_id: None,
            input_relation_id: "purchases".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, window_start, window_end, sum(amount) filter (where amount > 5) as total_amount, count(amount) filter (where event_time >= 0) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("filtered nullable purchase amount counts by event-time minute".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_response = call_json(
            &router,
            Method::POST,
            "/v1/relations/ingest",
            json!({
                "batches": [{
                    "relation_id": "purchases",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "purchases-filtered-nullable-window-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "event_time_watermark": {
                        "event_time_column_id": "event_time",
                        "max_observed_event_time_ns": 70_000_000_000i64,
                        "watermark_ns": 60_000_000_000i64
                    },
                    "rows": [
                        {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                        {"user_id": "alice", "amount": null, "event_time": 20_000_000_000i64, "delta": 1},
                        {"user_id": "alice", "amount": 7, "event_time": 30_000_000_000i64, "delta": 1},
                        {"user_id": "bob", "amount": null, "event_time": 30_000_000_000i64, "delta": 1}
                    ]
                }]
            }),
        )
        .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/purchases_filtered_nullable_amounts_by_user_minute/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_window_rows(
        &query_response.1,
        4,
        json!([
            {"user_id": "alice", "window_start": 0, "window_end": 60_000_000_000i64, "total_amount": 17, "event_count": 2}
        ]),
    );

    let restarted_state = test_api_state_with_store(
        store,
        "api-test-window-filtered-nullable-count-owner-b",
        true,
    )
    .await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let restarted_app = app(restarted_state);
    let restarted_query = call_json(
        &restarted_app,
        Method::POST,
        "/v1/views/purchases_filtered_nullable_amounts_by_user_minute/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(restarted_query.1["rows"], query_response.1["rows"]);
}

#[tokio::test]
async fn rest_hopping_window_advanced_aggregate_view_survives_api_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state =
        test_api_state_with_store(store.clone(), "api-test-hop-advanced-owner-a", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(distinct amount) as event_count, min(amount) as minimum_amount, max(amount) as maximum_amount, avg(amount) as average_amount from purchases group by user_id, hop(interval '30 seconds', interval '60 seconds') having avg(amount) > 11 order by sum(amount) desc limit 1";
    let view_request = CreateViewRequest {
        view_id: "purchases_by_user_hop_stats".to_string(),
        url_path: Some("/purchases/by-user-hop-stats".to_string()),
        output_relation_id: None,
        input_relation_id: "purchases".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: sql.to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("advanced hopping purchase stats by user".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "purchases",
                "relation_version": "2026-05-24.v1",
                "stream_id": "purchases-hop-advanced-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "event_time_watermark": {
                    "event_time_column_id": "event_time",
                    "max_observed_event_time_ns": 90_000_000_000i64,
                    "watermark_ns": 90_000_000_000i64
                },
                "rows": [
                    {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                    {"user_id": "alice", "amount": 14, "event_time": 20_000_000_000i64, "delta": 1},
                    {"user_id": "alice", "amount": 10, "event_time": 40_000_000_000i64, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/purchases_by_user_hop_stats/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    let expected_average = 34.0 / 3.0;
    assert_window_rows(
        &query_response.1,
        3,
        json!([{
            "user_id": "alice",
            "window_start": 0,
            "window_end": 60_000_000_000i64,
            "total_amount": 34,
            "event_count": 2,
            "minimum_amount": 10,
            "maximum_amount": 14,
            "average_amount": expected_average
        }]),
    );

    let restarted_state =
        test_api_state_with_store(store, "api-test-hop-advanced-owner-b", true).await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let restarted_app = app(restarted_state);
    let restarted_query = call_json(
        &restarted_app,
        Method::POST,
        "/v1/views/purchases_by_user_hop_stats/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(restarted_query.1["rows"], query_response.1["rows"]);
}

#[tokio::test]
async fn rest_hopping_and_session_window_views_materialize_outputs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-hop-session-window-owner", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    for (view_id, url_path, sql) in [
            (
                "purchases_by_user_hop",
                "/purchases/by-user-hop",
                "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id, hop(interval '30 seconds', interval '60 seconds')",
            ),
            (
                "purchases_by_user_session",
                "/purchases/by-user-session",
                "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id, session(interval '30 seconds')",
            ),
        ] {
            let view_request = CreateViewRequest {
                view_id: view_id.to_string(),
                url_path: Some(url_path.to_string()),
                output_relation_id: None,
                input_relation_id: "purchases".to_string(),
                input_relation_version: "2026-05-24.v1".to_string(),
                input_relation_refs: Vec::new(),
                input_relations: Vec::new(),
                sql: sql.to_string(),
                source_kind: SqlSourceKind::StandingView,
                output_relation_ids: Vec::new(),
                sql_template: None,
                description: Some(format!("event-time window view {view_id}")),
                request: Vec::new(),
                response_schema: None,
                response_formats: vec!["json".to_string()],
                query_policy_id: None,
            };
            let view_response =
                call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
            assert_eq!(
                view_response.0,
                StatusCode::CREATED,
                "view creation response: {}",
                view_response.1
            );
            assert_eq!(view_response.1["query_enabled"], true);
        }

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "purchases",
            "relation_version": "2026-05-24.v1",
            "stream_id": "hop-session-purchases-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "event_time_watermark": {
                "event_time_column_id": "event_time",
                "max_observed_event_time_ns": 120_000_000_000i64,
                "watermark_ns": 120_000_000_000i64
            },
            "rows": [
                {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                {"user_id": "alice", "amount": 7, "event_time": 25_000_000_000i64, "delta": 1},
                {"user_id": "alice", "amount": 11, "event_time": 80_000_000_000i64, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest_response.0, StatusCode::CREATED);
    assert_eq!(ingest_response.1["outcome"], "appended");

    let hop_query = call_json(
        &router,
        Method::POST,
        "/v1/views/purchases_by_user_hop/query",
        json!({}),
    )
    .await;
    assert_eq!(hop_query.0, StatusCode::OK, "hop query: {}", hop_query.1);
    assert_window_rows(
        &hop_query.1,
        3,
        json!([
            {"user_id": "alice", "window_start": -30_000_000_000i64, "window_end": 30_000_000_000i64, "total_amount": 17, "event_count": 2},
            {"user_id": "alice", "window_start": 0, "window_end": 60_000_000_000i64, "total_amount": 17, "event_count": 2},
            {"user_id": "alice", "window_start": 30_000_000_000i64, "window_end": 90_000_000_000i64, "total_amount": 11, "event_count": 1},
            {"user_id": "alice", "window_start": 60_000_000_000i64, "window_end": 120_000_000_000i64, "total_amount": 11, "event_count": 1}
        ]),
    );

    let session_query = call_json(
        &router,
        Method::POST,
        "/v1/views/purchases_by_user_session/query",
        json!({}),
    )
    .await;
    assert_eq!(
        session_query.0,
        StatusCode::OK,
        "session query: {}",
        session_query.1
    );
    assert_window_rows(
        &session_query.1,
        3,
        json!([
            {"user_id": "alice", "window_start": 10_000_000_000i64, "window_end": 25_000_000_000i64, "total_amount": 17, "event_count": 2},
            {"user_id": "alice", "window_start": 80_000_000_000i64, "window_end": 80_000_000_000i64, "total_amount": 11, "event_count": 1}
        ]),
    );
}

#[tokio::test]
async fn rest_session_window_view_materialized_output_survives_api_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state =
        test_api_state_with_store(store.clone(), "api-test-session-window-owner-a", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "purchases_by_user_session_restart".to_string(),
            url_path: Some("/purchases/by-user-session-restart".to_string()),
            output_relation_id: None,
            input_relation_id: "purchases".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id, session(interval '30 seconds')".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("restart-restored session window view".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/purchases/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "session-restart-purchases-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "event_time_watermark": {
                "event_time_column_id": "event_time",
                "max_observed_event_time_ns": 120_000_000_000i64,
                "watermark_ns": 120_000_000_000i64
            },
            "rows": [
                {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                {"user_id": "alice", "amount": 7, "event_time": 25_000_000_000i64, "delta": 1},
                {"user_id": "alice", "amount": 11, "event_time": 80_000_000_000i64, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );
    assert_eq!(ingest_response.1["materialization"]["status"], "completed");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/purchases_by_user_session_restart/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_window_rows(
        &query_response.1,
        3,
        json!([
            {"user_id": "alice", "window_start": 10_000_000_000i64, "window_end": 25_000_000_000i64, "total_amount": 17, "event_count": 2},
            {"user_id": "alice", "window_start": 80_000_000_000i64, "window_end": 80_000_000_000i64, "total_amount": 11, "event_count": 1}
        ]),
    );

    let restarted_state =
        test_api_state_with_store(store, "api-test-session-window-owner-b", true).await;
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restarted_router = app(restarted_state);
    let restarted_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/purchases_by_user_session_restart/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(restarted_query.1["rows"], query_response.1["rows"]);
}

#[tokio::test]
async fn rest_late_tumbling_window_view_reports_materialization_lag_on_first_query() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-late-window-owner", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(
        relation_response.0,
        StatusCode::CREATED,
        "relation creation response: {}",
        relation_response.1
    );

    let first_ingest = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "purchases",
            "relation_version": "2026-05-24.v1",
            "stream_id": "late-purchases-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "event_time_watermark": {
                "event_time_column_id": "event_time",
                "max_observed_event_time_ns": 80_000_000_000i64,
                "watermark_ns": 60_000_000_000i64
            },
            "rows": [
                {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                {"user_id": "bob", "amount": 5, "event_time": 30_000_000_000i64, "delta": 1},
                {"user_id": "alice", "amount": 7, "event_time": 80_000_000_000i64, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(
        first_ingest.0,
        StatusCode::CREATED,
        "initial ingest response: {}",
        first_ingest.1
    );

    let sql = "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id, tumble(interval '60 seconds')";
    let view_request = CreateViewRequest {
        view_id: "late_purchases_by_user_minute".to_string(),
        url_path: Some("/purchases/late-by-user-minute".to_string()),
        output_relation_id: None,
        input_relation_id: "purchases".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: sql.to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("late-created purchase totals by event-time minute".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(
        view_response.0,
        StatusCode::CREATED,
        "late window view creation response: {}",
        view_response.1
    );
    assert_eq!(view_response.1["query_enabled"], false);
    assert_eq!(view_response.1["coverage"]["state"], "backfill_required");

    let second_ingest = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "purchases",
            "relation_version": "2026-05-24.v1",
            "stream_id": "late-purchases-stream",
            "partition_id": 0,
            "start_offset_inclusive": 3,
            "event_time_watermark": {
                "event_time_column_id": "event_time",
                "max_observed_event_time_ns": 120_000_000_000i64,
                "watermark_ns": 120_000_000_000i64
            },
            "rows": [
                {"user_id": "bob", "amount": 11, "event_time": 80_000_000_000i64, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(
        second_ingest.0,
        StatusCode::CREATED,
        "late window view must not block later ingest: {}",
        second_ingest.1
    );

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/late_purchases_by_user_minute/query",
        json!({}),
    )
    .await;
    assert_eq!(
        query_response.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "late window query response: {}",
        query_response.1
    );
    assert!(query_response.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("MATERIALIZATION_LAG"));
    assert_eq!(query_response.1["details"]["code"], "MATERIALIZATION_LAG");
    assert_eq!(
        query_response.1["details"]["view_id"],
        "late_purchases_by_user_minute"
    );
    assert_eq!(
        query_response.1["details"]["query_authority"],
        "published_materialized_output"
    );
    assert_eq!(
        query_response.1["details"]["coverage_state"],
        "backfill_required"
    );
    assert_eq!(
        query_response.1["details"]["committed_frontier"]["status"],
        "ahead_of_materialized_output"
    );
    assert_eq!(
        query_response.1["details"]["committed_frontier"]["source_read_on_query_path"],
        false
    );
    assert_eq!(
        query_response.1["details"]["materialized_frontier"]["status"],
        "not_queryable_until_backfill_checkpoint_published"
    );

    let refreshed_view = call_json(
        &router,
        Method::GET,
        "/v1/views/late_purchases_by_user_minute",
        json!({}),
    )
    .await;
    assert_eq!(refreshed_view.0, StatusCode::OK);
    assert_eq!(refreshed_view.1["query_enabled"], false);
    assert_eq!(
        refreshed_view.1["lifecycle"]["deployment_status"],
        "deploying"
    );
}

#[tokio::test]
async fn rest_late_hopping_and_session_window_views_report_materialization_lag_on_first_query() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state =
        test_api_state_with_store(store, "api-test-late-hop-session-window-owner", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "purchases",
            "relation_version": "2026-05-24.v1",
            "stream_id": "late-hop-session-purchases-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "event_time_watermark": {
                "event_time_column_id": "event_time",
                "max_observed_event_time_ns": 120_000_000_000i64,
                "watermark_ns": 120_000_000_000i64
            },
            "rows": [
                {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                {"user_id": "alice", "amount": 7, "event_time": 25_000_000_000i64, "delta": 1},
                {"user_id": "alice", "amount": 11, "event_time": 80_000_000_000i64, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest_response.0, StatusCode::CREATED);
    assert_eq!(ingest_response.1["outcome"], "appended");

    for (view_id, url_path, sql) in [
            (
                "late_purchases_by_user_hop",
                "/purchases/late-by-user-hop",
                "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id, hop(interval '30 seconds', interval '60 seconds')",
            ),
            (
                "late_purchases_by_user_session",
                "/purchases/late-by-user-session",
                "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from purchases group by user_id, session(interval '30 seconds')",
            ),
        ] {
            let view_request = CreateViewRequest {
                view_id: view_id.to_string(),
                url_path: Some(url_path.to_string()),
                output_relation_id: None,
                input_relation_id: "purchases".to_string(),
                input_relation_version: "2026-05-24.v1".to_string(),
                input_relation_refs: Vec::new(),
                input_relations: Vec::new(),
                sql: sql.to_string(),
                source_kind: SqlSourceKind::StandingView,
                output_relation_ids: Vec::new(),
                sql_template: None,
                description: Some(format!("late-created event-time window view {view_id}")),
                request: Vec::new(),
                response_schema: None,
                response_formats: vec!["json".to_string()],
                query_policy_id: None,
            };
            let view_response =
                call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
            assert_eq!(
                view_response.0,
                StatusCode::CREATED,
                "view creation response: {}",
                view_response.1
            );
            assert_eq!(view_response.1["query_enabled"], false);
            assert_eq!(view_response.1["coverage"]["state"], "backfill_required");
        }

    let hop_query = call_json(
        &router,
        Method::POST,
        "/v1/views/late_purchases_by_user_hop/query",
        json!({}),
    )
    .await;
    assert_eq!(
        hop_query.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "hop query: {}",
        hop_query.1
    );
    assert!(hop_query.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("MATERIALIZATION_LAG"));

    let session_query = call_json(
        &router,
        Method::POST,
        "/v1/views/late_purchases_by_user_session/query",
        json!({}),
    )
    .await;
    assert_eq!(
        session_query.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "session query: {}",
        session_query.1
    );
    assert!(session_query.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("MATERIALIZATION_LAG"));

    for view_id in [
        "late_purchases_by_user_hop",
        "late_purchases_by_user_session",
    ] {
        let refreshed_view = call_json(
            &router,
            Method::GET,
            &format!("/v1/views/{view_id}"),
            json!({}),
        )
        .await;
        assert_eq!(refreshed_view.0, StatusCode::OK);
        assert_eq!(refreshed_view.1["query_enabled"], false);
        assert_eq!(
            refreshed_view.1["lifecycle"]["deployment_status"],
            "deploying"
        );
    }
}

#[tokio::test]
async fn rest_unsupported_single_input_sql_admission_fails_closed_without_creating_view() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-unsupported-single-input-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    for (view_id, sql) in [
            (
                "unsupported_scores_window_function",
                "select user_id, row_number() over (partition by user_id order by score) as sum, count(*) as count from scores group by user_id",
            ),
            (
                "unsupported_scores_bad_offset",
                "select user_id, sum(score) as sum, count(*) as count from scores group by user_id order by sum desc limit 1 offset delta",
            ),
            (
                "unsupported_scores_rollup",
                "select user_id, sum(score) as sum, count(*) as count from scores group by rollup(user_id)",
            ),
            (
                "unsupported_scores_union_all",
                "select user_id, score from scores where score > 0 union all select user_id, score from scores where score >= 10",
            ),
        ] {
            let view_request = CreateViewRequest {
                view_id: view_id.to_string(),
                url_path: None,
                output_relation_id: None,
                input_relation_id: "scores".to_string(),
                input_relation_version: "2026-05-24.v1".to_string(),
                input_relation_refs: Vec::new(),
                input_relations: Vec::new(),
                sql: sql.to_string(),
                source_kind: SqlSourceKind::StandingView,
                output_relation_ids: Vec::new(),
                sql_template: None,
                description: Some("unsupported single-input SQL".to_string()),
                request: Vec::new(),
                response_schema: None,
                response_formats: vec!["json".to_string()],
                query_policy_id: None,
            };
            let response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
            assert!(
                response.0.is_client_error(),
                "unsupported SQL should fail with 4xx, got {response:?}"
            );
        }

    let views = call_json(&router, Method::GET, "/v1/views", Value::Null).await;
    assert_eq!(views.0, StatusCode::OK);
    assert_eq!(views.1["views"], json!([]));
}

#[tokio::test]
async fn rest_unsupported_join_sql_admission_fails_closed_without_creating_view() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-unsupported-join-owner",
        false,
    )
    .await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    for (view_id, sql) in [
            (
                "unsupported_scores_left_join",
                "select a.account_id, sum(s.score) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id group by a.account_id",
            ),
            (
                "unsupported_scores_cross_join",
                "select a.account_id, sum(s.score) as sum, count(*) as count from scores s cross join accounts a group by a.account_id",
            ),
            (
                "unsupported_scores_non_equi_join",
                "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id <> a.account_id group by a.account_id",
            ),
        ] {
            let view_request = CreateViewRequest {
                view_id: view_id.to_string(),
                url_path: None,
                output_relation_id: None,
                input_relation_id: String::new(),
                input_relation_version: String::new(),
                input_relation_refs: vec![
                    InputRelationRef {
                        relation_id: "scores".to_string(),
                        relation_version: "2026-05-24.v1".to_string(),
                    },
                    InputRelationRef {
                        relation_id: "accounts".to_string(),
                        relation_version: "2026-05-24.v1".to_string(),
                    },
                ],
                input_relations: Vec::new(),
                sql: sql.to_string(),
                source_kind: SqlSourceKind::StandingView,
                output_relation_ids: Vec::new(),
                sql_template: None,
                description: Some("unsupported join SQL".to_string()),
                request: Vec::new(),
                response_schema: None,
                response_formats: vec!["json".to_string()],
                query_policy_id: None,
            };
            let response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
            assert!(
                response.0.is_client_error(),
                "unsupported join SQL should fail with 4xx, got {response:?}"
            );
        }

    let views = call_json(&router, Method::GET, "/v1/views", Value::Null).await;
    assert_eq!(views.0, StatusCode::OK);
    assert_eq!(views.1["views"], json!([]));
}

#[tokio::test]
async fn rest_unsupported_three_table_join_admission_fails_closed_without_active_view() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-unsupported-three-table-join-owner",
        false,
    )
    .await;
    let router = app(state);

    for catalog in [
        test_scores_catalog(),
        test_accounts_catalog(),
        test_device_status_catalog(),
    ] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_id = "unsupported_scores_three_table_join";
    let view_request = CreateViewRequest {
            view_id: view_id.to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "device_status".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id join device_status d on d.device_id = s.user_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("unsupported three-table join SQL".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };

    let create_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert!(
        create_response.0.is_client_error(),
        "unsupported three-table join should fail with 4xx, got {create_response:?}"
    );

    let views = call_json(&router, Method::GET, "/v1/views", Value::Null).await;
    assert_eq!(views.0, StatusCode::OK);
    assert_eq!(views.1["views"], json!([]));

    let get_response = call_json(
        &router,
        Method::GET,
        &format!("/v1/views/{view_id}"),
        Value::Null,
    )
    .await;
    assert!(
        get_response.0.is_client_error(),
        "rejected view should not be readable as active, got {get_response:?}"
    );

    let query_response = call_json(
        &router,
        Method::POST,
        &format!("/v1/views/{view_id}/query"),
        json!({}),
    )
    .await;
    assert!(
        query_response.0.is_client_error(),
        "rejected view should not be queryable, got {query_response:?}"
    );
}

#[tokio::test]
async fn rest_sql_admission_corpus_fails_closed_without_metadata_or_runtime_binding() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-sql-admission-corpus-owner",
        false,
    )
    .await;
    let router = app(state);

    for catalog in [
        test_scores_catalog(),
        test_accounts_catalog(),
        test_device_status_catalog(),
    ] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let scores = vec![InputRelationRef {
        relation_id: "scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
    }];
    let scores_accounts = vec![
        InputRelationRef {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
        },
        InputRelationRef {
            relation_id: "accounts".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
        },
    ];
    let scores_accounts_devices = vec![
        InputRelationRef {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
        },
        InputRelationRef {
            relation_id: "accounts".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
        },
        InputRelationRef {
            relation_id: "device_status".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
        },
    ];

    for (view_id, sql, refs) in [
            (
                "corpus_window_row_number",
                "select user_id, row_number() over (partition by user_id order by score) as row_number from scores",
                scores.clone(),
            ),
            (
                "corpus_window_aggregate_over",
                "select user_id, sum(score) over (partition by user_id) as sum from scores",
                scores.clone(),
            ),
            (
                "corpus_tumble_window",
                "select window_start, user_id, sum(amount) as sum from tumble(purchases, ts, interval '1 minute') group by window_start, user_id",
                scores.clone(),
            ),
            (
                "corpus_hop_window",
                "select window_start, user_id, sum(amount) as sum from hop(purchases, ts, interval '1 minute', interval '5 minutes') group by window_start, user_id",
                scores.clone(),
            ),
            (
                "corpus_session_window",
                "select window_start, user_id, sum(amount) as sum from session(purchases, ts, interval '5 minutes') group by window_start, user_id",
                scores.clone(),
            ),
            (
                "corpus_rollup",
                "select user_id, sum(score) as sum from scores group by rollup(user_id)",
                scores.clone(),
            ),
            (
                "corpus_union_all",
                "select user_id, score from scores where score > 0 union all select user_id, score from scores where score >= 10",
                scores.clone(),
            ),
            (
                "corpus_offset_expression",
                "select user_id, sum(score) as sum from scores group by user_id order by sum desc limit 1 offset delta",
                scores.clone(),
            ),
            (
                "corpus_cross_join",
                "select a.account_id, sum(s.score) as sum from scores s cross join accounts a group by a.account_id",
                scores_accounts.clone(),
            ),
            (
                "corpus_non_equi_join",
                "select a.account_id, sum(s.score) as sum from scores s join accounts a on s.user_id <> a.account_id group by a.account_id",
                scores_accounts.clone(),
            ),
            (
                "corpus_three_table_join",
                "select a.account_id, sum(s.score) as sum from scores s join accounts a on s.user_id = a.account_id join device_status d on d.device_id = s.user_id group by a.account_id",
                scores_accounts_devices.clone(),
            ),
        ] {
            let view_request = CreateViewRequest {
                view_id: view_id.to_string(),
                url_path: None,
                output_relation_id: None,
                input_relation_id: String::new(),
                input_relation_version: String::new(),
                input_relation_refs: refs,
                input_relations: Vec::new(),
                sql: sql.to_string(),
                source_kind: SqlSourceKind::StandingView,
                output_relation_ids: Vec::new(),
                sql_template: None,
                description: Some("unsupported SQL admission corpus".to_string()),
                request: Vec::new(),
                response_schema: None,
                response_formats: vec!["json".to_string()],
                query_policy_id: None,
            };

            let create_response =
                call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
            assert!(
                create_response.0.is_client_error(),
                "unsupported SQL corpus case `{view_id}` should fail with 4xx, got {create_response:?}"
            );

            let get_response = call_json(
                &router,
                Method::GET,
                &format!("/v1/views/{view_id}"),
                Value::Null,
            )
            .await;
            assert!(
                get_response.0.is_client_error(),
                "rejected corpus case `{view_id}` should not leave view metadata, got {get_response:?}"
            );

            let query_response = call_json(
                &router,
                Method::POST,
                &format!("/v1/views/{view_id}/query"),
                json!({}),
            )
            .await;
            assert!(
                query_response.0.is_client_error(),
                "rejected corpus case `{view_id}` should not leave runtime binding, got {query_response:?}"
            );
        }

    let views = call_json(&router, Method::GET, "/v1/views", Value::Null).await;
    assert_eq!(views.0, StatusCode::OK);
    assert_eq!(views.1["views"], json!([]));
}

async fn call_json(app: &Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!({ "error": String::from_utf8_lossy(&bytes).to_string() }))
    };
    (status, value)
}

#[test]
fn ingest_epoch_request_rejects_ack_mode_negotiation_field() {
    let error = serde_json::from_value::<IngestEpochRequest>(json!({
        "ack_mode": "append_committed",
        "batches": [{
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{"user_id": "alice", "score": 1, "delta": 1}]
        }]
    }))
    .unwrap_err();

    assert!(error.to_string().contains("ack_mode"));
}

#[test]
fn output_compaction_policy_schedules_only_on_configured_epoch_interval() {
    assert!(!should_schedule_background_output_compaction(0, 10));
    assert!(!should_schedule_background_output_compaction(3, 0));
    assert!(!should_schedule_background_output_compaction(3, 5));
    assert!(should_schedule_background_output_compaction(3, 6));
}

#[tokio::test]
async fn background_compaction_registry_deduplicates_same_view_work() {
    let state = test_api_state().await;

    assert!(state.try_start_background_compaction("scores_by_user"));
    assert!(!state.try_start_background_compaction("scores_by_user"));
    state.record_background_compaction_already_running();
    assert!(state.try_start_background_compaction("orders_by_region"));

    let status = state.background_task_status();
    assert_eq!(status.compaction_already_running, 1);

    state.finish_background_compaction("scores_by_user");
    assert!(state.try_start_background_compaction("scores_by_user"));
}

#[tokio::test]
async fn direct_apply_uses_prepared_current_batch_without_replaying_ingest_object() {
    let object_access_counts = ObjectStoreAccessCounts::default();
    let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        Arc::new(InMemory::new()),
        object_access_counts.clone(),
    ));
    let state = test_api_state_with_store(store, "api-test-direct-apply-owner", false).await;
    let router = app(state.clone());

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "direct_apply_scores_by_user".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("direct apply current batch proof".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED);

    let prepared = prepare_ingest_batch(
        &state,
        IngestRowsRequest {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            stream_id: "direct-apply-scores-stream".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            event_time_watermark: None,
            rows: vec![json!({"user_id": "alice", "score": 10, "delta": 1})],
        },
        None,
    )
    .await
    .unwrap();

    object_access_counts.clear();
    let summary = apply_standing_runtime_prepared_ingests(&state, None, &[prepared], None)
        .await
        .unwrap();

    assert_eq!(summary.active_views, 1);
    assert_eq!(summary.applied_batches, 1);
    let ingest_get_paths = object_access_counts
        .get_paths()
        .into_iter()
        .filter(|path| path.starts_with("v1/ingest/"))
        .collect::<Vec<_>>();
    assert_eq!(
        ingest_get_paths,
        Vec::<String>::new(),
        "direct apply must use the prepared current batch instead of reading source ingest objects"
    );

    let next_prepared = prepare_ingest_batch(
        &state,
        IngestRowsRequest {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            stream_id: "direct-apply-scores-stream".to_string(),
            partition_id: 0,
            start_offset_inclusive: 1,
            event_time_watermark: None,
            rows: vec![json!({"user_id": "alice", "score": 5, "delta": 1})],
        },
        None,
    )
    .await
    .unwrap();

    object_access_counts.clear();
    let second_summary =
        apply_standing_runtime_prepared_ingests(&state, None, &[next_prepared], None)
            .await
            .unwrap();
    assert_eq!(second_summary.applied_batches, 1);
    let checkpoint_get_paths = object_access_counts
        .get_paths()
        .into_iter()
        .filter(|path| path.starts_with("v1/standing-runtime-checkpoints/"))
        .collect::<Vec<_>>();
    assert_eq!(
        checkpoint_get_paths.len(),
        1,
        "direct apply should reuse the pre-apply checkpoint record during checkpoint publish"
    );
    let checkpoint_list_prefixes = object_access_counts
        .list_prefixes()
        .into_iter()
        .filter(|path| path.starts_with("v1/standing-runtime-checkpoints/"))
        .collect::<Vec<_>>();
    assert_eq!(
        checkpoint_list_prefixes,
        Vec::<String>::new(),
        "direct apply should read latest checkpoint cache instead of listing checkpoint epochs"
    );
}

#[tokio::test]
async fn rest_relation_scoped_materialized_ingest_uses_prepared_current_batch_without_object_replay(
) {
    let baseline_counts = ObjectStoreAccessCounts::default();
    let baseline_store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        Arc::new(InMemory::new()),
        baseline_counts.clone(),
    ));
    let baseline_state =
        test_api_state_with_store(baseline_store, "api-test-rest-direct-apply-baseline", false)
            .await;
    let baseline_router = app(baseline_state);
    let baseline_relation = call_json(
        &baseline_router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(baseline_relation.0, StatusCode::CREATED);
    baseline_counts.clear();
    let baseline_ingest = call_json(
        &baseline_router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "rest-direct-apply-baseline-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
        }),
    )
    .await;
    assert_eq!(
        baseline_ingest.0,
        StatusCode::CREATED,
        "{baseline_ingest:?}"
    );
    let baseline_ingest_list_count = baseline_counts
        .list_prefixes()
        .into_iter()
        .filter(|path| path == "v1/ingest")
        .count();
    let baseline_ingest_get_count = baseline_counts
        .get_paths()
        .into_iter()
        .filter(|path| path.starts_with("v1/ingest/"))
        .count();

    let object_access_counts = ObjectStoreAccessCounts::default();
    let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        Arc::new(InMemory::new()),
        object_access_counts.clone(),
    ));
    let state = test_api_state_with_store(store, "api-test-rest-direct-apply-owner", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "rest_direct_apply_scores_by_user".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("REST direct apply current batch proof".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    object_access_counts.clear();
    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "rest-direct-apply-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );
    assert_eq!(ingest_response.1["ack_mode"], "materialized");
    assert_eq!(ingest_response.1["materialization"]["status"], "completed");
    assert_eq!(ingest_response.1["materialization"]["applied_batches"], 1);
    let ingest_list_count = object_access_counts
        .list_prefixes()
        .into_iter()
        .filter(|path| path == "v1/ingest")
        .count();
    let ingest_get_count = object_access_counts
        .get_paths()
        .into_iter()
        .filter(|path| path.starts_with("v1/ingest/"))
        .count();
    assert_eq!(
            ingest_list_count,
            baseline_ingest_list_count,
            "relation-scoped materialized ingest should not add committed-ingest replay listing beyond the append/admission baseline"
        );
    assert_eq!(
            ingest_get_count,
            baseline_ingest_get_count,
            "relation-scoped materialized ingest should not add ingest object replay reads beyond the append/admission baseline"
        );

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/rest_direct_apply_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"user_id": "alice", "sum": 10, "count": 1}])
    );
}

#[tokio::test]
async fn rest_relation_scoped_ingest_materializes_views_automatically() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-relation-ingest", false)
            .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "relation_scoped_scores_by_user".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("relation scoped ingest proof".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "relation-scoped-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": -4, "delta": 1},
                {"user_id": "bob", "score": 7, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );
    assert_eq!(ingest_response.1["outcome"], "appended");
    assert_eq!(ingest_response.1["ack_mode"], "materialized");
    assert_eq!(ingest_response.1["materialization"]["status"], "completed");
    assert_eq!(ingest_response.1["materialization"]["active_views"], 1);
    assert_eq!(ingest_response.1["materialization"]["applied_batches"], 1);
    assert_eq!(ingest_response.1["batches"][0]["outcome"], "appended");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/relation_scoped_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"user_id": "alice", "sum": 10, "count": 1},
            {"user_id": "bob", "sum": 7, "count": 1}
        ])
    );
}

#[tokio::test]
async fn rest_relation_batch_ingest_materializes_multiple_batches_without_ingest_replay() {
    let baseline_counts = ObjectStoreAccessCounts::default();
    let baseline_store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        Arc::new(InMemory::new()),
        baseline_counts.clone(),
    ));
    let baseline_state = test_api_state_with_store(
        baseline_store,
        "api-test-relation-batch-baseline-owner",
        false,
    )
    .await;
    let baseline_router = app(baseline_state);
    let baseline_relation = call_json(
        &baseline_router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(baseline_relation.0, StatusCode::CREATED);
    baseline_counts.clear();
    let baseline_ingest = call_json(
        &baseline_router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "relation-batch-baseline-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
                },
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "relation-batch-baseline-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 1,
                    "rows": [{"user_id": "bob", "score": 7, "delta": 1}]
                }
            ]
        }),
    )
    .await;
    assert_eq!(
        baseline_ingest.0,
        StatusCode::CREATED,
        "{baseline_ingest:?}"
    );
    let baseline_ingest_list_count = baseline_counts
        .list_prefixes()
        .into_iter()
        .filter(|path| path == "v1/ingest")
        .count();
    let baseline_ingest_get_count = baseline_counts
        .get_paths()
        .into_iter()
        .filter(|path| path.starts_with("v1/ingest/"))
        .count();

    let object_access_counts = ObjectStoreAccessCounts::default();
    let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        Arc::new(InMemory::new()),
        object_access_counts.clone(),
    ));
    let state =
        test_api_state_with_store(store, "api-test-relation-batch-direct-apply-owner", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "relation_batch_scores_by_user".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("relation batch ingest direct apply proof".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    object_access_counts.clear();
    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "relation-batch-direct-apply-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
                },
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "relation-batch-direct-apply-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 1,
                    "rows": [{"user_id": "bob", "score": 7, "delta": 1}]
                }
            ]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );
    assert_eq!(ingest_response.1["ack_mode"], "materialized");
    assert_eq!(ingest_response.1["materialization"]["status"], "completed");
    assert_eq!(ingest_response.1["materialization"]["active_views"], 1);
    assert_eq!(ingest_response.1["materialization"]["applied_batches"], 2);
    assert_eq!(
        ingest_response.1["materialization"]["applied_batches_per_checkpoint_write"],
        2
    );
    assert_eq!(ingest_response.1["timings"]["batch_count"], 2);
    assert_eq!(ingest_response.1["timings"]["row_count"], 2);
    assert!(ingest_response.1["timings"]["avg_batch_us"]
        .as_u64()
        .is_some());
    assert!(ingest_response.1["timings"]["avg_row_us"]
        .as_u64()
        .is_some());
    assert!(ingest_response.1["timings"]["rows_per_second"]
        .as_u64()
        .is_some());
    let ingest_list_count = object_access_counts
        .list_prefixes()
        .into_iter()
        .filter(|path| path == "v1/ingest")
        .count();
    let ingest_get_count = object_access_counts
        .get_paths()
        .into_iter()
        .filter(|path| path.starts_with("v1/ingest/"))
        .count();
    assert_eq!(
            ingest_list_count,
            baseline_ingest_list_count,
            "relation batch materialized ingest should not add committed-ingest replay listing beyond the append/admission baseline"
        );
    assert_eq!(
            ingest_get_count,
            baseline_ingest_get_count,
            "relation batch materialized ingest should not add ingest object replay reads beyond the append/admission baseline"
        );

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/relation_batch_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"user_id": "alice", "sum": 10, "count": 1},
            {"user_id": "bob", "sum": 7, "count": 1}
        ])
    );
}

#[tokio::test]
async fn rest_row_number_view_materializes_relation_scoped_ingest_and_survives_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state =
        test_api_state_with_store(store.clone(), "api-test-row-number-owner-a", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_accounts_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "accounts_ranked_by_tier".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "accounts".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select account_id, row_number() over (partition by tier order by limit desc, account_id asc) as tier_rank from accounts where limit > 0".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("account tier row_number ranking".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/accounts/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "accounts-row-number-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                {"account_id": "aaron", "limit": 100, "tier": "gold", "delta": 1},
                {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1},
                {"account_id": "carol", "limit": 80, "tier": "silver", "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );
    assert_eq!(ingest_response.1["materialization"]["status"], "completed");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/accounts_ranked_by_tier/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"account_id": "aaron", "tier_rank": 1},
            {"account_id": "alice", "tier_rank": 2},
            {"account_id": "bob", "tier_rank": 3},
            {"account_id": "carol", "tier_rank": 1}
        ])
    );

    let restarted_state =
        test_api_state_with_store(store, "api-test-row-number-owner-b", true).await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let restarted_router = app(restarted_state);
    let restarted_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/accounts_ranked_by_tier/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(restarted_query.1["rows"], query_response.1["rows"]);
}

#[tokio::test]
async fn rest_late_filter_project_view_reports_materialization_lag_on_first_query() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-late-filter-project-owner", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "late-filter-project-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "bob", "score": -2, "delta": 1},
                {"user_id": "carol", "score": 7, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let view_request = CreateViewRequest {
        view_id: "late_positive_scores_project".to_string(),
        url_path: Some("/scores/late-positive-project".to_string()),
        output_relation_id: None,
        input_relation_id: "scores".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: "select user_id, score from scores where score > 0".to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("late-created positive score projection".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], false);
    assert_eq!(view_response.1["coverage"]["state"], "backfill_required");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/late_positive_scores_project/query",
        json!({}),
    )
    .await;
    assert_eq!(
        query_response.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "{query_response:?}"
    );
    assert!(query_response.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("MATERIALIZATION_LAG"));
}

#[tokio::test]
async fn rest_between_predicate_aggregate_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-between-owner", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "between_purchases_by_user".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "purchases".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(amount) as sum, count(*) as count from purchases where amount between 6 and 20 group by user_id having sum(amount) between 10 and 20".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("between predicate materialization proof".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/purchases/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "between-purchases-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "amount": 10, "delta": 1},
                {"user_id": "bob", "amount": 5, "delta": 1},
                {"user_id": "alice", "amount": 7, "delta": 1},
                {"user_id": "carol", "amount": 21, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );
    assert_eq!(ingest_response.1["materialization"]["status"], "completed");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/between_purchases_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"user_id": "alice", "sum": 17, "count": 2}])
    );
}

#[tokio::test]
async fn rest_ingest_retry_converges_after_runtime_failure_after_durable_append() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state =
        test_api_state_with_store(store, "api-test-runtime-failure-retry-owner", false).await;
    state.register_standing_program_runtime_factory(
        MATERIALIZED_VIEW_RUNTIME_NAME,
        FailingApplyRuntimeFactory::new("injected materialization failure after durable append"),
    );
    let router = app(state.clone());

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "retry_scores_by_user".to_string(),
            url_path: Some("/scores/retry".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("runtime failure retry proof".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_request = IngestRowsRequest {
        relation_id: "scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        stream_id: "retry-scores-stream".to_string(),
        partition_id: 0,
        start_offset_inclusive: 0,
        event_time_watermark: None,
        rows: vec![json!({"user_id": "alice", "score": 10, "delta": 1})],
    };
    let prepared = prepare_ingest_batch(&state, ingest_request.clone(), None)
        .await
        .unwrap();
    let epoch_manifest_id =
        ingest_epoch_manifest_id(&[ingest_epoch_manifest_batch_record(&prepared).unwrap()])
            .unwrap();

    let failed = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": ingest_request.relation_version,
            "stream_id": ingest_request.stream_id,
            "partition_id": ingest_request.partition_id,
            "start_offset_inclusive": ingest_request.start_offset_inclusive,
            "rows": ingest_request.rows
        }),
    )
    .await;
    assert!(!failed.0.is_success(), "{failed:?}");
    assert!(failed.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("injected materialization failure after durable append"));

    let active = state
        .view_registry()
        .unwrap()
        .read_active("retry_scores_by_user")
        .await
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap().clone();
    let epoch_manifest = PersistedIngestEpochManifest {
        epoch_manifest_key: ObjectKey::ingest_epoch_manifest(&epoch_manifest_id)
            .unwrap()
            .as_str()
            .to_string(),
        epoch_manifest_id: epoch_manifest_id.clone(),
    };
    let marker = read_ingest_epoch_view_runtime_failure(
        &state,
        &epoch_manifest,
        &identity,
        "retry_scores_by_user",
    )
    .await
    .unwrap()
    .unwrap();
    assert!(marker
        .failure_reason
        .contains("injected materialization failure after durable append"));

    let blocked_retry = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "retry-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
        }),
    )
    .await;
    assert_eq!(
        blocked_retry.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "{blocked_retry:?}"
    );
    assert!(blocked_retry.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("durable runtime failure marker"));

    state.register_standing_program_runtime_factory(
        MATERIALIZED_VIEW_RUNTIME_NAME,
        MaterializedViewRuntimeFactory,
    );
    let repair = call_json(
        &router,
        Method::POST,
        "/v1/standing-runtime/ingest-epoch-failures/repair",
        json!({
            "epoch_manifest_id": epoch_manifest_id,
            "tenant_id": identity.tenant_id,
            "program_id": identity.program_id,
            "view_id": "retry_scores_by_user",
            "confirm_standing_runtime_repaired": true,
            "repair_reason": "test restored the materialized view runtime factory"
        }),
    )
    .await;
    assert_eq!(repair.0, StatusCode::OK, "{repair:?}");

    let repaired_retry = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "retry-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
        }),
    )
    .await;
    assert_eq!(repaired_retry.0, StatusCode::OK, "{repaired_retry:?}");
    assert_eq!(repaired_retry.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/retry_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([{"user_id": "alice", "sum": 10, "count": 1}])
    );
}

#[tokio::test]
async fn rest_ingest_optimization_modes_report_timings_and_materialize_correctly() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-ingest-optimization-owner", false).await;
    let router = app(state);

    let ready = call_json(&router, Method::GET, "/readyz", Value::Null).await;
    assert_eq!(ready.0, StatusCode::OK, "{ready:?}");
    assert_eq!(
        ready.1["materialization_policy"]["preferred_ingest_path"],
        "/v1/relations/{relation_id}/ingest"
    );
    assert_eq!(
        ready.1["materialization_policy"]["multi_relation_ingest_path"],
        "/v1/relations/ingest"
    );
    assert_eq!(
        ready.1["materialization_policy"]["ack_modes"],
        json!(["materialized"])
    );
    assert_eq!(
        ready.1["materialization_policy"]["enforced_public_1_0_limits"]
            ["max_output_delta_records_per_commit"],
        DEFAULT_MAX_STANDING_RUNTIME_OUTPUT_DELTA_RECORDS
    );
    assert_eq!(
        ready.1["materialization_policy"]["enforced_public_1_0_limits"]
            ["max_state_payload_bytes_per_checkpoint"],
        DEFAULT_MAX_STANDING_RUNTIME_STATE_PAYLOAD_BYTES
    );
    assert!(!ready.1["materialization_policy"]
        .as_object()
        .unwrap()
        .contains_key("append_committed_background_materialization"));
    assert_eq!(
        ready.1["materialization_policy"]["checkpoint_coalescing"],
        "one checkpoint publish per affected active view per committed epoch"
    );
    assert_eq!(
            ready.1["materialization_policy"]["latency_diagnostics"],
            "ingest responses include total_us, avg_batch_us, avg_row_us, rows_per_second, workload shape, and write coalescing counters; detailed stage timings belong in traces/metrics"
        );
    assert!(
        ready.1["materialization_policy"]["materialization_write_counters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|counter| counter == "state_payload_writes")
    );
    assert!(ready.1.get("background_tasks").is_none());
    assert!(ready.1["materialization_policy"]
        .get("output_compaction")
        .is_none());

    let openapi_response = call_json(&router, Method::GET, "/v1/openapi.json", Value::Null).await;
    assert_eq!(openapi_response.0, StatusCode::OK, "{openapi_response:?}");
    let ingest_properties = &openapi_response.1["paths"]["/v1/relations/{relation_id}/ingest"]
        ["post"]["requestBody"]["content"]["application/json"]["schema"]["properties"];
    assert!(ingest_properties["ack_mode"].is_null());
    let relation_batch_path = &openapi_response.1["paths"]["/v1/relations/ingest"];
    assert!(relation_batch_path.is_object());
    assert_eq!(
        relation_batch_path["post"]["requestBody"]["content"]["application/json"]["schema"]
            ["properties"]["batches"]["minItems"],
        1
    );

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "optimized_scores_by_user".to_string(),
            url_path: Some("/scores/optimized".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("optimized score totals".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED);

    let first_ingest = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "optimized-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
        }),
    )
    .await;
    assert_eq!(first_ingest.0, StatusCode::CREATED, "{first_ingest:?}");
    assert_eq!(first_ingest.1["ack_mode"], "materialized");
    assert_eq!(
        first_ingest.1["ingest_epoch"],
        first_ingest.1["epoch_manifest_id"]
    );
    assert_eq!(first_ingest.1["materialized_through"], 1);
    assert_eq!(first_ingest.1["materialization"]["status"], "completed");
    assert_eq!(first_ingest.1["materialization"]["active_views"], 1);
    assert_eq!(first_ingest.1["materialization"]["applied_batches"], 1);
    assert_eq!(first_ingest.1["materialization"]["checkpoint_writes"], 1);
    assert_eq!(
        first_ingest.1["materialization"]["applied_batches_per_checkpoint_write"],
        1
    );
    assert_eq!(first_ingest.1["materialization"]["output_delta_writes"], 1);
    assert_eq!(first_ingest.1["materialization"]["state_payload_writes"], 1);
    assert_eq!(
        first_ingest.1["materialization"]["checkpoint_record_writes"],
        1
    );
    assert_eq!(
        first_ingest.1["materialization"]["checkpoint_pointer_writes"],
        0
    );
    assert_eq!(first_ingest.1["materialization"]["latest_cache_writes"], 1);
    assert_eq!(
        first_ingest.1["materialization"]["checkpoint_publication_writes"],
        1
    );
    assert!(first_ingest.1["materialization"]
        .get("compaction_scheduled")
        .is_none());
    assert_eq!(first_ingest.1["timings"]["batch_count"], 1);
    assert_eq!(first_ingest.1["timings"]["row_count"], 1);
    assert!(first_ingest.1["timings"]["avg_batch_us"].as_u64().is_some());
    assert!(first_ingest.1["timings"]["avg_row_us"].as_u64().is_some());
    assert!(first_ingest.1["timings"]["rows_per_second"]
        .as_u64()
        .is_some());
    assert!(first_ingest.1["timings"]["total_us"].as_u64().unwrap() > 0);
    assert!(first_ingest.1["timings"].get("stages").is_none());

    let epoch_ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "optimized-scores-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 1,
                    "rows": [{"user_id": "alice", "score": 1, "delta": 1}]
                },
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "optimized-scores-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 2,
                    "rows": [{"user_id": "bob", "score": 2, "delta": 1}]
                }
            ]
        }),
    )
    .await;
    assert_eq!(epoch_ingest.0, StatusCode::CREATED, "{epoch_ingest:?}");
    assert_eq!(epoch_ingest.1["ack_mode"], "materialized");
    assert_eq!(
        epoch_ingest.1["ingest_epoch"],
        epoch_ingest.1["epoch_manifest_id"]
    );
    assert_eq!(epoch_ingest.1["materialized_through"], 3);
    assert_eq!(epoch_ingest.1["materialization"]["status"], "completed");
    assert_eq!(epoch_ingest.1["materialization"]["applied_batches"], 2);
    assert_eq!(epoch_ingest.1["materialization"]["checkpoint_writes"], 1);
    assert_eq!(
        epoch_ingest.1["materialization"]["applied_batches_per_checkpoint_write"],
        2
    );
    assert_eq!(epoch_ingest.1["materialization"]["output_delta_writes"], 1);
    assert_eq!(epoch_ingest.1["materialization"]["state_payload_writes"], 1);
    assert_eq!(
        epoch_ingest.1["materialization"]["checkpoint_record_writes"],
        1
    );
    assert_eq!(
        epoch_ingest.1["materialization"]["checkpoint_pointer_writes"],
        0
    );
    assert_eq!(epoch_ingest.1["materialization"]["latest_cache_writes"], 1);
    assert_eq!(
        epoch_ingest.1["materialization"]["checkpoint_publication_writes"],
        1
    );
    assert!(epoch_ingest.1["materialization"]
        .get("compaction_scheduled")
        .is_none());
    assert_eq!(epoch_ingest.1["timings"]["batch_count"], 2);
    assert_eq!(epoch_ingest.1["timings"]["row_count"], 2);
    assert!(epoch_ingest.1["timings"]["avg_batch_us"].as_u64().is_some());
    assert!(epoch_ingest.1["timings"]["avg_row_us"].as_u64().is_some());
    assert!(epoch_ingest.1["timings"]["rows_per_second"]
        .as_u64()
        .is_some());
    assert!(epoch_ingest.1["timings"].get("stages").is_none());

    let rejected_ack_mode = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "ack_mode": "append_committed",
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "optimized-scores-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 3,
                    "rows": [{"user_id": "alice", "score": 3, "delta": 1}]
                },
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "optimized-scores-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 4,
                    "rows": [{"user_id": "bob", "score": 4, "delta": 1}]
                }
            ]
        }),
    )
    .await;
    assert!(
        rejected_ack_mode.0.is_client_error(),
        "{rejected_ack_mode:?}"
    );
    assert!(rejected_ack_mode.1["error"]
        .as_str()
        .unwrap()
        .contains("ack_mode"));
    assert!(rejected_ack_mode.1["error"]
        .as_str()
        .unwrap()
        .contains("unknown field"));

    let rejected_batch_ack_mode = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "optimized-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 3,
                "ack_mode": "append_committed",
                "rows": [{"user_id": "alice", "score": 3, "delta": 1}]
            }]
        }),
    )
    .await;
    assert!(
        rejected_batch_ack_mode.0.is_client_error(),
        "{rejected_batch_ack_mode:?}"
    );
    assert!(rejected_batch_ack_mode.1["error"]
        .as_str()
        .unwrap()
        .contains("ack_mode"));
}

#[tokio::test]
async fn rest_materialization_rejects_output_delta_over_runtime_budget() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-runtime-budget-owner", false)
        .await
        .with_standing_runtime_budget_limits(1, DEFAULT_MAX_STANDING_RUNTIME_STATE_PAYLOAD_BYTES);
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "budgeted_scores_by_user".to_string(),
            url_path: Some("/scores/budgeted".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("runtime output delta budget proof".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "budgeted-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "bob", "score": 20, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::PAYLOAD_TOO_LARGE,
        "{ingest_response:?}"
    );
    assert!(ingest_response.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("output delta record count 2 exceeds configured limit 1"));

    let query_response = call_json(
        &router,
        Method::GET,
        "/v1/views/budgeted_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(
        query_response.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "{query_response:?}"
    );
    assert!(query_response.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("not fully materialized"));
}

#[tokio::test]
async fn rest_epoch_ingest_appends_batches_concurrently() {
    let counts = ObjectStoreAccessCounts::default();
    let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::with_ingest_put_delay(
        Arc::new(InMemory::new()),
        counts.clone(),
        Duration::from_millis(25),
    ));
    let state =
        test_api_state_with_store(store, "api-test-epoch-append-parallel-owner", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    counts.clear();
    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "parallel-append-a",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [{"user_id": "alice", "score": 1, "delta": 1}]
                },
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "parallel-append-b",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [{"user_id": "bob", "score": 2, "delta": 1}]
                },
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "parallel-append-c",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [{"user_id": "carol", "score": 3, "delta": 1}]
                },
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "parallel-append-d",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [{"user_id": "dave", "score": 4, "delta": 1}]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["outcome"], "appended");
    assert_eq!(ingest.1["timings"]["batch_count"], 4);
    assert!(ingest.1["timings"].get("stages").is_none());
    assert!(
        counts.max_concurrent_ingest_puts() > 1,
        "epoch append should overlap ingest object puts; max concurrency was {}",
        counts.max_concurrent_ingest_puts()
    );
}

#[tokio::test]
async fn rest_sum_arithmetic_expression_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-sum-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "adjusted_scores_by_user".to_string(),
            url_path: Some("/scores/adjusted".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score + 1) as adjusted_sum, count(*) as count from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("adjusted score totals".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "sum-expression-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "alice", "score": 7, "delta": 1},
                    {"user_id": "bob", "score": 5, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/adjusted_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    let rows = rows
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["adjusted_sum"].as_i64().unwrap(),
                    row["count"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(19, 2)));
    assert_eq!(rows.get("bob"), Some(&(6, 1)));
}

#[tokio::test]
async fn rest_cast_int64_aggregate_expression_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-cast-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "cast_scores_by_user".to_string(),
            url_path: Some("/scores/cast".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(cast(score as bigint)) as sum, count(*) as count from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("cast score totals".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "cast-expression-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": 7, "delta": 1},
                {"user_id": "bob", "score": 5, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/cast_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    let rows = rows
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (row["sum"].as_i64().unwrap(), row["count"].as_i64().unwrap()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(17, 2)));
    assert_eq!(rows.get("bob"), Some(&(5, 1)));
}

#[tokio::test]
async fn rest_nested_double_colon_cast_int64_aggregate_expression_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-double-colon-cast-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "double_colon_cast_scores_by_user".to_string(),
            url_path: Some("/scores/double-colon-cast".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum((score + 1)::bigint) as adjusted_sum, count(*) as count from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("double-colon cast adjusted score totals".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "double-colon-cast-expression-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": 7, "delta": 1},
                {"user_id": "bob", "score": 5, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/double_colon_cast_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    let rows = rows
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["adjusted_sum"].as_i64().unwrap(),
                    row["count"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(19, 2)));
    assert_eq!(rows.get("bob"), Some(&(6, 1)));
}

#[tokio::test]
async fn rest_try_and_safe_cast_int64_aggregate_expression_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-try-safe-cast-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "try_safe_cast_scores_by_user".to_string(),
            url_path: Some("/scores/try-safe-cast".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(try_cast(score as bigint)) as try_sum, sum(safe_cast(score as int64)) as safe_sum from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("try and safe cast score totals".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "try-safe-cast-expression-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": 7, "delta": 1},
                {"user_id": "bob", "score": 5, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/try_safe_cast_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    let rows = rows
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["try_sum"].as_i64().unwrap(),
                    row["safe_sum"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(17, 17)));
    assert_eq!(rows.get("bob"), Some(&(5, 5)));
}

#[tokio::test]
async fn rest_abs_int64_aggregate_expression_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-abs-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "absolute_scores_by_user".to_string(),
            url_path: Some("/scores/absolute".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(abs(score)) as absolute_sum, count(*) as count from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("absolute score totals".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "abs-expression-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": -10, "delta": 1},
                    {"user_id": "alice", "score": 7, "delta": 1},
                    {"user_id": "bob", "score": -5, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/absolute_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    let rows = rows
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["absolute_sum"].as_i64().unwrap(),
                    row["count"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(17, 2)));
    assert_eq!(rows.get("bob"), Some(&(5, 1)));
}

#[tokio::test]
async fn rest_greatest_least_int64_aggregate_expression_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-greatest-least-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "bounded_scores_by_user".to_string(),
            url_path: Some("/scores/bounded".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(greatest(score, 0)) as positive_floor_sum, sum(least(score, 10)) as capped_sum, count(*) as count from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("bounded score totals".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "greatest-least-expression-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": -10, "delta": 1},
                    {"user_id": "alice", "score": 17, "delta": 1},
                    {"user_id": "bob", "score": 5, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/bounded_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    let rows = rows
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["positive_floor_sum"].as_i64().unwrap(),
                    row["capped_sum"].as_i64().unwrap(),
                    row["count"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(17, 0, 2)));
    assert_eq!(rows.get("bob"), Some(&(5, 5, 1)));
}

#[tokio::test]
async fn rest_coalesce_nullable_int64_aggregate_expression_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-coalesce-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog_with_nullable_score(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "coalesced_scores_by_user".to_string(),
            url_path: Some("/scores/coalesced".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(coalesce(score, 0)) as coalesced_sum, count(*) as count from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("coalesced nullable score totals".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "coalesce-expression-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "alice", "score": null, "delta": 1},
                    {"user_id": "alice", "score": 7, "delta": 1},
                    {"user_id": "bob", "score": null, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/coalesced_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    let rows = rows
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["coalesced_sum"].as_i64().unwrap(),
                    row["count"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(17, 3)));
    assert_eq!(rows.get("bob"), Some(&(0, 1)));
}

#[tokio::test]
async fn rest_is_not_distinct_from_null_predicate_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-is-not-distinct-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog_with_nullable_score(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "null_scores_by_user".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(coalesce(score, 0)) as sum, count(*) as count from scores where score is not distinct from null group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("null-safe score aggregate".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "is-not-distinct-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": null, "delta": 1},
                {"user_id": "bob", "score": null, "delta": 1},
                {"user_id": "carol", "score": 3, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/null_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (row["sum"].as_i64().unwrap(), row["count"].as_i64().unwrap()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(0, 1)));
    assert_eq!(rows.get("bob"), Some(&(0, 1)));
    assert_eq!(rows.get("carol"), None);
}

#[tokio::test]
async fn rest_case_when_distinct_from_null_predicate_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-case-null-safe-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog_with_nullable_score(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "case_null_safe_scores_by_user".to_string(),
            url_path: Some("/scores/case-null-safe".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(case when score is distinct from null then coalesce(score, 0) else 0 end) as present_sum, sum(case when score is not distinct from null then 1 else 0 end) as null_count from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("CASE null-safe score totals".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "case-null-safe-expression-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": null, "delta": 1},
                {"user_id": "alice", "score": 7, "delta": 1},
                {"user_id": "bob", "score": null, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/case_null_safe_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["present_sum"].as_i64().unwrap(),
                    row["null_count"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(17, 1)));
    assert_eq!(rows.get("bob"), Some(&(0, 1)));
}

#[tokio::test]
async fn rest_case_when_is_null_predicate_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-case-null-predicate-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog_with_nullable_score(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "case_null_predicate_scores_by_user".to_string(),
            url_path: Some("/scores/case-null-predicate".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(case when score is null then 1 else 0 end) as null_count, sum(case when score is not null then coalesce(score, 0) else 0 end) as present_sum from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("CASE nullable score predicate totals".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "case-null-predicate-expression-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": null, "delta": 1},
                {"user_id": "alice", "score": 7, "delta": 1},
                {"user_id": "bob", "score": null, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/case_null_predicate_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["null_count"].as_i64().unwrap(),
                    row["present_sum"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(1, 17)));
    assert_eq!(rows.get("bob"), Some(&(1, 0)));
}

#[tokio::test]
async fn rest_case_when_int64_aggregate_expression_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-case-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "positive_score_sum_by_user".to_string(),
            url_path: Some("/scores/positive-sum".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(case when score > 0 then score else 0 end) as positive_sum, count(*) as count from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("positive score totals through CASE".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "case-expression-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "alice", "score": -7, "delta": 1},
                    {"user_id": "bob", "score": -5, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/positive_score_sum_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    let rows = rows
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["positive_sum"].as_i64().unwrap(),
                    row["count"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(10, 2)));
    assert_eq!(rows.get("bob"), Some(&(0, 1)));
}

#[tokio::test]
async fn rest_case_when_between_and_in_predicate_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-case-predicate-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "case_predicate_scores_by_user".to_string(),
            url_path: Some("/scores/case-predicate-sum".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(case when score between 1 and 10 then score else 0 end) as bounded_sum, sum(case when score in (5, 7) then score else 0 end) as selected_sum, count(*) as count from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("score totals through CASE BETWEEN and IN predicates".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "case-predicate-expression-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": 7, "delta": 1},
                {"user_id": "bob", "score": 5, "delta": 1},
                {"user_id": "carol", "score": 12, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/case_predicate_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["bounded_sum"].as_i64().unwrap(),
                    row["selected_sum"].as_i64().unwrap(),
                    row["count"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(17, 7, 2)));
    assert_eq!(rows.get("bob"), Some(&(5, 5, 1)));
    assert_eq!(rows.get("carol"), Some(&(0, 0, 1)));
}

#[tokio::test]
async fn rest_if_int64_aggregate_expression_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-if-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "positive_if_score_sum_by_user".to_string(),
            url_path: Some("/scores/positive-if-sum".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(if(score > 0, score, 0)) as positive_sum, count(*) as count from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("positive score totals through IF".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "if-expression-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "alice", "score": -7, "delta": 1},
                    {"user_id": "bob", "score": -5, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/positive_if_score_sum_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    let rows = rows
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["positive_sum"].as_i64().unwrap(),
                    row["count"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(10, 2)));
    assert_eq!(rows.get("bob"), Some(&(0, 1)));
}

#[tokio::test]
async fn rest_multi_branch_case_when_int64_aggregate_expression_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-multi-case-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "capped_positive_score_sum_by_user".to_string(),
            url_path: Some("/scores/capped-positive-sum".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(case when score > 10 then 10 when score > 0 then score else 0 end) as capped_sum, count(*) as count from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("capped positive score totals through CASE".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "multi-case-expression-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 20, "delta": 1},
                    {"user_id": "alice", "score": 7, "delta": 1},
                    {"user_id": "bob", "score": -5, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/capped_positive_score_sum_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    let rows = rows
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["capped_sum"].as_i64().unwrap(),
                    row["count"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(17, 2)));
    assert_eq!(rows.get("bob"), Some(&(0, 1)));
}

#[tokio::test]
async fn rest_simple_case_when_int64_aggregate_expression_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-simple-case-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "bucketed_score_sum_by_user".to_string(),
            url_path: Some("/scores/bucketed-sum".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(case score when 1 then 10 when 2 then 20 else 0 end) as bucket_sum, count(*) as count from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("bucketed score totals through simple CASE".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "simple-case-expression-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 1, "delta": 1},
                    {"user_id": "alice", "score": 2, "delta": 1},
                    {"user_id": "bob", "score": 9, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/bucketed_score_sum_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    let rows = rows
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["bucket_sum"].as_i64().unwrap(),
                    row["count"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(30, 2)));
    assert_eq!(rows.get("bob"), Some(&(0, 1)));
}

#[tokio::test]
async fn rest_min_max_arithmetic_expression_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-min-max-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "score_extrema_by_user".to_string(),
            url_path: Some("/scores/extrema".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, min(score + 1) as smallest, max(score + 1) as largest from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("adjusted score extrema".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "min-max-expression-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "alice", "score": 7, "delta": 1},
                    {"user_id": "bob", "score": 5, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/score_extrema_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    let rows = rows
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                (
                    row["smallest"].as_i64().unwrap(),
                    row["largest"].as_i64().unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&(8, 11)));
    assert_eq!(rows.get("bob"), Some(&(6, 6)));
}

#[tokio::test]
async fn rest_avg_arithmetic_expression_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-avg-expression-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
        view_id: "average_adjusted_scores_by_user".to_string(),
        url_path: Some("/scores/average-adjusted".to_string()),
        output_relation_id: None,
        input_relation_id: "scores".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: "select user_id, avg(score + 1) as average from scores group by user_id".to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("adjusted score average".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "avg-expression-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "alice", "score": 7, "delta": 1},
                    {"user_id": "bob", "score": 5, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/average_adjusted_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    let rows = rows
        .iter()
        .map(|row| {
            (
                row["user_id"].as_str().unwrap().to_string(),
                row["average"].as_f64().unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows.get("alice"), Some(&9.5));
    assert_eq!(rows.get("bob"), Some(&6.0));
}

#[tokio::test]
async fn rest_filter_project_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-filter-project-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
        view_id: "positive_scores".to_string(),
        url_path: Some("/scores/positive-project".to_string()),
        output_relation_id: None,
        input_relation_id: "scores".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: "select user_id, score from scores where score > 0".to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("positive score projection".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "filter-project-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "bob", "score": -3, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/positive_scores/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(query.1["rows"], json!([{"user_id": "alice", "score": 10}]));
}

#[tokio::test]
async fn rest_filter_project_union_distinct_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-filter-project-union-distinct-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "positive_scores_union_distinct".to_string(),
            url_path: Some("/scores/positive-union-distinct".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, score from scores where score > 0 union distinct select user_id, score from scores where score >= 10".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("positive score projection through union distinct".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "filter-project-union-distinct-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "bob", "score": -3, "delta": 1},
                {"user_id": "carol", "score": 7, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/positive_scores_union_distinct/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([
            {"user_id": "alice", "score": 10},
            {"user_id": "carol", "score": 7}
        ])
    );
}

#[tokio::test]
async fn rest_filter_project_order_by_limit_view_materializes_top_k_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-filter-project-top-k-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
        view_id: "positive_scores_top_k".to_string(),
        url_path: Some("/scores/positive-top-k".to_string()),
        output_relation_id: None,
        input_relation_id: "scores".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: "select user_id, score from scores where score > 0 order by score desc limit 2"
            .to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("positive score top-k projection".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "filter-project-top-k-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "bob", "score": 8, "delta": 1},
                {"user_id": "carol", "score": 6, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/positive_scores_top_k/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([
            {"user_id": "alice", "score": 10},
            {"user_id": "bob", "score": 8}
        ])
    );

    let delete_top_row = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "filter-project-top-k-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 3,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": -1}
            ]
        }),
    )
    .await;
    assert_eq!(delete_top_row.0, StatusCode::CREATED, "{delete_top_row:?}");
    assert_eq!(delete_top_row.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/positive_scores_top_k/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([
            {"user_id": "bob", "score": 8},
            {"user_id": "carol", "score": 6}
        ])
    );
}

#[tokio::test]
async fn rest_filter_project_case_over_bool_predicate_materializes_relation_scoped_ingest() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-filter-project-bool-case-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_device_status_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "device_enabled_flags".to_string(),
            url_path: Some("/devices/enabled-flags".to_string()),
            output_relation_id: None,
            input_relation_id: "device_status".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select device_id, case when enabled = true then 1 else 0 end as enabled_flag from device_status".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("device enabled flags".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/device_status/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "device-status-bool-case-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"device_id": "device-a", "enabled": true, "event_time": 100, "delta": 1},
                {"device_id": "device-b", "enabled": false, "event_time": 101, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/device_enabled_flags/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([
            {"device_id": "device-a", "enabled_flag": 1},
            {"device_id": "device-b", "enabled_flag": 0}
        ])
    );
}

#[tokio::test]
async fn rest_filter_project_cte_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-filter-project-cte-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "positive_scores_cte".to_string(),
            url_path: Some("/scores/positive-project-cte".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "with score_source as (select * from scores where score > 0) select user_id, score from score_source where user_id <> 'bob'".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("positive score projection through CTE source".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "filter-project-cte-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "bob", "score": 8, "delta": 1},
                    {"user_id": "carol", "score": -2, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/positive_scores_cte/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(query.1["rows"], json!([{"user_id": "alice", "score": 10}]));
}

#[tokio::test]
async fn rest_filter_project_derived_table_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-filter-project-derived-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "positive_scores_derived".to_string(),
            url_path: Some("/scores/positive-project-derived".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select s.user_id, s.score from (select * from scores where score > 0) s where s.user_id <> 'bob'".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("positive score projection through derived table source".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "filter-project-derived-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "bob", "score": 8, "delta": 1},
                    {"user_id": "carol", "score": -2, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/positive_scores_derived/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(query.1["rows"], json!([{"user_id": "alice", "score": 10}]));
}

#[tokio::test]
async fn rest_filter_project_view_materializes_nullable_value_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-filter-project-nullable-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog_with_nullable_score(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
        view_id: "nullable_scores_project".to_string(),
        url_path: Some("/scores/nullable-project".to_string()),
        output_relation_id: None,
        input_relation_id: "scores".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: "select user_id, score from scores where user_id is not null".to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("nullable score projection".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "filter-project-nullable-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "bob", "score": null, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/nullable_scores_project/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([
            {"user_id": "alice", "score": 10},
            {"user_id": "bob", "score": null}
        ])
    );
}

#[tokio::test]
async fn rest_computed_filter_project_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-computed-filter-project-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "positive_score_normalized".to_string(),
            url_path: Some("/scores/positive-normalized".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, -score + score / 2 + score % 3 as normalized_score from scores where score > 0"
                .to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("positive score normalized value".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "computed-filter-project-scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "bob", "score": -3, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/positive_score_normalized/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([{"user_id": "alice", "normalized_score": -4}])
    );
}

#[tokio::test]
async fn rest_aggregate_having_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-having-owner", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "scores_having_by_user".to_string(),
            url_path: Some("/scores/having".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select s.user_id as user, sum(s.score) as sum, count(1) as count from scores as s where s.user_id like 'a%' and s.user_id not like 'admin_%' and s.score in (5, 7, 100) and s.score not in (100) group by s.user_id having sum(s.score) in (12) and count(1) not in (0, 1)".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("score totals above threshold".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-having-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 7, "delta": 1},
                {"user_id": "alice", "score": 5, "delta": 1},
                {"user_id": "bob", "score": 3, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_having_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([{"user": "alice", "sum": 12, "count": 2}])
    );
}

#[tokio::test]
async fn rest_count_distinct_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-count-distinct-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
        view_id: "distinct_scores_by_user".to_string(),
        url_path: Some("/scores/distinct".to_string()),
        output_relation_id: None,
        input_relation_id: "scores".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: "select user_id, count(distinct score) as count from scores group by user_id"
            .to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("distinct score count by user".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-count-distinct-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 7, "delta": 1},
                    {"user_id": "alice", "score": 7, "delta": 1},
                    {"user_id": "alice", "score": 5, "delta": 1},
                    {"user_id": "bob", "score": 3, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/distinct_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([
            {"user_id": "alice", "count": 2},
            {"user_id": "bob", "count": 1}
        ])
    );
}

#[tokio::test]
async fn rest_having_count_distinct_function_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-having-count-distinct-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "scores_distinct_having_by_user".to_string(),
            url_path: Some("/scores/distinct-having".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(distinct score) as distinct_scores from scores group by user_id having count(distinct score) > 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("distinct score count HAVING by user".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-having-count-distinct-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": 7, "delta": 1},
                {"user_id": "bob", "score": 5, "delta": 1},
                {"user_id": "bob", "score": 5, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_distinct_having_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([{"user_id": "alice", "sum": 27, "distinct_scores": 2}])
    );
}

#[tokio::test]
async fn rest_filtered_count_distinct_mixed_filter_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-filtered-count-distinct-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "filtered_distinct_scores_by_user".to_string(),
            url_path: Some("/scores/filtered-distinct".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) filter (where score > 5) as sum, count(distinct score) filter (where score > 0) as count from scores group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("filtered distinct score count by user".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-filtered-count-distinct-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "alice", "score": 10, "delta": 1},
                    {"user_id": "alice", "score": 7, "delta": 1},
                    {"user_id": "bob", "score": 5, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/filtered_distinct_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([
            {"user_id": "alice", "sum": 27, "count": 2},
            {"user_id": "bob", "sum": 0, "count": 1}
        ])
    );
}

#[tokio::test]
async fn rest_order_by_limit_top_k_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-top-k-owner", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "top_purchaser".to_string(),
            url_path: Some("/purchases/top".to_string()),
            output_relation_id: None,
            input_relation_id: "purchases".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id order by sum desc limit 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("top purchaser by materialized sum".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let first_ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "purchases",
                "relation_version": "2026-05-24.v1",
                "stream_id": "top-purchaser-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "amount": 10, "delta": 1},
                    {"user_id": "alice", "amount": 7, "delta": 1},
                    {"user_id": "bob", "amount": 5, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(first_ingest.0, StatusCode::CREATED, "{first_ingest:?}");
    let first_query = call_json(
        &router,
        Method::POST,
        "/v1/views/top_purchaser/query",
        json!({}),
    )
    .await;
    assert_eq!(first_query.0, StatusCode::OK, "{first_query:?}");
    assert_eq!(
        first_query.1["rows"],
        json!([{"user_id": "alice", "sum": 17, "count": 2}])
    );

    let second_ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "purchases",
                "relation_version": "2026-05-24.v1",
                "stream_id": "top-purchaser-stream",
                "partition_id": 0,
                "start_offset_inclusive": 3,
                "rows": [{"user_id": "bob", "amount": 20, "delta": 1}]
            }]
        }),
    )
    .await;
    assert_eq!(second_ingest.0, StatusCode::CREATED, "{second_ingest:?}");
    let second_query = call_json(&router, Method::GET, "/v1/api/purchases/top", Value::Null).await;
    assert_eq!(second_query.0, StatusCode::OK, "{second_query:?}");
    assert_eq!(
        second_query.1["rows"],
        json!([{"user_id": "bob", "sum": 25, "count": 2}])
    );
}

#[tokio::test]
async fn rest_order_by_limit_offset_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-aggregate-limit-offset",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "second_purchaser".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "purchases".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id order by sum desc limit 1 offset 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("second purchaser by materialized sum".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "purchases",
                "relation_version": "2026-05-24.v1",
                "stream_id": "second-purchaser-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "amount": 10, "delta": 1},
                    {"user_id": "alice", "amount": 7, "delta": 1},
                    {"user_id": "bob", "amount": 5, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/second_purchaser/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([{"user_id": "bob", "sum": 5, "count": 1}])
    );
}

#[tokio::test]
async fn rest_order_by_function_top_k_view_materializes_relation_scoped_ingest() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-order-by-function-top-k",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "top_scores_by_user".to_string(),
            url_path: Some("/scores/top-user".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as total_score, count(*) as event_count from scores group by user_id order by sum(score) desc limit 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("top score user by materialized sum".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "top-score-users-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": 7, "delta": 1},
                {"user_id": "bob", "score": 5, "delta": 1},
                {"user_id": "carol", "score": 20, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/top_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([{"user_id": "carol", "total_score": 20, "event_count": 1}])
    );
}

#[tokio::test]
async fn rest_concurrent_ingest_replays_from_fresh_checkpoint_per_view() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-concurrent-ingest-owner", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = default_positive_scores_view_request(&test_scores_catalog()).unwrap();
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(
        view_response.0,
        StatusCode::CREATED,
        "view creation response: {}",
        view_response.1
    );

    let mut tasks = tokio::task::JoinSet::new();
    for stream_index in 0..8 {
        let app = router.clone();
        tasks.spawn(async move {
            call_json(
                &app,
                Method::POST,
                "/v1/ingest",
                json!({
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": format!("concurrent-scores-{stream_index}"),
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {
                            "user_id": format!("concurrent-user-{stream_index}"),
                            "score": stream_index + 1,
                            "delta": 1
                        }
                    ]
                }),
            )
            .await
        });
    }

    while let Some(result) = tasks.join_next().await {
        let (status, body) = result.unwrap();
        assert_eq!(status, StatusCode::CREATED, "ingest response: {body}");
        assert_eq!(body["outcome"], "appended");
    }

    let query_response = call_json(
        &router,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(
        query_response.0,
        StatusCode::OK,
        "query response: {}",
        query_response.1
    );
    let rows = query_response.1["rows"].as_array().unwrap();
    for stream_index in 0..8 {
        let user_id = format!("concurrent-user-{stream_index}");
        assert!(
            rows.iter().any(|row| {
                row["user_id"] == user_id
                    && row["sum"].as_i64() == Some(i64::from(stream_index + 1))
                    && row["count"].as_i64() == Some(1)
            }),
            "missing materialized row for {user_id}: {}",
            query_response.1
        );
    }
}

async fn append_admitted_ingest_without_runtime_apply(
    store: Arc<dyn ObjectStore>,
    request: IngestRowsRequest,
) {
    let state = test_api_state_with_store(store, "api-test-crash-window-writer", false).await;
    let prepared = prepare_ingest_batch(&state, request, None).await.unwrap();
    let outcome = append_ingest_envelope(&state, prepared.envelope)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        AppendValidatedEnvelopeOutcome::Appended { .. }
    ));
}

#[tokio::test]
async fn rest_late_view_query_reports_materialization_lag_without_blocking_later_ingest() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-late-view-owner", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let first_ingest = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": 3, "delta": 1},
                {"user_id": "bob", "score": 4, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(first_ingest.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "late_scores_by_user".to_string(),
            url_path: Some("/scores/late-by-user".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("late-created score totals".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(
        view_response.0,
        StatusCode::CREATED,
        "late view creation response: {}",
        view_response.1
    );
    assert_eq!(view_response.1["query_enabled"], false);
    assert_eq!(
        view_response.1["lifecycle"]["deployment_status"],
        "deploying"
    );
    assert!(
        view_response.1["lifecycle"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("backfill_required"),
        "late view lifecycle: {}",
        view_response.1
    );

    let second_ingest = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 3,
            "rows": [
                {"user_id": "alice", "score": 2, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(
        second_ingest.0,
        StatusCode::CREATED,
        "late view must not block later ingest: {}",
        second_ingest.1
    );

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/late_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(
        query_response.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "late view query response: {}",
        query_response.1
    );
    assert!(query_response.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("MATERIALIZATION_LAG"));

    let refreshed_view = call_json(
        &router,
        Method::GET,
        "/v1/views/late_scores_by_user",
        json!({}),
    )
    .await;
    assert_eq!(refreshed_view.0, StatusCode::OK);
    assert_eq!(refreshed_view.1["query_enabled"], false);
    assert_eq!(
        refreshed_view.1["lifecycle"]["deployment_status"],
        "deploying"
    );
}

#[tokio::test]
async fn rest_late_view_backfill_api_reports_coverage_and_runs_limited_steps() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-backfill-api-owner", false)
        .await
        .with_experimental_advanced_view_features(true);
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    for (start, rows) in [
        (
            0,
            json!([
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "bob", "score": 4, "delta": 1}
            ]),
        ),
        (
            2,
            json!([
                {"user_id": "alice", "score": 5, "delta": 1}
            ]),
        ),
    ] {
        let ingest = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": start,
                "rows": rows
            }),
        )
        .await;
        assert_eq!(
            ingest.0,
            StatusCode::CREATED,
            "ingest response: {}",
            ingest.1
        );
    }

    let view_request = CreateViewRequest {
            view_id: "late_scores_backfill_api".to_string(),
            url_path: Some("/scores/backfill-api".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("late-created score totals with explicit backfill".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED);
    assert_eq!(view_response.1["coverage"]["state"], "backfill_required");
    assert_eq!(
        view_response.1["coverage"]["full_view"]["status"],
        "available"
    );
    assert!(view_response.1["coverage"].get("request_scope").is_none());
    assert!(view_response.1["coverage"].get("range").is_none());
    assert!(view_response.1["coverage"]
        .get("background_backfill")
        .is_none());

    let status = call_json(
        &router,
        Method::GET,
        "/v1/views/late_scores_backfill_api/backfill",
        json!({}),
    )
    .await;
    assert_eq!(status.0, StatusCode::OK);
    assert_eq!(status.1["coverage"]["state"], "backfill_required");
    assert_eq!(status.1["progress"]["processed_batches"], 0);
    assert_eq!(status.1["progress"]["remaining_batches"], 2);
    assert_eq!(status.1["progress"]["total_batches"], 2);
    assert_eq!(status.1["progress"]["percent"], 0.0);

    let first_step = call_json(
        &router,
        Method::POST,
        "/v1/views/late_scores_backfill_api/backfill",
        json!({"batch_limit": 1}),
    )
    .await;
    assert_eq!(
        first_step.0,
        StatusCode::OK,
        "first backfill response: {}",
        first_step.1
    );
    assert_eq!(first_step.1["applied_batches"], 1);
    assert_eq!(first_step.1["remaining_batches"], 1);
    assert_eq!(first_step.1["query_enabled"], false);
    assert_eq!(first_step.1["progress"]["processed_batches"], 1);
    assert_eq!(first_step.1["progress"]["remaining_batches"], 1);
    assert_eq!(first_step.1["progress"]["total_batches"], 2);
    assert_eq!(first_step.1["progress"]["percent"], 50.0);

    let finish = call_json(
        &router,
        Method::POST,
        "/v1/views/late_scores_backfill_api/backfill",
        json!({}),
    )
    .await;
    assert_eq!(finish.0, StatusCode::OK, "finish response: {}", finish.1);
    assert_eq!(finish.1["remaining_batches"], 0);
    assert_eq!(finish.1["query_enabled"], true);
    assert_eq!(finish.1["progress"]["processed_batches"], 2);
    assert_eq!(finish.1["progress"]["remaining_batches"], 0);
    assert_eq!(finish.1["progress"]["total_batches"], 2);
    assert_eq!(finish.1["progress"]["percent"], 100.0);

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/late_scores_backfill_api/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK);
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"user_id": "alice", "sum": 15, "count": 2},
            {"user_id": "bob", "sum": 4, "count": 1}
        ])
    );
}

#[tokio::test]
async fn rest_late_single_relation_stats_having_top_k_backfill_api_runs_limited_steps() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(
        store,
        "api-test-late-stats-having-top-k-backfill-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    for (start, rows) in [
        (
            0,
            json!([
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "bob", "score": 12, "delta": 1}
            ]),
        ),
        (
            2,
            json!([
                {"user_id": "alice", "score": 8, "delta": 1}
            ]),
        ),
    ] {
        let ingest = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-stats-having-top-k-stream",
                "partition_id": 0,
                "start_offset_inclusive": start,
                "rows": rows
            }),
        )
        .await;
        assert_eq!(
            ingest.0,
            StatusCode::CREATED,
            "ingest response: {}",
            ingest.1
        );
    }

    let view_request = CreateViewRequest {
            view_id: "late_score_stats_having_top_k".to_string(),
            url_path: Some("/scores/stats-having-top-k".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, min(score) as min_score, max(score) as max_score, avg(score) as avg_score, count(*) as count from scores group by user_id having count(*) > 1 order by avg_score desc limit 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("late-created score stats with HAVING and top-k".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(
        view_response.0,
        StatusCode::CREATED,
        "view creation response: {}",
        view_response.1
    );
    assert_eq!(view_response.1["coverage"]["state"], "backfill_required");
    assert_eq!(view_response.1["query_enabled"], false);

    let first_step = call_json(
        &router,
        Method::POST,
        "/v1/views/late_score_stats_having_top_k/backfill",
        json!({"batch_limit": 1}),
    )
    .await;
    assert_eq!(
        first_step.0,
        StatusCode::OK,
        "first backfill response: {}",
        first_step.1
    );
    assert_eq!(first_step.1["applied_batches"], 1);
    assert_eq!(first_step.1["remaining_batches"], 1);
    assert_eq!(first_step.1["query_enabled"], false);

    let finish = call_json(
        &router,
        Method::POST,
        "/v1/views/late_score_stats_having_top_k/backfill",
        json!({"batch_limit": 1}),
    )
    .await;
    assert_eq!(
        finish.0,
        StatusCode::OK,
        "finish backfill response: {}",
        finish.1
    );
    assert_eq!(finish.1["applied_batches"], 1);
    assert_eq!(finish.1["remaining_batches"], 0);
    assert_eq!(finish.1["query_enabled"], true);

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/late_score_stats_having_top_k/query",
        json!({}),
    )
    .await;
    assert_eq!(
        query_response.0,
        StatusCode::OK,
        "query response: {}",
        query_response.1
    );
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"user_id": "alice", "min_score": 8, "max_score": 10, "avg_score": 9.0, "count": 2}
        ])
    );
}

#[tokio::test]
async fn rest_late_view_backfill_rejects_offset_range_scope() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-range-backfill-owner", false)
        .await
        .with_experimental_advanced_view_features(true);
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    for (start, rows) in [
        (
            0,
            json!([
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "bob", "score": 4, "delta": 1}
            ]),
        ),
        (
            2,
            json!([
                {"user_id": "alice", "score": 2, "delta": 1}
            ]),
        ),
    ] {
        let ingest_response = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": start,
                "rows": rows
            }),
        )
        .await;
        assert_eq!(ingest_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "late_scores_range_backfill".to_string(),
            url_path: Some("/scores/range-backfill".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("late-created score totals with range backfill".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED);
    assert_eq!(view_response.1["query_enabled"], false);

    let range_backfill = call_json(
        &router,
        Method::POST,
        "/v1/views/late_scores_range_backfill/backfill",
        json!({
            "mode": "sync",
            "range": {
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "end_offset_exclusive": 2
            }
        }),
    )
    .await;
    assert_eq!(
        range_backfill.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "range backfill response: {}",
        range_backfill.1
    );

    let full_backfill = call_json(
        &router,
        Method::POST,
        "/v1/views/late_scores_range_backfill/backfill",
        json!({}),
    )
    .await;
    assert_eq!(full_backfill.0, StatusCode::OK);
    assert_eq!(full_backfill.1["query_enabled"], true);

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/late_scores_range_backfill/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK);
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"user_id": "alice", "sum": 12, "count": 2},
            {"user_id": "bob", "sum": 4, "count": 1}
        ])
    );
}

#[tokio::test]
async fn rest_late_view_backfill_rejects_predicate_request_scope() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-predicate-backfill-owner", false)
        .await
        .with_experimental_advanced_view_features(true);
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    for (start, rows) in [
        (
            0,
            json!([
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "bob", "score": 4, "delta": 1}
            ]),
        ),
        (
            2,
            json!([
                {"user_id": "alice", "score": 2, "delta": 1}
            ]),
        ),
    ] {
        let ingest_response = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": start,
                "rows": rows
            }),
        )
        .await;
        assert_eq!(ingest_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "late_scores_predicate_backfill".to_string(),
            url_path: Some("/scores/predicate-backfill".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("late-created score totals with predicate backfill".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED);
    assert_eq!(view_response.1["query_enabled"], false);

    let scoped_backfill = call_json(
        &router,
        Method::POST,
        "/v1/views/late_scores_predicate_backfill/backfill",
        json!({
            "mode": "sync",
            "scope": {
                "where": "score > 9"
            }
        }),
    )
    .await;
    assert_eq!(
        scoped_backfill.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "predicate backfill response: {}",
        scoped_backfill.1
    );

    let full_backfill = call_json(
        &router,
        Method::POST,
        "/v1/views/late_scores_predicate_backfill/backfill",
        json!({}),
    )
    .await;
    assert_eq!(full_backfill.0, StatusCode::OK);
    assert_eq!(full_backfill.1["query_enabled"], true);

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/late_scores_predicate_backfill/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK);
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"user_id": "alice", "sum": 12, "count": 2},
            {"user_id": "bob", "sum": 4, "count": 1}
        ])
    );
}

#[tokio::test]
async fn rest_late_view_background_backfill_request_fails_closed() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-background-backfill-owner", false)
        .await
        .with_experimental_advanced_view_features(true);
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    for (start, rows) in [
        (
            0,
            json!([
                {"user_id": "alice", "score": 10, "delta": 1}
            ]),
        ),
        (
            1,
            json!([
                {"user_id": "bob", "score": 4, "delta": 1}
            ]),
        ),
    ] {
        let ingest = call_json(
            &router,
            Method::POST,
            "/v1/ingest",
            json!({
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": start,
                "rows": rows
            }),
        )
        .await;
        assert_eq!(ingest.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "late_scores_background".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: None,
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED);
    assert_eq!(view_response.1["query_enabled"], false);

    let scheduled = call_json(
        &router,
        Method::POST,
        "/v1/views/late_scores_background/backfill",
        json!({"mode": "background", "batch_limit": 1, "pause_ms": 1}),
    )
    .await;
    assert_eq!(
        scheduled.0,
        StatusCode::UNPROCESSABLE_ENTITY,
        "scheduled response: {}",
        scheduled.1
    );

    let full_backfill = call_json(
        &router,
        Method::POST,
        "/v1/views/late_scores_background/backfill",
        json!({}),
    )
    .await;
    assert_eq!(full_backfill.0, StatusCode::OK);
    let latest = call_json(
        &router,
        Method::GET,
        "/v1/views/late_scores_background",
        json!({}),
    )
    .await
    .1;
    assert_eq!(
        latest["query_enabled"], true,
        "view did not become queryable: {latest}"
    );
    assert_eq!(latest["coverage"]["state"], "materialized");
}

#[tokio::test]
async fn rest_view_query_fails_closed_without_published_checkpoint_output() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-no-manifest-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(
        relation_response.0,
        StatusCode::CREATED,
        "relation creation response: {}",
        relation_response.1
    );

    let view_request = CreateViewRequest {
            view_id: "scores_empty_until_ingest".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: None,
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(
        view_response.0,
        StatusCode::CREATED,
        "view creation response: {}",
        view_response.1
    );
    assert_eq!(view_response.1["query_enabled"], true);

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_empty_until_ingest/query",
        json!({}),
    )
    .await;
    assert_eq!(
        query_response.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "{query_response:?}"
    );
    assert!(query_response.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("MATERIALIZATION_LAG"));
}

#[tokio::test]
async fn rest_standing_runtime_output_query_without_sql_reads_materialized_rows_directly() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-output-direct-owner",
        false,
    )
    .await;
    let router = app(state);

    let policy_response = call_json(
        &router,
        Method::POST,
        "/v1/query-policies",
        json!({
            "query_policy_id": "no-sql-output-read",
            "policy": {
                "max_sql_bytes": 0,
                "planning_timeout_ms": 1000,
                "execution_timeout_ms": 1000,
                "max_output_rows": 100,
                "max_output_bytes": 1048576,
                "max_scan_files": 1,
                "max_scan_bytes": 1048576,
                "max_object_requests": 1,
                "max_concurrent_queries": 1,
                "memory_limit_bytes": 1048576,
                "spill_limit_bytes": 1048576
            }
        }),
    )
    .await;
    assert_eq!(policy_response.0, StatusCode::CREATED);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "scores_output_direct".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("direct output query should not plan generated SQL".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: Some("no-sql-output-read".to_string()),
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED);
    assert_eq!(view_response.1["query_enabled"], true);
    assert_eq!(view_response.1["execution_mode"], "standing_runtime");
    assert_eq!(
        view_response.1["output_relations"][0]["relation_id"],
        "scores_output_direct"
    );

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-output-direct-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "bob", "score": 5, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest_response.0, StatusCode::CREATED);

    let query_response = call_json(
        &router,
        Method::GET,
        "/v1/views/scores_output_direct/outputs/scores_output_direct/query",
        Value::Null,
    )
    .await;
    assert_eq!(
        query_response.0,
        StatusCode::OK,
        "output query response: {}",
        query_response.1
    );
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"user_id": "alice", "sum": 10, "count": 1},
            {"user_id": "bob", "sum": 5, "count": 1}
        ])
    );
}

#[tokio::test]
async fn rest_view_compaction_and_openapi_paths_are_available_after_materialization() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-compact-openapi-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "scores_compact_openapi".to_string(),
            url_path: Some("/scores/compact-openapi".to_string()),
            output_relation_id: None,
            input_relation_id: "scores".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("score totals for compaction and OpenAPI route smoke".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED);
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-compact-openapi-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": 15, "delta": 1},
                {"user_id": "bob", "score": 5, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest_response.0, StatusCode::CREATED);
    assert_eq!(ingest_response.1["outcome"], "appended");

    let query_response = call_json(
        &router,
        Method::GET,
        "/v1/api/scores/compact-openapi?max_rows=1000",
        Value::Null,
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK);
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"user_id": "alice", "sum": 25, "count": 2},
            {"user_id": "bob", "sum": 5, "count": 1}
        ])
    );

    let direct_sql_response = call_json(
            &router,
            Method::GET,
            "/v1/views/scores_compact_openapi/query?sql=select%20user_id%2C%20sum%20from%20scores_compact_openapi%20where%20sum%20%3E%2010%20order%20by%20user_id",
            Value::Null,
        )
        .await;
    assert_eq!(direct_sql_response.0, StatusCode::OK);
    assert_eq!(
        direct_sql_response.1["rows"],
        json!([{"user_id": "alice", "sum": 25}])
    );

    let compact_response = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_compact_openapi/compact",
        json!({"mode": "sync"}),
    )
    .await;
    assert_eq!(
        compact_response.0,
        StatusCode::NOT_FOUND,
        "compaction response: {}",
        compact_response.1
    );

    let openapi_response = call_json(&router, Method::GET, "/v1/openapi.json", Value::Null).await;
    assert_eq!(openapi_response.0, StatusCode::OK);
    let paths = openapi_response.1["paths"].as_object().unwrap();
    assert!(!paths.contains_key("/v1/views/{view_id}/compact"));
    assert!(paths.contains_key("/v1/api/scores/compact-openapi"));
}

#[tokio::test]
async fn raw_sql_count_and_sum_fail_closed_when_the_materialized_input_is_paged() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-raw-sql-page-cap-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let policy_response = call_json(
        &router,
        Method::POST,
        "/v1/query-policies",
        json!({
            "query_policy_id": "raw-sql-page-cap",
            "policy": {
                "max_sql_bytes": 16384,
                "planning_timeout_ms": 1000,
                "execution_timeout_ms": 1000,
                "max_output_rows": 2,
                "max_output_bytes": 1048576,
                "max_scan_files": 1,
                "max_scan_bytes": 1048576,
                "max_object_requests": 1,
                "max_concurrent_queries": 1,
                "memory_limit_bytes": 1048576,
                "spill_limit_bytes": 1048576
            }
        }),
    )
    .await;
    assert_eq!(policy_response.0, StatusCode::CREATED);

    for (view_id, query_policy_id) in [
        ("scores_raw_sql_explicit_page_cap", None),
        ("scores_raw_sql_policy_page_cap", Some("raw-sql-page-cap")),
    ] {
        let view_response = call_json(
                &router,
                Method::POST,
                "/v1/views",
                json!(CreateViewRequest {
                    view_id: view_id.to_string(),
                    url_path: None,
                    output_relation_id: None,
                    input_relation_id: "scores".to_string(),
                    input_relation_version: "2026-05-24.v1".to_string(),
                    input_relation_refs: Vec::new(),
                    input_relations: Vec::new(),
                    sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".to_string(),
                    source_kind: SqlSourceKind::StandingView,
                    output_relation_ids: Vec::new(),
                    sql_template: None,
                    description: None,
                    request: Vec::new(),
                    response_schema: None,
                    response_formats: vec!["json".to_string()],
                    query_policy_id: query_policy_id.map(str::to_string),
                }),
            )
            .await;
        assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    }

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-raw-sql-page-cap-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "bob", "score": 20, "delta": 1},
                {"user_id": "carol", "score": 30, "delta": 1},
                {"user_id": "dave", "score": 40, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let paged_output = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_raw_sql_explicit_page_cap/query",
        json!({"max_rows": 2}),
    )
    .await;
    assert_eq!(paged_output.0, StatusCode::OK, "{paged_output:?}");
    let page_token = paged_output.1["next_page_token"]
        .as_str()
        .expect("direct materialized read should expose a next page")
        .to_string();
    let cursor_response = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_raw_sql_explicit_page_cap/query",
        json!({
            "sql": "select count(*) as total_count from scores_raw_sql_explicit_page_cap",
            "page_token": page_token
        }),
    )
    .await;
    assert_eq!(
        cursor_response.0,
        StatusCode::BAD_REQUEST,
        "{cursor_response:?}"
    );
    assert!(cursor_response.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("cursor pagination is not supported for raw SQL"));

    let sql = "select count(*) as total_count, sum(sum) as total_sum from {view}";
    for (view_id, request) in [
        (
            "scores_raw_sql_explicit_page_cap",
            json!({"sql": sql.replace("{view}", "scores_raw_sql_explicit_page_cap"), "max_rows": 2}),
        ),
        (
            "scores_raw_sql_policy_page_cap",
            json!({"sql": sql.replace("{view}", "scores_raw_sql_policy_page_cap")}),
        ),
    ] {
        let response = call_json(
            &router,
            Method::POST,
            &format!("/v1/views/{view_id}/query"),
            request,
        )
        .await;
        assert_eq!(response.0, StatusCode::CONFLICT, "{response:?}");
        assert!(response.1["error"]
            .as_str()
            .unwrap_or_default()
            .contains("full materialized snapshot"));
        assert!(response.1.get("rows").is_none(), "{response:?}");
        assert!(response.1.get("next_page_token").is_none(), "{response:?}");
    }
}

#[tokio::test]
async fn rest_two_relation_join_view_materialized_output_survives_api_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "api-test-join-owner-a", false).await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(
            relation_response.0,
            StatusCode::CREATED,
            "relation creation response: {}",
            relation_response.1
        );
    }

    let view_request = CreateViewRequest {
            view_id: "scores_by_account".to_string(),
            url_path: Some("/scores/by-account".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("score metrics grouped by account join".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(
        view_response.0,
        StatusCode::CREATED,
        "join view creation response: {}",
        view_response.1
    );
    assert_eq!(view_response.1["view_id"], "scores_by_account");
    assert_eq!(view_response.1["query_enabled"], true);

    let relations_ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "accounts-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(relations_ingest.0, StatusCode::CREATED);
    assert_eq!(relations_ingest.1["outcome"], "appended");
    assert_eq!(relations_ingest.1["materialization"]["status"], "completed");
    assert_eq!(relations_ingest.1["materialization"]["applied_batches"], 2);

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_by_account/query",
        json!({}),
    )
    .await;
    assert_eq!(
        query_response.0,
        StatusCode::OK,
        "join query response: {}",
        query_response.1
    );
    assert_eq!(relations_ingest.1["materialized_through"], 3);
    assert_join_rows(&query_response.1, 3, 17);

    let restarted_state =
        test_api_state_with_store(store.clone(), "api-test-join-owner-b", true).await;
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restarted_router = app(restarted_state);
    let restarted_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/scores_by_account/query",
        json!({}),
    )
    .await;
    assert_eq!(
        restarted_query.0,
        StatusCode::OK,
        "restarted join query response: {}",
        restarted_query.1
    );
    assert_join_rows(&restarted_query.1, 3, 17);
}

#[tokio::test]
async fn rest_two_relation_join_count_only_view_materialized_output_survives_api_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "api-test-join-count-only-a", false).await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "score_counts_by_account".to_string(),
            url_path: Some("/scores/counts-by-account".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("score event counts grouped by joined account".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);
    let output_columns = view_response.1["output_relations"][0]["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|column| column["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(output_columns, vec!["account_id", "count"]);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-join-count-only-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "accounts-join-count-only-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/score_counts_by_account/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"account_id": "alice", "count": 2},
            {"account_id": "bob", "count": 1}
        ])
    );

    let restarted_state =
        test_api_state_with_store(store, "api-test-join-count-only-b", true).await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let restarted_router = app(restarted_state);
    let restarted_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/score_counts_by_account/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(restarted_query.1["rows"], query_response.1["rows"]);
}

#[tokio::test]
async fn rest_left_join_left_group_key_view_materializes_unmatched_left_rows() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-join-left-key", false).await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "scores_by_user".to_string(),
            url_path: Some("/scores/by-user".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select s.user_id as user, sum(s.score) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id group by s.user_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("score metrics grouped by left join key".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "left-key-scores",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1},
                        {"user_id": "charlie", "score": 30, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "left-key-accounts",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"user": "alice", "sum": 17, "count": 2},
            {"user": "bob", "sum": 5, "count": 1},
            {"user": "charlie", "sum": 30, "count": 1}
        ])
    );
}

#[tokio::test]
async fn rest_two_relation_join_min_max_avg_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-join-stats", false).await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "score_stats_by_account".to_string(),
            url_path: Some("/scores/account-stats".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) as sum, count(*) as count, min(s.score) as min_score, max(s.score) as max_score, avg(s.score) as avg_score from scores s join accounts a on s.user_id = a.account_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("score stats by account materialized join".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "join-stats-scores",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "join-stats-accounts",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/score_stats_by_account/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"account_id": "alice", "sum": 17, "count": 2, "min_score": 7, "max_score": 10, "avg_score": 8.5},
            {"account_id": "bob", "sum": 5, "count": 1, "min_score": 5, "max_score": 5, "avg_score": 5.0}
        ])
    );
}

#[tokio::test]
async fn rest_two_relation_join_right_min_max_avg_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-join-right-stats",
        false,
    )
    .await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "score_limits_by_account".to_string(),
            url_path: Some("/scores/account-limits".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) as sum, count(*) as count, count(a.limit) as count_limit, count(distinct a.limit) as distinct_limits, min(a.limit) as min_limit, max(a.limit) as max_limit, avg(a.limit) as avg_limit from scores s join accounts a on s.user_id = a.account_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("account limits through materialized join".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "join-right-stats-scores",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "join-right-stats-accounts",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/score_limits_by_account/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"account_id": "alice", "sum": 17, "count": 2, "count_limit": 2, "distinct_limits": 1, "min_limit": 100, "max_limit": 100, "avg_limit": 100.0},
            {"account_id": "bob", "sum": 5, "count": 1, "count_limit": 1, "distinct_limits": 1, "min_limit": 50, "max_limit": 50, "avg_limit": 50.0}
        ])
    );
}

#[tokio::test]
async fn rest_late_two_relation_join_having_top_k_reports_materialization_lag_on_first_query() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-late-join-having-top-k",
        false,
    )
    .await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let first_ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "late-join-having-top-k-scores",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "late-join-having-top-k-accounts",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(first_ingest.0, StatusCode::CREATED, "{first_ingest:?}");

    let view_request = CreateViewRequest {
            view_id: "late_top_scores_by_account_having".to_string(),
            url_path: Some("/scores/late-top-account-having".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id having sum(s.score) >= 5 order by sum desc limit 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("late-created top account score join above threshold".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], false);
    assert_eq!(view_response.1["coverage"]["state"], "backfill_required");

    let second_ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "late-join-having-top-k-scores",
                "partition_id": 0,
                "start_offset_inclusive": 3,
                "rows": [{"user_id": "bob", "score": 20, "delta": 1}]
            }]
        }),
    )
    .await;
    assert_eq!(
        second_ingest.0,
        StatusCode::CREATED,
        "late join view must not block later ingest: {}",
        second_ingest.1
    );

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/late_top_scores_by_account_having/query",
        json!({}),
    )
    .await;
    assert_eq!(
        query_response.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "late join query response: {}",
        query_response.1
    );
    assert!(query_response.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("MATERIALIZATION_LAG"));

    let refreshed_view = call_json(
        &router,
        Method::GET,
        "/v1/views/late_top_scores_by_account_having",
        json!({}),
    )
    .await;
    assert_eq!(refreshed_view.0, StatusCode::OK);
    assert_eq!(refreshed_view.1["query_enabled"], false);
    assert_eq!(
        refreshed_view.1["lifecycle"]["deployment_status"],
        "deploying"
    );
}

#[tokio::test]
async fn rest_two_relation_join_order_by_limit_top_k_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-join-top-k", false).await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "top_scores_by_account".to_string(),
            url_path: Some("/scores/top-account".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id order by sum desc limit 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("top score account by materialized join sum".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let first_ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "join-top-k-scores",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "join-top-k-accounts",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(first_ingest.0, StatusCode::CREATED, "{first_ingest:?}");
    let first_query = call_json(
        &router,
        Method::POST,
        "/v1/views/top_scores_by_account/query",
        json!({}),
    )
    .await;
    assert_eq!(first_query.0, StatusCode::OK, "{first_query:?}");
    assert_eq!(
        first_query.1["rows"],
        json!([{"account_id": "alice", "sum": 17, "count": 2}])
    );

    let second_ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "join-top-k-scores",
                "partition_id": 0,
                "start_offset_inclusive": 3,
                "rows": [{"user_id": "bob", "score": 20, "delta": 1}]
            }]
        }),
    )
    .await;
    assert_eq!(second_ingest.0, StatusCode::CREATED, "{second_ingest:?}");
    let second_query = call_json(
        &router,
        Method::GET,
        "/v1/api/scores/top-account",
        Value::Null,
    )
    .await;
    assert_eq!(second_query.0, StatusCode::OK, "{second_query:?}");
    assert_eq!(
        second_query.1["rows"],
        json!([{"account_id": "bob", "sum": 25, "count": 2}])
    );
}

#[tokio::test]
async fn rest_two_relation_join_having_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-join-having", false).await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "scores_by_account_having".to_string(),
            url_path: Some("/scores/by-account-having".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id having sum(s.score) > 10 or count(*) = 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("score metrics above threshold grouped by account join".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-join-having-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "accounts-join-having-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_by_account_having/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"account_id": "alice", "sum": 17, "count": 2},
            {"account_id": "bob", "sum": 5, "count": 1}
        ])
    );
}

#[tokio::test]
async fn rest_two_relation_join_mixed_aggregate_filter_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-join-mixed-filter",
        false,
    )
    .await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "scores_by_account_mixed_filter".to_string(),
            url_path: Some("/scores/by-account-mixed-filter".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) filter (where s.score > 5) as sum, count(*) filter (where s.score > 0) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("mixed filtered score metrics grouped by account join".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-join-mixed-filter-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "accounts-join-mixed-filter-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_by_account_mixed_filter/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"account_id": "alice", "sum": 17, "count": 2},
            {"account_id": "bob", "sum": 0, "count": 1}
        ])
    );
}

#[tokio::test]
async fn rest_two_relation_join_filtered_count_distinct_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-join-filtered-distinct",
        false,
    )
    .await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "filtered_distinct_scores_by_account".to_string(),
            url_path: Some("/scores/filtered-distinct-by-account".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) filter (where s.score > 5) as sum, count(distinct s.score) filter (where s.score > 0) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("filtered distinct score counts grouped by account join".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-join-filtered-distinct-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "accounts-join-filtered-distinct-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/filtered_distinct_scores_by_account/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"account_id": "alice", "sum": 27, "distinct_scores": 2},
            {"account_id": "bob", "sum": 0, "distinct_scores": 1}
        ])
    );
}

#[tokio::test]
async fn rest_two_relation_join_nullable_left_value_count_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-join-nullable-count",
        false,
    )
    .await;
    let router = app(state);

    for catalog in [
        test_scores_catalog_with_nullable_score(),
        test_accounts_catalog(),
    ] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "nullable_score_counts_by_account".to_string(),
            url_path: Some("/scores/nullable-count-by-account".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) as sum, count(s.score) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("nullable score counts grouped by account join".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-join-nullable-count-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": null, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "accounts-join-nullable-count-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/nullable_score_counts_by_account/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"account_id": "alice", "sum": 17, "count": 2},
            {"account_id": "bob", "sum": 5, "count": 1}
        ])
    );
}

#[tokio::test]
async fn rest_two_relation_join_count_distinct_view_materializes_outputs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state =
        test_api_state_with_store(store.clone(), "api-test-join-count-distinct-a", false).await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "distinct_scores_by_account".to_string(),
            url_path: Some("/scores/distinct-by-account".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) as sum, count(distinct s.score) as distinct_scores from scores s join accounts a on s.user_id = a.account_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("distinct score counts grouped by joined account".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-join-distinct-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "accounts-join-distinct-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/distinct_scores_by_account/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([
            {"account_id": "alice", "sum": 27, "distinct_scores": 2},
            {"account_id": "bob", "sum": 5, "distinct_scores": 1}
        ])
    );

    let restarted_state =
        test_api_state_with_store(store, "api-test-join-count-distinct-b", true).await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let restarted_router = app(restarted_state);
    let restarted_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/distinct_scores_by_account/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(restarted_query.1["rows"], query_response.1["rows"]);
}

#[tokio::test]
async fn rest_two_relation_join_alias_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-join-alias", false).await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "scores_by_account_alias".to_string(),
            url_path: Some("/scores/by-account-alias".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id as account, sum(s.score) as total_score, count(1) as score_events from scores s join accounts a on s.user_id = a.account_id group by a.account_id having total_score > 10 and count(1) > 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("aliased score metrics grouped by account join".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-join-alias-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "accounts-join-alias-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_by_account_alias/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"account": "alice", "total_score": 17, "score_events": 2}])
    );
}

#[tokio::test]
async fn rest_two_relation_join_where_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-join-where", false).await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "scores_by_account_where".to_string(),
            url_path: Some("/scores/by-account-where".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id where (s.score > 0 or s.score = -3) and s.score < 100 and a.limit > 60 and a.tier = 'gold' group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("positive score metrics grouped by account join".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-join-where-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1},
                        {"user_id": "alice", "score": 150, "delta": 1},
                        {"user_id": "charlie", "score": 8, "delta": 1},
                        {"user_id": "alice", "score": -3, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "accounts-join-where-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1},
                        {"account_id": "charlie", "limit": 100, "tier": "silver", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_by_account_where/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"account_id": "alice", "sum": 14, "count": 3}])
    );
}

#[tokio::test]
async fn rest_two_relation_join_cte_source_filter_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-join-cte", false).await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "scores_by_account_cte".to_string(),
            url_path: Some("/scores/by-account-cte".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "with positive_scores as (select * from scores where score > 0) select a.account_id, sum(s.score) as sum, count(*) as count from positive_scores s join accounts a on s.user_id = a.account_id where a.limit > 60 group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("positive score metrics from CTE source grouped by account join".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-join-cte-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": -3, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "accounts-join-cte-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1},
                        {"account_id": "charlie", "limit": 100, "tier": "silver", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_by_account_cte/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"account_id": "alice", "sum": 17, "count": 2}])
    );
}

#[tokio::test]
async fn rest_two_relation_join_right_cte_source_filter_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-join-right-cte", false)
            .await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "scores_by_account_right_cte".to_string(),
            url_path: Some("/scores/by-account-right-cte".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "with eligible_accounts as (select * from accounts where limit > 60) select a.account_id, sum(s.score) as sum, count(*) as count from scores s join eligible_accounts a on s.user_id = a.account_id where s.score > 0 group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("positive score metrics joined to right CTE account source".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-join-right-cte-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": -3, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "accounts-join-right-cte-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1},
                        {"account_id": "charlie", "limit": 100, "tier": "silver", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_by_account_right_cte/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"account_id": "alice", "sum": 17, "count": 2}])
    );
}

#[tokio::test]
async fn rest_two_relation_join_two_cte_source_filter_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-join-two-cte", false).await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "scores_by_account_two_cte".to_string(),
            url_path: Some("/scores/by-account-two-cte".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "with positive_scores as (select * from scores where score > 0), eligible_accounts as (select * from accounts where limit > 60) select a.account_id, sum(s.score) as sum, count(*) as count from positive_scores s join eligible_accounts a on s.user_id = a.account_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("positive score metrics joined through two CTE sources".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-join-two-cte-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": -3, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "accounts-join-two-cte-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1},
                        {"account_id": "charlie", "limit": 100, "tier": "silver", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_by_account_two_cte/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"account_id": "alice", "sum": 17, "count": 2}])
    );
}

#[tokio::test]
async fn rest_two_relation_join_derived_table_source_filter_view_materializes_outputs() {
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-join-derived-source",
        false,
    )
    .await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog,
                "default_orders_sum_count": false
            }),
        )
        .await;
        assert_eq!(relation_response.0, StatusCode::CREATED);
    }

    let view_request = CreateViewRequest {
            view_id: "scores_by_account_derived_source".to_string(),
            url_path: Some("/scores/by-account-derived-source".to_string()),
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: vec![
                InputRelationRef {
                    relation_id: "scores".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
                InputRelationRef {
                    relation_id: "accounts".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) as sum, count(*) as count from (select * from scores where score > 0) s join (select * from accounts where limit > 60) a on s.user_id = a.account_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("positive score metrics joined through derived table sources".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores-join-derived-source-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "alice", "score": -3, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1},
                        {"user_id": "alice", "score": 7, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "accounts-join-derived-source-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                        {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1},
                        {"account_id": "charlie", "limit": 100, "tier": "silver", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/scores_by_account_derived_source/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"account_id": "alice", "sum": 17, "count": 2}])
    );
}

fn assert_join_rows(response: &Value, expected_epoch: u64, expected_alice_sum: i64) {
    assert_eq!(response["logical_epoch"], expected_epoch);
    assert_eq!(
        response["rows"],
        json!([
            {"account_id": "alice", "sum": expected_alice_sum, "count": 2},
            {"account_id": "bob", "sum": 5, "count": 1}
        ])
    );
}

fn assert_latest_device_rows(
    response: &Value,
    expected_epoch: u64,
    expected_device_a: bool,
    expected_device_b: bool,
) {
    assert_eq!(response["logical_epoch"], expected_epoch);
    assert_eq!(
        response["rows"],
        json!([
            {"device": "device-a", "enabled": expected_device_a},
            {"device": "device-b", "enabled": expected_device_b}
        ])
    );
}

fn assert_window_rows(response: &Value, expected_epoch: u64, expected_rows: Value) {
    assert_eq!(response["logical_epoch"], expected_epoch);
    assert_eq!(response["rows"], expected_rows);
}

fn test_runtime_checkpoint(output_manifest_refs: Vec<String>) -> RuntimeCheckpoint {
    let state_payload = serde_json::json!({
        "schema_version": 1,
        "published_output": {
            "records": []
        }
    })
    .to_string();
    let content_hash = stable_bytes_hash(state_payload.as_bytes());
    RuntimeCheckpoint {
        identity: StandingProgramIdentity {
            tenant_id: "tenant-a".to_string(),
            program_id: "program-purchases".to_string(),
            view_ids: vec!["purchases_by_user".to_string()],
            sql_hash: format!("sha256:{}", "a".repeat(64)),
            input_catalog_hash: format!("sha256:{}", "b".repeat(64)),
            output_schema_hash: format!("sha256:{}", "c".repeat(64)),
            planner_identity: "velorix-logical-view-planner-v1".to_string(),
            builtin_runtime_identities: vec![BuiltinRuntimeIdentity {
                name: "velorix-runtime".to_string(),
                version: "test".to_string(),
            }],
            runtime_capabilities: vec!["materialized-view-runtime".to_string()],
            runtime_compatibility: "velorix-materialized-view-runtime-v1".to_string(),
            checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
            native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
        },
        logical_epoch: 7,
        input_frontiers: vec![RelationFrontier {
            relation_id: "purchases".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            stream_id: "test-stream".to_string(),
            partition_id: 0,
            committed_offset_exclusive: 11,
        }],
        input_event_time_frontiers: Vec::new(),
        output_frontiers: vec![ViewFrontier {
            view_id: "purchases_by_user".to_string(),
            committed_epoch: 7,
        }],
        checkpoint_codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
        state_root: DurableStateRoot {
            object_key: "v1/state/materialized-view-runtime/program-purchases/checkpoint"
                .to_string(),
            content_hash,
        },
        state_payload: Some(RuntimeCheckpointStatePayload {
            codec_identity: "velorix-standing-program-checkpoint-v1".to_string(),
            payload: state_payload,
        }),
        output_manifest_refs,
        owner_epoch: None,
    }
}

#[test]
fn standing_runtime_budget_rejects_oversized_state_payload() {
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let error = validate_standing_runtime_budget(
        &[],
        &checkpoint,
        StandingRuntimeBudgetLimits {
            max_output_delta_records: 1,
            max_state_payload_bytes: 1,
        },
    )
    .unwrap_err();

    assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(error.to_string().contains("checkpoint state payload size"));
}

async fn test_api_state() -> ApiState {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    test_api_state_with_store(store, "api-test-owner", false).await
}

async fn test_api_state_with_store(
    store: Arc<dyn ObjectStore>,
    owner_id: &str,
    reconstruct_ingest_admission: bool,
) -> ApiState {
    test_public_api_state_with_store(store, owner_id, reconstruct_ingest_admission)
        .await
        .with_experimental_advanced_view_features(true)
}

async fn test_public_api_state_with_store(
    store: Arc<dyn ObjectStore>,
    owner_id: &str,
    reconstruct_ingest_admission: bool,
) -> ApiState {
    let validated = validate_operator_authority(
        ObjectStoreAuthorityRef {
            store_id: "test".to_string(),
            namespace: "unit".to_string(),
        },
        store,
        "memory",
        "api-test",
    )
    .await
    .unwrap();
    ApiState::from_validated_authority_with_ingest_admission_startup(
        validated,
        "ignored",
        owner_id,
        reconstruct_ingest_admission,
    )
    .await
    .unwrap()
}

struct FailingApplyRuntimeFactory {
    delegate: MaterializedViewRuntimeFactory,
    reason: String,
}

impl FailingApplyRuntimeFactory {
    fn new(reason: impl Into<String>) -> Self {
        Self {
            delegate: MaterializedViewRuntimeFactory,
            reason: reason.into(),
        }
    }
}

impl StandingProgramRuntimeFactory for FailingApplyRuntimeFactory {
    fn output_schemas_for_view_request(
        &self,
        view_id: &str,
        sql: &str,
        catalog: &VelorixRelationCatalogV1,
        input_schema_fingerprint: &str,
    ) -> Result<Option<Vec<RelationSchema>>, ApiError> {
        self.delegate.output_schemas_for_view_request(
            view_id,
            sql,
            catalog,
            input_schema_fingerprint,
        )
    }

    fn output_schemas_for_view_request_with_catalogs(
        &self,
        view_id: &str,
        sql: &str,
        catalogs: &[VelorixRelationCatalogV1],
        input_schema_fingerprint: &str,
    ) -> Result<Option<Vec<RelationSchema>>, ApiError> {
        self.delegate.output_schemas_for_view_request_with_catalogs(
            view_id,
            sql,
            catalogs,
            input_schema_fingerprint,
        )
    }

    fn create(
        &self,
        identity: &StandingProgramIdentity,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Ok(Box::new(FailingApplyRuntime {
            identity: identity.clone(),
            input_schemas: Vec::new(),
            output_schemas: Vec::new(),
            reason: self.reason.clone(),
        }))
    }

    fn create_with_catalogs_plan_and_spec(
        &self,
        identity: &StandingProgramIdentity,
        _catalogs: &[VelorixRelationCatalogV1],
        _logical_plan: &VelorixLogicalViewPlanV1,
        _spec: &StandingViewSpec,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Ok(Box::new(FailingApplyRuntime {
            identity: identity.clone(),
            input_schemas: input_schemas.to_vec(),
            output_schemas: output_schemas.to_vec(),
            reason: self.reason.clone(),
        }))
    }

    fn restore(
        &self,
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Ok(Box::new(FailingApplyRuntime {
            identity: checkpoint.identity,
            input_schemas: Vec::new(),
            output_schemas: Vec::new(),
            reason: self.reason.clone(),
        }))
    }
}

struct FailingApplyRuntime {
    identity: StandingProgramIdentity,
    input_schemas: Vec<RelationSchema>,
    output_schemas: Vec<RelationSchema>,
    reason: String,
}

impl FailingApplyRuntime {
    fn error(&self) -> StandingProgramRuntimeError {
        StandingProgramRuntimeError::ExternalRuntime {
            reason: self.reason.clone(),
        }
    }
}

impl StandingProgramRuntime for FailingApplyRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        self.input_schemas.clone()
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        self.output_schemas.clone()
    }

    fn logical_epoch(&self) -> u64 {
        0
    }

    fn apply_changes(
        &mut self,
        _logical_epoch: u64,
        _idempotency_key: EpochIdempotencyKey,
        _input_changes: Vec<RelationInputBatch>,
    ) -> Result<velorix_core::standing_program::EpochCommit, StandingProgramRuntimeError> {
        Err(self.error())
    }

    fn materialized_view_page(
        &self,
        _view: ScopedViewId,
        _page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        Err(self.error())
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        Err(self.error())
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        Ok(Self {
            identity: checkpoint.identity,
            input_schemas: Vec::new(),
            output_schemas: Vec::new(),
            reason: "restored failing apply runtime".to_string(),
        })
    }
}

#[test]
fn public_view_response_json_omits_legacy_artifact_key() {
    let input = RelationSchema {
        relation_id: "scores".to_string(),
        relation_name: "scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        schema_fingerprint: "sha256:input".to_string(),
        columns: vec![ColumnSchema {
            name: "user_id".to_string(),
            data_type: SqlDataType::Utf8,
            nullable: false,
        }],
        primary_key: vec!["user_id".to_string()],
    };
    let output = RelationSchema {
        relation_id: "scores_by_user".to_string(),
        relation_name: "scores_by_user".to_string(),
        relation_version: "view".to_string(),
        schema_fingerprint: "sha256:output".to_string(),
        columns: input.columns.clone(),
        primary_key: input.primary_key.clone(),
    };
    let spec = StandingViewSpec {
        view_id: "scores_by_user".to_string(),
        sql: "select user_id from scores".to_string(),
        dialect: SqlDialect::VelorixSql,
        source_kind: SqlSourceKind::StandingView,
        input_relations: vec![input],
        output_relations: vec![output],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let active = ActiveMaterializedView {
        spec_hash: "sha256:spec".to_string(),
        spec,
        execution_mode: MaterializedViewExecutionMode::StandingRuntime,
        api: Some(MaterializedViewApiMetadata::default()),
        artifact: Some(
            velorix_control::storage_admin::MaterializedViewArtifactBinding {
                artifact_id: "legacy-artifact".to_string(),
                artifact_hash: "sha256:artifact".to_string(),
                runtime_crate_name: "legacy-runtime".to_string(),
                state_codec: "legacy-codec".to_string(),
                state_schema_version: 1,
                execution_status: "ready".to_string(),
                execution_path: "legacy/external/path".to_string(),
                standing_program_identity: None,
            },
        ),
        runtime: None,
        lifecycle: MaterializedViewLifecycleStatus::standing_runtime(),
    };

    let response = active_view_response(&active, None).unwrap();
    let response_json = serde_json::to_value(response).unwrap();
    assert!(response_json.get("artifact").is_none(), "{response_json}");
}

#[tokio::test]
async fn public_1_0_rejects_experimental_view_surfaces_by_default() {
    let state = test_public_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-public-1-0-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    for sql in [
            "select user_id, window_start, window_end, sum(score) as sum, count(*) as count from scores group by user_id, tumble(interval '60 seconds')",
            "select user_id, window_start, window_end, sum(score) as sum, count(*) as count from scores group by user_id, hop (interval '60 seconds', interval '5 seconds')",
            "select user_id, window_start, window_end, sum(score) as sum, count(*) as count from scores group by user_id, session(interval '60 seconds')",
            "select user_id, row_number() over (partition by user_id order by score desc, user_id asc) as rank from scores",
            "select user_id, sum(score)\nover (partition by user_id) as total from scores",
        ] {
            let view_response = call_json(
                &router,
                Method::POST,
                "/v1/views",
                json!({
                    "view_id": "experimental_view",
                    "input_relation_id": "scores",
                    "input_relation_version": "2026-05-24.v1",
                    "sql": sql
                }),
            )
            .await;
            assert_eq!(view_response.0, StatusCode::BAD_REQUEST, "{view_response:?}");
            assert!(view_response.1["error"]
                .as_str()
                .unwrap_or_default()
                .contains("experimental"));
        }

    for body in [
        json!({"mode": "background"}),
        json!({"mode": "sync", "range": {
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "start_offset_inclusive": 0,
            "end_offset_exclusive": 1
        }}),
        json!({"mode": "sync", "scope": {"where": "score > 0"}}),
    ] {
        let backfill_response =
            call_json(&router, Method::POST, "/v1/views/missing/backfill", body).await;
        assert_eq!(
            backfill_response.0,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{backfill_response:?}"
        );
    }

    let compact_response = call_json(
        &router,
        Method::POST,
        "/v1/views/missing/compact",
        json!({"mode": "background"}),
    )
    .await;
    assert_eq!(compact_response.0, StatusCode::NOT_FOUND);

    let ready = call_json(&router, Method::GET, "/readyz", Value::Null).await;
    assert_eq!(ready.0, StatusCode::OK);
    assert!(ready.1["materialization_policy"]
        .get("output_compaction")
        .is_none());
    assert!(ready.1.get("background_tasks").is_none());
}

fn test_checkpoint_key(checkpoint: &RuntimeCheckpoint) -> ObjectKey {
    ObjectKey::standing_runtime_checkpoint(
        &checkpoint.identity.tenant_id,
        &checkpoint.identity.program_id,
        "purchases_by_user",
        checkpoint.logical_epoch,
        &checkpoint.state_root.content_hash,
    )
    .unwrap()
}

fn test_checkpoint_pointer(
    checkpoint_key: &ObjectKey,
    checkpoint: &RuntimeCheckpoint,
) -> StandingRuntimeCheckpointPointer {
    StandingRuntimeCheckpointPointer {
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        view_id: "purchases_by_user".to_string(),
        checkpoint_key: checkpoint_key.as_str().to_string(),
        logical_epoch: checkpoint.logical_epoch,
        content_hash: checkpoint.state_root.content_hash.clone(),
        manifest_hash: "sha256:test-manifest".to_string(),
        output_manifest_refs: checkpoint.output_manifest_refs.clone(),
    }
}

fn test_checkpoint_record(
    checkpoint_key: &ObjectKey,
    checkpoint: RuntimeCheckpoint,
) -> StandingRuntimeCheckpointRecord {
    StandingRuntimeCheckpointRecord {
        schema_version: 1,
        record_kind: "standing_runtime_checkpoint_v1".to_string(),
        view_id: "purchases_by_user".to_string(),
        checkpoint_key: checkpoint_key.as_str().to_string(),
        previous_checkpoint: None,
        checkpoint,
        replay_checkpoints: Vec::new(),
        manifest_hash: String::new(),
    }
}

fn test_purchases_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "purchases".to_string(),
        relation_name: "purchases".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "user_id".to_string(),
                name: "user_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::Metadata,
            },
            RelationColumnV1 {
                column_id: "amount".to_string(),
                name: "amount".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Metadata,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["user_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)
        .expect("test relation schema should fingerprint");

    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "purchases".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "purchases".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn test_purchases_event_time_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = test_purchases_catalog();
    catalog.relation_schema.columns.insert(
        2,
        RelationColumnV1 {
            column_id: "event_time".to_string(),
            name: "event_time".to_string(),
            logical_type: VelorixLogicalTypeV1::Int64,
            physical_arrow_type: ArrowPhysicalTypeV1::Int64,
            nullable: false,
            ordinal: 2,
            semantic_role: RelationSemanticRoleV1::EventTime,
        },
    );
    for (ordinal, column) in catalog.relation_schema.columns.iter_mut().enumerate() {
        column.ordinal = ordinal as u32;
    }
    catalog.relation_schema.event_time_column_id = Some("event_time".to_string());
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("event-time purchases schema should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
}

fn test_scores_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "scores".to_string(),
        relation_name: "scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "user_id".to_string(),
                name: "user_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "score".to_string(),
                name: "score".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Metadata,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["user_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)
        .expect("scores catalog should fingerprint");
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "scores".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "scores".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn test_scores_catalog_with_nullable_score() -> VelorixRelationCatalogV1 {
    let mut catalog = test_scores_catalog();
    let score = catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable scores catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
}

fn test_accounts_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "accounts".to_string(),
        relation_name: "accounts".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "limit".to_string(),
                name: "limit".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "tier".to_string(),
                name: "tier".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Metadata,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 3,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["account_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)
        .expect("accounts catalog should fingerprint");
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "accounts".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "accounts".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn test_device_status_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "device_status".to_string(),
        relation_name: "device_status".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "device_id".to_string(),
                name: "device_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "enabled".to_string(),
                name: "enabled".to_string(),
                logical_type: VelorixLogicalTypeV1::Bool,
                physical_arrow_type: ArrowPhysicalTypeV1::Boolean,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "event_time".to_string(),
                name: "event_time".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::EventTime,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 3,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["device_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: Some("event_time".to_string()),
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)
        .expect("device status catalog should fingerprint");
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "device_status".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "device_status".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

// -----------------------------------------------------------------------
// Scenario tests: full Relation → View → Ingest → Query flows
// -----------------------------------------------------------------------

#[tokio::test]
async fn scenario_relation_view_ingest_query_aggregate() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "test-owner", false).await;
    let router = app(state);

    let (status, _) = call_json(&router, Method::GET, "/healthz", Value::Null).await;
    assert_eq!(status, StatusCode::OK);

    let (status, resp) = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "create relation: status={status} resp={resp}"
    );

    let (status, resp) = call_json(
            &router,
            Method::POST,
            "/v1/views",
            json!({
                "view_id": "user_score_totals",
                "sql": "select user_id, sum(score) as total, count(*) as cnt from scores group by user_id",
                "input_relation_id": "scores",
                "input_relation_version": "2026-05-24.v1",
                "source_kind": "standing_view"
            }),
        )
        .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "create view: status={status} resp={resp}"
    );

    let (status, resp) = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 100, "delta": 1},
                    {"user_id": "bob", "score": 200, "delta": 1},
                    {"user_id": "alice", "score": 150, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "ingest: status={status} resp={resp}"
    );

    let (status, resp) = call_json(
        &router,
        Method::POST,
        "/v1/views/user_score_totals/query",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query: status={status} resp={resp}");

    let rows = resp["rows"].as_array().expect("expected rows array");
    assert!(
        !rows.is_empty(),
        "expected at least one row in materialized view"
    );
}

#[tokio::test]
async fn scenario_multi_epoch_incremental() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "test-owner", false).await;
    let router = app(state);

    call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;

    let (status, resp) = call_json(
            &router,
            Method::POST,
            "/v1/views",
            json!({
                "view_id": "user_score_totals",
                "sql": "select user_id, sum(score) as total, count(*) as cnt from scores group by user_id",
                "input_relation_id": "scores",
                "input_relation_version": "2026-05-24.v1",
                "source_kind": "standing_view"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "create view: {resp}");

    let (status, _) = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "s1",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"user_id": "alice", "score": 100, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, resp) = call_json(
        &router,
        Method::POST,
        "/v1/views/user_score_totals/query",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query: {resp}");
    assert_eq!(resp["logical_epoch"], 1, "query: {resp}");
    assert_eq!(
        resp["rows"],
        json!([{"user_id": "alice", "total": 100, "cnt": 1}]),
        "epoch 1 must publish the exact aggregate row, value, and multiplicity: {resp}"
    );

    let (status, _) = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "s1",
                "partition_id": 0,
                "start_offset_inclusive": 1,
                "rows": [
                    {"user_id": "alice", "score": 50, "delta": 1},
                    {"user_id": "bob", "score": 200, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, resp) = call_json(
        &router,
        Method::POST,
        "/v1/views/user_score_totals/query",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query: {resp}");
    assert_eq!(resp["logical_epoch"], 3, "query: {resp}");
    assert_eq!(
        resp["rows"],
        json!([
            {"user_id": "alice", "total": 150, "cnt": 2},
            {"user_id": "bob", "total": 200, "cnt": 1}
        ]),
        "epoch 2 must replace stale values and preserve per-key multiplicities: {resp}"
    );
}

#[tokio::test]
async fn scenario_list_views_empty_then_create() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "test-owner", false).await;
    let router = app(state);

    let (status, resp) = call_json(&router, Method::GET, "/v1/views", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    let views = resp["views"].as_array().expect("expected views array");
    assert!(views.is_empty(), "expected no views initially");

    call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog(),
            "default_orders_sum_count": false
        }),
    )
    .await;

    call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "test_view",
            "sql": "select user_id, sum(score) as total from scores group by user_id",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "source_kind": "standing_view"
        }),
    )
    .await;

    let (status, resp) = call_json(&router, Method::GET, "/v1/views", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    let views = resp["views"].as_array().unwrap();
    assert_eq!(views.len(), 1, "expected 1 view after creation");
    assert_eq!(views[0]["view_id"], "test_view");
}
