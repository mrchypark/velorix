use super::*;
use crate::example_fixtures::default_positive_scores_view_request;
#[path = "generic_demand_materialization_tests.rs"]
mod generic_demand_materialization_tests;
#[path = "generic_demand_migration_tests.rs"]
mod generic_demand_migration_tests;
#[path = "generic_query_http_format_tests.rs"]
mod generic_query_http_format_tests;
#[path = "materialized_output_pages_tests.rs"]
mod materialized_output_pages_tests;
use async_trait::async_trait;
use axum::http::Method;
use futures::{stream::BoxStream, StreamExt};
use object_store::{
    memory::InMemory, path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;
use tower::ServiceExt as _;
use velorix_control::meta_admin as meta;
use velorix_control::meta_admin::{
    CaptureIngestSourceCutRequest, CaptureRelationIngestSourceCutsRequest, InMemoryMetaStore,
    IngestSourceRelationIdentityV1, ViewBootstrapLifecycleV1,
};
use velorix_core::{
    delta::{DeltaKey, DeltaRecord, DeltaValue},
    relation::CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID,
    standing_program::{
        DurableStateRoot, RelationFrontier, RuntimeCheckpointInputCoverageV1,
        RuntimeCheckpointPartitionCoverageV1, RuntimeCheckpointRelationCoverageV1,
        RuntimeCheckpointStatePayload, ViewFrontier,
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
pub(super) struct ObjectStoreAccessCounts {
    get_paths: Arc<Mutex<Vec<String>>>,
    get_object_bytes: Arc<Mutex<Vec<(String, u64)>>>,
    list_prefixes: Arc<Mutex<Vec<String>>>,
    active_ingest_puts: Arc<std::sync::atomic::AtomicUsize>,
    max_concurrent_ingest_puts: Arc<std::sync::atomic::AtomicUsize>,
}

impl ObjectStoreAccessCounts {
    pub(super) fn clear(&self) {
        self.get_paths.lock().unwrap().clear();
        self.get_object_bytes.lock().unwrap().clear();
        self.list_prefixes.lock().unwrap().clear();
        self.active_ingest_puts
            .store(0, std::sync::atomic::Ordering::SeqCst);
        self.max_concurrent_ingest_puts
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn get_paths(&self) -> Vec<String> {
        self.get_paths.lock().unwrap().clone()
    }

    pub(super) fn get_object_bytes(&self) -> Vec<(String, u64)> {
        self.get_object_bytes.lock().unwrap().clone()
    }

    pub(super) fn list_prefixes(&self) -> Vec<String> {
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
pub(super) struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counts: ObjectStoreAccessCounts,
    ingest_put_delay: Duration,
    page_get_gate: Option<(
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
        Arc<std::sync::atomic::AtomicBool>,
    )>,
}

#[derive(Debug)]
pub(super) struct ArmedPrefixFailingObjectStore {
    inner: Arc<dyn ObjectStore>,
    failing_prefix: Mutex<Option<String>>,
}

impl ArmedPrefixFailingObjectStore {
    pub(super) fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            failing_prefix: Mutex::new(None),
        }
    }

    pub(super) fn arm(&self, prefix: impl Into<String>) {
        *self.failing_prefix.lock().unwrap() = Some(prefix.into());
    }
}

impl fmt::Display for ArmedPrefixFailingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArmedPrefixFailingObjectStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for ArmedPrefixFailingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let should_fail = {
            let mut failing_prefix = self.failing_prefix.lock().unwrap();
            failing_prefix
                .as_ref()
                .is_some_and(|prefix| location.as_ref().starts_with(prefix.as_str()))
                .then(|| failing_prefix.take())
                .flatten()
                .is_some()
        };
        if should_fail {
            return Err(object_store::Error::Generic {
                store: "armed-prefix-failure",
                source: "injected checkpoint publication write failure".into(),
            });
        }
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

impl CountingObjectStore {
    pub(super) fn new(inner: Arc<dyn ObjectStore>, counts: ObjectStoreAccessCounts) -> Self {
        Self {
            inner,
            counts,
            ingest_put_delay: Duration::ZERO,
            page_get_gate: None,
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
            page_get_gate: None,
        }
    }

    pub(super) fn with_page_get_gate(
        mut self,
        entered: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    ) -> Self {
        self.page_get_gate = Some((
            entered,
            resume,
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
        ));
        self
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
        // ObjectStore's default head() delegates to get_opts(head=true).
        // Count payload reads separately from metadata-only size checks.
        if !options.head {
            self.counts
                .get_paths
                .lock()
                .unwrap()
                .push(location.to_string());
        }
        if !options.head
            && location
                .as_ref()
                .starts_with("v1/standing-runtime-output-pages/")
        {
            if let Some((entered, resume, armed)) = &self.page_get_gate {
                if armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    entered.notify_one();
                    resume.notified().await;
                }
            }
        }
        let payload_read = !options.head;
        let result = self.inner.get_opts(location, options).await?;
        if payload_read {
            self.counts
                .get_object_bytes
                .lock()
                .unwrap()
                .push((location.to_string(), result.meta.size));
        }
        Ok(result)
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
    assert_eq!(record.output_encoding, "velorix-delta-batch-arrow-v1");
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
        test_checkpoint_persist_context(&state, &checkpoint, "purchases_by_user").await,
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
    let record = standing_runtime_checkpoint_record_from_slice(&checkpoint_bytes).unwrap();
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
    let delta_record =
        output_delta_record::decode(&delta_bytes, state.max_standing_runtime_state_payload_bytes)
            .unwrap();
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

#[test]
fn standing_runtime_output_delta_publication_is_canonical_and_detects_a_mutant() {
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let schema_fingerprint =
        "sha256:0000000000000000000000000000000000000000000000000000000000000001";
    let alice = DeltaRecord::new(
        DeltaKey::from_json(json!("alice")),
        DeltaValue::from_json(json!({"count": 1, "sum": 10})),
        1,
    );
    let bob = DeltaRecord::new(
        DeltaKey::from_json(json!("bob")),
        DeltaValue::from_json(json!({"count": 1, "sum": 5})),
        1,
    );
    let canonical = ViewOutputDelta {
        view_id: "purchases_by_user".to_string(),
        schema_fingerprint: schema_fingerprint.to_string(),
        delta: DeltaBatch::from_records([alice.clone(), bob.clone()]),
    };
    let differently_ordered = ViewOutputDelta {
        view_id: canonical.view_id.clone(),
        schema_fingerprint: canonical.schema_fingerprint.clone(),
        delta: DeltaBatch::from_records([
            bob,
            DeltaRecord::new(alice.key.clone(), alice.value.clone(), 2),
            DeltaRecord::new(alice.key.clone(), alice.value.clone(), -1),
        ]),
    };
    let mutant = ViewOutputDelta {
        view_id: canonical.view_id.clone(),
        schema_fingerprint: canonical.schema_fingerprint.clone(),
        delta: DeltaBatch::from_records([alice]),
    };

    let canonical_publication = standing_runtime_output_delta_records_for_checkpoint(
        &checkpoint,
        "purchases_by_user",
        &[canonical],
    )
    .unwrap();
    let reordered_publication = standing_runtime_output_delta_records_for_checkpoint(
        &checkpoint,
        "purchases_by_user",
        &[differently_ordered],
    )
    .unwrap();
    let mutant_publication = standing_runtime_output_delta_records_for_checkpoint(
        &checkpoint,
        "purchases_by_user",
        &[mutant],
    )
    .unwrap();

    assert_eq!(
        canonical_publication[0].delta_key,
        reordered_publication[0].delta_key
    );
    assert_eq!(
        canonical_publication[0].delta_record.output_delta,
        reordered_publication[0].delta_record.output_delta
    );
    assert_ne!(
        canonical_publication[0].delta_key,
        mutant_publication[0].delta_key
    );
}

#[test]
fn published_relation_output_commit_fences_empty_delta_and_checkpoint_cut() {
    let mut checkpoint = test_runtime_checkpoint(Vec::new());
    let plan_hash = "velorix-logical-view-plan-sha256-v1:test";
    checkpoint.input_coverage = Some(
        RuntimeCheckpointInputCoverageV1 {
            schema_version: RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
            view_generation: 3,
            plan_hash: plan_hash.to_string(),
            input_catalog_epoch: 9,
            relations: vec![RuntimeCheckpointRelationCoverageV1 {
                relation_id: "purchases".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                relation_generation: 1,
                schema_fingerprint: format!("sha256:{}", "9".repeat(64)),
                partitions: vec![RuntimeCheckpointPartitionCoverageV1 {
                    stream_id: "test-stream".to_string(),
                    stream_generation: 1,
                    partition_id: 0,
                    partition_generation: 1,
                    covered_from_offset_inclusive: 0,
                    processed_offset_exclusive: 11,
                }],
            }],
        }
        .canonicalized()
        .unwrap(),
    );
    checkpoint.causal_cut = Some(
        CausalCutV1::from_input_coverage(
            checkpoint.input_coverage.as_ref().unwrap(),
            vec![CausalViewCursorV1 {
                input_edge: "upstream_orders->purchases_by_user".to_string(),
                producer_tenant_id: checkpoint.identity.tenant_id.clone(),
                producer_program_id: "upstream_orders".to_string(),
                producer_view_id: "upstream_orders".to_string(),
                producer_generation: 2,
                output_stream: "view/upstream_orders/generation/2/output/primary".to_string(),
                output_epoch: 7,
                commit_digest: format!("sha256:{}", "7".repeat(64)),
            }],
        )
        .unwrap(),
    );
    let output_schema = RelationSchema {
        relation_id: "purchases_by_user".to_string(),
        relation_name: "purchases_by_user".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        columns: vec![ColumnSchema {
            name: "user_id".to_string(),
            data_type: SqlDataType::Utf8,
            nullable: false,
        }],
        primary_key: vec!["user_id".to_string()],
    };
    let binding =
        published_relation_binding_v1("purchases_by_user", 3, plan_hash, &output_schema).unwrap();
    let empty_delta = ViewOutputDelta {
        view_id: "purchases_by_user".to_string(),
        schema_fingerprint: output_schema.schema_fingerprint.clone(),
        delta: DeltaBatch::default(),
    };
    let checkpoint_key = test_checkpoint_key(&checkpoint);

    let publications = standing_runtime_output_delta_records_for_checkpoint_with_binding(
        &checkpoint,
        "purchases_by_user",
        std::slice::from_ref(&empty_delta),
        &checkpoint_key,
        Some(&binding),
    )
    .unwrap();

    assert_eq!(publications.len(), 1);
    let record = &publications[0].delta_record;
    assert_eq!(record.schema_version, 2);
    assert_eq!(record.record_kind, "standing_runtime_output_commit_v1");
    assert_eq!(record.delta_row_count, 0);
    let commit = record.producer_commit.as_ref().unwrap();
    assert_eq!(commit.producer_view_generation, 3);
    assert_eq!(commit.producer_plan_hash, plan_hash);
    assert_eq!(commit.checkpoint_key, checkpoint_key.as_str());
    assert_eq!(
        commit.causal_cut_digest,
        checkpoint
            .causal_cut
            .as_ref()
            .unwrap()
            .stable_digest()
            .unwrap()
    );
    validate_standing_runtime_output_delta_record(&publications[0].delta_key, record).unwrap();

    let state_payload_key = ObjectKey::standing_runtime_state_payload(
        "tenant-a",
        "program-purchases",
        "purchases_by_user",
        checkpoint.logical_epoch,
        &checkpoint.state_root.content_hash,
    )
    .unwrap();
    let durable = standing_runtime_checkpoint_with_durable_publication_refs(
        &checkpoint,
        None,
        &publications,
        &state_payload_key,
    );
    assert_eq!(durable.output_manifest_refs.len(), 1);
    assert!(durable.output_manifest_refs[0].starts_with(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX));

    let missing_empty_commit = standing_runtime_output_delta_records_for_checkpoint_with_binding(
        &checkpoint,
        "purchases_by_user",
        &[],
        &checkpoint_key,
        Some(&binding),
    )
    .unwrap_err();
    assert_eq!(missing_empty_commit.status, StatusCode::BAD_REQUEST);

    let wrong_generation =
        published_relation_binding_v1("purchases_by_user", 4, plan_hash, &output_schema).unwrap();
    let generation_mismatch = standing_runtime_output_delta_records_for_checkpoint_with_binding(
        &checkpoint,
        "purchases_by_user",
        std::slice::from_ref(&empty_delta),
        &checkpoint_key,
        Some(&wrong_generation),
    )
    .unwrap_err();
    assert_eq!(generation_mismatch.status, StatusCode::BAD_REQUEST);

    let mut mutant = record.clone();
    mutant
        .producer_commit
        .as_mut()
        .unwrap()
        .checkpoint_content_hash = format!("sha256:{}", "f".repeat(64));
    assert!(
        validate_standing_runtime_output_delta_record(&publications[0].delta_key, &mutant).is_err()
    );
    for mutate in [
        |commit: &mut StandingRuntimeProducerCommitV1| {
            commit.causal_cut_digest = format!("sha256:{}", "e".repeat(64));
        },
        |commit: &mut StandingRuntimeProducerCommitV1| {
            commit.output_schema_hash = format!("sha256:{}", "d".repeat(64));
        },
        |commit: &mut StandingRuntimeProducerCommitV1| {
            commit.key_descriptor_hash = format!("sha256:{}", "c".repeat(64));
        },
        |commit: &mut StandingRuntimeProducerCommitV1| {
            commit.delta_codec_identity = "unknown-codec".to_string();
        },
    ] {
        let mut mutant = record.clone();
        mutate(mutant.producer_commit.as_mut().unwrap());
        assert!(
            validate_standing_runtime_output_delta_record(&publications[0].delta_key, &mutant,)
                .is_err()
        );
    }
}

#[tokio::test]
async fn standing_runtime_checkpoint_crash_matrix_keeps_previous_authoritative_pointer() {
    for failing_prefix in [
        "v1/standing-runtime-output-deltas/",
        "v1/standing-runtime-state-payloads/",
        "v1/standing-runtime-checkpoints/",
    ] {
        assert_checkpoint_publication_failure_keeps_previous_pointer(failing_prefix).await;
    }
}

#[tokio::test]
async fn mixed_source_view_causal_cut_survives_authoritative_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let state = test_api_state_with_store(Arc::clone(&store), "api-test-mixed-causal-cut-a", false)
        .await
        .with_meta_store(Arc::clone(&meta_store) as Arc<dyn MetaStore>);
    let (producer_checkpoint, producer_binding, producer_empty_delta) =
        test_checkpoint_with_named_published_relation_contract("upstream_orders", 2, "upstream");
    let producer_owner = state
        .acquire_standing_runtime_owner(&producer_checkpoint.identity, "upstream_orders")
        .await
        .unwrap()
        .unwrap();
    persist_standing_runtime_checkpoint(
        &state,
        "upstream_orders",
        &producer_checkpoint,
        std::slice::from_ref(&producer_empty_delta),
        StandingRuntimeCheckpointPersistContext::new(
            None,
            Vec::new(),
            Some(producer_owner.clone()),
        )
        .with_published_relation(Some(producer_binding.clone())),
        None,
    )
    .await
    .unwrap();
    let first_producer_record = read_latest_standing_runtime_checkpoint(
        &state,
        &producer_checkpoint.identity,
        "upstream_orders",
    )
    .await
    .unwrap()
    .unwrap();
    let (_, first_commit_record) = read_standing_runtime_output_delta_record(
        &state,
        test_output_commit_ref(&first_producer_record.checkpoint),
        "upstream_orders",
    )
    .await
    .unwrap();
    let first_cursor = CausalViewCursorV1 {
        input_edge: "upstream_orders->purchases_by_user".to_string(),
        producer_tenant_id: producer_checkpoint.identity.tenant_id.clone(),
        producer_program_id: producer_checkpoint.identity.program_id.clone(),
        producer_view_id: "upstream_orders".to_string(),
        producer_generation: producer_binding.producer_view_generation,
        output_stream: producer_binding.output_stream_id.clone(),
        output_epoch: producer_checkpoint.logical_epoch,
        commit_digest: first_commit_record
            .producer_commit
            .as_ref()
            .unwrap()
            .producer_commit_digest
            .clone(),
    };

    let mut next_producer = advanced_test_runtime_checkpoint(&producer_checkpoint, 1, "upstream-2");
    next_producer.input_coverage.as_mut().unwrap().relations[0].partitions[0]
        .processed_offset_exclusive += 1;
    persist_standing_runtime_checkpoint(
        &state,
        "upstream_orders",
        &next_producer,
        std::slice::from_ref(&producer_empty_delta),
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(producer_owner))
            .with_published_relation(Some(producer_binding.clone())),
        None,
    )
    .await
    .unwrap();
    let second_producer_record = read_latest_standing_runtime_checkpoint(
        &state,
        &producer_checkpoint.identity,
        "upstream_orders",
    )
    .await
    .unwrap()
    .unwrap();
    let (_, second_commit_record) = read_standing_runtime_output_delta_record(
        &state,
        test_output_commit_ref(&second_producer_record.checkpoint),
        "upstream_orders",
    )
    .await
    .unwrap();
    let advanced_cursor = CausalViewCursorV1 {
        output_epoch: next_producer.logical_epoch,
        commit_digest: second_commit_record
            .producer_commit
            .as_ref()
            .unwrap()
            .producer_commit_digest
            .clone(),
        ..first_cursor.clone()
    };

    let (checkpoint, published_relation, empty_output_delta) =
        test_checkpoint_with_published_relation_contract();
    let owner = state
        .acquire_standing_runtime_owner(&checkpoint.identity, "purchases_by_user")
        .await
        .unwrap()
        .unwrap();
    let mut first_context =
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner.clone()))
            .with_published_relation(Some(published_relation.clone()));
    first_context.direct_view_inputs = vec![StandingRuntimeDirectViewInputV1 {
        published_relation: producer_binding.clone(),
        cursor: first_cursor,
    }];
    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        std::slice::from_ref(&empty_output_delta),
        first_context,
        None,
    )
    .await
    .unwrap();

    let mut next_checkpoint = checkpoint.clone();
    next_checkpoint.logical_epoch += 1;
    next_checkpoint.output_frontiers[0].committed_epoch = next_checkpoint.logical_epoch;
    let mut next_context =
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner))
            .with_published_relation(Some(published_relation));
    next_context.direct_view_inputs = vec![StandingRuntimeDirectViewInputV1 {
        published_relation: producer_binding,
        cursor: advanced_cursor.clone(),
    }];
    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &next_checkpoint,
        std::slice::from_ref(&empty_output_delta),
        next_context,
        None,
    )
    .await
    .unwrap();

    drop(state);
    let restarted = test_api_state_with_store(store, "api-test-mixed-causal-cut-b", false)
        .await
        .with_meta_store(meta_store as Arc<dyn MetaStore>);
    let restored = read_latest_standing_runtime_checkpoint(
        &restarted,
        &checkpoint.identity,
        "purchases_by_user",
    )
    .await
    .unwrap()
    .unwrap();
    restored
        .checkpoint
        .validate_identity(&checkpoint.identity)
        .unwrap();
    let cut = restored.checkpoint.causal_cut.as_ref().unwrap();
    assert_eq!(cut.direct_source_frontiers.len(), 1);
    assert_eq!(cut.direct_view_cursors, vec![advanced_cursor]);
    let commit_ref = test_output_commit_ref(&restored.checkpoint).to_owned();
    let (_, commit_record) =
        read_standing_runtime_output_delta_record(&restarted, &commit_ref, "purchases_by_user")
            .await
            .unwrap();
    assert_eq!(
        commit_record
            .producer_commit
            .as_ref()
            .unwrap()
            .causal_cut_digest,
        cut.stable_digest().unwrap()
    );
    let mut causal_mutant = restored.clone();
    causal_mutant
        .checkpoint
        .causal_cut
        .as_mut()
        .unwrap()
        .direct_view_cursors[0]
        .output_epoch += 1;
    assert!(
        validate_standing_runtime_checkpoint_output_manifest_records(&restarted, &causal_mutant,)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn authoritative_view_cursor_resolution_fails_closed_at_every_trust_boundary() {
    if let Some(root) = std::env::var_os("VELORIX_CURSOR_SANDBOX") {
        profile_preserved_authoritative_cursor_checkpoint(std::path::Path::new(&root)).await;
    }
    let counts = ObjectStoreAccessCounts::default();
    let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        Arc::new(InMemory::new()),
        counts.clone(),
    ));
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let state = test_api_state_with_store(
        Arc::clone(&store),
        "api-test-authoritative-view-cursor",
        false,
    )
    .await
    .with_meta_store(Arc::clone(&meta_store) as Arc<dyn MetaStore>);
    let (producer_checkpoint, producer_binding, producer_empty_delta) =
        test_checkpoint_with_named_published_relation_contract("upstream_orders", 2, "upstream");
    let producer_owner = state
        .acquire_standing_runtime_owner(&producer_checkpoint.identity, "upstream_orders")
        .await
        .unwrap()
        .unwrap();
    persist_standing_runtime_checkpoint(
        &state,
        "upstream_orders",
        &producer_checkpoint,
        std::slice::from_ref(&producer_empty_delta),
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(producer_owner))
            .with_published_relation(Some(producer_binding.clone())),
        None,
    )
    .await
    .unwrap();
    let producer_record = read_latest_standing_runtime_checkpoint(
        &state,
        &producer_checkpoint.identity,
        "upstream_orders",
    )
    .await
    .unwrap()
    .unwrap();
    let commit_ref = test_output_commit_ref(&producer_record.checkpoint).to_owned();
    let (_, commit_record) =
        read_standing_runtime_output_delta_record(&state, &commit_ref, "upstream_orders")
            .await
            .unwrap();
    let cursor = CausalViewCursorV1 {
        input_edge: "upstream_orders->purchases_by_user".to_string(),
        producer_tenant_id: producer_checkpoint.identity.tenant_id.clone(),
        producer_program_id: producer_checkpoint.identity.program_id.clone(),
        producer_view_id: "upstream_orders".to_string(),
        producer_generation: producer_binding.producer_view_generation,
        output_stream: producer_binding.output_stream_id.clone(),
        output_epoch: producer_checkpoint.logical_epoch,
        commit_digest: commit_record
            .producer_commit
            .as_ref()
            .unwrap()
            .producer_commit_digest
            .clone(),
    };

    counts.clear();
    resolve_authoritative_view_cursor(
        &state,
        &producer_checkpoint.identity.tenant_id,
        &producer_binding,
        &cursor,
    )
    .await
    .unwrap();
    let commit_path = commit_ref
        .strip_prefix(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX)
        .unwrap();
    assert_eq!(
        counts
            .get_paths()
            .iter()
            .filter(|path| path.as_str() == commit_path)
            .count(),
        1,
        "cursor resolution must reuse its already validated producer commit"
    );

    let (_, proof) = checkpoint_publication::read_authoritative_cursor_checkpoint(
        &state,
        &producer_checkpoint.identity.tenant_id,
        &producer_binding,
        &cursor,
    )
    .await
    .unwrap();
    let input = StandingRuntimeDirectViewInputV1 {
        published_relation: producer_binding.clone(),
        cursor: cursor.clone(),
    };
    counts.clear();
    checkpoint_publication::resolve_authoritative_direct_view_inputs(
        &state,
        &producer_checkpoint.identity.tenant_id,
        std::slice::from_ref(&input),
        std::slice::from_ref(&proof),
    )
    .await
    .unwrap();
    assert!(
        counts.get_paths().is_empty(),
        "unchanged head must reuse request-local validation"
    );
    let mut changed_input = input.clone();
    changed_input.cursor.commit_digest = format!("sha256:{}", "0".repeat(64));
    assert!(
        checkpoint_publication::resolve_authoritative_direct_view_inputs(
            &state,
            &producer_checkpoint.identity.tenant_id,
            &[changed_input],
            std::slice::from_ref(&proof),
        )
        .await
        .is_err()
    );
    let mut changed_binding = input.clone();
    changed_binding
        .published_relation
        .producer_plan_hash
        .push_str("-different");
    assert!(
        checkpoint_publication::resolve_authoritative_direct_view_inputs(
            &state,
            &producer_checkpoint.identity.tenant_id,
            &[changed_binding],
            std::slice::from_ref(&proof),
        )
        .await
        .is_err()
    );

    let mut state_without_meta = test_api_state_with_store(
        Arc::clone(&store),
        "api-test-authoritative-view-cursor-no-meta",
        false,
    )
    .await;
    state_without_meta.meta_store = None;
    state_without_meta.view_bootstrap_meta_store = None;
    // Without a meta store the resolver falls back to the object-store latest
    // checkpoint cache; the cursor is still validated against the durable
    // producer commit records.
    resolve_authoritative_view_cursor(
        &state_without_meta,
        &producer_checkpoint.identity.tenant_id,
        &producer_binding,
        &cursor,
    )
    .await
    .unwrap();

    let state_without_pointer = test_api_state_with_store(
        Arc::clone(&store),
        "api-test-authoritative-view-cursor-no-pointer",
        false,
    )
    .await
    .with_meta_store(Arc::new(InMemoryMetaStore::default()));
    let error = resolve_authoritative_view_cursor(
        &state_without_pointer,
        &producer_checkpoint.identity.tenant_id,
        &producer_binding,
        &cursor,
    )
    .await
    .unwrap_err();
    assert!(error
        .message
        .contains("no authoritative producer checkpoint"));
    assert!(
        checkpoint_publication::resolve_authoritative_direct_view_inputs(
            &state_without_pointer,
            &producer_checkpoint.identity.tenant_id,
            std::slice::from_ref(&input),
            std::slice::from_ref(&proof),
        )
        .await
        .is_err()
    );

    let mut generation_mismatch = cursor.clone();
    generation_mismatch.producer_generation += 1;
    let mut tenant_mismatch = cursor.clone();
    tenant_mismatch.producer_tenant_id.push_str("-other");
    let mut program_mismatch = cursor.clone();
    program_mismatch.producer_program_id.push_str("-other");
    let mut view_mismatch = cursor.clone();
    view_mismatch.producer_view_id.push_str("-other");
    let mut stream_mismatch = cursor.clone();
    stream_mismatch.output_stream.push_str("-other");
    let mut digest_mismatch = cursor.clone();
    digest_mismatch.commit_digest = format!("sha256:{}", "0".repeat(64));
    let mut future_cursor = cursor.clone();
    future_cursor.output_epoch += 1;
    for mutant in [
        generation_mismatch,
        tenant_mismatch,
        program_mismatch,
        view_mismatch,
        stream_mismatch,
        digest_mismatch,
        future_cursor,
    ] {
        assert!(resolve_authoritative_view_cursor(
            &state,
            &producer_checkpoint.identity.tenant_id,
            &producer_binding,
            &mutant,
        )
        .await
        .is_err());
    }

    let mut plan_mismatch = producer_binding.clone();
    plan_mismatch.producer_plan_hash.push_str("-other");
    assert!(resolve_authoritative_view_cursor(
        &state,
        &producer_checkpoint.identity.tenant_id,
        &plan_mismatch,
        &cursor,
    )
    .await
    .is_err());

    let mut missing_lineage = cursor.clone();
    missing_lineage.output_epoch = missing_lineage.output_epoch.saturating_sub(1);
    assert!(resolve_authoritative_view_cursor(
        &state,
        &producer_checkpoint.identity.tenant_id,
        &producer_binding,
        &missing_lineage,
    )
    .await
    .is_err());

    let mut advanced = producer_checkpoint.clone();
    advanced.logical_epoch += 1;
    let owner = state
        .acquire_standing_runtime_owner(&advanced.identity, "upstream_orders")
        .await
        .unwrap()
        .unwrap();
    persist_standing_runtime_checkpoint(
        &state,
        "upstream_orders",
        &advanced,
        std::slice::from_ref(&producer_empty_delta),
        StandingRuntimeCheckpointPersistContext::new(
            Some(producer_record.clone()),
            Vec::new(),
            Some(owner),
        )
        .with_published_relation(Some(producer_binding.clone())),
        None,
    )
    .await
    .unwrap();
    counts.clear();
    checkpoint_publication::resolve_authoritative_direct_view_inputs(
        &state,
        &producer_checkpoint.identity.tenant_id,
        std::slice::from_ref(&input),
        std::slice::from_ref(&proof),
    )
    .await
    .unwrap();
    assert!(
        !counts.get_paths().is_empty(),
        "advanced head must validate fresh lineage"
    );
    let state_path = Path::from(producer_record.checkpoint.state_root.object_key.clone());
    let original_state = state
        .store
        .get(&state_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    state
        .store
        .put(
            &state_path,
            bytes::Bytes::from_static(b"corrupt-state").into(),
        )
        .await
        .unwrap();
    assert!(
        checkpoint_publication::resolve_authoritative_direct_view_inputs(
            &state,
            &producer_checkpoint.identity.tenant_id,
            std::slice::from_ref(&input),
            std::slice::from_ref(&proof),
        )
        .await
        .is_err(),
        "changed head cannot reuse a proof to hide corrupt state"
    );
    state
        .store
        .put(&state_path, original_state.into())
        .await
        .unwrap();
    let commit_key = commit_ref
        .strip_prefix(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX)
        .unwrap();
    state.store.delete(&Path::from(commit_key)).await.unwrap();
    assert!(resolve_authoritative_view_cursor(
        &state,
        &producer_checkpoint.identity.tenant_id,
        &producer_binding,
        &cursor,
    )
    .await
    .is_err());
}

// Additional opt-in profiling on exact private gate bytes. Ordinary CI still
// runs all trust-boundary assertions in the test above; this is not an oracle.
async fn profile_preserved_authoritative_cursor_checkpoint(root: &std::path::Path) {
    let descriptor: Value =
        serde_json::from_slice(&std::fs::read(root.join("sandbox.json")).unwrap()).unwrap();
    let pointer: StandingRuntimeCheckpointPointer =
        serde_json::from_value(descriptor["checkpoint_pointer"].clone()).unwrap();
    let runtime: MaterializedViewRuntimeBinding =
        serde_json::from_value(descriptor["runtime_binding"].clone()).unwrap();
    let binding = runtime.published_relations.first().unwrap().clone();
    for object in descriptor["objects"].as_array().unwrap() {
        let key = object["key"].as_str().unwrap();
        assert!(std::path::Path::new(key)
            .components()
            .all(|part| matches!(part, std::path::Component::Normal(_))));
        let bytes = std::fs::read(root.join("objects").join(key)).unwrap();
        assert_eq!(bytes.len() as u64, object["bytes"].as_u64().unwrap());
        assert_eq!(
            stable_bytes_hash(&bytes).strip_prefix("sha256:").unwrap(),
            object["sha256"].as_str().unwrap()
        );
    }
    let counts = ObjectStoreAccessCounts::default();
    let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(root.join("objects")).unwrap(),
        ),
        counts.clone(),
    ));
    let meta = Arc::new(InMemoryMetaStore::default());
    let acquired = meta
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: pointer.tenant_id.clone(),
            program_id: pointer.program_id.clone(),
            view_id: pointer.view_id.clone(),
            owner_id: "isolated-cursor-profile".into(),
            ttl_ms: 30_000,
        })
        .await
        .unwrap();
    let AcquireStandingRuntimeOwnerOutcome::Acquired(owner) = acquired else {
        panic!("isolated metadata owner");
    };
    assert_eq!(
        meta.publish_standing_runtime_checkpoint(meta::PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: pointer.clone(),
            owner: standing_runtime_owner_token_from_claim(&owner),
            expected_relation_source_cuts: None,
        })
        .await
        .unwrap(),
        meta::PublishStandingRuntimeCheckpointOutcome::Published
    );
    let state = test_api_state_with_store(store, "isolated-cursor-profile", false)
        .await
        .with_meta_store(meta);
    let commit_ref = pointer
        .output_manifest_refs
        .iter()
        .find(|reference| reference.starts_with(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX))
        .unwrap();
    let (_, delta) =
        read_standing_runtime_output_delta_record(&state, commit_ref, &pointer.view_id)
            .await
            .unwrap();
    let cursor = CausalViewCursorV1 {
        input_edge: "preserved-actual-producer->isolated-profile".into(),
        producer_tenant_id: pointer.tenant_id.clone(),
        producer_program_id: pointer.program_id.clone(),
        producer_view_id: pointer.view_id.clone(),
        producer_generation: binding.producer_view_generation,
        output_stream: binding.output_stream_id.clone(),
        output_epoch: pointer.logical_epoch,
        commit_digest: delta.producer_commit.unwrap().producer_commit_digest,
    };
    let started = std::time::Instant::now();
    let (_, proof) = checkpoint_publication::read_authoritative_cursor_checkpoint(
        &state,
        &pointer.tenant_id,
        &binding,
        &cursor,
    )
    .await
    .unwrap();
    eprintln!(
        "preserved_cursor_profile full_validation_ms={} objects={} source_epoch={}",
        started.elapsed().as_millis(),
        descriptor["object_count"],
        pointer.logical_epoch
    );
    counts.clear();
    let started = std::time::Instant::now();
    checkpoint_publication::resolve_authoritative_direct_view_inputs(
        &state,
        &pointer.tenant_id,
        &[StandingRuntimeDirectViewInputV1 {
            published_relation: binding,
            cursor,
        }],
        &[proof],
    )
    .await
    .unwrap();
    assert!(counts.get_paths().is_empty());
    assert!(started.elapsed() < Duration::from_secs(10));
    eprintln!(
        "preserved_cursor_profile unchanged_head_resolution_ms={} object_gets=0",
        started.elapsed().as_millis()
    );
}

#[tokio::test]
async fn authoritative_view_cursor_resolution_bounds_checkpoint_lineage_work() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-view-cursor-budget", false)
        .await
        .with_meta_store(Arc::new(InMemoryMetaStore::default()));
    let (mut checkpoint, binding, empty_delta) =
        test_checkpoint_with_named_published_relation_contract("upstream_orders", 2, "upstream");
    let owner = state
        .acquire_standing_runtime_owner(&checkpoint.identity, "upstream_orders")
        .await
        .unwrap()
        .unwrap();
    persist_standing_runtime_checkpoint(
        &state,
        "upstream_orders",
        &checkpoint,
        std::slice::from_ref(&empty_delta),
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner.clone()))
            .with_published_relation(Some(binding.clone())),
        None,
    )
    .await
    .unwrap();
    let first_record =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "upstream_orders")
            .await
            .unwrap()
            .unwrap();
    let (_, first_commit) = read_standing_runtime_output_delta_record(
        &state,
        test_output_commit_ref(&first_record.checkpoint),
        "upstream_orders",
    )
    .await
    .unwrap();
    let first_cursor = CausalViewCursorV1 {
        input_edge: "upstream_orders->consumer".to_string(),
        producer_tenant_id: checkpoint.identity.tenant_id.clone(),
        producer_program_id: checkpoint.identity.program_id.clone(),
        producer_view_id: "upstream_orders".to_string(),
        producer_generation: binding.producer_view_generation,
        output_stream: binding.output_stream_id.clone(),
        output_epoch: checkpoint.logical_epoch,
        commit_digest: first_commit
            .producer_commit
            .as_ref()
            .unwrap()
            .producer_commit_digest
            .clone(),
    };

    for epoch in 1..=8 {
        let mut next = advanced_test_runtime_checkpoint(&checkpoint, 1, &format!("epoch-{epoch}"));
        next.input_coverage.as_mut().unwrap().relations[0].partitions[0]
            .processed_offset_exclusive += 1;
        persist_standing_runtime_checkpoint(
            &state,
            "upstream_orders",
            &next,
            std::slice::from_ref(&empty_delta),
            StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner.clone()))
                .with_published_relation(Some(binding.clone())),
            None,
        )
        .await
        .unwrap();
        checkpoint = next;
    }

    let error = resolve_authoritative_view_cursor(
        &state,
        &checkpoint.identity.tenant_id,
        &binding,
        &first_cursor,
    )
    .await
    .unwrap_err();
    assert!(error.message.contains("lineage budget"));
}

#[tokio::test]
async fn orphan_output_commit_is_not_authoritative_progress() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let failing_store = Arc::new(ArmedPrefixFailingObjectStore::new(Arc::clone(&inner)));
    let state = test_api_state_with_store(
        Arc::clone(&failing_store) as Arc<dyn ObjectStore>,
        "api-test-orphan-output-commit",
        false,
    )
    .await
    .with_meta_store(Arc::new(InMemoryMetaStore::default()));
    let (checkpoint, published_relation, empty_output_delta) =
        test_checkpoint_with_published_relation_contract();
    let owner = state
        .acquire_standing_runtime_owner(&checkpoint.identity, "purchases_by_user")
        .await
        .unwrap()
        .unwrap();
    failing_store.arm("v1/standing-runtime-state-payloads/");
    let error = persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        std::slice::from_ref(&empty_output_delta),
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner))
            .with_published_relation(Some(published_relation)),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
    let orphan_count = inner
        .list(Some(&Path::from("v1/standing-runtime-output-deltas")))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .filter(Result::is_ok)
        .count();
    assert_eq!(orphan_count, 1, "the unreferenced commit object must exist");
    assert!(read_latest_standing_runtime_checkpoint(
        &state,
        &checkpoint.identity,
        "purchases_by_user",
    )
    .await
    .unwrap()
    .is_none());
}

async fn assert_checkpoint_publication_failure_keeps_previous_pointer(failing_prefix: &str) {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let failing_store = Arc::new(ArmedPrefixFailingObjectStore::new(inner));
    let state = test_api_state_with_store(
        Arc::clone(&failing_store) as Arc<dyn ObjectStore>,
        "api-test-checkpoint-crash-matrix",
        false,
    )
    .await
    .with_meta_store(Arc::new(InMemoryMetaStore::default()));
    let (checkpoint, published_relation, empty_output_delta) =
        test_checkpoint_with_published_relation_contract();
    let owner = state
        .acquire_standing_runtime_owner(&checkpoint.identity, "purchases_by_user")
        .await
        .unwrap()
        .expect("test metadata store should grant publication ownership");

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        std::slice::from_ref(&empty_output_delta),
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner.clone()))
            .with_published_relation(Some(published_relation.clone())),
        None,
    )
    .await
    .unwrap();

    let mut next = checkpoint.clone();
    next.logical_epoch += 1;
    next.input_frontiers[0].committed_offset_exclusive += 1;
    next.output_frontiers[0].committed_epoch = next.logical_epoch;
    next.input_coverage.as_mut().unwrap().relations[0].partitions[0].processed_offset_exclusive +=
        1;
    let next_payload = serde_json::json!({
        "schema_version": 1,
        "published_output": { "records": [] },
        "crash_matrix_epoch": next.logical_epoch,
    });
    let next_payload = velorix_core::arrow_state::to_state(&next_payload).unwrap();
    next.state_root.content_hash = stable_bytes_hash(next_payload.as_bytes());
    next.state_payload = Some(RuntimeCheckpointStatePayload {
        codec_identity: next.checkpoint_codec_identity.clone(),
        payload: next_payload,
    });
    let output_delta = ViewOutputDelta {
        view_id: "purchases_by_user".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        delta: DeltaBatch::from_records([DeltaRecord::new(
            DeltaKey::from_json(json!("alice")),
            DeltaValue::from_json(json!({ "count": 1, "sum": 10 })),
            1,
        )]),
    };

    failing_store.arm(failing_prefix);
    let error = persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &next,
        std::slice::from_ref(&output_delta),
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner.clone()))
            .with_published_relation(Some(published_relation.clone())),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);

    let authoritative =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap()
            .expect("the previous checkpoint should remain authoritative");
    assert_eq!(
        authoritative.checkpoint.logical_epoch,
        checkpoint.logical_epoch
    );
    assert_eq!(
        authoritative.checkpoint.state_root.content_hash,
        checkpoint.state_root.content_hash
    );

    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &next,
        std::slice::from_ref(&output_delta),
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner))
            .with_published_relation(Some(published_relation)),
        None,
    )
    .await
    .expect("retry after every pre-pointer crash window must converge");
    let committed =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(committed.checkpoint.logical_epoch, next.logical_epoch);
    assert_eq!(committed.checkpoint.output_manifest_refs.len(), 2);
    test_output_commit_ref(&committed.checkpoint);
}

#[tokio::test]
async fn standing_runtime_checkpoint_pointer_conflict_keeps_winning_checkpoint_after_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let state = test_api_state_with_store(
        Arc::clone(&store),
        "api-test-checkpoint-pointer-conflict",
        false,
    )
    .await
    .with_meta_store(Arc::clone(&meta_store) as Arc<dyn MetaStore>);
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let owner = state
        .acquire_standing_runtime_owner(&checkpoint.identity, "purchases_by_user")
        .await
        .unwrap()
        .expect("test metadata store should grant publication ownership");
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
    let baseline =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap()
            .unwrap();

    let winner = advanced_test_runtime_checkpoint(&checkpoint, 1, "winner");
    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &winner,
        &[],
        StandingRuntimeCheckpointPersistContext::new(
            Some(baseline.clone()),
            Vec::new(),
            Some(owner.clone()),
        ),
        None,
    )
    .await
    .unwrap();

    let loser = advanced_test_runtime_checkpoint(&checkpoint, 2, "loser");
    let conflict = persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &loser,
        &[],
        StandingRuntimeCheckpointPersistContext::new(Some(baseline), Vec::new(), Some(owner)),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(conflict.status, StatusCode::CONFLICT);

    let restarted = test_api_state_with_store(
        Arc::clone(&store),
        "api-test-checkpoint-pointer-conflict-restarted",
        false,
    )
    .await
    .with_meta_store(meta_store as Arc<dyn MetaStore>);
    let authoritative = read_latest_standing_runtime_checkpoint(
        &restarted,
        &checkpoint.identity,
        "purchases_by_user",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(authoritative.checkpoint.logical_epoch, winner.logical_epoch);
    assert_eq!(
        authoritative.checkpoint.state_root.content_hash,
        winner.state_root.content_hash
    );
}

#[tokio::test]
async fn standing_runtime_checkpoint_publish_uses_metadata_as_authoritative() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::clone(&inner);
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let state = test_api_state_with_store(store, "api-test-metadata-authoritative", false)
        .await
        .with_meta_store(Arc::clone(&meta_store) as Arc<dyn MetaStore>);
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let owner = state
        .acquire_standing_runtime_owner(&checkpoint.identity, "purchases_by_user")
        .await
        .unwrap()
        .expect("test metadata store should grant publication ownership");
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

    let next = advanced_test_runtime_checkpoint(&checkpoint, 1, "metadata-authoritative");
    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &next,
        &[],
        StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner)),
        None,
    )
    .await
    .expect("metadata pointer publication should succeed");

    let restarted =
        test_api_state_with_store(inner, "api-test-metadata-authoritative-restarted", false)
            .await
            .with_meta_store(meta_store as Arc<dyn MetaStore>);
    let authoritative = read_latest_standing_runtime_checkpoint(
        &restarted,
        &checkpoint.identity,
        "purchases_by_user",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(authoritative.checkpoint.logical_epoch, next.logical_epoch);
    assert_eq!(
        authoritative.checkpoint.state_root.content_hash,
        next.state_root.content_hash
    );
}

fn advanced_test_runtime_checkpoint(
    checkpoint: &RuntimeCheckpoint,
    epoch_increment: u64,
    marker: &str,
) -> RuntimeCheckpoint {
    let mut next = checkpoint.clone();
    next.logical_epoch += epoch_increment;
    next.input_frontiers[0].committed_offset_exclusive += epoch_increment;
    next.output_frontiers[0].committed_epoch = next.logical_epoch;
    let payload = serde_json::json!({
        "schema_version": 1,
        "published_output": { "records": [] },
        "test_marker": marker,
    });
    let payload = velorix_core::arrow_state::to_state(&payload).unwrap();
    next.state_root.content_hash = stable_bytes_hash(payload.as_bytes());
    next.state_payload = Some(RuntimeCheckpointStatePayload {
        codec_identity: next.checkpoint_codec_identity.clone(),
        payload,
    });
    next
}

fn test_checkpoint_with_published_relation_contract() -> (
    RuntimeCheckpoint,
    PublishedRelationBindingV1,
    ViewOutputDelta,
) {
    test_checkpoint_with_named_published_relation_contract("purchases_by_user", 1, "crash-matrix")
}

fn test_checkpoint_with_named_published_relation_contract(
    view_id: &str,
    producer_generation: u64,
    plan_tag: &str,
) -> (
    RuntimeCheckpoint,
    PublishedRelationBindingV1,
    ViewOutputDelta,
) {
    let mut checkpoint = test_runtime_checkpoint(Vec::new());
    checkpoint.identity.program_id = view_id.to_string();
    checkpoint.identity.view_ids = vec![view_id.to_string()];
    checkpoint.output_frontiers[0].view_id = view_id.to_string();
    let plan_hash = format!("velorix-logical-view-plan-sha256-v1:{plan_tag}");
    checkpoint.input_coverage = Some(
        RuntimeCheckpointInputCoverageV1 {
            schema_version: RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
            view_generation: producer_generation,
            plan_hash: plan_hash.clone(),
            input_catalog_epoch: 1,
            relations: vec![RuntimeCheckpointRelationCoverageV1 {
                relation_id: "purchases".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                relation_generation: 1,
                schema_fingerprint: format!("sha256:{}", "9".repeat(64)),
                partitions: vec![RuntimeCheckpointPartitionCoverageV1 {
                    stream_id: "test-stream".to_string(),
                    stream_generation: 1,
                    partition_id: 0,
                    partition_generation: 1,
                    covered_from_offset_inclusive: 0,
                    processed_offset_exclusive: 11,
                }],
            }],
        }
        .canonicalized()
        .unwrap(),
    );
    let output_schema = RelationSchema {
        relation_id: view_id.to_string(),
        relation_name: view_id.to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        columns: vec![ColumnSchema {
            name: "user_id".to_string(),
            data_type: SqlDataType::Utf8,
            nullable: false,
        }],
        primary_key: vec!["user_id".to_string()],
    };
    let binding =
        published_relation_binding_v1(view_id, producer_generation, &plan_hash, &output_schema)
            .unwrap();
    let output_delta = ViewOutputDelta {
        view_id: view_id.to_string(),
        schema_fingerprint: output_schema.schema_fingerprint,
        delta: DeltaBatch::default(),
    };
    (checkpoint, binding, output_delta)
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
        test_checkpoint_persist_context(&state, &checkpoint, "purchases_by_user").await,
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
    let record = standing_runtime_checkpoint_record_from_slice(&checkpoint_bytes).unwrap();
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
        test_checkpoint_persist_context(&state, &checkpoint, "purchases_by_user").await,
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
    let record = standing_runtime_checkpoint_record_from_slice(&checkpoint_bytes).unwrap();
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
    let mut delta_record =
        output_delta_record::decode(&delta_bytes, state.max_standing_runtime_state_payload_bytes)
            .unwrap();
    delta_record.output_delta = json!({"records": []});
    state
        .store
        .put(
            &Path::from(delta_key),
            bytes::Bytes::from(
                output_delta_record::encode(
                    &delta_record,
                    state.max_standing_runtime_state_payload_bytes,
                )
                .unwrap(),
            )
            .into(),
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
    let mut full_value = serde_json::to_value(&full_output).unwrap();
    full_value.sort_all_objects();
    let full_hash = stable_bytes_hash(&serde_json::to_vec(&full_value).unwrap());
    let mut pages = Vec::new();
    for (index, output) in page_outputs.into_iter().enumerate() {
        let mut output_value = serde_json::to_value(output).unwrap();
        output_value.sort_all_objects();
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
            output_encoding: materialized_output_pages::GENERIC_ARROW_CODEC.to_string(),
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
            output_encoding: materialized_output_pages::GENERIC_ARROW_CODEC.to_string(),
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
        output_encoding: materialized_output_pages::GENERIC_ARROW_CODEC.to_string(),
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
async fn standing_runtime_output_compaction_crash_windows_publish_only_complete_snapshots() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let failing_store = Arc::new(ArmedPrefixFailingObjectStore::new(Arc::clone(&inner)));
    let state = test_api_state_with_store(
        Arc::clone(&failing_store) as Arc<dyn ObjectStore>,
        "api-test-output-compaction-crash-windows",
        false,
    )
    .await;
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
    let mut full_value = serde_json::to_value(&full_output).unwrap();
    full_value.sort_all_objects();
    let full_hash = stable_bytes_hash(&serde_json::to_vec(&full_value).unwrap());
    let mut pages = Vec::new();
    for (index, output) in page_outputs.into_iter().enumerate() {
        let mut output_value = serde_json::to_value(output).unwrap();
        output_value.sort_all_objects();
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
            output_encoding: materialized_output_pages::GENERIC_ARROW_CODEC.to_string(),
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
            output_encoding: materialized_output_pages::GENERIC_ARROW_CODEC.to_string(),
            source_kind: "standing_runtime_checkpoint_published_output".to_string(),
            published_output: output_value,
        };
        put_standing_runtime_output_page(&state, &page_key, &page_record)
            .await
            .unwrap();
        pages.push(page_ref);
    }
    let fragmented = StandingRuntimeOutputManifestRecord {
        schema_version: 1,
        record_kind: "standing_runtime_output_manifest_v1".to_string(),
        tenant_id: "tenant-a".to_string(),
        program_id: "program-purchases".to_string(),
        view_id: "purchases_by_user".to_string(),
        checkpoint_key: checkpoint_key.as_str().to_string(),
        logical_epoch: checkpoint.logical_epoch,
        checkpoint_content_hash: checkpoint.state_root.content_hash.clone(),
        output_content_hash: full_hash.clone(),
        output_encoding: materialized_output_pages::GENERIC_ARROW_CODEC.to_string(),
        output_row_count: 2,
        source_kind: "standing_runtime_checkpoint_published_output".to_string(),
        pages,
        published_output: full_value,
    };
    let compacted_manifest_key = ObjectKey::standing_runtime_output_manifest(
        "tenant-a",
        "program-purchases",
        "purchases_by_user",
        checkpoint.logical_epoch,
        &full_hash,
    )
    .unwrap();

    failing_store.arm("v1/standing-runtime-output-pages/");
    let page_error = compact_standing_runtime_output_manifest(&state, &record, &fragmented)
        .await
        .unwrap_err();
    assert_eq!(page_error.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(state
        .store
        .get(&Path::from(compacted_manifest_key.as_str()))
        .await
        .is_err());

    failing_store.arm("v1/standing-runtime-output-manifests/");
    let manifest_error = compact_standing_runtime_output_manifest(&state, &record, &fragmented)
        .await
        .unwrap_err();
    assert_eq!(manifest_error.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(state
        .store
        .get(&Path::from(compacted_manifest_key.as_str()))
        .await
        .is_err());

    let compacted = compact_standing_runtime_output_manifest(&state, &record, &fragmented)
        .await
        .unwrap();
    let restarted = test_api_state_with_store(
        Arc::clone(&failing_store) as Arc<dyn ObjectStore>,
        "api-test-output-compaction-crash-windows-restarted",
        false,
    )
    .await;
    let restored = standing_runtime_published_output_from_manifest_page(
        &restarted,
        &compacted.manifest_record,
    )
    .await
    .unwrap();
    assert_eq!(restored, full_output);
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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

    let active = read_test_active_view(&state, "scores_compact_no_side_effect")
        .await
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let record =
        read_latest_standing_runtime_checkpoint(&state, identity, "scores_compact_no_side_effect")
            .await
            .unwrap()
            .unwrap();
    assert_snapshot_and_commit_refs(&record.checkpoint.output_manifest_refs);
    let manifest_ref = record
        .checkpoint
        .output_manifest_refs
        .iter()
        .find(|reference| reference.starts_with(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX))
        .expect("authoritative checkpoint references its published output manifest")
        .clone();

    let response = compact_view_output_once(&state, "scores_compact_no_side_effect", "sync")
        .await
        .unwrap();

    assert_eq!(response.outcome, "compacted");
    assert_eq!(response.compacted_manifests, 1);
    let latest =
        read_latest_standing_runtime_checkpoint(&state, identity, "scores_compact_no_side_effect")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        latest.checkpoint.output_manifest_refs,
        record.checkpoint.output_manifest_refs
    );
    let (_, manifest) = checkpoint_publication::read_standing_runtime_output_manifest_record(
        &state,
        &manifest_ref,
        "scores_compact_no_side_effect",
    )
    .await
    .unwrap();
    assert_eq!(manifest.pages.len(), response.after_pages);
    for page in &manifest.pages {
        checkpoint_publication::read_standing_runtime_output_page_record(
            &state,
            page,
            "scores_compact_no_side_effect",
        )
        .await
        .unwrap();
    }
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
    let corrupt_bytes = materialized_output_pages::encode_page(&corrupt).unwrap();
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
        test_checkpoint_persist_context(&state, &checkpoint, "purchases_by_user").await,
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
    let raw_record = standing_runtime_checkpoint_record_from_slice(&checkpoint_bytes).unwrap();
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
    assert!(serde_json::from_slice::<Value>(&state_bytes).is_err());
    let state_record =
        checkpoint_arrow::decode(&state_bytes, state.max_standing_runtime_state_payload_bytes)
            .unwrap();
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
        test_checkpoint_persist_context(&state, &checkpoint, "purchases_by_user").await,
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
async fn standing_runtime_checkpoint_read_rejects_legacy_identity_keys() {
    let state = test_api_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    persist_standing_runtime_checkpoint(
        &state,
        "purchases_by_user",
        &checkpoint,
        &[],
        test_checkpoint_persist_context(&state, &checkpoint, "purchases_by_user").await,
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
    let mut record: Value = velorix_core::arrow_state::from_state(
        &velorix_core::arrow_state::ArrowState::from_ipc(
            &checkpoint_bytes,
            checkpoint_record::MAX_ENCODED_RECORD_BYTES,
        )
        .unwrap(),
    )
    .unwrap();
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
    let metadata = velorix_core::arrow_state::ArrowState::metadata(
        &checkpoint_bytes,
        checkpoint_record::MAX_ENCODED_RECORD_BYTES,
    )
    .unwrap();
    let arrow_state = velorix_core::arrow_state::ArrowState::from_serializable_bounded(
        &record,
        checkpoint_record::MAX_ENCODED_RECORD_BYTES,
    )
    .unwrap();
    let checkpoint_bytes = velorix_core::arrow_state::ArrowState::replace_metadata(
        arrow_state.as_bytes(),
        metadata,
        checkpoint_record::MAX_ENCODED_RECORD_BYTES,
    )
    .unwrap();
    object_store::ObjectStoreExt::put(
        &*state.store,
        &Path::from(checkpoint_key.as_str()),
        checkpoint_bytes.into(),
    )
    .await
    .unwrap();

    // This malformed legacy alias payload is deliberately discovered without
    // the metadata pointer so the rejection exercises checkpoint decoding.
    let mut state_without_metadata = state.clone();
    state_without_metadata.meta_store = None;
    let error = read_latest_standing_runtime_checkpoint(
        &state_without_metadata,
        &checkpoint.identity,
        "purchases_by_user",
    )
    .await
    .unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}

#[test]
fn standing_runtime_replay_plan_uses_input_frontier_when_replay_checkpoints_are_absent() {
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let record = StandingRuntimeCheckpointRecord {
        published_input_bootstrap: None,
        demand_materialization: None,
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
fn empty_checkpoint_frontiers_require_empty_replay_claims() {
    let mut checkpoint = test_runtime_checkpoint(Vec::new());
    checkpoint.input_frontiers.clear();
    assert!(checkpoint.state_payload.is_some());
    let mut record = StandingRuntimeCheckpointRecord {
        published_input_bootstrap: None,
        demand_materialization: None,
        schema_version: 1,
        record_kind: "standing_runtime_checkpoint_v1".into(),
        view_id: "empty_stage".into(),
        checkpoint_key: test_checkpoint_key(&checkpoint).as_str().to_string(),
        previous_checkpoint: None,
        checkpoint,
        replay_checkpoints: Vec::new(),
        manifest_hash: String::new(),
    };
    validate_standing_runtime_checkpoint_replay_frontiers(&record).unwrap();
    record
        .replay_checkpoints
        .push(ReplayCheckpoint::for_relation(
            "empty_source",
            "v1",
            "unseen-stream",
            0,
            1,
        ));
    assert!(validate_standing_runtime_checkpoint_replay_frontiers(&record).is_err());
}

#[test]
fn standing_runtime_checkpoint_validation_allows_historical_replay_without_active_frontier() {
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let record = StandingRuntimeCheckpointRecord {
        published_input_bootstrap: None,
        demand_materialization: None,
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
        test_checkpoint_persist_context(&state, &checkpoint, "purchases_by_user").await,
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
    let raw_record = standing_runtime_checkpoint_record_from_slice(&checkpoint_bytes).unwrap();
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
    let state = object_only_checkpoint_codec_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    state
        .store
        .put(
            &Path::from(checkpoint_key.as_str()),
            bytes::Bytes::from_static(b"not a checkpoint Arrow record").into(),
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
async fn standing_runtime_checkpoint_read_rejects_legacy_json_record_bodies() {
    let state = object_only_checkpoint_codec_state().await;
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let legacy_record = test_checkpoint_record(&checkpoint_key, checkpoint.clone());
    state
        .store
        .put(
            &Path::from(checkpoint_key.as_str()),
            serde_json::to_vec(&legacy_record).unwrap().into(),
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
async fn view_admission_does_not_resurrect_stale_object_store_catalog_when_meta_is_authoritative() {
    // The recovery read path may fall back to the object-store registry when
    // the meta store is empty, but view admission must treat the meta store
    // as the authoritative source namespace: a stale registry copy of a
    // catalog the meta store never registered is a missing dependency, not
    // a resolvable source.
    let state = test_api_state().await;
    let catalog = test_scores_catalog();
    materialize_relation_catalog_to_object_store(&state, &catalog)
        .await
        .unwrap();
    let state = state.with_meta_store(Arc::new(InMemoryMetaStore::default()));
    let router = app(state);

    let rejected = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "stale_source_consumer",
            "sql": "select user_id, score from scores where score > 0",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(rejected.0, StatusCode::BAD_REQUEST, "{rejected:?}");
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap_or_default()
            .contains("not a registered relation"),
        "{rejected:?}"
    );
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
        test_checkpoint_persist_context(&state, &checkpoint, "purchases_by_user").await,
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
async fn standing_runtime_checkpoint_read_always_uses_meta_pointer() {
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
    });
    let next_payload = velorix_core::arrow_state::to_state(&next_payload).unwrap();
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
    let orphaned_payload = velorix_core::arrow_state::to_state(&serde_json::json!({
        "schema_version": 1,
        "published_output": {
            "records": []
        },
        "orphaned_checkpoint": true
    }))
    .unwrap();
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
        test_checkpoint_persist_context(&orphan_writer, &orphaned, "purchases_by_user").await,
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
        test_checkpoint_persist_context(&state, &checkpoint, "purchases_by_user").await,
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
        test_checkpoint_persist_context(&state, &checkpoint, "purchases_by_user").await,
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
        test_checkpoint_persist_context(&state, &previous, "purchases_by_user").await,
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
    });
    let next_payload = velorix_core::arrow_state::to_state(&next_payload).unwrap();
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
        test_checkpoint_persist_context(&state, &checkpoint, "purchases_by_user").await,
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
    let raw_record = standing_runtime_checkpoint_record_from_slice(&checkpoint_bytes).unwrap();
    let state_key = ObjectKey::parse_standing_runtime_state_payload(
        raw_record.checkpoint.state_root.object_key.clone(),
    )
    .unwrap()
    .0;
    let mut corrupt_record =
        standing_runtime_state_payload_record_for_checkpoint(&checkpoint, "purchases_by_user")
            .unwrap()
            .1;
    corrupt_record.payload.payload = velorix_core::arrow_state::to_state(&serde_json::json!({
        "schema_version": 1,
        "published_output": {
            "records": [{"key": "poison", "value": {"sum": 1, "count": 1}, "weight": 1}]
        }
    }))
    .unwrap();
    state
        .store
        .put(
            &Path::from(state_key.as_str()),
            bytes::Bytes::from(checkpoint_arrow::encode(&corrupt_record).unwrap()).into(),
        )
        .await
        .unwrap();

    let error =
        read_latest_standing_runtime_checkpoint(&state, &checkpoint.identity, "purchases_by_user")
            .await
            .unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(format!("{error:?}").contains("invalid Arrow/ZSTD checkpoint body"));
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
        aggregate_output_schema("purchase_metrics", &catalog, &sum_count_plan).unwrap();
    let avg_schema = aggregate_output_schema("purchase_metrics", &catalog, &avg_plan).unwrap();

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
    assert!(sum_count_schema.columns[1].nullable);
    assert!(!sum_count_schema.columns[2].nullable);
    assert!(avg_schema.columns[1].nullable);
    assert!(!avg_schema.columns[2].nullable);
    assert!(avg_schema.columns[3].nullable);
}

#[test]
fn aggregate_output_schema_supports_composite_computed_and_singleton_keys() {
    let catalog = test_order_facts_catalog();
    let composite = validate_catalog_backed_sum_count_view_sql(
        "select user_id, category, sum(amount) as sum, count(*) as count from order_facts group by user_id, category",
        &catalog,
    )
    .unwrap();
    let composite_schema =
        aggregate_output_schema("order_totals_by_user_category", &catalog, &composite).unwrap();
    assert_eq!(composite_schema.primary_key, ["user_id", "category"]);
    assert!(!composite_schema.columns[0].nullable);
    assert!(composite_schema.columns[1].nullable);

    let computed = validate_catalog_backed_sum_count_view_sql(
        "select user_id, amount / 10 as bucket, sum(amount) as sum, count(*) as count from order_facts group by user_id, bucket",
        &catalog,
    )
    .unwrap();
    let computed_schema =
        aggregate_output_schema("order_totals_by_user_bucket", &catalog, &computed).unwrap();
    assert_eq!(computed_schema.primary_key, ["user_id", "bucket"]);
    assert_eq!(computed_schema.columns[1].data_type, SqlDataType::Int64);
    assert!(!computed_schema.columns[1].nullable);

    let singleton = validate_catalog_backed_sum_count_view_sql(
        "select count(*) as count from order_facts",
        &catalog,
    )
    .unwrap();
    let singleton_schema =
        aggregate_output_schema("order_fact_count", &catalog, &singleton).unwrap();
    assert!(singleton_schema.primary_key.is_empty());
    assert_eq!(singleton_schema.columns.len(), 1);
    assert_eq!(singleton_schema.columns[0].name, "count");
}

#[test]
fn materialized_runtime_binding_persists_admitted_logical_plan() {
    let catalog = test_purchases_catalog();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let plan = validate_catalog_backed_sum_count_view_sql(sql, &catalog).unwrap();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = aggregate_output_schema("purchases_by_user", &catalog, &plan).unwrap();
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

    let binding = materialized_view_runtime_binding_for_spec(
        std::slice::from_ref(&catalog),
        &spec,
        &[],
        None,
    )
    .unwrap();
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

    let identity = standing_program_identity_from_materialized_view_runtime(&[catalog], &spec, &[])
        .expect("standing program identity should bind admitted semantics");
    assert!(identity
        .runtime_capabilities
        .iter()
        .any(|capability| capability == INCREMENTAL_KEY_SEMANTICS_VERSION_V1));
    assert!(identity
        .runtime_capabilities
        .iter()
        .any(|capability| capability == INCREMENTAL_BAG_SEMANTICS_VERSION_V1));
}

#[test]
fn latest_by_key_output_schema_uses_arg_max_value_type() {
    let mut catalog = test_item_status_catalog();
    catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "flag")
        .unwrap()
        .nullable = true;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("nullable latest-by-key input schema should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    let plan = validate_supported_latest_by_key_sql(
        "select key as item, arg_max(flag, ts) as flag from item_status group by key",
        &catalog,
    )
    .unwrap();

    let schema = latest_by_key_output_schema("latest_item_status", &catalog, &plan).unwrap();

    assert_eq!(schema.relation_id, "latest_item_status");
    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect::<Vec<_>>(),
        vec![("item", &SqlDataType::Utf8), ("flag", &SqlDataType::Bool)]
    );
    assert!(schema.columns[1].nullable);
    assert_eq!(schema.primary_key, vec!["item"]);
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
    let mut records = test_scores_catalog();
    records.relation_schema.relation_id = "rest_join_records_test".to_string();
    records.relation_schema.relation_name = "rest_join_records_test".to_string();
    records.relation_schema.columns[0].column_id = "key".to_string();
    records.relation_schema.columns[0].name = "key".to_string();
    records.relation_schema.columns[1].column_id = "value".to_string();
    records.relation_schema.columns[1].name = "value".to_string();
    records.relation_schema.primary_key_column_ids = vec!["key".to_string()];
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&records.relation_schema)
        .expect("records schema should fingerprint");
    records.schema_fingerprint = schema_fingerprint.clone();
    records.incremental_relation.relation_id = "rest_join_records_test".to_string();
    records.incremental_relation.schema_fingerprint = schema_fingerprint;
    records.datafusion_registration.name = "rest_join_records_test".to_string();

    let mut items = test_accounts_catalog();
    items.relation_schema.relation_id = "rest_join_items_test".to_string();
    items.relation_schema.relation_name = "rest_join_items_test".to_string();
    items.relation_schema.columns[0].column_id = "key".to_string();
    items.relation_schema.columns[0].name = "key".to_string();
    items.relation_schema.columns[1].column_id = "offset".to_string();
    items.relation_schema.columns[1].name = "offset".to_string();
    items.relation_schema.columns[2].column_id = "label".to_string();
    items.relation_schema.columns[2].name = "label".to_string();
    items.relation_schema.primary_key_column_ids = vec!["key".to_string()];
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&items.relation_schema)
        .expect("items schema should fingerprint");
    items.schema_fingerprint = schema_fingerprint.clone();
    items.incremental_relation.relation_id = "rest_join_items_test".to_string();
    items.incremental_relation.schema_fingerprint = schema_fingerprint;
    items.datafusion_registration.name = "rest_join_items_test".to_string();

    let catalogs = vec![records, items];
    let schemas = MaterializedViewRuntimeFactory
            .output_schemas_for_view_request_with_catalogs(
                "rest_join_records_by_item_test",
                "select d.key, sum(r.value) as total_value, count(*) as record_count, min(r.value) as min_value, max(r.value) as max_value from rest_join_records_test r join rest_join_items_test d on r.key = d.key group by d.key",
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
            ("key", &SqlDataType::Utf8),
            ("total_value", &SqlDataType::Int64),
            ("record_count", &SqlDataType::Int64),
            ("min_value", &SqlDataType::Int64),
            ("max_value", &SqlDataType::Int64),
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

#[test]
fn materialized_runtime_output_schema_supports_single_relation_self_join() {
    let mut scores = test_scores_catalog();
    scores.incremental_adapter.adapter_id = CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string();
    let catalogs = vec![scores];
    let schemas = MaterializedViewRuntimeFactory
        .output_schemas_for_view_request_with_catalogs(
            "score_self_join_count",
            "select count(*) as count from scores l join scores r on l.score = r.score",
            &catalogs,
            catalogs[0].schema_fingerprint.as_str(),
        )
        .unwrap()
        .expect("supported self-join should be admitted");

    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].columns.len(), 1);
    assert_eq!(schemas[0].columns[0].name, "count");
    assert_eq!(schemas[0].columns[0].data_type, SqlDataType::Int64);
    assert!(schemas[0].primary_key.is_empty());
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
            "catalog": test_item_status_catalog()
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "latest_item_status_cte".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "item_status".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "with status_source as (select * from item_status where ts > 95) select key as item, arg_max(flag, ts) as flag from status_source where flag = true group by key".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("latest item status through CTE source".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
            correction_horizon_ns: None,
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
                "relation_id": "item_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "item-status-cte-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "event_time_watermark": {
                    "event_time_column_id": "ts",
                    "max_observed_event_time_ns": 110,
                    "watermark_ns": 100
                },
                "rows": [
                    {"key": "item-a", "flag": true, "ts": 100, "delta": 1},
                    {"key": "item-a", "flag": false, "ts": 110, "delta": 1},
                    {"key": "item-b", "flag": true, "ts": 90, "delta": 1}
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
        "/v1/views/latest_item_status_cte/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"item": "item-a", "flag": true}])
    );
}

#[tokio::test]
async fn rest_latest_by_key_nullable_value_survives_query_and_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "api-test-latest-nullable-a", false).await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state.clone());

    let mut catalog = test_item_status_catalog();
    catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "flag")
        .expect("item status fixture has flag")
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
            "catalog": catalog
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
            "view_id": "latest_nullable_item_status",
            "sql": "select key as item, arg_max(flag, ts) as flag from item_status group by key",
            "input_relation_id": "item_status",
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
                "relation_id": "item_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "nullable-item-status-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"key": "item-a", "flag": true, "ts": 100, "delta": 1},
                    {"key": "item-a", "flag": null, "ts": 110, "delta": 1},
                    {"key": "item-b", "flag": false, "ts": 120, "delta": 1}
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
        {"item": "item-a", "flag": null},
        {"item": "item-b", "flag": false}
    ]);
    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/latest_nullable_item_status/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"], expected_rows,
        "{query_response:?}"
    );

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-latest-nullable-b",
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
    let restarted_router = app(restarted_state);
    let restarted_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/latest_nullable_item_status/query",
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
                "relation_id": "item_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "nullable-item-status-stream",
                "partition_id": 0,
                "start_offset_inclusive": 3,
                "rows": [
                    {"key": "item-a", "flag": false, "ts": 120, "delta": 1}
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
        "/v1/views/latest_nullable_item_status/query",
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
            {"item": "item-a", "flag": false},
            {"item": "item-b", "flag": false}
        ]),
        "{newest_value_query:?}"
    );

    let retract_newer_value = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "item_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "nullable-item-status-stream",
                "partition_id": 0,
                "start_offset_inclusive": 4,
                "rows": [
                    {"key": "item-a", "flag": false, "ts": 120, "delta": -1}
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
        "/v1/views/latest_nullable_item_status/query",
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
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_item_status_catalog()
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);
    assert_eq!(relation_response.1["relation_id"], "item_status");

    let view_request = CreateViewRequest {
            view_id: "latest_item_status".to_string(),
            url_path: Some("/items/latest-status".to_string()),
            output_relation_id: None,
            input_relation_id: "item_status".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select d.key as item, arg_max(d.flag, d.ts) as flag from item_status as d where d.flag = true group by d.key".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("latest bool status by item".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
            correction_horizon_ns: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(
        view_response.0,
        StatusCode::CREATED,
        "view creation response: {}",
        view_response.1
    );
    assert_eq!(view_response.1["view_id"], "latest_item_status");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "item_status",
            "relation_version": "2026-05-24.v1",
            "stream_id": "item-status-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "event_time_watermark": {
                "event_time_column_id": "ts",
                "max_observed_event_time_ns": 110,
                "watermark_ns": 100
            },
            "rows": [
                {"key": "item-a", "flag": true, "ts": 100, "delta": 1},
                {"key": "item-a", "flag": false, "ts": 110, "delta": 1},
                {"key": "item-b", "flag": true, "ts": 90, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest_response.0, StatusCode::CREATED);
    assert_eq!(ingest_response.1["outcome"], "appended");

    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/latest_item_status/query",
        json!({}),
    )
    .await;
    assert_eq!(
        query_response.0,
        StatusCode::OK,
        "join query response: {}",
        query_response.1
    );
    assert_latest_key_rows(&query_response.1, 3, true, true);

    let get_sql_response = call_json(
            &router,
            Method::GET,
            "/v1/views/latest_item_status/query?sql=select%20item%2C%20flag%20from%20latest_item_status%20where%20flag%20%3D%20true%20order%20by%20item",
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
            {"item": "item-a", "flag": true},
            {"item": "item-b", "flag": true}
        ])
    );

    append_admitted_ingest_without_runtime_apply(
        store.clone(),
        IngestRowsRequest {
            relation_id: "item_status".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            stream_id: "item-status-stream".to_string(),
            partition_id: 0,
            start_offset_inclusive: 3,
            event_time_watermark: Some(IngestEventTimeWatermarkRequest {
                event_time_column_id: "ts".to_string(),
                max_observed_event_time_ns: 120,
                watermark_ns: 115,
            }),
            rows: vec![json!({
                "key": "item-b",
                "flag": false,
                "ts": 120,
                "delta": 1
            })],
        },
    )
    .await;

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store.clone(),
        "api-test-owner-b",
        true,
    )
    .await;
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restarted_app = app(restarted_state);
    let restarted_query_response = call_json(
        &restarted_app,
        Method::POST,
        "/v1/views/latest_item_status/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query_response.0, StatusCode::OK);
    assert_latest_key_rows(&restarted_query_response.1, 4, true, true);
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
            "catalog": test_item_status_catalog()
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "latest_item_status_top".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "item_status".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select key as item, arg_max(flag, ts) as flag from item_status group by key order by item desc limit 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("top latest item status".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
            correction_horizon_ns: None,
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
                "relation_id": "item_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "item-status-top-k-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"key": "item-a", "flag": true, "ts": 100, "delta": 1},
                    {"key": "item-b", "flag": false, "ts": 110, "delta": 1}
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
        "/v1/views/latest_item_status_top/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"item": "item-b", "flag": false}])
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
            "catalog": test_item_status_catalog()
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
            view_id: "latest_item_status_offset".to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: "item_status".to_string(),
            input_relation_version: "2026-05-24.v1".to_string(),
            input_relation_refs: Vec::new(),
            input_relations: Vec::new(),
            sql: "select key as item, arg_max(flag, ts) as flag from item_status group by key order by item desc limit 1 offset 1".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("second latest item status by item order".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
            correction_horizon_ns: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "item_status",
                "relation_version": "2026-05-24.v1",
                "stream_id": "item-status-offset-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"key": "item-a", "flag": true, "ts": 100, "delta": 1},
                    {"key": "item-b", "flag": false, "ts": 110, "delta": 1},
                    {"key": "item-c", "flag": true, "ts": 120, "delta": 1}
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
        "/v1/views/latest_item_status_offset/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"],
        json!([{"item": "item-b", "flag": false}])
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
            "catalog": test_scores_catalog()
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
            "catalog": test_item_status_catalog()
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "item_status",
            "relation_version": "2026-05-24.v1",
            "stream_id": "item-status-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "event_time_watermark": {
                "event_time_column_id": "ts",
                "max_observed_event_time_ns": 109,
                "watermark_ns": 100
            },
            "rows": [
                {"key": "item-a", "flag": true, "ts": 100, "delta": 1},
                {"key": "item-b", "flag": false, "ts": 110, "delta": 1}
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
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog()
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
        correction_horizon_ns: None,
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

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store.clone(),
        "api-test-window-owner-b",
        true,
    )
    .await;
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
            "catalog": test_purchases_event_time_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_purchases_event_time_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_purchases_event_time_catalog()
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
            correction_horizon_ns: None,
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
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog()
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
            correction_horizon_ns: None,
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

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-window-count-distinct-owner-b",
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
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog()
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
            correction_horizon_ns: None,
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

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
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
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
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
            "catalog": catalog
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
            correction_horizon_ns: None,
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

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-window-nullable-count-owner-b",
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
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
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
            "catalog": catalog
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
            correction_horizon_ns: None,
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

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
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
async fn rest_filtered_aggregate_retains_zero_and_all_filtered_groups_across_restart_retraction() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(
        store.clone(),
        "api-test-filtered-group-survival-owner-a",
        false,
    )
    .await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog()
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
            "view_id": "filtered_score_groups",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) filter (where score = 0) as filtered_sum, count(*) filter (where score = 0) as filtered_count from scores group by user_id"
        }),
    )
    .await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let rows = json!([
        {"user_id": "zero", "score": 0, "delta": 1},
        {"user_id": "filtered_out", "score": 1, "delta": 1}
    ]);
    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "filtered-group-survival-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": rows
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let expected_rows = json!([
        {"user_id": "filtered_out", "filtered_sum": null, "filtered_count": 0},
        {"user_id": "zero", "filtered_sum": 0, "filtered_count": 1}
    ]);
    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/filtered_score_groups/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"], expected_rows,
        "{query_response:?}"
    );

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store.clone(),
        "api-test-filtered-group-survival-owner-b",
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
    let restarted_router = app(restarted_state);
    let restarted_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/filtered_score_groups/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(
        restarted_query.1["rows"], expected_rows,
        "{restarted_query:?}"
    );

    let retraction_response = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "filtered-group-survival-stream",
            "partition_id": 0,
            "start_offset_inclusive": 2,
            "rows": [
                {"user_id": "zero", "score": 0, "delta": -1},
                {"user_id": "filtered_out", "score": 1, "delta": -1}
            ]
        }),
    )
    .await;
    assert_eq!(
        retraction_response.0,
        StatusCode::CREATED,
        "{retraction_response:?}"
    );

    let empty_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/filtered_score_groups/query",
        json!({}),
    )
    .await;
    assert_eq!(empty_query.0, StatusCode::OK, "{empty_query:?}");
    assert_eq!(empty_query.1["rows"], json!([]), "{empty_query:?}");

    let reinsert_response = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "filtered-group-survival-stream",
            "partition_id": 0,
            "start_offset_inclusive": 4,
            "rows": rows
        }),
    )
    .await;
    assert_eq!(
        reinsert_response.0,
        StatusCode::CREATED,
        "{reinsert_response:?}"
    );

    let reinserted_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/filtered_score_groups/query",
        json!({}),
    )
    .await;
    assert_eq!(reinserted_query.0, StatusCode::OK, "{reinserted_query:?}");
    assert_eq!(
        reinserted_query.1["rows"], expected_rows,
        "{reinserted_query:?}"
    );
}

#[tokio::test]
async fn rest_nullable_numeric_aggregates_preserve_nulls_and_retractions_across_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(
        store.clone(),
        "api-test-nullable-aggregate-survival-owner-a",
        false,
    )
    .await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog_with_nullable_score()
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
            "view_id": "nullable_score_aggregate_groups",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as score_sum, avg(score) as score_avg, min(score) as score_min, max(score) as score_max, count(score) as score_count from scores group by user_id"
        }),
    )
    .await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let rows = json!([
        {"user_id": "empty", "score": null, "delta": 1},
        {"user_id": "mixed", "score": 0, "delta": 1},
        {"user_id": "mixed", "score": null, "delta": 1},
        {"user_id": "zero", "score": 0, "delta": 1}
    ]);
    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "nullable-aggregate-survival-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": rows
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    let expected_rows = json!([
        {
            "user_id": "empty",
            "score_sum": null,
            "score_avg": null,
            "score_min": null,
            "score_max": null,
            "score_count": 0
        },
        {
            "user_id": "mixed",
            "score_sum": 0,
            "score_avg": 0.0,
            "score_min": 0,
            "score_max": 0,
            "score_count": 1
        },
        {
            "user_id": "zero",
            "score_sum": 0,
            "score_avg": 0.0,
            "score_min": 0,
            "score_max": 0,
            "score_count": 1
        }
    ]);
    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/nullable_score_aggregate_groups/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"], expected_rows,
        "{query_response:?}"
    );

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store.clone(),
        "api-test-nullable-aggregate-survival-owner-b",
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
    let restarted_router = app(restarted_state);
    let restarted_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/nullable_score_aggregate_groups/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(
        restarted_query.1["rows"], expected_rows,
        "{restarted_query:?}"
    );

    let retraction_response = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "nullable-aggregate-survival-stream",
            "partition_id": 0,
            "start_offset_inclusive": 4,
            "rows": [
                {"user_id": "empty", "score": null, "delta": -1},
                {"user_id": "mixed", "score": 0, "delta": -1},
                {"user_id": "mixed", "score": null, "delta": -1},
                {"user_id": "zero", "score": 0, "delta": -1}
            ]
        }),
    )
    .await;
    assert_eq!(
        retraction_response.0,
        StatusCode::CREATED,
        "{retraction_response:?}"
    );
    let empty_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/nullable_score_aggregate_groups/query",
        json!({}),
    )
    .await;
    assert_eq!(empty_query.0, StatusCode::OK, "{empty_query:?}");
    assert_eq!(empty_query.1["rows"], json!([]), "{empty_query:?}");

    let reinsert_response = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "nullable-aggregate-survival-stream",
            "partition_id": 0,
            "start_offset_inclusive": 8,
            "rows": rows
        }),
    )
    .await;
    assert_eq!(
        reinsert_response.0,
        StatusCode::CREATED,
        "{reinsert_response:?}"
    );
    let reinserted_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/nullable_score_aggregate_groups/query",
        json!({}),
    )
    .await;
    assert_eq!(reinserted_query.0, StatusCode::OK, "{reinserted_query:?}");
    assert_eq!(
        reinserted_query.1["rows"], expected_rows,
        "{reinserted_query:?}"
    );
}

#[tokio::test]
async fn rest_hopping_window_advanced_aggregate_view_survives_api_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state =
        test_api_state_with_store(store.clone(), "api-test-hop-advanced-owner-a", false).await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog()
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
        correction_horizon_ns: None,
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

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-hop-advanced-owner-b",
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
            "catalog": test_purchases_event_time_catalog()
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
            correction_horizon_ns: None,
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
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog()
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
            correction_horizon_ns: None,
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

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-session-window-owner-b",
        true,
    )
    .await;
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
            "catalog": test_purchases_event_time_catalog()
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
        correction_horizon_ns: None,
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
            "catalog": test_purchases_event_time_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
        test_item_status_catalog(),
    ] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog
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
                    relation_id: "item_status".to_string(),
                    relation_version: "2026-05-24.v1".to_string(),
                },
            ],
            input_relations: Vec::new(),
            sql: "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id join item_status d on d.key = s.user_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("unsupported three-table join SQL".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
            correction_horizon_ns: None,
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
        test_item_status_catalog(),
    ] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog
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
    let scores_accounts_items = vec![
        InputRelationRef {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
        },
        InputRelationRef {
            relation_id: "accounts".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
        },
        InputRelationRef {
            relation_id: "item_status".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
        },
    ];

    for (view_id, sql, refs) in [
        (
            "corpus_order_using_operator",
            "select user_id, sum(score) as sum, count(*) as count from scores group by user_id order by sum using > limit 1",
            scores.clone(),
        ),
        (
            "corpus_cast_array",
            "select user_id, cast(score as bigint array) as score from scores",
            scores.clone(),
        ),
        (
            "corpus_cast_format",
            "select user_id, cast(score as bigint format '999') as score from scores",
            scores.clone(),
        ),
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
            "select a.account_id, sum(s.score) as sum from scores s join accounts a on s.user_id = a.account_id join item_status d on d.key = s.user_id group by a.account_id",
            scores_accounts_items.clone(),
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
            correction_horizon_ns: None,
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
                .header(header::ACCEPT, "application/json")
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
    let summary =
        apply_standing_runtime_prepared_ingests(&state, None, &[prepared], None, &BTreeSet::new())
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
    let second_summary = apply_standing_runtime_prepared_ingests(
        &state,
        None,
        &[next_prepared],
        None,
        &BTreeSet::new(),
    )
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
        3,
        "direct apply reads the prior checkpoint once, probes the candidate descriptor before creation, and verifies the newly created Meta checkpoint object"
    );
    assert_ne!(
        checkpoint_get_paths[0], checkpoint_get_paths[1],
        "checkpoint publication must not reread the prior checkpoint"
    );
    assert_eq!(
        checkpoint_get_paths[1], checkpoint_get_paths[2],
        "candidate-key GETs are the legacy-descriptor collision probe and post-create Meta pointer verification"
    );
    let checkpoint_list_prefixes = object_access_counts
        .list_prefixes()
        .into_iter()
        .filter(|path| path.starts_with("v1/standing-runtime-checkpoints/"))
        .collect::<Vec<_>>();
    assert_eq!(
        checkpoint_list_prefixes.len(),
        0,
        "Meta-authoritative direct apply does not list checkpoint epochs"
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
            "catalog": test_scores_catalog()
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
        baseline_ingest_list_count + 1,
        "first fenced bootstrap activation needs one legacy-ingest coverage listing; later ingests must not repeat it"
    );
    assert_eq!(
        ingest_get_count, baseline_ingest_get_count,
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
async fn rest_ingest_commits_metadata_source_cut_only_after_durable_append() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (state, meta_store) =
        test_api_state_with_uninitialized_active_registry(store, "source-cut-owner").await;
    let state = with_initialized_active_view_meta_store(
        state,
        Arc::clone(&meta_store) as Arc<dyn MetaStore>,
    )
    .await;
    let router = app(state.clone());
    let catalog = test_scores_catalog();
    let schema_fingerprint = catalog.schema_fingerprint.to_string();
    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": catalog
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
            "stream_id": "source-cut-stream",
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

    let cut = meta_store
        .capture_ingest_source_cut(CaptureIngestSourceCutRequest {
            relations: vec![IngestSourceRelationIdentityV1 {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                relation_generation: 1,
                schema_fingerprint,
            }],
        })
        .await
        .unwrap();
    assert_eq!(cut.relations[0].partitions.len(), 1);
    assert_eq!(cut.relations[0].partitions[0].committed_offset_exclusive, 1);
}

#[tokio::test]
async fn rest_ingest_does_not_commit_source_cut_when_durable_append_fails() {
    let failing_store = Arc::new(ArmedPrefixFailingObjectStore::new(
        Arc::new(InMemory::new()),
    ));
    let (state, meta_store) = test_api_state_with_uninitialized_active_registry(
        Arc::clone(&failing_store) as Arc<dyn ObjectStore>,
        "api-test-source-cut-append-failure",
    )
    .await;
    let state = with_initialized_active_view_meta_store(
        state,
        Arc::clone(&meta_store) as Arc<dyn MetaStore>,
    )
    .await;
    let router = app(state);
    let catalog = test_scores_catalog();
    let schema_fingerprint = catalog.schema_fingerprint.to_string();
    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": catalog
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let ingest = json!({
        "relation_id": "scores",
        "relation_version": "2026-05-24.v1",
        "stream_id": "source-cut-failure-stream",
        "partition_id": 0,
        "start_offset_inclusive": 0,
        "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
    });
    failing_store.arm("v1/ingest/");
    let failed = call_json(&router, Method::POST, "/v1/ingest", ingest.clone()).await;
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR, "{failed:?}");

    let request = CaptureIngestSourceCutRequest {
        relations: vec![IngestSourceRelationIdentityV1 {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            relation_generation: 1,
            schema_fingerprint,
        }],
    };
    let cut_after_failure = meta_store
        .capture_ingest_source_cut(request.clone())
        .await
        .unwrap();
    assert_eq!(cut_after_failure.relations[0].partitions.len(), 1);
    assert_eq!(
        cut_after_failure.relations[0].partitions[0].committed_offset_exclusive,
        0
    );

    let retried = call_json(&router, Method::POST, "/v1/ingest", ingest).await;
    assert_eq!(retried.0, StatusCode::CREATED);
    let cut_after_retry = meta_store.capture_ingest_source_cut(request).await.unwrap();
    assert_eq!(
        cut_after_retry.relations[0].partitions[0].committed_offset_exclusive,
        1
    );
}

#[tokio::test]
async fn rest_view_admission_persists_plan_spec_and_source_cut_in_metadata() {
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let state = with_initialized_active_view_meta_store(
        test_api_state().await,
        Arc::clone(&meta_store) as Arc<dyn MetaStore>,
    )
    .await;
    let restart_meta = Arc::clone(&meta_store) as Arc<dyn MetaStore>;
    let restart_owner_id = state.owner_id.clone();
    let router = app(state.clone());
    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog()
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
            "stream_id": "view-bootstrap-source-cut-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
        }),
    )
    .await;
    assert_eq!(ingest_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
        view_id: "source_cut_scores_by_user".to_string(),
        url_path: None,
        output_relation_id: None,
        input_relation_id: "scores".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: "select user_id, sum(score) as sum, count(*) as count from scores group by user_id"
            .to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: None,
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
        correction_horizon_ns: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(
        view_response.1["lifecycle"]["deployment_status"],
        "deploying"
    );
    assert!(view_response.1["lifecycle"]["message"]
        .as_str()
        .unwrap()
        .contains("backfill_required"));

    let control = meta_store
        .read_view_bootstrap(
            "default",
            "source_cut_scores_by_user",
            "source_cut_scores_by_user",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(control.bootstrap_generation, 1);
    assert_eq!(control.lifecycle, ViewBootstrapLifecycleV1::Bootstrapping);
    assert!(!control.plan_hash.is_empty());
    let persisted_spec: StandingViewSpec = serde_json::from_slice(&control.view_spec_json).unwrap();
    assert_eq!(persisted_spec.view_id, "source_cut_scores_by_user");
    assert_eq!(control.bootstrap_cut.input_catalog_epoch, 1);
    assert_eq!(control.bootstrap_cut.relations[0].partitions.len(), 1);
    assert_eq!(
        control.bootstrap_cut.relations[0].partitions[0].committed_offset_exclusive,
        1
    );

    let admitted = read_test_active_view(&state, "source_cut_scores_by_user")
        .await
        .unwrap();
    update_test_active_view_lifecycle(
        &state,
        "source_cut_scores_by_user",
        &admitted.spec_hash,
        MaterializedViewLifecycleStatus::standing_runtime(),
    )
    .await
    .unwrap();
    let premature_query = call_json(
        &router,
        Method::POST,
        "/v1/views/source_cut_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(premature_query.0, StatusCode::SERVICE_UNAVAILABLE);
    assert!(premature_query.1["error"]
        .as_str()
        .unwrap()
        .contains("authoritative activation is incomplete"));
    update_test_active_view_lifecycle(
        &state,
        "source_cut_scores_by_user",
        &admitted.spec_hash,
        admitted.lifecycle,
    )
    .await
    .unwrap();

    let pinned_batch_key =
        ObjectKey::ingest_batch("view-bootstrap-source-cut-stream", 0, 0, 1).unwrap();
    let pinned_batch_path = Path::from(pinned_batch_key.as_str());
    let pinned_batch = state
        .store
        .get(&pinned_batch_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    state.store.delete(&pinned_batch_path).await.unwrap();
    let missing_retained_input = call_json(
        &router,
        Method::POST,
        "/v1/views/source_cut_scores_by_user/backfill",
        json!({}),
    )
    .await;
    assert_eq!(missing_retained_input.0, StatusCode::CONFLICT);
    assert!(missing_retained_input.1["error"]
        .as_str()
        .unwrap()
        .contains("current checkpoint does not cover the bootstrap cut"));
    assert_eq!(
        meta_store
            .read_view_bootstrap(
                "default",
                "source_cut_scores_by_user",
                "source_cut_scores_by_user",
            )
            .await
            .unwrap()
            .unwrap()
            .lifecycle,
        ViewBootstrapLifecycleV1::Bootstrapping
    );
    state
        .store
        .put(&pinned_batch_path, pinned_batch.into())
        .await
        .unwrap();

    let same_partition_tail = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "view-bootstrap-source-cut-stream",
            "partition_id": 0,
            "start_offset_inclusive": 1,
            "rows": [{"user_id": "bob", "score": 20, "delta": 1}]
        }),
    )
    .await;
    assert_eq!(same_partition_tail.0, StatusCode::CREATED);
    let new_partition_tail = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "view-bootstrap-source-cut-stream",
            "partition_id": 1,
            "start_offset_inclusive": 0,
            "rows": [{"user_id": "carol", "score": 30, "delta": 1}]
        }),
    )
    .await;
    assert_eq!(new_partition_tail.0, StatusCode::CREATED);

    let backfill = call_json(
        &router,
        Method::POST,
        "/v1/views/source_cut_scores_by_user/backfill",
        json!({}),
    )
    .await;
    assert_eq!(backfill.0, StatusCode::OK, "{backfill:?}");
    assert_eq!(backfill.1["remaining_batches"], 0);
    let committed_pointer = meta_store
        .read_standing_runtime_checkpoint(
            "default",
            "source_cut_scores_by_user",
            "source_cut_scores_by_user",
        )
        .await
        .unwrap()
        .unwrap();
    let committed_coverage = committed_pointer.input_coverage.as_ref().unwrap();
    assert_eq!(
        committed_pointer.bootstrap_generation,
        control.bootstrap_generation
    );
    assert_eq!(committed_pointer.plan_hash, control.plan_hash);
    assert_eq!(
        committed_pointer.coverage_hash,
        committed_coverage.stable_hash().unwrap()
    );
    assert_eq!(committed_coverage.relations[0].partitions.len(), 2);
    let activated_control = meta_store
        .read_view_bootstrap(
            "default",
            "source_cut_scores_by_user",
            "source_cut_scores_by_user",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        activated_control.lifecycle,
        ViewBootstrapLifecycleV1::Active
    );
    assert_eq!(
        activated_control
            .activation_cut
            .as_ref()
            .unwrap()
            .input_catalog_epoch,
        3
    );
    assert_eq!(
        activated_control.active_checkpoint.as_ref(),
        Some(&committed_pointer)
    );
    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/source_cut_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let rows = query.1["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    for (user_id, sum) in [("alice", 10), ("bob", 20), ("carol", 30)] {
        assert!(rows
            .iter()
            .any(|row| row["user_id"] == user_id && row["sum"] == sum));
    }

    // Caught-up restore is read-only and must coexist with the live writer;
    // no lease takeover or checkpoint publication is needed.
    let restarted_state = restart_test_api_state_with_store(
        Arc::clone(&restart_meta),
        restart_owner_id.clone(),
        Arc::clone(&state.store),
        "source-cut-restart-owner",
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
    let restarted_router = app(restarted_state);
    let restarted_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/source_cut_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(restarted_query.1["rows"], query.1["rows"]);

    let unchanged_cut = meta_store
        .read_view_bootstrap(
            "default",
            "source_cut_scores_by_user",
            "source_cut_scores_by_user",
        )
        .await
        .unwrap()
        .unwrap()
        .bootstrap_cut;
    assert_eq!(unchanged_cut, control.bootstrap_cut);
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
async fn rest_composite_and_global_aggregates_survive_restart_and_final_retraction() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_public_api_state_with_store(
        store.clone(),
        "api-test-general-group-keys-owner-a",
        false,
    )
    .await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_order_facts_catalog()
        }),
    )
    .await;
    assert_eq!(
        relation_response.0,
        StatusCode::CREATED,
        "{relation_response:?}"
    );

    for request in [
        json!({
            "view_id": "order_totals_by_user_category",
            "input_relation_id": "order_facts",
            "input_relation_version": "2026-08-10.v1",
            "sql": "select user_id, category, sum(amount) as sum, count(*) as count from order_facts group by user_id, category"
        }),
        json!({
            "view_id": "order_fact_count",
            "input_relation_id": "order_facts",
            "input_relation_version": "2026-08-10.v1",
            "sql": "select count(*) as count from order_facts"
        }),
    ] {
        let response = call_json(&router, Method::POST, "/v1/views", request).await;
        assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
    }

    let empty_global = call_json(
        &router,
        Method::POST,
        "/v1/views/order_fact_count/query",
        json!({}),
    )
    .await;
    assert_eq!(
        empty_global.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "{empty_global:?}"
    );
    assert!(empty_global.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("MATERIALIZATION_LAG"));

    let rows = json!([
        {"order_id": "o1", "user_id": "u1", "category": "a", "amount": 5, "delta": 1},
        {"order_id": "o2", "user_id": "u1", "category": "a", "amount": 7, "delta": 1},
        {"order_id": "o3", "user_id": "u1", "category": null, "amount": 15, "delta": 1}
    ]);
    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/order_facts/ingest",
        json!({
            "relation_version": "2026-08-10.v1",
            "stream_id": "general-group-keys-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": rows
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );
    assert_eq!(ingest_response.1["materialization"]["active_views"], 2);

    let composite = call_json(
        &router,
        Method::POST,
        "/v1/views/order_totals_by_user_category/query",
        json!({}),
    )
    .await;
    assert_eq!(composite.0, StatusCode::OK, "{composite:?}");
    assert_eq!(
        composite.1["rows"],
        json!([
            {"user_id": "u1", "category": "a", "sum": 12, "count": 2},
            {"user_id": "u1", "category": null, "sum": 15, "count": 1}
        ])
    );
    let global = call_json(
        &router,
        Method::POST,
        "/v1/views/order_fact_count/query",
        json!({}),
    )
    .await;
    assert_eq!(global.0, StatusCode::OK, "{global:?}");
    assert_eq!(global.1["rows"], json!([{"count": 3}]));

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-general-group-keys-owner-b",
        true,
    )
    .await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        2
    );
    let restarted_router = app(restarted_state);
    for (view_id, expected_rows) in [
        ("order_totals_by_user_category", composite.1["rows"].clone()),
        ("order_fact_count", global.1["rows"].clone()),
    ] {
        let restored = call_json(
            &restarted_router,
            Method::POST,
            &format!("/v1/views/{view_id}/query"),
            json!({}),
        )
        .await;
        assert_eq!(restored.0, StatusCode::OK, "{restored:?}");
        assert_eq!(restored.1["rows"], expected_rows);
    }

    let retract_rows = json!([
        {"order_id": "o1", "user_id": "u1", "category": "a", "amount": 5, "delta": -1},
        {"order_id": "o2", "user_id": "u1", "category": "a", "amount": 7, "delta": -1},
        {"order_id": "o3", "user_id": "u1", "category": null, "amount": 15, "delta": -1}
    ]);
    let retraction = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/order_facts/ingest",
        json!({
            "relation_version": "2026-08-10.v1",
            "stream_id": "general-group-keys-stream",
            "partition_id": 0,
            "start_offset_inclusive": 3,
            "rows": retract_rows
        }),
    )
    .await;
    assert_eq!(retraction.0, StatusCode::CREATED, "{retraction:?}");

    let empty_composite = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/order_totals_by_user_category/query",
        json!({}),
    )
    .await;
    assert_eq!(empty_composite.0, StatusCode::OK, "{empty_composite:?}");
    assert_eq!(empty_composite.1["rows"], json!([]));
    let reset_global = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/order_fact_count/query",
        json!({}),
    )
    .await;
    assert_eq!(reset_global.0, StatusCode::OK, "{reset_global:?}");
    assert_eq!(reset_global.1["rows"], json!([{"count": 0}]));
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
            "catalog": test_scores_catalog()
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
        baseline_ingest_list_count + 1,
        "first fenced bootstrap activation needs one legacy-ingest coverage listing; the next ingest must reuse its committed checkpoint"
    );
    assert_eq!(
        ingest_get_count, baseline_ingest_get_count,
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
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_accounts_catalog()
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
            correction_horizon_ns: None,
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

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-row-number-owner-b",
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
            "catalog": test_scores_catalog()
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
        correction_horizon_ns: None,
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
            "catalog": test_purchases_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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

    let active = read_test_active_view(&state, "retry_scores_by_user")
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
    let inspection_state = state.clone();
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED);

    let first_ingest_body = json!({
        "relation_id": "scores",
        "relation_version": "2026-05-24.v1",
        "stream_id": "optimized-scores-stream",
        "partition_id": 0,
        "start_offset_inclusive": 0,
        "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
    });
    let first_ingest = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        first_ingest_body.clone(),
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
        first_ingest.1["materialization"]["checkpoint_pointer_writes"], 1,
        "Meta-authoritative checkpoint publication writes its pointer"
    );
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
    assert_eq!(first_ingest.1["descriptor"]["relation_id"], "scores");

    let active = list_test_active_views(&inspection_state)
        .await
        .unwrap()
        .into_iter()
        .find(|active| active.spec.view_id == "optimized_scores_by_user")
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let epoch_manifest = PersistedIngestEpochManifest {
        epoch_manifest_id: first_ingest.1["epoch_manifest_id"]
            .as_str()
            .unwrap()
            .to_string(),
        epoch_manifest_key: String::new(),
    };
    let convergence_key = ObjectKey::ingest_epoch_view_convergence(
        &epoch_manifest.epoch_manifest_id,
        &identity.tenant_id,
        &identity.program_id,
        "optimized_scores_by_user",
    )
    .unwrap();
    let convergence_bytes = inspection_state
        .store
        .get(&Path::from(convergence_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let convergence: IngestEpochViewConvergenceRecord =
        serde_json::from_slice(&convergence_bytes).unwrap();
    validate_ingest_epoch_view_convergence_record(
        &convergence,
        &epoch_manifest,
        identity,
        "optimized_scores_by_user",
        convergence_key.as_str(),
    )
    .unwrap();
    assert_eq!(
        convergence.output_publication_protocol_id,
        OUTPUT_PUBLICATION_PROTOCOL_VERSION_V1
    );
    assert_snapshot_and_commit_refs(&convergence.output_refs);
    let authoritative = read_latest_standing_runtime_checkpoint(
        &inspection_state,
        identity,
        "optimized_scores_by_user",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        convergence.output_refs,
        authoritative.checkpoint.output_manifest_refs
    );

    let mut tampered_convergence = convergence.clone();
    tampered_convergence.output_refs = vec![format!(
        "{STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX}v1/standing-runtime-output-deltas/tampered"
    )];
    inspection_state
        .store
        .put(
            &Path::from(convergence_key.as_str()),
            bytes::Bytes::from(serde_json::to_vec(&tampered_convergence).unwrap()).into(),
        )
        .await
        .unwrap();
    let rejected_false_ack = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        first_ingest_body.clone(),
    )
    .await;
    assert!(
        rejected_false_ack.0.is_client_error(),
        "{rejected_false_ack:?}"
    );
    assert!(rejected_false_ack.1["error"]
        .as_str()
        .unwrap()
        .contains("convergence checkpoint mismatch"));
    inspection_state
        .store
        .put(
            &Path::from(convergence_key.as_str()),
            convergence_bytes.into(),
        )
        .await
        .unwrap();

    let duplicate_ack = call_json(&router, Method::POST, "/v1/ingest", first_ingest_body).await;
    assert_eq!(duplicate_ack.0, StatusCode::OK, "{duplicate_ack:?}");
    assert_eq!(duplicate_ack.1["ack_mode"], "materialized");
    assert_eq!(duplicate_ack.1["materialization"]["checkpoint_writes"], 0);
    assert_eq!(duplicate_ack.1["materialization"]["output_delta_writes"], 0);

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
        epoch_ingest.1["materialization"]["checkpoint_pointer_writes"], 1,
        "Meta-authoritative checkpoint publication updates its pointer"
    );
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
    let epoch_batches = epoch_ingest.1["batches"].as_array().unwrap();
    assert_eq!(epoch_batches.len(), 2);
    assert_eq!(epoch_batches[0]["descriptor"]["relation_id"], "scores");
    assert_eq!(epoch_batches[1]["descriptor"]["relation_id"], "scores");

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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
        .contains(
            "MATERIALIZATION_LAG: authoritative activation is incomplete for view `budgeted_scores_by_user`"
        ));
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
            "catalog": test_scores_catalog()
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog_with_nullable_score()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog_with_nullable_score()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog_with_nullable_score()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog_with_nullable_score()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
        correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
        correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
        correction_horizon_ns: None,
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
            "catalog": test_item_status_catalog()
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
        view_id: "item_enabled_flags".to_string(),
        url_path: Some("/items/flag-flags".to_string()),
        output_relation_id: None,
        input_relation_id: "item_status".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: "select key, case when flag = true then 1 else 0 end as enabled_flag from item_status"
            .to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("item flag flags".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
        correction_horizon_ns: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/item_status/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "item-status-bool-case-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"key": "item-a", "flag": true, "ts": 100, "delta": 1},
                {"key": "item-b", "flag": false, "ts": 101, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/item_enabled_flags/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([
            {"key": "item-a", "enabled_flag": 1},
            {"key": "item-b", "enabled_flag": 0}
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog_with_nullable_score()
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
        correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
        correction_horizon_ns: None,
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
async fn rest_scalar_aggregate_modifier_admission_and_plain_matrix() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state =
        test_api_state_with_store(store.clone(), "api-test-scalar-aggregate-modifiers", false)
            .await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    let mut nullable_accounts_catalog = test_accounts_catalog();
    nullable_accounts_catalog.relation_schema.relation_id = "nullable_accounts".to_string();
    nullable_accounts_catalog.relation_schema.relation_name = "nullable_accounts".to_string();
    nullable_accounts_catalog.datafusion_registration.name = "nullable_accounts".to_string();
    nullable_accounts_catalog.incremental_relation.relation_id = "nullable_accounts".to_string();
    nullable_accounts_catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "limit")
        .expect("nullable accounts limit column")
        .nullable = true;
    let nullable_schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&nullable_accounts_catalog.relation_schema)
            .expect("nullable accounts catalog should fingerprint");
    nullable_accounts_catalog.schema_fingerprint = nullable_schema_fingerprint.clone();
    nullable_accounts_catalog
        .incremental_relation
        .schema_fingerprint = nullable_schema_fingerprint;

    for catalog in [
        test_scores_catalog(),
        test_accounts_catalog(),
        nullable_accounts_catalog,
    ] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog
            }),
        )
        .await;
        assert_eq!(
            relation_response.0,
            StatusCode::CREATED,
            "{relation_response:?}"
        );
    }

    let input_relation_refs = vec![
        InputRelationRef {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
        },
        InputRelationRef {
            relation_id: "accounts".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
        },
    ];
    let nullable_input_relation_refs = vec![
        InputRelationRef {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
        },
        InputRelationRef {
            relation_id: "nullable_accounts".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
        },
    ];
    let plain_cases = [
        ("scalar_sum", "sum(a.limit)"),
        ("scalar_count", "count(a.limit)"),
        ("scalar_min", "min(a.limit)"),
        ("scalar_max", "max(a.limit)"),
        ("scalar_avg", "avg(a.limit)"),
    ];
    for (view_id, aggregate) in plain_cases {
        let view_request = CreateViewRequest {
            view_id: view_id.to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: input_relation_refs.clone(),
            input_relations: Vec::new(),
            sql: format!(
                "select s.user_id, s.score from scores s where s.score > (select {aggregate} from accounts a)"
            ),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("scalar aggregate plain-function matrix".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
            correction_horizon_ns: None,
        };
        let view_response =
            call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
        assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
        assert_eq!(view_response.1["query_enabled"], true, "{view_response:?}");
    }

    for (view_id, aggregate) in [
        ("scalar_nullable_sum", "sum(a.limit)"),
        ("scalar_nullable_count", "count(a.limit)"),
        ("scalar_nullable_min", "min(a.limit)"),
        ("scalar_nullable_max", "max(a.limit)"),
        ("scalar_nullable_avg", "avg(a.limit)"),
        ("scalar_nullable_count_star", "count(*)"),
    ] {
        let view_request = CreateViewRequest {
            view_id: view_id.to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: nullable_input_relation_refs.clone(),
            input_relations: Vec::new(),
            sql: format!(
                "select s.user_id, s.score from scores s where s.score > (select {aggregate} from nullable_accounts a)"
            ),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("scalar aggregate nullable-input matrix".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
            correction_horizon_ns: None,
        };
        let view_response =
            call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
        assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    }

    let key_only_view_request = CreateViewRequest {
        view_id: "scalar_sum_key_only".to_string(),
        url_path: None,
        output_relation_id: None,
        input_relation_id: String::new(),
        input_relation_version: String::new(),
        input_relation_refs: input_relation_refs.clone(),
        input_relations: Vec::new(),
        sql: "select s.user_id from scores s where s.score > (select sum(a.limit) from accounts a)"
            .to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("scalar aggregate key-only projection".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
        correction_horizon_ns: None,
    };
    let key_only_response = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!(key_only_view_request),
    )
    .await;
    assert_eq!(
        key_only_response.0,
        StatusCode::CREATED,
        "{key_only_response:?}"
    );
    assert_eq!(
        key_only_response.1["query_enabled"], true,
        "{key_only_response:?}"
    );

    for (view_id, sql, modifier) in [
        (
            "scalar_sum_distinct_rejected",
            "select s.user_id, s.score from scores s where s.score > (select sum(distinct a.limit) from accounts a)",
            "DISTINCT",
        ),
        (
            "scalar_count_distinct_rejected",
            "select s.user_id, s.score from scores s where s.score > (select count(distinct a.limit) from accounts a)",
            "DISTINCT",
        ),
        (
            "scalar_sum_filter_rejected",
            "select s.user_id, s.score from scores s where s.score > (select sum(a.limit) filter (where a.limit > 60) from accounts a)",
            "FILTER",
        ),
        (
            "scalar_typed_projection_rejected",
            "select s.user_id, lower(s.user_id) as normalized_user from scores s where s.score > (select sum(a.limit) from accounts a)",
            "UNSUPPORTED VIEW SQL",
        ),
    ] {
        let view_request = CreateViewRequest {
            view_id: view_id.to_string(),
            url_path: None,
            output_relation_id: None,
            input_relation_id: String::new(),
            input_relation_version: String::new(),
            input_relation_refs: input_relation_refs.clone(),
            input_relations: Vec::new(),
            sql: sql.to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("scalar aggregate modifier must fail closed".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
            correction_horizon_ns: None,
        };
        let response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
        assert_eq!(response.0, StatusCode::BAD_REQUEST, "{response:?}");
        let error = response.1["error"].as_str().unwrap_or_default();
        assert!(
            !error.is_empty() && error.to_ascii_uppercase().contains(modifier),
            "modifier rejection should identify {modifier}: {response:?}"
        );
    }

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scalar-modifier-scores-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 100, "delta": 1},
                        {"user_id": "bob", "score": 140, "delta": 1},
                        {"user_id": "carol", "score": 190, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scalar-modifier-accounts-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "a1", "limit": 50, "tier": "gold", "delta": 1},
                        {"account_id": "a2", "limit": 50, "tier": "silver", "delta": 1},
                        {"account_id": "a3", "limit": 80, "tier": "gold", "delta": 1}
                    ]
                },
                {
                    "relation_id": "nullable_accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scalar-modifier-nullable-accounts-stream",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "na1", "limit": 50, "tier": "gold", "delta": 1},
                        {"account_id": "na2", "limit": null, "tier": "silver", "delta": 1},
                        {"account_id": "na3", "limit": 80, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(
        ingest.1["materialization"]["status"], "completed",
        "{ingest:?}"
    );

    for (view_id, expected_rows) in [
        ("scalar_sum", json!([{"user_id": "carol", "score": 190}])),
        (
            "scalar_count",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_min",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_max",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_avg",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
    ] {
        let query = call_json(
            &router,
            Method::POST,
            &format!("/v1/views/{view_id}/query"),
            json!({}),
        )
        .await;
        assert_eq!(query.0, StatusCode::OK, "{query:?}");
        assert_eq!(query.1["rows"], expected_rows, "{view_id}: {query:?}");
    }

    let key_only_query = call_json(
        &router,
        Method::POST,
        "/v1/views/scalar_sum_key_only/query",
        json!({}),
    )
    .await;
    assert_eq!(key_only_query.0, StatusCode::OK, "{key_only_query:?}");
    assert_eq!(
        key_only_query.1["rows"],
        json!([{"user_id": "carol"}]),
        "{key_only_query:?}"
    );

    for (view_id, expected_rows) in [
        (
            "scalar_nullable_sum",
            json!([
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_nullable_count",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_nullable_min",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_nullable_max",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_nullable_avg",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_nullable_count_star",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
    ] {
        let query = call_json(
            &router,
            Method::POST,
            &format!("/v1/views/{view_id}/query"),
            json!({}),
        )
        .await;
        assert_eq!(query.0, StatusCode::OK, "{query:?}");
        assert_eq!(query.1["rows"], expected_rows, "{view_id}: {query:?}");
    }

    let views = call_json(&router, Method::GET, "/v1/views", Value::Null).await;
    assert_eq!(views.0, StatusCode::OK, "{views:?}");
    let view_ids = views.1["views"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|view| view["view_id"].as_str())
        .collect::<Vec<_>>();
    assert!(!view_ids.contains(&"scalar_sum_distinct_rejected"));
    assert!(!view_ids.contains(&"scalar_count_distinct_rejected"));
    assert!(!view_ids.contains(&"scalar_sum_filter_rejected"));
    assert!(!view_ids.contains(&"scalar_typed_projection_rejected"));

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-scalar-aggregate-modifiers-restarted",
        true,
    )
    .await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        12
    );
    let restarted_router = app(restarted_state);
    for (view_id, expected_rows) in [
        ("scalar_sum", json!([{"user_id": "carol", "score": 190}])),
        (
            "scalar_count",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_min",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_max",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_avg",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        ("scalar_sum_key_only", json!([{"user_id": "carol"}])),
        (
            "scalar_nullable_sum",
            json!([
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_nullable_count",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_nullable_min",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_nullable_max",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_nullable_avg",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
        (
            "scalar_nullable_count_star",
            json!([
                {"user_id": "alice", "score": 100},
                {"user_id": "bob", "score": 140},
                {"user_id": "carol", "score": 190}
            ]),
        ),
    ] {
        let query = call_json(
            &restarted_router,
            Method::POST,
            &format!("/v1/views/{view_id}/query"),
            json!({}),
        )
        .await;
        assert_eq!(query.0, StatusCode::OK, "{query:?}");
        assert_eq!(query.1["rows"], expected_rows, "{query:?}");
    }
}

#[test]
fn standing_runtime_output_decoder_routes_native_scalar_and_temporal_payloads() {
    let mut scalar_checkpoint = test_runtime_checkpoint(Vec::new());
    scalar_checkpoint.state_payload = Some(RuntimeCheckpointStatePayload {
        codec_identity: scalar_checkpoint.checkpoint_codec_identity.clone(),
        payload: velorix_core::arrow_state::to_state(
            &json!({"runtime_kind": "scalar_aggregate_filter"}),
        )
        .unwrap(),
    });
    assert_eq!(
        crate::query_serving::standing_runtime_output_aggregate_outputs_for_checkpoint(
            &scalar_checkpoint
        )
        .expect("scalar checkpoint payload should route without generic decoding"),
        None
    );

    let mut temporal_checkpoint = test_runtime_checkpoint(Vec::new());
    temporal_checkpoint.state_payload = Some(RuntimeCheckpointStatePayload {
        codec_identity: temporal_checkpoint.checkpoint_codec_identity.clone(),
        payload: velorix_core::arrow_state::to_state(&json!({"runtime_kind": "temporal_join_v1"}))
            .unwrap(),
    });
    assert_eq!(
        crate::query_serving::standing_runtime_output_aggregate_outputs_for_checkpoint(
            &temporal_checkpoint
        )
        .expect("temporal checkpoint payload should route to native row paging"),
        Some(Vec::new())
    );

    for runtime_kind in [
        "analytic_window_frame",
        "two_input_semi_anti_join_project_v1",
    ] {
        let mut checkpoint = test_runtime_checkpoint(Vec::new());
        checkpoint.state_payload = Some(RuntimeCheckpointStatePayload {
            codec_identity: checkpoint.checkpoint_codec_identity.clone(),
            payload: velorix_core::arrow_state::to_state(&json!({"runtime_kind": runtime_kind}))
                .unwrap(),
        });
        assert_eq!(
            crate::query_serving::standing_runtime_output_aggregate_outputs_for_checkpoint(
                &checkpoint
            )
            .expect("native non-aggregate checkpoint should route without plan decoding"),
            None
        );
    }

    let generic_plan = json!({
        "input_relation_id": "scores",
        "group_key_column_id": "user_id",
        "output_key_column_id": "user_id",
        "sum_value_column_id": "score",
        "aggregate_outputs": [],
        "predicate": null
    });
    for runtime_kind in [Some("single_key_sum_count"), None] {
        let mut checkpoint = test_runtime_checkpoint(Vec::new());
        let mut payload = json!({"plan": generic_plan});
        if let Some(runtime_kind) = runtime_kind {
            payload["runtime_kind"] = json!(runtime_kind);
        }
        checkpoint.state_payload = Some(RuntimeCheckpointStatePayload {
            codec_identity: checkpoint.checkpoint_codec_identity.clone(),
            payload: velorix_core::arrow_state::to_state(&payload).unwrap(),
        });
        let aggregate_outputs =
            crate::query_serving::standing_runtime_output_aggregate_outputs_for_checkpoint(
                &checkpoint,
            )
            .expect("generic single-key checkpoint should decode its plan")
            .expect("single-key checkpoint should expose aggregate outputs");
        assert_eq!(aggregate_outputs.len(), 2);
    }

    let mut unknown_checkpoint = test_runtime_checkpoint(Vec::new());
    unknown_checkpoint.state_payload = Some(RuntimeCheckpointStatePayload {
        codec_identity: unknown_checkpoint.checkpoint_codec_identity.clone(),
        payload: velorix_core::arrow_state::to_state(
            &json!({"runtime_kind": "future_runtime_kind"}),
        )
        .unwrap(),
    });
    let error = crate::query_serving::standing_runtime_output_aggregate_outputs_for_checkpoint(
        &unknown_checkpoint,
    )
    .expect_err("unknown explicit runtime kinds must fail closed");
    assert!(error.to_string().contains("future_runtime_kind"));

    for runtime_kind in [
        "single_key_sum_count",
        "two_input_join_sum_count",
        "tumbling_event_time_aggregate",
    ] {
        let mut checkpoint = test_runtime_checkpoint(Vec::new());
        checkpoint.state_payload = Some(RuntimeCheckpointStatePayload {
            codec_identity: checkpoint.checkpoint_codec_identity.clone(),
            payload: velorix_core::arrow_state::to_state(&json!({"runtime_kind": runtime_kind}))
                .unwrap(),
        });
        let error = crate::query_serving::standing_runtime_output_aggregate_outputs_for_checkpoint(
            &checkpoint,
        )
        .expect_err("aggregate runtime kinds must not decode as generic without plan metadata");
        assert!(error.to_string().contains(runtime_kind), "{error:?}");
    }
}

#[tokio::test]
async fn rest_scalar_decimal_aggregate_admission_rejects_nullable_and_nonnullable_inputs() {
    fn decimal_catalog(
        mut catalog: VelorixRelationCatalogV1,
        relation_id: &str,
        value_column_id: &str,
        nullable: bool,
    ) -> VelorixRelationCatalogV1 {
        catalog.relation_schema.relation_id = relation_id.to_string();
        catalog.relation_schema.relation_name = relation_id.to_string();
        catalog.datafusion_registration.name = relation_id.to_string();
        catalog.incremental_relation.relation_id = relation_id.to_string();
        let value = catalog
            .relation_schema
            .columns
            .iter_mut()
            .find(|column| column.column_id == value_column_id)
            .expect("decimal test value column");
        value.logical_type = VelorixLogicalTypeV1::Decimal {
            precision: 12,
            scale: 2,
        };
        value.physical_arrow_type = ArrowPhysicalTypeV1::Decimal128 {
            precision: 12,
            scale: 2,
        };
        value.nullable = nullable;
        let schema_fingerprint =
            SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
        catalog.schema_fingerprint = schema_fingerprint.clone();
        catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
        catalog
    }

    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-scalar-decimal-admission",
        false,
    )
    .await;
    let router = app(state);
    let catalogs = [
        decimal_catalog(
            test_scores_catalog(),
            "decimal_scores_nonnull",
            "score",
            false,
        ),
        decimal_catalog(
            test_accounts_catalog(),
            "decimal_accounts_nonnull",
            "limit",
            false,
        ),
        decimal_catalog(
            test_scores_catalog(),
            "decimal_scores_nullable",
            "score",
            true,
        ),
        decimal_catalog(
            test_accounts_catalog(),
            "decimal_accounts_nullable",
            "limit",
            true,
        ),
    ];
    for catalog in catalogs {
        let response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({"catalog": catalog}),
        )
        .await;
        assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
    }

    let mut rejected_view_ids = Vec::new();
    for (pair_name, outer_relation_id, scalar_relation_id) in [
        (
            "nonnull",
            "decimal_scores_nonnull",
            "decimal_accounts_nonnull",
        ),
        (
            "nullable",
            "decimal_scores_nullable",
            "decimal_accounts_nullable",
        ),
    ] {
        for aggregate in ["sum", "avg", "min", "max"] {
            let view_id = format!("scalar_decimal_{aggregate}_{pair_name}_rejected");
            let response = call_json(
                &router,
                Method::POST,
                "/v1/views",
                json!({
                    "view_id": view_id,
                    "input_relation_refs": [
                        {"relation_id": outer_relation_id, "relation_version": "2026-05-24.v1"},
                        {"relation_id": scalar_relation_id, "relation_version": "2026-05-24.v1"}
                    ],
                    "sql": format!(
                        "select s.user_id from {outer_relation_id} s where s.score > (select {aggregate}(a.limit) from {scalar_relation_id} a)"
                    ),
                    "source_kind": "standing_view",
                    "response_formats": ["json"]
                }),
            )
            .await;
            assert_eq!(response.0, StatusCode::BAD_REQUEST, "{response:?}");
            assert!(!response.1["error"].as_str().unwrap_or_default().is_empty());
            rejected_view_ids.push(view_id);
        }
    }

    let views = call_json(&router, Method::GET, "/v1/views", Value::Null).await;
    assert_eq!(views.0, StatusCode::OK, "{views:?}");
    let view_ids = views.1["views"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|view| view["view_id"].as_str())
        .collect::<Vec<_>>();
    for view_id in rejected_view_ids {
        assert!(
            !view_ids.contains(&view_id.as_str()),
            "{view_id} was installed"
        );
    }
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            {"user_id": "bob", "sum": null, "count": 1}
        ])
    );
}

#[tokio::test]
async fn rest_order_by_limit_top_k_view_materializes_outputs() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-top-k-owner", false).await;
    let router = app(state.clone());

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_catalog()
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
            correction_horizon_ns: None,
        };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(
        read_test_active_view(&state, "top_purchaser")
            .await
            .unwrap()
            .api
            .unwrap()
            .url_path
            .as_deref(),
        Some("/purchases/top")
    );

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
            "catalog": test_purchases_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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

/// Test-only capability decorator.  The in-memory backend deliberately reports
/// non-durable capabilities; this wrapper models the durable metadata contract
/// while forwarding every operation to the real in-memory implementation.
#[derive(Clone)]
struct DurableCapabilityMetaStore {
    inner: Arc<InMemoryMetaStore>,
    guard_race: Option<Arc<GuardRaceHook>>,
    renewal_delay: Option<Arc<RenewalDelayHook>>,
}

struct RenewalDelayHook {
    enabled: AtomicBool,
    reached: Notify,
    release: Notify,
    before_commit: bool,
}

struct GuardRaceHook {
    reached: Notify,
    release: Notify,
    fire_once: AtomicBool,
    all_checkpoints: bool,
}

impl DurableCapabilityMetaStore {
    fn new(inner: InMemoryMetaStore) -> Self {
        Self {
            inner: Arc::new(inner),
            guard_race: None,
            renewal_delay: None,
        }
    }

    fn with_guard_race(mut self, guard_race: Arc<GuardRaceHook>) -> Self {
        self.guard_race = Some(guard_race);
        self
    }
}

#[async_trait]
impl MetaStore for DurableCapabilityMetaStore {
    async fn read_meta_store_capabilities(
        &self,
    ) -> Result<meta::MetaStoreCapabilities, meta::MetaStoreError> {
        let mut capabilities = self.inner.read_meta_store_capabilities().await?;
        capabilities.relation_ingest.durable_across_restart = true;
        Ok(capabilities)
    }

    async fn store_relation_catalog(
        &self,
        catalog: VelorixRelationCatalogV1,
    ) -> Result<StoreRelationCatalogOutcome, MetaStoreError> {
        self.inner.store_relation_catalog(catalog).await
    }

    async fn read_relation_catalog(
        &self,
        relation_id: &str,
        relation_version: &str,
    ) -> Result<VelorixRelationCatalogV1, MetaStoreError> {
        self.inner
            .read_relation_catalog(relation_id, relation_version)
            .await
    }

    async fn read_demand_metadata_batch(
        &self,
        request: meta::ReadDemandMetadataBatchRequest,
    ) -> Result<meta::ReadDemandMetadataBatchResponse, MetaStoreError> {
        self.inner.read_demand_metadata_batch(request).await
    }

    async fn read_active_view_registry_snapshot(
        &self,
        request: meta::ActiveViewRegistryReadRequestV1,
    ) -> Result<meta::ActiveViewRegistrySnapshotV1, MetaStoreError> {
        self.inner.read_active_view_registry_snapshot(request).await
    }

    async fn create_active_view_record(
        &self,
        request: meta::CreateActiveViewRecordRequestV1,
    ) -> Result<meta::CreateActiveViewRecordOutcomeV1, MetaStoreError> {
        self.inner.create_active_view_record(request).await
    }

    async fn compare_exchange_active_view_record(
        &self,
        request: meta::CompareExchangeActiveViewRecordRequestV1,
    ) -> Result<meta::CompareExchangeActiveViewRecordOutcomeV1, MetaStoreError> {
        self.inner
            .compare_exchange_active_view_record(request)
            .await
    }

    async fn import_active_view_registry(
        &self,
        request: meta::ImportActiveViewRegistryRequestV1,
    ) -> Result<meta::ImportActiveViewRegistryOutcomeV1, MetaStoreError> {
        self.inner.import_active_view_registry(request).await
    }

    async fn promote_view_bootstrap_with_active_view_record(
        &self,
        request: meta::PromoteViewBootstrapWithActiveViewRecordRequestV1,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        self.inner
            .promote_view_bootstrap_with_active_view_record(request)
            .await
    }

    async fn reserve_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        self.inner.reserve_ingest_range(reservation).await
    }

    async fn commit_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<CommitIngestRangeOutcome, MetaStoreError> {
        self.inner.commit_ingest_range(reservation).await
    }

    async fn commit_ingest_ranges(
        &self,
        reservations: Vec<IngestRangeReservation>,
    ) -> Result<Vec<CommitIngestRangeOutcome>, MetaStoreError> {
        self.inner.commit_ingest_ranges(reservations).await
    }

    async fn acquire_standing_runtime_owner(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        self.inner.acquire_standing_runtime_owner(request).await
    }

    async fn renew_standing_runtime_owner(
        &self,
        owner: StandingRuntimeOwnerToken,
        ttl_ms: u64,
    ) -> Result<StandingRuntimeOwnerClaim, MetaStoreError> {
        let delay = self
            .renewal_delay
            .as_ref()
            .filter(|hook| hook.enabled.load(Ordering::SeqCst));
        if let Some(hook) = delay.filter(|hook| hook.before_commit) {
            hook.reached.notify_one();
            hook.release.notified().await;
        }
        let claim = self
            .inner
            .renew_standing_runtime_owner(owner, ttl_ms)
            .await?;
        if let Some(hook) = delay.filter(|hook| !hook.before_commit) {
            hook.reached.notify_one();
            hook.release.notified().await;
        }
        Ok(claim)
    }

    async fn read_standing_runtime_owner(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeOwnerClaim>, MetaStoreError> {
        self.inner
            .read_standing_runtime_owner(tenant_id, program_id, view_id)
            .await
    }

    async fn publish_standing_runtime_checkpoint(
        &self,
        request: PublishStandingRuntimeCheckpointRequest,
    ) -> Result<PublishStandingRuntimeCheckpointOutcome, MetaStoreError> {
        if request.expected_relation_source_cuts.is_some()
            || self
                .guard_race
                .as_ref()
                .is_some_and(|hook| hook.all_checkpoints)
        {
            if let Some(guard_race) = &self.guard_race {
                if guard_race.fire_once.swap(false, Ordering::SeqCst) {
                    guard_race.reached.notify_one();
                    guard_race.release.notified().await;
                }
            }
        }
        self.inner
            .publish_standing_runtime_checkpoint(request)
            .await
    }

    async fn read_standing_runtime_checkpoint(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeCheckpointPointer>, MetaStoreError> {
        self.inner
            .read_standing_runtime_checkpoint(tenant_id, program_id, view_id)
            .await
    }

    async fn read_view_dependency_graph_revision(
        &self,
        tenant_id: &str,
    ) -> Result<u64, MetaStoreError> {
        self.inner
            .read_view_dependency_graph_revision(tenant_id)
            .await
    }

    async fn acquire_relation_partition_authority(
        &self,
        request: meta::AcquireRelationPartitionAuthorityRequest,
    ) -> Result<meta::AcquireRelationPartitionAuthorityOutcome, MetaStoreError> {
        self.inner
            .acquire_relation_partition_authority(request)
            .await
    }

    async fn read_relation_partition_authority(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Option<meta::RelationPartitionAuthorityToken>, MetaStoreError> {
        self.inner.read_relation_partition_authority(key).await
    }

    async fn reserve_relation_authoritative_ingest_range(
        &self,
        request: meta::ReserveRelationAuthoritativeIngestRangeRequest,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        self.inner
            .reserve_relation_authoritative_ingest_range(request)
            .await
    }

    async fn publish_relation_ingest_reservation(
        &self,
        request: meta::PublishRelationIngestReservationRequest,
    ) -> Result<meta::PublishIngestReservationOutcome, MetaStoreError> {
        self.inner
            .publish_relation_ingest_reservation(request)
            .await
    }

    async fn read_relation_authoritative_ingest_publication(
        &self,
        request_id: &str,
    ) -> Result<Option<meta::RelationAuthoritativeIngestPublication>, MetaStoreError> {
        self.inner
            .read_relation_authoritative_ingest_publication(request_id)
            .await
    }

    async fn list_relation_authoritative_ingest_publications(
        &self,
        key: &RelationPartitionAuthorityKey,
    ) -> Result<Vec<meta::RelationAuthoritativeIngestPublication>, MetaStoreError> {
        self.inner
            .list_relation_authoritative_ingest_publications(key)
            .await
    }

    async fn capture_ingest_source_cut(
        &self,
        request: CaptureIngestSourceCutRequest,
    ) -> Result<meta::IngestSourceCutV1, MetaStoreError> {
        self.inner.capture_ingest_source_cut(request).await
    }

    async fn capture_relation_ingest_source_cut(
        &self,
        request: CaptureRelationIngestSourceCutRequest,
    ) -> Result<meta::RelationIngestSourceCutV1, MetaStoreError> {
        self.inner.capture_relation_ingest_source_cut(request).await
    }

    async fn capture_relation_ingest_source_cuts(
        &self,
        request: CaptureRelationIngestSourceCutsRequest,
    ) -> Result<Vec<meta::RelationIngestSourceIdentityCutV1>, MetaStoreError> {
        self.inner
            .capture_relation_ingest_source_cuts(request)
            .await
    }

    async fn begin_view_bootstrap(
        &self,
        request: BeginViewBootstrapRequest,
    ) -> Result<BeginViewBootstrapOutcome, MetaStoreError> {
        self.inner.begin_view_bootstrap(request).await
    }

    async fn read_view_bootstrap(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<ViewBootstrapControlV1>, MetaStoreError> {
        self.inner
            .read_view_bootstrap(tenant_id, program_id, view_id)
            .await
    }

    async fn fix_view_bootstrap_activation_cut(
        &self,
        request: FixViewBootstrapActivationCutRequest,
    ) -> Result<FixViewBootstrapActivationCutOutcome, MetaStoreError> {
        self.inner.fix_view_bootstrap_activation_cut(request).await
    }

    async fn promote_view_bootstrap(
        &self,
        request: PromoteViewBootstrapRequest,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        self.inner.promote_view_bootstrap(request).await
    }
}

async fn test_authoritative_api_state_with_store(
    store: Arc<dyn ObjectStore>,
    owner_id: &str,
    meta_store: Arc<dyn MetaStore>,
) -> ApiState {
    let validated = validate_operator_authority(
        ObjectStoreAuthorityRef {
            store_id: "test-authoritative".to_string(),
            namespace: "unit-authoritative".to_string(),
        },
        Arc::clone(&store),
        "memory",
        "api-authoritative-test",
    )
    .await
    .unwrap();
    let state = ApiState::from_validated_authority_with_ingest_admission_startup(
        validated,
        "api-authoritative-process",
        false,
    )
    .await
    .unwrap();
    let state = state.with_view_bootstrap_meta_store(Arc::clone(&meta_store));
    with_initialized_active_view_meta_store(state, meta_store)
        .await
        .with_authoritative_relation_ingest(
            owner_id,
            "unit-authoritative",
            "test-authoritative",
            "memory",
            "api-authoritative-test",
            30_000,
        )
        .unwrap()
}

#[tokio::test]
async fn authoritative_relation_ingest_gate_rejects_missing_meta_capability() {
    let meta = InMemoryMetaStore::default();
    let error = validate_authoritative_relation_ingest_startup(&meta)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("metadata capability"));
    assert!(error.to_string().contains("durable_across_restart"));
}

#[tokio::test]
async fn authoritative_relation_ingest_requires_stable_owner_id() {
    let state = test_api_state().await;
    let error = match state.with_authoritative_relation_ingest(
        " ",
        "unit-authoritative",
        "test-authoritative",
        "memory",
        "api-authoritative-test",
        30_000,
    ) {
        Ok(_) => panic!("empty owner id unexpectedly enabled authoritative ingest"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("stable non-empty owner_id"));
}

#[tokio::test]
async fn authoritative_source_first_backfill_discovers_partitions_without_runtime_frontiers() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta: Arc<dyn MetaStore> =
        Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let mut state =
        test_authoritative_api_state_with_store(store.clone(), "source-first-owner", meta.clone())
            .await;
    let mut router = app(state.clone());
    let response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog":test_scores_catalog()}),
    )
    .await;
    assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
    for (stream, score) in [("first-stream", 3), ("second-stream", 5)] {
        let response = call_json(
            &router,
            Method::POST,
            "/v1/relations/scores/ingest",
            json!({"relation_version":"2026-05-24.v1","stream_id":stream,"partition_id":0,
                "start_offset_inclusive":0,"rows":[{"user_id":"oak","score":score,"delta":1}]}),
        )
        .await;
        assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
    }
    let response = call_json(&router, Method::POST, "/v1/views",
        json!({"view_id":"source_first_total","input_relation_id":"scores",
            "input_relation_version":"2026-05-24.v1",
            "sql":"SELECT user_id, SUM(score) AS total, COUNT(*) AS count FROM scores GROUP BY user_id"})).await;
    assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
    assert_eq!(response.1["coverage"]["state"], "backfill_required");
    // Neither a normal pending declaration nor an older incorrectly Running
    // declaration may materialize source data during process reconstruction.
    for legacy_running in [false, true] {
        let active = read_test_active_view(&state, "source_first_total")
            .await
            .unwrap();
        if legacy_running {
            update_test_active_view_lifecycle(
                &state,
                &active.spec.view_id,
                &active.spec_hash,
                MaterializedViewLifecycleStatus::standing_runtime(),
            )
            .await
            .unwrap();
        }
        if legacy_running {
            // Process reconstruction is a lease takeover scenario. Expire the
            // previous fixture owner's lease explicitly before starting the
            // replacement API state; do not bypass the fencing check.
            let identity = active_standing_runtime_identity(&active).unwrap();
            let owner = meta
                .read_standing_runtime_owner(
                    &identity.tenant_id,
                    &identity.program_id,
                    &active.spec.view_id,
                )
                .await
                .unwrap()
                .expect("restored fixture runtime has an owner lease");
            meta.renew_standing_runtime_owner(standing_runtime_owner_token_from_claim(&owner), 1)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        state = test_authoritative_api_state_with_store(
            store.clone(),
            "source-first-owner",
            meta.clone(),
        )
        .await;
        if legacy_running {
            // The same ingest preflight that triggers reconciliation must use
            // the refreshed pending lifecycle, not demand a restored runtime.
            ensure_standing_runtimes_for_ingest(
                &state,
                &IngestRowsRequest {
                    relation_id: "scores".into(),
                    relation_version: "2026-05-24.v1".into(),
                    stream_id: "first-stream".into(),
                    partition_id: 0,
                    start_offset_inclusive: 1,
                    event_time_watermark: None,
                    rows: vec![json!({"user_id":"oak","score":7,"delta":1})],
                },
            )
            .await
            .unwrap();
        }
        assert_eq!(
            state
                .restore_standing_program_runtimes_from_active_views()
                .await
                .unwrap(),
            1
        );
        let active = read_test_active_view(&state, "source_first_total")
            .await
            .unwrap();
        if !legacy_running {
            assert!(view_has_backfill_required_lag(&active));
        }
        let identity = active_standing_runtime_identity(&active).unwrap();
        assert!(state
            .standing_runtime(identity, &active.spec.view_id)
            .unwrap()
            .is_none());
        assert!(
            read_latest_standing_runtime_checkpoint(&state, identity, &active.spec.view_id)
                .await
                .unwrap()
                .is_none()
        );
        router = app(state.clone());
        // A query now starts demand work; inspect checkpoint absence above
        // instead so this loop continues to prove startup performs no work.
    }
    for _ in 0..2 {
        let response = call_json(
            &router,
            Method::POST,
            "/v1/views/source_first_total/backfill",
            json!({"batch_limit":10}),
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{response:?}");
        let response = call_json(
            &router,
            Method::POST,
            "/v1/views/source_first_total/query",
            json!({}),
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{response:?}");
        assert_eq!(
            response.1["rows"],
            json!([{"user_id":"oak","total":8,"count":2}])
        );
    }
    for (view, input) in [
        ("source_first_map", "source_first_total"),
        ("source_first_tail", "source_first_map"),
    ] {
        let response = call_json(
            &router,
            Method::POST,
            "/v1/views",
            json!({
                "view_id":view,"input_relation_id":input,"input_relation_version":"v1",
                "sql":format!("SELECT * FROM {input} WHERE total IS NOT NULL")
            }),
        )
        .await;
        assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
        for _ in 0..2 {
            let response = call_json(
                &router,
                Method::POST,
                &format!("/v1/views/{view}/backfill"),
                json!({"batch_limit":1}),
            )
            .await;
            assert_eq!(response.0, StatusCode::OK, "{response:?}");
            let response = call_json(
                &router,
                Method::POST,
                &format!("/v1/views/{view}/query"),
                json!({}),
            )
            .await;
            assert_eq!(response.0, StatusCode::OK, "{response:?}");
            assert_eq!(
                response.1["rows"],
                json!([{"user_id":"oak","total":8,"count":2}])
            );
        }
    }
}

#[tokio::test]
async fn authoritative_relation_ingest_materializes_query_and_recovers_from_meta_refs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta: Arc<dyn MetaStore> =
        Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let state = test_authoritative_api_state_with_store(
        Arc::clone(&store),
        "stable-authoritative-owner",
        meta.clone(),
    )
    .await;
    let router = app(state.clone());

    let relation = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": test_scores_catalog()}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "{relation:?}");
    let view = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "authoritative_scores_by_user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "{view:?}");
    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "authoritative-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    let retry = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "authoritative-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
        }),
    )
    .await;
    assert_eq!(retry.0, StatusCode::OK, "{retry:?}");
    assert_eq!(retry.1["outcome"], "duplicate");

    let canonical_key = ObjectKey::ingest_batch("authoritative-scores-stream", 0, 0, 1).unwrap();
    assert!(store
        .get(&Path::from(canonical_key.as_str()))
        .await
        .is_err());
    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/authoritative_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([{"user_id": "alice", "sum": 10, "count": 1}])
    );

    let restarted = test_authoritative_api_state_with_store(
        Arc::clone(&store),
        "stable-authoritative-owner",
        meta,
    )
    .await;
    let restarted_query = call_json(
        &app(restarted),
        Method::POST,
        "/v1/views/authoritative_scores_by_user/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(restarted_query.1["rows"], query.1["rows"]);
}

#[tokio::test]
async fn authoritative_legacy_single_key_rebuild_replays_source_and_preserves_old_objects() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let guard_race = Arc::new(GuardRaceHook {
        reached: Notify::new(),
        release: Notify::new(),
        fire_once: AtomicBool::new(true),
        all_checkpoints: false,
    });
    let meta: Arc<dyn MetaStore> = Arc::new(
        DurableCapabilityMetaStore::new(InMemoryMetaStore::default())
            .with_guard_race(Arc::clone(&guard_race)),
    );
    let state = test_authoritative_api_state_with_store(
        Arc::clone(&store),
        "legacy-rebuild-owner",
        meta.clone(),
    )
    .await;
    let router = app(state.clone());
    let relation = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": test_scores_catalog()}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "{relation:?}");
    let view = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "legacy_rebuild_scores",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) filter (where score > 0) as sum, count(*) filter (where score > 0) as count from scores group by user_id",
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "{view:?}");
    seed_legacy_eager_checkpoint_for_compatibility(&state, "legacy_rebuild_scores").await;
    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "legacy-rebuild-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let active = list_test_active_views(&state)
        .await
        .unwrap()
        .into_iter()
        .find(|view| view.spec.view_id == "legacy_rebuild_scores")
        .expect("legacy view should be active");
    let identity = active_standing_runtime_identity(&active).unwrap();
    let old_record =
        read_latest_standing_runtime_checkpoint(&state, identity, "legacy_rebuild_scores")
            .await
            .unwrap()
            .expect("ingest should publish a checkpoint");
    let old_checkpoint_object = old_record.checkpoint_key.clone();
    let old_state_object = old_record.checkpoint.state_root.object_key.clone();
    let old_pointer = standing_runtime_checkpoint_pointer_from_record(&old_record);
    let mut legacy_record = old_record.clone();
    let mut payload: Value = velorix_core::arrow_state::from_state(
        &legacy_record
            .checkpoint
            .state_payload
            .as_ref()
            .expect("checkpoint read should hydrate state")
            .payload,
    )
    .unwrap();
    payload
        .as_object_mut()
        .expect("generic runtime payload should be an object")
        .remove("group_input_counts");
    payload["engine"]["logical_epoch"] = json!(legacy_record.checkpoint.logical_epoch + 1);
    let payload = velorix_core::arrow_state::to_state(&payload).unwrap();
    let content_hash = stable_bytes_hash(payload.as_bytes());
    legacy_record.checkpoint.logical_epoch += 1;
    legacy_record.checkpoint.output_manifest_refs.clear();
    legacy_record.previous_checkpoint = Some(old_pointer.clone());
    legacy_record.manifest_hash.clear();
    for frontier in &mut legacy_record.checkpoint.output_frontiers {
        frontier.committed_epoch = legacy_record.checkpoint.logical_epoch;
    }
    let state_payload = legacy_record.checkpoint.state_payload.as_mut().unwrap();
    state_payload.payload = payload;
    legacy_record.checkpoint.state_root.content_hash = content_hash.clone();
    let (legacy_state_key, legacy_state_record) =
        standing_runtime_state_payload_record_for_checkpoint(
            &legacy_record.checkpoint,
            "legacy_rebuild_scores",
        )
        .unwrap();
    persist_standing_runtime_state_payload(&state, &legacy_state_key, &legacy_state_record)
        .await
        .unwrap();
    legacy_record.checkpoint.state_root.object_key = legacy_state_key.as_str().to_string();
    legacy_record.checkpoint_key = ObjectKey::standing_runtime_checkpoint(
        &legacy_record.checkpoint.identity.tenant_id,
        &legacy_record.checkpoint.identity.program_id,
        &legacy_record.view_id,
        legacy_record.checkpoint.logical_epoch,
        &content_hash,
    )
    .unwrap()
    .as_str()
    .to_string();
    let legacy_checkpoint_object = legacy_record.checkpoint_key.clone();
    let synthetic_restore = velorix_runtime::materialized_view_runtime::restore_standing_runtime(
        legacy_record.checkpoint.clone(),
    )
    .err()
    .expect("synthetic checkpoint should require the legacy rebuild path");
    assert!(
        synthetic_restore.contains("legacy_single_key_group_counts_missing"),
        "{synthetic_restore}"
    );
    // The runtime schema is old, but its durable body uses the sole Arrow codec.
    legacy_record.checkpoint.state_payload = None;
    store
        .put(
            &Path::from(legacy_checkpoint_object.as_str()),
            checkpoint_record::encode(&legacy_record, checkpoint_record::MAX_ENCODED_RECORD_BYTES)
                .unwrap()
                .into(),
        )
        .await
        .unwrap();
    let owner = state
        .acquire_standing_runtime_owner(identity, "legacy_rebuild_scores")
        .await
        .unwrap()
        .expect("authoritative metadata should fence rebuild");
    meta.publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
        expected_previous: Some(old_pointer),
        candidate: standing_runtime_checkpoint_pointer_from_record(&legacy_record),
        owner,
        expected_relation_source_cuts: None,
    })
    .await
    .unwrap();
    remove_standing_runtime(&state, identity, "legacy_rebuild_scores").unwrap();

    let rebuilt_state = state
        .clone()
        .with_legacy_rebuild_view_id(Some("legacy_rebuild_scores".to_string()));
    let pointer_before_gate = meta
        .read_standing_runtime_checkpoint(
            &identity.tenant_id,
            &identity.program_id,
            "legacy_rebuild_scores",
        )
        .await
        .unwrap();
    let registered = rebuilt_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .expect("ordinary startup only registers authoritative views");
    assert_eq!(registered, 1);
    assert!(rebuilt_state
        .standing_runtime(identity, "legacy_rebuild_scores")
        .unwrap()
        .is_none());
    assert_eq!(
        meta.read_standing_runtime_checkpoint(
            &identity.tenant_id,
            &identity.program_id,
            "legacy_rebuild_scores",
        )
        .await
        .unwrap(),
        pointer_before_gate,
    );
    let wrong_target = rebuilt_state
        .clone()
        .with_legacy_rebuild_view_id(Some("different-view".to_string()));
    let wrong_target_registered = wrong_target
        .restore_standing_program_runtimes_from_active_views_with_legacy_rebuild(true)
        .await
        .expect("a non-target view is registered without migration");
    assert_eq!(wrong_target_registered, 1);
    assert!(wrong_target
        .standing_runtime(identity, "legacy_rebuild_scores")
        .unwrap()
        .is_none());
    let migration_state = rebuilt_state.clone();
    let migration = tokio::spawn(async move {
        migration_state
            .restore_standing_program_runtimes_from_active_views_with_legacy_rebuild(true)
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        guard_race.reached.notified(),
    )
    .await
    .expect("legacy rebuild must reach guarded publication");

    // Commit a real new source batch after the captured cut but before the
    // guarded checkpoint publication. The migration must fail closed, leaving
    // the legacy pointer in place; the next startup retry captures the new cut
    // and replays both source batches.
    let source_state = state.clone();
    let source_request = IngestRowsRequest {
        relation_id: "scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        stream_id: "legacy-rebuild-stream".to_string(),
        partition_id: 0,
        start_offset_inclusive: 1,
        rows: vec![json!({"user_id": "bob", "score": 7, "delta": 1})],
        event_time_watermark: None,
    };
    let source_catalog = read_relation_catalog(
        &source_state,
        &source_request.relation_id,
        &source_request.relation_version,
    )
    .await
    .unwrap();
    let prepared =
        prepare_ingest_batch_with_catalog(&source_state, source_request, source_catalog, None)
            .unwrap();
    let publisher = source_state
        .relation_ingest_publisher(&prepared.catalog, &prepared.request)
        .await
        .unwrap();
    publisher.start().await.unwrap();
    publisher
        .publish(
            prepared.request.start_offset_inclusive,
            prepared.end_offset_exclusive,
            prepared.envelope,
        )
        .await
        .unwrap();

    guard_race.release.notify_one();
    let guarded_error = migration
        .await
        .unwrap()
        .expect_err("post-cut source commit must reject the guarded publication");
    assert!(guarded_error
        .to_string()
        .contains("authoritative relation source cut changed"));
    assert_eq!(
        meta.read_standing_runtime_checkpoint(
            &identity.tenant_id,
            &identity.program_id,
            "legacy_rebuild_scores",
        )
        .await
        .unwrap(),
        pointer_before_gate,
    );

    let restored = rebuilt_state
        .restore_standing_program_runtimes_from_active_views_with_legacy_rebuild(true)
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let query = call_json(
        &app(rebuilt_state.clone()),
        Method::POST,
        "/v1/views/legacy_rebuild_scores/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([
            {"user_id": "alice", "sum": 10, "count": 1},
            {"user_id": "bob", "sum": 7, "count": 1}
        ]),
    );
    assert!(store
        .get(&Path::from(old_checkpoint_object.as_str()))
        .await
        .is_ok());
    assert!(store
        .get(&Path::from(legacy_checkpoint_object.as_str()))
        .await
        .is_ok());
    assert!(store
        .get(&Path::from(old_state_object.as_str()))
        .await
        .is_ok());

    remove_standing_runtime(&rebuilt_state, identity, "legacy_rebuild_scores").unwrap();
    let retried = rebuilt_state
        .restore_standing_program_runtimes_from_active_views_with_legacy_rebuild(true)
        .await
        .unwrap();
    assert_eq!(retried, 1);
    let retried_query = call_json(
        &app(rebuilt_state),
        Method::POST,
        "/v1/views/legacy_rebuild_scores/query",
        json!({}),
    )
    .await;
    assert_eq!(retried_query.0, StatusCode::OK, "{retried_query:?}");
    assert_eq!(retried_query.1["rows"], query.1["rows"]);
}

#[tokio::test]
async fn authoritative_staging_visibility_and_digest_mismatch_fail_closed() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "visibility-owner", false).await;
    let log =
        IngestLog::new_catalog_checked(Arc::clone(&store), state.capabilities.as_ref()).unwrap();
    let staged_key =
        ObjectKey::ingest_staging("visibility-stream", 0, 0, 1, "uncommitted").unwrap();
    log.stage_write(
        staged_key.as_str(),
        bytes::Bytes::from_static(b"uncommitted"),
        stable_bytes_hash(b"uncommitted"),
    )
    .await
    .unwrap();
    assert!(log
        .read_staging(
            staged_key.as_str(),
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        )
        .await
        .is_err());
    assert!(store.get(&Path::from(staged_key.as_str())).await.is_ok());
}

#[tokio::test]
async fn authoritative_multi_batch_request_rejects_before_publication() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta: Arc<dyn MetaStore> =
        Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let state =
        test_authoritative_api_state_with_store(Arc::clone(&store), "multi-batch-owner", meta)
            .await;
    let router = app(state);
    let response = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {"relation_id": "scores", "relation_version": "v1", "stream_id": "s0", "partition_id": 0, "start_offset_inclusive": 0, "rows": [{"user_id": "a", "score": 1, "delta": 1}]},
                {"relation_id": "scores", "relation_version": "v1", "stream_id": "s1", "partition_id": 0, "start_offset_inclusive": 0, "rows": [{"user_id": "b", "score": 2, "delta": 1}]}
            ]
        }),
    )
    .await;
    assert_eq!(response.0, StatusCode::BAD_REQUEST, "{response:?}");
    assert!(response.1["error"]
        .as_str()
        .unwrap_or_default()
        .contains("exactly one relation batch"));
    let mut staged = store.list(Some(&Path::from("v1/ingest-staging")));
    assert!(staged.next().await.is_none());
}

#[tokio::test]
async fn authoritative_concurrent_same_scope_ingest_reuses_registry_publisher() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta: Arc<dyn MetaStore> =
        Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let state = test_authoritative_api_state_with_store(
        Arc::clone(&store),
        "concurrent-authoritative-owner",
        meta,
    )
    .await;
    let router = app(state);
    let relation = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": test_scores_catalog()}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "{relation:?}");
    let request = || {
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "concurrent-authoritative-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
        })
    };
    let (first, second) = tokio::join!(
        call_json(
            &router,
            Method::POST,
            "/v1/relations/scores/ingest",
            request()
        ),
        call_json(
            &router,
            Method::POST,
            "/v1/relations/scores/ingest",
            request()
        )
    );
    for response in [first, second] {
        assert!(
            response.0 == StatusCode::CREATED || response.0 == StatusCode::OK,
            "concurrent ingest unexpectedly failed: {response:?}"
        );
    }
}

#[tokio::test]
async fn authoritative_catalog_not_found_does_not_fallback_to_object_store() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "catalog-owner", false)
        .await
        .with_authoritative_relation_ingest(
            "catalog-owner",
            "unit-authoritative",
            "test-authoritative",
            "memory",
            "api-authoritative-test",
            30_000,
        )
        .unwrap();
    let catalog = test_scores_catalog();
    materialize_relation_catalog_to_object_store(&state, &catalog)
        .await
        .unwrap();
    let state = state.with_meta_store(Arc::new(InMemoryMetaStore::default()));
    let error = read_relation_catalog(&state, "scores", "2026-05-24.v1")
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert!(error.to_string().contains("authoritative metadata"));
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
async fn backfill_owner_renewal_grpc_survives_blocked_origin_executor() {
    let (ready, endpoint) = std::sync::mpsc::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                ready
                    .send(format!("http://{}", listener.local_addr().unwrap()))
                    .unwrap();
                tokio::time::timeout(
                    Duration::from_secs(5),
                    tonic::transport::Server::builder()
                        .add_service(meta::proto::velorix_meta_server::VelorixMetaServer::new(
                            meta::MetaGrpcService::with_bearer_token(
                                meta::InMemoryMetaStore::default(),
                                "lease-test-token",
                            )
                            .unwrap(),
                        ))
                        .serve_with_incoming_shutdown(
                            tokio_stream::wrappers::TcpListenerStream::new(listener),
                            async {
                                let _ = stopped.await;
                            },
                        ),
                )
                .await
                .expect("bounded test server shutdown")
                .unwrap();
            });
    });
    let meta = Arc::new(
        meta::GrpcMetaStore::connect_with_bearer_token(
            endpoint.recv().unwrap(),
            "lease-test-token",
        )
        .await
        .unwrap(),
    );
    let state = test_api_state_with_store(Arc::new(InMemory::new()), "grpc-lease-owner", false)
        .await
        .with_meta_store(meta.clone())
        .with_standing_runtime_owner_ttl_ms(300);
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let identity = &checkpoint.identity;
    let view = &identity.view_ids[0];
    let lease = owner_lease::BackfillOwnerLease::start(&state, identity, view)
        .await
        .unwrap();
    let original = lease.owner.clone().unwrap();
    // This current-thread executor cannot drive the original Channel worker.
    // A cloned Channel fails here; an independently connected transport survives.
    std::thread::sleep(Duration::from_millis(1000));
    lease.ensure_owned().unwrap();
    let claim = meta
        .read_standing_runtime_owner(&identity.tenant_id, &identity.program_id, view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(standing_runtime_owner_token_from_claim(&claim), original);
    drop(lease);
    drop(state);
    drop(meta);
    let _ = stop.send(());
    tokio::task::spawn_blocking(move || server.join().unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn backfill_owner_renewal_survives_blocked_executor_and_fences_takeover() {
    let meta = Arc::new(meta::InMemoryMetaStore::default());
    let state = test_api_state_with_store(Arc::new(InMemory::new()), "lease-owner-a", false)
        .await
        .with_meta_store(meta.clone())
        .with_standing_runtime_owner_ttl_ms(120);
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let identity = &checkpoint.identity;
    let view = &identity.view_ids[0];
    let lease = owner_lease::BackfillOwnerLease::start(&state, identity, view)
        .await
        .unwrap();
    let original = lease.owner.clone().unwrap();
    // Deliberately starve the request executor for several TTLs. Renewal must
    // run independently, retaining the same fencing epoch and resident claim.
    std::thread::sleep(Duration::from_millis(400));
    lease.ensure_owned().unwrap();
    let claim = meta
        .read_standing_runtime_owner(&identity.tenant_id, &identity.program_id, view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(standing_runtime_owner_token_from_claim(&claim), original);
    let resident = state
        .standing_runtimes
        .local_state
        .lock()
        .unwrap()
        .get(&standing_runtime_key(identity, view))
        .unwrap()
        .owner
        .clone()
        .unwrap();
    assert_eq!(standing_runtime_owner_token_from_claim(&resident), original);
    assert!(resident.expires_at_unix_ms >= claim.expires_at_unix_ms);
    let foreign = meta
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: identity.tenant_id.clone(),
            program_id: identity.program_id.clone(),
            view_id: view.clone(),
            owner_id: "lease-owner-b".into(),
            ttl_ms: 120,
        })
        .await
        .unwrap();
    assert!(matches!(
        foreign,
        AcquireStandingRuntimeOwnerOutcome::Conflict(_)
    ));
    let checkpoint_key = test_checkpoint_key(&checkpoint);
    let candidate = test_checkpoint_pointer(&checkpoint_key, &checkpoint);
    let publish = meta::PublishStandingRuntimeCheckpointRequest {
        expected_previous: None,
        candidate: candidate.clone(),
        owner: original.clone(),
        expected_relation_source_cuts: None,
    };
    assert_eq!(
        meta.publish_standing_runtime_checkpoint(publish.clone())
            .await
            .unwrap(),
        meta::PublishStandingRuntimeCheckpointOutcome::Published
    );
    assert_eq!(
        meta.publish_standing_runtime_checkpoint(publish.clone())
            .await
            .unwrap(),
        meta::PublishStandingRuntimeCheckpointOutcome::Duplicate
    );
    drop(lease);
    std::thread::sleep(Duration::from_millis(150));
    assert!(meta
        .renew_standing_runtime_owner(original.clone(), 120)
        .await
        .is_err());
    let replacement = meta
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: identity.tenant_id.clone(),
            program_id: identity.program_id.clone(),
            view_id: view.clone(),
            owner_id: "lease-owner-b".into(),
            ttl_ms: 120,
        })
        .await
        .unwrap();
    let AcquireStandingRuntimeOwnerOutcome::Acquired(replacement) = replacement else {
        panic!("expected takeover");
    };
    assert!(replacement.owner_epoch > original.owner_epoch);
    state
        .set_standing_runtime_owner(identity, view, replacement.clone())
        .unwrap();
    assert!(owner_lease::refresh_resident_owner(
        &state,
        identity,
        view,
        &standing_runtime_owner_token_from_claim(&claim),
        claim,
    )
    .is_err());
    let token = standing_runtime_owner_token_from_claim(&replacement);
    let mut newer = replacement.clone();
    newer.expires_at_unix_ms += 1;
    owner_lease::refresh_resident_owner(&state, identity, view, &token, newer.clone()).unwrap();
    owner_lease::refresh_resident_owner(&state, identity, view, &token, replacement).unwrap();
    assert_eq!(
        state
            .standing_runtimes
            .local_state
            .lock()
            .unwrap()
            .get(&standing_runtime_key(identity, view))
            .unwrap()
            .owner,
        Some(newer)
    );
    assert!(meta
        .renew_standing_runtime_owner(original, 120)
        .await
        .is_err());
    assert!(meta
        .publish_standing_runtime_checkpoint(publish)
        .await
        .is_err());
    assert_eq!(
        meta.read_standing_runtime_checkpoint(&identity.tenant_id, &identity.program_id, view)
            .await
            .unwrap(),
        Some(candidate)
    );
}

async fn exercise_owner_renewal_timeout(before_commit: bool) {
    let hook = Arc::new(RenewalDelayHook {
        enabled: AtomicBool::new(false),
        reached: Notify::new(),
        release: Notify::new(),
        before_commit,
    });
    let mut decorated = DurableCapabilityMetaStore::new(InMemoryMetaStore::default());
    decorated.renewal_delay = Some(Arc::clone(&hook));
    let meta = Arc::clone(&decorated.inner);
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "renewal-timeout-owner", false)
            .await
            .with_meta_store(Arc::new(decorated))
            .with_standing_runtime_owner_ttl_ms(1500);
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let identity = &checkpoint.identity;
    let view = &identity.view_ids[0];
    let lease = owner_lease::BackfillOwnerLease::start(&state, identity, view)
        .await
        .unwrap();
    let original = lease.owner.clone().unwrap();
    let initial = meta
        .read_standing_runtime_owner(&identity.tenant_id, &identity.program_id, view)
        .await
        .unwrap()
        .unwrap();
    hook.enabled.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), hook.reached.notified())
        .await
        .unwrap();
    let failure = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(error) = lease.failure.lock().unwrap().clone() {
                break error;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(failure.contains("owner renewal timed out"), "{failure}");
    assert!(failure.contains("deadline_ms=500"), "{failure}");
    assert!(
        failure.contains("last_confirmed_expiry_unix_ms=Some("),
        "{failure}"
    );
    assert!(failure.len() < 1024);
    if !before_commit {
        // The response timed out although the exact epoch was durably renewed.
        // This is an ambiguous RPC outcome, not proof that ownership expired.
        let renewed = meta
            .read_standing_runtime_owner(&identity.tenant_id, &identity.program_id, view)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(standing_runtime_owner_token_from_claim(&renewed), original);
        assert!(renewed.expires_at_unix_ms > initial.expires_at_unix_ms);
        let error = persist_standing_runtime_checkpoint(
            &state,
            view,
            &checkpoint,
            &[],
            StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(original.clone()))
                .with_owner_renewal(Arc::clone(&lease.failure)),
            None,
        )
        .await
        .unwrap_err();
        assert!(format!("{error:?}").contains("owner renewal failed before publication"));
    } else {
        // Let the last confirmed lease expire, then install a real successor.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let replacement = meta
            .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
                tenant_id: identity.tenant_id.clone(),
                program_id: identity.program_id.clone(),
                view_id: view.clone(),
                owner_id: "renewal-timeout-successor".into(),
                ttl_ms: 1500,
            })
            .await
            .unwrap();
        let AcquireStandingRuntimeOwnerOutcome::Acquired(replacement) = replacement else {
            panic!("expected expired owner takeover");
        };
        assert!(replacement.owner_epoch > original.owner_epoch);
        assert!(meta
            .renew_standing_runtime_owner(original.clone(), 1500)
            .await
            .is_err());
        assert!(meta
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: None,
                candidate: test_checkpoint_pointer(&test_checkpoint_key(&checkpoint), &checkpoint),
                owner: original,
                expected_relation_source_cuts: None,
            })
            .await
            .is_err());
    }
    hook.release.notify_one();
    assert!(lease.ensure_owned().is_err());
    assert_eq!(lease.failure.lock().unwrap().as_ref(), Some(&failure));
    assert!(meta
        .read_standing_runtime_checkpoint(&identity.tenant_id, &identity.program_id, view)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn backfill_owner_renewal_committed_but_response_timed_out_blocks_publication() {
    exercise_owner_renewal_timeout(false).await;
}

#[tokio::test]
async fn backfill_owner_renewal_timeout_then_takeover_fences_old_epoch() {
    exercise_owner_renewal_timeout(true).await;
}

#[tokio::test]
async fn backfill_progress_counts_weighted_batches_across_packed_epochs_without_payload_reads() {
    let counts = ObjectStoreAccessCounts::default();
    let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        Arc::new(InMemory::new()),
        counts.clone(),
    ));
    let state = with_initialized_active_view_meta_store(
        test_api_state_with_store(store, "weighted-progress-owner", false).await,
        Arc::new(meta::InMemoryMetaStore::default()),
    )
    .await
    .with_experimental_advanced_view_features(true);
    let router = app(state.clone());
    let registered = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog()
        }),
    )
    .await;
    assert_eq!(registered.0, StatusCode::CREATED, "{registered:?}");
    // Three submitted epochs contain twelve physical batches and 24 BAG copies.
    for epoch in 0..3_u64 {
        let batches = (0..4_u64)
            .map(|index| {
                json!({
                    "relation_id": "scores", "relation_version": "2026-05-24.v1",
                    "stream_id": "weighted-progress", "partition_id": 0,
                    "start_offset_inclusive": epoch * 4 + index,
                    "rows": [{"user_id": "alice", "score": 10, "delta": 2}]
                })
            })
            .collect::<Vec<_>>();
        let ingested = call_json(
            &router,
            Method::POST,
            "/v1/relations/ingest",
            json!({"batches": batches}),
        )
        .await;
        assert_eq!(ingested.0, StatusCode::CREATED, "{ingested:?}");
    }
    let request = CreateViewRequest {
        view_id: "weighted_progress".into(), url_path: None, output_relation_id: None,
        input_relation_id: "scores".into(), input_relation_version: "2026-05-24.v1".into(),
        input_relation_refs: Vec::new(), input_relations: Vec::new(),
        sql: "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id".into(),
        source_kind: SqlSourceKind::StandingView, output_relation_ids: Vec::new(),
        sql_template: None, description: None, request: Vec::new(), response_schema: None,
        response_formats: vec!["json".into()], query_policy_id: None, correction_horizon_ns: None,
    };
    let created = call_json(&router, Method::POST, "/v1/views", json!(request)).await;
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    for (limit, processed, remaining) in [(Some(5), 5, 7), (None, 12, 0)] {
        let step = call_json(
            &router,
            Method::POST,
            "/v1/views/weighted_progress/backfill",
            limit.map_or(json!({}), |limit| json!({"batch_limit": limit})),
        )
        .await;
        assert_eq!(step.0, StatusCode::OK, "{step:?}");
        counts.clear();
        let status = call_json(
            &router,
            Method::GET,
            "/v1/views/weighted_progress/backfill",
            json!({}),
        )
        .await;
        assert_eq!(status.0, StatusCode::OK, "{status:?}");
        assert_eq!(status.1["progress"]["total_batches"], 12);
        assert_eq!(status.1["progress"]["processed_batches"], processed);
        assert_eq!(status.1["progress"]["remaining_batches"], remaining);
        assert!(
            counts
                .get_paths()
                .iter()
                .all(|path| !path.starts_with("v1/ingest/")),
            "progress must not read committed Arrow payloads: {:?}",
            counts.get_paths()
        );
    }
    let output = call_json(
        &router,
        Method::POST,
        "/v1/views/weighted_progress/query",
        json!({}),
    )
    .await;
    assert_eq!(output.0, StatusCode::OK, "{output:?}");
    assert_eq!(
        output.1["rows"],
        json!([{"user_id": "alice", "sum": 240, "count": 24}])
    );
    let retry = call_json(
        &router,
        Method::POST,
        "/v1/views/weighted_progress/backfill",
        json!({}),
    )
    .await;
    assert_eq!(retry.0, StatusCode::OK, "{retry:?}");
    assert_eq!(retry.1["applied_batches"], 0);
    let after_retry = call_json(
        &router,
        Method::POST,
        "/v1/views/weighted_progress/query",
        json!({}),
    )
    .await;
    assert_eq!(after_retry.0, StatusCode::OK, "{after_retry:?}");
    assert_eq!(after_retry.1["logical_epoch"], output.1["logical_epoch"]);
    assert_eq!(after_retry.1["rows"], output.1["rows"]);
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
async fn rest_select_star_backfill_activates_authoritative_bootstrap_when_ingest_already_caught_up()
{
    let state = test_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-select-star-backfill-activation-owner",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog()
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let view_request = CreateViewRequest {
        view_id: "select_star_scores_backfill".to_string(),
        url_path: None,
        output_relation_id: None,
        input_relation_id: "scores".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: "select * from scores where score >= 10".to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("select-star backfill activation regression".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
        correction_horizon_ns: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(view_request)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_response = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "select-star-backfill-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "bob", "score": 9, "delta": 1},
                {"user_id": "carol", "score": 42, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(
        ingest_response.0,
        StatusCode::CREATED,
        "{ingest_response:?}"
    );

    for _ in 0..2 {
        let backfill_response = call_json(
            &router,
            Method::POST,
            "/v1/views/select_star_scores_backfill/backfill",
            json!({}),
        )
        .await;
        assert_eq!(backfill_response.0, StatusCode::OK, "{backfill_response:?}");
        assert_eq!(backfill_response.1["applied_batches"], 0);
        assert_eq!(backfill_response.1["remaining_batches"], 0);

        let query_response = call_json(
            &router,
            Method::POST,
            "/v1/views/select_star_scores_backfill/query",
            json!({}),
        )
        .await;
        assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
        assert_eq!(
            query_response.1["rows"],
            json!([
                {"user_id": "alice", "score": 10},
                {"user_id": "carol", "score": 42}
            ])
        );
    }
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
                "max_object_requests": 64,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
            "catalog": test_scores_catalog()
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
            correction_horizon_ns: None,
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
async fn raw_sql_count_and_sum_use_complete_materialized_input_despite_page_caps() {
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
            "catalog": test_scores_catalog()
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
                "max_object_requests": 64,
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
                    correction_horizon_ns: None,
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
        assert_eq!(response.0, StatusCode::OK, "{response:?}");
        assert_eq!(
            response.1["rows"],
            json!([{"total_count":4,"total_sum":100}])
        );
        assert!(response.1["next_page_token"].is_null(), "{response:?}");
    }
}

#[tokio::test]
async fn rest_exists_and_not_exists_views_survive_restart_and_match_transitions() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "api-test-semi-anti-owner-a", false).await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({"catalog": catalog}),
        )
        .await;
        assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
    }

    for (view_id, predicate) in [
        (
            "scores_with_accounts",
            "exists (select 1 from accounts a where a.account_id = s.user_id)",
        ),
        (
            "scores_without_accounts",
            "not exists (select 1 from accounts a where a.account_id = s.user_id)",
        ),
    ] {
        let request = CreateViewRequest {
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
            sql: format!("select s.user_id, s.score from scores s where {predicate}"),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("bounded correlated existence view".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
            correction_horizon_ns: None,
        };
        let response = call_json(&router, Method::POST, "/v1/views", json!(request)).await;
        assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
        assert_eq!(response.1["query_enabled"], true);
    }

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "semi-anti-scores",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "delta": 1},
                        {"user_id": "bob", "score": 5, "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "semi-anti-accounts",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");

    for (view_id, expected) in [
        (
            "scores_with_accounts",
            json!([{"user_id": "alice", "score": 10}]),
        ),
        (
            "scores_without_accounts",
            json!([{"user_id": "bob", "score": 5}]),
        ),
    ] {
        let response = call_json(
            &router,
            Method::POST,
            &format!("/v1/views/{view_id}/query"),
            json!({}),
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{response:?}");
        assert_eq!(response.1["rows"], expected);
    }

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-semi-anti-owner-b",
        true,
    )
    .await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        2
    );
    let restarted_router = app(restarted_state);
    let transition = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/accounts/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "semi-anti-accounts",
            "partition_id": 0,
            "start_offset_inclusive": 1,
            "rows": [
                {"account_id": "alice", "limit": 100, "tier": "gold", "delta": -1},
                {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(transition.0, StatusCode::CREATED, "{transition:?}");

    for (view_id, expected) in [
        (
            "scores_with_accounts",
            json!([{"user_id": "bob", "score": 5}]),
        ),
        (
            "scores_without_accounts",
            json!([{"user_id": "alice", "score": 10}]),
        ),
    ] {
        let response = call_json(
            &restarted_router,
            Method::POST,
            &format!("/v1/views/{view_id}/query"),
            json!({}),
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{response:?}");
        assert_eq!(response.1["rows"], expected);
    }
}

#[tokio::test]
async fn rest_two_relation_join_view_materialized_output_survives_api_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "api-test-join-owner-a", false).await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog
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
            correction_horizon_ns: None,
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

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store.clone(),
        "api-test-join-owner-b",
        true,
    )
    .await;
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
async fn rest_three_input_composite_pk_join_uses_binary_dag_and_survives_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state =
        test_public_api_state_with_store(store.clone(), "api-test-three-input-join-owner-a", false)
            .await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state.clone());
    let catalogs = test_three_input_composite_catalogs();
    for catalog in &catalogs {
        let response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({"catalog": catalog}),
        )
        .await;
        assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
    }

    let sql = "select s.tenant_id, s.user_id, count(*) as count from scores s join accounts a on s.tenant_id = a.account_tenant_id and s.user_id = a.account_id join profiles p on s.tenant_id = p.account_tenant_id and s.user_id = p.account_id group by s.tenant_id, s.user_id";
    let view = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "three_input_join_count",
            "input_relation_refs": [
                {"relation_id": "scores", "relation_version": "2026-05-24.v1"},
                {"relation_id": "accounts", "relation_version": "2026-05-24.v1"},
                {"relation_id": "profiles", "relation_version": "2026-05-24.v1"}
            ],
            "sql": sql,
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "{view:?}");
    assert_eq!(view.1["query_enabled"], true);

    let active = read_test_active_view(&state, "three_input_join_count")
        .await
        .unwrap();
    let published = active
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.published_relations.first())
        .expect("REST admission must persist the published relation contract");
    assert_eq!(published.producer_view_id, "three_input_join_count");
    assert_eq!(published.producer_view_generation, 1);
    assert_eq!(published.relation, active.spec.output_relations[0]);
    assert_eq!(published.frontier_kind, "producer_commit_epoch");
    assert_eq!(
        published.delta_codec_identity,
        "velorix-published-relation-delta-v1"
    );
    assert!(published
        .relation
        .columns
        .iter()
        .all(|column| column.name != "delta" && column.name != "weight"));
    let logical_plan = active
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.logical_plan.as_ref())
        .expect("REST admission must persist the three-input logical plan");
    assert_eq!(logical_plan.input_relations.len(), 3);
    assert_eq!(
        logical_plan
            .nodes
            .iter()
            .filter(|node| matches!(
                node,
                velorix_core::view_plan::VelorixLogicalViewPlanNodeV1::InnerEquiJoin { .. }
            ))
            .count(),
        2
    );
    let velorix_core::view_plan::VelorixLogicalViewExecutionV1::ThreeInputInnerJoinCount { plan } =
        &logical_plan.execution
    else {
        panic!("expected the bounded three-input execution plan");
    };
    assert_eq!(
        plan.join_key_codec_id,
        "velorix-composite-pk-positional-json-array-join-key-v1"
    );
    assert_eq!(
        logical_plan
            .execution_implementation
            .as_ref()
            .unwrap()
            .implementation_id,
        "velorix-native-three-input-inner-join-dag-v1"
    );

    let reordered_sql = "select s.tenant_id, s.user_id, count(*) as count from scores s join profiles p on s.tenant_id = p.account_tenant_id and s.user_id = p.account_id join accounts a on s.tenant_id = a.account_tenant_id and s.user_id = a.account_id group by s.tenant_id, s.user_id";
    let reordered_view = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "three_input_join_count_reordered",
            "input_relation_refs": [
                {"relation_id": "scores", "relation_version": "2026-05-24.v1"},
                {"relation_id": "accounts", "relation_version": "2026-05-24.v1"},
                {"relation_id": "profiles", "relation_version": "2026-05-24.v1"}
            ],
            "sql": reordered_sql,
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(reordered_view.0, StatusCode::CREATED, "{reordered_view:?}");
    let reordered_active = read_test_active_view(&state, "three_input_join_count_reordered")
        .await
        .unwrap();
    let reordered_plan = reordered_active
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.logical_plan.as_ref())
        .unwrap();
    assert_eq!(reordered_plan.execution, logical_plan.execution);
    let join_nodes = |plan: &VelorixLogicalViewPlanV1| {
        plan.nodes
            .iter()
            .filter(|node| {
                matches!(
                    node,
                    velorix_core::view_plan::VelorixLogicalViewPlanNodeV1::InnerEquiJoin { .. }
                )
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(join_nodes(reordered_plan), join_nodes(logical_plan));
    let mut reordered_implementation = reordered_plan.execution_implementation.clone().unwrap();
    let mut implementation = logical_plan.execution_implementation.clone().unwrap();
    reordered_implementation.physical_operator_dag_hash.clear();
    implementation.physical_operator_dag_hash.clear();
    assert_eq!(reordered_implementation, implementation);
    assert_ne!(
        reordered_plan
            .execution_implementation
            .as_ref()
            .unwrap()
            .physical_operator_dag_hash,
        logical_plan
            .execution_implementation
            .as_ref()
            .unwrap()
            .physical_operator_dag_hash,
        "different output relation identities must remain physically distinct"
    );
    assert_ne!(reordered_plan.plan_hash, logical_plan.plan_hash);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "three-input-scores",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"user_id": "alice", "score": 10, "tenant_id": "t1", "delta": 2},
                        {"user_id": "bob", "score": 5, "tenant_id": "t1", "delta": 1}
                    ]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "three-input-accounts",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 100, "tier": "gold", "account_tenant_id": "t1", "delta": 3},
                        {"account_id": "bob", "limit": 50, "tier": "silver", "account_tenant_id": "t1", "delta": 1}
                    ]
                },
                {
                    "relation_id": "profiles",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "three-input-profiles",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        {"account_id": "alice", "limit": 0, "tier": "profile", "account_tenant_id": "t1", "delta": 4}
                    ]
                }
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");
    assert_eq!(ingest.1["materialization"]["status"], "completed");
    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/three_input_join_count/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([{"tenant_id": "t1", "user_id": "alice", "count": 24}])
    );
    let reordered_query = call_json(
        &router,
        Method::POST,
        "/v1/views/three_input_join_count_reordered/query",
        json!({}),
    )
    .await;
    assert_eq!(reordered_query.0, StatusCode::OK, "{reordered_query:?}");
    assert_eq!(reordered_query.1["rows"], query.1["rows"]);

    let identity = active_standing_runtime_identity(&active).unwrap().clone();
    let checkpoint =
        read_latest_standing_runtime_checkpoint(&state, &identity, "three_input_join_count")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(checkpoint.checkpoint.input_frontiers.len(), 3);
    assert_snapshot_and_commit_refs(&checkpoint.checkpoint.output_manifest_refs);
    let output_commit_ref = checkpoint
        .checkpoint
        .output_manifest_refs
        .iter()
        .find(|reference| reference.starts_with(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX))
        .unwrap();
    assert!(output_commit_ref.starts_with(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX));
    let (_, output_commit_record) = read_standing_runtime_output_delta_record(
        &state,
        output_commit_ref,
        "three_input_join_count",
    )
    .await
    .unwrap();
    let output_commit = output_commit_record.producer_commit.as_ref().unwrap();
    assert_eq!(
        output_commit.producer_view_generation,
        published.producer_view_generation
    );
    assert_eq!(
        output_commit.producer_plan_hash,
        published.producer_plan_hash
    );
    assert_eq!(output_commit.output_stream_id, published.output_stream_id);
    assert_eq!(
        output_commit.output_schema_hash,
        published.output_schema_hash
    );
    assert_eq!(
        output_commit.key_descriptor_hash,
        published.key_descriptor_hash
    );
    assert_eq!(
        output_commit.causal_cut_digest,
        checkpoint
            .checkpoint
            .causal_cut
            .as_ref()
            .unwrap()
            .stable_digest()
            .unwrap()
    );
    let payload: Value = velorix_core::arrow_state::from_state(
        &checkpoint
            .checkpoint
            .state_payload
            .as_ref()
            .unwrap()
            .payload,
    )
    .unwrap();
    assert_eq!(payload["graph"]["operators"].as_array().unwrap().len(), 4);

    drop(router);
    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-three-input-join-owner-b",
        true,
    )
    .await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        2
    );
    let restarted_router = app(restarted_state);
    let restarted = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/three_input_join_count/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted.1["rows"], query.1["rows"]);

    let retract = json!({
        "relation_version": "2026-05-24.v1",
        "stream_id": "three-input-profiles",
        "partition_id": 0,
        "start_offset_inclusive": 1,
        "rows": [
            {"account_id": "alice", "limit": 0, "tier": "profile", "account_tenant_id": "t1", "delta": -1}
        ]
    });
    let retracted = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/profiles/ingest",
        retract.clone(),
    )
    .await;
    assert_eq!(retracted.0, StatusCode::CREATED, "{retracted:?}");
    let after_retract = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/three_input_join_count/query",
        json!({}),
    )
    .await;
    assert_eq!(
        after_retract.1["rows"],
        json!([{"tenant_id": "t1", "user_id": "alice", "count": 18}])
    );
    let replay = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/profiles/ingest",
        retract,
    )
    .await;
    assert!(replay.0.is_success(), "{replay:?}");
    let after_replay = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/three_input_join_count/query",
        json!({}),
    )
    .await;
    assert_eq!(after_replay.1["rows"], after_retract.1["rows"]);
}

#[tokio::test]
async fn rest_self_join_atomic_fanout_survives_restart_replay_and_final_retract() {
    let store = Arc::new(ArmedPrefixFailingObjectStore::new(
        Arc::new(InMemory::new()),
    ));
    let state = test_public_api_state_with_store(
        store.clone() as Arc<dyn ObjectStore>,
        "api-test-self-join-owner-a",
        false,
    )
    .await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state.clone());
    let mut scores = test_scores_catalog();
    scores.incremental_adapter.adapter_id = CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string();
    let relation = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": scores}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "{relation:?}");

    let sql = "select count(*) as count from scores l join scores r on l.score = r.score";
    let view = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "score_self_join_count",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": sql,
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "{view:?}");
    assert_eq!(view.1["query_enabled"], true);
    assert_eq!(view.1["output_relations"][0]["primary_key"], json!([]));
    assert_eq!(view.1["output_relations"][0]["columns"][0]["name"], "count");

    let active = read_test_active_view(&state, "score_self_join_count")
        .await
        .unwrap();
    let logical_plan = active
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.logical_plan.as_ref())
        .expect("REST admission must persist the self-join logical plan");
    assert_eq!(logical_plan.input_relations.len(), 1);
    assert_eq!(
        logical_plan
            .execution_implementation
            .as_ref()
            .unwrap()
            .input_fanout_protocol_id
            .as_deref(),
        Some("velorix-self-join-left-then-right-atomic-fanout-v1")
    );
    let (left_key, right_key) = logical_plan
        .nodes
        .iter()
        .find_map(|node| match node {
            velorix_core::view_plan::VelorixLogicalViewPlanNodeV1::InnerEquiJoin {
                left_key,
                right_key,
                ..
            } => Some((left_key, right_key)),
            _ => None,
        })
        .unwrap();
    assert_eq!(left_key.relation_id, "scores");
    assert_eq!(right_key.relation_id, "scores");
    assert_eq!(left_key.input_instance_id.as_deref(), Some("scan_left"));
    assert_eq!(right_key.input_instance_id.as_deref(), Some("scan_right"));

    let first_ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "self-join-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "left-a", "score": 10, "delta": 2},
                {"user_id": "left-b", "score": 10, "delta": 1},
                {"user_id": "left-c", "score": 5, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(first_ingest.0, StatusCode::CREATED, "{first_ingest:?}");
    assert_eq!(first_ingest.1["materialization"]["status"], "completed");
    let first_query = call_json(
        &router,
        Method::POST,
        "/v1/views/score_self_join_count/query",
        json!({}),
    )
    .await;
    assert_eq!(first_query.0, StatusCode::OK, "{first_query:?}");
    assert_eq!(first_query.1["rows"], json!([{"count": 10}]));

    let identity = active_standing_runtime_identity(&active).unwrap().clone();
    let first_checkpoint =
        read_latest_standing_runtime_checkpoint(&state, &identity, "score_self_join_count")
            .await
            .unwrap()
            .unwrap();
    let payload: Value = velorix_core::arrow_state::from_state(
        &first_checkpoint
            .checkpoint
            .state_payload
            .as_ref()
            .unwrap()
            .payload,
    )
    .unwrap();
    assert_eq!(payload["input_schemas"].as_array().unwrap().len(), 1);
    assert!(!payload["left_state"]["records"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(!payload["right_state"]["records"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(!first_checkpoint.checkpoint.output_manifest_refs.is_empty());

    drop(router);
    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store.clone() as Arc<dyn ObjectStore>,
        "api-test-self-join-owner-b",
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
    let restarted_router = app(restarted_state.clone());
    let restored_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/score_self_join_count/query",
        json!({}),
    )
    .await;
    assert_eq!(restored_query.0, StatusCode::OK, "{restored_query:?}");
    assert_eq!(restored_query.1["rows"], json!([{"count": 10}]));

    let partial_retract = json!({
        "relation_version": "2026-05-24.v1",
        "stream_id": "self-join-scores-stream",
        "partition_id": 0,
        "start_offset_inclusive": 3,
        "rows": [{"user_id": "left-a", "score": 10, "delta": -1}]
    });
    store.arm("v1/standing-runtime-output-deltas/");
    let failed_partial = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/scores/ingest",
        partial_retract.clone(),
    )
    .await;
    assert_eq!(
        failed_partial.0,
        StatusCode::INTERNAL_SERVER_ERROR,
        "{failed_partial:?}"
    );
    let still_authoritative = read_latest_standing_runtime_checkpoint(
        &restarted_state,
        &identity,
        "score_self_join_count",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        still_authoritative.checkpoint.logical_epoch,
        first_checkpoint.checkpoint.logical_epoch
    );
    assert_eq!(
        still_authoritative.checkpoint.state_root.content_hash,
        first_checkpoint.checkpoint.state_root.content_hash
    );

    drop(restarted_router);
    let recovered_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store as Arc<dyn ObjectStore>,
        "api-test-self-join-owner-c",
        true,
    )
    .await;
    assert_eq!(
        recovered_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let recovered_router = app(recovered_state);
    let count_five = call_json(
        &recovered_router,
        Method::POST,
        "/v1/views/score_self_join_count/query",
        json!({}),
    )
    .await;
    assert_eq!(count_five.1["rows"], json!([{"count": 5}]));

    let replay = call_json(
        &recovered_router,
        Method::POST,
        "/v1/relations/scores/ingest",
        partial_retract,
    )
    .await;
    assert!(replay.0.is_success(), "{replay:?}");
    let after_replay = call_json(
        &recovered_router,
        Method::POST,
        "/v1/views/score_self_join_count/query",
        json!({}),
    )
    .await;
    assert_eq!(after_replay.1["rows"], json!([{"count": 5}]));

    let final_retract = call_json(
        &recovered_router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version": "2026-05-24.v1",
            "stream_id": "self-join-scores-stream",
            "partition_id": 0,
            "start_offset_inclusive": 4,
            "rows": [
                {"user_id": "left-a", "score": 10, "delta": -1},
                {"user_id": "left-b", "score": 10, "delta": -1},
                {"user_id": "left-c", "score": 5, "delta": -1}
            ]
        }),
    )
    .await;
    assert_eq!(final_retract.0, StatusCode::CREATED, "{final_retract:?}");
    let zero = call_json(
        &recovered_router,
        Method::POST,
        "/v1/views/score_self_join_count/query",
        json!({}),
    )
    .await;
    assert_eq!(zero.0, StatusCode::OK, "{zero:?}");
    assert_eq!(zero.1["rows"], json!([{"count": 0}]));
}

#[derive(Debug, Eq, PartialEq)]
struct JoinDifferentialApiEvidence {
    initial_query: Value,
    restored_query: Value,
    tail_query: Value,
    initial_output_refs: Vec<String>,
    tail_output_refs: Vec<String>,
    initial_canonical_checkpoint: Value,
    tail_canonical_checkpoint: Value,
}

#[tokio::test]
async fn retained_join_specializations_match_common_dag_through_durable_api_restart() {
    for (view_id, sql) in [
        (
            "scores_by_account_diff_inner",
            "select a.account_id, sum(s.score) as sum, count(*) as count from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        ),
        (
            "scores_by_account_diff_left",
            "select s.user_id as account_id, sum(s.score) as sum, count(*) as count from scores s left join accounts a on s.user_id = a.account_id group by s.user_id",
        ),
        (
            "scores_by_account_diff_general",
            "select a.account_id, sum(s.score) as sum, count(*) as count, count(a.limit) as count_limit, count(distinct a.limit) as distinct_limits, sum(a.limit) as limit_sum, min(a.limit) as min_limit, max(a.limit) as max_limit, avg(a.limit) as avg_limit from scores s join accounts a on s.user_id = a.account_id group by a.account_id",
        ),
    ] {
        let selected = run_join_differential_api_trace(view_id, sql, false).await;
        let reference = run_join_differential_api_trace(view_id, sql, true).await;
        assert_eq!(
            selected, reference,
            "differential API evidence for {view_id}"
        );
    }
}

async fn run_join_differential_api_trace(
    view_id: &str,
    sql: &str,
    common_dag_reference: bool,
) -> JoinDifferentialApiEvidence {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mode = if common_dag_reference {
        "reference"
    } else {
        "selected"
    };
    let state = test_api_state_with_store(
        store.clone(),
        &format!("api-test-differential-{view_id}-{mode}-a"),
        false,
    )
    .await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    if common_dag_reference {
        state.register_standing_program_runtime_factory(
            MATERIALIZED_VIEW_RUNTIME_NAME,
            CommonDagReferenceRuntimeFactory::new(),
        );
    }
    let router = app(state.clone());

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({"catalog": catalog}),
        )
        .await;
        assert_eq!(relation.0, StatusCode::CREATED, "{relation:?}");
    }
    let view = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": view_id,
            "input_relation_refs": [
                {"relation_id": "scores", "relation_version": "2026-05-24.v1"},
                {"relation_id": "accounts", "relation_version": "2026-05-24.v1"}
            ],
            "sql": sql,
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "{view:?}");

    let initial_ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({"batches": [
            {
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-differential-stream",
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
                "stream_id": "accounts-differential-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"account_id": "alice", "limit": 100, "tier": "gold", "delta": 1},
                    {"account_id": "bob", "limit": 50, "tier": "gold", "delta": 1}
                ]
            }
        ]}),
    )
    .await;
    assert_eq!(initial_ingest.0, StatusCode::CREATED, "{initial_ingest:?}");
    let initial_query = call_json(
        &router,
        Method::POST,
        &format!("/v1/views/{view_id}/query"),
        json!({}),
    )
    .await;
    assert_eq!(initial_query.0, StatusCode::OK, "{initial_query:?}");
    let active = read_test_active_view(&state, view_id).await.unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap().clone();
    let initial_record = read_latest_standing_runtime_checkpoint(&state, &identity, view_id)
        .await
        .unwrap()
        .unwrap();
    validate_standing_runtime_checkpoint_output_manifest_records(&state, &initial_record)
        .await
        .unwrap();
    let initial_output_refs =
        assert_snapshot_and_commit_refs(&initial_record.checkpoint.output_manifest_refs);
    let initial_canonical_checkpoint = canonical_join_api_checkpoint(&initial_record.checkpoint);

    drop(router);
    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        &format!("api-test-differential-{view_id}-{mode}-b"),
        true,
    )
    .await;
    if common_dag_reference {
        restarted_state.register_standing_program_runtime_factory(
            MATERIALIZED_VIEW_RUNTIME_NAME,
            CommonDagReferenceRuntimeFactory::new(),
        );
    }
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let restarted_router = app(restarted_state.clone());
    let restored_query = call_json(
        &restarted_router,
        Method::POST,
        &format!("/v1/views/{view_id}/query"),
        json!({}),
    )
    .await;
    assert_eq!(restored_query.0, StatusCode::OK, "{restored_query:?}");

    let tail_body = json!({"batches": [
        {
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-differential-stream",
            "partition_id": 0,
            "start_offset_inclusive": 4,
            "rows": [
                {"user_id": "alice", "score": 7, "delta": -1},
                {"user_id": "bob", "score": 5, "delta": -1},
                {"user_id": "bob", "score": 8, "delta": 1}
            ]
        },
        {
            "relation_id": "accounts",
            "relation_version": "2026-05-24.v1",
            "stream_id": "accounts-differential-stream",
            "partition_id": 0,
            "start_offset_inclusive": 2,
            "rows": [
                {"account_id": "alice", "limit": 100, "tier": "gold", "delta": -1},
                {"account_id": "alice", "limit": 80, "tier": "silver", "delta": 1},
                {"account_id": "charlie", "limit": 90, "tier": "silver", "delta": 1}
            ]
        }
    ]});
    let tail_ingest = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/ingest",
        tail_body.clone(),
    )
    .await;
    assert!(tail_ingest.0.is_success(), "{tail_ingest:?}");
    let tail_query = call_json(
        &restarted_router,
        Method::POST,
        &format!("/v1/views/{view_id}/query"),
        json!({}),
    )
    .await;
    assert_eq!(tail_query.0, StatusCode::OK, "{tail_query:?}");
    let tail_record = read_latest_standing_runtime_checkpoint(&restarted_state, &identity, view_id)
        .await
        .unwrap()
        .unwrap();
    let duplicate_tail = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/ingest",
        tail_body,
    )
    .await;
    assert_eq!(duplicate_tail.0, StatusCode::OK, "{duplicate_tail:?}");
    assert_eq!(duplicate_tail.1["materialization"]["checkpoint_writes"], 0);
    let after_duplicate =
        read_latest_standing_runtime_checkpoint(&restarted_state, &identity, view_id)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        after_duplicate.checkpoint.output_manifest_refs,
        tail_record.checkpoint.output_manifest_refs
    );

    JoinDifferentialApiEvidence {
        initial_query: initial_query.1,
        restored_query: restored_query.1,
        tail_query: tail_query.1,
        initial_output_refs,
        // Snapshot roots bind runtime-specific checkpoint bytes; compare logical
        // output commits after validating each snapshot against its own checkpoint.
        tail_output_refs: assert_snapshot_and_commit_refs(
            &tail_record.checkpoint.output_manifest_refs,
        ),
        initial_canonical_checkpoint,
        tail_canonical_checkpoint: canonical_join_api_checkpoint(&tail_record.checkpoint),
    }
}

fn assert_snapshot_and_commit_refs(refs: &[String]) -> Vec<String> {
    let commits = refs
        .iter()
        .filter(|reference| reference.starts_with(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX))
        .cloned()
        .collect::<Vec<_>>();
    let snapshots = refs
        .iter()
        .filter_map(|reference| reference.strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX))
        .collect::<Vec<_>>();
    assert_eq!(
        refs.len(),
        2,
        "one durable output commit and one immutable snapshot"
    );
    assert_eq!(commits.len(), 1);
    assert_eq!(snapshots.len(), 1);
    ObjectKey::parse_standing_runtime_output_manifest(snapshots[0]).unwrap();
    commits
}

fn canonical_join_api_checkpoint(checkpoint: &RuntimeCheckpoint) -> Value {
    let payload: Value =
        velorix_core::arrow_state::from_state(&checkpoint.state_payload.as_ref().unwrap().payload)
            .unwrap();
    json!({
        "logical_epoch": checkpoint.logical_epoch,
        "input_frontiers": checkpoint.input_frontiers,
        "input_event_time_frontiers": checkpoint.input_event_time_frontiers,
        "output_frontiers": checkpoint.output_frontiers,
        "published_output": payload["published_output"],
        "applied_epochs": payload["applied_epochs"]
    })
}

#[tokio::test]
async fn rest_grouped_median_and_percentiles_materialize_and_survive_api_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state =
        test_public_api_state_with_store(store.clone(), "api-test-percentiles-a", false).await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);
    let score_samples = test_relation_catalog_for_e2e(
        "score_samples",
        &[
            ("sample_id", VelorixLogicalTypeV1::Utf8, false),
            ("bucket", VelorixLogicalTypeV1::Utf8, false),
            ("value", VelorixLogicalTypeV1::Int64, false),
        ],
    );
    let latency_samples = test_relation_catalog_for_e2e(
        "latency_samples",
        &[
            ("observation_id", VelorixLogicalTypeV1::Utf8, false),
            ("segment", VelorixLogicalTypeV1::Utf8, false),
            ("latency", VelorixLogicalTypeV1::Int64, false),
        ],
    );
    for catalog in [score_samples, latency_samples] {
        let response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({"catalog": catalog}),
        )
        .await;
        assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
    }

    for (view_id, relation_id, sql) in [
        (
            "median_score_by_bucket",
            "score_samples",
            "select bucket, median(value) as median from score_samples group by bucket",
        ),
        (
            "latency_percentiles_by_segment",
            "latency_samples",
            "select segment, percentile_disc(latency, 0.5) as p50_disc, percentile_cont(latency, 0.5) as p50_cont from latency_samples group by segment",
        ),
    ] {
        let response = call_json(
            &router,
            Method::POST,
            "/v1/views",
            json!({
                "view_id": view_id,
                "input_relation_id": relation_id,
                "input_relation_version": "2026-08-14.v1",
                "sql": sql,
            }),
        )
        .await;
        assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
        assert_eq!(response.1["query_enabled"], true, "{response:?}");
    }

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({"batches": [
            {
                "relation_id": "score_samples",
                "relation_version": "2026-08-14.v1",
                "stream_id": "median-samples",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"sample_id": "a-1", "bucket": "alpha", "value": 2, "delta": 1},
                    {"sample_id": "a-2", "bucket": "alpha", "value": 10, "delta": 1},
                    {"sample_id": "b-1", "bucket": "beta", "value": 4, "delta": 1},
                    {"sample_id": "b-2", "bucket": "beta", "value": 8, "delta": 1},
                    {"sample_id": "b-3", "bucket": "beta", "value": 12, "delta": 1}
                ]
            },
            {
                "relation_id": "latency_samples",
                "relation_version": "2026-08-14.v1",
                "stream_id": "percentile-samples",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"observation_id": "c-1", "segment": "critical", "latency": 20, "delta": 1},
                    {"observation_id": "c-2", "segment": "critical", "latency": 50, "delta": 1},
                    {"observation_id": "c-3", "segment": "critical", "latency": 100, "delta": 1},
                    {"observation_id": "s-1", "segment": "standard", "latency": 10, "delta": 1},
                    {"observation_id": "s-2", "segment": "standard", "latency": 20, "delta": 1}
                ]
            }
        ]}),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let expected_rows = [
        (
            "median_score_by_bucket",
            json!([
                {"bucket": "alpha", "median": 6.0},
                {"bucket": "beta", "median": 8.0}
            ]),
        ),
        (
            "latency_percentiles_by_segment",
            json!([
                {"segment": "critical", "p50_disc": 50, "p50_cont": 50.0},
                {"segment": "standard", "p50_disc": 10, "p50_cont": 15.0}
            ]),
        ),
    ];
    for (view_id, expected) in &expected_rows {
        let response = call_json(
            &router,
            Method::POST,
            &format!("/v1/views/{view_id}/query"),
            json!({}),
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{response:?}");
        assert_eq!(response.1["rows"], *expected, "{response:?}");
    }

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-percentiles-b",
        true,
    )
    .await;
    assert_eq!(
        restarted_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        2
    );
    let restarted_router = app(restarted_state);
    for (view_id, expected) in &expected_rows {
        let response = call_json(
            &restarted_router,
            Method::POST,
            &format!("/v1/views/{view_id}/query"),
            json!({}),
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{response:?}");
        assert_eq!(response.1["rows"], *expected, "{response:?}");
    }
}

#[tokio::test]
async fn rest_percentile_admission_rejects_global_invalid_and_non_int64_inputs() {
    let state = test_public_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-percentile-admission-rejections",
        false,
    )
    .await;
    let router = app(state);
    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": test_scores_catalog()}),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let mut decimal_scores = test_scores_catalog();
    decimal_scores.relation_schema.relation_id = "decimal_scores".to_string();
    decimal_scores.relation_schema.relation_name = "decimal_scores".to_string();
    decimal_scores.datafusion_registration.name = "decimal_scores".to_string();
    decimal_scores.incremental_relation.relation_id = "decimal_scores".to_string();
    let score = decimal_scores
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "score")
        .unwrap();
    score.logical_type = VelorixLogicalTypeV1::Decimal {
        precision: 12,
        scale: 2,
    };
    score.physical_arrow_type = ArrowPhysicalTypeV1::Decimal128 {
        precision: 12,
        scale: 2,
    };
    let schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&decimal_scores.relation_schema).unwrap();
    decimal_scores.schema_fingerprint = schema_fingerprint.clone();
    decimal_scores.incremental_relation.schema_fingerprint = schema_fingerprint;
    let decimal_relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": decimal_scores}),
    )
    .await;
    assert_eq!(decimal_relation_response.0, StatusCode::CREATED);

    for (view_id, relation_id, sql) in [
        (
            "global_median",
            "scores",
            "select median(score) as median from scores",
        ),
        (
            "percentile_wrong_arity",
            "scores",
            "select user_id, percentile_disc(score) as p50 from scores group by user_id",
        ),
        (
            "percentile_non_literal",
            "scores",
            "select user_id, percentile_cont(score, user_id) as p50 from scores group by user_id",
        ),
        (
            "percentile_out_of_range",
            "scores",
            "select user_id, percentile_disc(score, 1.1) as p110 from scores group by user_id",
        ),
        (
            "median_string_input",
            "scores",
            "select score, median(user_id) as median from scores group by score",
        ),
        (
            "percentile_disc_string_input",
            "scores",
            "select score, percentile_disc(user_id, 0.5) as p50 from scores group by score",
        ),
        (
            "percentile_cont_string_input",
            "scores",
            "select score, percentile_cont(user_id, 0.5) as p50 from scores group by score",
        ),
        (
            "median_decimal_input",
            "decimal_scores",
            "select user_id, median(score) as median from decimal_scores group by user_id",
        ),
        (
            "percentile_disc_decimal_input",
            "decimal_scores",
            "select user_id, percentile_disc(score, 0.5) as p50 from decimal_scores group by user_id",
        ),
        (
            "percentile_cont_decimal_input",
            "decimal_scores",
            "select user_id, percentile_cont(score, 0.5) as p50 from decimal_scores group by user_id",
        ),
    ] {
        let response = call_json(
            &router,
            Method::POST,
            "/v1/views",
            json!({
                "view_id": view_id,
                "input_relation_id": relation_id,
                "input_relation_version": "2026-05-24.v1",
                "sql": sql,
            }),
        )
        .await;
        assert_eq!(response.0, StatusCode::BAD_REQUEST, "{response:?}");
        assert!(
            response.1["error"]
                .as_str()
                .unwrap_or_default()
                .contains("unsupported view SQL for materialized runtime"),
            "{response:?}"
        );
        let persisted = call_json(
            &router,
            Method::GET,
            &format!("/v1/views/{view_id}"),
            Value::Null,
        )
        .await;
        assert!(persisted.0.is_client_error(), "{persisted:?}");
    }
}

#[tokio::test]
async fn rest_two_relation_join_count_only_view_materialized_output_survives_api_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "api-test-join-count-only-a", false).await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog
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
            correction_horizon_ns: None,
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

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-join-count-only-b",
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
async fn rest_right_join_swaps_operands_and_materializes_unmatched_right_rows() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-test-join-right-swap", false)
            .await;
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({"catalog": catalog}),
        )
        .await;
        assert_eq!(response.0, StatusCode::CREATED);
    }

    let response = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!(CreateViewRequest {
            view_id: "account_limits_right_join".to_string(),
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
            sql: "select a.account_id, sum(a.limit) as sum, count(*) as count from scores s right join accounts a on s.user_id = a.account_id group by a.account_id".to_string(),
            source_kind: SqlSourceKind::StandingView,
            output_relation_ids: Vec::new(),
            sql_template: None,
            description: Some("right join normalized to left join".to_string()),
            request: Vec::new(),
            response_schema: None,
            response_formats: vec!["json".to_string()],
            query_policy_id: None,
            correction_horizon_ns: None,
        }),
    )
    .await;
    assert_eq!(response.0, StatusCode::CREATED, "{response:?}");

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "right-swap-scores",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [{"user_id": "alice", "score": 10, "delta": 1}]
                },
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "right-swap-accounts",
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

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/account_limits_right_join/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([
            {"account_id": "alice", "sum": 100, "count": 1},
            {"account_id": "bob", "sum": 50, "count": 1}
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    for catalog in [test_scores_catalog(), test_accounts_catalog()] {
        let relation_response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({
                "catalog": catalog
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
            correction_horizon_ns: None,
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

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-join-count-distinct-b",
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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
                "catalog": catalog
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
            correction_horizon_ns: None,
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

fn assert_latest_key_rows(
    response: &Value,
    expected_epoch: u64,
    expected_item_a: bool,
    expected_item_b: bool,
) {
    assert_eq!(response["logical_epoch"], expected_epoch);
    assert_eq!(
        response["rows"],
        json!([
            {"item": "item-a", "flag": expected_item_a},
            {"item": "item-b", "flag": expected_item_b}
        ])
    );
}

fn assert_window_rows(response: &Value, expected_epoch: u64, expected_rows: Value) {
    assert_eq!(response["logical_epoch"], expected_epoch);
    assert_eq!(response["rows"], expected_rows);
}

pub(super) fn test_runtime_checkpoint(output_manifest_refs: Vec<String>) -> RuntimeCheckpoint {
    let state_payload = serde_json::json!({
        "schema_version": 1,
        "published_output": {
            "records": []
        }
    });
    let state_payload = velorix_core::arrow_state::to_state(&state_payload).unwrap();
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
        input_coverage: None,
        causal_cut: None,
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

fn synthetic_state_text(bytes: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
    let mut random = 0x1234_5678_9abc_def0u64;
    (0..bytes)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            ALPHABET[(random >> 58) as usize] as char
        })
        .collect()
}

#[tokio::test]
async fn generic_state_fragments_preserve_hash_restore_and_reject_corruption() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_public_api_state_with_store(store.clone(), "fragment-owner", false)
        .await
        .with_standing_runtime_budget_limits(100_000, 16 * 1024 * 1024);
    let mut checkpoint = test_runtime_checkpoint(Vec::new());
    let payload = json!({
        "runtime_kind": "generic_set_ops_v1",
        "retained": synthetic_state_text(10 * 1024 * 1024),
    });
    let payload = velorix_core::arrow_state::to_state(&payload).unwrap();
    checkpoint.state_root.content_hash = stable_bytes_hash(payload.as_bytes());
    checkpoint.state_payload.as_mut().unwrap().payload = payload;
    validate_standing_runtime_budget(
        &[],
        &checkpoint,
        StandingRuntimeBudgetLimits::from_state(&state),
    )
    .unwrap();
    let (key, record) =
        standing_runtime_state_payload_record_for_checkpoint(&checkpoint, "purchases_by_user")
            .unwrap();
    assert_eq!(
        checkpoint_fragments::durable_state_object_count(
            checkpoint_arrow::encode(&record).unwrap().len(),
            state.max_standing_runtime_state_fragment_bytes
        ),
        3
    );
    persist_standing_runtime_state_payload(&state, &key, &record)
        .await
        .unwrap();
    let bytes = store
        .get(&Path::from(key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(bytes.len() < 64 * 1024);
    let fresh = test_public_api_state_with_store(store.clone(), "fragment-fresh-owner", false)
        .await
        .with_standing_runtime_budget_limits(100_000, 16 * 1024 * 1024);
    let restored = checkpoint_fragments::hydrate_fragments(&fresh, &key, &bytes)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.payload, record.payload);
    assert_eq!(restored.state_content_hash, record.state_content_hash);
    // Restore enforces current limits even when the original writer allowed more.
    let lower_total =
        test_public_api_state_with_store(store.clone(), "fragment-lower-total", false)
            .await
            .with_standing_runtime_budget_limits(100_000, 8 * 1024 * 1024);
    assert!(
        checkpoint_fragments::hydrate_fragments(&lower_total, &key, &bytes)
            .await
            .is_err()
    );
    let lower_fragment =
        test_public_api_state_with_store(store.clone(), "fragment-lower-fragment", false)
            .await
            .with_standing_runtime_budget_limits(100_000, 16 * 1024 * 1024)
            .with_standing_runtime_state_fragment_bytes(2 * 1024 * 1024)
            .unwrap();
    assert!(
        checkpoint_fragments::hydrate_fragments(&lower_fragment, &key, &bytes)
            .await
            .is_err()
    );
    let manifest: Value = serde_json::from_slice(&bytes).unwrap();
    for changed in [
        {
            let mut changed = manifest.clone();
            changed["fragment_bytes"] = json!(0);
            changed
        },
        {
            let mut changed = manifest.clone();
            changed["byte_count"] = json!(17 * 1024 * 1024);
            changed
        },
        {
            let mut changed = manifest.clone();
            changed["header"]["view_id"] = json!("foreign-view");
            changed
        },
        {
            let mut changed = manifest.clone();
            changed["fragments"].as_array_mut().unwrap().reverse();
            changed
        },
        {
            let mut changed = manifest.clone();
            changed["fragments"].as_array_mut().unwrap().pop();
            changed
        },
    ] {
        assert!(checkpoint_fragments::hydrate_fragments(
            &fresh,
            &key,
            &serde_json::to_vec(&changed).unwrap()
        )
        .await
        .is_err());
    }
    // Corrupt a durable fragment while retaining the immutable manifest.
    let hash = manifest["fragments"][0]["content_hash"].as_str().unwrap();
    let path = Path::from(format!("{}.fragment-0-{hash}", key.as_str()));
    store
        .put(&path, bytes::Bytes::from_static(b"corrupt").into())
        .await
        .unwrap();
    assert!(
        checkpoint_fragments::hydrate_fragments(&fresh, &key, &bytes)
            .await
            .is_err()
    );
    // Fragmentation cannot relax the configured total-state bound.
    checkpoint.state_payload.as_mut().unwrap().payload =
        velorix_core::arrow_state::to_state(&json!({
            "runtime_kind": "generic_set_ops_v1",
            "retained": "x".repeat(17 * 1024 * 1024),
        }))
        .unwrap();
    assert!(validate_standing_runtime_budget(
        &[],
        &checkpoint,
        StandingRuntimeBudgetLimits::from_state(&state)
    )
    .is_err());
}

#[test]
fn legacy_rebuild_frontiers_must_match_the_captured_source_cut() {
    let checkpoint = test_runtime_checkpoint(Vec::new());
    let coverage = RuntimeCheckpointInputCoverageV1 {
        schema_version: 1,
        view_generation: 1,
        plan_hash: "sha256:plan".to_string(),
        input_catalog_epoch: 1,
        relations: vec![RuntimeCheckpointRelationCoverageV1 {
            relation_id: "purchases".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            relation_generation: 1,
            schema_fingerprint: "sha256:purchases".to_string(),
            partitions: vec![RuntimeCheckpointPartitionCoverageV1 {
                stream_id: "test-stream".to_string(),
                stream_generation: 1,
                partition_id: 0,
                partition_generation: 1,
                covered_from_offset_inclusive: 0,
                processed_offset_exclusive: 11,
            }],
        }],
    };
    validate_legacy_rebuild_frontiers(&checkpoint, &coverage).unwrap();

    let mut mismatched = checkpoint.clone();
    mismatched.input_frontiers[0].committed_offset_exclusive = 10;
    let error = validate_legacy_rebuild_frontiers(&mismatched, &coverage).unwrap_err();
    assert!(error
        .to_string()
        .contains("do not match the captured source cut"));

    let mut missing = coverage;
    missing.relations[0].partitions.clear();
    let error = validate_legacy_rebuild_frontiers(&checkpoint, &missing).unwrap_err();
    assert!(error
        .to_string()
        .contains("do not match the captured source cut"));
}

#[tokio::test]
async fn standing_runtime_state_quota_rejection_rolls_back_before_publication() {
    let catalog = test_purchases_catalog();
    let sql =
        "select user_id, sum(amount) as sum, count(*) as count from purchases group by user_id";
    let supported = validate_catalog_backed_sum_count_view_sql(sql, &catalog).unwrap();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let output_schema = aggregate_output_schema("purchases_by_user", &catalog, &supported).unwrap();
    let spec = StandingViewSpec {
        view_id: "purchases_by_user".to_string(),
        sql: sql.to_string(),
        dialect: SqlDialect::VelorixSql,
        source_kind: SqlSourceKind::StandingView,
        input_relations: vec![input_schema.clone()],
        output_relations: vec![output_schema.clone()],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let identity = standing_program_identity_from_materialized_view_runtime(
        std::slice::from_ref(&catalog),
        &spec,
        &[],
    )
    .unwrap();
    let logical_plan =
        lower_supported_sql_to_logical_plan(sql, std::slice::from_ref(&catalog), &output_schema)
            .unwrap();
    let runtime = velorix_runtime::materialized_view_runtime::create_standing_runtime_with_logical_plan_and_catalogs(
        &identity,
        std::slice::from_ref(&catalog),
        logical_plan,
        &[input_schema],
        &[output_schema],
    )
    .unwrap();
    let runtime: SharedStandingRuntime = Arc::new(Mutex::new(runtime));
    let before = runtime.lock().unwrap().checkpoint().unwrap();
    let input = RelationInputBatch {
        encoding: RelationInputEncodingV1::SourceRelationV1,
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        stream_id: "quota-test".to_string(),
        partition_id: 0,
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        start_offset_inclusive: 0,
        end_offset_exclusive: 1,
        event_time_watermark: None,
        batches: vec![RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
                Field::new("delta", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["alice"])) as _,
                Arc::new(Int64Array::from(vec![10])) as _,
                Arc::new(Int64Array::from(vec![1])) as _,
            ],
        )
        .unwrap()],
    };

    let error = apply_standing_runtime_changes_and_checkpoint(
        &test_api_state().await,
        runtime.clone(),
        1,
        EpochIdempotencyKey::new("quota-epoch").unwrap(),
        input.clone(),
        StandingRuntimeBudgetLimits {
            max_output_delta_records: usize::MAX,
            max_state_payload_bytes: 1,
        },
    )
    .await
    .unwrap_err();
    assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(runtime.lock().unwrap().checkpoint().unwrap(), before);

    let applied = apply_standing_runtime_changes_and_checkpoint(
        &test_api_state().await,
        runtime.clone(),
        1,
        EpochIdempotencyKey::new("quota-epoch").unwrap(),
        input,
        StandingRuntimeBudgetLimits {
            max_output_delta_records: usize::MAX,
            max_state_payload_bytes: usize::MAX,
        },
    )
    .await
    .unwrap();
    assert_eq!(applied.checkpoint.logical_epoch, 1);
}

async fn test_api_state() -> ApiState {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    test_api_state_with_store(store, "api-test-owner", false).await
}

pub(super) async fn with_initialized_active_view_meta_store(
    state: ApiState,
    meta_store: Arc<dyn meta::MetaStore>,
) -> ApiState {
    let scope = view_dependencies::active_view_registry_scope(&state);
    let snapshot = meta_store
        .read_active_view_registry_snapshot(meta::ActiveViewRegistryReadRequestV1 {
            scope: scope.clone(),
        })
        .await
        .unwrap();
    if !snapshot.imported {
        meta_store
            .import_active_view_registry(meta::ImportActiveViewRegistryRequestV1 {
                scope,
                records: Vec::new(),
            })
            .await
            .unwrap();
    }
    state.with_meta_store(meta_store)
}

pub(super) async fn test_api_state_with_uninitialized_active_registry(
    store: Arc<dyn ObjectStore>,
    owner_id: &str,
) -> (ApiState, Arc<InMemoryMetaStore>) {
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
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let state = ApiState::from_validated_authority_with_ingest_admission_startup(
        validated, owner_id, false,
    )
    .await
    .unwrap()
    .with_meta_store(meta_store.clone())
    .with_experimental_advanced_view_features(true);
    (state, meta_store)
}

#[tokio::test]
async fn first_admission_initializes_empty_meta_active_registry() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (state, meta_store) =
        test_api_state_with_uninitialized_active_registry(store, "fresh-registry-owner").await;
    let scope = view_dependencies::active_view_registry_scope(&state);
    assert!(
        !meta_store
            .read_active_view_registry_snapshot(meta::ActiveViewRegistryReadRequestV1 {
                scope: scope.clone(),
            })
            .await
            .unwrap()
            .imported
    );

    let router = app(state);
    let catalog = crate::example_fixtures::default_scores_relation_catalog().unwrap();
    let relation = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog":catalog}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "{relation:?}");
    let mut request = crate::example_fixtures::default_positive_scores_view_request(
        &crate::example_fixtures::default_scores_relation_catalog().unwrap(),
    )
    .unwrap();
    request.view_id = "fresh_registry_view".into();
    let created = call_json(
        &router,
        Method::POST,
        "/v1/views",
        serde_json::to_value(request).unwrap(),
    )
    .await;
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");

    let snapshot = meta_store
        .read_active_view_registry_snapshot(meta::ActiveViewRegistryReadRequestV1 { scope })
        .await
        .unwrap();
    assert!(snapshot.imported);
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(snapshot.records[0].record.view_id, "fresh_registry_view");
}

#[tokio::test]
async fn first_admission_refuses_to_auto_import_nonempty_legacy_registry() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let catalog = crate::example_fixtures::default_scores_relation_catalog().unwrap();
    let mut request =
        crate::example_fixtures::default_positive_scores_view_request(&catalog).unwrap();
    request.view_id = "legacy_registry_fixture".into();
    // Create a valid active record through normal admission, then place that
    // record in the legacy S3 location to model a pre-cutover registry.
    let seed_state = with_initialized_active_view_meta_store(
        test_api_state_with_store(Arc::clone(&store), "legacy-seed-owner", false).await,
        Arc::new(InMemoryMetaStore::default()),
    )
    .await;
    let seed_router = app(seed_state.clone());
    let relation = call_json(
        &seed_router,
        Method::POST,
        "/v1/relations",
        json!({"catalog":catalog}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "{relation:?}");
    let created = call_json(
        &seed_router,
        Method::POST,
        "/v1/views",
        serde_json::to_value(request.clone()).unwrap(),
    )
    .await;
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    let seed_snapshot = view_dependencies::read_active_view_registry_snapshot(&seed_state)
        .await
        .unwrap();
    let active_record = seed_snapshot
        .records
        .iter()
        .find(|versioned| versioned.record.view_id == request.view_id)
        .expect("admitted seed view is recorded in Meta");
    store
        .put(
            &Path::from(format!("v1/views/{}/active.json", request.view_id)),
            bytes::Bytes::from(serde_json::to_vec(&active_record.record).unwrap()).into(),
        )
        .await
        .unwrap();

    let (state, meta_store) = test_api_state_with_uninitialized_active_registry(
        Arc::clone(&store),
        "legacy-registry-owner",
    )
    .await;

    let router = app(state.clone());
    let response = call_json(
        &router,
        Method::POST,
        "/v1/views",
        serde_json::to_value(request).unwrap(),
    )
    .await;
    assert_eq!(response.0, StatusCode::SERVICE_UNAVAILABLE, "{response:?}");
    assert!(response.1["error"]
        .as_str()
        .unwrap()
        .contains("run POST /v1/admin/import-active-view-registry"));

    let snapshot = meta_store
        .read_active_view_registry_snapshot(meta::ActiveViewRegistryReadRequestV1 {
            scope: view_dependencies::active_view_registry_scope(&state),
        })
        .await
        .unwrap();
    assert!(
        !snapshot.imported,
        "admission must not auto-import legacy state"
    );
    assert!(snapshot.records.is_empty());
    assert_eq!(
        state
            .view_registry()
            .unwrap()
            .list_active_records()
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn api_path_lookup_hydrates_only_the_matching_immutable_spec() {
    let counts = ObjectStoreAccessCounts::default();
    let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        Arc::new(InMemory::new()),
        counts.clone(),
    ));
    let state = test_api_state_with_store(store, "api-path-spec-read-owner", false).await;
    let router = app(state.clone());
    let catalog = crate::example_fixtures::default_scores_relation_catalog().unwrap();
    let relation = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": catalog.clone()}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "{relation:?}");

    for (view_id, url_path) in [
        ("api_path_unselected", "/metrics/unselected"),
        ("api_path_selected", "/metrics/selected"),
    ] {
        let mut request =
            crate::example_fixtures::default_positive_scores_view_request(&catalog).unwrap();
        request.view_id = view_id.to_string();
        request.url_path = Some(url_path.to_string());
        let created = call_json(
            &router,
            Method::POST,
            "/v1/views",
            serde_json::to_value(request).unwrap(),
        )
        .await;
        assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    }

    counts.clear();
    let (active, parameters, _) =
        query_serving::read_active_view_by_api_path(&state, "metrics/selected")
            .await
            .unwrap();
    assert_eq!(active.spec.view_id, "api_path_selected");
    assert!(parameters.is_empty());
    let spec_gets = counts
        .get_paths()
        .into_iter()
        .filter(|path| path.contains("/views/") && path.contains("/spec-sha256/"))
        .collect::<Vec<_>>();
    assert_eq!(
        spec_gets.len(),
        1,
        "a public API path lookup should hydrate only its selected immutable view spec"
    );
}

pub(super) async fn read_test_active_view(
    state: &ApiState,
    view_id: &str,
) -> Result<ActiveMaterializedView, ApiError> {
    view_dependencies::read_active_from_meta(state, view_id).await
}

pub(super) async fn list_test_active_views(
    state: &ApiState,
) -> Result<Vec<ActiveMaterializedView>, ApiError> {
    view_dependencies::list_active_from_meta(state).await
}

pub(super) async fn update_test_active_view_lifecycle(
    state: &ApiState,
    view_id: &str,
    expected_spec_hash: &str,
    lifecycle: MaterializedViewLifecycleStatus,
) -> Result<(), ApiError> {
    view_dependencies::update_active_view_lifecycle_admin(
        state,
        view_id,
        expected_spec_hash,
        lifecycle,
    )
    .await
}

pub(super) async fn set_test_active_view_query_policy(
    state: &ApiState,
    view_id: &str,
    query_policy_id: Option<String>,
) {
    let snapshot = view_dependencies::read_active_view_registry_snapshot(state)
        .await
        .unwrap();
    let versioned = snapshot
        .records
        .iter()
        .find(|versioned| versioned.record.view_id == view_id)
        .unwrap_or_else(|| panic!("missing active test view {view_id}"));
    let mut record = versioned.record.clone();
    record
        .api
        .as_mut()
        .expect("active test view has API metadata")
        .query_policy_id = query_policy_id;
    view_dependencies::compare_exchange_active_view_record(
        state,
        view_id,
        versioned.generation,
        record,
        None,
    )
    .await
    .unwrap();
}

pub(super) async fn test_api_state_with_store(
    store: Arc<dyn ObjectStore>,
    owner_id: &str,
    reconstruct_ingest_admission: bool,
) -> ApiState {
    test_public_api_state_with_store(store, owner_id, reconstruct_ingest_admission)
        .await
        .with_experimental_advanced_view_features(true)
}

// Restart fixtures retain durable Meta state just as they retain the object
// store. Reusing the owner identity models a restart of the same test process;
// lease takeover/fencing behavior is covered by the dedicated owner tests.
async fn restart_test_api_state_with_store(
    meta_store: Arc<dyn MetaStore>,
    process_owner_id: String,
    store: Arc<dyn ObjectStore>,
    _fixture_label: &str,
    reconstruct_ingest_admission: bool,
) -> ApiState {
    let mut restarted =
        test_api_state_with_store(store, &process_owner_id, reconstruct_ingest_admission).await;
    restarted.meta_store = Some(Arc::clone(&meta_store));
    restarted.view_bootstrap_meta_store = Some(meta_store);
    restarted.owner_id = process_owner_id;
    restarted
}

async fn test_checkpoint_persist_context(
    state: &ApiState,
    checkpoint: &RuntimeCheckpoint,
    view_id: &str,
) -> StandingRuntimeCheckpointPersistContext {
    let owner = state
        .acquire_standing_runtime_owner(&checkpoint.identity, view_id)
        .await
        .unwrap()
        .expect("Meta-backed checkpoint fixture requires its runtime owner");
    StandingRuntimeCheckpointPersistContext::new(None, Vec::new(), Some(owner))
}

async fn object_only_checkpoint_codec_state() -> ApiState {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let authority = validate_operator_authority(
        ObjectStoreAuthorityRef {
            store_id: "checkpoint-codec-test".into(),
            namespace: "standalone".into(),
        },
        store,
        "memory",
        "checkpoint-codec-test",
    )
    .await
    .unwrap();
    // These two tests exercise only object-store record decoding. Keep this
    // isolated state Meta-free so they test the legacy object-only read path.
    ApiState::from_validated_authority_with_ingest_admission_startup(
        authority,
        "checkpoint-codec-test",
        false,
    )
    .await
    .unwrap()
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
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let state = ApiState::from_validated_authority_with_ingest_admission_startup(
        validated,
        owner_id,
        reconstruct_ingest_admission,
    )
    .await
    .unwrap()
    .with_view_bootstrap_meta_store(meta_store.clone());
    with_initialized_active_view_meta_store(state, meta_store).await
}

struct FailingApplyRuntimeFactory {
    delegate: MaterializedViewRuntimeFactory,
    reason: String,
}

struct CommonDagReferenceRuntimeFactory {
    delegate: MaterializedViewRuntimeFactory,
}

impl CommonDagReferenceRuntimeFactory {
    fn new() -> Self {
        Self {
            delegate: MaterializedViewRuntimeFactory,
        }
    }
}

impl StandingProgramRuntimeFactory for CommonDagReferenceRuntimeFactory {
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
        _identity: &StandingProgramIdentity,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Err("common DAG reference runtime requires an admitted join plan".to_string())
    }

    fn create_with_catalogs_plan_and_spec(
        &self,
        identity: &StandingProgramIdentity,
        catalogs: &[VelorixRelationCatalogV1],
        logical_plan: &VelorixLogicalViewPlanV1,
        _spec: &StandingViewSpec,
        input_schemas: &[RelationSchema],
        output_schemas: &[RelationSchema],
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        velorix_runtime::materialized_view_runtime::create_common_dag_reference_standing_runtime_with_logical_plan_and_catalogs(
            identity,
            catalogs,
            logical_plan.clone(),
            input_schemas,
            output_schemas,
        )
    }

    fn restore(
        &self,
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        velorix_runtime::materialized_view_runtime::restore_common_dag_reference_standing_runtime(
            checkpoint,
        )
    }
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
            "catalog": test_scores_catalog()
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    // Phase 5 gate split: event-time window SQL is now a public 1.0
    // capability; analytic window SQL stays experimental. The view id is
    // deliberately neutral so the assertion cannot pass via the echoed id.
    for sql in [
        "select user_id, row_number() over (partition by user_id order by score desc, user_id asc) as rank from scores",
        "select user_id, sum(score)\nover (partition by user_id) as total from scores",
        "select user_id, rank() over (order by score) as ranking from scores",
        "select user_id, dense_rank() over (order by score) as ranking from scores",
    ] {
        let view_response = call_json(
            &router,
            Method::POST,
            "/v1/views",
            json!({
                "view_id": "analytic_view",
                "input_relation_id": "scores",
                "input_relation_version": "2026-05-24.v1",
                "sql": sql
            }),
        )
        .await;
        assert_eq!(
            view_response.0,
            StatusCode::BAD_REQUEST,
            "{view_response:?}"
        );
        assert!(
            view_response.1["error"]
                .as_str()
                .unwrap_or_default()
                .contains("analytic window SQL is experimental"),
            "{view_response:?}"
        );
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
        published_input_bootstrap: None,
        tenant_id: checkpoint.identity.tenant_id.clone(),
        program_id: checkpoint.identity.program_id.clone(),
        view_id: "purchases_by_user".to_string(),
        checkpoint_key: checkpoint_key.as_str().to_string(),
        logical_epoch: checkpoint.logical_epoch,
        content_hash: checkpoint.state_root.content_hash.clone(),
        manifest_hash: "sha256:test-manifest".to_string(),
        output_manifest_refs: checkpoint.output_manifest_refs.clone(),
        bootstrap_generation: 0,
        plan_hash: String::new(),
        coverage_hash: String::new(),
        input_coverage: None,
        previous_checkpoint_key: String::new(),
        previous_manifest_hash: String::new(),
    }
}

fn test_checkpoint_record(
    checkpoint_key: &ObjectKey,
    checkpoint: RuntimeCheckpoint,
) -> StandingRuntimeCheckpointRecord {
    StandingRuntimeCheckpointRecord {
        published_input_bootstrap: None,
        demand_materialization: None,
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
        relation_source: VelorixRelationSourceV1::SourceRelation,
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

fn test_order_facts_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "order_facts".to_string(),
        relation_name: "order_facts".to_string(),
        relation_version: "2026-08-10.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "order_id".to_string(),
                name: "order_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "user_id".to_string(),
                name: "user_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Metadata,
            },
            RelationColumnV1 {
                column_id: "category".to_string(),
                name: "category".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: true,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Metadata,
            },
            RelationColumnV1 {
                column_id: "amount".to_string(),
                name: "amount".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 3,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 4,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["order_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    VelorixRelationCatalogV1::from_relation_schema(
        relation_schema,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
    )
    .unwrap()
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

#[tokio::test]
async fn public_1_0_admits_event_time_window_sql_by_default() {
    let state = test_public_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-public-1-0-event-time",
        false,
    )
    .await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_purchases_event_time_catalog()
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    for sql in [
        "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end",
        "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from hop(purchases, event_time, interval '5 seconds', interval '60 seconds') group by user_id, window_start, window_end",
        "select user_id, window_start, window_end, sum(amount) as total_amount, count(*) as event_count from session(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end",
    ] {
        let view_id = match sql {
            sql if sql.contains("tumble") => "tumbling_view",
            sql if sql.contains("hop(") => "hopping_view",
            _ => "session_view",
        };
        let view_response = call_json(
            &router,
            Method::POST,
            "/v1/views",
            json!({
                "view_id": view_id,
                "input_relation_id": "purchases",
                "input_relation_version": "2026-05-24.v1",
                "sql": sql,
                "source_kind": "standing_view"
            }),
        )
        .await;
        assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    }
}

#[tokio::test]
async fn rest_http_two_writers_materialize_exact_aggregate_with_latency_diagnostics() {
    let state =
        test_public_api_state_with_store(Arc::new(InMemory::new()), "http-load", false).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, app(state))
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    let base = format!("http://{address}");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap();
    for (path, body) in [
        (
            "/v1/relations",
            json!({"catalog":test_order_facts_catalog()}),
        ),
        (
            "/v1/views",
            json!({"view_id":"http_orders_by_user","input_relation_id":"order_facts",
            "input_relation_version":"2026-08-10.v1",
            "sql":"select user_id, sum(amount) as sum, count(*) as count from order_facts group by user_id",
            "source_kind":"standing_view","response_formats":["json"]}),
        ),
    ] {
        let response = client
            .post(format!("{base}{path}"))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(status.as_u16(), 201, "{body}");
    }
    // Independent streams, sequential offsets within each stream, and an 87.5% hot key.
    // This is bounded loopback load, not a production throughput certification.
    let writer = |writer_id: usize| {
        let client = client.clone();
        let base = base.clone();
        async move {
            let mut samples = Vec::new();
            for batch in 0..32 {
                let rows: Vec<_> = (0..8)
                    .map(|row| {
                        json!({
                            "order_id":format!("{writer_id}-{batch}-{row}"),
                            "user_id":if row == 7 { "cold" } else { "hot" },
                            "category":"load","amount":1,"delta":1
                        })
                    })
                    .collect();
                let started = std::time::Instant::now();
                let response = client
                    .post(format!("{base}/v1/relations/order_facts/ingest"))
                    .json(&json!({"relation_version":"2026-08-10.v1",
                        "stream_id":format!("http-writer-{writer_id}"),"partition_id":0,
                        "start_offset_inclusive":batch * 8,"rows":rows}))
                    .send()
                    .await
                    .unwrap();
                let status = response.status();
                let body = response.text().await.unwrap();
                samples.push(started.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(status.as_u16(), 201, "{body}");
            }
            samples
        }
    };
    let started = std::time::Instant::now();
    let (mut samples, other) = tokio::join!(writer(0), writer(1));
    let ingest_seconds = started.elapsed().as_secs_f64();
    samples.extend(other);
    samples.sort_by(f64::total_cmp);
    assert_eq!(samples.len(), 64);
    let query_started = std::time::Instant::now();
    let response = client
        .post(format!("{base}/v1/views/http_orders_by_user/query"))
        .header(header::ACCEPT, "application/json")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let body: Value = response.json().await.unwrap();
    let query_ms = query_started.elapsed().as_secs_f64() * 1000.0;
    let mut rows = body["rows"].as_array().unwrap().clone();
    rows.sort_by_key(Value::to_string);
    let mut expected = vec![
        json!({"user_id":"hot","sum":448,"count":448}),
        json!({"user_id":"cold","sum":64,"count":64}),
    ];
    expected.sort_by_key(Value::to_string);
    assert_eq!(rows, expected);
    eprintln!(
        "{}",
        json!({"scope":"loopback_http_in_memory_store","writers":2,
        "rows":512,"requests":samples.len(),"errors":0,"batch_rows":8,
        "ingest_rows_per_second":512.0 / ingest_seconds,
        "ingest_p50_ms":samples[31],"ingest_p95_ms":samples[60],
        "query_ms":query_ms,"performance_gate":false})
    );
    let _ = shutdown_tx.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn rest_join_families_materialize_retract_and_restart() {
    for interval in [false, true] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state = test_public_api_state_with_store(store.clone(), "join-family-a", false).await;
        let restart_meta = Arc::clone(
            state
                .meta_store
                .as_ref()
                .expect("restart fixture requires Meta"),
        );
        let restart_owner_id = state.owner_id.clone();
        let router = app(state);
        let (catalogs, sql, left_rows, right_rows, expected, after_retract) = if interval {
            (
                vec![
                    test_interval_side_catalog_for_e2e(
                        "rides",
                        "ride_id",
                        "booking_start",
                        "booking_end_time",
                    ),
                    test_interval_side_catalog_for_e2e(
                        "vehicles",
                        "vehicle_id",
                        "capacity_start_time",
                        "capacity_end",
                    ),
                ],
                "select r.ride_id, r.booking_start, r.booking_end_time from rides r join vehicles v on r.booking_start < v.capacity_end and v.capacity_start_time < r.booking_end_time",
                json!([{"ride_id":"r1","booking_start":10,"booking_end_time":20,"delta":1}]),
                json!([
                    {"vehicle_id":"v1","capacity_start_time":15,"capacity_end":25,"delta":1},
                    {"vehicle_id":"v2","capacity_start_time":18,"capacity_end":22,"delta":1},
                    {"vehicle_id":"touch_right","capacity_start_time":20,"capacity_end":30,"delta":1},
                    {"vehicle_id":"touch_left","capacity_start_time":0,"capacity_end":10,"delta":1},
                    {"vehicle_id":"disjoint","capacity_start_time":30,"capacity_end":40,"delta":1}
                ]),
                json!([
                    {"ride_id":"r1","booking_start":10,"booking_end_time":20,"vehicle_id":"v1"},
                    {"ride_id":"r1","booking_start":10,"booking_end_time":20,"vehicle_id":"v2"}
                ]),
                json!([{ "ride_id":"r1","booking_start":10,"booking_end_time":20,"vehicle_id":"v2"}]),
            )
        } else {
            (
                vec![
                    generic_adapter_catalog(test_scores_catalog()),
                    generic_adapter_catalog(test_accounts_catalog()),
                ],
                "select s.user_id, a.account_id, s.score, a.tier from scores s cross join accounts a",
                json!([{"user_id":"alice","score":10,"delta":1},{"user_id":"bob","score":5,"delta":1}]),
                json!([{"account_id":"a1","limit":100,"tier":"gold","delta":1},{"account_id":"a2","limit":50,"tier":"silver","delta":1}]),
                json!([
                    {"user_id":"alice","account_id":"a1","score":10,"tier":"gold"},
                    {"user_id":"alice","account_id":"a2","score":10,"tier":"silver"},
                    {"user_id":"bob","account_id":"a1","score":5,"tier":"gold"},
                    {"user_id":"bob","account_id":"a2","score":5,"tier":"silver"}
                ]),
                json!([
                    {"user_id":"alice","account_id":"a2","score":10,"tier":"silver"},
                    {"user_id":"bob","account_id":"a2","score":5,"tier":"silver"}
                ]),
            )
        };
        let refs: Vec<_> = catalogs
            .iter()
            .map(|catalog| {
                json!({
                    "relation_id":catalog.relation_schema.relation_id,
                    "relation_version":catalog.relation_schema.relation_version
                })
            })
            .collect();
        for catalog in catalogs {
            let response = call_json(
                &router,
                Method::POST,
                "/v1/relations",
                json!({"catalog":catalog}),
            )
            .await;
            assert_eq!(
                response.0,
                StatusCode::CREATED,
                "interval={interval}: {response:?}"
            );
        }
        let response = call_json(
            &router,
            Method::POST,
            "/v1/views",
            json!({
                "view_id":"join_pairs","input_relation_refs":refs,"sql":sql,
                "source_kind":"standing_view","response_formats":["json"]
            }),
        )
        .await;
        assert_eq!(
            response.0,
            StatusCode::CREATED,
            "interval={interval}: {response:?}"
        );
        for (index, rows) in [&left_rows, &right_rows].into_iter().enumerate() {
            let response = call_json(&router, Method::POST, "/v1/relations/ingest", json!({"batches":[{
                "relation_id":refs[index]["relation_id"],"relation_version":refs[index]["relation_version"],
                "stream_id":format!("join-stream-{index}"),"partition_id":0,"start_offset_inclusive":0,"rows":rows
            }]})).await;
            assert_eq!(
                response.0,
                StatusCode::CREATED,
                "interval={interval}: {response:?}"
            );
        }
        let restarted = restart_test_api_state_with_store(
            restart_meta.clone(),
            restart_owner_id.clone(),
            store,
            "join-family-b",
            true,
        )
        .await;
        assert_eq!(
            restarted
                .restore_standing_program_runtimes_from_active_views()
                .await
                .unwrap(),
            1
        );
        let restarted_router = app(restarted);
        for current in [&router, &restarted_router] {
            let response = call_json(
                current,
                Method::POST,
                "/v1/views/join_pairs/query",
                json!({}),
            )
            .await;
            assert_eq!(
                response.0,
                StatusCode::OK,
                "interval={interval}: {response:?}"
            );
            let mut actual = response.1["rows"].as_array().unwrap().clone();
            let mut expected = expected.as_array().unwrap().clone();
            actual.sort_by_key(Value::to_string);
            expected.sort_by_key(Value::to_string);
            assert_eq!(actual, expected, "interval={interval}");
        }
        let mut retracted = right_rows[0].clone();
        retracted["delta"] = json!(-1);
        let response = call_json(&restarted_router, Method::POST, "/v1/relations/ingest", json!({"batches":[{
            "relation_id":refs[1]["relation_id"],"relation_version":refs[1]["relation_version"],
            "stream_id":"join-stream-1","partition_id":0,"start_offset_inclusive":right_rows.as_array().unwrap().len(),"rows":[retracted]
        }]})).await;
        assert_eq!(
            response.0,
            StatusCode::CREATED,
            "interval={interval}: {response:?}"
        );
        let response = call_json(
            &restarted_router,
            Method::POST,
            "/v1/views/join_pairs/query",
            json!({}),
        )
        .await;
        assert_eq!(
            response.0,
            StatusCode::OK,
            "interval={interval}: {response:?}"
        );
        let mut actual = response.1["rows"].as_array().unwrap().clone();
        let mut expected = after_retract.as_array().unwrap().clone();
        actual.sort_by_key(Value::to_string);
        expected.sort_by_key(Value::to_string);
        assert_eq!(actual, expected, "interval={interval}");
    }
}

#[tokio::test]
async fn rest_temporal_asof_join_materializes_retracts_and_restores() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "api-test-temporal-asof-a", false).await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);
    let rides = test_temporal_asof_catalog("rides", "ride_id", "booking_start");
    let prices = test_temporal_asof_catalog("prices", "vehicle_id", "price");
    for catalog in [rides, prices] {
        let response = call_json(
            &router,
            Method::POST,
            "/v1/relations",
            json!({"catalog": catalog}),
        )
        .await;
        assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
    }

    let input_relation_refs = json!([
        {"relation_id": "rides", "relation_version": "2026-08-14.v1"},
        {"relation_id": "prices", "relation_version": "2026-08-14.v1"}
    ]);
    let sql = "select l.ride_id, l.booking_start, l.event_time, p.price, p.event_time as price_event_time from rides l ASOF JOIN prices p MATCH_CONDITION(p.event_time <= l.event_time) ON l.ride_id = p.vehicle_id WHERE p.vehicle_id IS NOT NULL";
    let view_response = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "temporal_asof_pricing",
            "input_relation_refs": input_relation_refs,
            "sql": sql,
            "source_kind": "standing_view",
            "response_formats": ["json"]
        }),
    )
    .await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let rejected_cases = [
        (
            "temporal_generic_inner_rejected",
            "select l.ride_id, l.booking_start, l.event_time, p.price, p.event_time as price_event_time from rides l join prices p on p.event_time <= l.event_time",
        ),
        (
            "temporal_missing_where_rejected",
            "select l.ride_id, l.booking_start, l.event_time, p.price, p.event_time as price_event_time from rides l ASOF JOIN prices p MATCH_CONDITION(p.event_time <= l.event_time) ON l.ride_id = p.vehicle_id",
        ),
    ];
    for (view_id, rejected_sql) in rejected_cases {
        let response = call_json(
            &router,
            Method::POST,
            "/v1/views",
            json!({
                "view_id": view_id,
                "input_relation_refs": input_relation_refs,
                "sql": rejected_sql,
                "source_kind": "standing_view",
                "response_formats": ["json"]
            }),
        )
        .await;
        assert_eq!(response.0, StatusCode::BAD_REQUEST, "{response:?}");
    }

    let prices_batch = json!({
        "relation_id": "prices",
        "relation_version": "2026-08-14.v1",
        "stream_id": "temporal-asof-prices",
        "partition_id": 0,
        "start_offset_inclusive": 0,
        "rows": [
            {"vehicle_id": "p1", "price": 1000, "event_time": 100, "delta": 1},
            {"vehicle_id": "p1", "price": 2000, "event_time": 200, "delta": 1},
            {"vehicle_id": "p1", "price": 3000, "event_time": 300, "delta": 1},
            {"vehicle_id": "p2", "price": 999, "event_time": 240, "delta": 1}
        ]
    });
    let ingest_prices = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({"batches": [prices_batch]}),
    )
    .await;
    assert_eq!(ingest_prices.0, StatusCode::CREATED, "{ingest_prices:?}");

    let ingest_rides = call_json(
        &router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "rides",
                "relation_version": "2026-08-14.v1",
                "stream_id": "temporal-asof-rides",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"ride_id": "p1", "booking_start": 10, "event_time": 250, "delta": 1},
                    {"ride_id": "missing", "booking_start": 20, "event_time": 250, "delta": 1}
                ]
            }]
        }),
    )
    .await;
    assert_eq!(ingest_rides.0, StatusCode::CREATED, "{ingest_rides:?}");

    let expected_before_retract = json!([{
        "ride_id": "p1",
        "booking_start": 10,
        "event_time": 250,
        "price": 2000,
        "price_event_time": 200
    }]);
    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/temporal_asof_pricing/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(query.1["rows"], expected_before_retract, "{query:?}");

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-temporal-asof-b",
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
    let restarted_router = app(restarted_state);
    let restarted_query = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/temporal_asof_pricing/query",
        json!({}),
    )
    .await;
    assert_eq!(restarted_query.0, StatusCode::OK, "{restarted_query:?}");
    assert_eq!(
        restarted_query.1["rows"], expected_before_retract,
        "{restarted_query:?}"
    );

    let retract = call_json(
        &restarted_router,
        Method::POST,
        "/v1/relations/ingest",
        json!({
            "batches": [{
                "relation_id": "prices",
                "relation_version": "2026-08-14.v1",
                "stream_id": "temporal-asof-prices",
                "partition_id": 0,
                "start_offset_inclusive": 4,
                "rows": [{"vehicle_id": "p1", "price": 2000, "event_time": 200, "delta": -1}]
            }]
        }),
    )
    .await;
    assert_eq!(retract.0, StatusCode::CREATED, "{retract:?}");
    let query_after_retract = call_json(
        &restarted_router,
        Method::POST,
        "/v1/views/temporal_asof_pricing/query",
        json!({}),
    )
    .await;
    assert_eq!(
        query_after_retract.0,
        StatusCode::OK,
        "{query_after_retract:?}"
    );
    assert_eq!(
        query_after_retract.1["rows"],
        json!([{
            "ride_id": "p1",
            "booking_start": 10,
            "event_time": 250,
            "price": 1000,
            "price_event_time": 100
        }]),
        "{query_after_retract:?}"
    );
}

#[tokio::test]
async fn rest_relation_admission_rejects_internal_published_view_output_source_kind() {
    let state = test_api_state().await;
    let router = app(state);
    let mut catalog = test_scores_catalog();
    catalog.relation_source = VelorixRelationSourceV1::PublishedViewOutput {
        producer_view_id: "producer".to_string(),
        producer_view_generation: 1,
        output_stream_id: "producer/v1".to_string(),
    };
    let rejected = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": catalog
        }),
    )
    .await;
    assert_eq!(rejected.0, StatusCode::BAD_REQUEST, "{rejected:?}");
    assert!(
        rejected.1["error"]
            .as_str()
            .unwrap_or_default()
            .contains("PublishedViewOutput"),
        "{rejected:?}"
    );
}

#[tokio::test]
async fn rest_recursive_cte_outer_first_ignores_disjoint_second_cte_across_restore() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state =
        test_api_state_with_store(store.clone(), "api-test-recursive-outer-first-a", false).await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);
    let catalog = test_edges_catalog_for_e2e();
    let relation_version = catalog.relation_schema.relation_version.clone();

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": catalog}),
    )
    .await;
    assert_eq!(
        relation_response.0,
        StatusCode::CREATED,
        "{relation_response:?}"
    );

    let sql = "with recursive fwd as (select src, dst from edges union distinct select r.src, e.dst from fwd r join edges e on r.dst = e.src), bwd as (select dst as src, src as dst from edges where src = 'isolated' union distinct select r.src, e.dst from bwd r join edges e on r.dst = e.src) select src, dst from fwd";
    let view_response = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "recursive_outer_first",
            "sql": sql,
            "input_relation_id": "edges",
            "input_relation_version": relation_version,
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
                "relation_id": "edges",
                "relation_version": "2026-08-14.v1",
                "stream_id": "recursive-outer-first-stream",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    {"edge_id": "e1", "src": "a", "dst": "b", "delta": 1},
                    {"edge_id": "e2", "src": "b", "dst": "c", "delta": 1},
                    {"edge_id": "e3", "src": "isolated", "dst": "leaf", "delta": 1}
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
        {"src": "a", "dst": "b"},
        {"src": "a", "dst": "c"},
        {"src": "b", "dst": "c"},
        {"src": "isolated", "dst": "leaf"}
    ]);
    let query_response = call_json(
        &router,
        Method::POST,
        "/v1/views/recursive_outer_first/query",
        json!({}),
    )
    .await;
    assert_eq!(query_response.0, StatusCode::OK, "{query_response:?}");
    assert_eq!(
        query_response.1["rows"], expected_rows,
        "{query_response:?}"
    );

    let restored_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store,
        "api-test-recursive-outer-first-b",
        true,
    )
    .await;
    assert_eq!(
        restored_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let restored_query = call_json(
        &app(restored_state),
        Method::POST,
        "/v1/views/recursive_outer_first/query",
        json!({}),
    )
    .await;
    assert_eq!(restored_query.0, StatusCode::OK, "{restored_query:?}");
    assert_eq!(
        restored_query.1["rows"], expected_rows,
        "{restored_query:?}"
    );
}

#[tokio::test]
async fn phase8_families_derive_output_schemas_for_view_requests() {
    let state = test_public_api_state_with_store(
        Arc::new(InMemory::new()),
        "api-test-phase8-output-schemas",
        false,
    )
    .await;
    let edges = test_edges_catalog_for_e2e();
    let rides =
        test_interval_side_catalog_for_e2e("rides", "ride_id", "booking_start", "booking_end_time");
    let vehicles = test_interval_side_catalog_for_e2e(
        "vehicles",
        "vehicle_id",
        "capacity_start_time",
        "capacity_end",
    );
    let scores = test_scores_catalog();
    let accounts = test_accounts_catalog();
    let edges_generic = generic_adapter_catalog(edges);
    let scores_generic = generic_adapter_catalog(scores);
    let accounts_generic = generic_adapter_catalog(accounts);

    let recursive = state
        .materialized_runtime_output_schemas_for_view_request(
            "reachability",
            "with recursive reach as (select src, dst from edges union distinct select r.src, e.dst from reach r join edges e on r.dst = e.src) select src, dst from reach",
            std::slice::from_ref(&edges_generic),
            edges_generic.schema_fingerprint.as_str(),
        )
        .unwrap()
        .expect("recursive CTE view must derive an output schema");
    let schema = &recursive[0];
    assert_eq!(
        schema.primary_key,
        vec!["src".to_string(), "dst".to_string()]
    );
    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column.data_type == SqlDataType::Utf8))
            .collect::<Vec<_>>(),
        vec![("src", true), ("dst", true)]
    );

    let interval = state
        .materialized_runtime_output_schemas_for_view_request(
            "interval_matches",
            "select r.ride_id, r.booking_start, r.booking_end_time from rides r join vehicles v on r.booking_start < v.capacity_end and v.capacity_start_time < r.booking_end_time",
            &[rides, vehicles],
            "",
        )
        .unwrap()
        .expect("interval join view must derive an output schema");
    let schema = &interval[0];
    assert_eq!(
        schema.primary_key,
        vec![
            "ride_id".to_string(),
            "booking_start".to_string(),
            "booking_end_time".to_string(),
            "vehicle_id".to_string()
        ]
    );

    let cross = state
        .materialized_runtime_output_schemas_for_view_request(
            "pair_matches",
            "select s.user_id, a.account_id, s.score, a.limit from scores s cross join accounts a",
            &[scores_generic, accounts_generic],
            "",
        )
        .unwrap()
        .expect("cross join view must derive an output schema");
    let schema = &cross[0];
    assert_eq!(
        schema.primary_key,
        vec![
            "user_id".to_string(),
            "account_id".to_string(),
            "score".to_string(),
            "limit".to_string()
        ]
    );
}

fn generic_adapter_catalog(mut catalog: VelorixRelationCatalogV1) -> VelorixRelationCatalogV1 {
    catalog.incremental_adapter.adapter_id = CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string();
    catalog
}

fn test_edges_catalog_for_e2e() -> VelorixRelationCatalogV1 {
    generic_adapter_catalog(test_relation_catalog_for_e2e(
        "edges",
        &[
            ("edge_id", VelorixLogicalTypeV1::Utf8, false),
            ("src", VelorixLogicalTypeV1::Utf8, false),
            ("dst", VelorixLogicalTypeV1::Utf8, false),
        ],
    ))
}

fn test_interval_side_catalog_for_e2e(
    relation_id: &str,
    key_column: &str,
    start_column: &str,
    end_column: &str,
) -> VelorixRelationCatalogV1 {
    generic_adapter_catalog(test_relation_catalog_for_e2e(
        relation_id,
        &[
            (key_column, VelorixLogicalTypeV1::Utf8, false),
            (start_column, VelorixLogicalTypeV1::Int64, false),
            (end_column, VelorixLogicalTypeV1::Int64, false),
        ],
    ))
}

fn test_temporal_asof_catalog(
    relation_id: &str,
    key_column: &str,
    value_column: &str,
) -> VelorixRelationCatalogV1 {
    let mut catalog = generic_adapter_catalog(test_relation_catalog_for_e2e(
        relation_id,
        &[
            (key_column, VelorixLogicalTypeV1::Utf8, false),
            (value_column, VelorixLogicalTypeV1::Int64, false),
            ("event_time", VelorixLogicalTypeV1::Int64, false),
        ],
    ));
    catalog.relation_schema.event_time_column_id = Some("event_time".to_string());
    catalog
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "event_time")
        .expect("temporal as-of event-time column")
        .semantic_role = RelationSemanticRoleV1::EventTime;
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("temporal as-of catalog should fingerprint");
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.incremental_relation.schema_fingerprint = schema_fingerprint;
    catalog
}

fn test_relation_catalog_for_e2e(
    relation_id: &str,
    columns: &[(&str, VelorixLogicalTypeV1, bool)],
) -> VelorixRelationCatalogV1 {
    let mut schema_columns = Vec::new();
    for (index, (column_id, logical_type, nullable)) in columns.iter().enumerate() {
        let physical_arrow_type = match logical_type {
            VelorixLogicalTypeV1::Utf8 => ArrowPhysicalTypeV1::Utf8,
            VelorixLogicalTypeV1::Int64 => ArrowPhysicalTypeV1::Int64,
            _ => panic!("e2e test catalog supports utf8 and int64 only"),
        };
        schema_columns.push(RelationColumnV1 {
            column_id: column_id.to_string(),
            name: column_id.to_string(),
            logical_type: logical_type.clone(),
            physical_arrow_type,
            nullable: *nullable,
            ordinal: index as u32,
            semantic_role: if index == 0 {
                RelationSemanticRoleV1::PrimaryKey
            } else {
                RelationSemanticRoleV1::Value
            },
        });
    }
    schema_columns.push(RelationColumnV1 {
        column_id: "delta".to_string(),
        name: "delta".to_string(),
        logical_type: VelorixLogicalTypeV1::Int64,
        physical_arrow_type: ArrowPhysicalTypeV1::Int64,
        nullable: false,
        ordinal: columns.len() as u32,
        semantic_role: RelationSemanticRoleV1::Weight,
    });
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: relation_id.to_string(),
        relation_name: relation_id.to_string(),
        relation_version: "2026-08-14.v1".to_string(),
        columns: schema_columns,
        primary_key_column_ids: vec![columns[0].0.to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: relation_id.to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: relation_id.to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
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
        relation_source: VelorixRelationSourceV1::SourceRelation,
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
        relation_source: VelorixRelationSourceV1::SourceRelation,
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

fn test_three_input_composite_catalogs() -> [VelorixRelationCatalogV1; 3] {
    let add_tenant_key = |mut catalog: VelorixRelationCatalogV1| {
        let weight_index = catalog
            .relation_schema
            .columns
            .iter()
            .position(|column| column.column_id == catalog.relation_schema.weight_column_id)
            .unwrap();
        catalog.relation_schema.columns.insert(
            weight_index,
            RelationColumnV1 {
                column_id: "tenant_id".into(),
                name: "tenant_id".into(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
        );
        for (ordinal, column) in catalog.relation_schema.columns.iter_mut().enumerate() {
            column.ordinal = ordinal as u32;
        }
        catalog
            .relation_schema
            .primary_key_column_ids
            .insert(0, "tenant_id".into());
        catalog.incremental_adapter.adapter_id = CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into();
        let fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
            .expect("composite API catalog should fingerprint");
        catalog.schema_fingerprint = fingerprint.clone();
        catalog.incremental_relation.schema_fingerprint = fingerprint;
        catalog
    };
    let scores = add_tenant_key(test_scores_catalog());
    let mut accounts = add_tenant_key(test_accounts_catalog());
    let tenant = accounts
        .relation_schema
        .columns
        .iter_mut()
        .find(|column| column.column_id == "tenant_id")
        .unwrap();
    tenant.column_id = "account_tenant_id".into();
    tenant.name = "account_tenant_id".into();
    accounts.relation_schema.primary_key_column_ids =
        vec!["account_id".into(), "account_tenant_id".into()];
    let fingerprint = SchemaFingerprintV1::for_relation_schema(&accounts.relation_schema)
        .expect("renamed composite API catalog should fingerprint");
    accounts.schema_fingerprint = fingerprint.clone();
    accounts.incremental_relation.schema_fingerprint = fingerprint;
    let mut profiles = accounts.clone();
    profiles.relation_schema.relation_id = "profiles".into();
    profiles.relation_schema.relation_name = "profiles".into();
    profiles.datafusion_registration.name = "profiles".into();
    profiles.incremental_relation.relation_id = "profiles".into();
    let fingerprint = SchemaFingerprintV1::for_relation_schema(&profiles.relation_schema)
        .expect("profile API catalog should fingerprint");
    profiles.schema_fingerprint = fingerprint.clone();
    profiles.incremental_relation.schema_fingerprint = fingerprint;
    [scores, accounts, profiles]
}

fn test_item_status_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "item_status".to_string(),
        relation_name: "item_status".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "key".to_string(),
                name: "key".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "flag".to_string(),
                name: "flag".to_string(),
                logical_type: VelorixLogicalTypeV1::Bool,
                physical_arrow_type: ArrowPhysicalTypeV1::Boolean,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "ts".to_string(),
                name: "ts".to_string(),
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
        primary_key_column_ids: vec!["key".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: Some("ts".to_string()),
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema)
        .expect("item status catalog should fingerprint");
    VelorixRelationCatalogV1 {
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "item_status".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "item_status".to_string(),
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
            "catalog": test_scores_catalog()
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
            "catalog": test_scores_catalog()
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
            "catalog": test_scores_catalog()
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

#[tokio::test]
async fn rest_three_level_filter_aggregate_topk_chain_exact() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "api-test-chain-owner-a", false).await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog()
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let filter_view = CreateViewRequest {
        view_id: "filtered_scores".to_string(),
        url_path: Some("/scores/filtered".to_string()),
        output_relation_id: None,
        input_relation_id: "scores".to_string(),
        input_relation_version: "2026-05-24.v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: "select user_id, score from scores where score > 0".to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("positive scores filter".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
        correction_horizon_ns: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(filter_view)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");

    let ingest_one = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-chain-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1},
                {"user_id": "alice", "score": 7, "delta": 1},
                {"user_id": "bob", "score": 5, "delta": 1},
                {"user_id": "carol", "score": -3, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest_one.0, StatusCode::CREATED, "{ingest_one:?}");

    let filter_query = call_json(
        &router,
        Method::POST,
        "/v1/views/filtered_scores/query",
        json!({}),
    )
    .await;
    assert_eq!(filter_query.0, StatusCode::OK, "{filter_query:?}");
    assert_eq!(
        filter_query.1["rows"],
        json!([
            {"user_id": "alice", "score": 10},
            {"user_id": "alice", "score": 7},
            {"user_id": "bob", "score": 5}
        ])
    );

    let aggregate_view = CreateViewRequest {
        view_id: "score_totals".to_string(),
        url_path: Some("/scores/totals".to_string()),
        output_relation_id: None,
        input_relation_id: "filtered_scores".to_string(),
        input_relation_version: "v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: "select user_id, sum(score) as total from filtered_scores group by user_id"
            .to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("score totals over filtered scores".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
        correction_horizon_ns: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(aggregate_view)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true, "{view_response:?}");
    let backfill = call_json(
        &router,
        Method::POST,
        "/v1/views/score_totals/backfill",
        json!({}),
    )
    .await;
    assert_eq!(backfill.0, StatusCode::OK, "{backfill:?}");

    let totals_query = call_json(
        &router,
        Method::POST,
        "/v1/views/score_totals/query",
        json!({}),
    )
    .await;
    assert_eq!(totals_query.0, StatusCode::OK, "{totals_query:?}");
    assert_eq!(
        totals_query.1["rows"],
        json!([
            {"user_id": "alice", "total": 17},
            {"user_id": "bob", "total": 5}
        ])
    );

    let topk_view = CreateViewRequest {
        view_id: "top_scores".to_string(),
        url_path: Some("/scores/top".to_string()),
        output_relation_id: None,
        input_relation_id: "score_totals".to_string(),
        input_relation_version: "v1".to_string(),
        input_relation_refs: Vec::new(),
        input_relations: Vec::new(),
        sql: "select user_id, total from score_totals order by total desc limit 2".to_string(),
        source_kind: SqlSourceKind::StandingView,
        output_relation_ids: Vec::new(),
        sql_template: None,
        description: Some("top two score totals".to_string()),
        request: Vec::new(),
        response_schema: None,
        response_formats: vec!["json".to_string()],
        query_policy_id: None,
        correction_horizon_ns: None,
    };
    let view_response = call_json(&router, Method::POST, "/v1/views", json!(topk_view)).await;
    assert_eq!(view_response.0, StatusCode::CREATED, "{view_response:?}");
    assert_eq!(view_response.1["query_enabled"], true, "{view_response:?}");
    let backfill = call_json(
        &router,
        Method::POST,
        "/v1/views/top_scores/backfill",
        json!({}),
    )
    .await;
    assert_eq!(backfill.0, StatusCode::OK, "{backfill:?}");

    let top_query = call_json(
        &router,
        Method::POST,
        "/v1/views/top_scores/query",
        json!({}),
    )
    .await;
    assert_eq!(top_query.0, StatusCode::OK, "{top_query:?}");
    assert_eq!(
        top_query.1["rows"],
        json!([
            {"user_id": "alice", "total": 17},
            {"user_id": "bob", "total": 5}
        ])
    );

    let ingest_two = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-chain-stream",
            "partition_id": 0,
            "start_offset_inclusive": 4,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": -1},
                {"user_id": "carol", "score": 8, "delta": 1},
                {"user_id": "dave", "score": 3, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest_two.0, StatusCode::CREATED, "{ingest_two:?}");

    let filter_query = call_json(
        &router,
        Method::POST,
        "/v1/views/filtered_scores/query",
        json!({}),
    )
    .await;
    assert_eq!(filter_query.0, StatusCode::OK, "{filter_query:?}");
    assert_eq!(
        filter_query.1["rows"],
        json!([
            {"user_id": "alice", "score": 7},
            {"user_id": "bob", "score": 5},
            {"user_id": "carol", "score": 8},
            {"user_id": "dave", "score": 3}
        ])
    );

    let totals_query = call_json(
        &router,
        Method::POST,
        "/v1/views/score_totals/query",
        json!({}),
    )
    .await;
    assert_eq!(totals_query.0, StatusCode::OK, "{totals_query:?}");
    assert_eq!(
        totals_query.1["rows"],
        json!([
            {"user_id": "alice", "total": 7},
            {"user_id": "bob", "total": 5},
            {"user_id": "carol", "total": 8},
            {"user_id": "dave", "total": 3}
        ])
    );

    let top_query = call_json(
        &router,
        Method::POST,
        "/v1/views/top_scores/query",
        json!({}),
    )
    .await;
    assert_eq!(top_query.0, StatusCode::OK, "{top_query:?}");
    assert_eq!(
        top_query.1["rows"],
        json!([
            {"user_id": "alice", "total": 7},
            {"user_id": "carol", "total": 8}
        ])
    );
    let top_sql_query = call_json(
        &router,
        Method::GET,
        "/v1/views/top_scores/query?sql=select%20user_id%2C%20total%20from%20top_scores%20order%20by%20total%20desc",
        Value::Null,
    )
    .await;
    assert_eq!(top_sql_query.0, StatusCode::OK, "{top_sql_query:?}");
    assert_eq!(
        top_sql_query.1["rows"],
        json!([
            {"user_id": "carol", "total": 8},
            {"user_id": "alice", "total": 7}
        ])
    );

    // Crash window: append a committed ingest that is never applied before the
    // restart; restart restore must replay it and the drain must propagate it
    // through the whole chain.
    append_admitted_ingest_without_runtime_apply(
        store.clone(),
        IngestRowsRequest {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            stream_id: "scores-chain-stream".to_string(),
            partition_id: 0,
            start_offset_inclusive: 7,
            event_time_watermark: None,
            rows: vec![
                json!({"user_id": "bob", "score": 5, "delta": -1}),
                json!({"user_id": "dave", "score": 3, "delta": -1}),
                json!({"user_id": "alice", "score": 7, "delta": -1}),
                json!({"user_id": "alice", "score": 6, "delta": 1}),
            ],
        },
    )
    .await;

    let restarted_state = restart_test_api_state_with_store(
        restart_meta.clone(),
        restart_owner_id.clone(),
        store.clone(),
        "api-test-chain-owner-b",
        true,
    )
    .await;
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 3, "expected all three chain views restored");
    let restarted_app = app(restarted_state);

    let filter_query = call_json(
        &restarted_app,
        Method::POST,
        "/v1/views/filtered_scores/query",
        json!({}),
    )
    .await;
    assert_eq!(filter_query.0, StatusCode::OK, "{filter_query:?}");
    assert_eq!(
        filter_query.1["rows"],
        json!([
            {"user_id": "alice", "score": 6},
            {"user_id": "carol", "score": 8}
        ])
    );

    let totals_query = call_json(
        &restarted_app,
        Method::POST,
        "/v1/views/score_totals/query",
        json!({}),
    )
    .await;
    assert_eq!(totals_query.0, StatusCode::OK, "{totals_query:?}");
    assert_eq!(
        totals_query.1["rows"],
        json!([
            {"user_id": "alice", "total": 6},
            {"user_id": "carol", "total": 8}
        ])
    );

    let top_query = call_json(
        &restarted_app,
        Method::POST,
        "/v1/views/top_scores/query",
        json!({}),
    )
    .await;
    assert_eq!(top_query.0, StatusCode::OK, "{top_query:?}");
    assert_eq!(
        top_query.1["rows"],
        json!([
            {"user_id": "alice", "total": 6},
            {"user_id": "carol", "total": 8}
        ])
    );

    // Live ingest after restart flows through the restored chain.
    let ingest_three = call_json(
        &restarted_app,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-chain-stream",
            "partition_id": 0,
            "start_offset_inclusive": 11,
            "rows": [
                {"user_id": "eve", "score": 12, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest_three.0, StatusCode::CREATED, "{ingest_three:?}");

    let top_query = call_json(
        &restarted_app,
        Method::POST,
        "/v1/views/top_scores/query",
        json!({}),
    )
    .await;
    assert_eq!(top_query.0, StatusCode::OK, "{top_query:?}");
    assert_eq!(
        top_query.1["rows"],
        json!([
            {"user_id": "carol", "total": 8},
            {"user_id": "eve", "total": 12}
        ])
    );
}

#[tokio::test]
async fn view_on_view_admission_rejects_cycles_missing_producers_and_ambiguity() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-chain-reject", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog()
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    // Missing producer: no view publishes `ghost_output`.
    let missing = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "ghost_consumer",
            "sql": "select user_id, score from ghost_output where score > 0",
            "input_relation_id": "ghost_output",
            "input_relation_version": "v1",
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(missing.0, StatusCode::BAD_REQUEST, "{missing:?}");
    assert!(
        missing.1["error"]
            .as_str()
            .unwrap_or_default()
            .contains("not a registered relation"),
        "{missing:?}"
    );

    // A producer over an empty source gets a real empty checkpoint on demand.
    let no_checkpoint_producer = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "never_materialized",
            "sql": "select user_id, score from scores where score > 0",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(
        no_checkpoint_producer.0,
        StatusCode::CREATED,
        "{no_checkpoint_producer:?}"
    );

    let premature_consumer = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "premature_consumer",
            "sql": "select user_id, score from never_materialized",
            "input_relation_id": "never_materialized",
            "input_relation_version": "v1",
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(
        premature_consumer.0,
        StatusCode::CREATED,
        "{premature_consumer:?}"
    );
    let empty_consumer = call_json(
        &router,
        Method::POST,
        "/v1/views/premature_consumer/query",
        json!({}),
    )
    .await;
    assert_eq!(empty_consumer.0, StatusCode::OK, "{empty_consumer:?}");
    assert_eq!(empty_consumer.1["rows"], json!([]));

    // Ambiguity: a physical relation with the same id as a published view
    // output makes the consumer input unresolvable.
    let mut competing_catalog = test_scores_catalog();
    competing_catalog.relation_schema.relation_id = "never_materialized".to_string();
    competing_catalog.relation_schema.relation_name = "never_materialized".to_string();
    competing_catalog.relation_schema.relation_version = "v1".to_string();
    competing_catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&competing_catalog.relation_schema).unwrap();
    competing_catalog.incremental_relation.relation_id = "never_materialized".to_string();
    competing_catalog.incremental_relation.schema_fingerprint =
        competing_catalog.schema_fingerprint.clone();
    let competing = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": competing_catalog
        }),
    )
    .await;
    assert_eq!(competing.0, StatusCode::CREATED, "{competing:?}");

    let ambiguous = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "ambiguous_consumer",
            "sql": "select user_id, score from never_materialized",
            "input_relation_id": "never_materialized",
            "input_relation_version": "v1",
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(ambiguous.0, StatusCode::BAD_REQUEST, "{ambiguous:?}");
    assert!(
        ambiguous.1["error"]
            .as_str()
            .unwrap_or_default()
            .contains("ambiguous"),
        "{ambiguous:?}"
    );
}

#[tokio::test]
async fn view_on_view_chain_rejects_cycles_at_admission() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store, "api-test-chain-cycle", false).await;
    let router = app(state);

    let relation_response = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({
            "catalog": test_scores_catalog()
        }),
    )
    .await;
    assert_eq!(relation_response.0, StatusCode::CREATED);

    let ingest = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores-cycle-stream",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {"user_id": "alice", "score": 10, "delta": 1}
            ]
        }),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "{ingest:?}");

    let view_a = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "view_a",
            "sql": "select user_id, score from scores where score > 0",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(view_a.0, StatusCode::CREATED, "{view_a:?}");

    // view_a requires backfill (source data exists); its published output is
    // only consumable once it is active with an authoritative checkpoint.
    let backfill = call_json(
        &router,
        Method::POST,
        "/v1/views/view_a/backfill",
        json!({}),
    )
    .await;
    assert_eq!(backfill.0, StatusCode::OK, "{backfill:?}");

    let view_b = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "view_b",
            "sql": "select user_id, score from view_a where score > 0",
            "input_relation_id": "view_a",
            "input_relation_version": "v1",
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(view_b.0, StatusCode::CREATED, "{view_b:?}");

    assert_eq!(view_b.1["query_enabled"], true, "{view_b:?}");
    let backfill = call_json(
        &router,
        Method::POST,
        "/v1/views/view_b/backfill",
        json!({}),
    )
    .await;
    assert_eq!(backfill.0, StatusCode::OK, "{backfill:?}");

    // Extending the chain with another layer is valid: view_a_again -> view_b
    // -> view_a -> scores stays acyclic.
    let view_a_again = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "view_a_again",
            "sql": "select user_id, score from view_b where score > 0",
            "input_relation_id": "view_b",
            "input_relation_version": "v1",
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert_eq!(view_a_again.0, StatusCode::CREATED, "{view_a_again:?}");
    assert_eq!(view_a_again.1["query_enabled"], true, "{view_a_again:?}");
    let backfill = call_json(
        &router,
        Method::POST,
        "/v1/views/view_a_again/backfill",
        json!({}),
    )
    .await;
    assert_eq!(backfill.0, StatusCode::OK, "{backfill:?}");

    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/view_a_again/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(query.1["rows"], json!([{"user_id": "alice", "score": 10}]));

    // A self-referencing view cannot be admitted: the input resolves neither
    // to a registered relation nor to an existing published view output, so
    // admission fails closed before any graph edge is created.
    let self_cycle = call_json(
        &router,
        Method::POST,
        "/v1/views",
        json!({
            "view_id": "self_cycle_view",
            "sql": "select user_id, score from self_cycle_view where score > 0",
            "input_relation_id": "self_cycle_view",
            "input_relation_version": "v1",
            "source_kind": "standing_view"
        }),
    )
    .await;
    assert!(
        self_cycle.0.is_client_error(),
        "self-referencing view admission must fail closed: {self_cycle:?}"
    );
}

#[test]
fn parse_bool_env_defaults_to_false_when_unset() {
    // Ensure the env var is not set for this test.
    std::env::remove_var("VELORIX_TEST_PARSE_BOOL_UNSET");
    let result = parse_bool_env("VELORIX_TEST_PARSE_BOOL_UNSET", false);
    assert!(!result.unwrap());
}

#[test]
fn parse_bool_env_defaults_to_true_when_unset() {
    std::env::remove_var("VELORIX_TEST_PARSE_BOOL_UNSET_DEFAULT_TRUE");
    let result = parse_bool_env("VELORIX_TEST_PARSE_BOOL_UNSET_DEFAULT_TRUE", true);
    assert!(result.unwrap());
}

#[test]
fn parse_bool_env_accepts_one_as_true() {
    std::env::set_var("VELORIX_TEST_PARSE_BOOL_ONE", "1");
    let result = parse_bool_env("VELORIX_TEST_PARSE_BOOL_ONE", false);
    assert!(result.unwrap());
    std::env::remove_var("VELORIX_TEST_PARSE_BOOL_ONE");
}

#[test]
fn parse_bool_env_accepts_true_literal() {
    std::env::set_var("VELORIX_TEST_PARSE_BOOL_TRUE", "true");
    let result = parse_bool_env("VELORIX_TEST_PARSE_BOOL_TRUE", false);
    assert!(result.unwrap());
    std::env::remove_var("VELORIX_TEST_PARSE_BOOL_TRUE");
}

#[test]
fn parse_bool_env_accepts_zero_as_false() {
    std::env::set_var("VELORIX_TEST_PARSE_BOOL_ZERO", "0");
    let result = parse_bool_env("VELORIX_TEST_PARSE_BOOL_ZERO", true);
    assert!(!result.unwrap());
    std::env::remove_var("VELORIX_TEST_PARSE_BOOL_ZERO");
}

#[test]
fn parse_bool_env_accepts_false_literal() {
    std::env::set_var("VELORIX_TEST_PARSE_BOOL_FALSE", "false");
    let result = parse_bool_env("VELORIX_TEST_PARSE_BOOL_FALSE", true);
    assert!(!result.unwrap());
    std::env::remove_var("VELORIX_TEST_PARSE_BOOL_FALSE");
}

#[test]
fn parse_bool_env_rejects_invalid_value() {
    std::env::set_var("VELORIX_TEST_PARSE_BOOL_INVALID", "yes");
    let result = parse_bool_env("VELORIX_TEST_PARSE_BOOL_INVALID", false);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("VELORIX_TEST_PARSE_BOOL_INVALID"),
        "error should name the env var: {err}"
    );
    assert!(
        err.contains("yes"),
        "error should show the invalid value: {err}"
    );
    std::env::remove_var("VELORIX_TEST_PARSE_BOOL_INVALID");
}

#[test]
fn experimental_advanced_view_features_default_false_wires_policy() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = rt.block_on(test_public_api_state_with_store(
        store,
        "api-test-default-owner",
        false,
    ));
    assert!(
        !state.experimental_advanced_view_features,
        "default must be false/fail-closed"
    );
    assert_eq!(
        state.public_view_feature_policy.analytic_windows,
        FeatureAdmissionModeV1::Experimental,
        "default policy must keep analytic windows experimental"
    );
}

#[test]
fn experimental_advanced_view_features_enabled_true_wires_policy() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = rt
        .block_on(test_public_api_state_with_store(
            store,
            "api-test-enabled-owner",
            false,
        ))
        .with_experimental_advanced_view_features(true);
    assert!(state.experimental_advanced_view_features);
    assert_eq!(
        state.public_view_feature_policy.analytic_windows,
        FeatureAdmissionModeV1::Enabled,
        "enabled flag must promote analytic windows to Enabled"
    );
}

#[test]
fn experimental_advanced_view_features_explicit_false_wires_policy() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = rt
        .block_on(test_public_api_state_with_store(
            store,
            "api-test-explicit-false-owner",
            false,
        ))
        .with_experimental_advanced_view_features(false);
    assert!(!state.experimental_advanced_view_features);
    assert_eq!(
        state.public_view_feature_policy.analytic_windows,
        FeatureAdmissionModeV1::Experimental,
        "explicit false must keep analytic windows experimental"
    );
}

#[test]
fn correction_configuration_defaults_and_binds_runtime_identity() {
    let catalog = test_purchases_event_time_catalog();
    let sql = "select user_id, window_start, window_end, sum(amount) as total, count(*) as count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    let request: CreateViewRequest =
        serde_json::from_value(json!({"view_id": "correctable", "sql": sql})).unwrap();
    assert_eq!(request.correction_horizon_ns, None);
    assert!(serde_json::to_value(&request)
        .unwrap()
        .get("correction_horizon_ns")
        .is_none());
    let aliased: CreateViewRequest = serde_json::from_value(
        json!({"view_id": "correctable", "sql": sql, "correctionHorizonNs": 120}),
    )
    .unwrap();
    assert_eq!(aliased.correction_horizon_ns, Some(120));
    let supported = validate_supported_tumbling_window_sql(sql, &catalog).unwrap();
    let spec = StandingViewSpec {
        view_id: request.view_id,
        sql: sql.to_string(),
        dialect: SqlDialect::VelorixSql,
        source_kind: SqlSourceKind::StandingView,
        input_relations: vec![catalog_input_relation_schema(&catalog).unwrap()],
        output_relations: vec![
            tumbling_window_output_schema("correctable", &catalog, &supported).unwrap(),
        ],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let binding = |horizon| {
        materialized_view_runtime_binding_for_spec(
            std::slice::from_ref(&catalog),
            &spec,
            &[],
            horizon,
        )
        .unwrap()
    };
    let default = binding(None);
    let legacy_identity = standing_program_identity_from_materialized_view_runtime(
        std::slice::from_ref(&catalog),
        &spec,
        &[],
    )
    .unwrap();
    assert_eq!(
        default.standing_program_identity.sql_hash,
        legacy_identity.sql_hash
    );
    assert_eq!(default, binding(Some(60_000_000_000)));
    assert_ne!(default.standing_program_identity, legacy_identity);
    let corrected = binding(Some(120));
    let other = binding(Some(121));
    assert_eq!(
        corrected.standing_program_identity.sql_hash,
        default.standing_program_identity.sql_hash
    );
    assert_ne!(
        default.logical_plan.as_ref().unwrap().plan_hash,
        corrected.logical_plan.as_ref().unwrap().plan_hash
    );
    assert_ne!(
        corrected.logical_plan.as_ref().unwrap().plan_hash,
        other.logical_plan.as_ref().unwrap().plan_hash
    );
    assert_ne!(
        stable_bytes_hash(&serde_json::to_vec(&default.standing_program_identity).unwrap()),
        stable_bytes_hash(&serde_json::to_vec(&corrected.standing_program_identity).unwrap())
    );
    assert_ne!(
        stable_bytes_hash(&serde_json::to_vec(&corrected.standing_program_identity).unwrap()),
        stable_bytes_hash(&serde_json::to_vec(&other.standing_program_identity).unwrap())
    );
    let restored: MaterializedViewRuntimeBinding =
        serde_json::from_value(serde_json::to_value(&corrected).unwrap()).unwrap();
    assert_eq!(restored, corrected);
    let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan } =
        &restored.logical_plan.unwrap().execution
    else {
        panic!("expected window plan")
    };
    assert_eq!(
        plan.late_row_policy,
        Some(LateRowPolicy::CorrectWithinHorizon { horizon_ns: 120 })
    );
}

#[tokio::test]
async fn rest_correction_configuration_rejects_invalid_horizons_and_shapes() {
    let state =
        test_api_state_with_store(Arc::new(InMemory::new()), "api-correction-admission", false)
            .await;
    let router = app(state.clone());
    let relation = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": test_purchases_event_time_catalog()}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "{relation:?}");
    let tumble = "select user_id, window_start, window_end, sum(amount) as total from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end";
    for (id, sql, horizon) in [
        ("zero", tumble, 0),
        ("negative", tumble, -1),
        (
            "plain",
            "select user_id, sum(amount) as total from purchases group by user_id",
            120,
        ),
        (
            "session",
            "select user_id, window_start, window_end, sum(amount) as total from session(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end",
            120,
        ),
        (
            "topk",
            "select user_id, window_start, window_end, sum(amount) as total from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end order by total desc limit 1",
            120,
        ),
    ] {
        let response = call_json(&router, Method::POST, "/v1/views", json!({"view_id": id, "input_relation_id": "purchases", "input_relation_version": "2026-05-24.v1", "sql": sql, "correction_horizon_ns": horizon})).await;
        assert_eq!(response.0, StatusCode::BAD_REQUEST, "{id}: {response:?}");
        assert!(
            response.1["error"]
                .as_str()
                .unwrap()
                .contains("correction_horizon_ns"),
            "{response:?}"
        );
        assert!(
            read_test_active_view(&state, id).await.is_err()
        );
    }
    for (id, sql) in [
        ("default_window", tumble),
        (
            "corrected_hop",
            "select user_id, window_start, window_end, sum(amount) as total from hop(purchases, event_time, interval '30 seconds', interval '60 seconds') group by user_id, window_start, window_end",
        ),
    ] {
        let mut request = json!({"view_id": id, "input_relation_id": "purchases", "input_relation_version": "2026-05-24.v1", "sql": sql});
        if id == "corrected_hop" {
            request["correctionHorizonNs"] = json!(120);
        }
        let response = call_json(&router, Method::POST, "/v1/views", request).await;
        assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
        let active = read_test_active_view(&state, id).await.unwrap();
        let VelorixLogicalViewExecutionV1::TumblingEventTimeAggregate { plan } =
            &active.runtime.unwrap().logical_plan.unwrap().execution
        else {
            panic!("expected window plan")
        };
        assert_eq!(
            plan.late_row_policy,
            Some(LateRowPolicy::CorrectWithinHorizon {
                horizon_ns: if id == "corrected_hop" {
                    120
                } else {
                    60_000_000_000
                }
            })
        );
    }
}

#[tokio::test]
async fn rest_correction_configuration_survives_activation_and_checkpoint_restart() {
    correction_restart_regression(false).await;
}

#[tokio::test]
async fn authoritative_correction_configuration_survives_activation_and_checkpoint_restart() {
    correction_restart_regression(true).await;
}

#[test]
fn authoritative_correction_checkpoint_restart_on_two_mib_stack() {
    const CHILD: &str = "VELORIX_CORRECTION_TWO_MIB_STACK_CHILD";
    if std::env::var_os(CHILD).is_some() {
        std::thread::Builder::new()
            .name("correction-checkpoint-two-mib".into())
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(correction_restart_regression(true));
            })
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    // Isolate a stack-overflow abort while exercising the existing correction assertions.
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tests::authoritative_correction_checkpoint_restart_on_two_mib_stack",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "2 MiB correction checkpoint child failed: {}\n{}\n{}",
        child.status,
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
}

#[tokio::test]
async fn rest_default_window_far_late_extrema_and_avg_load_only_affected_state() {
    far_late_window_regression(false, false).await;
}

#[tokio::test]
async fn authoritative_default_window_far_late_extrema_and_avg_load_only_affected_state() {
    far_late_window_regression(true, false).await;
}

#[tokio::test]
async fn rest_far_late_legacy_checkpoint_reconstructs_affected_window() {
    far_late_window_regression(false, true).await;
}

#[tokio::test]
async fn authoritative_far_late_legacy_checkpoint_reconstructs_affected_window() {
    far_late_window_regression(true, true).await;
}

async fn far_late_window_regression(authoritative: bool, migrate: bool) {
    let counts = ObjectStoreAccessCounts::default();
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let failing = Arc::new(ArmedPrefixFailingObjectStore::new(inner));
    let store: Arc<dyn ObjectStore> =
        Arc::new(CountingObjectStore::new(failing.clone(), counts.clone()));
    let meta = authoritative
        .then(|| Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default())));
    let authority_clock = Arc::new(std::sync::atomic::AtomicU64::new(100_000));
    let new_state = |reconstruct| {
        let store = store.clone();
        let meta = meta.clone();
        let authority_clock = authority_clock.clone();
        async move {
            match meta {
                Some(meta) => {
                    meta.inner
                        .set_partition_authority_clock_for_test(
                            authority_clock.fetch_add(100_000, Ordering::SeqCst),
                        )
                        .await;
                    let mut state =
                        test_authoritative_api_state_with_store(store, "far-late-owner", meta)
                            .await;
                    state.owner_id = "far-late-runtime-owner".to_string();
                    state
                }
                None => test_api_state_with_store(store, "far-late-owner", reconstruct).await,
            }
        }
    };
    let state = new_state(false).await;
    let restart_meta = Arc::clone(
        state
            .meta_store
            .as_ref()
            .expect("far-late restart fixture requires Meta"),
    );
    let restart_owner_id = state.owner_id.clone();
    let router = app(state.clone());
    let relation = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": test_purchases_event_time_catalog()}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "{relation:?}");
    let created = call_json(&router, Method::POST, "/v1/views", json!({
        "view_id": "cold_aggregates", "input_relation_id": "purchases", "input_relation_version": "2026-05-24.v1",
        "sql": "select user_id, window_start, window_end, sum(amount) as total, count(*) as count, min(amount) as minimum, max(amount) as maximum, avg(amount) as average from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end"
    })).await;
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    if authoritative {
        seed_legacy_eager_checkpoint_for_compatibility(&state, "cold_aggregates").await;
    }
    let batch = |offset, rows: Value, watermark| json!({"relation_id": "purchases", "relation_version": "2026-05-24.v1", "stream_id": "cold-stream", "partition_id": 0, "start_offset_inclusive": offset, "event_time_watermark": {"event_time_column_id": "event_time", "max_observed_event_time_ns": watermark, "watermark_ns": watermark}, "rows": rows});
    let first = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        batch(
            0,
            json!([
                {"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1},
                {"user_id": "alice", "amount": 4, "event_time": 20_000_000_000i64, "delta": 1},
                {"user_id": "bob", "amount": 20, "event_time": 10_000_000_000i64, "delta": 1}
            ]),
            60_000_000_000i64,
        ),
    )
    .await;
    assert_eq!(first.0, StatusCode::CREATED, "{first:?}");
    let advanced = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        batch(
            3,
            json!([
                {"user_id": "charlie", "amount": 3, "event_time": 10_000_000_000_000i64, "delta": 1}
            ]),
            10_080_000_000_000i64,
        ),
    )
    .await;
    assert_eq!(advanced.0, StatusCode::CREATED, "{advanced:?}");
    let restarted = if authoritative {
        new_state(true).await
    } else {
        restart_test_api_state_with_store(
            restart_meta.clone(),
            restart_owner_id.clone(),
            store.clone(),
            "far-late-restarted",
            true,
        )
        .await
    };
    assert_eq!(
        restarted
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let active = read_test_active_view(&restarted, "cold_aggregates")
        .await
        .unwrap();
    let identity = active.runtime.unwrap().standing_program_identity;
    let checkpoint =
        read_latest_standing_runtime_checkpoint(&restarted, &identity, "cold_aggregates")
            .await
            .unwrap()
            .unwrap();
    let body_key = &checkpoint.checkpoint.state_root.object_key;
    assert!(body_key.ends_with(".state-payload.arrow"));
    let body = store
        .get(&Path::from(body_key.clone()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(
        body.starts_with(&[255; 4]),
        "restart must use the Arrow state body"
    );
    let decoded =
        checkpoint_arrow::decode(&body, restarted.max_standing_runtime_state_payload_bytes)
            .unwrap();
    assert_eq!(Some(decoded.payload), checkpoint.checkpoint.state_payload);
    let payload: Value = velorix_core::arrow_state::from_state(
        &checkpoint
            .checkpoint
            .state_payload
            .as_ref()
            .unwrap()
            .payload,
    )
    .unwrap();
    assert!(
        payload["state"]["rows"].as_object().unwrap().is_empty(),
        "expired accumulators must not remain hot"
    );
    let references = payload["cold_state_refs"].as_object().unwrap();
    let alice_ref = references
        .iter()
        .find(|(key, _)| key.contains("alice"))
        .unwrap()
        .1;
    let alice_path = Path::from(alice_ref["state_root"]["object_key"].as_str().unwrap());
    let saved = store.get(&alice_path).await.unwrap().bytes().await.unwrap();
    assert!(alice_path.as_ref().ends_with(".arrow"));
    assert!(
        saved.starts_with(&[255; 4]),
        "retained correction state must also use Arrow"
    );
    let archived: EventTimeStateRef = serde_json::from_value(alice_ref.clone()).unwrap();
    checkpoint_arrow::decode_retained_state(
        &saved,
        &archived,
        restarted.max_standing_runtime_state_payload_bytes,
    )
    .unwrap();
    let router = app(restarted.clone());
    let late = batch(
        4,
        json!([{"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": -1}]),
        10_080_000_000_000i64,
    );
    let before_objects = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .len();
    store.delete(&alice_path).await.unwrap();
    let missing = call_json(&router, Method::POST, "/v1/ingest", late.clone()).await;
    assert_eq!(missing.0, StatusCode::SERVICE_UNAVAILABLE, "{missing:?}");
    assert!(missing.1["error"]
        .as_str()
        .unwrap()
        .contains("retained window state unavailable"));
    assert_eq!(
        store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        before_objects - 1,
        "cold load failure must precede source publication"
    );
    store
        .put(&alice_path, bytes::Bytes::from_static(b"corrupt").into())
        .await
        .unwrap();
    let corrupt = call_json(&router, Method::POST, "/v1/ingest", late.clone()).await;
    assert_eq!(corrupt.0, StatusCode::BAD_REQUEST, "{corrupt:?}");
    assert!(corrupt.1["error"].as_str().unwrap().contains("Arrow/ZSTD"));
    store.put(&alice_path, saved.into()).await.unwrap();
    if migrate {
        let mut old = checkpoint.checkpoint.clone();
        let mut legacy = payload.clone();
        for field in [
            "cold_state_version",
            "cold_state_refs",
            "legacy_replay_before_ns",
        ] {
            legacy.as_object_mut().unwrap().remove(field);
        }
        old.state_payload.as_mut().unwrap().payload =
            velorix_core::arrow_state::to_state(&legacy).unwrap();
        old.state_root.content_hash =
            stable_bytes_hash(old.state_payload.as_ref().unwrap().payload.as_bytes());
        let runtime =
            velorix_runtime::materialized_view_runtime::restore_standing_runtime(old).unwrap();
        insert_standing_runtime(&restarted, "cold_aggregates", runtime).unwrap();
    }
    counts.clear();
    let corrected = call_json(&router, Method::POST, "/v1/ingest", late.clone()).await;
    assert_eq!(corrected.0, StatusCode::CREATED, "{corrected:?}");
    let cold_reads = counts
        .get_paths()
        .into_iter()
        .filter(|path| path.contains("/windows/"))
        .collect::<Vec<_>>();
    assert_eq!(
        cold_reads,
        if migrate {
            Vec::new()
        } else {
            vec![alice_path.to_string()]
        },
        "only the affected accumulator should be loaded"
    );
    let objects = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .len();
    counts.clear();
    let duplicate = call_json(&router, Method::POST, "/v1/ingest", late).await;
    assert_eq!(duplicate.0, StatusCode::OK, "{duplicate:?}");
    assert_eq!(
        store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        objects
    );
    assert!(
        counts
            .get_paths()
            .iter()
            .all(|path| !path.contains("/windows/")),
        "idempotent retry should not reload cold state"
    );
    let final_state = if authoritative {
        new_state(true).await
    } else {
        restart_test_api_state_with_store(
            restart_meta.clone(),
            restart_owner_id.clone(),
            store.clone(),
            "far-late-final",
            true,
        )
        .await
    };
    assert_eq!(
        final_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let query = call_json(
        &app(final_state.clone()),
        Method::POST,
        "/v1/views/cold_aggregates/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    let alice = query.1["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["user_id"] == "alice")
        .unwrap();
    assert_eq!(alice["total"], 4);
    assert_eq!(alice["count"], 1);
    assert_eq!(alice["minimum"], 4);
    assert_eq!(alice["maximum"], 4);
    assert_eq!(alice["average"].as_f64(), Some(4.0));
    assert_eq!(query.1["rows"].as_array().unwrap().len(), 3);
    if !migrate {
        let committed =
            read_latest_standing_runtime_checkpoint(&final_state, &identity, "cold_aggregates")
                .await
                .unwrap()
                .unwrap();
        let old_bob_archive = references
            .iter()
            .find(|(key, _)| key.contains("bob"))
            .unwrap()
            .1["state_root"]["object_key"]
            .as_str()
            .unwrap();
        failing.arm("v1/state/materialized-view-runtime/");
        let failing_ingest = batch(
            5,
            json!([
                {"user_id": "bob", "amount": 5, "event_time": 20_000_000_000i64, "delta": 1}
            ]),
            10_080_000_000_000i64,
        );
        let failed = call_json(
            &app(final_state.clone()),
            Method::POST,
            "/v1/ingest",
            failing_ingest.clone(),
        )
        .await;
        assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR, "{failed:?}");
        let after_failure =
            read_latest_standing_runtime_checkpoint(&final_state, &identity, "cold_aggregates")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            after_failure.checkpoint_key, committed.checkpoint_key,
            "archive failure cannot publish a new checkpoint"
        );
        assert!(
            store.get(&Path::from(old_bob_archive)).await.is_ok(),
            "prior archive must survive failed replacement"
        );
        let source_count = store
            .list(Some(&Path::from("v1/ingest")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len();
        let retry = call_json(
            &app(final_state.clone()),
            Method::POST,
            "/v1/ingest",
            failing_ingest,
        )
        .await;
        assert_eq!(
            retry.0,
            StatusCode::OK,
            "same ingest must recover after transient archive write failure without restart: {retry:?}"
        );
        let retry_query = call_json(
            &app(final_state.clone()),
            Method::POST,
            "/v1/views/cold_aggregates/query",
            json!({}),
        )
        .await;
        assert_eq!(retry_query.0, StatusCode::OK, "{retry_query:?}");
        let bob = retry_query.1["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["user_id"] == "bob")
            .unwrap();
        assert_eq!(bob["total"], 25);
        assert_eq!(bob["count"], 2);
        let recovered = if authoritative {
            new_state(true).await
        } else {
            restart_test_api_state_with_store(
                restart_meta.clone(),
                restart_owner_id.clone(),
                store.clone(),
                "far-late-recovered",
                true,
            )
            .await
        };
        assert_eq!(
            recovered
                .restore_standing_program_runtimes_from_active_views()
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .list(Some(&Path::from("v1/ingest")))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            source_count,
            "recovery must not duplicate committed source objects"
        );
        let restored = call_json(
            &app(recovered),
            Method::POST,
            "/v1/views/cold_aggregates/query",
            json!({}),
        )
        .await;
        assert_eq!(restored.0, StatusCode::OK, "{restored:?}");
        let bob = restored.1["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["user_id"] == "bob")
            .unwrap();
        assert_eq!(bob["total"], 25);
        assert_eq!(bob["count"], 2);
        assert_eq!(bob["minimum"], 5);
        assert_eq!(bob["maximum"], 20);
        assert_eq!(bob["average"].as_f64(), Some(12.5));
    }
}

#[tokio::test]
async fn rest_default_session_accepts_late_input_after_closure() {
    let state = test_api_state().await;
    let router = app(state.clone());
    let relation = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": test_purchases_event_time_catalog()}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);
    let created = call_json(&router, Method::POST, "/v1/views", json!({"view_id": "late_session", "input_relation_id": "purchases", "input_relation_version": "2026-05-24.v1", "sql": "select user_id, window_start, window_end, sum(amount) as total from session(purchases, event_time, interval '30 seconds') group by user_id, window_start, window_end"})).await;
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    for (offset, amount) in [(0, 10), (1, 7)] {
        let ingested = call_json(&router, Method::POST, "/v1/ingest", json!({"relation_id": "purchases", "relation_version": "2026-05-24.v1", "stream_id": "session-stream", "partition_id": 0, "start_offset_inclusive": offset, "event_time_watermark": {"event_time_column_id": "event_time", "max_observed_event_time_ns": 1_000_000_000_000i64, "watermark_ns": 1_000_000_000_000i64}, "rows": [{"user_id": "alice", "amount": amount, "event_time": 10_000_000_000i64, "delta": 1}]})).await;
        assert_eq!(ingested.0, StatusCode::CREATED, "{ingested:?}");
    }
    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/late_session/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(query.1["rows"][0]["total"], 17);
}

#[tokio::test]
async fn rest_window_archival_drains_multiple_bounded_export_chunks() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "archive-chunks-owner", false).await;
    let router = app(state.clone());
    let relation = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": test_purchases_event_time_catalog()}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);
    let created = call_json(&router, Method::POST, "/v1/views", json!({"view_id": "archive_chunks", "input_relation_id": "purchases", "input_relation_version": "2026-05-24.v1", "sql": "select user_id, window_start, window_end, sum(amount) as total from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end"})).await;
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    let rows = (0..130).map(|key| json!({"user_id": format!("user-{key}"), "amount": key, "event_time": 10_000_000_000i64, "delta": 1})).collect::<Vec<_>>();
    let ingested = call_json(&router, Method::POST, "/v1/ingest", json!({"relation_id": "purchases", "relation_version": "2026-05-24.v1", "stream_id": "archive-chunks-stream", "partition_id": 0, "start_offset_inclusive": 0, "event_time_watermark": {"event_time_column_id": "event_time", "max_observed_event_time_ns": 10_080_000_000_000i64, "watermark_ns": 10_080_000_000_000i64}, "rows": rows})).await;
    assert_eq!(ingested.0, StatusCode::CREATED, "{ingested:?}");
    let active = read_test_active_view(&state, "archive_chunks")
        .await
        .unwrap();
    let identity = active.runtime.unwrap().standing_program_identity;
    let record = read_latest_standing_runtime_checkpoint(&state, &identity, "archive_chunks")
        .await
        .unwrap()
        .unwrap();
    let payload: Value =
        velorix_core::arrow_state::from_state(&record.checkpoint.state_payload.unwrap().payload)
            .unwrap();
    assert_eq!(payload["cold_state_refs"].as_object().unwrap().len(), 130);
    assert!(payload["state"]["rows"].as_object().unwrap().is_empty());
    let archived = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .filter(|object| object.location.as_ref().contains("/windows/"))
        .count();
    assert_eq!(
        archived, 130,
        "all three export chunks must be durable before checkpoint publication"
    );
    let runtime = state
        .standing_runtime(&identity, "archive_chunks")
        .unwrap()
        .unwrap();
    assert!(
        runtime
            .lock()
            .unwrap()
            .export_event_time_state()
            .unwrap()
            .is_empty(),
        "published archives must be acknowledged"
    );
}

async fn correction_restart_regression(authoritative: bool) {
    async fn state_for_mode(
        store: Arc<dyn ObjectStore>,
        owner: &str,
        reconstruct: bool,
        meta: Option<Arc<DurableCapabilityMetaStore>>,
        previous: Option<&ApiState>,
    ) -> ApiState {
        match meta {
            Some(meta) => {
                meta.inner
                    .set_partition_authority_clock_for_test(
                        u64::from(*owner.as_bytes().last().unwrap()) * 100_000,
                    )
                    .await;
                let mut state =
                    test_authoritative_api_state_with_store(store, "stable-correction-owner", meta)
                        .await;
                // Isolate correction reconstruction from lease expiry between process owners.
                state.owner_id = "correction-runtime-owner".to_string();
                state
            }
            None => {
                let mut state = test_api_state_with_store(store, owner, reconstruct).await;
                if let Some(previous) = previous {
                    state.meta_store = previous.meta_store.clone();
                    state.view_bootstrap_meta_store = previous.view_bootstrap_meta_store.clone();
                    state.owner_id = previous.owner_id.clone();
                }
                state
            }
        }
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta = authoritative
        .then(|| Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default())));
    let state = state_for_mode(store.clone(), "api-correction-a", false, meta.clone(), None).await;
    let router = app(state.clone());
    let relation = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": test_purchases_event_time_catalog()}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);
    let request = json!({"view_id": "correctable", "input_relation_id": "purchases", "input_relation_version": "2026-05-24.v1", "sql": "select user_id, window_start, window_end, sum(amount) as total, count(*) as count from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end", "correction_horizon_ns": 120_000_000_000i64});
    let created = call_json(&router, Method::POST, "/v1/views", request.clone()).await;
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    if authoritative {
        seed_legacy_eager_checkpoint_for_compatibility(&state, "correctable").await;
    }
    let active = read_test_active_view(&state, "correctable").await.unwrap();
    let binding = active.runtime.unwrap();
    let reconstructed = state_for_mode(
        store.clone(),
        "api-correction-b",
        true,
        meta.clone(),
        Some(&state),
    )
    .await;
    assert_eq!(
        reconstructed
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let restored_active = read_test_active_view(&reconstructed, "correctable")
        .await
        .unwrap();
    assert_eq!(restored_active.runtime.as_ref(), Some(&binding));
    let router = app(reconstructed.clone());
    let duplicate = call_json(&router, Method::POST, "/v1/views", request.clone()).await;
    assert_eq!(duplicate.0, StatusCode::OK, "{duplicate:?}");
    let mut different = request;
    different["correction_horizon_ns"] = json!(121_000_000_000i64);
    let conflict = call_json(&router, Method::POST, "/v1/views", different).await;
    assert_eq!(conflict.0, StatusCode::CONFLICT, "{conflict:?}");
    let batch = |offset, amount, time, delta, watermark| json!({"relation_id": "purchases", "relation_version": "2026-05-24.v1", "stream_id": "correction-stream", "partition_id": 0, "start_offset_inclusive": offset, "event_time_watermark": {"event_time_column_id": "event_time", "max_observed_event_time_ns": watermark, "watermark_ns": watermark}, "rows": [{"user_id": "alice", "amount": amount, "event_time": time, "delta": delta}]});
    let initial = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        batch(0, 10, 10_000_000_000i64, 1, 60_000_000_000i64),
    )
    .await;
    assert_eq!(initial.0, StatusCode::CREATED, "{initial:?}");
    let published = call_json(
        &router,
        Method::POST,
        "/v1/views/correctable/query",
        json!({}),
    )
    .await;
    assert_eq!(published.0, StatusCode::OK, "{published:?}");
    assert_eq!(
        published.1["rows"],
        json!([{"user_id": "alice", "window_start": 0, "window_end": 60_000_000_000i64, "total": 10, "count": 1}])
    );
    let correction = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        batch(1, 7, 20_000_000_000i64, 1, 60_000_000_000i64),
    )
    .await;
    assert_eq!(correction.0, StatusCode::CREATED, "{correction:?}");
    let corrected = call_json(
        &router,
        Method::POST,
        "/v1/views/correctable/query",
        json!({}),
    )
    .await;
    assert_eq!(corrected.1["rows"][0]["total"], 17);
    assert_eq!(corrected.1["rows"][0]["count"], 2);
    let restarted = state_for_mode(
        store.clone(),
        "api-correction-c",
        true,
        meta.clone(),
        Some(&reconstructed),
    )
    .await;
    assert_eq!(
        restarted
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let checkpoint = read_latest_standing_runtime_checkpoint(
        &restarted,
        &binding.standing_program_identity,
        "correctable",
    )
    .await
    .unwrap()
    .unwrap();
    assert!(checkpoint
        .checkpoint
        .validate_identity(&binding.standing_program_identity)
        .is_ok());
    let mut wrong_identity = binding.standing_program_identity.clone();
    wrong_identity
        .runtime_compatibility
        .push_str(":different correction horizon");
    assert!(checkpoint
        .checkpoint
        .validate_identity(&wrong_identity)
        .is_err());
    let router = app(restarted.clone());
    let restored_query = call_json(
        &router,
        Method::POST,
        "/v1/views/correctable/query",
        json!({}),
    )
    .await;
    assert_eq!(restored_query.1["rows"], corrected.1["rows"]);
    let retraction = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        batch(2, 10, 10_000_000_000i64, -1, 60_000_000_000i64),
    )
    .await;
    assert_eq!(retraction.0, StatusCode::CREATED, "{retraction:?}");
    let retracted = call_json(
        &router,
        Method::POST,
        "/v1/views/correctable/query",
        json!({}),
    )
    .await;
    assert_eq!(retracted.1["rows"][0]["total"], 7);
    assert_eq!(retracted.1["rows"][0]["count"], 1);
    let advance = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        batch(3, 5, 130_000_000_000i64, 1, 180_000_000_000i64),
    )
    .await;
    assert_eq!(advance.0, StatusCode::CREATED, "{advance:?}");
    let retained = call_json(
        &router,
        Method::POST,
        "/v1/views/correctable/query",
        json!({}),
    )
    .await;
    assert_eq!(retained.0, StatusCode::OK, "{retained:?}");
    assert_eq!(retained.1["rows"].as_array().unwrap().len(), 2);
    assert_eq!(retained.1["rows"][0]["total"], 7);
    let before = read_latest_standing_runtime_checkpoint(
        &restarted,
        &binding.standing_program_identity,
        "correctable",
    )
    .await
    .unwrap()
    .unwrap();
    let mut mixed_epoch = batch(4, 2, 20_000_000_000i64, 1, 180_000_000_000i64);
    mixed_epoch["rows"].as_array_mut().unwrap().insert(
        0,
        json!({
            "user_id": "alice", "amount": 3, "event_time": 140_000_000_000i64, "delta": 1,
        }),
    );
    let objects_before_rejection = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .len();
    let expired = call_json(&router, Method::POST, "/v1/ingest", mixed_epoch.clone()).await;
    assert_eq!(expired.0, StatusCode::CREATED, "{expired:?}");
    let objects_after_correction = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .len();
    assert!(objects_after_correction > objects_before_rejection);
    let retry = call_json(&router, Method::POST, "/v1/ingest", mixed_epoch).await;
    assert_eq!(retry.0, StatusCode::OK, "{retry:?}");
    assert_eq!(
        store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        objects_after_correction
    );
    let after = read_latest_standing_runtime_checkpoint(
        &restarted,
        &binding.standing_program_identity,
        "correctable",
    )
    .await
    .unwrap()
    .unwrap();
    assert!(after.checkpoint.logical_epoch > before.checkpoint.logical_epoch);
    assert_ne!(
        after.checkpoint.state_payload,
        before.checkpoint.state_payload
    );
    let mut lower_partition_watermark = batch(0, 3, 150_000_000_000i64, 1, 180_000_000_000i64);
    lower_partition_watermark["partition_id"] = json!(1);
    lower_partition_watermark["event_time_watermark"]["watermark_ns"] = json!(60_000_000_000i64);
    let regressed = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        lower_partition_watermark,
    )
    .await;
    assert_eq!(regressed.0, StatusCode::BAD_REQUEST, "{regressed:?}");
    assert!(
        regressed.1["error"]
            .as_str()
            .unwrap()
            .contains("window correction watermark cannot move backwards"),
        "{regressed:?}"
    );
    assert_eq!(
        store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        objects_after_correction
    );
    let accepted_retry = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        batch(0, 10, 10_000_000_000i64, 1, 60_000_000_000i64),
    )
    .await;
    assert_eq!(
        accepted_retry.0,
        StatusCode::OK,
        "accepted retry after expiry: {accepted_retry:?}"
    );
    let after_rejection = state_for_mode(
        store.clone(),
        "api-correction-d",
        true,
        meta.clone(),
        Some(&restarted),
    )
    .await;
    assert_eq!(
        after_rejection
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let router = app(after_rejection.clone());
    let mut replacement = batch(6, 4, 150_000_000_000i64, 1, 180_000_000_000i64);
    replacement["rows"].as_array_mut().unwrap().push(
        json!({"user_id": "alice", "amount": 6, "event_time": 160_000_000_000i64, "delta": 1}),
    );
    let valid = call_json(&router, Method::POST, "/v1/ingest", replacement).await;
    assert_eq!(
        valid.0,
        StatusCode::CREATED,
        "replacement at rejected start: {valid:?}"
    );
    let next = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        batch(8, 2, 170_000_000_000i64, 1, 180_000_000_000i64),
    )
    .await;
    assert_eq!(next.0, StatusCode::CREATED, "next valid ingest: {next:?}");
    let mut partition_replacement = batch(0, 3, 150_000_000_000i64, 1, 180_000_000_000i64);
    partition_replacement["partition_id"] = json!(1);
    let partition_valid =
        call_json(&router, Method::POST, "/v1/ingest", partition_replacement).await;
    assert_eq!(
        partition_valid.0,
        StatusCode::CREATED,
        "replacement at rejected partition offset zero: {partition_valid:?}"
    );
    let final_state = state_for_mode(
        store,
        "api-correction-e",
        true,
        meta,
        Some(&after_rejection),
    )
    .await;
    assert_eq!(
        final_state
            .restore_standing_program_runtimes_from_active_views()
            .await
            .unwrap(),
        1
    );
    let query = call_json(
        &app(final_state),
        Method::POST,
        "/v1/views/correctable/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([
            {"user_id": "alice", "window_start": 0, "window_end": 60_000_000_000i64, "total": 9, "count": 2},
            {"user_id": "alice", "window_start": 120_000_000_000i64, "window_end": 180_000_000_000i64, "total": 23, "count": 6},
        ])
    );
}

#[tokio::test]
async fn correction_source_commit_fences_concurrent_view_creation_and_backfill() {
    let hook = Arc::new(GuardRaceHook {
        reached: Notify::new(),
        release: Notify::new(),
        fire_once: AtomicBool::new(false),
        all_checkpoints: true,
    });
    let meta: Arc<dyn MetaStore> = Arc::new(
        DurableCapabilityMetaStore::new(InMemoryMetaStore::default()).with_guard_race(hook.clone()),
    );
    let state = test_authoritative_api_state_with_store(
        Arc::new(InMemory::new()),
        "correction-race-owner",
        meta,
    )
    .await;
    let router = app(state.clone());
    let relation = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": test_purchases_event_time_catalog()}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);
    let request = json!({"view_id": "correctable", "input_relation_id": "purchases", "input_relation_version": "2026-05-24.v1", "sql": "select user_id, window_start, window_end, sum(amount) as total from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end", "correction_horizon_ns": 120_000_000_000i64});
    let created = call_json(&router, Method::POST, "/v1/views", request.clone()).await;
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    seed_legacy_eager_checkpoint_for_compatibility(&state, "correctable").await;
    hook.fire_once.store(true, Ordering::SeqCst);
    let ingest_router = router.clone();
    let ingest = tokio::spawn(async move {
        call_json(&ingest_router, Method::POST, "/v1/ingest", json!({"relation_id": "purchases", "relation_version": "2026-05-24.v1", "stream_id": "correction-race-stream", "partition_id": 0, "start_offset_inclusive": 0, "event_time_watermark": {"event_time_column_id": "event_time", "max_observed_event_time_ns": 60_000_000_000i64, "watermark_ns": 60_000_000_000i64}, "rows": [{"user_id": "alice", "amount": 10, "event_time": 10_000_000_000i64, "delta": 1}]})).await
    });
    tokio::time::timeout(Duration::from_secs(5), hook.reached.notified())
        .await
        .unwrap();
    let create_router = router.clone();
    let mut request = request;
    request["view_id"] = json!("created_during_ingest");
    let mut create =
        tokio::spawn(
            async move { call_json(&create_router, Method::POST, "/v1/views", request).await },
        );
    let backfill_router = router.clone();
    let mut backfill = tokio::spawn(async move {
        call_json(
            &backfill_router,
            Method::POST,
            "/v1/views/correctable/backfill",
            json!({}),
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut create)
            .await
            .is_err(),
        "view creation crossed an uncommitted source cut"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut backfill)
            .await
            .is_err(),
        "backfill activated before source/runtime commit completed"
    );
    hook.release.notify_one();
    let ingested = tokio::time::timeout(Duration::from_secs(5), ingest)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ingested.0, StatusCode::CREATED, "{ingested:?}");
    let created = tokio::time::timeout(Duration::from_secs(5), create)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    seed_legacy_eager_checkpoint_for_compatibility(&state, "correctable").await;
    hook.fire_once.store(true, Ordering::SeqCst);
    let backfilled = tokio::time::timeout(Duration::from_secs(5), backfill)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(backfilled.0, StatusCode::OK, "{backfilled:?}");
    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/correctable/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(query.1["rows"][0]["total"], 10);
}

#[tokio::test]
async fn deploying_correction_view_accepts_far_late_input_after_partial_backfill() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = test_api_state_with_store(store.clone(), "partial-correction-owner", false).await;
    let router = app(state.clone());
    let relation = call_json(
        &router,
        Method::POST,
        "/v1/relations",
        json!({"catalog": test_purchases_event_time_catalog()}),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);
    let batch = |offset, amount, time, watermark| json!({"relation_id": "purchases", "relation_version": "2026-05-24.v1", "stream_id": "partial-correction-stream", "partition_id": 0, "start_offset_inclusive": offset, "event_time_watermark": {"event_time_column_id": "event_time", "max_observed_event_time_ns": watermark, "watermark_ns": watermark}, "rows": [{"user_id": "alice", "amount": amount, "event_time": time, "delta": 1}]});
    for input in [
        batch(0, 10, 10_000_000_000i64, 180_000_000_000i64),
        batch(1, 5, 190_000_000_000i64, 240_000_000_000i64),
    ] {
        let ingested = call_json(&router, Method::POST, "/v1/ingest", input).await;
        assert_eq!(ingested.0, StatusCode::CREATED, "{ingested:?}");
    }
    let created = call_json(&router, Method::POST, "/v1/views", json!({"view_id": "partial_correctable", "input_relation_id": "purchases", "input_relation_version": "2026-05-24.v1", "sql": "select user_id, window_start, window_end, sum(amount) as total from tumble(purchases, event_time, interval '60 seconds') group by user_id, window_start, window_end", "correction_horizon_ns": 120_000_000_000i64})).await;
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    assert_eq!(created.1["query_enabled"], false);
    let initial = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        batch(2, 3, 250_000_000_000i64, 300_000_000_000i64),
    )
    .await;
    assert_eq!(
        initial.0,
        StatusCode::CREATED,
        "a deploying view without a checkpoint must admit source: {initial:?}"
    );
    let partial = call_json(
        &router,
        Method::POST,
        "/v1/views/partial_correctable/backfill",
        json!({"batch_limit": 1}),
    )
    .await;
    assert_eq!(partial.0, StatusCode::OK, "{partial:?}");
    assert_eq!(partial.1["query_enabled"], false);
    assert_eq!(partial.1["applied_batches"], 1);
    let before = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .len();
    let expired = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        batch(3, 2, 20_000_000_000i64, 300_000_000_000i64),
    )
    .await;
    assert_eq!(expired.0, StatusCode::CREATED, "{expired:?}");
    assert!(
        store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len()
            > before
    );
    let valid = call_json(
        &router,
        Method::POST,
        "/v1/ingest",
        batch(4, 4, 260_000_000_000i64, 300_000_000_000i64),
    )
    .await;
    assert_eq!(
        valid.0,
        StatusCode::CREATED,
        "valid replacement while deploying: {valid:?}"
    );
    let completed = call_json(
        &router,
        Method::POST,
        "/v1/views/partial_correctable/backfill",
        json!({}),
    )
    .await;
    assert_eq!(completed.0, StatusCode::OK, "{completed:?}");
    assert_eq!(completed.1["query_enabled"], true);
    let query = call_json(
        &router,
        Method::POST,
        "/v1/views/partial_correctable/query",
        json!({}),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "{query:?}");
    assert_eq!(
        query.1["rows"],
        json!([
            {"user_id": "alice", "window_start": 0, "window_end": 60_000_000_000i64, "total": 12},
            {"user_id": "alice", "window_start": 180_000_000_000i64, "window_end": 240_000_000_000i64, "total": 5},
            {"user_id": "alice", "window_start": 240_000_000_000i64, "window_end": 300_000_000_000i64, "total": 7},
        ])
    );
}
#[path = "generic_authoritative_stage_tests.rs"]
mod generic_authoritative_stage_tests;

#[path = "generic_derived_backfill_tests.rs"]
mod generic_derived_backfill_tests;

#[test]
fn authoritative_mode_requires_owner_and_metadata_without_legacy_fallback() {
    assert!(validate_relation_ingest_mode_config(false, None, None).is_ok());
    let missing_owner =
        validate_relation_ingest_mode_config(true, None, Some("http://127.0.0.1:50051"))
            .unwrap_err();
    assert!(missing_owner
        .to_string()
        .contains("VELORIX_RELATION_INGEST_OWNER_ID"));
    let missing_meta =
        validate_relation_ingest_mode_config(true, Some("stable-owner"), None).unwrap_err();
    assert!(missing_meta
        .to_string()
        .contains("VELORIX_META_GRPC_ENDPOINT"));
    assert!(
        validate_relation_ingest_mode_config(true, Some(" "), Some("http://127.0.0.1:50051"))
            .is_err()
    );
    assert!(validate_relation_ingest_mode_config(true, Some("stable-owner"), Some(" ")).is_err());
    assert!(validate_relation_ingest_mode_config(
        true,
        Some("stable-owner"),
        Some("http://127.0.0.1:50051")
    )
    .is_ok());
}

// Compatibility-only fixture: seed the persisted eager checkpoint that an
// upgraded deployment already owns. Demand-default fixtures never call this.
async fn seed_legacy_eager_checkpoint_for_compatibility(state: &ApiState, view_id: &str) {
    let mut legacy = state.clone();
    legacy.relation_ingest_mode = RelationIngestMode::Legacy;
    let active = read_test_active_view(&legacy, view_id).await.unwrap();
    bootstrap_legacy_view_if_empty(&legacy, &active)
        .await
        .unwrap();
    update_test_active_view_lifecycle(
        &legacy,
        view_id,
        &active.spec_hash,
        MaterializedViewLifecycleStatus::standing_runtime(),
    )
    .await
    .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let checkpoint = read_latest_standing_runtime_checkpoint(state, identity, view_id)
        .await
        .unwrap()
        .unwrap();
    assert!(checkpoint.demand_materialization.is_none());
}

fn test_output_commit_ref(checkpoint: &RuntimeCheckpoint) -> &str {
    let mut refs = checkpoint
        .output_manifest_refs
        .iter()
        .filter(|reference| reference.starts_with(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX));
    let reference = refs.next().expect("one authoritative output commit");
    assert!(refs.next().is_none(), "multiple authoritative commits");
    reference
}

#[tokio::test]
async fn explicit_http_scope_migrates_an_existing_eager_checkpoint() {
    let store = Arc::new(InMemory::new());
    let meta = Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    let state = test_authoritative_api_state_with_store(store, "http-migration-owner", meta)
        .await
        .with_experimental_advanced_view_features(true);
    let router = app(state.clone());
    for (path, body) in [
        ("/v1/relations", json!({"catalog":test_scores_catalog()})),
        (
            "/v1/views",
            json!({"view_id":"http_scope","input_relation_id":"scores",
            "input_relation_version":"2026-05-24.v1","sql":"SELECT user_id, score FROM scores",
            "response_formats":["json"]}),
        ),
    ] {
        let (status, body) = call_json(&router, Method::POST, path, body).await;
        assert!(status.is_success(), "{status}: {body}");
    }
    seed_legacy_eager_checkpoint_for_compatibility(&state, "http_scope").await;
    let (status, body) = call_json(
        &router,
        Method::POST,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version":"2026-05-24.v1","stream_id":"http-scope-stream","partition_id":0,
            "start_offset_inclusive":0,"rows":[
                {"user_id":"a","score":3,"delta":1},
                {"user_id":"b","score":7,"delta":1}
            ]
        }),
    )
    .await;
    assert!(status.is_success(), "{status}: {body}");
    let active = read_test_active_view(&state, "http_scope").await.unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let old = read_latest_standing_runtime_checkpoint(&state, identity, "http_scope")
        .await
        .unwrap()
        .unwrap();
    assert!(old.demand_materialization.is_none());
    // No internal materialization call precedes this public request.
    let (status, body) = call_json(
        &router,
        Method::POST,
        "/v1/views/http_scope/query",
        json!({
            "materialization":{"scope":{"where":"score >= 7"},"budget":{
                "batch_limit":4,"max_rows":10,"max_bytes":16777216,"max_duration_ms":10000}}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"], json!([{"user_id":"b","score":7}]));
    let migrated = read_latest_standing_runtime_checkpoint(&state, identity, "http_scope")
        .await
        .unwrap()
        .unwrap();
    assert!(migrated.demand_materialization.is_some());
    assert_eq!(
        migrated.checkpoint.input_frontiers, old.checkpoint.input_frontiers,
        "migration must preserve the already applied input prefix"
    );
}
