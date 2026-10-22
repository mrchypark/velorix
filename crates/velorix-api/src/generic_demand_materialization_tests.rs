use super::*;
use crate::demand_published::{
    capture_demand_published_artifacts, demand_published_artifact_key,
    load_demand_published_artifact,
};

#[test]
fn demand_recovery_preserves_lineage_without_claiming_completed_coverage() {
    let mut checkpoint = test_runtime_checkpoint(Vec::new());
    checkpoint.input_coverage = None;
    let key = test_checkpoint_key(&checkpoint);
    let mut record = test_checkpoint_record(&key, checkpoint);
    record.manifest_hash = "sha256:test-manifest".into();
    let legacy = standing_runtime_checkpoint_pointer_from_record(&record);
    validate_standing_runtime_checkpoint_record(
        &record.checkpoint.identity,
        &record.view_id,
        &legacy,
        &record,
    )
    .unwrap();
    assert_eq!(legacy.bootstrap_generation, 0);
    let mut predecessor_checkpoint = record.checkpoint.clone();
    predecessor_checkpoint.logical_epoch -= 1;
    let predecessor_key = test_checkpoint_key(&predecessor_checkpoint);
    let mut predecessor = test_checkpoint_pointer(&predecessor_key, &predecessor_checkpoint);
    predecessor.bootstrap_generation = 3;
    predecessor.plan_hash = format!("sha256:{}", "d".repeat(64));
    record.demand_materialization = Some(
        serde_json::from_value(json!({
            "schema_version":1,"spec_hash":"spec","plan_hash":predecessor.plan_hash,
            "output_schema_hash":"schema","paused":false,"order":"forward",
            "artifacts":{},"certificates":[],"full_complete":false
        }))
        .unwrap(),
    );
    record.previous_checkpoint = Some(predecessor.clone());
    let pointer = standing_runtime_checkpoint_pointer_from_record(&record);
    assert_eq!(pointer.bootstrap_generation, 3);
    assert_eq!(pointer.plan_hash, predecessor.plan_hash);
    assert!(pointer.input_coverage.is_none());
    assert!(pointer.coverage_hash.is_empty());
    validate_standing_runtime_checkpoint_record(
        &record.checkpoint.identity,
        &record.view_id,
        &pointer,
        &record,
    )
    .unwrap();
    record.demand_materialization.as_mut().unwrap().plan_hash =
        format!("sha256:{}", "e".repeat(64));
    let forged = standing_runtime_checkpoint_pointer_from_record(&record);
    let error = validate_standing_runtime_checkpoint_record(
        &record.checkpoint.identity,
        &record.view_id,
        &forged,
        &record,
    )
    .unwrap_err();
    assert!(error.message.contains("predecessor lineage"));
}

// Child of tests.rs. Module registration belongs to the integration owner.
fn demand_budget(rows: usize) -> Value {
    json!({"batch_limit":1,"max_rows":rows,"max_bytes":1048576,"max_duration_ms":10000})
}

fn demand_body(scope: Option<&str>, rows: usize) -> Value {
    let mut materialization = json!({"budget":demand_budget(rows)});
    if let Some(scope) = scope {
        materialization["scope"] = json!({"where":scope});
    }
    json!({"materialization":materialization})
}

async fn post(router: &Router, path: &str, body: Value) -> (StatusCode, Value) {
    call_json(router, Method::POST, path, body).await
}

fn successful(response: &(StatusCode, Value)) {
    assert!(response.0.is_success(), "{response:?}");
}

fn assert_pending(response: &(StatusCode, Value), row_budget: usize) {
    assert_eq!(response.0, StatusCode::ACCEPTED, "{response:?}");
    let progress = &response.1["materialization"];
    assert_eq!(progress["complete"], false, "{response:?}");
    assert!(progress["status"].is_string(), "{response:?}");
    assert!(
        progress["processed_rows"].as_u64().unwrap() <= row_budget as u64,
        "{response:?}"
    );
    assert!(
        progress["remaining_rows"].as_u64().unwrap() > 0,
        "{response:?}"
    );
    assert!(
        matches!(
            progress["dependency_mode"].as_str(),
            Some("scope" | "full_closure")
        ),
        "{response:?}"
    );
    assert!(
        response.1.get("rows").is_none(),
        "uncertified output escaped: {response:?}"
    );
}

#[tokio::test]
async fn demand_reuses_verified_publication_bytes_for_checkpoint_coverage() {
    let (state, router, store, _meta, counts) = fixture().await;
    ingest(&router, 0, json!([{"user_id":"a","score":7,"delta":1}])).await;
    declare(
        &router,
        "single_source_publication",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
    )
    .await;
    counts.clear();

    assert_eq!(
        demand(&router, "single_source_publication", None, 8).await,
        vec![json!({"user_id":"a","total":7})]
    );
    assert!(
        checkpoint(&state, "single_source_publication")
            .await
            .is_some(),
        "query returned rows without publishing a demand checkpoint; GETs={:?}",
        counts.get_paths()
    );

    let source_reads = counts
        .get_object_bytes()
        .into_iter()
        .filter(|(path, _)| path.starts_with("v1/ingest-staging/"))
        .collect::<Vec<_>>();
    assert_eq!(source_reads.len(), 1, "all GETs: {:?}", counts.get_paths());
    let source_key = ObjectPath::from(source_reads[0].0.clone());
    let source_size = store.head(&source_key).await.unwrap().size;
    assert_eq!(source_reads[0].1, source_size, "source bytes delivered");

    let active = state
        .view_registry()
        .unwrap()
        .read_active("single_source_publication")
        .await
        .unwrap();
    let (_, artifacts) = demand_source::capture_demand_source_artifacts(&state, &active)
        .await
        .unwrap();
    let artifact = artifacts.first().expect("one immutable source artifact");
    let raw = store
        .get(&ObjectPath::from(artifact.publication.object_key.clone()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let (_, _, proof) = demand_source::load_demand_source_artifact(&state, artifact, 1024 * 1024)
        .await
        .unwrap();
    assert!(proof.matches(
        &artifact.relation,
        &artifact.stream_id,
        artifact.partition_id,
        &artifact.publication
    ));
    let mut tampered_ref = artifact.publication.clone();
    tampered_ref.object_digest = format!("sha256:{}", "0".repeat(64));
    assert!(!proof.matches(
        &artifact.relation,
        &artifact.stream_id,
        artifact.partition_id,
        &tampered_ref
    ));
    let mut corrupted = raw.to_vec();
    corrupted[0] ^= 0xff;
    assert!(recovery::validate_demand_source_payload(artifact, corrupted.into()).is_err());
}

async fn demand(router: &Router, view: &str, scope: Option<&str>, rows: usize) -> Vec<Value> {
    for _ in 0..64 {
        let response = post(
            router,
            &format!("/v1/views/{view}/query"),
            demand_body(scope, rows),
        )
        .await;
        if response.0 == StatusCode::OK {
            let mut result = response.1["rows"].as_array().unwrap().clone();
            result.sort_by_key(Value::to_string);
            return result;
        }
        assert_pending(&response, rows);
    }
    panic!("bounded demand did not complete for {view}");
}

async fn demand_with_batch_limit(
    router: &Router,
    view: &str,
    batch_limit: usize,
    rows: usize,
) -> Vec<Value> {
    for _ in 0..64 {
        let mut body = demand_body(None, rows);
        body["materialization"]["budget"]["batch_limit"] = json!(batch_limit);
        let response = post(router, &format!("/v1/views/{view}/query"), body).await;
        if let Some(processed) = response.1["materialization"]["processed_rows"].as_u64() {
            assert!(
                processed <= rows as u64,
                "request exceeded row budget: {response:?}"
            );
        }
        if response.0 == StatusCode::OK {
            let mut result = response.1["rows"].as_array().unwrap().clone();
            result.sort_by_key(Value::to_string);
            return result;
        }
        assert_pending(&response, rows);
    }
    panic!("bounded demand did not complete for {view}");
}

async fn output_manifest_for(
    store: &Arc<dyn ObjectStore>,
    record: &StandingRuntimeCheckpointRecord,
) -> StandingRuntimeOutputManifestRecord {
    let key = record
        .checkpoint
        .output_manifest_refs
        .iter()
        .find_map(|reference| reference.strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX))
        .expect("checkpoint has an output manifest");
    let bytes = store
        .get(&ObjectPath::from(key.to_owned()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn published_positive_pages_batch_once_and_keep_signed_boundary() {
    let (state, router, store, _meta, _counts) = fixture().await;
    let source_rows = (0..257)
        .map(|i| json!({"user_id":format!("page-{i:04}"),"score":i,"delta":1}))
        .collect::<Vec<_>>();
    ingest(&router, 0, json!(source_rows)).await;
    declare(
        &router,
        "paged_positive_producer",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, score FROM scores UNION ALL SELECT user_id, score FROM scores",
    )
    .await;
    let mut expected = demand(&router, "paged_positive_producer", None, 1024).await;
    assert_eq!(expected.len(), 514);
    let producer = checkpoint(&state, "paged_positive_producer").await.unwrap();
    let manifest = output_manifest_for(&store, &producer).await;
    assert!(
        manifest.pages.len() >= 2,
        "fixture must exercise multiple pages"
    );

    // Leave the positive baseline unconsumed, then append one later producer
    // commit. The first consumer request batches only baseline pages; the
    // signed commit remains its own apply/checkpoint boundary.
    declare(
        &router,
        "paged_positive_consumer",
        &[("paged_positive_producer", "v1")],
        "SELECT user_id, score FROM paged_positive_producer",
    )
    .await;
    ingest(
        &router,
        257,
        json!([{"user_id":"after-baseline","score":9000,"delta":1}]),
    )
    .await;
    expected = demand(&router, "paged_positive_producer", None, 1024).await;
    assert_eq!(expected.len(), 516);
    expected.sort_by_key(Value::to_string);

    assert_eq!(
        demand_with_batch_limit(&router, "paged_positive_consumer", 16, 1024).await,
        expected
    );
    let consumer = checkpoint(&state, "paged_positive_consumer").await.unwrap();
    assert_eq!(
        consumer.checkpoint.logical_epoch, 3,
        "one baseline group, one indivisible signed commit, and one completion certificate"
    );
}

#[tokio::test]
async fn published_positive_page_budget_resumes_without_duplicate_or_missing_rows() {
    let (state, router, store, _meta, _counts) = fixture().await;
    let source_rows = (0..257)
        .map(|i| json!({"user_id":format!("resume-{i:04}"),"score":i,"delta":1}))
        .collect::<Vec<_>>();
    ingest(&router, 0, json!(source_rows)).await;
    declare(
        &router,
        "paged_resume_producer",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, score FROM scores UNION ALL SELECT user_id, score FROM scores",
    )
    .await;
    let expected = demand(&router, "paged_resume_producer", None, 1024).await;
    assert_eq!(expected.len(), 514);
    let producer = checkpoint(&state, "paged_resume_producer").await.unwrap();
    let manifest = output_manifest_for(&store, &producer).await;
    assert!(
        manifest.pages.len() >= 2,
        "fixture must exercise multiple pages"
    );
    declare(
        &router,
        "paged_resume_consumer",
        &[("paged_resume_producer", "v1")],
        "SELECT user_id, score FROM paged_resume_producer",
    )
    .await;

    assert_eq!(
        demand_with_batch_limit(&router, "paged_resume_consumer", 16, 300).await,
        expected
    );
    let consumer = checkpoint(&state, "paged_resume_consumer").await.unwrap();
    assert_eq!(
        consumer.checkpoint.logical_epoch, 2,
        "partial requests must resume exact spans and coalesce each request's page group"
    );
}

#[tokio::test]
async fn published_page_byte_budget_commits_fitting_prefix_then_resumes() {
    let (state, router, store, _meta, _counts) = fixture().await;
    let source_rows = (0..257)
        .map(|i| json!({"user_id":format!("bytes-{i:04}"),"score":i,"delta":1}))
        .collect::<Vec<_>>();
    ingest(&router, 0, json!(source_rows)).await;
    declare(
        &router,
        "paged_bytes_producer",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, score FROM scores UNION ALL SELECT user_id, score FROM scores",
    )
    .await;
    let expected = demand(&router, "paged_bytes_producer", None, 1024).await;
    let producer = checkpoint(&state, "paged_bytes_producer").await.unwrap();
    let manifest = output_manifest_for(&store, &producer).await;
    assert!(
        manifest.pages.len() >= 2,
        "fixture must exercise multiple pages"
    );
    declare(
        &router,
        "paged_bytes_consumer",
        &[("paged_bytes_producer", "v1")],
        "SELECT user_id, score FROM paged_bytes_producer",
    )
    .await;

    let active = state
        .view_registry()
        .unwrap()
        .read_active("paged_bytes_consumer")
        .await
        .unwrap();
    let (previous, migration_bytes) =
        demand_materialization::migration_read_bytes_for_test(&state, &active, 16 * 1024 * 1024)
            .await
            .unwrap();
    let producer_active = state
        .view_registry()
        .unwrap()
        .read_active("paged_bytes_producer")
        .await
        .unwrap();
    let (_, dependency_migration_bytes) = demand_materialization::migration_read_bytes_for_test(
        &state,
        &producer_active,
        16 * 1024 * 1024,
    )
    .await
    .unwrap();
    assert!(
        dependency_migration_bytes > 0,
        "the dependency closure must account for the producer's existing checkpoint migration"
    );
    let inputs = if previous
        .as_ref()
        .is_some_and(|record| record.demand_materialization.is_none())
    {
        view_dependencies::persisted_consumer_bootstrap_inputs(&active).unwrap()
    } else {
        view_dependencies::resolve_consumer_bootstrap_inputs_locked(
            &state,
            &active,
            previous.as_ref(),
        )
        .await
        .unwrap()
    };
    let binding = published_relation_binding_for_active_view(&active)
        .unwrap()
        .unwrap();
    let (artifacts, inventory_bytes) = capture_demand_published_artifacts(
        &state,
        &inputs,
        16 * 1024 * 1024,
        previous.as_ref(),
        &binding,
    )
    .await
    .unwrap();
    let page_artifacts = artifacts
        .iter()
        .filter(|artifact| artifact.positive_baseline_page_group().unwrap().is_some())
        .collect::<Vec<_>>();
    assert!(
        page_artifacts.len() >= 2,
        "two authenticated page artifacts required"
    );
    // Use the exact loader's byte accounting. A page can require reading its
    // predecessor too, so manifest page sizes alone understate the first-page
    // budget and can make a supposed prefix test fail before committing any page.
    let (first_page_input, first_page_bytes) =
        load_demand_published_artifact(&state, page_artifacts[0], usize::MAX)
            .await
            .unwrap();
    let first_page_ordinals = first_page_input
        .batches
        .iter()
        .map(|batch| batch.num_rows() as u64)
        .sum::<u64>();
    let second_page_bytes = load_demand_published_artifact(&state, page_artifacts[1], usize::MAX)
        .await
        .unwrap()
        .1;
    let one_page_budget =
        dependency_migration_bytes + migration_bytes + inventory_bytes + first_page_bytes;
    assert!(
        one_page_budget
            < dependency_migration_bytes
                + migration_bytes
                + inventory_bytes
                + first_page_bytes
                + second_page_bytes,
        "fixture must leave insufficient budget for the second page: dependency_migration={dependency_migration_bytes}, target_migration={migration_bytes}, inventory={inventory_bytes}, first_authenticated_page={first_page_bytes}, second_authenticated_page={second_page_bytes}"
    );

    let mut tight = demand_body(None, 1024);
    tight["materialization"]["budget"]["batch_limit"] = json!(16);
    tight["materialization"]["budget"]["max_bytes"] = json!(one_page_budget);
    let partial = post(&router, "/v1/views/paged_bytes_consumer/query", tight).await;
    assert_eq!(
        partial.0,
        StatusCode::ACCEPTED,
        "{partial:?}; dependency_migration={dependency_migration_bytes}, target_migration={migration_bytes}, inventory={inventory_bytes}, first_page={first_page_bytes}, second_page={second_page_bytes}, budget={one_page_budget}"
    );
    assert_eq!(partial.1["materialization"]["complete"], false);
    assert_eq!(
        partial.1["materialization"]["processed_rows"],
        json!(first_page_ordinals),
        "the page that fit must be committed when only the next page exceeds max_bytes"
    );
    let partial_checkpoint = checkpoint(&state, "paged_bytes_consumer")
        .await
        .expect("the fitting page prefix must be checkpointed");
    let first_artifact_key = demand_published_artifact_key(page_artifacts[0]).unwrap();
    let applied = partial_checkpoint
        .demand_materialization
        .as_ref()
        .and_then(|progress| progress.artifacts.get(&first_artifact_key))
        .expect("checkpoint must retain progress for the fitting page");
    assert!(
        applied.applied.is_complete(),
        "the exact fitting page ordinal span must be durable before resume"
    );

    assert_eq!(
        demand_with_batch_limit(&router, "paged_bytes_consumer", 16, 1024).await,
        expected
    );
}

async fn declare(router: &Router, view: &str, inputs: &[(&str, &str)], sql: &str) {
    let refs = inputs
        .iter()
        .map(|(id, version)| json!({"relation_id":id,"relation_version":version}))
        .collect::<Vec<_>>();
    let response = post(
        router,
        "/v1/views",
        json!({"view_id":view,"input_relation_refs":refs,"sql":sql,"response_formats":["json"]}),
    )
    .await;
    assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
}

async fn ingest(router: &Router, offset: u64, rows: Value) {
    let response = post(
        router,
        "/v1/relations/scores/ingest",
        json!({
            "relation_version":"2026-05-24.v1","stream_id":"demand-stream","partition_id":0,
            "start_offset_inclusive":offset,"rows":rows
        }),
    )
    .await;
    assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
}

async fn fixture() -> (
    ApiState,
    Router,
    Arc<dyn ObjectStore>,
    Arc<dyn MetaStore>,
    ObjectStoreAccessCounts,
) {
    fixture_with_meta(Arc::new(DurableCapabilityMetaStore::new(
        InMemoryMetaStore::default(),
    )))
    .await
}

async fn fixture_with_meta(
    meta: Arc<dyn MetaStore>,
) -> (
    ApiState,
    Router,
    Arc<dyn ObjectStore>,
    Arc<dyn MetaStore>,
    ObjectStoreAccessCounts,
) {
    let counts = ObjectStoreAccessCounts::default();
    let store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        Arc::new(InMemory::new()),
        counts.clone(),
    ));
    let state =
        test_authoritative_api_state_with_store(store.clone(), "demand-owner", meta.clone())
            .await
            .with_experimental_advanced_view_features(true);
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
    let response = post(&router, "/v1/relations", json!({"catalog":catalog})).await;
    assert_eq!(response.0, StatusCode::CREATED, "{response:?}");
    (state, router, store, meta, counts)
}

// A continuation request carries only the page token. The scope that the
// first request certified must survive that hop, otherwise the continuation
// recomputes a different scope hash and the server rejects its own token.
#[tokio::test]
async fn demand_continuation_keeps_the_certified_scope() {
    let (_state, router, _store, _meta, _counts) = fixture().await;
    ingest(
        &router,
        0,
        json!((0..8)
            .map(|i| json!({"user_id":format!("k{i}"),"score":i,"delta":1}))
            .collect::<Vec<_>>()),
    )
    .await;
    declare(
        &router,
        "scoped_pages",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, score FROM scores",
    )
    .await;
    let scope = "user_id >= 'k2' AND user_id <= 'k6'";
    let mut first = demand_body(Some(scope), 64);
    first["max_rows"] = json!(2);
    let page = post(&router, "/v1/views/scoped_pages/query", first).await;
    assert_eq!(page.0, StatusCode::OK, "{page:?}");
    let token = page.1["next_page_token"]
        .as_str()
        .expect("fixture must cross a page boundary")
        .to_owned();
    let next = post(
        &router,
        "/v1/views/scoped_pages/query",
        json!({"page_token":token,"max_rows":2}),
    )
    .await;
    assert_eq!(next.0, StatusCode::OK, "{next:?}");
}

async fn checkpoint(state: &ApiState, view: &str) -> Option<StandingRuntimeCheckpointRecord> {
    let active = state
        .view_registry()
        .unwrap()
        .read_active(view)
        .await
        .unwrap();
    read_latest_standing_runtime_checkpoint(
        state,
        active_standing_runtime_identity(&active).unwrap(),
        view,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn demand_scopes_survive_restart_without_skipping_gaps_or_recomputing() {
    let backend = Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    backend
        .inner
        .set_partition_authority_clock_for_test(0)
        .await;
    let (state, router, store, meta, counts) = fixture_with_meta(backend.clone()).await;
    ingest(
        &router,
        0,
        json!([
            {"user_id":"a","score":1,"delta":1}, {"user_id":"b","score":2,"delta":1},
            {"user_id":"a","score":3,"delta":1}, {"user_id":"b","score":4,"delta":1},
            {"user_id":"a","score":5,"delta":1}, {"user_id":"b","score":6,"delta":1}
        ]),
    )
    .await;
    counts.clear();
    for view in ["requested_rows", "unrequested_rows"] {
        declare(
            &router,
            view,
            &[("scores", "2026-05-24.v1")],
            "SELECT user_id, score FROM scores",
        )
        .await;
        assert!(checkpoint(&state, view).await.is_none());
    }
    assert!(
        counts
            .get_paths()
            .iter()
            .all(|p| !p.starts_with("v1/ingest/")
                && !p.starts_with("v1/standing-runtime-output-pages/")),
        "declaration read payloads: {:?}",
        counts.get_paths()
    );
    let first = post(
        &router,
        "/v1/views/requested_rows/query",
        demand_body(Some("user_id = 'a'"), 1),
    )
    .await;
    assert_pending(&first, 1);
    let a = vec![
        json!({"user_id":"a","score":1}),
        json!({"user_id":"a","score":3}),
        json!({"user_id":"a","score":5}),
    ];
    assert_eq!(
        demand(&router, "requested_rows", Some("user_id = 'a'"), 1).await,
        a
    );
    assert!(checkpoint(&state, "unrequested_rows").await.is_none());
    let saved = checkpoint(&state, "requested_rows")
        .await
        .unwrap()
        .checkpoint_key;
    let owner = state.owner_id.clone();
    drop(router);
    drop(state);
    // A restarted publisher must wait for the previous process's fenced lease.
    backend
        .inner
        .set_partition_authority_clock_for_test(30_001)
        .await;
    let mut restored = test_authoritative_api_state_with_store(store, "demand-owner", meta)
        .await
        .with_experimental_advanced_view_features(true);
    restored.owner_id = owner;
    restored
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    let router = app(restored.clone());
    counts.clear();
    assert_eq!(
        demand(&router, "requested_rows", Some("user_id = 'a'"), 1).await,
        a
    );
    assert_eq!(
        checkpoint(&restored, "requested_rows")
            .await
            .unwrap()
            .checkpoint_key,
        saved
    );
    assert!(
        counts
            .get_paths()
            .iter()
            .all(|p| !p.starts_with("v1/ingest/")),
        "certified requery replayed source: {:?}",
        counts.get_paths()
    );
    assert!(checkpoint(&restored, "unrequested_rows").await.is_none());
    // A's scan must not advance a global cursor past B's unprocessed rows.
    assert_eq!(
        demand(&router, "requested_rows", Some("user_id = 'b'"), 1).await,
        vec![
            json!({"user_id":"b","score":2}),
            json!({"user_id":"b","score":4}),
            json!({"user_id":"b","score":6})
        ]
    );
    let mut all = a;
    all.extend([
        json!({"user_id":"b","score":2}),
        json!({"user_id":"b","score":4}),
        json!({"user_id":"b","score":6}),
    ]);
    all.sort_by_key(Value::to_string);
    assert_eq!(
        demand(
            &router,
            "requested_rows",
            Some("user_id >= 'a' AND user_id <= 'b'"),
            1
        )
        .await,
        all
    );
    assert!(
        demand(&router, "requested_rows", Some("user_id = 'missing'"), 1)
            .await
            .is_empty()
    );
    let empty_proof = checkpoint(&restored, "requested_rows")
        .await
        .unwrap()
        .checkpoint_key;
    assert!(
        demand(&router, "requested_rows", Some("user_id = 'missing'"), 1)
            .await
            .is_empty()
    );
    assert_eq!(
        checkpoint(&restored, "requested_rows")
            .await
            .unwrap()
            .checkpoint_key,
        empty_proof
    );

    // A page token pins the certified cut even after new input invalidates
    // current coverage. Fresh demand must see the append; old pages must not.
    assert_eq!(demand(&router, "requested_rows", None, 1).await, all);
    let mut page_request = demand_body(None, 64);
    page_request["max_rows"] = json!(2);
    let first_page = post(&router, "/v1/views/requested_rows/query", page_request).await;
    assert_eq!(first_page.0, StatusCode::OK, "{first_page:?}");
    let mut pinned = first_page.1["rows"].as_array().unwrap().clone();
    let mut token = first_page.1["next_page_token"].as_str().map(str::to_owned);
    assert!(token.is_some(), "fixture must cross a page boundary");
    ingest(&router, 6, json!([{"user_id":"b","score":7,"delta":1}])).await;
    for _ in 0..8 {
        let Some(current) = token.take() else { break };
        let page = post(
            &router,
            "/v1/views/requested_rows/query",
            json!({"page_token":current,"max_rows":2}),
        )
        .await;
        assert_eq!(page.0, StatusCode::OK, "{page:?}");
        pinned.extend(page.1["rows"].as_array().unwrap().iter().cloned());
        token = page.1["next_page_token"].as_str().map(str::to_owned);
    }
    assert!(token.is_none(), "pinned pagination failed to terminate");
    pinned.sort_by_key(Value::to_string);
    assert_eq!(pinned, all);
    all.push(json!({"user_id":"b","score":7}));
    all.sort_by_key(Value::to_string);
    assert_eq!(demand(&router, "requested_rows", None, 1).await, all);
}

#[tokio::test]
async fn paused_explicit_backfill_allows_query_demand_and_enforces_shared_budget() {
    let (state, router, _store, _meta, _counts) = fixture().await;
    ingest(
        &router,
        0,
        json!((0..8)
            .map(|i| json!({"user_id":format!("k{i}"),"score":i,"delta":1}))
            .collect::<Vec<_>>()),
    )
    .await;
    declare(
        &router,
        "ordered_work",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, score FROM scores",
    )
    .await;
    let conflict = post(
        &router,
        "/v1/views/ordered_work/backfill",
        json!({"action":"step","batch_limit":2,"budget":demand_budget(1)}),
    )
    .await;
    assert_eq!(conflict.0, StatusCode::BAD_REQUEST, "{conflict:?}");
    let step = post(&router, "/v1/views/ordered_work/backfill", json!({"action":"step","order":"reverse","scope":{"where":"user_id >= 'k4'"},"budget":demand_budget(1)})).await;
    successful(&step);
    // This single source artifact retains its eight ingestion ordinals. Inspect
    // durable progress before any query can compute the missing first scope.
    let reverse = checkpoint(&state, "ordered_work").await.unwrap();
    let progress = reverse.demand_materialization.as_ref().unwrap();
    assert_eq!(progress.artifacts.len(), 1);
    let artifact = progress.artifacts.values().next().unwrap();
    assert_eq!(artifact.applied.rows_total(), 8);
    assert_eq!(
        artifact.applied.as_spans(),
        vec![[7, 8]],
        "reverse with one row of work must apply the last ordinal, not the first"
    );
    assert!(!artifact.applied.contains(0));
    assert!(!progress.full_complete);
    let scope_b = "user_id >= 'k4'";
    let hash_b = demand_materialization::scope_hash(Some(scope_b)).unwrap();
    // Inspect all relevant ordinals, including A's rejected rows, before
    // certifying B. Every explicit request retains its one-row work budget.
    for inspected in 2..=8u64 {
        let step = post(&router, "/v1/views/ordered_work/backfill",
            json!({"action":"step","order":"reverse","scope":{"where":scope_b},"budget":demand_budget(1)})).await;
        successful(&step);
        let record = checkpoint(&state, "ordered_work").await.unwrap();
        let progress = record.demand_materialization.as_ref().unwrap();
        let artifact = progress.artifacts.values().next().unwrap();
        assert_eq!(
            artifact.inspected[&hash_b].as_spans(),
            vec![[8 - inspected, 8]],
            "reverse inspection must advance exactly one ordinal per request"
        );
        assert_eq!(artifact.applied.as_spans(), vec![[4.max(8 - inspected), 8]]);
        assert!(
            !artifact.applied.contains(0),
            "A must remain unmaterialized while proving B"
        );
        assert!(!progress.full_complete);
        if inspected == 8 {
            assert!(
                progress
                    .certificates
                    .iter()
                    .any(|certificate| certificate.scope_hash == hash_b),
                "B must have durable completion proof after its full inspection"
            );
            let hash_a = demand_materialization::scope_hash(Some("user_id < 'k4'")).unwrap();
            assert!(
                !progress
                    .certificates
                    .iter()
                    .any(|certificate| certificate.scope_hash == hash_a
                        || certificate.scope_sql.is_none()),
                "A/full coverage must remain pending"
            );
        }
    }
    let pause = post(
        &router,
        "/v1/views/ordered_work/backfill",
        json!({"action":"pause"}),
    )
    .await;
    successful(&pause);
    let paused = checkpoint(&state, "ordered_work").await.unwrap();
    assert!(paused.demand_materialization.as_ref().unwrap().paused);
    let blocked = post(
        &router,
        "/v1/views/ordered_work/backfill",
        json!({"action":"step","budget":demand_budget(1)}),
    )
    .await;
    assert_eq!(blocked.0, StatusCode::ACCEPTED, "{blocked:?}");
    assert_eq!(blocked.1["materialization"]["status"], "paused");
    assert_eq!(
        checkpoint(&state, "ordered_work")
            .await
            .unwrap()
            .checkpoint_key,
        paused.checkpoint_key,
        "paused step must not advance durable work"
    );
    assert_eq!(
        demand(&router, "ordered_work", Some("user_id = 'k0'"), 1).await,
        vec![json!({"user_id":"k0","score":0})]
    );
    assert!(
        checkpoint(&state, "ordered_work")
            .await
            .unwrap()
            .demand_materialization
            .unwrap()
            .paused,
        "foreground demand must not resume explicit backfill"
    );
    let resume = post(
        &router,
        "/v1/views/ordered_work/backfill",
        json!({"action":"resume","order":"forward","budget":demand_budget(1)}),
    )
    .await;
    successful(&resume);
    let resumed = checkpoint(&state, "ordered_work").await.unwrap();
    let progress = resumed.demand_materialization.as_ref().unwrap();
    assert!(!progress.paused);
    let applied_before = progress
        .artifacts
        .values()
        .map(|artifact| artifact.applied.processed_len())
        .sum::<u64>();
    // Full-scope inspection starts at ordinal 0, already applied by demand.
    let step = post(
        &router,
        "/v1/views/ordered_work/backfill",
        json!({"action":"step","budget":demand_budget(2)}),
    )
    .await;
    successful(&step);
    let advanced = checkpoint(&state, "ordered_work").await.unwrap();
    assert_eq!(
        advanced
            .demand_materialization
            .unwrap()
            .artifacts
            .values()
            .map(|artifact| artifact.applied.processed_len())
            .sum::<u64>(),
        applied_before + 1
    );
    let expected = (0..8)
        .map(|i| json!({"user_id":format!("k{i}"),"score":i}))
        .collect::<Vec<_>>();
    assert_eq!(demand(&router, "ordered_work", None, 1).await, expected);
}

#[tokio::test]
async fn scoped_join_lag_closure_repairs_derived_replacements_from_positive_appends() {
    let (_state, router, _store, _meta, _counts) = fixture().await;
    ingest(
        &router,
        0,
        json!([
            {"user_id":"a","score":10,"delta":1}, {"user_id":"b","score":20,"delta":1},
            {"user_id":"c","score":30,"delta":1}
        ]),
    )
    .await;
    declare(
        &router,
        "grouped",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
    )
    .await;
    declare(
        &router,
        "timed",
        &[("grouped", "v1")],
        "SELECT user_id, total, to_timestamp(total) AS ts FROM grouped",
    )
    .await;
    declare(
        &router,
        "lagged",
        &[("timed", "v1")],
        "SELECT user_id, total, LAG(total) OVER (ORDER BY ts) AS previous FROM timed",
    )
    .await;
    declare(&router, "joined", &[("grouped","v1"),("lagged","v1")], "SELECT g.user_id, g.total, l.previous FROM grouped g JOIN lagged l ON g.user_id = l.user_id").await;
    assert_eq!(
        demand(&router, "joined", Some("user_id = 'c'"), 1).await,
        vec![json!({"user_id":"c","total":30,"previous":20})]
    );
    // Positive source append moves A between B and C; C's LAG changes even
    // though the appended source row lies outside the requested output scope.
    ingest(&router, 3, json!([{"user_id":"a","score":15,"delta":1}])).await;
    let mut insufficient = post(
        &router,
        "/v1/views/joined/query",
        demand_body(Some("user_id = 'c'"), 1),
    )
    .await;
    for _ in 0..64 {
        if insufficient.0 != StatusCode::ACCEPTED {
            break;
        }
        assert_pending(&insufficient, 1);
        assert_eq!(
            insufficient.1["materialization"]["status"],
            "dependency_pending"
        );
        insufficient = post(
            &router,
            "/v1/views/joined/query",
            demand_body(Some("user_id = 'c'"), 1),
        )
        .await;
    }
    assert_eq!(
        insufficient.0,
        StatusCode::PAYLOAD_TOO_LARGE,
        "{insufficient:?}"
    );
    let atomic_rows = insufficient.1["details"]["required_min_rows"]
        .as_u64()
        .unwrap() as usize;
    assert_eq!(
        atomic_rows, 2,
        "replacement requires a retraction and insertion"
    );
    let mut lag_budget = None;
    for _ in 0..64 {
        let response = post(
            &router,
            "/v1/views/joined/query",
            demand_body(Some("user_id = 'c'"), atomic_rows),
        )
        .await;
        if response.0 == StatusCode::PAYLOAD_TOO_LARGE {
            assert_eq!(
                response.1["details"]["code"],
                "DEMAND_PUBLISHED_BUDGET_REQUIRED"
            );
            lag_budget = response.1["details"]["required_min_rows"].as_u64();
            break;
        }
        assert_pending(&response, atomic_rows);
    }
    assert_eq!(lag_budget, Some(6), "LAG replaces three rows atomically");
    let atomic_rows = lag_budget.unwrap() as usize;
    assert_eq!(
        demand(&router, "joined", Some("user_id = 'c'"), atomic_rows).await,
        vec![json!({"user_id":"c","total":30,"previous":25})]
    );
    let mut expected = vec![
        json!({"user_id":"a","total":25,"previous":20}),
        json!({"user_id":"b","total":20,"previous":null}),
        json!({"user_id":"c","total":30,"previous":25}),
    ];
    expected.sort_by_key(Value::to_string);
    assert_eq!(demand(&router, "joined", None, atomic_rows).await, expected);
}

#[tokio::test]
async fn empty_scope_cannot_certify_by_ignoring_scanned_byte_budget() {
    let (state, router, _store, _meta, counts) = fixture().await;
    ingest(
        &router,
        0,
        json!([
            {"user_id":"outside-a","score":11,"delta":1},
            {"user_id":"outside-b","score":13,"delta":1}
        ]),
    )
    .await;
    declare(
        &router,
        "byte_limited",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, score FROM scores",
    )
    .await;
    let mut body = demand_body(Some("user_id = 'absent'"), 64);
    body["materialization"]["budget"]["max_bytes"] = json!(1);
    counts.clear();
    let response = post(&router, "/v1/views/byte_limited/query", body).await;
    assert_eq!(response.0, StatusCode::PAYLOAD_TOO_LARGE, "{response:?}");
    assert_eq!(
        response.1["details"]["code"],
        "DEMAND_SOURCE_BUDGET_REQUIRED"
    );
    let required = response.1["details"]["required_min_bytes"]
        .as_u64()
        .unwrap();
    assert!(required > 1, "{response:?}");
    assert_eq!(response.1["details"]["budget_phase"], "serialized_read");
    assert_eq!(
        response.1["details"]["required_min_bytes_is_lower_bound"],
        true
    );
    assert_eq!(response.1["details"]["supplied_max_bytes"], 1);
    assert!(
        counts
            .get_paths()
            .iter()
            .all(|path| !path.starts_with("v1/ingest/")),
        "undersized verification budget fetched source payload: {:?}",
        counts.get_paths()
    );
    assert!(
        checkpoint(&state, "byte_limited").await.is_none(),
        "rejected artifact must not apply or certify a checkpoint"
    );
    let mut sufficient = demand_body(Some("user_id = 'absent'"), 64);
    sufficient["materialization"]["budget"]["max_bytes"] = json!(required);
    // This small IPC artifact fits its serialized size once shared backing
    // allocations are counted once. Independent decoded allocations exceeding
    // the limit are checked separately in demand_source's IPC budget tests.
    let completed = post(&router, "/v1/views/byte_limited/query", sufficient).await;
    assert_eq!(
        completed.0,
        StatusCode::OK,
        "reported minimum must allow this single artifact: {completed:?}"
    );
    assert_eq!(completed.1["rows"], json!([]));
    assert!(
        demand(&router, "byte_limited", Some("user_id = 'absent'"), 64)
            .await
            .is_empty()
    );
    let proof = checkpoint(&state, "byte_limited")
        .await
        .unwrap()
        .checkpoint_key;
    assert!(
        demand(&router, "byte_limited", Some("user_id = 'absent'"), 64)
            .await
            .is_empty()
    );
    assert_eq!(
        checkpoint(&state, "byte_limited")
            .await
            .unwrap()
            .checkpoint_key,
        proof
    );
}

// Deterministic scheduler contract, separate from HTTP demand assertions above.
// Holding the boundary models an in-flight atomic checkpoint; polling foreground
// once registers the waiter before releasing it. No timing sleeps are involved.
#[tokio::test]
async fn foreground_waiter_wins_next_checkpoint_boundary_over_explicit_work() {
    let scheduler = Arc::new(demand_materialization::DemandScheduler::default());
    let boundary = scheduler
        .explicit_turn()
        .expect("idle explicit work may start");
    let mut foreground = Box::pin(scheduler.query_turn());
    std::future::poll_fn(|cx| {
        assert!(
            std::future::Future::poll(foreground.as_mut(), cx).is_pending(),
            "foreground must not interrupt an atomic checkpoint"
        );
        std::task::Poll::Ready(())
    })
    .await;
    drop(boundary);
    assert!(
        scheduler.explicit_turn().is_none(),
        "explicit work must yield to a foreground waiter at the boundary"
    );
    let foreground = tokio::time::timeout(Duration::from_secs(1), foreground)
        .await
        .unwrap();
    assert!(scheduler.explicit_turn().is_none());
    drop(foreground);
    assert!(
        scheduler.explicit_turn().is_some(),
        "foreground completion must release scheduling priority"
    );

    let boundary = scheduler.explicit_turn().unwrap();
    let mut cancelled = Box::pin(scheduler.query_turn());
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(cancelled.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(cancelled);
    drop(boundary);
    assert!(
        scheduler.explicit_turn().is_some(),
        "cancelled foreground must not strand explicit work"
    );
}

#[tokio::test]
async fn cancelled_demand_before_pointer_cas_retries_exactly_once() {
    let gate = Arc::new(GuardRaceHook {
        reached: Notify::new(),
        release: Notify::new(),
        fire_once: AtomicBool::new(false),
        all_checkpoints: true,
    });
    let meta: Arc<dyn MetaStore> = Arc::new(
        DurableCapabilityMetaStore::new(InMemoryMetaStore::default()).with_guard_race(gate.clone()),
    );
    let (state, router, _store, meta, _counts) = fixture_with_meta(meta).await;
    ingest(
        &router,
        0,
        json!([
            {"user_id":"a","score":3,"delta":1},
            {"user_id":"a","score":5,"delta":1}
        ]),
    )
    .await;
    declare(
        &router,
        "cancelled_total",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
    )
    .await;
    gate.fire_once.store(true, Ordering::SeqCst);
    let request_router = router.clone();
    let request = tokio::spawn(async move {
        post(
            &request_router,
            "/v1/views/cancelled_total/query",
            demand_body(None, 64),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), gate.reached.notified())
        .await
        .unwrap();
    let active = state
        .view_registry()
        .unwrap()
        .read_active("cancelled_total")
        .await
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap();
    let owner = meta
        .read_standing_runtime_owner(&identity.tenant_id, &identity.program_id, "cancelled_total")
        .await
        .unwrap()
        .unwrap();
    assert!(
        checkpoint(&state, "cancelled_total").await.is_none(),
        "speculative output/coverage must not escape before authoritative CAS"
    );
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert!(
        state
            .standing_runtime_operation_lock(identity, "cancelled_total")
            .unwrap()
            .try_lock_owned()
            .is_err(),
        "cancelled HTTP future released the atomic publication lock"
    );
    // A cancellation-safe unit may finish its CAS, or abort and restore. Either
    // outcome must serialize with this retry and apply each source row once.
    gate.release.notify_one();
    let expected = vec![json!({"user_id":"a","total":8})];
    let rows = tokio::time::timeout(
        Duration::from_secs(10),
        demand(&router, "cancelled_total", None, 64),
    )
    .await
    .unwrap();
    assert_eq!(rows, expected);
    let committed = checkpoint(&state, "cancelled_total").await.unwrap();
    let proof = committed.demand_materialization.as_ref().unwrap();
    assert!(proof.full_complete);
    assert_eq!(
        proof
            .artifacts
            .values()
            .map(|artifact| artifact.applied.processed_len())
            .sum::<u64>(),
        2
    );
    assert_eq!(demand(&router, "cancelled_total", None, 64).await, expected);
    assert_eq!(
        checkpoint(&state, "cancelled_total")
            .await
            .unwrap()
            .checkpoint_key,
        committed.checkpoint_key
    );
    let after = meta
        .read_standing_runtime_owner(&identity.tenant_id, &identity.program_id, "cancelled_total")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        standing_runtime_owner_token_from_claim(&after),
        standing_runtime_owner_token_from_claim(&owner),
        "retry must retain the fencing token rather than reacquire a new epoch"
    );
}

// Decorate the real admitted runtime, blocking only its synchronous apply entry.
// The timeout prevents a failed assertion from leaving a blocking worker alive.
struct ApplyBoundaryRuntime {
    inner: SharedStandingRuntime,
    identity: StandingProgramIdentity,
    entered: Arc<Notify>,
    release: Option<std::sync::mpsc::Receiver<()>>,
}

impl StandingProgramRuntime for ApplyBoundaryRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }
    fn input_schemas(&self) -> Vec<RelationSchema> {
        self.inner.lock().unwrap().input_schemas()
    }
    fn output_schemas(&self) -> Vec<RelationSchema> {
        self.inner.lock().unwrap().output_schemas()
    }
    fn logical_epoch(&self) -> u64 {
        self.inner.lock().unwrap().logical_epoch()
    }
    fn apply_changes(
        &mut self,
        epoch: u64,
        key: EpochIdempotencyKey,
        inputs: Vec<RelationInputBatch>,
    ) -> Result<velorix_core::standing_program::EpochCommit, StandingProgramRuntimeError> {
        if let Some(release) = self.release.take() {
            self.entered.notify_one();
            release
                .recv_timeout(Duration::from_secs(5))
                .map_err(|error| StandingProgramRuntimeError::ExternalRuntime {
                    reason: format!("test apply boundary: {error}"),
                })?;
        }
        self.inner.lock().unwrap().apply_changes(epoch, key, inputs)
    }
    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        self.inner
            .lock()
            .unwrap()
            .materialized_view_page(view, page)
    }
    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        self.inner.lock().unwrap().checkpoint()
    }
    fn restore(_checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError> {
        Err(StandingProgramRuntimeError::ExternalRuntime {
            reason: "test decorator restores through the real registered factory".into(),
        })
    }
}

#[tokio::test]
async fn cancelled_demand_during_blocking_apply_retains_lock_and_retries_once() {
    let (state, router, _store, _meta, _counts) = fixture().await;
    ingest(
        &router,
        0,
        json!([
            {"user_id":"a","score":3,"delta":1},
            {"user_id":"a","score":5,"delta":1}
        ]),
    )
    .await;
    declare(
        &router,
        "cancelled_apply",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
    )
    .await;
    let active = state
        .view_registry()
        .unwrap()
        .read_active("cancelled_apply")
        .await
        .unwrap();
    let identity = active_standing_runtime_identity(&active).unwrap().clone();
    ensure_standing_runtime_for_active_view(&state, &active)
        .await
        .unwrap();
    let inner = state
        .standing_runtime(&identity, "cancelled_apply")
        .unwrap()
        .unwrap();
    let entered = Arc::new(Notify::new());
    let (release, stopped) = std::sync::mpsc::channel();
    insert_standing_runtime(
        &state,
        "cancelled_apply",
        Box::new(ApplyBoundaryRuntime {
            inner,
            identity: identity.clone(),
            entered: entered.clone(),
            release: Some(stopped),
        }),
    )
    .unwrap();
    let request_router = router.clone();
    let request = tokio::spawn(async move {
        post(
            &request_router,
            "/v1/views/cancelled_apply/query",
            demand_body(None, 64),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    let operation = state
        .standing_runtime_operation_lock(&identity, "cancelled_apply")
        .unwrap();
    let retained = operation.try_lock_owned().is_err();
    // Release before asserting so failure on the original implementation does
    // not strand the real runtime's blocking worker.
    release.send(()).unwrap();
    assert!(
        retained,
        "HTTP cancellation released the lock while apply was still running"
    );
    let expected = vec![json!({"user_id":"a","total":8})];
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(10),
            demand(&router, "cancelled_apply", None, 64)
        )
        .await
        .unwrap(),
        expected
    );
    let record = checkpoint(&state, "cancelled_apply").await.unwrap();
    let proof = record.demand_materialization.as_ref().unwrap();
    assert!(proof.full_complete);
    assert_eq!(
        proof
            .artifacts
            .values()
            .map(|artifact| artifact.applied.processed_len())
            .sum::<u64>(),
        2
    );
    assert_eq!(demand(&router, "cancelled_apply", None, 64).await, expected);
    assert_eq!(
        checkpoint(&state, "cancelled_apply")
            .await
            .unwrap()
            .checkpoint_key,
        record.checkpoint_key
    );
}

#[tokio::test]
async fn late_bounded_producer_publishes_delta_from_last_complete_output() {
    let (state, router, store, meta, _counts) = fixture().await;
    ingest(&router, 0, json!([{"user_id":"a","score":3,"delta":1}])).await;
    declare(
        &router,
        "bounded_totals",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
    )
    .await;
    assert_eq!(
        demand(&router, "bounded_totals", None, 64).await,
        vec![json!({"user_id":"a","total":3})]
    );
    declare(
        &router,
        "totals_copy",
        &[("bounded_totals", "v1")],
        "SELECT user_id, total FROM bounded_totals",
    )
    .await;
    assert_eq!(
        demand(&router, "totals_copy", None, 64).await,
        vec![json!({"user_id":"a","total":3})]
    );
    let consumer = state
        .view_registry()
        .unwrap()
        .read_active("totals_copy")
        .await
        .unwrap();
    let baseline = checkpoint(&state, "totals_copy").await.unwrap();
    let inputs = consumer_direct_view_inputs(&consumer, &baseline).unwrap();
    assert_eq!(inputs.len(), 1);
    let input = &inputs[0];
    let old_producer = checkpoint(&state, "bounded_totals").await.unwrap();

    // Separate immutable source publications. Both change the same aggregate,
    // so publishing only the final unit's delta would leave total 3 resident
    // or attempt to retract total 8, which the consumer has never received.
    ingest(&router, 1, json!([{"user_id":"a","score":5,"delta":1}])).await;
    ingest(&router, 2, json!([{"user_id":"a","score":7,"delta":1}])).await;
    let first = post(
        &router,
        "/v1/views/bounded_totals/query",
        demand_body(None, 64),
    )
    .await;
    assert_pending(&first, 64);
    let partial = checkpoint(&state, "bounded_totals").await.unwrap();
    assert_ne!(partial.checkpoint_key, old_producer.checkpoint_key);
    assert!(
        !partial
            .demand_materialization
            .as_ref()
            .unwrap()
            .full_complete
    );
    assert!(
        read_validated_producer_commits_after_cursor(
            &state,
            &input.published_relation,
            &input.cursor,
        )
        .await
        .unwrap()
        .is_empty(),
        "partial state must not become a producer commit"
    );
    assert_eq!(
        checkpoint(&state, "totals_copy")
            .await
            .unwrap()
            .checkpoint_key,
        baseline.checkpoint_key
    );

    // Recover the partial unit before finishing Y, not only after publication.
    let owner = state.owner_id.clone();
    let partial_key = partial.checkpoint_key.clone();
    drop(router);
    drop(state);
    let mut state =
        test_authoritative_api_state_with_store(store.clone(), "demand-owner", meta.clone())
            .await
            .with_experimental_advanced_view_features(true);
    state.owner_id = owner;
    state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(
        checkpoint(&state, "bounded_totals")
            .await
            .unwrap()
            .checkpoint_key,
        partial_key,
        "restart must not finish or discard the partial producer unit"
    );
    assert_eq!(
        checkpoint(&state, "totals_copy")
            .await
            .unwrap()
            .checkpoint_key,
        baseline.checkpoint_key
    );
    let router = app(state.clone());
    let second = post(
        &router,
        "/v1/views/bounded_totals/query",
        demand_body(None, 64),
    )
    .await;
    assert_eq!(second.0, StatusCode::OK, "{second:?}");
    assert_eq!(second.1["rows"], json!([{"user_id":"a","total":15}]));
    let commits = read_validated_producer_commits_after_cursor(
        &state,
        &input.published_relation,
        &input.cursor,
    )
    .await
    .unwrap();
    assert_eq!(
        commits.len(),
        1,
        "one complete publication must span both bounded units"
    );

    // Restart before consumer replay forces recovery to use durable producer
    // commit history, not any in-memory intermediate output or pending delta.
    let owner = state.owner_id.clone();
    drop(router);
    drop(state);
    let mut restored = test_authoritative_api_state_with_store(store, "demand-owner", meta)
        .await
        .with_experimental_advanced_view_features(true);
    restored.owner_id = owner;
    restored
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    let router = app(restored.clone());
    let expected = vec![json!({"user_id":"a","total":15})];
    assert_eq!(demand(&router, "totals_copy", None, 64).await, expected);
    let completed = checkpoint(&restored, "totals_copy")
        .await
        .unwrap()
        .checkpoint_key;
    assert_eq!(demand(&router, "totals_copy", None, 64).await, expected);
    assert_eq!(
        checkpoint(&restored, "totals_copy")
            .await
            .unwrap()
            .checkpoint_key,
        completed
    );
}

#[tokio::test]
async fn reverse_derived_backfill_preserves_baseline_and_atomic_replacement_order() {
    let (state, router, _store, _meta, _counts) = fixture().await;
    ingest(&router, 0, json!([{"user_id":"a","score":3,"delta":1}])).await;
    declare(
        &router,
        "signed_producer",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
    )
    .await;
    assert_eq!(
        demand(&router, "signed_producer", None, 64).await,
        vec![json!({"user_id":"a","total":3})]
    );
    // Capture the insertion baseline but do not apply it to the consumer yet.
    declare(
        &router,
        "signed_consumer",
        &[("signed_producer", "v1")],
        "SELECT user_id, total FROM signed_producer",
    )
    .await;
    for (offset, amount, total) in [(1, 5, 8), (2, 7, 15)] {
        ingest(
            &router,
            offset,
            json!([{"user_id":"a","score":amount,"delta":1}]),
        )
        .await;
        assert_eq!(
            demand(&router, "signed_producer", None, 64).await,
            vec![json!({"user_id":"a","total":total})]
        );
    }
    assert!(checkpoint(&state, "signed_consumer").await.is_none());
    let step_body = |rows| json!({"action":"step","order":"reverse","budget":demand_budget(rows)});
    // Reverse scheduling cannot select -8,+15 before the baseline insertion.
    let baseline = post(&router, "/v1/views/signed_consumer/backfill", step_body(1)).await;
    successful(&baseline);
    let inserted = checkpoint(&state, "signed_consumer").await.unwrap();
    assert_eq!(
        inserted
            .demand_materialization
            .as_ref()
            .unwrap()
            .artifacts
            .values()
            .map(|artifact| artifact.applied.processed_len())
            .sum::<u64>(),
        1
    );

    // A signed replacement needs two physical delta rows. Report the minimum
    // actionable budget instead of yielding forever or applying half the pair.
    let small = post(&router, "/v1/views/signed_consumer/backfill", step_body(1)).await;
    assert_eq!(small.0, StatusCode::PAYLOAD_TOO_LARGE, "{small:?}");
    assert_eq!(
        small.1["details"]["required_min_rows"], 2,
        "signed replacement rejection must report the minimum usable row budget: {small:?}"
    );
    let after_small = checkpoint(&state, "signed_consumer").await.unwrap();
    assert_eq!(
        after_small
            .demand_materialization
            .as_ref()
            .unwrap()
            .artifacts
            .values()
            .map(|artifact| artifact.applied.processed_len())
            .sum::<u64>(),
        1,
        "one-row budget must not split a two-row replacement or exceed its budget"
    );
    for expected_applied in [3, 5] {
        let replacement = post(&router, "/v1/views/signed_consumer/backfill", step_body(2)).await;
        successful(&replacement);
        let record = checkpoint(&state, "signed_consumer").await.unwrap();
        assert_eq!(
            record
                .demand_materialization
                .as_ref()
                .unwrap()
                .artifacts
                .values()
                .map(|artifact| artifact.applied.processed_len())
                .sum::<u64>(),
            expected_applied,
            "each one-artifact budget must commit one whole replacement in causal order"
        );
    }
    let expected = vec![json!({"user_id":"a","total":15})];
    assert_eq!(demand(&router, "signed_consumer", None, 2).await, expected);
    let completed = checkpoint(&state, "signed_consumer")
        .await
        .unwrap()
        .checkpoint_key;
    assert_eq!(demand(&router, "signed_consumer", None, 2).await, expected);
    assert_eq!(
        checkpoint(&state, "signed_consumer")
            .await
            .unwrap()
            .checkpoint_key,
        completed
    );
}

#[tokio::test]
async fn invalid_query_selector_is_rejected_before_demand_writes() {
    let (state, router, store, _meta, _counts) = fixture().await;
    ingest(&router, 0, json!([{"user_id":"a","score":3,"delta":1}])).await;
    declare(
        &router,
        "validated_first",
        &[("scores", "2026-05-24.v1")],
        "SELECT user_id, score FROM scores",
    )
    .await;
    let mut before = store
        .list(None)
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.location.to_string(),
                entry.e_tag,
                entry.version,
                entry.size,
            )
        })
        .collect::<Vec<_>>()
        .await;
    before.sort();
    let mut body = demand_body(Some("user_id = 'a'"), 64);
    body["sql"] = json!("SELECT absent_column FROM validated_first");
    let response = post(&router, "/v1/views/validated_first/query", body).await;
    assert_eq!(response.0, StatusCode::BAD_REQUEST, "{response:?}");
    assert!(
        checkpoint(&state, "validated_first").await.is_none(),
        "invalid selector must not trigger an applied checkpoint"
    );
    let mut after = store
        .list(None)
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.location.to_string(),
                entry.e_tag,
                entry.version,
                entry.size,
            )
        })
        .collect::<Vec<_>>()
        .await;
    after.sort();
    assert_eq!(
        after, before,
        "SQL validation must precede demand output/state writes"
    );
}

#[tokio::test]
async fn ordinary_query_where_drives_scoped_demand_and_reuses_bound_coverage() {
    let (state, router, _store, _meta, counts) = fixture().await;
    ingest(
        &router,
        0,
        json!([
            {"user_id":"a","score":3,"delta":1},
            {"user_id":"b","score":5,"delta":1},
            {"user_id":"a","score":7,"delta":1},
            {"user_id":"b","score":11,"delta":1}
        ]),
    )
    .await;
    for (view, parameterized) in [("literal_scope", false), ("bound_scope", true)] {
        declare(
            &router,
            view,
            &[("scores", "2026-05-24.v1")],
            "SELECT user_id, score FROM scores",
        )
        .await;
        let predicate = if parameterized {
            "user_id = {{ context.params.group }}"
        } else {
            "user_id = 'a'"
        };
        let mut body = demand_body(None, 1);
        body["sql"] = json!(format!(
            "SELECT score AS amount FROM {view} WHERE {predicate}"
        ));
        if parameterized {
            body["parameters"] = json!({"group":"a"});
        }
        assert!(body["materialization"].get("scope").is_none());
        let mut completed = false;
        for _ in 0..16 {
            let response = post(&router, &format!("/v1/views/{view}/query"), body.clone()).await;
            if response.0 == StatusCode::OK {
                let mut rows = response.1["rows"].as_array().unwrap().clone();
                rows.sort_by_key(Value::to_string);
                assert_eq!(rows, vec![json!({"amount":3}), json!({"amount":7})]);
                completed = true;
                break;
            }
            assert_pending(&response, 1);
        }
        assert!(completed, "ordinary WHERE demand must finish: {view}");
        let record = checkpoint(&state, view).await.unwrap();
        let proof = record.demand_materialization.as_ref().unwrap();
        assert!(
            !proof.full_complete,
            "ordinary WHERE must not force full-view materialization"
        );
        assert_eq!(proof.artifacts.len(), 1);
        assert_eq!(
            proof.artifacts.values().next().unwrap().applied.as_spans(),
            vec![[0, 1], [2, 3]],
            "B rows must stay unapplied despite being scanned to certify A"
        );
        assert!(!proof.certificates.is_empty());
        assert!(
            proof
                .certificates
                .iter()
                .all(|certificate| certificate.scope_sql.is_some()),
            "A's predicate must not certify the full output"
        );
        counts.clear();
        let repeated = post(&router, &format!("/v1/views/{view}/query"), body).await;
        assert_eq!(
            repeated.0,
            StatusCode::OK,
            "certified query must not enter demand again: {repeated:?}"
        );
        let mut repeated_rows = repeated.1["rows"].as_array().unwrap().clone();
        repeated_rows.sort_by_key(Value::to_string);
        assert_eq!(
            repeated_rows,
            vec![json!({"amount":3}), json!({"amount":7})]
        );
        assert_eq!(
            checkpoint(&state, view).await.unwrap().checkpoint_key,
            record.checkpoint_key
        );
        assert!(
            counts
                .get_paths()
                .iter()
                .all(|path| !path.starts_with("v1/ingest/")),
            "repeat query replayed source payload: {:?}",
            counts.get_paths()
        );
    }
}

#[tokio::test]
async fn reverse_sparse_source_defers_watermark_until_full_closure_after_restart() {
    let backend = Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    backend
        .inner
        .set_partition_authority_clock_for_test(0)
        .await;
    let (state, router, store, meta, _counts) = fixture_with_meta(backend.clone()).await;
    let mut schema = test_purchases_event_time_catalog().relation_schema;
    schema.relation_id = "measurements".into();
    schema.relation_name = "measurements".into();
    schema.primary_key_column_ids.clear();
    for column in &mut schema.columns {
        if column.semantic_role == RelationSemanticRoleV1::PrimaryKey {
            column.semantic_role = RelationSemanticRoleV1::Value;
        }
    }
    schema.allowed_operations = vec![RelationOperationV1::Insert];
    let catalog = VelorixRelationCatalogV1::from_relation_schema(
        schema,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap();
    let registered = post(&router, "/v1/relations", json!({"catalog":catalog})).await;
    assert_eq!(registered.0, StatusCode::CREATED, "{registered:?}");
    let watermark = 120_000_000_000i64;
    let batch = |offset, rows| {
        json!({
            "relation_version":"2026-05-24.v1","stream_id":"measurement-stream","partition_id":0,
            "start_offset_inclusive":offset,"rows":rows,
            "event_time_watermark":{"event_time_column_id":"event_time",
                "max_observed_event_time_ns":watermark,"watermark_ns":watermark}
        })
    };
    let appended = post(
        &router,
        "/v1/relations/measurements/ingest",
        batch(
            0,
            json!([
                {"user_id":"a","amount":3,"event_time":10_000_000_000i64,"delta":1},
                {"user_id":"b","amount":7,"event_time":70_000_000_000i64,"delta":1}
            ]),
        ),
    )
    .await;
    assert_eq!(appended.0, StatusCode::CREATED, "{appended:?}");
    declare(&router, "measured_windows", &[("measurements","2026-05-24.v1")],
        "SELECT user_id, window_start, window_end, SUM(amount) AS total FROM TUMBLE(measurements, event_time, INTERVAL '60 seconds') GROUP BY user_id, window_start, window_end").await;
    let window_step = post(
        &router,
        "/v1/views/measured_windows/backfill",
        json!({"action":"step","order":"reverse","budget":demand_budget(1)}),
    )
    .await;
    successful(&window_step);
    let partial_window = checkpoint(&state, "measured_windows").await.unwrap();
    let window_progress = partial_window.demand_materialization.as_ref().unwrap();
    assert!(!window_progress.full_complete);
    assert_eq!(
        window_progress
            .artifacts
            .values()
            .map(|artifact| artifact.applied.processed_len())
            .sum::<u64>(),
        1
    );
    assert!(partial_window
        .checkpoint
        .input_event_time_frontiers
        .is_empty());
    declare(
        &router,
        "measured_rows",
        &[("measurements", "2026-05-24.v1")],
        "SELECT user_id, amount, event_time FROM measurements",
    )
    .await;
    let partial = post(
        &router,
        "/v1/views/measured_rows/backfill",
        json!({"action":"step","order":"reverse","budget":demand_budget(1)}),
    )
    .await;
    successful(&partial);
    let record = checkpoint(&state, "measured_rows").await.unwrap();
    let progress = record.demand_materialization.as_ref().unwrap();
    assert!(!progress.full_complete);
    assert_eq!(
        progress
            .artifacts
            .values()
            .next()
            .unwrap()
            .applied
            .as_spans(),
        vec![[1, 2]]
    );
    assert!(
        record.checkpoint.input_event_time_frontiers.is_empty(),
        "processing the last ordinal cannot advance a global watermark over the first hole"
    );
    assert!(record
        .checkpoint
        .input_frontiers
        .iter()
        .all(|frontier| frontier.committed_offset_exclusive == 0));
    let owner = state.owner_id.clone();
    drop(router);
    drop(state);
    backend
        .inner
        .set_partition_authority_clock_for_test(30_001)
        .await;
    let mut restored = test_authoritative_api_state_with_store(store, "demand-owner", meta)
        .await
        .with_experimental_advanced_view_features(true);
    restored.owner_id = owner;
    restored
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    let recovered = checkpoint(&restored, "measured_rows").await.unwrap();
    assert_eq!(recovered.checkpoint_key, record.checkpoint_key);
    assert!(recovered.checkpoint.input_event_time_frontiers.is_empty());
    let recovered_window = checkpoint(&restored, "measured_windows").await.unwrap();
    assert_eq!(
        recovered_window.checkpoint_key,
        partial_window.checkpoint_key
    );
    assert!(recovered_window
        .checkpoint
        .input_event_time_frontiers
        .is_empty());
    let router = app(restored.clone());
    let mut expected_rows = vec![
        json!({"user_id":"a","amount":3,"event_time":10_000_000_000i64}),
        json!({"user_id":"b","amount":7,"event_time":70_000_000_000i64}),
    ];
    expected_rows.sort_by_key(Value::to_string);
    assert_eq!(
        demand(&router, "measured_rows", None, 64).await,
        expected_rows
    );
    let mapped = checkpoint(&restored, "measured_rows").await.unwrap();
    assert!(
        mapped
            .demand_materialization
            .as_ref()
            .unwrap()
            .full_complete
    );
    assert_eq!(mapped.checkpoint.input_event_time_frontiers.len(), 1);
    assert_eq!(
        mapped.checkpoint.input_event_time_frontiers[0].watermark_ns,
        watermark
    );
    let mut expected = vec![
        json!({"user_id":"a","window_start":0,"window_end":60_000_000_000i64,"total":3}),
        json!({"user_id":"b","window_start":60_000_000_000i64,"window_end":120_000_000_000i64,"total":7}),
    ];
    expected.sort_by_key(Value::to_string);
    assert_eq!(demand(&router, "measured_windows", None, 1).await, expected);
    let complete = checkpoint(&restored, "measured_windows").await.unwrap();
    assert!(
        complete
            .demand_materialization
            .as_ref()
            .unwrap()
            .full_complete
    );
    let frontiers = &complete.checkpoint.input_event_time_frontiers;
    assert_eq!(
        frontiers, &mapped.checkpoint.input_event_time_frontiers,
        "sparse map finalization and atomic window apply must forward identical source evidence"
    );
    assert_eq!(frontiers.len(), 1);
    assert_eq!(frontiers[0].relation_id, "measurements");
    assert_eq!(frontiers[0].relation_version, "2026-05-24.v1");
    assert_eq!(
        frontiers[0].schema_fingerprint,
        catalog.schema_fingerprint.to_string()
    );
    assert_eq!(frontiers[0].stream_id, "measurement-stream");
    assert_eq!(frontiers[0].partition_id, 0);
    assert_eq!(frontiers[0].event_time_column_id, "event_time");
    assert_eq!(frontiers[0].watermark_ns, watermark);
    assert_eq!(frontiers[0].max_observed_event_time_ns, watermark);
    assert_eq!(complete.checkpoint.input_frontiers.len(), 1);
    assert_eq!(
        complete.checkpoint.input_frontiers[0].committed_offset_exclusive,
        2
    );
    // Positive late data remains accepted; forwarding the watermark must not
    // silently turn it into a source rejection or an output drop.
    let late = post(
        &router,
        "/v1/relations/measurements/ingest",
        batch(
            2,
            json!([
                {"user_id":"a","amount":5,"event_time":20_000_000_000i64,"delta":1}
            ]),
        ),
    )
    .await;
    assert_eq!(late.0, StatusCode::CREATED, "{late:?}");
    let mut corrected = vec![
        json!({"user_id":"a","window_start":0,"window_end":60_000_000_000i64,"total":8}),
        json!({"user_id":"b","window_start":60_000_000_000i64,"window_end":120_000_000_000i64,"total":7}),
    ];
    corrected.sort_by_key(Value::to_string);
    assert_eq!(
        demand(&router, "measured_windows", None, 1).await,
        corrected
    );
}

#[tokio::test]
async fn window_replay_preserves_commit_order_across_source_partitions() {
    let (state, router, _store, _meta, _counts) = fixture().await;
    let mut schema = test_purchases_event_time_catalog().relation_schema;
    schema.relation_id = "partitioned_measurements".into();
    schema.relation_name = "partitioned_measurements".into();
    schema.primary_key_column_ids.clear();
    for column in &mut schema.columns {
        if column.semantic_role == RelationSemanticRoleV1::PrimaryKey {
            column.semantic_role = RelationSemanticRoleV1::Value;
        }
    }
    schema.allowed_operations = vec![RelationOperationV1::Insert];
    let catalog = VelorixRelationCatalogV1::from_relation_schema(
        schema,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap();
    let registered = post(&router, "/v1/relations", json!({"catalog":catalog})).await;
    assert_eq!(registered.0, StatusCode::CREATED, "{registered:?}");
    // The earlier commit deliberately has the larger partition ID. Sorting by
    // partition would replay the high watermark before the older, valid row.
    // Both partition frontiers must close the later window; event times alone do not.
    for (partition, user, amount, event_time, watermark) in [
        (1, "early", 3, 10_000_000_000i64, 240_000_000_000i64),
        (0, "later", 7, 180_000_000_000i64, 300_000_000_000i64),
    ] {
        let appended = post(
            &router,
            "/v1/relations/partitioned_measurements/ingest",
            json!({
                "relation_version":"2026-05-24.v1",
                "stream_id":"partitioned-stream","partition_id":partition,
                "start_offset_inclusive":0,
                "rows":[{"user_id":user,"amount":amount,"event_time":event_time,"delta":1}],
                "event_time_watermark":{"event_time_column_id":"event_time",
                    "max_observed_event_time_ns":watermark,"watermark_ns":watermark}
            }),
        )
        .await;
        assert_eq!(appended.0, StatusCode::CREATED, "{appended:?}");
    }
    let declared = post(
        &router,
        "/v1/views",
        json!({
            "view_id":"partitioned_windows",
            "input_relation_id":"partitioned_measurements",
            "input_relation_version":"2026-05-24.v1",
            "correction_horizon_ns":60_000_000_000i64,
            "sql":"SELECT user_id, window_start, window_end, SUM(amount) AS total FROM TUMBLE(partitioned_measurements, event_time, INTERVAL '60 seconds') GROUP BY user_id, window_start, window_end"
        }),
    )
    .await;
    assert_eq!(declared.0, StatusCode::CREATED, "{declared:?}");
    assert!(checkpoint(&state, "partitioned_windows").await.is_none());
    let first = post(
        &router,
        "/v1/views/partitioned_windows/backfill",
        json!({"action":"step","order":"reverse","budget":demand_budget(1)}),
    )
    .await;
    successful(&first);
    let partial = checkpoint(&state, "partitioned_windows").await.unwrap();
    assert!(
        !partial
            .demand_materialization
            .as_ref()
            .unwrap()
            .full_complete
    );
    assert!(partial.checkpoint.input_event_time_frontiers.is_empty());
    assert_eq!(
        partial
            .demand_materialization
            .as_ref()
            .unwrap()
            .artifacts
            .values()
            .map(|artifact| artifact.applied.processed_len())
            .sum::<u64>(),
        1
    );
    let mut expected = vec![
        json!({"user_id":"early","window_start":0,"window_end":60_000_000_000i64,"total":3}),
        json!({"user_id":"later","window_start":180_000_000_000i64,"window_end":240_000_000_000i64,"total":7}),
    ];
    expected.sort_by_key(Value::to_string);
    assert_eq!(
        demand(&router, "partitioned_windows", None, 1).await,
        expected
    );
    let complete = checkpoint(&state, "partitioned_windows").await.unwrap();
    assert!(
        complete
            .demand_materialization
            .as_ref()
            .unwrap()
            .full_complete
    );
    assert_eq!(complete.checkpoint.input_frontiers.len(), 2);
    assert!(complete
        .checkpoint
        .input_frontiers
        .iter()
        .all(|frontier| frontier.committed_offset_exclusive == 1));
    assert_eq!(
        demand(&router, "partitioned_windows", None, 1).await,
        expected
    );
    assert_eq!(
        checkpoint(&state, "partitioned_windows")
            .await
            .unwrap()
            .checkpoint_key,
        complete.checkpoint_key
    );
}

#[tokio::test]
async fn source_appends_leave_declared_views_unbuilt_until_requested() {
    for historical in [false, true] {
        let (state, router, _store, _meta, _counts) = fixture().await;
        if historical {
            ingest(&router, 0, json!([{"user_id":"a","score":3,"delta":1}])).await;
        }
        declare(
            &router,
            "requested_totals",
            &[("scores", "2026-05-24.v1")],
            "SELECT user_id, SUM(score) AS total FROM scores GROUP BY user_id",
        )
        .await;
        assert!(
            checkpoint(&state, "requested_totals").await.is_none(),
            "declaration must not build output (historical={historical})"
        );
        ingest(
            &router,
            if historical { 1 } else { 0 },
            json!([{"user_id":"a","score":5,"delta":1}]),
        )
        .await;
        assert!(
            checkpoint(&state, "requested_totals").await.is_none(),
            "positive ingest must not build an unrequested view (historical={historical})"
        );
        assert_eq!(
            demand(&router, "requested_totals", None, 1).await,
            vec![json!({"user_id":"a","total":if historical { 8 } else { 5 }})],
            "first demand must include every committed source row"
        );
        assert!(checkpoint(&state, "requested_totals").await.is_some());
    }
}

#[tokio::test]
async fn fixed_tumbling_scopes_are_available_before_full_coverage_and_survive_restart() {
    let backend = Arc::new(DurableCapabilityMetaStore::new(InMemoryMetaStore::default()));
    backend
        .inner
        .set_partition_authority_clock_for_test(0)
        .await;
    let (state, router, store, meta, counts) = fixture_with_meta(backend.clone()).await;
    let mut schema = test_purchases_event_time_catalog().relation_schema;
    schema.relation_id = "scoped_measurements".into();
    schema.relation_name = "scoped_measurements".into();
    schema.primary_key_column_ids.clear();
    for column in &mut schema.columns {
        if column.semantic_role == RelationSemanticRoleV1::PrimaryKey {
            column.semantic_role = RelationSemanticRoleV1::Value;
        }
    }
    schema.allowed_operations = vec![RelationOperationV1::Insert];
    let catalog = VelorixRelationCatalogV1::from_relation_schema(
        schema,
        CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.into(),
    )
    .unwrap();
    let registered = post(
        &router,
        "/v1/relations",
        json!({
            "catalog":catalog
        }),
    )
    .await;
    assert_eq!(registered.0, StatusCode::CREATED, "{registered:?}");
    let batch = |offset, rows| {
        json!({
            "relation_version":"2026-05-24.v1","stream_id":"scoped-stream","partition_id":0,
            "start_offset_inclusive":offset,"rows":rows,
            "event_time_watermark":{"event_time_column_id":"event_time",
                "max_observed_event_time_ns":120_000_000_000i64,"watermark_ns":120_000_000_000i64}
        })
    };
    let appended = post(
        &router,
        "/v1/relations/scoped_measurements/ingest",
        batch(
            0,
            json!([
                {"user_id":"x","amount":3,"event_time":10_000_000_000i64,"delta":1},
                {"user_id":"x","amount":7,"event_time":70_000_000_000i64,"delta":1},
                {"user_id":"x","amount":5,"event_time":20_000_000_000i64,"delta":1},
                {"user_id":"x","amount":11,"event_time":80_000_000_000i64,"delta":1}
            ]),
        ),
    )
    .await;
    assert_eq!(appended.0, StatusCode::CREATED, "{appended:?}");
    declare(&router, "scoped_windows", &[("scoped_measurements", "2026-05-24.v1")],
        "SELECT user_id, window_start, window_end, SUM(amount) AS total, COUNT(*) AS n FROM TUMBLE(scoped_measurements, event_time, INTERVAL '60 seconds') GROUP BY user_id, window_start, window_end").await;
    let a_scope = "window_start >= 0 AND window_start < 60000000000";
    let b_scope = "window_start >= 60000000000 AND window_start < 120000000000";
    let a = json!({"user_id":"x","window_start":0,"window_end":60_000_000_000i64,"total":8,"n":2});
    let b = json!({"user_id":"x","window_start":60_000_000_000i64,"window_end":120_000_000_000i64,"total":18,"n":2});
    assert_eq!(
        demand(&router, "scoped_windows", Some(b_scope), 1).await,
        vec![b.clone()]
    );
    let saved = checkpoint(&state, "scoped_windows").await.unwrap();
    let progress = saved.demand_materialization.as_ref().unwrap();
    assert!(!progress.full_complete);
    assert_eq!(progress.artifacts.len(), 1);
    assert_eq!(
        progress
            .artifacts
            .values()
            .next()
            .unwrap()
            .applied
            .as_spans(),
        vec![[1, 2], [3, 4]]
    );
    assert!(saved.checkpoint.input_event_time_frontiers.is_empty());
    // Invalidate B's certificate while A still has unapplied source ordinals.
    let late_partial = post(
        &router,
        "/v1/relations/scoped_measurements/ingest",
        batch(
            4,
            json!([
                {"user_id":"x","amount":13,"event_time":90_000_000_000i64,"delta":1}
            ]),
        ),
    )
    .await;
    assert_eq!(late_partial.0, StatusCode::CREATED, "{late_partial:?}");
    let b = json!({"user_id":"x","window_start":60_000_000_000i64,"window_end":120_000_000_000i64,"total":31,"n":3});
    assert_eq!(
        demand(&router, "scoped_windows", Some(b_scope), 1).await,
        vec![b.clone()]
    );
    let saved = checkpoint(&state, "scoped_windows").await.unwrap();
    let progress = saved.demand_materialization.as_ref().unwrap();
    assert!(!progress.full_complete, "late B must not materialize A");
    assert_eq!(progress.artifacts.len(), 2);
    let original = progress
        .artifacts
        .values()
        .find(|artifact| artifact.applied.rows_total() == 4)
        .unwrap();
    assert_eq!(original.applied.as_spans(), vec![[1, 2], [3, 4]]);
    assert_eq!(
        progress
            .artifacts
            .values()
            .map(|artifact| artifact.applied.processed_len())
            .sum::<u64>(),
        3
    );
    assert!(saved.checkpoint.input_event_time_frontiers.is_empty());
    let owner = state.owner_id.clone();
    drop(router);
    drop(state);
    backend
        .inner
        .set_partition_authority_clock_for_test(30_001)
        .await;
    let mut restored = test_authoritative_api_state_with_store(store, "demand-owner", meta)
        .await
        .with_experimental_advanced_view_features(true);
    restored.owner_id = owner;
    restored
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    let router = app(restored.clone());
    counts.clear();
    let reused = post(
        &router,
        "/v1/views/scoped_windows/query",
        demand_body(Some(b_scope), 1),
    )
    .await;
    assert_eq!(reused.0, StatusCode::OK, "{reused:?}");
    assert_eq!(reused.1["rows"], json!([b.clone()]));
    let recovered = checkpoint(&restored, "scoped_windows").await.unwrap();
    let recovered_progress = recovered.demand_materialization.as_ref().unwrap();
    assert!(!recovered_progress.full_complete);
    assert_eq!(
        recovered_progress
            .artifacts
            .values()
            .find(|artifact| artifact.applied.rows_total() == 4)
            .unwrap()
            .applied
            .as_spans(),
        vec![[1, 2], [3, 4]]
    );
    assert_eq!(
        checkpoint(&restored, "scoped_windows")
            .await
            .unwrap()
            .checkpoint_key,
        saved.checkpoint_key
    );
    assert!(
        counts
            .get_paths()
            .iter()
            .all(|path| !path.starts_with("v1/ingest/")),
        "certified B read source payload: {:?}",
        counts.get_paths()
    );
    assert_eq!(
        demand(&router, "scoped_windows", Some(a_scope), 1).await,
        vec![a.clone()]
    );
    let mut expected = vec![a.clone(), b];
    expected.sort_by_key(Value::to_string);
    assert_eq!(
        demand(
            &router,
            "scoped_windows",
            Some("window_start >= 0 AND window_start < 120000000000"),
            1
        )
        .await,
        expected
    );
    assert_eq!(demand(&router, "scoped_windows", None, 1).await, expected);
    assert!(
        checkpoint(&restored, "scoped_windows")
            .await
            .unwrap()
            .demand_materialization
            .unwrap()
            .full_complete
    );
    let late = post(
        &router,
        "/v1/relations/scoped_measurements/ingest",
        batch(
            5,
            json!([
                {"user_id":"x","amount":13,"event_time":90_000_000_000i64,"delta":1}
            ]),
        ),
    )
    .await;
    assert_eq!(late.0, StatusCode::CREATED, "{late:?}");
    let corrected_b = json!({"user_id":"x","window_start":60_000_000_000i64,"window_end":120_000_000_000i64,"total":44,"n":4});
    assert_eq!(
        demand(&router, "scoped_windows", Some(b_scope), 1).await,
        vec![corrected_b.clone()]
    );
    let mut corrected = vec![a, corrected_b];
    corrected.sort_by_key(Value::to_string);
    assert_eq!(demand(&router, "scoped_windows", None, 1).await, corrected);
}
