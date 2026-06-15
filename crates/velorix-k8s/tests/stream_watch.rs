use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use serde_json::{json, Value};
use velorix_k8s::{
    controller::AuthoritySnapshot,
    crd::{
        CheckpointRef, ConditionState, ObjectStoreAuthorityRef, RelationVersionRef, StreamStatus,
        VelorixCondition, VelorixStream, VelorixStreamSpec,
    },
    startup::{validate_operator_authority, OperatorAuthorityStartupComponents},
    status::{KubernetesStatusError, StreamStatusApi, StreamStatusWriter},
    stream_watch::{
        handle_stream_event, AuthoritySnapshotProvider, RelationCatalogSnapshotProvider,
        StreamWatchError, StreamWatchEvent,
    },
};
use velorix_storage::{
    capability::AuthoritativeNamespace,
    checkpoint_index::CheckpointLifecycleRecord,
    manifest::{CheckpointManifest, InputRange, StateObjectRef},
    object_key::ObjectKey,
    relation_catalog_registry::RelationCatalogRegistry,
    state::{CheckpointPublisher, StateObjectWrite},
};

#[tokio::test]
async fn stream_watch_handler_writes_reconcile_output_for_applied_stream() {
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let snapshot = StaticSnapshotProvider(Ok(AuthoritySnapshot::default()
        .with_authority(authority())
        .with_relation_for_authority(&authority(), &relation())));

    handle_stream_event(StreamWatchEvent::Applied(stream()), &snapshot, &writer)
        .await
        .unwrap();

    assert_eq!(
        api.writes(),
        vec![StatusWrite {
            namespace: "analytics".to_string(),
            name: "deposits".to_string(),
            patch: json!({
                "status": StreamStatus {
                    observed_generation: Some(1),
                    last_accepted_relation_schema_fingerprint: Some(relation().schema_fingerprint),
                    latest_published_checkpoint: None,
                    readiness: Some(ready_condition()),
                }
            }),
        }]
    );
}

#[tokio::test]
async fn stream_watch_handler_returns_writer_error_without_claiming_success() {
    let api = FakeStatusApi::failing(KubernetesStatusError::Api {
        operation: "patch_status",
        message: "forbidden".to_string(),
    });
    let writer = StreamStatusWriter::new(api.clone());
    let snapshot = StaticSnapshotProvider(Ok(AuthoritySnapshot::default()
        .with_authority(authority())
        .with_relation_for_authority(&authority(), &relation())));

    let err = handle_stream_event(StreamWatchEvent::Applied(stream()), &snapshot, &writer)
        .await
        .unwrap_err();

    assert_eq!(
        err,
        StreamWatchError::Status(KubernetesStatusError::Api {
            operation: "patch_status",
            message: "forbidden".to_string(),
        })
    );
    assert!(api.writes().is_empty());
}

#[tokio::test]
async fn stream_watch_handler_ignores_deleted_stream_without_status_write() {
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let snapshot = StaticSnapshotProvider(Ok(AuthoritySnapshot::default()));

    handle_stream_event(StreamWatchEvent::Deleted(stream()), &snapshot, &writer)
        .await
        .unwrap();

    assert!(api.writes().is_empty());
}

#[tokio::test]
async fn stream_watch_handler_returns_snapshot_error_without_status_write() {
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let snapshot = StaticSnapshotProvider(Err(StreamWatchError::snapshot(
        "authority object store unavailable",
    )));

    let err = handle_stream_event(StreamWatchEvent::Applied(stream()), &snapshot, &writer)
        .await
        .unwrap_err();

    assert_eq!(
        err,
        StreamWatchError::Snapshot {
            message: "authority object store unavailable".to_string()
        }
    );
    assert!(api.writes().is_empty());
}

#[tokio::test]
async fn relation_catalog_snapshot_provider_reports_ready_when_catalog_exists() {
    let store = memory_store();
    create_relation_catalog(&store).await;
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let provider = production_provider(store).await;

    handle_stream_event(StreamWatchEvent::Applied(stream()), &provider, &writer)
        .await
        .unwrap();

    assert_eq!(
        api.writes(),
        vec![StatusWrite {
            namespace: "analytics".to_string(),
            name: "deposits".to_string(),
            patch: json!({
                "status": StreamStatus {
                    observed_generation: Some(1),
                    last_accepted_relation_schema_fingerprint: Some(relation().schema_fingerprint),
                    latest_published_checkpoint: None,
                    readiness: Some(ready_condition()),
                }
            }),
        }]
    );
}

#[tokio::test]
async fn relation_catalog_snapshot_provider_does_not_report_stream_only_checkpoint_without_manifest_relation_identity(
) {
    let store = memory_store();
    create_relation_catalog(&store).await;
    publish_checkpoint(&store, 0, "deposits").await;
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let provider = production_provider(store).await;

    handle_stream_event(StreamWatchEvent::Applied(stream()), &provider, &writer)
        .await
        .unwrap();

    assert_eq!(
        api.writes(),
        vec![StatusWrite {
            namespace: "analytics".to_string(),
            name: "deposits".to_string(),
            patch: json!({
                "status": StreamStatus {
                    observed_generation: Some(1),
                    last_accepted_relation_schema_fingerprint: Some(relation().schema_fingerprint),
                    latest_published_checkpoint: None,
                    readiness: Some(ready_condition()),
                }
            }),
        }]
    );
}

#[tokio::test]
async fn production_relation_catalog_snapshot_provider_retains_validated_capability_evidence() {
    let store = memory_store();
    let validated = validate_operator_authority(
        authority(),
        Arc::clone(&store),
        "memory-k8s-authority",
        "v1/stream-watch-probes",
    )
    .await
    .unwrap();
    let expected_capabilities = validated.capabilities().clone();

    let provider = OperatorAuthorityStartupComponents::from_validated_authority(validated)
        .relation_snapshot_provider();

    assert_eq!(provider.capabilities(), &expected_capabilities);
    assert_eq!(
        provider.capabilities().profiles[&AuthoritativeNamespace::RelationCatalog].backend_name,
        "memory-k8s-authority"
    );
    assert_eq!(
        provider.capabilities().profiles[&AuthoritativeNamespace::Checkpoint].backend_name,
        "memory-k8s-authority"
    );
}

#[tokio::test]
async fn production_relation_catalog_snapshot_provider_reads_checkpoint_from_validated_store_only()
{
    let validated_store = memory_store();
    let other_store = memory_store();
    create_relation_catalog(&validated_store).await;
    let other_manifest_digest = publish_checkpoint(&other_store, 0, "deposits").await;
    let provider = production_provider(validated_store).await;
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());

    handle_stream_event(StreamWatchEvent::Applied(stream()), &provider, &writer)
        .await
        .unwrap();

    assert_ne!(
        api.writes()[0].patch["status"]["latest_published_checkpoint"],
        json!(CheckpointRef {
            checkpoint_version: 0,
            manifest_digest: other_manifest_digest,
        }),
        "production provider must not read checkpoint evidence from an unvalidated store"
    );
    assert_eq!(
        api.writes()[0].patch["status"]["latest_published_checkpoint"],
        Value::Null
    );
}

#[tokio::test]
async fn relation_catalog_snapshot_provider_does_not_report_another_stream_checkpoint() {
    let store = memory_store();
    create_relation_catalog(&store).await;
    publish_checkpoint(&store, 0, "other-stream").await;
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let provider = production_provider(store).await;

    handle_stream_event(StreamWatchEvent::Applied(stream()), &provider, &writer)
        .await
        .unwrap();

    assert_eq!(
        api.writes()[0].patch["status"]["latest_published_checkpoint"],
        Value::Null
    );
}

#[tokio::test]
async fn relation_catalog_snapshot_provider_does_not_report_checkpoint_without_lifecycle_digest() {
    let store = memory_store();
    create_relation_catalog(&store).await;
    publish_checkpoint(&store, 0, "deposits").await;
    store
        .delete(&Path::from(
            ObjectKey::checkpoint_lifecycle_record(0).as_str(),
        ))
        .await
        .unwrap();
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let provider = production_provider(store).await;

    handle_stream_event(StreamWatchEvent::Applied(stream()), &provider, &writer)
        .await
        .unwrap();

    assert_eq!(
        api.writes()[0].patch["status"]["latest_published_checkpoint"],
        Value::Null
    );
}

#[tokio::test]
async fn relation_catalog_snapshot_provider_does_not_report_checkpoint_with_stale_lifecycle_digest()
{
    let store = memory_store();
    create_relation_catalog(&store).await;
    publish_checkpoint(&store, 0, "deposits").await;
    overwrite_lifecycle_digest(&store, 0, "sha256:bad").await;
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let provider = production_provider(store).await;

    handle_stream_event(StreamWatchEvent::Applied(stream()), &provider, &writer)
        .await
        .unwrap();

    assert_eq!(
        api.writes()[0].patch["status"]["latest_published_checkpoint"],
        Value::Null
    );
}

#[tokio::test]
async fn relation_catalog_snapshot_provider_reports_missing_relation_without_catalog() {
    let store = memory_store();
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let provider = production_provider(store).await;

    handle_stream_event(StreamWatchEvent::Applied(stream()), &provider, &writer)
        .await
        .unwrap();

    assert_eq!(
        api.writes(),
        vec![StatusWrite {
            namespace: "analytics".to_string(),
            name: "deposits".to_string(),
            patch: json!({
                "status": StreamStatus {
                    observed_generation: Some(1),
                    last_accepted_relation_schema_fingerprint: None,
                    latest_published_checkpoint: None,
                    readiness: Some(VelorixCondition {
                        type_: "Ready".to_string(),
                        status: ConditionState::False,
                        reason: "MissingRelationCatalogRecord".to_string(),
                        message: "relation catalog record is not visible".to_string(),
                    }),
                }
            }),
        }]
    );
}

#[tokio::test]
async fn relation_catalog_snapshot_provider_ignores_unconfigured_authority() {
    let store = memory_store();
    create_relation_catalog(&store).await;
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let provider = production_provider_for(
        store,
        ObjectStoreAuthorityRef {
            store_id: "secondary".to_string(),
            namespace: "analytics".to_string(),
        },
    )
    .await;

    handle_stream_event(StreamWatchEvent::Applied(stream()), &provider, &writer)
        .await
        .unwrap();

    assert_eq!(
        api.writes(),
        vec![StatusWrite {
            namespace: "analytics".to_string(),
            name: "deposits".to_string(),
            patch: json!({
                "status": StreamStatus {
                    observed_generation: Some(1),
                    last_accepted_relation_schema_fingerprint: None,
                    latest_published_checkpoint: None,
                    readiness: Some(VelorixCondition {
                        type_: "Ready".to_string(),
                        status: ConditionState::False,
                        reason: "MissingAuthorityRecord".to_string(),
                        message: "object-store authority record is not visible".to_string(),
                    }),
                }
            }),
        }]
    );
}

#[derive(Clone)]
struct StaticSnapshotProvider(Result<AuthoritySnapshot, StreamWatchError>);

#[async_trait]
impl AuthoritySnapshotProvider for StaticSnapshotProvider {
    async fn snapshot_for_stream(
        &self,
        _stream: &VelorixStream,
    ) -> Result<AuthoritySnapshot, StreamWatchError> {
        self.0.clone()
    }
}

#[derive(Clone, Default)]
struct FakeStatusApi {
    writes: Arc<Mutex<Vec<StatusWrite>>>,
    error: Option<KubernetesStatusError>,
}

impl FakeStatusApi {
    fn failing(error: KubernetesStatusError) -> Self {
        Self {
            writes: Arc::default(),
            error: Some(error),
        }
    }

    fn writes(&self) -> Vec<StatusWrite> {
        self.writes.lock().unwrap().clone()
    }
}

#[async_trait]
impl StreamStatusApi for FakeStatusApi {
    async fn patch_status(
        &self,
        namespace: &str,
        name: &str,
        patch: Value,
    ) -> Result<(), KubernetesStatusError> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }

        self.writes.lock().unwrap().push(StatusWrite {
            namespace: namespace.to_string(),
            name: name.to_string(),
            patch,
        });
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StatusWrite {
    namespace: String,
    name: String,
    patch: Value,
}

fn stream() -> VelorixStream {
    let mut stream = VelorixStream::new(
        "deposits",
        VelorixStreamSpec {
            stream_id: "deposits".to_string(),
            database_id: "analytics".to_string(),
            relation: relation(),
            authority: authority(),
        },
    );
    stream.metadata.namespace = Some("analytics".to_string());
    stream.metadata.generation = Some(1);
    stream
}

fn authority() -> ObjectStoreAuthorityRef {
    ObjectStoreAuthorityRef {
        store_id: "primary".to_string(),
        namespace: "analytics".to_string(),
    }
}

fn relation() -> RelationVersionRef {
    RelationVersionRef {
        relation_id: "deposits".to_string(),
        relation_version: 1,
        schema_fingerprint:
            "sha256:9b09fa82241fce3bb9025911ed78168799ad384fe68f065258afe09eca6ede62".to_string(),
    }
}

fn ready_condition() -> VelorixCondition {
    VelorixCondition {
        type_: "Ready".to_string(),
        status: ConditionState::True,
        reason: "AuthorityValidated".to_string(),
        message: "object-store authority and relation catalog records validated".to_string(),
    }
}

fn memory_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

async fn production_provider(store: Arc<dyn ObjectStore>) -> RelationCatalogSnapshotProvider {
    production_provider_for(store, authority()).await
}

async fn production_provider_for(
    store: Arc<dyn ObjectStore>,
    authority: ObjectStoreAuthorityRef,
) -> RelationCatalogSnapshotProvider {
    let validated = validate_operator_authority(
        authority,
        store,
        "memory-k8s-authority",
        "v1/stream-watch-probes",
    )
    .await
    .unwrap();

    OperatorAuthorityStartupComponents::from_validated_authority(validated)
        .relation_snapshot_provider()
}

async fn create_relation_catalog(store: &Arc<dyn ObjectStore>) {
    let catalog = serde_json::from_value(relation_catalog_json()).unwrap();
    RelationCatalogRegistry::new(Arc::clone(store))
        .create(&catalog)
        .await
        .unwrap();
}

async fn publish_checkpoint(
    store: &Arc<dyn ObjectStore>,
    checkpoint_version: u64,
    stream_id: &str,
) -> String {
    let publisher = CheckpointPublisher::new(Arc::clone(store));
    let state = StateObjectWrite::new(
        "deposits",
        0,
        checkpoint_version,
        format!("state-{checkpoint_version}"),
        format!("state-{checkpoint_version}").into_bytes().into(),
    )
    .unwrap();
    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let manifest = checkpoint_manifest(checkpoint_version, stream_id, state_ref);

    publisher.publish_manifest(&manifest).await.unwrap();

    publisher
        .read_checkpoint_lifecycle_record(checkpoint_version)
        .await
        .unwrap()
        .manifest_digest
}

async fn overwrite_lifecycle_digest(
    store: &Arc<dyn ObjectStore>,
    checkpoint_version: u64,
    manifest_digest: &str,
) {
    let lifecycle_key = ObjectKey::checkpoint_lifecycle_record(checkpoint_version);
    let mut record: CheckpointLifecycleRecord = serde_json::from_slice(
        &store
            .get(&Path::from(lifecycle_key.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    record.manifest_digest = manifest_digest.to_string();
    store
        .delete(&Path::from(lifecycle_key.as_str()))
        .await
        .unwrap();
    store
        .put(
            &Path::from(lifecycle_key.as_str()),
            serde_json::to_vec(&record).unwrap().into(),
        )
        .await
        .unwrap();
}

fn checkpoint_manifest(
    checkpoint_version: u64,
    stream_id: &str,
    state_ref: StateObjectRef,
) -> CheckpointManifest {
    CheckpointManifest {
        schema_version: 1,
        checkpoint_version,
        input_ranges: vec![InputRange {
            stream_id: stream_id.to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 10,
        }],
        state_objects: vec![state_ref],
        output_objects: Vec::new(),
        parent_checkpoint: None,
        created_at: "2026-05-03T00:00:00Z".to_string(),
    }
}

fn relation_catalog_json() -> Value {
    json!({
        "schema_version": 1,
        "relation_schema": {
            "relation_id": "deposits",
            "relation_name": "deposits",
            "relation_version": "1",
            "columns": [
                {
                    "column_id": "deposit_id",
                    "name": "deposit_id",
                    "logical_type": { "kind": "utf8" },
                    "physical_arrow_type": { "kind": "utf8" },
                    "nullable": false,
                    "ordinal": 0,
                    "semantic_role": "primary_key",
                },
                {
                    "column_id": "amount",
                    "name": "amount",
                    "logical_type": { "kind": "int64" },
                    "physical_arrow_type": { "kind": "int64" },
                    "nullable": false,
                    "ordinal": 1,
                    "semantic_role": "value",
                },
                {
                    "column_id": "weight",
                    "name": "weight",
                    "logical_type": { "kind": "int64" },
                    "physical_arrow_type": { "kind": "int64" },
                    "nullable": false,
                    "ordinal": 2,
                    "semantic_role": "weight",
                },
            ],
            "primary_key_column_ids": ["deposit_id"],
            "weight_column_id": "weight",
            "allowed_operations": ["insert", "delete"],
            "event_time_column_id": null,
        },
        "schema_fingerprint": relation().schema_fingerprint,
        "datafusion_registration": {
            "name": "deposits",
            "mode": "table",
        },
        "incremental_relation": {
            "relation_id": "deposits",
            "schema_fingerprint": relation().schema_fingerprint,
        },
        "incremental_adapter": {
            "adapter_id": "incremental-adapter-single-key-sum-count-v1",
        },
    })
}
