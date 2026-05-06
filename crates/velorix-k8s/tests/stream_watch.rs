use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use object_store::{memory::InMemory, ObjectStore};
use serde_json::{json, Value};
use velorix_k8s::{
    controller::AuthoritySnapshot,
    crd::{
        ConditionState, ObjectStoreAuthorityRef, RelationVersionRef, StreamStatus,
        VelorixCondition, VelorixStream, VelorixStreamSpec,
    },
    status::{KubernetesStatusError, StreamStatusApi, StreamStatusWriter},
    stream_watch::{
        handle_stream_event, AuthoritySnapshotProvider, RelationCatalogSnapshotProvider,
        StreamWatchError, StreamWatchEvent,
    },
};
use velorix_storage::relation_catalog_registry::RelationCatalogRegistry;

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
    let provider = RelationCatalogSnapshotProvider::new(authority(), store);

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
async fn relation_catalog_snapshot_provider_reports_missing_relation_without_catalog() {
    let store = memory_store();
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let provider = RelationCatalogSnapshotProvider::new(authority(), store);

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
    let provider = RelationCatalogSnapshotProvider::new(
        ObjectStoreAuthorityRef {
            store_id: "secondary".to_string(),
            namespace: "analytics".to_string(),
        },
        store,
    );

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

async fn create_relation_catalog(store: &Arc<dyn ObjectStore>) {
    let catalog = serde_json::from_value(relation_catalog_json()).unwrap();
    RelationCatalogRegistry::new(Arc::clone(store))
        .create(&catalog)
        .await
        .unwrap();
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
        "feldera_relation": {
            "relation_id": "deposits",
            "schema_fingerprint": relation().schema_fingerprint,
        },
        "incremental_adapter": {
            "adapter_id": "incremental-adapter-deposits-v1",
        },
    })
}
