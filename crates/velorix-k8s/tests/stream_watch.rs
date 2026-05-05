use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use velorix_k8s::{
    controller::AuthoritySnapshot,
    crd::{
        ConditionState, ObjectStoreAuthorityRef, RelationVersionRef, StreamStatus,
        VelorixCondition, VelorixStream, VelorixStreamSpec,
    },
    status::{KubernetesStatusError, StreamStatusApi, StreamStatusWriter},
    stream_watch::{
        handle_stream_event, AuthoritySnapshotProvider, StreamWatchError, StreamWatchEvent,
    },
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
        schema_fingerprint: format!("sha256:{}", "1".repeat(64)),
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
