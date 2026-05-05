use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use velorix_k8s::{
    crd::{
        CheckpointRef, ConditionState, ObjectStoreAuthorityRef, RelationVersionRef, StreamStatus,
        VelorixCondition, VelorixStream, VelorixStreamSpec,
    },
    status::{KubernetesStatusError, StreamStatusApi, StreamStatusWriter},
};

#[tokio::test]
async fn stream_status_writer_patches_expected_status_payload() {
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let status = ready_status(7);

    writer
        .write_stream_status(&stream(), status.clone())
        .await
        .unwrap();

    assert_eq!(
        api.writes(),
        vec![StatusWrite {
            namespace: "analytics".to_string(),
            name: "deposits".to_string(),
            patch: json!({ "status": status }),
        }]
    );
}

#[tokio::test]
async fn stream_status_writer_writes_supplied_status_without_reading_stale_existing_status() {
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let mut stream = stream();
    stream.status = Some(ready_status(99));
    let authoritative_status = StreamStatus {
        observed_generation: Some(1),
        last_accepted_relation_schema_fingerprint: None,
        latest_published_checkpoint: None,
        readiness: Some(VelorixCondition {
            type_: "Ready".to_string(),
            status: ConditionState::False,
            reason: "MissingAuthorityRecord".to_string(),
            message: "object-store authority record is not visible".to_string(),
        }),
    };

    writer
        .write_stream_status(&stream, authoritative_status.clone())
        .await
        .unwrap();

    assert_eq!(
        api.writes(),
        vec![StatusWrite {
            namespace: "analytics".to_string(),
            name: "deposits".to_string(),
            patch: json!({ "status": authoritative_status }),
        }]
    );
}

#[tokio::test]
async fn stream_status_writer_rejects_stream_without_namespace() {
    let api = FakeStatusApi::default();
    let writer = StreamStatusWriter::new(api.clone());
    let mut stream = stream();
    stream.metadata.namespace = None;

    let err = writer
        .write_stream_status(&stream, ready_status(7))
        .await
        .unwrap_err();

    assert_eq!(
        err,
        KubernetesStatusError::MissingObjectField {
            field: "metadata.namespace"
        }
    );
    assert!(api.writes().is_empty());
}

#[derive(Clone, Default)]
struct FakeStatusApi {
    writes: Arc<Mutex<Vec<StatusWrite>>>,
}

impl FakeStatusApi {
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
            relation: RelationVersionRef {
                relation_id: "deposits".to_string(),
                relation_version: 1,
                schema_fingerprint: format!("sha256:{}", "1".repeat(64)),
            },
            authority: ObjectStoreAuthorityRef {
                store_id: "primary".to_string(),
                namespace: "analytics".to_string(),
            },
        },
    );
    stream.metadata.namespace = Some("analytics".to_string());
    stream.metadata.generation = Some(1);
    stream
}

fn ready_status(checkpoint_version: u64) -> StreamStatus {
    StreamStatus {
        observed_generation: Some(1),
        last_accepted_relation_schema_fingerprint: Some(format!("sha256:{}", "1".repeat(64))),
        latest_published_checkpoint: Some(CheckpointRef {
            checkpoint_version,
            manifest_digest: format!("sha256:{checkpoint_version:064x}"),
        }),
        readiness: Some(VelorixCondition {
            type_: "Ready".to_string(),
            status: ConditionState::True,
            reason: "AuthorityValidated".to_string(),
            message: "object-store authority and relation catalog records validated".to_string(),
        }),
    }
}
