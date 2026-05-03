use std::sync::Arc;

use bytes::Bytes;
use object_store::{local::LocalFileSystem, ObjectStore};
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    operator::KeyedSumCountAggregate,
};
use velorix_runtime::recovery::RecoveredRuntime;
use velorix_storage::{
    log::{IngestBatch, IngestLog},
    manifest::{CheckpointManifest, InputRange},
    state::{CheckpointPublisher, StateObjectWrite},
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

fn input_delta(account: &str, amount: i64, weight: i64) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(json!(account)),
        DeltaValue::from_json(json!(amount)),
        weight,
    )
}

fn batch(records: impl IntoIterator<Item = DeltaRecord>) -> DeltaBatch {
    DeltaBatch::from_records(records)
}

fn batch_bytes(batch: &DeltaBatch) -> Bytes {
    Bytes::from(serde_json::to_vec(batch).unwrap())
}

fn manifest(
    input_end: u64,
    state_ref: velorix_storage::manifest::StateObjectRef,
) -> CheckpointManifest {
    CheckpointManifest {
        schema_version: 1,
        checkpoint_version: 0,
        input_ranges: vec![InputRange {
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: input_end,
        }],
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint: None,
        created_at: "2026-05-03T00:00:00Z".to_string(),
    }
}

#[tokio::test]
async fn local_recovery_recovers_checkpointed_view_and_replays_only_later_ingest_batches() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let first_input = batch([
        input_delta("account-a", 10, 1),
        input_delta("account-a", 5, 1),
        input_delta("account-b", 7, 1),
    ]);
    let second_input = batch([
        input_delta("account-a", 3, 1),
        input_delta("account-b", 7, -1),
        input_delta("account-c", 11, 1),
    ]);

    ingest_log
        .append(&IngestBatch::new("orders", 0, 0, 3, batch_bytes(&first_input)).unwrap())
        .await
        .unwrap();
    ingest_log
        .append(&IngestBatch::new("orders", 0, 3, 6, batch_bytes(&second_input)).unwrap())
        .await
        .unwrap();

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&first_input).unwrap();
    let checkpointed_state = checkpointed_view.state();
    let state = StateObjectWrite::new(
        "orders_sum_count",
        0,
        0,
        "state-0001",
        batch_bytes(&checkpointed_state),
    )
    .unwrap();
    let state_ref = publisher.write_state_object(&state).await.unwrap();
    publisher
        .publish_manifest(&manifest(3, state_ref))
        .await
        .unwrap();

    drop(checkpointed_view);
    drop(ingest_log);
    drop(publisher);

    let recovered = RecoveredRuntime::recover(Arc::clone(&store)).await.unwrap();

    let mut expected_view = KeyedSumCountAggregate::new();
    expected_view.apply(&first_input).unwrap();
    expected_view.apply(&second_input).unwrap();

    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        expected_view.state().net_rows().unwrap()
    );
    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.latest_checkpoint_version(), Some(0));
}

#[tokio::test]
async fn local_recovery_without_manifest_starts_empty_and_replays_from_zero() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let input = batch([input_delta("account-a", 4, 1)]);

    ingest_log
        .append(&IngestBatch::new("orders", 0, 0, 1, batch_bytes(&input)).unwrap())
        .await
        .unwrap();

    let recovered = RecoveredRuntime::recover(Arc::clone(&store)).await.unwrap();
    let mut expected_view = KeyedSumCountAggregate::new();
    expected_view.apply(&input).unwrap();

    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        expected_view.state().net_rows().unwrap()
    );
    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.latest_checkpoint_version(), None);
}
