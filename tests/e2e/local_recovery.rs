use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::EngineCheckpoint,
    operator::KeyedSumCountAggregate,
};
use velorix_runtime::recovery::{
    orders_sum_count_relation_catalog, RecoveredRuntime, ORDERS_SUM_COUNT_RELATION_ID,
    ORDERS_SUM_COUNT_RELATION_VERSION,
};
use velorix_storage::{
    ingest_envelope::IngestEnvelope,
    log::IngestLog,
    manifest::{CheckpointManifest, InputRange, OutputObjectRef, StateObjectRef},
    object_key::ObjectKey,
    state::{CheckpointPublisher, StateObjectWrite},
};

const RECOVERY_OWNER: &str = "orders_sum_count";

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

fn ingest_record_batch(input: &DeltaBatch) -> RecordBatch {
    let keys = input
        .records()
        .iter()
        .map(|record| record.key.as_json().as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    let values = input
        .records()
        .iter()
        .map(|record| record.value.as_json().as_i64().unwrap())
        .collect::<Vec<_>>();
    let weights = input
        .records()
        .iter()
        .map(|record| record.weight)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
            Arc::new(Int64Array::from(weights)) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn append_ingest_envelope(
    ingest_log: &IngestLog,
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    input: &DeltaBatch,
) {
    let catalog = orders_sum_count_relation_catalog().unwrap();
    let bytes = IngestEnvelope::encode_batches(
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        catalog.schema_fingerprint.as_str(),
        stream_id,
        partition_id,
        start_offset_inclusive,
        end_offset_exclusive,
        &[ingest_record_batch(input)],
    )
    .unwrap();

    ingest_log.append_validated_envelope(bytes).await.unwrap();
}

fn checkpoint_bytes(checkpoint: &EngineCheckpoint) -> Bytes {
    Bytes::from(serde_json::to_vec(&checkpoint.to_payload()).unwrap())
}

fn manifest(input_end: u64, state_ref: StateObjectRef) -> CheckpointManifest {
    manifest_with_ranges(
        0,
        None,
        vec![input_range("orders", 0, 0, input_end)],
        state_ref,
    )
}

fn manifest_with_ranges(
    checkpoint_version: u64,
    parent_checkpoint: Option<u64>,
    input_ranges: Vec<InputRange>,
    state_ref: StateObjectRef,
) -> CheckpointManifest {
    CheckpointManifest {
        schema_version: 1,
        checkpoint_version,
        input_ranges,
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint,
        created_at: "2026-05-03T00:00:00Z".to_string(),
    }
}

fn input_range(
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
) -> InputRange {
    InputRange {
        stream_id: stream_id.to_string(),
        partition_id,
        start_offset_inclusive,
        end_offset_exclusive,
    }
}

fn output_ref(
    stream_id: &str,
    partition_id: u32,
    checkpoint_version: u64,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    object_id: &str,
) -> OutputObjectRef {
    OutputObjectRef {
        object_id: object_id.to_string(),
        object_key: ObjectKey::output_object(
            stream_id,
            partition_id,
            checkpoint_version,
            start_offset_inclusive,
            end_offset_exclusive,
            object_id,
        )
        .unwrap(),
        stream_id: stream_id.to_string(),
        partition_id,
        checkpoint_version,
        start_offset_inclusive,
        end_offset_exclusive,
        owner_claim: None,
    }
}

async fn write_checkpoint_state(
    publisher: &CheckpointPublisher,
    checkpoint_version: u64,
    object_id: &str,
    state: &DeltaBatch,
) -> StateObjectRef {
    write_checkpoint_state_for_owner(
        publisher,
        RECOVERY_OWNER,
        checkpoint_version,
        object_id,
        state,
    )
    .await
}

async fn write_checkpoint_state_for_owner(
    publisher: &CheckpointPublisher,
    owner: &str,
    checkpoint_version: u64,
    object_id: &str,
    state: &DeltaBatch,
) -> StateObjectRef {
    let state =
        StateObjectWrite::new(owner, 0, checkpoint_version, object_id, batch_bytes(state)).unwrap();

    publisher.write_state_object(&state).await.unwrap()
}

async fn write_engine_checkpoint_state(
    publisher: &CheckpointPublisher,
    checkpoint_version: u64,
    object_id: &str,
    checkpoint: &EngineCheckpoint,
) -> StateObjectRef {
    let state = StateObjectWrite::new(
        RECOVERY_OWNER,
        0,
        checkpoint_version,
        object_id,
        checkpoint_bytes(checkpoint),
    )
    .unwrap();

    publisher.write_state_object(&state).await.unwrap()
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

    append_ingest_envelope(&ingest_log, "orders", 0, 0, 3, &first_input).await;
    append_ingest_envelope(&ingest_log, "orders", 0, 3, 6, &second_input).await;

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&first_input).unwrap();
    let checkpointed_state = checkpointed_view.state();
    let state_ref = write_checkpoint_state(&publisher, 0, "state-0001", &checkpointed_state).await;
    let output_ref = output_ref("orders", 0, 0, 3, 6, "materialized-out-0001");
    store
        .put(
            &Path::from(output_ref.object_key.as_str()),
            batch_bytes(&batch([input_delta("account-z", 99, 1)])).into(),
        )
        .await
        .unwrap();
    let mut checkpoint_manifest = manifest(3, state_ref);
    checkpoint_manifest.output_objects = vec![output_ref];
    publisher
        .publish_manifest(&checkpoint_manifest)
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

    append_ingest_envelope(&ingest_log, "orders", 0, 0, 1, &input).await;

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

#[tokio::test]
async fn local_recovery_uses_manifest_boundaries_per_stream_partition_range() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let covered_orders_p0 = batch([input_delta("account-a", 10, 1)]);
    let later_orders_p0 = batch([input_delta("account-a", 2, 1)]);
    let covered_orders_p1 = batch([input_delta("account-b", 5, 1)]);
    let later_orders_p1 = batch([input_delta("account-b", 3, 1)]);
    let covered_payments = batch([input_delta("account-c", 7, 1)]);
    let later_payments = batch([input_delta("account-c", 1, -1)]);

    for (stream, partition, start, end, input) in [
        ("orders", 0, 0, 2, &covered_orders_p0),
        ("orders", 0, 2, 4, &later_orders_p0),
        ("orders", 1, 0, 5, &covered_orders_p1),
        ("orders", 1, 5, 8, &later_orders_p1),
        ("payments", 0, 0, 1, &covered_payments),
        ("payments", 0, 1, 2, &later_payments),
    ] {
        append_ingest_envelope(&ingest_log, stream, partition, start, end, input).await;
    }

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&covered_orders_p0).unwrap();
    checkpointed_view.apply(&covered_orders_p1).unwrap();
    checkpointed_view.apply(&covered_payments).unwrap();
    let state_ref = write_checkpoint_state(
        &publisher,
        0,
        "state-multi-range",
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest_with_ranges(
            0,
            None,
            vec![
                input_range("orders", 0, 0, 2),
                input_range("orders", 1, 0, 5),
                input_range("payments", 0, 0, 1),
            ],
            state_ref,
        ))
        .await
        .unwrap();

    let recovered = RecoveredRuntime::recover(Arc::clone(&store)).await.unwrap();
    let mut expected_view = KeyedSumCountAggregate::new();
    for input in [
        &covered_orders_p0,
        &covered_orders_p1,
        &covered_payments,
        &later_orders_p0,
        &later_orders_p1,
        &later_payments,
    ] {
        expected_view.apply(input).unwrap();
    }

    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        expected_view.state().net_rows().unwrap()
    );
    assert_eq!(recovered.replayed_batch_count(), 3);
}

#[tokio::test]
async fn local_recovery_uses_latest_manifest_when_multiple_are_published() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let first = batch([input_delta("account-a", 10, 1)]);
    let second = batch([input_delta("account-a", 5, 1)]);
    let third = batch([input_delta("account-a", 3, 1)]);

    for (start, end, input) in [(0, 1, &first), (1, 2, &second), (2, 3, &third)] {
        append_ingest_envelope(&ingest_log, "orders", 0, start, end, input).await;
    }

    let mut older_view = KeyedSumCountAggregate::new();
    older_view.apply(&first).unwrap();
    let older_ref = write_checkpoint_state(&publisher, 0, "state-older", &older_view.state()).await;
    publisher
        .publish_manifest(&manifest_with_ranges(
            0,
            None,
            vec![input_range("orders", 0, 0, 1)],
            older_ref,
        ))
        .await
        .unwrap();

    let mut newer_view = KeyedSumCountAggregate::new();
    newer_view.apply(&first).unwrap();
    newer_view.apply(&second).unwrap();
    let newer_ref = write_checkpoint_state(&publisher, 1, "state-newer", &newer_view.state()).await;
    publisher
        .publish_manifest(&manifest_with_ranges(
            1,
            Some(0),
            vec![input_range("orders", 0, 0, 2)],
            newer_ref,
        ))
        .await
        .unwrap();

    let recovered = RecoveredRuntime::recover(Arc::clone(&store)).await.unwrap();
    let mut expected_view = KeyedSumCountAggregate::new();
    expected_view.apply(&first).unwrap();
    expected_view.apply(&second).unwrap();
    expected_view.apply(&third).unwrap();

    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        expected_view.state().net_rows().unwrap()
    );
    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.latest_checkpoint_version(), Some(1));
}

#[tokio::test]
async fn local_recovery_preserves_signed_checkpoint_state_and_signed_replay() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let checkpoint_input = batch([
        input_delta("account-a", 10, 1),
        input_delta("account-a", 5, -1),
        input_delta("account-b", 7, -1),
    ]);
    let replay_input = batch([
        input_delta("account-a", 2, 1),
        input_delta("account-b", 3, -1),
    ]);

    append_ingest_envelope(&ingest_log, "orders", 0, 0, 3, &checkpoint_input).await;
    append_ingest_envelope(&ingest_log, "orders", 0, 3, 5, &replay_input).await;

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&checkpoint_input).unwrap();
    let state_ref =
        write_checkpoint_state(&publisher, 0, "state-signed", &checkpointed_view.state()).await;
    publisher
        .publish_manifest(&manifest(3, state_ref))
        .await
        .unwrap();

    let recovered = RecoveredRuntime::recover(Arc::clone(&store)).await.unwrap();
    let mut expected_view = KeyedSumCountAggregate::new();
    expected_view.apply(&checkpoint_input).unwrap();
    expected_view.apply(&replay_input).unwrap();

    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        expected_view.state().net_rows().unwrap()
    );
    assert!(recovered
        .materialized_state()
        .net_rows()
        .unwrap()
        .contains(&DeltaRecord::new(
            DeltaKey::from_json(json!("account-b")),
            DeltaValue::from_json(json!({ "sum": -10, "count": -2 })),
            1,
        )));
}

#[tokio::test]
async fn local_recovery_resumes_from_checkpointed_engine_logical_epoch_not_manifest_version() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let first_input = batch([input_delta("account-a", 10, 1)]);
    let second_input = batch([input_delta("account-a", 5, 1)]);

    append_ingest_envelope(&ingest_log, "orders", 0, 0, 1, &first_input).await;
    append_ingest_envelope(&ingest_log, "orders", 0, 1, 2, &second_input).await;

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&first_input).unwrap();
    let checkpoint = EngineCheckpoint::new(3, checkpointed_view.state());
    let state_ref =
        write_engine_checkpoint_state(&publisher, 0, "state-logical-epoch", &checkpoint).await;
    publisher
        .publish_manifest(&manifest_with_ranges(
            0,
            None,
            vec![input_range("orders", 0, 0, 1)],
            state_ref,
        ))
        .await
        .unwrap();

    let recovered = RecoveredRuntime::recover(Arc::clone(&store)).await.unwrap();

    assert_eq!(recovered.logical_epoch(), 4);
}

#[tokio::test]
async fn local_recovery_rejects_manifest_state_with_unexpected_owner() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = DeltaBatch::from_records([DeltaRecord::new(
        DeltaKey::from_json(json!("account-a")),
        DeltaValue::from_json(json!({ "sum": 10, "count": 1 })),
        1,
    )]);
    let state_ref =
        write_checkpoint_state_for_owner(&publisher, "other_owner", 0, "state-wrong-owner", &state)
            .await;
    publisher
        .publish_manifest(&manifest(1, state_ref))
        .await
        .unwrap();

    let err = RecoveredRuntime::recover(Arc::clone(&store))
        .await
        .unwrap_err();

    assert!(err.to_string().contains("unexpected state object owner"));
}
