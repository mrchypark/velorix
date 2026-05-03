use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use bytes::Bytes;
use object_store::{local::LocalFileSystem, ObjectStore};
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    operator::KeyedSumCountAggregate,
};
use velorix_runtime::query::query_recovered_materialized_view;
use velorix_storage::{
    log::{IngestBatch, IngestLog},
    manifest::{CheckpointManifest, InputRange, StateObjectRef},
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

fn input_range(end_offset_exclusive: u64) -> InputRange {
    InputRange {
        stream_id: "orders".to_string(),
        partition_id: 0,
        start_offset_inclusive: 0,
        end_offset_exclusive,
    }
}

fn manifest(input_end: u64, state_ref: StateObjectRef) -> CheckpointManifest {
    CheckpointManifest {
        schema_version: 1,
        checkpoint_version: 0,
        input_ranges: vec![input_range(input_end)],
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint: None,
        created_at: "2026-05-04T00:00:00Z".to_string(),
    }
}

async fn write_checkpoint_state(
    publisher: &CheckpointPublisher,
    object_id: &str,
    state: &DeltaBatch,
) -> StateObjectRef {
    let state = StateObjectWrite::new(RECOVERY_OWNER, 0, 0, object_id, batch_bytes(state)).unwrap();

    publisher.write_state_object(&state).await.unwrap()
}

#[tokio::test]
async fn query_recovered_materialized_view_reads_checkpointed_state_and_replayed_ingest() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let checkpoint_input = batch([
        input_delta("account-a", 10, 1),
        input_delta("account-a", 5, 1),
    ]);
    let replay_input = batch([
        input_delta("account-a", 3, 1),
        input_delta("account-b", 7, 1),
    ]);

    ingest_log
        .append(&IngestBatch::new("orders", 0, 0, 2, batch_bytes(&checkpoint_input)).unwrap())
        .await
        .unwrap();
    ingest_log
        .append(&IngestBatch::new("orders", 0, 2, 4, batch_bytes(&replay_input)).unwrap())
        .await
        .unwrap();

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&checkpoint_input).unwrap();
    let state_ref =
        write_checkpoint_state(&publisher, "state-query", &checkpointed_view.state()).await;
    publisher
        .publish_manifest(&manifest(2, state_ref))
        .await
        .unwrap();

    let output = query_recovered_materialized_view(
        Arc::clone(&store),
        "select key_json, value_json, weight from input order by key_json",
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 2);
    assert_eq!(string_value(&output[0], 0, 0), "\"account-a\"");
    assert_eq!(string_value(&output[0], 1, 0), "{\"count\":3,\"sum\":18}");
    assert_eq!(int64_value(&output[0], 2, 0), 1);
    assert_eq!(string_value(&output[0], 0, 1), "\"account-b\"");
    assert_eq!(string_value(&output[0], 1, 1), "{\"count\":1,\"sum\":7}");
    assert_eq!(int64_value(&output[0], 2, 1), 1);
}

#[tokio::test]
async fn query_recovered_materialized_view_propagates_datafusion_errors() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let input = batch([input_delta("account-a", 4, 1)]);

    ingest_log
        .append(&IngestBatch::new("orders", 0, 0, 1, batch_bytes(&input)).unwrap())
        .await
        .unwrap();

    let error =
        query_recovered_materialized_view(Arc::clone(&store), "select missing_column from input")
            .await
            .unwrap_err();

    assert!(error.to_string().contains("missing_column"));
}

fn string_value(batch: &arrow::record_batch::RecordBatch, column: usize, row: usize) -> &str {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(row)
}

fn int64_value(batch: &arrow::record_batch::RecordBatch, column: usize, row: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(row)
}
