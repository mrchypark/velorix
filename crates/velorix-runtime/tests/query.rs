use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::object_store::{
    memory::InMemory as DataFusionInMemory, path::Path as DataFusionPath,
    ObjectStore as DataFusionObjectStore, ObjectStoreExt as DataFusionObjectStoreExt,
};
use object_store::{local::LocalFileSystem, ObjectStore};
use parquet::arrow::ArrowWriter;
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::EngineCheckpoint,
    operator::KeyedSumCountAggregate,
    query::{QueryError, QueryPolicy, QueryPolicyError},
};
use velorix_runtime::{
    query::{
        query_object_backed_input_with_policy, query_recovered_materialized_view,
        query_recovered_materialized_view_with_policy, RuntimeQueryError,
    },
    recovery::{
        orders_sum_count_relation_catalog, RecoveredRuntime, RecoveryError,
        ORDERS_SUM_COUNT_RELATION_ID, ORDERS_SUM_COUNT_RELATION_VERSION,
    },
};
use velorix_storage::{
    ingest_envelope::IngestEnvelope,
    log::{IngestBatch, IngestLog, IngestLogError},
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

fn ingest_envelope_bytes(
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    input: &DeltaBatch,
) -> Bytes {
    let catalog = orders_sum_count_relation_catalog().unwrap();
    ingest_envelope_bytes_with_relation(
        stream_id,
        partition_id,
        start_offset_inclusive,
        end_offset_exclusive,
        input,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        catalog.schema_fingerprint.as_str(),
    )
}

fn ingest_envelope_bytes_with_relation(
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    input: &DeltaBatch,
    relation_id: &str,
    relation_version: &str,
    schema_fingerprint: &str,
) -> Bytes {
    IngestEnvelope::encode_batches(
        relation_id,
        relation_version,
        schema_fingerprint,
        stream_id,
        partition_id,
        start_offset_inclusive,
        end_offset_exclusive,
        &[ingest_record_batch(input)],
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
    ingest_log
        .append_validated_envelope(ingest_envelope_bytes(
            stream_id,
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
            input,
        ))
        .await
        .unwrap();
}

fn parquet_input_batch(keys: &[&str], values: &[&str], weights: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key_json", DataType::Utf8, false),
            Field::new("value_json", DataType::Utf8, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(keys.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(weights.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn parquet_bytes(batch: &RecordBatch) -> Bytes {
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }
    Bytes::from(bytes)
}

async fn put_parquet_input(
    store: &Arc<dyn DataFusionObjectStore>,
    path: &str,
    batch: &RecordBatch,
) {
    store
        .put(&DataFusionPath::from(path), parquet_bytes(batch).into())
        .await
        .unwrap();
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
    logical_epoch: u64,
    state: &DeltaBatch,
) -> StateObjectRef {
    let checkpoint = EngineCheckpoint::new(logical_epoch, state.clone());
    let state = StateObjectWrite::new(
        RECOVERY_OWNER,
        0,
        0,
        object_id,
        Bytes::from(serde_json::to_vec(&checkpoint.to_payload()).unwrap()),
    )
    .unwrap();

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

    append_ingest_envelope(&ingest_log, "orders", 0, 0, 2, &checkpoint_input).await;
    append_ingest_envelope(&ingest_log, "orders", 0, 2, 4, &replay_input).await;

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&checkpoint_input).unwrap();
    let state_ref =
        write_checkpoint_state(&publisher, "state-query", 2, &checkpointed_view.state()).await;
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
async fn query_recovered_materialized_view_with_policy_applies_row_limit_to_recovered_state() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let checkpoint_input = batch([
        input_delta("account-a", 10, 1),
        input_delta("account-a", 5, 1),
    ]);
    let replay_input = batch([input_delta("account-b", 7, 1)]);

    append_ingest_envelope(&ingest_log, "orders", 0, 0, 2, &checkpoint_input).await;
    append_ingest_envelope(&ingest_log, "orders", 0, 2, 3, &replay_input).await;

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&checkpoint_input).unwrap();
    let state_ref = write_checkpoint_state(
        &publisher,
        "state-query-policy",
        2,
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest(2, state_ref))
        .await
        .unwrap();

    let error = query_recovered_materialized_view_with_policy(
        Arc::clone(&store),
        "select key_json, value_json, weight from input order by key_json",
        QueryPolicy {
            max_output_rows: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(QueryPolicyError::OutputRowsExceeded {
            observed_rows: 2,
            max_rows: 1
        }))
    ));
}

#[tokio::test]
async fn recovery_rejects_json_bytes_under_valid_v1_ingest_key() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let input = batch([input_delta("account-a", 4, 1)]);

    ingest_log
        .append(&IngestBatch::new("orders", 0, 0, 1, batch_bytes(&input)).unwrap())
        .await
        .unwrap();

    let error = RecoveredRuntime::recover(Arc::clone(&store))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::Ingest(IngestLogError::IngestEnvelope(_))
    ));
}

#[tokio::test]
async fn recovery_rejects_ingest_envelope_with_wrong_relation_version() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let input = batch([input_delta("account-a", 4, 1)]);
    let catalog = orders_sum_count_relation_catalog().unwrap();
    let bytes = ingest_envelope_bytes_with_relation(
        "orders",
        0,
        0,
        1,
        &input,
        ORDERS_SUM_COUNT_RELATION_ID,
        "2026-05-06.v1",
        catalog.schema_fingerprint.as_str(),
    );

    ingest_log.append_validated_envelope(bytes).await.unwrap();

    let error = RecoveredRuntime::recover(Arc::clone(&store))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::IngestRelationMismatch {
            field: "relation_version",
            ..
        }
    ));
}

#[tokio::test]
async fn recovery_rejects_ingest_envelope_with_wrong_schema_fingerprint() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let input = batch([input_delta("account-a", 4, 1)]);
    let bytes = ingest_envelope_bytes_with_relation(
        "orders",
        0,
        0,
        1,
        &input,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );

    ingest_log.append_validated_envelope(bytes).await.unwrap();

    let error = RecoveredRuntime::recover(Arc::clone(&store))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::IngestRelationMismatch {
            field: "schema_fingerprint",
            ..
        }
    ));
}

#[tokio::test]
async fn query_recovered_materialized_view_propagates_datafusion_errors() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let input = batch([input_delta("account-a", 4, 1)]);

    append_ingest_envelope(&ingest_log, "orders", 0, 0, 1, &input).await;

    let error =
        query_recovered_materialized_view(Arc::clone(&store), "select missing_column from input")
            .await
            .unwrap_err();

    assert!(error.to_string().contains("missing_column"));
}

#[tokio::test]
async fn query_object_backed_input_scans_parquet_objects_without_materializing_delta_batch() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(
            &["\"account-a\"", "\"account-a\"", "\"account-b\""],
            &["10", "5", "7"],
            &[1, 1, -1],
        ),
    )
    .await;

    let output = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, sum(cast(value_json as int)) as total_value, sum(weight) as total_weight \
         from input where weight > 0 group by key_json order by key_json",
        QueryPolicy::default(),
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(string_value(&output[0], 0, 0), "\"account-a\"");
    assert_eq!(int64_value(&output[0], 1, 0), 15);
    assert_eq!(int64_value(&output[0], 2, 0), 2);
}

#[tokio::test]
async fn query_object_backed_input_applies_policy_row_limit() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\"", "\"account-b\""], &["10", "7"], &[1, 1]),
    )
    .await;

    let error = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, value_json, weight from input order by key_json",
        QueryPolicy {
            max_output_rows: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(QueryPolicyError::OutputRowsExceeded {
            observed_rows: 2,
            max_rows: 1
        }))
    ));
}

#[tokio::test]
async fn query_object_backed_input_rejects_scan_above_byte_limit() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\""], &["10"], &[1]),
    )
    .await;

    let error = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, value_json, weight from input",
        QueryPolicy {
            max_scan_bytes: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(QueryPolicyError::ScanBytesExceeded {
            observed_bytes,
            max_bytes: 1,
        })) if observed_bytes > 1
    ));
}

#[tokio::test]
async fn query_object_backed_input_propagates_datafusion_scan_errors() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());

    let error = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/missing/",
        "select key_json, value_json, weight from input",
        QueryPolicy::default(),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::DataFusion(_))
    ));
}

fn string_value(batch: &arrow::record_batch::RecordBatch, column: usize, row: usize) -> String {
    let column = batch.column(column);
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        return array.value(row).to_string();
    }
    if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() {
        return array.value(row).to_string();
    }

    panic!(
        "expected string-compatible column, got {:?}",
        column.data_type()
    );
}

fn int64_value(batch: &arrow::record_batch::RecordBatch, column: usize, row: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(row)
}
