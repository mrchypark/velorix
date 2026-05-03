use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::EngineCheckpoint,
    operator::KeyedSumCountAggregate,
    query::{QueryError, QueryPolicy, QueryPolicyError},
};
use velorix_runtime::persisted_query::{
    query_persisted_recovered_materialized_view, PersistedQueryError, PersistedQueryStore,
};
use velorix_storage::{
    log::{IngestBatch, IngestLog},
    manifest::{CheckpointManifest, InputRange, StateObjectRef},
    object_key::ObjectKey,
    state::{CheckpointPublisher, StateObjectWrite},
};

const RECOVERY_OWNER: &str = "orders_sum_count";

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

#[tokio::test]
async fn persisted_query_store_creates_and_reads_spec_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let policy = QueryPolicy {
        max_sql_bytes: Some(128),
        max_output_rows: Some(10),
        ..QueryPolicy::default()
    };

    let created = catalog
        .create(
            "orders-active",
            "select key_json, value_json, weight from input where weight > 0",
            policy,
        )
        .await
        .unwrap();
    let read = catalog.get("orders-active").await.unwrap();

    assert_eq!(created, read);
    assert_eq!(read.schema_version, 1);
    assert_eq!(read.query_id, "orders-active");
    assert_eq!(read.policy, policy);
}

#[tokio::test]
async fn persisted_query_store_rejects_duplicate_ids_using_create_semantics() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));

    catalog
        .create(
            "orders-active",
            "select key_json, value_json, weight from input",
            QueryPolicy::default(),
        )
        .await
        .unwrap();

    let error = catalog
        .create(
            "orders-active",
            "select key_json from input",
            QueryPolicy::default(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, PersistedQueryError::ObjectStore(_)));
}

#[tokio::test]
async fn persisted_query_store_does_not_write_catalog_object_when_sql_is_invalid() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));

    let error = catalog
        .create(
            "broken-query",
            "select missing_column from input",
            QueryPolicy::default(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, PersistedQueryError::Query(_)));
    let key = ObjectKey::persisted_query("broken-query").unwrap();
    let path = Path::from(key.as_str());
    assert!(matches!(
        store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn persisted_query_store_rejects_unsupported_schema_version_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));

    write_catalog_object(
        Arc::clone(&store),
        "orders-active",
        json!({
            "schema_version": 2,
            "query_id": "orders-active",
            "sql": "select key_json, value_json, weight from input",
            "policy": QueryPolicy::default(),
        }),
    )
    .await;

    let error = catalog.get("orders-active").await.unwrap_err();

    assert!(matches!(
        error,
        PersistedQueryError::UnsupportedSchemaVersion { schema_version: 2 }
    ));
}

#[tokio::test]
async fn persisted_query_store_rejects_mismatched_query_id_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));

    write_catalog_object(
        Arc::clone(&store),
        "orders-active",
        json!({
            "schema_version": 1,
            "query_id": "other-query",
            "sql": "select key_json, value_json, weight from input",
            "policy": QueryPolicy::default(),
        }),
    )
    .await;

    let error = catalog.get("orders-active").await.unwrap_err();

    assert!(matches!(
        error,
        PersistedQueryError::QueryIdMismatch {
            expected,
            actual,
        } if expected == "orders-active" && actual == "other-query"
    ));
}

#[tokio::test]
async fn persisted_query_store_rejects_malformed_json_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let key = ObjectKey::persisted_query("orders-active").unwrap();

    store
        .put(
            &Path::from(key.as_str()),
            Bytes::from_static(br#"{"schema_version":"#).into(),
        )
        .await
        .unwrap();

    let error = catalog.get("orders-active").await.unwrap_err();

    assert!(matches!(error, PersistedQueryError::Json(_)));
}

#[tokio::test]
async fn persisted_recovered_query_execution_uses_stored_sql_and_policy() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
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
    let state_ref = write_checkpoint_state(
        &publisher,
        "state-persisted-query",
        2,
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest(2, state_ref))
        .await
        .unwrap();

    catalog
        .create(
            "account-a-only",
            "select key_json, value_json, weight from input where key_json = '\"account-a\"'",
            QueryPolicy {
                max_output_rows: Some(1),
                ..QueryPolicy::default()
            },
        )
        .await
        .unwrap();

    let output = query_persisted_recovered_materialized_view(Arc::clone(&store), "account-a-only")
        .await
        .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(string_value(&output[0], 0, 0), "\"account-a\"");
    assert_eq!(string_value(&output[0], 1, 0), "{\"count\":3,\"sum\":18}");
    assert_eq!(int64_value(&output[0], 2, 0), 1);
}

#[tokio::test]
async fn persisted_recovered_query_execution_applies_stored_policy() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let checkpoint_input = batch([input_delta("account-a", 10, 1)]);
    let replay_input = batch([input_delta("account-b", 7, 1)]);

    ingest_log
        .append(&IngestBatch::new("orders", 0, 0, 1, batch_bytes(&checkpoint_input)).unwrap())
        .await
        .unwrap();
    ingest_log
        .append(&IngestBatch::new("orders", 0, 1, 2, batch_bytes(&replay_input)).unwrap())
        .await
        .unwrap();

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&checkpoint_input).unwrap();
    let state_ref = write_checkpoint_state(
        &publisher,
        "state-persisted-query-policy",
        1,
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest(1, state_ref))
        .await
        .unwrap();

    catalog
        .create(
            "too-many-rows",
            "select key_json, value_json, weight from input order by key_json",
            QueryPolicy {
                max_output_rows: Some(1),
                ..QueryPolicy::default()
            },
        )
        .await
        .unwrap();

    let error = query_persisted_recovered_materialized_view(Arc::clone(&store), "too-many-rows")
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        PersistedQueryError::RuntimeQuery(velorix_runtime::query::RuntimeQueryError::Query(
            QueryError::Policy(QueryPolicyError::OutputRowsExceeded {
                observed_rows: 2,
                max_rows: 1
            })
        ))
    ));
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

async fn write_catalog_object(
    store: Arc<dyn ObjectStore>,
    query_id: &str,
    catalog: serde_json::Value,
) {
    let key = ObjectKey::persisted_query(query_id).unwrap();
    store
        .put(
            &Path::from(key.as_str()),
            Bytes::from(serde_json::to_vec(&catalog).unwrap()).into(),
        )
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
