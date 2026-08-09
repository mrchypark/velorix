use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore, ObjectStoreExt};
use tempfile::TempDir;
use velorix_storage::{
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::{IngestBatch, IngestLog, ReplayCheckpoint},
    object_key::ObjectKey,
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

fn envelope_bytes(
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    account: &str,
    amount: i64,
) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("account_id", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("weight", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![account.to_string()])) as ArrayRef,
            Arc::new(Int64Array::from(vec![amount])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ],
    )
    .unwrap();

    IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "orders_relation".to_string(),
            relation_version: "2026-05-05".to_string(),
            schema_fingerprint:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            stream_id: stream_id.to_string(),
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
            event_time_watermark: None,
        },
        &[batch],
    )
    .unwrap()
}

#[tokio::test]
async fn log_replay_returns_committed_batches_in_deterministic_order() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);

    let late_orders = IngestBatch::new_bootstrap_unchecked(
        "orders",
        0,
        10,
        20,
        Bytes::from_static(b"orders-10-20"),
    )
    .unwrap();
    let early_orders = IngestBatch::new_bootstrap_unchecked(
        "orders",
        0,
        0,
        10,
        Bytes::from_static(b"orders-0-10"),
    )
    .unwrap();
    let other_partition =
        IngestBatch::new_bootstrap_unchecked("orders", 1, 0, 5, Bytes::from_static(b"orders-p1"))
            .unwrap();
    let payments =
        IngestBatch::new_bootstrap_unchecked("payments", 0, 0, 3, Bytes::from_static(b"payments"))
            .unwrap();

    log.append(&late_orders).await.unwrap();
    log.append(&payments).await.unwrap();
    log.append(&early_orders).await.unwrap();
    log.append(&other_partition).await.unwrap();

    let committed = log.list_committed().await.unwrap();

    assert_eq!(
        committed,
        vec![
            early_orders.descriptor(),
            late_orders.descriptor(),
            other_partition.descriptor(),
            payments.descriptor(),
        ]
    );

    let replayed = log.replay_from(&[]).await.unwrap();

    assert_eq!(
        replayed,
        vec![early_orders, late_orders, other_partition, payments]
    );
}

#[tokio::test]
async fn log_replay_skips_batches_covered_by_checkpoint_boundary() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);

    let covered =
        IngestBatch::new_bootstrap_unchecked("orders", 0, 0, 10, Bytes::from_static(b"covered"))
            .unwrap();
    let next =
        IngestBatch::new_bootstrap_unchecked("orders", 0, 10, 20, Bytes::from_static(b"next"))
            .unwrap();
    let other_partition = IngestBatch::new_bootstrap_unchecked(
        "orders",
        1,
        0,
        7,
        Bytes::from_static(b"other-partition"),
    )
    .unwrap();
    let other_stream = IngestBatch::new_bootstrap_unchecked(
        "payments",
        0,
        0,
        3,
        Bytes::from_static(b"other-stream"),
    )
    .unwrap();

    log.append(&covered).await.unwrap();
    log.append(&next).await.unwrap();
    log.append(&other_partition).await.unwrap();
    log.append(&other_stream).await.unwrap();

    let replayed = log
        .replay_from(&[ReplayCheckpoint::new("orders", 0, 10)])
        .await
        .unwrap();

    assert_eq!(replayed, vec![next, other_partition, other_stream]);
}

#[tokio::test]
async fn log_replay_uses_earliest_relation_checkpoint_for_shared_stream_partition() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);

    let covered_order =
        IngestBatch::new_bootstrap_unchecked("ledger", 0, 0, 10, Bytes::from_static(b"order-0-10"))
            .unwrap();
    let next_order = IngestBatch::new_bootstrap_unchecked(
        "ledger",
        0,
        10,
        20,
        Bytes::from_static(b"order-10-20"),
    )
    .unwrap();
    let covered_account = IngestBatch::new_bootstrap_unchecked(
        "ledger",
        0,
        90,
        100,
        Bytes::from_static(b"account-90-100"),
    )
    .unwrap();

    log.append(&covered_order).await.unwrap();
    log.append(&next_order).await.unwrap();
    log.append(&covered_account).await.unwrap();

    let replayed = log
        .replay_from(&[
            ReplayCheckpoint::for_relation("orders", "v1", "ledger", 0, 10),
            ReplayCheckpoint::for_relation("accounts", "v1", "ledger", 0, 100),
        ])
        .await
        .unwrap();

    assert_eq!(replayed, vec![next_order, covered_account]);
}

#[tokio::test]
async fn log_replay_ignores_output_namespace_objects_with_matching_ranges() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(Arc::clone(&store));

    let input =
        IngestBatch::new_bootstrap_unchecked("orders", 0, 0, 10, Bytes::from_static(b"input"))
            .unwrap();
    let output_key = ObjectKey::output_object("orders", 0, 0, 10, 20, "out-0001").unwrap();

    log.append(&input).await.unwrap();
    store
        .put(
            &Path::from(output_key.as_str()),
            Bytes::from_static(b"output").into(),
        )
        .await
        .unwrap();

    assert_eq!(
        log.list_committed().await.unwrap(),
        vec![input.descriptor()]
    );
    assert_eq!(log.replay_from(&[]).await.unwrap(), vec![input]);
}

#[tokio::test]
async fn log_replay_rejects_checkpoint_inside_committed_batch() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);
    let batch =
        IngestBatch::from_validated_envelope(envelope_bytes("orders", 0, 5, 15, "a", 10)).unwrap();

    log.append(&batch).await.unwrap();

    let err = log
        .replay_validated_envelopes_from(&[ReplayCheckpoint::new("orders", 0, 10)])
        .await
        .unwrap_err();

    assert!(err.to_string().contains("checkpoint boundary"));
}

#[tokio::test]
async fn log_replay_rejects_overlapping_committed_ranges_for_same_stream_partition() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);
    let first =
        IngestBatch::from_validated_envelope(envelope_bytes("orders", 0, 0, 10, "a", 10)).unwrap();
    let overlapping =
        IngestBatch::from_validated_envelope(envelope_bytes("orders", 0, 5, 15, "b", 20)).unwrap();

    log.append(&first).await.unwrap();
    log.append(&overlapping).await.unwrap();

    let err = log.list_committed().await.unwrap_err();

    assert!(err
        .to_string()
        .contains("overlapping committed ingest ranges"));
}

#[tokio::test]
async fn log_replay_rejects_duplicate_checkpoints_for_same_stream_partition() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);

    let err = log
        .replay_from(&[
            ReplayCheckpoint::new("orders", 0, 10),
            ReplayCheckpoint::new("orders", 0, 20),
        ])
        .await
        .unwrap_err();

    assert!(err.to_string().contains("duplicate replay checkpoint"));
}

#[tokio::test]
async fn log_replay_rejects_overwrites_and_invalid_ranges() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);
    let batch =
        IngestBatch::new_bootstrap_unchecked("orders", 0, 0, 10, Bytes::from_static(b"first"))
            .unwrap();

    log.append(&batch).await.unwrap();

    let duplicate =
        IngestBatch::new_bootstrap_unchecked("orders", 0, 0, 10, Bytes::from_static(b"second"))
            .unwrap();
    let err = log.append(&duplicate).await.unwrap_err();
    assert!(err.to_string().contains("already exists"));

    let invalid =
        IngestBatch::new_bootstrap_unchecked("orders", 0, 10, 10, Bytes::new()).unwrap_err();
    assert!(invalid
        .to_string()
        .contains("offset range must be nonempty"));
}

#[tokio::test]
async fn log_replay_errors_on_malformed_ingest_keys_under_target_prefix() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(Arc::clone(&store));

    store
        .put(
            &Path::from("v1/state/owner/p=0000000000/chk=00000000000000000001/state-0001.state"),
            Bytes::from_static(b"ignored").into(),
        )
        .await
        .unwrap();
    store
        .put(
            &Path::from("v1/ingest/orders/p=0/00000000000000000000-00000000000000000001.batch"),
            Bytes::from_static(b"malformed").into(),
        )
        .await
        .unwrap();

    let err = log.list_committed().await.unwrap_err();

    assert!(err.to_string().contains("malformed ingest batch key"));
}
