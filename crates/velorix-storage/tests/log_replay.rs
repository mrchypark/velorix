use std::sync::Arc;

use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use tempfile::TempDir;
use velorix_storage::log::{IngestBatch, IngestLog, ReplayCheckpoint};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

#[tokio::test]
async fn log_replay_returns_committed_batches_in_deterministic_order() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);

    let late_orders =
        IngestBatch::new("orders", 0, 10, 20, Bytes::from_static(b"orders-10-20")).unwrap();
    let early_orders =
        IngestBatch::new("orders", 0, 0, 10, Bytes::from_static(b"orders-0-10")).unwrap();
    let other_partition =
        IngestBatch::new("orders", 1, 0, 5, Bytes::from_static(b"orders-p1")).unwrap();
    let payments = IngestBatch::new("payments", 0, 0, 3, Bytes::from_static(b"payments")).unwrap();

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

    let covered = IngestBatch::new("orders", 0, 0, 10, Bytes::from_static(b"covered")).unwrap();
    let next = IngestBatch::new("orders", 0, 10, 20, Bytes::from_static(b"next")).unwrap();
    let other_partition =
        IngestBatch::new("orders", 1, 0, 7, Bytes::from_static(b"other-partition")).unwrap();
    let other_stream =
        IngestBatch::new("payments", 0, 0, 3, Bytes::from_static(b"other-stream")).unwrap();

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
async fn log_replay_rejects_checkpoint_inside_committed_batch() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);
    let batch = IngestBatch::new("orders", 0, 5, 15, Bytes::from_static(b"opaque")).unwrap();

    log.append(&batch).await.unwrap();

    let err = log
        .replay_from(&[ReplayCheckpoint::new("orders", 0, 10)])
        .await
        .unwrap_err();

    assert!(err.to_string().contains("checkpoint boundary"));
}

#[tokio::test]
async fn log_replay_rejects_overlapping_committed_ranges_for_same_stream_partition() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);
    let first = IngestBatch::new("orders", 0, 0, 10, Bytes::from_static(b"first")).unwrap();
    let overlapping = IngestBatch::new("orders", 0, 5, 15, Bytes::from_static(b"overlap")).unwrap();

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
    let batch = IngestBatch::new("orders", 0, 0, 10, Bytes::from_static(b"first")).unwrap();

    log.append(&batch).await.unwrap();

    let duplicate = IngestBatch::new("orders", 0, 0, 10, Bytes::from_static(b"second")).unwrap();
    let err = log.append(&duplicate).await.unwrap_err();
    assert!(err.to_string().contains("already exists"));

    let invalid = IngestBatch::new("orders", 0, 10, 10, Bytes::new()).unwrap_err();
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
