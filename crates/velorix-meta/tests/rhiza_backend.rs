#![cfg(feature = "rhiza-backend")]

use serde_json::json;
use tempfile::tempdir;
use velorix_meta::rhiza::RhizaSqlStore;

#[tokio::test]
async fn rhiza_capability_fails_closed_without_server_timestamp() {
    assert!(!RhizaSqlStore::server_serialized_timestamp_available());
    assert!(!RhizaSqlStore::authority_operations_available());
}

#[tokio::test]
async fn rhiza_atomic_sql_conditional_write_and_changed_payload_rejection() {
    let dir = tempdir().unwrap();
    let store = RhizaSqlStore::open(dir.path().display().to_string(), "rhiza-test")
        .await
        .unwrap();
    store
        .execute_atomic(
            "schema",
            "CREATE TABLE requests (request_id TEXT PRIMARY KEY, digest TEXT NOT NULL, value INTEGER NOT NULL)",
            json!([]),
        )
        .await
        .unwrap();
    store
        .execute_atomic(
            "request-1",
            "INSERT INTO requests(request_id,digest,value) VALUES ($1,$2,$3)",
            json!(["request-1", "digest-a", 1]),
        )
        .await
        .unwrap();

    // A changed payload with the same request ID is rejected by the SQL
    // guard and cannot overwrite the committed request.
    let changed = store
        .execute_atomic(
            "request-1",
            "INSERT INTO requests(request_id,digest,value) VALUES ($1,$2,$3)",
            json!(["request-1", "digest-b", 2]),
        )
        .await;
    let changed = changed.expect_err("same request ID with changed payload must reject");
    match changed {
        velorix_meta::rhiza::RhizaSqlError::Operation { code, .. } => {
            assert_eq!(code, "request_conflict")
        }
        other => panic!("expected request_conflict, got {other:?}"),
    }
    let result = store
        .query_linearizable(
            "SELECT digest,value FROM requests WHERE request_id=$1",
            json!(["request-1"]),
        )
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], json!("digest-a"));
    assert_eq!(result.rows[0][1], json!(1));
}

#[tokio::test]
async fn rhiza_restart_preserves_committed_rows() {
    let dir = tempdir().unwrap();
    let path = dir.path().display().to_string();
    let store = RhizaSqlStore::open(path.clone(), "rhiza-restart")
        .await
        .unwrap();
    store
        .execute_atomic(
            "schema",
            "CREATE TABLE persisted (value INTEGER NOT NULL)",
            json!([]),
        )
        .await
        .unwrap();
    store
        .execute_atomic("value", "INSERT INTO persisted VALUES ($1)", json!([42]))
        .await
        .unwrap();
    drop(store);

    let reopened = RhizaSqlStore::open(path, "rhiza-restart").await.unwrap();
    let result = reopened
        .query_linearizable("SELECT value FROM persisted", json!([]))
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![json!(42)]]);
}

#[tokio::test]
async fn rhiza_atomic_batch_rolls_back_on_expected_rows_mismatch() {
    let dir = tempdir().unwrap();
    let store = RhizaSqlStore::open(dir.path().display().to_string(), "rhiza-rollback")
        .await
        .unwrap();
    store
        .execute_atomic(
            "schema",
            "CREATE TABLE rollback_test (value INTEGER)",
            json!([]),
        )
        .await
        .unwrap();
    let result = store
        .execute_atomic_statements(
            "rollback-request",
            vec![
                velorix_meta::rhiza::SqlStatement {
                    sql: "INSERT INTO rollback_test VALUES (1)".into(),
                    args: vec![],
                    want_rows: false,
                    expected_rows_affected: Some(1),
                },
                velorix_meta::rhiza::SqlStatement {
                    sql: "UPDATE rollback_test SET value=2 WHERE value=99".into(),
                    args: vec![],
                    want_rows: false,
                    expected_rows_affected: Some(1),
                },
            ],
        )
        .await;
    assert!(result.is_err());
    let rows = store
        .query_linearizable("SELECT value FROM rollback_test", json!([]))
        .await
        .unwrap();
    assert!(rows.rows.is_empty());
}

#[tokio::test]
async fn rhiza_same_request_replay_is_idempotent_and_close_requires_unique_owner() {
    let dir = tempdir().unwrap();
    let store = RhizaSqlStore::open(dir.path().display().to_string(), "rhiza-replay")
        .await
        .unwrap();
    store
        .execute_atomic(
            "schema",
            "CREATE TABLE replay_test (value INTEGER)",
            json!([]),
        )
        .await
        .unwrap();
    let first = store
        .execute_atomic(
            "replay-request",
            "INSERT INTO replay_test VALUES ($1)",
            json!([7]),
        )
        .await
        .unwrap();
    let second = store
        .execute_atomic(
            "replay-request",
            "INSERT INTO replay_test VALUES ($1)",
            json!([7]),
        )
        .await
        .unwrap();
    assert_eq!(first.rows_affected, second.rows_affected);
    let rows = store
        .query_linearizable("SELECT value FROM replay_test", json!([]))
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    let clone = store.clone();
    assert!(clone.close().await.is_err());
    store.close().await.unwrap();
}
