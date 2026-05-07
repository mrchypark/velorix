use std::sync::Arc;

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use velorix_storage::{
    manifest::{SlateDbCheckpointRefV1, StateObjectRef, StateRefType},
    state::{CheckpointPublishError, StateObjectWrite},
    state_store::{SlateDbStateStore, StateObjectStore},
};

fn temp_store() -> (TempDir, Arc<dyn object_store::ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

fn state_marker_key(state_key: &str) -> String {
    format!(
        "__velorix_state_ref_v1/sha256:{:x}",
        Sha256::digest(state_key.as_bytes())
    )
}

fn state_write(checkpoint_version: u64, object_id: &str, bytes: &'static [u8]) -> StateObjectWrite {
    StateObjectWrite::new(
        "balances_by_account",
        0,
        checkpoint_version,
        object_id,
        Bytes::from_static(bytes),
    )
    .unwrap()
}

fn raw_state_ref(state: &StateObjectWrite) -> StateObjectRef {
    StateObjectRef {
        object_id: state.object_id().to_string(),
        object_key: state.object_key().clone(),
        owner: state.owner().to_string(),
        partition_id: state.partition_id(),
        checkpoint_version: state.checkpoint_version(),
        ref_type: StateRefType::RawObject,
        owner_claim: None,
        slatedb: None,
    }
}

#[tokio::test]
async fn slatedb_state_store_writes_recoverable_checkpoint_metadata() {
    let (_temp_dir, store) = temp_store();
    let state_store = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();
    let state = state_write(7, "state-0007", b"slatedb-state");

    let written_ref = state_store.write_state_object(&state).await.unwrap();

    assert_eq!(written_ref.ref_type, StateRefType::SlateDbCheckpoint);
    assert_eq!(
        written_ref.slatedb,
        Some(SlateDbCheckpointRefV1 {
            db_path: "v1/slatedb/state".to_string(),
            state_key: state.object_key().as_str().to_string(),
            state_digest: "sha256:6822df83b222cdd273643b4502434d96c70cf96400fdd9a80946d6cc40a4d07a"
                .to_string(),
            state_bytes: 13,
            created_by_checkpoint_version: 7,
        })
    );
    assert_eq!(
        state_store.read_state_object(&written_ref).await.unwrap(),
        Bytes::from_static(b"slatedb-state")
    );
}

#[tokio::test]
async fn slatedb_state_store_recovers_written_state_after_reopen() {
    let (_temp_dir, store) = temp_store();
    let state = state_write(11, "state-0011", b"recoverable-state");
    let written_ref = {
        let state_store = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
            .await
            .unwrap();
        let written_ref = state_store.write_state_object(&state).await.unwrap();
        state_store.close().await.unwrap();
        written_ref
    };

    let reopened = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();

    assert!(reopened.state_object_exists(&written_ref).await.unwrap());
    assert_eq!(
        reopened.read_state_object(&written_ref).await.unwrap(),
        Bytes::from_static(b"recoverable-state")
    );
}

#[tokio::test]
async fn slatedb_state_store_release_makes_checkpoint_ref_unreadable() {
    let (_temp_dir, store) = temp_store();
    let state_store = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();
    let state = state_write(12, "state-0012", b"released-state");
    let written_ref = state_store.write_state_object(&state).await.unwrap();

    state_store
        .release_state_object(&written_ref)
        .await
        .unwrap();

    assert!(!state_store.state_object_exists(&written_ref).await.unwrap());
    let err = state_store
        .read_state_object(&written_ref)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        CheckpointPublishError::MissingStateObject(object_key)
            if object_key == *state.object_key()
    ));
}

#[tokio::test]
async fn slatedb_state_store_release_fails_closed_when_payload_is_missing() {
    let (_temp_dir, store) = temp_store();
    let state = state_write(13, "state-0013", b"missing-payload");
    let written_ref = {
        let state_store = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
            .await
            .unwrap();
        let written_ref = state_store.write_state_object(&state).await.unwrap();
        state_store.close().await.unwrap();
        written_ref
    };
    let db = slatedb::Db::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();
    db.delete(written_ref.object_key.as_str().as_bytes())
        .await
        .unwrap();
    db.close().await.unwrap();

    let reopened = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();
    let err = reopened
        .release_state_object(&written_ref)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::MissingStateObject(object_key)
            if object_key == *state.object_key()
    ));
}

#[tokio::test]
async fn slatedb_state_store_fails_closed_for_raw_state_refs() {
    let (_temp_dir, store) = temp_store();
    let state_store = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();
    let state = state_write(0, "state-0001", b"raw-state");
    let raw_ref = raw_state_ref(&state);

    assert!(!state_store.state_object_exists(&raw_ref).await.unwrap());
    let err = state_store.read_state_object(&raw_ref).await.unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::InvalidSlateDbStateRef { object_key, .. }
            if object_key == *state.object_key()
    ));
}

#[tokio::test]
async fn slatedb_state_store_fails_closed_when_checkpoint_metadata_is_missing() {
    let (_temp_dir, store) = temp_store();
    let state_store = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();
    let state = state_write(2, "state-0002", b"state");
    let mut written_ref = state_store.write_state_object(&state).await.unwrap();
    written_ref.slatedb = None;

    assert!(!state_store.state_object_exists(&written_ref).await.unwrap());
    let err = state_store
        .read_state_object(&written_ref)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::InvalidSlateDbStateRef { object_key, .. }
            if object_key == *state.object_key()
    ));
}

#[tokio::test]
async fn slatedb_state_store_fails_closed_when_digest_does_not_match() {
    let (_temp_dir, store) = temp_store();
    let state_store = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();
    let state = state_write(3, "state-0003", b"state");
    let mut written_ref = state_store.write_state_object(&state).await.unwrap();
    written_ref.slatedb.as_mut().unwrap().state_digest = format!("sha256:{}", "0".repeat(64));

    let err = state_store
        .read_state_object(&written_ref)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::SlateDbStatePayloadMismatch { object_key, .. }
            if object_key == *state.object_key()
    ));
}

#[tokio::test]
async fn slatedb_state_store_requires_marker_and_payload_for_publish_existence() {
    let (_temp_dir, store) = temp_store();
    let state = state_write(4, "state-0004", b"state");
    let written_ref = {
        let state_store = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
            .await
            .unwrap();
        let written_ref = state_store.write_state_object(&state).await.unwrap();
        state_store.close().await.unwrap();
        written_ref
    };

    let db = slatedb::Db::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();
    db.delete(written_ref.object_key.as_str().as_bytes())
        .await
        .unwrap();
    db.close().await.unwrap();

    let reopened = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();

    assert!(!reopened.state_object_exists(&written_ref).await.unwrap());
    let err = reopened.read_state_object(&written_ref).await.unwrap_err();
    assert!(matches!(
        err,
        CheckpointPublishError::MissingStateObject(object_key)
            if object_key == *state.object_key()
    ));
}

#[tokio::test]
async fn slatedb_state_store_rejects_marker_metadata_mismatch() {
    let (_temp_dir, store) = temp_store();
    let state = state_write(5, "state-0005", b"state");
    let written_ref = {
        let state_store = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
            .await
            .unwrap();
        let written_ref = state_store.write_state_object(&state).await.unwrap();
        state_store.close().await.unwrap();
        written_ref
    };
    let metadata = written_ref.slatedb.as_ref().unwrap();
    let marker_key = state_marker_key(&metadata.state_key);
    let bad_marker = serde_json::json!({
        "db_path": metadata.db_path,
        "state_key": metadata.state_key,
        "state_digest": format!("sha256:{}", "0".repeat(64)),
        "state_bytes": metadata.state_bytes,
        "created_by_checkpoint_version": metadata.created_by_checkpoint_version
    });

    let db = slatedb::Db::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();
    db.put(
        marker_key.as_bytes(),
        serde_json::to_vec(&bad_marker).unwrap(),
    )
    .await
    .unwrap();
    db.close().await.unwrap();

    let reopened = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();
    let err = reopened
        .state_object_exists(&written_ref)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::InvalidSlateDbStateRef { object_key, .. }
            if object_key == *state.object_key()
    ));
}

#[test]
fn state_object_ref_keeps_raw_json_compatibility_without_slatedb_metadata() {
    let value = serde_json::json!({
        "object_id": "state-0001",
        "object_key": "v1/state/balances_by_account/p=0000000000/chk=00000000000000000001/state-0001.state",
        "owner": "balances_by_account",
        "partition_id": 0,
        "checkpoint_version": 1,
        "ref_type": "raw_object"
    });

    let state_ref: StateObjectRef = serde_json::from_value(value).unwrap();

    assert_eq!(state_ref.ref_type, StateRefType::RawObject);
    assert_eq!(state_ref.slatedb, None);
}
