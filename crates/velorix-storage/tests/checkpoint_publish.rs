use std::sync::Arc;

use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use tempfile::TempDir;
use velorix_storage::{
    manifest::{CheckpointManifest, InputRange},
    state::{CheckpointPublisher, StateObjectWrite},
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

fn input_range() -> InputRange {
    InputRange {
        stream_id: "orders".to_string(),
        partition_id: 0,
        start_offset_inclusive: 0,
        end_offset_exclusive: 10,
    }
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

#[tokio::test]
async fn checkpoint_publish_makes_valid_manifest_visible_after_state_write() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let state = state_write(0, "state-0001", b"state-bytes");

    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let manifest = CheckpointManifest {
        schema_version: 1,
        checkpoint_version: 0,
        input_ranges: vec![input_range()],
        state_objects: vec![state_ref.clone()],
        output_objects: vec![],
        parent_checkpoint: None,
        created_at: "2026-05-03T00:00:00Z".to_string(),
    };

    publisher.publish_manifest(&manifest).await.unwrap();

    assert_eq!(
        publisher.read_state_object(&state_ref).await.unwrap(),
        state.bytes
    );
    assert_eq!(publisher.latest_manifest().await.unwrap(), Some(manifest));
}

#[tokio::test]
async fn checkpoint_publish_crash_before_manifest_publication_leaves_no_visible_checkpoint() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);

    assert_eq!(publisher.latest_manifest().await.unwrap(), None);
    assert_eq!(
        publisher.list_published_manifests().await.unwrap(),
        Vec::new()
    );
}

#[tokio::test]
async fn checkpoint_publish_orphan_state_object_does_not_advance_checkpoint_visibility() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"orphan-state");

    let state_ref = publisher.write_state_object(&state).await.unwrap();

    assert_eq!(
        publisher.read_state_object(&state_ref).await.unwrap(),
        Bytes::from_static(b"orphan-state")
    );
    assert_eq!(publisher.latest_manifest().await.unwrap(), None);
    assert_eq!(
        publisher.list_published_manifests().await.unwrap(),
        Vec::new()
    );

    let state_path = Path::from(state_ref.object_key.as_str());
    assert!(store.head(&state_path).await.is_ok());
}

#[tokio::test]
async fn checkpoint_publish_rejects_duplicate_manifest_publication() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let state = state_write(0, "state-0001", b"state-bytes");
    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let manifest = CheckpointManifest {
        schema_version: 1,
        checkpoint_version: 0,
        input_ranges: vec![input_range()],
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint: None,
        created_at: "2026-05-03T00:00:00Z".to_string(),
    };

    publisher.publish_manifest(&manifest).await.unwrap();
    let err = publisher.publish_manifest(&manifest).await.unwrap_err();

    assert!(err.to_string().contains("already exists"));
}

#[tokio::test]
async fn checkpoint_publish_rejects_duplicate_state_object_write() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let state = state_write(0, "state-0001", b"first");

    publisher.write_state_object(&state).await.unwrap();
    let duplicate = state_write(0, "state-0001", b"second");
    let err = publisher.write_state_object(&duplicate).await.unwrap_err();

    assert!(err.to_string().contains("already exists"));
}

#[tokio::test]
async fn checkpoint_publish_rejects_invalid_manifest_before_writing() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-bytes");
    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let invalid_manifest = CheckpointManifest {
        schema_version: 1,
        checkpoint_version: 0,
        input_ranges: Vec::new(),
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint: None,
        created_at: "2026-05-03T00:00:00Z".to_string(),
    };

    let err = publisher
        .publish_manifest(&invalid_manifest)
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("manifest must include at least one input range"));
    let manifest_path = Path::from(invalid_manifest.object_key().as_str());
    assert!(store.head(&manifest_path).await.is_err());
}
