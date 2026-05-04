use std::sync::Arc;

use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use tempfile::TempDir;
use tokio::sync::Barrier;
use velorix_storage::{
    manifest::{
        CheckpointManifest, InputRange, OutputObjectRef, PartitionOwnerClaim, StateObjectRef,
    },
    object_key::ObjectKey,
    state::{CheckpointPublisher, StateObjectWrite},
    state_store::{SlateDbStateStore, StateObjectStore},
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
    state_write_bytes(checkpoint_version, object_id, Bytes::from_static(bytes))
}

fn state_write_bytes(checkpoint_version: u64, object_id: &str, bytes: Bytes) -> StateObjectWrite {
    StateObjectWrite::new(
        "balances_by_account",
        0,
        checkpoint_version,
        object_id,
        bytes,
    )
    .unwrap()
}

fn fenced_state_write(
    checkpoint_version: u64,
    object_id: &str,
    owner_claim: PartitionOwnerClaim,
    bytes: &'static [u8],
) -> StateObjectWrite {
    StateObjectWrite::new_fenced(
        "balances_by_account",
        0,
        checkpoint_version,
        object_id,
        owner_claim,
        Bytes::from_static(bytes),
    )
    .unwrap()
}

fn fenced_state_write_for_partition(
    partition_id: u32,
    checkpoint_version: u64,
    object_id: &str,
    owner_claim: PartitionOwnerClaim,
    bytes: &'static [u8],
) -> StateObjectWrite {
    StateObjectWrite::new_fenced(
        "balances_by_account",
        partition_id,
        checkpoint_version,
        object_id,
        owner_claim,
        Bytes::from_static(bytes),
    )
    .unwrap()
}

fn owner_claim(owner_id: &str, owner_epoch: u64) -> PartitionOwnerClaim {
    PartitionOwnerClaim {
        owner_id: owner_id.to_string(),
        owner_epoch,
    }
}

fn output_ref_for_partition(partition_id: u32) -> OutputObjectRef {
    OutputObjectRef {
        object_id: format!("out-p{partition_id}"),
        object_key: ObjectKey::ingest_batch("settlements", partition_id, 20, 25).unwrap(),
        stream_id: "settlements".to_string(),
        partition_id,
        start_offset_inclusive: 20,
        end_offset_exclusive: 25,
    }
}

fn state_ref(state: &StateObjectWrite) -> StateObjectRef {
    StateObjectRef {
        object_id: state.object_id().to_string(),
        object_key: state.object_key().clone(),
        owner: state.owner().to_string(),
        partition_id: state.partition_id(),
        checkpoint_version: state.checkpoint_version(),
        owner_claim: state.owner_claim().cloned(),
    }
}

fn manifest(checkpoint_version: u64, state_ref: StateObjectRef) -> CheckpointManifest {
    CheckpointManifest {
        schema_version: 1,
        checkpoint_version,
        input_ranges: vec![input_range()],
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint: checkpoint_version.checked_sub(1),
        created_at: "2026-05-03T00:00:00Z".to_string(),
    }
}

#[tokio::test]
async fn checkpoint_publish_makes_valid_manifest_visible_after_state_write() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let state = state_write(0, "state-0001", b"state-bytes");

    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let manifest = manifest(0, state_ref.clone());

    publisher.publish_manifest(&manifest).await.unwrap();

    assert_eq!(
        publisher.read_state_object(&state_ref).await.unwrap(),
        state.bytes().clone()
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
    let manifest = manifest(0, state_ref);

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

#[tokio::test]
async fn checkpoint_publish_rejects_manifest_body_that_does_not_match_object_key() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let state = state_write(0, "state-0001", b"state-bytes");
    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let manifest = manifest(0, state_ref);
    let wrong_key = ObjectKey::checkpoint_manifest(99);

    store
        .put(
            &Path::from(wrong_key.as_str()),
            Bytes::from(serde_json::to_vec(&manifest).unwrap()).into(),
        )
        .await
        .unwrap();

    let err = publisher.list_published_manifests().await.unwrap_err();

    assert!(err.to_string().contains("does not match manifest body"));
}

#[tokio::test]
async fn checkpoint_publish_latest_manifest_uses_numerically_latest_valid_checkpoint() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let state_0 = state_write(0, "state-0001", b"state-0");
    let state_1 = state_write(1, "state-0002", b"state-1");
    let manifest_0 = manifest(0, publisher.write_state_object(&state_0).await.unwrap());
    let manifest_1 = manifest(1, publisher.write_state_object(&state_1).await.unwrap());

    publisher.publish_manifest(&manifest_1).await.unwrap();
    publisher.publish_manifest(&manifest_0).await.unwrap();

    assert_eq!(
        publisher.list_published_manifests().await.unwrap(),
        vec![manifest_0, manifest_1.clone()]
    );
    assert_eq!(publisher.latest_manifest().await.unwrap(), Some(manifest_1));
}

#[tokio::test]
async fn checkpoint_publish_rejects_manifest_that_references_missing_state_object() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let missing_state = state_write(0, "missing-state", b"not-written");
    let manifest = manifest(0, state_ref(&missing_state));

    let err = publisher.publish_manifest(&manifest).await.unwrap_err();

    assert!(err.to_string().contains("referenced state object"));
    let manifest_path = Path::from(manifest.object_key().as_str());
    assert!(store.head(&manifest_path).await.is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_stale_owner_epoch_before_state_write() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let stale_claim = owner_claim("worker-a", 1);
    let current_claim = owner_claim("worker-b", 2);

    let old_state = fenced_state_write(0, "state-0001", stale_claim.clone(), b"old-state");
    let old_ref = publisher
        .write_state_object_fenced(&old_state, &stale_claim)
        .await
        .unwrap();
    publisher
        .publish_manifest_fenced(&manifest(0, old_ref), &stale_claim)
        .await
        .unwrap();

    let current_state = fenced_state_write(1, "state-0002", current_claim.clone(), b"current");
    let current_ref = publisher
        .write_state_object_fenced(&current_state, &current_claim)
        .await
        .unwrap();
    publisher
        .publish_manifest_fenced(&manifest(1, current_ref), &current_claim)
        .await
        .unwrap();

    let stale_state = fenced_state_write(2, "state-0003", stale_claim.clone(), b"stale");
    let err = publisher
        .write_state_object_fenced(&stale_state, &stale_claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("stale owner claim"));
    assert!(store
        .head(&Path::from(stale_state.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_same_owner_epoch_with_different_owner_id_before_state_write() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let published_claim = owner_claim("worker-a", 7);
    let conflicting_claim = owner_claim("worker-b", 7);

    let published_state =
        fenced_state_write(0, "state-0001", published_claim.clone(), b"published");
    let published_ref = publisher
        .write_state_object_fenced(&published_state, &published_claim)
        .await
        .unwrap();
    publisher
        .publish_manifest_fenced(&manifest(0, published_ref), &published_claim)
        .await
        .unwrap();

    let conflicting_state =
        fenced_state_write(1, "state-0002", conflicting_claim.clone(), b"conflict");
    let err = publisher
        .write_state_object_fenced(&conflicting_state, &conflicting_claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("stale owner claim"));
    assert!(store
        .head(&Path::from(conflicting_state.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_allows_newer_owner_after_old_orphan_state_object_exists() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let old_claim = owner_claim("worker-a", 1);
    let current_claim = owner_claim("worker-b", 2);

    let orphan_state = fenced_state_write(0, "state-0001", old_claim.clone(), b"orphan");
    let orphan_ref = publisher
        .write_state_object_fenced(&orphan_state, &old_claim)
        .await
        .unwrap();

    let current_state = fenced_state_write(0, "state-0002", current_claim.clone(), b"current");
    let current_ref = publisher
        .write_state_object_fenced(&current_state, &current_claim)
        .await
        .unwrap();
    let current_manifest = manifest(0, current_ref.clone());
    publisher
        .publish_manifest_fenced(&current_manifest, &current_claim)
        .await
        .unwrap();

    assert_eq!(
        publisher.read_state_object(&orphan_ref).await.unwrap(),
        Bytes::from_static(b"orphan")
    );
    assert_eq!(
        publisher.read_state_object(&current_ref).await.unwrap(),
        Bytes::from_static(b"current")
    );
    assert_eq!(
        publisher.latest_manifest().await.unwrap(),
        Some(current_manifest)
    );
}

#[tokio::test]
async fn checkpoint_publish_rejects_stale_owner_epoch_before_manifest_publication() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let stale_claim = owner_claim("worker-a", 1);
    let current_claim = owner_claim("worker-b", 2);

    let old_state = fenced_state_write(0, "state-0001", stale_claim.clone(), b"old-state");
    let old_ref = publisher
        .write_state_object_fenced(&old_state, &stale_claim)
        .await
        .unwrap();
    publisher
        .publish_manifest_fenced(&manifest(0, old_ref), &stale_claim)
        .await
        .unwrap();

    let stale_state = fenced_state_write(2, "state-0003", stale_claim.clone(), b"stale");
    let stale_ref = publisher.write_state_object(&stale_state).await.unwrap();
    let stale_manifest = manifest(2, stale_ref);

    let current_state = fenced_state_write(1, "state-0002", current_claim.clone(), b"current");
    let current_ref = publisher
        .write_state_object_fenced(&current_state, &current_claim)
        .await
        .unwrap();
    publisher
        .publish_manifest_fenced(&manifest(1, current_ref), &current_claim)
        .await
        .unwrap();

    let err = publisher
        .publish_manifest_fenced(&stale_manifest, &stale_claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("stale owner claim"));
    let manifest_path = Path::from(stale_manifest.object_key().as_str());
    assert!(store.head(&manifest_path).await.is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_fenced_manifest_with_unclaimed_input_partition_before_writing()
{
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let claim = owner_claim("worker-a", 1);
    let state = fenced_state_write(0, "state-0001", claim.clone(), b"state");
    let state_ref = publisher
        .write_state_object_fenced(&state, &claim)
        .await
        .unwrap();
    let manifest = CheckpointManifest {
        schema_version: 1,
        checkpoint_version: 0,
        input_ranges: vec![
            input_range(),
            InputRange {
                stream_id: "orders".to_string(),
                partition_id: 1,
                start_offset_inclusive: 0,
                end_offset_exclusive: 10,
            },
        ],
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint: None,
        created_at: "2026-05-03T00:00:00Z".to_string(),
    };

    let err = publisher
        .publish_manifest_fenced(&manifest, &claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("owner claim"));
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_fenced_manifest_with_unclaimed_output_partition_before_writing()
{
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let claim = owner_claim("worker-a", 1);
    let state = fenced_state_write(0, "state-0001", claim.clone(), b"state");
    let state_ref = publisher
        .write_state_object_fenced(&state, &claim)
        .await
        .unwrap();
    let mut manifest = manifest(0, state_ref);
    manifest.output_objects = vec![output_ref_for_partition(1)];

    let err = publisher
        .publish_manifest_fenced(&manifest, &claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("owner claim"));
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn checkpoint_publish_rejects_fenced_manifest_with_state_refs_from_different_owner_claims() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let requested_claim = owner_claim("worker-a", 1);
    let other_claim = owner_claim("worker-b", 1);
    let requested_state =
        fenced_state_write_for_partition(0, 0, "state-0001", requested_claim.clone(), b"state-a");
    let other_state =
        fenced_state_write_for_partition(1, 0, "state-0002", other_claim.clone(), b"state-b");
    let requested_ref = publisher
        .write_state_object_fenced(&requested_state, &requested_claim)
        .await
        .unwrap();
    let other_ref = publisher
        .write_state_object_fenced(&other_state, &other_claim)
        .await
        .unwrap();
    let mut manifest = manifest(0, requested_ref);
    manifest.state_objects.push(other_ref);

    let err = publisher
        .publish_manifest_fenced(&manifest, &requested_claim)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("owner claim mismatch"));
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn slatedb_state_store_reads_checkpoint_versioned_state_payloads() {
    let (_temp_dir, store) = temp_store();
    let state_store = SlateDbStateStore::open("v1/slatedb/state", Arc::clone(&store))
        .await
        .unwrap();
    let state = state_write(7, "state-0007", b"slatedb-state");
    let written_ref = state_store.write_state_object(&state).await.unwrap();

    assert_eq!(written_ref, state_ref(&state));
    assert!(state_store.state_object_exists(&written_ref).await.unwrap());
    assert_eq!(
        state_store.read_state_object(&written_ref).await.unwrap(),
        Bytes::from_static(b"slatedb-state")
    );
}

#[tokio::test]
async fn checkpoint_publish_slatedb_state_store_keeps_manifests_authoritative() {
    let (_temp_dir, store) = temp_store();
    let publisher =
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap();
    let published_state = state_write(0, "state-0001", b"published-state");
    let published_ref = publisher
        .write_state_object(&published_state)
        .await
        .unwrap();
    let published_manifest = manifest(0, published_ref.clone());
    publisher
        .publish_manifest(&published_manifest)
        .await
        .unwrap();

    let orphan_state = state_write(1, "state-0002", b"orphan-state");
    let orphan_ref = publisher.write_state_object(&orphan_state).await.unwrap();

    assert_eq!(
        publisher.read_state_object(&orphan_ref).await.unwrap(),
        Bytes::from_static(b"orphan-state")
    );
    assert_eq!(
        publisher.latest_manifest().await.unwrap(),
        Some(published_manifest.clone())
    );
    assert_eq!(
        publisher
            .read_state_object(&published_manifest.state_objects[0])
            .await
            .unwrap(),
        Bytes::from_static(b"published-state")
    );
}

#[tokio::test]
async fn checkpoint_publish_slatedb_state_store_rejects_duplicate_state_object_write() {
    let (_temp_dir, store) = temp_store();
    let publisher =
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap();
    let state = state_write(0, "state-0001", b"first");

    publisher.write_state_object(&state).await.unwrap();
    let duplicate = state_write(0, "state-0001", b"second");
    let err = publisher.write_state_object(&duplicate).await.unwrap_err();

    assert!(err.to_string().contains("already exists"));
    assert_eq!(
        publisher
            .read_state_object(&state_ref(&state))
            .await
            .unwrap(),
        Bytes::from_static(b"first")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_publish_slatedb_state_store_rejects_concurrent_duplicate_state_object_writes() {
    let (_temp_dir, store) = temp_store();
    let publisher = Arc::new(
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap(),
    );
    let barrier = Arc::new(Barrier::new(16));

    let handles = (0..16)
        .map(|attempt| {
            let publisher = Arc::clone(&publisher);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                let state =
                    state_write_bytes(0, "state-0001", Bytes::from(format!("payload-{attempt}")));
                barrier.wait().await;
                let result = publisher.write_state_object(&state).await;
                (state, result)
            })
        })
        .collect::<Vec<_>>();

    let results = futures::future::try_join_all(handles).await.unwrap();
    let successes = results
        .iter()
        .filter(|(_, result)| result.is_ok())
        .collect::<Vec<_>>();
    let duplicates = results
        .iter()
        .filter(|(_, result)| {
            result
                .as_ref()
                .is_err_and(|err| err.to_string().contains("already exists"))
        })
        .count();

    assert_eq!(successes.len(), 1);
    assert_eq!(duplicates, 15);

    let (winning_state, winning_ref) = successes[0];
    assert_eq!(winning_ref.as_ref().unwrap(), &state_ref(winning_state));
    assert_eq!(
        publisher
            .read_state_object(winning_ref.as_ref().unwrap())
            .await
            .unwrap(),
        winning_state.bytes().clone()
    );
}

#[tokio::test]
async fn checkpoint_publish_slatedb_state_store_rejects_manifest_that_references_missing_state_object(
) {
    let (_temp_dir, store) = temp_store();
    let publisher =
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap();
    let missing_state = state_write(0, "missing-state", b"not-written");
    let manifest = manifest(0, state_ref(&missing_state));

    let err = publisher.publish_manifest(&manifest).await.unwrap_err();

    assert!(err.to_string().contains("referenced state object"));
    let manifest_path = Path::from(manifest.object_key().as_str());
    assert!(store.head(&manifest_path).await.is_err());
}
