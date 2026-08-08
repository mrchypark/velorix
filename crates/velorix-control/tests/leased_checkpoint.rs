use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore, ObjectStoreExt};
use tempfile::TempDir;
use velorix_control::lease::{PartitionLeaseGrant, PartitionLeaseKey};
use velorix_control::leased_checkpoint::{LeasedCheckpointError, LeasedCheckpointPublisher};
use velorix_storage::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
        AuthoritativeObjectStoreCapabilityError, ObjectStoreCapabilityProfile,
    },
    manifest::{
        CheckpointManifest, InputRange, OutputObjectRef, PartitionOwnerClaim, StateObjectRef,
        StateRefType,
    },
    ownership::OwnershipEpochRecord,
    state::{
        CheckpointPublishError, CheckpointPublisher, FencedOutputObjectWriteRequest,
        OutputObjectWrite, StateObjectWrite,
    },
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

fn all_namespace_capabilities() -> AuthoritativeObjectStoreCapabilitiesV1 {
    let profile = ObjectStoreCapabilityProfile::local_development();
    AuthoritativeObjectStoreCapabilitiesV1::new(
        AuthoritativeNamespace::all()
            .into_iter()
            .map(|namespace| (namespace, profile.clone()))
            .collect::<BTreeMap<_, _>>(),
    )
}

fn capabilities_missing(
    namespace: AuthoritativeNamespace,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    let mut profiles = all_namespace_capabilities().profiles;
    profiles.remove(&namespace);
    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}

fn capabilities_with_weak_namespace(
    namespace: AuthoritativeNamespace,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    let mut profile = ObjectStoreCapabilityProfile::local_development();
    profile.conditional_create = false;
    let mut profiles = all_namespace_capabilities().profiles;
    profiles.insert(namespace, profile);
    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}

async fn checked_production_leased_publisher(
    store: Arc<dyn ObjectStore>,
) -> LeasedCheckpointPublisher {
    LeasedCheckpointPublisher::with_slatedb_state_store_authoritative(
        store,
        "v1/slatedb/state",
        &all_namespace_capabilities(),
    )
    .await
    .unwrap()
}

fn assert_missing_namespace(err: LeasedCheckpointError, namespace: AuthoritativeNamespace) {
    assert!(matches!(
        err,
        LeasedCheckpointError::Publish(
            CheckpointPublishError::AuthoritativeObjectStoreCapabilities(
                AuthoritativeObjectStoreCapabilityError::MissingNamespace { namespace: actual }
            )
        ) if actual == namespace
    ));
}

fn assert_weak_namespace(err: LeasedCheckpointError, namespace: AuthoritativeNamespace) {
    assert!(matches!(
        err,
        LeasedCheckpointError::Publish(
            CheckpointPublishError::AuthoritativeObjectStoreCapabilities(
                AuthoritativeObjectStoreCapabilityError::NamespaceProfile {
                    namespace: actual,
                    ..
                }
            )
        ) if actual == namespace
    ));
}

fn grant(owner_id: &str, owner_epoch: u64, expires_at_unix_ms: u64) -> PartitionLeaseGrant {
    PartitionLeaseGrant {
        key: PartitionLeaseKey {
            namespace: "default".to_string(),
            view_id: "balances".to_string(),
            stream_id: "orders".to_string(),
            partition_id: 0,
        },
        owner_id: owner_id.to_string(),
        owner_epoch,
        expires_at_unix_ms,
    }
}

fn owner_claim(owner_id: &str, owner_epoch: u64) -> PartitionOwnerClaim {
    PartitionOwnerClaim {
        owner_id: owner_id.to_string(),
        owner_epoch,
    }
}

fn state_write(
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

fn output_write(
    partition_id: u32,
    checkpoint_version: u64,
    object_id: &str,
    owner_claim: PartitionOwnerClaim,
    bytes: &'static [u8],
) -> OutputObjectWrite {
    OutputObjectWrite::new_fenced(FencedOutputObjectWriteRequest {
        stream_id: "settlements".to_string(),
        partition_id,
        checkpoint_version,
        start_offset_inclusive: 20,
        end_offset_exclusive: 25,
        object_id: object_id.to_string(),
        owner_claim,
        bytes: Bytes::from_static(bytes),
    })
    .unwrap()
}

fn state_ref(state: &StateObjectWrite) -> StateObjectRef {
    StateObjectRef {
        object_id: state.object_id().to_string(),
        object_key: state.object_key().clone(),
        owner: state.owner().to_string(),
        partition_id: state.partition_id(),
        checkpoint_version: state.checkpoint_version(),
        ref_type: StateRefType::RawObject,
        slatedb: None,
        owner_claim: state.owner_claim().cloned(),
    }
}

fn slatedb_state_ref(state: &StateObjectWrite) -> StateObjectRef {
    let mut state_ref = state_ref(state);
    state_ref.ref_type = StateRefType::SlateDbCheckpoint;
    state_ref
}

fn output_ref(output: &OutputObjectWrite) -> OutputObjectRef {
    OutputObjectRef {
        object_id: output.object_id().to_string(),
        object_key: output.object_key().clone(),
        stream_id: output.stream_id().to_string(),
        partition_id: output.partition_id(),
        checkpoint_version: output.checkpoint_version(),
        start_offset_inclusive: output.start_offset_inclusive(),
        end_offset_exclusive: output.end_offset_exclusive(),
        owner_claim: output.owner_claim().cloned(),
    }
}

fn ownership_record(
    stream_id: &str,
    partition_id: u32,
    owner_id: &str,
    owner_epoch: u64,
) -> OwnershipEpochRecord {
    OwnershipEpochRecord {
        stream_id: stream_id.to_string(),
        partition_id,
        owner_id: owner_id.to_string(),
        owner_epoch,
        lease_identity: format!("{owner_id}-lease"),
        created_at: "2026-05-05T00:00:00Z".to_string(),
        previous_epoch: owner_epoch.checked_sub(1),
        previous_checkpoint_version: owner_epoch.checked_sub(1),
    }
}

fn input_range(stream_id: &str, partition_id: u32) -> InputRange {
    input_range_offsets(stream_id, partition_id, 0, 10)
}

fn input_range_offsets(
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

fn manifest(
    checkpoint_version: u64,
    input_ranges: Vec<InputRange>,
    state_objects: Vec<StateObjectRef>,
    output_objects: Vec<OutputObjectRef>,
) -> CheckpointManifest {
    CheckpointManifest {
        schema_version: 1,
        checkpoint_version,
        input_ranges,
        state_objects,
        output_objects,
        parent_checkpoint: checkpoint_version.checked_sub(1),
        created_at: "2026-05-04T00:00:00Z".to_string(),
    }
}

#[tokio::test]
async fn leased_checkpoint_publishes_fenced_objects_and_manifest_when_grant_is_valid() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let leased = LeasedCheckpointPublisher::new(publisher.clone());
    let grant = grant("worker-a", 7, 2_000);
    let claim = owner_claim("worker-a", 7);
    let state = state_write(0, 0, "state-0001", claim.clone(), b"state");
    let output = output_write(0, 0, "out-0001", claim.clone(), b"output");
    let manifest = manifest(
        0,
        vec![input_range("orders", 0)],
        vec![state_ref(&state)],
        vec![output_ref(&output)],
    );

    leased
        .publish(
            grant,
            1_000,
            vec![state.clone()],
            vec![output],
            manifest.clone(),
        )
        .await
        .unwrap();

    let latest = publisher.latest_manifest().await.unwrap().unwrap();
    assert_eq!(latest.checkpoint_version, manifest.checkpoint_version);
    assert_eq!(latest.input_ranges, manifest.input_ranges);
    assert_eq!(latest.output_objects, manifest.output_objects);
    assert!(latest
        .state_objects
        .iter()
        .all(|state_ref| state_ref.owner_claim.as_ref() == Some(&claim)));
    assert!(latest
        .output_objects
        .iter()
        .all(|output_ref| output_ref.owner_claim.as_ref() == Some(&claim)));
    assert_eq!(
        publisher
            .read_state_object(&latest.state_objects[0])
            .await
            .unwrap(),
        state.bytes().clone()
    );
}

#[tokio::test]
async fn leased_checkpoint_rejects_expired_grant_before_durable_writes() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let leased = LeasedCheckpointPublisher::new(publisher.clone());
    let grant = grant("worker-a", 7, 1_000);
    let claim = owner_claim("worker-a", 7);
    let state = state_write(0, 0, "state-0001", claim.clone(), b"state");
    let output = output_write(0, 0, "out-0001", claim, b"output");
    let manifest = manifest(
        0,
        vec![input_range("orders", 0)],
        vec![state_ref(&state)],
        vec![output_ref(&output)],
    );

    let err = leased
        .publish(
            grant,
            1_000,
            vec![state.clone()],
            vec![output.clone()],
            manifest,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        LeasedCheckpointError::ExpiredGrant {
            expires_at_unix_ms: 1_000,
            now_unix_ms: 1_000
        }
    ));
    assert_eq!(publisher.latest_manifest().await.unwrap(), None);
    assert!(store
        .head(&Path::from(state.object_key().as_str()))
        .await
        .is_err());
    assert!(store
        .head(&Path::from(output.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn leased_checkpoint_rejects_grant_stream_mismatch_before_durable_writes() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let leased = LeasedCheckpointPublisher::new(publisher.clone());
    let grant = grant("worker-a", 7, 2_000);
    let claim = owner_claim("worker-a", 7);
    let state = state_write(0, 0, "state-0001", claim.clone(), b"state");
    let output = output_write(0, 0, "out-0001", claim, b"output");
    let manifest = manifest(
        0,
        vec![input_range("payments", 0)],
        vec![state_ref(&state)],
        vec![output_ref(&output)],
    );

    let err = leased
        .publish(
            grant,
            1_000,
            vec![state.clone()],
            vec![output.clone()],
            manifest,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        LeasedCheckpointError::InputStreamMismatch {
            expected,
            actual
        } if expected == "orders" && actual == "payments"
    ));
    assert_eq!(publisher.latest_manifest().await.unwrap(), None);
    assert!(store
        .head(&Path::from(state.object_key().as_str()))
        .await
        .is_err());
    assert!(store
        .head(&Path::from(output.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn leased_checkpoint_rejects_grant_partition_mismatch_before_durable_writes() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let leased = LeasedCheckpointPublisher::new(publisher.clone());
    let grant = grant("worker-a", 7, 2_000);
    let claim = owner_claim("worker-a", 7);
    let state = state_write(1, 0, "state-0001", claim.clone(), b"state");
    let output = output_write(1, 0, "out-0001", claim, b"output");
    let manifest = manifest(
        0,
        vec![input_range("orders", 1)],
        vec![state_ref(&state)],
        vec![output_ref(&output)],
    );

    let err = leased
        .publish(
            grant,
            1_000,
            vec![state.clone()],
            vec![output.clone()],
            manifest,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        LeasedCheckpointError::PartitionMismatch {
            kind: "manifest",
            expected: 0,
            actual: 1,
            ..
        }
    ));
    assert_eq!(publisher.latest_manifest().await.unwrap(), None);
    assert!(store
        .head(&Path::from(state.object_key().as_str()))
        .await
        .is_err());
    assert!(store
        .head(&Path::from(output.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn leased_checkpoint_rejects_multi_partition_manifest_explicitly() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let leased = LeasedCheckpointPublisher::new(publisher.clone());
    let grant = grant("worker-a", 7, 2_000);
    let claim = owner_claim("worker-a", 7);
    let state = state_write(0, 0, "state-0001", claim.clone(), b"state");
    let output = output_write(0, 0, "out-0001", claim, b"output");
    let manifest = manifest(
        0,
        vec![input_range("orders", 0), input_range("orders", 1)],
        vec![state_ref(&state)],
        vec![output_ref(&output)],
    );

    let err = leased
        .publish(
            grant,
            1_000,
            vec![state.clone()],
            vec![output.clone()],
            manifest,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        LeasedCheckpointError::MultiPartitionManifest { partitions }
            if partitions == vec![0, 1]
    ));
    assert_eq!(publisher.latest_manifest().await.unwrap(), None);
    assert!(store
        .head(&Path::from(state.object_key().as_str()))
        .await
        .is_err());
    assert!(store
        .head(&Path::from(output.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn leased_checkpoint_rejects_missing_parent_before_durable_writes() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let leased = LeasedCheckpointPublisher::new(publisher.clone());
    let grant = grant("worker-a", 7, 2_000);
    let claim = owner_claim("worker-a", 7);
    let state = state_write(0, 1, "state-0002", claim.clone(), b"state");
    let output = output_write(0, 1, "out-0002", claim, b"output");
    let manifest = manifest(
        1,
        vec![input_range("orders", 0)],
        vec![state_ref(&state)],
        vec![output_ref(&output)],
    );

    let err = leased
        .publish(
            grant,
            1_000,
            vec![state.clone()],
            vec![output.clone()],
            manifest,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        LeasedCheckpointError::Publish(CheckpointPublishError::MissingParentManifest {
            checkpoint_version: 1,
            parent_checkpoint: 0
        })
    ));
    assert_eq!(publisher.latest_manifest().await.unwrap(), None);
    assert!(store
        .head(&Path::from(state.object_key().as_str()))
        .await
        .is_err());
    assert!(store
        .head(&Path::from(output.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn leased_checkpoint_rejects_parent_input_regression_before_durable_writes() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let leased = LeasedCheckpointPublisher::new(publisher.clone());
    let parent_grant = grant("worker-a", 7, 2_000);
    let parent_claim = owner_claim("worker-a", 7);
    let parent_state = state_write(0, 0, "state-0001", parent_claim, b"parent");
    let parent_manifest = manifest(
        0,
        vec![input_range_offsets("orders", 0, 0, 10)],
        vec![state_ref(&parent_state)],
        vec![],
    );
    leased
        .publish(
            parent_grant,
            1_000,
            vec![parent_state],
            vec![],
            parent_manifest.clone(),
        )
        .await
        .unwrap();

    let child_grant = grant("worker-a", 7, 2_000);
    let child_claim = owner_claim("worker-a", 7);
    let child_state = state_write(0, 1, "state-0002", child_claim, b"child");
    let child_manifest = manifest(
        1,
        vec![input_range_offsets("orders", 0, 0, 9)],
        vec![state_ref(&child_state)],
        vec![],
    );

    let err = leased
        .publish(
            child_grant,
            1_000,
            vec![child_state.clone()],
            vec![],
            child_manifest,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        LeasedCheckpointError::Publish(CheckpointPublishError::RegressedParentInputBoundary {
            checkpoint_version: 1,
            parent_checkpoint: 0,
            stream_id,
            partition_id: 0,
            parent_start_offset_inclusive: 0,
            parent_end_offset_exclusive: 10,
            child_start_offset_inclusive: 0,
            child_end_offset_exclusive: 9,
        }) if stream_id == "orders"
    ));
    assert_eq!(
        publisher.latest_manifest().await.unwrap(),
        Some(parent_manifest)
    );
    assert!(store
        .head(&Path::from(child_state.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn leased_checkpoint_rejects_stale_lower_epoch_grant_through_storage_fence() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let leased = LeasedCheckpointPublisher::new(publisher.clone());
    let current_grant = grant("worker-b", 2, 2_000);
    let current_claim = owner_claim("worker-b", 2);
    let current_state = state_write(0, 0, "state-0001", current_claim.clone(), b"current");
    let current_manifest = manifest(
        0,
        vec![input_range("orders", 0)],
        vec![state_ref(&current_state)],
        vec![],
    );
    leased
        .publish(
            current_grant,
            1_000,
            vec![current_state],
            vec![],
            current_manifest.clone(),
        )
        .await
        .unwrap();

    let stale_grant = grant("worker-a", 1, 3_000);
    let stale_claim = owner_claim("worker-a", 1);
    let stale_state = state_write(0, 1, "state-0002", stale_claim, b"stale");
    let stale_manifest = manifest(
        1,
        vec![input_range("orders", 0)],
        vec![state_ref(&stale_state)],
        vec![],
    );

    let err = leased
        .publish(
            stale_grant,
            1_000,
            vec![stale_state.clone()],
            vec![],
            stale_manifest,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        LeasedCheckpointError::Publish(CheckpointPublishError::StaleOwnerClaim { .. })
    ));
    assert_eq!(
        publisher.latest_manifest().await.unwrap(),
        Some(current_manifest)
    );
    assert!(store
        .head(&Path::from(stale_state.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn leased_checkpoint_production_constructor_rejects_missing_output_capabilities_before_writes(
) {
    let (_temp_dir, store) = temp_store();

    let err = LeasedCheckpointPublisher::with_slatedb_state_store_authoritative(
        Arc::clone(&store),
        "v1/slatedb/state",
        &capabilities_missing(AuthoritativeNamespace::Output),
    )
    .await
    .unwrap_err();

    assert_missing_namespace(err, AuthoritativeNamespace::Output);
    let objects = store.list(None).try_collect::<Vec<_>>().await.unwrap();
    assert!(objects.is_empty());
}

#[tokio::test]
async fn leased_checkpoint_production_constructor_rejects_weak_output_capabilities_before_writes() {
    let (_temp_dir, store) = temp_store();

    let err = LeasedCheckpointPublisher::with_slatedb_state_store_authoritative(
        Arc::clone(&store),
        "v1/slatedb/state",
        &capabilities_with_weak_namespace(AuthoritativeNamespace::Output),
    )
    .await
    .unwrap_err();

    assert_weak_namespace(err, AuthoritativeNamespace::Output);
    let objects = store.list(None).try_collect::<Vec<_>>().await.unwrap();
    assert!(objects.is_empty());
}

#[tokio::test]
async fn leased_checkpoint_production_constructor_rejects_missing_ownership_capabilities_before_writes(
) {
    let (_temp_dir, store) = temp_store();

    let err = LeasedCheckpointPublisher::with_slatedb_state_store_authoritative(
        Arc::clone(&store),
        "v1/slatedb/state",
        &capabilities_missing(AuthoritativeNamespace::Ownership),
    )
    .await
    .unwrap_err();

    assert_missing_namespace(err, AuthoritativeNamespace::Ownership);
    let objects = store.list(None).try_collect::<Vec<_>>().await.unwrap();
    assert!(objects.is_empty());
}

#[tokio::test]
async fn leased_checkpoint_production_constructor_rejects_weak_ownership_capabilities_before_writes(
) {
    let (_temp_dir, store) = temp_store();

    let err = LeasedCheckpointPublisher::with_slatedb_state_store_authoritative(
        Arc::clone(&store),
        "v1/slatedb/state",
        &capabilities_with_weak_namespace(AuthoritativeNamespace::Ownership),
    )
    .await
    .unwrap_err();

    assert_weak_namespace(err, AuthoritativeNamespace::Ownership);
    let objects = store.list(None).try_collect::<Vec<_>>().await.unwrap();
    assert!(objects.is_empty());
}

#[tokio::test]
async fn leased_checkpoint_production_rejects_missing_ownership_record_before_durable_writes() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let leased = checked_production_leased_publisher(Arc::clone(&store)).await;
    let grant = grant("worker-a", 7, 2_000);
    let claim = owner_claim("worker-a", 7);
    let state = state_write(0, 0, "state-0001", claim.clone(), b"state");
    let output = output_write(0, 0, "out-0001", claim, b"output");
    let manifest = manifest(
        0,
        vec![input_range("orders", 0)],
        vec![slatedb_state_ref(&state)],
        vec![output_ref(&output)],
    );

    let err = leased
        .publish_production(
            grant,
            1_000,
            vec![state.clone()],
            vec![output.clone()],
            manifest.clone(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        LeasedCheckpointError::Publish(CheckpointPublishError::MissingOwnershipEpochRecord(_))
    ));
    assert_eq!(publisher.latest_manifest().await.unwrap(), None);
    assert!(publisher
        .read_state_object(&manifest.state_objects[0])
        .await
        .is_err());
    assert!(store
        .head(&Path::from(output.object_key().as_str()))
        .await
        .is_err());
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn leased_checkpoint_production_rejects_stale_lower_epoch_grant_after_newer_durable_epoch_record(
) {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let leased = checked_production_leased_publisher(Arc::clone(&store)).await;
    let stale_grant = grant("worker-a", 1, 2_000);
    let stale_claim = owner_claim("worker-a", 1);
    let current_claim = owner_claim("worker-b", 2);
    publisher
        .create_ownership_epoch_record(&ownership_record("orders", 0, "worker-a", 1))
        .await
        .unwrap();
    publisher
        .create_ownership_epoch_record(&ownership_record("orders", 0, "worker-b", 2))
        .await
        .unwrap();
    publisher
        .create_ownership_epoch_record(&ownership_record("settlements", 0, "worker-a", 1))
        .await
        .unwrap();
    publisher
        .create_ownership_epoch_record(&ownership_record("settlements", 0, "worker-b", 2))
        .await
        .unwrap();
    let state = state_write(0, 0, "state-0001", stale_claim.clone(), b"state");
    let output = output_write(0, 0, "out-0001", stale_claim, b"output");
    let manifest = manifest(
        0,
        vec![input_range("orders", 0)],
        vec![slatedb_state_ref(&state)],
        vec![output_ref(&output)],
    );

    let err = leased
        .publish_production(
            stale_grant,
            1_000,
            vec![state.clone()],
            vec![output.clone()],
            manifest.clone(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        LeasedCheckpointError::Publish(CheckpointPublishError::StaleOwnerClaim {
            partition_id: 0,
            current,
            attempted
        }) if current == current_claim && attempted == owner_claim("worker-a", 1)
    ));
    assert_eq!(publisher.latest_manifest().await.unwrap(), None);
    assert!(publisher
        .read_state_object(&manifest.state_objects[0])
        .await
        .is_err());
    assert!(store
        .head(&Path::from(output.object_key().as_str()))
        .await
        .is_err());
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn leased_checkpoint_production_rejects_unchecked_slatedb_publisher_before_durable_writes() {
    let (_temp_dir, store) = temp_store();
    let publisher =
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap();
    let leased = LeasedCheckpointPublisher::new(publisher.clone());
    let grant = grant("worker-a", 7, 2_000);
    let claim = owner_claim("worker-a", 7);
    publisher
        .create_ownership_epoch_record(&ownership_record("orders", 0, "worker-a", 7))
        .await
        .unwrap();
    publisher
        .create_ownership_epoch_record(&ownership_record("settlements", 0, "worker-a", 7))
        .await
        .unwrap();
    let state = state_write(0, 0, "state-0001", claim.clone(), b"state");
    let output = output_write(0, 0, "out-0001", claim, b"output");
    let manifest = manifest(
        0,
        vec![input_range("orders", 0)],
        vec![slatedb_state_ref(&state)],
        vec![output_ref(&output)],
    );

    let err = leased
        .publish_production(
            grant,
            1_000,
            vec![state.clone()],
            vec![output.clone()],
            manifest.clone(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        LeasedCheckpointError::MissingProductionAuthorityEvidence
    ));
    assert_eq!(publisher.latest_manifest().await.unwrap(), None);
    assert!(publisher
        .read_state_object(&manifest.state_objects[0])
        .await
        .is_err());
    assert!(store
        .head(&Path::from(output.object_key().as_str()))
        .await
        .is_err());
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn leased_checkpoint_production_rejects_unchecked_raw_state_publisher_before_durable_writes()
{
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let leased = LeasedCheckpointPublisher::new(publisher.clone());
    let grant = grant("worker-a", 7, 2_000);
    let claim = owner_claim("worker-a", 7);
    publisher
        .create_ownership_epoch_record(&ownership_record("orders", 0, "worker-a", 7))
        .await
        .unwrap();
    publisher
        .create_ownership_epoch_record(&ownership_record("settlements", 0, "worker-a", 7))
        .await
        .unwrap();
    let state = state_write(0, 0, "state-0001", claim.clone(), b"state");
    let output = output_write(0, 0, "out-0001", claim, b"output");
    let manifest = manifest(
        0,
        vec![input_range("orders", 0)],
        vec![slatedb_state_ref(&state)],
        vec![output_ref(&output)],
    );

    let err = leased
        .publish_production(
            grant,
            1_000,
            vec![state.clone()],
            vec![output.clone()],
            manifest.clone(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        LeasedCheckpointError::MissingProductionAuthorityEvidence
    ));
    assert_eq!(publisher.latest_manifest().await.unwrap(), None);
    assert!(store
        .head(&Path::from(state.object_key().as_str()))
        .await
        .is_err());
    assert!(store
        .head(&Path::from(output.object_key().as_str()))
        .await
        .is_err());
    assert!(store
        .head(&Path::from(manifest.object_key().as_str()))
        .await
        .is_err());
}

#[tokio::test]
async fn leased_checkpoint_production_publishes_with_slatedb_refs_and_ownership_records() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let leased = checked_production_leased_publisher(Arc::clone(&store)).await;
    let grant = grant("worker-a", 7, 2_000);
    let claim = owner_claim("worker-a", 7);
    publisher
        .create_ownership_epoch_record(&ownership_record("orders", 0, "worker-a", 7))
        .await
        .unwrap();
    publisher
        .create_ownership_epoch_record(&ownership_record("settlements", 0, "worker-a", 7))
        .await
        .unwrap();
    let state = state_write(0, 0, "state-0001", claim.clone(), b"state");
    let output = output_write(0, 0, "out-0001", claim.clone(), b"output");
    let manifest = manifest(
        0,
        vec![input_range("orders", 0)],
        vec![slatedb_state_ref(&state)],
        vec![output_ref(&output)],
    );

    leased
        .publish_production(
            grant,
            1_000,
            vec![state.clone()],
            vec![output],
            manifest.clone(),
        )
        .await
        .unwrap();

    let latest_bytes = store
        .get(&Path::from(manifest.object_key().as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let latest: CheckpointManifest = serde_json::from_slice(&latest_bytes).unwrap();
    assert_eq!(latest.checkpoint_version, manifest.checkpoint_version);
    assert_eq!(latest.input_ranges, manifest.input_ranges);
    assert_eq!(latest.output_objects, manifest.output_objects);
    assert_eq!(
        latest.state_objects[0].ref_type,
        StateRefType::SlateDbCheckpoint
    );
    let checked_reader = CheckpointPublisher::with_slatedb_state_store_authoritative(
        Arc::clone(&store),
        "v1/slatedb/state",
        &all_namespace_capabilities(),
    )
    .await
    .unwrap();
    assert_eq!(
        checked_reader
            .read_state_object(&latest.state_objects[0])
            .await
            .unwrap(),
        state.bytes().clone()
    );
}

#[tokio::test]
async fn leased_checkpoint_bootstrap_publish_still_accepts_raw_state_refs() {
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let leased = LeasedCheckpointPublisher::new(publisher.clone());
    let grant = grant("worker-a", 7, 2_000);
    let claim = owner_claim("worker-a", 7);
    let state = state_write(0, 0, "state-0001", claim.clone(), b"state");
    let manifest = manifest(
        0,
        vec![input_range("orders", 0)],
        vec![state_ref(&state)],
        vec![],
    );

    leased
        .publish(grant, 1_000, vec![state], vec![], manifest.clone())
        .await
        .unwrap();

    assert_eq!(publisher.latest_manifest().await.unwrap(), Some(manifest));
}
