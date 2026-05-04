use std::sync::Arc;

use bytes::Bytes;
use object_store::{local::LocalFileSystem, ObjectStore};
use tempfile::TempDir;
use velorix_control::lease::{
    InMemoryPartitionLeaseClient, LeaseAcquireRequest, LeaseError, PartitionLeaseClient,
    PartitionLeaseGrant, PartitionLeaseKey,
};
use velorix_storage::{
    manifest::{CheckpointManifest, InputRange, PartitionOwnerClaim},
    state::{CheckpointPublisher, OutputObjectWrite, StateObjectWrite},
};

fn lease_key() -> PartitionLeaseKey {
    PartitionLeaseKey {
        namespace: "default".to_string(),
        view_id: "balances_by_account".to_string(),
        stream_id: "orders".to_string(),
        partition_id: 0,
    }
}

fn acquire_request(owner_id: &str, now_unix_ms: u64, ttl_ms: u64) -> LeaseAcquireRequest {
    LeaseAcquireRequest {
        key: lease_key(),
        owner_id: owner_id.to_string(),
        now_unix_ms,
        ttl_ms,
    }
}

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

#[tokio::test]
async fn partition_lease_renews_without_epoch_change_when_same_owner_before_expiry() {
    let client = InMemoryPartitionLeaseClient::default();

    let first = client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 500))
        .await
        .unwrap();
    let renewed = client
        .acquire_or_renew(acquire_request("worker-a", 1_200, 800))
        .await
        .unwrap();

    assert_eq!(first.owner_epoch, 1);
    assert_eq!(renewed.owner_id, "worker-a");
    assert_eq!(renewed.owner_epoch, first.owner_epoch);
    assert_eq!(renewed.expires_at_unix_ms, 2_000);
}

#[tokio::test]
async fn partition_lease_rejects_conflicting_owner_when_existing_grant_is_unexpired() {
    let client = InMemoryPartitionLeaseClient::default();
    let first = client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 500))
        .await
        .unwrap();

    let err = client
        .acquire_or_renew(acquire_request("worker-b", 1_100, 500))
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        LeaseError::Conflict { current } if current == first
    ));
    assert_eq!(
        client.current(&lease_key(), 1_100).await.unwrap(),
        Some(first)
    );
}

#[tokio::test]
async fn partition_lease_acquires_with_higher_epoch_when_different_owner_waits_until_expiry() {
    let client = InMemoryPartitionLeaseClient::default();
    let first = client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 500))
        .await
        .unwrap();

    let second = client
        .acquire_or_renew(acquire_request("worker-b", 1_500, 500))
        .await
        .unwrap();

    assert_eq!(second.owner_id, "worker-b");
    assert!(second.owner_epoch > first.owner_epoch);
    assert_eq!(second.owner_epoch, 2);
}

#[tokio::test]
async fn partition_lease_release_fails_closed_when_caller_is_not_current_holder() {
    let client = InMemoryPartitionLeaseClient::default();
    let grant = client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 500))
        .await
        .unwrap();

    let err = client
        .release(&grant.key, "worker-b", grant.owner_epoch, 1_100)
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        LeaseError::NotLeaseHolder { current } if current == grant
    ));
    assert_eq!(
        client.current(&lease_key(), 1_100).await.unwrap(),
        Some(grant)
    );
}

#[tokio::test]
async fn partition_lease_preserves_epoch_history_after_holder_release() {
    let client = InMemoryPartitionLeaseClient::default();
    let first = client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 500))
        .await
        .unwrap();

    client
        .release(&first.key, "worker-a", first.owner_epoch, 1_100)
        .await
        .unwrap();

    assert_eq!(client.current(&lease_key(), 1_100).await.unwrap(), None);

    let second = client
        .acquire_or_renew(acquire_request("worker-b", 1_200, 500))
        .await
        .unwrap();

    assert_eq!(first.owner_epoch, 1);
    assert_eq!(second.owner_id, "worker-b");
    assert!(second.owner_epoch > first.owner_epoch);
    assert_ne!(second.owner_epoch, 1);
}

#[tokio::test]
async fn partition_lease_release_fails_closed_when_same_owner_uses_stale_epoch() {
    let client = InMemoryPartitionLeaseClient::default();
    let first = client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 100))
        .await
        .unwrap();
    let current = client
        .acquire_or_renew(acquire_request("worker-a", 1_100, 500))
        .await
        .unwrap();

    let err = client
        .release(&first.key, "worker-a", first.owner_epoch, 1_200)
        .await
        .unwrap_err();

    assert_eq!(first.owner_epoch, 1);
    assert!(current.owner_epoch > first.owner_epoch);
    assert!(matches!(
        err,
        LeaseError::NotLeaseHolder { current: holder } if holder == current
    ));
    assert_eq!(
        client.current(&lease_key(), 1_200).await.unwrap(),
        Some(current)
    );
}

#[tokio::test]
async fn partition_lease_never_issues_same_epoch_to_different_owner() {
    let client = InMemoryPartitionLeaseClient::default();
    let first = client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 100))
        .await
        .unwrap();
    let second = client
        .acquire_or_renew(acquire_request("worker-b", 1_100, 100))
        .await
        .unwrap();

    assert_ne!(first.owner_id, second.owner_id);
    assert!(second.owner_epoch > first.owner_epoch);
}

#[tokio::test]
async fn partition_lease_rejects_invalid_ttl_owner_and_key_fields() {
    let client = InMemoryPartitionLeaseClient::default();

    let ttl_err = client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 0))
        .await
        .unwrap_err();
    assert!(matches!(ttl_err, LeaseError::InvalidTtl { ttl_ms: 0 }));

    let owner_err = client
        .acquire_or_renew(acquire_request(" ", 1_000, 500))
        .await
        .unwrap_err();
    assert!(matches!(owner_err, LeaseError::InvalidOwnerId));

    let key_err = client
        .acquire_or_renew(LeaseAcquireRequest {
            key: PartitionLeaseKey {
                namespace: String::new(),
                view_id: "balances_by_account".to_string(),
                stream_id: "orders".to_string(),
                partition_id: 0,
            },
            owner_id: "worker-a".to_string(),
            now_unix_ms: 1_000,
            ttl_ms: 500,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        key_err,
        LeaseError::InvalidLeaseKey { field: "namespace" }
    ));
}

#[tokio::test]
async fn partition_lease_grant_owner_claim_fences_storage_publication_when_used_with_checkpoint_publisher(
) {
    let client = InMemoryPartitionLeaseClient::default();
    let grant = client
        .acquire_or_renew(acquire_request("worker-a", 1_000, 500))
        .await
        .unwrap();
    let owner_claim: PartitionOwnerClaim = grant.clone().into();
    let (_temp_dir, store) = temp_store();
    let publisher = CheckpointPublisher::new(store);
    let state = StateObjectWrite::new_fenced(
        "balances_by_account",
        0,
        0,
        "state-0001",
        owner_claim.clone(),
        Bytes::from_static(b"state"),
    )
    .unwrap();
    let output = OutputObjectWrite::new_fenced(
        "settlements",
        0,
        0,
        20,
        25,
        "out-0001",
        owner_claim.clone(),
        Bytes::from_static(b"output"),
    )
    .unwrap();

    let state_ref = publisher
        .write_state_object_fenced(&state, &owner_claim)
        .await
        .unwrap();
    let output_ref = publisher
        .write_output_object_fenced(&output, &owner_claim)
        .await
        .unwrap();
    let manifest = CheckpointManifest {
        schema_version: 1,
        checkpoint_version: 0,
        input_ranges: vec![InputRange {
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 10,
        }],
        state_objects: vec![state_ref],
        output_objects: vec![output_ref],
        parent_checkpoint: None,
        created_at: "2026-05-04T00:00:00Z".to_string(),
    };

    publisher
        .publish_manifest_fenced(&manifest, &owner_claim)
        .await
        .unwrap();

    assert_eq!(owner_claim.owner_id, grant.owner_id);
    assert_eq!(owner_claim.owner_epoch, grant.owner_epoch);
    assert_eq!(publisher.latest_manifest().await.unwrap(), Some(manifest));
}

#[allow(dead_code)]
fn assert_grant_is_storage_compatible(grant: PartitionLeaseGrant) -> PartitionOwnerClaim {
    grant.into()
}
