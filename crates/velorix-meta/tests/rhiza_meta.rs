#![cfg(feature = "rhiza-backend")]

use tempfile::tempdir;
use tokio::time::{sleep, Duration};
use velorix_meta::rhiza_kv::RhizaKvStore;
use velorix_meta::rhiza_kv_snapshot::RhizaKvSnapshot;
use velorix_meta::rhiza_meta::RhizaKvMetaStore;
use velorix_meta::{
    AcquirePartitionAuthorityOutcome, AcquirePartitionAuthorityRequest,
    AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest,
    CommitIngestRangeOutcome, IngestRangeReservation, MetaStore, PartitionAuthorityKey,
    PartitionCheckpointPointer, PublishPartitionCheckpointPointerRequest,
    PublishStandingRuntimeCheckpointOutcome, PublishStandingRuntimeCheckpointRequest,
    ReserveIngestRangeOutcome, StandingRuntimeCheckpointPointer, StandingRuntimeOwnerToken,
    StoreRelationCatalogOutcome,
};

mod common;

fn standing_runtime_checkpoint_pointer(
    epoch: u64,
    hash_seed: &str,
) -> StandingRuntimeCheckpointPointer {
    let hash = format!("{hash_seed:0<64}");
    StandingRuntimeCheckpointPointer {
        tenant_id: "tenant".to_string(),
        program_id: "program".to_string(),
        view_id: "view".to_string(),
        checkpoint_key: format!(
            "v1/standing-runtime-checkpoints/tenant/program/view/epochs/{epoch:020}/sha256/{hash}.checkpoint.json"
        ),
        logical_epoch: epoch,
        content_hash: format!("sha256:{hash}"),
        manifest_hash: format!("sha256:{hash}"),
        output_manifest_refs: Vec::new(),
        bootstrap_generation: 0,
        plan_hash: String::new(),
        coverage_hash: String::new(),
        input_coverage: None,
        previous_checkpoint_key: String::new(),
        previous_manifest_hash: String::new(),
    }
}

fn standing_runtime_owner_token(
    claim: velorix_meta::StandingRuntimeOwnerClaim,
) -> StandingRuntimeOwnerToken {
    StandingRuntimeOwnerToken {
        tenant_id: claim.tenant_id,
        program_id: claim.program_id,
        view_id: claim.view_id,
        owner_id: claim.owner_id,
        owner_epoch: claim.owner_epoch,
    }
}

fn reservation() -> IngestRangeReservation {
    IngestRangeReservation {
        stream_id: "orders-stream".into(),
        partition_id: 0,
        start_offset_inclusive: 0,
        end_offset_exclusive: 10,
        batch_key: "orders-batch-0".into(),
        payload_digest: "sha256:payload".into(),
        relation_id: "orders".into(),
        relation_version: "v1".into(),
        schema_fingerprint: "sha256:schema".into(),
        writer_epoch: 1,
    }
}

#[tokio::test]
async fn catalog_reserve_commit_and_reopen_are_durable() {
    let directory = tempdir().unwrap();
    let path = directory.path().display().to_string();
    let store = RhizaKvMetaStore::open(path.clone(), "meta-a")
        .await
        .unwrap();
    let capabilities = store.read_meta_store_capabilities().await.unwrap();
    assert_eq!(
        capabilities.standing_runtime_fencing.backend_name,
        "rhiza-kv"
    );
    assert!(
        capabilities
            .standing_runtime_fencing
            .durable_monotonic_owner_epoch
    );
    assert!(
        !capabilities
            .standing_runtime_fencing
            .authoritative_backend_time
    );
    assert!(
        !capabilities
            .standing_runtime_fencing
            .bounded_wall_clock_failover
    );
    assert!(
        !capabilities
            .standing_runtime_fencing
            .production_multi_writer_safe
    );
    assert!(capabilities.partition_authority.durable_across_restart);
    assert!(!capabilities.partition_authority.production_safe);
    let catalog = common::orders_relation_catalog("v1");

    assert_eq!(
        store.store_relation_catalog(catalog.clone()).await.unwrap(),
        StoreRelationCatalogOutcome::Created
    );
    let range = reservation();
    assert_eq!(
        store.reserve_ingest_range(range.clone()).await.unwrap(),
        ReserveIngestRangeOutcome::Reserved
    );
    assert_eq!(
        store.commit_ingest_range(range.clone()).await.unwrap(),
        CommitIngestRangeOutcome::Committed
    );
    drop(store);

    let reopened = RhizaKvMetaStore::open(path, "meta-a").await.unwrap();
    assert_eq!(
        reopened
            .read_relation_catalog("orders", "v1")
            .await
            .unwrap(),
        catalog
    );
    assert_eq!(
        reopened.reserve_ingest_range(range.clone()).await.unwrap(),
        ReserveIngestRangeOutcome::Duplicate
    );
    assert_eq!(
        reopened.commit_ingest_range(range).await.unwrap(),
        CommitIngestRangeOutcome::Duplicate
    );
}

#[tokio::test]
async fn concurrent_clients_single_native_node_fence_the_other() {
    let directory = tempdir().unwrap();
    let path = directory.path().display().to_string();
    let first = RhizaKvMetaStore::open(path, "meta-b").await.unwrap();
    // Rhiza currently permits one open WAL owner per directory; cloning the
    // facade gives independent concurrent clients over that single native node.
    let second = first.clone();
    let request = |owner: &str| AcquireStandingRuntimeOwnerRequest {
        tenant_id: "tenant".into(),
        program_id: "program".into(),
        view_id: "view".into(),
        owner_id: owner.into(),
        ttl_ms: 30_000,
    };

    let (left, right) = tokio::join!(
        first.acquire_standing_runtime_owner(request("owner-a")),
        second.acquire_standing_runtime_owner(request("owner-b")),
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AcquireStandingRuntimeOwnerOutcome::Acquired(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AcquireStandingRuntimeOwnerOutcome::Conflict(_)))
            .count(),
        1
    );
}

#[tokio::test]
async fn native_takeover_fences_stale_owner_and_reopens_with_new_epoch() {
    let directory = tempdir().unwrap();
    let path = directory.path().display().to_string();
    let store = RhizaKvMetaStore::open(path.clone(), "meta-takeover")
        .await
        .unwrap();
    let owner_a = match store
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: "tenant".into(),
            program_id: "program".into(),
            view_id: "view".into(),
            owner_id: "owner-a".into(),
            ttl_ms: 1,
        })
        .await
        .unwrap()
    {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim) => standing_runtime_owner_token(claim),
        other => panic!("unexpected initial owner outcome: {other:?}"),
    };

    sleep(Duration::from_millis(20)).await;
    let owner_b = match store
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: "tenant".into(),
            program_id: "program".into(),
            view_id: "view".into(),
            owner_id: "owner-b".into(),
            ttl_ms: 30_000,
        })
        .await
        .unwrap()
    {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim) => standing_runtime_owner_token(claim),
        other => panic!("expired owner did not permit takeover: {other:?}"),
    };
    assert_eq!(owner_b.owner_epoch, owner_a.owner_epoch + 1);

    let candidate = standing_runtime_checkpoint_pointer(1, "a");
    let stale = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: candidate.clone(),
            owner: owner_a,
        })
        .await;
    assert!(
        matches!(
            &stale,
            Err(velorix_meta::MetaStoreError::StandingRuntimeOwnerMismatch)
        ),
        "stale owner result: {stale:?}"
    );
    assert_eq!(
        store
            .read_standing_runtime_checkpoint("tenant", "program", "view")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: None,
                candidate: candidate.clone(),
                owner: owner_b.clone(),
            })
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );
    drop(store);

    let reopened = RhizaKvMetaStore::open(path, "meta-takeover").await.unwrap();
    let recovered_owner = reopened
        .read_standing_runtime_owner("tenant", "program", "view")
        .await
        .unwrap()
        .expect("new owner survives reopen");
    assert_eq!(recovered_owner.owner_epoch, owner_b.owner_epoch);
    assert_eq!(
        reopened
            .read_standing_runtime_checkpoint("tenant", "program", "view")
            .await
            .unwrap(),
        Some(candidate)
    );
}

#[tokio::test]
async fn stale_partition_token_is_rejected_without_publishing_checkpoint() {
    let directory = tempdir().unwrap();
    let kv = RhizaKvStore::open(directory.path().display().to_string(), "meta-c")
        .await
        .unwrap();
    let snapshot = RhizaKvSnapshot::new(kv.clone());
    let store = RhizaKvMetaStore::new(kv);
    let key = PartitionAuthorityKey {
        namespace: "ns".into(),
        view_id: "view".into(),
        stream_id: "stream".into(),
        partition_id: 0,
    };
    let token = match store
        .acquire_partition_authority(AcquirePartitionAuthorityRequest {
            key: key.clone(),
            owner_id: "owner".into(),
            current_token: None,
            ttl_ms: 1,
        })
        .await
        .unwrap()
    {
        AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        other => panic!("unexpected outcome: {other:?}"),
    };
    sleep(Duration::from_millis(10)).await;
    let replacement = match store
        .acquire_partition_authority(AcquirePartitionAuthorityRequest {
            key: key.clone(),
            owner_id: "replacement".into(),
            current_token: None,
            ttl_ms: 30_000,
        })
        .await
        .unwrap()
    {
        AcquirePartitionAuthorityOutcome::Acquired(token) => token,
        other => panic!("unexpected outcome: {other:?}"),
    };
    assert_eq!(replacement.owner_epoch, token.owner_epoch + 1);
    let before_failed_publish = snapshot.load().await.unwrap();
    let candidate = PartitionCheckpointPointer {
        key: key.clone(),
        checkpoint_key: "checkpoint-1".into(),
    };
    let error = store
        .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
            expected_previous: None,
            candidate: candidate.clone(),
            authority: token.clone(),
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        velorix_meta::MetaStoreError::PartitionAuthorityInvalidToken
    ));
    assert!(store
        .read_partition_checkpoint_pointer(&key)
        .await
        .unwrap()
        .is_none());
    assert_eq!(snapshot.load().await.unwrap(), before_failed_publish);
    assert_eq!(
        store
            .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
                expected_previous: None,
                candidate,
                authority: replacement,
            })
            .await
            .unwrap(),
        velorix_meta::PublishPartitionCheckpointPointerOutcome::Published
    );
}
