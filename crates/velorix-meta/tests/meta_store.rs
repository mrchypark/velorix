use std::sync::Arc;

use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use velorix_core::relation::SchemaFingerprintV1;
use velorix_meta::{
    AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest, InMemoryMetaStore,
    IngestRangeReservation, MetaStore, OssMetaStore, PublishStandingRuntimeCheckpointOutcome,
    PublishStandingRuntimeCheckpointRequest, ReserveIngestRangeOutcome,
    StandingRuntimeCheckpointPointer, StandingRuntimeOwnerToken, StoreRelationCatalogOutcome,
    STANDING_RUNTIME_BACKEND_TIME_SOURCE_PROCESS_CLOCK,
    STANDING_RUNTIME_BACKEND_TIME_SOURCE_UNAVAILABLE,
};

mod common;

#[tokio::test]
async fn meta_store_capabilities_mark_in_memory_and_oss_as_not_production_multi_writer_safe() {
    let in_memory = InMemoryMetaStore::default()
        .read_meta_store_capabilities()
        .await
        .unwrap()
        .standing_runtime_fencing;
    assert_eq!(in_memory.backend_name, "in-memory");
    assert!(in_memory.linearizable_owner_lease);
    assert!(in_memory.owner_validated_checkpoint_publish);
    assert!(in_memory.publish_checks_owner_and_latest_atomically);
    assert!(!in_memory.durable_monotonic_owner_epoch);
    assert!(!in_memory.authoritative_backend_time);
    assert_eq!(
        in_memory.backend_time_source_kind,
        STANDING_RUNTIME_BACKEND_TIME_SOURCE_PROCESS_CLOCK
    );
    assert_eq!(
        in_memory.backend_time_blocked_reason,
        "in_memory_process_clock_not_backend_authority"
    );
    assert_eq!(in_memory.lease_authority_kind, "process_local");
    assert_eq!(in_memory.lease_expiry_semantics, "process_clock_ttl");
    assert!(!in_memory.control_plane_auth_enforced);
    assert!(!in_memory.multi_writer_fencing_safe);
    assert!(!in_memory.bounded_wall_clock_failover);
    assert_eq!(in_memory.failover_time_bound_ms, 0);
    assert!(!in_memory.production_bounded_failover_safe);
    assert!(!in_memory.production_multi_writer_safe);

    let temp = TempDir::new().unwrap();
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp.path()).unwrap());
    let oss = OssMetaStore::new(object_store)
        .read_meta_store_capabilities()
        .await
        .unwrap()
        .standing_runtime_fencing;
    assert_eq!(oss.backend_name, "oss");
    assert!(!oss.linearizable_owner_lease);
    assert!(!oss.owner_validated_checkpoint_publish);
    assert!(!oss.latest_read_linearizable);
    assert_eq!(
        oss.backend_time_source_kind,
        STANDING_RUNTIME_BACKEND_TIME_SOURCE_UNAVAILABLE
    );
    assert_eq!(
        oss.backend_time_blocked_reason,
        "oss_backend_has_no_standing_runtime_lease_authority"
    );
    assert_eq!(oss.lease_authority_kind, "none");
    assert_eq!(oss.lease_expiry_semantics, "unavailable");
    assert!(!oss.multi_writer_fencing_safe);
    assert!(!oss.bounded_wall_clock_failover);
    assert_eq!(oss.failover_time_bound_ms, 0);
    assert!(!oss.production_bounded_failover_safe);
    assert!(!oss.production_multi_writer_safe);
}

#[tokio::test]
async fn relation_catalog_create_is_idempotent_when_body_matches() {
    let store = InMemoryMetaStore::default();
    let catalog = common::orders_relation_catalog("v1");

    let created = store.store_relation_catalog(catalog.clone()).await.unwrap();
    let duplicate = store.store_relation_catalog(catalog.clone()).await.unwrap();
    let read = store.read_relation_catalog("orders", "v1").await.unwrap();

    assert_eq!(created, StoreRelationCatalogOutcome::Created);
    assert_eq!(duplicate, StoreRelationCatalogOutcome::Duplicate);
    assert_eq!(read, catalog);
}

#[tokio::test]
async fn relation_catalog_create_rejects_same_identity_with_different_body() {
    let store = InMemoryMetaStore::default();
    let first = common::orders_relation_catalog("v1");
    let mut changed = common::orders_relation_catalog("v1");
    changed.relation_schema.relation_name = "orders_changed".to_string();
    changed.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&changed.relation_schema).unwrap();
    changed.incremental_relation.schema_fingerprint = changed.schema_fingerprint.clone();

    store.store_relation_catalog(first).await.unwrap();
    let error = store.store_relation_catalog(changed).await.unwrap_err();

    assert!(error.to_string().contains("relation catalog conflict"));
}

#[tokio::test]
async fn ingest_range_reservation_rejects_overlapping_range_for_partition() {
    let store = InMemoryMetaStore::default();
    let first = reservation("orders", 0, 0, 100, "sha256:first");
    let overlapping = reservation("orders", 0, 50, 150, "sha256:second");

    let first_outcome = store.reserve_ingest_range(first).await.unwrap();
    let second_outcome = store.reserve_ingest_range(overlapping).await.unwrap();

    assert_eq!(first_outcome, ReserveIngestRangeOutcome::Reserved);
    assert_eq!(second_outcome, ReserveIngestRangeOutcome::Conflict);
}

#[tokio::test]
async fn ingest_range_reservation_allows_adjacent_ranges_and_duplicate_retry() {
    let store = InMemoryMetaStore::default();
    let first = reservation("orders", 0, 0, 100, "sha256:first");
    let adjacent = reservation("orders", 0, 100, 150, "sha256:second");

    let first_outcome = store.reserve_ingest_range(first.clone()).await.unwrap();
    let retry_outcome = store.reserve_ingest_range(first).await.unwrap();
    let adjacent_outcome = store.reserve_ingest_range(adjacent).await.unwrap();

    assert_eq!(first_outcome, ReserveIngestRangeOutcome::Reserved);
    assert_eq!(retry_outcome, ReserveIngestRangeOutcome::Duplicate);
    assert_eq!(adjacent_outcome, ReserveIngestRangeOutcome::Reserved);
}

#[tokio::test]
async fn oss_meta_store_persists_relation_catalogs_and_ingest_admission_in_object_store() {
    let temp = TempDir::new().unwrap();
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(temp.path()).unwrap());
    let store = OssMetaStore::new(object_store);
    let catalog = common::orders_relation_catalog("v1");
    let first = reservation(
        "orders",
        0,
        0,
        100,
        "sha256:1111111111111111111111111111111111111111111111111111111111111111",
    );
    let duplicate = first.clone();
    let overlapping = reservation(
        "orders",
        0,
        50,
        150,
        "sha256:2222222222222222222222222222222222222222222222222222222222222222",
    );
    let adjacent = reservation(
        "orders",
        0,
        100,
        150,
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
    );

    let created = store.store_relation_catalog(catalog.clone()).await.unwrap();
    let duplicate_catalog = store.store_relation_catalog(catalog.clone()).await.unwrap();
    let read = store.read_relation_catalog("orders", "v1").await.unwrap();
    let first_outcome = store.reserve_ingest_range(first).await.unwrap();
    let duplicate_outcome = store.reserve_ingest_range(duplicate).await.unwrap();
    let overlapping_outcome = store.reserve_ingest_range(overlapping).await.unwrap();
    let adjacent_outcome = store.reserve_ingest_range(adjacent).await.unwrap();

    assert_eq!(created, StoreRelationCatalogOutcome::Created);
    assert_eq!(duplicate_catalog, StoreRelationCatalogOutcome::Duplicate);
    assert_eq!(read, catalog);
    assert_eq!(first_outcome, ReserveIngestRangeOutcome::Reserved);
    assert_eq!(duplicate_outcome, ReserveIngestRangeOutcome::Duplicate);
    assert_eq!(overlapping_outcome, ReserveIngestRangeOutcome::Conflict);
    assert_eq!(adjacent_outcome, ReserveIngestRangeOutcome::Reserved);
}

#[tokio::test]
async fn standing_runtime_checkpoint_publish_is_linearizable_and_idempotent() {
    let store = InMemoryMetaStore::default();
    let owner = acquire_owner(&store, "owner-a").await;
    let first = checkpoint_pointer(1, "a");
    let retry = first.clone();
    let second = checkpoint_pointer(2, "b");
    let conflicting_second = checkpoint_pointer(2, "c");

    let published = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: first.clone(),
            owner: owner.clone(),
        })
        .await
        .unwrap();
    let duplicate = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: retry,
            owner: owner.clone(),
        })
        .await
        .unwrap();
    let conflict = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: conflicting_second,
            owner: owner.clone(),
        })
        .await
        .unwrap();
    let advanced = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(first),
            candidate: second.clone(),
            owner,
        })
        .await
        .unwrap();
    let latest = store
        .read_standing_runtime_checkpoint("default", "program", "view")
        .await
        .unwrap();

    assert_eq!(
        published,
        PublishStandingRuntimeCheckpointOutcome::Published
    );
    assert_eq!(
        duplicate,
        PublishStandingRuntimeCheckpointOutcome::Duplicate
    );
    assert_eq!(conflict, PublishStandingRuntimeCheckpointOutcome::Conflict);
    assert_eq!(advanced, PublishStandingRuntimeCheckpointOutcome::Published);
    assert_eq!(latest, Some(second));
}

#[tokio::test]
async fn standing_runtime_checkpoint_publish_conflicts_on_stale_expected_previous() {
    let store = InMemoryMetaStore::default();
    let owner = acquire_owner(&store, "owner-a").await;
    let first = checkpoint_pointer(1, "a");
    let second = checkpoint_pointer(2, "b");
    let third = checkpoint_pointer(3, "c");

    store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: first.clone(),
            owner: owner.clone(),
        })
        .await
        .unwrap();
    store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(first.clone()),
            candidate: second.clone(),
            owner: owner.clone(),
        })
        .await
        .unwrap();

    let stale_expected_previous = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(first),
            candidate: third,
            owner,
        })
        .await
        .unwrap();
    let latest = store
        .read_standing_runtime_checkpoint("default", "program", "view")
        .await
        .unwrap();

    assert_eq!(
        stale_expected_previous,
        PublishStandingRuntimeCheckpointOutcome::Conflict
    );
    assert_eq!(latest, Some(second));
}

#[tokio::test]
async fn standing_runtime_checkpoint_pointer_preserves_output_manifest_refs() {
    let store = InMemoryMetaStore::default();
    let owner = acquire_owner(&store, "owner-a").await;
    let mut pointer = checkpoint_pointer(1, "a");
    let output_hash = "b".repeat(64);
    let delta_hash = "c".repeat(64);
    pointer.output_manifest_refs = vec![format!(
        "{}v1/standing-runtime-output-manifests/default/program/view/epochs/00000000000000000001/sha256/{output_hash}.output-manifest.json",
        velorix_meta::STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX,
    ), format!(
        "{}v1/standing-runtime-output-deltas/default/program/view/epochs/00000000000000000001/sha256/{delta_hash}.output-delta.json",
        velorix_meta::STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX,
    )];

    let outcome = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: pointer.clone(),
            owner,
        })
        .await
        .unwrap();
    let latest = store
        .read_standing_runtime_checkpoint("default", "program", "view")
        .await
        .unwrap();

    assert_eq!(outcome, PublishStandingRuntimeCheckpointOutcome::Published);
    assert_eq!(latest, Some(pointer));
}

#[tokio::test]
async fn standing_runtime_checkpoint_pointer_rejects_invalid_output_manifest_refs() {
    let store = InMemoryMetaStore::default();
    let owner = acquire_owner(&store, "owner-a").await;
    let mut wrong_prefix = checkpoint_pointer(1, "a");
    wrong_prefix.output_manifest_refs = vec![format!(
        "standing-runtime-checkpoint:{}",
        wrong_prefix.checkpoint_key
    )];

    assert!(store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: wrong_prefix,
            owner: owner.clone(),
        })
        .await
        .is_err());

    let mut wrong_epoch = checkpoint_pointer(1, "a");
    let output_hash = "b".repeat(64);
    wrong_epoch.output_manifest_refs = vec![format!(
        "{}v1/standing-runtime-output-manifests/default/program/view/epochs/00000000000000000002/sha256/{output_hash}.output-manifest.json",
        velorix_meta::STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX,
    )];

    assert!(store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: wrong_epoch,
            owner: owner.clone(),
        })
        .await
        .is_err());

    let mut wrong_delta_epoch = checkpoint_pointer(1, "a");
    let delta_hash = "c".repeat(64);
    wrong_delta_epoch.output_manifest_refs = vec![format!(
        "{}v1/standing-runtime-output-deltas/default/program/view/epochs/00000000000000000002/sha256/{delta_hash}.output-delta.json",
        velorix_meta::STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX,
    )];

    assert!(store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: wrong_delta_epoch,
            owner,
        })
        .await
        .is_err());
}

#[tokio::test]
async fn standing_runtime_owner_lease_conflicts_renews_and_fences_publish() {
    let store = InMemoryMetaStore::default();
    let owner_a = acquire_owner(&store, "owner-a").await;
    let renewed = store
        .acquire_standing_runtime_owner(owner_request("owner-a", 30_000))
        .await
        .unwrap();
    let conflict = store
        .acquire_standing_runtime_owner(owner_request("owner-b", 30_000))
        .await
        .unwrap();
    let pointer = checkpoint_pointer(1, "a");
    let stale_publish = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: pointer.clone(),
            owner: StandingRuntimeOwnerToken {
                owner_id: "owner-b".to_string(),
                ..owner_a.clone()
            },
        })
        .await
        .unwrap_err();
    let owner_publish = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: pointer.clone(),
            owner: owner_a.clone(),
        })
        .await
        .unwrap();

    assert!(matches!(
        renewed,
        AcquireStandingRuntimeOwnerOutcome::Renewed(claim)
            if claim.owner_id == "owner-a" && claim.owner_epoch == owner_a.owner_epoch
    ));
    assert!(matches!(
        conflict,
        AcquireStandingRuntimeOwnerOutcome::Conflict(claim)
            if claim.owner_id == "owner-a" && claim.owner_epoch == owner_a.owner_epoch
    ));
    assert!(stale_publish
        .to_string()
        .contains("standing runtime owner token"));
    assert_eq!(
        owner_publish,
        PublishStandingRuntimeCheckpointOutcome::Published
    );
    assert_eq!(
        store
            .read_standing_runtime_checkpoint("default", "program", "view")
            .await
            .unwrap(),
        Some(pointer)
    );
}

#[tokio::test]
async fn standing_runtime_checkpoint_publish_rejects_owner_scope_mismatch() {
    let store = InMemoryMetaStore::default();
    let owner = acquire_owner(&store, "owner-a").await;
    let pointer = checkpoint_pointer(1, "a");
    let mismatched_owner = StandingRuntimeOwnerToken {
        view_id: "other-view".to_string(),
        ..owner
    };

    let error = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: pointer,
            owner: mismatched_owner,
        })
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("standing runtime checkpoint pointer scope mismatch"));
    assert_eq!(
        store
            .read_standing_runtime_checkpoint("default", "program", "view")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn standing_runtime_owner_lease_rejects_unbounded_ttl() {
    let store = InMemoryMetaStore::default();

    let zero = store
        .acquire_standing_runtime_owner(owner_request("owner-a", 0))
        .await
        .unwrap_err();
    assert!(zero.to_string().contains("ttl_ms"));

    let excessive = store
        .acquire_standing_runtime_owner(owner_request("owner-a", u64::MAX))
        .await
        .unwrap_err();
    assert!(excessive.to_string().contains("ttl_ms"));
}

fn reservation(
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    payload_digest: &str,
) -> IngestRangeReservation {
    IngestRangeReservation {
        stream_id: stream_id.to_string(),
        partition_id,
        start_offset_inclusive,
        end_offset_exclusive,
        batch_key: format!(
            "v1/ingest/{stream_id}/p={partition_id:010}/{start_offset_inclusive:020}-{end_offset_exclusive:020}.batch"
        ),
        payload_digest: payload_digest.to_string(),
        relation_id: "orders".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint:
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        writer_epoch: 7,
    }
}

fn checkpoint_pointer(epoch: u64, hash_seed: &str) -> StandingRuntimeCheckpointPointer {
    let hash = format!("{hash_seed:0<64}");
    StandingRuntimeCheckpointPointer {
        tenant_id: "default".to_string(),
        program_id: "program".to_string(),
        view_id: "view".to_string(),
        checkpoint_key: format!(
            "v1/standing-runtime-checkpoints/default/program/view/epochs/{epoch:020}/sha256/{hash}.checkpoint.json"
        ),
        logical_epoch: epoch,
        content_hash: format!("sha256:{hash}"),
        manifest_hash: format!("sha256:{hash}"),
        output_manifest_refs: Vec::new(),
    }
}

async fn acquire_owner(store: &InMemoryMetaStore, owner_id: &str) -> StandingRuntimeOwnerToken {
    match store
        .acquire_standing_runtime_owner(owner_request(owner_id, 30_000))
        .await
        .unwrap()
    {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim)
        | AcquireStandingRuntimeOwnerOutcome::Renewed(claim) => StandingRuntimeOwnerToken {
            tenant_id: claim.tenant_id,
            program_id: claim.program_id,
            view_id: claim.view_id,
            owner_id: claim.owner_id,
            owner_epoch: claim.owner_epoch,
        },
        AcquireStandingRuntimeOwnerOutcome::Conflict(claim) => {
            panic!("unexpected owner conflict: {claim:?}")
        }
    }
}

fn owner_request(owner_id: &str, ttl_ms: u64) -> AcquireStandingRuntimeOwnerRequest {
    AcquireStandingRuntimeOwnerRequest {
        tenant_id: "default".to_string(),
        program_id: "program".to_string(),
        view_id: "view".to_string(),
        owner_id: owner_id.to_string(),
        ttl_ms,
    }
}
