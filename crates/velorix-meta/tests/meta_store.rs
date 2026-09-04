use std::sync::Arc;

use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use velorix_core::relation::SchemaFingerprintV1;
use velorix_core::standing_program::{
    RuntimeCheckpointInputCoverageV1, RuntimeCheckpointPartitionCoverageV1,
    RuntimeCheckpointRelationCoverageV1, RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
};
use velorix_meta::{
    AcquirePartitionAuthorityOutcome, AcquirePartitionAuthorityRequest,
    AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest,
    BeginViewBootstrapOutcome, BeginViewBootstrapRequest, CaptureIngestSourceCutRequest,
    CommitIngestRangeOutcome, FixViewBootstrapActivationCutOutcome,
    FixViewBootstrapActivationCutRequest, InMemoryMetaStore, IngestRangeReservation,
    IngestSourceCutV1, IngestSourceRelationIdentityV1, MetaStore, MetaStoreCapabilities,
    OssMetaStore, PartitionAuthorityKey, PartitionAuthorityToken, PartitionCheckpointPointer,
    PromoteViewBootstrapOutcome, PromoteViewBootstrapRequest,
    PublishPartitionCheckpointPointerOutcome, PublishPartitionCheckpointPointerRequest,
    PublishStandingRuntimeCheckpointOutcome, PublishStandingRuntimeCheckpointRequest,
    ReserveIngestRangeOutcome, StandingRuntimeCheckpointPointer, StandingRuntimeOwnerToken,
    StoreRelationCatalogOutcome, ViewBootstrapLifecycleV1,
    STANDING_RUNTIME_BACKEND_TIME_SOURCE_PROCESS_CLOCK,
    STANDING_RUNTIME_BACKEND_TIME_SOURCE_UNAVAILABLE,
};

mod common;

#[tokio::test]
async fn meta_store_capabilities_mark_in_memory_and_oss_as_not_production_multi_writer_safe() {
    let in_memory_capabilities = InMemoryMetaStore::default()
        .read_meta_store_capabilities()
        .await
        .unwrap();
    let in_memory = in_memory_capabilities.standing_runtime_fencing.clone();
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
    assert!(
        in_memory_capabilities
            .partition_authority
            .partition_scoped_authority
    );
    assert!(
        in_memory_capabilities
            .partition_authority
            .backend_owned_time
    );
    assert!(!in_memory_capabilities.partition_authority.production_safe);

    let mut legacy_json = serde_json::to_value(&in_memory_capabilities).unwrap();
    legacy_json
        .as_object_mut()
        .unwrap()
        .remove("partition_authority");
    let legacy: MetaStoreCapabilities = serde_json::from_value(legacy_json).unwrap();
    assert!(!legacy.partition_authority.partition_scoped_authority);
    assert!(!legacy.partition_authority.production_safe);

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
async fn partition_authority_is_explicitly_unsupported_by_unwired_backends() {
    let temp = TempDir::new().unwrap();
    let store = OssMetaStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(temp.path()).unwrap(),
    ));
    let capability = store.read_partition_authority_capability().await;
    let acquire = store
        .acquire_partition_authority(AcquirePartitionAuthorityRequest {
            key: partition_key(),
            owner_id: "writer-a".to_string(),
            current_token: None,
            ttl_ms: 100,
        })
        .await;

    assert!(matches!(
        capability,
        Err(velorix_meta::MetaStoreError::UnsupportedCapability(
            "partition_authority"
        ))
    ));
    assert!(matches!(
        acquire,
        Err(velorix_meta::MetaStoreError::UnsupportedCapability(
            "partition_authority"
        ))
    ));
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
async fn ingest_source_cut_stops_before_uncommitted_reservation_hole() {
    let store = InMemoryMetaStore::default();
    let first = reservation("orders", 0, 0, 10, "sha256:first");
    let hole = reservation("orders", 0, 10, 20, "sha256:hole");
    let higher = reservation("orders", 0, 20, 30, "sha256:higher");
    for range in [&first, &hole, &higher] {
        assert_eq!(
            store.reserve_ingest_range(range.clone()).await.unwrap(),
            ReserveIngestRangeOutcome::Reserved
        );
    }
    assert_eq!(
        store.commit_ingest_range(first.clone()).await.unwrap(),
        CommitIngestRangeOutcome::Committed
    );
    assert_eq!(
        store.commit_ingest_range(higher).await.unwrap(),
        CommitIngestRangeOutcome::Committed
    );

    let cut = store
        .capture_ingest_source_cut(orders_source_cut_request())
        .await
        .unwrap();
    assert_eq!(cut.input_catalog_epoch, 3);
    assert_eq!(cut.relations[0].partitions.len(), 1);
    assert_eq!(cut.relations[0].partitions[0].base_offset_inclusive, 0);
    assert_eq!(
        cut.relations[0].partitions[0].committed_offset_exclusive,
        10
    );

    assert_eq!(
        store.commit_ingest_range(hole.clone()).await.unwrap(),
        CommitIngestRangeOutcome::Committed
    );
    assert_eq!(
        store.commit_ingest_range(hole).await.unwrap(),
        CommitIngestRangeOutcome::Duplicate
    );
    let advanced = store
        .capture_ingest_source_cut(orders_source_cut_request())
        .await
        .unwrap();
    assert_eq!(
        advanced.relations[0].partitions[0].committed_offset_exclusive,
        30
    );
}

#[tokio::test]
async fn ingest_source_cut_catalog_epoch_distinguishes_later_partition_discovery() {
    let store = InMemoryMetaStore::default();
    let first = reservation("orders", 0, 42, 43, "sha256:first");
    store.reserve_ingest_range(first.clone()).await.unwrap();
    store.commit_ingest_range(first).await.unwrap();
    let before = store
        .capture_ingest_source_cut(orders_source_cut_request())
        .await
        .unwrap();

    let later = reservation("orders", 1, 0, 1, "sha256:later");
    store.reserve_ingest_range(later.clone()).await.unwrap();
    store.commit_ingest_range(later).await.unwrap();
    let after = store
        .capture_ingest_source_cut(orders_source_cut_request())
        .await
        .unwrap();

    assert_eq!(before.input_catalog_epoch, 1);
    assert_eq!(before.relations[0].partitions.len(), 1);
    assert_eq!(before.relations[0].partitions[0].base_offset_inclusive, 42);
    assert_eq!(after.input_catalog_epoch, 2);
    assert_eq!(after.relations[0].partitions.len(), 2);
}

#[tokio::test]
async fn ingest_source_cut_rejects_commit_without_exact_reservation() {
    let store = InMemoryMetaStore::default();
    assert_eq!(
        store
            .commit_ingest_range(reservation("orders", 0, 0, 1, "sha256:missing"))
            .await
            .unwrap(),
        CommitIngestRangeOutcome::Conflict
    );
}

#[tokio::test]
async fn view_bootstrap_atomically_freezes_source_cut_and_is_idempotent() {
    let store = InMemoryMetaStore::default();
    let first = reservation("orders", 0, 0, 10, "sha256:first");
    store.reserve_ingest_range(first.clone()).await.unwrap();
    store.commit_ingest_range(first).await.unwrap();
    let request = orders_view_bootstrap_request();

    let created = match store.begin_view_bootstrap(request.clone()).await.unwrap() {
        BeginViewBootstrapOutcome::Created(control) => control,
        other => panic!("unexpected bootstrap outcome: {other:?}"),
    };
    assert_eq!(created.lifecycle, ViewBootstrapLifecycleV1::Bootstrapping);
    assert_eq!(created.bootstrap_generation, 1);
    assert_eq!(created.bootstrap_cut.input_catalog_epoch, 1);
    assert_eq!(
        created.bootstrap_cut.relations[0].partitions[0].committed_offset_exclusive,
        10
    );

    let tail = reservation("orders", 0, 10, 20, "sha256:tail");
    store.reserve_ingest_range(tail.clone()).await.unwrap();
    store.commit_ingest_range(tail).await.unwrap();
    let persisted = store
        .read_view_bootstrap("default", "orders-view", "orders-view")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted, created);
    assert_eq!(
        persisted.bootstrap_cut.relations[0].partitions[0].committed_offset_exclusive,
        10
    );

    assert!(matches!(
        store.begin_view_bootstrap(request.clone()).await.unwrap(),
        BeginViewBootstrapOutcome::Duplicate(control) if control == created
    ));
    let mut conflicting = request;
    conflicting.plan_hash = "sha256:other".to_string();
    assert_eq!(
        store.begin_view_bootstrap(conflicting).await.unwrap(),
        BeginViewBootstrapOutcome::Conflict
    );
}

#[tokio::test]
async fn view_bootstrap_sealed_partition_base_rejects_late_lower_range() {
    let store = InMemoryMetaStore::default();
    let first = reservation("orders", 0, 42, 43, "sha256:first");
    store.reserve_ingest_range(first.clone()).await.unwrap();
    store.commit_ingest_range(first).await.unwrap();
    store
        .begin_view_bootstrap(orders_view_bootstrap_request())
        .await
        .unwrap();

    assert_eq!(
        store
            .reserve_ingest_range(reservation("orders", 0, 0, 1, "sha256:too-low"))
            .await
            .unwrap(),
        ReserveIngestRangeOutcome::Conflict
    );
    assert_eq!(
        store
            .reserve_ingest_range(reservation("orders", 0, 43, 44, "sha256:tail"))
            .await
            .unwrap(),
        ReserveIngestRangeOutcome::Reserved
    );
    assert_eq!(
        store
            .reserve_ingest_range(reservation("orders", 1, 0, 1, "sha256:new-partition"))
            .await
            .unwrap(),
        ReserveIngestRangeOutcome::Reserved
    );
}

#[tokio::test]
async fn view_bootstrap_new_partition_reserved_before_snapshot_and_committed_after_is_tail() {
    let store = InMemoryMetaStore::default();
    let initial = reservation("orders", 0, 0, 10, "sha256:initial");
    store.reserve_ingest_range(initial.clone()).await.unwrap();
    store.commit_ingest_range(initial).await.unwrap();
    let in_flight_new_partition = reservation("orders", 1, 0, 1, "sha256:in-flight-new-partition");
    store
        .reserve_ingest_range(in_flight_new_partition.clone())
        .await
        .unwrap();

    let frozen = match store
        .begin_view_bootstrap(orders_view_bootstrap_request())
        .await
        .unwrap()
    {
        BeginViewBootstrapOutcome::Created(control) => control.bootstrap_cut,
        other => panic!("unexpected bootstrap outcome: {other:?}"),
    };
    assert_eq!(frozen.input_catalog_epoch, 2);
    assert_eq!(frozen.relations[0].partitions.len(), 2);
    let frozen_new_partition = frozen.relations[0]
        .partitions
        .iter()
        .find(|partition| partition.partition_id == 1)
        .unwrap();
    assert_eq!(frozen_new_partition.base_offset_inclusive, 0);
    assert_eq!(frozen_new_partition.committed_offset_exclusive, 0);

    assert_eq!(
        store
            .commit_ingest_range(in_flight_new_partition)
            .await
            .unwrap(),
        CommitIngestRangeOutcome::Committed
    );
    let current = store
        .capture_ingest_source_cut(orders_source_cut_request())
        .await
        .unwrap();
    assert_eq!(
        current.relations[0]
            .partitions
            .iter()
            .find(|partition| partition.partition_id == 1)
            .unwrap()
            .committed_offset_exclusive,
        1
    );
    assert_eq!(
        store
            .read_view_bootstrap("default", "orders-view", "orders-view")
            .await
            .unwrap()
            .unwrap()
            .bootstrap_cut,
        frozen
    );
}

#[tokio::test]
async fn view_bootstrap_activation_cut_and_promotion_fail_closed_across_tail_race() {
    let store = InMemoryMetaStore::default();
    let first = reservation("orders", 0, 0, 10, "sha256:first");
    store.reserve_ingest_range(first.clone()).await.unwrap();
    store.commit_ingest_range(first).await.unwrap();
    let control = match store
        .begin_view_bootstrap(orders_view_bootstrap_request())
        .await
        .unwrap()
    {
        BeginViewBootstrapOutcome::Created(control) => control,
        other => panic!("unexpected bootstrap outcome: {other:?}"),
    };
    let owner = acquire_owner_for_scope(&store, "orders-view", "orders-view", "owner-a").await;
    let first_pointer = checkpoint_pointer_for_cut(
        1,
        "a",
        control.bootstrap_generation,
        &control.plan_hash,
        &control.bootstrap_cut,
    );
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: None,
                candidate: first_pointer.clone(),
                owner: owner.clone(),
            })
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );

    let tail = reservation("orders", 0, 10, 20, "sha256:tail");
    store.reserve_ingest_range(tail.clone()).await.unwrap();
    store.commit_ingest_range(tail).await.unwrap();
    let new_partition = reservation("orders", 1, 0, 1, "sha256:new-partition");
    store
        .reserve_ingest_range(new_partition.clone())
        .await
        .unwrap();
    store.commit_ingest_range(new_partition).await.unwrap();

    let fixed = match store
        .fix_view_bootstrap_activation_cut(FixViewBootstrapActivationCutRequest {
            tenant_id: "default".to_string(),
            program_id: "orders-view".to_string(),
            view_id: "orders-view".to_string(),
            bootstrap_generation: control.bootstrap_generation,
            plan_hash: control.plan_hash.clone(),
            owner: owner.clone(),
        })
        .await
        .unwrap()
    {
        FixViewBootstrapActivationCutOutcome::Fixed(control) => control,
        other => panic!("unexpected activation-cut outcome: {other:?}"),
    };
    let activation_cut = fixed.activation_cut.clone().unwrap();
    assert_eq!(activation_cut.input_catalog_epoch, 3);
    assert_eq!(activation_cut.relations[0].partitions.len(), 2);
    assert_eq!(
        store
            .promote_view_bootstrap(PromoteViewBootstrapRequest {
                tenant_id: "default".to_string(),
                program_id: "orders-view".to_string(),
                view_id: "orders-view".to_string(),
                bootstrap_generation: control.bootstrap_generation,
                plan_hash: control.plan_hash.clone(),
                checkpoint: first_pointer.clone(),
                owner: owner.clone(),
            })
            .await
            .unwrap(),
        PromoteViewBootstrapOutcome::Conflict
    );

    let mut covering_pointer = checkpoint_pointer_for_cut(
        2,
        "b",
        control.bootstrap_generation,
        &control.plan_hash,
        &activation_cut,
    );
    bind_checkpoint_predecessor(&mut covering_pointer, &first_pointer);
    store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(first_pointer),
            candidate: covering_pointer.clone(),
            owner: owner.clone(),
        })
        .await
        .unwrap();
    let mut mutated_pointer = covering_pointer.clone();
    let output_hash = "d".repeat(64);
    mutated_pointer.output_manifest_refs = vec![format!(
        "{}v1/standing-runtime-output-manifests/default/orders-view/orders-view/epochs/00000000000000000002/sha256/{output_hash}.output-manifest.json",
        velorix_meta::STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX,
    )];
    assert_eq!(
        store
            .promote_view_bootstrap(PromoteViewBootstrapRequest {
                tenant_id: "default".to_string(),
                program_id: "orders-view".to_string(),
                view_id: "orders-view".to_string(),
                bootstrap_generation: control.bootstrap_generation,
                plan_hash: control.plan_hash.clone(),
                checkpoint: mutated_pointer,
                owner: owner.clone(),
            })
            .await
            .unwrap(),
        PromoteViewBootstrapOutcome::Conflict
    );
    let promotion_request = PromoteViewBootstrapRequest {
        tenant_id: "default".to_string(),
        program_id: "orders-view".to_string(),
        view_id: "orders-view".to_string(),
        bootstrap_generation: control.bootstrap_generation,
        plan_hash: control.plan_hash.clone(),
        checkpoint: covering_pointer.clone(),
        owner: owner.clone(),
    };
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let store = store.clone();
        let request = promotion_request.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            store.promote_view_bootstrap(request).await.unwrap()
        }));
    }
    barrier.wait().await;
    let first = workers.remove(0).await.unwrap();
    let second = workers.remove(0).await.unwrap();
    let outcomes = [first, second];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, PromoteViewBootstrapOutcome::Promoted(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, PromoteViewBootstrapOutcome::Duplicate(_)))
            .count(),
        1
    );
    let promoted = store
        .read_view_bootstrap("default", "orders-view", "orders-view")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(promoted.lifecycle, ViewBootstrapLifecycleV1::Active);
    assert_eq!(promoted.active_checkpoint, Some(covering_pointer.clone()));
    assert!(matches!(
        store
            .promote_view_bootstrap(PromoteViewBootstrapRequest {
                tenant_id: "default".to_string(),
                program_id: "orders-view".to_string(),
                view_id: "orders-view".to_string(),
                bootstrap_generation: control.bootstrap_generation,
                plan_hash: control.plan_hash.clone(),
                checkpoint: covering_pointer.clone(),
                owner: owner.clone(),
            })
            .await
            .unwrap(),
        PromoteViewBootstrapOutcome::Duplicate(_)
    ));
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
    let mut second = checkpoint_pointer(2, "b");
    bind_checkpoint_predecessor(&mut second, &first);
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
async fn partition_authority_uses_backend_clock_and_requires_exact_token_to_renew() {
    let store = InMemoryMetaStore::default();
    store.set_partition_authority_clock_for_test(100).await;
    let key = partition_key();

    let first = acquire_partition(&store, key.clone(), "writer-a", None).await;
    assert_eq!(first.owner_epoch, 1);
    assert_eq!(first.expires_at_unix_ms, 200);

    let round_tripped: PartitionAuthorityToken =
        serde_json::from_slice(&serde_json::to_vec(&first).unwrap()).unwrap();
    assert_eq!(round_tripped, first);
    // The request carries no time, so a caller clock cannot skew authority.
    store.set_partition_authority_clock_for_test(150).await;
    let renewed = acquire_partition(&store, key.clone(), "writer-a", Some(round_tripped)).await;
    assert_eq!(renewed.owner_epoch, 1);
    assert_eq!(renewed.expires_at_unix_ms, 250);
    assert_eq!(
        store.read_partition_authority(&key).await.unwrap(),
        Some(renewed.clone())
    );

    let stale_renewal = store
        .acquire_partition_authority(AcquirePartitionAuthorityRequest {
            key: key.clone(),
            owner_id: "writer-a".to_string(),
            current_token: Some(first),
            ttl_ms: 100,
        })
        .await
        .unwrap();
    assert!(matches!(
        stale_renewal,
        AcquirePartitionAuthorityOutcome::Conflict(_)
    ));

    let wrong_key = store
        .acquire_partition_authority(AcquirePartitionAuthorityRequest {
            key: PartitionAuthorityKey {
                partition_id: 9,
                ..key.clone()
            },
            owner_id: "writer-a".to_string(),
            current_token: Some(renewed.clone()),
            ttl_ms: 100,
        })
        .await;
    assert!(matches!(
        wrong_key,
        Err(velorix_meta::MetaStoreError::PartitionAuthorityTokenScopeMismatch)
    ));
    let wrong_owner = store
        .acquire_partition_authority(AcquirePartitionAuthorityRequest {
            key,
            owner_id: "writer-b".to_string(),
            current_token: Some(renewed),
            ttl_ms: 100,
        })
        .await;
    assert!(matches!(
        wrong_owner,
        Err(velorix_meta::MetaStoreError::PartitionAuthorityInvalidToken)
    ));
}

#[tokio::test]
async fn partition_checkpoint_publish_rejects_token_at_and_after_expiry_before_duplicate() {
    let store = InMemoryMetaStore::default();
    let key = partition_key();
    store.set_partition_authority_clock_for_test(10).await;
    let authority = acquire_partition(&store, key.clone(), "writer-a", None).await;
    let candidate = partition_pointer(key, "first");
    assert_eq!(
        store
            .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
                expected_previous: None,
                candidate: candidate.clone(),
                authority: authority.clone(),
            })
            .await
            .unwrap(),
        PublishPartitionCheckpointPointerOutcome::Published
    );

    for now in [
        authority.expires_at_unix_ms,
        authority.expires_at_unix_ms + 1,
    ] {
        store.set_partition_authority_clock_for_test(now).await;
        let result = store
            .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
                expected_previous: None,
                candidate: candidate.clone(),
                authority: authority.clone(),
            })
            .await;
        assert!(matches!(
            result,
            Err(velorix_meta::MetaStoreError::PartitionAuthorityInvalidToken)
        ));
    }
}

#[tokio::test]
async fn partition_authority_takeover_fences_stale_tokens_and_key_mismatches() {
    let store = InMemoryMetaStore::default();
    let key = partition_key();
    store.set_partition_authority_clock_for_test(10).await;
    let first = acquire_partition(&store, key.clone(), "writer-a", None).await;
    store.set_partition_authority_clock_for_test(110).await;
    let second = acquire_partition(&store, key.clone(), "writer-b", None).await;
    assert_eq!(second.owner_epoch, 2);

    let stale = store
        .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
            expected_previous: None,
            candidate: partition_pointer(key.clone(), "first"),
            authority: first,
        })
        .await;
    assert!(matches!(
        stale,
        Err(velorix_meta::MetaStoreError::PartitionAuthorityInvalidToken)
    ));

    let other_key = PartitionAuthorityKey {
        partition_id: 1,
        ..key.clone()
    };
    let mismatched = store
        .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
            expected_previous: None,
            candidate: partition_pointer(other_key, "wrong"),
            authority: second,
        })
        .await;
    assert!(matches!(
        mismatched,
        Err(velorix_meta::MetaStoreError::PartitionCheckpointScopeMismatch)
    ));
}

#[tokio::test]
async fn partition_checkpoint_pointer_cas_has_one_winner_and_idempotent_retry() {
    let store = Arc::new(InMemoryMetaStore::default());
    let key = partition_key();
    store.set_partition_authority_clock_for_test(10).await;
    let authority = acquire_partition(&store, key.clone(), "writer-a", None).await;
    let first = partition_pointer(key.clone(), "first");
    let second = partition_pointer(key.clone(), "second");
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for candidate in [first.clone(), second] {
        let store = Arc::clone(&store);
        let authority = authority.clone();
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
                    expected_previous: None,
                    candidate,
                    authority,
                })
                .await
                .unwrap()
        }));
    }
    barrier.wait().await;
    let outcomes = [
        tasks.remove(0).await.unwrap(),
        tasks.remove(0).await.unwrap(),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == PublishPartitionCheckpointPointerOutcome::Published)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == PublishPartitionCheckpointPointerOutcome::Conflict)
            .count(),
        1
    );

    let latest = store
        .read_partition_checkpoint_pointer(&key)
        .await
        .unwrap()
        .unwrap();
    let duplicate = store
        .publish_partition_checkpoint_pointer(PublishPartitionCheckpointPointerRequest {
            expected_previous: None,
            candidate: latest.clone(),
            authority,
        })
        .await
        .unwrap();
    assert_eq!(
        duplicate,
        PublishPartitionCheckpointPointerOutcome::Duplicate
    );
    assert_eq!(
        store.read_partition_checkpoint_pointer(&key).await.unwrap(),
        Some(latest)
    );
}

#[tokio::test]
async fn standing_runtime_checkpoint_publish_conflicts_on_stale_expected_previous() {
    let store = InMemoryMetaStore::default();
    let owner = acquire_owner(&store, "owner-a").await;
    let first = checkpoint_pointer(1, "a");
    let mut second = checkpoint_pointer(2, "b");
    bind_checkpoint_predecessor(&mut second, &first);
    let mut third = checkpoint_pointer(3, "c");
    bind_checkpoint_predecessor(&mut third, &first);

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
async fn standing_runtime_checkpoint_publish_rejects_rollback_fork_and_aba() {
    let store = InMemoryMetaStore::default();
    let owner = acquire_owner(&store, "owner-a").await;
    let first = checkpoint_pointer(1, "a");
    store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: first.clone(),
            owner: owner.clone(),
        })
        .await
        .unwrap();

    let unbound_fork = checkpoint_pointer(2, "b");
    assert!(store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(first.clone()),
            candidate: unbound_fork,
            owner: owner.clone(),
        })
        .await
        .is_err());

    let mut second = checkpoint_pointer(2, "b");
    bind_checkpoint_predecessor(&mut second, &first);
    store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(first.clone()),
            candidate: second.clone(),
            owner: owner.clone(),
        })
        .await
        .unwrap();

    assert!(store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(second.clone()),
            candidate: first.clone(),
            owner: owner.clone(),
        })
        .await
        .is_err());

    let mut divergent = checkpoint_pointer(3, "c");
    bind_checkpoint_predecessor(&mut divergent, &first);
    assert!(store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(second.clone()),
            candidate: divergent,
            owner,
        })
        .await
        .is_err());
    assert_eq!(
        store
            .read_standing_runtime_checkpoint("default", "program", "view")
            .await
            .unwrap(),
        Some(second)
    );
}

#[tokio::test]
async fn standing_runtime_checkpoint_pointer_preserves_output_manifest_refs() {
    let store = InMemoryMetaStore::default();
    let owner = acquire_owner(&store, "owner-a").await;
    let mut pointer = checkpoint_pointer(1, "a");
    let output_hash = "b".repeat(64);
    let delta_hash = "c".repeat(64);
    let commit_hash = "d".repeat(64);
    pointer.output_manifest_refs = vec![format!(
        "{}v1/standing-runtime-output-manifests/default/program/view/epochs/00000000000000000001/sha256/{output_hash}.output-manifest.json",
        velorix_meta::STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX,
    ), format!(
        "{}v1/standing-runtime-output-deltas/default/program/view/epochs/00000000000000000001/sha256/{delta_hash}.output-delta.json",
        velorix_meta::STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX,
    ), format!(
        "{}v1/standing-runtime-output-deltas/default/program/view/epochs/00000000000000000001/sha256/{commit_hash}.output-delta.json",
        velorix_meta::STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX,
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
async fn standing_runtime_checkpoint_pointer_rejects_non_monotonic_successor() {
    let store = InMemoryMetaStore::default();
    let owner = acquire_owner(&store, "owner-a").await;
    let error = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: Some(checkpoint_pointer(2, "b")),
            candidate: checkpoint_pointer(1, "a"),
            owner,
        })
        .await
        .unwrap_err();

    assert!(error.to_string().contains("logical epoch must increase"));
}

#[tokio::test]
async fn standing_runtime_checkpoint_same_state_requires_and_accepts_a_distinct_next_epoch_key() {
    let store = InMemoryMetaStore::default();
    let owner = acquire_owner(&store, "owner-a").await;
    let previous = checkpoint_pointer(1, "a");
    let mut candidate = checkpoint_pointer(2, "a");
    bind_checkpoint_predecessor(&mut candidate, &previous);
    assert_eq!(previous.content_hash, candidate.content_hash);
    assert_ne!(previous.checkpoint_key, candidate.checkpoint_key);

    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: None,
                candidate: previous.clone(),
                owner: owner.clone(),
            })
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: Some(previous),
                candidate: candidate.clone(),
                owner,
            })
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );
    assert_eq!(
        store
            .read_standing_runtime_checkpoint("default", "program", "view")
            .await
            .unwrap(),
        Some(candidate)
    );
}

#[tokio::test]
async fn standing_runtime_checkpoint_pointer_preserves_validated_input_coverage() {
    let store = InMemoryMetaStore::default();
    let owner = acquire_owner(&store, "owner-a").await;
    let mut pointer = checkpoint_pointer(1, "a");
    let coverage = RuntimeCheckpointInputCoverageV1 {
        schema_version: RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
        view_generation: 3,
        plan_hash: format!("sha256:{}", "b".repeat(64)),
        input_catalog_epoch: 11,
        relations: vec![RuntimeCheckpointRelationCoverageV1 {
            relation_id: "orders".to_string(),
            relation_version: "v1".to_string(),
            relation_generation: 1,
            schema_fingerprint: format!("sha256:{}", "c".repeat(64)),
            partitions: vec![RuntimeCheckpointPartitionCoverageV1 {
                stream_id: "orders".to_string(),
                stream_generation: 1,
                partition_id: 0,
                partition_generation: 1,
                covered_from_offset_inclusive: 0,
                processed_offset_exclusive: 42,
            }],
        }],
    };
    pointer.bootstrap_generation = coverage.view_generation;
    pointer.plan_hash = coverage.plan_hash.clone();
    pointer.coverage_hash = coverage.stable_hash().unwrap();
    pointer.input_coverage = Some(coverage);

    let outcome = store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: pointer.clone(),
            owner,
        })
        .await
        .unwrap();

    assert_eq!(outcome, PublishStandingRuntimeCheckpointOutcome::Published);
    assert_eq!(
        store
            .read_standing_runtime_checkpoint("default", "program", "view")
            .await
            .unwrap(),
        Some(pointer)
    );
}

#[tokio::test]
async fn standing_runtime_checkpoint_pointer_rejects_mismatched_coverage_hash() {
    let store = InMemoryMetaStore::default();
    let owner = acquire_owner(&store, "owner-a").await;
    let mut pointer = checkpoint_pointer(1, "a");
    pointer.bootstrap_generation = 1;
    pointer.plan_hash = format!("sha256:{}", "b".repeat(64));
    pointer.coverage_hash = format!("sha256:{}", "c".repeat(64));
    pointer.input_coverage = Some(RuntimeCheckpointInputCoverageV1 {
        schema_version: RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
        view_generation: 1,
        plan_hash: pointer.plan_hash.clone(),
        input_catalog_epoch: 0,
        relations: Vec::new(),
    });

    assert!(store
        .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: pointer,
            owner,
        })
        .await
        .is_err());
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

fn orders_source_cut_request() -> CaptureIngestSourceCutRequest {
    CaptureIngestSourceCutRequest {
        relations: vec![IngestSourceRelationIdentityV1 {
            relation_id: "orders".to_string(),
            relation_version: "v1".to_string(),
            relation_generation: 1,
            schema_fingerprint:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
        }],
    }
}

fn orders_view_bootstrap_request() -> BeginViewBootstrapRequest {
    BeginViewBootstrapRequest {
        tenant_id: "default".to_string(),
        program_id: "orders-view".to_string(),
        view_id: "orders-view".to_string(),
        plan_hash: "sha256:plan".to_string(),
        view_spec_json: br#"{"view_id":"orders-view"}"#.to_vec(),
        relations: orders_source_cut_request().relations,
        view_inputs: Vec::new(),
        expected_graph_revision: 0,
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
        bootstrap_generation: 0,
        plan_hash: String::new(),
        coverage_hash: String::new(),
        input_coverage: None,
        previous_checkpoint_key: String::new(),
        previous_manifest_hash: String::new(),
    }
}

fn bind_checkpoint_predecessor(
    candidate: &mut StandingRuntimeCheckpointPointer,
    previous: &StandingRuntimeCheckpointPointer,
) {
    candidate.previous_checkpoint_key = previous.checkpoint_key.clone();
    candidate.previous_manifest_hash = previous.manifest_hash.clone();
}

fn checkpoint_pointer_for_cut(
    epoch: u64,
    hash_seed: &str,
    view_generation: u64,
    plan_hash: &str,
    cut: &IngestSourceCutV1,
) -> StandingRuntimeCheckpointPointer {
    let mut pointer = checkpoint_pointer(epoch, hash_seed);
    pointer.program_id = "orders-view".to_string();
    pointer.view_id = "orders-view".to_string();
    let content_hex = pointer.content_hash.strip_prefix("sha256:").unwrap();
    pointer.checkpoint_key = format!(
        "v1/standing-runtime-checkpoints/default/orders-view/orders-view/epochs/{epoch:020}/sha256/{content_hex}.checkpoint.json"
    );
    let coverage = RuntimeCheckpointInputCoverageV1 {
        schema_version: RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
        view_generation,
        plan_hash: plan_hash.to_string(),
        input_catalog_epoch: cut.input_catalog_epoch,
        relations: cut
            .relations
            .iter()
            .map(|relation| RuntimeCheckpointRelationCoverageV1 {
                relation_id: relation.relation.relation_id.clone(),
                relation_version: relation.relation.relation_version.clone(),
                relation_generation: relation.relation.relation_generation,
                schema_fingerprint: relation.relation.schema_fingerprint.clone(),
                partitions: relation
                    .partitions
                    .iter()
                    .map(|partition| RuntimeCheckpointPartitionCoverageV1 {
                        stream_id: partition.stream_id.clone(),
                        stream_generation: partition.stream_generation,
                        partition_id: partition.partition_id,
                        partition_generation: partition.partition_generation,
                        covered_from_offset_inclusive: partition.base_offset_inclusive,
                        processed_offset_exclusive: partition.committed_offset_exclusive,
                    })
                    .collect(),
            })
            .collect(),
    };
    pointer.bootstrap_generation = view_generation;
    pointer.plan_hash = plan_hash.to_string();
    pointer.coverage_hash = coverage.stable_hash().unwrap();
    pointer.input_coverage = Some(coverage);
    pointer
}

fn partition_key() -> PartitionAuthorityKey {
    PartitionAuthorityKey {
        namespace: "default".to_string(),
        view_id: "orders-view".to_string(),
        stream_id: "orders".to_string(),
        partition_id: 0,
    }
}

fn partition_pointer(key: PartitionAuthorityKey, suffix: &str) -> PartitionCheckpointPointer {
    PartitionCheckpointPointer {
        checkpoint_key: format!("partition-checkpoint-{suffix}"),
        key,
    }
}

async fn acquire_partition(
    store: &InMemoryMetaStore,
    key: PartitionAuthorityKey,
    owner_id: &str,
    current_token: Option<PartitionAuthorityToken>,
) -> PartitionAuthorityToken {
    match store
        .acquire_partition_authority(AcquirePartitionAuthorityRequest {
            key,
            owner_id: owner_id.to_string(),
            current_token,
            ttl_ms: 100,
        })
        .await
        .unwrap()
    {
        AcquirePartitionAuthorityOutcome::Acquired(token)
        | AcquirePartitionAuthorityOutcome::Renewed(token) => token,
        AcquirePartitionAuthorityOutcome::Conflict(token) => {
            panic!("unexpected partition authority conflict: {token:?}")
        }
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

async fn acquire_owner_for_scope(
    store: &InMemoryMetaStore,
    program_id: &str,
    view_id: &str,
    owner_id: &str,
) -> StandingRuntimeOwnerToken {
    match store
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: "default".to_string(),
            program_id: program_id.to_string(),
            view_id: view_id.to_string(),
            owner_id: owner_id.to_string(),
            ttl_ms: 30_000,
        })
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

fn view_input_edge(edge_id: &str) -> velorix_meta::BeginViewDependencyEdgeV1 {
    velorix_meta::BeginViewDependencyEdgeV1 {
        edge_id: edge_id.to_string(),
        producer_program_id: "producer".to_string(),
        producer_view_id: "producer".to_string(),
        producer_generation: 1,
        producer_plan_hash: "sha256:producer-plan".to_string(),
        input_relation_id: "producer-output".to_string(),
        input_relation_version: "v1".to_string(),
        output_stream_id: "producer-output/v1".to_string(),
        output_schema_hash: "sha256:output-schema".to_string(),
        key_descriptor_hash: "sha256:key-descriptor".to_string(),
        delta_codec_identity: "velorix-published-delta-v1".to_string(),
        frontier_kind: "producer_commit_epoch".to_string(),
        bootstrap_cursor: velorix_core::standing_program::CausalViewCursorV1 {
            input_edge: edge_id.to_string(),
            producer_tenant_id: "default".to_string(),
            producer_program_id: "producer".to_string(),
            producer_view_id: "producer".to_string(),
            producer_generation: 1,
            output_stream: "producer-output/v1".to_string(),
            output_epoch: 3,
            commit_digest:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
        },
    }
}

#[tokio::test]
async fn view_dependency_graph_revision_cas_fences_only_view_input_admissions() {
    let store = InMemoryMetaStore::default();

    // Source-only admissions do not consume the graph revision and never
    // bump it: they create isolated nodes with no dependency edges.
    let source_only = orders_view_bootstrap_request();
    assert!(matches!(
        store
            .begin_view_bootstrap(source_only.clone())
            .await
            .unwrap(),
        BeginViewBootstrapOutcome::Created(_)
    ));
    assert_eq!(
        store
            .read_view_dependency_graph_revision("default")
            .await
            .unwrap(),
        0
    );
    let mut stale = orders_view_bootstrap_request();
    stale.program_id = "second-source-view".to_string();
    stale.view_id = "second-source-view".to_string();
    assert!(matches!(
        store.begin_view_bootstrap(stale).await.unwrap(),
        BeginViewBootstrapOutcome::Created(_)
    ));
    assert_eq!(
        store
            .read_view_dependency_graph_revision("default")
            .await
            .unwrap(),
        0
    );

    // A view-input admission CAS-checks the expected revision and bumps it.
    let mut consumer = orders_view_bootstrap_request();
    consumer.program_id = "consumer-a".to_string();
    consumer.view_id = "consumer-a".to_string();
    consumer.view_inputs = vec![view_input_edge("edge-a")];
    assert!(matches!(
        store.begin_view_bootstrap(consumer.clone()).await.unwrap(),
        BeginViewBootstrapOutcome::Created(_)
    ));
    assert_eq!(
        store
            .read_view_dependency_graph_revision("default")
            .await
            .unwrap(),
        1
    );

    // A concurrent admission that validated against the stale snapshot must
    // fail closed with Conflict instead of silently passing the gate.
    let mut racing = orders_view_bootstrap_request();
    racing.program_id = "consumer-b".to_string();
    racing.view_id = "consumer-b".to_string();
    racing.view_inputs = vec![view_input_edge("edge-b")];
    assert_eq!(
        store.begin_view_bootstrap(racing.clone()).await.unwrap(),
        BeginViewBootstrapOutcome::Conflict
    );

    // Re-validating against the current revision succeeds and advances it.
    racing.expected_graph_revision = 1;
    assert!(matches!(
        store.begin_view_bootstrap(racing).await.unwrap(),
        BeginViewBootstrapOutcome::Created(_)
    ));
    assert_eq!(
        store
            .read_view_dependency_graph_revision("default")
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn arc_dyn_meta_store_forwarding_reads_live_graph_revision() {
    // Regression probe for the silent-Ok(0) Arc forwarding bug: the API holds
    // Arc<dyn MetaStore> and must observe the live revision, never a default.
    let store: Arc<dyn MetaStore> = Arc::new(InMemoryMetaStore::default());
    assert_eq!(
        store
            .read_view_dependency_graph_revision("default")
            .await
            .unwrap(),
        0
    );
    let mut consumer = orders_view_bootstrap_request();
    consumer.program_id = "consumer-a".to_string();
    consumer.view_id = "consumer-a".to_string();
    consumer.view_inputs = vec![view_input_edge("edge-a")];
    assert!(matches!(
        store.begin_view_bootstrap(consumer).await.unwrap(),
        BeginViewBootstrapOutcome::Created(_)
    ));
    assert_eq!(
        store
            .read_view_dependency_graph_revision("default")
            .await
            .unwrap(),
        1,
        "Arc<dyn MetaStore> must forward read_view_dependency_graph_revision to the inner store"
    );
}
