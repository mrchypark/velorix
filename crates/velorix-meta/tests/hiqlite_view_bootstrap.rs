#![cfg(feature = "hiqlite-backend")]

use std::{net::TcpListener, sync::Arc, time::Duration};

use tempfile::TempDir;
use velorix_core::standing_program::{
    RuntimeCheckpointInputCoverageV1, RuntimeCheckpointPartitionCoverageV1,
    RuntimeCheckpointRelationCoverageV1, RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
};
use velorix_meta::{
    AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest,
    BeginViewBootstrapOutcome, BeginViewBootstrapRequest, CommitIngestRangeOutcome,
    FixViewBootstrapActivationCutOutcome, FixViewBootstrapActivationCutRequest, HiqliteMetaStore,
    IngestRangeReservation, IngestSourceCutV1, IngestSourceRelationIdentityV1, MetaStore,
    PromoteViewBootstrapOutcome, PromoteViewBootstrapRequest,
    PublishStandingRuntimeCheckpointOutcome, PublishStandingRuntimeCheckpointRequest,
    ReserveIngestRangeOutcome, StandingRuntimeCheckpointPointer, StandingRuntimeOwnerToken,
    ViewBootstrapLifecycleV1,
};

#[tokio::test]
async fn hiqlite_activation_cas_fences_expired_owner_pointer_change_and_concurrent_workers() {
    let (_dir, store, client) = start_store().await;
    let first = reservation(0, 0, 10, "sha256:first");
    assert_eq!(
        store.reserve_ingest_range(first.clone()).await.unwrap(),
        ReserveIngestRangeOutcome::Reserved
    );
    assert_eq!(
        store.commit_ingest_range(first).await.unwrap(),
        CommitIngestRangeOutcome::Committed
    );
    let in_flight_new_partition = reservation(1, 0, 1, "sha256:new-partition");
    assert_eq!(
        store
            .reserve_ingest_range(in_flight_new_partition.clone())
            .await
            .unwrap(),
        ReserveIngestRangeOutcome::Reserved
    );
    let control = match store
        .begin_view_bootstrap(bootstrap_request())
        .await
        .unwrap()
    {
        BeginViewBootstrapOutcome::Created(control) => control,
        other => panic!("unexpected begin outcome: {other:?}"),
    };
    assert_eq!(control.bootstrap_cut.relations[0].partitions.len(), 2);
    assert_eq!(
        control.bootstrap_cut.relations[0]
            .partitions
            .iter()
            .find(|partition| partition.partition_id == 1)
            .unwrap()
            .committed_offset_exclusive,
        0
    );
    assert_eq!(
        store
            .commit_ingest_range(in_flight_new_partition)
            .await
            .unwrap(),
        CommitIngestRangeOutcome::Committed
    );

    let owner_a = acquire_owner(&store, "owner-a", 1_000).await;
    let pointer_1 = checkpoint_pointer(1, "a", &control.bootstrap_cut, &control.plan_hash, "", "");
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: None,
                candidate: pointer_1.clone(),
                owner: owner_a.clone(),
            })
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let expired_fix_without_takeover = store
        .fix_view_bootstrap_activation_cut(fix_request(&control, owner_a.clone()))
        .await
        .unwrap_err();
    assert!(expired_fix_without_takeover
        .to_string()
        .contains("owner token"));

    let owner_a_renewed = acquire_owner(&store, "owner-a", 1_000).await;
    assert!(owner_a_renewed.owner_epoch > owner_a.owner_epoch);
    assert!(matches!(
        store
            .fix_view_bootstrap_activation_cut(fix_request(&control, owner_a_renewed.clone(),))
            .await
            .unwrap(),
        FixViewBootstrapActivationCutOutcome::Fixed(_)
    ));
    let activation_cut = store
        .read_view_bootstrap("default", "orders-view", "orders-view")
        .await
        .unwrap()
        .unwrap()
        .activation_cut
        .unwrap();
    let pointer_2 = checkpoint_pointer(
        2,
        "b",
        &activation_cut,
        &control.plan_hash,
        &pointer_1.checkpoint_key,
        &pointer_1.manifest_hash,
    );
    assert_eq!(
        store
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: Some(pointer_1.clone()),
                candidate: pointer_2.clone(),
                owner: owner_a_renewed.clone(),
            })
            .await
            .unwrap(),
        PublishStandingRuntimeCheckpointOutcome::Published
    );
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let expired_promote_without_takeover = store
        .promote_view_bootstrap(promote_request(
            &control,
            pointer_2.clone(),
            owner_a_renewed.clone(),
        ))
        .await
        .unwrap_err();
    assert!(expired_promote_without_takeover
        .to_string()
        .contains("owner token"));

    let owner_b = acquire_owner(&store, "owner-b", 30_000).await;
    assert!(owner_b.owner_epoch > owner_a_renewed.owner_epoch);
    assert!(matches!(
        store
            .fix_view_bootstrap_activation_cut(fix_request(&control, owner_b.clone()))
            .await
            .unwrap(),
        FixViewBootstrapActivationCutOutcome::Duplicate(_)
    ));

    assert_eq!(
        store
            .promote_view_bootstrap(promote_request(&control, pointer_1, owner_b.clone(),))
            .await
            .unwrap(),
        PromoteViewBootstrapOutcome::Conflict
    );

    let store = Arc::new(store);
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let request = promote_request(&control, pointer_2.clone(), owner_b.clone());
        workers.push(tokio::spawn(async move {
            barrier.wait().await;
            store.promote_view_bootstrap(request).await.unwrap()
        }));
    }
    barrier.wait().await;
    let outcomes = [
        workers.remove(0).await.unwrap(),
        workers.remove(0).await.unwrap(),
    ];
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
    let active = store
        .read_view_bootstrap("default", "orders-view", "orders-view")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.lifecycle, ViewBootstrapLifecycleV1::Active);
    assert_eq!(active.active_checkpoint, Some(pointer_2));
    drop(store);
    client.shutdown().await.unwrap();
}

#[allow(clippy::field_reassign_with_default)]
async fn start_store() -> (TempDir, HiqliteMetaStore, hiqlite::Client) {
    let dir = TempDir::new().unwrap();
    let raft_addr = free_addr();
    let api_addr = free_addr();
    let mut config = hiqlite::NodeConfig::default();
    config.node_id = 1;
    config.nodes = vec![hiqlite::Node {
        id: 1,
        addr_raft: raft_addr.clone(),
        addr_api: api_addr.clone(),
    }];
    config.listen_addr_raft = "127.0.0.1".into();
    config.listen_addr_api = "127.0.0.1".into();
    config.data_dir = dir.path().to_string_lossy().into_owned().into();
    config.filename_db = "velorix-meta.db".into();
    config.secret_raft = "velorix-test-raft-secret".to_string();
    config.secret_api = "velorix-test-api-secret".to_string();
    config
        .enc_keys
        .append_new_random_with_id("velorix-test-key".to_string())
        .unwrap();
    config.health_check_delay_secs = 0;
    config.raft_config = hiqlite::NodeConfig::default_raft_config(100);
    let client = hiqlite::start_node(config).await.unwrap();
    let store = HiqliteMetaStore::new(client.clone()).await.unwrap();
    (dir, store, client)
}

fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn relation() -> IngestSourceRelationIdentityV1 {
    IngestSourceRelationIdentityV1 {
        relation_id: "orders".to_string(),
        relation_version: "v1".to_string(),
        relation_generation: 1,
        schema_fingerprint: format!("sha256:{}", "c".repeat(64)),
    }
}

fn reservation(partition_id: u32, start: u64, end: u64, digest: &str) -> IngestRangeReservation {
    IngestRangeReservation {
        stream_id: "orders".to_string(),
        partition_id,
        start_offset_inclusive: start,
        end_offset_exclusive: end,
        batch_key: format!("v1/ingest/orders/p={partition_id:010}/{start:020}-{end:020}.batch"),
        payload_digest: digest.to_string(),
        relation_id: "orders".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "c".repeat(64)),
        writer_epoch: 1,
    }
}

fn bootstrap_request() -> BeginViewBootstrapRequest {
    BeginViewBootstrapRequest {
        tenant_id: "default".to_string(),
        program_id: "orders-view".to_string(),
        view_id: "orders-view".to_string(),
        plan_hash: "sha256:plan".to_string(),
        view_spec_json: br#"{"view_id":"orders-view"}"#.to_vec(),
        relations: vec![relation()],
        view_inputs: Vec::new(),
        expected_graph_revision: 0,
    }
}

async fn acquire_owner(
    store: &HiqliteMetaStore,
    owner_id: &str,
    ttl_ms: u64,
) -> StandingRuntimeOwnerToken {
    match store
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: "default".to_string(),
            program_id: "orders-view".to_string(),
            view_id: "orders-view".to_string(),
            owner_id: owner_id.to_string(),
            ttl_ms,
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

fn checkpoint_pointer(
    epoch: u64,
    hash_seed: &str,
    cut: &IngestSourceCutV1,
    plan_hash: &str,
    previous_checkpoint_key: &str,
    previous_manifest_hash: &str,
) -> StandingRuntimeCheckpointPointer {
    let coverage = RuntimeCheckpointInputCoverageV1 {
        schema_version: RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
        view_generation: 1,
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
    let hash = hash_seed.repeat(64);
    StandingRuntimeCheckpointPointer {
        tenant_id: "default".to_string(),
        program_id: "orders-view".to_string(),
        view_id: "orders-view".to_string(),
        checkpoint_key: format!(
            "v1/standing-runtime-checkpoints/default/orders-view/orders-view/epochs/{epoch:020}/sha256/{hash}.checkpoint.json"
        ),
        logical_epoch: epoch,
        content_hash: format!("sha256:{hash}"),
        manifest_hash: format!("sha256:{hash}"),
        output_manifest_refs: Vec::new(),
        bootstrap_generation: 1,
        plan_hash: plan_hash.to_string(),
        coverage_hash: coverage.stable_hash().unwrap(),
        input_coverage: Some(coverage),
        previous_checkpoint_key: previous_checkpoint_key.to_string(),
        previous_manifest_hash: previous_manifest_hash.to_string(),
    }
}

fn fix_request(
    control: &velorix_meta::ViewBootstrapControlV1,
    owner: StandingRuntimeOwnerToken,
) -> FixViewBootstrapActivationCutRequest {
    FixViewBootstrapActivationCutRequest {
        tenant_id: control.tenant_id.clone(),
        program_id: control.program_id.clone(),
        view_id: control.view_id.clone(),
        bootstrap_generation: control.bootstrap_generation,
        plan_hash: control.plan_hash.clone(),
        owner,
    }
}

fn promote_request(
    control: &velorix_meta::ViewBootstrapControlV1,
    checkpoint: StandingRuntimeCheckpointPointer,
    owner: StandingRuntimeOwnerToken,
) -> PromoteViewBootstrapRequest {
    PromoteViewBootstrapRequest {
        tenant_id: control.tenant_id.clone(),
        program_id: control.program_id.clone(),
        view_id: control.view_id.clone(),
        bootstrap_generation: control.bootstrap_generation,
        plan_hash: control.plan_hash.clone(),
        checkpoint,
        owner,
    }
}
