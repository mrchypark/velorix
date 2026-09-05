#![cfg(feature = "rhiza-backend")]

//! Live embedded Rhiza KV/MetaStore recovery drill. Run through
//! `scripts/check-rhiza-recovery.sh`; it is ignored because it needs MinIO.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tempfile::TempDir;
use velorix_meta::rhiza_meta::RhizaKvMetaStore;
use velorix_meta::{
    AcquireStandingRuntimeOwnerOutcome, AcquireStandingRuntimeOwnerRequest, MetaStore,
    MetaStoreError, PublishStandingRuntimeCheckpointOutcome,
    PublishStandingRuntimeCheckpointRequest, StandingRuntimeCheckpointPointer,
    StandingRuntimeOwnerToken,
};
use velorix_storage::object_key::ObjectKey;

mod common;

const NODE_IDS: [&str; 3] = ["node-a", "node-b", "node-c"];
const TENANT: &str = "tenant-recovery";
const PROGRAM: &str = "program-recovery";
const VIEW: &str = "view-recovery";

struct Node {
    id: &'static str,
    data_dir: PathBuf,
    store: Option<RhizaKvMetaStore>,
}

fn env_or(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_string())
}

fn node_config(root: &Path, id: &'static str, index: usize) -> rhizadb::Config {
    let base_port: u16 = env_or("RHIZA_RECOVERY_BASE_PORT", "28100")
        .parse()
        .expect("RHIZA_RECOVERY_BASE_PORT must be a port");
    let members = NODE_IDS
        .iter()
        .enumerate()
        .map(|(member_index, member_id)| {
            serde_json::json!({
                "node_id": member_id,
                "url": format!("http://127.0.0.1:{}", base_port + member_index as u16),
                "peer_url": format!("quic://127.0.0.1:{}", base_port + 100 + member_index as u16),
                "log_url": "",
                "token": format!("velorix-test-voter-{}", member_index + 1),
            })
        })
        .collect::<Vec<_>>();
    rhizadb::Config::new(root.join(id))
        .node_id(id)
        .cluster_id(env_or("RHIZA_RECOVERY_CLUSTER_ID", "velorix-recovery-test"))
        .bind_addr(format!("127.0.0.1:{}", base_port + index as u16))
        .peer_addr(format!("127.0.0.1:{}", base_port + 100 + index as u16))
        .set_option("Members", serde_json::json!(members))
        .set_option("ObjStoreProvider", serde_json::json!("s3"))
        // The native S3 provider expects host:port here; insecure selects HTTP.
        .set_option(
            "ObjStoreEndpoint",
            serde_json::json!(env_or("RHIZA_RECOVERY_S3_ENDPOINT", "127.0.0.1:29000")),
        )
        .set_option(
            "ObjStoreBucket",
            serde_json::json!(env_or("RHIZA_RECOVERY_S3_BUCKET", "velorix-rhiza-recovery")),
        )
        .set_option(
            "ObjStorePrefix",
            serde_json::json!(env_or("RHIZA_RECOVERY_S3_PREFIX", "recovery-test")),
        )
        .set_option("ObjStoreRegion", serde_json::json!("us-east-1"))
        .set_option("ObjStoreInsecure", serde_json::json!(true))
        .set_option(
            "ObjStoreAccessKey",
            serde_json::json!(env_or(
                "RHIZA_RECOVERY_S3_ACCESS_KEY",
                "velorix-test-access"
            )),
        )
        .set_option(
            "ObjStoreSecretKey",
            serde_json::json!(env_or(
                "RHIZA_RECOVERY_S3_SECRET_KEY",
                "velorix-test-secret"
            )),
        )
        .set_option("ObjStoreDurability", serde_json::json!("before-ack"))
        .set_option("ObjStoreSyncInterval", serde_json::json!(100_000_000_u64))
        .set_option("ObjStoreBatchDelay", serde_json::json!(0_u64))
        // Keep auto-checkpoint publication away from shutdown; before-ack
        // archive sync is the acknowledgement barrier exercised here.
        .set_option(
            "CheckpointInterval",
            serde_json::json!(3_600_000_000_000_u64),
        )
        .set_option("CheckpointTailBytes", serde_json::json!(1_048_576_u64))
}

async fn open_node(root: &Path, index: usize) -> Node {
    let id = NODE_IDS[index];
    let data_dir = root.join(id);
    let store = RhizaKvMetaStore::open_config(node_config(root, id, index))
        .await
        .unwrap_or_else(|error| panic!("open {id}: {error}"));
    Node {
        id,
        data_dir,
        store: Some(store),
    }
}

async fn close_node(node: &mut Node) {
    if let Some(store) = node.store.take() {
        if let Err(error) = store.close().await {
            let text = error.to_string();
            let expected_shutdown_race = [
                "archive maintenance is active",
                "checkpoint publisher is active",
                "archive state changed during I/O",
                "quorum_unavailable",
                "context deadline exceeded",
                "timeout: no recent network activity",
            ]
            .iter()
            .any(|marker| text.contains(marker));
            assert!(
                expected_shutdown_race,
                "unexpected native shutdown failure for {}: {error}",
                node.id
            );
            // The native handle is consumed and the subsequent fresh-open
            // phase verifies that its lock/resources were actually released.
            eprintln!("rhiza recovery: {} close warning: {error}", node.id);
        }
    }
}

async fn wait_catalog(store: &RhizaKvMetaStore) {
    let expected = common::orders_relation_catalog("v1");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match store.read_relation_catalog("orders", "v1").await {
            Ok(catalog) if catalog == expected => return,
            Ok(_) if tokio::time::Instant::now() < deadline => {}
            Err(_) if tokio::time::Instant::now() < deadline => {}
            Ok(other) => panic!("catalog mismatch after recovery: {other:?}"),
            Err(error) => panic!("catalog was not recovered: {error}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn checkpoint_pointer(epoch: u64, hash_char: char) -> StandingRuntimeCheckpointPointer {
    let content_hash = format!("sha256:{}", hash_char.to_string().repeat(64));
    let key = ObjectKey::standing_runtime_checkpoint(TENANT, PROGRAM, VIEW, epoch, &content_hash)
        .expect("checkpoint object key")
        .to_string();
    StandingRuntimeCheckpointPointer {
        tenant_id: TENANT.into(),
        program_id: PROGRAM.into(),
        view_id: VIEW.into(),
        checkpoint_key: key,
        logical_epoch: epoch,
        content_hash: content_hash.clone(),
        manifest_hash: content_hash,
        output_manifest_refs: vec![],
        bootstrap_generation: 0,
        plan_hash: String::new(),
        coverage_hash: String::new(),
        input_coverage: None,
        previous_checkpoint_key: String::new(),
        previous_manifest_hash: String::new(),
    }
}

async fn acquire_owner(store: &RhizaKvMetaStore) -> StandingRuntimeOwnerToken {
    match store
        .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
            tenant_id: TENANT.into(),
            program_id: PROGRAM.into(),
            view_id: VIEW.into(),
            owner_id: "owner-recovery".into(),
            ttl_ms: 300_000,
        })
        .await
        .expect("owner CAS")
    {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim)
        | AcquireStandingRuntimeOwnerOutcome::Renewed(claim) => StandingRuntimeOwnerToken {
            tenant_id: claim.tenant_id,
            program_id: claim.program_id,
            view_id: claim.view_id,
            owner_id: claim.owner_id,
            owner_epoch: claim.owner_epoch,
        },
        other => panic!("owner acquisition unexpectedly conflicted: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires local MinIO; run scripts/check-rhiza-recovery.sh"]
async fn rhiza_three_node_kv_meta_quorum_loss_and_empty_directory_recovery() {
    let (root, _temporary_root) = match std::env::var("RHIZA_RECOVERY_WORKDIR") {
        Ok(path) => {
            let path = PathBuf::from(path);
            std::fs::create_dir_all(&path).expect("recovery evidence work root");
            (path, None)
        }
        Err(_) => {
            let temp = TempDir::new().expect("temporary node root");
            (temp.path().to_path_buf(), Some(temp))
        }
    };
    let mut nodes = vec![
        open_node(&root, 0).await,
        open_node(&root, 1).await,
        open_node(&root, 2).await,
    ];
    for node in &nodes {
        let capabilities = node
            .store
            .as_ref()
            .unwrap()
            .read_meta_store_capabilities()
            .await
            .expect("capabilities read");
        assert_eq!(
            capabilities.standing_runtime_fencing.backend_name,
            "rhiza-kv"
        );
        assert!(capabilities.partition_authority.durable_across_restart);
    }

    let first = nodes[0].store.as_ref().unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match first
            .store_relation_catalog(common::orders_relation_catalog("v1"))
            .await
        {
            Ok(_) => break,
            Err(error) if tokio::time::Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("initial quorum never became writable: {error}"),
        }
    }
    let owner = acquire_owner(first).await;
    let pointer = checkpoint_pointer(7, 'a');
    assert!(matches!(
        first
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: None,
                candidate: pointer.clone(),
                owner: owner.clone(),
            })
            .await
            .expect("checkpoint CAS"),
        PublishStandingRuntimeCheckpointOutcome::Published
    ));
    // This is the acknowledged metadata record whose exact value must survive.
    for node in &nodes {
        wait_catalog(node.store.as_ref().unwrap()).await;
        let claim = node
            .store
            .as_ref()
            .unwrap()
            .read_standing_runtime_owner(TENANT, PROGRAM, VIEW)
            .await
            .expect("owner read")
            .expect("owner exists");
        assert_eq!(claim.owner_epoch, owner.owner_epoch);
        assert_eq!(
            node.store
                .as_ref()
                .unwrap()
                .read_standing_runtime_checkpoint(TENANT, PROGRAM, VIEW)
                .await
                .expect("checkpoint read"),
            Some(pointer.clone())
        );
    }

    close_node(&mut nodes[2]).await;
    let second = acquire_owner(nodes[0].store.as_ref().unwrap()).await;
    assert_eq!(second.owner_epoch, owner.owner_epoch);
    let expected_owner = nodes[0]
        .store
        .as_ref()
        .unwrap()
        .read_standing_runtime_owner(TENANT, PROGRAM, VIEW)
        .await
        .expect("owner read after one-node loss")
        .expect("owner after one-node loss");

    // Two live nodes race to publish different successors from the same
    // checkpoint. The root CAS must publish exactly one complete winner and
    // return Conflict for the loser, regardless of proposer.
    let mut candidate8 = checkpoint_pointer(8, 'b');
    candidate8.previous_checkpoint_key = pointer.checkpoint_key.clone();
    candidate8.previous_manifest_hash = pointer.manifest_hash.clone();
    let mut candidate9 = checkpoint_pointer(9, 'c');
    candidate9.previous_checkpoint_key = pointer.checkpoint_key.clone();
    candidate9.previous_manifest_hash = pointer.manifest_hash.clone();
    let (left, right) = tokio::join!(
        nodes[0]
            .store
            .as_ref()
            .unwrap()
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: Some(pointer.clone()),
                candidate: candidate8.clone(),
                owner: second.clone(),
            }),
        nodes[1]
            .store
            .as_ref()
            .unwrap()
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous: Some(pointer.clone()),
                candidate: candidate9.clone(),
                owner: second.clone(),
            })
    );
    let outcomes = [
        left.expect("node-a checkpoint race"),
        right.expect("node-b checkpoint race"),
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, PublishStandingRuntimeCheckpointOutcome::Published))
            .count(),
        1,
        "root CAS must publish one checkpoint race winner"
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, PublishStandingRuntimeCheckpointOutcome::Conflict))
            .count(),
        1,
        "root CAS must conflict the stale checkpoint race loser"
    );
    let winner = if matches!(
        outcomes[0],
        PublishStandingRuntimeCheckpointOutcome::Published
    ) {
        candidate8
    } else {
        candidate9
    };
    for node in &nodes[..2] {
        assert_eq!(
            node.store
                .as_ref()
                .unwrap()
                .read_standing_runtime_checkpoint(TENANT, PROGRAM, VIEW)
                .await
                .expect("winning checkpoint read"),
            Some(winner.clone())
        );
    }
    close_node(&mut nodes[1]).await;
    let no_quorum = nodes[0]
        .store
        .as_ref()
        .unwrap()
        .read_relation_catalog("orders", "v1")
        .await;
    let no_quorum = no_quorum.expect_err("quorum loss must fail closed");
    assert!(
        matches!(
            &no_quorum,
            MetaStoreError::Rhiza(message) if message.contains("quorum_unavailable")
        ),
        "quorum loss must report typed quorum_unavailable: {no_quorum}"
    );
    let write_without_quorum = nodes[0]
        .store
        .as_ref()
        .unwrap()
        .store_relation_catalog(common::orders_relation_catalog("v2"))
        .await;
    assert!(matches!(
        write_without_quorum,
        Err(MetaStoreError::Rhiza(message)) if message.contains("quorum_unavailable")
    ));

    close_node(&mut nodes[0]).await;
    // Retain the old local state instead of deleting it; each original path is
    // now genuinely empty and all recovery evidence remains inspectable.
    for node in &nodes {
        let backup = node.data_dir.with_extension("retained-before-recovery");
        std::fs::rename(&node.data_dir, &backup)
            .unwrap_or_else(|error| panic!("retain old working directory: {error}"));
    }
    for (index, node) in nodes.iter_mut().enumerate() {
        *node = open_node(&root, index).await;
    }
    for node in &nodes {
        wait_catalog(node.store.as_ref().unwrap()).await;
        let claim = node
            .store
            .as_ref()
            .unwrap()
            .read_standing_runtime_owner(TENANT, PROGRAM, VIEW)
            .await
            .expect("recovered owner read")
            .expect("recovered owner");
        assert_eq!(claim.tenant_id, TENANT);
        assert_eq!(claim.program_id, PROGRAM);
        assert_eq!(claim.view_id, VIEW);
        assert_eq!(claim.owner_id, "owner-recovery");
        assert_eq!(
            claim, expected_owner,
            "owner claim must survive exact restart"
        );
        assert_eq!(
            node.store
                .as_ref()
                .unwrap()
                .read_standing_runtime_checkpoint(TENANT, PROGRAM, VIEW)
                .await
                .expect("recovered checkpoint read"),
            Some(winner.clone())
        );
    }
    for node in &mut nodes {
        close_node(node).await;
    }
}
