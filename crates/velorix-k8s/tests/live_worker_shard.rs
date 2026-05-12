use std::{
    env,
    error::Error,
    time::{SystemTime, UNIX_EPOCH},
};

use k8s_openapi::{
    api::{coordination::v1::Lease, core::v1::Namespace},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    api::{Api, DeleteParams, PostParams},
    Client,
};
use object_store::memory::InMemory;
use velorix_control::lease::PartitionLeaseClient;
use velorix_k8s::{
    crd::{ObjectStoreAuthorityRef, VelorixWorkerShard, VelorixWorkerShardSpec},
    lease::{partition_lease_identity, KubeLeaseApi, KubernetesPartitionLeaseClient},
    worker_shard::{
        handle_worker_shard_event, partition_lease_key_from_worker_shard,
        CheckpointPublisherEpochStore, WorkerShardCommand, WorkerShardEvent,
        WorkerShardReconcileConfig, WorkerShardReconcileInput,
    },
};
use velorix_storage::state::CheckpointPublisher;

#[tokio::test]
async fn live_worker_shard_reconciles_lease_and_epoch_record_when_enabled(
) -> Result<(), Box<dyn Error>> {
    if env::var("VELORIX_K8S_INTEGRATION").as_deref() != Ok("1") {
        eprintln!("skipping live Kubernetes worker shard test; set VELORIX_K8S_INTEGRATION=1");
        return Ok(());
    }

    let namespace =
        env::var("VELORIX_K8S_NAMESPACE").unwrap_or_else(|_| "velorix-live".to_string());
    let suffix = unique_suffix()?;
    let client = Client::try_default().await?;
    ensure_namespace(client.clone(), &namespace).await?;

    let shard = worker_shard(&namespace, &suffix);
    let key = partition_lease_key_from_worker_shard(&shard)?;
    let lease_client = KubernetesPartitionLeaseClient::new(KubeLeaseApi::new(client.clone()));
    let publisher = CheckpointPublisher::new(std::sync::Arc::new(InMemory::new()));
    let epoch_store = CheckpointPublisherEpochStore::new(publisher.clone());

    let output = handle_worker_shard_event(
        WorkerShardEvent::Applied(shard),
        &lease_client,
        &epoch_store,
        WorkerShardReconcileInput {
            now_unix_ms: unix_ms()?,
            ttl_ms: 60_000,
            running_worker: None,
            config: WorkerShardReconcileConfig {
                created_at: "2026-05-12T17:33:23Z".to_string(),
                previous_checkpoint_version: None,
            },
        },
    )
    .await?
    .expect("applied worker shard should reconcile");

    assert_eq!(
        output.commands,
        vec![
            WorkerShardCommand::AcquireLease {
                owner_id: "worker-a".to_string(),
            },
            WorkerShardCommand::PersistEpochRecord {
                owner_id: "worker-a".to_string(),
                owner_epoch: 1,
            },
            WorkerShardCommand::StartWorker {
                owner_id: "worker-a".to_string(),
                owner_epoch: 1,
            },
        ]
    );

    let record = publisher
        .read_ownership_epoch_record(&key.stream_id, key.partition_id, 1)
        .await?;
    assert_eq!(record.owner_id, "worker-a");
    assert_eq!(record.lease_identity, partition_lease_identity(&key));

    lease_client
        .release(&key, "worker-a", 1, unix_ms()?)
        .await?;
    delete_lease(client, &key).await?;

    Ok(())
}

fn worker_shard(namespace: &str, suffix: &str) -> VelorixWorkerShard {
    let mut shard = VelorixWorkerShard::new(
        &format!("orders-p0-{suffix}"),
        VelorixWorkerShardSpec {
            worker_id: "worker-a".to_string(),
            view_id: "live-worker-view".to_string(),
            stream_id: format!("orders-{suffix}"),
            partition_id: 0,
            desired_owner_id: "worker-a".to_string(),
            authority: ObjectStoreAuthorityRef {
                store_id: "primary".to_string(),
                namespace: "analytics".to_string(),
            },
        },
    );
    shard.metadata.namespace = Some(namespace.to_string());
    shard.metadata.generation = Some(1);
    shard
}

async fn ensure_namespace(client: Client, namespace: &str) -> Result<(), Box<dyn Error>> {
    let api: Api<Namespace> = Api::all(client);
    let namespace = Namespace {
        metadata: ObjectMeta {
            name: Some(namespace.to_string()),
            ..ObjectMeta::default()
        },
        ..Namespace::default()
    };

    match api.create(&PostParams::default(), &namespace).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(response)) if response.code == 409 => Ok(()),
        Err(error) => Err(Box::new(error)),
    }
}

async fn delete_lease(
    client: Client,
    key: &velorix_control::lease::PartitionLeaseKey,
) -> Result<(), Box<dyn Error>> {
    let name = partition_lease_identity(key)
        .rsplit('/')
        .next()
        .ok_or("missing lease name")?
        .to_string();
    let api: Api<Lease> = Api::namespaced(client, &key.namespace);

    match api.delete(&name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(response)) if response.code == 404 => Ok(()),
        Err(error) => Err(Box::new(error)),
    }
}

fn unique_suffix() -> Result<String, Box<dyn Error>> {
    Ok(format!("{}-{}", std::process::id(), unix_ms()?))
}

fn unix_ms() -> Result<u64, Box<dyn Error>> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}
