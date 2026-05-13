use std::{
    env,
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use k8s_openapi::{
    api::{
        coordination::v1::Lease,
        core::v1::{Namespace, Pod},
    },
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    api::{Api, DeleteParams, PostParams},
    Client, ResourceExt,
};
use object_store::memory::InMemory;
use velorix_control::lease::PartitionLeaseClient;
use velorix_k8s::{
    crd::{ObjectStoreAuthorityRef, VelorixWorkerShard, VelorixWorkerShardSpec},
    lease::{partition_lease_identity, KubeLeaseApi, KubernetesPartitionLeaseClient},
    worker_shard::{
        handle_worker_shard_event, handle_worker_shard_event_with_command_executor,
        partition_lease_key_from_worker_shard, watch_worker_shards_with_command_executor,
        worker_shard_pod_name, CheckpointPublisherEpochStore,
        KubernetesPodWorkerShardCommandExecutor, WorkerShardCommand, WorkerShardCommandExecutor,
        WorkerShardEvent, WorkerShardPodTemplate, WorkerShardReconcileConfig,
        WorkerShardReconcileInput,
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

#[tokio::test]
async fn live_worker_shard_reconciles_and_creates_worker_pod_when_enabled(
) -> Result<(), Box<dyn Error>> {
    if env::var("VELORIX_K8S_INTEGRATION").as_deref() != Ok("1") {
        eprintln!("skipping live Kubernetes worker pod test; set VELORIX_K8S_INTEGRATION=1");
        return Ok(());
    }

    let namespace =
        env::var("VELORIX_K8S_NAMESPACE").unwrap_or_else(|_| "velorix-live".to_string());
    let worker_image = env::var("VELORIX_K8S_WORKER_IMAGE")
        .unwrap_or_else(|_| "registry.k8s.io/pause:3.10".to_string());
    let suffix = unique_suffix()?;
    let owner_id = format!("worker-{suffix}");
    let client = Client::try_default().await?;
    ensure_namespace(client.clone(), &namespace).await?;

    let shard = worker_shard_with_owner(&namespace, &suffix, &owner_id);
    let key = partition_lease_key_from_worker_shard(&shard)?;
    let lease_client = KubernetesPartitionLeaseClient::new(KubeLeaseApi::new(client.clone()));
    let publisher = CheckpointPublisher::new(std::sync::Arc::new(InMemory::new()));
    let epoch_store = CheckpointPublisherEpochStore::new(publisher.clone());
    let executor = KubernetesPodWorkerShardCommandExecutor::new(
        client.clone(),
        &namespace,
        WorkerShardPodTemplate::new(worker_image.clone())?,
    );

    let output = handle_worker_shard_event_with_command_executor(
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
        &executor,
    )
    .await?
    .expect("applied worker shard should reconcile");

    assert_eq!(
        output.commands,
        vec![
            WorkerShardCommand::AcquireLease {
                owner_id: owner_id.clone(),
            },
            WorkerShardCommand::PersistEpochRecord {
                owner_id: owner_id.clone(),
                owner_epoch: 1,
            },
            WorkerShardCommand::StartWorker {
                owner_id: owner_id.clone(),
                owner_epoch: 1,
            },
        ]
    );

    let record = publisher
        .read_ownership_epoch_record(&key.stream_id, key.partition_id, 1)
        .await?;
    assert_eq!(record.owner_id, owner_id);
    assert_eq!(record.lease_identity, partition_lease_identity(&key));

    let pod_name = worker_shard_pod_name(&record.owner_id, 1);
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let pod = pod_api.get(&pod_name).await?;
    assert_eq!(pod.metadata.name.as_deref(), Some(pod_name.as_str()));
    assert_eq!(
        pod.metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get("control.velorix.io/owner-epoch"))
            .map(String::as_str),
        Some("1")
    );
    let container = pod
        .spec
        .as_ref()
        .and_then(|spec| spec.containers.first())
        .ok_or("worker Pod is missing its first container")?;
    assert_eq!(container.image.as_deref(), Some(worker_image.as_str()));
    assert_eq!(
        container_env_value(container, "VELORIX_WORKER_OWNER_ID"),
        Some(record.owner_id.as_str())
    );
    assert_eq!(
        container_env_value(container, "VELORIX_WORKER_OWNER_EPOCH"),
        Some("1")
    );

    executor.stop_worker(&record.owner_id, 1).await?;
    wait_for_pod_deleted(&pod_api, &pod_name).await?;
    lease_client
        .release(&key, &record.owner_id, 1, unix_ms()?)
        .await?;
    delete_lease(client, &key).await?;

    Ok(())
}

#[tokio::test]
async fn live_worker_shard_watch_loop_creates_worker_pod_when_enabled() -> Result<(), Box<dyn Error>>
{
    if env::var("VELORIX_K8S_INTEGRATION").as_deref() != Ok("1") {
        eprintln!(
            "skipping live Kubernetes worker shard watch-loop test; set VELORIX_K8S_INTEGRATION=1"
        );
        return Ok(());
    }

    let namespace =
        env::var("VELORIX_K8S_NAMESPACE").unwrap_or_else(|_| "velorix-live".to_string());
    let worker_image = env::var("VELORIX_K8S_WORKER_IMAGE")
        .unwrap_or_else(|_| "registry.k8s.io/pause:3.10".to_string());
    let suffix = unique_suffix()?;
    let owner_id = format!("watch-worker-{suffix}");
    let client = Client::try_default().await?;
    ensure_namespace(client.clone(), &namespace).await?;

    let shard = worker_shard_with_owner(&namespace, &suffix, &owner_id);
    let key = partition_lease_key_from_worker_shard(&shard)?;
    let lease_client = KubernetesPartitionLeaseClient::new(KubeLeaseApi::new(client.clone()));
    let publisher = CheckpointPublisher::new(std::sync::Arc::new(InMemory::new()));
    let epoch_store = CheckpointPublisherEpochStore::new(publisher.clone());
    let executor = KubernetesPodWorkerShardCommandExecutor::new(
        client.clone(),
        &namespace,
        WorkerShardPodTemplate::new(worker_image.clone())?,
    );

    let watch_namespace = namespace.clone();
    let watch_client = client.clone();
    let watch_task = tokio::spawn(async move {
        watch_worker_shards_with_command_executor(
            watch_client,
            &watch_namespace,
            lease_client,
            epoch_store,
            |_| WorkerShardReconcileInput {
                now_unix_ms: unix_ms().expect("system clock should be after Unix epoch"),
                ttl_ms: 60_000,
                running_worker: None,
                config: WorkerShardReconcileConfig {
                    created_at: "2026-05-12T17:33:23Z".to_string(),
                    previous_checkpoint_version: None,
                },
            },
            executor,
        )
        .await
    });

    let shard_api: Api<VelorixWorkerShard> = Api::namespaced(client.clone(), &namespace);
    shard_api.create(&PostParams::default(), &shard).await?;

    let pod_name = worker_shard_pod_name(&owner_id, 1);
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let pod = wait_for_pod(&pod_api, &pod_name).await?;
    assert_eq!(pod.metadata.name.as_deref(), Some(pod_name.as_str()));
    let container = pod
        .spec
        .as_ref()
        .and_then(|spec| spec.containers.first())
        .ok_or("worker Pod is missing its first container")?;
    assert_eq!(container.image.as_deref(), Some(worker_image.as_str()));
    assert_eq!(
        container_env_value(container, "VELORIX_WORKER_OWNER_ID"),
        Some(owner_id.as_str())
    );
    assert_eq!(
        container_env_value(container, "VELORIX_WORKER_OWNER_EPOCH"),
        Some("1")
    );

    let record = publisher
        .read_ownership_epoch_record(&key.stream_id, key.partition_id, 1)
        .await?;
    assert_eq!(record.owner_id, owner_id);
    assert_eq!(record.lease_identity, partition_lease_identity(&key));

    watch_task.abort();
    shard_api
        .delete(shard.name_any().as_str(), &DeleteParams::default())
        .await?;
    delete_pod_if_present(&pod_api, &pod_name).await?;
    wait_for_pod_deleted(&pod_api, &pod_name).await?;
    let lease_client = KubernetesPartitionLeaseClient::new(KubeLeaseApi::new(client.clone()));
    lease_client
        .release(&key, &record.owner_id, 1, unix_ms()?)
        .await?;
    delete_lease(client, &key).await?;

    Ok(())
}

fn worker_shard(namespace: &str, suffix: &str) -> VelorixWorkerShard {
    worker_shard_with_owner(namespace, suffix, "worker-a")
}

fn worker_shard_with_owner(namespace: &str, suffix: &str, owner_id: &str) -> VelorixWorkerShard {
    let mut shard = VelorixWorkerShard::new(
        &format!("orders-p0-{suffix}"),
        VelorixWorkerShardSpec {
            worker_id: "worker-a".to_string(),
            view_id: "live-worker-view".to_string(),
            stream_id: format!("orders-{suffix}"),
            partition_id: 0,
            desired_owner_id: owner_id.to_string(),
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

fn container_env_value<'a>(
    container: &'a k8s_openapi::api::core::v1::Container,
    name: &str,
) -> Option<&'a str> {
    container.env.as_ref()?.iter().find_map(|env| {
        if env.name == name {
            env.value.as_deref()
        } else {
            None
        }
    })
}

async fn wait_for_pod(pod_api: &Api<Pod>, pod_name: &str) -> Result<Pod, Box<dyn Error>> {
    for _ in 0..100 {
        if let Some(pod) = pod_api.get_opt(pod_name).await? {
            return Ok(pod);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(format!("worker Pod {pod_name} was not created within 10s").into())
}

async fn delete_pod_if_present(pod_api: &Api<Pod>, pod_name: &str) -> Result<(), Box<dyn Error>> {
    match pod_api.delete(pod_name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(response)) if response.code == 404 => Ok(()),
        Err(error) => Err(Box::new(error)),
    }
}

async fn wait_for_pod_deleted(pod_api: &Api<Pod>, pod_name: &str) -> Result<(), Box<dyn Error>> {
    for _ in 0..50 {
        if pod_api.get_opt(pod_name).await?.is_none() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(format!("worker Pod {pod_name} was not deleted within 5s").into())
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
