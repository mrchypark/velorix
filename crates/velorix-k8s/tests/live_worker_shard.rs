use std::{
    env,
    error::Error,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
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
    api::{Api, DeleteParams, ListParams, PostParams},
    Client, ResourceExt,
};
use object_store::local::LocalFileSystem;
use velorix_control::lease::PartitionLeaseClient;
use velorix_k8s::{
    crd::{ObjectStoreAuthorityRef, VelorixWorkerShard, VelorixWorkerShardSpec},
    lease::{partition_lease_identity, KubeLeaseApi, KubernetesPartitionLeaseClient},
    startup::{validate_operator_authority, OperatorAuthorityStartupComponents},
    worker_shard::{
        build_kubernetes_worker_shard_operator_runtime, handle_worker_shard_event,
        partition_lease_key_from_worker_shard,
        resync_worker_shards_before_watch_with_kubernetes_runtime,
        runtime_identity_from_worker_shard, watch_worker_shards_with_kubernetes_runtime,
        worker_shard_pod_name_for_identity, WorkerShardCommand, WorkerShardEpochStore,
        WorkerShardEvent, WorkerShardPodTemplate, WorkerShardReconcileConfig,
        WorkerShardReconcileInput, WorkerShardResyncOptions,
    },
};
use velorix_storage::ownership::OwnershipEpochRecord;

#[tokio::test]
async fn live_worker_shard_epoch_store_reads_record_after_checked_authority_reconstruction_on_local_filesystem(
) -> Result<(), Box<dyn Error>> {
    let suffix = unique_suffix()?;
    let authority = LiveWorkerShardAuthority::new(&suffix)?;
    let components = authority.startup_components().await?;
    let epoch_store = components.worker_shard_epoch_store()?;
    let record = OwnershipEpochRecord {
        stream_id: format!("orders-{suffix}"),
        partition_id: 0,
        owner_id: "worker-a".to_string(),
        owner_epoch: 1,
        lease_identity: format!("default/live-worker-view/orders-{suffix}/0"),
        created_at: "2026-05-12T17:33:23Z".to_string(),
        previous_epoch: None,
        previous_checkpoint_version: None,
    };

    epoch_store.create(record.clone()).await?;

    drop(epoch_store);
    drop(components);
    let restarted_components = authority.startup_components().await?;
    let restarted_epoch_store = restarted_components.worker_shard_epoch_store()?;

    assert_eq!(
        restarted_epoch_store
            .read(&record.stream_id, record.partition_id, record.owner_epoch)
            .await?,
        Some(record)
    );

    Ok(())
}

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
    let authority = LiveWorkerShardAuthority::new(&suffix)?;
    let components = authority.startup_components().await?;
    let epoch_store = components.worker_shard_epoch_store()?;

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

    let record = epoch_store
        .read(&key.stream_id, key.partition_id, 1)
        .await?
        .expect("epoch record should be persisted");
    assert_eq!(record.owner_id, "worker-a");
    assert_eq!(record.lease_identity, partition_lease_identity(&key));

    drop(epoch_store);
    drop(components);
    let restarted_components = authority.startup_components().await?;
    let restarted_epoch_store = restarted_components.worker_shard_epoch_store()?;
    let restarted_record = restarted_epoch_store
        .read(&key.stream_id, key.partition_id, 1)
        .await?
        .expect("epoch record should survive checked authority reconstruction");
    assert_eq!(restarted_record, record);

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
    let authority = LiveWorkerShardAuthority::new(&suffix)?;
    let components = authority.startup_components().await?;
    let epoch_store = components.worker_shard_epoch_store()?;
    let runtime = build_kubernetes_worker_shard_operator_runtime(
        client.clone(),
        &namespace,
        &components,
        WorkerShardPodTemplate::new(worker_image.clone())?,
    )?;

    let output = runtime
        .handle_event(
            WorkerShardEvent::Applied(shard),
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

    let record = epoch_store
        .read(&key.stream_id, key.partition_id, 1)
        .await?
        .expect("epoch record should be persisted");
    assert_eq!(record.owner_id, owner_id);
    assert_eq!(record.lease_identity, partition_lease_identity(&key));

    drop(epoch_store);
    let restarted_components = authority.startup_components().await?;
    let restarted_epoch_store = restarted_components.worker_shard_epoch_store()?;
    let restarted_record = restarted_epoch_store
        .read(&key.stream_id, key.partition_id, 1)
        .await?
        .expect("epoch record should survive checked authority reconstruction");
    assert_eq!(restarted_record, record);

    let identity = runtime_identity_from_worker_shard(
        &worker_shard_with_owner(&namespace, &suffix, &record.owner_id),
        &record.owner_id,
        1,
    )?;
    let pod_name = worker_shard_pod_name_for_identity(&identity);
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
    assert_eq!(
        container_env_value(container, "VELORIX_WORKER_STREAM_ID"),
        Some(key.stream_id.as_str())
    );

    delete_pod_if_present(&pod_api, &pod_name).await?;
    wait_for_pod_deleted(&pod_api, &pod_name).await?;
    let lease_client = KubernetesPartitionLeaseClient::new(KubeLeaseApi::new(client.clone()));
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
    let authority = LiveWorkerShardAuthority::new(&suffix)?;
    let components = authority.startup_components().await?;
    let epoch_store_for_assert = components.worker_shard_epoch_store()?;

    let watch_namespace = namespace.clone();
    let watch_client = client.clone();
    let watch_components = components.clone();
    let watch_worker_image = worker_image.clone();
    let watch_task = tokio::spawn(async move {
        watch_worker_shards_with_kubernetes_runtime(
            watch_client,
            &watch_namespace,
            &watch_components,
            WorkerShardPodTemplate::new(watch_worker_image.clone())
                .expect("worker image should be valid"),
            |_| WorkerShardReconcileInput {
                now_unix_ms: unix_ms().expect("system clock should be after Unix epoch"),
                ttl_ms: 60_000,
                running_worker: None,
                config: WorkerShardReconcileConfig {
                    created_at: "2026-05-12T17:33:23Z".to_string(),
                    previous_checkpoint_version: None,
                },
            },
        )
        .await
    });

    let shard_api: Api<VelorixWorkerShard> = Api::namespaced(client.clone(), &namespace);
    shard_api.create(&PostParams::default(), &shard).await?;

    let identity = runtime_identity_from_worker_shard(&shard, &owner_id, 1)?;
    let pod_name = worker_shard_pod_name_for_identity(&identity);
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
    assert_eq!(
        container_env_value(container, "VELORIX_WORKER_STREAM_ID"),
        Some(key.stream_id.as_str())
    );

    let record = epoch_store_for_assert
        .read(&key.stream_id, key.partition_id, 1)
        .await?
        .expect("epoch record should be persisted");
    assert_eq!(record.owner_id, owner_id);
    assert_eq!(record.lease_identity, partition_lease_identity(&key));

    drop(epoch_store_for_assert);
    let restarted_components = authority.startup_components().await?;
    let restarted_epoch_store = restarted_components.worker_shard_epoch_store()?;
    let restarted_record = restarted_epoch_store
        .read(&key.stream_id, key.partition_id, 1)
        .await?
        .expect("epoch record should survive checked authority reconstruction");
    assert_eq!(restarted_record, record);

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

#[tokio::test]
async fn live_worker_shard_startup_resync_reconciles_existing_shard_when_enabled(
) -> Result<(), Box<dyn Error>> {
    if env::var("VELORIX_K8S_INTEGRATION").as_deref() != Ok("1") {
        eprintln!(
            "skipping live Kubernetes worker shard startup-resync test; set VELORIX_K8S_INTEGRATION=1"
        );
        return Ok(());
    }

    let namespace =
        env::var("VELORIX_K8S_NAMESPACE").unwrap_or_else(|_| "velorix-live".to_string());
    let worker_image = env::var("VELORIX_K8S_WORKER_IMAGE")
        .unwrap_or_else(|_| "registry.k8s.io/pause:3.10".to_string());
    let suffix = unique_suffix()?;
    let owner_id = "worker-a".to_string();
    let client = Client::try_default().await?;
    ensure_namespace(client.clone(), &namespace).await?;

    let shard = worker_shard_with_owner(&namespace, &suffix, &owner_id);
    let key = partition_lease_key_from_worker_shard(&shard)?;
    let shard_api: Api<VelorixWorkerShard> = Api::namespaced(client.clone(), &namespace);
    shard_api.create(&PostParams::default(), &shard).await?;
    wait_for_worker_shard_listed(&shard_api, &shard.name_any()).await?;

    let authority = LiveWorkerShardAuthority::new(&suffix)?;
    let components = authority.startup_components().await?;
    let epoch_store_for_assert = components.worker_shard_epoch_store()?;

    let (_runtime, summary) = resync_worker_shards_before_watch_with_kubernetes_runtime(
        client.clone(),
        &namespace,
        &components,
        WorkerShardPodTemplate::new(worker_image.clone())?,
        |_| WorkerShardReconcileInput {
            now_unix_ms: unix_ms().expect("system clock should be after Unix epoch"),
            ttl_ms: 60_000,
            running_worker: None,
            config: WorkerShardReconcileConfig {
                created_at: "2026-05-12T17:33:23Z".to_string(),
                previous_checkpoint_version: None,
            },
        },
        WorkerShardResyncOptions::default(),
    )
    .await?;
    assert_eq!(summary.listed, 1);
    assert_eq!(summary.applied, 1);

    let identity = runtime_identity_from_worker_shard(&shard, &owner_id, 1)?;
    let pod_name = worker_shard_pod_name_for_identity(&identity);
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
    assert_eq!(
        container_env_value(container, "VELORIX_WORKER_STREAM_ID"),
        Some(key.stream_id.as_str())
    );

    let record = epoch_store_for_assert
        .read(&key.stream_id, key.partition_id, 1)
        .await?
        .expect("startup resync should persist epoch record before watcher events are required");
    assert_eq!(record.owner_id, owner_id);
    assert_eq!(record.lease_identity, partition_lease_identity(&key));

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

struct LiveWorkerShardAuthority {
    suffix: String,
    temp_dir: tempfile::TempDir,
    probe_sequence: AtomicU64,
}

impl LiveWorkerShardAuthority {
    fn new(suffix: &str) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            suffix: suffix.to_string(),
            temp_dir: tempfile::tempdir()?,
            probe_sequence: AtomicU64::new(0),
        })
    }

    async fn startup_components(
        &self,
    ) -> Result<OperatorAuthorityStartupComponents, Box<dyn Error>> {
        let probe_sequence = self.probe_sequence.fetch_add(1, Ordering::Relaxed);
        let store = LocalFileSystem::new_with_prefix(self.temp_dir.path())?;
        let validated_authority = validate_operator_authority(
            authority(),
            Arc::new(store),
            "live-worker-shard-local-filesystem-authority",
            format!("v1/probes/worker-shard-{}-{probe_sequence}", self.suffix),
        )
        .await?;

        Ok(OperatorAuthorityStartupComponents::from_validated_authority(validated_authority))
    }
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
            authority: authority(),
        },
    );
    shard.metadata.namespace = Some(namespace.to_string());
    shard.metadata.generation = Some(1);
    shard
}

fn authority() -> ObjectStoreAuthorityRef {
    ObjectStoreAuthorityRef {
        store_id: "primary".to_string(),
        namespace: "analytics".to_string(),
    }
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
    for _ in 0..400 {
        if let Some(pod) = pod_api.get_opt(pod_name).await? {
            return Ok(pod);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(format!("worker Pod {pod_name} was not created within 40s").into())
}

async fn wait_for_worker_shard_listed(
    shard_api: &Api<VelorixWorkerShard>,
    shard_name: &str,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..100 {
        let listed = shard_api.list(&ListParams::default()).await?;
        if listed
            .items
            .iter()
            .any(|shard| shard.name_any() == shard_name)
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(format!("worker shard {shard_name} was not listed within 10s").into())
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
