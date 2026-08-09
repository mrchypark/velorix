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
use velorix_control::lease::{LeaseAcquireRequest, PartitionLeaseClient, PartitionLeaseKey};
use velorix_k8s::lease::{partition_lease_identity, KubeLeaseApi, KubernetesPartitionLeaseClient};

#[tokio::test]
async fn live_kubernetes_lease_acquires_renews_reads_and_releases_when_enabled(
) -> Result<(), Box<dyn Error>> {
    if env::var("VELORIX_K8S_INTEGRATION").as_deref() != Ok("1") {
        eprintln!("skipping live Kubernetes lease test; set VELORIX_K8S_INTEGRATION=1");
        return Ok(());
    }

    let namespace =
        env::var("VELORIX_K8S_NAMESPACE").unwrap_or_else(|_| "velorix-live".to_string());
    let client = Client::try_default().await?;
    ensure_namespace(client.clone(), &namespace).await?;

    let lease_client = KubernetesPartitionLeaseClient::new(KubeLeaseApi::new(client.clone()));
    let key = PartitionLeaseKey {
        namespace,
        view_id: "live-vind-view".to_string(),
        stream_id: format!("orders-live-{}-{}", std::process::id(), unix_ms()?),
        partition_id: 0,
    };

    let acquired = lease_client
        .acquire_or_renew(LeaseAcquireRequest {
            key: key.clone(),
            owner_id: "worker-a".to_string(),
            now_unix_ms: unix_ms()?,
            ttl_ms: 60_000,
        })
        .await?;
    assert_eq!(acquired.key, key);
    assert_eq!(acquired.owner_id, "worker-a");

    let renewed = lease_client
        .acquire_or_renew(LeaseAcquireRequest {
            key: key.clone(),
            owner_id: "worker-a".to_string(),
            now_unix_ms: unix_ms()?,
            ttl_ms: 60_000,
        })
        .await?;
    assert_eq!(renewed.owner_epoch, acquired.owner_epoch);

    let current = lease_client.current(&key, unix_ms()?).await?;
    assert_eq!(current, Some(renewed.clone()));

    lease_client
        .release(&key, &renewed.owner_id, renewed.owner_epoch, unix_ms()?)
        .await?;
    assert_eq!(lease_client.current(&key, unix_ms()?).await?, None);
    delete_lease(client, &key).await?;

    Ok(())
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

fn unix_ms() -> Result<u64, Box<dyn Error>> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

async fn delete_lease(client: Client, key: &PartitionLeaseKey) -> Result<(), Box<dyn Error>> {
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
