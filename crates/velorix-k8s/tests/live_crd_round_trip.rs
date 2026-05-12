use std::{env, error::Error, fmt::Debug, time::SystemTime};

use async_trait::async_trait;
use k8s_openapi::{api::core::v1::Namespace, apimachinery::pkg::apis::meta::v1::ObjectMeta};
use kube::{
    api::{Api, DeleteParams, PostParams},
    Client,
};
use velorix_k8s::{
    controller::AuthoritySnapshot,
    crd::{
        ConditionState, ObjectStoreAuthorityRef, RelationVersionRef, StreamStatus,
        VelorixCondition, VelorixDatabase, VelorixDatabaseSpec, VelorixStream, VelorixStreamSpec,
        VelorixWorkerShard, VelorixWorkerShardSpec,
    },
    status::{KubeStreamStatusApi, StreamStatusWriter},
    stream_watch::{
        handle_stream_event, AuthoritySnapshotProvider, StreamWatchError, StreamWatchEvent,
    },
};

#[tokio::test]
async fn live_velorix_crds_create_read_and_delete_when_enabled() -> Result<(), Box<dyn Error>> {
    if env::var("VELORIX_K8S_INTEGRATION").as_deref() != Ok("1") {
        eprintln!("skipping live Kubernetes CRD test; set VELORIX_K8S_INTEGRATION=1");
        return Ok(());
    }

    let namespace =
        env::var("VELORIX_K8S_NAMESPACE").unwrap_or_else(|_| "velorix-live".to_string());
    let suffix = unique_suffix()?;
    let client = Client::try_default().await?;
    ensure_namespace(client.clone(), &namespace).await?;

    let database_api: Api<VelorixDatabase> = Api::namespaced(client.clone(), &namespace);
    let stream_api: Api<VelorixStream> = Api::namespaced(client.clone(), &namespace);
    let shard_api: Api<VelorixWorkerShard> = Api::namespaced(client.clone(), &namespace);

    let database_name = format!("db-{suffix}");
    let stream_name = format!("stream-{suffix}");
    let shard_name = format!("shard-{suffix}");

    let authority = ObjectStoreAuthorityRef {
        store_id: "primary".to_string(),
        namespace: "analytics".to_string(),
    };
    let relation = RelationVersionRef {
        relation_id: "orders_sum_count".to_string(),
        relation_version: 2_026_050_501,
        schema_fingerprint: format!("sha256:{}", "a".repeat(64)),
    };

    let database = VelorixDatabase::new(
        &database_name,
        VelorixDatabaseSpec {
            database_id: format!("database-{suffix}"),
            authority: authority.clone(),
        },
    );
    let mut stream = VelorixStream::new(
        &stream_name,
        VelorixStreamSpec {
            stream_id: format!("orders-{suffix}"),
            database_id: format!("database-{suffix}"),
            relation,
            authority: authority.clone(),
        },
    );
    stream.metadata.namespace = Some(namespace.clone());
    let shard = VelorixWorkerShard::new(
        &shard_name,
        VelorixWorkerShardSpec {
            worker_id: "worker-a".to_string(),
            view_id: "live-crd-view".to_string(),
            stream_id: format!("orders-{suffix}"),
            partition_id: 0,
            desired_owner_id: "worker-a".to_string(),
            authority: authority.clone(),
        },
    );

    database_api
        .create(&PostParams::default(), &database)
        .await?;
    stream_api.create(&PostParams::default(), &stream).await?;
    shard_api.create(&PostParams::default(), &shard).await?;

    let created_stream = stream_api.get(&stream_name).await?;
    let snapshot = StaticSnapshotProvider(
        AuthoritySnapshot::default()
            .with_authority(authority.clone())
            .with_relation_for_authority(&authority, &created_stream.spec.relation),
    );
    handle_stream_event(
        StreamWatchEvent::Applied(created_stream.clone()),
        &snapshot,
        &StreamStatusWriter::new(KubeStreamStatusApi::new(client)),
    )
    .await?;

    assert_eq!(
        database_api.get(&database_name).await?.spec.database_id,
        format!("database-{suffix}")
    );
    assert_eq!(
        stream_api.get(&stream_name).await?.spec.stream_id,
        format!("orders-{suffix}")
    );
    assert_eq!(
        stream_api.get(&stream_name).await?.status,
        Some(ready_status(created_stream.metadata.generation))
    );
    assert_eq!(
        shard_api.get(&shard_name).await?.spec.desired_owner_id,
        "worker-a"
    );

    delete_if_exists(&shard_api, &shard_name).await?;
    delete_if_exists(&stream_api, &stream_name).await?;
    delete_if_exists(&database_api, &database_name).await?;

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

async fn delete_if_exists<K>(api: &Api<K>, name: &str) -> Result<(), Box<dyn Error>>
where
    K: Clone + Debug + serde::de::DeserializeOwned + kube::Resource + Send + Sync + 'static,
    <K as kube::Resource>::DynamicType: Default,
{
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(response)) if response.code == 404 => Ok(()),
        Err(error) => Err(Box::new(error)),
    }
}

fn unique_suffix() -> Result<String, Box<dyn Error>> {
    let elapsed = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
    Ok(format!("{}-{}", std::process::id(), elapsed.as_millis()))
}

#[derive(Clone)]
struct StaticSnapshotProvider(AuthoritySnapshot);

#[async_trait]
impl AuthoritySnapshotProvider for StaticSnapshotProvider {
    async fn snapshot_for_stream(
        &self,
        _stream: &VelorixStream,
    ) -> Result<AuthoritySnapshot, StreamWatchError> {
        Ok(self.0.clone())
    }
}

fn ready_status(observed_generation: Option<i64>) -> StreamStatus {
    StreamStatus {
        observed_generation,
        last_accepted_relation_schema_fingerprint: Some(format!("sha256:{}", "a".repeat(64))),
        latest_published_checkpoint: None,
        readiness: Some(VelorixCondition {
            type_: "Ready".to_string(),
            status: ConditionState::True,
            reason: "AuthorityValidated".to_string(),
            message: "object-store authority and relation catalog records validated".to_string(),
        }),
    }
}
