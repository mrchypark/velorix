use std::{env, error::Error, fmt::Debug, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use k8s_openapi::{api::core::v1::Namespace, apimachinery::pkg::apis::meta::v1::ObjectMeta};
use kube::{
    api::{Api, DeleteParams, PostParams},
    Client,
};
use object_store::local::LocalFileSystem;
use serde_json::{json, Value};
use tokio::time::{sleep, Duration};
use velorix_k8s::{
    controller::AuthoritySnapshot,
    crd::{
        ConditionState, ObjectStoreAuthorityRef, RelationVersionRef, StreamStatus,
        VelorixCondition, VelorixDatabase, VelorixDatabaseSpec, VelorixStream, VelorixStreamSpec,
        VelorixWorkerShard, VelorixWorkerShardSpec,
    },
    startup::{validate_operator_authority, OperatorAuthorityStartupComponents},
    status::{KubeStreamStatusApi, StreamStatusWriter},
    stream_watch::{
        handle_stream_event, watch_streams_with_kubernetes_runtime, AuthoritySnapshotProvider,
        StreamWatchError, StreamWatchEvent,
    },
};
use velorix_storage::{
    capability::AuthoritativeNamespace, relation_catalog_registry::RelationCatalogRegistry,
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

#[tokio::test]
async fn live_stream_watch_uses_startup_components_when_enabled() -> Result<(), Box<dyn Error>> {
    if env::var("VELORIX_K8S_INTEGRATION").as_deref() != Ok("1") {
        eprintln!("skipping live Kubernetes stream-watch test; set VELORIX_K8S_INTEGRATION=1");
        return Ok(());
    }

    let namespace =
        env::var("VELORIX_K8S_NAMESPACE").unwrap_or_else(|_| "velorix-live".to_string());
    let suffix = unique_suffix()?;
    let client = Client::try_default().await?;
    ensure_namespace(client.clone(), &namespace).await?;

    let authority = ObjectStoreAuthorityRef {
        store_id: "primary".to_string(),
        namespace: "analytics".to_string(),
    };
    let relation = RelationVersionRef {
        relation_id: "deposits".to_string(),
        relation_version: 1,
        schema_fingerprint:
            "sha256:9b09fa82241fce3bb9025911ed78168799ad384fe68f065258afe09eca6ede62".to_string(),
    };
    let temp_dir = tempfile::tempdir()?;
    let authority_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path())?);
    let validated = validate_operator_authority(
        authority.clone(),
        Arc::clone(&authority_store),
        "live-stream-watch-local-filesystem-authority",
        format!("v1/probes/stream-watch-{suffix}"),
    )
    .await?;
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);
    let relation_catalog_profile = components
        .capabilities()
        .validate_namespace(AuthoritativeNamespace::RelationCatalog)?;
    let catalog = serde_json::from_value(relation_catalog_json(&relation))?;
    RelationCatalogRegistry::new_checked(Arc::clone(&authority_store), relation_catalog_profile)?
        .create(&catalog)
        .await?;

    let stream_name = format!("stream-watch-{suffix}");
    let mut stream = VelorixStream::new(
        &stream_name,
        VelorixStreamSpec {
            stream_id: format!("deposits-{suffix}"),
            database_id: format!("database-{suffix}"),
            relation: relation.clone(),
            authority: authority.clone(),
        },
    );
    stream.metadata.namespace = Some(namespace.clone());

    let watch_client = client.clone();
    let watch_namespace = namespace.clone();
    let watch_components = components.clone();
    let watch_task = tokio::spawn(async move {
        watch_streams_with_kubernetes_runtime(watch_client, &watch_namespace, &watch_components)
            .await
    });

    let stream_api: Api<VelorixStream> = Api::namespaced(client.clone(), &namespace);
    stream_api.create(&PostParams::default(), &stream).await?;
    let created_stream = wait_for_stream_status(
        &stream_api,
        &stream_name,
        relation.schema_fingerprint.as_str(),
    )
    .await?;

    assert_eq!(
        created_stream.status,
        Some(StreamStatus {
            observed_generation: created_stream.metadata.generation,
            last_accepted_relation_schema_fingerprint: Some(relation.schema_fingerprint.clone()),
            latest_published_checkpoint: None,
            readiness: Some(VelorixCondition {
                type_: "Ready".to_string(),
                status: ConditionState::True,
                reason: "AuthorityValidated".to_string(),
                message: "object-store authority and relation catalog records validated"
                    .to_string(),
            }),
        })
    );

    watch_task.abort();
    delete_if_exists(&stream_api, &stream_name).await?;

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

async fn wait_for_stream_status(
    api: &Api<VelorixStream>,
    stream_name: &str,
    schema_fingerprint: &str,
) -> Result<VelorixStream, Box<dyn Error>> {
    for _ in 0..100 {
        let stream = api.get(stream_name).await?;
        let is_ready = stream.status.as_ref().is_some_and(|status| {
            status.last_accepted_relation_schema_fingerprint.as_deref() == Some(schema_fingerprint)
                && status
                    .readiness
                    .as_ref()
                    .is_some_and(|condition| condition.status == ConditionState::True)
        });
        if is_ready {
            return Ok(stream);
        }
        sleep(Duration::from_millis(100)).await;
    }

    Err(format!("stream {stream_name} status was not reconciled within 10s").into())
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

fn relation_catalog_json(relation: &RelationVersionRef) -> Value {
    json!({
        "schema_version": 1,
        "relation_schema": {
            "relation_id": relation.relation_id,
            "relation_name": relation.relation_id,
            "relation_version": relation.relation_version.to_string(),
            "columns": [
                {
                    "column_id": "deposit_id",
                    "name": "deposit_id",
                    "logical_type": { "kind": "utf8" },
                    "physical_arrow_type": { "kind": "utf8" },
                    "nullable": false,
                    "ordinal": 0,
                    "semantic_role": "primary_key",
                },
                {
                    "column_id": "amount",
                    "name": "amount",
                    "logical_type": { "kind": "int64" },
                    "physical_arrow_type": { "kind": "int64" },
                    "nullable": false,
                    "ordinal": 1,
                    "semantic_role": "value",
                },
                {
                    "column_id": "weight",
                    "name": "weight",
                    "logical_type": { "kind": "int64" },
                    "physical_arrow_type": { "kind": "int64" },
                    "nullable": false,
                    "ordinal": 2,
                    "semantic_role": "weight",
                },
            ],
            "primary_key_column_ids": ["deposit_id"],
            "weight_column_id": "weight",
            "allowed_operations": ["insert", "delete"],
            "event_time_column_id": null,
        },
        "schema_fingerprint": relation.schema_fingerprint,
        "datafusion_registration": {
            "name": relation.relation_id,
            "mode": "table",
        },
        "incremental_relation": {
            "relation_id": relation.relation_id,
            "schema_fingerprint": relation.schema_fingerprint,
        },
        "incremental_adapter": {
            "adapter_id": "incremental-adapter-single-key-sum-count-v1",
        },
    })
}
