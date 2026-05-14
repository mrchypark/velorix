use std::{env, error::Error, sync::Arc, time::SystemTime};

use k8s_openapi::{api::core::v1::Namespace, apimachinery::pkg::apis::meta::v1::ObjectMeta};
use kube::{
    api::{Api, PostParams},
    Client,
};
use object_store::{local::LocalFileSystem, path::Path, ObjectStore, PutMode};
use tempfile::TempDir;
use velorix_k8s::{
    crd::ObjectStoreAuthorityRef,
    startup::validate_operator_authority,
    stream_watch::{IngestAdmissionCoordinatorProvider, StreamWatchError},
};

#[tokio::test]
async fn live_vind_gated_ingest_admission_startup_preflight_runs_when_enabled(
) -> Result<(), Box<dyn Error>> {
    if env::var("VELORIX_K8S_INTEGRATION").as_deref() != Ok("1") {
        eprintln!(
            "skipping live Kubernetes ingest-admission startup preflight; set VELORIX_K8S_INTEGRATION=1"
        );
        return Ok(());
    }

    let namespace =
        env::var("VELORIX_K8S_NAMESPACE").unwrap_or_else(|_| "velorix-live".to_string());
    let suffix = unique_suffix()?;
    let client = Client::try_default().await?;
    ensure_namespace(client, &namespace).await?;
    let (_temp_dir, store) = temp_store();

    let validated = validate_operator_authority(
        authority(&namespace),
        Arc::clone(&store),
        "vind-ingest-authority",
        &format!("v1/vind-ingest-startup-probes/{suffix}/clean"),
    )
    .await?;
    let provider = IngestAdmissionCoordinatorProvider::for_production(validated);
    let report = provider.startup().await?;

    assert_eq!(report.active_admission_records, 0);
    assert_eq!(report.expired_orphan_admission_records, 0);

    store
        .put_opts(
            &Path::from(
                "v1/ingest-admission/vind/p=0000000000/ranges/00000000000000000000-00000000000000000010/notes.txt",
            ),
            "unexpected admission namespace object".into(),
            PutMode::Create.into(),
        )
        .await?;
    let validated = validate_operator_authority(
        authority(&namespace),
        Arc::clone(&store),
        "vind-ingest-authority",
        &format!("v1/vind-ingest-startup-probes/{suffix}/corrupt"),
    )
    .await?;
    let provider = IngestAdmissionCoordinatorProvider::for_production(validated);
    let err = provider.startup().await.unwrap_err();

    assert!(matches!(err, StreamWatchError::Snapshot { message }
        if message.contains("unexpected object under v1/ingest-admission")));

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

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

fn authority(namespace: &str) -> ObjectStoreAuthorityRef {
    ObjectStoreAuthorityRef {
        store_id: "primary".to_string(),
        namespace: namespace.to_string(),
    }
}

fn unique_suffix() -> Result<String, Box<dyn Error>> {
    let elapsed = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
    Ok(format!("{}-{}", std::process::id(), elapsed.as_millis()))
}
