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
use velorix_storage::{log::DurableIngestAdmissionRecordV1, object_key::ObjectKey};

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

    let orphan = durable_orphan_admission_record("vind", 0, 0, 10)?;
    put_durable_admission_record(&store, &orphan).await?;
    let validated = validate_operator_authority(
        authority(&namespace),
        Arc::clone(&store),
        "vind-ingest-authority",
        &format!("v1/vind-ingest-startup-probes/{suffix}/orphan-active"),
    )
    .await?;
    let provider = IngestAdmissionCoordinatorProvider::for_production(validated);
    let (coordinator, report) = provider.coordinator_after_startup_reconstruction().await?;

    assert_eq!(report.active_admission_records, 1);
    assert_eq!(report.expired_orphan_admission_records, 0);

    let decision = coordinator
        .expire_orphan_admission(
            "vind",
            0,
            0,
            10,
            "repair-restart-evidence",
            "batch_append_failed_after_admission",
            "live-ingest-admission-test",
        )
        .await?;

    assert_eq!(decision.stream_id, orphan.stream_id);
    assert_eq!(decision.partition_id, orphan.partition_id);
    assert_eq!(
        decision.start_offset_inclusive,
        orphan.start_offset_inclusive
    );
    assert_eq!(decision.end_offset_exclusive, orphan.end_offset_exclusive);
    assert_eq!(decision.batch_key, orphan.batch_key);
    assert_eq!(decision.observed_missing_batch_key, orphan.batch_key);
    assert_eq!(decision.admission_record_key, orphan.admission_record_key);
    assert_eq!(
        decision.expired_reason,
        "batch_append_failed_after_admission"
    );
    assert_eq!(decision.operator_id, "live-ingest-admission-test");
    store
        .get(&Path::from(decision.expiry_decision_key.as_str()))
        .await?;

    let duplicate_record_create = put_durable_admission_record(&store, &orphan).await;
    assert!(matches!(
        duplicate_record_create,
        Err(object_store::Error::AlreadyExists { .. })
    ));

    let validated = validate_operator_authority(
        authority(&namespace),
        Arc::clone(&store),
        "vind-ingest-authority",
        &format!("v1/vind-ingest-startup-probes/{suffix}/orphan-expired-restart"),
    )
    .await?;
    let provider = IngestAdmissionCoordinatorProvider::for_production(validated);
    let (restarted, report) = provider.coordinator_after_startup_reconstruction().await?;

    assert_eq!(report.active_admission_records, 0);
    assert_eq!(report.expired_orphan_admission_records, 1);
    assert_eq!(restarted.list_committed().await?, Vec::new());

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

fn durable_orphan_admission_record(
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
) -> Result<DurableIngestAdmissionRecordV1, Box<dyn Error>> {
    Ok(DurableIngestAdmissionRecordV1 {
        schema_version: 1,
        record_kind: "ingest_range_admission_v1".to_string(),
        stream_id: stream_id.to_string(),
        partition_id,
        start_offset_inclusive,
        end_offset_exclusive,
        batch_key: ObjectKey::ingest_batch(
            stream_id,
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
        )?,
        admission_record_key: ObjectKey::ingest_admission_record(
            stream_id,
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
        )?,
        payload_digest: format!("sha256:{}", "1".repeat(64)),
        relation_id: "vind_relation".to_string(),
        relation_version: "live-ingest-admission-test".to_string(),
        schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
        admission_mode: "process_local_serialized".to_string(),
    })
}

async fn put_durable_admission_record(
    store: &Arc<dyn ObjectStore>,
    record: &DurableIngestAdmissionRecordV1,
) -> Result<(), object_store::Error> {
    store
        .put_opts(
            &Path::from(record.admission_record_key.as_str()),
            serde_json::to_vec(record)
                .expect("test durable admission record should serialize")
                .into(),
            PutMode::Create.into(),
        )
        .await
        .map(|_| ())
}

fn unique_suffix() -> Result<String, Box<dyn Error>> {
    let elapsed = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
    Ok(format!("{}-{}", std::process::id(), elapsed.as_millis()))
}
