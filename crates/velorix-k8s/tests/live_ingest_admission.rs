use std::{env, error::Error, sync::Arc, time::SystemTime};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use k8s_openapi::{
    api::core::v1::{Namespace, Pod},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    api::{Api, DeleteParams, PostParams},
    Client,
};
use object_store::{local::LocalFileSystem, path::Path, ObjectStore, ObjectStoreExt, PutMode};
use serde_json::json;
use tempfile::TempDir;
use velorix_k8s::{
    crd::ObjectStoreAuthorityRef,
    ingest_writer::{
        build_kubernetes_ingest_writer_operator_runtime, DeployedIngestWriterRuntime,
        IngestWriterPodTemplate,
    },
    startup::{validate_operator_authority, OperatorAuthorityStartupComponents},
    stream_watch::StreamWatchError,
};
use velorix_storage::{
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::{AppendValidatedEnvelopeOutcome, DurableIngestAdmissionRecordV1},
    object_key::ObjectKey,
    relation_catalog_registry::RelationCatalogRegistry,
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
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);
    let report = components
        .ingest_admission_startup_preflight()
        .await?
        .ingest_admission;

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
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);
    let provider = components.ingest_admission_coordinator_provider();
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
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);
    let provider = components.ingest_admission_coordinator_provider();
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
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);
    let err = components
        .ingest_admission_startup_preflight()
        .await
        .unwrap_err();

    assert!(matches!(err, StreamWatchError::Snapshot { message }
        if message.contains("unexpected object under v1/ingest-admission")));

    Ok(())
}

#[tokio::test]
async fn live_vind_deployed_ingest_writer_runtime_appends_after_startup_preflight(
) -> Result<(), Box<dyn Error>> {
    if env::var("VELORIX_K8S_INTEGRATION").as_deref() != Ok("1") {
        eprintln!(
            "skipping live Kubernetes deployed ingest writer runtime; set VELORIX_K8S_INTEGRATION=1"
        );
        return Ok(());
    }

    let namespace =
        env::var("VELORIX_K8S_NAMESPACE").unwrap_or_else(|_| "velorix-live".to_string());
    let suffix = unique_suffix()?;
    let client = Client::try_default().await?;
    ensure_namespace(client, &namespace).await?;
    let (_temp_dir, store) = temp_store();
    create_vind_relation_catalog(&store).await?;

    let validated = validate_operator_authority(
        authority(&namespace),
        Arc::clone(&store),
        "vind-ingest-authority",
        &format!("v1/vind-ingest-writer-probes/{suffix}/clean"),
    )
    .await?;
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);
    let runtime = DeployedIngestWriterRuntime::from_startup_components(&components).await?;

    assert_eq!(runtime.authority(), components.authority());
    assert_eq!(runtime.startup_report().active_admission_records, 0);
    assert_eq!(runtime.startup_report().expired_orphan_admission_records, 0);

    let outcome = runtime
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(0, 10))
        .await?;
    assert!(matches!(
        outcome,
        AppendValidatedEnvelopeOutcome::Appended { descriptor }
            if descriptor.stream_id == "vind"
                && descriptor.partition_id == 0
                && descriptor.start_offset_inclusive == 0
                && descriptor.end_offset_exclusive == 10
    ));

    Ok(())
}

#[tokio::test]
async fn live_vind_deployed_ingest_writer_pod_created_after_startup_preflight(
) -> Result<(), Box<dyn Error>> {
    if env::var("VELORIX_K8S_INTEGRATION").as_deref() != Ok("1") {
        eprintln!(
            "skipping live Kubernetes deployed ingest writer pod; set VELORIX_K8S_INTEGRATION=1"
        );
        return Ok(());
    }
    let image = match env::var("VELORIX_K8S_INGEST_WRITER_IMAGE") {
        Ok(image) => image,
        Err(_) => {
            eprintln!(
                "skipping live Kubernetes deployed ingest writer pod; set VELORIX_K8S_INGEST_WRITER_IMAGE"
            );
            return Ok(());
        }
    };

    let namespace =
        env::var("VELORIX_K8S_NAMESPACE").unwrap_or_else(|_| "velorix-live".to_string());
    let suffix = unique_suffix()?;
    let client = Client::try_default().await?;
    ensure_namespace(client.clone(), &namespace).await?;
    let (_temp_dir, store) = temp_store();
    create_vind_relation_catalog(&store).await?;

    let validated = validate_operator_authority(
        authority(&namespace),
        Arc::clone(&store),
        "vind-ingest-authority",
        &format!("v1/vind-ingest-writer-pod-probes/{suffix}/clean"),
    )
    .await?;
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);
    let runtime = build_kubernetes_ingest_writer_operator_runtime(
        client.clone(),
        &namespace,
        &components,
        IngestWriterPodTemplate::new(image)?,
        "live-ingest-admission-test",
        format!("vind-writer-{suffix}"),
    )
    .await?;

    runtime.pod_executor().create_writer_pod().await?;

    let pod_api: Api<Pod> = Api::namespaced(client, &namespace);
    let pod_name = runtime.pod_executor().pod_name();
    let pod = pod_api.get(&pod_name).await?;
    let env = pod
        .spec
        .as_ref()
        .and_then(|spec| spec.containers.first())
        .and_then(|container| container.env.as_ref())
        .expect("ingest writer pod should carry identity env");

    assert_eq!(
        env_value(env, "VELORIX_INGEST_WRITER_NAMESPACE"),
        Some(namespace.as_str())
    );
    assert_eq!(
        env_value(env, "VELORIX_INGEST_WRITER_AUTHORITY_STORE_ID"),
        Some("primary")
    );
    assert_eq!(
        env_value(env, "VELORIX_INGEST_WRITER_AUTHORITY_NAMESPACE"),
        Some(namespace.as_str())
    );
    assert_eq!(
        env_value(env, "VELORIX_INGEST_WRITER_OPERATOR_ID"),
        Some("live-ingest-admission-test")
    );

    let _ = pod_api.delete(&pod_name, &DeleteParams::default()).await;

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

async fn create_vind_relation_catalog(store: &Arc<dyn ObjectStore>) -> Result<(), Box<dyn Error>> {
    let catalog = serde_json::from_value(json!({
        "schema_version": 1,
        "relation_schema": {
            "relation_id": "deposits",
            "relation_name": "deposits",
            "relation_version": "1",
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
        "schema_fingerprint": deposits_schema_fingerprint(),
        "datafusion_registration": {
            "name": "deposits",
            "mode": "table",
        },
        "incremental_relation": {
            "relation_id": "deposits",
            "schema_fingerprint": deposits_schema_fingerprint(),
        },
        "incremental_adapter": {
            "adapter_id": "incremental-adapter-single-key-sum-count-v1",
        },
    }))?;
    RelationCatalogRegistry::new(Arc::clone(store))
        .create(&catalog)
        .await?;
    Ok(())
}

fn catalog_envelope_bytes_for(start_offset_inclusive: u64, end_offset_exclusive: u64) -> Bytes {
    IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "deposits".to_string(),
            relation_version: "1".to_string(),
            schema_fingerprint: deposits_schema_fingerprint(),
            stream_id: "vind".to_string(),
            partition_id: 0,
            start_offset_inclusive,
            end_offset_exclusive,
            event_time_watermark: None,
        },
        &[valid_batch()],
    )
    .unwrap()
}

fn valid_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("deposit_id", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("weight", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["dep-1", "dep-2"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, -1])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn deposits_schema_fingerprint() -> String {
    "sha256:9b09fa82241fce3bb9025911ed78168799ad384fe68f065258afe09eca6ede62".to_string()
}

fn orphan_schema_fingerprint() -> String {
    format!("sha256:{}", "2".repeat(64))
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
        event_time_watermark: None,
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
        schema_fingerprint: orphan_schema_fingerprint(),
        admission_mode: "process_local_serialized".to_string(),
        commit_guard_binding: None,
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

fn env_value<'a>(env: &'a [k8s_openapi::api::core::v1::EnvVar], name: &str) -> Option<&'a str> {
    env.iter()
        .find(|var| var.name == name)
        .and_then(|var| var.value.as_deref())
}
