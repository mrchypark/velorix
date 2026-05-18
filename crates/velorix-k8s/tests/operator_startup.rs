use std::{convert::Infallible, fmt, fs, sync::Arc};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use http::{Method, Request, Response, StatusCode};
use k8s_openapi::api::core::v1::EnvVar;
use kube::client::{Body, ClientBuilder};
use object_store::{
    local::LocalFileSystem, path::Path, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use velorix_k8s::{
    controller::{reconcile_stream, ControllerAction},
    crd::{ObjectStoreAuthorityRef, RelationVersionRef, VelorixStream, VelorixStreamSpec},
    ingest_writer::{
        build_kubernetes_ingest_writer_operator_runtime, ingest_writer_pod_name_for_identity,
        DeployedIngestWriterRuntime, IngestWriterPodTemplate, IngestWriterRuntimeIdentity,
    },
    startup::{
        validate_operator_authority, OperatorAuthorityStartupComponents, OperatorStartupError,
    },
    stream_watch::AuthoritySnapshotProvider,
    worker_shard::WorkerShardEpochStore,
};
use velorix_storage::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilityProbeError,
        ObjectStoreCapabilityProbeError, RequiredObjectStoreCapability,
    },
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::AppendValidatedEnvelopeOutcome,
    ownership::OwnershipEpochRecord,
    relation_catalog_registry::RelationCatalogRegistry,
};

#[tokio::test]
async fn operator_startup_accepts_authority_store_with_all_namespace_capabilities() {
    let (_temp_dir, store) = temp_store();

    let validated = validate_operator_authority(
        authority(),
        Arc::clone(&store),
        "local-k8s-authority",
        "v1/operator-startup-probes",
    )
    .await
    .unwrap();

    assert_eq!(validated.authority(), &authority());
    validated.capabilities().validate_for_startup().unwrap();
    for namespace in AuthoritativeNamespace::all() {
        let profile = validated.capabilities().profiles.get(&namespace).unwrap();
        assert_eq!(profile.backend_name, "local-k8s-authority");
    }
}

#[tokio::test]
async fn operator_startup_rejects_store_without_create_only_behavior() {
    let (_temp_dir, inner) = temp_store();
    let store = OverwriteCreateStore { inner };

    let err = validate_operator_authority(
        authority(),
        Arc::new(store),
        "overwrite-create",
        "v1/operator-startup-probes",
    )
    .await
    .unwrap_err();

    match err {
        OperatorStartupError::Probe(AuthoritativeObjectStoreCapabilityProbeError::Namespace {
            namespace,
            source,
        }) => {
            assert_eq!(namespace, AuthoritativeNamespace::Ingest);
            match source {
                ObjectStoreCapabilityProbeError::Capability(error) => {
                    assert_eq!(error.backend_name(), "overwrite-create");
                    assert_eq!(
                        error.required_capability(),
                        RequiredObjectStoreCapability::ConditionalCreate
                    );
                }
                other => panic!("expected conditional-create capability error, got {other:?}"),
            }
        }
        other => panic!("expected namespace probe error, got {other:?}"),
    }
}

#[tokio::test]
async fn production_snapshot_provider_uses_validated_operator_authority() {
    let (_temp_dir, store) = temp_store();
    create_relation_catalog(&store).await;
    let validated = validate_operator_authority(
        authority(),
        Arc::clone(&store),
        "local-k8s-authority",
        "v1/operator-startup-probes",
    )
    .await
    .unwrap();
    let provider = OperatorAuthorityStartupComponents::from_validated_authority(validated)
        .relation_snapshot_provider();

    let snapshot = provider.snapshot_for_stream(&stream()).await.unwrap();
    let ControllerAction::WriteStreamStatus(status) = reconcile_stream(&stream(), &snapshot).action;

    assert_eq!(
        status.last_accepted_relation_schema_fingerprint,
        Some(relation().schema_fingerprint)
    );
    assert_eq!(status.readiness.unwrap().reason, "AuthorityValidated");
}

#[tokio::test]
async fn production_snapshot_provider_reads_from_validated_store_only() {
    let (_validated_temp_dir, validated_store) = temp_store();
    let (_other_temp_dir, other_store) = temp_store();
    create_relation_catalog(&other_store).await;
    let validated = validate_operator_authority(
        authority(),
        Arc::clone(&validated_store),
        "local-k8s-authority",
        "v1/operator-startup-probes",
    )
    .await
    .unwrap();
    let provider = OperatorAuthorityStartupComponents::from_validated_authority(validated)
        .relation_snapshot_provider();

    let snapshot = provider.snapshot_for_stream(&stream()).await.unwrap();
    let ControllerAction::WriteStreamStatus(status) = reconcile_stream(&stream(), &snapshot).action;

    assert_eq!(
        status.last_accepted_relation_schema_fingerprint, None,
        "production provider must not read catalog evidence from an unvalidated store"
    );
    assert_eq!(
        status.readiness.unwrap().reason,
        "MissingRelationCatalogRecord"
    );
}

#[tokio::test]
async fn production_ingest_admission_provider_constructs_from_validated_authority() {
    let (_temp_dir, store) = temp_store();
    let validated = validate_operator_authority(
        authority(),
        Arc::clone(&store),
        "local-k8s-authority",
        "v1/operator-startup-probes",
    )
    .await
    .unwrap();
    let provider = OperatorAuthorityStartupComponents::from_validated_authority(validated)
        .ingest_admission_coordinator_provider();

    let (coordinator, report) = provider
        .coordinator_after_startup_reconstruction()
        .await
        .unwrap();

    assert_eq!(
        provider.capabilities().profiles[&AuthoritativeNamespace::Ingest].backend_name,
        "local-k8s-authority"
    );
    assert_eq!(
        provider.capabilities().profiles[&AuthoritativeNamespace::IngestAdmission].backend_name,
        "local-k8s-authority"
    );
    assert_eq!(report.active_admission_records, 0);
    assert_eq!(report.expired_orphan_admission_records, 0);
    assert_eq!(coordinator.list_committed().await.unwrap(), Vec::new());
}

#[tokio::test]
async fn production_ingest_admission_provider_startup_reconstructs_admissions_before_use() {
    let (_temp_dir, store) = temp_store();
    store
        .put_opts(
            &Path::from(
                "v1/ingest-admission/deposits/p=0000000000/ranges/00000000000000000000-00000000000000000010/notes.txt",
            ),
            "unexpected admission namespace object".into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    let validated = validate_operator_authority(
        authority(),
        Arc::clone(&store),
        "local-k8s-authority",
        "v1/operator-startup-probes",
    )
    .await
    .unwrap();
    let provider = OperatorAuthorityStartupComponents::from_validated_authority(validated)
        .ingest_admission_coordinator_provider();

    let err = provider.startup().await.unwrap_err();

    assert!(
        matches!(err, velorix_k8s::stream_watch::StreamWatchError::Snapshot { message }
        if message.contains("unexpected object under v1/ingest-admission"))
    );
}

#[tokio::test]
async fn operator_authority_startup_components_reuse_validated_evidence_for_providers() {
    let (_temp_dir, store) = temp_store();
    create_relation_catalog(&store).await;
    let validated = validate_operator_authority(
        authority(),
        Arc::clone(&store),
        "local-k8s-authority",
        "v1/operator-startup-probes",
    )
    .await
    .unwrap();
    let expected_capabilities = validated.capabilities().clone();
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);

    let snapshot_provider = components.relation_snapshot_provider();
    let admission_provider = components.ingest_admission_coordinator_provider();
    let epoch_store = components.worker_shard_epoch_store().unwrap();

    assert_eq!(components.authority(), &authority());
    assert_eq!(components.capabilities(), &expected_capabilities);
    assert_eq!(snapshot_provider.capabilities(), components.capabilities());
    assert_eq!(admission_provider.authority(), components.authority());
    assert_eq!(admission_provider.capabilities(), components.capabilities());
    for namespace in [
        AuthoritativeNamespace::RelationCatalog,
        AuthoritativeNamespace::Checkpoint,
        AuthoritativeNamespace::Ingest,
        AuthoritativeNamespace::IngestAdmission,
        AuthoritativeNamespace::Ownership,
    ] {
        assert_eq!(
            components.capabilities().profiles[&namespace].backend_name,
            "local-k8s-authority"
        );
    }

    let snapshot = snapshot_provider
        .snapshot_for_stream(&stream())
        .await
        .unwrap();
    let ControllerAction::WriteStreamStatus(status) = reconcile_stream(&stream(), &snapshot).action;
    assert_eq!(
        status.last_accepted_relation_schema_fingerprint,
        Some(relation().schema_fingerprint)
    );

    let report = components
        .ingest_admission_startup_preflight()
        .await
        .unwrap();
    assert_eq!(report.ingest_admission.active_admission_records, 0);

    let record = ownership_epoch_record();
    epoch_store.create(record.clone()).await.unwrap();
    assert_eq!(
        epoch_store
            .read(&record.stream_id, record.partition_id, record.owner_epoch)
            .await
            .unwrap(),
        Some(record)
    );
}

#[tokio::test]
async fn operator_authority_startup_components_preflight_fails_before_component_use() {
    let (_temp_dir, store) = temp_store();
    store
        .put_opts(
            &Path::from(
                "v1/ingest-admission/deposits/p=0000000000/ranges/00000000000000000000-00000000000000000010/notes.txt",
            ),
            "unexpected admission namespace object".into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    let validated = validate_operator_authority(
        authority(),
        Arc::clone(&store),
        "local-k8s-authority",
        "v1/operator-startup-probes",
    )
    .await
    .unwrap();
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);

    let err = components
        .ingest_admission_startup_preflight()
        .await
        .unwrap_err();

    assert!(
        matches!(err, velorix_k8s::stream_watch::StreamWatchError::Snapshot { message }
        if message.contains("unexpected object under v1/ingest-admission"))
    );
}

#[tokio::test]
async fn deployed_ingest_writer_runtime_preflights_before_append() {
    let (_temp_dir, store) = temp_store();
    create_relation_catalog(&store).await;
    let validated = validate_operator_authority(
        authority(),
        Arc::clone(&store),
        "local-k8s-authority",
        "v1/operator-startup-probes",
    )
    .await
    .unwrap();
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);

    let runtime = DeployedIngestWriterRuntime::from_startup_components(&components)
        .await
        .unwrap();
    assert_eq!(runtime.authority(), components.authority());
    assert_eq!(runtime.startup_report().active_admission_records, 0);
    assert_eq!(runtime.startup_report().expired_orphan_admission_records, 0);

    let outcome = runtime
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(0, 100))
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        AppendValidatedEnvelopeOutcome::Appended { descriptor }
            if descriptor.stream_id == "deposits"
                && descriptor.partition_id == 0
                && descriptor.start_offset_inclusive == 0
                && descriptor.end_offset_exclusive == 100
    ));
}

#[tokio::test]
async fn deployed_ingest_writer_runtime_rejects_malformed_admission_before_append() {
    let (_temp_dir, store) = temp_store();
    create_relation_catalog(&store).await;
    store
        .put_opts(
            &Path::from(
                "v1/ingest-admission/deposits/p=0000000000/ranges/00000000000000000000-00000000000000000010/notes.txt",
            ),
            "unexpected admission namespace object".into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    let validated = validate_operator_authority(
        authority(),
        Arc::clone(&store),
        "local-k8s-authority",
        "v1/operator-startup-probes",
    )
    .await
    .unwrap();
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);

    let err = DeployedIngestWriterRuntime::from_startup_components(&components)
        .await
        .unwrap_err();

    assert!(
        matches!(err, velorix_k8s::stream_watch::StreamWatchError::Snapshot { message }
        if message.contains("unexpected object under v1/ingest-admission"))
    );
}

#[test]
fn deployed_ingest_writer_pod_template_builds_deterministic_identity_bound_pod() {
    let template = IngestWriterPodTemplate::new("ghcr.io/velorix/velorix-ingest-writer:1.0.0")
        .unwrap()
        .with_command(["velorix-ingest-writer"])
        .with_args(["serve"])
        .with_label("control.velorix.io/custom", "startup-preflighted")
        .with_service_account_name("velorix-ingest-writer");
    let identity = IngestWriterRuntimeIdentity {
        namespace: "analytics".to_string(),
        authority: authority(),
        operator_id: "Operator_A/West".to_string(),
        writer_id: "Writer_A/0".to_string(),
    };

    let first = template.pod_for_identity(&identity);
    let second = template.pod_for_identity(&identity);
    assert_eq!(first, second);

    let labels = first.metadata.labels.unwrap();
    let spec = first.spec.unwrap();
    let container = spec.containers.into_iter().next().unwrap();

    assert_eq!(
        first.metadata.name.as_deref(),
        Some(ingest_writer_pod_name_for_identity(&identity).as_str())
    );
    assert_eq!(labels["app.kubernetes.io/name"], "velorix-ingest-writer");
    assert_eq!(labels["app.kubernetes.io/component"], "ingest-writer");
    assert_eq!(labels["control.velorix.io/operator-id"], "operator-a-west");
    assert_eq!(labels["control.velorix.io/writer-id"], "writer-a-0");
    assert_eq!(labels["control.velorix.io/authority-store-id"], "primary");
    assert_eq!(
        labels["control.velorix.io/authority-namespace"],
        "analytics"
    );
    assert_eq!(labels["control.velorix.io/custom"], "startup-preflighted");
    assert_eq!(spec.restart_policy.as_deref(), Some("Never"));
    assert_eq!(
        spec.service_account_name.as_deref(),
        Some("velorix-ingest-writer")
    );
    assert_eq!(container.name, "velorix-ingest-writer");
    assert_eq!(
        container.image.as_deref(),
        Some("ghcr.io/velorix/velorix-ingest-writer:1.0.0")
    );
    assert_eq!(
        container.command.as_deref(),
        Some(["velorix-ingest-writer".to_string()].as_slice())
    );
    assert_eq!(
        container.args.as_deref(),
        Some(["serve".to_string()].as_slice())
    );
    assert_eq!(
        container.env.unwrap(),
        vec![
            env_var("VELORIX_INGEST_WRITER_NAMESPACE", "analytics"),
            env_var("VELORIX_INGEST_WRITER_AUTHORITY_STORE_ID", "primary"),
            env_var("VELORIX_INGEST_WRITER_AUTHORITY_NAMESPACE", "analytics"),
            env_var("VELORIX_INGEST_WRITER_OPERATOR_ID", "Operator_A/West"),
            env_var("VELORIX_INGEST_WRITER_ID", "Writer_A/0"),
        ]
    );
}

#[test]
fn deployed_ingest_writer_pod_template_rejects_empty_image() {
    let error = IngestWriterPodTemplate::new(" ").unwrap_err();

    assert!(error
        .to_string()
        .contains("ingest writer pod image must not be empty"));
}

#[tokio::test]
async fn deployed_ingest_writer_kubernetes_runtime_exposes_pod_executor_after_checked_runtime() {
    let (_temp_dir, store) = temp_store();
    let validated = validate_operator_authority(
        authority(),
        Arc::clone(&store),
        "local-k8s-authority",
        "v1/operator-startup-probes",
    )
    .await
    .unwrap();
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);

    let runtime = build_kubernetes_ingest_writer_operator_runtime(
        fake_kube_client(),
        "analytics",
        &components,
        IngestWriterPodTemplate::new("ghcr.io/velorix/velorix-ingest-writer:1.0.0").unwrap(),
        "operator-a",
        "writer-a",
    )
    .await
    .unwrap();

    assert_eq!(
        runtime.deployed_runtime().authority(),
        components.authority()
    );
    assert_eq!(runtime.pod_executor().identity().authority, authority());
    assert_eq!(runtime.pod_executor().identity().namespace, "analytics");
    assert_eq!(runtime.pod_executor().identity().operator_id, "operator-a");
    assert_eq!(runtime.pod_executor().identity().writer_id, "writer-a");
}

#[tokio::test]
async fn deployed_ingest_writer_kubernetes_runtime_rejects_malformed_admission_before_pod_executor()
{
    let (_temp_dir, store) = temp_store();
    store
        .put_opts(
            &Path::from(
                "v1/ingest-admission/deposits/p=0000000000/ranges/00000000000000000000-00000000000000000010/notes.txt",
            ),
            "unexpected admission namespace object".into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
    let validated = validate_operator_authority(
        authority(),
        Arc::clone(&store),
        "local-k8s-authority",
        "v1/operator-startup-probes",
    )
    .await
    .unwrap();
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);

    let err = build_kubernetes_ingest_writer_operator_runtime(
        fake_kube_client(),
        "analytics",
        &components,
        IngestWriterPodTemplate::new("ghcr.io/velorix/velorix-ingest-writer:1.0.0").unwrap(),
        "operator-a",
        "writer-a",
    )
    .await
    .unwrap_err();

    assert!(err
        .to_string()
        .contains("unexpected object under v1/ingest-admission"));
}

#[tokio::test]
async fn deployed_ingest_writer_pod_executor_rejects_conflicting_existing_pod() {
    let template = IngestWriterPodTemplate::new("ghcr.io/velorix/velorix-ingest-writer:1.0.0")
        .unwrap()
        .with_args(["serve"]);
    let requested = IngestWriterRuntimeIdentity {
        namespace: "analytics".to_string(),
        authority: authority(),
        operator_id: "operator-a".to_string(),
        writer_id: "writer-a".to_string(),
    };
    let conflicting = IngestWriterRuntimeIdentity {
        namespace: "analytics".to_string(),
        authority: authority(),
        operator_id: "operator-a".to_string(),
        writer_id: "writer-b".to_string(),
    };
    let mut existing_pod = template.pod_for_identity(&conflicting);
    existing_pod.metadata.name = Some(ingest_writer_pod_name_for_identity(&requested));
    let client = fake_ingest_writer_pod_create_conflict_then_get_client(existing_pod);
    let executor = velorix_k8s::ingest_writer::KubernetesPodIngestWriterExecutor::new(
        client,
        "analytics",
        template,
        requested,
    );

    let err = executor.create_writer_pod().await.unwrap_err();

    assert!(err.to_string().contains("identity mismatch"));
}

#[test]
fn deployed_ingest_writer_kubernetes_runtime_source_gates_pod_executor_after_preflight() {
    let source_code = include_str!("../src/ingest_writer.rs");
    let runtime_body = function_body(
        source_code,
        "pub async fn build_kubernetes_ingest_writer_operator_runtime(",
    )
    .expect("deployed ingest writer Kubernetes assembly function should exist");

    assert!(
        runtime_body.contains("startup_components: &OperatorAuthorityStartupComponents"),
        "Kubernetes ingest writer runtime assembly must require checked startup components"
    );
    let checked_runtime_index = runtime_body
        .find("DeployedIngestWriterRuntime::from_startup_components(startup_components).await?")
        .expect("assembly must construct checked ingest writer runtime first");
    let pod_executor_index = runtime_body
        .find("KubernetesPodIngestWriterExecutor::new(")
        .expect("assembly must expose a Kubernetes pod executor");
    assert!(
        checked_runtime_index < pod_executor_index,
        "pod executor must not be constructed before ingest admission startup preflight succeeds"
    );
}

#[test]
fn deployed_ingest_writer_runtime_source_requires_startup_components() {
    let source_code = include_str!("../src/ingest_writer.rs");
    let runtime_body = function_body(source_code, "pub async fn from_startup_components(")
        .expect("deployed ingest writer runtime assembly function should exist");

    assert!(
        runtime_body.contains("startup_components: &OperatorAuthorityStartupComponents"),
        "deployed ingest writer runtime assembly must require checked startup components"
    );
    assert!(
        runtime_body.contains("startup_components.ingest_admission_coordinator_provider()"),
        "deployed ingest writer runtime must construct admission from startup components"
    );
    assert!(
        runtime_body.contains("provider.coordinator_after_startup_reconstruction().await?"),
        "deployed ingest writer runtime must reconstruct admission before append is exposed"
    );
    for forbidden_call in [
        "IngestAdmissionCoordinator::new_checked(",
        "IngestAdmissionCoordinatorProvider::from_authority_parts(",
        "validate_operator_authority(",
    ] {
        assert!(
            !runtime_body.contains(forbidden_call),
            "deployed ingest writer runtime must not bypass OperatorAuthorityStartupComponents with {forbidden_call}",
        );
    }
}

#[test]
fn k8s_authority_part_factories_are_startup_token_gated() {
    let src_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let startup_source = strip_line_comments(
        &fs::read_to_string(src_dir.join("startup.rs")).expect("read startup source"),
    );
    let stream_watch_source = strip_line_comments(
        &fs::read_to_string(src_dir.join("stream_watch.rs")).expect("read stream-watch source"),
    );
    let worker_shard_source = strip_line_comments(
        &fs::read_to_string(src_dir.join("worker_shard.rs")).expect("read worker-shard source"),
    );

    assert!(
        startup_source.contains("pub(crate) struct ValidatedStartupAuthorityToken"),
        "startup module must own the crate-visible validated startup token type"
    );
    assert!(
        startup_source.contains("fn new() -> Self")
            && !startup_source.contains("pub(crate) fn new() -> Self"),
        "validated startup token construction must remain private to startup.rs"
    );
    assert!(
        stream_watch_source
            .matches("_token: ValidatedStartupAuthorityToken")
            .count()
            >= 2,
        "stream-watch authority-part factories must require the startup token"
    );
    assert!(
        worker_shard_source
            .matches("_token: ValidatedStartupAuthorityToken")
            .count()
            >= 1,
        "worker-shard authority-part factory must require the startup token"
    );

    let mut violations = Vec::new();
    for entry in fs::read_dir(&src_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()) == Some("startup.rs") {
            continue;
        }

        let source_code = strip_line_comments(&fs::read_to_string(&path).unwrap());
        for (line_number, line) in source_code.lines().enumerate() {
            if line.contains("from_authority_parts(") && !line.contains("fn from_authority_parts(")
            {
                violations.push(format!(
                    "{}:{} calls crate-local authority-part factory outside startup.rs",
                    path.strip_prefix(&src_dir).unwrap_or(&path).display(),
                    line_number + 1
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "k8s authority-part factories must only be called by OperatorAuthorityStartupComponents:\n{}",
        violations.join("\n")
    );
}

#[test]
fn live_gate_sources_construct_production_components_through_operator_startup_components() {
    for (path, source, forbidden) in [
        (
            "stream_watch.rs",
            include_str!("stream_watch.rs"),
            "RelationCatalogSnapshotProvider::for_production",
        ),
        (
            "live_ingest_admission.rs",
            include_str!("live_ingest_admission.rs"),
            "IngestAdmissionCoordinatorProvider::for_production",
        ),
        (
            "live_worker_shard.rs",
            include_str!("live_worker_shard.rs"),
            "CheckpointPublisherEpochStore::for_production",
        ),
    ] {
        let source_code = strip_line_comments(source);
        assert!(
            source_code.contains("OperatorAuthorityStartupComponents::from_validated_authority"),
            "{path} must route live gate construction through OperatorAuthorityStartupComponents",
        );
        assert!(
            !source_code.contains(forbidden),
            "{path} must not directly call {forbidden}",
        );
    }
}

#[test]
fn stream_watch_exposes_startup_component_kubernetes_runtime() {
    let source_code = include_str!("../src/stream_watch.rs");

    assert!(
        source_code.contains("pub async fn watch_streams_with_kubernetes_runtime("),
        "stream watch should expose a Kubernetes runtime assembly helper that takes startup components"
    );
    assert!(
        source_code.contains("startup_components.relation_snapshot_provider()"),
        "stream-watch runtime assembly must build snapshots from OperatorAuthorityStartupComponents"
    );
    assert!(
        source_code.contains("StreamStatusWriter::new(KubeStreamStatusApi::new(client.clone()))"),
        "stream-watch runtime assembly must wire the Kubernetes status writer"
    );
}

#[test]
fn worker_shard_exposes_startup_component_kubernetes_runtime() {
    let source_code = include_str!("../src/worker_shard.rs");
    let runtime_body = function_body(
        source_code,
        "pub fn build_kubernetes_worker_shard_operator_runtime(",
    )
    .expect("worker-shard runtime assembly function should exist");
    let watch_body = function_body(
        source_code,
        "pub async fn watch_worker_shards_with_kubernetes_runtime(",
    )
    .expect("worker-shard watch runtime wrapper should exist");
    let resync_body = function_body(
        source_code,
        "pub async fn resync_worker_shards_before_watch_with_kubernetes_runtime(",
    )
    .expect("worker-shard startup resync runtime wrapper should exist");
    let resync_then_watch_body = function_body(
        source_code,
        "pub async fn watch_worker_shards_with_kubernetes_runtime_after_initial_resync(",
    )
    .expect("worker-shard resync-then-watch runtime wrapper should exist");
    let periodic_resync_body = function_body(
        source_code,
        "pub async fn resync_worker_shards_periodically_with_kubernetes_runtime(",
    )
    .expect("worker-shard periodic resync runtime wrapper should exist");
    let lifecycle_resync_body = function_body(
        source_code,
        "pub async fn run_worker_shards_with_kubernetes_runtime_lifecycle<Shutdown>(",
    )
    .expect("worker-shard lifecycle supervisor runtime wrapper should exist");

    assert!(
        runtime_body.contains("startup_components: &OperatorAuthorityStartupComponents"),
        "worker-shard runtime assembly must take checked startup components"
    );
    assert!(
        runtime_body.contains("startup_components.worker_shard_epoch_store()?"),
        "worker-shard runtime assembly must build the epoch store from startup components"
    );
    assert!(
        runtime_body
            .contains("KubernetesPartitionLeaseClient::new(KubeLeaseApi::new(client.clone()))"),
        "worker-shard runtime assembly must wire Kubernetes Lease coordination"
    );
    assert!(
        runtime_body.contains(
            "KubernetesPodWorkerShardScopedCommandExecutor::new(client, namespace, pod_template)"
        ),
        "worker-shard runtime assembly must wire the scoped Kubernetes Pod executor"
    );
    assert!(
        runtime_body.contains("startup_components.authority().clone()"),
        "worker-shard runtime assembly must keep the validated authority on the runtime"
    );
    assert!(
        !runtime_body.contains("CheckpointPublisher::new_checked("),
        "worker-shard runtime assembly must not bypass startup components for ownership epoch construction"
    );

    for (name, body) in [
        ("watch", watch_body),
        ("startup resync", resync_body),
        ("resync then watch", resync_then_watch_body),
        ("periodic resync", periodic_resync_body),
        ("lifecycle supervisor", lifecycle_resync_body),
    ] {
        assert!(
            body.contains("build_kubernetes_worker_shard_operator_runtime(")
                || body.contains("resync_worker_shards_before_watch_with_kubernetes_runtime("),
            "{name} worker-shard Kubernetes wrapper must use the startup-component runtime assembly"
        );
    }
}

#[test]
fn k8s_src_runtime_assembly_does_not_call_direct_production_constructors() {
    let forbidden = [
        "RelationCatalogSnapshotProvider::for_production",
        "IngestAdmissionCoordinatorProvider::for_production",
        "CheckpointPublisherEpochStore::for_production",
    ];
    let src_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");

    for entry in fs::read_dir(&src_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }

        let source_code = strip_line_comments(&fs::read_to_string(&path).unwrap());
        for forbidden_call in forbidden {
            assert!(
                !source_code.contains(forbidden_call),
                "{} must route live gate construction through OperatorAuthorityStartupComponents, not {forbidden_call}",
                path.display(),
            );
        }
    }
}

fn strip_line_comments(source: &str) -> String {
    source
        .lines()
        .map(|line| line.split_once("//").map_or(line, |(code, _comment)| code))
        .collect::<Vec<_>>()
        .join("\n")
}

fn function_body<'a>(source: &'a str, signature: &str) -> Option<&'a str> {
    let signature_start = source.find(signature)?;
    let body_start = source[signature_start..].find('{')? + signature_start;
    let mut depth = 0usize;
    for (offset, byte) in source[body_start..].bytes().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(&source[signature_start..=body_start + offset]);
                }
            }
            _ => {}
        }
    }
    None
}

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

async fn create_relation_catalog(store: &Arc<dyn ObjectStore>) {
    let catalog = serde_json::from_value(relation_catalog_json()).unwrap();
    RelationCatalogRegistry::new(Arc::clone(store))
        .create(&catalog)
        .await
        .unwrap();
}

fn stream() -> VelorixStream {
    let mut stream = VelorixStream::new(
        "deposits",
        VelorixStreamSpec {
            stream_id: "deposits".to_string(),
            database_id: "analytics".to_string(),
            relation: relation(),
            authority: authority(),
        },
    );
    stream.metadata.namespace = Some("analytics".to_string());
    stream.metadata.generation = Some(1);
    stream
}

fn authority() -> ObjectStoreAuthorityRef {
    ObjectStoreAuthorityRef {
        store_id: "primary".to_string(),
        namespace: "analytics".to_string(),
    }
}

fn relation() -> RelationVersionRef {
    RelationVersionRef {
        relation_id: "deposits".to_string(),
        relation_version: 1,
        schema_fingerprint:
            "sha256:9b09fa82241fce3bb9025911ed78168799ad384fe68f065258afe09eca6ede62".to_string(),
    }
}

fn relation_catalog_json() -> Value {
    json!({
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
        "schema_fingerprint": relation().schema_fingerprint,
        "datafusion_registration": {
            "name": "deposits",
            "mode": "table",
        },
        "feldera_relation": {
            "relation_id": "deposits",
            "schema_fingerprint": relation().schema_fingerprint,
        },
        "incremental_adapter": {
            "adapter_id": "incremental-adapter-single-key-sum-count-v1",
        },
    })
}

fn catalog_envelope_bytes_for(start_offset_inclusive: u64, end_offset_exclusive: u64) -> Bytes {
    IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "deposits".to_string(),
            relation_version: "1".to_string(),
            schema_fingerprint: relation().schema_fingerprint,
            stream_id: "deposits".to_string(),
            partition_id: 0,
            start_offset_inclusive,
            end_offset_exclusive,
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

fn ownership_epoch_record() -> OwnershipEpochRecord {
    OwnershipEpochRecord {
        stream_id: "deposits".to_string(),
        partition_id: 0,
        owner_id: "worker-a".to_string(),
        owner_epoch: 1,
        lease_identity: "analytics/deposits/0".to_string(),
        created_at: "2026-05-14T00:00:00Z".to_string(),
        previous_epoch: None,
        previous_checkpoint_version: None,
    }
}

fn env_var(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value: Some(value.to_string()),
        value_from: None,
    }
}

fn fake_kube_client() -> kube::Client {
    let service = tower::service_fn(|_request: Request<Body>| async move {
        Ok::<_, Infallible>(
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "apiVersion": "v1",
                        "kind": "Status",
                        "metadata": {},
                        "status": "Failure",
                        "message": "fake client should not be called during assembly",
                        "reason": "InternalError",
                        "code": 500
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
    });

    ClientBuilder::new(service, "default").build()
}

fn fake_ingest_writer_pod_create_conflict_then_get_client(
    existing_pod: k8s_openapi::api::core::v1::Pod,
) -> kube::Client {
    let existing_pod = Arc::new(existing_pod);
    let service = tower::service_fn({
        let existing_pod = Arc::clone(&existing_pod);
        move |request: Request<Body>| {
            let existing_pod = Arc::clone(&existing_pod);
            async move {
                let response_body = match *request.method() {
                    Method::POST => json!({
                        "apiVersion": "v1",
                        "kind": "Status",
                        "metadata": {},
                        "status": "Failure",
                        "message": "already exists",
                        "reason": "AlreadyExists",
                        "code": 409
                    }),
                    Method::GET => serde_json::to_value(&*existing_pod).unwrap(),
                    _ => json!({
                        "apiVersion": "v1",
                        "kind": "Status",
                        "metadata": {},
                        "status": "Failure",
                        "message": "unexpected fake kubernetes request",
                        "reason": "InternalError",
                        "code": 500
                    }),
                };
                let status = if *request.method() == Method::POST {
                    StatusCode::CONFLICT
                } else if *request.method() == Method::GET {
                    StatusCode::OK
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };

                Ok::<_, Infallible>(
                    Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&response_body).unwrap()))
                        .unwrap(),
                )
            }
        }
    });

    ClientBuilder::new(service, "default").build()
}

#[derive(Debug)]
struct OverwriteCreateStore {
    inner: Arc<dyn ObjectStore>,
}

impl fmt::Display for OverwriteCreateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OverwriteCreateStore")
    }
}

#[async_trait]
impl ObjectStore for OverwriteCreateStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if matches!(opts.mode, PutMode::Create) {
            opts.mode = PutMode::Overwrite;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
