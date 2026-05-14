use std::{fmt, sync::Arc};

use async_trait::async_trait;
use futures::stream::BoxStream;
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
    startup::{
        validate_operator_authority, OperatorAuthorityStartupComponents, OperatorStartupError,
    },
    stream_watch::{
        AuthoritySnapshotProvider, IngestAdmissionCoordinatorProvider,
        RelationCatalogSnapshotProvider,
    },
    worker_shard::WorkerShardEpochStore,
};
use velorix_storage::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilityProbeError,
        ObjectStoreCapabilityProbeError, RequiredObjectStoreCapability,
    },
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
    let provider = RelationCatalogSnapshotProvider::for_production(validated);

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
    let provider = RelationCatalogSnapshotProvider::for_production(validated);

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
    let provider = IngestAdmissionCoordinatorProvider::for_production(validated);

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
    let provider = IngestAdmissionCoordinatorProvider::for_production(validated);

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
