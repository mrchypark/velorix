use std::{fmt, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    local::LocalFileSystem, path::Path, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use tempfile::TempDir;
use velorix_storage::{
    capability::{
        probe_authoritative_object_store_capabilities, probe_object_store_capabilities,
        probe_production_object_store_profile, AuthoritativeNamespace,
        AuthoritativeObjectStoreCapabilitiesV1, AuthoritativeObjectStoreCapabilityError,
        AuthoritativeObjectStoreCapabilityProbeError, ObjectStoreCapabilityError,
        ObjectStoreCapabilityProbeError, ObjectStoreCapabilityProfile,
        RequiredObjectStoreCapability,
    },
    log::{IngestAdmissionCoordinator, IngestBatch, IngestLog, IngestLogError},
    manifest::{CheckpointManifest, InputRange, StateObjectRef},
    state::{CheckpointPublishError, CheckpointPublisher, StateObjectWrite},
    state_store::{RawObjectStateStore, SlateDbStateStore},
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

fn complete_profile(backend_name: &str) -> ObjectStoreCapabilityProfile {
    ObjectStoreCapabilityProfile {
        backend_name: backend_name.to_string(),
        conditional_create: true,
        atomic_visibility: true,
        list_after_write: true,
        read_after_write: true,
    }
}

fn all_namespace_profiles(
) -> std::collections::BTreeMap<AuthoritativeNamespace, ObjectStoreCapabilityProfile> {
    AuthoritativeNamespace::all()
        .into_iter()
        .map(|namespace| {
            (
                namespace,
                complete_profile(&format!("complete-{namespace}")),
            )
        })
        .collect()
}

fn profile_missing(
    required_capability: RequiredObjectStoreCapability,
) -> ObjectStoreCapabilityProfile {
    let mut profile = complete_profile(&format!("missing-{required_capability}"));

    match required_capability {
        RequiredObjectStoreCapability::ConditionalCreate => profile.conditional_create = false,
        RequiredObjectStoreCapability::AtomicVisibility => profile.atomic_visibility = false,
        RequiredObjectStoreCapability::ListAfterWrite => profile.list_after_write = false,
        RequiredObjectStoreCapability::ReadAfterWrite => profile.read_after_write = false,
    }

    profile
}

fn assert_capability_error(
    err: ObjectStoreCapabilityError,
    profile: &ObjectStoreCapabilityProfile,
    expected: RequiredObjectStoreCapability,
) {
    assert_eq!(err.backend_name(), profile.backend_name);
    assert_eq!(err.required_capability(), expected);
}

fn expected_authoritative_namespaces() -> [AuthoritativeNamespace; 18] {
    [
        AuthoritativeNamespace::Ingest,
        AuthoritativeNamespace::IngestAdmission,
        AuthoritativeNamespace::State,
        AuthoritativeNamespace::Output,
        AuthoritativeNamespace::Checkpoint,
        AuthoritativeNamespace::CheckpointIndex,
        AuthoritativeNamespace::CheckpointLifecycle,
        AuthoritativeNamespace::CheckpointRetention,
        AuthoritativeNamespace::CheckpointGcTransition,
        AuthoritativeNamespace::CheckpointRecovery,
        AuthoritativeNamespace::Ownership,
        AuthoritativeNamespace::TableCatalog,
        AuthoritativeNamespace::RelationCatalog,
        AuthoritativeNamespace::ArtifactCatalog,
        AuthoritativeNamespace::BenchmarkEvidence,
        AuthoritativeNamespace::GcRuns,
        AuthoritativeNamespace::Queries,
        AuthoritativeNamespace::QueryPolicy,
    ]
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

fn manifest_for(state_ref: StateObjectRef) -> CheckpointManifest {
    CheckpointManifest {
        schema_version: 1,
        checkpoint_version: 0,
        input_ranges: vec![InputRange {
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 10,
        }],
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint: None,
        created_at: "2026-05-04T00:00:00Z".to_string(),
    }
}

#[tokio::test]
async fn object_store_capability_probe_observes_create_read_and_list_behavior() {
    let (_temp_dir, store) = temp_store();

    let report =
        probe_object_store_capabilities(store.as_ref(), "local-test", "v1/capability-probes")
            .await
            .unwrap();

    assert_eq!(report.backend_name, "local-test");
    assert!(report.probe_key.starts_with("v1/capability-probes/"));
    assert!(report.conditional_create);
    assert!(report.atomic_visibility);
    assert!(report.list_after_write);
    assert!(report.read_after_write);
    report
        .observed_profile()
        .validate_for_velorix_durability()
        .unwrap();
}

#[test]
fn authoritative_namespaces_include_current_velorix_owned_durable_prefixes() {
    assert_eq!(
        AuthoritativeNamespace::all().as_slice(),
        expected_authoritative_namespaces().as_slice()
    );
}

#[tokio::test]
async fn production_capability_probe_rejects_store_without_create_only_behavior() {
    let (_temp_dir, inner) = temp_store();
    let store = OverwriteCreateStore { inner };

    let err = probe_production_object_store_profile(&store, "overwrite-create", "v1/probes")
        .await
        .unwrap_err();

    match err {
        ObjectStoreCapabilityProbeError::Capability(err) => {
            assert_eq!(err.backend_name(), "overwrite-create");
            assert_eq!(
                err.required_capability(),
                RequiredObjectStoreCapability::ConditionalCreate
            );
        }
        other => panic!("expected capability error, got {other:?}"),
    }
}

#[tokio::test]
async fn authoritative_capability_probe_covers_every_namespace() {
    let (_temp_dir, store) = temp_store();

    let capabilities =
        probe_authoritative_object_store_capabilities(store.as_ref(), "local-test", "v1/probes")
            .await
            .unwrap();

    capabilities.validate_for_startup().unwrap();
    let observed_namespaces = capabilities.profiles.keys().copied().collect::<Vec<_>>();
    assert_eq!(
        observed_namespaces.as_slice(),
        expected_authoritative_namespaces().as_slice()
    );
    for namespace in AuthoritativeNamespace::all() {
        let profile = capabilities.profiles.get(&namespace).unwrap();
        assert_eq!(profile.backend_name, "local-test");
    }
}

#[tokio::test]
async fn authoritative_capability_probe_reports_namespace_for_create_only_failure() {
    let (_temp_dir, inner) = temp_store();
    let store = OverwriteCreateStore { inner };

    let err =
        probe_authoritative_object_store_capabilities(&store, "overwrite-create", "v1/probes")
            .await
            .unwrap_err();

    match err {
        AuthoritativeObjectStoreCapabilityProbeError::Namespace { namespace, source } => {
            assert_eq!(namespace, AuthoritativeNamespace::Ingest);
            match source {
                ObjectStoreCapabilityProbeError::Capability(err) => {
                    assert_eq!(
                        err.required_capability(),
                        RequiredObjectStoreCapability::ConditionalCreate
                    );
                }
                other => panic!("expected capability error, got {other:?}"),
            }
        }
    }
}

#[test]
fn authoritative_capabilities_reject_missing_namespace() {
    let mut profiles = all_namespace_profiles();
    profiles.remove(&AuthoritativeNamespace::IngestAdmission);
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let error = capabilities.validate_for_startup().unwrap_err();

    assert!(matches!(
        error,
        AuthoritativeObjectStoreCapabilityError::MissingNamespace {
            namespace: AuthoritativeNamespace::IngestAdmission
        }
    ));
}

#[test]
fn authoritative_capabilities_report_weak_namespace_profile() {
    let mut profiles = all_namespace_profiles();
    let profile = profile_missing(RequiredObjectStoreCapability::ConditionalCreate);
    profiles.insert(AuthoritativeNamespace::IngestAdmission, profile.clone());
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let error = capabilities.validate_for_startup().unwrap_err();

    match error {
        AuthoritativeObjectStoreCapabilityError::NamespaceProfile { namespace, source } => {
            assert_eq!(namespace, AuthoritativeNamespace::IngestAdmission);
            assert_capability_error(
                source,
                &profile,
                RequiredObjectStoreCapability::ConditionalCreate,
            );
        }
        other => panic!("expected namespace profile error, got {other:?}"),
    }
}

#[test]
fn authoritative_capabilities_reject_weak_production_namespace_profiles() {
    for required_capability in [
        RequiredObjectStoreCapability::ConditionalCreate,
        RequiredObjectStoreCapability::ListAfterWrite,
    ] {
        let mut profiles = all_namespace_profiles();
        let profile = profile_missing(required_capability);
        profiles.insert(AuthoritativeNamespace::RelationCatalog, profile.clone());
        let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

        let error = capabilities.validate_for_startup().unwrap_err();

        match error {
            AuthoritativeObjectStoreCapabilityError::NamespaceProfile { namespace, source } => {
                assert_eq!(namespace, AuthoritativeNamespace::RelationCatalog);
                assert_capability_error(source, &profile, required_capability);
            }
            other => panic!("expected namespace profile error, got {other:?}"),
        }
    }
}

#[test]
fn authoritative_capabilities_accept_all_valid_namespaces() {
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(all_namespace_profiles());

    capabilities.validate_for_startup().unwrap();
}

#[test]
fn authoritative_capabilities_diagnostics_report_backend_namespace_and_missing_capabilities() {
    let mut profiles = all_namespace_profiles();
    let weak_profile = profile_missing(RequiredObjectStoreCapability::ConditionalCreate);
    profiles.insert(AuthoritativeNamespace::Output, weak_profile.clone());
    profiles.remove(&AuthoritativeNamespace::GcRuns);
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let diagnostics = capabilities.diagnostics();
    let observed_namespaces = diagnostics
        .iter()
        .map(|diagnostic| diagnostic.namespace)
        .collect::<Vec<_>>();
    assert_eq!(
        observed_namespaces.as_slice(),
        expected_authoritative_namespaces().as_slice()
    );

    let output = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.namespace == AuthoritativeNamespace::Output)
        .unwrap();
    assert_eq!(
        output.backend_name.as_deref(),
        Some(weak_profile.backend_name.as_str())
    );
    assert_eq!(
        output.missing_capabilities,
        vec![RequiredObjectStoreCapability::ConditionalCreate]
    );

    let gc_runs = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.namespace == AuthoritativeNamespace::GcRuns)
        .unwrap();
    assert_eq!(gc_runs.backend_name, None);
    assert_eq!(
        gc_runs.missing_capabilities,
        vec![
            RequiredObjectStoreCapability::ConditionalCreate,
            RequiredObjectStoreCapability::AtomicVisibility,
            RequiredObjectStoreCapability::ListAfterWrite,
            RequiredObjectStoreCapability::ReadAfterWrite,
        ]
    );
}

#[test]
fn capability_profile_validation_reports_each_missing_required_capability() {
    for required_capability in [
        RequiredObjectStoreCapability::ConditionalCreate,
        RequiredObjectStoreCapability::AtomicVisibility,
        RequiredObjectStoreCapability::ListAfterWrite,
        RequiredObjectStoreCapability::ReadAfterWrite,
    ] {
        let profile = profile_missing(required_capability);

        let err = profile.validate_for_velorix_durability().unwrap_err();

        assert_capability_error(err, &profile, required_capability);
    }
}

#[test]
fn capability_gate_rejects_each_missing_capability_for_checkpoint_publisher() {
    for required_capability in [
        RequiredObjectStoreCapability::ConditionalCreate,
        RequiredObjectStoreCapability::AtomicVisibility,
        RequiredObjectStoreCapability::ListAfterWrite,
        RequiredObjectStoreCapability::ReadAfterWrite,
    ] {
        let (_temp_dir, store) = temp_store();
        let profile = profile_missing(required_capability);

        let err = CheckpointPublisher::new_checked(store, &profile).unwrap_err();

        assert_capability_error(err, &profile, required_capability);
    }
}

#[test]
fn capability_gate_rejects_each_missing_capability_for_ingest_log() {
    for required_capability in [
        RequiredObjectStoreCapability::ConditionalCreate,
        RequiredObjectStoreCapability::AtomicVisibility,
        RequiredObjectStoreCapability::ListAfterWrite,
        RequiredObjectStoreCapability::ReadAfterWrite,
    ] {
        let (_temp_dir, store) = temp_store();
        let profile = profile_missing(required_capability);

        let err = IngestLog::new_checked(store, &profile).unwrap_err();

        assert_capability_error(err, &profile, required_capability);
    }
}

#[test]
fn authoritative_capability_gate_rejects_missing_checkpoint_gc_transition_namespace_for_checkpoint_publisher(
) {
    let (_temp_dir, store) = temp_store();
    let mut profiles = all_namespace_profiles();
    profiles.remove(&AuthoritativeNamespace::CheckpointGcTransition);
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let err = CheckpointPublisher::new_authoritative(store, &capabilities).unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::AuthoritativeObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace {
                namespace: AuthoritativeNamespace::CheckpointGcTransition
            }
        )
    ));
}

#[test]
fn capability_gate_rejects_missing_ingest_capability_for_ingest_admission_coordinator() {
    let (_temp_dir, store) = temp_store();
    let mut profiles = all_namespace_profiles();
    profiles.remove(&AuthoritativeNamespace::Ingest);
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let Err(err) = IngestAdmissionCoordinator::new_checked(store, &capabilities) else {
        panic!("expected ingest capability validation to reject coordinator construction");
    };

    assert!(matches!(
        err,
        AuthoritativeObjectStoreCapabilityError::MissingNamespace {
            namespace: AuthoritativeNamespace::Ingest
        }
    ));
}

#[test]
fn capability_gate_rejects_missing_ingest_admission_capability_for_ingest_admission_coordinator() {
    let (_temp_dir, store) = temp_store();
    let mut profiles = all_namespace_profiles();
    profiles.remove(&AuthoritativeNamespace::IngestAdmission);
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let Err(err) = IngestAdmissionCoordinator::new_checked(store, &capabilities) else {
        panic!(
            "expected ingest-admission capability validation to reject coordinator construction"
        );
    };

    assert!(matches!(
        err,
        AuthoritativeObjectStoreCapabilityError::MissingNamespace {
            namespace: AuthoritativeNamespace::IngestAdmission
        }
    ));
}

#[test]
fn capability_gate_rejects_missing_relation_catalog_capability_for_ingest_admission_coordinator() {
    let (_temp_dir, store) = temp_store();
    let mut profiles = all_namespace_profiles();
    profiles.remove(&AuthoritativeNamespace::RelationCatalog);
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let Err(err) = IngestAdmissionCoordinator::new_checked(store, &capabilities) else {
        panic!(
            "expected relation-catalog capability validation to reject coordinator construction"
        );
    };

    assert!(matches!(
        err,
        AuthoritativeObjectStoreCapabilityError::MissingNamespace {
            namespace: AuthoritativeNamespace::RelationCatalog
        }
    ));
}

#[test]
fn capability_gate_rejects_weak_relation_catalog_capability_for_ingest_admission_coordinator() {
    let (_temp_dir, store) = temp_store();
    let mut profiles = all_namespace_profiles();
    let relation_catalog_profile =
        profile_missing(RequiredObjectStoreCapability::ConditionalCreate);
    profiles.insert(
        AuthoritativeNamespace::RelationCatalog,
        relation_catalog_profile.clone(),
    );
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let Err(err) = IngestAdmissionCoordinator::new_checked(store, &capabilities) else {
        panic!(
            "expected weak relation-catalog capability validation to reject coordinator construction"
        );
    };

    match err {
        AuthoritativeObjectStoreCapabilityError::NamespaceProfile { namespace, source } => {
            assert_eq!(namespace, AuthoritativeNamespace::RelationCatalog);
            assert_capability_error(
                source,
                &relation_catalog_profile,
                RequiredObjectStoreCapability::ConditionalCreate,
            );
        }
        other => panic!("expected relation-catalog namespace profile error, got {other:?}"),
    }
}

#[test]
fn capability_gate_rejects_custom_state_store_checkpoint_publisher() {
    let (_temp_dir, store) = temp_store();
    let profile = profile_missing(RequiredObjectStoreCapability::ConditionalCreate);
    let state_store = Arc::new(RawObjectStateStore::new(Arc::clone(&store)));

    let err =
        CheckpointPublisher::with_state_store_checked(store, state_store, &profile).unwrap_err();

    assert_capability_error(
        err,
        &profile,
        RequiredObjectStoreCapability::ConditionalCreate,
    );
}

#[tokio::test]
async fn capability_gate_rejects_slatedb_checkpoint_publisher_before_opening_state_store() {
    let (_temp_dir, store) = temp_store();
    let profile = profile_missing(RequiredObjectStoreCapability::ConditionalCreate);

    let err = CheckpointPublisher::with_slatedb_state_store_checked(
        store,
        Path::from("state-db"),
        &profile,
    )
    .await
    .unwrap_err();

    match err {
        CheckpointPublishError::ObjectStoreCapability(err) => assert_capability_error(
            err,
            &profile,
            RequiredObjectStoreCapability::ConditionalCreate,
        ),
        other => panic!("expected object-store capability error, got {other:?}"),
    }
}

#[tokio::test]
async fn authoritative_capability_gate_rejects_slatedb_state_namespace_before_opening_state_store()
{
    let (_temp_dir, store) = temp_store();
    let mut profiles = all_namespace_profiles();
    profiles.remove(&AuthoritativeNamespace::State);
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let err = CheckpointPublisher::with_slatedb_state_store_authoritative(
        store,
        Path::from("state-db"),
        &capabilities,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::AuthoritativeObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace {
                namespace: AuthoritativeNamespace::State
            }
        )
    ));
}

#[tokio::test]
async fn authoritative_state_store_opener_rejects_missing_state_namespace_before_opening_state_store(
) {
    let (_temp_dir, store) = temp_store();
    let mut profiles = all_namespace_profiles();
    profiles.remove(&AuthoritativeNamespace::State);
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let err = SlateDbStateStore::open_authoritative(
        Path::from("state-db"),
        Arc::clone(&store),
        &capabilities,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::AuthoritativeObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace {
                namespace: AuthoritativeNamespace::State
            }
        )
    ));
}

#[tokio::test]
async fn authoritative_state_store_opener_rejects_weak_state_namespace_before_opening_state_store()
{
    let (_temp_dir, store) = temp_store();
    let mut profiles = all_namespace_profiles();
    let profile = profile_missing(RequiredObjectStoreCapability::ConditionalCreate);
    profiles.insert(AuthoritativeNamespace::State, profile.clone());
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let err = SlateDbStateStore::open_authoritative(
        Path::from("state-db"),
        Arc::clone(&store),
        &capabilities,
    )
    .await
    .unwrap_err();

    match err {
        CheckpointPublishError::AuthoritativeObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::NamespaceProfile { namespace, source },
        ) => {
            assert_eq!(namespace, AuthoritativeNamespace::State);
            assert_capability_error(
                source,
                &profile,
                RequiredObjectStoreCapability::ConditionalCreate,
            );
        }
        other => panic!("expected authoritative state namespace error, got {other:?}"),
    }
}

#[tokio::test]
async fn local_development_profile_allows_checkpoint_checked_construction_and_create_only_writes() {
    let (_temp_dir, store) = temp_store();
    let publisher =
        CheckpointPublisher::new_checked(store, &ObjectStoreCapabilityProfile::local_development())
            .unwrap();
    let state = StateObjectWrite::new(
        "balances_by_account",
        0,
        0,
        "state-0001",
        Bytes::from_static(b"state"),
    )
    .unwrap();

    let state_ref = publisher.write_state_object(&state).await.unwrap();
    let manifest = manifest_for(state_ref);
    publisher.publish_manifest(&manifest).await.unwrap();

    let err = publisher.publish_manifest(&manifest).await.unwrap_err();

    assert!(matches!(
        err,
        CheckpointPublishError::ManifestAlreadyExists(object_key)
            if object_key == manifest.object_key()
    ));
}

#[tokio::test]
async fn local_development_profile_allows_ingest_checked_construction_and_create_only_writes() {
    let (_temp_dir, store) = temp_store();
    let log =
        IngestLog::new_checked(store, &ObjectStoreCapabilityProfile::local_development()).unwrap();
    let batch = IngestBatch::new("orders", 0, 0, 10, Bytes::from_static(b"orders")).unwrap();

    log.append(&batch).await.unwrap();
    let err = log.append(&batch).await.unwrap_err();

    assert!(matches!(
        err,
        IngestLogError::AlreadyExists(object_key) if object_key == *batch.object_key()
    ));
}
