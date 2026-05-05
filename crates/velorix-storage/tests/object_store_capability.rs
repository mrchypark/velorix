use std::sync::Arc;

use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use tempfile::TempDir;
use velorix_storage::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
        AuthoritativeObjectStoreCapabilityError, ObjectStoreCapabilityError,
        ObjectStoreCapabilityProfile, RequiredObjectStoreCapability,
    },
    log::{IngestBatch, IngestLog, IngestLogError},
    manifest::{CheckpointManifest, InputRange, StateObjectRef},
    state::{CheckpointPublishError, CheckpointPublisher, StateObjectWrite},
    state_store::RawObjectStateStore,
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

#[test]
fn authoritative_capabilities_reject_missing_namespace() {
    let mut profiles = all_namespace_profiles();
    profiles.remove(&AuthoritativeNamespace::RelationCatalog);
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let error = capabilities.validate_for_startup().unwrap_err();

    assert!(matches!(
        error,
        AuthoritativeObjectStoreCapabilityError::MissingNamespace {
            namespace: AuthoritativeNamespace::RelationCatalog
        }
    ));
}

#[test]
fn authoritative_capabilities_report_weak_namespace_profile() {
    let mut profiles = all_namespace_profiles();
    let profile = profile_missing(RequiredObjectStoreCapability::ConditionalCreate);
    profiles.insert(AuthoritativeNamespace::Output, profile.clone());
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let error = capabilities.validate_for_startup().unwrap_err();

    match error {
        AuthoritativeObjectStoreCapabilityError::NamespaceProfile { namespace, source } => {
            assert_eq!(namespace, AuthoritativeNamespace::Output);
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
    profiles.remove(&AuthoritativeNamespace::BenchmarkEvidence);
    let capabilities = AuthoritativeObjectStoreCapabilitiesV1::new(profiles);

    let diagnostics = capabilities.diagnostics();

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

    let benchmark = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.namespace == AuthoritativeNamespace::BenchmarkEvidence)
        .unwrap();
    assert_eq!(benchmark.backend_name, None);
    assert_eq!(
        benchmark.missing_capabilities,
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
