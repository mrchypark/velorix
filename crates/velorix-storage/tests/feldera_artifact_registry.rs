use std::{path::PathBuf, sync::Arc};

use object_store::{local::LocalFileSystem, ObjectStore};
use tempfile::TempDir;
use velorix_core::feldera_artifact::{
    feldera_spec_hash, FelderaCompileArtifactMetadata, StandingViewSpec,
};
use velorix_storage::{
    capability::{ObjectStoreCapabilityProfile, RequiredObjectStoreCapability},
    feldera_artifact_registry::{
        FelderaArtifactRegistry, FelderaArtifactRegistryError, RegisterFelderaArtifactOutcome,
    },
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("velorix-core")
        .join("tests")
        .join("fixtures")
        .join("feldera")
        .join(format!("{name}.json"))
}

fn load_spec(name: &str) -> StandingViewSpec {
    serde_json::from_str(&std::fs::read_to_string(fixture_path(name)).unwrap()).unwrap()
}

fn load_artifact(name: &str) -> FelderaCompileArtifactMetadata {
    serde_json::from_str(&std::fs::read_to_string(fixture_path(name)).unwrap()).unwrap()
}

fn weak_profile() -> ObjectStoreCapabilityProfile {
    ObjectStoreCapabilityProfile {
        backend_name: "weak-artifact-store".to_string(),
        conditional_create: false,
        atomic_visibility: true,
        list_after_write: true,
        read_after_write: true,
    }
}

#[tokio::test]
async fn feldera_artifact_registry_creates_and_reads_valid_artifact_record() {
    let (_temp_dir, store) = temp_store();
    let registry = FelderaArtifactRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_valid");

    let outcome = registry.register(&spec, &artifact).await.unwrap();
    let read_back = registry
        .read(&artifact.artifact_id, &artifact.artifact_hash)
        .await
        .unwrap();

    assert_eq!(outcome, RegisterFelderaArtifactOutcome::Created);
    assert_eq!(read_back, artifact);
}

#[tokio::test]
async fn feldera_artifact_registry_treats_duplicate_same_record_as_idempotent() {
    let (_temp_dir, store) = temp_store();
    let registry = FelderaArtifactRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_valid");

    registry.register(&spec, &artifact).await.unwrap();
    let duplicate = registry.register(&spec, &artifact).await.unwrap();

    assert_eq!(duplicate, RegisterFelderaArtifactOutcome::Duplicate);
}

#[tokio::test]
async fn feldera_artifact_registry_allows_same_artifact_id_with_different_hash_identity() {
    let (_temp_dir, store) = temp_store();
    let registry = FelderaArtifactRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_valid");
    let mut conflicting = artifact.clone();
    conflicting.artifact_hash = format!("sha256:{}", "1".repeat(64));

    registry.register(&spec, &artifact).await.unwrap();
    let outcome = registry.register(&spec, &conflicting).await.unwrap();

    assert_eq!(outcome, RegisterFelderaArtifactOutcome::Created);
}

#[tokio::test]
async fn feldera_artifact_registry_rejects_same_key_with_different_spec() {
    let (_temp_dir, store) = temp_store();
    let registry = FelderaArtifactRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");
    let artifact = load_artifact("compile_artifact_valid");
    let mut changed_spec = spec.clone();
    changed_spec.sql = "select region from orders".to_string();

    registry.register(&spec, &artifact).await.unwrap();
    let error = registry
        .register(&changed_spec, &artifact)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactRegistryError::Validation(
            velorix_core::feldera_artifact::FelderaArtifactError::MismatchedSpecHash { .. }
        )
    ));
}

#[tokio::test]
async fn feldera_artifact_registry_rejects_unknown_or_malformed_stored_fields_on_read() {
    let (_temp_dir, store) = temp_store();
    let registry = FelderaArtifactRegistry::new(Arc::clone(&store));
    let artifact = load_artifact("compile_artifact_valid");
    let path = registry
        .object_key(&artifact.artifact_id, &artifact.artifact_hash)
        .unwrap();

    object_store::ObjectStore::put(
        &*store,
        &object_store::path::Path::from(path.as_str()),
        br#"{"metadata_version":1,"unexpected":true}"#.as_slice().into(),
    )
    .await
    .unwrap();

    let error = registry
        .read(&artifact.artifact_id, &artifact.artifact_hash)
        .await
        .unwrap_err();

    assert!(matches!(error, FelderaArtifactRegistryError::Serde(_)));
}

#[tokio::test]
async fn feldera_artifact_registry_rejects_stored_body_identity_mismatch_on_read() {
    let (_temp_dir, store) = temp_store();
    let registry = FelderaArtifactRegistry::new(Arc::clone(&store));
    let artifact = load_artifact("compile_artifact_valid");
    let mut wrong_body = artifact.clone();
    wrong_body.artifact_hash = format!("sha256:{}", "1".repeat(64));
    let path = registry
        .object_key(&artifact.artifact_id, &artifact.artifact_hash)
        .unwrap();

    object_store::ObjectStore::put(
        &*store,
        &object_store::path::Path::from(path.as_str()),
        serde_json::to_vec(&wrong_body).unwrap().into(),
    )
    .await
    .unwrap();

    let error = registry
        .read(&artifact.artifact_id, &artifact.artifact_hash)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactRegistryError::RecordIdentityMismatch { .. }
    ));
}

#[tokio::test]
async fn feldera_artifact_registry_validation_rejects_mismatched_spec_fingerprint() {
    let (_temp_dir, store) = temp_store();
    let registry = FelderaArtifactRegistry::new(store);
    let mut spec = load_spec("standing_view_spec_valid");
    let mut artifact = load_artifact("compile_artifact_valid");
    spec.input_relations[0].schema_fingerprint = format!("sha256:{}", "2".repeat(64));
    artifact.spec_hash = feldera_spec_hash(&spec).unwrap();

    let error = registry.register(&spec, &artifact).await.unwrap_err();

    assert!(matches!(
        error,
        FelderaArtifactRegistryError::Validation(
            velorix_core::feldera_artifact::FelderaArtifactError::SchemaFingerprintMismatch {
                field: "input_schemas"
            }
        )
    ));
}

#[test]
fn feldera_artifact_registry_checked_constructor_requires_durable_store_capabilities() {
    let (_temp_dir, store) = temp_store();

    let error = FelderaArtifactRegistry::new_checked(store, &weak_profile()).unwrap_err();

    assert_eq!(
        error.required_capability(),
        RequiredObjectStoreCapability::ConditionalCreate
    );
}
