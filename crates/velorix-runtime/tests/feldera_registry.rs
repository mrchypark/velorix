use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use object_store::{local::LocalFileSystem, ObjectStore};
use tempfile::TempDir;
use velorix_core::{
    feldera_artifact::{
        feldera_artifact_bytes_hash, feldera_spec_hash, FelderaArtifactError,
        FelderaCompileArtifactMetadata, FelderaReleaseArtifactProvenanceV1, RelationSchema,
        StandingViewSpec,
    },
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1, RELATION_SCHEMA_VERSION_V1,
    },
};
use velorix_runtime::feldera_registry::{
    GeneratedRustArtifactPackage, RuntimeFelderaArtifactError, RuntimeFelderaArtifactRegistry,
    RuntimeFelderaArtifactSelectionStatus,
};
use velorix_storage::capability::{
    AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
    AuthoritativeObjectStoreCapabilityError, ObjectStoreCapabilityProfile,
    RequiredObjectStoreCapability,
};
use velorix_storage::feldera_artifact_registry::{
    FelderaArtifactRegistryError, RegisterFelderaArtifactOutcome,
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

fn all_namespace_capabilities() -> AuthoritativeObjectStoreCapabilitiesV1 {
    AuthoritativeObjectStoreCapabilitiesV1::new(
        AuthoritativeNamespace::all()
            .into_iter()
            .map(|namespace| (namespace, ObjectStoreCapabilityProfile::local_development()))
            .collect::<BTreeMap<_, _>>(),
    )
}

fn capabilities_missing(
    namespace: AuthoritativeNamespace,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    let mut profiles = all_namespace_capabilities().profiles;
    profiles.remove(&namespace);
    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}

fn capabilities_with_weak_namespace(
    namespace: AuthoritativeNamespace,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    let mut profiles = all_namespace_capabilities().profiles;
    profiles.insert(
        namespace,
        ObjectStoreCapabilityProfile {
            backend_name: "weak-artifact-catalog".to_string(),
            conditional_create: false,
            conditional_update: true,
            atomic_visibility: true,
            list_after_write: true,
            read_after_write: true,
        },
    );
    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}

#[test]
fn runtime_feldera_artifact_registry_source_keeps_unchecked_bootstrap_explicit() {
    let source = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("feldera_registry.rs"),
    )
    .unwrap();

    assert!(source.contains("pub fn new_with_startup_capabilities("));
    assert!(source.contains("pub fn for_local_bootstrap_unchecked("));
    assert!(!source.contains("\n    pub fn new("));
    assert!(!source.contains("pub fn new_checked("));
    assert!(!source.contains("pub fn from_storage_registry("));
    assert!(!source.contains("RuntimeFelderaArtifactRegistry::new("));
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

fn load_provenance(name: &str) -> FelderaReleaseArtifactProvenanceV1 {
    serde_json::from_str(&std::fs::read_to_string(fixture_path(name)).unwrap()).unwrap()
}

fn fixture_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "orders".to_string(),
        relation_name: "orders".to_string(),
        relation_version: "2026-05-05.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "order_id".to_string(),
                name: "order_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "region".to_string(),
                name: "region".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "amount".to_string(),
                name: "amount".to_string(),
                logical_type: VelorixLogicalTypeV1::Decimal {
                    precision: 18,
                    scale: 2,
                },
                physical_arrow_type: ArrowPhysicalTypeV1::Decimal128 {
                    precision: 18,
                    scale: 2,
                },
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Value,
            },
        ],
        primary_key_column_ids: vec!["order_id".to_string()],
        weight_column_id: "amount".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "orders".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "orders".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: "incremental-adapter-orders-v1".to_string(),
        },
    }
}

fn catalog_input_schema(catalog: &VelorixRelationCatalogV1) -> RelationSchema {
    RelationSchema {
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_name: catalog.relation_schema.relation_name.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        schema_fingerprint: catalog.schema_fingerprint.to_string(),
        columns: load_spec("standing_view_spec_valid").input_relations[0]
            .columns
            .clone(),
        primary_key: vec!["order_id".to_string()],
    }
}

fn catalog_valid_fixture_parts() -> (
    VelorixRelationCatalogV1,
    StandingViewSpec,
    FelderaCompileArtifactMetadata,
) {
    let catalog = fixture_catalog();
    let mut spec = load_spec("standing_view_spec_valid");
    spec.input_relations = vec![catalog_input_schema(&catalog)];

    let mut artifact = load_artifact("compile_artifact_valid");
    artifact.input_schemas = spec.input_relations.clone();
    artifact.spec_hash = feldera_spec_hash(&spec).unwrap();

    (catalog, spec, artifact)
}

fn release_provenance_for_artifact(
    artifact: &FelderaCompileArtifactMetadata,
) -> FelderaReleaseArtifactProvenanceV1 {
    let mut provenance = load_provenance("release_provenance_valid");
    provenance.build.artifact_id = artifact.artifact_id.clone();
    provenance.build.artifact_hash = artifact.artifact_hash.clone();
    provenance.build.spec_hash = artifact.spec_hash.clone();
    provenance.build.generated_rust = artifact.generated_rust.clone();
    provenance.provenance.compiler_name = artifact.compiler.name.clone();
    provenance.provenance.compiler_version = artifact.compiler.version.clone();

    provenance
}

#[tokio::test]
async fn feldera_runtime_registry_selects_valid_registered_artifact_for_catalog() {
    let (_temp_dir, store) = temp_store();
    let registry = RuntimeFelderaArtifactRegistry::for_local_bootstrap_unchecked(store);
    let (catalog, spec, artifact) = catalog_valid_fixture_parts();

    let registered = registry
        .register_trusted_artifact(&catalog, &spec, &artifact)
        .await
        .unwrap();
    let selected = registry
        .select_trusted_artifact(
            &catalog,
            &spec,
            &artifact.artifact_id,
            &artifact.artifact_hash,
        )
        .await
        .unwrap();

    assert_eq!(
        registered.register_outcome,
        RegisterFelderaArtifactOutcome::Created
    );
    assert_eq!(selected.metadata, artifact);
    assert_eq!(
        selected.status,
        RuntimeFelderaArtifactSelectionStatus::DirectExecutionDisabled
    );
}

#[tokio::test]
async fn feldera_runtime_registry_selects_executable_artifact_when_package_is_registered() {
    let (_temp_dir, store) = temp_store();
    let (catalog, spec, artifact) = catalog_valid_fixture_parts();
    let registry = RuntimeFelderaArtifactRegistry::for_local_bootstrap_with_generated_packages(
        store,
        [GeneratedRustArtifactPackage {
            abi_version: artifact.generated_rust.abi_version.clone(),
            crate_name: artifact.generated_rust.crate_name.clone(),
        }],
    );

    let registered = registry
        .register_trusted_artifact(&catalog, &spec, &artifact)
        .await
        .unwrap();
    let selected = registry
        .select_trusted_artifact(
            &catalog,
            &spec,
            &artifact.artifact_id,
            &artifact.artifact_hash,
        )
        .await
        .unwrap();

    assert_eq!(
        registered.status,
        RuntimeFelderaArtifactSelectionStatus::DirectExecutionEnabled {
            package: GeneratedRustArtifactPackage {
                abi_version: artifact.generated_rust.abi_version.clone(),
                crate_name: artifact.generated_rust.crate_name.clone(),
            }
        }
    );
    assert_eq!(selected.status, registered.status);
}

#[tokio::test]
async fn feldera_runtime_registry_accepts_startup_capabilities_for_artifact_catalog() {
    let (_temp_dir, store) = temp_store();
    let registry = RuntimeFelderaArtifactRegistry::new_with_startup_capabilities(
        store,
        &all_namespace_capabilities(),
    )
    .unwrap();
    let (catalog, spec, artifact) = catalog_valid_fixture_parts();

    let registered = registry
        .register_trusted_artifact(&catalog, &spec, &artifact)
        .await
        .unwrap();

    assert_eq!(
        registered.register_outcome,
        RegisterFelderaArtifactOutcome::Created
    );
    assert_eq!(
        registered.status,
        RuntimeFelderaArtifactSelectionStatus::DirectExecutionDisabled
    );
}

#[test]
fn feldera_runtime_registry_rejects_missing_artifact_catalog_startup_capability() {
    let (_temp_dir, store) = temp_store();
    let capabilities = capabilities_missing(AuthoritativeNamespace::ArtifactCatalog);

    let error = RuntimeFelderaArtifactRegistry::new_with_startup_capabilities(store, &capabilities)
        .unwrap_err();

    assert!(matches!(
        error,
        AuthoritativeObjectStoreCapabilityError::MissingNamespace {
            namespace: AuthoritativeNamespace::ArtifactCatalog
        }
    ));
}

#[test]
fn feldera_runtime_registry_rejects_weak_artifact_catalog_startup_capability() {
    let (_temp_dir, store) = temp_store();
    let capabilities = capabilities_with_weak_namespace(AuthoritativeNamespace::ArtifactCatalog);

    let error = RuntimeFelderaArtifactRegistry::new_with_startup_capabilities(store, &capabilities)
        .unwrap_err();

    assert!(matches!(
        error,
        AuthoritativeObjectStoreCapabilityError::NamespaceProfile {
            namespace: AuthoritativeNamespace::ArtifactCatalog,
            source
        } if source.required_capability() == RequiredObjectStoreCapability::ConditionalCreate
    ));
}

#[tokio::test]
async fn feldera_runtime_registry_release_provenance_verified_path_keeps_execution_disabled() {
    let (_temp_dir, store) = temp_store();
    let registry = RuntimeFelderaArtifactRegistry::for_local_bootstrap_unchecked(store);
    let (catalog, spec, mut artifact) = catalog_valid_fixture_parts();
    let artifact_bytes = b"compiled release artifact bytes";
    artifact.artifact_hash = feldera_artifact_bytes_hash(artifact_bytes);
    let provenance = release_provenance_for_artifact(&artifact);

    let registered = registry
        .register_release_provenance_verified_artifact(
            &catalog,
            &spec,
            &artifact,
            artifact_bytes,
            &provenance,
        )
        .await
        .unwrap();
    let selected = registry
        .select_release_provenance_verified_artifact(
            &catalog,
            &spec,
            &artifact.artifact_id,
            &artifact.artifact_hash,
            &provenance,
        )
        .await
        .unwrap();

    assert_eq!(
        registered.register_outcome,
        RegisterFelderaArtifactOutcome::Created
    );
    assert_eq!(selected.metadata, artifact);
    assert_eq!(
        registered.status,
        RuntimeFelderaArtifactSelectionStatus::DirectExecutionDisabled
    );
    assert_eq!(
        selected.status,
        RuntimeFelderaArtifactSelectionStatus::DirectExecutionDisabled
    );
}

#[tokio::test]
async fn feldera_runtime_registry_release_provenance_mismatch_does_not_persist() {
    let (_temp_dir, store) = temp_store();
    let registry = RuntimeFelderaArtifactRegistry::for_local_bootstrap_unchecked(store);
    let (catalog, spec, mut artifact) = catalog_valid_fixture_parts();
    let artifact_bytes = b"compiled release artifact bytes";
    artifact.artifact_hash = feldera_artifact_bytes_hash(artifact_bytes);
    let mut provenance = release_provenance_for_artifact(&artifact);
    provenance.build.artifact_hash = format!("sha256:{}", "9".repeat(64));

    let error = registry
        .register_release_provenance_verified_artifact(
            &catalog,
            &spec,
            &artifact,
            artifact_bytes,
            &provenance,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RuntimeFelderaArtifactError::Validation(
            FelderaArtifactError::MismatchedReleaseProvenanceField {
                field: "build.artifact_hash"
            }
        )
    ));
    let error = registry
        .select_trusted_artifact(
            &catalog,
            &spec,
            &artifact.artifact_id,
            &artifact.artifact_hash,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RuntimeFelderaArtifactError::Storage(FelderaArtifactRegistryError::ObjectStore(
            object_store::Error::NotFound { .. }
        ))
    ));
}

#[tokio::test]
async fn feldera_runtime_registry_release_provenance_selection_revalidates_stored_identity() {
    let (_temp_dir, store) = temp_store();
    let registry = RuntimeFelderaArtifactRegistry::for_local_bootstrap_unchecked(store);
    let (catalog, spec, mut artifact) = catalog_valid_fixture_parts();
    let artifact_bytes = b"compiled release artifact bytes";
    artifact.artifact_hash = feldera_artifact_bytes_hash(artifact_bytes);
    let provenance = release_provenance_for_artifact(&artifact);

    registry
        .register_hash_verified_artifact(&catalog, &spec, &artifact, artifact_bytes)
        .await
        .unwrap();

    let mut mismatched_provenance = provenance.clone();
    mismatched_provenance.build.artifact_id = "different-artifact".to_string();
    let error = registry
        .select_release_provenance_verified_artifact(
            &catalog,
            &spec,
            &artifact.artifact_id,
            &artifact.artifact_hash,
            &mismatched_provenance,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RuntimeFelderaArtifactError::Validation(
            FelderaArtifactError::MismatchedReleaseProvenanceField {
                field: "build.artifact_id"
            }
        )
    ));

    let mut changed_catalog = catalog;
    changed_catalog.relation_schema.relation_version = "2026-05-05.v2".to_string();
    changed_catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&changed_catalog.relation_schema).unwrap();
    changed_catalog.feldera_relation.schema_fingerprint =
        changed_catalog.schema_fingerprint.clone();
    let mut changed_spec = spec;
    changed_spec.input_relations = vec![catalog_input_schema(&changed_catalog)];

    let error = registry
        .select_release_provenance_verified_artifact(
            &changed_catalog,
            &changed_spec,
            &artifact.artifact_id,
            &artifact.artifact_hash,
            &provenance,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RuntimeFelderaArtifactError::Validation(FelderaArtifactError::MismatchedSpecHash { .. })
    ));
}

#[tokio::test]
async fn feldera_runtime_registry_treats_duplicate_registration_as_idempotent() {
    let (_temp_dir, store) = temp_store();
    let registry = RuntimeFelderaArtifactRegistry::for_local_bootstrap_unchecked(store);
    let (catalog, spec, artifact) = catalog_valid_fixture_parts();

    registry
        .register_trusted_artifact(&catalog, &spec, &artifact)
        .await
        .unwrap();
    let duplicate = registry
        .register_trusted_artifact(&catalog, &spec, &artifact)
        .await
        .unwrap();

    assert_eq!(
        duplicate.register_outcome,
        RegisterFelderaArtifactOutcome::Duplicate
    );
    assert_eq!(duplicate.metadata, artifact);
}

#[tokio::test]
async fn feldera_runtime_registry_rejects_relation_fingerprint_mismatch() {
    let (_temp_dir, store) = temp_store();
    let registry = RuntimeFelderaArtifactRegistry::for_local_bootstrap_unchecked(store);
    let (catalog, mut spec, mut artifact) = catalog_valid_fixture_parts();
    spec.input_relations[0].schema_fingerprint = format!("sha256:{}", "1".repeat(64));
    artifact.input_schemas = spec.input_relations.clone();
    artifact.spec_hash = feldera_spec_hash(&spec).unwrap();

    let error = registry
        .register_trusted_artifact(&catalog, &spec, &artifact)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RuntimeFelderaArtifactError::Validation(FelderaArtifactError::SchemaFingerprintMismatch {
            field: "spec.input_relations"
        })
    ));
}

#[tokio::test]
async fn feldera_runtime_registry_rejects_unknown_generated_rust_abi() {
    let (_temp_dir, store) = temp_store();
    let registry = RuntimeFelderaArtifactRegistry::for_local_bootstrap_unchecked(store);
    let (catalog, spec, mut artifact) = catalog_valid_fixture_parts();
    artifact.generated_rust =
        load_artifact("compile_artifact_unsupported_generated_rust_abi").generated_rust;

    let error = registry
        .register_trusted_artifact(&catalog, &spec, &artifact)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RuntimeFelderaArtifactError::Validation(
            FelderaArtifactError::UnsupportedGeneratedRustAbi { .. }
        )
    ));
}

#[tokio::test]
async fn feldera_runtime_registry_rejects_selection_when_catalog_changes() {
    let (_temp_dir, store) = temp_store();
    let registry = RuntimeFelderaArtifactRegistry::for_local_bootstrap_unchecked(store);
    let (catalog, spec, artifact) = catalog_valid_fixture_parts();

    registry
        .register_trusted_artifact(&catalog, &spec, &artifact)
        .await
        .unwrap();

    let mut changed_catalog = catalog;
    changed_catalog.relation_schema.relation_version = "2026-05-05.v2".to_string();
    changed_catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&changed_catalog.relation_schema).unwrap();
    changed_catalog.feldera_relation.schema_fingerprint =
        changed_catalog.schema_fingerprint.clone();
    let mut changed_spec = spec;
    changed_spec.input_relations = vec![catalog_input_schema(&changed_catalog)];

    let error = registry
        .select_trusted_artifact(
            &changed_catalog,
            &changed_spec,
            &artifact.artifact_id,
            &artifact.artifact_hash,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RuntimeFelderaArtifactError::Validation(FelderaArtifactError::MismatchedSpecHash { .. })
    ));
}

#[tokio::test]
async fn feldera_runtime_registry_keeps_direct_execution_disabled() {
    let (_temp_dir, store) = temp_store();
    let registry = RuntimeFelderaArtifactRegistry::for_local_bootstrap_unchecked(store);
    let (catalog, spec, artifact) = catalog_valid_fixture_parts();

    let selected = registry
        .register_trusted_artifact(&catalog, &spec, &artifact)
        .await
        .unwrap();

    assert_eq!(
        selected.status,
        RuntimeFelderaArtifactSelectionStatus::DirectExecutionDisabled
    );
}

#[tokio::test]
async fn feldera_runtime_registry_admits_hash_verified_artifact_without_direct_execution() {
    let (_temp_dir, store) = temp_store();
    let registry = RuntimeFelderaArtifactRegistry::for_local_bootstrap_unchecked(store);
    let (catalog, spec, mut artifact) = catalog_valid_fixture_parts();
    let artifact_bytes = b"compiled artifact bytes";
    artifact.artifact_hash = feldera_artifact_bytes_hash(artifact_bytes);

    let registered = registry
        .register_hash_verified_artifact(&catalog, &spec, &artifact, artifact_bytes)
        .await
        .unwrap();

    assert_eq!(
        registered.status,
        RuntimeFelderaArtifactSelectionStatus::DirectExecutionDisabled
    );
    assert_eq!(
        registered.register_outcome,
        RegisterFelderaArtifactOutcome::Created
    );
}

#[tokio::test]
async fn feldera_runtime_registry_rejects_hash_mismatch_without_persisting_artifact() {
    let (_temp_dir, store) = temp_store();
    let registry = RuntimeFelderaArtifactRegistry::for_local_bootstrap_unchecked(store);
    let (catalog, spec, mut artifact) = catalog_valid_fixture_parts();
    artifact.artifact_hash = feldera_artifact_bytes_hash(b"expected artifact bytes");

    let error = registry
        .register_hash_verified_artifact(&catalog, &spec, &artifact, b"different artifact bytes")
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RuntimeFelderaArtifactError::Validation(
            FelderaArtifactError::MismatchedArtifactHash { .. }
        )
    ));
    let error = registry
        .select_trusted_artifact(
            &catalog,
            &spec,
            &artifact.artifact_id,
            &artifact.artifact_hash,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RuntimeFelderaArtifactError::Storage(FelderaArtifactRegistryError::ObjectStore(
            object_store::Error::NotFound { .. }
        ))
    ));
}
