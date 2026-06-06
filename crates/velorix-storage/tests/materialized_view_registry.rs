use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    memory::InMemory, path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use tempfile::TempDir;
use tokio::sync::Barrier;
use velorix_core::{
    feldera_artifact::{feldera_spec_hash, StandingViewSpec},
    standing_program::{FelderaRuntimePackageIdentity, NativeCodePolicy, StandingProgramIdentity},
};
use velorix_storage::{
    capability::{ObjectStoreCapabilityProfile, RequiredObjectStoreCapability},
    materialized_view_registry::{
        ActivateMaterializedViewOutcome, InvalidExecutionModeReason, MaterializedViewApiMetadata,
        MaterializedViewArtifactBinding, MaterializedViewCompileStatus,
        MaterializedViewDeploymentStatus, MaterializedViewExecutionMode,
        MaterializedViewLifecycleStatus, MaterializedViewRegistry, MaterializedViewRegistryError,
        MaterializedViewRequestFieldSpec, MaterializedViewResponseColumnSpec,
        MaterializedViewResponseSchema, RegisterMaterializedViewOutcome,
    },
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = InMemory::new();

    (temp_dir, Arc::new(store))
}

#[derive(Debug)]
struct BarrierOnActiveUpdateStore {
    inner: Arc<dyn ObjectStore>,
    barrier: Arc<Barrier>,
}

impl std::fmt::Display for BarrierOnActiveUpdateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BarrierOnActiveUpdateStore")
    }
}

#[async_trait]
impl ObjectStore for BarrierOnActiveUpdateStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if location.as_ref().ends_with("/active.json") && matches!(opts.mode, PutMode::Update(_)) {
            self.barrier.wait().await;
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

fn weak_profile() -> ObjectStoreCapabilityProfile {
    ObjectStoreCapabilityProfile {
        backend_name: "weak-materialized-view-store".to_string(),
        conditional_create: false,
        conditional_update: true,
        atomic_visibility: true,
        list_after_write: true,
        read_after_write: true,
    }
}

fn artifact_binding(spec: &StandingViewSpec) -> MaterializedViewArtifactBinding {
    MaterializedViewArtifactBinding {
        artifact_id: "feldera-artifact-orders-by-region-20260503".to_string(),
        artifact_hash: format!("sha256:{}", "0".repeat(64)),
        generated_rust_crate_name: "orders_by_region_pipeline".to_string(),
        state_codec: "feldera-dbsp-state-v1".to_string(),
        state_schema_version: 1,
        execution_status: "direct_execution_enabled".to_string(),
        execution_path: "static_release_artifact".to_string(),
        standing_program_identity: Some(standing_program_identity(&spec.view_id)),
    }
}

fn standing_program_identity(view_id: &str) -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "default".to_string(),
        program_id: view_id.to_string(),
        view_ids: vec![view_id.to_string()],
        sql_hash: format!("sha256:{}", "1".repeat(64)),
        input_catalog_hash: format!("sha256:{}", "2".repeat(64)),
        output_schema_hash: format!("sha256:{}", "3".repeat(64)),
        compiler_identity: "feldera-sql-compiler:test".to_string(),
        runtime_packages: vec![FelderaRuntimePackageIdentity {
            name: "orders_by_region_pipeline".to_string(),
            version: "feldera-generated-rust-abi-v1".to_string(),
        }],
        package_feature_set: vec!["static_release_artifact".to_string()],
        dbsp_runtime_compatibility: "feldera-generated-rust-abi-v1".to_string(),
        checkpoint_codec_identity: "feldera-dbsp-state-v1".to_string(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
    }
}

async fn write_active_record_json(
    store: &Arc<dyn ObjectStore>,
    view_id: &str,
    value: serde_json::Value,
) {
    object_store::ObjectStore::put(
        &**store,
        &Path::from(format!("v1/views/{view_id}/active.json")),
        serde_json::to_vec(&value).unwrap().into(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn materialized_view_registry_creates_and_reads_view_definition() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");
    let spec_hash = feldera_spec_hash(&spec).unwrap();

    let outcome = registry.register(&spec).await.unwrap();
    let read_back = registry.read(&spec.view_id, &spec_hash).await.unwrap();

    assert_eq!(outcome, RegisterMaterializedViewOutcome::Created);
    assert_eq!(read_back, spec);
}

#[tokio::test]
async fn materialized_view_registry_treats_duplicate_same_definition_as_idempotent() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");

    registry.register(&spec).await.unwrap();
    let duplicate = registry.register(&spec).await.unwrap();

    assert_eq!(duplicate, RegisterMaterializedViewOutcome::Duplicate);
}

#[tokio::test]
async fn materialized_view_registry_reads_active_definition_by_view_id() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");
    let spec_hash = feldera_spec_hash(&spec).unwrap();

    registry.register(&spec).await.unwrap();
    let active = registry.read_active(&spec.view_id).await.unwrap();

    assert_eq!(active.spec_hash, spec_hash);
    assert_eq!(active.spec, spec);
    assert_eq!(
        active.execution_mode,
        MaterializedViewExecutionMode::LegacyRecoveredSql
    );
    assert_eq!(
        active.lifecycle,
        MaterializedViewLifecycleStatus::legacy_recovered_sql()
    );
}

#[tokio::test]
async fn materialized_view_registry_lists_active_definitions() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");
    let spec_hash = feldera_spec_hash(&spec).unwrap();

    registry.register(&spec).await.unwrap();
    let active = registry.list_active().await.unwrap();

    assert_eq!(active.len(), 1);
    assert_eq!(active[0].spec_hash, spec_hash);
    assert_eq!(active[0].spec, spec);
}

#[tokio::test]
async fn materialized_view_registry_preserves_active_view_response_schema() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");
    let api = MaterializedViewApiMetadata {
        response_schema: Some(MaterializedViewResponseSchema {
            columns: vec![MaterializedViewResponseColumnSpec {
                name: "account_id".to_string(),
                r#type: "string".to_string(),
                source: "key_json".to_string(),
                description: Some("Account id".to_string()),
            }],
        }),
        ..MaterializedViewApiMetadata::default()
    };

    registry
        .register_with_api_metadata(&spec, Some(api.clone()))
        .await
        .unwrap();
    let active = registry.read_active(&spec.view_id).await.unwrap();

    assert_eq!(active.api.unwrap(), api);
}

#[tokio::test]
async fn materialized_view_registry_preserves_active_generated_artifact_binding() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");
    let artifact = artifact_binding(&spec);

    registry
        .register_with_api_metadata_and_artifact(&spec, None, Some(artifact.clone()))
        .await
        .unwrap();
    let active = registry.read_active(&spec.view_id).await.unwrap();

    assert_eq!(active.artifact, Some(artifact));
    assert_eq!(
        active.execution_mode,
        MaterializedViewExecutionMode::StandingRuntime
    );
    assert_eq!(
        active.lifecycle,
        MaterializedViewLifecycleStatus::standing_runtime()
    );
}

#[tokio::test]
async fn materialized_view_registry_preserves_feldera_compile_pending_lifecycle() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");
    let lifecycle = MaterializedViewLifecycleStatus::feldera_compile_pending(Some(
        "compile worker not configured".to_string(),
    ));

    registry
        .register_with_api_metadata_artifact_execution(
            &spec,
            None,
            None,
            Some(MaterializedViewExecutionMode::FelderaCompilePending),
            Some(lifecycle.clone()),
        )
        .await
        .unwrap();
    let active = registry.read_active(&spec.view_id).await.unwrap();

    assert_eq!(
        active.execution_mode,
        MaterializedViewExecutionMode::FelderaCompilePending
    );
    assert_eq!(active.lifecycle, lifecycle);
    assert_eq!(
        active.lifecycle.compile_status,
        MaterializedViewCompileStatus::Pending
    );
    assert_eq!(
        active.lifecycle.deployment_status,
        MaterializedViewDeploymentStatus::NotDeployed
    );
}

#[tokio::test]
async fn materialized_view_registry_activates_pending_view_with_artifact() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");
    let spec_hash = feldera_spec_hash(&spec).unwrap();
    let pending_lifecycle = MaterializedViewLifecycleStatus::feldera_compile_pending(Some(
        "compile worker not configured".to_string(),
    ));
    let artifact = artifact_binding(&spec);

    registry
        .register_with_api_metadata_artifact_execution(
            &spec,
            None,
            None,
            Some(MaterializedViewExecutionMode::FelderaCompilePending),
            Some(pending_lifecycle),
        )
        .await
        .unwrap();
    let outcome = registry
        .activate_pending_with_artifact(
            &spec.view_id,
            &spec_hash,
            artifact.clone(),
            MaterializedViewLifecycleStatus::standing_runtime(),
        )
        .await
        .unwrap();

    assert_eq!(outcome, ActivateMaterializedViewOutcome::Activated);
    let active = registry.read_active(&spec.view_id).await.unwrap();
    assert_eq!(
        active.execution_mode,
        MaterializedViewExecutionMode::StandingRuntime
    );
    assert_eq!(active.artifact, Some(artifact));
    assert_eq!(
        active.lifecycle,
        MaterializedViewLifecycleStatus::standing_runtime()
    );
}

#[tokio::test]
async fn materialized_view_registry_rejects_racing_pending_activation_with_conditional_update() {
    let (_temp_dir, inner_store) = temp_store();
    let store = Arc::new(BarrierOnActiveUpdateStore {
        inner: Arc::clone(&inner_store),
        barrier: Arc::new(Barrier::new(2)),
    });
    let registry = MaterializedViewRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");
    let spec_hash = feldera_spec_hash(&spec).unwrap();
    let pending_lifecycle = MaterializedViewLifecycleStatus::feldera_compile_pending(Some(
        "compile worker not configured".to_string(),
    ));
    let mut left_artifact = artifact_binding(&spec);
    left_artifact.artifact_id = "left-artifact".to_string();
    let mut right_artifact = artifact_binding(&spec);
    right_artifact.artifact_id = "right-artifact".to_string();

    registry
        .register_with_api_metadata_artifact_execution(
            &spec,
            None,
            None,
            Some(MaterializedViewExecutionMode::FelderaCompilePending),
            Some(pending_lifecycle),
        )
        .await
        .unwrap();

    let left_registry = registry.clone();
    let left_view_id = spec.view_id.clone();
    let left_spec_hash = spec_hash.clone();
    let left = tokio::spawn(async move {
        left_registry
            .activate_pending_with_artifact(
                &left_view_id,
                &left_spec_hash,
                left_artifact,
                MaterializedViewLifecycleStatus::standing_runtime(),
            )
            .await
    });
    let right_registry = registry.clone();
    let right_view_id = spec.view_id.clone();
    let right_spec_hash = spec_hash.clone();
    let right = tokio::spawn(async move {
        right_registry
            .activate_pending_with_artifact(
                &right_view_id,
                &right_spec_hash,
                right_artifact,
                MaterializedViewLifecycleStatus::standing_runtime(),
            )
            .await
    });
    let (left, right) = tokio::join!(left, right);
    let outcomes = [left.unwrap(), right.unwrap()];

    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(ActivateMaterializedViewOutcome::Activated)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(
                outcome,
                Err(MaterializedViewRegistryError::ActiveRecordConflict { .. })
            ))
            .count(),
        1
    );
    let active = registry.read_active(&spec.view_id).await.unwrap();
    let artifact_id = active.artifact.unwrap().artifact_id;
    assert!(artifact_id == "left-artifact" || artifact_id == "right-artifact");
}

#[tokio::test]
async fn materialized_view_registry_derives_execution_mode_for_old_active_records() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let spec = load_spec("standing_view_spec_valid");
    let spec_hash = feldera_spec_hash(&spec).unwrap();

    registry.register(&spec).await.unwrap();
    write_active_record_json(
        &store,
        &spec.view_id,
        serde_json::json!({
            "schema_version": 1,
            "view_id": spec.view_id,
            "spec_hash": spec_hash,
            "artifact": artifact_binding(&spec)
        }),
    )
    .await;

    let active = registry.read_active(&spec.view_id).await.unwrap();

    assert_eq!(
        active.execution_mode,
        MaterializedViewExecutionMode::StandingRuntime
    );

    let mut legacy_spec = spec.clone();
    legacy_spec.view_id = "orders_by_region_legacy_sql".to_string();
    let legacy_spec_hash = feldera_spec_hash(&legacy_spec).unwrap();
    registry.register(&legacy_spec).await.unwrap();
    write_active_record_json(
        &store,
        &legacy_spec.view_id,
        serde_json::json!({
            "schema_version": 1,
            "view_id": legacy_spec.view_id,
            "spec_hash": legacy_spec_hash
        }),
    )
    .await;

    let legacy_active = registry.read_active(&legacy_spec.view_id).await.unwrap();

    assert_eq!(
        legacy_active.execution_mode,
        MaterializedViewExecutionMode::LegacyRecoveredSql
    );
}

#[tokio::test]
async fn materialized_view_registry_rejects_current_schema_active_record_without_execution_mode() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let spec = load_spec("standing_view_spec_valid");
    let spec_hash = feldera_spec_hash(&spec).unwrap();

    registry.register(&spec).await.unwrap();
    write_active_record_json(
        &store,
        &spec.view_id,
        serde_json::json!({
            "schema_version": 2,
            "view_id": spec.view_id,
            "spec_hash": spec_hash
        }),
    )
    .await;

    let error = registry.read_active(&spec.view_id).await.unwrap_err();

    assert!(matches!(
        error,
        MaterializedViewRegistryError::InvalidExecutionMode {
            reason: InvalidExecutionModeReason::MissingExecutionModeForCurrentSchema {
                schema_version: 2
            },
            ..
        }
    ));
}

#[tokio::test]
async fn materialized_view_registry_rejects_standing_runtime_mode_without_artifact() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let spec = load_spec("standing_view_spec_valid");
    let spec_hash = feldera_spec_hash(&spec).unwrap();

    registry.register(&spec).await.unwrap();
    write_active_record_json(
        &store,
        &spec.view_id,
        serde_json::json!({
            "schema_version": 1,
            "view_id": spec.view_id,
            "spec_hash": spec_hash,
            "execution_mode": "standing_runtime"
        }),
    )
    .await;

    let error = registry.read_active(&spec.view_id).await.unwrap_err();

    assert!(matches!(
        error,
        MaterializedViewRegistryError::InvalidExecutionMode { .. }
    ));
}

#[tokio::test]
async fn materialized_view_registry_rejects_legacy_mode_with_artifact() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let spec = load_spec("standing_view_spec_valid");
    let spec_hash = feldera_spec_hash(&spec).unwrap();

    registry.register(&spec).await.unwrap();
    write_active_record_json(
        &store,
        &spec.view_id,
        serde_json::json!({
            "schema_version": 1,
            "view_id": spec.view_id,
            "spec_hash": spec_hash,
            "execution_mode": "legacy_recovered_sql",
            "artifact": artifact_binding(&spec)
        }),
    )
    .await;

    let error = registry.read_active(&spec.view_id).await.unwrap_err();

    assert!(matches!(
        error,
        MaterializedViewRegistryError::InvalidExecutionMode { .. }
    ));
}

#[tokio::test]
async fn materialized_view_registry_rejects_feldera_compile_pending_mode_with_artifact() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let spec = load_spec("standing_view_spec_valid");
    let spec_hash = feldera_spec_hash(&spec).unwrap();

    registry.register(&spec).await.unwrap();
    write_active_record_json(
        &store,
        &spec.view_id,
        serde_json::json!({
            "schema_version": 3,
            "view_id": spec.view_id,
            "spec_hash": spec_hash,
            "execution_mode": "feldera_compile_pending",
            "artifact": artifact_binding(&spec)
        }),
    )
    .await;

    let error = registry.read_active(&spec.view_id).await.unwrap_err();

    assert!(matches!(
        error,
        MaterializedViewRegistryError::InvalidExecutionMode {
            reason: InvalidExecutionModeReason::FelderaCompilePendingWithArtifact,
            ..
        }
    ));
}

#[tokio::test]
async fn materialized_view_registry_indexes_active_view_api_path() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = load_spec("standing_view_spec_valid");
    let api = MaterializedViewApiMetadata {
        url_path: Some("/orders/:account_id".to_string()),
        request: vec![MaterializedViewRequestFieldSpec {
            field_name: "account_id".to_string(),
            field_in: "path".to_string(),
            r#type: "string".to_string(),
            default_value: None,
            description: None,
            validators: vec!["required".to_string()],
        }],
        ..MaterializedViewApiMetadata::default()
    };

    registry
        .register_with_api_metadata(&spec, Some(api.clone()))
        .await
        .unwrap();
    let index = registry
        .list_api_path_indexes()
        .await
        .unwrap()
        .into_iter()
        .find(|index| index.normalized_url_path == "orders/:account_id")
        .unwrap();

    assert_eq!(index.view_id, spec.view_id);
}

#[tokio::test]
async fn materialized_view_registry_rejects_duplicate_active_view_api_path() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let first = load_spec("standing_view_spec_valid");
    let mut second = first.clone();
    second.view_id = "orders_by_account_copy".to_string();
    let api = MaterializedViewApiMetadata {
        url_path: Some("/orders/:account_id".to_string()),
        ..MaterializedViewApiMetadata::default()
    };

    registry
        .register_with_api_metadata(&first, Some(api.clone()))
        .await
        .unwrap();
    let error = registry
        .register_with_api_metadata(&second, Some(api))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        MaterializedViewRegistryError::ApiPathConflict { .. }
    ));
}

#[tokio::test]
async fn materialized_view_registry_rejects_same_key_with_different_body() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let spec = load_spec("standing_view_spec_valid");
    let mut wrong_body = spec.clone();
    wrong_body.sql = "select region from orders".to_string();
    let spec_hash = feldera_spec_hash(&spec).unwrap();
    let path = registry.object_key(&spec.view_id, &spec_hash).unwrap();

    object_store::ObjectStore::put(
        &*store,
        &object_store::path::Path::from(path.as_str()),
        serde_json::to_vec(&wrong_body).unwrap().into(),
    )
    .await
    .unwrap();

    let error = registry.register(&spec).await.unwrap_err();

    assert!(matches!(
        error,
        MaterializedViewRegistryError::RecordConflict { .. }
    ));
}

#[tokio::test]
async fn materialized_view_registry_rejects_non_materialized_view_definitions() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let mut spec = load_spec("standing_view_spec_valid");
    spec.shape.is_materialized = false;

    let error = registry.register(&spec).await.unwrap_err();

    assert!(matches!(
        error,
        MaterializedViewRegistryError::Validation(
            velorix_core::feldera_artifact::FelderaArtifactError::UnsupportedShape {
                shape: "spec.shape.is_materialized"
            }
        )
    ));
}

#[test]
fn materialized_view_registry_checked_constructor_requires_durable_store_capabilities() {
    let (_temp_dir, store) = temp_store();

    let error = MaterializedViewRegistry::new_checked(store, &weak_profile()).unwrap_err();

    assert_eq!(
        error.required_capability(),
        RequiredObjectStoreCapability::ConditionalCreate
    );
}

#[test]
fn materialized_view_registry_product_profile_requires_conditional_update() {
    let profile = ObjectStoreCapabilityProfile {
        backend_name: "no-active-view-cas".to_string(),
        conditional_create: true,
        conditional_update: false,
        atomic_visibility: true,
        list_after_write: true,
        read_after_write: true,
    };

    let error = profile.validate_for_conditional_update().unwrap_err();

    assert_eq!(
        error.required_capability(),
        RequiredObjectStoreCapability::ConditionalUpdate
    );
}

#[tokio::test]
async fn materialized_view_registry_rejects_activation_without_conditional_update_capability() {
    let (_temp_dir, store) = temp_store();
    let profile = ObjectStoreCapabilityProfile {
        backend_name: "no-active-view-cas".to_string(),
        conditional_create: true,
        conditional_update: false,
        atomic_visibility: true,
        list_after_write: true,
        read_after_write: true,
    };
    let registry = MaterializedViewRegistry::new_checked(store, &profile).unwrap();
    let spec = load_spec("standing_view_spec_valid");
    let artifact = artifact_binding(&spec);

    registry
        .register_with_api_metadata_artifact_execution(
            &spec,
            None,
            None,
            Some(MaterializedViewExecutionMode::FelderaCompilePending),
            Some(MaterializedViewLifecycleStatus::feldera_compile_pending(
                None,
            )),
        )
        .await
        .unwrap();
    let error = registry
        .activate_pending_with_artifact(
            &spec.view_id,
            &feldera_spec_hash(&spec).unwrap(),
            artifact,
            MaterializedViewLifecycleStatus::standing_runtime_deploying(None),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        MaterializedViewRegistryError::ActiveRecordConditionalUpdateUnsupported { .. }
    ));
}
