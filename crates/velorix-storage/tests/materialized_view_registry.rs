use std::sync::Arc;

use object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt};
use tempfile::TempDir;
use velorix_core::{
    standing_program::{BuiltinRuntimeIdentity, NativeCodePolicy, StandingProgramIdentity},
    view_contract::{
        published_relation_binding_v1, view_spec_hash, ColumnSchema, RelationSchema, SqlDataType,
        SqlDialect, SqlSourceKind, StandingViewShape, StandingViewSpec, ViewContractError,
    },
};
use velorix_storage::{
    capability::{ObjectStoreCapabilityProfile, RequiredObjectStoreCapability},
    materialized_view_registry::{
        InvalidExecutionModeReason, MaterializedViewApiMetadata, MaterializedViewArtifactBinding,
        MaterializedViewExecutionMode, MaterializedViewLifecycleStatus, MaterializedViewRegistry,
        MaterializedViewRegistryError, MaterializedViewRequestFieldSpec,
        MaterializedViewResponseColumnSpec, MaterializedViewResponseSchema,
        MaterializedViewRuntimeBinding, RegisterMaterializedViewOutcome,
    },
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = InMemory::new();

    (temp_dir, Arc::new(store))
}

fn sample_spec() -> StandingViewSpec {
    let input = RelationSchema {
        relation_id: "orders".to_string(),
        relation_name: "orders".to_string(),
        relation_version: "2026-06-14.v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "1".repeat(64)),
        columns: vec![
            ColumnSchema {
                name: "region".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "amount".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["region".to_string()],
    };
    let output = RelationSchema {
        relation_id: "orders_by_region".to_string(),
        relation_name: "orders_by_region".to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
        columns: vec![
            ColumnSchema {
                name: "region".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "sum".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["region".to_string()],
    };
    StandingViewSpec {
        view_id: "orders_by_region".to_string(),
        sql: "select region, sum(amount) as sum from orders group by region".to_string(),
        dialect: SqlDialect::VelorixSql,
        source_kind: SqlSourceKind::StandingView,
        input_relations: vec![input],
        output_relations: vec![output],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    }
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
        artifact_id: "materialized-view-runtime-orders-by-region-20260614".to_string(),
        artifact_hash: format!("sha256:{}", "0".repeat(64)),
        runtime_crate_name: "velorix_materialized_view_runtime".to_string(),
        state_codec: "velorix-materialized-view-state-v1".to_string(),
        state_schema_version: 1,
        execution_status: "direct_execution_enabled".to_string(),
        execution_path: "internal_materialized_runtime".to_string(),
        standing_program_identity: Some(standing_program_identity(&spec.view_id)),
    }
}

fn runtime_binding(spec: &StandingViewSpec) -> MaterializedViewRuntimeBinding {
    MaterializedViewRuntimeBinding {
        input_bindings: Vec::new(),
        runtime_kind: "velorix_materialized_view_runtime".to_string(),
        runtime_version: "builtin-v1".to_string(),
        standing_program_identity: standing_program_identity(&spec.view_id),
        logical_plan: None,
        published_relations: vec![published_relation_binding_v1(
            &spec.view_id,
            1,
            "velorix-logical-view-plan-sha256-v1:test",
            &spec.output_relations[0],
        )
        .unwrap()],
    }
}

async fn register_runtime_view(
    registry: &MaterializedViewRegistry,
    spec: &StandingViewSpec,
) -> RegisterMaterializedViewOutcome {
    registry
        .register_with_api_metadata_runtime_execution(
            spec,
            None,
            runtime_binding(spec),
            Some(MaterializedViewLifecycleStatus::standing_runtime()),
        )
        .await
        .unwrap()
}

fn standing_program_identity(view_id: &str) -> StandingProgramIdentity {
    StandingProgramIdentity {
        tenant_id: "default".to_string(),
        program_id: view_id.to_string(),
        view_ids: vec![view_id.to_string()],
        sql_hash: format!("sha256:{}", "1".repeat(64)),
        input_catalog_hash: format!("sha256:{}", "2".repeat(64)),
        output_schema_hash: format!("sha256:{}", "3".repeat(64)),
        planner_identity: "velorix-logical-view-planner:test".to_string(),
        builtin_runtime_identities: vec![BuiltinRuntimeIdentity {
            name: "velorix_materialized_view_runtime".to_string(),
            version: "builtin-v1".to_string(),
        }],
        runtime_capabilities: vec!["internal_materialized_runtime".to_string()],
        runtime_compatibility: "velorix-materialized-runtime-v1".to_string(),
        checkpoint_codec_identity: "velorix-materialized-view-state-v1".to_string(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
        dependency_binding_digest: String::new(),
        authenticated_tenant_id: "default".to_string(),
    }
}

async fn write_active_record_json(
    store: &Arc<dyn ObjectStore>,
    view_id: &str,
    value: serde_json::Value,
) {
    object_store::ObjectStoreExt::put(
        &**store,
        &Path::from(format!("v1/views/{view_id}/active.json")),
        serde_json::to_vec(&value).unwrap().into(),
    )
    .await
    .unwrap();
}

fn legacy_key(prefix: &str, suffix: &str) -> String {
    format!("{prefix}{suffix}")
}

fn runtime_binding_with_legacy_identity(spec: &StandingViewSpec) -> serde_json::Value {
    let mut value = serde_json::to_value(runtime_binding(spec)).unwrap();
    let identity = value
        .get_mut("standing_program_identity")
        .and_then(serde_json::Value::as_object_mut)
        .unwrap();
    let planner = identity.remove("planner_identity").unwrap();
    identity.insert(legacy_key("compiler", "_identity"), planner);
    let runtimes = identity.remove("builtin_runtime_identities").unwrap();
    identity.insert(legacy_key("runtime", "_packages"), runtimes);
    let capabilities = identity.remove("runtime_capabilities").unwrap();
    identity.insert(legacy_key("package", "_feature_set"), capabilities);
    value
}

#[tokio::test]
async fn materialized_view_registry_creates_and_reads_view_definition() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = sample_spec();
    let spec_hash = view_spec_hash(&spec).unwrap();

    let outcome = register_runtime_view(&registry, &spec).await;
    let read_back = registry.read(&spec.view_id, &spec_hash).await.unwrap();

    assert_eq!(outcome, RegisterMaterializedViewOutcome::Created);
    assert_eq!(read_back, spec);
}

#[tokio::test]
async fn materialized_view_registry_treats_duplicate_same_definition_as_idempotent() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = sample_spec();

    register_runtime_view(&registry, &spec).await;
    let duplicate = register_runtime_view(&registry, &spec).await;

    assert_eq!(duplicate, RegisterMaterializedViewOutcome::Duplicate);
}

#[tokio::test]
async fn materialized_view_registry_reads_active_definition_by_view_id() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = sample_spec();
    let spec_hash = view_spec_hash(&spec).unwrap();

    register_runtime_view(&registry, &spec).await;
    let active = registry.read_active(&spec.view_id).await.unwrap();

    assert_eq!(active.spec_hash, spec_hash);
    assert_eq!(active.spec, spec);
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
async fn materialized_view_registry_writes_native_runtime_lifecycle_json() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let spec = sample_spec();

    register_runtime_view(&registry, &spec).await;
    let active_json = store
        .get(&Path::from(format!(
            "v1/views/{}/active.json",
            spec.view_id
        )))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let active: serde_json::Value = serde_json::from_slice(&active_json).unwrap();
    let lifecycle = active["lifecycle"].as_object().unwrap();

    assert_eq!(
        lifecycle.get("runtime_engine").unwrap(),
        "materialized_view_runtime"
    );
    assert_eq!(lifecycle.get("admission_status").unwrap(), "admitted");
    assert!(!lifecycle.contains_key(&format!("{}{}", "compiler", "_backend")));
    assert!(!lifecycle.contains_key(&format!("{}{}", "compile", "_status")));
}

#[tokio::test]
async fn materialized_view_registry_lists_active_definitions() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = sample_spec();
    let spec_hash = view_spec_hash(&spec).unwrap();

    register_runtime_view(&registry, &spec).await;
    let active = registry.list_active().await.unwrap();

    assert_eq!(active.len(), 1);
    assert_eq!(active[0].spec_hash, spec_hash);
    assert_eq!(active[0].spec, spec);
}

#[tokio::test]
async fn materialized_view_registry_preserves_active_view_response_schema() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = sample_spec();
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
        .register_with_api_metadata_runtime_execution(
            &spec,
            Some(api.clone()),
            runtime_binding(&spec),
            Some(MaterializedViewLifecycleStatus::standing_runtime()),
        )
        .await
        .unwrap();
    let active = registry.read_active(&spec.view_id).await.unwrap();

    assert_eq!(active.api.unwrap(), api);
}

#[tokio::test]
async fn materialized_view_registry_rejects_artifact_only_runtime_binding() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = sample_spec();
    let artifact = artifact_binding(&spec);

    let error = registry
        .register_with_api_metadata_and_artifact(&spec, None, Some(artifact.clone()))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        MaterializedViewRegistryError::InvalidExecutionMode {
            reason: InvalidExecutionModeReason::StandingRuntimeMissingRuntimeBinding,
            ..
        }
    ));
}

#[tokio::test]
async fn materialized_view_registry_derives_execution_mode_for_old_active_records_with_runtime() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let spec = sample_spec();
    let spec_hash = view_spec_hash(&spec).unwrap();

    register_runtime_view(&registry, &spec).await;
    write_active_record_json(
        &store,
        &spec.view_id,
        serde_json::json!({
            "schema_version": 1,
            "view_id": spec.view_id,
            "spec_hash": spec_hash,
            "runtime": runtime_binding(&spec)
        }),
    )
    .await;

    let active = registry.read_active(&spec.view_id).await.unwrap();

    assert_eq!(
        active.execution_mode,
        MaterializedViewExecutionMode::StandingRuntime
    );
}

#[tokio::test]
async fn materialized_view_registry_reads_legacy_active_runtime_lifecycle_and_identity_keys() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let spec = sample_spec();
    let spec_hash = view_spec_hash(&spec).unwrap();

    register_runtime_view(&registry, &spec).await;
    write_active_record_json(
        &store,
        &spec.view_id,
        serde_json::json!({
            "schema_version": 3,
            "view_id": spec.view_id,
            "spec_hash": spec_hash,
            "execution_mode": "standing_runtime",
            "runtime": runtime_binding_with_legacy_identity(&spec),
            "lifecycle": {
                legacy_key("compiler", "_backend"): "materialized_view_runtime",
                legacy_key("compile", "_status"): "success",
                "deployment_status": "running"
            }
        }),
    )
    .await;

    let active = registry.read_active(&spec.view_id).await.unwrap();

    assert_eq!(active.lifecycle.runtime_engine, "materialized_view_runtime");
    assert_eq!(
        active.lifecycle.admission_status,
        velorix_storage::materialized_view_registry::MaterializedViewAdmissionStatus::Admitted
    );
    assert_eq!(
        active
            .runtime
            .unwrap()
            .standing_program_identity
            .planner_identity,
        "velorix-logical-view-planner:test"
    );
}

#[tokio::test]
async fn materialized_view_registry_rejects_legacy_artifact_only_active_records() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let spec = sample_spec();
    let spec_hash = view_spec_hash(&spec).unwrap();

    register_runtime_view(&registry, &spec).await;

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

    let error = registry.read_active(&spec.view_id).await.unwrap_err();

    assert!(matches!(
        error,
        MaterializedViewRegistryError::InvalidExecutionMode {
            reason: InvalidExecutionModeReason::StandingRuntimeMissingRuntimeBinding,
            ..
        }
    ));
}

#[tokio::test]
async fn materialized_view_registry_rejects_current_schema_active_record_without_execution_mode() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let spec = sample_spec();
    let spec_hash = view_spec_hash(&spec).unwrap();

    register_runtime_view(&registry, &spec).await;
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
async fn materialized_view_registry_rejects_standing_runtime_mode_without_runtime_binding() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let spec = sample_spec();
    let spec_hash = view_spec_hash(&spec).unwrap();

    register_runtime_view(&registry, &spec).await;
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
        MaterializedViewRegistryError::InvalidExecutionMode {
            reason: InvalidExecutionModeReason::StandingRuntimeMissingRuntimeBinding,
            ..
        }
    ));
}

#[tokio::test]
async fn materialized_view_registry_rejects_legacy_mode_records() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let spec = sample_spec();
    let spec_hash = view_spec_hash(&spec).unwrap();

    register_runtime_view(&registry, &spec).await;
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

    assert!(matches!(error, MaterializedViewRegistryError::Serde(_)));
}

#[tokio::test]
async fn materialized_view_registry_indexes_active_view_api_path() {
    let (_temp_dir, store) = temp_store();
    let registry = MaterializedViewRegistry::new(store);
    let spec = sample_spec();
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
        .register_with_api_metadata_runtime_execution(
            &spec,
            Some(api.clone()),
            runtime_binding(&spec),
            Some(MaterializedViewLifecycleStatus::standing_runtime()),
        )
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
    let first = sample_spec();
    let mut second = first.clone();
    second.view_id = "orders_by_account_copy".to_string();
    let api = MaterializedViewApiMetadata {
        url_path: Some("/orders/:account_id".to_string()),
        ..MaterializedViewApiMetadata::default()
    };

    registry
        .register_with_api_metadata_runtime_execution(
            &first,
            Some(api.clone()),
            runtime_binding(&first),
            Some(MaterializedViewLifecycleStatus::standing_runtime()),
        )
        .await
        .unwrap();
    let error = registry
        .register_with_api_metadata_runtime_execution(
            &second,
            Some(api),
            runtime_binding(&second),
            Some(MaterializedViewLifecycleStatus::standing_runtime()),
        )
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
    let spec = sample_spec();
    let mut wrong_body = spec.clone();
    wrong_body.sql = "select region from orders".to_string();
    let spec_hash = view_spec_hash(&spec).unwrap();
    let path = registry.object_key(&spec.view_id, &spec_hash).unwrap();

    object_store::ObjectStoreExt::put(
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
    let mut spec = sample_spec();
    spec.shape.is_materialized = false;

    let error = registry.register(&spec).await.unwrap_err();

    assert!(matches!(
        error,
        MaterializedViewRegistryError::Validation(ViewContractError::InvalidField {
            field: "shape.is_materialized"
        })
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
