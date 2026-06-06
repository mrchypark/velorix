use std::{fs, sync::Arc};

use object_store::{local::LocalFileSystem, ObjectStore};
use tempfile::TempDir;
use velorix_core::feldera_artifact::{
    ColumnSchema, RelationSchema, SqlDataType, SqlDialect, SqlSourceKind, StandingViewShape,
    StandingViewSpec,
};
use velorix_storage::{
    materialized_view_registry::{
        MaterializedViewCompileStatus, MaterializedViewDeploymentStatus,
        MaterializedViewLifecycleStatus,
    },
    view_compile_deploy_job_registry::{
        view_compile_deploy_job_id, RegisterViewCompileDeployJobOutcome,
        ViewCompileDeployJobRegistry, ViewCompileDeployJobRegistryError,
    },
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

#[tokio::test]
async fn view_compile_deploy_job_registry_persists_self_contained_compiler_request() {
    let (_temp_dir, store) = temp_store();
    let registry = ViewCompileDeployJobRegistry::new(store);
    let spec = standing_view_spec();
    let spec_hash = format!("velorix-feldera-spec-sha256-v1:{}", "d".repeat(64));
    let lifecycle = MaterializedViewLifecycleStatus::feldera_compile_pending(Some(
        "compiler worker queued".to_string(),
    ));

    let created = registry
        .register_pending_for_spec(&spec, &spec_hash, &lifecycle)
        .await
        .unwrap();
    let duplicate = registry
        .register_pending_for_spec(&spec, &spec_hash, &lifecycle)
        .await
        .unwrap();
    let record = registry.read("scores_by_user", &spec_hash).await.unwrap();
    let compiler_request = record.compiler_request.unwrap();

    assert_eq!(created, RegisterViewCompileDeployJobOutcome::Created);
    assert_eq!(duplicate, RegisterViewCompileDeployJobOutcome::Duplicate);
    assert_eq!(
        compiler_request.request_kind,
        "feldera_standing_view_compile_request_v1"
    );
    assert_eq!(compiler_request.view_id, "scores_by_user");
    assert_eq!(compiler_request.spec_hash, spec_hash);
    assert_eq!(compiler_request.sql, spec.sql);
    assert_eq!(compiler_request.input_relations, spec.input_relations);
    assert_eq!(compiler_request.output_relations, spec.output_relations);
    assert_eq!(compiler_request.shape, spec.shape);
}

#[tokio::test]
async fn view_compile_deploy_job_registry_rejects_compiler_request_identity_mismatch() {
    let (temp_dir, store) = temp_store();
    let registry = ViewCompileDeployJobRegistry::new(store);
    let spec_hash = format!("velorix-feldera-spec-sha256-v1:{}", "e".repeat(64));
    let object_key = registry.object_key("scores_by_user", &spec_hash).unwrap();
    let path = temp_dir.path().join(object_key.as_str());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "job_id": view_compile_deploy_job_id("scores_by_user", &spec_hash),
            "view_id": "scores_by_user",
            "spec_hash": spec_hash,
            "compiler_backend": "feldera_compiler",
            "compiler_request": {
                "request_kind": "feldera_standing_view_compile_request_v1",
                "view_id": "other_view",
                "spec_hash": spec_hash,
                "sql": "select user_id from scores",
                "input_relations": standing_view_spec().input_relations,
                "output_relations": standing_view_spec().output_relations,
                "shape": standing_view_spec().shape
            },
            "compile_status": "pending",
            "deployment_status": "not_deployed"
        }))
        .unwrap(),
    )
    .unwrap();

    let error = registry
        .read("scores_by_user", &spec_hash)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ViewCompileDeployJobRegistryError::RecordIdentityMismatch { .. }
    ));
}

#[tokio::test]
async fn view_compile_deploy_job_registry_rejects_non_materialized_compiler_request() {
    let (temp_dir, store) = temp_store();
    let registry = ViewCompileDeployJobRegistry::new(store);
    let spec_hash = format!("velorix-feldera-spec-sha256-v1:{}", "f".repeat(64));
    let object_key = registry.object_key("scores_by_user", &spec_hash).unwrap();
    let path = temp_dir.path().join(object_key.as_str());
    let mut shape = standing_view_spec().shape;
    shape.is_materialized = false;
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "job_id": view_compile_deploy_job_id("scores_by_user", &spec_hash),
            "view_id": "scores_by_user",
            "spec_hash": spec_hash,
            "compiler_backend": "feldera_compiler",
            "compiler_request": {
                "request_kind": "feldera_standing_view_compile_request_v1",
                "view_id": "scores_by_user",
                "spec_hash": spec_hash,
                "sql": "select user_id from scores",
                "input_relations": standing_view_spec().input_relations,
                "output_relations": standing_view_spec().output_relations,
                "shape": shape
            },
            "compile_status": "pending",
            "deployment_status": "not_deployed"
        }))
        .unwrap(),
    )
    .unwrap();

    let error = registry
        .read("scores_by_user", &spec_hash)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ViewCompileDeployJobRegistryError::RecordIdentityMismatch { .. }
    ));
}

#[tokio::test]
async fn view_compile_deploy_job_registry_registers_pending_job_idempotently() {
    let (_temp_dir, store) = temp_store();
    let registry = ViewCompileDeployJobRegistry::new(store);
    let spec_hash = format!("velorix-feldera-spec-sha256-v1:{}", "a".repeat(64));
    let lifecycle = MaterializedViewLifecycleStatus::feldera_compile_pending(Some(
        "compiler worker queued".to_string(),
    ));

    let created = registry
        .register_pending("scores_by_user", &spec_hash, &lifecycle)
        .await
        .unwrap();
    let duplicate = registry
        .register_pending("scores_by_user", &spec_hash, &lifecycle)
        .await
        .unwrap();
    let record = registry.read("scores_by_user", &spec_hash).await.unwrap();

    assert_eq!(created, RegisterViewCompileDeployJobOutcome::Created);
    assert_eq!(duplicate, RegisterViewCompileDeployJobOutcome::Duplicate);
    assert_eq!(
        record.job_id,
        view_compile_deploy_job_id("scores_by_user", &spec_hash)
    );
    assert_eq!(record.view_id, "scores_by_user");
    assert_eq!(record.spec_hash, spec_hash);
    assert_eq!(record.compile_status, lifecycle.compile_status);
    assert_eq!(record.deployment_status, lifecycle.deployment_status);
}

fn standing_view_spec() -> StandingViewSpec {
    StandingViewSpec {
        view_id: "scores_by_user".to_string(),
        sql: "select user_id, sum(score) as sum, count(*) as count from scores group by user_id"
            .to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        input_relations: vec![RelationSchema {
            relation_id: "scores".to_string(),
            relation_name: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "1".repeat(64)),
            columns: vec![
                ColumnSchema {
                    name: "user_id".to_string(),
                    data_type: SqlDataType::Utf8,
                    nullable: false,
                },
                ColumnSchema {
                    name: "score".to_string(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "delta".to_string(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                },
            ],
            primary_key: vec!["user_id".to_string()],
        }],
        output_relations: vec![RelationSchema {
            relation_id: "scores_by_user".to_string(),
            relation_name: "scores_by_user".to_string(),
            relation_version: "v1".to_string(),
            schema_fingerprint: format!("sha256:{}", "2".repeat(64)),
            columns: vec![
                ColumnSchema {
                    name: "user_id".to_string(),
                    data_type: SqlDataType::Utf8,
                    nullable: false,
                },
                ColumnSchema {
                    name: "sum".to_string(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "count".to_string(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                },
            ],
            primary_key: vec!["user_id".to_string()],
        }],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    }
}

#[tokio::test]
async fn view_compile_deploy_job_registry_lists_pending_jobs_in_stable_order() {
    let (_temp_dir, store) = temp_store();
    let registry = ViewCompileDeployJobRegistry::new(store);
    let left_hash = format!("velorix-feldera-spec-sha256-v1:{}", "b".repeat(64));
    let right_hash = format!("velorix-feldera-spec-sha256-v1:{}", "a".repeat(64));
    let lifecycle = MaterializedViewLifecycleStatus::feldera_compile_pending(None);

    registry
        .register_pending("z_view", &left_hash, &lifecycle)
        .await
        .unwrap();
    registry
        .register_pending("a_view", &right_hash, &lifecycle)
        .await
        .unwrap();

    let pending = registry.list_pending().await.unwrap();

    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].view_id, "a_view");
    assert_eq!(pending[1].view_id, "z_view");
}

#[tokio::test]
async fn view_compile_deploy_job_registry_does_not_mark_terminal_failure_running() {
    let (temp_dir, store) = temp_store();
    let registry = ViewCompileDeployJobRegistry::new(store);
    let spec_hash = format!("velorix-feldera-spec-sha256-v1:{}", "c".repeat(64));
    let object_key = registry.object_key("failed_view", &spec_hash).unwrap();
    let path = temp_dir.path().join(object_key.as_str());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "job_id": view_compile_deploy_job_id("failed_view", &spec_hash),
            "view_id": "failed_view",
            "spec_hash": spec_hash,
            "compiler_backend": "feldera_compiler",
            "compile_status": "rust_error",
            "deployment_status": "failed",
            "message": "rust build failed"
        }))
        .unwrap(),
    )
    .unwrap();

    let error = registry
        .mark_running(
            "failed_view",
            &spec_hash,
            Some("should not overwrite".to_string()),
        )
        .await
        .unwrap_err();
    let record = registry.read("failed_view", &spec_hash).await.unwrap();

    assert!(matches!(
        error,
        ViewCompileDeployJobRegistryError::RecordConflict { .. }
    ));
    assert_eq!(
        record.compile_status,
        MaterializedViewCompileStatus::RustError
    );
    assert_eq!(
        record.deployment_status,
        MaterializedViewDeploymentStatus::Failed
    );
}
