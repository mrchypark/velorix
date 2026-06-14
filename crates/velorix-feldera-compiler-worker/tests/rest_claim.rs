use std::sync::{Arc, Mutex};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use velorix_core::{
    feldera_artifact::{
        catalog_input_relation_schema, feldera_compile_request_hash, ColumnSchema,
        FelderaCompileRequestV1, OutputSchemaContract, RelationSchema, SqlDataType, SqlDialect,
        SqlSourceKind, StandingViewShape, StandingViewSpec,
    },
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
};
use velorix_feldera_compiler_worker::{
    run_once, JarlessProductRuntimeConfig, WorkerBackendKind, WorkerConfig,
};

#[tokio::test]
async fn worker_reports_jarless_infer_output_as_unsupported_without_complete() {
    let complete_bodies = Arc::new(Mutex::new(Vec::<Value>::new()));
    let catalog = device_signal_catalog("device_signals_worker");
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view_id = "device_positive_totals_worker";
    let spec = StandingViewSpec {
        view_id: view_id.to_string(),
        sql: "select device_id, sum(reading) as sum, count(*) as count from device_signals_worker where reading > 0 group by device_id".to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema],
        output_relations: Vec::new(),
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compiler_request = FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec);
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let api_url = spawn_fake_velorix_api_with_compiler_job(
        Arc::clone(&complete_bodies),
        catalog,
        compiler_request,
        compile_request_hash,
        view_id,
    )
    .await;

    let report = run_once(WorkerConfig {
        api_url: api_url.parse().unwrap(),
        admin_auth_header: "authorization: Bearer admin-token".to_string(),
        worker_id: "worker-a".to_string(),
        lease_duration_ms: 30000,
        max_claims: 1,
        request_timeout_ms: 5000,
        feldera_pipeline_manager_url: None,
        feldera_bearer_token: None,
        feldera_program_profile: "dev".to_string(),
        feldera_pipeline_workers: 1,
        feldera_poll_interval_ms: 10,
        feldera_poll_timeout_ms: 5000,
        claim_without_backend: true,
        backend_kind: WorkerBackendKind::FelderaPackageJarless,
        jarless_product_runtime: None,
    })
    .await
    .unwrap();

    assert_eq!(report.pending_jobs, 1);
    assert_eq!(report.claimed, 1);
    assert_eq!(report.failed, 0);
    assert_eq!(report.outcomes[0].status, "unsupported_by_selected_backend");
    assert!(report.outcomes[0]
        .reason
        .as_deref()
        .unwrap()
        .contains("requires_java_sql_compiler=true"));
    assert!(complete_bodies.lock().unwrap().is_empty());
}

#[tokio::test]
async fn worker_reports_empty_catalog_without_claims() {
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let app = Router::new().route(
        "/v1/view-compile-deploy/jobs",
        get({
            let requests = Arc::clone(&requests);
            move || async move {
                requests
                    .lock()
                    .unwrap()
                    .push("/v1/view-compile-deploy/jobs".to_string());
                Json(json!({ "pending_jobs": 0, "jobs": [] }))
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let report = run_once(WorkerConfig {
        api_url: format!("http://{addr}").parse().unwrap(),
        admin_auth_header: "authorization: Bearer admin-token".to_string(),
        worker_id: "worker-a".to_string(),
        lease_duration_ms: 30000,
        max_claims: 1,
        request_timeout_ms: 5000,
        feldera_pipeline_manager_url: None,
        feldera_bearer_token: None,
        feldera_program_profile: "dev".to_string(),
        feldera_pipeline_workers: 1,
        feldera_poll_interval_ms: 10,
        feldera_poll_timeout_ms: 5000,
        claim_without_backend: false,
        backend_kind: WorkerBackendKind::FelderaPackageJarless,
        jarless_product_runtime: None,
    })
    .await
    .unwrap();

    assert_eq!(report.pending_jobs, 0);
    assert_eq!(report.claimed, 0);
    assert!(report.outcomes.is_empty());
}

#[tokio::test]
async fn worker_reports_claim_error_response_body() {
    let app = Router::new()
        .route(
            "/v1/view-compile-deploy/jobs",
            get(|| async {
                Json(json!({
                    "pending_jobs": 1,
                    "jobs": [{
                        "job_id": "orders_by_account:compile-hash",
                        "view_id": "orders_by_account"
                    }]
                }))
            }),
        )
        .route(
            "/v1/view-compile-deploy/jobs/orders_by_account/claim",
            post(|| async { (StatusCode::CONFLICT, "active worker still owns job") }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let report = run_once(WorkerConfig {
        api_url: format!("http://{addr}").parse().unwrap(),
        admin_auth_header: "authorization: Bearer admin-token".to_string(),
        worker_id: "worker-b".to_string(),
        lease_duration_ms: 30000,
        max_claims: 1,
        request_timeout_ms: 5000,
        feldera_pipeline_manager_url: None,
        feldera_bearer_token: None,
        feldera_program_profile: "dev".to_string(),
        feldera_pipeline_workers: 1,
        feldera_poll_interval_ms: 10,
        feldera_poll_timeout_ms: 5000,
        claim_without_backend: true,
        backend_kind: WorkerBackendKind::FelderaPackageJarless,
        jarless_product_runtime: None,
    })
    .await
    .unwrap();

    assert_eq!(report.failed, 1);
    assert!(report.outcomes[0]
        .reason
        .as_deref()
        .unwrap()
        .contains("active worker still owns job"));
}

#[tokio::test]
async fn worker_refuses_pipeline_manager_url_unless_compatibility_backend_is_selected() {
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let api_url = spawn_fake_velorix_api(Arc::clone(&requests)).await;

    let error = run_once(WorkerConfig {
        api_url: api_url.parse().unwrap(),
        admin_auth_header: "authorization: Bearer admin-token".to_string(),
        worker_id: "worker-a".to_string(),
        lease_duration_ms: 30000,
        max_claims: 1,
        request_timeout_ms: 5000,
        feldera_pipeline_manager_url: Some("http://127.0.0.1:18082".parse().unwrap()),
        feldera_bearer_token: None,
        feldera_program_profile: "dev".to_string(),
        feldera_pipeline_workers: 1,
        feldera_poll_interval_ms: 10,
        feldera_poll_timeout_ms: 5000,
        claim_without_backend: false,
        backend_kind: WorkerBackendKind::FelderaPackageJarless,
        jarless_product_runtime: None,
    })
    .await
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("set compiler backend to compatibility-pipeline-manager"));
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn worker_compiles_pipeline_manager_job_and_completes_with_runtime_deployment() {
    let complete_bodies = Arc::new(Mutex::new(Vec::<Value>::new()));
    let feldera_requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let catalog = device_signal_catalog("device_signals_worker");
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view_id = "device_positive_totals_worker";
    let spec = StandingViewSpec {
        view_id: view_id.to_string(),
        sql: "select device_id, sum(reading) as sum, count(*) as count from device_signals_worker where reading > 0 group by device_id".to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema],
        output_relations: Vec::new(),
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compiler_request = FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec);
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let api_url = spawn_fake_velorix_api_with_compiler_job(
        Arc::clone(&complete_bodies),
        catalog,
        compiler_request,
        compile_request_hash.clone(),
        view_id,
    )
    .await;
    let feldera_url =
        spawn_fake_feldera_pipeline_manager(Arc::clone(&feldera_requests), view_id).await;

    let report = run_once(WorkerConfig {
        api_url: api_url.parse().unwrap(),
        admin_auth_header: "authorization: Bearer admin-token".to_string(),
        worker_id: "worker-a".to_string(),
        lease_duration_ms: 30000,
        max_claims: 1,
        request_timeout_ms: 5000,
        feldera_pipeline_manager_url: Some(feldera_url.parse().unwrap()),
        feldera_bearer_token: None,
        feldera_program_profile: "dev".to_string(),
        feldera_pipeline_workers: 1,
        feldera_poll_interval_ms: 10,
        feldera_poll_timeout_ms: 5000,
        claim_without_backend: false,
        backend_kind: WorkerBackendKind::CompatibilityPipelineManager,
        jarless_product_runtime: None,
    })
    .await
    .unwrap();

    assert_eq!(report.failed, 0, "report: {report:?}");
    assert_eq!(report.claimed, 1);
    assert_eq!(
        report.outcomes[0].status, "completed_compatibility_runtime_deployment",
        "report: {report:?}"
    );

    let feldera_requests = feldera_requests.lock().unwrap();
    assert!(feldera_requests
        .iter()
        .any(|request| request
            .starts_with("PUT /v0/pipelines/velorix-device_positive_totals_worker-")));
    assert!(feldera_requests
        .iter()
        .any(|request| request
            .starts_with("GET /v0/pipelines/velorix-device_positive_totals_worker-")));

    let complete_bodies = complete_bodies.lock().unwrap();
    assert_eq!(complete_bodies.len(), 1);
    let body = &complete_bodies[0];
    assert_eq!(body["compile_request_hash"], compile_request_hash);
    assert_eq!(body["tenant_id"], "default");
    assert_eq!(body["job_generation"], 1);
    assert_eq!(body["worker_id"], "worker-a");
    assert_eq!(body["lease_id"], "lease-id");
    assert_eq!(body["fencing_token"], 1);
    assert_eq!(body["runtime_deployment"]["mode"], "external_managed");
    assert_eq!(
        body["resolved_spec"]["output_relations"][0]["relation_id"],
        view_id
    );
    assert_eq!(
        body["resolved_spec"]["output_relations"][0]["columns"][1]["name"],
        "sum"
    );
    let report_json = serde_json::to_string(&report).unwrap();
    assert!(!report_json.contains("completed_runtime_deployment"));
    assert!(!report_json.contains("lease-id"));
    assert!(!report_json.contains("fencing_token"));
}

#[tokio::test]
async fn worker_passes_feldera_program_jobs_to_compatibility_backend() {
    let complete_bodies = Arc::new(Mutex::new(Vec::<Value>::new()));
    let feldera_requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let catalog = device_signal_catalog("device_signals_worker");
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view_id = "device_program_worker";
    let spec = StandingViewSpec {
        view_id: view_id.to_string(),
        sql: "CREATE MATERIALIZED VIEW device_program_worker AS SELECT device_id, sum(reading) AS sum, count(*) AS count FROM device_signals_worker GROUP BY device_id".to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::FelderaProgram,
        rust_extension: Default::default(),
        input_relations: vec![input_schema],
        output_relations: Vec::new(),
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compiler_request = FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec);
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let api_url = spawn_fake_velorix_api_with_compiler_job(
        Arc::clone(&complete_bodies),
        catalog,
        compiler_request,
        compile_request_hash,
        view_id,
    )
    .await;
    let feldera_url =
        spawn_fake_feldera_pipeline_manager(Arc::clone(&feldera_requests), view_id).await;

    let report = run_once(WorkerConfig {
        api_url: api_url.parse().unwrap(),
        admin_auth_header: "authorization: Bearer admin-token".to_string(),
        worker_id: "worker-a".to_string(),
        lease_duration_ms: 30000,
        max_claims: 1,
        request_timeout_ms: 5000,
        feldera_pipeline_manager_url: Some(feldera_url.parse().unwrap()),
        feldera_bearer_token: None,
        feldera_program_profile: "dev".to_string(),
        feldera_pipeline_workers: 1,
        feldera_poll_interval_ms: 10,
        feldera_poll_timeout_ms: 5000,
        claim_without_backend: false,
        backend_kind: WorkerBackendKind::CompatibilityPipelineManager,
        jarless_product_runtime: None,
    })
    .await
    .unwrap();

    assert_eq!(report.failed, 0, "report: {report:?}");
    assert_eq!(
        report.outcomes[0].status, "completed_compatibility_runtime_deployment",
        "report: {report:?}"
    );

    let feldera_requests = feldera_requests.lock().unwrap();
    assert!(feldera_requests.iter().any(
        |request| request.contains("CREATE MATERIALIZED VIEW device_program_worker AS SELECT")
    ));
}

#[tokio::test]
async fn worker_resolves_must_match_output_as_jarless_schema_only_without_complete() {
    let complete_bodies = Arc::new(Mutex::new(Vec::<Value>::new()));
    let catalog = device_signal_catalog("device_signals_worker");
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view_id = "device_positive_totals_worker";
    let output_schema = device_positive_totals_output_schema(view_id);
    let spec = StandingViewSpec {
        view_id: view_id.to_string(),
        sql: "select device_id, sum(reading) as sum, count(*) as count from device_signals_worker where reading > 0 group by device_id".to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema],
        output_relations: vec![output_schema.clone()],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compiler_request = FelderaCompileRequestV1 {
        view_id: spec.view_id.clone(),
        sql: spec.sql.clone(),
        dialect: spec.dialect.clone(),
        source_kind: spec.source_kind.clone(),
        rust_extension: spec.rust_extension.clone(),
        input_relations: spec.input_relations.clone(),
        output_contract: OutputSchemaContract::MustMatch {
            output_relations: vec![output_schema],
        },
        shape: spec.shape.clone(),
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let api_url = spawn_fake_velorix_api_with_compiler_job(
        Arc::clone(&complete_bodies),
        catalog,
        compiler_request,
        compile_request_hash,
        view_id,
    )
    .await;

    let report = run_once(WorkerConfig {
        api_url: api_url.parse().unwrap(),
        admin_auth_header: "authorization: Bearer admin-token".to_string(),
        worker_id: "worker-a".to_string(),
        lease_duration_ms: 30000,
        max_claims: 1,
        request_timeout_ms: 5000,
        feldera_pipeline_manager_url: None,
        feldera_bearer_token: None,
        feldera_program_profile: "dev".to_string(),
        feldera_pipeline_workers: 1,
        feldera_poll_interval_ms: 10,
        feldera_poll_timeout_ms: 5000,
        claim_without_backend: false,
        backend_kind: WorkerBackendKind::FelderaPackageJarless,
        jarless_product_runtime: None,
    })
    .await
    .unwrap();

    assert_eq!(report.pending_jobs, 1);
    assert_eq!(report.claimed, 1);
    assert_eq!(report.failed, 0);
    assert_eq!(
        report.outcomes[0].status,
        "compiled_schema_only_not_deployed"
    );
    assert!(report.outcomes[0]
        .reason
        .as_deref()
        .unwrap()
        .contains("feldera-package-schema-only-v1:"));
    assert!(complete_bodies.lock().unwrap().is_empty());
}

#[tokio::test]
async fn worker_completes_must_match_output_as_jarless_product_runtime_when_binding_configured() {
    let complete_bodies = Arc::new(Mutex::new(Vec::<Value>::new()));
    let catalog = device_signal_catalog("device_signals_worker");
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view_id = "device_positive_totals_worker";
    let output_schema = device_positive_totals_output_schema(view_id);
    let spec = StandingViewSpec {
        view_id: view_id.to_string(),
        sql: "select device_id, sum(reading) as sum, count(*) as count from device_signals_worker where reading > 0 group by device_id".to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema],
        output_relations: vec![output_schema.clone()],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compiler_request = FelderaCompileRequestV1 {
        view_id: spec.view_id.clone(),
        sql: spec.sql.clone(),
        dialect: spec.dialect.clone(),
        source_kind: spec.source_kind.clone(),
        rust_extension: spec.rust_extension.clone(),
        input_relations: spec.input_relations.clone(),
        output_contract: OutputSchemaContract::MustMatch {
            output_relations: vec![output_schema],
        },
        shape: spec.shape.clone(),
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let api_url = spawn_fake_velorix_api_with_compiler_job(
        Arc::clone(&complete_bodies),
        catalog,
        compiler_request,
        compile_request_hash.clone(),
        view_id,
    )
    .await;

    let report = run_once(WorkerConfig {
        api_url: api_url.parse().unwrap(),
        admin_auth_header: "authorization: Bearer admin-token".to_string(),
        worker_id: "worker-a".to_string(),
        lease_duration_ms: 30000,
        max_claims: 1,
        request_timeout_ms: 5000,
        feldera_pipeline_manager_url: None,
        feldera_bearer_token: None,
        feldera_program_profile: "dev".to_string(),
        feldera_pipeline_workers: 1,
        feldera_poll_interval_ms: 10,
        feldera_poll_timeout_ms: 5000,
        claim_without_backend: false,
        backend_kind: WorkerBackendKind::FelderaPackageJarless,
        jarless_product_runtime: Some(JarlessProductRuntimeConfig {
            backend_version: "0.299.0-test".to_string(),
            backend_source: "feldera public Rust packages".to_string(),
            runtime_crate_name: "velorix_feldera_package_device_runtime".to_string(),
            runtime_crate_version: "0.299.0-test".to_string(),
            runtime_factory_symbol: "create_standing_runtime".to_string(),
            state_codec: "feldera-package-runtime-state-v1".to_string(),
            state_schema_version: 1,
        }),
    })
    .await
    .unwrap();

    assert_eq!(report.pending_jobs, 1);
    assert_eq!(report.claimed, 1);
    assert_eq!(report.failed, 0);
    assert_eq!(
        report.outcomes[0].status,
        "completed_product_runtime_deployment"
    );
    let complete_bodies = complete_bodies.lock().unwrap();
    assert_eq!(complete_bodies.len(), 1);
    let body = &complete_bodies[0];
    assert_eq!(body["compile_request_hash"], compile_request_hash);
    assert!(body.get("artifact").is_none());
    assert!(body.get("runtime_deployment").is_none());
    assert_eq!(
        body["product_runtime"]["runtime_factory"]["crate_name"],
        "velorix_feldera_package_device_runtime"
    );
    assert_eq!(
        body["product_runtime"]["standing_program_identity"]["package_feature_set"][0],
        "feldera_package_runtime"
    );
    assert_eq!(
        body["resolved_spec"]["output_relations"][0]["relation_id"],
        view_id
    );
}

async fn spawn_fake_velorix_api(requests: Arc<Mutex<Vec<String>>>) -> String {
    let app = Router::new()
        .route("/v1/view-compile-deploy/jobs", get(list_jobs))
        .route(
            "/v1/view-compile-deploy/jobs/orders_by_account/claim",
            post(claim_job),
        )
        .route(
            "/v1/view-compile-deploy/run-once",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .route(
            "/v1/view-compile-deploy/jobs/orders_by_account/complete",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .with_state(requests);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn spawn_fake_velorix_api_with_compiler_job(
    complete_bodies: Arc<Mutex<Vec<Value>>>,
    catalog: VelorixRelationCatalogV1,
    compiler_request: FelderaCompileRequestV1,
    compile_request_hash: String,
    view_id: &str,
) -> String {
    let view_id_owned = view_id.to_string();
    let job = json!({
        "pending_jobs": 1,
        "jobs": [{
            "job_id": format!("{view_id}:{compile_request_hash}"),
            "tenant_id": "default",
            "view_id": view_id,
            "job_generation": 1,
            "spec_hash": "spec-hash",
            "compiler_backend": "feldera_compiler",
            "compiler_request": {
                "request_kind": "feldera_standing_view_compile_request_v1",
                "view_id": compiler_request.view_id,
                "compile_request_hash": compile_request_hash,
                "spec_hash": "spec-hash",
                "sql": compiler_request.sql,
                "dialect": compiler_request.dialect,
                "source_kind": compiler_request.source_kind,
                "input_relations": compiler_request.input_relations,
                "output_contract": compiler_request.output_contract,
                "output_relations": [],
                "shape": compiler_request.shape
            },
            "compile_status": "pending",
            "deployment_status": "not_deployed",
            "input_relation_catalogs": [catalog]
        }]
    });
    let claim_hash = job["jobs"][0]["compiler_request"]["compile_request_hash"]
        .as_str()
        .unwrap()
        .to_string();
    let claim_view_id = view_id_owned.clone();
    let app = Router::new()
        .route(
            "/v1/view-compile-deploy/jobs",
            get({
                let job = job.clone();
                move || async move { Json(job.clone()) }
            }),
        )
        .route(
            &format!("/v1/view-compile-deploy/jobs/{view_id}/claim"),
            post(move || {
                let claim_hash = claim_hash.clone();
                let claim_view_id = claim_view_id.clone();
                async move {
                    Json(json!({
                        "claim_status": "claimed",
                        "tenant_id": "default",
                        "view_id": claim_view_id,
                        "job_generation": 1,
                        "compile_request_hash": claim_hash,
                        "worker_id": "worker-a",
                        "lease_id": "lease-id",
                        "fencing_token": 1,
                        "claimed_at_ms": 1,
                        "lease_expires_at_ms": 30001
                    }))
                }
            }),
        )
        .route(
            &format!("/v1/view-compile-deploy/jobs/{view_id}/complete"),
            post({
                let complete_bodies = Arc::clone(&complete_bodies);
                |Json(body): Json<Value>| async move {
                    complete_bodies.lock().unwrap().push(body);
                    Json(json!({ "view_id": "device_positive_totals_worker", "query_enabled": true }))
                }
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn spawn_fake_feldera_pipeline_manager(
    requests: Arc<Mutex<Vec<String>>>,
    view_id: &str,
) -> String {
    let view_id = view_id.to_string();
    let app = Router::new().route(
        "/v0/pipelines/{pipeline}",
        post(|| async { StatusCode::METHOD_NOT_ALLOWED })
            .put({
                let requests = Arc::clone(&requests);
                |Path(pipeline): Path<String>, Json(body): Json<Value>| async move {
                    requests.lock().unwrap().push(format!(
                        "PUT /v0/pipelines/{pipeline}:{}",
                        body["program_code"].as_str().unwrap()
                    ));
                    let program_code = body["program_code"].as_str().unwrap();
                    assert!(program_code.contains("CREATE TABLE \"device_signals_worker\""));
                    assert!(!program_code.contains("\"delta\" BIGINT"));
                    Json(json!({ "ok": true }))
                }
            })
            .get({
                let requests = Arc::clone(&requests);
                let view_id = view_id.clone();
                |Path(pipeline): Path<String>| async move {
                    requests
                        .lock()
                        .unwrap()
                        .push(format!("GET /v0/pipelines/{pipeline}"));
                    Json(json!({
                        "program_status": "Success",
                        "deployment_status": "Stopped",
                        "deployment_resources_status": "Provisioned",
                        "program_version": 7,
                        "program_info": {
                            "schema": {
                                "outputs": [{
                                    "name": view_id,
                                    "materialized": true,
                                    "fields": [
                                        { "name": "device_id", "columntype": "VARCHAR" },
                                        { "name": "sum", "columntype": "BIGINT" },
                                        { "name": "count", "columntype": "BIGINT" }
                                    ],
                                    "primary_key": ["device_id"]
                                }]
                            }
                        }
                    }))
                }
            }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn list_jobs(State(requests): State<Arc<Mutex<Vec<String>>>>) -> Json<Value> {
    requests
        .lock()
        .unwrap()
        .push("/v1/view-compile-deploy/jobs".to_string());
    Json(json!({
        "pending_jobs": 1,
        "jobs": [{
            "job_id": "orders_by_account:compile-hash",
            "view_id": "orders_by_account",
            "spec_hash": "spec-hash",
            "compiler_backend": "feldera_compiler",
            "compile_status": "pending",
            "deployment_status": "not_deployed"
        }]
    }))
}

async fn claim_job(State(requests): State<Arc<Mutex<Vec<String>>>>) -> Json<Value> {
    requests
        .lock()
        .unwrap()
        .push("/v1/view-compile-deploy/jobs/orders_by_account/claim".to_string());
    Json(json!({
        "claim_status": "claimed",
        "tenant_id": "default",
        "view_id": "orders_by_account",
        "job_generation": 1,
        "compile_request_hash": "compile-hash",
        "worker_id": "worker-a",
        "lease_id": "lease-id",
        "fencing_token": 1,
        "claimed_at_ms": 1,
        "lease_expires_at_ms": 30001
    }))
}

fn device_signal_catalog(relation_id: &str) -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: relation_id.to_string(),
        relation_name: relation_id.to_string(),
        relation_version: "2026-06-14.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "device_id".to_string(),
                name: "device_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "reading".to_string(),
                name: "reading".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["device_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: relation_id.to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: relation_id.to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn device_positive_totals_output_schema(relation_id: &str) -> RelationSchema {
    RelationSchema {
        relation_id: relation_id.to_string(),
        relation_name: relation_id.to_string(),
        relation_version: "2026-06-14.output.v1".to_string(),
        schema_fingerprint:
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        columns: vec![
            ColumnSchema {
                name: "device_id".to_string(),
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
        primary_key: vec!["device_id".to_string()],
    }
}
