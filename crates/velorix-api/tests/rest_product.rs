use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use http_body_util::BodyExt as _;
use object_store::{local::LocalFileSystem, memory::InMemory, path::Path, ObjectStore};
use serde_json::{json, Value};
use std::{fs, sync::Arc};
use tempfile::TempDir;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tower::ServiceExt as _;
use velorix_api::{app, ApiState, IngestRowsRequest, StandingProgramRuntimeFactory};
use velorix_core::{
    feldera_artifact::{
        catalog_input_relation_schema, feldera_artifact_bytes_hash, feldera_spec_hash,
        ColumnSchema, FelderaCompileArtifactMetadata, FelderaCompilerIdentity,
        GeneratedRustIdentity, RelationSchema, SqlDataType, SqlDialect, SqlSourceKind,
        StandingViewShape, StandingViewSpec, FELDERA_ARTIFACT_METADATA_VERSION,
        SUPPORTED_EPOCH_POLICY, SUPPORTED_GENERATED_RUST_ABI_VERSION, SUPPORTED_STATE_CODEC,
    },
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
    standing_program::{
        DurableStateRoot, EpochCommit, EpochIdempotencyKey, MaterializedViewPage, RelationFrontier,
        RelationInputBatch, RuntimeCheckpoint, ScopedViewId, SnapshotPageRequest,
        StandingProgramIdentity, StandingProgramRuntime, StandingProgramRuntimeError, ViewFrontier,
        ViewOutputBatch,
    },
};
use velorix_k8s::{crd::ObjectStoreAuthorityRef, startup::validate_operator_authority};
use velorix_meta::{
    proto::velorix_meta_server::VelorixMetaServer, GrpcMetaStore, InMemoryMetaStore,
    MetaGrpcService,
};
use velorix_storage::{
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::{IngestAdmissionCoordinator, IngestLog},
    materialized_view_registry::{MaterializedViewLifecycleStatus, MaterializedViewRegistry},
    view_compile_deploy_job_registry::{view_compile_deploy_job_id, ViewCompileDeployJobRegistry},
};

#[tokio::test]
async fn rest_product_required_fencing_rejects_unsafe_metadata_before_standing_runtime_activation()
{
    let (state, _temp) = api_state("required-fencing-unsafe-meta").await;
    let state = state
        .with_meta_store(Arc::new(InMemoryMetaStore::default()))
        .with_standing_runtime_fencing_required(true);
    let app = app(state);

    let ready = request_json(app.clone(), Method::GET, "/readyz", None).await;
    assert_eq!(ready.0, StatusCode::SERVICE_UNAVAILABLE);
    assert!(ready.1["error"]
        .as_str()
        .unwrap()
        .contains("control_plane_auth_enforced"));

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(app, Method::POST, "/v1/views/scores-positive-default", None).await;
    assert_eq!(view.0, StatusCode::SERVICE_UNAVAILABLE);
    assert!(view.1["error"]
        .as_str()
        .unwrap()
        .contains("production-safe"));
}

#[tokio::test]
async fn rest_product_readyz_reports_authenticated_grpc_metadata_capability() {
    let endpoint = spawn_authenticated_meta_service("secret").await;
    let meta_store = GrpcMetaStore::connect_with_bearer_token(endpoint, "secret")
        .await
        .unwrap();
    let (state, _temp) = api_state("readyz-authenticated-grpc-meta").await;
    let app = app(state
        .with_meta_store(Arc::new(meta_store))
        .with_meta_store_endpoint("http://127.0.0.1:9090"));

    let ready = request_json(app, Method::GET, "/readyz", None).await;
    assert_eq!(ready.0, StatusCode::OK, "ready body: {}", ready.1);
    assert_eq!(ready.1["status"], "ready");
    assert_eq!(ready.1["standing_runtime_fencing_required"], false);
    assert_eq!(ready.1["standing_runtime_fencing_mode"], "unsafe-dev-only");
    assert_eq!(ready.1["metadata_store"]["configured"], true);
    assert_eq!(
        ready.1["metadata_store"]["endpoint"],
        "http://127.0.0.1:9090"
    );
    assert_eq!(
        ready.1["metadata_store"]["standing_runtime_fencing"]["backend_name"],
        "in-memory"
    );
    assert_eq!(
        ready.1["metadata_store"]["standing_runtime_fencing"]["control_plane_auth_enforced"],
        true
    );
    assert_eq!(
        ready.1["metadata_store"]["standing_runtime_fencing"]["production_multi_writer_safe"],
        false
    );
    assert_eq!(
        ready.1["metadata_store"]["standing_runtime_fencing"]["backend_time_source_kind"],
        "process_clock"
    );
    assert_eq!(
        ready.1["metadata_store"]["standing_runtime_fencing"]["backend_time_blocked_reason"],
        "in_memory_process_clock_not_backend_authority"
    );
    assert_eq!(
        ready.1["metadata_store"]["standing_runtime_fencing"]["lease_authority_kind"],
        "process_local"
    );
    assert_eq!(
        ready.1["metadata_store"]["standing_runtime_fencing"]["lease_expiry_semantics"],
        "process_clock_ttl"
    );
    assert_eq!(
        ready.1["metadata_store"]["standing_runtime_fencing"]["bounded_wall_clock_failover"],
        false
    );
    assert_eq!(
        ready.1["metadata_store"]["standing_runtime_fencing"]["failover_time_bound_ms"],
        0
    );
    assert_eq!(
        ready.1["metadata_store"]["standing_runtime_fencing"]["multi_writer_fencing_safe"],
        false
    );
    assert_eq!(
        ready.1["metadata_store"]["standing_runtime_fencing"]["production_bounded_failover_safe"],
        false
    );
}

#[tokio::test]
async fn rest_product_readyz_reports_object_store_conditional_update_capability() {
    let (state, _store) = api_state_memory("readyz-object-store-conditional-update").await;
    let app = app(state);

    let ready = request_json(app, Method::GET, "/readyz", None).await;

    assert_eq!(ready.0, StatusCode::OK, "ready body: {}", ready.1);
    assert_eq!(ready.1["object_store"]["schema_version"], 1);
    assert_eq!(ready.1["object_store"]["authoritative_namespace_count"], 19);
    assert_eq!(
        ready.1["object_store"]["artifact_catalog"]["backend_name"],
        "velorix-api-test"
    );
    assert_eq!(
        ready.1["object_store"]["artifact_catalog"]["conditional_update"],
        true
    );
}

#[tokio::test]
async fn rest_product_api_bearer_token_protects_v1_routes() {
    let (state, _temp) = api_state("api-auth").await;
    let app = app(state
        .with_api_bearer_token("secret")
        .unwrap()
        .with_admin_bearer_token("admin-secret")
        .unwrap());

    let health = request_json(app.clone(), Method::GET, "/healthz", None).await;
    assert_eq!(health.0, StatusCode::OK);

    let ready = request_json(app.clone(), Method::GET, "/readyz", None).await;
    assert_eq!(ready.0, StatusCode::OK);
    assert_eq!(ready.1["api_auth"]["configured"], true);
    assert_eq!(ready.1["api_auth"]["mode"], "bearer-token");
    assert_eq!(ready.1["admin_auth"]["configured"], true);
    assert_eq!(ready.1["admin_auth"]["mode"], "bearer-token");

    for (method, uri) in [
        (Method::POST, "/v1/relations/scores-default"),
        (Method::POST, "/v1/views/scores-positive-default"),
        (Method::POST, "/v1/query-policies"),
        (Method::GET, "/v1/query-policies/standard"),
        (Method::POST, "/v1/ingest"),
        (Method::GET, "/v1/views/positive_scores_by_user/query"),
        (Method::GET, "/v1/api/scores/positive"),
        (Method::GET, "/v1/openapi.json"),
    ] {
        let missing = request_json(app.clone(), method.clone(), uri, None).await;
        assert_eq!(
            missing.0,
            StatusCode::UNAUTHORIZED,
            "missing bearer token should be rejected for {method} {uri}: {}",
            missing.1
        );

        let wrong = request_json_with_headers(
            app.clone(),
            method,
            uri,
            None,
            &[("authorization", "Bearer wrong")],
        )
        .await;
        assert_eq!(
            wrong.0,
            StatusCode::UNAUTHORIZED,
            "wrong bearer token should be rejected for {uri}: {}",
            wrong.1
        );
    }

    let authorized = request_json_with_headers(
        app.clone(),
        Method::GET,
        "/v1/openapi.json",
        None,
        &[("authorization", "Bearer secret")],
    )
    .await;
    assert_eq!(authorized.0, StatusCode::OK, "body: {}", authorized.1);

    let missing_admin = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(
        missing_admin.0,
        StatusCode::UNAUTHORIZED,
        "missing admin token body: {}",
        missing_admin.1
    );

    let missing_admin_jobs = request_json(
        app.clone(),
        Method::GET,
        "/v1/view-compile-deploy/jobs",
        None,
    )
    .await;
    assert_eq!(
        missing_admin_jobs.0,
        StatusCode::UNAUTHORIZED,
        "missing admin token jobs body: {}",
        missing_admin_jobs.1
    );

    for (token, expected_status) in [
        ("secret", StatusCode::UNAUTHORIZED),
        ("wrong", StatusCode::UNAUTHORIZED),
        ("admin-secret", StatusCode::OK),
    ] {
        let response = request_json_with_headers(
            app.clone(),
            Method::POST,
            "/v1/view-compile-deploy/run-once",
            None,
            &[("authorization", &format!("Bearer {token}"))],
        )
        .await;
        assert_eq!(
            response.0, expected_status,
            "admin route auth body for token {token}: {}",
            response.1
        );

        let jobs_response = request_json_with_headers(
            app.clone(),
            Method::GET,
            "/v1/view-compile-deploy/jobs",
            None,
            &[("authorization", &format!("Bearer {token}"))],
        )
        .await;
        assert_eq!(
            jobs_response.0, expected_status,
            "admin jobs route auth body for token {token}: {}",
            jobs_response.1
        );
    }

    let owner_report = request_json_with_headers(
        app.clone(),
        Method::GET,
        "/v1/standing-runtime/owners",
        None,
        &[("authorization", "Bearer admin-secret")],
    )
    .await;
    assert_eq!(
        owner_report.0,
        StatusCode::OK,
        "standing runtime owner report body: {}",
        owner_report.1
    );
    assert!(owner_report.1["local_owner_id"].as_str().is_some());
    assert_eq!(owner_report.1["owners"], serde_json::json!([]));

    let owner_acquire = request_json_with_headers(
        app.clone(),
        Method::POST,
        "/v1/standing-runtime/owners",
        None,
        &[("authorization", "Bearer admin-secret")],
    )
    .await;
    assert_eq!(
        owner_acquire.0,
        StatusCode::OK,
        "standing runtime owner acquire body: {}",
        owner_acquire.1
    );
    assert!(owner_acquire.1["local_owner_id"].as_str().is_some());
    assert_eq!(owner_acquire.1["outcomes"], serde_json::json!([]));
    assert_eq!(owner_acquire.1["owners"], serde_json::json!([]));
}

#[tokio::test]
async fn rest_product_admin_routes_fail_closed_when_api_auth_has_no_admin_token() {
    let (state, _temp) = api_state("admin-auth-required").await;
    let app = app(state.with_api_bearer_token("secret").unwrap());

    let response = request_json_with_headers(
        app,
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
        &[("authorization", "Bearer secret")],
    )
    .await;

    assert_eq!(
        response.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "admin route without configured admin token body: {}",
        response.1
    );
    assert!(response.1["error"]
        .as_str()
        .unwrap()
        .contains("admin_auth_required"));
}

#[tokio::test]
async fn rest_product_rejects_ingest_over_configured_row_limit() {
    let (state, _temp) = api_state("ingest-row-limit").await;
    let app = app(state.with_request_limits(1024 * 1024, 1));

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let ingest = request_json(
        app,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(ingest.1["error"].as_str().unwrap().contains("row count"));
}

#[tokio::test]
async fn rest_product_accepts_view_as_feldera_compile_pending_when_no_artifact() {
    let (state, temp) = api_state("feldera-compile-pending-view").await;
    let app = app(state);
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "scores_by_user",
            "urlPath": "/scores/by-user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "this is not valid sql"
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(view.1["query_enabled"], false);
    assert_eq!(view.1["disabled_reason"], "feldera_compile_pending");
    assert_eq!(view.1["lifecycle"]["compiler_backend"], "feldera_compiler");
    assert_eq!(view.1["lifecycle"]["compile_status"], "pending");
    assert_eq!(view.1["lifecycle"]["deployment_status"], "not_deployed");
    assert!(view.1["compile_job_id"]
        .as_str()
        .unwrap()
        .starts_with("scores_by_user:velorix-feldera-spec-sha256-v1:"));
    let spec_hash = view.1["spec_hash"].as_str().unwrap();
    let spec_hash_segment = spec_hash
        .strip_prefix("velorix-feldera-spec-sha256-v1:")
        .unwrap();
    let job_record_path = temp.path().join(format!(
        "v1/view-compile-deploy-jobs/scores_by_user/spec-sha256/{spec_hash_segment}.job.json"
    ));
    assert!(
        job_record_path.is_file(),
        "missing compile/deploy job record at {}",
        job_record_path.display()
    );
    let job_record: Value = serde_json::from_slice(&fs::read(&job_record_path).unwrap()).unwrap();
    assert_eq!(
        job_record["compiler_request"]["request_kind"],
        "feldera_standing_view_compile_request_v1"
    );
    assert_eq!(job_record["compiler_request"]["view_id"], "scores_by_user");
    assert_eq!(job_record["compiler_request"]["spec_hash"], spec_hash);
    assert_eq!(
        job_record["compiler_request"]["sql"],
        "this is not valid sql"
    );
    assert_eq!(
        job_record["compiler_request"]["input_relations"][0]["relation_name"],
        "scores"
    );
    assert_eq!(
        job_record["compiler_request"]["output_relations"][0]["relation_name"],
        "scores_by_user"
    );
    assert_eq!(
        job_record["compiler_request"]["shape"]["is_materialized"],
        true
    );

    let jobs = request_json(
        app.clone(),
        Method::GET,
        "/v1/view-compile-deploy/jobs",
        None,
    )
    .await;
    assert_eq!(jobs.0, StatusCode::OK, "jobs body: {}", jobs.1);
    assert_eq!(jobs.1["pending_jobs"], 1);
    assert_eq!(jobs.1["jobs"][0]["job_id"], view.1["compile_job_id"]);
    assert_eq!(jobs.1["jobs"][0]["view_id"], "scores_by_user");
    assert_eq!(jobs.1["jobs"][0]["spec_hash"], spec_hash);
    assert_eq!(
        jobs.1["jobs"][0]["compiler_request"]["sql"],
        "this is not valid sql"
    );

    let catalog = request_json(app.clone(), Method::GET, "/v1/views", None).await;
    assert_eq!(catalog.0, StatusCode::OK, "catalog body: {}", catalog.1);
    assert_eq!(catalog.1["views"][0]["view_id"], "scores_by_user");
    assert_eq!(
        catalog.1["views"][0]["execution_mode"],
        "feldera_compile_pending"
    );

    let openapi = request_json(app.clone(), Method::GET, "/v1/openapi.json", None).await;
    assert_eq!(openapi.0, StatusCode::OK, "openapi body: {}", openapi.1);
    assert!(!openapi.1["paths"]
        .as_object()
        .unwrap()
        .contains_key("/v1/api/scores/by-user"));

    for path in ["/v1/views/scores_by_user/query", "/v1/api/scores/by-user"] {
        let query = request_json(app.clone(), Method::GET, path, None).await;
        assert_eq!(
            query.0,
            StatusCode::SERVICE_UNAVAILABLE,
            "query body: {}",
            query.1
        );
        assert!(query.1["error"]
            .as_str()
            .unwrap()
            .contains("feldera_compile_pending"));
    }
}

#[tokio::test]
async fn rest_product_openapi_omits_removed_generic_query_route() {
    let (state, _temp) = api_state("generic-query-removed").await;
    let app = app(state);

    let ready = request_json(app.clone(), Method::GET, "/readyz", None).await;
    assert_eq!(ready.0, StatusCode::OK, "ready body: {}", ready.1);
    assert!(ready.1.get("generic_query_enabled").is_none());

    let openapi = request_json(app.clone(), Method::GET, "/v1/openapi.json", None).await;
    assert_eq!(openapi.0, StatusCode::OK, "openapi body: {}", openapi.1);
    assert!(!openapi.1["paths"]
        .as_object()
        .unwrap()
        .contains_key("/v1/query"));
}

#[tokio::test]
async fn rest_product_rejects_request_default_value_that_violates_validators() {
    let (state, _temp) = api_state("view-request-default-validation").await;
    let app = app(state);
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "scores_by_user_default_invalid",
            "urlPath": "/scores/default-invalid",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
            "sql_template": "select key_json, value_json, weight from scores_by_user_default_invalid where {{ context.params.min_sum | is_integer(min=10) }} >= 10",
            "request": [
                {
                    "fieldName": "min_sum",
                    "fieldIn": "query",
                    "type": "integer",
                    "defaultValue": 9,
                    "validators": ["integer(min=10)"]
                }
            ]
        })),
    )
    .await;

    assert_eq!(view.0, StatusCode::BAD_REQUEST, "view body: {}", view.1);
    assert!(
        view.1["error"]
            .as_str()
            .unwrap()
            .contains("parameter `min_sum` must pass is_integer"),
        "view body: {}",
        view.1
    );
}

#[tokio::test]
async fn rest_product_rejects_unbounded_query_policy_for_view_api_catalog() {
    let (state, _temp) = api_state("view-query-policy-unbounded").await;
    let app = app(state);

    let policy = request_json(
        app,
        Method::POST,
        "/v1/query-policies",
        Some(json!({
            "query_policy_id": "weak",
            "policy": {
                "max_output_rows": 1,
                "max_concurrent_queries": 1
            }
        })),
    )
    .await;

    assert_eq!(
        policy.0,
        StatusCode::BAD_REQUEST,
        "policy body: {}",
        policy.1
    );
    assert!(
        policy.1["error"]
            .as_str()
            .unwrap()
            .contains("production table scans require query policy field max_sql_bytes"),
        "policy body: {}",
        policy.1
    );
}

#[tokio::test]
async fn rest_product_enforces_query_policy_catalog_for_view_api() {
    let (state, _temp) = api_state("view-query-policy").await;
    let app = app(state);
    let catalog = scores_sum_count_relation_catalog();

    let policy = request_json(
        app.clone(),
        Method::POST,
        "/v1/query-policies",
        Some(json!({
            "query_policy_id": "one_row",
            "policy": {
                "max_sql_bytes": 16384,
                "planning_timeout_ms": 1000,
                "execution_timeout_ms": 10000,
                "max_output_rows": 1,
                "max_output_bytes": 1000000,
                "max_scan_files": 100,
                "max_scan_bytes": 134217728,
                "max_object_requests": 1000,
                "max_concurrent_queries": 1,
                "memory_limit_bytes": 536870912,
                "spill_limit_bytes": 1073741824
            }
        })),
    )
    .await;
    assert_eq!(policy.0, StatusCode::CREATED, "policy body: {}", policy.1);
    assert_eq!(policy.1["tenant_id"], "default");
    assert_eq!(policy.1["query_policy_id"], "one_row");
    assert_eq!(policy.1["policy"]["max_output_rows"], 1);
    assert_eq!(policy.1["policy"]["max_concurrent_queries"], 1);

    let fetched_policy =
        request_json(app.clone(), Method::GET, "/v1/query-policies/one_row", None).await;
    assert_eq!(
        fetched_policy.0,
        StatusCode::OK,
        "policy body: {}",
        fetched_policy.1
    );
    assert_eq!(fetched_policy.1["policy"]["max_output_rows"], 1);

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/policy-limited",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "sql_template": "select user_id, sum, count from positive_scores_by_user order by user_id",
            "query_policy_id": "one_row"
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "standing_runtime");
    assert_eq!(view.1["query_policy_id"], "one_row");

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u2", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let openapi = request_json(app.clone(), Method::GET, "/v1/openapi.json", None).await;
    assert_eq!(openapi.0, StatusCode::OK, "openapi body: {}", openapi.1);
    assert_eq!(
        openapi.1["paths"]["/v1/api/scores/policy-limited"]["get"]["x-velorix-query-policy-id"],
        "one_row"
    );

    let query = request_json(app, Method::GET, "/v1/api/scores/policy-limited", None).await;
    assert_eq!(query.0, StatusCode::BAD_REQUEST, "query body: {}", query.1);
    assert!(
        query.1["error"]
            .as_str()
            .unwrap()
            .contains("above query policy limit"),
        "query body: {}",
        query.1
    );
}

#[tokio::test]
async fn rest_product_creates_artifact_backed_view_without_dbsp_sql_shape_gate() {
    let (state, _temp) = api_state("artifact-backed-view").await;
    let app = app(state
        .with_generated_artifact_packages(["scores_by_user_generated"])
        .with_standing_program_runtime_factory(
            "scores_by_user_generated",
            FixedScoresStandingRuntimeFactory,
        ));
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "positive_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);
    assert_eq!(view.1["view_id"], "positive_scores_by_user");
    assert_eq!(view.1["execution_mode"], "standing_runtime");
    assert_eq!(
        view.1["artifact"]["artifact_id"],
        "feldera-artifact-positive-scores-by-user"
    );
    assert_eq!(
        view.1["artifact"]["generated_rust_crate_name"],
        "scores_by_user_generated"
    );
    assert_eq!(
        view.1["artifact"]["execution_status"],
        "direct_execution_enabled"
    );
    assert_eq!(
        view.1["artifact"]["execution_path"],
        "static_release_artifact"
    );
    assert_eq!(
        view.1["artifact"]["standing_program_identity"]["program_id"],
        "positive_scores_by_user"
    );
    assert_eq!(
        view.1["artifact"]["standing_program_identity"]["runtime_packages"][0]["name"],
        "scores_by_user_generated"
    );

    let detail = request_json(app, Method::GET, "/v1/views/positive_scores_by_user", None).await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(detail.1["artifact"], view.1["artifact"]);
}

#[tokio::test]
async fn rest_product_queries_artifact_backed_view_through_parameterized_api() {
    let (state, _temp) = api_state("artifact-backed-parameterized-view").await;
    let app = app(state);
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "parameterized_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "parameterized_scores_by_user",
            "urlPath": "/scores/positive/:user_id",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "request": [
                {
                    "fieldName": "user_id",
                    "fieldIn": "path",
                    "type": "string",
                    "validators": ["required", "string"]
                }
            ],
            "sql_template": "select user_id, sum, count from parameterized_scores_by_user where user_id = {{ context.params.user_id | is_required | is_string }} order by user_id",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;

    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "standing_runtime");
    let openapi = request_json(app.clone(), Method::GET, "/v1/openapi.json", None).await;
    assert_eq!(openapi.0, StatusCode::OK, "openapi body: {}", openapi.1);
    let parameters = openapi.1["paths"]["/v1/api/scores/positive/{user_id}"]["get"]["parameters"]
        .as_array()
        .unwrap();
    assert!(parameters
        .iter()
        .any(|parameter| parameter["name"] == "epoch"));
    assert!(!parameters
        .iter()
        .any(|parameter| parameter["name"] == "max_rows"));
    assert!(!parameters
        .iter()
        .any(|parameter| parameter["name"] == "page_token"));

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                    json!({ "user_id": "u2", "score": 11, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = request_json(app.clone(), Method::GET, "/v1/api/scores/positive/u1", None).await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(query.1["logical_epoch"], 3);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
    assert!(query.1.get("next_page_token").is_none());

    let query_supplied_path_param = request_json(
        app.clone(),
        Method::GET,
        "/v1/api/scores/positive/u1?user_id=u1",
        None,
    )
    .await;
    assert_eq!(
        query_supplied_path_param.0,
        StatusCode::BAD_REQUEST,
        "query-supplied path param body: {}",
        query_supplied_path_param.1
    );
    assert!(
        query_supplied_path_param.1["error"]
            .as_str()
            .unwrap()
            .contains("must be supplied by the API path"),
        "query-supplied path param body: {}",
        query_supplied_path_param.1
    );

    let direct_view_query = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/parameterized_scores_by_user/query?user_id=u1",
        None,
    )
    .await;
    assert_eq!(
        direct_view_query.0,
        StatusCode::BAD_REQUEST,
        "direct query body: {}",
        direct_view_query.1
    );
    assert!(
        direct_view_query.1["error"]
            .as_str()
            .unwrap()
            .contains("must be supplied by the promoted API path"),
        "direct query body: {}",
        direct_view_query.1
    );

    let pagination = request_json(
        app,
        Method::GET,
        "/v1/api/scores/positive/u1?max_rows=1",
        None,
    )
    .await;
    assert_eq!(
        pagination.0,
        StatusCode::BAD_REQUEST,
        "pagination body: {}",
        pagination.1
    );
    assert!(
        pagination.1["error"]
            .as_str()
            .unwrap()
            .contains("cursor pagination is not supported for templated standing runtime"),
        "pagination body: {}",
        pagination.1
    );
}

#[tokio::test]
async fn rest_product_rejects_artifact_backed_template_that_omits_path_parameter() {
    let (state, _temp) = api_state("artifact-backed-template-omits-path").await;
    let app = app(state);
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "omits_path_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "omits_path_scores_by_user",
            "urlPath": "/scores/omits-path/:user_id",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "request": [
                {
                    "fieldName": "user_id",
                    "fieldIn": "path",
                    "type": "string",
                    "validators": ["required", "string"]
                }
            ],
            "sql_template": "select user_id, sum, count from omits_path_scores_by_user order by user_id",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;

    assert_eq!(view.0, StatusCode::BAD_REQUEST, "view body: {}", view.1);
    assert!(
        view.1["error"]
            .as_str()
            .unwrap()
            .contains("must reference path parameter `user_id`"),
        "view body: {}",
        view.1
    );
}

#[tokio::test]
async fn rest_product_rejects_path_request_field_missing_from_url_path() {
    let (state, _temp) = api_state("artifact-backed-path-field-missing-url").await;
    let app = app(state);
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "path_field_missing_url_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "path_field_missing_url_scores_by_user",
            "urlPath": "/scores/path-field-missing-url",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "request": [
                {
                    "fieldName": "user_id",
                    "fieldIn": "path",
                    "type": "string",
                    "validators": ["required", "string"]
                }
            ],
            "sql_template": "select user_id, sum, count from path_field_missing_url_scores_by_user where user_id = {{ context.params.user_id | is_required | is_string }}",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;

    assert_eq!(view.0, StatusCode::BAD_REQUEST, "view body: {}", view.1);
    assert!(
        view.1["error"]
            .as_str()
            .unwrap()
            .contains("is declared as path but is not present in urlPath"),
        "view body: {}",
        view.1
    );
}

#[tokio::test]
async fn rest_product_rejects_template_filter_before_fetching_standing_snapshot() {
    let (state, _temp) = api_state("artifact-backed-template-filter-pre-snapshot").await;
    let app = app(state);
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "template_filter_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "template_filter_scores_by_user",
            "urlPath": "/scores/template-filter/:user_id",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "request": [
                {
                    "fieldName": "user_id",
                    "fieldIn": "path",
                    "type": "string",
                    "validators": ["required", "string"]
                },
                {
                    "fieldName": "min_sum",
                    "fieldIn": "query",
                    "type": "integer",
                    "validators": ["integer"]
                }
            ],
            "sql_template": "select user_id, sum, count from template_filter_scores_by_user where user_id = {{ context.params.user_id | is_required | is_string }} and {{ context.params.min_sum | is_integer(min=0) }} >= 0",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let query = request_json(
        app,
        Method::GET,
        "/v1/api/scores/template-filter/u1?min_sum=-1",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::BAD_REQUEST, "query body: {}", query.1);
    assert!(
        query.1["error"]
            .as_str()
            .unwrap()
            .contains("parameter `min_sum` must pass is_integer"),
        "query body: {}",
        query.1
    );
}

#[tokio::test]
async fn rest_product_rejects_artifact_backed_template_against_unknown_output_column() {
    let (state, _temp) = api_state("artifact-backed-template-unknown-column").await;
    let app = app(state);
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "unknown_column_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "unknown_column_scores_by_user",
            "urlPath": "/scores/unknown-column/:user_id",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "request": [
                {
                    "fieldName": "user_id",
                    "fieldIn": "path",
                    "type": "string",
                    "validators": ["required", "string"]
                }
            ],
            "sql_template": "select key_json, value_json, weight from unknown_column_scores_by_user where key_json = {{ context.params.user_id | is_required | is_string | to_json }}",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;

    assert_eq!(view.0, StatusCode::BAD_REQUEST, "view body: {}", view.1);
    assert!(
        view.1["error"].as_str().unwrap().contains("key_json"),
        "view body: {}",
        view.1
    );
}

#[tokio::test]
async fn rest_product_rejects_templated_standing_runtime_when_full_snapshot_is_unavailable() {
    let (state, _temp) = api_state("artifact-backed-template-paged-runtime").await;
    let app = app(state
        .with_generated_artifact_packages(["scores_by_user_generated"])
        .with_standing_program_runtime_factory(
            "scores_by_user_generated",
            PagedScoresStandingRuntimeFactory,
        ));
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "paged_runtime_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "paged_runtime_scores_by_user",
            "urlPath": "/scores/paged/:user_id",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "request": [
                {
                    "fieldName": "user_id",
                    "fieldIn": "path",
                    "type": "string",
                    "validators": ["required", "string"]
                }
            ],
            "sql_template": "select user_id, sum, count from paged_runtime_scores_by_user where user_id = {{ context.params.user_id | is_required | is_string }}",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![json!({ "user_id": "u1", "score": 5, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = request_json(app, Method::GET, "/v1/api/scores/paged/u1", None).await;

    assert_eq!(query.0, StatusCode::CONFLICT, "query body: {}", query.1);
    assert!(
        query.1["error"]
            .as_str()
            .unwrap()
            .contains("full snapshot is unavailable"),
        "query body: {}",
        query.1
    );
}

#[tokio::test]
async fn rest_product_rejects_artifact_backed_view_when_package_has_no_runtime_factory() {
    let (state, _temp) = api_state("artifact-backed-view-no-factory").await;
    let app = app(state.with_generated_artifact_packages(["missing_factory_generated"]));
    let catalog = scores_sum_count_relation_catalog();
    let mut artifact = artifact_for_scores_view(
        &catalog,
        "no_factory_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    artifact.generated_rust.crate_name = "missing_factory_generated".to_string();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "no_factory_scores_by_user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;

    assert_eq!(view.0, StatusCode::BAD_REQUEST, "view body: {}", view.1);
    assert!(
        view.1["error"]
            .as_str()
            .unwrap()
            .contains("standing runtime factory is not registered"),
        "view body: {}",
        view.1
    );
}

#[tokio::test]
async fn rest_product_uses_default_generated_scores_package_without_test_factory() {
    let (state, _temp) = api_state("artifact-runtime-generated-package").await;
    let app = app(state);
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "generated_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "generated_scores_by_user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                    json!({ "user_id": "u2", "score": -1, "delta": 1 }),
                    json!({ "user_id": "u3", "score": 0, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = request_json(
        app,
        Method::POST,
        "/v1/views/generated_scores_by_user/query",
        Some(json!({ "parameters": {} })),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_creates_default_scores_relation_and_generated_view() {
    let (state, _temp) = api_state("default-generated-scores-view").await;
    let app = app(state);

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );
    assert_eq!(relation.1["relation_id"], "scores");
    assert_eq!(relation.1["relation_version"], "2026-05-24.v1");

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);
    assert_eq!(view.1["view_id"], "positive_scores_by_user");
    assert_eq!(
        view.1["artifact"]["generated_rust_crate_name"],
        "scores_by_user_generated"
    );
    assert_eq!(
        view.1["artifact"]["execution_status"],
        "direct_execution_enabled"
    );

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                    json!({ "user_id": "u2", "score": -1, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let promoted = request_json(app.clone(), Method::GET, "/v1/api/scores/positive", None).await;
    assert_eq!(promoted.0, StatusCode::OK, "api body: {}", promoted.1);
    assert_eq!(
        promoted.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );

    let openapi = request_json(app.clone(), Method::GET, "/v1/openapi.json", None).await;
    assert_eq!(openapi.0, StatusCode::OK, "openapi body: {}", openapi.1);
    assert!(openapi.1["paths"]
        .as_object()
        .unwrap()
        .contains_key("/v1/api/scores/positive"));
    assert!(!openapi.1["paths"]
        .as_object()
        .unwrap()
        .contains_key("/v1/api/scores/positive/{user_id}"));

    let query = request_json(
        app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_auto_deploys_trusted_linked_generated_view_without_client_artifact() {
    let (state, _temp) = api_state("trusted-linked-generated-view").await;
    let app = app(state);

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "standing_runtime");
    assert_eq!(view.1["query_enabled"], true);
    assert_eq!(view.1["lifecycle"]["compile_status"], "success");
    assert_eq!(view.1["lifecycle"]["deployment_status"], "running");
    assert_eq!(
        view.1["artifact"]["generated_rust_crate_name"],
        "scores_by_user_generated"
    );

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                    json!({ "user_id": "u2", "score": -1, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = request_json(
        app,
        Method::GET,
        "/v1/api/scores/positive?max_rows=100",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_auto_deploys_multiple_dynamic_generated_view_apis_without_client_artifacts() {
    let (state, _temp) = api_state("dynamic-generated-view-apis").await;
    let app = app(state);

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let dynamic_views = [
        (
            "top_customer_scores",
            "/analytics/scores/top-customers",
            "select user_id, sum, count from top_customer_scores order by user_id",
        ),
        (
            "user_score_summary",
            "/analytics/users/:user_id/scores",
            "select user_id, sum, count from user_score_summary where user_id = {{ context.params.user_id | is_required | is_string }} order by user_id",
        ),
    ];
    let mut generated_artifact_ids = Vec::new();
    let mut generated_artifact_hashes = Vec::new();
    for (view_id, url_path, sql_template) in dynamic_views {
        let request = if view_id == "user_score_summary" {
            json!([
                {
                    "fieldName": "user_id",
                    "fieldIn": "path",
                    "type": "string",
                    "validators": ["required", "string"]
                }
            ])
        } else {
            json!([])
        };
        let view = request_json(
            app.clone(),
            Method::POST,
            "/v1/views",
            Some(json!({
                "view_id": view_id,
                "urlPath": url_path,
                "input_relation_id": "scores",
                "input_relation_version": "2026-05-24.v1",
                "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
                "request": request,
                "sql_template": sql_template,
                "response_formats": ["json"]
            })),
        )
        .await;
        assert_eq!(
            view.0,
            StatusCode::CREATED,
            "dynamic view {view_id} body: {}",
            view.1
        );
        assert_eq!(view.1["view_id"], view_id);
        assert_eq!(view.1["execution_mode"], "standing_runtime");
        assert_eq!(view.1["query_enabled"], true);
        assert_eq!(
            view.1["artifact"]["generated_rust_crate_name"],
            "scores_by_user_generated"
        );
        assert_eq!(
            view.1["artifact"]["standing_program_identity"]["program_id"],
            view_id
        );
        generated_artifact_ids.push(view.1["artifact"]["artifact_id"].clone());
        generated_artifact_hashes.push(view.1["artifact"]["artifact_hash"].clone());
    }
    assert_ne!(generated_artifact_ids[0], generated_artifact_ids[1]);
    assert_eq!(generated_artifact_hashes[0], generated_artifact_hashes[1]);

    let unsupported_view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "all_customer_scores",
            "urlPath": "/analytics/scores/all",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score >= 0 group by user_id",
            "sql_template": "select user_id, sum, count from all_customer_scores order by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(
        unsupported_view.0,
        StatusCode::ACCEPTED,
        "unsupported view body: {}",
        unsupported_view.1
    );
    assert_eq!(
        unsupported_view.1["execution_mode"],
        "feldera_compile_pending"
    );
    assert_eq!(unsupported_view.1["query_enabled"], false);
    assert!(unsupported_view.1.get("artifact").is_none());

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                    json!({ "user_id": "u2", "score": 11, "delta": 1 }),
                    json!({ "user_id": "u3", "score": -100, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let all_scores = request_json(
        app.clone(),
        Method::GET,
        "/v1/api/analytics/scores/top-customers",
        None,
    )
    .await;
    assert_eq!(all_scores.0, StatusCode::OK, "query body: {}", all_scores.1);
    assert_eq!(
        all_scores.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 },
            { "user_id": "u2", "sum": 11, "count": 1 }
        ])
    );

    let user_scores = request_json(
        app.clone(),
        Method::GET,
        "/v1/api/analytics/users/u2/scores",
        None,
    )
    .await;
    assert_eq!(
        user_scores.0,
        StatusCode::OK,
        "user query body: {}",
        user_scores.1
    );
    assert_eq!(
        user_scores.1["rows"],
        json!([
            { "user_id": "u2", "sum": 11, "count": 1 }
        ])
    );

    let openapi = request_json(app.clone(), Method::GET, "/v1/openapi.json", None).await;
    assert_eq!(openapi.0, StatusCode::OK, "openapi body: {}", openapi.1);
    let openapi_paths = openapi.1["paths"].as_object().unwrap();
    assert!(openapi_paths.contains_key("/v1/api/analytics/scores/top-customers"));
    assert!(openapi_paths.contains_key("/v1/api/analytics/users/{user_id}/scores"));
    assert!(!openapi_paths.contains_key("/v1/api/analytics/scores/all"));

    let direct_query = request_json(
        app,
        Method::GET,
        "/v1/views/top_customer_scores/query",
        None,
    )
    .await;
    assert_eq!(
        direct_query.0,
        StatusCode::OK,
        "direct query body: {}",
        direct_query.1
    );
    assert_eq!(direct_query.1["logical_epoch"], json!(4));
}

#[tokio::test]
async fn rest_product_keeps_generated_view_pending_when_linked_package_is_disabled() {
    let (state, _temp) = api_state("trusted-linked-generated-view-disabled").await;
    let app = app(state.with_generated_artifact_packages(std::iter::empty::<&str>()));

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(view.1["query_enabled"], false);
    assert_eq!(view.1["disabled_reason"], "feldera_compile_pending");

    let query = request_json(
        app,
        Method::GET,
        "/v1/api/scores/positive?max_rows=100",
        None,
    )
    .await;
    assert_eq!(
        query.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "query body: {}",
        query.1
    );
    assert!(query.1["error"]
        .as_str()
        .unwrap()
        .contains("feldera_compile_pending"));
}

#[tokio::test]
async fn rest_product_worker_activates_pending_generated_view_when_package_becomes_available() {
    let (state, store) = api_state_memory("trusted-linked-generated-view-worker").await;
    let initial_app = app(state.with_generated_artifact_packages(std::iter::empty::<&str>()));

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let pending_ingest = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                    json!({ "user_id": "u2", "score": -1, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        pending_ingest.0,
        StatusCode::CREATED,
        "pending ingest body: {}",
        pending_ingest.1
    );

    let pending_query =
        request_json(initial_app, Method::GET, "/v1/api/scores/positive", None).await;
    assert_eq!(
        pending_query.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "pending query body: {}",
        pending_query.1
    );

    let restarted_state =
        api_state_from_store("trusted-linked-generated-view-worker", Arc::clone(&store)).await;
    let worker_app = app(restarted_state);
    let worker = request_json(
        worker_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(worker.0, StatusCode::OK, "worker body: {}", worker.1);
    assert_eq!(worker.1["pending_jobs"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["activated"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["failed"], 0, "worker body: {}", worker.1);
    assert_eq!(worker.1["outcomes"][0]["status"], "activated");

    let detail = request_json(
        worker_app.clone(),
        Method::GET,
        "/v1/views/positive_scores_by_user",
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(detail.1["execution_mode"], "standing_runtime");
    assert_eq!(detail.1["query_enabled"], true);
    assert_eq!(detail.1["lifecycle"]["compile_status"], "success");
    assert_eq!(detail.1["lifecycle"]["deployment_status"], "running");
    assert!(detail.1.get("compile_job_id").is_none());

    let query = request_json(
        worker_app.clone(),
        Method::GET,
        "/v1/api/scores/positive",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );

    let post_activation_ingest = request_json(
        worker_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 3,
                rows: vec![
                    json!({ "user_id": "u1", "score": 3, "delta": 1 }),
                    json!({ "user_id": "u3", "score": 11, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        post_activation_ingest.0,
        StatusCode::CREATED,
        "post-activation ingest body: {}",
        post_activation_ingest.1
    );

    let query = request_json(worker_app, Method::GET, "/v1/api/scores/positive", None).await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 15, "count": 3 },
            { "user_id": "u3", "sum": 11, "count": 1 }
        ])
    );
}

#[tokio::test]
async fn rest_product_complete_compile_deploy_job_activates_pending_view_from_artifact() {
    let (state, store) = api_state_memory("trusted-linked-generated-view-complete").await;
    let initial_app = app(state.with_generated_artifact_packages(std::iter::empty::<&str>()));

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let pending_ingest = request_json(
        initial_app,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                    json!({ "user_id": "u2", "score": -1, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        pending_ingest.0,
        StatusCode::CREATED,
        "pending ingest body: {}",
        pending_ingest.1
    );

    let restarted_state =
        api_state_from_store("trusted-linked-generated-view-complete", Arc::clone(&store)).await;
    let completion_app = app(restarted_state);
    let artifact = artifact_for_scores_view(
        &scores_sum_count_relation_catalog(),
        "positive_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    let complete = request_json(
        completion_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(json!({
            "spec_hash": view.1["spec_hash"],
            "artifact": artifact,
        })),
    )
    .await;
    assert_eq!(complete.0, StatusCode::OK, "complete body: {}", complete.1);
    assert_eq!(complete.1["execution_mode"], "standing_runtime");
    assert_eq!(complete.1["query_enabled"], true);
    assert_eq!(complete.1["lifecycle"]["compile_status"], "success");
    assert_eq!(complete.1["lifecycle"]["deployment_status"], "running");

    let query = request_json(completion_app, Method::GET, "/v1/api/scores/positive", None).await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_complete_compile_deploy_job_rejects_wrong_spec_hash() {
    let (state, _store) =
        api_state_memory("trusted-linked-generated-view-complete-wrong-hash").await;
    let app = app(state.with_generated_artifact_packages(std::iter::empty::<&str>()));

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let artifact = artifact_for_scores_view(
        &scores_sum_count_relation_catalog(),
        "positive_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    let complete = request_json(
        app,
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(json!({
            "spec_hash": "velorix-feldera-spec-sha256-v1:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "artifact": artifact,
        })),
    )
    .await;
    assert_eq!(
        complete.0,
        StatusCode::CONFLICT,
        "complete body: {}",
        complete.1
    );
    assert!(complete.1["error"].as_str().unwrap().contains("spec hash"));
}

#[tokio::test]
async fn rest_product_complete_compile_deploy_job_rejects_artifact_schema_mismatch() {
    let (state, store) = api_state_memory("trusted-linked-generated-view-complete-schema").await;
    let initial_app = app(state.with_generated_artifact_packages(std::iter::empty::<&str>()));

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let restarted_state = api_state_from_store(
        "trusted-linked-generated-view-complete-schema",
        Arc::clone(&store),
    )
    .await;
    let completion_app = app(restarted_state);
    let mut artifact = artifact_for_scores_view(
        &scores_sum_count_relation_catalog(),
        "positive_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    artifact.output_schemas[0].columns[1].name = "total".to_string();
    let complete = request_json(
        completion_app,
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(json!({
            "spec_hash": view.1["spec_hash"],
            "artifact": artifact,
        })),
    )
    .await;
    assert_eq!(
        complete.0,
        StatusCode::BAD_REQUEST,
        "complete body: {}",
        complete.1
    );
    assert!(complete.1["error"].as_str().unwrap().contains("schema"));
}

#[tokio::test]
async fn rest_product_complete_compile_deploy_job_rejects_active_view() {
    let (state, _store) = api_state_memory("trusted-linked-generated-view-complete-active").await;
    let app = app(state);

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "standing_runtime");

    let artifact = artifact_for_scores_view(
        &scores_sum_count_relation_catalog(),
        "positive_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    let complete = request_json(
        app,
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(json!({
            "spec_hash": view.1["spec_hash"],
            "artifact": artifact,
        })),
    )
    .await;
    assert_eq!(
        complete.0,
        StatusCode::CONFLICT,
        "complete body: {}",
        complete.1
    );
    assert!(complete.1["error"]
        .as_str()
        .unwrap()
        .contains("not waiting"));
}

#[tokio::test]
async fn rest_product_complete_compile_deploy_job_rejects_missing_pending_job() {
    let (state, store) =
        api_state_memory("trusted-linked-generated-view-complete-missing-job").await;
    let app = app(state.with_generated_artifact_packages(std::iter::empty::<&str>()));

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    let spec_hash = view.1["spec_hash"].as_str().unwrap();
    let job_registry = ViewCompileDeployJobRegistry::new(Arc::clone(&store));
    let object_key = job_registry
        .object_key("positive_scores_by_user", spec_hash)
        .unwrap();
    store
        .delete(&Path::from(object_key.as_str()))
        .await
        .unwrap();

    let artifact = artifact_for_scores_view(
        &scores_sum_count_relation_catalog(),
        "positive_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    let complete = request_json(
        app,
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(json!({
            "spec_hash": spec_hash,
            "artifact": artifact,
        })),
    )
    .await;
    assert_eq!(
        complete.0,
        StatusCode::CONFLICT,
        "complete body: {}",
        complete.1
    );
    assert!(complete.1["error"]
        .as_str()
        .unwrap()
        .contains("does not exist"));
}

#[tokio::test]
async fn rest_product_complete_compile_deploy_job_rejects_mismatched_compiler_request() {
    let (state, temp) = api_state("trusted-linked-generated-view-complete-stale-job").await;
    let app = app(state.with_generated_artifact_packages(std::iter::empty::<&str>()));

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    let spec_hash = view.1["spec_hash"].as_str().unwrap();
    let job_registry = ViewCompileDeployJobRegistry::new(Arc::new(
        LocalFileSystem::new_with_prefix(temp.path()).unwrap(),
    ));
    let object_key = job_registry
        .object_key("positive_scores_by_user", spec_hash)
        .unwrap();
    let path = temp.path().join(object_key.as_str());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "job_id": view_compile_deploy_job_id("positive_scores_by_user", spec_hash),
            "view_id": "positive_scores_by_user",
            "spec_hash": spec_hash,
            "compiler_backend": "feldera_compiler",
            "compiler_request": {
                "request_kind": "feldera_standing_view_compile_request_v1",
                "view_id": "positive_scores_by_user",
                "spec_hash": spec_hash,
                "sql": "select user_id from scores",
                "input_relations": artifact_for_scores_view(
                    &scores_sum_count_relation_catalog(),
                    "positive_scores_by_user",
                    "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
                ).input_schemas,
                "output_relations": artifact_for_scores_view(
                    &scores_sum_count_relation_catalog(),
                    "positive_scores_by_user",
                    "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
                ).output_schemas,
                "shape": {
                    "is_materialized": true,
                    "multi_input": false,
                    "multi_output": false
                }
            },
            "compile_status": "pending",
            "deployment_status": "not_deployed"
        }))
        .unwrap(),
    )
    .unwrap();

    let artifact = artifact_for_scores_view(
        &scores_sum_count_relation_catalog(),
        "positive_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    let complete = request_json(
        app,
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(json!({
            "spec_hash": spec_hash,
            "artifact": artifact,
        })),
    )
    .await;
    assert_eq!(
        complete.0,
        StatusCode::CONFLICT,
        "complete body: {}",
        complete.1
    );
    assert!(complete.1["error"]
        .as_str()
        .unwrap()
        .contains("compiler_request"));
}

#[tokio::test]
async fn rest_product_complete_compile_deploy_job_rejects_unlinked_runtime_package() {
    let (state, _store) = api_state_memory("trusted-linked-generated-view-complete-unlinked").await;
    let app = app(state.with_generated_artifact_packages(std::iter::empty::<&str>()));

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let artifact = artifact_for_scores_view(
        &scores_sum_count_relation_catalog(),
        "positive_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    let complete = request_json(
        app,
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(json!({
            "spec_hash": view.1["spec_hash"],
            "artifact": artifact,
        })),
    )
    .await;
    assert_eq!(
        complete.0,
        StatusCode::BAD_REQUEST,
        "complete body: {}",
        complete.1
    );
    assert!(complete.1["error"]
        .as_str()
        .unwrap()
        .contains("is not registered"));
}

#[tokio::test]
async fn rest_product_complete_compile_deploy_job_rejects_unknown_request_fields() {
    let (state, _store) = api_state_memory("trusted-linked-generated-view-complete-unknown").await;
    let app = app(state);
    let artifact = artifact_for_scores_view(
        &scores_sum_count_relation_catalog(),
        "positive_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    let response = request_raw_json(
        app,
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        json!({
            "spec_hash": "velorix-feldera-spec-sha256-v1:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "artifact": artifact,
            "unexpected": true
        }),
    )
    .await;

    assert_eq!(response.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(response.1.contains("unknown field"));
}

#[tokio::test]
async fn rest_product_query_rejects_standing_runtime_before_lifecycle_is_running() {
    let (state, store) = api_state_memory("standing-runtime-deploying-query").await;
    let app = app(state);

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "standing_runtime");
    assert_eq!(view.1["query_enabled"], true);

    MaterializedViewRegistry::new(Arc::clone(&store))
        .update_standing_runtime_lifecycle(
            "positive_scores_by_user",
            view.1["spec_hash"].as_str().unwrap(),
            MaterializedViewLifecycleStatus::standing_runtime_deploying(Some(
                "test deploy in progress".to_string(),
            )),
        )
        .await
        .unwrap();

    let api_query = request_json(app.clone(), Method::GET, "/v1/api/scores/positive", None).await;
    assert_eq!(
        api_query.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "api query body: {}",
        api_query.1
    );
    assert!(api_query.1["error"]
        .as_str()
        .unwrap()
        .contains("standing_runtime_not_deployed"));

    let direct_query = request_json(
        app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(
        direct_query.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "direct query body: {}",
        direct_query.1
    );
    assert!(direct_query.1["error"]
        .as_str()
        .unwrap()
        .contains("standing_runtime_not_deployed"));
}

#[tokio::test]
async fn rest_product_worker_activates_catalog_backed_orders_sum_count_view() {
    let (state, store) = api_state_memory("catalog-backed-orders-view-worker").await;
    let initial_app = app(state);
    let catalog = orders_sum_count_relation_catalog();

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "orders_by_account",
            "urlPath": "/orders/by-account",
            "input_relation_id": "orders",
            "input_relation_version": "2026-06-06.v1",
            "sql": "select account_id, sum(amount) as sum, count(*) as count from orders group by account_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(view.1["query_enabled"], false);
    assert_eq!(view.1["disabled_reason"], "feldera_compile_pending");

    let pending_ingest = request_json(
        initial_app,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "orders".to_string(),
                relation_version: "2026-06-06.v1".to_string(),
                stream_id: "orders".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "account_id": "acct-a", "amount": 10, "delta": 1 }),
                    json!({ "account_id": "acct-a", "amount": 4, "delta": 1 }),
                    json!({ "account_id": "acct-b", "amount": 7, "delta": 1 }),
                    json!({ "account_id": "acct-a", "amount": 4, "delta": -1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        pending_ingest.0,
        StatusCode::CREATED,
        "pending ingest body: {}",
        pending_ingest.1
    );

    let restarted_state =
        api_state_from_store("catalog-backed-orders-view-worker", Arc::clone(&store)).await;
    let worker_app = app(restarted_state);
    let worker = request_json(
        worker_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(worker.0, StatusCode::OK, "worker body: {}", worker.1);
    assert_eq!(worker.1["pending_jobs"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["activated"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["skipped"], 0, "worker body: {}", worker.1);
    assert_eq!(worker.1["failed"], 0, "worker body: {}", worker.1);

    let detail = request_json(
        worker_app.clone(),
        Method::GET,
        "/v1/views/orders_by_account",
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(detail.1["execution_mode"], "standing_runtime");
    assert_eq!(detail.1["query_enabled"], true);

    let query = request_json(
        worker_app.clone(),
        Method::GET,
        "/v1/api/orders/by-account",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "account_id": "acct-a", "sum": 10, "count": 1 },
            { "account_id": "acct-b", "sum": 7, "count": 1 }
        ])
    );

    let post_activation_ingest = request_json(
        worker_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "orders".to_string(),
                relation_version: "2026-06-06.v1".to_string(),
                stream_id: "orders".to_string(),
                partition_id: 0,
                start_offset_inclusive: 4,
                rows: vec![json!({ "account_id": "acct-b", "amount": 2, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        post_activation_ingest.0,
        StatusCode::CREATED,
        "post-activation ingest body: {}",
        post_activation_ingest.1
    );

    let query = request_json(
        worker_app,
        Method::GET,
        "/v1/views/orders_by_account/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "account_id": "acct-a", "sum": 10, "count": 1 },
            { "account_id": "acct-b", "sum": 9, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_worker_keeps_unsupported_catalog_view_pending() {
    let (state, _store) = api_state_memory("catalog-backed-unsupported-view-worker").await;
    let app = app(state);
    let catalog = orders_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "large_orders_by_account",
            "urlPath": "/orders/large-by-account",
            "input_relation_id": "orders",
            "input_relation_version": "2026-06-06.v1",
            "sql": "select account_id, sum(amount) as sum, count(*) as count from orders where amount > 0 group by account_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let worker = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(worker.0, StatusCode::OK, "worker body: {}", worker.1);
    assert_eq!(worker.1["pending_jobs"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["activated"], 0, "worker body: {}", worker.1);
    assert_eq!(worker.1["skipped"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["failed"], 0, "worker body: {}", worker.1);
    assert_eq!(worker.1["outcomes"][0]["status"], "skipped");

    let detail = request_json(app, Method::GET, "/v1/views/large_orders_by_account", None).await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(detail.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(detail.1["query_enabled"], false);
}

#[tokio::test]
async fn rest_product_worker_activates_decimal_value_date_key_sum_count_view() {
    let (state, store) = api_state_memory("catalog-backed-decimal-date-view-worker").await;
    let initial_app = app(state);
    let catalog = daily_revenue_sum_count_relation_catalog();

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "daily_revenue_by_date",
            "urlPath": "/daily-revenue/by-date",
            "input_relation_id": "daily_revenue",
            "input_relation_version": "2026-06-06.v1",
            "sql": "select business_date, sum(amount) as sum, count(*) as count from daily_revenue group by business_date",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let ingest = request_json(
        initial_app,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "daily_revenue".to_string(),
                relation_version: "2026-06-06.v1".to_string(),
                stream_id: "daily_revenue".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "business_date": 20510, "amount": "10.25", "row_weight": 1 }),
                    json!({ "business_date": 20510, "amount": "1.25", "row_weight": 1 }),
                    json!({ "business_date": 20510, "amount": "1.25", "row_weight": -1 }),
                    json!({ "business_date": 20511, "amount": "2.50", "row_weight": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let restarted_state = api_state_from_store(
        "catalog-backed-decimal-date-view-worker",
        Arc::clone(&store),
    )
    .await;
    let worker_app = app(restarted_state);
    let worker = request_json(
        worker_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(worker.0, StatusCode::OK, "worker body: {}", worker.1);
    assert_eq!(worker.1["activated"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["failed"], 0, "worker body: {}", worker.1);

    let query = request_json(
        worker_app,
        Method::GET,
        "/v1/api/daily-revenue/by-date",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "business_date": 20510, "sum": "10.25", "count": 1 },
            { "business_date": 20511, "sum": "2.50", "count": 1 }
        ])
    );
}

#[tokio::test]
async fn rest_product_worker_keeps_float_value_catalog_view_pending() {
    let (state, _store) = api_state_memory("catalog-backed-float-value-view-worker").await;
    let app = app(state);
    let catalog = float_value_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "float_orders_by_account",
            "urlPath": "/float-orders/by-account",
            "input_relation_id": "float_orders",
            "input_relation_version": "2026-06-06.v1",
            "sql": "select account_id, sum(amount) as sum, count(*) as count from float_orders group by account_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let worker = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(worker.0, StatusCode::OK, "worker body: {}", worker.1);
    assert_eq!(worker.1["pending_jobs"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["activated"], 0, "worker body: {}", worker.1);
    assert_eq!(worker.1["skipped"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["failed"], 0, "worker body: {}", worker.1);

    let detail = request_json(app, Method::GET, "/v1/views/float_orders_by_account", None).await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(detail.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(detail.1["query_enabled"], false);
}

#[tokio::test]
async fn rest_product_worker_skips_pending_job_when_compiler_request_differs_from_active_spec() {
    let (state, store) =
        api_state_memory("trusted-linked-generated-view-compiler-request-mismatch").await;
    let initial_app = app(state.with_generated_artifact_packages(std::iter::empty::<&str>()));

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    let spec_hash = view.1["spec_hash"].as_str().unwrap();
    let spec_hash_segment = spec_hash
        .strip_prefix("velorix-feldera-spec-sha256-v1:")
        .unwrap();
    let job_record_path = format!(
        "v1/view-compile-deploy-jobs/positive_scores_by_user/spec-sha256/{spec_hash_segment}.job.json"
    );
    let job_path = Path::from(job_record_path);
    let mut job_record: Value =
        serde_json::from_slice(&store.get(&job_path).await.unwrap().bytes().await.unwrap())
            .unwrap();
    job_record["compiler_request"]["sql"] = json!("select user_id from scores");
    store
        .put(
            &job_path,
            serde_json::to_vec_pretty(&job_record).unwrap().into(),
        )
        .await
        .unwrap();

    let restarted_state = api_state_from_store(
        "trusted-linked-generated-view-compiler-request-mismatch",
        Arc::clone(&store),
    )
    .await;
    let worker_app = app(restarted_state);
    let worker = request_json(
        worker_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(worker.0, StatusCode::OK, "worker body: {}", worker.1);
    assert_eq!(worker.1["pending_jobs"], 1);
    assert_eq!(worker.1["activated"], 0);
    assert_eq!(worker.1["skipped"], 1);
    assert_eq!(worker.1["failed"], 0);
    assert_eq!(worker.1["outcomes"][0]["status"], "skipped");
    assert!(worker.1["outcomes"][0]["reason"]
        .as_str()
        .unwrap()
        .contains("compiler_request does not match"));

    let detail = request_json(
        worker_app,
        Method::GET,
        "/v1/views/positive_scores_by_user",
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(detail.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(detail.1["query_enabled"], false);
}

#[tokio::test]
async fn rest_product_worker_activates_pending_positive_scores_descriptor_alias() {
    let (state, store) = api_state_memory("trusted-linked-generated-view-pending-alias").await;
    let initial_app = app(state.with_generated_artifact_packages(std::iter::empty::<&str>()));

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "pending_scores_by_user",
            "urlPath": "/pending/scores/by-user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let restarted_state = api_state_from_store(
        "trusted-linked-generated-view-pending-alias",
        Arc::clone(&store),
    )
    .await;
    let worker_app = app(restarted_state);
    let worker = request_json(
        worker_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(worker.0, StatusCode::OK, "worker body: {}", worker.1);
    assert_eq!(worker.1["pending_jobs"], 1);
    assert_eq!(worker.1["activated"], 1);
    assert_eq!(worker.1["skipped"], 0);
    assert_eq!(worker.1["failed"], 0);

    let ingest = request_json(
        worker_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                    json!({ "user_id": "u2", "score": -1, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = request_json(
        worker_app,
        Method::GET,
        "/v1/views/pending_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_worker_does_not_activate_pending_alias_with_unfiltered_sql() {
    let (state, store) =
        api_state_memory("trusted-linked-generated-view-pending-alias-unfiltered").await;
    let initial_app = app(state.with_generated_artifact_packages(std::iter::empty::<&str>()));

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "pending_scores_by_user",
            "urlPath": "/pending/scores/by-user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let restarted_state = api_state_from_store(
        "trusted-linked-generated-view-pending-alias-unfiltered",
        Arc::clone(&store),
    )
    .await;
    let worker = request_json(
        app(restarted_state),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(worker.0, StatusCode::OK, "worker body: {}", worker.1);
    assert_eq!(worker.1["pending_jobs"], 1);
    assert_eq!(worker.1["activated"], 0);
    assert_eq!(worker.1["skipped"], 1);
    assert_eq!(worker.1["failed"], 0);
    assert!(worker.1["outcomes"][0]["reason"]
        .as_str()
        .unwrap()
        .contains("no trusted generated descriptor"));
}

#[tokio::test]
async fn rest_product_creates_multi_replica_positive_scores_descriptor_alias() {
    let (state, _store) =
        api_state_memory("trusted-linked-generated-view-multi-replica-alias").await;
    let app = app(state);

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "multi_replica_positive_scores_by_user",
            "urlPath": "/multi-replica/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "standing_runtime");
    assert_eq!(view.1["query_enabled"], true);

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                    json!({ "user_id": "u2", "score": -4, "delta": 1 }),
                    json!({ "user_id": "u3", "score": 0, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = request_json(
        app,
        Method::GET,
        "/v1/views/multi_replica_positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_ingest_restores_active_artifact_view_runtime_on_new_replica() {
    let (state, store) =
        api_state_memory("trusted-linked-generated-view-new-replica-restore").await;
    let first_replica = app(state);

    let relation = request_json(
        first_replica.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        first_replica,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "pending_scores_by_user",
            "urlPath": "/pending/scores/by-user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let second_replica_state = api_state_from_store(
        "trusted-linked-generated-view-new-replica-restore",
        Arc::clone(&store),
    )
    .await;
    let second_replica = app(second_replica_state);
    let ingest = request_json(
        second_replica.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![json!({ "user_id": "u1", "score": 5, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = request_json(
        second_replica,
        Method::GET,
        "/v1/views/pending_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 5, "count": 1 }
        ])
    );
}

#[tokio::test]
async fn rest_product_query_restores_active_artifact_view_runtime_on_new_replica() {
    let (state, store) = api_state_memory("trusted-linked-generated-view-new-replica-query").await;
    let first_replica = app(state);

    let relation = request_json(
        first_replica.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        first_replica.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "pending_scores_by_user",
            "urlPath": "/pending/scores/by-user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        first_replica,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![json!({ "user_id": "u1", "score": 5, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let second_replica_state = api_state_from_store(
        "trusted-linked-generated-view-new-replica-query",
        Arc::clone(&store),
    )
    .await;
    let query = request_json(
        app(second_replica_state),
        Method::GET,
        "/v1/views/pending_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 5, "count": 1 }
        ])
    );
}

#[tokio::test]
async fn rest_product_worker_repairs_missing_pending_job_before_activation() {
    let (state, store) = api_state_memory("trusted-linked-generated-view-missing-job").await;
    let initial_app = app(state.with_generated_artifact_packages(std::iter::empty::<&str>()));

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    let spec_hash = view.1["spec_hash"].as_str().unwrap();
    let spec_hash_segment = spec_hash
        .strip_prefix("velorix-feldera-spec-sha256-v1:")
        .unwrap();
    let job_record_path = format!(
        "v1/view-compile-deploy-jobs/positive_scores_by_user/spec-sha256/{spec_hash_segment}.job.json"
    );
    store.delete(&Path::from(job_record_path)).await.unwrap();

    let restarted_state = api_state_from_store(
        "trusted-linked-generated-view-missing-job",
        Arc::clone(&store),
    )
    .await;
    let worker_app = app(restarted_state);
    let worker = request_json(
        worker_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(worker.0, StatusCode::OK, "worker body: {}", worker.1);
    assert_eq!(worker.1["pending_jobs"], 1);
    assert_eq!(worker.1["activated"], 1);
    assert_eq!(worker.1["failed"], 0);

    let detail = request_json(
        worker_app,
        Method::GET,
        "/v1/views/positive_scores_by_user",
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(detail.1["execution_mode"], "standing_runtime");
    assert_eq!(detail.1["query_enabled"], true);
}

#[tokio::test]
async fn rest_product_worker_repairs_stale_pending_job_for_already_active_view() {
    let (state, store) = api_state_memory("trusted-linked-generated-view-stale-job").await;
    let app = app(state);

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);
    let spec_hash = view.1["spec_hash"].as_str().unwrap();
    let spec_hash_segment = spec_hash
        .strip_prefix("velorix-feldera-spec-sha256-v1:")
        .unwrap();
    let job_record_path = format!(
        "v1/view-compile-deploy-jobs/positive_scores_by_user/spec-sha256/{spec_hash_segment}.job.json"
    );
    store
        .put(
            &Path::from(job_record_path),
            serde_json::to_vec_pretty(&json!({
                "schema_version": 1,
                "job_id": format!("positive_scores_by_user:{spec_hash}"),
                "view_id": "positive_scores_by_user",
                "spec_hash": spec_hash,
                "compiler_backend": "feldera_compiler",
                "compile_status": "pending",
                "deployment_status": "not_deployed",
                "message": "simulated stale pending job"
            }))
            .unwrap()
            .into(),
        )
        .await
        .unwrap();

    let worker = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(worker.0, StatusCode::OK, "worker body: {}", worker.1);
    assert_eq!(worker.1["pending_jobs"], 1);
    assert_eq!(worker.1["skipped"], 1);
    assert_eq!(worker.1["failed"], 0);
    assert_eq!(worker.1["outcomes"][0]["status"], "duplicate");

    let next = request_json(app, Method::POST, "/v1/view-compile-deploy/run-once", None).await;
    assert_eq!(next.0, StatusCode::OK, "next worker body: {}", next.1);
    assert_eq!(next.1["pending_jobs"], 0);
}

#[tokio::test]
async fn rest_product_pages_default_generated_view_query_with_cursor_parameters() {
    let (state, _temp) = api_state("default-scores-pagination").await;
    let app = app(state);

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u2", "score": 9, "delta": 1 }),
                    json!({ "user_id": "u3", "score": 13, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let first = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/positive_scores_by_user/query?max_rows=2&epoch=3",
        None,
    )
    .await;
    assert_eq!(first.0, StatusCode::OK, "first query body: {}", first.1);
    assert_eq!(first.1["logical_epoch"], 3);
    assert_eq!(
        first.1["rows"],
        json!([
            { "user_id": "u1", "sum": 5, "count": 1 },
            { "user_id": "u2", "sum": 9, "count": 1 }
        ])
    );
    assert_eq!(first.1["next_page_token"], "u2");

    let second = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/positive_scores_by_user/query?max_rows=2&page_token=u2&epoch=3",
        None,
    )
    .await;
    assert_eq!(second.0, StatusCode::OK, "second query body: {}", second.1);
    assert_eq!(
        second.1["rows"],
        json!([
            { "user_id": "u3", "sum": 13, "count": 1 }
        ])
    );
    assert!(second.1.get("next_page_token").is_none());

    let stale_epoch = request_json(
        app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query?epoch=0",
        None,
    )
    .await;
    assert_eq!(
        stale_epoch.0,
        StatusCode::BAD_REQUEST,
        "stale epoch body: {}",
        stale_epoch.1
    );
    assert!(stale_epoch.1["error"]
        .as_str()
        .unwrap()
        .contains("committed epoch 0 is unavailable"));
}

#[tokio::test]
async fn rest_product_routes_artifact_backed_view_ingest_and_query_through_standing_runtime() {
    let (state, _temp) = api_state("artifact-runtime-view").await;
    let state = state
        .with_generated_artifact_packages(["scores_by_user_generated"])
        .with_standing_program_runtime_factory(
            "scores_by_user_generated",
            FixedScoresStandingRuntimeFactory,
        );
    let app = app(state);
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "runtime_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "runtime_scores_by_user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                    json!({ "user_id": "u2", "score": 11, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = request_json(
        app,
        Method::POST,
        "/v1/views/runtime_scores_by_user/query",
        Some(json!({ "parameters": {} })),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_rejects_standing_runtime_parameters_without_sql_template() {
    let (state, _temp) = api_state("artifact-runtime-params-no-template").await;
    let state = state
        .with_generated_artifact_packages(["scores_by_user_generated"])
        .with_standing_program_runtime_factory(
            "scores_by_user_generated",
            FixedScoresStandingRuntimeFactory,
        );
    let app = app(state);
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "runtime_params_without_template_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "runtime_params_without_template_scores_by_user",
            "urlPath": "/scores/no-template/:user_id",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "request": [
                {
                    "fieldName": "user_id",
                    "fieldIn": "path",
                    "type": "string",
                    "validators": ["required", "string"]
                }
            ],
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::BAD_REQUEST, "view body: {}", view.1);
    assert!(
        view.1["error"]
            .as_str()
            .unwrap()
            .contains("has request parameters but no sql_template"),
        "view body: {}",
        view.1
    );
}

#[tokio::test]
async fn rest_product_rejects_artifact_backed_query_when_standing_runtime_is_unavailable() {
    let (initial_state, temp) = api_state("artifact-runtime-missing-on-query").await;
    let initial_state = initial_state
        .with_generated_artifact_packages(["runtime_missing_generated"])
        .with_standing_program_runtime_factory(
            "runtime_missing_generated",
            FixedScoresStandingRuntimeFactory,
        );
    let initial_app = app(initial_state);
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "runtime_missing_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    let mut artifact = artifact;
    artifact.generated_rust.crate_name = "runtime_missing_generated".to_string();

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "runtime_missing_scores_by_user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let restarted_without_runtime =
        api_state_from_path("artifact-runtime-missing-on-query", temp.path()).await;
    let restarted_app = app(restarted_without_runtime);

    let ingest = request_json(
        restarted_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![json!({ "user_id": "u1", "score": 5, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        ingest.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "ingest body: {}",
        ingest.1
    );
    assert!(
        ingest.1["error"]
            .as_str()
            .unwrap()
            .contains("standing runtime is unavailable for active artifact-backed view"),
        "ingest body: {}",
        ingest.1
    );

    let query = request_json(
        restarted_app,
        Method::POST,
        "/v1/views/runtime_missing_scores_by_user/query",
        Some(json!({ "parameters": {} })),
    )
    .await;

    assert_eq!(
        query.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "query body: {}",
        query.1
    );
    assert!(
        query.1["error"]
            .as_str()
            .unwrap()
            .contains("standing runtime is unavailable for active artifact-backed view"),
        "query body: {}",
        query.1
    );
}

#[tokio::test]
async fn rest_product_rejects_artifact_backed_query_when_identity_is_missing() {
    let (initial_state, temp) = api_state("artifact-runtime-missing-identity").await;
    let initial_state = initial_state
        .with_generated_artifact_packages(["scores_by_user_generated"])
        .with_standing_program_runtime_factory(
            "scores_by_user_generated",
            FixedScoresStandingRuntimeFactory,
        );
    let initial_app = app(initial_state);
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "runtime_missing_identity_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "runtime_missing_identity_scores_by_user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let active_path = temp
        .path()
        .join("v1/views/runtime_missing_identity_scores_by_user/active.json");
    let mut active_record: Value =
        serde_json::from_slice(&fs::read(&active_path).unwrap()).unwrap();
    active_record["artifact"]
        .as_object_mut()
        .unwrap()
        .remove("standing_program_identity");
    fs::write(
        &active_path,
        serde_json::to_vec_pretty(&active_record).unwrap(),
    )
    .unwrap();

    let restarted_state =
        api_state_from_path("artifact-runtime-missing-identity", temp.path()).await;
    let restarted_app = app(restarted_state);

    let query = request_json(
        restarted_app,
        Method::POST,
        "/v1/views/runtime_missing_identity_scores_by_user/query",
        Some(json!({ "parameters": {} })),
    )
    .await;

    assert_eq!(query.0, StatusCode::CONFLICT, "query body: {}", query.1);
    assert!(
        query.1["error"]
            .as_str()
            .unwrap()
            .contains("is missing standing runtime identity"),
        "query body: {}",
        query.1
    );
}

#[tokio::test]
async fn rest_product_rejects_artifact_runtime_schema_drift_before_view_activation() {
    let (state, _temp) = api_state("artifact-runtime-schema-drift").await;
    let app = app(state
        .with_generated_artifact_packages(["scores_by_user_generated"])
        .with_standing_program_runtime_factory(
            "scores_by_user_generated",
            SchemaDriftScoresStandingRuntimeFactory,
        ));
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "schema_drift_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "schema_drift_scores_by_user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;

    assert_eq!(view.0, StatusCode::BAD_REQUEST, "view body: {}", view.1);
    assert!(
        view.1["error"]
            .as_str()
            .unwrap()
            .contains("standing runtime output schemas do not match artifact metadata"),
        "view body: {}",
        view.1
    );

    let detail = request_json(
        app,
        Method::GET,
        "/v1/views/schema_drift_scores_by_user",
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rest_product_restores_standing_runtime_from_active_artifact_view_on_restart() {
    let (initial_state, temp) = api_state("artifact-runtime-restart").await;
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "restart_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    let initial_app = app(initial_state
        .clone()
        .with_generated_artifact_packages(["scores_by_user_generated"])
        .with_standing_program_runtime_factory(
            "scores_by_user_generated",
            FixedScoresStandingRuntimeFactory,
        ));

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        initial_app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "restart_scores_by_user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let restarted_state = api_state_from_path("artifact-runtime-restart", temp.path())
        .await
        .with_generated_artifact_packages(["scores_by_user_generated"])
        .with_standing_program_runtime_factory(
            "scores_by_user_generated",
            FixedScoresStandingRuntimeFactory,
        );
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restarted_app = app(restarted_state);

    let ingest = request_json(
        restarted_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = request_json(
        restarted_app,
        Method::POST,
        "/v1/views/restart_scores_by_user/query",
        Some(json!({ "parameters": {} })),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_replays_committed_ingest_into_standing_runtime_on_restore() {
    let (initial_state, temp) = api_state("artifact-runtime-replay").await;
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "replayed_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    let initial_app = app(initial_state
        .clone()
        .with_generated_artifact_packages(["scores_by_user_generated"])
        .with_standing_program_runtime_factory(
            "scores_by_user_generated",
            FixedScoresStandingRuntimeFactory,
        ));

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "replayed_scores_by_user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let restarted_state = api_state_from_path("artifact-runtime-replay", temp.path())
        .await
        .with_generated_artifact_packages(["scores_by_user_generated"])
        .with_standing_program_runtime_factory(
            "scores_by_user_generated",
            FixedScoresStandingRuntimeFactory,
        );
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restarted_app = app(restarted_state);

    let query = request_json(
        restarted_app,
        Method::POST,
        "/v1/views/replayed_scores_by_user/query",
        Some(json!({ "parameters": {} })),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_restores_generated_runtime_from_checkpoint_without_ingest_replay() {
    let (initial_state, temp) = api_state("generated-runtime-checkpoint-restore").await;
    let initial_app = app(initial_state);

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let second_ingest = request_json(
        initial_app,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 2,
                rows: vec![json!({ "user_id": "u1", "score": 3, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        second_ingest.0,
        StatusCode::CREATED,
        "second ingest body: {}",
        second_ingest.1
    );

    let stale_epoch2_checkpoint = fs::read(
        fs::read_dir(temp.path().join(
            "v1/standing-runtime-checkpoints/default/positive_scores_by_user/positive_scores_by_user/epochs/00000000000000000002/sha256",
        ))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path(),
    )
    .unwrap();
    fs::write(
        temp.path()
            .join("v1/standing-runtime-checkpoints/default/positive_scores_by_user/positive_scores_by_user/latest.json"),
        stale_epoch2_checkpoint,
    )
    .unwrap();
    fs::remove_dir_all(temp.path().join("v1/ingest")).unwrap();

    let restarted_state =
        api_state_from_path("generated-runtime-checkpoint-restore", temp.path()).await;
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restarted_app = app(restarted_state);

    let query = request_json(
        restarted_app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(query.1["logical_epoch"], json!(3));
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 15, "count": 3 }
        ])
    );
}

#[tokio::test]
async fn rest_product_uses_meta_latest_checkpoint_instead_of_higher_epoch_orphan() {
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let (initial_state, temp) = api_state("generated-runtime-meta-checkpoint-authority").await;
    let initial_app = app(initial_state.with_meta_store(meta_store.clone()));

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let second_ingest = request_json(
        initial_app,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 2,
                rows: vec![json!({ "user_id": "u1", "score": 3, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        second_ingest.0,
        StatusCode::CREATED,
        "second ingest body: {}",
        second_ingest.1
    );

    let epoch3_dir = temp.path().join(
        "v1/standing-runtime-checkpoints/default/positive_scores_by_user/positive_scores_by_user/epochs/00000000000000000003/sha256",
    );
    let epoch3_path = fs::read_dir(&epoch3_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut orphan_record: Value =
        serde_json::from_slice(&fs::read(&epoch3_path).unwrap()).unwrap();
    let content_hash = orphan_record["checkpoint"]["state_root"]["content_hash"]
        .as_str()
        .unwrap()
        .to_string();
    let content_hash_segment = content_hash
        .strip_prefix("sha256:")
        .unwrap_or(&content_hash);
    let orphan_key = format!(
        "v1/standing-runtime-checkpoints/default/positive_scores_by_user/positive_scores_by_user/epochs/00000000000000000100/sha256/{content_hash_segment}.checkpoint.json"
    );
    orphan_record["checkpoint_key"] = Value::String(orphan_key.clone());
    orphan_record["checkpoint"]["logical_epoch"] = json!(100);
    orphan_record["checkpoint"]["output_frontiers"][0]["committed_epoch"] = json!(100);
    let orphan_path = temp.path().join(&orphan_key);
    fs::create_dir_all(orphan_path.parent().unwrap()).unwrap();
    fs::write(orphan_path, serde_json::to_vec(&orphan_record).unwrap()).unwrap();

    let restarted_state =
        api_state_from_path("generated-runtime-meta-checkpoint-authority", temp.path())
            .await
            .with_meta_store(meta_store);
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restarted_app = app(restarted_state);

    let query = request_json(
        restarted_app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(query.1["logical_epoch"], json!(3));
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 15, "count": 3 }
        ])
    );
}

#[tokio::test]
async fn rest_product_keeps_metadata_committed_checkpoint_when_advisory_latest_marker_fails() {
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let (initial_state, temp) = api_state("metadata-commit-advisory-latest-failure").await;
    let initial_app = app(initial_state.with_meta_store(meta_store.clone()));

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    fs::create_dir_all(temp.path().join(
        "v1/standing-runtime-checkpoints/default/positive_scores_by_user/positive_scores_by_user/latest.json",
    ))
    .unwrap();

    let ingest = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = request_json(
        initial_app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );

    let restarted_state =
        api_state_from_path("metadata-commit-advisory-latest-failure", temp.path())
            .await
            .with_meta_store(meta_store);
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restarted_app = app(restarted_state);

    let restarted_query = request_json(
        restarted_app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(
        restarted_query.0,
        StatusCode::OK,
        "query body: {}",
        restarted_query.1
    );
    assert_eq!(
        restarted_query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_fences_second_writer_but_allows_committed_read_replica() {
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let (owner_state, temp) = api_state("generated-runtime-owner-fencing").await;
    let owner_state = owner_state.with_meta_store(meta_store.clone());
    let owner_app = app(owner_state.clone());

    let relation = request_json(
        owner_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        owner_app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let first_ingest = request_json(
        owner_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![json!({ "user_id": "u1", "score": 5, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        first_ingest.0,
        StatusCode::CREATED,
        "first ingest body: {}",
        first_ingest.1
    );

    let replica_state = api_state_from_path("generated-runtime-owner-fencing", temp.path())
        .await
        .with_meta_store(meta_store);
    let restored = replica_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let replica_app = app(replica_state);

    let replica_query = request_json(
        replica_app.clone(),
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(
        replica_query.0,
        StatusCode::OK,
        "replica query body: {}",
        replica_query.1
    );
    assert_eq!(
        replica_query.1["rows"],
        json!([{ "user_id": "u1", "sum": 5, "count": 1 }])
    );

    let fenced_ingest = request_json(
        replica_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 1,
                rows: vec![json!({ "user_id": "u1", "score": 7, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        fenced_ingest.0,
        StatusCode::CONFLICT,
        "fenced ingest body: {}",
        fenced_ingest.1
    );
    assert!(fenced_ingest.1["error"]
        .as_str()
        .unwrap()
        .contains("standing runtime owner conflict"));

    let owner_retry = request_json(
        owner_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 1,
                rows: vec![json!({ "user_id": "u1", "score": 7, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert!(
        matches!(owner_retry.0, StatusCode::OK | StatusCode::CREATED),
        "owner retry body: {}",
        owner_retry.1
    );

    let owner_query = request_json(
        owner_app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(
        owner_query.0,
        StatusCode::OK,
        "owner query body: {}",
        owner_query.1
    );
    assert_eq!(
        owner_query.1["rows"],
        json!([{ "user_id": "u1", "sum": 12, "count": 2 }])
    );
}

#[tokio::test]
async fn rest_product_ingest_catches_up_external_committed_batches_before_advancing_runtime_frontier(
) {
    let (state, store) = api_state_memory("generated-runtime-catch-up-external-before-gap").await;
    let app = app(state);
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let first = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        first.0,
        StatusCode::CREATED,
        "first ingest body: {}",
        first.1
    );

    let external_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["u3"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![11])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ],
    )
    .unwrap();
    let external_envelope = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "scores".to_string(),
            relation_version: "2026-05-24.v1".to_string(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: "scores".to_string(),
            partition_id: 0,
            start_offset_inclusive: 2,
            end_offset_exclusive: 3,
        },
        &[external_batch],
    )
    .unwrap();
    let external = IngestAdmissionCoordinator::new(IngestLog::new(store))
        .append_catalog_validated_envelope(external_envelope)
        .await
        .unwrap();
    assert!(
        matches!(
            external,
            velorix_storage::log::AppendValidatedEnvelopeOutcome::Appended { .. }
        ),
        "external append outcome: {external:?}"
    );

    let later = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 1000,
                rows: vec![json!({ "user_id": "u4", "score": 13, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        later.0,
        StatusCode::CREATED,
        "later ingest body: {}",
        later.1
    );

    let query = request_json(
        app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 },
            { "user_id": "u3", "sum": 11, "count": 1 },
            { "user_id": "u4", "sum": 13, "count": 1 }
        ])
    );
}

#[tokio::test]
async fn rest_product_restarts_generated_runtime_from_checkpoint_without_replaying_covered_ingest()
{
    let (initial_state, temp) = api_state("generated-runtime-checkpoint-skip-covered").await;
    let initial_app = app(initial_state);

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        initial_app,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let restarted_state =
        api_state_from_path("generated-runtime-checkpoint-skip-covered", temp.path()).await;
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restarted_app = app(restarted_state);

    let query = request_json(
        restarted_app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(query.1["logical_epoch"], json!(2));
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_restores_generated_runtime_checkpoint_without_reading_corrupt_covered_ingest()
{
    let (initial_state, temp) = api_state("generated-runtime-checkpoint-corrupt-covered").await;
    let initial_app = app(initial_state);

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        initial_app,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let restarted_state =
        api_state_from_path("generated-runtime-checkpoint-corrupt-covered", temp.path()).await;
    fs::write(
        temp.path()
            .join("v1/ingest/scores/p=0000000000/00000000000000000000-00000000000000000002.batch"),
        b"not an arrow ingest envelope",
    )
    .unwrap();
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restarted_app = app(restarted_state);

    let query = request_json(
        restarted_app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_rejects_generated_runtime_checkpoint_record_under_mismatched_object_key() {
    let (initial_state, temp) = api_state("generated-runtime-checkpoint-key-mismatch").await;
    let initial_app = app(initial_state);

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        initial_app,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let epoch2_dir = temp.path().join(
        "v1/standing-runtime-checkpoints/default/positive_scores_by_user/positive_scores_by_user/epochs/00000000000000000002/sha256",
    );
    let epoch2_path = fs::read_dir(&epoch2_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let epoch999_dir = temp.path().join(
        "v1/standing-runtime-checkpoints/default/positive_scores_by_user/positive_scores_by_user/epochs/00000000000000000999/sha256",
    );
    fs::create_dir_all(&epoch999_dir).unwrap();
    fs::write(
        epoch999_dir.join(epoch2_path.file_name().unwrap()),
        fs::read(epoch2_path).unwrap(),
    )
    .unwrap();

    let restarted_state =
        api_state_from_path("generated-runtime-checkpoint-key-mismatch", temp.path()).await;
    let error = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("standing runtime checkpoint object key/body mismatch"),
        "restore error: {error}"
    );
}

#[tokio::test]
async fn rest_product_rejects_multiple_valid_generated_runtime_checkpoints_for_same_epoch() {
    let (initial_state, temp) = api_state("generated-runtime-checkpoint-duplicate-epoch").await;
    let initial_app = app(initial_state);

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        initial_app,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let epoch2_dir = temp.path().join(
        "v1/standing-runtime-checkpoints/default/positive_scores_by_user/positive_scores_by_user/epochs/00000000000000000002/sha256",
    );
    let epoch2_path = fs::read_dir(&epoch2_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut duplicate_record: Value =
        serde_json::from_slice(&fs::read(&epoch2_path).unwrap()).unwrap();
    let payload_text = duplicate_record["checkpoint"]["state_payload"]["payload"]
        .as_str()
        .unwrap();
    let mut payload: Value = serde_json::from_str(payload_text).unwrap();
    payload["applied_epochs"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "idempotency_key": "same-epoch-divergent-state",
            "logical_epoch": 2
        }));
    let payload_text = serde_json::to_string(&payload).unwrap();
    let content_hash = feldera_artifact_bytes_hash(payload_text.as_bytes());
    duplicate_record["checkpoint"]["state_payload"]["payload"] = Value::String(payload_text);
    duplicate_record["checkpoint"]["state_root"]["content_hash"] =
        Value::String(content_hash.clone());
    let content_hash_segment = content_hash.strip_prefix("sha256:").unwrap();
    fs::write(
        epoch2_dir.join(format!("{content_hash_segment}.checkpoint.json")),
        serde_json::to_vec(&duplicate_record).unwrap(),
    )
    .unwrap();

    let restarted_state =
        api_state_from_path("generated-runtime-checkpoint-duplicate-epoch", temp.path()).await;
    let error = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains(
            "multiple standing runtime checkpoints for `default/positive_scores_by_user/positive_scores_by_user` epoch 2"
        ),
        "restore error: {error}"
    );
}

#[tokio::test]
async fn rest_product_rejects_generated_runtime_checkpoint_replay_frontier_ahead_of_state_frontier()
{
    let (initial_state, temp) = api_state("generated-runtime-checkpoint-frontier-ahead").await;
    let initial_app = app(initial_state);

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        initial_app,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let epoch2_dir = temp.path().join(
        "v1/standing-runtime-checkpoints/default/positive_scores_by_user/positive_scores_by_user/epochs/00000000000000000002/sha256",
    );
    let epoch2_path = fs::read_dir(&epoch2_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut record: Value = serde_json::from_slice(&fs::read(&epoch2_path).unwrap()).unwrap();
    record["replay_checkpoints"][0]["end_offset_exclusive"] = json!(999);
    fs::write(&epoch2_path, serde_json::to_vec(&record).unwrap()).unwrap();

    let restarted_state =
        api_state_from_path("generated-runtime-checkpoint-frontier-ahead", temp.path()).await;
    let error = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains(
            "standing runtime checkpoint replay frontier is ahead of checkpoint input frontier"
        ),
        "restore error: {error}"
    );
}

#[tokio::test]
async fn rest_product_applies_generated_runtime_ingest_with_global_epoch_across_stream_offsets() {
    let (state, _temp) = api_state("generated-runtime-global-epoch").await;
    let app = app(state);

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let first = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores-a".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        first.0,
        StatusCode::CREATED,
        "first ingest body: {}",
        first.1
    );

    let second = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores-b".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![json!({ "user_id": "u1", "score": 3, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        second.0,
        StatusCode::CREATED,
        "second ingest body: {}",
        second.1
    );

    let query = request_json(
        app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(query.1["logical_epoch"], json!(3));
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 15, "count": 3 }
        ])
    );
}

#[tokio::test]
async fn rest_product_restores_generated_runtime_after_global_epoch_multi_stream_ingest() {
    let (initial_state, temp) = api_state("generated-runtime-global-epoch-restore").await;
    let initial_app = app(initial_state);

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let first = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores-a".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u1", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        first.0,
        StatusCode::CREATED,
        "first ingest body: {}",
        first.1
    );

    let second = request_json(
        initial_app,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores-b".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![json!({ "user_id": "u1", "score": 3, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        second.0,
        StatusCode::CREATED,
        "second ingest body: {}",
        second.1
    );

    let latest_checkpoint: Value = serde_json::from_slice(
        &fs::read(temp.path().join(
            "v1/standing-runtime-checkpoints/default/positive_scores_by_user/positive_scores_by_user/latest.json",
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        latest_checkpoint["replay_checkpoints"],
        json!([
            {
                "stream_id": "scores-a",
                "partition_id": 0,
                "end_offset_exclusive": 2
            },
            {
                "stream_id": "scores-b",
                "partition_id": 0,
                "end_offset_exclusive": 1
            }
        ])
    );

    let restarted_state =
        api_state_from_path("generated-runtime-global-epoch-restore", temp.path()).await;
    let restored = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restarted_app = app(restarted_state);

    let query = request_json(
        restarted_app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(query.1["logical_epoch"], json!(3));
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 15, "count": 3 }
        ])
    );
}

#[tokio::test]
async fn rest_product_treats_duplicate_generated_runtime_ingest_as_idempotent_after_success() {
    let (state, _temp) = api_state("generated-runtime-duplicate-success").await;
    let app = app(state);

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest_body = serde_json::to_value(IngestRowsRequest {
        relation_id: "scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        stream_id: "scores".to_string(),
        partition_id: 0,
        start_offset_inclusive: 0,
        rows: vec![
            json!({ "user_id": "u1", "score": 5, "delta": 1 }),
            json!({ "user_id": "u1", "score": 7, "delta": 1 }),
        ],
    })
    .unwrap();
    let first = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(ingest_body.clone()),
    )
    .await;
    assert_eq!(
        first.0,
        StatusCode::CREATED,
        "first ingest body: {}",
        first.1
    );

    let duplicate = request_json(app.clone(), Method::POST, "/v1/ingest", Some(ingest_body)).await;
    assert_eq!(
        duplicate.0,
        StatusCode::OK,
        "duplicate body: {}",
        duplicate.1
    );
    assert_eq!(duplicate.1["outcome"], "duplicate");

    let query = request_json(
        app,
        Method::GET,
        "/v1/views/positive_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(query.1["logical_epoch"], json!(2));
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_retries_standing_runtime_apply_after_duplicate_ingest_append() {
    let (state, _temp) = api_state("artifact-runtime-duplicate-apply").await;
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "duplicate_apply_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    let app = app(state
        .with_generated_artifact_packages(["scores_by_user_generated"])
        .with_standing_program_runtime_factory(
            "scores_by_user_generated",
            FailingFirstApplyScoresStandingRuntimeFactory,
        ));

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "duplicate_apply_scores_by_user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest_body = serde_json::to_value(IngestRowsRequest {
        relation_id: "scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        stream_id: "scores".to_string(),
        partition_id: 0,
        start_offset_inclusive: 0,
        rows: vec![
            json!({ "user_id": "u1", "score": 5, "delta": 1 }),
            json!({ "user_id": "u1", "score": 7, "delta": 1 }),
        ],
    })
    .unwrap();
    let first = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(ingest_body.clone()),
    )
    .await;
    assert_eq!(
        first.0,
        StatusCode::BAD_REQUEST,
        "first ingest body: {}",
        first.1
    );

    let retry = request_json(app.clone(), Method::POST, "/v1/ingest", Some(ingest_body)).await;
    assert_eq!(retry.0, StatusCode::OK, "retry body: {}", retry.1);
    assert_eq!(retry.1["outcome"], "duplicate");

    let query = request_json(
        app,
        Method::GET,
        "/v1/views/duplicate_apply_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_rejects_artifact_runtime_factory_with_mismatched_identity_on_restore() {
    let (initial_state, temp) = api_state("artifact-runtime-mismatch").await;
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "mismatch_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    let initial_app = app(initial_state
        .with_generated_artifact_packages(["scores_by_user_generated"])
        .with_standing_program_runtime_factory(
            "scores_by_user_generated",
            FixedScoresStandingRuntimeFactory,
        ));

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let view = request_json(
        initial_app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "mismatch_scores_by_user",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "artifact": {
                "metadata": artifact
            }
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let restarted_state = api_state_from_path("artifact-runtime-mismatch", temp.path())
        .await
        .with_generated_artifact_packages(["scores_by_user_generated"])
        .with_standing_program_runtime_factory(
            "scores_by_user_generated",
            MismatchedScoresStandingRuntimeFactory,
        );

    let err = restarted_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("standing program identity mismatch"),
        "restore error: {err}"
    );
}

#[tokio::test]
async fn rest_product_uses_grpc_meta_service_for_relation_catalog_and_ingest_admission() {
    let endpoint = spawn_meta_service().await;
    let meta_store = GrpcMetaStore::connect(endpoint).await.unwrap();
    let (state, _temp) = api_state("grpc-meta").await;
    let app = app(state.with_meta_store(Arc::new(meta_store)));
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);
    assert_eq!(relation.1["relation_id"], "scores");

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u2", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);
    assert_eq!(ingest.1["outcome"], "appended");

    let overlap = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 1,
                rows: vec![
                    json!({ "user_id": "u3", "score": 11, "delta": 1 }),
                    json!({ "user_id": "u4", "score": 13, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(overlap.0, StatusCode::CONFLICT);
    assert!(
        overlap.1["error"]
            .as_str()
            .unwrap()
            .contains("metadata service"),
        "overlap body: {}",
        overlap.1
    );

    let query = request_json(
        app,
        Method::GET,
        "/v1/api/scores/positive?max_rows=100",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 5, "count": 1 },
            { "user_id": "u2", "sum": 7, "count": 1 }
        ])
    );
}

#[tokio::test]
async fn rest_product_uses_metadata_catalog_when_object_store_materialization_fails() {
    let meta_store = Arc::new(InMemoryMetaStore::default());
    let (state, temp) = api_state("metadata-catalog-materialization-failure").await;
    fs::create_dir_all(
        temp.path()
            .join("v1/relations/scores/versions/2026-05-24.v1.relation.json"),
    )
    .unwrap();
    let app = app(state.with_meta_store(meta_store));

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations/scores-default",
        None,
    )
    .await;
    assert_eq!(
        relation.0,
        StatusCode::CREATED,
        "relation body: {}",
        relation.1
    );

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views/scores-positive-default",
        None,
    )
    .await;
    assert_eq!(view.0, StatusCode::CREATED, "view body: {}", view.1);

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "scores".to_string(),
                relation_version: "2026-05-24.v1".to_string(),
                stream_id: "scores".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "user_id": "u1", "score": 5, "delta": 1 }),
                    json!({ "user_id": "u2", "score": 7, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = request_json(
        app,
        Method::GET,
        "/v1/api/scores/positive?max_rows=100",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 5, "count": 1 },
            { "user_id": "u2", "sum": 7, "count": 1 }
        ])
    );
}

async fn api_state(probe_id: &str) -> (ApiState, TempDir) {
    let temp = TempDir::new().unwrap();
    let state = api_state_from_path(probe_id, temp.path()).await;
    (state, temp)
}

async fn api_state_from_path(probe_id: &str, path: &std::path::Path) -> ApiState {
    let store = Arc::new(LocalFileSystem::new_with_prefix(path).unwrap());
    api_state_from_store(probe_id, store).await
}

async fn api_state_memory(probe_id: &str) -> (ApiState, Arc<dyn ObjectStore>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = api_state_from_store(probe_id, Arc::clone(&store)).await;
    (state, store)
}

async fn api_state_from_store(probe_id: &str, store: Arc<dyn ObjectStore>) -> ApiState {
    let validated = validate_operator_authority(
        ObjectStoreAuthorityRef {
            store_id: format!("s3://rustfs/velorix-product/{probe_id}"),
            namespace: "velorix".to_string(),
        },
        store,
        "velorix-api-test",
        format!("v1/api-test-probes/{probe_id}"),
    )
    .await
    .unwrap();
    let state = ApiState::from_validated_authority(validated, "v1/state/slatedb", "api-test")
        .await
        .unwrap();
    state
}

fn scores_sum_count_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "scores".to_string(),
        relation_name: "scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "user_id".to_string(),
                name: "user_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "score".to_string(),
                name: "score".to_string(),
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
        primary_key_column_ids: vec!["user_id".to_string()],
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
            name: "scores".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "scores".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn orders_sum_count_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "orders".to_string(),
        relation_name: "orders".to_string(),
        relation_version: "2026-06-06.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "amount".to_string(),
                name: "amount".to_string(),
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
        primary_key_column_ids: vec!["account_id".to_string()],
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
            name: "orders".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "orders".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn daily_revenue_sum_count_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "daily_revenue".to_string(),
        relation_name: "daily_revenue".to_string(),
        relation_version: "2026-06-06.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "business_date".to_string(),
                name: "business_date".to_string(),
                logical_type: VelorixLogicalTypeV1::Date,
                physical_arrow_type: ArrowPhysicalTypeV1::Date32,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "amount".to_string(),
                name: "amount".to_string(),
                logical_type: VelorixLogicalTypeV1::Decimal {
                    precision: 38,
                    scale: 2,
                },
                physical_arrow_type: ArrowPhysicalTypeV1::Decimal128 {
                    precision: 38,
                    scale: 2,
                },
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "row_weight".to_string(),
                name: "row_weight".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["business_date".to_string()],
        weight_column_id: "row_weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "daily_revenue".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "daily_revenue".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn float_value_sum_count_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "float_orders".to_string(),
        relation_name: "float_orders".to_string(),
        relation_version: "2026-06-06.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "amount".to_string(),
                name: "amount".to_string(),
                logical_type: VelorixLogicalTypeV1::Float64,
                physical_arrow_type: ArrowPhysicalTypeV1::Float64,
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
        primary_key_column_ids: vec!["account_id".to_string()],
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
            name: "float_orders".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "float_orders".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn artifact_for_scores_view(
    catalog: &VelorixRelationCatalogV1,
    view_id: &str,
    sql: &str,
) -> FelderaCompileArtifactMetadata {
    let input = catalog_input_relation_schema(catalog).unwrap();
    let output = scores_view_output_schema(view_id, catalog.schema_fingerprint.as_str());
    let spec = StandingViewSpec {
        view_id: view_id.to_string(),
        sql: sql.to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        input_relations: vec![input.clone()],
        output_relations: vec![output.clone()],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };

    FelderaCompileArtifactMetadata {
        metadata_version: FELDERA_ARTIFACT_METADATA_VERSION,
        view_id: view_id.to_string(),
        spec_hash: feldera_spec_hash(&spec).unwrap(),
        artifact_id: "feldera-artifact-positive-scores-by-user".to_string(),
        artifact_hash: format!("sha256:{}", "7".repeat(64)),
        compiler: FelderaCompilerIdentity {
            name: "feldera-sql-compiler".to_string(),
            version: "0.0.0-test".to_string(),
            source: "test-fixture".to_string(),
        },
        generated_rust: GeneratedRustIdentity {
            abi_version: SUPPORTED_GENERATED_RUST_ABI_VERSION.to_string(),
            crate_name: "scores_by_user_generated".to_string(),
        },
        input_schemas: vec![input],
        output_schemas: vec![output],
        state_codec: SUPPORTED_STATE_CODEC.to_string(),
        state_schema_version: 1,
        epoch_policy: SUPPORTED_EPOCH_POLICY.to_string(),
    }
}

fn scores_view_output_schema(view_id: &str, schema_fingerprint: &str) -> RelationSchema {
    RelationSchema {
        relation_id: view_id.to_string(),
        relation_name: view_id.to_string(),
        relation_version: "v1".to_string(),
        schema_fingerprint: schema_fingerprint.to_string(),
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
    }
}

#[derive(Clone, Debug)]
struct FixedScoresStandingRuntime {
    identity: StandingProgramIdentity,
    logical_epoch: u64,
    materialized: Vec<RecordBatch>,
    schema_drift: bool,
    fail_next_apply: bool,
    force_next_page_token: bool,
}

impl FixedScoresStandingRuntime {
    fn new(identity: StandingProgramIdentity) -> Self {
        Self {
            identity,
            logical_epoch: 0,
            materialized: Vec::new(),
            schema_drift: false,
            fail_next_apply: false,
            force_next_page_token: false,
        }
    }

    fn with_schema_drift(mut self) -> Self {
        self.schema_drift = true;
        self
    }

    fn with_failing_first_apply(mut self) -> Self {
        self.fail_next_apply = true;
        self
    }

    fn with_forced_next_page_token(mut self) -> Self {
        self.force_next_page_token = true;
        self
    }
}

#[derive(Clone, Debug)]
struct FixedScoresStandingRuntimeFactory;

impl StandingProgramRuntimeFactory for FixedScoresStandingRuntimeFactory {
    fn create(
        &self,
        identity: &StandingProgramIdentity,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Ok(Box::new(FixedScoresStandingRuntime::new(identity.clone())))
    }

    fn restore(
        &self,
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        FixedScoresStandingRuntime::restore(checkpoint)
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string())
    }
}

#[derive(Clone, Debug)]
struct MismatchedScoresStandingRuntimeFactory;

impl StandingProgramRuntimeFactory for MismatchedScoresStandingRuntimeFactory {
    fn create(
        &self,
        identity: &StandingProgramIdentity,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        let mut mismatched = identity.clone();
        mismatched.program_id = "wrong_program".to_string();
        Ok(Box::new(FixedScoresStandingRuntime::new(mismatched)))
    }

    fn restore(
        &self,
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        let mut mismatched = checkpoint.identity;
        mismatched.program_id = "wrong_program".to_string();
        Ok(Box::new(FixedScoresStandingRuntime::new(mismatched)))
    }
}

#[derive(Clone, Debug)]
struct SchemaDriftScoresStandingRuntimeFactory;

impl StandingProgramRuntimeFactory for SchemaDriftScoresStandingRuntimeFactory {
    fn create(
        &self,
        identity: &StandingProgramIdentity,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Ok(Box::new(
            FixedScoresStandingRuntime::new(identity.clone()).with_schema_drift(),
        ))
    }

    fn restore(
        &self,
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Ok(Box::new(
            FixedScoresStandingRuntime::restore(checkpoint)
                .map_err(|error| error.to_string())?
                .with_schema_drift(),
        ))
    }
}

#[derive(Clone, Debug)]
struct FailingFirstApplyScoresStandingRuntimeFactory;

impl StandingProgramRuntimeFactory for FailingFirstApplyScoresStandingRuntimeFactory {
    fn create(
        &self,
        identity: &StandingProgramIdentity,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Ok(Box::new(
            FixedScoresStandingRuntime::new(identity.clone()).with_failing_first_apply(),
        ))
    }

    fn restore(
        &self,
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        FixedScoresStandingRuntime::restore(checkpoint)
            .map(|runtime| Box::new(runtime) as Box<dyn StandingProgramRuntime + Send>)
            .map_err(|error| error.to_string())
    }
}

#[derive(Clone, Debug)]
struct PagedScoresStandingRuntimeFactory;

impl StandingProgramRuntimeFactory for PagedScoresStandingRuntimeFactory {
    fn create(
        &self,
        identity: &StandingProgramIdentity,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        Ok(Box::new(
            FixedScoresStandingRuntime::new(identity.clone()).with_forced_next_page_token(),
        ))
    }

    fn restore(
        &self,
        checkpoint: RuntimeCheckpoint,
    ) -> Result<Box<dyn StandingProgramRuntime + Send>, String> {
        FixedScoresStandingRuntime::restore(checkpoint)
            .map(|runtime| {
                Box::new(runtime.with_forced_next_page_token())
                    as Box<dyn StandingProgramRuntime + Send>
            })
            .map_err(|error| error.to_string())
    }
}

impl StandingProgramRuntime for FixedScoresStandingRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity {
        &self.identity
    }

    fn input_schemas(&self) -> Vec<RelationSchema> {
        vec![catalog_input_relation_schema(&scores_sum_count_relation_catalog()).unwrap()]
    }

    fn output_schemas(&self) -> Vec<RelationSchema> {
        let mut schema = scores_view_output_schema(
            &self.identity.view_ids[0],
            &self.identity.input_catalog_hash,
        );
        if self.schema_drift {
            schema.columns[1].name = "total".to_string();
        }
        vec![schema]
    }

    fn logical_epoch(&self) -> u64 {
        self.logical_epoch
    }

    fn apply_changes(
        &mut self,
        logical_epoch: u64,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError> {
        if self.fail_next_apply {
            self.fail_next_apply = false;
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "test_runtime_apply",
            });
        }
        let input = input_changes.first().unwrap();
        self.logical_epoch = logical_epoch;
        self.materialized = vec![runtime_scores_view_batch()];

        Ok(EpochCommit {
            logical_epoch,
            idempotency_key,
            input_frontiers: vec![RelationFrontier {
                relation_id: input.relation_id.clone(),
                relation_version: input.relation_version.clone(),
                committed_offset_exclusive: input.end_offset_exclusive,
            }],
            output_batches: vec![ViewOutputBatch {
                view_id: self.identity.view_ids[0].clone(),
                schema_fingerprint: self.output_schemas()[0].schema_fingerprint.clone(),
                batches: self.materialized.clone(),
            }],
        })
    }

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        _page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError> {
        Ok(MaterializedViewPage {
            view,
            logical_epoch: self.logical_epoch,
            schema_fingerprint: self.output_schemas()[0].schema_fingerprint.clone(),
            batches: self.materialized.clone(),
            next_page_token: self.force_next_page_token.then(|| "p2".to_string()),
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError> {
        Ok(RuntimeCheckpoint {
            identity: self.identity.clone(),
            logical_epoch: self.logical_epoch,
            input_frontiers: Vec::new(),
            output_frontiers: vec![ViewFrontier {
                view_id: self.identity.view_ids[0].clone(),
                committed_epoch: self.logical_epoch,
            }],
            checkpoint_codec_identity: self.identity.checkpoint_codec_identity.clone(),
            state_root: DurableStateRoot {
                object_key: "v1/state/test-runtime".to_string(),
                content_hash: format!("sha256:{}", "8".repeat(64)),
            },
            state_payload: None,
            output_manifest_refs: Vec::new(),
            owner_epoch: None,
        })
    }

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError>
    where
        Self: Sized,
    {
        Ok(Self {
            identity: checkpoint.identity,
            logical_epoch: checkpoint.logical_epoch,
            materialized: Vec::new(),
            schema_drift: false,
            fail_next_apply: false,
            force_next_page_token: false,
        })
    }
}

fn runtime_scores_view_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("sum", DataType::Int64, false),
            Field::new("count", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["u1"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![12])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2])) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn request_json(
    app: axum::Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    request_json_with_headers(app, method, uri, body, &[]).await
}

async fn request_json_with_headers(
    app: axum::Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    let request = headers
        .iter()
        .fold(request, |request, (name, value)| {
            request.header(*name, *value)
        })
        .body(match body {
            Some(value) => Body::from(serde_json::to_vec(&value).unwrap()),
            None => Body::empty(),
        })
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes).unwrap();
    (status, body)
}

async fn request_raw_json(
    app: axum::Router,
    method: Method,
    uri: &str,
    body: Value,
) -> (StatusCode, String) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

async fn spawn_meta_service() -> String {
    let service = MetaGrpcService::new(InMemoryMetaStore::default());
    spawn_meta_service_with(service).await
}

async fn spawn_authenticated_meta_service(token: &'static str) -> String {
    let service = MetaGrpcService::with_bearer_token(InMemoryMetaStore::default(), token).unwrap();
    spawn_meta_service_with(service).await
}

async fn spawn_meta_service_with(service: MetaGrpcService<InMemoryMetaStore>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(VelorixMetaServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    format!("http://{addr}")
}
