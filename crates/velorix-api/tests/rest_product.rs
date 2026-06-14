use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{Path as AxumPath, Query, State as AxumState},
    http::{HeaderMap, Method, Request, StatusCode},
    routing::{get, post, put},
    Json, Router,
};
use futures::TryStreamExt;
use http_body_util::BodyExt as _;
use object_store::{local::LocalFileSystem, memory::InMemory, path::Path, ObjectStore};
use serde_json::{json, Value};
use std::{
    collections::{BTreeSet, HashMap},
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};
use tempfile::TempDir;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tower::ServiceExt as _;
use velorix_api::{
    app, ApiError, ApiState, FelderaCompilerBackend, FelderaCompilerBackendRequest,
    FelderaCompilerBackendResponse, FelderaPipelineManagerCompilerBackend,
    FelderaPipelineManagerRuntimeDeploymentMode, IngestRowsRequest, StandingProgramRuntimeFactory,
};
use velorix_core::{
    feldera_artifact::{
        catalog_input_relation_schema, feldera_artifact_bytes_hash, feldera_compile_request_hash,
        feldera_spec_hash, feldera_sql_program_for_compile_request, ColumnSchema,
        FelderaCompileArtifactMetadata, FelderaCompileRequestV1, FelderaCompilerIdentity,
        GeneratedRustIdentity, RelationSchema, SqlDataType, SqlDialect, SqlSourceKind,
        StandingViewShape, StandingViewSpec, FELDERA_ARTIFACT_METADATA_VERSION,
        SUPPORTED_EPOCH_POLICY, SUPPORTED_GENERATED_RUST_ABI_VERSION, SUPPORTED_STATE_CODEC,
    },
    feldera_product_runtime::{
        feldera_package_runtime_identity_for_descriptor, FelderaPackageBackendIdentity,
        FelderaPackageRuntimeDescriptorV1, FelderaPackageRuntimeFactoryBinding,
        FELDERA_PACKAGE_RUNTIME_EXECUTION_PATH, FELDERA_PRODUCT_RUNTIME_DESCRIPTOR_VERSION,
    },
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        CATALOG_FELDERA_GENERIC_INCREMENTAL_ADAPTER_ID,
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
async fn rest_product_accepts_generic_feldera_relation_create_and_ingest_without_value_shape() {
    let (state, _temp) = api_state("generic-feldera-relation-ingest").await;
    let app = app(state);
    let catalog = generic_feldera_activity_relation_catalog();

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

    let ingest = request_json(
        app,
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "activity_events",
            "relation_version": "2026-06-11.v1",
            "stream_id": "activity-events",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {
                    "event_id": "e1",
                    "user_id": "u1",
                    "score": 7,
                    "delta": 1
                }
            ]
        })),
    )
    .await;

    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);
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
        .starts_with("scores_by_user:velorix-feldera-compile-request-sha256-v1:"));
    let spec_hash = view.1["spec_hash"].as_str().unwrap();
    let job_record_path = temp
        .path()
        .join(compile_request_job_object_path("scores_by_user", &view.1));
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
    assert!(job_record["compiler_request"]["compile_request_hash"]
        .as_str()
        .unwrap()
        .starts_with("velorix-feldera-compile-request-sha256-v1:"));
    assert_eq!(job_record["compiler_request"]["spec_hash"], spec_hash);
    assert_eq!(
        job_record["compiler_request"]["sql"],
        "this is not valid sql"
    );
    assert_eq!(job_record["compiler_request"]["dialect"], "feldera_sql");
    assert_eq!(
        job_record["compiler_request"]["source_kind"],
        "standing_view"
    );
    assert_eq!(
        job_record["compiler_request"]["input_relations"][0]["relation_name"],
        "scores"
    );
    assert_eq!(
        job_record["compiler_request"]["output_contract"]["kind"],
        "infer"
    );
    assert_eq!(
        job_record["compiler_request"]["output_relations"],
        json!([])
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
            .contains("row limits are not supported for templated standing runtime"),
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
async fn rest_product_static_generated_runtime_rejects_caller_supplied_view_sql() {
    let (state, _temp) = api_state("artifact-runtime-generated-package-rejects-sql").await;
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

    let query = request_json(
        app,
        Method::POST,
        "/v1/views/generated_scores_by_user/query",
        Some(json!({ "sql": "select * from generated_scores_by_user" })),
    )
    .await;
    assert_eq!(query.0, StatusCode::BAD_REQUEST, "query body: {}", query.1);
    assert!(
        query.1["error"]
            .as_str()
            .unwrap()
            .contains("caller-supplied SQL is not supported"),
        "query body: {}",
        query.1
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
async fn rest_product_complete_compile_deploy_job_activates_pending_view_from_runtime_deployment() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager().await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_runtime_deployment_mode(FelderaPipelineManagerRuntimeDeploymentMode::ExternalManaged);
    let (state, _store) =
        api_state_memory("trusted-linked-generated-view-complete-runtime-deployment").await;
    let app = app(state
        .with_generated_artifact_packages(std::iter::empty::<&str>())
        .with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let sql = "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id";
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/runtime-deployment",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": sql,
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");
    let compile_request_hash =
        compile_request_hash_from_view_response("positive_scores_by_user", &view.1);
    let pipeline_name =
        test_feldera_pipeline_name_for_parts("positive_scores_by_user", &compile_request_hash);
    let resolved_spec = scores_view_spec(
        &scores_sum_count_relation_catalog(),
        "positive_scores_by_user",
        sql,
    );

    let complete = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(json!({
            "compile_request_hash": compile_request_hash,
            "resolved_spec": resolved_spec,
            "runtime_deployment": {
                "pipeline_name": pipeline_name,
                "mode": "external_managed"
            }
        })),
    )
    .await;
    assert_eq!(complete.0, StatusCode::OK, "complete body: {}", complete.1);
    assert_eq!(complete.1["execution_mode"], "standing_runtime");
    assert_eq!(complete.1["query_enabled"], true);
    assert_eq!(
        complete.1["artifact"]["execution_path"],
        "feldera_pipeline_manager"
    );

    let query = request_json(
        app.clone(),
        Method::GET,
        "/v1/api/scores/runtime-deployment",
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

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .any(|request| request["kind"] == "start_pipeline"),
        "runtime completion should start the external Feldera pipeline: {requests:?}"
    );
    assert!(
        !requests
            .iter()
            .any(|request| request["kind"] == "compile_put"),
        "runtime completion should not compile inside velorix-api: {requests:?}"
    );
}

#[tokio::test]
async fn rest_product_claim_compile_deploy_job_returns_lease_and_fencing_token() {
    let (state, _store) = api_state_memory("view-compile-deploy-claim").await;
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
            "urlPath": "/scores/claim",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    let compile_request_hash =
        compile_request_hash_from_view_response("positive_scores_by_user", &view.1);

    let first = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/claim",
        Some(json!({
            "worker_id": "worker-a",
            "lease_duration_ms": 30000
        })),
    )
    .await;
    assert_eq!(first.0, StatusCode::OK, "claim body: {}", first.1);
    assert_eq!(first.1["claim_status"], "claimed");
    assert_eq!(first.1["tenant_id"], "default");
    assert_eq!(first.1["job_generation"], 1);
    assert_eq!(first.1["view_id"], "positive_scores_by_user");
    assert_eq!(first.1["compile_request_hash"], compile_request_hash);
    assert_eq!(first.1["worker_id"], "worker-a");
    assert_eq!(first.1["fencing_token"], 1);
    assert!(first.1["lease_id"]
        .as_str()
        .unwrap()
        .starts_with("velorix-feldera-compile-lease-sha256-v1:"));

    let duplicate = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/claim",
        Some(json!({
            "worker_id": "worker-a",
            "lease_duration_ms": 30000
        })),
    )
    .await;
    assert_eq!(
        duplicate.0,
        StatusCode::OK,
        "duplicate claim body: {}",
        duplicate.1
    );
    assert_eq!(duplicate.1["claim_status"], "duplicate");
    assert_eq!(duplicate.1["lease_id"], first.1["lease_id"]);

    let conflict = request_json(
        app,
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/claim",
        Some(json!({
            "worker_id": "worker-b",
            "lease_duration_ms": 30000
        })),
    )
    .await;
    assert_eq!(
        conflict.0,
        StatusCode::CONFLICT,
        "conflict claim body: {}",
        conflict.1
    );
    assert!(conflict.1["error"]
        .as_str()
        .unwrap()
        .contains("active worker"));
}

#[tokio::test]
async fn rest_product_complete_compile_deploy_job_requires_matching_claim_when_job_is_claimed() {
    let (state, store) = api_state_memory("view-compile-deploy-claim-complete").await;
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

    let sql = "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id";
    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/claimed-complete",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "sql": sql,
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    let compile_request_hash =
        compile_request_hash_from_view_response("positive_scores_by_user", &view.1);
    let completion_state =
        api_state_from_store("view-compile-deploy-claim-complete", Arc::clone(&store)).await;
    let completion_app = app(completion_state);

    let claim = request_json(
        completion_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/claim",
        Some(json!({
            "worker_id": "worker-a",
            "lease_duration_ms": 30000
        })),
    )
    .await;
    assert_eq!(claim.0, StatusCode::OK, "claim body: {}", claim.1);

    let artifact = artifact_for_scores_view(
        &scores_sum_count_relation_catalog(),
        "positive_scores_by_user",
        sql,
    );
    let missing_claim = request_json(
        completion_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(json!({
            "compile_request_hash": compile_request_hash,
            "artifact": artifact,
        })),
    )
    .await;
    assert_eq!(
        missing_claim.0,
        StatusCode::CONFLICT,
        "missing claim complete body: {}",
        missing_claim.1
    );
    assert!(missing_claim.1["error"]
        .as_str()
        .unwrap()
        .contains("claimed compile/deploy job"));

    let artifact = artifact_for_scores_view(
        &scores_sum_count_relation_catalog(),
        "positive_scores_by_user",
        sql,
    );
    let complete = request_json(
        completion_app,
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(json!({
            "compile_request_hash": compile_request_hash,
            "tenant_id": claim.1["tenant_id"],
            "job_generation": claim.1["job_generation"],
            "worker_id": claim.1["worker_id"],
            "lease_id": claim.1["lease_id"],
            "fencing_token": claim.1["fencing_token"],
            "artifact": artifact,
        })),
    )
    .await;
    assert_eq!(
        complete.0,
        StatusCode::OK,
        "claimed complete body: {}",
        complete.1
    );
    assert_eq!(complete.1["execution_mode"], "standing_runtime");
    assert_eq!(complete.1["query_enabled"], true);
}

#[tokio::test]
async fn rest_product_complete_compile_deploy_job_activates_pending_view_from_resolved_spec() {
    let (state, store) = api_state_memory("trusted-linked-generated-view-complete-resolved").await;
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

    let sql = "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id";
    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relations": [input_schema],
            "sql": sql,
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

    let resolved_spec = scores_view_spec(&catalog, "positive_scores_by_user", sql);
    let resolved_spec_hash = feldera_spec_hash(&resolved_spec).unwrap();
    assert_ne!(view.1["spec_hash"].as_str().unwrap(), resolved_spec_hash);
    let artifact = artifact_for_scores_view(&catalog, "positive_scores_by_user", sql);

    let completion_app =
        app(api_state_from_store("trusted-linked-generated-view-complete-resolved", store).await);
    let complete = request_json(
        completion_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(json!({
            "compile_request_hash": compile_request_hash_from_view_response(
                "positive_scores_by_user",
                &view.1
            ),
            "resolved_spec": resolved_spec,
            "artifact": artifact,
        })),
    )
    .await;
    assert_eq!(complete.0, StatusCode::OK, "complete body: {}", complete.1);
    assert_eq!(complete.1["spec_hash"], resolved_spec_hash);
    assert_eq!(complete.1["execution_mode"], "standing_runtime");
    assert_eq!(complete.1["query_enabled"], true);

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
async fn rest_product_complete_compile_deploy_job_repairs_pending_job_after_resolved_spec_activation(
) {
    let (state, store) =
        api_state_memory("trusted-linked-generated-view-complete-resolved-retry").await;
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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let sql = "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id";
    let view = request_json(
        initial_app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relations": [input_schema],
            "sql": sql,
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let completion_app = app(api_state_from_store(
        "trusted-linked-generated-view-complete-resolved-retry",
        Arc::clone(&store),
    )
    .await);
    let jobs = request_json(
        completion_app.clone(),
        Method::GET,
        "/v1/view-compile-deploy/jobs",
        None,
    )
    .await;
    assert_eq!(jobs.0, StatusCode::OK, "jobs body: {}", jobs.1);
    let pending_job = jobs.1["jobs"][0].clone();

    let resolved_spec = scores_view_spec(&catalog, "positive_scores_by_user", sql);
    let artifact = artifact_for_scores_view(&catalog, "positive_scores_by_user", sql);
    let complete_body = json!({
        "compile_request_hash": compile_request_hash_from_view_response(
            "positive_scores_by_user",
            &view.1
        ),
        "resolved_spec": resolved_spec,
        "artifact": artifact,
    });
    let complete = request_json(
        completion_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(complete_body.clone()),
    )
    .await;
    assert_eq!(complete.0, StatusCode::OK, "complete body: {}", complete.1);
    assert_eq!(complete.1["execution_mode"], "standing_runtime");

    store
        .put(
            &Path::from(compile_request_job_object_path(
                "positive_scores_by_user",
                &view.1,
            )),
            serde_json::to_vec_pretty(&pending_job).unwrap().into(),
        )
        .await
        .unwrap();

    let retry = request_json(
        completion_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(complete_body),
    )
    .await;
    assert_eq!(retry.0, StatusCode::OK, "retry body: {}", retry.1);
    assert_eq!(retry.1["execution_mode"], "standing_runtime");
    assert_eq!(retry.1["outcome"], "duplicate");

    let jobs = request_json(
        completion_app,
        Method::GET,
        "/v1/view-compile-deploy/jobs",
        None,
    )
    .await;
    assert_eq!(jobs.0, StatusCode::OK, "jobs body: {}", jobs.1);
    assert_eq!(jobs.1["pending_jobs"], 0, "jobs body: {}", jobs.1);
}

#[tokio::test]
async fn rest_product_worker_repairs_pending_job_after_resolved_spec_activation() {
    let (state, store) =
        api_state_memory("trusted-linked-generated-view-worker-resolved-repair").await;
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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let sql = "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id";
    let view = request_json(
        initial_app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "positive_scores_by_user",
            "urlPath": "/scores/positive",
            "input_relations": [input_schema],
            "sql": sql,
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let worker_app = app(api_state_from_store(
        "trusted-linked-generated-view-worker-resolved-repair",
        Arc::clone(&store),
    )
    .await);
    let jobs = request_json(
        worker_app.clone(),
        Method::GET,
        "/v1/view-compile-deploy/jobs",
        None,
    )
    .await;
    assert_eq!(jobs.0, StatusCode::OK, "jobs body: {}", jobs.1);
    let pending_job = jobs.1["jobs"][0].clone();

    let resolved_spec = scores_view_spec(&catalog, "positive_scores_by_user", sql);
    let artifact = artifact_for_scores_view(&catalog, "positive_scores_by_user", sql);
    let complete = request_json(
        worker_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(json!({
            "compile_request_hash": compile_request_hash_from_view_response(
                "positive_scores_by_user",
                &view.1
            ),
            "resolved_spec": resolved_spec,
            "artifact": artifact,
        })),
    )
    .await;
    assert_eq!(complete.0, StatusCode::OK, "complete body: {}", complete.1);
    assert_eq!(complete.1["execution_mode"], "standing_runtime");

    store
        .put(
            &Path::from(compile_request_job_object_path(
                "positive_scores_by_user",
                &view.1,
            )),
            serde_json::to_vec_pretty(&pending_job).unwrap().into(),
        )
        .await
        .unwrap();

    let worker = request_json(
        worker_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(worker.0, StatusCode::OK, "worker body: {}", worker.1);
    assert_eq!(worker.1["pending_jobs"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["skipped"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["failed"], 0, "worker body: {}", worker.1);
    assert_eq!(worker.1["outcomes"][0]["status"], "duplicate");

    let next = request_json(
        worker_app,
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(next.0, StatusCode::OK, "next worker body: {}", next.1);
    assert_eq!(next.1["pending_jobs"], 0, "next worker body: {}", next.1);
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
async fn rest_product_complete_compile_deploy_job_rejects_mismatched_artifact_compile_request_hash()
{
    let (state, _store) =
        api_state_memory("trusted-linked-generated-view-complete-wrong-request-hash").await;
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

    let mut artifact = artifact_for_scores_view(
        &scores_sum_count_relation_catalog(),
        "positive_scores_by_user",
        "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
    );
    artifact.compile_request_hash = Some(format!(
        "velorix-feldera-compile-request-sha256-v1:{}",
        "0".repeat(64)
    ));
    let complete = request_json(
        app,
        Method::POST,
        "/v1/view-compile-deploy/jobs/positive_scores_by_user/complete",
        Some(json!({
            "compile_request_hash": compile_request_hash_from_view_response(
                "positive_scores_by_user",
                &view.1
            ),
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
        .contains("compile request hash"));
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
    store
        .delete(&Path::from(compile_request_job_object_path(
            "positive_scores_by_user",
            &view.1,
        )))
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
    let compile_request_job_path =
        compile_request_job_object_path("positive_scores_by_user", &view.1);
    fs::remove_file(temp.path().join(compile_request_job_path)).unwrap();
    let job_registry = ViewCompileDeployJobRegistry::new(Arc::new(
        LocalFileSystem::new_with_prefix(temp.path()).unwrap(),
    ));
    let object_key = job_registry
        .object_key("positive_scores_by_user", spec_hash)
        .unwrap();
    let path = temp.path().join(object_key.as_str());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut job_record = json!({
        "schema_version": 1,
        "job_id": view_compile_deploy_job_id("positive_scores_by_user", spec_hash),
        "view_id": "positive_scores_by_user",
        "spec_hash": spec_hash,
        "compiler_backend": "feldera_compiler",
        "compiler_request": {
            "request_kind": "feldera_standing_view_compile_request_v1",
            "view_id": "positive_scores_by_user",
            "compile_request_hash": "",
            "spec_hash": spec_hash,
            "sql": "select user_id from scores",
            "dialect": "feldera_sql",
            "source_kind": "standing_view",
            "input_relations": artifact_for_scores_view(
                &scores_sum_count_relation_catalog(),
                "positive_scores_by_user",
                "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            ).input_schemas,
            "output_contract": { "kind": "infer" },
            "output_relations": [],
            "shape": {
                "is_materialized": true,
                "multi_input": false,
                "multi_output": false
            }
        },
        "compile_status": "pending",
        "deployment_status": "not_deployed"
    });
    refresh_compile_request_hash_for_job_json(&mut job_record);
    fs::write(&path, serde_json::to_vec_pretty(&job_record).unwrap()).unwrap();

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
async fn rest_product_fixture_worker_activates_catalog_backed_orders_sum_count_view() {
    let (state, store) = api_state_memory("catalog-backed-orders-view-worker").await;
    let initial_app = app(state.with_builtin_fixture_compile_worker_enabled(true));
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
        api_state_from_store("catalog-backed-orders-view-worker", Arc::clone(&store))
            .await
            .with_builtin_fixture_compile_worker_enabled(true);
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
async fn rest_product_worker_does_not_activate_fixture_sql_without_compiler_or_fixture_opt_in() {
    let (state, store) = api_state_memory("catalog-backed-orders-view-worker-no-fixture").await;
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
        initial_app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "orders_by_account_no_fixture",
            "urlPath": "/orders/by-account-no-fixture",
            "input_relation_id": "orders",
            "input_relation_version": "2026-06-06.v1",
            "sql": "select account_id, sum(amount) as sum, count(*) as count from orders group by account_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let worker_app = app(api_state_from_store(
        "catalog-backed-orders-view-worker-no-fixture",
        Arc::clone(&store),
    )
    .await);
    let worker = request_json(
        worker_app.clone(),
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
    assert!(worker.1["outcomes"][0]["reason"]
        .as_str()
        .unwrap()
        .contains("feldera compiler backend is not configured"));

    let detail = request_json(
        worker_app,
        Method::GET,
        "/v1/views/orders_by_account_no_fixture",
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(detail.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(detail.1["query_enabled"], false);
}

#[tokio::test]
async fn rest_product_fixture_worker_activates_catalog_backed_filtered_orders_sum_count_view_and_restores(
) {
    let (state, store) = api_state_memory("catalog-backed-filtered-view-worker").await;
    let initial_app = app(state.with_builtin_fixture_compile_worker_enabled(true));
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
            "view_id": "large_orders_by_account",
            "urlPath": "/orders/large-by-account",
            "input_relation_id": "orders",
            "input_relation_version": "2026-06-06.v1",
            "sql": "select account_id, sum(amount) as sum, count(*) as count from orders where amount > -1 group by account_id",
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
                relation_id: "orders".to_string(),
                relation_version: "2026-06-06.v1".to_string(),
                stream_id: "orders".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "account_id": "acct-a", "amount": 10, "delta": 1 }),
                    json!({ "account_id": "acct-a", "amount": -30, "delta": 1 }),
                    json!({ "account_id": "acct-b", "amount": 7, "delta": 1 }),
                    json!({ "account_id": "acct-a", "amount": 4, "delta": 1 }),
                    json!({ "account_id": "acct-a", "amount": 4, "delta": -1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let restarted_state =
        api_state_from_store("catalog-backed-filtered-view-worker", Arc::clone(&store))
            .await
            .with_builtin_fixture_compile_worker_enabled(true);
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
        "/v1/views/large_orders_by_account",
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(detail.1["execution_mode"], "standing_runtime");
    assert_eq!(detail.1["query_enabled"], true);

    let query = request_json(
        worker_app,
        Method::GET,
        "/v1/api/orders/large-by-account",
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

    let restored_state =
        api_state_from_store("catalog-backed-filtered-view-worker", Arc::clone(&store))
            .await
            .with_builtin_fixture_compile_worker_enabled(true);
    let restored = restored_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restored_app = app(restored_state);
    let restored_query = request_json(
        restored_app,
        Method::GET,
        "/v1/views/large_orders_by_account/query",
        None,
    )
    .await;
    assert_eq!(
        restored_query.0,
        StatusCode::OK,
        "restored query body: {}",
        restored_query.1
    );
    assert_eq!(
        restored_query.1["rows"],
        json!([
            { "account_id": "acct-a", "sum": 10, "count": 1 },
            { "account_id": "acct-b", "sum": 7, "count": 1 }
        ])
    );
}

#[tokio::test]
async fn rest_product_fixture_worker_activates_two_relation_join_view_and_applies_both_inputs() {
    let (state, store) = api_state_memory("catalog-backed-join-view-worker").await;
    let initial_app = app(state.with_builtin_fixture_compile_worker_enabled(true));
    let orders_catalog = orders_sum_count_relation_catalog();
    let accounts_catalog = accounts_sum_count_relation_catalog();
    let orders_input = catalog_input_relation_schema(&orders_catalog).unwrap();
    let accounts_input = catalog_input_relation_schema(&accounts_catalog).unwrap();

    for catalog in [orders_catalog, accounts_catalog] {
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
    }

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "joined_orders_by_account",
            "urlPath": "/orders/joined-by-account",
            "input_relations": [orders_input, accounts_input],
            "sql": "select a.account_id, sum(o.amount) as sum, count(*) as count from orders o join accounts a on o.account_id = a.account_id group by a.account_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let orders_ingest = request_json(
        initial_app.clone(),
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
                    json!({ "account_id": "acct-b", "amount": 7, "delta": 1 }),
                    json!({ "account_id": "acct-c", "amount": 3, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        orders_ingest.0,
        StatusCode::CREATED,
        "orders ingest body: {}",
        orders_ingest.1
    );

    let accounts_ingest = request_json(
        initial_app,
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "accounts".to_string(),
                relation_version: "2026-06-06.v1".to_string(),
                stream_id: "accounts".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![
                    json!({ "account_id": "acct-a", "limit": 100, "delta": 1 }),
                    json!({ "account_id": "acct-b", "limit": 50, "delta": 1 }),
                ],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        accounts_ingest.0,
        StatusCode::CREATED,
        "accounts ingest body: {}",
        accounts_ingest.1
    );

    let restarted_state =
        api_state_from_store("catalog-backed-join-view-worker", Arc::clone(&store))
            .await
            .with_builtin_fixture_compile_worker_enabled(true);
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
        "/v1/views/joined_orders_by_account",
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(detail.1["execution_mode"], "standing_runtime");
    assert_eq!(detail.1["query_enabled"], true);

    let query = request_json(
        worker_app.clone(),
        Method::GET,
        "/v1/api/orders/joined-by-account",
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

    let late_account = request_json(
        worker_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "accounts".to_string(),
                relation_version: "2026-06-06.v1".to_string(),
                stream_id: "accounts".to_string(),
                partition_id: 0,
                start_offset_inclusive: 2,
                rows: vec![json!({ "account_id": "acct-c", "limit": 25, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        late_account.0,
        StatusCode::CREATED,
        "late account body: {}",
        late_account.1
    );

    let query = request_json(
        worker_app.clone(),
        Method::GET,
        "/v1/views/joined_orders_by_account/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "account_id": "acct-a", "sum": 10, "count": 1 },
            { "account_id": "acct-b", "sum": 7, "count": 1 },
            { "account_id": "acct-c", "sum": 3, "count": 1 }
        ])
    );

    let late_order = request_json(
        worker_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "orders".to_string(),
                relation_version: "2026-06-06.v1".to_string(),
                stream_id: "orders".to_string(),
                partition_id: 0,
                start_offset_inclusive: 3,
                rows: vec![json!({ "account_id": "acct-a", "amount": 2, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        late_order.0,
        StatusCode::CREATED,
        "late order body: {}",
        late_order.1
    );

    let query = request_json(
        worker_app,
        Method::GET,
        "/v1/views/joined_orders_by_account/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "account_id": "acct-a", "sum": 12, "count": 2 },
            { "account_id": "acct-b", "sum": 7, "count": 1 },
            { "account_id": "acct-c", "sum": 3, "count": 1 }
        ])
    );

    let restored_state =
        api_state_from_store("catalog-backed-join-view-worker", Arc::clone(&store))
            .await
            .with_builtin_fixture_compile_worker_enabled(true);
    let restored = restored_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restored_app = app(restored_state);
    let restored_query = request_json(
        restored_app,
        Method::GET,
        "/v1/views/joined_orders_by_account/query",
        None,
    )
    .await;
    assert_eq!(
        restored_query.0,
        StatusCode::OK,
        "restored query body: {}",
        restored_query.1
    );
    assert_eq!(
        restored_query.1["rows"],
        json!([
            { "account_id": "acct-a", "sum": 12, "count": 2 },
            { "account_id": "acct-b", "sum": 7, "count": 1 },
            { "account_id": "acct-c", "sum": 3, "count": 1 }
        ])
    );
}

#[tokio::test]
async fn rest_product_replays_multi_relation_shared_stream_from_relation_frontier_minimum() {
    let (state, store) = api_state_memory("catalog-backed-join-shared-stream-replay").await;
    let initial_app = app(state.with_builtin_fixture_compile_worker_enabled(true));
    let orders_catalog = orders_sum_count_relation_catalog();
    let accounts_catalog = accounts_sum_count_relation_catalog();
    let orders_input = catalog_input_relation_schema(&orders_catalog).unwrap();
    let accounts_input = catalog_input_relation_schema(&accounts_catalog).unwrap();

    for catalog in [orders_catalog.clone(), accounts_catalog.clone()] {
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
    }

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "joined_shared_stream_orders",
            "input_relations": [orders_input, accounts_input],
            "sql": "select a.account_id, sum(o.amount) as sum, count(*) as count from orders o join accounts a on o.account_id = a.account_id group by a.account_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let order = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "orders".to_string(),
                relation_version: "2026-06-06.v1".to_string(),
                stream_id: "shared-ledger".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                rows: vec![json!({ "account_id": "acct-a", "amount": 10, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(order.0, StatusCode::CREATED, "order body: {}", order.1);

    let account = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(
            serde_json::to_value(IngestRowsRequest {
                relation_id: "accounts".to_string(),
                relation_version: "2026-06-06.v1".to_string(),
                stream_id: "shared-ledger".to_string(),
                partition_id: 0,
                start_offset_inclusive: 99,
                rows: vec![json!({ "account_id": "acct-a", "limit": 100, "delta": 1 })],
            })
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        account.0,
        StatusCode::CREATED,
        "account body: {}",
        account.1
    );

    let worker = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(worker.0, StatusCode::OK, "worker body: {}", worker.1);
    assert_eq!(worker.1["activated"], 1, "worker body: {}", worker.1);

    let external_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["acct-a"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![5])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ],
    )
    .unwrap();
    let external_envelope = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "orders".to_string(),
            relation_version: "2026-06-06.v1".to_string(),
            schema_fingerprint: orders_catalog.schema_fingerprint.as_str().to_string(),
            stream_id: "shared-ledger".to_string(),
            partition_id: 0,
            start_offset_inclusive: 1,
            end_offset_exclusive: 2,
        },
        &[external_batch],
    )
    .unwrap();
    let external = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)))
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

    let restored_state = api_state_from_store(
        "catalog-backed-join-shared-stream-replay",
        Arc::clone(&store),
    )
    .await
    .with_builtin_fixture_compile_worker_enabled(true);
    let restored = restored_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restored_app = app(restored_state);
    let query = request_json(
        restored_app,
        Method::GET,
        "/v1/views/joined_shared_stream_orders/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([{ "account_id": "acct-a", "sum": 15, "count": 2 }])
    );
}

#[tokio::test]
async fn rest_product_keeps_three_input_view_pending_without_unusable_generated_artifact() {
    let (state, _store) = api_state_memory("catalog-backed-three-input-view-worker").await;
    let app = app(state);
    let orders_catalog = orders_sum_count_relation_catalog();
    let accounts_catalog = accounts_sum_count_relation_catalog();
    let daily_catalog = daily_revenue_sum_count_relation_catalog();
    let orders_input = catalog_input_relation_schema(&orders_catalog).unwrap();
    let accounts_input = catalog_input_relation_schema(&accounts_catalog).unwrap();
    let daily_input = catalog_input_relation_schema(&daily_catalog).unwrap();

    for catalog in [orders_catalog, accounts_catalog, daily_catalog] {
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
    }

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "unsupported_three_input_view",
            "urlPath": "/unsupported/three-input",
            "input_relations": [orders_input, accounts_input, daily_input],
            "sql": "select a.account_id, sum(o.amount) as sum, count(*) as count from orders o join accounts a on o.account_id = a.account_id join daily_revenue d on a.account_id = d.business_date group by a.account_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(view.1["query_enabled"], false);

    let worker = request_json(app, Method::POST, "/v1/view-compile-deploy/run-once", None).await;
    assert_eq!(worker.0, StatusCode::OK, "worker body: {}", worker.1);
    assert_eq!(worker.1["pending_jobs"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["activated"], 0, "worker body: {}", worker.1);
    assert_eq!(worker.1["skipped"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["failed"], 0, "worker body: {}", worker.1);
}

#[tokio::test]
async fn rest_product_fixture_worker_activates_decimal_value_date_key_sum_count_view() {
    let (state, store) = api_state_memory("catalog-backed-decimal-date-view-worker").await;
    let initial_app = app(state.with_builtin_fixture_compile_worker_enabled(true));
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
            "sql": "select business_date, sum(amount) as sum, count(*) as count from daily_revenue where amount > -1.00 group by business_date",
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
                    json!({ "business_date": 20510, "amount": "-99.99", "row_weight": 1 }),
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
    .await
    .with_builtin_fixture_compile_worker_enabled(true);
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
async fn rest_product_worker_keeps_catalog_view_pending_when_feldera_compiler_is_unconfigured() {
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
    let compile_request_job_path = Path::from(compile_request_job_object_path(
        "positive_scores_by_user",
        &view.1,
    ));
    let mut job_record: Value = serde_json::from_slice::<Value>(
        &store
            .get(&compile_request_job_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    store.delete(&compile_request_job_path).await.unwrap();
    job_record["compiler_request"]["sql"] = json!("select user_id from scores");
    refresh_compile_request_hash_for_job_json(&mut job_record);
    job_record["job_id"] = json!(view_compile_deploy_job_id(
        "positive_scores_by_user",
        spec_hash
    ));
    let spec_hash_segment = spec_hash
        .strip_prefix("velorix-feldera-spec-sha256-v1:")
        .unwrap();
    let legacy_job_path = Path::from(format!(
        "v1/view-compile-deploy-jobs/positive_scores_by_user/spec-sha256/{spec_hash_segment}.job.json"
    ));
    store
        .put(
            &legacy_job_path,
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
    assert_eq!(worker.1["pending_jobs"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["activated"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["skipped"], 0, "worker body: {}", worker.1);
    assert_eq!(worker.1["failed"], 0, "worker body: {}", worker.1);

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
async fn rest_product_worker_activates_pending_view_through_configured_feldera_compiler_backend() {
    let (state, _store) = api_state_memory("feldera-compiler-backend-worker").await;
    let app = app(state.with_feldera_compiler_backend(Arc::new(TestFelderaCompilerBackend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let sql = "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id";
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "compiler_backend_scores_by_user",
            "urlPath": "/compiler/scores/by-user",
            "input_relations": [input_schema],
            "sql": sql,
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
    assert_eq!(worker.1["activated"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["skipped"], 0, "worker body: {}", worker.1);
    assert_eq!(worker.1["failed"], 0, "worker body: {}", worker.1);
    assert_eq!(
        worker.1["outcomes"][0]["reason"],
        Value::Null,
        "worker body: {}",
        worker.1
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

    let query = request_json(app, Method::GET, "/v1/api/compiler/scores/by-user", None).await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 }
        ])
    );
}

#[tokio::test]
async fn rest_product_worker_activates_pending_view_from_jarless_product_runtime_descriptor() {
    let (state, _store) = api_state_memory("feldera-product-runtime-backend-worker").await;
    let app = app(state
        .with_feldera_compiler_backend(Arc::new(TestProductRuntimeFelderaCompilerBackend))
        .with_standing_program_runtime_factory(
            TEST_PRODUCT_RUNTIME_CRATE_NAME,
            FixedScoresStandingRuntimeFactory,
        ));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let sql = "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id";
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "product_runtime_scores_by_user",
            "urlPath": "/product-runtime/scores/by-user",
            "input_relations": [input_schema],
            "sql": sql,
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
    assert_eq!(worker.1["activated"], 1, "worker body: {}", worker.1);
    assert_eq!(worker.1["skipped"], 0, "worker body: {}", worker.1);
    assert_eq!(worker.1["failed"], 0, "worker body: {}", worker.1);

    let detail = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/product_runtime_scores_by_user",
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(detail.1["execution_mode"], "standing_runtime");
    assert_eq!(detail.1["query_enabled"], true);
    assert_eq!(
        detail.1["artifact"]["execution_path"],
        FELDERA_PACKAGE_RUNTIME_EXECUTION_PATH
    );
    assert_eq!(
        detail.1["artifact"]["generated_rust_crate_name"],
        TEST_PRODUCT_RUNTIME_CRATE_NAME
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
        "/v1/api/product-runtime/scores/by-user",
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
async fn rest_product_worker_marks_compile_validated_without_runtime_artifact() {
    let (state, _store) = api_state_memory("feldera-schema-only-compiler-backend-worker").await;
    let app =
        app(state.with_feldera_compiler_backend(Arc::new(TestSchemaOnlyFelderaCompilerBackend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let sql = "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id";
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "schema_only_scores_by_user",
            "urlPath": "/schema-only/scores/by-user",
            "input_relations": [input_schema],
            "sql": sql,
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(view.1["lifecycle"]["compile_status"], "pending");
    assert_eq!(view.1["query_enabled"], false);

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
    assert_eq!(
        worker.1["outcomes"][0]["status"], "compile_validated",
        "worker body: {}",
        worker.1
    );

    let read_back = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/schema_only_scores_by_user",
        None,
    )
    .await;
    assert_eq!(read_back.0, StatusCode::OK, "view body: {}", read_back.1);
    assert_eq!(read_back.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(read_back.1["lifecycle"]["compile_status"], "success");
    assert_eq!(
        read_back.1["lifecycle"]["deployment_status"],
        "not_deployed"
    );
    assert_eq!(read_back.1["query_enabled"], false);
    assert_eq!(read_back.1["disabled_reason"], "feldera_compile_pending");
    assert!(read_back.1["artifact"].is_null());

    let second_worker =
        request_json(app, Method::POST, "/v1/view-compile-deploy/run-once", None).await;
    assert_eq!(
        second_worker.0,
        StatusCode::OK,
        "worker body: {}",
        second_worker.1
    );
    assert_eq!(second_worker.1["pending_jobs"], 0);
}

#[tokio::test]
async fn rest_product_pipeline_manager_compiler_only_accepts_sql_compiled_or_later_schema() {
    let fake_feldera =
        spawn_fake_feldera_pipeline_manager_with_program_status("CompilingRust").await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-sql-compiled").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "urlPath": "/feldera-http/schema-only/scores/by-user",
            "input_relations": [input_schema],
            "sql": "select user_id, sum(score) as sum, count(*) as count, avg(score) as avg_score from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["skipped"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "compile_validated");

    let read_back = request_json(
        app,
        Method::GET,
        "/v1/views/feldera_http_scores_by_user",
        None,
    )
    .await;
    assert_eq!(read_back.0, StatusCode::OK, "view body: {}", read_back.1);
    assert_eq!(read_back.1["lifecycle"]["compile_status"], "success");
    assert_eq!(
        read_back.1["lifecycle"]["deployment_status"],
        "not_deployed"
    );
    assert_eq!(read_back.1["query_enabled"], false);

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_sql_error_fails_compile_job_without_fixture_fallback(
) {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_error(
        "SqlError",
        json!({
            "sql_compilation": {
                "messages": [
                    { "message": "unsupported Feldera SQL in test program" }
                ]
            }
        }),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-sql-error").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_sql_error",
            "urlPath": "/feldera-http/scores/sql-error",
            "input_relations": [input_schema],
            "sql": "CREATE MATERIALIZED VIEW broken AS SELECT * FROM scores BROKEN_FELDERA_SYNTAX",
            "source_kind": "feldera_program",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(view.1["query_enabled"], false);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "failed");
    assert!(
        run.1["outcomes"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("unsupported Feldera SQL in test program"),
        "run body: {}",
        run.1
    );

    let read_back = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/feldera_http_scores_sql_error",
        None,
    )
    .await;
    assert_eq!(read_back.0, StatusCode::OK, "view body: {}", read_back.1);
    assert_eq!(read_back.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(read_back.1["query_enabled"], false);
    assert_eq!(read_back.1["disabled_reason"], "feldera_compile_pending");

    let query = request_json(
        app.clone(),
        Method::GET,
        "/v1/api/feldera-http/scores/sql-error",
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

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    assert!(!requests.iter().any(|request| request["kind"] == "query"));
}

#[tokio::test]
async fn rest_product_feldera_program_forwards_rust_uda_payload_to_pipeline_manager() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager().await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-rust-uda").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let udf_rust = "\
use feldera_sqllib::*;
pub type signed_sum_accumulator_type = i64;
pub fn signed_sum_map(value: i64) -> signed_sum_accumulator_type { value }
pub fn signed_sum_post(value: signed_sum_accumulator_type) -> i64 { value }
";
    let udf_toml = "[dependencies]\n";
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_rust_uda",
            "input_relations": [input_schema],
            "sql": "CREATE LINEAR AGGREGATE signed_sum(value BIGINT) RETURNS BIGINT; CREATE MATERIALIZED VIEW by_user AS SELECT user_id, signed_sum(score) AS sum FROM scores GROUP BY user_id",
            "source_kind": "feldera_program",
            "udf_rust": udf_rust,
            "udf_toml": udf_toml,
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");
    assert!(view.1["compile_job_id"]
        .as_str()
        .unwrap()
        .starts_with("feldera_http_scores_rust_uda:velorix-feldera-compile-request-sha256-v1:"));

    let jobs = request_json(
        app.clone(),
        Method::GET,
        "/v1/view-compile-deploy/jobs",
        None,
    )
    .await;
    assert_eq!(jobs.0, StatusCode::OK, "jobs body: {}", jobs.1);
    assert_eq!(
        jobs.1["jobs"][0]["compiler_request"]["rust_extension"]["udf_rust"],
        udf_rust
    );
    assert_eq!(
        jobs.1["jobs"][0]["compiler_request"]["rust_extension"]["udf_toml"],
        udf_toml
    );

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let requests = fake_feldera.requests.lock().unwrap();
    let compile = requests
        .iter()
        .find(|request| request["kind"] == "compile_put")
        .expect("compile request was recorded");
    let program_code = compile["body"]["program_code"].as_str().unwrap();
    assert!(program_code.contains("CREATE LINEAR AGGREGATE signed_sum"));
    assert_eq!(compile["body"]["udf_rust"], udf_rust);
    assert_eq!(compile["body"]["udf_toml"], udf_toml);
}

#[tokio::test]
async fn rest_product_feldera_program_rejects_external_rust_dependencies() {
    let (state, _store) = api_state_memory("feldera-program-external-rust-dependency").await;
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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_rust_dependency",
            "input_relations": [input_schema],
            "sql": "CREATE LINEAR AGGREGATE signed_sum(value BIGINT) RETURNS BIGINT; CREATE MATERIALIZED VIEW by_user AS SELECT user_id, signed_sum(score) AS sum FROM scores GROUP BY user_id",
            "source_kind": "feldera_program",
            "udf_rust": "pub fn unused() {}\n",
            "udf_toml": "[dependencies]\nserde = \"1\"\n",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::BAD_REQUEST, "view body: {}", view.1);
    assert!(
        view.1["error"]
            .as_str()
            .unwrap()
            .contains("rust_extension.udf_toml.external_dependencies"),
        "view body: {}",
        view.1
    );
}

#[tokio::test]
async fn rest_product_feldera_program_rejects_unregistered_compiled_input_relation() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        json!([
            {
                "name": "scores",
                "fields": [
                    { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "score", "columntype": { "type": "BIGINT", "nullable": false } },
                    { "name": "delta", "columntype": { "type": "BIGINT", "nullable": false } }
                ]
            },
            {
                "name": "external_scores",
                "fields": [
                    { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "score", "columntype": { "type": "BIGINT", "nullable": false } }
                ]
            }
        ]),
        default_fake_feldera_program_outputs(),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-unregistered-compiled-input").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_unregistered_input",
            "source_kind": "feldera_program",
            "input_relations": [input_schema],
            "sql": "CREATE MATERIALIZED VIEW feldera_http_scores_by_user AS SELECT user_id, SUM(score) AS sum, COUNT(*) AS count, AVG(score) AS avg_score FROM scores GROUP BY user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "failed");
    assert!(
        run.1["outcomes"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("unregistered input relation `external_scores`"),
        "run body: {}",
        run.1
    );

    let read_back = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/score_program_unregistered_input",
        None,
    )
    .await;
    assert_eq!(read_back.0, StatusCode::OK, "view body: {}", read_back.1);
    assert_eq!(read_back.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(read_back.1["query_enabled"], false);

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    assert!(!requests.iter().any(|request| request["kind"] == "ingress"));
}

#[tokio::test]
async fn rest_product_feldera_program_rejects_compiled_input_schema_mismatch() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        json!([{
            "name": "scores",
            "case_sensitive": false,
            "fields": [
                { "name": "user_id", "case_sensitive": false, "columntype": { "type": "VARCHAR", "nullable": false } },
                { "name": "score", "case_sensitive": false, "columntype": { "type": "VARCHAR", "nullable": false } },
                { "name": "delta", "case_sensitive": false, "columntype": { "type": "BIGINT", "nullable": false } }
            ],
            "primary_key": ["user_id"]
        }]),
        default_fake_feldera_program_outputs(),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-input-schema-mismatch").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_input_schema_mismatch",
            "source_kind": "feldera_program",
            "input_relations": [input_schema],
            "sql": "CREATE MATERIALIZED VIEW feldera_http_scores_by_user AS SELECT user_id, SUM(score) AS sum, COUNT(*) AS count, AVG(score) AS avg_score FROM scores GROUP BY user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "failed");
    assert!(
        run.1["outcomes"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("input relation `scores` column `score` type does not match"),
        "run body: {}",
        run.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    assert!(!requests.iter().any(|request| request["kind"] == "ingress"));
}

#[tokio::test]
async fn rest_product_feldera_program_accepts_case_insensitive_compiled_input_schema() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        json!([{
            "name": "SCORES",
            "case_sensitive": false,
            "fields": [
                { "name": "USER_ID", "case_sensitive": false, "columntype": { "type": "VARCHAR", "nullable": false } },
                { "name": "SCORE", "case_sensitive": false, "columntype": { "type": "BIGINT", "nullable": false } },
                { "name": "DELTA", "case_sensitive": false, "columntype": { "type": "BIGINT", "nullable": false } }
            ],
            "primary_key": ["USER_ID"]
        }]),
        default_fake_feldera_program_outputs(),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-case-input-accepted").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_case_input_accepted",
            "source_kind": "feldera_program",
            "input_relations": [input_schema],
            "sql": "CREATE MATERIALIZED VIEW feldera_http_scores_by_user AS SELECT user_id, SUM(score) AS sum, COUNT(*) AS count, AVG(score) AS avg_score FROM scores GROUP BY user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 0, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "activated");

    let query = request_json(
        app,
        Method::GET,
        "/v1/views/score_program_case_input_accepted/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([{ "user_id": "u1", "sum": 12, "count": 2, "avg_score": 6.0 }])
    );

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    assert!(requests.iter().any(|request| request["kind"] == "query"));
}

#[tokio::test]
async fn rest_product_feldera_program_rejects_case_insensitive_duplicate_input_relation() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        json!([
            {
                "name": "scores",
                "case_sensitive": false,
                "fields": [
                    { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "score", "columntype": { "type": "BIGINT", "nullable": false } },
                    { "name": "delta", "columntype": { "type": "BIGINT", "nullable": false } }
                ]
            },
            {
                "name": "SCORES",
                "case_sensitive": false,
                "fields": [
                    { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "score", "columntype": { "type": "BIGINT", "nullable": false } },
                    { "name": "delta", "columntype": { "type": "BIGINT", "nullable": false } }
                ]
            }
        ]),
        default_fake_feldera_program_outputs(),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-case-duplicate-input").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_case_duplicate_input",
            "source_kind": "feldera_program",
            "input_relations": [input_schema],
            "sql": "CREATE MATERIALIZED VIEW feldera_http_scores_by_user AS SELECT user_id, SUM(score) AS sum, COUNT(*) AS count, AVG(score) AS avg_score FROM scores GROUP BY user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "failed");
    assert!(
        run.1["outcomes"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("duplicate input relation `SCORES`"),
        "run body: {}",
        run.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    assert!(!requests.iter().any(|request| request["kind"] == "ingress"));
}

#[tokio::test]
async fn rest_product_feldera_program_rejects_non_materialized_output_view() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        default_fake_feldera_program_inputs(),
        json!([{
            "name": "by_user",
            "materialized": false,
            "fields": [
                { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                { "name": "score", "columntype": { "type": "BIGINT", "nullable": false } }
            ],
            "primary_key": ["user_id"]
        }]),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-non-materialized-output").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_non_materialized",
            "input_relations": [input_schema],
            "output_relation_ids": ["by_user"],
            "sql": "CREATE VIEW by_user AS SELECT user_id, score FROM scores",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["source_kind"], "feldera_program");

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "failed");
    assert!(
        run.1["outcomes"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("Feldera output view `by_user` is not materialized"),
        "run body: {}",
        run.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    let compile_body = requests
        .iter()
        .find(|request| request["kind"] == "compile_put")
        .expect("compile request must be sent");
    let program_code = compile_body["body"]["program_code"].as_str().unwrap();
    assert!(program_code.contains("CREATE VIEW by_user AS SELECT user_id, score FROM scores"));
    assert!(
        !program_code.contains("CREATE MATERIALIZED VIEW \"score_program_non_materialized\" AS")
    );
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    assert!(!requests.iter().any(|request| request["kind"] == "ingress"));
}

#[tokio::test]
async fn rest_product_feldera_program_rejects_connector_bearing_output_view() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        default_fake_feldera_program_inputs(),
        json!([{
            "name": "by_user",
            "materialized": true,
            "fields": [
                { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                { "name": "total_score", "columntype": { "type": "BIGINT", "nullable": false } }
            ],
            "primary_key": ["user_id"],
            "properties": {
                "connectors": [{ "name": "test-output-sink" }]
            }
        }]),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-connector-output").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_connector_output",
            "source_kind": "feldera_program",
            "input_relations": [input_schema],
            "output_relation_ids": ["by_user"],
            "sql": "CREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "failed");
    assert!(
        run.1["outcomes"][0]["reason"].as_str().unwrap().contains(
            "Feldera output view `by_user` contains unmanaged connector/external IO properties"
        ),
        "run body: {}",
        run.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    let compile_body = requests
        .iter()
        .find(|request| request["kind"] == "compile_put")
        .expect("compile request must be sent");
    let program_code = compile_body["body"]["program_code"].as_str().unwrap();
    assert!(program_code.contains("CREATE MATERIALIZED VIEW by_user AS"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    assert!(!requests.iter().any(|request| request["kind"] == "ingress"));
}

#[tokio::test]
async fn rest_product_feldera_program_rejects_duplicate_output_relation_names() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        default_fake_feldera_program_inputs(),
        json!([
            {
                "name": "by_user",
                "materialized": true,
                "fields": [
                    { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "total_score", "columntype": { "type": "BIGINT", "nullable": false } }
                ],
                "primary_key": ["user_id"]
            },
            {
                "name": "by_user",
                "materialized": true,
                "fields": [
                    { "name": "region", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "event_count", "columntype": { "type": "BIGINT", "nullable": false } }
                ],
                "primary_key": ["region"]
            }
        ]),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-duplicate-output").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_duplicate_output",
            "source_kind": "feldera_program",
            "input_relations": [input_schema],
            "sql": "CREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id; CREATE MATERIALIZED VIEW by_region AS SELECT user_id AS region, COUNT(*) AS event_count FROM scores GROUP BY user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "failed");
    assert!(
        run.1["outcomes"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("Feldera compiled program contains duplicate output view `by_user`"),
        "run body: {}",
        run.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    assert!(!requests.iter().any(|request| request["kind"] == "ingress"));
}

#[tokio::test]
async fn rest_product_feldera_program_rejects_case_insensitive_duplicate_output_relation_names() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        default_fake_feldera_program_inputs(),
        json!([
            {
                "name": "by_user",
                "case_sensitive": false,
                "materialized": true,
                "fields": [
                    { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "total_score", "columntype": { "type": "BIGINT", "nullable": false } }
                ],
                "primary_key": ["user_id"]
            },
            {
                "name": "BY_USER",
                "case_sensitive": false,
                "materialized": true,
                "fields": [
                    { "name": "region", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "event_count", "columntype": { "type": "BIGINT", "nullable": false } }
                ],
                "primary_key": ["region"]
            }
        ]),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-case-duplicate-output").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_case_duplicate_output",
            "source_kind": "feldera_program",
            "input_relations": [input_schema],
            "sql": "CREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id; CREATE MATERIALIZED VIEW BY_USER AS SELECT user_id AS region, COUNT(*) AS event_count FROM scores GROUP BY user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "failed");
    assert!(
        run.1["outcomes"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("Feldera compiled program contains duplicate output view `BY_USER`"),
        "run body: {}",
        run.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    assert!(!requests.iter().any(|request| request["kind"] == "ingress"));
}

#[tokio::test]
async fn rest_product_feldera_program_rejects_duplicate_output_fields() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        default_fake_feldera_program_inputs(),
        json!([{
            "name": "by_user",
            "case_sensitive": false,
            "materialized": true,
            "fields": [
                { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                { "name": "USER_ID", "columntype": { "type": "BIGINT", "nullable": false } }
            ],
            "primary_key": []
        }]),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-duplicate-output-fields").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_duplicate_output_fields",
            "source_kind": "feldera_program",
            "input_relations": [input_schema],
            "sql": "CREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "failed");
    assert!(
        run.1["outcomes"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("Feldera output view `by_user` contains duplicate field `USER_ID`"),
        "run body: {}",
        run.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    assert!(!requests.iter().any(|request| request["kind"] == "ingress"));
}

#[tokio::test]
async fn rest_product_feldera_program_rejects_unknown_primary_key_output_field() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        default_fake_feldera_program_inputs(),
        json!([{
            "name": "by_user",
            "materialized": true,
            "fields": [
                { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                { "name": "total_score", "columntype": { "type": "BIGINT", "nullable": false } }
            ],
            "primary_key": ["missing_user_id"]
        }]),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-unknown-primary-key-output").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_unknown_primary_key_output",
            "source_kind": "feldera_program",
            "input_relations": [input_schema],
            "sql": "CREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "failed");
    assert!(
        run.1["outcomes"][0]["reason"].as_str().unwrap().contains(
            "Feldera output view `by_user` primary_key entry `missing_user_id` does not reference a field"
        ),
        "run body: {}",
        run.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    assert!(!requests.iter().any(|request| request["kind"] == "ingress"));
}

#[tokio::test]
async fn rest_product_feldera_program_rejects_output_relation_hint_mismatch() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        default_fake_feldera_program_inputs(),
        json!([
            {
                "name": "by_user",
                "materialized": true,
                "fields": [
                    { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "total_score", "columntype": { "type": "BIGINT", "nullable": false } }
                ],
                "primary_key": ["user_id"]
            },
            {
                "name": "unexpected",
                "materialized": true,
                "fields": [
                    { "name": "region", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "event_count", "columntype": { "type": "BIGINT", "nullable": false } }
                ],
                "primary_key": ["region"]
            }
        ]),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-output-hint-mismatch").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_output_hint_mismatch",
            "source_kind": "feldera_program",
            "input_relations": [input_schema],
            "output_relation_ids": ["by_user", "by_region"],
            "sql": "CREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id; CREATE MATERIALIZED VIEW by_region AS SELECT user_id AS region, COUNT(*) AS event_count FROM scores GROUP BY user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "failed");
    assert!(
        run.1["outcomes"][0]["reason"].as_str().unwrap().contains(
            "resolved Feldera program output relations do not match requested output_relation_ids"
        ),
        "run body: {}",
        run.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    assert!(!requests.iter().any(|request| request["kind"] == "ingress"));
}

#[tokio::test]
async fn rest_product_feldera_program_accepts_case_insensitive_output_relation_hints() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        default_fake_feldera_program_inputs(),
        json!([
            {
                "name": "BY_USER",
                "case_sensitive": false,
                "materialized": true,
                "fields": [
                    { "name": "USER_ID", "case_sensitive": false, "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "TOTAL_SCORE", "case_sensitive": false, "columntype": { "type": "BIGINT", "nullable": false } }
                ],
                "primary_key": ["USER_ID"]
            },
            {
                "name": "BY_REGION",
                "case_sensitive": false,
                "materialized": true,
                "fields": [
                    { "name": "REGION", "case_sensitive": false, "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "EVENT_COUNT", "case_sensitive": false, "columntype": { "type": "BIGINT", "nullable": false } }
                ],
                "primary_key": ["REGION"]
            }
        ]),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-output-hint-case-insensitive").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_output_hint_case_insensitive",
            "source_kind": "feldera_program",
            "input_relations": [input_schema],
            "output_relation_ids": ["by_user", "by_region"],
            "sql": "CREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id; CREATE MATERIALIZED VIEW by_region AS SELECT user_id AS region, COUNT(*) AS event_count FROM scores GROUP BY user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 0, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "activated");

    let detail = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/score_program_output_hint_case_insensitive",
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(detail.1["output_relations"][0]["relation_id"], "by_user");
    assert_eq!(detail.1["output_relations"][0]["relation_name"], "BY_USER");
    assert_eq!(
        detail.1["output_query_endpoints"][0],
        "/v1/views/score_program_output_hint_case_insensitive/outputs/by_user/query"
    );

    let query = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/score_program_output_hint_case_insensitive/outputs/by_user/query",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(query.1["rows"][0]["USER_ID"], "u1");
    assert_eq!(query.1["rows"][0]["TOTAL_SCORE"], 12);

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    assert!(requests.iter().any(|request| {
        request["kind"] == "query"
            && request["params"]["sql"]
                .as_str()
                .is_some_and(|sql| sql.contains("SELECT * FROM \"BY_USER\""))
    }));
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_semantic_warning_fails_compile_job_without_fixture_fallback(
) {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_error(
        "Success",
        json!({
            "sql_compilation": {
                "exit_code": 0,
                "messages": [{
                    "warning": true,
                    "error_type": "ORDER BY is ignored",
                    "message": "ORDER BY clause is currently ignored\n(the result will contain the correct data, but the data is not ordered)"
                }]
            }
        }),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-semantic-warning").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_order_warning",
            "urlPath": "/feldera-http/scores/order-warning",
            "input_relations": [input_schema],
            "sql": "select user_id, score from scores order by score desc limit 2",
            "source_kind": "standing_view",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "failed");
    assert!(
        run.1["outcomes"][0]["reason"]
            .as_str()
            .unwrap()
            .contains("ORDER BY clause is currently ignored"),
        "run body: {}",
        run.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
}

#[tokio::test]
async fn rest_product_pipeline_manager_runtime_compile_rejects_sql_compiled_semantic_warning_without_timeout(
) {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_error(
        "SqlCompiled",
        json!({
            "sql_compilation": {
                "exit_code": 0,
                "messages": [{
                    "warning": true,
                    "error_type": "ORDER BY is ignored",
                    "message": "ORDER BY clause is currently ignored\n(the result will contain the correct data, but the data is not ordered)"
                }]
            }
        }),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let compiler_request = FelderaCompileRequestV1 {
        view_id: "feldera_http_scores_order_warning_runtime".to_string(),
        sql: "select user_id, score from scores order by score desc limit 2".to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema],
        output_contract: velorix_core::feldera_artifact::OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();

    let error = backend
        .compile(FelderaCompilerBackendRequest {
            job_id: "feldera-http-scores-order-warning-runtime".to_string(),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "test-spec-hash".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog],
        })
        .await
        .expect_err("SqlCompiled semantic warnings must fail before runtime compile polling");
    let error_debug = format!("{error:?}");
    assert!(
        error_debug.contains("ORDER BY clause is currently ignored"),
        "unexpected semantic warning error: {error_debug}"
    );

    let requests = fake_feldera.requests.lock().unwrap();
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
}

#[tokio::test]
async fn rest_product_pipeline_manager_compiler_receives_sql_beyond_linked_dbsp_shape() {
    let view_id = "feldera_http_scores_ranked";
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_outputs_and_status(
        json!([{
            "name": view_id,
            "case_sensitive": false,
            "materialized": true,
            "primary_key": [],
            "fields": [
                {
                    "name": "user_id",
                    "case_sensitive": false,
                    "columntype": {
                        "type": "VARCHAR",
                        "nullable": false
                    }
                },
                {
                    "name": "score",
                    "case_sensitive": false,
                    "columntype": {
                        "type": "BIGINT",
                        "nullable": false
                    }
                },
                {
                    "name": "rn",
                    "case_sensitive": false,
                    "columntype": {
                        "type": "BIGINT",
                        "nullable": false
                    }
                }
            ]
        }]),
        "CompilingRust",
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-window-sql").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let sql = "select user_id, score, row_number() over (partition by user_id order by score) as rn from scores where score > 0";
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": view_id,
            "urlPath": "/feldera-http/scores/ranked",
            "input_relations": [input_schema],
            "sql": sql,
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["skipped"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "compile_validated");

    let read_back = request_json(
        app,
        Method::GET,
        "/v1/views/feldera_http_scores_ranked",
        None,
    )
    .await;
    assert_eq!(read_back.0, StatusCode::OK, "view body: {}", read_back.1);
    assert_eq!(read_back.1["lifecycle"]["compile_status"], "success");
    assert_eq!(
        read_back.1["lifecycle"]["deployment_status"],
        "not_deployed"
    );
    assert_eq!(read_back.1["output_relations"][0]["relation_id"], view_id);

    let requests = fake_feldera.requests.lock().unwrap();
    let compile = requests
        .iter()
        .find(|request| request["kind"] == "compile_put")
        .expect("compile request was recorded");
    let program_code = compile["body"]["program_code"].as_str().unwrap();
    assert!(program_code.contains("CREATE MATERIALIZED VIEW \"feldera_http_scores_ranked\" AS"));
    assert!(program_code.contains("row_number() over"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
}

#[tokio::test]
async fn rest_product_worker_validates_pending_view_through_feldera_pipeline_manager_backend() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager().await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, store) = api_state_memory("feldera-pipeline-manager-compiler-worker").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "urlPath": "/feldera-http/scores/by-user",
            "input_relations": [input_schema],
            "sql": "select user_id, sum(score) as sum, count(*) as count, avg(score) as avg_score from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["skipped"], 0, "run body: {}", run.1);
    assert_eq!(
        run.1["outcomes"][0]["status"], "activated",
        "run body: {}",
        run.1
    );

    let read_back = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/feldera_http_scores_by_user",
        None,
    )
    .await;
    assert_eq!(read_back.0, StatusCode::OK, "view body: {}", read_back.1);
    assert_eq!(read_back.1["execution_mode"], "standing_runtime");
    assert_eq!(read_back.1["lifecycle"]["compile_status"], "success");
    assert_eq!(read_back.1["lifecycle"]["deployment_status"], "running");
    assert_eq!(read_back.1["query_enabled"], true);

    let registry = MaterializedViewRegistry::new(Arc::clone(&store));
    let active = registry
        .read_active("feldera_http_scores_by_user")
        .await
        .unwrap();
    let output = active.spec.output_relations.first().unwrap();
    assert_eq!(output.relation_version, "feldera-program-v7");
    assert_eq!(
        output.columns,
        vec![
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
            ColumnSchema {
                name: "avg_score".to_string(),
                data_type: SqlDataType::Float64,
                nullable: true,
            },
        ]
    );

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "user_id": "u1", "score": 5, "delta": 1 },
                { "user_id": "u1", "score": 7, "delta": 1 },
                { "user_id": "u2", "score": -1, "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = request_json(
        app.clone(),
        Method::GET,
        "/v1/api/feldera-http/scores/by-user",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([{ "user_id": "u1", "sum": 12, "count": 2, "avg_score": 6.0 }])
    );

    let negative_weight_ingest = request_json(
        app,
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 3,
            "rows": [
                { "user_id": "u1", "score": 5, "delta": -1 }
            ]
        })),
    )
    .await;
    assert_eq!(
        negative_weight_ingest.0,
        StatusCode::CREATED,
        "ingest body: {}",
        negative_weight_ingest.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    let compile = requests
        .iter()
        .find(|request| request["kind"] == "compile_put")
        .expect("compile request was recorded");
    assert_eq!(compile["headers"]["authorization"], "Bearer feldera-secret");
    assert_eq!(compile["body"]["program_config"]["profile"], "dev");
    assert_eq!(compile["body"]["runtime_config"]["workers"], 1);
    assert!(compile["body"]["program_code"]
        .as_str()
        .unwrap()
        .contains("CREATE MATERIALIZED VIEW \"feldera_http_scores_by_user\" AS"));
    assert!(!compile["body"]["program_code"]
        .as_str()
        .unwrap()
        .contains("\"delta\" BIGINT"));
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    let first_ingress = requests
        .iter()
        .find(|request| request["kind"] == "ingress")
        .expect("ingress request was recorded");
    assert_eq!(
        requests
            .iter()
            .filter(|request| request["kind"] == "ingress")
            .count(),
        2
    );
    assert_eq!(first_ingress["table_name"], "scores");
    assert_eq!(
        first_ingress["body"],
        json!([
            { "insert": { "user_id": "u1", "score": 5 } },
            { "insert": { "user_id": "u1", "score": 7 } },
            { "insert": { "user_id": "u2", "score": -1 } }
        ])
    );
    let second_ingress = requests
        .iter()
        .filter(|request| request["kind"] == "ingress")
        .nth(1)
        .expect("second ingress request was recorded");
    assert_eq!(second_ingress["table_name"], "scores");
    assert_eq!(
        second_ingress["body"],
        json!([{ "delete": { "user_id": "u1", "score": 5 } }])
    );
    let query = requests
        .iter()
        .find(|request| request["kind"] == "query")
        .expect("query request was recorded");
    assert_eq!(query["params"]["format"], "json");
    assert_eq!(
        query["params"]["sql"],
        "SELECT * FROM \"feldera_http_scores_by_user\""
    );
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_promoted_api_pushes_template_sql_to_feldera() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager().await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-template-pushdown").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "urlPath": "/feldera-http/scores/:user_id",
            "input_relations": [input_schema],
            "sql": "select user_id, sum(score) as sum, count(*) as count, avg(score) as avg_score from scores where score > 0 group by user_id",
            "request": [{
                "fieldName": "user_id",
                "fieldIn": "path",
                "type": "string",
                "validators": ["required", "string"]
            }],
            "sql_template": "select '{{ context.params.literal }}' as literal, user_id, sum, count from feldera_http_scores_by_user where user_id = {{ context.params.user_id | is_required | is_string }} -- {{ context.params.comment }}\norder by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let query = request_json(
        app.clone(),
        Method::GET,
        "/v1/api/feldera-http/scores/u1",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    let limited_query = request_json(
        app,
        Method::GET,
        "/v1/api/feldera-http/scores/u1?max_rows=1",
        None,
    )
    .await;
    assert_eq!(
        limited_query.0,
        StatusCode::OK,
        "limited query body: {}",
        limited_query.1
    );
    let requests = fake_feldera.requests.lock().unwrap();
    let queries = requests
        .iter()
        .filter(|request| request["kind"] == "query")
        .map(|request| request["params"]["sql"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        queries,
        vec![
            "PREPARE velorix_query AS select '{{ context.params.literal }}' as literal, user_id, sum, count from feldera_http_scores_by_user where user_id = $1 -- {{ context.params.comment }}\norder by user_id;\nEXECUTE velorix_query('u1');",
            "PREPARE velorix_query AS SELECT * FROM (select '{{ context.params.literal }}' as literal, user_id, sum, count from feldera_http_scores_by_user where user_id = $1 -- {{ context.params.comment }}\norder by user_id) AS \"velorix_limited_query\" LIMIT 2;\nEXECUTE velorix_query('u1');"
        ]
    );
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_promoted_api_shapes_complex_response_schema() {
    let view_id = "feldera_http_complex_profile";
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_outputs(json!([{
        "name": view_id,
        "case_sensitive": false,
        "materialized": true,
        "primary_key": ["user_id"],
        "fields": [
            {
                "name": "user_id",
                "case_sensitive": false,
                "columntype": {
                    "type": "VARCHAR",
                    "nullable": false
                }
            },
            {
                "name": "score_window",
                "case_sensitive": false,
                "columntype": {
                    "type": "ARRAY",
                    "nullable": false,
                    "component": {
                        "type": "BIGINT",
                        "nullable": true
                    }
                }
            },
            {
                "name": "profile",
                "case_sensitive": false,
                "columntype": {
                    "type": "STRUCT",
                    "nullable": false,
                    "fields": [
                        {
                            "name": "name",
                            "columntype": {
                                "type": "VARCHAR",
                                "nullable": false
                            }
                        },
                        {
                            "name": "tier",
                            "columntype": {
                                "type": "BIGINT",
                                "nullable": true
                            }
                        }
                    ]
                }
            },
            {
                "name": "maybe_count",
                "case_sensitive": false,
                "columntype": {
                    "type": "BIGINT",
                    "nullable": true
                }
            }
        ]
    }]))
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-complex-response").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": view_id,
            "urlPath": "/feldera-http/profiles/complex",
            "input_relations": [input_schema],
            "sql": "select user_id, ARRAY[sum(score), cast(null as bigint)] as score_window, cast(row(user_id, count(*)) as row(name varchar, tier bigint)) as profile from scores group by user_id",
            "response_schema": {
                "columns": [
                    {
                        "name": "user_id",
                        "type": "string",
                        "source": "user_id"
                    },
                    {
                        "name": "scores",
                        "type": "array",
                        "source": "score_window"
                    },
                    {
                        "name": "profile",
                        "type": "object",
                        "source": "profile"
                    },
                    {
                        "name": "maybe_count",
                        "type": "int64",
                        "source": "maybe_count"
                    }
                ]
            },
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let query = request_json(
        app.clone(),
        Method::GET,
        "/v1/api/feldera-http/profiles/complex",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(
        query.1["rows"],
        json!([{
            "user_id": "u1",
            "scores": [8, null, 13],
            "profile": {
                "name": "Ada",
                "tier": 2
            },
            "maybe_count": null
        }])
    );

    let openapi = request_json(app, Method::GET, "/v1/openapi.json", None).await;
    assert_eq!(openapi.0, StatusCode::OK, "openapi body: {}", openapi.1);
    let response_properties = &openapi.1["paths"]["/v1/api/feldera-http/profiles/complex"]["get"]
        ["responses"]["200"]["content"]["application/json"]["schema"]["properties"]["rows"]
        ["items"]["properties"];
    assert_eq!(response_properties["scores"]["type"], "array");
    assert_eq!(response_properties["scores"]["nullable"], true);
    assert_eq!(response_properties["profile"]["type"], "object");
    assert_eq!(response_properties["profile"]["nullable"], true);
    assert_eq!(response_properties["maybe_count"]["type"], "integer");
    assert_eq!(response_properties["maybe_count"]["nullable"], true);
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_promoted_api_binds_array_query_parameter() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager().await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-array-query-param").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "urlPath": "/feldera-http/scores/filter",
            "input_relations": [input_schema],
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "request": [{
                "fieldName": "user_ids",
                "fieldIn": "query",
                "type": "array",
                "validators": ["required", "array(element=string)"]
            }],
            "sql_template": "select user_id, sum, count from feldera_http_scores_by_user where user_id in unnest({{ context.params.user_ids | is_required | is_array(element=string) }}) order by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let query = request_json(
        app,
        Method::GET,
        "/v1/api/feldera-http/scores/filter?user_ids=%5B%22u1%22%2C%22u%272%22%5D&max_rows=1",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);

    let requests = fake_feldera.requests.lock().unwrap();
    let queries = requests
        .iter()
        .filter(|request| request["kind"] == "query")
        .map(|request| request["params"]["sql"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        queries,
        vec!["PREPARE velorix_query AS SELECT * FROM (select user_id, sum, count from feldera_http_scores_by_user where user_id IN ($1, $2) order by user_id) AS \"velorix_limited_query\" LIMIT 2;\nEXECUTE velorix_query('u1', 'u''2');"]
    );
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_defers_template_sql_grammar_to_feldera() {
    let view_id = "feldera_http_scores_template_grammar";
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_outputs(json!([{
        "name": view_id,
        "case_sensitive": false,
        "materialized": true,
        "primary_key": ["user_id"],
        "fields": [
            { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
            { "name": "sum", "columntype": { "type": "BIGINT", "nullable": false } }
        ]
    }]))
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) =
        api_state_memory("feldera-pipeline-manager-template-feldera-grammar").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": view_id,
            "urlPath": "/feldera-http/scores/template-grammar/:user_id",
            "input_relations": [input_schema],
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "request": [{
                "fieldName": "user_id",
                "fieldIn": "path",
                "type": "string",
                "validators": ["required", "string"]
            }],
            "sql_template": "select {{ context.params.user_id | is_required | is_string }} as user_id FELDERA_RUNTIME_ONLY",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let query = request_json(
        app,
        Method::GET,
        "/v1/api/feldera-http/scores/template-grammar/u1",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);

    let requests = fake_feldera.requests.lock().unwrap();
    let query_sql = requests
        .iter()
        .filter(|request| request["kind"] == "query")
        .map(|request| request["params"]["sql"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        query_sql,
        vec!["PREPARE velorix_query AS select $1 as user_id FELDERA_RUNTIME_ONLY;\nEXECUTE velorix_query('u1');"]
    );
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_does_not_normalize_template_sql() {
    let view_id = "feldera_http_scores_template_normalize";
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_outputs(json!([{
        "name": view_id,
        "case_sensitive": false,
        "materialized": true,
        "primary_key": ["key"],
        "fields": [
            { "name": "key", "columntype": { "type": "VARCHAR", "nullable": false } },
            { "name": "value", "columntype": { "type": "VARCHAR", "nullable": false } },
            { "name": "weight", "columntype": { "type": "BIGINT", "nullable": false } }
        ]
    }]))
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-template-no-normalize").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let sql_template = format!("select key, value, weight from {view_id}");
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": view_id,
            "urlPath": "/feldera-http/scores/template-normalize",
            "input_relations": [input_schema],
            "sql": "select user_id as key, cast(sum(score) as varchar) as value, count(*) as weight from scores where score > 0 group by user_id",
            "sql_template": sql_template,
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let query = request_json(
        app,
        Method::GET,
        "/v1/api/feldera-http/scores/template-normalize",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);

    let requests = fake_feldera.requests.lock().unwrap();
    let query_sql = requests
        .iter()
        .filter(|request| request["kind"] == "query")
        .map(|request| request["params"]["sql"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(query_sql, vec![sql_template.as_str()]);
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_view_query_posts_caller_sql_to_feldera() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager().await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-view-caller-sql").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
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

    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "input_relations": [input_schema],
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let caller_sql = "WITH scoped AS (SELECT user_id, sum FROM feldera_http_scores_by_user) SELECT user_id, sum, '{{ context.params.literal }}' AS literal FROM scoped -- {{ context.params.comment }}\nWHERE user_id = {{ context.params.user_id | is_required | is_string }} AND sum >= {{ context.params.min_sum | is_required | is_integer(min=0) }} ORDER BY user_id";
    let expected_sql = "PREPARE velorix_query AS WITH scoped AS (SELECT user_id, sum FROM feldera_http_scores_by_user) SELECT user_id, sum, '{{ context.params.literal }}' AS literal FROM scoped -- {{ context.params.comment }}\nWHERE user_id = $1 AND sum >= $2 ORDER BY user_id;\nEXECUTE velorix_query('u1', 1);";
    let query = request_json(
        app.clone(),
        Method::POST,
        "/v1/views/feldera_http_scores_by_user/query",
        Some(json!({
            "sql": caller_sql,
            "parameters": {
                "user_id": "u1",
                "min_sum": 1
            }
        })),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);

    let literal_braces_sql =
        "SELECT '{{ not_a_parameter }}' AS literal FROM feldera_http_scores_by_user";
    let literal_braces_query = request_json(
        app.clone(),
        Method::POST,
        "/v1/views/feldera_http_scores_by_user/query",
        Some(json!({
            "sql": literal_braces_sql
        })),
    )
    .await;
    assert_eq!(
        literal_braces_query.0,
        StatusCode::OK,
        "literal braces query body: {}",
        literal_braces_query.1
    );

    let insert_alias_sql = "SELECT 'literal' AS insert FROM feldera_http_scores_by_user";
    let insert_alias_query = request_json(
        app.clone(),
        Method::POST,
        "/v1/views/feldera_http_scores_by_user/query",
        Some(json!({
            "sql": insert_alias_sql
        })),
    )
    .await;
    assert_eq!(
        insert_alias_query.0,
        StatusCode::OK,
        "insert alias query body: {}",
        insert_alias_query.1
    );
    assert_eq!(
        insert_alias_query.1["rows"],
        json!([{ "insert": "literal" }])
    );

    let delete_alias_sql = "SELECT 'literal' AS delete FROM feldera_http_scores_by_user";
    let delete_alias_query = request_json(
        app,
        Method::POST,
        "/v1/views/feldera_http_scores_by_user/query",
        Some(json!({
            "sql": delete_alias_sql
        })),
    )
    .await;
    assert_eq!(
        delete_alias_query.0,
        StatusCode::OK,
        "delete alias query body: {}",
        delete_alias_query.1
    );
    assert_eq!(
        delete_alias_query.1["rows"],
        json!([{ "delete": "literal" }])
    );

    let requests = fake_feldera.requests.lock().unwrap();
    let query_sql = requests
        .iter()
        .filter(|request| request["kind"] == "query")
        .map(|request| request["params"]["sql"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        query_sql,
        vec![
            expected_sql,
            literal_braces_sql,
            insert_alias_sql,
            delete_alias_sql
        ]
    );
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_pages_view_and_promoted_api_queries() {
    let view_id = "feldera_http_scores_page";
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_outputs(json!([{
        "name": view_id,
        "case_sensitive": false,
        "materialized": true,
        "primary_key": ["user_id"],
        "fields": [
            {
                "name": "user_id",
                "case_sensitive": false,
                "columntype": {
                    "type": "VARCHAR",
                    "nullable": false
                }
            },
            {
                "name": "sum",
                "case_sensitive": false,
                "columntype": {
                    "type": "BIGINT",
                    "nullable": false
                }
            },
            {
                "name": "count",
                "case_sensitive": false,
                "columntype": {
                    "type": "BIGINT",
                    "nullable": false
                }
            }
        ]
    }]))
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-query-pages").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": view_id,
            "urlPath": "/feldera-http/scores/page",
            "input_relations": [input_schema],
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "sql_template": "select user_id, sum, count from feldera_http_scores_page order by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let direct_first = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/feldera_http_scores_page/outputs/feldera_http_scores_page/query?max_rows=2",
        None,
    )
    .await;
    assert_eq!(
        direct_first.0,
        StatusCode::OK,
        "direct first body: {}",
        direct_first.1
    );
    assert_eq!(
        direct_first.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 },
            { "user_id": "u2", "sum": 9, "count": 1 }
        ])
    );
    assert_eq!(direct_first.1["next_page_token"], "offset:2");

    let direct_second = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/feldera_http_scores_page/outputs/feldera_http_scores_page/query?max_rows=2&page_token=offset:2",
        None,
    )
    .await;
    assert_eq!(
        direct_second.0,
        StatusCode::OK,
        "direct second body: {}",
        direct_second.1
    );
    assert_eq!(
        direct_second.1["rows"],
        json!([{ "user_id": "u3", "sum": 13, "count": 1 }])
    );
    assert!(direct_second.1.get("next_page_token").is_none());

    let api_first = request_json(
        app.clone(),
        Method::GET,
        "/v1/api/feldera-http/scores/page?max_rows=2",
        None,
    )
    .await;
    assert_eq!(
        api_first.0,
        StatusCode::OK,
        "api first body: {}",
        api_first.1
    );
    assert_eq!(
        api_first.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 },
            { "user_id": "u2", "sum": 9, "count": 1 }
        ])
    );
    assert_eq!(api_first.1["next_page_token"], "offset:2");

    let api_second = request_json(
        app.clone(),
        Method::GET,
        "/v1/api/feldera-http/scores/page?max_rows=2&page_token=offset:2",
        None,
    )
    .await;
    assert_eq!(
        api_second.0,
        StatusCode::OK,
        "api second body: {}",
        api_second.1
    );
    assert_eq!(
        api_second.1["rows"],
        json!([{ "user_id": "u3", "sum": 13, "count": 1 }])
    );
    assert!(api_second.1.get("next_page_token").is_none());

    let raw_sql_first = request_json(
        app.clone(),
        Method::POST,
        "/v1/views/feldera_http_scores_page/outputs/feldera_http_scores_page/query",
        Some(json!({
            "sql": "select user_id, sum, count from feldera_http_scores_page where sum >= 0 order by user_id",
            "max_rows": 2
        })),
    )
    .await;
    assert_eq!(
        raw_sql_first.0,
        StatusCode::OK,
        "raw SQL first body: {}",
        raw_sql_first.1
    );
    assert_eq!(
        raw_sql_first.1["rows"],
        json!([
            { "user_id": "u1", "sum": 12, "count": 2 },
            { "user_id": "u2", "sum": 9, "count": 1 }
        ])
    );
    assert_eq!(raw_sql_first.1["next_page_token"], "offset:2");

    let invalid = request_json(
        app,
        Method::GET,
        "/v1/views/feldera_http_scores_page/outputs/feldera_http_scores_page/query?max_rows=2&page_token=u2",
        None,
    )
    .await;
    assert_eq!(
        invalid.0,
        StatusCode::BAD_REQUEST,
        "invalid body: {}",
        invalid.1
    );
    assert!(
        invalid.1["error"]
            .as_str()
            .unwrap()
            .contains("invalid Feldera pipeline-manager page_token"),
        "invalid body: {}",
        invalid.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    let queries = requests
        .iter()
        .filter(|request| request["kind"] == "query")
        .map(|request| request["params"]["sql"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        queries,
        vec![
            "SELECT * FROM (SELECT * FROM \"feldera_http_scores_page\") AS \"velorix_limited_query\" LIMIT 3",
            "SELECT * FROM (SELECT * FROM \"feldera_http_scores_page\") AS \"velorix_limited_query\" LIMIT 3 OFFSET 2",
            "SELECT * FROM (select user_id, sum, count from feldera_http_scores_page order by user_id) AS \"velorix_limited_query\" LIMIT 3",
            "SELECT * FROM (select user_id, sum, count from feldera_http_scores_page order by user_id) AS \"velorix_limited_query\" LIMIT 3 OFFSET 2",
            "SELECT * FROM (select user_id, sum, count from feldera_http_scores_page where sum >= 0 order by user_id) AS \"velorix_limited_query\" LIMIT 3"
        ],
        "queries: {queries:?}"
    );
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_direct_output_query_ignores_other_output_api_template(
) {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_outputs(json!([
        {
            "name": "by_user",
            "materialized": true,
            "primary_key": ["user_id"],
            "fields": [
                { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                { "name": "sum", "columntype": { "type": "BIGINT", "nullable": false } }
            ]
        },
        {
            "name": "by_region",
            "materialized": true,
            "primary_key": ["region"],
            "fields": [
                { "name": "region", "columntype": { "type": "VARCHAR", "nullable": false } },
                { "name": "count", "columntype": { "type": "BIGINT", "nullable": false } }
            ]
        }
    ]))
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-multi-output-api-bound").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program",
            "source_kind": "feldera_program",
            "urlPath": "/feldera-http/scores/by-user/:user_id",
            "outputRelationId": "by_user",
            "output_relation_ids": ["by_user", "by_region"],
            "input_relations": [input_schema],
            "request": [{
                "fieldName": "user_id",
                "fieldIn": "path",
                "type": "string",
                "validators": ["required", "string"]
            }],
            "sql_template": "select user_id, sum from by_user where user_id = {{ context.params.user_id | is_required | is_string }}",
            "sql": "CREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS sum FROM scores GROUP BY user_id; CREATE MATERIALIZED VIEW by_region AS SELECT region, COUNT(*) AS count FROM scores GROUP BY region",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let api_query = request_json(
        app.clone(),
        Method::GET,
        "/v1/api/feldera-http/scores/by-user/u1",
        None,
    )
    .await;
    assert_eq!(
        api_query.0,
        StatusCode::OK,
        "api query body: {}",
        api_query.1
    );

    let output_query = request_json(
        app,
        Method::GET,
        "/v1/views/score_program/outputs/by_region/query",
        None,
    )
    .await;
    assert_eq!(
        output_query.0,
        StatusCode::OK,
        "output query body: {}",
        output_query.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    let query_sql = requests
        .iter()
        .filter(|request| request["kind"] == "query")
        .map(|request| request["params"]["sql"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        query_sql,
        vec![
            "PREPARE velorix_query AS select user_id, sum from by_user where user_id = $1;\nEXECUTE velorix_query('u1');",
            "SELECT * FROM \"by_region\""
        ]
    );
}

#[tokio::test]
async fn rest_product_feldera_program_direct_output_query_supports_encoded_output_identifier() {
    let output_id = "Order/Summary \"By User\"";
    let encoded_output_id = "Order%2FSummary%20%22By%20User%22";
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_outputs(json!([{
        "name": output_id,
        "materialized": true,
        "primary_key": ["user_id"],
        "fields": [
            { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
            { "name": "sum", "columntype": { "type": "BIGINT", "nullable": false } }
        ]
    }]))
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-encoded-output-id").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_encoded_output",
            "source_kind": "feldera_program",
            "output_relation_ids": [output_id],
            "input_relations": [input_schema],
            "sql": "CREATE MATERIALIZED VIEW \"Order/Summary \"\"By User\"\"\" AS SELECT user_id, SUM(score) AS sum FROM scores GROUP BY user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let detail = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/score_program_encoded_output",
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(
        detail.1["output_query_endpoints"],
        json!([format!(
            "/v1/views/score_program_encoded_output/outputs/{encoded_output_id}/query"
        )])
    );

    let output_query = request_json(
        app,
        Method::GET,
        &format!("/v1/views/score_program_encoded_output/outputs/{encoded_output_id}/query"),
        None,
    )
    .await;
    assert_eq!(
        output_query.0,
        StatusCode::OK,
        "output query body: {}",
        output_query.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    let query_sql = requests
        .iter()
        .filter(|request| request["kind"] == "query")
        .map(|request| request["params"]["sql"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        query_sql,
        vec!["SELECT * FROM \"Order/Summary \"\"By User\"\"\""]
    );
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_preserves_output_field_named_insert() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_outputs(json!([{
        "name": "insert_field_output",
        "materialized": true,
        "primary_key": [],
        "fields": [
            { "name": "insert", "columntype": { "type": "VARCHAR", "nullable": false } }
        ]
    }]))
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-output-insert-field").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_insert_field",
            "source_kind": "feldera_program",
            "output_relation_ids": ["insert_field_output"],
            "input_relations": [input_schema],
            "sql": "CREATE MATERIALIZED VIEW insert_field_output AS SELECT 'literal' AS insert",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let output_query = request_json(
        app,
        Method::GET,
        "/v1/views/score_program_insert_field/outputs/insert_field_output/query",
        None,
    )
    .await;
    assert_eq!(
        output_query.0,
        StatusCode::OK,
        "output query body: {}",
        output_query.1
    );
    assert_eq!(output_query.1["rows"], json!([{ "insert": "literal" }]));

    let requests = fake_feldera.requests.lock().unwrap();
    let query_sql = requests
        .iter()
        .filter(|request| request["kind"] == "query")
        .map(|request| request["params"]["sql"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(query_sql, vec!["SELECT * FROM \"insert_field_output\""]);
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_preserves_output_field_named_delete() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_outputs(json!([{
        "name": "delete_field_output",
        "materialized": true,
        "primary_key": [],
        "fields": [
            { "name": "delete", "columntype": { "type": "VARCHAR", "nullable": false } }
        ]
    }]))
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-program-output-delete-field").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_delete_field",
            "source_kind": "feldera_program",
            "output_relation_ids": ["delete_field_output"],
            "input_relations": [input_schema],
            "sql": "CREATE MATERIALIZED VIEW delete_field_output AS SELECT 'literal' AS delete",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let output_query = request_json(
        app,
        Method::GET,
        "/v1/views/score_program_delete_field/outputs/delete_field_output/query",
        None,
    )
    .await;
    assert_eq!(
        output_query.0,
        StatusCode::OK,
        "output query body: {}",
        output_query.1
    );
    assert_eq!(output_query.1["rows"], json!([{ "delete": "literal" }]));

    let requests = fake_feldera.requests.lock().unwrap();
    let query_sql = requests
        .iter()
        .filter(|request| request["kind"] == "query")
        .map(|request| request["params"]["sql"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(query_sql, vec!["SELECT * FROM \"delete_field_output\""]);
}

#[tokio::test]
async fn rest_product_feldera_program_discovers_outputs_without_relation_id_hints() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_outputs(json!([
        {
            "name": "by_user",
            "case_sensitive": false,
            "materialized": true,
            "primary_key": ["user_id"],
            "fields": [
                { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                { "name": "sum", "columntype": { "type": "BIGINT", "nullable": false } }
            ]
        },
        {
            "name": "by_region",
            "case_sensitive": false,
            "materialized": true,
            "primary_key": ["region"],
            "fields": [
                { "name": "region", "columntype": { "type": "VARCHAR", "nullable": false } },
                { "name": "count", "columntype": { "type": "BIGINT", "nullable": false } }
            ]
        }
    ]))
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, store) = api_state_memory("feldera-program-output-discovery").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_discovered",
            "source_kind": "feldera_program",
            "input_relations": [input_schema],
            "sql": "CREATE MATERIALIZED VIEW by_user AS SELECT user_id, SUM(score) AS sum FROM scores GROUP BY user_id; CREATE MATERIALIZED VIEW by_region AS SELECT region, COUNT(*) AS count FROM scores GROUP BY region",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let compile_job_path = compile_request_job_object_path("score_program_discovered", &view.1);
    let stored_job = store
        .get(&Path::from(compile_job_path))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let stored_job: Value = serde_json::from_slice(&stored_job).unwrap();
    assert_eq!(
        stored_job["compiler_request"]["shape"]["multi_output"],
        json!(true)
    );

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let detail = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/score_program_discovered",
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(
        detail.1["output_query_endpoints"],
        json!([
            "/v1/views/score_program_discovered/outputs/by_user/query",
            "/v1/views/score_program_discovered/outputs/by_region/query"
        ])
    );

    let output_query = request_json(
        app,
        Method::GET,
        "/v1/views/score_program_discovered/outputs/by_region/query",
        None,
    )
    .await;
    assert_eq!(
        output_query.0,
        StatusCode::OK,
        "output query body: {}",
        output_query.1
    );
    assert_eq!(
        output_query.1["rows"],
        json!([{ "region": "apac", "count": 3 }])
    );

    let requests = fake_feldera.requests.lock().unwrap();
    let compile_body = requests
        .iter()
        .find(|request| request["kind"] == "compile_put")
        .expect("compile request must be sent");
    let program_code = compile_body["body"]["program_code"].as_str().unwrap();
    assert!(program_code.contains("CREATE MATERIALIZED VIEW by_user AS"));
    assert!(program_code.contains("CREATE MATERIALIZED VIEW by_region AS"));
    assert!(!program_code.contains("CREATE MATERIALIZED VIEW \"score_program_discovered\" AS"));
}

#[tokio::test]
async fn rest_product_feldera_program_passes_cte_having_union_to_pipeline_manager() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_outputs_and_status(
        json!([{
            "name": "score_rollup",
            "case_sensitive": false,
            "materialized": true,
            "primary_key": [],
            "fields": [
                {
                    "name": "user_id",
                    "case_sensitive": false,
                    "columntype": {
                        "type": "VARCHAR",
                        "nullable": false
                    }
                },
                {
                    "name": "total_score",
                    "case_sensitive": false,
                    "columntype": {
                        "type": "BIGINT",
                        "nullable": true
                    }
                }
            ]
        }]),
        "CompilingRust",
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap();
    let (state, _store) = api_state_memory("feldera-program-cte-having-union").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let program_sql = "\
CREATE MATERIALIZED VIEW score_rollup AS \
WITH positives AS (SELECT user_id, score FROM scores WHERE score > 0), \
high_users AS (SELECT user_id, SUM(score) AS total_score FROM positives GROUP BY user_id HAVING SUM(score) > 10) \
SELECT user_id, total_score FROM high_users \
UNION ALL \
SELECT user_id, score AS total_score FROM scores WHERE score = 0";
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_complex_sql",
            "urlPath": "/feldera-http/scores/complex-rollup",
            "input_relations": [input_schema],
            "source_kind": "feldera_program",
            "sql": program_sql,
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["skipped"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "compile_validated");

    let read_back = request_json(
        app,
        Method::GET,
        "/v1/views/score_program_complex_sql",
        None,
    )
    .await;
    assert_eq!(read_back.0, StatusCode::OK, "view body: {}", read_back.1);
    assert_eq!(read_back.1["source_kind"], "feldera_program");
    assert_eq!(
        read_back.1["output_query_endpoints"],
        json!(["/v1/views/score_program_complex_sql/outputs/score_rollup/query"])
    );
    assert_eq!(
        read_back.1["lifecycle"]["deployment_status"],
        "not_deployed"
    );

    let requests = fake_feldera.requests.lock().unwrap();
    let compile_body = requests
        .iter()
        .find(|request| request["kind"] == "compile_put")
        .expect("compile request must be sent");
    let program_code = compile_body["body"]["program_code"].as_str().unwrap();
    assert!(program_code.contains("CREATE TABLE \"scores\""));
    assert!(program_code.contains("CREATE MATERIALIZED VIEW score_rollup AS"));
    assert!(program_code.contains("WITH positives AS"));
    assert!(program_code.contains("HAVING SUM(score) > 10"));
    assert!(program_code.contains("UNION ALL"));
    assert!(!program_code.contains("CREATE MATERIALIZED VIEW \"score_program_complex_sql\" AS"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
}

#[tokio::test]
async fn rest_product_feldera_program_passes_multi_relation_complex_sql_to_pipeline_manager() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        json!([
            {
                "name": "orders",
                "case_sensitive": false,
                "primary_key": ["account_id"],
                "fields": [
                    { "name": "account_id", "case_sensitive": false, "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "amount", "case_sensitive": false, "columntype": { "type": "BIGINT", "nullable": false } },
                    { "name": "delta", "case_sensitive": false, "columntype": { "type": "BIGINT", "nullable": false } }
                ]
            },
            {
                "name": "accounts",
                "case_sensitive": false,
                "primary_key": ["account_id"],
                "fields": [
                    { "name": "account_id", "case_sensitive": false, "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "limit", "case_sensitive": false, "columntype": { "type": "BIGINT", "nullable": false } },
                    { "name": "delta", "case_sensitive": false, "columntype": { "type": "BIGINT", "nullable": false } }
                ]
            }
        ]),
        json!([
            {
                "name": "account_rollup",
                "case_sensitive": false,
                "materialized": true,
                "primary_key": ["account_id"],
                "fields": [
                    { "name": "account_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "total_amount", "columntype": { "type": "BIGINT", "nullable": true } },
                    { "name": "min_amount", "columntype": { "type": "BIGINT", "nullable": true } },
                    { "name": "max_amount", "columntype": { "type": "BIGINT", "nullable": true } },
                    { "name": "avg_amount", "columntype": { "type": "DOUBLE", "nullable": true } },
                    { "name": "account_band", "columntype": { "type": "VARCHAR", "nullable": false } }
                ]
            },
            {
                "name": "active_accounts",
                "case_sensitive": false,
                "materialized": true,
                "primary_key": ["account_id"],
                "fields": [
                    { "name": "account_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "limit", "columntype": { "type": "BIGINT", "nullable": false } }
                ]
            }
        ]),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap();
    let (state, _store) = api_state_memory("feldera-program-multi-relation-complex-sql").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

    for catalog in [
        orders_sum_count_relation_catalog(),
        accounts_sum_count_relation_catalog(),
    ] {
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
    }

    let program_sql = "\
CREATE MATERIALIZED VIEW account_rollup AS \
WITH eligible AS (\
    SELECT o.account_id, o.amount, a.limit \
    FROM orders o JOIN accounts a ON o.account_id = a.account_id \
    WHERE a.limit > 0\
) \
SELECT account_id, \
       SUM(amount) AS total_amount, \
       MIN(amount) AS min_amount, \
       MAX(amount) AS max_amount, \
       AVG(amount) AS avg_amount, \
       CASE WHEN SUM(amount) > 100 THEN 'large' ELSE 'standard' END AS account_band \
FROM eligible \
GROUP BY account_id; \
CREATE MATERIALIZED VIEW active_accounts AS \
SELECT a.account_id, a.limit \
FROM accounts a \
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.account_id = a.account_id)";
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "account_program_complex",
            "source_kind": "feldera_program",
            "input_relation_refs": [
                { "relation_id": "orders", "relation_version": "2026-06-06.v1" },
                { "relation_id": "accounts", "relation_version": "2026-06-06.v1" }
            ],
            "sql": program_sql,
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 0, "run body: {}", run.1);
    assert_eq!(run.1["skipped"], 1, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "compile_validated");

    let detail = request_json(app, Method::GET, "/v1/views/account_program_complex", None).await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(
        detail.1["output_query_endpoints"],
        json!([
            "/v1/views/account_program_complex/outputs/account_rollup/query",
            "/v1/views/account_program_complex/outputs/active_accounts/query"
        ])
    );

    let requests = fake_feldera.requests.lock().unwrap();
    let compile_body = requests
        .iter()
        .find(|request| request["kind"] == "compile_put")
        .expect("compile request must be sent");
    let program_code = compile_body["body"]["program_code"].as_str().unwrap();
    assert!(program_code.contains("CREATE TABLE \"orders\""));
    assert!(program_code.contains("CREATE TABLE \"accounts\""));
    assert!(program_code.contains("CREATE MATERIALIZED VIEW account_rollup AS"));
    assert!(program_code.contains("CREATE MATERIALIZED VIEW active_accounts AS"));
    assert!(program_code.contains("WITH eligible AS"));
    assert!(program_code.contains("JOIN accounts"));
    assert!(program_code.contains("MIN(amount)"));
    assert!(program_code.contains("MAX(amount)"));
    assert!(program_code.contains("AVG(amount)"));
    assert!(program_code.contains("CASE WHEN SUM(amount) > 100"));
    assert!(program_code.contains("WHERE EXISTS"));
    assert!(!program_code.contains("CREATE MATERIALIZED VIEW \"account_program_complex\" AS"));
    assert!(!requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
}

#[tokio::test]
async fn rest_product_feldera_program_auto_detects_create_sql_without_source_kind() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_outputs_and_status(
        json!([
            {
                "name": "by_user",
                "case_sensitive": false,
                "materialized": true,
                "primary_key": ["user_id"],
                "fields": [
                    { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "sum", "columntype": { "type": "BIGINT", "nullable": false } }
                ]
            },
            {
                "name": "by_region",
                "case_sensitive": false,
                "materialized": true,
                "primary_key": ["region"],
                "fields": [
                    { "name": "region", "columntype": { "type": "VARCHAR", "nullable": false } },
                    { "name": "count", "columntype": { "type": "BIGINT", "nullable": false } }
                ]
            }
        ]),
        "CompilingRust",
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap();
    let (state, _store) = api_state_memory("feldera-program-auto-detect-create").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

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

    let catalog = scores_sum_count_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let program_sql = "\
-- Velorix should still treat this as a Feldera program body.
/* Multiple Feldera outputs follow. */
CREATE MATERIALIZED VIEW by_user AS \
SELECT user_id, SUM(score) AS sum FROM scores GROUP BY user_id; \
CREATE MATERIALIZED VIEW by_region AS \
SELECT region, COUNT(*) AS count FROM scores GROUP BY region";
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "score_program_auto_detected",
            "input_relations": [input_schema],
            "output_relation_ids": ["by_user", "by_region"],
            "sql": program_sql,
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["source_kind"], "feldera_program");

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "compile_validated");

    let read_back = request_json(
        app,
        Method::GET,
        "/v1/views/score_program_auto_detected",
        None,
    )
    .await;
    assert_eq!(read_back.0, StatusCode::OK, "view body: {}", read_back.1);
    assert_eq!(read_back.1["source_kind"], "feldera_program");
    assert_eq!(
        read_back.1["output_query_endpoints"],
        json!([
            "/v1/views/score_program_auto_detected/outputs/by_user/query",
            "/v1/views/score_program_auto_detected/outputs/by_region/query"
        ])
    );

    let requests = fake_feldera.requests.lock().unwrap();
    let compile_body = requests
        .iter()
        .find(|request| request["kind"] == "compile_put")
        .expect("compile request must be sent");
    let program_code = compile_body["body"]["program_code"].as_str().unwrap();
    assert!(program_code.contains("CREATE TABLE \"scores\""));
    assert!(program_code.contains("CREATE MATERIALIZED VIEW by_user AS"));
    assert!(program_code.contains("CREATE MATERIALIZED VIEW by_region AS"));
    assert!(!program_code.contains("CREATE MATERIALIZED VIEW \"score_program_auto_detected\" AS"));
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_activates_multi_input_view_in_local_runtime() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager().await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-multi-input-gate").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let orders_catalog = orders_sum_count_relation_catalog();
    let accounts_catalog = accounts_sum_count_relation_catalog();

    for catalog in [orders_catalog, accounts_catalog] {
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
    }

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "urlPath": "/feldera-http/orders/joined-by-account",
            "input_relation_refs": [
                { "relation_id": "orders", "relation_version": "2026-06-06.v1" },
                { "relation_id": "accounts", "relation_version": "2026-06-06.v1" }
            ],
            "sql": "select o.account_id as user_id, sum(o.amount) as sum, count(*) as count, avg(o.amount) as avg_score from orders o join accounts a on o.account_id = a.account_id where a.limit > 0 group by o.account_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["pending_jobs"], 1, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["skipped"], 0, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 0, "run body: {}", run.1);
    assert_eq!(run.1["outcomes"][0]["status"], "activated");

    let read_back = request_json(
        app.clone(),
        Method::GET,
        "/v1/views/feldera_http_scores_by_user",
        None,
    )
    .await;
    assert_eq!(read_back.0, StatusCode::OK, "view body: {}", read_back.1);
    assert_eq!(read_back.1["execution_mode"], "standing_runtime");
    assert_eq!(read_back.1["lifecycle"]["compile_status"], "success");
    assert_eq!(read_back.1["lifecycle"]["deployment_status"], "running");
    assert_eq!(read_back.1["query_enabled"], true);

    let accounts_ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "accounts",
            "relation_version": "2026-06-06.v1",
            "stream_id": "accounts",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "account_id": "a1", "limit": 100, "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(
        accounts_ingest.0,
        StatusCode::CREATED,
        "accounts ingest body: {}",
        accounts_ingest.1
    );

    let orders_ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "orders",
            "relation_version": "2026-06-06.v1",
            "stream_id": "orders",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "account_id": "a1", "amount": 10, "delta": 1 },
                { "account_id": "a1", "amount": 2, "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(
        orders_ingest.0,
        StatusCode::CREATED,
        "orders ingest body: {}",
        orders_ingest.1
    );

    let query = request_json(
        app,
        Method::GET,
        "/v1/api/feldera-http/orders/joined-by-account",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);

    let requests = fake_feldera.requests.lock().unwrap();
    let compile = requests
        .iter()
        .find(|request| request["kind"] == "compile_put")
        .expect("compile request was recorded");
    let program_code = compile["body"]["program_code"].as_str().unwrap();
    assert!(program_code.contains("CREATE TABLE \"orders\""));
    assert!(program_code.contains("CREATE TABLE \"accounts\""));
    assert!(program_code.contains("join accounts"));
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "compile_put"));
    assert!(requests
        .iter()
        .any(|request| request["kind"] == "start_pipeline"));
    let ingress_tables: Vec<_> = requests
        .iter()
        .filter(|request| request["kind"] == "ingress")
        .map(|request| request["table_name"].as_str().unwrap())
        .collect();
    assert_eq!(ingress_tables, vec!["accounts", "orders"]);
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_ingresses_to_quoted_input_relation_name() {
    let relation_name = "Scores/Events \"Raw\"";
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
        json!([{
            "name": relation_name,
            "fields": [
                { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                { "name": "score", "columntype": { "type": "BIGINT", "nullable": false } },
                { "name": "delta", "columntype": { "type": "BIGINT", "nullable": false } }
            ],
            "primary_key": ["user_id"]
        }]),
        json!([{
            "name": "quoted_input_rollup",
            "materialized": true,
            "primary_key": ["user_id"],
            "fields": [
                { "name": "user_id", "columntype": { "type": "VARCHAR", "nullable": false } },
                { "name": "sum", "columntype": { "type": "BIGINT", "nullable": false } }
            ]
        }]),
    )
    .await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-quoted-input").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let catalog = special_scores_relation_catalog(relation_name);

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "quoted_input_program",
            "source_kind": "feldera_program",
            "input_relations": [input_schema],
            "output_relation_ids": ["quoted_input_rollup"],
            "sql": "CREATE MATERIALIZED VIEW quoted_input_rollup AS SELECT user_id, SUM(score) AS sum FROM \"Scores/Events \"\"Raw\"\"\" GROUP BY user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "special_scores",
            "relation_version": "2026-06-10.v1",
            "stream_id": "special-scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "user_id": "u1", "score": 5, "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let requests = fake_feldera.requests.lock().unwrap();
    let compile = requests
        .iter()
        .find(|request| request["kind"] == "compile_put")
        .expect("compile request was recorded");
    let program_code = compile["body"]["program_code"].as_str().unwrap();
    assert!(program_code.contains("CREATE TABLE \"Scores/Events \"\"Raw\"\"\""));
    assert!(program_code.contains("FROM \"Scores/Events \"\"Raw\"\"\""));
    let ingress_tables = requests
        .iter()
        .filter(|request| request["kind"] == "ingress")
        .map(|request| request["table_name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ingress_tables, vec![relation_name]);
}

#[tokio::test]
async fn rest_product_create_view_rejects_mixed_input_relation_selectors() {
    let (state, _store) = api_state_memory("mixed-input-relation-selectors").await;
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
        app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "mixed_input_selector_view",
            "urlPath": "/mixed-input-selector",
            "input_relation_id": "scores",
            "input_relation_version": "2026-05-24.v1",
            "input_relation_refs": [
                { "relation_id": "scores", "relation_version": "2026-05-24.v1" }
            ],
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::BAD_REQUEST, "view body: {}", view.1);
    assert!(view.1["error"]
        .as_str()
        .unwrap()
        .contains("only one input relation selector"));
}

#[tokio::test]
async fn rest_product_create_view_rejects_duplicate_input_relation_refs() {
    let (state, _store) = api_state_memory("duplicate-input-relation-refs").await;
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
        app,
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "duplicate_input_refs_view",
            "urlPath": "/duplicate-input-refs",
            "input_relation_refs": [
                { "relation_id": "scores", "relation_version": "2026-05-24.v1" },
                { "relation_id": "scores", "relation_version": "2026-05-24.v1" }
            ],
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::BAD_REQUEST, "view body: {}", view.1);
    assert!(view.1["error"]
        .as_str()
        .unwrap()
        .contains("duplicate input_relation_refs"));
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_applies_multi_relation_ingest_as_one_epoch() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager().await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, store) = api_state_memory("feldera-pipeline-manager-atomic-epoch").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let orders_catalog = orders_sum_count_relation_catalog();
    let accounts_catalog = accounts_sum_count_relation_catalog();

    for catalog in [orders_catalog, accounts_catalog] {
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
    }

    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "urlPath": "/feldera-http/orders/joined-by-account",
            "input_relation_refs": [
                { "relation_id": "orders", "relation_version": "2026-06-06.v1" },
                { "relation_id": "accounts", "relation_version": "2026-06-06.v1" }
            ],
            "sql": "select o.account_id as user_id, sum(o.amount) as sum, count(*) as count, avg(o.amount) as avg_score from orders o join accounts a on o.account_id = a.account_id where a.limit > 0 group by o.account_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let epoch_body = json!({
        "batches": [
                {
                    "relation_id": "accounts",
                    "relation_version": "2026-06-06.v1",
                    "stream_id": "accounts",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        { "account_id": "a1", "limit": 100, "delta": 1 }
                    ]
                },
                {
                    "relation_id": "orders",
                    "relation_version": "2026-06-06.v1",
                    "stream_id": "orders",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        { "account_id": "a1", "amount": 10, "delta": 1 },
                        { "account_id": "a1", "amount": 2, "delta": 1 }
                    ]
                }
            ]
    });
    let ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest/epoch",
        Some(epoch_body.clone()),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);
    assert_eq!(ingest.1["outcome"], "appended");
    assert_eq!(ingest.1["batches"].as_array().unwrap().len(), 2);
    let epoch_manifest_id = ingest.1["epoch_manifest_id"]
        .as_str()
        .expect("epoch_manifest_id must be returned");
    assert!(epoch_manifest_id.starts_with("sha256:"));
    let epoch_manifest_hash = epoch_manifest_id.strip_prefix("sha256:").unwrap();
    let convergence_key = format!(
        "v1/ingest-epoch-convergence/sha256/{epoch_manifest_hash}/default/feldera_http_scores_by_user/feldera_http_scores_by_user.convergence.json"
    );
    let convergence_bytes = store
        .get(&Path::from(convergence_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let convergence: Value = serde_json::from_slice(&convergence_bytes).unwrap();
    assert_eq!(
        convergence["record_kind"],
        "ingest_epoch_view_convergence_v1"
    );
    assert_eq!(convergence["epoch_manifest_id"], epoch_manifest_id);
    assert_eq!(convergence["tenant_id"], "default");
    assert_eq!(convergence["program_id"], "feldera_http_scores_by_user");
    assert_eq!(convergence["view_id"], "feldera_http_scores_by_user");
    assert_eq!(convergence["logical_epoch"], 1);

    let query = request_json(
        app.clone(),
        Method::GET,
        "/v1/api/feldera-http/orders/joined-by-account",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    assert_eq!(query.1["logical_epoch"], json!(1));

    let duplicate = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest/epoch",
        Some(epoch_body.clone()),
    )
    .await;
    assert_eq!(
        duplicate.0,
        StatusCode::OK,
        "duplicate body: {}",
        duplicate.1
    );
    assert_eq!(duplicate.1["outcome"], "duplicate");

    let checkpoint_key = convergence["checkpoint_key"].as_str().unwrap();
    store.delete(&Path::from(checkpoint_key)).await.unwrap();
    let checkpoint_missing_retry =
        request_json(app, Method::POST, "/v1/ingest/epoch", Some(epoch_body)).await;
    assert_eq!(
        checkpoint_missing_retry.0,
        StatusCode::INTERNAL_SERVER_ERROR,
        "checkpoint missing retry body: {}",
        checkpoint_missing_retry.1
    );
    assert!(
        checkpoint_missing_retry.1["error"]
            .as_str()
            .unwrap()
            .contains("checkpoint"),
        "checkpoint missing retry body: {}",
        checkpoint_missing_retry.1
    );

    let requests = fake_feldera.requests.lock().unwrap();
    let transaction_events: Vec<_> = requests
        .iter()
        .filter_map(|request| request["kind"].as_str())
        .filter(|kind| {
            matches!(
                *kind,
                "start_transaction" | "commit_transaction" | "transaction_stats"
            )
        })
        .collect();
    assert_eq!(
        transaction_events,
        vec![
            "start_transaction",
            "commit_transaction",
            "transaction_stats"
        ]
    );
    let ingress_tables: Vec<_> = requests
        .iter()
        .filter(|request| request["kind"] == "ingress")
        .map(|request| request["table_name"].as_str().unwrap())
        .collect();
    assert_eq!(ingress_tables, vec!["accounts", "orders"]);
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_persists_epoch_runtime_failure_marker() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_failed_ingress("orders").await;
    let backend = Arc::new(
        FelderaPipelineManagerCompilerBackend::new(
            fake_feldera.endpoint.clone(),
            Some("feldera-secret".to_string()),
            Duration::from_millis(1),
            Duration::from_secs(2),
            "dev",
            1,
        )
        .unwrap()
        .with_runtime_deployment_mode(FelderaPipelineManagerRuntimeDeploymentMode::ExternalManaged),
    );
    let (state, store) = api_state_memory("feldera-pipeline-manager-epoch-failure-marker").await;
    let initial_app = app(state.with_feldera_pipeline_manager_backend(Arc::clone(&backend)));
    let orders_catalog = orders_sum_count_relation_catalog();
    let accounts_catalog = accounts_sum_count_relation_catalog();

    for catalog in [orders_catalog, accounts_catalog] {
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
    }

    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "urlPath": "/feldera-http/orders/joined-by-account-failure",
            "input_relation_refs": [
                { "relation_id": "orders", "relation_version": "2026-06-06.v1" },
                { "relation_id": "accounts", "relation_version": "2026-06-06.v1" }
            ],
            "sql": "select o.account_id as user_id, sum(o.amount) as sum, count(*) as count, avg(o.amount) as avg_score from orders o join accounts a on o.account_id = a.account_id where a.limit > 0 group by o.account_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);

    let run = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let epoch_body = json!({
        "batches": [
            {
                "relation_id": "accounts",
                "relation_version": "2026-06-06.v1",
                "stream_id": "accounts",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    { "account_id": "a1", "limit": 100, "delta": 1 }
                ]
            },
            {
                "relation_id": "orders",
                "relation_version": "2026-06-06.v1",
                "stream_id": "orders",
                "partition_id": 0,
                "start_offset_inclusive": 0,
                "rows": [
                    { "account_id": "a1", "amount": 10, "delta": 1 }
                ]
            }
        ]
    });
    let failed = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/ingest/epoch",
        Some(epoch_body.clone()),
    )
    .await;
    assert_eq!(
        failed.0,
        StatusCode::BAD_REQUEST,
        "failed body: {}",
        failed.1
    );
    assert!(
        failed.1["error"]
            .as_str()
            .unwrap()
            .contains("Feldera ingress"),
        "failed body: {}",
        failed.1
    );
    let failure_markers = store
        .list(Some(&Path::from("v1/ingest-epoch-runtime-failures")))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(failure_markers.len(), 1);
    assert!(failure_markers[0]
        .location
        .as_ref()
        .ends_with(".failure.json"));
    let marker_bytes = store
        .get(&failure_markers[0].location)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let marker: Value = serde_json::from_slice(&marker_bytes).unwrap();
    let epoch_manifest_id = marker["epoch_manifest_id"].as_str().unwrap().to_string();

    let restarted_state = api_state_from_store(
        "feldera-pipeline-manager-epoch-failure-marker",
        Arc::clone(&store),
    )
    .await
    .with_feldera_pipeline_manager_backend(Arc::clone(&backend));
    let restarted_app = app(restarted_state);
    let retry = request_json(
        restarted_app.clone(),
        Method::POST,
        "/v1/ingest/epoch",
        Some(epoch_body.clone()),
    )
    .await;
    assert_eq!(
        retry.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "retry body: {}",
        retry.1
    );
    assert!(
        retry.1["error"]
            .as_str()
            .unwrap()
            .contains("durable runtime failure marker"),
        "retry body: {}",
        retry.1
    );

    let repair = request_json(
        restarted_app.clone(),
        Method::POST,
        "/v1/standing-runtime/ingest-epoch-failures/repair",
        Some(json!({
            "epoch_manifest_id": epoch_manifest_id,
            "tenant_id": "default",
            "program_id": "feldera_http_scores_by_user",
            "view_id": "feldera_http_scores_by_user",
            "confirm_external_runtime_rebuilt": true,
            "repair_reason": "test rebuilt external Feldera pipeline before replay"
        })),
    )
    .await;
    assert_eq!(repair.0, StatusCode::OK, "repair body: {}", repair.1);
    assert_eq!(repair.1["outcome"], "repaired");
    let failure_markers_after_repair = store
        .list(Some(&Path::from("v1/ingest-epoch-runtime-failures")))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(failure_markers_after_repair.len(), 0);

    let retry_after_repair = request_json(
        restarted_app,
        Method::POST,
        "/v1/ingest/epoch",
        Some(epoch_body),
    )
    .await;
    assert_eq!(
        retry_after_repair.0,
        StatusCode::OK,
        "retry after repair body: {}",
        retry_after_repair.1
    );
    assert_eq!(retry_after_repair.1["outcome"], "duplicate");

    let requests = fake_feldera.requests.lock().unwrap();
    let ingress_tables: Vec<_> = requests
        .iter()
        .filter(|request| request["kind"] == "ingress")
        .map(|request| request["table_name"].as_str().unwrap())
        .collect();
    assert_eq!(
        ingress_tables,
        vec!["accounts", "orders", "accounts", "orders"]
    );
}

#[tokio::test]
async fn rest_product_epoch_writes_durable_manifest_before_runtime_apply() {
    let (state, store) = api_state_memory("epoch-durable-manifest").await;
    let app = app(state);
    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": scores_sum_count_relation_catalog() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let ingest = request_json(
        app,
        Method::POST,
        "/v1/ingest/epoch",
        Some(json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        { "user_id": "u1", "score": 5, "delta": 1 },
                        { "user_id": "u2", "score": 7, "delta": 1 }
                    ]
                }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);
    let manifest_id = ingest.1["epoch_manifest_id"].as_str().unwrap();
    let manifest_hash = manifest_id.strip_prefix("sha256:").unwrap();
    let manifest_key = format!("v1/ingest-epochs/sha256/{manifest_hash}.epoch.json");
    let manifest_bytes = store
        .get(&Path::from(manifest_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let manifest: Value = serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(manifest["record_kind"], "ingest_epoch_manifest_v1");
    assert_eq!(manifest["epoch_manifest_id"], manifest_id);
    assert_eq!(manifest["batches"].as_array().unwrap().len(), 1);
    assert_eq!(manifest["batches"][0]["relation_id"], "scores");
    assert_eq!(manifest["batches"][0]["stream_id"], "scores");
    assert_eq!(manifest["batches"][0]["start_offset_inclusive"], 0);
    assert_eq!(manifest["batches"][0]["end_offset_exclusive"], 2);
    assert!(manifest["batches"][0]["payload_digest"]
        .as_str()
        .unwrap()
        .starts_with("sha256:"));
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_runtime_rejects_queries_after_ingress_failure() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager_with_failed_ingress("scores").await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-ingress-failure").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "urlPath": "/feldera-http/scores/by-user",
            "input_relations": [input_schema],
            "sql": "select user_id, sum(score) as sum, count(*) as count, avg(score) as avg_score from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let failed_ingest = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "user_id": "u1", "score": 5, "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(
        failed_ingest.0,
        StatusCode::BAD_REQUEST,
        "ingest body: {}",
        failed_ingest.1
    );
    assert!(
        failed_ingest.1["error"]
            .as_str()
            .unwrap()
            .contains("Feldera ingress"),
        "ingest body: {}",
        failed_ingest.1
    );

    let query = request_json(
        app,
        Method::GET,
        "/v1/api/feldera-http/scores/by-user",
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::BAD_REQUEST, "query body: {}", query.1);
    assert!(
        query.1["error"]
            .as_str()
            .unwrap()
            .contains("poisoned after a failed Feldera ingress"),
        "query body: {}",
        query.1
    );
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_runtime_restores_from_checkpoint() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager().await;
    let backend = Arc::new(
        FelderaPipelineManagerCompilerBackend::new(
            fake_feldera.endpoint.clone(),
            Some("feldera-secret".to_string()),
            Duration::from_millis(1),
            Duration::from_secs(2),
            "dev",
            1,
        )
        .unwrap()
        .with_volatile_runtime_deployment(),
    );
    let (state, store) = api_state_memory("feldera-pipeline-manager-runtime-restore").await;
    let initial_app = app(state.with_feldera_pipeline_manager_backend(Arc::clone(&backend)));
    let catalog = scores_sum_count_relation_catalog();

    let relation = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let view = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "urlPath": "/feldera-http/restore-scores/by-user",
            "input_relations": [catalog_input_relation_schema(&catalog).unwrap()],
            "sql": "select user_id, sum(score) as sum, count(*) as count, avg(score) as avg_score from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "body: {}", view.1);
    let run = request_json(
        initial_app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let ingest = request_json(
        initial_app,
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "user_id": "u1", "score": 5, "delta": 1 },
                { "user_id": "u1", "score": 7, "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);
    let ingress_count_after_checkpoint = fake_feldera
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request["kind"] == "ingress")
        .count();
    assert_eq!(ingress_count_after_checkpoint, 1);

    let restored_state = api_state_from_store(
        "feldera-pipeline-manager-runtime-restore",
        Arc::clone(&store),
    )
    .await
    .with_feldera_pipeline_manager_backend(backend);
    let restored = restored_state
        .restore_standing_program_runtimes_from_active_views()
        .await
        .unwrap();
    assert_eq!(restored, 1);
    let restored_app = app(restored_state);
    let restored_query = request_json(
        restored_app,
        Method::GET,
        "/v1/views/feldera_http_scores_by_user/query",
        None,
    )
    .await;
    assert_eq!(
        restored_query.0,
        StatusCode::OK,
        "restored query body: {}",
        restored_query.1
    );
    assert_eq!(restored_query.1["logical_epoch"], 2);
    assert_eq!(
        fake_feldera
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["kind"] == "ingress")
            .count(),
        ingress_count_after_checkpoint,
        "restore should not replay already checkpointed ingest"
    );
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_runtime_rejects_delete_for_insert_only_relation() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager().await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-insert-only-delete").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));
    let mut catalog = scores_sum_count_relation_catalog();
    catalog.relation_schema.allowed_operations = vec![RelationOperationV1::Insert];
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.feldera_relation.schema_fingerprint = catalog.schema_fingerprint.clone();

    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "urlPath": "/feldera-http/insert-only-scores",
            "input_relations": [input_schema],
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let ingest = request_json(
        app,
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "user_id": "u1", "score": 5, "delta": -1 }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::BAD_REQUEST, "body: {}", ingest.1);
    assert!(
        ingest.1["error"]
            .as_str()
            .unwrap()
            .contains("insert-only relation"),
        "body: {}",
        ingest.1
    );
    let requests = fake_feldera.requests.lock().unwrap();
    assert!(!requests.iter().any(|request| request["kind"] == "ingress"));
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_runtime_accepts_update_capable_relation() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager().await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-update-capable").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

    let mut catalog = scores_sum_count_relation_catalog();
    catalog.relation_schema.allowed_operations =
        vec![RelationOperationV1::Insert, RelationOperationV1::Update];
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.feldera_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "urlPath": "/feldera-http/update-capable-scores",
            "input_relations": [input_schema],
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let ingest = request_json(
        app,
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "user_id": "u1", "score": 5, "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "body: {}", ingest.1);
    let requests = fake_feldera.requests.lock().unwrap();
    let ingress = requests
        .iter()
        .find(|request| request["kind"] == "ingress")
        .expect("update-capable relation should still ingest insert events");
    assert_eq!(ingress["table_name"], "scores");
    assert_eq!(
        ingress["body"],
        json!([{ "insert": { "user_id": "u1", "score": 5 } }])
    );
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_runtime_maps_update_envelope_to_delete_insert() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager().await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-update-envelope").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

    let mut catalog = scores_sum_count_relation_catalog();
    catalog.relation_schema.allowed_operations =
        vec![RelationOperationV1::Insert, RelationOperationV1::Update];
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.feldera_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "urlPath": "/feldera-http/update-envelope-scores",
            "input_relations": [input_schema],
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let ingest = request_json(
        app,
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {
                    "operation": "update",
                    "before": { "user_id": "u1", "score": 5 },
                    "after": { "user_id": "u1", "score": 7 }
                }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "body: {}", ingest.1);
    assert_eq!(ingest.1["descriptor"]["end_offset_exclusive"], 2);

    let requests = fake_feldera.requests.lock().unwrap();
    let ingress = requests
        .iter()
        .find(|request| request["kind"] == "ingress")
        .expect("update envelope should reach Feldera ingress");
    assert_eq!(
        ingress["body"],
        json!([
            { "delete": { "user_id": "u1", "score": 5 } },
            { "insert": { "user_id": "u1", "score": 7 } }
        ])
    );
}

#[tokio::test]
async fn rest_product_feldera_pipeline_manager_runtime_maps_upsert_envelope_to_delete_insert() {
    let fake_feldera = spawn_fake_feldera_pipeline_manager().await;
    let backend = FelderaPipelineManagerCompilerBackend::new(
        fake_feldera.endpoint.clone(),
        Some("feldera-secret".to_string()),
        Duration::from_millis(1),
        Duration::from_secs(2),
        "dev",
        1,
    )
    .unwrap()
    .with_volatile_runtime_deployment();
    let (state, _store) = api_state_memory("feldera-pipeline-manager-upsert-envelope").await;
    let app = app(state.with_feldera_pipeline_manager_backend(Arc::new(backend)));

    let mut catalog = scores_sum_count_relation_catalog();
    catalog.relation_schema.allowed_operations =
        vec![RelationOperationV1::Insert, RelationOperationV1::Upsert];
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.feldera_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": catalog.clone() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED, "body: {}", relation.1);
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let view = request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": "feldera_http_scores_by_user",
            "urlPath": "/feldera-http/upsert-envelope-scores",
            "input_relations": [input_schema],
            "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "body: {}", view.1);

    let run = request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);

    let ingest = request_json(
        app,
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "2026-05-24.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {
                    "operation": "upsert",
                    "before": { "user_id": "u1", "score": 5 },
                    "row": { "user_id": "u1", "score": 11 }
                }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "body: {}", ingest.1);
    assert_eq!(ingest.1["descriptor"]["end_offset_exclusive"], 2);

    let requests = fake_feldera.requests.lock().unwrap();
    let ingress = requests
        .iter()
        .find(|request| request["kind"] == "ingress")
        .expect("upsert envelope should reach Feldera ingress");
    assert_eq!(
        ingress["body"],
        json!([
            { "delete": { "user_id": "u1", "score": 5 } },
            { "insert": { "user_id": "u1", "score": 11 } }
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
        .contains("feldera compiler backend is not configured"));
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
    store
        .delete(&Path::from(compile_request_job_object_path(
            "positive_scores_by_user",
            &view.1,
        )))
        .await
        .unwrap();

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
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
                "stream_id": "scores-a",
                "partition_id": 0,
                "end_offset_exclusive": 2
            },
            {
                "relation_id": "scores",
                "relation_version": "2026-05-24.v1",
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
async fn rest_product_epoch_retries_standing_runtime_apply_after_duplicate_ingest_append() {
    let (state, _temp) = api_state("artifact-runtime-epoch-duplicate-apply").await;
    let catalog = scores_sum_count_relation_catalog();
    let artifact = artifact_for_scores_view(
        &catalog,
        "epoch_duplicate_apply_scores_by_user",
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
            "view_id": "epoch_duplicate_apply_scores_by_user",
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

    let ingest_batch = IngestRowsRequest {
        relation_id: "scores".to_string(),
        relation_version: "2026-05-24.v1".to_string(),
        stream_id: "scores".to_string(),
        partition_id: 0,
        start_offset_inclusive: 0,
        rows: vec![
            json!({ "user_id": "u1", "score": 5, "delta": 1 }),
            json!({ "user_id": "u1", "score": 7, "delta": 1 }),
        ],
    };
    let ingest_body = json!({ "batches": [ingest_batch] });
    let first = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest/epoch",
        Some(ingest_body.clone()),
    )
    .await;
    assert_eq!(
        first.0,
        StatusCode::BAD_REQUEST,
        "first ingest body: {}",
        first.1
    );

    let retry = request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest/epoch",
        Some(ingest_body),
    )
    .await;
    assert_eq!(retry.0, StatusCode::OK, "retry body: {}", retry.1);
    assert_eq!(retry.1["outcome"], "duplicate");

    let query = request_json(
        app,
        Method::GET,
        "/v1/views/epoch_duplicate_apply_scores_by_user/query",
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
async fn rest_product_epoch_rejects_duplicate_source_range_when_batch_repeated() {
    let (state, _temp) = api_state("epoch-duplicate-source-range").await;
    let app = app(state);
    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": scores_sum_count_relation_catalog() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let batch = json!({
        "relation_id": "scores",
        "relation_version": "2026-05-24.v1",
        "stream_id": "scores",
        "partition_id": 0,
        "start_offset_inclusive": 0,
        "rows": [
            { "user_id": "u1", "score": 5, "delta": 1 }
        ]
    });
    let ingest = request_json(
        app,
        Method::POST,
        "/v1/ingest/epoch",
        Some(json!({ "batches": [batch.clone(), batch] })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::BAD_REQUEST, "body: {}", ingest.1);
    assert!(
        ingest.1["error"]
            .as_str()
            .unwrap()
            .contains("duplicate ingest epoch range"),
        "body: {}",
        ingest.1
    );
}

#[tokio::test]
async fn rest_product_epoch_rejects_overlapping_source_ranges() {
    let (state, _temp) = api_state("epoch-overlapping-source-range").await;
    let app = app(state);
    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": scores_sum_count_relation_catalog() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let ingest = request_json(
        app,
        Method::POST,
        "/v1/ingest/epoch",
        Some(json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": [
                        { "user_id": "u1", "score": 5, "delta": 1 },
                        { "user_id": "u2", "score": 7, "delta": 1 }
                    ]
                },
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores",
                    "partition_id": 0,
                    "start_offset_inclusive": 1,
                    "rows": [
                        { "user_id": "u3", "score": 11, "delta": 1 }
                    ]
                }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::BAD_REQUEST, "body: {}", ingest.1);
    assert!(
        ingest.1["error"]
            .as_str()
            .unwrap()
            .contains("overlapping ingest epoch ranges"),
        "body: {}",
        ingest.1
    );
}

#[tokio::test]
async fn rest_product_epoch_rejects_zero_row_batches() {
    let (state, _temp) = api_state("epoch-zero-row-batch").await;
    let app = app(state);
    let relation = request_json(
        app.clone(),
        Method::POST,
        "/v1/relations",
        Some(json!({ "catalog": scores_sum_count_relation_catalog() })),
    )
    .await;
    assert_eq!(relation.0, StatusCode::CREATED);

    let ingest = request_json(
        app,
        Method::POST,
        "/v1/ingest/epoch",
        Some(json!({
            "batches": [
                {
                    "relation_id": "scores",
                    "relation_version": "2026-05-24.v1",
                    "stream_id": "scores",
                    "partition_id": 0,
                    "start_offset_inclusive": 0,
                    "rows": []
                }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::BAD_REQUEST, "body: {}", ingest.1);
    assert!(
        ingest.1["error"]
            .as_str()
            .unwrap()
            .contains("ingest epoch batches must contain at least one row"),
        "body: {}",
        ingest.1
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

fn special_scores_relation_catalog(relation_name: &str) -> VelorixRelationCatalogV1 {
    let mut catalog = scores_sum_count_relation_catalog();
    catalog.relation_schema.relation_id = "special_scores".to_string();
    catalog.relation_schema.relation_name = relation_name.to_string();
    catalog.relation_schema.relation_version = "2026-06-10.v1".to_string();
    catalog.schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema)
        .expect("special scores relation schema must fingerprint");
    catalog.datafusion_registration.name = "special_scores".to_string();
    catalog.feldera_relation.relation_id = "special_scores".to_string();
    catalog.feldera_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    catalog
}

fn generic_feldera_activity_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "activity_events".to_string(),
        relation_name: "activity_events".to_string(),
        relation_version: "2026-06-11.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "event_id".to_string(),
                name: "event_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "user_id".to_string(),
                name: "user_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Metadata,
            },
            RelationColumnV1 {
                column_id: "score".to_string(),
                name: "score".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Metadata,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 3,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["event_id".to_string()],
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
            name: "activity_events".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "activity_events".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_FELDERA_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
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

fn accounts_sum_count_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "accounts".to_string(),
        relation_name: "accounts".to_string(),
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
                column_id: "limit".to_string(),
                name: "limit".to_string(),
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
            name: "accounts".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "accounts".to_string(),
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
    let spec = scores_view_spec(catalog, view_id, sql);
    artifact_for_scores_spec(&spec, view_id)
}

fn artifact_for_scores_spec(
    spec: &StandingViewSpec,
    view_id: &str,
) -> FelderaCompileArtifactMetadata {
    FelderaCompileArtifactMetadata {
        metadata_version: FELDERA_ARTIFACT_METADATA_VERSION,
        view_id: view_id.to_string(),
        spec_hash: feldera_spec_hash(&spec).unwrap(),
        compile_request_hash: Some(
            feldera_compile_request_hash(
                &FelderaCompileRequestV1::infer_output_from_standing_view_spec(&spec),
            )
            .unwrap(),
        ),
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
        input_schemas: spec.input_relations.clone(),
        output_schemas: spec.output_relations.clone(),
        state_codec: SUPPORTED_STATE_CODEC.to_string(),
        state_schema_version: 1,
        epoch_policy: SUPPORTED_EPOCH_POLICY.to_string(),
    }
}

fn scores_view_spec(
    catalog: &VelorixRelationCatalogV1,
    view_id: &str,
    sql: &str,
) -> StandingViewSpec {
    let input = catalog_input_relation_schema(catalog).unwrap();
    let output = scores_view_output_schema(view_id, catalog.schema_fingerprint.as_str());
    StandingViewSpec {
        view_id: view_id.to_string(),
        sql: sql.to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input],
        output_relations: vec![output],
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    }
}

fn compile_request_hash_for_job_json(compiler_request: &Value) -> String {
    let output_contract = compiler_request
        .get("output_contract")
        .cloned()
        .unwrap_or_else(|| json!({ "kind": "infer" }));
    let request = FelderaCompileRequestV1 {
        view_id: compiler_request["view_id"].as_str().unwrap().to_string(),
        sql: compiler_request["sql"].as_str().unwrap().to_string(),
        dialect: serde_json::from_value(compiler_request["dialect"].clone()).unwrap(),
        source_kind: serde_json::from_value(compiler_request["source_kind"].clone()).unwrap(),
        rust_extension: serde_json::from_value(
            compiler_request
                .get("rust_extension")
                .cloned()
                .unwrap_or_else(|| json!({})),
        )
        .unwrap(),
        input_relations: serde_json::from_value(compiler_request["input_relations"].clone())
            .unwrap(),
        output_contract: serde_json::from_value(output_contract).unwrap(),
        shape: serde_json::from_value(compiler_request["shape"].clone()).unwrap(),
    };
    feldera_compile_request_hash(&request).unwrap()
}

fn refresh_compile_request_hash_for_job_json(job_record: &mut Value) {
    let hash = compile_request_hash_for_job_json(&job_record["compiler_request"]);
    job_record["compiler_request"]["compile_request_hash"] = json!(hash);
}

fn compile_request_hash_from_view_response(view_id: &str, view: &Value) -> String {
    view["compile_job_id"]
        .as_str()
        .and_then(|job_id| job_id.strip_prefix(&format!("{view_id}:")))
        .expect("view response must include compile request job id")
        .to_string()
}

fn test_feldera_pipeline_name_for_parts(view_id: &str, compile_request_hash: &str) -> String {
    let hash_tail = compile_request_hash
        .rsplit_once(':')
        .map(|(_, tail)| tail)
        .unwrap_or(compile_request_hash)
        .chars()
        .take(16)
        .collect::<String>();
    let view = view_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    let view = if view.is_empty() {
        "view".to_string()
    } else {
        view
    };
    let max_view_chars = 63usize.saturating_sub("velorix--".len() + hash_tail.len());
    let view = view.chars().take(max_view_chars).collect::<String>();
    format!("velorix-{view}-{hash_tail}")
}

fn compile_request_job_object_path(view_id: &str, view: &Value) -> String {
    let compile_request_hash = compile_request_hash_from_view_response(view_id, view);
    let hash_segment = compile_request_hash
        .strip_prefix("velorix-feldera-compile-request-sha256-v1:")
        .expect("compile request hash must use v1 prefix");
    format!("v1/view-compile-deploy-jobs/{view_id}/compile-request-sha256/{hash_segment}.job.json")
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

struct TestFelderaCompilerBackend;

struct TestProductRuntimeFelderaCompilerBackend;

struct TestSchemaOnlyFelderaCompilerBackend;

const TEST_PRODUCT_RUNTIME_CRATE_NAME: &str = "velorix_feldera_package_scores_runtime";
const TEST_PRODUCT_RUNTIME_CRATE_VERSION: &str = "0.299.0-test";

#[derive(Clone)]
struct FakeFelderaPipelineManager {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
}

#[derive(Clone, Default)]
struct FakeFelderaPipelineManagerState {
    requests: Arc<Mutex<Vec<Value>>>,
    fail_next_ingress_tables: Arc<Mutex<BTreeSet<String>>>,
    transaction_status: Arc<Mutex<String>>,
    program_inputs: Arc<Mutex<Option<Value>>>,
    program_outputs: Arc<Mutex<Option<Value>>>,
    program_status: Arc<Mutex<String>>,
    program_error: Arc<Mutex<Option<Value>>>,
}

async fn spawn_fake_feldera_pipeline_manager() -> FakeFelderaPipelineManager {
    let state = FakeFelderaPipelineManagerState::default();
    spawn_fake_feldera_pipeline_manager_with_state(state).await
}

async fn spawn_fake_feldera_pipeline_manager_with_program_outputs(
    outputs: Value,
) -> FakeFelderaPipelineManager {
    spawn_fake_feldera_pipeline_manager_with_program_outputs_and_status(outputs, "Success").await
}

async fn spawn_fake_feldera_pipeline_manager_with_program_status(
    status: &str,
) -> FakeFelderaPipelineManager {
    spawn_fake_feldera_pipeline_manager_with_program_outputs_and_status(
        default_fake_feldera_program_outputs(),
        status,
    )
    .await
}

async fn spawn_fake_feldera_pipeline_manager_with_program_outputs_and_status(
    outputs: Value,
    status: &str,
) -> FakeFelderaPipelineManager {
    let state = FakeFelderaPipelineManagerState::default();
    *state.program_outputs.lock().unwrap() = Some(outputs);
    *state.program_status.lock().unwrap() = status.to_string();
    spawn_fake_feldera_pipeline_manager_with_state(state).await
}

async fn spawn_fake_feldera_pipeline_manager_with_program_inputs_and_outputs(
    inputs: Value,
    outputs: Value,
) -> FakeFelderaPipelineManager {
    let state = FakeFelderaPipelineManagerState::default();
    *state.program_inputs.lock().unwrap() = Some(inputs);
    *state.program_outputs.lock().unwrap() = Some(outputs);
    *state.program_status.lock().unwrap() = "Success".to_string();
    spawn_fake_feldera_pipeline_manager_with_state(state).await
}

async fn spawn_fake_feldera_pipeline_manager_with_program_error(
    status: &str,
    error: Value,
) -> FakeFelderaPipelineManager {
    let state = FakeFelderaPipelineManagerState::default();
    *state.program_outputs.lock().unwrap() = Some(default_fake_feldera_program_outputs());
    *state.program_status.lock().unwrap() = status.to_string();
    *state.program_error.lock().unwrap() = Some(error);
    spawn_fake_feldera_pipeline_manager_with_state(state).await
}

async fn spawn_fake_feldera_pipeline_manager_with_failed_ingress(
    table_name: &str,
) -> FakeFelderaPipelineManager {
    let state = FakeFelderaPipelineManagerState::default();
    state
        .fail_next_ingress_tables
        .lock()
        .unwrap()
        .insert(table_name.to_string());
    spawn_fake_feldera_pipeline_manager_with_state(state).await
}

async fn spawn_fake_feldera_pipeline_manager_with_state(
    state: FakeFelderaPipelineManagerState,
) -> FakeFelderaPipelineManager {
    let requests = Arc::clone(&state.requests);
    let app = Router::new()
        .route(
            "/v0/pipelines/{pipeline_name}",
            put(fake_feldera_pipeline_put).get(fake_feldera_pipeline_get),
        )
        .route(
            "/v0/pipelines/{pipeline_name}/start",
            post(fake_feldera_pipeline_start),
        )
        .route(
            "/v0/pipelines/{pipeline_name}/start_transaction",
            post(fake_feldera_pipeline_start_transaction),
        )
        .route(
            "/v0/pipelines/{pipeline_name}/commit_transaction",
            post(fake_feldera_pipeline_commit_transaction),
        )
        .route(
            "/v0/pipelines/{pipeline_name}/stats",
            get(fake_feldera_pipeline_stats),
        )
        .route(
            "/v0/pipelines/{pipeline_name}/ingress/{table_name}",
            post(fake_feldera_pipeline_ingress),
        )
        .route(
            "/v0/pipelines/{pipeline_name}/query",
            get(fake_feldera_pipeline_query),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    FakeFelderaPipelineManager {
        endpoint: format!("http://{addr}"),
        requests,
    }
}

async fn fake_feldera_pipeline_put(
    AxumPath(pipeline_name): AxumPath<String>,
    AxumState(state): AxumState<FakeFelderaPipelineManagerState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    state.requests.lock().unwrap().push(json!({
        "kind": "compile_put",
        "pipeline_name": pipeline_name.clone(),
        "headers": {
            "authorization": authorization
        },
        "body": body
    }));
    (
        StatusCode::CREATED,
        Json(json!({
            "name": pipeline_name,
            "program_status": "Pending",
            "program_version": 7,
            "program_info": null
        })),
    )
}

async fn fake_feldera_pipeline_get(
    AxumPath(pipeline_name): AxumPath<String>,
    AxumState(state): AxumState<FakeFelderaPipelineManagerState>,
) -> (StatusCode, Json<Value>) {
    state.requests.lock().unwrap().push(json!({
        "kind": "pipeline_get",
        "pipeline_name": pipeline_name.clone()
    }));
    let inputs = state
        .program_inputs
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(default_fake_feldera_program_inputs);
    let outputs = state
        .program_outputs
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(default_fake_feldera_program_outputs);
    let program_status = {
        let status = state.program_status.lock().unwrap();
        if status.is_empty() {
            "Success".to_string()
        } else {
            status.clone()
        }
    };
    let program_error = state.program_error.lock().unwrap().clone();
    (
        StatusCode::OK,
        Json(json!({
            "name": pipeline_name,
            "program_status": program_status,
            "program_version": 7,
            "deployment_status": "Running",
            "deployment_resources_status": "Provisioned",
            "program_error": program_error,
            "program_info": {
                "schema": {
                    "inputs": inputs,
                    "outputs": outputs
                }
            }
        })),
    )
}

fn default_fake_feldera_program_inputs() -> Value {
    json!([{
        "name": "scores",
        "case_sensitive": false,
        "materialized": false,
        "primary_key": ["user_id"],
        "fields": [
            {
                "name": "user_id",
                "case_sensitive": false,
                "columntype": {
                    "type": "VARCHAR",
                    "nullable": false
                }
            },
            {
                "name": "score",
                "case_sensitive": false,
                "columntype": {
                    "type": "BIGINT",
                    "nullable": false
                }
            },
            {
                "name": "delta",
                "case_sensitive": false,
                "columntype": {
                    "type": "BIGINT",
                    "nullable": false
                }
            }
        ]
    }])
}

fn default_fake_feldera_program_outputs() -> Value {
    json!([{
        "name": "feldera_http_scores_by_user",
        "case_sensitive": false,
        "materialized": true,
        "primary_key": ["user_id"],
        "fields": [
            {
                "name": "user_id",
                "case_sensitive": false,
                "columntype": {
                    "type": "VARCHAR",
                    "nullable": false
                }
            },
            {
                "name": "sum",
                "case_sensitive": false,
                "columntype": {
                    "type": "BIGINT",
                    "nullable": false
                }
            },
            {
                "name": "count",
                "case_sensitive": false,
                "columntype": {
                    "type": "BIGINT",
                    "nullable": false
                }
            },
            {
                "name": "avg_score",
                "case_sensitive": false,
                "columntype": {
                    "type": "DOUBLE",
                    "nullable": true
                }
            }
        ]
    }])
}

async fn fake_feldera_pipeline_start(
    AxumPath(pipeline_name): AxumPath<String>,
    AxumState(state): AxumState<FakeFelderaPipelineManagerState>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    state.requests.lock().unwrap().push(json!({
        "kind": "start_pipeline",
        "pipeline_name": pipeline_name,
        "headers": {
            "authorization": authorization
        }
    }));
    (StatusCode::OK, Json(json!({ "status": "started" })))
}

async fn fake_feldera_pipeline_start_transaction(
    AxumPath(pipeline_name): AxumPath<String>,
    AxumState(state): AxumState<FakeFelderaPipelineManagerState>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    state.requests.lock().unwrap().push(json!({
        "kind": "start_transaction",
        "pipeline_name": pipeline_name,
        "headers": {
            "authorization": authorization
        }
    }));
    *state.transaction_status.lock().unwrap() = "TransactionInProgress".to_string();
    (
        StatusCode::OK,
        Json(json!({
            "transaction_id": 1
        })),
    )
}

async fn fake_feldera_pipeline_commit_transaction(
    AxumPath(pipeline_name): AxumPath<String>,
    AxumState(state): AxumState<FakeFelderaPipelineManagerState>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    state.requests.lock().unwrap().push(json!({
        "kind": "commit_transaction",
        "pipeline_name": pipeline_name,
        "headers": {
            "authorization": authorization
        }
    }));
    *state.transaction_status.lock().unwrap() = "NoTransaction".to_string();
    (StatusCode::OK, Json(json!("Transaction commit initiated")))
}

async fn fake_feldera_pipeline_stats(
    AxumPath(pipeline_name): AxumPath<String>,
    AxumState(state): AxumState<FakeFelderaPipelineManagerState>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let status = {
        let status = state.transaction_status.lock().unwrap();
        if status.is_empty() {
            "NoTransaction".to_string()
        } else {
            status.clone()
        }
    };
    state.requests.lock().unwrap().push(json!({
        "kind": "transaction_stats",
        "pipeline_name": pipeline_name,
        "headers": {
            "authorization": authorization
        },
        "transaction_status": status
    }));
    (
        StatusCode::OK,
        Json(json!({
            "global_metrics": {
                "transaction_status": status,
                "transaction_id": 1
            }
        })),
    )
}

async fn fake_feldera_pipeline_ingress(
    AxumPath((pipeline_name, table_name)): AxumPath<(String, String)>,
    AxumState(state): AxumState<FakeFelderaPipelineManagerState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    state.requests.lock().unwrap().push(json!({
        "kind": "ingress",
        "pipeline_name": pipeline_name,
        "table_name": table_name,
        "headers": {
            "authorization": authorization
        },
        "body": body
    }));
    if state
        .fail_next_ingress_tables
        .lock()
        .unwrap()
        .remove(&table_name)
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "forced ingress failure" })),
        );
    }
    (StatusCode::OK, Json(json!({ "status": "accepted" })))
}

async fn fake_feldera_pipeline_query(
    AxumPath(pipeline_name): AxumPath<String>,
    AxumState(state): AxumState<FakeFelderaPipelineManagerState>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    state.requests.lock().unwrap().push(json!({
        "kind": "query",
        "pipeline_name": pipeline_name,
        "headers": {
            "authorization": authorization
        },
        "params": params
    }));
    let sql = params.get("sql").map(String::as_str);
    let rows = match sql {
        Some(sql) if sql.contains("insert_field_output") => {
            json!([{ "insert": "literal" }])
        }
        Some(sql) if sql.contains("delete_field_output") => {
            json!([{ "delete": "literal" }])
        }
        Some("SELECT 'literal' AS insert FROM feldera_http_scores_by_user") => {
            json!([{ "insert": "literal" }])
        }
        Some("SELECT 'literal' AS delete FROM feldera_http_scores_by_user") => {
            json!([{ "delete": "literal" }])
        }
        Some(sql) if sql.contains("feldera_http_complex_profile") => {
            json!([{
                "insert": {
                    "user_id": "u1",
                    "score_window": [8, null, 13],
                    "profile": {
                        "name": "Ada",
                        "tier": 2
                    },
                    "maybe_count": null
                }
            }])
        }
        Some(sql) if sql.contains("\"BY_USER\"") => {
            json!([{ "insert": { "USER_ID": "u1", "TOTAL_SCORE": 12 } }])
        }
        Some(sql) if sql.contains("\"by_region\"") || sql.contains(" by_region") => {
            json!([{ "insert": { "region": "apac", "count": 3 } }])
        }
        Some(sql) if sql.contains("feldera_http_scores_page") => fake_feldera_paginated_rows(
            sql,
            vec![
                json!({ "insert": { "user_id": "u1", "sum": 12, "count": 2 } }),
                json!({ "insert": { "user_id": "u2", "sum": 9, "count": 1 } }),
                json!({ "insert": { "user_id": "u3", "sum": 13, "count": 1 } }),
            ],
        ),
        _ => json!([
            { "insert": { "user_id": "u1", "sum": 12, "count": 2, "avg_score": 6.0 } }
        ]),
    };
    (StatusCode::OK, Json(rows))
}

fn fake_feldera_paginated_rows(sql: &str, rows: Vec<Value>) -> Value {
    let upper = sql.to_ascii_uppercase();
    let limit = parse_sql_usize_after_keyword(&upper, "LIMIT");
    let offset = parse_sql_usize_after_keyword(&upper, "OFFSET").unwrap_or(0);
    let page = rows
        .into_iter()
        .skip(offset)
        .take(limit.unwrap_or(usize::MAX))
        .collect::<Vec<_>>();
    Value::Array(page)
}

fn parse_sql_usize_after_keyword(sql_upper: &str, keyword: &str) -> Option<usize> {
    let start = sql_upper.find(keyword)?;
    sql_upper[start + keyword.len()..]
        .trim_start()
        .split(|ch: char| !ch.is_ascii_digit())
        .next()
        .filter(|digits| !digits.is_empty())?
        .parse()
        .ok()
}

#[async_trait]
impl FelderaCompilerBackend for TestFelderaCompilerBackend {
    async fn compile(
        &self,
        request: FelderaCompilerBackendRequest,
    ) -> Result<FelderaCompilerBackendResponse, ApiError> {
        assert_eq!(
            request.compile_request_hash,
            feldera_compile_request_hash(&request.compiler_request).unwrap()
        );
        assert_eq!(
            request.program_code,
            feldera_sql_program_for_compile_request(&request.compiler_request).unwrap()
        );
        assert!(request.program_code.contains("CREATE TABLE \"scores\""));
        assert!(request
            .program_code
            .contains("CREATE MATERIALIZED VIEW \"compiler_backend_scores_by_user\" AS"));
        let catalog = request
            .catalogs
            .first()
            .expect("test compiler backend requires a catalog");
        let resolved_spec = scores_view_spec(
            catalog,
            request.view_id.as_str(),
            request.compiler_request.sql.as_str(),
        );
        let artifact = artifact_for_scores_spec(&resolved_spec, request.view_id.as_str());
        Ok(FelderaCompilerBackendResponse {
            resolved_spec,
            artifact: Some(artifact),
            product_runtime: None,
            runtime_deployment: None,
        })
    }
}

#[async_trait]
impl FelderaCompilerBackend for TestProductRuntimeFelderaCompilerBackend {
    async fn compile(
        &self,
        request: FelderaCompilerBackendRequest,
    ) -> Result<FelderaCompilerBackendResponse, ApiError> {
        assert_eq!(
            request.compile_request_hash,
            feldera_compile_request_hash(&request.compiler_request).unwrap()
        );
        assert_eq!(
            request.program_code,
            feldera_sql_program_for_compile_request(&request.compiler_request).unwrap()
        );
        assert!(request.program_code.contains("CREATE TABLE \"scores\""));
        assert!(request
            .program_code
            .contains("CREATE MATERIALIZED VIEW \"product_runtime_scores_by_user\" AS"));
        let catalog = request
            .catalogs
            .first()
            .expect("test compiler backend requires a catalog");
        let input_schema = catalog_input_relation_schema(catalog).unwrap();
        let input_schemas = vec![input_schema];
        let product_input_hash =
            feldera_artifact_bytes_hash(serde_json::to_vec(&input_schemas).unwrap().as_slice());
        let output_schema =
            scores_view_output_schema(request.view_id.as_str(), product_input_hash.as_str());
        let resolved_spec = StandingViewSpec {
            view_id: request.view_id.clone(),
            sql: request.compiler_request.sql.clone(),
            dialect: request.compiler_request.dialect.clone(),
            source_kind: request.compiler_request.source_kind.clone(),
            rust_extension: request.compiler_request.rust_extension.clone(),
            input_relations: input_schemas,
            output_relations: vec![output_schema],
            shape: StandingViewShape {
                is_materialized: true,
                multi_input: false,
                multi_output: false,
            },
        };
        let mut descriptor = FelderaPackageRuntimeDescriptorV1 {
            descriptor_version: FELDERA_PRODUCT_RUNTIME_DESCRIPTOR_VERSION,
            view_id: resolved_spec.view_id.clone(),
            spec_hash: feldera_spec_hash(&resolved_spec).unwrap(),
            compile_request_hash: request.compile_request_hash.clone(),
            backend: FelderaPackageBackendIdentity {
                name: "feldera-package-jarless".to_string(),
                version: "0.299.0-test".to_string(),
                source: "feldera public Rust packages".to_string(),
            },
            runtime_factory: FelderaPackageRuntimeFactoryBinding {
                crate_name: TEST_PRODUCT_RUNTIME_CRATE_NAME.to_string(),
                crate_version: TEST_PRODUCT_RUNTIME_CRATE_VERSION.to_string(),
                factory_symbol: "create_standing_runtime".to_string(),
            },
            input_schemas: resolved_spec.input_relations.clone(),
            output_schemas: resolved_spec.output_relations.clone(),
            state_codec: "feldera-package-runtime-state-v1".to_string(),
            state_schema_version: 1,
            standing_program_identity: StandingProgramIdentity {
                tenant_id: "default".to_string(),
                program_id: "placeholder".to_string(),
                view_ids: vec!["placeholder".to_string()],
                sql_hash: feldera_artifact_bytes_hash(b"placeholder-sql"),
                input_catalog_hash: feldera_artifact_bytes_hash(b"placeholder-input"),
                output_schema_hash: feldera_artifact_bytes_hash(b"placeholder-output"),
                compiler_identity: "placeholder".to_string(),
                runtime_packages: vec![
                    velorix_core::standing_program::FelderaRuntimePackageIdentity {
                        name: "placeholder".to_string(),
                        version: "placeholder".to_string(),
                    },
                ],
                package_feature_set: vec!["placeholder".to_string()],
                dbsp_runtime_compatibility: "placeholder".to_string(),
                checkpoint_codec_identity: "placeholder".to_string(),
                native_code_policy:
                    velorix_core::standing_program::NativeCodePolicy::DisabledNoExternalDependencies,
            },
        };
        descriptor.standing_program_identity =
            feldera_package_runtime_identity_for_descriptor(&resolved_spec, &descriptor)
                .expect("test product runtime descriptor identity must be valid");
        Ok(FelderaCompilerBackendResponse {
            resolved_spec,
            artifact: None,
            product_runtime: Some(descriptor),
            runtime_deployment: None,
        })
    }
}

#[async_trait]
impl FelderaCompilerBackend for TestSchemaOnlyFelderaCompilerBackend {
    async fn compile(
        &self,
        request: FelderaCompilerBackendRequest,
    ) -> Result<FelderaCompilerBackendResponse, ApiError> {
        assert_eq!(
            request.compile_request_hash,
            feldera_compile_request_hash(&request.compiler_request).unwrap()
        );
        assert_eq!(
            request.program_code,
            feldera_sql_program_for_compile_request(&request.compiler_request).unwrap()
        );
        assert!(request.program_code.contains("CREATE TABLE \"scores\""));
        assert!(request
            .program_code
            .contains("CREATE MATERIALIZED VIEW \"schema_only_scores_by_user\" AS"));
        let catalog = request
            .catalogs
            .first()
            .expect("test compiler backend requires a catalog");
        let resolved_spec = scores_view_spec(
            catalog,
            request.view_id.as_str(),
            request.compiler_request.sql.as_str(),
        );
        Ok(FelderaCompilerBackendResponse {
            resolved_spec,
            artifact: None,
            product_runtime: None,
            runtime_deployment: None,
        })
    }
}

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
