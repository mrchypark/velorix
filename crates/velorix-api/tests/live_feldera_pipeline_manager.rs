use std::{
    env, process,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{
        BinaryArray, Date32Array, Float32Array, Int16Array, Int32Array, Int64Array, Int8Array,
        StringArray, Time64NanosecondArray, TimestampNanosecondArray, UInt16Array, UInt32Array,
        UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use http_body_util::BodyExt as _;
use object_store::{memory::InMemory, ObjectStore};
use serde_json::json;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tower::ServiceExt as _;
use velorix_api::{
    app, ApiState, FelderaCompilerBackend, FelderaCompilerBackendRequest,
    FelderaPipelineManagerCompilerBackend, StandingProgramRuntimeFactory,
};
use velorix_core::{
    feldera_artifact::{
        catalog_input_relation_schema, feldera_compile_request_hash,
        feldera_sql_program_for_compile_request, ColumnSchema, FelderaCompileRequestV1,
        FelderaRustExtensionV1, OutputSchemaContract, RelationSchema, SqlDataType, SqlDialect,
        SqlSourceKind, StandingViewShape, StandingViewSpec,
    },
    relation::{
        ArrowPhysicalTypeV1, ArrowStructFieldV1, DataFusionRegistrationModeV1,
        DataFusionRegistrationV1, FelderaRelationBindingV1, IncrementalAdapterBindingV1,
        RelationColumnV1, RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1,
        VelorixLogicalTypeV1, VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        VelorixStructFieldV1, CATALOG_FELDERA_GENERIC_INCREMENTAL_ADAPTER_ID,
        CATALOG_ROW_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
    standing_program::{
        EpochIdempotencyKey, FelderaRuntimePackageIdentity, NativeCodePolicy, RelationInputBatch,
        ScopedViewId, SnapshotPageRequest, StandingProgramIdentity,
    },
};
use velorix_k8s::{crd::ObjectStoreAuthorityRef, startup::validate_operator_authority};

const LIVE_FELDERA_SCHEMA_TIMEOUT_DEFAULT_MS: u64 = 600_000;
const LIVE_FELDERA_RUNTIME_TIMEOUT_DEFAULT_MS: u64 = 3_600_000;

#[tokio::test]
async fn live_feldera_pipeline_manager_compiles_velorix_generated_program() {
    let Some(base_url) = live_feldera_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager compatibility test; set LIVE_FELDERA=1 and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let view_id = live_unique_id("velorix_live_scores_by_user");
    let compiler_request = FelderaCompileRequestV1 {
        view_id: view_id.clone(),
        sql: "select user_id, sum(score) as sum, count(*) as count from scores group by user_id"
            .to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: FelderaRustExtensionV1::default(),
        input_relations: vec![input_schema],
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let backend = FelderaPipelineManagerCompilerBackend::new(
        base_url,
        env::var("VELORIX_FELDERA_BEARER_TOKEN").ok(),
        Duration::from_millis(
            env_u64("VELORIX_FELDERA_COMPILER_POLL_INTERVAL_MS").unwrap_or(1_000),
        ),
        Duration::from_millis(live_feldera_compiler_timeout_ms()),
        env::var("VELORIX_FELDERA_COMPILER_PROFILE").unwrap_or_else(|_| "dev".to_string()),
        u32::try_from(env_u64("VELORIX_FELDERA_COMPILER_WORKERS").unwrap_or(1))
            .expect("VELORIX_FELDERA_COMPILER_WORKERS must fit u32"),
    )
    .unwrap();

    let response = backend
        .compile(FelderaCompilerBackendRequest {
            job_id: "live-feldera-compile".to_string(),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog],
        })
        .await
        .unwrap();

    let output = response.resolved_spec.output_relations.first().unwrap();
    assert_eq!(output.relation_id, view_id);
    assert_eq!(
        output
            .columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("user_id", &SqlDataType::Utf8),
            ("sum", &SqlDataType::Int64),
            ("count", &SqlDataType::Int64),
        ]
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_compiles_required_sql_families() {
    let Some(base_url) = live_feldera_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager SQL family compile test; set LIVE_FELDERA=1 and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let scores = live_scores_relation_catalog();
    let profiles = live_profiles_relation_catalog();

    let projection = live_compile_only(
        base_url.clone(),
        &live_unique_id("velorix_live_compile_positive_score_doubles"),
        "select user_id, score * 2 as doubled_score from scores where score >= 7",
        vec![scores.clone()],
        vec![live_scores_input_schema(&scores)],
    )
    .await;
    assert!(live_output_has_columns(
        &projection,
        &["user_id", "doubled_score"]
    ));

    let aggregate = live_compile_only(
        base_url.clone(),
        &live_unique_id("velorix_live_compile_score_aggregate_family"),
        "select user_id, min(score) as min_score, max(score) as max_score, avg(score) as avg_score from scores group by user_id",
        vec![scores.clone()],
        vec![live_scores_input_schema(&scores)],
    )
    .await;
    assert!(live_output_has_columns(
        &aggregate,
        &["user_id", "min_score", "max_score", "avg_score"]
    ));

    let join = live_compile_only(
        base_url,
        &live_unique_id("velorix_live_compile_scores_by_tier"),
        "select p.tier, sum(s.score) as total_score, count(*) as event_count from scores s join profiles p on s.user_id = p.user_id group by p.tier",
        vec![scores.clone(), profiles.clone()],
        vec![
            live_scores_input_schema(&scores),
            live_profiles_input_schema(&profiles),
        ],
    )
    .await;
    assert!(live_output_has_columns(
        &join,
        &["tier", "total_score", "event_count"]
    ));
}

#[tokio::test]
async fn live_feldera_pipeline_manager_compiles_complex_feldera_program_sql() {
    let Some(base_url) = live_feldera_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager FelderaProgram complex compile test; set LIVE_FELDERA=1 and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let scores = live_scores_relation_catalog();
    let profiles = live_profiles_relation_catalog();
    let tier_rollup = live_unique_id("velorix_live_program_tier_rollup");
    let gold_scores = live_unique_id("velorix_live_program_gold_scores");

    let outputs = live_compile_program_only(
        base_url,
        &live_unique_id("velorix_live_compile_complex_program"),
        &format!(
            "CREATE MATERIALIZED VIEW {tier_rollup} AS \
             WITH joined_scores AS (\
                 SELECT s.user_id, s.score, p.tier \
                 FROM scores s JOIN profiles p ON s.user_id = p.user_id \
                 WHERE s.score > 0\
             ) \
             SELECT tier, \
                    SUM(score) AS total_score, \
                    MIN(score) AS min_score, \
                    MAX(score) AS max_score, \
                    AVG(score) AS avg_score, \
                    CASE WHEN SUM(score) > 10 THEN 'large' ELSE 'standard' END AS score_band \
             FROM joined_scores \
             GROUP BY tier; \
             CREATE MATERIALIZED VIEW {gold_scores} AS \
             SELECT s.user_id, s.score \
             FROM scores s \
             WHERE EXISTS (\
                 SELECT 1 FROM profiles p WHERE p.user_id = s.user_id AND p.tier = 'gold'\
             )"
        ),
        vec![scores.clone(), profiles.clone()],
        vec![
            live_scores_input_schema(&scores),
            live_profiles_input_schema(&profiles),
        ],
    )
    .await;

    let tier_rollup_schema = outputs
        .iter()
        .find(|schema| schema.relation_id == tier_rollup)
        .unwrap_or_else(|| panic!("missing tier_rollup output; outputs={outputs:?}"));
    assert!(live_output_has_columns(
        tier_rollup_schema,
        &[
            "tier",
            "total_score",
            "min_score",
            "max_score",
            "avg_score",
            "score_band",
        ]
    ));
    let gold_scores_schema = outputs
        .iter()
        .find(|schema| schema.relation_id == gold_scores)
        .unwrap_or_else(|| panic!("missing gold_scores output; outputs={outputs:?}"));
    assert!(live_output_has_columns(
        gold_scores_schema,
        &["user_id", "score"]
    ));
}

#[tokio::test]
async fn live_feldera_pipeline_manager_compiles_expanded_scalar_input_types() {
    let Some(base_url) = live_feldera_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager expanded scalar input compile test; set LIVE_FELDERA=1 and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_expanded_scalars_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();

    let output = live_compile_only(
        base_url,
        &live_unique_id("velorix_live_compile_expanded_scalar_inputs"),
        "select id, i8_value, i16_value, i32_value, u8_value, u16_value, u32_value, u64_value, f32_value, code, raw, bytes, event_time, event_date, event_ts, uuid_value from expanded_scalars where u64_value > 0",
        vec![catalog],
        vec![input_schema],
    )
    .await;

    assert!(live_output_has_columns(
        &output,
        &[
            "id",
            "i8_value",
            "i16_value",
            "i32_value",
            "u8_value",
            "u16_value",
            "u32_value",
            "u64_value",
            "f32_value",
            "code",
            "raw",
            "bytes",
            "event_time",
            "event_date",
            "event_ts",
            "uuid_value",
        ]
    ));
}

#[tokio::test]
async fn live_feldera_pipeline_manager_compiles_nested_input_types() {
    let Some(base_url) = live_feldera_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager nested input compile test; set LIVE_FELDERA=1 and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_nested_inputs_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();

    let output = live_compile_only(
        base_url,
        &live_unique_id("velorix_live_compile_nested_inputs"),
        "select id, scores, attributes, profile from nested_inputs",
        vec![catalog],
        vec![input_schema],
    )
    .await;

    assert!(live_output_has_columns(
        &output,
        &["id", "scores", "attributes", "profile"]
    ));
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rejects_invalid_sql_without_fallback() {
    let Some(base_url) = live_feldera_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager invalid-SQL compatibility test; set LIVE_FELDERA=1 and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let view_id = live_unique_id("velorix_live_invalid_sql");
    let compiler_request = FelderaCompileRequestV1 {
        view_id: view_id.clone(),
        sql: "select user_id, definitely_not_a_feldera_function(score) as bad_value from scores group by user_id".to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema],
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let error = live_feldera_backend(base_url)
        .with_volatile_runtime_deployment()
        .compile(FelderaCompilerBackendRequest {
            job_id: "live-feldera-invalid-sql".to_string(),
            view_id,
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog],
        })
        .await
        .expect_err("invalid Feldera SQL must fail instead of falling back to a fixture runtime");
    let error_debug = format!("{error:?}");
    assert!(
        error_debug.contains("400")
            && (error_debug.contains("SqlError") || error_debug.contains("compiler returned")),
        "unexpected invalid-SQL error: {}",
        error_debug
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rejects_ignored_order_by_warning_without_fallback() {
    let Some(base_url) = live_feldera_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager ORDER BY warning compatibility test; set LIVE_FELDERA=1 and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let view_id = live_unique_id("velorix_live_ignored_order_by");
    let compiler_request = FelderaCompileRequestV1 {
        view_id: view_id.clone(),
        sql: "select user_id, score from scores order by score desc limit 2".to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema],
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let error = live_feldera_backend(base_url)
        .with_volatile_runtime_deployment()
        .compile(FelderaCompilerBackendRequest {
            job_id: "live-feldera-ignored-order-by".to_string(),
            view_id,
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog],
        })
        .await
        .expect_err("ORDER BY ignored warning must fail instead of accepting changed semantics");
    let error_debug = format!("{error:?}");
    assert!(
        error_debug.contains("ORDER BY clause is currently ignored")
            || error_debug.contains("ORDER BY is ignored"),
        "unexpected ORDER BY warning error: {}",
        error_debug
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rejects_unregistered_feldera_program_input_without_deploying(
) {
    let Some(base_url) = live_feldera_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager unregistered-input admission test; set LIVE_FELDERA=1 and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let view_id = live_unique_id("velorix_live_unregistered_program_input");
    let compiler_request = FelderaCompileRequestV1 {
        view_id: view_id.clone(),
        sql: "\
CREATE TABLE external_scores(\
    event_id VARCHAR NOT NULL, \
    user_id VARCHAR NOT NULL, \
    score BIGINT NOT NULL, \
    PRIMARY KEY(event_id)\
); \
CREATE MATERIALIZED VIEW external_score_rollup AS \
SELECT s.user_id, SUM(s.score + e.score) AS total_score \
FROM scores s JOIN external_scores e ON s.user_id = e.user_id \
GROUP BY s.user_id"
            .to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::FelderaProgram,
        rust_extension: Default::default(),
        input_relations: vec![input_schema],
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: true,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let error = live_feldera_backend(base_url)
        .compile(FelderaCompilerBackendRequest {
            job_id: "live-feldera-unregistered-program-input".to_string(),
            view_id,
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog],
        })
        .await
        .expect_err(
            "unregistered Feldera program input must fail admission before runtime deployment",
        );
    let error_debug = format!("{error:?}");
    assert!(
        error_debug.contains("unregistered input relation `external_scores`"),
        "unexpected unregistered-input admission error: {}",
        error_debug
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rejects_geometry_output_until_feldera_runtime_supports_it_without_fallback(
) {
    let Some(base_url) = live_feldera_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager GEOMETRY fail-closed compatibility test; set LIVE_FELDERA=1 and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let view_id = live_unique_id("velorix_live_geometry_output_unsupported");
    let compiler_request = FelderaCompileRequestV1 {
        view_id: view_id.clone(),
        sql: "select user_id, score, cast('POINT(1 2)' as GEOMETRY) as sample_shape from scores where event_id = 'e2'".to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema],
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let error = live_feldera_backend(base_url)
        .with_volatile_runtime_deployment()
        .compile(FelderaCompilerBackendRequest {
            job_id: "live-feldera-geometry-output-unsupported".to_string(),
            view_id,
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog],
        })
        .await
        .expect_err("GEOMETRY output must fail closed until Feldera runtime/codegen supports it");
    let error_debug = format!("{error:?}");
    assert!(
        (error_debug.contains("RustError")
            && (error_debug.contains("cast_to_geopoint_s")
                || error_debug.contains("GeoPoint")
                || error_debug.contains("GEOMETRY")))
            || error_debug.contains("stalled after SQL compilation"),
        "unexpected GEOMETRY fail-closed error: {}",
        error_debug
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rejects_two_arg_trunc_until_feldera_runtime_supports_it_without_fallback(
) {
    let Some(base_url) = live_feldera_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager two-arg TRUNC fail-closed compatibility test; set LIVE_FELDERA=1 and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let view_id = live_unique_id("velorix_live_two_arg_trunc_unsupported");
    let compiler_request = FelderaCompileRequestV1 {
        view_id: view_id.clone(),
        sql: "select event_id, user_id, score, trunc(cast(score as double), 2) as truncated_score from scores where event_id = 'e2'".to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema],
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let error = live_feldera_backend(base_url)
        .with_volatile_runtime_deployment()
        .compile(FelderaCompilerBackendRequest {
            job_id: "live-feldera-two-arg-trunc-unsupported".to_string(),
            view_id,
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog],
        })
        .await
        .expect_err("two-arg TRUNC must fail closed until Feldera runtime/codegen supports it");
    let error_debug = format!("{error:?}");
    assert!(
        (error_debug.contains("RustError") && error_debug.contains("trunc_d_i32"))
            || error_debug.contains("stalled after SQL compilation"),
        "unexpected two-arg TRUNC fail-closed error: {}",
        error_debug
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rejects_documented_unsupported_sql_without_fallback() {
    let Some(base_url) = live_feldera_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager documented unsupported SQL compatibility test; set LIVE_FELDERA=1 and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let cases: &[(&str, &str, &[&str])] = &[
        (
            "intersect-all",
            "select user_id from scores intersect all select user_id from scores",
            &["INTERSECT ALL", "not supported", "SqlError"],
        ),
        (
            "except-all",
            "select user_id from scores except all select user_id from scores where score = 0",
            &["EXCEPT ALL", "not supported", "SqlError"],
        ),
        (
            "match-recognize",
            "\
select *
from scores
match_recognize (
  partition by user_id
  order by score
  measures score as matched_score
  pattern (a)
  define a as score > 0
)",
            &["MATCH_RECOGNIZE", "SqlError", "not supported"],
        ),
        (
            "window-rows-frame",
            "\
select
  event_id,
  user_id,
  sum(score) over (
    partition by user_id
    order by score
    rows between unbounded preceding and current row
  ) as running_score
from scores",
            &["ROWS BETWEEN", "not supported", "SqlError"],
        ),
        (
            "window-ntile",
            "select event_id, user_id, ntile(2) over (partition by user_id order by score) as score_bucket from scores",
            &["NTILE", "not implemented", "SqlError"],
        ),
    ];

    for (case, sql, expected_fragments) in cases {
        expect_live_feldera_compile_rejects(
            base_url.clone(),
            &live_unique_id(&format!("velorix_live_unsupported_sql_{case}")),
            &format!("live-feldera-unsupported-sql-{case}"),
            sql,
            expected_fragments,
        )
        .await;
    }
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rest_api_compiles_ingests_and_queries_join_view() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager REST product runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let view_id = live_unique_id("velorix_live_rest_scores_by_tier");
    let url_path = format!("/live-feldera/{view_id}/scores-by-tier");
    let scores = live_scores_rest_relation_catalog();
    let profiles = live_profiles_relation_catalog();
    let (state, _store) = live_api_state_memory(&view_id).await;
    let backend = Arc::new(live_feldera_backend(base_url).with_volatile_runtime_deployment());
    let app = app(state.with_feldera_pipeline_manager_backend(backend));

    for catalog in [scores, profiles] {
        let relation = live_request_json(
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

    let view = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": view_id,
            "urlPath": url_path,
            "input_relation_refs": [
                { "relation_id": "scores", "relation_version": "live.v1" },
                { "relation_id": "profiles", "relation_version": "live.v1" }
            ],
            "sql": "select p.tier, sum(s.score) as total_score, count(*) as event_count from scores s join profiles p on s.user_id = p.user_id where s.score > 0 group by p.tier",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let run = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 0, "run body: {}", run.1);

    let scores_ingest = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "live.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "event_id": "e1", "user_id": "u1", "score": 7, "delta": 1 },
                { "event_id": "e2", "user_id": "u1", "score": 5, "delta": 1 },
                { "event_id": "e3", "user_id": "u2", "score": 3, "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(
        scores_ingest.0,
        StatusCode::CREATED,
        "scores ingest body: {}",
        scores_ingest.1
    );

    let profiles_ingest = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "profiles",
            "relation_version": "live.v1",
            "stream_id": "profiles",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "user_id": "u1", "tier": "gold", "delta": 1 },
                { "user_id": "u2", "tier": "silver", "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(
        profiles_ingest.0,
        StatusCode::CREATED,
        "profiles ingest body: {}",
        profiles_ingest.1
    );

    let query = live_request_json(app, Method::GET, &format!("/v1/api{}", url_path), None).await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    let rows = query.1["rows"]
        .as_array()
        .expect("query rows must be array");
    assert!(
        rows.iter()
            .any(|row| row == &json!({ "tier": "gold", "total_score": 12, "event_count": 2 })),
        "expected gold aggregate; rows={:?}",
        rows
    );
    assert!(
        rows.iter()
            .any(|row| row == &json!({ "tier": "silver", "total_score": 3, "event_count": 1 })),
        "expected silver aggregate; rows={:?}",
        rows
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rest_api_ingests_and_queries_nested_input_view() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager REST nested input runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let view_id = live_unique_id("velorix_live_rest_nested_inputs");
    let url_path = format!("/live-feldera/{view_id}/nested-inputs");
    let catalog = live_nested_inputs_relation_catalog();
    let (state, _store) = live_api_state_memory(&view_id).await;
    let backend = Arc::new(live_feldera_backend(base_url).with_volatile_runtime_deployment());
    let app = app(state.with_feldera_pipeline_manager_backend(backend));

    let relation = live_request_json(
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

    let view = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": view_id,
            "urlPath": url_path,
            "input_relation_refs": [
                { "relation_id": "nested_inputs", "relation_version": "live.v1" }
            ],
            "sql": "select id, scores, attributes, profile from nested_inputs where amount > 0",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let run = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 0, "run body: {}", run.1);

    let ingest = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "nested_inputs",
            "relation_version": "live.v1",
            "stream_id": "nested_inputs",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                {
                    "id": "row-1",
                    "scores": [10, null, 30],
                    "attributes": { "critical": 9, "batch": null },
                    "profile": { "name": "ada", "tier": 2 },
                    "amount": 42,
                    "delta": 1
                },
                {
                    "id": "row-filtered",
                    "scores": [1],
                    "attributes": { "critical": 1 },
                    "profile": { "name": "ignored", "tier": null },
                    "amount": 0,
                    "delta": 1
                }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = live_request_json(app, Method::GET, &format!("/v1/api{}", url_path), None).await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    let rows = query.1["rows"]
        .as_array()
        .expect("query rows must be array");
    assert!(
        rows.iter().any(|row| {
            row == &json!({
                "id": "row-1",
                "scores": [10, null, 30],
                "attributes": { "critical": 9, "batch": null },
                "profile": { "name": "ada", "tier": 2 }
            })
        }),
        "expected nested input projection output; rows={rows:?}"
    );
    assert!(
        !rows.iter().any(|row| row["id"] == json!("row-filtered")),
        "amount=0 row should be filtered; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rest_api_supports_feldera_program_multi_output() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager REST feldera_program multi-output test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let program_id = live_unique_id("velorix_live_rest_program_multi_output");
    let by_user = live_unique_id("velorix_live_rest_program_by_user");
    let zero_scores = live_unique_id("velorix_live_rest_program_zero_scores");
    let url_path = format!("/live-feldera/{program_id}/program/by-user/:user_id");
    let catalog = live_scores_rest_relation_catalog();
    let (state, _store) = live_api_state_memory(&program_id).await;
    let backend = Arc::new(live_feldera_backend(base_url).with_volatile_runtime_deployment());
    let app = app(state.with_feldera_pipeline_manager_backend(backend));

    let relation = live_request_json(
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

    let view = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": program_id,
            "urlPath": url_path,
            "outputRelationId": by_user,
            "output_relation_ids": [by_user, zero_scores],
            "input_relation_id": "scores",
            "input_relation_version": "live.v1",
            "source_kind": "feldera_program",
            "request": [{
                "fieldName": "user_id",
                "fieldIn": "path",
                "type": "string",
                "validators": ["required", "string"]
            }],
            "sql_template": format!(
                "select user_id, total_score from \"{by_user}\" where user_id = {{{{ context.params.user_id | is_required | is_string }}}}"
            ),
            "sql": format!(
                "CREATE MATERIALIZED VIEW {by_user} AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id; \
                 CREATE MATERIALIZED VIEW {zero_scores} AS SELECT user_id, score FROM scores WHERE score = 0"
            ),
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["source_kind"], "feldera_program");
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let run = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 0, "run body: {}", run.1);

    let detail = live_request_json(
        app.clone(),
        Method::GET,
        &format!("/v1/views/{program_id}"),
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    let output_endpoints = detail.1["output_query_endpoints"]
        .as_array()
        .expect("output endpoints must be array");
    assert!(
        output_endpoints.iter().any(|endpoint| endpoint
            == &json!(format!("/v1/views/{program_id}/outputs/{by_user}/query"))),
        "expected by_user output endpoint; detail={}",
        detail.1
    );
    assert!(
        output_endpoints.iter().any(|endpoint| endpoint
            == &json!(format!(
                "/v1/views/{program_id}/outputs/{zero_scores}/query"
            ))),
        "expected zero_scores output endpoint; detail={}",
        detail.1
    );

    let ingest = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "live.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "event_id": "e1", "user_id": "u1", "score": 5, "delta": 1 },
                { "event_id": "e2", "user_id": "u1", "score": 7, "delta": 1 },
                { "event_id": "e3", "user_id": "u2", "score": 9, "delta": 1 },
                { "event_id": "e4", "user_id": "u3", "score": 0, "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let promoted = live_request_json(
        app.clone(),
        Method::GET,
        &format!(
            "/v1/api{}",
            url_path.trim_end_matches(":user_id").to_string() + "u1"
        ),
        None,
    )
    .await;
    assert_eq!(
        promoted.0,
        StatusCode::OK,
        "promoted query body: {}",
        promoted.1
    );
    assert!(
        promoted.1["rows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row == &json!({ "user_id": "u1", "total_score": 12 })),
        "expected promoted by_user output for u1; rows={:?}",
        promoted.1["rows"]
    );

    let zero_query = live_request_json(
        app,
        Method::GET,
        &format!("/v1/views/{program_id}/outputs/{zero_scores}/query"),
        None,
    )
    .await;
    assert_eq!(
        zero_query.0,
        StatusCode::OK,
        "zero output query body: {}",
        zero_query.1
    );
    assert!(
        zero_query.1["rows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row == &json!({ "user_id": "u3", "score": 0 })),
        "expected zero_scores output for u3; rows={:?}",
        zero_query.1["rows"]
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rest_api_discovers_feldera_program_outputs_without_hints() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager REST feldera_program output-discovery test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let program_id = live_unique_id("velorix_live_rest_program_discovered");
    let by_user = live_unique_id("velorix_live_rest_discovered_by_user");
    let zero_scores = live_unique_id("velorix_live_rest_discovered_zero_scores");
    let catalog = live_scores_rest_relation_catalog();
    let (state, _store) = live_api_state_memory(&program_id).await;
    let backend = Arc::new(live_feldera_backend(base_url).with_volatile_runtime_deployment());
    let app = app(state.with_feldera_pipeline_manager_backend(backend));

    let relation = live_request_json(
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

    let view = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": program_id,
            "input_relation_id": "scores",
            "input_relation_version": "live.v1",
            "source_kind": "feldera_program",
            "sql": format!(
                "CREATE MATERIALIZED VIEW {by_user} AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id; \
                 CREATE MATERIALIZED VIEW {zero_scores} AS SELECT user_id, score FROM scores WHERE score = 0"
            ),
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["source_kind"], "feldera_program");
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");
    assert_eq!(
        view.1["lifecycle"]["compile_status"], "pending",
        "view body: {}",
        view.1
    );

    let run = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 0, "run body: {}", run.1);

    let detail = live_request_json(
        app.clone(),
        Method::GET,
        &format!("/v1/views/{program_id}"),
        None,
    )
    .await;
    assert_eq!(detail.0, StatusCode::OK, "detail body: {}", detail.1);
    assert_eq!(detail.1["execution_mode"], "standing_runtime");
    let output_endpoints = detail.1["output_query_endpoints"]
        .as_array()
        .expect("output endpoints must be array");
    assert!(
        output_endpoints.iter().any(|endpoint| endpoint
            == &json!(format!("/v1/views/{program_id}/outputs/{by_user}/query"))),
        "expected compiler-discovered by_user output endpoint; detail={}",
        detail.1
    );
    assert!(
        output_endpoints.iter().any(|endpoint| endpoint
            == &json!(format!(
                "/v1/views/{program_id}/outputs/{zero_scores}/query"
            ))),
        "expected compiler-discovered zero_scores output endpoint; detail={}",
        detail.1
    );

    let ingest = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "live.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "event_id": "e1", "user_id": "u1", "score": 5, "delta": 1 },
                { "event_id": "e2", "user_id": "u1", "score": 7, "delta": 1 },
                { "event_id": "e3", "user_id": "u2", "score": 9, "delta": 1 },
                { "event_id": "e4", "user_id": "u3", "score": 0, "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let by_user_query = live_request_json(
        app.clone(),
        Method::GET,
        &format!("/v1/views/{program_id}/outputs/{by_user}/query"),
        None,
    )
    .await;
    assert_eq!(
        by_user_query.0,
        StatusCode::OK,
        "by_user query body: {}",
        by_user_query.1
    );
    assert!(
        by_user_query.1["rows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row == &json!({ "user_id": "u1", "total_score": 12 })),
        "expected compiler-discovered by_user output for u1; rows={:?}",
        by_user_query.1["rows"]
    );

    let zero_query = live_request_json(
        app,
        Method::GET,
        &format!("/v1/views/{program_id}/outputs/{zero_scores}/query"),
        None,
    )
    .await;
    assert_eq!(
        zero_query.0,
        StatusCode::OK,
        "zero output query body: {}",
        zero_query.1
    );
    assert!(
        zero_query.1["rows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row == &json!({ "user_id": "u3", "score": 0 })),
        "expected compiler-discovered zero_scores output for u3; rows={:?}",
        zero_query.1["rows"]
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rest_api_supports_raw_sql_query_on_output_endpoint() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager REST raw SQL output query test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let view_id = live_unique_id("velorix_live_rest_raw_sql_by_user");
    let catalog = live_scores_rest_relation_catalog();
    let (state, _store) = live_api_state_memory(&view_id).await;
    let backend = Arc::new(live_feldera_backend(base_url).with_volatile_runtime_deployment());
    let app = app(state.with_feldera_pipeline_manager_backend(backend));

    let relation = live_request_json(
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

    let view = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": view_id,
            "input_relation_id": "scores",
            "input_relation_version": "live.v1",
            "sql": "select user_id, sum(score) as total_score from scores where score > 0 group by user_id",
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let run = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 0, "run body: {}", run.1);

    let ingest = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "live.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "event_id": "e1", "user_id": "u1", "score": 5, "delta": 1 },
                { "event_id": "e2", "user_id": "u1", "score": 7, "delta": 1 },
                { "event_id": "e3", "user_id": "u2", "score": 3, "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let caller_sql = format!(
        "WITH scoped AS (SELECT user_id, total_score FROM \"{view_id}\") \
         SELECT user_id, total_score, 'large' AS bucket FROM scoped WHERE total_score >= 10 \
         UNION ALL \
         SELECT user_id, total_score, 'small' AS bucket FROM scoped WHERE total_score < 10"
    );
    let query = live_request_json(
        app,
        Method::POST,
        &format!("/v1/views/{view_id}/outputs/{view_id}/query"),
        Some(json!({ "sql": caller_sql })),
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    let rows = query.1["rows"]
        .as_array()
        .expect("query rows must be array");
    assert!(
        rows.iter()
            .any(|row| row == &json!({ "user_id": "u1", "total_score": 12, "bucket": "large" })),
        "expected raw SQL large bucket row; rows={rows:?}"
    );
    assert!(
        rows.iter()
            .any(|row| row == &json!({ "user_id": "u2", "total_score": 3, "bucket": "small" })),
        "expected raw SQL small bucket row; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rest_api_supports_array_query_parameter() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager REST array query parameter test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let view_id = live_unique_id("velorix_live_rest_array_param_by_user");
    let url_path = format!("/live-feldera/{view_id}/scores/filter");
    let catalog = live_scores_rest_relation_catalog();
    let (state, _store) = live_api_state_memory(&view_id).await;
    let backend = Arc::new(live_feldera_backend(base_url).with_volatile_runtime_deployment());
    let app = app(state.with_feldera_pipeline_manager_backend(backend));

    let relation = live_request_json(
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

    let view = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": view_id,
            "urlPath": url_path,
            "input_relation_id": "scores",
            "input_relation_version": "live.v1",
            "sql": "select user_id, sum(score) as total_score from scores where score > 0 group by user_id",
            "request": [{
                "fieldName": "user_ids",
                "fieldIn": "query",
                "type": "array",
                "validators": ["required", "array(element=string)"]
            }],
            "sql_template": format!(
                "select user_id, total_score from \"{view_id}\" where user_id in unnest({{{{ context.params.user_ids | is_required | is_array(element=string) }}}}) order by user_id"
            ),
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let run = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 0, "run body: {}", run.1);

    let ingest = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "live.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "event_id": "e1", "user_id": "u1", "score": 5, "delta": 1 },
                { "event_id": "e2", "user_id": "u1", "score": 7, "delta": 1 },
                { "event_id": "e3", "user_id": "u2", "score": 3, "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = live_request_json(
        app,
        Method::GET,
        &format!("/v1/api{url_path}?user_ids=%5B%22u1%22%5D"),
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    let rows = query.1["rows"]
        .as_array()
        .expect("query rows must be array");
    assert!(
        rows.iter()
            .any(|row| row == &json!({ "user_id": "u1", "total_score": 12 })),
        "expected array parameter filtered row for u1; rows={rows:?}"
    );
    assert!(
        !rows
            .iter()
            .any(|row| row == &json!({ "user_id": "u2", "total_score": 3 })),
        "array parameter should filter out u2; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rest_api_supports_typed_literal_query_parameters() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager REST typed literal query parameter test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let view_id = live_unique_id("velorix_live_rest_typed_params");
    let url_path = format!("/live-feldera/{view_id}/typed/filter");
    let catalog = live_expanded_scalars_relation_catalog();
    let (state, _store) = live_api_state_memory(&view_id).await;
    let backend = Arc::new(live_feldera_backend(base_url).with_volatile_runtime_deployment());
    let app = app(state.with_feldera_pipeline_manager_backend(backend));

    let relation = live_request_json(
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

    let view = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": view_id,
            "urlPath": url_path,
            "input_relation_id": "expanded_scalars",
            "input_relation_version": "live.v1",
            "sql": "select id, event_date, event_time, event_ts, uuid_value, amount, raw from expanded_scalars",
            "request": [
                { "fieldName": "event_date", "fieldIn": "query", "type": "string", "validators": ["required", "date"] },
                { "fieldName": "event_time", "fieldIn": "query", "type": "string", "validators": ["required", "time"] },
                { "fieldName": "event_ts", "fieldIn": "query", "type": "string", "validators": ["required", "timestamp"] },
                { "fieldName": "uuid_value", "fieldIn": "query", "type": "string", "validators": ["required", "uuid"] },
                { "fieldName": "amount", "fieldIn": "query", "type": "string", "validators": ["required", "decimal"] },
                { "fieldName": "raw", "fieldIn": "query", "type": "string", "validators": ["required", "binary_hex"] }
            ],
            "sql_template": format!(
                "select id, amount from \"{view_id}\" \
                 where event_date = {{{{ context.params.event_date | is_required | is_date }}}} \
                   and event_time = {{{{ context.params.event_time | is_required | is_time }}}} \
                   and event_ts = {{{{ context.params.event_ts | is_required | is_timestamp }}}} \
                   and uuid_value = {{{{ context.params.uuid_value | is_required | is_uuid }}}} \
                   and amount = {{{{ context.params.amount | is_required | is_decimal }}}} \
                   and raw = {{{{ context.params.raw | is_required | is_binary_hex }}}}"
            ),
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let run = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 0, "run body: {}", run.1);

    let ingest = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "expanded_scalars",
            "relation_version": "live.v1",
            "stream_id": "expanded_scalars",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{
                "id": "x1",
                "i8_value": -8,
                "i16_value": -16,
                "i32_value": -32,
                "u8_value": 8,
                "u16_value": 16,
                "u32_value": 32,
                "u64_value": 64,
                "f32_value": 3.5,
                "code": "ABCD",
                "raw": "0x0A0BFF",
                "bytes": "0xDEADBEEF",
                "event_time": "01:02:03",
                "event_date": "2026-06-10",
                "event_ts": "2026-06-10 01:02:03",
                "uuid_value": "018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa",
                "amount": 100,
                "delta": 1
            }]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = live_request_json(
        app,
        Method::GET,
        &format!(
            "/v1/api{url_path}?event_date=2026-06-10&event_time=01%3A02%3A03&event_ts=2026-06-10T01%3A02%3A03&uuid_value=018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa&amount=100&raw=0x0A0BFF"
        ),
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    let rows = query.1["rows"]
        .as_array()
        .expect("query rows must be array");
    assert!(
        rows.iter()
            .any(|row| row == &json!({ "id": "x1", "amount": 100 })),
        "expected typed literal query parameter filtered row; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rest_api_supports_typed_array_query_parameters() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager REST typed array query parameter test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let view_id = live_unique_id("velorix_live_rest_typed_array_params");
    let url_path = format!("/live-feldera/{view_id}/typed-array/filter");
    let catalog = live_expanded_scalars_relation_catalog();
    let (state, _store) = live_api_state_memory(&view_id).await;
    let backend = Arc::new(live_feldera_backend(base_url).with_volatile_runtime_deployment());
    let app = app(state.with_feldera_pipeline_manager_backend(backend));

    let relation = live_request_json(
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

    let view = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": view_id,
            "urlPath": url_path,
            "input_relation_id": "expanded_scalars",
            "input_relation_version": "live.v1",
            "sql": "select id, event_date, event_time, event_ts, uuid_value, amount, raw from expanded_scalars",
            "request": [
                { "fieldName": "event_dates", "fieldIn": "query", "type": "array", "validators": ["required", "array(element=date)"] },
                { "fieldName": "event_times", "fieldIn": "query", "type": "array", "validators": ["required", "array(element=time)"] },
                { "fieldName": "event_timestamps", "fieldIn": "query", "type": "array", "validators": ["required", "array(element=timestamp)"] },
                { "fieldName": "uuid_values", "fieldIn": "query", "type": "array", "validators": ["required", "array(element=uuid)"] },
                { "fieldName": "amounts", "fieldIn": "query", "type": "array", "validators": ["required", "array(element=decimal)"] },
                { "fieldName": "raw_values", "fieldIn": "query", "type": "array", "validators": ["required", "array(element=binary_hex)"] }
            ],
            "sql_template": format!(
                "select id, amount from \"{view_id}\" \
                 where event_date in unnest({{{{ context.params.event_dates | is_required | is_array(element=date) }}}}) \
                   and event_time in unnest({{{{ context.params.event_times | is_required | is_array(element=time) }}}}) \
                   and event_ts in unnest({{{{ context.params.event_timestamps | is_required | is_array(element=timestamp) }}}}) \
                   and uuid_value in unnest({{{{ context.params.uuid_values | is_required | is_array(element=uuid) }}}}) \
                   and CAST(amount AS DECIMAL(10, 0)) in unnest({{{{ context.params.amounts | is_required | is_array(element=decimal) }}}}) \
                   and raw in unnest({{{{ context.params.raw_values | is_required | is_array(element=binary_hex) }}}})"
            ),
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let run = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 0, "run body: {}", run.1);

    let ingest = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "expanded_scalars",
            "relation_version": "live.v1",
            "stream_id": "expanded_scalars",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{
                "id": "x1",
                "i8_value": -8,
                "i16_value": -16,
                "i32_value": -32,
                "u8_value": 8,
                "u16_value": 16,
                "u32_value": 32,
                "u64_value": 64,
                "f32_value": 3.5,
                "code": "ABCD",
                "raw": "0x0A0BFF",
                "bytes": "0xDEADBEEF",
                "event_time": "01:02:03",
                "event_date": "2026-06-10",
                "event_ts": "2026-06-10 01:02:03",
                "uuid_value": "018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa",
                "amount": 100,
                "delta": 1
            }]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = live_request_json(
        app,
        Method::GET,
        &format!(
            "/v1/api{url_path}?event_dates=%5B%222026-06-10%22%2C%222026-06-11%22%5D&event_times=%5B%2201%3A02%3A03%22%5D&event_timestamps=%5B%222026-06-10T01%3A02%3A03%22%5D&uuid_values=%5B%22018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa%22%5D&amounts=%5B%22100%22%5D&raw_values=%5B%220x0A0BFF%22%5D"
        ),
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    let rows = query.1["rows"]
        .as_array()
        .expect("query rows must be array");
    assert!(
        rows.iter()
            .any(|row| row == &json!({ "id": "x1", "amount": 100 })),
        "expected typed array query parameter filtered row; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rest_api_supports_json_query_parameter() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager REST JSON query parameter test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let view_id = live_unique_id("velorix_live_rest_json_param");
    let url_path = format!("/live-feldera/{view_id}/json/filter");
    let catalog = live_json_events_relation_catalog();
    let (state, _store) = live_api_state_memory(&view_id).await;
    let backend = Arc::new(live_feldera_backend(base_url).with_volatile_runtime_deployment());
    let app = app(state.with_feldera_pipeline_manager_backend(backend));

    let relation = live_request_json(
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

    let view = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": view_id,
            "urlPath": url_path,
            "input_relation_id": "json_events",
            "input_relation_version": "live.v1",
            "sql": "select id, raw_json from json_events",
            "request": [
                { "fieldName": "payload", "fieldIn": "query", "type": "json", "validators": ["required", "json"] }
            ],
            "sql_template": format!(
                "select id from \"{view_id}\" \
                 where raw_json = {{{{ context.params.payload | is_required | is_json }}}}"
            ),
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let run = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 0, "run body: {}", run.1);

    let ingest = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "json_events",
            "relation_version": "live.v1",
            "stream_id": "json_events",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [{
                "id": "j1",
                "payload": { "name": "Ada", "scores": [8, 13], "nested": { "active": true } },
                "raw_json": "{\"count\":3,\"flag\":true}",
                "weight": 1
            }]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let query = live_request_json(
        app,
        Method::GET,
        &format!("/v1/api{url_path}?payload=%7B%22count%22%3A3%2C%22flag%22%3Atrue%7D"),
        None,
    )
    .await;
    assert_eq!(query.0, StatusCode::OK, "query body: {}", query.1);
    let rows = query.1["rows"]
        .as_array()
        .expect("query rows must be array");
    assert!(
        rows.iter().any(|row| row == &json!({ "id": "j1" })),
        "expected JSON query parameter filtered row; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_rest_api_paginates_promoted_sql_template() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager REST promoted API pagination test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let view_id = live_unique_id("velorix_live_rest_api_page_scores");
    let url_path = format!("/live-feldera/{view_id}/scores/page");
    let catalog = live_scores_rest_relation_catalog();
    let (state, _store) = live_api_state_memory(&view_id).await;
    let backend = Arc::new(live_feldera_backend(base_url).with_volatile_runtime_deployment());
    let app = app(state.with_feldera_pipeline_manager_backend(backend));

    let relation = live_request_json(
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

    let view = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/views",
        Some(json!({
            "view_id": view_id,
            "urlPath": url_path,
            "input_relation_id": "scores",
            "input_relation_version": "live.v1",
            "sql": "select user_id, sum(score) as total_score from scores where score > 0 group by user_id",
            "sql_template": format!(
                "select user_id, total_score from \"{view_id}\" order by user_id"
            ),
            "response_formats": ["json"]
        })),
    )
    .await;
    assert_eq!(view.0, StatusCode::ACCEPTED, "view body: {}", view.1);
    assert_eq!(view.1["execution_mode"], "feldera_compile_pending");

    let run = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/view-compile-deploy/run-once",
        None,
    )
    .await;
    assert_eq!(run.0, StatusCode::OK, "run body: {}", run.1);
    assert_eq!(run.1["activated"], 1, "run body: {}", run.1);
    assert_eq!(run.1["failed"], 0, "run body: {}", run.1);

    let ingest = live_request_json(
        app.clone(),
        Method::POST,
        "/v1/ingest",
        Some(json!({
            "relation_id": "scores",
            "relation_version": "live.v1",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "rows": [
                { "event_id": "e1", "user_id": "u1", "score": 5, "delta": 1 },
                { "event_id": "e2", "user_id": "u1", "score": 7, "delta": 1 },
                { "event_id": "e3", "user_id": "u2", "score": 3, "delta": 1 },
                { "event_id": "e4", "user_id": "u3", "score": 9, "delta": 1 }
            ]
        })),
    )
    .await;
    assert_eq!(ingest.0, StatusCode::CREATED, "ingest body: {}", ingest.1);

    let first = live_request_json(
        app.clone(),
        Method::GET,
        &format!("/v1/api{url_path}?max_rows=2"),
        None,
    )
    .await;
    assert_eq!(first.0, StatusCode::OK, "first page body: {}", first.1);
    let first_rows = first.1["rows"]
        .as_array()
        .expect("first page rows must be array");
    assert_eq!(first_rows.len(), 2, "first page rows={first_rows:?}");
    assert!(
        first_rows
            .iter()
            .any(|row| row == &json!({ "user_id": "u1", "total_score": 12 })),
        "expected first page to include u1; rows={first_rows:?}"
    );
    assert_eq!(first.1["next_page_token"], "offset:2");

    let second = live_request_json(
        app,
        Method::GET,
        &format!("/v1/api{url_path}?max_rows=2&page_token=offset:2"),
        None,
    )
    .await;
    assert_eq!(second.0, StatusCode::OK, "second page body: {}", second.1);
    let second_rows = second.1["rows"]
        .as_array()
        .expect("second page rows must be array");
    assert_eq!(second_rows.len(), 1, "second page rows={second_rows:?}");
    assert!(
        second_rows
            .iter()
            .any(|row| row == &json!({ "user_id": "u3", "total_score": 9 })),
        "expected second page to include u3; rows={second_rows:?}"
    );
    assert!(second.1.get("next_page_token").is_none());
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_ingests_and_queries_velorix_program() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager runtime compatibility test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let view_id = live_unique_id("velorix_live_scores_by_user");
    let compiler_request = live_scores_compile_request(input_schema.clone(), &view_id);
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let backend = live_feldera_backend(base_url).with_volatile_runtime_deployment();

    let response = backend
        .compile(FelderaCompilerBackendRequest {
            job_id: "live-feldera-runtime".to_string(),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request: compiler_request.clone(),
            catalogs: vec![catalog.clone()],
        })
        .await
        .unwrap();
    let output_schemas = response.resolved_spec.output_relations.clone();
    let input_schemas = vec![input_schema];
    let identity =
        live_standing_program_identity_for(&view_id, &response.resolved_spec, &[catalog.clone()]);
    let mut runtime = backend
        .create_with_catalogs_and_spec(
            &identity,
            &[catalog.clone()],
            &response.resolved_spec,
            &input_schemas,
            &output_schemas,
        )
        .unwrap();
    let batch = live_scores_record_batch();
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("live-feldera-runtime-epoch-1").unwrap(),
            vec![RelationInputBatch {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: batch.num_rows() as u64,
                batches: vec![batch],
            }],
        )
        .unwrap();
    let page = runtime
        .materialized_view_sql_page(
            ScopedViewId {
                tenant_id: identity.tenant_id.clone(),
                program_id: identity.program_id.clone(),
                view_id: view_id.clone(),
            },
            format!("SELECT * FROM \"{view_id}\""),
            SnapshotPageRequest::default(),
        )
        .unwrap();

    assert!(
        page.rows.iter().any(|row| live_row_matches_sum_count(row)),
        "expected u1 sum/count output after live Feldera ingest; rows={:?}",
        page.rows
    );

    let checkpoint = runtime.checkpoint().unwrap();
    let restored = backend
        .restore_with_catalogs_and_spec(
            checkpoint,
            &[catalog],
            &response.resolved_spec,
            &input_schemas,
            &output_schemas,
        )
        .unwrap();
    let restored_page = restored
        .materialized_view_sql_page(
            ScopedViewId {
                tenant_id: identity.tenant_id,
                program_id: identity.program_id,
                view_id: view_id.clone(),
            },
            format!("SELECT * FROM \"{view_id}\""),
            SnapshotPageRequest::default(),
        )
        .unwrap();
    assert!(
        restored_page
            .rows
            .iter()
            .any(|row| live_row_matches_sum_count(row)),
        "expected u1 sum/count output after live Feldera restore; rows={:?}",
        restored_page.rows
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_feldera_program_multi_output() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager feldera_program multi-output runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let program_id = live_unique_id("velorix_live_program_multi_output");
    let by_user = live_unique_id("velorix_live_program_by_user");
    let zero_scores = live_unique_id("velorix_live_program_zero_scores");
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let input_schemas = vec![input_schema];
    let compiler_request = FelderaCompileRequestV1 {
        view_id: program_id.clone(),
        sql: format!(
            "CREATE MATERIALIZED VIEW {by_user} AS SELECT user_id, SUM(score) AS total_score FROM scores GROUP BY user_id; \
             CREATE MATERIALIZED VIEW {zero_scores} AS SELECT user_id, score FROM scores WHERE score = 0"
        ),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::FelderaProgram,
        rust_extension: Default::default(),
        input_relations: input_schemas.clone(),
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: true,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let backend = live_feldera_backend(base_url).with_volatile_runtime_deployment();

    let response = backend
        .compile(FelderaCompilerBackendRequest {
            job_id: "live-feldera-program-multi-output".to_string(),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog.clone()],
        })
        .await
        .unwrap();
    let output_schemas = response.resolved_spec.output_relations.clone();
    let output_ids = output_schemas
        .iter()
        .map(|schema| schema.relation_id.clone())
        .collect::<Vec<_>>();
    assert!(
        output_ids.iter().any(|output| output == &by_user),
        "expected by_user output in compiler-resolved outputs: {output_ids:?}"
    );
    assert!(
        output_ids.iter().any(|output| output == &zero_scores),
        "expected zero_scores output in compiler-resolved outputs: {output_ids:?}"
    );
    let identity = live_standing_program_identity_for_outputs(
        &program_id,
        &response.resolved_spec,
        &[catalog.clone()],
        output_ids,
    );
    let mut runtime = backend
        .create_with_catalogs_and_spec(
            &identity,
            &[catalog.clone()],
            &response.resolved_spec,
            &input_schemas,
            &output_schemas,
        )
        .unwrap();
    let batch = live_scores_complex_rollup_record_batch();
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("live-feldera-program-multi-output-epoch-1").unwrap(),
            vec![RelationInputBatch {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: batch.num_rows() as u64,
                batches: vec![batch],
            }],
        )
        .unwrap();

    let by_user_page = runtime
        .materialized_view_sql_page(
            ScopedViewId {
                tenant_id: identity.tenant_id.clone(),
                program_id: identity.program_id.clone(),
                view_id: by_user.clone(),
            },
            format!("SELECT user_id, total_score FROM \"{by_user}\" ORDER BY user_id"),
            SnapshotPageRequest::default(),
        )
        .unwrap();
    assert!(
        by_user_page.rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "total_score") == Some(12)
        }),
        "expected feldera_program by_user output for u1; rows={:?}",
        by_user_page.rows
    );

    let zero_scores_page = runtime
        .materialized_view_sql_page(
            ScopedViewId {
                tenant_id: identity.tenant_id,
                program_id: identity.program_id,
                view_id: zero_scores.clone(),
            },
            format!("SELECT user_id, score FROM \"{zero_scores}\" ORDER BY user_id"),
            SnapshotPageRequest::default(),
        )
        .unwrap();
    assert!(
        zero_scores_page.rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u3")
                && live_row_i64(row, "score") == Some(0)
        }),
        "expected feldera_program zero_scores output for u3; rows={:?}",
        zero_scores_page.rows
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_pages_materialized_and_sql_queries() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager cursor pagination runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let view_id = live_unique_id("velorix_live_scores_page");
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let compiler_request = FelderaCompileRequestV1 {
        view_id: view_id.clone(),
        sql: "select user_id, sum(score) as sum, count(*) as count from scores group by user_id"
            .to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema.clone()],
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let backend = live_feldera_backend(base_url).with_volatile_runtime_deployment();

    let response = backend
        .compile(FelderaCompilerBackendRequest {
            job_id: "live-feldera-runtime-pagination".to_string(),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog.clone()],
        })
        .await
        .unwrap();
    let output_schemas = response.resolved_spec.output_relations.clone();
    let identity =
        live_standing_program_identity_for(&view_id, &response.resolved_spec, &[catalog.clone()]);
    let mut runtime = backend
        .create_with_catalogs_and_spec(
            &identity,
            &[catalog.clone()],
            &response.resolved_spec,
            &[input_schema],
            &output_schemas,
        )
        .unwrap();
    let batch = live_scores_three_users_record_batch();
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new("live-feldera-runtime-pagination-epoch-1").unwrap(),
            vec![RelationInputBatch {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: batch.num_rows() as u64,
                batches: vec![batch],
            }],
        )
        .unwrap();

    let scoped_view = ScopedViewId {
        tenant_id: identity.tenant_id.clone(),
        program_id: identity.program_id.clone(),
        view_id: view_id.clone(),
    };
    let materialized_first = runtime
        .materialized_view_page(
            scoped_view.clone(),
            SnapshotPageRequest {
                committed_epoch: None,
                page_token: None,
                max_rows: Some(2),
            },
        )
        .unwrap();
    assert_eq!(materialized_first.batches[0].num_rows(), 2);
    assert_eq!(
        materialized_first.next_page_token.as_deref(),
        Some("offset:2")
    );
    let materialized_second = runtime
        .materialized_view_page(
            scoped_view.clone(),
            SnapshotPageRequest {
                committed_epoch: None,
                page_token: Some("offset:2".to_string()),
                max_rows: Some(2),
            },
        )
        .unwrap();
    assert_eq!(materialized_second.batches[0].num_rows(), 1);
    assert!(materialized_second.next_page_token.is_none());

    let sql_first = runtime
        .materialized_view_sql_page(
            scoped_view.clone(),
            format!("SELECT user_id, sum, count FROM \"{view_id}\" ORDER BY user_id"),
            SnapshotPageRequest {
                committed_epoch: None,
                page_token: None,
                max_rows: Some(2),
            },
        )
        .unwrap();
    assert_eq!(sql_first.rows.len(), 2);
    assert_eq!(sql_first.next_page_token.as_deref(), Some("offset:2"));
    assert!(
        sql_first
            .rows
            .iter()
            .any(|row| live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")),
        "expected first SQL page to include u1; rows={:?}",
        sql_first.rows
    );

    let sql_second = runtime
        .materialized_view_sql_page(
            scoped_view,
            format!("SELECT user_id, sum, count FROM \"{view_id}\" ORDER BY user_id"),
            SnapshotPageRequest {
                committed_epoch: None,
                page_token: Some("offset:2".to_string()),
                max_rows: Some(2),
            },
        )
        .unwrap();
    assert_eq!(sql_second.rows.len(), 1);
    assert!(sql_second.next_page_token.is_none());
    assert!(
        sql_second
            .rows
            .iter()
            .any(|row| live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u3")),
        "expected second SQL page to include u3; rows={:?}",
        sql_second.rows
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_deletes_local_volatile_pipeline_on_drop() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager local volatile cleanup test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let view_id = live_unique_id("velorix_live_drop_cleanup");
    let compiler_request = FelderaCompileRequestV1 {
        view_id: view_id.clone(),
        sql: "select user_id, sum(score) as sum, count(*) as count from scores group by user_id"
            .to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema.clone()],
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let backend = live_feldera_backend(base_url.clone()).with_volatile_runtime_deployment();
    let response = backend
        .compile(FelderaCompilerBackendRequest {
            job_id: format!("live-feldera-{view_id}"),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog.clone()],
        })
        .await
        .unwrap();
    let pipeline_name = response
        .runtime_deployment
        .as_ref()
        .expect("volatile backend should return runtime deployment metadata")
        .pipeline_name
        .clone();
    let output_schemas = response.resolved_spec.output_relations.clone();
    let identity =
        live_standing_program_identity_for(&view_id, &response.resolved_spec, &[catalog.clone()]);
    let runtime = backend
        .create_with_catalogs_and_spec(
            &identity,
            &[catalog],
            &response.resolved_spec,
            &[input_schema],
            &output_schemas,
        )
        .unwrap();

    assert!(
        live_feldera_pipeline_exists(&base_url, &pipeline_name).await,
        "expected local volatile pipeline to exist before runtime drop"
    );
    drop(runtime);
    assert!(
        live_feldera_pipeline_deleted(&base_url, &pipeline_name).await,
        "expected local volatile pipeline `{pipeline_name}` to be deleted after runtime drop"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_projection_and_filter() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager projection/filter runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_positive_score_doubles"),
        "select user_id, score * 2 as doubled_score from scores where score >= 7",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "doubled_score") == Some(14)
        }),
        "expected projected/filter output for u1 score 7; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_min_max_avg_aggregates() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager aggregate-family runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_score_aggregate_family"),
        "select user_id, min(score) as min_score, max(score) as max_score, avg(score) as avg_score from scores group by user_id",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "min_score") == Some(5)
                && live_row_i64(row, "max_score") == Some(7)
                && live_row_f64(row, "avg_score").is_some_and(|avg| (avg - 6.0).abs() < 0.0001)
        }),
        "expected min/max/avg aggregate output for u1; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_cte_having_union() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager CTE/HAVING/UNION runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_score_complex_rollup"),
        "with positives as (select user_id, score from scores where score > 0), high_users as (select user_id, sum(score) as total_score from positives group by user_id having sum(score) > 10) select user_id, total_score from high_users union all select user_id, score as total_score from scores where score = 0",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_complex_rollup_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "total_score") == Some(12)
        }),
        "expected CTE/HAVING branch output for u1; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u3")
                && live_row_i64(row, "total_score") == Some(0)
        }),
        "expected UNION ALL zero-score branch output for u3; rows={rows:?}"
    );
    assert!(
        !rows
            .iter()
            .any(|row| live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u2")),
        "u2 should be filtered by HAVING and not matched by UNION zero branch; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_distinct_intersect_except() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager DISTINCT/INTERSECT/EXCEPT runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_score_set_operations"),
        "select 'intersect' as branch, user_id from (select distinct user_id from scores where score >= 5 intersect select user_id from scores where score < 8) as intersected \
         union all \
         select 'except' as branch, user_id from (select distinct user_id from scores where score >= 5 except select user_id from scores where score = 9) as excepted",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_complex_rollup_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "branch").is_some_and(|branch| branch == "intersect")
                && live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
        }),
        "expected INTERSECT result for u1; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "branch").is_some_and(|branch| branch == "except")
                && live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
        }),
        "expected EXCEPT result for u1; rows={rows:?}"
    );
    assert!(
        !rows
            .iter()
            .any(|row| live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u2")),
        "u2 should be removed by EXCEPT and not matched by INTERSECT; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_scalar_string_and_math_functions() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager scalar string/math runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_scalar_string_math"),
        "select user_id, upper(user_id) as upper_user, score * score + abs(score - 10) as score_metric from scores where score = 7",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_string(row, "upper_user").is_some_and(|upper| upper == "U1")
                && live_row_i64(row, "score_metric") == Some(52)
        }),
        "expected scalar string/math expression output for u1; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_string_binary_hash_functions() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager string/binary/hash runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_expanded_scalars_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_string_binary_hash_functions"),
        "select id, \
            lower('Velorix') = 'velorix' as lower_matches, \
            char_length('Velorix') = 7 as char_length_matches, \
            concat('ve', 'lo', 'rix') = 'velorix' as concat_matches, \
            concat_ws('-', 've', 'lo', 'rix') = 've-lo-rix' as concat_ws_matches, \
            left('velorix', 4) = 'velo' as left_matches, \
            right('velorix', 3) = 'rix' as right_matches, \
            substr('velorix', 2, 3) = 'elo' as substr_matches, \
            split_part('ve/lo/rix', '/', 2) = 'lo' as split_part_matches, \
            replace('velorix', 'rix', 'db') = 'velodb' as replace_matches, \
            regexp_replace('abc123', '[0-9]+', '') = 'abc' as regexp_replace_matches, \
            reverse('abc') = 'cba' as reverse_matches, \
            repeat('ab', 3) = 'ababab' as repeat_matches, \
            trim('  velorix  ') = 'velorix' as trim_matches, \
            position('lo' in 'velorix') = 3 as position_matches, \
            ascii('A') = 65 as ascii_matches, \
            chr(65) = 'A' as chr_matches, \
            'Velorix' ilike 'velo%' as ilike_matches, \
            char_length(code) = 4 as code_length_matches, \
            code = 'ABCD' as code_matches, \
            bin2utf8(x'76656C6F') = 'velo' as bin2utf8_matches, \
            to_hex(x'76656C6F') = '76656c6f' as to_hex_matches, \
            octet_length(x'76656C6F') = 4 as octet_length_matches, \
            left(x'76656C6F', 2) = x'7665' as binary_left_matches, \
            right(x'76656C6F', 2) = x'6C6F' as binary_right_matches, \
            md5('velorix') = md5('velorix') as md5_matches, \
            xxhash('velorix', 10) = xxhash('velorix', 10) as xxhash_matches \
         from expanded_scalars where id = 'x1'",
        vec![catalog.clone()],
        vec![input_schema],
        vec![(catalog, live_expanded_scalars_record_batch())],
    )
    .await;

    let row = rows
        .iter()
        .find(|row| live_row_string(row, "id").is_some_and(|id| id == "x1"))
        .unwrap_or_else(|| panic!("expected string/binary/hash function output; rows={rows:?}"));
    for field in [
        "lower_matches",
        "char_length_matches",
        "concat_matches",
        "concat_ws_matches",
        "left_matches",
        "right_matches",
        "substr_matches",
        "split_part_matches",
        "replace_matches",
        "regexp_replace_matches",
        "reverse_matches",
        "repeat_matches",
        "trim_matches",
        "position_matches",
        "ascii_matches",
        "chr_matches",
        "ilike_matches",
        "code_length_matches",
        "code_matches",
        "bin2utf8_matches",
        "to_hex_matches",
        "octet_length_matches",
        "binary_left_matches",
        "binary_right_matches",
        "md5_matches",
        "xxhash_matches",
    ] {
        assert_eq!(
            live_row_bool(row, field),
            Some(true),
            "expected {field} to be true; row={row:?}"
        );
    }
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_floating_numeric_functions() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager floating/numeric runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_expanded_scalars_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_floating_numeric_functions"),
        "select id, \
            abs(cast(i8_value as double) + 8.0e0) < 0.0001e0 as input_i8_matches, \
            i16_value = -16 as input_i16_matches, \
            i32_value = -32 as input_i32_matches, \
            u8_value = 8 as input_u8_matches, \
            u16_value = 16 as input_u16_matches, \
            u32_value = 32 as input_u32_matches, \
            u64_value = 64 as input_u64_matches, \
            abs(cast(f32_value as double) - 3.5e0) < 0.0001e0 as input_f32_matches, \
            raw = x'0A0BFF' as input_raw_matches, \
            bytes = x'DEADBEEF' as input_bytes_matches, \
            event_date = DATE '2026-06-10' as input_date_matches, \
            event_time = TIME '01:02:03' as input_time_matches, \
            event_ts = TIMESTAMP '2026-06-10 01:02:03' as input_ts_matches, \
            uuid_value = '018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa' as input_uuid_matches, \
            amount = 100 as input_amount_matches, \
            abs(acos(1.0e0) - 0.0e0) < 0.0001e0 as acos_matches, \
            abs(acosh(1.0e0) - 0.0e0) < 0.0001e0 as acosh_matches, \
            abs(asin(0.0e0) - 0.0e0) < 0.0001e0 as asin_matches, \
            abs(asinh(0.0e0) - 0.0e0) < 0.0001e0 as asinh_matches, \
            abs(atan(0.0e0) - 0.0e0) < 0.0001e0 as atan_matches, \
            abs(atan2(0.0e0, 1.0e0) - 0.0e0) < 0.0001e0 as atan2_matches, \
            abs(atanh(0.0e0) - 0.0e0) < 0.0001e0 as atanh_matches, \
            abs(cbrt(27.0e0) - 3.0e0) < 0.0001e0 as cbrt_matches, \
            abs(ceil(3.2e0) - 4.0e0) < 0.0001e0 as ceil_matches, \
            abs(floor(3.8e0) - 3.0e0) < 0.0001e0 as floor_matches, \
            abs(degrees(PI) - 180.0e0) < 0.0001e0 as degrees_matches, \
            abs(radians(180.0e0) - PI) < 0.0001e0 as radians_matches, \
            abs(cosh(0.0e0) - 1.0e0) < 0.0001e0 as cosh_matches, \
            abs(cot(PI / 4.0e0) - 1.0e0) < 0.0001e0 as cot_matches, \
            coth(1.0e0) > 1.3e0 and coth(1.0e0) < 1.4e0 as coth_matches, \
            abs(csc(PI / 2.0e0) - 1.0e0) < 0.0001e0 as csc_matches, \
            csch(1.0e0) > 0.85e0 and csch(1.0e0) < 0.86e0 as csch_matches, \
            abs(exp(0.0e0) - 1.0e0) < 0.0001e0 as exp_matches, \
            abs(ln(1.0e0) - 0.0e0) < 0.0001e0 as ln_matches, \
            abs(log(8.0e0, 2.0e0) - 3.0e0) < 0.0001e0 as log_matches, \
            abs(log10(100.0e0) - 2.0e0) < 0.0001e0 as log10_matches, \
            abs(power(2.0e0, 3) - 8.0e0) < 0.0001e0 as power_matches, \
            abs(round(2.6e0) - 3.0e0) < 0.0001e0 as round_matches, \
            abs(round(12.345e0, 2) - 12.35e0) < 0.0001e0 as round_digits_matches, \
            abs(truncate(12.345e0) - 12.0e0) < 0.0001e0 as truncate_matches, \
            div_null(1.0e0, cast(i8_value as double) + 8.0e0) is null as div_null_matches, \
            is_inf(1.0e0 / (cast(i8_value as double) + 8.0e0)) as is_inf_matches, \
            is_nan(sqrt(-1.0e0)) as is_nan_matches, \
            finite_or_null(1.0e0 / (cast(i8_value as double) + 8.0e0)) is null as finite_or_null_matches, \
            abs(sec(0.0e0) - 1.0e0) < 0.0001e0 as sec_matches, \
            abs(sech(0.0e0) - 1.0e0) < 0.0001e0 as sech_matches, \
            abs(sin(0.0e0) - 0.0e0) < 0.0001e0 as sin_matches, \
            abs(sinh(0.0e0) - 0.0e0) < 0.0001e0 as sinh_matches, \
            abs(cos(0.0e0) - 1.0e0) < 0.0001e0 as cos_matches, \
            abs(sqrt(4.0e0) - 2.0e0) < 0.0001e0 as sqrt_matches, \
            abs(tan(0.0e0) - 0.0e0) < 0.0001e0 as tan_matches, \
            abs(tanh(0.0e0) - 0.0e0) < 0.0001e0 as tanh_matches \
         from expanded_scalars where id = 'x1'",
        vec![catalog.clone()],
        vec![input_schema],
        vec![(catalog, live_expanded_scalars_record_batch())],
    )
    .await;

    let row = rows
        .iter()
        .find(|row| live_row_string(row, "id").is_some_and(|id| id == "x1"))
        .unwrap_or_else(|| panic!("expected floating/numeric function output; rows={rows:?}"));
    for field in [
        "input_i8_matches",
        "input_i16_matches",
        "input_i32_matches",
        "input_u8_matches",
        "input_u16_matches",
        "input_u32_matches",
        "input_u64_matches",
        "input_f32_matches",
        "input_raw_matches",
        "input_bytes_matches",
        "input_date_matches",
        "input_time_matches",
        "input_ts_matches",
        "input_uuid_matches",
        "input_amount_matches",
        "acos_matches",
        "acosh_matches",
        "asin_matches",
        "asinh_matches",
        "atan_matches",
        "atan2_matches",
        "atanh_matches",
        "cbrt_matches",
        "ceil_matches",
        "floor_matches",
        "degrees_matches",
        "radians_matches",
        "cosh_matches",
        "cot_matches",
        "coth_matches",
        "csc_matches",
        "csch_matches",
        "exp_matches",
        "ln_matches",
        "log_matches",
        "log10_matches",
        "power_matches",
        "round_matches",
        "round_digits_matches",
        "truncate_matches",
        "div_null_matches",
        "is_inf_matches",
        "is_nan_matches",
        "finite_or_null_matches",
        "sec_matches",
        "sech_matches",
        "sin_matches",
        "sinh_matches",
        "cos_matches",
        "sqrt_matches",
        "tan_matches",
        "tanh_matches",
    ] {
        assert_eq!(
            live_row_bool(row, field),
            Some(true),
            "expected {field} to be true; row={row:?}"
        );
    }
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_computed_grouping_expressions() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager computed grouping runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_computed_score_bands"),
        "select case when score >= 7 then 'high' else 'low' end as score_band, count(*) as event_count, sum(score) as total_score from scores group by case when score >= 7 then 'high' else 'low' end",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_complex_rollup_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "score_band").is_some_and(|band| band == "high")
                && live_row_i64(row, "event_count") == Some(2)
                && live_row_i64(row, "total_score") == Some(16)
        }),
        "expected computed grouping high band output; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "score_band").is_some_and(|band| band == "low")
                && live_row_i64(row, "event_count") == Some(2)
                && live_row_i64(row, "total_score") == Some(5)
        }),
        "expected computed grouping low band output; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_lateral_column_aliasing() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager lateral column aliasing runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_lateral_column_aliasing"),
        "select score + 1 as bucket, bucket * 2 as doubled_bucket, count(*) as event_count \
         from scores \
         group by bucket \
         having bucket > 5",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_complex_rollup_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_i64(row, "bucket") == Some(8)
                && live_row_i64(row, "doubled_bucket") == Some(16)
                && live_row_i64(row, "event_count") == Some(1)
        }),
        "expected lateral column alias to feed SELECT, GROUP BY, and HAVING; rows={rows:?}"
    );
    assert!(
        !rows
            .iter()
            .any(|row| live_row_i64(row, "bucket") == Some(1)),
        "HAVING should filter the zero-score bucket through the lateral alias; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_between_in_and_like_predicates() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager predicate-family runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_predicate_family"),
        "select user_id, score from scores where score between 5 and 9 and score in (5, 7, 9) and user_id like 'u%'",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_complex_rollup_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score") == Some(5)
        }),
        "expected predicate family output for u1 score 5; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score") == Some(7)
        }),
        "expected predicate family output for u1 score 7; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u2")
                && live_row_i64(row, "score") == Some(9)
        }),
        "expected predicate family output for u2 score 9; rows={rows:?}"
    );
    assert!(
        !rows
            .iter()
            .any(|row| live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u3")),
        "u3 score 0 should be filtered by predicate family; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_distinct_aggregates() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager DISTINCT aggregate runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_distinct_aggregates"),
        "select count(distinct user_id) as unique_users, count(distinct score) as unique_scores from scores",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_complex_rollup_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_i64(row, "unique_users") == Some(3)
                && live_row_i64(row, "unique_scores") == Some(4)
        }),
        "expected DISTINCT aggregate counts; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_advanced_aggregates() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager advanced aggregate runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_advanced_aggregates"),
        "select \
             count(*) as total_events, \
             count(*) filter (where score > 0) as positive_events, \
             countif(score = 0) as zero_events, \
             sum(score) filter (where user_id = 'u1') as u1_score, \
             arg_max(user_id, score) as top_user, \
             arg_min(user_id, score) as low_user, \
             bit_and(score) as bit_and_score, \
             bit_or(score) as bit_or_score, \
             bit_xor(score) as bit_xor_score, \
             bool_and(score >= 0) as all_non_negative, \
             bool_or(score = 0) as has_zero, \
             logical_and(score < 10) as all_below_ten, \
             logical_or(score = 9) as has_nine, \
             every(score >= 0) as every_non_negative, \
             some(score = 9) as some_nine \
         from scores",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_complex_rollup_record_batch())],
    )
    .await;

    assert_eq!(
        rows.len(),
        1,
        "expected one aggregate output row; rows={rows:?}"
    );
    let row = rows.first().unwrap();
    assert_eq!(live_row_i64(row, "total_events"), Some(4));
    assert_eq!(live_row_i64(row, "positive_events"), Some(3));
    assert_eq!(live_row_i64(row, "zero_events"), Some(1));
    assert_eq!(live_row_i64(row, "u1_score"), Some(12));
    assert_eq!(live_row_string(row, "top_user").as_deref(), Some("u2"));
    assert_eq!(live_row_string(row, "low_user").as_deref(), Some("u3"));
    assert_eq!(live_row_i64(row, "bit_and_score"), Some(0));
    assert_eq!(live_row_i64(row, "bit_or_score"), Some(15));
    assert_eq!(live_row_i64(row, "bit_xor_score"), Some(11));
    assert_eq!(live_row_bool(row, "all_non_negative"), Some(true));
    assert_eq!(live_row_bool(row, "has_zero"), Some(true));
    assert_eq!(live_row_bool(row, "all_below_ten"), Some(true));
    assert_eq!(live_row_bool(row, "has_nine"), Some(true));
    assert_eq!(live_row_bool(row, "every_non_negative"), Some(true));
    assert_eq!(live_row_bool(row, "some_nine"), Some(true));
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_pivot_aggregates() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager PIVOT runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_pivot_aggregates"),
        "select * from (select user_id, score from scores) \
         pivot (sum(score) as total for user_id in ('u1' as u1, 'u2' as u2, 'u3' as u3))",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_complex_rollup_record_batch())],
    )
    .await;

    assert_eq!(
        rows.len(),
        1,
        "expected one PIVOT output row; rows={rows:?}"
    );
    let row = rows.first().unwrap();
    assert_eq!(live_row_i64(row, "u1_total"), Some(12));
    assert_eq!(live_row_i64(row, "u2_total"), Some(9));
    assert_eq!(live_row_i64(row, "u3_total"), Some(0));
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_unpivot_and_join_using() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager UNPIVOT/JOIN USING runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let scores = live_scores_relation_catalog();
    let profiles = live_profiles_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_unpivot_join_using"),
        "with wide_scores as ( \
             select user_id, score as first_score, score + 1 as next_score \
             from scores \
             where event_id = 'e2' \
         ) \
         select 'using' as branch, user_id, score as value, tier as label \
         from scores join profiles using (user_id) \
         where event_id = 'e2' \
         union all \
         select 'unpivot' as branch, user_id, score_value as value, score_kind as label \
         from wide_scores \
         unpivot (score_value for score_kind in (first_score as 'first', next_score as 'next'))",
        vec![scores.clone(), profiles.clone()],
        vec![
            live_scores_input_schema(&scores),
            live_profiles_input_schema(&profiles),
        ],
        vec![
            (scores, live_scores_record_batch()),
            (profiles, live_profiles_record_batch()),
        ],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "branch").is_some_and(|branch| branch == "using")
                && live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "value") == Some(7)
                && live_row_string(row, "label").is_some_and(|label| label == "gold")
        }),
        "expected JOIN USING output for u1 profile tier; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "branch").is_some_and(|branch| branch == "unpivot")
                && live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "value") == Some(7)
                && live_row_string(row, "label").is_some_and(|label| label == "first")
        }),
        "expected first UNPIVOT output row; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "branch").is_some_and(|branch| branch == "unpivot")
                && live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "value") == Some(8)
                && live_row_string(row, "label").is_some_and(|label| label == "next")
        }),
        "expected second UNPIVOT output row; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_window_row_number() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager window function runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_window_score_rank"),
        "select user_id, score, row_number() over (partition by user_id order by score desc) as score_rank from scores",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_complex_rollup_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score") == Some(7)
                && live_row_i64(row, "score_rank") == Some(1)
        }),
        "expected window row_number rank 1 for u1 score 7; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score") == Some(5)
                && live_row_i64(row, "score_rank") == Some(2)
        }),
        "expected window row_number rank 2 for u1 score 5; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_scalar_subqueries() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager scalar subquery runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_scalar_subqueries"),
        "select user_id, score, \
            (select max(score) from scores) as global_max_score \
         from scores \
         where score > (select avg(score) from scores)",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_complex_rollup_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score") == Some(7)
                && live_row_i64(row, "global_max_score") == Some(9)
        }),
        "expected scalar subquery filtered output for u1 score 7; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u2")
                && live_row_i64(row, "score") == Some(9)
                && live_row_i64(row, "global_max_score") == Some(9)
        }),
        "expected scalar subquery filtered output for u2 score 9; rows={rows:?}"
    );
    assert!(
        !rows
            .iter()
            .any(|row| live_row_i64(row, "score").is_some_and(|score| score <= 5)),
        "scores at or below the scalar AVG subquery should be filtered; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_window_aggregates() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager window aggregate runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_window_aggregates"),
        "select user_id, score, \
            sum(score) over (partition by user_id order by score range between unbounded preceding and current row) as running_score, \
            count(*) over (partition by user_id order by score range between unbounded preceding and current row) as running_count \
         from scores",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_complex_rollup_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score") == Some(5)
                && live_row_i64(row, "running_score") == Some(5)
                && live_row_i64(row, "running_count") == Some(1)
        }),
        "expected first u1 window aggregate row; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score") == Some(7)
                && live_row_i64(row, "running_score") == Some(12)
                && live_row_i64(row, "running_count") == Some(2)
        }),
        "expected second u1 window aggregate row; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_lambda_array_functions() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager lambda/array runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_lambda_array_functions"),
        "select user_id, score, \
            array_exists(array[score, score - 10], x -> x > 6) as has_large_element, \
            array_length(array_compact(array[score, cast(null as bigint), score + 1])) as compact_len \
         from scores \
         where event_id = 'e2'",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score") == Some(7)
                && live_row_bool(row, "has_large_element") == Some(true)
                && live_row_i64(row, "compact_len") == Some(2)
        }),
        "expected lambda/array function output for e2; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_interval_datetime_operations() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager interval/date-time operation runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_interval_datetime_ops"),
        "select user_id, score, \
            timestampdiff(day, timestamp '2026-06-01 00:00:00', timestampadd(day, score, timestamp '2026-06-01 00:00:00')) as shifted_days, \
            extract(day from interval '1 02:03:04.005' day to second) as interval_day_part, \
            extract(hour from interval '1 02:03:04.005' day to second) as interval_hour_part, \
            extract(year from interval '2-03' year to month) as interval_year_part, \
            extract(month from interval '2-03' year to month) as interval_month_part \
         from scores \
         where event_id = 'e2'",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_record_batch())],
    )
    .await;

    let row = rows
        .iter()
        .find(|row| live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1"))
        .unwrap_or_else(|| panic!("expected interval/date-time Feldera SQL output; rows={rows:?}"));
    assert_eq!(live_row_i64(row, "score"), Some(7));
    assert_eq!(live_row_i64(row, "shifted_days"), Some(7));
    assert_eq!(live_row_i64(row, "interval_day_part"), Some(1));
    assert_eq!(live_row_i64(row, "interval_hour_part"), Some(2));
    assert_eq!(live_row_i64(row, "interval_year_part"), Some(2));
    assert_eq!(live_row_i64(row, "interval_month_part"), Some(3));
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_select_replace_exclude_values_unnest() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager SELECT REPLACE/EXCLUDE, VALUES, and UNNEST runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_select_replace_exclude_values_unnest"),
        "with replaced as ( \
             select * replace (score + 10 as score) \
             from scores \
             where event_id = 'e2' \
         ), \
         excluded as ( \
             select * exclude (event_id) from replaced \
         ), \
         bonus(user_id, extra_score) as ( \
             values ('u1', 3), ('u2', 4) \
         ) \
         select excluded.user_id, expanded.score_value, bonus.extra_score \
         from excluded \
         join bonus on excluded.user_id = bonus.user_id \
         cross join unnest(array[excluded.score, excluded.score + bonus.extra_score]) as expanded(score_value)",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score_value") == Some(17)
                && live_row_i64(row, "extra_score") == Some(3)
        }),
        "expected first UNNEST row from replaced score; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score_value") == Some(20)
                && live_row_i64(row, "extra_score") == Some(3)
        }),
        "expected second UNNEST row from replaced score plus VALUES bonus; rows={rows:?}"
    );
    assert!(
        rows.iter().all(|row| row.get("event_id").is_none()),
        "SELECT * EXCLUDE should remove event_id from downstream projection; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_qualify_and_lateral_apply() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager QUALIFY and LATERAL/APPLY runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_qualify_lateral_apply"),
        "select ranked.user_id, ranked.score, ranked.score_rank, derived.bumped_score \
         from ( \
             select user_id, score, event_id, \
                 row_number() over (partition by user_id order by score desc) as score_rank \
             from scores \
             qualify row_number() over (partition by user_id order by score desc) = 1 \
         ) as ranked \
         cross apply (select ranked.score + 1 as bumped_score) as derived \
         where ranked.event_id = 'e2'",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_complex_rollup_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score") == Some(7)
                && live_row_i64(row, "score_rank") == Some(1)
                && live_row_i64(row, "bumped_score") == Some(8)
        }),
        "expected QUALIFY to retain the top u1 row and CROSS APPLY to derive bumped_score; rows={rows:?}"
    );
    assert!(
        !rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score") == Some(5)
        }),
        "QUALIFY should filter the lower-ranked u1 row; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_rollup_and_cube_grouping() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager ROLLUP/CUBE runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_rollup_cube_grouping"),
        "with classified as ( \
             select user_id, score, case when score > 0 then 'positive' else 'zero' end as score_band \
             from scores \
         ) \
         select 'rollup' as grouping_kind, coalesce(user_id, 'all') as bucket, count(*) as event_count, sum(score) as total_score \
         from classified \
         group by rollup(user_id) \
         union all \
         select 'cube' as grouping_kind, coalesce(score_band, 'all') as bucket, count(*) as event_count, sum(score) as total_score \
         from classified \
         group by cube(score_band)",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_complex_rollup_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "grouping_kind").is_some_and(|kind| kind == "rollup")
                && live_row_string(row, "bucket").is_some_and(|bucket| bucket == "all")
                && live_row_i64(row, "event_count") == Some(4)
                && live_row_i64(row, "total_score") == Some(21)
        }),
        "expected ROLLUP grand total row; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "grouping_kind").is_some_and(|kind| kind == "cube")
                && live_row_string(row, "bucket").is_some_and(|bucket| bucket == "positive")
                && live_row_i64(row, "event_count") == Some(3)
                && live_row_i64(row, "total_score") == Some(21)
        }),
        "expected CUBE positive bucket row; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "grouping_kind").is_some_and(|kind| kind == "cube")
                && live_row_string(row, "bucket").is_some_and(|bucket| bucket == "zero")
                && live_row_i64(row, "event_count") == Some(1)
                && live_row_i64(row, "total_score") == Some(0)
        }),
        "expected CUBE zero bucket row; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_sql_udf_programs() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager SQL UDF program runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let program_id = live_unique_id("velorix_live_sql_udf_program");
    let output_id = live_unique_id("velorix_live_sql_udf_output");
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let input_schemas = vec![input_schema];
    let compiler_request = FelderaCompileRequestV1 {
        view_id: program_id.clone(),
        sql: format!(
            "CREATE FUNCTION add_bonus(points BIGINT) RETURNS BIGINT AS points + 5; \
             CREATE MATERIALIZED VIEW {output_id} AS \
             SELECT user_id, add_bonus(score) AS boosted_score FROM scores WHERE event_id = 'e2'"
        ),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::FelderaProgram,
        rust_extension: Default::default(),
        input_relations: input_schemas.clone(),
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: true,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let backend = live_feldera_backend(base_url).with_volatile_runtime_deployment();

    let response = backend
        .compile(FelderaCompilerBackendRequest {
            job_id: format!("live-feldera-sql-udf-{program_id}"),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog.clone()],
        })
        .await
        .unwrap();
    let output_schemas = response.resolved_spec.output_relations.clone();
    let output_ids = output_schemas
        .iter()
        .map(|schema| schema.relation_id.clone())
        .collect::<Vec<_>>();
    assert!(
        output_ids.iter().any(|output| output == &output_id),
        "expected SQL UDF program output in compiler-resolved outputs: {output_ids:?}"
    );
    let identity = live_standing_program_identity_for_outputs(
        &program_id,
        &response.resolved_spec,
        &[catalog.clone()],
        output_ids,
    );
    let mut runtime = backend
        .create_with_catalogs_and_spec(
            &identity,
            &[catalog.clone()],
            &response.resolved_spec,
            &input_schemas,
            &output_schemas,
        )
        .unwrap();
    let batch = live_scores_record_batch();
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new(format!("live-feldera-sql-udf-{program_id}-epoch-1")).unwrap(),
            vec![RelationInputBatch {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: batch.num_rows() as u64,
                batches: vec![batch],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_sql_page(
            ScopedViewId {
                tenant_id: identity.tenant_id,
                program_id: identity.program_id,
                view_id: output_id.clone(),
            },
            format!("SELECT user_id, boosted_score FROM \"{output_id}\""),
            SnapshotPageRequest::default(),
        )
        .unwrap();
    assert!(
        page.rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "boosted_score") == Some(12)
        }),
        "expected SQL UDF output to apply add_bonus; rows={:?}",
        page.rows
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_rust_user_defined_aggregates() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager Rust UDA runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let program_id = live_unique_id("velorix_live_rust_uda_program");
    let output_id = live_unique_id("velorix_live_rust_uda_output");
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let input_schemas = vec![input_schema];
    let compiler_request = FelderaCompileRequestV1 {
        view_id: program_id.clone(),
        sql: format!(
            "CREATE LINEAR AGGREGATE signed_sum(value BIGINT) RETURNS BIGINT; \
             CREATE MATERIALIZED VIEW {output_id} AS \
             SELECT user_id, signed_sum(score) AS total_score \
             FROM scores GROUP BY user_id"
        ),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::FelderaProgram,
        rust_extension: FelderaRustExtensionV1 {
            udf_rust: Some(
                "\
use feldera_sqllib::*;
pub type signed_sum_accumulator_type = i64;
pub fn signed_sum_map(value: i64) -> signed_sum_accumulator_type { value }
pub fn signed_sum_post(value: signed_sum_accumulator_type) -> i64 { value }
"
                .to_string(),
            ),
            udf_toml: None,
        },
        input_relations: input_schemas.clone(),
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: true,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let backend = live_feldera_backend(base_url).with_volatile_runtime_deployment();

    let response = backend
        .compile(FelderaCompilerBackendRequest {
            job_id: format!("live-feldera-rust-uda-{program_id}"),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog.clone()],
        })
        .await
        .unwrap();
    let output_schemas = response.resolved_spec.output_relations.clone();
    let output_ids = output_schemas
        .iter()
        .map(|schema| schema.relation_id.clone())
        .collect::<Vec<_>>();
    assert!(
        output_ids.iter().any(|output| output == &output_id),
        "expected Rust UDA program output in compiler-resolved outputs: {output_ids:?}"
    );
    let identity = live_standing_program_identity_for_outputs(
        &program_id,
        &response.resolved_spec,
        &[catalog.clone()],
        output_ids,
    );
    let mut runtime = backend
        .create_with_catalogs_and_spec(
            &identity,
            &[catalog.clone()],
            &response.resolved_spec,
            &input_schemas,
            &output_schemas,
        )
        .unwrap();
    let batch = live_scores_record_batch();
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new(format!("live-feldera-rust-uda-{program_id}-epoch-1"))
                .unwrap(),
            vec![RelationInputBatch {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: batch.num_rows() as u64,
                batches: vec![batch],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_sql_page(
            ScopedViewId {
                tenant_id: identity.tenant_id,
                program_id: identity.program_id,
                view_id: output_id.clone(),
            },
            format!("SELECT user_id, total_score FROM \"{output_id}\""),
            SnapshotPageRequest::default(),
        )
        .unwrap();
    assert!(
        page.rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "total_score") == Some(12)
        }),
        "expected Rust UDA signed_sum to aggregate u1 scores; rows={:?}",
        page.rows
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_user_defined_types_and_indexes() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager user-defined type/index runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let program_id = live_unique_id("velorix_live_udt_index_program");
    let output_id = live_unique_id("velorix_live_udt_index_output");
    let type_id = live_unique_id("velorix_live_score_event_type");
    let index_id = live_unique_id("velorix_live_udt_index_output_by_event");
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let input_schemas = vec![input_schema];
    let compiler_request = FelderaCompileRequestV1 {
        view_id: program_id.clone(),
        sql: format!(
            "CREATE TYPE {type_id} AS (owner VARCHAR, points BIGINT); \
             CREATE MATERIALIZED VIEW {output_id} AS \
             SELECT event_id, CAST(ROW(user_id, score) AS {type_id}) AS typed_score \
             FROM scores WHERE event_id = 'e2'; \
             CREATE INDEX {index_id} ON {output_id}(event_id)"
        ),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::FelderaProgram,
        rust_extension: Default::default(),
        input_relations: input_schemas.clone(),
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: true,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let backend = live_feldera_backend(base_url).with_volatile_runtime_deployment();

    let response = backend
        .compile(FelderaCompilerBackendRequest {
            job_id: format!("live-feldera-udt-index-{program_id}"),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog.clone()],
        })
        .await
        .unwrap();
    let output_schemas = response.resolved_spec.output_relations.clone();
    let output_ids = output_schemas
        .iter()
        .map(|schema| schema.relation_id.clone())
        .collect::<Vec<_>>();
    assert!(
        output_ids.iter().any(|output| output == &output_id),
        "expected user-defined type/index program output in compiler-resolved outputs: {output_ids:?}"
    );
    let identity = live_standing_program_identity_for_outputs(
        &program_id,
        &response.resolved_spec,
        &[catalog.clone()],
        output_ids,
    );
    let mut runtime = backend
        .create_with_catalogs_and_spec(
            &identity,
            &[catalog.clone()],
            &response.resolved_spec,
            &input_schemas,
            &output_schemas,
        )
        .unwrap();
    let batch = live_scores_record_batch();
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new(format!("live-feldera-udt-index-{program_id}-epoch-1"))
                .unwrap(),
            vec![RelationInputBatch {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: batch.num_rows() as u64,
                batches: vec![batch],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_sql_page(
            ScopedViewId {
                tenant_id: identity.tenant_id,
                program_id: identity.program_id,
                view_id: output_id.clone(),
            },
            format!("SELECT event_id, typed_score FROM \"{output_id}\""),
            SnapshotPageRequest::default(),
        )
        .unwrap();
    let row = page
        .rows
        .iter()
        .find(|row| live_row_string(row, "event_id").is_some_and(|event_id| event_id == "e2"))
        .unwrap_or_else(|| {
            panic!(
                "expected user-defined typed output row through indexed view; rows={:?}",
                page.rows
            )
        });
    assert_eq!(
        row["typed_score"],
        json!({
            "owner": "u1",
            "points": 7
        })
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_recursive_views() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager recursive view runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let program_id = live_unique_id("velorix_live_recursive_program");
    let output_id = live_unique_id("velorix_live_recursive_closure");
    let catalog = live_edges_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let input_schemas = vec![input_schema];
    let compiler_request = FelderaCompileRequestV1 {
        view_id: program_id.clone(),
        sql: format!(
            "DECLARE RECURSIVE VIEW {output_id}(src BIGINT, dst BIGINT); \
             CREATE LOCAL VIEW step AS \
             SELECT e.src, c.dst \
             FROM edges AS e \
             JOIN {output_id} AS c ON e.dst = c.src; \
             CREATE MATERIALIZED VIEW {output_id} AS \
             (SELECT src, dst FROM edges) \
             UNION \
             (SELECT src, dst FROM step)"
        ),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::FelderaProgram,
        rust_extension: Default::default(),
        input_relations: input_schemas.clone(),
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: true,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let backend = live_feldera_backend(base_url).with_volatile_runtime_deployment();

    let response = backend
        .compile(FelderaCompilerBackendRequest {
            job_id: format!("live-feldera-recursive-{program_id}"),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog.clone()],
        })
        .await
        .unwrap();
    let output_schemas = response.resolved_spec.output_relations.clone();
    let output_ids = output_schemas
        .iter()
        .map(|schema| schema.relation_id.clone())
        .collect::<Vec<_>>();
    assert!(
        output_ids.iter().any(|output| output == &output_id),
        "expected recursive materialized view output in compiler-resolved outputs: {output_ids:?}"
    );
    let identity = live_standing_program_identity_for_outputs(
        &program_id,
        &response.resolved_spec,
        &[catalog.clone()],
        output_ids,
    );
    let mut runtime = backend
        .create_with_catalogs_and_spec(
            &identity,
            &[catalog.clone()],
            &response.resolved_spec,
            &input_schemas,
            &output_schemas,
        )
        .unwrap();
    let batch = live_edges_record_batch();
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new(format!("live-feldera-recursive-{program_id}-epoch-1"))
                .unwrap(),
            vec![RelationInputBatch {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                start_offset_inclusive: 0,
                end_offset_exclusive: batch.num_rows() as u64,
                batches: vec![batch],
            }],
        )
        .unwrap();

    let page = runtime
        .materialized_view_sql_page(
            ScopedViewId {
                tenant_id: identity.tenant_id,
                program_id: identity.program_id,
                view_id: output_id.clone(),
            },
            format!("SELECT src, dst FROM \"{output_id}\" ORDER BY src, dst"),
            SnapshotPageRequest::default(),
        )
        .unwrap();
    for (src, dst) in [(1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4)] {
        assert!(
            page.rows.iter().any(|row| {
                live_row_i64(row, "src") == Some(src) && live_row_i64(row, "dst") == Some(dst)
            }),
            "expected recursive transitive closure edge ({src}, {dst}); rows={:?}",
            page.rows
        );
    }
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_asof_join() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager ASOF JOIN runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let events = live_expanded_scalars_relation_catalog();
    let rates = live_rates_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_asof_join"),
        "select e.id, e.event_ts, e.amount, r.rate_id, r.rate_value \
         from expanded_scalars as e \
         left asof join rates as r \
         match_condition (e.event_ts >= r.effective_ts) \
         on e.id = r.entity_id \
         where e.id = 'x1'",
        vec![events.clone(), rates.clone()],
        vec![
            catalog_input_relation_schema(&events).unwrap(),
            catalog_input_relation_schema(&rates).unwrap(),
        ],
        vec![
            (events, live_expanded_scalars_record_batch()),
            (rates, live_rates_record_batch()),
        ],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "id").is_some_and(|id| id == "x1")
                && live_row_string(row, "rate_id").is_some_and(|rate_id| rate_id == "r2")
                && live_row_i64(row, "amount") == Some(100)
                && live_row_i64(row, "rate_value") == Some(12)
        }),
        "expected ASOF JOIN to choose the latest matching rate r2; rows={rows:?}"
    );
    assert!(
        !rows.iter().any(|row| {
            live_row_string(row, "id").is_some_and(|id| id == "x1")
                && live_row_string(row, "rate_id").is_some_and(|rate_id| rate_id == "r1")
        }),
        "ASOF JOIN should not choose older rate r1 when r2 is closer; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_tumble_and_hop_table_functions() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager TUMBLE/HOP table-function runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_expanded_scalars_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_table_window_functions"),
        "select 'tumble' as window_kind, id, amount, window_start, window_end \
         from table(tumble(table expanded_scalars, descriptor(event_ts), interval '1' hour)) \
         where id = 'x1' \
         union all \
         select 'hop' as window_kind, id, amount, window_start, window_end \
         from table(hop(table expanded_scalars, descriptor(event_ts), interval '30' minute, interval '1' hour)) \
         where id = 'x1'",
        vec![catalog.clone()],
        vec![catalog_input_relation_schema(&catalog).unwrap()],
        vec![(catalog, live_expanded_scalars_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "window_kind").is_some_and(|kind| kind == "tumble")
                && live_row_string(row, "id").is_some_and(|id| id == "x1")
                && live_row_i64(row, "amount") == Some(100)
        }),
        "expected TUMBLE table function row; rows={rows:?}"
    );
    let hop_rows = rows
        .iter()
        .filter(|row| {
            live_row_string(row, "window_kind").is_some_and(|kind| kind == "hop")
                && live_row_string(row, "id").is_some_and(|id| id == "x1")
        })
        .count();
    assert!(
        hop_rows >= 1,
        "expected at least one HOP table function row for x1; rows={rows:?}"
    );
    assert!(
        rows.iter()
            .all(|row| row.get("window_start").is_some() && row.get("window_end").is_some()),
        "TUMBLE/HOP rows should include window_start/window_end; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_expanded_scalar_functions() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager expanded scalar function runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_expanded_scalars_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_expanded_scalar_functions"),
        "select id, \
            abs(i8_value) as abs_i8, \
            cast(u16_value as bigint) + amount as widened_total, \
            ceil(cast(f32_value as double)) as ceil_f32, \
            raw = x'0A0BFF' as raw_matches, \
            bytes = x'DEADBEEF' as bytes_matches, \
            event_date = DATE '2026-06-10' as date_matches, \
            event_time = TIME '01:02:03' as time_matches, \
            event_ts = TIMESTAMP '2026-06-10 01:02:03' as ts_matches \
         from expanded_scalars where id = 'x1'",
        vec![catalog.clone()],
        vec![input_schema],
        vec![(catalog, live_expanded_scalars_record_batch())],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "id").is_some_and(|id| id == "x1")
                && live_row_i64(row, "abs_i8") == Some(8)
                && live_row_i64(row, "widened_total") == Some(116)
                && live_row_f64(row, "ceil_f32").is_some_and(|value| (value - 4.0).abs() < 0.0001)
                && live_row_bool(row, "raw_matches") == Some(true)
                && live_row_bool(row, "bytes_matches") == Some(true)
                && live_row_bool(row, "date_matches") == Some(true)
                && live_row_bool(row, "time_matches") == Some(true)
                && live_row_bool(row, "ts_matches") == Some(true)
        }),
        "expected expanded scalar function output; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_two_table_join() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager join runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let scores = live_scores_relation_catalog();
    let profiles = live_profiles_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_scores_by_tier"),
        "select p.tier, sum(s.score) as total_score, count(*) as event_count from scores s join profiles p on s.user_id = p.user_id group by p.tier",
        vec![scores.clone(), profiles.clone()],
        vec![
            live_scores_input_schema(&scores),
            live_profiles_input_schema(&profiles),
        ],
        vec![
            (scores, live_scores_record_batch()),
            (profiles, live_profiles_record_batch()),
        ],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "tier").is_some_and(|tier| tier == "gold")
                && live_row_i64(row, "total_score") == Some(12)
                && live_row_i64(row, "event_count") == Some(2)
        }),
        "expected two-table join aggregate output for gold tier; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_left_outer_join() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager LEFT JOIN runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let scores = live_scores_relation_catalog();
    let profiles = live_profiles_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_scores_with_optional_tier"),
        "select s.user_id, s.score, p.tier from scores s left join profiles p on s.user_id = p.user_id where s.score = 13 or s.score = 5",
        vec![scores.clone(), profiles.clone()],
        vec![
            live_scores_input_schema(&scores),
            live_profiles_input_schema(&profiles),
        ],
        vec![
            (scores, live_scores_three_users_record_batch()),
            (profiles, live_profiles_record_batch()),
        ],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score") == Some(5)
                && live_row_string(row, "tier").is_some_and(|tier| tier == "gold")
        }),
        "expected LEFT JOIN matched output for u1; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u3")
                && live_row_i64(row, "score") == Some(13)
                && live_row_is_null(row, "tier")
        }),
        "expected LEFT JOIN null-preserving output for u3; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_right_and_full_outer_join() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager RIGHT/FULL OUTER JOIN runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let scores = live_scores_relation_catalog();
    let profiles = live_profiles_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_scores_right_full_join"),
        "select 'right' as join_kind, p.user_id as user_id, s.score as score, p.tier as tier \
         from scores s right join profiles p on s.user_id = p.user_id \
         where p.user_id = 'u4' \
         union all \
         select 'full' as join_kind, coalesce(s.user_id, p.user_id) as user_id, s.score as score, p.tier as tier \
         from scores s full outer join profiles p on s.user_id = p.user_id \
         where s.user_id = 'u3' or p.user_id = 'u4'",
        vec![scores.clone(), profiles.clone()],
        vec![
            live_scores_input_schema(&scores),
            live_profiles_input_schema(&profiles),
        ],
        vec![
            (scores, live_scores_three_users_record_batch()),
            (profiles, live_profiles_with_unmatched_record_batch()),
        ],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "join_kind").is_some_and(|join_kind| join_kind == "right")
                && live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u4")
                && live_row_is_null(row, "score")
                && live_row_string(row, "tier").is_some_and(|tier| tier == "platinum")
        }),
        "expected RIGHT JOIN null-preserving output for unmatched profile u4; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "join_kind").is_some_and(|join_kind| join_kind == "full")
                && live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u3")
                && live_row_i64(row, "score") == Some(13)
                && live_row_is_null(row, "tier")
        }),
        "expected FULL OUTER JOIN null-preserving output for unmatched score u3; rows={rows:?}"
    );
    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "join_kind").is_some_and(|join_kind| join_kind == "full")
                && live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u4")
                && live_row_is_null(row, "score")
                && live_row_string(row, "tier").is_some_and(|tier| tier == "platinum")
        }),
        "expected FULL OUTER JOIN null-preserving output for unmatched profile u4; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_correlated_exists_subquery() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager correlated EXISTS runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let scores = live_scores_relation_catalog();
    let profiles = live_profiles_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_scores_with_profile_exists"),
        "select s.user_id, s.score from scores s where exists (select 1 from profiles p where p.user_id = s.user_id and p.tier = 'gold')",
        vec![scores.clone(), profiles.clone()],
        vec![
            live_scores_input_schema(&scores),
            live_profiles_input_schema(&profiles),
        ],
        vec![
            (scores, live_scores_three_users_record_batch()),
            (profiles, live_profiles_record_batch()),
        ],
    )
    .await;

    assert!(
        rows.iter().any(|row| {
            live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1")
                && live_row_i64(row, "score") == Some(5)
        }),
        "expected correlated EXISTS output for gold user u1; rows={rows:?}"
    );
    assert!(
        !rows
            .iter()
            .any(|row| live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u2")),
        "u2 should be filtered by correlated EXISTS tier predicate; rows={rows:?}"
    );
    assert!(
        !rows
            .iter()
            .any(|row| live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u3")),
        "u3 should be filtered because no profile exists; rows={rows:?}"
    );
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_complex_feldera_sql_result_types() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager complex SQL result-type runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_complex_feldera_types"),
        "select user_id, \
            ARRAY[score, score + 1, CAST(NULL AS BIGINT)] as score_window, \
            CAST(ROW(user_id, score) AS ROW(owner VARCHAR, points BIGINT)) as score_row, \
            CAST(score AS VARIANT) as score_variant, \
            CAST('018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa' AS UUID) as sample_uuid, \
            x'0A0BFF' as raw_bytes, \
            CASE WHEN score >= 7 THEN 'high' ELSE 'low' END as score_band, \
            COALESCE(CAST(NULL AS BIGINT), score) as fallback_score \
         from scores where score = 7",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_record_batch())],
    )
    .await;

    let row = rows
        .iter()
        .find(|row| live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1"))
        .unwrap_or_else(|| panic!("expected complex Feldera SQL output for u1; rows={rows:?}"));
    assert_eq!(row["score_window"], json!([7, 8, null]));
    assert_eq!(
        row["score_row"],
        json!({
            "owner": "u1",
            "points": 7
        })
    );
    assert_eq!(row["score_variant"], json!("7"));
    assert_eq!(
        row["sample_uuid"],
        json!("018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa")
    );
    assert_eq!(row["raw_bytes"], json!("0a0bff"));
    assert_eq!(row["score_band"], json!("high"));
    assert_eq!(row["fallback_score"], json!(7));
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_map_output_values() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager MAP output runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_scores_relation_catalog();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_map_output"),
        "select 'all' as bucket, MAP(select user_id, score from scores where score > 0) as score_map",
        vec![catalog.clone()],
        vec![live_scores_input_schema(&catalog)],
        vec![(catalog, live_scores_record_batch())],
    )
    .await;

    let row = rows
        .iter()
        .find(|row| live_row_string(row, "bucket").is_some_and(|bucket| bucket == "all"))
        .unwrap_or_else(|| panic!("expected Feldera MAP output row; rows={rows:?}"));
    assert_eq!(row["score_map"], json!({ "u1": 7 }));
}

#[tokio::test]
async fn live_feldera_pipeline_manager_runtime_supports_json_variant_functions() {
    let Some(base_url) = live_feldera_runtime_base_url() else {
        eprintln!(
            "skipping live Feldera pipeline-manager JSON/VARIANT runtime test; set LIVE_FELDERA=1, LIVE_FELDERA_RUNTIME=1, and VELORIX_FELDERA_PIPELINE_MANAGER_URL"
        );
        return;
    };
    let _guard = live_feldera_test_guard().await;
    let catalog = live_json_events_relation_catalog();
    let input_schema = catalog_input_relation_schema(&catalog).unwrap();
    let rows = live_compile_ingest_query(
        base_url,
        &live_unique_id("velorix_live_json_variant_functions"),
        "select id, \
            cast(payload['name'] as varchar) as payload_name, \
            cast(payload['scores'][1] as bigint) + cast(payload['scores'][2] as bigint) as payload_score_sum, \
            cast(payload['nested']['active'] as boolean) as payload_active, \
            cast(parse_json(raw_json)['flag'] as boolean) as parsed_flag, \
            cast(parse_json(raw_json)['count'] as bigint) as parsed_count, \
            parse_json('null') = variantnull() as json_null_is_variant_null, \
            variantnull() = variantnull() as variant_null_equals, \
            variantnull() is null as variant_null_is_sql_null \
         from json_events where id = 'j1'",
        vec![catalog.clone()],
        vec![input_schema],
        vec![(catalog, live_json_events_record_batch())],
    )
    .await;

    let row = rows
        .iter()
        .find(|row| live_row_string(row, "id").is_some_and(|id| id == "j1"))
        .unwrap_or_else(|| panic!("expected JSON/VARIANT output; rows={rows:?}"));
    assert_eq!(live_row_string(row, "payload_name"), Some("Ada"));
    assert_eq!(live_row_i64(row, "payload_score_sum"), Some(21));
    assert_eq!(live_row_bool(row, "payload_active"), Some(true));
    assert_eq!(live_row_bool(row, "parsed_flag"), Some(true));
    assert_eq!(live_row_i64(row, "parsed_count"), Some(3));
    assert_eq!(live_row_bool(row, "json_null_is_variant_null"), Some(true));
    assert_eq!(live_row_bool(row, "variant_null_equals"), Some(true));
    assert_eq!(live_row_bool(row, "variant_null_is_sql_null"), Some(false));
}

async fn live_compile_only(
    base_url: String,
    view_id: &str,
    sql: &str,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
) -> RelationSchema {
    let compiler_request = FelderaCompileRequestV1 {
        view_id: view_id.to_string(),
        sql: sql.to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: input_schemas,
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: catalogs.len() > 1,
            multi_output: false,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let response = live_feldera_backend(base_url)
        .compile(FelderaCompilerBackendRequest {
            job_id: format!("live-feldera-compile-{view_id}"),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs,
        })
        .await
        .unwrap();
    response
        .resolved_spec
        .output_relations
        .into_iter()
        .next()
        .unwrap()
}

async fn expect_live_feldera_compile_rejects(
    base_url: String,
    view_id: &str,
    job_id: &str,
    sql: &str,
    expected_fragments: &[&str],
) {
    let catalog = live_scores_relation_catalog();
    let input_schema = live_scores_input_schema(&catalog);
    let compiler_request = FelderaCompileRequestV1 {
        view_id: view_id.to_string(),
        sql: sql.to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema],
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let error = live_feldera_backend(base_url)
        .with_volatile_runtime_deployment()
        .compile(FelderaCompilerBackendRequest {
            job_id: job_id.to_string(),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: vec![catalog],
        })
        .await
        .expect_err("documented unsupported Feldera SQL must fail instead of falling back");
    let error_debug = format!("{error:?}");
    assert!(
        expected_fragments.iter().any(|fragment| error_debug
            .to_ascii_uppercase()
            .contains(&fragment.to_ascii_uppercase())),
        "unsupported SQL error did not contain any expected fragment {:?}: {}",
        expected_fragments,
        error_debug
    );
}

async fn live_compile_program_only(
    base_url: String,
    program_id: &str,
    sql: &str,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
) -> Vec<RelationSchema> {
    let compiler_request = FelderaCompileRequestV1 {
        view_id: program_id.to_string(),
        sql: sql.to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::FelderaProgram,
        rust_extension: Default::default(),
        input_relations: input_schemas,
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: catalogs.len() > 1,
            multi_output: true,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let response = live_feldera_backend(base_url)
        .compile(FelderaCompilerBackendRequest {
            job_id: format!("live-feldera-program-compile-{program_id}"),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs,
        })
        .await
        .unwrap();
    response.resolved_spec.output_relations
}

async fn live_compile_ingest_query(
    base_url: String,
    view_id: &str,
    sql: &str,
    catalogs: Vec<VelorixRelationCatalogV1>,
    input_schemas: Vec<RelationSchema>,
    batches: Vec<(VelorixRelationCatalogV1, RecordBatch)>,
) -> Vec<Value> {
    let compiler_request = FelderaCompileRequestV1 {
        view_id: view_id.to_string(),
        sql: sql.to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: input_schemas.clone(),
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: input_schemas.len() > 1,
            multi_output: false,
        },
    };
    let compile_request_hash = feldera_compile_request_hash(&compiler_request).unwrap();
    let program_code = feldera_sql_program_for_compile_request(&compiler_request).unwrap();
    let backend = live_feldera_backend(base_url).with_volatile_runtime_deployment();

    let response = backend
        .compile(FelderaCompilerBackendRequest {
            job_id: format!("live-feldera-{view_id}"),
            view_id: compiler_request.view_id.clone(),
            spec_hash: "live-feldera-spec-not-used".to_string(),
            compile_request_hash,
            program_code,
            compiler_request,
            catalogs: catalogs.clone(),
        })
        .await
        .unwrap();
    let output_schemas = response.resolved_spec.output_relations.clone();
    let identity = live_standing_program_identity_for(view_id, &response.resolved_spec, &catalogs);
    let mut runtime = backend
        .create_with_catalogs_and_spec(
            &identity,
            &catalogs,
            &response.resolved_spec,
            &input_schemas,
            &output_schemas,
        )
        .unwrap();
    let relation_batches = batches
        .into_iter()
        .map(|(catalog, batch)| RelationInputBatch {
            relation_id: catalog.relation_schema.relation_id.clone(),
            relation_version: catalog.relation_schema.relation_version.clone(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            start_offset_inclusive: 0,
            end_offset_exclusive: batch.num_rows() as u64,
            batches: vec![batch],
        })
        .collect::<Vec<_>>();
    runtime
        .apply_changes(
            1,
            EpochIdempotencyKey::new(format!("live-feldera-{view_id}-epoch-1")).unwrap(),
            relation_batches,
        )
        .unwrap();
    runtime
        .materialized_view_sql_page(
            ScopedViewId {
                tenant_id: identity.tenant_id,
                program_id: identity.program_id,
                view_id: view_id.to_string(),
            },
            format!("SELECT * FROM \"{view_id}\""),
            SnapshotPageRequest::default(),
        )
        .unwrap()
        .rows
}

fn live_scores_compile_request(
    input_schema: RelationSchema,
    view_id: &str,
) -> FelderaCompileRequestV1 {
    FelderaCompileRequestV1 {
        view_id: view_id.to_string(),
        sql: "select user_id, sum(score) as sum, count(*) as count from scores group by user_id"
            .to_string(),
        dialect: SqlDialect::FelderaSql,
        source_kind: SqlSourceKind::StandingView,
        rust_extension: Default::default(),
        input_relations: vec![input_schema],
        output_contract: OutputSchemaContract::Infer,
        shape: StandingViewShape {
            is_materialized: true,
            multi_input: false,
            multi_output: false,
        },
    }
}

fn live_feldera_base_url() -> Option<String> {
    let enabled = env::var("LIVE_FELDERA")
        .ok()
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    if !enabled {
        return None;
    }
    env::var("VELORIX_FELDERA_PIPELINE_MANAGER_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn live_feldera_runtime_base_url() -> Option<String> {
    let enabled = env::var("LIVE_FELDERA_RUNTIME")
        .ok()
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    if !enabled {
        return None;
    }
    live_feldera_base_url()
}

fn live_feldera_runtime_enabled() -> bool {
    env::var("LIVE_FELDERA_RUNTIME")
        .ok()
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

fn live_feldera_compiler_timeout_ms() -> u64 {
    env_u64("VELORIX_FELDERA_COMPILER_TIMEOUT_MS").unwrap_or_else(|| {
        if live_feldera_runtime_enabled() {
            LIVE_FELDERA_RUNTIME_TIMEOUT_DEFAULT_MS
        } else {
            LIVE_FELDERA_SCHEMA_TIMEOUT_DEFAULT_MS
        }
    })
}

fn env_u64(name: &str) -> Option<u64> {
    env::var(name).ok().and_then(|value| value.parse().ok())
}

fn live_unique_id(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    static RUN_ID: OnceLock<String> = OnceLock::new();
    let run_id = RUN_ID.get_or_init(|| {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after UNIX epoch")
            .as_millis();
        format!("{:x}_{:x}", process::id(), millis)
    });
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{run_id}_{counter:x}")
}

async fn live_feldera_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

fn live_feldera_backend(base_url: String) -> FelderaPipelineManagerCompilerBackend {
    FelderaPipelineManagerCompilerBackend::new(
        base_url,
        env::var("VELORIX_FELDERA_BEARER_TOKEN").ok(),
        Duration::from_millis(
            env_u64("VELORIX_FELDERA_COMPILER_POLL_INTERVAL_MS").unwrap_or(1_000),
        ),
        Duration::from_millis(live_feldera_compiler_timeout_ms()),
        env::var("VELORIX_FELDERA_COMPILER_PROFILE").unwrap_or_else(|_| "dev".to_string()),
        u32::try_from(env_u64("VELORIX_FELDERA_COMPILER_WORKERS").unwrap_or(1))
            .expect("VELORIX_FELDERA_COMPILER_WORKERS must fit u32"),
    )
    .unwrap()
}

async fn live_api_state_memory(probe_id: &str) -> (ApiState, Arc<dyn ObjectStore>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let validated = validate_operator_authority(
        ObjectStoreAuthorityRef {
            store_id: format!("s3://rustfs/velorix-live-feldera/{probe_id}"),
            namespace: "velorix".to_string(),
        },
        Arc::clone(&store),
        "velorix-live-feldera-test",
        format!("v1/live-feldera-test-probes/{probe_id}"),
    )
    .await
    .unwrap();
    let state =
        ApiState::from_validated_authority(validated, "v1/state/slatedb", "live-feldera-api-test")
            .await
            .unwrap();
    (state, store)
}

async fn live_request_json(
    app: axum::Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
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

async fn live_feldera_pipeline_exists(base_url: &str, pipeline_name: &str) -> bool {
    let url = format!(
        "{}/v0/pipelines/{pipeline_name}",
        base_url.trim_end_matches('/')
    );
    reqwest::get(url)
        .await
        .is_ok_and(|response| response.status().is_success())
}

async fn live_feldera_pipeline_deleted(base_url: &str, pipeline_name: &str) -> bool {
    let url = format!(
        "{}/v0/pipelines/{pipeline_name}",
        base_url.trim_end_matches('/')
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(35);
    loop {
        match reqwest::get(url.clone()).await {
            Ok(response) if response.status() == reqwest::StatusCode::NOT_FOUND => return true,
            Ok(_) | Err(_) if tokio::time::Instant::now() >= deadline => return false,
            Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

fn live_scores_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "scores".to_string(),
        relation_name: "scores".to_string(),
        relation_version: "live.v1".to_string(),
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
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "score".to_string(),
                name: "score".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Value,
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

fn live_scores_rest_relation_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = live_scores_relation_catalog();
    for column in &mut catalog.relation_schema.columns {
        if column.column_id == "user_id" {
            column.semantic_role = RelationSemanticRoleV1::PrimaryKey;
        }
    }
    catalog.relation_schema.primary_key_column_ids =
        vec!["event_id".to_string(), "user_id".to_string()];
    catalog.incremental_adapter.adapter_id =
        CATALOG_ROW_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string();
    let schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.schema_fingerprint = schema_fingerprint.clone();
    catalog.feldera_relation.schema_fingerprint = schema_fingerprint;
    catalog
}

fn live_expanded_scalars_relation_catalog() -> VelorixRelationCatalogV1 {
    let columns = vec![
        live_relation_column(
            "id",
            VelorixLogicalTypeV1::Utf8,
            ArrowPhysicalTypeV1::Utf8,
            RelationSemanticRoleV1::PrimaryKey,
            0,
        ),
        live_relation_column(
            "i8_value",
            VelorixLogicalTypeV1::Int8,
            ArrowPhysicalTypeV1::Int8,
            RelationSemanticRoleV1::Metadata,
            1,
        ),
        live_relation_column(
            "i16_value",
            VelorixLogicalTypeV1::Int16,
            ArrowPhysicalTypeV1::Int16,
            RelationSemanticRoleV1::Metadata,
            2,
        ),
        live_relation_column(
            "i32_value",
            VelorixLogicalTypeV1::Int32,
            ArrowPhysicalTypeV1::Int32,
            RelationSemanticRoleV1::Metadata,
            3,
        ),
        live_relation_column(
            "u8_value",
            VelorixLogicalTypeV1::UInt8,
            ArrowPhysicalTypeV1::UInt8,
            RelationSemanticRoleV1::Metadata,
            4,
        ),
        live_relation_column(
            "u16_value",
            VelorixLogicalTypeV1::UInt16,
            ArrowPhysicalTypeV1::UInt16,
            RelationSemanticRoleV1::Metadata,
            5,
        ),
        live_relation_column(
            "u32_value",
            VelorixLogicalTypeV1::UInt32,
            ArrowPhysicalTypeV1::UInt32,
            RelationSemanticRoleV1::Metadata,
            6,
        ),
        live_relation_column(
            "u64_value",
            VelorixLogicalTypeV1::UInt64,
            ArrowPhysicalTypeV1::UInt64,
            RelationSemanticRoleV1::Metadata,
            7,
        ),
        live_relation_column(
            "f32_value",
            VelorixLogicalTypeV1::Float32,
            ArrowPhysicalTypeV1::Float32,
            RelationSemanticRoleV1::Metadata,
            8,
        ),
        live_relation_column(
            "code",
            VelorixLogicalTypeV1::Char { length: Some(8) },
            ArrowPhysicalTypeV1::Utf8,
            RelationSemanticRoleV1::Metadata,
            9,
        ),
        live_relation_column(
            "raw",
            VelorixLogicalTypeV1::Binary { length: 3 },
            ArrowPhysicalTypeV1::Binary,
            RelationSemanticRoleV1::Metadata,
            10,
        ),
        live_relation_column(
            "bytes",
            VelorixLogicalTypeV1::Varbinary,
            ArrowPhysicalTypeV1::Binary,
            RelationSemanticRoleV1::Metadata,
            11,
        ),
        live_relation_column(
            "event_time",
            VelorixLogicalTypeV1::Time,
            ArrowPhysicalTypeV1::Time64Nanosecond,
            RelationSemanticRoleV1::Metadata,
            12,
        ),
        live_relation_column(
            "event_date",
            VelorixLogicalTypeV1::Date,
            ArrowPhysicalTypeV1::Date32,
            RelationSemanticRoleV1::Metadata,
            13,
        ),
        live_relation_column(
            "event_ts",
            VelorixLogicalTypeV1::Timestamp { timezone: None },
            ArrowPhysicalTypeV1::TimestampNanosecond { timezone: None },
            RelationSemanticRoleV1::Metadata,
            14,
        ),
        live_relation_column(
            "uuid_value",
            VelorixLogicalTypeV1::Uuid,
            ArrowPhysicalTypeV1::Utf8,
            RelationSemanticRoleV1::Metadata,
            15,
        ),
        live_relation_column(
            "amount",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::Value,
            16,
        ),
        live_relation_column(
            "delta",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::Weight,
            17,
        ),
    ];
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "expanded_scalars".to_string(),
        relation_name: "expanded_scalars".to_string(),
        relation_version: "live.v1".to_string(),
        columns,
        primary_key_column_ids: vec!["id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "expanded_scalars".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "expanded_scalars".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn live_rates_relation_catalog() -> VelorixRelationCatalogV1 {
    let columns = vec![
        live_relation_column(
            "rate_id",
            VelorixLogicalTypeV1::Utf8,
            ArrowPhysicalTypeV1::Utf8,
            RelationSemanticRoleV1::PrimaryKey,
            0,
        ),
        live_relation_column(
            "entity_id",
            VelorixLogicalTypeV1::Utf8,
            ArrowPhysicalTypeV1::Utf8,
            RelationSemanticRoleV1::Metadata,
            1,
        ),
        live_relation_column(
            "effective_ts",
            VelorixLogicalTypeV1::Timestamp { timezone: None },
            ArrowPhysicalTypeV1::TimestampNanosecond { timezone: None },
            RelationSemanticRoleV1::Metadata,
            2,
        ),
        live_relation_column(
            "rate_value",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::Value,
            3,
        ),
        live_relation_column(
            "delta",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::Weight,
            4,
        ),
    ];
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "rates".to_string(),
        relation_name: "rates".to_string(),
        relation_version: "live.v1".to_string(),
        columns,
        primary_key_column_ids: vec!["rate_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert],
        event_time_column_id: Some("effective_ts".to_string()),
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "rates".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "rates".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn live_json_events_relation_catalog() -> VelorixRelationCatalogV1 {
    let columns = vec![
        live_relation_column(
            "id",
            VelorixLogicalTypeV1::Utf8,
            ArrowPhysicalTypeV1::Utf8,
            RelationSemanticRoleV1::PrimaryKey,
            0,
        ),
        live_relation_column(
            "payload",
            VelorixLogicalTypeV1::Json,
            ArrowPhysicalTypeV1::JsonUtf8,
            RelationSemanticRoleV1::Metadata,
            1,
        ),
        live_relation_column(
            "raw_json",
            VelorixLogicalTypeV1::Utf8,
            ArrowPhysicalTypeV1::Utf8,
            RelationSemanticRoleV1::Metadata,
            2,
        ),
        live_relation_column(
            "weight",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::Weight,
            3,
        ),
    ];
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "json_events".to_string(),
        relation_name: "json_events".to_string(),
        relation_version: "live.v1".to_string(),
        columns,
        primary_key_column_ids: vec!["id".to_string()],
        weight_column_id: "weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "json_events".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "json_events".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_FELDERA_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn live_edges_relation_catalog() -> VelorixRelationCatalogV1 {
    let columns = vec![
        live_relation_column(
            "src",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::PrimaryKey,
            0,
        ),
        live_relation_column(
            "dst",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::PrimaryKey,
            1,
        ),
        live_relation_column(
            "delta",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::Weight,
            2,
        ),
    ];
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "edges".to_string(),
        relation_name: "edges".to_string(),
        relation_version: "live.v1".to_string(),
        columns,
        primary_key_column_ids: vec!["src".to_string(), "dst".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "edges".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "edges".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn live_nested_inputs_relation_catalog() -> VelorixRelationCatalogV1 {
    let columns = vec![
        live_relation_column(
            "id",
            VelorixLogicalTypeV1::Utf8,
            ArrowPhysicalTypeV1::Utf8,
            RelationSemanticRoleV1::PrimaryKey,
            0,
        ),
        live_relation_column(
            "scores",
            VelorixLogicalTypeV1::Array {
                element_type: Box::new(VelorixLogicalTypeV1::Int64),
            },
            ArrowPhysicalTypeV1::List {
                element_type: Box::new(ArrowPhysicalTypeV1::Int64),
            },
            RelationSemanticRoleV1::Metadata,
            1,
        ),
        live_relation_column(
            "attributes",
            VelorixLogicalTypeV1::Map {
                key_type: Box::new(VelorixLogicalTypeV1::Utf8),
                value_type: Box::new(VelorixLogicalTypeV1::Int64),
            },
            ArrowPhysicalTypeV1::Map {
                key_type: Box::new(ArrowPhysicalTypeV1::Utf8),
                value_type: Box::new(ArrowPhysicalTypeV1::Int64),
            },
            RelationSemanticRoleV1::Metadata,
            2,
        ),
        live_relation_column(
            "profile",
            VelorixLogicalTypeV1::Struct {
                fields: vec![
                    VelorixStructFieldV1 {
                        name: "name".to_string(),
                        logical_type: VelorixLogicalTypeV1::Utf8,
                        nullable: false,
                    },
                    VelorixStructFieldV1 {
                        name: "tier".to_string(),
                        logical_type: VelorixLogicalTypeV1::Int32,
                        nullable: true,
                    },
                ],
            },
            ArrowPhysicalTypeV1::Struct {
                fields: vec![
                    ArrowStructFieldV1 {
                        name: "name".to_string(),
                        physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                        nullable: false,
                    },
                    ArrowStructFieldV1 {
                        name: "tier".to_string(),
                        physical_arrow_type: ArrowPhysicalTypeV1::Int32,
                        nullable: true,
                    },
                ],
            },
            RelationSemanticRoleV1::Metadata,
            3,
        ),
        live_relation_column(
            "amount",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::Value,
            4,
        ),
        live_relation_column(
            "delta",
            VelorixLogicalTypeV1::Int64,
            ArrowPhysicalTypeV1::Int64,
            RelationSemanticRoleV1::Weight,
            5,
        ),
    ];
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "nested_inputs".to_string(),
        relation_name: "nested_inputs".to_string(),
        relation_version: "live.v1".to_string(),
        columns,
        primary_key_column_ids: vec!["id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();
    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "nested_inputs".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "nested_inputs".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn live_relation_column(
    name: &str,
    logical_type: VelorixLogicalTypeV1,
    physical_arrow_type: ArrowPhysicalTypeV1,
    semantic_role: RelationSemanticRoleV1,
    ordinal: u32,
) -> RelationColumnV1 {
    RelationColumnV1 {
        column_id: name.to_string(),
        name: name.to_string(),
        logical_type,
        physical_arrow_type,
        nullable: false,
        ordinal,
        semantic_role,
    }
}

fn live_scores_input_schema(catalog: &VelorixRelationCatalogV1) -> RelationSchema {
    RelationSchema {
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_name: catalog.relation_schema.relation_name.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
        columns: vec![
            ColumnSchema {
                name: "event_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
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
        primary_key: vec!["event_id".to_string()],
    }
}

fn live_scores_record_batch() -> RecordBatch {
    RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["e1", "e2"])),
            std::sync::Arc::new(StringArray::from(vec!["u1", "u1"])),
            std::sync::Arc::new(Int64Array::from(vec![5, 7])),
            std::sync::Arc::new(Int64Array::from(vec![1, 1])),
        ],
    )
    .unwrap()
}

fn live_scores_three_users_record_batch() -> RecordBatch {
    RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["e1", "e2", "e3"])),
            std::sync::Arc::new(StringArray::from(vec!["u1", "u2", "u3"])),
            std::sync::Arc::new(Int64Array::from(vec![5, 9, 13])),
            std::sync::Arc::new(Int64Array::from(vec![1, 1, 1])),
        ],
    )
    .unwrap()
}

fn live_scores_complex_rollup_record_batch() -> RecordBatch {
    RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["e1", "e2", "e3", "e4"])),
            std::sync::Arc::new(StringArray::from(vec!["u1", "u1", "u2", "u3"])),
            std::sync::Arc::new(Int64Array::from(vec![5, 7, 9, 0])),
            std::sync::Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
        ],
    )
    .unwrap()
}

fn live_expanded_scalars_record_batch() -> RecordBatch {
    RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("i8_value", DataType::Int8, false),
            Field::new("i16_value", DataType::Int16, false),
            Field::new("i32_value", DataType::Int32, false),
            Field::new("u8_value", DataType::UInt8, false),
            Field::new("u16_value", DataType::UInt16, false),
            Field::new("u32_value", DataType::UInt32, false),
            Field::new("u64_value", DataType::UInt64, false),
            Field::new("f32_value", DataType::Float32, false),
            Field::new("code", DataType::Utf8, false),
            Field::new("raw", DataType::Binary, false),
            Field::new("bytes", DataType::Binary, false),
            Field::new("event_time", DataType::Time64(TimeUnit::Nanosecond), false),
            Field::new("event_date", DataType::Date32, false),
            Field::new(
                "event_ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("uuid_value", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["x1"])),
            std::sync::Arc::new(Int8Array::from(vec![-8])),
            std::sync::Arc::new(Int16Array::from(vec![-16])),
            std::sync::Arc::new(Int32Array::from(vec![-32])),
            std::sync::Arc::new(UInt8Array::from(vec![8])),
            std::sync::Arc::new(UInt16Array::from(vec![16])),
            std::sync::Arc::new(UInt32Array::from(vec![32])),
            std::sync::Arc::new(UInt64Array::from(vec![64])),
            std::sync::Arc::new(Float32Array::from(vec![3.5])),
            std::sync::Arc::new(StringArray::from(vec!["ABCD"])),
            std::sync::Arc::new(BinaryArray::from_vec(vec![b"\x0a\x0b\xff"])),
            std::sync::Arc::new(BinaryArray::from_vec(vec![b"\xde\xad\xbe\xef"])),
            std::sync::Arc::new(Time64NanosecondArray::from(vec![3_723_000_000_000])),
            std::sync::Arc::new(Date32Array::from(vec![20_614])),
            std::sync::Arc::new(TimestampNanosecondArray::from(vec![
                1_781_053_323_000_000_000,
            ])),
            std::sync::Arc::new(StringArray::from(vec![
                "018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa",
            ])),
            std::sync::Arc::new(Int64Array::from(vec![100])),
            std::sync::Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap()
}

fn live_rates_record_batch() -> RecordBatch {
    RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("rate_id", DataType::Utf8, false),
            Field::new("entity_id", DataType::Utf8, false),
            Field::new(
                "effective_ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("rate_value", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["r1", "r2", "r3"])),
            std::sync::Arc::new(StringArray::from(vec!["x1", "x1", "x1"])),
            std::sync::Arc::new(TimestampNanosecondArray::from(vec![
                1_781_049_600_000_000_000,
                1_781_052_000_000_000_000,
                1_781_056_800_000_000_000,
            ])),
            std::sync::Arc::new(Int64Array::from(vec![10, 12, 99])),
            std::sync::Arc::new(Int64Array::from(vec![1, 1, 1])),
        ],
    )
    .unwrap()
}

fn live_json_events_record_batch() -> RecordBatch {
    RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
            Field::new("raw_json", DataType::Utf8, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["j1"])),
            std::sync::Arc::new(StringArray::from(vec![
                r#"{"name":"Ada","scores":[8,13],"nested":{"active":true}}"#,
            ])),
            std::sync::Arc::new(StringArray::from(vec![r#"{"flag":true,"count":3}"#])),
            std::sync::Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap()
}

fn live_edges_record_batch() -> RecordBatch {
    RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, false),
            Field::new("dst", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            std::sync::Arc::new(Int64Array::from(vec![1, 2, 3])),
            std::sync::Arc::new(Int64Array::from(vec![2, 3, 4])),
            std::sync::Arc::new(Int64Array::from(vec![1, 1, 1])),
        ],
    )
    .unwrap()
}

fn live_standing_program_identity_for(
    view_id: &str,
    spec: &StandingViewSpec,
    catalogs: &[VelorixRelationCatalogV1],
) -> StandingProgramIdentity {
    live_standing_program_identity_for_outputs(view_id, spec, catalogs, vec![view_id.to_string()])
}

fn live_standing_program_identity_for_outputs(
    program_id: &str,
    spec: &StandingViewSpec,
    catalogs: &[VelorixRelationCatalogV1],
    view_ids: Vec<String>,
) -> StandingProgramIdentity {
    let spec_bytes = serde_json::to_vec(spec).unwrap();
    let catalog_bytes = serde_json::to_vec(catalogs).unwrap();
    let output_bytes = serde_json::to_vec(&spec.output_relations).unwrap();
    StandingProgramIdentity {
        tenant_id: "live".to_string(),
        program_id: format!("live-feldera-runtime-{program_id}"),
        view_ids,
        sql_hash: sha256_hex(spec.sql.as_bytes()),
        input_catalog_hash: sha256_hex(&catalog_bytes),
        output_schema_hash: sha256_hex(&output_bytes),
        compiler_identity: sha256_hex(&spec_bytes),
        runtime_packages: vec![FelderaRuntimePackageIdentity {
            name: "feldera-pipeline-manager-runtime".to_string(),
            version: "feldera-pipeline-manager-runtime-v1".to_string(),
        }],
        package_feature_set: vec!["live-feldera-pipeline-manager".to_string()],
        dbsp_runtime_compatibility: "feldera-pipeline-manager-runtime-v1".to_string(),
        checkpoint_codec_identity: "feldera-pipeline-manager-state-v2".to_string(),
        native_code_policy: NativeCodePolicy::DisabledNoExternalDependencies,
    }
}

fn live_profiles_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "profiles".to_string(),
        relation_name: "profiles".to_string(),
        relation_version: "live.v1".to_string(),
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
                column_id: "tier".to_string(),
                name: "tier".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
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
            name: "profiles".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "profiles".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn live_profiles_input_schema(catalog: &VelorixRelationCatalogV1) -> RelationSchema {
    RelationSchema {
        relation_id: catalog.relation_schema.relation_id.clone(),
        relation_name: catalog.relation_schema.relation_name.clone(),
        relation_version: catalog.relation_schema.relation_version.clone(),
        schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
        columns: vec![
            ColumnSchema {
                name: "user_id".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "tier".to_string(),
                data_type: SqlDataType::Utf8,
                nullable: false,
            },
            ColumnSchema {
                name: "delta".to_string(),
                data_type: SqlDataType::Int64,
                nullable: false,
            },
        ],
        primary_key: vec!["user_id".to_string()],
    }
}

fn live_profiles_record_batch() -> RecordBatch {
    RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("tier", DataType::Utf8, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["u1", "u2"])),
            std::sync::Arc::new(StringArray::from(vec!["gold", "silver"])),
            std::sync::Arc::new(Int64Array::from(vec![1, 1])),
        ],
    )
    .unwrap()
}

fn live_profiles_with_unmatched_record_batch() -> RecordBatch {
    RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("tier", DataType::Utf8, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["u1", "u2", "u4"])),
            std::sync::Arc::new(StringArray::from(vec!["gold", "silver", "platinum"])),
            std::sync::Arc::new(Int64Array::from(vec![1, 1, 1])),
        ],
    )
    .unwrap()
}

fn live_row_matches_sum_count(row: &Value) -> bool {
    if !row.is_object() {
        return false;
    }
    let user_id_matches = live_row_string(row, "user_id").is_some_and(|user_id| user_id == "u1");
    let sum_matches = live_row_i64(row, "sum") == Some(12);
    let count_matches = live_row_i64(row, "count") == Some(2);
    user_id_matches && sum_matches && count_matches
}

fn live_row_string<'a>(row: &'a Value, field: &str) -> Option<&'a str> {
    row.as_object()?.get(field)?.as_str()
}

fn live_row_i64(row: &Value, field: &str) -> Option<i64> {
    let value = row.as_object()?.get(field)?;
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_str()?.parse().ok())
}

fn live_row_f64(row: &Value, field: &str) -> Option<f64> {
    let value = row.as_object()?.get(field)?;
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

fn live_row_bool(row: &Value, field: &str) -> Option<bool> {
    row.as_object()?.get(field)?.as_bool()
}

fn live_row_is_null(row: &Value, field: &str) -> bool {
    row.as_object()
        .and_then(|object| object.get(field))
        .is_some_and(Value::is_null)
}

fn live_output_has_columns(output: &RelationSchema, expected: &[&str]) -> bool {
    let names = output
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    expected
        .iter()
        .all(|expected| names.iter().any(|name| name == expected))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}
