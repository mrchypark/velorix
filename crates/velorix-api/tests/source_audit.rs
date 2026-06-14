use std::{fs, path::PathBuf, process::Command};

const API_LIB: &str = include_str!("../src/lib.rs");
const REST_PRODUCT_TESTS: &str = include_str!("rest_product.rs");
const LIVE_FELDERA_TESTS: &str = include_str!("live_feldera_pipeline_manager.rs");
const LIVE_FELDERA_RUNNER: &str =
    include_str!("../../../scripts/run-live-feldera-pipeline-manager.sh");
const LIVE_FELDERA_EVIDENCE_VALIDATOR: &str =
    include_str!("../../../scripts/validate-live-feldera-evidence.py");
const FELDERA_COMPILER_WORKER_DOC: &str =
    include_str!("../../../docs/architecture/feldera-compiler-worker.md");
const FELDERA_ARTIFACT_CONTRACT_DOC: &str =
    include_str!("../../../docs/architecture/feldera-artifact-contract.md");
const FELDERA_COMPILER_WORKER_LIB: &str =
    include_str!("../../velorix-feldera-compiler-worker/src/lib.rs");
const FELDERA_COMPILER_WORKER_MAIN: &str =
    include_str!("../../velorix-feldera-compiler-worker/src/main.rs");
const FELDERA_COMPILER_WORKER_DOCKERFILE: &str =
    include_str!("../../../Dockerfile.feldera-compiler-worker");

#[test]
fn source_audit_feldera_compiler_path_precedes_linked_fixture_activation() {
    let compiler_backend = API_LIB
        .find("let resolution = if let Some(backend) = self.feldera_compiler_backend.as_ref()")
        .expect("compile/deploy worker should branch on Feldera compiler backend");
    let linked_descriptor = API_LIB
        .find("trusted_generated_descriptor_for_spec(self, catalog, &active.spec)")
        .expect("compile/deploy worker should still support linked descriptors");
    let linked_fixture = API_LIB
        .find("self.generated_package_artifact_for_spec(&catalogs, &active.spec)")
        .expect("compile/deploy worker should still support linked fixture packages");

    assert!(
        compiler_backend < linked_descriptor && compiler_backend < linked_fixture,
        "Feldera compiler backend must be tried before Velorix linked/generated fixture activation"
    );
}

#[test]
fn source_audit_dbsp_sql_shape_validators_are_quarantined_to_linked_fixtures() {
    let allowed_functions = [
        "output_schemas_for_view_request",
        "output_schemas_for_view_request_with_catalogs",
        "generic_single_key_sum_count_artifact_for_spec_with_catalogs",
    ];
    for needle in [
        "validate_catalog_backed_sum_count_view_sql",
        "validate_supported_dbsp_join_view_sql",
    ] {
        for byte_index in occurrence_indices(API_LIB, needle) {
            let line = line_at(API_LIB, byte_index);
            let Some(function) = enclosing_function_name(API_LIB, byte_index) else {
                continue;
            };
            assert!(
                allowed_functions.contains(&function.as_str()),
                "`{needle}` is only allowed in linked/generated DBSP fixture helpers; found in `{}` at line {}: {}",
                function,
                line_number_at(API_LIB, byte_index),
                line.trim()
            );
        }
    }
}

#[test]
fn source_audit_api_product_path_does_not_import_sql_execution_engines() {
    for forbidden in [
        "datafusion::sql",
        "sqlparser::",
        "feldera_ir::",
        "dbsp::",
        "RootCircuit",
        "TypedBox",
    ] {
        assert!(
            !API_LIB.contains(forbidden),
            "velorix-api product path must not import or construct SQL/DBSP execution internals directly: found `{forbidden}`"
        );
    }
}

#[test]
fn source_audit_feldera_product_path_is_jarless_by_default() {
    assert!(
        FELDERA_COMPILER_WORKER_LIB.contains("WorkerBackendKind::FelderaPackageJarless"),
        "compiler worker must expose a jarless Feldera package backend kind"
    );
    assert!(
        FELDERA_COMPILER_WORKER_MAIN.contains("default_value = \"feldera-package-jarless\""),
        "compiler worker CLI must default to the jarless Feldera package backend"
    );
    assert!(
        FELDERA_COMPILER_WORKER_LIB.contains("compatibility-pipeline-manager"),
        "pipeline-manager must be an explicit compatibility backend, not the implicit product backend"
    );
    assert!(
        FELDERA_COMPILER_WORKER_LIB.contains("JAR-backed compatibility fixture"),
        "supplying a pipeline-manager URL without explicit compatibility backend selection must fail closed"
    );
    assert!(
        FELDERA_COMPILER_WORKER_LIB.contains("enum CompileOutcome")
            && FELDERA_COMPILER_WORKER_LIB.contains("CompatibilityRuntime")
            && FELDERA_COMPILER_WORKER_LIB.contains("ProductRuntime")
            && FELDERA_COMPILER_WORKER_LIB.contains("product_runtime")
            && FELDERA_COMPILER_WORKER_LIB.contains("SchemaOnly")
            && FELDERA_COMPILER_WORKER_LIB.contains("Unsupported")
            && FELDERA_COMPILER_WORKER_LIB
                .contains("completed_product_runtime_deployment")
            && FELDERA_COMPILER_WORKER_LIB.contains("compiled_schema_only_not_deployed")
            && FELDERA_COMPILER_WORKER_LIB
                .contains("completed_compatibility_runtime_deployment"),
        "compiler worker must expose a backend outcome boundary and label pipeline-manager completion as compatibility runtime"
    );
    assert!(
        !FELDERA_COMPILER_WORKER_LIB.contains("completed_runtime_deployment")
            && !FELDERA_COMPILER_WORKER_DOC.contains("completed_runtime_deployment"),
        "pipeline-manager worker reports/docs must not use ambiguous bare runtime deployment completion wording"
    );
    assert!(
        FELDERA_COMPILER_WORKER_DOC.contains("the product backend must not ship Feldera's SQL compiler jar")
            && FELDERA_COMPILER_WORKER_DOC.contains("compatibility fixture only")
            && FELDERA_COMPILER_WORKER_DOC.contains("product_runtime")
            && FELDERA_COMPILER_WORKER_DOC.contains("does not satisfy this product gate"),
        "compiler-worker architecture must distinguish jarless product gates from pipeline-manager fixture evidence"
    );
    assert!(
        REST_PRODUCT_TESTS
            .contains("rest_product_worker_activates_pending_view_from_jarless_product_runtime_descriptor")
            && REST_PRODUCT_TESTS.contains("product_runtime")
            && REST_PRODUCT_TESTS.contains("FELDERA_PACKAGE_RUNTIME_EXECUTION_PATH"),
        "REST product tests must cover product_runtime activation separately from pipeline-manager compatibility runtime"
    );
    assert!(
        FELDERA_ARTIFACT_CONTRACT_DOC.contains("not the product backend target")
            && FELDERA_ARTIFACT_CONTRACT_DOC.contains("jarless Feldera package path")
            && FELDERA_ARTIFACT_CONTRACT_DOC.contains("requested SQL family still requires the Java SQL compiler"),
        "artifact contract must not present the Java/Calcite generated artifact path as the product backend target"
    );
}

#[test]
fn source_audit_feldera_worker_image_does_not_bundle_official_feldera_or_jars() {
    for forbidden in [
        "images.feldera.com/feldera/pipeline-manager",
        "sql2dbsp",
        ".jar",
        "openjdk",
        "maven",
    ] {
        assert!(
            !FELDERA_COMPILER_WORKER_DOCKERFILE
                .to_ascii_lowercase()
                .contains(forbidden),
            "compiler worker Dockerfile must stay jarless and must not repackage official Feldera: found `{forbidden}`"
        );
    }
}

#[test]
fn source_audit_live_feldera_runner_covers_required_sql_families() {
    let required_runtime_tests: &[(&str, &str, &[&str])] = &[
        (
            "projection/filter",
            "live_feldera_pipeline_manager_runtime_supports_projection_and_filter",
            &["score * 2", "where score >= 7"],
        ),
        (
            "min/max/avg aggregates",
            "live_feldera_pipeline_manager_runtime_supports_min_max_avg_aggregates",
            &["min(score)", "max(score)", "avg(score)"],
        ),
        (
            "two-table join",
            "live_feldera_pipeline_manager_runtime_supports_two_table_join",
            &["join profiles", "group by p.tier"],
        ),
        (
            "cte/having/union",
            "live_feldera_pipeline_manager_runtime_supports_cte_having_union",
            &["with positives", "having sum(score) > 10", "union all"],
        ),
        (
            "distinct/intersect/except",
            "live_feldera_pipeline_manager_runtime_supports_distinct_intersect_except",
            &["select distinct", "intersect", " except "],
        ),
        (
            "scalar string/math functions",
            "live_feldera_pipeline_manager_runtime_supports_scalar_string_and_math_functions",
            &["upper(user_id)", "abs(score - 10)"],
        ),
        (
            "string/binary/hash functions",
            "live_feldera_pipeline_manager_runtime_supports_string_binary_hash_functions",
            &[
                "lower('Velorix')",
                "char_length('Velorix')",
                "concat_ws('-', 've', 'lo', 'rix')",
                "split_part('ve/lo/rix', '/', 2)",
                "regexp_replace('abc123'",
                "position('lo' in 'velorix')",
                "char_length(code)",
                "code = 'ABCD'",
                "bin2utf8(x'76656C6F')",
                "to_hex(x'76656C6F')",
                "octet_length(x'76656C6F')",
                "md5('velorix')",
                "xxhash('velorix', 10)",
            ],
        ),
        (
            "floating/numeric functions",
            "live_feldera_pipeline_manager_runtime_supports_floating_numeric_functions",
            &[
                "i16_value = -16",
                "u64_value = 64",
                "event_ts = TIMESTAMP '2026-06-10 01:02:03'",
                "uuid_value = '018f4b6e-9cb5-7f5a-8027-2ce24be4d3aa'",
                "acos(1.0e0)",
                "acosh(1.0e0)",
                "asinh(0.0e0)",
                "atan(0.0e0)",
                "atan2(0.0e0, 1.0e0)",
                "atanh(0.0e0)",
                "cbrt(27.0e0)",
                "cosh(0.0e0)",
                "cot(PI / 4.0e0)",
                "coth(1.0e0)",
                "csc(PI / 2.0e0)",
                "csch(1.0e0)",
                "degrees(PI)",
                "radians(180.0e0)",
                "log(8.0e0, 2.0e0)",
                "power(2.0e0, 3)",
                "round(12.345e0, 2)",
                "truncate(12.345e0)",
                "cast(i8_value as double) + 8.0e0",
                "div_null(1.0e0, cast(i8_value as double) + 8.0e0)",
                "is_inf(1.0e0 / (cast(i8_value as double) + 8.0e0))",
                "is_nan(sqrt(-1.0e0))",
                "finite_or_null(1.0e0 / (cast(i8_value as double) + 8.0e0))",
                "sec(0.0e0)",
                "sech(0.0e0)",
                "sinh(0.0e0)",
                "sqrt(4.0e0)",
                "tan(0.0e0)",
                "tanh(0.0e0)",
            ],
        ),
        (
            "computed grouping expressions",
            "live_feldera_pipeline_manager_runtime_supports_computed_grouping_expressions",
            &["case when score >=", "group by case"],
        ),
        (
            "lateral column aliasing",
            "live_feldera_pipeline_manager_runtime_supports_lateral_column_aliasing",
            &[
                "score + 1 as bucket",
                "bucket * 2",
                "group by bucket",
                "having bucket > 5",
            ],
        ),
        (
            "predicate family",
            "live_feldera_pipeline_manager_runtime_supports_between_in_and_like_predicates",
            &["between", " in ", " like "],
        ),
        (
            "distinct aggregates",
            "live_feldera_pipeline_manager_runtime_supports_distinct_aggregates",
            &["count(distinct user_id)", "count(distinct score)"],
        ),
        (
            "advanced aggregates",
            "live_feldera_pipeline_manager_runtime_supports_advanced_aggregates",
            &[
                "count(*) filter",
                "countif(score = 0)",
                "arg_max(user_id, score)",
                "arg_min(user_id, score)",
                "bit_and(score)",
                "bit_or(score)",
                "bit_xor(score)",
                "bool_and(score >= 0)",
                "logical_or(score = 9)",
                "every(score >= 0)",
                "some(score = 9)",
            ],
        ),
        (
            "pivot aggregates",
            "live_feldera_pipeline_manager_runtime_supports_pivot_aggregates",
            &["pivot (sum(score) as total", "for user_id in", "'u1' as u1"],
        ),
        (
            "unpivot and join using",
            "live_feldera_pipeline_manager_runtime_supports_unpivot_and_join_using",
            &["unpivot (score_value for score_kind", "join profiles using"],
        ),
        (
            "raw SQL output endpoint",
            "live_feldera_pipeline_manager_rest_api_supports_raw_sql_query_on_output_endpoint",
            &["WITH scoped", "union all", "/outputs/"],
        ),
        (
            "array query parameter",
            "live_feldera_pipeline_manager_rest_api_supports_array_query_parameter",
            &["is_array(element=string)", "user_ids=%5B%22u1%22%5D"],
        ),
        (
            "typed literal query parameters",
            "live_feldera_pipeline_manager_rest_api_supports_typed_literal_query_parameters",
            &[
                "is_date",
                "is_timestamp",
                "is_uuid",
                "is_binary_hex",
                "event_time=01%3A02%3A03",
            ],
        ),
        (
            "typed array query parameters",
            "live_feldera_pipeline_manager_rest_api_supports_typed_array_query_parameters",
            &[
                "array(element=date)",
                "array(element=uuid)",
                "array(element=binary_hex)",
                "event_dates=%5B%222026-06-10%22",
            ],
        ),
        (
            "JSON query parameters",
            "live_feldera_pipeline_manager_rest_api_supports_json_query_parameter",
            &[
                "\"type\": \"json\"",
                "\"json\"",
                "context.params.payload",
                "raw_json",
            ],
        ),
        (
            "promoted API pagination",
            "live_feldera_pipeline_manager_rest_api_paginates_promoted_sql_template",
            &["max_rows=2", "page_token=offset:2", "next_page_token"],
        ),
        (
            "window function",
            "live_feldera_pipeline_manager_runtime_supports_window_row_number",
            &["row_number() over"],
        ),
        (
            "scalar subqueries",
            "live_feldera_pipeline_manager_runtime_supports_scalar_subqueries",
            &[
                "(select max(score) from scores)",
                "score > (select avg(score) from scores)",
            ],
        ),
        (
            "window aggregates",
            "live_feldera_pipeline_manager_runtime_supports_window_aggregates",
            &[
                "sum(score) over",
                "range between unbounded preceding and current row",
            ],
        ),
        (
            "lambda array functions",
            "live_feldera_pipeline_manager_runtime_supports_lambda_array_functions",
            &["x -> x > 6", "array_compact"],
        ),
        (
            "interval/date-time operations",
            "live_feldera_pipeline_manager_runtime_supports_interval_datetime_operations",
            &[
                "timestampdiff(day",
                "timestampadd(day",
                "interval '1 02:03:04.005' day to second",
                "interval '2-03' year to month",
            ],
        ),
        (
            "select replace/exclude values unnest",
            "live_feldera_pipeline_manager_runtime_supports_select_replace_exclude_values_unnest",
            &[
                "select * replace",
                "select * exclude",
                "values ('u1', 3)",
                "cross join unnest",
            ],
        ),
        (
            "qualify and lateral apply",
            "live_feldera_pipeline_manager_runtime_supports_qualify_and_lateral_apply",
            &["qualify row_number() over", "cross apply"],
        ),
        (
            "rollup and cube grouping",
            "live_feldera_pipeline_manager_runtime_supports_rollup_and_cube_grouping",
            &["group by rollup", "group by cube"],
        ),
        (
            "SQL UDF Feldera program",
            "live_feldera_pipeline_manager_runtime_supports_sql_udf_programs",
            &["CREATE FUNCTION add_bonus", "CREATE MATERIALIZED VIEW"],
        ),
        (
            "Rust UDA Feldera program",
            "live_feldera_pipeline_manager_runtime_supports_rust_user_defined_aggregates",
            &[
                "CREATE LINEAR AGGREGATE signed_sum",
                "udf_rust",
                "signed_sum_accumulator_type",
                "signed_sum_map",
                "signed_sum_post",
            ],
        ),
        (
            "user-defined types and output indexes",
            "live_feldera_pipeline_manager_runtime_supports_user_defined_types_and_indexes",
            &[
                "CREATE TYPE",
                "CAST(ROW(user_id, score)",
                "CREATE INDEX",
                "typed_score",
            ],
        ),
        (
            "recursive views",
            "live_feldera_pipeline_manager_runtime_supports_recursive_views",
            &[
                "DECLARE RECURSIVE VIEW",
                "CREATE LOCAL VIEW step",
                "CREATE MATERIALIZED VIEW",
                "JOIN {output_id}",
            ],
        ),
        (
            "ASOF JOIN",
            "live_feldera_pipeline_manager_runtime_supports_asof_join",
            &["left asof join rates", "match_condition"],
        ),
        (
            "TUMBLE/HOP table functions",
            "live_feldera_pipeline_manager_runtime_supports_tumble_and_hop_table_functions",
            &[
                "table(tumble(table expanded_scalars",
                "table(hop(table expanded_scalars",
                "descriptor(event_ts)",
            ],
        ),
        (
            "expanded scalar functions and literal types",
            "live_feldera_pipeline_manager_runtime_supports_expanded_scalar_functions",
            &[
                "ceil(cast(f32_value as double))",
                "x'0A0BFF'",
                "DATE '2026-06-10'",
            ],
        ),
        (
            "left outer join",
            "live_feldera_pipeline_manager_runtime_supports_left_outer_join",
            &["left join profiles", "live_row_is_null"],
        ),
        (
            "right and full outer joins",
            "live_feldera_pipeline_manager_runtime_supports_right_and_full_outer_join",
            &["right join profiles", "full outer join"],
        ),
        (
            "correlated exists subquery",
            "live_feldera_pipeline_manager_runtime_supports_correlated_exists_subquery",
            &["where exists", "p.user_id = s.user_id"],
        ),
        (
            "complex result types",
            "live_feldera_pipeline_manager_runtime_supports_complex_feldera_sql_result_types",
            &[
                "ARRAY[score",
                "CAST(ROW",
                "CAST(score AS VARIANT)",
                "COALESCE",
            ],
        ),
        (
            "map output values",
            "live_feldera_pipeline_manager_runtime_supports_map_output_values",
            &["MAP(select user_id, score"],
        ),
        (
            "JSON/VARIANT functions",
            "live_feldera_pipeline_manager_runtime_supports_json_variant_functions",
            &[
                "VelorixLogicalTypeV1::Json",
                "ArrowPhysicalTypeV1::JsonUtf8",
                "parse_json(raw_json)",
                "variantnull()",
                "payload['scores'][1]",
            ],
        ),
    ];

    for (family, test_name, sql_fragments) in required_runtime_tests {
        assert!(
            LIVE_FELDERA_TESTS.contains(&format!("async fn {test_name}")),
            "live Feldera SQL family coverage must include runtime test `{test_name}` for {family}"
        );
        assert!(
            LIVE_FELDERA_RUNNER.contains(test_name),
            "live Feldera runner must execute runtime test `{test_name}` for {family}"
        );
        assert!(
            LIVE_FELDERA_EVIDENCE_VALIDATOR.contains(test_name),
            "live Feldera evidence validator must require runtime test `{test_name}` for {family}"
        );
        for fragment in *sql_fragments {
            assert!(
                LIVE_FELDERA_TESTS
                    .to_ascii_lowercase()
                    .contains(&fragment.to_ascii_lowercase()),
                "live Feldera runtime test `{test_name}` must cover SQL fragment `{fragment}` for {family}"
            );
        }
    }
}

#[test]
fn source_audit_live_feldera_runner_keeps_fail_closed_sql_tests() {
    let fail_closed_compile_tests: &[(&str, &str, &[&str])] = &[
        (
            "invalid SQL",
            "live_feldera_pipeline_manager_rejects_invalid_sql_without_fallback",
            &[
                "definitely_not_a_feldera_function",
                "expect_err",
                "must fail instead of falling back",
                "SqlError",
            ],
        ),
        (
            "semantic warning",
            "live_feldera_pipeline_manager_rejects_ignored_order_by_warning_without_fallback",
            &[
                "order by score desc",
                "expect_err",
                "ORDER BY ignored warning must fail",
                "ORDER BY clause is currently ignored",
            ],
        ),
        (
            "unregistered Feldera program input",
            "live_feldera_pipeline_manager_rejects_unregistered_feldera_program_input_without_deploying",
            &[
                "CREATE TABLE external_scores",
                "SqlSourceKind::FelderaProgram",
                "expect_err",
                "unregistered input relation `external_scores`",
            ],
        ),
        (
            "GEOMETRY runtime/codegen blocker",
            "live_feldera_pipeline_manager_rejects_geometry_output_until_feldera_runtime_supports_it_without_fallback",
            &[
                "cast('POINT(1 2)' as GEOMETRY)",
                "expect_err",
                "GEOMETRY output must fail closed",
                "RustError",
            ],
        ),
        (
            "two-arg TRUNC runtime/codegen blocker",
            "live_feldera_pipeline_manager_rejects_two_arg_trunc_until_feldera_runtime_supports_it_without_fallback",
            &[
                "trunc(cast(score as double), 2)",
                "expect_err",
                "two-arg TRUNC must fail closed",
                "trunc_d_i32",
            ],
        ),
        (
            "documented unsupported Feldera SQL",
            "live_feldera_pipeline_manager_rejects_documented_unsupported_sql_without_fallback",
            &[
                "INTERSECT ALL",
                "EXCEPT ALL",
                "MATCH_RECOGNIZE",
                "ROWS BETWEEN",
                "NTILE",
                "documented unsupported Feldera SQL must fail",
            ],
        ),
    ];

    for (case, test_name, required_fragments) in fail_closed_compile_tests {
        assert!(
            LIVE_FELDERA_TESTS.contains(&format!("async fn {test_name}")),
            "live Feldera fail-closed coverage must include `{test_name}` for {case}"
        );
        assert!(
            LIVE_FELDERA_RUNNER.contains(test_name),
            "live Feldera runner must execute fail-closed test `{test_name}` for {case}"
        );
        for fragment in *required_fragments {
            assert!(
                LIVE_FELDERA_TESTS.contains(fragment),
                "live Feldera fail-closed test `{test_name}` must include `{fragment}` for {case}"
            );
        }
    }
}

#[test]
fn source_audit_live_feldera_evidence_uses_runner_test_arrays() {
    assert!(
        LIVE_FELDERA_RUNNER.contains("compile_filters_json=\"$(json_array_from_args \""),
        "live Feldera evidence must serialize compile_tests from the runner array"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains("runtime_filters_json=\"$(json_array_from_args \""),
        "live Feldera evidence must serialize runtime_tests from the runner array"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains("\"compile_test_filters\": compile_filters"),
        "live Feldera evidence must write compile_test_filters from serialized runner state"
    );
    for fragment in [
        "\"evidence_scope\": \"compatibility_fixture\"",
        "\"product_evidence\": False",
        "\"backend_kind\": \"pipeline_manager\"",
        "\"jarless_backend_attested\": False",
    ] {
        assert!(
            LIVE_FELDERA_RUNNER.contains(fragment),
            "live Feldera evidence must declare compatibility-only scope with `{fragment}`"
        );
    }
    assert!(
        LIVE_FELDERA_RUNNER.contains("\"runtime_test_filters\": runtime_filters"),
        "live Feldera evidence must write runtime_test_filters from executed runtime state"
    );
    assert!(
        LIVE_FELDERA_RUNNER
            .contains("runtime_filters = available_runtime_filters if runtime_is_enabled else []"),
        "live Feldera evidence must record runtime_test_filters as executed runtime tests only"
    );
    assert!(
        LIVE_FELDERA_RUNNER
            .contains("\"executed_test_filters\": compile_filters + runtime_filters"),
        "live Feldera evidence must explicitly list executed test filters"
    );
    assert!(
        LIVE_FELDERA_RUNNER
            .contains("\"available_runtime_test_filters\": available_runtime_filters"),
        "live Feldera evidence must preserve the full available runtime coverage list"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains(
            "\"skipped_runtime_test_filters\": [] if runtime_is_enabled else available_runtime_filters"
        ),
        "live Feldera evidence must explicitly list skipped runtime tests when runtime is disabled"
    );
    for stale_hardcoded_payload in ["\"compile_test_filters\": [", "\"runtime_test_filters\": ["] {
        assert!(
            !LIVE_FELDERA_RUNNER.contains(stale_hardcoded_payload),
            "live Feldera evidence must not duplicate runner test arrays in Python payload: found `{stale_hardcoded_payload}`"
        );
    }
}

#[test]
fn source_audit_live_feldera_runner_defaults_compiler_cache_to_target_directory() {
    assert!(
        LIVE_FELDERA_RUNNER.contains(
            "compiler_cache_mode=\"${VELORIX_LIVE_FELDERA_COMPILER_CACHE_MODE:-loop}\""
        ),
        "live Feldera runner must default compiler cache to loop mode for Linux filesystem semantics"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains(
            "compiler_cache_image=\"${VELORIX_LIVE_FELDERA_COMPILER_CACHE_IMAGE:-${repo_root}/target/feldera-compiler-cache.ext4}\""
        ),
        "live Feldera runner must default the loop compiler cache backing file to repo target"
    );
    assert!(
        LIVE_FELDERA_RUNNER
            .contains("compiler_cache_volume=\"${VELORIX_LIVE_FELDERA_COMPILER_CACHE_VOLUME:-}\""),
        "live Feldera runner must not default to a Docker named compiler cache volume"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains("sudo mkfs.ext4 -F '$compiler_cache_image'"),
        "live Feldera runner must format the target-backed compiler cache image as ext4"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains(
            "-v \"${compiler_cache_source}:/home/ubuntu/.feldera/compiler/rust-compilation\""
        ),
        "live Feldera runner must mount the selected compiler cache source into Feldera"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains("trim_loop_compiler_cache()"),
        "live Feldera runner must define loop cache trimming so sparse image space is reclaimed"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains(r#"sudo fstrim \"\$base\""#),
        "live Feldera runner must fstrim the ext4 compiler cache after cleanup"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains("target/debug/.fingerprint/feldera_pipe_*"),
        "live Feldera runner must clear generated crate fingerprints to avoid stale link artifacts"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains("target/debug/incremental/*"),
        "live Feldera runner must clear generated incremental state after runtime cases"
    );
}

#[test]
fn source_audit_live_feldera_runner_writes_failure_evidence() {
    assert!(
        LIVE_FELDERA_RUNNER.contains("trap on_exit EXIT"),
        "live Feldera runner must trap script exits and write failure evidence"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains("if [ \"$exit_code\" -ne 0 ]; then"),
        "live Feldera runner must only write failure evidence for nonzero exits"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains("write_failure_evidence \"$exit_code\""),
        "live Feldera runner must route nonzero exits through evidence writer"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains("if [ \"$exit_code\" -eq 75 ]; then"),
        "live Feldera runner must classify existing preflight exit 75 as blocked"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains("\"status\": status"),
        "live Feldera evidence must include status"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains("\"exit_code\": int(exit_code)"),
        "live Feldera evidence must include exit_code"
    );
    assert!(
        LIVE_FELDERA_RUNNER
            .contains("\"failure_kind\": \"local_environment_blocker\" if status == \"blocked\""),
        "live Feldera blocked evidence must be distinguishable from test failure evidence"
    );
    assert!(
        LIVE_FELDERA_RUNNER.contains("evidence_file=\"$(write_evidence passed 0)\""),
        "live Feldera runner must write passed evidence with exit code 0"
    );
}

#[test]
fn source_audit_live_feldera_evidence_validator_fails_closed() {
    for required in [
        "EXPECTED_KIND = \"velorix_live_feldera_pipeline_manager_evidence\"",
        "REQUIRED_COMPILE_FILTERS",
        "REQUIRED_RUNTIME_FILTERS",
        "full runtime evidence is required but runtime_enabled is false",
        "exit_code=65",
        "live Feldera evidence is blocked by the local environment",
        "exit_code=75",
        "executed_test_filters must equal compile_test_filters + runtime_test_filters",
    ] {
        assert!(
            LIVE_FELDERA_EVIDENCE_VALIDATOR.contains(required),
            "live Feldera evidence validator must keep fail-closed contract fragment `{required}`"
        );
    }
}

#[test]
fn source_audit_live_feldera_evidence_validator_classifies_sample_evidence() {
    let temp_dir = std::env::temp_dir().join(format!(
        "velorix-live-feldera-evidence-validator-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).unwrap();

    let compile_only = temp_dir.join("compile-only.json");
    let full_runtime = temp_dir.join("full-runtime.json");
    let blocked = temp_dir.join("blocked.json");
    fs::write(
        &compile_only,
        sample_live_feldera_evidence(false, "passed", 0, None),
    )
    .unwrap();
    fs::write(
        &full_runtime,
        sample_live_feldera_evidence(true, "passed", 0, None),
    )
    .unwrap();
    fs::write(
        &blocked,
        sample_live_feldera_evidence(false, "blocked", 75, Some("local_environment_blocker")),
    )
    .unwrap();

    assert_validator_exit(&compile_only, &[], 0);
    assert_validator_exit(&compile_only, &["--require-runtime"], 65);
    assert_validator_exit(&full_runtime, &["--require-runtime"], 0);
    assert_validator_exit(&blocked, &[], 75);

    let _ = fs::remove_dir_all(&temp_dir);
}

fn assert_validator_exit(evidence_path: &PathBuf, args: &[&str], expected_code: i32) {
    let output = Command::new("python3")
        .arg(live_feldera_evidence_validator_path())
        .args(args)
        .arg(evidence_path)
        .arg("--json")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(expected_code),
        "validator stdout: {}\nvalidator stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn live_feldera_evidence_validator_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/validate-live-feldera-evidence.py")
}

fn sample_live_feldera_evidence(
    runtime_enabled: bool,
    status: &str,
    exit_code: u16,
    failure_kind: Option<&str>,
) -> String {
    let compile_filters = [
        "live_feldera_pipeline_manager_compiles",
        "live_feldera_pipeline_manager_rejects_invalid_sql_without_fallback",
        "live_feldera_pipeline_manager_rejects_ignored_order_by_warning_without_fallback",
        "live_feldera_pipeline_manager_rejects_unregistered_feldera_program_input_without_deploying",
        "live_feldera_pipeline_manager_rejects_geometry_output_until_feldera_runtime_supports_it_without_fallback",
        "live_feldera_pipeline_manager_rejects_two_arg_trunc_until_feldera_runtime_supports_it_without_fallback",
        "live_feldera_pipeline_manager_rejects_documented_unsupported_sql_without_fallback",
    ];
    let runtime_filters = [
        "live_feldera_pipeline_manager_runtime_ingests_and_queries_velorix_program",
        "live_feldera_pipeline_manager_runtime_supports_feldera_program_multi_output",
        "live_feldera_pipeline_manager_runtime_pages_materialized_and_sql_queries",
        "live_feldera_pipeline_manager_runtime_deletes_local_volatile_pipeline_on_drop",
        "live_feldera_pipeline_manager_runtime_supports_projection_and_filter",
        "live_feldera_pipeline_manager_runtime_supports_min_max_avg_aggregates",
        "live_feldera_pipeline_manager_runtime_supports_cte_having_union",
        "live_feldera_pipeline_manager_runtime_supports_distinct_intersect_except",
        "live_feldera_pipeline_manager_runtime_supports_scalar_string_and_math_functions",
        "live_feldera_pipeline_manager_runtime_supports_string_binary_hash_functions",
        "live_feldera_pipeline_manager_runtime_supports_floating_numeric_functions",
        "live_feldera_pipeline_manager_runtime_supports_computed_grouping_expressions",
        "live_feldera_pipeline_manager_runtime_supports_lateral_column_aliasing",
        "live_feldera_pipeline_manager_runtime_supports_between_in_and_like_predicates",
        "live_feldera_pipeline_manager_runtime_supports_distinct_aggregates",
        "live_feldera_pipeline_manager_runtime_supports_advanced_aggregates",
        "live_feldera_pipeline_manager_runtime_supports_pivot_aggregates",
        "live_feldera_pipeline_manager_runtime_supports_unpivot_and_join_using",
        "live_feldera_pipeline_manager_runtime_supports_window_row_number",
        "live_feldera_pipeline_manager_runtime_supports_scalar_subqueries",
        "live_feldera_pipeline_manager_runtime_supports_window_aggregates",
        "live_feldera_pipeline_manager_runtime_supports_lambda_array_functions",
        "live_feldera_pipeline_manager_runtime_supports_interval_datetime_operations",
        "live_feldera_pipeline_manager_runtime_supports_select_replace_exclude_values_unnest",
        "live_feldera_pipeline_manager_runtime_supports_qualify_and_lateral_apply",
        "live_feldera_pipeline_manager_runtime_supports_rollup_and_cube_grouping",
        "live_feldera_pipeline_manager_runtime_supports_sql_udf_programs",
        "live_feldera_pipeline_manager_runtime_supports_rust_user_defined_aggregates",
        "live_feldera_pipeline_manager_runtime_supports_user_defined_types_and_indexes",
        "live_feldera_pipeline_manager_runtime_supports_recursive_views",
        "live_feldera_pipeline_manager_runtime_supports_asof_join",
        "live_feldera_pipeline_manager_runtime_supports_tumble_and_hop_table_functions",
        "live_feldera_pipeline_manager_runtime_supports_expanded_scalar_functions",
        "live_feldera_pipeline_manager_runtime_supports_two_table_join",
        "live_feldera_pipeline_manager_runtime_supports_left_outer_join",
        "live_feldera_pipeline_manager_runtime_supports_right_and_full_outer_join",
        "live_feldera_pipeline_manager_runtime_supports_correlated_exists_subquery",
        "live_feldera_pipeline_manager_runtime_supports_complex_feldera_sql_result_types",
        "live_feldera_pipeline_manager_runtime_supports_map_output_values",
        "live_feldera_pipeline_manager_runtime_supports_json_variant_functions",
        "live_feldera_pipeline_manager_rest_api_compiles_ingests_and_queries_join_view",
        "live_feldera_pipeline_manager_rest_api_ingests_and_queries_nested_input_view",
        "live_feldera_pipeline_manager_rest_api_supports_feldera_program_multi_output",
        "live_feldera_pipeline_manager_rest_api_discovers_feldera_program_outputs_without_hints",
        "live_feldera_pipeline_manager_rest_api_supports_raw_sql_query_on_output_endpoint",
        "live_feldera_pipeline_manager_rest_api_supports_array_query_parameter",
        "live_feldera_pipeline_manager_rest_api_supports_typed_literal_query_parameters",
        "live_feldera_pipeline_manager_rest_api_supports_typed_array_query_parameters",
        "live_feldera_pipeline_manager_rest_api_supports_json_query_parameter",
        "live_feldera_pipeline_manager_rest_api_paginates_promoted_sql_template",
    ];
    let executed_filters = if runtime_enabled {
        compile_filters
            .iter()
            .chain(runtime_filters.iter())
            .copied()
            .collect::<Vec<_>>()
    } else {
        compile_filters.to_vec()
    };
    let active_runtime_filters = if runtime_enabled {
        runtime_filters.to_vec()
    } else {
        Vec::new()
    };
    let skipped_runtime_filters = if runtime_enabled {
        Vec::new()
    } else {
        runtime_filters.to_vec()
    };
    format!(
        r#"{{
  "evidence_kind": "velorix_live_feldera_pipeline_manager_evidence",
  "schema_version": 1,
  "evidence_scope": "compatibility_fixture",
  "product_evidence": false,
  "backend_kind": "pipeline_manager",
  "backend_image": "unknown_external",
  "backend_image_digest": "unknown_external",
  "official_image_allowed": false,
  "jarless_backend_attested": false,
  "status": "{status}",
  "exit_code": {exit_code},
  "failure_kind": {failure_kind},
  "run_id": "sample",
  "pipeline_manager_url": "http://127.0.0.1:18082",
  "runtime_enabled": {runtime_enabled},
  "cargo_target_dir": "cargo-default",
  "compile_test_filters": {compile_filters},
  "runtime_test_filters": {active_runtime_filters},
  "executed_test_filters": {executed_filters},
  "available_runtime_test_filters": {runtime_filters},
  "skipped_runtime_test_filters": {skipped_runtime_filters}
}}"#,
        failure_kind = json_optional_string(failure_kind),
        compile_filters = json_string_array(&compile_filters),
        active_runtime_filters = json_string_array(&active_runtime_filters),
        executed_filters = json_string_array(&executed_filters),
        runtime_filters = json_string_array(&runtime_filters),
        skipped_runtime_filters = json_string_array(&skipped_runtime_filters),
    )
}

fn json_optional_string(value: Option<&str>) -> String {
    value
        .map(|value| format!(r#""{value}""#))
        .unwrap_or_else(|| "null".to_string())
}

fn json_string_array(values: &[&str]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format!(r#""{value}""#))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn occurrence_indices<'a>(source: &'a str, needle: &'a str) -> impl Iterator<Item = usize> + 'a {
    source.match_indices(needle).map(|(index, _)| index)
}

fn line_at(source: &str, byte_index: usize) -> &str {
    let start = source[..byte_index]
        .rfind('\n')
        .map_or(0, |index| index + 1);
    let end = source[byte_index..]
        .find('\n')
        .map_or(source.len(), |index| byte_index + index);
    &source[start..end]
}

fn line_number_at(source: &str, byte_index: usize) -> usize {
    source[..byte_index]
        .bytes()
        .filter(|byte| *byte == b'\n')
        .count()
        + 1
}

fn enclosing_function_name(source: &str, byte_index: usize) -> Option<String> {
    source[..byte_index]
        .lines()
        .rev()
        .find_map(function_name_from_line)
}

fn function_name_from_line(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let rest = trimmed
        .strip_prefix("async fn ")
        .or_else(|| trimmed.strip_prefix("fn "))?;
    let name = rest
        .split(|ch: char| !(ch == '_' || ch.is_ascii_alphanumeric()))
        .next()?;
    (!name.is_empty()).then(|| name.to_string())
}
