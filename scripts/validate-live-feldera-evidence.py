#!/usr/bin/env python3
"""Validate live Feldera pipeline-manager evidence JSON."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


EXPECTED_KIND = "velorix_live_feldera_pipeline_manager_evidence"
EXPECTED_SCHEMA_VERSION = 1

REQUIRED_COMPILE_FILTERS = {
    "live_feldera_pipeline_manager_compiles",
    "live_feldera_pipeline_manager_rejects_invalid_sql_without_fallback",
    "live_feldera_pipeline_manager_rejects_ignored_order_by_warning_without_fallback",
    "live_feldera_pipeline_manager_rejects_unregistered_feldera_program_input_without_deploying",
    "live_feldera_pipeline_manager_rejects_geometry_output_until_feldera_runtime_supports_it_without_fallback",
    "live_feldera_pipeline_manager_rejects_two_arg_trunc_until_feldera_runtime_supports_it_without_fallback",
    "live_feldera_pipeline_manager_rejects_documented_unsupported_sql_without_fallback",
}

REQUIRED_RUNTIME_FILTERS = {
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
}


class EvidenceError(Exception):
    def __init__(self, message: str, exit_code: int = 64) -> None:
        super().__init__(message)
        self.exit_code = exit_code


def string_list(data: dict[str, Any], field: str) -> list[str]:
    value = data.get(field)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise EvidenceError(f"{field} must be an array of strings")
    if len(value) != len(set(value)):
        raise EvidenceError(f"{field} must not contain duplicate entries")
    return value


def require_subset(name: str, required: set[str], actual: list[str]) -> None:
    missing = sorted(required.difference(actual))
    if missing:
        raise EvidenceError(f"{name} is missing required filters: {missing}")


def validate_evidence(data: dict[str, Any], require_runtime: bool) -> dict[str, Any]:
    if data.get("evidence_kind") != EXPECTED_KIND:
        raise EvidenceError(f"evidence_kind must be {EXPECTED_KIND}")
    if data.get("schema_version") != EXPECTED_SCHEMA_VERSION:
        raise EvidenceError(f"schema_version must be {EXPECTED_SCHEMA_VERSION}")
    if data.get("evidence_scope") != "compatibility_fixture":
        raise EvidenceError("evidence_scope must be compatibility_fixture")
    if data.get("product_evidence") is not False:
        raise EvidenceError("live Feldera pipeline-manager evidence must declare product_evidence=false")
    if data.get("backend_kind") != "pipeline_manager":
        raise EvidenceError("backend_kind must be pipeline_manager")
    if data.get("jarless_backend_attested") is not False:
        raise EvidenceError("pipeline-manager evidence must declare jarless_backend_attested=false")
    backend_image = data.get("backend_image")
    if not isinstance(backend_image, str) or not backend_image.strip():
        raise EvidenceError("backend_image must be a non-empty string")
    backend_image_digest = data.get("backend_image_digest")
    if not isinstance(backend_image_digest, str) or not backend_image_digest.strip():
        raise EvidenceError("backend_image_digest must be a non-empty string")
    official_image_allowed = data.get("official_image_allowed")
    if not isinstance(official_image_allowed, bool):
        raise EvidenceError("official_image_allowed must be a boolean")

    status = data.get("status")
    if status not in {"passed", "blocked", "failed"}:
        raise EvidenceError("status must be one of passed, blocked, failed")
    exit_code = data.get("exit_code")
    if not isinstance(exit_code, int) or exit_code < 0:
        raise EvidenceError("exit_code must be a non-negative integer")
    runtime_enabled = data.get("runtime_enabled")
    if not isinstance(runtime_enabled, bool):
        raise EvidenceError("runtime_enabled must be a boolean")

    compile_filters = string_list(data, "compile_test_filters")
    runtime_filters = string_list(data, "runtime_test_filters")
    executed_filters = string_list(data, "executed_test_filters")
    available_runtime_filters = string_list(data, "available_runtime_test_filters")
    skipped_runtime_filters = string_list(data, "skipped_runtime_test_filters")

    require_subset("compile_test_filters", REQUIRED_COMPILE_FILTERS, compile_filters)
    require_subset(
        "available_runtime_test_filters",
        REQUIRED_RUNTIME_FILTERS,
        available_runtime_filters,
    )

    expected_runtime_filters = available_runtime_filters if runtime_enabled else []
    if runtime_filters != expected_runtime_filters:
        raise EvidenceError(
            "runtime_test_filters must equal available runtime filters only when runtime_enabled is true"
        )
    expected_skipped_runtime = [] if runtime_enabled else available_runtime_filters
    if skipped_runtime_filters != expected_skipped_runtime:
        raise EvidenceError(
            "skipped_runtime_test_filters must be empty only when runtime_enabled is true"
        )
    if executed_filters != compile_filters + runtime_filters:
        raise EvidenceError("executed_test_filters must equal compile_test_filters + runtime_test_filters")

    failure_kind = data.get("failure_kind")
    if status == "passed":
        if exit_code != 0:
            raise EvidenceError("passed evidence must have exit_code 0")
        if failure_kind is not None:
            raise EvidenceError("passed evidence must have null failure_kind")
        if require_runtime and not runtime_enabled:
            raise EvidenceError(
                "full runtime evidence is required but runtime_enabled is false",
                exit_code=65,
            )
    elif status == "blocked":
        if exit_code != 75:
            raise EvidenceError("blocked evidence must have exit_code 75")
        if failure_kind != "local_environment_blocker":
            raise EvidenceError("blocked evidence must have failure_kind local_environment_blocker")
        raise EvidenceError("live Feldera evidence is blocked by the local environment", exit_code=75)
    else:
        if exit_code == 0:
            raise EvidenceError("failed evidence must have nonzero exit_code")
        if failure_kind != "test_failure":
            raise EvidenceError("failed evidence must have failure_kind test_failure")
        raise EvidenceError("live Feldera evidence records a failed run", exit_code=1)

    return {
        "valid": True,
        "status": status,
        "runtime_enabled": runtime_enabled,
        "executed_test_count": len(executed_filters),
        "available_runtime_test_count": len(available_runtime_filters),
        "skipped_runtime_test_count": len(skipped_runtime_filters),
    }


def load_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise EvidenceError(f"could not read evidence file: {error}") from error
    except json.JSONDecodeError as error:
        raise EvidenceError(f"evidence file is not valid JSON: {error}") from error
    if not isinstance(data, dict):
        raise EvidenceError("evidence JSON must be an object")
    return data


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path, help="live Feldera evidence JSON path")
    parser.add_argument(
        "--require-runtime",
        action="store_true",
        help="require a passed full-runtime evidence record",
    )
    parser.add_argument("--json", action="store_true", help="print a JSON validation report")
    args = parser.parse_args()

    try:
        report = validate_evidence(load_json(args.evidence), args.require_runtime)
    except EvidenceError as error:
        report = {
            "valid": False,
            "error": str(error),
            "exit_code": error.exit_code,
        }
        if args.json:
            print(json.dumps(report, indent=2, sort_keys=True))
        else:
            print(str(error), file=sys.stderr)
        return error.exit_code

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print("live Feldera evidence is valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
