#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
base_url="${VELORIX_FELDERA_PIPELINE_MANAGER_URL:-http://127.0.0.1:18082}"
runtime_enabled="${LIVE_FELDERA_RUNTIME:-${VELORIX_LIVE_FELDERA_RUNTIME:-0}}"
docker_context="${VELORIX_LIVE_FELDERA_DOCKER_CONTEXT:-colima-velorix-live}"
colima_profile="${VELORIX_LIVE_FELDERA_COLIMA_PROFILE:-velorix-live}"
container="${VELORIX_LIVE_FELDERA_CONTAINER:-velorix-feldera-live}"
compiler_cache_mode="${VELORIX_LIVE_FELDERA_COMPILER_CACHE_MODE:-loop}"
compiler_cache_dir="${VELORIX_LIVE_FELDERA_COMPILER_CACHE_DIR:-${repo_root}/target/feldera-compiler-cache}"
compiler_cache_image="${VELORIX_LIVE_FELDERA_COMPILER_CACHE_IMAGE:-${repo_root}/target/feldera-compiler-cache.ext4}"
compiler_cache_image_size="${VELORIX_LIVE_FELDERA_COMPILER_CACHE_IMAGE_SIZE:-80G}"
compiler_cache_mountpoint="${VELORIX_LIVE_FELDERA_COMPILER_CACHE_MOUNTPOINT:-/mnt/velorix-feldera-compiler-cache}"
compiler_cache_volume="${VELORIX_LIVE_FELDERA_COMPILER_CACHE_VOLUME:-}"
image="${VELORIX_LIVE_FELDERA_IMAGE:-}"
allow_official_image="${VELORIX_LIVE_FELDERA_ALLOW_OFFICIAL_IMAGE:-0}"
host_port="${VELORIX_LIVE_FELDERA_HOST_PORT:-18082}"
start_container="${VELORIX_LIVE_FELDERA_START_CONTAINER:-auto}"
clean_between_runtime_tests="${VELORIX_LIVE_FELDERA_CLEAN_BETWEEN_RUNTIME_TESTS:-1}"
clean_stale_pipelines="${VELORIX_LIVE_FELDERA_CLEAN_STALE_PIPELINES:-1}"
wait_seconds="${VELORIX_LIVE_FELDERA_WAIT_SECONDS:-120}"
min_free_kib="${VELORIX_LIVE_FELDERA_MIN_FREE_KIB:-8388608}"
min_cache_free_kib="${VELORIX_LIVE_FELDERA_MIN_CACHE_FREE_KIB:-12582912}"
cargo_target_dir="${CARGO_TARGET_DIR:-}"
evidence_dir="${VELORIX_LIVE_FELDERA_EVIDENCE_DIR:-${repo_root}/target/velorix-feldera-live/${run_id}}"
evidence_path="${VELORIX_LIVE_FELDERA_EVIDENCE_PATH:-${evidence_dir}/live-feldera-pipeline-manager-evidence.json}"

compile_tests=(
  live_feldera_pipeline_manager_compiles
  live_feldera_pipeline_manager_rejects_invalid_sql_without_fallback
  live_feldera_pipeline_manager_rejects_ignored_order_by_warning_without_fallback
  live_feldera_pipeline_manager_rejects_unregistered_feldera_program_input_without_deploying
  live_feldera_pipeline_manager_rejects_geometry_output_until_feldera_runtime_supports_it_without_fallback
  live_feldera_pipeline_manager_rejects_two_arg_trunc_until_feldera_runtime_supports_it_without_fallback
  live_feldera_pipeline_manager_rejects_documented_unsupported_sql_without_fallback
)

runtime_tests=(
  live_feldera_pipeline_manager_runtime_ingests_and_queries_velorix_program
  live_feldera_pipeline_manager_runtime_supports_feldera_program_multi_output
  live_feldera_pipeline_manager_runtime_pages_materialized_and_sql_queries
  live_feldera_pipeline_manager_runtime_deletes_local_volatile_pipeline_on_drop
  live_feldera_pipeline_manager_runtime_supports_projection_and_filter
  live_feldera_pipeline_manager_runtime_supports_min_max_avg_aggregates
  live_feldera_pipeline_manager_runtime_supports_cte_having_union
  live_feldera_pipeline_manager_runtime_supports_distinct_intersect_except
  live_feldera_pipeline_manager_runtime_supports_scalar_string_and_math_functions
  live_feldera_pipeline_manager_runtime_supports_string_binary_hash_functions
  live_feldera_pipeline_manager_runtime_supports_floating_numeric_functions
  live_feldera_pipeline_manager_runtime_supports_computed_grouping_expressions
  live_feldera_pipeline_manager_runtime_supports_lateral_column_aliasing
  live_feldera_pipeline_manager_runtime_supports_between_in_and_like_predicates
  live_feldera_pipeline_manager_runtime_supports_distinct_aggregates
  live_feldera_pipeline_manager_runtime_supports_advanced_aggregates
  live_feldera_pipeline_manager_runtime_supports_pivot_aggregates
  live_feldera_pipeline_manager_runtime_supports_unpivot_and_join_using
  live_feldera_pipeline_manager_runtime_supports_window_row_number
  live_feldera_pipeline_manager_runtime_supports_scalar_subqueries
  live_feldera_pipeline_manager_runtime_supports_window_aggregates
  live_feldera_pipeline_manager_runtime_supports_lambda_array_functions
  live_feldera_pipeline_manager_runtime_supports_interval_datetime_operations
  live_feldera_pipeline_manager_runtime_supports_select_replace_exclude_values_unnest
  live_feldera_pipeline_manager_runtime_supports_qualify_and_lateral_apply
  live_feldera_pipeline_manager_runtime_supports_rollup_and_cube_grouping
  live_feldera_pipeline_manager_runtime_supports_sql_udf_programs
  live_feldera_pipeline_manager_runtime_supports_rust_user_defined_aggregates
  live_feldera_pipeline_manager_runtime_supports_user_defined_types_and_indexes
  live_feldera_pipeline_manager_runtime_supports_recursive_views
  live_feldera_pipeline_manager_runtime_supports_asof_join
  live_feldera_pipeline_manager_runtime_supports_tumble_and_hop_table_functions
  live_feldera_pipeline_manager_runtime_supports_expanded_scalar_functions
  live_feldera_pipeline_manager_runtime_supports_two_table_join
  live_feldera_pipeline_manager_runtime_supports_left_outer_join
  live_feldera_pipeline_manager_runtime_supports_right_and_full_outer_join
  live_feldera_pipeline_manager_runtime_supports_correlated_exists_subquery
  live_feldera_pipeline_manager_runtime_supports_complex_feldera_sql_result_types
  live_feldera_pipeline_manager_runtime_supports_map_output_values
  live_feldera_pipeline_manager_runtime_supports_json_variant_functions
  live_feldera_pipeline_manager_rest_api_compiles_ingests_and_queries_join_view
  live_feldera_pipeline_manager_rest_api_ingests_and_queries_nested_input_view
  live_feldera_pipeline_manager_rest_api_supports_feldera_program_multi_output
  live_feldera_pipeline_manager_rest_api_discovers_feldera_program_outputs_without_hints
  live_feldera_pipeline_manager_rest_api_supports_raw_sql_query_on_output_endpoint
  live_feldera_pipeline_manager_rest_api_supports_array_query_parameter
  live_feldera_pipeline_manager_rest_api_supports_typed_literal_query_parameters
  live_feldera_pipeline_manager_rest_api_supports_typed_array_query_parameters
  live_feldera_pipeline_manager_rest_api_supports_json_query_parameter
  live_feldera_pipeline_manager_rest_api_paginates_promoted_sql_template
)

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

usage() {
  cat <<'EOF'
Usage:
  scripts/run-live-feldera-pipeline-manager.sh

Environment:
  VELORIX_FELDERA_PIPELINE_MANAGER_URL
    Feldera pipeline-manager URL. Default: http://127.0.0.1:18082

  LIVE_FELDERA_RUNTIME=1
    Run runtime ingest/query/checkpoint tests as well as compile/schema tests.
    Without this, only compile/schema compatibility tests run.

  VELORIX_LIVE_FELDERA_START_CONTAINER=auto|0|1
    auto: use an already reachable URL; otherwise start the configured Docker
    context/container when available. Default: auto.

  VELORIX_LIVE_FELDERA_DOCKER_CONTEXT
    Docker context for the dedicated Feldera container. Default:
    colima-velorix-live.

  VELORIX_LIVE_FELDERA_COLIMA_PROFILE
    Colima profile used to mount the target-backed ext4 compiler cache image.
    Default: velorix-live.

  VELORIX_LIVE_FELDERA_CLEAN_BETWEEN_RUNTIME_TESTS=1
    Remove stale Feldera compiled pipeline binaries between runtime test cases.
    Default: 1. Runtime tests run before compile-only tests so this cleanup does
    not invalidate compile-only background Rust compilation.

  VELORIX_LIVE_FELDERA_COMPILER_CACHE_DIR
    Host directory used only when VELORIX_LIVE_FELDERA_COMPILER_CACHE_MODE=bind.
    Default: target/feldera-compiler-cache under this repository.

  VELORIX_LIVE_FELDERA_COMPILER_CACHE_MODE=loop|bind
    Local target-backed compiler cache mode. Default: loop. loop creates an
    ext4 sparse image at target/feldera-compiler-cache.ext4, mounts it inside
    the Colima VM, and gives Feldera Linux filesystem semantics while keeping
    the backing store in this repository's target directory. bind mounts
    VELORIX_LIVE_FELDERA_COMPILER_CACHE_DIR directly and is useful only for
    lightweight compile-only checks.

  VELORIX_LIVE_FELDERA_COMPILER_CACHE_IMAGE
    Sparse ext4 image path for loop mode. Default:
    target/feldera-compiler-cache.ext4 under this repository.

  VELORIX_LIVE_FELDERA_COMPILER_CACHE_IMAGE_SIZE
    Size for a newly created loop-mode compiler cache image. Default: 80G.

  VELORIX_LIVE_FELDERA_COMPILER_CACHE_VOLUME
    Optional legacy Docker named volume to mount instead of
    VELORIX_LIVE_FELDERA_COMPILER_CACHE_DIR. Prefer the target-backed directory
    for local full-runtime runs.

  VELORIX_LIVE_FELDERA_IMAGE
    Optional Feldera pipeline-manager backend image for compatibility fixture
    runs. There is no default image because the upstream pipeline-manager path
    requires the SQL compiler jar, which is not the Velorix product target.

  VELORIX_LIVE_FELDERA_ALLOW_OFFICIAL_IMAGE=1
    Allow VELORIX_LIVE_FELDERA_IMAGE to point at the upstream all-in-one
    pipeline-manager image for compatibility checks. This must not be used as
    product serving-image evidence.

  VELORIX_LIVE_FELDERA_CLEAN_STALE_PIPELINES=1
    Stop, clear, and delete stale local Velorix pipeline-manager pipelines whose
    names start with velorix-. This keeps repeated local runs from accumulating
    running local volatile pipelines.

  VELORIX_LIVE_FELDERA_MIN_CACHE_FREE_KIB
    Minimum free space required inside the Feldera compiler cache filesystem
    after cleanup. Default: 12582912 (12 GiB).
EOF
}

case "${1:-}" in
  -h | --help)
    usage
    exit 0
    ;;
esac

case "$runtime_enabled" in
  0 | 1 | true | TRUE | True | false | FALSE | False) ;;
  *)
    echo "LIVE_FELDERA_RUNTIME must be 0/1/true/false" >&2
    exit 64
    ;;
esac

case "$start_container" in
  auto | 0 | 1) ;;
  *)
    echo "VELORIX_LIVE_FELDERA_START_CONTAINER must be auto, 0, or 1" >&2
    exit 64
    ;;
esac

case "$clean_between_runtime_tests" in
  0 | 1) ;;
  *)
    echo "VELORIX_LIVE_FELDERA_CLEAN_BETWEEN_RUNTIME_TESTS must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$clean_stale_pipelines" in
  0 | 1) ;;
  *)
    echo "VELORIX_LIVE_FELDERA_CLEAN_STALE_PIPELINES must be 0 or 1" >&2
    exit 64
    ;;
esac

case "$compiler_cache_mode" in
  loop | bind) ;;
  *)
    echo "VELORIX_LIVE_FELDERA_COMPILER_CACHE_MODE must be loop or bind" >&2
    exit 64
    ;;
esac

case "$allow_official_image" in
  0 | 1) ;;
  *)
    echo "VELORIX_LIVE_FELDERA_ALLOW_OFFICIAL_IMAGE must be 0 or 1" >&2
    exit 64
    ;;
esac

if [ -z "$image" ] && [ "$start_container" != "0" ]; then
  echo "VELORIX_LIVE_FELDERA_IMAGE is not set." >&2
  echo "The live pipeline-manager runner has no default image because the upstream path requires the SQL compiler jar. Start a jarless Velorix/Feldera backend yourself and set VELORIX_LIVE_FELDERA_START_CONTAINER=0, or set VELORIX_LIVE_FELDERA_IMAGE explicitly for a compatibility fixture." >&2
  exit 64
fi

if [[ "$image" == images.feldera.com/feldera/pipeline-manager* ]] && [ "$allow_official_image" != "1" ]; then
  echo "Refusing upstream Feldera all-in-one image by default: ${image}" >&2
  echo "The upstream image uses the SQL compiler jar path. Set VELORIX_LIVE_FELDERA_ALLOW_OFFICIAL_IMAGE=1 only for an explicit compatibility fixture run." >&2
  exit 64
fi

case "$min_free_kib" in
  '' | *[!0-9]*)
    echo "VELORIX_LIVE_FELDERA_MIN_FREE_KIB must be a positive integer" >&2
    exit 64
    ;;
  0)
    echo "VELORIX_LIVE_FELDERA_MIN_FREE_KIB must be greater than zero" >&2
    exit 64
    ;;
esac

case "$min_cache_free_kib" in
  '' | *[!0-9]*)
    echo "VELORIX_LIVE_FELDERA_MIN_CACHE_FREE_KIB must be a positive integer" >&2
    exit 64
    ;;
  0)
    echo "VELORIX_LIVE_FELDERA_MIN_CACHE_FREE_KIB must be greater than zero" >&2
    exit 64
    ;;
esac

runtime_truthy() {
  case "$runtime_enabled" in
    1 | true | TRUE | True) return 0 ;;
    *) return 1 ;;
  esac
}

docker_ctx() {
  docker --context "$docker_context" "$@"
}

docker_available() {
  command -v docker >/dev/null 2>&1
}

pipeline_manager_ready() {
  curl -fsS "${base_url%/}/v0/pipelines" >/dev/null 2>&1
}

preflight_disk_space() {
  local available_kib
  available_kib="$(df -k "$repo_root" | awk 'NR == 2 { print $4 }')"
  if [ -z "$available_kib" ]; then
    echo "could not determine available disk space for ${repo_root}" >&2
    exit 1
  fi
  if [ "$available_kib" -lt "$min_free_kib" ]; then
    echo "insufficient host disk space for live Feldera tests: available_kib=${available_kib} required_kib=${min_free_kib}" >&2
    echo "Free disk space or explicitly lower VELORIX_LIVE_FELDERA_MIN_FREE_KIB after reviewing the risk." >&2
    exit 75
  fi
}

docker_context_available() {
  docker_available || return 1
  docker context inspect "$docker_context" >/dev/null 2>&1
}

colima_available() {
  command -v colima >/dev/null 2>&1
}

colima_ssh() {
  colima ssh -p "$colima_profile" -- "$@"
}

setup_loop_compiler_cache() {
  if ! colima_available; then
    echo "Colima is required for loop compiler cache mode." >&2
    echo "Install Colima, set VELORIX_LIVE_FELDERA_COMPILER_CACHE_MODE=bind for compile-only checks, or set VELORIX_LIVE_FELDERA_COMPILER_CACHE_VOLUME for the legacy Docker volume path." >&2
    exit 1
  fi
  mkdir -p "$(dirname "$compiler_cache_image")"
  if [ ! -f "$compiler_cache_image" ]; then
    truncate -s "$compiler_cache_image_size" "$compiler_cache_image"
  fi
  colima_ssh bash -lc "set -e
    sudo mkdir -p '$compiler_cache_mountpoint'
    if ! sudo blkid '$compiler_cache_image' >/dev/null 2>&1; then
      sudo mkfs.ext4 -F '$compiler_cache_image' >/dev/null
    fi
    if ! mountpoint -q '$compiler_cache_mountpoint'; then
      sudo mount -o loop '$compiler_cache_image' '$compiler_cache_mountpoint'
    fi
    sudo chown 1000:1000 '$compiler_cache_mountpoint'"
}

compiler_cache_mount_source() {
  if [ -n "$compiler_cache_volume" ]; then
    printf '%s\n' "$compiler_cache_volume"
  elif [ "$compiler_cache_mode" = "loop" ]; then
    setup_loop_compiler_cache
    printf '%s\n' "$compiler_cache_mountpoint"
  else
    mkdir -p "$compiler_cache_dir"
    printf '%s\n' "$compiler_cache_dir"
  fi
}

reset_local_container_for_runtime() {
  if ! runtime_truthy; then
    return 0
  fi
  if [ "$start_container" = "0" ]; then
    return 0
  fi
  if ! docker_context_available; then
    return 0
  fi
  if docker_ctx container inspect "$container" >/dev/null 2>&1; then
    docker_ctx rm -f "$container" >/dev/null
  fi
}

ensure_feldera_container() {
  if pipeline_manager_ready; then
    echo "Feldera pipeline-manager is reachable at ${base_url}"
    return 0
  fi

  if [ "$start_container" = "0" ]; then
    echo "Feldera pipeline-manager is not reachable at ${base_url}" >&2
    echo "Start it manually or set VELORIX_LIVE_FELDERA_START_CONTAINER=auto/1." >&2
    exit 1
  fi

  if ! docker_available; then
    echo "Docker is required to start a local Feldera pipeline-manager container." >&2
    echo "Install Docker or start Feldera manually and set VELORIX_LIVE_FELDERA_START_CONTAINER=0." >&2
    exit 1
  fi

  if [ -z "$image" ]; then
    echo "VELORIX_LIVE_FELDERA_IMAGE is required when this script starts a container." >&2
    echo "For Velorix product work, prefer a jarless backend started separately with VELORIX_LIVE_FELDERA_START_CONTAINER=0." >&2
    exit 64
  fi

  if ! docker_context_available; then
    echo "Docker context ${docker_context} is not available." >&2
    echo "Create a dedicated local profile first, for example:" >&2
    echo "  colima start --profile velorix-live --cpus 4 --memory 8 --disk 160 --runtime docker --activate=false" >&2
    exit 1
  fi

  if [ -n "$compiler_cache_volume" ]; then
    docker_ctx volume create "$compiler_cache_volume" >/dev/null
  fi
  local compiler_cache_source
  compiler_cache_source="$(compiler_cache_mount_source)"

  if docker_ctx container inspect "$container" >/dev/null 2>&1; then
    local state
    state="$(docker_ctx inspect -f '{{.State.Status}}' "$container" 2>/dev/null || true)"
    if [ "$state" != "running" ]; then
      docker_ctx start "$container" >/dev/null
    fi
  else
    docker_ctx run -d \
      --name "$container" \
      -p "${host_port}:8080" \
      -v "${compiler_cache_source}:/home/ubuntu/.feldera/compiler/rust-compilation" \
      "$image" >/dev/null
  fi

  local deadline=$((SECONDS + wait_seconds))
  until pipeline_manager_ready; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "Timed out waiting for Feldera pipeline-manager at ${base_url}" >&2
      docker_ctx logs --tail 80 "$container" >&2 || true
      exit 1
    fi
    sleep 2
  done
  echo "Feldera pipeline-manager is reachable at ${base_url}"
}

clean_feldera_compiler_cache() {
  if [ "$clean_between_runtime_tests" != "1" ]; then
    return 0
  fi
  if [ -z "$compiler_cache_volume" ] && [ "$compiler_cache_mode" = "bind" ] && [ -d "$compiler_cache_dir" ]; then
    rm -rf "$compiler_cache_dir"/pipeline-binaries/* "$compiler_cache_dir"/target/optimized/*
    rm -rf "$compiler_cache_dir"/crates/feldera_pipe_*
    rm -rf "$compiler_cache_dir"/target/debug/incremental/*
    rm -rf "$compiler_cache_dir"/target/debug/.fingerprint/feldera_pipe_*
    rm -rf "$compiler_cache_dir"/target/debug/build/feldera_pipe_*
    rm -f "$compiler_cache_dir"/target/debug/feldera_pipe_* "$compiler_cache_dir"/target/debug/libfeldera_pipe_*
    rm -f "$compiler_cache_dir"/target/debug/deps/feldera_pipe_* "$compiler_cache_dir"/target/debug/deps/libfeldera_pipe_*
  elif [ -z "$compiler_cache_volume" ] && [ "$compiler_cache_mode" = "loop" ] && colima_available; then
    colima_ssh bash -lc "base='$compiler_cache_mountpoint'
      if mountpoint -q \"\$base\"; then
        sudo rm -rf \"\$base\"/pipeline-binaries/* \"\$base\"/target/optimized/*
        sudo rm -rf \"\$base\"/crates/feldera_pipe_*
        sudo rm -rf \"\$base\"/target/debug/incremental/*
        sudo rm -rf \"\$base\"/target/debug/.fingerprint/feldera_pipe_*
        sudo rm -rf \"\$base\"/target/debug/build/feldera_pipe_*
        sudo rm -f \"\$base\"/target/debug/feldera_pipe_* \"\$base\"/target/debug/libfeldera_pipe_*
        sudo rm -f \"\$base\"/target/debug/deps/feldera_pipe_* \"\$base\"/target/debug/deps/libfeldera_pipe_*
        sudo chown -R 1000:1000 \"\$base\"
      fi" >/dev/null 2>&1 || true
  fi
  if ! docker_context_available; then
    return 0
  fi
  if ! docker_ctx container inspect "$container" >/dev/null 2>&1; then
    return 0
  fi
  docker_ctx exec "$container" sh -lc \
    'base=/home/ubuntu/.feldera/compiler/rust-compilation
     rm -rf "$base"/pipeline-binaries/* "$base"/target/optimized/*
     rm -rf "$base"/crates/feldera_pipe_*
     rm -rf "$base"/target/debug/incremental/*
     rm -rf "$base"/target/debug/.fingerprint/feldera_pipe_*
     rm -rf "$base"/target/debug/build/feldera_pipe_*
     rm -f "$base"/target/debug/feldera_pipe_* "$base"/target/debug/libfeldera_pipe_*
     rm -f "$base"/target/debug/deps/feldera_pipe_* "$base"/target/debug/deps/libfeldera_pipe_*' \
    >/dev/null 2>&1 || true
}

trim_loop_compiler_cache() {
  if [ -n "$compiler_cache_volume" ] || [ "$compiler_cache_mode" != "loop" ] || ! colima_available; then
    return 0
  fi
  colima_ssh bash -lc "base='$compiler_cache_mountpoint'
    if mountpoint -q \"\$base\"; then
      sudo fstrim \"\$base\" >/dev/null 2>&1 || true
    fi" >/dev/null 2>&1 || true
}

clean_feldera_stale_pipelines() {
  if [ "$clean_stale_pipelines" != "1" ]; then
    return 0
  fi
  python3 - "$base_url" <<'PY'
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

base_url = sys.argv[1].rstrip("/")
token = os.environ.get("VELORIX_FELDERA_BEARER_TOKEN")


def request(method, path):
    req = urllib.request.Request(f"{base_url}{path}", method=method)
    if token:
        req.add_header("authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()
    except urllib.error.URLError as error:
        print(f"warning: Feldera stale pipeline cleanup request failed: {error}", file=sys.stderr)
        return 0, b""


def pipeline_path(name, suffix=""):
    return f"/v0/pipelines/{urllib.parse.quote(name, safe='')}{suffix}"


status, body = request("GET", "/v0/pipelines")
if status != 200:
    print(f"warning: could not list Feldera pipelines for cleanup: HTTP {status}", file=sys.stderr)
    raise SystemExit(0)

try:
    pipelines = json.loads(body)
except json.JSONDecodeError as error:
    print(f"warning: Feldera pipeline list is not JSON: {error}", file=sys.stderr)
    raise SystemExit(0)

cleaned = 0
for pipeline in pipelines:
    if not isinstance(pipeline, dict):
        continue
    name = pipeline.get("name")
    if not isinstance(name, str) or not name.startswith("velorix-"):
        continue

    deployment = pipeline.get("deployment_status")
    if deployment != "Stopped":
        status, _ = request("POST", pipeline_path(name, "/stop?force=true"))
        if status == 404:
            cleaned += 1
            continue
        if status not in (200, 202, 503):
            print(f"warning: could not force-stop Feldera pipeline {name}: HTTP {status}", file=sys.stderr)
            continue

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            status, body = request("GET", pipeline_path(name))
            if status == 404:
                cleaned += 1
                break
            if status != 200:
                time.sleep(0.2)
                continue
            try:
                deployment = json.loads(body).get("deployment_status")
            except json.JSONDecodeError:
                deployment = None
            if deployment == "Stopped":
                break
            time.sleep(0.2)
        else:
            print(f"warning: timed out waiting for Feldera pipeline {name} to stop", file=sys.stderr)
            continue

    status, _ = request("POST", pipeline_path(name, "/clear"))
    if status == 404:
        cleaned += 1
        continue
    if status not in (200, 202):
        print(f"warning: could not clear Feldera pipeline {name}: HTTP {status}", file=sys.stderr)
        continue

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        status, body = request("GET", pipeline_path(name))
        if status == 404:
            cleaned += 1
            break
        if status != 200:
            time.sleep(0.2)
            continue
        try:
            storage = json.loads(body).get("storage_status")
        except json.JSONDecodeError:
            storage = None
        if storage == "Cleared":
            break
        time.sleep(0.2)
    else:
        print(f"warning: timed out waiting for Feldera pipeline {name} storage to clear", file=sys.stderr)
        continue

    status, _ = request("DELETE", pipeline_path(name))
    if status in (200, 202, 404):
        cleaned += 1
    else:
        print(f"warning: could not delete Feldera pipeline {name}: HTTP {status}", file=sys.stderr)

if cleaned:
    print(f"cleaned Feldera stale Velorix pipelines: {cleaned}")
PY
}

preflight_feldera_cache_space() {
  if ! docker_context_available; then
    return 0
  fi
  if ! docker_ctx container inspect "$container" >/dev/null 2>&1; then
    return 0
  fi
  local available_kib
  available_kib="$(docker_ctx exec "$container" sh -lc \
    'df -k /home/ubuntu/.feldera/compiler/rust-compilation | awk '\''NR == 2 { print $4 }'\''' \
    2>/dev/null || true)"
  if [ -z "$available_kib" ]; then
    return 0
  fi
  if [ "$available_kib" -lt "$min_cache_free_kib" ]; then
    echo "insufficient Feldera compiler cache free space: available_kib=${available_kib} required_kib=${min_cache_free_kib}" >&2
    echo "Use a larger dedicated Colima profile, lower VELORIX_LIVE_FELDERA_MIN_CACHE_FREE_KIB after review, or remove/recreate the dedicated compiler cache volume." >&2
    exit 75
  fi
}

run_cargo_live_test() {
  local test_name="$1"
  local env_args=(
    LIVE_FELDERA=1
    LIVE_FELDERA_RUNTIME="$runtime_enabled"
    VELORIX_FELDERA_PIPELINE_MANAGER_URL="$base_url"
  )
  if [ -n "$cargo_target_dir" ]; then
    mkdir -p "$cargo_target_dir"
    env_args+=(CARGO_TARGET_DIR="$cargo_target_dir")
  fi
  echo "running ${test_name}"
  env "${env_args[@]}" \
    cargo test -p velorix-api --test live_feldera_pipeline_manager \
      "$test_name" -- --nocapture
}

write_evidence() {
  local status="$1"
  local exit_code="${2:-0}"
  mkdir -p "$evidence_dir"
  local compile_filters_json
  local runtime_filters_json
  compile_filters_json="$(json_array_from_args "${compile_tests[@]}")"
  runtime_filters_json="$(json_array_from_args "${runtime_tests[@]}")"
  python3 - "$status" "$exit_code" "$run_id" "$base_url" "$runtime_enabled" "$cargo_target_dir" "$evidence_path" "$compile_filters_json" "$runtime_filters_json" "$image" "$allow_official_image" <<'PY'
import json
import os
import sys
from pathlib import Path

(
    status,
    exit_code,
    run_id,
    base_url,
    runtime_enabled,
    cargo_target_dir,
    evidence_path,
    compile_filters_json,
    runtime_filters_json,
    backend_image,
    official_image_allowed,
) = sys.argv[1:]
compile_filters = json.loads(compile_filters_json)
available_runtime_filters = json.loads(runtime_filters_json)
runtime_is_enabled = runtime_enabled in {"1", "true", "TRUE", "True"}
runtime_filters = available_runtime_filters if runtime_is_enabled else []
compiler_cache_mode = os.environ.get("VELORIX_LIVE_FELDERA_COMPILER_CACHE_MODE", "loop")
compiler_cache_volume = os.environ.get("VELORIX_LIVE_FELDERA_COMPILER_CACHE_VOLUME")
payload = {
    "evidence_kind": "velorix_live_feldera_pipeline_manager_evidence",
    "schema_version": 1,
    "evidence_scope": "compatibility_fixture",
    "product_evidence": False,
    "backend_kind": "pipeline_manager",
    "backend_image": backend_image or "unknown_external",
    "backend_image_digest": "unknown_external",
    "official_image_allowed": official_image_allowed == "1",
    "jarless_backend_attested": False,
    "status": status,
    "exit_code": int(exit_code),
    "failure_kind": "local_environment_blocker" if status == "blocked" else ("test_failure" if status == "failed" else None),
    "run_id": run_id,
    "pipeline_manager_url": base_url,
    "runtime_enabled": runtime_is_enabled,
    "cargo_target_dir": cargo_target_dir or "cargo-default",
    "compiler_cache": {
        "kind": "docker_volume"
        if compiler_cache_volume
        else ("host_ext4_image" if compiler_cache_mode == "loop" else "host_directory"),
        "source": compiler_cache_volume or os.environ.get("VELORIX_LIVE_FELDERA_COMPILER_CACHE_IMAGE") or os.environ.get("VELORIX_LIVE_FELDERA_COMPILER_CACHE_DIR") or "target/feldera-compiler-cache.ext4",
    },
    "compile_test_filters": compile_filters,
    "runtime_test_filters": runtime_filters,
    "executed_test_filters": compile_filters + runtime_filters,
    "available_runtime_test_filters": available_runtime_filters,
    "skipped_runtime_test_filters": [] if runtime_is_enabled else available_runtime_filters,
}
path = Path(evidence_path)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(path)
PY
}

write_failure_evidence() {
  local exit_code="$1"
  local status="failed"
  if [ "$exit_code" -eq 75 ]; then
    status="blocked"
  fi
  echo "live Feldera pipeline-manager checks ${status}; writing evidence to ${evidence_path}" >&2
  write_evidence "$status" "$exit_code" >/dev/null || true
}

json_array_from_args() {
  python3 - "$@" <<'PY'
import json
import sys

print(json.dumps(sys.argv[1:]))
PY
}

on_exit() {
  local exit_code="$?"
  trap - EXIT
  if [ "$exit_code" -ne 0 ]; then
    trim_loop_compiler_cache
    write_failure_evidence "$exit_code"
  fi
  exit "$exit_code"
}

trap on_exit EXIT

require cargo
require curl
require python3
preflight_disk_space
reset_local_container_for_runtime
clean_feldera_compiler_cache
ensure_feldera_container

clean_feldera_stale_pipelines
preflight_feldera_cache_space

if runtime_truthy; then
  for test_name in "${runtime_tests[@]}"; do
    trim_loop_compiler_cache
    preflight_disk_space
    preflight_feldera_cache_space
    run_cargo_live_test "$test_name"
    clean_feldera_compiler_cache
    trim_loop_compiler_cache
  done
fi

for test_name in "${compile_tests[@]}"; do
  trim_loop_compiler_cache
  preflight_disk_space
  run_cargo_live_test "$test_name"
done

clean_feldera_stale_pipelines
clean_feldera_compiler_cache
trim_loop_compiler_cache
evidence_file="$(write_evidence passed 0)"
echo "live Feldera pipeline-manager checks passed"
echo "evidence=${evidence_file}"
