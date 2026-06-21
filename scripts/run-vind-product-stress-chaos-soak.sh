#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
work_dir="${VELORIX_STRESS_CHAOS_SOAK_DIR:-${product_dir}/stress-chaos-soak}"
summary_file="${VELORIX_STRESS_CHAOS_SOAK_EVIDENCE:-${product_dir}/stress-chaos-soak.json}"
stress_iterations="${VELORIX_STRESS_ITERATIONS:-1}"
soak_seconds="${VELORIX_SOAK_SECONDS:-0}"
soak_interval_seconds="${VELORIX_SOAK_INTERVAL_SECONDS:-5}"
chaos_failover_iterations="${VELORIX_CHAOS_FAILOVER_ITERATIONS:-0}"
chaos_enable_pod_delete="${VELORIX_CHAOS_ENABLE_POD_DELETE:-0}"

usage() {
  cat <<'EOF'
Run stress, soak, and opt-in chaos checks against an existing vind product slice.

Usage:
  scripts/run-vind-product-stress-chaos-soak.sh

Main environment overrides:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product
  VELORIX_STRESS_CHAOS_SOAK_DIR=target/velorix-product/stress-chaos-soak
  VELORIX_STRESS_CHAOS_SOAK_EVIDENCE=target/velorix-product/stress-chaos-soak.json
  VELORIX_STRESS_ITERATIONS=1
  VELORIX_SOAK_SECONDS=0
  VELORIX_SOAK_INTERVAL_SECONDS=5
  VELORIX_CHAOS_FAILOVER_ITERATIONS=0
  VELORIX_CHAOS_ENABLE_POD_DELETE=0

Chaos failover deletes an API owner pod through the existing failover smoke and
requires VELORIX_CHAOS_ENABLE_POD_DELETE=1.
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi
if [ "$#" -ne 0 ]; then
  usage >&2
  exit 64
fi

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 127
  fi
}

require date
require python3

non_negative_int() {
  case "$2" in
    '' | *[!0-9]*)
      echo "$1 must be a non-negative integer" >&2
      exit 64
      ;;
  esac
}

non_negative_int VELORIX_STRESS_ITERATIONS "$stress_iterations"
non_negative_int VELORIX_SOAK_SECONDS "$soak_seconds"
non_negative_int VELORIX_SOAK_INTERVAL_SECONDS "$soak_interval_seconds"
non_negative_int VELORIX_CHAOS_FAILOVER_ITERATIONS "$chaos_failover_iterations"
case "$chaos_enable_pod_delete" in
  0 | 1) ;;
  *)
    echo "VELORIX_CHAOS_ENABLE_POD_DELETE must be 0 or 1" >&2
    exit 64
    ;;
esac
if [ "$soak_seconds" -gt 0 ] && [ "$soak_interval_seconds" -eq 0 ]; then
  echo "VELORIX_SOAK_INTERVAL_SECONDS must be greater than 0 when soak is enabled" >&2
  exit 64
fi
if [ "$chaos_failover_iterations" -gt 0 ] && [ "$chaos_enable_pod_delete" != "1" ]; then
  echo "set VELORIX_CHAOS_ENABLE_POD_DELETE=1 to run pod-delete chaos failover" >&2
  exit 64
fi

cd "$repo_root"
mkdir -p "$work_dir" "$(dirname "$summary_file")"

stress_files="${work_dir}/stress-files.txt"
soak_files="${work_dir}/soak-files.txt"
chaos_files="${work_dir}/chaos-files.txt"
failures_file="${work_dir}/failures.txt"
: >"$stress_files"
: >"$soak_files"
: >"$chaos_files"
: >"$failures_file"

write_summary() {
  local status="$1"
  python3 - "$summary_file" "$status" "$product_dir" "$stress_iterations" "$soak_seconds" \
    "$soak_interval_seconds" "$chaos_failover_iterations" "$chaos_enable_pod_delete" \
    "$stress_files" "$soak_files" "$chaos_files" "$failures_file" <<'PY'
import json
import sys
from datetime import datetime, timezone

(
    output_path,
    status,
    product_dir,
    stress_iterations,
    soak_seconds,
    soak_interval_seconds,
    chaos_failover_iterations,
    chaos_enable_pod_delete,
    stress_files,
    soak_files,
    chaos_files,
    failures_file,
) = sys.argv[1:]

def read_lines(path):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

stress = read_lines(stress_files)
soak = read_lines(soak_files)
chaos = read_lines(chaos_files)
failures = read_lines(failures_file)
payload = {
    "schema_version": 1,
    "evidence_kind": "velorix_stress_chaos_soak",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "status": status,
    "product_dir": product_dir,
    "stress": {
        "iterations_requested": int(stress_iterations),
        "iterations_passed": len(stress),
        "evidence_files": stress,
    },
    "soak": {
        "duration_seconds": int(soak_seconds),
        "interval_seconds": int(soak_interval_seconds),
        "iterations_passed": len(soak),
        "evidence_files": soak,
    },
    "chaos": {
        "enabled": chaos_enable_pod_delete == "1",
        "failover_iterations_requested": int(chaos_failover_iterations),
        "failover_iterations_passed": len(chaos),
        "pod_delete_required_opt_in": True,
        "evidence_files": chaos,
    },
    "failures": failures,
}
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, sort_keys=True)
    f.write("\n")
PY
}

record_failure() {
  printf '%s\n' "$1" >>"$failures_file"
  write_summary fail
}

run_rest_smoke() {
  local phase="$1"
  local index="$2"
  local list_file="$3"
  local phase_dir="${work_dir}/${phase}/${index}"
  local evidence="${phase_dir}/rest-api-smoke.json"
  mkdir -p "$phase_dir"
  if ! VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    VELORIX_REST_API_SMOKE_DIR="$phase_dir" \
    VELORIX_REST_API_SMOKE_EVIDENCE="$evidence" \
    VELORIX_REST_API_SMOKE_ATTACH="${VELORIX_REST_API_SMOKE_ATTACH:-auto}" \
    scripts/smoke-vind-rest-api.sh; then
    record_failure "${phase}:${index}:rest_api_smoke_failed"
    exit 1
  fi
  printf '%s\n' "$evidence" >>"$list_file"
}

run_failover_smoke() {
  local index="$1"
  local phase_dir="${work_dir}/chaos/${index}"
  local evidence="${phase_dir}/standing-runtime-failover-smoke.json"
  mkdir -p "$phase_dir"
  if ! VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    VELORIX_STANDING_RUNTIME_FAILOVER_EVIDENCE="$evidence" \
    VELORIX_STANDING_RUNTIME_FAILOVER_UPDATE_PRODUCT_EVIDENCE="${VELORIX_STANDING_RUNTIME_FAILOVER_UPDATE_PRODUCT_EVIDENCE:-1}" \
    VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST="${VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST:-0}" \
    scripts/smoke-vind-standing-runtime-failover.sh; then
    record_failure "chaos:${index}:standing_runtime_failover_failed"
    exit 1
  fi
  printf '%s\n' "$evidence" >>"$chaos_files"
}

for ((i = 1; i <= stress_iterations; i += 1)); do
  run_rest_smoke stress "$(printf '%04d' "$i")" "$stress_files"
done

if [ "$soak_seconds" -gt 0 ]; then
  soak_deadline=$((SECONDS + soak_seconds))
  soak_iteration=0
  while [ "$SECONDS" -lt "$soak_deadline" ]; do
    soak_iteration=$((soak_iteration + 1))
    run_rest_smoke soak "$(printf '%04d' "$soak_iteration")" "$soak_files"
    if [ "$SECONDS" -lt "$soak_deadline" ]; then
      sleep "$soak_interval_seconds"
    fi
  done
fi

for ((i = 1; i <= chaos_failover_iterations; i += 1)); do
  run_failover_smoke "$(printf '%04d' "$i")"
done

write_summary pass
echo "wrote ${summary_file}"
