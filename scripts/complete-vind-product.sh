#!/usr/bin/env bash
set -euo pipefail
umask 077

case "$-" in
  *x*)
    echo "Refusing to run with shell xtrace enabled because product completion can use credentials and tokens" >&2
    exit 64
    ;;
esac

repo_root="$(git rev-parse --show-toplevel)"
env_file="${VELORIX_COMPLETE_PRODUCT_ENV_FILE:-}"

usage() {
  cat <<'EOF'
Complete a vind product slice by running the remaining product-complete helpers.

Usage:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
  AWS_ENDPOINT_URL=https://S3_OR_OSS_ENDPOINT \
  AWS_ACCESS_KEY_ID=... \
  AWS_SECRET_ACCESS_KEY=... \
  AWS_REGION=us-east-1 \
  VELORIX_S3_BUCKET=velorix-product \
  VELORIX_PRODUCT_INGRESS_HOST=velorix.example.com \
  VELORIX_PRODUCT_INGRESS_CLASS=nginx \
  VELORIX_PRODUCT_INGRESS_TLS_SECRET=velorix-api-public-tls \
  VELORIX_INGRESS_ENDPOINT_URL=https://velorix.example.com \
  VELORIX_INGRESS_CONTROLLER=nginx \
  scripts/complete-vind-product.sh \
    --versioning-or-object-lock-enabled \
    --server-side-encryption-enabled \
    --backup-or-replication-configured \
    --lifecycle-delete-policy-reviewed \
    --destructive-delete-protection-reviewed \
    --cost-controls-reviewed

Or use the generated handoff env template:
  scripts/write-complete-vind-product-env.sh \
    --product-evidence target/velorix-product/product-evidence.json
  # Edit target/velorix-product/complete-vind-product.env first.
  VELORIX_COMPLETE_PRODUCT_DRY_RUN=1 \
    scripts/complete-vind-product.sh \
      --env-file target/velorix-product/complete-vind-product.env
  scripts/complete-vind-product.sh \
    --env-file target/velorix-product/complete-vind-product.env

Step modes, each auto|0|1:
  VELORIX_COMPLETE_PRODUCT_EXTERNAL_S3=auto
  VELORIX_COMPLETE_PRODUCT_INGRESS=auto
  VELORIX_COMPLETE_PRODUCT_DURABILITY=auto
  VELORIX_COMPLETE_PRODUCT_HIQLITE_BACKEND_TIME=auto
  VELORIX_COMPLETE_PRODUCT_LOCAL_EVIDENCE=auto

Other:
  --env-file PATH
  VELORIX_COMPLETE_PRODUCT_ENV_FILE=target/velorix-product/complete-vind-product.env
  VELORIX_COMPLETE_PRODUCT_DRY_RUN=0
  VELORIX_COMPLETE_PRODUCT_REPORT=1

Unknown CLI flags are passed to scripts/complete-vind-object-store-durability.sh
as explicit operator durability review flags. This helper creates no PVCs.
EOF
}

durability_args=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    --env-file)
      env_file="${2:-}"
      if [ -z "$env_file" ]; then
        echo "--env-file requires a path" >&2
        exit 64
      fi
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      durability_args+=("$1")
      shift
      ;;
  esac
done
set -- "${durability_args[@]}"

valid_step_mode() {
  case "$2" in
    auto | 0 | 1) ;;
    *)
      echo "$1 must be auto, 0, or 1" >&2
      exit 64
      ;;
  esac
}

valid_bool() {
  case "$2" in
    0 | 1) ;;
    *)
      echo "$1 must be 0 or 1" >&2
      exit 64
      ;;
  esac
}

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 127
  fi
}

source_env_file_preserving_overrides() {
  local env_path="$1"
  shift
  local name flag_var value_var
  for name in "$@"; do
    flag_var="__velorix_env_override_${name}"
    value_var="__velorix_env_override_value_${name}"
    if [ "${!name+x}" = x ]; then
      printf -v "$flag_var" '%s' 1
      printf -v "$value_var" '%s' "${!name}"
    else
      printf -v "$flag_var" '%s' 0
    fi
  done
  # shellcheck disable=SC1090
  . "$env_path"
  for name in "$@"; do
    flag_var="__velorix_env_override_${name}"
    value_var="__velorix_env_override_value_${name}"
    if [ "${!flag_var}" = "1" ]; then
      export "$name=${!value_var}"
    fi
    unset "$flag_var" "$value_var"
  done
}

cd "$repo_root"
require python3

if [ -n "$env_file" ]; then
  case "$env_file" in
    /*) ;;
    *) env_file="${repo_root}/${env_file}" ;;
  esac
  if [ ! -f "$env_file" ]; then
    echo "--env-file does not exist: ${env_file}" >&2
    exit 66
  fi
  source_env_file_preserving_overrides "$env_file" \
    VELORIX_VIND_PRODUCT_DIR \
    VELORIX_VIND_PRODUCT_EVIDENCE \
    VELORIX_PRODUCT_COMPLETION_REPORT \
    VELORIX_COMPLETE_PRODUCT_INPUT_PREFLIGHT \
    VELORIX_COMPLETE_PRODUCT_EXTERNAL_S3 \
    VELORIX_COMPLETE_PRODUCT_INGRESS \
    VELORIX_COMPLETE_PRODUCT_DURABILITY \
    VELORIX_COMPLETE_PRODUCT_HIQLITE_BACKEND_TIME \
    VELORIX_COMPLETE_PRODUCT_LOCAL_EVIDENCE \
    VELORIX_COMPLETE_PRODUCT_REPORT \
    VELORIX_COMPLETE_PRODUCT_DRY_RUN \
    VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3 \
    VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS \
    VELORIX_PRODUCT_COMPLETE_REQUIRE_HIQLITE_RELEASE \
    AWS_ENDPOINT_URL \
    AWS_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY \
    AWS_SESSION_TOKEN \
    AWS_REGION \
    VELORIX_S3_BUCKET \
    VELORIX_S3_PREFIX \
    VELORIX_AUTHORITY_STORE_ID \
    VELORIX_S3_FORCE_PATH_STYLE \
    VELORIX_S3_CREDENTIALS_SECRET_NAME \
    VELORIX_S3_CREDENTIALS_SECRET_MANAGED \
    VELORIX_EXTERNAL_S3_ALLOW_LOCAL_ENDPOINT \
    VELORIX_EXTERNAL_S3_RUN_PRODUCT \
    VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY \
    VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE \
    VELORIX_PRODUCT_INGRESS_HOST \
    VELORIX_PRODUCT_INGRESS_APPLY \
    VELORIX_PRODUCT_INGRESS_ATTEST \
    VELORIX_PRODUCT_INGRESS_ATTACH \
    VELORIX_PRODUCT_INGRESS_CLASS \
    VELORIX_PRODUCT_INGRESS_TLS_SECRET \
    VELORIX_PRODUCT_INGRESS_WAIT_TIMEOUT_SECONDS \
    VELORIX_PRODUCT_INGRESS_WAIT_INTERVAL_SECONDS \
    VELORIX_INGRESS_ENDPOINT_URL \
    VELORIX_INGRESS_CONTROLLER \
    VELORIX_INGRESS_TLS_AUTH_READY_TIMEOUT_SECONDS \
    VELORIX_INGRESS_TLS_AUTH_READY_INTERVAL_SECONDS \
    VELORIX_API_AUTH_ENV \
    VELORIX_API_BEARER_TOKEN \
    VELORIX_ADMIN_BEARER_TOKEN \
    VELORIX_API_AUTH_HEADER \
    VELORIX_ADMIN_AUTH_HEADER \
    VELORIX_OBJECT_STORE_VERSIONING_OR_OBJECT_LOCK_ENABLED \
    VELORIX_OBJECT_STORE_SERVER_SIDE_ENCRYPTION_ENABLED \
    VELORIX_OBJECT_STORE_BACKUP_OR_REPLICATION_CONFIGURED \
    VELORIX_OBJECT_STORE_LIFECYCLE_DELETE_POLICY_REVIEWED \
    VELORIX_OBJECT_STORE_DESTRUCTIVE_DELETE_PROTECTION_REVIEWED \
    VELORIX_OBJECT_STORE_COST_CONTROLS_REVIEWED \
    VELORIX_PRODUCT_EVIDENCE_PATH \
    VELORIX_HIQLITE_BACKEND_TIME_RELEASE_ENV_FORCE \
    VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE \
    VELORIX_SOURCE_REPOSITORY \
    VELORIX_SOURCE_REVISION \
    VELORIX_RELEASE_COMMIT \
    VELORIX_API_IMAGE_DIGEST \
    VELORIX_META_IMAGE_DIGEST \
    VELORIX_HIQLITE_IMAGE_DIGEST \
    VELORIX_CI_WORKFLOW_NAME \
    VELORIX_CI_WORKFLOW_RUN_ID \
    VELORIX_CI_JOB_NAME \
    VELORIX_CI_OIDC_SUBJECT \
    VELORIX_CI_WORKFLOW_REF \
    VELORIX_CI_JOB_WORKFLOW_REF \
    VELORIX_CI_SIGSTORE_BUNDLE_BASE64 \
    VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY \
    VELORIX_CI_SIGSTORE_BUNDLE_SHA256 \
    VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST
fi

product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
product_evidence="${VELORIX_VIND_PRODUCT_EVIDENCE:-${product_dir}/product-evidence.json}"
report_file="${VELORIX_PRODUCT_COMPLETION_REPORT:-${product_dir}/product-completion-report.json}"
input_preflight_file="${VELORIX_COMPLETE_PRODUCT_INPUT_PREFLIGHT:-${product_dir}/complete-vind-product-input-preflight.json}"
external_s3_required="${VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3:-0}"
public_ingress_required="${VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS:-0}"
hiqlite_release_required="${VELORIX_PRODUCT_COMPLETE_REQUIRE_HIQLITE_RELEASE:-0}"
external_s3_step="${VELORIX_COMPLETE_PRODUCT_EXTERNAL_S3:-auto}"
ingress_step="${VELORIX_COMPLETE_PRODUCT_INGRESS:-auto}"
durability_step="${VELORIX_COMPLETE_PRODUCT_DURABILITY:-auto}"
hiqlite_backend_time_step="${VELORIX_COMPLETE_PRODUCT_HIQLITE_BACKEND_TIME:-auto}"
local_evidence_step="${VELORIX_COMPLETE_PRODUCT_LOCAL_EVIDENCE:-auto}"
final_report="${VELORIX_COMPLETE_PRODUCT_REPORT:-1}"
dry_run="${VELORIX_COMPLETE_PRODUCT_DRY_RUN:-0}"
hiqlite_release_env_force="${VELORIX_HIQLITE_BACKEND_TIME_RELEASE_ENV_FORCE:-0}"

valid_step_mode VELORIX_COMPLETE_PRODUCT_EXTERNAL_S3 "$external_s3_step"
valid_step_mode VELORIX_COMPLETE_PRODUCT_INGRESS "$ingress_step"
valid_step_mode VELORIX_COMPLETE_PRODUCT_DURABILITY "$durability_step"
valid_step_mode VELORIX_COMPLETE_PRODUCT_HIQLITE_BACKEND_TIME "$hiqlite_backend_time_step"
valid_step_mode VELORIX_COMPLETE_PRODUCT_LOCAL_EVIDENCE "$local_evidence_step"
valid_bool VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3 "$external_s3_required"
valid_bool VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS "$public_ingress_required"
valid_bool VELORIX_PRODUCT_COMPLETE_REQUIRE_HIQLITE_RELEASE "$hiqlite_release_required"
valid_bool VELORIX_COMPLETE_PRODUCT_DRY_RUN "$dry_run"
valid_bool VELORIX_COMPLETE_PRODUCT_REPORT "$final_report"
valid_bool VELORIX_HIQLITE_BACKEND_TIME_RELEASE_ENV_FORCE "$hiqlite_release_env_force"

if [ "$external_s3_required" != "1" ]; then
  external_s3_step="0"
  durability_step="0"
fi
if [ "$public_ingress_required" != "1" ]; then
  ingress_step="0"
fi

mkdir -p "$product_dir"

preflight_step_ready() {
  python3 - "$input_preflight_file" "$1" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
step_name = sys.argv[2]
if not path.is_file():
    raise SystemExit(1)
with path.open("r", encoding="utf-8") as f:
    preflight = json.load(f)
step = (preflight.get("steps") or {}).get(step_name) or {}
raise SystemExit(0 if step.get("ready") is True else 1)
PY
}

run_completion_input_preflight() {
  scripts/write-complete-vind-product-input-preflight.py \
    --product-evidence "$product_evidence" \
    --output "$input_preflight_file" \
    --external-s3-mode "$external_s3_step" \
    --ingress-mode "$ingress_step" \
    --durability-mode "$durability_step" \
    --hiqlite-mode "$hiqlite_backend_time_step" \
    -- "$@"
}

run_local_evidence_refresh() {
  if [ "$local_evidence_step" = "0" ]; then
    return 0
  fi
  if [ ! -f "$product_evidence" ]; then
    if [ "$local_evidence_step" = "1" ]; then
      echo "local evidence refresh requires product evidence: ${product_evidence}" >&2
      exit 66
    fi
    echo "local_evidence=skipped_missing_product_evidence"
    return 0
  fi

  echo "local_evidence=refreshing_deployed_image_digests"
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    VELORIX_VIND_PRODUCT_EVIDENCE="$product_evidence" \
    VELORIX_VIND_PRODUCT_EVIDENCE_OUT="$product_evidence" \
    scripts/refresh-vind-product-deployed-images.sh

  echo "local_evidence=running_rest_api_smoke"
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    VELORIX_API_AUTH_ENV="${product_dir}/api-auth.env" \
    VELORIX_API_ATTACH_EVIDENCE="${product_dir}/rest-attach-evidence.json" \
    VELORIX_REST_API_SMOKE_DIR="${product_dir}/rest-api-smoke" \
    VELORIX_REST_API_SMOKE_EVIDENCE="${product_dir}/rest-api-smoke.json" \
    VELORIX_REST_API_SMOKE_ATTACH=auto \
    scripts/smoke-vind-rest-api.sh
}

run_hiqlite_backend_time_release_preflight() {
  local release_env="${product_dir}/hiqlite-backend-time-release.env"
  local release_env_report="${product_dir}/hiqlite-backend-time-release-env.json"

  if [ "$hiqlite_release_env_force" = "1" ] || [ ! -f "$release_env" ]; then
    scripts/write-hiqlite-backend-time-release-env.sh \
      --product-evidence "$product_evidence" \
      --output "$release_env" \
      --report "$release_env_report" >/dev/null
  else
    echo "hiqlite_backend_time=using_existing_release_env"
  fi

  scripts/check-hiqlite-backend-time-release-inputs.sh \
    --env-file "$release_env" \
    --product-evidence "$product_evidence"
}

product_external_s3_ready() {
  python3 - "$product_evidence" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.is_file():
    raise SystemExit(1)
with path.open("r", encoding="utf-8") as f:
    product = json.load(f)
store = product.get("object_store") or {}
ok = (
    store.get("mode") == "external-s3"
    and store.get("local_development_authority") is not True
    and store.get("external_s3_bucket_validated") is True
    and store.get("external_s3_prefix_validated") is True
)
raise SystemExit(0 if ok else 1)
PY
}

gate_status() {
  python3 - "$report_file" "$1" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
gate_id = sys.argv[2]
if not path.is_file():
    print("missing")
    raise SystemExit(0)
with path.open("r", encoding="utf-8") as f:
    report = json.load(f)
for gate in report.get("gates", []):
    if gate.get("id") == gate_id:
        print(gate.get("status") or "missing")
        raise SystemExit(0)
print("missing")
PY
}

write_plan() {
  python3 - "$product_evidence" "$report_file" "$input_preflight_file" "${env_file:-}" "$external_s3_step" "$ingress_step" "$durability_step" "$hiqlite_backend_time_step" "$local_evidence_step" "$dry_run" "$hiqlite_release_required" "$@" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    product_evidence,
    report_file,
    input_preflight_file,
    env_file,
    external_mode,
    ingress_mode,
    durability_mode,
    hiqlite_mode,
    local_evidence_mode,
    dry_run,
    hiqlite_release_required,
    *durability_args,
) = sys.argv[1:]
env = os.environ


def load_json(path):
    candidate = Path(path)
    if not candidate.is_file():
        return {}
    try:
        loaded = json.loads(candidate.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def product_external_s3_ready(product):
    store = product.get("object_store") or {}
    return (
        store.get("mode") == "external-s3"
        and store.get("local_development_authority") is not True
        and store.get("external_s3_bucket_validated") is True
        and store.get("external_s3_prefix_validated") is True
    )


def issue_count(step, key):
    return len(step.get(key) or [])


def preflight_step(preflight, name):
    step = (preflight.get("steps") or {}).get(name) or {}
    return step if isinstance(step, dict) else {}


def issue_subjects(step, key):
    return sorted(
        {
            issue.get("subject")
            for issue in step.get(key) or []
            if isinstance(issue, dict) and issue.get("subject")
        }
    )


def redacted_step_summary(step):
    return {
        "status": step.get("status"),
        "ready": step.get("ready"),
        "missing_count": issue_count(step, "missing"),
        "invalid_count": issue_count(step, "invalid"),
        "missing_subjects": issue_subjects(step, "missing"),
        "invalid_subjects": issue_subjects(step, "invalid"),
    }


def planned_step(name, *, mode, helper, preflight, waiting_on=None):
    waiting_on = waiting_on or []
    step = preflight_step(preflight, name)
    status = step.get("status")
    ready = step.get("ready")
    summary = redacted_step_summary(step)
    summary.update(
        {
            "step": name,
            "mode": mode,
            "helper": helper,
            "waiting_on": waiting_on,
            "will_run": False,
            "state": "disabled",
        }
    )
    if mode == "0":
        summary["state"] = "disabled"
    elif status == "already_validated":
        summary["state"] = "already_validated"
    elif waiting_on:
        summary["state"] = "waiting_on_prerequisite"
    elif ready is True:
        summary["state"] = "ready_to_run"
        summary["will_run"] = external_execution_allowed
    elif status == "blocked":
        summary["state"] = "blocked"
    elif status == "incomplete":
        summary["state"] = "input_incomplete"
    else:
        summary["state"] = status or "unknown"
    return summary


product = load_json(product_evidence)
preflight = load_json(input_preflight_file)
local_execution_allowed = dry_run != "1"
external_execution_allowed = dry_run != "1" and preflight.get("status") != "blocked"
external_s3_current_ready = product_external_s3_ready(product)

local_evidence_state = "disabled"
local_evidence_will_run = False
local_evidence_missing = []
if local_evidence_mode != "0":
    if Path(product_evidence).is_file():
        local_evidence_state = "ready_to_run"
        local_evidence_will_run = local_execution_allowed
    elif local_evidence_mode == "1":
        local_evidence_state = "blocked"
        local_evidence_missing = ["product_evidence"]
    else:
        local_evidence_state = "skipped_missing_product_evidence"

durability_waiting_on = [] if external_s3_current_ready else ["external_s3"]

payload = {
    "schema_version": 1,
    "report_kind": "velorix_complete_vind_product_plan",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "product_evidence": product_evidence,
    "product_completion_report": report_file,
    "env_file": env_file or None,
    "input_preflight_report": input_preflight_file,
    "dry_run": dry_run == "1",
    "preflight_status": preflight.get("status"),
    "forced_blocker_count": len(preflight.get("forced_blockers") or []),
    "external_s3_current_ready": external_s3_current_ready,
    "run_order": [
        "local_evidence",
        "external_s3",
        "ingress",
        "durability",
        "hiqlite_backend_time",
        "final_report",
    ],
    "steps": {
        "local_evidence": {
            "step": "local_evidence",
            "mode": local_evidence_mode,
            "helper": "scripts/refresh-vind-product-deployed-images.sh + scripts/smoke-vind-rest-api.sh",
            "state": local_evidence_state,
            "will_run": local_evidence_will_run,
            "product_evidence_exists": Path(product_evidence).is_file(),
            "missing_subjects": local_evidence_missing,
        },
        "external_s3": planned_step(
            "external_s3",
            mode=external_mode,
            helper="scripts/run-vind-product-external-s3.sh",
            preflight=preflight,
        ),
        "ingress": planned_step(
            "ingress",
            mode=ingress_mode,
            helper="scripts/complete-vind-product-ingress.sh",
            preflight=preflight,
        ),
        "durability": planned_step(
            "durability",
            mode=durability_mode,
            helper="scripts/complete-vind-object-store-durability.sh",
            preflight=preflight,
            waiting_on=durability_waiting_on,
        ),
        "hiqlite_backend_time": {
            "step": "hiqlite_backend_time",
            "mode": hiqlite_mode,
            "helper": "scripts/check-hiqlite-backend-time-release-inputs.sh + scripts/attest-hiqlite-backend-time.sh",
            "state": "disabled"
            if hiqlite_mode == "0"
            else (
                "release_preflight_required"
                if hiqlite_release_required == "1"
                else "diagnostic_attestation_only"
            ),
            "will_run": external_execution_allowed and hiqlite_mode != "0" and Path(product_evidence).is_file(),
            "release_preflight_required": hiqlite_release_required == "1",
            "trusted_provenance_requested": env.get("VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE") == "1",
            "release_failover_requested": env.get("VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST") == "1",
            **redacted_step_summary(preflight_step(preflight, "hiqlite_backend_time")),
        },
        "final_report": {
            "step": "final_report",
            "mode": env.get("VELORIX_COMPLETE_PRODUCT_REPORT", "1"),
            "helper": "scripts/report-vind-product-completion.sh",
            "state": "ready_to_run" if env.get("VELORIX_COMPLETE_PRODUCT_REPORT", "1") == "1" else "disabled",
            "will_run": local_execution_allowed and env.get("VELORIX_COMPLETE_PRODUCT_REPORT", "1") == "1" and Path(product_evidence).is_file(),
        },
    },
}
plan_path = Path(product_evidence).parent / "complete-vind-product-plan.json"
plan_path.parent.mkdir(parents=True, exist_ok=True)
plan_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(f"plan={plan_path}")
print(f"preflight_status={payload['preflight_status']}")
print(f"forced_blocker_count={payload['forced_blocker_count']}")
print(json.dumps(payload["steps"], indent=2, sort_keys=True))
PY
}

if [ "$dry_run" = "1" ]; then
  run_completion_input_preflight "$@" >/dev/null || true
  write_plan "$@"
  if [ -f "$product_evidence" ] && [ "$hiqlite_backend_time_step" != "0" ] && [ "$hiqlite_release_required" = "1" ]; then
    release_env="${product_dir}/hiqlite-backend-time-release.env"
    if [ "$hiqlite_release_env_force" = "1" ] || [ ! -f "$release_env" ]; then
      scripts/write-hiqlite-backend-time-release-env.sh \
        --product-evidence "$product_evidence" \
        --output "$release_env" \
        --report "${product_dir}/hiqlite-backend-time-release-env.json" >/dev/null
    fi
  fi
  if [ "$final_report" = "1" ] && [ -f "$product_evidence" ]; then
    VELORIX_VIND_PRODUCT_DIR="$product_dir" \
      VELORIX_VIND_PRODUCT_EVIDENCE="$product_evidence" \
      VELORIX_PRODUCT_COMPLETION_REPORT="$report_file" \
      scripts/report-vind-product-completion.sh >/dev/null || true
    echo "product_completion_report=${report_file}"
  fi
  exit 0
fi

preflight_failed=0
if ! run_completion_input_preflight "$@"; then
  echo "complete product input preflight failed: ${input_preflight_file}" >&2
  preflight_failed=1
fi

write_plan "$@"

run_local_evidence_refresh

if [ "$final_report" = "1" ] && [ -f "$product_evidence" ]; then
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    VELORIX_VIND_PRODUCT_EVIDENCE="$product_evidence" \
    VELORIX_PRODUCT_COMPLETION_REPORT="$report_file" \
    scripts/report-vind-product-completion.sh >/dev/null || true
fi

if [ "$preflight_failed" = "1" ]; then
  exit 64
fi

if [ "$external_s3_step" != "0" ]; then
  if product_external_s3_ready; then
    echo "external_s3=already_validated"
  elif preflight_step_ready external_s3; then
    echo "external_s3=running"
    VELORIX_VIND_PRODUCT_DIR="$product_dir" scripts/run-vind-product-external-s3.sh
  elif [ "$external_s3_step" = "1" ]; then
    echo "external S3 step requires AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and VELORIX_S3_BUCKET" >&2
    exit 64
  else
    echo "external_s3=skipped_missing_env"
  fi
fi

if [ "$final_report" = "1" ] && [ -f "$product_evidence" ]; then
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    VELORIX_VIND_PRODUCT_EVIDENCE="$product_evidence" \
    VELORIX_PRODUCT_COMPLETION_REPORT="$report_file" \
    scripts/report-vind-product-completion.sh >/dev/null || true
fi

if [ "$ingress_step" != "0" ]; then
  ingress_status="$(gate_status public_ingress_tls_auth)"
  if [ "$ingress_status" = "pass" ]; then
    echo "ingress=already_validated"
  elif preflight_step_ready ingress; then
    echo "ingress=running"
    VELORIX_VIND_PRODUCT_DIR="$product_dir" scripts/complete-vind-product-ingress.sh
  elif [ "$ingress_step" = "1" ]; then
    echo "ingress step requires VELORIX_PRODUCT_INGRESS_HOST, VELORIX_PRODUCT_INGRESS_CLASS, VELORIX_PRODUCT_INGRESS_TLS_SECRET, VELORIX_INGRESS_ENDPOINT_URL, and VELORIX_INGRESS_CONTROLLER" >&2
    exit 64
  else
    echo "ingress=skipped_missing_env"
  fi
fi

if [ "$final_report" = "1" ] && [ -f "$product_evidence" ]; then
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    VELORIX_VIND_PRODUCT_EVIDENCE="$product_evidence" \
    VELORIX_PRODUCT_COMPLETION_REPORT="$report_file" \
    scripts/report-vind-product-completion.sh >/dev/null || true
fi

if [ "$durability_step" != "0" ]; then
  durability_status="$(gate_status object_store_durability_policy)"
  if [ "$durability_status" = "pass" ]; then
    echo "durability=already_validated"
  elif product_external_s3_ready && preflight_step_ready durability; then
    echo "durability=running"
    VELORIX_VIND_PRODUCT_DIR="$product_dir" scripts/complete-vind-object-store-durability.sh "$@"
  elif [ "$durability_step" = "1" ]; then
    echo "durability step requires validated external S3 product evidence and explicit durability review flags" >&2
    exit 64
  else
    echo "durability=skipped_missing_external_authority_or_review"
  fi
fi

if [ "$hiqlite_backend_time_step" != "0" ] && [ -f "$product_evidence" ]; then
  if [ "$hiqlite_release_required" = "1" ] && [ "${VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST:-0}" = "1" ]; then
    echo "hiqlite_backend_time=release_failover_smoke"
    VELORIX_VIND_PRODUCT_DIR="$product_dir" \
      VELORIX_VIND_PRODUCT_EVIDENCE="$product_evidence" \
      VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST=1 \
      VELORIX_STANDING_RUNTIME_FAILOVER_UPDATE_PRODUCT_EVIDENCE=1 \
      scripts/smoke-vind-standing-runtime-failover.sh
  fi
  echo "hiqlite_backend_time=diagnostic_attestation"
  VELORIX_PRODUCT_EVIDENCE_PATH="$product_evidence" \
    scripts/attest-hiqlite-backend-time.sh \
      --product-evidence "$product_evidence" \
      --output "${product_dir}/hiqlite-backend-time-attestation.json" \
      --attester complete-vind-product \
      --update-product-evidence
  echo "hiqlite_backend_time=preflight"
  if [ "$hiqlite_release_required" = "1" ]; then
    if run_hiqlite_backend_time_release_preflight; then
      if [ "${VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE:-}" = "1" ]; then
        VELORIX_PRODUCT_EVIDENCE_PATH="$product_evidence" \
          scripts/attest-hiqlite-backend-time.sh \
            --product-evidence "$product_evidence" \
            --output "${product_dir}/hiqlite-backend-time-attestation.json" \
            --update-product-evidence
      fi
    elif [ "$hiqlite_backend_time_step" = "1" ]; then
      exit 65
    fi
  else
    echo "hiqlite_backend_time=release_preflight_out_of_scope"
  fi
fi

if [ "$final_report" = "1" ] && [ -f "$product_evidence" ]; then
  VELORIX_VIND_PRODUCT_DIR="$product_dir" \
    VELORIX_VIND_PRODUCT_EVIDENCE="$product_evidence" \
    VELORIX_PRODUCT_COMPLETION_REPORT="$report_file" \
    scripts/report-vind-product-completion.sh
fi

python3 - "$report_file" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.is_file():
    raise SystemExit("missing product completion report")
with path.open("r", encoding="utf-8") as f:
    report = json.load(f)
complete = report.get("product_complete") is True
print(f"product_complete={str(complete).lower()}")
raise SystemExit(0 if complete else 65)
PY
