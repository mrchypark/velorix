#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
scenario_dir="${VELORIX_S3_CHECKPOINT_FAULT_MATRIX_SCENARIO_DIR:-${product_dir}/s3-checkpoint-fault-matrix-scenarios}"
output_file="${VELORIX_S3_CHECKPOINT_FAULT_MATRIX_EVIDENCE_PATH:-${product_dir}/s3-checkpoint-fault-matrix.json}"
run_compat_tests="${VELORIX_S3_CHECKPOINT_FAULT_MATRIX_RUN_COMPAT_TESTS:-1}"

usage() {
  cat <<'EOF'
Aggregate live S3-compatible checkpoint fault-matrix evidence.

Usage:
  scripts/run-s3-checkpoint-fault-matrix.sh

Required:
  VELORIX_S3_COMPAT=1
  AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
  VELORIX_S3_BUCKET
  VELORIX_PRODUCT_DEPLOYMENT_ID
  VELORIX_AUTHORITY_STORE_ID
  VELORIX_RELEASE_COMMIT
  VELORIX_API_IMAGE_DIGEST
  VELORIX_META_IMAGE_DIGEST

When VELORIX_S3_CHECKPOINT_FAULT_MATRIX_RUN_COMPAT_TESTS=1, this script runs
the live Rust S3 compatibility test that produces scenario files in:
  target/velorix-product/s3-checkpoint-fault-matrix-scenarios/

When VELORIX_S3_CHECKPOINT_FAULT_MATRIX_RUN_COMPAT_TESTS=0, scenario files
must already exist in that directory.

Override aggregate output with:
  VELORIX_S3_CHECKPOINT_FAULT_MATRIX_EVIDENCE_PATH=target/velorix-product/s3-checkpoint-fault-matrix.json

Required scenario file names:
  object_write_failure.json
  verification_read_failure.json
  manifest_write_failure.json
  metadata_cas_failure.json
  delayed_visibility.json
  retry_after_failure.json

Each scenario file must contain: {"name": "...", "status": "pass",
"live_s3_compatible": true}. This script aggregates evidence; it does not fake
or synthesize individual fault-injection scenario results.
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

required_env() {
  if [ -z "${!1:-}" ]; then
    echo "$1 is required" >&2
    exit 64
  fi
}

required_env VELORIX_S3_COMPAT
if [ "$VELORIX_S3_COMPAT" != "1" ]; then
  echo "VELORIX_S3_COMPAT must be 1" >&2
  exit 64
fi
required_env AWS_ENDPOINT_URL
required_env AWS_ACCESS_KEY_ID
required_env AWS_SECRET_ACCESS_KEY
required_env AWS_REGION
required_env VELORIX_S3_BUCKET
required_env VELORIX_PRODUCT_DEPLOYMENT_ID
required_env VELORIX_AUTHORITY_STORE_ID
required_env VELORIX_RELEASE_COMMIT
required_env VELORIX_API_IMAGE_DIGEST
required_env VELORIX_META_IMAGE_DIGEST

cd "$repo_root"
mkdir -p "$(dirname "$output_file")"
mkdir -p "$scenario_dir"

if [ "$run_compat_tests" = "1" ]; then
  rm -f \
    "$scenario_dir/object_write_failure.json" \
    "$scenario_dir/verification_read_failure.json" \
    "$scenario_dir/manifest_write_failure.json" \
    "$scenario_dir/metadata_cas_failure.json" \
    "$scenario_dir/delayed_visibility.json" \
    "$scenario_dir/retry_after_failure.json"
  export VELORIX_S3_CHECKPOINT_FAULT_MATRIX_SCENARIO_DIR="$scenario_dir"
  cargo test -p velorix-storage --test s3_compat --features s3-compat-tests -- --nocapture --test-threads=1
elif [ "$run_compat_tests" != "0" ]; then
  echo "VELORIX_S3_CHECKPOINT_FAULT_MATRIX_RUN_COMPAT_TESTS must be 0 or 1" >&2
  exit 64
fi

python3 - "$scenario_dir" "$output_file" "$AWS_ENDPOINT_URL" "$VELORIX_S3_BUCKET" "$VELORIX_PRODUCT_DEPLOYMENT_ID" "$VELORIX_AUTHORITY_STORE_ID" "$VELORIX_RELEASE_COMMIT" "$VELORIX_API_IMAGE_DIGEST" "$VELORIX_META_IMAGE_DIGEST" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

scenario_dir = Path(sys.argv[1])
output_file = Path(sys.argv[2])
endpoint = sys.argv[3]
bucket = sys.argv[4]
deployment_id = sys.argv[5]
authority_store_id = sys.argv[6]
source_revision = sys.argv[7]
api_image_digest = sys.argv[8]
meta_image_digest = sys.argv[9]

required = [
    "object_write_failure",
    "verification_read_failure",
    "manifest_write_failure",
    "metadata_cas_failure",
    "delayed_visibility",
    "retry_after_failure",
]
errors = []
scenarios = []

for name in required:
    path = scenario_dir / f"{name}.json"
    if not path.is_file():
        errors.append(f"missing scenario evidence: {path}")
        continue
    try:
        item = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"{path}: {exc}")
        continue
    if item.get("name") != name:
        errors.append(f"{path}: name must be {name}")
    if item.get("status") != "pass":
        errors.append(f"{path}: status must be pass")
    if item.get("live_s3_compatible") is not True:
        errors.append(f"{path}: live_s3_compatible must be true")
    scenarios.append({"name": name, "evidence": str(path), "status": item.get("status")})

if errors:
    print(json.dumps({"status": "fail", "errors": errors}, indent=2, sort_keys=True), file=sys.stderr)
    raise SystemExit(1)

payload = {
    "schema_version": 1,
    "evidence_kind": "s3_compatible_checkpoint_fault_matrix",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "status": "pass",
    "backend": "external-s3-compatible",
    "provider": "s3-compatible",
    "endpoint": endpoint,
    "bucket": bucket,
    "deployment_id": deployment_id,
    "authority_store_id": authority_store_id,
    "source_revision": source_revision,
    "deployed_image_digests": {
        "velorix-api": api_image_digest,
        "velorix-meta": meta_image_digest,
    },
    "live_s3_compatible": True,
    "delayed_visibility_cases_passed": True,
    "retry_fault_cases_passed": True,
    "mixed_checkpoint_publish_prevented": True,
    "object_refs_verified": True,
    "scenarios": scenarios,
}
output_file.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(f"wrote {output_file}")
PY

scripts/check-s3-checkpoint-fault-matrix-evidence.sh "$output_file"
