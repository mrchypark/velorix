#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_evidence="${VELORIX_PRODUCT_EVIDENCE_PATH:-${VELORIX_VIND_PRODUCT_EVIDENCE:-${VELORIX_VIND_PRODUCT_DIR:-${repo_root}/target/velorix-product}/product-evidence.json}}"
output_file="${VELORIX_HIQLITE_BACKEND_TIME_RELEASE_ENV:-}"
report_file="${VELORIX_HIQLITE_BACKEND_TIME_RELEASE_ENV_REPORT:-}"

usage() {
  cat <<'EOF'
Write a release-CI env template for trusted Hiqlite backend-time attestation.

Usage:
  scripts/write-hiqlite-backend-time-release-env.sh \
    --product-evidence target/velorix-product/product-evidence.json

Options:
  --product-evidence PATH   Product evidence JSON to inspect.
  --output PATH             Shell env output path.
  --report PATH             JSON report output path.

Environment equivalents:
  VELORIX_PRODUCT_EVIDENCE_PATH or VELORIX_VIND_PRODUCT_EVIDENCE
  VELORIX_HIQLITE_BACKEND_TIME_RELEASE_ENV
  VELORIX_HIQLITE_BACKEND_TIME_RELEASE_ENV_REPORT

The generated env file is a template. It fills values that can be derived from
product evidence, such as deployed image digests, and leaves CI/Sigstore values
as explicit REPLACE_* placeholders. It does not create release provenance.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --product-evidence)
      product_evidence="${2:-}"
      shift 2
      ;;
    --output)
      output_file="${2:-}"
      shift 2
      ;;
    --report)
      report_file="${2:-}"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

if [ -z "$product_evidence" ]; then
  echo "--product-evidence or VELORIX_PRODUCT_EVIDENCE_PATH is required" >&2
  exit 64
fi
if [ -z "$output_file" ]; then
  output_file="$(dirname "$product_evidence")/hiqlite-backend-time-release.env"
fi
if [ -z "$report_file" ]; then
  report_file="$(dirname "$product_evidence")/hiqlite-backend-time-release-env.json"
fi

python3 - "$product_evidence" "$output_file" "$report_file" <<'PY'
import json
import os
import re
import shlex
import sys
from datetime import datetime, timezone
from pathlib import Path

product_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
report_path = Path(sys.argv[3])
sha256_pattern = re.compile(r"^sha256:[0-9a-fA-F]{64}$")
git_sha_pattern = re.compile(r"^[0-9a-fA-F]{40}$")
placeholder_commit = "REPLACE_WITH_40_CHAR_RELEASE_COMMIT"
release_ref = os.environ.get("GITHUB_REF", "refs/heads/main").strip() or "refs/heads/main"
if release_ref != "refs/heads/main" and not release_ref.startswith("refs/tags/v"):
    release_ref = "refs/heads/main"


def pointer(value, path):
    current = value
    for part in path.strip("/").split("/"):
        if not part:
            continue
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def clean_git_sha(value):
    value = (value or "").strip()
    return value if git_sha_pattern.match(value) else ""


github_sha = clean_git_sha(os.environ.get("GITHUB_SHA", ""))


def sha256_or_placeholder(value, placeholder):
    value = (value or "").strip()
    return value if sha256_pattern.match(value) else placeholder


if not product_path.is_file():
    raise SystemExit(f"missing product evidence: {product_path}")
with product_path.open("r", encoding="utf-8") as f:
    product = json.load(f)
if product.get("evidence_kind") != "velorix_product_slice_evidence":
    raise SystemExit("product evidence_kind must be velorix_product_slice_evidence")

api_digest = sha256_or_placeholder(
    pointer(product, "/deployed_images/velorix-api/image_digest"),
    "sha256:REPLACE_WITH_VELORIX_API_IMAGE_DIGEST",
)
meta_digest = sha256_or_placeholder(
    pointer(product, "/deployed_images/velorix-meta/image_digest"),
    "sha256:REPLACE_WITH_VELORIX_META_IMAGE_DIGEST",
)
hiqlite_digest = sha256_or_placeholder(
    pointer(product, "/metadata_store/hiqlite_authority_attestation/image_digest"),
    "sha256:REPLACE_WITH_HIQLITE_IMAGE_DIGEST",
)
authority_source_revision = str(
    pointer(product, "/metadata_store/hiqlite_authority_attestation/source_revision") or ""
)
# This is the Hiqlite authority source. It is reported for audit only and must
# never become the Velorix product release commit.
source_revision_env = clean_git_sha(os.environ.get("VELORIX_SOURCE_REVISION", ""))
source_revision = source_revision_env or github_sha or placeholder_commit
release_commit_env = clean_git_sha(os.environ.get("VELORIX_RELEASE_COMMIT", ""))
release_commit = release_commit_env or source_revision
if release_commit == placeholder_commit:
    source_revision = placeholder_commit
source_revision_source = (
    "VELORIX_SOURCE_REVISION"
    if source_revision_env
    else ("GITHUB_SHA" if github_sha and source_revision == github_sha else "placeholder")
)
release_commit_source = (
    "VELORIX_RELEASE_COMMIT"
    if release_commit_env
    else ("VELORIX_SOURCE_REVISION" if release_commit != placeholder_commit else "placeholder")
)

workflow_ref = f"mrchypark/velorix/.github/workflows/release-gate.yml@{release_ref}"
job_workflow_ref = (
    f"mrchypark/velorix/.github/workflows/release-gate.yml@{release_commit}"
    if release_commit != placeholder_commit
    else "mrchypark/velorix/.github/workflows/release-gate.yml@REPLACE_WITH_40_CHAR_RELEASE_COMMIT"
)
certificate_identity = f"https://github.com/mrchypark/velorix/.github/workflows/release-gate.yml@{release_ref}"

values = {
    "VELORIX_PRODUCT_EVIDENCE_PATH": str(product_path),
    "VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE": "1",
    "VELORIX_SOURCE_REPOSITORY": "github.com/mrchypark/velorix",
    "VELORIX_SOURCE_REVISION": source_revision,
    "VELORIX_RELEASE_COMMIT": release_commit,
    "VELORIX_API_IMAGE_DIGEST": api_digest,
    "VELORIX_META_IMAGE_DIGEST": meta_digest,
    "VELORIX_HIQLITE_IMAGE_DIGEST": hiqlite_digest,
    "VELORIX_CI_WORKFLOW_NAME": os.environ.get("GITHUB_WORKFLOW", "release-gate"),
    "VELORIX_CI_WORKFLOW_RUN_ID": os.environ.get("GITHUB_RUN_ID", "REPLACE_WITH_GITHUB_RUN_ID"),
    "VELORIX_CI_JOB_NAME": os.environ.get("GITHUB_JOB", "release-product-complete"),
    "VELORIX_CI_OIDC_SUBJECT": f"repo:mrchypark/velorix:ref:{release_ref}",
    "VELORIX_CI_WORKFLOW_REF": workflow_ref,
    "VELORIX_CI_JOB_WORKFLOW_REF": job_workflow_ref,
    "VELORIX_CI_SIGSTORE_BUNDLE_BASE64": "REPLACE_WITH_SIGSTORE_BUNDLE_BASE64",
    "VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY": certificate_identity,
    "VELORIX_CI_SIGSTORE_BUNDLE_SHA256": "sha256:REPLACE_WITH_SIGSTORE_BUNDLE_SHA256",
}

placeholders = [
    key
    for key, value in values.items()
    if "REPLACE_WITH" in value
]
derived = [
    key
    for key in [
        "VELORIX_API_IMAGE_DIGEST",
        "VELORIX_META_IMAGE_DIGEST",
        "VELORIX_HIQLITE_IMAGE_DIGEST",
        "VELORIX_SOURCE_REVISION",
        "VELORIX_RELEASE_COMMIT",
    ]
    if key not in placeholders
]
fixed_release_values = [
    "VELORIX_SOURCE_REPOSITORY",
]

lines = [
    "# Generated by scripts/write-hiqlite-backend-time-release-env.sh",
    "# Source this only in trusted release CI after replacing every REPLACE_WITH_* value.",
    "# Trusted release refs must be refs/heads/main or refs/tags/v*.",
    "",
]
for key, value in values.items():
    lines.append(f"export {key}={shlex.quote(value)}")
lines.extend(
    [
        "",
        "# After replacing placeholders, verify before attesting:",
        f"# scripts/check-hiqlite-backend-time-release-inputs.sh --env-file {output_path} --product-evidence \"$VELORIX_PRODUCT_EVIDENCE_PATH\"",
        "# Then regenerate the attestation:",
        "# scripts/attest-hiqlite-backend-time.sh --product-evidence \"$VELORIX_PRODUCT_EVIDENCE_PATH\" --output \"$(dirname \"$VELORIX_PRODUCT_EVIDENCE_PATH\")/hiqlite-backend-time-attestation.json\" --update-product-evidence",
        "",
    ]
)

output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text("\n".join(lines), encoding="utf-8")
os.chmod(output_path, 0o600)

report = {
    "schema_version": 1,
    "report_kind": "velorix_hiqlite_backend_time_release_env_template",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "product_evidence": str(product_path),
    "env_file": str(output_path),
    "placeholders": placeholders,
    "derived_from_product_evidence": derived,
    "fixed_release_values": fixed_release_values,
    "trusted_release_ref": release_ref,
    "source_revision_source": source_revision_source,
    "release_commit_source": release_commit_source,
    "hiqlite_authority_source_revision": authority_source_revision or None,
    "ready_for_preflight": not placeholders,
    "next_action": (
        f"Replace placeholders in {output_path}, source it in release CI, then run "
        "scripts/check-hiqlite-backend-time-release-inputs.sh"
    ),
}
report_path.parent.mkdir(parents=True, exist_ok=True)
report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.chmod(report_path, 0o600)

print(f"release_env={output_path}")
print(f"release_env_report={report_path}")
print(f"placeholders={len(placeholders)}")
PY
