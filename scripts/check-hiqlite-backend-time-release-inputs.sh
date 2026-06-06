#!/usr/bin/env bash
set -euo pipefail
umask 077

case "$-" in
  *x*)
    echo "Refusing to run with shell xtrace enabled because release/Sigstore inputs may be logged" >&2
    exit 64
    ;;
esac

repo_root="$(git rev-parse --show-toplevel)"
product_evidence="${VELORIX_PRODUCT_EVIDENCE_PATH:-${VELORIX_VIND_PRODUCT_EVIDENCE:-${VELORIX_VIND_PRODUCT_DIR:-${repo_root}/target/velorix-product}/product-evidence.json}}"
output_file="${VELORIX_HIQLITE_BACKEND_TIME_RELEASE_PREFLIGHT:-}"
env_file="${VELORIX_HIQLITE_BACKEND_TIME_RELEASE_ENV_FILE:-}"
product_evidence_explicit=0
output_file_explicit=0

usage() {
  cat <<'EOF'
Check whether Hiqlite backend-time trusted release inputs are complete.

Usage:
  scripts/check-hiqlite-backend-time-release-inputs.sh \
    --product-evidence target/velorix-product/product-evidence.json

Options:
  --product-evidence PATH   Product evidence JSON to inspect.
  --output PATH             Output preflight JSON path.
  --env-file PATH           Source release/Sigstore inputs from a shell env file.

Environment equivalents:
  VELORIX_PRODUCT_EVIDENCE_PATH or VELORIX_VIND_PRODUCT_EVIDENCE
  VELORIX_HIQLITE_BACKEND_TIME_RELEASE_PREFLIGHT
  VELORIX_HIQLITE_BACKEND_TIME_RELEASE_ENV_FILE

This preflight does not create attestation evidence. It validates the local
evidence bundle shape and checks the release-only environment required before
scripts/attest-hiqlite-backend-time.sh can run with
VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --product-evidence)
      product_evidence="${2:-}"
      product_evidence_explicit=1
      shift 2
      ;;
    --output)
      output_file="${2:-}"
      output_file_explicit=1
      shift 2
      ;;
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
      echo "unknown option: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

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
  source "$env_path"
  for name in "$@"; do
    flag_var="__velorix_env_override_${name}"
    value_var="__velorix_env_override_value_${name}"
    if [ "${!flag_var}" = "1" ]; then
      export "$name=${!value_var}"
    fi
    unset "$flag_var" "$value_var"
  done
}

if [ -n "$env_file" ]; then
  if [ ! -f "$env_file" ]; then
    echo "--env-file does not exist: ${env_file}" >&2
    exit 66
  fi
  source_env_file_preserving_overrides "$env_file" \
    VELORIX_PRODUCT_EVIDENCE_PATH \
    VELORIX_VIND_PRODUCT_EVIDENCE \
    VELORIX_HIQLITE_BACKEND_TIME_RELEASE_PREFLIGHT \
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
  if [ "$product_evidence_explicit" = "0" ]; then
    product_evidence="${VELORIX_PRODUCT_EVIDENCE_PATH:-${VELORIX_VIND_PRODUCT_EVIDENCE:-$product_evidence}}"
  fi
  if [ "$output_file_explicit" = "0" ]; then
    output_file="${VELORIX_HIQLITE_BACKEND_TIME_RELEASE_PREFLIGHT:-$output_file}"
  fi
fi

if [ -z "$product_evidence" ]; then
  echo "--product-evidence or VELORIX_PRODUCT_EVIDENCE_PATH is required" >&2
  exit 64
fi

if [ -z "$output_file" ]; then
  output_file="$(dirname "$product_evidence")/hiqlite-backend-time-release-preflight.json"
fi

python3 - "$product_evidence" "$output_file" <<'PY'
import base64
import binascii
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

product_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
root = product_path.parent
missing = []
invalid = []
warnings = []
checks = {}

sha256_pattern = re.compile(r"^sha256:[0-9a-fA-F]{64}$")
hex_sha256_pattern = re.compile(r"^[0-9a-fA-F]{64}$")
git_sha_pattern = re.compile(r"^[0-9a-fA-F]{40}$")
trusted_repository = "github.com/mrchypark/velorix"
trusted_workflow_ref_prefix = "mrchypark/velorix/.github/workflows/release-gate.yml@"


def add_missing(subject, detail):
    missing.append({"subject": subject, "detail": detail})


def add_invalid(subject, detail):
    invalid.append({"subject": subject, "detail": detail})


def add_warning(subject, detail):
    warnings.append({"subject": subject, "detail": detail})


def set_check(name, passed, detail=None):
    checks[name] = {"passed": bool(passed)}
    if detail is not None:
        checks[name]["detail"] = detail


def load_json(path, label, required=True):
    if not path.is_file():
        if required:
            add_missing(label, f"missing {label}: {path}")
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        add_invalid(label, f"invalid JSON {label}: {path}: {exc}")
        return {}
    if not isinstance(value, dict):
        add_invalid(label, f"{label} must be a JSON object: {path}")
        return {}
    return value


def env(name):
    return os.environ.get(name, "").strip()


def require_env(name, secret=False):
    value = env(name)
    if not value:
        add_missing(name, f"{name} is required")
    elif "REPLACE_WITH" in value:
        add_invalid(name, f"{name} still contains a REPLACE_WITH placeholder")
    set_check(
        f"env.{name}",
        bool(value) and "REPLACE_WITH" not in value,
        {
            "present": bool(value),
            "placeholder": "REPLACE_WITH" in value,
            "secret": bool(secret),
            "length": len(value) if value else 0,
        },
    )
    return value


def is_sha256(value):
    return bool(sha256_pattern.match((value or "").strip()))


def is_hex_sha256(value):
    return bool(hex_sha256_pattern.match((value or "").strip()))


def require_sha(subject, value, allow_missing=False):
    value = (value or "").strip()
    if not value:
        if not allow_missing:
            add_missing(subject, f"{subject} is required")
        return value
    if not is_sha256(value):
        add_invalid(subject, f"{subject} must be a sha256 digest")
        return value
    return "sha256:" + value[len("sha256:") :].lower()


def require_hex_sha256(subject, value, allow_missing=True):
    value = (value or "").strip()
    if not value:
        if not allow_missing:
            add_missing(subject, f"{subject} is required")
        return value
    if not is_hex_sha256(value):
        add_invalid(subject, f"{subject} must be a 64-character hex sha256")
    return value


def require_git_sha(subject, value, allow_missing=False):
    value = (value or "").strip()
    if not value:
        if not allow_missing:
            add_missing(subject, f"{subject} is required")
        return value
    lowered = value.lower()
    if not git_sha_pattern.match(value):
        add_invalid(subject, f"{subject} must be a full 40-character git SHA")
    elif "placeholder" in lowered or lowered in {"unknown", "local"} or "+dirty" in lowered:
        add_invalid(subject, f"{subject} must be clean and non-placeholder")
    return value


def is_full_git_sha(value):
    return bool(git_sha_pattern.match((value or "").strip()))


def trusted_release_ref(value, subject):
    value = (value or "").strip()
    if not value:
        return ""
    if value == "refs/heads/main" or (value.startswith("refs/tags/v") and value[len("refs/tags/v") :].strip()):
        return value
    add_invalid(subject, f"{subject} must use refs/heads/main or refs/tags/v*")
    return value


def workflow_release_ref(value, subject):
    value = (value or "").strip()
    if not value:
        return ""
    if not value.startswith(trusted_workflow_ref_prefix):
        add_invalid(subject, f"{subject} must start with {trusted_workflow_ref_prefix}")
        return ""
    return trusted_release_ref(value[len(trusted_workflow_ref_prefix) :], subject)


def pointer(value, path):
    current = value
    for part in path.strip("/").split("/"):
        if part == "":
            continue
        part = part.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            index = int(part)
            current = current[index] if index < len(current) else None
        else:
            return None
    return current


def deployed_digest(product, role):
    return str(pointer(product, f"/deployed_images/{role}/image_digest") or "").strip()


def compare_digest(subject, supplied, expected):
    if supplied and expected and supplied != expected:
        add_invalid(subject, f"{subject} must match {expected}")


def compare_value(subject, supplied, expected):
    if supplied != expected:
        add_invalid(subject, f"{subject} must equal {expected!r}")


def positive_int(value):
    return isinstance(value, int) and value > 0


def sigstore_certificate_raw_bytes(bundle):
    material = bundle.get("verificationMaterial")
    if not isinstance(material, dict):
        add_invalid("VELORIX_CI_SIGSTORE_BUNDLE_BASE64", "Sigstore bundle missing verificationMaterial")
        return ""
    certificate = material.get("certificate")
    if isinstance(certificate, dict) and isinstance(certificate.get("rawBytes"), str):
        return certificate["rawBytes"]
    chain = material.get("x509CertificateChain")
    certificates = chain.get("certificates") if isinstance(chain, dict) else None
    if (
        isinstance(certificates, list)
        and certificates
        and isinstance(certificates[0], dict)
        and isinstance(certificates[0].get("rawBytes"), str)
    ):
        return certificates[0]["rawBytes"]
    add_invalid("VELORIX_CI_SIGSTORE_BUNDLE_BASE64", "Sigstore bundle missing signing certificate rawBytes")
    return ""


def validate_sigstore_bundle_shape(decoded):
    try:
        bundle = json.loads(decoded.decode("utf-8"))
    except Exception as exc:
        add_invalid("VELORIX_CI_SIGSTORE_BUNDLE_BASE64", f"Sigstore bundle must decode to JSON: {exc}")
        return
    if not isinstance(bundle, dict):
        add_invalid("VELORIX_CI_SIGSTORE_BUNDLE_BASE64", "Sigstore bundle must decode to a JSON object")
        return
    raw_certificate = sigstore_certificate_raw_bytes(bundle)
    if raw_certificate:
        try:
            base64.b64decode(raw_certificate, validate=True)
        except binascii.Error as exc:
            add_invalid("VELORIX_CI_SIGSTORE_BUNDLE_BASE64", f"Sigstore certificate rawBytes must be base64: {exc}")
    entries = (bundle.get("verificationMaterial") or {}).get("tlogEntries")
    if not isinstance(entries, list) or not entries or not isinstance(entries[0], dict):
        add_invalid("VELORIX_CI_SIGSTORE_BUNDLE_BASE64", "Sigstore bundle missing Rekor tlogEntries")
        return
    first_entry = entries[0]
    if not ((first_entry.get("logId") or {}).get("keyId")):
        add_invalid("VELORIX_CI_SIGSTORE_BUNDLE_BASE64", "Sigstore bundle missing tlogEntries[0].logId.keyId")
    if not isinstance(first_entry.get("inclusionProof"), dict):
        add_invalid("VELORIX_CI_SIGSTORE_BUNDLE_BASE64", "Sigstore bundle missing tlogEntries[0].inclusionProof")


product = load_json(product_path, "product evidence")
assessment = load_json(root / "hiqlite-backend-time-assessment.json", "Hiqlite backend-time assessment")
readyz = load_json(root / "readyz.json", "readyz evidence")
multi = load_json(root / "multi-replica-fencing-smoke.json", "multi-replica fencing smoke")
failover = load_json(root / "standing-runtime-failover-smoke.json", "standing-runtime failover smoke")

meta_log_path = root / "velorix-meta-smoke.log"
if not meta_log_path.is_file():
    add_missing("velorix-meta-smoke.log", f"missing metadata adversarial smoke log: {meta_log_path}")
else:
    set_check("sibling.velorix-meta-smoke.log", True, {"path": str(meta_log_path)})

backend = pointer(product, "/metadata_store/backend")
set_check("product.metadata_store.backend", backend == "hiqlite", {"value": backend})
if backend != "hiqlite":
    add_invalid("metadata_store.backend", "product metadata_store.backend must be hiqlite")

api_deployed_digest = deployed_digest(product, "velorix-api")
meta_deployed_digest = deployed_digest(product, "velorix-meta")
authority_digest = str(pointer(product, "/metadata_store/hiqlite_authority_attestation/image_digest") or "").strip()
authority_source_revision = str(pointer(product, "/metadata_store/hiqlite_authority_attestation/source_revision") or "").strip()

for subject, value in [
    ("deployed_images.velorix-api.image_digest", api_deployed_digest),
    ("deployed_images.velorix-meta.image_digest", meta_deployed_digest),
    ("metadata_store.hiqlite_authority_attestation.image_digest", authority_digest),
]:
    require_sha(subject, value)
    set_check(subject, is_sha256(value), {"present": bool(value)})

if assessment:
    compare_value(
        "assessment.evidence_kind",
        assessment.get("evidence_kind"),
        "velorix_hiqlite_backend_time_assessment",
    )
    compare_value(
        "assessment.backend_time_source_kind",
        assessment.get("backend_time_source_kind"),
        "raft_replicated_authority_time",
    )
    can_attest = assessment.get("can_generate_product_complete_backend_time_attestation") is True
    set_check("assessment.can_generate_product_complete_backend_time_attestation", can_attest)
    if not can_attest:
        add_invalid(
            "assessment.can_generate_product_complete_backend_time_attestation",
            "assessment cannot generate product-complete backend-time attestation",
        )

readyz_backend_time = pointer(readyz, "/metadata_store/standing_runtime_fencing/backend_time_source_kind")
if readyz and readyz_backend_time != "raft_replicated_authority_time":
    add_warning("readyz.metadata_store.standing_runtime_fencing.backend_time_source_kind", "readyz does not advertise raft_replicated_authority_time backend-time source")
set_check(
    "readyz.backend_time_source_kind",
    readyz_backend_time == "raft_replicated_authority_time",
    {"value": readyz_backend_time},
)

multi_pass = multi.get("status") == "pass" if multi else False
set_check("multi_replica_fencing_smoke.status", multi_pass, {"value": multi.get("status") if multi else None})
if multi and not multi_pass:
    add_invalid("multi-replica fencing smoke", "multi-replica fencing smoke must pass")

if failover:
    expected = {
        "trusted_for_product_complete": True,
        "production_wall_clock_failover_attestation": True,
        "evidence_scope": "release_ci_deployed_product",
        "failover_probe_kind": "release_bounded_wall_clock_failover",
        "backend_time_source_kind": "raft_replicated_authority_time",
        "authority_time_observed": True,
    }
    release_failover_required_messages = {
        "trusted_for_product_complete": "failover evidence requires trusted_for_product_complete=True",
        "production_wall_clock_failover_attestation": "failover evidence requires production_wall_clock_failover_attestation=True",
        "evidence_scope": "failover evidence requires evidence_scope='release_ci_deployed_product'",
        "failover_probe_kind": "failover evidence requires failover_probe_kind='release_bounded_wall_clock_failover'",
        "backend_time_source_kind": "failover evidence requires backend_time_source_kind='raft_replicated_authority_time'",
        "authority_time_observed": "failover evidence requires authority_time_observed=True",
    }
    for key, expected_value in expected.items():
        passed = failover.get(key) == expected_value
        set_check(f"failover.{key}", passed, {"value": failover.get(key), "expected": expected_value})
        if not passed:
            add_invalid(f"failover.{key}", release_failover_required_messages[key])

    pre_epoch = failover.get("pre_failover_owner_epoch")
    post_epoch = failover.get("post_failover_owner_epoch")
    epoch_passed = isinstance(pre_epoch, int) and isinstance(post_epoch, int) and post_epoch > pre_epoch
    set_check("failover.owner_epoch_increases", epoch_passed, {"pre": pre_epoch, "post": post_epoch})
    if not epoch_passed:
        add_invalid("failover.owner_epoch", "failover evidence requires post_failover_owner_epoch > pre_failover_owner_epoch")

    affected_pods = failover.get("affected_api_pods")
    affected_passed = isinstance(affected_pods, list) and bool(affected_pods)
    set_check("failover.affected_api_pods", affected_passed)
    if not affected_passed:
        add_invalid("failover.affected_api_pods", "failover evidence requires affected_api_pods")

    assessment_capability = assessment.get("product_capability") or {}
    failover_bound = assessment_capability.get("failover_time_bound_ms")
    max_owner_ttl = assessment_capability.get("max_owner_ttl_ms")
    owner_ttl = failover.get("owner_ttl_ms")
    failover_time_bound = failover.get("failover_time_bound_ms")
    if positive_int(failover_bound):
        if owner_ttl != failover_bound:
            add_invalid("failover.owner_ttl_ms", "failover owner_ttl_ms must equal capability.failover_time_bound_ms")
        if failover_time_bound != failover_bound:
            add_invalid("failover.failover_time_bound_ms", "failover failover_time_bound_ms must equal capability.failover_time_bound_ms")
    if positive_int(max_owner_ttl) and owner_ttl and owner_ttl > max_owner_ttl:
        add_invalid("failover.owner_ttl_ms", "failover owner_ttl_ms must not exceed capability.max_owner_ttl_ms")

trusted_provenance = env("VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE")
set_check(
    "env.VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE",
    trusted_provenance == "1",
    {"present": bool(trusted_provenance)},
)
if trusted_provenance != "1":
    add_missing(
        "VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE",
        "VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1 is required",
    )

source_revision = require_git_sha(
    "VELORIX_SOURCE_REVISION",
    require_env("VELORIX_SOURCE_REVISION"),
    allow_missing=True,
)
source_repository = require_env("VELORIX_SOURCE_REPOSITORY")
if source_repository and source_repository != "github.com/mrchypark/velorix":
    add_invalid(
        "VELORIX_SOURCE_REPOSITORY",
        "VELORIX_SOURCE_REPOSITORY must be github.com/mrchypark/velorix",
    )
release_commit = require_git_sha("VELORIX_RELEASE_COMMIT", require_env("VELORIX_RELEASE_COMMIT"), allow_missing=True)
if source_revision and release_commit and source_revision != release_commit:
    add_invalid("VELORIX_SOURCE_REVISION", "VELORIX_SOURCE_REVISION must match VELORIX_RELEASE_COMMIT")
authority_source_revision_sha = authority_source_revision if git_sha_pattern.match(authority_source_revision) else ""
if authority_source_revision_sha:
    if source_revision == authority_source_revision_sha:
        add_invalid(
            "VELORIX_SOURCE_REVISION",
            "VELORIX_SOURCE_REVISION must be the Velorix release commit, not metadata_store.hiqlite_authority_attestation.source_revision",
        )
    if release_commit == authority_source_revision_sha:
        add_invalid(
            "VELORIX_RELEASE_COMMIT",
            "VELORIX_RELEASE_COMMIT must be the Velorix release commit, not metadata_store.hiqlite_authority_attestation.source_revision",
        )

api_digest = require_sha("VELORIX_API_IMAGE_DIGEST", require_env("VELORIX_API_IMAGE_DIGEST"), allow_missing=True)
meta_digest = require_sha("VELORIX_META_IMAGE_DIGEST", require_env("VELORIX_META_IMAGE_DIGEST"), allow_missing=True)
hiqlite_digest = require_sha(
    "VELORIX_HIQLITE_IMAGE_DIGEST",
    env("VELORIX_HIQLITE_IMAGE_DIGEST") or env("VELORIX_SUBJECT_IMAGE_DIGEST") or authority_digest,
)
compare_digest("VELORIX_API_IMAGE_DIGEST", api_digest, api_deployed_digest)
compare_digest("VELORIX_META_IMAGE_DIGEST", meta_digest, meta_deployed_digest)
compare_digest("VELORIX_HIQLITE_IMAGE_DIGEST", hiqlite_digest, authority_digest)

subject_images = {
    "velorix-api": api_digest,
    "velorix-meta": meta_digest,
    "hiqlite": hiqlite_digest,
}
set_check(
    "subject_images.bound_to_product_evidence",
    all(is_sha256(value) for value in subject_images.values())
    and api_digest == api_deployed_digest
    and meta_digest == meta_deployed_digest
    and hiqlite_digest == authority_digest,
    {"roles": sorted(subject_images)},
)

ci_workflow_name = require_env("VELORIX_CI_WORKFLOW_NAME")
ci_workflow_run_id = require_env("VELORIX_CI_WORKFLOW_RUN_ID")
ci_job_name = require_env("VELORIX_CI_JOB_NAME")
ci_oidc_subject = require_env("VELORIX_CI_OIDC_SUBJECT")
ci_workflow_ref = require_env("VELORIX_CI_WORKFLOW_REF")
ci_job_workflow_ref = require_env("VELORIX_CI_JOB_WORKFLOW_REF")

workflow_ref_release_ref = workflow_release_ref(ci_workflow_ref, "VELORIX_CI_WORKFLOW_REF")
expected_oidc_subject = f"repo:mrchypark/velorix:ref:{workflow_ref_release_ref}" if workflow_ref_release_ref else ""
if ci_oidc_subject and expected_oidc_subject and ci_oidc_subject != expected_oidc_subject:
    add_invalid("VELORIX_CI_OIDC_SUBJECT", "VELORIX_CI_OIDC_SUBJECT must match trusted release workflow ref")
expected_job_workflow_ref = (
    f"{trusted_workflow_ref_prefix}{release_commit}" if is_full_git_sha(release_commit) else ""
)
if ci_job_workflow_ref and expected_job_workflow_ref and ci_job_workflow_ref != expected_job_workflow_ref:
    add_invalid("VELORIX_CI_JOB_WORKFLOW_REF", "VELORIX_CI_JOB_WORKFLOW_REF must match VELORIX_RELEASE_COMMIT")
set_check(
    "ci_identity.workflow_ref",
    bool(workflow_ref_release_ref)
    and (not ci_oidc_subject or ci_oidc_subject == expected_oidc_subject)
    and (not expected_job_workflow_ref or ci_job_workflow_ref == expected_job_workflow_ref),
    {
        "workflow_ref_release_ref": workflow_ref_release_ref or None,
        "expected_oidc_subject": expected_oidc_subject or None,
        "expected_job_workflow_ref": expected_job_workflow_ref or None,
        "workflow_name_present": bool(ci_workflow_name),
        "workflow_run_id_present": bool(ci_workflow_run_id),
        "job_name_present": bool(ci_job_name),
    },
)

sigstore_bundle = env("VELORIX_CI_SIGSTORE_BUNDLE_BASE64")
sigstore_bundle_bytes = b""
set_check(
    "env.VELORIX_CI_SIGSTORE_BUNDLE_BASE64",
    bool(sigstore_bundle),
    {"present": bool(sigstore_bundle), "secret": True, "length": len(sigstore_bundle) if sigstore_bundle else 0},
)
if not sigstore_bundle:
    add_missing(
        "VELORIX_CI_SIGSTORE_BUNDLE_BASE64",
        "VELORIX_CI_SIGSTORE_BUNDLE_BASE64 is required for product-complete release readiness",
    )
else:
    try:
        sigstore_bundle_bytes = base64.b64decode(sigstore_bundle, validate=True)
    except binascii.Error as exc:
        add_invalid("VELORIX_CI_SIGSTORE_BUNDLE_BASE64", f"VELORIX_CI_SIGSTORE_BUNDLE_BASE64 must be valid base64: {exc}")
    else:
        set_check("env.VELORIX_CI_SIGSTORE_BUNDLE_BASE64.base64", bool(sigstore_bundle_bytes), {"decoded_bytes": len(sigstore_bundle_bytes)})
        validate_sigstore_bundle_shape(sigstore_bundle_bytes)

sigstore_certificate_identity = require_env("VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY")
certificate_identity_release_ref = ""
if sigstore_certificate_identity:
    certificate_identity_release_ref = workflow_release_ref(
        sigstore_certificate_identity.removeprefix("https://github.com/"),
        "VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY",
    )
    if workflow_ref_release_ref and certificate_identity_release_ref and certificate_identity_release_ref != workflow_ref_release_ref:
        add_invalid(
            "VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY",
            "VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY must match trusted release workflow ref",
        )
set_check(
    "ci_identity.sigstore_certificate_identity",
    bool(certificate_identity_release_ref)
    and (not workflow_ref_release_ref or certificate_identity_release_ref == workflow_ref_release_ref),
    {
        "certificate_identity_release_ref": certificate_identity_release_ref or None,
        "workflow_ref_release_ref": workflow_ref_release_ref or None,
    },
)
sigstore_bundle_sha256 = require_sha(
    "VELORIX_CI_SIGSTORE_BUNDLE_SHA256",
    env("VELORIX_CI_SIGSTORE_BUNDLE_SHA256"),
    allow_missing=False,
)
if sigstore_bundle_bytes and sigstore_bundle_sha256:
    actual_sigstore_bundle_sha256 = "sha256:" + hashlib.sha256(sigstore_bundle_bytes).hexdigest()
    if sigstore_bundle_sha256 != actual_sigstore_bundle_sha256:
        add_invalid(
            "VELORIX_CI_SIGSTORE_BUNDLE_SHA256",
            "VELORIX_CI_SIGSTORE_BUNDLE_SHA256 must match VELORIX_CI_SIGSTORE_BUNDLE_BASE64",
        )

status = "pass" if not missing and not invalid else "blocked"
next_action = (
    "Provide the missing release CI environment, product image digests, "
    "sigstore bundle, and release-scoped failover evidence; then run "
    "VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1 "
    "scripts/attest-hiqlite-backend-time.sh --product-evidence "
    f"{product_path} --output {root / 'hiqlite-backend-time-attestation.json'} --update-product-evidence"
)

payload = {
    "schema_version": 1,
    "report_kind": "velorix_hiqlite_backend_time_release_preflight",
    "evidence_kind": "velorix_hiqlite_backend_time_release_preflight",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "product_evidence": str(product_path),
    "output": str(output_path),
    "status": status,
    "missing": missing,
    "invalid": invalid,
    "warnings": warnings,
    "errors": [item["detail"] for item in missing + invalid],
    "checks": checks,
    "subject_images": subject_images,
    "canonical_bundle_entries": [
        "product_evidence",
        "hiqlite_backend_time_assessment",
        "readyz",
        "multi_replica_fencing_smoke",
        "standing_runtime_failover_smoke",
        "velorix_meta_smoke_log",
    ],
    "next_action": next_action,
    "required_next_command": (
        "VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1 "
        "scripts/attest-hiqlite-backend-time.sh --product-evidence "
        f"{product_path} --output {root / 'hiqlite-backend-time-attestation.json'} --update-product-evidence"
    ),
}

output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

print(f"status={status}")
print(f"preflight={output_path}")
if missing:
    print("missing:")
    for item in missing:
        print(f"- {item['detail']}")
if invalid:
    print("invalid:")
    for item in invalid:
        print(f"- {item['detail']}")
raise SystemExit(0 if status == "pass" else 1)
PY
