#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
product_evidence="${VELORIX_PRODUCT_EVIDENCE_PATH:-${VELORIX_VIND_PRODUCT_EVIDENCE:-${repo_root}/target/velorix-product/product-evidence.json}}"
assessment=""
readyz=""
multi_replica=""
failover=""
meta_smoke_log=""
output_file="${VELORIX_HIQLITE_BACKEND_TIME_ATTESTATION_FILE:-}"
attester="${VELORIX_ATTESTER:-$(id -un 2>/dev/null || printf 'operator')}"
update_product_evidence="${VELORIX_HIQLITE_BACKEND_TIME_ATTESTATION_UPDATE_PRODUCT_EVIDENCE:-0}"

usage() {
  cat <<'EOF'
Generate a Hiqlite backend-time attestation candidate from deployed product smoke evidence.

Usage:
  scripts/attest-hiqlite-backend-time.sh \
    --product-evidence target/velorix-product/product-evidence.json \
    --output target/velorix-product/hiqlite-backend-time-attestation.json

Environment equivalents:
  VELORIX_PRODUCT_EVIDENCE_PATH or VELORIX_VIND_PRODUCT_EVIDENCE
  VELORIX_HIQLITE_BACKEND_TIME_ATTESTATION_FILE
  VELORIX_HIQLITE_BACKEND_TIME_ATTESTATION_UPDATE_PRODUCT_EVIDENCE=1
  VELORIX_ATTESTER
  VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1
  VELORIX_SOURCE_REPOSITORY
  VELORIX_SOURCE_REVISION
  VELORIX_RELEASE_COMMIT
  VELORIX_CI_WORKFLOW_NAME
  VELORIX_CI_WORKFLOW_RUN_ID
  VELORIX_CI_JOB_NAME
  VELORIX_CI_OIDC_SUBJECT
  VELORIX_CI_WORKFLOW_REF
  VELORIX_CI_JOB_WORKFLOW_REF
  VELORIX_CI_SIGNING_CERTIFICATE_SHA256
  VELORIX_CI_SIGNATURE_ALGORITHM=ed25519
  VELORIX_CI_PUBLIC_KEY_BASE64
  VELORIX_CI_PUBLIC_KEY_SHA256
  VELORIX_CI_SIGNATURE_BASE64
  VELORIX_CI_SIGSTORE_BUNDLE_BASE64
  VELORIX_CI_SIGSTORE_BUNDLE_SHA256
  VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY
  VELORIX_HIQLITE_BACKEND_TIME_CANONICAL_BUNDLE_FILE
  VELORIX_CI_TRANSPARENCY_LOG_ID
  VELORIX_CI_TRANSPARENCY_LOG_INDEX
  VELORIX_CI_INCLUSION_PROOF_SHA256
  VELORIX_API_IMAGE_DIGEST
  VELORIX_META_IMAGE_DIGEST
  VELORIX_HIQLITE_IMAGE_DIGEST or VELORIX_SUBJECT_IMAGE_DIGEST

By default this reads sibling evidence beside product-evidence.json:
  hiqlite-backend-time-assessment.json
  readyz.json
  multi-replica-fencing-smoke.json
  standing-runtime-failover-smoke.json
  velorix-meta-smoke.log

The evidence file is intentionally diagnostic. It proves that a deployed
vind product run produced the currently required smoke evidence and records the
core backend-time claim shape. It becomes release-validator trusted only when
VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1 supplies CI provenance over
the canonical evidence bundle.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --product-evidence)
      product_evidence="${2:-}"
      shift 2
      ;;
    --assessment)
      assessment="${2:-}"
      shift 2
      ;;
    --readyz)
      readyz="${2:-}"
      shift 2
      ;;
    --multi-replica)
      multi_replica="${2:-}"
      shift 2
      ;;
    --failover)
      failover="${2:-}"
      shift 2
      ;;
    --meta-smoke-log)
      meta_smoke_log="${2:-}"
      shift 2
      ;;
    --output)
      output_file="${2:-}"
      shift 2
      ;;
    --attester)
      attester="${2:-}"
      shift 2
      ;;
    --update-product-evidence)
      update_product_evidence=1
      shift
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
  output_file="$(dirname "$product_evidence")/hiqlite-backend-time-attestation.json"
fi

python3 - "$product_evidence" "$assessment" "$readyz" "$multi_replica" "$failover" "$meta_smoke_log" "$output_file" "$attester" "$update_product_evidence" <<'PY'
import base64
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

product_path = Path(sys.argv[1])
assessment_path = Path(sys.argv[2]) if sys.argv[2] else product_path.parent / "hiqlite-backend-time-assessment.json"
readyz_path = Path(sys.argv[3]) if sys.argv[3] else product_path.parent / "readyz.json"
multi_replica_path = Path(sys.argv[4]) if sys.argv[4] else product_path.parent / "multi-replica-fencing-smoke.json"
failover_path = Path(sys.argv[5]) if sys.argv[5] else product_path.parent / "standing-runtime-failover-smoke.json"
meta_smoke_log_path = Path(sys.argv[6]) if sys.argv[6] else product_path.parent / "velorix-meta-smoke.log"
output_path = Path(sys.argv[7])
attester = sys.argv[8]
update_product_evidence = sys.argv[9] == "1"
trusted_provenance_enabled = os.environ.get("VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE") == "1"


def load_json(path: Path, description: str) -> dict:
    if not path.is_file():
        raise SystemExit(f"missing {description}: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid JSON in {description} {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise SystemExit(f"{description} must be a JSON object: {path}")
    return data


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def pointer(data: dict, path: str):
    current = data
    for part in path.strip("/").split("/"):
        if not isinstance(current, dict) or part not in current:
            raise SystemExit(f"missing required field {path}")
        current = current[part]
    return current


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalized_product_evidence_bytes(path: Path) -> bytes:
    product = load_json(path, "product evidence for backend-time canonicalization")
    metadata_store = product.get("metadata_store")
    if isinstance(metadata_store, dict):
        metadata_store.pop("hiqlite_backend_time_attestation", None)
    return json.dumps(product, sort_keys=True, separators=(",", ":")).encode("utf-8")


def evidence_file(path: Path, kind: str) -> dict:
    if not path.is_file():
        raise SystemExit(f"missing {kind}: {path}")
    if kind == "product_evidence":
        normalized = normalized_product_evidence_bytes(path)
        return {
            "kind": kind,
            "path": path.name,
            "sha256": hashlib.sha256(normalized).hexdigest(),
            "size_bytes": len(normalized),
            "canonicalization": "without_metadata_store_hiqlite_backend_time_attestation",
        }
    return {
        "kind": kind,
        "path": path.name,
        "sha256": sha256(path),
        "size_bytes": path.stat().st_size,
    }


def canonical_bundle_sha256(entries: list[dict]) -> str:
    digest = hashlib.sha256(canonical_bundle_bytes(entries)).hexdigest()
    return f"sha256:{digest}"


def canonical_bundle_bytes(entries: list[dict]) -> bytes:
    lines = []
    for entry in sorted(entries, key=lambda item: item["kind"]):
        lines.append(
            f"{entry['kind']}\t{entry['path']}\t{entry['sha256']}\t{entry['size_bytes']}\n"
        )
    return "".join(lines).encode("utf-8")


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is required when VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1")
    return value


def require_sha256_digest(value: str, description: str) -> str:
    if not value.startswith("sha256:") or len(value) != len("sha256:") + 64:
        raise SystemExit(f"{description} must be a sha256 digest")
    hex_part = value[len("sha256:") :]
    if any(ch not in "0123456789abcdefABCDEF" for ch in hex_part):
        raise SystemExit(f"{description} must be a sha256 digest")
    return f"sha256:{hex_part.lower()}"


def require_base64_bytes(value: str, description: str) -> bytes:
    try:
        return base64.b64decode(value.encode("ascii"), validate=True)
    except Exception as exc:
        raise SystemExit(f"{description} must be base64") from exc


def require_full_git_sha(value: str, description: str) -> str:
    if len(value) != 40 or any(ch not in "0123456789abcdefABCDEF" for ch in value):
        raise SystemExit(f"{description} must be a full 40-character git commit SHA")
    lowered = value.lower()
    if "placeholder" in lowered or lowered in {"unknown", "local"} or "+dirty" in lowered:
        raise SystemExit(f"{description} must be clean and non-placeholder")
    return value


def full_git_sha_or_empty(value: str) -> str:
    value = (value or "").strip()
    if len(value) == 40 and all(ch in "0123456789abcdefABCDEF" for ch in value):
        return value
    return ""


def require_trusted_release_ref(value: str, description: str) -> str:
    if value == "refs/heads/main":
        return value
    if value.startswith("refs/tags/v") and value[len("refs/tags/v") :].strip():
        return value
    raise SystemExit(f"{description} must use refs/heads/main or refs/tags/v* for trusted backend-time provenance")


def require_deployed_image_digest(product: dict, role: str) -> str:
    deployed = product.get("deployed_images")
    if not isinstance(deployed, dict):
        raise SystemExit("trusted backend-time provenance requires product deployed_images")
    role_image = deployed.get(role)
    if not isinstance(role_image, dict):
        raise SystemExit(f"trusted backend-time provenance requires deployed_images.{role}")
    digest = str(role_image.get("image_digest") or "").strip()
    require_sha256_digest(digest, f"deployed_images.{role}.image_digest")
    return digest


def read_sigstore_bundle_from_base64(value: str) -> tuple[dict, bytes]:
    bundle_bytes = require_base64_bytes(value, "VELORIX_CI_SIGSTORE_BUNDLE_BASE64")
    try:
        bundle = json.loads(bundle_bytes.decode("utf-8"))
    except Exception as exc:
        raise SystemExit("VELORIX_CI_SIGSTORE_BUNDLE_BASE64 must decode to Sigstore bundle JSON") from exc
    if not isinstance(bundle, dict):
        raise SystemExit("VELORIX_CI_SIGSTORE_BUNDLE_BASE64 must decode to a Sigstore bundle object")
    return bundle, bundle_bytes


def first_sigstore_certificate_bytes(bundle: dict) -> bytes:
    material = bundle.get("verificationMaterial") or {}
    certificate = material.get("certificate")
    if isinstance(certificate, dict) and isinstance(certificate.get("rawBytes"), str):
        return require_base64_bytes(certificate["rawBytes"], "Sigstore bundle certificate.rawBytes")
    chain = material.get("x509CertificateChain") or {}
    certificates = chain.get("certificates") or []
    if certificates and isinstance(certificates[0], dict) and isinstance(certificates[0].get("rawBytes"), str):
        return require_base64_bytes(certificates[0]["rawBytes"], "Sigstore bundle x509CertificateChain.certificates[0].rawBytes")
    raise SystemExit("Sigstore bundle must contain a Fulcio signing certificate")


def first_sigstore_tlog_entry(bundle: dict) -> dict:
    entries = (bundle.get("verificationMaterial") or {}).get("tlogEntries") or []
    if not entries or not isinstance(entries[0], dict):
        raise SystemExit("Sigstore bundle must contain a Rekor transparency log entry")
    return entries[0]


def sigstore_log_index(entry: dict) -> int:
    value = entry.get("logIndex")
    if value is None:
        value = (entry.get("inclusionProof") or {}).get("logIndex", 0)
    try:
        return int(value)
    except Exception as exc:
        raise SystemExit("Sigstore bundle logIndex must be an integer") from exc


def sigstore_integrated_time(entry: dict) -> int:
    try:
        return int(entry.get("integratedTime", 0))
    except Exception as exc:
        raise SystemExit("Sigstore bundle integratedTime must be an integer") from exc


def sigstore_log_id_sha256(entry: dict) -> str:
    key_id = ((entry.get("logId") or {}).get("keyId") or "").strip()
    if not key_id:
        raise SystemExit("Sigstore bundle logId.keyId is required")
    return f"sha256:{hashlib.sha256(require_base64_bytes(key_id, 'Sigstore bundle logId.keyId')).hexdigest()}"


def sigstore_inclusion_proof_sha256(entry: dict) -> str:
    proof = entry.get("inclusionProof")
    if not isinstance(proof, dict):
        raise SystemExit("Sigstore bundle inclusionProof is required")
    proof_bytes = json.dumps(proof, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"sha256:{hashlib.sha256(proof_bytes).hexdigest()}"


product = load_json(product_path, "product evidence")
assessment = load_json(assessment_path, "Hiqlite backend-time assessment")
readyz = load_json(readyz_path, "readyz evidence")
multi_replica = load_json(multi_replica_path, "multi-replica fencing smoke")
failover = load_json(failover_path, "standing-runtime failover smoke")

require(product.get("evidence_kind") == "velorix_product_slice_evidence", "product evidence has unsupported evidence_kind")
require(pointer(product, "/metadata_store/backend") == "hiqlite", "product evidence must use metadata_store.backend=hiqlite")
require(pointer(product, "/standing_runtime_fencing/configured_mode") == "required", "product evidence must use required standing-runtime fencing")

capability = pointer(product, "/standing_runtime_fencing/capability")
required_capability = {
    "backend_name": "hiqlite",
    "backend_time_source_kind": "raft_replicated_authority_time",
    "lease_authority_kind": "raft_replicated_time",
    "lease_expiry_semantics": "backend_wall_clock_ttl",
    "authoritative_backend_time": True,
    "bounded_wall_clock_failover": True,
    "production_bounded_failover_safe": True,
    "production_multi_writer_safe": True,
    "linearizable_owner_lease": True,
    "latest_read_linearizable": True,
    "durable_monotonic_owner_epoch": True,
    "owner_validated_checkpoint_publish": True,
    "publish_rejects_expired_owner": True,
    "publish_checks_owner_and_latest_atomically": True,
}
for field, expected in required_capability.items():
    require(capability.get(field) == expected, f"standing-runtime capability {field} must be {expected!r}")

failover_bound = capability.get("failover_time_bound_ms")
require(isinstance(failover_bound, int) and failover_bound > 0, "capability.failover_time_bound_ms must be a positive integer")

require(assessment.get("evidence_kind") == "velorix_hiqlite_backend_time_assessment", "assessment has unsupported evidence_kind")
require(assessment.get("required_mode_supported") is True, "assessment must support required mode")
require(
    assessment.get("can_generate_product_complete_backend_time_attestation") is True,
    "assessment must be able to generate backend-time attestation",
)
require(assessment.get("backend_time_source_kind") == "raft_replicated_authority_time", "assessment must use raft_replicated_authority_time")
require(assessment.get("lease_authority_kind") == "raft_replicated_time", "assessment must use raft_replicated_time")
require(assessment.get("lease_expiry_semantics") == "backend_wall_clock_ttl", "assessment must use backend_wall_clock_ttl")
require(assessment.get("missing_capabilities") == [], "assessment still reports missing capabilities")
require(assessment.get("product_capability") == capability, "assessment product_capability must match product evidence capability")

readyz_capability = pointer(readyz, "/metadata_store/standing_runtime_fencing")
for field in required_capability:
    require(readyz_capability.get(field) == capability.get(field), f"readyz capability {field} must match product evidence")

adversarial = pointer(product, "/metadata_store/standing_runtime_adversarial_smoke")
require(adversarial.get("status") == "pass", "metadata adversarial smoke must pass")
adversarial_assertions = adversarial.get("assertions") or {}
for assertion in [
    "logical_owner_expiry_checked",
    "new_owner_epoch_fences_old_owner",
    "stale_owner_checkpoint_publish_rejected",
    "stale_checkpoint_pointer_publish_conflicted",
    "latest_checkpoint_remains_metadata_authoritative",
]:
    require(adversarial_assertions.get(assertion) is True, f"metadata adversarial assertion {assertion} must be true")

require(pointer(product, "/standing_runtime_fencing/multi_replica_fencing_smoke/status") == "pass", "product multi-replica smoke must pass")
require(multi_replica.get("evidence_kind") == "velorix_deployed_multi_replica_fencing_smoke", "multi-replica smoke has unsupported evidence_kind")
require(multi_replica.get("status") == "pass", "multi-replica smoke must pass")
multi_assertions = multi_replica.get("assertions") or {}
for assertion in [
    "distinct_api_pods",
    "non_owner_ingest_rejected",
    "owner_retry_converged",
    "read_replica_served_query",
]:
    require(multi_assertions.get(assertion) is True, f"multi-replica assertion {assertion} must be true")

local_failover_summary = pointer(product, "/standing_runtime_fencing/local_api_pod_failover_smoke")
require(local_failover_summary.get("status") == "pass", "product local API pod failover smoke must pass")
require(failover.get("evidence_kind") == "velorix_standing_runtime_failover_smoke", "failover smoke has unsupported evidence_kind")
require(failover.get("status") == "pass", "failover smoke must pass")
release_failover_shape = (
    failover.get("trusted_for_product_complete") is True
    or failover.get("production_wall_clock_failover_attestation") is True
    or failover.get("evidence_scope") == "release_ci_deployed_product"
    or failover.get("failover_probe_kind") == "release_bounded_wall_clock_failover"
)
if release_failover_shape:
    require(failover.get("trusted_for_product_complete") is True, "release-shaped backend-time failover evidence requires trusted_for_product_complete=True")
    require(failover.get("production_wall_clock_failover_attestation") is True, "release-shaped backend-time failover evidence requires release wall-clock failover attestation")
    require(failover.get("evidence_scope") == "release_ci_deployed_product", "release-shaped backend-time failover evidence requires release_ci_deployed_product scope")
    require(failover.get("failover_probe_kind") == "release_bounded_wall_clock_failover", "release-shaped backend-time failover evidence requires release_bounded_wall_clock_failover")
    require(failover.get("backend_time_source_kind") == "raft_replicated_authority_time", "release-shaped backend-time failover evidence requires raft_replicated_authority_time")
    require(failover.get("authority_time_observed") is True, "release-shaped backend-time failover evidence requires authority_time_observed")
    require(failover.get("owner_ttl_ms") == failover_bound, "release-shaped backend-time failover evidence requires owner_ttl_ms to match capability bound")
    require(failover.get("failover_time_bound_ms") == failover_bound, "release-shaped backend-time failover evidence requires failover_time_bound_ms to match capability bound")
    require(isinstance(failover.get("pre_failover_owner_epoch"), int), "release-shaped backend-time failover evidence requires pre_failover_owner_epoch")
    require(isinstance(failover.get("post_failover_owner_epoch"), int), "release-shaped backend-time failover evidence requires post_failover_owner_epoch")
    require(failover.get("post_failover_owner_epoch") > failover.get("pre_failover_owner_epoch"), "release-shaped backend-time failover evidence requires owner epoch to advance")
    affected_api_pods = failover.get("affected_api_pods")
    require(isinstance(affected_api_pods, list) and affected_api_pods and all(isinstance(pod, str) and pod.strip() for pod in affected_api_pods), "release-shaped backend-time failover evidence requires affected_api_pods")
    for field in [
        "trusted_for_product_complete",
        "production_wall_clock_failover_attestation",
        "evidence_scope",
        "failover_probe_kind",
        "backend_time_source_kind",
        "authority_time_observed",
        "owner_ttl_ms",
        "failover_time_bound_ms",
        "pre_failover_owner_epoch",
        "post_failover_owner_epoch",
        "affected_api_pods",
    ]:
        require(
            local_failover_summary.get(field) == failover.get(field),
            f"product failover summary {field} must match release-shaped failover evidence",
        )
else:
    require(local_failover_summary.get("trusted_for_product_complete") is False, "local failover smoke must remain explicitly non-product-complete")
    require(local_failover_summary.get("production_wall_clock_failover_attestation") is False, "local failover smoke must not claim product-complete wall-clock attestation")
    require(failover.get("trusted_for_product_complete") is False, "failover smoke must remain explicitly non-product-complete")
    require(failover.get("production_wall_clock_failover_attestation") is False, "failover smoke must not claim product-complete wall-clock attestation")
if trusted_provenance_enabled:
    require(release_failover_shape, "trusted backend-time provenance requires release-shaped failover evidence")
observed_failover = failover.get("observed_failover_ms")
require(isinstance(observed_failover, int) and observed_failover > 0, "failover smoke must record observed_failover_ms")
require(observed_failover <= failover_bound, "observed_failover_ms exceeds advertised failover_time_bound_ms")

require(meta_smoke_log_path.is_file(), f"missing metadata adversarial smoke log: {meta_smoke_log_path}")
meta_smoke_log = meta_smoke_log_path.read_text(encoding="utf-8", errors="replace")
for required_fragment in [
    "standing runtime adversarial smoke ok",
    "owner_a_epoch=",
    "owner_b_epoch=",
    "latest_epoch=",
    "backend_time_source_kind=raft_replicated_authority_time",
]:
    require(required_fragment in meta_smoke_log, f"metadata smoke log missing {required_fragment!r}")

core = {
    "schema_version": 1,
    "evidence_kind": "velorix_hiqlite_backend_time_attestation",
    "attestation_origin": "trusted_release_ci" if trusted_provenance_enabled else "diagnostic_deployed_product",
    "source_kind": "trusted_ci_provenance" if trusted_provenance_enabled else "local_diagnostic",
    "failover_evidence_shape": "release_scoped" if release_failover_shape else "local_diagnostic",
    "diagnostic_release_failover_included": release_failover_shape and not trusted_provenance_enabled,
    "backend_name": "hiqlite",
    "time_source_kind": "raft_replicated_authority_time",
    "lease_authority_kind": "raft_replicated_time",
    "lease_expiry_semantics": "backend_wall_clock_ttl",
    "authoritative_backend_time": True,
    "bounded_wall_clock_failover": True,
    "production_bounded_failover_safe": True,
    "authority_sampled_unix_time_ms_in_raft_operation": True,
    "owner_expiry_bound_to_authority_time": True,
    "checkpoint_publish_rejects_expired_owner_with_authority_time": True,
    "bounded_failover_probe_passed": True,
    "failover_time_bound_ms": failover_bound,
    "observed_max_failover_ms": observed_failover,
    "metrics_time_source_rejected": True,
    "raft_log_index_time_source_rejected": True,
    "distributed_lock_ttl_source_rejected": True,
    "attested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "attester": attester,
}

evidence_files = [
    evidence_file(product_path, "product_evidence"),
    evidence_file(assessment_path, "hiqlite_backend_time_assessment"),
    evidence_file(readyz_path, "readyz"),
    evidence_file(multi_replica_path, "multi_replica_fencing_smoke"),
    evidence_file(failover_path, "standing_runtime_failover_smoke"),
    evidence_file(meta_smoke_log_path, "metadata_adversarial_smoke_log"),
]
canonical_bundle_digest = canonical_bundle_sha256(evidence_files)
canonical_bundle_file = os.environ.get("VELORIX_HIQLITE_BACKEND_TIME_CANONICAL_BUNDLE_FILE", "").strip()
if canonical_bundle_file:
    canonical_bundle_path = Path(canonical_bundle_file)
    canonical_bundle_path.parent.mkdir(parents=True, exist_ok=True)
    canonical_bundle_path.write_bytes(canonical_bundle_bytes(evidence_files))

trusted_provenance = None
trusted_for_release_validator = False
trusted_for_product_complete = False
release_validator_fail_closed = True
if trusted_provenance_enabled:
    if attester not in {"velorix-release-operator", "velorix-ci"}:
        raise SystemExit("trusted backend-time provenance requires attester velorix-release-operator or velorix-ci")
    authority = product.get("metadata_store", {}).get("hiqlite_authority_attestation", {})
    source_repository = require_env("VELORIX_SOURCE_REPOSITORY")
    if source_repository != "github.com/mrchypark/velorix":
        raise SystemExit("VELORIX_SOURCE_REPOSITORY must be github.com/mrchypark/velorix")
    authority_source_revision = str(authority.get("source_revision") or "").strip()
    source_revision = require_env("VELORIX_SOURCE_REVISION")
    if not source_revision or source_revision.lower() in {"unknown", "local"} or "placeholder" in source_revision.lower():
        raise SystemExit("trusted backend-time provenance requires a non-placeholder source revision")
    source_revision = require_full_git_sha(source_revision, "VELORIX_SOURCE_REVISION")
    authority_source_revision_sha = full_git_sha_or_empty(authority_source_revision)
    if authority_source_revision_sha and authority_source_revision_sha == source_revision:
        raise SystemExit("VELORIX_SOURCE_REVISION must be the Velorix release commit, not metadata_store.hiqlite_authority_attestation.source_revision")
    release_commit = require_full_git_sha(require_env("VELORIX_RELEASE_COMMIT"), "VELORIX_RELEASE_COMMIT")
    if authority_source_revision_sha and authority_source_revision_sha == release_commit:
        raise SystemExit("VELORIX_RELEASE_COMMIT must be the Velorix release commit, not metadata_store.hiqlite_authority_attestation.source_revision")
    if source_revision != release_commit:
        raise SystemExit("trusted backend-time provenance source revision must match VELORIX_RELEASE_COMMIT")
    api_image_digest = require_env("VELORIX_API_IMAGE_DIGEST")
    require_sha256_digest(api_image_digest, "VELORIX_API_IMAGE_DIGEST")
    if api_image_digest != require_deployed_image_digest(product, "velorix-api"):
        raise SystemExit("VELORIX_API_IMAGE_DIGEST must match product deployed_images.velorix-api.image_digest")
    meta_image_digest = require_env("VELORIX_META_IMAGE_DIGEST")
    require_sha256_digest(meta_image_digest, "VELORIX_META_IMAGE_DIGEST")
    if meta_image_digest != require_deployed_image_digest(product, "velorix-meta"):
        raise SystemExit("VELORIX_META_IMAGE_DIGEST must match product deployed_images.velorix-meta.image_digest")
    subject_image_digest = os.environ.get("VELORIX_HIQLITE_IMAGE_DIGEST", "").strip() or os.environ.get("VELORIX_SUBJECT_IMAGE_DIGEST", "").strip() or str(
        authority.get("image_digest") or ""
    ).strip()
    require_sha256_digest(subject_image_digest, "VELORIX_HIQLITE_IMAGE_DIGEST")
    subject_images = [
        {"role": "velorix-api", "image_digest": api_image_digest},
        {"role": "velorix-meta", "image_digest": meta_image_digest},
        {"role": "hiqlite-authority", "image_digest": subject_image_digest},
    ]
    workflow_ref = require_env("VELORIX_CI_WORKFLOW_REF")
    job_workflow_ref = require_env("VELORIX_CI_JOB_WORKFLOW_REF")
    oidc_subject = require_env("VELORIX_CI_OIDC_SUBJECT")
    workflow_ref_suffix = workflow_ref.split("@", 1)[1] if "@" in workflow_ref else ""
    workflow_release_ref = require_trusted_release_ref(workflow_ref_suffix, "VELORIX_CI_WORKFLOW_REF")
    if oidc_subject != f"repo:mrchypark/velorix:ref:{workflow_release_ref}":
        raise SystemExit("VELORIX_CI_OIDC_SUBJECT must match trusted release workflow ref")
    sigstore_bundle_base64 = os.environ.get("VELORIX_CI_SIGSTORE_BUNDLE_BASE64", "").strip()
    sigstore_certificate_identity = os.environ.get("VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY", "").strip()
    if sigstore_bundle_base64 and not sigstore_certificate_identity:
        sigstore_certificate_identity = f"https://github.com/{os.environ.get('GITHUB_REPOSITORY', 'mrchypark/velorix')}/.github/workflows/release-gate.yml@{os.environ.get('GITHUB_REF', '')}"
    certificate_identity = sigstore_certificate_identity or oidc_subject
    if sigstore_bundle_base64:
        certificate_identity_suffix = certificate_identity.split("@", 1)[1] if "@" in certificate_identity else ""
        if require_trusted_release_ref(certificate_identity_suffix, "VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY") != workflow_release_ref:
            raise SystemExit("VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY must match trusted release workflow ref")
    signature_bundle = {
        "bundle_kind": "sigstore_rekor_dsse",
        "oidc_issuer": "https://token.actions.githubusercontent.com",
        "certificate_identity": certificate_identity,
        "signed_payload_sha256": canonical_bundle_digest,
    }
    if sigstore_bundle_base64:
        sigstore_bundle, sigstore_bundle_bytes = read_sigstore_bundle_from_base64(sigstore_bundle_base64)
        sigstore_bundle_sha256 = require_env("VELORIX_CI_SIGSTORE_BUNDLE_SHA256")
        sigstore_bundle_sha256 = require_sha256_digest(sigstore_bundle_sha256, "VELORIX_CI_SIGSTORE_BUNDLE_SHA256")
        actual_sigstore_bundle_sha256 = f"sha256:{hashlib.sha256(sigstore_bundle_bytes).hexdigest()}"
        if actual_sigstore_bundle_sha256 != sigstore_bundle_sha256:
            raise SystemExit("VELORIX_CI_SIGSTORE_BUNDLE_SHA256 must match VELORIX_CI_SIGSTORE_BUNDLE_BASE64")
        cert_bytes = first_sigstore_certificate_bytes(sigstore_bundle)
        tlog_entry = first_sigstore_tlog_entry(sigstore_bundle)
        signing_certificate_sha256 = os.environ.get("VELORIX_CI_SIGNING_CERTIFICATE_SHA256", "").strip() or f"sha256:{hashlib.sha256(cert_bytes).hexdigest()}"
        transparency_log_id = os.environ.get("VELORIX_CI_TRANSPARENCY_LOG_ID", "").strip() or sigstore_log_id_sha256(tlog_entry)
        inclusion_proof_sha256 = os.environ.get("VELORIX_CI_INCLUSION_PROOF_SHA256", "").strip() or sigstore_inclusion_proof_sha256(tlog_entry)
        transparency_log_index = int(os.environ.get("VELORIX_CI_TRANSPARENCY_LOG_INDEX", "").strip() or sigstore_log_index(tlog_entry))
        integrated_time_unix = int(os.environ.get("VELORIX_CI_INTEGRATED_TIME_UNIX", "").strip() or sigstore_integrated_time(tlog_entry))
        signature_bundle.update(
            {
                "signing_certificate_sha256": signing_certificate_sha256,
                "sigstore_bundle_base64": sigstore_bundle_base64,
                "sigstore_bundle_sha256": sigstore_bundle_sha256,
                "transparency_log_id": transparency_log_id,
                "transparency_log_index": transparency_log_index,
                "integrated_time_unix": integrated_time_unix,
                "inclusion_proof_sha256": inclusion_proof_sha256,
            }
        )
    else:
        signing_certificate_sha256 = require_env("VELORIX_CI_SIGNING_CERTIFICATE_SHA256")
        signature_algorithm = os.environ.get("VELORIX_CI_SIGNATURE_ALGORITHM", "ed25519").strip()
        if signature_algorithm != "ed25519":
            raise SystemExit("VELORIX_CI_SIGNATURE_ALGORITHM must be ed25519")
        public_key_base64 = require_env("VELORIX_CI_PUBLIC_KEY_BASE64")
        public_key_sha256 = require_env("VELORIX_CI_PUBLIC_KEY_SHA256")
        require_sha256_digest(public_key_sha256, "VELORIX_CI_PUBLIC_KEY_SHA256")
        signature_base64 = require_env("VELORIX_CI_SIGNATURE_BASE64")
        transparency_log_id = require_env("VELORIX_CI_TRANSPARENCY_LOG_ID")
        inclusion_proof_sha256 = require_env("VELORIX_CI_INCLUSION_PROOF_SHA256")
        transparency_log_index = int(require_env("VELORIX_CI_TRANSPARENCY_LOG_INDEX"))
        integrated_time_unix = int(os.environ.get("VELORIX_CI_INTEGRATED_TIME_UNIX", "0") or "0")
        signature_bundle.update(
            {
                "signing_certificate_sha256": signing_certificate_sha256,
                "signature_algorithm": signature_algorithm,
                "public_key_base64": public_key_base64,
                "public_key_sha256": public_key_sha256,
                "signature_base64": signature_base64,
                "transparency_log_id": transparency_log_id,
                "transparency_log_index": transparency_log_index,
                "integrated_time_unix": integrated_time_unix,
                "inclusion_proof_sha256": inclusion_proof_sha256,
            }
        )
    require_sha256_digest(signature_bundle["signing_certificate_sha256"], "VELORIX_CI_SIGNING_CERTIFICATE_SHA256")
    require_sha256_digest(signature_bundle["transparency_log_id"], "VELORIX_CI_TRANSPARENCY_LOG_ID")
    require_sha256_digest(signature_bundle["inclusion_proof_sha256"], "VELORIX_CI_INCLUSION_PROOF_SHA256")
    if signature_bundle["transparency_log_index"] < 0:
        raise SystemExit("VELORIX_CI_TRANSPARENCY_LOG_INDEX must be non-negative")
    if signature_bundle["integrated_time_unix"] <= 0:
        raise SystemExit("VELORIX_CI_INTEGRATED_TIME_UNIX must be nonzero")
    trusted_provenance = {
        "schema_version": 1,
        "provenance_kind": "velorix_ci_evidence_bundle_provenance",
        "source_repository": source_repository,
        "source_revision": source_revision,
        "workflow_name": require_env("VELORIX_CI_WORKFLOW_NAME"),
        "workflow_run_id": require_env("VELORIX_CI_WORKFLOW_RUN_ID"),
        "job_name": require_env("VELORIX_CI_JOB_NAME"),
        "subject_image_digest": subject_image_digest,
        "subject_images": subject_images,
        "ci_identity": {
            "identity_kind": "github_actions_oidc",
            "issuer": "https://token.actions.githubusercontent.com",
            "audience": "sigstore",
            "repository": "mrchypark/velorix",
            "subject": oidc_subject,
            "workflow_ref": workflow_ref,
            "workflow_sha": release_commit,
            "job_workflow_ref": job_workflow_ref,
            "run_id": os.environ.get("GITHUB_RUN_ID", os.environ.get("VELORIX_CI_WORKFLOW_RUN_ID", "")).strip(),
            "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "1").strip(),
        },
        "signature_bundle": signature_bundle,
        "generated_at": core["attested_at"],
        "attester": attester,
        "canonical_bundle_sha256": canonical_bundle_digest,
        "canonical_bundle_entries": evidence_files,
    }
    trusted_for_release_validator = True
    trusted_for_product_complete = True
    release_validator_fail_closed = False

attestation = {
    **core,
    "trusted_for_product_complete": trusted_for_product_complete,
    "trusted_for_release_validator": trusted_for_release_validator,
    "release_validator_fail_closed": release_validator_fail_closed,
    "source": "deployed_vind_product_smoke_diagnostic",
    "product_evidence_file": product_path.name,
    "product_generated_at": product.get("generated_at"),
    "product_complete_at_generation_time": product.get("product_complete"),
    "local_failover_scope": failover.get("scope"),
    "local_failover_trusted_for_product_complete": failover.get("trusted_for_product_complete"),
    "trusted_provenance": trusted_provenance,
    "evidence_files": evidence_files,
    "validated_smoke_assertions": {
        "metadata_adversarial_smoke": adversarial_assertions,
        "multi_replica_fencing_smoke": multi_assertions,
        "local_api_pod_failover_smoke": {
            "observed_failover_ms": observed_failover,
            "post_failover_owners_match_local_process": failover.get("post_failover_owners_match_local_process"),
            "post_failover_ingest_outcome": failover.get("post_failover_ingest_outcome"),
        },
    },
}

output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(json.dumps(attestation, indent=2, sort_keys=True) + "\n", encoding="utf-8")

if update_product_evidence:
    summary = {
        "validated": True,
        "evidence": "hiqlite-backend-time-attestation.json",
        **core,
        "trusted_for_product_complete": trusted_for_product_complete,
        "trusted_for_release_validator": trusted_for_release_validator,
        "release_validator_fail_closed": release_validator_fail_closed,
    }
    metadata_store = product.setdefault("metadata_store", {})
    metadata_store["hiqlite_backend_time_attestation"] = summary
    product_path.write_text(json.dumps(product, indent=2, sort_keys=True) + "\n", encoding="utf-8")

print(str(output_path))
PY
