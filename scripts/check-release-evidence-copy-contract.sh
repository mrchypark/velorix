#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
scratch_dir="${VELORIX_CONTRACT_SCRATCH_DIR:-${repo_root}/target/velorix-contract/check-release-evidence-copy}"
scratch_dir="$(
  python3 - "$repo_root" "$scratch_dir" <<'PY'
import sys
from pathlib import Path

repo_root = Path(sys.argv[1]).resolve()
target_root = repo_root / "target"
scratch = Path(sys.argv[2])
if not scratch.is_absolute():
    scratch = repo_root / scratch
scratch = scratch.resolve(strict=False)
required_root = (target_root / "velorix-contract").resolve(strict=False)
try:
    scratch.relative_to(required_root)
except ValueError:
    raise SystemExit(
        "VELORIX_CONTRACT_SCRATCH_DIR must resolve under "
        f"{required_root}; got {scratch}"
    )
if scratch == required_root:
    raise SystemExit(
        "VELORIX_CONTRACT_SCRATCH_DIR must name a child directory under "
        f"{required_root}"
    )
print(scratch)
PY
)"
rm -rf "$scratch_dir"
mkdir -p "$scratch_dir"

python3 - "$repo_root" "$scratch_dir" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
scratch_dir = Path(sys.argv[2])
workflow_path = repo_root / ".github" / "workflows" / "release-gate.yml"
src = scratch_dir / "src"
out = scratch_dir / "out"
src.mkdir()

lifecycle_files = {
    "pod_internal_job": "velorix-ingest-writer-smoke-log.json",
    "overlap_job": "velorix-ingest-lifecycle-overlap-log.json",
    "adjacent_job": "velorix-ingest-lifecycle-adjacent-log.json",
    "restart_job": "velorix-ingest-lifecycle-restart-log.json",
    "lease_loss_job": "velorix-ingest-lifecycle-lease-loss-log.json",
    "handoff_probe_job": "velorix-ingest-lifecycle-handoff-log.json",
}
query_policy_files = {
    "created": "query-policy-interactive.json",
    "read_back": "query-policy-interactive-read.json",
    "weak_policy_rejection": "query-policy-weak-rejection.json",
    "missing_policy_rejection": "query-policy-missing-view.json",
}

for filename in {
    "no-pvc-namespace.json",
    "hiqlite-authority-attestation.json",
    "hiqlite-backend-time-attestation.json",
    "no-pvc-hiqlite-statefulset.json",
    "velorix-hiqlite.yaml",
    "ingress-tls-auth-attestation.json",
    "openapi.json",
    "tls-auth-smoke.json",
    "external-s3-validate-job.json",
    "external-s3-validate.log",
    "ingest-writer-job-log.json",
    "ingest-writer-job.json",
    "ingest-writer-pods.json",
    "velorix-api.yaml",
    "velorix-api-deployment-observed.json",
    "velorix-api-pods.json",
    "velorix-meta.yaml",
    "velorix-meta-deployment-observed.json",
    "velorix-meta-pods.json",
    *lifecycle_files.values(),
    *query_policy_files.values(),
}:
    (src / filename).write_text("{}\n", encoding="utf-8")

lifecycle = {
    "schema_version": 1,
    "evidence_kind": "velorix_ingest_writer_lifecycle_attestation",
    "evidence_files": lifecycle_files,
}
product = {
    "schema_version": 1,
    "evidence_kind": "velorix_product_slice_evidence",
    "deployed_images": {
        "velorix-api": {
            "evidence_files": {
                "manifest": "velorix-api.yaml",
                "deployment": "velorix-api-deployment-observed.json",
                "pods": "velorix-api-pods.json",
            }
        },
        "velorix-meta": {
            "evidence_files": {
                "manifest": "velorix-meta.yaml",
                "deployment": "velorix-meta-deployment-observed.json",
                "pods": "velorix-meta-pods.json",
            }
        },
    },
    "no_pvc": {"evidence": "no-pvc-namespace.json"},
    "metadata_store": {
        "hiqlite_authority_attestation": {
            "evidence": "hiqlite-authority-attestation.json",
            "authority_kind": "velorix_managed_hiqlite",
            "no_pvc_evidence_files": {
                "namespace_pvc_list": "no-pvc-namespace.json",
                "hiqlite_statefulset": "no-pvc-hiqlite-statefulset.json",
                "manifest": "velorix-hiqlite.yaml",
            },
        },
        "hiqlite_backend_time_attestation": {
            "evidence": "hiqlite-backend-time-attestation.json",
        },
    },
    "object_store": {
        "external_s3_validation_evidence": {
            "job": "external-s3-validate-job.json",
            "log": "external-s3-validate.log",
        }
    },
    "api": {
        "openapi": {"evidence_file": "openapi.json"},
        "auth": {
            "local_tls_auth_smoke": {"evidence": "tls-auth-smoke.json"},
            "ingress_tls_auth_attestation": {
                "evidence": "ingress-tls-auth-attestation.json",
            },
        },
        "query_policy": {"evidence_files": query_policy_files},
    },
    "ingest_writer": {
        "evidence_files": {
            "job_log": "ingest-writer-job-log.json",
            "job": "ingest-writer-job.json",
            "pods": "ingest-writer-pods.json",
        },
        "lifecycle_attestation": {"evidence_files": lifecycle_files},
    },
}
(src / "lifecycle.json").write_text(json.dumps(lifecycle), encoding="utf-8")
(src / "product.json").write_text(json.dumps(product), encoding="utf-8")
(src / "rustfs-s3-gate-evidence.json").write_text("{}\n", encoding="utf-8")
(src / "rustfs-production-gc-seed.json").write_text("{}\n", encoding="utf-8")
(src / "rustfs-production-gc-run.json").write_text("{}\n", encoding="utf-8")
(src / "rustfs-production-gc.json").write_text("{}\n", encoding="utf-8")
(src / "rustfs-production-gc-validation.json").write_text(
    json.dumps(
        {
            "schema_version": 1,
            "status": "pass",
            "evidence_kind": "rustfs_production_gc_evidence_family_validated",
            "gate_evidence_path": "target/velorix-s3/rustfs-s3-gate-evidence.json",
            "seed_evidence_path": "target/release-evidence/rustfs-production-gc-seed.json",
            "execute_evidence_path": "target/release-evidence/rustfs-production-gc-run.json",
            "production_evidence_path": "target/release-evidence/rustfs-production-gc.json",
            "checks": [
                "rustfs_s3_compatible_gate_present",
                "seed_fixture_created_retired_checkpoint_state",
                "s3_gc_execute_deleted_seeded_candidate",
                "production_gc_evidence_verified_listing_retention_and_transition",
                "artifact_family_paths_and_identity_bound",
            ],
        }
    ),
    encoding="utf-8",
)

helper = repo_root / "scripts" / "copy-readiness-sibling-evidence.py"


def run_helper_expect_fail(args, expected_message: str) -> None:
    result = subprocess.run(
        [str(helper), *args],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode == 0:
        raise SystemExit(
            "release evidence copy contract unexpectedly succeeded for:\n"
            + " ".join(args)
        )
    output = result.stdout + result.stderr
    if expected_message not in output:
        raise SystemExit(
            "release evidence copy contract failed with unexpected message:\n"
            + output
        )


copied_lifecycle = subprocess.check_output(
    [
        str(helper),
        "--kind",
        "ingest-writer-lifecycle",
        "--artifact",
        str(src / "lifecycle.json"),
        "--out-dir",
        str(out / "lifecycle"),
        "--artifact-name",
        "ingest-writer-lifecycle-evidence.json",
    ],
    text=True,
).strip()
copied_product = subprocess.check_output(
    [
        str(helper),
        "--kind",
        "product",
        "--artifact",
        str(src / "product.json"),
        "--out-dir",
        str(out / "product"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    text=True,
).strip()
copied_rustfs_gc = subprocess.check_output(
    [
        str(helper),
        "--kind",
        "rustfs-production-gc",
        "--artifact",
        str(src / "rustfs-production-gc-validation.json"),
        "--out-dir",
        str(out / "rustfs-production-gc"),
        "--artifact-name",
        "rustfs-production-gc-validation.json",
    ],
    text=True,
).strip()

required = [
    Path(copied_lifecycle),
    Path(copied_product),
    Path(copied_rustfs_gc),
    out / "rustfs-production-gc" / "rustfs-s3-gate-evidence.json",
    out / "rustfs-production-gc" / "rustfs-production-gc-seed.json",
    out / "rustfs-production-gc" / "rustfs-production-gc-run.json",
    out / "rustfs-production-gc" / "rustfs-production-gc.json",
    *(out / "lifecycle" / filename for filename in lifecycle_files.values()),
    *(out / "product" / filename for filename in lifecycle_files.values()),
    *(out / "product" / filename for filename in query_policy_files.values()),
    out / "product" / "no-pvc-namespace.json",
    out / "product" / "velorix-api.yaml",
    out / "product" / "velorix-api-deployment-observed.json",
    out / "product" / "velorix-api-pods.json",
    out / "product" / "velorix-meta.yaml",
    out / "product" / "velorix-meta-deployment-observed.json",
    out / "product" / "velorix-meta-pods.json",
    out / "product" / "hiqlite-authority-attestation.json",
    out / "product" / "hiqlite-backend-time-attestation.json",
    out / "product" / "no-pvc-hiqlite-statefulset.json",
    out / "product" / "velorix-hiqlite.yaml",
    out / "product" / "ingress-tls-auth-attestation.json",
    out / "product" / "openapi.json",
    out / "product" / "tls-auth-smoke.json",
    out / "product" / "external-s3-validate-job.json",
    out / "product" / "external-s3-validate.log",
    out / "product" / "ingest-writer-job-log.json",
    out / "product" / "ingest-writer-job.json",
    out / "product" / "ingest-writer-pods.json",
]
missing = [str(path) for path in required if not path.is_file()]
if missing:
    raise SystemExit("release evidence copy contract missing files:\n- " + "\n- ".join(missing))

missing_src = scratch_dir / "missing-src"
missing_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (missing_src / path.name).write_bytes(path.read_bytes())
(missing_src / "query-policy-weak-rejection.json").unlink()
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(missing_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "missing-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "query-policy-weak-rejection.json",
)

missing_no_pvc_src = scratch_dir / "missing-no-pvc-src"
missing_no_pvc_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (missing_no_pvc_src / path.name).write_bytes(path.read_bytes())
(missing_no_pvc_src / "no-pvc-namespace.json").unlink()
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(missing_no_pvc_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "missing-no-pvc-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "no-pvc-namespace.json",
)

missing_hiqlite_src = scratch_dir / "missing-hiqlite-src"
missing_hiqlite_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (missing_hiqlite_src / path.name).write_bytes(path.read_bytes())
(missing_hiqlite_src / "hiqlite-authority-attestation.json").unlink()
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(missing_hiqlite_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "missing-hiqlite-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "hiqlite-authority-attestation.json",
)

missing_hiqlite_backend_time_src = scratch_dir / "missing-hiqlite-backend-time-src"
missing_hiqlite_backend_time_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (missing_hiqlite_backend_time_src / path.name).write_bytes(path.read_bytes())
(missing_hiqlite_backend_time_src / "hiqlite-backend-time-attestation.json").unlink()
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(missing_hiqlite_backend_time_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "missing-hiqlite-backend-time-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "hiqlite-backend-time-attestation.json",
)

missing_ingress_src = scratch_dir / "missing-ingress-src"
missing_ingress_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (missing_ingress_src / path.name).write_bytes(path.read_bytes())
(missing_ingress_src / "ingress-tls-auth-attestation.json").unlink()
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(missing_ingress_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "missing-ingress-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "ingress-tls-auth-attestation.json",
)

missing_lifecycle_src = scratch_dir / "missing-lifecycle-src"
missing_lifecycle_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (missing_lifecycle_src / path.name).write_bytes(path.read_bytes())
(missing_lifecycle_src / "velorix-ingest-lifecycle-handoff-log.json").unlink()
run_helper_expect_fail(
    [
        "--kind",
        "ingest-writer-lifecycle",
        "--artifact",
        str(missing_lifecycle_src / "lifecycle.json"),
        "--out-dir",
        str(scratch_dir / "missing-lifecycle-out"),
        "--artifact-name",
        "ingest-writer-lifecycle-evidence.json",
    ],
    "velorix-ingest-lifecycle-handoff-log.json",
)

missing_product_ingest_src = scratch_dir / "missing-product-ingest-src"
missing_product_ingest_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (missing_product_ingest_src / path.name).write_bytes(path.read_bytes())
(missing_product_ingest_src / "ingest-writer-job-log.json").unlink()
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(missing_product_ingest_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "missing-product-ingest-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "ingest-writer-job-log.json",
)

missing_rustfs_gc_src = scratch_dir / "missing-rustfs-gc-src"
missing_rustfs_gc_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (missing_rustfs_gc_src / path.name).write_bytes(path.read_bytes())
(missing_rustfs_gc_src / "rustfs-production-gc-run.json").unlink()
run_helper_expect_fail(
    [
        "--kind",
        "rustfs-production-gc",
        "--artifact",
        str(missing_rustfs_gc_src / "rustfs-production-gc-validation.json"),
        "--out-dir",
        str(scratch_dir / "missing-rustfs-gc-out"),
        "--artifact-name",
        "rustfs-production-gc-validation.json",
    ],
    "rustfs-production-gc-run.json",
)

invalid_src = scratch_dir / "invalid-src"
invalid_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (invalid_src / path.name).write_bytes(path.read_bytes())
invalid_product = json.loads((invalid_src / "product.json").read_text(encoding="utf-8"))
invalid_product["api"]["query_policy"]["evidence_files"]["created"] = "../openapi.json"
(invalid_src / "product.json").write_text(json.dumps(invalid_product), encoding="utf-8")
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(invalid_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "invalid-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "product query-policy evidence_files.created must be query-policy-interactive.json",
)

invalid_no_pvc_src = scratch_dir / "invalid-no-pvc-src"
invalid_no_pvc_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (invalid_no_pvc_src / path.name).write_bytes(path.read_bytes())
invalid_product = json.loads((invalid_no_pvc_src / "product.json").read_text(encoding="utf-8"))
invalid_product["no_pvc"]["evidence"] = "../no-pvc.json"
(invalid_no_pvc_src / "product.json").write_text(json.dumps(invalid_product), encoding="utf-8")
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(invalid_no_pvc_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "invalid-no-pvc-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "product no_pvc.evidence must be no-pvc-namespace.json",
)

invalid_openapi_src = scratch_dir / "invalid-openapi-src"
invalid_openapi_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (invalid_openapi_src / path.name).write_bytes(path.read_bytes())
invalid_product = json.loads((invalid_openapi_src / "product.json").read_text(encoding="utf-8"))
invalid_product["api"]["openapi"]["evidence_file"] = "../openapi.json"
(invalid_openapi_src / "product.json").write_text(json.dumps(invalid_product), encoding="utf-8")
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(invalid_openapi_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "invalid-openapi-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "product openapi.evidence_file must be openapi.json",
)

invalid_local_tls_src = scratch_dir / "invalid-local-tls-src"
invalid_local_tls_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (invalid_local_tls_src / path.name).write_bytes(path.read_bytes())
invalid_product = json.loads((invalid_local_tls_src / "product.json").read_text(encoding="utf-8"))
invalid_product["api"]["auth"]["local_tls_auth_smoke"]["evidence"] = "../tls.json"
(invalid_local_tls_src / "product.json").write_text(json.dumps(invalid_product), encoding="utf-8")
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(invalid_local_tls_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "invalid-local-tls-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "product local_tls_auth_smoke.evidence must be tls-auth-smoke.json",
)

invalid_hiqlite_src = scratch_dir / "invalid-hiqlite-src"
invalid_hiqlite_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (invalid_hiqlite_src / path.name).write_bytes(path.read_bytes())
invalid_product = json.loads((invalid_hiqlite_src / "product.json").read_text(encoding="utf-8"))
invalid_product["metadata_store"]["hiqlite_authority_attestation"]["evidence"] = "../hiqlite.json"
(invalid_hiqlite_src / "product.json").write_text(json.dumps(invalid_product), encoding="utf-8")
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(invalid_hiqlite_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "invalid-hiqlite-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "product hiqlite_authority_attestation.evidence must be hiqlite-authority-attestation.json",
)

invalid_hiqlite_no_pvc_src = scratch_dir / "invalid-hiqlite-no-pvc-src"
invalid_hiqlite_no_pvc_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (invalid_hiqlite_no_pvc_src / path.name).write_bytes(path.read_bytes())
invalid_product = json.loads(
    (invalid_hiqlite_no_pvc_src / "product.json").read_text(encoding="utf-8")
)
invalid_product["metadata_store"]["hiqlite_authority_attestation"][
    "no_pvc_evidence_files"
]["namespace_pvc_list"] = "../pvc.json"
(invalid_hiqlite_no_pvc_src / "product.json").write_text(
    json.dumps(invalid_product), encoding="utf-8"
)
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(invalid_hiqlite_no_pvc_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "invalid-hiqlite-no-pvc-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "product hiqlite_authority_attestation.no_pvc_evidence_files.namespace_pvc_list must be no-pvc-namespace.json",
)

invalid_hiqlite_backend_time_src = scratch_dir / "invalid-hiqlite-backend-time-src"
invalid_hiqlite_backend_time_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (invalid_hiqlite_backend_time_src / path.name).write_bytes(path.read_bytes())
invalid_product = json.loads(
    (invalid_hiqlite_backend_time_src / "product.json").read_text(encoding="utf-8")
)
invalid_product["metadata_store"]["hiqlite_backend_time_attestation"][
    "evidence"
] = "../backend-time.json"
(invalid_hiqlite_backend_time_src / "product.json").write_text(
    json.dumps(invalid_product), encoding="utf-8"
)
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(invalid_hiqlite_backend_time_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "invalid-hiqlite-backend-time-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "product hiqlite_backend_time_attestation.evidence must be hiqlite-backend-time-attestation.json",
)

invalid_ingress_src = scratch_dir / "invalid-ingress-src"
invalid_ingress_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (invalid_ingress_src / path.name).write_bytes(path.read_bytes())
invalid_product = json.loads((invalid_ingress_src / "product.json").read_text(encoding="utf-8"))
invalid_product["api"]["auth"]["ingress_tls_auth_attestation"]["evidence"] = "../ingress.json"
(invalid_ingress_src / "product.json").write_text(json.dumps(invalid_product), encoding="utf-8")
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(invalid_ingress_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "invalid-ingress-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "product ingress_tls_auth_attestation.evidence must be ingress-tls-auth-attestation.json",
)

invalid_product_ingest_src = scratch_dir / "invalid-product-ingest-src"
invalid_product_ingest_src.mkdir()
for path in src.iterdir():
    if path.is_file():
        (invalid_product_ingest_src / path.name).write_bytes(path.read_bytes())
invalid_product = json.loads(
    (invalid_product_ingest_src / "product.json").read_text(encoding="utf-8")
)
invalid_product["ingest_writer"]["evidence_files"]["job"] = "../job.json"
(invalid_product_ingest_src / "product.json").write_text(
    json.dumps(invalid_product), encoding="utf-8"
)
run_helper_expect_fail(
    [
        "--kind",
        "product",
        "--artifact",
        str(invalid_product_ingest_src / "product.json"),
        "--out-dir",
        str(scratch_dir / "invalid-product-ingest-out"),
        "--artifact-name",
        "standing-runtime-product-evidence.json",
    ],
    "product ingest_writer.evidence_files.job must be ingest-writer-job.json",
)

workflow = workflow_path.read_text(encoding="utf-8")
workflow_checks = {
    "workflow requires RustFS production GC validation input": (
        "rustfs-production-gc-validation-evidence-path" in workflow
        and "RUSTFS_PRODUCTION_GC_VALIDATION_EVIDENCE_PATH" in workflow
        and "release gate requires inputs.rustfs-production-gc-validation-evidence-path"
        in workflow
    ),
    "workflow requires image subject digests for trusted backend-time provenance": (
        "velorix-api-image-digest" in workflow
        and "velorix-meta-image-digest" in workflow
        and "hiqlite-image-digest" in workflow
        and "release gate requires inputs.velorix-api-image-digest" in workflow
        and "release gate requires inputs.velorix-meta-image-digest" in workflow
        and "release gate requires inputs.hiqlite-image-digest" in workflow
    ),
    "workflow trusted provenance requires protected release ref": (
        'refs/heads/main | refs/tags/v*)' in workflow
        and "release gate trusted provenance requires refs/heads/main or refs/tags/v*" in workflow
    ),
    "workflow can mint Sigstore bundle with GitHub OIDC": (
        "id-token: write" in workflow
        and "sigstore/cosign-installer" in workflow
        and "cosign sign-blob" in workflow
        and "--bundle \"$HIQLITE_BACKEND_TIME_SIGSTORE_BUNDLE_PATH\"" in workflow
        and "VELORIX_CI_SIGSTORE_BUNDLE_BASE64" in workflow
        and "VELORIX_CI_SIGSTORE_BUNDLE_SHA256" in workflow
    ),
    "workflow regenerates trusted backend-time attestation before release readiness": (
        "HIQLITE_BACKEND_TIME_CANONICAL_BUNDLE_PATH" in workflow
        and "VELORIX_HIQLITE_BACKEND_TIME_CANONICAL_BUNDLE_FILE" in workflow
        and "HIQLITE_BACKEND_TIME_CANONICAL_BUNDLE_SHA256" in workflow
        and "VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1" in workflow
        and "VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY" in workflow
        and "--update-product-evidence" in workflow
        and 'test "$HIQLITE_BACKEND_TIME_CANONICAL_BUNDLE_SHA256" = "$(sha256sum "$HIQLITE_BACKEND_TIME_CANONICAL_BUNDLE_PATH" | awk \'{print $1}\')"' in workflow
        and "--standing-runtime-product-evidence \"$STANDING_RUNTIME_PRODUCT_RELEASE_PATH\""
        in workflow
    ),
    "workflow copies RustFS production GC family": (
        "--kind rustfs-production-gc" in workflow
        and "--out-dir target/release-evidence/rustfs-production-gc" in workflow
        and "RUSTFS_PRODUCTION_GC_VALIDATION_RELEASE_PATH" in workflow
        and "PRODUCTION_GC_RELEASE_PATH" in workflow
    ),
    "workflow validates copied RustFS production GC family": (
        'cmp -s "$PRODUCTION_GC_EVIDENCE_PATH" "$PRODUCTION_GC_RELEASE_PATH"' in workflow
        and "rustfs-production-gc-evidence-validate" in workflow
        and "--gate-evidence target/release-evidence/rustfs-production-gc/rustfs-s3-gate-evidence.json"
        in workflow
        and "--seed-evidence target/release-evidence/rustfs-production-gc/rustfs-production-gc-seed.json"
        in workflow
        and "--execute-evidence target/release-evidence/rustfs-production-gc/rustfs-production-gc-run.json"
        in workflow
        and '--production-evidence "$PRODUCTION_GC_RELEASE_PATH"' in workflow
        and "rustfs-production-gc-validation-rechecked.json" in workflow
        and '--production-gc-run-evidence "$PRODUCTION_GC_RELEASE_PATH"' in workflow
        and '--rustfs-production-gc-validation-evidence "$RUSTFS_PRODUCTION_GC_RECHECKED_PATH"'
        in workflow
    ),
    "workflow uploads RustFS production GC family directory": (
        "target/release-evidence/rustfs-production-gc/**" in workflow
    ),
    "workflow copies lifecycle siblings": (
        "scripts/copy-readiness-sibling-evidence.py \\" in workflow
        and "--kind ingest-writer-lifecycle" in workflow
        and "--out-dir target/release-evidence/ingest-writer-lifecycle" in workflow
    ),
    "workflow copies product siblings": (
        "--kind product" in workflow
        and "--out-dir target/release-evidence/standing-runtime-product" in workflow
    ),
    "workflow validates copied lifecycle artifact": (
        '--ingest-writer-lifecycle-evidence "$INGEST_WRITER_LIFECYCLE_RELEASE_PATH"'
        in workflow
    ),
    "workflow validates copied product artifact": (
        '--standing-runtime-product-evidence "$STANDING_RUNTIME_PRODUCT_RELEASE_PATH"'
        in workflow
    ),
    "workflow uploads lifecycle sibling directory": (
        "target/release-evidence/ingest-writer-lifecycle/**" in workflow
    ),
    "workflow uploads product sibling directory": (
        "target/release-evidence/standing-runtime-product/**" in workflow
    ),
}
failed = [name for name, ok in workflow_checks.items() if not ok]
if failed:
    raise SystemExit(
        "release workflow sibling evidence contract failed:\n- " + "\n- ".join(failed)
    )

print("release evidence copy contract check passed")
PY
