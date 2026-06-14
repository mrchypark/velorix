#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
doc_path="${repo_root}/docs/architecture/feldera-compiler-worker.md"
product_doc_path="${repo_root}/docs/development/vind-product.md"
dockerfile_path="${repo_root}/Dockerfile.feldera-compiler-worker"
crate_manifest_path="${repo_root}/crates/velorix-feldera-compiler-worker/Cargo.toml"
live_runner_path="${repo_root}/scripts/run-live-feldera-pipeline-manager.sh"

python3 - "$doc_path" "$product_doc_path" "$dockerfile_path" "$crate_manifest_path" "$live_runner_path" <<'PY'
import sys
from pathlib import Path

doc_path = Path(sys.argv[1])
product_doc_path = Path(sys.argv[2])
dockerfile_path = Path(sys.argv[3])
crate_manifest_path = Path(sys.argv[4])
live_runner_path = Path(sys.argv[5])
live_validator_path = live_runner_path.parent / "validate-live-feldera-evidence.py"

doc = doc_path.read_text(encoding="utf-8")
product_doc = product_doc_path.read_text(encoding="utf-8")
dockerfile = dockerfile_path.read_text(encoding="utf-8")
crate_manifest = crate_manifest_path.read_text(encoding="utf-8")
live_runner = live_runner_path.read_text(encoding="utf-8")
live_validator = live_validator_path.read_text(encoding="utf-8")
normalized_product_doc = " ".join(product_doc.split())

checks = {
    "defines lean API image": "`velorix-api`" in doc
    and "admits relations" in doc
    and "Cargo" in doc
    and "Java/Maven" in doc,
    "defines compiler worker": "`velorix-feldera-compiler-worker`" in doc
    and "compilation outside the API process" in doc,
    "uses REST completion authority": "POST /v1/view-compile-deploy/jobs/{view_id}/complete" in doc
    and "POST /v1/view-compile-deploy/jobs/{view_id}/claim" in doc
    and "does not write Velorix metadata or object" in doc
    and "runtime_deployment" in doc,
    "defines product runtime completion payload": "product_runtime" in doc
    and "jarless Feldera package runtime descriptor" in doc
    and "exactly one of `artifact`, `product_runtime`, or `runtime_deployment`" in doc,
    "keeps official image as fixture": "official" in doc
    and "pipeline-manager" in doc
    and "compatibility fixture" in doc,
    "preserves no-PVC constraint": "PVC remains out of scope" in doc
    and "default product deployment creates no PVCs" in doc,
    "requires compiler-backed SQL": "jarless package-backed" in doc
    and "does not fall" in doc
    and "fake generic implementation" in doc,
    "records production worker hardening gates": "claim, lease, fencing token" in doc
    and "tenant-scoped admin auth" in doc
    and "orphan" in doc
    and "arbitrary caller SQL" in doc,
    "product doc links split": "Feldera Compiler Worker Split" in product_doc
    and "Do not add PVCs" in product_doc
    and "bundle the Feldera all-in-one image into `velorix-api`" in product_doc,
    "product doc documents runtime deployment completion": "runtime_deployment" in product_doc
    and "product_runtime" in product_doc
    and "rest_product_worker_activates_pending_view_from_jarless_product_runtime_descriptor" in product_doc
    and "exactly one" in normalized_product_doc
    and '"mode": "external_managed"' in product_doc,
    "product doc documents claim proof": "/v1/view-compile-deploy/jobs/$VIEW_ID/claim" in product_doc
    and "tenant_id" in product_doc
    and "job_generation" in product_doc
    and "lease_id" in product_doc
    and "fencing_token" in product_doc,
    "worker crate exists": "name = \"velorix-feldera-compiler-worker\"" in crate_manifest
    and "reqwest" in crate_manifest
    and "clap" in crate_manifest,
    "worker dockerfile is split": "velorix-feldera-compiler-worker" in dockerfile
    and "pipeline-manager:latest" not in dockerfile
    and "USER 65532:65532" in dockerfile,
    "jarless product rule is documented": "jarless" in doc
    and "SQL compiler jar" in doc
    and "the product backend must not ship Feldera's SQL compiler jar" in doc
    and "not the default backend" in doc
    and "does not satisfy this product gate" in doc
    and "jarless product path" in product_doc,
    "worker defaults to jarless backend": "default compiler backend is `feldera-package-jarless`" in product_doc
    and "--compiler-backend compatibility-pipeline-manager" in product_doc,
    "live runner has no implicit jar backend": 'image="${VELORIX_LIVE_FELDERA_IMAGE:-}"' in live_runner
    and "VELORIX_LIVE_FELDERA_ALLOW_OFFICIAL_IMAGE" in live_runner
    and "has no default image because the upstream path requires the SQL compiler jar" in live_runner
    and "Refusing upstream Feldera all-in-one image by default" in live_runner,
    "live runner evidence is compatibility-only": '"evidence_scope": "compatibility_fixture"' in live_runner
    and '"product_evidence": False' in live_runner
    and '"backend_kind": "pipeline_manager"' in live_runner
    and '"jarless_backend_attested": False' in live_runner
    and "product_evidence=false" in live_validator
    and "jarless_backend_attested=false" in live_validator,
    "worker doc says jarless backend is schema-only by default": "output_contract=must_match" in doc
    and "compiled_schema_only_not_deployed" in doc
    and "requires_java_sql_compiler=true" in doc
    and "--claim-without-backend" in doc
    and "claimed_not_compiled" in doc
    and "/v1/view-compile-deploy/run-once" in doc,
    "worker doc says backend completes deployment": "VELORIX_FELDERA_PIPELINE_MANAGER_URL" in doc
    and "completed_compatibility_runtime_deployment" in doc
    and "completed_product_runtime_deployment" in doc
    and "compiled_schema_only_not_deployed" in doc
    and "unsupported_by_selected_backend" in doc
    and "jarless product-runtime completion result" in doc
    and "program_info.schema.outputs" in doc,
    "product doc includes worker usage": "Dockerfile.feldera-compiler-worker" in product_doc
    and "compiled_schema_only_not_deployed" in product_doc
    and "unsupported_by_selected_backend" in product_doc
    and "--claim-without-backend" in product_doc
    and "claimed_not_compiled" in product_doc,
    "product doc includes backend worker usage": "--feldera-pipeline-manager-url" in product_doc
    and "completed" in product_doc
    and "runtime_deployment.mode=external_managed" in product_doc,
    "worker crate depends on core contract": "velorix-core" in crate_manifest,
}

failed = [name for name, ok in checks.items() if not ok]
if failed:
    for name in failed:
        print(f"missing contract: {name}", file=sys.stderr)
    raise SystemExit(1)

print("feldera compiler-worker split contract: ok")
PY
