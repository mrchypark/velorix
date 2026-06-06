#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
workflow_path="${repo_root}/.github/workflows/release-gate.yml"
ci_path="${repo_root}/.github/workflows/ci.yml"
doc_path="${repo_root}/docs/release/1.0-readiness-checklist.md"

python3 - "$workflow_path" "$ci_path" "$doc_path" <<'PY'
import re
import sys

workflow_path, ci_path, doc_path = sys.argv[1:]
with open(workflow_path, "r", encoding="utf-8") as f:
    workflow = f.read()
with open(ci_path, "r", encoding="utf-8") as f:
    ci = f.read()
with open(doc_path, "r", encoding="utf-8") as f:
    doc = f.read()

checks = {
    "workflow requires Feldera input dispatch fields": (
        "feldera-spec-path:" in workflow
        and "feldera-metadata-path:" in workflow
        and "feldera-artifact-package-path:" in workflow
        and "release gate requires inputs.feldera-spec-path" in workflow
        and "release gate requires inputs.feldera-metadata-path" in workflow
        and "release gate requires inputs.feldera-artifact-package-path" in workflow
    ),
    "workflow maps Feldera input env vars": (
        "FELDERA_SPEC_PATH: ${{ inputs.feldera-spec-path }}" in workflow
        and "FELDERA_METADATA_PATH: ${{ inputs.feldera-metadata-path }}" in workflow
        and "FELDERA_ARTIFACT_PACKAGE_PATH: ${{ inputs.feldera-artifact-package-path }}"
        in workflow
    ),
    "workflow probes all Feldera input files before verification": (
        'test -f "$FELDERA_SPEC_PATH"' in workflow
        and 'test -f "$FELDERA_METADATA_PATH"' in workflow
        and 'test -f "$FELDERA_ARTIFACT_PACKAGE_PATH"' in workflow
    ),
    "workflow preserves raw Feldera inputs in release evidence directory": (
        "mkdir -p target/release-evidence/feldera-inputs" in workflow
        and 'cp "$FELDERA_SPEC_PATH" target/release-evidence/feldera-inputs/standing-view-spec.json'
        in workflow
        and 'cp "$FELDERA_METADATA_PATH" target/release-evidence/feldera-inputs/compile-artifact-metadata.json'
        in workflow
        and 'cp "$FELDERA_ARTIFACT_PACKAGE_PATH" target/release-evidence/feldera-inputs/artifact-package'
        in workflow
    ),
    "workflow writes checksum manifest for preserved Feldera inputs": bool(
        re.search(
            r"cd target/release-evidence/feldera-inputs\s+sha256sum standing-view-spec\.json compile-artifact-metadata\.json artifact-package > SHA256SUMS",
            workflow,
        )
    ),
    "workflow verifies Feldera artifact hash from supplied inputs": (
        "cargo run -p velorix-cli -- feldera-artifact-verify \\" in workflow
        and '--spec "$FELDERA_SPEC_PATH"' in workflow
        and '--metadata "$FELDERA_METADATA_PATH"' in workflow
        and '--artifact-package "$FELDERA_ARTIFACT_PACKAGE_PATH"' in workflow
        and "> target/release-evidence/feldera-artifact-hash.json" in workflow
    ),
    "workflow passes hash evidence to readiness report": (
        "--feldera-artifact-hash-evidence target/release-evidence/feldera-artifact-hash.json"
        in workflow
    ),
    "workflow uploads preserved Feldera inputs": (
        "target/release-evidence/feldera-inputs/**" in workflow
    ),
    "workflow does not require release provenance input": (
        "release gate requires inputs.feldera-release-provenance" not in workflow
        and "FELDERA_RELEASE_PROVENANCE_PATH" not in workflow
    ),
    "CI runs this contract in script-contract job": (
        "bash -n scripts/check-feldera-release-inputs-contract.sh" in ci
        and "scripts/check-feldera-release-inputs-contract.sh" in ci
    ),
    "release checklist documents preserved raw inputs and optional provenance": (
        "target/release-evidence/feldera-inputs/" in doc
        and "SHA256SUMS" in doc
        and "it is not required for product/readiness completion" in doc
    ),
}

failed = [name for name, ok in checks.items() if not ok]
if failed:
    raise SystemExit(
        "Feldera release input contract check failed:\n- " + "\n- ".join(failed)
    )

print("Feldera release input contract check passed")
PY
