#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
default_evidence="${repo_root}/target/velorix-product/security-release-provenance.json"
evidence_path="${VELORIX_SECURITY_RELEASE_PROVENANCE_EVIDENCE_PATH:-$default_evidence}"

usage() {
  cat <<EOF
Run the fail-closed live security and release provenance evidence check.

Usage:
  scripts/run-security-release-provenance-check.sh [--evidence PATH]

This helper does not automate live security verification or release
provenance generation. It validates real evidence after release/live steps
write:
  target/velorix-product/security-release-provenance.json

Live/release evidence must prove:
  1. deployment_id and an s3:// authority_store_id are recorded.
  2. API and metadata auth are mandatory.
  3. Tenant authorization and negative cross-tenant tests passed.
  4. TLS, secret rotation, body limits, and rate limits were verified.
  5. Object prefix isolation was verified against the release object store.
  6. The source revision was clean, recorded as a 40-character git SHA, and
     exact deployed image digests for velorix-api and velorix-meta were checked
     and recorded as sha256 digests.
  7. SBOM, dependency-policy, and immutable test evidence are attached.
  8. evidence_refs are attached for auth, TLS, secret rotation, limits,
     object-prefix isolation, cross-tenant negative tests, SBOM, dependency
     policy, and immutable test evidence.

Override evidence with --evidence PATH or
VELORIX_SECURITY_RELEASE_PROVENANCE_EVIDENCE_PATH=PATH.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --evidence)
      if [ "$#" -lt 2 ] || [ -z "${2:-}" ]; then
        echo "--evidence requires a path" >&2
        exit 64
      fi
      evidence_path="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

validator="${repo_root}/scripts/check-security-release-provenance-evidence.sh"
if [ ! -f "$validator" ]; then
  echo "missing validator: $validator" >&2
  exit 1
fi

if [ ! -f "$evidence_path" ]; then
  usage >&2
  echo >&2
  echo "Security/release provenance evidence not found: $evidence_path" >&2
  echo "No pass artifact was produced; run the live release/security steps above, then rerun this helper." >&2
  exit 1
fi

bash "$validator" "$evidence_path"
