#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
default_evidence="${repo_root}/target/velorix-product/remaining-release-readiness.json"
evidence_path="${VELORIX_REMAINING_RELEASE_READINESS_EVIDENCE_PATH:-$default_evidence}"

usage() {
  cat <<EOF
Run the fail-closed remaining 1.0 release-readiness evidence check.

Usage:
  scripts/run-remaining-release-readiness-check.sh [--evidence PATH]

This helper does not automate the live/release readiness drills. It validates
real evidence after release/live steps write:
  target/velorix-product/remaining-release-readiness.json

Manual live/release evidence must prove:
  1. deployment_id and an s3:// authority_store_id are recorded.
  2. Release-image contract tests passed against the exact release image.
  3. A versioned OpenAPI contract is verified and no conflicting accepted
     contracts remain.
  4. The fail-closed SQL admission corpus covers unsupported DataFusion plan
     and expression nodes; unsupported SQL leaves no persisted metadata or
     runtime binding; mutation CI fails when a capability check is removed.
  5. Persistent-write-boundary crash matrix covers one view, multiple affected
     views, joins, and compaction.
  6. Replay determinism covers duplicate, reordered, gapped, and retried
     batches; live crash, clean replay, outputs, and checkpoint hashes match;
     non-contiguous input never advances a frontier.
  7. Join-frontier evidence verifies the spec, concurrent two-input ingest with
     crash and leader handoff, and exact input frontiers in output manifests.
  8. Published limits are verified and multi-day soak passed against the
     supported object store.
  9. evidence_refs are attached for release-image contract tests, OpenAPI
     contract, SQL admission corpus, crash matrix, replay determinism, join
     frontier, and scale soak.

Override evidence with --evidence PATH or
VELORIX_REMAINING_RELEASE_READINESS_EVIDENCE_PATH=PATH.
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

validator="${repo_root}/scripts/check-remaining-release-readiness-evidence.sh"
if [ ! -f "$validator" ]; then
  echo "missing validator: $validator" >&2
  exit 1
fi

if [ ! -f "$evidence_path" ]; then
  usage >&2
  echo >&2
  echo "Remaining 1.0 release-readiness evidence not found: $evidence_path" >&2
  echo "No pass artifact was produced; run the live/release steps above, then rerun this helper." >&2
  exit 1
fi

bash "$validator" "$evidence_path"
