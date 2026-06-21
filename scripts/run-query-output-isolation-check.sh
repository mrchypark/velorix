#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
default_evidence="${repo_root}/target/velorix-product/query-output-isolation.json"
evidence_path="${VELORIX_QUERY_OUTPUT_ISOLATION_EVIDENCE_PATH:-$default_evidence}"

usage() {
  cat <<EOF
Run the fail-closed live query output isolation evidence check.

Usage:
  scripts/run-query-output-isolation-check.sh [--evidence PATH]

This helper does not automate the live release isolation drill. It validates
real evidence after live steps write:
  target/velorix-product/query-output-isolation.json

Live-only boundary:
  1. Record deployment_id and an s3:// authority_store_id.
  2. Deploy a release-like environment with published materialized output.
  3. Run a cold query through the release query path.
  4. Verify query authority is published_materialized_output.
  5. Deny query-pod reads of source ingest prefixes.
  6. Deny query-pod metadata writes.
  7. Audit object storage for no source reads, source writes, or durable writes.
  8. Verify materialized output was read and no source recomputation occurred.
  9. Attach evidence_refs for query_pod_iam_policy, cold_query_log,
     object_store_audit_log, and materialized_output_read.
  10. Write evidence kind query_output_isolation and run this helper.

Override evidence with --evidence PATH or VELORIX_QUERY_OUTPUT_ISOLATION_EVIDENCE_PATH=PATH.
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

validator="${repo_root}/scripts/check-query-output-isolation-evidence.sh"
if [ ! -f "$validator" ]; then
  echo "missing validator: $validator" >&2
  exit 1
fi

if [ ! -f "$evidence_path" ]; then
  usage >&2
  echo >&2
  echo "Query output isolation evidence not found: $evidence_path" >&2
  echo "No pass artifact was produced; run the live release isolation steps above, then rerun this helper." >&2
  exit 1
fi

bash "$validator" "$evidence_path"
