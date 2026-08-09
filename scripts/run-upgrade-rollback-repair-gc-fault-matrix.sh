#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
default_evidence="${repo_root}/target/velorix-product/upgrade-rollback-repair-gc-fault-matrix.json"
evidence_path="${VELORIX_UPGRADE_ROLLBACK_REPAIR_GC_FAULT_MATRIX_EVIDENCE_PATH:-$default_evidence}"

usage() {
  cat <<EOF
Run the fail-closed upgrade/rollback/repair/GC fault-matrix evidence check.

Usage:
  scripts/run-upgrade-rollback-repair-gc-fault-matrix.sh [--evidence PATH]

This helper does not automate the live release matrix. It validates real
evidence after the live drill writes:
  target/velorix-product/upgrade-rollback-repair-gc-fault-matrix.json

Live-only boundary:
  1. Record deployment_id and an s3:// authority_store_id.
  2. Run rolling_upgrade with active ingest and persisted checkpoints.
  3. Run rollback_after_upgrade and prove N reads N-1 checkpoint/output formats.
  4. Run corrupt_latest_checkpoint_repair without source-query recomputation.
  5. Run GC concurrently with query, compaction, recovery, and checkpoint publication.
  6. Prove gc_retains_repair_roots and acknowledged data survive.
  7. Write evidence kind upgrade_rollback_repair_gc_fault_matrix and run this helper.

Override evidence with --evidence PATH or
VELORIX_UPGRADE_ROLLBACK_REPAIR_GC_FAULT_MATRIX_EVIDENCE_PATH=PATH.
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

validator="${repo_root}/scripts/check-upgrade-rollback-repair-gc-fault-matrix-evidence.sh"
if [ ! -f "$validator" ]; then
  echo "missing validator: $validator" >&2
  exit 1
fi

if [ ! -f "$evidence_path" ]; then
  usage >&2
  echo >&2
  echo "Upgrade/rollback/repair/GC fault-matrix evidence not found: $evidence_path" >&2
  echo "No pass artifact was produced; run the live matrix above, then rerun this helper." >&2
  exit 1
fi

bash "$validator" "$evidence_path"
