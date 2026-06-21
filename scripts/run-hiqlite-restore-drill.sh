#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
default_evidence="${repo_root}/target/velorix-product/hiqlite-restore-drill.json"
evidence_path="${VELORIX_HIQLITE_RESTORE_DRILL_EVIDENCE_PATH:-$default_evidence}"

usage() {
  cat <<EOF
Run the fail-closed Hiqlite total-voter-loss restore drill evidence check.

Usage:
  scripts/run-hiqlite-restore-drill.sh [--evidence PATH]

This helper does not automate the live Kubernetes/Hiqlite/RustFS disaster drill.
It validates real evidence after the live drill writes:
  target/velorix-product/hiqlite-restore-drill.json

Live-only boundary:
  1. Record deployment_id and an s3:// authority_store_id.
  2. Start a release-like no-PVC, three-voter Hiqlite authority with object-store backups.
  3. Perform acknowledged Velorix metadata writes after materialized ingest.
  4. Destroy every Hiqlite voter and node-local disk.
  5. Restore only from object-store backup.
  6. Verify catalog, owner epoch, checkpoint pointer, and post-restore ingest/query.
  7. Attach evidence_refs for object_store_backup, total_voter_loss_log,
     restore_log, metadata_write_survival, and post_restore_ingest_query.
  8. Write standalone drill evidence kind hiqlite_total_voter_loss_restore_drill
     or readiness compatibility evidence kind
     hiqlite_no_pvc_three_voter_backup_restore, then run this helper.

Override evidence with --evidence PATH or VELORIX_HIQLITE_RESTORE_DRILL_EVIDENCE_PATH=PATH.
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

validator="${repo_root}/scripts/check-hiqlite-restore-drill-evidence.sh"
if [ ! -f "$validator" ]; then
  echo "missing validator: $validator" >&2
  exit 1
fi

if [ ! -f "$evidence_path" ]; then
  usage >&2
  echo >&2
  echo "Hiqlite restore drill evidence not found: $evidence_path" >&2
  echo "No pass artifact was produced; run the live drill above, then rerun this helper." >&2
  exit 1
fi

bash "$validator" "$evidence_path"
