#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Validate Hiqlite backup/restore configuration evidence.

Usage:
  scripts/check-hiqlite-backup-restore-evidence.sh PATH

This validates configuration evidence only. It does not prove that a restore
drill has succeeded.
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

if [ "$#" -ne 1 ]; then
  usage >&2
  exit 64
fi

python3 - "$1" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    evidence = json.load(f)

authority = evidence.get("hiqlite_authority") or evidence
errors = []

authority_kind = authority.get("authority_kind")
if authority_kind not in {
    "velorix_managed_hiqlite",
    "external_hiqlite",
    "hiqlite",
}:
    errors.append("authority_kind must identify a Hiqlite authority")

if (
    authority.get("metadata_authority_storage_mode")
    != "object-store-backup-restore-with-ephemeral-node-disk"
):
    errors.append(
        "metadata_authority_storage_mode must be object-store-backup-restore-with-ephemeral-node-disk"
    )

if authority.get("backup_restore_configured") is not True:
    errors.append("backup_restore_configured must be true")

nodes = authority.get("nodes")
if nodes is not None:
    if not isinstance(nodes, list) or len(set(nodes)) != 3:
        errors.append("nodes must contain exactly three unique voters when present")

text = json.dumps(evidence, sort_keys=True).lower()
for forbidden in ("persistentvolumeclaim", "volumeclaimtemplates", '"pvc"'):
    if forbidden in text:
        errors.append(f"evidence must not contain {forbidden}")

if errors:
    print(json.dumps({"status": "fail", "errors": errors}, indent=2, sort_keys=True), file=sys.stderr)
    raise SystemExit(1)

print(json.dumps({
    "status": "pass",
    "evidence_kind": "hiqlite_backup_restore_configuration",
    "restore_drill_verified": False,
    "message": "backup/restore configuration evidence is valid; restore execution is not proven by this check",
}, sort_keys=True))
PY
