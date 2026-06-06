#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="${1:-${VELORIX_HIQLITE_BACKEND_TIME_ASSESSMENT_DIR:-${repo_root}/target/velorix-hiqlite-backend-time-assessment}}"
output_path="${VELORIX_HIQLITE_BACKEND_TIME_ASSESSMENT_PATH:-${output_dir}/hiqlite-backend-time-assessment.json}"
product_evidence_path="${VELORIX_PRODUCT_EVIDENCE_PATH:-}"
require_backend_time="${VELORIX_REQUIRE_HIQLITE_BACKEND_TIME:-0}"
update_product_evidence="${VELORIX_HIQLITE_BACKEND_TIME_UPDATE_PRODUCT_EVIDENCE:-0}"

mkdir -p "$output_dir"

python3 - "$repo_root" "$output_path" "$product_evidence_path" "$require_backend_time" "$update_product_evidence" <<'PY'
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

repo_root = Path(sys.argv[1])
output_path = Path(sys.argv[2])
product_evidence_path = Path(sys.argv[3]) if sys.argv[3] else None
require_backend_time = sys.argv[4] == "1"
update_product_evidence = sys.argv[5] == "1"


def cargo_metadata() -> dict:
    result = subprocess.run(
        [
            "cargo",
            "metadata",
            "--format-version",
            "1",
            "--features",
            "velorix-meta/hiqlite-backend",
        ],
        cwd=repo_root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return json.loads(result.stdout)


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def find_hiqlite_package(metadata: dict) -> dict:
    candidates = [
        package for package in metadata.get("packages", []) if package.get("name") == "hiqlite"
    ]
    if candidates:
        return candidates[0]
    lock_package = find_cargo_lock_package()
    checkout_manifest = find_hiqlite_checkout_manifest(lock_package.get("source"))
    lock_package["manifest_path"] = str(checkout_manifest)
    return lock_package


def find_cargo_lock_package() -> dict:
    cargo_lock = read_text(repo_root / "Cargo.lock")
    blocks = cargo_lock.split("\n[[package]]\n")
    for block in blocks:
        if re.search(r'^name = "hiqlite"$', block, flags=re.MULTILINE):
            version = re.search(r'^version = "([^"]+)"$', block, flags=re.MULTILINE)
            source = re.search(r'^source = "([^"]+)"$', block, flags=re.MULTILINE)
            return {
                "name": "hiqlite",
                "version": version.group(1) if version else None,
                "source": source.group(1) if source else None,
            }
    raise SystemExit("Cargo.lock did not include a hiqlite package")


def find_hiqlite_checkout_manifest(source: str | None) -> Path:
    cargo_home = Path(os.environ.get("CARGO_HOME", str(Path.home() / ".cargo")))
    checkout_root = cargo_home / "git" / "checkouts"
    wanted_prefix = None
    if source and "#" in source:
        wanted_prefix = source.rsplit("#", 1)[1][:7]
    candidates = sorted(checkout_root.glob("hiqlite-*/*/hiqlite/Cargo.toml"))
    if wanted_prefix:
        for candidate in candidates:
            if candidate.parts[-3].startswith(wanted_prefix):
                return candidate
    if candidates:
        return candidates[-1]
    raise SystemExit(f"could not find hiqlite checkout below {checkout_root}")


def scan_source(source_root: Path) -> dict:
    client_dir = source_root / "src" / "client"
    network_dir = source_root / "src" / "network"
    memory_dir = source_root / "src" / "store" / "state_machine" / "memory"

    execute_rs = read_text(client_dir / "execute.rs")
    query_rs = read_text(client_dir / "query.rs")
    transaction_rs = read_text(client_dir / "transaction.rs")
    param_rs = read_text(source_root / "src" / "store" / "state_machine" / "sqlite" / "param.rs")
    transaction_env_rs = read_text(
        source_root / "src" / "store" / "state_machine" / "sqlite" / "transaction_env.rs"
    )
    state_machine_rs = read_text(
        source_root / "src" / "store" / "state_machine" / "sqlite" / "state_machine.rs"
    )
    mgmt_rs = read_text(client_dir / "mgmt.rs")
    dlock_rs = read_text(client_dir / "dlock.rs")
    dlock_handler_rs = read_text(memory_dir / "dlock_handler.rs")
    api_rs = read_text(network_dir / "api.rs")
    combined = "\n".join(
        [
            execute_rs,
            query_rs,
            transaction_rs,
            param_rs,
            transaction_env_rs,
            state_machine_rs,
            mgmt_rs,
            dlock_rs,
            dlock_handler_rs,
            api_rs,
        ]
    )
    authority_time_transaction_api = (
        "pub async fn txn_with_authority_time" in transaction_rs
        and "QueryWrite::TransactionWithAuthorityTime" in transaction_rs
    )
    authority_unix_ms_param = (
        "AuthorityUnixMs" in param_rs
        and "pub fn authority_unix_ms()" in param_rs
        and "lookup_authority_unix_ms" in transaction_env_rs
    )
    raft_replicated_authority_time_payload = (
        "pub struct AuthorityTime" in state_machine_rs
        and "TransactionWithAuthorityTime" in state_machine_rs
        and "authority_unix_ms" in state_machine_rs
    )

    public_time_api_pattern = re.compile(
        r"pub\s+(?:async\s+)?fn\s+[a-zA-Z0-9_]*(?:backend|authority|server|raft)[a-zA-Z0-9_]*time|"
        r"pub\s+(?:async\s+)?fn\s+[a-zA-Z0-9_]*time[a-zA-Z0-9_]*(?:backend|authority|server|raft)",
        flags=re.IGNORECASE,
    )
    raft_time_payload_pattern = re.compile(
        r"\b(?:AuthorityTime|BackendTime|RaftTime|ServerTime|TimeSource|LeaseTime)\b"
    )

    return {
        "linearizable_sql_write_api": "client_write(QueryWrite::Execute" in execute_rs
        and "pub async fn execute" in execute_rs,
        "serialized_transaction_api": "client_write(QueryWrite::Transaction" in transaction_rs
        and "pub async fn txn" in transaction_rs,
        "consistent_read_api": "pub async fn query_consistent" in query_rs
        and "quorum" in query_rs,
        "raft_metrics_api": "pub async fn metrics_db" in mgmt_rs
        and "pub async fn metrics_cache" in mgmt_rs,
        "raft_metrics_are_observation_only": "millis_since_quorum_ack" in mgmt_rs
        or "RaftMetrics" in mgmt_rs,
        "distributed_lock_api": "pub async fn lock" in dlock_rs,
        "distributed_lock_process_clock_ttl": "Utc::now().timestamp()" in dlock_handler_rs
        and "LOCK_VALID_SECONDS" in dlock_handler_rs,
        "distributed_lock_fixed_timeout_seconds": 10
        if "const LOCK_VALID_SECONDS: i64 = 10" in dlock_handler_rs
        else None,
        "public_backend_authority_time_api": authority_time_transaction_api
        or public_time_api_pattern.search(combined) is not None,
        "authority_time_transaction_api": authority_time_transaction_api,
        "authority_unix_ms_transaction_param": authority_unix_ms_param,
        "raft_replicated_time_payload_found": raft_replicated_authority_time_payload
        or raft_time_payload_pattern.search(combined) is not None,
        "raft_replicated_authority_time_payload": raft_replicated_authority_time_payload,
        "backup_uses_leader_process_time": "Utc::now().timestamp()" in api_rs
        and "QueryWrite::Backup" in api_rs,
        "source_files": {
            "execute": str((client_dir / "execute.rs").resolve()),
            "query": str((client_dir / "query.rs").resolve()),
            "transaction": str((client_dir / "transaction.rs").resolve()),
            "param": str(
                (
                    source_root
                    / "src"
                    / "store"
                    / "state_machine"
                    / "sqlite"
                    / "param.rs"
                ).resolve()
            ),
            "transaction_env": str(
                (
                    source_root
                    / "src"
                    / "store"
                    / "state_machine"
                    / "sqlite"
                    / "transaction_env.rs"
                ).resolve()
            ),
            "state_machine": str(
                (
                    source_root
                    / "src"
                    / "store"
                    / "state_machine"
                    / "sqlite"
                    / "state_machine.rs"
                ).resolve()
            ),
            "management": str((client_dir / "mgmt.rs").resolve()),
            "dlock": str((client_dir / "dlock.rs").resolve()),
            "dlock_handler": str((memory_dir / "dlock_handler.rs").resolve()),
            "api": str((network_dir / "api.rs").resolve()),
        },
    }


def section_between(source: str, start: str, end: str) -> str:
    if start not in source:
        return ""
    tail = source.split(start, 1)[1]
    if end in tail:
        return tail.split(end, 1)[0]
    return tail


def scan_velorix_meta_source(repo_root: Path) -> dict:
    source_path = repo_root / "crates" / "velorix-meta" / "src" / "lib.rs"
    source = read_text(source_path)
    hiqlite_impl = section_between(
        source,
        "impl MetaStore for HiqliteMetaStore",
        "struct CatalogJsonRow",
    )
    acquire_impl = section_between(
        hiqlite_impl,
        "async fn acquire_standing_runtime_owner",
        "async fn read_standing_runtime_owner",
    )
    read_impl = section_between(
        hiqlite_impl,
        "async fn read_standing_runtime_owner",
        "async fn publish_standing_runtime_checkpoint",
    )
    publish_impl = section_between(
        hiqlite_impl,
        "async fn publish_standing_runtime_checkpoint",
        "async fn read_standing_runtime_checkpoint",
    )

    owner_acquire_uses_authority_time = (
        ".txn_with_authority_time([" in acquire_impl
        and "hiqlite::Param::authority_unix_ms()" in acquire_impl
        and "expires_at_unix_ms > $5" in acquire_impl
        and "unix_time_ms()?" not in acquire_impl
    )
    owner_read_uses_authority_time = (
        ".txn_with_authority_time([" in read_impl
        and "authority_time.unix_ms" in read_impl
        and ".filter(|claim| claim.expires_at_unix_ms > now)" in read_impl
        and "unix_time_ms()?" not in read_impl
    )
    checkpoint_publish_update_uses_authority_time = (
        ".txn_with_authority_time([" in publish_impl
        and "hiqlite::Param::authority_unix_ms()" in publish_impl
        and "owner.expires_at_unix_ms > $12" in publish_impl
    )
    checkpoint_publish_insert_uses_authority_time = (
        publish_impl.count(".txn_with_authority_time([") >= 2
        and publish_impl.count("hiqlite::Param::authority_unix_ms()") >= 2
        and "owner.expires_at_unix_ms > $9" in publish_impl
    )
    checkpoint_publish_rejects_scope_mismatch = (
        "AND $13 = $4" in publish_impl
        and "AND $14 = $5" in publish_impl
        and "AND $15 = $6" in publish_impl
        and "AND $10 = $1" in publish_impl
        and "AND $11 = $2" in publish_impl
        and "AND $12 = $3" in publish_impl
    )
    unsafe_runtime_sources_absent = all(
        forbidden not in hiqlite_impl
        for forbidden in ["metrics_db", "RaftMetrics", ".metrics()", ".lock("]
    )

    return {
        "source_file": str(source_path.resolve()),
        "owner_acquire_uses_authority_time": owner_acquire_uses_authority_time,
        "owner_read_uses_authority_time": owner_read_uses_authority_time,
        "checkpoint_publish_update_uses_authority_time": checkpoint_publish_update_uses_authority_time,
        "checkpoint_publish_insert_uses_authority_time": checkpoint_publish_insert_uses_authority_time,
        "checkpoint_publish_rejects_scope_mismatch": checkpoint_publish_rejects_scope_mismatch,
        "unsafe_runtime_time_sources_absent": unsafe_runtime_sources_absent,
    }


metadata = cargo_metadata()
package = find_hiqlite_package(metadata)
manifest_path = Path(package["manifest_path"])
source_root = manifest_path.parent
source_scan = scan_source(source_root)
velorix_meta_scan = scan_velorix_meta_source(repo_root)
required_mode_supported = (
    source_scan["authority_time_transaction_api"]
    and source_scan["authority_unix_ms_transaction_param"]
    and source_scan["raft_replicated_authority_time_payload"]
    and velorix_meta_scan["owner_acquire_uses_authority_time"]
    and velorix_meta_scan["owner_read_uses_authority_time"]
    and velorix_meta_scan["checkpoint_publish_update_uses_authority_time"]
    and velorix_meta_scan["checkpoint_publish_insert_uses_authority_time"]
    and velorix_meta_scan["checkpoint_publish_rejects_scope_mismatch"]
    and velorix_meta_scan["unsafe_runtime_time_sources_absent"]
)

product_capability = None
if product_evidence_path and product_evidence_path.is_file():
    product = json.loads(product_evidence_path.read_text(encoding="utf-8"))
    product_capability = (
        product.get("standing_runtime_fencing", {}).get("capability")
        or product.get("metadata_store", {}).get("standing_runtime_fencing")
    )

missing_capabilities = []
if not source_scan["authority_time_transaction_api"]:
    missing_capabilities.append(
        "public Hiqlite API or Velorix-owned operation that samples authority wall-clock time inside a Raft-replicated write"
    )
if not source_scan["authority_unix_ms_transaction_param"]:
    missing_capabilities.append(
        "transaction parameter that binds the Raft-serialized authority Unix timestamp into lease SQL"
    )
if not source_scan["raft_replicated_authority_time_payload"]:
    missing_capabilities.append(
        "Raft command/response payload carrying the same authority timestamp to all replicas"
    )
if not velorix_meta_scan["owner_acquire_uses_authority_time"]:
    missing_capabilities.append(
        "Velorix Hiqlite owner acquire implemented against the Raft-serialized authority timestamp"
    )
if not velorix_meta_scan["owner_read_uses_authority_time"]:
    missing_capabilities.append(
        "Velorix Hiqlite owner read filters lease expiry against the Raft-serialized authority timestamp"
    )
if not velorix_meta_scan["checkpoint_publish_update_uses_authority_time"]:
    missing_capabilities.append(
        "Velorix Hiqlite checkpoint update path rejects expired owners with authority time"
    )
if not velorix_meta_scan["checkpoint_publish_insert_uses_authority_time"]:
    missing_capabilities.append(
        "Velorix Hiqlite checkpoint insert path rejects expired owners with authority time"
    )
if not velorix_meta_scan["checkpoint_publish_rejects_scope_mismatch"]:
    missing_capabilities.append(
        "Velorix Hiqlite checkpoint publish SQL rejects owner/candidate scope mismatch"
    )
if not velorix_meta_scan["unsafe_runtime_time_sources_absent"]:
    missing_capabilities.append(
        "Velorix Hiqlite standing-runtime implementation does not derive lease safety from metrics, Raft log index, or dlock TTL"
    )
if not required_mode_supported:
    missing_capabilities.extend(
        [
            "bounded wall-clock failover smoke proving stale owner rejection after the configured TTL",
        ]
    )

unsafe_substitutes_rejected = [
    "client-side SystemTime/UNIX_EPOCH timestamp",
    "SQLite CURRENT_TIMESTAMP/strftime/unixepoch evaluated independently by replicated state machines",
    "Raft metrics such as last_applied/current_term/millis_since_quorum_ack used as a timestamp",
    "Hiqlite dlock fixed 10 second process-clock timeout used as the standing-runtime owner lease",
]

assessment = {
    "schema_version": 1,
    "evidence_kind": "velorix_hiqlite_backend_time_assessment",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "hiqlite_package": {
        "name": package.get("name"),
        "version": package.get("version"),
        "source": package.get("source"),
        "manifest_path": str(manifest_path.resolve()),
        "source_root": str(source_root.resolve()),
    },
    "observed_capabilities": source_scan,
    "velorix_meta_runtime": velorix_meta_scan,
    "product_capability": product_capability,
    "required_mode_supported": required_mode_supported,
    "can_generate_product_complete_backend_time_attestation": required_mode_supported,
    "backend_time_source_kind": "raft_replicated_authority_time"
    if required_mode_supported
    else "unavailable",
    "backend_time_blocked_reason": ""
    if required_mode_supported
    else "hiqlite_raft_replicated_authority_time_primitive_missing",
    "lease_authority_kind": "raft_replicated_time"
    if required_mode_supported
    else "hiqlite_raft_serialized",
    "lease_expiry_semantics": "backend_wall_clock_ttl"
    if required_mode_supported
    else "operation_driven_logical",
    "bounded_wall_clock_failover": required_mode_supported,
    "missing_capabilities": missing_capabilities,
    "unsafe_substitutes_rejected": unsafe_substitutes_rejected,
    "verdict": (
        "The hiqlite package exposes an authority-time transaction API and an "
        "AuthorityUnixMs transaction parameter that can bind the Raft-serialized "
        "authority wall-clock timestamp into Velorix standing-runtime lease SQL."
        if required_mode_supported
        else (
            "The pinned hiqlite package exposes Raft-serialized SQL writes, transactions, "
            "consistent reads, metrics, and a distributed lock, but it does not expose a "
            "public backend-authoritative wall-clock time/TTL primitive that Velorix can "
            "use for required standing-runtime fencing."
        )
    ),
}

output_path.write_text(json.dumps(assessment, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(output_path)

if update_product_evidence:
    if not product_evidence_path:
        raise SystemExit(
            "VELORIX_HIQLITE_BACKEND_TIME_UPDATE_PRODUCT_EVIDENCE=1 requires "
            "VELORIX_PRODUCT_EVIDENCE_PATH"
        )
    if not product_evidence_path.is_file():
        raise SystemExit(f"product evidence file does not exist: {product_evidence_path}")

    product = json.loads(product_evidence_path.read_text(encoding="utf-8"))
    if product.get("evidence_kind") != "velorix_product_slice_evidence":
        raise SystemExit(
            f"unexpected product evidence_kind in {product_evidence_path}: "
            f"{product.get('evidence_kind')!r}"
        )

    sibling_path = product_evidence_path.parent / "hiqlite-backend-time-assessment.json"
    if output_path.resolve() != sibling_path.resolve():
        sibling_path.write_text(
            json.dumps(assessment, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    metadata_store = product.setdefault("metadata_store", {})
    metadata_store["hiqlite_backend_time_assessment"] = {
        "validated": True,
        "evidence": "hiqlite-backend-time-assessment.json",
        "schema_version": assessment["schema_version"],
        "evidence_kind": assessment["evidence_kind"],
        "required_mode_supported": assessment["required_mode_supported"],
        "can_generate_product_complete_backend_time_attestation": assessment[
            "can_generate_product_complete_backend_time_attestation"
        ],
        "backend_time_source_kind": assessment["backend_time_source_kind"],
        "backend_time_blocked_reason": assessment["backend_time_blocked_reason"],
        "lease_authority_kind": assessment["lease_authority_kind"],
        "lease_expiry_semantics": assessment["lease_expiry_semantics"],
        "bounded_wall_clock_failover": assessment["bounded_wall_clock_failover"],
        "trusted_for_product_complete": False,
    }
    product_evidence_path.write_text(
        json.dumps(product, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(product_evidence_path)

if require_backend_time and not required_mode_supported:
    raise SystemExit(65)
PY
