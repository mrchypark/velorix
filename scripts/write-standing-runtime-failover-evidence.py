#!/usr/bin/env python3
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        value = json.load(f)
    if not isinstance(value, dict):
        raise SystemExit(f"{path} must contain a JSON object")
    return value


def owner_epoch(report: dict, view_id: str):
    for owner in report.get("owners") or []:
        if owner.get("view_id") == view_id:
            current = owner.get("current_owner") or {}
            value = current.get("owner_epoch")
            if isinstance(value, int):
                return value
    epochs = []
    for owner in report.get("owners") or []:
        current = owner.get("current_owner") or {}
        value = current.get("owner_epoch")
        if isinstance(value, int):
            epochs.append(value)
    return max(epochs) if epochs else None


def pod_names(report: dict) -> set[str]:
    names = set()
    for item in report.get("items") or []:
        if not isinstance(item, dict):
            continue
        metadata = item.get("metadata") or {}
        name = metadata.get("name")
        if isinstance(name, str) and name:
            names.add(name)
    return names


def validate_owner_shape(report: dict, label: str) -> None:
    owners = report.get("owners") or []
    for index, owner in enumerate(owners):
        current = owner.get("current_owner")
        if not isinstance(current, dict):
            raise SystemExit(f"{label} owner {index} missing current_owner object")
        if not isinstance(current.get("owner_epoch"), int):
            raise SystemExit(f"{label} owner {index} missing integer current_owner.owner_epoch")


def main() -> int:
    if len(sys.argv) != 21:
        raise SystemExit(
            "usage: write-standing-runtime-failover-evidence.py "
            "OUTPUT PRODUCT RUN_ID CONTEXT NAMESPACE CLUSTER INITIAL_TARGET ATTACH PRE_OWNER "
            "POST_OWNER INGEST QUERY PODS_BEFORE PODS_AFTER START_MS END_MS USER_ID "
            "EXPECTED_SUM EXPECTED_COUNT RELEASE_ATTEST"
        )
    (
        evidence_path,
        product_path,
        run_id,
        context,
        namespace,
        cluster,
        initial_target,
        attach_evidence_path,
        pre_owner_path,
        post_owner_path,
        ingest_path,
        query_path,
        pods_before_path,
        pods_after_path,
        start_ms,
        end_ms,
        user_id,
        expected_sum_raw,
        expected_count_raw,
        release_attest,
    ) = sys.argv[1:]

    attach = load_json(attach_evidence_path)
    pre_owner = load_json(pre_owner_path)
    post_owner = load_json(post_owner_path)
    ingest = load_json(ingest_path)
    query = load_json(query_path)
    pods_before = load_json(pods_before_path)
    pods_after = load_json(pods_after_path)
    product = load_json(product_path)

    expected_sum = int(expected_sum_raw)
    expected_count = int(expected_count_raw)
    post_rows = query.get("rows") or []
    matched = [row for row in post_rows if row.get("user_id") == user_id]
    expected = [{"user_id": user_id, "sum": expected_sum, "count": expected_count}]
    if not pre_owner.get("owners"):
        raise SystemExit("pre-failover owner report had no owners")
    if not post_owner.get("owners"):
        raise SystemExit("post-failover owner report had no owners")
    validate_owner_shape(pre_owner, "pre-failover")
    validate_owner_shape(post_owner, "post-failover")
    if not all(
        owner.get("current_owner_matches_local_process") is True
        for owner in post_owner["owners"]
    ):
        raise SystemExit(f"post-failover owner report does not match local process: {post_owner}")
    if attach.get("writer_owner_attach_status") != "selected":
        raise SystemExit(f"post-failover attach did not select writer owner: {attach}")
    if attach.get("port_forward_target") == initial_target:
        raise SystemExit(f"post-failover target did not change from deleted pod: {attach}")
    initial_pod = initial_target.removeprefix("pod/")
    post_target = attach.get("port_forward_target") or ""
    post_pod = post_target.removeprefix("pod/") if post_target.startswith("pod/") else ""
    if initial_pod not in pod_names(pods_before):
        raise SystemExit(f"initial target pod missing from pre-failover pod list: {initial_pod}")
    if not post_pod or post_pod not in pod_names(pods_after):
        raise SystemExit(f"post-failover target pod missing from post-failover pod list: {post_target}")
    if ingest.get("outcome") != "appended":
        raise SystemExit(f"post-failover ingest did not append: {ingest}")
    if matched != expected:
        raise SystemExit(f"post-failover promoted API query missing expected row: {matched}")

    start = int(start_ms)
    end = int(end_ms)
    observed_failover_ms = max(0, end - start)
    release_attest_enabled = release_attest == "1"
    capability = product.get("standing_runtime_fencing", {}).get("capability") or {}
    backend_time_source_kind = capability.get("backend_time_source_kind")
    authority_time_observed = (
        capability.get("authoritative_backend_time") is True
        and backend_time_source_kind == "raft_replicated_authority_time"
    )
    owner_ttl_ms = capability.get("max_owner_ttl_ms")
    failover_time_bound_ms = capability.get("failover_time_bound_ms")
    pre_failover_owner_epoch = owner_epoch(pre_owner, "positive_scores_by_user")
    post_failover_owner_epoch = owner_epoch(post_owner, "positive_scores_by_user")
    affected_api_pods = [initial_pod, post_pod]

    if release_attest_enabled:
        errors = []
        if not authority_time_observed:
            errors.append("release failover attestation requires raft_replicated_authority_time capability")
        if not isinstance(owner_ttl_ms, int) or owner_ttl_ms <= 0:
            errors.append("release failover attestation requires capability.max_owner_ttl_ms")
        if not isinstance(failover_time_bound_ms, int) or failover_time_bound_ms <= 0:
            errors.append("release failover attestation requires capability.failover_time_bound_ms")
        if (
            not isinstance(pre_failover_owner_epoch, int)
            or not isinstance(post_failover_owner_epoch, int)
            or post_failover_owner_epoch <= pre_failover_owner_epoch
        ):
            errors.append("release failover attestation requires owner epoch advance")
        if observed_failover_ms > failover_time_bound_ms:
            errors.append("release failover attestation observed_failover_ms exceeded capability.failover_time_bound_ms")
        if errors:
            raise SystemExit("invalid release failover attestation:\n- " + "\n- ".join(errors))

    payload = {
        "schema_version": 1,
        "evidence_kind": "velorix_standing_runtime_failover_smoke",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "status": "pass",
        "run_id": run_id,
        "context": context,
        "namespace": namespace,
        "cluster": cluster,
        "initial_port_forward_target": initial_target,
        "post_failover_port_forward_target": attach.get("port_forward_target"),
        "writer_owner_attach_status": attach.get("writer_owner_attach_status"),
        "observed_failover_ms": observed_failover_ms,
        "deleted_owner_pod": initial_target.removeprefix("pod/"),
        "post_failover_owner_count": len(post_owner["owners"]),
        "post_failover_owners_match_local_process": True,
        "post_failover_ingest_outcome": ingest.get("outcome"),
        "post_failover_query_row": expected[0],
        "scope": "local vind product API pod deletion and owner reacquire smoke",
        "trusted_for_product_complete": release_attest_enabled,
        "production_wall_clock_failover_attestation": release_attest_enabled,
        "evidence_files": {
            "pre_owner_report": pre_owner_path,
            "post_owner_report": post_owner_path,
            "post_failover_attach": attach_evidence_path,
            "post_failover_ingest": ingest_path,
            "post_failover_query": query_path,
            "pods_before": pods_before_path,
            "pods_after": pods_after_path,
        },
    }
    if release_attest_enabled:
        payload.update(
            {
                "scope": "release CI deployed product bounded wall-clock failover",
                "evidence_scope": "release_ci_deployed_product",
                "failover_probe_kind": "release_bounded_wall_clock_failover",
                "backend_time_source_kind": backend_time_source_kind,
                "authority_time_observed": authority_time_observed,
                "owner_ttl_ms": owner_ttl_ms,
                "failover_time_bound_ms": failover_time_bound_ms,
                "pre_failover_owner_epoch": pre_failover_owner_epoch,
                "post_failover_owner_epoch": post_failover_owner_epoch,
                "affected_api_pods": affected_api_pods,
            }
        )

    Path(evidence_path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "status": "pass",
                "observed_failover_ms": payload["observed_failover_ms"],
                "initial_target": initial_target,
                "post_failover_target": attach.get("port_forward_target"),
                "post_failover_row": expected[0],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
