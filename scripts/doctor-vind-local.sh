#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

namespace="${VELORIX_K8S_NAMESPACE:-velorix-product}"
cluster="${VELORIX_VIND_CLUSTER:-}"
output_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
output_file=""
prune_build_cache=0
fail_on_blocked=0
yes=0
local_min_free_disk_gib="${VELORIX_LOCAL_MIN_FREE_DISK_GIB:-20}"
vcluster_standalone_probe="${VELORIX_DOCTOR_VCLUSTER_STANDALONE_PROBE:-0}"

usage() {
  cat <<'EOF'
Diagnose the local Docker/vCluster environment used by scripts/run-vind-product.sh.

Default mode is read-only and writes:
  target/velorix-product/local-environment-doctor.json

Usage:
  scripts/doctor-vind-local.sh [options]

Options:
  --cluster NAME          vCluster name. Defaults to VELORIX_VIND_CLUSTER, or
                          the current vcluster-docker_* context when available.
  --namespace NAME        Kubernetes namespace. Default: velorix-product.
  --output FILE           JSON report path.
  --fail-on-blocked       Exit 75 when the doctor report status is blocked.
  --probe-vcluster-standalone
                          Run a short ghcr.io/loft-sh/vm-container probe to
                          check whether local Docker can keep vCluster
                          standalone's systemd container alive.
  --prune-build-cache     Run docker builder prune -f. Requires --yes.
  --yes                   Confirm the destructive --prune-build-cache action.
  VELORIX_LOCAL_MIN_FREE_DISK_GIB=20 sets the host disk warning/blocker floor.
  VELORIX_DOCTOR_VCLUSTER_STANDALONE_PROBE=1 enables the standalone probe.
  -h, --help              Show this help.

This script does not create PVCs and does not modify Kubernetes resources.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --cluster)
      cluster="${2:-}"
      shift 2
      ;;
    --namespace)
      namespace="${2:-}"
      shift 2
      ;;
    --output)
      output_file="${2:-}"
      shift 2
      ;;
    --prune-build-cache)
      prune_build_cache=1
      shift
      ;;
    --fail-on-blocked)
      fail_on_blocked=1
      shift
      ;;
    --probe-vcluster-standalone)
      vcluster_standalone_probe=1
      shift
      ;;
    --yes)
      yes=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

case "$namespace" in
  '' | *[!A-Za-z0-9_.-]*)
    echo "invalid namespace: ${namespace}" >&2
    exit 64
    ;;
esac

current_context="$(kubectl config current-context 2>/dev/null || true)"
if [ -z "$cluster" ]; then
  case "$current_context" in
    vcluster-docker_*) cluster="${current_context#vcluster-docker_}" ;;
    *) cluster="velorix-product" ;;
  esac
fi

case "$cluster" in
  '' | *[!A-Za-z0-9_.:-]*)
    echo "invalid vCluster name: ${cluster}" >&2
    exit 64
    ;;
esac
case "$local_min_free_disk_gib" in
  '' | *[!0-9]*)
    echo "VELORIX_LOCAL_MIN_FREE_DISK_GIB must be a non-negative integer" >&2
    exit 64
    ;;
esac

if [ "$prune_build_cache" = "1" ] && [ "$yes" != "1" ]; then
  echo "--prune-build-cache requires --yes because it deletes local Docker build cache" >&2
  exit 64
fi
case "$fail_on_blocked" in
  0 | 1) ;;
  *)
    echo "--fail-on-blocked internal value must be 0 or 1" >&2
    exit 64
    ;;
esac
case "$vcluster_standalone_probe" in
  0 | 1) ;;
  *)
    echo "VELORIX_DOCTOR_VCLUSTER_STANDALONE_PROBE must be 0 or 1" >&2
    exit 64
    ;;
esac

context="vcluster-docker_${cluster}"
mkdir -p "$output_dir"
if [ -z "$output_file" ]; then
  output_file="${output_dir}/local-environment-doctor.json"
fi

docker_df_before="${output_dir}/doctor-docker-system-df-before.txt"
docker_df_after="${output_dir}/doctor-docker-system-df-after.txt"
docker_prune_log="${output_dir}/doctor-docker-builder-prune.log"
host_df_file="${output_dir}/doctor-host-df.txt"
vcluster_list_file="${output_dir}/doctor-vcluster-list.txt"
nodes_json="${output_dir}/doctor-k8s-nodes.json"
pods_json="${output_dir}/doctor-k8s-pods.json"
events_file="${output_dir}/doctor-k8s-events.txt"
vcluster_standalone_probe_log="${output_dir}/doctor-vcluster-standalone-probe.log"
vcluster_standalone_probe_inspect="${output_dir}/doctor-vcluster-standalone-probe-inspect.json"

df -Pk "$repo_root" >"$host_df_file" 2>&1 || true
docker system df >"$docker_df_before" 2>&1 || true
vcluster list --driver docker >"$vcluster_list_file" 2>&1 || true

destructive_actions_json="[]"
if [ "$prune_build_cache" = "1" ]; then
  docker builder prune -f >"$docker_prune_log" 2>&1
  destructive_actions_json='["docker builder prune -f"]'
else
  rm -f "$docker_prune_log"
fi
docker system df >"$docker_df_after" 2>&1 || true

rm -f "$vcluster_standalone_probe_log" "$vcluster_standalone_probe_inspect"
if [ "$vcluster_standalone_probe" = "1" ]; then
  probe_name="velorix-vcluster-standalone-probe-$(date -u +%Y%m%dT%H%M%SZ)-$$"
  probe_start_log="${output_dir}/doctor-vcluster-standalone-probe-start.log"
  rm -f "$probe_start_log"
  if docker run -d \
    --name "$probe_name" \
    --privileged \
    --tmpfs /run \
    --tmpfs /tmp \
    --mount type=bind,src=/run/containerd/containerd.sock,dst=/var/run/docker/containerd/containerd.sock,ro \
    ghcr.io/loft-sh/vm-container >"${output_dir}/doctor-vcluster-standalone-probe-container-id.txt" 2>"$probe_start_log"; then
    sleep 3
    docker inspect "$probe_name" >"$vcluster_standalone_probe_inspect" 2>>"$probe_start_log" || true
    docker logs "$probe_name" >"$vcluster_standalone_probe_log" 2>&1 || true
    docker rm -f "$probe_name" >/dev/null 2>&1 || true
  else
    cp "$probe_start_log" "$vcluster_standalone_probe_log"
    printf '[]\n' >"$vcluster_standalone_probe_inspect"
  fi
  rm -f "$probe_start_log"
fi

if kubectl --context "$context" get nodes -o json >"$nodes_json" 2>/dev/null; then
  if kubectl --context "$context" get namespace "$namespace" >/dev/null 2>&1; then
    kubectl --context "$context" -n "$namespace" get pods -o json >"$pods_json" 2>/dev/null || rm -f "$pods_json"
  else
    rm -f "$pods_json"
  fi
  kubectl --context "$context" get events -A --sort-by=.lastTimestamp >"$events_file" 2>&1 || true
else
  rm -f "$nodes_json" "$pods_json"
  kubectl config get-contexts >"$events_file" 2>&1 || true
fi

python3 - \
  "$output_file" \
  "$cluster" \
  "$context" \
  "$namespace" \
  "$docker_df_before" \
  "$docker_df_after" \
  "$docker_prune_log" \
  "$host_df_file" \
  "$vcluster_list_file" \
  "$nodes_json" \
  "$pods_json" \
  "$events_file" \
  "$vcluster_standalone_probe" \
  "$vcluster_standalone_probe_log" \
  "$vcluster_standalone_probe_inspect" \
  "$local_min_free_disk_gib" \
  "$destructive_actions_json" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone

(
    output_file,
    cluster,
    context,
    namespace,
    docker_df_before,
    docker_df_after,
    docker_prune_log,
    host_df_file,
    vcluster_list_file,
    nodes_json,
    pods_json,
    events_file,
    vcluster_standalone_probe,
    vcluster_standalone_probe_log,
    vcluster_standalone_probe_inspect,
    local_min_free_disk_gib,
    destructive_actions_raw,
) = sys.argv[1:]

def read_text(path):
    if not path or not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()

def load_json(path):
    if not path or not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def parse_size_to_gib(value):
    if not value or value == "0B":
        return 0.0
    value = value.strip()
    number = ""
    unit = ""
    for char in value:
        if char.isdigit() or char == ".":
            number += char
        else:
            unit += char
    if not number:
        return 0.0
    multiplier = {
        "B": 1 / 1024 / 1024 / 1024,
        "kB": 1 / 1024 / 1024,
        "KB": 1 / 1024 / 1024,
        "MB": 1 / 1024,
        "MiB": 1 / 1024,
        "GB": 1,
        "GiB": 1,
        "TB": 1024,
        "TiB": 1024,
    }.get(unit, 0)
    return float(number) * multiplier

def parse_docker_system_df(text):
    summary = {
        "build_cache_reclaimable_gib": None,
        "images_reclaimable_gib": None,
        "local_volumes_reclaimable_gib": None,
    }
    if not text:
        return summary
    for line in text.splitlines():
        parts = line.split()
        if not parts:
            continue
        if parts[0] == "Build" and len(parts) >= 6:
            summary["build_cache_reclaimable_gib"] = round(parse_size_to_gib(parts[5]), 2)
        elif parts[0] == "Images" and len(parts) >= 5:
            summary["images_reclaimable_gib"] = round(parse_size_to_gib(parts[4]), 2)
        elif parts[0] == "Local" and len(parts) >= 6:
            summary["local_volumes_reclaimable_gib"] = round(parse_size_to_gib(parts[5]), 2)
    return summary

blockers = []
warnings = []
nodes = load_json(nodes_json)
pods = load_json(pods_json)
host_df_text = read_text(host_df_file)
docker_df_before_text = read_text(docker_df_before)
docker_df_after_text = read_text(docker_df_after)
docker_capacity = parse_docker_system_df(docker_df_after_text or docker_df_before_text)
vcluster_standalone_probe_enabled = vcluster_standalone_probe == "1"
vcluster_standalone_probe_result = {
    "enabled": vcluster_standalone_probe_enabled,
    "status": "not_run",
    "image": "ghcr.io/loft-sh/vm-container",
    "containerd_socket_mount": "/run/containerd/containerd.sock:/var/run/docker/containerd/containerd.sock:ro",
    "running_after_seconds": 3,
    "exit_code": None,
    "oom_killed": None,
    "log_excerpt": None,
}
if vcluster_standalone_probe_enabled:
    probe_inspect = load_json(vcluster_standalone_probe_inspect)
    probe_log = read_text(vcluster_standalone_probe_log) or ""
    vcluster_standalone_probe_result["log_excerpt"] = "\n".join(probe_log.splitlines()[-20:]) or None
    state = None
    if isinstance(probe_inspect, list) and probe_inspect:
        state = (probe_inspect[0] or {}).get("State") or {}
    if not state:
        vcluster_standalone_probe_result["status"] = "blocked"
        blockers.append({
            "kind": "vcluster_vm_container_probe_start_failed",
            "subject": "ghcr.io/loft-sh/vm-container",
            "message": "could not start or inspect the vCluster standalone compatibility probe",
        })
    elif state.get("Running") is True:
        vcluster_standalone_probe_result["status"] = "pass"
    else:
        exit_code = state.get("ExitCode")
        oom_killed = state.get("OOMKilled")
        vcluster_standalone_probe_result["exit_code"] = exit_code
        vcluster_standalone_probe_result["oom_killed"] = oom_killed
        if exit_code == 0:
            vcluster_standalone_probe_result["status"] = "pass"
        else:
            vcluster_standalone_probe_result["status"] = "blocked"
            blockers.append({
                "kind": "vcluster_vm_container_systemd_exit",
                "subject": "ghcr.io/loft-sh/vm-container",
                "message": (
                    "vCluster standalone compatibility probe exited before staying ready "
                    f"(exit_code={exit_code}, oom_killed={oom_killed})"
                ),
            })
required_free_gib = int(local_min_free_disk_gib)
host_capacity = {"required_free_gib": required_free_gib}
if host_df_text:
    lines = [line for line in host_df_text.splitlines() if line.strip()]
    if len(lines) >= 2:
        try:
            available_kib = int(lines[1].split()[3])
            required_kib = required_free_gib * 1024 * 1024
            host_capacity.update({
                "available_kib": available_kib,
                "available_free_gib": round(available_kib / 1024 / 1024, 2),
                "required_kib": required_kib,
            })
            if available_kib < required_kib:
                blockers.append({
                    "kind": "local_host_disk_capacity",
                    "subject": os.getcwd(),
                    "message": (
                        f"available={available_kib / 1024 / 1024:.2f}GiB "
                        f"required={required_free_gib}GiB"
                    ),
                })
        except (IndexError, ValueError):
            warnings.append(f"could not parse host df output from {host_df_file}")

available_free_gib = host_capacity.get("available_free_gib")
build_cache_reclaimable_gib = docker_capacity.get("build_cache_reclaimable_gib") or 0.0
estimated_after_build_cache_prune_gib = None
if isinstance(available_free_gib, (int, float)):
    estimated_after_build_cache_prune_gib = round(
        available_free_gib + build_cache_reclaimable_gib, 2
    )
    host_capacity["estimated_after_build_cache_prune_gib"] = estimated_after_build_cache_prune_gib
    host_capacity["shortfall_gib"] = round(
        max(0.0, required_free_gib - available_free_gib), 2
    )

pressure_conditions = {"DiskPressure", "MemoryPressure", "PIDPressure"}
blocking_taints = {
    "node.kubernetes.io/disk-pressure",
    "node.kubernetes.io/memory-pressure",
    "node.kubernetes.io/pid-pressure",
    "node.kubernetes.io/not-ready",
    "node.kubernetes.io/unreachable",
}
if nodes is None:
    warnings.append(f"Kubernetes context {context} is not reachable or has no nodes JSON")
else:
    for node in nodes.get("items") or []:
        metadata = node.get("metadata") or {}
        status = node.get("status") or {}
        spec = node.get("spec") or {}
        name = metadata.get("name", "<unnamed>")
        if spec.get("unschedulable") is True:
            blockers.append({
                "kind": "node_unschedulable",
                "subject": name,
                "message": "node is marked unschedulable",
            })
        for condition in status.get("conditions") or []:
            ctype = condition.get("type")
            cstatus = condition.get("status")
            if ctype == "Ready" and cstatus != "True":
                blockers.append({
                    "kind": "node_not_ready",
                    "subject": name,
                    "message": f"Ready={cstatus} reason={condition.get('reason')} message={condition.get('message')}",
                })
            elif ctype in pressure_conditions and cstatus == "True":
                blockers.append({
                    "kind": "node_pressure",
                    "subject": name,
                    "message": f"{ctype}=True reason={condition.get('reason')} message={condition.get('message')}",
                })
        for taint in spec.get("taints") or []:
            if taint.get("effect") in {"NoSchedule", "NoExecute"} and taint.get("key") in blocking_taints:
                blockers.append({
                    "kind": "blocking_taint",
                    "subject": name,
                    "message": f"{taint.get('key')}:{taint.get('effect')}",
                })

if pods is not None:
    markers = [
        "disk-pressure",
        "noschedule",
        "insufficient ephemeral-storage",
        "had taint",
        "evicted",
        "unschedulable",
    ]
    for pod in pods.get("items") or []:
        metadata = pod.get("metadata") or {}
        status = pod.get("status") or {}
        name = metadata.get("name", "<unnamed>")
        if status.get("phase") == "Failed" and status.get("reason") == "Evicted":
            blockers.append({
                "kind": "pod_evicted",
                "subject": name,
                "message": status.get("message"),
            })
        for condition in status.get("conditions") or []:
            if condition.get("type") != "PodScheduled" or condition.get("status") != "False":
                continue
            text = " ".join(str(part or "") for part in [condition.get("reason"), condition.get("message")])
            lowered = text.lower()
            if condition.get("reason") == "Unschedulable" or any(marker in lowered for marker in markers):
                blockers.append({
                    "kind": "pod_unschedulable",
                    "subject": name,
                    "message": text,
                })

try:
    destructive_actions = json.loads(destructive_actions_raw)
except json.JSONDecodeError:
    destructive_actions = []

remediation_commands = [
    {
        "description": "Re-run the read-only local environment doctor",
        "command": "scripts/doctor-vind-local.sh",
        "destructive": False,
    },
    {
        "description": "Delete local Docker build cache after review",
        "command": "scripts/doctor-vind-local.sh --prune-build-cache --yes",
        "destructive": True,
        "expected_free_gib": build_cache_reclaimable_gib,
    },
]
if any(blocker.get("kind") in {"node_pressure", "blocking_taint", "pod_evicted", "pod_unschedulable"} for blocker in blockers):
    remediation_commands.append({
        "description": "Recreate the local vCluster after freeing capacity if pressure taints remain",
        "command": f"VELORIX_VIND_CLUSTER={cluster} VELORIX_VIND_CLEANUP=1 scripts/run-vind-product.sh",
        "destructive": True,
    })
if any(blocker.get("kind", "").startswith("vcluster_vm_container_") for blocker in blockers):
    remediation_commands.append({
        "description": "Re-run the vCluster standalone compatibility probe",
        "command": "VELORIX_DOCTOR_VCLUSTER_STANDALONE_PROBE=1 scripts/doctor-vind-local.sh",
        "destructive": False,
    })
    remediation_commands.append({
        "description": "Verify the raw vm-container bootstrap outside the product runner",
        "command": "docker run --rm --privileged --tmpfs /run --tmpfs /tmp --mount type=bind,src=/run/containerd/containerd.sock,dst=/var/run/docker/containerd/containerd.sock,ro ghcr.io/loft-sh/vm-container",
        "destructive": False,
    })

report = {
    "schema_version": 1,
    "evidence_kind": "velorix_local_environment_doctor",
    "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "cluster": cluster,
    "context": context,
    "namespace": namespace,
    "status": "blocked" if blockers else "pass",
    "trusted_for_product_complete": False,
    "no_pvc_workaround_allowed": True,
    "destructive_actions_run": destructive_actions,
    "blockers": blockers,
    "warnings": warnings,
    "capacity": {
        "host": host_capacity,
        "docker": docker_capacity,
    },
    "vcluster_standalone_probe": vcluster_standalone_probe_result,
    "remediation_commands": remediation_commands,
    "evidence_files": {
        "docker_system_df_before": docker_df_before,
        "docker_system_df_after": docker_df_after,
        "docker_builder_prune_log": docker_prune_log if os.path.exists(docker_prune_log) else None,
        "host_df": host_df_file,
        "vcluster_list": vcluster_list_file,
        "k8s_nodes": nodes_json if os.path.exists(nodes_json) else None,
        "k8s_pods": pods_json if os.path.exists(pods_json) else None,
        "k8s_events": events_file,
        "vcluster_standalone_probe_log": vcluster_standalone_probe_log
        if os.path.exists(vcluster_standalone_probe_log)
        else None,
        "vcluster_standalone_probe_inspect": vcluster_standalone_probe_inspect
        if os.path.exists(vcluster_standalone_probe_inspect)
        else None,
    },
    "docker_system_df_before": docker_df_before_text,
    "docker_system_df_after": docker_df_after_text,
    "host_df": host_df_text,
    "remediation": [
        "Run scripts/doctor-vind-local.sh again after freeing local capacity.",
        "If you choose to delete local Docker build cache, run scripts/doctor-vind-local.sh --prune-build-cache --yes.",
        "If the reused vCluster remains tainted after capacity is available, recreate the local vCluster.",
        "If the vCluster standalone compatibility probe is blocked, fix or replace the local Docker runtime before rerunning the product slice.",
        "Do not add PVCs to bypass this no-PVC product path.",
    ],
}

with open(output_file, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2, sort_keys=True)
    f.write("\n")

print(f"wrote {output_file}")
print(f"status={report['status']}")
if blockers:
    for blocker in blockers[:12]:
        print(f"- {blocker['kind']}: {blocker['subject']}: {blocker.get('message')}")
    if len(blockers) > 12:
        print(f"- ... {len(blockers) - 12} more")
PY

if [ "$fail_on_blocked" = "1" ]; then
  VELORIX_DOCTOR_FAIL_ON_BLOCKED=1 python3 - "$output_file" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    report = json.load(f)
if report.get("status") == "blocked":
    raise SystemExit(75)
PY
fi
