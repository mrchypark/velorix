#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(git rev-parse --show-toplevel)"
product_dir="${VELORIX_VIND_PRODUCT_DIR:-target/velorix-product}"
product_evidence="${VELORIX_VIND_PRODUCT_EVIDENCE:-${product_dir}/product-evidence.json}"
output_evidence="${VELORIX_VIND_PRODUCT_EVIDENCE_OUT:-${product_evidence}}"

usage() {
  cat <<'EOF'
Refresh deployed image digest evidence for an existing vind product slice.

Usage:
  scripts/refresh-vind-product-deployed-images.sh

Main environment overrides:
  VELORIX_VIND_PRODUCT_DIR=target/velorix-product
  VELORIX_VIND_PRODUCT_EVIDENCE=target/velorix-product/product-evidence.json
  VELORIX_VIND_PRODUCT_EVIDENCE_OUT=target/velorix-product/product-evidence.json

The script reads product-evidence.json for context and namespace, collects
velorix-api/velorix-meta Deployment and Pod evidence with kubectl, and updates
the product evidence only when the Deployment image-digest annotation and the
current Ready Pod imageID digests agree. If the current Ready Pods all agree
but the annotation is stale, it synchronizes the Deployment template annotation
to the observed digest, waits for rollout, and then records the refreshed
evidence. It does not create PVCs or change container images.
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

if [ "$#" -ne 0 ]; then
  usage >&2
  exit 64
fi

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 127
  fi
}

cd "$repo_root"
require kubectl
require python3

if [ ! -f "$product_evidence" ]; then
  echo "missing product evidence: ${product_evidence}" >&2
  exit 66
fi

mkdir -p "$product_dir"

IFS=$'\t' read -r context namespace meta_enabled < <(
  python3 - "$product_evidence" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    product = json.load(f)
context = product.get("context")
namespace = product.get("namespace")
meta_enabled = bool((product.get("metadata_store") or {}).get("enabled"))
if not context:
    raise SystemExit("product evidence is missing context")
if not namespace:
    raise SystemExit("product evidence is missing namespace")
print(f"{context}\t{namespace}\t{1 if meta_enabled else 0}")
PY
)

kubectl --context "$context" -n "$namespace" get deployment velorix-api \
  -o json >"${product_dir}/velorix-api-deployment-observed.json"
kubectl --context "$context" -n "$namespace" get pods -l app=velorix-api \
  -o json >"${product_dir}/velorix-api-pods.json"

if [ "$meta_enabled" = "1" ]; then
  kubectl --context "$context" -n "$namespace" get deployment velorix-meta \
    -o json >"${product_dir}/velorix-meta-deployment-observed.json"
  kubectl --context "$context" -n "$namespace" get pods -l app=velorix-meta \
    -o json >"${product_dir}/velorix-meta-pods.json"
fi

role_digest_report() {
  local deployment_file="$1"
  local pods_file="$2"
  local container_name="$3"
  python3 - "$deployment_file" "$pods_file" "$container_name" <<'PY'
import json
import re
import sys

deployment_file, pods_file, container_name = sys.argv[1:]
sha256_pattern = re.compile(r"sha256:[0-9a-fA-F]{64}")

with open(deployment_file, "r", encoding="utf-8") as f:
    deployment = json.load(f)
with open(pods_file, "r", encoding="utf-8") as f:
    pods = json.load(f)

template = deployment.get("spec", {}).get("template", {})
template_labels = template.get("metadata", {}).get("labels", {}) or {}
annotations = template.get("metadata", {}).get("annotations", {}) or {}
annotation_digest = annotations.get("velorix.dev/image-digest")
if annotation_digest == "unknown":
    annotation_digest = ""
match = sha256_pattern.search(annotation_digest or "")
annotation_digest = match.group(0).lower() if match else ""

digests = set()
matched_pods = []
for pod in pods.get("items") or []:
    metadata = pod.get("metadata") or {}
    if metadata.get("deletionTimestamp"):
        continue
    labels = metadata.get("labels") or {}
    if any(labels.get(key) != value for key, value in template_labels.items()):
        continue
    status = pod.get("status") or {}
    conditions = {
        condition.get("type"): condition.get("status")
        for condition in status.get("conditions") or []
    }
    if status.get("phase") != "Running" or conditions.get("Ready") != "True":
        continue
    for container_status in status.get("containerStatuses") or []:
        if container_status.get("name") != container_name:
            continue
        image_id = container_status.get("imageID") or ""
        digest_match = sha256_pattern.search(image_id)
        if not digest_match:
            raise SystemExit(
                f"pod {metadata.get('name')} {container_name} imageID lacks sha256 digest"
            )
        digests.add(digest_match.group(0).lower())
        matched_pods.append(metadata.get("name") or "")

if not matched_pods:
    raise SystemExit(f"pods evidence has no current Ready pod for {container_name}")
if len(digests) != 1:
    raise SystemExit(f"current Ready pods for {container_name} use multiple image digests: {sorted(digests)}")
print(f"{annotation_digest}\t{next(iter(digests))}\t{','.join(sorted(matched_pods))}")
PY
}

sync_role_annotation() {
  local deployment="$1"
  local container_name="$2"
  local deployment_file="$3"
  local pods_file="$4"
  local annotation_digest
  local observed_digest
  local matched_pods
  local patch_json

  IFS=$'\t' read -r annotation_digest observed_digest matched_pods < <(
    role_digest_report "$deployment_file" "$pods_file" "$container_name"
  )
  if [ "$annotation_digest" = "$observed_digest" ]; then
    return 0
  fi

  echo "syncing ${deployment} image digest annotation from ${annotation_digest:-missing} to observed Ready Pod digest ${observed_digest} (${matched_pods})" >&2
  patch_json="$(
    python3 - "$observed_digest" <<'PY'
import json
import sys

print(json.dumps({
    "spec": {
        "template": {
            "metadata": {
                "annotations": {
                    "velorix.dev/image-digest": sys.argv[1],
                    "velorix.dev/image-digest-source": "observed-pod-imageid-after-rollout",
                }
            }
        }
    }
}))
PY
  )"
  kubectl --context "$context" -n "$namespace" patch deployment "$deployment" \
    --type merge -p "$patch_json" >/dev/null
  kubectl --context "$context" -n "$namespace" rollout status "deployment/${deployment}" --timeout=300s
  kubectl --context "$context" -n "$namespace" get deployment "$deployment" \
    -o json >"$deployment_file"
  kubectl --context "$context" -n "$namespace" get pods -l "app=${deployment}" \
    -o json >"$pods_file"

  IFS=$'\t' read -r annotation_digest observed_digest matched_pods < <(
    role_digest_report "$deployment_file" "$pods_file" "$container_name"
  )
  if [ "$annotation_digest" != "$observed_digest" ]; then
    echo "${deployment} annotation still does not match observed pod digest after rollout: ${annotation_digest} != ${observed_digest}" >&2
    exit 1
  fi
}

sync_role_annotation \
  velorix-api \
  api \
  "${product_dir}/velorix-api-deployment-observed.json" \
  "${product_dir}/velorix-api-pods.json"
if [ "$meta_enabled" = "1" ]; then
  sync_role_annotation \
    velorix-meta \
    meta \
    "${product_dir}/velorix-meta-deployment-observed.json" \
    "${product_dir}/velorix-meta-pods.json"
fi

tmp_output="${output_evidence}.tmp.$$"
python3 - \
  "$product_evidence" \
  "$tmp_output" \
  "$product_dir" \
  "$meta_enabled" <<'PY'
import json
import re
import sys
from pathlib import Path

product_path, output_path, product_dir, meta_enabled = sys.argv[1:]
product_dir = Path(product_dir)
sha256_pattern = re.compile(r"sha256:[0-9a-fA-F]{64}")


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def sha256_digest_in_text(value):
    if not isinstance(value, str):
        return None
    match = sha256_pattern.search(value)
    return match.group(0).lower() if match else None


def find_container(containers, name):
    for container in containers or []:
        if container.get("name") == name:
            return container
    return None


def deployment_role(role, deployment_name, container_name):
    deployment_path = product_dir / f"{deployment_name}-deployment-observed.json"
    pods_path = product_dir / f"{deployment_name}-pods.json"
    deployment = load_json(deployment_path)
    pods = load_json(pods_path)
    if deployment.get("kind") != "Deployment":
        raise SystemExit(f"{deployment_path} kind must be Deployment")
    if deployment.get("metadata", {}).get("name") != deployment_name:
        raise SystemExit(f"{deployment_path} deployment name mismatch")
    template = deployment.get("spec", {}).get("template", {})
    template_labels = template.get("metadata", {}).get("labels", {}) or {}
    annotations = template.get("metadata", {}).get("annotations", {}) or {}
    image_digest = annotations.get("velorix.dev/image-digest")
    if image_digest == "unknown":
        image_digest = None
    if not sha256_digest_in_text(image_digest):
        raise SystemExit(
            f"{deployment_path} is missing velorix.dev/image-digest sha256 annotation; "
            "rerun scripts/run-vind-product.sh with a local image digest available or set "
            "VELORIX_API_IMAGE_DIGEST/VELORIX_META_IMAGE_DIGEST"
        )
    image_digest = sha256_digest_in_text(image_digest)
    container = find_container(
        template.get("spec", {}).get("containers", []),
        container_name,
    )
    if container is None:
        raise SystemExit(f"{deployment_path} missing container {container_name}")
    image = container.get("image")
    if not image:
        raise SystemExit(f"{deployment_path} container {container_name} is missing image")
    items = pods.get("items")
    if not isinstance(items, list) or not items:
        raise SystemExit(f"{pods_path} pods evidence is empty")
    matched = 0
    for pod in items:
        metadata = pod.get("metadata", {}) or {}
        if metadata.get("deletionTimestamp"):
            continue
        labels = metadata.get("labels") or {}
        if any(labels.get(key) != value for key, value in template_labels.items()):
            continue
        status_body = pod.get("status", {}) or {}
        conditions = {
            condition.get("type"): condition.get("status")
            for condition in status_body.get("conditions") or []
        }
        if status_body.get("phase") != "Running" or conditions.get("Ready") != "True":
            continue
        status = find_container(
            status_body.get("containerStatuses", []),
            container_name,
        )
        if status is None:
            continue
        observed_digest = sha256_digest_in_text(status.get("imageID"))
        if observed_digest != image_digest:
            raise SystemExit(
                f"{pods_path} pod {metadata.get('name')} imageID digest "
                f"{observed_digest} does not match deployment annotation {image_digest}"
            )
        matched += 1
    if matched == 0:
        raise SystemExit(f"{pods_path} has no current Ready container status for {container_name}")
    return {
        "image": image,
        "image_digest": image_digest,
        "evidence_files": {
            "manifest": f"{deployment_name}.yaml",
            "deployment": f"{deployment_name}-deployment-observed.json",
            "pods": f"{deployment_name}-pods.json",
        },
    }


product = load_json(product_path)
deployed_images = {
    "velorix-api": deployment_role("velorix-api", "velorix-api", "api"),
}
if meta_enabled == "1":
    deployed_images["velorix-meta"] = deployment_role(
        "velorix-meta",
        "velorix-meta",
        "meta",
    )
product["deployed_images"] = deployed_images

blockers = product.get("product_complete_blockers")
if isinstance(blockers, list):
    blockers = [
        blocker
        for blocker in blockers
        if blocker
        not in {
            "velorix-api deployed image digest was not recorded",
            "velorix-meta deployed image digest was not recorded",
        }
    ]
    product["product_complete_blockers"] = blockers
    product["product_complete"] = (
        product.get("product_complete") is True and len(blockers) == 0
    )

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(product, f, indent=2, sort_keys=True)
    f.write("\n")

print("refreshed deployed image evidence")
for role, info in deployed_images.items():
    print(f"{role}={info['image_digest']}")
PY

mv "$tmp_output" "$output_evidence"
chmod 600 "$output_evidence"

echo "product_evidence=${output_evidence}"
