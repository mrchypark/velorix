#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="v26.34.0"
image="${MATERIALIZE_IMAGE:-materialize/materialized:$version}"
image_digest="${MATERIALIZE_IMAGE_DIGEST:-sha256:8cbcbb6446d5050142dbb8c738d367f2e0361b8e980fb4a88b073a35ed7664f6}"
output_file="${1:-$repo_root/baselines/incremental-sql/materialize-emulator-v26.34.0.json}"

case "$(uname -m)" in
  arm64|aarch64)
    runtime_platform="linux/arm64"
    image_platform_digest="sha256:b4bc651548dac1f2094e7e3d9757dab8b559fb77bf9f3df2608ac8ead27732ac"
    ;;
  x86_64|amd64)
    runtime_platform="linux/amd64"
    image_platform_digest="sha256:1b6e77a9ae8b437cdb83b2256f023b299b06fc20f3c8a23b2283bd3eec3f96b6"
    ;;
  *)
    echo "unsupported Materialize baseline architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

for dependency in docker duckdb psql python3; do
  command -v "$dependency" >/dev/null || {
    echo "required dependency is missing: $dependency" >&2
    exit 1
  }
done

observed_digest="$(docker buildx imagetools inspect "$image" | awk '$1 == "Digest:" {print $2; exit}')"
[[ "$observed_digest" == "$image_digest" ]] || {
  echo "Materialize Emulator image digest mismatch" >&2
  exit 1
}
observed_platform_digest="$(docker buildx imagetools inspect --raw "$image" | python3 -c '
import json
import sys

platform = sys.argv[1].split("/", 1)
manifest = json.load(sys.stdin)
for item in manifest["manifests"]:
    current = item["platform"]
    if current["os"] == platform[0] and current["architecture"] == platform[1]:
        print(item["digest"])
        break
' "$runtime_platform")"
[[ "$observed_platform_digest" == "$image_platform_digest" ]] || {
  echo "Materialize Emulator platform image digest mismatch" >&2
  exit 1
}
docker pull --platform "$runtime_platform" "$image" >/dev/null

exec python3 "$repo_root/scripts/incremental_sql_materialize.py" \
  --image "$image" \
  --image-digest "$image_digest" \
  --image-platform-digest "$image_platform_digest" \
  --runtime-platform "$runtime_platform" \
  --corpus "$repo_root/crates/velorix-runtime/benches/fixtures/incremental_sql_corpus_v1.json" \
  --output "$output_file"
