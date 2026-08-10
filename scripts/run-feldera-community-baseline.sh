#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="0.330.0"
image="${FELDERA_IMAGE:-images.feldera.com/feldera/pipeline-manager:$version}"
image_digest="${FELDERA_IMAGE_DIGEST:-sha256:3163343ecb55dafb1d61e9f3dfca18dcc6479601a649b5c02b55e483b1437350}"
output_file="${1:-$repo_root/baselines/incremental-sql/feldera-community-$version.json}"

case "$(uname -m)" in
  arm64|aarch64)
    runtime_platform="linux/arm64"
    image_platform_digest="sha256:dadbfe7cc919b5e8200833ad63249381967345445b221ba365fe6a4ea886c781"
    ;;
  x86_64|amd64)
    runtime_platform="linux/amd64"
    image_platform_digest="sha256:1fff901bd1aa49e62327675ea987df77c2de3a9b4e4d21bbc2d7392dfb57ba1c"
    ;;
  *)
    echo "unsupported Feldera baseline architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

for dependency in docker duckdb python3; do
  command -v "$dependency" >/dev/null || {
    echo "required dependency is missing: $dependency" >&2
    exit 1
  }
done

observed_digest="$(docker buildx imagetools inspect "$image" | awk '$1 == "Digest:" {print $2; exit}')"
[[ "$observed_digest" == "$image_digest" ]] || {
  echo "Feldera image digest mismatch" >&2
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
  echo "Feldera platform image digest mismatch" >&2
  exit 1
}
docker pull --platform "$runtime_platform" "$image" >/dev/null

exec python3 "$repo_root/scripts/incremental_sql_feldera.py" \
  --image "$image" \
  --image-digest "$image_digest" \
  --image-platform-digest "$image_platform_digest" \
  --runtime-platform "$runtime_platform" \
  --corpus "$repo_root/crates/velorix-runtime/benches/fixtures/incremental_sql_corpus_v1.json" \
  --output "$output_file"
