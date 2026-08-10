#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="v3.0.2"
runtime_image="${RISINGWAVE_RUNTIME_IMAGE:-ubuntu:24.04}"
runtime_image_digest="${RISINGWAVE_RUNTIME_IMAGE_DIGEST:-sha256:561618e2c15bf2397621dd04f96926663a3b5616c189cf7e38db7e82f5c538ea}"
output_file="${1:-$repo_root/baselines/incremental-sql/risingwave-v3.0.2.json}"
tool_root="$repo_root/target/external-tools/risingwave-$version"
risingwave_bin="${RISINGWAVE_BIN:-$tool_root/risingwave}"
package_sha=""

case "$(uname -m)" in
  arm64|aarch64)
    runtime_platform="linux/arm64"
    runtime_image_platform_digest="sha256:b17516cd982bf06bdd5d5600253d12a8de017b9eb831cc052b532a0363d294f9"
    ;;
  x86_64|amd64)
    runtime_platform="linux/amd64"
    runtime_image_platform_digest="sha256:019e8eb29a85e74d64925745884f2ec79aa27e3feab36353d24656f4d6b89467"
    ;;
  *)
    echo "unsupported RisingWave baseline architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

for dependency in curl docker duckdb psql python3 shasum tar; do
  command -v "$dependency" >/dev/null || {
    echo "required dependency is missing: $dependency" >&2
    exit 1
  }
done

if [[ -z "${RISINGWAVE_BIN:-}" ]]; then
  case "$(uname -m)" in
    arm64|aarch64)
      platform="aarch64-unknown-linux"
      package_sha="b9478e8ca0c14e718054eab709e3772cdd5d074631b5600c6d755a0744753bb4"
      ;;
    x86_64|amd64)
      platform="x86_64-unknown-linux"
      package_sha="3bbd15907aad45a3f3f8b9a8bc9bf9db0b1ed83fa96dd34681326ec0365e3188"
      ;;
  esac
  archive="risingwave-$version-$platform.tar.gz"
  mkdir -p "$tool_root"
  if [[ ! -f "$tool_root/$archive" ]]; then
    curl -fL --retry 3 -o "$tool_root/$archive" \
      "https://github.com/risingwavelabs/risingwave/releases/download/$version/$archive"
  fi
  actual_sha="$(shasum -a 256 "$tool_root/$archive" | awk '{print $1}')"
  [[ "$actual_sha" == "$package_sha" ]] || {
    echo "RisingWave release package checksum mismatch" >&2
    exit 1
  }
  if [[ ! -x "$risingwave_bin" ]]; then
    tar -xzf "$tool_root/$archive" -C "$tool_root"
  fi
else
  package_sha="external-binary-override"
fi

observed_digest="$(docker buildx imagetools inspect "$runtime_image" | awk '$1 == "Digest:" {print $2; exit}')"
[[ "$observed_digest" == "$runtime_image_digest" ]] || {
  echo "RisingWave runtime image digest mismatch" >&2
  exit 1
}
observed_platform_digest="$(docker buildx imagetools inspect --raw "$runtime_image" | python3 -c '
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
[[ "$observed_platform_digest" == "$runtime_image_platform_digest" ]] || {
  echo "RisingWave runtime platform image digest mismatch" >&2
  exit 1
}
docker pull --platform "$runtime_platform" "$runtime_image" >/dev/null

exec python3 "$repo_root/scripts/incremental_sql_risingwave.py" \
  --risingwave-bin "$risingwave_bin" \
  --runtime-image "$runtime_image" \
  --runtime-image-digest "$runtime_image_digest" \
  --runtime-image-platform-digest "$runtime_image_platform_digest" \
  --runtime-platform "$runtime_platform" \
  --package-sha256 "$package_sha" \
  --corpus "$repo_root/crates/velorix-runtime/benches/fixtures/incremental_sql_corpus_v1.json" \
  --output "$output_file"
