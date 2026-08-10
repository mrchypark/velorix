#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="v1.1.4"
output_path="${1:-$repo_root/baselines/incremental-sql/greptimedb-flow-v1.1.4.json}"
tool_root="$repo_root/target/external-tools/greptimedb-$version"
greptime_bin="${GREPTIME_BIN:-$tool_root/greptime}"

if [[ ! -x "$greptime_bin" ]]; then
  case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) platform="darwin-arm64" ;;
    Linux-aarch64|Linux-arm64) platform="linux-arm64" ;;
    Linux-x86_64) platform="linux-amd64" ;;
    *) echo "unsupported GreptimeDB baseline platform: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
  esac
  archive="greptime-$platform-$version.tar.gz"
  checksum_asset="greptime-$platform-$version.sha256sum"
  expected_sha="$(curl -fsSL "https://github.com/GreptimeTeam/greptimedb/releases/download/$version/$checksum_asset")"
  mkdir -p "$tool_root"
  curl -fL --retry 3 -o "$tool_root/$archive" \
    "https://github.com/GreptimeTeam/greptimedb/releases/download/$version/$archive"
  actual_sha="$(shasum -a 256 "$tool_root/$archive" | awk '{print $1}')"
  [[ "$actual_sha" == "$expected_sha" ]] || {
    echo "GreptimeDB archive checksum mismatch" >&2
    exit 1
  }
  tar -xzf "$tool_root/$archive" -C "$tool_root" --strip-components=1
fi

exec python3 "$repo_root/scripts/incremental_sql_greptimedb.py" \
  --greptime-bin "$greptime_bin" \
  --corpus "$repo_root/crates/velorix-runtime/benches/fixtures/incremental_sql_corpus_v1.json" \
  --output "$output_path"
