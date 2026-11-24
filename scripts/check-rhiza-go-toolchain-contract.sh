#!/bin/sh
set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)
rhiza_manifest="$repo_root/crates/velorix-meta/Cargo.toml"
ci_workflow="$repo_root/.github/workflows/ci.yml"
expected_version=1.27.4

# CI needs Go for the recovery fixture; images link the published C archive.
grep -Fq 'rhizadb = { version = "=0.27.4"' "$rhiza_manifest" || {
  echo "rhizadb pin changed; re-audit its native Go minimum before updating this contract" >&2
  exit 1
}

for required in \
  "Install Go $expected_version" \
  "go-version: '$expected_version'"; do
  grep -Fq -- "$required" "$ci_workflow" || {
    echo ".github/workflows/ci.yml is missing Go/Rhiza toolchain contract: $required" >&2
    exit 1
  }
done

for dockerfile in Dockerfile.meta Dockerfile.all-in-one; do
  path="$repo_root/$dockerfile"
  for required in \
    'GOTOOLCHAIN=local' \
    'RUN sh /tmp/fetch-rhiza-native.sh /opt/rhiza-native' \
    'ENV RHIZA_NATIVE_LIB_DIR=/opt/rhiza-native' \
    '--features "rhiza-backend"'; do
    grep -Fq -- "$required" "$path" || {
      echo "$dockerfile is missing Go/Rhiza toolchain contract: $required" >&2
      exit 1
    }
  done
done

# Match the literal workflow variable rather than this process's environment.
# shellcheck disable=SC2016
grep -Fq 'sh scripts/fetch-rhiza-native.sh "$RUNNER_TEMP/rhiza-native"' "$ci_workflow"
grep -Fq 'releases/download/v0.27.4/librhiza_ffi-0.27.4-x86_64-unknown-linux-gnu.a' "$repo_root/scripts/fetch-rhiza-native.sh"
grep -Fq 'bb669dcde5d9db83c6ade8c8496cf55c03470f5b8222df44220501683728cdf1' "$repo_root/scripts/fetch-rhiza-native.sh"

echo "Rhiza 0.27.4: CI fixture Go 1.27.4 and pinned Linux AMD64 native archive configured"
