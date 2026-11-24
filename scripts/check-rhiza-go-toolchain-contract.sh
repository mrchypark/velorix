#!/bin/sh
set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)
rhiza_manifest="$repo_root/crates/velorix-meta/Cargo.toml"
ci_workflow="$repo_root/.github/workflows/ci.yml"
expected_version=1.27.4

# CI builds the bundled native sources; images link the published C archive.
grep -Fq 'rhizadb = { version = "=0.27.5"' "$rhiza_manifest" || {
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

# Match the literal shell variable in the fetch script.
# shellcheck disable=SC2016
grep -Fq 'releases/download/v0.27.5/librhiza_ffi-0.27.5-$target.a' "$repo_root/scripts/fetch-rhiza-native.sh"
grep -Fq 'target=x86_64-unknown-linux-gnu' "$repo_root/scripts/fetch-rhiza-native.sh"
grep -Fq '6c9ddf8f191fc4fc26df824fa142d37d8693fb311ac02e71f815c68e9b82660e' "$repo_root/scripts/fetch-rhiza-native.sh"
grep -Fq 'target=aarch64-unknown-linux-gnu' "$repo_root/scripts/fetch-rhiza-native.sh"
grep -Fq '0b0151b5eab35ed59224212e3f6ba9a272306d86a49c5980ca2c0a1f237fbf33' "$repo_root/scripts/fetch-rhiza-native.sh"

echo "Rhiza 0.27.5: CI Go 1.27.4 and pinned Linux AMD64/ARM64 native archives configured"
