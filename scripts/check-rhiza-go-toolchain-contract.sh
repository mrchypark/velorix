#!/bin/sh
set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)
rhiza_manifest="$repo_root/crates/velorix-meta/Cargo.toml"
expected_version=1.27.3
expected_sha=65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40

# rhizadb 0.21.2's published native/go.mod requires Go 1.27.3. The checksum
# is the official go.dev/dl/?mode=json linux-amd64 archive entry for that release.
grep -Fq 'rhizadb = { version = "=0.21.2"' "$rhiza_manifest" || {
  echo "rhizadb pin changed; re-audit its native Go minimum before updating this contract" >&2
  exit 1
}

for dockerfile in Dockerfile.meta Dockerfile.all-in-one; do
  path="$repo_root/$dockerfile"
  for required in \
    "ARG GO_VERSION=$expected_version" \
    "ARG GO_TARBALL_SHA256=$expected_sha" \
    "^go version go1\\.27\\.3 linux/amd64$" \
    'GOTOOLCHAIN=local' \
    '--features "rhiza-backend"'; do
    grep -Fq -- "$required" "$path" || {
      echo "$dockerfile is missing Go/Rhiza toolchain contract: $required" >&2
      exit 1
    }
  done
done

echo "Dockerfiles match rhizadb 0.21.2 Go 1.27.3 minimum and official Go archive checksum"
