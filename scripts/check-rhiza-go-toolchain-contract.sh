#!/bin/sh
set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)
rhiza_manifest="$repo_root/crates/velorix-meta/Cargo.toml"
expected_version=1.27.3
amd64_sha=65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40
arm64_sha=a06e99309dbcbdca1a8ccd2282c3ee146e55930886e26b4f1193269587165f49

# rhizadb 0.21.2 requires Go 1.27.3. Checksums are official go.dev archive entries.
grep -Fq 'rhizadb = { version = "=0.21.2"' "$rhiza_manifest" || {
  echo "rhizadb pin changed; re-audit its native Go minimum before updating this contract" >&2
  exit 1
}

check_dockerfile() {
  path=$1
  label=$2
  for required in "ARG GO_VERSION=$expected_version" 'ARG TARGETARCH' 'GOTOOLCHAIN=local' '--features "rhiza-backend"' "$amd64_sha" "$arm64_sha" 'sha256sum --check --strict' 'go version go1\\.27\\.3 linux/${goarch}'; do
    grep -Fq -- "$required" "$path" || {
      echo "$label is missing Go/Rhiza toolchain contract: $required" >&2
      exit 1
    }
  done
}

if [ "${1-}" = --self-test ]; then
  tmp=${TMPDIR:-/tmp}/rhiza-go-contract.$$
  trap 'rm -rf "$tmp"' EXIT HUP INT TERM
  mkdir -p "$tmp"
  for dockerfile in Dockerfile.meta Dockerfile.all-in-one; do
    cp "$repo_root/$dockerfile" "$tmp/$dockerfile"
    check_dockerfile "$tmp/$dockerfile" "$dockerfile fixture"
    sed "s/$arm64_sha/0000000000000000000000000000000000000000000000000000000000000000/" "$tmp/$dockerfile" > "$tmp/bad-sha"
    if (check_dockerfile "$tmp/bad-sha" 'wrong ARM checksum fixture') 2>/dev/null; then exit 1; fi
    sed '/go version go1/d' "$tmp/$dockerfile" > "$tmp/no-version"
    if (check_dockerfile "$tmp/no-version" 'missing Go assertion fixture') 2>/dev/null; then exit 1; fi
  done
  echo 'Go toolchain contract self-test passed'
  exit 0
fi

for dockerfile in Dockerfile.meta Dockerfile.all-in-one; do check_dockerfile "$repo_root/$dockerfile" "$dockerfile"; done

echo "Dockerfiles match rhizadb 0.21.2 Go 1.27.3 minimum and official Go archive checksum"
