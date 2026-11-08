#!/bin/sh
set -eu

# Manual opt-in integration check for macOS ARM64; this is not CI or provider
# qualification. It uses disposable Local DataDir state and no object store.
repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)
cd "$repo_root"
target_dir=${CARGO_TARGET_DIR:-"$repo_root/target"}
free_space_limit_kib=$((5 * 1024 * 1024))
legacy_archive_sha=19a256b8098f144afc0f5fc007d90a5b8550086140264025abf2ecb1ac5bc75e
legacy_sidecar_sha=a95fb666bded00c963a536877ab67da237c736afd94a9ccb91dadf57bd0bc86f
legacy_crate_sha=c913bfcb9a6c6c6b5340a92c320d08c4941ae07d985e6174de7d59ad12f03db0
current_archive_sha=bb5d698149a9b7e94d8db74dfee45db2ecc5420b4e5c327bcc8b36f5ac3696bb
current_crate_sha=7478cd8fd7e10dd35ffd8d457a2fac8dca9cfa45d0a97027663dc4c3de85bb8c
legacy_archive_name=librhiza_ffi-0.21.2-aarch64-apple-darwin.a
legacy_sidecar_name=$legacy_archive_name.sha256
current_archive_name=librhiza_ffi-0.23.3-aarch64-apple-darwin.a
artifact_dir=${RHIZA_0233_ARTIFACT_DIR:-"$repo_root/target/rhiza-0233-integration/native"}
current_archive=$artifact_dir/$current_archive_name
cargo_home=${CARGO_HOME:-"$HOME/.cargo"}

check_space() {
  space_root=${1:-$repo_root}
  minimum_free_kib=${2:-$free_space_limit_kib}
  free_kib=$(df -Pk "$space_root" | awk 'NR == 2 { print $4 }')
  if [ -z "$free_kib" ] || [ "$free_kib" -lt "$minimum_free_kib" ]; then
    echo "stopping before Cargo: free disk space is below 5 GiB ($free_kib KiB)" >&2
    return 75
  fi
  echo "free_disk_kib=$free_kib"
}

check_sha() {
  expected=$1
  path=$2
  actual=$(shasum -a 256 "$path" | awk '{print $1}')
  if [ "$actual" != "$expected" ]; then
    echo "SHA-256 mismatch for $path: expected $expected, got $actual" >&2
    return 1
  fi
  echo "sha256 ok: $path"
}

if [ "${1-}" = --self-test ]; then
  check_sha "$current_archive_sha" "$current_archive"
  if check_sha "0000000000000000000000000000000000000000000000000000000000000000" "$current_archive"; then
    echo "checksum guard accepted an incorrect digest" >&2
    exit 1
  fi
  check_space "$repo_root" 1 >/dev/null
  if check_space "$repo_root" 999999999999 >/dev/null 2>&1; then
    echo "disk guard accepted an unavailable space requirement" >&2
    exit 1
  fi
  echo "self-test passed: archive checksum and disk guards accept/reject through production functions"
  exit 0
fi

check_space
if [ "$(uname -s)" != Darwin ] || [ "$(uname -m)" != arm64 ]; then
  echo "this verified artifact pair is macOS ARM64 only; current host is $(uname -s)/$(uname -m)" >&2
  exit 1
fi
grep -Fq 'rhizadb = { version = "=0.23.3"' "$repo_root/crates/velorix-meta/Cargo.toml" || {
  echo "workspace rhizadb dependency is not pinned to 0.23.3" >&2
  exit 1
}
locked_version=$(awk '
  /^\[\[package\]\]$/ { in_package = 0 }
  /^name = "rhizadb"$/ { in_package = 1 }
  in_package && /^version = / { gsub(/"/, "", $3); print $3; exit }
' "$repo_root/Cargo.lock")
if [ "$locked_version" != 0.23.3 ]; then
  echo "Cargo.lock rhizadb version is $locked_version, expected 0.23.3" >&2
  exit 1
fi
set -- "$cargo_home"/registry/cache/*/rhizadb-0.21.2.crate
if [ ! -f "$1" ]; then
  echo "rhizadb 0.21.2 crate is not cached; refusing to download a crate" >&2
  exit 1
fi
check_sha "$legacy_crate_sha" "$1"
set -- "$cargo_home"/registry/cache/*/rhizadb-0.23.3.crate
if [ ! -f "$1" ]; then
  echo "rhizadb 0.23.3 crate is not cached; refusing to download a crate" >&2
  exit 1
fi
check_sha "$current_crate_sha" "$1"
echo "verified rhizadb crates: 0.21.2 ($legacy_crate_sha), 0.23.3 ($current_crate_sha)"
check_sha "$current_archive_sha" "$current_archive"

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-local-cross-version.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM
legacy_native_dir=$tmp_dir/native-0.21.2
current_native_dir=$tmp_dir/native-0.23.3
mkdir -p "$legacy_native_dir" "$current_native_dir" "$tmp_dir/legacy/src"
legacy_archive=$legacy_native_dir/librhiza_ffi.a
cp "$current_archive" "$current_native_dir/librhiza_ffi.a"
echo "verified current native archive: 0.23.3 macOS ARM64 ($current_archive_sha)"

echo "Downloading published 0.21.2 macOS ARM64 native archive"
curl --fail --location --retry 2 --connect-timeout 15 --max-time 300 \
  "https://github.com/mrchypark/rhiza/releases/download/v0.21.2/$legacy_archive_name" \
  -o "$legacy_archive"
curl --fail --location --retry 2 --connect-timeout 15 --max-time 60 \
  "https://github.com/mrchypark/rhiza/releases/download/v0.21.2/$legacy_sidecar_name" \
  -o "$tmp_dir/$legacy_sidecar_name"
check_sha "$legacy_sidecar_sha" "$tmp_dir/$legacy_sidecar_name"
sidecar_archive_sha=$(awk 'NR == 1 { print $1 }' "$tmp_dir/$legacy_sidecar_name")
if [ "$sidecar_archive_sha" != "$legacy_archive_sha" ]; then
  echo "published 0.21.2 sidecar does not match the release digest" >&2
  exit 1
fi
check_sha "$sidecar_archive_sha" "$legacy_archive"
echo "verified legacy native archive: 0.21.2 macOS ARM64 ($sidecar_archive_sha)"

cat >"$tmp_dir/legacy/Cargo.toml" <<'EOF'
[package]
name = "rhiza-0212-local-writer"
version = "0.1.0"
edition = "2021"

[dependencies]
rhizadb = "=0.21.2"
EOF

cat >"$tmp_dir/legacy/src/main.rs" <<'EOF'
use rhizadb::{Config, Db};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    assert_eq!(args.next().as_deref(), Some(std::ffi::OsStr::new("write")));
    let data_dir = args.next().expect("Local DataDir argument");
    assert!(args.next().is_none(), "unexpected fixture arguments");
    let mut db = Db::open(Config::new(data_dir).local().node_id("compat-node"))?;
    db.kv_put("legacy-kv", "compat/key", b"old-kv")?;
    db.close()?;
    Ok(())
}
EOF

check_space
echo "Building the isolated 0.21.2 writer with one Cargo job and the shared target directory"
RHIZA_NATIVE_LIB_DIR=$legacy_native_dir \
  cargo build --offline --jobs 1 --manifest-path "$tmp_dir/legacy/Cargo.toml" \
    --target-dir "$target_dir"
check_space

echo "Running the 0.23.3 reader/append/restart test with one Cargo job"
RHIZA_NATIVE_LIB_DIR=$current_native_dir \
RHIZA_0212_FIXTURE_BIN=$target_dir/debug/rhiza-0212-local-writer \
  cargo test --offline --locked --jobs 1 -p velorix-meta \
    --features rhiza-backend --test rhiza_local_cross_version \
    rhiza_0212_local_kv_opens_appends_and_restarts_on_0233 -- --ignored --exact
check_space
echo "PASS: isolated Local DataDir 0.21.2 to 0.23.3 KV restart compatibility"
echo "Artifact scope: published macOS ARM64 SDK archives verified by SHA-256; current archive SHA-256 $current_archive_sha; Cargo.toml/Cargo.lock pin checked."
echo "Scope: KV and DataDir only; SQL, object-store archives, and provider behavior were not tested."
