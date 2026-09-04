#!/bin/sh
# Validates the intentional separation between Velorix's MSRV and build toolchain.
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

msrv=1.98.0
build_toolchain=1.98.1
builder_base='FROM rust:1.98.0-bookworm@sha256:82150a52ec202c1b14d7817e14516c392bb7f5cfebd88f1ed531cb37ebd39922 AS builder'

test "$(sed -n 's/^rust-version = "\([^"]*\)"$/\1/p' Cargo.toml)" = "$msrv"
test "$(sed -n 's/^channel = "\([^"]*\)"$/\1/p' rust-toolchain.toml)" = "$build_toolchain"

for dockerfile in Dockerfile.api Dockerfile.all-in-one Dockerfile.meta Dockerfile.ingest-writer Dockerfile.hiqlite; do
    grep -Fqx "$builder_base" "$dockerfile"
    grep -Fqx "RUN rustup toolchain install $build_toolchain --profile minimal --no-self-update \\" "$dockerfile"
    grep -Fqx "    && rustup default $build_toolchain \\" "$dockerfile"
    grep -Fqx "    && rustc --version | grep -Eq '^rustc 1\\.98\\.1 '" "$dockerfile"
done

toolchain_refs=$(find .github/workflows -type f \( -name '*.yml' -o -name '*.yaml' \) -exec \
    sed -n 's/^[[:space:]]*uses:[[:space:]]*\(dtolnay\/rust-toolchain@[^[:space:]#]*\)[[:space:]]*$/\1/p' {} +)
if ! printf '%s\n' "$toolchain_refs" | awk -v build="$build_toolchain" -v minimum="$msrv" '
    $0 != "dtolnay/rust-toolchain@" build && $0 != "dtolnay/rust-toolchain@" minimum {
        invalid = 1
    }
    END { exit invalid }
'; then
    echo "all Rust CI actions must use an explicit build or MSRV toolchain" >&2
    exit 1
fi

test "$(printf '%s\n' "$toolchain_refs" | awk -v ref="dtolnay/rust-toolchain@$build_toolchain" '$0 == ref { count += 1 } END { print count + 0 }')" -eq 10
test "$(printf '%s\n' "$toolchain_refs" | awk -v ref="dtolnay/rust-toolchain@$msrv" '$0 == ref { count += 1 } END { print count + 0 }')" -eq 1
grep -Fqx "        uses: dtolnay/rust-toolchain@$msrv" .github/workflows/ci.yml

echo "Rust toolchain contract passed: MSRV $msrv; build toolchain $build_toolchain"
