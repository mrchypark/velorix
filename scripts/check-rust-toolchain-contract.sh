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

toolchain_refs=$(rg -o 'dtolnay/rust-toolchain@[^[:space:]]+' .github/workflows)
if printf '%s\n' "$toolchain_refs" | rg -qv "dtolnay/rust-toolchain@($build_toolchain|$msrv)$"; then
    echo "all Rust CI actions must use an explicit build or MSRV toolchain" >&2
    exit 1
fi

test "$(printf '%s\n' "$toolchain_refs" | rg -Fc "dtolnay/rust-toolchain@$build_toolchain")" -eq 9
test "$(printf '%s\n' "$toolchain_refs" | rg -Fc "dtolnay/rust-toolchain@$msrv")" -eq 1
grep -Fqx "        uses: dtolnay/rust-toolchain@$msrv" .github/workflows/ci.yml

echo "Rust toolchain contract passed: MSRV $msrv; build toolchain $build_toolchain"
