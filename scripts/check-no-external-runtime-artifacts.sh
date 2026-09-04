#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: scripts/check-no-external-runtime-artifacts.sh [--help]

Fails if product runtime files contain forbidden external runtime artifact terms.
EOF
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  usage
  exit 0
fi

if [ "$#" -ne 0 ]; then
  usage >&2
  exit 64
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

python3 - <<'PY'
from pathlib import Path
import json
import subprocess
import sys

dockerfiles = [
    "Dockerfile.api",
    "Dockerfile.meta",
    "Dockerfile.ingest-writer",
    "Dockerfile.hiqlite",
    "Dockerfile.all-in-one",
]
runtime_source_dirs = [
    "crates/velorix-api/src",
    "crates/velorix-control/src",
    "crates/velorix-core/src",
    "crates/velorix-ingest-writer/src",
    "crates/velorix-meta/src",
    "crates/velorix-runtime/src",
    "crates/velorix-storage/src",
]
product_release_packages = [
    "velorix-api",
    "velorix-ingest-writer",
    "velorix-meta",
]
cargo_files = [
    "Cargo.toml",
    "Cargo.lock",
    *[str(path) for path in sorted(Path("crates").glob("*/Cargo.toml"))],
]
deployment_files = [
    *[str(path) for path in sorted(Path("scripts").glob("*.sh"))],
    *[str(path) for path in sorted(Path(".github/workflows").glob("*.yml"))],
    *[str(path) for path in sorted(Path(".github/workflows").glob("*.yaml"))],
]
deployment_scan_excludes = {
    "scripts/check-no-external-runtime-artifacts.sh",
    "scripts/check-vind-product-contract.sh",
    # External-comparison baseline tooling (Feldera/DBSP reference runs) is
    # not a product runtime path and is scanned separately.
    "scripts/run-feldera-community-baseline.sh",
}
forbidden_runtime_terms = [
    "feldera",
    "dbsp",
    "pipeline-manager",
    "pipeline_manager",
    "compiler-worker",
    "compiler_worker",
    ".jar",
]
forbidden_dockerfile_terms = [
    *forbidden_runtime_terms,
    "javac",
    "rustc",
    "persistentVolumeClaim",
    "volumeClaimTemplates",
    "pvc",
]

violations = []


def scan_file(path, label, forbidden_terms):
    if not path.is_file():
        violations.append((label, None, "missing", "file is missing"))
        return
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        lowered = line.lower()
        for term in forbidden_terms:
            # The product build policy verifies the exact pinned Rust compiler
            # selected by rustup. This is a builder-only assertion, not an
            # external runtime/compiler path.
            if (
                term == "rustc"
                and line.strip()
                == "&& rustc --version | grep -Eq '^rustc 1\\.98\\.1 '"
            ):
                continue
            if term.lower() in lowered:
                violations.append((label, line_number, term, line.strip()))


def scan_tree(directory, forbidden_terms):
    path = Path(directory)
    if not path.is_file():
        if not path.is_dir():
            violations.append((directory, None, "missing", "directory is missing"))
            return
        for child in sorted(path.rglob("*.rs")):
            scan_file(child, str(child), forbidden_terms)
        return
    scan_file(path, str(path), forbidden_terms)


def scan_product_dependency_closure():
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--locked", "--format-version", "1"],
            text=True,
        )
    )
    packages_by_id = {package["id"]: package for package in metadata["packages"]}
    nodes_by_id = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    product_ids = [
        package["id"]
        for package in metadata["packages"]
        if package["name"] in product_release_packages
    ]
    found = {packages_by_id[package_id]["name"] for package_id in product_ids}
    for package in sorted(set(product_release_packages) - found):
        violations.append((package, None, "missing", "product package is missing from cargo metadata"))
    stack = list(product_ids)
    seen = set()
    while stack:
        package_id = stack.pop()
        if package_id in seen:
            continue
        seen.add(package_id)
        package = packages_by_id[package_id]
        lowered = package["name"].lower()
        for term in forbidden_runtime_terms:
            if term.lower() in lowered:
                violations.append((package["name"], None, term, "forbidden product dependency package"))
        stack.extend(dep["pkg"] for dep in nodes_by_id[package_id]["deps"])


for dockerfile in dockerfiles:
    scan_file(Path(dockerfile), dockerfile, forbidden_dockerfile_terms)

for source_dir in runtime_source_dirs:
    scan_tree(source_dir, forbidden_runtime_terms)

for cargo_file in cargo_files:
    scan_file(Path(cargo_file), cargo_file, forbidden_runtime_terms)

for deployment_file in deployment_files:
    if deployment_file in deployment_scan_excludes:
        continue
    scan_file(Path(deployment_file), deployment_file, forbidden_runtime_terms)

scan_product_dependency_closure()

if violations:
    print("forbidden external runtime artifacts found:", file=sys.stderr)
    for dockerfile, line_number, term, line in violations:
        if line_number is None:
            print(f"{dockerfile}: {line}", file=sys.stderr)
        else:
            print(f"{dockerfile}:{line_number}: {term}: {line}", file=sys.stderr)
    raise SystemExit(1)

print("no forbidden external runtime artifacts found in product Dockerfiles, source, cargo metadata, or deployment scripts")
PY
