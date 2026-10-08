#!/bin/sh
# Regression for rhiza_verify_operator_manifests in scripts/rhiza-kv-k8s-lib.sh.
#
# UPSTREAM.sha256 ships without a trailing newline. A plain `while read -r want
# file` loop stops on that last record, so deployment.yaml was never hashed and a
# tampered final manifest passed verification. These cases pin the real behavior:
# the pristine vendored manifests verify, and a tampered final manifest is
# rejected whether the checksum file ends with a newline or not.
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

operator_dir=deploy/rhiza-k8s/operator
. scripts/rhiza-kv-k8s-lib.sh

scratch_dir=$(mktemp -d "${TMPDIR:-/tmp}/velorix-operator-manifests-contract.XXXXXX")
trap 'rm -rf "$scratch_dir"' EXIT
tamper_log="$scratch_dir/tamper.log"

fail() {
  echo "$1" >&2
  exit 1
}

# The pristine vendored operator is what the gate verifies before it claims a
# recovery, so it must pass unchanged.
rhiza_verify_operator_manifests "$operator_dir" \
  || fail "the pinned operator manifests in $operator_dir failed sha256 verification"

# Tamper the final manifest named in UPSTREAM.sha256, leaving the recorded
# digests as upstream published them.
manifest_case() {
  case_dir="$scratch_dir/$1"
  mkdir -p "$case_dir"
  cp "$operator_dir/crd.yaml" "$operator_dir/rbac.yaml" \
    "$operator_dir/deployment.yaml" "$case_dir/"
}

tamper_case() {
  manifest_case "$1"
  printf '\n# tampered\n' >>"$scratch_dir/$1/deployment.yaml"
}

reject_case() {
  if rhiza_verify_operator_manifests "$1" >/dev/null 2>>"$tamper_log"; then
    fail "a tampered final operator manifest was accepted: $2"
  fi
}

# Upstream shape: the checksum file ends without a newline, so this is the exact
# record the reader used to drop.
tamper_case no-trailing-newline
cp "$operator_dir/UPSTREAM.sha256" "$scratch_dir/no-trailing-newline/UPSTREAM.sha256"
reject_case "$scratch_dir/no-trailing-newline" \
  "UPSTREAM.sha256 without a final newline"

# Same bytes, newline terminated: a trailing newline must not be what keeps the
# last manifest verified.
tamper_case trailing-newline
sed -e '$a\' "$operator_dir/UPSTREAM.sha256" \
  >"$scratch_dir/trailing-newline/UPSTREAM.sha256"
reject_case "$scratch_dir/trailing-newline" \
  "UPSTREAM.sha256 with a final newline"

# No checksum file at all: there is nothing to verify against, so this fails
# closed instead of reporting success.
tamper_case no-checksum-file
reject_case "$scratch_dir/no-checksum-file" "no UPSTREAM.sha256"

# A final record that is a bare digest names no manifest. Without a trailing
# newline it must still reach the malformed-record rejection, not end the loop.
manifest_case final-digest-without-filename
final_record=$scratch_dir/final-digest-without-filename/UPSTREAM.sha256
awk '$2 != "deployment.yaml"' "$operator_dir/UPSTREAM.sha256" >"$final_record"
printf '%s' \
  "$(awk '$2 == "deployment.yaml" { print $1 }' "$operator_dir/UPSTREAM.sha256")" \
  >>"$final_record"
reject_case "$scratch_dir/final-digest-without-filename" \
  "a final UPSTREAM.sha256 digest with no manifest name and no final newline"

# Comment-only: every record is skipped, so no manifest was verified and success
# would be a false claim.
manifest_case comment-only-checksum-file
grep '^#' "$operator_dir/UPSTREAM.sha256" \
  >"$scratch_dir/comment-only-checksum-file/UPSTREAM.sha256"
reject_case "$scratch_dir/comment-only-checksum-file" \
  "a UPSTREAM.sha256 that verifies no manifest"

echo "Rhiza operator manifest contract passed: pinned manifests verify and a tampered final manifest is rejected"