#!/bin/sh
# Explicitly prepared versioned binaries only; this runner never builds Cargo.
set -eu
if [ "$#" -ne 5 ]; then
  echo 'usage: run-slatedb-upgrade-check.sh OLD_BINARY OLD_LOCK NEW_BINARY NEW_LOCK FRESH_OUTPUT' >&2
  exit 64
fi
old_binary=$1
old_lock=$2
new_binary=$3
new_lock=$4
output=$5
for path in "$old_binary" "$old_lock" "$new_binary" "$new_lock" "$output"; do
  case "$path" in /*) ;; *) echo 'All paths must be absolute' >&2; exit 64;; esac
done
test -x "$old_binary"
test -x "$new_binary"
test -f "$old_lock"
test -f "$new_lock"
if [ -e "$output" ]; then echo 'Output already exists' >&2; exit 64; fi
version() {
  awk '$0 == "name = \"slatedb\"" { p=1; next } p && /^version = / { gsub(/"/, "", $3); print $3; exit }' "$1"
}
if [ "$(version "$old_lock")" != '0.15.0' ] || [ "$(version "$new_lock")" != '0.16.0' ]; then
  echo 'Expected SlateDB 0.15.0 old lock and 0.16.0 new lock' >&2
  exit 64
fi
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH='' cd -- "$script_dir/.." && pwd)
source_git_sha=$(git -C "$repo_root" rev-parse HEAD)
source_dirty=$(git -C "$repo_root" status --porcelain | wc -l | tr -d ' ')
hash() { shasum -a 256 "$1" | awk '{print $1}'; }
old_binary_sha=$(hash "$old_binary")
new_binary_sha=$(hash "$new_binary")
old_lock_sha=$(hash "$old_lock")
new_lock_sha=$(hash "$new_lock")
mkdir "$output"
steps="$output/steps.jsonl"
touch "$steps"
finish() {
  result=$?
  trap - EXIT HUP INT TERM
  jq -s --argjson exit_code "$result" --arg git_sha "$source_git_sha" \
    --argjson dirty_entries "$source_dirty" --arg old_binary_sha "$old_binary_sha" \
    --arg new_binary_sha "$new_binary_sha" --arg old_lock_sha "$old_lock_sha" \
    --arg new_lock_sha "$new_lock_sha" \
    '{schema_version:1,exit_code:$exit_code,status:(if $exit_code==0 then "passed" else "failed" end),source_git_sha:$git_sha,source_dirty_entries:$dirty_entries,old:{slatedb:"0.15.0",binary_sha256:$old_binary_sha,lock_sha256:$old_lock_sha},new:{slatedb:"0.16.0",binary_sha256:$new_binary_sha,lock_sha256:$new_lock_sha},steps:.,scope:"Prepared-binary compatibility fixture only; source Git SHA describes runner checkout, not proof of binary build provenance"}' \
    "$steps" > "$output/result.json"
  exit "$result"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' HUP TERM
step() {
  name=$1
  shift
  code=0
  "$@" > "$output/$name.stdout" 2> "$output/$name.stderr" || code=$?
  jq -cn --arg name "$name" --argjson exit_code "$code" \
    --arg stdout "$name.stdout" --arg stderr "$name.stderr" \
    --args '{name:$name,exit_code:$exit_code,stdout:$stdout,stderr:$stderr,command:$ARGS.positional}' -- "$@" >> "$steps"
  if [ "$code" -ne 0 ]; then exit "$code"; fi
}
record_tree() {
  phase=$1
  fixture=$2
  binary=$3
  lock=$4
  expected=$5
  test -d "$fixture/objects"
  actual=$(version "$lock")
  test "$actual" = "$expected"
  binary_hash=$(hash "$binary")
  lock_hash=$(hash "$lock")
  # Fixture paths are controlled ASCII paths without newlines.
  (
    cd "$fixture"
    find . -type f | LC_ALL=C sort | while IFS= read -r file; do
      file_hash=$(hash "$file")
      bytes=$(wc -c < "$file" | tr -d ' ')
      jq -cn --arg path "${file#./}" --arg sha256 "$file_hash" --argjson bytes "$bytes" \
        '{path:$path,bytes:$bytes,sha256:$sha256}'
    done
  ) | jq -s --arg phase "$phase" --arg slatedb "$actual" --arg binary_sha256 "$binary_hash" \
    --arg lock_sha256 "$lock_hash" \
    '{schema_version:1,phase:$phase,slatedb_version:$slatedb,binary_sha256:$binary_sha256,lock_sha256:$lock_sha256,files:.}'
}
record() { step "$1" record_tree "$@"; }
step old-write "$old_binary" old-write "$output/pristine-old"
record old-tree "$output/pristine-old" "$old_binary" "$old_lock" 0.15.0
step copy-for-upgrade cp -R "$output/pristine-old" "$output/upgraded"
step verify-old-copy diff -qr "$output/pristine-old" "$output/upgraded"
step new-upgrade "$new_binary" upgrade "$output/upgraded"
record new-tree "$output/upgraded" "$new_binary" "$new_lock" 0.16.0
step copy-for-rollback cp -R "$output/upgraded" "$output/rollback-probe"
step verify-upgraded-copy diff -qr "$output/upgraded" "$output/rollback-probe"
step old-rollback "$old_binary" verify "$output/rollback-probe"
record rollback-tree "$output/rollback-probe" "$old_binary" "$old_lock" 0.15.0
record pristine-recheck "$output/pristine-old" "$old_binary" "$old_lock" 0.15.0
step assert-pristine-unchanged jq -e --slurpfile original "$output/old-tree.stdout" \
  '.files == $original[0].files' "$output/pristine-recheck.stdout"
# Reject a prepared binary or lockfile changed while the sequence was running.
test "$(hash "$old_binary")" = "$old_binary_sha"
test "$(hash "$new_binary")" = "$new_binary_sha"
test "$(hash "$old_lock")" = "$old_lock_sha"
test "$(hash "$new_lock")" = "$new_lock_sha"
