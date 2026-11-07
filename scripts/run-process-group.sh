#!/bin/sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
repo_root=$(git -C "$script_dir" rev-parse --show-toplevel)
source_dir="$script_dir/run-process-group-src"
cache_dir="$repo_root/target/process-group-runner"
go_version=$(go version)
go_platform=$(go env GOOS GOARCH)
source_hash=$(
  {
    for source_file in "$source_dir/go.mod" "$source_dir"/*.go; do
      [ -f "$source_file" ] || continue
      printf '%s ' "$(basename "$source_file")"
      git hash-object "$source_file"
    done
  } | git hash-object --stdin
)
cache_key=$(printf '%s\n%s\n%s\n' "$source_hash" "$go_version" "$go_platform" | git hash-object --stdin)
binary_path="$cache_dir/run-process-group-$cache_key"

if [ ! -x "$binary_path" ]; then
  mkdir -p "$cache_dir"
  temporary_binary=$(mktemp "$cache_dir/.run-process-group.XXXXXX")
  trap 'rm -f "$temporary_binary"' EXIT HUP INT TERM
  if ! (cd "$source_dir" && GOWORK=off go build -trimpath -o "$temporary_binary" .); then
    echo 'failed to build cached process-group helper' >&2
    exit 1
  fi
  chmod 700 "$temporary_binary"
  mv "$temporary_binary" "$binary_path"
  trap - EXIT HUP INT TERM
fi

exec "$binary_path" "$@"
