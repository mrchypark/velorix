#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_path="${1:-$repo_root/baselines/incremental-sql/velorix-v0.1.0.json}"
source_revision="$(git -C "$repo_root" rev-parse HEAD)"
if [[ -n "$(git -C "$repo_root" status --porcelain)" ]]; then
  source_revision="${source_revision}+worktree"
fi

exec cargo run \
  --manifest-path "$repo_root/Cargo.toml" \
  -p velorix-runtime \
  --example incremental_sql_baseline \
  -- \
  --output "$output_path" \
  --source-revision "$source_revision"
