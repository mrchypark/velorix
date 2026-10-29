#!/bin/sh
set -eu

# Manual local recovery probe using the source-pinned VersityGW fixture.
# This invokes only the one-voter test; it does not run the three-voter suite.
repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
RHIZA_RECOVERY_TEST_FILTER=single-node exec sh scripts/check-rhiza-recovery.sh
