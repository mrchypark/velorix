#!/bin/sh
set -eu

# Focused Local recovery: first exercise the native SDK Config::local
# before-ack/empty-directory probe, then the split-process HTTP service path
# with init-local and graceful-close assertions. Both use owned local fixtures.
repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
RHIZA_RECOVERY_TEST_FILTER=local sh scripts/check-rhiza-recovery.sh
RHIZA_SERVICE_MODE=local exec sh scripts/check-rhiza-service-recovery.sh
