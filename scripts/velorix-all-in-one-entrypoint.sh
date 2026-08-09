#!/usr/bin/env sh
set -eu

mode="${VELORIX_IMAGE_MODE:-api}"
if [ "$#" -gt 0 ]; then
  mode="$1"
  shift
fi

case "$mode" in
  api)
    exec velorix-api "$@"
    ;;
  meta)
    exec velorix-meta "$@"
    ;;
  ingest-writer)
    exec velorix-ingest-writer-entrypoint "$@"
    ;;
  -h | --help | help)
    cat <<'EOF'
Usage:
  velorix-all-in-one-entrypoint [api|meta|ingest-writer] [args...]

Modes:
  api            Run velorix-api.
  meta           Run velorix-meta.
  ingest-writer  Run the bounded ingest-writer entrypoint.

The all-in-one image is a convenience artifact. Product deployments should use
the role-specific velorix-api, velorix-meta, and ingest-writer images.
EOF
    ;;
  *)
    echo "unknown Velorix image mode: ${mode}" >&2
    echo "expected one of: api, meta, ingest-writer" >&2
    exit 64
    ;;
esac
