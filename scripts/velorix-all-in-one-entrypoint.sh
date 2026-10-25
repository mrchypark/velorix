#!/usr/bin/env sh
set -eu

mode="${VELORIX_IMAGE_MODE:-all}"
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
  all)
    if [ "$#" -ne 0 ]; then
      echo "all mode does not accept command arguments" >&2
      exit 64
    fi

    if [ "${VELORIX_SERVICE_ROLE:-all}" != all ]; then
      echo "all image mode requires VELORIX_SERVICE_ROLE=all" >&2
      exit 64
    fi
    export VELORIX_SERVICE_ROLE=all
    if [ "${VELORIX_META_BACKEND:-}" != rhiza-kv ]; then
      echo "all image mode requires VELORIX_META_BACKEND=rhiza-kv" >&2
      exit 64
    fi
    if [ -z "${VELORIX_META_MODE:-}" ] || [ -z "${VELORIX_META_BIND:-}" ] \
      || [ -z "${VELORIX_META_GRPC_ENDPOINT:-}" ]; then
      echo "all image mode requires VELORIX_META_MODE, VELORIX_META_BIND, and VELORIX_META_GRPC_ENDPOINT" >&2
      exit 64
    fi

    case "${VELORIX_META_TRANSPORT_SECURITY:-}" in
      native-mtls) meta_scheme=https ;;
      '') meta_scheme=http ;;
      *)
        echo "all image mode supports unset or native-mtls VELORIX_META_TRANSPORT_SECURITY" >&2
        exit 64
        ;;
    esac

    meta_port="${VELORIX_META_BIND##*:}"
    if [ "$meta_port" = "$VELORIX_META_BIND" ] \
      || [ "$VELORIX_META_GRPC_ENDPOINT" != "$meta_scheme://127.0.0.1:$meta_port" ]; then
      echo "all image mode requires VELORIX_META_GRPC_ENDPOINT to use loopback and the VELORIX_META_BIND port" >&2
      exit 64
    fi

    meta_pid=
    api_pid=
    smoke_pid=
    poll_pid=

    stop_child() {
      target_pid="$1"
      [ -n "$target_pid" ] || return 0
      kill -TERM "$target_pid" 2>/dev/null || :
      i=0
      while kill -0 "$target_pid" 2>/dev/null && [ "$i" -lt 5 ]; do
        sleep 1 &
        poll_pid=$!
        wait "$poll_pid" 2>/dev/null || :
        poll_pid=
        i=$((i + 1))
      done
      if kill -0 "$target_pid" 2>/dev/null; then
        kill -KILL "$target_pid" 2>/dev/null || :
      fi
      wait "$target_pid" 2>/dev/null || :
    }

    terminate_children() {
      trap - 0
      trap '' HUP INT TERM
      if [ -n "$poll_pid" ]; then
        kill -TERM "$poll_pid" 2>/dev/null || :
        wait "$poll_pid" 2>/dev/null || :
        poll_pid=
      fi
      if [ -n "$api_pid" ]; then
        stop_child "$api_pid"
        api_pid=
      fi
      if [ -n "$smoke_pid" ]; then
        stop_child "$smoke_pid"
        smoke_pid=
      fi
      if [ -n "$meta_pid" ]; then
        stop_child "$meta_pid"
        meta_pid=
      fi
    }

    on_signal() {
      exit_status="$1"
      terminate_children
      exit "$exit_status"
    }

    trap 'terminate_children' 0
    trap 'on_signal 143' TERM
    trap 'on_signal 130' INT
    trap 'on_signal 129' HUP

    velorix-meta serve &
    meta_pid=$!
    velorix-meta smoke --capabilities-only --expect-backend rhiza-kv \
      --expect-auth-enforced true --expect-production-multi-writer-safe false &
    smoke_pid=$!
    if wait "$smoke_pid"; then
      smoke_status=0
    else
      smoke_status=$?
    fi
    smoke_pid=
    if [ "$smoke_status" -ne 0 ]; then
      terminate_children
      exit "$smoke_status"
    fi

    velorix-api &
    api_pid=$!
    while :; do
      if ! kill -0 "$meta_pid" 2>/dev/null; then
        if wait "$meta_pid"; then
          child_status=0
        else
          child_status=$?
        fi
        [ "$child_status" -ne 0 ] || child_status=1
        meta_pid=
        terminate_children
        exit "$child_status"
      fi
      if ! kill -0 "$api_pid" 2>/dev/null; then
        if wait "$api_pid"; then
          child_status=0
        else
          child_status=$?
        fi
        [ "$child_status" -ne 0 ] || child_status=1
        api_pid=
        terminate_children
        exit "$child_status"
      fi
      sleep 1 &
      poll_pid=$!
      wait "$poll_pid" || :
      poll_pid=
    done
    ;;
  -h | --help | help)
    cat <<'EOF'
Usage:
  velorix-all-in-one-entrypoint [all|api|meta|ingest-writer] [args...]

Modes:
  all            Run Rhiza-backed Meta and API with API role all (default).
  api            Run velorix-api.
  meta           Run velorix-meta.
  ingest-writer  Run the bounded ingest-writer entrypoint.

All mode requires explicit VELORIX_META_MODE, VELORIX_META_BACKEND=rhiza-kv,
VELORIX_META_BIND, and a loopback VELORIX_META_GRPC_ENDPOINT on the same port.
Meta bearer and TLS settings are read from the existing VELORIX_META_* env vars.
EOF
    ;;
  *)
    echo "unknown Velorix image mode: ${mode}" >&2
    echo "expected one of: all, api, meta, ingest-writer" >&2
    exit 64
    ;;
esac
