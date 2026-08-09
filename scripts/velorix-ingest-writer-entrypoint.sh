#!/usr/bin/env sh
set -eu

required_env() {
  name="$1"
  case "$name" in
    VELORIX_* | AWS_*) ;;
    *)
      echo "invalid environment variable name: ${name}" >&2
      exit 64
      ;;
  esac
  value="$(eval "printf '%s' \"\${${name}:-}\"")"
  if [ -z "$value" ]; then
    echo "missing required environment variable: ${name}" >&2
    exit 64
  fi
}

if [ "$#" -gt 0 ]; then
  case "$1" in
    lease-guarded-append | probe-lease-guarded-append | probe-kubernetes-lease-acquire | probe-ingest-admission-crash-restart | probe-lease-loss-during-reservation | -h | --help | help)
      exec velorix-ingest-writer "$@"
      ;;
    append | probe-kubernetes-lease-handoff | lease-handoff-probe)
      if [ "${VELORIX_ALLOW_DIAGNOSTIC_CLI:-0}" = "1" ]; then
        exec velorix-ingest-writer "$@"
      fi
      echo "diagnostic ingest-writer command requires VELORIX_ALLOW_DIAGNOSTIC_CLI=1: $1" >&2
      exit 64
      ;;
    *)
      echo "unsupported ingest-writer command for product image: $1" >&2
      exit 64
      ;;
  esac
fi

required_env VELORIX_INGEST_WRITER_AUTHORITY_STORE_ID
required_env VELORIX_INGEST_WRITER_AUTHORITY_NAMESPACE
required_env VELORIX_INGEST_WRITER_OPERATOR_ID
required_env VELORIX_INGEST_WRITER_ID
required_env VELORIX_INGEST_WRITER_PAYLOAD_FILE
required_env VELORIX_INGEST_WRITER_NAMESPACE
required_env VELORIX_INGEST_WRITER_LEASE_VIEW_ID
required_env VELORIX_INGEST_WRITER_LEASE_STREAM_ID
required_env VELORIX_INGEST_WRITER_LEASE_PARTITION_ID
required_env VELORIX_INGEST_WRITER_LEASE_OWNER_ID
required_env VELORIX_S3_COMPAT
required_env AWS_ENDPOINT_URL
required_env AWS_ACCESS_KEY_ID
required_env AWS_SECRET_ACCESS_KEY
required_env AWS_REGION
required_env VELORIX_S3_BUCKET

if [ ! -f "$VELORIX_INGEST_WRITER_PAYLOAD_FILE" ]; then
  echo "payload file does not exist or is not a regular file: ${VELORIX_INGEST_WRITER_PAYLOAD_FILE}" >&2
  exit 66
fi

if [ ! -r "$VELORIX_INGEST_WRITER_PAYLOAD_FILE" ]; then
  echo "payload file is not readable: ${VELORIX_INGEST_WRITER_PAYLOAD_FILE}" >&2
  exit 66
fi

lease_ttl_ms="${VELORIX_INGEST_WRITER_LEASE_TTL_MS:-60000}"

exec velorix-ingest-writer lease-guarded-append \
  --payload-file "$VELORIX_INGEST_WRITER_PAYLOAD_FILE" \
  --authority-store-id "$VELORIX_INGEST_WRITER_AUTHORITY_STORE_ID" \
  --authority-namespace "$VELORIX_INGEST_WRITER_AUTHORITY_NAMESPACE" \
  --operator-id "$VELORIX_INGEST_WRITER_OPERATOR_ID" \
  --writer-id "$VELORIX_INGEST_WRITER_ID" \
  --lease-namespace "$VELORIX_INGEST_WRITER_NAMESPACE" \
  --lease-view-id "$VELORIX_INGEST_WRITER_LEASE_VIEW_ID" \
  --lease-stream-id "$VELORIX_INGEST_WRITER_LEASE_STREAM_ID" \
  --lease-partition-id "$VELORIX_INGEST_WRITER_LEASE_PARTITION_ID" \
  --owner-id "$VELORIX_INGEST_WRITER_LEASE_OWNER_ID" \
  --ttl-ms "$lease_ttl_ms" \
  --acquire-lease \
  --expected-outcome appended \
  --json
