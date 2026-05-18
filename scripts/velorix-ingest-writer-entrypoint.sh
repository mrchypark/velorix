#!/usr/bin/env sh
set -eu

required_env() {
  name="$1"
  value="$(eval "printf '%s' \"\${${name}:-}\"")"
  if [ -z "$value" ]; then
    echo "missing required environment variable: ${name}" >&2
    exit 64
  fi
}

required_env VELORIX_INGEST_WRITER_AUTHORITY_STORE_ID
required_env VELORIX_INGEST_WRITER_AUTHORITY_NAMESPACE
required_env VELORIX_INGEST_WRITER_OPERATOR_ID
required_env VELORIX_INGEST_WRITER_ID
required_env VELORIX_INGEST_WRITER_PAYLOAD_FILE
required_env VELORIX_S3_COMPAT
required_env AWS_ENDPOINT_URL
required_env AWS_ACCESS_KEY_ID
required_env AWS_SECRET_ACCESS_KEY
required_env AWS_REGION
required_env VELORIX_S3_BUCKET

exec velorix-cli ingest-writer-append \
  --payload-file "$VELORIX_INGEST_WRITER_PAYLOAD_FILE" \
  --authority-store-id "$VELORIX_INGEST_WRITER_AUTHORITY_STORE_ID" \
  --authority-namespace "$VELORIX_INGEST_WRITER_AUTHORITY_NAMESPACE" \
  --operator-id "$VELORIX_INGEST_WRITER_OPERATOR_ID" \
  --writer-id "$VELORIX_INGEST_WRITER_ID" \
  --json
