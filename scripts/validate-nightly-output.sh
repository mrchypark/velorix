#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: validate-nightly-output.sh OUTPUT_NAME OUTPUT_VALUE" >&2
  exit 2
fi

output_name=$1
output_value=$2
: "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"

case "$output_name" in
  ''|*[!A-Za-z0-9_]* )
    echo "invalid GitHub output name" >&2
    exit 1
    ;;
esac

# Environment variables and shell arguments cannot contain NUL bytes. The
# explicit newline/CR case catches line-breaking payloads, while the C-locale
# control-byte check rejects every other representable unsafe byte.
case "$output_value" in
  *$'\n'*|*$'\r'*)
    echo "$output_name contains a newline or carriage return" >&2
    exit 1
    ;;
esac
if LC_ALL=C printf '%s' "$output_value" | LC_ALL=C grep -q '[[:cntrl:]]'; then
  echo "$output_name contains a NUL or control character" >&2
  exit 1
fi

printf '%s=%s\n' "$output_name" "$output_value" >> "$GITHUB_OUTPUT"
