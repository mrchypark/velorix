#!/bin/sh
set -eu

test "$#" -eq 1 || { echo "usage: $0 DESTINATION" >&2; exit 2; }
case "$(uname -s)/$(uname -m)" in
  Linux/x86_64)
    target=x86_64-unknown-linux-gnu
    sha=6c9ddf8f191fc4fc26df824fa142d37d8693fb311ac02e71f815c68e9b82660e ;;
  Linux/aarch64)
    target=aarch64-unknown-linux-gnu
    sha=0b0151b5eab35ed59224212e3f6ba9a272306d86a49c5980ca2c0a1f237fbf33 ;;
  *) echo "this archive pin requires Linux AMD64 or ARM64" >&2; exit 1 ;;
esac
mkdir -p "$1"
test ! -e "$1/librhiza_ffi.a"
archive=$(mktemp "$1/.rhiza.XXXXXX")
trap 'unlink "$archive"' EXIT HUP INT TERM
curl --fail --silent --show-error --location --retry 2 --connect-timeout 15 --max-time 300 \
  "https://github.com/mrchypark/rhiza/releases/download/v0.27.5/librhiza_ffi-0.27.5-$target.a" \
  -o "$archive"
printf '%s  %s\n' "$sha" "$archive" | sha256sum --check --strict
mv "$archive" "$1/librhiza_ffi.a"
trap - EXIT HUP INT TERM
