#!/bin/sh
set -eu

# Only the Linux AMD64 image/CI target; macOS fixtures verify their own pair.
test "$#" -eq 1 || { echo "usage: $0 DESTINATION" >&2; exit 2; }
test "$(uname -s)/$(uname -m)" = Linux/x86_64 || {
  echo "this archive pin requires Linux AMD64" >&2; exit 1;
}
mkdir -p "$1"
test ! -e "$1/librhiza_ffi.a"
archive=$(mktemp "$1/.rhiza.XXXXXX")
trap 'unlink "$archive"' EXIT HUP INT TERM
curl --fail --silent --show-error --location --retry 2 --connect-timeout 15 --max-time 300 \
  https://github.com/mrchypark/rhiza/releases/download/v0.27.4/librhiza_ffi-0.27.4-x86_64-unknown-linux-gnu.a \
  -o "$archive"
printf '%s  %s\n' bb669dcde5d9db83c6ade8c8496cf55c03470f5b8222df44220501683728cdf1 "$archive" | sha256sum --check --strict
mv "$archive" "$1/librhiza_ffi.a"
trap - EXIT HUP INT TERM
