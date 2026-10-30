#!/bin/sh
set -eu

repo_root=$(CDPATH= cd "$(dirname "$0")/.." && pwd)
cd "$repo_root"

checksum_file=third_party/SHA256SUMS
vendor_dirs='third_party/arrow-ipc-59.3.0
third_party/parquet-59.3.0
third_party/foyer-storage-0.22.6'

for dir in $vendor_dirs; do
    if [ ! -d "$dir" ]; then
        printf 'missing vendored source directory: %s\n' "$dir" >&2
        exit 1
    fi
done

if [ -n "$(find $vendor_dirs -type l -print)" ]; then
    printf 'vendored source payloads must not contain symlinks\n' >&2
    exit 1
fi

actual_files=$(find $vendor_dirs -type f -print | LC_ALL=C sort)
listed_files=$(awk 'NF == 2 { print $2 }' "$checksum_file" | LC_ALL=C sort)
if [ "$actual_files" != "$listed_files" ]; then
    printf 'vendor file inventory does not match %s\n' "$checksum_file" >&2
    exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -c "$checksum_file"
elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 -c "$checksum_file"
else
    printf 'need sha256sum or shasum to verify vendored source\n' >&2
    exit 2
fi
