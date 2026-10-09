#!/bin/sh
# Contract strings intentionally remain literal.
# shellcheck disable=SC2016
set -eu

repo_root=$(git rev-parse --show-toplevel)
script_path=$repo_root/scripts/run-versitygw-s3-gate.sh
doc_path=$repo_root/docs/release/1.0-readiness-checklist.md
status_path=$repo_root/docs/architecture/production-readiness-status.md
benchmark_path=$repo_root/docs/architecture/benchmark-gate-v1.md

require_text() {
    file=$1
    text=$2
    if ! grep -F -- "$text" "$file" >/dev/null; then
        echo "Versity Gateway S3 gate contract check failed: $file lacks: $text" >&2
        exit 1
    fi
}

require_text "$script_path" 'production_gc_seed_path="${VELORIX_VERSITYGW_PRODUCTION_GC_SEED_PATH:-'
require_text "$script_path" 'production_gc_run_path="${VELORIX_VERSITYGW_PRODUCTION_GC_RUN_PATH:-'
require_text "$script_path" 'production_gc_validation_path="${VELORIX_VERSITYGW_PRODUCTION_GC_VALIDATION_PATH:-'
require_text "$script_path" 'run_production_gc_evidence="${VELORIX_VERSITYGW_RUN_PRODUCTION_GC_EVIDENCE:-0}"'
require_text "$script_path" 'Versity Gateway production GC is unavailable: durable cross-process coordinator is required'
require_text "$script_path" 'exit 75'
require_text "$script_path" 'cargo test -p velorix-storage --test s3_compat --features s3-compat-tests'
require_text "$script_path" 'cargo test -p velorix-storage --test multi_process_ingest_admission --features s3-compat-tests'
require_text "$script_path" 'export AWS_ACCESS_KEY_ID="$versitygw_access_key"'
require_text "$script_path" 'export AWS_SECRET_ACCESS_KEY="$versitygw_secret_key"'
require_text "$script_path" 'docker run'
require_text "$script_path" 'docker rm -f'
require_text "$script_path" 's3_compatible_gc_execution_unavailable'
require_text "$script_path" 'image="${VELORIX_VERSITYGW_IMAGE:-versity/versitygw:v1.8.0}"'
require_text "$script_path" '-e ROOT_ACCESS_KEY="$versitygw_access_key"'
require_text "$script_path" '-e ROOT_SECRET_KEY="$versitygw_secret_key"'
require_text "$script_path" '--port :9000 posix /data'
require_text "$script_path" 'export VELORIX_S3_COMPAT=1'
require_text "$script_path" 'evidence_kind: "versitygw_s3_compatible_gate"'
if grep -E 'python|RUSTFS|rustfs|VERSITYGW_ADDRESS' "$script_path" >/dev/null; then
    echo "Versity Gateway S3 gate contract check failed: obsolete runtime or backend contract" >&2
    exit 1
fi
require_text "$doc_path" 'blocked pending a durable cross-process'
require_text "$doc_path" 'cannot create a live run'
require_text "$status_path" 'currently blocked'
require_text "$status_path" 'no live `GcRunV1` deletion evidence is claimed'
require_text "$benchmark_path" 'gc_execution_denied'
require_text "$benchmark_path" 'must not be presented as deletion, retention, or'

if grep -F 'run_production_gc_evidence" = "2"' "$script_path" >/dev/null; then
    echo "Versity Gateway S3 gate contract check failed: hidden GC execution mode remains" >&2
    exit 1
fi
if grep -F 'can create the live `GcRunV1`' "$status_path" >/dev/null; then
    echo "Versity Gateway S3 gate contract check failed: stale live-GC status claim" >&2
    exit 1
fi

# Exercise launch/evidence and fail-closed behavior without Docker or Rust builds.
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT HUP INT TERM
mkdir -p "$fixture/bin"
cat > "$fixture/bin/docker" <<'SH'
#!/bin/sh
printf '%s\n' "$*" >> "$GATE_FIXTURE/docker.log"
case "$*" in
    'context show') echo fixture ;;
    'container inspect --format '*) echo fixture-image-id ;;
    'image inspect --format '*) printf '%s\n' "$GATE_FIXTURE_REPO_DIGESTS" ;;
    'volume inspect --format '*) printf '%s\n' "${GATE_FIXTURE_VOLUME_LABEL:-}" ;;
    'volume inspect '*) test "${GATE_FIXTURE_VOLUME_EXISTS:-0}" = 1 ;;
    'container inspect '* | 'network inspect '*) exit 1 ;;
    'version '*) echo fixture-server ;;
esac
SH
cat > "$fixture/bin/cargo" <<'SH'
#!/bin/sh
set -eu
test "$VELORIX_S3_COMPAT" = 1
test "$AWS_ENDPOINT_URL" = http://127.0.0.1:9000
test "$AWS_ACCESS_KEY_ID" = fixture-access
test "$AWS_SECRET_ACCESS_KEY" = fixture-secret
printf '%s\n' "$*" >> "$GATE_FIXTURE/cargo.log"
exit "${GATE_FIXTURE_CARGO_EXIT:-0}"
SH
chmod +x "$fixture/bin/docker" "$fixture/bin/cargo"
export GATE_FIXTURE="$fixture"
fixture_digest="sha256:$(printf '%064d' 1)"
export GATE_FIXTURE_REPO_DIGESTS="[\"versity/versitygw@$fixture_digest\"]"
export GATE_FIXTURE_VOLUME_EXISTS=0
export VELORIX_VERSITYGW_VOLUME=fixture-volume
export PATH="$fixture/bin:$PATH"
export VELORIX_VERSITYGW_IMAGE=versity/versitygw:v1.8.0
export VELORIX_ALLOW_MUTABLE_VERSITYGW_IMAGE=0
export VELORIX_AWS_CLI_IMAGE=amazon/aws-cli:2.17.36
export VELORIX_ALLOW_MUTABLE_AWS_CLI_IMAGE=0
export VELORIX_VERSITYGW_ACCESS_KEY=fixture-access
export VELORIX_VERSITYGW_SECRET_KEY=fixture-secret
export VELORIX_VERSITYGW_PORT=9000
export VELORIX_VERSITYGW_CLEANUP=1
export VELORIX_VERSITYGW_MIN_FREE_KIB=1
export VELORIX_VERSITYGW_CARGO_TARGET_DIR="$fixture/target"
export VELORIX_VERSITYGW_EVIDENCE_PATH="$fixture/evidence.json"
export VELORIX_VERSITYGW_PRODUCTION_GC_SEED_PATH="$fixture/seed.json"
export VELORIX_VERSITYGW_PRODUCTION_GC_RUN_PATH="$fixture/run.json"
export VELORIX_VERSITYGW_PRODUCTION_GC_PATH="$fixture/gc.json"
export VELORIX_VERSITYGW_PRODUCTION_GC_VALIDATION_PATH="$fixture/validation.json"
export VELORIX_VERSITYGW_PRODUCTION_GC_RETAIN_LATEST_MANIFESTS=1
export VELORIX_VERSITYGW_RUN_PRODUCTION_GC_EVIDENCE=0
export VELORIX_S3_PREFIX='fixture/quote"/backslash\suffix'
bash "$script_path" > "$fixture/output.log" 2>&1
require_text "$fixture/docker.log" '-p 9000:9000 -e ROOT_ACCESS_KEY=fixture-access -e ROOT_SECRET_KEY=fixture-secret'
require_text "$fixture/docker.log" 'versity/versitygw:v1.8.0 --port :9000 posix /data'
require_text "$fixture/docker.log" 'volume create --label velorix.dev/object-store=versitygw-posix fixture-volume'
require_text "$fixture/docker.log" '-v fixture-volume:/data'
require_text "$fixture/cargo.log" 'test -p velorix-storage --test s3_compat --features s3-compat-tests -- --nocapture --test-threads=1'
require_text "$fixture/cargo.log" 'test -p velorix-storage --test multi_process_ingest_admission --features s3-compat-tests -- --nocapture --test-threads=1'
jq -e --arg prefix "$VELORIX_S3_PREFIX" --arg digest "$fixture_digest" '
    .evidence_kind == "versitygw_s3_compatible_gate" and
    .provider == "versitygw" and .versitygw_image_digest == $digest and
    .versitygw_image == ("versity/versitygw@" + $digest) and
    .prefix == $prefix and .credentials_redacted == true and
    .benchmark == {ran: false, result_path: null, validation: null} and
    (has("production_gc_artifact") | not) and (has("rustfs_image") | not)
' "$fixture/evidence.json" >/dev/null
if grep -F fixture-secret "$fixture/evidence.json" >/dev/null; then
    echo "gate leaked credentials" >&2
    exit 1
fi
expect_exit() {
    expected=$1
    shift
    actual=0
    "$@" > "$fixture/output.log" 2>&1 || actual=$?
    if [ "$actual" != "$expected" ]; then
        cat "$fixture/output.log" >&2
        echo "gate exit $actual; expected $expected" >&2
        exit 1
    fi
}
expect_exit 23 env GATE_FIXTURE_CARGO_EXIT=23 bash "$script_path"
test ! -f "$fixture/evidence.json"
require_text "$fixture/docker.log" "{{json .RepoDigests}} fixture-image-id"
for digests in '[]' '["rustfs/rustfs@sha256:0000000000000000000000000000000000000000000000000000000000000001"]' '["versity/versitygw@sha256:invalid"]'; do
    expect_exit 1 env GATE_FIXTURE_REPO_DIGESTS="$digests" bash "$script_path"
    test ! -f "$fixture/evidence.json"
done
for image in versity/versitygw versity/versitygw:latest versity/versitygw:beta localhost:5000/versity/versitygw; do
    expect_exit 64 env VELORIX_VERSITYGW_IMAGE="$image" bash "$script_path"
done
expect_exit 75 env VELORIX_VERSITYGW_RUN_PRODUCTION_GC_EVIDENCE=1 bash "$script_path"
expect_exit 64 env VELORIX_VERSITYGW_RUN_PRODUCTION_GC_EVIDENCE=2 bash "$script_path"
expect_exit 0 env VELORIX_VERSITYGW_IMAGE=localhost:5000/versity/versitygw:v1.8.0 bash "$script_path"
expect_exit 0 env VELORIX_VERSITYGW_IMAGE=versity/versitygw:v1.8.0@sha256:30292fc2eeacc67a36993b01f7a7a5e3361a19cced0e80c1d71cfa2a4b0a2499 bash "$script_path"
expect_exit 0 env VELORIX_VERSITYGW_IMAGE=versity/versitygw:latest VELORIX_ALLOW_MUTABLE_VERSITYGW_IMAGE=1 bash "$script_path"
for label in '' rustfs; do
    : > "$fixture/docker.log"
    expect_exit 64 env GATE_FIXTURE_VOLUME_EXISTS=1 GATE_FIXTURE_VOLUME_LABEL="$label" bash "$script_path"
    require_text "$fixture/output.log" 'requires label velorix.dev/object-store=versitygw-posix'
    if grep -E '^(run -d|volume (create|rm)) ' "$fixture/docker.log" >/dev/null; then
        echo "gate launched or changed an incompatible reused volume" >&2
        exit 1
    fi
done
: > "$fixture/docker.log"
expect_exit 0 env GATE_FIXTURE_VOLUME_EXISTS=1 GATE_FIXTURE_VOLUME_LABEL=versitygw-posix bash "$script_path"
require_text "$fixture/docker.log" 'versity/versitygw:v1.8.0 --port :9000 posix /data'
if grep -E '^volume (create|rm) ' "$fixture/docker.log" >/dev/null; then
    echo "gate created or deleted a reused POSIX volume" >&2
    exit 1
fi

echo "Versity Gateway S3 gate contract check passed"
