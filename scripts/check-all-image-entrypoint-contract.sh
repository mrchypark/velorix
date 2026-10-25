#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
entrypoint="${repo_root}/scripts/velorix-all-in-one-entrypoint.sh"
test_root=$(mktemp -d "${TMPDIR:-/tmp}/velorix-all-entrypoint.XXXXXX")
test_bin="${test_root}/bin"
test_log="${test_root}/events.log"
supervisor_pid=
poll_pid=

cleanup_pidfile() {
  pid_file="$1"
  if [ -f "$pid_file" ]; then
    IFS= read -r process_id < "$pid_file" || :
    if [ -n "${process_id:-}" ]; then
      kill -TERM "$process_id" 2>/dev/null || :
      i=0
      while kill -0 "$process_id" 2>/dev/null && [ "$i" -lt 5 ]; do
        sleep 1 &
        poll_pid=$!
        wait "$poll_pid" 2>/dev/null || :
        poll_pid=
        i=$((i + 1))
      done
      if kill -0 "$process_id" 2>/dev/null; then
        kill -KILL "$process_id" 2>/dev/null || :
      fi
    fi
  fi
}

cleanup() {
  trap - 0
  trap '' HUP INT TERM
  if [ -n "$poll_pid" ]; then
    kill -TERM "$poll_pid" 2>/dev/null || :
    wait "$poll_pid" 2>/dev/null || :
    poll_pid=
  fi
  if [ -n "$supervisor_pid" ]; then
    kill -TERM "$supervisor_pid" 2>/dev/null || :
    i=0
    while kill -0 "$supervisor_pid" 2>/dev/null && [ "$i" -lt 5 ]; do
      sleep 1 &
      poll_pid=$!
      wait "$poll_pid" 2>/dev/null || :
      poll_pid=
      i=$((i + 1))
    done
    if kill -0 "$supervisor_pid" 2>/dev/null; then
      kill -KILL "$supervisor_pid" 2>/dev/null || :
    fi
    wait "$supervisor_pid" 2>/dev/null || :
    supervisor_pid=
  fi
  cleanup_pidfile "${test_root}/api.pid"
  cleanup_pidfile "${test_root}/meta.pid"
  cleanup_pidfile "${test_root}/api-child.pid"
  cleanup_pidfile "${test_root}/meta-child.pid"
  rm -rf "$test_root"
}

trap cleanup 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

fail() {
  echo "all-image entrypoint contract failed: $*" >&2
  if [ -f "$test_log" ]; then
    cat "$test_log" >&2
  fi
  exit 1
}

wait_for_event() {
  expected_event="$1"
  i=0
  while ! grep -Fqx "$expected_event" "$test_log" 2>/dev/null; do
    [ "$i" -lt 10 ] || fail "timed out waiting for ${expected_event}"
    sleep 1 &
    poll_pid=$!
    wait "$poll_pid" 2>/dev/null || :
    poll_pid=
    i=$((i + 1))
  done
}

wait_for_supervisor_exit() {
  i=0
  while kill -0 "$supervisor_pid" 2>/dev/null; do
    [ "$i" -lt 10 ] || fail "supervisor did not exit"
    sleep 1 &
    poll_pid=$!
    wait "$poll_pid" 2>/dev/null || :
    poll_pid=
    i=$((i + 1))
  done
}

assert_supervisor_status() {
  expected_status="$1"
  if wait "$supervisor_pid"; then
    actual_status=0
  else
    actual_status=$?
  fi
  supervisor_pid=
  [ "$actual_status" -eq "$expected_status" ] \
    || fail "expected supervisor status ${expected_status}, got ${actual_status}"
}

assert_pidfile_stopped() {
  pid_file="$1"
  if [ -f "$pid_file" ]; then
    IFS= read -r process_id < "$pid_file" || :
    if [ -n "${process_id:-}" ] && kill -0 "$process_id" 2>/dev/null; then
      fail "fixture process ${process_id} remained alive after cleanup"
    fi
  fi
}

clean_fixture_processes() {
  cleanup_pidfile "${test_root}/api.pid"
  cleanup_pidfile "${test_root}/meta.pid"
  cleanup_pidfile "${test_root}/smoke.pid"
  cleanup_pidfile "${test_root}/api-child.pid"
  cleanup_pidfile "${test_root}/meta-child.pid"
  assert_pidfile_stopped "${test_root}/api.pid"
  assert_pidfile_stopped "${test_root}/meta.pid"
  assert_pidfile_stopped "${test_root}/smoke.pid"
  assert_pidfile_stopped "${test_root}/api-child.pid"
  assert_pidfile_stopped "${test_root}/meta-child.pid"
  rm -f "${test_root}/api.pid" "${test_root}/meta.pid" "${test_root}/smoke.pid" \
    "${test_root}/api-child.pid" "${test_root}/meta-child.pid"
}

mkdir -p "$test_bin"
cat > "${test_bin}/velorix-meta" <<'EOF'
#!/bin/sh
set -eu
test_root="$TEST_ROOT"
record() { printf '%s\n' "$1" >> "$TEST_LOG"; }
child_pid=
stop() {
  record meta-term
  if [ -n "$child_pid" ]; then
    kill -TERM "$child_pid" 2>/dev/null || :
    wait "$child_pid" 2>/dev/null || :
    child_pid=
  fi
  exit 0
}
stop_smoke() {
  record smoke-term
  if [ -n "$child_pid" ]; then
    kill -TERM "$child_pid" 2>/dev/null || :
    wait "$child_pid" 2>/dev/null || :
    child_pid=
  fi
  exit 0
}
ignore_term() {
  record meta-term-ignored
  if [ -n "$child_pid" ]; then
    kill -TERM "$child_pid" 2>/dev/null || :
    wait "$child_pid" 2>/dev/null || :
    child_pid=
  fi
}
case "${1:-}" in
  --contract-direct)
    record meta-direct
    exit 0
    ;;
  serve)
    record meta-start
    printf '%s\n' "$$" > "${test_root}/meta.pid"
    if [ "${TEST_META_IGNORE_TERM:-0}" = 1 ]; then
      trap ignore_term TERM
    else
      trap stop TERM
    fi
    trap stop INT HUP
    if [ "${TEST_META_EXIT:-0}" = 1 ]; then
      while [ ! -f "${test_root}/api.pid" ]; do
        sleep 1 &
        child_pid=$!
        printf '%s\n' "$child_pid" > "${test_root}/meta-child.pid"
        wait "$child_pid" || :
        child_pid=
      done
      record meta-exit-17
      exit 17
    fi
    while :; do
      sleep 1 &
      child_pid=$!
      printf '%s\n' "$child_pid" > "${test_root}/meta-child.pid"
      wait "$child_pid" || :
      child_pid=
    done
    ;;
  smoke)
    [ "${VELORIX_META_BEARER_TOKEN:-}" = contract-test-token ] || exit 31
    [ "${VELORIX_META_TRANSPORT_SECURITY:-}" = native-mtls ] || exit 32
    [ "${VELORIX_META_GRPC_ENDPOINT:-}" = https://127.0.0.1:19090 ] || exit 33
    [ -n "${VELORIX_META_TLS_CA_FILE:-}" ] || exit 34
    [ -n "${VELORIX_META_TLS_CLIENT_CERT_FILE:-}" ] || exit 35
    [ -n "${VELORIX_META_TLS_CLIENT_KEY_FILE:-}" ] || exit 36
    [ -n "${VELORIX_META_TLS_DOMAIN_NAME:-}" ] || exit 37
    shift
    expect_backend=0
    expect_auth=0
    expect_multi_writer=0
    capabilities_only=0
    while [ "$#" -gt 0 ]; do
      argument="$1"
      [ "$argument" != contract-test-token ] || exit 35
      case "$argument" in
        --capabilities-only) capabilities_only=1 ;;
        --expect-backend)
          [ "${2:-}" = rhiza-kv ] || exit 38
          expect_backend=1
          shift
          ;;
        --expect-auth-enforced)
          [ "${2:-}" = true ] || exit 39
          expect_auth=1
          shift
          ;;
        --expect-production-multi-writer-safe)
          [ "${2:-}" = false ] || exit 40
          expect_multi_writer=1
          shift
          ;;
        *) exit 42 ;;
      esac
      shift
    done
    [ "$capabilities_only" -eq 1 ] && [ "$expect_backend" -eq 1 ] \
      && [ "$expect_auth" -eq 1 ] && [ "$expect_multi_writer" -eq 1 ] || exit 43
    record smoke-capabilities
    if [ "${TEST_SMOKE_BLOCK:-0}" = 1 ]; then
      printf '%s\n' "$$" > "${test_root}/smoke.pid"
      record smoke-waiting
      trap stop_smoke TERM INT HUP
      while :; do
        sleep 30 &
        child_pid=$!
        printf '%s\n' "$child_pid" > "${test_root}/meta-child.pid"
        wait "$child_pid" || :
        child_pid=
      done
    fi
    if [ "${TEST_SMOKE_EXIT:-0}" = 1 ]; then
      exit 9
    fi
    ;;
  *)
    exit 36
    ;;
esac
EOF
cat > "${test_bin}/velorix-api" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "api-invoked-role-${VELORIX_SERVICE_ROLE:-missing}" >> "$TEST_LOG"
test_root="$TEST_ROOT"
record() { printf '%s\n' "$1" >> "$TEST_LOG"; }
child_pid=
stop() {
  record api-term
  if [ -n "$child_pid" ]; then
    kill -TERM "$child_pid" 2>/dev/null || :
    wait "$child_pid" 2>/dev/null || :
    child_pid=
  fi
  exit 0
}
ignore_term() {
  record api-term-ignored
  if [ -n "$child_pid" ]; then
    kill -TERM "$child_pid" 2>/dev/null || :
    wait "$child_pid" 2>/dev/null || :
    child_pid=
  fi
}
if [ "${1:-}" = --contract-direct ]; then
  record api-direct
  exit 0
fi
[ "${VELORIX_SERVICE_ROLE:-}" = all ] || exit 41
record api-start
printf '%s\n' "$$" > "${test_root}/api.pid"
if [ "${TEST_API_EXIT:-0}" = 1 ]; then
  exit 23
fi
if [ "${TEST_API_IGNORE_TERM:-0}" = 1 ]; then
  trap ignore_term TERM
else
  trap stop TERM
fi
trap stop INT HUP
while :; do
  sleep 1 &
  child_pid=$!
  printf '%s\n' "$child_pid" > "${test_root}/api-child.pid"
  wait "$child_pid" || :
  child_pid=
done
EOF
cat > "${test_bin}/velorix-ingest-writer-entrypoint" <<'EOF'
#!/bin/sh
set -eu
[ "${1:-}" = --contract-direct ] || exit 51
printf '%s\n' ingest-writer-direct >> "$TEST_LOG"
EOF
chmod 0755 "${test_bin}/velorix-meta" "${test_bin}/velorix-api" \
  "${test_bin}/velorix-ingest-writer-entrypoint"

export TEST_ROOT="$test_root" TEST_LOG="$test_log"
export PATH="${test_bin}:${PATH}"
export VELORIX_META_MODE=development
export VELORIX_META_BACKEND=rhiza-kv
export VELORIX_META_BIND=127.0.0.1:19090
export VELORIX_META_GRPC_ENDPOINT=https://127.0.0.1:19090
export VELORIX_META_BEARER_TOKEN=contract-test-token
export VELORIX_META_TRANSPORT_SECURITY=native-mtls
export VELORIX_META_TLS_CA_FILE=/contract/client-ca.pem
export VELORIX_META_TLS_CLIENT_CERT_FILE=/contract/client-cert.pem
export VELORIX_META_TLS_CLIENT_KEY_FILE=/contract/client-key.pem
export VELORIX_META_TLS_DOMAIN_NAME=meta.contract.test
unset VELORIX_IMAGE_MODE TEST_META_EXIT TEST_API_EXIT TEST_SMOKE_EXIT \
  TEST_META_IGNORE_TERM TEST_API_IGNORE_TERM TEST_SMOKE_BLOCK || :

"$entrypoint" api --contract-direct
"$entrypoint" meta --contract-direct
"$entrypoint" ingest-writer --contract-direct
grep -Fqx api-direct "$test_log" || fail "api compatibility path was not execed"
grep -Fqx meta-direct "$test_log" || fail "meta compatibility path was not execed"
grep -Fqx ingest-writer-direct "$test_log" || fail "ingest-writer compatibility path was not execed"

: > "$test_log"
saved_meta_endpoint="$VELORIX_META_GRPC_ENDPOINT"
export VELORIX_META_GRPC_ENDPOINT=https://127.0.0.1:19091
if "$entrypoint" all >/dev/null 2>&1; then
  fail "all mode accepted a Meta endpoint on the wrong port"
fi
if grep -Fq meta-start "$test_log"; then
  fail "all mode started Meta before checking endpoint consistency"
fi
export VELORIX_META_GRPC_ENDPOINT="$saved_meta_endpoint"

: > "$test_log"
printf '%s\n' case-default >> "$test_log"
"$entrypoint" &
supervisor_pid=$!
wait_for_event smoke-capabilities
wait_for_event api-start
awk '
  $0 == "meta-start" { meta = NR }
  $0 == "smoke-capabilities" { smoke = NR }
  $0 == "api-start" { api = NR }
  END { exit !(meta > 0 && smoke > 0 && api > 0 && meta < api && smoke < api) }
' "$test_log" || fail "default mode did not gate API startup on Meta capabilities"
kill -TERM "$supervisor_pid"
wait_for_event api-term
wait_for_event meta-term
wait_for_supervisor_exit
assert_supervisor_status 143
clean_fixture_processes

: > "$test_log"
printf '%s\n' case-meta-exit >> "$test_log"
TEST_META_EXIT=1
export TEST_META_EXIT
"$entrypoint" all &
supervisor_pid=$!
wait_for_event smoke-capabilities
wait_for_event api-start
wait_for_event api-term
wait_for_supervisor_exit
assert_supervisor_status 17
grep -Fqx meta-exit-17 "$test_log" || fail "Meta exit fixture did not exit"
grep -Fqx api-term "$test_log" || fail "Meta exit did not stop its API peer"
clean_fixture_processes
unset TEST_META_EXIT

: > "$test_log"
printf '%s\n' case-api-exit >> "$test_log"
TEST_API_EXIT=1
export TEST_API_EXIT
"$entrypoint" all &
supervisor_pid=$!
wait_for_event api-start
wait_for_event meta-term
wait_for_supervisor_exit
assert_supervisor_status 23
clean_fixture_processes
unset TEST_API_EXIT

: > "$test_log"
printf '%s\n' case-smoke-failure >> "$test_log"
TEST_SMOKE_EXIT=1
export TEST_SMOKE_EXIT
"$entrypoint" all &
supervisor_pid=$!
wait_for_event smoke-capabilities
wait_for_event meta-term
wait_for_supervisor_exit
assert_supervisor_status 9
if grep -Fq api-start "$test_log"; then
  fail "API started after Meta capability validation failed"
fi
clean_fixture_processes
unset TEST_SMOKE_EXIT

: > "$test_log"
printf '%s\n' case-smoke-signal >> "$test_log"
TEST_SMOKE_BLOCK=1
export TEST_SMOKE_BLOCK
"$entrypoint" all &
supervisor_pid=$!
wait_for_event smoke-waiting
kill -TERM "$supervisor_pid"
wait_for_event smoke-term
wait_for_event meta-term
wait_for_supervisor_exit
assert_supervisor_status 143
if grep -Fq api-start "$test_log"; then
  fail "API started before the in-flight readiness probe completed"
fi
clean_fixture_processes
unset TEST_SMOKE_BLOCK

: > "$test_log"
printf '%s\n' case-stubborn-api >> "$test_log"
TEST_API_IGNORE_TERM=1
export TEST_API_IGNORE_TERM
"$entrypoint" all &
supervisor_pid=$!
wait_for_event api-start
kill -TERM "$supervisor_pid"
wait_for_event api-term-ignored
kill -TERM "$supervisor_pid"
wait_for_event meta-term
wait_for_supervisor_exit
assert_supervisor_status 143
clean_fixture_processes
unset TEST_API_IGNORE_TERM

: > "$test_log"
printf '%s\n' case-stubborn-meta >> "$test_log"
TEST_META_IGNORE_TERM=1
export TEST_META_IGNORE_TERM
"$entrypoint" all &
supervisor_pid=$!
wait_for_event api-start
kill -TERM "$supervisor_pid"
wait_for_event api-term
wait_for_event meta-term-ignored
wait_for_supervisor_exit
assert_supervisor_status 143
clean_fixture_processes
unset TEST_META_IGNORE_TERM

echo "all-image entrypoint contract passed"
