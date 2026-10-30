#!/bin/sh
set -eu

# End-to-end recovery through separate real meta, API, and worker processes.
# Requires local cargo binaries and uses a private, pinned VersityGW fixture.

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
service_mode=${RHIZA_SERVICE_MODE:-cluster}
case "$service_mode" in
    cluster|local) ;;
    *) echo "unsupported RHIZA_SERVICE_MODE: $service_mode (expected cluster or local)" >&2; exit 2 ;;
esac
meta_mode=development
if [ "$service_mode" = local ]; then meta_mode=production; fi
gateway_commit=fd04bc1df2656298577b82667a4195c77f8c7563
run_id=$(date -u +%Y%m%dt%H%M%Sz)-$$
evidence_root="$repo_root/target/rhiza-service-recovery-evidence"
evidence_dir="$evidence_root/$run_id"
private_dir=$(mktemp -d "${TMPDIR:-/tmp}/velorix-rhiza-service.XXXXXX")
mkdir -p "$evidence_dir" "$evidence_dir/versity-data" "$evidence_dir/cache-1" "$evidence_dir/cache-2" "$evidence_dir/cache-3" "$evidence_dir/bin"
chmod 700 "$private_dir"
rhizadb_version=$(cargo tree -p velorix-meta --features rhiza-backend -i rhizadb | sed -n 's/^rhizadb v\([^ ]*\).*/\1/p' | head -n 1)
[ -n "$rhizadb_version" ] || { echo 'could not derive the resolved rhizadb version' >&2; exit 1; }

s3_port=${RHIZA_SERVICE_S3_PORT:-29103}
meta_port=${RHIZA_SERVICE_META_PORT:-29100}
rhiza_port=${RHIZA_SERVICE_RHIZA_PORT:-29101}
peer_port=${RHIZA_SERVICE_PEER_PORT:-29102}
api_port=${RHIZA_SERVICE_API_PORT:-29104}
worker_port=${RHIZA_SERVICE_WORKER_PORT:-29105}
bucket="velorix-service-$(date -u +%Y%m%d%H%M%S)-$$"
prefix="rhiza-service-$run_id"
cluster_id="velorix-service-$run_id"
access_key=$(openssl rand -hex 16)
secret_key=$(openssl rand -hex 32)
api_token=$(openssl rand -hex 32)
admin_token=$(openssl rand -hex 32)
meta_token=$(openssl rand -hex 32)

gateway_pid=
meta_pid=
api_pid=
worker_pid=
generation=0
meta_generation=0

stop_pid() {
    pid=$1
    [ -n "$pid" ] || return 0
    kill -TERM "$pid" 2>/dev/null || true
    i=0
    while kill -0 "$pid" 2>/dev/null && [ "$i" -lt 10 ]; do
        sleep 1
        i=$((i + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null || true; fi
    wait "$pid" 2>/dev/null || true
}

stop_local_meta() {
    pid=$meta_pid
    [ -n "$pid" ] || return 0
    log="$evidence_dir/meta-$meta_generation.log"
    status_file="$evidence_dir/meta-$meta_generation-stop.status"
    kill -TERM "$pid" 2>/dev/null || {
        echo "could not send SIGTERM to Local meta generation $meta_generation (pid $pid)" >&2
        return 1
    }
    waited=0
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 60 ]; do
        state=$(ps -p "$pid" -o stat= 2>/dev/null | tr -d ' ' || true)
        case "$state" in *Z*) break ;; esac
        sleep 1
        waited=$((waited + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
        state=$(ps -p "$pid" -o stat= 2>/dev/null | tr -d ' ' || true)
        case "$state" in *Z*) ;; *)
            printf 'exit=unknown\nforced_kill=false\nclose_diagnostic=missing\n' >"$status_file"
            echo "Local meta generation $meta_generation did not exit after SIGTERM; preserving its evidence" >&2
            return 1
        esac
    fi
    if wait "$pid"; then
        exit_status=0
    else
        exit_status=$?
    fi
    meta_pid=
    close_pattern='local Rhiza database closed'
    if [ "$exit_status" -ne 0 ] || ! rg -i -q "$close_pattern" "$log"; then
        printf 'exit=%s\nforced_kill=false\nclose_diagnostic=missing\n' \
            "$exit_status" \
            >"$status_file"
        echo "Local meta generation $meta_generation failed graceful close; inspect $log and $status_file" >&2
        return 1
    fi
    printf 'exit=0\nforced_kill=false\nclose_diagnostic=present\n' >"$status_file"
}

pid_running() {
    pid=$1
    kill -0 "$pid" 2>/dev/null || return 1
    state=$(ps -p "$pid" -o stat= 2>/dev/null | tr -d ' ' || true)
    case "$state" in *Z*) return 1 ;; esac
    return 0
}

remove_owned_evidence_dir() {
    path=$1
    case "$path" in
        "$evidence_dir"/*) ;;
        *) echo "refusing to remove path outside this run's evidence directory: $path" >&2; return 1 ;;
    esac
    [ -d "$path" ] || return 0
    rm -r "$path"
}

cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    stop_pid "$api_pid"
    stop_pid "$worker_pid"
    if [ "$service_mode" = local ]; then
        stop_local_meta || status=1
    else
        stop_pid "$meta_pid"
    fi
    stop_pid "$gateway_pid"
    if [ "$status" -eq 0 ]; then
        for owned_dir in \
            "$evidence_dir/versity-data" "$evidence_dir/rhiza" \
            "$evidence_dir/rhiza-before-emptydir-restore" \
            "$evidence_dir/rhiza-before-second-emptydir-restore" \
            "$evidence_dir/rhiza-init-failure" \
            "$evidence_dir/cache-1" "$evidence_dir/cache-2" "$evidence_dir/cache-3" \
            "$evidence_dir/bin"; do
            if ! remove_owned_evidence_dir "$owned_dir"; then status=1; fi
        done
        if [ "$status" -eq 0 ]; then
            printf '%s\n' 'owned Rhiza state, S3 fixture data, caches, and VersityGW binary removed after evidence capture' >"$evidence_dir/cleanup.txt"
        fi
    fi
    if [ -n "$private_dir" ] && [ -d "$private_dir" ]; then
        if ! rm -r "$private_dir"; then status=1; fi
    fi
    if [ "$status" -ne 0 ]; then
        printf 'recovery harness failed; evidence retained: %s\n' "$evidence_dir" >&2
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

for binary in "$repo_root/target/debug/velorix-api" "$repo_root/target/debug/velorix-meta"; do
    [ -x "$binary" ] || { echo "required built binary is missing: $binary" >&2; exit 1; }
done
shasum -a 256 "$repo_root/target/debug/velorix-api" "$repo_root/target/debug/velorix-meta" >"$evidence_dir/binary-sha256.txt"
cargo tree -p velorix-meta --features rhiza-backend -e features -i rhizadb >"$evidence_dir/rhizadb-feature-provenance.txt" 2>&1
rg -q 'velorix-meta feature "rhiza-backend"' "$evidence_dir/rhizadb-feature-provenance.txt" || {
    echo 'meta build feature provenance is missing rhiza-backend' >&2
    exit 1
}

ports="$s3_port $meta_port $api_port $worker_port"
if [ "$service_mode" = cluster ]; then ports="$ports $rhiza_port $peer_port"; fi
for port in $ports; do
    if nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
        echo "required loopback port already occupied: $port" >&2
        exit 1
    fi
done

GOBIN="$evidence_dir/bin" go install "github.com/versity/versitygw/cmd/versitygw@$gateway_commit" >"$evidence_dir/gateway-build.log" 2>&1
go version -m "$evidence_dir/bin/versitygw" >"$evidence_dir/fixture-versions.log"
ROOT_ACCESS_KEY_ID="$access_key" ROOT_SECRET_ACCESS_KEY="$secret_key" \
    "$evidence_dir/bin/versitygw" --port "127.0.0.1:$s3_port" --region us-east-1 posix "$evidence_dir/versity-data" >"$evidence_dir/gateway.log" 2>&1 &
gateway_pid=$!
endpoint="http://127.0.0.1:$s3_port"
s3() {
    curl --silent --show-error --max-time 15 --noproxy '*' --aws-sigv4 'aws:amz:us-east-1:s3' \
        --user "$access_key:$secret_key" "$@"
}
ready=0
i=0
while [ "$i" -lt 60 ]; do
    if s3 --max-time 1 --fail "$endpoint/" >/dev/null 2>&1; then ready=1; break; fi
    sleep 1
    i=$((i + 1))
done
[ "$ready" -eq 1 ] || { echo 'VersityGW did not become ready' >&2; exit 1; }
s3 --fail -X PUT "$endpoint/$bucket" >"$evidence_dir/bucket-create.log" 2>&1

# Generate loopback-only mTLS credentials following run-rhiza-kv-k8s-fixture.sh.
cat >"$private_dir/server.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
EOF
cat >"$private_dir/client.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
EOF
openssl genrsa -out "$private_dir/ca.key" 2048 2>"$private_dir/openssl.log"
openssl req -x509 -new -nodes -key "$private_dir/ca.key" -sha256 -days 2 -subj "/CN=velorix-local-$run_id" -out "$private_dir/ca.crt" 2>>"$private_dir/openssl.log"
openssl genrsa -out "$private_dir/server.key" 2048 2>>"$private_dir/openssl.log"
openssl req -new -key "$private_dir/server.key" -subj '/CN=localhost' -out "$private_dir/server.csr" 2>>"$private_dir/openssl.log"
openssl x509 -req -in "$private_dir/server.csr" -CA "$private_dir/ca.crt" -CAkey "$private_dir/ca.key" -CAcreateserial -out "$private_dir/server.crt" -days 2 -sha256 -extfile "$private_dir/server.ext" 2>>"$private_dir/openssl.log"
openssl genrsa -out "$private_dir/client.key" 2048 2>>"$private_dir/openssl.log"
openssl req -new -key "$private_dir/client.key" -subj "/CN=velorix-client-$run_id" -out "$private_dir/client.csr" 2>>"$private_dir/openssl.log"
openssl x509 -req -in "$private_dir/client.csr" -CA "$private_dir/ca.crt" -CAkey "$private_dir/ca.key" -CAcreateserial -out "$private_dir/client.crt" -days 2 -sha256 -extfile "$private_dir/client.ext" 2>>"$private_dir/openssl.log"
chmod 600 "$private_dir"/*

check_local_meta_config() {
    env \
        -u VELORIX_RHIZA_BIND_ADDR \
        -u VELORIX_RHIZA_PEER_ADDR \
        -u VELORIX_RHIZA_ADMIN_TOKEN \
        -u VELORIX_RHIZA_PEER_TOKEN \
        -u VELORIX_RHIZA_PEER_TOKEN_FILE \
        -u VELORIX_RHIZA_MEMBERS_JSON \
        -u VELORIX_RHIZA_MEMBERS_FILE \
        -u VELORIX_RHIZA_PEER_TOKENS \
        -u VELORIX_RHIZA_MODE \
        VELORIX_META_MODE=production \
        VELORIX_META_BACKEND=rhiza-kv \
        VELORIX_META_BIND="127.0.0.1:$meta_port" \
        VELORIX_META_BEARER_TOKEN="$meta_token" \
        VELORIX_META_TRANSPORT_SECURITY=native-mtls \
        VELORIX_META_TLS_CERT_FILE="$private_dir/server.crt" \
        VELORIX_META_TLS_KEY_FILE="$private_dir/server.key" \
        VELORIX_META_TLS_CLIENT_CA_FILE="$private_dir/ca.crt" \
        VELORIX_RHIZA_MODE=local \
        VELORIX_RHIZA_DATA_DIR="${RHIZA_LOCAL_INIT_DATA_DIR:-$evidence_dir/rhiza}" \
        VELORIX_RHIZA_NODE_ID=local-0 \
        VELORIX_RHIZA_CLUSTER_ID="$cluster_id" \
        VELORIX_RHIZA_OBJECT_STORE_PROVIDER=s3 \
        VELORIX_RHIZA_OBJECT_STORE_ENDPOINT="127.0.0.1:$s3_port" \
        VELORIX_RHIZA_OBJECT_STORE_BUCKET="${RHIZA_LOCAL_INIT_BUCKET:-$bucket}" \
        VELORIX_RHIZA_OBJECT_STORE_REGION=us-east-1 \
        VELORIX_RHIZA_OBJECT_STORE_PREFIX="$prefix/meta" \
        VELORIX_RHIZA_OBJECT_STORE_ACCESS_KEY="$access_key" \
        VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY="$secret_key" \
        VELORIX_RHIZA_OBJECT_STORE_INSECURE=1 \
        VELORIX_RHIZA_OBJECT_STORE_DURABILITY=before-ack \
        "$repo_root/target/debug/velorix-meta" check-config >"$evidence_dir/meta-config-check.log" 2>&1
    [ ! -e "$evidence_dir/rhiza" ] || {
        echo 'meta check-config unexpectedly created the Local Rhiza data directory' >&2
        return 1
    }
    printf '%s\n' 'configuration passed; Local Rhiza data directory remained absent' >"$evidence_dir/meta-config-check-result.txt"
    : >"$evidence_dir/local-meta-config-checked"
}

run_local_meta() {
    run_local_command=$1
    shift
    set -- env \
        -u VELORIX_RHIZA_BIND_ADDR \
        -u VELORIX_RHIZA_PEER_ADDR \
        -u VELORIX_RHIZA_MEMBERS_JSON \
        -u VELORIX_RHIZA_MEMBERS_FILE \
        -u VELORIX_RHIZA_PEER_TOKEN \
        -u VELORIX_RHIZA_PEER_TOKEN_FILE \
        -u VELORIX_RHIZA_PEER_TOKENS \
        -u VELORIX_RHIZA_MODE \
        VELORIX_META_MODE="$meta_mode" \
        VELORIX_META_BACKEND=rhiza-kv \
        VELORIX_META_BIND="127.0.0.1:$meta_port" \
        VELORIX_META_BEARER_TOKEN="$meta_token" \
        VELORIX_META_TRANSPORT_SECURITY=native-mtls \
        VELORIX_META_TLS_CERT_FILE="$private_dir/server.crt" \
        VELORIX_META_TLS_KEY_FILE="$private_dir/server.key" \
        VELORIX_META_TLS_CLIENT_CA_FILE="$private_dir/ca.crt" \
        VELORIX_RHIZA_MODE=local \
        VELORIX_RHIZA_DATA_DIR="${RHIZA_LOCAL_INIT_DATA_DIR:-$evidence_dir/rhiza}" \
        VELORIX_RHIZA_NODE_ID=local-0 \
        VELORIX_RHIZA_CLUSTER_ID="$cluster_id" \
        VELORIX_RHIZA_OBJECT_STORE_PROVIDER=s3 \
        VELORIX_RHIZA_OBJECT_STORE_ENDPOINT="127.0.0.1:$s3_port" \
        VELORIX_RHIZA_OBJECT_STORE_BUCKET="${RHIZA_LOCAL_INIT_BUCKET:-$bucket}" \
        VELORIX_RHIZA_OBJECT_STORE_REGION=us-east-1 \
        VELORIX_RHIZA_OBJECT_STORE_PREFIX="$prefix/meta" \
        VELORIX_RHIZA_OBJECT_STORE_ACCESS_KEY="$access_key" \
        VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY="$secret_key" \
        VELORIX_RHIZA_OBJECT_STORE_INSECURE=1 \
        VELORIX_RHIZA_OBJECT_STORE_DURABILITY=before-ack \
        "$repo_root/target/debug/velorix-meta" "$run_local_command" "$@"
    if [ "$run_local_command" = serve ]; then
        exec "$@"
    fi
    "$@"
}

init_local_meta() {
    label=$1
    if ! run_local_meta init-local >"$evidence_dir/init-local-$label.log" 2>&1; then
        printf 'init-local_exit=nonzero\ndata_dir_exists=%s\n' \
            "$([ -d "$evidence_dir/rhiza" ] && printf true || printf false)" \
            >"$evidence_dir/init-local-$label.status"
        cat "$evidence_dir/init-local-$label.log" >&2
        echo "init-local failed before meta generation $generation" >&2
        return 1
    fi
    [ -d "$evidence_dir/rhiza" ] || {
        echo "init-local succeeded without creating the shared Local data directory ($label)" >&2
        return 1
    }
    rg -F -q 'local Rhiza initialization completed and database closed' "$evidence_dir/init-local-$label.log" || {
        echo "init-local ($label) did not report successful database close" >&2
        return 1
    }
    printf 'init-local_exit=0\ndata_dir_exists=true\ndata_dir=%s\n' \
        "$evidence_dir/rhiza" >"$evidence_dir/init-local-$label.status"
}

check_local_init_storage_failure() {
    failure_dir="$evidence_dir/rhiza-init-failure"
    failure_bucket="velorix-missing-$run_id"
    mkdir "$failure_dir"
    if nc -z 127.0.0.1 "$meta_port" >/dev/null 2>&1; then
        echo "meta listener is already present before init-local failure probe on $meta_port" >&2
        return 1
    fi
    if RHIZA_LOCAL_INIT_DATA_DIR="$failure_dir" RHIZA_LOCAL_INIT_BUCKET="$failure_bucket" \
        run_local_meta init-local >"$evidence_dir/init-local-storage-failure.log" 2>&1; then
        echo 'init-local unexpectedly succeeded against the known nonexistent bucket' >&2
        return 1
    fi
    rg -i -q 'NoSuchBucket|bucket.*(not found|does not exist)|object.?store|S3' \
        "$evidence_dir/init-local-storage-failure.log" || {
        echo 'init-local failed without evidence of an object-storage open error' >&2
        return 1
    }
    if nc -z 127.0.0.1 "$meta_port" >/dev/null 2>&1; then
        echo 'failed init-local probe left a meta listener running' >&2
        return 1
    fi
    printf 'init-local_exit=nonzero\nmain_invoked=false\nmeta_listener_present=false\ndata_dir=%s\n' \
        "$failure_dir" >"$evidence_dir/init-local-storage-failure.status"
}

start_meta() {
    if [ "$service_mode" = local ] && [ ! -e "$evidence_dir/local-meta-config-checked" ]; then
        check_local_meta_config
    fi
    meta_generation=$generation
    if [ "$service_mode" = local ]; then
        if [ "$generation" -eq 0 ] && [ ! -e "$evidence_dir/init-local-storage-failure.status" ]; then
            check_local_init_storage_failure
        fi
        init_local_meta "$generation"
        run_local_meta serve >"$evidence_dir/meta-$generation.log" 2>&1 &
    else
        VELORIX_META_MODE=development \
        VELORIX_META_BACKEND=rhiza-kv \
        VELORIX_META_BIND="127.0.0.1:$meta_port" \
        VELORIX_META_BEARER_TOKEN="$meta_token" \
        VELORIX_META_TRANSPORT_SECURITY=native-mtls \
        VELORIX_META_TLS_CERT_FILE="$private_dir/server.crt" \
        VELORIX_META_TLS_KEY_FILE="$private_dir/server.key" \
        VELORIX_META_TLS_CLIENT_CA_FILE="$private_dir/ca.crt" \
        VELORIX_RHIZA_MODE=cluster \
        VELORIX_RHIZA_DATA_DIR="$evidence_dir/rhiza" \
        VELORIX_RHIZA_NODE_ID=local-0 \
        VELORIX_RHIZA_CLUSTER_ID="$cluster_id" \
        VELORIX_RHIZA_BIND_ADDR="127.0.0.1:$rhiza_port" \
        VELORIX_RHIZA_PEER_ADDR="127.0.0.1:$peer_port" \
        VELORIX_RHIZA_MEMBERS_JSON="[{\"node_id\":\"local-0\",\"url\":\"http://127.0.0.1:$rhiza_port\",\"peer_url\":\"quic://127.0.0.1:$peer_port\"}]" \
        VELORIX_RHIZA_OBJECT_STORE_PROVIDER=s3 \
        VELORIX_RHIZA_OBJECT_STORE_ENDPOINT="127.0.0.1:$s3_port" \
        VELORIX_RHIZA_OBJECT_STORE_BUCKET="$bucket" \
        VELORIX_RHIZA_OBJECT_STORE_REGION=us-east-1 \
        VELORIX_RHIZA_OBJECT_STORE_PREFIX="$prefix/meta" \
        VELORIX_RHIZA_OBJECT_STORE_ACCESS_KEY="$access_key" \
        VELORIX_RHIZA_OBJECT_STORE_SECRET_KEY="$secret_key" \
        VELORIX_RHIZA_OBJECT_STORE_INSECURE=1 \
        VELORIX_RHIZA_OBJECT_STORE_DURABILITY=before-ack \
        "$repo_root/target/debug/velorix-meta" serve >"$evidence_dir/meta-$generation.log" 2>&1 &
    fi
    meta_pid=$!
    if [ "$service_mode" = local ]; then
        printf '%s\n' "$meta_pid" >"$evidence_dir/meta-$meta_generation.pid"
        meta_command=
        process_check=0
        while [ "$process_check" -lt 10 ]; do
            meta_command=$(ps -p "$meta_pid" -o command= 2>/dev/null || true)
            case "$meta_command" in
                *"$repo_root/target/debug/velorix-meta"*) break ;;
            esac
            pid_running "$meta_pid" || break
            sleep 1
            process_check=$((process_check + 1))
        done
        case "$meta_command" in
            *"$repo_root/target/debug/velorix-meta"*)
                printf '%s\n' "$meta_command" >"$evidence_dir/meta-$meta_generation.process.txt"
                ;;
            *)
                echo "background Local meta PID $meta_pid is not the velorix-meta binary (command: ${meta_command:-exited})" >&2
                return 1
                ;;
        esac
    fi
    i=0
    while [ "$i" -lt 90 ]; do
        if [ -s "$evidence_dir/meta-$generation.log" ] && ! pid_running "$meta_pid"; then
            cat "$evidence_dir/meta-$generation.log" >&2
            return 1
        fi
        if VELORIX_META_GRPC_ENDPOINT="https://127.0.0.1:$meta_port" \
           VELORIX_META_TLS_CA_FILE="$private_dir/ca.crt" \
           VELORIX_META_TLS_CLIENT_CERT_FILE="$private_dir/client.crt" \
           VELORIX_META_TLS_CLIENT_KEY_FILE="$private_dir/client.key" \
           VELORIX_META_TLS_DOMAIN_NAME=localhost \
           VELORIX_META_BEARER_TOKEN="$meta_token" \
           "$repo_root/target/debug/velorix-meta" smoke --expect-backend rhiza-kv --expect-auth-enforced true --expect-production-multi-writer-safe false --capabilities-only --connect-retry-timeout-seconds 2 \
           >"$evidence_dir/meta-smoke-$generation.log" 2>&1; then return 0; fi
        sleep 1
        i=$((i + 1))
    done
    cat "$evidence_dir/meta-$generation.log" >&2
    return 1
}

start_worker_api() {
    generation=$1
    cache="$evidence_dir/cache-$generation"
    # Keep identities stable across empty-dir restores so new processes
    # reacquire the same logical owner scope.
    worker_operator="recovery-worker"
    api_operator="recovery-api"
    common_env=""
    # Keep assignments explicit so neither process inherits the other's local state.
    env VELORIX_S3_COMPAT=1 AWS_ENDPOINT_URL="$endpoint" AWS_ACCESS_KEY_ID="$access_key" AWS_SECRET_ACCESS_KEY="$secret_key" AWS_REGION=us-east-1 \
        VELORIX_S3_BUCKET="$bucket" VELORIX_S3_PREFIX="$prefix/product" VELORIX_S3_FORCE_PATH_STYLE=true \
        VELORIX_AUTHORITY_STORE_ID="s3://$bucket/$prefix/product" VELORIX_AUTHORITY_NAMESPACE=velorix \
        VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=1 \
        VELORIX_META_GRPC_ENDPOINT="https://127.0.0.1:$meta_port" VELORIX_META_BEARER_TOKEN="$meta_token" \
        VELORIX_META_TLS_CA_FILE="$private_dir/ca.crt" VELORIX_META_TLS_CLIENT_CERT_FILE="$private_dir/client.crt" \
        VELORIX_META_TLS_CLIENT_KEY_FILE="$private_dir/client.key" VELORIX_META_TLS_DOMAIN_NAME=localhost \
        VELORIX_API_BEARER_TOKEN="$api_token" VELORIX_ADMIN_BEARER_TOKEN="$admin_token" \
        VELORIX_RELATION_INGEST_OWNER_ID="recovery-ingest" \
        VELORIX_STANDING_RUNTIME_OWNER_TTL_MS=30000 \
        VELORIX_STANDING_RUNTIME_FENCING=logical-fencing VELORIX_DEMAND_QUERY_DIAGNOSTICS=1 \
        VELORIX_MATERIALIZED_READ_CACHE_DIR="$cache/worker" VELORIX_SERVICE_ROLE=worker \
        VELORIX_API_BIND="127.0.0.1:$worker_port" VELORIX_API_OWNER_ENDPOINT="http://127.0.0.1:$worker_port" \
        VELORIX_OPERATOR_ID="$worker_operator" VELORIX_API_REPLICA_COUNT=1 \
        "$repo_root/target/debug/velorix-api" >"$evidence_dir/worker-$generation.log" 2>&1 &
    worker_pid=$!
    i=0
    while [ "$i" -lt 90 ]; do
        if curl --silent --fail "http://127.0.0.1:$worker_port/healthz" >/dev/null 2>&1; then break; fi
        if ! pid_running "$worker_pid"; then cat "$evidence_dir/worker-$generation.log" >&2; return 1; fi
        sleep 1
        i=$((i + 1))
    done
    [ "$i" -lt 90 ] || { cat "$evidence_dir/worker-$generation.log" >&2; return 1; }
    assert_logical_fencing worker "$worker_port" "$evidence_dir/worker-readyz-$generation.json"
    env VELORIX_S3_COMPAT=1 AWS_ENDPOINT_URL="$endpoint" AWS_ACCESS_KEY_ID="$access_key" AWS_SECRET_ACCESS_KEY="$secret_key" AWS_REGION=us-east-1 \
        VELORIX_S3_BUCKET="$bucket" VELORIX_S3_PREFIX="$prefix/product" VELORIX_S3_FORCE_PATH_STYLE=true \
        VELORIX_AUTHORITY_STORE_ID="s3://$bucket/$prefix/product" VELORIX_AUTHORITY_NAMESPACE=velorix \
        VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=1 \
        VELORIX_META_GRPC_ENDPOINT="https://127.0.0.1:$meta_port" VELORIX_META_BEARER_TOKEN="$meta_token" \
        VELORIX_META_TLS_CA_FILE="$private_dir/ca.crt" VELORIX_META_TLS_CLIENT_CERT_FILE="$private_dir/client.crt" \
        VELORIX_META_TLS_CLIENT_KEY_FILE="$private_dir/client.key" VELORIX_META_TLS_DOMAIN_NAME=localhost \
        VELORIX_API_BEARER_TOKEN="$api_token" VELORIX_ADMIN_BEARER_TOKEN="$admin_token" \
        VELORIX_RELATION_INGEST_OWNER_ID="recovery-ingest" \
        VELORIX_STANDING_RUNTIME_OWNER_TTL_MS=30000 \
        VELORIX_STANDING_RUNTIME_FENCING=logical-fencing VELORIX_DEMAND_QUERY_DIAGNOSTICS=1 \
        VELORIX_MATERIALIZED_READ_CACHE_DIR="$cache/api" VELORIX_SERVICE_ROLE=api \
        VELORIX_WORKER_ENDPOINTS="http://127.0.0.1:$worker_port" \
        VELORIX_API_BIND="127.0.0.1:$api_port" VELORIX_API_OWNER_ENDPOINT="http://127.0.0.1:$api_port" \
        VELORIX_OPERATOR_ID="$api_operator" VELORIX_API_REPLICA_COUNT=1 \
        "$repo_root/target/debug/velorix-api" >"$evidence_dir/api-$generation.log" 2>&1 &
    api_pid=$!
    i=0
    while [ "$i" -lt 90 ]; do
        if curl --silent --fail "http://127.0.0.1:$api_port/healthz" >/dev/null 2>&1; then
            assert_logical_fencing api "$api_port" "$evidence_dir/api-readyz-$generation.json"
            return 0
        fi
        if ! pid_running "$api_pid"; then cat "$evidence_dir/api-$generation.log" >&2; return 1; fi
        sleep 1
        i=$((i + 1))
    done
    cat "$evidence_dir/api-$generation.log" >&2
    return 1
}

api_post() {
    path=$1 payload=$2 output=$3
    code=$(curl --silent --show-error --max-time 60 -o "$output" -w '%{http_code}' \
        -H "Authorization: Bearer $api_token" -H 'Content-Type: application/json' \
        --data-binary "@$payload" "http://127.0.0.1:$api_port$path")
    printf '%s %s\n' "$path" "$code" >>"$evidence_dir/http-status.log"
    case "$code" in 2*|3*) ;; *) cat "$output" >&2; return 1 ;; esac
}

api_query() {
    view=$1 output=$2 query_payload=${3:-$evidence_dir/payloads/query.json}
    i=0
    while [ "$i" -lt 90 ]; do
        code=$(curl --silent --show-error --max-time 30 -o "$output" -w '%{http_code}' \
            -H "Authorization: Bearer $api_token" -H 'Content-Type: application/json' -H 'Accept: application/json' \
            --data-binary "@$query_payload" \
            "http://127.0.0.1:$api_port/v1/views/$view/query")
        if [ "$code" = 200 ]; then
            jq -e '(.rows | type == "array") and (.rows | length > 0) and (has("materialization") | not)' "$output" >/dev/null || {
                echo "view query returned HTTP 200 without completed result rows: $view" >&2
                cat "$output" >&2
                return 1
            }
            return 0
        fi
        [ "$code" = 202 ] || { cat "$output" >&2; return 1; }
        sleep 1
        i=$((i + 1))
    done
    echo "timed out waiting for view query: $view" >&2
    cat "$output" >&2
    return 1
}

assert_query_epoch_unchanged() {
    before=$1 after=$2 label=$3
    jq -e -s '
        length == 2 and
        (.[0].logical_epoch | type == "number") and
        (.[1].logical_epoch | type == "number") and
        .[0].logical_epoch == .[1].logical_epoch
    ' "$before" "$after" >/dev/null || {
        echo "$label logical_epoch changed or was missing across restore" >&2
        return 1
    }
}

assert_logical_fencing() {
    role=$1 port=$2 output=$3
    curl --silent --show-error --fail --max-time 15 \
        -H "Authorization: Bearer $api_token" "http://127.0.0.1:$port/readyz" >"$output"
    jq -e --arg role "$role" '
        (.metadata_store.standing_runtime_fencing) as $c |
        .status == "ready" and .service_role == $role and
        .standing_runtime_fencing_required == true and
        .standing_runtime_fencing_mode == "logical-fencing" and
        $c.backend_name == "rhiza-kv" and
        $c.control_plane_auth_enforced == true and
        $c.linearizable_owner_lease == true and
        $c.durable_monotonic_owner_epoch == true and
        $c.owner_validated_checkpoint_publish == true and
        $c.publish_checks_owner_and_latest_atomically == true and
        $c.publish_rejects_expired_owner == true and
        $c.latest_read_linearizable == true and
        $c.publish_rejects_scope_mismatch == true and
        $c.backend_time_source_kind == "root_cas_logical_clock" and
        $c.authoritative_backend_time == false and
        $c.lease_authority_kind == "root_cas_logical_clock" and
        $c.lease_expiry_semantics == "operation_driven_logical" and
        $c.multi_writer_fencing_safe == true and
        $c.bounded_wall_clock_failover == false and
        $c.production_bounded_failover_safe == false and
        $c.production_multi_writer_safe == false
    ' "$output" >/dev/null || {
        echo "$role readyz did not report the required Rhiza logical-fencing capabilities" >&2
        return 1
    }
}

object_inventory() {
    name=$1
    s3 --fail --get --data-urlencode 'list-type=2' --data-urlencode "prefix=$prefix/" --data-urlencode 'max-keys=1000' "$endpoint/$bucket" >"$evidence_dir/$name.xml" 2>"$evidence_dir/$name.stderr.log"
    [ "$(xmllint --nonet --xpath 'string(/*[local-name()="ListBucketResult"]/*[local-name()="IsTruncated"])' "$evidence_dir/$name.xml")" = false ]
    count=$(xmllint --nonet --xpath 'count(/*[local-name()="ListBucketResult"]/*[local-name()="Contents"])' "$evidence_dir/$name.xml")
    printf '%s\n' "$count" >"$evidence_dir/$name.count"
}

. "$repo_root/scripts/rhiza-service-recovery-payloads.sh"
rhiza_service_recovery_write_payloads "$evidence_dir/payloads"

printf '%s\n' "evidence_dir=$evidence_dir" "versitygw_commit=$gateway_commit" "rhizadb_version=$rhizadb_version" >"$evidence_dir/run.json.tmp"
mv "$evidence_dir/run.json.tmp" "$evidence_dir/run.txt"
generation=0
start_meta
start_worker_api 1

api_post /v1/relations "$evidence_dir/payloads/relation.json" "$evidence_dir/register-response.json"
api_post /v1/views "$evidence_dir/payloads/view-sum.json" "$evidence_dir/view-sum-response.json"
api_post /v1/views "$evidence_dir/payloads/view-count.json" "$evidence_dir/view-count-response.json"
api_post /v1/relations/samples/ingest "$evidence_dir/payloads/ingest.json" "$evidence_dir/ingest-initial-response.json"
api_query sample_sum "$evidence_dir/query-initial-sum.json"
api_query sample_count "$evidence_dir/query-initial-count.json"
jq -e '.rows[0].result == 2.5' "$evidence_dir/query-initial-sum.json" >/dev/null
jq -e '.rows[0].result == 1' "$evidence_dir/query-initial-count.json" >/dev/null
object_inventory s3-before-recovery

stop_pid "$api_pid"; api_pid=
stop_pid "$worker_pid"; worker_pid=
if [ "$service_mode" = local ]; then stop_local_meta; else stop_pid "$meta_pid"; fi
meta_pid=
mv "$evidence_dir/rhiza" "$evidence_dir/rhiza-before-emptydir-restore"
mkdir "$evidence_dir/rhiza"
generation=1
gateway_lines_before_restore_boot_query=$(wc -l <"$evidence_dir/gateway.log" | tr -d ' ')
start_meta
start_worker_api 2
api_query sample_sum "$evidence_dir/query-after-recovery-sum.json"
api_query sample_count "$evidence_dir/query-after-recovery-count.json"
jq -e '.rows[0].result == 2.5' "$evidence_dir/query-after-recovery-sum.json" >/dev/null
jq -e '.rows[0].result == 1' "$evidence_dir/query-after-recovery-count.json" >/dev/null
assert_query_epoch_unchanged "$evidence_dir/query-initial-sum.json" "$evidence_dir/query-after-recovery-sum.json" sample_sum
assert_query_epoch_unchanged "$evidence_dir/query-initial-count.json" "$evidence_dir/query-after-recovery-count.json" sample_count
tail -n +$((gateway_lines_before_restore_boot_query + 1)) "$evidence_dir/gateway.log" >"$evidence_dir/s3-first-restore-boot-query-audit.log"
restore_old_source_gets=$(rg -c 'GET.*ingest-staging/samples-stream/p=0000000000/00000000000000000000-00000000000000000001/' "$evidence_dir/s3-first-restore-boot-query-audit.log" || true)
restore_old_source_gets=${restore_old_source_gets:-0}
if [ "$restore_old_source_gets" -ne 0 ]; then
    echo "first-restore startup/query reread the initial source batch (GET count=$restore_old_source_gets)" >&2
    exit 1
fi
object_inventory s3-after-recovery

# Append a unique post-restore input exactly once, then repeat the same request
# to exercise the public offset/idempotency path before a second cold restart.
# The relation has no event-time column, so this is not a late-event test.
jq '.start_offset_inclusive=1 | .rows=[{"reading":1.5,"delta":1}]' "$evidence_dir/payloads/ingest.json" >"$evidence_dir/payloads/ingest-post-restore.json"
api_post /v1/relations/samples/ingest "$evidence_dir/payloads/ingest-post-restore.json" "$evidence_dir/ingest-post-restore-response.json"
gateway_lines_before_incremental_query=$(wc -l <"$evidence_dir/gateway.log" | tr -d ' ')
api_query sample_sum "$evidence_dir/query-incremental-sum.json"
api_query sample_count "$evidence_dir/query-incremental-count.json"
jq -e '.rows[0].result == 4' "$evidence_dir/query-incremental-sum.json" >/dev/null
jq -e '.rows[0].result == 2' "$evidence_dir/query-incremental-count.json" >/dev/null
tail -n +$((gateway_lines_before_incremental_query + 1)) "$evidence_dir/gateway.log" >"$evidence_dir/s3-incremental-query-audit.log"
new_source_gets=$(rg -c 'GET.*ingest-staging/samples-stream/p=0000000000/00000000000000000001-00000000000000000002/' "$evidence_dir/s3-incremental-query-audit.log" || true)
old_source_gets=$(rg -c 'GET.*ingest-staging/samples-stream/p=0000000000/00000000000000000000-00000000000000000001/' "$evidence_dir/s3-incremental-query-audit.log" || true)
new_source_gets=${new_source_gets:-0}
old_source_gets=${old_source_gets:-0}
if [ "$new_source_gets" -le 0 ]; then
    echo 'incremental source processing did not GET the post-restore source batch' >&2
    exit 1
fi
if [ "$old_source_gets" -ne 0 ]; then
    echo "incremental source processing reread the initial source batch (GET count=$old_source_gets)" >&2
    exit 1
fi
api_post /v1/relations/samples/ingest "$evidence_dir/payloads/ingest-post-restore.json" "$evidence_dir/ingest-idempotent-retry-response.json"
jq -e '.outcome == "appended"' "$evidence_dir/ingest-post-restore-response.json" >/dev/null
jq -e -s '
    length == 2 and
    .[0].outcome == "duplicate" and
    .[1].outcome == "appended" and
    .[0].batches[0].descriptor == .[1].batches[0].descriptor and
    .[0].batches[0].descriptor.start_offset_inclusive == 1 and
    .[0].batches[0].descriptor.end_offset_exclusive == 2
' \
    "$evidence_dir/ingest-idempotent-retry-response.json" "$evidence_dir/ingest-post-restore-response.json" >/dev/null
api_query sample_sum "$evidence_dir/query-after-retry-sum.json"
api_query sample_count "$evidence_dir/query-after-retry-count.json"
jq -e '.rows[0].result == 4' "$evidence_dir/query-after-retry-sum.json" >/dev/null
jq -e '.rows[0].result == 2' "$evidence_dir/query-after-retry-count.json" >/dev/null

stop_pid "$api_pid"; api_pid=
stop_pid "$worker_pid"; worker_pid=
if [ "$service_mode" = local ]; then stop_local_meta; else stop_pid "$meta_pid"; fi
meta_pid=
mv "$evidence_dir/rhiza" "$evidence_dir/rhiza-before-second-emptydir-restore"
mkdir "$evidence_dir/rhiza"
generation=2
start_meta
start_worker_api 3
api_query sample_sum "$evidence_dir/query-after-second-recovery-sum.json"
api_query sample_count "$evidence_dir/query-after-second-recovery-count.json"
jq -e '.rows[0].result == 4' "$evidence_dir/query-after-second-recovery-sum.json" >/dev/null
jq -e '.rows[0].result == 2' "$evidence_dir/query-after-second-recovery-count.json" >/dev/null
assert_query_epoch_unchanged "$evidence_dir/query-incremental-sum.json" "$evidence_dir/query-after-second-recovery-sum.json" sample_sum
assert_query_epoch_unchanged "$evidence_dir/query-incremental-count.json" "$evidence_dir/query-after-second-recovery-count.json" sample_count
object_inventory s3-after-second-recovery

if ! rg -q 'GET.*standing-runtime-checkpoints.*/sample_sum/sample_sum/epochs/[0-9]+/sha256/[0-9a-f]+\.checkpoint\.arrow' "$evidence_dir/s3-first-restore-boot-query-audit.log" \
    || ! rg -q 'GET.*standing-runtime-checkpoints.*/sample_count/sample_count/epochs/[0-9]+/sha256/[0-9a-f]+\.checkpoint\.arrow' "$evidence_dir/s3-first-restore-boot-query-audit.log"; then
    echo 'S3 request audit has no checkpoint body GET for both restored views' >&2
    exit 1
fi
checkpoint_codec_count=0
for checkpoint in $(find "$evidence_dir/versity-data" -type f -name '*.checkpoint.arrow'); do
    if rg -a -q 'velorix-arrow-ipc-zstd3-v1' "$checkpoint"; then checkpoint_codec_count=$((checkpoint_codec_count + 1)); fi
done
[ "$checkpoint_codec_count" -gt 0 ] || { echo 'Arrow/ZSTD checkpoint codec identity was not found in stored checkpoint bodies' >&2; exit 1; }
stop_pid "$api_pid"; api_pid=
stop_pid "$worker_pid"; worker_pid=
if [ "$service_mode" = local ]; then stop_local_meta; else stop_pid "$meta_pid"; fi
meta_pid=
local_init_passed=false
local_graceful_close_passed=false
local_init_failure_passed=false
if [ "$service_mode" = local ]; then
    local_init_passed=true
    local_graceful_close_passed=true
    local_init_failure_passed=true
    for init_generation in 0 1 2; do
        rg -q '^init-local_exit=0$' "$evidence_dir/init-local-$init_generation.status" \
            && rg -q '^data_dir_exists=true$' "$evidence_dir/init-local-$init_generation.status" \
            || { echo "Local init evidence incomplete for generation $init_generation" >&2; exit 1; }
        rg -q '^exit=0$' "$evidence_dir/meta-$init_generation-stop.status" \
            && rg -q '^forced_kill=false$' "$evidence_dir/meta-$init_generation-stop.status" \
            && rg -q '^close_diagnostic=present$' "$evidence_dir/meta-$init_generation-stop.status" \
            || { echo "Local graceful-close evidence incomplete for generation $init_generation" >&2; exit 1; }
    done
    rg -q '^init-local_exit=nonzero$' "$evidence_dir/init-local-storage-failure.status" \
        && rg -q '^main_invoked=false$' "$evidence_dir/init-local-storage-failure.status" \
        && rg -q '^meta_listener_present=false$' "$evidence_dir/init-local-storage-failure.status" \
        || { echo 'Local init storage-failure evidence incomplete' >&2; exit 1; }
fi
if ! rg -q 'completed_demand_preflight_result result=hit source_prep=0 output_write=0' "$evidence_dir/api-2.log" "$evidence_dir/api-3.log"; then
    echo 'no completed preflight hit with zero source preparation/output writes observed' >&2
    exit 1
fi
jq -n \
    --arg evidence_dir "$evidence_dir" \
    --arg versitygw_commit "$gateway_commit" \
    --arg rhizadb_version "$rhizadb_version" \
    --argjson s3_before "$(cat "$evidence_dir/s3-before-recovery.count")" \
    --argjson s3_after "$(cat "$evidence_dir/s3-after-recovery.count")" \
    --argjson s3_after_second "$(cat "$evidence_dir/s3-after-second-recovery.count")" \
    --argjson checkpoint_codec_count "$checkpoint_codec_count" \
    --argjson new_source_gets "$new_source_gets" \
    --argjson old_source_gets "$old_source_gets" \
    --argjson restore_old_source_gets "$restore_old_source_gets" \
    --arg service_mode "$service_mode" \
    --arg meta_mode "$meta_mode" \
    --argjson local_init_passed "$local_init_passed" \
    --argjson local_graceful_close_passed "$local_graceful_close_passed" \
    --argjson local_init_failure_passed "$local_init_failure_passed" \
    --arg qualification "Production mode was applied to meta service admission only; API and worker retained the harness local-development and logical-fencing setup" \
    '{evidence_kind:"real_split_process_generic_view_recovery",scope:("local API and worker binaries plus one Rhiza " + $rhizadb_version + " meta process; pinned local VersityGW; no Kubernetes"),evidence_dir:$evidence_dir,versitygw_commit:$versitygw_commit,rhizadb_version:$rhizadb_version,service_mode:$service_mode,rhiza_mode:$service_mode,meta_mode:$meta_mode,qualification:$qualification,distinct_process_roles:true,public_http_registration_ingest_and_query:true,local_init_before_every_meta_start:$local_init_passed,local_init_real_storage_failure_fails_closed:$local_init_failure_passed,local_sigterm_graceful_close:$local_graceful_close_passed,initial_outputs:{sum:2.5,count:1},after_first_emptydir_restore:{sum:2.5,count:1},post_restore_unique_input:{sum:4,count:2},same_offset_retry_outputs:{sum:4,count:2},duplicate_retry_outcome:"duplicate",duplicate_retry_descriptor_unchanged:true,after_second_emptydir_restore:{sum:4,count:2},checkpoint_body_s3_get_for_both_views:true,checkpoint_codec_identity:"velorix-arrow-ipc-zstd3-v1",checkpoint_bodies_with_codec_identity:$checkpoint_codec_count,restored_query_epoch_pinned:false,restored_query_epoch_unchanged:true,restored_query_source_preparation:0,first_restore_boot_query_old_source_batch_gets:$restore_old_source_gets,incremental_new_source_batch_gets:$new_source_gets,incremental_old_source_batch_gets:$old_source_gets,service_fencing_mode:"logical-fencing",logical_fencing_capabilities_verified:true,logical_clock_kind:"root_cas_logical_clock",lease_expiry_semantics:"operation_driven_logical",owner_lease_ttl_parameter:30000,owner_lease_ttl_units:"logical_ticks",stable_worker_operator_id:"recovery-worker",stable_api_operator_id:"recovery-api",stable_relation_ingest_owner_id:"recovery-ingest",production_bounded_failover_safe:false,production_multi_writer_safe:false,s3_objects:{before:$s3_before,after_first_restore:$s3_after,after_second_restore:$s3_after_second}}' >"$evidence_dir/result.json"
printf 'generic Rhiza service recovery passed (service_mode=%s meta_mode=%s); evidence: %s\n' "$service_mode" "$meta_mode" "$evidence_dir"
