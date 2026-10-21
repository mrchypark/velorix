#!/usr/bin/env bash
set -euo pipefail

# Exercise the launch configuration itself, without Docker, kubectl or servers.
repo_root="$(git rev-parse --show-toplevel)"
script_path="$repo_root/scripts/run-vind-product.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
printf 'set -eu\n' >"$scratch/config.sh"
awk '
  /^repo_root=/ { capture=1 }
  /^case "\$cluster_driver" in/ { capture=0 }
  capture { print }
' "$script_path" >>"$scratch/config.sh"
awk '
  /^validate_external_meta_config\(\)/ { capture=1 }
  /^validate_external_meta_readyz\(\)/ { capture=0 }
  capture { print }
' "$script_path" >>"$scratch/config.sh"
cat >>"$scratch/config.sh" <<'EOF'
validate_external_meta_config
printf '%s\n' "$api_authoritative_relation_ingest/$meta_external/$meta_backend/$relation_ingest_owner_id/$standing_runtime_fencing"
EOF
printf 'configuration fixture, not a valid certificate\n' >"$scratch/cert"
base=(env -i "PATH=$PATH" "HOME=$HOME")
external=("VELORIX_META_GRPC_ENDPOINT=https://meta.example:9090"
  "VELORIX_META_BEARER_TOKEN=fixture-token"
  "VELORIX_META_TLS_CA_FILE=$scratch/cert"
  "VELORIX_META_TLS_CLIENT_CERT_FILE=$scratch/cert"
  "VELORIX_META_TLS_CLIENT_KEY_FILE=$scratch/cert"
  "VELORIX_META_TLS_DOMAIN_NAME=meta.example")
expect_reject() {
  local status=0
  "${base[@]}" "$@" bash "$scratch/config.sh" >"$scratch/result" 2>&1 || status=$?
  [ "$status" = 64 ] || { cat "$scratch/result"; echo "expected config rejection, got $status" >&2; exit 1; }
  ! grep -F 'fixture-token' "$scratch/result"
}
expect_reject
expect_reject "${external[@]}" VELORIX_META_ENABLED=0
expect_reject "${external[@]}" VELORIX_META_BACKEND=memory
expect_reject "${external[@]}" VELORIX_META_GRPC_ENDPOINT=http://meta.example:9090
expect_reject "${external[@]}" VELORIX_META_GRPC_ENDPOINT='https://user:password@meta.example:9090'
expect_reject "${external[@]}" VELORIX_META_BEARER_TOKEN=
expect_reject "${external[@]}" VELORIX_META_BEARER_TOKEN='bad token'
expect_reject "${external[@]}" VELORIX_META_TLS_CLIENT_KEY_FILE=/missing
expect_reject "${external[@]}" VELORIX_META_TLS_DOMAIN_NAME=
expect_reject "${external[@]}" VELORIX_RELATION_INGEST_OWNER_ID=pod-random
expect_reject "${external[@]}" VELORIX_STANDING_RUNTIME_FENCING=unsafe-dev-only
expect_reject "${external[@]}" VELORIX_VIND_PRODUCT_SMOKE=1
expected='1/1/rhiza-kv/velorix-api-velorix-product/logical-fencing'
[ "$("${base[@]}" "${external[@]}" bash "$scratch/config.sh")" = "$expected" ]
[ "$("${base[@]}" "${external[@]}" bash "$scratch/config.sh")" = "$expected" ]
[ "$("${base[@]}" VELORIX_API_AUTHORITATIVE_RELATION_INGEST=false bash "$scratch/config.sh")" = '0/0/memory/velorix-api-velorix-product/unsafe-dev-only' ]
[ "$("${base[@]}" VELORIX_API_AUTHORITATIVE_RELATION_INGEST=0 bash "$scratch/config.sh")" = '0/0/memory/velorix-api-velorix-product/unsafe-dev-only' ]
[ "$("${base[@]}" "${external[@]}" VELORIX_API_AUTHORITATIVE_RELATION_INGEST=false bash "$scratch/config.sh")" = '0/1/rhiza-kv/velorix-api-velorix-product/logical-fencing' ]

eval "$(awk '/^validate_external_meta_readyz\(\)/ { capture=1 } /^write_public_evidence\(\)/ { capture=0 } capture { print }' "$script_path")"
meta_endpoint=https://meta.example:9090
api_auth_mode=bearer-token
standing_runtime_fencing=logical-fencing
api_authoritative_relation_ingest=1
jq -n '{status:"ready", api_auth:{mode:"bearer-token"}, admin_auth:{mode:"bearer-token"},
  relation_ingest:{mode:"authoritative",authoritative:true},
  standing_runtime_fencing_mode:"logical-fencing", standing_runtime_fencing_required:true,
  object_store:{schema_version:1,artifact_catalog:{conditional_update:true},authoritative_namespace_count:1},
  metadata_store:{configured:true,endpoint:"https://meta.example:9090",standing_runtime_fencing:{
    backend_name:"rhiza-kv",capability_schema_version:2,control_plane_auth_enforced:true,
    linearizable_owner_lease:true,durable_monotonic_owner_epoch:true,owner_validated_checkpoint_publish:true,
    publish_checks_owner_and_latest_atomically:true,publish_rejects_expired_owner:true,latest_read_linearizable:true,
    publish_rejects_scope_mismatch:true,multi_writer_fencing_safe:true,
    lease_authority_kind:"raft_replicated_time",lease_expiry_semantics:"operation_driven_logical"}}}' >"$scratch/ready.json"
validate_external_meta_readyz "$scratch/ready.json" >/dev/null
for change in '.metadata_store.endpoint="https://other:9090"' \
  '.metadata_store.standing_runtime_fencing.backend_name="in-memory"' \
  '.metadata_store.standing_runtime_fencing.publish_rejects_expired_owner=false' \
  '.relation_ingest={mode:"legacy",authoritative:false}' \
  '.object_store.artifact_catalog.conditional_update=false'; do
  jq "$change" "$scratch/ready.json" >"$scratch/bad.json"
  if validate_external_meta_readyz "$scratch/bad.json" >/dev/null; then
    echo "accepted invalid readyz: $change" >&2; exit 1
  fi
done
standing_runtime_fencing=required
if validate_external_meta_readyz "$scratch/ready.json" >/dev/null; then
  echo "required fencing accepted logical-only readiness" >&2; exit 1
fi
# Match the public standing_runtime_fencing_capability_json fields, not internal names.
jq '.standing_runtime_fencing_mode="required" |
  .metadata_store.standing_runtime_fencing += {
    authoritative_backend_time:true, bounded_wall_clock_failover:true,
    production_bounded_failover_safe:true, production_multi_writer_safe:true,
    failover_time_bound_ms:30000, backend_time_source_kind:"raft_replicated_authority_time",
    lease_expiry_semantics:"backend_wall_clock_ttl"
  }' "$scratch/ready.json" >"$scratch/required.json"
validate_external_meta_readyz "$scratch/required.json" >/dev/null
# Even with the required mode label, logical-only capabilities cannot pass.
jq '.standing_runtime_fencing_mode="required"' "$scratch/ready.json" >"$scratch/logical-only.json"
if validate_external_meta_readyz "$scratch/logical-only.json" >/dev/null; then
  echo "required fencing accepted logical-only capabilities" >&2; exit 1
fi
for change in 'del(.metadata_store.standing_runtime_fencing.failover_time_bound_ms)' \
  '.metadata_store.standing_runtime_fencing.failover_time_bound_ms=0' \
  '.metadata_store.standing_runtime_fencing.failover_time_bound_ms="30000"' \
  '.metadata_store.standing_runtime_fencing |= (.owner_failover_bound_ms=.failover_time_bound_ms | del(.failover_time_bound_ms))'; do
  jq "$change" "$scratch/required.json" >"$scratch/bad-required.json"
  if validate_external_meta_readyz "$scratch/bad-required.json" >/dev/null; then
    echo "required fencing accepted invalid failover bound: $change" >&2; exit 1
  fi
done
eval "$(awk '/^configure_external_meta_client\(\)/ { capture=1 } /^configure_external_meta_client$/ { capture=0 } capture { print }' "$script_path")"
kubectl() {
  printf '%s\n' "$*" >>"$scratch/kubectl-calls"
  case " $* " in
    *' create secret generic '*) printf 'kind: Secret\n' ;;
    *' apply -f - '*) cat >/dev/null ;;
    *) echo 'unexpected kubectl operation' >&2; return 1 ;;
  esac
}
context=fixture-context
namespace=fixture-namespace
meta_external=1
VELORIX_META_TLS_CA_FILE="$scratch/cert"
VELORIX_META_TLS_CLIENT_CERT_FILE="$scratch/cert"
VELORIX_META_TLS_CLIENT_KEY_FILE="$scratch/cert"
VELORIX_META_TLS_DOMAIN_NAME=meta.example
for api_tls in 0 1; do
  api_meta_env='            - name: VELORIX_META_GRPC_ENDPOINT'
  api_tls_volume_mounts=''
  api_tls_volumes=''
  if [ "$api_tls" = 1 ]; then
    api_tls_volume_mounts=$'          volumeMounts:\n            - name: api-tls'
    api_tls_volumes=$'      volumes:\n        - name: api-tls'
  fi
  configure_external_meta_client
  [ "$(printf '%s\n' "$api_tls_volume_mounts" | grep -c 'volumeMounts:')" = 1 ]
  [ "$(printf '%s\n' "$api_tls_volumes" | grep -c 'volumes:')" = 1 ]
  [[ "$api_tls_volume_mounts" == *'/var/run/velorix-meta-client-tls'* ]]
  [[ "$api_tls_volumes" == *'secretName: velorix-api-meta-client-tls'* ]]
  [[ "$api_meta_env" == *'VELORIX_META_TLS_CLIENT_KEY_FILE'* ]]
done
[ "$(wc -l <"$scratch/kubectl-calls" | tr -d ' ')" = 4 ]
# Read-only utility mode must not require any server configuration.
printf '{"endpoint":"https://private.example:9090","safe":true}\n' >"$scratch/redact.json"
"${base[@]}" VELORIX_EVIDENCE_REDACT_ONLY=1 VELORIX_EVIDENCE_REDACT_ONLY_FILE="$scratch/redact.json" bash "$script_path"
jq -e '.endpoint == "<redacted>" and .safe == true' "$scratch/redact.public.json" >/dev/null
bash -n "$script_path"
echo 'external Rhiza configuration and readiness contracts passed (no services started)'
