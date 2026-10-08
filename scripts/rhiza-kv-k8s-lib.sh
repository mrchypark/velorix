#!/bin/sh
# Shared preflight helpers for the Rhiza KV Kubernetes validation scripts.
#
# This file is sourced, never executed. It exists so the gate and the test-only
# fixture derive Rhiza peer identities and verify the vendored operator manifests
# exactly once. Duplicating that logic is how a run ends up publishing a key no
# node accepts, or claiming a recovery an unverified operator cannot perform.
#
# Derivation, as Rhiza performs it:
#   seed       = HMAC-SHA256(key = peer token,
#                            msg  = "rhiza-peer-certificate\0" clusterID "\0" nodeID)
#   public     = Ed25519 public key of that seed
#   public_key = standard padded base64 of the 32 raw bytes
#
# OpenSSL cannot import a raw Ed25519 seed, so the seed is wrapped in the fixed
# 16-byte PKCS#8 Ed25519 prefix and the SPKI public key is the trailing 32 bytes
# of the DER output. Every intermediate result is length- and alphabet-checked,
# so an OpenSSL build that cannot produce exactly the accepted encoding fails
# closed instead of publishing a key the node would reject at open time.

# The official published Rhiza 0.19.0 operator image, by manifest digest.
#
# This is the default and the production value. Nothing in these scripts
# rewrites, relaxes, or infers it: a run either uses exactly this reference or
# fails closed. The digest is recorded in deploy/rhiza-k8s/operator/
# UPSTREAM.sha256 alongside the vendored manifests it was published with.
rhiza_official_operator_image='ghcr.io/mrchypark/rhiza-operator@sha256:00b5e7a4c33c84ddbc2b7272b7d06bd3dd8315e31acb29507ad627d522a19d67'

# rhiza_select_operator_image <pin> <override> <provenance>
#
# Prints the operator image reference the run must use, and nothing else, so a
# caller can capture it with a command substitution. Returns non-zero with a
# message on stderr when the inputs cannot be trusted.
#
# Three rules, none of them negotiable:
#   * with no override the reference must be the official published digest pin,
#     so an unset or edited input can never silently downgrade the operator;
#   * an override must itself be an immutable sha256 reference, because this
#     component replaces cluster identity, membership, and credentials, and the
#     recovery claim depends on exactly which binary ran; and
#   * an override additionally requires a recorded build provenance, because a
#     digest that cannot be attributed to a source tree and a build proves
#     nothing about what was verified.
#
# An override is never presented as the published release image. Callers record
# it through rhiza_operator_image_is_official so the evidence states which of
# the two actually ran.
rhiza_select_operator_image() {
  _rhiza_pin=$1
  _rhiza_override=$2
  _rhiza_provenance=$3
  if [ -n "$_rhiza_override" ]; then
    case "$_rhiza_override" in
      *@sha256:*) ;;
      *)
        echo "the operator image override must be an immutable sha256 reference" >&2
        return 1
        ;;
    esac
    if [ -z "$_rhiza_provenance" ]; then
      echo "an operator image override requires a recorded build provenance" >&2
      return 1
    fi
    case "$_rhiza_provenance" in
      *[[:cntrl:]]*)
        echo "the operator image build provenance must be a single line" >&2
        return 1
        ;;
    esac
    [ "${#_rhiza_provenance}" -le 512 ] || {
      echo "the operator image build provenance must be at most 512 characters" >&2
      return 1
    }
    printf '%s' "$_rhiza_override"
    return 0
  fi
  [ -n "$_rhiza_pin" ] || {
    echo "an operator image reference is required" >&2
    return 1
  }
  [ "$_rhiza_pin" = "$rhiza_official_operator_image" ] || {
    echo "the operator image must be the official published pin ${rhiza_official_operator_image}, or an explicit override with recorded build provenance" >&2
    return 1
  }
  printf '%s' "$_rhiza_pin"
  return 0
}

# rhiza_operator_image_is_official <image>
#
# Succeeds only for the exact official published pin. Callers use this to keep
# an overridden image out of any "published release" claim.
rhiza_operator_image_is_official() {
  [ "$1" = "$rhiza_official_operator_image" ]
}

# rhiza_sha256_file <path>
#
# Prints the lowercase hex sha256. sha256sum and shasum are both accepted because
# the validation scripts run on macOS and on Linux CI.
rhiza_sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" 2>/dev/null | cut -d' ' -f1
  else
    shasum -a 256 "$1" 2>/dev/null | cut -d' ' -f1
  fi
}

# rhiza_derive_public_key <scratch-dir> <peer-token> <cluster-id> <node-id>
#
# Prints the standard padded base64 public key on success. Returns non-zero, with
# no stdout, when the local OpenSSL cannot produce the accepted encoding.
rhiza_derive_public_key() {
  _rhiza_scratch=$1
  _rhiza_token=$2
  _rhiza_cluster=$3
  _rhiza_node=$4
  printf 'rhiza-peer-certificate\000%s\000%s' "$_rhiza_cluster" "$_rhiza_node" \
    | openssl dgst -sha256 -hmac "$_rhiza_token" -binary \
      >"$_rhiza_scratch/seed" 2>"$_rhiza_scratch/derive.error" || return 1
  {
    printf '\060\056\002\001\000\060\005\006\003\053\145\160\004\042\004\040'
    cat "$_rhiza_scratch/seed"
  } >"$_rhiza_scratch/pkcs8.der"
  openssl pkey -inform DER -in "$_rhiza_scratch/pkcs8.der" -pubout -outform DER \
    -out "$_rhiza_scratch/spki.der" 2>>"$_rhiza_scratch/derive.error" || return 1
  [ "$(wc -c <"$_rhiza_scratch/spki.der" | tr -d ' ')" = 44 ] || return 1
  tail -c 32 "$_rhiza_scratch/spki.der" \
    | openssl base64 -A 2>>"$_rhiza_scratch/derive.error" \
    | tr -d '\n' >"$_rhiza_scratch/public_key" || return 1
  _rhiza_key=$(cat "$_rhiza_scratch/public_key")
  [ "${#_rhiza_key}" -eq 44 ] || return 1
  # Exactly one encoding is accepted: standard padded base64 of 32 raw bytes.
  case "$_rhiza_key" in
    *=) ;;
    *) return 1 ;;
  esac
  # Rejects the URL-safe alphabet, raw/unpadded output, and any other encoding
  # Rhiza does not accept for public_key.
  case "$_rhiza_key" in
    *[!A-Za-z0-9+/=]*) return 1 ;;
  esac
  openssl base64 -d -A -in "$_rhiza_scratch/public_key" \
    -out "$_rhiza_scratch/public_key.raw" 2>>"$_rhiza_scratch/derive.error" || return 1
  [ "$(wc -c <"$_rhiza_scratch/public_key.raw" | tr -d ' ')" = 32 ] || return 1
  printf '%s' "$_rhiza_key"
  return 0
}

# rhiza_verify_operator_manifests <directory>
#
# Recomputes the sha256 of every vendored upstream operator manifest named in
# UPSTREAM.sha256 and compares it. The operator is the component that replaces
# cluster identity, membership, and credentials, so a recovery claim must rest on
# manifests that are verifiably the official ones. Returns non-zero on any missing
# file or digest mismatch, and prints the mismatching file names to stderr.
#
# A checksum file whose last record has no trailing newline makes read fail after
# filling the field variables, so the loop body would never run for that record and
# the final record would go unchecked. The non-empty guard is on the digest field so
# a bare final digest still reaches the malformed-record rejection below instead of
# ending the loop. A file that names no manifest at all verifies nothing, so that
# fails too rather than reporting success.
rhiza_verify_operator_manifests() {
  _rhiza_dir=$1
  if [ ! -f "$_rhiza_dir/UPSTREAM.sha256" ]; then
    echo "missing UPSTREAM.sha256 in $_rhiza_dir" >&2
    return 1
  fi
  _rhiza_status=0
  _rhiza_count=0
  while read -r _rhiza_want _rhiza_file || [ -n "$_rhiza_want" ]; do
    case "$_rhiza_want" in
      ''|\#*) continue ;;
    esac
    case "$_rhiza_file" in
      '')
        echo "malformed UPSTREAM.sha256 record, no manifest name: $_rhiza_want" >&2
        _rhiza_status=1
        continue
        ;;
    esac
    _rhiza_count=$((_rhiza_count + 1))
    if [ ! -f "$_rhiza_dir/$_rhiza_file" ]; then
      echo "missing vendored operator manifest $_rhiza_dir/$_rhiza_file" >&2
      _rhiza_status=1
      continue
    fi
    _rhiza_got=$(rhiza_sha256_file "$_rhiza_dir/$_rhiza_file")
    if [ "$_rhiza_got" != "$_rhiza_want" ]; then
      echo "vendored operator manifest $_rhiza_file does not match UPSTREAM.sha256" >&2
      _rhiza_status=1
    fi
  done <"$_rhiza_dir/UPSTREAM.sha256"
  if [ "$_rhiza_status" = 0 ] && [ "$_rhiza_count" -eq 0 ]; then
    echo "UPSTREAM.sha256 in $_rhiza_dir names no vendored operator manifest" >&2
    _rhiza_status=1
  fi
  [ "$_rhiza_status" = 1 ] || return 0
  return 1
}