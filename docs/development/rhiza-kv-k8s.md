# Rhiza KV Kubernetes validation

`scripts/run-rhiza-kv-k8s-gate.sh` is an explicit, isolated validation harness
for the embedded Rhiza KV metadata service. It is preflight-only by default.
It does not select a cluster, create a namespace, or change Kubernetes state
unless `VELORIX_RHIZA_EXECUTE=1` is supplied. Run it from the repository root:
the fixture wrapper invokes it by relative path.

This harness targets the Rhiza peer-identity contract of Rhiza 0.19.0. It is
greenfield. It provisions a fresh namespace, a fresh object-store prefix, a
fresh cluster identity, and empty local working directories, and it assumes no
preexisting membership document, token set, or metadata directory. There is no
migration path from an earlier membership shape and none is described here: a
pre-0.19.0 membership document is not a valid input to this harness and is
rejected during preflight.

## Membership document: public material only

The membership document is supplied through `VELORIX_RHIZA_MEMBERS_JSON` and
must be a three-element array whose node IDs are exactly `velorix-meta-0`,
`velorix-meta-1`, and `velorix-meta-2`. Each member carries exactly four fields
and no others:

| field | value |
| --- | --- |
| `node_id` | `velorix-meta-<ordinal>`, matching the StatefulSet Pod name |
| `url` | `https://<node_id>.velorix-meta.<namespace>.svc.cluster.local:9090` |
| `peer_url` | `quic://<node_id>.velorix-meta.<namespace>.svc.cluster.local:8200` |
| `public_key` | standard padded base64 of the 32 raw Ed25519 public key bytes |

Rhiza 0.19.0 decodes this document with unknown fields disallowed, so a
per-member `token` field is a hard error rather than an ignored field. The
harness rejects the document outright, because a document that parses in one
layer and fails in another produces a misleading diagnostic.

`public_key` accepts exactly one encoding: 43 characters from `A-Za-z0-9+/`
followed by a single `=`. The URL-safe alphabet used to generate peer tokens is
not accepted here. `A-Za-z0-9+/` may legitimately appear in a published key, so
a key must never be transported through a URL-safe encoder on the way in.

The key is not independently generatable. It is derived:

```
seed     = HMAC-SHA256(key = peer token,
                       msg  = "rhiza-peer-certificate\0" clusterID "\0" nodeID)
public   = Ed25519 public key of that seed
public_key = standard padded base64 of those 32 bytes
```

Preflight recomputes this for every member and refuses to continue unless the
published key matches. The keys follow from `(cluster id, node ID, token)`, so
changing any one of the three rotates that node's identity and every member
document must be republished together. The harness verifies derivation against
the same cluster identity the workload will run with.

That identity is one name. `RHIZA_CLUSTER_ID` is the canonical, native, and
operator-owned variable, published in the `rhiza-config` ConfigMap. There is no
`VELORIX_RHIZA_CLUSTER_ID`: in operator-managed mode every Velorix-owned
`VELORIX_RHIZA_*` name except the opt-in is a hard startup conflict, so
`VELORIX_RHIZA_CLUSTER_ID` is not an alias for `RHIZA_CLUSTER_ID`, is not a
fallback, and must never be set. The harness supplies it as
`VELORIX_RHIZA_RUN_ID`, which is both the source `RHIZA_CLUSTER_ID` and the value
peer-key derivation is checked against, and the gate asserts the published
`RHIZA_CLUSTER_ID` against it after the operator rotates the generation.

## Operator-managed mode

`VELORIX_RHIZA_OPERATOR_MANAGED=1` is the only Velorix-owned variable that takes
part in this deployment, and it is an opt-in rather than configuration. It
makes `velorix-meta` read the canonical `RHIZA_*` environment natively and open
the private recovery listener on the required `RHIZA_BIND_ADDR` (the fixture sets `0.0.0.0:9091`),
which stays separate from the Velorix gRPC listener on `VELORIX_META_BIND`.
That separation is required, not cosmetic: the operator resolves each source Pod
through the container port named `recovery` and falls back to the wrong port
without it, and the gRPC port must never be published to the recovery listener.

The flag is mutually exclusive with every other `VELORIX_RHIZA_*` name. Setting
one of those alongside the flag is a startup error, because a shadow value that
the operator does not know about would be either silently ignored or, worse,
invite a later edit that contradicts the generation the operator just published.
The harness therefore asserts that the applied StatefulSet carries the flag, no
inline `RHIZA_PEER_TOKEN`, and no other `VELORIX_RHIZA_*` entry, both before and
after recovery.

The harness therefore requires `kubectl`, `jq`, and `openssl`, in addition to
the fixture wrapper's use of `openssl`. Key derivation needs no helper
language: it is HMAC-SHA256, a fixed 16-byte PKCS#8 Ed25519 prefix around the
resulting seed, `openssl pkey` to read out the SPKI public key, and standard
padded base64 of the trailing 32 bytes. Each intermediate result is length- and
alphabet-checked, so an OpenSSL build that cannot produce the accepted encoding
is rejected rather than allowed to publish a key no node would accept.

## Peer tokens: private, per node, never in the membership document

Private peer tokens are supplied separately through `VELORIX_RHIZA_PEER_TOKENS`,
a JSON object keyed by node ID, for example
`{"velorix-meta-0": "...", "velorix-meta-1": "...", "velorix-meta-2": "..."}`.
Tokens must match `[A-Za-z0-9][A-Za-z0-9._~-]{15,127}`. The three tokens must
be distinct, and none may equal `VELORIX_RHIZA_ADMIN_TOKEN`; Rhiza rejects both
conditions independently.

The two inputs are deliberately separated. The membership document is public
key material and is mounted from its own Secret. The private peer tokens travel
as a single standard Secret carrying exactly `RHIZA_ADMIN_TOKEN` and
`RHIZA_PEER_TOKENS`, the latter being the native node-keyed token map.

No private peer token is ever placed in the Pod spec, in the manifest, in the
membership document, or in the evidence bundle. Every Pod receives the whole map
and selects its own entry by `RHIZA_NODE_ID`, which is
`metadata.name`; that is the upstream shape, because a single Pod template
cannot mount a distinct Secret per replica. `RHIZA_PEER_TOKEN` also exists as an
inline worker input; the harness deliberately does not use it, because an inline
value would be stored in the Pod spec where the map is not.

What this does and does not establish: no private peer token is visible in the
Pod spec, and the harness asserts that against the applied StatefulSet. The
underlying Secret object still holds all three tokens and is readable by any
principal with Secret read access in that namespace, so this is Pod-spec
separation, not namespace or RBAC isolation. The recovery operator depends on
this same native shape: it republishes target credentials as
`rhiza-recovery-<id>` with the same two keys and rewrites the StatefulSet to
point at that Secret, and it must additionally blank any inherited scalar
`RHIZA_PEER_TOKEN`, because a scalar token alongside the map fails the next
process start.

## Fail-closed preflight

Preflight runs before any cluster mutation and refuses to proceed on any of the
following, with a message naming the offending input:

- a membership document that is not a three-member array of the required node IDs
- any member field outside the exact four-field allowlist, including a legacy
  `token`
- a `public_key` that is not 44-character standard padded base64
- a `public_key` that does not equal the derivation for that node, which also
  catches a document published for a different cluster ID
- a peer-token input that is not an object keyed by the three node IDs, or a
  token outside the accepted character and length bounds
- duplicate peer tokens, or a peer token equal to the admin token
- a Secret or fixed resource name already present in the isolated namespace
- any object-store, TLS, image, or namespace-isolation violation described below

## Executed workload

Native mTLS is mandatory for the executed workload. Set
`VELORIX_RHIZA_SERVER_TLS_SECRET` and `VELORIX_RHIZA_CLIENT_TLS_SECRET` to
different Secrets in the isolated namespace. Each Secret must contain
`tls.crt`, `tls.key`, and `ca.crt`; the server CA is the trusted client CA and
the client CA is the trusted server CA. If the namespace does not already
contain these Secrets, execution may create them from the six explicit local
file inputs `VELORIX_RHIZA_SERVER_TLS_CERT_FILE`,
`VELORIX_RHIZA_SERVER_TLS_KEY_FILE`, `VELORIX_RHIZA_SERVER_TLS_CLIENT_CA_FILE`,
`VELORIX_RHIZA_CLIENT_TLS_CERT_FILE`, `VELORIX_RHIZA_CLIENT_TLS_KEY_FILE`, and
`VELORIX_RHIZA_CLIENT_TLS_CA_FILE`. Certificate material is never emitted in
the manifest or evidence.

Required inputs are supplied through `VELORIX_RHIZA_*` environment variables.
`VELORIX_K8S_CONTEXT` is required but is never printed or written to evidence.
The object-store endpoint and credentials must refer to an externally managed
S3-compatible service. The harness defaults to TLS for that service (set
`VELORIX_RHIZA_OBJECT_STORE_INSECURE=1` only for an explicitly approved local
fixture); provide the native `host:port` endpoint without a URL scheme. It
sets `before-ack` durability and uses
`emptyDir` only for each node's local working directory.

With execution enabled, the harness creates a headless `velorix-meta` Service
with `publishNotReadyAddresses: true`, a three-replica `OnDelete` StatefulSet, a
`rhiza-config` ConfigMap, and three Secrets: `rhiza-object-store` for the shared
object-store location and credentials, `rhiza-peer-credentials` for
`RHIZA_ADMIN_TOKEN` and the node-keyed `RHIZA_PEER_TOKENS` map, and
`rhiza-client-credentials` for the unrelated Velorix gRPC bearer token. The
public membership document lives in the ConfigMap, not in a Secret, because it
holds nothing but derived public keys. The Service exposes a TCP gRPC port plus
a UDP QUIC peer port, and never the recovery port. The harness runs an
authenticated metadata service-connection smoke over HTTPS/mTLS. Readiness uses
the non-mutating `velorix-meta smoke --capabilities-only` linearizable capability
read. The StatefulSet uses `OnDelete` deliberately: a rolling update would
restart one new voter into the old fixed membership, which is exactly what the
upstream no-PVC contract forbids, and `kubectl rollout status` does not accept
that strategy, so readiness is polled on the StatefulSet status instead.

Recovery is performed by the official operator, not by the harness. After an
observation-only `RhizaRecovery` confirms the source generation's quorum, the
harness writes a unique catalog with a read-write probe and then applies a second
`RhizaRecovery` carrying an explicit `spec.fence` attestation. The operator then
seals the source archive, forks the certified history, mints the target
credentials, scales `spec.replicas` to zero, waits for every Pod owned by the
source StatefulSet UID to terminate, and only then publishes the target identity
and restores three replicas. The harness must not pre-destroy the source: the
upstream controller rejects a first reconcile whose StatefulSet does not declare
exactly three desired replicas, and it captures the certified archive suffix
from the live source Pod recovery endpoints during that same first pass. It also
refuses to activate the target while replicas are non-zero or while any source
Pod survives, so a `Complete` phase is itself proof that the whole generation,
`emptyDir` state included, was torn down by the official path. The harness then
runs `velorix-meta smoke --verify-only` against the exact catalog written before
recovery, against the Service and against each recovered Pod individually, so the
post-recovery check cannot recreate missing state, and finally runs a read-write
probe to prove the recovered generation is a live voter set rather than a
read-only artifact. Every later probe is strictly read-only except that final
one.
It fails if a PVC or `volumeClaimTemplates` is observed. It also re-reads the
applied StatefulSet and fails unless the shared peer-token map arrives through
the native `RHIZA_PEER_TOKENS` env entry with exactly the two native Secret keys,
no inline `RHIZA_PEER_TOKEN` is present, and no Velorix-owned `RHIZA_*` shadow
variable survives alongside the operator's own contract. Evidence is written to
`target/rhiza-kv-k8s` with production trust disabled; this is a validation
artifact, not a production durability attestation.

The image workflow builds `Dockerfile.meta` with the `rhiza-backend` feature;
its pinned Go 1.27.0 toolchain is used only in the builder stage for Rhiza's
native FFI.

## Operator image pin

The official operator image defaults to the published Rhiza 0.19.0 manifest
digest. `VELORIX_RHIZA_OPERATOR_IMAGE` may only be set to that exact reference;
anything else is refused, so an unset or edited input cannot downgrade the
component that replaces cluster identity, membership, and credentials. The
vendored manifests in `deploy/rhiza-k8s/operator` are separately verified
against `UPSTREAM.sha256` before anything is rendered, and the installed CRD is
compared against the vendored one so a different schema cannot silently change
what the operator accepts.

Upstream publishes the operator for `linux/amd64` only. A platform that cannot
execute that artifact has no honest substitute, but it does have one honest
alternative: build the same binary from the same pinned upstream source tree.
That is the only supported override, and it is explicit:

- `VELORIX_RHIZA_OPERATOR_IMAGE_OVERRIDE` — a digest reference to that locally
  built image, resolved by its real digest. No tag, no alias, and no invented
  digest standing in for the published one.
- `VELORIX_RHIZA_OPERATOR_IMAGE_PROVENANCE` — a required single-line record of
  how the image was built: upstream repository, tag, commit, and build method.

Without provenance the override is refused. Evidence reports
`operator_image_official_published_release: false`,
`operator_image_override_used: true`, and the provenance string, so an overridden
run is never presented as the published release image, and the production pin is
never weakened to make one run possible.

## Test-only Versity Gateway fixture

If no approved external S3 service is available, the explicitly opt-in
`scripts/run-rhiza-kv-k8s-fixture.sh` wrapper provisions a fresh, test-only
Versity Gateway Deployment in a new `velorix-rhiza-validation-fixture-*`
namespace, then delegates to the generic gate. Set
`VELORIX_RHIZA_FIXTURE_EXECUTE=1` and `VELORIX_RHIZA_FIXTURE_VERSITY_IMAGE` to an
immutable image reference.

The gateway is the official Versity Gateway, the same implementation
`scripts/check-rhiza-recovery.sh` builds from source, running its POSIX driver
against an `emptyDir` root as a non-root user with a read-only root filesystem.
The bucket is created as a directory directly in that POSIX root, which is how
the POSIX driver represents a bucket, so the fixture needs no provisioning client
and no separate bucket Job. Using the gateway this repository already builds
means the Kubernetes fixture and the local recovery drill speak S3 to one
implementation instead of two different emulations.

The fixture also generates the peer identities for the run: three random peer
tokens, the derived public key for each node against the run's cluster ID, a
membership document carrying only those public keys, and the node-keyed
peer-token object. Both are handed to the gate, so the fixture exercises the
same preflight derivation check as an operator-supplied document. The derivation
uses only `openssl`, and every intermediate encoding is validated, so an
OpenSSL build that cannot produce the accepted form fails the fixture instead of
publishing a key no node would accept. The fixture uses `emptyDir`, random
credentials, and generated short-lived certificates; it is retained by default for
inspection and can delete only its own created namespace with
`VELORIX_RHIZA_FIXTURE_CLEANUP=1`. Its evidence is explicitly marked
fixture-only and cannot establish provider-loss or production durability behavior.

## Local recovery regression and proof boundaries

`sh scripts/check-rhiza-recovery.sh` builds official Versity Gateway v1.8.0
(`fd04bc1df2656298577b82667a4195c77f8c7563`) into a run-local binary directory,
then starts an isolated loopback POSIX-backed S3
fixture and runs three native Rhiza nodes through the real `RhizaKvMetaStore`
snapshot/CAS path. It checks cross-node reads, competing checkpoint CAS writes
(one winner), continued operation with two voters, and fail-closed operation
without quorum. It then closes all nodes, verifies that empty working directories
cannot reuse registered voter identities, and restarts with the retained WAL
directories. It reads the exact acknowledged catalog, owner claim, and winning
checkpoint without recreating those records. The Kubernetes operator gate above
provides the separate evidence for recovery without PVCs.

The fixture requires Go, GNU `timeout`, netcat, curl with native AWS SigV4, and
`xmllint` (CI installs `libxml2-utils` only in the Rhiza job). Signed requests use
the same `us-east-1` region as Rhiza. Source builds, startup, client operations and tests are bounded. The
server binds only loopback, uses run-local scratch storage, and cleanup signals
only its own supervisor PID. Source SHAs and Go binary build information are
retained with the evidence; this changes fixture transport, not the S3 backend
or recovery assertions. Signed probes require conditional creation to succeed
once, duplicate creation and a wrong ETag to return 412 without changing the
body, and a matching ETag update to succeed. XML listings must be valid,
non-truncated, and nonempty.

These probes and the drill do not certify full S3 semantics or production
compatibility. In this pinned gateway, GET metadata and body opening are not
protected by the publication lock, so concurrent replacement can mix an old
ETag with a new body. Rhiza obtains CAS tokens through separate HEAD requests;
publisher claims and archive heads also check versions around reads. Recovery
pins and GC locks lack that second HEAD check. Concurrent DELETE and unexpected
metadata-error handling remain unverified limits. The drill's competing CAS
assertion concerns Rhiza's replicated metadata, not general S3 atomicity.

The former MinIO-backed local drill passed on 2026-09-05, including an independent rerun in 18.53
seconds. GitHub Actions runs this ignored integration test explicitly and
retains its logs and JSON evidence. The JSON summarizes passing assertions;
the test and logs are the underlying evidence.

This is a graceful cold-restart test, not a SIGKILL or power-loss test. The gateway
remains available throughout the retained-WAL restart. Neither this drill nor
the Kubernetes fixture establishes recovery from loss of the object-store
provider itself, migration of existing metadata, or production cutover.

Rhiza still reports no authoritative bounded-skew clock, no bounded wall-clock
failover, and no production multi-writer safety. The API's existing
`logical-fencing`/required production runtime gates must not be bypassed merely
because metadata service connectivity and recovery pass. Native mTLS client
wiring is separate from those runtime admission guarantees.

The drill now uses the 0.19.0 peer-identity contract. The Rust-side consumers of the membership document, the local recovery test
in `crates/velorix-meta/tests/rhiza_recovery.rs`, and
`scripts/check-rhiza-recovery.sh` build their own membership input and must
carry the same `public_key` and per-node `PeerToken` treatment as this harness.
They are outside this harness's contract and are not covered by it.

## Isolated kind evidence, 0.19.0 operator path

The 0.19.0 harness passed end to end again on 2026-10-08, on the current
branch source, against an explicitly selected disposable kind context, on a
single `arm64` control-plane node, with the official operator performing the
recovery. Evidence is under `/tmp/velorix-prepush-k8s-evidence/`, with
`gate/` holding everything the harness wrote and `final-snapshot/` holding the
operator journal, both Pod inventories, the applied manifests, the probe logs,
and the final cluster state, all copied out before the disposable cluster was
deleted. The earlier 2026-10-07 copy under `target/rhiza-kv-k8s-e2e019a/` was
removed with the ignored `target/` tree and is not cited below.

| fact | value |
| --- | --- |
| Meta image | `docker.io/library/velorix-meta@sha256:ec7ff64afd55c6d929fd15ceddcf5e1a938d3b1f954ea566387628f3faa70886`, built fresh for `linux/arm64`, confirmed as the running `imageID` of all three recovered Pods |
| recovery performed by | the official operator, `phase`/`stage` `Complete` |
| archive capture | `certified suffix captured from 3/3 reachable pod endpoints` |
| `recoveredTip` | `2`, positive |
| source cluster | `rhiza-kv-e2e019prepush` |
| target cluster | `rhiza-r-04115df1c7ee148c61295531`, a different identity |
| source StatefulSet UID | preserved across the whole generation replacement |
| recovered Pods | three new Pod UIDs, all owned by that UID, all created after the recovery request |
| PVCs in the namespace | zero |
| pre-loss probe | `catalog_store_outcome=Created` |
| post-recovery probe | `smoke verified ... mutations=0` for the same `catalog_probe_id` |
| per-Pod post-recovery probe | `mutations=0` against each of the three recovered voters |
| post-recovery write | supplemental probe with its own id returned `catalog_store_outcome=Created`, proving the recovered generation is a live voter set |

The operator ran as a locally built `arm64` binary
`docker.io/library/rhiza-operator@sha256:46e6694606eaed7d19dcb83538ff2e48dc11947b24260051c4472aaae50e45ff`,
compiled from a fresh clone of upstream tag `v0.19.0` commit
`abb87a0336cba8fee3fd1d9e5a0bf797de5b25b8` with the unmodified upstream
`Dockerfile.operator`, because upstream publishes `linux/amd64` only. The
vendored manifests were independently confirmed byte for byte against that
same clone before the run. The evidence records the run as
`operator_image_official_published_release: false` with the build provenance
attached; it is not evidence that the published `linux/amd64` artifact was
executed, and nothing about the production pin was relaxed to obtain it.

`Dockerfile.meta` cannot be built for `linux/arm64` unmodified: its builder
stage installs the Go 1.27.0 `linux-amd64` tarball and asserts that GOARCH.
The failing unmodified build is retained in the evidence directory, and the
`linux/arm64` Meta image above was produced from a build-time copy of
`Dockerfile.meta` held outside the repository whose only difference is those
three Go-toolchain lines; every other line, including both base-image index
digests and the `cargo build ... --features "rhiza-backend" --locked`
invocation, is unchanged, and the diff is retained alongside the run. The
committed Dockerfile and the CI `linux/amd64` path are untouched.

What this run does not establish is unchanged: it is a single-namespace fixture
whose only writer is the operator, so the fence attestation is harness ownership
rather than an attested external production fence, and the evidence records
`production_fence_attested: false` and `trusted_for_production: false`.

## Superseded Kubernetes evidence

The isolated fixture passed against an explicitly selected target context on
2026-09-05 using runtime source `c4d957b` and GHCR Meta image
`sha256:7c4f01896611a20f8c9130da9ace9a9c5f6ff3dc67b72e03c620c1fdacaf2b41`.
All three replacement Meta Pods were Running/Ready, used `emptyDir`, and the
namespace contained zero PVCs. Their UIDs differed from the original three
Pods. Each replacement node returned the exact pre-restart catalog over native
mTLS with bearer authentication and `mutations=0`. An independent coordinator
then repeated the read-only probe inside each Pod against its loopback endpoint.

That run used the pre-0.19.0 membership shape, in which every Pod read all three
voter tokens from one shared Secret. It is therefore not evidence for the
current harness and must not be cited for the peer-token separation or the
derived-key preflight. The local evidence directory from that run is
`target/rhiza-kv-live-fixture-approved-evidence/`. Cluster identifiers and
credentials are deliberately excluded from this document. The earlier default-
context run is not evidence for the selected target; operators must resolve and
check the explicit context before execution, rather than reuse current-context.
