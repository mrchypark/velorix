# Rhiza KV Kubernetes validation

`scripts/run-rhiza-kv-k8s-gate.sh` is an explicit, isolated validation harness
for the embedded Rhiza KV metadata service. It is preflight-only by default.
It does not select a cluster, create a namespace, or change Kubernetes state
unless `VELORIX_RHIZA_EXECUTE=1` is supplied.

The harness expects an immutable `velorix-meta` image and a Secret-backed JSON
membership document. The document must contain exactly these three node IDs:
`velorix-meta-0`, `velorix-meta-1`, and `velorix-meta-2`. Each member's `url`
is the matching `https://` service DNS address and `peer_url` is the matching
`quic://` address. Voter tokens are supplied by the operator; the harness
never invents defaults or prints them.

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
with `publishNotReadyAddresses: true`, a three-replica StatefulSet, and a
validation Secret. The Service exposes a TCP gRPC port plus a UDP QUIC peer
port. It runs an authenticated metadata service-connection
smoke over HTTPS/mTLS. Readiness uses the non-mutating
`velorix-meta smoke --capabilities-only` linearizable capability read. It
scales the isolated StatefulSet to zero, waits for every selected Pod to be
deleted, then restores three replicas and runs `velorix-meta smoke
--verify-only` against the exact catalog written before replacement. Thus the
post-restart check cannot recreate missing state.
It fails if a PVC or `volumeClaimTemplates` is observed. Evidence is written to
`target/rhiza-kv-k8s` with production trust disabled; this is a validation
artifact, not a production durability attestation.

The image workflow builds `Dockerfile.meta` with both `hiqlite-backend` and
`rhiza-backend`; its pinned Go 1.27.0 toolchain is used only in the builder
stage for Rhiza's native FFI.

If no approved external S3 service is available, the explicitly opt-in
`scripts/run-rhiza-kv-k8s-fixture.sh` wrapper provisions a fresh, test-only
MinIO Deployment and bucket in a new `velorix-rhiza-validation-fixture-*`
namespace, then delegates to the generic gate. Set
`VELORIX_RHIZA_FIXTURE_EXECUTE=1`, `VELORIX_RHIZA_FIXTURE_MINIO_IMAGE`, and
`VELORIX_RHIZA_FIXTURE_MC_IMAGE` to immutable image references. The fixture
uses `emptyDir`, random credentials, and generated short-lived certificates;
it is retained by default for inspection and can delete only its own created
namespace with `VELORIX_RHIZA_FIXTURE_CLEANUP=1`. Its evidence is explicitly
marked fixture-only and cannot establish provider-loss or production
durability behavior.

## Local recovery regression and proof boundaries

`sh scripts/check-rhiza-recovery.sh` starts a digest-pinned, isolated MinIO
fixture and runs three native Rhiza nodes through the real `RhizaKvMetaStore`
snapshot/CAS path. It checks cross-node reads, competing checkpoint CAS writes
(one winner), continued operation with two voters, and fail-closed operation
without quorum. It then closes all nodes, retains their old directories, opens
three empty working directories, and reads the exact acknowledged catalog,
owner claim, and winning checkpoint without recreating those records.

The local drill passed on 2026-09-05, including an independent rerun in 18.53
seconds. GitHub Actions runs this ignored integration test explicitly and
retains its logs and JSON evidence. The JSON summarizes passing assertions;
the test and logs are the underlying evidence.

This is a graceful cold-restart test, not a SIGKILL or power-loss test. MinIO
remains available while metadata nodes lose local state. Neither this drill nor
the Kubernetes fixture establishes recovery from loss of the object-store
provider itself, migration of existing metadata, or production cutover.

Rhiza still reports no authoritative bounded-skew clock, no bounded wall-clock
failover, and no production multi-writer safety. The API's existing
`logical-fencing`/required production runtime gates must not be bypassed merely
because metadata service connectivity and recovery pass. Native mTLS client
wiring is separate from those runtime admission guarantees.
