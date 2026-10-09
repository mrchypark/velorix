# Meta Service

The metadata service owns hot control-plane metadata. Object storage owns large
artifacts, checkpoint payloads, and the durable recovery archive. Foyer remains a
non-authoritative local cache only.

The product path is the embedded Rhiza KV backend. It is not production
multi-writer safe and does not provide bounded wall-clock failover: the backend
reports `authoritative_backend_time=false`,
`backend_time_source_kind=process_clock`,
`lease_authority_kind=rhiza_quepaxa_root_cas`,
`lease_expiry_semantics=process_clock_ttl`, `bounded_wall_clock_failover=false`,
`multi_writer_fencing_safe=false`, and `production_multi_writer_safe=false`.
Those values are the current product truth and must not be relabeled to clear a
release gate. What exists today is a no-PVC manual generation replacement shown
by the official operator's `RhizaRecovery`; automatic operator fencing is not
part of that evidence.

## First Meta API Slice

`crates/velorix-meta` defines the first typed gRPC boundary:

- `StoreRelationCatalog`
- `ReadRelationCatalog`
- `ReserveIngestRange`

The current implementation has:

- an in-memory backend for contract tests and local API work
- a `GrpcMetaStore` client used by `velorix-api`
- an `OssMetaStore` backend for standalone or low-cost deployments
- a `RhizaKvMetaStore` backend behind the `rhiza-backend` feature

The in-memory backend is not durable and must not be used as the production
meta authority. The object-store backend is durable and simple, but it keeps
metadata on the same object-store primitives as the existing replay path, so it
is best for small deployments, standalone E2E, and cost-optimized operation.

For local contract work, the gRPC service can be built with:

```bash
cargo run -p velorix-meta
```

## Rhiza KV service mode

The metadata binary also supports the embedded Rhiza KV backend behind the
`rhiza-backend` feature. Membership is fixed explicitly with either
`VELORIX_RHIZA_MEMBERS_JSON` or a secret-mounted
`VELORIX_RHIZA_MEMBERS_FILE` (never both). The value is a JSON array whose
members carry exactly `node_id`, `url`, `peer_url` (`quic://...`), and
`public_key` fields. Under the Rhiza 0.19.0 contract `public_key` is required
public material: standard padded base64 of the 32 raw Ed25519 public key
bytes, never the URL-safe alphabet. Any other field, including the legacy
per-member `token`, is a hard unknown-field error rather than an ignored key.
`public_key` is empty only for a single-node cluster that authenticates no
peers. The local `VELORIX_RHIZA_NODE_ID` must be one of the members. A
multi-node cluster also requires this node's own private peer token through
exactly one of `VELORIX_RHIZA_PEER_TOKEN` or
`VELORIX_RHIZA_PEER_TOKEN_FILE` (never both), and that token must differ from
`VELORIX_RHIZA_ADMIN_TOKEN`. Private peer tokens and derived keys never enter
the membership document, a log line, or diagnostics.
`VELORIX_RHIZA_PEER_ADDR` is the native bind address (`host:port`); the
`quic://` scheme belongs only in the member `peer_url`. For the native
S3-compatible provider, `VELORIX_RHIZA_OBJECT_STORE_ENDPOINT` is also a host
and port (for example `versitygw:9000`), with
`VELORIX_RHIZA_OBJECT_STORE_INSECURE=1` selecting HTTP.

This contract is greenfield. A run starts from a fresh empty
`VELORIX_RHIZA_DATA_DIR` with newly generated identities, and there is no
migration path from a pre-0.19.0 membership document; such a document is not a
valid input.

Development Rhiza mode is explicitly single-node and defaults to asynchronous
object-store durability. Production mode requires exactly three voters,
explicit S3/GCS/Azure object storage, and
`VELORIX_RHIZA_OBJECT_STORE_DURABILITY=before-ack`; no local filesystem object
store is accepted as production durability. The service performs a
linearizable metadata read before opening its gRPC listener, so an unavailable
Rhiza quorum fails startup. Rhiza's production fencing capability remains
fail-closed until the separately recorded three-node recovery evidence exists.

Production transport may use native mutual TLS with
`VELORIX_META_TRANSPORT_SECURITY=native-mtls` and the server files
`VELORIX_META_TLS_CERT_FILE`, `VELORIX_META_TLS_KEY_FILE`, and
`VELORIX_META_TLS_CLIENT_CA_FILE`. Every client must present an identity signed
by the configured CA. `service-mesh-mtls` remains an operator-attested
external boundary and does not enable native TLS in this binary.

The read-only smoke probes accept the same trust material through
`VELORIX_META_TLS_CA_FILE`, `VELORIX_META_TLS_CLIENT_CERT_FILE`,
`VELORIX_META_TLS_CLIENT_KEY_FILE`, and optional
`VELORIX_META_TLS_DOMAIN_NAME`. Use `smoke --capabilities-only` for an
authenticated, non-mutating linearizable readiness check. Use
`smoke --probe-unauthenticated` with a trusted client identity to verify that
omitting the bearer token is rejected; this probe never falls back to a
plaintext or insecure TLS channel.

Example development configuration. Development Rhiza mode is single-node, so
the one membership entry publishes no key and needs no peer token, and
`VELORIX_RHIZA_DATA_DIR` must be a fresh empty directory:

```bash
VELORIX_META_MODE=development \
VELORIX_META_BIND=127.0.0.1:9090 \
VELORIX_META_BACKEND=rhiza-kv \
VELORIX_RHIZA_DATA_DIR=/tmp/velorix-rhiza \
VELORIX_RHIZA_NODE_ID=dev-1 \
VELORIX_RHIZA_CLUSTER_ID=velorix-dev \
VELORIX_RHIZA_BIND_ADDR=127.0.0.1:8100 \
VELORIX_RHIZA_PEER_ADDR=127.0.0.1:8200 \
VELORIX_RHIZA_MEMBERS_JSON='[{"node_id":"dev-1","url":"http://127.0.0.1:8100","peer_url":"quic://127.0.0.1:8200","public_key":""}]' \
cargo run -p velorix-meta --features rhiza-backend
```

Production startup is fail-closed: it requires an explicit mode, bind address,
durable backend, bearer token, and native mTLS certificate files. Unverified
service-mesh attestation is not accepted by this binary. It also requires exactly
three Rhiza voters plus before-ack object storage for `rhiza-kv`.
This backend can be used for catalog/admission durability work, but it must not
be used to satisfy `VELORIX_STANDING_RUNTIME_FENCING=required`.

For the isolated three-node no-PVC operator recovery gate, see
[development/rhiza-kv-k8s.md](../development/rhiza-kv-k8s.md). It is a validation
artifact with production trust disabled, not a durability attestation.

Run the object-store backend with the same S3-compatible settings used by
`velorix-api`:

```bash
VELORIX_META_MODE=development \
VELORIX_META_BACKEND=oss \
VELORIX_META_BIND=127.0.0.1:9090 \
VELORIX_S3_COMPAT=1 \
AWS_ENDPOINT_URL=http://127.0.0.1:9000 \
AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
VELORIX_S3_BUCKET=velorix \
VELORIX_S3_PREFIX=meta \
cargo run -p velorix-meta
```

For non-durable local API work, memory mode must also be explicit:

```bash
VELORIX_META_MODE=development VELORIX_META_BACKEND=memory cargo run -p velorix-meta
```

Point `velorix-api` at the meta service with:

```bash
VELORIX_META_GRPC_ENDPOINT=http://velorix-meta:9090 cargo run -p velorix-api
```

For a native-mTLS metadata endpoint, set the complete client bundle on the API
as well: `VELORIX_META_TLS_CA_FILE`,
`VELORIX_META_TLS_CLIENT_CERT_FILE`,
`VELORIX_META_TLS_CLIENT_KEY_FILE`, and `VELORIX_META_TLS_DOMAIN_NAME`, with an
`https://` endpoint. Partial bundles and HTTPS without verification material
fail closed.

or containerized with:

```bash
docker build -f Dockerfile.meta -t velorix-meta:dev .
```

## Deployment Shape

Velorix product evidence must not depend on PVC-created local disk. Rhiza nodes
keep their working state on ephemeral `emptyDir` volumes and rely on object
storage for the durable archive, so a cluster loss is recovered by generation
replacement rather than by restoring a volume.

## Required Product Semantics

The meta service is the hot admission authority. In `rhiza-kv` and `oss` modes,
object storage still receives recovery evidence and batch payloads so the
existing replay/query path can validate committed batches after restart.

Append admission order is:

1. Reserve the relation ingest range through the gRPC meta service.
2. Materialize Velorix recovery evidence to object storage.
3. Append the ingest batch payload to object storage with create-only semantics.
4. Periodically back up metadata to object storage.

If a range reservation succeeds but the payload append fails, an identical retry
is admitted as a duplicate metadata reservation and can complete the missing
object-store payload write.