# S3 API Testing With Versity Gateway

Versity Gateway exposes an S3 API over POSIX storage. Velorix uses its live
gate to verify the API operations required by the storage and ingest-admission
harnesses. Compatibility is established by those checks, including conditional
PUT/CAS, rather than assumed from the backend name.

Fresh Versity Gateway evidence is pending until this gate passes. Historical
RustFS measurements remain RustFS evidence and must not be relabeled as Versity
Gateway results.

The generated S3 gate evidence records the readiness evidence kinds it
exercises, `s3_compatible` and `s3_compatible_integration_harness`, plus
gate-local detail for ingest-admission crash/restart. Production GC execution
remains unavailable without the durable cross-process coordinator; the gate
records `s3_compatible_gc_execution_unavailable` and emits no production GC
artifact. Setting `VELORIX_VERSITYGW_RUN_PRODUCTION_GC_EVIDENCE=1` exits 75 before
Docker or tests start.

Prerequisites:

The external manual product runner, `scripts/run-vind-product-external-versitygw.sh`, also requires `jq` and `openssl`.

```bash
docker version
cargo --version
df -h .
```

The gate checks available disk space before starting Docker or Cargo work. The
default minimum is 4 GiB on the repository filesystem; override
`VELORIX_VERSITYGW_MIN_FREE_KIB` only when the lower threshold has been reviewed for
the current machine and target cache state.
Versity Gateway gate Cargo builds use `target/versitygw-s3-gate` by default, keeping
live-gate build artifacts under the repository's normal local target tree while
separating them from the default development profile artifacts. Set
`VELORIX_VERSITYGW_CARGO_TARGET_DIR` only when a different local target cache is
needed.

Run the full Versity Gateway S3 gate:

```bash
scripts/run-versitygw-s3-gate.sh
```

The script starts `versity/versitygw:v1.8.0` on
`http://127.0.0.1:9000`, creates the configured bucket through the AWS S3 API,
and sets the normal live harness environment. Override `VELORIX_VERSITYGW_IMAGE`
with another version tag or digest if needed; mutable tags such as `latest` are
rejected unless `VELORIX_ALLOW_MUTABLE_VERSITYGW_IMAGE=1` is set explicitly. The
AWS CLI helper image is pinned to `amazon/aws-cli:2.17.36`; override
`VELORIX_AWS_CLI_IMAGE` with another version tag or digest if needed, or set
`VELORIX_ALLOW_MUTABLE_AWS_CLI_IMAGE=1` to opt into a mutable helper tag.
The script uses run-local non-default credentials unless
`VELORIX_VERSITYGW_ACCESS_KEY` and `VELORIX_VERSITYGW_SECRET_KEY` are supplied.
These script settings become the container environment `ROOT_ACCESS_KEY` and
`ROOT_SECRET_KEY`. The container receives `--port :9000 posix /data` and a fresh
POSIX data volume. Existing RustFS data requires an S3-level export/copy;
attaching a raw RustFS volume to Versity Gateway is not a supported migration.

```text
VELORIX_S3_COMPAT=1
AWS_ENDPOINT_URL=http://127.0.0.1:9000
AWS_ACCESS_KEY_ID=<run-local non-default Versity Gateway access key>
AWS_SECRET_ACCESS_KEY=<run-local non-default Versity Gateway secret key>
AWS_REGION=us-east-1
VELORIX_S3_BUCKET=velorix-versitygw
```

The live harness enables S3 conditional PUT (`aws_conditional_put=etag`) and
checks both create-only writes and ETag-based conditional update. This is the
storage proof needed by active view CAS in `velorix-api`; a backend's generic
S3-compatible claim is not enough unless this check passes for the selected
Versity Gateway/S3-compatible image and endpoint.

Before creating the Versity Gateway container, the script creates a disposable Docker
network and runs a short-lived AWS CLI container on it. This catches broken
Docker bridge-network state before the gate writes evidence artifacts.

On success, the gate writes:

```text
target/velorix-s3/versitygw-s3-gate-evidence.json
```

The gate does not run benchmarks: its JSON always records `benchmark.ran=false`
with null result and validation paths. Fresh benchmark measurements and
production GC deletion evidence remain separate, pending release requirements.
`versitygw-production-gc-evidence-validate` requires a complete live seed/run/
production artifact family and rejects this compatibility-only gate output.
It also requires `versitygw_image` to identify the official `versity/versitygw`
repository pinned by digest, matching `versitygw_image_digest` from the running
container. Historical `rustfs_*` gate fields and `s3://rustfs` family authorities
are rejected even if their evidence kind is relabeled. This binds the declared
provider identity; it does not independently authenticate artifact provenance.

## GitHub Actions

The manual `Versity Gateway S3-Compatible Gate` workflow runs the same script on an
Ubuntu runner and uploads `versitygw-s3-compatible-evidence` with the Versity Gateway-backed
S3 evidence JSON. It has no benchmark input and uploads no benchmark or
production GC artifacts.

Keep the Versity Gateway container for debugging. The script uses run-scoped container and
network names by default, so set explicit names when you want to inspect them:

```bash
VELORIX_VERSITYGW_CLEANUP=0 \
VELORIX_VERSITYGW_CONTAINER=velorix-versitygw-s3 \
VELORIX_VERSITYGW_NETWORK=velorix-versitygw-s3 \
  scripts/run-versitygw-s3-gate.sh
docker logs velorix-versitygw-s3
docker rm -f velorix-versitygw-s3
docker network rm velorix-versitygw-s3
```

`fsouza/fake-gcs-server` is a Google Cloud Storage emulator, not an S3 REST XML
endpoint. Use it only for a future GCS-specific backend harness; do not wire it
into `VELORIX_S3_COMPAT` or use it for S3-compatible benchmark baselines.
