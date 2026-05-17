# S3 API Testing With RustFS

RustFS is a 100% S3-compatible object store. Velorix uses the RustFS gate as
live S3-compatible evidence because the storage, runtime, and benchmark
harnesses talk to a RustFS server through the normal S3 API rather than an
in-process or filesystem fake.

The generated evidence records both readiness evidence kinds it exercises:
`s3_compatible` and `s3_compatible_integration_harness`.

Prerequisites:

```bash
docker version
cargo --version
```

Run the full RustFS S3 gate:

```bash
scripts/run-rustfs-s3-gate.sh
```

The script starts `rustfs/rustfs:latest` on `http://127.0.0.1:9000`, creates the
configured bucket through the AWS S3 API, and sets the normal live harness
environment:

```text
VELORIX_S3_COMPAT=1
AWS_ENDPOINT_URL=http://127.0.0.1:9000
AWS_ACCESS_KEY_ID=rustfsadmin
AWS_SECRET_ACCESS_KEY=rustfsadmin
AWS_REGION=us-east-1
VELORIX_S3_BUCKET=velorix-rustfs
```

Before creating the RustFS container, the script creates a disposable Docker
network and runs a short-lived AWS CLI container on it. This catches broken
Docker bridge-network state before the gate writes evidence artifacts.

On success, the gate writes:

```text
target/velorix-s3/rustfs-s3-gate-evidence.json
target/velorix-bench/rustfs-s3-nightly.json
```

The benchmark step can be skipped when only storage/runtime API behavior is
needed:

```bash
VELORIX_RUSTFS_RUN_BENCHMARK=0 scripts/run-rustfs-s3-gate.sh
```

When the benchmark step is enabled, the script sets
`VELORIX_BENCHMARK_EVIDENCE_SCOPE=live_or_native`. The resulting benchmark JSON
can be used as S3-compatible gate input after it is measured against RustFS and
then compared with `velorix-cli benchmark-gate` at the selected gate level.
S3-compatible gate results must carry an explicit `backend_evidence_scope` field
so missing scope metadata cannot default into release-quality evidence.

## GitHub Actions

The manual `RustFS S3-Compatible Gate` workflow runs the same script on an
Ubuntu runner and uploads `rustfs-s3-compatible-evidence` with the RustFS-backed
S3 evidence JSON. Its `run-benchmark` input defaults to `false`; enable it only
when the slower benchmark JSON is useful for review.

Keep the RustFS container for debugging. The script uses run-scoped container and
network names by default, so set explicit names when you want to inspect them:

```bash
VELORIX_RUSTFS_CLEANUP=0 \
VELORIX_RUSTFS_CONTAINER=velorix-rustfs-s3 \
VELORIX_RUSTFS_NETWORK=velorix-rustfs-s3 \
  scripts/run-rustfs-s3-gate.sh
docker logs velorix-rustfs-s3
docker rm -f velorix-rustfs-s3
docker network rm velorix-rustfs-s3
```

`fsouza/fake-gcs-server` is a Google Cloud Storage emulator, not an S3 REST XML
endpoint. Use it only for a future GCS-specific backend harness; do not wire it
into `VELORIX_S3_COMPAT` or use it for S3-compatible benchmark baselines.
