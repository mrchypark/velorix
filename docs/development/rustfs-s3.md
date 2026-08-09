# S3 API Testing With RustFS

RustFS is a 100% S3-compatible object store. Velorix uses the RustFS gate as
live S3-compatible evidence because the storage, runtime, and benchmark
harnesses talk to a RustFS server through the normal S3 API rather than an
in-process or filesystem fake.

The generated S3 gate evidence records the readiness evidence kinds it
exercises, `s3_compatible` and `s3_compatible_integration_harness`, plus
gate-local detail for ingest-admission crash/restart and GC execution/retention.
By default, the script also emits a separate production GC verifier artifact;
that artifact is release evidence input, not a claim that the full readiness
report is production-ready.

Prerequisites:

```bash
docker version
cargo --version
df -h .
```

The gate checks available disk space before starting Docker or Cargo work. The
default minimum is 4 GiB on the repository filesystem; override
`VELORIX_RUSTFS_MIN_FREE_KIB` only when the lower threshold has been reviewed for
the current machine and target cache state.
RustFS gate Cargo builds use `target/rustfs-s3-gate` by default, keeping
live-gate build artifacts under the repository's normal local target tree while
separating them from the default development profile artifacts. Set
`VELORIX_RUSTFS_CARGO_TARGET_DIR` only when a different local target cache is
needed.

Run the full RustFS S3 gate:

```bash
scripts/run-rustfs-s3-gate.sh
```

The script starts `rustfs/rustfs:1.0.0-beta.4` on
`http://127.0.0.1:9000`, creates the configured bucket through the AWS S3 API,
and sets the normal live harness environment. Override `VELORIX_RUSTFS_IMAGE`
with another version tag or digest if needed; mutable tags such as `latest` are
rejected unless `VELORIX_ALLOW_MUTABLE_RUSTFS_IMAGE=1` is set explicitly. The
AWS CLI helper image is pinned to `amazon/aws-cli:2.17.36`; override
`VELORIX_AWS_CLI_IMAGE` with another version tag or digest if needed, or set
`VELORIX_ALLOW_MUTABLE_AWS_CLI_IMAGE=1` to opt into a mutable helper tag.
The script refuses the RustFS default root credential pair and uses run-local
non-default credentials unless `VELORIX_RUSTFS_ACCESS_KEY` and
`VELORIX_RUSTFS_SECRET_KEY` are supplied.

```text
VELORIX_S3_COMPAT=1
AWS_ENDPOINT_URL=http://127.0.0.1:9000
AWS_ACCESS_KEY_ID=<run-local non-default RustFS access key>
AWS_SECRET_ACCESS_KEY=<run-local non-default RustFS secret key>
AWS_REGION=us-east-1
VELORIX_S3_BUCKET=velorix-rustfs
```

The live harness enables S3 conditional PUT (`aws_conditional_put=etag`) and
checks both create-only writes and ETag-based conditional update. This is the
storage proof needed by active view CAS in `velorix-api`; a backend's generic
S3-compatible claim is not enough unless this check passes for the selected
RustFS/S3-compatible image and endpoint.

Before creating the RustFS container, the script creates a disposable Docker
network and runs a short-lived AWS CLI container on it. This catches broken
Docker bridge-network state before the gate writes evidence artifacts.

On success, the gate writes:

```text
target/velorix-s3/rustfs-s3-gate-evidence.json
target/velorix-bench/rustfs-s3-nightly.json
target/release-evidence/rustfs-production-gc.json
```

The benchmark step can be skipped when only storage/runtime API behavior is
needed:

```bash
VELORIX_RUSTFS_RUN_BENCHMARK=0 scripts/run-rustfs-s3-gate.sh
```

The production GC artifact step can be disabled for fast local diagnostics:

```bash
VELORIX_RUSTFS_RUN_PRODUCTION_GC_EVIDENCE=0 scripts/run-rustfs-s3-gate.sh
```

When enabled, the script runs the S3 compatibility GC harness under a known
prefix, then runs `velorix-cli gc-production-evidence --json` against that same
prefix and writes `target/release-evidence/rustfs-production-gc.json`. After the
gate JSON is written it also runs `rustfs-production-gc-evidence-validate` and
writes `target/release-evidence/rustfs-production-gc-validation.json`, binding
the gate JSON, seed fixture, executed `GcRunV1`, and production evidence to the
same authority store, run id, fixed retain-1 policy, persisted run digest, and
seed-declared full deleted object keys. The persisted-run digest is computed
from a canonical projection of the `GcRunV1`, not from raw Rust struct field
order. The fixed release smoke fixture uses two checkpoints, so
`VELORIX_RUSTFS_PRODUCTION_GC_RETAIN_LATEST_MANIFESTS` must remain `1` until
the fixture is generalized. The RustFS gate evidence includes the production GC
artifact path and details only after that verifier command succeeds. Regenerate
the seed/run/production/validation family together after schema changes; stale
pre-digest artifacts fail closed.

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
