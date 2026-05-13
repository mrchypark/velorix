# Local S3 API Testing With Floci

Floci is a disposable local AWS emulator with S3 support. It lets Velorix run
the existing S3-compatible storage, runtime, and benchmark harnesses without
writing to a shared object store.

This is local emulator evidence only. It exercises Velorix's S3-compatible
client wiring, capability probes, catalog/query/table paths, and benchmark JSON
generation, but it does not replace measured release-quality S3-compatible
backend evidence.
The generated evidence records both local readiness evidence kinds it exercises:
`s3_compatible` and `s3_compatible_integration_harness`.

Prerequisites:

```bash
docker version
cargo --version
```

Run the full local Floci S3 gate:

```bash
scripts/run-floci-s3-gate.sh
```

The script starts `floci/floci:latest` on `http://127.0.0.1:4566`, creates the
configured bucket through the AWS S3 API, and sets the normal live harness
environment:

```text
VELORIX_S3_COMPAT=1
AWS_ENDPOINT_URL=http://127.0.0.1:4566
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_REGION=us-east-1
VELORIX_S3_BUCKET=velorix-floci
```

Before creating the Floci container, the script creates a disposable Docker
network and runs a short-lived AWS CLI container on it. This catches broken
Docker bridge-network state before the gate writes evidence artifacts.

On success, the gate writes:

```text
target/velorix-s3/floci-s3-gate-evidence.json
target/velorix-bench/floci-s3-nightly.json
```

The benchmark step can be skipped when only storage/runtime API behavior is
needed:

```bash
VELORIX_FLOCI_RUN_BENCHMARK=0 scripts/run-floci-s3-gate.sh
```

When the benchmark step is enabled, the script sets
`VELORIX_BENCHMARK_EVIDENCE_SCOPE=local_emulator`. The resulting benchmark JSON
can be validated for local review, but `velorix-cli benchmark-gate` rejects it
as S3-compatible nightly or release comparison evidence. S3-compatible gate
results must carry an explicit `backend_evidence_scope` field so missing scope
metadata cannot default into release-quality evidence.

## GitHub Actions

The manual `Floci S3 Emulator Gate` workflow runs the same script on an
Ubuntu runner and uploads `floci-s3-emulator-evidence` with the local emulator
evidence JSON. Its `run-benchmark` input defaults to `false`; enable it only
when the slower local benchmark JSON is useful for review.

Keep the Floci container for debugging. The script uses run-scoped container and
network names by default, so set explicit names when you want to inspect them:

```bash
VELORIX_FLOCI_CLEANUP=0 \
VELORIX_FLOCI_CONTAINER=velorix-floci-s3 \
VELORIX_FLOCI_NETWORK=velorix-floci-s3 \
  scripts/run-floci-s3-gate.sh
docker logs velorix-floci-s3
docker rm -f velorix-floci-s3
docker network rm velorix-floci-s3
```

`fsouza/fake-gcs-server` is a Google Cloud Storage emulator, not an S3 REST XML
endpoint. Use it only for a future GCS-specific backend harness; do not wire it
into `VELORIX_S3_COMPAT` or use it for S3-compatible benchmark baselines.
