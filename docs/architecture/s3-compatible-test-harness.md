# S3-Compatible Test Harness

Status: Accepted
Applies to: env-gated live object-store evidence for Velorix 1.0 readiness.

The harness validates S3-compatible object-store behavior only when explicitly
enabled. It does not create a new authority model: object storage remains the
durable authority, Velorix-owned manifests remain the production contract, and
the harness only proves backend assumptions that local filesystem tests cannot
prove.

## Environment Contract

The live storage test target is compiled only when the explicit Cargo feature is
enabled:

```bash
cargo test -p velorix-storage --test s3_compat --features s3-compat-tests
cargo test -p velorix-runtime --test s3_compat_query --features s3-compat-tests
cargo bench -p velorix-runtime --bench s3_incremental --features s3-compat-tests
```

When that target is enabled, the test still skips unless:

```text
VELORIX_S3_COMPAT=1
```

When enabled, these variables are required:

```text
AWS_ENDPOINT_URL
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION
VELORIX_S3_BUCKET
```

`VELORIX_S3_PREFIX` is optional. Each live run appends a unique run prefix under
the configured prefix before writing objects, so independent runs do not share
keys. Live tests clean up written objects with best-effort deletes.

## Storage Harness

`crates/velorix-storage/tests/s3_compat.rs` builds an `object_store` 0.12
`AmazonS3` client from the environment and validates these observable
behaviors:

- create-only `put` succeeds for a new key
- create-only `put` fails for the same key
- `get` after `put` returns the exact bytes
- `list` by prefix observes the written key
- range read returns the expected bytes
- the authoritative object-store capability probe validates startup
  capabilities for every authoritative namespace under the configured prefix
- cleanup deletes the written key on a best-effort basis

These are capability checks for production assumptions, not replacements for
Velorix checkpoint, ingest, output, or catalog manifests.

## Runtime Query Harness

`crates/velorix-runtime/tests/s3_compat_query.rs` is also feature-gated and
skipped by default. When enabled, it builds both object-store clients used by
the runtime boundary:

- `object_store` 0.12 for Velorix authority/catalog/probe writes.
- `object_store` 0.13 for DataFusion 53 Parquet scans.

The test writes Parquet under the configured S3-compatible prefix, registers a
production table through the storage registry's authority-store probe path,
stores the relation catalog and query policy in object storage, writes
catalog-aware ingest envelopes for recovery coverage, and verifies a DataFusion
aggregate query over the registered table. This proves the current two-version
object-store boundary without adding an adapter or changing SlateDB/DataFusion
dependency versions.

## Skip Behavior

Without `--features s3-compat-tests`, default storage test builds do not compile
the live S3 harnesses or enable the S3 HTTP/TLS stacks. Without
`VELORIX_S3_COMPAT=1`, the explicitly enabled tests return early and print a
skip message. This keeps normal local and PR runs deterministic and avoids
accidental writes to shared MinIO or S3 buckets.

## Nightly Workflow Gate

`.github/workflows/nightly.yml` keeps benchmark evidence and live backend
evidence independent:

- `S3_BENCHMARK_RESULT_PATH` may be omitted only when live S3-compatible tests
  are explicitly requested. When set, the workflow validates that existing JSON
  against `baselines/benchmark/s3/nightly.json`. When omitted during explicit
  live opt-in, the workflow generates a new S3-compatible benchmark JSON result
  under `target/velorix-bench/s3-nightly.json`, gates it, and uploads it.
- Live S3-compatible tests and benchmark generation run only after explicit
  opt-in. Manual runs use the `run-live-s3-compat` input. Scheduled runs
  require the repository variable `VELORIX_RUN_LIVE_S3_COMPAT` set to `1`,
  `true`, or `yes`.
- If live tests are requested, the workflow fails before running tests unless
  `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
  `AWS_REGION`, and `VELORIX_S3_BUCKET` are present. `VELORIX_S3_PREFIX` remains
  optional.
- If live tests are not requested, the workflow does not set
  `VELORIX_S3_COMPAT=1`, so scheduled runs cannot write to S3-compatible storage
  only because credentials happen to exist.
- If neither benchmark JSON nor live S3-compatible tests are configured, the
  nightly gate fails closed instead of passing without S3 evidence.

## RustFS S3-Compatible Gate

For RustFS-backed S3 API compatibility checks,
`scripts/run-rustfs-s3-gate.sh` starts a RustFS container, creates a test bucket
through the AWS S3 API, and runs the same env-gated storage/runtime harnesses
against `http://127.0.0.1:9000`.
Before setup, it creates a disposable Docker bridge network and runs a
short-lived AWS CLI container on that network so Docker network-store or
container-attach failures fail before evidence artifacts are written.

```bash
scripts/run-rustfs-s3-gate.sh
```

The script sets the normal live harness environment:

```text
VELORIX_S3_COMPAT=1
AWS_ENDPOINT_URL=http://127.0.0.1:9000
AWS_ACCESS_KEY_ID=rustfsadmin
AWS_SECRET_ACCESS_KEY=rustfsadmin
AWS_REGION=us-east-1
VELORIX_S3_BUCKET=velorix-rustfs
VELORIX_S3_PREFIX=rustfs-s3-gate/<timestamp>
```

It runs:

```bash
cargo test -p velorix-storage --test s3_compat --features s3-compat-tests
cargo test -p velorix-storage --test multi_process_ingest_admission --features s3-compat-tests
cargo test -p velorix-runtime --test s3_compat_query --features s3-compat-tests
cargo bench -p velorix-runtime --bench s3_incremental --features s3-compat-tests
cargo run -p velorix-cli -- benchmark-validate --result target/velorix-bench/rustfs-s3-nightly.json
```

The `multi_process_ingest_admission` target exercises the checked
`RangeAdmissionIndexV1` coordinator path against RustFS through the S3 API: two
same-host OS processes mark themselves ready after store/coordinator/payload
setup, are released together into overlapping appends with zero artificial
post-release delay, and admit exactly one append with the loser returning
`range_overlap_reserved`; the same target also proves adjacent ranges produce a
valid chained index. It additionally simulates an indexed admission crash window
by deleting the committed batch after admission, reconstructing from the same
S3-compatible authority prefix, writing the digest-bound expiry decision,
proving stale retries return `admission_expired` without a new transition, and
then appending an adjacent range with the chained index preserved. Deployed
writer/operator topology evidence remains a separate ingest row blocker.

The benchmark step can be skipped with `VELORIX_RUSTFS_RUN_BENCHMARK=0`. The
script writes `target/velorix-s3/rustfs-s3-gate-evidence.json` and deletes the
RustFS container/network/volume by default. Set `VELORIX_RUSTFS_CLEANUP=0` to
keep the container for debugging.
When the benchmark step runs, it marks the benchmark JSON with
`backend_evidence_scope=live_or_native`; S3-compatible benchmark gate results
must include an explicit `backend_evidence_scope`; omitted scope remains
backward-compatible for old baselines but is rejected for current S3-compatible
gate evidence.

The manual `RustFS S3-Compatible Gate` workflow runs this same RustFS-backed
gate on a GitHub-hosted runner and uploads the JSON as
`rustfs-s3-compatible-evidence`.
Its benchmark input defaults off so the fast storage/runtime API behavior gate
can be reviewed separately from slower benchmark output. The generated artifact
records the `s3_compatible` and `s3_compatible_integration_harness` readiness
evidence kinds plus gate-local detail
`s3_compatible_ingest_admission_crash_restart` for the indexed admission
crash/restart path. RustFS evidence counts as live S3-compatible evidence for
Velorix readiness when it is produced through the S3 API, with local filesystem
and generic emulator evidence still rejected by readiness validators.

## GCS Emulator Boundary

`fsouza/fake-gcs-server` is a Google Cloud Storage API emulator, not an S3 REST
XML endpoint. Do not wire it into `VELORIX_S3_COMPAT` or count it as
S3-compatible release evidence. If Velorix adds a first-class GCS backend later,
it should get a separate GCS-specific harness and evidence kind instead of
sharing the S3-compatible gate.

## Out Of Scope

The current slice intentionally does not validate Foyer, Kubernetes
coordination, or release-quality S3-compatible baselines. DataFusion 53 uses
`object_store` 0.13 while Velorix storage uses `object_store` 0.12; the runtime
query harness and benchmark keep those clients explicit instead of adding an
adapter between the versions.
