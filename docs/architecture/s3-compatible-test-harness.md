# S3-Compatible Test Harness

Status: Accepted
Applies to: env-gated live object-store evidence for Velorix 1.0 readiness.

The harness validates S3-compatible object-store behavior only when explicitly
enabled. It does not create a new authority model: object storage remains the
durable authority, Velorix-owned manifests remain the production contract, and
the harness only proves backend assumptions that local filesystem tests cannot
prove.

## Environment Contract

The live test target is compiled only when the explicit Cargo feature is
enabled:

```bash
cargo test -p velorix-storage --test s3_compat --features s3-compat-tests
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
- cleanup deletes the written key on a best-effort basis

These are capability checks for production assumptions, not replacements for
Velorix checkpoint, ingest, output, or catalog manifests.

## Skip Behavior

Without `--features s3-compat-tests`, default storage test builds do not compile
the live S3 harness or enable the `object_store/aws` HTTP/TLS stack. Without
`VELORIX_S3_COMPAT=1`, the explicitly enabled test returns early and prints a
skip message. This keeps normal local and PR runs deterministic and avoids
accidental writes to shared MinIO or S3 buckets.

## Out Of Scope

The current slice intentionally does not validate DataFusion S3 scans, SlateDB,
Foyer, benchmark artifacts, Kubernetes coordination, or end-to-end recovery on
S3-compatible storage. DataFusion 53 uses `object_store` 0.13 while Velorix
storage uses `object_store` 0.12; runtime S3 query coverage should be added only
when the dependency path stays small and does not require an adapter between
those versions.
