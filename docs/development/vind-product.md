# Vind Product Development

Vind is the local product exercise path for Velorix.

The product flow starts from an empty service:

1. create a relation with a schema
2. ingest rows into that relation
3. create a supported view over one or more relations
4. let ingest commit advance the materialized output
5. query the view or a promoted API route
6. restart and verify recovery from metadata plus object/local storage

Views are derived tables. Users do not ingest into views directly. Querying a
view reads the materialized output maintained by the internal runtime.

Unsupported SQL or unsupported view shapes must fail during admission. The
product must not use fake fallback recomputation to pretend support.

## Runtime Boundary

The standing runtime is internal and jarless. Product operation does not start
external compiler services, build runtime packages at view creation time, load
third-party manager images, or rely on PVC state.

## Local Smoke

Use the REST smoke scripts under `scripts/` for product checks. A useful manual
loop is:

```bash
cargo run -p velorix-api
```

Then call relation create, relation-scoped ingest through
`/v1/relations/{relation_id}/ingest`, view create, query, and promoted API
routes against the local server. Use `/v1/relations/ingest` as the public
ordered batch ingest path when one request carries relation batches; do not use
`/v1/ingest/epoch` as a user-facing product route.

For an existing deployed product slice, run the authenticated REST E2E check:

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product scripts/smoke-vind-rest-api.sh
```

The smoke uses `scripts/attach-vind-product-rest.sh` when it needs to attach to
an existing port-forward. The attach helper can prefer the standing-runtime
writer owner with `VELORIX_API_ATTACH_WRITER_OWNER=auto`; the evidence includes
`GET /v1/standing-runtime/owners` and the protected OpenAPI readback.
The smoke records both `scores-ingest.json` for
`/v1/relations/{relation_id}/ingest` and `scores-batch-ingest.json` for the
public `/v1/relations/ingest` batch path.

`VELORIX_REST_API_SMOKE_ATTACH=auto` is the default. Set
`VELORIX_REST_API_SMOKE_ATTACH=0` when the authenticated API is already
reachable. The smoke writes `rest-api-smoke.json` and is a local product smoke;
it does not by itself prove external object-store durability or public ingress
reachability.

## Product Evidence

The local product path is no-PVC and jarless. Evidence is written under
`target/velorix-product` by default. The important sibling evidence files are:

- `no-pvc-namespace.json`: proves no `PersistentVolumeClaim` objects exist in
  the product namespace.
- `hiqlite-authority-attestation.json`: records the managed or external
  Hiqlite authority shape and source revision.
- `hiqlite-backend-time-assessment.json`: local diagnostic assessment for
  backend-time lease semantics. Set `VELORIX_REQUIRE_HIQLITE_BACKEND_TIME=1`
  when this diagnostic must pass.
- `hiqlite-backend-time-attestation.json`: diagnostic backend-time attestation;
  release/Sigstore provenance is tracked separately.
- `tls-auth-smoke.json`: local TLS/auth smoke evidence.
- `ingress-tls-auth-attestation.json`: public ingress/TLS/auth product
  attestation.
- `ingest-writer-job-log.json`: product ingest-writer append evidence.
- `rest-api-smoke.json`: authenticated REST relation-scoped ingest, public
  relation batch ingest, view, and query smoke.

## Feature-gated authoritative relation ingest

The API has an opt-in authoritative relation-ingest path. Enable it only with a
metadata service that advertises the required relation-ingest capability:

```sh
VELORIX_API_AUTHORITATIVE_RELATION_INGEST=1
VELORIX_RELATION_INGEST_OWNER_ID=<stable-deployment-owner-id>
```

The owner ID must be stable for the logical publisher and must not be a
per-request random value. Startup fails closed when metadata capability checks
or the owner ID requirement fail. In this mode, one relation batch is the only
accepted unit of publication: relation-scoped authority precedes range
reservation, bounded staging write, and metadata publication. The runtime then
applies the published batch. Checkpoint persistence stores validated input
coverage. At restart recovery, the replay frontier and checkpoints drive a fresh
capture and validation of the relation source cut from Meta. Multi-batch
requests fail closed; atomic multi-batch publication is not implemented. With
the feature gate off, the legacy ingestion path remains unchanged.

This is implementation and focused-test coverage, not proof that a current
live Kubernetes deployment exercises the feature. The no-PVC recovery contract
still requires a replacement pod to use durable remote object storage plus
metadata; local storage is only suitable for same-host restart. Do not add PVC
state, Cloud Build, or an alternate source-query path to bypass those bounds.

Bearer-token auth evidence must include
`data_plane_token_rejected_on_admin_route=true`. The product ingress wrappers
read the same product auth env:

```bash
scripts/attest-vind-product-ingress.sh
scripts/complete-vind-product-ingress.sh --validate-only
```

`VELORIX_LOCAL_SCRATCH_DIR` defaults to `target/velorix-product/scratch`; helper
scripts should use that target-backed scratch instead of `/tmp`. The ingress
apply helper, `scripts/apply-vind-product-ingress.sh`, creates Kubernetes
`Ingress` resources only. It does not create DNS records, public certificates,
TLS Secrets, or PVCs.

Refresh deployed image digest evidence with:

```bash
scripts/refresh-vind-product-deployed-images.sh
```

The helper patches only the Velorix deployment template image-digest annotation,
does not change container images, and does not create PVCs. Release evidence
must not infer release product evidence from Pod status alone; the annotation
and observed Pod `imageID` digest must agree.

## Completion Driver

The current completion driver is:

```bash
scripts/complete-vind-product.sh --env-file target/velorix-product/complete-vind-product.env
```

Generate the default env handoff with:

```bash
scripts/write-complete-vind-product-env.sh
```

The report is generated as `product-completion-report.json`. Its
`completion_plan` is gate-oriented product completion status, and
`completion_execution_plan` summarizes the fixed run order. Use:

```bash
scripts/next-vind-product-step.sh --json
scripts/next-vind-product-step.sh --fail-on-incomplete
scripts/next-vind-product-step.sh --doctor
```

The same gate data is exposed as `completion_plan`; each in-scope gate is
classified as `input_required`, `waiting_on_prerequisite`, `runnable`, or
`blocked_without_action`. Input-related plan steps include `input_summary`,
including `secret_placeholders`, `missing_subjects`, `invalid_subjects`, and
redacted release preflight details such as
`hiqlite-backend-time-release-preflight.json`.

`VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3=0` is the current default.
Therefore `object_store_external_authority` and
`object_store_durability_policy` can be `out_of_scope` for local diagnostics.
The scope warning
`object_store_external_authority_out_of_scope_does_not_prove_object_store_durability`
is intentional: the local product slice can complete its diagnostic scope
without claiming external S3/OSS durability. The report accepts `pass` or
`out_of_scope` only for `local_diagnostic_complete=true`; release
`product_complete=true` requires every product gate to be `pass`.
Out-of-scope gates remain in `product_complete_blockers` and
`completion_plan.deferred_steps` so the next-step helper can surface the
operator action still needed for product completion.

If external S3/OSS is later required, use `scripts/run-vind-product-external-s3.sh`
and provide an S3_OR_OSS_ENDPOINT value as `AWS_ENDPOINT_URL`. That endpoint
must be the service endpoint only, without bucket, prefix, query, or fragment.
The prefix must be stable through validate-only and execution. Raw private IP
endpoints are rejected by default. Managed credential Secret mode and existing
Kubernetes Secret mode are separate paths; validate-only checks input shape
while real execution checks Secret existence and keys. The helpers do not print
credential values.

Object-store durability review is also out of scope until the external
authority is proven. Review flags alone do not make the durability step ready,
and an already attached `object_store.durability_policy_attestation` is not
trusted only because it says `validated=true`; the report rechecks the summary
against the current `authority_store_id`, bucket, and S3 prefix.

## Evidence

First-completion evidence should prove:

- relation creation from empty state
- ingest for multiple schemas
- view creation with clear admission errors for unsupported SQL
- automatic materialized output updates after ingest
- restart recovery from metadata and storage checkpoints
- two-relation join view consistency at committed epochs
