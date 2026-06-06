# Running the Velorix Product Slice on vind

Use this when you want a real REST-callable Velorix deployment on local
vind/vCluster, not a test harness.

```bash
scripts/run-vind-product.sh
```

The script builds `Dockerfile.api` and `Dockerfile.meta`, creates or reuses a
vCluster, deploys `velorix-meta` plus `velorix-api`, and opens a local
port-forward. By default it also deploys RustFS as the local S3-compatible
authority and creates the bucket. Set `VELORIX_OBJECT_STORE_MODE=external-s3`
to use an already-provisioned S3-compatible object store instead; that mode
skips the RustFS Deployment and bucket creation. By default the API is reachable
at:

```text
http://127.0.0.1:8080
```

The script also runs a small REST smoke by default: it checks `/readyz`,
asserts legacy recovered SQL views and the generic SQL query endpoint are
disabled, registers the default scores relation, verifies a no-artifact view is
accepted as a durable Feldera compile/deploy pending record while
`POST /v1/query` is rejected, creates and reads a durable query policy, verifies
views that reference a missing query policy fail closed, creates the default
generated view linked to that query policy, ingests three rows, queries the view
by id, queries the promoted API route, checks the generated OpenAPI path, runs
the deployed
`velorix-ingest-writer` Job with a real `.vlxingest` payload, restarts
`velorix-api`, and queries again to prove the writer-appended batch is recovered
into the materialized view while the metadata service remains alive. For durable metadata backends (`oss` or
`hiqlite`), the smoke also restarts `velorix-meta`, reruns the metadata smoke
Job, restarts `velorix-api`, and queries again to prove metadata-backed
recovery across both process restarts.
The JSON responses are written under `target/velorix-product/`. Set
`VELORIX_VIND_PRODUCT_SMOKE=0` to only deploy the services.
Readiness validation treats those response files as part of the evidence, not
just as debug output: `product-evidence.json` must be accompanied in the same
directory by the referenced OpenAPI, query-policy rejection, external S3
validation, and ingest-writer lifecycle job-log artifacts.

After a successful smoke, the script also writes
`target/velorix-product/product-evidence.json`. The default
`VELORIX_PRODUCT_EVIDENCE_LEVEL` is `local-vind-only`: this is evidence for a
real REST-callable local vind/vCluster slice, not product-complete evidence.
`VELORIX_PRODUCT_EVIDENCE_LEVEL=product-complete` runs the same deployment and
smoke path, writes `product-evidence.json`, and then fails closed with exit
code `65` unless that evidence proves every product-complete gate.
The evidence file includes the selected object-store authority and a
`standing_runtime_fencing` block copied from `/readyz` so a release reviewer can
see exactly why multi-replica fencing is `pass`, `not_run`,
`blocked_by_capability`, or `disabled`.
The local smoke verifies bearer-token behavior through the port-forwarded
service. By default it also generates a short-lived self-signed certificate,
mounts it into `velorix-api`, opens an optional TLS listener on `8443`, and
verifies the same bearer/admin auth boundaries over HTTPS with `curl --cacert`.
That evidence is recorded as local vind TLS/auth smoke, not as public ingress
evidence. Product ingress/TLS/auth evidence is still separate: set
`VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE` to record an externally verified
ingress boundary.
By default the product smoke also generates
`target/velorix-product/ingest-writer-lifecycle-attestation.json` from actual
Kubernetes `velorix-ingest-writer` Jobs when the initial Pod-internal append
succeeds and no PVC is present. That generated attestation proves a deployed
writer Pod append, a second Pod rejecting an overlapping range, adjacent
appends from later Pods, restart reconstruction through a fresh writer Pod,
lease-loss-during-reservation rejection plus orphan expiry, and handoff to a
different writer identity. Set
`VELORIX_INGEST_WRITER_LIFECYCLE_AUTO=0` to disable generation, or provide
`VELORIX_INGEST_WRITER_LIFECYCLE_ATTESTATION_FILE` to use externally reviewed
lifecycle evidence instead.

## Kubernetes Target Modes

The default target mode is the vCluster Docker driver:

```bash
VELORIX_VIND_CLUSTER_DRIVER=docker-vcluster scripts/run-vind-product.sh
```

When the local Docker runtime cannot run vCluster standalone, you can deploy
the same product manifests into an already-running local Kubernetes context:

```bash
VELORIX_VIND_CLUSTER_DRIVER=existing-context \
VELORIX_K8S_CONTEXT=k3d-certd-k3d \
VELORIX_IMAGE_LOAD_MODE=auto \
scripts/run-vind-product.sh
```

In `existing-context` mode, the runner refuses non-local Kubernetes API servers
unless `VELORIX_EXISTING_CONTEXT_ALLOW_REMOTE=1` is set. For `k3d-*` contexts,
`VELORIX_IMAGE_LOAD_MODE=auto` imports locally built role images directly into
the k3d server and agent containers with `ctr -n k8s.io images import`, so it
does not depend on the `k3d` CLI. Set `VELORIX_IMAGE_LOAD_MODE=none` only when
the selected cluster can already pull the configured images.

No PVCs are introduced by this mode. The existing cluster must still pass the
same no-PVC namespace validation, image security context checks, REST smoke, and
product evidence gates as the vCluster mode.

## Object Store Modes

The default object store mode is local RustFS:

```bash
scripts/run-vind-product.sh
```

This mode is useful for quick local REST testing. It generates non-default
per-run S3 credentials, deploys RustFS inside the vCluster, and stores objects
under `s3://rustfs/${VELORIX_S3_BUCKET}/${VELORIX_S3_PREFIX}`. It intentionally
uses `emptyDir`, not PVC, so deleting the RustFS pod loses object-store data.

Use external S3-compatible storage when you want object-store durability without
PVC:

```bash
VELORIX_OBJECT_STORE_MODE=external-s3 \
AWS_ENDPOINT_URL=https://s3.example.internal \
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
AWS_SESSION_TOKEN=... \
AWS_REGION=us-east-1 \
VELORIX_S3_BUCKET=velorix-product \
VELORIX_S3_PREFIX=product/manual-run \
VELORIX_S3_FORCE_PATH_STYLE=1 \
scripts/run-vind-product.sh
```

For real nonlocal S3/OSS-compatible storage, prefer the fail-closed wrapper:

```bash
AWS_ENDPOINT_URL=https://oss.example.com \
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
AWS_SESSION_TOKEN=... \
AWS_REGION=us-east-1 \
VELORIX_S3_BUCKET=velorix-product \
VELORIX_S3_PREFIX=product/manual-run \
VELORIX_S3_FORCE_PATH_STYLE=1 \
scripts/run-vind-product-external-s3.sh
```

When you are following the product-completion handoff, use the generated env
file instead of inline credentials:

```bash
scripts/run-vind-product-external-s3.sh \
  --env-file target/velorix-product/complete-vind-product.env \
  --output-dir target/velorix-product \
  --validate-only
scripts/run-vind-product-external-s3.sh \
  --env-file target/velorix-product/complete-vind-product.env \
  --output-dir target/velorix-product
```

`--validate-only` sets `VELORIX_EXTERNAL_S3_RUN_PRODUCT=0`, validates the
nonlocal endpoint/bucket/prefix/authority inputs, requires a stable explicit
`VELORIX_S3_PREFIX`, and writes
`external-s3-product-input.json` without deploying the product slice. The
wrapper writes `external-s3-product-input.json`, rejects
`VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=1`, rejects localhost-style
and loopback/link-local/private IP endpoints by default, derives
`VELORIX_AUTHORITY_STORE_ID=s3://external/${VELORIX_S3_BUCKET}/${VELORIX_S3_PREFIX}`,
and delegates to `scripts/run-vind-product.sh` with
`VELORIX_OBJECT_STORE_MODE=external-s3`. Set
`VELORIX_EXTERNAL_S3_RUN_PRODUCT=0` to validate the inputs and write the input
evidence without deploying. Local Docker RustFS remains intentionally separate:
use `scripts/run-vind-product-external-rustfs.sh` for that path.

For a local manual product run without an in-cluster RustFS `emptyDir`, use the
external-RustFS wrapper:

```bash
scripts/run-vind-product-external-rustfs.sh
```

That wrapper starts RustFS as a local Docker container with a Docker volume,
creates the configured bucket, writes
`target/velorix-product/external-rustfs.env`, and then runs
`scripts/run-vind-product.sh` with `VELORIX_OBJECT_STORE_MODE=external-s3`.
The product slice still validates the bucket/prefix from inside the vCluster
through the `external-s3-validate` Job. This removes the in-cluster RustFS
`emptyDir` object-store blocker for local manual testing, but it does not
replace public ingress/TLS/auth evidence, Hiqlite backend-time proof, or an
operator-reviewed object-store durability policy. Set
`VELORIX_EXTERNAL_RUSTFS_POD_ENDPOINT` when
`http://host.docker.internal:${VELORIX_RUSTFS_PORT:-9000}` is not reachable
from the vCluster pods. In `VELORIX_VIND_CLUSTER_DRIVER=existing-context` runs
against a `k3d-*` context, the wrapper resolves the k3d node's host gateway and
uses that IP automatically when `host.docker.internal` would not resolve inside
pods.
Runs launched through this wrapper set
`VELORIX_OBJECT_STORE_LOCAL_DEVELOPMENT_AUTHORITY=1`, so the generated product
evidence is explicitly marked as a local development authority. Release
validation rejects product-complete durability attestation for that authority
class even if a matching attestation file is supplied.

External mode assumes the endpoint, bucket, credentials, and network path from
the vCluster pods already work. By default it proves the cluster-side
object-store path with a Kubernetes Job that runs `head-bucket` plus a scoped
`put-object`, `get-object`, exact-key `list-objects-v2 --max-keys 1`, and
`delete-object` probe under the configured authority prefix. Set
`VELORIX_EXTERNAL_S3_VALIDATE=0` only when you intentionally want to skip that
validation; doing so leaves an explicit product-complete blocker in the
evidence. The script does not create the bucket in external mode and does not
validate provider-side lifecycle, versioning, encryption, or retention policy.
`VELORIX_S3_FORCE_PATH_STYLE` defaults to `1` for OSS-compatible endpoints whose
bucket names are not exposed through virtual-hosted DNS; set it to `0` only for
providers where virtual-hosted addressing is required and validated.
In `product-evidence.json`, check
`object_store.mode`, `object_store.durability`, `object_store.endpoint`,
`object_store.authority_store_id`,
`object_store.external_s3_bucket_validated`,
`object_store.external_s3_prefix_validated`, and
`object_store.external_s3_validation_key` to see which authority was used and
whether that bucket/prefix was reachable from inside the cluster. External mode
removes the RustFS `emptyDir` product-complete blocker from the generated
evidence, but `product-complete` still fails closed until the remaining
ingress/TLS/auth and backend-authoritative bounded failover gates are
implemented. Logical-fencing multi-replica smoke is already executable and
recorded, but it is not the same as the required production wall-clock failover
contract.

To clear the object-store durability policy gate for an external S3-compatible
authority, run the durability completion helper after the product slice is
already backed by a nonlocal external S3/OSS authority:

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
scripts/complete-vind-object-store-durability.sh \
  --versioning-or-object-lock-enabled \
  --server-side-encryption-enabled \
  --backup-or-replication-configured \
  --lifecycle-delete-policy-reviewed \
  --destructive-delete-protection-reviewed \
  --cost-controls-reviewed
```

When following the generated product-completion env handoff, validate the
current external authority and operator review flags before generating or
attaching product-complete durability evidence:

```bash
scripts/complete-vind-object-store-durability.sh \
  --env-file target/velorix-product/complete-vind-product.env \
  --output-dir target/velorix-product \
  --validate-only
scripts/complete-vind-object-store-durability.sh \
  --env-file target/velorix-product/complete-vind-product.env \
  --output-dir target/velorix-product
```

`--validate-only` writes `object-store-durability-input.json` and checks that
`product-evidence.json` already proves a nonlocal external S3/OSS authority and
that every durability/cost/delete-protection review flag is set, without
running assessment probes, generating attestation evidence, attaching evidence,
editing product evidence, or creating PVCs.

`scripts/complete-vind-object-store-durability.sh` runs
`scripts/assess-object-store-durability-policy.sh`,
`scripts/attest-object-store-durability-policy.sh`, and
`scripts/attach-vind-object-store-durability.sh` in order. The attach step
copies the attestation beside `product-evidence.json` as
`object-store-durability-attestation.json`, records it under
`object_store.durability_policy_attestation`, removes the exact durability
blocker from `product_complete_blockers`, and refreshes
`product-completion-report.json`. It refuses local development authorities and
requires the current product evidence to have
`object_store.external_s3_bucket_validated=true` and
`object_store.external_s3_prefix_validated=true`. The attestation must match the
current `authority_store_id`, bucket, and prefix, and it must prove that
versioning or object lock, server-side encryption, backup or replication,
lifecycle deletion, destructive-delete protection, and cost controls were
reviewed for the configured authority. Example:

```json
{
  "schema_version": 1,
  "evidence_kind": "velorix_object_store_durability_policy_attestation",
  "provider_kind": "s3-compatible",
  "authority_store_id": "s3://external/velorix-product/product/20260531T000000Z",
  "bucket": "velorix-product",
  "s3_prefix": "product/20260531T000000Z",
  "versioning_or_object_lock_enabled": true,
  "server_side_encryption_enabled": true,
  "backup_or_replication_configured": true,
  "lifecycle_delete_policy_reviewed": true,
  "destructive_delete_protection_reviewed": true,
  "cost_controls_reviewed": true,
  "attested_at": "2026-05-31T00:00:00Z",
  "attester": "operator"
}
```

Before attaching that product-complete evidence, generate a fail-closed
assessment from the current product evidence:

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
scripts/assess-object-store-durability-policy.sh
```

When that assessment and the backing provider review are both satisfactory,
generate the attestation with explicit operator checks:

```bash
scripts/attest-object-store-durability-policy.sh \
  --product-evidence target/velorix-product/product-evidence.json \
  --assessment target/velorix-product/object-store-durability-assessment.json \
  --output target/velorix-product/object-store-durability-attestation.json \
  --versioning-or-object-lock-enabled \
  --server-side-encryption-enabled \
  --backup-or-replication-configured \
  --lifecycle-delete-policy-reviewed \
  --destructive-delete-protection-reviewed \
  --cost-controls-reviewed
```

When the attestation must be prepared before the product slice is rerun, bind
it directly to the planned authority instead of reading `product-evidence.json`:

```bash
scripts/attest-object-store-durability-policy.sh \
  --authority-store-id s3://external/velorix-product/product/20260531T000000Z \
  --bucket velorix-product \
  --s3-prefix product/20260531T000000Z \
  --output target/velorix-product/object-store-durability-attestation.json \
  --versioning-or-object-lock-enabled \
  --server-side-encryption-enabled \
  --backup-or-replication-configured \
  --lifecycle-delete-policy-reviewed \
  --destructive-delete-protection-reviewed \
  --cost-controls-reviewed
```

`scripts/report-vind-product-completion.sh` surfaces this staged file as
`gates[].evidence.staged_attestation` and, when present,
`completion_plan.steps[].input_summary.staged_attestation`. Staged durability
evidence is only operator-review readiness: it keeps
`object_store_durability_policy` blocked by `object_store_external_authority`,
sets `creates_product_complete_evidence=false`, and never makes
`product_complete=true` until the product slice itself proves the same nonlocal
external S3/OSS authority and the attestation is attached.

The helper refuses local development authorities, binds the attestation to the
current `object_store.authority_store_id`, `bucket`, and `s3_prefix`, and
requires every durability/cost/delete-protection review flag to be supplied
explicitly. If the attestation was generated separately, attach it without
regenerating:

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
VELORIX_OBJECT_STORE_DURABILITY_ATTESTATION_FILE=target/velorix-product/object-store-durability-attestation.json \
scripts/attach-vind-object-store-durability.sh
```

The first-E2E wrapper forwards the same file when
`VELORIX_FIRST_E2E_PRODUCT_OBJECT_STORE_DURABILITY_ATTESTATION_FILE` is set, but
rejects it for the internally generated local RustFS authority because that
authority is deliberately marked as local development evidence.

The assessment is written as `object-store-durability-assessment.json` and does
not create product-complete evidence. This helper intentionally does not create
product-complete evidence. It records provider API observations such
as bucket versioning, bucket encryption, and lifecycle configuration, then lists
the remaining fields that a real operator must review before writing
`object-store-durability-attestation.json`. For local Docker-volume RustFS,
this assessment is expected to remain fail-closed unless the backing authority
actually satisfies encryption, backup or replication, destructive-delete
protection, lifecycle deletion policy, and cost-control review requirements.
Its `authority_class=local_single_node_docker_volume` is a compatibility smoke
scope, not a production durability boundary.
In other words, this assessment does not create product-complete evidence.

## Image Layout

Velorix keeps role-specific images as the product default:

- `Dockerfile.api`: API server only, exposed on `8080`.
- `Dockerfile.meta`: metadata gRPC service only, exposed on `9090`.
- `Dockerfile.ingest-writer`: bounded ingest-writer job wrapper around the
  lease-guarded append path. The raw `append` command is still available only
  when explicitly passed as CLI arguments with `VELORIX_ALLOW_DIAGNOSTIC_CLI=1`
  for diagnostics.

Velorix now pins `crates/velorix-meta` to the `mrchypark/hiqlite` fork commit
that carries the required Raft-serialized timestamp API. Product runs must
continue to verify that the resolved package exposes
`txn_with_raft_serialized_timestamp` and `Param::raft_serialized_unix_ms()`
before treating it as the authority-time source. After `sebadob/hiqlite`
includes the same API in a release, Velorix should switch to that pinned release
dependency.

`Dockerfile.all-in-one` is also available as an explicit convenience image for
local development, demos, and smoke testing:

```bash
DOCKER_BUILDKIT=1 docker build \
  --build-context velorix-hiqlite-source=../hiqlite \
  -f Dockerfile.all-in-one \
  -t velorix:all-in-one \
  .
docker run --rm velorix:all-in-one --help
docker run --rm -e VELORIX_IMAGE_MODE=api velorix:all-in-one
docker run --rm velorix:all-in-one meta
docker run --rm velorix:all-in-one ingest-writer
```

The all-in-one dispatcher intentionally supports only `api`, `meta`, and
`ingest-writer`. It does not expose a raw `cli` mode or include the broad
`velorix-cli` toolbox because product deployments should preserve role
separation and avoid shipping broader mutation tooling into API or metadata
workloads. The vind product script therefore keeps using the mode-specific
images unless you explicitly build and run the all-in-one image yourself.
All Velorix-owned runtime images run as UID/GID `65532`. The product script
adds restricted security contexts to the API, meta, and ingest-writer
containers: no privilege escalation, read-only root filesystem, `RuntimeDefault`
seccomp, and all Linux capabilities dropped. Third-party helper images are not
force-patched with those settings because their filesystem/user assumptions are
owned by their upstream images.
The Dockerfiles use BuildKit cache mounts for Cargo registry, git, and target
directories, then copy only the stripped release binaries into stable builder
paths before the runtime stage. This keeps the role-specific image layout while
reducing repeated local build pressure during product smoke runs.
If images already exist locally, set `VELORIX_BUILD_API_IMAGE=0`,
`VELORIX_BUILD_META_IMAGE=0`, `VELORIX_BUILD_INGEST_WRITER_IMAGE=0`, and
`VELORIX_LOAD_EXISTING_IMAGES=1`; the product script will inspect the local
Docker images and load them into the vCluster instead of rebuilding them. Without
`VELORIX_LOAD_EXISTING_IMAGES=1`, `BUILD_*_IMAGE=0` assumes the images are
already present inside the vCluster.

By default the local script generates distinct random API and admin bearer
tokens, stores them in the `velorix-api-auth` and `velorix-admin-auth`
Kubernetes Secrets, injects them into `velorix-api`, sends the API token on
data-plane smoke calls, and writes a local `0600` helper file at
`target/velorix-product/api-auth.env`. This default exercises the REST auth
boundary instead of silently running the product slice in unauthenticated
development mode. Set `VELORIX_API_BEARER_TOKEN` and
`VELORIX_ADMIN_BEARER_TOKEN` to supply your own tokens. They must be distinct.
Set `VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1` only when you explicitly want
local dev mode; it is mutually exclusive with both bearer tokens.

Basic calls:

```bash
source target/velorix-product/api-auth.env

curl "$VELORIX_API_URL/healthz"

curl -X POST "$VELORIX_API_URL/v1/relations/scores-default" \
  -H "$VELORIX_API_AUTH_HEADER"

curl -X POST "$VELORIX_API_URL/v1/views" \
  -H "$VELORIX_API_AUTH_HEADER" \
  -H 'content-type: application/json' \
  -d '{"view_id":"positive_scores_by_user","urlPath":"/scores/positive","input_relation_id":"scores","input_relation_version":"2026-05-24.v1","sql":"select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id","response_formats":["json"]}'

curl -X POST "$VELORIX_API_URL/v1/ingest" \
  -H "$VELORIX_API_AUTH_HEADER" \
  -H 'content-type: application/json' \
  -d '{"relation_id":"scores","relation_version":"2026-05-24.v1","stream_id":"scores","partition_id":0,"start_offset_inclusive":0,"rows":[{"user_id":"u1","score":5,"delta":1},{"user_id":"u1","score":7,"delta":1},{"user_id":"u2","score":-1,"delta":1}]}'

curl "$VELORIX_API_URL/v1/views/positive_scores_by_user/query" \
  -H "$VELORIX_API_AUTH_HEADER"

curl "$VELORIX_API_URL/v1/views/positive_scores_by_user/query?max_rows=2&epoch=3" \
  -H "$VELORIX_API_AUTH_HEADER"

curl "$VELORIX_API_URL/v1/views/positive_scores_by_user/query?max_rows=2&page_token=u2&epoch=3" \
  -H "$VELORIX_API_AUTH_HEADER"
```

If the product slice is still deployed but the local port-forward has exited,
reattach without creating a new vCluster or rebuilding images:

```bash
scripts/attach-vind-product-rest.sh
```

The attach script reads `target/velorix-product/product-evidence.json`, checks
the recorded Kubernetes context, namespace, `service/velorix-api`, and available
API replicas, starts a fresh `kubectl port-forward` to either the service or a
selected owner pod, validates `/healthz` and authenticated `/readyz`, and
refreshes `target/velorix-product/api-auth.env`.
For multi-replica standing-runtime runs it uses the admin
`GET /v1/standing-runtime/owners` route on each API pod to prefer the current
writer-owner pod. That keeps the local `VELORIX_API_URL` useful for write-path
REST checks even though read replicas are intentionally fenced from ingest with
`409`. Set `VELORIX_API_ATTACH_WRITER_OWNER=0` to attach to
`service/velorix-api` without owner selection, or `=1` to fail if no owner pod
can be identified. Before selecting a pod it filters out terminating API pods
and calls admin `POST /v1/standing-runtime/owners` on each Ready pod to advance
the metadata-backed lease clock and let a current pod acquire ownership after a
rollout. Set `VELORIX_API_ATTACH_BACKGROUND=1` when another wrapper needs the
validated port-forward to stay alive while the attach script returns.
In background mode the attach script uses a local `tmux` session when available;
this keeps `kubectl port-forward` alive after non-interactive shells exit. The
session name and process id are recorded beside the attach evidence.
`scripts/run-vind-product.sh` uses that mode by default at the end of a
multi-replica fenced run when `VELORIX_API_HOLD_PORT_FORWARD=1`, and requires a
writer-owner pod instead of silently falling back to service-level routing; set
`VELORIX_API_FINAL_OWNER_AWARE_ATTACH=0` to keep the original service-level
port-forward. It also writes
`target/velorix-product/rest-attach-evidence.json`. On success that evidence
links the readyz response, port-forward log, deployment JSON, pod JSON, selected
port-forward target, and owner-selection status. It exits `75` and writes
blocker evidence when the recorded deployment exists but has no available API
pod, which means the product slice must be rerun or the local vCluster/Docker
pressure must be cleared first.

To run a repeatable REST E2E check against an already deployed product slice,
without rebuilding images or creating a new vCluster:

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
scripts/smoke-vind-rest-api.sh
```

For the external RustFS product run, point the smoke at that evidence
directory:

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product-external-rustfs-corrected \
scripts/smoke-vind-rest-api.sh
```

The smoke reads `api-auth.env`, recreates the local port-forward with
`scripts/attach-vind-product-rest.sh` only when `/healthz` is not reachable,
then exercises the live REST product surface: `/readyz`, default `scores`
relation admission, the durable `interactive` query policy, the
`positive_scores_by_user` standing-runtime view, generic `/v1/query` rejection,
admin standing-runtime owner acquisition/reporting when an admin token is
available, REST ingest into `scores`, `/v1/views/positive_scores_by_user/query`,
the promoted `GET /v1/api/scores/positive` route, `GET /v1/views`, and
`GET /v1/openapi.json`. It writes `rest-api-smoke.json` plus response bodies
under `target/.../rest-api-smoke/`. This is local product operation evidence
only; `trusted_for_product_complete` stays `false`.
`scripts/run-vind-product.sh` also runs this REST smoke automatically at the end
of a bearer-token product smoke run and prints `rest_api_smoke_status` plus
`rest_api_smoke_evidence`. Set `VELORIX_VIND_REST_API_SMOKE=0` to skip it, or
`=1` to require it explicitly; the default `auto` runs it only for the normal
authenticated product path.

Every product run also writes a product-completion report by default:

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
scripts/report-vind-product-completion.sh
```

When all external inputs are available, use the top-level completion driver
instead of running the remaining helpers manually:

```bash
scripts/write-complete-vind-product-env.sh \
  --product-evidence target/velorix-product/product-evidence.json

# Edit target/velorix-product/complete-vind-product.env and replace every
# REPLACE_WITH_*, PUBLIC_HOST.*, INGRESS_CONTROLLER, TLS_SECRET_NAME, and
# S3_OR_OSS_ENDPOINT value.

VELORIX_COMPLETE_PRODUCT_DRY_RUN=1 \
  scripts/complete-vind-product.sh \
    --env-file target/velorix-product/complete-vind-product.env
scripts/complete-vind-product.sh \
  --env-file target/velorix-product/complete-vind-product.env
```

The env helper writes `complete-vind-product.env` and
`complete-vind-product-env.json` under the product evidence directory. It also
regenerates and embeds `hiqlite-backend-time-release.env`, so one env-file
contains the public ingress/TLS/auth and Hiqlite release/Sigstore inputs for the
current completion scope. Actual external S3/OSS, its object-store durability
attestation, and public/enterprise ingress are excluded by default, so the
generated file sets
`VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3=0`,
`VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS=0`,
`VELORIX_PRODUCT_COMPLETE_REQUIRE_HIQLITE_RELEASE=0`,
`VELORIX_COMPLETE_PRODUCT_EXTERNAL_S3=0`, and
`VELORIX_COMPLETE_PRODUCT_DURABILITY=0`, and
`VELORIX_COMPLETE_PRODUCT_INGRESS=0`. Regenerate with
`VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3=1` to opt back into S3/OSS and
durability-review placeholders, or
`VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS=1` to opt back into public
ingress/TLS/auth placeholders, or
`VELORIX_PRODUCT_COMPLETE_REQUIRE_HIQLITE_RELEASE=1` to opt back into trusted
Hiqlite release/Sigstore provenance placeholders. The generated file is a
template, not evidence: by default it has no remaining placeholders and creates
no product-complete evidence or PVCs. The JSON report groups remaining inputs
under `placeholder_groups` only for opt-in scopes (`release_identity` and
`sigstore_provenance` for trusted Hiqlite release; `public_ingress_tls_auth`,
`external_s3`, and `object_store_durability_review` for their respective
scopes) and separately lists `secret_placeholders` so bundles and optional
credentials can be handled without logging them. The template and report also include
`scope_warnings`; in the default scope, `product_complete=true` proves local or
internal REST TLS/auth, Hiqlite replicated backend-time boundary, and the other
in-scope gates, but it does not prove public DNS, public TLS issuance,
ingress-controller routing, external-client reachability, object-store
durability, or Sigstore-backed release provenance.

When a completion helper reads `--env-file`, the file acts as defaults. Explicit
values supplied by the caller environment, and CLI options such as
`--output-dir` or `--validate-only`, take precedence over values exported by the
env file. This lets an operator keep the generated handoff template unchanged
while supplying real secrets or toggles in the process environment.
If the S3/OSS scope is explicitly enabled and the cluster already has a
Kubernetes Secret for S3 credentials, set
`VELORIX_S3_CREDENTIALS_SECRET_MANAGED=0`, set
`VELORIX_S3_CREDENTIALS_SECRET_NAME`, and leave
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` empty in
the effective environment; otherwise preflight treats the credential source as
ambiguous and fails closed.

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
AWS_ENDPOINT_URL=https://S3_OR_OSS_ENDPOINT \
AWS_ACCESS_KEY_ID=REPLACE_WITH_ACCESS_KEY \
AWS_SECRET_ACCESS_KEY=REPLACE_WITH_SECRET_KEY \
AWS_REGION=REPLACE_WITH_REGION \
VELORIX_S3_BUCKET=REPLACE_WITH_BUCKET \
VELORIX_S3_PREFIX=product/manual-run \
VELORIX_S3_FORCE_PATH_STYLE=1 \
VELORIX_PRODUCT_INGRESS_HOST=velorix.example.com \
VELORIX_PRODUCT_INGRESS_CLASS=nginx \
VELORIX_PRODUCT_INGRESS_TLS_SECRET=velorix-api-public-tls \
VELORIX_INGRESS_ENDPOINT_URL=https://velorix.example.com \
VELORIX_INGRESS_CONTROLLER=nginx \
scripts/complete-vind-product.sh \
  --versioning-or-object-lock-enabled \
  --server-side-encryption-enabled \
  --backup-or-replication-configured \
  --lifecycle-delete-policy-reviewed \
  --destructive-delete-protection-reviewed \
  --cost-controls-reviewed
```

`scripts/complete-vind-product.sh` first refreshes local evidence for an
existing product slice by running
`scripts/refresh-vind-product-deployed-images.sh` and
`scripts/smoke-vind-rest-api.sh` unless
`VELORIX_COMPLETE_PRODUCT_LOCAL_EVIDENCE=0` is set. It then runs
`scripts/check-hiqlite-backend-time-release-inputs.sh`, and the final
`scripts/report-vind-product-completion.sh` in dependency order. By default it
does not run `scripts/run-vind-product-external-s3.sh`,
`scripts/complete-vind-object-store-durability.sh`, or
`scripts/complete-vind-product-ingress.sh`; set
`VELORIX_PRODUCT_COMPLETE_REQUIRE_EXTERNAL_S3=1` to include those steps in the
completion scope, and set `VELORIX_PRODUCT_COMPLETE_REQUIRE_PUBLIC_INGRESS=1`
to include public/enterprise ingress in the completion scope. In `auto` mode it
skips steps whose required external inputs are absent; set
`VELORIX_COMPLETE_PRODUCT_LOCAL_EVIDENCE=1`,
`VELORIX_COMPLETE_PRODUCT_EXTERNAL_S3=1`,
`VELORIX_COMPLETE_PRODUCT_INGRESS=1`,
`VELORIX_COMPLETE_PRODUCT_DURABILITY=1`, or
`VELORIX_COMPLETE_PRODUCT_HIQLITE_BACKEND_TIME=1` to make a step mandatory.
Set `VELORIX_COMPLETE_PRODUCT_DRY_RUN=1` to write
`complete-vind-product-plan.json` without touching Kubernetes, object storage,
or product evidence; when reporting is enabled, dry-run also refreshes
`product-completion-report.json` so `completion_plan` reflects the latest
env-file preflight. Both dry-run and real execution also write
`complete-vind-product-input-preflight.json`, a redacted target-backed report
that validates the external S3/OSS, public ingress, and durability-review
inputs before those helpers run when those scopes are enabled. In `auto` mode missing external inputs are
reported as incomplete and skipped, and helper execution is gated by the
preflight step `ready` value rather than nonempty placeholder strings; in `=1`
mandatory mode the same missing or invalid inputs fail before any external
product-complete helper runs. Local evidence refresh and the final report can
still run first because they do not require S3/OSS credentials, public ingress,
durability review, PVCs, generic `/v1/query`, or Feldera provenance. The plan is
also preflight-backed: it records
`preflight_status`, `forced_blocker_count`, the fixed `run_order`, and each
step's state (`ready_to_run`, `blocked`, `input_incomplete`,
`waiting_on_prerequisite`, `already_validated`, or `disabled`) plus redacted
missing/invalid subjects. Real execution writes that plan immediately after
preflight; mandatory preflight failure still leaves the plan behind, refreshes
local evidence when enabled, refreshes the report when enabled, and exits
nonzero before external helpers run. If the existing
`product-evidence.json` already proves a gate, the input preflight records that
step as `already_validated` instead of requiring the original environment
variables again. The driver creates no PVCs and exits nonzero until
`product-completion-report.json` proves `product_complete=true`.
For Hiqlite backend-time, the driver always writes a local diagnostic
`hiqlite-backend-time-attestation.json` when product evidence exists; this
keeps the current backend-time claim reviewable while release/product-complete
trust still requires the separate Sigstore-backed release preflight and
release-scoped failover evidence.
The top-level input preflight intentionally mirrors the ingress helper's bearer
token-source requirement before marking ingress ready: when
`VELORIX_PRODUCT_INGRESS_ATTEST=1`, it requires both data-plane and admin bearer
token sources from `VELORIX_API_BEARER_TOKEN`/`VELORIX_ADMIN_BEARER_TOKEN`,
`Authorization: Bearer ...` header env vars, or the product `api-auth.env`.
The report and doctor expose only booleans under `auth_token_source`, never the
token values.
It also mirrors the ingress attestation endpoint shape checks: the product
ingress host must be a public DNS hostname, not `localhost`, and
`VELORIX_INGRESS_ENDPOINT_URL` must be an HTTPS URL without query parameters or
fragment. A path component is allowed because the lower TLS/auth attestation
normalizes it as the public base path before appending product API routes.
It also mirrors the durability helper's authority prerequisite: durability
review flags alone do not make the durability step ready until the same product
evidence proves a validated nonlocal external S3/OSS authority. The redacted
preflight/report/doctor path exposes `authority_ready`, the authority fields,
and `object_store_external_authority` invalid details without creating or
attaching durability evidence.
An already attached `object_store.durability_policy_attestation` is not trusted
only because it says `validated=true`; the preflight/report path rechecks the
summary against the current `authority_store_id`, `bucket`, `s3_prefix`,
`schema_version`, `evidence_kind`, and all six durability review booleans before
treating the durability gate as already satisfied.

The report reads `product-evidence.json` and optional `rest-api-smoke.json`,
then writes `product-completion-report.json`. It also reads the optional
`complete-vind-product-plan.json` and embeds a redacted
`completion_execution_plan` summary so callers can see the driver run order,
preflight status, forced blocker count, per-step state, `will_run`, waiting
prerequisites, and missing/invalid subjects without opening a second file. This
execution summary is separate from `completion_plan`: `completion_plan` is
gate-oriented product completion status, while `completion_execution_plan` is
the concrete `scripts/complete-vind-product.sh` run plan. It is diagnostic
handoff data and does not create product-complete evidence.

To ask the report for the next actionable command, run:

```bash
scripts/next-vind-product-step.sh
scripts/next-vind-product-step.sh --json
scripts/next-vind-product-step.sh --doctor
scripts/next-vind-product-step.sh --fail-on-incomplete
```

`scripts/next-vind-product-step.sh` is read-only. It selects the earliest
ready, blocked, or prerequisite-waiting step from
`completion_execution_plan.run_order`, maps execution steps back to the
gate-oriented `completion_plan`, prefers external completion helpers over
repeatable local/report refresh helpers when choosing the next action, and
prints the command or required `missing_subjects`/`invalid_subjects` without
printing secret values. Steps whose gates are `out_of_scope` are not selected as
the next action. `--doctor` prints a redacted operator checklist for the
next step, including placeholder names, secret placeholder names, preflight
missing/invalid subjects and details, redacted effective env-field status such
as present/placeholder/value for non-secret fields and present/placeholder/length
for secret fields, and the next command. It is read-only, does not print
credential values, and does not create product-complete evidence.
For `external_s3`, the doctor also prints `guidance[external_s3].*` lines for
endpoint, bucket, stable prefix, path-style mode, managed credential Secret mode,
existing Kubernetes Secret mode, derived authority id, and the validate-then-run
sequence. The guidance reminds operators that `--env-file` values are defaults,
caller environment variables override them, existing-Secret mode requires
effective AWS credential env vars to be empty, the existing Secret lives in
`VELORIX_K8S_NAMESPACE` default `velorix-product`, validate-only checks input
shape while real execution checks Secret existence and keys, `AWS_ENDPOINT_URL`
must be the service endpoint only, and `VELORIX_S3_PREFIX` must be a stable safe
object prefix. Those lines appear only when the report selects an S3 step, which
requires opting into actual external S3/OSS completion.
For `ingress`, `durability`, and `hiqlite_backend_time`, the same doctor output
prints gate-specific `guidance[...]` lines instead of only raw missing/invalid
subjects. Ingress guidance distinguishes apply mode from pre-managed public
ingress/DNS/TLS, requires the HTTPS endpoint host to match
`VELORIX_PRODUCT_INGRESS_HOST`, and reminds operators that public attestation
needs data-plane and admin bearer tokens. Durability guidance makes the
external-S3 prerequisite explicit, lists the operator review flags, and states
that the helper records review evidence rather than creating provider policies
or PVCs. Hiqlite backend-time guidance keeps product-complete trust scoped to
release CI, points to the release env template and Sigstore inputs, and repeats
that the release commit must be the Velorix commit rather than the Hiqlite
authority source revision. Doctor output also expands Hiqlite release preflight
missing/invalid details, ingress bearer-token-source booleans, durability
authority status, and durability review-flag field status while keeping secret
values redacted.
With
`--fail-on-incomplete` it exits `75` until the report proves
`product_complete=true`.

The report lists `product_complete`,
`product_complete_blockers`, gate counts, per-gate status, and concrete next
commands for the remaining external evidence, such as public ingress/TLS/auth,
and Hiqlite backend-time release attestation. It also writes
`completion_scope.external_s3_required=false`,
`completion_scope.public_ingress_required=false`,
`completion_scope.hiqlite_release_required=false`, and
`completion_scope.excluded_gates=["public_ingress_tls_auth",
"object_store_external_authority", "object_store_durability_policy",
"hiqlite_backend_time_release"]` for the default scope. The report also keeps
separate in-scope `tls_auth_boundary` and `hiqlite_backend_time_boundary` gates
for local REST access and replicated backend-time safety; the public-ingress and
trusted-release gates remain out of scope.
`completion_scope.warnings` repeats that this default scope does not prove public
DNS, public TLS issuance, ingress-controller routing, external-client
reachability, object-store durability, or Sigstore-backed release provenance.
The report derives its
final `product_complete` value from the gate statuses, treating `pass` and
`out_of_scope` as accepted statuses. A helper that attaches one piece of
evidence cannot promote the whole product to complete while another in-scope
gate is still blocked, diagnostic, or missing. It also derives
`product_complete_blockers` from those non-passing in-scope gates and preserves
the raw `product-evidence.json` blocker strings under
`product_completion_source.product_evidence_product_complete_blockers` for
provenance. The same gate data is exposed as `completion_plan`: each non-pass
in-scope gate is classified as `input_required`, `waiting_on_prerequisite`,
`runnable`, or `blocked_without_action`, and the plan lists
`input_required_steps`, `waiting_steps`, and `runnable_steps` for automation.
Out-of-scope gates are listed separately under `completion_plan.excluded_steps`
and are retained in `gates[]` for audit. The classification uses the command
text and `input_summary`: any remaining placeholder group,
preflight `missing`/`invalid` issue, or fail-closed release preflight issue keeps
the step in `input_required` instead of `runnable`. Input-related plan steps also
include `input_summary`, which merges the redacted preflight status with the
relevant `placeholder_groups`, including `secret_placeholders`. Each
preflight step carries redacted `missing` and `invalid` issue subjects/details
plus `missing_subjects` and `invalid_subjects`, so callers do not have to infer
required env values from separate report sections. The Hiqlite backend-time
plan step also includes a redacted `release_preflight` summary from
`hiqlite-backend-time-release-preflight.json`, because that release/Sigstore
validation is separate from the general completion input preflight. When
`complete-vind-product-input-preflight.json` exists, the report includes each
completion step's redacted input status, missing count, invalid count, and
forced blocker count. When `complete-vind-product-env.json` exists, the report
also includes `completion_handoff` with the generated
`complete-vind-product.env` path, placeholder count, product-evidence-derived
values, fixed release values, `placeholder_groups`, `secret_placeholders`, and
the single top-level command sequence to pass the env file to
`scripts/complete-vind-product.sh --env-file`, dry-run, and execute the
completion driver. The
object-store durability gate is
ordered deliberately when S3/OSS is in scope: while a
slice is still using local RustFS or any local development authority, the
report points first to `scripts/run-vind-product-external-s3.sh`; it only
prints the durability attestation command after a nonlocal external S3/OSS
authority is already proven. In JSON, that dependent durability gate carries
`blocked_by: ["object_store_external_authority"]` so automation can distinguish
the prerequisite authority blocker from the follow-on durability review. In the
default scope, both `object_store_external_authority` and
`object_store_durability_policy` are `out_of_scope`; this is not durability
evidence and does not imply local RustFS or any local object store is durable.
Likewise, `public_ingress_tls_auth` is `out_of_scope` by default; this is not
public DNS, public TLS issuance, ingress-controller routing, or external-client
reachability evidence. `hiqlite_backend_time_release` is also `out_of_scope` by
default; this is not trusted CI/Sigstore release provenance, even though the
in-scope `hiqlite_backend_time_boundary` gate can prove the deployed local-vind
slice is using replicated Hiqlite authority time for owner TTL and failover.
This report is diagnostic only and does not create product-complete evidence. Set
`VELORIX_VIND_PRODUCT_COMPLETION_REPORT=0` to skip automatic report generation.

If a product slice is still deployed but `product-evidence.json` is missing
top-level `deployed_images`, refresh that evidence from the live Kubernetes
Deployment and Pod status:

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
scripts/refresh-vind-product-deployed-images.sh
```

The refresh fails closed unless every current Ready Pod selected by the
Deployment template labels reports a single `imageID` digest. If the
Deployment's `velorix.dev/image-digest` annotation is missing or stale, the
helper patches only that Deployment template annotation to the observed digest,
records `velorix.dev/image-digest-source=observed-pod-imageid-after-rollout`,
waits for rollout, re-collects Deployment/Pod evidence, and then updates
product evidence. This keeps product evidence bound to both the declared
Deployment template and the observed runtime Pod status, while avoiding stale
local Docker image IDs after image import into k3d/containerd. The script does
not infer release product evidence from Pod status alone, does not change
container images, and does not create PVCs. You can still rerun the product
slice with explicit `VELORIX_API_IMAGE_DIGEST`/`VELORIX_META_IMAGE_DIGEST` when
you want to supply release digests before rollout.

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
VELORIX_API_IMAGE_DIGEST=sha256:REPLACE_WITH_API_DIGEST \
VELORIX_META_IMAGE_DIGEST=sha256:REPLACE_WITH_META_DIGEST \
scripts/run-vind-product.sh
```

To test local API pod handoff without creating PVCs, run the standing-runtime
failover smoke against an already deployed product slice:

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
VELORIX_API_LOCAL_PORT=8080 \
scripts/smoke-vind-standing-runtime-failover.sh
```

The smoke deletes the current owner API pod, waits for
`deployment/velorix-api`, reattaches to a writer-owner pod, ingests two rows
through REST, and queries the promoted API route. It writes
`standing-runtime-failover-smoke.json` and updates
`product-evidence.json` by default. Set
`VELORIX_STANDING_RUNTIME_FAILOVER_UPDATE_PRODUCT_EVIDENCE=0` to leave product
evidence untouched. This is local operation evidence only:
`trusted_for_product_complete` and
`production_wall_clock_failover_attestation` stay `false`.
In trusted release CI, rerun the same real failover probe with
`VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST=1` before Hiqlite backend-time
preflight. That mode still deletes and reacquires a real API owner pod, but it
also records `evidence_scope=release_ci_deployed_product`,
`failover_probe_kind=release_bounded_wall_clock_failover`,
`backend_time_source_kind=raft_replicated_authority_time`,
`authority_time_observed=true`, owner TTL/bound values, owner epoch advance,
and affected API pods.

```bash

curl -X POST "$VELORIX_API_URL/v1/query-policies" \
  -H "$VELORIX_API_AUTH_HEADER" \
  -H 'content-type: application/json' \
  -d '{
    "query_policy_id": "interactive",
    "policy": {
      "max_sql_bytes": 4096,
      "planning_timeout_ms": 1000,
      "execution_timeout_ms": 5000,
      "max_output_rows": 1000,
      "max_output_bytes": 1048576,
      "max_scan_files": 100,
      "max_scan_bytes": 134217728,
      "max_object_requests": 100,
      "max_concurrent_queries": 4,
      "memory_limit_bytes": 536870912,
      "spill_limit_bytes": 1073741824
    }
  }'

curl -X POST "$VELORIX_API_URL/v1/views" \
  -H "$VELORIX_API_AUTH_HEADER" \
  -H 'content-type: application/json' \
  -d '{
    "view_id": "orders_by_account",
    "urlPath": "/orders/by-account/:account_id",
    "input_relation_id": "orders",
    "input_relation_version": "2026-05-05.v1",
    "sql": "select account_id, sum(amount) as sum, count(*) as count from orders group by account_id",
    "sql_template": "select key_json, value_json, weight from orders_by_account where key_json = {{ context.params.account_id | is_required | is_string | to_json }} and {{ context.params.min_sum | is_integer(min=0) }} >= 0 order by key_json",
    "description": "Order totals by account",
    "request": [
      {
        "fieldName": "account_id",
        "fieldIn": "path",
        "type": "string",
        "description": "Account id",
        "validators": ["required", "string"]
      },
      {
        "fieldName": "min_sum",
        "fieldIn": "query",
        "type": "integer",
        "description": "Minimum aggregate sum",
        "defaultValue": 0,
        "validators": ["required", "integer(min=0)"]
      }
    ],
    "response_schema": {
      "columns": [
        { "name": "account_id", "type": "string", "source": "key_json" },
        { "name": "sum", "type": "int64", "source": "value_json.sum" },
        { "name": "count", "type": "int64", "source": "value_json.count" },
        { "name": "weight", "type": "int64", "source": "weight" }
      ]
    },
    "response_formats": ["json"],
    "query_policy_id": "interactive"
  }'

curl "$VELORIX_API_URL/v1/views" -H "$VELORIX_API_AUTH_HEADER"

curl "$VELORIX_API_URL/v1/views/positive_scores_by_user" \
  -H "$VELORIX_API_AUTH_HEADER"

curl "$VELORIX_API_URL/v1/openapi.json" -H "$VELORIX_API_AUTH_HEADER"

curl "$VELORIX_API_URL/v1/api/scores/positive" -H "$VELORIX_API_AUTH_HEADER"

curl "$VELORIX_API_URL/v1/api/orders/by-account/acct-a?min_sum=0" \
  -H "$VELORIX_API_AUTH_HEADER"
```

For an intentionally unauthenticated local development run:

```bash
VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1 scripts/run-vind-product.sh
```

`POST /v1/ingest` is schema-driven. It reads the registered
`VelorixRelationCatalogV1` for the request's `relation_id` and
`relation_version`, then converts each JSON row using the catalog column names
and Arrow physical types. `POST /v1/relations/scores-default` registers the
default Feldera/generated `scores` relation used by the built-in
`positive_scores_by_user` view. The older `orders` shortcut remains only a
generic DataFusion convenience path; custom relations registered through
`POST /v1/relations` can ingest rows with their own column names as long as the
registered incremental adapter scope is supported.

`POST /v1/views` creates a named materialized view API over a registered
relation. The optional `urlPath`, `description`, `request`, `response_schema`,
`sql_template`, `response_formats`, and `query_policy_id` fields are returned
by `GET /v1/views`, `GET /v1/views/{view_id}`, and `GET /v1/openapi.json` so
clients can discover available Data APIs. OpenAPI view operations expose the
linked policy as `x-velorix-query-policy-id`. `urlPath` promotes the view into a
GET Data API under `/v1/api/*`; `:name` segments become path parameters. For
example,
`/orders/by-account/:account_id` becomes
`GET /v1/api/orders/by-account/acct-a?min_sum=0`.

`POST /v1/query-policies` creates a durable query policy under the default
tenant's query-policy catalog. `POST /v1/views` can then reference that policy
by `query_policy_id`; view queries read the catalog record and pass the policy
to the same DataFusion execution path used by recovered materialized views and
standing-runtime template scans. For example:

```bash
curl -X POST "$VELORIX_API_URL/v1/query-policies" \
  -H "$VELORIX_API_AUTH_HEADER" \
  -H 'content-type: application/json' \
  -d '{
    "query_policy_id": "interactive",
    "policy": {
      "max_sql_bytes": 4096,
      "planning_timeout_ms": 1000,
      "execution_timeout_ms": 5000,
      "max_output_rows": 1000,
      "max_output_bytes": 1048576,
      "max_object_requests": 100,
      "max_concurrent_queries": 4
    }
  }'

curl "$VELORIX_API_URL/v1/query-policies/interactive" \
  -H "$VELORIX_API_AUTH_HEADER"
```

The API catalog accepts only production-bounded table-scan policies for this
path. If a policy omits required bounds such as SQL size, planning/execution
timeouts, output limits, scan/object-request limits, memory, or spill limits,
creation fails before the policy can be linked to a view. If a view references a
missing or invalid policy, creation fails before the view is made active. If a
query exceeds the linked policy, the query fails closed with the underlying
query-policy error.

The product smoke records this as executable evidence: it writes the accepted
`query-policy-interactive.json` and `query-policy-interactive-read.json`, then
also writes `query-policy-weak-rejection.json` for a policy missing production
bounds and `query-policy-missing-view.json` for a view that references an absent
policy. `product-evidence.json` must report both `production_bounds_required`
and `weak_policy_rejected` before the first-E2E readiness validator accepts the
query-policy section.

In product mode, no-artifact `POST /v1/views` is the Feldera compiler/deploy
entry point. If the requested spec exactly matches a trusted linked generated
package descriptor already present in the Velorix image, `velorix-api` resolves
that server-owned descriptor, deploys the standing runtime, and returns `201
Created` with `execution_mode: "standing_runtime"`. The descriptor owns the
allowed SQL, relation binding, generated crate/ABI, compiler identity, artifact
id/hash identity, output schema mapping, and runtime factory selection; callers
do not submit artifact provenance for this path. The current trusted linked
package is `scores_by_user_generated` for the built-in
`positive_scores_by_user` SQL.

If no linked package matches, `velorix-api` stores the view spec and returns
`202 Accepted` with `execution_mode: "feldera_compile_pending"`,
`lifecycle.compile_status: "pending"`, and `lifecycle.deployment_status:
"not_deployed"`. It also writes a create-only compile/deploy job record under
`v1/view-compile-deploy-jobs/{view_id}/spec-sha256/...` and returns
`compile_job_id` so a worker can pick up the durable request. The job record is
self-contained for the compiler boundary: it carries
`compiler_request.request_kind`, `view_id`, `spec_hash`, SQL, input relation
schemas, output relation schemas, and the materialized view shape. The worker
compares that embedded request with the active pending view before activation
and skips the job if they diverge. A linked-package activation worker can be run
with:

```bash
curl "$VELORIX_API_URL/v1/view-compile-deploy/jobs" \
  -H "$VELORIX_ADMIN_AUTH_HEADER"

curl -X POST "$VELORIX_API_URL/v1/view-compile-deploy/run-once" \
  -H "$VELORIX_ADMIN_AUTH_HEADER"
```

The product smoke now calls the job catalog route after creating the
no-artifact pending view and stores the response as
`view-compile-deploy-jobs.json`; it then calls the admin worker route, stores
`view-compile-deploy-run-once.json`, reads the activated
`pending_scores_by_user` view into
`pending-scores-view-after-compile-deploy.json`, and queries the activated
standing runtime into `pending-scores-query-after-compile-deploy.json`.
`product-evidence.json` reports this under
`api.compile_deploy.job_catalog_verified` and
`api.compile_deploy.worker_run_verified`.
If a reused product namespace already has `pending_scores_by_user` active,
Velorix does not treat an unrelated pending job catalog as valid product
evidence. The smoke fails closed unless `view-compile-deploy-jobs.json` still
contains the `pending_scores_by_user` job and embedded compiler request, because
the product-complete proof must bind the durable compile/deploy queue to the
view named in `product-evidence.json`.

That route first reconciles active `feldera_compile_pending` views back into
missing compile/deploy job records, then scans pending jobs. Matching jobs are
activated only when their trusted descriptor now has a generated package linked
into the running image, and the job is marked `success`/`running`. If a previous
activation reached the active view but failed before updating the job record,
the next run repairs the stale pending job instead of leaving it pending
forever. During activation, the view is first attached as a non-queryable
`deploying` standing runtime so committed ingest that arrived while the view was
pending can be replayed before the route becomes queryable. Jobs without a
matching descriptor or package stay pending and are reported as skipped. While
pending or deploying, `query_enabled` is `false`, direct
`/v1/views/{view_id}/query` and promoted `/v1/api/*` query calls fail closed,
and pending promoted routes are omitted from OpenAPI.
The active view transition is guarded by object-store conditional update
(`PutMode::Update` / ETag CAS). `velorix-api` enables S3 conditional PUT
explicitly and product startup fails closed if the artifact-catalog namespace
cannot prove conditional update support, so parallel activations cannot silently
degrade to last-writer-wins on RustFS/S3-compatible storage.

`velorix-api` must not compile Rust, run Cargo, or dynamically load generated
native code inside the API process. The intended product boundary is:
`compile` means Feldera SQL -> generated Rust/DBSP -> executable runner
artifact in a separate compiler/build plane; `deploy` means a validated runner
instance is started and atomically marked active in metadata. The built-in
generated-package endpoint such as `POST /v1/views/scores-positive-default`
remains a static release fixture for exercising the standing-runtime path.
No-artifact legacy recovered SQL views are a development compatibility path
only. `velorix-api` defaults `VELORIX_ALLOW_LEGACY_RECOVERED_SQL_VIEWS` to
`false`; existing `legacy_recovered_sql` view records remain visible for
migration/debugging but report `query_enabled: false`, their query endpoints
fail closed, and their promoted `/v1/api/*` routes are omitted from OpenAPI.

`POST /v1/query` is also disabled by default in product mode. It is a generic
ad hoc SQL endpoint over recovered DataFusion state, not a named Feldera/DBSP
view API. Leaving it enabled would let callers bypass the product contract where
relations are ingest targets and views are the predefined materialized
computation/query surface. Use the promoted `GET /v1/api/*` routes or
`/v1/views/{view_id}/query` for product queries. Set
`VELORIX_ENABLE_GENERIC_QUERY=1` only for development compatibility or local
diagnostics where that broader SQL surface is intentionally accepted.

Artifact-backed generated runtime views support committed-epoch cursor
pagination on GET query endpoints. The response includes `logical_epoch` for
the returned materialized snapshot. Pass `epoch=<logical_epoch>` to read a
specific committed epoch; the current generated runtime serves its current
materialized epoch and fails closed for unavailable older epochs. Pass
`max_rows=<positive integer>` to limit returned materialized rows. If the
response contains `next_page_token`, pass it back as `page_token=<value>` to
read the next page. `epoch`, `page_token`, and `max_rows` are reserved query
parameters for view APIs and cannot be declared as custom request fields.
When an artifact-backed view API has a `sql_template`, Velorix currently
fetches the committed materialized snapshot from the linked standing runtime
and applies the template through DataFusion prepared bindings over that Arrow
snapshot. Cursor pagination with `page_token` or `max_rows` is rejected for
this templated path until predicate/pagination pushdown exists in the standing
runtime API. `epoch=<logical_epoch>` remains supported for the runtime's
current committed epoch.

After each successful artifact-backed ingest application, Velorix writes an
immutable standing runtime checkpoint under
`v1/standing-runtime-checkpoints/{tenant_id}/{program_id}/{view_id}/epochs/...`.
When a `MetaStore` is configured, the metadata service is the recovery
authority: the API publishes the new checkpoint pointer with a compare-and-
publish operation against the previously committed pointer and the current
standing-runtime owner token. The owner lease fences the mutable runtime state
for `{tenant_id, program_id, view_id}` in this product slice. `velorix-api`
uses a process-incarnation owner id derived from the configured operator id,
acquires or renews ownership before mutating the runtime, and includes the
returned `{owner_id, owner_epoch}` token in the checkpoint publish request.
Restart restores exactly the published pointer before replaying committed ingest
envelopes. A higher-epoch object-store checkpoint that did not win metadata
publication is an orphan and is ignored by recovery. Without a `MetaStore`,
local development continues to restore from the newest checkpoint object by
epoch and writes a best-effort `latest.json` marker in the scoped directory.
The checkpoint record also stores stream/partition replay frontiers, and restart
replay begins after those frontiers instead of re-reading ingest objects already
covered by the restored runtime checkpoint.
Within one `velorix-api` process, artifact-backed runtime apply, checkpoint
creation, replay-frontier merge, checkpoint object write, and latest-marker
write are serialized per `{tenant_id, program_id, view_id}`. That protects the
local vind product slice from concurrent REST ingests losing replay-frontier
metadata. View activation and runtime insertion use the same per-view boundary,
so ingest does not silently skip an active artifact-backed view whose runtime is
not installed yet. If runtime mutation succeeds but checkpoint persistence
fails, the in-process runtime is removed and later calls fail closed until the
runtime is restored from durable state. Standing runtime queries also acquire
the same per-view boundary, so a request cannot observe a candidate runtime
while an ingest is still waiting for checkpoint publication. A read replica may
serve a restored runtime without owning the write lease, but only when its local
committed checkpoint identity matches the `MetaStore` latest pointer; otherwise
it evicts the local runtime and fails closed. Hiqlite or a singleton gRPC
metadata service can provide the owner lease and compare-and-publish authority;
per-pod in-memory metadata and OSS-only metadata are not production-safe
multi-writer authorities and must be guarded by deployment readiness before
production scale-out. `velorix-api` therefore treats standing-runtime fencing as
required in environment-driven startup unless the operator explicitly sets
`VELORIX_STANDING_RUNTIME_FENCING=unsafe-dev-only` for a single-replica local
development run. `VELORIX_API_REPLICA_COUNT>1` is incompatible with that unsafe
mode. The production capability check is granular and is revalidated by
`/readyz` and by standing-runtime create/apply/query paths before they serve
state. The metadata service must report a supported capability schema, the
expected view-runtime owner scope, linearizable owner acquire/read/latest
semantics, durable monotonic owner epochs, backend-time expiry, owner+latest
atomic publish validation, and control-plane authentication. Configure
`VELORIX_META_BEARER_TOKEN` on both `velorix-meta` and `velorix-api` to require
an `Authorization: Bearer ...` token on every metadata RPC, including capability
reads. A configured token must be nonempty ASCII without whitespace or control
characters; an invalid configured token is a startup error rather than a silent
dev fallback. Without metadata auth the capability remains not production
multi-writer safe even when Hiqlite is the durable backend. In a real cluster,
the bearer token still needs a protected transport or service-mesh/workload
identity boundary because Velorix's current gRPC listener does not itself
terminate mTLS.

The current owner scope is intentionally the view runtime scope because the
generated vind package instantiates and checkpoints one standing runtime per
view. If a future generated Feldera package shares mutable DBSP state across
multiple output views, the lease, checkpoint path, latest pointer, replay
frontiers, and query validation must move to that shared runtime scope, for
example `{tenant_id, program_id}` or an explicit `runtime_instance_id`.

`sql_template` follows the VulcanSQL-style dynamic parameter form
`{{ context.params.account_id | is_required | is_string }}`. Legacy recovered
SQL views expose materialized rows as `key_json`, `value_json`, and `weight`;
for those views Velorix also supports a `to_json` filter for matching
JSON-encoded keys. Artifact-backed standing runtime templates query the
generated runtime's output columns directly, such as `user_id`, `sum`, and
`count` for `scores_by_user_generated`. The `request` list documents where
parameters are supplied (`path` and `query` are currently enabled for promoted
GET APIs) and the OpenAPI output exposes those request fields as path/query
parameters. Query fields can declare `defaultValue`; Velorix validates the
default against the same type and validator contract as caller-supplied values,
uses it when the caller omits the parameter, and includes it in the generated
OpenAPI schema. Velorix enforces parameter provenance: `path` fields must
appear in `urlPath` and must be supplied by the promoted API path, not by query
string or the generic `/v1/views/{view_id}/query` endpoint. Query responses are
shaped by `response_schema` when one is
configured. Caller supplied SQL is rejected on view APIs; execution uses the
registered `sql_template`. Template placeholders are compiled into DataFusion
positional parameters (`$1`, `$2`, ...) and the caller values are passed
separately as prepared bind values, not interpolated into SQL text.

Legacy recovered SQL view materialization is DBSP-backed only for the
development bootstrap single-relation sum/count shape:

```sql
select <primary_key>, sum(<value_column>) as sum, count(*) as count
from <relation>
group by <primary_key>
```

When `VELORIX_ALLOW_LEGACY_RECOVERED_SQL_VIEWS=1` and no trusted generated
package matches the view, `POST /v1/views` rejects SQL outside that legacy
materialization scope, such as joins, filters, windows, nested queries, or
arbitrary projections. This is intentional: unsupported SQL must fail at
definition time instead of silently falling back to a non-incremental full scan
or storing an unfiltered aggregate while the view definition says it is
filtered.

Linked generated-package views are the first product Feldera package path. They
bypass the hand-coded DBSP SQL shape gate because the already-linked generated
package is the executable view implementation. This follows the package-first
Feldera runtime integration described in
[Feldera Package-First Runtime Design](../superpowers/specs/2026-05-27-feldera-package-first-runtime-design.md).
For linked package auto-deploy, the request supplies only the relation/view
spec. Velorix derives the `StandingViewSpec`, resolves server-owned
`FelderaCompileArtifactMetadata`, validates it against the registered relation
catalog and spec hash, persists the metadata under
`v1/feldera-artifacts/{artifact_id}/sha256/{artifact_hash}.artifact.json`, and
stores the selected artifact binding on the active view record. The generated
Rust package must already be linked into the running Velorix image and have a
registered `StandingProgramRuntimeFactory`; Velorix does not compile or
dynamically load Rust from object storage at request time.

The product image exposes linked generated packages by default. The current
default package is `scores_by_user_generated`. `VELORIX_GENERATED_ARTIFACT_PACKAGES`
is an optional operator override for restricting or replacing that effective
package list; it is not required to enable the default Feldera/generated runtime
path.

For the default package, `POST /v1/views` with the built-in spec below creates
the REST-callable materialized view without requiring the caller to hand-author
artifact metadata. It registers `positive_scores_by_user`, backed by the linked
`scores_by_user_generated` package, over:

```sql
select user_id, sum(score) as sum, count(*) as count
from scores
where score > 0
group by user_id
```

You can query it either by view id:

```bash
curl "$VELORIX_API_URL/v1/views/positive_scores_by_user/query" \
  -H "$VELORIX_API_AUTH_HEADER"
```

or through the promoted root-style API path:

```bash
curl "$VELORIX_API_URL/v1/api/scores/positive" -H "$VELORIX_API_AUTH_HEADER"
```

Abbreviated request shape:

```json
{
  "view_id": "positive_scores_by_user",
  "urlPath": "/scores/positive",
  "input_relation_id": "scores",
  "input_relation_version": "2026-05-24.v1",
  "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
  "request": []
}
```

For a custom promoted artifact-backed endpoint filtered by user, define the
API template against the generated output columns:

```json
{
  "urlPath": "/scores/by-user/:user_id",
  "request": [
    {
      "fieldName": "user_id",
      "fieldIn": "path",
      "type": "string",
      "validators": ["required", "string"]
    }
  ],
  "sql_template": "select user_id, sum, count from positive_scores_by_user where user_id = {{ context.params.user_id | is_required | is_string }} order by user_id"
}
```

If `generated_rust.crate_name` is not in the effective package list, view
creation fails closed with a package-unavailable error. If the package is listed
but the binary does not contain a matching runtime factory, view creation also
fails closed before the active view record is written.

Useful overrides:

```bash
VELORIX_VIND_CLUSTER=velorix-product \
VELORIX_VIND_REUSE_EXISTING=1 \
VELORIX_API_LOCAL_PORT=18080 \
  scripts/run-vind-product.sh
```

Local environment blockers:

The script treats host capacity, vCluster bootstrap failures, node pressure, and
unschedulable pods as local environment blockers, not product evidence failures.
It writes `target/velorix-product/local-environment-blocker.json` when local
host free disk is below `VELORIX_LOCAL_MIN_FREE_DISK_GIB` (default `20`),
vCluster creation, context selection, or API readiness fails, or when Kubernetes
reports disk, memory, or PID pressure, a not-ready node, a matching scheduler
taint, an Evicted pod, or an Unschedulable pod. Bootstrap blockers include local
runtime issues such as a missing CRI v1 `RuntimeService`, local open-file
exhaustion, a vCluster kube context that was not created, or vCluster standalone
exiting with status `137`, which is treated as local runtime resource pressure or
forced termination until proven otherwise. It also recognizes vCluster
standalone `vm-container` failures such as `procReady not received` or `cannot
exec in a stopped container`, which mean the local Docker runtime could not keep
the privileged systemd container alive long enough for the vCluster install
step. Remediation is to inspect the local environment, fix the local
Docker/vCluster runtime, free file descriptors or local capacity, increase
Docker/Colima resource limits when needed, prune stale local artifacts when
appropriate, or recreate the failed/reused vCluster after capacity is available.
Do not add PVCs to work around this path; the product contract remains no-PVC.
Set `VELORIX_LOCAL_DISK_PREFLIGHT=0` only when you intentionally accept local
capacity risk for a diagnostic run.
The product runner will retry vCluster bootstrap twice by default after
transient local standalone failures such as `procReady not received` or exit
status `137`, cleaning only the failed cluster container/network for the current
generated cluster name. Override the retry count with
`VELORIX_VCLUSTER_CREATE_RETRIES=0` or a larger non-negative integer. On final
bootstrap failure, it also writes a read-only doctor snapshot to
`local-environment-doctor.json` and links that report plus its
`remediation_commands` from `local-environment-blocker.json`.
That final doctor snapshot enables the vCluster standalone compatibility probe
with `VELORIX_DOCTOR_VCLUSTER_STANDALONE_PROBE=1`, so
`local-environment-doctor.json` records whether `ghcr.io/loft-sh/vm-container`
can stay running for a short systemd bootstrap check with the same containerd
socket bind mount used by vCluster's Docker driver. This probe is not trusted for
product-complete evidence; it only classifies the local environment blocker.

Before rerunning after a local failure, inspect the environment with:

```bash
scripts/doctor-vind-local.sh
```

The doctor is read-only by default, uses the same
`VELORIX_LOCAL_MIN_FREE_DISK_GIB` floor, and writes
`target/velorix-product/local-environment-doctor.json`. The JSON includes
`capacity.host.available_free_gib` and parsed Docker reclaimable capacity such
as `capacity.docker.build_cache_reclaimable_gib`, so you can distinguish product
failures from local cleanup work. It also includes `remediation_commands` with
read-only checks and explicitly marked destructive cleanup commands. For
automation that should fail before a product run when local capacity is
insufficient, pass `--fail-on-blocked`; it returns exit code `75` when the report
status is `blocked`. To include the vCluster standalone compatibility probe in a
manual doctor run, use:

```bash
VELORIX_DOCTOR_VCLUSTER_STANDALONE_PROBE=1 scripts/doctor-vind-local.sh
```

To explicitly delete Docker build cache, run:

```bash
scripts/doctor-vind-local.sh --prune-build-cache --yes
```

`velorix-meta` is enabled by default in the product script so relation catalog
and ingest admission calls cross the same gRPC metadata boundary used by the
API service. The default backend is `memory`, protected by a generated local
bearer token stored in the Kubernetes Secret `velorix-meta-auth`. API and meta
images default to per-run tags, use locally loaded images, and carry pod-template
annotations for the run id plus Secret hashes so reruns roll pods instead of
silently reusing stale binaries or stale bearer-token environments. In the
generated Deployment, `velorix-meta` uses Kubernetes `Recreate` rollout
strategy. The metadata service can hold backend authority state while starting,
so the runner must not rely on a rolling update that keeps an old metadata pod
alive while the replacement waits to become ready.
In the
default local RustFS mode, the RustFS image is pinned to
`rustfs/rustfs:1.0.0-beta.4`; override `VELORIX_RUSTFS_IMAGE` with another
version tag or digest if needed. Mutable tags such as `latest` are rejected
unless `VELORIX_ALLOW_MUTABLE_RUSTFS_IMAGE=1` is set explicitly. Leave
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` unset for the default local
RustFS deployment; the script generates non-default per-run credentials because
RustFS rejects known default credentials on non-loopback listeners. In
`VELORIX_OBJECT_STORE_MODE=external-s3`, set `AWS_ENDPOINT_URL`,
`AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`; set `AWS_SESSION_TOKEN` too
when using temporary credentials. Supplying only one credential variable or
known defaults such as `rustfsadmin/rustfsadmin` fails preflight. For production
clusters that already manage S3 credentials, set
`VELORIX_S3_CREDENTIALS_SECRET_NAME` and
`VELORIX_S3_CREDENTIALS_SECRET_MANAGED=0`; the Secret must contain
`access-key-id`, `secret-access-key`, and optional `session-token` keys.
`VELORIX_S3_FORCE_PATH_STYLE=1` is the default for S3-compatible OSS endpoints
and is propagated to the validation Job, API, meta service, and ingest-writer
pods so every client uses the same request shape.
Set `VELORIX_S3_PREFIX` explicitly when using external S3/OSS product
completion; the generated handoff env file already does this. Direct
`--validate-only` runs require the same stable prefix that will be used for the
execution run, so the evidence and durability authority do not drift between
commands.
For VPC/private-network OSS endpoints, prefer a provider DNS name over a raw
RFC1918 IP address. Raw private IP endpoints are rejected by default together
with localhost and link-local endpoints so product-completion evidence cannot
silently collapse back to a local-development authority.
The script then runs `velorix-external-s3-validate` inside the vCluster and
requires the configured bucket plus authority prefix to pass a scoped
`head-bucket`, `put-object`, `get-object`, exact-key
`list-objects-v2 --max-keys 1`, and `delete-object` probe unless
`VELORIX_EXTERNAL_S3_VALIDATE=0` is set. Product evidence records the validation
key and the Kubernetes Job/log artifacts so the first-E2E wrapper can prove the
product slice used the same bucket and prefix as the production GC authority.
The script stores S3 and Hiqlite credentials in Kubernetes Secrets rather than
in the generated Deployment YAML files, can reuse an existing S3 credential
Secret, records only credential source plus hashes in evidence, and writes local
artifacts with private file permissions.
By default each run writes authority, ingest, view, and metadata
objects under a run-scoped S3 prefix (`product/<run-id>`), even when the
vCluster is reused. Set `VELORIX_VIND_PRESERVE_STATE=1` to use the stable
`product` prefix, or set `VELORIX_S3_PREFIX` explicitly, when the run is meant
to test recovery from preserved object-store state. Before opening the local
REST port-forward, the script refuses to continue if the chosen local port is
already serving something else, and the API Service selector is scoped to the
current run id so smoke traffic cannot be
routed to a stale API pod from an earlier run. The local RustFS and
`velorix-meta` Services are scoped the same way. After `velorix-meta` rolls out, the script
runs a `velorix-meta smoke` Job inside the cluster; that Job verifies unauthenticated
metadata RPCs are rejected and that the configured bearer token can read the
expected metadata backend capability, then writes and reads a run-scoped smoke
relation catalog before `velorix-api` is deployed. When
`VELORIX_INGEST_WRITER_SMOKE=1` (the default), the script also builds and loads
`Dockerfile.ingest-writer`, creates a run-scoped ConfigMap containing an encoded
default scores ingest envelope, runs a Kubernetes Job with the
`velorix-ingest-writer` image, verifies the Job and Pod succeeded through the
default lease-guarded entrypoint, stores the Job, Pod, and log artifacts as
`ingest-writer-job.json`, `ingest-writer-pods.json`, and
`ingest-writer-job-log.json`, and then restarts `velorix-api` to prove the
writer-appended object-store batch is recovered into the default materialized view. Set
`VELORIX_INGEST_WRITER_SMOKE=0` only when you intentionally want to skip that
deployed writer append check. That single Job is product evidence for
Pod-internal lease-guarded append, but product-complete still requires
script-generated lifecycle evidence proving deployed multi-pod overlap
rejection, adjacent append, crash/restart reconstruction, and Kubernetes Lease
handoff. Externally supplied lifecycle JSON is advisory and does not clear
`product_complete` by itself.
The API Deployment uses
`/readyz` as its Kubernetes readiness probe, so rollout completion also proves
the process can reach its configured metadata service, read the advertised
fencing capability, and expose object-store startup capability evidence. The
product script fails closed unless `/readyz` reports
`object_store.artifact_catalog.conditional_update=true`; this is the evidence
that active view CAS is backed by S3/RustFS conditional update rather than
last-writer-wins overwrites. `/healthz` remains the liveness probe. Because the
default metadata backend is not durable production fencing, the script still runs the API with
`VELORIX_STANDING_RUNTIME_FENCING=unsafe-dev-only` and
`VELORIX_API_REPLICA_COUNT=1` by default. The script rejects
`VELORIX_API_REPLICA_COUNT>1` in `unsafe-dev-only` mode. Set
`VELORIX_STANDING_RUNTIME_FENCING=logical-fencing` to run multiple API replicas
against a metadata backend that proves linearizable owner fencing, durable owner
epochs, owner-validated checkpoint publish, and explicit lease authority
semantics. Set `VELORIX_STANDING_RUNTIME_FENCING=required` only for the
stronger production profile; it fails closed unless `/readyz` reports every
advertised production fencing capability, including bounded wall-clock
failover, as true.
When running the broader first-E2E readiness script, set
`VELORIX_FIRST_E2E_RUN_PRODUCT=1` and
`VELORIX_FIRST_E2E_PRODUCT_PROFILE=logical-fencing` to make that script deploy
this Hiqlite logical-fencing product slice itself. That first-E2E profile sets
`VELORIX_API_REPLICA_COUNT=2`, `VELORIX_META_BACKEND=hiqlite`, and
`VELORIX_STANDING_RUNTIME_FENCING=logical-fencing`; provide
`VELORIX_HIQLITE_NODES` and `VELORIX_HIQLITE_API_SECRET` in the environment.
Readiness schema v5 fails fast without this product evidence, or without an
equivalent `VELORIX_FIRST_E2E_PRODUCT_EVIDENCE` artifact that proves non-dev
standing-runtime fencing, two API replicas, metadata adversarial smoke, and
multi-replica fencing smoke.
The first-E2E wrapper can run `VELORIX_FIRST_E2E_PRODUCT_PROFILE=required` only
when the deployed `velorix-meta` reports the Hiqlite authority-time capability
described below. Even then, this script still emits only `local-vind-only`
evidence until the
product-complete gates exist: external durable object storage unless
`VELORIX_OBJECT_STORE_MODE=external-s3` is configured, passing deployed
multi-replica adversarial ingest/fencing smoke, product-complete ingress/TLS/auth evidence,
and a bootstrapped or externally-attested three-voter Hiqlite authority with
backend-time lease semantics. The local evidence JSON records the selected
object-store mode, the active-view-CAS readiness fields under
`object_store.active_view_cas`, and the full metadata fencing capability under
`standing_runtime_fencing`. Current Hiqlite authority-time runs record
`backend_time_source=raft_replicated_authority_time`,
`lease_authority_kind=raft_replicated_time`,
`lease_expiry_semantics=backend_wall_clock_ttl`,
`multi_writer_fencing_safe=true`, `bounded_wall_clock_failover=true`, and an
empty `blocked_reason`. Owner lease expiry and checkpoint publish validation
consume `Param::raft_serialized_unix_ms()` inside the same Hiqlite
`txn_with_raft_serialized_timestamp` Raft write that mutates the
owner/checkpoint state.
When fencing is set to `logical-fencing` or `required`, the deployed
`velorix-meta` smoke job also runs a metadata-level adversarial check: owner A
publishes a checkpoint with a short logical lease, that lease is driven to
expiry through metadata authority operations, owner B acquires a higher epoch,
owner B publishes the next checkpoint, and a stale owner A publish is rejected
while the latest checkpoint remains the metadata-published pointer. The result
is recorded under
`metadata_store.standing_runtime_adversarial_smoke` in
`product-evidence.json`.
When `VELORIX_MULTI_REPLICA_FENCING_SMOKE=1` (the default), the script includes
a deployed two-pod adversarial smoke runner. It only runs when
`VELORIX_API_REPLICA_COUNT>=2` and the selected fencing profile is satisfied.
For `logical-fencing`, `/readyz.metadata_store.standing_runtime_fencing` must
report `multi_writer_fencing_safe=true` and recognized logical or wall-clock
lease semantics. For `required`, it must additionally report
`production_multi_writer_safe=true`, `production_bounded_failover_safe=true`,
`bounded_wall_clock_failover=true`, and
`backend_time_source_kind=raft_replicated_authority_time`. The smoke
port-forwards to two distinct `velorix-api` pods, creates a view through one
pod, ingests through the owner pod, verifies the other pod can query, verifies a
non-owner ingest is rejected with `409`, then retries on the owner and queries
the converged view through the second pod. Passing evidence is written to
`target/velorix-product/multi-replica-fencing-smoke.json` and surfaced as
`standing_runtime_fencing.multi_replica_fencing_smoke.status=pass` in
`product-evidence.json`. A passing logical-fencing smoke is useful runtime
evidence, but it still does not make `product_complete=true` while
`bounded_wall_clock_failover=false`.
When `VELORIX_STANDING_RUNTIME_FAILOVER_SMOKE=auto` (the default), the product
script also runs a destructive local API-pod failover smoke for non-dev
multi-replica product slices after writing the initial product evidence. Use
`=0` to skip it or `=1` to require it. To run the same smoke manually after a
product slice is up:

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
  scripts/smoke-vind-standing-runtime-failover.sh
```

The smoke creates owner-aware REST attach evidence if needed. It deletes the current
writer-owner API pod, waits for `deployment/velorix-api` to become available,
reattaches to a Ready non-terminating writer-owner pod, then proves REST ingest
and the promoted API query still work. It writes
`target/velorix-product/standing-runtime-failover-smoke.json` and surfaces it
as `standing_runtime_fencing.local_api_pod_failover_smoke.status=pass` in
`product-evidence.json` by default. Set
`VELORIX_STANDING_RUNTIME_FAILOVER_UPDATE_PRODUCT_EVIDENCE=0` when you want to
collect only the standalone smoke evidence. This is local operation-driven
failover evidence for manual E2E testing; it is deliberately not
product-complete wall-clock failover evidence and does not change the Hiqlite
capability bits.
`velorix-api` enforces a bearer token on `/v1/*` when
`VELORIX_API_BEARER_TOKEN` is configured. Control-plane routes such as
`POST /v1/view-compile-deploy/run-once` require the separate
`VELORIX_ADMIN_BEARER_TOKEN`; the data-plane API token is rejected for that
route. The product script configures both token boundaries by default, rejects
missing and wrong bearer tokens during smoke, and only uses
`VELORIX_API_ALLOW_UNAUTHENTICATED_DEV=1` when that explicit local-dev override
is set. `/healthz` and `/readyz` remain unauthenticated so Kubernetes probes
and local diagnostics keep working.
To remove the product-complete ingress/TLS/auth blocker from
`product-evidence.json`, complete the public ingress path against the already
deployed `velorix-api` Service:

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
VELORIX_PRODUCT_INGRESS_HOST=velorix.example.com \
VELORIX_PRODUCT_INGRESS_APPLY=1 \
VELORIX_PRODUCT_INGRESS_ATTEST=1 \
VELORIX_PRODUCT_INGRESS_ATTACH=1 \
VELORIX_PRODUCT_INGRESS_CLASS=nginx \
VELORIX_PRODUCT_INGRESS_TLS_SECRET=velorix-api-public-tls \
VELORIX_PRODUCT_INGRESS_WAIT_TIMEOUT_SECONDS=600 \
VELORIX_INGRESS_ENDPOINT_URL=https://velorix.example.com \
VELORIX_INGRESS_CONTROLLER=nginx \
VELORIX_INGRESS_TLS_AUTH_READY_TIMEOUT_SECONDS=120 \
scripts/complete-vind-product-ingress.sh
```

When you are following the product-completion handoff, use the generated env
file and validate the public ingress inputs before applying Kubernetes or
calling the external HTTPS endpoint:

```bash
scripts/complete-vind-product-ingress.sh \
  --env-file target/velorix-product/complete-vind-product.env \
  --output-dir target/velorix-product \
  --validate-only
scripts/complete-vind-product-ingress.sh \
  --env-file target/velorix-product/complete-vind-product.env \
  --output-dir target/velorix-product
```

`--validate-only` writes `product-ingress-input.json` and checks the public host,
ingress class/controller, TLS Secret name, and HTTPS endpoint/host match without
applying Ingress, calling the public endpoint, attaching evidence, creating
product-complete evidence, or creating PVCs. When
`VELORIX_PRODUCT_INGRESS_ATTEST=1`, it also checks for a data-plane and admin
bearer-token source in `VELORIX_API_AUTH_ENV`, `VELORIX_API_BEARER_TOKEN` /
`VELORIX_ADMIN_BEARER_TOKEN`, or `VELORIX_API_AUTH_HEADER` /
`VELORIX_ADMIN_AUTH_HEADER`; `product-ingress-input.json` records only redacted
source booleans under `auth_token_source`, never token values.

If the Ingress, DNS, and TLS Secret already exist outside Velorix, set
`VELORIX_PRODUCT_INGRESS_APPLY=0` and keep `VELORIX_PRODUCT_INGRESS_ATTEST=1`
and `VELORIX_PRODUCT_INGRESS_ATTACH=1`. Existing-ingress mode does not require
`VELORIX_PRODUCT_INGRESS_CLASS` or `VELORIX_PRODUCT_INGRESS_TLS_SECRET`; it
still requires `VELORIX_PRODUCT_INGRESS_HOST`, `VELORIX_INGRESS_ENDPOINT_URL`,
and `VELORIX_INGRESS_CONTROLLER` so the helper can attest the actual HTTPS
boundary and attach that evidence.

The complete helper runs `scripts/apply-vind-product-ingress.sh`,
`scripts/attest-vind-product-ingress.sh`, and
`scripts/attach-vind-product-ingress.sh` in order. The apply step writes
`product-ingress.json` and `product-ingress-observed.json` under the product
target directory and applies a `networking.k8s.io/v1` Ingress for the existing
`velorix-api` Service. It does not create DNS records, public certificates, TLS
Secrets, or PVCs. `VELORIX_PRODUCT_INGRESS_BACKEND_PROTOCOL` defaults to `http`
on service port `8080`; set it to `https` to route to service port `8443`.
After `kubectl apply`, the apply step waits up to
`VELORIX_PRODUCT_INGRESS_WAIT_TIMEOUT_SECONDS` seconds, default `600`, for
`status.loadBalancer.ingress` to contain an IP or hostname before the attestation
step runs. Set the timeout to `0` only when another operator workflow has already
confirmed ingress readiness. `VELORIX_PRODUCT_INGRESS_WAIT_INTERVAL_SECONDS`
defaults to `5`.

After `scripts/run-vind-product.sh` has generated
`target/velorix-product/api-auth.env`, the attestation step calls the actual
externally reachable ingress endpoint:

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
VELORIX_INGRESS_ENDPOINT_URL=https://velorix.example.com \
VELORIX_INGRESS_CONTROLLER=nginx \
scripts/attest-vind-product-ingress.sh
```

`scripts/attest-vind-product-ingress.sh` reads the data-plane and admin bearer
tokens from `target/velorix-product/api-auth.env`, calls
`scripts/attest-ingress-tls-auth.sh`, and writes
`target/velorix-product/ingress-tls-auth-attestation.json`. The attach step then
validates the attestation, copies it beside `product-evidence.json` as
`ingress-tls-auth-attestation.json`, updates
`api.auth.ingress_tls_auth_attestation`, removes the ingress blocker from
`product_complete_blockers`, and refreshes `product-completion-report.json`:

The TLS/auth attestation helper waits up to
`VELORIX_INGRESS_TLS_AUTH_READY_TIMEOUT_SECONDS` seconds, default `120`, for the
endpoint to present a TLS certificate and for the missing-token probe to return
`401`. `VELORIX_INGRESS_TLS_AUTH_READY_INTERVAL_SECONDS` defaults to `5`. These
readiness waits cover normal LB, DNS, and certificate propagation delays; they do
not create DNS, certificates, TLS Secrets, or PVCs.

```bash
VELORIX_VIND_PRODUCT_DIR=target/velorix-product \
scripts/attach-vind-product-ingress.sh
```

The underlying helper can still be called directly when an operator wants to
provide tokens explicitly:

```bash
scripts/attest-ingress-tls-auth.sh \
  --endpoint https://velorix.example.com \
  --api-token "$VELORIX_API_BEARER_TOKEN" \
  --admin-token "$VELORIX_ADMIN_BEARER_TOKEN" \
  --ingress-controller nginx \
  --output target/velorix-product/ingress-tls-auth-attestation.json
```

The helpers' local certificate scratch directory defaults to
`target/velorix-product/scratch`; override it with
`VELORIX_LOCAL_SCRATCH_DIR` when you need a different local `target` location.

When running the full product script, the same helper is invoked automatically
after the local product smoke if `VELORIX_INGRESS_ENDPOINT_URL` is set and
`VELORIX_INGRESS_TLS_AUTH_ATTESTATION_FILE` is not already supplied:

```bash
VELORIX_INGRESS_ENDPOINT_URL=https://velorix.example.com \
VELORIX_INGRESS_CONTROLLER=nginx \
scripts/run-vind-product.sh
```

The full product script can also apply the Ingress resource during deployment
when the target cluster already has an ingress controller and TLS Secret:

```bash
VELORIX_PRODUCT_INGRESS_APPLY=1 \
VELORIX_PRODUCT_INGRESS_HOST=velorix.example.com \
VELORIX_PRODUCT_INGRESS_CLASS=nginx \
VELORIX_PRODUCT_INGRESS_TLS_SECRET=velorix-api-public-tls \
scripts/run-vind-product.sh
```

The helper rejects localhost and Kubernetes service DNS endpoints, captures the
served TLS certificate fingerprint and issuer, verifies missing/wrong
data-plane bearer tokens are rejected, verifies the data-plane token cannot use
the mutating admin route, verifies missing/wrong/data-plane tokens cannot read
the admin job catalog route, verifies the admin token can read that catalog
route, and verifies the correct data-plane token can read `/v1/openapi.json`.
It intentionally does not send the admin token to the mutating
`/v1/view-compile-deploy/run-once` route.

```json
{
  "schema_version": 1,
  "evidence_kind": "velorix_ingress_tls_auth_attestation",
  "endpoint_url": "https://velorix.example.com",
  "external_hostname": "velorix.example.com",
  "ingress_controller": "nginx",
  "transport_security": "public-tls",
  "tls_enabled": true,
  "tls_certificate_sha256": "sha256:...",
  "tls_certificate_issuer": "example-ca",
  "auth_enforced": true,
  "missing_token_rejected": true,
  "wrong_token_rejected": true,
  "admin_auth_separate": true,
  "admin_route_missing_token_rejected": true,
  "admin_route_wrong_token_rejected": true,
  "data_plane_token_rejected_on_admin_catalog_route": true,
  "admin_token_accepted_on_admin_route": true,
  "data_plane_token_rejected_on_admin_route": true,
  "attested_at": "2026-05-31T00:00:00Z",
  "attester": "operator"
}
```

The script validates this file only in bearer-token mode and records a
sanitized copy under `api.auth.ingress_tls_auth_attestation`. The raw
attestation is also copied beside `product-evidence.json` as
`ingress-tls-auth-attestation.json`, and release evidence copying preserves that
sibling file. This is external boundary evidence; it is intentionally separate
from the local HTTPS smoke.
The local smoke is recorded under `api.auth.local_tls_auth_smoke` with
`public_ingress_attestation=false`. It proves only that the deployed
`velorix-api` TLS listener and auth middleware work through a local
port-forwarded vind/vCluster Service. It does not prove public DNS, a public CA,
mTLS, ingress-controller behavior, or an externally reachable production
boundary. First-E2E product evidence requires this local smoke to be enabled,
passed, and accompanied by sibling `tls-auth-smoke.json`, while still requiring
`trusted_for_product_complete=false`. Neither form of ingress evidence makes
`product_complete=true` while fencing and multi-replica evidence are still
missing.
To remove the product-complete ingest-writer lifecycle blocker, let
`scripts/run-vind-product.sh` auto-generate the attestation from deployed
Kubernetes Jobs. `VELORIX_INGEST_WRITER_LIFECYCLE_ATTESTATION_FILE` is still
accepted as externally supplied advisory evidence and must follow this schema,
but it no longer clears `product_complete` by itself. The attestation is bound
to the current product deployment: `deployment_id` must match
`VELORIX_PRODUCT_DEPLOYMENT_ID`, which defaults to `<VELORIX_VIND_CLUSTER>/<current run id>`,
and `authority_store_id` must match the script's current
`object_store.authority_store_id`.

```json
{
  "schema_version": 1,
  "evidence_kind": "velorix_ingest_writer_lifecycle_attestation",
  "deployment_id": "velorix-product/20260531T000000Z-12345",
  "authority_store_id": "s3://external/velorix-product/product/20260531T000000Z-12345",
  "deployed_topology": "kubernetes_jobs",
  "pod_internal_append_completed": true,
  "multi_pod_overlap_conflict_rejected": true,
  "adjacent_append_succeeded": true,
  "crash_restart_reconstruction_checked": true,
  "leader_handoff_checked": false,
  "kubernetes_lease_handoff_checked": true,
  "lease_held_through_append_checked": true,
  "commit_guard_checked": true,
  "admission_commit_guard_bound_checked": true,
  "lease_loss_during_reservation_checked": true,
  "no_pvc_created_by_vind": true,
  "evidence_files": {
    "pod_internal_job": "velorix-ingest-writer-smoke-log.json",
    "overlap_job": "velorix-ingest-lifecycle-overlap-log.json",
    "adjacent_job": "velorix-ingest-lifecycle-adjacent-log.json",
    "restart_job": "velorix-ingest-lifecycle-restart-log.json",
    "lease_loss_job": "velorix-ingest-lifecycle-lease-loss-log.json",
    "handoff_probe_job": "velorix-ingest-lifecycle-handoff-log.json"
  },
  "attested_at": "2026-05-31T00:00:00Z",
  "attester": "operator"
}
```

The auto-generated path runs additional deployed ingest-writer Jobs after the
REST smoke: one cross-Pod overlap probe that must conflict, one adjacent append
probe that must succeed, one controlled crash/restart probe that first creates
an admission/index record without a batch object, then starts a fresh runtime
that reconstructs the active admission and completes the append, one lease-loss
during reservation probe, and a multi-Pod Kubernetes Lease handoff probe. The
lease-loss probe acquires a Kubernetes partition Lease, admits and materializes
the durable ingest admission record with the lease owner/epoch binding, releases
the Lease at the commit-guard `BeforeCommit` phase, verifies that no batch object
was published, proves the target admission rejects an overlapping reservation
before expiry, expires that exact orphan with an orphan-expiry decision, and
proves the expired original retry is rejected as `admission_expired`. The
handoff probe has owner A acquire a short lease and terminate without releasing
it, owner B acquire a higher epoch after expiry and complete a lease-guarded
append while still holding that same stream/partition lease after append, then a
stale owner A Pod attempt the same guarded append and get rejected before
writing. The final attestation is written only after those artifacts and the
no-PVC namespace and service-account permission checks pass.
The guarded append path binds the lease owner/epoch into the durable admission
record and enforces it again through an ingest commit guard before the batch
object is published. The evidence proves the deployed Kubernetes Job lifecycle
path; a broader controller deployment must keep using the guarded append
boundary rather than the raw diagnostic append command.
The script records a sanitized copy under
`ingest_writer.lifecycle_attestation` and marks whether the source is generated
or external. A stale attestation from a different deployment or object-store
authority is rejected. External lifecycle attestations remain useful for review
and release packaging, but product-complete requires the script-generated
Kubernetes Job evidence path. The attestation must also carry
`evidence_provenance` for the pod-internal append, overlap, adjacent,
crash/restart, lease-loss, and handoff Jobs, including each Job UID, Pod UID,
Pod name, configured container image, and observed container image ID. It must
also carry the `evidence_files` map shown above so those claims remain bound to
the job-log artifacts produced by the current run. The readiness validator also
requires those referenced job-log files to exist beside the attestation file, so
a copied JSON manifest without the actual run artifacts is rejected. The
`attested_at` value must be RFC3339, no more than 15 minutes in the future, and
no older than 24 hours at validation time, so a same-deployment attestation
cannot be reused indefinitely. The
local first-E2E wrapper
`scripts/run-first-e2e-readiness.sh` requires the same attestation before it
will emit a passing readiness report. With
`VELORIX_FIRST_E2E_RUN_PRODUCT=1`, the default source is the product slice's
freshly generated
`target/velorix-product/ingest-writer-lifecycle-attestation.json`. Without a
product run, or when `VELORIX_FIRST_E2E_INGEST_WRITER_LIFECYCLE_EVIDENCE` is
set explicitly, the wrapper uses the supplied standalone attestation path.
The script also checks local vCluster scheduling health before image work and
after image load. If the vCluster node reports disk, memory, PID pressure,
not-ready state, or a matching `NoSchedule`/`NoExecute` taint, the run exits as
a local environment blocker before producing product-complete evidence. Free
Docker/Colima/vCluster ephemeral storage or recreate the local vCluster and
rerun; do not add PVCs to bypass this no-PVC product path.
`/readyz` also reports `legacy_recovered_sql_views_allowed`; the product script
requires this value and `generic_query_enabled` to be `false` and injects
`VELORIX_ALLOW_LEGACY_RECOVERED_SQL_VIEWS=0` plus
`VELORIX_ENABLE_GENERIC_QUERY=0` into the API Deployment.
First-E2E product evidence also requires `api.auth.mode=bearer-token`,
`missing_token_rejected=true`, `wrong_token_rejected=true`,
`correct_token_smoke_passed=true`,
`data_plane_token_rejected_on_admin_route=true`,
`healthz_unauthenticated=true`, `readyz_unauthenticated=true`,
`deployment_env_verified=true`, and the expected `velorix-api-auth` plus
`velorix-admin-auth` Secret names. It also requires
`api.auth.local_tls_auth_smoke.passed=true` with sibling `tls-auth-smoke.json`.
This keeps the manual REST path from passing readiness through the local
unauthenticated-dev override or through an HTTP-only local service.
The same product evidence requires `no_pvc.namespace_validated=true`,
`no_pvc.evidence=no-pvc-namespace.json`, and a sibling
`no-pvc-namespace.json` file. That file is the Kubernetes namespace PVC list
captured by the product run, so the no-PVC decision is validated as live
cluster evidence instead of only a JSON flag.
The product evidence also requires `api.query_policy.catalog_smoke_passed=true`,
`api.query_policy.missing_policy_rejected=true`, and
`api.query_policy.linked_view_policy_id=interactive` when the product smoke is
included in first-E2E readiness evidence. The same product evidence now also
requires `api.compile_deploy.job_catalog_verified=true` with sibling
`view-compile-deploy-jobs.json`, proving that the no-artifact Feldera view
entered the durable compile/deploy queue with an embedded `compiler_request`
before a worker can activate it. It also requires
`api.compile_deploy.worker_run_verified=true`,
`api.compile_deploy.run_once_evidence_file=view-compile-deploy-run-once.json`,
`api.compile_deploy.activated_view_id=pending_scores_by_user`, and
`api.compile_deploy.activated_execution_mode=standing_runtime`, with sibling
`pending-scores-view-after-compile-deploy.json` and
`pending-scores-query-after-compile-deploy.json`, proving that the worker
actually promoted the pending view into a callable standing runtime. The same
product evidence also
records the OpenAPI catalog smoke under `api.openapi`: `openapi.json` must be
attached, `/v1/api/scores/positive` must be present, generic `/v1/query` and
the non-default parameterized scores path must be absent, the response schema
must be checked, and the OpenAPI operation must expose
`x-velorix-query-policy-id=interactive`.
The referenced OpenAPI, local TLS/auth, no-PVC namespace, query-policy,
compile/deploy job, product ingest-writer append, external S3 validation, and
lifecycle evidence files must exist beside `product-evidence.json`; first-E2E
readiness rejects a JSON-only manifest with missing sibling artifacts.
When first-E2E includes product evidence, it also requires the product slice's
own `ingest_writer.lifecycle_attestation` to be validated, generated by
`scripts/run-vind-product.sh`, trusted for product-complete, and populated with
the same per-Job `evidence_provenance` and `evidence_files` fields as the
standalone lifecycle attestation. The product evidence, its object-store authority, and the nested
lifecycle attestation must use the same `deployment_id` and `authority_store_id`
as the production GC evidence attached to the first-E2E readiness report. It also
requires external S3 validation to be enabled and proven for the same bucket and
prefix, with `object_store.external_s3_prefix_validated=true` and attached
`external-s3-validate` Job/log evidence.
When `VELORIX_FIRST_E2E_RUN_PRODUCT=1`, the wrapper reads that production GC
artifact before running the product slice and injects the matching
`VELORIX_PRODUCT_DEPLOYMENT_ID`, `VELORIX_AUTHORITY_STORE_ID`,
`VELORIX_OBJECT_STORE_MODE`, `VELORIX_S3_BUCKET`, and `VELORIX_S3_PREFIX` into
`scripts/run-vind-product.sh`. It runs the product slice in external
S3-compatible mode so the product deployment talks to the same backend
authority that produced the production GC evidence instead of replacing it with
a fresh in-cluster RustFS Pod. When the wrapper itself starts the RustFS S3
gate and product evidence is enabled, it keeps that RustFS container, network,
and volume alive through the product run, then performs the requested RustFS
cleanup afterward. The wrapper forwards `VELORIX_LOCAL_DISK_PREFLIGHT` and the
resolved free-disk floor to the nested product run, so an intentional
`VELORIX_FIRST_E2E_MIN_FREE_DISK_GIB` override does not get reset to the product
script's default. The wrapper also reuses the already built
`VELORIX_INGEST_WRITER_IMAGE` for the product slice by setting
`VELORIX_BUILD_INGEST_WRITER_IMAGE=0` and `VELORIX_LOAD_EXISTING_IMAGES=1`.
When `VELORIX_FIRST_E2E_SKIP_DOCKER_BUILD=1` is combined with
`VELORIX_FIRST_E2E_RUN_PRODUCT=1`, set `VELORIX_API_IMAGE` and
`VELORIX_META_IMAGE` to existing local images; the wrapper validates them and
loads them into the product vCluster. If the product slice should also deploy
the managed no-PVC Hiqlite authority, set `VELORIX_HIQLITE_DEPLOY=1` and
`VELORIX_HIQLITE_IMAGE` to an existing local image; the wrapper validates that
image, sets `VELORIX_BUILD_HIQLITE_IMAGE=0`, and passes the image through to
`scripts/run-vind-product.sh`. For `s3://rustfs/...` evidence, set
`VELORIX_FIRST_E2E_PRODUCT_AWS_ENDPOINT_URL` if the default
`http://host.docker.internal:${VELORIX_RUSTFS_PORT:-9000}` is not reachable
from the vCluster. The wrapper also sets `VELORIX_API_HOLD_PORT_FORWARD=0` so
the product slice returns control to the readiness report instead of waiting in
manual REST-inspection mode. A `s3://external/...` authority requires the
external S3 endpoint and credentials in the environment before the product slice
is run.
When a metadata service is configured, relation catalog reads, ingest admission,
and the development-only generic recovered query path use metadata as the
catalog authority when that path is explicitly enabled.
Velorix still best-effort materializes the relation catalog to object storage as
cache/evidence for compatibility, but failure to write that copy does not make
the metadata-backed catalog create fail.
Override `VELORIX_META_BACKEND=oss` to use the configured object store metadata
backend under `VELORIX_META_S3_PREFIX` (default:
`${VELORIX_S3_PREFIX}/meta`), or set `VELORIX_META_ENABLED=0` to run the older
single-process local path. Hiqlite mode can use an existing endpoint set through
`VELORIX_HIQLITE_NODES` and `VELORIX_HIQLITE_API_SECRET`; in this external reuse
mode, `scripts/run-vind-product.sh` creates only the product-owned
`velorix-meta-hiqlite-auth` client Secret for the metadata service and does not
mutate `velorix-hiqlite` authority Services, Secrets, or StatefulSets. Set
`VELORIX_HIQLITE_DEPLOY=1` to deploy a no-PVC three-voter Hiqlite authority
inside the vind namespace. The managed authority is backed by a StatefulSet,
headless Service, per-pod `emptyDir`, and Hiqlite S3 backup/restore settings;
the script auto-generates `VELORIX_HIQLITE_NODES`, API/Raft secrets, encryption
keys, and `metadata_store.hiqlite_authority_attestation` evidence unless those
values are supplied. The no-PVC validation reads the managed StatefulSet back
from Kubernetes, rejects `volumeClaimTemplates`, rejects PVC volume mounts,
requires `emptyDir` for Hiqlite node data, requires voters not to run with
`HQL_LEARNER_ONLY=true`, and checks the `velorix-hiqlite` ServiceAccount cannot
create PVCs or read Kubernetes Secrets. Set
`VELORIX_HIQLITE_AUTHORITY_ATTESTATION_FILE` when an
externally operated three-voter authority should be attested instead. The
attestation confirms no PVC was created by this script, confirms the metadata
authority itself does not use PVC, confirms voters are not learner-only, and
records API/raft auth, transport security, backup/restore, storage mode, and
image/source provenance. The script validates the file, checks that its `nodes`
match `VELORIX_HIQLITE_NODES`, and records a sanitized copy under
`metadata_store.hiqlite_authority_attestation` in `product-evidence.json`.
The raw authority attestation is preserved beside `product-evidence.json` as
`hiqlite-authority-attestation.json`; managed Hiqlite, metadata, and API
Services use stable selectors so failed reruns cannot leave long-lived Services
pointing at a new run-id with no endpoints. For the managed no-PVC authority, release
evidence also checks the attestation's `namespace_pvc_list` pointer against
`no-pvc-namespace.json` and preserves `no-pvc-hiqlite-statefulset.json` plus
`velorix-hiqlite.yaml`. It also validates that the Velorix product namespace contains no
`PersistentVolumeClaim` objects and records the result under `no_pvc` with
`no-pvc-namespace.json` as the required sibling evidence. The
current Hiqlite metadata backend is accepted for
`VELORIX_STANDING_RUNTIME_FENCING=required` when it is built against the pinned
Hiqlite Raft-serialized timestamp API and `/readyz` reports
`backend_time_source_kind=raft_replicated_authority_time`,
`bounded_wall_clock_failover=true`, and
`production_bounded_failover_safe=true`. The release validator still rejects
weak substitutes such as metrics, Raft log index, or distributed-lock TTL. It
also parses `hiqlite-backend-time-attestation.json` and rejects mismatches with
the product-evidence summary, but remains fail-closed for product-complete until
the attestation is generated from deployed adversarial product smoke and
validated end to end. The static assessment below is diagnostic only.
To re-check the currently pinned package instead of relying on a stale design
note, run:

```bash
scripts/assess-hiqlite-backend-time.sh target/velorix-hiqlite-backend-time-assessment
```

The helper writes `hiqlite-backend-time-assessment.json` with the exact local
`hiqlite` source path, observed APIs, rejected substitutes, and the current
authority-time support verdict. When `VELORIX_META_BACKEND=hiqlite`,
`scripts/run-vind-product.sh` runs this assessment by default with
`VELORIX_HIQLITE_BACKEND_TIME_ASSESS=auto` and stores the diagnostic result in
`metadata_store.hiqlite_backend_time_assessment` in `product-evidence.json`.
That field is deliberately not a release attestation and is not trusted for
`product_complete`; it records whether the local package can generate a
product-complete backend-time attestation. Set
`VELORIX_HIQLITE_BACKEND_TIME_ASSESS=0` to skip the local diagnostic, or
`VELORIX_REQUIRE_HIQLITE_BACKEND_TIME=1` when a CI or release probe should fail
until the package exposes a usable backend-authoritative wall-clock lease
primitive.

After a deployed vind product run has produced `product-evidence.json`,
`readyz.json`, `multi-replica-fencing-smoke.json`,
`standing-runtime-failover-smoke.json`, `velorix-meta-smoke.log`, and the
static package assessment, you can generate a backend-time attestation
candidate:

```bash
scripts/attest-hiqlite-backend-time.sh \
  --product-evidence target/velorix-product/product-evidence.json
```

The helper writes sibling `hiqlite-backend-time-attestation.json`, records
sha256 provenance for the deployed smoke inputs, binds
`observed_max_failover_ms` to the measured API-pod failover smoke, and can
attach the summary expected by the release validator when
`VELORIX_HIQLITE_BACKEND_TIME_ATTESTATION_UPDATE_PRODUCT_EVIDENCE=1` or
`--update-product-evidence` is set. The default candidate is diagnostic: it is
useful to inspect the evidence shape and remove static assessment ambiguity,
but it is not itself a product-complete pass.
When the product-completion report already sees a diagnostic
`metadata_store.hiqlite_backend_time_attestation`, rerunning the local helper
without trusted release inputs will not change that gate. To pass the gate,
first write the release env template and then run the release-input preflight:

```bash
scripts/write-hiqlite-backend-time-release-env.sh \
  --product-evidence target/velorix-product/product-evidence.json

scripts/check-hiqlite-backend-time-release-inputs.sh \
  --env-file target/velorix-product/hiqlite-backend-time-release.env \
  --product-evidence target/velorix-product/product-evidence.json
```

The env helper writes `hiqlite-backend-time-release.env` and
`hiqlite-backend-time-release-env.json`. It fills product-evidence-derived
values such as `VELORIX_API_IMAGE_DIGEST`, `VELORIX_META_IMAGE_DIGEST`, and
`VELORIX_HIQLITE_IMAGE_DIGEST`, but leaves CI/Sigstore values as explicit
`REPLACE_WITH_*` placeholders. Pass it to
`scripts/check-hiqlite-backend-time-release-inputs.sh --env-file` from trusted
release CI after replacing every placeholder; the preflight reads the env file
as defaults, so explicit release CI environment variables take precedence over
values exported by the file, without creating provenance or bypassing validation.
The top-level completion driver preserves an existing
`hiqlite-backend-time-release.env` so filled release values are not overwritten;
set `VELORIX_HIQLITE_BACKEND_TIME_RELEASE_ENV_FORCE=1` only when you intentionally
want to regenerate that template from current product evidence.
`VELORIX_SOURCE_REVISION`
and `VELORIX_RELEASE_COMMIT` are Velorix
product repository commits; the helper may fill them from explicit
`VELORIX_*` env values or GitHub Actions `GITHUB_SHA`, but never from the
Hiqlite authority source revision recorded in product evidence.
The release preflight also rejects a 40-character Hiqlite authority revision if
it is supplied as either Velorix release commit field, and the attestation
generator enforces the same boundary before writing trusted provenance.
The preflight writes `hiqlite-backend-time-release-preflight.json`, checks the
deployed smoke evidence bundle and release CI environment, and fails closed with
all missing inputs instead of discovering them one at a time during attestation
generation. It also rejects any release env value that still contains a
`REPLACE_WITH_*` placeholder. After that passes, the attestation must be regenerated in release CI
with
`VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1`, deployed image digests,
trusted release wall-clock failover evidence, a clean source revision, and the
required signing or sigstore provenance.
The `product_evidence` entry in that canonical evidence bundle is normalized
by removing `metadata_store.hiqlite_backend_time_attestation` before hashing.
That avoids a circular self-reference when the same helper later copies the
attestation summary back into `product-evidence.json`; all other referenced
evidence files are hashed as raw bytes.
Set
`VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1` only from a trusted release
or CI job that also provides `VELORIX_SOURCE_REVISION`,
`VELORIX_RELEASE_COMMIT`, `VELORIX_CI_WORKFLOW_NAME`,
`VELORIX_CI_WORKFLOW_RUN_ID`, `VELORIX_CI_JOB_NAME`,
`VELORIX_API_IMAGE_DIGEST`, `VELORIX_META_IMAGE_DIGEST`, and
`VELORIX_HIQLITE_IMAGE_DIGEST` matching the managed Hiqlite authority image.
`VELORIX_SOURCE_REPOSITORY` must be exactly `github.com/mrchypark/velorix`;
the release env template and release-gate workflow set it explicitly, and both
preflight and attestation generation reject any other repository value.
The preflight also checks that `VELORIX_CI_WORKFLOW_REF` uses the trusted
`mrchypark/velorix/.github/workflows/release-gate.yml@refs/heads/main` or
`@refs/tags/v*` form, that `VELORIX_CI_OIDC_SUBJECT` matches that ref, that
`VELORIX_CI_JOB_WORKFLOW_REF` is pinned to `VELORIX_RELEASE_COMMIT`, and that
`VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY` names the same trusted workflow ref.
`scripts/run-vind-product.sh` records the deployed `velorix-api` and
`velorix-meta` images under `deployed_images`, including Deployment and Pod
siblings. Release validation requires the trusted backend-time
`subject_images` digests for `velorix-api` and `velorix-meta` to match that
deployed image evidence; the `hiqlite-authority` digest must still match the
Hiqlite authority attestation.
The same trusted job must also provide OIDC/signature-bundle fields:
`VELORIX_CI_OIDC_SUBJECT`, `VELORIX_CI_WORKFLOW_REF`,
`VELORIX_CI_JOB_WORKFLOW_REF`, `VELORIX_CI_SIGSTORE_BUNDLE_BASE64`,
`VELORIX_CI_SIGSTORE_BUNDLE_SHA256`, and
`VELORIX_CI_SIGSTORE_CERTIFICATE_IDENTITY`.
`VELORIX_CI_SIGSTORE_BUNDLE_SHA256` uses the same `sha256:<64 hex>` format as
the attestation and release-gate workflow, and the preflight checks that it
matches the decoded `VELORIX_CI_SIGSTORE_BUNDLE_BASE64` bytes. The SHA value is
required whenever trusted Sigstore provenance is used; it is not silently
derived during attestation. The preflight also checks that the decoded Sigstore
bundle contains verification material, a signing certificate, and Rekor
transparency-log evidence before the attestation helper derives certificate,
Rekor log, inclusion-proof, and integrated-time metadata. Trusted
product-complete provenance must come from
`refs/heads/main` or a `refs/tags/v*` release tag; feature-branch runs remain
diagnostic and fail closed for product-complete readiness. Legacy Ed25519 fields remain supported for fail-closed
diagnostics, but they are not sufficient for product-complete release readiness.
`VELORIX_SOURCE_REVISION` and `VELORIX_RELEASE_COMMIT` must be the same full
40-character clean Velorix commit SHA. In that mode the helper records
`velorix_ci_evidence_bundle_provenance`, `subject_images` for `velorix-api`,
`velorix-meta`, and `hiqlite-authority`, GitHub Actions `ci_identity`, a
Sigstore/Rekor-style `signature_bundle` with a real Sigstore bundle over the
canonical evidence bundle digest,
`canonical_bundle_sha256`, and `canonical_bundle_entries` over the deployed
smoke evidence bundle. Trusted
provenance mode also requires the failover evidence itself to be release-scoped:
`trusted_for_product_complete=true`,
`production_wall_clock_failover_attestation=true`,
`evidence_scope=release_ci_deployed_product`, and
`failover_probe_kind=release_bounded_wall_clock_failover`. The default local
`scripts/smoke-vind-standing-runtime-failover.sh` output remains diagnostic and
cannot clear product-complete; trusted release CI must set
`VELORIX_STANDING_RUNTIME_FAILOVER_RELEASE_ATTEST=1` and rerun the real failover
probe before backend-time attestation.
When release-shaped failover evidence is present without trusted CI provenance,
`scripts/attest-hiqlite-backend-time.sh` may include it in the diagnostic
attestation, but records
`attestation_origin=diagnostic_deployed_product`,
`failover_evidence_shape=release_scoped`, and
`diagnostic_release_failover_included=true`; the backend-time attestation still
keeps `trusted_for_release_validator=false` and
`release_validator_fail_closed=true` until the Sigstore-backed release
provenance is supplied.
Release validation also rejects stale backend-time attestations and unknown
attesters, and deserializes the full standing-runtime fencing capability schema
with unknown fields rejected before accepting the summary. The accepted
diagnostic attesters are the product script (`scripts/run-vind-product.sh`),
`velorix-release-operator`, and `velorix-ci`; the `attested_at` timestamp must
be fresh within the same 24-hour window used for other live release
attestations.
When the product run uses `VELORIX_META_BACKEND=hiqlite` and
`VELORIX_STANDING_RUNTIME_FENCING=required`,
`scripts/run-vind-product.sh` runs this helper automatically after the static
assessment, metadata adversarial smoke, multi-replica fencing smoke, and local
API-pod failover smoke have passed. Set
`VELORIX_HIQLITE_BACKEND_TIME_ATTEST=0` to skip it or `=1` to require it. The
product evidence records
`metadata_store.hiqlite_backend_time_attestation`, but keeps a product-complete
blocker while the attestation is marked
`trusted_for_release_validator=false`.

This script is local vind/vCluster-only. Do not point its kube context or
default unsafe single-replica settings at a shared or production Kubernetes
cluster. The current `memory` metadata backend is ephemeral; restarting
`velorix-meta` loses its control-plane metadata. The `oss` backend is useful for
catalog/admission durability checks; in that mode the product smoke restarts
`velorix-meta` and then `velorix-api` before re-querying the generated view. It
is still not a production standing-runtime multi-writer fencing authority.
`VELORIX_VIND_PRESERVE_STATE=1` preserves the configured S3 key prefix across
runs. In the default RustFS mode this is developer convenience, not durable
product evidence, because the RustFS Deployment still uses `emptyDir`.
External S3-compatible mode preserves data according to the external object
store's own durability and retention configuration.

Set `VELORIX_VIND_CLEANUP=1` if you want the script-created vCluster deleted
after the script exits. The default keeps the cluster and the port-forward so
manual REST testing can continue.
