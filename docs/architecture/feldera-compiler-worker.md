# Feldera Compiler Worker Split

Velorix must reuse Feldera's public Rust packages for standing-view semantics,
but the product API image must not become a Feldera all-in-one distribution and
the product backend must not ship Feldera's SQL compiler jar. The official
`images.feldera.com/feldera/pipeline-manager:latest` image is valid as a live
compatibility fixture, not as the default Velorix runtime image: it carries the
Feldera compiler, runtime, build toolchains, demos, and precompile cache in one
large image.

The product split is:

- `velorix-api`: admits relations, ingest, views, promoted APIs, queries, and
  standing-runtime ownership. It remains a lean API/runtime image and does not
  run Cargo, Java/Maven, or dynamically load untrusted generated Rust.
- `velorix-feldera-compiler-worker`: an optional control-plane service or
  Kubernetes Job that owns Feldera compilation outside the API process. It reads
  pending compile/deploy jobs from Velorix admin APIs, calls a Feldera compiler
  backend, resolves output schemas, writes/validates artifact metadata, and
  completes the job through Velorix.
- Feldera compiler/runtime backend: an implementation detail behind the worker.
  The product direction is a jarless Velorix-owned backend assembled from
  Feldera Rust packages such as DBSP, `feldera-types`, and `feldera-sqllib`.
  The upstream pipeline-manager path depends on the SQL compiler jar and is
  therefore an explicit compatibility fixture only, not the default backend
  image or product completion target.

## Control-Plane Protocol

The worker talks to `velorix-api`; it does not write Velorix metadata or object
store records directly. The API remains the single admission and activation
authority.

1. User creates relations with `POST /v1/relations`.
2. User ingests data with `POST /v1/ingest` or `POST /v1/ingest/epoch`.
3. User creates a view with `POST /v1/views`.
4. If no trusted linked runtime already matches the view, `velorix-api` stores a
   pending compile/deploy job.
5. The worker polls `GET /v1/view-compile-deploy/jobs` with admin auth.
6. The worker claims one job with
   `POST /v1/view-compile-deploy/jobs/{view_id}/claim`. The response includes
   `tenant_id`, `view_id`, `job_generation`, `compile_request_hash`,
   `lease_id`, `fencing_token`, `worker_id`, `claimed_at_ms`, and
   `lease_expires_at_ms`.
7. The worker submits the job's Feldera SQL and catalog-owned input schemas to
   its Feldera backend.
8. The worker captures the compiler output schema. If it produced a legacy
   static/generated artifact, it captures `artifact` metadata. If it produced a
   jarless package-backed product runtime, it captures a `product_runtime`
   descriptor with the Feldera package backend identity, runtime factory
   binding, standing-program identity, state codec, and schema contract. If it
   deployed an externally managed Feldera pipeline-manager runtime, it captures
   a `runtime_deployment` binding with the pipeline name and deployment mode
   instead.
9. The worker completes activation with
   `POST /v1/view-compile-deploy/jobs/{view_id}/complete`, echoing `worker_id`,
   `tenant_id`, `job_generation`, `lease_id`, and `fencing_token` when the job
   was claimed.
10. `velorix-api` revalidates the active pending job, `compile_request_hash`,
    active claim proof, optional resolved spec, exactly one completion payload
    (`artifact`, `product_runtime`, or `runtime_deployment`), relation
    fingerprints, schema shape flags, ABI/state codec when present, and runtime
    factory binding before the view becomes queryable.

Completing through REST is intentional. It keeps all fail-closed validation in
the same process that serves user APIs, prevents a worker from bypassing view
lifecycle invariants, and makes the worker replaceable. A future gRPC admin
surface can mirror this contract, but it should not change ownership.

The worker is fail-closed when the selected backend cannot compile. Its default
backend is `feldera-package-jarless`, which is the product direction and runs
in-process against public Feldera Rust package descriptor APIs. It does not
infer output schemas from SQL. When the pending request uses
`output_contract=must_match`, the worker validates the declared input/output
schemas through Feldera package descriptor types and reports
`compiled_schema_only_not_deployed`; the view remains not queryable. When the
pending request uses `output_contract=infer`, the worker reports
`unsupported_by_selected_backend` with `requires_java_sql_compiler=true`.

Use `--claim-without-backend` only for a diagnostic claim/lease/fencing pass
with a deliberately unconfigured compatibility backend; that mode claims
pending jobs, emits a JSON report, and stops with `claimed_not_compiled`.

The pipeline-manager path is available only as an explicit compatibility
backend: set `--compiler-backend compatibility-pipeline-manager` and
`VELORIX_FELDERA_PIPELINE_MANAGER_URL`. That path uses the claimed job's
compiler request plus API-supplied input relation catalogs, strips Velorix
ingest weight columns for Feldera table DDL, submits the program to
pipeline-manager, resolves `program_info.schema.outputs`, and completes the job
through `/complete` with `runtime_deployment`. It is useful for compatibility
checks, but because it relies on the upstream SQL compiler jar it is not product
completion evidence.

Run a claim-only pass with:

```bash
VELORIX_API_URL="$VELORIX_API_URL" \
VELORIX_ADMIN_AUTH_HEADER="$VELORIX_ADMIN_AUTH_HEADER" \
VELORIX_FELDERA_COMPILER_WORKER_ID="compiler-worker-a" \
VELORIX_FELDERA_COMPILER_BACKEND="compatibility-pipeline-manager" \
VELORIX_FELDERA_COMPILER_CLAIM_WITHOUT_BACKEND=1 \
velorix-feldera-compiler-worker once
```

The expected diagnostic status is `claimed_not_compiled`. This is not a
product-complete dynamic view execution result; it proves the worker is using
the external claim/lease/fencing API instead of the older in-process
`/v1/view-compile-deploy/run-once` path.

Run a pipeline-manager backed pass with:

```bash
VELORIX_API_URL="$VELORIX_API_URL" \
VELORIX_ADMIN_AUTH_HEADER="$VELORIX_ADMIN_AUTH_HEADER" \
VELORIX_FELDERA_COMPILER_WORKER_ID="compiler-worker-a" \
VELORIX_FELDERA_COMPILER_BACKEND="compatibility-pipeline-manager" \
VELORIX_FELDERA_PIPELINE_MANAGER_URL="http://127.0.0.1:18082" \
velorix-feldera-compiler-worker once
```

The expected compatibility-fixture status is
`completed_compatibility_runtime_deployment`. The worker still does not write
Velorix metadata directly and does not call `/v1/view-compile-deploy/run-once`;
Velorix API performs the final validation and activation. This status is not a
jarless product-runtime completion result.

Worker backend outcomes are intentionally split:

- `unsupported_by_selected_backend`: the selected backend cannot compile or run
  the request. For the current `feldera-package-jarless` backend, this remains
  the correct fail-closed result for SQL families that still require Feldera's
  Java SQL compiler.
- `compiled_schema_only_not_deployed`: the jarless package backend validated a
  declared descriptor/schema contract but did not produce an executable product
  runtime. This must not enable queries.
- `completed_product_runtime_deployment`: a future jarless package-backed
  backend completed the job with a product runtime artifact.
- `completed_compatibility_runtime_deployment`: the explicit pipeline-manager
  compatibility backend completed a runtime deployment. This is compatibility
  evidence only.

## Image Strategy

The official Feldera pipeline-manager image is too broad for the default
Velorix product image because it bundles concerns that belong to different
planes. Velorix should provide at least two image shapes:

- `velorix-api`: the lean product serving image. It includes Velorix runtime
  code and trusted linked packages only. It does not include the Feldera Java
  compiler, Rust toolchain, Go toolchain, demos, or generated pipeline build
  cache.
- `velorix-feldera-compiler-worker`: the heavy optional compiler image. It may
  include a jarless Feldera package backend and only the runtime/build tooling
  needed to produce the artifact metadata and runtime binding supported by
  Velorix. It must not include the Feldera SQL compiler jar as the product
  path.

`Dockerfile.feldera-compiler-worker` currently builds the lean control-plane
worker binary only. It deliberately does not inherit from
`images.feldera.com/feldera/pipeline-manager:latest` and does not add a SQL
compiler jar. A future Velorix-owned Feldera backend image must keep the same
jarless product rule: use Feldera Rust packages and DBSP runtime APIs directly,
or fail closed if the requested SQL family still requires Feldera's Java SQL
compiler.

The live compatibility runner has no default pipeline-manager image. Using
`images.feldera.com/feldera/pipeline-manager:*` requires the explicit
`VELORIX_LIVE_FELDERA_ALLOW_OFFICIAL_IMAGE=1` opt-in and is not product
serving-image evidence.

An all-in-one developer image may still exist for local demos, but it must not
be the passing condition for product completion. Product completion requires the
split images to be selectable and the API image to run without the compiler
toolchain.

## Cache And Storage

PVC remains out of scope. The compiler worker may use:

- ephemeral `emptyDir` or container-local cache for local and CI runs;
- repository-local `target/` caches for developer scripts;
- object-store backed cache or release artifact storage when that becomes part
  of a production authority.

The worker must tolerate cache loss. Cache improves compile latency but cannot
be the only durable source of an active view. Durable authority remains the
Velorix relation catalog, view registry, pending compile job, accepted artifact
metadata, and checkpoint/object-store manifests.

## Security And Fencing

The compiler worker requires admin credentials, but it should not receive broad
object-store or metadata write authority unless a later implementation proves it
needs those rights. The minimum worker authority is:

- read pending compile/deploy jobs;
- call Feldera compiler/runtime backend;
- complete one job with a resolved spec and artifact metadata.

Activation must remain idempotent and compare the worker result with the active
pending job. If a view was replaced, deleted, or its compile request hash
changed while compilation was running, the completion request must fail closed.

Runtime ownership remains separate from compile ownership. A compiler worker
that produced an artifact is not automatically allowed to own standing runtime
ingest or query execution. Runtime owners still use the existing
standing-runtime owner protocol.

The current REST claim path provides the first lease/fencing slice for external
workers. A claimed job must complete with the same `tenant_id`,
`job_generation`, `worker_id`, `lease_id`, and `fencing_token`; otherwise
activation fails closed. Before parallel workers are considered production
complete, this must be extended with cancellation, explicit job generation
increment on view replacement, canonical compiler request identity, and
idempotent duplicate-completion semantics.

Worker credentials must be scoped to compile-job read/claim/complete actions.
User API credentials must not call `/v1/view-compile-deploy/*`, and production
profiles should keep imperative `run-once` behind a separate operational role
or disable it.

Pipeline-manager backed runtime completion also needs an explicit durability
class. With no PVC and without an external durable Feldera state authority, a
pipeline-manager runtime is local/transient evidence. On restart, Velorix must
reconcile every active pipeline-manager backed view against the actual Feldera
pipeline and fail closed as runtime-unavailable if the backing pipeline cannot
be recovered.

## Acceptance Gates

The split is not complete until these checks pass:

- `velorix-api` can start and serve relation/ingest/view/query APIs without a
  Feldera compiler URL or compiler toolchain.
- Creating an unsupported no-artifact view records a pending
  `feldera_compile_pending` job instead of running a local hardcoded parser.
- A separate worker process completes a pending job through
  `/v1/view-compile-deploy/jobs/{view_id}/complete`; direct metadata writes are
  not required. The completion request must include exactly one of `artifact`, `product_runtime`, or `runtime_deployment`. `product_runtime` is the jarless Feldera package runtime descriptor; `runtime_deployment` remains compatibility-only pipeline-manager evidence.
- At least projection/filter, grouped aggregates, aggregate variants, and a
  two-relation join are verified through the same jarless package-backed worker
  path. Pipeline-manager fixture evidence can be retained as compatibility
  evidence, but it does not satisfy this product gate.
- Unsupported SQL returns a Feldera/compiler admission error and does not fall
  back to a Velorix fake generic implementation.
- The default product deployment creates no PVCs.
- The official Feldera all-in-one image is documented as a development/live
  compatibility fixture, not as the default product serving image.
- Multi-worker production mode has claim, lease, fencing token, generation,
  tenant-scoped admin auth, bounded retries, compile resource limits, and orphan
  pipeline cleanup.
- Product query routes do not expose arbitrary caller SQL unless output
  authorization is enforced by Feldera or by a compiler-backed dependency
  admission flow. Promoted APIs should use server-owned templates or
  relation-scoped reads.
