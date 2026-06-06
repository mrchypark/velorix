# Hiqlite Meta Service Direction

Velorix should use upstream `sebadob/hiqlite` main as soon as that branch
contains the Raft-serialized timestamp API needed by the metadata backend. Until then,
Velorix uses a pinned `mrchypark/hiqlite` fork commit. When an upstream release
includes that support, Velorix should switch from the fork commit to the
released package or pinned release source.

## Authority Split

The product target is no longer object-store-only metadata:

- A dedicated hiqlite write cluster owns hot control-plane metadata.
- Object storage owns large artifacts, checkpoint payloads, and hiqlite
  backup/restore evidence.
- Foyer remains a non-authoritative local cache only.

The write cluster is a fixed three-pod Raft voter set. Query-engine pods may run
a hiqlite learner sidecar for local reads, but those sidecars must not become
voters and must not accept Velorix metadata writes.

## First Meta API Slice

`crates/velorix-meta` defines the first typed gRPC boundary:

- `StoreRelationCatalog`
- `ReadRelationCatalog`
- `ReserveIngestRange`

The current implementation has:

- an in-memory backend for contract tests and local API work
- a `GrpcMetaStore` client used by `velorix-api`
- an `OssMetaStore` backend for standalone or low-cost deployments
- a `HiqliteMetaStore` backend behind the `hiqlite-backend` feature, pinned to
  `mrchypark/hiqlite@b1dbcb3572558ac1fc09cc1eac080a5578600452` until upstream
  main/release exposes the required API

The in-memory backend is not durable and must not be used as the production
meta authority.
The object-store backend is durable and simple, but it keeps metadata on the
same object-store primitives as the existing replay path, so it is best for
small deployments, standalone E2E, and cost-optimized operation where a
dedicated hiqlite cluster is not justified. Hiqlite remains the preferred
scale-out backend for hot metadata. The current `HiqliteMetaStore`
implementation advertises production-safe standing-runtime fencing when built
against the pinned Hiqlite Raft-serialized timestamp API: standing-runtime owner
expiry and checkpoint publish validation run inside
`txn_with_raft_serialized_timestamp`, and lease SQL binds
`Param::raft_serialized_unix_ms()` from
the same Raft write operation. With that package contract,
`authoritative_backend_time=true`,
`backend_time_source_kind=raft_replicated_authority_time`,
`lease_authority_kind=raft_replicated_time`,
`lease_expiry_semantics=backend_wall_clock_ttl`,
`multi_writer_fencing_safe=true`, `bounded_wall_clock_failover=true`, and
`production_multi_writer_safe=true`.
Standing-runtime owner and checkpoint reads use Hiqlite consistent queries, so
the fencing contract no longer depends on stale standing-runtime pointer reads.
Do not satisfy this gate by relabeling a Raft log index, metrics timestamp, or
distributed-lock TTL as wall-clock time. The accepted primitive is specifically
a Raft command carrying an authority-sampled Unix timestamp and a transaction
parameter that binds that timestamp into the owner-expiry and checkpoint-publish
SQL mutation. Raft metrics, log indexes, and distributed-lock TTLs remain
rejected substitutes because they do not prove elapsed wall-clock failover for a
quiet or partitioned standing runtime.
The separate
`logical-fencing` profile accepts the operation-driven logical lease semantics
for multi-replica functional execution, but it records
`bounded_wall_clock_failover=false` and does not clear product-complete.
The product smoke for this profile must prove the logical fencing path against
the deployed metadata service: stale owner tokens are rejected after a higher
epoch owner is acquired, checkpoint publication remains owner-validated, and
the metadata-published latest checkpoint pointer stays authoritative.

For local contract work, the gRPC service can be built with:

```bash
cargo run -p velorix-meta
```

Run the durable backend with:

```bash
VELORIX_META_BACKEND=hiqlite \
VELORIX_HIQLITE_NODES=velorix-meta-0:8200,velorix-meta-1:8200,velorix-meta-2:8200 \
VELORIX_HIQLITE_API_SECRET="$HQL_SECRET_API" \
cargo run -p velorix-meta --features hiqlite-backend
```

`VELORIX_HIQLITE_WITH_PROXY=1` enables hiqlite remote proxy mode when the
deployment requires it.
This backend can already be used for catalog/admission durability work, but it
must not be used to satisfy `VELORIX_STANDING_RUNTIME_FENCING=required` until
backend-time lease semantics are implemented and verified.

For no-PVC product evidence, the vind product script can either deploy a
Velorix-managed three-voter Hiqlite authority with `VELORIX_HIQLITE_DEPLOY=1`
or record an externally operated Hiqlite authority with
`VELORIX_HIQLITE_AUTHORITY_ATTESTATION_FILE`. The managed vind authority uses a
StatefulSet, headless Service, `emptyDir` node disks, Hiqlite S3
backup/restore settings, a locked-down `velorix-hiqlite` ServiceAccount, and no
PVCs. The product runner validates that the StatefulSet has no
`volumeClaimTemplates`, mounts no PVC volumes, keeps voters out of learner-only
mode, and that the Hiqlite ServiceAccount cannot create PVCs or read Kubernetes
Secrets. This is evidence about the authority shape, not proof of backend-time
fencing. The file must use this schema:

```json
{
  "schema_version": 1,
  "authority_kind": "external_hiqlite",
  "nodes": [
    "http://velorix-meta-0:8100",
    "http://velorix-meta-1:8100",
    "http://velorix-meta-2:8100"
  ],
  "expected_voter_count": 3,
  "no_pvc_created_by_vind": true,
  "metadata_authority_no_pvc_used": true,
  "metadata_authority_storage_mode": "object-store-backup-restore-with-ephemeral-node-disk",
  "voters_learner_only_disabled": true,
  "api_auth_configured": true,
  "raft_auth_configured": true,
  "transport_security": "service-mesh-mtls",
  "backup_restore_configured": true,
  "image_digest": "sha256:...",
  "source_revision": "<hiqlite-authority-source>@<authority-time-revision>",
  "attested_at": "2026-05-31T00:00:00Z",
  "attester": "operator"
}
```

The script validates required fields, requires exactly three unique voter
endpoints, verifies that `nodes` matches `VELORIX_HIQLITE_NODES`, requires the
attested authority itself to be no-PVC, requires a SHA-256 image digest for
managed Hiqlite authority evidence, and copies a sanitized version into
`product-evidence.json`. For
`VELORIX_HIQLITE_DEPLOY=1`, it generates the same attestation with
`authority_kind=velorix_managed_hiqlite` after the StatefulSet rolls out.
Product-complete still remains false until `authoritative_backend_time=true`,
`bounded_wall_clock_failover=true`, and multi-replica adversarial fencing
evidence exist.
Release validation recognizes a future
`metadata_store.hiqlite_backend_time_attestation` with sibling
`hiqlite-backend-time-attestation.json`, parses that sibling evidence, and
checks it against the product-evidence summary. It also deserializes the full
typed `StandingRuntimeFencingCapability` schema before accepting required-mode
claims, so missing or unknown capability fields fail closed. Product-complete
requires the sibling attestation to carry trusted CI provenance over the
canonical backend-time evidence bundle; diagnostic local bundles without that
provenance remain fail-closed. The attestation must prove
Raft-replicated authority time, backend wall-clock TTL expiry, bounded failover,
and explicit rejection of Raft metrics, log indexes, or distributed-lock TTLs as
substitute time sources.
`scripts/assess-hiqlite-backend-time.sh` is the local fail-closed check for
this claim; it scans the pinned Cargo package and writes
`hiqlite-backend-time-assessment.json` so the blocker can be revalidated without
turning the assessment into a product-complete attestation. In Hiqlite-backed
vind runs, `scripts/run-vind-product.sh` may attach that assessment to
`product-evidence.json` as
`metadata_store.hiqlite_backend_time_assessment` with
`trusted_for_product_complete=false`. This field is diagnostic evidence for the
current blocker, not a substitute for
`metadata_store.hiqlite_backend_time_attestation`.

`scripts/attest-hiqlite-backend-time.sh` is the next diagnostic step after a
deployed vind run. It reads `product-evidence.json` plus sibling deployed smoke
artifacts (`readyz.json`, `multi-replica-fencing-smoke.json`,
`standing-runtime-failover-smoke.json`, `velorix-meta-smoke.log`, and
`hiqlite-backend-time-assessment.json`), emits
`hiqlite-backend-time-attestation.json`, and can attach the matching
`metadata_store.hiqlite_backend_time_attestation` summary only when explicitly
requested. In the canonical evidence bundle, the `product_evidence` entry is
hashed after removing `metadata_store.hiqlite_backend_time_attestation`; this
keeps the release-gate copy/update step from signing a self-referential product
evidence file. The artifact is intentionally marked
`trusted_for_product_complete=false` and `trusted_for_release_validator=false`;
release validation accepts product-complete only when
`VELORIX_HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE=1` supplies CI workflow
identity, a full clean source revision matching the release commit, subject
image digests for `velorix-api`, `velorix-meta`, and `hiqlite-authority`, and
canonical bundle digest metadata over the referenced smoke evidence. The
`velorix-api` and `velorix-meta` subject digests are checked against the
product evidence `deployed_images` Deployment/Pod evidence, while
`hiqlite-authority` is checked against the managed authority attestation. It also
requires GitHub Actions OIDC identity fields bound to
`mrchypark/velorix/.github/workflows/release-gate.yml` on `refs/heads/main` or
a `refs/tags/v*` release tag and a Sigstore/Rekor-style
signature bundle whose signed payload digest equals the canonical bundle digest.
The validator verifies the compatibility Ed25519 signature over that canonical
digest. When `sigstore_bundle_base64` is present, it also parses the real
Sigstore bundle and verifies the canonical evidence bundle digest against the
Sigstore production trusted root, the expected GitHub Actions OIDC identity,
Fulcio certificate chain, and Rekor inclusion proof. Without that real Sigstore
bundle the product-complete claim remains fail-closed. The release gate signs the
canonical evidence bundle with `cosign sign-blob --bundle`, attaches the bundle
to `hiqlite-backend-time-attestation.json`, and updates the copied
standing-runtime product evidence before running `readiness-report`. The
failover evidence must be
release-scoped (`evidence_scope=release_ci_deployed_product`,
`failover_probe_kind=release_bounded_wall_clock_failover`,
`production_wall_clock_failover_attestation=true`). The local failover smoke
generated by `scripts/smoke-vind-standing-runtime-failover.sh` remains
diagnostic and is rejected for trusted backend-time release claims.
`scripts/run-vind-product.sh` wires this in as
`VELORIX_HIQLITE_BACKEND_TIME_ATTEST=auto`: for Hiqlite required-mode runs it
generates and attaches the diagnostic candidate after the relevant deployed
smokes pass, but it keeps product-complete blocked while the candidate remains
diagnostic.

Run the object-store backend with the same S3-compatible settings used by
`velorix-api`:

```bash
VELORIX_META_BACKEND=oss \
VELORIX_S3_COMPAT=1 \
AWS_ENDPOINT_URL=http://127.0.0.1:9000 \
AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
VELORIX_S3_BUCKET=velorix \
VELORIX_S3_PREFIX=meta \
cargo run -p velorix-meta
```

Point `velorix-api` at the meta service with:

```bash
VELORIX_META_GRPC_ENDPOINT=http://velorix-meta:9090 cargo run -p velorix-api
```

or containerized with:

```bash
docker build -f Dockerfile.meta -t velorix-meta:dev .
```

## Deployment Shape

Velorix product evidence must not depend on PVC-created local disk. The
production Hiqlite voter set is therefore treated as an externally operated
three-voter authority with its own backup/restore contract to object storage.
The vind product script may attest that authority, but it does not create
voters, StatefulSet PVCs, or any other durable volume for Hiqlite.

A Velorix-managed query or API pod may attach a learner-only sidecar for local
metadata reads. That sidecar is not the write authority and must not be counted
as one of the three voters.

Query workers can add a learner-only sidecar:

```yaml
containers:
  - name: query-engine
    image: velorix-query:dev
  - name: hiqlite-read-replica
    image: velorix-hiqlite:main
    env:
      - name: HQL_LEARNER_ONLY
        value: "true"
      - name: HQL_SECRET_RAFT
        valueFrom:
          secretKeyRef:
            name: velorix-meta-secrets
            key: raft
      - name: HQL_SECRET_API
        valueFrom:
          secretKeyRef:
            name: velorix-meta-secrets
            key: api
```

## Required Product Semantics

Writes must go to the three-pod writer cluster. Learner sidecars may serve local
reads only when the caller can tolerate the sidecar's applied-index freshness.
Fresh reads must carry a required revision or fall back to the write cluster.

Append admission order is:

1. Reserve the relation ingest range through the gRPC meta service.
2. Materialize Velorix recovery evidence to object storage.
3. Append the ingest batch payload to object storage with create-only semantics.
4. Periodically back up hiqlite metadata to object storage.

The meta service is the hot admission authority. In `hiqlite` mode, object
storage still receives recovery evidence and batch payloads so the existing
replay/query path can validate committed batches after restart. In `oss` mode,
the same object-store durable admission index is used as the meta authority and
as the recovery evidence source. If a range reservation succeeds but the payload
append fails, an identical retry is admitted as a duplicate metadata reservation
and can complete the missing object-store payload write.
