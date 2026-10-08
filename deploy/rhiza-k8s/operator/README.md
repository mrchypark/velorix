# Rhiza 0.19.0 recovery operator scaffold for Velorix

This directory vendors the official Rhiza recovery-operator manifests and
overlays only what a Velorix validation run must change.

## Provenance

| item | value |
| --- | --- |
| upstream repository | `github.com/mrchypark/rhiza` |
| tag | `v0.19.0` |
| release commit | `abb87a0336cba8fee3fd1d9e5a0bf797de5b25b8` |
| upstream path | `deploy/operator/` |
| operator image | `ghcr.io/mrchypark/rhiza-operator:v0.19.0` |
| pinned image digest | `sha256:00b5e7a4c33c84ddbc2b7272b7d06bd3dd8315e31acb29507ad627d522a19d67` (`linux/amd64`) |

`crd.yaml`, `rbac.yaml`, and `deployment.yaml` are byte-for-byte copies. Their
sha256 digests are recorded in `UPSTREAM.sha256`, and
`scripts/run-rhiza-kv-k8s-gate.sh` recomputes them before rendering. A digest
mismatch aborts the gate without touching a cluster: a recovery claim must rest
on manifests that are verifiably the official ones.

The operator image is pinned by digest, not by the `latest` tag, because the
operator is the component that replaces cluster identity and credentials. A
mutable reference there is not acceptable.

### The pinned digest is a single-platform `linux/amd64` image

`sha256:00b5e7a4…19d67` is one `application/vnd.oci.image.manifest.v1+json`
manifest, not a manifest list, and its config is `os: linux, architecture:
amd64` (`org.opencontainers.image.revision: abb87a0336cba8fee3fd1d9e5a0bf797de5b25b8`,
matching the release commit above). The node that runs the operator must
therefore be able to execute amd64 binaries. An arm64 node can satisfy this only
with a real x86 emulator. FEX (`FEX-x86_64` in `binfmt_misc`) does not satisfy
it: that image ships a squashfs rootfs, so FEX fails with `Couldn't execute:
FEXServer … the squashFS rootfs won't be mounted`. Install `qemu-x86_64` binfmt
in the node, or run the gate on an amd64 node.

### Running on a platform the published artifact cannot execute

On an arm64 node with no x86 emulator, the published digest simply cannot run.
The gate does not paper over that by weakening the pin. It supports one explicit,
recorded alternative: a binary built from the same pinned upstream source tree,
resolved by its own real digest.

```sh
VELORIX_RHIZA_OPERATOR_IMAGE_OVERRIDE=<repo>@sha256:<locally built digest> \
VELORIX_RHIZA_OPERATOR_IMAGE_PROVENANCE='github.com/mrchypark/rhiza v0.19.0 abb87a0336cba8fee3fd1d9e5a0bf797de5b25b8, built with the unmodified upstream Dockerfile.operator' \
  sh scripts/run-rhiza-kv-k8s-gate.sh
```

The rules this preserves:

- `VELORIX_RHIZA_OPERATOR_IMAGE` is only accepted when it is the exact official
  published pin above, so nothing about the default or production path moves.
- An override must itself be an immutable `sha256:` reference. No tag, and no
  alias digest invented to make a local image look like the published one.
- An override without `VELORIX_RHIZA_OPERATOR_IMAGE_PROVENANCE` is refused. A
  digest that cannot be attributed to a source tree and a build proves nothing.
- The gate still resolves the operator Deployment's live image reference and
  requires it to equal the requested one, so a crash-looping or
  image-mismatched operator can never be read as a recovery result.
- Evidence records `operator_image_official_published_release: false`,
  `operator_image_override_used: true`, and the provenance string. An overridden
  run is never reported as the published release image.

An overridden run is a real recovery through a real operator binary, but it is
not evidence that the published `linux/amd64` artifact executes, and it must not
be cited as such.

## Overlay

`kustomization.yaml` changes exactly two things, both required by the upstream
`deploy/operator/README.md`:

- `namespace`: the operator is namespace-scoped. Its ServiceAccount, Role,
  RoleBinding, and Deployment must be created in the same namespace as the
  application StatefulSet, otherwise the controller reconciles a namespace it
  has no permission to read.
- `image`: the upstream sample uses the local name `rhiza-operator:latest`.

Render with:

```sh
kubectl kustomize deploy/rhiza-k8s/operator
```

This vendored overlay always keeps the official published pin. The gate builds
its own per-run overlay in a private staging directory instead, so that a
digest override never rewrites a committed manifest: `kustomize.yaml` here stays
the statement of what production runs, and the gate's evidence states what
actually ran.

## What is deliberately not here

- `cluster-crd.yaml` and `fence-crd.yaml` exist upstream for opt-in
  `RhizaCluster` automatic recovery with an external fencing executor. Velorix
  does not ship a fencing executor, so automatic recovery is out of scope. The
  manual `RhizaRecovery` path in `crd.yaml` is what the gate uses, and it
  requires an explicit, separately confirmed `spec.fence` attestation.
- No recovery orchestration code. Generation replacement, archive sealing,
  credential rotation, and the target restart are all the operator's job. A
  Velorix script that restarted Pods itself would be a different, weaker
  mechanism wearing the same evidence.
- No `rhiza-config` or `rhiza-object-store` manifests. The gate creates those
  per run so that credentials and the bucket prefix are never committed.

## Applying

Order matters, per the upstream README:

1. CRD (cluster-scoped, so `crd.yaml` is applied without `-n`).
2. RBAC and the operator Deployment (namespaced).
3. The application ConfigMap, object-store Secret, peer-credential Secret,
   headless Service, and StatefulSet.
4. An observation-only `RhizaRecovery` with an empty `spec.recoveryID`, to
   confirm the operator can read the deployment contract before any destructive
   step.

The gate performs exactly this order. Applying these manifests to a cluster
that holds real data is an operator decision; the validation scripts refuse to
run against a namespace that is not `velorix-rhiza-validation*`.

## Do not pre-destroy the source generation

The actionable `RhizaRecovery` must be applied while the source StatefulSet
still declares exactly three desired replicas and its Pods are still running:

- `native/pkg/operator/controller.go` rejects any first reconcile whose source
  StatefulSet does not declare exactly three desired replicas, and
- that same first pass captures the certified archive suffix from the live source
  Pod recovery endpoints, so a stopped generation yields
  `unavailable: source admin token is not configured` instead of certified
  history.

The whole-generation loss is the operator's own work. After the fence is
confirmed it seals the archive, forks the certified history, writes the target
credentials, sets `spec.replicas` to `0`, and waits for every Pod owned by the
source StatefulSet UID to terminate. It refuses to activate the target while
replicas are non-zero or any source Pod survives, and it fails closed with
`source replicas reappeared before activation` or `source pods reappeared before
activation` if either happens. A harness that scales the StatefulSet down or
deletes Pods before applying the request does not produce a stronger recovery
claim; it produces a `Blocked` resource and no recovery at all.