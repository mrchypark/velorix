# Partition Ownership Protocol V1

Status: Accepted
Applies to: distributed writers, fenced state/output writes, and manifest
publication.

Kubernetes Lease coordinates worker ownership. Kubernetes and etcd are not the
durable database authority. Production fencing requires an object-storage-backed
epoch record combined with storage-side commit checks.

Current implementation evidence is intentionally narrow: `velorix-control`
defines a pure production ownership backend gate that rejects in-memory/dev
leases and rejects backends without durable epoch record support. It does not
implement a Kubernetes adapter, operator, CRDs, or reconciliation.

## Durable Epoch Record

After acquiring a Kubernetes Lease, a production worker creates a durable epoch
record:

```text
v1/ownership/{stream_id}/p={partition_id:010}/epoch={owner_epoch:020}.claim
```

The record includes stream, partition, owner id, owner epoch, lease identity,
diagnostic timestamp, previous epoch when known, and previous checkpoint version
when known.

Fenced state writes, output writes, and manifest publication must carry a claim
that references the durable epoch record. Kubernetes Lease acquisition alone is
not enough to make `owner_epoch` durable or monotonic.

## Rules

- Matching durable epoch record is required in production fenced writes.
- Lower epoch writers are stale after a higher epoch record or manifest is
  visible.
- Same epoch with different owner is a conflict.
- Lease loss requires the worker to stop all authoritative writes.
- Distributed write mode requires object-store capabilities for `v1/ownership`.

## Verification

- Old worker writes after lease loss are rejected.
- Owner claim without epoch record is rejected in production.
- Same epoch/different owner fails closed.
- Lower epoch writes fail after higher epoch record creation.
- In-memory/fake lease clients cannot enable production distributed writer mode.
- Production ownership backend validation accepts a Kubernetes lease backend
  only when durable epoch records are supported.
