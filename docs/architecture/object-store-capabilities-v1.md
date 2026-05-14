# Object Store Capabilities V1

Status: Accepted
Applies to: production storage adapters and every authoritative write namespace.

Object storage is the durable database authority. Production Velorix must fail
closed when a configured backend cannot prove the capabilities required by the
authoritative namespaces. Capability checks are not limited to checkpoint
manifest publication.

## Required Namespaces

| Namespace | Write requirement | Read/list requirement |
| --- | --- | --- |
| `v1/ingest` | Create-only object writes; no overwrite fallback. | Read-after-create, partition/range listing, metadata or ETag. |
| `v1/ingest-admission` | Create-only serialized admission evidence writes; no overwrite fallback. | Read-after-create and stream/partition/range listing. |
| `v1/state/raw` | Create-only raw bootstrap state writes. | Read-after-create, metadata or ETag. |
| SlateDB state | SlateDB-required object semantics. | Owned by the SlateDB adapter. |
| `v1/outputs` | Create-only output writes. | Read-after-create, metadata or ETag. |
| `v1/checkpoints` | Create-only manifest writes. | Read-after-create, listing, full reads. |
| `v1/queries` | Create-only catalog writes. | Read-after-create. |
| `v1/tables` | Create-only catalog writes. | Read-after-create. |
| `v1/relations` | Create-only relation catalog writes. | Read-after-create. |
| `v1/ownership` | Create-only epoch records, when enabled. | Read-after-create and documented listing semantics. |

Production startup must reject unsupported conditional create or CAS behavior.
Overwrite-based emulation is forbidden in production. Local filesystem or fake
object-store emulation is dev/test only and must require an explicit local mode.

All storage users must be created through one shared object-store registry.
Velorix storage, DataFusion scans, SlateDB integration, and Foyer fetch-through
must not silently use different endpoints, credentials, retry policies, or
telemetry.

## Acceptance Criteria

- Production startup probes and records `AuthoritativeObjectStoreCapabilitiesV1`.
- Missing create-only support rejects production startup.
- Each authoritative namespace is covered by the probe.
- Dev/local fallback requires an explicit non-production flag.
- Diagnostics expose backend, namespace, and capability status.
- Directly constructed storage clients are rejected outside tests.

## Current Minimal Model

Phase 2.3 records declared capability profiles as
`AuthoritativeObjectStoreCapabilitiesV1`, keyed by these authoritative
namespaces: ingest, state, output, checkpoint, ownership, table catalog,
relation catalog, artifact catalog, ingest admission, and benchmark evidence.

Startup validation rejects a missing namespace profile and rejects any namespace
whose `ObjectStoreCapabilityProfile` is missing a required durability
capability. A minimal S3-compatible storage harness now validates create-only,
read-after-write, list-after-write, range-read behavior, and startup
capabilities for every authoritative namespace when explicitly enabled.
Capability diagnostics expose the backend name, namespace, and missing
durability capabilities for every authoritative namespace.

## Current Verification

- Missing namespace declarations fail validation.
- Weak namespace declarations fail validation and report the namespace plus the
  missing durability capability.
- Diagnostics report backend, namespace, and missing capability status for every
  authoritative namespace.
- Complete namespace declarations pass validation.
- Production persisted table scans reject stores registered without production
  capabilities.
- Production persisted table scans accept stores registered with complete
  declared namespace capabilities.
- Checked production SlateDB recovery opens the state store only through shared
  startup `AuthoritativeObjectStoreCapabilitiesV1` evidence and requires both
  checkpoint and state namespaces before opening SlateDB.
- Env-gated S3-compatible `slatedb_state_reopen` benchmark evidence uses the
  same already-probed startup capability object before the initial SlateDB open
  and before reopening SlateDB.
- Production leased checkpoint publishing requires the checked leased-publisher
  constructor, which reuses the checked SlateDB checkpoint publisher and
  additionally requires output and ownership namespace evidence before
  production publication can proceed.
- Kubernetes worker-shard epoch-store construction can reuse validated operator
  startup evidence and requires the ownership namespace before persisting
  durable epoch records; env-gated local Kubernetes evidence reconstructs checked
  startup components over a local-filesystem authority and reads those epoch
  records back without falling through in-memory authority.
- Env-gated S3-compatible storage evidence validates create-only conflict,
  read-after-write, list-after-write, and range-read behavior.
- Env-gated S3-compatible capability evidence validates startup capabilities for
  every authoritative namespace.

## Future Verification

- Runtime probes reject backends without create-only support.
- Runtime probes reject overwrite-permitting backends for authoritative writes.
- Local emulation cannot run under production configuration.
- Shared registry tests prove DataFusion, SlateDB, Foyer, and Velorix storage use
  the same configured store identity.
