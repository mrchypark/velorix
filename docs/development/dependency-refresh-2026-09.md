# September 2026 dependency refresh

Status: local verification passed as of 2026-09-21; PR CI remains pending.
Compatibility holds below mean this is not an all-latest or deployed-migration
claim. Rust remains pinned to 1.98.1; OS-installed packages are outside scope.

## Update scope

The initial compatible resolver update reported 74 package updates. That count
describes the first resolver pass, not the final graph after the reviewed
release-family changes below. Versions were checked against the official
crates.io sparse index. Existing user script changes are outside this update.

| Family | Before | Selected or candidate | Review boundary |
| --- | --- | --- | --- |
| rustls | 0.23.43 | 0.23.45 | Fix RUSTSEC-2026-0285; no advisory waiver |
| DataFusion | 54.1.0 | 55.1.0 | Query API and semantic regression tests |
| Arrow / Parquet | 58.4.0 | 59.3.0 | Match DataFusion's compatible Arrow family |
| base64 | 0.22.1 | 0.23.1 | Preserve stored encoding and binary round trips |
| sha2 | 0.10.9 direct | 0.11.0 direct | Preserve digest bytes, formatting and stored keys |
| sqlparser | 0.62.0 direct | 0.63.0 direct | AST adaptation, admission and saved-plan compatibility |
| object_store | 0.14.1 authority path | 0.14.2 resolved | Keep create-only/CAS and durability behavior |
| clap | 4.6.6 | 4.6.7 resolved | Compatible range update |
| reqwest | 0.13.4 | 0.13.5 resolved | Compatible range update |
| rcgen | 0.14.9 | 0.14.10 resolved | TLS fixture regression checks |
| uuid | 1.26.0 | 1.26.1 resolved | Compatible range update |
| Rhiza | exact 0.12.0 | exact 0.12.2 | Backend tests and local three-node recovery passed; later migration boundaries held |
| Hiqlite | pinned fork revision | Unchanged | Required authority-time API and fork main remain at the same revision |

Already-current or compatible-resolved direct families include anyhow,
async-trait, axum, axum-server, bytes, futures, kube/k8s-openapi, serde,
serde_json, schemars, tokio, tonic/prost, tempfile, thiserror, tower, url,
sigstore, ring, proptest and the vendored protoc binary packages. A workspace
declaration that is not consumed does not add a new runtime dependency.

## Compatibility holds are intentional

Latest standalone Arrow and Parquet are 60.0.0, but published DataFusion 55.1.0
requires `^59.2.0`. The selected 59.3.0 family is the latest compatible 59
release observed. Introducing incompatible Arrow types or a development
DataFusion build merely to report version 60 is not part of this update.
DataFusion also retains object_store `^0.13.2` and sqlparser `^0.62.0`; these
transitive lines may coexist with Velorix's direct lines. Duplicate versions
must be evaluated against actual dependency-governance evidence, not forcibly
removed across incompatible APIs.

Rhiza 0.12.2 is selected as the reviewed low-risk patch update. Version 0.12.1
rechecks the original slot's durability barrier before returning a cached
SQL/KV/Graph/Notify receipt. Version 0.12.2 reuses the archive extent's encoded
bytes while retaining the same content hash, size/integrity checks and
conditional publication. The reviewed 0.12.0-to-0.12.2 source diff changes no
exported API, Go dependencies, Rust SDK implementation or storage schema.
Backend and local recovery tests passed; no performance improvement is claimed.

Rhiza 0.12.3 changes the nested LatticeDB dependency to 0.6.0 and is held for
separate persisted-state compatibility testing. Rhiza 0.15.4 is available, but its native engine incorporates a migration
boundary introduced in 0.13. Existing multi-voter deployments must stop all
old voters, enroll every original intact WAL offline, and restart upgraded
binaries with original membership, credentials and namespace. Mixed old/new
binaries and enrollment of empty replacement WALs are unsupported. Current
Rust SDK methods do not provide the documented Go/CLI enrollment operation.
This is especially material to the no-PVC recovery contract: a version bump
does not authorize replacing lost voter identity or generating a new cluster.
The later-version upgrade is therefore held for a separate reviewed migration workflow.
Rhiza remains a leaderless QuePaxa/KV backend; Hiqlite's protocol is not its
migration model.

The current Hiqlite fork main still matches the pinned revision
`26c6d22a72d7bd0a1e2de073fc4c076ddad4e588`, which supplies the required
authority-time API. Substitution with the registry release has not been shown
to preserve that contract. Governance descriptions refer to the actual pinned
Git fork, including the remaining transitive exceptions. Existing exception
expiration dates and policy requirements remain unchanged.


## Primary references

- [Rustls advisory RUSTSEC-2026-0285](https://rustsec.org/advisories/RUSTSEC-2026-0285).
- [Published DataFusion dependency metadata](https://index.crates.io/da/ta/datafusion).
- [Published Arrow release metadata](https://index.crates.io/ar/ro/arrow).
- [Rhiza voter registration and upgrade requirements](https://github.com/mrchypark/rhiza/blob/v0.15.4/docs/recovery.md).
- [Rhiza 0.12.1 cached-receipt durability fix](https://github.com/mrchypark/rhiza/releases/tag/v0.12.1).
- [Rhiza 0.12.2 archive-byte reuse and compatibility](https://github.com/mrchypark/rhiza/releases/tag/v0.12.2).
- [Reviewed Rhiza 0.12.0-to-0.12.2 source changes](https://github.com/mrchypark/rhiza/compare/v0.12.0...v0.12.2).
- [Rhiza 0.15.4 native packaging](https://github.com/mrchypark/rhiza/releases/tag/v0.15.4).
- [Hiqlite pinned authority-time revision](https://github.com/mrchypark/hiqlite/commit/26c6d22a72d7bd0a1e2de073fc4c076ddad4e588).
