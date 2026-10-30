## README.md

- Replaced blanket eager-ingest/query wording with configured authoritative demand defaults, existing eager-checkpoint behavior, and the public materialized-output query boundary. Evidence: `crates/velorix-api/src/lib.rs`, `ingest_epoch.rs`, `query_serving.rs`.

## docs/architecture/advanced-capabilities-design-v1.md

- Clarified that the file is a proposal and listed narrow implemented families with explicit limits; proposals remain targets unless validators/runtime establish them. Evidence: `crates/velorix-core/src/view_plan/mod.rs`, `crates/velorix-runtime/src/materialized_view_runtime.rs`.

## docs/architecture/api-serving-scope-v1.md

- Distinguished public routes and typed view admission from policy-bounded read-time DataFusion and internal table scans; recorded public feature defaults. Evidence: `crates/velorix-api/src/lib.rs`, `query_serving.rs`, `view_admission.rs`.

## docs/architecture/external-table-surface-contract.md

- Scoped the persisted-table contract to internal/production scans; the public router has no arbitrary source scan or persisted-table registration route. Evidence: `crates/velorix-api/src/lib.rs`.

## docs/architecture/incremental-key-semantics-v1.md

- Replaced the stale claim that plan-level key/bag version binding was absent; validation and plan hashing include these semantics. Broader checkpoint migration compatibility remains unproven. Evidence: `crates/velorix-core/src/view_plan/mod.rs`.

## docs/architecture/incremental-processing-semantics-v1.md

- Replaced the stale boolean-scan bootstrap limitation with the current persisted source cut/generation and activation-cut/checkpoint-coverage path. Evidence: `crates/velorix-api/src/view_admission.rs`, `crates/velorix-control/src/meta_admin.rs`, test `rest_view_admission_persists_plan_spec_and_source_cut_in_metadata` in `crates/velorix-api/src/tests.rs`.

## docs/architecture/incremental-sql-gap-plan.md

- Added a baseline note that this is a historical gap ledger; later narrow SQL/dependency families exist, but the ledger's broader public/restart/benchmark gates do not follow from source presence. Evidence: current validators and runtime dispatch; `docs/architecture/supported-sql.md`.

## docs/architecture/materialized-view-runtime-roadmap.md

- Updated the dated milestone framing for later narrow implementation and clarified that producer/consumer cursor handling is implemented for bounded shapes. A three-level test does not qualify arbitrary DAGs or performance. Evidence: `crates/velorix-api/src/view_dependencies.rs`, `view_dependency_dag_tests.rs`, `tests.rs::rest_three_level_filter_aggregate_topk_chain_exact`.

## docs/architecture/materialized-view-runtime.md

- Changed the universal ingest-driven update description to distinguish Legacy/eager from authoritative demand-managed views. Evidence: `crates/velorix-api/src/ingest_epoch.rs` and `docs/development/generic-view-pipeline.md`.

## docs/architecture/operator-edge-contract-v1.md

- Reconciled edge/operator status with current narrow typed-plan and runtime implementations while retaining the broader proposal boundary. Evidence: `crates/velorix-core/src/view_plan/mod.rs`, `crates/velorix-runtime/src/materialized_view_runtime.rs`.

## docs/architecture/phase-4-8-execution-plan.md

- Marked historical suite totals/status as dated evidence and linked the current SQL admission matrix; old counts are not current test results. Evidence: `docs/architecture/supported-sql.md`.

## docs/architecture/phase-8-cross-join-design.md

- Replaced design-only status with narrow runtime/public admission evidence and exact restrictions; broad composition and scale remain unqualified. Evidence: `validate_supported_cross_join_sql`, runtime and API tests linked in the file.

## docs/architecture/phase-8-interval-join-design.md

- Replaced design-only status with strict overlap INNER JOIN support and bounded grammar; unsupported clauses remain rejected. Evidence: `validate_supported_interval_join_sql`, interval runtime/API tests linked in the file.

## docs/architecture/phase-8-percentile-design.md

- Replaced design-only status with grouped direct-Int64 percentile/median support and admission exclusions; ordered-set/Decimal128/general forms remain unsupported. Evidence: percentile runtime and API tests linked in the file.

## docs/architecture/phase-8-recursive-cte-design.md

- Replaced design-only status with narrow positive fixpoint admission and bounded first-CTE execution; arbitrary/mutual recursion is not implied. Evidence: `validate_supported_recursive_cte_sql`, API restore test linked in the file.

## docs/architecture/phase-8-udf-design.md

- Clarified that the implemented registry is compiled-in deterministic typed expressions, not user-provided code or dynamic registration. Evidence: `crates/velorix-core/src/view_plan/typed_expr.rs`.

## docs/architecture/phase-8-window-frames-design.md

- Recorded bounded ROWS navigation runtime support and narrow admission limits; public API end-to-end qualification remains unestablished. Evidence: validator and runtime links in the file.

## docs/architecture/query-rewrite-design-v1.md

- Added narrow implemented rewrite families while retaining the proposal boundary; these do not imply general subquery/CTE support. Evidence: `crates/velorix-core/src/view_plan/mod.rs`.

## docs/architecture/supported-sql.md

- Rebased the canonical SQL admission status from the stale 2026-09-05/`677c125` claim to baseline `c6222ab`. Rewrote product flow and matrix definitions to distinguish public admission, `StandingRuntime` execution mode, authoritative demand defaults, Legacy/eager behavior, and scope/cut-specific 202/200 responses. Corrected the universal late-view backfill claim and synchronous materialized-ACK claim: demand queries can materialize without explicit backfill; the fixed `ack_mode` label is not a freshness certificate; demand-managed views may remain behind source ingest. Evidence: `view_admission.rs`, `ingest_epoch.rs`, `query_serving.rs`, `demand_materialization.rs`, `view_admission.rs`.

## docs/development/generic-view-pipeline.md

- Reconciled configured mode defaults, deferred declarations, demand/backfill budget and pause semantics, dependency closure, and 202/200 completion scope with code; explicitly leaves cited tests unrun and external qualification open. Evidence: `crates/velorix-api/src/lib.rs`, `view_admission.rs`, `query_serving.rs`, `demand_materialization.rs`, focused API tests.

## docs/how-to.md

- Added required authoritative ingest owner configuration and corrected the example to show deferred query/backfill materialization. Evidence: API configuration and view admission paths.
## docs/architecture/ingest-admission-contract.md

- Production multiwriter overlap safety remains a cutover requirement, but current checked source includes `RangeAdmissionIndexV1` and checked startup reconstruction. The document already distinguishes source/local harness support from deployment qualification; this source comparison did not establish a deployed external coordinator or production topology.
- The public API's fixed `ack_mode: materialized` wire label did not prove that all demand-managed view output was current. Rewrote the body to distinguish the label from `materialization.status`: authoritative demand views are skipped on ingest, the local pass can report `completed` after those skips, and API-only split mode can report `deferred`. Legacy/eager view application remains mode-specific; per-view progress/coverage establishes freshness. Evidence: `crates/velorix-api/src/ingest_epoch.rs::materialize_prepared_ingest_epoch_for_ack_mode`, `is_demand_managed_view`, and `worker_role.rs`. No historical replay-409 fix was verified or claimed.

## docs/architecture/ingest-envelope-v1.md

- The framing requirement was stale: the codec already uses magic + little-endian `u32` JSON-header length + JSON header + Arrow IPC body.
- The schema list omitted optional `event_time_watermark`, which is part of the header and digest.
- The doc required the codec to require a literal Int64 `weight` column; the codec does not. Relation catalog validation chooses the weight column, and the envelope codec validates framing, digest, and Arrow stream.
- Oversized header/body rejection is listed as a verification requirement but no explicit size cap exists in this codec. This remains an implementation/evidence gap.

## docs/architecture/ingest-storage-format-decision.md

- The accepted Arrow envelope is implemented in the current storage codec and API preparation path; the requirements section read as a still-unlanded breaking slice. Added a current implementation status while retaining benchmark/provider release gates.
- JSON rows remain accepted at the HTTP request boundary and are encoded into Arrow IPC before durable ingest; JSON is not the durable `v1/ingest` format.

## docs/architecture/legacy-json-deltabatch-removal.md

- Raw JSON at a valid ingest object key fails replay as typed `MalformedEnvelope`, not a dedicated “unsupported envelope” error.
- The prohibition was broader than the source contract: `DeltaBatch` remains an internal runtime/output representation. The durable JSON prohibition is scoped to source ingest/replay payloads.

## docs/architecture/meta-service.md

- The “First Meta API Slice” is historical; current proto/service source contains additional ingest publication/source-cut, active-view/bootstrap, runtime-owner/checkpoint, and partition-authority RPCs. Added a current source-contract summary rather than revising the dated implementation history.
- Metadata consists of multiple scoped APIs and pointer/reference records, not one general checkpoint snapshot. Rhiza Local archive recovery and object-store runtime checkpoint bodies are separate durability paths.
- Source configuration requires explicit mode/backend; production rejects memory, gates OSS/Rhiza durability, and treats Local mode as single-writer rather than consensus. Root-CAS logical ticks do not establish bounded wall-clock failover or production multiwriter safety.
- Historical release/test counts remain historical and were not reverified here.

## docs/architecture/partition-ownership-protocol-v1.md

- The document’s design is ahead of the deployed adapter: source provides ownership record validation, claim checks, and a pure backend gate, but no Kubernetes adapter/operator/reconciliation. Added a warning that source presence does not prove Lease-to-commit wiring in a deployment.

## docs/architecture/relation-contract-v1.md

- The “required fields” list mixed actual catalog fields with fields not stored by `VelorixRelationCatalogV1` (partitioning policy, offset domain, and ingest format). Replaced it with the actual nested model and clarified that stream/partition/range live in the ingest envelope.
- Core validation includes `incremental-adapter-generic-v1`, contrary to the prior claim that only sum/count adapters exist. A local public API test exercises generic registration, ingest, and recursive-view recovery; this does not establish production deployment or whole-DAG qualification for every generic shape. Kept unknown/multi-value shapes fail-closed.

## docs/architecture/rhiza-migration.md

- No contradiction established in the current source summary. Release, local recovery, and test evidence later in the document is historical/run-local and was not reverified during this source reconciliation; labeled that boundary and linked current source entry points.

## docs/architecture/schema-fingerprint-v1.md

- No drift detected.

## docs/architecture/state-substrate-contract.md

- No direct mismatch established in the inspected storage/meta pointer types. Full compatibility of every recovery, GC, and provider claim was not independently verified; added source links and clarified that source/local tests do not establish provider-wide or whole-DAG qualification.

## docs/architecture/storage-contract.md

- The doc described JSON `DeltaBatch` replay as current bootstrap behavior to be removed, but the current validated durable ingest/replay path uses Arrow IPC. JSON remains at request/runtime/output boundaries under separate contracts.
- Range-overlap rejection was described as awaiting implementation of an admission index. The checked catalog-aware coordinator and index now exist; deployed production multiwriter qualification remains gated by the documented writer/topology/provider evidence.
- The opening “current prototype contract” label understated implemented object/pointer validation while obscuring remaining deployment gates; replaced it with a current-contract/readiness distinction.
- The public API paragraph treated the `materialized` wire label as universal synchronous materialization. Replaced it with mode/path-specific status semantics; a local `completed` pass does not prove demand-managed views advanced. Evidence: `crates/velorix-api/src/ingest_epoch.rs` and `worker_role.rs`.

## docs/architecture/type-inventory-v1.md

- The document labels many per-type semantics “Fully Implemented” and states broad arithmetic, aggregate, null, and checkpoint rules. Those claims were not reconciled against current runtime operators and focused tests in this metadata/ingest slice; they are explicitly marked unverified design inventory pending the canonical/runtime review.

## docs/development/rhiza-kv-k8s.md

- Several current/historical test and recovery claims linked directly to ignored `target/` run artifacts. Removed those links and clarified that the records are run-local, historical evidence rather than public proof; retained the stated limits (no Kubernetes/live-cloud qualification).

## docs/architecture/benchmark-gate-v1.md

- The current local harness fixes top-level `scan_bytes` and `spill_bytes` at zero, and records terminal RSS rather than a high-water peak. The document described these as required benchmark metrics without stating that limitation. Added a qualification and implementation links; workload counts remain fixture- and phase-specific.

## docs/architecture/checkpoint-recovery-index-v1.md

- The document used a nonexistent `CheckpointManifestV1` and described fields from the API `RuntimeCheckpoint` as though they belonged to the storage `CheckpointManifest`. Rewrote this contract to distinguish storage manifests, API Arrow checkpoint records, and their different pointer paths.
- `strict`, `admin_inspect`, and `last_known_good_read_only` are not `CheckpointRecoveryMode` enum variants. Clarified them as behavioral descriptions and named the two actual enum variants.

## docs/architecture/checkpoint-state-arrow-v1.md

No drift detected.

## docs/architecture/datafusion-resource-policy-v1.md

- The document called DataFusion 54 the current wiring and treated its memory-exhaustion result as current evidence. `Cargo.toml` pins DataFusion 55.1.0. Updated the version and marked the DataFusion 54 result historical, with current-version behavior unverified.

## docs/architecture/materialized-output-segment-index-v1.md

- The status was only Proposed although materialized Arrow page indexes, typed per-page null/min/max statistics, and conservative filtered page reads are implemented. Clarified partial implementation and linked the code/test.
- The proposed persisted segment fields and recent-K proof are not the current `PageIndex`; the recent-K benchmark uses a synthetic fixture. Added these limits as remaining design targets, not as public API support.

## docs/architecture/object-store-capabilities-v1.md

- The document implied generic startup requires conditional update/CAS and that the per-namespace capability profile proves range/list/metadata behavior. The generic profile has conditional-create, atomic-visibility, read-after-write, and list-after-write fields; conditional update is caller-specific, and range reads are checked separately by the S3 harness. Marked per-namespace range/list/metadata requirements as caller-level and not wholly proven by the generic probe; clarified that request counts are phase-specific.

## docs/architecture/s3-compatible-test-harness.md

- The dependency note identified DataFusion 54 and `object_store` 0.13. Updated it to the current workspace pin, DataFusion 55.1.0 with resolved `object_store` 0.13.2, alongside storage's resolved 0.14.2.

## docs/development/functional-performance-validation.md

No drift detected.

## docs/development/materialized-query-cache.md

- The read-site inventory omitted the Foyer cache used for immutable physical ingest-envelope inputs during demand materialization. Added its identity bindings and distinguished it from derived published-view inputs. Clarified the existing admission-time walk of published parents to physical history roots; no claim is made that a consumer cache key authorizes upstream state.

## docs/development/query-performance-validation-2026-09-22.md

No drift detected.

## docs/development/runtime-arrow-snapshot-verification.md

No drift detected.

## docs/development/versitygw-s3.md

No drift detected.
## docs/architecture-critique.md

- The “Current status” paragraphs summarize an older critique/bundle and are not a baseline-current audit. The document itself says its line references are to `attachments-bundle.txt`; that attachment is not repository source. Current assessment of its historical findings is unverified here. Marked the critique as historical and pointed to the canonical spec/index.

## docs/architecture/dependency-governance.md

- The 2026-10-08 graph counts, package totals, exception totals, and validator outcome are dated evidence, not a fresh current graph. The October 8 notes name Rhiza 0.19.0-era review and Hiqlite removal; current Cargo metadata pins Rhiza 0.21.2 and has no Hiqlite package. Added a freshness boundary and linked current policy inputs. Fresh CI deny success is recorded separately and does not replace the saved diagnostic artifact or release gate.

## docs/architecture/package-review-opendata.md

- OpenData details are a review of upstream commit `0961492a72bc5911f44f782f774ede233af8778e` from 2026-04-30. The proposal is not current dependency approval or current Velorix architecture evidence. Marked it as dated research and linked current product facts to the canonical spec/Cargo manifests.

## docs/architecture/production-readiness-status.md

- The file did not record the supplied fresh CI baseline. Added run `41020731224` against baseline `c6222abd054858109c90a63a061db698fa9fc9e8`, bounded to the eight reported jobs and explicitly excluding SQL completeness, the representative S3 release benchmark, and production readiness.

## docs/consultation/phase-4-design-review-packet.md

- Its “Current Architecture” and gap lists say there is no dependency graph/cycle validation, inter-view input lineage, or delta forwarding. Current source contains dependency-edge and graph validation, published relation bindings/cursors, and DAG-focused API tests. The old packet remains a design snapshot; those source/test presences do not establish complete DAG semantics or production qualification. Marked its claims historical and linked the canonical spec.

## docs/development/dependency-refresh-2026-09.md

- The September snapshot says Rhiza 0.12.2 and an unchanged Hiqlite fork. Current Cargo files select Rhiza 0.21.2 and omit Hiqlite. Marked the version table and migration assessment historical and linked current manifests/policy.

## docs/development/vind-k8s.md

- No material contract drift found in the documented gate scope, cleanup controls, or object-store evidence boundary when compared with `scripts/run-vind-k8s-gate.sh`, `.github/workflows/k8s-vind.yml`, and the named live test targets. Corrected the sentence that said the artifact “is writes” evidence. Added an explicit note that the CI baseline is not evidence this manual gate ran.

## docs/development/vind-product.md

- The embedded Rhiza service/harness section names version 0.19.0 while current dependency policy/Cargo metadata pin Rhiza 0.21.2. The current gate script still enforces the documented member fields and separate node-keyed peer-token map. Updated the version boundary and linked the current script; this source-level contract does not qualify runtime/deployment behavior.

## docs/issue-24-aggregate-group-liveness.md

No drift detected.

## docs/issue-26-interval-join-semantics.md

No drift detected.

## docs/issue-27-temporal-asof-contract.md

No drift detected.

## docs/issue-28-recursive-predicate-direction.md

No drift detected.

## docs/issue-29-recursive-cte-reachability.md

No drift detected.

## docs/issue-31-scalar-aggregate-modifiers.md

No drift detected.

## docs/open-issue-priority-2026-09-12.md

- The issue count, effective ordering, issue states, CI/GHCR outcome, and PR progress are explicitly scoped to the 2026-09-12 snapshot/base `350105f`; they are not current baseline or issue-tracker status. Marked the ledger historical and directed current implementation status to the canonical spec.

## docs/release/0.1-native-runtime-migration.md

- The migration records API/export and checkpoint compatibility at an earlier pre-release point. Current crate exports and supported API compatibility were not exhaustively reconstructed from this note; marked it historical and pointed readers to current source/specification.

## docs/release/0.6.0-preview.md

- This is a candidate-specific draft with local target evidence paths and a historical candidate/evidence snapshot. Those paths are not public or independently revalidated evidence. It correctly withholds authorization and release qualification; added a clear snapshot boundary and canonical readiness pointer. The supplied CI run does not clear its representative S3 release benchmark gap.

## docs/release/1.0-readiness-checklist.md

- The checklist defined evidence requirements but omitted the newly supplied CI baseline. Added the run and explicitly limited its meaning; the checklist still does not assert that release evidence exists or that the release gate passed.

## docs/reviews/2026-08-28-comprehensive-review.md

- Review findings and completion marks are tied to review SHA `c86c5299e5f0a158f68c29fd1daaecb18442623e`; they are not current implementation/performance status. Marked as historical. Fresh disposition of each old finding is unverified here.

## docs/reviews/2026-08-29-phase3-5-plan.md

- Proposed phases and unchecked validations are from the dated planning snapshot, not a current code inventory. Marked as historical. Item-by-item current disposition is unverified here.

## docs/reviews/2026-08-31-remaining-gaps.md

- Claims that 16 modules remain standalone and the issue-status tables are a 2026-08-31 snapshot. Current source has moved beyond that snapshot (including active recursive runtime code and DAG components). Marked historical; the old item-by-item disposition is unverified here.

## docs/reviews/2026-09-04-main-review-reassessment.md

- The reassessment explicitly covers only the range through `677c125`, not baseline `c6222abd054858109c90a63a061db698fa9fc9e8`. Marked as historical; its individual statuses are not fresh baseline findings.

Source baseline: `c6222abd054858109c90a63a061db698fa9fc9e8` (2026-10-30). Scope: the 68 paths in `target/egm-spec-reconciliation-20261030/scope.json`. Findings derive from the group reports and source cross-checks. No builds or tests were executed during this documentation reconciliation. No formal EGM Goldfish review has completed; these are source-drift findings, not Goldfish reviewer results.
