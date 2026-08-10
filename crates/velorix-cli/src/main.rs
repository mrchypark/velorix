#![forbid(unsafe_code)]
#![recursion_limit = "256"]

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::{self, Read},
    path::Path,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use bytes::Bytes;
use clap::{CommandFactory, Parser, Subcommand};
use object_store::{
    aws::AmazonS3Builder, local::LocalFileSystem, path::Path as ObjectStorePath,
    prefix::PrefixStore, ObjectStore,
};
use ring::signature::{UnparsedPublicKey, ED25519};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sigstore_trust_root::{TrustedRoot, SIGSTORE_PRODUCTION_TRUSTED_ROOT};
use sigstore_types::{Bundle as SigstoreBundle, Sha256Hash as SigstoreSha256Hash};
use sigstore_verify::{
    verify as verify_sigstore_bundle, VerificationPolicy as SigstoreVerificationPolicy,
};
use velorix_control::readiness::{ProductionReadinessEvidenceV1, ProductionReadinessReportV1};
use velorix_control::storage_admin::{
    probe_authoritative_object_store_capabilities, AppendValidatedEnvelopeOutcome,
    AuthoritativeObjectStoreCapabilitiesV1, CheckpointAdminInspection, CheckpointAdminRepairReport,
    CheckpointLifecycleStatus, CheckpointManifest, CheckpointManifestInspectionStatus,
    CheckpointPublisher, CheckpointRetentionRecordV1, GarbageCollectionCandidate,
    GarbageCollectionPlan, GarbageCollectionPolicy, GarbageCollectionRunV1, IngestBatchDescriptor,
    InputRange, LatestCandidateMarker, StateObjectWrite,
};
use velorix_core::relation::{VelorixRelationCatalogV1, VelorixRelationSchemaV1};
use velorix_k8s::{
    crd::ObjectStoreAuthorityRef,
    ingest_writer::DeployedIngestWriterRuntime,
    startup::{validate_operator_authority, OperatorAuthorityStartupComponents},
};
use velorix_meta::{
    StandingRuntimeFencingCapability, STANDING_RUNTIME_BACKEND_TIME_SOURCE_RAFT_REPLICATED,
    STANDING_RUNTIME_FENCING_CAPABILITY_SCHEMA_VERSION,
    STANDING_RUNTIME_LEASE_AUTHORITY_KIND_RAFT_REPLICATED_TIME,
    STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_BACKEND_WALL_CLOCK_TTL,
    STANDING_RUNTIME_OWNER_SCOPE_KIND_TENANT_PROGRAM_VIEW,
};
use velorix_runtime::benchmark_gate::{
    BenchmarkBackend, BenchmarkBudgetV1, BenchmarkEvidenceScope, BenchmarkGateLevel,
    BenchmarkGateResultV1,
};

const OBJECT_STORE_CAPABILITY_PROBE_WORKLOAD: &str = "object_store_capability_probe";
const LOCAL_BENCHMARK_GATE_WORKLOADS: &[&str] = &[
    OBJECT_STORE_CAPABILITY_PROBE_WORKLOAD,
    "ingest_envelope_validation",
    "native_sql_materialized_view_apply",
    "checkpoint_publish",
    "checkpoint_recovery",
    "materialized_output_segment_pruning",
    "materialized_output_recent_k",
    "materialized_output_compaction_equivalence",
    "materialized_output_compaction_debt",
    "materialized_output_delete_vector",
    "materialized_output_ttl_vector",
    "materialized_output_late_materialization",
    "slatedb_state_reopen",
    "gc_dry_run_planning",
    "gc_execution_evidence",
];
const S3_COMPATIBLE_BENCHMARK_GATE_WORKLOADS: &[&str] = &[
    OBJECT_STORE_CAPABILITY_PROBE_WORKLOAD,
    "ingest_envelope_validation",
    "checkpoint_publish",
    "checkpoint_recovery",
    "materialized_output_segment_pruning",
    "materialized_output_recent_k",
    "materialized_output_compaction_equivalence",
    "materialized_output_compaction_debt",
    "materialized_output_delete_vector",
    "materialized_output_ttl_vector",
    "materialized_output_late_materialization",
    "slatedb_state_reopen",
    "gc_dry_run_planning",
];
const INGEST_WRITER_LIFECYCLE_MAX_AGE_SECS: u64 = 24 * 60 * 60;
const INGEST_WRITER_LIFECYCLE_FUTURE_SKEW_SECS: u64 = 15 * 60;
const INGRESS_TLS_AUTH_ATTESTATION_MAX_AGE_SECS: u64 = 24 * 60 * 60;
const INGRESS_TLS_AUTH_ATTESTATION_FUTURE_SKEW_SECS: u64 = 15 * 60;
const HIQLITE_BACKEND_TIME_ATTESTATION_MAX_AGE_SECS: u64 = 24 * 60 * 60;
const HIQLITE_BACKEND_TIME_ATTESTATION_FUTURE_SKEW_SECS: u64 = 15 * 60;
const HIQLITE_BACKEND_TIME_ALLOWED_ATTESTERS: &[&str] = &[
    "scripts/run-vind-product.sh",
    "velorix-release-operator",
    "velorix-ci",
];
const HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE_KIND: &str = "velorix_ci_evidence_bundle_provenance";
const HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE_ATTESTERS: &[&str] =
    &["velorix-release-operator", "velorix-ci"];
const HIQLITE_BACKEND_TIME_REQUIRED_SUBJECT_IMAGE_ROLES: &[&str] =
    &["velorix-api", "velorix-meta", "hiqlite-authority"];
const HIQLITE_BACKEND_TIME_TRUSTED_SOURCE_REPOSITORY: &str = "github.com/mrchypark/velorix";
const HIQLITE_BACKEND_TIME_TRUSTED_GITHUB_REPOSITORY: &str = "mrchypark/velorix";
const HIQLITE_BACKEND_TIME_TRUSTED_OIDC_ISSUER: &str =
    "https://token.actions.githubusercontent.com";
const HIQLITE_BACKEND_TIME_TRUSTED_OIDC_AUDIENCE: &str = "sigstore";
const HIQLITE_BACKEND_TIME_TRUSTED_WORKFLOW_REF_PREFIX: &str =
    "mrchypark/velorix/.github/workflows/release-gate.yml@";
const HIQLITE_BACKEND_TIME_TRUSTED_SIGSTORE_CERTIFICATE_IDENTITY_PREFIX: &str =
    "https://github.com/mrchypark/velorix/.github/workflows/release-gate.yml@";
const HIQLITE_BACKEND_TIME_TRUSTED_RELEASE_BRANCH_REF: &str = "refs/heads/main";
const HIQLITE_BACKEND_TIME_TRUSTED_RELEASE_TAG_REF_PREFIX: &str = "refs/tags/v";
const REQUIRED_INGEST_WRITER_LIFECYCLE_EVIDENCE_FILES: &[(&str, &str)] = &[
    ("pod_internal_job", "velorix-ingest-writer-smoke-log.json"),
    ("overlap_job", "velorix-ingest-lifecycle-overlap-log.json"),
    ("adjacent_job", "velorix-ingest-lifecycle-adjacent-log.json"),
    ("restart_job", "velorix-ingest-lifecycle-restart-log.json"),
    (
        "lease_loss_job",
        "velorix-ingest-lifecycle-lease-loss-log.json",
    ),
    (
        "handoff_probe_job",
        "velorix-ingest-lifecycle-handoff-log.json",
    ),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HiqliteBackendTimeTrustStatus {
    Diagnostic,
    TrustedWithoutSigstoreBundle,
    SigstoreVerified,
}
const REQUIRED_RELEASE_CONTRACTS: &[&str] = &[
    "ingest",
    "relation catalog",
    "object-store capability",
    "ownership",
    "checkpoint lifecycle",
    "state substrate",
    "DataFusion policy",
    "table registry",
    "materialized view runtime",
    "benchmark gate",
    "S3-compatible tests",
    "Kubernetes operator",
    "GC",
    "dependency governance",
];

const ORDERS_SUM_COUNT_OWNER: &str = "orders_sum_count";

#[derive(Debug, Parser)]
#[command(name = "velorix-cli")]
#[command(about = "Local Velorix runtime utilities")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
enum Command {
    /// Generate a validated POST /v1/relations request from a relation schema.
    RelationCatalog {
        /// VelorixRelationSchemaV1 JSON file, or - for stdin.
        #[arg(long)]
        schema: PathBuf,
        /// Explicit incremental adapter ID.
        #[arg(long)]
        adapter_id: String,
    },
    CheckpointInspectLocal {
        #[arg(long)]
        object_store_dir: PathBuf,
        #[arg(long)]
        json: bool,
    },
    CheckpointRepairLatestLocal {
        #[arg(long)]
        object_store_dir: PathBuf,
        #[arg(long)]
        json: bool,
    },
    CheckpointRepairLocal {
        #[arg(long)]
        object_store_dir: PathBuf,
        #[arg(long)]
        json: bool,
    },
    GcPlanLocal {
        #[arg(long)]
        object_store_dir: PathBuf,
        #[arg(long)]
        retain_latest_manifests: usize,
        #[arg(long)]
        json: bool,
    },
    GcExecuteLocal {
        #[arg(long)]
        object_store_dir: PathBuf,
        #[arg(long)]
        retain_latest_manifests: usize,
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        json: bool,
    },
    GcExecuteS3Compatible {
        #[arg(long)]
        authority_store_id: String,
        #[arg(long)]
        retain_latest_manifests: usize,
        #[arg(long)]
        run_id: String,
        #[arg(long)]
        json: bool,
    },
    GcSeedS3CompatibleFixture {
        #[arg(long)]
        authority_store_id: String,
        #[arg(long)]
        seed_id: String,
        #[arg(long)]
        json: bool,
    },
    GcProductionEvidence {
        #[arg(long)]
        deployment_id: String,
        #[arg(long)]
        authority_store_id: String,
        #[arg(long)]
        gc_run_id: String,
        #[arg(long)]
        json: bool,
    },
    RustfsProductionGcEvidenceValidate {
        #[arg(long)]
        gate_evidence: PathBuf,
        #[arg(long)]
        seed_evidence: PathBuf,
        #[arg(long)]
        execute_evidence: PathBuf,
        #[arg(long)]
        production_evidence: PathBuf,
        #[arg(long)]
        json: bool,
    },
    IngestWriterAppend {
        #[arg(long)]
        payload_file: PathBuf,
        #[arg(long)]
        authority_store_id: String,
        #[arg(long)]
        authority_namespace: String,
        #[arg(long)]
        operator_id: String,
        #[arg(long)]
        writer_id: String,
        #[arg(long)]
        json: bool,
    },
    BenchmarkValidate {
        #[arg(long)]
        result: PathBuf,
    },
    BenchmarkGate {
        #[arg(long, value_parser = parse_benchmark_gate_level)]
        gate_level: BenchmarkGateLevel,
        #[arg(long, value_parser = parse_benchmark_backend)]
        backend: BenchmarkBackend,
        #[arg(long)]
        baseline: PathBuf,
        #[arg(long)]
        result: PathBuf,
        #[arg(long, default_value_t = 0.10)]
        max_regression_fraction: f64,
        #[arg(long)]
        json: bool,
    },
    ReadinessReport {
        #[arg(long)]
        evidence: PathBuf,
        #[arg(long)]
        require_release_artifacts: bool,
        #[arg(long)]
        first_e2e_artifacts: bool,
        #[arg(long)]
        dependency_governance_evidence: Option<PathBuf>,
        #[arg(long)]
        dependency_governance_manifest: Option<PathBuf>,
        #[arg(long)]
        release_commit: Option<String>,
        #[arg(long)]
        s3_release_benchmark_gate_evidence: Option<PathBuf>,
        #[arg(long)]
        production_gc_run_evidence: Option<PathBuf>,
        #[arg(long)]
        rustfs_production_gc_validation_evidence: Option<PathBuf>,
        #[arg(long)]
        ingest_writer_lifecycle_evidence: Option<PathBuf>,
        #[arg(long)]
        standing_runtime_product_evidence: Option<PathBuf>,
        #[arg(long)]
        s3_checkpoint_fault_matrix_evidence: Option<PathBuf>,
        #[arg(long)]
        hiqlite_restore_drill_evidence: Option<PathBuf>,
        #[arg(long)]
        upgrade_rollback_repair_gc_fault_matrix_evidence: Option<PathBuf>,
        #[arg(long)]
        query_output_isolation_evidence: Option<PathBuf>,
        #[arg(long)]
        security_release_provenance_evidence: Option<PathBuf>,
        #[arg(long)]
        remaining_release_readiness_evidence: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    DependencyGovernanceValidate {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long = "cargo-deny-json")]
        cargo_deny_json: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    ReleaseStatusValidate {
        #[arg(long = "status-matrix")]
        status_matrix: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::RelationCatalog { schema, adapter_id }) => {
            let request = relation_catalog_request(&schema, adapter_id)?;
            println!("{}", serde_json::to_string(&request)?);
        }
        Some(Command::CheckpointInspectLocal {
            object_store_dir,
            json,
        }) => {
            let store = local_object_store(&object_store_dir)?;
            let inspection = inspect_local_checkpoints(store)
                .await
                .context("failed to inspect local checkpoints")?;

            if json {
                println!("{}", format_checkpoint_inspection_json(&inspection)?);
            } else {
                print!("{}", format_checkpoint_inspection(&inspection));
            }
        }
        Some(Command::CheckpointRepairLatestLocal {
            object_store_dir,
            json,
        }) => {
            let store = local_object_store(&object_store_dir)?;
            let marker = repair_local_latest_checkpoint_marker(store)
                .await
                .context("failed to repair local latest checkpoint marker")?;

            if json {
                println!("{}", format_checkpoint_latest_repair_json(marker.as_ref())?);
            } else {
                print!("{}", format_checkpoint_latest_repair(marker.as_ref()));
            }
        }
        Some(Command::CheckpointRepairLocal {
            object_store_dir,
            json,
        }) => {
            let store = local_object_store(&object_store_dir)?;
            let report = repair_local_checkpoint_admin_records(store)
                .await
                .context("failed to repair local checkpoint admin records")?;

            if json {
                println!("{}", format_checkpoint_repair_json(&report)?);
            } else {
                print!("{}", format_checkpoint_repair(&report));
            }
        }
        Some(Command::GcPlanLocal {
            object_store_dir,
            retain_latest_manifests,
            json,
        }) => {
            let store = local_object_store(&object_store_dir)?;
            let plan = plan_local_garbage_collection(
                store,
                GarbageCollectionPolicy {
                    retain_latest_manifests,
                },
            )
            .await
            .context("failed to plan local garbage collection")?;

            if json {
                println!("{}", format_gc_plan_json(&plan)?);
            } else {
                print!("{}", format_gc_plan(&plan));
            }
        }
        Some(Command::GcExecuteLocal {
            object_store_dir,
            retain_latest_manifests,
            run_id,
            json,
        }) => {
            let store = local_object_store(&object_store_dir)?;
            let policy = GarbageCollectionPolicy {
                retain_latest_manifests,
            };
            let run = execute_local_garbage_collection(store, &run_id, policy)
                .await
                .context("failed to execute local garbage collection")?;

            if json {
                println!("{}", format_gc_run_json(&run)?);
            } else {
                print!("{}", format_gc_run(&run));
            }
        }
        Some(Command::GcExecuteS3Compatible {
            authority_store_id,
            retain_latest_manifests,
            run_id,
            json,
        }) => {
            let store = production_gc_authority_store_from_env()?;
            let policy = GarbageCollectionPolicy {
                retain_latest_manifests,
            };
            let run = execute_s3_compatible_garbage_collection(
                store,
                &authority_store_id,
                &run_id,
                policy,
            )
            .await
            .context("failed to execute S3-compatible garbage collection")?;

            if json {
                println!("{}", format_gc_run_json(&run)?);
            } else {
                print!("{}", format_gc_run(&run));
            }
        }
        Some(Command::GcSeedS3CompatibleFixture {
            authority_store_id,
            seed_id,
            json,
        }) => {
            let store = production_gc_authority_store_from_env()?;
            let artifact =
                seed_s3_compatible_gc_fixture(store, &authority_store_id, &seed_id).await?;

            if json {
                println!("{}", format_s3_compatible_gc_seed_fixture_json(&artifact)?);
            } else {
                print!("{}", format_s3_compatible_gc_seed_fixture(&artifact));
            }
        }
        Some(Command::GcProductionEvidence {
            deployment_id,
            authority_store_id,
            gc_run_id,
            json,
        }) => {
            validate_production_gc_authority_store_id(&authority_store_id)?;
            let store = production_gc_authority_store_from_env()
                .context("failed to construct S3-compatible production GC authority store")?;
            let artifact = generate_production_gc_run_evidence(
                store,
                deployment_id,
                authority_store_id,
                gc_run_id,
            )
            .await
            .context("failed to verify production GC run evidence")?;

            if json {
                println!("{}", format_production_gc_run_evidence_json(&artifact)?);
            } else {
                print!("{}", format_production_gc_run_evidence(&artifact));
            }
        }
        Some(Command::RustfsProductionGcEvidenceValidate {
            gate_evidence,
            seed_evidence,
            execute_evidence,
            production_evidence,
            json,
        }) => {
            let report = validate_rustfs_production_gc_evidence_family(
                &gate_evidence,
                &seed_evidence,
                &execute_evidence,
                &production_evidence,
            )?;
            if json {
                println!(
                    "{}",
                    format_rustfs_production_gc_evidence_report_json(&report)?
                );
            } else {
                print!("{}", format_rustfs_production_gc_evidence_report(&report));
            }
        }
        Some(Command::IngestWriterAppend {
            payload_file,
            authority_store_id,
            authority_namespace,
            operator_id,
            writer_id,
            json,
        }) => {
            let store = s3_compatible_authority_store_from_env()
                .context("failed to construct S3-compatible ingest writer authority store")?;
            let payload = fs::read(&payload_file).with_context(|| {
                format!(
                    "failed to read ingest payload from {}",
                    payload_file.display()
                )
            })?;
            let artifact = run_ingest_writer_append(
                store,
                IngestWriterAppendRequest {
                    authority_store_id,
                    authority_namespace,
                    operator_id,
                    writer_id,
                    payload: Bytes::from(payload),
                },
            )
            .await
            .context("failed to append ingest payload through checked writer runtime")?;

            if json {
                println!("{}", format_ingest_writer_append_json(&artifact)?);
            } else {
                print!("{}", format_ingest_writer_append(&artifact));
            }
        }
        Some(Command::BenchmarkValidate { result }) => {
            run_benchmark_gate(None, &result, None, None, None)?;
            println!("benchmark result valid");
        }
        Some(Command::BenchmarkGate {
            gate_level,
            backend,
            baseline,
            result,
            max_regression_fraction,
            json,
        }) => {
            let evidence = run_benchmark_gate(
                Some(&baseline),
                &result,
                Some(gate_level),
                Some(backend),
                Some(max_regression_fraction),
            )?;
            if json {
                println!("{}", serde_json::to_string_pretty(&evidence)?);
            } else {
                println!("benchmark gate passed");
            }
        }
        Some(Command::ReadinessReport {
            evidence,
            require_release_artifacts,
            first_e2e_artifacts,
            dependency_governance_evidence,
            dependency_governance_manifest,
            release_commit,
            s3_release_benchmark_gate_evidence,
            production_gc_run_evidence,
            rustfs_production_gc_validation_evidence,
            ingest_writer_lifecycle_evidence,
            standing_runtime_product_evidence,
            s3_checkpoint_fault_matrix_evidence,
            hiqlite_restore_drill_evidence,
            upgrade_rollback_repair_gc_fault_matrix_evidence,
            query_output_isolation_evidence,
            security_release_provenance_evidence,
            remaining_release_readiness_evidence,
            json,
        }) => {
            let artifacts = ReadinessReleaseArtifactPaths {
                require_release_artifacts,
                first_e2e_artifacts,
                dependency_governance_evidence,
                dependency_governance_manifest,
                release_commit,
                s3_release_benchmark_gate_evidence,
                production_gc_run_evidence,
                rustfs_production_gc_validation_evidence,
                ingest_writer_lifecycle_evidence,
                standing_runtime_product_evidence,
                s3_checkpoint_fault_matrix_evidence,
                hiqlite_restore_drill_evidence,
                upgrade_rollback_repair_gc_fault_matrix_evidence,
                query_output_isolation_evidence,
                security_release_provenance_evidence,
                remaining_release_readiness_evidence,
            };
            let report = read_readiness_report(&evidence, &artifacts)?;
            if json {
                println!("{}", report.to_json_pretty()?);
            } else {
                print!("{}", format_readiness_report(&report));
            }
            ensure_readiness_report_passes(&report)?;
        }
        Some(Command::DependencyGovernanceValidate {
            manifest,
            cargo_deny_json,
            json,
        }) => {
            if json && cargo_deny_json.is_none() {
                bail!("dependency-governance-validate --json requires --cargo-deny-json");
            }
            let evidence =
                validate_dependency_governance_file(&manifest, cargo_deny_json.as_deref())?;
            if json {
                println!("{}", serde_json::to_string_pretty(&evidence)?);
            } else {
                println!("dependency governance manifest valid");
            }
        }
        Some(Command::ReleaseStatusValidate { status_matrix }) => {
            validate_release_status_file(&status_matrix)?;
            println!("release status matrix valid");
        }
        None => {
            Cli::command().print_help()?;
            println!();
        }
    }

    Ok(())
}

#[derive(Debug, Serialize)]
struct RelationCatalogRequest {
    catalog: VelorixRelationCatalogV1,
}

fn relation_catalog_request(
    schema_path: &Path,
    adapter_id: String,
) -> anyhow::Result<RelationCatalogRequest> {
    let bytes = if schema_path == Path::new("-") {
        let mut bytes = Vec::new();
        io::stdin()
            .read_to_end(&mut bytes)
            .context("failed to read relation schema from stdin")?;
        bytes
    } else {
        fs::read(schema_path)
            .with_context(|| format!("failed to read relation schema {}", schema_path.display()))?
    };
    let schema: VelorixRelationSchemaV1 =
        serde_json::from_slice(&bytes).context("failed to parse VelorixRelationSchemaV1 JSON")?;
    let catalog = VelorixRelationCatalogV1::from_relation_schema(schema, adapter_id)
        .context("relation schema or adapter is not supported for ingest")?;
    Ok(RelationCatalogRequest { catalog })
}

#[derive(Debug, Default)]
struct ReadinessReleaseArtifactPaths {
    require_release_artifacts: bool,
    first_e2e_artifacts: bool,
    dependency_governance_evidence: Option<PathBuf>,
    dependency_governance_manifest: Option<PathBuf>,
    release_commit: Option<String>,
    s3_release_benchmark_gate_evidence: Option<PathBuf>,
    production_gc_run_evidence: Option<PathBuf>,
    rustfs_production_gc_validation_evidence: Option<PathBuf>,
    ingest_writer_lifecycle_evidence: Option<PathBuf>,
    standing_runtime_product_evidence: Option<PathBuf>,
    s3_checkpoint_fault_matrix_evidence: Option<PathBuf>,
    hiqlite_restore_drill_evidence: Option<PathBuf>,
    upgrade_rollback_repair_gc_fault_matrix_evidence: Option<PathBuf>,
    query_output_isolation_evidence: Option<PathBuf>,
    security_release_provenance_evidence: Option<PathBuf>,
    remaining_release_readiness_evidence: Option<PathBuf>,
}

fn read_readiness_report(
    path: &Path,
    artifacts: &ReadinessReleaseArtifactPaths,
) -> anyhow::Result<ProductionReadinessReportV1> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read readiness evidence from {}", path.display()))?;
    let evidence = ProductionReadinessEvidenceV1::from_json_str(&contents)
        .with_context(|| format!("failed to parse readiness evidence {}", path.display()))?;
    let report = if artifacts.first_e2e_artifacts {
        evidence
            .try_into_first_e2e_report()
            .map_err(anyhow::Error::msg)?
    } else {
        evidence.try_into_report().map_err(anyhow::Error::msg)?
    };
    validate_readiness_release_artifacts(
        artifacts,
        &report.deployment_id,
        &report.authority_store_id,
    )?;
    Ok(report)
}

fn validate_readiness_release_artifacts(
    artifacts: &ReadinessReleaseArtifactPaths,
    deployment_id: &str,
    authority_store_id: &str,
) -> anyhow::Result<()> {
    if artifacts.require_release_artifacts && artifacts.first_e2e_artifacts {
        bail!(
            "readiness-report cannot combine --require-release-artifacts with --first-e2e-artifacts"
        );
    }
    let release_artifacts_required = !artifacts.first_e2e_artifacts;

    if release_artifacts_required {
        require_artifact_path(
            "dependency-governance-evidence",
            &artifacts.dependency_governance_evidence,
        )?;
        require_artifact_path(
            "s3-release-benchmark-gate-evidence",
            &artifacts.s3_release_benchmark_gate_evidence,
        )?;
        require_artifact_path(
            "production-gc-run-evidence",
            &artifacts.production_gc_run_evidence,
        )?;
        require_artifact_path(
            "ingest-writer-lifecycle-evidence",
            &artifacts.ingest_writer_lifecycle_evidence,
        )?;
        require_artifact_path(
            "standing-runtime-product-evidence",
            &artifacts.standing_runtime_product_evidence,
        )?;
        require_artifact_path(
            "s3-checkpoint-fault-matrix-evidence",
            &artifacts.s3_checkpoint_fault_matrix_evidence,
        )?;
        require_artifact_path(
            "hiqlite-restore-drill-evidence",
            &artifacts.hiqlite_restore_drill_evidence,
        )?;
        require_artifact_path(
            "upgrade-rollback-repair-gc-fault-matrix-evidence",
            &artifacts.upgrade_rollback_repair_gc_fault_matrix_evidence,
        )?;
        require_artifact_path(
            "query-output-isolation-evidence",
            &artifacts.query_output_isolation_evidence,
        )?;
        require_artifact_path(
            "security-release-provenance-evidence",
            &artifacts.security_release_provenance_evidence,
        )?;
        require_artifact_path(
            "remaining-release-readiness-evidence",
            &artifacts.remaining_release_readiness_evidence,
        )?;
    } else if artifacts.first_e2e_artifacts {
        require_first_e2e_artifact_path(
            "dependency-governance-evidence",
            &artifacts.dependency_governance_evidence,
        )?;
        require_first_e2e_artifact_path(
            "s3-release-benchmark-gate-evidence",
            &artifacts.s3_release_benchmark_gate_evidence,
        )?;
        require_first_e2e_artifact_path(
            "production-gc-run-evidence",
            &artifacts.production_gc_run_evidence,
        )?;
        require_first_e2e_artifact_path(
            "ingest-writer-lifecycle-evidence",
            &artifacts.ingest_writer_lifecycle_evidence,
        )?;
        require_first_e2e_artifact_path(
            "standing-runtime-product-evidence",
            &artifacts.standing_runtime_product_evidence,
        )?;
    }

    if let Some(path) = &artifacts.dependency_governance_evidence {
        validate_dependency_governance_evidence_artifact(
            path,
            artifacts.release_commit.as_deref(),
            artifacts.dependency_governance_manifest.as_deref(),
        )?;
    }
    if let Some(path) = &artifacts.s3_release_benchmark_gate_evidence {
        validate_s3_release_benchmark_gate_evidence_artifact(path)?;
    }
    if let Some(path) = &artifacts.production_gc_run_evidence {
        validate_production_gc_run_evidence_artifact(path, deployment_id, authority_store_id)?;
    }
    if let Some(path) = &artifacts.ingest_writer_lifecycle_evidence {
        validate_ingest_writer_lifecycle_evidence_artifact(
            path,
            deployment_id,
            authority_store_id,
        )?;
    }
    if let Some(path) = &artifacts.standing_runtime_product_evidence {
        let mode = if release_artifacts_required {
            StandingRuntimeProductEvidenceMode::Release
        } else {
            StandingRuntimeProductEvidenceMode::FirstE2e
        };
        validate_standing_runtime_product_evidence_artifact(
            path,
            deployment_id,
            authority_store_id,
            mode,
            artifacts.release_commit.as_deref(),
        )?;
    }
    if release_artifacts_required {
        require_artifact_path(
            "rustfs-production-gc-validation-evidence",
            &artifacts.rustfs_production_gc_validation_evidence,
        )?;
    } else if artifacts.first_e2e_artifacts {
        require_first_e2e_artifact_path(
            "rustfs-production-gc-validation-evidence",
            &artifacts.rustfs_production_gc_validation_evidence,
        )?;
    }
    if let Some(path) = &artifacts.rustfs_production_gc_validation_evidence {
        validate_rustfs_production_gc_validation_evidence_artifact(
            path,
            artifacts.production_gc_run_evidence.as_deref(),
            deployment_id,
            authority_store_id,
        )?;
    }
    if let Some(path) = &artifacts.s3_checkpoint_fault_matrix_evidence {
        validate_critique_release_evidence_artifact(
            path,
            &["s3_compatible_checkpoint_fault_matrix"],
            deployment_id,
            authority_store_id,
            artifacts.release_commit.as_deref(),
            release_artifacts_required,
            &[
                "live_s3_compatible",
                "delayed_visibility_cases_passed",
                "retry_fault_cases_passed",
                "mixed_checkpoint_publish_prevented",
                "object_refs_verified",
            ],
            &[],
            &[],
            &[
                "object_write_failure",
                "verification_read_failure",
                "manifest_write_failure",
                "metadata_cas_failure",
                "delayed_visibility",
                "retry_after_failure",
            ],
        )?;
    }
    if let Some(path) = &artifacts.hiqlite_restore_drill_evidence {
        validate_critique_release_evidence_artifact(
            path,
            &[
                "hiqlite_total_voter_loss_restore_drill",
                "hiqlite_no_pvc_three_voter_backup_restore",
            ],
            deployment_id,
            authority_store_id,
            artifacts.release_commit.as_deref(),
            release_artifacts_required,
            &[
                "no_pvc",
                "total_voter_loss_exercised",
                "restored_from_object_store_backup",
                "acknowledged_metadata_writes_survived",
                "catalog_verified",
                "owner_epoch_verified",
                "checkpoint_pointer_verified",
                "post_restore_ingest_query_verified",
                "restore_drill_verified",
            ],
            &[],
            &[
                "object_store_backup",
                "total_voter_loss_log",
                "restore_log",
                "metadata_write_survival",
                "post_restore_ingest_query",
            ],
            &[],
        )?;
    }
    if let Some(path) = &artifacts.upgrade_rollback_repair_gc_fault_matrix_evidence {
        validate_critique_release_evidence_artifact(
            path,
            &["upgrade_rollback_repair_gc_fault_matrix"],
            deployment_id,
            authority_store_id,
            artifacts.release_commit.as_deref(),
            release_artifacts_required,
            &[
                "live_upgrade_rollback_repair_gc_matrix",
                "upgrade_verified",
                "rollback_verified",
                "repair_verified",
                "gc_reachability_verified",
                "acknowledged_data_preserved",
                "no_source_query_recomputation",
            ],
            &[],
            &[],
            &[
                "rolling_upgrade",
                "rollback_after_upgrade",
                "corrupt_latest_checkpoint_repair",
                "gc_concurrent_with_query",
                "gc_concurrent_with_compaction",
                "gc_concurrent_with_recovery",
                "gc_concurrent_with_checkpoint_publication",
                "gc_retains_repair_roots",
            ],
        )?;
    }
    if let Some(path) = &artifacts.query_output_isolation_evidence {
        validate_critique_release_evidence_artifact(
            path,
            &["query_output_isolation"],
            deployment_id,
            authority_store_id,
            artifacts.release_commit.as_deref(),
            release_artifacts_required,
            &[
                "live_release_query_isolation",
                "cold_query_succeeded",
                "object_store_audit_no_source_reads",
                "object_store_audit_no_source_writes",
                "object_store_audit_no_durable_writes",
                "materialized_output_read_verified",
                "no_source_query_recomputation",
            ],
            &[
                "query_pod_source_ingest_prefix_read_access",
                "query_pod_metadata_write_access",
            ],
            &[
                "query_pod_iam_policy",
                "cold_query_log",
                "object_store_audit_log",
                "materialized_output_read",
            ],
            &[],
        )?;
    }
    if let Some(path) = &artifacts.security_release_provenance_evidence {
        validate_critique_release_evidence_artifact(
            path,
            &["security_release_provenance"],
            deployment_id,
            authority_store_id,
            artifacts.release_commit.as_deref(),
            release_artifacts_required,
            &[
                "mandatory_api_auth",
                "mandatory_metadata_auth",
                "tenant_authorization_verified",
                "tls_verified",
                "secret_rotation_verified",
                "body_limits_verified",
                "rate_limits_verified",
                "object_prefix_isolation_verified",
                "negative_cross_tenant_tests_passed",
                "clean_source_revision_verified",
                "exact_deployed_image_digests_verified",
                "sbom_attached",
                "dependency_policy_passed",
                "immutable_test_evidence_attached",
            ],
            &[],
            &[
                "api_auth_test",
                "metadata_auth_test",
                "tenant_authorization_test",
                "tls_attestation",
                "secret_rotation_test",
                "limit_tests",
                "object_prefix_isolation_test",
                "cross_tenant_negative_tests",
                "sbom",
                "dependency_policy",
                "immutable_test_evidence",
            ],
            &[],
        )?;
        validate_security_release_provenance_identity(
            path,
            artifacts.release_commit.as_deref(),
            release_artifacts_required,
        )?;
    }
    if let Some(path) = &artifacts.remaining_release_readiness_evidence {
        validate_critique_release_evidence_artifact(
            path,
            &["remaining_release_readiness"],
            deployment_id,
            authority_store_id,
            artifacts.release_commit.as_deref(),
            release_artifacts_required,
            &[
                "release_image_contract_tests_passed",
                "versioned_openapi_contract_verified",
                "no_conflicting_accepted_contracts",
                "sql_admission_corpus_generated",
                "sql_admission_corpus_covers_unsupported_datafusion_plan_nodes",
                "sql_admission_corpus_covers_unsupported_datafusion_expression_nodes",
                "unsupported_sql_leaves_no_persisted_view_metadata",
                "unsupported_sql_leaves_no_runtime_binding",
                "sql_admission_mutation_ci_failure_verified",
                "persistent_write_boundary_crash_matrix_passed",
                "crash_matrix_covers_one_view",
                "crash_matrix_covers_multiple_affected_views",
                "crash_matrix_covers_joins",
                "crash_matrix_covers_compaction",
                "replay_duplicate_reordered_gapped_retried_batches_verified",
                "replay_live_crash_clean_outputs_identical",
                "replay_checkpoint_hashes_identical",
                "non_contiguous_input_never_advances_frontier",
                "join_frontier_spec_verified",
                "concurrent_two_input_ingest_crash_leader_handoff_verified",
                "output_manifests_record_exact_input_frontiers",
                "published_limits_verified",
                "multi_day_supported_object_store_soak_passed",
            ],
            &[],
            &[
                "release_image_contract_tests",
                "openapi_contract",
                "sql_admission_corpus",
                "crash_matrix",
                "replay_determinism",
                "join_frontier",
                "scale_soak",
            ],
            &[],
        )?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_critique_release_evidence_artifact(
    path: &Path,
    expected_evidence_kinds: &[&str],
    deployment_id: &str,
    authority_store_id: &str,
    release_commit: Option<&str>,
    release_artifacts_required: bool,
    required_true_fields: &[&str],
    required_false_fields: &[&str],
    required_evidence_refs: &[&str],
    required_scenarios: &[&str],
) -> anyhow::Result<()> {
    let value: serde_json::Value = read_json_artifact(path)?;
    let artifact = value
        .get("s3_compatible_test_status")
        .or_else(|| value.get("gc_status"))
        .unwrap_or(&value);

    let evidence_kind = require_json_str(path, artifact, "/evidence_kind")?;
    if !expected_evidence_kinds.contains(&evidence_kind) {
        bail!(
            "{} has evidence_kind {}, expected one of {}",
            path.display(),
            evidence_kind,
            expected_evidence_kinds.join(", ")
        );
    }
    if require_json_str(path, artifact, "/status")? != "pass" {
        bail!("{} critique release evidence is not pass", path.display());
    }
    validate_critique_release_evidence_kind_specific_fields(path, artifact, evidence_kind)?;
    if require_json_str(path, artifact, "/deployment_id")? != deployment_id {
        bail!(
            "{} deployment_id does not match readiness evidence",
            path.display()
        );
    }
    let observed_authority_store_id = require_json_str(path, artifact, "/authority_store_id")?;
    if observed_authority_store_id != authority_store_id
        || !observed_authority_store_id.starts_with("s3://")
    {
        bail!(
            "{} authority_store_id does not match readiness evidence s3:// authority",
            path.display()
        );
    }
    validate_critique_release_identity(path, artifact, release_commit, release_artifacts_required)?;
    for field in required_true_fields {
        require_json_true(path, artifact, &format!("/{field}"))?;
    }
    for field in required_false_fields {
        require_json_false(path, artifact, &format!("/{field}"))?;
    }
    if !required_evidence_refs.is_empty() {
        let refs = artifact
            .get("evidence_refs")
            .and_then(serde_json::Value::as_object)
            .with_context(|| format!("{} missing object /evidence_refs", path.display()))?;
        for field in required_evidence_refs {
            match refs.get(*field).and_then(serde_json::Value::as_str) {
                Some(value) if !value.trim().is_empty() => {
                    validate_release_evidence_ref(path, value, &format!("evidence_refs.{field}"))?;
                }
                _ => bail!(
                    "{} critique release evidence missing nonempty evidence_refs.{field}",
                    path.display()
                ),
            }
        }
    }
    if !required_scenarios.is_empty() {
        let scenarios = artifact
            .get("scenarios")
            .and_then(serde_json::Value::as_array)
            .with_context(|| format!("{} missing array /scenarios", path.display()))?;
        let mut observed = BTreeSet::new();
        for (index, scenario) in scenarios.iter().enumerate() {
            let name = scenario
                .get("name")
                .and_then(serde_json::Value::as_str)
                .filter(|name| !name.trim().is_empty())
                .with_context(|| {
                    format!(
                        "{} critique release evidence missing scenarios[{index}].name",
                        path.display()
                    )
                })?;
            if scenario.get("status").and_then(serde_json::Value::as_str) != Some("pass") {
                bail!(
                    "{} critique release evidence requires scenarios[{index}].status=pass",
                    path.display()
                );
            }
            let evidence_ref = scenario
                .get("evidence")
                .and_then(serde_json::Value::as_str)
                .filter(|evidence| !evidence.trim().is_empty())
                .with_context(|| {
                    format!(
                        "{} critique release evidence missing nonempty scenarios[{index}].evidence",
                        path.display()
                    )
                })?;
            validate_release_evidence_ref(
                path,
                evidence_ref,
                &format!("scenarios[{index}].evidence"),
            )?;
            observed.insert(name);
        }
        for scenario in required_scenarios {
            if !observed.contains(scenario) {
                bail!(
                    "{} critique release evidence missing scenario {scenario}",
                    path.display()
                );
            }
        }
    }

    let text = serde_json::to_string(&value)?.to_ascii_lowercase();
    reject_critique_release_forbidden_tokens(path, evidence_kind, &text)?;

    Ok(())
}

fn validate_critique_release_identity(
    path: &Path,
    artifact: &serde_json::Value,
    release_commit: Option<&str>,
    release_artifacts_required: bool,
) -> anyhow::Result<()> {
    if !release_artifacts_required {
        return Ok(());
    }
    let Some(release_commit) = release_commit else {
        bail!(
            "{} critique release evidence requires --release-commit",
            path.display()
        );
    };
    validate_full_git_commit_sha(path, release_commit, "release_commit")?;
    let source_revision = require_json_str(path, artifact, "/source_revision")?;
    validate_full_git_commit_sha(path, source_revision, "source_revision")?;
    if source_revision != release_commit {
        bail!(
            "{} critique release evidence source_revision does not match release_commit",
            path.display()
        );
    }
    let digests = artifact
        .get("deployed_image_digests")
        .and_then(serde_json::Value::as_object)
        .with_context(|| format!("{} missing object /deployed_image_digests", path.display()))?;
    for role in ["velorix-api", "velorix-meta"] {
        let digest = digests
            .get(role)
            .and_then(serde_json::Value::as_str)
            .with_context(|| format!("{} missing deployed_image_digests.{role}", path.display()))?;
        validate_sha256_digest(path, digest, &format!("deployed_image_digests.{role}"))?;
    }
    Ok(())
}

fn validate_critique_release_evidence_kind_specific_fields(
    path: &Path,
    artifact: &serde_json::Value,
    evidence_kind: &str,
) -> anyhow::Result<()> {
    match evidence_kind {
        "s3_compatible_checkpoint_fault_matrix" => {
            let backend = artifact
                .get("backend")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let provider = artifact
                .get("provider")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            if !format!("{backend} {provider}").contains("s3-compatible") {
                bail!(
                    "{} S3 checkpoint fault matrix backend or provider must identify s3-compatible",
                    path.display()
                );
            }
        }
        "hiqlite_total_voter_loss_restore_drill" | "hiqlite_no_pvc_three_voter_backup_restore" => {
            if require_json_u64(path, artifact, "/voter_count")? != 3 {
                bail!(
                    "{} Hiqlite restore drill requires voter_count=3",
                    path.display()
                );
            }
        }
        "query_output_isolation"
            if require_json_str(path, artifact, "/query_authority")?
                != "published_materialized_output" =>
        {
            bail!(
                "{} query output isolation requires query_authority=published_materialized_output",
                path.display()
            );
        }
        _ => {}
    }

    Ok(())
}

fn reject_critique_release_forbidden_tokens(
    path: &Path,
    evidence_kind: &str,
    text: &str,
) -> anyhow::Result<()> {
    for token in [
        "local-only",
        "local_only",
        "local only",
        "local smoke",
        "local_smoke",
        "emulator",
        "fake",
        "synthetic",
        "placeholder",
        "todo",
        "tbd",
    ] {
        if text.contains(token) {
            bail!(
                "{} critique release evidence must not contain {token}",
                path.display()
            );
        }
    }

    for token in match evidence_kind {
        "s3_compatible_checkpoint_fault_matrix" => &[
            "rustfs-only",
            "localstack",
            "minio",
            "moto",
            "localhost",
            "127.0.0.1",
        ][..],
        "hiqlite_total_voter_loss_restore_drill" | "hiqlite_no_pvc_three_voter_backup_restore" => {
            &[
                "persistentvolumeclaim",
                "volumeclaimtemplates",
                "volumeclaim",
            ]
        }
        "security_release_provenance" => &[
            "127.0.0.1",
            "::1",
            "changeme",
            "dummy",
            "example.com",
            "example.net",
            "example.org",
            "localhost",
            "localstack",
            "lorem ipsum",
            "minio",
            "mock",
            "moto",
            "replace-me",
            "replace_me",
            "replace_with",
        ],
        "remaining_release_readiness" => &["mock"],
        _ => &[],
    } {
        if text.contains(token) {
            bail!(
                "{} critique release evidence must not contain {token}",
                path.display()
            );
        }
    }

    if matches!(
        evidence_kind,
        "hiqlite_total_voter_loss_restore_drill" | "hiqlite_no_pvc_three_voter_backup_restore"
    ) && contains_json_word_token(text, "pvc")
    {
        bail!(
            "{} critique release evidence must not contain pvc",
            path.display()
        );
    }

    Ok(())
}

fn contains_json_word_token(text: &str, token: &str) -> bool {
    text.match_indices(token).any(|(index, _)| {
        let before = text[..index].chars().next_back();
        let after = text[index + token.len()..].chars().next();
        !matches!(before, Some(ch) if ch.is_ascii_alphanumeric() || ch == '_')
            && !matches!(after, Some(ch) if ch.is_ascii_alphanumeric() || ch == '_')
    })
}

fn validate_release_evidence_ref(path: &Path, reference: &str, label: &str) -> anyhow::Result<()> {
    let reference = reference.trim();
    if has_uri_scheme(reference) {
        if extract_sha256_identity(reference).is_some() {
            return Ok(());
        }
        bail!(
            "{} {label} immutable URI must include sha256/digest identity",
            path.display()
        );
    }

    let local_ref = reference
        .split(['#', '?'])
        .next()
        .unwrap_or(reference)
        .trim();
    let local_path = if Path::new(local_ref).is_absolute() {
        PathBuf::from(local_ref)
    } else {
        path.parent()
            .unwrap_or_else(|| Path::new("."))
            .join(local_ref)
    };
    if !local_path.is_file() {
        bail!(
            "{} {label} local evidence file does not exist: {}",
            path.display(),
            local_path.display()
        );
    }

    let Some(expected_sha256) =
        extract_sha256_identity(reference).or_else(|| sidecar_sha256(&local_path).ok().flatten())
    else {
        bail!(
            "{} {label} local evidence file must include inline or sidecar sha256 identity",
            path.display()
        );
    };
    let actual_sha256 = file_sha256(&local_path)?;
    if actual_sha256 != expected_sha256 {
        bail!(
            "{} {label} sha256 mismatch: expected {expected_sha256}, got {actual_sha256}",
            path.display()
        );
    }

    Ok(())
}

fn has_uri_scheme(reference: &str) -> bool {
    let Some(index) = reference.find("://") else {
        return false;
    };
    let scheme = &reference[..index];
    !scheme.is_empty()
        && scheme
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '.' | '-'))
}

fn extract_sha256_identity(reference: &str) -> Option<String> {
    let lower = reference.to_ascii_lowercase();
    if !lower.contains("sha256") && !lower.contains("digest") {
        return None;
    }
    let chars: Vec<char> = lower.chars().collect();
    chars
        .windows(64)
        .find(|window| window.iter().all(|ch| ch.is_ascii_hexdigit()))
        .map(|window| window.iter().collect())
}

fn sidecar_sha256(path: &Path) -> anyhow::Result<Option<String>> {
    for suffix in [".sha256", ".sha256sum", ".sha256.txt"] {
        let sidecar = PathBuf::from(format!("{}{}", path.display(), suffix));
        if sidecar.is_file() {
            let contents = fs::read_to_string(&sidecar)
                .with_context(|| format!("failed to read {}", sidecar.display()))?;
            if let Some(value) = first_sha256_hex(&contents) {
                return Ok(Some(value));
            }
        }
    }
    Ok(None)
}

fn first_sha256_hex(contents: &str) -> Option<String> {
    let lower = contents.to_ascii_lowercase();
    let chars: Vec<char> = lower.chars().collect();
    chars
        .windows(64)
        .find(|window| window.iter().all(|ch| ch.is_ascii_hexdigit()))
        .map(|window| window.iter().collect())
}

fn file_sha256(path: &Path) -> anyhow::Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(&bytes)))
}

fn validate_security_release_provenance_identity(
    path: &Path,
    release_commit: Option<&str>,
    release_artifacts_required: bool,
) -> anyhow::Result<()> {
    let value: serde_json::Value = read_json_artifact(path)?;
    let source_revision = require_json_str(path, &value, "/source_revision")?;
    if source_revision.len() != 40
        || !source_revision
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
    {
        bail!(
            "{} security release provenance source_revision must be a 40-character git SHA",
            path.display()
        );
    }
    if release_artifacts_required {
        let Some(release_commit) = release_commit else {
            bail!(
                "{} security release provenance requires --release-commit",
                path.display()
            );
        };
        if release_commit.len() != 40
            || !release_commit
                .chars()
                .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
        {
            bail!(
                "{} security release provenance release_commit must be a 40-character git SHA",
                path.display()
            );
        }
        if source_revision != release_commit {
            bail!(
                "{} security release provenance source_revision does not match release_commit",
                path.display()
            );
        }
    }
    let digests = value
        .get("deployed_image_digests")
        .and_then(serde_json::Value::as_object)
        .with_context(|| format!("{} missing object /deployed_image_digests", path.display()))?;
    for role in ["velorix-api", "velorix-meta"] {
        let digest = digests
            .get(role)
            .and_then(serde_json::Value::as_str)
            .with_context(|| format!("{} missing deployed_image_digests.{role}", path.display()))?;
        let hash = digest.strip_prefix("sha256:").unwrap_or_default();
        if hash.len() != 64
            || !hash
                .chars()
                .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
        {
            bail!(
                "{} deployed_image_digests.{role} must be a sha256 digest",
                path.display()
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn write_artifact(name: &str, value: serde_json::Value) -> PathBuf {
        let mut path = env::temp_dir();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        path.push(format!("velorix-cli-{name}-{nonce}.json"));
        fs::write(
            &path,
            serde_json::to_vec(&value).expect("serialize artifact"),
        )
        .expect("write artifact");
        path
    }

    fn evidence_uri(name: &str) -> String {
        format!("s3://release-evidence/{name}?sha256={}", "c".repeat(64))
    }

    fn valid_relation_schema() -> serde_json::Value {
        json!({
            "relation_id": "measurements",
            "relation_name": "measurements",
            "relation_version": "v1",
            "columns": [
                {
                    "column_id": "sensor_id",
                    "name": "sensor_id",
                    "logical_type": {"kind": "utf8"},
                    "physical_arrow_type": {"kind": "utf8"},
                    "nullable": false,
                    "ordinal": 0,
                    "semantic_role": "primary_key"
                },
                {
                    "column_id": "reading",
                    "name": "reading",
                    "logical_type": {"kind": "int64"},
                    "physical_arrow_type": {"kind": "int64"},
                    "nullable": false,
                    "ordinal": 1,
                    "semantic_role": "value"
                },
                {
                    "column_id": "weight",
                    "name": "weight",
                    "logical_type": {"kind": "int64"},
                    "physical_arrow_type": {"kind": "int64"},
                    "nullable": false,
                    "ordinal": 2,
                    "semantic_role": "weight"
                }
            ],
            "primary_key_column_ids": ["sensor_id"],
            "weight_column_id": "weight",
            "allowed_operations": ["insert", "delete"],
            "event_time_column_id": null
        })
    }

    #[test]
    fn relation_catalog_command_builds_a_valid_api_request() {
        let path = write_artifact("relation-schema", valid_relation_schema());
        let request = relation_catalog_request(
            &path,
            velorix_core::relation::CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
        )
        .expect("build relation catalog request");

        request.catalog.validate_ingest_adapter_scope().unwrap();
        assert_eq!(
            request.catalog.schema_fingerprint,
            request.catalog.incremental_relation.schema_fingerprint
        );
        let value = serde_json::to_value(request).unwrap();
        assert_eq!(
            value["catalog"]["relation_schema"]["relation_id"],
            "measurements"
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn relation_catalog_command_rejects_unknown_fields_and_adapters() {
        let mut schema = valid_relation_schema();
        schema["unexpected"] = json!(true);
        let unknown_field_path = write_artifact("relation-schema-unknown-field", schema);
        assert!(relation_catalog_request(
            &unknown_field_path,
            velorix_core::relation::CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string()
        )
        .is_err());

        let valid_path = write_artifact("relation-schema-unknown-adapter", valid_relation_schema());
        assert!(relation_catalog_request(&valid_path, "unknown-adapter".to_string()).is_err());
        fs::remove_file(unknown_field_path).unwrap();
        fs::remove_file(valid_path).unwrap();
    }

    #[test]
    fn relation_catalog_command_requires_an_explicit_adapter() {
        let error =
            Cli::try_parse_from(["velorix-cli", "relation-catalog", "--schema", "schema.json"])
                .unwrap_err();
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    fn add_release_identity(
        mut artifact: serde_json::Value,
        source_revision: &str,
    ) -> serde_json::Value {
        artifact["source_revision"] = json!(source_revision);
        artifact["deployed_image_digests"] = json!({
            "velorix-api": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "velorix-meta": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        });
        artifact
    }

    fn valid_s3_checkpoint_fault_matrix() -> serde_json::Value {
        json!({
            "evidence_kind": "s3_compatible_checkpoint_fault_matrix",
            "status": "pass",
            "deployment_id": "release-prod",
            "authority_store_id": "s3://velorix-release/checkpoints",
            "backend": "external-s3-compatible",
            "provider": "s3-compatible",
            "live_s3_compatible": true,
            "delayed_visibility_cases_passed": true,
            "retry_fault_cases_passed": true,
            "mixed_checkpoint_publish_prevented": true,
            "object_refs_verified": true,
            "scenarios": [
                {"name": "object_write_failure", "status": "pass", "evidence": evidence_uri("object-write.json")},
                {"name": "verification_read_failure", "status": "pass", "evidence": evidence_uri("verification-read.json")},
                {"name": "manifest_write_failure", "status": "pass", "evidence": evidence_uri("manifest-write.json")},
                {"name": "metadata_cas_failure", "status": "pass", "evidence": evidence_uri("metadata-cas.json")},
                {"name": "delayed_visibility", "status": "pass", "evidence": evidence_uri("delayed-visibility.json")},
                {"name": "retry_after_failure", "status": "pass", "evidence": evidence_uri("retry-after-failure.json")}
            ]
        })
    }

    fn valid_hiqlite_restore_drill() -> serde_json::Value {
        json!({
            "evidence_kind": "hiqlite_total_voter_loss_restore_drill",
            "status": "pass",
            "deployment_id": "release-prod",
            "authority_store_id": "s3://velorix-release/checkpoints",
            "no_pvc": true,
            "voter_count": 3,
            "total_voter_loss_exercised": true,
            "restored_from_object_store_backup": true,
            "acknowledged_metadata_writes_survived": true,
            "catalog_verified": true,
            "owner_epoch_verified": true,
            "checkpoint_pointer_verified": true,
            "post_restore_ingest_query_verified": true,
            "restore_drill_verified": true,
            "evidence_refs": {
                "object_store_backup": evidence_uri("object-store-backup.json"),
                "total_voter_loss_log": evidence_uri("total-voter-loss.log"),
                "restore_log": evidence_uri("restore.log"),
                "metadata_write_survival": evidence_uri("metadata-write-survival.json"),
                "post_restore_ingest_query": evidence_uri("post-restore-ingest-query.json")
            }
        })
    }

    fn valid_upgrade_rollback_repair_gc_matrix() -> serde_json::Value {
        json!({
            "evidence_kind": "upgrade_rollback_repair_gc_fault_matrix",
            "status": "pass",
            "deployment_id": "release-prod",
            "authority_store_id": "s3://velorix-release/checkpoints",
            "live_upgrade_rollback_repair_gc_matrix": true,
            "upgrade_verified": true,
            "rollback_verified": true,
            "repair_verified": true,
            "gc_reachability_verified": true,
            "acknowledged_data_preserved": true,
            "no_source_query_recomputation": true,
            "scenarios": [
                {"name": "rolling_upgrade", "status": "pass", "evidence": evidence_uri("rolling-upgrade.json")},
                {"name": "rollback_after_upgrade", "status": "pass", "evidence": evidence_uri("rollback.json")},
                {"name": "corrupt_latest_checkpoint_repair", "status": "pass", "evidence": evidence_uri("repair.json")},
                {"name": "gc_concurrent_with_query", "status": "pass", "evidence": evidence_uri("gc-query.json")},
                {"name": "gc_concurrent_with_compaction", "status": "pass", "evidence": evidence_uri("gc-compaction.json")},
                {"name": "gc_concurrent_with_recovery", "status": "pass", "evidence": evidence_uri("gc-recovery.json")},
                {"name": "gc_concurrent_with_checkpoint_publication", "status": "pass", "evidence": evidence_uri("gc-checkpoint.json")},
                {"name": "gc_retains_repair_roots", "status": "pass", "evidence": evidence_uri("gc-retains-repair-roots.json")}
            ]
        })
    }

    fn valid_query_output_isolation() -> serde_json::Value {
        json!({
            "evidence_kind": "query_output_isolation",
            "status": "pass",
            "deployment_id": "release-prod",
            "authority_store_id": "s3://velorix-release/checkpoints",
            "live_release_query_isolation": true,
            "query_authority": "published_materialized_output",
            "cold_query_succeeded": true,
            "query_pod_source_ingest_prefix_read_access": false,
            "query_pod_metadata_write_access": false,
            "object_store_audit_no_source_reads": true,
            "object_store_audit_no_source_writes": true,
            "object_store_audit_no_durable_writes": true,
            "materialized_output_read_verified": true,
            "no_source_query_recomputation": true,
            "evidence_refs": {
                "query_pod_iam_policy": evidence_uri("query-pod-iam-policy.json"),
                "cold_query_log": evidence_uri("cold-query.log"),
                "object_store_audit_log": evidence_uri("object-store-audit.log"),
                "materialized_output_read": evidence_uri("materialized-output-read.json")
            }
        })
    }

    fn valid_security_release_provenance() -> serde_json::Value {
        json!({
            "evidence_kind": "security_release_provenance",
            "status": "pass",
            "deployment_id": "release-prod",
            "authority_store_id": "s3://velorix-release/checkpoints",
            "mandatory_api_auth": true,
            "mandatory_metadata_auth": true,
            "tenant_authorization_verified": true,
            "tls_verified": true,
            "secret_rotation_verified": true,
            "body_limits_verified": true,
            "rate_limits_verified": true,
            "object_prefix_isolation_verified": true,
            "negative_cross_tenant_tests_passed": true,
            "clean_source_revision_verified": true,
            "exact_deployed_image_digests_verified": true,
            "sbom_attached": true,
            "dependency_policy_passed": true,
            "immutable_test_evidence_attached": true,
            "source_revision": "0123456789abcdef0123456789abcdef01234567",
            "deployed_image_digests": {
                "velorix-api": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "velorix-meta": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            },
            "evidence_refs": {
                "api_auth_test": evidence_uri("api-auth.json"),
                "metadata_auth_test": evidence_uri("metadata-auth.json"),
                "tenant_authorization_test": evidence_uri("tenant-auth.json"),
                "tls_attestation": evidence_uri("tls.json"),
                "secret_rotation_test": evidence_uri("secret-rotation.json"),
                "limit_tests": evidence_uri("limits.json"),
                "object_prefix_isolation_test": evidence_uri("object-prefix-isolation.json"),
                "cross_tenant_negative_tests": evidence_uri("cross-tenant-negative.json"),
                "sbom": evidence_uri("sbom.spdx.json"),
                "dependency_policy": evidence_uri("dependency-policy.json"),
                "immutable_test_evidence": evidence_uri("immutable-test-evidence.json")
            }
        })
    }

    fn valid_remaining_release_readiness() -> serde_json::Value {
        json!({
            "evidence_kind": "remaining_release_readiness",
            "status": "pass",
            "deployment_id": "release-prod",
            "authority_store_id": "s3://velorix-release/checkpoints",
            "release_image_contract_tests_passed": true,
            "versioned_openapi_contract_verified": true,
            "no_conflicting_accepted_contracts": true,
            "sql_admission_corpus_generated": true,
            "sql_admission_corpus_covers_unsupported_datafusion_plan_nodes": true,
            "sql_admission_corpus_covers_unsupported_datafusion_expression_nodes": true,
            "unsupported_sql_leaves_no_persisted_view_metadata": true,
            "unsupported_sql_leaves_no_runtime_binding": true,
            "sql_admission_mutation_ci_failure_verified": true,
            "persistent_write_boundary_crash_matrix_passed": true,
            "crash_matrix_covers_one_view": true,
            "crash_matrix_covers_multiple_affected_views": true,
            "crash_matrix_covers_joins": true,
            "crash_matrix_covers_compaction": true,
            "replay_duplicate_reordered_gapped_retried_batches_verified": true,
            "replay_live_crash_clean_outputs_identical": true,
            "replay_checkpoint_hashes_identical": true,
            "non_contiguous_input_never_advances_frontier": true,
            "join_frontier_spec_verified": true,
            "concurrent_two_input_ingest_crash_leader_handoff_verified": true,
            "output_manifests_record_exact_input_frontiers": true,
            "published_limits_verified": true,
            "multi_day_supported_object_store_soak_passed": true,
            "evidence_refs": {
                "release_image_contract_tests": evidence_uri("release-image-contract-tests.json"),
                "openapi_contract": evidence_uri("openapi-contract.json"),
                "sql_admission_corpus": evidence_uri("sql-admission-corpus.json"),
                "crash_matrix": evidence_uri("crash-matrix.json"),
                "replay_determinism": evidence_uri("replay-determinism.json"),
                "join_frontier": evidence_uri("join-frontier.json"),
                "scale_soak": evidence_uri("scale-soak.json")
            }
        })
    }

    #[test]
    fn s3_checkpoint_fault_matrix_rejects_non_s3_compatible_backend() {
        let mut artifact = valid_s3_checkpoint_fault_matrix();
        artifact["backend"] = json!("rustfs-only");
        artifact["provider"] = json!("release-object-store");
        let path = write_artifact("s3-checkpoint-fault-matrix", artifact);

        let result = validate_critique_release_evidence_artifact(
            &path,
            &["s3_compatible_checkpoint_fault_matrix"],
            "release-prod",
            "s3://velorix-release/checkpoints",
            None,
            false,
            &[
                "live_s3_compatible",
                "delayed_visibility_cases_passed",
                "retry_fault_cases_passed",
                "mixed_checkpoint_publish_prevented",
                "object_refs_verified",
            ],
            &[],
            &[],
            &[
                "object_write_failure",
                "verification_read_failure",
                "manifest_write_failure",
                "metadata_cas_failure",
                "delayed_visibility",
                "retry_after_failure",
            ],
        );

        fs::remove_file(path).ok();
        assert!(result.is_err());
    }

    #[test]
    fn critique_release_evidence_requires_release_identity_when_release_artifacts_are_required() {
        let path = write_artifact(
            "s3-checkpoint-fault-matrix-missing-release-identity",
            valid_s3_checkpoint_fault_matrix(),
        );

        let result = validate_critique_release_evidence_artifact(
            &path,
            &["s3_compatible_checkpoint_fault_matrix"],
            "release-prod",
            "s3://velorix-release/checkpoints",
            Some("0123456789abcdef0123456789abcdef01234567"),
            true,
            &[
                "live_s3_compatible",
                "delayed_visibility_cases_passed",
                "retry_fault_cases_passed",
                "mixed_checkpoint_publish_prevented",
                "object_refs_verified",
            ],
            &[],
            &[],
            &[
                "object_write_failure",
                "verification_read_failure",
                "manifest_write_failure",
                "metadata_cas_failure",
                "delayed_visibility",
                "retry_after_failure",
            ],
        );

        fs::remove_file(path).ok();
        assert!(result
            .expect_err("missing release identity should fail")
            .to_string()
            .contains("/source_revision"));
    }

    #[test]
    fn critique_release_evidence_source_revision_must_match_release_commit() {
        let path = write_artifact(
            "s3-checkpoint-fault-matrix-release-identity-mismatch",
            add_release_identity(
                valid_s3_checkpoint_fault_matrix(),
                "0123456789abcdef0123456789abcdef01234567",
            ),
        );

        let result = validate_critique_release_evidence_artifact(
            &path,
            &["s3_compatible_checkpoint_fault_matrix"],
            "release-prod",
            "s3://velorix-release/checkpoints",
            Some("fedcba9876543210fedcba9876543210fedcba98"),
            true,
            &[
                "live_s3_compatible",
                "delayed_visibility_cases_passed",
                "retry_fault_cases_passed",
                "mixed_checkpoint_publish_prevented",
                "object_refs_verified",
            ],
            &[],
            &[],
            &[
                "object_write_failure",
                "verification_read_failure",
                "manifest_write_failure",
                "metadata_cas_failure",
                "delayed_visibility",
                "retry_after_failure",
            ],
        );

        fs::remove_file(path).ok();
        assert!(result
            .expect_err("release identity mismatch should fail")
            .to_string()
            .contains("source_revision does not match release_commit"));
    }

    #[test]
    fn critique_release_evidence_rejects_uppercase_image_digest_identity() {
        let mut artifact = add_release_identity(
            valid_s3_checkpoint_fault_matrix(),
            "0123456789abcdef0123456789abcdef01234567",
        );
        artifact["deployed_image_digests"]["velorix-api"] =
            json!("sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let path = write_artifact(
            "s3-checkpoint-fault-matrix-uppercase-release-identity",
            artifact,
        );

        let result = validate_critique_release_evidence_artifact(
            &path,
            &["s3_compatible_checkpoint_fault_matrix"],
            "release-prod",
            "s3://velorix-release/checkpoints",
            Some("0123456789abcdef0123456789abcdef01234567"),
            true,
            &[
                "live_s3_compatible",
                "delayed_visibility_cases_passed",
                "retry_fault_cases_passed",
                "mixed_checkpoint_publish_prevented",
                "object_refs_verified",
            ],
            &[],
            &[],
            &[
                "object_write_failure",
                "verification_read_failure",
                "manifest_write_failure",
                "metadata_cas_failure",
                "delayed_visibility",
                "retry_after_failure",
            ],
        );

        fs::remove_file(path).ok();
        assert!(result
            .expect_err("uppercase release digest should fail")
            .to_string()
            .contains("lowercase hex"));
    }

    #[test]
    fn hiqlite_restore_drill_requires_three_voters() {
        let mut artifact = valid_hiqlite_restore_drill();
        artifact["voter_count"] = json!(1);
        let path = write_artifact("hiqlite-restore-drill", artifact);

        let result = validate_critique_release_evidence_artifact(
            &path,
            &[
                "hiqlite_total_voter_loss_restore_drill",
                "hiqlite_no_pvc_three_voter_backup_restore",
            ],
            "release-prod",
            "s3://velorix-release/checkpoints",
            None,
            false,
            &[
                "no_pvc",
                "total_voter_loss_exercised",
                "restored_from_object_store_backup",
                "acknowledged_metadata_writes_survived",
                "catalog_verified",
                "owner_epoch_verified",
                "checkpoint_pointer_verified",
                "post_restore_ingest_query_verified",
                "restore_drill_verified",
            ],
            &[],
            &[
                "object_store_backup",
                "total_voter_loss_log",
                "restore_log",
                "metadata_write_survival",
                "post_restore_ingest_query",
            ],
            &[],
        );

        fs::remove_file(path).ok();
        assert!(result.is_err());
    }

    #[test]
    fn upgrade_rollback_repair_gc_rejects_local_smoke() {
        let mut artifact = valid_upgrade_rollback_repair_gc_matrix();
        artifact["evidence_note"] = json!("local_smoke");
        let path = write_artifact("upgrade-rollback-repair-gc", artifact);

        let result = validate_critique_release_evidence_artifact(
            &path,
            &["upgrade_rollback_repair_gc_fault_matrix"],
            "release-prod",
            "s3://velorix-release/checkpoints",
            None,
            false,
            &[
                "live_upgrade_rollback_repair_gc_matrix",
                "upgrade_verified",
                "rollback_verified",
                "repair_verified",
                "gc_reachability_verified",
                "acknowledged_data_preserved",
                "no_source_query_recomputation",
            ],
            &[],
            &[],
            &[
                "rolling_upgrade",
                "rollback_after_upgrade",
                "corrupt_latest_checkpoint_repair",
                "gc_concurrent_with_query",
                "gc_concurrent_with_compaction",
                "gc_concurrent_with_recovery",
                "gc_concurrent_with_checkpoint_publication",
                "gc_retains_repair_roots",
            ],
        );

        fs::remove_file(path).ok();
        assert!(result.is_err());
    }

    #[test]
    fn query_output_isolation_requires_published_materialized_output_authority() {
        let mut artifact = valid_query_output_isolation();
        artifact["query_authority"] = json!("source_recompute");
        let path = write_artifact("query-output-isolation", artifact);

        let result = validate_critique_release_evidence_artifact(
            &path,
            &["query_output_isolation"],
            "release-prod",
            "s3://velorix-release/checkpoints",
            None,
            false,
            &[
                "live_release_query_isolation",
                "cold_query_succeeded",
                "object_store_audit_no_source_reads",
                "object_store_audit_no_source_writes",
                "object_store_audit_no_durable_writes",
                "materialized_output_read_verified",
                "no_source_query_recomputation",
            ],
            &[
                "query_pod_source_ingest_prefix_read_access",
                "query_pod_metadata_write_access",
            ],
            &[
                "query_pod_iam_policy",
                "cold_query_log",
                "object_store_audit_log",
                "materialized_output_read",
            ],
            &[],
        );

        fs::remove_file(path).ok();
        assert!(result.is_err());
    }

    #[test]
    fn release_evidence_refs_must_resolve_to_local_file_or_digest_uri() {
        let mut artifact = valid_query_output_isolation();
        artifact["evidence_refs"]["cold_query_log"] = json!("missing-cold-query.log");
        let path = write_artifact("query-output-isolation-missing-ref", artifact);

        let result = validate_critique_release_evidence_artifact(
            &path,
            &["query_output_isolation"],
            "release-prod",
            "s3://velorix-release/checkpoints",
            None,
            false,
            &[
                "live_release_query_isolation",
                "cold_query_succeeded",
                "object_store_audit_no_source_reads",
                "object_store_audit_no_source_writes",
                "object_store_audit_no_durable_writes",
                "materialized_output_read_verified",
                "no_source_query_recomputation",
            ],
            &[
                "query_pod_source_ingest_prefix_read_access",
                "query_pod_metadata_write_access",
            ],
            &[
                "query_pod_iam_policy",
                "cold_query_log",
                "object_store_audit_log",
                "materialized_output_read",
            ],
            &[],
        );

        fs::remove_file(path).ok();
        assert!(result.is_err());
    }

    #[test]
    fn release_evidence_refs_reject_bare_local_file_without_sha256() {
        let path = write_artifact("query-output-isolation-bare-local-ref", json!({}));
        let local = path.with_file_name("velorix-cli-bare-local-proof.log");
        fs::write(&local, b"release proof").expect("write local proof");

        let result = validate_release_evidence_ref(
            &path,
            local.file_name().unwrap().to_str().unwrap(),
            "evidence_refs.proof",
        );

        fs::remove_file(path).ok();
        fs::remove_file(local).ok();
        assert!(result.is_err());
    }

    #[test]
    fn security_release_provenance_rejects_uppercase_hex_identity() {
        let mut artifact = valid_security_release_provenance();
        artifact["source_revision"] = json!("0123456789ABCDEF0123456789ABCDEF01234567");
        artifact["deployed_image_digests"]["velorix-api"] =
            json!("sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let path = write_artifact("security-release-provenance", artifact);

        let result = validate_security_release_provenance_identity(&path, None, false);

        fs::remove_file(path).ok();
        assert!(result.is_err());
    }

    #[test]
    fn security_release_provenance_requires_source_revision_to_match_release_commit() {
        let path = write_artifact(
            "security-release-provenance-mismatch",
            valid_security_release_provenance(),
        );

        let result = validate_security_release_provenance_identity(
            &path,
            Some("fedcba9876543210fedcba9876543210fedcba98"),
            true,
        );

        fs::remove_file(path).ok();
        assert!(result.is_err());
    }

    #[test]
    fn normal_readiness_report_requires_release_artifacts_by_default() {
        let artifacts = ReadinessReleaseArtifactPaths::default();

        let result = validate_readiness_release_artifacts(
            &artifacts,
            "release-prod",
            "s3://velorix-release/checkpoints",
        );

        assert!(result
            .expect_err("normal readiness should require release artifacts")
            .to_string()
            .contains("release readiness requires --dependency-governance-evidence"));
    }

    #[test]
    fn first_e2e_readiness_report_uses_first_e2e_artifact_requirements() {
        let artifacts = ReadinessReleaseArtifactPaths {
            first_e2e_artifacts: true,
            ..Default::default()
        };

        let result = validate_readiness_release_artifacts(
            &artifacts,
            "release-prod",
            "s3://velorix-release/checkpoints",
        );

        assert!(result
            .expect_err("first-E2E readiness should use first-E2E artifact requirements")
            .to_string()
            .contains("--first-e2e-artifacts requires --dependency-governance-evidence"));
    }

    #[test]
    fn security_release_provenance_rejects_placeholder_string_tokens() {
        let mut artifact = valid_security_release_provenance();
        artifact["external_endpoint"] = json!("replace_with");
        let path = write_artifact("security-release-provenance-token", artifact);

        let result = validate_critique_release_evidence_artifact(
            &path,
            &["security_release_provenance"],
            "release-prod",
            "s3://velorix-release/checkpoints",
            None,
            false,
            &[
                "mandatory_api_auth",
                "mandatory_metadata_auth",
                "tenant_authorization_verified",
                "tls_verified",
                "secret_rotation_verified",
                "body_limits_verified",
                "rate_limits_verified",
                "object_prefix_isolation_verified",
                "negative_cross_tenant_tests_passed",
                "clean_source_revision_verified",
                "exact_deployed_image_digests_verified",
                "sbom_attached",
                "dependency_policy_passed",
                "immutable_test_evidence_attached",
            ],
            &[],
            &[
                "api_auth_test",
                "metadata_auth_test",
                "tenant_authorization_test",
                "tls_attestation",
                "secret_rotation_test",
                "limit_tests",
                "object_prefix_isolation_test",
                "cross_tenant_negative_tests",
                "sbom",
                "dependency_policy",
                "immutable_test_evidence",
            ],
            &[],
        );

        fs::remove_file(path).ok();
        assert!(result.is_err());
    }

    #[test]
    fn remaining_release_readiness_rejects_mock_tokens() {
        let mut artifact = valid_remaining_release_readiness();
        artifact["scale_soak_note"] = json!("mock");
        let path = write_artifact("remaining-release-readiness", artifact);

        let result = validate_critique_release_evidence_artifact(
            &path,
            &["remaining_release_readiness"],
            "release-prod",
            "s3://velorix-release/checkpoints",
            None,
            false,
            &[
                "release_image_contract_tests_passed",
                "versioned_openapi_contract_verified",
                "no_conflicting_accepted_contracts",
                "sql_admission_corpus_generated",
                "sql_admission_corpus_covers_unsupported_datafusion_plan_nodes",
                "sql_admission_corpus_covers_unsupported_datafusion_expression_nodes",
                "unsupported_sql_leaves_no_persisted_view_metadata",
                "unsupported_sql_leaves_no_runtime_binding",
                "sql_admission_mutation_ci_failure_verified",
                "persistent_write_boundary_crash_matrix_passed",
                "crash_matrix_covers_one_view",
                "crash_matrix_covers_multiple_affected_views",
                "crash_matrix_covers_joins",
                "crash_matrix_covers_compaction",
                "replay_duplicate_reordered_gapped_retried_batches_verified",
                "replay_live_crash_clean_outputs_identical",
                "replay_checkpoint_hashes_identical",
                "non_contiguous_input_never_advances_frontier",
                "join_frontier_spec_verified",
                "concurrent_two_input_ingest_crash_leader_handoff_verified",
                "output_manifests_record_exact_input_frontiers",
                "published_limits_verified",
                "multi_day_supported_object_store_soak_passed",
            ],
            &[],
            &[
                "release_image_contract_tests",
                "openapi_contract",
                "sql_admission_corpus",
                "crash_matrix",
                "replay_determinism",
                "join_frontier",
                "scale_soak",
            ],
            &[],
        );

        fs::remove_file(path).ok();
        assert!(result.is_err());
    }

    fn write_rustfs_production_gc_family(dir: &Path) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
        let gate = dir.join("rustfs-s3-gate-evidence.json");
        let seed = dir.join("rustfs-production-gc-seed.json");
        let execute = dir.join("rustfs-production-gc-run.json");
        let production = dir.join("rustfs-production-gc.json");
        let authority_store_id = "s3://rustfs/velorix-rustfs/rustfs-s3-gate/test/production-gc";
        let gc_run_id = "rustfs-production-gc-test";
        let deleted_object_key = "v1/state/orders_sum_count/p=0000000000/chk=00000000000000000000/rustfs-production-gc-test-state-0000.state";

        fs::write(&gate, serde_json::to_vec(&json!({
            "schema_version": 1,
            "evidence_kind": "rustfs_s3_compatible_gate",
            "readiness_evidence_kind": ["s3_compatible", "s3_compatible_integration_harness"],
            "gate_detail_kind": ["s3_compatible_ingest_admission_crash_restart", "s3_compatible_gc_execution_retention"],
            "backend_evidence_scope": "live_or_native",
            "production_gc_artifact": {
                "generated": true,
                "evidence_kind": "production_gc_run_evidence",
                "fixture_kind": "release_smoke_gc_fixture",
                "seed_artifact_path": seed.display().to_string(),
                "execute_artifact_path": execute.display().to_string(),
                "artifact_path": production.display().to_string(),
                "deployment_id": "rustfs-s3-gate",
                "authority_store_id": authority_store_id,
                "gc_run_id": gc_run_id,
                "prefix": "rustfs-s3-gate/test/production-gc",
                "retain_latest_manifests": 1,
                "expected_min_deleted_candidates": 1
            }
        })).unwrap()).unwrap();
        fs::write(&seed, serde_json::to_vec(&json!({
            "schema_version": 1,
            "status": "pass",
            "evidence_kind": "s3_compatible_gc_seed_fixture",
            "fixture_kind": "release_smoke_gc_fixture",
            "authority_store_id": authority_store_id,
            "seed_id": gc_run_id,
            "checkpoint_versions": [0, 1],
            "state_object_ids": ["rustfs-production-gc-test-state-0000", "rustfs-production-gc-test-state-0001"],
            "expected_deleted_object_keys": [deleted_object_key],
            "state_objects_written": 2,
            "expected_min_deleted_candidates": 1,
            "expected_deleted_candidates_at_retain_latest_manifests": 1
        })).unwrap()).unwrap();
        fs::write(&execute, serde_json::to_vec(&json!({
            "schema_version": 1,
            "run_id": gc_run_id,
            "policy": {"retain_latest_manifests": 1},
            "plan": {"retained_manifest_versions": [1], "candidates": [{"object_key": deleted_object_key, "kind": "raw_state_object"}]},
            "report": {"deleted": [{"object_key": deleted_object_key, "kind": "raw_state_object"}], "skipped": []}
        })).unwrap()).unwrap();
        let run: GarbageCollectionRunV1 =
            serde_json::from_slice(&fs::read(&execute).unwrap()).unwrap();
        fs::write(
            &production,
            serde_json::to_vec(&json!({
                "schema_version": 1,
                "status": "pass",
                "evidence_kind": "production_gc_run_evidence",
                "deployment_id": "rustfs-s3-gate",
                "authority_store_id": authority_store_id,
                "gc_run_id": gc_run_id,
                "listing_consistency_checked": true,
                "checkpoint_retention_records_checked": true,
                "checkpoint_gc_transition_records_checked": true,
                "verified_gc_run_digest": garbage_collection_run_digest(&run).unwrap(),
                "verified_gc_run_deleted_count": 1,
                "verified_gc_run_retain_latest_manifests": 1,
                "verified_gc_run_deleted_object_keys": gc_run_deleted_object_keys(&run)
            }))
            .unwrap(),
        )
        .unwrap();
        (gate, seed, execute, production)
    }

    #[test]
    fn rustfs_production_gc_evidence_validate_rejects_stale_execute_digest() {
        let dir = tempdir().unwrap();
        let (gate, seed, execute, production) = write_rustfs_production_gc_family(dir.path());
        let mut run: serde_json::Value =
            serde_json::from_slice(&fs::read(&execute).unwrap()).unwrap();
        run["report"]["skipped"] = json!([{
            "object_key": "v1/state/other/p=0000000000/chk=00000000000000000000/stale.state",
            "kind": "raw_state_object"
        }]);
        fs::write(&execute, serde_json::to_vec(&run).unwrap()).unwrap();

        let error =
            validate_rustfs_production_gc_evidence_family(&gate, &seed, &execute, &production)
                .unwrap_err();
        assert!(
            format!("{error:#}").contains("verified_gc_run_digest"),
            "unexpected validation error: {error:#}"
        );
    }

    #[test]
    fn rustfs_production_gc_evidence_validate_rejects_seed_id_substring_key() {
        let dir = tempdir().unwrap();
        let (gate, seed, execute, production) = write_rustfs_production_gc_family(dir.path());
        let different_key = "v1/state/other_owner/p=0000000000/chk=00000000000000000000/rustfs-production-gc-test-state-0000.state";
        let mut run: serde_json::Value =
            serde_json::from_slice(&fs::read(&execute).unwrap()).unwrap();
        run["plan"]["candidates"][0]["object_key"] = json!(different_key);
        run["report"]["deleted"][0]["object_key"] = json!(different_key);
        fs::write(&execute, serde_json::to_vec(&run).unwrap()).unwrap();
        let run: GarbageCollectionRunV1 =
            serde_json::from_slice(&fs::read(&execute).unwrap()).unwrap();
        let mut evidence: serde_json::Value =
            serde_json::from_slice(&fs::read(&production).unwrap()).unwrap();
        evidence["verified_gc_run_digest"] = json!(garbage_collection_run_digest(&run).unwrap());
        evidence["verified_gc_run_deleted_object_keys"] = json!(gc_run_deleted_object_keys(&run));
        fs::write(&production, serde_json::to_vec(&evidence).unwrap()).unwrap();

        let error =
            validate_rustfs_production_gc_evidence_family(&gate, &seed, &execute, &production)
                .unwrap_err();
        assert!(format!("{error:#}").contains("deleted keys do not match seeded expectation"));
    }

    #[tokio::test]
    async fn gc_production_evidence_rejects_empty_live_gc_run() {
        let dir = tempdir().unwrap();
        let store = local_object_store(dir.path()).unwrap();
        let run = execute_s3_compatible_garbage_collection(
            Arc::clone(&store),
            "s3://velorix-test",
            "run-empty",
            GarbageCollectionPolicy {
                retain_latest_manifests: 1,
            },
        )
        .await
        .unwrap();
        let error = generate_production_gc_run_evidence(
            store,
            "prod-a".to_string(),
            "s3://velorix-test".to_string(),
            "run-empty".to_string(),
        )
        .await
        .unwrap_err();

        assert!(run.report.deleted.is_empty());
        assert!(format!("{error:#}").contains("at least one deleted candidate"));
    }

    #[test]
    fn local_pr_smoke_evidence_reports_only_deterministic_cost_comparisons() {
        let result = BenchmarkGateResultV1::from_json_str(include_str!(
            "../../../baselines/benchmark/local/pr-smoke.json"
        ))
        .unwrap();
        let evidence = BenchmarkGateEvidenceV1::passed(
            Path::new("baseline.json"),
            Path::new("result.json"),
            &result,
            &result,
            0.25,
        );

        assert!(evidence.performance_compared_workloads.is_empty());
        assert_eq!(
            evidence.deterministic_cost_compared_workloads,
            evidence.workload_metrics
        );
    }
}

fn require_artifact_path(name: &str, path: &Option<PathBuf>) -> anyhow::Result<()> {
    if path.is_some() {
        Ok(())
    } else {
        bail!("readiness-report release readiness requires --{name}")
    }
}

fn require_first_e2e_artifact_path(name: &str, path: &Option<PathBuf>) -> anyhow::Result<()> {
    if path.is_some() {
        Ok(())
    } else {
        bail!("readiness-report --first-e2e-artifacts requires --{name}")
    }
}

fn validate_dependency_governance_evidence_artifact(
    path: &Path,
    _release_commit: Option<&str>,
    _manifest_path: Option<&Path>,
) -> anyhow::Result<()> {
    let artifact: DependencyGovernanceEvidenceArtifactV1 = read_json_artifact(path)?;

    if artifact.schema_version != 1 {
        bail!(
            "{} has unsupported schema_version {}, expected 1",
            path.display(),
            artifact.schema_version
        );
    }
    if artifact.status != "pass" {
        bail!(
            "{} dependency governance evidence is not pass",
            path.display()
        );
    }
    if artifact.evidence_kind != "dependency_governance_validated" {
        bail!(
            "{} has evidence_kind {}, expected dependency_governance_validated",
            path.display(),
            artifact.evidence_kind
        );
    }
    if !artifact.cargo_deny.diagnostics_checked {
        bail!(
            "{} dependency governance evidence did not check cargo-deny diagnostics",
            path.display()
        );
    }
    if !artifact.missing_required_package_review_subjects.is_empty() {
        bail!(
            "{} dependency governance evidence has missing package review subjects",
            path.display()
        );
    }

    Ok(())
}
fn validate_s3_release_benchmark_gate_evidence_artifact(path: &Path) -> anyhow::Result<()> {
    reject_local_readiness_artifact(&read_artifact_evidence_kind(path)?, path)?;
    let artifact = read_s3_benchmark_gate_evidence_artifact(path)?;

    if artifact.schema_version != 1 {
        bail!(
            "{} has unsupported schema_version {}, expected 1",
            path.display(),
            artifact.schema_version
        );
    }
    if artifact.status != "pass" {
        bail!("{} benchmark gate evidence is not pass", path.display());
    }
    if artifact.evidence_kind != "s3_compatible_benchmark_gate" {
        bail!(
            "{} has evidence_kind {}, expected s3_compatible_benchmark_gate",
            path.display(),
            artifact.evidence_kind
        );
    }
    if artifact.gate_level != BenchmarkGateLevel::Release {
        bail!(
            "{} benchmark gate evidence is not release level",
            path.display()
        );
    }
    if artifact.backend != BenchmarkBackend::S3Compatible {
        bail!(
            "{} benchmark gate evidence is not s3-compatible",
            path.display()
        );
    }
    if artifact.backend_evidence_scope == BenchmarkEvidenceScope::LocalEmulator {
        bail!(
            "{} benchmark gate evidence uses local emulator scope",
            path.display()
        );
    }
    if artifact.workload != "s3_incremental" {
        bail!(
            "{} benchmark gate workload is {}, expected s3_incremental",
            path.display(),
            artifact.workload
        );
    }
    let missing_workload_metrics: Vec<&str> = S3_COMPATIBLE_BENCHMARK_GATE_WORKLOADS
        .iter()
        .copied()
        .filter(|required| {
            !artifact
                .workload_metrics
                .iter()
                .any(|actual| actual == required)
        })
        .collect();
    if !missing_workload_metrics.is_empty() {
        bail!(
            "{} benchmark gate evidence is missing required S3 workload_metrics: {}",
            path.display(),
            missing_workload_metrics.join(", ")
        );
    }
    if artifact
        .baseline_path
        .as_deref()
        .unwrap_or("")
        .trim()
        .is_empty()
        || artifact
            .baseline_commit
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
        || artifact.max_regression_fraction.is_none()
    {
        bail!(
            "{} benchmark gate evidence is missing release baseline comparison fields",
            path.display()
        );
    }
    if is_placeholder_commit(&artifact.baseline_commit.unwrap_or_default())
        || is_placeholder_commit(&artifact.result_commit)
    {
        bail!(
            "{} benchmark gate evidence uses placeholder commits",
            path.display()
        );
    }

    Ok(())
}

fn read_s3_benchmark_gate_evidence_artifact(
    path: &Path,
) -> anyhow::Result<BenchmarkGateEvidenceArtifactV1> {
    let value: serde_json::Value = read_json_artifact(path)?;
    if value.get("backend_evidence_scope").is_none() {
        bail!(
            "{} benchmark gate evidence is missing backend_evidence_scope",
            path.display()
        );
    }

    serde_json::from_value(value)
        .with_context(|| format!("failed to parse benchmark gate evidence {}", path.display()))
}

fn validate_production_gc_run_evidence_artifact(
    path: &Path,
    deployment_id: &str,
    authority_store_id: &str,
) -> anyhow::Result<()> {
    reject_local_readiness_artifact(&read_artifact_evidence_kind(path)?, path)?;
    let artifact: ProductionGcRunEvidenceArtifactV1 = read_json_artifact(path)?;

    if artifact.schema_version != 1 {
        bail!(
            "{} has unsupported schema_version {}, expected 1",
            path.display(),
            artifact.schema_version
        );
    }
    if artifact.status != "pass" {
        bail!("{} production GC evidence is not pass", path.display());
    }
    if artifact.evidence_kind != "production_gc_run_evidence" {
        bail!(
            "{} has evidence_kind {}, expected production_gc_run_evidence",
            path.display(),
            artifact.evidence_kind
        );
    }
    if artifact.deployment_id.trim().is_empty() {
        bail!(
            "{} production GC evidence is missing deployment_id",
            path.display()
        );
    }
    if artifact.deployment_id != deployment_id {
        bail!(
            "{} production GC evidence deployment_id does not match readiness report",
            path.display()
        );
    }
    if artifact.authority_store_id.trim().is_empty()
        || is_local_dev_authority_store_id(&artifact.authority_store_id)
    {
        bail!(
            "{} production GC evidence uses local/dev authority_store_id",
            path.display()
        );
    }
    if artifact.authority_store_id != authority_store_id {
        bail!(
            "{} production GC evidence authority_store_id does not match readiness report",
            path.display()
        );
    }
    if artifact.gc_run_id.trim().is_empty() {
        bail!(
            "{} production GC evidence is missing gc_run_id",
            path.display()
        );
    }
    if !artifact.listing_consistency_checked {
        bail!(
            "{} production GC evidence did not check listing consistency",
            path.display()
        );
    }
    if !artifact.checkpoint_retention_records_checked {
        bail!(
            "{} production GC evidence did not check checkpoint retention records",
            path.display()
        );
    }
    if !artifact.checkpoint_gc_transition_records_checked {
        bail!(
            "{} production GC evidence did not check checkpoint GC transition records",
            path.display()
        );
    }
    if artifact
        .verified_gc_run_digest
        .as_deref()
        .filter(|digest| digest.starts_with("sha256:") && digest.len() == "sha256:".len() + 64)
        .is_none()
    {
        bail!(
            "{} production GC evidence is missing verified_gc_run_digest",
            path.display()
        );
    }
    let Some(deleted_count) = artifact.verified_gc_run_deleted_count else {
        bail!(
            "{} production GC evidence is missing verified_gc_run_deleted_count",
            path.display()
        );
    };
    if deleted_count == 0 {
        bail!(
            "{} production GC evidence must verify at least one deleted candidate",
            path.display()
        );
    }
    let Some(retain_latest_manifests) = artifact.verified_gc_run_retain_latest_manifests else {
        bail!(
            "{} production GC evidence is missing verified_gc_run_retain_latest_manifests",
            path.display()
        );
    };
    if retain_latest_manifests == 0 {
        bail!(
            "{} production GC evidence has invalid verified_gc_run_retain_latest_manifests",
            path.display()
        );
    }
    let Some(deleted_object_keys) = &artifact.verified_gc_run_deleted_object_keys else {
        bail!(
            "{} production GC evidence is missing verified_gc_run_deleted_object_keys",
            path.display()
        );
    };
    if deleted_object_keys.len() != deleted_count
        || deleted_object_keys
            .iter()
            .any(|key| key.trim().is_empty() || !key.starts_with("v1/"))
    {
        bail!(
            "{} production GC evidence has invalid verified_gc_run_deleted_object_keys",
            path.display()
        );
    }

    Ok(())
}

fn validate_rustfs_production_gc_validation_evidence_artifact(
    path: &Path,
    production_gc_run_evidence_path: Option<&Path>,
    deployment_id: &str,
    authority_store_id: &str,
) -> anyhow::Result<()> {
    reject_local_readiness_artifact(&read_artifact_evidence_kind(path)?, path)?;
    let artifact: RustfsProductionGcEvidenceValidationReportV1 = read_json_artifact(path)?;

    if artifact.schema_version != 1 {
        bail!(
            "{} has unsupported schema_version {}, expected 1",
            path.display(),
            artifact.schema_version
        );
    }
    if artifact.status != "pass" {
        bail!(
            "{} RustFS production GC validation evidence is not pass",
            path.display()
        );
    }
    if artifact.evidence_kind != "rustfs_production_gc_evidence_family_validated" {
        bail!(
            "{} has evidence_kind {}, expected rustfs_production_gc_evidence_family_validated",
            path.display(),
            artifact.evidence_kind
        );
    }
    if artifact.deployment_id != deployment_id {
        bail!(
            "{} RustFS production GC validation deployment_id does not match readiness report",
            path.display()
        );
    }
    if artifact.authority_store_id != authority_store_id {
        bail!(
            "{} RustFS production GC validation authority_store_id does not match readiness report",
            path.display()
        );
    }
    if artifact.gc_run_id.trim().is_empty() {
        bail!(
            "{} RustFS production GC validation evidence is missing gc_run_id",
            path.display()
        );
    }
    if artifact.retain_latest_manifests == 0 || artifact.deleted_candidates == 0 {
        bail!(
            "{} RustFS production GC validation evidence must include a retained policy and deleted candidates",
            path.display()
        );
    }
    for (reported, label) in [
        (artifact.gate_evidence_path.as_str(), "gate_evidence_path"),
        (artifact.seed_evidence_path.as_str(), "seed_evidence_path"),
        (
            artifact.execute_evidence_path.as_str(),
            "execute_evidence_path",
        ),
        (
            artifact.production_evidence_path.as_str(),
            "production_evidence_path",
        ),
    ] {
        if reported.trim().is_empty() {
            bail!(
                "{} RustFS production GC validation {} must be non-empty",
                path.display(),
                label
            );
        }
    }
    for required_check in [
        "rustfs_s3_compatible_gate_present",
        "seed_fixture_created_retired_checkpoint_state",
        "s3_gc_execute_deleted_seeded_candidate",
        "production_gc_evidence_verified_listing_retention_and_transition",
        "artifact_family_paths_and_identity_bound",
    ] {
        if !artifact.checks.iter().any(|check| check == required_check) {
            bail!(
                "{} RustFS production GC validation evidence is missing check {}",
                path.display(),
                required_check
            );
        }
    }
    if let Some(production_gc_run_evidence_path) = production_gc_run_evidence_path {
        if !evidence_path_matches(
            artifact.production_evidence_path.as_str(),
            production_gc_run_evidence_path,
        ) && !evidence_path_filename_matches(
            artifact.production_evidence_path.as_str(),
            production_gc_run_evidence_path,
        ) {
            bail!(
                "{} RustFS production GC validation production_evidence_path does not match {}",
                path.display(),
                production_gc_run_evidence_path.display()
            );
        }
    }

    Ok(())
}

fn evidence_path_filename_matches(reported: &str, path: &Path) -> bool {
    let Some(reported_name) = Path::new(reported).file_name() else {
        return false;
    };
    path.file_name() == Some(reported_name)
}

fn validate_ingest_writer_lifecycle_evidence_artifact(
    path: &Path,
    deployment_id: &str,
    authority_store_id: &str,
) -> anyhow::Result<()> {
    let artifact: IngestWriterLifecycleEvidenceArtifactV1 = read_json_artifact(path)?;

    if artifact.schema_version != 1 {
        bail!(
            "{} has unsupported schema_version {}, expected 1",
            path.display(),
            artifact.schema_version
        );
    }
    if artifact.evidence_kind != "velorix_ingest_writer_lifecycle_attestation" {
        bail!(
            "{} has evidence_kind {}, expected velorix_ingest_writer_lifecycle_attestation",
            path.display(),
            artifact.evidence_kind
        );
    }
    if artifact.deployment_id.trim().is_empty() {
        bail!(
            "{} ingest-writer lifecycle evidence is missing deployment_id",
            path.display()
        );
    }
    if artifact.deployment_id != deployment_id {
        bail!(
            "{} ingest-writer lifecycle evidence deployment_id does not match readiness report",
            path.display()
        );
    }
    if artifact.authority_store_id.trim().is_empty()
        || is_local_dev_authority_store_id(&artifact.authority_store_id)
    {
        bail!(
            "{} ingest-writer lifecycle evidence uses local/dev authority_store_id",
            path.display()
        );
    }
    if artifact.authority_store_id != authority_store_id {
        bail!(
            "{} ingest-writer lifecycle evidence authority_store_id does not match readiness report",
            path.display()
        );
    }
    if !matches!(
        artifact.deployed_topology.as_str(),
        "kubernetes_jobs" | "kubernetes_operator" | "replicated_controller"
    ) {
        bail!(
            "{} ingest-writer lifecycle evidence has unsupported deployed_topology {}",
            path.display(),
            artifact.deployed_topology
        );
    }
    if artifact.attested_at.trim().is_empty() {
        bail!(
            "{} ingest-writer lifecycle evidence is missing attested_at",
            path.display()
        );
    }
    validate_recent_ingest_writer_lifecycle_attested_at(path, &artifact.attested_at)?;
    if artifact.attester.trim().is_empty() {
        bail!(
            "{} ingest-writer lifecycle evidence is missing attester",
            path.display()
        );
    }
    if !artifact.pod_internal_append_completed {
        bail!(
            "{} ingest-writer lifecycle evidence requires pod_internal_append_completed=true",
            path.display()
        );
    }
    if !artifact.multi_pod_overlap_conflict_rejected {
        bail!(
            "{} ingest-writer lifecycle evidence requires multi_pod_overlap_conflict_rejected=true",
            path.display()
        );
    }
    if !artifact.adjacent_append_succeeded {
        bail!(
            "{} ingest-writer lifecycle evidence requires adjacent_append_succeeded=true",
            path.display()
        );
    }
    if !artifact.crash_restart_reconstruction_checked {
        bail!(
            "{} ingest-writer lifecycle evidence requires crash_restart_reconstruction_checked=true",
            path.display()
        );
    }
    if artifact.deployed_topology == "kubernetes_jobs" && artifact.leader_handoff_checked {
        bail!(
            "{} Kubernetes Job lifecycle evidence must not claim leader_handoff_checked=true",
            path.display()
        );
    }
    if !artifact.kubernetes_lease_handoff_checked {
        bail!(
            "{} ingest-writer lifecycle evidence requires kubernetes_lease_handoff_checked=true",
            path.display()
        );
    }
    if !artifact.lease_held_through_append_checked {
        bail!(
            "{} ingest-writer lifecycle evidence requires lease_held_through_append_checked=true",
            path.display()
        );
    }
    if !artifact.commit_guard_checked {
        bail!(
            "{} ingest-writer lifecycle evidence requires commit_guard_checked=true",
            path.display()
        );
    }
    if !artifact.admission_commit_guard_bound_checked {
        bail!(
            "{} ingest-writer lifecycle evidence requires admission_commit_guard_bound_checked=true",
            path.display()
        );
    }
    if !artifact.lease_loss_during_reservation_checked {
        bail!(
            "{} ingest-writer lifecycle evidence requires lease_loss_during_reservation_checked=true",
            path.display()
        );
    }
    if !artifact.no_pvc_created_by_vind {
        bail!(
            "{} ingest-writer lifecycle evidence requires no_pvc_created_by_vind=true",
            path.display()
        );
    }
    validate_ingest_writer_lifecycle_evidence_provenance(path, &artifact)?;
    validate_ingest_writer_lifecycle_evidence_files(
        path,
        &artifact.evidence_files,
        "ingest-writer lifecycle evidence",
    )?;

    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StandingRuntimeProductEvidenceMode {
    FirstE2e,
    Release,
}

fn validate_standing_runtime_product_evidence_artifact(
    path: &Path,
    deployment_id: &str,
    authority_store_id: &str,
    mode: StandingRuntimeProductEvidenceMode,
    release_commit: Option<&str>,
) -> anyhow::Result<()> {
    let artifact: serde_json::Value = read_json_artifact(path)?;

    require_json_u64(path, &artifact, "/schema_version")?;
    if require_json_u64(path, &artifact, "/schema_version")? != 1 {
        bail!(
            "{} has unsupported schema_version, expected 1",
            path.display()
        );
    }
    if require_json_str(path, &artifact, "/evidence_kind")? != "velorix_product_slice_evidence" {
        bail!(
            "{} has evidence_kind {}, expected velorix_product_slice_evidence",
            path.display(),
            require_json_str(path, &artifact, "/evidence_kind")?
        );
    }
    if require_json_str(path, &artifact, "/deployment_id")? != deployment_id {
        bail!(
            "{} product evidence deployment_id does not match readiness report",
            path.display()
        );
    }
    let product_authority_store_id =
        require_json_str(path, &artifact, "/object_store/authority_store_id")?;
    if product_authority_store_id.trim().is_empty()
        || is_local_dev_authority_store_id(product_authority_store_id)
    {
        bail!(
            "{} product evidence uses local/dev authority_store_id",
            path.display()
        );
    }
    if product_authority_store_id != authority_store_id {
        bail!(
            "{} product evidence authority_store_id does not match readiness report",
            path.display()
        );
    }
    let authority_scope = s3_authority_scope(product_authority_store_id).with_context(|| {
        format!(
            "{} product evidence authority_store_id is not a supported S3 authority",
            path.display()
        )
    })?;

    require_json_true(path, &artifact, "/rest_callable")?;
    validate_product_api_auth_evidence(path, &artifact)?;
    require_json_true(path, &artifact, "/api/openapi/catalog_smoke_passed")?;
    if require_json_str(path, &artifact, "/api/openapi/evidence_file")? != "openapi.json" {
        bail!(
            "{} product evidence must attach OpenAPI catalog evidence",
            path.display()
        );
    }
    require_sibling_evidence_file(path, "openapi.json", "product OpenAPI evidence")?;
    validate_product_openapi_sibling_evidence(path, &artifact)?;
    if require_json_str(path, &artifact, "/api/openapi/promoted_api_path")?
        != "/v1/api/scores/positive"
    {
        bail!(
            "{} product evidence OpenAPI smoke did not use the default promoted API path",
            path.display()
        );
    }
    require_json_true(path, &artifact, "/api/openapi/promoted_api_path_present")?;
    require_json_true(path, &artifact, "/api/openapi/generic_query_path_absent")?;
    require_json_true(
        path,
        &artifact,
        "/api/openapi/legacy_parameterized_path_absent",
    )?;
    require_json_true(
        path,
        &artifact,
        "/api/openapi/query_policy_extension_present",
    )?;
    if require_json_str(path, &artifact, "/api/openapi/linked_view_policy_id")? != "interactive" {
        bail!(
            "{} product evidence OpenAPI catalog did not bind the interactive query policy",
            path.display()
        );
    }
    require_json_true(path, &artifact, "/api/openapi/response_schema_checked")?;
    require_json_true(path, &artifact, "/api/query_policy/catalog_smoke_passed")?;
    require_json_true(
        path,
        &artifact,
        "/api/query_policy/production_bounds_required",
    )?;
    require_json_true(path, &artifact, "/api/query_policy/weak_policy_rejected")?;
    require_json_true(path, &artifact, "/api/query_policy/missing_policy_rejected")?;
    for (pointer, expected) in [
        (
            "/api/query_policy/evidence_files/created",
            "query-policy-interactive.json",
        ),
        (
            "/api/query_policy/evidence_files/read_back",
            "query-policy-interactive-read.json",
        ),
        (
            "/api/query_policy/evidence_files/weak_policy_rejection",
            "query-policy-weak-rejection.json",
        ),
        (
            "/api/query_policy/evidence_files/missing_policy_rejection",
            "query-policy-missing-view.json",
        ),
    ] {
        if require_json_str(path, &artifact, pointer)? != expected {
            bail!(
                "{} product evidence query policy evidence file {pointer} did not match {expected}",
                path.display()
            );
        }
        require_sibling_evidence_file(path, expected, "product query-policy evidence")?;
    }
    if require_json_str(path, &artifact, "/api/query_policy/linked_view_policy_id")?
        != "interactive"
    {
        bail!(
            "{} product evidence query policy did not bind the interactive linked view policy",
            path.display()
        );
    }
    validate_product_query_policy_sibling_evidence(path)?;
    if require_json_str(path, &artifact, "/object_store/mode")? != "external-s3" {
        bail!(
            "{} product evidence must use external-s3 object_store.mode",
            path.display()
        );
    }
    require_json_true(
        path,
        &artifact,
        "/object_store/external_s3_validate_enabled",
    )?;
    require_json_true(
        path,
        &artifact,
        "/object_store/external_s3_bucket_validated",
    )?;
    require_json_true(
        path,
        &artifact,
        "/object_store/external_s3_prefix_validated",
    )?;
    if require_json_str(path, &artifact, "/object_store/bucket")? != authority_scope.bucket {
        bail!(
            "{} product evidence bucket does not match authority_store_id",
            path.display()
        );
    }
    if require_json_str(path, &artifact, "/object_store/s3_prefix")? != authority_scope.prefix {
        bail!(
            "{} product evidence s3_prefix does not match authority_store_id",
            path.display()
        );
    }
    let validation_key =
        require_json_str(path, &artifact, "/object_store/external_s3_validation_key")?;
    let expected_validation_prefix = if authority_scope.prefix.is_empty() {
        "_velorix_external_s3_validation/".to_string()
    } else {
        format!(
            "{}/_velorix_external_s3_validation/",
            authority_scope.prefix.trim_end_matches('/')
        )
    };
    if !validation_key.starts_with(&expected_validation_prefix) {
        bail!(
            "{} product evidence external_s3_validation_key is outside the authority prefix",
            path.display()
        );
    }
    let validation_probe_prefix = if authority_scope.prefix.is_empty() {
        "_velorix_external_s3_validation".to_string()
    } else {
        authority_scope.prefix.trim_end_matches('/').to_string()
    };
    if require_json_str(
        path,
        &artifact,
        "/object_store/external_s3_validation_evidence/job",
    )? != "external-s3-validate-job.json"
    {
        bail!(
            "{} product evidence must attach external S3 validation job evidence",
            path.display()
        );
    }
    require_sibling_evidence_file(
        path,
        "external-s3-validate-job.json",
        "external S3 validation job evidence",
    )?;
    if require_json_str(
        path,
        &artifact,
        "/object_store/external_s3_validation_evidence/log",
    )? != "external-s3-validate.log"
    {
        bail!(
            "{} product evidence must attach external S3 validation log evidence",
            path.display()
        );
    }
    require_sibling_evidence_file(
        path,
        "external-s3-validate.log",
        "external S3 validation log evidence",
    )?;
    validate_product_external_s3_validation_siblings(
        path,
        &authority_scope,
        &validation_probe_prefix,
        validation_key,
    )?;
    require_json_true(path, &artifact, "/no_pvc/namespace_validated")?;
    if require_json_str(path, &artifact, "/no_pvc/evidence")? != "no-pvc-namespace.json" {
        bail!(
            "{} product evidence must attach no-PVC namespace evidence",
            path.display()
        );
    }
    require_sibling_evidence_file(
        path,
        "no-pvc-namespace.json",
        "product no-PVC namespace evidence",
    )?;
    validate_product_no_pvc_namespace_sibling(path)?;
    if require_json_str(path, &artifact, "/no_pvc/contract")?
        != "no PersistentVolumeClaim objects in the Velorix product namespace"
    {
        bail!(
            "{} product evidence no-PVC contract did not match the expected namespace contract",
            path.display()
        );
    }
    require_json_true(
        path,
        &artifact,
        "/ingest_writer/pod_internal_append_verified",
    )?;
    for (pointer, expected) in [
        ("job_log", "ingest-writer-job-log.json"),
        ("job", "ingest-writer-job.json"),
        ("pods", "ingest-writer-pods.json"),
    ] {
        let evidence_pointer = format!("/ingest_writer/evidence_files/{pointer}");
        if require_json_str(path, &artifact, &evidence_pointer)? != expected {
            bail!("{} product evidence must attach {expected}", path.display());
        }
        require_sibling_evidence_file(path, expected, "product ingest-writer append evidence")?;
    }
    validate_product_ingest_writer_lifecycle_attestation(
        path,
        &artifact,
        deployment_id,
        authority_store_id,
    )?;

    let configured_mode =
        require_json_str(path, &artifact, "/standing_runtime_fencing/configured_mode")?;
    if configured_mode == "unsafe-dev-only" {
        bail!(
            "{} product evidence uses unsafe-dev-only standing-runtime fencing",
            path.display()
        );
    }
    if mode == StandingRuntimeProductEvidenceMode::FirstE2e
        && !matches!(configured_mode, "logical-fencing" | "required")
    {
        bail!(
            "{} first-E2E product evidence requires logical-fencing or required standing-runtime fencing",
            path.display()
        );
    }
    if require_json_u64(path, &artifact, "/api/replica_count")? < 2 {
        bail!(
            "{} product evidence requires at least two API replicas",
            path.display()
        );
    }
    require_json_true(
        path,
        &artifact,
        "/standing_runtime_fencing/capability/multi_writer_fencing_safe",
    )?;
    if require_json_str(
        path,
        &artifact,
        "/metadata_store/standing_runtime_adversarial_smoke/status",
    )? != "pass"
    {
        bail!(
            "{} product evidence missing metadata standing-runtime adversarial smoke pass",
            path.display()
        );
    }
    for pointer in [
        "/metadata_store/standing_runtime_adversarial_smoke/assertions/logical_owner_expiry_checked",
        "/metadata_store/standing_runtime_adversarial_smoke/assertions/new_owner_epoch_fences_old_owner",
        "/metadata_store/standing_runtime_adversarial_smoke/assertions/stale_owner_checkpoint_publish_rejected",
        "/metadata_store/standing_runtime_adversarial_smoke/assertions/latest_checkpoint_remains_metadata_authoritative",
    ] {
        require_json_true(path, &artifact, pointer)?;
    }
    if require_json_str(
        path,
        &artifact,
        "/standing_runtime_fencing/multi_replica_fencing_smoke/status",
    )? != "pass"
    {
        bail!(
            "{} product evidence missing multi-replica fencing smoke pass",
            path.display()
        );
    }
    if mode == StandingRuntimeProductEvidenceMode::FirstE2e {
        if require_json_str(
            path,
            &artifact,
            "/standing_runtime_fencing/local_api_pod_failover_smoke/status",
        )? != "pass"
        {
            bail!(
                "{} first-E2E product evidence missing local API pod failover smoke pass",
                path.display()
            );
        }
        if require_json_str(
            path,
            &artifact,
            "/standing_runtime_fencing/local_api_pod_failover_smoke/evidence",
        )? != "standing-runtime-failover-smoke.json"
        {
            bail!(
                "{} product evidence must attach local API pod failover evidence",
                path.display()
            );
        }
        require_sibling_evidence_file(
            path,
            "standing-runtime-failover-smoke.json",
            "product local API pod failover evidence",
        )?;
        require_json_false(
            path,
            &artifact,
            "/standing_runtime_fencing/local_api_pod_failover_smoke/trusted_for_product_complete",
        )?;
        require_json_false(
            path,
            &artifact,
            "/standing_runtime_fencing/local_api_pod_failover_smoke/production_wall_clock_failover_attestation",
        )?;
    }

    if mode == StandingRuntimeProductEvidenceMode::Release {
        if configured_mode != "required" {
            bail!(
                "{} release product evidence requires required standing-runtime fencing mode",
                path.display()
            );
        }
        require_json_true(path, &artifact, "/product_complete")?;
        validate_product_object_store_durability_policy_attestation(
            path,
            &artifact,
            &authority_scope,
        )?;
        validate_product_ingress_tls_auth_attestation(path, &artifact)?;
        validate_product_deployed_image_evidence(path, &artifact)?;
        validate_product_metadata_authority(path, &artifact, release_commit)?;
        require_json_true(path, &artifact, "/standing_runtime_fencing/required_mode")?;
        require_json_true(
            path,
            &artifact,
            "/standing_runtime_fencing/capability/bounded_wall_clock_failover",
        )?;
        require_json_true(
            path,
            &artifact,
            "/standing_runtime_fencing/capability/production_bounded_failover_safe",
        )?;
        require_json_true(
            path,
            &artifact,
            "/standing_runtime_fencing/capability/production_multi_writer_safe",
        )?;
        require_json_true(
            path,
            &artifact,
            "/standing_runtime_fencing/capability/authoritative_backend_time",
        )?;
        if require_json_str(
            path,
            &artifact,
            "/standing_runtime_fencing/capability/backend_time_source_kind",
        )? != "raft_replicated_authority_time"
        {
            bail!(
                "{} release product evidence requires raft_replicated_authority_time backend time",
                path.display()
            );
        }
    }

    Ok(())
}

fn parse_product_standing_runtime_fencing_capability(
    path: &Path,
    artifact: &serde_json::Value,
) -> anyhow::Result<StandingRuntimeFencingCapability> {
    let capability = artifact
        .pointer("/standing_runtime_fencing/capability")
        .with_context(|| {
            format!(
                "{} product evidence missing /standing_runtime_fencing/capability",
                path.display()
            )
        })?;
    serde_json::from_value(capability.clone()).with_context(|| {
        format!(
            "{} standing-runtime capability schema is invalid",
            path.display()
        )
    })
}

fn validate_release_standing_runtime_fencing_capability(
    path: &Path,
    capability: &StandingRuntimeFencingCapability,
) -> anyhow::Result<()> {
    let mut missing = Vec::new();
    if capability.capability_schema_version != STANDING_RUNTIME_FENCING_CAPABILITY_SCHEMA_VERSION {
        missing.push("supported_capability_schema_version");
    }
    if capability.backend_name != "hiqlite" {
        missing.push("hiqlite_backend_name");
    }
    if capability.owner_scope_kind != STANDING_RUNTIME_OWNER_SCOPE_KIND_TENANT_PROGRAM_VIEW {
        missing.push("tenant_program_view_owner_scope");
    }
    if !capability.linearizable_owner_lease {
        missing.push("linearizable_owner_lease");
    }
    if !capability.durable_monotonic_owner_epoch {
        missing.push("durable_monotonic_owner_epoch");
    }
    if !capability.authoritative_backend_time {
        missing.push("authoritative_backend_time");
    }
    if !capability.owner_validated_checkpoint_publish {
        missing.push("owner_validated_checkpoint_publish");
    }
    if !capability.publish_checks_owner_and_latest_atomically {
        missing.push("publish_checks_owner_and_latest_atomically");
    }
    if !capability.publish_rejects_expired_owner {
        missing.push("publish_rejects_expired_owner");
    }
    if !capability.latest_read_linearizable {
        missing.push("latest_read_linearizable");
    }
    if !capability.publish_rejects_scope_mismatch {
        missing.push("publish_rejects_scope_mismatch");
    }
    if capability.max_owner_ttl_ms == 0 {
        missing.push("max_owner_ttl_ms");
    }
    if !capability.control_plane_auth_enforced {
        missing.push("control_plane_auth_enforced");
    }
    if !capability.production_multi_writer_safe {
        missing.push("production_multi_writer_safe");
    }
    if capability.backend_time_source_kind != STANDING_RUNTIME_BACKEND_TIME_SOURCE_RAFT_REPLICATED {
        missing.push("raft_replicated_authority_time_source");
    }
    if !capability.backend_time_blocked_reason.is_empty() {
        missing.push("empty_backend_time_blocked_reason");
    }
    if capability.lease_authority_kind != STANDING_RUNTIME_LEASE_AUTHORITY_KIND_RAFT_REPLICATED_TIME
    {
        missing.push("raft_replicated_time_lease_authority");
    }
    if capability.lease_expiry_semantics
        != STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_BACKEND_WALL_CLOCK_TTL
    {
        missing.push("backend_wall_clock_ttl_lease_expiry");
    }
    if !capability.bounded_wall_clock_failover {
        missing.push("bounded_wall_clock_failover");
    }
    if capability.failover_time_bound_ms == 0 {
        missing.push("failover_time_bound_ms");
    }
    if capability.failover_time_bound_ms > capability.max_owner_ttl_ms {
        missing.push("failover_time_bound_within_owner_ttl");
    }
    if !capability.multi_writer_fencing_safe {
        missing.push("multi_writer_fencing_safe");
    }
    if !capability.production_bounded_failover_safe {
        missing.push("production_bounded_failover_safe");
    }

    if missing.is_empty() {
        Ok(())
    } else {
        bail!(
            "{} standing-runtime capability schema is typed but not release-safe for product_complete; missing {}",
            path.display(),
            missing.join(", ")
        );
    }
}

fn validate_product_metadata_authority(
    path: &Path,
    artifact: &serde_json::Value,
    release_commit: Option<&str>,
) -> anyhow::Result<()> {
    require_json_true(path, artifact, "/metadata_store/enabled")?;
    let backend = require_json_str(path, artifact, "/metadata_store/backend")?;
    let normalized_backend = backend.trim().to_ascii_lowercase();
    if normalized_backend.is_empty()
        || matches!(
            normalized_backend.as_str(),
            "memory" | "in-memory" | "oss" | "object-store" | "disabled" | "local"
        )
    {
        bail!(
            "{} release product evidence requires a production metadata authority backend",
            path.display()
        );
    }
    if normalized_backend != "hiqlite" {
        bail!(
            "{} release product evidence supports metadata_store.backend=hiqlite with release attestation; backend {backend:?} is unsupported",
            path.display()
        );
    }
    let capability_backend = require_json_str(
        path,
        artifact,
        "/standing_runtime_fencing/capability/backend_name",
    )?;
    if capability_backend != backend {
        bail!(
            "{} metadata_store.backend does not match standing-runtime capability backend_name",
            path.display()
        );
    }
    let capability = parse_product_standing_runtime_fencing_capability(path, artifact)?;
    validate_release_standing_runtime_fencing_capability(path, &capability)?;
    validate_product_hiqlite_authority_attestation(path, artifact)?;
    validate_product_hiqlite_backend_time_claim(path, artifact, release_commit)?;

    Ok(())
}

fn validate_product_hiqlite_backend_time_claim(
    path: &Path,
    artifact: &serde_json::Value,
    release_commit: Option<&str>,
) -> anyhow::Result<()> {
    let backend_time_kind = require_json_str(
        path,
        artifact,
        "/standing_runtime_fencing/capability/backend_time_source_kind",
    )?;
    if backend_time_kind == "raft_replicated_authority_time" {
        if artifact
            .pointer("/metadata_store/hiqlite_backend_time_attestation")
            .is_none()
        {
            bail!(
                "{} Hiqlite authority attestation proves topology/no-PVC only; product_complete requires a separate backend-authoritative raft-time attestation",
                path.display()
            );
        }
        let trust_status =
            validate_product_hiqlite_backend_time_attestation(path, artifact, release_commit)?;
        match trust_status {
            HiqliteBackendTimeTrustStatus::Diagnostic => {
                bail!(
                    "{} Hiqlite backend-time attestation is diagnostic; product_complete requires trusted CI provenance over the canonical backend-time evidence bundle",
                    path.display()
                );
            }
            HiqliteBackendTimeTrustStatus::TrustedWithoutSigstoreBundle => {
                bail!(
                    "{} Hiqlite backend-time trusted provenance Ed25519 signature is verified, but product_complete remains fail-closed until full Sigstore certificate-chain and transparency-log verification is implemented",
                    path.display()
                );
            }
            HiqliteBackendTimeTrustStatus::SigstoreVerified => {}
        }
    }

    Ok(())
}

fn validate_product_hiqlite_backend_time_attestation(
    path: &Path,
    artifact: &serde_json::Value,
    release_commit: Option<&str>,
) -> anyhow::Result<HiqliteBackendTimeTrustStatus> {
    let prefix = "/metadata_store/hiqlite_backend_time_attestation";
    require_json_true(path, artifact, &format!("{prefix}/validated"))?;
    if require_json_str(path, artifact, &format!("{prefix}/evidence"))?
        != "hiqlite-backend-time-attestation.json"
    {
        bail!(
            "{} product evidence must attach Hiqlite backend-time evidence",
            path.display()
        );
    }
    let sibling = read_sibling_json_artifact(
        path,
        "hiqlite-backend-time-attestation.json",
        "product Hiqlite backend-time evidence",
    )?;
    if require_json_u64(path, artifact, &format!("{prefix}/schema_version"))? != 1 {
        bail!(
            "{} Hiqlite backend-time attestation has unsupported schema_version",
            path.display()
        );
    }
    if require_json_str(path, artifact, &format!("{prefix}/evidence_kind"))?
        != "velorix_hiqlite_backend_time_attestation"
    {
        bail!(
            "{} Hiqlite backend-time attestation has unsupported evidence_kind",
            path.display()
        );
    }
    if require_json_str(path, artifact, &format!("{prefix}/backend_name"))? != "hiqlite" {
        bail!(
            "{} Hiqlite backend-time attestation must target backend_name=hiqlite",
            path.display()
        );
    }
    if require_json_str(path, artifact, &format!("{prefix}/time_source_kind"))?
        != "raft_replicated_authority_time"
    {
        bail!(
            "{} Hiqlite backend-time attestation time_source_kind must be raft_replicated_authority_time",
            path.display()
        );
    }
    if require_json_str(path, artifact, &format!("{prefix}/lease_authority_kind"))?
        != "raft_replicated_time"
    {
        bail!(
            "{} Hiqlite backend-time attestation lease_authority_kind must be raft_replicated_time",
            path.display()
        );
    }
    if require_json_str(path, artifact, &format!("{prefix}/lease_expiry_semantics"))?
        != "backend_wall_clock_ttl"
    {
        bail!(
            "{} Hiqlite backend-time attestation lease_expiry_semantics must be backend_wall_clock_ttl",
            path.display()
        );
    }
    for field in [
        "authoritative_backend_time",
        "bounded_wall_clock_failover",
        "production_bounded_failover_safe",
        "authority_sampled_unix_time_ms_in_raft_operation",
        "owner_expiry_bound_to_authority_time",
        "checkpoint_publish_rejects_expired_owner_with_authority_time",
        "bounded_failover_probe_passed",
        "metrics_time_source_rejected",
        "raft_log_index_time_source_rejected",
        "distributed_lock_ttl_source_rejected",
    ] {
        require_json_true(path, artifact, &format!("{prefix}/{field}"))?;
    }
    let capability_failover_bound = require_json_u64(
        path,
        artifact,
        "/standing_runtime_fencing/capability/failover_time_bound_ms",
    )?;
    let attested_failover_bound =
        require_json_u64(path, artifact, &format!("{prefix}/failover_time_bound_ms"))?;
    if attested_failover_bound == 0 || attested_failover_bound != capability_failover_bound {
        bail!(
            "{} Hiqlite backend-time attestation failover_time_bound_ms must match the advertised capability",
            path.display()
        );
    }
    let observed_failover = require_json_u64(
        path,
        artifact,
        &format!("{prefix}/observed_max_failover_ms"),
    )?;
    if observed_failover > attested_failover_bound {
        bail!(
            "{} Hiqlite backend-time attestation observed_max_failover_ms exceeds failover_time_bound_ms",
            path.display()
        );
    }
    let attested_at = require_json_str(path, artifact, &format!("{prefix}/attested_at"))?;
    parse_rfc3339_utc_epoch_seconds(attested_at).with_context(|| {
        format!(
            "{} Hiqlite backend-time attestation has invalid attested_at",
            path.display()
        )
    })?;
    validate_recent_hiqlite_backend_time_attested_at(path, attested_at)?;
    let attester = require_json_str(path, artifact, &format!("{prefix}/attester"))?;
    validate_hiqlite_backend_time_attester(path, attester)?;
    for pointer in [
        "/schema_version",
        "/evidence_kind",
        "/backend_name",
        "/time_source_kind",
        "/lease_authority_kind",
        "/lease_expiry_semantics",
        "/authoritative_backend_time",
        "/bounded_wall_clock_failover",
        "/production_bounded_failover_safe",
        "/authority_sampled_unix_time_ms_in_raft_operation",
        "/owner_expiry_bound_to_authority_time",
        "/checkpoint_publish_rejects_expired_owner_with_authority_time",
        "/bounded_failover_probe_passed",
        "/failover_time_bound_ms",
        "/observed_max_failover_ms",
        "/metrics_time_source_rejected",
        "/raft_log_index_time_source_rejected",
        "/distributed_lock_ttl_source_rejected",
        "/attested_at",
        "/attester",
        "/trusted_for_product_complete",
        "/trusted_for_release_validator",
        "/release_validator_fail_closed",
    ] {
        let summary_pointer = format!("{prefix}{pointer}");
        let summary_value = artifact.pointer(&summary_pointer).with_context(|| {
            format!(
                "{} Hiqlite backend-time attestation missing {summary_pointer}",
                path.display()
            )
        })?;
        let sibling_value = sibling.pointer(pointer).with_context(|| {
            format!(
                "{} product Hiqlite backend-time evidence sibling hiqlite-backend-time-attestation.json missing {pointer}",
                path.display()
            )
        })?;
        if summary_value != sibling_value {
            bail!(
                "{} product Hiqlite backend-time evidence sibling hiqlite-backend-time-attestation.json {pointer} does not match {summary_pointer}",
                path.display()
            );
        }
    }
    validate_product_hiqlite_backend_time_evidence_files(
        path,
        artifact,
        &sibling,
        observed_failover,
        release_commit,
    )
}

fn validate_product_hiqlite_backend_time_evidence_files(
    path: &Path,
    artifact: &serde_json::Value,
    sibling: &serde_json::Value,
    observed_failover: u64,
    release_commit: Option<&str>,
) -> anyhow::Result<HiqliteBackendTimeTrustStatus> {
    let evidence_files = sibling
        .pointer("/evidence_files")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} product Hiqlite backend-time evidence sibling hiqlite-backend-time-attestation.json missing array /evidence_files",
                path.display()
            )
        })?;
    let mut by_kind = BTreeMap::new();
    for file in evidence_files {
        let kind = file
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .with_context(|| {
                format!(
                    "{} product Hiqlite backend-time evidence /evidence_files entry missing kind",
                    path.display()
                )
            })?;
        if by_kind.insert(kind, file).is_some() {
            bail!(
                "{} product Hiqlite backend-time evidence /evidence_files has duplicate kind {kind}",
                path.display()
            );
        }
    }

    let require_evidence_file = |kind: &str| -> anyhow::Result<(PathBuf, &serde_json::Value)> {
        let entry = by_kind.get(kind).copied().with_context(|| {
            format!(
                "{} product Hiqlite backend-time evidence /evidence_files missing kind {kind}",
                path.display()
            )
        })?;
        let evidence_path = entry
            .get("path")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .with_context(|| {
                format!(
                    "{} product Hiqlite backend-time evidence /evidence_files {kind} missing path",
                    path.display()
                )
            })?;
        let sibling_path = if kind == "product_evidence" {
            let product_filename = path.file_name().and_then(|value| value.to_str());
            if product_filename != Some(evidence_path) {
                bail!(
                    "{} product Hiqlite backend-time evidence /evidence_files product_evidence path must match the validated product evidence filename",
                    path.display()
                );
            }
            path.to_path_buf()
        } else {
            sibling_evidence_path(path, evidence_path, "product Hiqlite backend-time evidence")?
        };
        let expected_size = entry
            .get("size_bytes")
            .and_then(serde_json::Value::as_u64)
            .with_context(|| {
                format!(
                    "{} product Hiqlite backend-time evidence /evidence_files {kind} missing size_bytes",
                    path.display()
                )
            })?;
        let canonicalization = entry
            .get("canonicalization")
            .and_then(serde_json::Value::as_str);
        let canonicalized_bytes = if canonicalization
            == Some("without_metadata_store_hiqlite_backend_time_attestation")
        {
            if kind != "product_evidence" {
                bail!(
                    "{} product Hiqlite backend-time evidence /evidence_files {kind} cannot use product_evidence canonicalization",
                    path.display()
                );
            }
            Some(canonical_product_evidence_without_backend_time_attestation_bytes(&sibling_path)?)
        } else {
            if kind == "product_evidence" {
                bail!(
                    "{} product Hiqlite backend-time evidence /evidence_files product_evidence must use canonicalization=without_metadata_store_hiqlite_backend_time_attestation",
                    path.display()
                );
            }
            if canonicalization.is_some() {
                bail!(
                    "{} product Hiqlite backend-time evidence /evidence_files {kind} has unsupported canonicalization",
                    path.display()
                );
            }
            None
        };
        let actual_size = if let Some(bytes) = &canonicalized_bytes {
            bytes.len() as u64
        } else {
            fs::metadata(&sibling_path)
                .with_context(|| {
                    format!(
                        "{} product Hiqlite backend-time evidence failed to stat {}",
                        path.display(),
                        sibling_path.display()
                    )
                })?
                .len()
        };
        if actual_size != expected_size {
            bail!(
                "{} product Hiqlite backend-time evidence /evidence_files {kind} size_bytes mismatch",
                path.display()
            );
        }
        let expected_sha256 = entry
            .get("sha256")
            .and_then(serde_json::Value::as_str)
            .with_context(|| {
                format!(
                    "{} product Hiqlite backend-time evidence /evidence_files {kind} missing sha256",
                    path.display()
                )
            })?;
        let actual_sha256 = if let Some(bytes) = &canonicalized_bytes {
            sha256_hex_of_bytes(bytes)
        } else {
            sha256_hex_of_file(&sibling_path)?
        };
        if expected_sha256 != actual_sha256 {
            bail!(
                "{} product Hiqlite backend-time evidence /evidence_files {kind} sha256 mismatch",
                path.display()
            );
        }
        Ok((sibling_path, entry))
    };

    let (_product_evidence, _entry) = require_evidence_file("product_evidence")?;
    let (assessment_path, _entry) = require_evidence_file("hiqlite_backend_time_assessment")?;
    let (readyz_path, _entry) = require_evidence_file("readyz")?;
    let (multi_replica_path, _entry) = require_evidence_file("multi_replica_fencing_smoke")?;
    let (failover_path, _entry) = require_evidence_file("standing_runtime_failover_smoke")?;
    let (meta_smoke_log_path, _entry) = require_evidence_file("metadata_adversarial_smoke_log")?;

    let assessment: serde_json::Value = read_json_artifact(&assessment_path)?;
    if require_json_str(&assessment_path, &assessment, "/evidence_kind")?
        != "velorix_hiqlite_backend_time_assessment"
    {
        bail!(
            "{} Hiqlite backend-time assessment evidence has unsupported evidence_kind",
            assessment_path.display()
        );
    }
    require_json_true(&assessment_path, &assessment, "/required_mode_supported")?;
    require_json_true(
        &assessment_path,
        &assessment,
        "/can_generate_product_complete_backend_time_attestation",
    )?;
    if require_json_str(&assessment_path, &assessment, "/backend_time_source_kind")?
        != "raft_replicated_authority_time"
    {
        bail!(
            "{} Hiqlite backend-time assessment must use raft_replicated_authority_time",
            assessment_path.display()
        );
    }
    let missing_capabilities = assessment
        .pointer("/missing_capabilities")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} Hiqlite backend-time assessment missing /missing_capabilities",
                assessment_path.display()
            )
        })?;
    if !missing_capabilities.is_empty() {
        bail!(
            "{} Hiqlite backend-time assessment still reports missing capabilities",
            assessment_path.display()
        );
    }
    if assessment.pointer("/product_capability")
        != artifact.pointer("/standing_runtime_fencing/capability")
    {
        bail!(
            "{} Hiqlite backend-time assessment product_capability does not match product evidence",
            assessment_path.display()
        );
    }
    for pointer in [
        "/velorix_meta_runtime/owner_acquire_uses_authority_time",
        "/velorix_meta_runtime/owner_read_uses_authority_time",
        "/velorix_meta_runtime/checkpoint_publish_update_uses_authority_time",
        "/velorix_meta_runtime/checkpoint_publish_insert_uses_authority_time",
        "/velorix_meta_runtime/checkpoint_publish_rejects_scope_mismatch",
        "/velorix_meta_runtime/unsafe_runtime_time_sources_absent",
    ] {
        require_json_true(&assessment_path, &assessment, pointer)?;
    }

    let readyz: serde_json::Value = read_json_artifact(&readyz_path)?;
    if readyz.pointer("/metadata_store/standing_runtime_fencing")
        != artifact.pointer("/standing_runtime_fencing/capability")
    {
        bail!(
            "{} readyz standing_runtime_fencing does not match product evidence capability",
            readyz_path.display()
        );
    }

    let multi_replica: serde_json::Value = read_json_artifact(&multi_replica_path)?;
    if require_json_str(&multi_replica_path, &multi_replica, "/evidence_kind")?
        != "velorix_deployed_multi_replica_fencing_smoke"
    {
        bail!(
            "{} multi-replica fencing smoke has unsupported evidence_kind",
            multi_replica_path.display()
        );
    }
    if require_json_str(&multi_replica_path, &multi_replica, "/status")? != "pass" {
        bail!(
            "{} multi-replica fencing smoke must have status=pass",
            multi_replica_path.display()
        );
    }
    for pointer in [
        "/assertions/distinct_api_pods",
        "/assertions/non_owner_ingest_rejected",
        "/assertions/owner_retry_converged",
        "/assertions/read_replica_served_query",
    ] {
        require_json_true(&multi_replica_path, &multi_replica, pointer)?;
    }

    let failover: serde_json::Value = read_json_artifact(&failover_path)?;
    let trusted_for_release_validator =
        require_json_bool(path, sibling, "/trusted_for_release_validator")?;
    let capability_owner_ttl_ms = require_json_u64(
        path,
        artifact,
        "/standing_runtime_fencing/capability/max_owner_ttl_ms",
    )?;
    let capability_failover_bound_ms = require_json_u64(
        path,
        artifact,
        "/standing_runtime_fencing/capability/failover_time_bound_ms",
    )?;
    if require_json_str(&failover_path, &failover, "/evidence_kind")?
        != "velorix_standing_runtime_failover_smoke"
    {
        bail!(
            "{} standing-runtime failover smoke has unsupported evidence_kind",
            failover_path.display()
        );
    }
    if require_json_str(&failover_path, &failover, "/status")? != "pass" {
        bail!(
            "{} standing-runtime failover smoke must have status=pass",
            failover_path.display()
        );
    }
    validate_hiqlite_backend_time_failover_evidence(
        &failover_path,
        &failover,
        observed_failover,
        trusted_for_release_validator,
        capability_failover_bound_ms,
        capability_owner_ttl_ms,
    )?;

    let meta_smoke_log = fs::read_to_string(&meta_smoke_log_path).with_context(|| {
        format!(
            "{} failed to read metadata adversarial smoke log",
            meta_smoke_log_path.display()
        )
    })?;
    for fragment in [
        "standing runtime adversarial smoke ok",
        "owner_a_epoch=",
        "owner_b_epoch=",
        "latest_epoch=",
        "backend_time_source_kind=raft_replicated_authority_time",
    ] {
        if !meta_smoke_log.contains(fragment) {
            bail!(
                "{} metadata adversarial smoke log missing {fragment}",
                meta_smoke_log_path.display()
            );
        }
    }

    validate_product_hiqlite_backend_time_trusted_provenance(
        path,
        artifact,
        sibling,
        &by_kind,
        release_commit,
    )
}

fn validate_hiqlite_backend_time_failover_evidence(
    failover_path: &Path,
    failover: &serde_json::Value,
    observed_failover: u64,
    trusted_for_release_validator: bool,
    capability_failover_bound_ms: u64,
    capability_owner_ttl_ms: u64,
) -> anyhow::Result<()> {
    if trusted_for_release_validator {
        require_json_true(failover_path, failover, "/trusted_for_product_complete")?;
        require_json_true(
            failover_path,
            failover,
            "/production_wall_clock_failover_attestation",
        )?;
        if require_json_str(failover_path, failover, "/evidence_scope")?
            != "release_ci_deployed_product"
        {
            bail!(
                "{} release Hiqlite backend-time failover evidence requires evidence_scope=release_ci_deployed_product",
                failover_path.display()
            );
        }
        if require_json_str(failover_path, failover, "/failover_probe_kind")?
            != "release_bounded_wall_clock_failover"
        {
            bail!(
                "{} release Hiqlite backend-time failover evidence requires failover_probe_kind=release_bounded_wall_clock_failover",
                failover_path.display()
            );
        }
        if require_json_str(failover_path, failover, "/backend_time_source_kind")?
            != "raft_replicated_authority_time"
        {
            bail!(
                "{} release Hiqlite backend-time failover evidence requires raft_replicated_authority_time",
                failover_path.display()
            );
        }
        require_json_true(failover_path, failover, "/authority_time_observed")?;
        let owner_ttl = require_json_u64(failover_path, failover, "/owner_ttl_ms")?;
        if owner_ttl != capability_owner_ttl_ms {
            bail!(
                "{} release Hiqlite backend-time failover evidence owner_ttl_ms does not match capability",
                failover_path.display()
            );
        }
        let failover_bound = require_json_u64(failover_path, failover, "/failover_time_bound_ms")?;
        if failover_bound != capability_failover_bound_ms {
            bail!(
                "{} release Hiqlite backend-time failover evidence failover_time_bound_ms does not match capability",
                failover_path.display()
            );
        }
        let pre_epoch = require_json_u64(failover_path, failover, "/pre_failover_owner_epoch")?;
        let post_epoch = require_json_u64(failover_path, failover, "/post_failover_owner_epoch")?;
        if post_epoch <= pre_epoch {
            bail!(
                "{} release Hiqlite backend-time failover evidence post_failover_owner_epoch must advance",
                failover_path.display()
            );
        }
        let affected_pods =
            require_json_string_array(failover_path, failover, "/affected_api_pods")?;
        if affected_pods.is_empty() {
            bail!(
                "{} release Hiqlite backend-time failover evidence requires affected_api_pods",
                failover_path.display()
            );
        }
    } else {
        require_json_false(failover_path, failover, "/trusted_for_product_complete")?;
        require_json_false(
            failover_path,
            failover,
            "/production_wall_clock_failover_attestation",
        )?;
    }
    let smoke_observed_failover =
        require_json_u64(failover_path, failover, "/observed_failover_ms")?;
    if smoke_observed_failover != observed_failover {
        bail!(
            "{} standing-runtime failover smoke observed_failover_ms does not match backend-time attestation",
            failover_path.display()
        );
    }

    Ok(())
}

fn validate_product_hiqlite_backend_time_trusted_provenance(
    path: &Path,
    artifact: &serde_json::Value,
    sibling: &serde_json::Value,
    evidence_files: &BTreeMap<&str, &serde_json::Value>,
    release_commit: Option<&str>,
) -> anyhow::Result<HiqliteBackendTimeTrustStatus> {
    let prefix = "/metadata_store/hiqlite_backend_time_attestation";
    let trusted_for_release_validator = require_json_bool(
        path,
        artifact,
        &format!("{prefix}/trusted_for_release_validator"),
    )?;
    let trusted_for_product_complete = require_json_bool(
        path,
        artifact,
        &format!("{prefix}/trusted_for_product_complete"),
    )?;
    let release_validator_fail_closed = require_json_bool(
        path,
        artifact,
        &format!("{prefix}/release_validator_fail_closed"),
    )?;

    if !trusted_for_release_validator {
        if trusted_for_product_complete || !release_validator_fail_closed {
            bail!(
                "{} diagnostic Hiqlite backend-time attestation must keep product-complete trust disabled",
                path.display()
            );
        }
        return Ok(HiqliteBackendTimeTrustStatus::Diagnostic);
    }
    if !trusted_for_product_complete || release_validator_fail_closed {
        bail!(
            "{} trusted Hiqlite backend-time attestation must clear release fail-closed flags",
            path.display()
        );
    }

    let sibling_provenance = sibling.pointer("/trusted_provenance").with_context(|| {
        format!(
            "{} product Hiqlite backend-time evidence sibling hiqlite-backend-time-attestation.json missing /trusted_provenance",
            path.display()
        )
    })?;

    if require_json_u64(path, sibling_provenance, "/schema_version")? != 1 {
        bail!(
            "{} Hiqlite backend-time trusted provenance has unsupported schema_version",
            path.display()
        );
    }
    if require_json_str(path, sibling_provenance, "/source_repository")?
        != HIQLITE_BACKEND_TIME_TRUSTED_SOURCE_REPOSITORY
    {
        bail!(
            "{} Hiqlite backend-time trusted provenance source_repository is not trusted",
            path.display()
        );
    }
    if require_json_str(path, sibling_provenance, "/provenance_kind")?
        != HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE_KIND
    {
        bail!(
            "{} Hiqlite backend-time trusted provenance has unsupported provenance_kind",
            path.display()
        );
    }
    let provenance_attester = require_json_str(path, sibling_provenance, "/attester")?;
    if !HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE_ATTESTERS.contains(&provenance_attester) {
        bail!(
            "{} Hiqlite backend-time trusted provenance attester is not trusted: {provenance_attester}",
            path.display()
        );
    }
    let top_level_attester = require_json_str(path, artifact, &format!("{prefix}/attester"))?;
    if provenance_attester != top_level_attester {
        bail!(
            "{} Hiqlite backend-time trusted provenance attester does not match attestation attester",
            path.display()
        );
    }

    for pointer in [
        "/source_repository",
        "/source_revision",
        "/workflow_name",
        "/workflow_run_id",
        "/job_name",
        "/subject_image_digest",
        "/generated_at",
        "/canonical_bundle_sha256",
    ] {
        require_nonempty_json_str(path, sibling_provenance, pointer)?;
    }
    let source_revision = require_json_str(path, sibling_provenance, "/source_revision")?;
    if is_placeholder_commit(source_revision) {
        bail!(
            "{} Hiqlite backend-time trusted provenance uses placeholder source_revision",
            path.display()
        );
    }
    validate_full_git_commit_sha(path, source_revision, "source_revision")?;
    let Some(release_commit) = release_commit else {
        bail!(
            "{} Hiqlite backend-time trusted provenance requires --release-commit",
            path.display()
        );
    };
    validate_full_git_commit_sha(path, release_commit, "release_commit")?;
    if source_revision != release_commit {
        bail!(
            "{} Hiqlite backend-time trusted provenance source_revision does not match release_commit",
            path.display()
        );
    }
    let subject_image_digest = require_json_str(path, sibling_provenance, "/subject_image_digest")?;
    validate_sha256_digest(path, subject_image_digest, "subject_image_digest")?;
    validate_hiqlite_backend_time_subject_images(
        path,
        artifact,
        sibling_provenance,
        subject_image_digest,
    )?;
    validate_hiqlite_backend_time_ci_identity(path, sibling_provenance, release_commit)?;
    let sigstore_verified =
        validate_hiqlite_backend_time_signature_bundle(path, sibling_provenance)?;
    if let Some(authority_image_digest) = artifact
        .pointer("/metadata_store/hiqlite_authority_attestation/image_digest")
        .and_then(serde_json::Value::as_str)
    {
        if subject_image_digest != authority_image_digest {
            bail!(
                "{} Hiqlite backend-time trusted provenance subject_image_digest does not match Hiqlite authority attestation",
                path.display()
            );
        }
    }
    let generated_at = require_json_str(path, sibling_provenance, "/generated_at")?;
    validate_recent_hiqlite_backend_time_attested_at(path, generated_at)?;

    validate_hiqlite_backend_time_canonical_bundle_entries(
        path,
        sibling_provenance,
        evidence_files,
    )?;

    if sigstore_verified {
        Ok(HiqliteBackendTimeTrustStatus::SigstoreVerified)
    } else {
        Ok(HiqliteBackendTimeTrustStatus::TrustedWithoutSigstoreBundle)
    }
}

fn validate_hiqlite_backend_time_ci_identity(
    path: &Path,
    provenance: &serde_json::Value,
    release_commit: &str,
) -> anyhow::Result<()> {
    let identity = provenance.pointer("/ci_identity").with_context(|| {
        format!(
            "{} Hiqlite backend-time trusted provenance missing /ci_identity",
            path.display()
        )
    })?;
    if require_json_str(path, identity, "/identity_kind")? != "github_actions_oidc" {
        bail!(
            "{} Hiqlite backend-time trusted provenance ci_identity must be github_actions_oidc",
            path.display()
        );
    }
    if require_json_str(path, identity, "/issuer")? != HIQLITE_BACKEND_TIME_TRUSTED_OIDC_ISSUER {
        bail!(
            "{} Hiqlite backend-time trusted provenance ci_identity issuer is not trusted",
            path.display()
        );
    }
    if require_json_str(path, identity, "/audience")? != HIQLITE_BACKEND_TIME_TRUSTED_OIDC_AUDIENCE
    {
        bail!(
            "{} Hiqlite backend-time trusted provenance ci_identity audience is not trusted",
            path.display()
        );
    }
    if require_json_str(path, identity, "/repository")?
        != HIQLITE_BACKEND_TIME_TRUSTED_GITHUB_REPOSITORY
    {
        bail!(
            "{} Hiqlite backend-time trusted provenance ci_identity repository is not trusted",
            path.display()
        );
    }
    let subject = require_json_str(path, identity, "/subject")?;
    if !subject.starts_with("repo:mrchypark/velorix:") {
        bail!(
            "{} Hiqlite backend-time trusted provenance ci_identity subject is not trusted",
            path.display()
        );
    }
    let workflow_ref = require_json_str(path, identity, "/workflow_ref")?;
    let Some(workflow_release_ref) =
        workflow_ref.strip_prefix(HIQLITE_BACKEND_TIME_TRUSTED_WORKFLOW_REF_PREFIX)
    else {
        bail!(
            "{} Hiqlite backend-time trusted provenance ci_identity workflow_ref is not trusted",
            path.display()
        );
    };
    validate_hiqlite_backend_time_trusted_release_ref(
        path,
        workflow_release_ref,
        "ci_identity.workflow_ref",
    )?;
    if subject != format!("repo:mrchypark/velorix:ref:{workflow_release_ref}") {
        bail!(
            "{} Hiqlite backend-time trusted provenance ci_identity subject does not match trusted release workflow_ref",
            path.display()
        );
    }
    let workflow_sha = require_json_str(path, identity, "/workflow_sha")?;
    validate_full_git_commit_sha(path, workflow_sha, "ci_identity.workflow_sha")?;
    if workflow_sha != release_commit {
        bail!(
            "{} Hiqlite backend-time trusted provenance ci_identity workflow_sha does not match release_commit",
            path.display()
        );
    }
    for pointer in ["/run_id", "/run_attempt", "/job_workflow_ref"] {
        require_nonempty_json_str(path, identity, pointer)?;
    }
    if require_json_str(path, identity, "/job_workflow_ref")?
        != format!(
            "{}{}",
            HIQLITE_BACKEND_TIME_TRUSTED_WORKFLOW_REF_PREFIX, release_commit
        )
    {
        bail!(
            "{} Hiqlite backend-time trusted provenance ci_identity job_workflow_ref does not match release_commit",
            path.display()
        );
    }
    Ok(())
}

fn validate_hiqlite_backend_time_trusted_release_ref(
    path: &Path,
    release_ref: &str,
    field: &str,
) -> anyhow::Result<()> {
    if release_ref == HIQLITE_BACKEND_TIME_TRUSTED_RELEASE_BRANCH_REF
        || release_ref
            .strip_prefix(HIQLITE_BACKEND_TIME_TRUSTED_RELEASE_TAG_REF_PREFIX)
            .is_some_and(|suffix| !suffix.trim().is_empty())
    {
        return Ok(());
    }
    bail!(
        "{} Hiqlite backend-time trusted provenance {field} must use refs/heads/main or refs/tags/v*",
        path.display()
    );
}

fn validate_hiqlite_backend_time_signature_bundle(
    path: &Path,
    provenance: &serde_json::Value,
) -> anyhow::Result<bool> {
    let signature_bundle = provenance.pointer("/signature_bundle").with_context(|| {
        format!(
            "{} Hiqlite backend-time trusted provenance missing /signature_bundle",
            path.display()
        )
    })?;
    if require_json_str(path, signature_bundle, "/bundle_kind")? != "sigstore_rekor_dsse" {
        bail!(
            "{} Hiqlite backend-time trusted provenance signature_bundle bundle_kind is unsupported",
            path.display()
        );
    }
    let signed_payload_sha256 = require_json_str(path, signature_bundle, "/signed_payload_sha256")?;
    validate_sha256_digest(
        path,
        signed_payload_sha256,
        "signature_bundle.signed_payload_sha256",
    )?;
    if signed_payload_sha256 != require_json_str(path, provenance, "/canonical_bundle_sha256")? {
        bail!(
            "{} Hiqlite backend-time trusted provenance signature_bundle signed_payload_sha256 does not match canonical_bundle_sha256",
            path.display()
        );
    }
    for (pointer, label) in [
        (
            "/signing_certificate_sha256",
            "signature_bundle.signing_certificate_sha256",
        ),
        (
            "/transparency_log_id",
            "signature_bundle.transparency_log_id",
        ),
        (
            "/inclusion_proof_sha256",
            "signature_bundle.inclusion_proof_sha256",
        ),
    ] {
        validate_sha256_digest(
            path,
            require_json_str(path, signature_bundle, pointer)?,
            label,
        )?;
    }
    let sigstore_bundle_present = signature_bundle
        .pointer("/sigstore_bundle_base64")
        .is_some();
    if require_json_str(path, signature_bundle, "/oidc_issuer")?
        != HIQLITE_BACKEND_TIME_TRUSTED_OIDC_ISSUER
    {
        bail!(
            "{} Hiqlite backend-time trusted provenance signature_bundle oidc_issuer is not trusted",
            path.display()
        );
    }
    let certificate_identity = require_json_str(path, signature_bundle, "/certificate_identity")?;
    if sigstore_bundle_present {
        let Some(certificate_release_ref) = certificate_identity
            .strip_prefix(HIQLITE_BACKEND_TIME_TRUSTED_SIGSTORE_CERTIFICATE_IDENTITY_PREFIX)
        else {
            bail!(
                "{} Hiqlite backend-time trusted provenance signature_bundle certificate_identity is not trusted",
                path.display()
            );
        };
        validate_hiqlite_backend_time_trusted_release_ref(
            path,
            certificate_release_ref,
            "signature_bundle.certificate_identity",
        )?;
        let workflow_ref = require_json_str(path, provenance, "/ci_identity/workflow_ref")?;
        let workflow_release_ref = workflow_ref
            .strip_prefix(HIQLITE_BACKEND_TIME_TRUSTED_WORKFLOW_REF_PREFIX)
            .unwrap_or_default();
        if certificate_release_ref != workflow_release_ref {
            bail!(
                "{} Hiqlite backend-time trusted provenance signature_bundle certificate_identity does not match ci_identity workflow_ref",
                path.display()
            );
        }
    } else if certificate_identity != require_json_str(path, provenance, "/ci_identity/subject")? {
        bail!(
            "{} Hiqlite backend-time trusted provenance signature_bundle certificate_identity does not match ci_identity subject",
            path.display()
        );
    }
    let transparency_log_index =
        require_json_u64(path, signature_bundle, "/transparency_log_index")?;
    if !sigstore_bundle_present && transparency_log_index == 0 {
        bail!(
            "{} Hiqlite backend-time trusted provenance signature_bundle transparency_log_index must be nonzero",
            path.display()
        );
    }
    if require_json_u64(path, signature_bundle, "/integrated_time_unix")? == 0 {
        bail!(
            "{} Hiqlite backend-time trusted provenance signature_bundle integrated_time_unix must be nonzero",
            path.display()
        );
    }
    if sigstore_bundle_present {
        return validate_hiqlite_backend_time_sigstore_bundle(
            path,
            signature_bundle,
            signed_payload_sha256,
        );
    }

    validate_hiqlite_backend_time_legacy_ed25519_signature(
        path,
        signature_bundle,
        signed_payload_sha256,
    )?;
    Ok(false)
}

fn validate_hiqlite_backend_time_legacy_ed25519_signature(
    path: &Path,
    signature_bundle: &serde_json::Value,
    signed_payload_sha256: &str,
) -> anyhow::Result<()> {
    if require_json_str(path, signature_bundle, "/signature_algorithm")? != "ed25519" {
        bail!(
            "{} Hiqlite backend-time trusted provenance signature_bundle signature_algorithm is unsupported",
            path.display()
        );
    }
    let public_key_base64 = require_json_str(path, signature_bundle, "/public_key_base64")?;
    let public_key = decode_base64_field(
        path,
        public_key_base64,
        "signature_bundle.public_key_base64",
    )?;
    if public_key.len() != 32 {
        bail!(
            "{} Hiqlite backend-time trusted provenance signature_bundle public_key_base64 must decode to a 32-byte Ed25519 public key",
            path.display()
        );
    }
    let public_key_sha256 = require_json_str(path, signature_bundle, "/public_key_sha256")?;
    validate_sha256_digest(
        path,
        public_key_sha256,
        "signature_bundle.public_key_sha256",
    )?;
    if public_key_sha256 != sha256_digest_of_bytes(&public_key) {
        bail!(
            "{} Hiqlite backend-time trusted provenance signature_bundle public_key_sha256 does not match public_key_base64",
            path.display()
        );
    }
    let signature = decode_base64_field(
        path,
        require_json_str(path, signature_bundle, "/signature_base64")?,
        "signature_bundle.signature_base64",
    )?;
    if signature.len() != 64 {
        bail!(
            "{} Hiqlite backend-time trusted provenance signature_bundle signature_base64 must decode to a 64-byte Ed25519 signature",
            path.display()
        );
    }
    UnparsedPublicKey::new(&ED25519, &public_key)
        .verify(signed_payload_sha256.as_bytes(), &signature)
        .map_err(|_| {
            anyhow::anyhow!(
                "{} Hiqlite backend-time trusted provenance signature_bundle Ed25519 signature verification failed",
                path.display()
            )
        })?;
    Ok(())
}

fn validate_hiqlite_backend_time_sigstore_bundle(
    path: &Path,
    signature_bundle: &serde_json::Value,
    signed_payload_sha256: &str,
) -> anyhow::Result<bool> {
    let Some(sigstore_bundle_base64) = signature_bundle
        .pointer("/sigstore_bundle_base64")
        .and_then(serde_json::Value::as_str)
    else {
        return Ok(false);
    };

    let sigstore_bundle_bytes = decode_base64_field(
        path,
        sigstore_bundle_base64,
        "signature_bundle.sigstore_bundle_base64",
    )?;
    let sigstore_bundle_sha256 =
        require_json_str(path, signature_bundle, "/sigstore_bundle_sha256")?;
    validate_sha256_digest(
        path,
        sigstore_bundle_sha256,
        "signature_bundle.sigstore_bundle_sha256",
    )?;
    if sigstore_bundle_sha256 != sha256_digest_of_bytes(&sigstore_bundle_bytes) {
        bail!(
            "{} Hiqlite backend-time trusted provenance signature_bundle sigstore_bundle_sha256 does not match sigstore_bundle_base64",
            path.display()
        );
    }
    let sigstore_bundle_json = std::str::from_utf8(&sigstore_bundle_bytes).with_context(|| {
        format!(
            "{} Hiqlite backend-time trusted provenance signature_bundle sigstore_bundle_base64 must decode to UTF-8 JSON",
            path.display()
        )
    })?;
    let bundle = SigstoreBundle::from_json(sigstore_bundle_json).with_context(|| {
        format!(
            "{} Hiqlite backend-time trusted provenance Sigstore bundle verification failed while parsing bundle JSON",
            path.display()
        )
    })?;
    if !bundle.has_inclusion_proof() {
        bail!(
            "{} Hiqlite backend-time trusted provenance Sigstore bundle verification failed: bundle is missing Rekor inclusion proof",
            path.display()
        );
    }
    let Some(signing_certificate) = bundle.signing_certificate() else {
        bail!(
            "{} Hiqlite backend-time trusted provenance Sigstore bundle verification failed: bundle is missing Fulcio signing certificate",
            path.display()
        );
    };
    if require_json_str(path, signature_bundle, "/signing_certificate_sha256")?
        != sha256_digest_of_bytes(signing_certificate.as_bytes())
    {
        bail!(
            "{} Hiqlite backend-time trusted provenance Sigstore signing certificate digest does not match signature_bundle signing_certificate_sha256",
            path.display()
        );
    }
    let Some(tlog_entry) = bundle.verification_material.tlog_entries.first() else {
        bail!(
            "{} Hiqlite backend-time trusted provenance Sigstore bundle verification failed: bundle is missing Rekor transparency log entry",
            path.display()
        );
    };
    if tlog_entry.log_index.as_u64()
        != Some(require_json_u64(
            path,
            signature_bundle,
            "/transparency_log_index",
        )?)
    {
        bail!(
            "{} Hiqlite backend-time trusted provenance Sigstore transparency log index does not match signature_bundle transparency_log_index",
            path.display()
        );
    }
    let log_key_id = tlog_entry.log_id.key_id.decode().with_context(|| {
        format!(
            "{} Hiqlite backend-time trusted provenance Sigstore transparency log id is not valid base64",
            path.display()
        )
    })?;
    if sha256_digest_of_bytes(&log_key_id)
        != require_json_str(path, signature_bundle, "/transparency_log_id")?
    {
        bail!(
            "{} Hiqlite backend-time trusted provenance Sigstore transparency log id does not match signature_bundle transparency_log_id",
            path.display()
        );
    }

    let trusted_root = TrustedRoot::from_json(SIGSTORE_PRODUCTION_TRUSTED_ROOT).with_context(|| {
        format!(
            "{} Hiqlite backend-time trusted provenance failed to load Sigstore production trusted root",
            path.display()
        )
    })?;
    let artifact_digest = sigstore_sha256_hash_from_prefixed_digest(
        path,
        signed_payload_sha256,
        "signature_bundle.signed_payload_sha256",
    )?;
    let certificate_identity = require_json_str(path, signature_bundle, "/certificate_identity")?;
    let policy = SigstoreVerificationPolicy::default()
        .require_identity(certificate_identity)
        .require_issuer(HIQLITE_BACKEND_TIME_TRUSTED_OIDC_ISSUER);
    let result = verify_sigstore_bundle(artifact_digest, &bundle, &policy, &trusted_root)
        .with_context(|| {
            format!(
                "{} Hiqlite backend-time trusted provenance Sigstore bundle verification failed",
                path.display()
            )
        })?;
    if result.identity.as_deref() != Some(certificate_identity) {
        bail!(
            "{} Hiqlite backend-time trusted provenance Sigstore verified identity does not match signature_bundle certificate_identity",
            path.display()
        );
    }
    if result.issuer.as_deref() != Some(HIQLITE_BACKEND_TIME_TRUSTED_OIDC_ISSUER) {
        bail!(
            "{} Hiqlite backend-time trusted provenance Sigstore verified issuer is not trusted",
            path.display()
        );
    }
    let Some(integrated_time) = result.integrated_time else {
        bail!(
            "{} Hiqlite backend-time trusted provenance Sigstore bundle verification failed: verified Rekor integrated time is missing",
            path.display()
        );
    };
    if integrated_time < 0
        || integrated_time as u64
            != require_json_u64(path, signature_bundle, "/integrated_time_unix")?
    {
        bail!(
            "{} Hiqlite backend-time trusted provenance Sigstore verified integrated time does not match signature_bundle integrated_time_unix",
            path.display()
        );
    }

    Ok(true)
}

fn validate_hiqlite_backend_time_subject_images(
    path: &Path,
    artifact: &serde_json::Value,
    provenance: &serde_json::Value,
    legacy_subject_image_digest: &str,
) -> anyhow::Result<()> {
    let images = provenance
        .pointer("/subject_images")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} Hiqlite backend-time trusted provenance missing array /subject_images",
                path.display()
            )
        })?;
    let mut by_role = BTreeMap::new();
    for image in images {
        let role = require_json_str(path, image, "/role")?;
        if !HIQLITE_BACKEND_TIME_REQUIRED_SUBJECT_IMAGE_ROLES.contains(&role) {
            bail!(
                "{} Hiqlite backend-time trusted provenance subject_images has unsupported role {role}",
                path.display()
            );
        }
        let image_digest = require_json_str(path, image, "/image_digest")?;
        validate_sha256_digest(
            path,
            image_digest,
            &format!("subject_images[{role}].image_digest"),
        )?;
        if by_role.insert(role, image_digest).is_some() {
            bail!(
                "{} Hiqlite backend-time trusted provenance subject_images has duplicate role {role}",
                path.display()
            );
        }
    }
    for required_role in HIQLITE_BACKEND_TIME_REQUIRED_SUBJECT_IMAGE_ROLES {
        if !by_role.contains_key(required_role) {
            bail!(
                "{} Hiqlite backend-time trusted provenance subject_images missing required role {required_role}",
                path.display()
            );
        }
    }

    let hiqlite_subject_digest = by_role["hiqlite-authority"];
    if legacy_subject_image_digest != hiqlite_subject_digest {
        bail!(
            "{} Hiqlite backend-time trusted provenance subject_image_digest does not match subject_images hiqlite-authority",
            path.display()
        );
    }
    if let Some(authority_image_digest) = artifact
        .pointer("/metadata_store/hiqlite_authority_attestation/image_digest")
        .and_then(serde_json::Value::as_str)
    {
        if hiqlite_subject_digest != authority_image_digest {
            bail!(
                "{} Hiqlite backend-time trusted provenance subject_images hiqlite-authority image_digest does not match Hiqlite authority attestation",
                path.display()
            );
        }
    }
    for role in ["velorix-api", "velorix-meta"] {
        let Some(product_image_digest) = product_deployed_image_digest(artifact, role) else {
            bail!(
                "{} Hiqlite backend-time trusted provenance cannot bind subject_images {role} without product deployed image evidence",
                path.display()
            );
        };
        if by_role[role] != product_image_digest {
            bail!(
                "{} Hiqlite backend-time trusted provenance subject_images {role} image_digest does not match product deployed image evidence",
                path.display()
            );
        }
    }

    Ok(())
}

fn validate_hiqlite_backend_time_canonical_bundle_entries(
    path: &Path,
    provenance: &serde_json::Value,
    evidence_files: &BTreeMap<&str, &serde_json::Value>,
) -> anyhow::Result<()> {
    let entries = provenance
        .pointer("/canonical_bundle_entries")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} Hiqlite backend-time trusted provenance missing array /canonical_bundle_entries",
                path.display()
            )
        })?;
    if entries.len() != evidence_files.len() {
        bail!(
            "{} Hiqlite backend-time trusted provenance canonical_bundle_entries length mismatch",
            path.display()
        );
    }
    let mut provenance_entries = BTreeMap::new();
    for entry in entries {
        let kind = require_json_str(path, entry, "/kind")?;
        if provenance_entries.insert(kind, entry).is_some() {
            bail!(
                "{} Hiqlite backend-time trusted provenance has duplicate canonical bundle kind {kind}",
                path.display()
            );
        }
    }
    for (kind, evidence_file) in evidence_files {
        let provenance_entry = provenance_entries.get(kind).copied().with_context(|| {
            format!(
                "{} Hiqlite backend-time trusted provenance missing canonical bundle kind {kind}",
                path.display()
            )
        })?;
        for pointer in ["/path", "/sha256", "/size_bytes", "/canonicalization"] {
            if provenance_entry.pointer(pointer) != evidence_file.pointer(pointer) {
                bail!(
                    "{} Hiqlite backend-time trusted provenance canonical bundle {kind} {pointer} does not match evidence_files",
                    path.display()
                );
            }
        }
    }

    let expected_digest = require_json_str(path, provenance, "/canonical_bundle_sha256")?;
    validate_sha256_digest(path, expected_digest, "canonical_bundle_sha256")?;
    let actual_digest = hiqlite_backend_time_canonical_bundle_sha256(evidence_files)?;
    if expected_digest != actual_digest {
        bail!(
            "{} Hiqlite backend-time trusted provenance canonical_bundle_sha256 mismatch",
            path.display()
        );
    }

    Ok(())
}

fn hiqlite_backend_time_canonical_bundle_sha256(
    evidence_files: &BTreeMap<&str, &serde_json::Value>,
) -> anyhow::Result<String> {
    let mut canonical = String::new();
    for (kind, entry) in evidence_files {
        let path = entry
            .get("path")
            .and_then(serde_json::Value::as_str)
            .with_context(|| format!("Hiqlite backend-time evidence file {kind} missing path"))?;
        let sha256 = entry
            .get("sha256")
            .and_then(serde_json::Value::as_str)
            .with_context(|| format!("Hiqlite backend-time evidence file {kind} missing sha256"))?;
        let size_bytes = entry
            .get("size_bytes")
            .and_then(serde_json::Value::as_u64)
            .with_context(|| {
                format!("Hiqlite backend-time evidence file {kind} missing size_bytes")
            })?;
        canonical.push_str(kind);
        canonical.push('\t');
        canonical.push_str(path);
        canonical.push('\t');
        canonical.push_str(sha256);
        canonical.push('\t');
        canonical.push_str(&size_bytes.to_string());
        canonical.push('\n');
    }
    let digest = Sha256::digest(canonical.as_bytes());
    let mut output = String::with_capacity("sha256:".len() + digest.len() * 2);
    output.push_str("sha256:");
    for byte in digest {
        output.push_str(&format!("{byte:02x}"));
    }
    Ok(output)
}

fn validate_product_object_store_durability_policy_attestation(
    path: &Path,
    artifact: &serde_json::Value,
    authority_scope: &S3AuthorityScope,
) -> anyhow::Result<()> {
    if artifact
        .pointer("/object_store/local_development_authority")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        bail!(
            "{} local development object-store authorities cannot satisfy product-complete durability policy attestation",
            path.display()
        );
    }
    let prefix = "/object_store/durability_policy_attestation";
    let evidence_filename = "object-store-durability-attestation.json";
    let label = "object-store durability policy evidence";
    require_json_true(path, artifact, &format!("{prefix}/validated"))?;
    if require_json_str(path, artifact, &format!("{prefix}/evidence"))? != evidence_filename {
        bail!(
            "{} product evidence must attach object-store durability policy evidence",
            path.display()
        );
    }
    let sibling = read_sibling_json_artifact(path, evidence_filename, label)?;
    if require_json_u64(path, artifact, &format!("{prefix}/schema_version"))? != 1 {
        bail!(
            "{} object-store durability policy attestation has unsupported schema_version",
            path.display()
        );
    }
    if require_json_str(path, artifact, &format!("{prefix}/evidence_kind"))?
        != "velorix_object_store_durability_policy_attestation"
    {
        bail!(
            "{} object-store durability policy attestation has unsupported evidence_kind",
            path.display()
        );
    }
    if require_json_str(path, artifact, &format!("{prefix}/authority_store_id"))?
        != authority_scope.raw
    {
        bail!(
            "{} object-store durability policy attestation authority_store_id does not match product authority",
            path.display()
        );
    }
    if require_json_str(path, artifact, &format!("{prefix}/bucket"))? != authority_scope.bucket {
        bail!(
            "{} object-store durability policy attestation bucket does not match product authority",
            path.display()
        );
    }
    if require_json_str(path, artifact, &format!("{prefix}/s3_prefix"))? != authority_scope.prefix {
        bail!(
            "{} object-store durability policy attestation s3_prefix does not match product authority",
            path.display()
        );
    }
    for field in [
        "versioning_or_object_lock_enabled",
        "server_side_encryption_enabled",
        "backup_or_replication_configured",
        "lifecycle_delete_policy_reviewed",
        "destructive_delete_protection_reviewed",
        "cost_controls_reviewed",
    ] {
        require_json_true(path, artifact, &format!("{prefix}/{field}"))?;
    }
    require_nonempty_json_str(path, artifact, &format!("{prefix}/provider_kind"))?;
    let attested_at = require_json_str(path, artifact, &format!("{prefix}/attested_at"))?;
    parse_rfc3339_utc_epoch_seconds(attested_at).with_context(|| {
        format!(
            "{} object-store durability policy attestation has invalid attested_at",
            path.display()
        )
    })?;
    require_nonempty_json_str(path, artifact, &format!("{prefix}/attester"))?;
    validate_product_object_store_durability_policy_sibling(
        path,
        artifact,
        &sibling,
        prefix,
        evidence_filename,
        label,
    )?;

    Ok(())
}

fn validate_product_object_store_durability_policy_sibling(
    path: &Path,
    artifact: &serde_json::Value,
    sibling: &serde_json::Value,
    product_prefix: &str,
    filename: &str,
    label: &str,
) -> anyhow::Result<()> {
    require_sibling_json_u64(path, filename, sibling, "/schema_version", label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        sibling,
        "/evidence_kind",
        "velorix_object_store_durability_policy_attestation",
        label,
    )?;
    for field in [
        "provider_kind",
        "authority_store_id",
        "bucket",
        "s3_prefix",
        "attested_at",
        "attester",
    ] {
        require_sibling_json_str(path, filename, sibling, &format!("/{field}"), label)?;
        require_sibling_json_matches_product(
            path,
            filename,
            sibling,
            &format!("/{field}"),
            artifact,
            &format!("{product_prefix}/{field}"),
            label,
        )?;
    }
    for field in [
        "schema_version",
        "evidence_kind",
        "versioning_or_object_lock_enabled",
        "server_side_encryption_enabled",
        "backup_or_replication_configured",
        "lifecycle_delete_policy_reviewed",
        "destructive_delete_protection_reviewed",
        "cost_controls_reviewed",
    ] {
        require_sibling_json_matches_product(
            path,
            filename,
            sibling,
            &format!("/{field}"),
            artifact,
            &format!("{product_prefix}/{field}"),
            label,
        )?;
    }
    for field in [
        "versioning_or_object_lock_enabled",
        "server_side_encryption_enabled",
        "backup_or_replication_configured",
        "lifecycle_delete_policy_reviewed",
        "destructive_delete_protection_reviewed",
        "cost_controls_reviewed",
    ] {
        require_sibling_json_true(path, filename, sibling, &format!("/{field}"), label)?;
    }
    let attested_at = require_sibling_json_str(path, filename, sibling, "/attested_at", label)?;
    parse_rfc3339_utc_epoch_seconds(attested_at).with_context(|| {
        format!(
            "{} {label} sibling {filename} has invalid attested_at",
            path.display()
        )
    })?;

    Ok(())
}

fn validate_product_openapi_sibling_evidence(
    path: &Path,
    artifact: &serde_json::Value,
) -> anyhow::Result<()> {
    let filename = "openapi.json";
    let label = "product OpenAPI evidence";
    let openapi = read_sibling_json_artifact(path, filename, label)?;
    let version = require_sibling_json_str(path, filename, &openapi, "/openapi", label)?;
    if !version.starts_with("3.") {
        bail!(
            "{} {label} sibling {filename} must be an OpenAPI 3.x document",
            path.display()
        );
    }
    require_sibling_json_str_eq(
        path,
        filename,
        &openapi,
        "/info/title",
        "Velorix View APIs",
        label,
    )?;
    let promoted_path = require_json_str(path, artifact, "/api/openapi/promoted_api_path")?;
    let paths = openapi
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} missing paths object",
                path.display()
            )
        })?;
    if paths.contains_key("/v1/query") {
        bail!(
            "{} {label} sibling {filename} must not expose generic /v1/query",
            path.display()
        );
    }
    if paths.contains_key("/v1/api/scores/positive/{user_id}") {
        bail!(
            "{} {label} sibling {filename} must not expose legacy parameterized scores API",
            path.display()
        );
    }
    let operation = paths
        .get(promoted_path)
        .and_then(|path_item| path_item.get("get"))
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} missing promoted GET operation {promoted_path}",
                path.display()
            )
        })?;
    require_sibling_json_str_eq(
        path,
        filename,
        operation,
        "/x-velorix-view-id",
        "positive_scores_by_user",
        label,
    )?;
    require_sibling_json_str_eq(
        path,
        filename,
        operation,
        "/x-velorix-url-path",
        "/scores/positive",
        label,
    )?;
    require_sibling_json_str_eq(
        path,
        filename,
        operation,
        "/x-velorix-input-relation-id",
        "scores",
        label,
    )?;
    require_sibling_json_str_eq(
        path,
        filename,
        operation,
        "/x-velorix-input-relation-version",
        "2026-05-24.v1",
        label,
    )?;
    let linked_policy = require_json_str(path, artifact, "/api/openapi/linked_view_policy_id")?;
    require_sibling_json_str_eq(
        path,
        filename,
        operation,
        "/x-velorix-query-policy-id",
        linked_policy,
        label,
    )?;
    let spec_hash =
        require_sibling_json_str(path, filename, operation, "/x-velorix-spec-hash", label)?;
    if !spec_hash.starts_with("velorix-view-spec-sha256-v1:") {
        bail!(
            "{} {label} sibling {filename} promoted operation has unexpected spec hash",
            path.display()
        );
    }
    require_openapi_query_parameter(path, filename, operation, "epoch", label)?;
    require_openapi_query_parameter(path, filename, operation, "page_token", label)?;
    require_openapi_query_parameter(path, filename, operation, "max_rows", label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        operation,
        "/responses/200/content/application~1json/schema/type",
        "object",
        label,
    )?;
    require_sibling_json_str_eq(
        path,
        filename,
        operation,
        "/responses/200/content/application~1json/schema/properties/rows/type",
        "array",
        label,
    )?;
    for (field, expected_type) in [
        ("key", "string"),
        ("value", "string"),
        ("weight", "integer"),
    ] {
        require_sibling_json_str_eq(
            path,
            filename,
            operation,
            &format!(
                "/responses/200/content/application~1json/schema/properties/rows/items/properties/{field}/type"
            ),
            expected_type,
            label,
        )?;
    }

    Ok(())
}

fn require_openapi_query_parameter(
    path: &Path,
    filename: &str,
    operation: &serde_json::Value,
    name: &str,
    label: &str,
) -> anyhow::Result<()> {
    let parameters = operation
        .pointer("/parameters")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} missing OpenAPI parameters",
                path.display()
            )
        })?;
    let Some(parameter) = parameters.iter().find(|parameter| {
        parameter.get("name").and_then(serde_json::Value::as_str) == Some(name)
            && parameter.get("in").and_then(serde_json::Value::as_str) == Some("query")
    }) else {
        bail!(
            "{} {label} sibling {filename} missing query parameter {name}",
            path.display()
        );
    };
    if parameter
        .get("required")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        bail!(
            "{} {label} sibling {filename} query parameter {name} must be optional",
            path.display()
        );
    }

    Ok(())
}

fn validate_product_query_policy_sibling_evidence(path: &Path) -> anyhow::Result<()> {
    let label = "product query-policy evidence";
    let created_filename = "query-policy-interactive.json";
    let read_filename = "query-policy-interactive-read.json";
    let created = read_sibling_json_artifact(path, created_filename, label)?;
    let read_back = read_sibling_json_artifact(path, read_filename, label)?;
    validate_query_policy_catalog_sibling(path, created_filename, &created, label)?;
    validate_query_policy_catalog_sibling(path, read_filename, &read_back, label)?;
    if created.pointer("/tenant_id") != read_back.pointer("/tenant_id")
        || created.pointer("/query_policy_id") != read_back.pointer("/query_policy_id")
        || created.pointer("/policy") != read_back.pointer("/policy")
    {
        bail!(
            "{} {label} sibling {read_filename} policy body does not match created interactive policy",
            path.display()
        );
    }

    let weak_filename = "query-policy-weak-rejection.json";
    let weak = read_sibling_json_artifact(path, weak_filename, label)?;
    let weak_error = require_sibling_json_str(path, weak_filename, &weak, "/error", label)?;
    if !weak_error.contains("production table scans require query policy field max_sql_bytes") {
        bail!(
            "{} {label} sibling {weak_filename} does not prove weak policy rejection",
            path.display()
        );
    }

    let missing_filename = "query-policy-missing-view.json";
    let missing = read_sibling_json_artifact(path, missing_filename, label)?;
    let missing_error =
        require_sibling_json_str(path, missing_filename, &missing, "/error", label)?;
    let missing_error_lower = missing_error.to_ascii_lowercase();
    if !missing_error_lower.contains("query policy")
        || (!missing_error_lower.contains("not found") && !missing_error_lower.contains("missing"))
    {
        bail!(
            "{} {label} sibling {missing_filename} does not prove missing policy rejection",
            path.display()
        );
    }

    Ok(())
}

fn validate_query_policy_catalog_sibling(
    path: &Path,
    filename: &str,
    policy: &serde_json::Value,
    label: &str,
) -> anyhow::Result<()> {
    require_sibling_json_str_eq(path, filename, policy, "/tenant_id", "default", label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        policy,
        "/query_policy_id",
        "interactive",
        label,
    )?;
    for field in [
        "max_sql_bytes",
        "planning_timeout_ms",
        "execution_timeout_ms",
        "max_output_rows",
        "max_output_bytes",
        "max_scan_files",
        "max_scan_bytes",
        "max_object_requests",
        "max_concurrent_queries",
        "memory_limit_bytes",
        "spill_limit_bytes",
    ] {
        let value =
            require_sibling_json_u64(path, filename, policy, &format!("/policy/{field}"), label)?;
        if value == 0 {
            bail!(
                "{} {label} sibling {filename} requires /policy/{field} > 0",
                path.display()
            );
        }
    }

    Ok(())
}

fn validate_product_external_s3_validation_siblings(
    path: &Path,
    authority_scope: &S3AuthorityScope,
    validation_prefix: &str,
    validation_key: &str,
) -> anyhow::Result<()> {
    let job_filename = "external-s3-validate-job.json";
    let job_label = "external S3 validation job evidence";
    let job = read_sibling_json_artifact(path, job_filename, job_label)?;
    require_sibling_json_str_eq(path, job_filename, &job, "/kind", "Job", job_label)?;
    require_sibling_json_str_eq(
        path,
        job_filename,
        &job,
        "/metadata/name",
        "velorix-external-s3-validate",
        job_label,
    )?;
    require_sibling_json_str_eq(
        path,
        job_filename,
        &job,
        "/spec/template/spec/restartPolicy",
        "Never",
        job_label,
    )?;
    let succeeded =
        require_sibling_json_u64(path, job_filename, &job, "/status/succeeded", job_label)?;
    if succeeded == 0 {
        bail!(
            "{} {job_label} sibling {job_filename} requires /status/succeeded > 0",
            path.display()
        );
    }
    let complete = job
        .pointer("/status/conditions")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|conditions| {
            conditions.iter().any(|condition| {
                condition.get("type").and_then(serde_json::Value::as_str) == Some("Complete")
                    && condition.get("status").and_then(serde_json::Value::as_str) == Some("True")
            })
        });
    if !complete {
        bail!(
            "{} {job_label} sibling {job_filename} requires Complete=True condition",
            path.display()
        );
    }
    let volumes = job
        .pointer("/spec/template/spec/volumes")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} {job_label} sibling {job_filename} missing array /spec/template/spec/volumes",
                path.display()
            )
        })?;
    let has_work_empty_dir = volumes.iter().any(|volume| {
        volume.get("name").and_then(serde_json::Value::as_str) == Some("work")
            && volume.get("emptyDir").is_some()
    });
    if !has_work_empty_dir {
        bail!(
            "{} {job_label} sibling {job_filename} requires /work emptyDir volume",
            path.display()
        );
    }
    let containers = job
        .pointer("/spec/template/spec/containers")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} {job_label} sibling {job_filename} missing array /spec/template/spec/containers",
                path.display()
            )
        })?;
    let container = containers
        .iter()
        .find(|container| container.get("name").and_then(serde_json::Value::as_str) == Some("aws"))
        .with_context(|| {
            format!(
                "{} {job_label} sibling {job_filename} missing aws validation container",
                path.display()
            )
        })?;
    let command_text = container_command_text(container);
    for required in [
        "head-bucket",
        "put-object",
        "get-object",
        "list-objects-v2",
        "delete-object",
    ] {
        if !command_text.contains(required) {
            bail!(
                "{} {job_label} sibling {job_filename} command missing {required}",
                path.display()
            );
        }
    }
    for required in [
        format!("--bucket \"{}\"", authority_scope.bucket),
        format!("--key \"{}\"", validation_key),
        format!("--prefix \"{}\"", validation_key),
        "--max-keys 1".to_string(),
    ] {
        if !command_text.contains(&required) {
            bail!(
                "{} {job_label} sibling {job_filename} command does not bind {required}",
                path.display()
            );
        }
    }
    let volume_mounts = container
        .get("volumeMounts")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} {job_label} sibling {job_filename} missing aws container volumeMounts",
                path.display()
            )
        })?;
    let has_work_mount = volume_mounts.iter().any(|mount| {
        mount.get("name").and_then(serde_json::Value::as_str) == Some("work")
            && mount.get("mountPath").and_then(serde_json::Value::as_str) == Some("/work")
    });
    if !has_work_mount {
        bail!(
            "{} {job_label} sibling {job_filename} requires aws container /work mount",
            path.display()
        );
    }

    let log = read_sibling_text_artifact(
        path,
        "external-s3-validate.log",
        "external S3 validation log evidence",
    )?;
    let expected_line = format!(
        "velorix external-s3 validation ok bucket={} prefix={} key={}",
        authority_scope.bucket,
        validation_prefix.trim_end_matches('/'),
        validation_key
    );
    if !log.lines().any(|line| line.trim() == expected_line) {
        bail!(
            "{} external S3 validation log evidence sibling external-s3-validate.log missing success line for bucket/prefix/key",
            path.display()
        );
    }

    Ok(())
}

fn container_command_text(container: &serde_json::Value) -> String {
    let mut parts = Vec::new();
    for pointer in ["/command", "/args"] {
        if let Some(items) = container
            .pointer(pointer)
            .and_then(serde_json::Value::as_array)
        {
            parts.extend(
                items
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string),
            );
        }
    }
    parts.join("\n")
}

fn validate_product_no_pvc_namespace_sibling(path: &Path) -> anyhow::Result<()> {
    let filename = "no-pvc-namespace.json";
    let label = "product no-PVC namespace evidence";
    let artifact = read_sibling_json_artifact(path, filename, label)?;
    require_sibling_json_str_eq(path, filename, &artifact, "/kind", "List", label)?;
    let items = artifact
        .pointer("/items")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} missing array /items",
                path.display()
            )
        })?;
    if !items.is_empty() {
        bail!(
            "{} {label} sibling {filename} must contain zero PersistentVolumeClaim items",
            path.display()
        );
    }

    Ok(())
}

fn validate_product_hiqlite_authority_attestation(
    path: &Path,
    artifact: &serde_json::Value,
) -> anyhow::Result<()> {
    let prefix = "/metadata_store/hiqlite_authority_attestation";
    require_json_true(path, artifact, &format!("{prefix}/validated"))?;
    if require_json_str(path, artifact, &format!("{prefix}/evidence"))?
        != "hiqlite-authority-attestation.json"
    {
        bail!(
            "{} product evidence must attach Hiqlite authority evidence",
            path.display()
        );
    }
    require_sibling_evidence_file(
        path,
        "hiqlite-authority-attestation.json",
        "product Hiqlite authority evidence",
    )?;
    let sibling = read_sibling_json_artifact(
        path,
        "hiqlite-authority-attestation.json",
        "product Hiqlite authority evidence",
    )?;
    if require_json_u64(path, artifact, &format!("{prefix}/schema_version"))? != 1 {
        bail!(
            "{} Hiqlite authority attestation has unsupported schema_version",
            path.display()
        );
    }
    let authority_kind = require_json_str(path, artifact, &format!("{prefix}/authority_kind"))?;
    if !matches!(
        authority_kind,
        "external_hiqlite" | "velorix_managed_hiqlite"
    ) {
        bail!(
            "{} Hiqlite authority attestation has unsupported authority_kind",
            path.display()
        );
    }
    let nodes = require_json_string_array(path, artifact, &format!("{prefix}/nodes"))?;
    if nodes.len() != 3 {
        bail!(
            "{} Hiqlite authority attestation requires exactly 3 voter nodes",
            path.display()
        );
    }
    let unique_nodes = nodes.iter().collect::<BTreeSet<_>>();
    if unique_nodes.len() != nodes.len() {
        bail!(
            "{} Hiqlite authority attestation requires unique voter nodes",
            path.display()
        );
    }
    if require_json_u64(path, artifact, &format!("{prefix}/expected_voter_count"))? != 3 {
        bail!(
            "{} Hiqlite authority attestation requires expected_voter_count=3",
            path.display()
        );
    }
    for field in [
        "no_pvc_created_by_vind",
        "metadata_authority_no_pvc_used",
        "voters_learner_only_disabled",
        "api_auth_configured",
        "raft_auth_configured",
        "backup_restore_configured",
    ] {
        require_json_true(path, artifact, &format!("{prefix}/{field}"))?;
    }
    let storage_mode = require_json_str(
        path,
        artifact,
        &format!("{prefix}/metadata_authority_storage_mode"),
    )?;
    if storage_mode != "object-store-backup-restore-with-ephemeral-node-disk" {
        bail!(
            "{} Hiqlite authority attestation requires object-store backup/restore with ephemeral node disk",
            path.display()
        );
    }
    let transport_security =
        require_json_str(path, artifact, &format!("{prefix}/transport_security"))?;
    if transport_security.trim().is_empty()
        || matches!(
            transport_security.trim().to_ascii_lowercase().as_str(),
            "none" | "plaintext" | "local-only" | "generated-local-self-signed"
        )
    {
        bail!(
            "{} Hiqlite authority attestation requires non-local transport security",
            path.display()
        );
    }
    let attested_at = require_json_str(path, artifact, &format!("{prefix}/attested_at"))?;
    parse_rfc3339_utc_epoch_seconds(attested_at).with_context(|| {
        format!(
            "{} Hiqlite authority attestation has invalid attested_at",
            path.display()
        )
    })?;
    require_nonempty_json_str(path, artifact, &format!("{prefix}/attester"))?;
    let has_image_digest = artifact
        .pointer(&format!("{prefix}/image_digest"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| value.trim().starts_with("sha256:"));
    let has_source_revision = artifact
        .pointer(&format!("{prefix}/source_revision"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.trim().is_empty());
    if !has_image_digest && !has_source_revision {
        bail!(
            "{} Hiqlite authority attestation requires image_digest or source_revision",
            path.display()
        );
    }
    if authority_kind == "velorix_managed_hiqlite" {
        if !has_image_digest {
            bail!(
                "{} managed Hiqlite authority attestation requires sha256 image_digest",
                path.display()
            );
        }
        require_json_true(
            path,
            artifact,
            "/no_pvc/managed_hiqlite_authority_validated",
        )?;
        for (pointer, expected) in [
            ("namespace_pvc_list", "no-pvc-namespace.json"),
            ("hiqlite_statefulset", "no-pvc-hiqlite-statefulset.json"),
            ("manifest", "velorix-hiqlite.yaml"),
        ] {
            let evidence_pointer = format!("{prefix}/no_pvc_evidence_files/{pointer}");
            if require_json_str(path, artifact, &evidence_pointer)? != expected {
                bail!(
                    "{} managed Hiqlite authority attestation must attach {expected}",
                    path.display()
                );
            }
            require_sibling_evidence_file(path, expected, "product Hiqlite no-PVC evidence")?;
        }
    }
    validate_product_hiqlite_authority_sibling(path, artifact, &sibling, prefix)?;

    Ok(())
}

fn validate_product_hiqlite_authority_sibling(
    path: &Path,
    artifact: &serde_json::Value,
    sibling: &serde_json::Value,
    product_prefix: &str,
) -> anyhow::Result<()> {
    let filename = "hiqlite-authority-attestation.json";
    let label = "product Hiqlite authority evidence";
    require_sibling_json_u64(path, filename, sibling, "/schema_version", label)?;
    for field in [
        "schema_version",
        "authority_kind",
        "nodes",
        "expected_voter_count",
        "no_pvc_created_by_vind",
        "metadata_authority_no_pvc_used",
        "metadata_authority_storage_mode",
        "voters_learner_only_disabled",
        "api_auth_configured",
        "raft_auth_configured",
        "transport_security",
        "backup_restore_configured",
        "attested_at",
        "attester",
    ] {
        require_sibling_json_matches_product(
            path,
            filename,
            sibling,
            &format!("/{field}"),
            artifact,
            &format!("{product_prefix}/{field}"),
            label,
        )?;
    }
    for field in [
        "authority_kind",
        "metadata_authority_storage_mode",
        "transport_security",
        "attested_at",
        "attester",
    ] {
        require_sibling_json_str(path, filename, sibling, &format!("/{field}"), label)?;
    }
    for field in [
        "no_pvc_created_by_vind",
        "metadata_authority_no_pvc_used",
        "voters_learner_only_disabled",
        "api_auth_configured",
        "raft_auth_configured",
        "backup_restore_configured",
    ] {
        require_sibling_json_true(path, filename, sibling, &format!("/{field}"), label)?;
    }
    let nodes = sibling
        .pointer("/nodes")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} missing array /nodes",
                path.display()
            )
        })?;
    if nodes.len() != 3 {
        bail!(
            "{} {label} sibling {filename} requires exactly 3 voter nodes",
            path.display()
        );
    }
    let mut unique_nodes = BTreeSet::new();
    for node in nodes {
        let Some(node) = node.as_str().filter(|value| !value.trim().is_empty()) else {
            bail!(
                "{} {label} sibling {filename} requires /nodes to contain nonempty strings",
                path.display()
            );
        };
        unique_nodes.insert(node);
    }
    if unique_nodes.len() != nodes.len() {
        bail!(
            "{} {label} sibling {filename} requires unique voter nodes",
            path.display()
        );
    }
    if require_sibling_json_u64(path, filename, sibling, "/expected_voter_count", label)? != 3 {
        bail!(
            "{} {label} sibling {filename} requires /expected_voter_count=3",
            path.display()
        );
    }
    let storage_mode = require_sibling_json_str(
        path,
        filename,
        sibling,
        "/metadata_authority_storage_mode",
        label,
    )?;
    if storage_mode != "object-store-backup-restore-with-ephemeral-node-disk" {
        bail!(
            "{} {label} sibling {filename} requires object-store backup/restore with ephemeral node disk",
            path.display()
        );
    }
    let transport_security =
        require_sibling_json_str(path, filename, sibling, "/transport_security", label)?;
    if transport_security.trim().is_empty()
        || matches!(
            transport_security.trim().to_ascii_lowercase().as_str(),
            "none" | "plaintext" | "local-only" | "generated-local-self-signed"
        )
    {
        bail!(
            "{} {label} sibling {filename} requires non-local transport security",
            path.display()
        );
    }
    let attested_at = require_sibling_json_str(path, filename, sibling, "/attested_at", label)?;
    parse_rfc3339_utc_epoch_seconds(attested_at).with_context(|| {
        format!(
            "{} {label} sibling {filename} has invalid attested_at",
            path.display()
        )
    })?;
    for field in ["image_digest", "source_revision"] {
        if artifact
            .pointer(&format!("{product_prefix}/{field}"))
            .is_some_and(|value| !value.is_null())
        {
            require_sibling_json_matches_product(
                path,
                filename,
                sibling,
                &format!("/{field}"),
                artifact,
                &format!("{product_prefix}/{field}"),
                label,
            )?;
        }
    }
    if require_json_str(path, artifact, &format!("{product_prefix}/authority_kind"))?
        == "velorix_managed_hiqlite"
    {
        require_sibling_json_str(path, filename, sibling, "/image_digest", label)?;
        validate_product_managed_hiqlite_no_pvc_siblings(path)?;
        require_sibling_json_matches_product(
            path,
            filename,
            sibling,
            "/no_pvc_evidence_files",
            artifact,
            &format!("{product_prefix}/no_pvc_evidence_files"),
            label,
        )?;
    }

    Ok(())
}

fn validate_product_managed_hiqlite_no_pvc_siblings(path: &Path) -> anyhow::Result<()> {
    let filename = "no-pvc-hiqlite-statefulset.json";
    let label = "product Hiqlite no-PVC StatefulSet evidence";
    let statefulset = read_sibling_json_artifact(path, filename, label)?;
    require_sibling_json_str_eq(path, filename, &statefulset, "/kind", "StatefulSet", label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        &statefulset,
        "/metadata/name",
        "velorix-hiqlite",
        label,
    )?;
    if require_sibling_json_u64(path, filename, &statefulset, "/spec/replicas", label)? != 3 {
        bail!(
            "{} {label} sibling {filename} requires /spec/replicas=3",
            path.display()
        );
    }
    match statefulset.pointer("/spec/volumeClaimTemplates") {
        Some(serde_json::Value::Array(items)) if items.is_empty() => {}
        None => {}
        Some(_) => bail!(
            "{} {label} sibling {filename} must not define volumeClaimTemplates",
            path.display()
        ),
    }
    require_sibling_json_str_eq(
        path,
        filename,
        &statefulset,
        "/spec/template/spec/serviceAccountName",
        "velorix-hiqlite",
        label,
    )?;
    let volumes = statefulset
        .pointer("/spec/template/spec/volumes")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} missing array /spec/template/spec/volumes",
                path.display()
            )
        })?;
    let mut data_empty_dir = false;
    for volume in volumes {
        let name = volume.get("name").and_then(serde_json::Value::as_str);
        if volume.get("persistentVolumeClaim").is_some() {
            bail!(
                "{} {label} sibling {filename} must not mount persistentVolumeClaim volumes",
                path.display()
            );
        }
        if name == Some("data") && volume.get("emptyDir").is_some() {
            data_empty_dir = true;
        }
    }
    if !data_empty_dir {
        bail!(
            "{} {label} sibling {filename} requires data emptyDir volume",
            path.display()
        );
    }
    let containers = statefulset
        .pointer("/spec/template/spec/containers")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} missing array /spec/template/spec/containers",
                path.display()
            )
        })?;
    if containers.len() != 1 {
        bail!(
            "{} {label} sibling {filename} requires a single hiqlite container",
            path.display()
        );
    }
    let container = &containers[0];
    if container.get("name").and_then(serde_json::Value::as_str) != Some("hiqlite") {
        bail!(
            "{} {label} sibling {filename} requires a single hiqlite container",
            path.display()
        );
    }
    let env = container
        .get("env")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} missing hiqlite container env",
                path.display()
            )
        })?;
    let env_names = env
        .iter()
        .filter_map(|item| item.get("name").and_then(serde_json::Value::as_str))
        .collect::<BTreeSet<_>>();
    for required in [
        "HQL_SECRET_API",
        "HQL_SECRET_RAFT",
        "ENC_KEY_ACTIVE",
        "ENC_KEYS",
    ] {
        if !env_names.contains(required) {
            bail!(
                "{} {label} sibling {filename} missing required env {required}",
                path.display()
            );
        }
    }
    for item in env {
        if item.get("name").and_then(serde_json::Value::as_str) == Some("HQL_LEARNER_ONLY")
            && item
                .get("value")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            bail!(
                "{} {label} sibling {filename} must not set HQL_LEARNER_ONLY=true",
                path.display()
            );
        }
    }

    Ok(())
}

fn validate_product_api_auth_evidence(
    path: &Path,
    artifact: &serde_json::Value,
) -> anyhow::Result<()> {
    if require_json_str(path, artifact, "/api/auth/mode")? != "bearer-token" {
        bail!(
            "{} product evidence requires bearer-token API auth",
            path.display()
        );
    }
    for pointer in [
        "/api/auth/missing_token_rejected",
        "/api/auth/wrong_token_rejected",
        "/api/auth/correct_token_smoke_passed",
        "/api/auth/data_plane_token_rejected_on_admin_route",
        "/api/auth/healthz_unauthenticated",
        "/api/auth/readyz_unauthenticated",
        "/api/auth/deployment_env_verified",
    ] {
        require_json_true(path, artifact, pointer)?;
    }
    if require_json_str(path, artifact, "/api/auth/secret_name")? != "velorix-api-auth" {
        bail!(
            "{} product evidence must use the velorix-api-auth Secret",
            path.display()
        );
    }
    if require_json_str(path, artifact, "/api/auth/admin_secret_name")? != "velorix-admin-auth" {
        bail!(
            "{} product evidence must use the velorix-admin-auth Secret",
            path.display()
        );
    }
    require_json_true(path, artifact, "/api/auth/local_tls_auth_smoke/enabled")?;
    require_json_true(path, artifact, "/api/auth/local_tls_auth_smoke/passed")?;
    if require_json_str(path, artifact, "/api/auth/local_tls_auth_smoke/evidence")?
        != "tls-auth-smoke.json"
    {
        bail!(
            "{} product evidence must attach local TLS/auth smoke evidence",
            path.display()
        );
    }
    require_sibling_evidence_file(
        path,
        "tls-auth-smoke.json",
        "product local TLS/auth evidence",
    )?;
    require_json_false(
        path,
        artifact,
        "/api/auth/local_tls_auth_smoke/public_ingress_attestation",
    )?;
    require_json_false(
        path,
        artifact,
        "/api/auth/local_tls_auth_smoke/trusted_for_product_complete",
    )?;

    Ok(())
}

fn validate_product_deployed_image_evidence(
    path: &Path,
    artifact: &serde_json::Value,
) -> anyhow::Result<()> {
    for (role, deployment_name, container_name) in [
        ("velorix-api", "velorix-api", "api"),
        ("velorix-meta", "velorix-meta", "meta"),
    ] {
        validate_product_deployed_role_image_evidence(
            path,
            artifact,
            role,
            deployment_name,
            container_name,
        )?;
    }
    Ok(())
}

fn product_deployed_image_digest<'a>(
    artifact: &'a serde_json::Value,
    role: &str,
) -> Option<&'a str> {
    artifact
        .pointer(&format!("/deployed_images/{role}/image_digest"))
        .and_then(serde_json::Value::as_str)
}

fn validate_product_deployed_role_image_evidence(
    path: &Path,
    artifact: &serde_json::Value,
    role: &str,
    deployment_name: &str,
    container_name: &str,
) -> anyhow::Result<()> {
    let prefix = format!("/deployed_images/{role}");
    require_nonempty_json_str(path, artifact, &format!("{prefix}/image"))?;
    let image_digest = require_json_str(path, artifact, &format!("{prefix}/image_digest"))?;
    validate_sha256_digest(
        path,
        image_digest,
        &format!("deployed_images.{role}.image_digest"),
    )?;

    let expected_manifest = format!("{deployment_name}.yaml");
    let expected_deployment = format!("{deployment_name}-deployment-observed.json");
    let expected_pods = format!("{deployment_name}-pods.json");
    for (key, expected) in [
        ("manifest", expected_manifest.as_str()),
        ("deployment", expected_deployment.as_str()),
        ("pods", expected_pods.as_str()),
    ] {
        if require_json_str(path, artifact, &format!("{prefix}/evidence_files/{key}"))? != expected
        {
            bail!(
                "{} product deployed image evidence {role} must attach {expected}",
                path.display()
            );
        }
        require_sibling_evidence_file(path, expected, "product deployed image evidence")?;
    }

    let deployment = read_sibling_json_artifact(
        path,
        &expected_deployment,
        "product deployed image deployment evidence",
    )?;
    if require_json_str(path, &deployment, "/kind")? != "Deployment" {
        bail!(
            "{} product deployed image evidence {role} deployment kind must be Deployment",
            path.display()
        );
    }
    if require_json_str(path, &deployment, "/metadata/name")? != deployment_name {
        bail!(
            "{} product deployed image evidence {role} deployment name mismatch",
            path.display()
        );
    }
    if require_json_str(
        path,
        &deployment,
        "/spec/template/metadata/annotations/velorix.dev~1image-digest",
    )? != image_digest
    {
        bail!(
            "{} product deployed image evidence {role} deployment image digest annotation mismatch",
            path.display()
        );
    }
    let deployment_container = find_named_container(
        &deployment,
        "/spec/template/spec/containers",
        container_name,
    )
    .with_context(|| {
        format!(
            "{} product deployed image evidence {role} missing deployment container {container_name}",
            path.display()
        )
    })?;
    if require_json_str(path, deployment_container, "/image")?
        != require_json_str(path, artifact, &format!("{prefix}/image"))?
    {
        bail!(
            "{} product deployed image evidence {role} deployment image mismatch",
            path.display()
        );
    }

    let pods =
        read_sibling_json_artifact(path, &expected_pods, "product deployed image pod evidence")?;
    let items = pods
        .pointer("/items")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} product deployed image evidence {role} pods evidence missing array /items",
                path.display()
            )
        })?;
    if items.is_empty() {
        bail!(
            "{} product deployed image evidence {role} pods evidence is empty",
            path.display()
        );
    }
    let mut matched = false;
    for pod in items {
        let Some(status) = find_named_container_status(pod, container_name) else {
            continue;
        };
        let image_id = require_json_str(path, status, "/imageID")?;
        let Some(observed_digest) = sha256_digest_in_text(image_id) else {
            bail!(
                "{} product deployed image evidence {role} pod imageID does not contain a sha256 digest",
                path.display()
            );
        };
        if observed_digest != image_digest {
            bail!(
                "{} product deployed image evidence {role} pod imageID digest does not match image_digest",
                path.display()
            );
        }
        matched = true;
    }
    if !matched {
        bail!(
            "{} product deployed image evidence {role} pods evidence missing container status {container_name}",
            path.display()
        );
    }

    Ok(())
}

fn find_named_container<'a>(
    artifact: &'a serde_json::Value,
    pointer: &str,
    name: &str,
) -> Option<&'a serde_json::Value> {
    artifact
        .pointer(pointer)
        .and_then(serde_json::Value::as_array)?
        .iter()
        .find(|container| container.get("name").and_then(serde_json::Value::as_str) == Some(name))
}

fn find_named_container_status<'a>(
    pod: &'a serde_json::Value,
    name: &str,
) -> Option<&'a serde_json::Value> {
    pod.pointer("/status/containerStatuses")
        .and_then(serde_json::Value::as_array)?
        .iter()
        .find(|container| container.get("name").and_then(serde_json::Value::as_str) == Some(name))
}

fn sha256_digest_in_text(value: &str) -> Option<String> {
    let start = value.find("sha256:")?;
    let candidate = value.get(start..start + "sha256:".len() + 64)?;
    let hex = &candidate["sha256:".len()..];
    if hex.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Some(candidate.to_ascii_lowercase())
    } else {
        None
    }
}

fn validate_product_ingress_tls_auth_attestation(
    path: &Path,
    artifact: &serde_json::Value,
) -> anyhow::Result<()> {
    let attestation = "/api/auth/ingress_tls_auth_attestation";
    let evidence_filename = "ingress-tls-auth-attestation.json";
    let label = "product ingress/TLS/auth evidence";
    if require_json_str(path, artifact, &format!("{attestation}/evidence"))? != evidence_filename {
        bail!(
            "{} product evidence must attach ingress/TLS/auth attestation evidence",
            path.display()
        );
    }
    let sibling = read_sibling_json_artifact(path, evidence_filename, label)?;
    if require_json_u64(path, artifact, &format!("{attestation}/schema_version"))? != 1 {
        bail!(
            "{} ingress/TLS/auth attestation has unsupported schema_version",
            path.display()
        );
    }
    if require_json_str(path, artifact, &format!("{attestation}/evidence_kind"))?
        != "velorix_ingress_tls_auth_attestation"
    {
        bail!(
            "{} product evidence requires velorix_ingress_tls_auth_attestation",
            path.display()
        );
    }
    require_json_true(path, artifact, &format!("{attestation}/validated"))?;
    let endpoint_url = require_json_str(path, artifact, &format!("{attestation}/endpoint_url"))?;
    validate_external_https_endpoint(path, endpoint_url, "endpoint_url")?;
    let external_hostname =
        require_json_str(path, artifact, &format!("{attestation}/external_hostname"))?;
    validate_external_hostname(path, external_hostname, "external_hostname")?;
    require_nonempty_json_str(path, artifact, &format!("{attestation}/ingress_controller"))?;
    let transport_security =
        require_json_str(path, artifact, &format!("{attestation}/transport_security"))?;
    reject_local_ingress_marker(path, transport_security, "transport_security")?;
    for pointer in [
        "/tls_enabled",
        "/auth_enforced",
        "/missing_token_rejected",
        "/wrong_token_rejected",
        "/admin_auth_separate",
        "/admin_route_missing_token_rejected",
        "/admin_route_wrong_token_rejected",
        "/data_plane_token_rejected_on_admin_catalog_route",
        "/admin_token_accepted_on_admin_route",
        "/data_plane_token_rejected_on_admin_route",
    ] {
        require_json_true(path, artifact, &format!("{attestation}{pointer}"))?;
    }
    if let Some(issuer) = artifact
        .pointer(&format!("{attestation}/tls_certificate_issuer"))
        .and_then(serde_json::Value::as_str)
    {
        reject_local_ingress_marker(path, issuer, "tls_certificate_issuer")?;
    }
    if artifact
        .pointer(&format!("{attestation}/tls_certificate_sha256"))
        .and_then(serde_json::Value::as_str)
        .is_none_or(|value| value.trim().is_empty())
        && artifact
            .pointer(&format!("{attestation}/tls_certificate_issuer"))
            .and_then(serde_json::Value::as_str)
            .is_none_or(|value| value.trim().is_empty())
    {
        bail!(
            "{} ingress/TLS/auth attestation requires tls_certificate_sha256 or tls_certificate_issuer",
            path.display()
        );
    }
    let attested_at = require_json_str(path, artifact, &format!("{attestation}/attested_at"))?;
    if attested_at.trim().is_empty() {
        bail!(
            "{} product evidence missing nonempty string {attestation}/attested_at",
            path.display()
        );
    }
    validate_recent_ingress_tls_auth_attested_at(path, attested_at)?;
    require_nonempty_json_str(path, artifact, &format!("{attestation}/attester"))?;
    validate_product_ingress_tls_auth_sibling(
        path,
        artifact,
        &sibling,
        attestation,
        evidence_filename,
        label,
    )?;

    Ok(())
}

fn validate_product_ingress_tls_auth_sibling(
    path: &Path,
    artifact: &serde_json::Value,
    sibling: &serde_json::Value,
    product_prefix: &str,
    filename: &str,
    label: &str,
) -> anyhow::Result<()> {
    require_sibling_json_u64(path, filename, sibling, "/schema_version", label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        sibling,
        "/evidence_kind",
        "velorix_ingress_tls_auth_attestation",
        label,
    )?;
    for field in [
        "endpoint_url",
        "external_hostname",
        "ingress_controller",
        "transport_security",
        "attested_at",
        "attester",
    ] {
        require_sibling_json_str(path, filename, sibling, &format!("/{field}"), label)?;
        require_sibling_json_matches_product(
            path,
            filename,
            sibling,
            &format!("/{field}"),
            artifact,
            &format!("{product_prefix}/{field}"),
            label,
        )?;
    }
    for field in [
        "schema_version",
        "evidence_kind",
        "tls_enabled",
        "auth_enforced",
        "missing_token_rejected",
        "wrong_token_rejected",
        "admin_auth_separate",
        "admin_route_missing_token_rejected",
        "admin_route_wrong_token_rejected",
        "data_plane_token_rejected_on_admin_catalog_route",
        "admin_token_accepted_on_admin_route",
        "data_plane_token_rejected_on_admin_route",
    ] {
        require_sibling_json_matches_product(
            path,
            filename,
            sibling,
            &format!("/{field}"),
            artifact,
            &format!("{product_prefix}/{field}"),
            label,
        )?;
    }
    for field in [
        "tls_enabled",
        "auth_enforced",
        "missing_token_rejected",
        "wrong_token_rejected",
        "admin_auth_separate",
        "admin_route_missing_token_rejected",
        "admin_route_wrong_token_rejected",
        "data_plane_token_rejected_on_admin_catalog_route",
        "admin_token_accepted_on_admin_route",
        "data_plane_token_rejected_on_admin_route",
    ] {
        require_sibling_json_true(path, filename, sibling, &format!("/{field}"), label)?;
    }
    for field in ["tls_certificate_sha256", "tls_certificate_issuer"] {
        if artifact
            .pointer(&format!("{product_prefix}/{field}"))
            .is_some_and(|value| !value.is_null())
        {
            require_sibling_json_matches_product(
                path,
                filename,
                sibling,
                &format!("/{field}"),
                artifact,
                &format!("{product_prefix}/{field}"),
                label,
            )?;
        }
    }
    let attested_at = require_sibling_json_str(path, filename, sibling, "/attested_at", label)?;
    validate_recent_ingress_tls_auth_attested_at(path, attested_at)?;

    Ok(())
}

fn validate_external_https_endpoint(
    path: &Path,
    endpoint_url: &str,
    field: &str,
) -> anyhow::Result<()> {
    let Some(rest) = endpoint_url.strip_prefix("https://") else {
        bail!(
            "{} ingress/TLS/auth attestation {field} must be an https URL",
            path.display()
        );
    };
    let host = rest
        .split(['/', ':', '?', '#'])
        .next()
        .unwrap_or_default()
        .trim_matches(['[', ']']);
    validate_external_hostname(path, host, field)
}

fn validate_external_hostname(path: &Path, hostname: &str, field: &str) -> anyhow::Result<()> {
    let normalized = hostname.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || normalized == "localhost"
        || normalized.ends_with(".svc")
        || normalized.ends_with(".svc.cluster.local")
        || normalized
            .parse::<std::net::IpAddr>()
            .is_ok_and(|addr| addr.is_loopback())
    {
        bail!(
            "{} ingress/TLS/auth attestation {field} must be an external hostname",
            path.display()
        );
    }

    Ok(())
}

fn reject_local_ingress_marker(path: &Path, value: &str, field: &str) -> anyhow::Result<()> {
    let normalized = value.to_ascii_lowercase();
    if [
        "self-signed",
        "generated-local",
        "local-only",
        "velorix-api.local",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
    {
        bail!(
            "{} ingress/TLS/auth attestation {field} must not describe local-only TLS",
            path.display()
        );
    }

    Ok(())
}

struct S3AuthorityScope {
    raw: String,
    bucket: String,
    prefix: String,
}

fn s3_authority_scope(authority_store_id: &str) -> anyhow::Result<S3AuthorityScope> {
    let trimmed = authority_store_id.trim();
    let Some(remainder) = trimmed.strip_prefix("s3://") else {
        bail!("authority_store_id must start with s3://");
    };
    let mut parts = remainder.split('/').filter(|part| !part.is_empty());
    let Some(first) = parts.next() else {
        bail!("authority_store_id must include a bucket");
    };
    let remaining = parts.collect::<Vec<_>>();
    if matches!(first, "external" | "rustfs") {
        let Some(bucket) = remaining.first() else {
            bail!("authority_store_id must include a bucket after {first}");
        };
        return Ok(S3AuthorityScope {
            raw: trimmed.to_string(),
            bucket: (*bucket).to_string(),
            prefix: remaining[1..].join("/"),
        });
    }

    Ok(S3AuthorityScope {
        raw: trimmed.to_string(),
        bucket: first.to_string(),
        prefix: remaining.join("/"),
    })
}

fn validate_product_ingest_writer_lifecycle_attestation(
    path: &Path,
    artifact: &serde_json::Value,
    deployment_id: &str,
    authority_store_id: &str,
) -> anyhow::Result<()> {
    require_json_true(
        path,
        artifact,
        "/ingest_writer/lifecycle_attestation/validated",
    )?;
    if require_json_str(
        path,
        artifact,
        "/ingest_writer/lifecycle_attestation/source",
    )? != "generated"
    {
        bail!(
            "{} product evidence must use script-generated ingest-writer lifecycle attestation",
            path.display()
        );
    }
    require_json_true(
        path,
        artifact,
        "/ingest_writer/lifecycle_attestation/trusted_for_product_complete",
    )?;
    if require_json_str(
        path,
        artifact,
        "/ingest_writer/lifecycle_attestation/deployment_id",
    )? != deployment_id
    {
        bail!(
            "{} product evidence lifecycle deployment_id does not match readiness report",
            path.display()
        );
    }
    if require_json_str(
        path,
        artifact,
        "/ingest_writer/lifecycle_attestation/authority_store_id",
    )? != authority_store_id
    {
        bail!(
            "{} product evidence lifecycle authority_store_id does not match readiness report",
            path.display()
        );
    }
    for field in [
        "pod_internal_append_completed",
        "multi_pod_overlap_conflict_rejected",
        "adjacent_append_succeeded",
        "crash_restart_reconstruction_checked",
        "kubernetes_lease_handoff_checked",
        "lease_held_through_append_checked",
        "commit_guard_checked",
        "admission_commit_guard_bound_checked",
        "lease_loss_during_reservation_checked",
        "no_pvc_created_by_vind",
    ] {
        require_json_true(
            path,
            artifact,
            &format!("/ingest_writer/lifecycle_attestation/{field}"),
        )?;
    }
    for key in [
        "pod_internal_job",
        "overlap_job",
        "adjacent_job",
        "restart_job",
        "lease_loss_job",
        "handoff_owner_a_job",
        "handoff_owner_b_job",
        "handoff_stale_owner_job",
    ] {
        for field in [
            "job_uid",
            "pod_uid",
            "pod_name",
            "container_image",
            "container_image_id",
        ] {
            let pointer =
                format!("/ingest_writer/lifecycle_attestation/evidence_provenance/{key}/{field}");
            if require_json_str(path, artifact, &pointer)?
                .trim()
                .is_empty()
            {
                bail!(
                    "{} product evidence lifecycle provenance requires {key}.{field}",
                    path.display()
                );
            }
        }
    }
    for (key, expected) in REQUIRED_INGEST_WRITER_LIFECYCLE_EVIDENCE_FILES {
        let pointer = format!("/ingest_writer/lifecycle_attestation/evidence_files/{key}");
        if require_json_str(path, artifact, &pointer)? != *expected {
            bail!(
                "{} product evidence lifecycle evidence file {key} did not match {expected}",
                path.display()
            );
        }
        require_sibling_evidence_file(path, expected, "product ingest-writer lifecycle evidence")?;
    }
    validate_ingest_writer_lifecycle_sibling_evidence_contents(
        path,
        "product ingest-writer lifecycle evidence",
    )?;

    Ok(())
}

fn validate_ingest_writer_lifecycle_evidence_provenance(
    path: &Path,
    artifact: &IngestWriterLifecycleEvidenceArtifactV1,
) -> anyhow::Result<()> {
    let required = [
        "pod_internal_job",
        "overlap_job",
        "adjacent_job",
        "restart_job",
        "lease_loss_job",
        "handoff_owner_a_job",
        "handoff_owner_b_job",
        "handoff_stale_owner_job",
    ];
    for key in required {
        let Some(provenance) = artifact.evidence_provenance.get(key) else {
            bail!(
                "{} ingest-writer lifecycle evidence is missing evidence_provenance.{key}",
                path.display()
            );
        };
        if provenance.job_uid.trim().is_empty() {
            bail!(
                "{} ingest-writer lifecycle evidence provenance {key} is missing job_uid",
                path.display()
            );
        }
        if provenance.pod_uid.trim().is_empty() {
            bail!(
                "{} ingest-writer lifecycle evidence provenance {key} is missing pod_uid",
                path.display()
            );
        }
        if provenance.pod_name.trim().is_empty() {
            bail!(
                "{} ingest-writer lifecycle evidence provenance {key} is missing pod_name",
                path.display()
            );
        }
        if provenance.container_image.trim().is_empty() {
            bail!(
                "{} ingest-writer lifecycle evidence provenance {key} is missing container_image",
                path.display()
            );
        }
        if provenance.container_image_id.trim().is_empty() {
            bail!(
                "{} ingest-writer lifecycle evidence provenance {key} is missing container_image_id",
                path.display()
            );
        }
    }
    Ok(())
}

fn validate_ingest_writer_lifecycle_evidence_files(
    path: &Path,
    evidence_files: &BTreeMap<String, String>,
    label: &str,
) -> anyhow::Result<()> {
    for (key, expected) in REQUIRED_INGEST_WRITER_LIFECYCLE_EVIDENCE_FILES {
        match evidence_files.get(*key).map(String::as_str) {
            Some(actual) if actual == *expected => {}
            Some(_) => bail!(
                "{} {label} evidence_files.{key} must be {expected}",
                path.display()
            ),
            None => bail!("{} {label} is missing evidence_files.{key}", path.display()),
        }
        require_sibling_evidence_file(path, expected, label)?;
    }
    validate_ingest_writer_lifecycle_sibling_evidence_contents(path, label)?;

    Ok(())
}

fn require_sibling_evidence_file(path: &Path, filename: &str, label: &str) -> anyhow::Result<()> {
    sibling_evidence_path(path, filename, label).map(|_| ())
}

fn sibling_evidence_path(path: &Path, filename: &str, label: &str) -> anyhow::Result<PathBuf> {
    if filename.trim().is_empty() || filename.contains('/') || filename.contains('\\') {
        bail!(
            "{} {label} has invalid evidence filename {filename:?}",
            path.display()
        );
    }
    let parent = path.parent().with_context(|| {
        format!(
            "{} {label} cannot resolve sibling evidence directory",
            path.display()
        )
    })?;
    let sibling = parent.join(filename);
    if !sibling.is_file() {
        bail!(
            "{} {label} requires sibling evidence file {}",
            path.display(),
            sibling.display()
        );
    }

    Ok(sibling)
}

fn validate_ingest_writer_lifecycle_sibling_evidence_contents(
    path: &Path,
    label: &str,
) -> anyhow::Result<()> {
    validate_guarded_append_lifecycle_sibling(
        path,
        "velorix-ingest-writer-smoke-log.json",
        "appended",
        label,
    )?;
    validate_overlap_conflict_lifecycle_sibling(
        path,
        "velorix-ingest-lifecycle-overlap-log.json",
        label,
    )?;
    validate_guarded_append_lifecycle_sibling(
        path,
        "velorix-ingest-lifecycle-adjacent-log.json",
        "appended",
        label,
    )?;
    validate_restart_lifecycle_sibling(path, "velorix-ingest-lifecycle-restart-log.json", label)?;
    validate_lease_loss_lifecycle_sibling(
        path,
        "velorix-ingest-lifecycle-lease-loss-log.json",
        label,
    )?;
    validate_handoff_lifecycle_sibling(path, "velorix-ingest-lifecycle-handoff-log.json", label)?;

    Ok(())
}

fn validate_guarded_append_lifecycle_sibling(
    path: &Path,
    filename: &str,
    expected_outcome: &str,
    label: &str,
) -> anyhow::Result<()> {
    let artifact = read_sibling_json_artifact(path, filename, label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        &artifact,
        "/evidence_kind",
        "ingest_writer_lease_guarded_append_probe",
        label,
    )?;
    require_sibling_json_str_eq(path, filename, &artifact, "/status", "pass", label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        &artifact,
        "/outcome",
        expected_outcome,
        label,
    )?;
    for pointer in [
        "/lease_held_through_append",
        "/commit_guard_enforced",
        "/admission_commit_guard_bound",
    ] {
        require_sibling_json_true(path, filename, &artifact, pointer, label)?;
    }
    require_sibling_json_str_eq(
        path,
        filename,
        &artifact,
        "/admission_commit_guard_binding/binding_kind",
        "kubernetes_partition_lease",
        label,
    )?;
    if require_sibling_json_str(
        path,
        filename,
        &artifact,
        "/admission_commit_guard_binding/subject",
        label,
    )?
    .trim()
    .is_empty()
    {
        bail!(
            "{} {label} sibling {filename} requires nonempty /admission_commit_guard_binding/subject",
            path.display()
        );
    }

    Ok(())
}

fn validate_overlap_conflict_lifecycle_sibling(
    path: &Path,
    filename: &str,
    label: &str,
) -> anyhow::Result<()> {
    let artifact = read_sibling_json_artifact(path, filename, label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        &artifact,
        "/evidence_kind",
        "ingest_writer_lifecycle_overlap_conflict_probe",
        label,
    )?;
    require_sibling_json_str_eq(path, filename, &artifact, "/status", "pass", label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        &artifact,
        "/outcome",
        "conflict-rejected",
        label,
    )?;
    for pointer in [
        "/multi_pod_overlap_conflict_rejected",
        "/conflicting_append_rejected_before_append",
        "/conflict_log_observed",
    ] {
        require_sibling_json_true(path, filename, &artifact, pointer, label)?;
    }
    match artifact
        .pointer("/append_completed")
        .and_then(serde_json::Value::as_bool)
    {
        Some(false) => {}
        Some(true) => bail!(
            "{} {label} sibling {filename} requires /append_completed=false",
            path.display()
        ),
        None => bail!(
            "{} {label} sibling {filename} missing boolean /append_completed",
            path.display()
        ),
    }
    require_sibling_json_str_eq(path, filename, &artifact, "/stream_id", "scores", label)?;
    let partition_id = require_sibling_json_u64(path, filename, &artifact, "/partition_id", label)?;
    if partition_id != 0 {
        bail!(
            "{} {label} sibling {filename} requires /partition_id=0, got {partition_id}",
            path.display()
        );
    }
    require_sibling_json_u64(path, filename, &artifact, "/start_offset_inclusive", label)?;
    let attempted_row_count =
        require_sibling_json_u64(path, filename, &artifact, "/attempted_row_count", label)?;
    if attempted_row_count == 0 {
        bail!(
            "{} {label} sibling {filename} requires /attempted_row_count > 0",
            path.display()
        );
    }
    let raw_log = require_sibling_json_str(path, filename, &artifact, "/raw_conflict_log", label)?;
    if !raw_log.contains("conflicted before append")
        && !raw_log.contains("fresh append outcome, got conflict")
    {
        bail!(
            "{} {label} sibling {filename} requires raw overlap conflict evidence",
            path.display()
        );
    }

    Ok(())
}

fn validate_restart_lifecycle_sibling(
    path: &Path,
    filename: &str,
    label: &str,
) -> anyhow::Result<()> {
    let artifact = read_sibling_json_artifact(path, filename, label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        &artifact,
        "/evidence_kind",
        "ingest_writer_admission_crash_restart_probe",
        label,
    )?;
    require_sibling_json_str_eq(path, filename, &artifact, "/status", "pass", label)?;
    for pointer in [
        "/orphan_admission_created",
        "/restart_reconstructed_active_admission",
        "/recovered_append_completed",
        "/committed_admission_not_expirable",
    ] {
        require_sibling_json_true(path, filename, &artifact, pointer, label)?;
    }

    Ok(())
}

fn validate_lease_loss_lifecycle_sibling(
    path: &Path,
    filename: &str,
    label: &str,
) -> anyhow::Result<()> {
    let artifact = read_sibling_json_artifact(path, filename, label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        &artifact,
        "/evidence_kind",
        "ingest_writer_lease_loss_during_reservation_probe",
        label,
    )?;
    require_sibling_json_str_eq(path, filename, &artifact, "/status", "pass", label)?;
    for pointer in [
        "/before_admission_lease_verified",
        "/lease_released_before_commit",
        "/commit_guard_rejected_before_batch_commit",
        "/batch_object_absent_after_rejection",
        "/admission_commit_guard_bound",
        "/restart_reconstructed_active_admission",
        "/target_admission_rejected_overlapping_reservation_before_expiry",
        "/orphan_expired",
        "/expired_target_rejected_original_retry",
    ] {
        require_sibling_json_true(path, filename, &artifact, pointer, label)?;
    }

    Ok(())
}

fn validate_handoff_lifecycle_sibling(
    path: &Path,
    filename: &str,
    label: &str,
) -> anyhow::Result<()> {
    let artifact = read_sibling_json_artifact(path, filename, label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        &artifact,
        "/evidence_kind",
        "ingest_writer_two_pod_lease_handoff_probe",
        label,
    )?;
    require_sibling_json_str_eq(path, filename, &artifact, "/status", "pass", label)?;
    for pointer in [
        "/kubernetes_lease_handoff_checked",
        "/owner_b_append_completed",
        "/owner_b_lease_held_through_append",
        "/stale_owner_rejected",
        "/commit_guard_checked",
        "/admission_commit_guard_bound_checked",
    ] {
        require_sibling_json_true(path, filename, &artifact, pointer, label)?;
    }
    let owner_a_epoch =
        require_sibling_json_u64(path, filename, &artifact, "/owner_a_epoch", label)?;
    let owner_b_epoch =
        require_sibling_json_u64(path, filename, &artifact, "/owner_b_epoch", label)?;
    if owner_b_epoch <= owner_a_epoch {
        bail!(
            "{} {label} sibling {filename} requires owner_b_epoch greater than owner_a_epoch",
            path.display()
        );
    }

    Ok(())
}

fn read_sibling_json_artifact(
    path: &Path,
    filename: &str,
    label: &str,
) -> anyhow::Result<serde_json::Value> {
    let sibling = sibling_evidence_path(path, filename, label)?;
    let contents = fs::read_to_string(&sibling).with_context(|| {
        format!(
            "{} {label} failed to read sibling evidence {}",
            path.display(),
            sibling.display()
        )
    })?;
    serde_json::from_str(&contents).with_context(|| {
        format!(
            "{} {label} failed to parse sibling evidence {} as JSON",
            path.display(),
            sibling.display()
        )
    })
}

fn read_sibling_text_artifact(path: &Path, filename: &str, label: &str) -> anyhow::Result<String> {
    let sibling = sibling_evidence_path(path, filename, label)?;
    fs::read_to_string(&sibling).with_context(|| {
        format!(
            "{} {label} failed to read sibling evidence {}",
            path.display(),
            sibling.display()
        )
    })
}

fn require_sibling_json_str<'a>(
    path: &Path,
    filename: &str,
    value: &'a serde_json::Value,
    pointer: &str,
    label: &str,
) -> anyhow::Result<&'a str> {
    value
        .pointer(pointer)
        .and_then(serde_json::Value::as_str)
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} missing string {pointer}",
                path.display()
            )
        })
}

fn require_sibling_json_str_eq(
    path: &Path,
    filename: &str,
    value: &serde_json::Value,
    pointer: &str,
    expected: &str,
    label: &str,
) -> anyhow::Result<()> {
    let actual = require_sibling_json_str(path, filename, value, pointer, label)?;
    if actual != expected {
        bail!(
            "{} {label} sibling {filename} requires {pointer}={expected}, got {actual}",
            path.display()
        );
    }

    Ok(())
}

fn require_sibling_json_matches_product(
    path: &Path,
    filename: &str,
    sibling: &serde_json::Value,
    sibling_pointer: &str,
    product: &serde_json::Value,
    product_pointer: &str,
    label: &str,
) -> anyhow::Result<()> {
    let sibling_value = sibling.pointer(sibling_pointer).with_context(|| {
        format!(
            "{} {label} sibling {filename} missing {sibling_pointer}",
            path.display()
        )
    })?;
    let product_value = product.pointer(product_pointer).with_context(|| {
        format!(
            "{} product evidence missing {product_pointer} for {label}",
            path.display()
        )
    })?;
    if sibling_value != product_value {
        bail!(
            "{} {label} sibling {filename} {sibling_pointer} does not match product evidence {product_pointer}",
            path.display()
        );
    }

    Ok(())
}

fn require_sibling_json_true(
    path: &Path,
    filename: &str,
    value: &serde_json::Value,
    pointer: &str,
    label: &str,
) -> anyhow::Result<()> {
    match value.pointer(pointer).and_then(serde_json::Value::as_bool) {
        Some(true) => Ok(()),
        Some(false) => bail!(
            "{} {label} sibling {filename} requires {pointer}=true",
            path.display()
        ),
        None => bail!(
            "{} {label} sibling {filename} missing boolean {pointer}",
            path.display()
        ),
    }
}

fn require_sibling_json_u64(
    path: &Path,
    filename: &str,
    value: &serde_json::Value,
    pointer: &str,
    label: &str,
) -> anyhow::Result<u64> {
    value
        .pointer(pointer)
        .and_then(serde_json::Value::as_u64)
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} missing integer {pointer}",
                path.display()
            )
        })
}

fn validate_recent_ingest_writer_lifecycle_attested_at(
    path: &Path,
    attested_at: &str,
) -> anyhow::Result<()> {
    let attested_at_epoch = parse_rfc3339_utc_epoch_seconds(attested_at).with_context(|| {
        format!(
            "{} ingest-writer lifecycle evidence has invalid attested_at",
            path.display()
        )
    })?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before Unix epoch")?
        .as_secs();
    if attested_at_epoch > now + INGEST_WRITER_LIFECYCLE_FUTURE_SKEW_SECS {
        bail!(
            "{} ingest-writer lifecycle evidence attested_at is too far in the future",
            path.display()
        );
    }
    if now.saturating_sub(attested_at_epoch) > INGEST_WRITER_LIFECYCLE_MAX_AGE_SECS {
        bail!(
            "{} ingest-writer lifecycle evidence attested_at is older than {} seconds",
            path.display(),
            INGEST_WRITER_LIFECYCLE_MAX_AGE_SECS
        );
    }

    Ok(())
}

fn validate_recent_ingress_tls_auth_attested_at(
    path: &Path,
    attested_at: &str,
) -> anyhow::Result<()> {
    let attested_at_epoch = parse_rfc3339_utc_epoch_seconds(attested_at).with_context(|| {
        format!(
            "{} ingress/TLS/auth attestation has invalid attested_at",
            path.display()
        )
    })?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before Unix epoch")?
        .as_secs();
    if attested_at_epoch > now + INGRESS_TLS_AUTH_ATTESTATION_FUTURE_SKEW_SECS {
        bail!(
            "{} ingress/TLS/auth attestation attested_at is too far in the future",
            path.display()
        );
    }
    if now.saturating_sub(attested_at_epoch) > INGRESS_TLS_AUTH_ATTESTATION_MAX_AGE_SECS {
        bail!(
            "{} ingress/TLS/auth attestation attested_at is older than {} seconds",
            path.display(),
            INGRESS_TLS_AUTH_ATTESTATION_MAX_AGE_SECS
        );
    }

    Ok(())
}

fn validate_recent_hiqlite_backend_time_attested_at(
    path: &Path,
    attested_at: &str,
) -> anyhow::Result<()> {
    let attested_at_epoch = parse_rfc3339_utc_epoch_seconds(attested_at).with_context(|| {
        format!(
            "{} Hiqlite backend-time attestation has invalid attested_at",
            path.display()
        )
    })?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before Unix epoch")?
        .as_secs();
    if attested_at_epoch > now + HIQLITE_BACKEND_TIME_ATTESTATION_FUTURE_SKEW_SECS {
        bail!(
            "{} Hiqlite backend-time attestation attested_at is too far in the future",
            path.display()
        );
    }
    if now.saturating_sub(attested_at_epoch) > HIQLITE_BACKEND_TIME_ATTESTATION_MAX_AGE_SECS {
        bail!(
            "{} Hiqlite backend-time attestation attested_at is older than {} seconds",
            path.display(),
            HIQLITE_BACKEND_TIME_ATTESTATION_MAX_AGE_SECS
        );
    }

    Ok(())
}

fn validate_hiqlite_backend_time_attester(path: &Path, attester: &str) -> anyhow::Result<()> {
    let attester = attester.trim();
    if attester.is_empty() {
        bail!(
            "{} Hiqlite backend-time attestation is missing attester",
            path.display()
        );
    }
    if !HIQLITE_BACKEND_TIME_ALLOWED_ATTESTERS.contains(&attester) {
        bail!(
            "{} Hiqlite backend-time attestation attester is not allowlisted: {attester}",
            path.display()
        );
    }

    Ok(())
}

fn parse_rfc3339_utc_epoch_seconds(value: &str) -> anyhow::Result<u64> {
    let value = value.trim();
    let Some(value) = value.strip_suffix('Z') else {
        bail!("timestamp must end with Z");
    };
    let Some((date, time)) = value.split_once('T') else {
        bail!("timestamp must contain T");
    };
    let mut date_parts = date.split('-');
    let year: i32 = parse_timestamp_part(date_parts.next(), "year")?;
    let month: u32 = parse_timestamp_part(date_parts.next(), "month")?;
    let day: u32 = parse_timestamp_part(date_parts.next(), "day")?;
    if date_parts.next().is_some() {
        bail!("timestamp date has too many parts");
    }
    let time = time.split_once('.').map_or(time, |(whole, _)| whole);
    let mut time_parts = time.split(':');
    let hour: u32 = parse_timestamp_part(time_parts.next(), "hour")?;
    let minute: u32 = parse_timestamp_part(time_parts.next(), "minute")?;
    let second: u32 = parse_timestamp_part(time_parts.next(), "second")?;
    if time_parts.next().is_some() {
        bail!("timestamp time has too many parts");
    }
    if hour > 23 || minute > 59 || second > 59 {
        bail!("timestamp time is out of range");
    }
    if year < 1970 {
        bail!("timestamp is before Unix epoch");
    }
    validate_date(&format!("{year:04}-{month:02}-{day:02}"))?;
    let days = days_from_civil(year, month, day);
    if days < 0 {
        bail!("timestamp is before Unix epoch");
    }

    Ok((days as u64 * 86_400) + (hour as u64 * 3_600) + (minute as u64 * 60) + second as u64)
}

fn parse_timestamp_part<T>(value: Option<&str>, name: &str) -> anyhow::Result<T>
where
    T: std::str::FromStr,
{
    value
        .filter(|part| !part.is_empty())
        .with_context(|| format!("timestamp missing {name}"))?
        .parse()
        .map_err(|_| anyhow::anyhow!("timestamp has invalid {name}"))
}

fn reject_local_readiness_artifact(evidence_kind: &str, path: &Path) -> anyhow::Result<()> {
    if matches!(
        evidence_kind,
        "local_s3_compatible_gate" | "generic_local_s3_compatible_gate" | "local_benchmark_gate"
    ) {
        bail!(
            "{} is local-scoped evidence ({evidence_kind}) and cannot satisfy release readiness",
            path.display()
        );
    }
    Ok(())
}

fn require_json_str<'a>(
    path: &Path,
    value: &'a serde_json::Value,
    pointer: &str,
) -> anyhow::Result<&'a str> {
    value
        .pointer(pointer)
        .and_then(serde_json::Value::as_str)
        .with_context(|| {
            format!(
                "{} product evidence missing string {pointer}",
                path.display()
            )
        })
}

fn require_nonempty_json_str(
    path: &Path,
    value: &serde_json::Value,
    pointer: &str,
) -> anyhow::Result<()> {
    if require_json_str(path, value, pointer)?.trim().is_empty() {
        bail!(
            "{} product evidence missing nonempty string {pointer}",
            path.display()
        );
    }

    Ok(())
}

fn require_json_string_array<'a>(
    path: &Path,
    value: &'a serde_json::Value,
    pointer: &str,
) -> anyhow::Result<Vec<&'a str>> {
    let items = value
        .pointer(pointer)
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} product evidence missing array {pointer}",
                path.display()
            )
        })?;
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        let value = item.as_str().filter(|value| !value.trim().is_empty());
        let Some(value) = value else {
            bail!(
                "{} product evidence requires {pointer} to contain nonempty strings",
                path.display()
            );
        };
        values.push(value);
    }
    Ok(values)
}

fn require_json_u64(path: &Path, value: &serde_json::Value, pointer: &str) -> anyhow::Result<u64> {
    value
        .pointer(pointer)
        .and_then(serde_json::Value::as_u64)
        .with_context(|| {
            format!(
                "{} product evidence missing integer {pointer}",
                path.display()
            )
        })
}

fn require_json_bool(
    path: &Path,
    value: &serde_json::Value,
    pointer: &str,
) -> anyhow::Result<bool> {
    value
        .pointer(pointer)
        .and_then(serde_json::Value::as_bool)
        .with_context(|| {
            format!(
                "{} product evidence missing boolean {pointer}",
                path.display()
            )
        })
}

fn require_json_true(path: &Path, value: &serde_json::Value, pointer: &str) -> anyhow::Result<()> {
    if require_json_bool(path, value, pointer)? {
        Ok(())
    } else {
        bail!(
            "{} product evidence requires {pointer}=true",
            path.display()
        )
    }
}

fn require_json_false(path: &Path, value: &serde_json::Value, pointer: &str) -> anyhow::Result<()> {
    if !require_json_bool(path, value, pointer)? {
        Ok(())
    } else {
        bail!(
            "{} product evidence requires {pointer}=false",
            path.display()
        )
    }
}

fn read_json_artifact<T: for<'de> Deserialize<'de>>(path: &Path) -> anyhow::Result<T> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read evidence artifact from {}", path.display()))?;
    serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse evidence artifact {}", path.display()))
}

fn read_artifact_evidence_kind(path: &Path) -> anyhow::Result<String> {
    let value: serde_json::Value = read_json_artifact(path)?;
    value
        .get("evidence_kind")
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
        .with_context(|| {
            format!(
                "evidence artifact {} is missing evidence_kind",
                path.display()
            )
        })
}

fn is_placeholder_commit(value: &str) -> bool {
    let value = value.trim().to_ascii_lowercase();
    value.is_empty()
        || value.contains("placeholder")
        || value == "unknown"
        || value == "local"
        || value.chars().all(|c| c == '0')
}

fn validate_sha256_digest(path: &Path, value: &str, label: &str) -> anyhow::Result<()> {
    let value = value.trim();
    let Some(hex) = value.strip_prefix("sha256:") else {
        bail!("{} {label} must start with sha256:", path.display());
    };
    if hex.len() != 64
        || !hex
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
    {
        bail!(
            "{} {label} must be a sha256 digest with 64 lowercase hex characters",
            path.display()
        );
    }
    Ok(())
}

fn validate_full_git_commit_sha(path: &Path, value: &str, label: &str) -> anyhow::Result<()> {
    let value = value.trim();
    if value.len() != 40
        || !value
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
    {
        bail!(
            "{} {label} must be a full 40-character lowercase git commit SHA",
            path.display()
        );
    }
    if value.contains('+') || is_placeholder_commit(value) {
        bail!(
            "{} {label} must be clean and non-placeholder",
            path.display()
        );
    }
    Ok(())
}

fn decode_base64_field(path: &Path, value: &str, label: &str) -> anyhow::Result<Vec<u8>> {
    BASE64_STANDARD
        .decode(value.trim())
        .with_context(|| format!("{} {label} must be base64", path.display()))
}

fn sigstore_sha256_hash_from_prefixed_digest(
    path: &Path,
    value: &str,
    label: &str,
) -> anyhow::Result<SigstoreSha256Hash> {
    validate_sha256_digest(path, value, label)?;
    let hex = value.trim().trim_start_matches("sha256:");
    SigstoreSha256Hash::from_hex(hex).with_context(|| {
        format!(
            "{} {label} is not a valid Sigstore SHA-256 hash",
            path.display()
        )
    })
}

fn sha256_digest_of_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity("sha256:".len() + digest.len() * 2);
    output.push_str("sha256:");
    for byte in digest {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn is_local_dev_authority_store_id(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    [
        "memory://",
        "file://",
        "localhost",
        "127.0.0.1",
        "emulator",
        "local-only",
        "local only",
        "local filesystem",
    ]
    .iter()
    .any(|marker| value.contains(marker))
}

fn validate_production_gc_authority_store_id(authority_store_id: &str) -> anyhow::Result<()> {
    if authority_store_id.trim().is_empty() || is_local_dev_authority_store_id(authority_store_id) {
        bail!("gc-production-evidence rejects local/dev authority_store_id");
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProductionGcS3Config {
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    region: String,
    bucket: String,
    prefix: String,
    allow_http: bool,
}

fn production_gc_s3_config_from_env() -> anyhow::Result<ProductionGcS3Config> {
    production_gc_s3_config_from_lookup(|name| env::var(name).ok())
}

fn production_gc_s3_config_from_lookup(
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> anyhow::Result<ProductionGcS3Config> {
    if lookup("VELORIX_S3_COMPAT").as_deref() != Some("1") {
        bail!("gc-production-evidence requires VELORIX_S3_COMPAT=1");
    }

    let endpoint = required_production_gc_env(&mut lookup, "AWS_ENDPOINT_URL")?;
    let allow_http = endpoint.starts_with("http://");

    Ok(ProductionGcS3Config {
        endpoint,
        access_key_id: required_production_gc_env(&mut lookup, "AWS_ACCESS_KEY_ID")?,
        secret_access_key: required_production_gc_env(&mut lookup, "AWS_SECRET_ACCESS_KEY")?,
        region: required_production_gc_env(&mut lookup, "AWS_REGION")?,
        bucket: required_production_gc_env(&mut lookup, "VELORIX_S3_BUCKET")?,
        prefix: lookup("VELORIX_S3_PREFIX").unwrap_or_default(),
        allow_http,
    })
}

fn required_production_gc_env(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> anyhow::Result<String> {
    lookup(name)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .with_context(|| format!("gc-production-evidence requires {name}"))
}

fn production_gc_authority_store_from_env() -> anyhow::Result<Arc<dyn ObjectStore>> {
    production_gc_authority_store(production_gc_s3_config_from_env()?)
}

fn production_gc_authority_store(
    config: ProductionGcS3Config,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_endpoint(config.endpoint)
        .with_access_key_id(config.access_key_id)
        .with_secret_access_key(config.secret_access_key)
        .with_region(config.region)
        .with_bucket_name(config.bucket)
        .with_allow_http(config.allow_http)
        .build()
        .map_err(anyhow::Error::from)?;

    let prefix = config.prefix.trim().trim_matches('/').to_string();
    if prefix.is_empty() {
        Ok(Arc::new(store))
    } else {
        Ok(Arc::new(PrefixStore::new(
            store,
            ObjectStorePath::from(prefix),
        )))
    }
}

async fn generate_production_gc_run_evidence(
    store: Arc<dyn ObjectStore>,
    deployment_id: String,
    authority_store_id: String,
    gc_run_id: String,
) -> anyhow::Result<ProductionGcRunEvidenceArtifactV1> {
    validate_production_gc_authority_store_id(&authority_store_id)?;
    if deployment_id.trim().is_empty() {
        bail!("gc-production-evidence requires --deployment-id");
    }
    if gc_run_id.trim().is_empty() {
        bail!("gc-production-evidence requires --gc-run-id");
    }

    let publisher = production_gc_checkpoint_publisher(Arc::clone(&store), &gc_run_id).await?;
    let verified = publisher
        .verify_garbage_collection_run_retention_evidence(&gc_run_id)
        .await?;
    if verified.report.deleted.is_empty() {
        bail!("gc-production-evidence requires a live GC run with at least one deleted candidate");
    }
    let verified_gc_run_digest = garbage_collection_run_digest(&verified)?;
    let verified_gc_run_deleted_count = verified.report.deleted.len();
    let verified_gc_run_retain_latest_manifests = verified.policy.retain_latest_manifests;
    let verified_gc_run_deleted_object_keys = gc_run_deleted_object_keys(&verified);

    Ok(ProductionGcRunEvidenceArtifactV1 {
        schema_version: 1,
        status: "pass".to_string(),
        evidence_kind: "production_gc_run_evidence".to_string(),
        deployment_id,
        authority_store_id,
        gc_run_id,
        listing_consistency_checked: true,
        checkpoint_retention_records_checked: true,
        checkpoint_gc_transition_records_checked: true,
        verified_gc_run_digest: Some(verified_gc_run_digest),
        verified_gc_run_deleted_count: Some(verified_gc_run_deleted_count),
        verified_gc_run_retain_latest_manifests: Some(verified_gc_run_retain_latest_manifests),
        verified_gc_run_deleted_object_keys: Some(verified_gc_run_deleted_object_keys),
        _extra: BTreeMap::new(),
    })
}

fn garbage_collection_run_digest(run: &GarbageCollectionRunV1) -> anyhow::Result<String> {
    let canonical = serde_json::json!({
        "schema_version": run.schema_version,
        "run_id": run.run_id,
        "retain_latest_manifests": run.policy.retain_latest_manifests,
        "retained_manifest_versions": sorted_u64s(&run.plan.retained_manifest_versions),
        "plan_candidates": canonical_gc_candidates(&run.plan.candidates)?,
        "report_deleted": canonical_gc_candidates(&run.report.deleted)?,
        "report_skipped": canonical_gc_candidates(&run.report.skipped)?,
    });
    let bytes = serde_json::to_vec(&canonical).context("failed to serialize GC run digest")?;
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity("sha256:".len() + digest.len() * 2);
    output.push_str("sha256:");
    for byte in digest {
        output.push_str(&format!("{byte:02x}"));
    }
    Ok(output)
}

fn sha256_hex_of_file(path: &Path) -> anyhow::Result<String> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read {} for sha256 digest", path.display()))?;
    Ok(sha256_hex_of_bytes(&bytes))
}

fn sha256_hex_of_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn canonical_product_evidence_without_backend_time_attestation_bytes(
    path: &Path,
) -> anyhow::Result<Vec<u8>> {
    let mut value: serde_json::Value = read_json_artifact(path)?;
    if let Some(metadata_store) = value
        .get_mut("metadata_store")
        .and_then(serde_json::Value::as_object_mut)
    {
        metadata_store.remove("hiqlite_backend_time_attestation");
    }
    serde_json::to_vec(&value).with_context(|| {
        format!(
            "failed to canonicalize {} without metadata_store.hiqlite_backend_time_attestation",
            path.display()
        )
    })
}

fn sorted_u64s(values: &[u64]) -> Vec<u64> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted
}

fn canonical_gc_candidates(
    candidates: &[GarbageCollectionCandidate],
) -> anyhow::Result<Vec<serde_json::Value>> {
    let mut values = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        values.push(serde_json::json!({
            "kind": serde_json::to_value(candidate.kind)
                .context("failed to serialize GC candidate kind")?,
            "object_key": candidate.object_key.to_string(),
        }));
    }
    values.sort_by(|left, right| {
        let left_key = left["object_key"].as_str().unwrap_or_default();
        let right_key = right["object_key"].as_str().unwrap_or_default();
        left_key
            .cmp(right_key)
            .then_with(|| left["kind"].to_string().cmp(&right["kind"].to_string()))
    });
    Ok(values)
}

fn gc_run_deleted_object_keys(run: &GarbageCollectionRunV1) -> Vec<String> {
    let mut keys = run
        .report
        .deleted
        .iter()
        .map(|candidate| candidate.object_key.to_string())
        .collect::<Vec<_>>();
    keys.sort();
    keys
}

async fn execute_s3_compatible_garbage_collection(
    store: Arc<dyn ObjectStore>,
    authority_store_id: &str,
    run_id: &str,
    policy: GarbageCollectionPolicy,
) -> anyhow::Result<GarbageCollectionRunV1> {
    validate_production_gc_authority_store_id(authority_store_id)?;
    if run_id.trim().is_empty() {
        bail!("gc-execute-s3-compatible requires --run-id");
    }

    let publisher = production_gc_checkpoint_publisher(store, run_id).await?;
    let plan = publisher.plan_garbage_collection(policy).await?;
    let run = publisher
        .execute_garbage_collection_plan_with_evidence(run_id, policy, &plan)
        .await?;
    let verified = publisher
        .verify_garbage_collection_run_retention_evidence(run_id)
        .await?;
    if verified != run {
        bail!("verified S3-compatible GC run differs from executed run");
    }

    Ok(verified)
}

async fn seed_s3_compatible_gc_fixture(
    store: Arc<dyn ObjectStore>,
    authority_store_id: &str,
    seed_id: &str,
) -> anyhow::Result<S3CompatibleGcSeedFixtureArtifactV1> {
    validate_production_gc_authority_store_id(authority_store_id)?;
    if seed_id.trim().is_empty() {
        bail!("gc-seed-s3-compatible-fixture requires --seed-id");
    }

    let publisher = production_gc_checkpoint_publisher(store, seed_id).await?;
    let safe_seed_id = sanitize_gc_identifier(seed_id);
    let state_object_id_0 = format!("{safe_seed_id}-state-0000");
    let state_object_id_1 = format!("{safe_seed_id}-state-0001");

    let state_0 = StateObjectWrite::new(
        ORDERS_SUM_COUNT_OWNER,
        0,
        0,
        state_object_id_0.clone(),
        Bytes::from(format!("velorix-gc-seed:{seed_id}:checkpoint-0").into_bytes()),
    )
    .context("failed to construct release GC seed state object for checkpoint 0")?;
    let expected_deleted_object_key = state_0.object_key().to_string();
    let state_ref_0 = publisher
        .write_state_object(&state_0)
        .await
        .context("failed to write release GC seed state object for checkpoint 0")?;
    publisher
        .publish_manifest(&CheckpointManifest {
            schema_version: 1,
            checkpoint_version: 0,
            input_ranges: vec![InputRange {
                stream_id: "rustfs-gc-seed".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: 1,
            }],
            state_objects: vec![state_ref_0],
            output_objects: vec![],
            parent_checkpoint: None,
            created_at: "2026-05-31T00:00:00Z".to_string(),
        })
        .await
        .context("failed to publish release GC seed manifest for checkpoint 0")?;

    let state_1 = StateObjectWrite::new(
        ORDERS_SUM_COUNT_OWNER,
        0,
        1,
        state_object_id_1.clone(),
        Bytes::from(format!("velorix-gc-seed:{seed_id}:checkpoint-1").into_bytes()),
    )
    .context("failed to construct release GC seed state object for checkpoint 1")?;
    let state_ref_1 = publisher
        .write_state_object(&state_1)
        .await
        .context("failed to write release GC seed state object for checkpoint 1")?;
    publisher
        .publish_manifest(&CheckpointManifest {
            schema_version: 1,
            checkpoint_version: 1,
            input_ranges: vec![InputRange {
                stream_id: "rustfs-gc-seed".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
            }],
            state_objects: vec![state_ref_1],
            output_objects: vec![],
            parent_checkpoint: Some(0),
            created_at: "2026-05-31T00:01:00Z".to_string(),
        })
        .await
        .context("failed to publish release GC seed manifest for checkpoint 1")?;

    Ok(S3CompatibleGcSeedFixtureArtifactV1 {
        schema_version: 1,
        status: "pass".to_string(),
        evidence_kind: "s3_compatible_gc_seed_fixture".to_string(),
        fixture_kind: "release_smoke_gc_fixture".to_string(),
        authority_store_id: authority_store_id.to_string(),
        seed_id: seed_id.to_string(),
        checkpoint_versions: vec![0, 1],
        state_object_ids: vec![state_object_id_0, state_object_id_1],
        expected_deleted_object_keys: vec![expected_deleted_object_key],
        state_objects_written: 2,
        expected_min_deleted_candidates: 1,
        expected_deleted_candidates_at_retain_latest_manifests: 1,
        _extra: BTreeMap::new(),
    })
}

fn validate_rustfs_production_gc_evidence_family(
    gate_evidence_path: &Path,
    seed_evidence_path: &Path,
    execute_evidence_path: &Path,
    production_evidence_path: &Path,
) -> anyhow::Result<RustfsProductionGcEvidenceValidationReportV1> {
    let gate: serde_json::Value = read_json_artifact(gate_evidence_path)?;
    let seed: S3CompatibleGcSeedFixtureArtifactV1 = read_json_artifact(seed_evidence_path)?;
    let run: GarbageCollectionRunV1 = read_json_artifact(execute_evidence_path)?;
    let production: ProductionGcRunEvidenceArtifactV1 =
        read_json_artifact(production_evidence_path)?;

    let gate_schema_version = require_json_u64(gate_evidence_path, &gate, "/schema_version")?;
    if gate_schema_version != 1 {
        bail!(
            "{} has unsupported schema_version {}, expected 1",
            gate_evidence_path.display(),
            gate_schema_version
        );
    }
    if require_json_str(gate_evidence_path, &gate, "/evidence_kind")? != "rustfs_s3_compatible_gate"
    {
        bail!(
            "{} has evidence_kind other than rustfs_s3_compatible_gate",
            gate_evidence_path.display()
        );
    }
    let readiness_kinds =
        require_json_string_array(gate_evidence_path, &gate, "/readiness_evidence_kind")?;
    if !readiness_kinds.contains(&"s3_compatible_integration_harness") {
        bail!(
            "{} RustFS gate evidence is missing s3_compatible_integration_harness",
            gate_evidence_path.display()
        );
    }
    let detail_kinds = require_json_string_array(gate_evidence_path, &gate, "/gate_detail_kind")?;
    if !detail_kinds.contains(&"s3_compatible_gc_execution_retention") {
        bail!(
            "{} RustFS gate evidence is missing s3_compatible_gc_execution_retention",
            gate_evidence_path.display()
        );
    }
    require_json_true(
        gate_evidence_path,
        &gate,
        "/production_gc_artifact/generated",
    )?;
    if require_json_str(
        gate_evidence_path,
        &gate,
        "/production_gc_artifact/evidence_kind",
    )? != "production_gc_run_evidence"
    {
        bail!(
            "{} RustFS gate production_gc_artifact evidence_kind must be production_gc_run_evidence",
            gate_evidence_path.display()
        );
    }

    if seed.schema_version != 1
        || seed.status != "pass"
        || seed.evidence_kind != "s3_compatible_gc_seed_fixture"
        || seed.fixture_kind != "release_smoke_gc_fixture"
    {
        bail!(
            "{} is not a passing release_smoke_gc_fixture seed artifact",
            seed_evidence_path.display()
        );
    }
    if seed.expected_min_deleted_candidates == 0 {
        bail!(
            "{} seed evidence must expect at least one deleted candidate",
            seed_evidence_path.display()
        );
    }
    if seed.expected_deleted_object_keys.is_empty() {
        bail!(
            "{} seed evidence must include expected_deleted_object_keys",
            seed_evidence_path.display()
        );
    }
    if seed.expected_deleted_object_keys.len()
        != seed.expected_deleted_candidates_at_retain_latest_manifests
    {
        bail!(
            "{} seed expected_deleted_object_keys length does not match expected_deleted_candidates_at_retain_latest_manifests",
            seed_evidence_path.display()
        );
    }
    if seed.state_objects_written < 2 || seed.checkpoint_versions.len() < 2 {
        bail!(
            "{} seed evidence must create at least two checkpoint state objects",
            seed_evidence_path.display()
        );
    }

    if run.schema_version != 1 {
        bail!(
            "{} has unsupported schema_version {}, expected 1",
            execute_evidence_path.display(),
            run.schema_version
        );
    }
    if run.report.deleted.len() < seed.expected_min_deleted_candidates {
        bail!(
            "{} deleted {} candidates, below seed expected minimum {}",
            execute_evidence_path.display(),
            run.report.deleted.len(),
            seed.expected_min_deleted_candidates
        );
    }
    if run.plan.candidates.is_empty() {
        bail!(
            "{} production GC execute evidence has no planned candidates",
            execute_evidence_path.display()
        );
    }
    let deleted_object_keys = gc_run_deleted_object_keys(&run);
    let mut expected_deleted_object_keys = seed.expected_deleted_object_keys.clone();
    expected_deleted_object_keys.sort();
    if deleted_object_keys != expected_deleted_object_keys {
        bail!(
            "{} production GC execute evidence deleted keys do not match seeded expectation",
            execute_evidence_path.display(),
        );
    }
    let run_digest = garbage_collection_run_digest(&run)?;

    validate_production_gc_run_evidence_artifact(
        production_evidence_path,
        &production.deployment_id,
        &production.authority_store_id,
    )?;
    if production.evidence_kind != "production_gc_run_evidence"
        || production.status != "pass"
        || !production.listing_consistency_checked
        || !production.checkpoint_retention_records_checked
        || !production.checkpoint_gc_transition_records_checked
    {
        bail!(
            "{} is not a passing production GC evidence artifact",
            production_evidence_path.display()
        );
    }
    if production.verified_gc_run_digest.as_deref() != Some(run_digest.as_str()) {
        bail!(
            "{} verified_gc_run_digest does not match execute artifact digest",
            production_evidence_path.display()
        );
    }
    if production.verified_gc_run_deleted_count != Some(run.report.deleted.len()) {
        bail!(
            "{} verified_gc_run_deleted_count does not match execute artifact",
            production_evidence_path.display()
        );
    }
    if production.verified_gc_run_retain_latest_manifests
        != Some(run.policy.retain_latest_manifests)
    {
        bail!(
            "{} verified_gc_run_retain_latest_manifests does not match execute artifact",
            production_evidence_path.display()
        );
    }
    if production.verified_gc_run_deleted_object_keys.as_deref()
        != Some(deleted_object_keys.as_slice())
    {
        bail!(
            "{} verified_gc_run_deleted_object_keys does not match execute artifact",
            production_evidence_path.display()
        );
    }

    let gate_authority_store_id = require_json_str(
        gate_evidence_path,
        &gate,
        "/production_gc_artifact/authority_store_id",
    )?;
    let gate_gc_run_id = require_json_str(
        gate_evidence_path,
        &gate,
        "/production_gc_artifact/gc_run_id",
    )?;
    let gate_deployment_id = require_json_str(
        gate_evidence_path,
        &gate,
        "/production_gc_artifact/deployment_id",
    )?;
    let gate_retain_latest_manifests = require_json_u64(
        gate_evidence_path,
        &gate,
        "/production_gc_artifact/retain_latest_manifests",
    )? as usize;
    if gate_authority_store_id != seed.authority_store_id
        || gate_authority_store_id != production.authority_store_id
    {
        bail!("RustFS production GC evidence authority_store_id values do not match");
    }
    if gate_gc_run_id != seed.seed_id
        || gate_gc_run_id != run.run_id
        || gate_gc_run_id != production.gc_run_id
    {
        bail!("RustFS production GC evidence run identifiers do not match");
    }
    if gate_deployment_id != production.deployment_id {
        bail!("RustFS production GC evidence deployment_id values do not match");
    }
    if gate_retain_latest_manifests != run.policy.retain_latest_manifests {
        bail!("RustFS production GC retain_latest_manifests values do not match");
    }
    if seed.expected_deleted_candidates_at_retain_latest_manifests != run.report.deleted.len() {
        bail!("RustFS production GC deleted candidate count does not match seeded expectation");
    }

    for (pointer, path) in [
        (
            "/production_gc_artifact/seed_artifact_path",
            seed_evidence_path,
        ),
        (
            "/production_gc_artifact/execute_artifact_path",
            execute_evidence_path,
        ),
        (
            "/production_gc_artifact/artifact_path",
            production_evidence_path,
        ),
    ] {
        let reported = require_json_str(gate_evidence_path, &gate, pointer)?;
        if !evidence_path_matches(reported, path) {
            bail!(
                "{} RustFS gate {pointer} did not match {}",
                gate_evidence_path.display(),
                path.display()
            );
        }
    }

    Ok(RustfsProductionGcEvidenceValidationReportV1 {
        schema_version: 1,
        status: "pass".to_string(),
        evidence_kind: "rustfs_production_gc_evidence_family_validated".to_string(),
        gate_evidence_path: gate_evidence_path.display().to_string(),
        seed_evidence_path: seed_evidence_path.display().to_string(),
        execute_evidence_path: execute_evidence_path.display().to_string(),
        production_evidence_path: production_evidence_path.display().to_string(),
        deployment_id: production.deployment_id,
        authority_store_id: production.authority_store_id,
        gc_run_id: production.gc_run_id,
        retain_latest_manifests: run.policy.retain_latest_manifests,
        deleted_candidates: run.report.deleted.len(),
        checks: vec![
            "rustfs_s3_compatible_gate_present".to_string(),
            "seed_fixture_created_retired_checkpoint_state".to_string(),
            "s3_gc_execute_deleted_seeded_candidate".to_string(),
            "production_gc_evidence_verified_listing_retention_and_transition".to_string(),
            "artifact_family_paths_and_identity_bound".to_string(),
        ],
    })
}

fn evidence_path_matches(reported: &str, path: &Path) -> bool {
    let normalized_reported = reported.replace('\\', "/");
    let normalized_path = path.display().to_string().replace('\\', "/");
    if normalized_reported == normalized_path {
        return true;
    }
    if let Ok(current_dir) = env::current_dir() {
        if let Ok(relative) = path.strip_prefix(current_dir) {
            return normalized_reported == relative.display().to_string().replace('\\', "/");
        }
    }
    false
}

async fn production_gc_checkpoint_publisher(
    store: Arc<dyn ObjectStore>,
    gc_run_id: &str,
) -> anyhow::Result<CheckpointPublisher> {
    let capabilities = production_gc_authoritative_capabilities(store.as_ref(), gc_run_id).await?;
    capabilities
        .validate_for_startup()
        .map_err(anyhow::Error::from)?;
    CheckpointPublisher::new_authoritative(store, &capabilities).map_err(anyhow::Error::from)
}

async fn production_gc_authoritative_capabilities(
    store: &dyn ObjectStore,
    gc_run_id: &str,
) -> anyhow::Result<AuthoritativeObjectStoreCapabilitiesV1> {
    let probe_id = sanitize_gc_identifier(gc_run_id);
    probe_authoritative_object_store_capabilities(
        store,
        "gc-production-evidence",
        format!("v1/gc-production-evidence-capability-probes/{probe_id}"),
    )
    .await
    .map_err(anyhow::Error::from)
}

fn sanitize_gc_identifier(value: &str) -> String {
    let sanitized = value
        .trim()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "unknown-gc-run".to_string()
    } else {
        sanitized
    }
}

fn format_s3_compatible_gc_seed_fixture_json(
    artifact: &S3CompatibleGcSeedFixtureArtifactV1,
) -> anyhow::Result<String> {
    serde_json::to_string_pretty(artifact)
        .context("failed to serialize S3-compatible GC seed fixture")
}

fn format_s3_compatible_gc_seed_fixture(artifact: &S3CompatibleGcSeedFixtureArtifactV1) -> String {
    format!(
        "s3_compatible_gc_seed_fixture status={} fixture_kind={} authority_store_id={} seed_id={} checkpoint_versions={:?} state_objects_written={} expected_min_deleted_candidates={} expected_deleted_candidates_at_retain_latest_manifests={}\n",
        artifact.status,
        artifact.fixture_kind,
        artifact.authority_store_id,
        artifact.seed_id,
        artifact.checkpoint_versions,
        artifact.state_objects_written,
        artifact.expected_min_deleted_candidates,
        artifact.expected_deleted_candidates_at_retain_latest_manifests
    )
}

fn format_production_gc_run_evidence_json(
    artifact: &ProductionGcRunEvidenceArtifactV1,
) -> anyhow::Result<String> {
    serde_json::to_string_pretty(artifact).map_err(anyhow::Error::from)
}

fn format_production_gc_run_evidence(artifact: &ProductionGcRunEvidenceArtifactV1) -> String {
    format!(
        "production_gc_run_evidence status={} deployment_id={} authority_store_id={} gc_run_id={} listing_consistency_checked={} checkpoint_retention_records_checked={} checkpoint_gc_transition_records_checked={}\n",
        artifact.status,
        artifact.deployment_id,
        artifact.authority_store_id,
        artifact.gc_run_id,
        artifact.listing_consistency_checked,
        artifact.checkpoint_retention_records_checked,
        artifact.checkpoint_gc_transition_records_checked
    )
}

fn format_rustfs_production_gc_evidence_report_json(
    report: &RustfsProductionGcEvidenceValidationReportV1,
) -> anyhow::Result<String> {
    serde_json::to_string_pretty(report)
        .context("failed to serialize RustFS production GC evidence validation report")
}

fn format_rustfs_production_gc_evidence_report(
    report: &RustfsProductionGcEvidenceValidationReportV1,
) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "rustfs_production_gc_evidence status={} deployment_id={} authority_store_id={} gc_run_id={} retain_latest_manifests={} deleted_candidates={}\n",
        report.status,
        report.deployment_id,
        report.authority_store_id,
        report.gc_run_id,
        report.retain_latest_manifests,
        report.deleted_candidates
    ));
    for check in &report.checks {
        output.push_str(&format!("[x] {check}\n"));
    }
    output
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct S3CompatibleAuthorityConfig {
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    region: String,
    bucket: String,
    prefix: String,
    allow_http: bool,
}

fn s3_compatible_authority_config_from_env() -> anyhow::Result<S3CompatibleAuthorityConfig> {
    s3_compatible_authority_config_from_lookup(|name| env::var(name).ok())
}

fn s3_compatible_authority_config_from_lookup(
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> anyhow::Result<S3CompatibleAuthorityConfig> {
    if lookup("VELORIX_S3_COMPAT").as_deref() != Some("1") {
        bail!("S3-compatible authority requires VELORIX_S3_COMPAT=1");
    }

    let endpoint = required_s3_compatible_authority_env(&mut lookup, "AWS_ENDPOINT_URL")?;
    let allow_http = endpoint.starts_with("http://");

    Ok(S3CompatibleAuthorityConfig {
        endpoint,
        access_key_id: required_s3_compatible_authority_env(&mut lookup, "AWS_ACCESS_KEY_ID")?,
        secret_access_key: required_s3_compatible_authority_env(
            &mut lookup,
            "AWS_SECRET_ACCESS_KEY",
        )?,
        region: required_s3_compatible_authority_env(&mut lookup, "AWS_REGION")?,
        bucket: required_s3_compatible_authority_env(&mut lookup, "VELORIX_S3_BUCKET")?,
        prefix: lookup("VELORIX_S3_PREFIX").unwrap_or_default(),
        allow_http,
    })
}

fn required_s3_compatible_authority_env(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> anyhow::Result<String> {
    lookup(name)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .with_context(|| format!("S3-compatible authority requires {name}"))
}

fn s3_compatible_authority_store_from_env() -> anyhow::Result<Arc<dyn ObjectStore>> {
    s3_compatible_authority_store(s3_compatible_authority_config_from_env()?)
}

fn s3_compatible_authority_store(
    config: S3CompatibleAuthorityConfig,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_endpoint(config.endpoint)
        .with_access_key_id(config.access_key_id)
        .with_secret_access_key(config.secret_access_key)
        .with_region(config.region)
        .with_bucket_name(config.bucket)
        .with_allow_http(config.allow_http)
        .build()
        .map_err(anyhow::Error::from)?;

    let prefix = config.prefix.trim().trim_matches('/').to_string();
    if prefix.is_empty() {
        Ok(Arc::new(store))
    } else {
        Ok(Arc::new(PrefixStore::new(
            store,
            ObjectStorePath::from(prefix),
        )))
    }
}

struct IngestWriterAppendRequest {
    authority_store_id: String,
    authority_namespace: String,
    operator_id: String,
    writer_id: String,
    payload: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IngestWriterAppendArtifactV1 {
    schema_version: u16,
    evidence_kind: String,
    status: String,
    authority_store_id: String,
    authority_namespace: String,
    operator_id: String,
    writer_id: String,
    startup_active_admission_records: usize,
    startup_expired_orphan_admission_records: usize,
    outcome: String,
    descriptor: IngestWriterAppendDescriptorV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IngestWriterAppendDescriptorV1 {
    stream_id: String,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    object_key: String,
}

async fn run_ingest_writer_append(
    store: Arc<dyn ObjectStore>,
    request: IngestWriterAppendRequest,
) -> anyhow::Result<IngestWriterAppendArtifactV1> {
    validate_ingest_writer_authority_store_id(&request.authority_store_id)?;
    if request.authority_namespace.trim().is_empty() {
        bail!("ingest-writer-append requires --authority-namespace");
    }
    if request.operator_id.trim().is_empty() {
        bail!("ingest-writer-append requires --operator-id");
    }
    if request.writer_id.trim().is_empty() {
        bail!("ingest-writer-append requires --writer-id");
    }
    if request.payload.is_empty() {
        bail!("ingest-writer-append requires a non-empty --payload-file");
    }

    let probe_id = sanitize_probe_id(&format!("{}-{}", request.operator_id, request.writer_id));
    let validated = validate_operator_authority(
        ObjectStoreAuthorityRef {
            store_id: request.authority_store_id.clone(),
            namespace: request.authority_namespace.clone(),
        },
        store,
        "ingest-writer-append",
        format!("v1/ingest-writer-capability-probes/{probe_id}"),
    )
    .await
    .map_err(anyhow::Error::from)?;
    let components = OperatorAuthorityStartupComponents::from_validated_authority(validated);
    let runtime = DeployedIngestWriterRuntime::from_startup_components(&components)
        .await
        .map_err(anyhow::Error::from)?;
    let startup_report = runtime.startup_report().clone();
    let outcome = runtime
        .append_catalog_validated_envelope(request.payload)
        .await
        .map_err(anyhow::Error::from)?;
    let (outcome, descriptor) = ingest_writer_append_outcome_parts(outcome)?;

    Ok(IngestWriterAppendArtifactV1 {
        schema_version: 1,
        evidence_kind: "ingest_writer_checked_runtime_append".to_string(),
        status: "pass".to_string(),
        authority_store_id: request.authority_store_id,
        authority_namespace: request.authority_namespace,
        operator_id: request.operator_id,
        writer_id: request.writer_id,
        startup_active_admission_records: startup_report.active_admission_records,
        startup_expired_orphan_admission_records: startup_report.expired_orphan_admission_records,
        outcome,
        descriptor,
    })
}

fn validate_ingest_writer_authority_store_id(authority_store_id: &str) -> anyhow::Result<()> {
    let trimmed = authority_store_id.trim();
    if trimmed.is_empty() {
        bail!("ingest-writer-append requires --authority-store-id");
    }
    if trimmed.starts_with("file:")
        || trimmed.starts_with("local:")
        || trimmed.eq_ignore_ascii_case("local")
        || trimmed.eq_ignore_ascii_case("dev")
    {
        bail!(
            "ingest-writer-append authority_store_id must not be local/dev: {authority_store_id}"
        );
    }
    Ok(())
}

fn ingest_writer_append_outcome_parts(
    outcome: AppendValidatedEnvelopeOutcome,
) -> anyhow::Result<(String, IngestWriterAppendDescriptorV1)> {
    match outcome {
        AppendValidatedEnvelopeOutcome::Appended { descriptor } => Ok((
            "appended".to_string(),
            ingest_writer_descriptor(&descriptor),
        )),
        AppendValidatedEnvelopeOutcome::Duplicate { descriptor } => Ok((
            "duplicate".to_string(),
            ingest_writer_descriptor(&descriptor),
        )),
        AppendValidatedEnvelopeOutcome::Conflict {
            descriptor,
            object_key,
            reason,
        } => bail!(
            "ingest-writer-append conflicted before append: stream={} partition={} offsets={}-{} object_key={} reason={}",
            descriptor.stream_id,
            descriptor.partition_id,
            descriptor.start_offset_inclusive,
            descriptor.end_offset_exclusive,
            object_key.as_str(),
            reason
        ),
    }
}

fn ingest_writer_descriptor(descriptor: &IngestBatchDescriptor) -> IngestWriterAppendDescriptorV1 {
    IngestWriterAppendDescriptorV1 {
        stream_id: descriptor.stream_id.clone(),
        partition_id: descriptor.partition_id,
        start_offset_inclusive: descriptor.start_offset_inclusive,
        end_offset_exclusive: descriptor.end_offset_exclusive,
        object_key: descriptor.object_key.as_str().to_string(),
    }
}

fn format_ingest_writer_append_json(
    artifact: &IngestWriterAppendArtifactV1,
) -> anyhow::Result<String> {
    serde_json::to_string_pretty(artifact).map_err(anyhow::Error::from)
}

fn format_ingest_writer_append(artifact: &IngestWriterAppendArtifactV1) -> String {
    format!(
        "ingest_writer_append status={} outcome={} authority_store_id={} authority_namespace={} operator_id={} writer_id={} stream_id={} partition_id={} offsets={}-{} object_key={}\n",
        artifact.status,
        artifact.outcome,
        artifact.authority_store_id,
        artifact.authority_namespace,
        artifact.operator_id,
        artifact.writer_id,
        artifact.descriptor.stream_id,
        artifact.descriptor.partition_id,
        artifact.descriptor.start_offset_inclusive,
        artifact.descriptor.end_offset_exclusive,
        artifact.descriptor.object_key
    )
}

fn sanitize_probe_id(value: &str) -> String {
    let probe_id = value
        .trim()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    if probe_id.is_empty() {
        "unknown".to_string()
    } else {
        probe_id
    }
}

fn validate_release_status_file(path: &Path) -> anyhow::Result<()> {
    let contents = fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read release status matrix from {}",
            path.display()
        )
    })?;
    validate_release_status_text(&contents)
}

fn validate_release_status_text(contents: &str) -> anyhow::Result<()> {
    let rows = parse_release_status_rows(contents)?;
    let mut errors = Vec::new();

    for required in REQUIRED_RELEASE_CONTRACTS {
        match rows.get(*required) {
            Some(row) => {
                if row.status != "complete" {
                    errors.push(format!(
                        "{required} status is {}, expected complete",
                        row.status
                    ));
                }
                if !is_no_blocking_tasks(&row.blocking_tasks) {
                    errors.push(format!(
                        "{required} blocking tasks are {}, expected none",
                        row.blocking_tasks
                    ));
                }
            }
            None => errors.push(format!("missing required release status row: {required}")),
        }
    }

    for contract in rows.keys() {
        if !REQUIRED_RELEASE_CONTRACTS.contains(&contract.as_str()) {
            errors.push(format!("unexpected release status row: {contract}"));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        bail!("{}", errors.join("; "))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ReleaseStatusRow {
    status: String,
    blocking_tasks: String,
}

fn parse_release_status_rows(contents: &str) -> anyhow::Result<BTreeMap<String, ReleaseStatusRow>> {
    let mut rows = BTreeMap::new();

    for line in contents.lines().filter(|line| line.starts_with('|')) {
        let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();

        if cells.len() == 5 && (cells[0] == "Contract" || cells[0].starts_with("---")) {
            continue;
        }
        if cells.len() != 5 {
            bail!("malformed release status table row: {line}");
        }

        let contract = cells[0].to_string();
        let row = ReleaseStatusRow {
            status: cells[3].to_string(),
            blocking_tasks: cells[4].to_string(),
        };
        if rows.insert(contract.clone(), row).is_some() {
            bail!("duplicate release status row: {contract}");
        }
    }

    Ok(rows)
}

fn is_no_blocking_tasks(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case("none")
}
fn format_readiness_report(report: &ProductionReadinessReportV1) -> String {
    let mut output = format!(
        "production_ready={}\ndeployment_id={}\nauthority_store_id={}\n",
        report.production_ready, report.deployment_id, report.authority_store_id
    );
    if !report.blocking_reasons.is_empty() {
        output.push_str("blocking_reasons:\n");
        for reason in &report.blocking_reasons {
            output.push_str(&format!("- {reason}\n"));
        }
    }
    output
}

fn ensure_readiness_report_passes(report: &ProductionReadinessReportV1) -> anyhow::Result<()> {
    if report.production_ready {
        Ok(())
    } else {
        bail!(
            "production readiness report is blocked: {}",
            report.blocking_reasons.join("; ")
        )
    }
}
fn validate_dependency_governance_file(
    path: &Path,
    deny_diagnostics: Option<&Path>,
) -> anyhow::Result<DependencyGovernanceEvidenceV1> {
    let contents = fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read dependency governance manifest from {}",
            path.display()
        )
    })?;
    let today = today_utc_string()?;
    let diagnostics = if let Some(deny_diagnostics) = deny_diagnostics {
        Some((
            deny_diagnostics,
            fs::read_to_string(deny_diagnostics).with_context(|| {
                format!(
                    "failed to read cargo-deny diagnostics from {}",
                    deny_diagnostics.display()
                )
            })?,
        ))
    } else {
        None
    };

    build_dependency_governance_evidence(path, &contents, diagnostics, &today)
        .with_context(|| format!("invalid dependency governance manifest {}", path.display()))
}

fn build_dependency_governance_evidence(
    manifest_path: &Path,
    manifest_contents: &str,
    diagnostics: Option<(&Path, String)>,
    today: &str,
) -> anyhow::Result<DependencyGovernanceEvidenceV1> {
    let manifest: DependencyGovernanceManifestV1 = serde_json::from_str(manifest_contents)
        .context("failed to parse dependency governance JSON")?;
    manifest.validate(today)?;

    let (cargo_deny, warning_counts) = if let Some((diagnostics_path, diagnostics_contents)) =
        diagnostics
    {
        (
            CargoDenyGovernanceEvidenceV1 {
                diagnostics_checked: true,
                diagnostics_path: Some(stable_path(diagnostics_path)),
            },
            compare_dependency_governance_diagnostics(&manifest, &diagnostics_contents)
                .with_context(|| {
                    format!(
                        "dependency governance manifest {} does not match cargo-deny diagnostics {}",
                        manifest_path.display(),
                        diagnostics_path.display()
                    )
                })?,
        )
    } else {
        (
            CargoDenyGovernanceEvidenceV1 {
                diagnostics_checked: false,
                diagnostics_path: None,
            },
            BTreeMap::new(),
        )
    };

    Ok(DependencyGovernanceEvidenceV1 {
        schema_version: 1,
        status: "pass",
        evidence_kind: "dependency_governance_validated",
        manifest: ManifestGovernanceEvidenceV1 {
            path: stable_path(manifest_path),
            name: manifest_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("")
                .to_string(),
        },
        cargo_deny,
        required_package_review_subjects: REQUIRED_PACKAGE_REVIEW_SUBJECTS
            .iter()
            .map(|subject| (*subject).to_string())
            .collect(),
        reviewed_package_subjects: manifest.package_review_subjects(),
        missing_required_package_review_subjects: Vec::new(),
        exception_counts_by_kind: manifest.exception_counts(),
        warning_counts_by_kind: warning_counts,
        external_audit_attestation: false,
    })
}

fn stable_path(path: &Path) -> String {
    let stable = if path.is_absolute() {
        std::env::current_dir()
            .ok()
            .and_then(|cwd| path.strip_prefix(cwd).ok().map(Path::to_path_buf))
            .unwrap_or_else(|| path.to_path_buf())
    } else {
        path.to_path_buf()
    };
    let value = stable.display().to_string();
    value.strip_prefix("./").unwrap_or(&value).to_string()
}

fn compare_dependency_governance_diagnostics(
    manifest: &DependencyGovernanceManifestV1,
    diagnostics_contents: &str,
) -> anyhow::Result<BTreeMap<String, usize>> {
    let expected = manifest.warning_exceptions();
    let actual = parse_cargo_deny_warning_diagnostics(diagnostics_contents)?;
    let mut errors = Vec::new();

    for warning in actual.difference(&expected) {
        errors.push(format!(
            "uncovered {} warning for {}",
            warning.kind.as_str(),
            warning.crate_name
        ));
    }
    for warning in expected.difference(&actual) {
        errors.push(format!(
            "stale {} exception for {}",
            warning.kind.as_str(),
            warning.crate_name
        ));
    }

    if !errors.is_empty() {
        bail!("{}", errors.join("; "));
    }

    Ok(warning_counts(&actual))
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct DependencyGovernanceEvidenceV1 {
    schema_version: u16,
    status: &'static str,
    evidence_kind: &'static str,
    manifest: ManifestGovernanceEvidenceV1,
    cargo_deny: CargoDenyGovernanceEvidenceV1,
    required_package_review_subjects: Vec<String>,
    reviewed_package_subjects: Vec<String>,
    missing_required_package_review_subjects: Vec<String>,
    exception_counts_by_kind: BTreeMap<String, usize>,
    warning_counts_by_kind: BTreeMap<String, usize>,
    external_audit_attestation: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct ManifestGovernanceEvidenceV1 {
    path: String,
    name: String,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct CargoDenyGovernanceEvidenceV1 {
    diagnostics_checked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    diagnostics_path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DependencyGovernanceEvidenceArtifactV1 {
    schema_version: u16,
    status: String,
    evidence_kind: String,
    cargo_deny: CargoDenyGovernanceEvidenceArtifactV1,
    missing_required_package_review_subjects: Vec<String>,
    #[serde(flatten)]
    _extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct CargoDenyGovernanceEvidenceArtifactV1 {
    diagnostics_checked: bool,
    #[serde(flatten)]
    _extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct BenchmarkGateEvidenceArtifactV1 {
    schema_version: u16,
    status: String,
    evidence_kind: String,
    gate_level: BenchmarkGateLevel,
    backend: BenchmarkBackend,
    backend_evidence_scope: BenchmarkEvidenceScope,
    workload: String,
    workload_metrics: Vec<String>,
    baseline_path: Option<String>,
    baseline_commit: Option<String>,
    result_commit: String,
    max_regression_fraction: Option<f64>,
    #[serde(flatten)]
    _extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ProductionGcRunEvidenceArtifactV1 {
    schema_version: u16,
    status: String,
    evidence_kind: String,
    deployment_id: String,
    authority_store_id: String,
    gc_run_id: String,
    listing_consistency_checked: bool,
    checkpoint_retention_records_checked: bool,
    checkpoint_gc_transition_records_checked: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    verified_gc_run_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    verified_gc_run_deleted_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    verified_gc_run_retain_latest_manifests: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    verified_gc_run_deleted_object_keys: Option<Vec<String>>,
    #[serde(flatten)]
    _extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RustfsProductionGcEvidenceValidationReportV1 {
    schema_version: u16,
    status: String,
    evidence_kind: String,
    gate_evidence_path: String,
    seed_evidence_path: String,
    execute_evidence_path: String,
    production_evidence_path: String,
    deployment_id: String,
    authority_store_id: String,
    gc_run_id: String,
    retain_latest_manifests: usize,
    deleted_candidates: usize,
    checks: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct S3CompatibleGcSeedFixtureArtifactV1 {
    schema_version: u16,
    status: String,
    evidence_kind: String,
    fixture_kind: String,
    authority_store_id: String,
    seed_id: String,
    checkpoint_versions: Vec<u64>,
    state_object_ids: Vec<String>,
    #[serde(default)]
    expected_deleted_object_keys: Vec<String>,
    state_objects_written: usize,
    expected_min_deleted_candidates: usize,
    expected_deleted_candidates_at_retain_latest_manifests: usize,
    #[serde(flatten)]
    _extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct IngestWriterLifecycleEvidenceArtifactV1 {
    schema_version: u16,
    evidence_kind: String,
    deployment_id: String,
    authority_store_id: String,
    deployed_topology: String,
    pod_internal_append_completed: bool,
    multi_pod_overlap_conflict_rejected: bool,
    adjacent_append_succeeded: bool,
    crash_restart_reconstruction_checked: bool,
    leader_handoff_checked: bool,
    kubernetes_lease_handoff_checked: bool,
    lease_held_through_append_checked: bool,
    commit_guard_checked: bool,
    admission_commit_guard_bound_checked: bool,
    lease_loss_during_reservation_checked: bool,
    no_pvc_created_by_vind: bool,
    attested_at: String,
    attester: String,
    #[serde(default)]
    evidence_provenance: BTreeMap<String, IngestWriterLifecycleEvidenceProvenanceV1>,
    #[serde(default)]
    evidence_files: BTreeMap<String, String>,
    #[serde(flatten)]
    _extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
struct IngestWriterLifecycleEvidenceProvenanceV1 {
    job_uid: String,
    pod_uid: String,
    pod_name: String,
    container_image: String,
    container_image_id: String,
    #[serde(flatten)]
    _extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DependencyGovernanceManifestV1 {
    schema_version: u16,
    msrv: DependencyGovernanceMsrvV1,
    package_reviews: Vec<DependencyGovernancePackageReviewV1>,
    exceptions: Vec<DependencyGovernanceExceptionV1>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DependencyGovernanceMsrvV1 {
    minimum_rust_version: String,
    policy: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DependencyGovernancePackageReviewV1 {
    subject: String,
    owner: Option<String>,
    reviewed_on: Option<String>,
    audit_status: Option<String>,
    feature_policy: Option<String>,
    replacement_plan: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DependencyGovernanceExceptionV1 {
    kind: DependencyGovernanceExceptionKind,
    #[serde(rename = "crate")]
    crate_name: String,
    owner: Option<String>,
    expires_on: Option<String>,
    reason: Option<String>,
    replacement_plan: Option<String>,
    promotion_rule: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DependencyGovernanceExceptionKind {
    Duplicate,
    Unmaintained,
    Advisory,
    Yanked,
}

impl DependencyGovernanceExceptionKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Duplicate => "duplicate",
            Self::Unmaintained => "unmaintained",
            Self::Advisory => "advisory",
            Self::Yanked => "yanked",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct DependencyGovernanceWarning {
    kind: CargoDenyWarningKind,
    crate_name: String,
}

const REQUIRED_PACKAGE_REVIEW_SUBJECTS: &[&str] = &[
    "datafusion",
    "object_store",
    "kube",
    "k8s-openapi",
    "slatedb",
    "foyer",
    "hiqlite",
    "materialized_view_runtime",
];

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
enum CargoDenyWarningKind {
    Duplicate,
    Unmaintained,
    Yanked,
}

impl DependencyGovernanceManifestV1 {
    fn validate(&self, today: &str) -> anyhow::Result<()> {
        if self.schema_version != 1 {
            bail!(
                "unsupported dependency governance schema_version {}, expected 1",
                self.schema_version
            );
        }
        self.msrv.validate()?;
        validate_package_reviews(&self.package_reviews)?;
        validate_date(today).context("validator today must use YYYY-MM-DD")?;

        let mut seen_exceptions = BTreeSet::new();
        for exception in &self.exceptions {
            exception.validate(today)?;
            let key = (exception.kind.as_str(), exception.crate_name.as_str());
            if !seen_exceptions.insert(key) {
                bail!(
                    "duplicate dependency governance exception for {} {}",
                    exception.kind.as_str(),
                    exception.crate_name
                );
            }
        }

        Ok(())
    }

    fn warning_exceptions(&self) -> BTreeSet<DependencyGovernanceWarning> {
        self.exceptions
            .iter()
            .filter_map(|exception| {
                let kind = CargoDenyWarningKind::from_exception_kind(&exception.kind)?;
                Some(DependencyGovernanceWarning {
                    kind,
                    crate_name: exception.crate_name.clone(),
                })
            })
            .collect()
    }

    fn package_review_subjects(&self) -> Vec<String> {
        self.package_reviews
            .iter()
            .map(|review| review.subject.clone())
            .collect()
    }

    fn exception_counts(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for exception in &self.exceptions {
            *counts
                .entry(exception.kind.as_str().to_string())
                .or_insert(0) += 1;
        }
        counts
    }
}

impl CargoDenyWarningKind {
    fn from_exception_kind(kind: &DependencyGovernanceExceptionKind) -> Option<Self> {
        match kind {
            DependencyGovernanceExceptionKind::Duplicate => Some(Self::Duplicate),
            DependencyGovernanceExceptionKind::Unmaintained => Some(Self::Unmaintained),
            DependencyGovernanceExceptionKind::Yanked => Some(Self::Yanked),
            DependencyGovernanceExceptionKind::Advisory => None,
        }
    }

    fn from_diagnostic_code(code: &str) -> Option<Self> {
        match code {
            "duplicate" => Some(Self::Duplicate),
            "unmaintained" => Some(Self::Unmaintained),
            "yanked" => Some(Self::Yanked),
            _ => None,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Duplicate => "duplicate",
            Self::Unmaintained => "unmaintained",
            Self::Yanked => "yanked",
        }
    }
}

fn parse_cargo_deny_warning_diagnostics(
    contents: &str,
) -> anyhow::Result<BTreeSet<DependencyGovernanceWarning>> {
    let mut warnings = BTreeSet::new();

    for (index, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let value: serde_json::Value = serde_json::from_str(line)
            .with_context(|| format!("invalid cargo-deny JSONL on line {}", index + 1))?;
        if value["type"] != "diagnostic" || value["fields"]["severity"] != "warning" {
            continue;
        }

        let Some(code) = cargo_deny_diagnostic_code(&value) else {
            continue;
        };
        let Some(kind) = CargoDenyWarningKind::from_diagnostic_code(code) else {
            continue;
        };

        match kind {
            CargoDenyWarningKind::Duplicate => {
                let crate_names = duplicate_diagnostic_crate_names(&value);
                if crate_names.is_empty() {
                    bail!(
                        "cargo-deny duplicate warning on line {} did not include crate names",
                        index + 1
                    );
                }
                for crate_name in crate_names {
                    warnings.insert(DependencyGovernanceWarning {
                        kind: CargoDenyWarningKind::Duplicate,
                        crate_name,
                    });
                }
            }
            CargoDenyWarningKind::Unmaintained => {
                let Some(crate_name) = unmaintained_diagnostic_crate_name(&value) else {
                    bail!(
                        "cargo-deny unmaintained warning on line {} did not include a crate name",
                        index + 1
                    );
                };
                warnings.insert(DependencyGovernanceWarning { kind, crate_name });
            }
            CargoDenyWarningKind::Yanked => {
                let Some(crate_name) = cargo_deny_graph_crate_name(&value) else {
                    bail!(
                        "cargo-deny yanked warning on line {} did not include a crate name",
                        index + 1
                    );
                };
                warnings.insert(DependencyGovernanceWarning { kind, crate_name });
            }
        }
    }

    Ok(warnings)
}

fn warning_counts(warnings: &BTreeSet<DependencyGovernanceWarning>) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for warning in warnings {
        *counts.entry(warning.kind.as_str().to_string()).or_insert(0) += 1;
    }
    counts
}

fn cargo_deny_diagnostic_code(value: &serde_json::Value) -> Option<&str> {
    value["fields"]["code"]
        .as_str()
        .or_else(|| value["fields"]["code"]["code"].as_str())
}

fn duplicate_diagnostic_crate_names(value: &serde_json::Value) -> BTreeSet<String> {
    value["fields"]["graphs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|graph| graph["Krate"]["name"].as_str())
        .map(str::to_string)
        .collect()
}

fn unmaintained_diagnostic_crate_name(value: &serde_json::Value) -> Option<String> {
    value["fields"]["labels"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|label| label["span"].as_str())
        .filter_map(|span| span.split_whitespace().next())
        .next()
        .or_else(|| {
            value["fields"]["message"]
                .as_str()
                .and_then(|message| message.split_once(" - ").map(|(crate_name, _)| crate_name))
        })
        .map(str::to_string)
}

fn cargo_deny_graph_crate_name(value: &serde_json::Value) -> Option<String> {
    value["fields"]["graphs"]
        .as_array()
        .and_then(|graphs| graphs.first())
        .and_then(|graph| graph["Krate"]["name"].as_str())
        .map(str::to_string)
}

impl DependencyGovernanceMsrvV1 {
    fn validate(&self) -> anyhow::Result<()> {
        if !is_rust_version(&self.minimum_rust_version) {
            bail!(
                "msrv.minimum_rust_version must use Rust version form MAJOR.MINOR.PATCH, got {}",
                self.minimum_rust_version
            );
        }
        if self.policy.trim().is_empty() {
            bail!("msrv.policy must be non-empty");
        }
        Ok(())
    }
}

fn validate_package_reviews(reviews: &[DependencyGovernancePackageReviewV1]) -> anyhow::Result<()> {
    let mut seen = BTreeSet::new();
    for review in reviews {
        review.validate()?;
        if !seen.insert(review.subject.clone()) {
            bail!("duplicate package review subject {}", review.subject);
        }
    }

    for subject in REQUIRED_PACKAGE_REVIEW_SUBJECTS {
        if !seen.contains(*subject) {
            bail!("missing required package review for {subject}");
        }
    }

    Ok(())
}

impl DependencyGovernancePackageReviewV1 {
    fn validate(&self) -> anyhow::Result<()> {
        require_non_empty_text(Some(&self.subject), "subject", "package review")?;
        require_non_empty_text(self.owner.as_deref(), "owner", &self.subject)?;
        let reviewed_on =
            require_non_empty_text(self.reviewed_on.as_deref(), "reviewed_on", &self.subject)?;
        validate_date(reviewed_on).with_context(|| {
            format!(
                "package review for {} has invalid reviewed_on",
                self.subject
            )
        })?;
        require_non_empty_text(self.audit_status.as_deref(), "audit_status", &self.subject)?;
        require_non_empty_text(
            self.feature_policy.as_deref(),
            "feature_policy",
            &self.subject,
        )?;
        require_non_empty_text(
            self.replacement_plan.as_deref(),
            "replacement_plan",
            &self.subject,
        )?;

        Ok(())
    }
}

impl DependencyGovernanceExceptionV1 {
    fn validate(&self, today: &str) -> anyhow::Result<()> {
        if self.crate_name.trim().is_empty() {
            bail!("dependency governance exception is missing crate");
        }

        require_non_empty(self.owner.as_deref(), "owner", &self.crate_name, &self.kind)?;
        require_non_empty(
            self.reason.as_deref(),
            "reason",
            &self.crate_name,
            &self.kind,
        )?;
        let expires_on = require_non_empty(
            self.expires_on.as_deref(),
            "expires_on",
            &self.crate_name,
            &self.kind,
        )?;
        validate_date(expires_on).with_context(|| {
            format!(
                "{:?} exception for {} has invalid expires_on",
                self.kind, self.crate_name
            )
        })?;
        if expires_on < today {
            bail!(
                "{:?} exception for {} expired on {}",
                self.kind,
                self.crate_name,
                expires_on
            );
        }
        require_non_empty(
            self.replacement_plan.as_deref(),
            "replacement_plan",
            &self.crate_name,
            &self.kind,
        )?;
        require_non_empty(
            self.promotion_rule.as_deref(),
            "promotion_rule",
            &self.crate_name,
            &self.kind,
        )?;

        Ok(())
    }
}

fn require_non_empty<'a>(
    value: Option<&'a str>,
    field: &str,
    crate_name: &str,
    kind: &DependencyGovernanceExceptionKind,
) -> anyhow::Result<&'a str> {
    match value.map(str::trim) {
        Some(value) if !value.is_empty() => Ok(value),
        _ => bail!("{kind:?} exception for {crate_name} is missing {field}"),
    }
}

fn require_non_empty_text<'a>(
    value: Option<&'a str>,
    field: &str,
    subject: &str,
) -> anyhow::Result<&'a str> {
    match value.map(str::trim) {
        Some(value) if !value.is_empty() => Ok(value),
        _ => bail!("{subject} is missing {field}"),
    }
}

fn is_rust_version(version: &str) -> bool {
    let mut parts = version.split('.');
    let Some(major) = parts.next() else {
        return false;
    };
    let Some(minor) = parts.next() else {
        return false;
    };
    let Some(patch) = parts.next() else {
        return false;
    };
    parts.next().is_none()
        && [major, minor, patch]
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
}

fn validate_date(date: &str) -> anyhow::Result<()> {
    let bytes = date.as_bytes();
    let shape_valid = bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit());
    if !shape_valid {
        bail!("expected YYYY-MM-DD");
    }

    let year: u32 = date[0..4].parse().context("invalid year")?;
    let month: u32 = date[5..7].parse().context("invalid month")?;
    let day: u32 = date[8..10].parse().context("invalid day")?;
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => bail!("month must be 01 through 12"),
    };
    if day == 0 || day > max_day {
        bail!("day is out of range for month");
    }
    Ok(())
}

#[allow(clippy::manual_is_multiple_of)]
fn is_leap_year(year: u32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn today_utc_string() -> anyhow::Result<String> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time is before Unix epoch")?
        .as_secs();
    let days = (seconds / 86_400) as i64;
    let (year, month, day) = civil_from_days(days);

    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i32, u32, u32) {
    // Howard Hinnant's civil calendar conversion; avoids a runtime date crate for one CLI check.
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };

    (year as i32, month as u32, day as u32)
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    // Inverse of civil_from_days, also from Howard Hinnant's civil calendar conversion.
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i32;
    let day = day as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468) as i64
}

fn local_object_store(object_store_dir: &Path) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let store = LocalFileSystem::new_with_prefix(object_store_dir).with_context(|| {
        format!(
            "failed to open local object store at {}",
            object_store_dir.display()
        )
    })?;

    Ok(Arc::new(store) as Arc<dyn ObjectStore>)
}

async fn local_admin_capabilities(
    store: &dyn ObjectStore,
) -> anyhow::Result<AuthoritativeObjectStoreCapabilitiesV1> {
    Ok(probe_authoritative_object_store_capabilities(
        store,
        "checkpoint-admin-local",
        "checkpoint-admin-local-capability-probes",
    )
    .await?)
}

async fn checked_local_admin_checkpoint_publisher(
    store: Arc<dyn ObjectStore>,
) -> anyhow::Result<CheckpointPublisher> {
    let capabilities = local_admin_capabilities(store.as_ref()).await?;
    capabilities
        .validate_for_startup()
        .map_err(anyhow::Error::from)?;

    CheckpointPublisher::new_authoritative(store, &capabilities).map_err(anyhow::Error::from)
}

async fn inspect_local_checkpoints(
    store: Arc<dyn ObjectStore>,
) -> anyhow::Result<CheckpointAdminInspection> {
    Ok(checked_local_admin_checkpoint_publisher(store)
        .await?
        .inspect_checkpoints()
        .await?)
}

async fn repair_local_latest_checkpoint_marker(
    store: Arc<dyn ObjectStore>,
) -> anyhow::Result<Option<LatestCandidateMarker>> {
    Ok(checked_local_admin_checkpoint_publisher(store)
        .await?
        .repair_latest_candidate_marker()
        .await?)
}

async fn repair_local_checkpoint_admin_records(
    store: Arc<dyn ObjectStore>,
) -> anyhow::Result<CheckpointAdminRepairReport> {
    Ok(checked_local_admin_checkpoint_publisher(store)
        .await?
        .repair_checkpoint_admin_records()
        .await?)
}

async fn plan_local_garbage_collection(
    store: Arc<dyn ObjectStore>,
    policy: GarbageCollectionPolicy,
) -> anyhow::Result<GarbageCollectionPlan> {
    Ok(checked_local_admin_checkpoint_publisher(store)
        .await?
        .plan_garbage_collection(policy)
        .await?)
}

async fn execute_local_garbage_collection(
    store: Arc<dyn ObjectStore>,
    run_id: &str,
    policy: GarbageCollectionPolicy,
) -> anyhow::Result<GarbageCollectionRunV1> {
    let publisher = checked_local_admin_checkpoint_publisher(store).await?;
    let plan = publisher.plan_garbage_collection(policy).await?;

    Ok(publisher
        .execute_garbage_collection_plan_with_evidence(run_id, policy, &plan)
        .await?)
}

fn format_checkpoint_inspection(inspection: &CheckpointAdminInspection) -> String {
    let latest = inspection
        .latest_valid_checkpoint
        .map_or_else(|| "none".to_string(), |checkpoint| checkpoint.to_string());
    let mut output = format!("latest_valid_checkpoint={latest}\nmanifests:\n");

    for manifest in &inspection.manifests {
        output.push_str(&format!(
            "checkpoint={} key={} lifecycle={} gc_transitions={} retention={} recovery_transitions={} status={}\n",
            manifest.checkpoint_version,
            manifest.manifest_key,
            format_lifecycle_status(manifest.lifecycle_status),
            manifest.gc_transition_records.len(),
            format_retention_status(manifest.retention_record.as_ref()),
            manifest.recovery_transition_records.len(),
            format_manifest_status(&manifest.status),
        ));
    }

    output
}

fn format_checkpoint_inspection_json(
    inspection: &CheckpointAdminInspection,
) -> anyhow::Result<String> {
    #[derive(Serialize)]
    struct CheckpointInspectionReport<'a> {
        schema_version: u16,
        #[serde(flatten)]
        inspection: &'a CheckpointAdminInspection,
    }

    serde_json::to_string_pretty(&CheckpointInspectionReport {
        schema_version: 3,
        inspection,
    })
    .context("failed to serialize checkpoint inspection")
}

fn format_checkpoint_latest_repair(marker: Option<&LatestCandidateMarker>) -> String {
    marker.map_or_else(
        || "latest_candidate_marker=none\n".to_string(),
        |marker| {
            format!(
                "latest_candidate_marker=checkpoint {} key={} digest={}\n",
                marker.checkpoint_version, marker.manifest_key, marker.manifest_digest
            )
        },
    )
}

fn format_checkpoint_latest_repair_json(
    marker: Option<&LatestCandidateMarker>,
) -> anyhow::Result<String> {
    #[derive(Serialize)]
    struct CheckpointLatestRepairReport<'a> {
        schema_version: u16,
        latest_candidate_marker: Option<&'a LatestCandidateMarker>,
    }

    serde_json::to_string_pretty(&CheckpointLatestRepairReport {
        schema_version: 1,
        latest_candidate_marker: marker,
    })
    .context("failed to serialize checkpoint latest repair")
}

fn format_checkpoint_repair(report: &CheckpointAdminRepairReport) -> String {
    let latest = report.latest_candidate_marker.as_ref().map_or_else(
        || "none".to_string(),
        |marker| marker.checkpoint_version.to_string(),
    );
    format!(
        "lifecycle_records_repaired={}\nlatest_candidate_marker={latest}\n",
        report.lifecycle_records_repaired.len()
    )
}

fn format_checkpoint_repair_json(report: &CheckpointAdminRepairReport) -> anyhow::Result<String> {
    #[derive(Serialize)]
    struct CheckpointRepairReport<'a> {
        schema_version: u16,
        #[serde(flatten)]
        report: &'a CheckpointAdminRepairReport,
    }

    serde_json::to_string_pretty(&CheckpointRepairReport {
        schema_version: 1,
        report,
    })
    .context("failed to serialize checkpoint repair")
}

fn format_gc_plan(plan: &GarbageCollectionPlan) -> String {
    let mut output = format!(
        "retained_manifest_versions={:?}\ncandidates:\n",
        plan.retained_manifest_versions
    );
    for candidate in &plan.candidates {
        output.push_str(&format!(
            "kind={:?} key={}\n",
            candidate.kind, candidate.object_key
        ));
    }
    output
}

fn format_gc_plan_json(plan: &GarbageCollectionPlan) -> anyhow::Result<String> {
    #[derive(Serialize)]
    struct GarbageCollectionPlanReport<'a> {
        schema_version: u16,
        #[serde(flatten)]
        plan: &'a GarbageCollectionPlan,
    }

    serde_json::to_string_pretty(&GarbageCollectionPlanReport {
        schema_version: 1,
        plan,
    })
    .context("failed to serialize garbage collection plan")
}

fn format_gc_run(run: &GarbageCollectionRunV1) -> String {
    format!(
        "run_id={}\nretained_manifest_versions={:?}\ndeleted={}\nskipped={}\n",
        run.run_id,
        run.plan.retained_manifest_versions,
        run.report.deleted.len(),
        run.report.skipped.len()
    )
}

fn format_gc_run_json(run: &GarbageCollectionRunV1) -> anyhow::Result<String> {
    serde_json::to_string_pretty(run).context("failed to serialize garbage collection run")
}

fn format_lifecycle_status(status: Option<CheckpointLifecycleStatus>) -> &'static str {
    match status {
        Some(CheckpointLifecycleStatus::Published) => "published",
        None => "none",
    }
}

fn format_retention_status(record: Option<&CheckpointRetentionRecordV1>) -> String {
    record.map_or_else(
        || "none".to_string(),
        |record| {
            format!(
                "gc_run={} deleted={}",
                record.gc_run_id,
                record.deleted_candidate_keys.len()
            )
        },
    )
}

fn format_manifest_status(status: &CheckpointManifestInspectionStatus) -> String {
    match status {
        CheckpointManifestInspectionStatus::Valid => "valid".to_string(),
        CheckpointManifestInspectionStatus::Invalid { reason } => {
            format!("invalid reason={}", reason.replace('\n', " "))
        }
    }
}

fn run_benchmark_gate(
    baseline: Option<&Path>,
    result: &Path,
    gate_level: Option<BenchmarkGateLevel>,
    backend: Option<BenchmarkBackend>,
    max_regression_fraction: Option<f64>,
) -> anyhow::Result<BenchmarkGateEvidenceV1> {
    let current_result = read_benchmark_result(result)
        .with_context(|| format!("failed to validate benchmark result {}", result.display()))?;
    if let (Some(gate_level), Some(backend)) = (gate_level, backend) {
        current_result
            .expect_gate(gate_level, backend)
            .with_context(|| {
                format!(
                    "benchmark result {} does not match CLI gate",
                    result.display()
                )
            })?;
        current_result
            .require_workloads(benchmark_gate_workloads_for_backend(backend))
            .with_context(|| {
                format!(
                    "benchmark result {} is missing required workload metrics",
                    result.display()
                )
            })?;
        if has_placeholder_commit(&current_result) {
            bail!(
                "benchmark result {} must be real backend evidence, got placeholder commit {}",
                result.display(),
                current_result.commit
            );
        }
        require_explicit_s3_benchmark_evidence_scope(backend, result)?;
        reject_local_emulator_s3_benchmark(&current_result, result)?;
    }

    let Some(baseline) = baseline else {
        return Ok(BenchmarkGateEvidenceV1::validate_only(
            result,
            &current_result,
        ));
    };

    let baseline_result = read_benchmark_result(baseline).with_context(|| {
        format!(
            "failed to validate benchmark baseline {}",
            baseline.display()
        )
    })?;
    if let (Some(gate_level), Some(backend)) = (gate_level, backend) {
        baseline_result
            .expect_gate(gate_level, backend)
            .with_context(|| {
                format!(
                    "benchmark baseline {} does not match CLI gate",
                    baseline.display()
                )
            })?;
        baseline_result
            .require_workloads(benchmark_gate_workloads_for_backend(backend))
            .with_context(|| {
                format!(
                    "benchmark baseline {} is missing required workload metrics",
                    baseline.display()
                )
            })?;
        if has_placeholder_commit(&baseline_result) {
            bail!(
                "benchmark gate requires a real baseline, got placeholder commit {} in {}",
                baseline_result.commit,
                baseline.display()
            );
        }
        reject_local_emulator_s3_benchmark(&baseline_result, baseline)?;
    }
    let max_regression_fraction =
        max_regression_fraction.context("benchmark gate requires --max-regression-fraction")?;

    current_result
        .compare_against(
            &baseline_result,
            BenchmarkBudgetV1::relative(max_regression_fraction),
        )
        .context("benchmark result exceeds gate")?;

    Ok(BenchmarkGateEvidenceV1::passed(
        baseline,
        result,
        &baseline_result,
        &current_result,
        max_regression_fraction,
    ))
}

#[derive(Debug, PartialEq, Serialize)]
struct BenchmarkGateEvidenceV1 {
    schema_version: u16,
    status: &'static str,
    evidence_kind: &'static str,
    gate_level: BenchmarkGateLevel,
    backend: BenchmarkBackend,
    backend_evidence_scope: BenchmarkEvidenceScope,
    workload: String,
    baseline_path: Option<String>,
    result_path: String,
    baseline_commit: Option<String>,
    result_commit: String,
    max_regression_fraction: Option<f64>,
    workload_metrics: Vec<String>,
    deterministic_cost_compared_workloads: Vec<String>,
    performance_compared_workloads: Vec<String>,
    functional_shape_checked_workloads: Vec<String>,
}

impl BenchmarkGateEvidenceV1 {
    fn validate_only(result_path: &Path, result: &BenchmarkGateResultV1) -> Self {
        Self {
            schema_version: 1,
            status: "pass",
            evidence_kind: benchmark_gate_evidence_kind(result.backend),
            gate_level: result.gate_level,
            backend: result.backend,
            backend_evidence_scope: result.backend_evidence_scope,
            workload: result.workload.clone(),
            baseline_path: None,
            result_path: stable_path(result_path),
            baseline_commit: None,
            result_commit: result.commit.clone(),
            max_regression_fraction: None,
            workload_metrics: workload_metric_names(result),
            deterministic_cost_compared_workloads: Vec::new(),
            performance_compared_workloads: Vec::new(),
            functional_shape_checked_workloads: functional_shape_checked_workload_names(result),
        }
    }

    fn passed(
        baseline_path: &Path,
        result_path: &Path,
        baseline: &BenchmarkGateResultV1,
        result: &BenchmarkGateResultV1,
        max_regression_fraction: f64,
    ) -> Self {
        Self {
            schema_version: 1,
            status: "pass",
            evidence_kind: benchmark_gate_evidence_kind(result.backend),
            gate_level: result.gate_level,
            backend: result.backend,
            backend_evidence_scope: result.backend_evidence_scope,
            workload: result.workload.clone(),
            baseline_path: Some(stable_path(baseline_path)),
            result_path: stable_path(result_path),
            baseline_commit: Some(baseline.commit.clone()),
            result_commit: result.commit.clone(),
            max_regression_fraction: Some(max_regression_fraction),
            workload_metrics: workload_metric_names(result),
            deterministic_cost_compared_workloads: workload_metric_names(result),
            performance_compared_workloads: performance_compared_workload_names(result),
            functional_shape_checked_workloads: functional_shape_checked_workload_names(result),
        }
    }
}

fn benchmark_gate_evidence_kind(backend: BenchmarkBackend) -> &'static str {
    match backend {
        BenchmarkBackend::Local => "local_benchmark_gate",
        BenchmarkBackend::S3Compatible => "s3_compatible_benchmark_gate",
    }
}

fn workload_metric_names(result: &BenchmarkGateResultV1) -> Vec<String> {
    result
        .workload_metrics
        .iter()
        .map(|metrics| metrics.name.clone())
        .collect()
}

fn performance_compared_workload_names(result: &BenchmarkGateResultV1) -> Vec<String> {
    if !result.compares_wall_clock_metrics() {
        return Vec::new();
    }

    result
        .workload_metrics
        .iter()
        .filter(|metrics| metrics.name != OBJECT_STORE_CAPABILITY_PROBE_WORKLOAD)
        .map(|metrics| metrics.name.clone())
        .collect()
}

fn functional_shape_checked_workload_names(result: &BenchmarkGateResultV1) -> Vec<String> {
    result
        .workload_metrics
        .iter()
        .filter(|metrics| metrics.name == OBJECT_STORE_CAPABILITY_PROBE_WORKLOAD)
        .map(|metrics| metrics.name.clone())
        .collect()
}

fn benchmark_gate_workloads_for_backend(backend: BenchmarkBackend) -> &'static [&'static str] {
    match backend {
        BenchmarkBackend::Local => LOCAL_BENCHMARK_GATE_WORKLOADS,
        BenchmarkBackend::S3Compatible => S3_COMPATIBLE_BENCHMARK_GATE_WORKLOADS,
    }
}

fn reject_local_emulator_s3_benchmark(
    result: &BenchmarkGateResultV1,
    path: &Path,
) -> anyhow::Result<()> {
    result.reject_local_emulator_s3_evidence().with_context(|| {
        format!(
            "benchmark result {} is local emulator evidence and cannot satisfy S3-compatible benchmark gates",
            path.display()
        )
    })
}

fn require_explicit_s3_benchmark_evidence_scope(
    backend: BenchmarkBackend,
    path: &Path,
) -> anyhow::Result<()> {
    if backend != BenchmarkBackend::S3Compatible {
        return Ok(());
    }

    let value = read_benchmark_result_json_value(path)?;
    if value.get("backend_evidence_scope").is_none() {
        bail!(
            "benchmark result {} is missing backend_evidence_scope for S3-compatible gate evidence",
            path.display()
        );
    }

    Ok(())
}

fn read_benchmark_result_json_value(path: &Path) -> anyhow::Result<serde_json::Value> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read benchmark JSON from {}", path.display()))?;
    match serde_json::from_str(&contents) {
        Ok(value) => Ok(value),
        Err(full_error) => {
            let last_json_line = contents
                .lines()
                .rev()
                .find(|line| !line.trim().is_empty())
                .context("benchmark output is empty")?;
            serde_json::from_str(last_json_line)
                .with_context(|| format!("benchmark output is not valid JSON: {full_error}"))
        }
    }
}

fn has_placeholder_commit(result: &BenchmarkGateResultV1) -> bool {
    is_placeholder_commit(&result.commit)
}

fn parse_benchmark_gate_level(value: &str) -> Result<BenchmarkGateLevel, String> {
    match value {
        "pr-smoke" | "pr_smoke" => Ok(BenchmarkGateLevel::PrSmoke),
        "nightly-integration" | "nightly_integration" => Ok(BenchmarkGateLevel::NightlyIntegration),
        "release" => Ok(BenchmarkGateLevel::Release),
        _ => Err("expected pr-smoke, nightly-integration, or release".to_string()),
    }
}

fn parse_benchmark_backend(value: &str) -> Result<BenchmarkBackend, String> {
    match value {
        "local" => Ok(BenchmarkBackend::Local),
        "s3-compatible" | "s3_compatible" => Ok(BenchmarkBackend::S3Compatible),
        _ => Err("expected local or s3-compatible".to_string()),
    }
}

fn read_benchmark_result(path: &Path) -> anyhow::Result<BenchmarkGateResultV1> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read benchmark JSON from {}", path.display()))?;
    parse_benchmark_result_text(&contents)
}

fn parse_benchmark_result_text(contents: &str) -> anyhow::Result<BenchmarkGateResultV1> {
    match BenchmarkGateResultV1::from_json_str(contents) {
        Ok(result) => Ok(result),
        Err(full_error) => {
            let last_json_line = contents
                .lines()
                .rev()
                .find(|line| !line.trim().is_empty())
                .context("benchmark output is empty")?;
            BenchmarkGateResultV1::from_json_str(last_json_line)
                .with_context(|| format!("benchmark output is not valid JSON: {full_error}"))
        }
    }
}
