#![forbid(unsafe_code)]
#![recursion_limit = "256"]

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
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
use velorix_runtime::readiness::{
    verify_feldera_artifact_hash_evidence, verify_feldera_artifact_release_provenance_evidence,
    FelderaArtifactHashVerifiedEvidenceV1, FelderaArtifactReleaseProvenanceEvidenceV1,
    ProductionReadinessEvidenceV1, ProductionReadinessReportV1, ReadinessEvidenceKind,
    ReadinessStatus,
};

const OBJECT_STORE_CAPABILITY_PROBE_WORKLOAD: &str = "object_store_capability_probe";
const LOCAL_BENCHMARK_GATE_WORKLOADS: &[&str] = &[
    OBJECT_STORE_CAPABILITY_PROBE_WORKLOAD,
    "ingest_envelope_validation",
    "checkpoint_publish",
    "checkpoint_recovery",
    "datafusion_table_scan",
    "slatedb_state_reopen",
    "gc_dry_run_planning",
    "gc_execution_evidence",
];
const S3_COMPATIBLE_BENCHMARK_GATE_WORKLOADS: &[&str] = &[
    OBJECT_STORE_CAPABILITY_PROBE_WORKLOAD,
    "ingest_envelope_validation",
    "checkpoint_publish",
    "checkpoint_recovery",
    "datafusion_table_scan",
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
    "Feldera artifact registry",
    "benchmark gate",
    "S3-compatible tests",
    "Kubernetes operator",
    "GC",
    "dependency governance",
];
use velorix_runtime::recovery::{RecoveredRuntime, ORDERS_SUM_COUNT_OWNER};
use velorix_storage::{
    capability::{
        probe_authoritative_object_store_capabilities, AuthoritativeObjectStoreCapabilitiesV1,
    },
    checkpoint_index::{
        CheckpointAdminInspection, CheckpointAdminRepairReport, CheckpointLifecycleStatus,
        CheckpointManifestInspectionStatus, CheckpointRetentionRecordV1, LatestCandidateMarker,
    },
    gc::{GarbageCollectionPlan, GarbageCollectionPolicy, GarbageCollectionRunV1},
    log::{AppendValidatedEnvelopeOutcome, IngestBatchDescriptor},
    manifest::{CheckpointManifest, InputRange},
    relation_catalog_registry::RelationCatalogRegistry,
    state::{CheckpointPublisher, StateObjectWrite},
};

#[derive(Debug, Parser)]
#[command(name = "velorix-cli")]
#[command(about = "Local Velorix runtime utilities")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    RecoverLocal {
        #[arg(long)]
        object_store_dir: PathBuf,
        #[arg(long)]
        relation_id: String,
        #[arg(long)]
        relation_version: String,
        #[arg(
            long,
            help = "Open checkpoint state through this SlateDB database path"
        )]
        slatedb_state_path: Option<String>,
        #[arg(
            long,
            conflicts_with = "slatedb_state_path",
            help = "Permit bootstrap/migration recovery from legacy raw object state refs"
        )]
        allow_bootstrap_raw_state: bool,
        #[arg(
            long,
            help = "Start recovery from this published checkpoint, then replay later ingest"
        )]
        checkpoint_version: Option<u64>,
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
        feldera_artifact_hash_evidence: Option<PathBuf>,
        #[arg(long)]
        feldera_release_provenance_evidence: Option<PathBuf>,
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
        json: bool,
    },
    FelderaArtifactVerify {
        #[arg(long)]
        spec: PathBuf,
        #[arg(long)]
        metadata: PathBuf,
        #[arg(long = "artifact-package")]
        artifact_package: PathBuf,
        #[arg(long)]
        json: bool,
    },
    FelderaArtifactProvenanceVerify {
        #[arg(long)]
        metadata: PathBuf,
        #[arg(long)]
        provenance: PathBuf,
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
        Some(Command::RecoverLocal {
            object_store_dir,
            relation_id,
            relation_version,
            slatedb_state_path,
            allow_bootstrap_raw_state,
            checkpoint_version,
        }) => {
            let store = local_object_store(&object_store_dir)?;
            let recovered = recover_local_runtime(
                store,
                relation_id,
                relation_version,
                slatedb_state_path,
                checkpoint_version,
                allow_bootstrap_raw_state,
            )
            .await
            .context("failed to recover local runtime")?;
            let materialized_records = recovered.materialized_state().records().len();

            println!(
                "recovered checkpoint={:?} replayed_batches={} materialized_records={}",
                recovered.latest_checkpoint_version(),
                recovered.replayed_batch_count(),
                materialized_records
            );
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
            feldera_artifact_hash_evidence,
            feldera_release_provenance_evidence,
            s3_release_benchmark_gate_evidence,
            production_gc_run_evidence,
            rustfs_production_gc_validation_evidence,
            ingest_writer_lifecycle_evidence,
            standing_runtime_product_evidence,
            json,
        }) => {
            let artifacts = ReadinessReleaseArtifactPaths {
                require_release_artifacts,
                first_e2e_artifacts,
                dependency_governance_evidence,
                dependency_governance_manifest,
                release_commit,
                feldera_artifact_hash_evidence,
                feldera_release_provenance_evidence,
                s3_release_benchmark_gate_evidence,
                production_gc_run_evidence,
                rustfs_production_gc_validation_evidence,
                ingest_writer_lifecycle_evidence,
                standing_runtime_product_evidence,
            };
            let report = read_readiness_report(&evidence, &artifacts)?;
            if json {
                println!("{}", report.to_json_pretty()?);
            } else {
                print!("{}", format_readiness_report(&report));
            }
            ensure_readiness_report_passes(&report)?;
        }
        Some(Command::FelderaArtifactVerify {
            spec,
            metadata,
            artifact_package,
            json,
        }) => {
            let evidence =
                read_feldera_artifact_hash_verified_evidence(&spec, &metadata, &artifact_package)?;
            if json {
                println!("{}", format_feldera_artifact_evidence_json(&evidence)?);
            } else {
                println!(
                    "feldera_artifact_hash_verified artifact_id={} artifact_hash={}",
                    evidence.artifact_id, evidence.artifact_hash
                );
            }
        }
        Some(Command::FelderaArtifactProvenanceVerify {
            metadata,
            provenance,
            json,
        }) => {
            let evidence =
                read_feldera_artifact_release_provenance_evidence(&metadata, &provenance)?;
            if json {
                println!(
                    "{}",
                    format_feldera_artifact_release_provenance_evidence_json(&evidence)?
                );
            } else {
                println!(
                    "feldera_artifact_release_provenance release_id={} artifact_id={}",
                    evidence.release_id, evidence.artifact_id
                );
            }
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

#[derive(Debug, Default)]
struct ReadinessReleaseArtifactPaths {
    require_release_artifacts: bool,
    first_e2e_artifacts: bool,
    dependency_governance_evidence: Option<PathBuf>,
    dependency_governance_manifest: Option<PathBuf>,
    release_commit: Option<String>,
    feldera_artifact_hash_evidence: Option<PathBuf>,
    feldera_release_provenance_evidence: Option<PathBuf>,
    s3_release_benchmark_gate_evidence: Option<PathBuf>,
    production_gc_run_evidence: Option<PathBuf>,
    rustfs_production_gc_validation_evidence: Option<PathBuf>,
    ingest_writer_lifecycle_evidence: Option<PathBuf>,
    standing_runtime_product_evidence: Option<PathBuf>,
}

impl ReadinessReleaseArtifactPaths {
    fn any_path_supplied(&self) -> bool {
        self.dependency_governance_evidence.is_some()
            || self.dependency_governance_manifest.is_some()
            || self.release_commit.is_some()
            || self.feldera_artifact_hash_evidence.is_some()
            || self.feldera_release_provenance_evidence.is_some()
            || self.s3_release_benchmark_gate_evidence.is_some()
            || self.production_gc_run_evidence.is_some()
            || self.rustfs_production_gc_validation_evidence.is_some()
            || self.ingest_writer_lifecycle_evidence.is_some()
            || self.standing_runtime_product_evidence.is_some()
    }
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
        bail!("readiness-report cannot combine --require-release-artifacts with --first-e2e-artifacts");
    }

    if artifacts.require_release_artifacts {
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
    } else if !artifacts.any_path_supplied() {
        return Ok(());
    }

    if let Some(path) = &artifacts.dependency_governance_evidence {
        validate_dependency_governance_evidence_artifact(
            path,
            artifacts.release_commit.as_deref(),
            artifacts.dependency_governance_manifest.as_deref(),
        )?;
    }
    if let Some(hash_path) = &artifacts.feldera_artifact_hash_evidence {
        validate_feldera_hash_evidence_artifact(hash_path)?;
    }
    if let Some(provenance_path) = &artifacts.feldera_release_provenance_evidence {
        let Some(hash_path) = &artifacts.feldera_artifact_hash_evidence else {
            bail!("Feldera release provenance evidence requires --feldera-artifact-hash-evidence");
        };
        validate_feldera_release_evidence_artifacts(
            hash_path,
            provenance_path,
            artifacts.release_commit.as_deref(),
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
        let mode = if artifacts.require_release_artifacts {
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
    if artifacts.require_release_artifacts {
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

    Ok(())
}

fn require_artifact_path(name: &str, path: &Option<PathBuf>) -> anyhow::Result<()> {
    if path.is_some() {
        Ok(())
    } else {
        bail!("readiness-report --require-release-artifacts requires --{name}")
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

fn validate_feldera_hash_evidence_artifact(hash_path: &Path) -> anyhow::Result<()> {
    let hash: FelderaArtifactHashVerifiedEvidenceV1 = read_json_artifact(hash_path)?;

    if hash.schema_version != 1 {
        bail!(
            "{} has unsupported schema_version {}, expected 1",
            hash_path.display(),
            hash.schema_version
        );
    }
    if hash.evidence_kind != ReadinessEvidenceKind::FelderaArtifactHashVerified {
        bail!(
            "{} has evidence_kind {:?}, expected feldera_artifact_hash_verified",
            hash_path.display(),
            hash.evidence_kind
        );
    }
    if hash.status != ReadinessStatus::Pass {
        bail!(
            "{} Feldera artifact hash evidence is not pass",
            hash_path.display()
        );
    }

    Ok(())
}

fn validate_feldera_release_evidence_artifacts(
    hash_path: &Path,
    provenance_path: &Path,
    release_commit: Option<&str>,
) -> anyhow::Result<()> {
    let hash: FelderaArtifactHashVerifiedEvidenceV1 = read_json_artifact(hash_path)?;
    let provenance: FelderaArtifactReleaseProvenanceEvidenceV1 =
        read_json_artifact(provenance_path)?;

    validate_feldera_hash_evidence_artifact(hash_path)?;
    if provenance.schema_version != 1 {
        bail!(
            "{} has unsupported schema_version {}, expected 1",
            provenance_path.display(),
            provenance.schema_version
        );
    }
    if provenance.evidence_kind != ReadinessEvidenceKind::FelderaArtifactReleaseProvenance {
        bail!(
            "{} has evidence_kind {:?}, expected feldera_artifact_release_provenance",
            provenance_path.display(),
            provenance.evidence_kind
        );
    }
    if provenance.status != ReadinessStatus::Pass {
        bail!(
            "{} Feldera release provenance evidence is not pass",
            provenance_path.display()
        );
    }
    if hash.artifact_id != provenance.artifact_id
        || hash.artifact_hash != provenance.artifact_hash
        || hash.spec_hash != provenance.spec_hash
        || hash.generated_rust_abi_version != provenance.generated_rust_abi_version
    {
        bail!("Feldera hash and release provenance evidence do not describe the same artifact");
    }
    let source_revision = provenance.source_revision.trim();
    if is_placeholder_commit(source_revision) {
        bail!(
            "{} Feldera release provenance uses blank or placeholder source_revision",
            provenance_path.display()
        );
    }
    if let Some(release_commit) = release_commit
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if source_revision != release_commit {
            bail!(
                "{} Feldera release provenance source_revision does not match release commit",
                provenance_path.display()
            );
        }
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
    require_json_false(path, &artifact, "/api/generic_query_enabled")?;
    require_json_false(path, &artifact, "/api/legacy_recovered_sql_views_allowed")?;
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
    require_json_true(path, &artifact, "/api/compile_deploy/job_catalog_verified")?;
    if require_json_str(
        path,
        &artifact,
        "/api/compile_deploy/job_catalog_evidence_file",
    )? != "view-compile-deploy-jobs.json"
    {
        bail!(
            "{} product evidence must attach compile/deploy job catalog evidence",
            path.display()
        );
    }
    require_sibling_evidence_file(
        path,
        "view-compile-deploy-jobs.json",
        "product compile/deploy job evidence",
    )?;
    validate_product_compile_deploy_job_catalog_sibling_evidence(path, &artifact)?;
    if require_json_str(path, &artifact, "/api/compile_deploy/pending_view_id")?
        != "pending_scores_by_user"
    {
        bail!(
            "{} product evidence compile/deploy smoke did not use pending_scores_by_user",
            path.display()
        );
    }
    require_json_true(
        path,
        &artifact,
        "/api/compile_deploy/compiler_request_embedded",
    )?;
    if require_json_str(path, &artifact, "/api/compile_deploy/admin_route")?
        != "/v1/view-compile-deploy/jobs"
    {
        bail!(
            "{} product evidence compile/deploy admin route did not match /v1/view-compile-deploy/jobs",
            path.display()
        );
    }
    require_json_true(path, &artifact, "/api/compile_deploy/worker_run_verified")?;
    if require_json_str(path, &artifact, "/api/compile_deploy/run_once_admin_route")?
        != "/v1/view-compile-deploy/run-once"
    {
        bail!(
            "{} product evidence compile/deploy run-once admin route did not match /v1/view-compile-deploy/run-once",
            path.display()
        );
    }
    if require_json_str(
        path,
        &artifact,
        "/api/compile_deploy/run_once_evidence_file",
    )? != "view-compile-deploy-run-once.json"
    {
        bail!(
            "{} product evidence must attach compile/deploy run-once evidence",
            path.display()
        );
    }
    require_sibling_evidence_file(
        path,
        "view-compile-deploy-run-once.json",
        "product compile/deploy run-once evidence",
    )?;
    if require_json_str(path, &artifact, "/api/compile_deploy/activated_view_id")?
        != "pending_scores_by_user"
    {
        bail!(
            "{} product evidence compile/deploy worker did not activate pending_scores_by_user",
            path.display()
        );
    }
    if require_json_str(
        path,
        &artifact,
        "/api/compile_deploy/activated_execution_mode",
    )? != "standing_runtime"
    {
        bail!(
            "{} product evidence compile/deploy worker did not promote the view to standing_runtime",
            path.display()
        );
    }
    for (pointer, expected, label) in [
        (
            "/api/compile_deploy/activated_view_evidence_file",
            "pending-scores-view-after-compile-deploy.json",
            "product compile/deploy activated-view evidence",
        ),
        (
            "/api/compile_deploy/activated_query_evidence_file",
            "pending-scores-query-after-compile-deploy.json",
            "product compile/deploy activated-query evidence",
        ),
    ] {
        if require_json_str(path, &artifact, pointer)? != expected {
            bail!(
                "{} product evidence compile/deploy evidence file {pointer} did not match {expected}",
                path.display()
            );
        }
        require_sibling_evidence_file(path, expected, label)?;
    }
    validate_product_compile_deploy_activation_sibling_evidence(path)?;
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
        require_json_u64(&failover_path, &failover, "/observed_failover_ms")?;
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
    if !result.success {
        bail!(
            "{} Hiqlite backend-time trusted provenance Sigstore bundle verification failed",
            path.display()
        );
    }
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

fn validate_product_compile_deploy_activation_sibling_evidence(path: &Path) -> anyhow::Result<()> {
    let run_once = read_sibling_json_artifact(
        path,
        "view-compile-deploy-run-once.json",
        "product compile/deploy run-once evidence",
    )?;
    if require_json_u64(path, &run_once, "/pending_jobs")? != 1 {
        bail!(
            "{} product compile/deploy run-once evidence must have pending_jobs=1",
            path.display()
        );
    }
    if require_json_u64(path, &run_once, "/activated")? != 1 {
        bail!(
            "{} product compile/deploy run-once evidence must have activated=1",
            path.display()
        );
    }
    if require_json_u64(path, &run_once, "/failed")? != 0 {
        bail!(
            "{} product compile/deploy run-once evidence must have failed=0",
            path.display()
        );
    }
    let outcomes = run_once
        .pointer("/outcomes")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} product compile/deploy run-once evidence missing outcomes",
                path.display()
            )
        })?;
    if outcomes.len() != 1 {
        bail!(
            "{} product compile/deploy run-once evidence must have exactly one outcome",
            path.display()
        );
    }
    let outcome = &outcomes[0];
    if require_json_str(path, outcome, "/view_id")? != "pending_scores_by_user" {
        bail!(
            "{} product compile/deploy run-once evidence activated unexpected view",
            path.display()
        );
    }
    if require_json_str(path, outcome, "/status")? != "activated" {
        bail!(
            "{} product compile/deploy run-once evidence did not activate the pending view",
            path.display()
        );
    }

    let activated_view = read_sibling_json_artifact(
        path,
        "pending-scores-view-after-compile-deploy.json",
        "product compile/deploy activated-view evidence",
    )?;
    if require_json_str(path, &activated_view, "/view_id")? != "pending_scores_by_user" {
        bail!(
            "{} product compile/deploy activated-view evidence has wrong view_id",
            path.display()
        );
    }
    if require_json_str(path, &activated_view, "/execution_mode")? != "standing_runtime" {
        bail!(
            "{} product compile/deploy activated-view evidence did not reach standing_runtime",
            path.display()
        );
    }
    require_json_true(path, &activated_view, "/query_enabled")?;
    if require_json_str(path, &activated_view, "/lifecycle/compile_status")? != "success" {
        bail!(
            "{} product compile/deploy activated-view evidence compile_status is not success",
            path.display()
        );
    }
    if require_json_str(path, &activated_view, "/lifecycle/deployment_status")? != "running" {
        bail!(
            "{} product compile/deploy activated-view evidence deployment_status is not running",
            path.display()
        );
    }
    if activated_view.pointer("/compile_job_id").is_some() {
        bail!(
            "{} product compile/deploy activated-view evidence must not expose compile_job_id",
            path.display()
        );
    }

    let activated_query = read_sibling_json_artifact(
        path,
        "pending-scores-query-after-compile-deploy.json",
        "product compile/deploy activated-query evidence",
    )?;
    require_rows_match(
        path,
        &activated_query,
        &[("u1", serde_json::json!(12), 2)],
        "product compile/deploy activated-query evidence",
    )?;

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
    if !spec_hash.starts_with("velorix-feldera-spec-sha256-v1:") {
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

fn validate_product_compile_deploy_job_catalog_sibling_evidence(
    path: &Path,
    artifact: &serde_json::Value,
) -> anyhow::Result<()> {
    let filename = "view-compile-deploy-jobs.json";
    let label = "product compile/deploy job catalog evidence";
    let expected_view_id = require_json_str(path, artifact, "/api/compile_deploy/pending_view_id")?;
    let catalog = read_sibling_json_artifact(path, filename, label)?;
    if require_sibling_json_u64(path, filename, &catalog, "/pending_jobs", label)? != 1 {
        bail!(
            "{} {label} sibling {filename} must have pending_jobs=1",
            path.display()
        );
    }
    let jobs = catalog
        .pointer("/jobs")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} missing jobs array",
                path.display()
            )
        })?;
    if jobs.len() != 1 {
        bail!(
            "{} {label} sibling {filename} must contain exactly one job",
            path.display()
        );
    }
    let job = &jobs[0];
    require_sibling_json_str_eq(path, filename, job, "/view_id", expected_view_id, label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        job,
        "/compiler_backend",
        "feldera_compiler",
        label,
    )?;
    require_sibling_json_str_eq(path, filename, job, "/compile_status", "pending", label)?;
    require_sibling_json_str_eq(
        path,
        filename,
        job,
        "/deployment_status",
        "not_deployed",
        label,
    )?;
    let spec_hash = require_sibling_json_str(path, filename, job, "/spec_hash", label)?;
    if !spec_hash.starts_with("velorix-feldera-spec-sha256-v1:") {
        bail!(
            "{} {label} sibling {filename} has unexpected spec_hash prefix",
            path.display()
        );
    }
    let job_id = require_sibling_json_str(path, filename, job, "/job_id", label)?;
    if !job_id.contains(expected_view_id) || !job_id.contains(spec_hash) {
        bail!(
            "{} {label} sibling {filename} job_id must bind the pending view and spec_hash",
            path.display()
        );
    }

    let request = job.pointer("/compiler_request").with_context(|| {
        format!(
            "{} {label} sibling {filename} missing compiler_request",
            path.display()
        )
    })?;
    require_sibling_json_str_eq(
        path,
        filename,
        request,
        "/request_kind",
        "feldera_standing_view_compile_request_v1",
        label,
    )?;
    require_sibling_json_str_eq(path, filename, request, "/view_id", expected_view_id, label)?;
    require_sibling_json_str_eq(path, filename, request, "/spec_hash", spec_hash, label)?;
    require_sibling_json_true(path, filename, request, "/shape/is_materialized", label)?;
    require_sibling_json_bool_eq(path, filename, request, "/shape/multi_input", false, label)?;
    require_sibling_json_bool_eq(path, filename, request, "/shape/multi_output", false, label)?;

    let sql = require_sibling_json_str(path, filename, request, "/sql", label)?;
    let sql_lower = sql.to_ascii_lowercase();
    let sql_compact = sql_lower.split_whitespace().collect::<String>();
    if !sql_lower.contains("from scores")
        || !sql_compact.contains("sum(score)")
        || !sql_compact.contains("groupbyuser_id")
    {
        bail!(
            "{} {label} sibling {filename} compiler_request sql does not prove scores aggregation semantics",
            path.display()
        );
    }

    require_relation_column_kind(
        path,
        filename,
        request,
        "/input_relations",
        "scores",
        &[("user_id", "utf8"), ("score", "int64"), ("delta", "int64")],
        label,
    )?;
    require_relation_column_kind(
        path,
        filename,
        request,
        "/output_relations",
        expected_view_id,
        &[("user_id", "utf8"), ("sum", "int64"), ("count", "int64")],
        label,
    )?;

    Ok(())
}

fn require_sibling_json_bool_eq(
    path: &Path,
    filename: &str,
    value: &serde_json::Value,
    pointer: &str,
    expected: bool,
    label: &str,
) -> anyhow::Result<()> {
    match value.pointer(pointer).and_then(serde_json::Value::as_bool) {
        Some(actual) if actual == expected => Ok(()),
        Some(actual) => bail!(
            "{} {label} sibling {filename} requires {pointer}={expected}, got {actual}",
            path.display()
        ),
        None => bail!(
            "{} {label} sibling {filename} missing boolean {pointer}",
            path.display()
        ),
    }
}

fn require_relation_column_kind(
    path: &Path,
    filename: &str,
    value: &serde_json::Value,
    relations_pointer: &str,
    relation_id: &str,
    expected_columns: &[(&str, &str)],
    label: &str,
) -> anyhow::Result<()> {
    let relations = value
        .pointer(relations_pointer)
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} missing array {relations_pointer}",
                path.display()
            )
        })?;
    let relation = relations
        .iter()
        .find(|relation| {
            relation
                .get("relation_id")
                .and_then(serde_json::Value::as_str)
                == Some(relation_id)
        })
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} missing relation {relation_id}",
                path.display()
            )
        })?;
    let columns = relation
        .pointer("/columns")
        .and_then(serde_json::Value::as_array)
        .with_context(|| {
            format!(
                "{} {label} sibling {filename} relation {relation_id} missing columns",
                path.display()
            )
        })?;
    for (column_name, expected_kind) in expected_columns {
        let column = columns
            .iter()
            .find(|column| {
                column.get("name").and_then(serde_json::Value::as_str) == Some(*column_name)
            })
            .with_context(|| {
                format!(
                    "{} {label} sibling {filename} relation {relation_id} missing column {column_name}",
                    path.display()
                )
            })?;
        let actual_kind = column
            .pointer("/data_type/kind")
            .and_then(serde_json::Value::as_str)
            .with_context(|| {
                format!(
                    "{} {label} sibling {filename} relation {relation_id} column {column_name} missing data_type.kind",
                    path.display()
                )
            })?;
        if actual_kind != *expected_kind {
            bail!(
                "{} {label} sibling {filename} relation {relation_id} column {column_name} requires kind {expected_kind}, got {actual_kind}",
                path.display()
            );
        }
    }

    Ok(())
}

fn require_rows_match(
    path: &Path,
    artifact: &serde_json::Value,
    expected: &[(&str, serde_json::Value, u64)],
    label: &str,
) -> anyhow::Result<()> {
    let rows = artifact
        .pointer("/rows")
        .and_then(serde_json::Value::as_array)
        .with_context(|| format!("{} {label} missing rows", path.display()))?;
    let mut actual = Vec::with_capacity(rows.len());
    for row in rows {
        let user_id = row
            .get("user_id")
            .and_then(serde_json::Value::as_str)
            .with_context(|| format!("{} {label} has row without user_id", path.display()))?;
        let sum = row
            .get("sum")
            .cloned()
            .with_context(|| format!("{} {label} has row without sum", path.display()))?;
        let count = row
            .get("count")
            .and_then(serde_json::Value::as_u64)
            .with_context(|| format!("{} {label} has row without count", path.display()))?;
        actual.push((user_id.to_string(), sum, count));
    }
    actual.sort_by(|left, right| left.0.cmp(&right.0));
    let mut expected_rows = expected
        .iter()
        .map(|(user_id, sum, count)| ((*user_id).to_string(), sum.clone(), *count))
        .collect::<Vec<_>>();
    expected_rows.sort_by(|left, right| left.0.cmp(&right.0));
    if actual != expected_rows {
        bail!(
            "{} {label} rows did not match expected compile/deploy output",
            path.display()
        );
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
    if hex.len() != 64 || !hex.chars().all(|ch| ch.is_ascii_hexdigit()) {
        bail!(
            "{} {label} must be a sha256 digest with 64 hex characters",
            path.display()
        );
    }
    Ok(())
}

fn validate_full_git_commit_sha(path: &Path, value: &str, label: &str) -> anyhow::Result<()> {
    let value = value.trim();
    if value.len() != 40 || !value.chars().all(|ch| ch.is_ascii_hexdigit()) {
        bail!(
            "{} {label} must be a full 40-character git commit SHA",
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
    candidates: &[velorix_storage::gc::GarbageCollectionCandidate],
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

fn read_feldera_artifact_hash_verified_evidence(
    spec: &Path,
    metadata: &Path,
    artifact_package: &Path,
) -> anyhow::Result<FelderaArtifactHashVerifiedEvidenceV1> {
    let spec_json = fs::read_to_string(spec)
        .with_context(|| format!("failed to read Feldera spec from {}", spec.display()))?;
    let metadata_json = fs::read_to_string(metadata).with_context(|| {
        format!(
            "failed to read Feldera artifact metadata from {}",
            metadata.display()
        )
    })?;
    let artifact_bytes = fs::read(artifact_package).with_context(|| {
        format!(
            "failed to read Feldera artifact package from {}",
            artifact_package.display()
        )
    })?;

    verify_feldera_artifact_hash_evidence(&spec_json, &metadata_json, &artifact_bytes)
        .map_err(anyhow::Error::msg)
}

fn read_feldera_artifact_release_provenance_evidence(
    metadata: &Path,
    provenance: &Path,
) -> anyhow::Result<FelderaArtifactReleaseProvenanceEvidenceV1> {
    let metadata_json = fs::read_to_string(metadata).with_context(|| {
        format!(
            "failed to read Feldera artifact metadata from {}",
            metadata.display()
        )
    })?;
    let provenance_json = fs::read_to_string(provenance).with_context(|| {
        format!(
            "failed to read Feldera release artifact provenance from {}",
            provenance.display()
        )
    })?;

    verify_feldera_artifact_release_provenance_evidence(&metadata_json, &provenance_json)
        .map_err(anyhow::Error::msg)
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

fn format_feldera_artifact_evidence_json(
    evidence: &FelderaArtifactHashVerifiedEvidenceV1,
) -> anyhow::Result<String> {
    evidence
        .to_json_pretty()
        .context("failed to serialize Feldera artifact evidence")
}

fn format_feldera_artifact_release_provenance_evidence_json(
    evidence: &FelderaArtifactReleaseProvenanceEvidenceV1,
) -> anyhow::Result<String> {
    evidence
        .to_json_pretty()
        .context("failed to serialize Feldera release provenance evidence")
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

#[cfg(test)]
fn validate_dependency_governance_manifest_text(contents: &str, today: &str) -> anyhow::Result<()> {
    let manifest: DependencyGovernanceManifestV1 =
        serde_json::from_str(contents).context("failed to parse dependency governance JSON")?;
    manifest.validate(today)
}

#[cfg(test)]
fn validate_dependency_governance_with_diagnostics_text(
    manifest_contents: &str,
    diagnostics_contents: &str,
    today: &str,
) -> anyhow::Result<()> {
    let manifest: DependencyGovernanceManifestV1 = serde_json::from_str(manifest_contents)
        .context("failed to parse dependency governance JSON")?;
    manifest.validate(today)?;

    compare_dependency_governance_diagnostics(&manifest, diagnostics_contents)?;

    Ok(())
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
}

impl DependencyGovernanceExceptionKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Duplicate => "duplicate",
            Self::Unmaintained => "unmaintained",
            Self::Advisory => "advisory",
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
    "feldera_artifacts",
];

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
enum CargoDenyWarningKind {
    Duplicate,
    Unmaintained,
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
            DependencyGovernanceExceptionKind::Advisory => None,
        }
    }

    fn from_diagnostic_code(code: &str) -> Option<Self> {
        match code {
            "duplicate" => Some(Self::Duplicate),
            "unmaintained" => Some(Self::Unmaintained),
            _ => None,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Duplicate => "duplicate",
            Self::Unmaintained => "unmaintained",
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

async fn recover_local_runtime(
    store: Arc<dyn ObjectStore>,
    relation_id: String,
    relation_version: String,
    slatedb_state_path: Option<String>,
    checkpoint_version: Option<u64>,
    allow_bootstrap_raw_state: bool,
) -> anyhow::Result<RecoveredRuntime> {
    if slatedb_state_path.is_some() && allow_bootstrap_raw_state {
        bail!(
            "recover-local --allow-bootstrap-raw-state cannot be combined with \
             --slatedb-state-path"
        );
    }
    if slatedb_state_path.is_none() && !allow_bootstrap_raw_state {
        bail!(
            "recover-local raw object state recovery requires --allow-bootstrap-raw-state when \
             --slatedb-state-path is omitted"
        );
    }

    match (slatedb_state_path, checkpoint_version) {
        (None, None) => Ok(
            RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
                store,
                ORDERS_SUM_COUNT_OWNER,
                &relation_id,
                &relation_version,
            )
            .await?,
        ),
        (Some(db_path), Some(checkpoint_version)) => {
            let capabilities = recover_local_capabilities(store.as_ref()).await?;
            Ok(RecoveredRuntime::recover_from_published_checkpoint_version_with_slatedb_state_store_and_relation_catalog_record_checked(
                store,
                db_path,
                checkpoint_version,
                ORDERS_SUM_COUNT_OWNER,
                &relation_id,
                &relation_version,
                &capabilities,
            )
            .await?)
        }
        (Some(db_path), None) => {
            let capabilities = recover_local_capabilities(store.as_ref()).await?;
            Ok(
                RecoveredRuntime::recover_with_slatedb_state_store_and_catalog_record_checked(
                    store,
                    db_path,
                    ORDERS_SUM_COUNT_OWNER,
                    &relation_id,
                    &relation_version,
                    &capabilities,
                )
                .await?,
            )
        }
        (None, Some(checkpoint_version)) => {
            let capabilities = recover_local_capabilities(store.as_ref()).await?;
            let relation_catalog = RelationCatalogRegistry::new_checked(
                Arc::clone(&store),
                capabilities
                    .validate_namespace(
                        velorix_storage::capability::AuthoritativeNamespace::RelationCatalog,
                    )
                    .map_err(anyhow::Error::from)?,
            )
            .map_err(anyhow::Error::from)?
            .read(&relation_id, &relation_version)
            .await?;
            Ok(RecoveredRuntime::recover_from_published_checkpoint_version_with_owner_and_relation_catalog_checked(
                store,
                checkpoint_version,
                ORDERS_SUM_COUNT_OWNER,
                relation_catalog,
                &capabilities,
            )
            .await?)
        }
    }
}

async fn recover_local_capabilities(
    store: &dyn ObjectStore,
) -> anyhow::Result<velorix_storage::capability::AuthoritativeObjectStoreCapabilitiesV1> {
    Ok(probe_authoritative_object_store_capabilities(
        store,
        "recover-local",
        "recover-local-capability-probes",
    )
    .await?)
}

async fn checked_local_admin_checkpoint_publisher(
    store: Arc<dyn ObjectStore>,
) -> anyhow::Result<CheckpointPublisher> {
    let capabilities = recover_local_capabilities(store.as_ref()).await?;
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
            performance_compared_workloads: performance_compared_workload_names(result),
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

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use super::*;
    use arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use bytes::Bytes;
    use object_store::path::Path as ObjectStorePath;
    use ring::signature::KeyPair as _;
    use tempfile::tempdir;
    use velorix_runtime::recovery::{orders_sum_count_relation_catalog, RecoveryError};
    use velorix_storage::ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest};
    use velorix_storage::{
        checkpoint_index::{
            CheckpointAdminInspection, CheckpointGcTransitionRecordV1, CheckpointLifecycleRecord,
            CheckpointLifecycleStatus, CheckpointManifestInspection,
            CheckpointManifestInspectionStatus, CheckpointRecoveryMode,
            CheckpointRecoveryTransitionRecordV1,
        },
        gc::{GarbageCollectionCandidate, GarbageCollectionCandidateKind, GarbageCollectionReport},
        manifest::{CheckpointManifest, InputRange},
        object_key::ObjectKey,
        relation_catalog_registry::RelationCatalogRegistryError,
        state::{CheckpointPublisher, StateObjectWrite},
    };

    const TEST_RELEASE_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn benchmark_result_parser_accepts_pretty_json() {
        parse_benchmark_result_text(&valid_result_json()).unwrap();
    }

    #[test]
    fn benchmark_result_parser_accepts_jsonl_last_line() {
        let jsonl = valid_result_json_compact();
        parse_benchmark_result_text(&format!("ignored status line\n{jsonl}\n")).unwrap();
    }

    #[test]
    fn benchmark_gate_comparison_rejects_mismatched_backend() {
        let baseline = parse_benchmark_result_text(&valid_result_json()).unwrap();
        let current = parse_benchmark_result_text(&s3_result_json()).unwrap();

        let error = current
            .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
            .unwrap_err();

        assert!(error.to_string().contains("baseline mismatch"));
    }

    #[test]
    fn benchmark_gate_validate_only_accepts_valid_result() {
        let dir = tempdir().unwrap();
        let result = dir.path().join("result.json");
        fs::write(&result, valid_result_json()).unwrap();

        run_benchmark_gate(None, &result, None, None, None).unwrap();
    }

    #[test]
    fn benchmark_gate_validate_only_requires_local_gc_execution_evidence() {
        let dir = tempdir().unwrap();
        let result = dir.path().join("result.json");
        fs::write(&result, missing_gc_execution_workload_json()).unwrap();

        let error = run_benchmark_gate(
            None,
            &result,
            Some(BenchmarkGateLevel::PrSmoke),
            Some(BenchmarkBackend::Local),
            None,
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("gc_execution_evidence"));
    }

    #[test]
    fn benchmark_gate_validate_only_accepts_s3_without_local_gc_execution_evidence() {
        let dir = tempdir().unwrap();
        let result = dir.path().join("result.json");
        fs::write(&result, s3_result_json()).unwrap();

        run_benchmark_gate(
            None,
            &result,
            Some(BenchmarkGateLevel::PrSmoke),
            Some(BenchmarkBackend::S3Compatible),
            None,
        )
        .unwrap();
    }

    #[test]
    fn benchmark_gate_cli_requires_gate_level_and_backend() {
        let error = Cli::try_parse_from([
            "velorix-cli",
            "benchmark-gate",
            "--baseline",
            "baseline.json",
            "--result",
            "result.json",
        ])
        .unwrap_err();

        assert!(error.to_string().contains("--gate-level"));
        assert!(error.to_string().contains("--backend"));
    }

    #[test]
    fn benchmark_gate_cli_parses_expected_gate_level_and_backend() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "benchmark-gate",
            "--gate-level",
            "pr-smoke",
            "--backend",
            "local",
            "--baseline",
            "baseline.json",
            "--result",
            "result.json",
            "--json",
        ])
        .unwrap();

        let Some(Command::BenchmarkGate {
            gate_level,
            backend,
            json,
            ..
        }) = cli.command
        else {
            panic!("expected benchmark-gate command");
        };

        assert_eq!(gate_level, BenchmarkGateLevel::PrSmoke);
        assert_eq!(backend, BenchmarkBackend::Local);
        assert!(json);
    }

    #[test]
    fn benchmark_gate_outputs_stable_gate_evidence() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, valid_result_json()).unwrap();
        fs::write(&result, valid_result_json()).unwrap();

        let evidence = run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::PrSmoke),
            Some(BenchmarkBackend::Local),
            Some(0.10),
        )
        .unwrap();

        assert_eq!(evidence.schema_version, 1);
        assert_eq!(evidence.status, "pass");
        assert_eq!(evidence.evidence_kind, "local_benchmark_gate");
        assert_eq!(evidence.gate_level, BenchmarkGateLevel::PrSmoke);
        assert_eq!(evidence.backend, BenchmarkBackend::Local);
        assert_eq!(evidence.workload, "local_incremental");
        assert_eq!(
            evidence.baseline_path.as_deref(),
            Some(baseline.to_str().unwrap())
        );
        assert_eq!(evidence.result_path, result.to_str().unwrap());
        assert_eq!(evidence.baseline_commit.as_deref(), Some("abc123"));
        assert_eq!(evidence.result_commit, "abc123");
        assert_eq!(evidence.max_regression_fraction, Some(0.10));
        assert_eq!(
            evidence.workload_metrics,
            vec![
                "object_store_capability_probe",
                "ingest_envelope_validation",
                "checkpoint_publish",
                "checkpoint_recovery",
                "datafusion_table_scan",
                "slatedb_state_reopen",
                "gc_dry_run_planning",
                "gc_execution_evidence"
            ]
        );
        assert_eq!(
            evidence.performance_compared_workloads,
            vec![
                "ingest_envelope_validation",
                "checkpoint_publish",
                "checkpoint_recovery",
                "datafusion_table_scan",
                "slatedb_state_reopen",
                "gc_dry_run_planning",
                "gc_execution_evidence"
            ]
        );
        assert_eq!(
            evidence.functional_shape_checked_workloads,
            vec!["object_store_capability_probe"]
        );
    }

    #[test]
    fn recover_local_cli_parses_selected_checkpoint_version() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "recover-local",
            "--object-store-dir",
            "/tmp/velorix",
            "--relation-id",
            "orders",
            "--relation-version",
            "2026-05-05.v1",
            "--checkpoint-version",
            "7",
        ])
        .unwrap();

        let Some(Command::RecoverLocal {
            object_store_dir,
            relation_id,
            relation_version,
            slatedb_state_path,
            allow_bootstrap_raw_state,
            checkpoint_version,
        }) = cli.command
        else {
            panic!("expected recover-local command");
        };

        assert_eq!(object_store_dir, PathBuf::from("/tmp/velorix"));
        assert_eq!(relation_id, "orders");
        assert_eq!(relation_version, "2026-05-05.v1");
        assert_eq!(slatedb_state_path, None);
        assert!(!allow_bootstrap_raw_state);
        assert_eq!(checkpoint_version, Some(7));
    }

    #[test]
    fn recover_local_cli_parses_slatedb_state_path() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "recover-local",
            "--object-store-dir",
            "/tmp/velorix",
            "--relation-id",
            "orders",
            "--relation-version",
            "2026-05-05.v1",
            "--slatedb-state-path",
            "v1/slatedb/state",
            "--checkpoint-version",
            "7",
        ])
        .unwrap();

        let Some(Command::RecoverLocal {
            object_store_dir,
            relation_id,
            relation_version,
            slatedb_state_path,
            allow_bootstrap_raw_state,
            checkpoint_version,
        }) = cli.command
        else {
            panic!("expected recover-local command");
        };

        assert_eq!(object_store_dir, PathBuf::from("/tmp/velorix"));
        assert_eq!(relation_id, "orders");
        assert_eq!(relation_version, "2026-05-05.v1");
        assert_eq!(slatedb_state_path, Some("v1/slatedb/state".to_string()));
        assert!(!allow_bootstrap_raw_state);
        assert_eq!(checkpoint_version, Some(7));
    }

    #[test]
    fn recover_local_cli_parses_bootstrap_raw_state_flag() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "recover-local",
            "--object-store-dir",
            "/tmp/velorix",
            "--relation-id",
            "orders",
            "--relation-version",
            "2026-05-05.v1",
            "--allow-bootstrap-raw-state",
        ])
        .unwrap();

        let Some(Command::RecoverLocal {
            allow_bootstrap_raw_state,
            ..
        }) = cli.command
        else {
            panic!("expected recover-local command");
        };

        assert!(allow_bootstrap_raw_state);
    }

    #[test]
    fn recover_local_cli_rejects_bootstrap_raw_state_with_slatedb_state_path() {
        let error = Cli::try_parse_from([
            "velorix-cli",
            "recover-local",
            "--object-store-dir",
            "/tmp/velorix",
            "--relation-id",
            "orders",
            "--relation-version",
            "2026-05-05.v1",
            "--slatedb-state-path",
            "v1/slatedb/state",
            "--allow-bootstrap-raw-state",
        ])
        .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("--allow-bootstrap-raw-state"));
        assert!(message.contains("--slatedb-state-path"));
    }

    #[test]
    fn recover_local_cli_requires_relation_catalog_identity() {
        let error = Cli::try_parse_from([
            "velorix-cli",
            "recover-local",
            "--object-store-dir",
            "/tmp/velorix",
        ])
        .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("--relation-id"));
        assert!(message.contains("--relation-version"));
    }

    #[tokio::test]
    async fn recover_local_runtime_rejects_raw_state_without_bootstrap_flag() {
        let dir = tempdir().unwrap();
        let store = local_object_store(dir.path()).unwrap();

        let error = recover_local_runtime(
            store,
            "orders".to_string(),
            "2026-05-05.v1".to_string(),
            None,
            None,
            false,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("--allow-bootstrap-raw-state"));
    }

    #[tokio::test]
    async fn recover_local_runtime_rejects_bootstrap_raw_state_with_slatedb_state_path() {
        let dir = tempdir().unwrap();
        let store = local_object_store(dir.path()).unwrap();

        let error = recover_local_runtime(
            store,
            "orders".to_string(),
            "2026-05-05.v1".to_string(),
            Some("v1/slatedb/state".to_string()),
            None,
            true,
        )
        .await
        .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("--allow-bootstrap-raw-state"));
        assert!(message.contains("--slatedb-state-path"));
    }

    #[tokio::test]
    async fn recover_local_runtime_slatedb_latest_checks_capabilities_before_catalog_recovery() {
        let dir = tempdir().unwrap();
        let prefix_file = dir.path().join("not-a-directory");
        fs::write(&prefix_file, b"not a directory").unwrap();
        let store = local_object_store(&prefix_file).unwrap();

        let error = recover_local_runtime(
            store,
            "orders".to_string(),
            "2026-05-05.v1".to_string(),
            Some("v1/slatedb/state".to_string()),
            None,
            false,
        )
        .await
        .unwrap_err();

        assert_recover_local_capability_probe_failed_before_recovery(error);
    }

    #[tokio::test]
    async fn recover_local_runtime_slatedb_selected_checkpoint_checks_capabilities_before_recovery()
    {
        let dir = tempdir().unwrap();
        let prefix_file = dir.path().join("not-a-directory");
        fs::write(&prefix_file, b"not a directory").unwrap();
        let store = local_object_store(&prefix_file).unwrap();

        let error = recover_local_runtime(
            store,
            "orders".to_string(),
            "2026-05-05.v1".to_string(),
            Some("v1/slatedb/state".to_string()),
            Some(7),
            false,
        )
        .await
        .unwrap_err();

        assert_recover_local_capability_probe_failed_before_recovery(error);
    }

    #[tokio::test]
    async fn recover_local_runtime_raw_selected_checkpoint_checks_capabilities_before_recovery() {
        let dir = tempdir().unwrap();
        let prefix_file = dir.path().join("not-a-directory");
        fs::write(&prefix_file, b"not a directory").unwrap();
        let store = local_object_store(&prefix_file).unwrap();

        let error = recover_local_runtime(
            store,
            "orders".to_string(),
            "2026-05-05.v1".to_string(),
            None,
            Some(7),
            true,
        )
        .await
        .unwrap_err();

        assert_recover_local_capability_probe_failed_before_recovery(error);
    }

    #[tokio::test]
    async fn checkpoint_inspect_local_checks_capabilities_before_inspection() {
        let dir = tempdir().unwrap();
        let prefix_file = dir.path().join("not-a-directory");
        fs::write(&prefix_file, b"not a directory").unwrap();
        let store = local_object_store(&prefix_file).unwrap();

        let error = inspect_local_checkpoints(store).await.unwrap_err();

        assert_local_admin_capability_probe_failed(error);
    }

    #[tokio::test]
    async fn gc_plan_local_checks_capabilities_before_planning() {
        let dir = tempdir().unwrap();
        let prefix_file = dir.path().join("not-a-directory");
        fs::write(&prefix_file, b"not a directory").unwrap();
        let store = local_object_store(&prefix_file).unwrap();

        let error = plan_local_garbage_collection(
            store,
            GarbageCollectionPolicy {
                retain_latest_manifests: 1,
            },
        )
        .await
        .unwrap_err();

        assert_local_admin_capability_probe_failed(error);
    }

    #[tokio::test]
    async fn gc_execute_local_checks_capabilities_before_execution() {
        let dir = tempdir().unwrap();
        let prefix_file = dir.path().join("not-a-directory");
        fs::write(&prefix_file, b"not a directory").unwrap();
        let store = local_object_store(&prefix_file).unwrap();

        let error = execute_local_garbage_collection(
            store,
            "local-admin-capability-test",
            GarbageCollectionPolicy {
                retain_latest_manifests: 1,
            },
        )
        .await
        .unwrap_err();

        assert_local_admin_capability_probe_failed(error);
    }

    #[tokio::test]
    async fn recover_local_runtime_allows_raw_state_with_bootstrap_flag() {
        let dir = tempdir().unwrap();
        let store = local_object_store(dir.path()).unwrap();

        let error = recover_local_runtime(
            store,
            "orders".to_string(),
            "2026-05-05.v1".to_string(),
            None,
            None,
            true,
        )
        .await
        .unwrap_err();

        let recovery_error = error.downcast_ref::<RecoveryError>().unwrap();
        assert!(matches!(
            recovery_error,
            RecoveryError::RelationCatalogRegistry(RelationCatalogRegistryError::ObjectStore(
                object_store::Error::NotFound { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn recover_local_runtime_uses_slatedb_state_store_for_selected_checkpoint() {
        let dir = tempdir().unwrap();
        let store = local_object_store(dir.path()).unwrap();
        let catalog = orders_sum_count_relation_catalog().unwrap();
        let relation_id = catalog.relation_schema.relation_id.clone();
        let relation_version = catalog.relation_schema.relation_version.clone();
        RelationCatalogRegistry::new(Arc::clone(&store))
            .create(&catalog)
            .await
            .unwrap();
        let publisher =
            CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
                .await
                .unwrap();
        let state = StateObjectWrite::new(
            ORDERS_SUM_COUNT_OWNER,
            0,
            0,
            "state-0000",
            Bytes::from_static(br#"{"records":[]}"#),
        )
        .unwrap();
        let state_ref = publisher.write_state_object(&state).await.unwrap();
        publisher
            .publish_manifest(&CheckpointManifest {
                schema_version: 1,
                checkpoint_version: 0,
                input_ranges: vec![InputRange {
                    stream_id: "orders".to_string(),
                    partition_id: 0,
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 1,
                }],
                state_objects: vec![state_ref],
                output_objects: vec![],
                parent_checkpoint: None,
                created_at: "2026-05-06T00:00:00Z".to_string(),
            })
            .await
            .unwrap();
        drop(publisher);

        let recovered = recover_local_runtime(
            Arc::clone(&store),
            relation_id,
            relation_version,
            Some("v1/slatedb/state".to_string()),
            Some(0),
            false,
        )
        .await
        .unwrap();

        assert_eq!(recovered.latest_checkpoint_version(), Some(0));
        assert_eq!(recovered.replayed_batch_count(), 0);
        assert_eq!(
            single_recovery_transition_record(store.as_ref(), 0)
                .await
                .recovery_mode,
            CheckpointRecoveryMode::SelectedCheckpoint
        );
    }

    #[tokio::test]
    async fn recover_local_runtime_uses_slatedb_state_store_for_latest_checkpoint() {
        let dir = tempdir().unwrap();
        let store = local_object_store(dir.path()).unwrap();
        let catalog = orders_sum_count_relation_catalog().unwrap();
        let relation_id = catalog.relation_schema.relation_id.clone();
        let relation_version = catalog.relation_schema.relation_version.clone();
        RelationCatalogRegistry::new(Arc::clone(&store))
            .create(&catalog)
            .await
            .unwrap();
        let publisher =
            CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
                .await
                .unwrap();
        let state = StateObjectWrite::new(
            ORDERS_SUM_COUNT_OWNER,
            0,
            0,
            "state-0000",
            Bytes::from_static(br#"{"records":[]}"#),
        )
        .unwrap();
        let state_ref = publisher.write_state_object(&state).await.unwrap();
        publisher
            .publish_manifest(&CheckpointManifest {
                schema_version: 1,
                checkpoint_version: 0,
                input_ranges: vec![InputRange {
                    stream_id: "orders".to_string(),
                    partition_id: 0,
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 1,
                }],
                state_objects: vec![state_ref],
                output_objects: vec![],
                parent_checkpoint: None,
                created_at: "2026-05-06T00:00:00Z".to_string(),
            })
            .await
            .unwrap();
        drop(publisher);

        let recovered = recover_local_runtime(
            Arc::clone(&store),
            relation_id,
            relation_version,
            Some("v1/slatedb/state".to_string()),
            None,
            false,
        )
        .await
        .unwrap();

        assert_eq!(recovered.latest_checkpoint_version(), Some(0));
        assert_eq!(recovered.replayed_batch_count(), 0);
        assert_eq!(
            single_recovery_transition_record(store.as_ref(), 0)
                .await
                .recovery_mode,
            CheckpointRecoveryMode::SlateDbLatest
        );
    }

    async fn single_recovery_transition_record(
        store: &dyn ObjectStore,
        checkpoint_version: u64,
    ) -> CheckpointRecoveryTransitionRecordV1 {
        let prefix = format!("v1/checkpoint-recovery/{checkpoint_version:020}/transitions");
        let listing = store
            .list_with_delimiter(Some(&ObjectStorePath::from(prefix)))
            .await
            .unwrap();
        let objects = listing.objects;
        assert_eq!(objects.len(), 1);

        let bytes = store
            .get(&objects[0].location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn assert_recover_local_capability_probe_failed_before_recovery(error: anyhow::Error) {
        let message = format!("{error:#}");
        assert!(
            message.contains("capability probe write failed"),
            "expected recover-local checked path to fail in capability probe, got: {message}"
        );
        assert!(
            !message.contains("relation catalog"),
            "capability gate should run before relation-catalog recovery, got: {message}"
        );
        assert!(
            !message.contains("published checkpoint"),
            "capability gate should run before checkpoint recovery, got: {message}"
        );
    }

    fn assert_local_admin_capability_probe_failed(error: anyhow::Error) {
        let message = format!("{error:#}");
        assert!(
            message.contains("capability probe write failed"),
            "expected local-admin checked path to fail in capability probe, got: {message}"
        );
        assert!(
            !message.contains("garbage collection policy"),
            "capability gate should run before GC policy/planning logic, got: {message}"
        );
    }

    #[test]
    fn readiness_report_cli_parses_json_flag() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "readiness-report",
            "--evidence",
            "readiness.json",
            "--json",
        ])
        .unwrap();

        let Some(Command::ReadinessReport {
            evidence,
            require_release_artifacts,
            first_e2e_artifacts,
            dependency_governance_evidence,
            dependency_governance_manifest,
            release_commit,
            feldera_artifact_hash_evidence,
            feldera_release_provenance_evidence,
            s3_release_benchmark_gate_evidence,
            production_gc_run_evidence,
            rustfs_production_gc_validation_evidence,
            ingest_writer_lifecycle_evidence,
            standing_runtime_product_evidence,
            json,
        }) = cli.command
        else {
            panic!("expected readiness-report command");
        };

        assert_eq!(evidence, PathBuf::from("readiness.json"));
        assert!(!require_release_artifacts);
        assert!(!first_e2e_artifacts);
        assert!(dependency_governance_evidence.is_none());
        assert!(dependency_governance_manifest.is_none());
        assert!(release_commit.is_none());
        assert!(feldera_artifact_hash_evidence.is_none());
        assert!(feldera_release_provenance_evidence.is_none());
        assert!(s3_release_benchmark_gate_evidence.is_none());
        assert!(production_gc_run_evidence.is_none());
        assert!(rustfs_production_gc_validation_evidence.is_none());
        assert!(ingest_writer_lifecycle_evidence.is_none());
        assert!(standing_runtime_product_evidence.is_none());
        assert!(json);
    }

    #[test]
    fn readiness_report_cli_parses_release_artifact_flags() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "readiness-report",
            "--evidence",
            "readiness.json",
            "--require-release-artifacts",
            "--dependency-governance-evidence",
            "dependency.json",
            "--dependency-governance-manifest",
            "dependency-governance.json",
            "--release-commit",
            "0123456789abcdef0123456789abcdef01234567",
            "--feldera-artifact-hash-evidence",
            "feldera-hash.json",
            "--feldera-release-provenance-evidence",
            "feldera-provenance.json",
            "--s3-release-benchmark-gate-evidence",
            "s3-gate.json",
            "--production-gc-run-evidence",
            "production-gc.json",
            "--rustfs-production-gc-validation-evidence",
            "rustfs-production-gc-validation.json",
            "--ingest-writer-lifecycle-evidence",
            "ingest-writer-lifecycle.json",
            "--standing-runtime-product-evidence",
            "product-evidence.json",
        ])
        .unwrap();

        let Some(Command::ReadinessReport {
            require_release_artifacts,
            first_e2e_artifacts,
            dependency_governance_evidence,
            dependency_governance_manifest,
            release_commit,
            feldera_artifact_hash_evidence,
            feldera_release_provenance_evidence,
            s3_release_benchmark_gate_evidence,
            production_gc_run_evidence,
            rustfs_production_gc_validation_evidence,
            ingest_writer_lifecycle_evidence,
            standing_runtime_product_evidence,
            ..
        }) = cli.command
        else {
            panic!("expected readiness-report command");
        };

        assert!(require_release_artifacts);
        assert!(!first_e2e_artifacts);
        assert_eq!(
            dependency_governance_evidence,
            Some(PathBuf::from("dependency.json"))
        );
        assert_eq!(
            dependency_governance_manifest,
            Some(PathBuf::from("dependency-governance.json"))
        );
        assert_eq!(
            release_commit.as_deref(),
            Some("0123456789abcdef0123456789abcdef01234567")
        );
        assert_eq!(
            feldera_artifact_hash_evidence,
            Some(PathBuf::from("feldera-hash.json"))
        );
        assert_eq!(
            feldera_release_provenance_evidence,
            Some(PathBuf::from("feldera-provenance.json"))
        );
        assert_eq!(
            s3_release_benchmark_gate_evidence,
            Some(PathBuf::from("s3-gate.json"))
        );
        assert_eq!(
            production_gc_run_evidence,
            Some(PathBuf::from("production-gc.json"))
        );
        assert_eq!(
            rustfs_production_gc_validation_evidence,
            Some(PathBuf::from("rustfs-production-gc-validation.json"))
        );
        assert_eq!(
            ingest_writer_lifecycle_evidence,
            Some(PathBuf::from("ingest-writer-lifecycle.json"))
        );
        assert_eq!(
            standing_runtime_product_evidence,
            Some(PathBuf::from("product-evidence.json"))
        );
    }

    #[test]
    fn readiness_report_cli_parses_first_e2e_artifact_flag() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "readiness-report",
            "--evidence",
            "readiness.json",
            "--first-e2e-artifacts",
            "--dependency-governance-evidence",
            "dependency.json",
            "--s3-release-benchmark-gate-evidence",
            "s3-gate.json",
            "--production-gc-run-evidence",
            "production-gc.json",
            "--rustfs-production-gc-validation-evidence",
            "rustfs-production-gc-validation.json",
            "--ingest-writer-lifecycle-evidence",
            "ingest-writer-lifecycle.json",
            "--standing-runtime-product-evidence",
            "product-evidence.json",
        ])
        .unwrap();

        let Some(Command::ReadinessReport {
            first_e2e_artifacts,
            dependency_governance_evidence,
            s3_release_benchmark_gate_evidence,
            production_gc_run_evidence,
            rustfs_production_gc_validation_evidence,
            ingest_writer_lifecycle_evidence,
            standing_runtime_product_evidence,
            ..
        }) = cli.command
        else {
            panic!("expected readiness-report command");
        };

        assert!(first_e2e_artifacts);
        assert_eq!(
            dependency_governance_evidence,
            Some(PathBuf::from("dependency.json"))
        );
        assert_eq!(
            s3_release_benchmark_gate_evidence,
            Some(PathBuf::from("s3-gate.json"))
        );
        assert_eq!(
            production_gc_run_evidence,
            Some(PathBuf::from("production-gc.json"))
        );
        assert_eq!(
            rustfs_production_gc_validation_evidence,
            Some(PathBuf::from("rustfs-production-gc-validation.json"))
        );
        assert_eq!(
            ingest_writer_lifecycle_evidence,
            Some(PathBuf::from("ingest-writer-lifecycle.json"))
        );
        assert_eq!(
            standing_runtime_product_evidence,
            Some(PathBuf::from("product-evidence.json"))
        );
    }

    #[test]
    fn release_status_validate_cli_parses_status_matrix_path() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "release-status-validate",
            "--status-matrix",
            "docs/architecture/production-readiness-status.md",
        ])
        .unwrap();

        let Some(Command::ReleaseStatusValidate { status_matrix }) = cli.command else {
            panic!("expected release-status-validate command");
        };

        assert_eq!(
            status_matrix,
            PathBuf::from("docs/architecture/production-readiness-status.md")
        );
    }

    #[test]
    fn dependency_governance_validate_cli_parses_json_command() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "dependency-governance-validate",
            "--manifest",
            "dependency-governance.json",
            "--cargo-deny-json",
            "target/dependency-governance/cargo-deny.jsonl",
            "--json",
        ])
        .unwrap();

        let Some(Command::DependencyGovernanceValidate {
            manifest,
            cargo_deny_json,
            json,
        }) = cli.command
        else {
            panic!("expected dependency-governance-validate command");
        };

        assert_eq!(manifest, PathBuf::from("dependency-governance.json"));
        assert_eq!(
            cargo_deny_json,
            Some(PathBuf::from(
                "target/dependency-governance/cargo-deny.jsonl"
            ))
        );
        assert!(json);
    }

    #[test]
    fn feldera_artifact_verify_cli_parses_json_command() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "feldera-artifact-verify",
            "--spec",
            "spec.json",
            "--metadata",
            "metadata.json",
            "--artifact-package",
            "artifact.tar",
            "--json",
        ])
        .unwrap();

        let Some(Command::FelderaArtifactVerify {
            spec,
            metadata,
            artifact_package,
            json,
        }) = cli.command
        else {
            panic!("expected feldera-artifact-verify command");
        };

        assert_eq!(spec, PathBuf::from("spec.json"));
        assert_eq!(metadata, PathBuf::from("metadata.json"));
        assert_eq!(artifact_package, PathBuf::from("artifact.tar"));
        assert!(json);
    }

    #[test]
    fn feldera_artifact_provenance_verify_cli_parses_json_command() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "feldera-artifact-provenance-verify",
            "--metadata",
            "metadata.json",
            "--provenance",
            "provenance.json",
            "--json",
        ])
        .unwrap();

        let Some(Command::FelderaArtifactProvenanceVerify {
            metadata,
            provenance,
            json,
        }) = cli.command
        else {
            panic!("expected feldera-artifact-provenance-verify command");
        };

        assert_eq!(metadata, PathBuf::from("metadata.json"));
        assert_eq!(provenance, PathBuf::from("provenance.json"));
        assert!(json);
    }

    #[test]
    fn feldera_artifact_verify_outputs_stable_json_evidence() {
        let dir = tempdir().unwrap();
        let spec = dir.path().join("spec.json");
        let metadata = dir.path().join("metadata.json");
        let artifact_package = dir.path().join("artifact.tar");
        fs::write(&spec, feldera_spec_json()).unwrap();
        fs::write(&metadata, feldera_metadata_json()).unwrap();
        fs::write(&artifact_package, b"compiled Feldera artifact bytes").unwrap();

        let evidence =
            read_feldera_artifact_hash_verified_evidence(&spec, &metadata, &artifact_package)
                .unwrap();
        let json: serde_json::Value =
            serde_json::from_str(&format_feldera_artifact_evidence_json(&evidence).unwrap())
                .unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "schema_version": 1,
                "status": "pass",
                "evidence_kind": "feldera_artifact_hash_verified",
                "view_id": "standing_view_orders_by_region",
                "artifact_id": "feldera-artifact-orders-by-region-20260503",
                "artifact_hash": "sha256:9063ca4eca6bd69190c68b01a00def0a5b86470abbc2312e94491c59a1c7f537",
                "spec_hash": "velorix-feldera-spec-sha256-v1:0e24cbe06543d735a6d62868f230c4610fb9139cb91e5e8f72042f17da0ecbea",
                "generated_rust_abi_version": "feldera-generated-rust-abi-v1"
            })
        );
    }

    #[test]
    fn feldera_artifact_provenance_verify_outputs_stable_json_evidence() {
        let dir = tempdir().unwrap();
        let metadata = dir.path().join("metadata.json");
        let provenance = dir.path().join("provenance.json");
        fs::write(&metadata, feldera_metadata_json()).unwrap();
        fs::write(&provenance, feldera_release_provenance_json()).unwrap();

        let evidence =
            read_feldera_artifact_release_provenance_evidence(&metadata, &provenance).unwrap();
        let json: serde_json::Value = serde_json::from_str(
            &format_feldera_artifact_release_provenance_evidence_json(&evidence).unwrap(),
        )
        .unwrap();

        assert_eq!(
            json,
            serde_json::json!({
                "schema_version": 1,
                "status": "pass",
                "evidence_kind": "feldera_artifact_release_provenance",
                "release_id": "velorix-feldera-release-20260507",
                "release_version": "1.0.0-rc.1",
                "build_id": "feldera-build-20260507T000000Z",
                "builder_id": "github-actions/feldera-artifacts",
                "artifact_id": "feldera-artifact-orders-by-region-20260503",
                "artifact_hash": "sha256:9063ca4eca6bd69190c68b01a00def0a5b86470abbc2312e94491c59a1c7f537",
                "spec_hash": "velorix-feldera-spec-sha256-v1:0e24cbe06543d735a6d62868f230c4610fb9139cb91e5e8f72042f17da0ecbea",
                "generated_rust_abi_version": "feldera-generated-rust-abi-v1",
                "generated_rust_crate_name": "orders_by_region_pipeline",
                "source_repository": "https://github.com/mrchypark/velorix",
                "source_revision": "0123456789abcdef0123456789abcdef01234567"
            })
        );
    }

    #[test]
    fn gc_plan_cli_parses_dry_run_json_command() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "gc-plan-local",
            "--object-store-dir",
            "/tmp/velorix",
            "--retain-latest-manifests",
            "2",
            "--json",
        ])
        .unwrap();

        let Some(Command::GcPlanLocal {
            object_store_dir,
            retain_latest_manifests,
            json,
        }) = cli.command
        else {
            panic!("expected gc-plan-local command");
        };

        assert_eq!(object_store_dir, PathBuf::from("/tmp/velorix"));
        assert_eq!(retain_latest_manifests, 2);
        assert!(json);
    }

    #[test]
    fn checkpoint_repair_latest_cli_parses_json_command() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "checkpoint-repair-latest-local",
            "--object-store-dir",
            "/tmp/velorix",
            "--json",
        ])
        .unwrap();

        let Some(Command::CheckpointRepairLatestLocal {
            object_store_dir,
            json,
        }) = cli.command
        else {
            panic!("expected checkpoint-repair-latest-local command");
        };

        assert_eq!(object_store_dir, PathBuf::from("/tmp/velorix"));
        assert!(json);
    }

    #[test]
    fn checkpoint_repair_cli_parses_json_command() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "checkpoint-repair-local",
            "--object-store-dir",
            "/tmp/velorix",
            "--json",
        ])
        .unwrap();

        let Some(Command::CheckpointRepairLocal {
            object_store_dir,
            json,
        }) = cli.command
        else {
            panic!("expected checkpoint-repair-local command");
        };

        assert_eq!(object_store_dir, PathBuf::from("/tmp/velorix"));
        assert!(json);
    }

    #[test]
    fn gc_execute_cli_parses_local_json_command() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "gc-execute-local",
            "--object-store-dir",
            "/tmp/velorix",
            "--retain-latest-manifests",
            "2",
            "--run-id",
            "run-0001",
            "--json",
        ])
        .unwrap();

        let Some(Command::GcExecuteLocal {
            object_store_dir,
            retain_latest_manifests,
            run_id,
            json,
        }) = cli.command
        else {
            panic!("expected gc-execute-local command");
        };

        assert_eq!(object_store_dir, PathBuf::from("/tmp/velorix"));
        assert_eq!(retain_latest_manifests, 2);
        assert_eq!(run_id, "run-0001");
        assert!(json);
    }

    #[test]
    fn gc_execute_s3_compatible_cli_parses_json_command() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "gc-execute-s3-compatible",
            "--authority-store-id",
            "s3://velorix-prod",
            "--retain-latest-manifests",
            "2",
            "--run-id",
            "run-0001",
            "--json",
        ])
        .unwrap();

        let Some(Command::GcExecuteS3Compatible {
            authority_store_id,
            retain_latest_manifests,
            run_id,
            json,
        }) = cli.command
        else {
            panic!("expected gc-execute-s3-compatible command");
        };

        assert_eq!(authority_store_id, "s3://velorix-prod");
        assert_eq!(retain_latest_manifests, 2);
        assert_eq!(run_id, "run-0001");
        assert!(json);
    }

    #[test]
    fn gc_seed_s3_compatible_fixture_cli_parses_json_command() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "gc-seed-s3-compatible-fixture",
            "--authority-store-id",
            "s3://velorix-prod",
            "--seed-id",
            "seed-0001",
            "--json",
        ])
        .unwrap();

        let Some(Command::GcSeedS3CompatibleFixture {
            authority_store_id,
            seed_id,
            json,
        }) = cli.command
        else {
            panic!("expected gc-seed-s3-compatible-fixture command");
        };

        assert_eq!(authority_store_id, "s3://velorix-prod");
        assert_eq!(seed_id, "seed-0001");
        assert!(json);
    }

    #[test]
    fn gc_production_evidence_cli_parses_json_command() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "gc-production-evidence",
            "--deployment-id",
            "prod-a",
            "--authority-store-id",
            "s3://velorix-prod",
            "--gc-run-id",
            "gc-run-20260513T000000Z",
            "--json",
        ])
        .unwrap();

        let Some(Command::GcProductionEvidence {
            deployment_id,
            authority_store_id,
            gc_run_id,
            json,
        }) = cli.command
        else {
            panic!("expected gc-production-evidence command");
        };

        assert_eq!(deployment_id, "prod-a");
        assert_eq!(authority_store_id, "s3://velorix-prod");
        assert_eq!(gc_run_id, "gc-run-20260513T000000Z");
        assert!(json);
    }

    #[test]
    fn rustfs_production_gc_evidence_validate_cli_parses_json_command() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "rustfs-production-gc-evidence-validate",
            "--gate-evidence",
            "target/velorix-s3/rustfs-s3-gate-evidence.json",
            "--seed-evidence",
            "target/release-evidence/rustfs-production-gc-seed.json",
            "--execute-evidence",
            "target/release-evidence/rustfs-production-gc-run.json",
            "--production-evidence",
            "target/release-evidence/rustfs-production-gc.json",
            "--json",
        ])
        .unwrap();

        let Some(Command::RustfsProductionGcEvidenceValidate {
            gate_evidence,
            seed_evidence,
            execute_evidence,
            production_evidence,
            json,
        }) = cli.command
        else {
            panic!("expected rustfs-production-gc-evidence-validate command");
        };

        assert_eq!(
            gate_evidence,
            PathBuf::from("target/velorix-s3/rustfs-s3-gate-evidence.json")
        );
        assert_eq!(
            seed_evidence,
            PathBuf::from("target/release-evidence/rustfs-production-gc-seed.json")
        );
        assert_eq!(
            execute_evidence,
            PathBuf::from("target/release-evidence/rustfs-production-gc-run.json")
        );
        assert_eq!(
            production_evidence,
            PathBuf::from("target/release-evidence/rustfs-production-gc.json")
        );
        assert!(json);
    }

    #[test]
    fn ingest_writer_append_cli_parses_payload_and_authority_flags() {
        let cli = Cli::try_parse_from([
            "velorix-cli",
            "ingest-writer-append",
            "--payload-file",
            "payload.vlxingest",
            "--authority-store-id",
            "s3://velorix-prod",
            "--authority-namespace",
            "prod-a",
            "--operator-id",
            "operator-a",
            "--writer-id",
            "writer-a",
            "--json",
        ])
        .unwrap();

        let Some(Command::IngestWriterAppend {
            payload_file,
            authority_store_id,
            authority_namespace,
            operator_id,
            writer_id,
            json,
        }) = cli.command
        else {
            panic!("expected ingest-writer-append command");
        };

        assert_eq!(payload_file, PathBuf::from("payload.vlxingest"));
        assert_eq!(authority_store_id, "s3://velorix-prod");
        assert_eq!(authority_namespace, "prod-a");
        assert_eq!(operator_id, "operator-a");
        assert_eq!(writer_id, "writer-a");
        assert!(json);
    }

    #[test]
    fn gc_production_evidence_rejects_local_dev_authority_store_id() {
        let error = validate_production_gc_authority_store_id("file:///tmp/velorix").unwrap_err();

        assert!(format!("{error:#}").contains("local/dev authority_store_id"));
    }

    #[test]
    fn gc_production_evidence_config_rejects_missing_s3_compat_gate() {
        let error = production_gc_s3_config_from_lookup(|_| None).unwrap_err();

        assert!(format!("{error:#}").contains("VELORIX_S3_COMPAT=1"));
    }

    #[test]
    fn rustfs_production_gc_evidence_validate_accepts_bound_artifact_family() {
        let dir = tempdir().unwrap();
        let (gate, seed, execute, production) = write_rustfs_production_gc_family(dir.path());

        let report =
            validate_rustfs_production_gc_evidence_family(&gate, &seed, &execute, &production)
                .unwrap();

        assert_eq!(report.status, "pass");
        assert_eq!(report.deployment_id, "rustfs-s3-gate");
        assert_eq!(
            report.authority_store_id,
            "s3://rustfs/velorix-rustfs/rustfs-s3-gate/test/production-gc"
        );
        assert_eq!(report.gc_run_id, "rustfs-production-gc-test");
        assert_eq!(report.deleted_candidates, 1);
        assert!(format_rustfs_production_gc_evidence_report(&report)
            .contains("[x] artifact_family_paths_and_identity_bound"));
    }

    #[test]
    fn rustfs_production_gc_evidence_validate_rejects_unbound_run_id() {
        let dir = tempdir().unwrap();
        let (gate, seed, execute, production) = write_rustfs_production_gc_family(dir.path());
        let mut run: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&execute).unwrap()).unwrap();
        run["run_id"] = serde_json::json!("other-run");
        fs::write(&execute, serde_json::to_string_pretty(&run).unwrap()).unwrap();
        let execute_run: GarbageCollectionRunV1 =
            serde_json::from_str(&fs::read_to_string(&execute).unwrap()).unwrap();
        let mut production_json: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&production).unwrap()).unwrap();
        production_json["verified_gc_run_digest"] =
            serde_json::json!(garbage_collection_run_digest(&execute_run).unwrap());
        fs::write(
            &production,
            serde_json::to_string_pretty(&production_json).unwrap(),
        )
        .unwrap();

        let error =
            validate_rustfs_production_gc_evidence_family(&gate, &seed, &execute, &production)
                .unwrap_err();

        assert!(format!("{error:#}").contains("run identifiers do not match"));
    }

    #[test]
    fn rustfs_production_gc_evidence_validate_rejects_stale_execute_digest() {
        let dir = tempdir().unwrap();
        let (gate, seed, execute, production) = write_rustfs_production_gc_family(dir.path());
        let mut run: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&execute).unwrap()).unwrap();
        run["report"]["skipped"] = serde_json::json!([
            {
                "object_key": "v1/state/orders_sum_count/p=0000000000/chk=00000000000000000000/stale-extra.state",
                "kind": "raw_state_object"
            }
        ]);
        fs::write(&execute, serde_json::to_string_pretty(&run).unwrap()).unwrap();

        let error =
            validate_rustfs_production_gc_evidence_family(&gate, &seed, &execute, &production)
                .unwrap_err();

        assert!(format!("{error:#}").contains("verified_gc_run_digest"));
    }

    #[test]
    fn rustfs_production_gc_evidence_validate_rejects_seed_id_substring_key() {
        let dir = tempdir().unwrap();
        let (gate, seed, execute, production) = write_rustfs_production_gc_family(dir.path());
        let mut run: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&execute).unwrap()).unwrap();
        let different_key =
            "v1/state/other_owner/p=0000000000/chk=00000000000000000000/rustfs-production-gc-test-state-0000.state";
        run["plan"]["candidates"][0]["object_key"] = serde_json::json!(different_key);
        run["report"]["deleted"][0]["object_key"] = serde_json::json!(different_key);
        fs::write(&execute, serde_json::to_string_pretty(&run).unwrap()).unwrap();
        let execute_run: GarbageCollectionRunV1 =
            serde_json::from_str(&fs::read_to_string(&execute).unwrap()).unwrap();
        let mut production_json: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&production).unwrap()).unwrap();
        production_json["verified_gc_run_digest"] =
            serde_json::json!(garbage_collection_run_digest(&execute_run).unwrap());
        production_json["verified_gc_run_deleted_object_keys"] =
            serde_json::json!(gc_run_deleted_object_keys(&execute_run));
        fs::write(
            &production,
            serde_json::to_string_pretty(&production_json).unwrap(),
        )
        .unwrap();

        let error =
            validate_rustfs_production_gc_evidence_family(&gate, &seed, &execute, &production)
                .unwrap_err();

        assert!(format!("{error:#}").contains("deleted keys do not match seeded expectation"));
    }

    #[test]
    fn rustfs_production_gc_evidence_validate_rejects_empty_delete_report() {
        let dir = tempdir().unwrap();
        let (gate, seed, execute, production) = write_rustfs_production_gc_family(dir.path());
        let mut run: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&execute).unwrap()).unwrap();
        run["report"]["deleted"] = serde_json::json!([]);
        fs::write(&execute, serde_json::to_string_pretty(&run).unwrap()).unwrap();

        let error =
            validate_rustfs_production_gc_evidence_family(&gate, &seed, &execute, &production)
                .unwrap_err();

        assert!(format!("{error:#}").contains("below seed expected minimum"));
    }

    #[tokio::test]
    async fn gc_execute_s3_compatible_persists_run_for_production_evidence_verifier() {
        let dir = tempdir().unwrap();
        let store = local_object_store(dir.path()).unwrap();
        let seeder = CheckpointPublisher::new(Arc::clone(&store));
        let state_0 = StateObjectWrite::new(
            ORDERS_SUM_COUNT_OWNER,
            0,
            0,
            "state-0000",
            Bytes::from_static(b"state-0"),
        )
        .unwrap();
        let state_ref_0 = seeder.write_state_object(&state_0).await.unwrap();
        seeder
            .publish_manifest(&CheckpointManifest {
                schema_version: 1,
                checkpoint_version: 0,
                input_ranges: vec![InputRange {
                    stream_id: "orders".to_string(),
                    partition_id: 0,
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 1,
                }],
                state_objects: vec![state_ref_0],
                output_objects: vec![],
                parent_checkpoint: None,
                created_at: "2026-05-06T00:00:00Z".to_string(),
            })
            .await
            .unwrap();
        let state_1 = StateObjectWrite::new(
            ORDERS_SUM_COUNT_OWNER,
            0,
            1,
            "state-0001",
            Bytes::from_static(b"state-1"),
        )
        .unwrap();
        let state_ref_1 = seeder.write_state_object(&state_1).await.unwrap();
        seeder
            .publish_manifest(&CheckpointManifest {
                schema_version: 1,
                checkpoint_version: 1,
                input_ranges: vec![InputRange {
                    stream_id: "orders".to_string(),
                    partition_id: 0,
                    start_offset_inclusive: 0,
                    end_offset_exclusive: 2,
                }],
                state_objects: vec![state_ref_1],
                output_objects: vec![],
                parent_checkpoint: Some(0),
                created_at: "2026-05-06T00:01:00Z".to_string(),
            })
            .await
            .unwrap();

        let run = execute_s3_compatible_garbage_collection(
            Arc::clone(&store),
            "s3://velorix-test",
            "run-0001",
            GarbageCollectionPolicy {
                retain_latest_manifests: 1,
            },
        )
        .await
        .unwrap();
        let verifier = production_gc_checkpoint_publisher(store, "run-0001")
            .await
            .unwrap();
        let verified = verifier
            .verify_garbage_collection_run_retention_evidence("run-0001")
            .await
            .unwrap();

        assert_eq!(run.run_id, "run-0001");
        assert_eq!(run.report.deleted.len(), 1);
        assert_eq!(verified.run_id, "run-0001");
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

    #[tokio::test]
    async fn gc_seed_s3_compatible_fixture_creates_live_deleted_candidate() {
        let dir = tempdir().unwrap();
        let store = local_object_store(dir.path()).unwrap();

        let seed =
            seed_s3_compatible_gc_fixture(Arc::clone(&store), "s3://velorix-test", "seed-0001")
                .await
                .unwrap();
        let run = execute_s3_compatible_garbage_collection(
            Arc::clone(&store),
            "s3://velorix-test",
            "run-0001",
            GarbageCollectionPolicy {
                retain_latest_manifests: 1,
            },
        )
        .await
        .unwrap();
        let evidence = generate_production_gc_run_evidence(
            store,
            "prod-a".to_string(),
            "s3://velorix-test".to_string(),
            "run-0001".to_string(),
        )
        .await
        .unwrap();

        assert_eq!(seed.checkpoint_versions, vec![0, 1]);
        assert_eq!(seed.fixture_kind, "release_smoke_gc_fixture");
        assert_eq!(seed.state_object_ids.len(), 2);
        assert_eq!(seed.expected_deleted_object_keys.len(), 1);
        assert_eq!(seed.state_objects_written, 2);
        assert_eq!(seed.expected_min_deleted_candidates, 1);
        assert_eq!(run.report.deleted.len(), 1);
        assert_eq!(evidence.status, "pass");
        assert_eq!(
            evidence.verified_gc_run_digest.as_deref(),
            Some(garbage_collection_run_digest(&run).unwrap().as_str())
        );
        assert_eq!(evidence.verified_gc_run_deleted_count, Some(1));
        assert_eq!(evidence.verified_gc_run_retain_latest_manifests, Some(1));
    }

    #[test]
    fn ingest_writer_append_config_rejects_missing_s3_compat_gate() {
        let error = s3_compatible_authority_config_from_lookup(|_| None).unwrap_err();

        assert!(format!("{error:#}").contains("VELORIX_S3_COMPAT=1"));
    }

    #[tokio::test]
    async fn ingest_writer_append_uses_checked_startup_runtime_before_append() {
        let dir = tempdir().unwrap();
        let store = local_object_store(dir.path()).unwrap();
        let catalog = orders_sum_count_relation_catalog().unwrap();
        RelationCatalogRegistry::new(Arc::clone(&store))
            .create(&catalog)
            .await
            .unwrap();

        let artifact = run_ingest_writer_append(
            store,
            IngestWriterAppendRequest {
                authority_store_id: "s3://velorix-test".to_string(),
                authority_namespace: "test".to_string(),
                operator_id: "operator-a".to_string(),
                writer_id: "writer-a".to_string(),
                payload: orders_envelope_bytes_for(catalog.schema_fingerprint.as_str(), 0, 2),
            },
        )
        .await
        .unwrap();

        assert_eq!(artifact.status, "pass");
        assert_eq!(
            artifact.evidence_kind,
            "ingest_writer_checked_runtime_append"
        );
        assert_eq!(artifact.outcome, "appended");
        assert_eq!(artifact.startup_active_admission_records, 0);
        assert_eq!(artifact.startup_expired_orphan_admission_records, 0);
        assert_eq!(artifact.descriptor.stream_id, "orders");
        assert_eq!(artifact.descriptor.partition_id, 0);
        assert_eq!(artifact.descriptor.start_offset_inclusive, 0);
        assert_eq!(artifact.descriptor.end_offset_exclusive, 2);
        assert_eq!(
            artifact.descriptor.object_key,
            "v1/ingest/orders/p=0000000000/00000000000000000000-00000000000000000002.batch"
        );
    }

    #[tokio::test]
    async fn ingest_writer_append_rejects_conflict_as_failed_evidence() {
        let dir = tempdir().unwrap();
        let store = local_object_store(dir.path()).unwrap();
        let catalog = orders_sum_count_relation_catalog().unwrap();
        RelationCatalogRegistry::new(Arc::clone(&store))
            .create(&catalog)
            .await
            .unwrap();

        run_ingest_writer_append(
            Arc::clone(&store),
            IngestWriterAppendRequest {
                authority_store_id: "s3://velorix-test".to_string(),
                authority_namespace: "test".to_string(),
                operator_id: "operator-a".to_string(),
                writer_id: "writer-a".to_string(),
                payload: orders_envelope_bytes_for(catalog.schema_fingerprint.as_str(), 0, 2),
            },
        )
        .await
        .unwrap();

        let error = run_ingest_writer_append(
            store,
            IngestWriterAppendRequest {
                authority_store_id: "s3://velorix-test".to_string(),
                authority_namespace: "test".to_string(),
                operator_id: "operator-a".to_string(),
                writer_id: "writer-b".to_string(),
                payload: orders_envelope_bytes_for(catalog.schema_fingerprint.as_str(), 1, 3),
            },
        )
        .await
        .unwrap_err();

        assert!(format!("{error:#}").contains("conflicted before append"));
    }

    #[test]
    fn gc_production_evidence_formats_text_summary() {
        let artifact: ProductionGcRunEvidenceArtifactV1 =
            serde_json::from_str(&production_gc_run_evidence_json()).unwrap();

        assert_eq!(
            format_production_gc_run_evidence(&artifact),
            "production_gc_run_evidence status=pass deployment_id=prod-a authority_store_id=s3://velorix-prod gc_run_id=gc-run-20260513T000000Z listing_consistency_checked=true checkpoint_retention_records_checked=true checkpoint_gc_transition_records_checked=true\n"
        );
    }

    #[test]
    fn gc_production_evidence_json_includes_gc_transition_check() {
        let artifact: ProductionGcRunEvidenceArtifactV1 =
            serde_json::from_str(&production_gc_run_evidence_json()).unwrap();
        let value: serde_json::Value =
            serde_json::from_str(&format_production_gc_run_evidence_json(&artifact).unwrap())
                .unwrap();

        assert_eq!(
            value["checkpoint_gc_transition_records_checked"],
            serde_json::json!(true)
        );
    }

    #[test]
    fn garbage_collection_run_digest_is_stable_for_candidate_ordering() {
        let mut first: GarbageCollectionRunV1 = serde_json::from_str(&serde_json::json!({
            "schema_version": 1,
            "run_id": "run-0001",
            "policy": {
                "retain_latest_manifests": 1
            },
            "plan": {
                "retained_manifest_versions": [2, 1],
                "candidates": [
                    {
                        "object_key": "v1/state/orders_sum_count/p=0000000000/chk=00000000000000000002/b.state",
                        "kind": "raw_state_object"
                    },
                    {
                        "object_key": "v1/state/orders_sum_count/p=0000000000/chk=00000000000000000001/a.state",
                        "kind": "raw_state_object"
                    }
                ]
            },
            "report": {
                "deleted": [
                    {
                        "object_key": "v1/state/orders_sum_count/p=0000000000/chk=00000000000000000002/b.state",
                        "kind": "raw_state_object"
                    },
                    {
                        "object_key": "v1/state/orders_sum_count/p=0000000000/chk=00000000000000000001/a.state",
                        "kind": "raw_state_object"
                    }
                ],
                "skipped": []
            }
        })
        .to_string())
        .unwrap();
        let mut second = first.clone();
        second.plan.retained_manifest_versions.reverse();
        second.plan.candidates.reverse();
        second.report.deleted.reverse();

        assert_eq!(
            garbage_collection_run_digest(&first).unwrap(),
            garbage_collection_run_digest(&second).unwrap()
        );
        first.run_id = "run-0002".to_string();
        assert_ne!(
            garbage_collection_run_digest(&first).unwrap(),
            garbage_collection_run_digest(&second).unwrap()
        );
    }

    #[test]
    fn ingest_writer_append_formats_stable_text_summary() {
        let artifact = IngestWriterAppendArtifactV1 {
            schema_version: 1,
            evidence_kind: "ingest_writer_checked_runtime_append".to_string(),
            status: "pass".to_string(),
            authority_store_id: "s3://velorix-prod".to_string(),
            authority_namespace: "prod-a".to_string(),
            operator_id: "operator-a".to_string(),
            writer_id: "writer-a".to_string(),
            startup_active_admission_records: 0,
            startup_expired_orphan_admission_records: 0,
            outcome: "appended".to_string(),
            descriptor: IngestWriterAppendDescriptorV1 {
                stream_id: "orders".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
                object_key:
                    "v1/ingest/orders/p=0000000000/00000000000000000000-00000000000000000002.batch"
                        .to_string(),
            },
        };

        assert_eq!(
            format_ingest_writer_append(&artifact),
            "ingest_writer_append status=pass outcome=appended authority_store_id=s3://velorix-prod authority_namespace=prod-a operator_id=operator-a writer_id=writer-a stream_id=orders partition_id=0 offsets=0-2 object_key=v1/ingest/orders/p=0000000000/00000000000000000000-00000000000000000002.batch\n"
        );
    }

    #[test]
    fn readiness_report_formatter_is_not_json() {
        let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json()).unwrap();
        let summary = format_readiness_report(&report.try_into_report().unwrap());

        assert!(summary.starts_with("production_ready=true\n"));
        assert!(!summary.trim_start().starts_with('{'));
    }

    #[test]
    fn readiness_report_gate_passes_when_production_ready() {
        let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json())
            .unwrap()
            .try_into_report()
            .unwrap();

        ensure_readiness_report_passes(&report).unwrap();
    }

    #[test]
    fn readiness_report_gate_fails_when_blocking_reasons_present() {
        let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json().replace(
            r#""evidence_kind":["dependency_governance_validated"]"#,
            r#""evidence_kind":[]"#,
        ))
        .unwrap()
        .try_into_report()
        .unwrap();

        let error = ensure_readiness_report_passes(&report).unwrap_err();

        assert!(format!("{error:#}").contains(
            "production readiness report is blocked: dependency_governance_status missing dependency_governance_validated evidence"
        ));
    }

    #[test]
    fn readiness_report_requires_all_release_artifact_paths_when_required() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        fs::write(&readiness, readiness_json()).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                require_release_artifacts: true,
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(
            format!("{error:#}")
                .contains("readiness-report --require-release-artifacts requires --dependency-governance-evidence")
        );
    }

    #[test]
    fn readiness_report_rejects_release_evidence_artifacts_without_hiqlite_backend_time_attestation_when_required(
    ) {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let feldera_hash = dir.path().join("feldera-hash.json");
        let s3_gate = dir.path().join("s3-gate.json");
        let production_gc = dir.path().join("production-gc.json");
        let production_gc_validation = dir.path().join("production-gc-validation.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        let standing_product = dir.path().join("product-evidence.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json()).unwrap();
        fs::write(&feldera_hash, feldera_hash_evidence_json()).unwrap();
        fs::write(&s3_gate, s3_release_benchmark_gate_json()).unwrap();
        fs::write(&production_gc, production_gc_run_evidence_json()).unwrap();
        fs::write(
            &production_gc_validation,
            rustfs_production_gc_validation_evidence_json(&production_gc),
        )
        .unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        fs::write(&standing_product, product_evidence_json("required", true)).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                require_release_artifacts: true,
                first_e2e_artifacts: false,
                dependency_governance_evidence: Some(dependency),
                dependency_governance_manifest: None,
                release_commit: None,
                feldera_artifact_hash_evidence: Some(feldera_hash),
                feldera_release_provenance_evidence: None,
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                production_gc_run_evidence: Some(production_gc),
                rustfs_production_gc_validation_evidence: Some(production_gc_validation),
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                standing_runtime_product_evidence: Some(standing_product),
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}")
            .contains("Hiqlite authority attestation proves topology/no-PVC only"));
    }

    #[test]
    fn readiness_report_requires_standing_runtime_product_evidence_when_release_artifacts_required()
    {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let feldera_hash = dir.path().join("feldera-hash.json");
        let s3_gate = dir.path().join("s3-gate.json");
        let production_gc = dir.path().join("production-gc.json");
        let production_gc_validation = dir.path().join("production-gc-validation.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json()).unwrap();
        fs::write(&feldera_hash, feldera_hash_evidence_json()).unwrap();
        fs::write(&s3_gate, s3_release_benchmark_gate_json()).unwrap();
        fs::write(&production_gc, production_gc_run_evidence_json()).unwrap();
        fs::write(
            &production_gc_validation,
            rustfs_production_gc_validation_evidence_json(&production_gc),
        )
        .unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                require_release_artifacts: true,
                dependency_governance_evidence: Some(dependency),
                feldera_artifact_hash_evidence: Some(feldera_hash),
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                production_gc_run_evidence: Some(production_gc),
                rustfs_production_gc_validation_evidence: Some(production_gc_validation),
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains(
            "readiness-report --require-release-artifacts requires --standing-runtime-product-evidence"
        ));
    }

    #[test]
    fn readiness_report_accepts_cargo_deny_dependency_governance_release_evidence() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json()).unwrap();

        let report = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                dependency_governance_evidence: Some(dependency),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap();

        assert!(report.production_ready);
    }

    #[test]
    fn readiness_report_first_e2e_artifacts_accept_local_dependency_governance_without_feldera_release_artifacts(
    ) {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let s3_gate = dir.path().join("s3-gate.json");
        let production_gc = dir.path().join("production-gc.json");
        let production_gc_validation = dir.path().join("production-gc-validation.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        let standing_product = dir.path().join("product-evidence.json");
        let mut readiness_json: serde_json::Value =
            serde_json::from_str(&readiness_json()).unwrap();
        readiness_json["feldera_artifact_status"]["evidence_kind"] =
            serde_json::json!(["feldera_artifact_registry"]);
        fs::write(&readiness, readiness_json.to_string()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json()).unwrap();
        fs::write(&s3_gate, s3_release_benchmark_gate_json()).unwrap();
        fs::write(&production_gc, production_gc_run_evidence_json()).unwrap();
        fs::write(
            &production_gc_validation,
            rustfs_production_gc_validation_evidence_json(&production_gc),
        )
        .unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        fs::write(
            &standing_product,
            product_evidence_json("logical-fencing", false),
        )
        .unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let report = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                first_e2e_artifacts: true,
                dependency_governance_evidence: Some(dependency),
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                production_gc_run_evidence: Some(production_gc),
                rustfs_production_gc_validation_evidence: Some(production_gc_validation),
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                standing_runtime_product_evidence: Some(standing_product),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap();

        assert!(report.production_ready);
    }

    #[test]
    fn readiness_report_rejects_logical_fencing_product_evidence_for_release_artifacts() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let feldera_hash = dir.path().join("feldera-hash.json");
        let s3_gate = dir.path().join("s3-gate.json");
        let production_gc = dir.path().join("production-gc.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        let product = dir.path().join("product-evidence.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json()).unwrap();
        fs::write(&feldera_hash, feldera_hash_evidence_json()).unwrap();
        fs::write(&s3_gate, s3_release_benchmark_gate_json()).unwrap();
        fs::write(&production_gc, production_gc_run_evidence_json()).unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        fs::write(&product, product_evidence_json("logical-fencing", false)).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                standing_runtime_product_evidence: Some(product),
                require_release_artifacts: true,
                dependency_governance_evidence: Some(dependency),
                feldera_artifact_hash_evidence: Some(feldera_hash),
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                production_gc_run_evidence: Some(production_gc),
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}")
            .contains("release product evidence requires required standing-runtime fencing mode"));
    }

    #[test]
    fn readiness_report_rejects_product_evidence_without_external_s3_validation_attachment() {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product
            .pointer_mut("/object_store")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("external_s3_validation_evidence");

        let error = release_readiness_error_for_product(product);

        assert!(format!("{error:#}").contains("/object_store/external_s3_validation_evidence/job"));
    }

    #[test]
    fn readiness_report_rejects_product_evidence_with_malformed_external_s3_validation_job_sibling()
    {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("external-s3-validate-job.json", "{}\n".to_string())),
        );

        assert!(
            format!("{error:#}").contains(
                "external S3 validation job evidence sibling external-s3-validate-job.json"
            ),
            "expected malformed external S3 validation job rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_evidence_with_mismatched_external_s3_validation_log() {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some((
                "external-s3-validate.log",
                "velorix external-s3 validation ok bucket=other prefix=other key=other\n"
                    .to_string(),
            )),
        );

        assert!(
            format!("{error:#}").contains(
                "external S3 validation log evidence sibling external-s3-validate.log missing success line for bucket/prefix/key"
            ),
            "expected mismatched external S3 validation log rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_without_object_store_durability_policy_attestation(
    ) {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product
            .pointer_mut("/object_store")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("durability_policy_attestation");

        let error = release_readiness_error_for_product(product);

        assert!(
            format!("{error:#}").contains("/object_store/durability_policy_attestation"),
            "expected missing durability policy attestation rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_with_mismatched_object_store_durability_policy_attestation(
    ) {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product["object_store"]["durability_policy_attestation"]["bucket"] =
            serde_json::json!("other-bucket");

        let error = release_readiness_error_for_product(product);

        assert!(
            format!("{error:#}").contains(
                "object-store durability policy attestation bucket does not match product authority"
            ),
            "expected mismatched durability policy attestation rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_with_malformed_object_store_durability_policy_sibling(
    ) {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some((
                "object-store-durability-attestation.json",
                "{}\n".to_string(),
            )),
        );

        assert!(
            format!("{error:#}").contains(
                "object-store durability policy evidence sibling object-store-durability-attestation.json missing integer /schema_version"
            ),
            "expected malformed durability sibling rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_with_mismatched_object_store_durability_policy_sibling(
    ) {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        let mut sibling = product["object_store"]["durability_policy_attestation"].clone();
        sibling["bucket"] = serde_json::json!("other-bucket");

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some((
                "object-store-durability-attestation.json",
                sibling.to_string(),
            )),
        );

        assert!(
            format!("{error:#}").contains(
                "object-store durability policy evidence sibling object-store-durability-attestation.json /bucket does not match product evidence /object_store/durability_policy_attestation/bucket"
            ),
            "expected mismatched durability sibling rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_local_development_object_store_durability_attestation() {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product["object_store"]["local_development_authority"] = serde_json::json!(true);

        let error = release_readiness_error_for_product(product);

        assert!(
            format!("{error:#}").contains(
                "local development object-store authorities cannot satisfy product-complete durability policy attestation"
            ),
            "expected local development durability attestation rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_evidence_without_openapi_catalog_smoke() {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product
            .pointer_mut("/api")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("openapi");

        let error = release_readiness_error_for_product(product);

        assert!(format!("{error:#}").contains("/api/openapi/catalog_smoke_passed"));
    }

    #[test]
    fn readiness_report_rejects_product_evidence_with_weak_openapi_catalog_smoke() {
        for (pointer, replacement, expected) in [
            (
                "/api/openapi/evidence_file",
                serde_json::json!(""),
                "must attach OpenAPI catalog evidence",
            ),
            (
                "/api/openapi/promoted_api_path",
                serde_json::json!("/v1/api/scores/other"),
                "default promoted API path",
            ),
            (
                "/api/openapi/promoted_api_path_present",
                serde_json::json!(false),
                "/api/openapi/promoted_api_path_present",
            ),
            (
                "/api/openapi/generic_query_path_absent",
                serde_json::json!(false),
                "/api/openapi/generic_query_path_absent",
            ),
            (
                "/api/openapi/legacy_parameterized_path_absent",
                serde_json::json!(false),
                "/api/openapi/legacy_parameterized_path_absent",
            ),
            (
                "/api/openapi/query_policy_extension_present",
                serde_json::json!(false),
                "/api/openapi/query_policy_extension_present",
            ),
            (
                "/api/openapi/linked_view_policy_id",
                serde_json::json!("other"),
                "interactive query policy",
            ),
            (
                "/api/openapi/response_schema_checked",
                serde_json::json!(false),
                "/api/openapi/response_schema_checked",
            ),
        ] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            *product.pointer_mut(pointer).unwrap() = replacement;

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(expected),
                "expected invalid {pointer} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_product_openapi_claim_with_mismatched_sibling_evidence() {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        let mut openapi: serde_json::Value =
            serde_json::from_str(&openapi_fixture_json(&product)).unwrap();
        openapi["paths"]["/v1/api/scores/positive"]["get"]["x-velorix-query-policy-id"] =
            serde_json::json!("other");

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("openapi.json", openapi.to_string())),
        );

        assert!(
            format!("{error:#}").contains("x-velorix-query-policy-id"),
            "expected OpenAPI policy mismatch rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_openapi_claim_with_forbidden_generic_query_path() {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        let mut openapi: serde_json::Value =
            serde_json::from_str(&openapi_fixture_json(&product)).unwrap();
        openapi["paths"]["/v1/query"] = serde_json::json!({
            "post": {
                "summary": "Generic query",
                "responses": {"200": {"description": "Rows"}}
            }
        });

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("openapi.json", openapi.to_string())),
        );

        assert!(
            format!("{error:#}").contains("must not expose generic /v1/query"),
            "expected generic query OpenAPI rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_openapi_claim_with_wrong_response_schema_sibling() {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        let mut openapi: serde_json::Value =
            serde_json::from_str(&openapi_fixture_json(&product)).unwrap();
        openapi["paths"]["/v1/api/scores/positive"]["get"]["responses"]["200"]["content"]
            ["application/json"]["schema"]["properties"]["rows"]["items"]["properties"]["weight"]
            ["type"] = serde_json::json!("string");

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("openapi.json", openapi.to_string())),
        );

        assert!(
            format!("{error:#}").contains("rows/items/properties/weight/type"),
            "expected OpenAPI response schema rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_evidence_without_production_query_policy_bounds() {
        for field in ["production_bounds_required", "weak_policy_rejected"] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            product
                .pointer_mut("/api/query_policy")
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(field);

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(&format!("/api/query_policy/{field}")),
                "expected missing {field} to fail, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_product_query_policy_claim_with_mismatched_readback_sibling() {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        let mut read_back: serde_json::Value =
            serde_json::from_str(&query_policy_interactive_fixture_json()).unwrap();
        read_back["policy"]["max_output_rows"] = serde_json::json!(999);

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("query-policy-interactive-read.json", read_back.to_string())),
        );

        assert!(
            format!("{error:#}").contains("does not match created interactive policy"),
            "expected query policy readback mismatch rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_query_policy_claim_without_weak_rejection_sibling() {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some((
                "query-policy-weak-rejection.json",
                serde_json::json!({"error": "some other error"}).to_string(),
            )),
        );

        assert!(
            format!("{error:#}").contains("does not prove weak policy rejection"),
            "expected weak query policy rejection evidence failure, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_query_policy_claim_with_unbounded_policy_sibling() {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        let mut policy: serde_json::Value =
            serde_json::from_str(&query_policy_interactive_fixture_json()).unwrap();
        policy["policy"]["max_sql_bytes"] = serde_json::json!(0);
        let contents = policy.to_string();

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("query-policy-interactive.json", contents)),
        );

        assert!(
            format!("{error:#}").contains("/policy/max_sql_bytes > 0"),
            "expected query policy bounds rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_evidence_without_query_policy_evidence_files() {
        for (field, expected) in [
            ("created", "query-policy-interactive.json"),
            ("read_back", "query-policy-interactive-read.json"),
            ("weak_policy_rejection", "query-policy-weak-rejection.json"),
            ("missing_policy_rejection", "query-policy-missing-view.json"),
        ] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            product
                .pointer_mut("/api/query_policy/evidence_files")
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(field);

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(&format!("/api/query_policy/evidence_files/{field}")),
                "expected missing query-policy evidence file {field} to fail, got {error:#}"
            );

            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            *product
                .pointer_mut(&format!("/api/query_policy/evidence_files/{field}"))
                .unwrap() = serde_json::json!("other.json");

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(expected),
                "expected mismatched query-policy evidence file {field} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_product_evidence_without_compile_deploy_job_catalog() {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product
            .pointer_mut("/api")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("compile_deploy");

        let error = release_readiness_error_for_product(product);

        assert!(format!("{error:#}").contains("/api/compile_deploy/job_catalog_verified"));
    }

    #[test]
    fn readiness_report_rejects_product_evidence_with_weak_compile_deploy_job_catalog() {
        for (pointer, replacement, expected) in [
            (
                "/api/compile_deploy/job_catalog_evidence_file",
                serde_json::json!("other.json"),
                "compile/deploy job catalog evidence",
            ),
            (
                "/api/compile_deploy/pending_view_id",
                serde_json::json!("other_view"),
                "pending_scores_by_user",
            ),
            (
                "/api/compile_deploy/compiler_request_embedded",
                serde_json::json!(false),
                "/api/compile_deploy/compiler_request_embedded",
            ),
            (
                "/api/compile_deploy/admin_route",
                serde_json::json!("/v1/view-compile-deploy/run-once"),
                "/v1/view-compile-deploy/jobs",
            ),
            (
                "/api/compile_deploy/worker_run_verified",
                serde_json::json!(false),
                "/api/compile_deploy/worker_run_verified",
            ),
            (
                "/api/compile_deploy/run_once_admin_route",
                serde_json::json!("/v1/view-compile-deploy/jobs"),
                "/v1/view-compile-deploy/run-once",
            ),
            (
                "/api/compile_deploy/run_once_evidence_file",
                serde_json::json!("other.json"),
                "compile/deploy run-once evidence",
            ),
            (
                "/api/compile_deploy/activated_view_id",
                serde_json::json!("other_view"),
                "pending_scores_by_user",
            ),
            (
                "/api/compile_deploy/activated_execution_mode",
                serde_json::json!("feldera_compile_pending"),
                "standing_runtime",
            ),
            (
                "/api/compile_deploy/activated_view_evidence_file",
                serde_json::json!("other.json"),
                "pending-scores-view-after-compile-deploy.json",
            ),
            (
                "/api/compile_deploy/activated_query_evidence_file",
                serde_json::json!("other.json"),
                "pending-scores-query-after-compile-deploy.json",
            ),
        ] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            *product.pointer_mut(pointer).unwrap() = replacement;

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(expected),
                "expected invalid {pointer} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_product_compile_deploy_job_catalog_sibling_without_compiler_request(
    ) {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        let mut catalog: serde_json::Value =
            serde_json::from_str(&compile_deploy_job_catalog_fixture_json(&product)).unwrap();
        catalog["jobs"][0]
            .as_object_mut()
            .unwrap()
            .remove("compiler_request");

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("view-compile-deploy-jobs.json", catalog.to_string())),
        );

        assert!(
            format!("{error:#}").contains("compiler_request"),
            "expected missing compiler_request rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_compile_deploy_job_catalog_sibling_with_wrong_view() {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        let mut catalog: serde_json::Value =
            serde_json::from_str(&compile_deploy_job_catalog_fixture_json(&product)).unwrap();
        catalog["jobs"][0]["view_id"] = serde_json::json!("other_view");

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("view-compile-deploy-jobs.json", catalog.to_string())),
        );

        assert!(
            format!("{error:#}").contains("pending_scores_by_user"),
            "expected wrong job view rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_compile_deploy_job_catalog_sibling_with_wrong_schema() {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        let mut catalog: serde_json::Value =
            serde_json::from_str(&compile_deploy_job_catalog_fixture_json(&product)).unwrap();
        catalog["jobs"][0]["compiler_request"]["input_relations"][0]["columns"][1]["data_type"]
            ["kind"] = serde_json::json!("utf8");

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("view-compile-deploy-jobs.json", catalog.to_string())),
        );

        assert!(
            format!("{error:#}").contains("column score requires kind int64"),
            "expected wrong input schema rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_compile_deploy_claim_without_activation_evidence() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        fs::write(
            dir.path().join("view-compile-deploy-run-once.json"),
            serde_json::json!({
                "pending_jobs": 1,
                "activated": 0,
                "skipped": 1,
                "failed": 0,
                "outcomes": [
                    {
                        "job_id": "pending_scores_by_user:velorix-feldera-spec-sha256-v1:test",
                        "view_id": "pending_scores_by_user",
                        "status": "skipped"
                    }
                ]
            })
            .to_string(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            None,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("activated=1"),
            "expected invalid compile/deploy activation evidence to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_evidence_with_weak_no_pvc_namespace_evidence() {
        for (pointer, replacement, expected) in [
            (
                "/no_pvc/namespace_validated",
                serde_json::json!(false),
                "/no_pvc/namespace_validated",
            ),
            (
                "/no_pvc/evidence",
                serde_json::json!("other.json"),
                "no-PVC namespace evidence",
            ),
            (
                "/no_pvc/contract",
                serde_json::json!("PVCs may exist"),
                "no-PVC contract",
            ),
        ] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            *product.pointer_mut(pointer).unwrap() = replacement;

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(expected),
                "expected invalid {pointer} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_product_evidence_with_weak_ingest_writer_append_evidence() {
        for (pointer, replacement, expected) in [
            (
                "/ingest_writer/pod_internal_append_verified",
                serde_json::json!(false),
                "/ingest_writer/pod_internal_append_verified",
            ),
            (
                "/ingest_writer/evidence_files/job_log",
                serde_json::json!("other.json"),
                "ingest-writer-job-log.json",
            ),
            (
                "/ingest_writer/evidence_files/job",
                serde_json::json!("other.json"),
                "ingest-writer-job.json",
            ),
            (
                "/ingest_writer/evidence_files/pods",
                serde_json::json!("other.json"),
                "ingest-writer-pods.json",
            ),
        ] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            *product.pointer_mut(pointer).unwrap() = replacement;

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(expected),
                "expected invalid {pointer} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_product_evidence_without_referenced_sibling_artifact() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let feldera_hash = dir.path().join("feldera-hash.json");
        let s3_gate = dir.path().join("s3-gate.json");
        let production_gc = dir.path().join("production-gc.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        let product = dir.path().join("product-evidence.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json()).unwrap();
        fs::write(&feldera_hash, feldera_hash_evidence_json()).unwrap();
        fs::write(&s3_gate, s3_release_benchmark_gate_json()).unwrap();
        fs::write(&production_gc, production_gc_run_evidence_json()).unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        fs::write(&product, product_evidence_json("required", true)).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        fs::remove_file(dir.path().join("no-pvc-namespace.json")).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                standing_runtime_product_evidence: Some(product),
                require_release_artifacts: true,
                dependency_governance_evidence: Some(dependency),
                feldera_artifact_hash_evidence: Some(feldera_hash),
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                production_gc_run_evidence: Some(production_gc),
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("no-pvc-namespace.json"));
        assert!(format!("{error:#}").contains("requires sibling evidence file"));
    }

    #[test]
    fn readiness_report_rejects_lifecycle_evidence_without_referenced_sibling_artifact() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        fs::remove_file(dir.path().join("velorix-ingest-lifecycle-handoff-log.json")).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("velorix-ingest-lifecycle-handoff-log.json"));
        assert!(format!("{error:#}").contains("requires sibling evidence file"));
    }

    #[test]
    fn readiness_report_rejects_product_evidence_without_external_s3_identity_fields() {
        for (field, expected) in [
            ("bucket", "/object_store/bucket"),
            ("s3_prefix", "/object_store/s3_prefix"),
            (
                "external_s3_validation_key",
                "/object_store/external_s3_validation_key",
            ),
        ] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            product
                .pointer_mut("/object_store")
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(field);

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(expected),
                "expected missing {field} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_product_evidence_with_mismatched_external_s3_identity() {
        for (field, replacement, expected) in [
            (
                "bucket",
                serde_json::json!("other-bucket"),
                "bucket does not match authority_store_id",
            ),
            (
                "s3_prefix",
                serde_json::json!("sibling-prefix"),
                "s3_prefix does not match authority_store_id",
            ),
            (
                "external_s3_validation_key",
                serde_json::json!("sibling/_velorix_external_s3_validation/product-slice.probe"),
                "external_s3_validation_key is outside the authority prefix",
            ),
        ] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            product
                .pointer_mut("/object_store")
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert(field.to_string(), replacement);

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(expected),
                "expected mismatched {field} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_product_evidence_without_ingest_writer_lifecycle_attestation() {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product
            .pointer_mut("/ingest_writer")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("lifecycle_attestation");

        let error = release_readiness_error_for_product(product);

        assert!(format!("{error:#}").contains("/ingest_writer/lifecycle_attestation/validated"));
    }

    #[test]
    fn readiness_report_rejects_product_evidence_with_untrusted_ingest_writer_lifecycle_attestation(
    ) {
        for (pointer, replacement, expected) in [
            (
                "/ingest_writer/lifecycle_attestation/source",
                serde_json::json!("manual"),
                "script-generated ingest-writer lifecycle attestation",
            ),
            (
                "/ingest_writer/lifecycle_attestation/trusted_for_product_complete",
                serde_json::json!(false),
                "/ingest_writer/lifecycle_attestation/trusted_for_product_complete",
            ),
            (
                "/ingest_writer/lifecycle_attestation/evidence_provenance/pod_internal_job/job_uid",
                serde_json::json!(""),
                "pod_internal_job.job_uid",
            ),
        ] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            *product.pointer_mut(pointer).unwrap() = replacement;

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(expected),
                "expected invalid {pointer} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_without_ingress_tls_auth_attestation() {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product
            .pointer_mut("/api/auth")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("ingress_tls_auth_attestation");

        let error = release_readiness_error_for_product(product);

        assert!(format!("{error:#}").contains("/api/auth/ingress_tls_auth_attestation"));
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_with_local_ingress_tls_auth_attestation() {
        for (pointer, replacement, expected) in [
            (
                "/api/auth/ingress_tls_auth_attestation/endpoint_url",
                serde_json::json!("https://localhost"),
                "endpoint_url must be an external hostname",
            ),
            (
                "/api/auth/ingress_tls_auth_attestation/evidence",
                serde_json::json!("other.json"),
                "ingress/TLS/auth attestation evidence",
            ),
            (
                "/api/auth/ingress_tls_auth_attestation/external_hostname",
                serde_json::json!("velorix-api.default.svc.cluster.local"),
                "external_hostname must be an external hostname",
            ),
            (
                "/api/auth/ingress_tls_auth_attestation/transport_security",
                serde_json::json!("generated-local-self-signed"),
                "transport_security must not describe local-only TLS",
            ),
        ] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            *product.pointer_mut(pointer).unwrap() = replacement;

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(expected),
                "expected invalid {pointer} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_without_bearer_auth_enforcement() {
        for (pointer, replacement, expected) in [
            (
                "/api/auth/mode",
                serde_json::json!("unauthenticated-dev"),
                "requires bearer-token API auth",
            ),
            (
                "/api/auth/ingress_tls_auth_attestation/auth_enforced",
                serde_json::json!(false),
                "/api/auth/ingress_tls_auth_attestation/auth_enforced",
            ),
            (
                "/api/auth/ingress_tls_auth_attestation/data_plane_token_rejected_on_admin_route",
                serde_json::json!(false),
                "/api/auth/ingress_tls_auth_attestation/data_plane_token_rejected_on_admin_route",
            ),
            (
                "/api/auth/ingress_tls_auth_attestation/admin_route_missing_token_rejected",
                serde_json::json!(false),
                "/api/auth/ingress_tls_auth_attestation/admin_route_missing_token_rejected",
            ),
            (
                "/api/auth/ingress_tls_auth_attestation/admin_route_wrong_token_rejected",
                serde_json::json!(false),
                "/api/auth/ingress_tls_auth_attestation/admin_route_wrong_token_rejected",
            ),
            (
                "/api/auth/ingress_tls_auth_attestation/data_plane_token_rejected_on_admin_catalog_route",
                serde_json::json!(false),
                "/api/auth/ingress_tls_auth_attestation/data_plane_token_rejected_on_admin_catalog_route",
            ),
            (
                "/api/auth/ingress_tls_auth_attestation/admin_token_accepted_on_admin_route",
                serde_json::json!(false),
                "/api/auth/ingress_tls_auth_attestation/admin_token_accepted_on_admin_route",
            ),
        ] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            *product.pointer_mut(pointer).unwrap() = replacement;

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(expected),
                "expected invalid {pointer} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_without_deployed_image_evidence() {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product.as_object_mut().unwrap().remove("deployed_images");

        let error = release_readiness_error_for_product(product);

        assert!(
            format!("{error:#}").contains("/deployed_images/velorix-api/image"),
            "expected missing deployed image evidence to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_stale_release_product_ingress_tls_auth_attestation() {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product["api"]["auth"]["ingress_tls_auth_attestation"]["attested_at"] =
            serde_json::json!("1970-01-01T00:00:00Z");

        let error = release_readiness_error_for_product(product);

        assert!(
            format!("{error:#}").contains("ingress/TLS/auth attestation attested_at is older than")
        );
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_with_malformed_ingress_tls_auth_sibling() {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("ingress-tls-auth-attestation.json", "{}\n".to_string())),
        );

        assert!(
            format!("{error:#}").contains(
                "product ingress/TLS/auth evidence sibling ingress-tls-auth-attestation.json missing integer /schema_version"
            ),
            "expected malformed ingress sibling rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_with_mismatched_ingress_tls_auth_sibling()
    {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        let mut sibling = product["api"]["auth"]["ingress_tls_auth_attestation"].clone();
        if let Some(object) = sibling.as_object_mut() {
            object.remove("validated");
            object.remove("evidence");
        }
        sibling["endpoint_url"] = serde_json::json!("https://other.example.com");

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("ingress-tls-auth-attestation.json", sibling.to_string())),
        );

        assert!(
            format!("{error:#}").contains(
                "product ingress/TLS/auth evidence sibling ingress-tls-auth-attestation.json /endpoint_url does not match product evidence /api/auth/ingress_tls_auth_attestation/endpoint_url"
            ),
            "expected mismatched ingress sibling rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_with_local_metadata_authority() {
        for (backend, expected) in [
            ("memory", "requires a production metadata authority backend"),
            ("oss", "requires a production metadata authority backend"),
        ] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            product["metadata_store"]["backend"] = serde_json::json!(backend);
            product["standing_runtime_fencing"]["capability"]["backend_name"] =
                serde_json::json!(backend);

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(expected),
                "expected backend {backend} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_with_unsupported_metadata_authority() {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product["metadata_store"]["backend"] = serde_json::json!("external-raft");
        product["standing_runtime_fencing"]["capability"]["backend_name"] =
            serde_json::json!("external-raft");

        let error = release_readiness_error_for_product(product);

        assert!(format!("{error:#}")
            .contains("supports metadata_store.backend=hiqlite with release attestation"));
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_with_mismatched_metadata_capability_backend(
    ) {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product["standing_runtime_fencing"]["capability"]["backend_name"] =
            serde_json::json!("other-backend");

        let error = release_readiness_error_for_product(product);

        assert!(format!("{error:#}").contains(
            "metadata_store.backend does not match standing-runtime capability backend_name"
        ));
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_with_incomplete_standing_runtime_capability_schema(
    ) {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product["standing_runtime_fencing"]["capability"]
            .as_object_mut()
            .unwrap()
            .remove("owner_scope_kind");

        let error = release_readiness_error_for_product(product);

        assert!(
            format!("{error:#}").contains("standing-runtime capability schema is invalid"),
            "expected typed capability schema failure, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_with_unknown_standing_runtime_capability_field(
    ) {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product["standing_runtime_fencing"]["capability"]["process_clock_fallback_allowed"] =
            serde_json::json!(true);

        let error = release_readiness_error_for_product(product);

        assert!(
            format!("{error:#}").contains("standing-runtime capability schema is invalid"),
            "expected unknown capability field to fail typed schema validation, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_with_unsafe_standing_runtime_capability_bits(
    ) {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product["standing_runtime_fencing"]["capability"]["publish_rejects_scope_mismatch"] =
            serde_json::json!(false);

        let error = release_readiness_error_for_product(product);

        assert!(
            format!("{error:#}").contains("publish_rejects_scope_mismatch"),
            "expected release-safe capability invariant failure, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_hiqlite_product_complete_without_backend_time_attestation() {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();

        let error = release_readiness_error_for_product(product);

        assert!(format!("{error:#}")
            .contains("Hiqlite authority attestation proves topology/no-PVC only"));
    }

    #[test]
    fn canonical_product_evidence_removes_hiqlite_backend_time_summary_with_sorted_json_bytes() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        fs::write(
            &product_path,
            r#"{"z":1,"metadata_store":{"keep":true,"hiqlite_backend_time_attestation":{"b":2}},"a":{"b":2,"a":1}}"#,
        )
        .unwrap();

        let bytes =
            canonical_product_evidence_without_backend_time_attestation_bytes(&product_path)
                .unwrap();

        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            r#"{"a":{"a":1,"b":2},"metadata_store":{"keep":true},"z":1}"#
        );
    }

    #[test]
    fn readiness_report_rejects_diagnostic_hiqlite_backend_time_attestation_without_trusted_provenance(
    ) {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);

        let error = release_readiness_error_for_product(product);

        assert!(
            format!("{error:#}").contains("requires trusted CI provenance"),
            "expected missing trusted provenance to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_keeps_trusted_hiqlite_backend_time_attestation_fail_closed_until_sigstore_verification(
    ) {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("full Sigstore certificate-chain"),
            "expected trusted provenance to remain fail-closed without signature verification, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_without_release_commit() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            None,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("requires --release-commit"),
            "expected trusted provenance without release_commit to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_for_different_release_commit(
    ) {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some("abcdefabcdefabcdefabcdefabcdefabcdefabcd"),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("source_revision does not match release_commit"),
            "expected trusted provenance commit mismatch to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_without_subject_images() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        attestation["trusted_provenance"]
            .as_object_mut()
            .unwrap()
            .remove("subject_images");
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("missing array /subject_images"),
            "expected missing subject_images to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_without_api_subject_image()
    {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        attestation["trusted_provenance"]["subject_images"]
            .as_array_mut()
            .unwrap()
            .retain(|entry| entry["role"] != "velorix-api");
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("missing required role velorix-api"),
            "expected missing velorix-api subject image to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_with_api_subject_image_mismatch(
    ) {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        let api_image = attestation["trusted_provenance"]["subject_images"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["role"] == "velorix-api")
            .unwrap();
        api_image["image_digest"] = serde_json::json!(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains(
                "subject_images velorix-api image_digest does not match product deployed image evidence"
            ),
            "expected velorix-api subject image mismatch to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_with_meta_subject_image_mismatch(
    ) {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        let meta_image = attestation["trusted_provenance"]["subject_images"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["role"] == "velorix-meta")
            .unwrap();
        meta_image["image_digest"] = serde_json::json!(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains(
                "subject_images velorix-meta image_digest does not match product deployed image evidence"
            ),
            "expected velorix-meta subject image mismatch to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_without_ci_identity() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        attestation["trusted_provenance"]
            .as_object_mut()
            .unwrap()
            .remove("ci_identity");
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("missing /ci_identity"),
            "expected missing ci_identity to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_for_wrong_workflow_sha() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        attestation["trusted_provenance"]["ci_identity"]["workflow_sha"] =
            serde_json::json!("abcdefabcdefabcdefabcdefabcdefabcdefabcd");
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("workflow_sha does not match release_commit"),
            "expected workflow_sha mismatch to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_from_unprotected_ref() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        attestation["trusted_provenance"]["ci_identity"]["subject"] =
            serde_json::json!("repo:mrchypark/velorix:ref:refs/heads/feature/product");
        attestation["trusted_provenance"]["ci_identity"]["workflow_ref"] = serde_json::json!(
            "mrchypark/velorix/.github/workflows/release-gate.yml@refs/heads/feature/product"
        );
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("must use refs/heads/main or refs/tags/v*"),
            "expected unprotected workflow ref to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_accepts_trusted_hiqlite_backend_time_attestation_from_release_tag_ref_until_sigstore_verification(
    ) {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        attestation["trusted_provenance"]["ci_identity"]["subject"] =
            serde_json::json!("repo:mrchypark/velorix:ref:refs/tags/v1.0.0");
        attestation["trusted_provenance"]["ci_identity"]["workflow_ref"] = serde_json::json!(
            "mrchypark/velorix/.github/workflows/release-gate.yml@refs/tags/v1.0.0"
        );
        attestation["trusted_provenance"]["signature_bundle"]["certificate_identity"] =
            serde_json::json!("repo:mrchypark/velorix:ref:refs/tags/v1.0.0");
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("full Sigstore certificate-chain"),
            "expected release tag ref to pass ref policy and remain fail-closed at Sigstore verification, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_with_unprotected_sigstore_certificate_ref(
    ) {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        let sigstore_bundle = br#"{}"#;
        attestation["trusted_provenance"]["signature_bundle"]["certificate_identity"] =
            serde_json::json!(
                "https://github.com/mrchypark/velorix/.github/workflows/release-gate.yml@refs/heads/feature/product"
            );
        attestation["trusted_provenance"]["signature_bundle"]["sigstore_bundle_base64"] =
            serde_json::json!(BASE64_STANDARD.encode(sigstore_bundle));
        attestation["trusted_provenance"]["signature_bundle"]["sigstore_bundle_sha256"] =
            serde_json::json!(sha256_digest_of_bytes(sigstore_bundle));
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains(
                "signature_bundle.certificate_identity must use refs/heads/main or refs/tags/v*"
            ),
            "expected unprotected Sigstore certificate ref to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_with_sigstore_certificate_ref_mismatch(
    ) {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        let sigstore_bundle = br#"{}"#;
        attestation["trusted_provenance"]["ci_identity"]["subject"] =
            serde_json::json!("repo:mrchypark/velorix:ref:refs/tags/v1.0.0");
        attestation["trusted_provenance"]["ci_identity"]["workflow_ref"] = serde_json::json!(
            "mrchypark/velorix/.github/workflows/release-gate.yml@refs/tags/v1.0.0"
        );
        attestation["trusted_provenance"]["signature_bundle"]["certificate_identity"] =
            serde_json::json!(
                "https://github.com/mrchypark/velorix/.github/workflows/release-gate.yml@refs/heads/main"
            );
        attestation["trusted_provenance"]["signature_bundle"]["sigstore_bundle_base64"] =
            serde_json::json!(BASE64_STANDARD.encode(sigstore_bundle));
        attestation["trusted_provenance"]["signature_bundle"]["sigstore_bundle_sha256"] =
            serde_json::json!(sha256_digest_of_bytes(sigstore_bundle));
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains(
                "signature_bundle certificate_identity does not match ci_identity workflow_ref"
            ),
            "expected Sigstore certificate/workflow ref mismatch to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_without_signature_bundle()
    {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        attestation["trusted_provenance"]
            .as_object_mut()
            .unwrap()
            .remove("signature_bundle");
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("missing /signature_bundle"),
            "expected missing signature_bundle to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_with_local_failover_smoke()
    {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let failover_path = dir.path().join("standing-runtime-failover-smoke.json");
        let mut failover: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&failover_path).unwrap()).unwrap();
        failover["trusted_for_product_complete"] = serde_json::json!(false);
        failover["production_wall_clock_failover_attestation"] = serde_json::json!(false);
        fs::write(&failover_path, serde_json::to_string(&failover).unwrap()).unwrap();
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        let failover_entry = attestation["evidence_files"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["kind"] == "standing_runtime_failover_smoke")
            .unwrap();
        failover_entry["sha256"] = serde_json::json!(sha256_hex_of_file(&failover_path).unwrap());
        failover_entry["size_bytes"] =
            serde_json::json!(fs::metadata(&failover_path).unwrap().len());
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("/trusted_for_product_complete=true"),
            "expected trusted backend-time evidence to reject local failover smoke, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_with_bad_bundle_digest() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        attestation["trusted_provenance"]["canonical_bundle_sha256"] = serde_json::json!(
            "sha256:2222222222222222222222222222222222222222222222222222222222222222"
        );
        attestation["trusted_provenance"]["signature_bundle"] =
            hiqlite_backend_time_fixture_signature_bundle(
                "sha256:2222222222222222222222222222222222222222222222222222222222222222",
            );
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("canonical_bundle_sha256 mismatch"),
            "expected canonical bundle digest mismatch to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_with_bad_signature() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        attestation["trusted_provenance"]["signature_bundle"]["signature_base64"] =
            serde_json::json!(BASE64_STANDARD.encode([1_u8; 64]));
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("Ed25519 signature verification failed"),
            "expected bad signature to fail cryptographic verification, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_with_bad_sigstore_bundle()
    {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        let sigstore_bundle = br#"{"not":"a sigstore bundle"}"#;
        attestation["trusted_provenance"]["signature_bundle"]["certificate_identity"] =
            serde_json::json!(
                "https://github.com/mrchypark/velorix/.github/workflows/release-gate.yml@refs/heads/main"
            );
        attestation["trusted_provenance"]["signature_bundle"]["sigstore_bundle_base64"] =
            serde_json::json!(BASE64_STANDARD.encode(sigstore_bundle));
        attestation["trusted_provenance"]["signature_bundle"]["sigstore_bundle_sha256"] =
            serde_json::json!(sha256_digest_of_bytes(sigstore_bundle));
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("Sigstore bundle verification failed"),
            "expected bad Sigstore bundle to fail real bundle verification, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_with_bad_sigstore_bundle_digest(
    ) {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        attestation["trusted_provenance"]["signature_bundle"]["certificate_identity"] =
            serde_json::json!(
                "https://github.com/mrchypark/velorix/.github/workflows/release-gate.yml@refs/heads/main"
            );
        attestation["trusted_provenance"]["signature_bundle"]["sigstore_bundle_base64"] =
            serde_json::json!(BASE64_STANDARD.encode(b"{}"));
        attestation["trusted_provenance"]["signature_bundle"]["sigstore_bundle_sha256"] = serde_json::json!(
            "sha256:7777777777777777777777777777777777777777777777777777777777777777"
        );
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}")
                .contains("sigstore_bundle_sha256 does not match sigstore_bundle_base64"),
            "expected bad Sigstore bundle digest to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_trusted_hiqlite_backend_time_attestation_with_image_mismatch() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        trust_hiqlite_backend_time_attestation_for_release(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        attestation["trusted_provenance"]["subject_image_digest"] = serde_json::json!(
            "sha256:3333333333333333333333333333333333333333333333333333333333333333"
        );
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            Some(TEST_RELEASE_COMMIT),
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("subject_image_digest does not match"),
            "expected subject image mismatch to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_unsupported_hiqlite_backend_time_attestation_schema() {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        product["metadata_store"]["hiqlite_backend_time_attestation"] = serde_json::json!({
            "validated": true,
            "evidence": "hiqlite-backend-time-attestation.json",
            "schema_version": 1,
            "evidence_kind": "velorix_hiqlite_backend_time_attestation",
            "time_source_kind": "raft_replicated_authority_time"
        });

        let error = release_readiness_error_for_product(product);

        assert!(format!("{error:#}")
            .contains("/metadata_store/hiqlite_backend_time_attestation/backend_name"));
    }

    #[test]
    fn readiness_report_rejects_hiqlite_backend_time_attestation_using_non_time_sources() {
        for time_source_kind in ["raft_log_index", "hiqlite_metrics", "distributed_lock_ttl"] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            add_valid_hiqlite_backend_time_attestation(&mut product);
            product["metadata_store"]["hiqlite_backend_time_attestation"]["time_source_kind"] =
                serde_json::json!(time_source_kind);

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}")
                    .contains("time_source_kind must be raft_replicated_authority_time"),
                "expected {time_source_kind} backend-time attestation to be rejected, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_hiqlite_backend_time_attestation_with_weak_proof_fields() {
        for (pointer, replacement, expected) in [
            (
                "/metadata_store/hiqlite_backend_time_attestation/evidence",
                serde_json::json!("other.json"),
                "Hiqlite backend-time evidence",
            ),
            (
                "/metadata_store/hiqlite_backend_time_attestation/lease_authority_kind",
                serde_json::json!("hiqlite_raft_serialized"),
                "lease_authority_kind must be raft_replicated_time",
            ),
            (
                "/metadata_store/hiqlite_backend_time_attestation/lease_expiry_semantics",
                serde_json::json!("operation_driven_logical"),
                "lease_expiry_semantics must be backend_wall_clock_ttl",
            ),
            (
                "/metadata_store/hiqlite_backend_time_attestation/authority_sampled_unix_time_ms_in_raft_operation",
                serde_json::json!(false),
                "/metadata_store/hiqlite_backend_time_attestation/authority_sampled_unix_time_ms_in_raft_operation",
            ),
            (
                "/metadata_store/hiqlite_backend_time_attestation/metrics_time_source_rejected",
                serde_json::json!(false),
                "/metadata_store/hiqlite_backend_time_attestation/metrics_time_source_rejected",
            ),
            (
                "/metadata_store/hiqlite_backend_time_attestation/observed_max_failover_ms",
                serde_json::json!(300001),
                "observed_max_failover_ms exceeds failover_time_bound_ms",
            ),
        ] {
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("required", true)).unwrap();
            add_valid_hiqlite_backend_time_attestation(&mut product);
            *product.pointer_mut(pointer).unwrap() = replacement;

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(expected),
                "expected invalid {pointer} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_hiqlite_backend_time_attestation_with_mismatched_sibling_evidence()
    {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        fs::write(
            dir.path().join("hiqlite-backend-time-attestation.json"),
            serde_json::json!({
                "schema_version": 1,
                "evidence_kind": "velorix_hiqlite_backend_time_attestation",
                "backend_name": "hiqlite",
                "time_source_kind": "raft_replicated_authority_time",
                "lease_authority_kind": "raft_replicated_time",
                "lease_expiry_semantics": "backend_wall_clock_ttl",
                "authoritative_backend_time": true,
                "bounded_wall_clock_failover": true,
                "production_bounded_failover_safe": true,
                "authority_sampled_unix_time_ms_in_raft_operation": true,
                "owner_expiry_bound_to_authority_time": true,
                "checkpoint_publish_rejects_expired_owner_with_authority_time": true,
                "bounded_failover_probe_passed": true,
                "failover_time_bound_ms": 300000,
                "observed_max_failover_ms": 300001,
                "metrics_time_source_rejected": true,
                "raft_log_index_time_source_rejected": true,
                "distributed_lock_ttl_source_rejected": true,
                "attested_at": current_rfc3339_utc(),
                "attester": "velorix-release-operator"
            })
            .to_string(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            None,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains(
                "sibling hiqlite-backend-time-attestation.json /observed_max_failover_ms does not match"
            ),
            "expected mismatched backend-time sibling evidence to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_hiqlite_backend_time_attestation_without_referenced_evidence_files()
    {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let mut backend_time: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(dir.path().join("hiqlite-backend-time-attestation.json")).unwrap(),
        )
        .unwrap();
        backend_time
            .as_object_mut()
            .unwrap()
            .remove("evidence_files");
        fs::write(
            dir.path().join("hiqlite-backend-time-attestation.json"),
            serde_json::to_string(&backend_time).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            None,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("missing array /evidence_files"),
            "expected missing backend-time evidence_files to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_uses_canonical_product_evidence_for_hiqlite_backend_time_self_reference() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        product["metadata_store"]["hiqlite_backend_time_attestation"]["release_gate_copy_note"] =
            serde_json::json!("copied after canonical evidence bundle was signed");
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            None,
        )
        .unwrap_err();

        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("requires trusted CI provenance")
                && !rendered.contains("/evidence_files product_evidence sha256 mismatch"),
            "expected canonicalized product evidence to avoid self-reference hash mismatch, got {rendered}"
        );
    }

    #[test]
    fn readiness_report_rejects_hiqlite_backend_time_attestation_with_bad_canonical_product_evidence_hash(
    ) {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        let product_entry = attestation["evidence_files"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["kind"] == "product_evidence")
            .unwrap();
        product_entry["sha256"] =
            serde_json::json!("0000000000000000000000000000000000000000000000000000000000000000");
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            None,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("/evidence_files product_evidence sha256 mismatch"),
            "expected bad canonical product evidence hash to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_hiqlite_backend_time_attestation_without_product_evidence_canonicalization(
    ) {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        let attestation_path = dir.path().join("hiqlite-backend-time-attestation.json");
        let mut attestation: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&attestation_path).unwrap()).unwrap();
        let product_entry = attestation["evidence_files"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|entry| entry["kind"] == "product_evidence")
            .unwrap();
        product_entry
            .as_object_mut()
            .unwrap()
            .remove("canonicalization");
        fs::write(
            &attestation_path,
            serde_json::to_string(&attestation).unwrap(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            None,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains(
                "product_evidence must use canonicalization=without_metadata_store_hiqlite_backend_time_attestation"
            ),
            "expected missing product evidence canonicalization to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_hiqlite_backend_time_attestation_with_stale_evidence_metadata() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        fs::write(
            dir.path().join("multi-replica-fencing-smoke.json"),
            serde_json::json!({
                "schema_version": 1,
                "evidence_kind": "velorix_deployed_multi_replica_fencing_smoke",
                "status": "pass",
                "assertions": {
                    "distinct_api_pods": true,
                    "non_owner_ingest_rejected": true,
                    "owner_retry_converged": true,
                    "read_replica_served_query": true
                },
                "tampered_after_attestation": true
            })
            .to_string(),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            None,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("/evidence_files multi_replica_fencing_smoke")
                && format!("{error:#}").contains("mismatch"),
            "expected stale backend-time evidence metadata to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_stale_hiqlite_backend_time_attestation() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        product["metadata_store"]["hiqlite_backend_time_attestation"]["attested_at"] =
            serde_json::json!("2020-01-01T00:00:00Z");
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            None,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("Hiqlite backend-time attestation attested_at is older"),
            "expected stale backend-time attestation to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_unallowlisted_hiqlite_backend_time_attester() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        add_valid_hiqlite_backend_time_attestation(&mut product);
        product["metadata_store"]["hiqlite_backend_time_attestation"]["attester"] =
            serde_json::json!("developer-laptop");
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            None,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}")
                .contains("Hiqlite backend-time attestation attester is not allowlisted"),
            "expected unallowlisted backend-time attester to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_hiqlite_release_product_evidence_without_authority_attestation() {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product["metadata_store"]
            .as_object_mut()
            .unwrap()
            .remove("hiqlite_authority_attestation");

        let error = release_readiness_error_for_product(product);

        assert!(format!("{error:#}")
            .contains("/metadata_store/hiqlite_authority_attestation/validated"));
    }

    #[test]
    fn readiness_report_rejects_hiqlite_release_product_evidence_with_weak_authority_attestation() {
        for (pointer, replacement, expected) in [
            (
                "/metadata_store/hiqlite_authority_attestation/evidence",
                serde_json::json!("other.json"),
                "Hiqlite authority evidence",
            ),
            (
                "/metadata_store/hiqlite_authority_attestation/metadata_authority_no_pvc_used",
                serde_json::json!(false),
                "/metadata_store/hiqlite_authority_attestation/metadata_authority_no_pvc_used",
            ),
            (
                "/metadata_store/hiqlite_authority_attestation/nodes",
                serde_json::json!(["http://hiqlite-0"]),
                "exactly 3 voter nodes",
            ),
            (
                "/metadata_store/hiqlite_authority_attestation/nodes",
                serde_json::json!([
                    "http://velorix-hiqlite-0.velorix-hiqlite:8200",
                    "http://velorix-hiqlite-0.velorix-hiqlite:8200",
                    "http://velorix-hiqlite-2.velorix-hiqlite:8200"
                ]),
                "unique voter nodes",
            ),
            (
                "/metadata_store/hiqlite_authority_attestation/nodes",
                serde_json::json!([
                    "http://velorix-hiqlite-0.velorix-hiqlite:8200",
                    "http://velorix-hiqlite-1.velorix-hiqlite:8200",
                    "http://velorix-hiqlite-2.velorix-hiqlite:8200",
                    "http://velorix-hiqlite-3.velorix-hiqlite:8200"
                ]),
                "exactly 3 voter nodes",
            ),
            (
                "/metadata_store/hiqlite_authority_attestation/metadata_authority_storage_mode",
                serde_json::json!("pvc"),
                "object-store backup/restore with ephemeral node disk",
            ),
            (
                "/metadata_store/hiqlite_authority_attestation/transport_security",
                serde_json::json!("none"),
                "non-local transport security",
            ),
            (
                "/metadata_store/hiqlite_authority_attestation/attested_at",
                serde_json::json!("not-a-timestamp"),
                "invalid attested_at",
            ),
            (
                "/metadata_store/hiqlite_authority_attestation/no_pvc_evidence_files/namespace_pvc_list",
                serde_json::json!("other.json"),
                "no-pvc-namespace.json",
            ),
            (
                "/metadata_store/hiqlite_authority_attestation/no_pvc_evidence_files/hiqlite_statefulset",
                serde_json::json!("other.json"),
                "no-pvc-hiqlite-statefulset.json",
            ),
            (
                "/metadata_store/hiqlite_authority_attestation/image_digest",
                serde_json::json!(""),
                "managed Hiqlite authority attestation requires sha256 image_digest",
            ),
        ] {
            let mut product = hiqlite_product_evidence_json();
            *product.pointer_mut(pointer).unwrap() = replacement;

            let error = release_readiness_error_for_product(product);

            assert!(
                format!("{error:#}").contains(expected),
                "expected invalid {pointer} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_release_product_evidence_with_pvc_in_no_pvc_namespace_sibling() {
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        let sibling = serde_json::json!({
            "apiVersion": "v1",
            "kind": "List",
            "items": [
                {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "metadata": {
                        "name": "unexpected-pvc"
                    }
                }
            ]
        });

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("no-pvc-namespace.json", sibling.to_string())),
        );

        assert!(
            format!("{error:#}").contains(
                "product no-PVC namespace evidence sibling no-pvc-namespace.json must contain zero PersistentVolumeClaim items"
            ),
            "expected PVC namespace sibling rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_hiqlite_authority_with_mismatched_sibling_evidence() {
        let product = hiqlite_product_evidence_json();
        let mut sibling = product["metadata_store"]["hiqlite_authority_attestation"].clone();
        if let Some(object) = sibling.as_object_mut() {
            object.remove("validated");
            object.remove("evidence");
        }
        sibling["image_digest"] = serde_json::json!(
            "sha256:2222222222222222222222222222222222222222222222222222222222222222"
        );

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("hiqlite-authority-attestation.json", sibling.to_string())),
        );

        assert!(
            format!("{error:#}").contains(
                "product Hiqlite authority evidence sibling hiqlite-authority-attestation.json /image_digest does not match product evidence /metadata_store/hiqlite_authority_attestation/image_digest"
            ),
            "expected mismatched Hiqlite authority sibling rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_managed_hiqlite_authority_with_pvc_statefulset_sibling() {
        let product = hiqlite_product_evidence_json();
        let mut statefulset: serde_json::Value =
            serde_json::from_str(&managed_hiqlite_no_pvc_statefulset_fixture_json()).unwrap();
        statefulset["spec"]["volumeClaimTemplates"] = serde_json::json!([
            {
                "metadata": {"name": "data"},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": "1Gi"}}
                }
            }
        ]);

        let error = release_readiness_error_for_product_with_sibling_override(
            product,
            Some(("no-pvc-hiqlite-statefulset.json", statefulset.to_string())),
        );

        assert!(
            format!("{error:#}").contains(
                "product Hiqlite no-PVC StatefulSet evidence sibling no-pvc-hiqlite-statefulset.json must not define volumeClaimTemplates"
            ),
            "expected managed Hiqlite PVC StatefulSet sibling rejection, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_unsafe_dev_product_evidence_for_first_e2e_artifacts() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let s3_gate = dir.path().join("s3-gate.json");
        let production_gc = dir.path().join("production-gc.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        let product = dir.path().join("product-evidence.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json()).unwrap();
        fs::write(&s3_gate, s3_release_benchmark_gate_json()).unwrap();
        fs::write(&production_gc, production_gc_run_evidence_json()).unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        fs::write(&product, product_evidence_json("unsafe-dev-only", false)).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                standing_runtime_product_evidence: Some(product),
                first_e2e_artifacts: true,
                dependency_governance_evidence: Some(dependency),
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                production_gc_run_evidence: Some(production_gc),
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("uses unsafe-dev-only standing-runtime fencing"));
    }

    #[test]
    fn readiness_report_rejects_first_e2e_product_evidence_without_bearer_auth_smoke() {
        for (pointer, replacement, expected) in [
            (
                "/api/auth/mode",
                serde_json::json!("unauthenticated-dev"),
                "requires bearer-token API auth",
            ),
            (
                "/api/auth/secret_name",
                serde_json::json!("other-secret"),
                "velorix-api-auth",
            ),
            (
                "/api/auth/admin_secret_name",
                serde_json::json!("other-admin-secret"),
                "velorix-admin-auth",
            ),
            (
                "/api/auth/data_plane_token_rejected_on_admin_route",
                serde_json::json!(false),
                "/api/auth/data_plane_token_rejected_on_admin_route",
            ),
            (
                "/api/auth/deployment_env_verified",
                serde_json::json!(false),
                "/api/auth/deployment_env_verified",
            ),
            (
                "/api/auth/local_tls_auth_smoke/passed",
                serde_json::json!(false),
                "/api/auth/local_tls_auth_smoke/passed",
            ),
            (
                "/api/auth/local_tls_auth_smoke/evidence",
                serde_json::json!("other.json"),
                "local TLS/auth smoke evidence",
            ),
            (
                "/api/auth/local_tls_auth_smoke/public_ingress_attestation",
                serde_json::json!(true),
                "/api/auth/local_tls_auth_smoke/public_ingress_attestation",
            ),
        ] {
            let dir = tempdir().unwrap();
            let readiness = dir.path().join("readiness.json");
            let dependency = dir.path().join("dependency.json");
            let s3_gate = dir.path().join("s3-gate.json");
            let production_gc = dir.path().join("production-gc.json");
            let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
            let product_path = dir.path().join("product-evidence.json");
            let mut readiness_json: serde_json::Value =
                serde_json::from_str(&readiness_json()).unwrap();
            readiness_json["feldera_artifact_status"]["evidence_kind"] =
                serde_json::json!(["feldera_artifact_registry"]);
            let mut product: serde_json::Value =
                serde_json::from_str(&product_evidence_json("logical-fencing", false)).unwrap();
            *product.pointer_mut(pointer).unwrap() = replacement;
            fs::write(&readiness, readiness_json.to_string()).unwrap();
            fs::write(&dependency, dependency_governance_evidence_json()).unwrap();
            fs::write(&s3_gate, s3_release_benchmark_gate_json()).unwrap();
            fs::write(&production_gc, production_gc_run_evidence_json()).unwrap();
            fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
            fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
            write_release_evidence_sibling_fixture_files(dir.path());

            let error = read_readiness_report(
                &readiness,
                &ReadinessReleaseArtifactPaths {
                    first_e2e_artifacts: true,
                    dependency_governance_evidence: Some(dependency),
                    s3_release_benchmark_gate_evidence: Some(s3_gate),
                    production_gc_run_evidence: Some(production_gc),
                    ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                    standing_runtime_product_evidence: Some(product_path),
                    ..ReadinessReleaseArtifactPaths::default()
                },
            )
            .unwrap_err();

            assert!(
                format!("{error:#}").contains(expected),
                "expected invalid {pointer} to mention {expected}, got {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_allows_release_commit_without_cargo_vet_dependency_attestation() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json()).unwrap();

        let report = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                dependency_governance_evidence: Some(dependency),
                dependency_governance_manifest: None,
                release_commit: Some("abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string()),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap();

        assert!(report.production_ready);
    }

    #[test]
    fn readiness_report_allows_manifest_path_without_cargo_vet_dependency_attestation() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let dependency_manifest = dir.path().join("dependency-governance.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json()).unwrap();
        fs::write(&dependency_manifest, "different manifest\n").unwrap();

        let report = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                dependency_governance_evidence: Some(dependency),
                dependency_governance_manifest: Some(dependency_manifest),
                release_commit: Some(TEST_RELEASE_COMMIT.to_string()),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap();

        assert!(report.production_ready);
    }

    #[test]
    fn readiness_report_requires_production_gc_artifact_when_release_artifacts_required() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let feldera_hash = dir.path().join("feldera-hash.json");
        let s3_gate = dir.path().join("s3-gate.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json()).unwrap();
        fs::write(&feldera_hash, feldera_hash_evidence_json()).unwrap();
        fs::write(&s3_gate, s3_release_benchmark_gate_json()).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                require_release_artifacts: true,
                dependency_governance_evidence: Some(dependency),
                dependency_governance_manifest: None,
                release_commit: None,
                feldera_artifact_hash_evidence: Some(feldera_hash),
                feldera_release_provenance_evidence: None,
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains(
            "readiness-report --require-release-artifacts requires --production-gc-run-evidence"
        ));
    }

    #[test]
    fn readiness_report_does_not_require_feldera_artifact_hash_when_release_artifacts_required() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let s3_gate = dir.path().join("s3-gate.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json()).unwrap();
        fs::write(&s3_gate, s3_release_benchmark_gate_json()).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                require_release_artifacts: true,
                dependency_governance_evidence: Some(dependency),
                dependency_governance_manifest: None,
                release_commit: None,
                feldera_artifact_hash_evidence: None,
                feldera_release_provenance_evidence: None,
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(
            !format!("{error:#}").contains("--feldera-artifact-hash-evidence"),
            "Feldera artifact hash is optional for release artifacts: {error:#}"
        );
        assert!(format!("{error:#}").contains(
            "readiness-report --require-release-artifacts requires --production-gc-run-evidence"
        ));
    }

    #[test]
    fn readiness_report_requires_rustfs_production_gc_validation_for_first_e2e_artifacts() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let s3_gate = dir.path().join("s3-gate.json");
        let production_gc = dir.path().join("production-gc.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        let product = dir.path().join("product-evidence.json");
        let mut readiness_json: serde_json::Value =
            serde_json::from_str(&readiness_json()).unwrap();
        readiness_json["feldera_artifact_status"]["evidence_kind"] =
            serde_json::json!(["feldera_artifact_registry"]);
        fs::write(&readiness, readiness_json.to_string()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json()).unwrap();
        fs::write(&s3_gate, s3_release_benchmark_gate_json()).unwrap();
        fs::write(&production_gc, production_gc_run_evidence_json()).unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        fs::write(&product, product_evidence_json("logical-fencing", false)).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                first_e2e_artifacts: true,
                dependency_governance_evidence: Some(dependency),
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                production_gc_run_evidence: Some(production_gc),
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                standing_runtime_product_evidence: Some(product),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains(
            "readiness-report --first-e2e-artifacts requires --rustfs-production-gc-validation-evidence"
        ));
    }

    #[test]
    fn readiness_report_rejects_local_production_gc_evidence_artifact() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let production_gc = dir.path().join("production-gc.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(
            &production_gc,
            production_gc_run_evidence_json().replace("s3://velorix-prod", "file:///tmp/velorix"),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                production_gc_run_evidence: Some(production_gc),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("local/dev authority_store_id"));
    }

    #[test]
    fn readiness_report_rejects_cross_deployment_production_gc_evidence_artifact() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let production_gc = dir.path().join("production-gc.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(
            &production_gc,
            production_gc_run_evidence_json().replace("prod-a", "prod-b"),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                production_gc_run_evidence: Some(production_gc),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("deployment_id does not match"));
    }

    #[test]
    fn readiness_report_rejects_production_gc_evidence_without_gc_transition_check() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let production_gc = dir.path().join("production-gc.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(
            &production_gc,
            production_gc_run_evidence_json().replace(
                r#""checkpoint_gc_transition_records_checked":true"#,
                r#""checkpoint_gc_transition_records_checked":false"#,
            ),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                production_gc_run_evidence: Some(production_gc),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("did not check checkpoint GC transition records"));
    }

    #[test]
    fn readiness_report_rejects_pre_digest_production_gc_evidence_artifact() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let production_gc = dir.path().join("production-gc.json");
        let mut artifact: serde_json::Value =
            serde_json::from_str(&production_gc_run_evidence_json()).unwrap();
        artifact
            .as_object_mut()
            .unwrap()
            .remove("verified_gc_run_digest");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&production_gc, serde_json::to_string(&artifact).unwrap()).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                production_gc_run_evidence: Some(production_gc),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("missing verified_gc_run_digest"));
    }

    #[test]
    fn readiness_report_accepts_ingest_writer_lifecycle_release_evidence() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let report = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap();

        assert!(report.production_ready);
    }

    #[test]
    fn readiness_report_rejects_cross_deployment_ingest_writer_lifecycle_evidence() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(
            &ingest_lifecycle,
            ingest_writer_lifecycle_evidence_json().replace("prod-a", "prod-b"),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("deployment_id does not match"));
    }

    #[test]
    fn readiness_report_rejects_local_ingest_writer_lifecycle_authority() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(
            &ingest_lifecycle,
            ingest_writer_lifecycle_evidence_json()
                .replace("s3://velorix-prod", "file:///tmp/velorix"),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("local/dev authority_store_id"));
    }

    #[test]
    fn readiness_report_rejects_incomplete_ingest_writer_lifecycle_evidence() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(
            &ingest_lifecycle,
            ingest_writer_lifecycle_evidence_json().replace(
                r#""kubernetes_lease_handoff_checked":true"#,
                r#""kubernetes_lease_handoff_checked":false"#,
            ),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("kubernetes_lease_handoff_checked=true"));
    }

    #[test]
    fn readiness_report_rejects_lifecycle_evidence_without_lease_held_through_append() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(
            &ingest_lifecycle,
            ingest_writer_lifecycle_evidence_json().replace(
                r#""lease_held_through_append_checked":true"#,
                r#""lease_held_through_append_checked":false"#,
            ),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("lease_held_through_append_checked=true"));
    }

    #[test]
    fn readiness_report_rejects_lifecycle_evidence_without_admission_commit_guard_binding() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(
            &ingest_lifecycle,
            ingest_writer_lifecycle_evidence_json().replace(
                r#""admission_commit_guard_bound_checked":true"#,
                r#""admission_commit_guard_bound_checked":false"#,
            ),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("admission_commit_guard_bound_checked=true"));
    }

    #[test]
    fn readiness_report_rejects_lifecycle_evidence_without_lease_loss_during_reservation() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(
            &ingest_lifecycle,
            ingest_writer_lifecycle_evidence_json().replace(
                r#""lease_loss_during_reservation_checked":true"#,
                r#""lease_loss_during_reservation_checked":false"#,
            ),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("lease_loss_during_reservation_checked=true"));
    }

    #[test]
    fn readiness_report_rejects_lifecycle_evidence_without_job_pod_provenance() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        let mut artifact: serde_json::Value =
            serde_json::from_str(&ingest_writer_lifecycle_evidence_json()).unwrap();
        artifact
            .as_object_mut()
            .unwrap()
            .remove("evidence_provenance");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&ingest_lifecycle, artifact.to_string()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("evidence_provenance.pod_internal_job"));
    }

    #[test]
    fn readiness_report_rejects_lifecycle_evidence_without_job_log_manifest() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        let mut artifact: serde_json::Value =
            serde_json::from_str(&ingest_writer_lifecycle_evidence_json()).unwrap();
        artifact
            .pointer_mut("/evidence_files")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("handoff_probe_job");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&ingest_lifecycle, artifact.to_string()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("evidence_files.handoff_probe_job"));
    }

    #[test]
    fn readiness_report_rejects_product_lifecycle_attestation_without_job_log_manifest() {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        *product
            .pointer_mut("/ingest_writer/lifecycle_attestation/evidence_files/pod_internal_job")
            .unwrap() = serde_json::json!("other-log.json");

        let error = release_readiness_error_for_product(product);

        assert!(
            format!("{error:#}").contains("velorix-ingest-writer-smoke-log.json"),
            "expected mismatched lifecycle evidence file to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_lifecycle_attestation_without_sibling_job_log() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        fs::remove_file(dir.path().join("velorix-ingest-lifecycle-handoff-log.json")).unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            None,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains(
                "product ingest-writer lifecycle evidence requires sibling evidence file"
            ),
            "expected missing product lifecycle sibling evidence file to fail, got {error:#}"
        );
    }

    #[test]
    fn first_e2e_rejects_product_evidence_without_local_api_pod_failover_smoke() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("logical-fencing", false)).unwrap();
        product
            .pointer_mut("/standing_runtime_fencing")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("local_api_pod_failover_smoke");
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::FirstE2e,
            None,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("local_api_pod_failover_smoke"),
            "expected missing local API pod failover smoke to fail, got {error:#}"
        );
    }

    #[test]
    fn first_e2e_accepts_local_development_object_store_without_durability_attestation() {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("logical-fencing", false)).unwrap();
        product["object_store"]["local_development_authority"] = serde_json::json!(true);
        product
            .pointer_mut("/object_store")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("durability_policy_attestation");
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());

        validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::FirstE2e,
            None,
        )
        .unwrap();
    }

    #[test]
    fn readiness_report_rejects_lifecycle_evidence_when_pod_internal_log_is_not_guarded_append() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        fs::write(
            dir.path().join("velorix-ingest-writer-smoke-log.json"),
            r#"{"evidence_kind":"other","status":"pass","outcome":"appended"}"#,
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("ingest_writer_lease_guarded_append_probe"),
            "expected invalid pod-internal lifecycle sibling evidence to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_lifecycle_evidence_when_overlap_log_has_no_conflict_text() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        fs::write(
            dir.path().join("velorix-ingest-lifecycle-overlap-log.json"),
            lifecycle_overlap_fixture_json().replace(
                r#""conflict_log_observed":true"#,
                r#""conflict_log_observed":false"#,
            ),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("/conflict_log_observed=true"),
            "expected missing overlap conflict lifecycle sibling evidence to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_lifecycle_evidence_when_overlap_log_is_only_text() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        fs::write(
            dir.path().join("velorix-ingest-lifecycle-overlap-log.json"),
            "fresh append outcome, got conflict\n",
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("failed to parse sibling evidence"),
            "expected raw text overlap lifecycle sibling evidence to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_lifecycle_evidence_when_handoff_log_missing_stale_owner_rejection()
    {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        fs::write(
            dir.path().join("velorix-ingest-lifecycle-handoff-log.json"),
            lifecycle_handoff_fixture_json().replace(
                r#""stale_owner_rejected":true"#,
                r#""stale_owner_rejected":false"#,
            ),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("/stale_owner_rejected=true"),
            "expected missing stale-owner rejection lifecycle sibling evidence to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_product_lifecycle_attestation_when_sibling_logs_do_not_support_claims(
    ) {
        let dir = tempdir().unwrap();
        let product_path = dir.path().join("product-evidence.json");
        let product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        fs::write(
            dir.path().join("velorix-ingest-lifecycle-restart-log.json"),
            lifecycle_restart_fixture_json().replace(
                r#""recovered_append_completed":true"#,
                r#""recovered_append_completed":false"#,
            ),
        )
        .unwrap();

        let error = validate_standing_runtime_product_evidence_artifact(
            &product_path,
            "prod-a",
            "s3://velorix-prod",
            StandingRuntimeProductEvidenceMode::Release,
            None,
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("/recovered_append_completed=true"),
            "expected unsupported product lifecycle sibling evidence to fail, got {error:#}"
        );
    }

    #[test]
    fn readiness_report_rejects_stale_ingest_writer_lifecycle_evidence() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        let mut artifact: serde_json::Value =
            serde_json::from_str(&ingest_writer_lifecycle_evidence_json()).unwrap();
        artifact["attested_at"] = serde_json::json!("1970-01-01T00:00:00Z");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&ingest_lifecycle, artifact.to_string()).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("attested_at is older than"));
    }

    #[test]
    fn readiness_report_rejects_generic_local_s3_artifact_as_s3_release_benchmark_evidence() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let s3_gate = dir.path().join("local-s3-gate.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(
            &s3_gate,
            serde_json::json!({
                "schema_version": 1,
                "evidence_kind": "local_s3_compatible_gate",
                "readiness_evidence_kind": [
                    "s3_compatible",
                    "s3_compatible_integration_harness"
                ],
                "scope": "local S3-compatible emulator evidence"
            })
            .to_string(),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("local-scoped evidence"));
    }

    #[test]
    fn readiness_release_artifact_filter_accepts_kubernetes_vind_gate_evidence() {
        reject_local_readiness_artifact(
            "kubernetes_vind_gate",
            Path::new("target/velorix-k8s/vind-k8s-gate-evidence.json"),
        )
        .unwrap();
    }

    #[test]
    fn readiness_release_artifact_filter_accepts_rustfs_gate_evidence() {
        reject_local_readiness_artifact(
            "rustfs_s3_compatible_gate",
            Path::new("target/velorix-s3/rustfs-s3-gate-evidence.json"),
        )
        .unwrap();
    }

    #[test]
    fn readiness_report_accepts_rustfs_named_s3_release_benchmark_evidence() {
        let dir = tempdir().unwrap();
        let s3_gate = dir.path().join("rustfs-s3-release.json");
        let mut artifact: serde_json::Value =
            serde_json::from_str(&s3_release_benchmark_gate_json()).unwrap();
        artifact["scope"] =
            serde_json::json!("RustFS S3-compatible live evidence through the configured S3 API");
        fs::write(&s3_gate, artifact.to_string()).unwrap();

        validate_s3_release_benchmark_gate_evidence_artifact(&s3_gate).unwrap();
    }

    #[test]
    fn readiness_report_rejects_nightly_s3_artifact_as_release_evidence() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let s3_gate = dir.path().join("s3-nightly.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(
            &s3_gate,
            s3_release_benchmark_gate_json().replace("\"release\"", "\"nightly_integration\""),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("not release level"));
    }

    #[test]
    fn readiness_report_rejects_local_emulator_s3_release_evidence() {
        let dir = tempdir().unwrap();
        let s3_gate = dir.path().join("s3-release-local-emulator.json");
        let mut artifact: serde_json::Value =
            serde_json::from_str(&s3_release_benchmark_gate_json()).unwrap();
        artifact["backend_evidence_scope"] = serde_json::json!("local_emulator");
        fs::write(&s3_gate, artifact.to_string()).unwrap();

        let error = validate_s3_release_benchmark_gate_evidence_artifact(&s3_gate).unwrap_err();

        assert!(format!("{error:#}").contains("local emulator scope"));
    }

    #[test]
    fn readiness_report_rejects_s3_release_evidence_missing_evidence_scope() {
        let dir = tempdir().unwrap();
        let s3_gate = dir.path().join("s3-release-missing-scope.json");
        let mut artifact: serde_json::Value =
            serde_json::from_str(&s3_release_benchmark_gate_json()).unwrap();
        artifact
            .as_object_mut()
            .unwrap()
            .remove("backend_evidence_scope");
        fs::write(&s3_gate, artifact.to_string()).unwrap();

        let error = validate_s3_release_benchmark_gate_evidence_artifact(&s3_gate).unwrap_err();

        assert!(format!("{error:#}").contains("missing backend_evidence_scope"));
    }

    #[test]
    fn readiness_report_rejects_s3_release_evidence_missing_required_workload_metric() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let s3_gate = dir.path().join("s3-gate.json");
        let mut artifact: serde_json::Value =
            serde_json::from_str(&s3_release_benchmark_gate_json()).unwrap();
        artifact
            .get_mut("workload_metrics")
            .and_then(|metrics| metrics.as_array_mut())
            .unwrap()
            .retain(|metric| metric.as_str() != Some("checkpoint_recovery"));
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&s3_gate, artifact.to_string()).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("missing required S3 workload_metrics"));
    }

    #[test]
    fn readiness_report_rejects_mismatched_feldera_release_provenance() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let feldera_hash = dir.path().join("feldera-hash.json");
        let feldera_provenance = dir.path().join("feldera-provenance.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&feldera_hash, feldera_hash_evidence_json()).unwrap();
        fs::write(
            &feldera_provenance,
            feldera_provenance_evidence_json().replace(
                "feldera-artifact-orders-by-region-20260503",
                "different-artifact",
            ),
        )
        .unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                feldera_artifact_hash_evidence: Some(feldera_hash),
                feldera_release_provenance_evidence: Some(feldera_provenance),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("do not describe the same artifact"));
    }

    #[test]
    fn readiness_report_rejects_feldera_release_provenance_for_different_commit() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let feldera_hash = dir.path().join("feldera-hash.json");
        let feldera_provenance = dir.path().join("feldera-provenance.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&feldera_hash, feldera_hash_evidence_json()).unwrap();
        fs::write(&feldera_provenance, feldera_provenance_evidence_json()).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                release_commit: Some("abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string()),
                feldera_artifact_hash_evidence: Some(feldera_hash),
                feldera_release_provenance_evidence: Some(feldera_provenance),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("source_revision does not match release commit"));
    }

    #[test]
    fn readiness_report_rejects_feldera_release_provenance_placeholder_source_revision() {
        for source_revision in ["", "placeholder-feldera-release-commit"] {
            let dir = tempdir().unwrap();
            let readiness = dir.path().join("readiness.json");
            let feldera_hash = dir.path().join("feldera-hash.json");
            let feldera_provenance = dir.path().join("feldera-provenance.json");
            let mut provenance_json: serde_json::Value =
                serde_json::from_str(&feldera_provenance_evidence_json()).unwrap();
            provenance_json["source_revision"] = serde_json::json!(source_revision);
            fs::write(&readiness, readiness_json()).unwrap();
            fs::write(&feldera_hash, feldera_hash_evidence_json()).unwrap();
            fs::write(&feldera_provenance, provenance_json.to_string()).unwrap();

            let error = read_readiness_report(
                &readiness,
                &ReadinessReleaseArtifactPaths {
                    release_commit: Some(TEST_RELEASE_COMMIT.to_string()),
                    feldera_artifact_hash_evidence: Some(feldera_hash),
                    feldera_release_provenance_evidence: Some(feldera_provenance),
                    ..ReadinessReleaseArtifactPaths::default()
                },
            )
            .unwrap_err();

            assert!(
                format!("{error:#}").contains("placeholder source_revision"),
                "source_revision={source_revision:?} produced unexpected error: {error:#}"
            );
        }
    }

    #[test]
    fn readiness_report_rejects_wrong_feldera_hash_evidence_kind() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let feldera_hash = dir.path().join("feldera-hash.json");
        let feldera_provenance = dir.path().join("feldera-provenance.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(
            &feldera_hash,
            feldera_hash_evidence_json().replace(
                "feldera_artifact_hash_verified",
                "feldera_artifact_release_provenance",
            ),
        )
        .unwrap();
        fs::write(&feldera_provenance, feldera_provenance_evidence_json()).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                feldera_artifact_hash_evidence: Some(feldera_hash),
                feldera_release_provenance_evidence: Some(feldera_provenance),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("expected feldera_artifact_hash_verified"));
    }

    #[test]
    fn dependency_governance_validator_accepts_valid_manifest() {
        validate_dependency_governance_manifest_text(
            &valid_dependency_governance_json(),
            "2026-05-06",
        )
        .unwrap();
    }

    #[test]
    fn dependency_governance_validator_accepts_matching_cargo_deny_warnings() {
        validate_dependency_governance_with_diagnostics_text(
            &valid_dependency_governance_json(),
            cargo_deny_warning_diagnostics_jsonl(),
            "2026-05-06",
        )
        .unwrap();
    }

    #[test]
    fn dependency_governance_validator_emits_stable_readiness_evidence() {
        let evidence = build_dependency_governance_evidence(
            Path::new("dependency-governance.json"),
            &valid_dependency_governance_json(),
            Some((
                Path::new("target/dependency-governance/cargo-deny.jsonl"),
                cargo_deny_warning_diagnostics_jsonl().to_string(),
            )),
            "2026-05-06",
        )
        .unwrap();

        assert_eq!(evidence.schema_version, 1);
        assert_eq!(evidence.status, "pass");
        assert_eq!(evidence.evidence_kind, "dependency_governance_validated");
        assert_eq!(evidence.manifest.path, "dependency-governance.json");
        assert_eq!(evidence.manifest.name, "dependency-governance.json");
        assert!(evidence.cargo_deny.diagnostics_checked);
        assert_eq!(
            evidence.cargo_deny.diagnostics_path.as_deref(),
            Some("target/dependency-governance/cargo-deny.jsonl")
        );
        assert_eq!(
            evidence.required_package_review_subjects,
            vec![
                "datafusion",
                "object_store",
                "kube",
                "k8s-openapi",
                "slatedb",
                "foyer",
                "hiqlite",
                "feldera_artifacts"
            ]
        );
        assert_eq!(
            evidence.reviewed_package_subjects,
            vec![
                "datafusion",
                "object_store",
                "kube",
                "k8s-openapi",
                "slatedb",
                "foyer",
                "hiqlite",
                "feldera_artifacts"
            ]
        );
        assert!(evidence.missing_required_package_review_subjects.is_empty());
        assert_eq!(evidence.exception_counts_by_kind["duplicate"], 1);
        assert_eq!(evidence.exception_counts_by_kind["unmaintained"], 1);
        assert_eq!(evidence.warning_counts_by_kind["duplicate"], 1);
        assert_eq!(evidence.warning_counts_by_kind["unmaintained"], 1);
        assert!(!evidence.external_audit_attestation);
    }

    #[test]
    fn dependency_governance_validator_rejects_uncovered_cargo_deny_warning() {
        let manifest = dependency_governance_json_with_exceptions([("duplicate", "hashbrown")]);

        let error = validate_dependency_governance_with_diagnostics_text(
            &manifest,
            cargo_deny_warning_diagnostics_jsonl(),
            "2026-05-06",
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("uncovered unmaintained warning for paste"));
    }

    #[test]
    fn dependency_governance_validator_rejects_stale_warning_exception() {
        let diagnostics = r#"{"type":"diagnostic","fields":{"code":"duplicate","severity":"warning","message":"found duplicate entries for crate 'hashbrown'","graphs":[{"Krate":{"name":"hashbrown","version":"0.15.5"}},{"Krate":{"name":"hashbrown","version":"0.16.0"}}]}}"#;

        let error = validate_dependency_governance_with_diagnostics_text(
            &valid_dependency_governance_json(),
            diagnostics,
            "2026-05-06",
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("stale unmaintained exception for paste"));
    }

    #[test]
    fn dependency_governance_validator_rejects_known_warning_without_crate_name() {
        let duplicate_without_graphs = r#"{"type":"diagnostic","fields":{"code":"duplicate","severity":"warning","message":"found duplicate entries"}}"#;
        let error = validate_dependency_governance_with_diagnostics_text(
            &valid_dependency_governance_json(),
            duplicate_without_graphs,
            "2026-05-06",
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("did not include crate names"));

        let unmaintained_without_name = r#"{"type":"diagnostic","fields":{"code":"unmaintained","severity":"warning","message":"no delimiter here","labels":[]}}"#;
        let error = validate_dependency_governance_with_diagnostics_text(
            &valid_dependency_governance_json(),
            unmaintained_without_name,
            "2026-05-06",
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("did not include a crate name"));
    }

    #[test]
    fn dependency_governance_validator_rejects_missing_owner() {
        let manifest = serde_json::json!({
            "schema_version": 1,
            "msrv": {
                "minimum_rust_version": "1.85.0",
                "policy": "CI and package updates keep the workspace buildable on the declared MSRV."
            },
            "package_reviews": valid_package_reviews_json(),
            "exceptions": [
                {
                    "kind": "duplicate",
                    "crate": "hashbrown",
                    "expires_on": "2026-06-30",
                    "reason": "Await upstream convergence.",
                    "replacement_plan": "Upgrade when upstream dependency ranges converge.",
                    "promotion_rule": "deny_after_expiry_or_renew_with_owner_and_updated_plan"
                }
            ]
        })
        .to_string();

        let error =
            validate_dependency_governance_manifest_text(&manifest, "2026-05-06").unwrap_err();

        assert!(format!("{error:#}").contains("missing owner"));
    }

    #[test]
    fn dependency_governance_validator_rejects_expired_exception() {
        let manifest = serde_json::json!({
            "schema_version": 1,
            "msrv": {
                "minimum_rust_version": "1.85.0",
                "policy": "CI and package updates keep the workspace buildable on the declared MSRV."
            },
            "package_reviews": valid_package_reviews_json(),
            "exceptions": [
                {
                    "kind": "unmaintained",
                    "crate": "paste",
                    "owner": "runtime",
                    "expires_on": "2026-05-05",
                    "reason": "Temporary allowance while replacing the transitive dependency.",
                    "replacement_plan": "Replace or remove the transitive dependency.",
                    "promotion_rule": "deny_after_expiry_or_renew_with_owner_and_updated_plan"
                }
            ]
        })
        .to_string();

        let error =
            validate_dependency_governance_manifest_text(&manifest, "2026-05-06").unwrap_err();

        assert!(format!("{error:#}").contains("expired"));
    }

    #[test]
    fn dependency_governance_validator_rejects_duplicate_exception() {
        let manifest = serde_json::json!({
            "schema_version": 1,
            "msrv": {
                "minimum_rust_version": "1.85.0",
                "policy": "CI and package updates keep the workspace buildable on the declared MSRV."
            },
            "package_reviews": valid_package_reviews_json(),
            "exceptions": [
                {
                    "kind": "duplicate",
                    "crate": "hashbrown",
                    "owner": "runtime",
                    "expires_on": "2026-06-30",
                    "reason": "Await upstream convergence.",
                    "replacement_plan": "Upgrade when upstream dependency ranges converge.",
                    "promotion_rule": "deny_after_expiry_or_renew_with_owner_and_updated_plan"
                },
                {
                    "kind": "duplicate",
                    "crate": "hashbrown",
                    "owner": "security",
                    "expires_on": "2026-07-31",
                    "reason": "Second owner should not hide the first exception.",
                    "replacement_plan": "Different duplicate plan.",
                    "promotion_rule": "deny_after_expiry_or_renew_with_owner_and_updated_plan"
                }
            ]
        })
        .to_string();

        let error =
            validate_dependency_governance_manifest_text(&manifest, "2026-05-06").unwrap_err();

        assert!(format!("{error:#}")
            .contains("duplicate dependency governance exception for duplicate hashbrown"));
    }

    #[test]
    fn dependency_governance_validator_rejects_invalid_expiry_date() {
        let manifest = serde_json::json!({
            "schema_version": 1,
            "msrv": {
                "minimum_rust_version": "1.85.0",
                "policy": "CI and package updates keep the workspace buildable on the declared MSRV."
            },
            "package_reviews": valid_package_reviews_json(),
            "exceptions": [
                {
                    "kind": "advisory",
                    "crate": "example",
                    "owner": "security",
                    "expires_on": "2026-02-30",
                    "reason": "Temporary advisory exception.",
                    "replacement_plan": "Patch or replace before promotion.",
                    "promotion_rule": "deny_after_expiry_or_renew_with_owner_and_updated_plan"
                }
            ]
        })
        .to_string();

        let error =
            validate_dependency_governance_manifest_text(&manifest, "2026-02-01").unwrap_err();

        assert!(format!("{error:#}").contains("day is out of range"));
    }

    #[test]
    fn dependency_governance_validator_rejects_misspelled_exceptions_key() {
        let manifest = serde_json::json!({
            "schema_version": 1,
            "msrv": {
                "minimum_rust_version": "1.85.0",
                "policy": "CI and package updates keep the workspace buildable on the declared MSRV."
            },
            "package_reviews": valid_package_reviews_json(),
            "exception": []
        })
        .to_string();

        let error =
            validate_dependency_governance_manifest_text(&manifest, "2026-05-06").unwrap_err();

        assert!(format!("{error:#}").contains("unknown field"));
    }

    #[test]
    fn dependency_governance_validator_accepts_leap_day() {
        let manifest = serde_json::json!({
            "schema_version": 1,
            "msrv": {
                "minimum_rust_version": "1.85.0",
                "policy": "CI and package updates keep the workspace buildable on the declared MSRV."
            },
            "package_reviews": valid_package_reviews_json(),
            "exceptions": [
                {
                    "kind": "advisory",
                    "crate": "example",
                    "owner": "security",
                    "expires_on": "2028-02-29",
                    "reason": "Temporary advisory exception.",
                    "replacement_plan": "Patch or replace before promotion.",
                    "promotion_rule": "deny_after_expiry_or_renew_with_owner_and_updated_plan"
                }
            ]
        })
        .to_string();

        validate_dependency_governance_manifest_text(&manifest, "2028-02-01").unwrap();
    }

    #[test]
    fn dependency_governance_validator_rejects_missing_required_package_review() {
        let mut manifest: serde_json::Value =
            serde_json::from_str(&valid_dependency_governance_json()).unwrap();
        manifest["package_reviews"]
            .as_array_mut()
            .unwrap()
            .retain(|review| review["subject"] != "datafusion");

        let error = validate_dependency_governance_manifest_text(
            &serde_json::to_string(&manifest).unwrap(),
            "2026-05-06",
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("missing required package review for datafusion"));
    }

    #[test]
    fn dependency_governance_validator_rejects_duplicate_package_review_subject() {
        let mut manifest: serde_json::Value =
            serde_json::from_str(&valid_dependency_governance_json()).unwrap();
        let reviews = manifest["package_reviews"].as_array_mut().unwrap();
        reviews.push(reviews[0].clone());

        let error = validate_dependency_governance_manifest_text(
            &serde_json::to_string(&manifest).unwrap(),
            "2026-05-06",
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("duplicate package review subject datafusion"));
    }

    #[test]
    fn dependency_governance_validator_rejects_missing_exception_replacement_plan() {
        let mut manifest: serde_json::Value =
            serde_json::from_str(&valid_dependency_governance_json()).unwrap();
        manifest["exceptions"][0]
            .as_object_mut()
            .unwrap()
            .remove("replacement_plan");

        let error = validate_dependency_governance_manifest_text(
            &serde_json::to_string(&manifest).unwrap(),
            "2026-05-06",
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("missing replacement_plan"));
    }

    #[test]
    fn dependency_governance_validator_rejects_missing_exception_promotion_rule() {
        let mut manifest: serde_json::Value =
            serde_json::from_str(&valid_dependency_governance_json()).unwrap();
        manifest["exceptions"][0]
            .as_object_mut()
            .unwrap()
            .remove("promotion_rule");

        let error = validate_dependency_governance_manifest_text(
            &serde_json::to_string(&manifest).unwrap(),
            "2026-05-06",
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("missing promotion_rule"));
    }

    #[test]
    fn dependency_governance_validator_rejects_invalid_package_review_date() {
        let mut manifest: serde_json::Value =
            serde_json::from_str(&valid_dependency_governance_json()).unwrap();
        manifest["package_reviews"][0]["reviewed_on"] = serde_json::json!("2026-02-30");

        let error = validate_dependency_governance_manifest_text(
            &serde_json::to_string(&manifest).unwrap(),
            "2026-05-06",
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("invalid reviewed_on"));
    }

    #[test]
    fn release_status_validator_accepts_complete_required_rows() {
        validate_release_status_text(&release_status_matrix("complete", &[], &[])).unwrap();
    }

    #[test]
    fn release_status_validator_rejects_partial_status() {
        let error = validate_release_status_text(&release_status_matrix(
            "complete",
            &[],
            &[("ingest", "partial")],
        ))
        .unwrap_err();

        assert!(format!("{error:#}").contains("ingest status is partial"));
    }

    #[test]
    fn release_status_validator_rejects_complete_status_with_blocking_tasks() {
        let error = validate_release_status_text(&release_status_matrix_with_blocking_tasks(
            "complete",
            &[],
            &[],
            &[("ingest", "add live production evidence")],
        ))
        .unwrap_err();

        assert!(format!("{error:#}")
            .contains("ingest blocking tasks are add live production evidence, expected none"));
    }

    #[test]
    fn release_status_validator_rejects_missing_required_row() {
        let error = validate_release_status_text(&release_status_matrix("complete", &["GC"], &[]))
            .unwrap_err();

        assert!(format!("{error:#}").contains("missing required release status row: GC"));
    }

    #[test]
    fn release_status_validator_rejects_duplicate_contract() {
        let mut matrix = release_status_matrix("complete", &[], &[]);
        matrix.push_str(
            "\n| ingest | duplicate evidence | duplicate required evidence | complete | none |\n",
        );

        let error = validate_release_status_text(&matrix).unwrap_err();

        assert!(format!("{error:#}").contains("duplicate release status row: ingest"));
    }

    #[test]
    fn release_status_validator_rejects_unexpected_row() {
        let mut matrix = release_status_matrix("complete", &[], &[]);
        matrix.push_str("\n| extra contract | evidence | required evidence | complete | none |\n");

        let error = validate_release_status_text(&matrix).unwrap_err();

        assert!(format!("{error:#}").contains("unexpected release status row: extra contract"));
    }

    #[test]
    fn release_status_validator_rejects_malformed_rows() {
        let mut matrix = release_status_matrix("complete", &[], &[]);
        matrix.push_str(
            "\n| unexpected | evidence with | extra pipe | complete | none | trailing |\n",
        );

        let error = validate_release_status_text(&matrix).unwrap_err();

        assert!(format!("{error:#}").contains("malformed release status table row"));
    }

    #[test]
    fn benchmark_gate_comparison_accepts_result_within_budget() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, valid_result_json()).unwrap();
        fs::write(&result, valid_result_json()).unwrap();

        run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::PrSmoke),
            Some(BenchmarkBackend::Local),
            Some(0.10),
        )
        .unwrap();
    }

    #[test]
    fn benchmark_gate_comparison_rejects_result_over_budget() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, valid_result_json()).unwrap();
        fs::write(&result, regressed_result_json()).unwrap();

        let error = run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::PrSmoke),
            Some(BenchmarkBackend::Local),
            Some(0.10),
        )
        .unwrap_err();
        let error_chain = format!("{error:#}");

        assert!(error_chain.contains("regressed"));
    }

    #[test]
    fn benchmark_gate_comparison_rejects_unexpected_backend() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, valid_result_json()).unwrap();
        fs::write(&result, valid_result_json()).unwrap();

        let error = run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::PrSmoke),
            Some(BenchmarkBackend::S3Compatible),
            Some(0.10),
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("benchmark expectation mismatch"));
    }

    #[test]
    fn benchmark_gate_comparison_rejects_placeholder_release_baseline() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, placeholder_release_baseline_json()).unwrap();
        fs::write(&result, release_result_json()).unwrap();

        let error = run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::Release),
            Some(BenchmarkBackend::S3Compatible),
            Some(0.10),
        )
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("requires a real baseline"), "{message}");
    }

    #[test]
    fn benchmark_gate_comparison_rejects_placeholder_s3_nightly_baseline() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, placeholder_nightly_baseline_json()).unwrap();
        fs::write(&result, nightly_result_json()).unwrap();

        let error = run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::NightlyIntegration),
            Some(BenchmarkBackend::S3Compatible),
            Some(0.10),
        )
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("requires a real baseline"), "{message}");
    }

    #[test]
    fn benchmark_gate_comparison_rejects_placeholder_s3_current_result() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, release_result_json()).unwrap();
        fs::write(&result, placeholder_current_s3_result_json()).unwrap();

        let error = run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::Release),
            Some(BenchmarkBackend::S3Compatible),
            Some(0.10),
        )
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(
            message.contains("must be real backend evidence"),
            "{message}"
        );
    }

    #[test]
    fn benchmark_gate_comparison_rejects_unknown_s3_current_result() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, release_result_json()).unwrap();
        fs::write(
            &result,
            serde_json::to_string_pretty(&normal_result(
                "unknown",
                "release",
                "s3_compatible",
                "s3_incremental",
                1000.0,
                s3_workload_metrics(),
            ))
            .unwrap(),
        )
        .unwrap();

        let error = run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::Release),
            Some(BenchmarkBackend::S3Compatible),
            Some(0.10),
        )
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(
            message.contains("must be real backend evidence"),
            "{message}"
        );
    }

    #[test]
    fn benchmark_gate_comparison_rejects_local_emulator_s3_current_result() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, release_result_json()).unwrap();
        fs::write(&result, local_emulator_s3_result_json()).unwrap();

        let error = run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::Release),
            Some(BenchmarkBackend::S3Compatible),
            Some(0.10),
        )
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("local emulator evidence"), "{message}");
    }

    #[test]
    fn benchmark_gate_comparison_rejects_missing_s3_current_evidence_scope() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        let mut current = normal_result(
            "abc123",
            "release",
            "s3_compatible",
            "s3_incremental",
            1000.0,
            s3_workload_metrics(),
        );
        current
            .as_object_mut()
            .unwrap()
            .remove("backend_evidence_scope");
        fs::write(&baseline, release_result_json()).unwrap();
        fs::write(&result, serde_json::to_string_pretty(&current).unwrap()).unwrap();

        let error = run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::Release),
            Some(BenchmarkBackend::S3Compatible),
            Some(0.10),
        )
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(
            message.contains("missing backend_evidence_scope"),
            "{message}"
        );
    }

    #[test]
    fn benchmark_gate_comparison_rejects_all_zero_s3_baseline() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(
            &baseline,
            serde_json::to_string_pretty(&normal_result(
                "0000000000000000000000000000000000000000",
                "release",
                "s3_compatible",
                "s3_incremental",
                1000.0,
                s3_workload_metrics(),
            ))
            .unwrap(),
        )
        .unwrap();
        fs::write(&result, release_result_json()).unwrap();

        let error = run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::Release),
            Some(BenchmarkBackend::S3Compatible),
            Some(0.10),
        )
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("requires a real baseline"), "{message}");
    }

    #[test]
    fn benchmark_gate_comparison_rejects_placeholder_local_current_result() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, valid_result_json()).unwrap();
        fs::write(&result, placeholder_current_local_result_json()).unwrap();

        let error = run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::PrSmoke),
            Some(BenchmarkBackend::Local),
            Some(0.10),
        )
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(
            message.contains("must be real backend evidence"),
            "{message}"
        );
    }

    #[test]
    fn benchmark_gate_comparison_rejects_placeholder_local_baseline() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, placeholder_local_baseline_json()).unwrap();
        fs::write(&result, valid_result_json()).unwrap();

        let error = run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::PrSmoke),
            Some(BenchmarkBackend::Local),
            Some(0.10),
        )
        .unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("requires a real baseline"), "{message}");
    }

    #[test]
    fn benchmark_gate_comparison_rejects_missing_required_workload_metric() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, valid_result_json()).unwrap();
        fs::write(&result, missing_required_workload_json()).unwrap();

        let error = run_benchmark_gate(
            Some(&baseline),
            &result,
            Some(BenchmarkGateLevel::PrSmoke),
            Some(BenchmarkBackend::Local),
            Some(0.10),
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("slatedb_state_reopen"));
    }

    #[test]
    fn checkpoint_inspection_formatter_prints_stable_operator_summary() {
        let summary = checkpoint_inspection_summary();
        let expected = concat!(
            "latest_valid_checkpoint=7\n",
            "manifests:\n",
            "checkpoint=3 key=v1/checkpoints/00000000000000000003.manifest lifecycle=published gc_transitions=1 retention=gc_run=run-0001 deleted=1 recovery_transitions=1 status=valid\n",
            "checkpoint=8 key=v1/checkpoints/00000000000000000008.manifest lifecycle=none gc_transitions=0 retention=none recovery_transitions=0 status=invalid reason=missing visible parent checkpoint 7\n",
        );

        assert_eq!(format_checkpoint_inspection(&summary), expected);
    }

    #[test]
    fn checkpoint_inspection_json_uses_stable_schema_version() {
        let json = format_checkpoint_inspection_json(&checkpoint_inspection_summary()).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["schema_version"], 3);
        assert_eq!(value["latest_valid_checkpoint"], 7);
        assert_eq!(value["manifests"][0]["status"], "valid");
        assert_eq!(
            value["manifests"][0]["gc_transition_records"][0]["transition_id"],
            "gc-retired-run-0001"
        );
        assert_eq!(
            value["manifests"][0]["retention_record"]["gc_run_id"],
            "run-0001"
        );
        assert_eq!(
            value["manifests"][0]["recovery_transition_records"][0]["transition_id"],
            "recovery-test-0001"
        );
        assert_eq!(
            value["manifests"][1]["status"]["invalid"]["reason"],
            "missing visible parent checkpoint\n7"
        );
    }

    #[test]
    fn checkpoint_latest_repair_formatter_reports_repaired_marker() {
        let marker = latest_candidate_marker(7);

        assert_eq!(
            format_checkpoint_latest_repair(Some(&marker)),
            "latest_candidate_marker=checkpoint 7 key=v1/checkpoints/00000000000000000007.manifest digest=sha256:manifest\n"
        );
        assert_eq!(
            format_checkpoint_latest_repair(None),
            "latest_candidate_marker=none\n"
        );

        let json = format_checkpoint_latest_repair_json(Some(&marker)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["latest_candidate_marker"]["checkpoint_version"], 7);
    }

    #[test]
    fn checkpoint_repair_formatter_reports_lifecycle_and_latest_repairs() {
        let report = CheckpointAdminRepairReport {
            lifecycle_records_repaired: vec![checkpoint_lifecycle_record(7)],
            latest_candidate_marker: Some(latest_candidate_marker(7)),
        };

        assert_eq!(
            format_checkpoint_repair(&report),
            "lifecycle_records_repaired=1\nlatest_candidate_marker=7\n"
        );

        let json = format_checkpoint_repair_json(&report).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(
            value["lifecycle_records_repaired"][0]["checkpoint_version"],
            7
        );
        assert_eq!(value["latest_candidate_marker"]["checkpoint_version"], 7);
    }

    #[test]
    fn gc_plan_json_uses_stable_schema_version() {
        let plan = GarbageCollectionPlan {
            retained_manifest_versions: vec![7, 8],
            candidates: vec![GarbageCollectionCandidate {
                object_key: ObjectKey::parse(
                    "v1/outputs/orders/p=0000000000/chk=00000000000000000001/00000000000000000000-00000000000000000010/out.output",
                )
                .unwrap(),
                kind: GarbageCollectionCandidateKind::OutputObject,
            }],
        };

        let json = format_gc_plan_json(&plan).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["schema_version"], 1);
        assert_eq!(
            value["retained_manifest_versions"],
            serde_json::json!([7, 8])
        );
        assert_eq!(value["candidates"][0]["kind"], "output_object");
    }

    #[test]
    fn gc_execute_json_is_the_stored_run_evidence() {
        let candidate = GarbageCollectionCandidate {
            object_key: ObjectKey::parse(
                "v1/state/orders/p=0000000000/chk=00000000000000000001/state-0001.state",
            )
            .unwrap(),
            kind: GarbageCollectionCandidateKind::RawStateObject,
        };
        let run = GarbageCollectionRunV1 {
            schema_version: 1,
            run_id: "run-0001".to_string(),
            policy: GarbageCollectionPolicy {
                retain_latest_manifests: 2,
            },
            plan: GarbageCollectionPlan {
                retained_manifest_versions: vec![7, 8],
                candidates: vec![candidate.clone()],
            },
            report: GarbageCollectionReport {
                deleted: vec![candidate],
                skipped: vec![],
            },
        };

        let json = format_gc_run_json(&run).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["run_id"], "run-0001");
        assert_eq!(value["policy"]["retain_latest_manifests"], 2);
        assert_eq!(
            value["plan"]["retained_manifest_versions"],
            serde_json::json!([7, 8])
        );
        assert_eq!(value["report"]["deleted"][0]["kind"], "raw_state_object");
    }

    fn valid_result_json() -> String {
        serde_json::to_string_pretty(&normal_result(
            "abc123",
            "pr_smoke",
            "local",
            "local_incremental",
            1000.0,
            workload_metrics(),
        ))
        .unwrap()
    }

    fn checkpoint_inspection_summary() -> CheckpointAdminInspection {
        CheckpointAdminInspection {
            latest_valid_checkpoint: Some(7),
            manifests: vec![
                CheckpointManifestInspection {
                    checkpoint_version: 3,
                    manifest_key: ObjectKey::checkpoint_manifest(3),
                    lifecycle_status: Some(CheckpointLifecycleStatus::Published),
                    gc_transition_records: vec![checkpoint_gc_transition_record(3)],
                    retention_record: Some(checkpoint_retention_record(3)),
                    recovery_transition_records: vec![checkpoint_recovery_transition_record(3)],
                    status: CheckpointManifestInspectionStatus::Valid,
                },
                CheckpointManifestInspection {
                    checkpoint_version: 8,
                    manifest_key: ObjectKey::checkpoint_manifest(8),
                    lifecycle_status: None,
                    gc_transition_records: vec![],
                    retention_record: None,
                    recovery_transition_records: vec![],
                    status: CheckpointManifestInspectionStatus::Invalid {
                        reason: "missing visible parent checkpoint\n7".to_string(),
                    },
                },
            ],
        }
    }

    fn latest_candidate_marker(checkpoint_version: u64) -> LatestCandidateMarker {
        LatestCandidateMarker {
            schema_version: 1,
            checkpoint_version,
            manifest_key: ObjectKey::checkpoint_manifest(checkpoint_version),
            manifest_digest: "sha256:manifest".to_string(),
            validated_parent_checkpoint: checkpoint_version.checked_sub(1),
            updated_at: "unix:0.000000001".to_string(),
        }
    }

    fn checkpoint_lifecycle_record(checkpoint_version: u64) -> CheckpointLifecycleRecord {
        CheckpointLifecycleRecord {
            schema_version: 1,
            checkpoint_version,
            manifest_key: ObjectKey::checkpoint_manifest(checkpoint_version),
            manifest_digest: "sha256:manifest".to_string(),
            status: CheckpointLifecycleStatus::Published,
            status_updated_at: "unix:0.000000001".to_string(),
        }
    }

    fn checkpoint_retention_record(checkpoint_version: u64) -> CheckpointRetentionRecordV1 {
        CheckpointRetentionRecordV1 {
            schema_version: 1,
            checkpoint_version,
            manifest_key: ObjectKey::checkpoint_manifest(checkpoint_version),
            manifest_digest: "sha256:retained".to_string(),
            gc_run_id: "run-0001".to_string(),
            policy: GarbageCollectionPolicy {
                retain_latest_manifests: 1,
            },
            retained_manifest_versions: vec![7],
            deleted_candidate_keys: vec![ObjectKey::state_object(
                "balances_by_account",
                0,
                checkpoint_version,
                "state-0001",
            )
            .unwrap()],
            retained_at: "unix:0.000000001".to_string(),
        }
    }

    fn orders_envelope_bytes_for(
        schema_fingerprint: &str,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
    ) -> Bytes {
        IngestEnvelope::encode_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: "orders".to_string(),
                relation_version: "2026-05-05.v1".to_string(),
                schema_fingerprint: schema_fingerprint.to_string(),
                stream_id: "orders".to_string(),
                partition_id: 0,
                start_offset_inclusive,
                end_offset_exclusive,
            },
            &[orders_batch()],
        )
        .unwrap()
    }

    fn orders_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["acct-1", "acct-2"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, -1])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn checkpoint_gc_transition_record(checkpoint_version: u64) -> CheckpointGcTransitionRecordV1 {
        CheckpointGcTransitionRecordV1 {
            schema_version: 1,
            checkpoint_version,
            transition_id: "gc-retired-run-0001".to_string(),
            manifest_key: ObjectKey::checkpoint_manifest(checkpoint_version),
            manifest_digest: "sha256:retained".to_string(),
            transition: velorix_storage::checkpoint_index::CheckpointGcTransition::PayloadReleased,
            gc_run_id: "run-0001".to_string(),
            gc_run_key: ObjectKey::garbage_collection_run("run-0001").unwrap(),
            gc_run_digest: "sha256:gc-run".to_string(),
            retention_record_key: ObjectKey::checkpoint_retention_record(checkpoint_version),
            retention_record_digest: "sha256:retention".to_string(),
            retained_manifest_versions: vec![7],
            released_payload_keys: vec![ObjectKey::state_object(
                "balances_by_account",
                0,
                checkpoint_version,
                "state-0001",
            )
            .unwrap()],
            created_at: "unix:0.000000002".to_string(),
            emitter: "checkpoint-publisher-gc".to_string(),
        }
    }

    fn checkpoint_recovery_transition_record(
        checkpoint_version: u64,
    ) -> CheckpointRecoveryTransitionRecordV1 {
        CheckpointRecoveryTransitionRecordV1 {
            schema_version: 1,
            checkpoint_version,
            transition_id: "recovery-test-0001".to_string(),
            manifest_key: ObjectKey::checkpoint_manifest(checkpoint_version),
            manifest_digest: "sha256:recovered".to_string(),
            recovery_mode: CheckpointRecoveryMode::SelectedCheckpoint,
            replay_checkpoint_count: 1,
            replayed_batch_count: 2,
            recovered_at: "2026-05-13T00:00:00Z".to_string(),
        }
    }

    fn readiness_json() -> String {
        serde_json::json!({
            "schema_version": 5,
            "deployment_id": "prod-a",
            "authority_store_id": "s3://velorix-prod",
            "capability_status": {
                "status": "pass",
                "evidence": "s3-compatible capability probe",
                "evidence_kind": ["s3_compatible"]
            },
            "s3_compatible_test_status": {
                "status": "pass",
                "evidence": "S3-compatible integration harness",
                "evidence_kind": ["s3_compatible_integration_harness"]
            },
            "ownership_status": {
                "status": "pass",
                "evidence": "durable epoch record",
                "evidence_kind": ["durable_ownership_epoch_record"]
            },
            "checkpoint_status": {
                "status": "pass",
                "evidence": "published checkpoint lifecycle and recovery transition",
                "evidence_kind": [
                    "published_checkpoint_lifecycle_record",
                    "checkpoint_recovery_transition_record"
                ]
            },
            "ingest_status": {
                "status": "pass",
                "evidence": "catalog-backed deployed ingest admission",
                "evidence_kind": [
                    "catalog_backed_ingest_admission",
                    "deployed_ingest_admission",
                    "ingest_writer_lifecycle_attestation"
                ]
            },
            "standing_runtime_status": {
                "status": "pass",
                "evidence": "standing-runtime fencing capability and deployed multi-replica fencing smoke",
                "evidence_kind": [
                    "standing_runtime_fencing_capability",
                    "multi_replica_standing_runtime_fencing_smoke",
                    "local_api_pod_failover_smoke"
                ]
            },
            "relation_catalog_status": {
                "status": "pass",
                "evidence": "durable relation catalog record, registry, closed adapter scope, and fail-closed unsupported adapters",
                "evidence_kind": ["relation_catalog_record", "relation_catalog_registry", "relation_catalog_closed_adapter_scope", "relation_catalog_unsupported_adapter_fail_closed"]
            },
            "state_status": {
                "status": "pass",
                "evidence": "SlateDB checkpoint ref and checked recovery",
                "evidence_kind": ["slate_db_checkpoint_ref", "slate_db_checked_recovery"]
            },
            "query_policy_status": {
                "status": "pass",
                "evidence": "bounded DataFusion policy",
                "evidence_kind": ["query_policy_catalog"]
            },
            "table_catalog_status": {
                "status": "pass",
                "evidence": "registry-backed table catalog",
                "evidence_kind": ["registry_backed_table_catalog"]
            },
            "feldera_artifact_status": {
                "status": "pass",
                "evidence": "trusted artifact metadata",
                "evidence_kind": [
                    "feldera_artifact_registry",
                    "feldera_artifact_hash_verified"
                ]
            },
            "dependency_governance_status": {
                "status": "pass",
                "evidence": "dependency governance validated",
                "evidence_kind": ["dependency_governance_validated"]
            },
            "benchmark_gate_status": {
                "status": "pass",
                "evidence": "S3-compatible benchmark gate",
                "evidence_kind": ["s3_compatible_benchmark_gate"]
            },
            "gc_status": {
                "status": "pass",
                "evidence": "production GC run and retention evidence",
                "evidence_kind": [
                    "gc_run_evidence",
                    "production_gc_run_evidence",
                    "rustfs_production_gc_evidence_family_validated",
                    "checkpoint_retention_record"
                ]
            },
            "kubernetes_status": {
                "status": "pass",
                "evidence": "Kubernetes Lease client",
                "evidence_kind": ["kubernetes_lease_client"]
            }
        })
        .to_string()
    }

    fn dependency_governance_evidence_json() -> String {
        serde_json::json!({
            "schema_version": 1,
            "status": "pass",
            "evidence_kind": "dependency_governance_validated",
            "cargo_deny": {
                "diagnostics_checked": true,
                "diagnostics_path": "target/dependency-governance/cargo-deny.jsonl"
            },
            "external_audit_attestation": false,
            "missing_required_package_review_subjects": []
        })
        .to_string()
    }

    fn feldera_hash_evidence_json() -> String {
        serde_json::json!({
            "schema_version": 1,
            "status": "pass",
            "evidence_kind": "feldera_artifact_hash_verified",
            "view_id": "standing_view_orders_by_region",
            "artifact_id": "feldera-artifact-orders-by-region-20260503",
            "artifact_hash": "sha256:9063ca4eca6bd69190c68b01a00def0a5b86470abbc2312e94491c59a1c7f537",
            "spec_hash": "velorix-feldera-spec-sha256-v1:0e24cbe06543d735a6d62868f230c4610fb9139cb91e5e8f72042f17da0ecbea",
            "generated_rust_abi_version": "feldera-generated-rust-abi-v1"
        })
        .to_string()
    }

    fn feldera_provenance_evidence_json() -> String {
        serde_json::json!({
            "schema_version": 1,
            "status": "pass",
            "evidence_kind": "feldera_artifact_release_provenance",
            "release_id": "velorix-feldera-release-20260507",
            "release_version": "1.0.0-rc.1",
            "build_id": "feldera-build-20260507T000000Z",
            "builder_id": "github-actions/feldera-artifacts",
            "artifact_id": "feldera-artifact-orders-by-region-20260503",
            "artifact_hash": "sha256:9063ca4eca6bd69190c68b01a00def0a5b86470abbc2312e94491c59a1c7f537",
            "spec_hash": "velorix-feldera-spec-sha256-v1:0e24cbe06543d735a6d62868f230c4610fb9139cb91e5e8f72042f17da0ecbea",
            "generated_rust_abi_version": "feldera-generated-rust-abi-v1",
            "generated_rust_crate_name": "orders_by_region_pipeline",
            "source_repository": "https://github.com/mrchypark/velorix",
            "source_revision": "0123456789abcdef0123456789abcdef01234567"
        })
        .to_string()
    }

    fn s3_release_benchmark_gate_json() -> String {
        serde_json::json!({
            "schema_version": 1,
            "status": "pass",
            "evidence_kind": "s3_compatible_benchmark_gate",
            "gate_level": "release",
            "backend": "s3_compatible",
            "backend_evidence_scope": "live_or_native",
            "workload": "s3_incremental",
            "baseline_path": "baselines/benchmark/s3/release.json",
            "result_path": "target/velorix-bench/s3-release.json",
            "baseline_commit": "1111111111111111111111111111111111111111",
            "result_commit": "2222222222222222222222222222222222222222",
            "max_regression_fraction": 0.10,
            "workload_metrics": S3_COMPATIBLE_BENCHMARK_GATE_WORKLOADS
        })
        .to_string()
    }

    fn production_gc_run_evidence_json() -> String {
        serde_json::json!({
            "schema_version": 1,
            "status": "pass",
            "evidence_kind": "production_gc_run_evidence",
            "deployment_id": "prod-a",
            "authority_store_id": "s3://velorix-prod",
            "gc_run_id": "gc-run-20260513T000000Z",
            "listing_consistency_checked": true,
            "checkpoint_retention_records_checked": true,
            "checkpoint_gc_transition_records_checked": true,
            "verified_gc_run_digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "verified_gc_run_deleted_count": 1,
            "verified_gc_run_retain_latest_manifests": 1,
            "verified_gc_run_deleted_object_keys": [
                "v1/state/orders_sum_count/p=0000000000/chk=00000000000000000000/gc-run-20260513T000000Z-state-0000.state"
            ]
        })
        .to_string()
    }

    fn rustfs_production_gc_validation_evidence_json(production_gc_path: &Path) -> String {
        serde_json::json!({
            "schema_version": 1,
            "status": "pass",
            "evidence_kind": "rustfs_production_gc_evidence_family_validated",
            "gate_evidence_path": "target/velorix-s3/rustfs-s3-gate-evidence.json",
            "seed_evidence_path": "target/release-evidence/rustfs-production-gc-seed.json",
            "execute_evidence_path": "target/release-evidence/rustfs-production-gc-run.json",
            "production_evidence_path": production_gc_path.display().to_string(),
            "deployment_id": "prod-a",
            "authority_store_id": "s3://velorix-prod",
            "gc_run_id": "gc-run-20260513T000000Z",
            "retain_latest_manifests": 1,
            "deleted_candidates": 1,
            "checks": [
                "rustfs_s3_compatible_gate_present",
                "seed_fixture_created_retired_checkpoint_state",
                "s3_gc_execute_deleted_seeded_candidate",
                "production_gc_evidence_verified_listing_retention_and_transition",
                "artifact_family_paths_and_identity_bound"
            ]
        })
        .to_string()
    }

    fn write_rustfs_production_gc_family(dir: &Path) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
        let gate = dir.join("rustfs-s3-gate-evidence.json");
        let seed = dir.join("rustfs-production-gc-seed.json");
        let execute = dir.join("rustfs-production-gc-run.json");
        let production = dir.join("rustfs-production-gc.json");
        let authority_store_id = "s3://rustfs/velorix-rustfs/rustfs-s3-gate/test/production-gc";
        let gc_run_id = "rustfs-production-gc-test";
        let state_object_id_0 = "rustfs-production-gc-test-state-0000";
        let state_object_id_1 = "rustfs-production-gc-test-state-0001";
        let deleted_object_key = format!(
            "v1/state/orders_sum_count/p=0000000000/chk=00000000000000000000/{state_object_id_0}.state"
        );

        fs::write(
            &gate,
            serde_json::to_string_pretty(&serde_json::json!({
                "schema_version": 1,
                "evidence_kind": "rustfs_s3_compatible_gate",
                "readiness_evidence_kind": [
                    "s3_compatible",
                    "s3_compatible_integration_harness"
                ],
                "gate_detail_kind": [
                    "s3_compatible_ingest_admission_crash_restart",
                    "s3_compatible_gc_execution_retention"
                ],
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
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &seed,
            serde_json::to_string_pretty(&serde_json::json!({
                "schema_version": 1,
                "status": "pass",
                "evidence_kind": "s3_compatible_gc_seed_fixture",
                "fixture_kind": "release_smoke_gc_fixture",
                "authority_store_id": authority_store_id,
                "seed_id": gc_run_id,
                "checkpoint_versions": [0, 1],
                "state_object_ids": [state_object_id_0, state_object_id_1],
                "expected_deleted_object_keys": [deleted_object_key],
                "state_objects_written": 2,
                "expected_min_deleted_candidates": 1,
                "expected_deleted_candidates_at_retain_latest_manifests": 1
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &execute,
            serde_json::to_string_pretty(&serde_json::json!({
                "schema_version": 1,
                "run_id": gc_run_id,
                "policy": {
                    "retain_latest_manifests": 1
                },
                "plan": {
                    "retained_manifest_versions": [1],
                    "candidates": [
                        {
                            "object_key": deleted_object_key,
                            "kind": "raw_state_object"
                        }
                    ]
                },
                "report": {
                    "deleted": [
                        {
                            "object_key": deleted_object_key,
                            "kind": "raw_state_object"
                        }
                    ],
                    "skipped": []
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let execute_run: GarbageCollectionRunV1 =
            serde_json::from_str(&fs::read_to_string(&execute).unwrap()).unwrap();
        let execute_run_digest = garbage_collection_run_digest(&execute_run).unwrap();
        let deleted_object_keys = execute_run
            .report
            .deleted
            .iter()
            .map(|candidate| candidate.object_key.to_string())
            .collect::<Vec<_>>();
        fs::write(
            &production,
            serde_json::to_string_pretty(&serde_json::json!({
                "schema_version": 1,
                "status": "pass",
                "evidence_kind": "production_gc_run_evidence",
                "deployment_id": "rustfs-s3-gate",
                "authority_store_id": authority_store_id,
                "gc_run_id": gc_run_id,
                "listing_consistency_checked": true,
                "checkpoint_retention_records_checked": true,
                "checkpoint_gc_transition_records_checked": true,
                "verified_gc_run_digest": execute_run_digest,
                "verified_gc_run_deleted_count": 1,
                "verified_gc_run_retain_latest_manifests": 1,
                "verified_gc_run_deleted_object_keys": deleted_object_keys
            }))
            .unwrap(),
        )
        .unwrap();

        (gate, seed, execute, production)
    }

    fn write_deployed_image_fixture_files(
        dir: &Path,
        deployment_name: &str,
        container_name: &str,
        image: &str,
        image_digest: &str,
    ) {
        fs::write(dir.join(format!("{deployment_name}.yaml")), "{}\n").unwrap();
        fs::write(
            dir.join(format!("{deployment_name}-deployment-observed.json")),
            serde_json::json!({
                "kind": "Deployment",
                "metadata": {
                    "name": deployment_name
                },
                "spec": {
                    "template": {
                        "metadata": {
                            "annotations": {
                                "velorix.dev/image-digest": image_digest
                            }
                        },
                        "spec": {
                            "containers": [
                                {
                                    "name": container_name,
                                    "image": image
                                }
                            ]
                        }
                    }
                }
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            dir.join(format!("{deployment_name}-pods.json")),
            serde_json::json!({
                "items": [
                    {
                        "status": {
                            "containerStatuses": [
                                {
                                    "name": container_name,
                                    "imageID": format!("docker-pullable://example/{deployment_name}@{image_digest}")
                                }
                            ]
                        }
                    }
                ]
            })
            .to_string(),
        )
        .unwrap();
    }

    fn current_rfc3339_utc() -> String {
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let days = (seconds / 86_400) as i64;
        let seconds_of_day = seconds % 86_400;
        let (year, month, day) = civil_from_days(days);
        let hour = seconds_of_day / 3_600;
        let minute = (seconds_of_day % 3_600) / 60;
        let second = seconds_of_day % 60;
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
    }

    fn hiqlite_backend_time_fixture_bundle_sha256(entries: &[serde_json::Value]) -> String {
        let mut rows = entries
            .iter()
            .map(|entry| {
                (
                    entry.get("kind").unwrap().as_str().unwrap().to_string(),
                    entry.get("path").unwrap().as_str().unwrap().to_string(),
                    entry.get("sha256").unwrap().as_str().unwrap().to_string(),
                    entry.get("size_bytes").unwrap().as_u64().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        let mut canonical = String::new();
        for (kind, path, sha256, size_bytes) in rows {
            canonical.push_str(&kind);
            canonical.push('\t');
            canonical.push_str(&path);
            canonical.push('\t');
            canonical.push_str(&sha256);
            canonical.push('\t');
            canonical.push_str(&size_bytes.to_string());
            canonical.push('\n');
        }
        let digest = Sha256::digest(canonical.as_bytes());
        let mut output = String::from("sha256:");
        for byte in digest {
            output.push_str(&format!("{byte:02x}"));
        }
        output
    }

    fn hiqlite_backend_time_fixture_signature_bundle(
        signed_payload_sha256: &str,
    ) -> serde_json::Value {
        let seed = [7_u8; 32];
        let key_pair = ring::signature::Ed25519KeyPair::from_seed_unchecked(&seed).unwrap();
        let public_key = key_pair.public_key().as_ref();
        let signature = key_pair.sign(signed_payload_sha256.as_bytes());
        serde_json::json!({
            "bundle_kind": "sigstore_rekor_dsse",
            "oidc_issuer": HIQLITE_BACKEND_TIME_TRUSTED_OIDC_ISSUER,
            "certificate_identity": "repo:mrchypark/velorix:ref:refs/heads/main",
            "signing_certificate_sha256": "sha256:4444444444444444444444444444444444444444444444444444444444444444",
            "signed_payload_sha256": signed_payload_sha256,
            "signature_algorithm": "ed25519",
            "public_key_base64": BASE64_STANDARD.encode(public_key),
            "public_key_sha256": sha256_digest_of_bytes(public_key),
            "signature_base64": BASE64_STANDARD.encode(signature.as_ref()),
            "transparency_log_id": "sha256:5555555555555555555555555555555555555555555555555555555555555555",
            "transparency_log_index": 42,
            "integrated_time_unix": 1767139200,
            "inclusion_proof_sha256": "sha256:6666666666666666666666666666666666666666666666666666666666666666"
        })
    }

    fn ingest_writer_lifecycle_evidence_json() -> String {
        serde_json::json!({
            "schema_version": 1,
            "evidence_kind": "velorix_ingest_writer_lifecycle_attestation",
            "deployment_id": "prod-a",
            "authority_store_id": "s3://velorix-prod",
            "deployed_topology": "replicated_controller",
            "pod_internal_append_completed": true,
            "multi_pod_overlap_conflict_rejected": true,
            "adjacent_append_succeeded": true,
            "crash_restart_reconstruction_checked": true,
            "leader_handoff_checked": true,
            "kubernetes_lease_handoff_checked": true,
            "lease_held_through_append_checked": true,
            "commit_guard_checked": true,
            "admission_commit_guard_bound_checked": true,
            "lease_loss_during_reservation_checked": true,
            "no_pvc_created_by_vind": true,
            "attested_at": current_rfc3339_utc(),
            "attester": "velorix-release-operator",
            "evidence_provenance": {
                "pod_internal_job": lifecycle_job_provenance_json("pod-internal"),
                "overlap_job": lifecycle_job_provenance_json("overlap"),
                "adjacent_job": lifecycle_job_provenance_json("adjacent"),
                "restart_job": lifecycle_job_provenance_json("restart"),
                "lease_loss_job": lifecycle_job_provenance_json("lease-loss"),
                "handoff_owner_a_job": lifecycle_job_provenance_json("handoff-owner-a"),
                "handoff_owner_b_job": lifecycle_job_provenance_json("handoff-owner-b"),
                "handoff_stale_owner_job": lifecycle_job_provenance_json("handoff-stale-owner")
            },
            "evidence_files": {
                "pod_internal_job": "velorix-ingest-writer-smoke-log.json",
                "overlap_job": "velorix-ingest-lifecycle-overlap-log.json",
                "adjacent_job": "velorix-ingest-lifecycle-adjacent-log.json",
                "restart_job": "velorix-ingest-lifecycle-restart-log.json",
                "lease_loss_job": "velorix-ingest-lifecycle-lease-loss-log.json",
                "handoff_probe_job": "velorix-ingest-lifecycle-handoff-log.json"
            }
        })
        .to_string()
    }

    fn lifecycle_job_provenance_json(name: &str) -> serde_json::Value {
        serde_json::json!({
            "job_uid": format!("{name}-job-uid"),
            "pod_uid": format!("{name}-pod-uid"),
            "pod_name": format!("{name}-pod"),
            "container_image": "velorix-ingest-writer:e2e",
            "container_image_id": format!("docker-pullable://velorix-ingest-writer@sha256:{name}")
        })
    }

    fn release_readiness_error_for_product(product: serde_json::Value) -> anyhow::Error {
        release_readiness_error_for_product_with_sibling_override(product, None)
    }

    fn release_readiness_error_for_product_with_sibling_override(
        product: serde_json::Value,
        sibling_override: Option<(&str, String)>,
    ) -> anyhow::Error {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let feldera_hash = dir.path().join("feldera-hash.json");
        let s3_gate = dir.path().join("s3-gate.json");
        let production_gc = dir.path().join("production-gc.json");
        let ingest_lifecycle = dir.path().join("ingest-lifecycle.json");
        let product_path = dir.path().join("product-evidence.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json()).unwrap();
        fs::write(&feldera_hash, feldera_hash_evidence_json()).unwrap();
        fs::write(&s3_gate, s3_release_benchmark_gate_json()).unwrap();
        fs::write(&production_gc, production_gc_run_evidence_json()).unwrap();
        fs::write(&ingest_lifecycle, ingest_writer_lifecycle_evidence_json()).unwrap();
        fs::write(&product_path, serde_json::to_string(&product).unwrap()).unwrap();
        write_release_evidence_sibling_fixture_files(dir.path());
        if let Some((filename, contents)) = sibling_override {
            fs::write(dir.path().join(filename), contents).unwrap();
        }

        read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                standing_runtime_product_evidence: Some(product_path),
                require_release_artifacts: true,
                dependency_governance_evidence: Some(dependency),
                feldera_artifact_hash_evidence: Some(feldera_hash),
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                production_gc_run_evidence: Some(production_gc),
                ingest_writer_lifecycle_evidence: Some(ingest_lifecycle),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err()
    }

    fn write_release_evidence_sibling_fixture_files(dir: &Path) {
        for filename in [
            "openapi.json",
            "standing-runtime-failover-smoke.json",
            "tls-auth-smoke.json",
            "no-pvc-namespace.json",
            "hiqlite-authority-attestation.json",
            "no-pvc-hiqlite-statefulset.json",
            "velorix-hiqlite.yaml",
            "ingress-tls-auth-attestation.json",
            "query-policy-interactive.json",
            "query-policy-interactive-read.json",
            "query-policy-weak-rejection.json",
            "query-policy-missing-view.json",
            "external-s3-validate-job.json",
            "external-s3-validate.log",
            "object-store-durability-attestation.json",
            "ingest-writer-job-log.json",
            "ingest-writer-job.json",
            "ingest-writer-pods.json",
            "velorix-meta-smoke.log",
        ] {
            fs::write(dir.join(filename), "{}\n").unwrap();
        }
        let product_path = dir.join("product-evidence.json");
        if product_path.is_file() {
            let product: serde_json::Value =
                serde_json::from_str(&fs::read_to_string(&product_path).unwrap()).unwrap();
            fs::write(dir.join("openapi.json"), openapi_fixture_json(&product)).unwrap();
            fs::write(
                dir.join("query-policy-interactive.json"),
                query_policy_interactive_fixture_json(),
            )
            .unwrap();
            fs::write(
                dir.join("query-policy-interactive-read.json"),
                query_policy_interactive_fixture_json(),
            )
            .unwrap();
            fs::write(
                dir.join("query-policy-weak-rejection.json"),
                serde_json::json!({
                    "error": "production table scans require query policy field max_sql_bytes"
                })
                .to_string(),
            )
            .unwrap();
            fs::write(
                dir.join("query-policy-missing-view.json"),
                serde_json::json!({
                    "error": "query policy not found"
                })
                .to_string(),
            )
            .unwrap();
            if product
                .pointer("/object_store/bucket")
                .and_then(serde_json::Value::as_str)
                .is_some()
                && product
                    .pointer("/object_store/external_s3_validation_key")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
            {
                fs::write(
                    dir.join("external-s3-validate-job.json"),
                    external_s3_validation_job_fixture_json(&product),
                )
                .unwrap();
                fs::write(
                    dir.join("external-s3-validate.log"),
                    external_s3_validation_log_fixture(&product),
                )
                .unwrap();
            }
            fs::write(
                dir.join("no-pvc-namespace.json"),
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "List",
                    "items": []
                })
                .to_string(),
            )
            .unwrap();
            if let Some(attestation) =
                product.pointer("/object_store/durability_policy_attestation")
            {
                let mut sibling = attestation.clone();
                if let Some(object) = sibling.as_object_mut() {
                    object.remove("validated");
                    object.remove("evidence");
                }
                fs::write(
                    dir.join("object-store-durability-attestation.json"),
                    sibling.to_string(),
                )
                .unwrap();
            }
            if let Some(attestation) =
                product.pointer("/metadata_store/hiqlite_authority_attestation")
            {
                let mut sibling = attestation.clone();
                if let Some(object) = sibling.as_object_mut() {
                    object.remove("validated");
                    object.remove("evidence");
                }
                fs::write(
                    dir.join("hiqlite-authority-attestation.json"),
                    sibling.to_string(),
                )
                .unwrap();
                if attestation
                    .pointer("/authority_kind")
                    .and_then(serde_json::Value::as_str)
                    == Some("velorix_managed_hiqlite")
                {
                    fs::write(
                        dir.join("no-pvc-hiqlite-statefulset.json"),
                        managed_hiqlite_no_pvc_statefulset_fixture_json(),
                    )
                    .unwrap();
                }
            }
            if let Some(attestation) = product.pointer("/api/auth/ingress_tls_auth_attestation") {
                let mut sibling = attestation.clone();
                if let Some(object) = sibling.as_object_mut() {
                    object.remove("validated");
                    object.remove("evidence");
                }
                fs::write(
                    dir.join("ingress-tls-auth-attestation.json"),
                    sibling.to_string(),
                )
                .unwrap();
            }
            fs::write(
                dir.join("view-compile-deploy-jobs.json"),
                compile_deploy_job_catalog_fixture_json(&product),
            )
            .unwrap();
            write_deployed_image_fixture_files(
                dir,
                "velorix-api",
                "api",
                "velorix-api:test",
                "sha256:2222222222222222222222222222222222222222222222222222222222222222",
            );
            write_deployed_image_fixture_files(
                dir,
                "velorix-meta",
                "meta",
                "velorix-meta:test",
                "sha256:3333333333333333333333333333333333333333333333333333333333333333",
            );
            let standing_capability = product
                .pointer("/standing_runtime_fencing/capability")
                .unwrap()
                .clone();
            let backend_time_summary =
                product.pointer("/metadata_store/hiqlite_backend_time_attestation");
            let backend_time_attested_at = backend_time_summary
                .and_then(|summary| summary.pointer("/attested_at"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(current_rfc3339_utc);
            let backend_time_attester = backend_time_summary
                .and_then(|summary| summary.pointer("/attester"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("velorix-release-operator")
                .to_string();
            let backend_time_trusted_for_product_complete = backend_time_summary
                .and_then(|summary| summary.pointer("/trusted_for_product_complete"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let backend_time_trusted_for_release_validator = backend_time_summary
                .and_then(|summary| summary.pointer("/trusted_for_release_validator"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let backend_time_release_validator_fail_closed = backend_time_summary
                .and_then(|summary| summary.pointer("/release_validator_fail_closed"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            fs::write(
                dir.join("hiqlite-backend-time-assessment.json"),
                serde_json::json!({
                    "schema_version": 1,
                    "evidence_kind": "velorix_hiqlite_backend_time_assessment",
                    "required_mode_supported": true,
                    "can_generate_product_complete_backend_time_attestation": true,
                        "backend_time_source_kind": "raft_replicated_authority_time",
                    "lease_authority_kind": "raft_replicated_time",
                    "lease_expiry_semantics": "backend_wall_clock_ttl",
                    "missing_capabilities": [],
                    "product_capability": standing_capability,
                    "velorix_meta_runtime": {
                        "owner_acquire_uses_authority_time": true,
                        "owner_read_uses_authority_time": true,
                        "checkpoint_publish_update_uses_authority_time": true,
                        "checkpoint_publish_insert_uses_authority_time": true,
                        "checkpoint_publish_rejects_scope_mismatch": true,
                        "unsafe_runtime_time_sources_absent": true
                    }
                })
                .to_string(),
            )
            .unwrap();
            fs::write(
                dir.join("readyz.json"),
                serde_json::json!({
                    "metadata_store": {
                        "standing_runtime_fencing": product
                            .pointer("/standing_runtime_fencing/capability")
                            .unwrap()
                    }
                })
                .to_string(),
            )
            .unwrap();
            fs::write(
                dir.join("multi-replica-fencing-smoke.json"),
                serde_json::json!({
                    "schema_version": 1,
                    "evidence_kind": "velorix_deployed_multi_replica_fencing_smoke",
                    "status": "pass",
                    "assertions": {
                        "distinct_api_pods": true,
                        "non_owner_ingest_rejected": true,
                        "owner_retry_converged": true,
                        "read_replica_served_query": true
                    }
                })
                .to_string(),
            )
            .unwrap();
            let failover_evidence = if backend_time_trusted_for_release_validator {
                serde_json::json!({
                    "schema_version": 1,
                    "evidence_kind": "velorix_standing_runtime_failover_smoke",
                    "status": "pass",
                    "trusted_for_product_complete": true,
                    "production_wall_clock_failover_attestation": true,
                    "evidence_scope": "release_ci_deployed_product",
                    "failover_probe_kind": "release_bounded_wall_clock_failover",
                    "backend_time_source_kind": "raft_replicated_authority_time",
                    "authority_time_observed": true,
                    "owner_ttl_ms": 300000,
                    "failover_time_bound_ms": 300000,
                    "pre_failover_owner_epoch": 1,
                    "post_failover_owner_epoch": 2,
                    "affected_api_pods": ["velorix-api-0"],
                    "observed_failover_ms": 240000
                })
            } else {
                serde_json::json!({
                    "schema_version": 1,
                    "evidence_kind": "velorix_standing_runtime_failover_smoke",
                    "status": "pass",
                    "trusted_for_product_complete": false,
                    "production_wall_clock_failover_attestation": false,
                    "observed_failover_ms": 240000
                })
            };
            fs::write(
                dir.join("standing-runtime-failover-smoke.json"),
                failover_evidence.to_string(),
            )
            .unwrap();
            fs::write(
                dir.join("velorix-meta-smoke.log"),
                "velorix-meta standing runtime adversarial smoke ok: owner_a_epoch=1 owner_b_epoch=2 latest_epoch=2\nvelorix-meta smoke ok: backend_time_source_kind=raft_replicated_authority_time\n",
            )
            .unwrap();
            let evidence_file = |kind: &str, filename: &str| {
                let path = dir.join(filename);
                if kind == "product_evidence" {
                    let bytes =
                        canonical_product_evidence_without_backend_time_attestation_bytes(&path)
                            .unwrap();
                    return serde_json::json!({
                        "kind": kind,
                        "path": filename,
                        "sha256": sha256_hex_of_bytes(&bytes),
                        "size_bytes": bytes.len(),
                        "canonicalization": "without_metadata_store_hiqlite_backend_time_attestation"
                    });
                }
                serde_json::json!({
                    "kind": kind,
                    "path": filename,
                    "sha256": sha256_hex_of_file(&path).unwrap(),
                    "size_bytes": fs::metadata(&path).unwrap().len()
                })
            };
            let evidence_files = vec![
                evidence_file("product_evidence", "product-evidence.json"),
                evidence_file(
                    "hiqlite_backend_time_assessment",
                    "hiqlite-backend-time-assessment.json",
                ),
                evidence_file("readyz", "readyz.json"),
                evidence_file(
                    "multi_replica_fencing_smoke",
                    "multi-replica-fencing-smoke.json",
                ),
                evidence_file(
                    "standing_runtime_failover_smoke",
                    "standing-runtime-failover-smoke.json",
                ),
                evidence_file("metadata_adversarial_smoke_log", "velorix-meta-smoke.log"),
            ];
            let canonical_bundle_sha256 =
                hiqlite_backend_time_fixture_bundle_sha256(&evidence_files);
            let trusted_provenance = if backend_time_trusted_for_release_validator {
                Some(serde_json::json!({
                    "schema_version": 1,
                    "provenance_kind": HIQLITE_BACKEND_TIME_TRUSTED_PROVENANCE_KIND,
                    "source_repository": "github.com/mrchypark/velorix",
                    "source_revision": TEST_RELEASE_COMMIT,
                    "workflow_name": "release-gate",
                    "workflow_run_id": "123456789",
                    "job_name": "vind-product-required",
                    "subject_image_digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                    "subject_images": [
                        {
                            "role": "velorix-api",
                            "image_digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222"
                        },
                        {
                            "role": "velorix-meta",
                            "image_digest": "sha256:3333333333333333333333333333333333333333333333333333333333333333"
                        },
                        {
                            "role": "hiqlite-authority",
                            "image_digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                        }
                    ],
                    "ci_identity": {
                        "identity_kind": "github_actions_oidc",
                        "issuer": HIQLITE_BACKEND_TIME_TRUSTED_OIDC_ISSUER,
                        "audience": HIQLITE_BACKEND_TIME_TRUSTED_OIDC_AUDIENCE,
                        "repository": HIQLITE_BACKEND_TIME_TRUSTED_GITHUB_REPOSITORY,
                        "subject": "repo:mrchypark/velorix:ref:refs/heads/main",
                        "workflow_ref": "mrchypark/velorix/.github/workflows/release-gate.yml@refs/heads/main",
                        "workflow_sha": TEST_RELEASE_COMMIT,
                        "job_workflow_ref": format!("{}{}", HIQLITE_BACKEND_TIME_TRUSTED_WORKFLOW_REF_PREFIX, TEST_RELEASE_COMMIT),
                        "run_id": "123456789",
                        "run_attempt": "1"
                    },
                    "signature_bundle": hiqlite_backend_time_fixture_signature_bundle(&canonical_bundle_sha256),
                    "generated_at": backend_time_attested_at,
                    "attester": backend_time_attester,
                    "canonical_bundle_sha256": canonical_bundle_sha256,
                    "canonical_bundle_entries": evidence_files.clone()
                }))
            } else {
                None
            };
            fs::write(
                dir.join("hiqlite-backend-time-attestation.json"),
                serde_json::json!({
                    "schema_version": 1,
                    "evidence_kind": "velorix_hiqlite_backend_time_attestation",
                    "backend_name": "hiqlite",
                    "time_source_kind": "raft_replicated_authority_time",
                    "lease_authority_kind": "raft_replicated_time",
                    "lease_expiry_semantics": "backend_wall_clock_ttl",
                    "authoritative_backend_time": true,
                    "bounded_wall_clock_failover": true,
                    "production_bounded_failover_safe": true,
                    "authority_sampled_unix_time_ms_in_raft_operation": true,
                    "owner_expiry_bound_to_authority_time": true,
                    "checkpoint_publish_rejects_expired_owner_with_authority_time": true,
                    "bounded_failover_probe_passed": true,
                    "failover_time_bound_ms": 300000,
                    "observed_max_failover_ms": 240000,
                    "metrics_time_source_rejected": true,
                    "raft_log_index_time_source_rejected": true,
                    "distributed_lock_ttl_source_rejected": true,
                    "attested_at": backend_time_attested_at,
                    "attester": backend_time_attester,
                    "trusted_for_product_complete": backend_time_trusted_for_product_complete,
                    "trusted_for_release_validator": backend_time_trusted_for_release_validator,
                    "release_validator_fail_closed": backend_time_release_validator_fail_closed,
                    "trusted_provenance": trusted_provenance,
                    "evidence_files": evidence_files
                })
                .to_string(),
            )
            .unwrap();
        }
        fs::write(
            dir.join("view-compile-deploy-run-once.json"),
            serde_json::json!({
                "pending_jobs": 1,
                "activated": 1,
                "skipped": 0,
                "failed": 0,
                "outcomes": [
                    {
                        "job_id": "pending_scores_by_user:velorix-feldera-spec-sha256-v1:test",
                        "view_id": "pending_scores_by_user",
                        "status": "activated"
                    }
                ]
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            dir.join("pending-scores-view-after-compile-deploy.json"),
            serde_json::json!({
                "view_id": "pending_scores_by_user",
                "execution_mode": "standing_runtime",
                "query_enabled": true,
                "lifecycle": {
                    "compile_status": "success",
                    "deployment_status": "running"
                }
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            dir.join("pending-scores-query-after-compile-deploy.json"),
            serde_json::json!({
                "rows": [
                    {"user_id": "u1", "sum": 12, "count": 2}
                ]
            })
            .to_string(),
        )
        .unwrap();
        for (_, filename) in REQUIRED_INGEST_WRITER_LIFECYCLE_EVIDENCE_FILES {
            fs::write(
                dir.join(filename),
                lifecycle_sibling_fixture_content(filename),
            )
            .unwrap();
        }
    }

    fn compile_deploy_job_catalog_fixture_json(product: &serde_json::Value) -> String {
        let view_id = product
            .pointer("/api/compile_deploy/pending_view_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("pending_scores_by_user");
        let spec_hash =
            "velorix-feldera-spec-sha256-v1:9866cb6a2ac9194a5c0ac4f4cccd834bf96b790b30b0e97fdf98a9a2b97142d2";
        serde_json::json!({
            "pending_jobs": 1,
            "jobs": [
                {
                    "schema_version": 1,
                    "job_id": format!("{view_id}:{spec_hash}"),
                    "view_id": view_id,
                    "spec_hash": spec_hash,
                    "compiler_backend": "feldera_compiler",
                    "compiler_request": {
                        "request_kind": "feldera_standing_view_compile_request_v1",
                        "view_id": view_id,
                        "spec_hash": spec_hash,
                        "sql": "select user_id, sum(score) as sum, count(*) as count from scores where score > 0 group by user_id",
                        "input_relations": [
                            {
                                "relation_id": "scores",
                                "relation_name": "scores",
                                "relation_version": "2026-05-24.v1",
                                "schema_fingerprint": "sha256:1e257147b96f379bbe4bfc98782f8cd1f019ed0bdd17532fb72901126000cc77",
                                "columns": [
                                    {
                                        "name": "user_id",
                                        "data_type": {"kind": "utf8"},
                                        "nullable": false
                                    },
                                    {
                                        "name": "score",
                                        "data_type": {"kind": "int64"},
                                        "nullable": false
                                    },
                                    {
                                        "name": "delta",
                                        "data_type": {"kind": "int64"},
                                        "nullable": false
                                    }
                                ],
                                "primary_key": ["user_id"]
                            }
                        ],
                        "output_relations": [
                            {
                                "relation_id": view_id,
                                "relation_name": view_id,
                                "relation_version": "v1",
                                "schema_fingerprint": "sha256:1e257147b96f379bbe4bfc98782f8cd1f019ed0bdd17532fb72901126000cc77",
                                "columns": [
                                    {
                                        "name": "user_id",
                                        "data_type": {"kind": "utf8"},
                                        "nullable": false
                                    },
                                    {
                                        "name": "sum",
                                        "data_type": {"kind": "int64"},
                                        "nullable": false
                                    },
                                    {
                                        "name": "count",
                                        "data_type": {"kind": "int64"},
                                        "nullable": false
                                    }
                                ],
                                "primary_key": ["user_id"]
                            }
                        ],
                        "shape": {
                            "is_materialized": true,
                            "multi_input": false,
                            "multi_output": false
                        }
                    },
                    "compile_status": "pending",
                    "deployment_status": "not_deployed",
                    "message": "view accepted; feldera compiler/deploy worker is not configured in this build"
                }
            ]
        })
        .to_string()
    }

    fn openapi_fixture_json(product: &serde_json::Value) -> String {
        let promoted_path = product
            .pointer("/api/openapi/promoted_api_path")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("/v1/api/scores/positive");
        let linked_policy = product
            .pointer("/api/openapi/linked_view_policy_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("interactive");
        serde_json::json!({
            "openapi": "3.0.3",
            "info": {
                "title": "Velorix View APIs",
                "version": "0.1.0"
            },
            "paths": {
                promoted_path: {
                    "get": {
                        "summary": "Query positive_scores_by_user",
                        "parameters": [
                            {
                                "name": "epoch",
                                "in": "query",
                                "required": false,
                                "schema": {"type": "integer", "minimum": 0}
                            },
                            {
                                "name": "page_token",
                                "in": "query",
                                "required": false,
                                "schema": {"type": "string"}
                            },
                            {
                                "name": "max_rows",
                                "in": "query",
                                "required": false,
                                "schema": {"type": "integer", "minimum": 1}
                            }
                        ],
                        "responses": {
                            "200": {
                                "description": "View query result rows",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "properties": {
                                                "rows": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "properties": {
                                                            "key": {"type": "string"},
                                                            "value": {"type": "string"},
                                                            "weight": {
                                                                "type": "integer",
                                                                "format": "int64"
                                                            }
                                                        }
                                                    }
                                                },
                                                "logical_epoch": {
                                                    "type": "integer",
                                                    "format": "int64"
                                                },
                                                "next_page_token": {"type": "string"}
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "x-velorix-view-id": "positive_scores_by_user",
                        "x-velorix-url-path": "/scores/positive",
                        "x-velorix-input-relation-id": "scores",
                        "x-velorix-input-relation-version": "2026-05-24.v1",
                        "x-velorix-query-policy-id": linked_policy,
                        "x-velorix-spec-hash": "velorix-feldera-spec-sha256-v1:9866cb6a2ac9194a5c0ac4f4cccd834bf96b790b30b0e97fdf98a9a2b97142d2",
                        "x-velorix-request": [],
                        "x-velorix-response-schema": null,
                        "x-velorix-sql-template": null
                    }
                },
                "/v1/relations": {
                    "post": {
                        "summary": "Create a relation catalog",
                        "responses": {"201": {"description": "Relation created"}}
                    }
                },
                "/v1/ingest": {
                    "post": {
                        "summary": "Ingest rows into a relation",
                        "responses": {"201": {"description": "Rows ingested"}}
                    }
                },
                "/v1/views": {
                    "get": {
                        "summary": "List view APIs",
                        "responses": {"200": {"description": "View catalog"}}
                    },
                    "post": {
                        "summary": "Create a view API",
                        "responses": {"201": {"description": "View created"}}
                    }
                }
            }
        })
        .to_string()
    }

    fn query_policy_interactive_fixture_json() -> String {
        serde_json::json!({
            "tenant_id": "default",
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
                "spill_limit_bytes": 1073741824,
                "batch_size": null,
                "target_partitions": null
            }
        })
        .to_string()
    }

    fn external_s3_validation_probe_prefix(product: &serde_json::Value) -> String {
        let prefix = product
            .pointer("/object_store/s3_prefix")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .trim_end_matches('/');
        if prefix.is_empty() {
            "_velorix_external_s3_validation".to_string()
        } else {
            prefix.to_string()
        }
    }

    fn external_s3_validation_job_fixture_json(product: &serde_json::Value) -> String {
        let bucket = product
            .pointer("/object_store/bucket")
            .and_then(serde_json::Value::as_str)
            .unwrap();
        let key = product
            .pointer("/object_store/external_s3_validation_key")
            .and_then(serde_json::Value::as_str)
            .unwrap();
        let prefix = external_s3_validation_probe_prefix(product);
        let script = format!(
            "aws s3api head-bucket --bucket \"{bucket}\"\n\
             aws s3api put-object --bucket \"{bucket}\" --key \"{key}\" --body /work/validation.txt\n\
             aws s3api get-object --bucket \"{bucket}\" --key \"{key}\" /work/read-back.txt\n\
             aws s3api list-objects-v2 --bucket \"{bucket}\" --prefix \"{key}\" --max-keys 1\n\
             aws s3api delete-object --bucket \"{bucket}\" --key \"{key}\"\n\
             echo \"velorix external-s3 validation ok bucket={bucket} prefix={prefix} key={key}\"\n"
        );
        serde_json::json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": "velorix-external-s3-validate"
            },
            "status": {
                "succeeded": 1,
                "conditions": [
                    {
                        "type": "Complete",
                        "status": "True"
                    }
                ]
            },
            "spec": {
                "template": {
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": "aws",
                                "command": ["/bin/sh", "-c"],
                                "args": [script],
                                "volumeMounts": [
                                    {
                                        "name": "work",
                                        "mountPath": "/work"
                                    }
                                ]
                            }
                        ],
                        "volumes": [
                            {
                                "name": "work",
                                "emptyDir": {}
                            }
                        ]
                    }
                }
            }
        })
        .to_string()
    }

    fn external_s3_validation_log_fixture(product: &serde_json::Value) -> String {
        let bucket = product
            .pointer("/object_store/bucket")
            .and_then(serde_json::Value::as_str)
            .unwrap();
        let key = product
            .pointer("/object_store/external_s3_validation_key")
            .and_then(serde_json::Value::as_str)
            .unwrap();
        let prefix = external_s3_validation_probe_prefix(product);
        format!("velorix external-s3 validation ok bucket={bucket} prefix={prefix} key={key}\n")
    }

    fn managed_hiqlite_no_pvc_statefulset_fixture_json() -> String {
        serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {
                "name": "velorix-hiqlite"
            },
            "spec": {
                "replicas": 3,
                "volumeClaimTemplates": [],
                "template": {
                    "spec": {
                        "serviceAccountName": "velorix-hiqlite",
                        "containers": [
                            {
                                "name": "hiqlite",
                                "env": [
                                    {"name": "HQL_SECRET_API", "valueFrom": {"secretKeyRef": {"name": "velorix-hiqlite-auth", "key": "api-secret"}}},
                                    {"name": "HQL_SECRET_RAFT", "valueFrom": {"secretKeyRef": {"name": "velorix-hiqlite-auth", "key": "raft-secret"}}},
                                    {"name": "ENC_KEY_ACTIVE", "valueFrom": {"secretKeyRef": {"name": "velorix-hiqlite-auth", "key": "enc-key-active"}}},
                                    {"name": "ENC_KEYS", "valueFrom": {"secretKeyRef": {"name": "velorix-hiqlite-auth", "key": "enc-keys"}}}
                                ]
                            }
                        ],
                        "volumes": [
                            {"name": "data", "emptyDir": {}},
                            {"name": "config", "configMap": {"name": "velorix-hiqlite-config"}}
                        ]
                    }
                }
            }
        })
        .to_string()
    }

    fn lifecycle_sibling_fixture_content(filename: &str) -> String {
        match filename {
            "velorix-ingest-writer-smoke-log.json"
            | "velorix-ingest-lifecycle-adjacent-log.json" => {
                lifecycle_guarded_append_fixture_json()
            }
            "velorix-ingest-lifecycle-overlap-log.json" => lifecycle_overlap_fixture_json(),
            "velorix-ingest-lifecycle-restart-log.json" => lifecycle_restart_fixture_json(),
            "velorix-ingest-lifecycle-lease-loss-log.json" => lifecycle_lease_loss_fixture_json(),
            "velorix-ingest-lifecycle-handoff-log.json" => lifecycle_handoff_fixture_json(),
            _ => "{}\n".to_string(),
        }
    }

    fn lifecycle_guarded_append_fixture_json() -> String {
        serde_json::json!({
            "evidence_kind": "ingest_writer_lease_guarded_append_probe",
            "status": "pass",
            "outcome": "appended",
            "lease_held_through_append": true,
            "commit_guard_enforced": true,
            "admission_commit_guard_bound": true,
            "admission_commit_guard_binding": {
                "binding_kind": "kubernetes_partition_lease",
                "subject": "coordination.k8s.io/v1/namespaces/velorix-live/leases/velorix-product-readiness-scores-p0"
            }
        })
        .to_string()
    }

    fn lifecycle_overlap_fixture_json() -> String {
        serde_json::json!({
            "schema_version": 1,
            "evidence_kind": "ingest_writer_lifecycle_overlap_conflict_probe",
            "status": "pass",
            "outcome": "conflict-rejected",
            "writer_id": "overlap-writer",
            "stream_id": "scores",
            "partition_id": 0,
            "start_offset_inclusive": 0,
            "attempted_row_count": 1,
            "multi_pod_overlap_conflict_rejected": true,
            "conflicting_append_rejected_before_append": true,
            "append_completed": false,
            "conflict_log_observed": true,
            "raw_conflict_log": "fresh append outcome, got conflict"
        })
        .to_string()
    }

    fn lifecycle_restart_fixture_json() -> String {
        serde_json::json!({
            "evidence_kind": "ingest_writer_admission_crash_restart_probe",
            "status": "pass",
            "orphan_admission_created": true,
            "restart_reconstructed_active_admission": true,
            "recovered_append_completed": true,
            "committed_admission_not_expirable": true
        })
        .to_string()
    }

    fn lifecycle_lease_loss_fixture_json() -> String {
        serde_json::json!({
            "evidence_kind": "ingest_writer_lease_loss_during_reservation_probe",
            "status": "pass",
            "before_admission_lease_verified": true,
            "lease_released_before_commit": true,
            "commit_guard_rejected_before_batch_commit": true,
            "batch_object_absent_after_rejection": true,
            "admission_commit_guard_bound": true,
            "restart_reconstructed_active_admission": true,
            "target_admission_rejected_overlapping_reservation_before_expiry": true,
            "orphan_expired": true,
            "expired_target_rejected_original_retry": true
        })
        .to_string()
    }

    fn lifecycle_handoff_fixture_json() -> String {
        serde_json::json!({
            "evidence_kind": "ingest_writer_two_pod_lease_handoff_probe",
            "status": "pass",
            "kubernetes_lease_handoff_checked": true,
            "commit_guard_checked": true,
            "admission_commit_guard_bound_checked": true,
            "owner_a_epoch": 10,
            "owner_b_epoch": 11,
            "owner_b_append_completed": true,
            "owner_b_lease_held_through_append": true,
            "stale_owner_rejected": true
        })
        .to_string()
    }

    fn hiqlite_product_evidence_json() -> serde_json::Value {
        let mut product: serde_json::Value =
            serde_json::from_str(&product_evidence_json("required", true)).unwrap();
        product["metadata_store"]["backend"] = serde_json::json!("hiqlite");
        product["standing_runtime_fencing"]["capability"]["backend_name"] =
            serde_json::json!("hiqlite");
        product["no_pvc"]["managed_hiqlite_authority_validated"] = serde_json::json!(true);
        product["metadata_store"]["hiqlite_authority_attestation"] = serde_json::json!({
            "validated": true,
            "evidence": "hiqlite-authority-attestation.json",
            "authority_kind": "velorix_managed_hiqlite",
            "schema_version": 1,
            "nodes": [
                "http://velorix-hiqlite-0.velorix-hiqlite:8200",
                "http://velorix-hiqlite-1.velorix-hiqlite:8200",
                "http://velorix-hiqlite-2.velorix-hiqlite:8200"
            ],
            "expected_voter_count": 3,
            "no_pvc_created_by_vind": true,
            "metadata_authority_no_pvc_used": true,
            "metadata_authority_storage_mode": "object-store-backup-restore-with-ephemeral-node-disk",
            "voters_learner_only_disabled": true,
            "api_auth_configured": true,
            "raft_auth_configured": true,
            "transport_security": "cluster-private-mtls",
            "backup_restore_configured": true,
            "image_digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "source_revision": "sebadob/hiqlite@abcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "no_pvc_evidence_files": {
                "namespace_pvc_list": "no-pvc-namespace.json",
                "hiqlite_statefulset": "no-pvc-hiqlite-statefulset.json",
                "manifest": "velorix-hiqlite.yaml"
            },
            "attested_at": "2026-05-31T00:00:00Z",
            "attester": "velorix-release-operator"
        });
        product
    }

    fn add_valid_hiqlite_backend_time_attestation(product: &mut serde_json::Value) {
        product["standing_runtime_fencing"]["capability"]["failover_time_bound_ms"] =
            serde_json::json!(300000);
        product["metadata_store"]["hiqlite_backend_time_attestation"] = serde_json::json!({
            "validated": true,
            "evidence": "hiqlite-backend-time-attestation.json",
            "schema_version": 1,
            "evidence_kind": "velorix_hiqlite_backend_time_attestation",
            "backend_name": "hiqlite",
            "time_source_kind": "raft_replicated_authority_time",
            "lease_authority_kind": "raft_replicated_time",
            "lease_expiry_semantics": "backend_wall_clock_ttl",
            "authoritative_backend_time": true,
            "bounded_wall_clock_failover": true,
            "production_bounded_failover_safe": true,
            "authority_sampled_unix_time_ms_in_raft_operation": true,
            "owner_expiry_bound_to_authority_time": true,
            "checkpoint_publish_rejects_expired_owner_with_authority_time": true,
            "bounded_failover_probe_passed": true,
            "failover_time_bound_ms": 300000,
            "observed_max_failover_ms": 240000,
            "metrics_time_source_rejected": true,
            "raft_log_index_time_source_rejected": true,
            "distributed_lock_ttl_source_rejected": true,
            "attested_at": current_rfc3339_utc(),
            "attester": "velorix-release-operator",
            "trusted_for_product_complete": false,
            "trusted_for_release_validator": false,
            "release_validator_fail_closed": true
        });
    }

    fn trust_hiqlite_backend_time_attestation_for_release(product: &mut serde_json::Value) {
        product["metadata_store"]["hiqlite_backend_time_attestation"]
            ["trusted_for_product_complete"] = serde_json::json!(true);
        product["metadata_store"]["hiqlite_backend_time_attestation"]
            ["trusted_for_release_validator"] = serde_json::json!(true);
        product["metadata_store"]["hiqlite_backend_time_attestation"]
            ["release_validator_fail_closed"] = serde_json::json!(false);
    }

    fn product_evidence_json(configured_mode: &str, product_complete: bool) -> String {
        let required = configured_mode == "required";
        let logical = configured_mode == "logical-fencing";
        serde_json::json!({
            "schema_version": 1,
            "evidence_kind": "velorix_product_slice_evidence",
            "deployment_id": "prod-a",
            "product_complete": product_complete,
            "product_complete_blockers": if product_complete {
                Vec::<String>::new()
            } else {
                vec!["metadata backend proves multi-writer fencing, but bounded wall-clock failover is not proven".to_string()]
            },
            "rest_callable": true,
            "deployed_images": {
                "velorix-api": {
                    "image": "velorix-api:test",
                    "image_digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
                    "evidence_files": {
                        "manifest": "velorix-api.yaml",
                        "deployment": "velorix-api-deployment-observed.json",
                        "pods": "velorix-api-pods.json"
                    }
                },
                "velorix-meta": {
                    "image": "velorix-meta:test",
                    "image_digest": "sha256:3333333333333333333333333333333333333333333333333333333333333333",
                    "evidence_files": {
                        "manifest": "velorix-meta.yaml",
                        "deployment": "velorix-meta-deployment-observed.json",
                        "pods": "velorix-meta-pods.json"
                    }
                }
            },
            "object_store": {
                "mode": "external-s3",
                "authority_store_id": "s3://velorix-prod",
                "bucket": "velorix-prod",
                "s3_prefix": "",
                "external_s3_validate_enabled": true,
                "external_s3_bucket_validated": true,
                "external_s3_prefix_validated": true,
                "external_s3_validation_key": "_velorix_external_s3_validation/product-slice.probe",
                "external_s3_validation_evidence": {
                    "job": "external-s3-validate-job.json",
                    "log": "external-s3-validate.log"
                },
                "durability_policy_attestation": {
                    "validated": true,
                    "evidence": "object-store-durability-attestation.json",
                    "schema_version": 1,
                    "evidence_kind": "velorix_object_store_durability_policy_attestation",
                    "provider_kind": "s3-compatible",
                    "authority_store_id": "s3://velorix-prod",
                    "bucket": "velorix-prod",
                    "s3_prefix": "",
                    "versioning_or_object_lock_enabled": true,
                    "server_side_encryption_enabled": true,
                    "backup_or_replication_configured": true,
                    "lifecycle_delete_policy_reviewed": true,
                    "destructive_delete_protection_reviewed": true,
                    "cost_controls_reviewed": true,
                    "attested_at": "2026-05-31T00:00:00Z",
                    "attester": "velorix-release-operator"
                }
            },
            "no_pvc": {
                "namespace_validated": true,
                "evidence": "no-pvc-namespace.json",
                "contract": "no PersistentVolumeClaim objects in the Velorix product namespace",
                "managed_hiqlite_authority_validated": true
            },
            "metadata_store": {
                "enabled": true,
                "backend": "hiqlite",
                "hiqlite_authority_attestation": {
                    "validated": true,
                    "evidence": "hiqlite-authority-attestation.json",
                    "authority_kind": "velorix_managed_hiqlite",
                    "schema_version": 1,
                    "nodes": [
                        "http://velorix-hiqlite-0.velorix-hiqlite:8200",
                        "http://velorix-hiqlite-1.velorix-hiqlite:8200",
                        "http://velorix-hiqlite-2.velorix-hiqlite:8200"
                    ],
                    "expected_voter_count": 3,
                    "no_pvc_created_by_vind": true,
                    "metadata_authority_no_pvc_used": true,
                    "metadata_authority_storage_mode": "object-store-backup-restore-with-ephemeral-node-disk",
                    "voters_learner_only_disabled": true,
                    "api_auth_configured": true,
                    "raft_auth_configured": true,
                    "transport_security": "cluster-private-mtls",
                    "backup_restore_configured": true,
                    "image_digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                    "source_revision": "sebadob/hiqlite@abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    "no_pvc_evidence_files": {
                        "namespace_pvc_list": "no-pvc-namespace.json",
                        "hiqlite_statefulset": "no-pvc-hiqlite-statefulset.json",
                        "manifest": "velorix-hiqlite.yaml"
                    },
                    "attested_at": "2026-05-31T00:00:00Z",
                    "attester": "velorix-release-operator"
                },
                "standing_runtime_adversarial_smoke": {
                    "status": if configured_mode == "unsafe-dev-only" { "not_required" } else { "pass" },
                    "assertions": {
                        "logical_owner_expiry_checked": configured_mode != "unsafe-dev-only",
                        "new_owner_epoch_fences_old_owner": configured_mode != "unsafe-dev-only",
                        "stale_owner_checkpoint_publish_rejected": configured_mode != "unsafe-dev-only",
                        "latest_checkpoint_remains_metadata_authoritative": configured_mode != "unsafe-dev-only"
                    }
                }
            },
            "standing_runtime_fencing": {
                "configured_mode": configured_mode,
                "required_mode": required,
                "logical_fencing_mode": logical,
                "capability": {
                    "capability_schema_version": 2,
                    "backend_name": "hiqlite",
                    "owner_scope_kind": "tenant_program_view",
                    "linearizable_owner_lease": configured_mode != "unsafe-dev-only",
                    "durable_monotonic_owner_epoch": configured_mode != "unsafe-dev-only",
                    "authoritative_backend_time": required,
                    "owner_validated_checkpoint_publish": configured_mode != "unsafe-dev-only",
                    "publish_checks_owner_and_latest_atomically": configured_mode != "unsafe-dev-only",
                    "publish_rejects_expired_owner": configured_mode != "unsafe-dev-only",
                    "latest_read_linearizable": configured_mode != "unsafe-dev-only",
                    "publish_rejects_scope_mismatch": configured_mode != "unsafe-dev-only",
                    "max_owner_ttl_ms": if configured_mode == "unsafe-dev-only" { 0 } else { 300000 },
                    "control_plane_auth_enforced": configured_mode != "unsafe-dev-only",
                    "production_multi_writer_safe": required,
                    "backend_time_source_kind": if required {
                        "raft_replicated_authority_time"
                    } else if logical {
                        "unavailable"
                    } else {
                        "process_clock"
                    },
                    "backend_time_blocked_reason": if required {
                        ""
                    } else if logical {
                        "hiqlite_raft_replicated_authority_time_primitive_missing"
                    } else {
                        "unsafe_dev_only_process_local"
                    },
                    "lease_authority_kind": if required {
                        "raft_replicated_time"
                    } else if logical {
                        "hiqlite_raft_serialized"
                    } else {
                        "process_local"
                    },
                    "lease_expiry_semantics": if required {
                        "backend_wall_clock_ttl"
                    } else if logical {
                        "operation_driven_logical"
                    } else {
                        "process_clock_ttl"
                    },
                    "bounded_wall_clock_failover": required,
                    "failover_time_bound_ms": if required { 300000 } else { 0 },
                    "multi_writer_fencing_safe": configured_mode != "unsafe-dev-only",
                    "production_bounded_failover_safe": required
                },
                "multi_replica_fencing_smoke": {
                    "status": if configured_mode == "unsafe-dev-only" { "not_required" } else { "pass" },
                    "enabled": true
                },
                "local_api_pod_failover_smoke": {
                    "status": if configured_mode == "unsafe-dev-only" { "not_required" } else { "pass" },
                    "enabled": configured_mode != "unsafe-dev-only",
                    "evidence": if configured_mode == "unsafe-dev-only" {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!("standing-runtime-failover-smoke.json")
                    },
                    "scope": "local vind product API pod deletion and owner reacquire smoke",
                    "trusted_for_product_complete": false,
                    "production_wall_clock_failover_attestation": false
                }
            },
            "api": {
                "replica_count": if configured_mode == "unsafe-dev-only" { 1 } else { 2 },
                "generic_query_enabled": false,
                "legacy_recovered_sql_views_allowed": false,
                "openapi": {
                    "catalog_smoke_passed": true,
                    "evidence_file": "openapi.json",
                    "promoted_api_path": "/v1/api/scores/positive",
                    "promoted_api_path_present": true,
                    "generic_query_path_absent": true,
                    "legacy_parameterized_path_absent": true,
                    "query_policy_extension_present": true,
                    "linked_view_policy_id": "interactive",
                    "response_schema_checked": true
                },
                "query_policy": {
                    "catalog_smoke_passed": true,
                    "production_bounds_required": true,
                    "weak_policy_rejected": true,
                    "missing_policy_rejected": true,
                    "linked_view_policy_id": "interactive",
                    "evidence_files": {
                        "created": "query-policy-interactive.json",
                        "read_back": "query-policy-interactive-read.json",
                        "weak_policy_rejection": "query-policy-weak-rejection.json",
                        "missing_policy_rejection": "query-policy-missing-view.json"
                    }
                },
                "compile_deploy": {
                    "job_catalog_verified": true,
                    "job_catalog_evidence_file": "view-compile-deploy-jobs.json",
                    "pending_view_id": "pending_scores_by_user",
                    "compiler_request_embedded": true,
                    "admin_route": "/v1/view-compile-deploy/jobs",
                    "worker_run_verified": true,
                    "run_once_admin_route": "/v1/view-compile-deploy/run-once",
                    "run_once_evidence_file": "view-compile-deploy-run-once.json",
                    "activated_view_id": "pending_scores_by_user",
                    "activated_execution_mode": "standing_runtime",
                    "activated_view_evidence_file": "pending-scores-view-after-compile-deploy.json",
                    "activated_query_evidence_file": "pending-scores-query-after-compile-deploy.json"
                },
                "auth": {
                    "mode": "bearer-token",
                    "secret_name": "velorix-api-auth",
                    "admin_secret_name": "velorix-admin-auth",
                    "missing_token_rejected": true,
                    "wrong_token_rejected": true,
                    "correct_token_smoke_passed": true,
                    "healthz_unauthenticated": true,
                    "readyz_unauthenticated": true,
                    "deployment_env_verified": true,
                    "data_plane_token_rejected_on_admin_route": true,
                    "local_tls_auth_smoke": {
                        "enabled": true,
                        "passed": true,
                        "evidence": "tls-auth-smoke.json",
                        "tls_certificate_sha256": "sha256:abcdef0123456789",
                        "cert_authority": "generated-self-signed-local",
                        "scope": "local port-forwarded vind/vCluster service",
                        "public_ingress_attestation": false,
                        "trusted_for_product_complete": false
                    },
                    "ingress_tls_auth_attestation": {
                        "validated": true,
                        "evidence": "ingress-tls-auth-attestation.json",
                        "schema_version": 1,
                        "evidence_kind": "velorix_ingress_tls_auth_attestation",
                        "endpoint_url": "https://velorix.example.com",
                        "external_hostname": "velorix.example.com",
                        "ingress_controller": "nginx",
                        "transport_security": "public-tls",
                        "tls_enabled": true,
                        "tls_certificate_sha256": "sha256:0123456789abcdef",
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
                        "attested_at": current_rfc3339_utc(),
                        "attester": "operator"
                    }
                }
            },
            "ingest_writer": {
                "pod_internal_append_verified": true,
                "evidence_files": {
                    "job_log": "ingest-writer-job-log.json",
                    "job": "ingest-writer-job.json",
                    "pods": "ingest-writer-pods.json"
                },
                "lifecycle_attestation": {
                    "validated": true,
                    "source": "generated",
                    "trusted_for_product_complete": true,
                    "deployment_id": "prod-a",
                    "authority_store_id": "s3://velorix-prod",
                    "pod_internal_append_completed": true,
                    "multi_pod_overlap_conflict_rejected": true,
                    "adjacent_append_succeeded": true,
                    "crash_restart_reconstruction_checked": true,
                    "kubernetes_lease_handoff_checked": true,
                    "lease_held_through_append_checked": true,
                    "commit_guard_checked": true,
                    "admission_commit_guard_bound_checked": true,
                    "lease_loss_during_reservation_checked": true,
                    "no_pvc_created_by_vind": true,
                    "evidence_provenance": {
                        "pod_internal_job": lifecycle_job_provenance_json("pod-internal"),
                        "overlap_job": lifecycle_job_provenance_json("overlap"),
                        "adjacent_job": lifecycle_job_provenance_json("adjacent"),
                        "restart_job": lifecycle_job_provenance_json("restart"),
                        "lease_loss_job": lifecycle_job_provenance_json("lease-loss"),
                        "handoff_owner_a_job": lifecycle_job_provenance_json("handoff-owner-a"),
                        "handoff_owner_b_job": lifecycle_job_provenance_json("handoff-owner-b"),
                        "handoff_stale_owner_job": lifecycle_job_provenance_json("handoff-stale-owner")
                    },
                    "evidence_files": {
                        "pod_internal_job": "velorix-ingest-writer-smoke-log.json",
                        "overlap_job": "velorix-ingest-lifecycle-overlap-log.json",
                        "adjacent_job": "velorix-ingest-lifecycle-adjacent-log.json",
                        "restart_job": "velorix-ingest-lifecycle-restart-log.json",
                        "lease_loss_job": "velorix-ingest-lifecycle-lease-loss-log.json",
                        "handoff_probe_job": "velorix-ingest-lifecycle-handoff-log.json"
                    }
                }
            }
        })
        .to_string()
    }

    fn feldera_spec_json() -> String {
        std::fs::read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("velorix-core")
                .join("tests")
                .join("fixtures")
                .join("feldera")
                .join("standing_view_spec_valid.json"),
        )
        .unwrap()
    }

    fn feldera_metadata_json() -> String {
        let mut metadata: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("..")
                    .join("velorix-core")
                    .join("tests")
                    .join("fixtures")
                    .join("feldera")
                    .join("compile_artifact_valid.json"),
            )
            .unwrap(),
        )
        .unwrap();
        metadata["artifact_hash"] = serde_json::json!(
            "sha256:9063ca4eca6bd69190c68b01a00def0a5b86470abbc2312e94491c59a1c7f537"
        );
        metadata.to_string()
    }

    fn feldera_release_provenance_json() -> String {
        let mut provenance: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("..")
                    .join("velorix-core")
                    .join("tests")
                    .join("fixtures")
                    .join("feldera")
                    .join("release_provenance_valid.json"),
            )
            .unwrap(),
        )
        .unwrap();
        provenance["build"]["artifact_hash"] = serde_json::json!(
            "sha256:9063ca4eca6bd69190c68b01a00def0a5b86470abbc2312e94491c59a1c7f537"
        );
        provenance.to_string()
    }

    fn valid_dependency_governance_json() -> String {
        serde_json::json!({
            "schema_version": 1,
            "msrv": {
                "minimum_rust_version": "1.85.0",
                "policy": "CI and package updates keep the workspace buildable on the declared MSRV."
            },
            "package_reviews": valid_package_reviews_json(),
            "exceptions": [
                {
                    "kind": "duplicate",
                    "crate": "hashbrown",
                    "owner": "runtime",
                    "expires_on": "2026-06-30",
                    "reason": "Await upstream convergence in the DataFusion dependency graph.",
                    "replacement_plan": "Upgrade when upstream dependency ranges converge.",
                    "promotion_rule": "deny_after_expiry_or_renew_with_owner_and_updated_plan"
                },
                {
                    "kind": "unmaintained",
                    "crate": "paste",
                    "owner": "runtime",
                    "expires_on": "2026-06-30",
                    "reason": "Temporary allowance while replacing the transitive dependency.",
                    "replacement_plan": "Replace or remove the transitive dependency.",
                    "promotion_rule": "deny_after_expiry_or_renew_with_owner_and_updated_plan"
                },
                {
                    "kind": "advisory",
                    "crate": "example",
                    "owner": "security",
                    "expires_on": "2026-06-30",
                    "reason": "Advisory does not affect the enabled feature set.",
                    "replacement_plan": "Patch or replace before promotion.",
                    "promotion_rule": "deny_after_expiry_or_renew_with_owner_and_updated_plan"
                }
            ]
        })
        .to_string()
    }

    fn valid_package_reviews_json() -> serde_json::Value {
        serde_json::json!([
            package_review_json("datafusion", "SQL and Parquet query engine; keep features narrow and upgrade with DataFusion releases."),
            package_review_json("object_store", "Authority-path object store client; keep create/read/list semantics explicit."),
            package_review_json("kube", "Kubernetes API client for coordination only; object storage remains authoritative."),
            package_review_json("k8s-openapi", "Kubernetes wire types generated for supported cluster APIs."),
            package_review_json("slatedb", "State substrate; do not duplicate internal LSM or retention semantics."),
            package_review_json("foyer", "Non-authoritative cache only; never part of durable database authority."),
            package_review_json("hiqlite", "Metadata backend; use the mrchypark fork only until required read-replica support lands upstream."),
            package_review_json("feldera_artifacts", "Feldera artifacts require registry metadata and hash verification before release readiness; release provenance remains optional diagnostics.")
        ])
    }

    fn package_review_json(subject: &str, replacement_plan: &str) -> serde_json::Value {
        serde_json::json!({
            "subject": subject,
            "owner": "runtime",
            "reviewed_on": "2026-05-07",
            "audit_status": "local_reviewed_until_external_audit",
            "feature_policy": "Use only the features documented in package reviews and CI-checked manifests.",
            "replacement_plan": replacement_plan
        })
    }

    fn dependency_governance_json_with_exceptions<const N: usize>(
        exceptions: [(&str, &str); N],
    ) -> String {
        let exceptions = exceptions
            .into_iter()
            .map(|(kind, crate_name)| {
                serde_json::json!({
                    "kind": kind,
                    "crate": crate_name,
                    "owner": "runtime",
                    "expires_on": "2026-06-30",
                    "reason": "Temporary warning exception.",
                    "replacement_plan": "Upgrade, replace, or remove the dependency before expiry.",
                    "promotion_rule": "deny_after_expiry_or_renew_with_owner_and_updated_plan"
                })
            })
            .collect::<Vec<_>>();

        serde_json::json!({
            "schema_version": 1,
            "msrv": {
                "minimum_rust_version": "1.85.0",
                "policy": "CI and package updates keep the workspace buildable on the declared MSRV."
            },
            "package_reviews": valid_package_reviews_json(),
            "exceptions": exceptions
        })
        .to_string()
    }

    fn cargo_deny_warning_diagnostics_jsonl() -> &'static str {
        concat!(
            r#"{"type":"diagnostic","fields":{"code":"duplicate","severity":"warning","message":"found duplicate entries for crate 'hashbrown'","graphs":[{"Krate":{"name":"hashbrown","version":"0.15.5"}},{"Krate":{"name":"hashbrown","version":"0.16.0"}}]}}"#,
            "\n",
            r#"{"type":"diagnostic","fields":{"code":"unmaintained","severity":"warning","message":"paste - no longer maintained","labels":[{"span":"paste 1.0.15 registry+https://github.com/rust-lang/crates.io-index"}]}}"#,
            "\n",
            r#"{"type":"summary","fields":{"bans":{"warnings":1},"advisories":{"warnings":1}}}"#,
        )
    }

    fn valid_result_json_compact() -> String {
        serde_json::to_string(&normal_result(
            "abc123",
            "pr_smoke",
            "local",
            "local_incremental",
            1000.0,
            workload_metrics(),
        ))
        .unwrap()
    }

    fn s3_result_json() -> String {
        serde_json::to_string_pretty(&normal_result(
            "abc123",
            "pr_smoke",
            "s3_compatible",
            "s3_incremental",
            1000.0,
            s3_workload_metrics(),
        ))
        .unwrap()
    }

    fn release_result_json() -> String {
        serde_json::to_string_pretty(&normal_result(
            "abc123",
            "release",
            "s3_compatible",
            "s3_incremental",
            1000.0,
            s3_workload_metrics(),
        ))
        .unwrap()
    }

    fn nightly_result_json() -> String {
        serde_json::to_string_pretty(&normal_result(
            "abc123",
            "nightly_integration",
            "s3_compatible",
            "s3_incremental",
            1000.0,
            s3_workload_metrics(),
        ))
        .unwrap()
    }

    fn local_emulator_s3_result_json() -> String {
        let mut result = normal_result(
            "abc123",
            "release",
            "s3_compatible",
            "s3_incremental",
            1000.0,
            s3_workload_metrics(),
        );
        result["backend_evidence_scope"] = serde_json::json!("local_emulator");
        serde_json::to_string_pretty(&result).unwrap()
    }

    fn regressed_result_json() -> String {
        serde_json::to_string_pretty(&normal_result(
            "def456",
            "pr_smoke",
            "local",
            "local_incremental",
            800.0,
            workload_metrics(),
        ))
        .unwrap()
    }

    fn missing_required_workload_json() -> String {
        serde_json::to_string_pretty(&normal_result(
            "def456",
            "pr_smoke",
            "local",
            "local_incremental",
            1000.0,
            serde_json::json!([
                workload_metrics()[0],
                workload_metrics()[1],
                workload_metrics()[2],
                workload_metrics()[3],
                workload_metrics()[4]
            ]),
        ))
        .unwrap()
    }

    fn missing_gc_execution_workload_json() -> String {
        serde_json::to_string_pretty(&normal_result(
            "abc123",
            "pr_smoke",
            "local",
            "local_incremental",
            1000.0,
            s3_workload_metrics(),
        ))
        .unwrap()
    }

    fn placeholder_release_baseline_json() -> String {
        serde_json::to_string_pretty(&serde_json::json!({
            "schema_version": 1,
            "commit": "placeholder-s3-release-baseline",
            "gate_level": "release",
            "backend": "s3_compatible",
            "workload": "s3_incremental",
            "metrics": {
                "rows_per_second": 1.0,
                "bytes_per_row": 1000000000.0,
                "put_per_gib": 1000000000.0,
                "object_requests": {
                    "put_count": 1000000,
                    "get_count": 1000000,
                    "list_count": 1000000,
                    "range_read_count": 1000000,
                    "bytes_written": 1000000000000_u64,
                    "bytes_read": 1000000000000_u64,
                },
                "checkpoint_p50_ms": 600000.0,
                "checkpoint_p95_ms": 600000.0,
                "recovery_p95_ms": 600000.0,
                "peak_rss_bytes": 1099511627776_u64,
                "spill_bytes": 1099511627776_u64,
                "scan_bytes": 1099511627776_u64,
            },
            "workload_metrics": workload_metrics(),
        }))
        .unwrap()
    }

    fn placeholder_nightly_baseline_json() -> String {
        serde_json::to_string_pretty(&normal_result(
            "placeholder-s3-nightly-baseline",
            "nightly_integration",
            "s3_compatible",
            "s3_incremental",
            1.0,
            workload_metrics(),
        ))
        .unwrap()
    }

    fn placeholder_current_s3_result_json() -> String {
        serde_json::to_string_pretty(&normal_result(
            "placeholder-s3-nightly-result",
            "release",
            "s3_compatible",
            "s3_incremental",
            1000.0,
            workload_metrics(),
        ))
        .unwrap()
    }

    fn placeholder_current_local_result_json() -> String {
        serde_json::to_string_pretty(&normal_result(
            "placeholder-local-pr-smoke-result",
            "pr_smoke",
            "local",
            "local_incremental",
            1000.0,
            workload_metrics(),
        ))
        .unwrap()
    }

    fn placeholder_local_baseline_json() -> String {
        serde_json::to_string_pretty(&normal_result(
            "placeholder-local-pr-smoke-baseline",
            "pr_smoke",
            "local",
            "local_incremental",
            1000.0,
            workload_metrics(),
        ))
        .unwrap()
    }

    fn release_status_matrix(
        default_status: &str,
        omitted_contracts: &[&str],
        overrides: &[(&str, &str)],
    ) -> String {
        release_status_matrix_with_blocking_tasks(default_status, omitted_contracts, overrides, &[])
    }

    fn release_status_matrix_with_blocking_tasks(
        default_status: &str,
        omitted_contracts: &[&str],
        overrides: &[(&str, &str)],
        blocking_overrides: &[(&str, &str)],
    ) -> String {
        let mut matrix = String::from(
            "| Contract | Current Evidence | 1.0 Required Evidence | Status | Blocking Tasks |\n\
             | --- | --- | --- | --- | --- |\n",
        );

        for contract in REQUIRED_RELEASE_CONTRACTS {
            if omitted_contracts.contains(contract) {
                continue;
            }
            let status = overrides
                .iter()
                .find_map(|(name, status)| (*name == *contract).then_some(*status))
                .unwrap_or(default_status);
            let blocking_tasks = blocking_overrides
                .iter()
                .find_map(|(name, tasks)| (*name == *contract).then_some(*tasks))
                .unwrap_or("none");
            matrix.push_str(&format!(
                "| {contract} | evidence | required evidence | {status} | {blocking_tasks} |\n"
            ));
        }

        matrix
    }

    fn normal_result(
        commit: &str,
        gate_level: &str,
        backend: &str,
        workload: &str,
        rows_per_second: f64,
        workload_metrics: serde_json::Value,
    ) -> serde_json::Value {
        let mut result = serde_json::json!({
            "schema_version": 1,
            "commit": commit,
            "gate_level": gate_level,
            "backend": backend,
            "workload": workload,
            "metrics": {
                "rows_per_second": rows_per_second,
                "bytes_per_row": 128.0,
                "put_per_gib": 8.0,
                "object_requests": {
                    "put_count": 8,
                    "get_count": 3,
                    "list_count": 2,
                    "range_read_count": 0,
                    "bytes_written": 1024,
                    "bytes_read": 512,
                },
                "checkpoint_p50_ms": 3.0,
                "checkpoint_p95_ms": 4.0,
                "recovery_p95_ms": 7.0,
                "peak_rss_bytes": 0,
                "spill_bytes": 0,
                "scan_bytes": 0,
            },
            "workload_metrics": workload_metrics,
        });
        if backend == "s3_compatible" {
            result["backend_evidence_scope"] = serde_json::json!("live_or_native");
        }
        result
    }

    fn workload_metrics() -> serde_json::Value {
        serde_json::json!([
            {
                "name": "object_store_capability_probe",
                "p50_ms": 1.0,
                "p95_ms": 1.0,
                "object_requests": {
                    "put_count": 4,
                    "get_count": 4,
                    "list_count": 4,
                    "range_read_count": 0,
                    "bytes_written": 1024,
                    "bytes_read": 1024,
                },
                "scan_bytes": 0,
            },
            {
                "name": "ingest_envelope_validation",
                "p50_ms": 2.0,
                "p95_ms": 3.0,
                "object_requests": {
                    "put_count": 4,
                    "get_count": 0,
                    "list_count": 0,
                    "range_read_count": 0,
                    "bytes_written": 1024,
                    "bytes_read": 0,
                },
                "scan_bytes": 0,
            },
            {
                "name": "checkpoint_publish",
                "p50_ms": 3.0,
                "p95_ms": 4.0,
                "object_requests": {
                    "put_count": 2,
                    "get_count": 0,
                    "list_count": 0,
                    "range_read_count": 0,
                    "bytes_written": 512,
                    "bytes_read": 0,
                },
                "scan_bytes": 0,
            },
            {
                "name": "checkpoint_recovery",
                "p50_ms": 7.0,
                "p95_ms": 7.0,
                "object_requests": {
                    "put_count": 0,
                    "get_count": 2,
                    "list_count": 1,
                    "range_read_count": 0,
                    "bytes_written": 0,
                    "bytes_read": 512,
                },
                "scan_bytes": 0,
            },
            {
                "name": "datafusion_table_scan",
                "p50_ms": 5.0,
                "p95_ms": 6.0,
                "object_requests": {
                    "put_count": 0,
                    "get_count": 1,
                    "list_count": 1,
                    "range_read_count": 2,
                    "bytes_written": 0,
                    "bytes_read": 2048,
                },
                "scan_bytes": 1024,
            },
            {
                "name": "slatedb_state_reopen",
                "p50_ms": 8.0,
                "p95_ms": 9.0,
                "object_requests": {
                    "put_count": 1,
                    "get_count": 1,
                    "list_count": 1,
                    "range_read_count": 0,
                    "bytes_written": 256,
                    "bytes_read": 256,
                },
                "scan_bytes": 0,
            },
            {
                "name": "gc_dry_run_planning",
                "p50_ms": 4.0,
                "p95_ms": 5.0,
                "object_requests": {
                    "put_count": 0,
                    "get_count": 2,
                    "list_count": 3,
                    "range_read_count": 0,
                    "bytes_written": 0,
                    "bytes_read": 1024,
                },
                "scan_bytes": 0,
            },
            {
                "name": "gc_execution_evidence",
                "p50_ms": 4.0,
                "p95_ms": 5.0,
                "object_requests": {
                    "put_count": 1,
                    "get_count": 4,
                    "list_count": 4,
                    "range_read_count": 0,
                    "bytes_written": 1024,
                    "bytes_read": 2048,
                },
                "scan_bytes": 0,
            },
        ])
    }

    fn s3_workload_metrics() -> serde_json::Value {
        let mut workloads = workload_metrics().as_array().unwrap().clone();
        workloads.retain(|workload| workload["name"] != "gc_execution_evidence");
        serde_json::Value::Array(workloads)
    }
}
