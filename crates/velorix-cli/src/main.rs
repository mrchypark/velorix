#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs,
    path::Path,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context};
use clap::{CommandFactory, Parser, Subcommand};
use object_store::{local::LocalFileSystem, ObjectStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
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

const LOCAL_BENCHMARK_GATE_WORKLOADS: &[&str] = &[
    "object_store_capability_probe",
    "ingest_envelope_validation",
    "checkpoint_publish",
    "checkpoint_recovery",
    "datafusion_table_scan",
    "slatedb_state_reopen",
    "gc_dry_run_planning",
    "gc_execution_evidence",
];
const S3_COMPATIBLE_BENCHMARK_GATE_WORKLOADS: &[&str] = &[
    "object_store_capability_probe",
    "ingest_envelope_validation",
    "checkpoint_publish",
    "checkpoint_recovery",
    "datafusion_table_scan",
    "slatedb_state_reopen",
    "gc_dry_run_planning",
];
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
    capability::probe_authoritative_object_store_capabilities,
    checkpoint_index::{
        CheckpointAdminInspection, CheckpointLifecycleStatus, CheckpointManifestInspectionStatus,
        CheckpointRetentionRecordV1,
    },
    gc::{GarbageCollectionPlan, GarbageCollectionPolicy, GarbageCollectionRunV1},
    relation_catalog_registry::RelationCatalogRegistry,
    state::CheckpointPublisher,
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
            let inspection = CheckpointPublisher::new(store)
                .inspect_checkpoints()
                .await
                .context("failed to inspect local checkpoints")?;

            if json {
                println!("{}", format_checkpoint_inspection_json(&inspection)?);
            } else {
                print!("{}", format_checkpoint_inspection(&inspection));
            }
        }
        Some(Command::GcPlanLocal {
            object_store_dir,
            retain_latest_manifests,
            json,
        }) => {
            let store = local_object_store(&object_store_dir)?;
            let plan = CheckpointPublisher::new(store)
                .plan_garbage_collection(GarbageCollectionPolicy {
                    retain_latest_manifests,
                })
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
            let publisher = CheckpointPublisher::new(store);
            let policy = GarbageCollectionPolicy {
                retain_latest_manifests,
            };
            let plan = publisher
                .plan_garbage_collection(policy)
                .await
                .context("failed to plan local garbage collection")?;
            let run = publisher
                .execute_garbage_collection_plan_with_evidence(&run_id, policy, &plan)
                .await
                .context("failed to execute local garbage collection")?;

            if json {
                println!("{}", format_gc_run_json(&run)?);
            } else {
                print!("{}", format_gc_run(&run));
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
            dependency_governance_evidence,
            dependency_governance_manifest,
            release_commit,
            feldera_artifact_hash_evidence,
            feldera_release_provenance_evidence,
            s3_release_benchmark_gate_evidence,
            production_gc_run_evidence,
            json,
        }) => {
            let artifacts = ReadinessReleaseArtifactPaths {
                require_release_artifacts,
                dependency_governance_evidence,
                dependency_governance_manifest,
                release_commit,
                feldera_artifact_hash_evidence,
                feldera_release_provenance_evidence,
                s3_release_benchmark_gate_evidence,
                production_gc_run_evidence,
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
    dependency_governance_evidence: Option<PathBuf>,
    dependency_governance_manifest: Option<PathBuf>,
    release_commit: Option<String>,
    feldera_artifact_hash_evidence: Option<PathBuf>,
    feldera_release_provenance_evidence: Option<PathBuf>,
    s3_release_benchmark_gate_evidence: Option<PathBuf>,
    production_gc_run_evidence: Option<PathBuf>,
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
    let report = evidence.try_into_report().map_err(anyhow::Error::msg)?;
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
    if artifacts.require_release_artifacts {
        require_artifact_path(
            "dependency-governance-evidence",
            &artifacts.dependency_governance_evidence,
        )?;
        require_artifact_path(
            "dependency-governance-manifest",
            &artifacts.dependency_governance_manifest,
        )?;
        require_release_commit(&artifacts.release_commit)?;
        require_artifact_path(
            "feldera-artifact-hash-evidence",
            &artifacts.feldera_artifact_hash_evidence,
        )?;
        require_artifact_path(
            "feldera-release-provenance-evidence",
            &artifacts.feldera_release_provenance_evidence,
        )?;
        require_artifact_path(
            "s3-release-benchmark-gate-evidence",
            &artifacts.s3_release_benchmark_gate_evidence,
        )?;
        require_artifact_path(
            "production-gc-run-evidence",
            &artifacts.production_gc_run_evidence,
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
    if let (Some(hash_path), Some(provenance_path)) = (
        &artifacts.feldera_artifact_hash_evidence,
        &artifacts.feldera_release_provenance_evidence,
    ) {
        validate_feldera_release_evidence_artifacts(hash_path, provenance_path)?;
    } else if artifacts.feldera_artifact_hash_evidence.is_some()
        || artifacts.feldera_release_provenance_evidence.is_some()
    {
        bail!("Feldera release evidence requires both hash and provenance artifacts");
    }
    if let Some(path) = &artifacts.s3_release_benchmark_gate_evidence {
        validate_s3_release_benchmark_gate_evidence_artifact(path)?;
    }
    if let Some(path) = &artifacts.production_gc_run_evidence {
        validate_production_gc_run_evidence_artifact(path, deployment_id, authority_store_id)?;
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

fn require_release_commit(release_commit: &Option<String>) -> anyhow::Result<()> {
    match release_commit.as_deref().map(str::trim) {
        Some(value) if !value.is_empty() => Ok(()),
        _ => bail!("readiness-report --require-release-artifacts requires --release-commit"),
    }
}

fn validate_dependency_governance_evidence_artifact(
    path: &Path,
    release_commit: Option<&str>,
    manifest_path: Option<&Path>,
) -> anyhow::Result<()> {
    reject_local_readiness_artifact(&read_artifact_evidence_kind(path)?, path)?;
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
    if !artifact.external_audit_attestation {
        bail!(
            "{} dependency governance evidence is local-only and missing external audit attestation",
            path.display()
        );
    }
    let Some(external_audit) = artifact.external_audit.as_ref() else {
        bail!(
            "{} dependency governance evidence is missing external audit details",
            path.display()
        );
    };
    require_external_audit_field(path, &external_audit.provider, "provider")?;
    require_external_audit_field(path, &external_audit.tool, "tool")?;
    require_external_audit_field(path, &external_audit.subject_commit, "subject_commit")?;
    require_external_audit_field(path, &external_audit.manifest_digest, "manifest_digest")?;
    require_external_audit_field(path, &external_audit.completed_at, "completed_at")?;
    require_external_audit_field(path, &external_audit.attestation_uri, "attestation_uri")?;
    if external_audit.tool != "cargo-vet" {
        bail!(
            "{} dependency governance external audit tool must be cargo-vet",
            path.display()
        );
    }
    if external_audit.result != "pass" {
        bail!(
            "{} dependency governance external audit result is not pass",
            path.display()
        );
    }
    if is_placeholder_commit(&external_audit.subject_commit) {
        bail!(
            "{} dependency governance external audit uses placeholder subject_commit",
            path.display()
        );
    }
    if !external_audit.manifest_digest.starts_with("sha256:") {
        bail!(
            "{} dependency governance external audit manifest_digest must start with sha256:",
            path.display()
        );
    }
    let Some(release_commit) = release_commit
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        bail!("readiness-report with --dependency-governance-evidence requires --release-commit");
    };
    if external_audit.subject_commit != release_commit {
        bail!(
            "{} dependency governance external audit subject_commit does not match release commit",
            path.display()
        );
    }
    let Some(manifest_path) = manifest_path else {
        bail!(
            "readiness-report with --dependency-governance-evidence requires --dependency-governance-manifest"
        );
    };
    let manifest_digest = sha256_digest_path(manifest_path)?;
    if external_audit.manifest_digest != manifest_digest {
        bail!(
            "{} dependency governance external audit manifest_digest does not match {}",
            path.display(),
            manifest_path.display()
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

fn validate_feldera_release_evidence_artifacts(
    hash_path: &Path,
    provenance_path: &Path,
) -> anyhow::Result<()> {
    let hash: FelderaArtifactHashVerifiedEvidenceV1 = read_json_artifact(hash_path)?;
    let provenance: FelderaArtifactReleaseProvenanceEvidenceV1 =
        read_json_artifact(provenance_path)?;

    if hash.schema_version != 1 {
        bail!(
            "{} has unsupported schema_version {}, expected 1",
            hash_path.display(),
            hash.schema_version
        );
    }
    if provenance.schema_version != 1 {
        bail!(
            "{} has unsupported schema_version {}, expected 1",
            provenance_path.display(),
            provenance.schema_version
        );
    }
    if hash.evidence_kind != ReadinessEvidenceKind::FelderaArtifactHashVerified {
        bail!(
            "{} has evidence_kind {:?}, expected feldera_artifact_hash_verified",
            hash_path.display(),
            hash.evidence_kind
        );
    }
    if provenance.evidence_kind != ReadinessEvidenceKind::FelderaArtifactReleaseProvenance {
        bail!(
            "{} has evidence_kind {:?}, expected feldera_artifact_release_provenance",
            provenance_path.display(),
            provenance.evidence_kind
        );
    }
    if hash.status != ReadinessStatus::Pass {
        bail!(
            "{} Feldera artifact hash evidence is not pass",
            hash_path.display()
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

    Ok(())
}

fn reject_local_readiness_artifact(evidence_kind: &str, path: &Path) -> anyhow::Result<()> {
    if matches!(
        evidence_kind,
        "floci_s3_compatible_gate" | "kubernetes_vind_gate" | "local_benchmark_gate"
    ) {
        bail!(
            "{} is local-scoped evidence ({evidence_kind}) and cannot satisfy release readiness",
            path.display()
        );
    }
    Ok(())
}

fn require_external_audit_field(path: &Path, value: &str, field: &str) -> anyhow::Result<()> {
    if value.trim().is_empty() {
        bail!(
            "{} dependency governance external audit is missing {field}",
            path.display()
        );
    }

    Ok(())
}

fn sha256_digest_path(path: &Path) -> anyhow::Result<String> {
    let contents = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let digest = Sha256::digest(contents);
    let mut value = String::from("sha256:");
    for byte in digest {
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(value)
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

fn is_local_dev_authority_store_id(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    [
        "memory://",
        "file://",
        "localhost",
        "127.0.0.1",
        "floci",
        "vind",
        "vcluster",
        "emulator",
        "local-only",
        "local only",
        "local filesystem",
    ]
    .iter()
    .any(|marker| value.contains(marker))
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
    #[serde(default)]
    external_audit_attestation: bool,
    external_audit: Option<ExternalAuditAttestationArtifactV1>,
    missing_required_package_review_subjects: Vec<String>,
    #[serde(flatten)]
    _extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ExternalAuditAttestationArtifactV1 {
    provider: String,
    tool: String,
    result: String,
    subject_commit: String,
    manifest_digest: String,
    completed_at: String,
    attestation_uri: String,
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

#[derive(Debug, Deserialize)]
struct ProductionGcRunEvidenceArtifactV1 {
    schema_version: u16,
    status: String,
    evidence_kind: String,
    deployment_id: String,
    authority_store_id: String,
    gc_run_id: String,
    listing_consistency_checked: bool,
    checkpoint_retention_records_checked: bool,
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
            RecoveredRuntime::recover_with_owner_and_relation_catalog_record(
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
            let relation_catalog = RelationCatalogRegistry::new(Arc::clone(&store))
                .read(&relation_id, &relation_version)
                .await?;
            Ok(RecoveredRuntime::recover_from_published_checkpoint_version_with_owner_and_relation_catalog(
                store,
                checkpoint_version,
                ORDERS_SUM_COUNT_OWNER,
                relation_catalog,
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

fn format_checkpoint_inspection(inspection: &CheckpointAdminInspection) -> String {
    let latest = inspection
        .latest_valid_checkpoint
        .map_or_else(|| "none".to_string(), |checkpoint| checkpoint.to_string());
    let mut output = format!("latest_valid_checkpoint={latest}\nmanifests:\n");

    for manifest in &inspection.manifests {
        output.push_str(&format!(
            "checkpoint={} key={} lifecycle={} retention={} recovery_transitions={} status={}\n",
            manifest.checkpoint_version,
            manifest.manifest_key,
            format_lifecycle_status(manifest.lifecycle_status),
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
        schema_version: 2,
        inspection,
    })
    .context("failed to serialize checkpoint inspection")
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
    use bytes::Bytes;
    use object_store::path::Path as ObjectStorePath;
    use tempfile::tempdir;
    use velorix_runtime::recovery::{orders_sum_count_relation_catalog, RecoveryError};
    use velorix_storage::{
        checkpoint_index::{
            CheckpointAdminInspection, CheckpointLifecycleStatus, CheckpointManifestInspection,
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
    const TEST_DEPENDENCY_MANIFEST_CONTENTS: &str = "dependency governance test manifest\n";
    const TEST_DEPENDENCY_MANIFEST_DIGEST: &str =
        "sha256:a40627c73380c28e7fff1a5dac4a874fb85ce79366d67a51570904715934ea88";

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
            "expected recover-local SlateDB path to fail in capability probe, got: {message}"
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
            dependency_governance_evidence,
            dependency_governance_manifest,
            release_commit,
            feldera_artifact_hash_evidence,
            feldera_release_provenance_evidence,
            s3_release_benchmark_gate_evidence,
            production_gc_run_evidence,
            json,
        }) = cli.command
        else {
            panic!("expected readiness-report command");
        };

        assert_eq!(evidence, PathBuf::from("readiness.json"));
        assert!(!require_release_artifacts);
        assert!(dependency_governance_evidence.is_none());
        assert!(dependency_governance_manifest.is_none());
        assert!(release_commit.is_none());
        assert!(feldera_artifact_hash_evidence.is_none());
        assert!(feldera_release_provenance_evidence.is_none());
        assert!(s3_release_benchmark_gate_evidence.is_none());
        assert!(production_gc_run_evidence.is_none());
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
        ])
        .unwrap();

        let Some(Command::ReadinessReport {
            require_release_artifacts,
            dependency_governance_evidence,
            dependency_governance_manifest,
            release_commit,
            feldera_artifact_hash_evidence,
            feldera_release_provenance_evidence,
            s3_release_benchmark_gate_evidence,
            production_gc_run_evidence,
            ..
        }) = cli.command
        else {
            panic!("expected readiness-report command");
        };

        assert!(require_release_artifacts);
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
    fn readiness_report_validates_release_evidence_artifacts_when_required() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let dependency_manifest = write_dependency_governance_manifest(dir.path());
        let feldera_hash = dir.path().join("feldera-hash.json");
        let feldera_provenance = dir.path().join("feldera-provenance.json");
        let s3_gate = dir.path().join("s3-gate.json");
        let production_gc = dir.path().join("production-gc.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json(true)).unwrap();
        fs::write(&feldera_hash, feldera_hash_evidence_json()).unwrap();
        fs::write(&feldera_provenance, feldera_provenance_evidence_json()).unwrap();
        fs::write(&s3_gate, s3_release_benchmark_gate_json()).unwrap();
        fs::write(&production_gc, production_gc_run_evidence_json()).unwrap();

        let report = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                require_release_artifacts: true,
                dependency_governance_evidence: Some(dependency),
                dependency_governance_manifest: Some(dependency_manifest),
                release_commit: Some(TEST_RELEASE_COMMIT.to_string()),
                feldera_artifact_hash_evidence: Some(feldera_hash),
                feldera_release_provenance_evidence: Some(feldera_provenance),
                s3_release_benchmark_gate_evidence: Some(s3_gate),
                production_gc_run_evidence: Some(production_gc),
            },
        )
        .unwrap();

        assert!(report.production_ready);
    }

    #[test]
    fn readiness_report_rejects_local_dependency_governance_release_evidence() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json(false)).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                dependency_governance_evidence: Some(dependency),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("missing external audit attestation"));
    }

    #[test]
    fn readiness_report_rejects_dependency_governance_evidence_without_attestation_field() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let mut dependency_json: serde_json::Value =
            serde_json::from_str(&dependency_governance_evidence_json(true)).unwrap();
        dependency_json
            .as_object_mut()
            .unwrap()
            .remove("external_audit_attestation");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_json.to_string()).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                dependency_governance_evidence: Some(dependency),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("missing external audit attestation"));
    }

    #[test]
    fn readiness_report_rejects_dependency_governance_evidence_without_external_audit_details() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let mut dependency_json: serde_json::Value =
            serde_json::from_str(&dependency_governance_evidence_json(true)).unwrap();
        dependency_json
            .as_object_mut()
            .unwrap()
            .remove("external_audit");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_json.to_string()).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                dependency_governance_evidence: Some(dependency),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("missing external audit details"));
    }

    #[test]
    fn readiness_report_rejects_dependency_governance_evidence_without_cargo_vet_attestation() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let mut dependency_json: serde_json::Value =
            serde_json::from_str(&dependency_governance_evidence_json(true)).unwrap();
        dependency_json["external_audit"]["tool"] = serde_json::json!("manual-review");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_json.to_string()).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                dependency_governance_evidence: Some(dependency),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("tool must be cargo-vet"));
    }

    #[test]
    fn readiness_report_rejects_dependency_governance_evidence_for_different_commit() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let dependency_manifest = write_dependency_governance_manifest(dir.path());
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json(true)).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                dependency_governance_evidence: Some(dependency),
                dependency_governance_manifest: Some(dependency_manifest),
                release_commit: Some("abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string()),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("subject_commit does not match release commit"));
    }

    #[test]
    fn readiness_report_rejects_dependency_governance_evidence_for_different_manifest() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let dependency_manifest = dir.path().join("dependency-governance.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json(true)).unwrap();
        fs::write(&dependency_manifest, "different manifest\n").unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                dependency_governance_evidence: Some(dependency),
                dependency_governance_manifest: Some(dependency_manifest),
                release_commit: Some(TEST_RELEASE_COMMIT.to_string()),
                ..ReadinessReleaseArtifactPaths::default()
            },
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("manifest_digest does not match"));
    }

    #[test]
    fn readiness_report_requires_production_gc_artifact_when_release_artifacts_required() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let dependency = dir.path().join("dependency.json");
        let dependency_manifest = write_dependency_governance_manifest(dir.path());
        let feldera_hash = dir.path().join("feldera-hash.json");
        let feldera_provenance = dir.path().join("feldera-provenance.json");
        let s3_gate = dir.path().join("s3-gate.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(&dependency, dependency_governance_evidence_json(true)).unwrap();
        fs::write(&feldera_hash, feldera_hash_evidence_json()).unwrap();
        fs::write(&feldera_provenance, feldera_provenance_evidence_json()).unwrap();
        fs::write(&s3_gate, s3_release_benchmark_gate_json()).unwrap();

        let error = read_readiness_report(
            &readiness,
            &ReadinessReleaseArtifactPaths {
                require_release_artifacts: true,
                dependency_governance_evidence: Some(dependency),
                dependency_governance_manifest: Some(dependency_manifest),
                release_commit: Some(TEST_RELEASE_COMMIT.to_string()),
                feldera_artifact_hash_evidence: Some(feldera_hash),
                feldera_release_provenance_evidence: Some(feldera_provenance),
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
    fn readiness_report_rejects_floci_artifact_as_s3_release_benchmark_evidence() {
        let dir = tempdir().unwrap();
        let readiness = dir.path().join("readiness.json");
        let s3_gate = dir.path().join("floci.json");
        fs::write(&readiness, readiness_json()).unwrap();
        fs::write(
            &s3_gate,
            serde_json::json!({
                "schema_version": 1,
                "evidence_kind": "floci_s3_compatible_gate",
                "readiness_evidence_kind": [
                    "s3_compatible",
                    "s3_compatible_integration_harness"
                ],
                "scope": "local floci S3-compatible emulator evidence"
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
            "checkpoint=3 key=v1/checkpoints/00000000000000000003.manifest lifecycle=published retention=gc_run=run-0001 deleted=1 recovery_transitions=1 status=valid\n",
            "checkpoint=8 key=v1/checkpoints/00000000000000000008.manifest lifecycle=none retention=none recovery_transitions=0 status=invalid reason=missing visible parent checkpoint 7\n",
        );

        assert_eq!(format_checkpoint_inspection(&summary), expected);
    }

    #[test]
    fn checkpoint_inspection_json_uses_stable_schema_version() {
        let json = format_checkpoint_inspection_json(&checkpoint_inspection_summary()).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["schema_version"], 2);
        assert_eq!(value["latest_valid_checkpoint"], 7);
        assert_eq!(value["manifests"][0]["status"], "valid");
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
                    retention_record: Some(checkpoint_retention_record(3)),
                    recovery_transition_records: vec![checkpoint_recovery_transition_record(3)],
                    status: CheckpointManifestInspectionStatus::Valid,
                },
                CheckpointManifestInspection {
                    checkpoint_version: 8,
                    manifest_key: ObjectKey::checkpoint_manifest(8),
                    lifecycle_status: None,
                    retention_record: None,
                    recovery_transition_records: vec![],
                    status: CheckpointManifestInspectionStatus::Invalid {
                        reason: "missing visible parent checkpoint\n7".to_string(),
                    },
                },
            ],
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
            "schema_version": 4,
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
                "evidence_kind": ["catalog_backed_ingest_admission", "deployed_ingest_admission"]
            },
            "relation_catalog_status": {
                "status": "pass",
                "evidence": "durable relation catalog record and registry",
                "evidence_kind": ["relation_catalog_record", "relation_catalog_registry"]
            },
            "state_status": {
                "status": "pass",
                "evidence": "SlateDB checkpoint ref",
                "evidence_kind": ["slate_db_checkpoint_ref"]
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
                    "feldera_artifact_hash_verified",
                    "feldera_artifact_release_provenance"
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

    fn dependency_governance_evidence_json(external_audit_attestation: bool) -> String {
        let mut evidence = serde_json::json!({
            "schema_version": 1,
            "status": "pass",
            "evidence_kind": "dependency_governance_validated",
            "cargo_deny": {
                "diagnostics_checked": true,
                "diagnostics_path": "target/dependency-governance/cargo-deny.jsonl"
            },
            "external_audit_attestation": external_audit_attestation,
            "missing_required_package_review_subjects": []
        });
        if external_audit_attestation {
            evidence["external_audit"] = serde_json::json!({
                "provider": "cargo-vet-review-board",
                "tool": "cargo-vet",
                "result": "pass",
                "subject_commit": TEST_RELEASE_COMMIT,
                "manifest_digest": TEST_DEPENDENCY_MANIFEST_DIGEST,
                "completed_at": "2026-05-13T00:00:00Z",
                "attestation_uri": "https://example.invalid/velorix/dependency-governance/attestation.json"
            });
        }

        evidence.to_string()
    }

    fn write_dependency_governance_manifest(parent: &Path) -> PathBuf {
        let path = parent.join("dependency-governance.json");
        fs::write(&path, TEST_DEPENDENCY_MANIFEST_CONTENTS).unwrap();
        path
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
            "checkpoint_retention_records_checked": true
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
            package_review_json("feldera_artifacts", "Feldera artifacts are trusted only after release provenance is available.")
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
