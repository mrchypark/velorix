#![forbid(unsafe_code)]

use std::{
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
use velorix_runtime::benchmark_gate::{
    BenchmarkBackend, BenchmarkBudgetV1, BenchmarkGateLevel, BenchmarkGateResultV1,
};
use velorix_runtime::readiness::{ProductionReadinessEvidenceV1, ProductionReadinessReportV1};

const BENCHMARK_GATE_WORKLOADS: &[&str] = &[
    "ingest_envelope_validation",
    "checkpoint_publish",
    "checkpoint_recovery",
    "datafusion_table_scan",
    "slatedb_state_reopen",
    "gc_dry_run_planning",
];
use velorix_runtime::recovery::RecoveredRuntime;
use velorix_storage::{
    checkpoint_index::{
        CheckpointAdminInspection, CheckpointLifecycleStatus, CheckpointManifestInspectionStatus,
    },
    gc::{GarbageCollectionPlan, GarbageCollectionPolicy},
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
    },
    ReadinessReport {
        #[arg(long)]
        evidence: PathBuf,
        #[arg(long)]
        json: bool,
    },
    DependencyGovernanceValidate {
        #[arg(long)]
        manifest: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::RecoverLocal { object_store_dir }) => {
            let recovered = RecoveredRuntime::recover(local_object_store(&object_store_dir)?)
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
        }) => {
            run_benchmark_gate(
                Some(&baseline),
                &result,
                Some(gate_level),
                Some(backend),
                Some(max_regression_fraction),
            )?;
            println!("benchmark gate passed");
        }
        Some(Command::ReadinessReport { evidence, json }) => {
            let report = read_readiness_report(&evidence)?;
            if json {
                println!("{}", report.to_json_pretty()?);
            } else {
                print!("{}", format_readiness_report(&report));
            }
        }
        Some(Command::DependencyGovernanceValidate { manifest }) => {
            validate_dependency_governance_file(&manifest)?;
            println!("dependency governance manifest valid");
        }
        None => {
            Cli::command().print_help()?;
            println!();
        }
    }

    Ok(())
}

fn read_readiness_report(path: &Path) -> anyhow::Result<ProductionReadinessReportV1> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read readiness evidence from {}", path.display()))?;
    let evidence = ProductionReadinessEvidenceV1::from_json_str(&contents)
        .with_context(|| format!("failed to parse readiness evidence {}", path.display()))?;
    evidence.try_into_report().map_err(anyhow::Error::msg)
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

fn validate_dependency_governance_file(path: &Path) -> anyhow::Result<()> {
    let contents = fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read dependency governance manifest from {}",
            path.display()
        )
    })?;
    validate_dependency_governance_manifest_text(&contents, &today_utc_string()?)
        .with_context(|| format!("invalid dependency governance manifest {}", path.display()))
}

fn validate_dependency_governance_manifest_text(contents: &str, today: &str) -> anyhow::Result<()> {
    let manifest: DependencyGovernanceManifestV1 =
        serde_json::from_str(contents).context("failed to parse dependency governance JSON")?;
    manifest.validate(today)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DependencyGovernanceManifestV1 {
    schema_version: u16,
    msrv: DependencyGovernanceMsrvV1,
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
struct DependencyGovernanceExceptionV1 {
    kind: DependencyGovernanceExceptionKind,
    #[serde(rename = "crate")]
    crate_name: String,
    owner: Option<String>,
    expires_on: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DependencyGovernanceExceptionKind {
    Duplicate,
    Unmaintained,
    Advisory,
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
        validate_date(today).context("validator today must use YYYY-MM-DD")?;

        for exception in &self.exceptions {
            exception.validate(today)?;
        }

        Ok(())
    }
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

fn format_checkpoint_inspection(inspection: &CheckpointAdminInspection) -> String {
    let latest = inspection
        .latest_valid_checkpoint
        .map_or_else(|| "none".to_string(), |checkpoint| checkpoint.to_string());
    let mut output = format!("latest_valid_checkpoint={latest}\nmanifests:\n");

    for manifest in &inspection.manifests {
        output.push_str(&format!(
            "checkpoint={} key={} lifecycle={} status={}\n",
            manifest.checkpoint_version,
            manifest.manifest_key,
            format_lifecycle_status(manifest.lifecycle_status),
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
        schema_version: 1,
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

fn format_lifecycle_status(status: Option<CheckpointLifecycleStatus>) -> &'static str {
    match status {
        Some(CheckpointLifecycleStatus::Published) => "published",
        None => "none",
    }
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
) -> anyhow::Result<()> {
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
            .require_workloads(BENCHMARK_GATE_WORKLOADS)
            .with_context(|| {
                format!(
                    "benchmark result {} is missing required workload metrics",
                    result.display()
                )
            })?;
    }

    let Some(baseline) = baseline else {
        return Ok(());
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
            .require_workloads(BENCHMARK_GATE_WORKLOADS)
            .with_context(|| {
                format!(
                    "benchmark baseline {} is missing required workload metrics",
                    baseline.display()
                )
            })?;
        if gate_level == BenchmarkGateLevel::Release && is_placeholder_baseline(&baseline_result) {
            bail!(
                "release benchmark gate requires a real S3-compatible baseline, got placeholder {}",
                baseline.display()
            );
        }
    }
    let max_regression_fraction =
        max_regression_fraction.context("benchmark gate requires --max-regression-fraction")?;

    current_result
        .compare_against(
            &baseline_result,
            BenchmarkBudgetV1::relative(max_regression_fraction),
        )
        .context("benchmark result exceeds gate")
}

fn is_placeholder_baseline(result: &BenchmarkGateResultV1) -> bool {
    result.commit.starts_with("placeholder-")
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
    use std::fs;

    use super::*;
    use tempfile::tempdir;
    use velorix_storage::{
        checkpoint_index::{
            CheckpointAdminInspection, CheckpointLifecycleStatus, CheckpointManifestInspection,
            CheckpointManifestInspectionStatus,
        },
        gc::{GarbageCollectionCandidate, GarbageCollectionCandidateKind},
        object_key::ObjectKey,
    };

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
        ])
        .unwrap();

        let Some(Command::BenchmarkGate {
            gate_level,
            backend,
            ..
        }) = cli.command
        else {
            panic!("expected benchmark-gate command");
        };

        assert_eq!(gate_level, BenchmarkGateLevel::PrSmoke);
        assert_eq!(backend, BenchmarkBackend::Local);
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

        let Some(Command::ReadinessReport { evidence, json }) = cli.command else {
            panic!("expected readiness-report command");
        };

        assert_eq!(evidence, PathBuf::from("readiness.json"));
        assert!(json);
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
    fn readiness_report_formatter_is_not_json() {
        let report = ProductionReadinessEvidenceV1::from_json_str(&readiness_json()).unwrap();
        let summary = format_readiness_report(&report.try_into_report().unwrap());

        assert!(summary.starts_with("production_ready=true\n"));
        assert!(!summary.trim_start().starts_with('{'));
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
    fn dependency_governance_validator_rejects_missing_owner() {
        let manifest = serde_json::json!({
            "schema_version": 1,
            "msrv": {
                "minimum_rust_version": "1.85.0",
                "policy": "CI and package updates keep the workspace buildable on the declared MSRV."
            },
            "exceptions": [
                {
                    "kind": "duplicate",
                    "crate": "hashbrown",
                    "expires_on": "2026-06-30",
                    "reason": "Await upstream convergence."
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
            "exceptions": [
                {
                    "kind": "unmaintained",
                    "crate": "paste",
                    "owner": "runtime",
                    "expires_on": "2026-05-05",
                    "reason": "Temporary allowance while replacing the transitive dependency."
                }
            ]
        })
        .to_string();

        let error =
            validate_dependency_governance_manifest_text(&manifest, "2026-05-06").unwrap_err();

        assert!(format!("{error:#}").contains("expired"));
    }

    #[test]
    fn dependency_governance_validator_rejects_invalid_expiry_date() {
        let manifest = serde_json::json!({
            "schema_version": 1,
            "msrv": {
                "minimum_rust_version": "1.85.0",
                "policy": "CI and package updates keep the workspace buildable on the declared MSRV."
            },
            "exceptions": [
                {
                    "kind": "advisory",
                    "crate": "example",
                    "owner": "security",
                    "expires_on": "2026-02-30",
                    "reason": "Temporary advisory exception."
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
            "exceptions": [
                {
                    "kind": "advisory",
                    "crate": "example",
                    "owner": "security",
                    "expires_on": "2028-02-29",
                    "reason": "Temporary advisory exception."
                }
            ]
        })
        .to_string();

        validate_dependency_governance_manifest_text(&manifest, "2028-02-01").unwrap();
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

        assert!(format!("{error:#}").contains("requires a real S3-compatible baseline"));
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

        assert!(format!("{error:#}").contains("gc_dry_run_planning"));
    }

    #[test]
    fn checkpoint_inspection_formatter_prints_stable_operator_summary() {
        let summary = checkpoint_inspection_summary();

        assert_eq!(
            format_checkpoint_inspection(&summary),
            concat!(
                "latest_valid_checkpoint=7\n",
                "manifests:\n",
                "checkpoint=3 key=v1/checkpoints/00000000000000000003.manifest lifecycle=published status=valid\n",
                "checkpoint=8 key=v1/checkpoints/00000000000000000008.manifest lifecycle=none status=invalid reason=missing visible parent checkpoint 7\n",
            )
        );
    }

    #[test]
    fn checkpoint_inspection_json_uses_stable_schema_version() {
        let json = format_checkpoint_inspection_json(&checkpoint_inspection_summary()).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["latest_valid_checkpoint"], 7);
        assert_eq!(value["manifests"][0]["status"], "valid");
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
                    status: CheckpointManifestInspectionStatus::Valid,
                },
                CheckpointManifestInspection {
                    checkpoint_version: 8,
                    manifest_key: ObjectKey::checkpoint_manifest(8),
                    lifecycle_status: None,
                    status: CheckpointManifestInspectionStatus::Invalid {
                        reason: "missing visible parent checkpoint\n7".to_string(),
                    },
                },
            ],
        }
    }

    fn readiness_json() -> String {
        serde_json::json!({
            "schema_version": 1,
            "deployment_id": "prod-a",
            "authority_store_id": "s3://velorix-prod",
            "capability_status": {
                "status": "pass",
                "evidence": "s3-compatible capability probe",
                "evidence_kind": ["s3_compatible"]
            },
            "ownership_status": { "status": "pass", "evidence": "durable epoch record" },
            "checkpoint_status": { "status": "pass", "evidence": "published checkpoint lifecycle" },
            "state_status": { "status": "pass", "evidence": "SlateDB checkpoint ref" },
            "query_policy_status": { "status": "pass", "evidence": "bounded DataFusion policy" },
            "table_catalog_status": { "status": "pass", "evidence": "registry-backed table catalog" },
            "feldera_artifact_status": { "status": "pass", "evidence": "trusted artifact metadata" },
            "benchmark_gate_status": { "status": "pass", "evidence": "S3-compatible benchmark gate" },
            "kubernetes_status": {
                "status": "pass",
                "evidence": "Kubernetes Lease client",
                "evidence_kind": ["kubernetes_lease_client"]
            }
        })
        .to_string()
    }

    fn valid_dependency_governance_json() -> String {
        serde_json::json!({
            "schema_version": 1,
            "msrv": {
                "minimum_rust_version": "1.85.0",
                "policy": "CI and package updates keep the workspace buildable on the declared MSRV."
            },
            "exceptions": [
                {
                    "kind": "duplicate",
                    "crate": "hashbrown",
                    "owner": "runtime",
                    "expires_on": "2026-06-30",
                    "reason": "Await upstream convergence in the DataFusion dependency graph."
                },
                {
                    "kind": "unmaintained",
                    "crate": "paste",
                    "owner": "runtime",
                    "expires_on": "2026-06-30",
                    "reason": "Temporary allowance while replacing the transitive dependency."
                },
                {
                    "kind": "advisory",
                    "crate": "example",
                    "owner": "security",
                    "expires_on": "2026-06-30",
                    "reason": "Advisory does not affect the enabled feature set."
                }
            ]
        })
        .to_string()
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
            "local_incremental",
            1000.0,
            workload_metrics(),
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
            workload_metrics(),
        ))
        .unwrap()
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

    fn normal_result(
        commit: &str,
        gate_level: &str,
        backend: &str,
        workload: &str,
        rows_per_second: f64,
        workload_metrics: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
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
        })
    }

    fn workload_metrics() -> serde_json::Value {
        serde_json::json!([
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
        ])
    }
}
