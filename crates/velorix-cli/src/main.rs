#![forbid(unsafe_code)]

use std::{fs, path::Path, path::PathBuf, sync::Arc};

use anyhow::Context;
use clap::{CommandFactory, Parser, Subcommand};
use object_store::{local::LocalFileSystem, ObjectStore};
use velorix_runtime::benchmark_gate::{BenchmarkBudgetV1, BenchmarkGateResultV1};
use velorix_runtime::recovery::RecoveredRuntime;
use velorix_storage::{
    checkpoint_index::{
        CheckpointAdminInspection, CheckpointLifecycleStatus, CheckpointManifestInspectionStatus,
    },
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
    },
    BenchmarkValidate {
        #[arg(long)]
        result: PathBuf,
    },
    BenchmarkGate {
        #[arg(long)]
        baseline: PathBuf,
        #[arg(long)]
        result: PathBuf,
        #[arg(long, default_value_t = 0.10)]
        max_regression_fraction: f64,
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
        Some(Command::CheckpointInspectLocal { object_store_dir }) => {
            let store = local_object_store(&object_store_dir)?;
            let inspection = CheckpointPublisher::new(store)
                .inspect_checkpoints()
                .await
                .context("failed to inspect local checkpoints")?;

            print!("{}", format_checkpoint_inspection(&inspection));
        }
        Some(Command::BenchmarkValidate { result }) => {
            run_benchmark_gate(None, &result, None)?;
            println!("benchmark result valid");
        }
        Some(Command::BenchmarkGate {
            baseline,
            result,
            max_regression_fraction,
        }) => {
            run_benchmark_gate(Some(&baseline), &result, Some(max_regression_fraction))?;
            println!("benchmark gate passed");
        }
        None => {
            Cli::command().print_help()?;
            println!();
        }
    }

    Ok(())
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
    max_regression_fraction: Option<f64>,
) -> anyhow::Result<()> {
    let current_result = read_benchmark_result(result)
        .with_context(|| format!("failed to validate benchmark result {}", result.display()))?;

    let Some(baseline) = baseline else {
        return Ok(());
    };

    let baseline_result = read_benchmark_result(baseline).with_context(|| {
        format!(
            "failed to validate benchmark baseline {}",
            baseline.display()
        )
    })?;
    let max_regression_fraction =
        max_regression_fraction.context("benchmark gate requires --max-regression-fraction")?;

    current_result
        .compare_against(
            &baseline_result,
            BenchmarkBudgetV1::relative(max_regression_fraction),
        )
        .context("benchmark result exceeds gate")
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
        object_key::ObjectKey,
    };

    #[test]
    fn benchmark_result_parser_accepts_pretty_json() {
        parse_benchmark_result_text(VALID_RESULT_JSON).unwrap();
    }

    #[test]
    fn benchmark_result_parser_accepts_jsonl_last_line() {
        parse_benchmark_result_text(&format!("ignored status line\n{VALID_RESULT_JSONL}\n"))
            .unwrap();
    }

    #[test]
    fn benchmark_gate_comparison_rejects_mismatched_backend() {
        let baseline = parse_benchmark_result_text(VALID_RESULT_JSON).unwrap();
        let current = parse_benchmark_result_text(S3_RESULT_JSON).unwrap();

        let error = current
            .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
            .unwrap_err();

        assert!(error.to_string().contains("baseline mismatch"));
    }

    #[test]
    fn benchmark_gate_validate_only_accepts_valid_result() {
        let dir = tempdir().unwrap();
        let result = dir.path().join("result.json");
        fs::write(&result, VALID_RESULT_JSON).unwrap();

        run_benchmark_gate(None, &result, None).unwrap();
    }

    #[test]
    fn benchmark_gate_comparison_accepts_result_within_budget() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, VALID_RESULT_JSON).unwrap();
        fs::write(&result, VALID_RESULT_JSON).unwrap();

        run_benchmark_gate(Some(&baseline), &result, Some(0.10)).unwrap();
    }

    #[test]
    fn benchmark_gate_comparison_rejects_result_over_budget() {
        let dir = tempdir().unwrap();
        let baseline = dir.path().join("baseline.json");
        let result = dir.path().join("result.json");
        fs::write(&baseline, VALID_RESULT_JSON).unwrap();
        fs::write(&result, REGRESSED_RESULT_JSON).unwrap();

        let error = run_benchmark_gate(Some(&baseline), &result, Some(0.10)).unwrap_err();
        let error_chain = format!("{error:#}");

        assert!(error_chain.contains("regressed"));
    }

    #[test]
    fn checkpoint_inspection_formatter_prints_stable_operator_summary() {
        let summary = CheckpointAdminInspection {
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
        };

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

    const VALID_RESULT_JSON: &str = r#"{
        "schema_version": 1,
        "commit": "abc123",
        "gate_level": "pr_smoke",
        "backend": "local",
        "workload": "local_incremental",
        "metrics": {
            "rows_per_second": 1000.0,
            "bytes_per_row": 128.0,
            "put_per_gib": 8.0,
            "object_requests": {
                "put_count": 8,
                "get_count": 3,
                "list_count": 2,
                "range_read_count": 0,
                "bytes_written": 1024,
                "bytes_read": 512
            },
            "checkpoint_p50_ms": 3.0,
            "checkpoint_p95_ms": 4.0,
            "recovery_p95_ms": 7.0,
            "peak_rss_bytes": 0,
            "spill_bytes": 0,
            "scan_bytes": 0
        }
    }"#;

    const VALID_RESULT_JSONL: &str = r#"{"schema_version":1,"commit":"abc123","gate_level":"pr_smoke","backend":"local","workload":"local_incremental","metrics":{"rows_per_second":1000.0,"bytes_per_row":128.0,"put_per_gib":8.0,"object_requests":{"put_count":8,"get_count":3,"list_count":2,"range_read_count":0,"bytes_written":1024,"bytes_read":512},"checkpoint_p50_ms":3.0,"checkpoint_p95_ms":4.0,"recovery_p95_ms":7.0,"peak_rss_bytes":0,"spill_bytes":0,"scan_bytes":0}}"#;

    const S3_RESULT_JSON: &str = r#"{
        "schema_version": 1,
        "commit": "abc123",
        "gate_level": "pr_smoke",
        "backend": "s3_compatible",
        "workload": "local_incremental",
        "metrics": {
            "rows_per_second": 1000.0,
            "bytes_per_row": 128.0,
            "put_per_gib": 8.0,
            "object_requests": {
                "put_count": 8,
                "get_count": 3,
                "list_count": 2,
                "range_read_count": 0,
                "bytes_written": 1024,
                "bytes_read": 512
            },
            "checkpoint_p50_ms": 3.0,
            "checkpoint_p95_ms": 4.0,
            "recovery_p95_ms": 7.0,
            "peak_rss_bytes": 0,
            "spill_bytes": 0,
            "scan_bytes": 0
        }
    }"#;

    const REGRESSED_RESULT_JSON: &str = r#"{
        "schema_version": 1,
        "commit": "def456",
        "gate_level": "pr_smoke",
        "backend": "local",
        "workload": "local_incremental",
        "metrics": {
            "rows_per_second": 800.0,
            "bytes_per_row": 128.0,
            "put_per_gib": 8.0,
            "object_requests": {
                "put_count": 8,
                "get_count": 3,
                "list_count": 2,
                "range_read_count": 0,
                "bytes_written": 1024,
                "bytes_read": 512
            },
            "checkpoint_p50_ms": 3.0,
            "checkpoint_p95_ms": 4.0,
            "recovery_p95_ms": 7.0,
            "peak_rss_bytes": 0,
            "spill_bytes": 0,
            "scan_bytes": 0
        }
    }"#;
}
