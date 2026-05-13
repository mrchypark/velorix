//! Machine-readable benchmark gate results and regression checks.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

const V1_SCHEMA_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkGateLevel {
    PrSmoke,
    NightlyIntegration,
    Release,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkBackend {
    Local,
    S3Compatible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkEvidenceScope {
    LiveOrNative,
    LocalEmulator,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkGateResultV1 {
    pub schema_version: u8,
    pub commit: String,
    pub gate_level: BenchmarkGateLevel,
    pub backend: BenchmarkBackend,
    #[serde(default = "default_benchmark_evidence_scope")]
    pub backend_evidence_scope: BenchmarkEvidenceScope,
    pub workload: String,
    pub metrics: BenchmarkMetricsV1,
    pub workload_metrics: Vec<BenchmarkWorkloadMetricsV1>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkMetricsV1 {
    pub rows_per_second: f64,
    pub bytes_per_row: f64,
    pub put_per_gib: f64,
    pub object_requests: ObjectRequestMetricsV1,
    pub checkpoint_p50_ms: f64,
    pub checkpoint_p95_ms: f64,
    pub recovery_p95_ms: f64,
    pub peak_rss_bytes: u64,
    pub spill_bytes: u64,
    pub scan_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkWorkloadMetricsV1 {
    pub name: String,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub object_requests: Option<ObjectRequestMetricsV1>,
    pub scan_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectRequestMetricsV1 {
    pub put_count: u64,
    pub get_count: u64,
    pub list_count: u64,
    pub range_read_count: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BenchmarkBudgetV1 {
    max_regression_fraction: f64,
}

impl BenchmarkBudgetV1 {
    pub fn relative(max_regression_fraction: f64) -> Self {
        Self {
            max_regression_fraction,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BenchmarkGateError {
    #[error("benchmark result is not valid JSON V1: {0}")]
    Json(#[from] serde_json::Error),
    #[error("benchmark result schema_version must be 1, got {actual}")]
    UnsupportedSchemaVersion { actual: u8 },
    #[error("benchmark result field {field} must be present")]
    MissingRequiredField { field: &'static str },
    #[error(
        "benchmark backend {backend:?} requires workload {expected_workload}, got {actual_workload}"
    )]
    BackendWorkloadMismatch {
        backend: BenchmarkBackend,
        expected_workload: &'static str,
        actual_workload: String,
    },
    #[error("benchmark S3-compatible result commit must not be a placeholder, got {commit}")]
    PlaceholderS3Commit { commit: String },
    #[error("benchmark S3-compatible result uses local emulator evidence")]
    LocalEmulatorS3Evidence,
    #[error("benchmark metric {metric} must be finite and non-negative, got {value}")]
    InvalidMetric { metric: &'static str, value: f64 },
    #[error("benchmark workload_metrics must be non-empty")]
    MissingWorkloadMetrics,
    #[error("benchmark workload_metrics contains duplicate workload metric {name}")]
    DuplicateWorkloadMetric { name: String },
    #[error(
        "benchmark workload metric {name} has invalid latency order: p95_ms {p95_ms} is below p50_ms {p50_ms}"
    )]
    InvalidWorkloadLatencyOrder {
        name: String,
        p50_ms: f64,
        p95_ms: f64,
    },
    #[error(
        "benchmark checkpoint latency has invalid order: checkpoint_p95_ms {p95_ms} is below checkpoint_p50_ms {p50_ms}"
    )]
    InvalidCheckpointLatencyOrder { p50_ms: f64, p95_ms: f64 },
    #[error("benchmark workload metric {name} requires object_requests")]
    MissingWorkloadObjectRequests { name: String },
    #[error("benchmark result is missing required workload metric {name}")]
    MissingRequiredWorkload { name: String },
    #[error("benchmark baseline is missing workload metric {name}")]
    MissingBaselineWorkload { name: String },
    #[error("benchmark result is missing baseline workload metric {name}")]
    MissingCurrentWorkload { name: String },
    #[error("benchmark budget must be finite and non-negative, got {value}")]
    InvalidBudget { value: f64 },
    #[error(
        "benchmark baseline mismatch: current {current_gate:?}/{current_backend:?}/{current_workload}, baseline {baseline_gate:?}/{baseline_backend:?}/{baseline_workload}"
    )]
    BaselineMismatch {
        current_gate: BenchmarkGateLevel,
        current_backend: BenchmarkBackend,
        current_workload: String,
        baseline_gate: BenchmarkGateLevel,
        baseline_backend: BenchmarkBackend,
        baseline_workload: String,
    },
    #[error(
        "benchmark expectation mismatch: expected {expected_gate:?}/{expected_backend:?}, got {actual_gate:?}/{actual_backend:?}"
    )]
    ExpectationMismatch {
        expected_gate: BenchmarkGateLevel,
        expected_backend: BenchmarkBackend,
        actual_gate: BenchmarkGateLevel,
        actual_backend: BenchmarkBackend,
    },
    #[error(
        "benchmark metric {metric} regressed by {regression_fraction:.3}, over budget {budget_fraction:.3}"
    )]
    Regression {
        metric: &'static str,
        regression_fraction: f64,
        budget_fraction: f64,
    },
    #[error(
        "benchmark workload {workload} metric {metric} regressed by {regression_fraction:.3}, over budget {budget_fraction:.3}"
    )]
    WorkloadRegression {
        workload: String,
        metric: &'static str,
        regression_fraction: f64,
        budget_fraction: f64,
    },
}

impl BenchmarkGateResultV1 {
    pub fn from_json_str(json: &str) -> Result<Self, BenchmarkGateError> {
        let result = serde_json::from_str::<Self>(json)?;
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<(), BenchmarkGateError> {
        if self.schema_version != V1_SCHEMA_VERSION {
            return Err(BenchmarkGateError::UnsupportedSchemaVersion {
                actual: self.schema_version,
            });
        }
        if self.commit.trim().is_empty() {
            return Err(BenchmarkGateError::MissingRequiredField { field: "commit" });
        }
        if self.workload.trim().is_empty() {
            return Err(BenchmarkGateError::MissingRequiredField { field: "workload" });
        }
        let expected_workload = expected_workload_for_backend(self.backend);
        if self.workload != expected_workload {
            return Err(BenchmarkGateError::BackendWorkloadMismatch {
                backend: self.backend,
                expected_workload,
                actual_workload: self.workload.clone(),
            });
        }
        self.metrics.validate()?;
        validate_workload_metrics(&self.workload_metrics)
    }

    pub fn expect_gate(
        &self,
        expected_gate: BenchmarkGateLevel,
        expected_backend: BenchmarkBackend,
    ) -> Result<(), BenchmarkGateError> {
        if self.gate_level == expected_gate && self.backend == expected_backend {
            Ok(())
        } else {
            Err(BenchmarkGateError::ExpectationMismatch {
                expected_gate,
                expected_backend,
                actual_gate: self.gate_level,
                actual_backend: self.backend,
            })
        }
    }

    pub fn compare_against(
        &self,
        baseline: &Self,
        budget: BenchmarkBudgetV1,
    ) -> Result<(), BenchmarkGateError> {
        self.validate()?;
        baseline.validate()?;
        budget.validate()?;
        self.reject_local_emulator_s3_evidence()?;
        baseline.reject_local_emulator_s3_evidence()?;

        if self.gate_level != baseline.gate_level
            || self.backend != baseline.backend
            || self.workload != baseline.workload
        {
            return Err(BenchmarkGateError::BaselineMismatch {
                current_gate: self.gate_level,
                current_backend: self.backend,
                current_workload: self.workload.clone(),
                baseline_gate: baseline.gate_level,
                baseline_backend: baseline.backend,
                baseline_workload: baseline.workload.clone(),
            });
        }

        compare_higher_is_better(
            "rows_per_second",
            self.metrics.rows_per_second,
            baseline.metrics.rows_per_second,
            budget.max_regression_fraction,
        )?;
        compare_lower_is_better(
            "bytes_per_row",
            self.metrics.bytes_per_row,
            baseline.metrics.bytes_per_row,
            budget.max_regression_fraction,
        )?;
        compare_lower_is_better(
            "put_per_gib",
            self.metrics.put_per_gib,
            baseline.metrics.put_per_gib,
            budget.max_regression_fraction,
        )?;
        compare_lower_is_better(
            "put_count",
            self.metrics.object_requests.put_count as f64,
            baseline.metrics.object_requests.put_count as f64,
            budget.max_regression_fraction,
        )?;
        compare_lower_is_better(
            "get_count",
            self.metrics.object_requests.get_count as f64,
            baseline.metrics.object_requests.get_count as f64,
            budget.max_regression_fraction,
        )?;
        compare_lower_is_better(
            "list_count",
            self.metrics.object_requests.list_count as f64,
            baseline.metrics.object_requests.list_count as f64,
            budget.max_regression_fraction,
        )?;
        compare_lower_is_better(
            "range_read_count",
            self.metrics.object_requests.range_read_count as f64,
            baseline.metrics.object_requests.range_read_count as f64,
            budget.max_regression_fraction,
        )?;
        compare_lower_is_better(
            "checkpoint_p50_ms",
            self.metrics.checkpoint_p50_ms,
            baseline.metrics.checkpoint_p50_ms,
            budget.max_regression_fraction,
        )?;
        compare_lower_is_better(
            "checkpoint_p95_ms",
            self.metrics.checkpoint_p95_ms,
            baseline.metrics.checkpoint_p95_ms,
            budget.max_regression_fraction,
        )?;
        compare_lower_is_better(
            "recovery_p95_ms",
            self.metrics.recovery_p95_ms,
            baseline.metrics.recovery_p95_ms,
            budget.max_regression_fraction,
        )?;
        compare_lower_is_better(
            "peak_rss_bytes",
            self.metrics.peak_rss_bytes as f64,
            baseline.metrics.peak_rss_bytes as f64,
            budget.max_regression_fraction,
        )?;
        compare_lower_is_better(
            "spill_bytes",
            self.metrics.spill_bytes as f64,
            baseline.metrics.spill_bytes as f64,
            budget.max_regression_fraction,
        )?;
        compare_lower_is_better(
            "scan_bytes",
            self.metrics.scan_bytes as f64,
            baseline.metrics.scan_bytes as f64,
            budget.max_regression_fraction,
        )?;
        compare_workload_metrics(
            &self.workload_metrics,
            &baseline.workload_metrics,
            budget.max_regression_fraction,
        )
    }

    pub fn require_workloads(&self, required: &[&str]) -> Result<(), BenchmarkGateError> {
        let present = self
            .workload_metrics
            .iter()
            .map(|workload| workload.name.as_str())
            .collect::<HashSet<_>>();

        for name in required {
            if !present.contains(name) {
                return Err(BenchmarkGateError::MissingRequiredWorkload {
                    name: (*name).to_string(),
                });
            }
        }

        Ok(())
    }

    pub fn reject_placeholder_s3_commit(&self) -> Result<(), BenchmarkGateError> {
        let commit = self.commit.trim().to_ascii_lowercase();
        let is_placeholder = commit.contains("placeholder")
            || commit == "unknown"
            || commit == "local"
            || (!commit.is_empty() && commit.chars().all(|c| c == '0'));

        if self.backend == BenchmarkBackend::S3Compatible && is_placeholder {
            return Err(BenchmarkGateError::PlaceholderS3Commit {
                commit: self.commit.clone(),
            });
        }

        Ok(())
    }

    pub fn reject_local_emulator_s3_evidence(&self) -> Result<(), BenchmarkGateError> {
        if self.backend == BenchmarkBackend::S3Compatible
            && self.backend_evidence_scope == BenchmarkEvidenceScope::LocalEmulator
        {
            return Err(BenchmarkGateError::LocalEmulatorS3Evidence);
        }

        Ok(())
    }
}

impl BenchmarkMetricsV1 {
    fn validate(&self) -> Result<(), BenchmarkGateError> {
        validate_finite_non_negative("rows_per_second", self.rows_per_second)?;
        validate_finite_non_negative("bytes_per_row", self.bytes_per_row)?;
        validate_finite_non_negative("put_per_gib", self.put_per_gib)?;
        validate_finite_non_negative("checkpoint_p50_ms", self.checkpoint_p50_ms)?;
        validate_finite_non_negative("checkpoint_p95_ms", self.checkpoint_p95_ms)?;
        if self.checkpoint_p95_ms < self.checkpoint_p50_ms {
            return Err(BenchmarkGateError::InvalidCheckpointLatencyOrder {
                p50_ms: self.checkpoint_p50_ms,
                p95_ms: self.checkpoint_p95_ms,
            });
        }
        validate_finite_non_negative("recovery_p95_ms", self.recovery_p95_ms)?;
        Ok(())
    }
}

impl BenchmarkBudgetV1 {
    fn validate(&self) -> Result<(), BenchmarkGateError> {
        if self.max_regression_fraction.is_finite() && self.max_regression_fraction >= 0.0 {
            Ok(())
        } else {
            Err(BenchmarkGateError::InvalidBudget {
                value: self.max_regression_fraction,
            })
        }
    }
}

fn compare_workload_metrics(
    current: &[BenchmarkWorkloadMetricsV1],
    baseline: &[BenchmarkWorkloadMetricsV1],
    budget_fraction: f64,
) -> Result<(), BenchmarkGateError> {
    for baseline_workload in baseline {
        if !current
            .iter()
            .any(|workload| workload.name == baseline_workload.name)
        {
            return Err(BenchmarkGateError::MissingCurrentWorkload {
                name: baseline_workload.name.clone(),
            });
        }
    }

    for current_workload in current {
        let Some(baseline_workload) = baseline
            .iter()
            .find(|workload| workload.name == current_workload.name)
        else {
            return Err(BenchmarkGateError::MissingBaselineWorkload {
                name: current_workload.name.clone(),
            });
        };

        compare_lower_workload_metric(
            &current_workload.name,
            "p50_ms",
            current_workload.p50_ms,
            baseline_workload.p50_ms,
            budget_fraction,
        )?;
        compare_lower_workload_metric(
            &current_workload.name,
            "p95_ms",
            current_workload.p95_ms,
            baseline_workload.p95_ms,
            budget_fraction,
        )?;
        compare_lower_workload_metric(
            &current_workload.name,
            "scan_bytes",
            current_workload.scan_bytes as f64,
            baseline_workload.scan_bytes as f64,
            budget_fraction,
        )?;

        if let (Some(current_requests), Some(baseline_requests)) = (
            &current_workload.object_requests,
            &baseline_workload.object_requests,
        ) {
            compare_workload_object_requests(
                &current_workload.name,
                current_requests,
                baseline_requests,
                budget_fraction,
            )?;
        }
    }

    Ok(())
}

fn compare_workload_object_requests(
    workload: &str,
    current: &ObjectRequestMetricsV1,
    baseline: &ObjectRequestMetricsV1,
    budget_fraction: f64,
) -> Result<(), BenchmarkGateError> {
    compare_lower_workload_metric(
        workload,
        "put_count",
        current.put_count as f64,
        baseline.put_count as f64,
        budget_fraction,
    )?;
    compare_lower_workload_metric(
        workload,
        "get_count",
        current.get_count as f64,
        baseline.get_count as f64,
        budget_fraction,
    )?;
    compare_lower_workload_metric(
        workload,
        "list_count",
        current.list_count as f64,
        baseline.list_count as f64,
        budget_fraction,
    )?;
    compare_lower_workload_metric(
        workload,
        "range_read_count",
        current.range_read_count as f64,
        baseline.range_read_count as f64,
        budget_fraction,
    )?;
    compare_lower_workload_metric(
        workload,
        "bytes_written",
        current.bytes_written as f64,
        baseline.bytes_written as f64,
        budget_fraction,
    )?;
    compare_lower_workload_metric(
        workload,
        "bytes_read",
        current.bytes_read as f64,
        baseline.bytes_read as f64,
        budget_fraction,
    )
}

fn validate_finite_non_negative(
    metric: &'static str,
    value: f64,
) -> Result<(), BenchmarkGateError> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err(BenchmarkGateError::InvalidMetric { metric, value })
    }
}

fn validate_workload_metrics(
    workloads: &[BenchmarkWorkloadMetricsV1],
) -> Result<(), BenchmarkGateError> {
    if workloads.is_empty() {
        return Err(BenchmarkGateError::MissingWorkloadMetrics);
    }

    let mut names = HashSet::new();
    for workload in workloads {
        let name = workload.name.trim();
        if name.is_empty() {
            return Err(BenchmarkGateError::MissingRequiredField {
                field: "workload_metrics.name",
            });
        }
        if !names.insert(name) {
            return Err(BenchmarkGateError::DuplicateWorkloadMetric {
                name: name.to_string(),
            });
        }

        validate_finite_non_negative("workload_metrics.p50_ms", workload.p50_ms)?;
        validate_finite_non_negative("workload_metrics.p95_ms", workload.p95_ms)?;
        if workload.p95_ms < workload.p50_ms {
            return Err(BenchmarkGateError::InvalidWorkloadLatencyOrder {
                name: name.to_string(),
                p50_ms: workload.p50_ms,
                p95_ms: workload.p95_ms,
            });
        }
        if workload.object_requests.is_none() && is_object_backed_workload(name) {
            return Err(BenchmarkGateError::MissingWorkloadObjectRequests {
                name: name.to_string(),
            });
        }
    }

    Ok(())
}

fn expected_workload_for_backend(backend: BenchmarkBackend) -> &'static str {
    match backend {
        BenchmarkBackend::Local => "local_incremental",
        BenchmarkBackend::S3Compatible => "s3_incremental",
    }
}

fn default_benchmark_evidence_scope() -> BenchmarkEvidenceScope {
    BenchmarkEvidenceScope::LiveOrNative
}

fn is_object_backed_workload(name: &str) -> bool {
    matches!(
        name,
        "object_store_capability_probe"
            | "ingest_envelope_validation"
            | "checkpoint_publish"
            | "checkpoint_recovery"
            | "datafusion_table_scan"
            | "slatedb_state_reopen"
            | "gc_dry_run_planning"
            | "gc_execution_evidence"
    )
}

fn compare_higher_is_better(
    metric: &'static str,
    current: f64,
    baseline: f64,
    budget_fraction: f64,
) -> Result<(), BenchmarkGateError> {
    if baseline == 0.0 {
        return Ok(());
    }

    let regression_fraction = (baseline - current) / baseline;
    fail_if_over_budget(metric, regression_fraction, budget_fraction)
}

fn compare_lower_is_better(
    metric: &'static str,
    current: f64,
    baseline: f64,
    budget_fraction: f64,
) -> Result<(), BenchmarkGateError> {
    let regression_fraction = if baseline == 0.0 {
        if current > 0.0 {
            f64::INFINITY
        } else {
            0.0
        }
    } else {
        (current - baseline) / baseline
    };
    fail_if_over_budget(metric, regression_fraction, budget_fraction)
}

fn compare_lower_workload_metric(
    workload: &str,
    metric: &'static str,
    current: f64,
    baseline: f64,
    budget_fraction: f64,
) -> Result<(), BenchmarkGateError> {
    let regression_fraction = if baseline == 0.0 {
        if current > 0.0 {
            f64::INFINITY
        } else {
            0.0
        }
    } else {
        (current - baseline) / baseline
    };

    if regression_fraction > budget_fraction {
        Err(BenchmarkGateError::WorkloadRegression {
            workload: workload.to_string(),
            metric,
            regression_fraction,
            budget_fraction,
        })
    } else {
        Ok(())
    }
}

fn fail_if_over_budget(
    metric: &'static str,
    regression_fraction: f64,
    budget_fraction: f64,
) -> Result<(), BenchmarkGateError> {
    if regression_fraction > budget_fraction {
        Err(BenchmarkGateError::Regression {
            metric,
            regression_fraction,
            budget_fraction,
        })
    } else {
        Ok(())
    }
}
