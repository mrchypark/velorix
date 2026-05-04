//! Machine-readable benchmark gate results and regression checks.

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkGateResultV1 {
    pub schema_version: u8,
    pub commit: String,
    pub gate_level: BenchmarkGateLevel,
    pub backend: BenchmarkBackend,
    pub workload: String,
    pub metrics: BenchmarkMetricsV1,
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
    #[error("benchmark metric {metric} must be finite and non-negative, got {value}")]
    InvalidMetric { metric: &'static str, value: f64 },
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
        "benchmark metric {metric} regressed by {regression_fraction:.3}, over budget {budget_fraction:.3}"
    )]
    Regression {
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
        self.metrics.validate()
    }

    pub fn compare_against(
        &self,
        baseline: &Self,
        budget: BenchmarkBudgetV1,
    ) -> Result<(), BenchmarkGateError> {
        self.validate()?;
        baseline.validate()?;
        budget.validate()?;

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
        )
    }
}

impl BenchmarkMetricsV1 {
    fn validate(&self) -> Result<(), BenchmarkGateError> {
        validate_finite_non_negative("rows_per_second", self.rows_per_second)?;
        validate_finite_non_negative("bytes_per_row", self.bytes_per_row)?;
        validate_finite_non_negative("put_per_gib", self.put_per_gib)?;
        validate_finite_non_negative("checkpoint_p50_ms", self.checkpoint_p50_ms)?;
        validate_finite_non_negative("checkpoint_p95_ms", self.checkpoint_p95_ms)?;
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
