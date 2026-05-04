use velorix_runtime::benchmark_gate::{
    BenchmarkBackend, BenchmarkBudgetV1, BenchmarkGateError, BenchmarkGateLevel,
    BenchmarkGateResultV1, BenchmarkMetricsV1, ObjectRequestMetricsV1,
};

#[test]
fn local_smoke_result_validates_when_required_metrics_are_present() {
    let result: BenchmarkGateResultV1 = serde_json::from_str(VALID_LOCAL_SMOKE_JSON).unwrap();

    result.validate().unwrap();
}

#[test]
fn local_smoke_result_validation_fails_when_object_request_metrics_are_missing() {
    let error = BenchmarkGateResultV1::from_json_str(
        r#"{
            "schema_version": 1,
            "commit": "abc123",
            "gate_level": "pr_smoke",
            "backend": "local",
            "workload": "local_incremental",
            "metrics": {
                "rows_per_second": 1000.0,
                "bytes_per_row": 128.0,
                "put_per_gib": 8.0,
                "checkpoint_p50_ms": 3.0,
                "checkpoint_p95_ms": 4.0,
                "recovery_p95_ms": 7.0,
                "peak_rss_bytes": 0,
                "spill_bytes": 0,
                "scan_bytes": 0
            }
        }"#,
    )
    .unwrap_err();

    assert!(matches!(error, BenchmarkGateError::Json(_)));
}

#[test]
fn benchmark_comparison_rejects_local_smoke_against_s3_baseline() {
    let current = local_smoke_result();
    let mut baseline = local_smoke_result();
    baseline.backend = BenchmarkBackend::S3Compatible;
    baseline.gate_level = BenchmarkGateLevel::NightlyIntegration;

    let error = current
        .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
        .unwrap_err();

    assert!(matches!(error, BenchmarkGateError::BaselineMismatch { .. }));
}

#[test]
fn benchmark_comparison_fails_when_synthetic_regression_exceeds_budget() {
    let mut current = local_smoke_result();
    let baseline = local_smoke_result();
    current.metrics.rows_per_second = baseline.metrics.rows_per_second * 0.88;

    let error = current
        .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
        .unwrap_err();

    assert!(matches!(error, BenchmarkGateError::Regression { .. }));
}

#[test]
fn benchmark_comparison_passes_when_synthetic_regression_is_within_budget() {
    let mut current = local_smoke_result();
    let baseline = local_smoke_result();
    current.metrics.rows_per_second = baseline.metrics.rows_per_second * 0.91;

    current
        .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
        .unwrap();
}

fn local_smoke_result() -> BenchmarkGateResultV1 {
    BenchmarkGateResultV1 {
        schema_version: 1,
        commit: "abc123".to_string(),
        gate_level: BenchmarkGateLevel::PrSmoke,
        backend: BenchmarkBackend::Local,
        workload: "local_incremental".to_string(),
        metrics: BenchmarkMetricsV1 {
            rows_per_second: 1000.0,
            bytes_per_row: 128.0,
            put_per_gib: 8.0,
            object_requests: ObjectRequestMetricsV1 {
                put_count: 8,
                get_count: 3,
                list_count: 2,
                range_read_count: 0,
                bytes_written: 1024,
                bytes_read: 512,
            },
            checkpoint_p50_ms: 3.0,
            checkpoint_p95_ms: 4.0,
            recovery_p95_ms: 7.0,
            peak_rss_bytes: 0,
            spill_bytes: 0,
            scan_bytes: 0,
        },
    }
}

const VALID_LOCAL_SMOKE_JSON: &str = r#"{
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
