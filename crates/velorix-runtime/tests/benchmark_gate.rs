use velorix_runtime::benchmark_gate::{
    BenchmarkBackend, BenchmarkBudgetV1, BenchmarkEvidenceScope, BenchmarkGateError,
    BenchmarkGateLevel, BenchmarkGateResultV1, BenchmarkMetricsV1, BenchmarkWorkloadMetricsV1,
    ObjectRequestMetricsV1,
};

#[test]
fn local_smoke_result_validates_when_required_metrics_are_present() {
    let result: BenchmarkGateResultV1 = serde_json::from_str(VALID_LOCAL_SMOKE_JSON).unwrap();

    result.validate().unwrap();
}

#[test]
fn benchmark_gate_validation_fails_when_s3_backend_uses_local_workload() {
    let mut result = s3_nightly_result();
    result.workload = "local_incremental".to_string();

    let error = result.validate().unwrap_err();

    assert!(matches!(
        error,
        BenchmarkGateError::BackendWorkloadMismatch { .. }
    ));
}

#[test]
fn benchmark_gate_rejects_placeholder_s3_current_result() {
    let mut result = s3_nightly_result();
    result.commit = "placeholder-s3-nightly".to_string();

    let error = result.reject_placeholder_s3_commit().unwrap_err();

    assert!(matches!(
        error,
        BenchmarkGateError::PlaceholderS3Commit { .. }
    ));
}

#[test]
fn benchmark_gate_rejects_unknown_s3_current_result() {
    let mut result = s3_nightly_result();
    result.commit = "unknown".to_string();

    let error = result.reject_placeholder_s3_commit().unwrap_err();

    assert!(matches!(
        error,
        BenchmarkGateError::PlaceholderS3Commit { .. }
    ));
}

#[test]
fn benchmark_gate_rejects_all_zero_s3_current_result() {
    let mut result = s3_nightly_result();
    result.commit = "0000000000000000000000000000000000000000".to_string();

    let error = result.reject_placeholder_s3_commit().unwrap_err();

    assert!(matches!(
        error,
        BenchmarkGateError::PlaceholderS3Commit { .. }
    ));
}

#[test]
fn benchmark_gate_allows_placeholder_local_current_result() {
    let mut result = local_smoke_result();
    result.commit = "placeholder-local-pr-smoke-result".to_string();

    result.reject_placeholder_s3_commit().unwrap();
}

#[test]
fn benchmark_gate_accepts_s3_nightly_result_with_real_commit() {
    let result = s3_nightly_result();

    result.validate().unwrap();
}

#[test]
fn benchmark_gate_defaults_missing_evidence_scope_to_live_or_native() {
    let result: BenchmarkGateResultV1 = serde_json::from_str(VALID_LOCAL_SMOKE_JSON).unwrap();

    assert_eq!(
        result.backend_evidence_scope,
        BenchmarkEvidenceScope::LiveOrNative
    );
}

#[test]
fn benchmark_gate_rejects_local_emulator_s3_evidence() {
    let mut result = s3_nightly_result();
    result.backend_evidence_scope = BenchmarkEvidenceScope::LocalEmulator;

    let error = result.reject_local_emulator_s3_evidence().unwrap_err();

    assert!(matches!(error, BenchmarkGateError::LocalEmulatorS3Evidence));
}

#[test]
fn benchmark_comparison_rejects_local_emulator_s3_current_result() {
    let mut current = s3_nightly_result();
    current.backend_evidence_scope = BenchmarkEvidenceScope::LocalEmulator;
    let baseline = s3_nightly_result();

    let error = current
        .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
        .unwrap_err();

    assert!(matches!(error, BenchmarkGateError::LocalEmulatorS3Evidence));
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
fn benchmark_result_validation_fails_when_workload_metrics_are_empty() {
    let mut result = local_smoke_result();
    result.workload_metrics.clear();

    let error = result.validate().unwrap_err();

    assert!(error.to_string().contains("workload_metrics"));
}

#[test]
fn benchmark_result_validation_fails_when_workload_metric_names_repeat() {
    let mut result = local_smoke_result();
    result
        .workload_metrics
        .push(result.workload_metrics[0].clone());

    let error = result.validate().unwrap_err();

    assert!(error.to_string().contains("duplicate workload metric"));
}

#[test]
fn benchmark_result_validation_fails_when_workload_p95_is_below_p50() {
    let mut result = local_smoke_result();
    result.workload_metrics[0].p50_ms = 5.0;
    result.workload_metrics[0].p95_ms = 4.0;

    let error = result.validate().unwrap_err();

    assert!(error.to_string().contains("p95_ms"));
}

#[test]
fn benchmark_result_validation_fails_when_checkpoint_p95_is_below_p50() {
    let mut result = local_smoke_result();
    result.metrics.checkpoint_p50_ms = 5.0;
    result.metrics.checkpoint_p95_ms = 4.0;

    let error = result.validate().unwrap_err();

    assert!(matches!(
        error,
        BenchmarkGateError::InvalidCheckpointLatencyOrder {
            p50_ms: 5.0,
            p95_ms: 4.0
        }
    ));
}

#[test]
fn benchmark_result_validation_fails_when_object_backed_workload_has_no_requests() {
    let mut result = local_smoke_result();
    result.workload_metrics[0].object_requests = None;

    let error = result.validate().unwrap_err();

    assert!(error.to_string().contains("object_requests"));
}

#[test]
fn benchmark_gate_can_require_specific_workload_names() {
    let result = local_smoke_result();

    result
        .require_workloads(&[
            "object_store_capability_probe",
            "ingest_envelope_validation",
            "checkpoint_publish",
            "checkpoint_recovery",
            "datafusion_table_scan",
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
            "aggregate_composite_high_cardinality",
            "aggregate_composite_hot_key_skew",
            "inner_join_one_to_one",
            "inner_join_one_to_many",
            "inner_join_many_to_many",
            "inner_join_hot_key_skew",
            "inner_join_unmatched",
        ])
        .unwrap();
}

#[test]
fn benchmark_gate_rejects_missing_required_workload_name() {
    let result = local_smoke_result();

    let error = result
        .require_workloads(&["ingest_envelope_validation", "future_live_s3_workload"])
        .unwrap_err();

    assert!(error.to_string().contains("future_live_s3_workload"));
}

#[test]
fn benchmark_comparison_rejects_local_smoke_against_s3_baseline() {
    let current = local_smoke_result();
    let mut baseline = local_smoke_result();
    baseline.backend = BenchmarkBackend::S3Compatible;
    baseline.gate_level = BenchmarkGateLevel::NightlyIntegration;
    baseline.workload = "s3_incremental".to_string();

    let error = current
        .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
        .unwrap_err();

    assert!(matches!(error, BenchmarkGateError::BaselineMismatch { .. }));
}

#[test]
fn benchmark_result_rejects_unexpected_gate_or_backend() {
    let result = local_smoke_result();

    let error = result
        .expect_gate(BenchmarkGateLevel::Release, BenchmarkBackend::Local)
        .unwrap_err();

    assert!(matches!(
        error,
        BenchmarkGateError::ExpectationMismatch { .. }
    ));
}

#[test]
fn benchmark_comparison_fails_when_synthetic_regression_exceeds_budget() {
    let mut current = s3_nightly_result();
    let baseline = s3_nightly_result();
    current.metrics.rows_per_second = baseline.metrics.rows_per_second * 0.88;

    let error = current
        .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
        .unwrap_err();

    assert!(matches!(error, BenchmarkGateError::Regression { .. }));
}

#[test]
fn benchmark_comparison_ignores_capability_probe_latency_regression() {
    let mut current = local_smoke_result();
    let baseline = local_smoke_result();
    current.workload_metrics[0].p50_ms = baseline.workload_metrics[0].p50_ms * 10.0;
    current.workload_metrics[0].p95_ms = baseline.workload_metrics[0].p95_ms * 10.0;

    current
        .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
        .unwrap();
}

#[test]
fn benchmark_comparison_fails_when_performance_workload_latency_regresses_over_budget() {
    let mut current = s3_nightly_result();
    let baseline = s3_nightly_result();
    current.workload_metrics[1].p95_ms = baseline.workload_metrics[1].p95_ms * 1.20;

    let error = current
        .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
        .unwrap_err();

    assert!(matches!(
        error,
        BenchmarkGateError::WorkloadRegression {
            workload,
            metric: "p95_ms",
            ..
        } if workload == "ingest_envelope_validation"
    ));
}

#[test]
fn benchmark_comparison_still_checks_capability_probe_object_requests() {
    let mut current = local_smoke_result();
    let baseline = local_smoke_result();
    current.workload_metrics[0]
        .object_requests
        .as_mut()
        .unwrap()
        .put_count = baseline.workload_metrics[0]
        .object_requests
        .as_ref()
        .unwrap()
        .put_count
        + 1;

    let error = current
        .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
        .unwrap_err();

    assert!(matches!(
        error,
        BenchmarkGateError::WorkloadRegression {
            workload,
            metric: "put_count",
            ..
        } if workload == "object_store_capability_probe"
    ));
}

#[test]
fn benchmark_comparison_fails_when_baseline_lacks_workload_metric() {
    let current = local_smoke_result();
    let mut baseline = local_smoke_result();
    baseline.workload_metrics.pop();

    let error = current
        .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
        .unwrap_err();

    assert!(matches!(
        error,
        BenchmarkGateError::MissingBaselineWorkload { name } if name == "gc_execution_evidence"
    ));
}

#[test]
fn benchmark_comparison_fails_when_current_lacks_baseline_workload_metric() {
    let mut current = local_smoke_result();
    let baseline = local_smoke_result();
    current.workload_metrics.pop();

    let error = current
        .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
        .unwrap_err();

    assert!(matches!(
        error,
        BenchmarkGateError::MissingCurrentWorkload { name } if name == "gc_execution_evidence"
    ));
}

#[test]
fn benchmark_comparison_passes_when_synthetic_regression_is_within_budget() {
    let mut current = s3_nightly_result();
    let baseline = s3_nightly_result();
    current.metrics.rows_per_second = baseline.metrics.rows_per_second * 0.91;

    current
        .compare_against(&baseline, BenchmarkBudgetV1::relative(0.10))
        .unwrap();
}

#[test]
fn local_pr_smoke_ignores_wall_clock_regressions() {
    let mut current = local_smoke_result();
    let baseline = local_smoke_result();
    current.metrics.rows_per_second = 1.0;
    current.metrics.checkpoint_p50_ms *= 10.0;
    current.metrics.checkpoint_p95_ms *= 10.0;
    current.metrics.recovery_p95_ms *= 10.0;
    current.metrics.peak_rss_bytes = 1;
    current.workload_metrics[1].p50_ms *= 10.0;
    current.workload_metrics[1].p95_ms *= 10.0;

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
        backend_evidence_scope: BenchmarkEvidenceScope::LiveOrNative,
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
        workload_metrics: local_workload_metrics(),
    }
}

fn s3_nightly_result() -> BenchmarkGateResultV1 {
    let mut result = local_smoke_result();
    result.commit = "abc123def456".to_string();
    result.gate_level = BenchmarkGateLevel::NightlyIntegration;
    result.backend = BenchmarkBackend::S3Compatible;
    result.workload = "s3_incremental".to_string();
    result
}

fn local_workload_metrics() -> Vec<BenchmarkWorkloadMetricsV1> {
    vec![
        BenchmarkWorkloadMetricsV1 {
            name: "object_store_capability_probe".to_string(),
            p50_ms: 1.0,
            p95_ms: 1.0,
            object_requests: Some(ObjectRequestMetricsV1 {
                put_count: 4,
                get_count: 4,
                list_count: 4,
                range_read_count: 0,
                bytes_written: 1024,
                bytes_read: 1024,
            }),
            scan_bytes: 0,
        },
        BenchmarkWorkloadMetricsV1 {
            name: "ingest_envelope_validation".to_string(),
            p50_ms: 2.0,
            p95_ms: 3.0,
            object_requests: Some(ObjectRequestMetricsV1 {
                put_count: 4,
                get_count: 0,
                list_count: 0,
                range_read_count: 0,
                bytes_written: 1024,
                bytes_read: 0,
            }),
            scan_bytes: 0,
        },
        BenchmarkWorkloadMetricsV1 {
            name: "checkpoint_publish".to_string(),
            p50_ms: 3.0,
            p95_ms: 4.0,
            object_requests: Some(ObjectRequestMetricsV1 {
                put_count: 2,
                get_count: 0,
                list_count: 0,
                range_read_count: 0,
                bytes_written: 512,
                bytes_read: 0,
            }),
            scan_bytes: 0,
        },
        BenchmarkWorkloadMetricsV1 {
            name: "checkpoint_recovery".to_string(),
            p50_ms: 7.0,
            p95_ms: 7.0,
            object_requests: Some(ObjectRequestMetricsV1 {
                put_count: 0,
                get_count: 2,
                list_count: 1,
                range_read_count: 0,
                bytes_written: 0,
                bytes_read: 512,
            }),
            scan_bytes: 0,
        },
        BenchmarkWorkloadMetricsV1 {
            name: "datafusion_table_scan".to_string(),
            p50_ms: 5.0,
            p95_ms: 6.0,
            object_requests: Some(ObjectRequestMetricsV1 {
                put_count: 0,
                get_count: 1,
                list_count: 1,
                range_read_count: 2,
                bytes_written: 0,
                bytes_read: 2048,
            }),
            scan_bytes: 1024,
        },
        BenchmarkWorkloadMetricsV1 {
            name: "materialized_output_segment_pruning".to_string(),
            p50_ms: 2.0,
            p95_ms: 3.0,
            object_requests: Some(ObjectRequestMetricsV1 {
                put_count: 0,
                get_count: 4,
                list_count: 0,
                range_read_count: 0,
                bytes_written: 0,
                bytes_read: 512,
            }),
            scan_bytes: 128,
        },
        materialized_output_metric("materialized_output_recent_k"),
        materialized_output_metric("materialized_output_compaction_equivalence"),
        materialized_output_metric("materialized_output_compaction_debt"),
        materialized_output_metric("materialized_output_delete_vector"),
        materialized_output_metric("materialized_output_ttl_vector"),
        materialized_output_metric("materialized_output_late_materialization"),
        BenchmarkWorkloadMetricsV1 {
            name: "slatedb_state_reopen".to_string(),
            p50_ms: 8.0,
            p95_ms: 9.0,
            object_requests: Some(ObjectRequestMetricsV1 {
                put_count: 1,
                get_count: 1,
                list_count: 1,
                range_read_count: 0,
                bytes_written: 256,
                bytes_read: 256,
            }),
            scan_bytes: 0,
        },
        BenchmarkWorkloadMetricsV1 {
            name: "gc_dry_run_planning".to_string(),
            p50_ms: 4.0,
            p95_ms: 5.0,
            object_requests: Some(ObjectRequestMetricsV1 {
                put_count: 0,
                get_count: 2,
                list_count: 3,
                range_read_count: 0,
                bytes_written: 0,
                bytes_read: 1024,
            }),
            scan_bytes: 0,
        },
        in_memory_scale_metric("aggregate_composite_high_cardinality"),
        in_memory_scale_metric("aggregate_composite_hot_key_skew"),
        in_memory_scale_metric("inner_join_one_to_one"),
        in_memory_scale_metric("inner_join_one_to_many"),
        in_memory_scale_metric("inner_join_many_to_many"),
        in_memory_scale_metric("inner_join_hot_key_skew"),
        in_memory_scale_metric("inner_join_unmatched"),
        BenchmarkWorkloadMetricsV1 {
            name: "gc_execution_evidence".to_string(),
            p50_ms: 4.0,
            p95_ms: 5.0,
            object_requests: Some(ObjectRequestMetricsV1 {
                put_count: 1,
                get_count: 4,
                list_count: 4,
                range_read_count: 0,
                bytes_written: 1024,
                bytes_read: 2048,
            }),
            scan_bytes: 0,
        },
    ]
}

fn in_memory_scale_metric(name: &str) -> BenchmarkWorkloadMetricsV1 {
    BenchmarkWorkloadMetricsV1 {
        name: name.to_string(),
        p50_ms: 2.0,
        p95_ms: 3.0,
        object_requests: Some(ObjectRequestMetricsV1 {
            put_count: 0,
            get_count: 0,
            list_count: 0,
            range_read_count: 0,
            bytes_written: 0,
            bytes_read: 0,
        }),
        scan_bytes: 0,
    }
}

fn materialized_output_metric(name: &str) -> BenchmarkWorkloadMetricsV1 {
    BenchmarkWorkloadMetricsV1 {
        name: name.to_string(),
        p50_ms: 2.0,
        p95_ms: 3.0,
        object_requests: Some(ObjectRequestMetricsV1 {
            put_count: 0,
            get_count: 4,
            list_count: 0,
            range_read_count: 0,
            bytes_written: 0,
            bytes_read: 512,
        }),
        scan_bytes: 128,
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
    },
    "workload_metrics": [
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
                "bytes_read": 1024
            },
            "scan_bytes": 0
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
                "bytes_read": 0
            },
            "scan_bytes": 0
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
                "bytes_read": 0
            },
            "scan_bytes": 0
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
                "bytes_read": 512
            },
            "scan_bytes": 0
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
                "bytes_read": 2048
            },
            "scan_bytes": 1024
        },
        {
            "name": "materialized_output_segment_pruning",
            "p50_ms": 2.0,
            "p95_ms": 3.0,
            "object_requests": {
                "put_count": 0,
                "get_count": 4,
                "list_count": 0,
                "range_read_count": 0,
                "bytes_written": 0,
                "bytes_read": 512
            },
            "scan_bytes": 128
        },
        {
            "name": "materialized_output_recent_k",
            "p50_ms": 2.0,
            "p95_ms": 3.0,
            "object_requests": {
                "put_count": 0,
                "get_count": 4,
                "list_count": 0,
                "range_read_count": 0,
                "bytes_written": 0,
                "bytes_read": 512
            },
            "scan_bytes": 128
        },
        {
            "name": "materialized_output_compaction_equivalence",
            "p50_ms": 2.0,
            "p95_ms": 3.0,
            "object_requests": {
                "put_count": 0,
                "get_count": 4,
                "list_count": 0,
                "range_read_count": 0,
                "bytes_written": 0,
                "bytes_read": 512
            },
            "scan_bytes": 128
        },
        {
            "name": "materialized_output_compaction_debt",
            "p50_ms": 2.0,
            "p95_ms": 3.0,
            "object_requests": {
                "put_count": 0,
                "get_count": 4,
                "list_count": 0,
                "range_read_count": 0,
                "bytes_written": 0,
                "bytes_read": 512
            },
            "scan_bytes": 128
        },
        {
            "name": "materialized_output_delete_vector",
            "p50_ms": 2.0,
            "p95_ms": 3.0,
            "object_requests": {
                "put_count": 0,
                "get_count": 4,
                "list_count": 0,
                "range_read_count": 0,
                "bytes_written": 0,
                "bytes_read": 512
            },
            "scan_bytes": 128
        },
        {
            "name": "materialized_output_ttl_vector",
            "p50_ms": 2.0,
            "p95_ms": 3.0,
            "object_requests": {
                "put_count": 0,
                "get_count": 4,
                "list_count": 0,
                "range_read_count": 0,
                "bytes_written": 0,
                "bytes_read": 512
            },
            "scan_bytes": 128
        },
        {
            "name": "materialized_output_late_materialization",
            "p50_ms": 2.0,
            "p95_ms": 3.0,
            "object_requests": {
                "put_count": 0,
                "get_count": 4,
                "list_count": 0,
                "range_read_count": 0,
                "bytes_written": 0,
                "bytes_read": 512
            },
            "scan_bytes": 128
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
                "bytes_read": 256
            },
            "scan_bytes": 0
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
                "bytes_read": 1024
            },
            "scan_bytes": 0
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
                "bytes_read": 2048
            },
            "scan_bytes": 0
        }
    ]
}"#;
