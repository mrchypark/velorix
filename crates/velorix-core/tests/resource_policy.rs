use std::num::NonZeroUsize;

use velorix_core::{
    query::{QueryPolicy, QueryPolicyError},
    resource_policy::QueryExecutionPolicyV1,
};

#[test]
fn query_execution_policy_v1_rejects_unknown_json_fields() {
    let error = serde_json::from_value::<QueryExecutionPolicyV1>(serde_json::json!({
        "max_sql_bytes": 1024,
        "unexpected": true,
    }))
    .unwrap_err();

    assert!(error.to_string().contains("unknown field"));
}

#[test]
fn query_execution_policy_v1_validation_rejects_zero_planning_timeout() {
    let policy = QueryExecutionPolicyV1 {
        planning_timeout_ms: Some(0),
        ..QueryExecutionPolicyV1::default()
    };

    assert_eq!(
        policy.validate().unwrap_err(),
        QueryPolicyError::InvalidZeroTimeout {
            field: "planning_timeout_ms"
        }
    );
}

#[test]
fn query_execution_policy_v1_validation_rejects_zero_execution_timeout() {
    let policy = QueryExecutionPolicyV1 {
        execution_timeout_ms: Some(0),
        ..QueryExecutionPolicyV1::default()
    };

    assert_eq!(
        policy.validate().unwrap_err(),
        QueryPolicyError::InvalidZeroTimeout {
            field: "execution_timeout_ms"
        }
    );
}

#[test]
fn query_execution_policy_v1_validation_rejects_zero_concurrency() {
    let policy = QueryExecutionPolicyV1 {
        max_concurrent_queries: Some(0),
        ..QueryExecutionPolicyV1::default()
    };

    assert_eq!(
        policy.validate().unwrap_err(),
        QueryPolicyError::InvalidZeroConcurrency {
            field: "max_concurrent_queries"
        }
    );
}

#[test]
fn query_execution_policy_v1_validation_rejects_zero_memory_budget() {
    let policy = QueryExecutionPolicyV1 {
        memory_limit_bytes: Some(0),
        ..QueryExecutionPolicyV1::default()
    };

    assert_eq!(
        policy.validate().unwrap_err(),
        QueryPolicyError::InvalidZeroBudget {
            field: "memory_limit_bytes"
        }
    );
}

#[test]
fn query_execution_policy_v1_validation_rejects_zero_spill_budget() {
    let policy = QueryExecutionPolicyV1 {
        spill_limit_bytes: Some(0),
        ..QueryExecutionPolicyV1::default()
    };

    assert_eq!(
        policy.validate().unwrap_err(),
        QueryPolicyError::InvalidZeroBudget {
            field: "spill_limit_bytes"
        }
    );
}

#[test]
fn query_execution_policy_v1_deserialization_rejects_zero_batch_size() {
    let error = serde_json::from_value::<QueryExecutionPolicyV1>(serde_json::json!({
        "batch_size": 0,
    }))
    .unwrap_err();

    assert!(error.to_string().contains("invalid value"));
}

#[test]
fn old_query_policy_callers_still_compile_and_use_existing_fields() {
    let policy = QueryPolicy {
        max_sql_bytes: Some(2048),
        max_output_rows: Some(10),
        max_scan_files: Some(3),
        max_scan_bytes: Some(4096),
        max_object_requests: Some(4),
        batch_size: NonZeroUsize::new(32),
        target_partitions: NonZeroUsize::new(2),
        ..QueryPolicy::default()
    };

    assert_eq!(policy.max_sql_bytes, Some(2048));
    assert_eq!(policy.max_output_rows, Some(10));
    assert_eq!(policy.max_scan_files, Some(3));
    assert_eq!(policy.max_scan_bytes, Some(4096));
    assert_eq!(policy.max_object_requests, Some(4));
    assert_eq!(policy.batch_size.map(NonZeroUsize::get), Some(32));
    assert_eq!(policy.target_partitions.map(NonZeroUsize::get), Some(2));
}

#[test]
fn output_byte_policy_error_displays_and_compares_by_observed_and_limit() {
    let error = QueryPolicyError::OutputBytesExceeded {
        observed_bytes: 2049,
        max_bytes: 2048,
    };

    assert_eq!(
        error,
        QueryPolicyError::OutputBytesExceeded {
            observed_bytes: 2049,
            max_bytes: 2048
        }
    );
    assert_eq!(
        error.to_string(),
        "query returned at least 2049 bytes, above query policy limit of 2048 bytes"
    );
}
