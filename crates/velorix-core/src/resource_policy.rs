use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryExecutionPolicyV1 {
    pub max_sql_bytes: Option<usize>,
    pub planning_timeout_ms: Option<u64>,
    pub execution_timeout_ms: Option<u64>,
    pub max_output_rows: Option<usize>,
    pub max_output_bytes: Option<u64>,
    pub max_scan_files: Option<usize>,
    pub max_scan_bytes: Option<u64>,
    pub max_object_requests: Option<usize>,
    pub max_concurrent_queries: Option<usize>,
    pub memory_limit_bytes: Option<u64>,
    pub spill_limit_bytes: Option<u64>,
    pub batch_size: Option<NonZeroUsize>,
    pub target_partitions: Option<NonZeroUsize>,
}

pub type QueryPolicy = QueryExecutionPolicyV1;

impl QueryExecutionPolicyV1 {
    pub fn validate(self) -> Result<(), QueryPolicyError> {
        validate_non_zero_timeout("planning_timeout_ms", self.planning_timeout_ms)?;
        validate_non_zero_timeout("execution_timeout_ms", self.execution_timeout_ms)?;

        if matches!(self.max_concurrent_queries, Some(0)) {
            return Err(QueryPolicyError::InvalidZeroConcurrency {
                field: "max_concurrent_queries",
            });
        }

        validate_non_zero_budget("memory_limit_bytes", self.memory_limit_bytes)?;
        validate_non_zero_budget("spill_limit_bytes", self.spill_limit_bytes)?;

        Ok(())
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum QueryPolicyError {
    #[error("SQL text is {actual_bytes} bytes, above query policy limit of {max_bytes} bytes")]
    SqlTextTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },
    #[error(
        "query returned at least {observed_rows} rows, above query policy limit of {max_rows} rows"
    )]
    OutputRowsExceeded {
        observed_rows: usize,
        max_rows: usize,
    },
    #[error(
        "query returned at least {observed_bytes} bytes, above query policy limit of {max_bytes} bytes"
    )]
    OutputBytesExceeded { observed_bytes: u64, max_bytes: u64 },
    #[error(
        "query would scan {observed_files} files, above query policy limit of {max_files} files"
    )]
    ScanFilesExceeded {
        observed_files: usize,
        max_files: usize,
    },
    #[error(
        "query would scan {observed_bytes} bytes, above query policy limit of {max_bytes} bytes"
    )]
    ScanBytesExceeded { observed_bytes: u64, max_bytes: u64 },
    #[error(
        "query would issue at least {observed_requests} object requests, above query policy limit of {max_requests} object requests"
    )]
    ObjectRequestsExceeded {
        observed_requests: usize,
        max_requests: usize,
    },
    #[error("query planning exceeded query policy timeout of {timeout_ms} ms")]
    PlanningTimeout { timeout_ms: u64 },
    #[error("query execution exceeded query policy timeout of {timeout_ms} ms")]
    ExecutionTimeout { timeout_ms: u64 },
    #[error(
        "query concurrency limit of {max_concurrent_queries} concurrent queries is already in use"
    )]
    ConcurrencyLimitExceeded { max_concurrent_queries: usize },
    #[error(
        "query policy requires a shared concurrency limiter for {max_concurrent_queries} concurrent queries"
    )]
    ConcurrencyLimiterRequired { max_concurrent_queries: usize },
    #[error("query policy field {field} must be greater than zero when set")]
    InvalidZeroTimeout { field: &'static str },
    #[error("query policy field {field} must be greater than zero when set")]
    InvalidZeroConcurrency { field: &'static str },
    #[error("query policy field {field} must be greater than zero when set")]
    InvalidZeroBudget { field: &'static str },
}

fn validate_non_zero_timeout(
    field: &'static str,
    value: Option<u64>,
) -> Result<(), QueryPolicyError> {
    if matches!(value, Some(0)) {
        return Err(QueryPolicyError::InvalidZeroTimeout { field });
    }

    Ok(())
}

fn validate_non_zero_budget(
    field: &'static str,
    value: Option<u64>,
) -> Result<(), QueryPolicyError> {
    if matches!(value, Some(0)) {
        return Err(QueryPolicyError::InvalidZeroBudget { field });
    }

    Ok(())
}
