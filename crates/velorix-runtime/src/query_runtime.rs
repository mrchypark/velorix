use std::{future::Future, sync::Arc, time::Duration};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use velorix_core::query::{QueryError, QueryPolicy, QueryPolicyError};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct QueryRuntimeLimits {
    planning_timeout_ms: Option<u64>,
    execution_timeout_ms: Option<u64>,
}

impl QueryRuntimeLimits {
    pub(crate) fn from_policy(policy: QueryPolicy) -> Self {
        Self {
            planning_timeout_ms: policy.planning_timeout_ms,
            execution_timeout_ms: policy.execution_timeout_ms,
        }
    }

    pub(crate) async fn run_planning<T, F>(&self, operation: F) -> Result<T, QueryError>
    where
        F: Future<Output = Result<T, QueryError>>,
    {
        let Some(timeout_ms) = self.planning_timeout_ms else {
            return operation.await;
        };

        run_with_timeout(
            timeout_ms,
            |timeout_ms| QueryPolicyError::PlanningTimeout { timeout_ms },
            operation,
        )
        .await
    }

    pub(crate) async fn run_execution<T, F>(&self, operation: F) -> Result<T, QueryError>
    where
        F: Future<Output = Result<T, QueryError>>,
    {
        let Some(timeout_ms) = self.execution_timeout_ms else {
            return operation.await;
        };

        run_with_timeout(
            timeout_ms,
            |timeout_ms| QueryPolicyError::ExecutionTimeout { timeout_ms },
            operation,
        )
        .await
    }
}

#[derive(Clone, Debug)]
pub struct QueryExecutionLimiter {
    permits: Arc<Semaphore>,
    max_concurrent_queries: usize,
}

impl QueryExecutionLimiter {
    pub fn from_policy(policy: QueryPolicy) -> Option<Self> {
        policy
            .max_concurrent_queries
            .map(|max_concurrent_queries| Self {
                permits: Arc::new(Semaphore::new(max_concurrent_queries)),
                max_concurrent_queries,
            })
    }

    pub(crate) fn try_acquire(&self) -> Result<OwnedSemaphorePermit, QueryPolicyError> {
        self.permits.clone().try_acquire_owned().map_err(|_| {
            QueryPolicyError::ConcurrencyLimitExceeded {
                max_concurrent_queries: self.max_concurrent_queries,
            }
        })
    }
}

async fn run_with_timeout<T, F>(
    timeout_ms: u64,
    timeout_error: fn(u64) -> QueryPolicyError,
    operation: F,
) -> Result<T, QueryError>
where
    F: Future<Output = Result<T, QueryError>>,
{
    tokio::time::timeout(Duration::from_millis(timeout_ms), operation)
        .await
        .map_err(|_| QueryError::from(timeout_error(timeout_ms)))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn query_runtime_limits_returns_planning_timeout_when_planning_does_not_finish() {
        let limits = QueryRuntimeLimits::from_policy(QueryPolicy {
            planning_timeout_ms: Some(1),
            ..QueryPolicy::default()
        });
        let error = limits
            .run_planning(std::future::pending::<Result<(), QueryError>>())
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            QueryError::Policy(QueryPolicyError::PlanningTimeout { timeout_ms: 1 })
        ));
    }

    #[tokio::test]
    async fn query_runtime_limits_returns_execution_timeout_when_execution_does_not_finish() {
        let limits = QueryRuntimeLimits::from_policy(QueryPolicy {
            execution_timeout_ms: Some(1),
            ..QueryPolicy::default()
        });
        let error = limits
            .run_execution(std::future::pending::<Result<(), QueryError>>())
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            QueryError::Policy(QueryPolicyError::ExecutionTimeout { timeout_ms: 1 })
        ));
    }
}
