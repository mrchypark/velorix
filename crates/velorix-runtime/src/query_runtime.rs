use std::{future::Future, sync::Arc, time::Duration};

use datafusion::{
    error::DataFusionError,
    execution::runtime_env::RuntimeEnvBuilder,
    prelude::{SessionConfig, SessionContext},
};
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DataFusionSessionFactory {
    batch_size: Option<usize>,
    target_partitions: Option<usize>,
    memory_limit_bytes: Option<u64>,
    spill_limit_bytes: Option<u64>,
}

impl DataFusionSessionFactory {
    pub(crate) fn from_policy(policy: QueryPolicy) -> Self {
        Self {
            batch_size: policy.batch_size.map(|batch_size| batch_size.get()),
            target_partitions: policy
                .target_partitions
                .map(|target_partitions| target_partitions.get()),
            memory_limit_bytes: policy.memory_limit_bytes,
            spill_limit_bytes: policy.spill_limit_bytes,
        }
    }

    pub(crate) fn session_context(self) -> Result<SessionContext, QueryError> {
        let mut config = SessionConfig::new();
        if let Some(batch_size) = self.batch_size {
            config = config.with_batch_size(batch_size);
        }
        if let Some(target_partitions) = self.target_partitions {
            config = config.with_target_partitions(target_partitions);
        }

        let mut runtime = RuntimeEnvBuilder::new();
        if let Some(memory_limit_bytes) = self.memory_limit_bytes {
            runtime = runtime.with_memory_limit(memory_limit_usize(memory_limit_bytes)?, 1.0);
        }
        if let Some(spill_limit_bytes) = self.spill_limit_bytes {
            runtime = runtime.with_max_temp_directory_size(spill_limit_bytes);
        }

        Ok(SessionContext::new_with_config_rt(
            config,
            runtime.build_arc().map_err(QueryError::engine)?,
        ))
    }
}

fn memory_limit_usize(memory_limit_bytes: u64) -> Result<usize, QueryError> {
    usize::try_from(memory_limit_bytes).map_err(|_| {
        QueryError::engine(DataFusionError::Configuration(
            "memory_limit_bytes exceeds usize".into(),
        ))
    })
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

    pub fn max_concurrent_queries(&self) -> usize {
        self.max_concurrent_queries
    }

    pub(crate) fn try_acquire(&self) -> Result<OwnedSemaphorePermit, QueryPolicyError> {
        self.permits.clone().try_acquire_owned().map_err(|_| {
            QueryPolicyError::ConcurrencyLimitExceeded {
                max_concurrent_queries: self.max_concurrent_queries,
            }
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct ProductionQueryRuntime {
    limiter: Option<QueryExecutionLimiter>,
}

impl ProductionQueryRuntime {
    pub fn from_policy(policy: QueryPolicy) -> Self {
        Self {
            limiter: QueryExecutionLimiter::from_policy(policy),
        }
    }

    pub fn compatible_limiter(
        &self,
        policy: QueryPolicy,
    ) -> Result<Option<QueryExecutionLimiter>, QueryError> {
        policy.validate().map_err(QueryError::from)?;
        match (policy.max_concurrent_queries, self.limiter.as_ref()) {
            (Some(max_concurrent_queries), None) => {
                Err(QueryPolicyError::ConcurrencyLimiterRequired {
                    max_concurrent_queries,
                }
                .into())
            }
            (Some(required_max_concurrent_queries), Some(limiter))
                if limiter.max_concurrent_queries() != required_max_concurrent_queries =>
            {
                Err(QueryPolicyError::ConcurrencyLimiterPolicyMismatch {
                    required_max_concurrent_queries,
                    actual_max_concurrent_queries: limiter.max_concurrent_queries(),
                }
                .into())
            }
            (Some(_), Some(limiter)) | (None, Some(limiter)) => Ok(Some(limiter.clone())),
            (None, None) => Ok(None),
        }
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
    use datafusion::execution::memory_pool::MemoryLimit;
    use std::{io::Write, num::NonZeroUsize};

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

    #[test]
    fn datafusion_session_factory_captures_policy_config() {
        let policy = QueryPolicy {
            batch_size: Some(NonZeroUsize::new(7).unwrap()),
            target_partitions: Some(NonZeroUsize::new(3).unwrap()),
            memory_limit_bytes: Some(64 * 1024 * 1024),
            spill_limit_bytes: Some(32 * 1024 * 1024),
            ..QueryPolicy::default()
        };

        let context = DataFusionSessionFactory::from_policy(policy)
            .session_context()
            .unwrap();
        let config = context.copied_config();
        let runtime = context.runtime_env();

        assert_eq!(config.batch_size(), 7);
        assert_eq!(config.target_partitions(), 3);
        assert!(matches!(
            runtime.memory_pool.memory_limit(),
            MemoryLimit::Finite(limit) if limit == 64 * 1024 * 1024
        ));
        assert_eq!(
            runtime.disk_manager.max_temp_directory_size(),
            32 * 1024 * 1024
        );
    }

    #[test]
    fn datafusion_session_factory_enforces_spill_quota_through_datafusion_disk_manager() {
        let policy = QueryPolicy {
            spill_limit_bytes: Some(8),
            ..QueryPolicy::default()
        };

        let context = DataFusionSessionFactory::from_policy(policy)
            .session_context()
            .unwrap();
        let disk_manager = &context.runtime_env().disk_manager;
        let mut spill_file = disk_manager
            .create_tmp_file("Velorix spill quota policy evidence")
            .unwrap();

        spill_file
            .inner()
            .as_file()
            .write_all(b"spill-over")
            .unwrap();
        let error = spill_file.update_disk_usage().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("used disk space during the spilling process has exceeded"),
            "expected DataFusion disk-manager spill quota error, got {error:?}"
        );
    }

    #[test]
    fn production_query_runtime_keeps_policy_validation_error_precedence() {
        let runtime = ProductionQueryRuntime::from_policy(QueryPolicy {
            max_concurrent_queries: Some(1),
            ..QueryPolicy::default()
        });

        let error = runtime
            .compatible_limiter(QueryPolicy {
                max_concurrent_queries: Some(0),
                ..QueryPolicy::default()
            })
            .unwrap_err();

        assert!(matches!(
            error,
            QueryError::Policy(QueryPolicyError::InvalidZeroConcurrency {
                field: "max_concurrent_queries"
            })
        ));
    }
}
