use std::{future::Future, sync::Arc, time::Duration};

use arrow::{
    array::{
        BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder,
    },
    datatypes::Schema,
    record_batch::RecordBatch,
};
use datafusion::{
    common::ScalarValue,
    dataframe::DataFrame,
    datasource::MemTable,
    error::DataFusionError,
    execution::runtime_env::RuntimeEnvBuilder,
    logical_expr::LogicalPlan,
    prelude::{SessionConfig, SessionContext},
};
use futures::TryStreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use velorix_core::query::{QueryBindValue, QueryError, QueryPolicy, QueryPolicyError};

pub const MATERIALIZED_VIEW_RUNTIME_NAME: &str = "velorix_materialized_view_runtime";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct QueryRuntimeLimits {
    planning_timeout_ms: Option<u64>,
    execution_timeout_ms: Option<u64>,
}

impl QueryRuntimeLimits {
    fn from_policy(policy: QueryPolicy) -> Self {
        Self {
            planning_timeout_ms: policy.planning_timeout_ms,
            execution_timeout_ms: policy.execution_timeout_ms,
        }
    }

    async fn run_planning<T, F>(&self, operation: F) -> Result<T, QueryError>
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

    async fn run_execution<T, F>(&self, operation: F) -> Result<T, QueryError>
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
struct DataFusionSessionFactory {
    batch_size: Option<usize>,
    target_partitions: Option<usize>,
    memory_limit_bytes: Option<u64>,
    spill_limit_bytes: Option<u64>,
}

impl DataFusionSessionFactory {
    fn from_policy(policy: QueryPolicy) -> Self {
        Self {
            batch_size: policy.batch_size.map(|batch_size| batch_size.get()),
            target_partitions: policy
                .target_partitions
                .map(|target_partitions| target_partitions.get()),
            memory_limit_bytes: policy.memory_limit_bytes,
            spill_limit_bytes: policy.spill_limit_bytes,
        }
    }

    fn session_context(self) -> Result<SessionContext, QueryError> {
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

    fn try_acquire(&self) -> Result<OwnedSemaphorePermit, QueryPolicyError> {
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

pub async fn query_record_batches_table_with_bindings_and_policy_and_limiter(
    table_name: &str,
    batches: Vec<RecordBatch>,
    sql: &str,
    bind_values: &[QueryBindValue],
    policy: QueryPolicy,
    limiter: Option<QueryExecutionLimiter>,
) -> Result<Vec<RecordBatch>, QueryError> {
    policy.validate().map_err(QueryError::from)?;
    validate_sql_text_policy(sql, policy).map_err(QueryError::from)?;

    let _permit = acquire_query_permit(policy, limiter.as_ref())?;
    let context = record_batches_context(table_name, batches, policy)?;
    let limits = QueryRuntimeLimits::from_policy(policy);
    let dataframe = limits
        .run_planning(async {
            let dataframe = context.sql(sql).await.map_err(map_datafusion_error)?;
            let dataframe = apply_bind_values(dataframe, bind_values)?;
            let plan = dataframe
                .clone()
                .into_optimized_plan()
                .map_err(map_datafusion_error)?;
            validate_logical_plan_scans_only_table(&plan, table_name)?;
            Ok(dataframe)
        })
        .await?;

    collect_with_policy(dataframe, policy, limits).await
}

pub async fn validate_record_batch_table_query_with_bindings_and_policy(
    table_name: &str,
    table_schema: Arc<Schema>,
    sql: &str,
    bind_values: &[QueryBindValue],
    policy: QueryPolicy,
) -> Result<(), QueryError> {
    policy.validate().map_err(QueryError::from)?;
    validate_sql_text_policy(sql, policy).map_err(QueryError::from)?;

    let input = RecordBatch::new_empty(table_schema);
    let context = record_batches_context(table_name, vec![input], policy)?;
    let limits = QueryRuntimeLimits::from_policy(policy);
    limits
        .run_planning(async {
            let dataframe = context.sql(sql).await.map_err(map_datafusion_error)?;
            let plan = apply_bind_values(dataframe, bind_values)?
                .into_optimized_plan()
                .map_err(map_datafusion_error)?;
            validate_logical_plan_scans_only_table(&plan, table_name)?;
            Ok(())
        })
        .await
}

fn apply_bind_values(
    dataframe: DataFrame,
    bind_values: &[QueryBindValue],
) -> Result<DataFrame, QueryError> {
    if bind_values.is_empty() {
        return Ok(dataframe);
    }
    dataframe
        .with_param_values(
            bind_values
                .iter()
                .map(query_bind_value_to_scalar_value)
                .collect::<Vec<_>>(),
        )
        .map_err(map_datafusion_error)
}

fn query_bind_value_to_scalar_value(value: &QueryBindValue) -> ScalarValue {
    match value {
        QueryBindValue::Utf8(value) => ScalarValue::Utf8(Some(value.clone())),
        QueryBindValue::Json(value) => ScalarValue::Utf8(Some(value.clone())),
        QueryBindValue::Int64(value) => ScalarValue::Int64(Some(*value)),
        QueryBindValue::Float64(value) => ScalarValue::Float64(Some(*value)),
        QueryBindValue::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        QueryBindValue::Date(value)
        | QueryBindValue::Time(value)
        | QueryBindValue::Timestamp(value)
        | QueryBindValue::Uuid(value)
        | QueryBindValue::Decimal(value) => ScalarValue::Utf8(Some(value.clone())),
        QueryBindValue::Binary(value) => ScalarValue::Binary(Some(value.clone())),
        QueryBindValue::Utf8Array(values) | QueryBindValue::JsonArray(values) => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for value in values {
                builder.values().append_value(value);
            }
            builder.append(true);
            ScalarValue::List(Arc::new(builder.finish()))
        }
        QueryBindValue::Int64Array(values) => {
            let mut builder = ListBuilder::new(Int64Builder::new());
            for value in values {
                builder.values().append_value(*value);
            }
            builder.append(true);
            ScalarValue::List(Arc::new(builder.finish()))
        }
        QueryBindValue::Float64Array(values) => {
            let mut builder = ListBuilder::new(Float64Builder::new());
            for value in values {
                builder.values().append_value(*value);
            }
            builder.append(true);
            ScalarValue::List(Arc::new(builder.finish()))
        }
        QueryBindValue::BooleanArray(values) => {
            let mut builder = ListBuilder::new(BooleanBuilder::new());
            for value in values {
                builder.values().append_value(*value);
            }
            builder.append(true);
            ScalarValue::List(Arc::new(builder.finish()))
        }
        QueryBindValue::DateArray(values)
        | QueryBindValue::TimeArray(values)
        | QueryBindValue::TimestampArray(values)
        | QueryBindValue::UuidArray(values)
        | QueryBindValue::DecimalArray(values) => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for value in values {
                builder.values().append_value(value);
            }
            builder.append(true);
            ScalarValue::List(Arc::new(builder.finish()))
        }
        QueryBindValue::BinaryArray(values) => {
            let mut builder = ListBuilder::new(BinaryBuilder::new());
            for value in values {
                builder.values().append_value(value);
            }
            builder.append(true);
            ScalarValue::List(Arc::new(builder.finish()))
        }
    }
}

fn acquire_query_permit(
    policy: QueryPolicy,
    limiter: Option<&QueryExecutionLimiter>,
) -> Result<Option<OwnedSemaphorePermit>, QueryError> {
    match (policy.max_concurrent_queries, limiter) {
        (Some(max_concurrent_queries), None) => Err(QueryPolicyError::ConcurrencyLimiterRequired {
            max_concurrent_queries,
        }
        .into()),
        (Some(required_max_concurrent_queries), Some(limiter))
            if limiter.max_concurrent_queries() != required_max_concurrent_queries =>
        {
            Err(QueryPolicyError::ConcurrencyLimiterPolicyMismatch {
                required_max_concurrent_queries,
                actual_max_concurrent_queries: limiter.max_concurrent_queries(),
            }
            .into())
        }
        (_, Some(limiter)) => limiter.try_acquire().map(Some).map_err(QueryError::from),
        (None, None) => Ok(None),
    }
}

async fn collect_with_policy(
    dataframe: DataFrame,
    policy: QueryPolicy,
    limits: QueryRuntimeLimits,
) -> Result<Vec<RecordBatch>, QueryError> {
    let dataframe = match policy
        .max_output_rows
        .and_then(|max_rows| max_rows.checked_add(1))
    {
        Some(fetch) => dataframe
            .limit(0, Some(fetch))
            .map_err(map_datafusion_error)?,
        None => dataframe,
    };

    limits
        .run_execution(async { collect_record_batches(dataframe, policy).await })
        .await
}

async fn collect_record_batches(
    dataframe: DataFrame,
    policy: QueryPolicy,
) -> Result<Vec<RecordBatch>, QueryError> {
    let mut output = Vec::new();
    let mut observed_rows = 0usize;
    let mut observed_bytes = 0u64;
    let mut stream = dataframe
        .execute_stream()
        .await
        .map_err(map_datafusion_error)?;

    while let Some(batch) = stream.try_next().await.map_err(map_datafusion_error)? {
        observed_rows = observed_rows.saturating_add(batch.num_rows());
        observed_bytes = observed_bytes.saturating_add(record_batch_memory_size(&batch));

        if let Some(max_rows) = policy.max_output_rows {
            if observed_rows > max_rows {
                return Err(QueryPolicyError::OutputRowsExceeded {
                    observed_rows,
                    max_rows,
                }
                .into());
            }
        }

        if let Some(max_bytes) = policy.max_output_bytes {
            if observed_bytes > max_bytes {
                return Err(QueryPolicyError::OutputBytesExceeded {
                    observed_bytes,
                    max_bytes,
                }
                .into());
            }
        }

        output.push(batch);
    }

    Ok(output)
}

fn record_batch_memory_size(batch: &RecordBatch) -> u64 {
    u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX)
}

fn validate_sql_text_policy(sql: &str, policy: QueryPolicy) -> Result<(), QueryPolicyError> {
    if let Some(max_bytes) = policy.max_sql_bytes {
        let actual_bytes = sql.len();
        if actual_bytes > max_bytes {
            return Err(QueryPolicyError::SqlTextTooLarge {
                actual_bytes,
                max_bytes,
            });
        }
    }

    Ok(())
}

fn record_batches_context(
    table_name: &str,
    batches: Vec<RecordBatch>,
    policy: QueryPolicy,
) -> Result<SessionContext, QueryError> {
    let schema = schema_for_record_batches(&batches)?;
    let table = MemTable::try_new(schema, vec![batches]).map_err(map_datafusion_error)?;
    let context = session_context(policy)?;
    context
        .register_table(table_name, Arc::new(table))
        .map_err(map_datafusion_error)?;

    Ok(context)
}

fn schema_for_record_batches(batches: &[RecordBatch]) -> Result<Arc<Schema>, QueryError> {
    let first = batches.first().ok_or_else(|| {
        QueryError::engine(DataFusionError::Plan(
            "materialized view page returned no record batches".to_string(),
        ))
    })?;
    let schema = first.schema();
    for batch in batches.iter().skip(1) {
        if batch.schema() != schema {
            return Err(QueryError::engine(DataFusionError::Plan(
                "materialized view page returned batches with inconsistent schemas".to_string(),
            )));
        }
    }

    Ok(schema)
}

fn validate_logical_plan_scans_only_table(
    plan: &LogicalPlan,
    table_name: &str,
) -> Result<(), QueryError> {
    let mut scanned_tables = Vec::new();
    collect_logical_plan_table_scans(plan, &mut scanned_tables);
    if scanned_tables.is_empty() {
        return Err(QueryError::engine(DataFusionError::Plan(format!(
            "query must scan table `{table_name}`"
        ))));
    }
    for scanned_table in scanned_tables {
        if !scanned_table.eq_ignore_ascii_case(table_name) {
            return Err(QueryError::engine(DataFusionError::Plan(format!(
                "query scans table `{scanned_table}` but only `{table_name}` is allowed"
            ))));
        }
    }
    Ok(())
}

fn collect_logical_plan_table_scans<'a>(plan: &'a LogicalPlan, scanned_tables: &mut Vec<&'a str>) {
    if let LogicalPlan::TableScan(scan) = plan {
        scanned_tables.push(scan.table_name.table());
    }
    for input in plan.inputs() {
        collect_logical_plan_table_scans(input, scanned_tables);
    }
}

fn session_context(policy: QueryPolicy) -> Result<SessionContext, QueryError> {
    DataFusionSessionFactory::from_policy(policy).session_context()
}

fn memory_limit_usize(memory_limit_bytes: u64) -> Result<usize, QueryError> {
    usize::try_from(memory_limit_bytes).map_err(|_| {
        QueryError::engine(DataFusionError::Configuration(
            "memory_limit_bytes exceeds usize".into(),
        ))
    })
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

fn map_datafusion_error(error: DataFusionError) -> QueryError {
    QueryError::engine(error)
}
