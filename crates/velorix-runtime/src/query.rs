#![allow(clippy::too_many_arguments)]

use std::sync::Arc;

use arrow::{datatypes::Schema, record_batch::RecordBatch};
use datafusion::{
    common::ScalarValue,
    dataframe::DataFrame,
    datasource::MemTable,
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    logical_expr::LogicalPlan,
    object_store::path::Path as DataFusionPath,
    object_store::ObjectStore as DataFusionObjectStore,
    prelude::{ParquetReadOptions, SessionContext},
};
use futures::TryStreamExt;
use parquet::arrow::{
    arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    async_reader::ParquetObjectReader,
};
use parquet::errors::ParquetError;
use thiserror::Error;
use url::Url;
use velorix_core::query::{QueryError, QueryPolicy, QueryPolicyError, INPUT_TABLE_NAME};

use crate::benchmark_gate::ObjectRequestMetricsV1;
use crate::object_meter::{object_request_policy_error, MeteredObjectStore, ObjectStoreMeter};
use crate::query_runtime::{DataFusionSessionFactory, QueryRuntimeLimits};
pub use crate::query_runtime::{ProductionQueryRuntime, QueryExecutionLimiter};
use crate::recovery::RecoveryError;

#[derive(Debug, Error)]
pub enum RuntimeQueryError {
    #[error(transparent)]
    Recovery(#[from] RecoveryError),
    #[error(transparent)]
    Query(#[from] QueryError),
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryBindValue {
    Utf8(String),
    Int64(i64),
    Float64(f64),
    Boolean(bool),
}

pub async fn query_record_batches_table_with_bindings_and_policy_and_limiter(
    table_name: &str,
    batches: Vec<RecordBatch>,
    sql: &str,
    bind_values: &[QueryBindValue],
    policy: QueryPolicy,
    limiter: Option<QueryExecutionLimiter>,
) -> Result<Vec<RecordBatch>, RuntimeQueryError> {
    policy.validate().map_err(QueryError::from)?;
    validate_sql_text_policy(sql, policy).map_err(QueryError::from)?;

    let _permit = acquire_query_permit(policy, limiter.as_ref())?;
    let context = record_batches_context(table_name, batches, policy)?;
    let limits = QueryRuntimeLimits::from_policy(policy);
    let dataframe = limits
        .run_planning(async {
            let dataframe = context.sql(sql).await.map_err(QueryError::from)?;
            let dataframe = apply_bind_values(dataframe, bind_values)?;
            let plan = dataframe.clone().into_optimized_plan()?;
            validate_logical_plan_scans_only_table(&plan, table_name)?;
            Ok(dataframe)
        })
        .await?;

    collect_with_policy(dataframe, policy, limits)
        .await
        .map_err(Into::into)
}

pub async fn validate_record_batch_table_query_with_bindings_and_policy(
    table_name: &str,
    table_schema: Arc<Schema>,
    sql: &str,
    bind_values: &[QueryBindValue],
    policy: QueryPolicy,
) -> Result<(), RuntimeQueryError> {
    policy.validate().map_err(QueryError::from)?;
    validate_sql_text_policy(sql, policy).map_err(QueryError::from)?;

    let input = RecordBatch::new_empty(table_schema);
    let context = record_batches_context(table_name, vec![input], policy)?;
    let limits = QueryRuntimeLimits::from_policy(policy);
    limits
        .run_planning(async {
            let dataframe = context.sql(sql).await.map_err(QueryError::from)?;
            let plan = apply_bind_values(dataframe, bind_values)?.into_optimized_plan()?;
            validate_logical_plan_scans_only_table(&plan, table_name)?;
            Ok(())
        })
        .await
        .map_err(Into::into)
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
                .map(QueryBindValue::to_scalar_value)
                .collect::<Vec<_>>(),
        )
        .map_err(QueryError::from)
}

impl QueryBindValue {
    fn to_scalar_value(&self) -> ScalarValue {
        match self {
            Self::Utf8(value) => ScalarValue::Utf8(Some(value.clone())),
            Self::Int64(value) => ScalarValue::Int64(Some(*value)),
            Self::Float64(value) => ScalarValue::Float64(Some(*value)),
            Self::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        }
    }
}

pub async fn query_object_backed_input_with_policy(
    store: Arc<dyn DataFusionObjectStore>,
    table_url: &str,
    sql: &str,
    policy: QueryPolicy,
) -> Result<Vec<RecordBatch>, RuntimeQueryError> {
    query_object_backed_input_with_policy_and_limiter(store, table_url, sql, policy, None).await
}

pub(crate) async fn query_object_backed_relation_with_policy_and_limiter(
    store: Arc<dyn DataFusionObjectStore>,
    table_url: &str,
    table_name: &str,
    table_schema: Arc<Schema>,
    sql: &str,
    policy: QueryPolicy,
    limiter: Option<QueryExecutionLimiter>,
) -> Result<Vec<RecordBatch>, RuntimeQueryError> {
    query_object_backed_table_with_policy_and_limiter_and_meter(
        store,
        table_url,
        ObjectBackedTableRegistration {
            table_name,
            table_schema: Some(table_schema),
        },
        sql,
        policy,
        limiter,
        None,
    )
    .await
}

pub(crate) async fn query_object_backed_relation_with_policy_and_limiter_and_metrics(
    store: Arc<dyn DataFusionObjectStore>,
    table_url: &str,
    table_name: &str,
    table_schema: Arc<Schema>,
    sql: &str,
    policy: QueryPolicy,
    limiter: Option<QueryExecutionLimiter>,
) -> Result<ObjectBackedQueryResult, RuntimeQueryError> {
    let meter = ObjectStoreMeter::default();
    let batches = query_object_backed_table_with_policy_and_limiter_and_meter(
        store,
        table_url,
        ObjectBackedTableRegistration {
            table_name,
            table_schema: Some(table_schema),
        },
        sql,
        policy,
        limiter,
        Some(meter.clone()),
    )
    .await?;

    Ok(ObjectBackedQueryResult {
        batches,
        object_requests: meter.snapshot(),
    })
}

#[derive(Debug)]
pub struct ObjectBackedQueryResult {
    pub batches: Vec<RecordBatch>,
    pub object_requests: ObjectRequestMetricsV1,
}

pub async fn query_object_backed_input_with_policy_and_metrics(
    store: Arc<dyn DataFusionObjectStore>,
    table_url: &str,
    sql: &str,
    policy: QueryPolicy,
) -> Result<ObjectBackedQueryResult, RuntimeQueryError> {
    let meter = ObjectStoreMeter::default();
    let batches = query_object_backed_input_with_policy_and_limiter_and_meter(
        store,
        table_url,
        sql,
        policy,
        None,
        Some(meter.clone()),
    )
    .await?;

    Ok(ObjectBackedQueryResult {
        batches,
        object_requests: meter.snapshot(),
    })
}

pub async fn query_object_backed_input_with_policy_and_limiter(
    store: Arc<dyn DataFusionObjectStore>,
    table_url: &str,
    sql: &str,
    policy: QueryPolicy,
    limiter: Option<QueryExecutionLimiter>,
) -> Result<Vec<RecordBatch>, RuntimeQueryError> {
    query_object_backed_input_with_policy_and_limiter_and_meter(
        store, table_url, sql, policy, limiter, None,
    )
    .await
}

pub async fn query_object_backed_input_with_policy_and_runtime(
    store: Arc<dyn DataFusionObjectStore>,
    table_url: &str,
    sql: &str,
    policy: QueryPolicy,
    runtime: &ProductionQueryRuntime,
) -> Result<Vec<RecordBatch>, RuntimeQueryError> {
    let limiter = runtime.compatible_limiter(policy)?;
    query_object_backed_input_with_policy_and_limiter(store, table_url, sql, policy, limiter).await
}

async fn query_object_backed_input_with_policy_and_limiter_and_meter(
    store: Arc<dyn DataFusionObjectStore>,
    table_url: &str,
    sql: &str,
    policy: QueryPolicy,
    limiter: Option<QueryExecutionLimiter>,
    meter: Option<ObjectStoreMeter>,
) -> Result<Vec<RecordBatch>, RuntimeQueryError> {
    query_object_backed_table_with_policy_and_limiter_and_meter(
        store,
        table_url,
        ObjectBackedTableRegistration {
            table_name: INPUT_TABLE_NAME,
            table_schema: None,
        },
        sql,
        policy,
        limiter,
        meter,
    )
    .await
}

struct ObjectBackedTableRegistration<'a> {
    table_name: &'a str,
    table_schema: Option<Arc<Schema>>,
}

async fn query_object_backed_table_with_policy_and_limiter_and_meter(
    store: Arc<dyn DataFusionObjectStore>,
    table_url: &str,
    registration: ObjectBackedTableRegistration<'_>,
    sql: &str,
    policy: QueryPolicy,
    limiter: Option<QueryExecutionLimiter>,
    meter: Option<ObjectStoreMeter>,
) -> Result<Vec<RecordBatch>, RuntimeQueryError> {
    policy.validate().map_err(QueryError::from)?;
    validate_sql_text_policy(sql, policy).map_err(QueryError::from)?;
    let _permit = acquire_query_permit(policy, limiter.as_ref())?;
    let query_store = query_execution_store(store, policy, meter);
    let limits = QueryRuntimeLimits::from_policy(policy);
    limits
        .run_execution(async {
            validate_scan_policy(query_store.as_ref(), table_url, policy).await
        })
        .await?;
    if let Some(schema) = registration.table_schema.as_ref() {
        limits
            .run_execution(async {
                validate_parquet_table_schema(Arc::clone(&query_store), table_url, schema).await
            })
            .await?;
    }

    let context = session_context(policy)?;

    let object_store_url = object_store_url_for_table(table_url)?;
    context.register_object_store(object_store_url.as_ref(), query_store);
    let dataframe = limits
        .run_planning(async {
            let options = match registration.table_schema.as_deref() {
                Some(schema) => ParquetReadOptions::default().schema(schema),
                None => ParquetReadOptions::default(),
            };
            context
                .register_parquet(registration.table_name, table_url, options)
                .await
                .map_err(map_datafusion_error)?;
            context.sql(sql).await.map_err(map_datafusion_error)
        })
        .await?;

    collect_with_policy(dataframe, policy, limits)
        .await
        .map_err(Into::into)
}

fn query_execution_store(
    store: Arc<dyn DataFusionObjectStore>,
    policy: QueryPolicy,
    meter: Option<ObjectStoreMeter>,
) -> Arc<dyn DataFusionObjectStore> {
    match (policy.max_object_requests, meter) {
        (Some(max_requests), Some(meter)) => Arc::new(MeteredObjectStore::with_meter(
            store,
            meter,
            Some(max_requests),
        )),
        (Some(max_requests), None) => Arc::new(MeteredObjectStore::new(store, Some(max_requests))),
        (None, Some(meter)) => Arc::new(MeteredObjectStore::with_meter(store, meter, None)),
        (None, None) => store,
    }
}

fn acquire_query_permit(
    policy: QueryPolicy,
    limiter: Option<&QueryExecutionLimiter>,
) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, QueryError> {
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
    dataframe: datafusion::dataframe::DataFrame,
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
    dataframe: datafusion::dataframe::DataFrame,
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
    let table = MemTable::try_new(schema, vec![batches])?;
    let context = session_context(policy)?;
    context.register_table(table_name, Arc::new(table))?;

    Ok(context)
}

fn schema_for_record_batches(batches: &[RecordBatch]) -> Result<Arc<Schema>, QueryError> {
    let first = batches.first().ok_or_else(|| {
        QueryError::from(DataFusionError::Plan(
            "materialized view page returned no record batches".to_string(),
        ))
    })?;
    let schema = first.schema();
    for batch in batches.iter().skip(1) {
        if batch.schema() != schema {
            return Err(DataFusionError::Plan(
                "materialized view page returned batches with inconsistent schemas".to_string(),
            )
            .into());
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
        return Err(DataFusionError::Plan(format!("query must scan table `{table_name}`")).into());
    }
    for scanned_table in scanned_tables {
        if !scanned_table.eq_ignore_ascii_case(table_name) {
            return Err(DataFusionError::Plan(format!(
                "query scans table `{scanned_table}` but only `{table_name}` is allowed"
            ))
            .into());
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

async fn validate_scan_policy(
    store: &dyn DataFusionObjectStore,
    table_url: &str,
    policy: QueryPolicy,
) -> Result<(), QueryError> {
    if policy.max_scan_files.is_none()
        && policy.max_scan_bytes.is_none()
        && policy.max_object_requests.is_none()
    {
        return Ok(());
    }

    let prefix = object_path_for_table_url(table_url)?;
    let mut observed_files = 0usize;
    let mut observed_bytes = 0u64;

    let mut objects = store.list(Some(&prefix));

    while let Some(object) = objects.try_next().await.map_err(map_object_store_error)? {
        observed_files = observed_files.saturating_add(1);
        observed_bytes = observed_bytes.saturating_add(object.size);

        if let Some(max_files) = policy.max_scan_files {
            if observed_files > max_files {
                return Err(QueryPolicyError::ScanFilesExceeded {
                    observed_files,
                    max_files,
                }
                .into());
            }
        }
        if let Some(max_bytes) = policy.max_scan_bytes {
            if observed_bytes > max_bytes {
                return Err(QueryPolicyError::ScanBytesExceeded {
                    observed_bytes,
                    max_bytes,
                }
                .into());
            }
        }
    }

    Ok(())
}

async fn validate_parquet_table_schema(
    store: Arc<dyn DataFusionObjectStore>,
    table_url: &str,
    expected_schema: &Arc<Schema>,
) -> Result<(), QueryError> {
    let prefix = object_path_for_table_url(table_url)?;
    let mut objects = store.list(Some(&prefix));

    while let Some(object) = objects.try_next().await.map_err(map_object_store_error)? {
        if !is_parquet_data_file(&object.location) {
            continue;
        }

        let mut reader = ParquetObjectReader::new(Arc::clone(&store), object.location)
            .with_file_size(object.size);
        let metadata = ArrowReaderMetadata::load_async(&mut reader, ArrowReaderOptions::new())
            .await
            .map_err(map_parquet_error)?;

        if metadata.schema().fields() != expected_schema.fields() {
            return Err(DataFusionError::Plan(
                "parquet file schema does not match relation catalog schema".to_string(),
            )
            .into());
        }
    }

    Ok(())
}

fn map_parquet_error(error: ParquetError) -> QueryError {
    match query_policy_error_from_source(&error) {
        Some(error) => error.into(),
        None => DataFusionError::External(Box::new(error)).into(),
    }
}

fn is_parquet_data_file(path: &DataFusionPath) -> bool {
    path.as_ref().ends_with(".parquet")
}

fn map_object_store_error(error: datafusion::object_store::Error) -> QueryError {
    match query_policy_error_from_source(&error) {
        Some(error) => error.into(),
        None => DataFusionError::ObjectStore(Box::new(error)).into(),
    }
}

fn map_datafusion_error(error: DataFusionError) -> QueryError {
    match query_policy_error_from_source(&error) {
        Some(error) => error.into(),
        None => error.into(),
    }
}

fn query_policy_error_from_source(
    error: &(dyn std::error::Error + 'static),
) -> Option<QueryPolicyError> {
    if let Some(error) = error.downcast_ref::<datafusion::object_store::Error>() {
        if let Some(error) = object_request_policy_error(error) {
            return Some(error);
        }
    }

    if let Some(QueryPolicyError::ObjectRequestsExceeded {
        observed_requests,
        max_requests,
    }) = error.downcast_ref::<QueryPolicyError>()
    {
        return Some(QueryPolicyError::ObjectRequestsExceeded {
            observed_requests: *observed_requests,
            max_requests: *max_requests,
        });
    }

    error.source().and_then(query_policy_error_from_source)
}

fn object_store_url_for_table(table_url: &str) -> Result<ObjectStoreUrl, QueryError> {
    let mut url =
        Url::parse(table_url).map_err(|error| DataFusionError::External(Box::new(error)))?;
    url.set_path("/");
    url.set_query(None);
    url.set_fragment(None);

    Ok(ObjectStoreUrl::parse(url.as_str())?)
}

fn object_path_for_table_url(table_url: &str) -> Result<DataFusionPath, QueryError> {
    let url = Url::parse(table_url).map_err(|error| DataFusionError::External(Box::new(error)))?;
    let path = url.path().trim_start_matches('/').trim_end_matches('/');

    Ok(DataFusionPath::from(path))
}
