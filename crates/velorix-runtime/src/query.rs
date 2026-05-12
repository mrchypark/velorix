use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    datasource::MemTable,
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    object_store::path::Path as DataFusionPath,
    object_store::ObjectStore as DataFusionObjectStore,
    prelude::{ParquetReadOptions, SessionContext},
};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
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
pub use crate::query_runtime::QueryExecutionLimiter;
use crate::query_runtime::{DataFusionSessionFactory, QueryRuntimeLimits};
use crate::recovery::{RecoveredRuntime, RecoveryError, ORDERS_SUM_COUNT_OWNER};
use velorix_storage::capability::AuthoritativeObjectStoreCapabilitiesV1;

#[derive(Debug, Error)]
pub enum RuntimeQueryError {
    #[error(transparent)]
    Recovery(#[from] RecoveryError),
    #[error(transparent)]
    Query(#[from] QueryError),
}

pub async fn query_recovered_materialized_view(
    store: Arc<dyn ObjectStore>,
    sql: &str,
) -> Result<Vec<RecordBatch>, RuntimeQueryError> {
    query_recovered_materialized_view_with_policy(store, sql, QueryPolicy::default()).await
}

pub async fn query_recovered_materialized_view_with_policy(
    store: Arc<dyn ObjectStore>,
    sql: &str,
    policy: QueryPolicy,
) -> Result<Vec<RecordBatch>, RuntimeQueryError> {
    query_recovered_materialized_view_with_policy_and_limiter(store, sql, policy, None).await
}

pub async fn query_recovered_materialized_view_with_policy_and_limiter(
    store: Arc<dyn ObjectStore>,
    sql: &str,
    policy: QueryPolicy,
    limiter: Option<QueryExecutionLimiter>,
) -> Result<Vec<RecordBatch>, RuntimeQueryError> {
    policy.validate().map_err(QueryError::from)?;
    validate_sql_text_policy(sql, policy).map_err(QueryError::from)?;

    let _permit = acquire_query_permit(policy, limiter.as_ref())?;
    let recovered = RecoveredRuntime::recover(store).await?;

    collect_recovered_materialized_view(recovered, sql, policy)
        .await
        .map_err(Into::into)
}

pub async fn query_production_recovered_materialized_view_with_policy_and_limiter(
    store: Arc<dyn ObjectStore>,
    slatedb_state_path: impl Into<Path>,
    relation_id: &str,
    relation_version: &str,
    capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    sql: &str,
    policy: QueryPolicy,
    limiter: Option<QueryExecutionLimiter>,
) -> Result<Vec<RecordBatch>, RuntimeQueryError> {
    policy.validate().map_err(QueryError::from)?;
    validate_sql_text_policy(sql, policy).map_err(QueryError::from)?;

    let _permit = acquire_query_permit(policy, limiter.as_ref())?;
    let recovered = RecoveredRuntime::recover_with_slatedb_state_store_and_catalog_record_checked(
        store,
        slatedb_state_path,
        ORDERS_SUM_COUNT_OWNER,
        relation_id,
        relation_version,
        capabilities,
    )
    .await?;

    collect_recovered_materialized_view(recovered, sql, policy)
        .await
        .map_err(Into::into)
}

async fn collect_recovered_materialized_view(
    recovered: RecoveredRuntime,
    sql: &str,
    policy: QueryPolicy,
) -> Result<Vec<RecordBatch>, QueryError> {
    let materialized = recovered.materialized_state();
    let input = RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(
                materialized
                    .records()
                    .iter()
                    .map(|record| record.key.as_json().to_string())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                materialized
                    .records()
                    .iter()
                    .map(|record| record.value.as_json().to_string())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                materialized
                    .records()
                    .iter()
                    .map(|record| record.weight)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .map_err(QueryError::from)?;
    let context = input_context(vec![input], policy)?;
    let limits = QueryRuntimeLimits::from_policy(policy);
    let dataframe = limits
        .run_planning(async { context.sql(sql).await.map_err(QueryError::from) })
        .await?;

    collect_with_policy(dataframe, policy, limits)
        .await
        .map_err(Into::into)
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

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key_json", DataType::Utf8, false),
        Field::new("value_json", DataType::Utf8, false),
        Field::new("weight", DataType::Int64, false),
    ]))
}

fn input_context(
    input_batches: Vec<RecordBatch>,
    policy: QueryPolicy,
) -> Result<SessionContext, QueryError> {
    let table = MemTable::try_new(input_schema(), vec![input_batches])?;
    let context = session_context(policy)?;
    context.register_table(INPUT_TABLE_NAME, Arc::new(table))?;

    Ok(context)
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
