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
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
};
use futures::TryStreamExt;
use object_store::ObjectStore;
use thiserror::Error;
use url::Url;
use velorix_core::query::{QueryError, QueryPolicy, QueryPolicyError, INPUT_TABLE_NAME};

use crate::query_runtime::QueryRuntimeLimits;
use crate::recovery::{RecoveredRuntime, RecoveryError};

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
    policy.validate().map_err(QueryError::from)?;
    validate_sql_text_policy(sql, policy).map_err(QueryError::from)?;

    let recovered = RecoveredRuntime::recover(store).await?;
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
    policy.validate().map_err(QueryError::from)?;
    validate_sql_text_policy(sql, policy).map_err(QueryError::from)?;
    validate_scan_policy(store.as_ref(), table_url, policy).await?;

    let context = session_context(policy);

    let object_store_url = object_store_url_for_table(table_url)?;
    context.register_object_store(object_store_url.as_ref(), store);
    context
        .register_parquet(INPUT_TABLE_NAME, table_url, ParquetReadOptions::default())
        .await
        .map_err(QueryError::from)?;

    let limits = QueryRuntimeLimits::from_policy(policy);
    let dataframe = limits
        .run_planning(async { context.sql(sql).await.map_err(QueryError::from) })
        .await?;

    collect_with_policy(dataframe, policy, limits)
        .await
        .map_err(Into::into)
}

async fn collect_with_policy(
    dataframe: datafusion::dataframe::DataFrame,
    policy: QueryPolicy,
    limits: QueryRuntimeLimits,
) -> Result<Vec<RecordBatch>, QueryError> {
    if let Some(max_rows) = policy.max_output_rows {
        let fetch = max_rows.checked_add(1);
        let output = match fetch {
            Some(fetch) => {
                let limited = dataframe.limit(0, Some(fetch)).map_err(QueryError::from)?;
                limits
                    .run_execution(async { limited.collect().await.map_err(QueryError::from) })
                    .await?
            }
            None => {
                limits
                    .run_execution(async { dataframe.collect().await.map_err(QueryError::from) })
                    .await?
            }
        };
        let observed_rows = output.iter().map(RecordBatch::num_rows).sum();
        if observed_rows > max_rows {
            return Err(QueryPolicyError::OutputRowsExceeded {
                observed_rows,
                max_rows,
            }
            .into());
        }

        return Ok(output);
    }

    limits
        .run_execution(async { dataframe.collect().await.map_err(QueryError::from) })
        .await
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
    let context = session_context(policy);
    context.register_table(INPUT_TABLE_NAME, Arc::new(table))?;

    Ok(context)
}

fn session_context(policy: QueryPolicy) -> SessionContext {
    let mut config = SessionConfig::new();
    if let Some(batch_size) = policy.batch_size {
        config = config.with_batch_size(batch_size.get());
    }
    if let Some(target_partitions) = policy.target_partitions {
        config = config.with_target_partitions(target_partitions.get());
    }

    SessionContext::new_with_config(config)
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
    let mut observed_requests = 1usize;
    if let Some(max_requests) = policy.max_object_requests {
        if observed_requests > max_requests {
            return Err(QueryPolicyError::ObjectRequestsExceeded {
                observed_requests,
                max_requests,
            }
            .into());
        }
    }

    let mut objects = store.list(Some(&prefix));

    while let Some(object) = objects
        .try_next()
        .await
        .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?
    {
        observed_files = observed_files.saturating_add(1);
        observed_bytes = observed_bytes.saturating_add(object.size);
        observed_requests = observed_requests.saturating_add(1);

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
        if let Some(max_requests) = policy.max_object_requests {
            if observed_requests > max_requests {
                return Err(QueryPolicyError::ObjectRequestsExceeded {
                    observed_requests,
                    max_requests,
                }
                .into());
            }
        }
    }

    Ok(())
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
