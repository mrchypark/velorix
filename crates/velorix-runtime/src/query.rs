use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::{
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
use velorix_core::query::{
    query_delta_batch_with_policy, QueryError, QueryPolicy, QueryPolicyError, INPUT_TABLE_NAME,
};

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
    let recovered = RecoveredRuntime::recover(store).await?;
    let materialized = recovered.materialized_state();

    Ok(query_delta_batch_with_policy(&materialized, sql, policy).await?)
}

pub async fn query_object_backed_input_with_policy(
    store: Arc<dyn DataFusionObjectStore>,
    table_url: &str,
    sql: &str,
    policy: QueryPolicy,
) -> Result<Vec<RecordBatch>, RuntimeQueryError> {
    validate_sql_text_policy(sql, policy).map_err(QueryError::from)?;
    validate_scan_policy(store.as_ref(), table_url, policy).await?;

    let mut config = SessionConfig::new();
    if let Some(batch_size) = policy.batch_size {
        config = config.with_batch_size(batch_size.get());
    }
    if let Some(target_partitions) = policy.target_partitions {
        config = config.with_target_partitions(target_partitions.get());
    }
    let context = SessionContext::new_with_config(config);

    let object_store_url = object_store_url_for_table(table_url)?;
    context.register_object_store(object_store_url.as_ref(), store);
    context
        .register_parquet(INPUT_TABLE_NAME, table_url, ParquetReadOptions::default())
        .await
        .map_err(QueryError::from)?;

    let dataframe = context.sql(sql).await.map_err(QueryError::from)?;
    if let Some(max_rows) = policy.max_output_rows {
        let fetch = max_rows.checked_add(1);
        let output = match fetch {
            Some(fetch) => dataframe
                .limit(0, Some(fetch))
                .map_err(QueryError::from)?
                .collect()
                .await
                .map_err(QueryError::from)?,
            None => dataframe.collect().await.map_err(QueryError::from)?,
        };
        let observed_rows = output.iter().map(RecordBatch::num_rows).sum();
        if observed_rows > max_rows {
            return Err(QueryError::from(QueryPolicyError::OutputRowsExceeded {
                observed_rows,
                max_rows,
            })
            .into());
        }

        return Ok(output);
    }

    Ok(dataframe.collect().await.map_err(QueryError::from)?)
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
