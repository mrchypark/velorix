use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use thiserror::Error;
use velorix_core::query::{query_delta_batch, QueryError};

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
    let recovered = RecoveredRuntime::recover(store).await?;
    let materialized = recovered.materialized_state();

    Ok(query_delta_batch(&materialized, sql).await?)
}
