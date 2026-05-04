use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::object_store::ObjectStore as DataFusionObjectStore;
use object_store::ObjectStore;
use thiserror::Error;

use crate::{
    persisted_query::{PersistedQueryError, PersistedQueryStore},
    persisted_table::{PersistedTableError, PersistedTableFormat, PersistedTableStore},
    query::{query_object_backed_input_with_policy, RuntimeQueryError},
};

#[derive(Debug, Error)]
pub enum PersistedViewError {
    #[error("query catalog error: {0}")]
    QueryCatalog(#[source] PersistedQueryError),
    #[error("table catalog error: {0}")]
    TableCatalog(#[source] PersistedTableError),
    #[error("runtime query error: {0}")]
    RuntimeQuery(#[source] RuntimeQueryError),
}

pub async fn query_persisted_object_backed_view(
    catalog_store: Arc<dyn ObjectStore>,
    scan_store: Arc<dyn DataFusionObjectStore>,
    table_id: &str,
    query_id: &str,
) -> Result<Vec<RecordBatch>, PersistedViewError> {
    let query_catalog = PersistedQueryStore::new(Arc::clone(&catalog_store));
    let table_catalog = PersistedTableStore::new(catalog_store);

    let query = query_catalog
        .get(query_id)
        .await
        .map_err(PersistedViewError::QueryCatalog)?;
    let table = table_catalog
        .get(table_id)
        .await
        .map_err(PersistedViewError::TableCatalog)?;

    match table.format {
        PersistedTableFormat::Parquet => query_object_backed_input_with_policy(
            scan_store,
            &table.table_url,
            &query.sql,
            query.policy,
        )
        .await
        .map_err(PersistedViewError::RuntimeQuery),
    }
}
