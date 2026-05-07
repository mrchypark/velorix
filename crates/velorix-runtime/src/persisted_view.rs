use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::object_store::ObjectStore as DataFusionObjectStore;
use object_store::ObjectStore;
use thiserror::Error;

use crate::{
    persisted_query::{PersistedQueryError, PersistedQueryStore},
    persisted_table::{
        query_production_persisted_object_backed_input_with_limiter, PersistedTableError,
        PersistedTableFormat, PersistedTableStore, ProductionPersistedTableQueryRequest,
    },
    query::{query_object_backed_input_with_policy, QueryExecutionLimiter, RuntimeQueryError},
    storage_registry::StorageRegistry,
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

#[derive(Clone, Debug)]
pub struct ProductionPersistedViewQueryRequest<'a> {
    pub registry: &'a StorageRegistry,
    pub tenant_id: &'a str,
    pub table_id: &'a str,
    pub query_id: &'a str,
    pub limiter: Option<QueryExecutionLimiter>,
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

pub async fn query_production_persisted_object_backed_view(
    catalog_store: Arc<dyn ObjectStore>,
    relation_catalog_store: Arc<dyn ObjectStore>,
    policy_catalog_store: Arc<dyn ObjectStore>,
    registry: &StorageRegistry,
    tenant_id: &str,
    table_id: &str,
    query_id: &str,
) -> Result<Vec<RecordBatch>, PersistedViewError> {
    query_production_persisted_object_backed_view_with_limiter(
        catalog_store,
        relation_catalog_store,
        policy_catalog_store,
        ProductionPersistedViewQueryRequest {
            registry,
            tenant_id,
            table_id,
            query_id,
            limiter: None,
        },
    )
    .await
}

pub async fn query_production_persisted_object_backed_view_with_limiter(
    catalog_store: Arc<dyn ObjectStore>,
    relation_catalog_store: Arc<dyn ObjectStore>,
    policy_catalog_store: Arc<dyn ObjectStore>,
    request: ProductionPersistedViewQueryRequest<'_>,
) -> Result<Vec<RecordBatch>, PersistedViewError> {
    let query_catalog = PersistedQueryStore::new(Arc::clone(&catalog_store));
    let query = query_catalog
        .get(request.query_id)
        .await
        .map_err(PersistedViewError::QueryCatalog)?;

    query_production_persisted_object_backed_input_with_limiter(
        catalog_store,
        relation_catalog_store,
        policy_catalog_store,
        ProductionPersistedTableQueryRequest {
            registry: request.registry,
            tenant_id: request.tenant_id,
            table_id: request.table_id,
            sql: &query.sql,
            limiter: request.limiter,
        },
    )
    .await
    .map_err(|error| match error {
        PersistedTableError::RuntimeQuery(error) => PersistedViewError::RuntimeQuery(error),
        error => PersistedViewError::TableCatalog(error),
    })
}
