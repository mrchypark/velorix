use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::object_store::ObjectStore as DataFusionObjectStore;
use object_store::{path::Path, ObjectStore, PutMode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;
use velorix_core::{query::QueryPolicy, relation::validate_schema_fingerprint};
use velorix_storage::object_key::{ObjectKey, ObjectKeyError};

use crate::query::{query_object_backed_input_with_policy, RuntimeQueryError};
use crate::query_policy_catalog::{QueryPolicyCatalogError, QueryPolicyCatalogStore};
use crate::storage_registry::{validate_tenant_prefix, StorageRegistry, StorageRegistryError};

pub const PERSISTED_TABLE_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PersistedTableFormat {
    Parquet,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PersistedTableSpec {
    pub schema_version: u32,
    pub table_id: String,
    pub table_url: String,
    pub format: PersistedTableFormat,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ProductionPersistedTableFormat {
    #[serde(rename = "parquet")]
    Parquet,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProductionPersistedTableSpec {
    pub schema_version: u32,
    pub table_id: String,
    pub tenant_id: String,
    pub store_id: String,
    pub object_key_prefix: String,
    pub snapshot_ref: String,
    pub format: ProductionPersistedTableFormat,
    pub schema_fingerprint: String,
    pub query_policy_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateProductionPersistedTableSpecRequest {
    pub table_id: String,
    pub tenant_id: String,
    pub store_id: String,
    pub object_key_prefix: String,
    pub snapshot_ref: String,
    pub format: ProductionPersistedTableFormat,
    pub schema_fingerprint: String,
    pub query_policy_id: String,
}

#[derive(Debug, Error)]
pub enum PersistedTableError {
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    RuntimeQuery(#[from] RuntimeQueryError),
    #[error(transparent)]
    QueryPolicyCatalog(#[from] QueryPolicyCatalogError),
    #[error(transparent)]
    StorageRegistry(#[from] StorageRegistryError),
    #[error("malformed table url: {0}")]
    MalformedTableUrl(url::ParseError),
    #[error("raw URL table spec is not allowed for production table scans")]
    RawUrlProductionSpec,
    #[error("cross-tenant object key prefix for tenant {tenant_id}: {object_key_prefix}")]
    CrossTenantPrefix {
        tenant_id: String,
        object_key_prefix: String,
    },
    #[error("invalid production persisted table field {field}")]
    InvalidProductionField { field: &'static str },
    #[error("unsupported persisted table schema version {schema_version}")]
    UnsupportedSchemaVersion { schema_version: u32 },
    #[error("persisted table id mismatch: expected {expected}, got {actual}")]
    TableIdMismatch { expected: String, actual: String },
}

#[derive(Clone, Debug)]
pub struct PersistedTableStore {
    store: Arc<dyn ObjectStore>,
}

impl PersistedTableStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub async fn create(
        &self,
        table_id: &str,
        table_url: &str,
        format: PersistedTableFormat,
    ) -> Result<PersistedTableSpec, PersistedTableError> {
        let object_key = ObjectKey::query_table(table_id)?;
        validate_table_url(table_url)?;

        let spec = PersistedTableSpec {
            schema_version: PERSISTED_TABLE_SCHEMA_VERSION,
            table_id: table_id.to_string(),
            table_url: table_url.to_string(),
            format,
        };
        let bytes = serde_json::to_vec(&spec)?;
        self.store
            .put_opts(
                &Path::from(object_key.as_str()),
                Bytes::from(bytes).into(),
                PutMode::Create.into(),
            )
            .await?;

        Ok(spec)
    }

    pub async fn get(&self, table_id: &str) -> Result<PersistedTableSpec, PersistedTableError> {
        let object_key = ObjectKey::query_table(table_id)?;
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;

        let spec: PersistedTableSpec = serde_json::from_slice(&bytes)?;
        if spec.schema_version != PERSISTED_TABLE_SCHEMA_VERSION {
            return Err(PersistedTableError::UnsupportedSchemaVersion {
                schema_version: spec.schema_version,
            });
        }
        if spec.table_id != table_id {
            return Err(PersistedTableError::TableIdMismatch {
                expected: table_id.to_string(),
                actual: spec.table_id,
            });
        }
        validate_table_url(&spec.table_url)?;

        Ok(spec)
    }

    pub async fn create_production(
        &self,
        request: CreateProductionPersistedTableSpecRequest,
    ) -> Result<ProductionPersistedTableSpec, PersistedTableError> {
        let object_key = ObjectKey::query_table(&request.table_id)?;
        validate_production_table_fields(
            &request.table_id,
            &request.tenant_id,
            &request.store_id,
            &request.object_key_prefix,
            &request.snapshot_ref,
            &request.schema_fingerprint,
            &request.query_policy_id,
        )?;

        let spec = ProductionPersistedTableSpec {
            schema_version: PERSISTED_TABLE_SCHEMA_VERSION,
            table_id: request.table_id,
            tenant_id: request.tenant_id,
            store_id: request.store_id,
            object_key_prefix: request.object_key_prefix,
            snapshot_ref: request.snapshot_ref,
            format: request.format,
            schema_fingerprint: request.schema_fingerprint,
            query_policy_id: request.query_policy_id,
        };
        let bytes = serde_json::to_vec(&spec)?;
        self.store
            .put_opts(
                &Path::from(object_key.as_str()),
                Bytes::from(bytes).into(),
                PutMode::Create.into(),
            )
            .await?;

        Ok(spec)
    }

    pub async fn get_production(
        &self,
        table_id: &str,
    ) -> Result<ProductionPersistedTableSpec, PersistedTableError> {
        let object_key = ObjectKey::query_table(table_id)?;
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;

        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        if value.get("table_url").is_some() {
            return Err(PersistedTableError::RawUrlProductionSpec);
        }

        let spec: ProductionPersistedTableSpec = serde_json::from_value(value)?;
        if spec.schema_version != PERSISTED_TABLE_SCHEMA_VERSION {
            return Err(PersistedTableError::UnsupportedSchemaVersion {
                schema_version: spec.schema_version,
            });
        }
        if spec.table_id != table_id {
            return Err(PersistedTableError::TableIdMismatch {
                expected: table_id.to_string(),
                actual: spec.table_id,
            });
        }
        validate_production_table_fields(
            &spec.table_id,
            &spec.tenant_id,
            &spec.store_id,
            &spec.object_key_prefix,
            &spec.snapshot_ref,
            &spec.schema_fingerprint,
            &spec.query_policy_id,
        )?;

        Ok(spec)
    }
}

pub async fn query_persisted_object_backed_input_with_policy(
    catalog_store: Arc<dyn ObjectStore>,
    scan_store: Arc<dyn DataFusionObjectStore>,
    table_id: &str,
    sql: &str,
    policy: QueryPolicy,
) -> Result<Vec<RecordBatch>, PersistedTableError> {
    let catalog = PersistedTableStore::new(catalog_store);
    let spec = catalog.get(table_id).await?;

    match spec.format {
        PersistedTableFormat::Parquet => {
            Ok(
                query_object_backed_input_with_policy(scan_store, &spec.table_url, sql, policy)
                    .await?,
            )
        }
    }
}

pub async fn query_production_persisted_object_backed_input(
    catalog_store: Arc<dyn ObjectStore>,
    policy_catalog_store: Arc<dyn ObjectStore>,
    registry: &StorageRegistry,
    tenant_id: &str,
    table_id: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, PersistedTableError> {
    let catalog = PersistedTableStore::new(catalog_store);
    let spec = catalog.get_production(table_id).await?;
    reject_cross_tenant_production_query(tenant_id, &spec)?;
    let policy = QueryPolicyCatalogStore::new(policy_catalog_store)
        .get(&spec.tenant_id, &spec.query_policy_id)
        .await?
        .policy;

    query_production_spec_with_policy(registry, tenant_id, spec, sql, policy).await
}

async fn query_production_spec_with_policy(
    registry: &StorageRegistry,
    tenant_id: &str,
    spec: ProductionPersistedTableSpec,
    sql: &str,
    policy: QueryPolicy,
) -> Result<Vec<RecordBatch>, PersistedTableError> {
    reject_cross_tenant_production_query(tenant_id, &spec)?;

    let location = registry.resolve_production_table_location(
        &spec.store_id,
        &spec.tenant_id,
        &spec.object_key_prefix,
        &spec.snapshot_ref,
    )?;

    match spec.format {
        ProductionPersistedTableFormat::Parquet => Ok(query_object_backed_input_with_policy(
            location.store,
            &location.table_url,
            sql,
            policy,
        )
        .await?),
    }
}

fn reject_cross_tenant_production_query(
    tenant_id: &str,
    spec: &ProductionPersistedTableSpec,
) -> Result<(), PersistedTableError> {
    if spec.tenant_id != tenant_id {
        return Err(PersistedTableError::CrossTenantPrefix {
            tenant_id: tenant_id.to_string(),
            object_key_prefix: spec.object_key_prefix.clone(),
        });
    }

    Ok(())
}

fn validate_table_url(table_url: &str) -> Result<(), PersistedTableError> {
    Url::parse(table_url).map_err(PersistedTableError::MalformedTableUrl)?;

    Ok(())
}

fn validate_production_table_fields(
    table_id: &str,
    tenant_id: &str,
    store_id: &str,
    object_key_prefix: &str,
    snapshot_ref: &str,
    schema_fingerprint: &str,
    query_policy_id: &str,
) -> Result<(), PersistedTableError> {
    require_non_empty("table_id", table_id)?;
    require_non_empty("store_id", store_id)?;
    require_non_empty("snapshot_ref", snapshot_ref)?;
    ObjectKey::query_policy(tenant_id, query_policy_id).map_err(|_| {
        PersistedTableError::InvalidProductionField {
            field: "query_policy_id",
        }
    })?;
    validate_schema_fingerprint("schema_fingerprint", schema_fingerprint).map_err(|_| {
        PersistedTableError::InvalidProductionField {
            field: "schema_fingerprint",
        }
    })?;
    validate_tenant_prefix(tenant_id, object_key_prefix).map_err(|error| match error {
        StorageRegistryError::CrossTenantPrefix {
            tenant_id,
            object_key_prefix,
        } => PersistedTableError::CrossTenantPrefix {
            tenant_id,
            object_key_prefix,
        },
        _ => PersistedTableError::InvalidProductionField {
            field: "object_key_prefix",
        },
    })?;

    Ok(())
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), PersistedTableError> {
    if value.is_empty() {
        return Err(PersistedTableError::InvalidProductionField { field });
    }

    Ok(())
}
