use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::object_store::ObjectStore as DataFusionObjectStore;
use object_store::{path::Path, ObjectStore, PutMode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;
use velorix_core::{
    query::QueryPolicy,
    relation::{
        datafusion_schema_from_catalog, validate_schema_fingerprint, DataFusionRegistrationModeV1,
        RelationSchemaError, VelorixRelationCatalogV1,
    },
};
use velorix_storage::{
    object_key::{ObjectKey, ObjectKeyError},
    relation_catalog_registry::{RelationCatalogRegistry, RelationCatalogRegistryError},
};

use crate::query::{
    query_object_backed_input_with_policy, query_object_backed_relation_with_policy,
    RuntimeQueryError,
};
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
    pub relation_id: String,
    pub relation_version: String,
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
    pub relation_id: String,
    pub relation_version: String,
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
    RelationSchema(#[from] RelationSchemaError),
    #[error(transparent)]
    RelationCatalogRegistry(#[from] RelationCatalogRegistryError),
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
    #[error(
        "relation catalog fingerprint mismatch for {relation_id}/{relation_version}: expected {expected}, got {actual}"
    )]
    RelationCatalogFingerprintMismatch {
        relation_id: String,
        relation_version: String,
        expected: String,
        actual: String,
    },
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
        relation_catalog_store: Arc<dyn ObjectStore>,
        query_policy_catalog_store: Arc<dyn ObjectStore>,
        request: CreateProductionPersistedTableSpecRequest,
    ) -> Result<ProductionPersistedTableSpec, PersistedTableError> {
        let spec = ProductionPersistedTableSpec {
            schema_version: PERSISTED_TABLE_SCHEMA_VERSION,
            table_id: request.table_id,
            tenant_id: request.tenant_id,
            store_id: request.store_id,
            object_key_prefix: request.object_key_prefix,
            snapshot_ref: request.snapshot_ref,
            format: request.format,
            relation_id: request.relation_id,
            relation_version: request.relation_version,
            schema_fingerprint: request.schema_fingerprint,
            query_policy_id: request.query_policy_id,
        };
        let object_key = ObjectKey::query_table(&spec.table_id)?;
        validate_production_table_fields(&spec)?;
        let relation_catalog = read_matching_relation_catalog(
            relation_catalog_store,
            &spec.relation_id,
            &spec.relation_version,
            &spec.schema_fingerprint,
        )
        .await?;
        require_table_relation_registration(&relation_catalog)?;
        QueryPolicyCatalogStore::new(query_policy_catalog_store)
            .get_for_production_table_scan(&spec.tenant_id, &spec.query_policy_id)
            .await?;

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
        validate_production_table_fields(&spec)?;

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
    relation_catalog_store: Arc<dyn ObjectStore>,
    policy_catalog_store: Arc<dyn ObjectStore>,
    registry: &StorageRegistry,
    tenant_id: &str,
    table_id: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>, PersistedTableError> {
    let catalog = PersistedTableStore::new(catalog_store);
    let spec = catalog.get_production(table_id).await?;
    reject_cross_tenant_production_query(tenant_id, &spec)?;
    let relation_catalog = read_matching_relation_catalog(
        relation_catalog_store,
        &spec.relation_id,
        &spec.relation_version,
        &spec.schema_fingerprint,
    )
    .await?;
    let policy = QueryPolicyCatalogStore::new(policy_catalog_store)
        .get_for_production_table_scan(&spec.tenant_id, &spec.query_policy_id)
        .await?
        .policy;

    query_production_spec_with_policy(registry, tenant_id, spec, relation_catalog, sql, policy)
        .await
}

async fn query_production_spec_with_policy(
    registry: &StorageRegistry,
    tenant_id: &str,
    spec: ProductionPersistedTableSpec,
    relation_catalog: VelorixRelationCatalogV1,
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
        ProductionPersistedTableFormat::Parquet => {
            query_relation_backed_parquet_with_policy(
                location.store,
                &location.table_url,
                &relation_catalog,
                sql,
                policy,
            )
            .await
        }
    }
}

async fn query_relation_backed_parquet_with_policy(
    store: Arc<dyn DataFusionObjectStore>,
    table_url: &str,
    relation_catalog: &VelorixRelationCatalogV1,
    sql: &str,
    policy: QueryPolicy,
) -> Result<Vec<RecordBatch>, PersistedTableError> {
    require_table_relation_registration(relation_catalog)?;
    let catalog_schema = datafusion_schema_from_catalog(relation_catalog)?;
    Ok(query_object_backed_relation_with_policy(
        store,
        table_url,
        relation_catalog.datafusion_registration.name.as_str(),
        catalog_schema,
        sql,
        policy,
    )
    .await?)
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
    spec: &ProductionPersistedTableSpec,
) -> Result<(), PersistedTableError> {
    require_non_empty("table_id", &spec.table_id)?;
    require_non_empty("store_id", &spec.store_id)?;
    require_non_empty("snapshot_ref", &spec.snapshot_ref)?;
    ObjectKey::relation_catalog(&spec.relation_id, &spec.relation_version).map_err(|_| {
        PersistedTableError::InvalidProductionField {
            field: "relation_catalog",
        }
    })?;
    ObjectKey::query_policy(&spec.tenant_id, &spec.query_policy_id).map_err(|_| {
        PersistedTableError::InvalidProductionField {
            field: "query_policy_id",
        }
    })?;
    validate_schema_fingerprint("schema_fingerprint", &spec.schema_fingerprint).map_err(|_| {
        PersistedTableError::InvalidProductionField {
            field: "schema_fingerprint",
        }
    })?;
    validate_tenant_prefix(&spec.tenant_id, &spec.object_key_prefix).map_err(
        |error| match error {
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
        },
    )?;

    Ok(())
}

async fn read_matching_relation_catalog(
    relation_catalog_store: Arc<dyn ObjectStore>,
    relation_id: &str,
    relation_version: &str,
    schema_fingerprint: &str,
) -> Result<VelorixRelationCatalogV1, PersistedTableError> {
    let relation_catalog = RelationCatalogRegistry::new(relation_catalog_store)
        .read(relation_id, relation_version)
        .await?;
    let catalog_fingerprint = relation_catalog.schema_fingerprint.as_str();
    if catalog_fingerprint != schema_fingerprint {
        return Err(PersistedTableError::RelationCatalogFingerprintMismatch {
            relation_id: relation_id.to_string(),
            relation_version: relation_version.to_string(),
            expected: catalog_fingerprint.to_string(),
            actual: schema_fingerprint.to_string(),
        });
    }

    Ok(relation_catalog)
}

fn require_table_relation_registration(
    relation_catalog: &VelorixRelationCatalogV1,
) -> Result<(), PersistedTableError> {
    if relation_catalog.datafusion_registration.mode == DataFusionRegistrationModeV1::Table {
        Ok(())
    } else {
        Err(RelationSchemaError::InvalidRelationSchema {
            field: "datafusion_registration.mode",
        }
        .into())
    }
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), PersistedTableError> {
    if value.is_empty() {
        return Err(PersistedTableError::InvalidProductionField { field });
    }

    Ok(())
}
