use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutMode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use velorix_core::query::{QueryExecutionPolicyV1, QueryPolicyError};
use velorix_storage::object_key::{ObjectKey, ObjectKeyError};

pub const QUERY_POLICY_CATALOG_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryPolicyCatalogRecord {
    pub schema_version: u32,
    pub tenant_id: String,
    pub query_policy_id: String,
    pub policy: QueryExecutionPolicyV1,
}

#[derive(Debug, Error)]
pub enum QueryPolicyCatalogError {
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Policy(#[from] QueryPolicyError),
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    #[error("unsupported query policy catalog schema version {schema_version}")]
    UnsupportedSchemaVersion { schema_version: u32 },
    #[error("query policy tenant id mismatch: expected {expected}, got {actual}")]
    TenantIdMismatch { expected: String, actual: String },
    #[error("query policy id mismatch: expected {expected}, got {actual}")]
    QueryPolicyIdMismatch { expected: String, actual: String },
}

#[derive(Clone, Debug)]
pub struct QueryPolicyCatalogStore {
    store: Arc<dyn ObjectStore>,
}

impl QueryPolicyCatalogStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub async fn create(
        &self,
        tenant_id: &str,
        query_policy_id: &str,
        policy: QueryExecutionPolicyV1,
    ) -> Result<QueryPolicyCatalogRecord, QueryPolicyCatalogError> {
        policy.validate()?;
        let object_key = policy_object_key(tenant_id, query_policy_id)?;
        let record = QueryPolicyCatalogRecord {
            schema_version: QUERY_POLICY_CATALOG_SCHEMA_VERSION,
            tenant_id: tenant_id.to_string(),
            query_policy_id: query_policy_id.to_string(),
            policy,
        };

        self.store
            .put_opts(
                &Path::from(object_key.as_str()),
                Bytes::from(serde_json::to_vec(&record)?).into(),
                PutMode::Create.into(),
            )
            .await?;

        Ok(record)
    }

    pub async fn create_for_production_table_scan(
        &self,
        tenant_id: &str,
        query_policy_id: &str,
        policy: QueryExecutionPolicyV1,
    ) -> Result<QueryPolicyCatalogRecord, QueryPolicyCatalogError> {
        policy.validate_production_table_scan()?;

        self.create(tenant_id, query_policy_id, policy).await
    }

    pub async fn get(
        &self,
        tenant_id: &str,
        query_policy_id: &str,
    ) -> Result<QueryPolicyCatalogRecord, QueryPolicyCatalogError> {
        let object_key = policy_object_key(tenant_id, query_policy_id)?;
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;

        let record: QueryPolicyCatalogRecord = serde_json::from_slice(&bytes)?;
        if record.schema_version != QUERY_POLICY_CATALOG_SCHEMA_VERSION {
            return Err(QueryPolicyCatalogError::UnsupportedSchemaVersion {
                schema_version: record.schema_version,
            });
        }
        if record.tenant_id != tenant_id {
            return Err(QueryPolicyCatalogError::TenantIdMismatch {
                expected: tenant_id.to_string(),
                actual: record.tenant_id,
            });
        }
        if record.query_policy_id != query_policy_id {
            return Err(QueryPolicyCatalogError::QueryPolicyIdMismatch {
                expected: query_policy_id.to_string(),
                actual: record.query_policy_id,
            });
        }
        record.policy.validate()?;

        Ok(record)
    }

    pub async fn get_for_production_table_scan(
        &self,
        tenant_id: &str,
        query_policy_id: &str,
    ) -> Result<QueryPolicyCatalogRecord, QueryPolicyCatalogError> {
        let record = self.get(tenant_id, query_policy_id).await?;
        record.policy.validate_production_table_scan()?;

        Ok(record)
    }
}

fn policy_object_key(
    tenant_id: &str,
    query_policy_id: &str,
) -> Result<ObjectKey, QueryPolicyCatalogError> {
    Ok(ObjectKey::query_policy(tenant_id, query_policy_id)?)
}
