use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutMode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use velorix_core::query::{validate_input_query_with_policy, QueryError, QueryPolicy};
use velorix_storage::object_key::{ObjectKey, ObjectKeyError};

use crate::query::{query_recovered_materialized_view_with_policy, RuntimeQueryError};

pub const PERSISTED_QUERY_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PersistedQuerySpec {
    pub schema_version: u32,
    pub query_id: String,
    pub sql: String,
    pub policy: QueryPolicy,
}

#[derive(Debug, Error)]
pub enum PersistedQueryError {
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Query(#[from] QueryError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    RuntimeQuery(#[from] RuntimeQueryError),
    #[error("unsupported persisted query schema version {schema_version}")]
    UnsupportedSchemaVersion { schema_version: u32 },
    #[error("persisted query id mismatch: expected {expected}, got {actual}")]
    QueryIdMismatch { expected: String, actual: String },
}

#[derive(Clone, Debug)]
pub struct PersistedQueryStore {
    store: Arc<dyn ObjectStore>,
}

impl PersistedQueryStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub async fn create(
        &self,
        query_id: &str,
        sql: &str,
        policy: QueryPolicy,
    ) -> Result<PersistedQuerySpec, PersistedQueryError> {
        let object_key = ObjectKey::persisted_query(query_id)?;
        validate_input_query_with_policy(sql, policy).await?;

        let spec = PersistedQuerySpec {
            schema_version: PERSISTED_QUERY_SCHEMA_VERSION,
            query_id: query_id.to_string(),
            sql: sql.to_string(),
            policy,
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

    pub async fn get(&self, query_id: &str) -> Result<PersistedQuerySpec, PersistedQueryError> {
        let object_key = ObjectKey::persisted_query(query_id)?;
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;

        let spec: PersistedQuerySpec = serde_json::from_slice(&bytes)?;
        if spec.schema_version != PERSISTED_QUERY_SCHEMA_VERSION {
            return Err(PersistedQueryError::UnsupportedSchemaVersion {
                schema_version: spec.schema_version,
            });
        }
        if spec.query_id != query_id {
            return Err(PersistedQueryError::QueryIdMismatch {
                expected: query_id.to_string(),
                actual: spec.query_id,
            });
        }

        Ok(spec)
    }
}

pub async fn query_persisted_recovered_materialized_view(
    store: Arc<dyn ObjectStore>,
    query_id: &str,
) -> Result<Vec<RecordBatch>, PersistedQueryError> {
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let spec = catalog.get(query_id).await?;

    Ok(query_recovered_materialized_view_with_policy(store, &spec.sql, spec.policy).await?)
}
