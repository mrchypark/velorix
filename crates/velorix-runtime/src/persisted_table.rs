use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::object_store::ObjectStore as DataFusionObjectStore;
use object_store::{path::Path, ObjectStore, PutMode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;
use velorix_core::query::QueryPolicy;
use velorix_storage::object_key::{ObjectKey, ObjectKeyError};

use crate::query::{query_object_backed_input_with_policy, RuntimeQueryError};

pub const PERSISTED_TABLE_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PersistedTableFormat {
    Parquet,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PersistedTableSpec {
    pub schema_version: u32,
    pub table_id: String,
    pub table_url: String,
    pub format: PersistedTableFormat,
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
    #[error("malformed table url: {0}")]
    MalformedTableUrl(url::ParseError),
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

fn validate_table_url(table_url: &str) -> Result<(), PersistedTableError> {
    Url::parse(table_url).map_err(PersistedTableError::MalformedTableUrl)?;

    Ok(())
}
