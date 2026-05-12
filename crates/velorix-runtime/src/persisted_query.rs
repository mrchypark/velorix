use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutMode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use velorix_core::{
    query::{
        validate_input_query_with_policy, validate_table_query_with_policy, QueryError, QueryPolicy,
    },
    relation::{
        datafusion_schema_from_catalog, DataFusionRegistrationModeV1, RelationSchemaError,
        VelorixRelationCatalogV1,
    },
};
use velorix_storage::object_key::{ObjectKey, ObjectKeyError};

use crate::query::{
    query_production_recovered_materialized_view_with_policy_and_limiter,
    query_recovered_materialized_view_with_policy_and_limiter, QueryExecutionLimiter,
    RuntimeQueryError,
};
use velorix_storage::capability::AuthoritativeObjectStoreCapabilitiesV1;

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
    #[error(transparent)]
    RelationSchema(#[from] RelationSchemaError),
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
        validate_input_query_with_policy(sql, policy).await?;
        self.write_create(query_id, sql, policy).await
    }

    pub async fn create_for_production_relation(
        &self,
        query_id: &str,
        sql: &str,
        policy: QueryPolicy,
        relation_catalog: &VelorixRelationCatalogV1,
    ) -> Result<PersistedQuerySpec, PersistedQueryError> {
        validate_production_relation_query(sql, policy, relation_catalog).await?;
        self.write_create(query_id, sql, policy).await
    }

    async fn write_create(
        &self,
        query_id: &str,
        sql: &str,
        policy: QueryPolicy,
    ) -> Result<PersistedQuerySpec, PersistedQueryError> {
        let object_key = ObjectKey::persisted_query(query_id)?;
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

    pub async fn get_for_production_relation(
        &self,
        query_id: &str,
        relation_catalog: &VelorixRelationCatalogV1,
    ) -> Result<PersistedQuerySpec, PersistedQueryError> {
        let spec = self.get(query_id).await?;
        validate_production_relation_query(&spec.sql, spec.policy, relation_catalog).await?;

        Ok(spec)
    }

    pub async fn get_for_production_relation_with_policy(
        &self,
        query_id: &str,
        relation_catalog: &VelorixRelationCatalogV1,
        production_policy: QueryPolicy,
    ) -> Result<PersistedQuerySpec, PersistedQueryError> {
        let spec = self.get(query_id).await?;
        validate_production_relation_query(&spec.sql, production_policy, relation_catalog).await?;

        Ok(spec)
    }
}

async fn validate_production_relation_query(
    sql: &str,
    policy: QueryPolicy,
    relation_catalog: &VelorixRelationCatalogV1,
) -> Result<(), PersistedQueryError> {
    if relation_catalog.datafusion_registration.mode != DataFusionRegistrationModeV1::Table {
        return Err(RelationSchemaError::InvalidRelationSchema {
            field: "datafusion_registration.mode",
        }
        .into());
    }

    let table_schema = datafusion_schema_from_catalog(relation_catalog)?;
    validate_table_query_with_policy(
        sql,
        relation_catalog.datafusion_registration.name.as_str(),
        table_schema,
        policy,
    )
    .await?;

    Ok(())
}

pub async fn query_persisted_recovered_materialized_view(
    store: Arc<dyn ObjectStore>,
    query_id: &str,
) -> Result<Vec<RecordBatch>, PersistedQueryError> {
    query_persisted_recovered_materialized_view_with_limiter(store, query_id, None).await
}

pub async fn query_persisted_recovered_materialized_view_with_limiter(
    store: Arc<dyn ObjectStore>,
    query_id: &str,
    limiter: Option<QueryExecutionLimiter>,
) -> Result<Vec<RecordBatch>, PersistedQueryError> {
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let spec = catalog.get(query_id).await?;

    Ok(query_recovered_materialized_view_with_policy_and_limiter(
        store,
        &spec.sql,
        spec.policy,
        limiter,
    )
    .await?)
}

pub async fn query_production_persisted_recovered_materialized_view(
    store: Arc<dyn ObjectStore>,
    query_id: &str,
    slatedb_state_path: impl Into<Path>,
    relation_id: &str,
    relation_version: &str,
    capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
) -> Result<Vec<RecordBatch>, PersistedQueryError> {
    query_production_persisted_recovered_materialized_view_with_limiter(
        store,
        query_id,
        slatedb_state_path,
        relation_id,
        relation_version,
        capabilities,
        None,
    )
    .await
}

pub async fn query_production_persisted_recovered_materialized_view_with_limiter(
    store: Arc<dyn ObjectStore>,
    query_id: &str,
    slatedb_state_path: impl Into<Path>,
    relation_id: &str,
    relation_version: &str,
    capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    limiter: Option<QueryExecutionLimiter>,
) -> Result<Vec<RecordBatch>, PersistedQueryError> {
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let spec = catalog.get(query_id).await?;

    Ok(
        query_production_recovered_materialized_view_with_policy_and_limiter(
            store,
            slatedb_state_path,
            relation_id,
            relation_version,
            capabilities,
            &spec.sql,
            spec.policy,
            limiter,
        )
        .await?,
    )
}
