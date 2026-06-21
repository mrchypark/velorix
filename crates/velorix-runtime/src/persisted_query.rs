use std::sync::Arc;

use crate::query::{validate_input_query_with_policy, validate_table_query_with_policy};
use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutMode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use velorix_core::{
    query::{QueryError, QueryPolicy},
    relation::{
        datafusion_schema_from_catalog, DataFusionRegistrationModeV1, RelationSchemaError,
        VelorixRelationCatalogV1,
    },
};
use velorix_storage::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
        AuthoritativeObjectStoreCapabilityError,
    },
    object_key::{ObjectKey, ObjectKeyError},
};

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
    RelationSchema(#[from] RelationSchemaError),
    #[error(transparent)]
    ObjectStoreCapabilities(#[from] AuthoritativeObjectStoreCapabilityError),
    #[error(
        "production persisted query catalog requires shared startup object-store capability evidence"
    )]
    MissingProductionAuthorityEvidence,
    #[error("unsupported persisted query schema version {schema_version}")]
    UnsupportedSchemaVersion { schema_version: u32 },
    #[error("persisted query id mismatch: expected {expected}, got {actual}")]
    QueryIdMismatch { expected: String, actual: String },
}

#[derive(Clone, Debug)]
pub struct PersistedQueryStore {
    store: Arc<dyn ObjectStore>,
    production_authority_validated: bool,
}

impl PersistedQueryStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            production_authority_validated: false,
        }
    }

    pub fn new_checked(
        store: Arc<dyn ObjectStore>,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, PersistedQueryError> {
        capabilities.validate_namespace(AuthoritativeNamespace::Queries)?;

        Ok(Self {
            store,
            production_authority_validated: true,
        })
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
        self.require_production_authority()?;
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
        self.require_production_authority()?;
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
        self.require_production_authority()?;
        let spec = self.get(query_id).await?;
        validate_production_relation_query(&spec.sql, production_policy, relation_catalog).await?;

        Ok(spec)
    }

    fn require_production_authority(&self) -> Result<(), PersistedQueryError> {
        if self.production_authority_validated {
            Ok(())
        } else {
            Err(PersistedQueryError::MissingProductionAuthorityEvidence)
        }
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
