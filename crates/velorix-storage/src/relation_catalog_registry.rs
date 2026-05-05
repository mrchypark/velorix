use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutMode};
use thiserror::Error;
use velorix_core::relation::{RelationSchemaError, VelorixRelationCatalogV1};

use crate::{
    capability::{ObjectStoreCapabilityError, ObjectStoreCapabilityProfile},
    object_key::{ObjectKey, ObjectKeyError},
};

#[derive(Clone, Debug)]
pub struct RelationCatalogRegistry {
    store: Arc<dyn ObjectStore>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateRelationCatalogOutcome {
    Created,
    Duplicate,
}

#[derive(Debug, Error)]
pub enum RelationCatalogRegistryError {
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    #[error(transparent)]
    Validation(#[from] RelationSchemaError),
    #[error("relation catalog record conflict at `{object_key}`")]
    RecordConflict { object_key: ObjectKey },
    #[error("relation catalog record `{object_key}` body identity does not match key")]
    RecordIdentityMismatch { object_key: ObjectKey },
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

impl RelationCatalogRegistry {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub fn new_checked(
        store: Arc<dyn ObjectStore>,
        profile: &ObjectStoreCapabilityProfile,
    ) -> Result<Self, ObjectStoreCapabilityError> {
        profile.validate_for_velorix_durability()?;

        Ok(Self::new(store))
    }

    pub fn object_key(
        &self,
        relation_id: &str,
        relation_version: &str,
    ) -> Result<ObjectKey, RelationCatalogRegistryError> {
        Ok(ObjectKey::relation_catalog(relation_id, relation_version)?)
    }

    pub async fn create(
        &self,
        catalog: &VelorixRelationCatalogV1,
    ) -> Result<CreateRelationCatalogOutcome, RelationCatalogRegistryError> {
        catalog.validate()?;

        let object_key = self.object_key(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )?;
        let bytes = serde_json::to_vec(catalog)?;
        let result = self
            .store
            .put_opts(
                &Path::from(object_key.as_str()),
                Bytes::from(bytes).into(),
                PutMode::Create.into(),
            )
            .await;

        match result {
            Ok(_) => Ok(CreateRelationCatalogOutcome::Created),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self.read_object(&object_key).await?;
                if existing == *catalog {
                    Ok(CreateRelationCatalogOutcome::Duplicate)
                } else {
                    Err(RelationCatalogRegistryError::RecordConflict { object_key })
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn read(
        &self,
        relation_id: &str,
        relation_version: &str,
    ) -> Result<VelorixRelationCatalogV1, RelationCatalogRegistryError> {
        let object_key = self.object_key(relation_id, relation_version)?;
        let record = self.read_object(&object_key).await?;

        record.validate()?;
        self.validate_record_identity(&object_key, &record)?;

        Ok(record)
    }

    async fn read_object(
        &self,
        object_key: &ObjectKey,
    ) -> Result<VelorixRelationCatalogV1, RelationCatalogRegistryError> {
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;

        Ok(serde_json::from_slice(&bytes)?)
    }

    fn validate_record_identity(
        &self,
        object_key: &ObjectKey,
        record: &VelorixRelationCatalogV1,
    ) -> Result<(), RelationCatalogRegistryError> {
        if *object_key
            == self.object_key(
                &record.relation_schema.relation_id,
                &record.relation_schema.relation_version,
            )?
        {
            Ok(())
        } else {
            Err(RelationCatalogRegistryError::RecordIdentityMismatch {
                object_key: object_key.clone(),
            })
        }
    }
}
