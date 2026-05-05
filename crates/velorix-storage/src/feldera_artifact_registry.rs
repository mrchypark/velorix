use std::sync::Arc;

use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutMode};
use thiserror::Error;
use velorix_core::feldera_artifact::{
    validate_feldera_compile_artifact, FelderaArtifactError, FelderaCompileArtifactMetadata,
    StandingViewSpec,
};

use crate::{
    capability::{ObjectStoreCapabilityError, ObjectStoreCapabilityProfile},
    object_key::{ObjectKey, ObjectKeyError},
};

#[derive(Clone, Debug)]
pub struct FelderaArtifactRegistry {
    store: Arc<dyn ObjectStore>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterFelderaArtifactOutcome {
    Created,
    Duplicate,
}

#[derive(Debug, Error)]
pub enum FelderaArtifactRegistryError {
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    #[error(transparent)]
    Validation(#[from] FelderaArtifactError),
    #[error("Feldera artifact registry record conflict at `{object_key}`")]
    RecordConflict { object_key: ObjectKey },
    #[error("Feldera artifact registry record `{object_key}` body identity does not match key")]
    RecordIdentityMismatch { object_key: ObjectKey },
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

impl FelderaArtifactRegistry {
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
        artifact_id: &str,
        artifact_hash: &str,
    ) -> Result<ObjectKey, FelderaArtifactRegistryError> {
        Ok(ObjectKey::feldera_artifact(artifact_id, artifact_hash)?)
    }

    pub async fn register(
        &self,
        spec: &StandingViewSpec,
        artifact: &FelderaCompileArtifactMetadata,
    ) -> Result<RegisterFelderaArtifactOutcome, FelderaArtifactRegistryError> {
        validate_feldera_compile_artifact(spec, artifact)?;

        let object_key = self.object_key(&artifact.artifact_id, &artifact.artifact_hash)?;
        let bytes = serde_json::to_vec(artifact)?;
        let result = self
            .store
            .put_opts(
                &Path::from(object_key.as_str()),
                Bytes::from(bytes).into(),
                PutMode::Create.into(),
            )
            .await;

        match result {
            Ok(_) => Ok(RegisterFelderaArtifactOutcome::Created),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self.read_object(&object_key).await?;
                if existing == *artifact {
                    Ok(RegisterFelderaArtifactOutcome::Duplicate)
                } else {
                    Err(FelderaArtifactRegistryError::RecordConflict { object_key })
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn read(
        &self,
        artifact_id: &str,
        artifact_hash: &str,
    ) -> Result<FelderaCompileArtifactMetadata, FelderaArtifactRegistryError> {
        let object_key = self.object_key(artifact_id, artifact_hash)?;

        let record = self.read_object(&object_key).await?;
        self.validate_record_identity(&object_key, &record)?;

        Ok(record)
    }

    async fn read_object(
        &self,
        object_key: &ObjectKey,
    ) -> Result<FelderaCompileArtifactMetadata, FelderaArtifactRegistryError> {
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
        record: &FelderaCompileArtifactMetadata,
    ) -> Result<(), FelderaArtifactRegistryError> {
        if *object_key == self.object_key(&record.artifact_id, &record.artifact_hash)? {
            Ok(())
        } else {
            Err(FelderaArtifactRegistryError::RecordIdentityMismatch {
                object_key: object_key.clone(),
            })
        }
    }
}
