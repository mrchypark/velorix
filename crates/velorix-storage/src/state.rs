use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode};
use thiserror::Error;

use crate::{
    manifest::{CheckpointManifest, ManifestError, StateObjectRef},
    object_key::{ObjectKey, ObjectKeyError},
};

const CHECKPOINT_PREFIX: &str = "v1/checkpoints";

#[derive(Clone, Debug)]
pub struct CheckpointPublisher {
    store: Arc<dyn ObjectStore>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateObjectWrite {
    pub owner: String,
    pub partition_id: u32,
    pub checkpoint_version: u64,
    pub object_id: String,
    pub bytes: Bytes,
    object_key: ObjectKey,
}

#[derive(Debug, Error)]
pub enum CheckpointPublishError {
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error("state object `{0}` already exists")]
    StateObjectAlreadyExists(ObjectKey),
    #[error("checkpoint manifest `{0}` already exists")]
    ManifestAlreadyExists(ObjectKey),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

impl CheckpointPublisher {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub async fn write_state_object(
        &self,
        state: &StateObjectWrite,
    ) -> Result<StateObjectRef, CheckpointPublishError> {
        let path = Path::from(state.object_key.as_str());
        let result = self
            .store
            .put_opts(&path, state.bytes.clone().into(), PutMode::Create.into())
            .await;

        match result {
            Ok(_) => Ok(state.object_ref()),
            Err(object_store::Error::AlreadyExists { .. }) => Err(
                CheckpointPublishError::StateObjectAlreadyExists(state.object_key.clone()),
            ),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn publish_manifest(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<(), CheckpointPublishError> {
        manifest.validate()?;

        let object_key = manifest.object_key();
        let path = Path::from(object_key.as_str());
        let bytes = serde_json::to_vec(manifest)?;
        let result = self
            .store
            .put_opts(&path, Bytes::from(bytes).into(), PutMode::Create.into())
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                Err(CheckpointPublishError::ManifestAlreadyExists(object_key))
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn list_published_manifests(
        &self,
    ) -> Result<Vec<CheckpointManifest>, CheckpointPublishError> {
        let mut objects = self
            .store
            .list(Some(&Path::from(CHECKPOINT_PREFIX)))
            .try_collect::<Vec<_>>()
            .await?;

        objects.sort_by(|left, right| left.location.cmp(&right.location));

        let mut manifests = Vec::with_capacity(objects.len());
        for object in objects {
            let bytes = self.store.get(&object.location).await?.bytes().await?;
            let manifest = serde_json::from_slice::<CheckpointManifest>(&bytes)?;
            manifest.validate()?;
            manifests.push(manifest);
        }

        Ok(manifests)
    }

    pub async fn latest_manifest(
        &self,
    ) -> Result<Option<CheckpointManifest>, CheckpointPublishError> {
        Ok(self.list_published_manifests().await?.pop())
    }

    pub async fn read_state_object(
        &self,
        state_ref: &StateObjectRef,
    ) -> Result<Bytes, CheckpointPublishError> {
        Ok(self
            .store
            .get(&Path::from(state_ref.object_key.as_str()))
            .await?
            .bytes()
            .await?)
    }
}

impl StateObjectWrite {
    pub fn new(
        owner: impl Into<String>,
        partition_id: u32,
        checkpoint_version: u64,
        object_id: impl Into<String>,
        bytes: Bytes,
    ) -> Result<Self, CheckpointPublishError> {
        let owner = owner.into();
        let object_id = object_id.into();
        let object_key =
            ObjectKey::state_object(&owner, partition_id, checkpoint_version, &object_id)?;

        Ok(Self {
            owner,
            partition_id,
            checkpoint_version,
            object_id,
            bytes,
            object_key,
        })
    }

    pub fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    fn object_ref(&self) -> StateObjectRef {
        StateObjectRef {
            object_id: self.object_id.clone(),
            object_key: self.object_key.clone(),
            owner: self.owner.clone(),
            partition_id: self.partition_id,
            checkpoint_version: self.checkpoint_version,
        }
    }
}
