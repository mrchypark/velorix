use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode};
use thiserror::Error;

use crate::{
    manifest::{CheckpointManifest, ManifestError, StateObjectRef},
    object_key::{ObjectKey, ObjectKeyError},
    state_store::{RawObjectStateStore, SlateDbStateStore, StateObjectStore},
};

const CHECKPOINT_PREFIX: &str = "v1/checkpoints";

#[derive(Clone, Debug)]
pub struct CheckpointPublisher {
    store: Arc<dyn ObjectStore>,
    state_store: Arc<dyn StateObjectStore>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateObjectWrite {
    owner: String,
    partition_id: u32,
    checkpoint_version: u64,
    object_id: String,
    bytes: Bytes,
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
    #[error(
        "checkpoint manifest key `{object_key}` does not match manifest body key `{body_key}`"
    )]
    ManifestKeyMismatch {
        object_key: ObjectKey,
        body_key: ObjectKey,
    },
    #[error("referenced state object `{0}` is missing")]
    MissingStateObject(ObjectKey),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    SlateDb(#[from] slatedb::Error),
}

impl CheckpointPublisher {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        let state_store = Arc::new(RawObjectStateStore::new(Arc::clone(&store)));
        Self { store, state_store }
    }

    pub fn with_state_store(
        store: Arc<dyn ObjectStore>,
        state_store: Arc<dyn StateObjectStore>,
    ) -> Self {
        Self { store, state_store }
    }

    pub async fn with_slatedb_state_store(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<Path>,
    ) -> Result<Self, CheckpointPublishError> {
        let state_store = SlateDbStateStore::open(db_path, Arc::clone(&store)).await?;

        Ok(Self::with_state_store(store, Arc::new(state_store)))
    }

    pub async fn write_state_object(
        &self,
        state: &StateObjectWrite,
    ) -> Result<StateObjectRef, CheckpointPublishError> {
        self.state_store.write_state_object(state).await
    }

    pub async fn publish_manifest(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<(), CheckpointPublishError> {
        manifest.validate()?;
        self.validate_state_objects_exist(manifest).await?;

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
        let objects = self
            .store
            .list(Some(&Path::from(CHECKPOINT_PREFIX)))
            .try_collect::<Vec<_>>()
            .await?;

        let mut manifests = Vec::with_capacity(objects.len());
        for object in objects {
            let object_key = ObjectKey::parse(object.location.to_string())?;
            let bytes = self.store.get(&object.location).await?.bytes().await?;
            let manifest = serde_json::from_slice::<CheckpointManifest>(&bytes)?;
            manifest.validate()?;
            let body_key = manifest.object_key();
            if object_key != body_key {
                return Err(CheckpointPublishError::ManifestKeyMismatch {
                    object_key,
                    body_key,
                });
            }
            manifests.push(manifest);
        }
        manifests.sort_by_key(|manifest| manifest.checkpoint_version);

        Ok(manifests)
    }

    pub async fn latest_manifest(
        &self,
    ) -> Result<Option<CheckpointManifest>, CheckpointPublishError> {
        Ok(self
            .list_published_manifests()
            .await?
            .into_iter()
            .max_by_key(|manifest| manifest.checkpoint_version))
    }

    pub async fn read_state_object(
        &self,
        state_ref: &StateObjectRef,
    ) -> Result<Bytes, CheckpointPublishError> {
        self.state_store.read_state_object(state_ref).await
    }

    async fn validate_state_objects_exist(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<(), CheckpointPublishError> {
        for state_ref in &manifest.state_objects {
            if !self.state_store.state_object_exists(state_ref).await? {
                return Err(CheckpointPublishError::MissingStateObject(
                    state_ref.object_key.clone(),
                ));
            }
        }

        Ok(())
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

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }

    pub fn checkpoint_version(&self) -> u64 {
        self.checkpoint_version
    }

    pub fn object_id(&self) -> &str {
        &self.object_id
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub(crate) fn object_ref(&self) -> StateObjectRef {
        StateObjectRef {
            object_id: self.object_id.clone(),
            object_key: self.object_key.clone(),
            owner: self.owner.clone(),
            partition_id: self.partition_id,
            checkpoint_version: self.checkpoint_version,
        }
    }
}
