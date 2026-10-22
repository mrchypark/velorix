use std::{fmt, sync::Arc};

use crate::{
    manifest::StateObjectRef,
    state::{CheckpointPublishError, StateObjectWrite},
};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore, ObjectStoreExt, PutMode};

#[async_trait]
pub trait StateObjectStore: fmt::Debug + Send + Sync {
    async fn write_state_object(
        &self,
        state: &StateObjectWrite,
    ) -> Result<StateObjectRef, CheckpointPublishError>;

    async fn read_state_object(
        &self,
        state_ref: &StateObjectRef,
    ) -> Result<Bytes, CheckpointPublishError>;

    async fn state_object_exists(
        &self,
        state_ref: &StateObjectRef,
    ) -> Result<bool, CheckpointPublishError>;

    async fn release_state_object(
        &self,
        state_ref: &StateObjectRef,
    ) -> Result<bool, CheckpointPublishError>;
}

#[derive(Clone, Debug)]
pub struct RawObjectStateStore {
    store: Arc<dyn ObjectStore>,
}

impl RawObjectStateStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}
#[async_trait]
impl StateObjectStore for RawObjectStateStore {
    async fn write_state_object(
        &self,
        state: &StateObjectWrite,
    ) -> Result<StateObjectRef, CheckpointPublishError> {
        let path = Path::from(state.object_key().as_str());
        let result = self
            .store
            .put_opts(&path, state.bytes().clone().into(), PutMode::Create.into())
            .await;

        match result {
            Ok(_) => Ok(state.object_ref()),
            Err(object_store::Error::AlreadyExists { .. }) => Err(
                CheckpointPublishError::StateObjectAlreadyExists(state.object_key().clone()),
            ),
            Err(err) => Err(err.into()),
        }
    }

    async fn read_state_object(
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

    async fn state_object_exists(
        &self,
        state_ref: &StateObjectRef,
    ) -> Result<bool, CheckpointPublishError> {
        match self
            .store
            .head(&Path::from(state_ref.object_key.as_str()))
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    async fn release_state_object(
        &self,
        state_ref: &StateObjectRef,
    ) -> Result<bool, CheckpointPublishError> {
        match self
            .store
            .delete(&Path::from(state_ref.object_key.as_str()))
            .await
        {
            Ok(()) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }
}
