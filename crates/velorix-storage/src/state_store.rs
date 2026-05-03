use std::{fmt, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutMode};

use crate::{
    manifest::StateObjectRef,
    state::{CheckpointPublishError, StateObjectWrite},
};

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
}

#[derive(Clone, Debug)]
pub struct RawObjectStateStore {
    store: Arc<dyn ObjectStore>,
}

#[derive(Clone)]
pub struct SlateDbStateStore {
    db: slatedb::Db,
    db_path: Path,
}

impl RawObjectStateStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

impl SlateDbStateStore {
    pub async fn open(
        db_path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, CheckpointPublishError> {
        let db_path = db_path.into();
        let db = slatedb::Db::open(db_path.clone(), object_store).await?;

        Ok(Self { db, db_path })
    }

    pub async fn close(&self) -> Result<(), CheckpointPublishError> {
        Ok(self.db.close().await?)
    }
}

impl fmt::Debug for SlateDbStateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlateDbStateStore")
            .field("db_path", &self.db_path)
            .finish_non_exhaustive()
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
}

#[async_trait]
impl StateObjectStore for SlateDbStateStore {
    async fn write_state_object(
        &self,
        state: &StateObjectWrite,
    ) -> Result<StateObjectRef, CheckpointPublishError> {
        let key = state.object_key().as_str().as_bytes();
        if self.db.get(key).await?.is_some() {
            return Err(CheckpointPublishError::StateObjectAlreadyExists(
                state.object_key().clone(),
            ));
        }

        self.db.put(key, state.bytes().as_ref()).await?;

        Ok(state.object_ref())
    }

    async fn read_state_object(
        &self,
        state_ref: &StateObjectRef,
    ) -> Result<Bytes, CheckpointPublishError> {
        self.db
            .get(state_ref.object_key.as_str().as_bytes())
            .await?
            .ok_or_else(|| CheckpointPublishError::MissingStateObject(state_ref.object_key.clone()))
    }

    async fn state_object_exists(
        &self,
        state_ref: &StateObjectRef,
    ) -> Result<bool, CheckpointPublishError> {
        Ok(self
            .db
            .get(state_ref.object_key.as_str().as_bytes())
            .await?
            .is_some())
    }
}
