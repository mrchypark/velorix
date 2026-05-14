use std::{fmt, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{path::Path, ObjectStore, PutMode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::{ErrorKind, IsolationLevel};

use crate::{
    capability::{AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1},
    manifest::{SlateDbCheckpointRefV1, StateObjectRef, StateRefType},
    state::{CheckpointPublishError, StateObjectWrite},
};

const SLATEDB_STATE_MARKER_PREFIX: &str = "__velorix_state_ref_v1";

#[async_trait]
pub trait StateObjectStore: fmt::Debug + Send + Sync {
    fn produced_state_ref_type(&self) -> StateRefType;

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

    /// Opens a SlateDB state store after validating the authoritative state
    /// namespace from shared startup capability evidence.
    pub async fn open_authoritative(
        db_path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, CheckpointPublishError> {
        capabilities.validate_namespace(AuthoritativeNamespace::State)?;

        Self::open(db_path, object_store).await
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
    fn produced_state_ref_type(&self) -> StateRefType {
        StateRefType::RawObject
    }

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
        if state_ref.ref_type == StateRefType::SlateDbCheckpoint {
            return Err(CheckpointPublishError::MissingStateObject(
                state_ref.object_key.clone(),
            ));
        }

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
        if state_ref.ref_type == StateRefType::SlateDbCheckpoint {
            return Ok(false);
        }

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
        if state_ref.ref_type == StateRefType::SlateDbCheckpoint {
            return Ok(false);
        }

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

#[async_trait]
impl StateObjectStore for SlateDbStateStore {
    fn produced_state_ref_type(&self) -> StateRefType {
        StateRefType::SlateDbCheckpoint
    }

    async fn write_state_object(
        &self,
        state: &StateObjectWrite,
    ) -> Result<StateObjectRef, CheckpointPublishError> {
        let key = state.object_key().as_str().as_bytes();
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
        if txn.get(key).await?.is_some() {
            return Err(CheckpointPublishError::StateObjectAlreadyExists(
                state.object_key().clone(),
            ));
        }

        let metadata = SlateDbCheckpointRefV1 {
            db_path: self.db_path.to_string(),
            state_key: state.object_key().as_str().to_string(),
            state_digest: state_digest(state.bytes()),
            state_bytes: state.bytes().len() as u64,
            created_by_checkpoint_version: state.checkpoint_version(),
        };
        let marker = SlateDbStateMarkerV1::from(metadata.clone());

        txn.put(key, state.bytes().as_ref())?;
        txn.put(
            state_marker_key(&metadata.state_key),
            serde_json::to_vec(&marker)?,
        )?;
        match txn.commit().await {
            Ok(_) => {
                let mut state_ref = state.object_ref();
                state_ref.ref_type = StateRefType::SlateDbCheckpoint;
                state_ref.slatedb = Some(metadata);
                Ok(state_ref)
            }
            Err(err) if err.kind() == ErrorKind::Transaction => Err(
                CheckpointPublishError::StateObjectAlreadyExists(state.object_key().clone()),
            ),
            Err(err) => Err(err.into()),
        }
    }

    async fn read_state_object(
        &self,
        state_ref: &StateObjectRef,
    ) -> Result<Bytes, CheckpointPublishError> {
        let metadata = self.validate_state_ref_metadata(state_ref)?;
        let bytes = self
            .db
            .get(metadata.state_key.as_bytes())
            .await?
            .ok_or_else(|| {
                CheckpointPublishError::MissingStateObject(state_ref.object_key.clone())
            })?;

        let actual_bytes = bytes.len() as u64;
        let actual_digest = state_digest(&bytes);
        if actual_bytes != metadata.state_bytes || actual_digest != metadata.state_digest {
            return Err(CheckpointPublishError::SlateDbStatePayloadMismatch {
                object_key: state_ref.object_key.clone(),
                expected_digest: metadata.state_digest.clone(),
                actual_digest,
                expected_bytes: metadata.state_bytes,
                actual_bytes,
            });
        }

        Ok(bytes)
    }

    async fn state_object_exists(
        &self,
        state_ref: &StateObjectRef,
    ) -> Result<bool, CheckpointPublishError> {
        let metadata = match self.validate_state_ref_metadata(state_ref) {
            Ok(metadata) => metadata,
            Err(CheckpointPublishError::InvalidSlateDbStateRef { .. }) => return Ok(false),
            Err(err) => return Err(err),
        };

        let Some(marker_bytes) = self.db.get(state_marker_key(&metadata.state_key)).await? else {
            return Ok(false);
        };
        let marker: SlateDbStateMarkerV1 = serde_json::from_slice(&marker_bytes)?;
        if marker != SlateDbStateMarkerV1::from(metadata.clone()) {
            return Err(CheckpointPublishError::InvalidSlateDbStateRef {
                object_key: state_ref.object_key.clone(),
                reason: "SlateDB state marker does not match state ref",
            });
        }

        Ok(self.db.get(metadata.state_key.as_bytes()).await?.is_some())
    }

    async fn release_state_object(
        &self,
        state_ref: &StateObjectRef,
    ) -> Result<bool, CheckpointPublishError> {
        let metadata = self.validate_state_ref_metadata(state_ref)?;
        let marker_key = state_marker_key(&metadata.state_key);
        let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

        let Some(marker_bytes) = txn.get(marker_key.as_bytes()).await? else {
            return Ok(false);
        };
        let marker: SlateDbStateMarkerV1 = serde_json::from_slice(&marker_bytes)?;
        if marker != SlateDbStateMarkerV1::from(metadata.clone()) {
            return Err(CheckpointPublishError::InvalidSlateDbStateRef {
                object_key: state_ref.object_key.clone(),
                reason: "SlateDB state marker does not match state ref",
            });
        }

        let Some(bytes) = txn.get(metadata.state_key.as_bytes()).await? else {
            return Err(CheckpointPublishError::MissingStateObject(
                state_ref.object_key.clone(),
            ));
        };
        let actual_bytes = bytes.len() as u64;
        let actual_digest = state_digest(&bytes);
        if actual_bytes != metadata.state_bytes || actual_digest != metadata.state_digest {
            return Err(CheckpointPublishError::SlateDbStatePayloadMismatch {
                object_key: state_ref.object_key.clone(),
                expected_digest: metadata.state_digest.clone(),
                actual_digest,
                expected_bytes: metadata.state_bytes,
                actual_bytes,
            });
        }

        txn.delete(metadata.state_key.as_bytes())?;
        txn.delete(marker_key.as_bytes())?;
        txn.commit().await?;

        Ok(true)
    }
}

impl SlateDbStateStore {
    fn validate_state_ref_metadata<'a>(
        &self,
        state_ref: &'a StateObjectRef,
    ) -> Result<&'a SlateDbCheckpointRefV1, CheckpointPublishError> {
        if state_ref.ref_type != StateRefType::SlateDbCheckpoint {
            return Err(CheckpointPublishError::InvalidSlateDbStateRef {
                object_key: state_ref.object_key.clone(),
                reason: "state ref type is not SlateDB checkpoint",
            });
        }

        let Some(metadata) = state_ref.slatedb.as_ref() else {
            return Err(CheckpointPublishError::InvalidSlateDbStateRef {
                object_key: state_ref.object_key.clone(),
                reason: "missing SlateDB checkpoint metadata",
            });
        };

        if metadata.db_path != self.db_path.to_string()
            || metadata.state_key != state_ref.object_key.as_str()
            || metadata.created_by_checkpoint_version != state_ref.checkpoint_version
        {
            return Err(CheckpointPublishError::InvalidSlateDbStateRef {
                object_key: state_ref.object_key.clone(),
                reason: "SlateDB checkpoint metadata does not match state ref",
            });
        }

        Ok(metadata)
    }
}

fn state_digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn state_marker_key(state_key: &str) -> String {
    format!(
        "{SLATEDB_STATE_MARKER_PREFIX}/{}",
        state_digest(state_key.as_bytes())
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SlateDbStateMarkerV1 {
    db_path: String,
    state_key: String,
    state_digest: String,
    state_bytes: u64,
    created_by_checkpoint_version: u64,
}

impl From<SlateDbCheckpointRefV1> for SlateDbStateMarkerV1 {
    fn from(value: SlateDbCheckpointRefV1) -> Self {
        Self {
            db_path: value.db_path,
            state_key: value.state_key,
            state_digest: value.state_digest,
            state_bytes: value.state_bytes,
            created_by_checkpoint_version: value.created_by_checkpoint_version,
        }
    }
}
