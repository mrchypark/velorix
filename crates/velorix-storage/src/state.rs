use std::{collections::HashSet, sync::Arc};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode};
use thiserror::Error;

use crate::{
    manifest::{CheckpointManifest, ManifestError, PartitionOwnerClaim, StateObjectRef},
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
    owner_claim: Option<PartitionOwnerClaim>,
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
    #[error("referenced output object `{0}` is missing")]
    MissingOutputObject(ObjectKey),
    #[error("state object `{object_key}` owner claim mismatch: expected `{expected}`, actual `{actual:?}`")]
    StateOwnerClaimMismatch {
        object_key: ObjectKey,
        expected: PartitionOwnerClaim,
        actual: Option<PartitionOwnerClaim>,
    },
    #[error("stale owner claim for partition {partition_id}: current `{current}`, attempted `{attempted}`")]
    StaleOwnerClaim {
        partition_id: u32,
        current: PartitionOwnerClaim,
        attempted: PartitionOwnerClaim,
    },
    #[error(
        "fenced manifest {progress_kind} partition {partition_id} is not covered by state objects for owner claim `{owner_claim}`"
    )]
    FencedManifestPartitionNotClaimed {
        progress_kind: &'static str,
        partition_id: u32,
        owner_claim: PartitionOwnerClaim,
    },
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

    pub async fn write_state_object_fenced(
        &self,
        state: &StateObjectWrite,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<StateObjectRef, CheckpointPublishError> {
        self.validate_state_write_owner_claim(state, owner_claim)?;
        self.validate_owner_claim_current([state.partition_id()], owner_claim)
            .await?;

        self.write_state_object(state).await
    }

    pub async fn publish_manifest(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<(), CheckpointPublishError> {
        manifest.validate()?;
        self.validate_state_objects_exist(manifest).await?;
        self.validate_output_objects_exist(manifest).await?;

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

    /// Publishes one checkpoint manifest for a single owner claim.
    ///
    /// This is a non-atomic storage-side stale-owner detection and structural
    /// authorization check. It rejects state refs that do not carry the
    /// requested claim, and rejects input/output progress for partitions not
    /// represented by state refs carrying that exact claim. Production
    /// linearizable fencing remains a future control-plane/commit protocol.
    pub async fn publish_manifest_fenced(
        &self,
        manifest: &CheckpointManifest,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<(), CheckpointPublishError> {
        manifest.validate()?;
        manifest.validate_owner_claim(owner_claim)?;
        self.validate_fenced_manifest_progress_claimed(manifest, owner_claim)?;

        let partitions = manifest
            .state_objects
            .iter()
            .map(|state_ref| state_ref.partition_id)
            .collect::<HashSet<_>>();
        self.validate_owner_claim_current(partitions, owner_claim)
            .await?;

        self.publish_manifest(manifest).await
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

    async fn validate_output_objects_exist(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<(), CheckpointPublishError> {
        for output_object in &manifest.output_objects {
            let path = Path::from(output_object.object_key.as_str());
            match self.store.head(&path).await {
                Ok(_) => {}
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(CheckpointPublishError::MissingOutputObject(
                        output_object.object_key.clone(),
                    ));
                }
                Err(err) => return Err(err.into()),
            }
        }

        Ok(())
    }

    fn validate_fenced_manifest_progress_claimed(
        &self,
        manifest: &CheckpointManifest,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<(), CheckpointPublishError> {
        let claimed_partitions = manifest
            .state_objects
            .iter()
            .filter(|state_ref| state_ref.owner_claim.as_ref() == Some(owner_claim))
            .map(|state_ref| state_ref.partition_id)
            .collect::<HashSet<_>>();

        for input_range in &manifest.input_ranges {
            if !claimed_partitions.contains(&input_range.partition_id) {
                return Err(CheckpointPublishError::FencedManifestPartitionNotClaimed {
                    progress_kind: "input",
                    partition_id: input_range.partition_id,
                    owner_claim: owner_claim.clone(),
                });
            }
        }

        for output_object in &manifest.output_objects {
            if !claimed_partitions.contains(&output_object.partition_id) {
                return Err(CheckpointPublishError::FencedManifestPartitionNotClaimed {
                    progress_kind: "output",
                    partition_id: output_object.partition_id,
                    owner_claim: owner_claim.clone(),
                });
            }
        }

        Ok(())
    }

    fn validate_state_write_owner_claim(
        &self,
        state: &StateObjectWrite,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<(), CheckpointPublishError> {
        if state.owner_claim() == Some(owner_claim) {
            Ok(())
        } else {
            Err(CheckpointPublishError::StateOwnerClaimMismatch {
                object_key: state.object_key().clone(),
                expected: owner_claim.clone(),
                actual: state.owner_claim().cloned(),
            })
        }
    }

    async fn validate_owner_claim_current(
        &self,
        partitions: impl IntoIterator<Item = u32>,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<(), CheckpointPublishError> {
        let partitions = partitions.into_iter().collect::<HashSet<_>>();
        for manifest in self.list_published_manifests().await? {
            for state_ref in manifest.state_objects {
                if !partitions.contains(&state_ref.partition_id) {
                    continue;
                }

                let Some(current) = state_ref.owner_claim else {
                    continue;
                };

                if current.owner_epoch > owner_claim.owner_epoch
                    || (current.owner_epoch == owner_claim.owner_epoch
                        && current.owner_id != owner_claim.owner_id)
                {
                    return Err(CheckpointPublishError::StaleOwnerClaim {
                        partition_id: state_ref.partition_id,
                        current,
                        attempted: owner_claim.clone(),
                    });
                }
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
            owner_claim: None,
        })
    }

    pub fn new_fenced(
        owner: impl Into<String>,
        partition_id: u32,
        checkpoint_version: u64,
        object_id: impl Into<String>,
        owner_claim: PartitionOwnerClaim,
        bytes: Bytes,
    ) -> Result<Self, CheckpointPublishError> {
        let mut state = Self::new(owner, partition_id, checkpoint_version, object_id, bytes)?;
        state.owner_claim = Some(owner_claim);

        Ok(state)
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

    pub fn owner_claim(&self) -> Option<&PartitionOwnerClaim> {
        self.owner_claim.as_ref()
    }

    pub(crate) fn object_ref(&self) -> StateObjectRef {
        StateObjectRef {
            object_id: self.object_id.clone(),
            object_key: self.object_key.clone(),
            owner: self.owner.clone(),
            partition_id: self.partition_id,
            checkpoint_version: self.checkpoint_version,
            owner_claim: self.owner_claim.clone(),
        }
    }
}
