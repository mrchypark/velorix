use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode};
use thiserror::Error;

use crate::{
    capability::{ObjectStoreCapabilityError, ObjectStoreCapabilityProfile},
    manifest::{
        CheckpointManifest, ManifestError, OutputObjectRef, PartitionOwnerClaim, StateObjectRef,
    },
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputObjectWrite {
    stream_id: String,
    partition_id: u32,
    checkpoint_version: u64,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
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
    #[error(transparent)]
    ObjectStoreCapability(#[from] ObjectStoreCapabilityError),
    #[error("state object `{0}` already exists")]
    StateObjectAlreadyExists(ObjectKey),
    #[error("output object `{0}` already exists")]
    OutputObjectAlreadyExists(ObjectKey),
    #[error("checkpoint manifest `{0}` already exists")]
    ManifestAlreadyExists(ObjectKey),
    #[error(
        "checkpoint manifest key `{object_key}` does not match manifest body key `{body_key}`"
    )]
    ManifestKeyMismatch {
        object_key: ObjectKey,
        body_key: ObjectKey,
    },
    #[error(
        "parent checkpoint manifest {parent_checkpoint} for checkpoint {checkpoint_version} is not durably visible"
    )]
    MissingParentManifest {
        checkpoint_version: u64,
        parent_checkpoint: u64,
    },
    #[error(
        "checkpoint {checkpoint_version} drops parent input progress for {stream_id}/p={partition_id} from parent checkpoint {parent_checkpoint}"
    )]
    DroppedParentInputProgress {
        checkpoint_version: u64,
        parent_checkpoint: u64,
        stream_id: String,
        partition_id: u32,
    },
    #[error(
        "checkpoint {checkpoint_version} regresses parent input boundary for {stream_id}/p={partition_id} from parent checkpoint {parent_checkpoint}: parent {parent_start_offset_inclusive}..{parent_end_offset_exclusive}, child {child_start_offset_inclusive}..{child_end_offset_exclusive}"
    )]
    RegressedParentInputBoundary {
        checkpoint_version: u64,
        parent_checkpoint: u64,
        stream_id: String,
        partition_id: u32,
        parent_start_offset_inclusive: u64,
        parent_end_offset_exclusive: u64,
        child_start_offset_inclusive: u64,
        child_end_offset_exclusive: u64,
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
    #[error("output object `{object_key}` owner claim mismatch: expected `{expected}`, actual `{actual:?}`")]
    OutputOwnerClaimMismatch {
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
    /// Constructs a checkpoint publisher without object-store capability
    /// validation. Production/durable callers should use [`Self::new_checked`].
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        let state_store = Arc::new(RawObjectStateStore::new(Arc::clone(&store)));
        Self { store, state_store }
    }

    /// Constructs a checkpoint publisher after validating the supplied
    /// object-store profile has the capabilities required by Velorix
    /// durability.
    pub fn new_checked(
        store: Arc<dyn ObjectStore>,
        profile: &ObjectStoreCapabilityProfile,
    ) -> Result<Self, ObjectStoreCapabilityError> {
        profile.validate_for_velorix_durability()?;

        Ok(Self::new(store))
    }

    /// Constructs a checkpoint publisher with a caller-supplied state store
    /// without object-store capability validation. Production/durable callers
    /// should use [`Self::with_state_store_checked`].
    pub fn with_state_store(
        store: Arc<dyn ObjectStore>,
        state_store: Arc<dyn StateObjectStore>,
    ) -> Self {
        Self { store, state_store }
    }

    /// Constructs a checkpoint publisher with a caller-supplied state store
    /// after validating the object-store profile. Production/durable callers
    /// that do not use the raw object state store should use this checked
    /// variant.
    pub fn with_state_store_checked(
        store: Arc<dyn ObjectStore>,
        state_store: Arc<dyn StateObjectStore>,
        profile: &ObjectStoreCapabilityProfile,
    ) -> Result<Self, ObjectStoreCapabilityError> {
        profile.validate_for_velorix_durability()?;

        Ok(Self::with_state_store(store, state_store))
    }

    /// Constructs a checkpoint publisher with a SlateDB state store without
    /// object-store capability validation. Production/durable callers should
    /// use [`Self::with_slatedb_state_store_checked`].
    pub async fn with_slatedb_state_store(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<Path>,
    ) -> Result<Self, CheckpointPublishError> {
        let state_store = SlateDbStateStore::open(db_path, Arc::clone(&store)).await?;

        Ok(Self::with_state_store(store, Arc::new(state_store)))
    }

    /// Constructs a checkpoint publisher with a SlateDB state store after
    /// validating the supplied object-store profile has the capabilities
    /// required by Velorix durability.
    pub async fn with_slatedb_state_store_checked(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<Path>,
        profile: &ObjectStoreCapabilityProfile,
    ) -> Result<Self, CheckpointPublishError> {
        profile.validate_for_velorix_durability()?;

        Self::with_slatedb_state_store(store, db_path).await
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

    pub async fn write_output_object(
        &self,
        output: &OutputObjectWrite,
    ) -> Result<OutputObjectRef, CheckpointPublishError> {
        let path = Path::from(output.object_key().as_str());
        let result = self
            .store
            .put_opts(&path, output.bytes().clone().into(), PutMode::Create.into())
            .await;

        match result {
            Ok(_) => Ok(output.object_ref()),
            Err(object_store::Error::AlreadyExists { .. }) => Err(
                CheckpointPublishError::OutputObjectAlreadyExists(output.object_key().clone()),
            ),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn write_output_object_fenced(
        &self,
        output: &OutputObjectWrite,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<OutputObjectRef, CheckpointPublishError> {
        self.validate_output_write_owner_claim(output, owner_claim)?;
        self.validate_owner_claim_current([output.partition_id()], owner_claim)
            .await?;

        self.write_output_object(output).await
    }

    /// Runs manifest publication checks that do not depend on newly written
    /// state or output objects.
    pub async fn preflight_manifest_publication(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<(), CheckpointPublishError> {
        manifest.validate()?;
        self.validate_parent_manifest_visible(manifest).await
    }

    pub async fn publish_manifest(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<(), CheckpointPublishError> {
        self.preflight_manifest_publication(manifest).await?;
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
        Self::validate_manifest_lineage(&manifests)?;

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

    async fn validate_parent_manifest_visible(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<(), CheckpointPublishError> {
        let Some(parent_checkpoint) = manifest.parent_checkpoint else {
            return Ok(());
        };

        let parent = self
            .read_parent_manifest(manifest.checkpoint_version, parent_checkpoint)
            .await?;
        Self::validate_child_input_progress(manifest, &parent)
    }

    async fn read_parent_manifest(
        &self,
        checkpoint_version: u64,
        parent_checkpoint: u64,
    ) -> Result<CheckpointManifest, CheckpointPublishError> {
        let object_key = ObjectKey::checkpoint_manifest(parent_checkpoint);
        let path = Path::from(object_key.as_str());
        let bytes = match self.store.get(&path).await {
            Ok(result) => result.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(CheckpointPublishError::MissingParentManifest {
                    checkpoint_version,
                    parent_checkpoint,
                });
            }
            Err(err) => return Err(err.into()),
        };

        let manifest = serde_json::from_slice::<CheckpointManifest>(&bytes)?;
        manifest.validate()?;
        let body_key = manifest.object_key();
        if object_key != body_key {
            return Err(CheckpointPublishError::ManifestKeyMismatch {
                object_key,
                body_key,
            });
        }

        Ok(manifest)
    }

    fn validate_manifest_lineage(
        manifests: &[CheckpointManifest],
    ) -> Result<(), CheckpointPublishError> {
        let manifests_by_checkpoint = manifests
            .iter()
            .map(|manifest| (manifest.checkpoint_version, manifest))
            .collect::<HashMap<_, _>>();

        for manifest in manifests {
            let Some(parent_checkpoint) = manifest.parent_checkpoint else {
                continue;
            };
            let Some(parent) = manifests_by_checkpoint.get(&parent_checkpoint) else {
                return Err(CheckpointPublishError::MissingParentManifest {
                    checkpoint_version: manifest.checkpoint_version,
                    parent_checkpoint,
                });
            };

            Self::validate_child_input_progress(manifest, parent)?;
        }

        Ok(())
    }

    fn validate_child_input_progress(
        child: &CheckpointManifest,
        parent: &CheckpointManifest,
    ) -> Result<(), CheckpointPublishError> {
        let parent_checkpoint = parent.checkpoint_version;
        let child_ranges = child
            .input_ranges
            .iter()
            .map(|range| ((range.stream_id.as_str(), range.partition_id), range))
            .collect::<HashMap<_, _>>();

        for parent_range in &parent.input_ranges {
            let Some(child_range) = child_ranges
                .get(&(parent_range.stream_id.as_str(), parent_range.partition_id))
                .copied()
            else {
                return Err(CheckpointPublishError::DroppedParentInputProgress {
                    checkpoint_version: child.checkpoint_version,
                    parent_checkpoint,
                    stream_id: parent_range.stream_id.clone(),
                    partition_id: parent_range.partition_id,
                });
            };

            if child_range.start_offset_inclusive > parent_range.start_offset_inclusive
                || child_range.end_offset_exclusive < parent_range.end_offset_exclusive
            {
                return Err(CheckpointPublishError::RegressedParentInputBoundary {
                    checkpoint_version: child.checkpoint_version,
                    parent_checkpoint,
                    stream_id: parent_range.stream_id.clone(),
                    partition_id: parent_range.partition_id,
                    parent_start_offset_inclusive: parent_range.start_offset_inclusive,
                    parent_end_offset_exclusive: parent_range.end_offset_exclusive,
                    child_start_offset_inclusive: child_range.start_offset_inclusive,
                    child_end_offset_exclusive: child_range.end_offset_exclusive,
                });
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

    fn validate_output_write_owner_claim(
        &self,
        output: &OutputObjectWrite,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<(), CheckpointPublishError> {
        if output.owner_claim() == Some(owner_claim) {
            Ok(())
        } else {
            Err(CheckpointPublishError::OutputOwnerClaimMismatch {
                object_key: output.object_key().clone(),
                expected: owner_claim.clone(),
                actual: output.owner_claim().cloned(),
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

                if Self::is_current_claim_newer_or_conflicting(&current, owner_claim) {
                    return Err(CheckpointPublishError::StaleOwnerClaim {
                        partition_id: state_ref.partition_id,
                        current,
                        attempted: owner_claim.clone(),
                    });
                }
            }

            for output_ref in manifest.output_objects {
                if !partitions.contains(&output_ref.partition_id) {
                    continue;
                }

                let Some(current) = output_ref.owner_claim else {
                    continue;
                };

                if Self::is_current_claim_newer_or_conflicting(&current, owner_claim) {
                    return Err(CheckpointPublishError::StaleOwnerClaim {
                        partition_id: output_ref.partition_id,
                        current,
                        attempted: owner_claim.clone(),
                    });
                }
            }
        }

        Ok(())
    }

    fn is_current_claim_newer_or_conflicting(
        current: &PartitionOwnerClaim,
        attempted: &PartitionOwnerClaim,
    ) -> bool {
        current.owner_epoch > attempted.owner_epoch
            || (current.owner_epoch == attempted.owner_epoch
                && current.owner_id != attempted.owner_id)
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

impl OutputObjectWrite {
    pub fn new(
        stream_id: impl Into<String>,
        partition_id: u32,
        checkpoint_version: u64,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
        object_id: impl Into<String>,
        bytes: Bytes,
    ) -> Result<Self, CheckpointPublishError> {
        let stream_id = stream_id.into();
        let object_id = object_id.into();
        let object_key = ObjectKey::output_object(
            &stream_id,
            partition_id,
            checkpoint_version,
            start_offset_inclusive,
            end_offset_exclusive,
            &object_id,
        )?;

        Ok(Self {
            stream_id,
            partition_id,
            checkpoint_version,
            start_offset_inclusive,
            end_offset_exclusive,
            object_id,
            bytes,
            object_key,
            owner_claim: None,
        })
    }

    pub fn new_fenced(
        stream_id: impl Into<String>,
        partition_id: u32,
        checkpoint_version: u64,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
        object_id: impl Into<String>,
        owner_claim: PartitionOwnerClaim,
        bytes: Bytes,
    ) -> Result<Self, CheckpointPublishError> {
        let mut output = Self::new(
            stream_id,
            partition_id,
            checkpoint_version,
            start_offset_inclusive,
            end_offset_exclusive,
            object_id,
            bytes,
        )?;
        output.owner_claim = Some(owner_claim);

        Ok(output)
    }

    pub fn object_key(&self) -> &ObjectKey {
        &self.object_key
    }

    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }

    pub fn checkpoint_version(&self) -> u64 {
        self.checkpoint_version
    }

    pub fn start_offset_inclusive(&self) -> u64 {
        self.start_offset_inclusive
    }

    pub fn end_offset_exclusive(&self) -> u64 {
        self.end_offset_exclusive
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

    pub(crate) fn object_ref(&self) -> OutputObjectRef {
        OutputObjectRef {
            object_id: self.object_id.clone(),
            object_key: self.object_key.clone(),
            stream_id: self.stream_id.clone(),
            partition_id: self.partition_id,
            checkpoint_version: self.checkpoint_version,
            start_offset_inclusive: self.start_offset_inclusive,
            end_offset_exclusive: self.end_offset_exclusive,
            owner_claim: self.owner_claim.clone(),
        }
    }
}
