use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
        AuthoritativeObjectStoreCapabilityError, ObjectStoreCapabilityError,
        ObjectStoreCapabilityProfile,
    },
    checkpoint_index::{
        manifest_digest, marker_updated_at_now, recovery_transition_id_now,
        CheckpointAdminInspection, CheckpointGcTransitionRecordV1, CheckpointLifecycleRecord,
        CheckpointLifecycleStatus, CheckpointManifestInspection,
        CheckpointManifestInspectionStatus, CheckpointRecoveryMode,
        CheckpointRecoveryTransitionRecordV1, CheckpointRetentionRecordV1, LatestCandidateMarker,
    },
    gc::{
        GarbageCollectionCandidate, GarbageCollectionCandidateKind, GarbageCollectionPlan,
        GarbageCollectionPolicy, GarbageCollectionReport, GarbageCollectionRunV1,
        GC_RUN_SCHEMA_VERSION,
    },
    manifest::{
        CheckpointManifest, ManifestError, OutputObjectRef, PartitionOwnerClaim, StateObjectRef,
        StateRefType,
    },
    object_key::{ObjectKey, ObjectKeyError},
    ownership::{OwnershipEpochRecord, OwnershipEpochRecordError},
    state_store::{RawObjectStateStore, SlateDbStateStore, StateObjectStore},
};

const CHECKPOINT_PREFIX: &str = "v1/checkpoints";
const GC_RUN_PREFIX: &str = "v1/gc-runs";
const STATE_PREFIX: &str = "v1/state";
const OUTPUT_PREFIX: &str = "v1/outputs";

#[derive(Clone, Debug)]
pub struct CheckpointPublisher {
    store: Arc<dyn ObjectStore>,
    state_store: Arc<dyn StateObjectStore>,
}

struct InspectableCheckpointManifest {
    manifest: CheckpointManifest,
    lifecycle_status: Option<CheckpointLifecycleStatus>,
    gc_transition_records: Vec<CheckpointGcTransitionRecordV1>,
    retention_record: Option<CheckpointRetentionRecordV1>,
    recovery_transition_records: Vec<CheckpointRecoveryTransitionRecordV1>,
    payload_status: Result<(), String>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FencedOutputObjectWriteRequest {
    pub stream_id: String,
    pub partition_id: u32,
    pub checkpoint_version: u64,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
    pub object_id: String,
    pub owner_claim: PartitionOwnerClaim,
    pub bytes: Bytes,
}

#[derive(Debug, Error)]
pub enum CheckpointPublishError {
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    OwnershipEpochRecord(#[from] OwnershipEpochRecordError),
    #[error(transparent)]
    ObjectStoreCapability(#[from] ObjectStoreCapabilityError),
    #[error(transparent)]
    AuthoritativeObjectStoreCapabilities(#[from] AuthoritativeObjectStoreCapabilityError),
    #[error("state object `{0}` already exists")]
    StateObjectAlreadyExists(ObjectKey),
    #[error("output object `{0}` already exists")]
    OutputObjectAlreadyExists(ObjectKey),
    #[error("checkpoint manifest `{0}` already exists")]
    ManifestAlreadyExists(ObjectKey),
    #[error("checkpoint recovery transition record `{0}` already exists")]
    CheckpointRecoveryTransitionAlreadyExists(ObjectKey),
    #[error(
        "checkpoint lifecycle transition record `{0}` already exists with different causal fields"
    )]
    CheckpointGcTransitionConflict(ObjectKey),
    #[error("garbage collection run evidence `{0}` already exists")]
    GarbageCollectionRunAlreadyExists(ObjectKey),
    #[error(
        "garbage collection plan does not match policy retain_latest_manifests={retain_latest_manifests}: expected retained manifests {expected_retained_manifest_versions:?} and candidates {expected_candidate_keys:?}, actual retained manifests {actual_retained_manifest_versions:?} and candidates {actual_candidate_keys:?}"
    )]
    GarbageCollectionPlanPolicyMismatch {
        retain_latest_manifests: usize,
        expected_retained_manifest_versions: Vec<u64>,
        actual_retained_manifest_versions: Vec<u64>,
        expected_candidate_keys: Vec<ObjectKey>,
        actual_candidate_keys: Vec<ObjectKey>,
    },
    #[error("invalid garbage collection run evidence `{object_key}`: {reason}")]
    InvalidGarbageCollectionRunEvidence {
        object_key: ObjectKey,
        reason: String,
    },
    #[error("garbage collection must retain at least one manifest")]
    InvalidGarbageCollectionPolicy,
    #[error("garbage collection retained manifest {0} is missing")]
    MissingGarbageCollectionRetainedManifest(u64),
    #[error(
        "garbage collection candidate `{object_key}` is still referenced by manifest {checkpoint_version}"
    )]
    GarbageCollectionCandidateStillReferenced {
        object_key: ObjectKey,
        checkpoint_version: u64,
    },
    #[error("ownership epoch record `{0}` is missing")]
    MissingOwnershipEpochRecord(ObjectKey),
    #[error("ownership epoch record conflict at `{object_key}`")]
    OwnershipEpochRecordConflict { object_key: ObjectKey },
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
    #[error("invalid SlateDB state reference `{object_key}`: {reason}")]
    InvalidSlateDbStateRef {
        object_key: ObjectKey,
        reason: &'static str,
    },
    #[error("SlateDB state payload mismatch for `{object_key}`: expected {expected_bytes} bytes/{expected_digest}, actual {actual_bytes} bytes/{actual_digest}")]
    SlateDbStatePayloadMismatch {
        object_key: ObjectKey,
        expected_digest: String,
        actual_digest: String,
        expected_bytes: u64,
        actual_bytes: u64,
    },
    #[error("production manifest state object `{object_id}` must reference a SlateDB checkpoint, found `{ref_type:?}`")]
    ProductionStateRefNotSlateDbCheckpoint {
        object_id: String,
        ref_type: StateRefType,
    },
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

    /// Constructs a checkpoint publisher after validating every authoritative
    /// namespace used by checkpoint publication, GC retention, and admin
    /// inspection evidence.
    pub fn new_authoritative(
        store: Arc<dyn ObjectStore>,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, CheckpointPublishError> {
        validate_checkpoint_publisher_authoritative_namespaces(capabilities)?;

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
    /// object-store capability validation. Production callers should use
    /// [`Self::with_slatedb_state_store_authoritative`] so both checkpoint and
    /// state namespaces are validated from shared startup evidence.
    pub async fn with_slatedb_state_store(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<Path>,
    ) -> Result<Self, CheckpointPublishError> {
        let state_store = SlateDbStateStore::open(db_path, Arc::clone(&store)).await?;

        Ok(Self::with_state_store(store, Arc::new(state_store)))
    }

    /// Constructs a checkpoint publisher with a SlateDB state store after
    /// validating one supplied object-store profile. This remains useful for
    /// bootstrap or compatibility callers that do not yet have namespace-scoped
    /// startup evidence; production callers should use
    /// [`Self::with_slatedb_state_store_authoritative`].
    pub async fn with_slatedb_state_store_checked(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<Path>,
        profile: &ObjectStoreCapabilityProfile,
    ) -> Result<Self, CheckpointPublishError> {
        profile.validate_for_velorix_durability()?;

        Self::with_slatedb_state_store(store, db_path).await
    }

    /// Constructs a production checkpoint publisher with a SlateDB state store
    /// after validating both checkpoint-manifest and state-substrate
    /// authoritative namespaces from the shared startup capability evidence.
    pub async fn with_slatedb_state_store_authoritative(
        store: Arc<dyn ObjectStore>,
        db_path: impl Into<Path>,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, CheckpointPublishError> {
        capabilities.validate_namespace(AuthoritativeNamespace::Checkpoint)?;
        capabilities.validate_namespace(AuthoritativeNamespace::State)?;
        capabilities.validate_namespace(AuthoritativeNamespace::CheckpointIndex)?;
        capabilities.validate_namespace(AuthoritativeNamespace::CheckpointLifecycle)?;
        capabilities.validate_namespace(AuthoritativeNamespace::CheckpointRetention)?;
        capabilities.validate_namespace(AuthoritativeNamespace::CheckpointGcTransition)?;
        capabilities.validate_namespace(AuthoritativeNamespace::CheckpointRecovery)?;
        capabilities.validate_namespace(AuthoritativeNamespace::GcRuns)?;

        Self::with_slatedb_state_store(store, db_path).await
    }

    pub async fn write_state_object(
        &self,
        state: &StateObjectWrite,
    ) -> Result<StateObjectRef, CheckpointPublishError> {
        self.state_store.write_state_object(state).await
    }

    pub fn produced_state_ref_type(&self) -> StateRefType {
        self.state_store.produced_state_ref_type()
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

    pub async fn write_state_object_fenced_production(
        &self,
        state: &StateObjectWrite,
        stream_id: &str,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<StateObjectRef, CheckpointPublishError> {
        self.validate_state_write_owner_claim(state, owner_claim)?;
        self.validate_production_owner_claim_current(stream_id, state.partition_id(), owner_claim)
            .await?;

        let ref_type = self.produced_state_ref_type();
        if ref_type != StateRefType::SlateDbCheckpoint {
            return Err(
                CheckpointPublishError::ProductionStateRefNotSlateDbCheckpoint {
                    object_id: state.object_id().to_string(),
                    ref_type,
                },
            );
        }

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

    pub async fn write_output_object_fenced_production(
        &self,
        output: &OutputObjectWrite,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<OutputObjectRef, CheckpointPublishError> {
        self.validate_output_write_owner_claim(output, owner_claim)?;
        self.validate_production_owner_claim_current(
            output.stream_id(),
            output.partition_id(),
            owner_claim,
        )
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
            .put_opts(
                &path,
                Bytes::from(bytes.clone()).into(),
                PutMode::Create.into(),
            )
            .await;

        match result {
            Ok(_) => {
                self.best_effort_publish_lifecycle_record(manifest, &bytes)
                    .await;
                self.best_effort_publish_latest_candidate_marker(manifest, &bytes)
                    .await;
                Ok(())
            }
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

    pub async fn publish_manifest_fenced_production(
        &self,
        manifest: &CheckpointManifest,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<(), CheckpointPublishError> {
        manifest.validate()?;
        manifest.validate_owner_claim(owner_claim)?;
        self.validate_fenced_manifest_progress_claimed(manifest, owner_claim)?;
        self.validate_manifest_production_owner_claims_current(manifest, owner_claim)
            .await?;
        self.validate_production_state_refs_are_slatedb_checkpoints(manifest)?;

        self.publish_manifest(manifest).await
    }

    pub async fn create_ownership_epoch_record(
        &self,
        record: &OwnershipEpochRecord,
    ) -> Result<ObjectKey, CheckpointPublishError> {
        record.validate()?;
        let object_key = record.object_key()?;
        let path = Path::from(object_key.as_str());
        let bytes = serde_json::to_vec(record)?;
        let result = self
            .store
            .put_opts(&path, Bytes::from(bytes).into(), PutMode::Create.into())
            .await;

        match result {
            Ok(_) => Ok(object_key),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self.read_ownership_epoch_record_object(&object_key).await?;
                if existing == *record {
                    Ok(object_key)
                } else {
                    Err(CheckpointPublishError::OwnershipEpochRecordConflict { object_key })
                }
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn read_ownership_epoch_record(
        &self,
        stream_id: &str,
        partition_id: u32,
        owner_epoch: u64,
    ) -> Result<OwnershipEpochRecord, CheckpointPublishError> {
        let object_key = ObjectKey::ownership_epoch_record(stream_id, partition_id, owner_epoch)?;
        self.read_ownership_epoch_record_object(&object_key).await
    }

    pub async fn has_newer_ownership_epoch_record(
        &self,
        stream_id: &str,
        partition_id: u32,
        owner_epoch: u64,
    ) -> Result<bool, CheckpointPublishError> {
        Ok(self
            .list_ownership_epoch_records(stream_id, partition_id)
            .await?
            .into_iter()
            .any(|record| record.owner_epoch > owner_epoch))
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

    pub async fn read_checkpoint_lifecycle_record(
        &self,
        checkpoint_version: u64,
    ) -> Result<CheckpointLifecycleRecord, CheckpointPublishError> {
        let object_key = ObjectKey::checkpoint_lifecycle_record(checkpoint_version);
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;
        let record = serde_json::from_slice::<CheckpointLifecycleRecord>(&bytes)?;
        self.validate_lifecycle_record(&object_key, &record)?;

        Ok(record)
    }

    pub async fn read_checkpoint_retention_record(
        &self,
        checkpoint_version: u64,
    ) -> Result<CheckpointRetentionRecordV1, CheckpointPublishError> {
        let object_key = ObjectKey::checkpoint_retention_record(checkpoint_version);
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;
        let record = serde_json::from_slice::<CheckpointRetentionRecordV1>(&bytes)?;
        self.validate_retention_record(&object_key, &record)?;

        Ok(record)
    }

    pub async fn read_checkpoint_gc_transition_record(
        &self,
        checkpoint_version: u64,
        transition_id: &str,
    ) -> Result<CheckpointGcTransitionRecordV1, CheckpointPublishError> {
        let object_key =
            ObjectKey::checkpoint_gc_transition_record(checkpoint_version, transition_id)?;
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;
        let record = serde_json::from_slice::<CheckpointGcTransitionRecordV1>(&bytes)?;
        self.validate_gc_transition_record(&object_key, &record)?;
        let manifest_bytes = self
            .store
            .get(&Path::from(record.manifest_key.as_str()))
            .await?
            .bytes()
            .await?;
        if record.manifest_digest != manifest_digest(&manifest_bytes) {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint GC transition manifest digest mismatch for checkpoint {} transition {}: expected {}, actual {}",
                    record.checkpoint_version,
                    record.transition_id,
                    manifest_digest(&manifest_bytes),
                    record.manifest_digest
                )),
            ));
        }
        let gc_run_bytes = self
            .store
            .get(&Path::from(record.gc_run_key.as_str()))
            .await?
            .bytes()
            .await?;
        let expected_gc_run_digest =
            checkpoint_admin_record_digest("velorix.gc-run.v1", &gc_run_bytes);
        if record.gc_run_digest != expected_gc_run_digest {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint GC transition run digest mismatch for checkpoint {} transition {}: expected {}, actual {}",
                    record.checkpoint_version,
                    record.transition_id,
                    expected_gc_run_digest,
                    record.gc_run_digest
                )),
            ));
        }
        let run = serde_json::from_slice::<GarbageCollectionRunV1>(&gc_run_bytes)?;
        Self::validate_garbage_collection_run_evidence(
            &record.gc_run_key,
            &record.gc_run_id,
            &run,
        )?;
        let retention_record_bytes = self
            .store
            .get(&Path::from(record.retention_record_key.as_str()))
            .await?
            .bytes()
            .await?;
        let expected_retention_digest = checkpoint_admin_record_digest(
            "velorix.checkpoint-retention.v1",
            &retention_record_bytes,
        );
        if record.retention_record_digest != expected_retention_digest {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint GC transition retention digest mismatch for checkpoint {} transition {}: expected {}, actual {}",
                    record.checkpoint_version,
                    record.transition_id,
                    expected_retention_digest,
                    record.retention_record_digest
                )),
            ));
        }
        let retention_record =
            serde_json::from_slice::<CheckpointRetentionRecordV1>(&retention_record_bytes)?;
        self.validate_retention_record(&record.retention_record_key, &retention_record)?;
        if !gc_transition_matches_retention_record(&record, &retention_record) {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint GC transition causal fields do not match retention record for checkpoint {} transition {}",
                    record.checkpoint_version, record.transition_id
                )),
            ));
        }

        Ok(record)
    }

    pub async fn write_checkpoint_recovery_transition_record(
        &self,
        checkpoint_version: u64,
        recovery_mode: CheckpointRecoveryMode,
        replay_checkpoint_count: usize,
        replayed_batch_count: usize,
    ) -> Result<CheckpointRecoveryTransitionRecordV1, CheckpointPublishError> {
        self.write_checkpoint_recovery_transition_record_with_id(
            checkpoint_version,
            recovery_transition_id_now(),
            recovery_mode,
            replay_checkpoint_count,
            replayed_batch_count,
        )
        .await
    }

    pub async fn write_checkpoint_recovery_transition_record_with_id(
        &self,
        checkpoint_version: u64,
        transition_id: String,
        recovery_mode: CheckpointRecoveryMode,
        replay_checkpoint_count: usize,
        replayed_batch_count: usize,
    ) -> Result<CheckpointRecoveryTransitionRecordV1, CheckpointPublishError> {
        let manifest_key = ObjectKey::checkpoint_manifest(checkpoint_version);
        let manifest_path = Path::from(manifest_key.as_str());
        let manifest_bytes = self.store.get(&manifest_path).await?.bytes().await?;
        let manifest = self
            .read_published_checkpoint_manifest(checkpoint_version)
            .await?;
        let record = CheckpointRecoveryTransitionRecordV1::for_manifest(
            &manifest,
            &manifest_bytes,
            transition_id,
            recovery_mode,
            replay_checkpoint_count,
            replayed_batch_count,
            marker_updated_at_now(),
        );
        let object_key = ObjectKey::checkpoint_recovery_transition_record(
            checkpoint_version,
            &record.transition_id,
        )?;
        let bytes = serde_json::to_vec(&record)?;
        let result = self
            .store
            .put_opts(
                &Path::from(object_key.as_str()),
                Bytes::from(bytes).into(),
                PutMode::Create.into(),
            )
            .await;

        match result {
            Ok(_) => Ok(record),
            Err(object_store::Error::AlreadyExists { .. }) => {
                Err(CheckpointPublishError::CheckpointRecoveryTransitionAlreadyExists(object_key))
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn read_checkpoint_recovery_transition_record(
        &self,
        checkpoint_version: u64,
        transition_id: &str,
    ) -> Result<CheckpointRecoveryTransitionRecordV1, CheckpointPublishError> {
        let object_key =
            ObjectKey::checkpoint_recovery_transition_record(checkpoint_version, transition_id)?;
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;
        let record = serde_json::from_slice::<CheckpointRecoveryTransitionRecordV1>(&bytes)?;
        self.validate_recovery_transition_record(&object_key, &record)?;
        let manifest_bytes = self
            .store
            .get(&Path::from(record.manifest_key.as_str()))
            .await?
            .bytes()
            .await?;
        if record.manifest_digest != manifest_digest(&manifest_bytes) {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint recovery transition manifest digest mismatch for checkpoint {} transition {}: expected {}, actual {}",
                    record.checkpoint_version,
                    record.transition_id,
                    manifest_digest(&manifest_bytes),
                    record.manifest_digest
                )),
            ));
        }

        Ok(record)
    }

    pub async fn inspect_checkpoints(
        &self,
    ) -> Result<CheckpointAdminInspection, CheckpointPublishError> {
        let objects = self
            .store
            .list(Some(&Path::from(CHECKPOINT_PREFIX)))
            .try_collect::<Vec<_>>()
            .await?;

        let mut manifest_objects = objects
            .into_iter()
            .map(|object| {
                let object_key = ObjectKey::parse(object.location.to_string())?;
                let checkpoint_version = checkpoint_version_from_manifest_key(&object_key)?;
                Ok((checkpoint_version, object_key, object.location))
            })
            .collect::<Result<Vec<_>, CheckpointPublishError>>()?;
        manifest_objects.sort_by_key(|(checkpoint_version, _, _)| *checkpoint_version);

        let mut lineage_manifests = HashMap::new();
        let mut inspections = Vec::with_capacity(manifest_objects.len());
        let mut latest_valid_checkpoint = None;

        for (checkpoint_version, manifest_key, location) in manifest_objects {
            let inspection = match self
                .inspect_checkpoint_manifest(
                    checkpoint_version,
                    manifest_key.clone(),
                    &location,
                    &lineage_manifests,
                )
                .await
            {
                Ok(inspectable) => {
                    let checkpoint_version = inspectable.manifest.checkpoint_version;
                    let lifecycle_status = inspectable.lifecycle_status;
                    let gc_transition_records = inspectable.gc_transition_records;
                    let retention_record = inspectable.retention_record;
                    let recovery_transition_records = inspectable.recovery_transition_records;
                    let payload_status = inspectable.payload_status;
                    lineage_manifests.insert(
                        inspectable.manifest.checkpoint_version,
                        inspectable.manifest,
                    );

                    match payload_status {
                        Ok(()) => {
                            latest_valid_checkpoint = Some(checkpoint_version);
                            CheckpointManifestInspection {
                                checkpoint_version,
                                manifest_key,
                                lifecycle_status,
                                gc_transition_records,
                                retention_record,
                                recovery_transition_records,
                                status: CheckpointManifestInspectionStatus::Valid,
                            }
                        }
                        Err(reason) => CheckpointManifestInspection {
                            checkpoint_version,
                            manifest_key,
                            lifecycle_status,
                            gc_transition_records,
                            retention_record,
                            recovery_transition_records,
                            status: CheckpointManifestInspectionStatus::Invalid { reason },
                        },
                    }
                }
                Err(reason) => CheckpointManifestInspection {
                    checkpoint_version,
                    manifest_key,
                    lifecycle_status: None,
                    gc_transition_records: vec![],
                    retention_record: None,
                    recovery_transition_records: vec![],
                    status: CheckpointManifestInspectionStatus::Invalid { reason },
                },
            };
            inspections.push(inspection);
        }

        Ok(CheckpointAdminInspection {
            latest_valid_checkpoint,
            manifests: inspections,
        })
    }

    pub async fn read_published_checkpoint_manifest(
        &self,
        checkpoint_version: u64,
    ) -> Result<CheckpointManifest, CheckpointPublishError> {
        let manifest_key = ObjectKey::checkpoint_manifest(checkpoint_version);
        let manifest_path = Path::from(manifest_key.as_str());
        let manifest_bytes = self.store.get(&manifest_path).await?.bytes().await?;
        let manifest = serde_json::from_slice::<CheckpointManifest>(&manifest_bytes)?;
        manifest.validate()?;

        let body_key = manifest.object_key();
        if manifest_key != body_key {
            return Err(CheckpointPublishError::ManifestKeyMismatch {
                object_key: manifest_key,
                body_key,
            });
        }
        if manifest.checkpoint_version != checkpoint_version {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint manifest key version {checkpoint_version} does not match body version {}",
                    manifest.checkpoint_version
                )),
            ));
        }

        self.validate_parent_manifest_visible(&manifest).await?;
        self.validate_state_objects_exist(&manifest).await?;
        self.validate_output_objects_exist(&manifest).await?;

        let lifecycle = self
            .read_checkpoint_lifecycle_record(checkpoint_version)
            .await?;
        let expected_digest = manifest_digest(&manifest_bytes);
        if lifecycle.status != CheckpointLifecycleStatus::Published {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint {checkpoint_version} is not published"
                )),
            ));
        }
        if lifecycle.manifest_digest != expected_digest {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint lifecycle manifest digest mismatch for checkpoint {checkpoint_version}: expected {expected_digest}, actual {}",
                    lifecycle.manifest_digest
                )),
            ));
        }

        Ok(manifest)
    }

    pub async fn plan_garbage_collection(
        &self,
        policy: GarbageCollectionPolicy,
    ) -> Result<GarbageCollectionPlan, CheckpointPublishError> {
        if policy.retain_latest_manifests == 0 {
            return Err(CheckpointPublishError::InvalidGarbageCollectionPolicy);
        }

        let manifests = self.list_published_manifests().await?;
        let retained_from = manifests
            .len()
            .saturating_sub(policy.retain_latest_manifests);
        let retained_manifests = &manifests[retained_from..];
        let retired_manifests = &manifests[..retained_from];

        let mut referenced = HashSet::new();
        let mut retained_manifest_versions = Vec::with_capacity(retained_manifests.len());
        for manifest in retained_manifests {
            retained_manifest_versions.push(manifest.checkpoint_version);
            referenced.extend(
                manifest
                    .state_objects
                    .iter()
                    .map(|state_ref| state_ref.object_key.clone()),
            );
            referenced.extend(
                manifest
                    .output_objects
                    .iter()
                    .map(|output_ref| output_ref.object_key.clone()),
            );
        }

        let mut raw_state_referenced = referenced.clone();
        raw_state_referenced.extend(Self::slatedb_state_keys(&manifests));

        let mut candidates = Vec::new();
        self.add_unreferenced_candidates(
            STATE_PREFIX,
            GarbageCollectionCandidateKind::RawStateObject,
            &raw_state_referenced,
            &mut candidates,
        )
        .await?;
        Self::add_retired_slatedb_state_candidates(retired_manifests, &referenced, &mut candidates);
        self.add_unreferenced_candidates(
            OUTPUT_PREFIX,
            GarbageCollectionCandidateKind::OutputObject,
            &referenced,
            &mut candidates,
        )
        .await?;
        candidates.sort();

        Ok(GarbageCollectionPlan {
            retained_manifest_versions,
            candidates,
        })
    }

    pub async fn execute_garbage_collection_plan(
        &self,
        plan: &GarbageCollectionPlan,
    ) -> Result<GarbageCollectionReport, CheckpointPublishError> {
        let referenced = self
            .referenced_garbage_collection_candidates_for_plan(plan)
            .await?;
        for candidate in &plan.candidates {
            if !candidate.kind.matches_key(&candidate.object_key) {
                continue;
            }
            if let Some(checkpoint_version) = referenced.get(&candidate.object_key) {
                return Err(
                    CheckpointPublishError::GarbageCollectionCandidateStillReferenced {
                        object_key: candidate.object_key.clone(),
                        checkpoint_version: *checkpoint_version,
                    },
                );
            }
        }

        let slatedb_state_refs = self.slatedb_state_refs_for_plan(plan).await?;
        let mut deleted = Vec::new();
        let mut skipped = Vec::new();

        for candidate in &plan.candidates {
            if !candidate.kind.matches_key(&candidate.object_key) {
                skipped.push(candidate.clone());
                continue;
            }

            if candidate.kind == GarbageCollectionCandidateKind::SlateDbStateRef {
                let Some(state_ref) = slatedb_state_refs.get(&candidate.object_key) else {
                    skipped.push(candidate.clone());
                    continue;
                };

                if self.state_store.release_state_object(state_ref).await? {
                    deleted.push(candidate.clone());
                } else {
                    skipped.push(candidate.clone());
                }
                continue;
            }

            let path = Path::from(candidate.object_key.as_str());
            match self.store.delete(&path).await {
                Ok(()) => deleted.push(candidate.clone()),
                Err(object_store::Error::NotFound { .. }) => skipped.push(candidate.clone()),
                Err(err) => return Err(err.into()),
            }
        }

        Ok(GarbageCollectionReport { deleted, skipped })
    }

    pub async fn execute_garbage_collection_plan_with_evidence(
        &self,
        run_id: &str,
        policy: GarbageCollectionPolicy,
        plan: &GarbageCollectionPlan,
    ) -> Result<GarbageCollectionRunV1, CheckpointPublishError> {
        let object_key = ObjectKey::garbage_collection_run(run_id)?;
        let object_path = Path::from(object_key.as_str());
        match self.store.head(&object_path).await {
            Ok(_) => {
                return Err(CheckpointPublishError::GarbageCollectionRunAlreadyExists(
                    object_key,
                ));
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(err.into()),
        }

        let expected_plan = self.plan_garbage_collection(policy).await?;
        if expected_plan != *plan {
            return Err(
                CheckpointPublishError::GarbageCollectionPlanPolicyMismatch {
                    retain_latest_manifests: policy.retain_latest_manifests,
                    expected_retained_manifest_versions: expected_plan
                        .retained_manifest_versions
                        .clone(),
                    actual_retained_manifest_versions: plan.retained_manifest_versions.clone(),
                    expected_candidate_keys: garbage_collection_candidate_keys(&expected_plan),
                    actual_candidate_keys: garbage_collection_candidate_keys(plan),
                },
            );
        }
        self.validate_no_conflicting_retention_records(run_id, policy, plan)
            .await?;
        self.validate_no_conflicting_gc_transition_records(run_id, policy, plan)
            .await?;

        let report = self.execute_garbage_collection_plan(plan).await?;
        let run = GarbageCollectionRunV1 {
            schema_version: GC_RUN_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            policy,
            plan: plan.clone(),
            report,
        };
        let bytes = serde_json::to_vec(&run)?;
        self.store
            .put_opts(
                &object_path,
                Bytes::from(bytes).into(),
                PutMode::Create.into(),
            )
            .await?;

        let restored = self.read_garbage_collection_run_evidence(run_id).await?;
        if restored != run {
            return Err(
                CheckpointPublishError::InvalidGarbageCollectionRunEvidence {
                    object_key,
                    reason: "persisted body does not match executed run".to_string(),
                },
            );
        }
        self.publish_retention_records_for_run(&restored).await?;

        Ok(restored)
    }

    pub async fn read_garbage_collection_run_evidence(
        &self,
        run_id: &str,
    ) -> Result<GarbageCollectionRunV1, CheckpointPublishError> {
        let object_key = ObjectKey::garbage_collection_run(run_id)?;
        let bytes = self
            .store
            .get(&Path::from(object_key.as_str()))
            .await?
            .bytes()
            .await?;
        let run: GarbageCollectionRunV1 = serde_json::from_slice(&bytes)?;

        Self::validate_garbage_collection_run_evidence(&object_key, run_id, &run)?;

        Ok(run)
    }

    pub async fn verify_garbage_collection_run_retention_evidence(
        &self,
        run_id: &str,
    ) -> Result<GarbageCollectionRunV1, CheckpointPublishError> {
        let object_key = ObjectKey::garbage_collection_run(run_id)?;
        let run = self.read_garbage_collection_run_evidence(run_id).await?;
        self.verify_garbage_collection_run_is_listed(&object_key)
            .await?;
        self.verify_retention_records_for_garbage_collection_run(&object_key, &run)
            .await?;

        Ok(run)
    }

    async fn verify_garbage_collection_run_is_listed(
        &self,
        object_key: &ObjectKey,
    ) -> Result<(), CheckpointPublishError> {
        let objects = self
            .store
            .list(Some(&Path::from(GC_RUN_PREFIX)))
            .try_collect::<Vec<_>>()
            .await?;
        let expected = Path::from(object_key.as_str());
        if objects.iter().any(|object| object.location == expected) {
            return Ok(());
        }

        Err(
            CheckpointPublishError::InvalidGarbageCollectionRunEvidence {
                object_key: object_key.clone(),
                reason: "run evidence is not visible in v1/gc-runs listing".to_string(),
            },
        )
    }

    async fn verify_retention_records_for_garbage_collection_run(
        &self,
        object_key: &ObjectKey,
        run: &GarbageCollectionRunV1,
    ) -> Result<(), CheckpointPublishError> {
        let deleted_keys = run
            .report
            .deleted
            .iter()
            .map(|candidate| candidate.object_key.clone())
            .collect::<HashSet<_>>();
        for expected in self
            .retention_records_for_deleted_keys(
                &run.run_id,
                run.policy,
                &run.plan.retained_manifest_versions,
                &deleted_keys,
            )
            .await?
        {
            let existing = match self
                .read_checkpoint_retention_record(expected.checkpoint_version)
                .await
            {
                Ok(existing) => existing,
                Err(CheckpointPublishError::ObjectStore(object_store::Error::NotFound {
                    ..
                })) => {
                    return Err(
                        CheckpointPublishError::InvalidGarbageCollectionRunEvidence {
                            object_key: object_key.clone(),
                            reason: format!(
                                "missing checkpoint retention record for checkpoint {}",
                                expected.checkpoint_version
                            ),
                        },
                    );
                }
                Err(err) => return Err(err),
            };

            if !same_retention_causal_fields(&existing, &expected) {
                return Err(
                    CheckpointPublishError::InvalidGarbageCollectionRunEvidence {
                        object_key: object_key.clone(),
                        reason: format!(
                        "checkpoint retention record for checkpoint {} does not match run evidence",
                        expected.checkpoint_version
                    ),
                    },
                );
            }
        }

        Ok(())
    }

    fn validate_garbage_collection_run_evidence(
        object_key: &ObjectKey,
        expected_run_id: &str,
        run: &GarbageCollectionRunV1,
    ) -> Result<(), CheckpointPublishError> {
        if run.schema_version != GC_RUN_SCHEMA_VERSION {
            return Err(
                CheckpointPublishError::InvalidGarbageCollectionRunEvidence {
                    object_key: object_key.clone(),
                    reason: format!(
                        "unsupported schema_version {}, expected {}",
                        run.schema_version, GC_RUN_SCHEMA_VERSION
                    ),
                },
            );
        }
        if run.run_id != expected_run_id {
            return Err(
                CheckpointPublishError::InvalidGarbageCollectionRunEvidence {
                    object_key: object_key.clone(),
                    reason: format!(
                        "run_id {} does not match requested run_id {}",
                        run.run_id, expected_run_id
                    ),
                },
            );
        }
        if run.policy.retain_latest_manifests == 0 {
            return Err(
                CheckpointPublishError::InvalidGarbageCollectionRunEvidence {
                    object_key: object_key.clone(),
                    reason: "retain_latest_manifests must be greater than zero".to_string(),
                },
            );
        }
        for candidate in run.report.deleted.iter().chain(&run.report.skipped) {
            if !run.plan.candidates.contains(candidate) {
                return Err(
                    CheckpointPublishError::InvalidGarbageCollectionRunEvidence {
                        object_key: object_key.clone(),
                        reason: format!(
                            "report candidate `{}` is not in the recorded plan",
                            candidate.object_key
                        ),
                    },
                );
            }
        }

        Ok(())
    }

    async fn publish_retention_records_for_run(
        &self,
        run: &GarbageCollectionRunV1,
    ) -> Result<(), CheckpointPublishError> {
        let deleted = run
            .report
            .deleted
            .iter()
            .map(|candidate| candidate.object_key.clone())
            .collect::<HashSet<_>>();
        for record in self
            .retention_records_for_deleted_keys(
                &run.run_id,
                run.policy,
                &run.plan.retained_manifest_versions,
                &deleted,
            )
            .await?
        {
            self.write_checkpoint_retention_record(record.clone())
                .await?;
            self.write_payload_released_gc_transition_for_retention_record(record)
                .await?;
        }

        Ok(())
    }

    async fn validate_no_conflicting_retention_records(
        &self,
        run_id: &str,
        policy: GarbageCollectionPolicy,
        plan: &GarbageCollectionPlan,
    ) -> Result<(), CheckpointPublishError> {
        let candidate_keys = plan
            .candidates
            .iter()
            .map(|candidate| candidate.object_key.clone())
            .collect::<HashSet<_>>();
        for expected in self
            .retention_records_for_deleted_keys(
                run_id,
                policy,
                &plan.retained_manifest_versions,
                &candidate_keys,
            )
            .await?
        {
            let existing = match self
                .read_checkpoint_retention_record(expected.checkpoint_version)
                .await
            {
                Ok(existing) => existing,
                Err(CheckpointPublishError::ObjectStore(object_store::Error::NotFound {
                    ..
                })) => continue,
                Err(err) => return Err(err),
            };
            if !same_retention_causal_fields(&existing, &expected) {
                return Err(retention_conflict_error(expected.checkpoint_version));
            }
        }

        Ok(())
    }

    async fn validate_no_conflicting_gc_transition_records(
        &self,
        run_id: &str,
        policy: GarbageCollectionPolicy,
        plan: &GarbageCollectionPlan,
    ) -> Result<(), CheckpointPublishError> {
        let candidate_keys = plan
            .candidates
            .iter()
            .map(|candidate| candidate.object_key.clone())
            .collect::<HashSet<_>>();
        let transition_id = checkpoint_gc_transition_id_for_gc_run(run_id)?;
        for expected in self
            .retention_records_for_deleted_keys(
                run_id,
                policy,
                &plan.retained_manifest_versions,
                &candidate_keys,
            )
            .await?
        {
            let object_key = ObjectKey::checkpoint_gc_transition_record(
                expected.checkpoint_version,
                &transition_id,
            )?;
            match self.store.head(&Path::from(object_key.as_str())).await {
                Ok(_) => {
                    return Err(CheckpointPublishError::CheckpointGcTransitionConflict(
                        object_key,
                    ))
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(err) => return Err(err.into()),
            }
        }

        Ok(())
    }

    async fn retention_records_for_deleted_keys(
        &self,
        run_id: &str,
        policy: GarbageCollectionPolicy,
        retained_manifest_versions: &[u64],
        deleted_keys: &HashSet<ObjectKey>,
    ) -> Result<Vec<CheckpointRetentionRecordV1>, CheckpointPublishError> {
        let retained = retained_manifest_versions
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if deleted_keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        for manifest in self.list_published_manifests().await? {
            if retained.contains(&manifest.checkpoint_version) {
                continue;
            }
            let manifest_deleted_keys = manifest
                .state_objects
                .iter()
                .map(|state_ref| &state_ref.object_key)
                .chain(
                    manifest
                        .output_objects
                        .iter()
                        .map(|output_ref| &output_ref.object_key),
                )
                .filter(|object_key| deleted_keys.contains(*object_key))
                .cloned()
                .collect::<Vec<_>>();
            if manifest_deleted_keys.is_empty() {
                continue;
            }

            let manifest_key = manifest.object_key();
            let manifest_bytes = self
                .store
                .get(&Path::from(manifest_key.as_str()))
                .await?
                .bytes()
                .await?;
            let record = CheckpointRetentionRecordV1::for_manifest(
                &manifest,
                &manifest_bytes,
                run_id.to_string(),
                policy,
                retained_manifest_versions.to_vec(),
                manifest_deleted_keys,
                marker_updated_at_now(),
            );
            records.push(record);
        }

        Ok(records)
    }

    async fn write_checkpoint_retention_record(
        &self,
        record: CheckpointRetentionRecordV1,
    ) -> Result<(), CheckpointPublishError> {
        let object_key = ObjectKey::checkpoint_retention_record(record.checkpoint_version);
        self.validate_retention_record(&object_key, &record)?;
        let bytes = serde_json::to_vec(&record)?;
        let path = Path::from(object_key.as_str());

        match self
            .store
            .put_opts(
                &path,
                Bytes::from(bytes.clone()).into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self
                    .read_checkpoint_retention_record(record.checkpoint_version)
                    .await?;
                if same_retention_causal_fields(&existing, &record) {
                    Ok(())
                } else {
                    Err(retention_conflict_error(record.checkpoint_version))
                }
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn write_payload_released_gc_transition_for_retention_record(
        &self,
        retention: CheckpointRetentionRecordV1,
    ) -> Result<(), CheckpointPublishError> {
        let transition_id = checkpoint_gc_transition_id_for_gc_run(&retention.gc_run_id)?;
        let gc_run_key = ObjectKey::garbage_collection_run(&retention.gc_run_id)?;
        let gc_run_bytes = self
            .store
            .get(&Path::from(gc_run_key.as_str()))
            .await?
            .bytes()
            .await?;
        let retention_record_key =
            ObjectKey::checkpoint_retention_record(retention.checkpoint_version);
        let retention_record_bytes = self
            .store
            .get(&Path::from(retention_record_key.as_str()))
            .await?
            .bytes()
            .await?;
        let record = CheckpointGcTransitionRecordV1::payload_released_from_retention_record(
            &retention,
            transition_id,
            checkpoint_admin_record_digest("velorix.gc-run.v1", &gc_run_bytes),
            checkpoint_admin_record_digest(
                "velorix.checkpoint-retention.v1",
                &retention_record_bytes,
            ),
            marker_updated_at_now(),
            "checkpoint-publisher-gc".to_string(),
        );
        let object_key = ObjectKey::checkpoint_gc_transition_record(
            record.checkpoint_version,
            &record.transition_id,
        )?;
        self.validate_gc_transition_record(&object_key, &record)?;
        let bytes = serde_json::to_vec(&record)?;

        match self
            .store
            .put_opts(
                &Path::from(object_key.as_str()),
                Bytes::from(bytes).into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let Ok(existing) = self
                    .read_checkpoint_gc_transition_record(
                        record.checkpoint_version,
                        &record.transition_id,
                    )
                    .await
                else {
                    return Err(CheckpointPublishError::CheckpointGcTransitionConflict(
                        object_key,
                    ));
                };
                if same_gc_transition_causal_fields(&existing, &record) {
                    return Ok(());
                }

                Err(CheckpointPublishError::CheckpointGcTransitionConflict(
                    object_key,
                ))
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn referenced_garbage_collection_candidates_for_plan(
        &self,
        plan: &GarbageCollectionPlan,
    ) -> Result<HashMap<ObjectKey, u64>, CheckpointPublishError> {
        let manifests = self.list_published_manifests().await?;
        let manifests_by_version = manifests
            .iter()
            .map(|manifest| (manifest.checkpoint_version, manifest))
            .collect::<HashMap<_, _>>();
        let mut referenced = HashMap::new();

        for checkpoint_version in &plan.retained_manifest_versions {
            let manifest = manifests_by_version.get(checkpoint_version).ok_or(
                CheckpointPublishError::MissingGarbageCollectionRetainedManifest(
                    *checkpoint_version,
                ),
            )?;
            Self::add_manifest_referenced_gc_keys(manifest, &mut referenced);
        }

        let newest_plan_retained = plan.retained_manifest_versions.iter().copied().max();
        for manifest in manifests {
            if newest_plan_retained.is_none_or(|version| manifest.checkpoint_version > version) {
                Self::add_manifest_referenced_gc_keys(&manifest, &mut referenced);
            }
        }

        Ok(referenced)
    }

    async fn slatedb_state_refs_for_plan(
        &self,
        plan: &GarbageCollectionPlan,
    ) -> Result<HashMap<ObjectKey, StateObjectRef>, CheckpointPublishError> {
        let slate_candidates = plan
            .candidates
            .iter()
            .filter(|candidate| candidate.kind == GarbageCollectionCandidateKind::SlateDbStateRef)
            .map(|candidate| candidate.object_key.clone())
            .collect::<HashSet<_>>();
        if slate_candidates.is_empty() {
            return Ok(HashMap::new());
        }

        let retained = plan
            .retained_manifest_versions
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut refs = HashMap::new();
        for manifest in self.list_published_manifests().await? {
            if retained.contains(&manifest.checkpoint_version) {
                continue;
            }
            for state_ref in manifest.state_objects {
                if state_ref.ref_type == StateRefType::SlateDbCheckpoint
                    && slate_candidates.contains(&state_ref.object_key)
                {
                    refs.entry(state_ref.object_key.clone())
                        .or_insert(state_ref);
                }
            }
        }

        Ok(refs)
    }

    fn add_manifest_referenced_gc_keys(
        manifest: &CheckpointManifest,
        referenced: &mut HashMap<ObjectKey, u64>,
    ) {
        referenced.extend(
            manifest
                .state_objects
                .iter()
                .map(|state_ref| (state_ref.object_key.clone(), manifest.checkpoint_version)),
        );
        referenced.extend(
            manifest
                .output_objects
                .iter()
                .map(|output_ref| (output_ref.object_key.clone(), manifest.checkpoint_version)),
        );
    }

    fn slatedb_state_keys(manifests: &[CheckpointManifest]) -> HashSet<ObjectKey> {
        manifests
            .iter()
            .flat_map(|manifest| &manifest.state_objects)
            .filter(|state_ref| state_ref.ref_type == StateRefType::SlateDbCheckpoint)
            .map(|state_ref| state_ref.object_key.clone())
            .collect()
    }

    fn add_retired_slatedb_state_candidates(
        manifests: &[CheckpointManifest],
        referenced: &HashSet<ObjectKey>,
        candidates: &mut Vec<GarbageCollectionCandidate>,
    ) {
        let mut seen = HashSet::new();
        for manifest in manifests {
            for state_ref in &manifest.state_objects {
                if state_ref.ref_type == StateRefType::SlateDbCheckpoint
                    && !referenced.contains(&state_ref.object_key)
                    && seen.insert(state_ref.object_key.clone())
                {
                    candidates.push(GarbageCollectionCandidate {
                        object_key: state_ref.object_key.clone(),
                        kind: GarbageCollectionCandidateKind::SlateDbStateRef,
                    });
                }
            }
        }
    }

    pub async fn latest_manifest(
        &self,
    ) -> Result<Option<CheckpointManifest>, CheckpointPublishError> {
        if let Some(manifest) = self.latest_manifest_from_candidate_marker().await? {
            return Ok(Some(manifest));
        }

        let latest = self
            .list_published_manifests()
            .await?
            .into_iter()
            .max_by_key(|manifest| manifest.checkpoint_version);
        if let Some(manifest) = &latest {
            self.validate_state_objects_exist(manifest).await?;
            self.validate_output_objects_exist(manifest).await?;
        }

        Ok(latest)
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

    async fn add_unreferenced_candidates(
        &self,
        prefix: &str,
        kind: GarbageCollectionCandidateKind,
        referenced: &HashSet<ObjectKey>,
        candidates: &mut Vec<GarbageCollectionCandidate>,
    ) -> Result<(), CheckpointPublishError> {
        let objects = self
            .store
            .list(Some(&Path::from(prefix)))
            .try_collect::<Vec<_>>()
            .await?;

        for object in objects {
            let Ok(object_key) = ObjectKey::parse(object.location.to_string()) else {
                continue;
            };
            if kind.matches_key(&object_key) && !referenced.contains(&object_key) {
                candidates.push(GarbageCollectionCandidate { object_key, kind });
            }
        }

        Ok(())
    }

    fn validate_production_state_refs_are_slatedb_checkpoints(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<(), CheckpointPublishError> {
        for state_ref in &manifest.state_objects {
            if state_ref.ref_type != StateRefType::SlateDbCheckpoint {
                return Err(
                    CheckpointPublishError::ProductionStateRefNotSlateDbCheckpoint {
                        object_id: state_ref.object_id.clone(),
                        ref_type: state_ref.ref_type,
                    },
                );
            }
        }

        Ok(())
    }

    async fn best_effort_publish_latest_candidate_marker(
        &self,
        manifest: &CheckpointManifest,
        manifest_bytes: &[u8],
    ) {
        let marker =
            LatestCandidateMarker::for_manifest(manifest, manifest_bytes, marker_updated_at_now());
        let Ok(bytes) = serde_json::to_vec(&marker) else {
            return;
        };

        let marker_key = ObjectKey::checkpoint_latest_candidate_marker();
        let marker_path = Path::from(marker_key.as_str());
        let _ = self
            .store
            .put(&marker_path, Bytes::from(bytes).into())
            .await;
    }

    async fn best_effort_publish_lifecycle_record(
        &self,
        manifest: &CheckpointManifest,
        manifest_bytes: &[u8],
    ) {
        let record =
            CheckpointLifecycleRecord::published(manifest, manifest_bytes, marker_updated_at_now());
        let Ok(bytes) = serde_json::to_vec(&record) else {
            return;
        };

        let object_key = ObjectKey::checkpoint_lifecycle_record(manifest.checkpoint_version);
        let path = Path::from(object_key.as_str());
        let _ = self
            .store
            .put_opts(
                &path,
                Bytes::from(bytes.clone()).into(),
                PutMode::Create.into(),
            )
            .await;
    }

    async fn inspect_checkpoint_manifest(
        &self,
        checkpoint_version: u64,
        manifest_key: ObjectKey,
        location: &Path,
        lineage_manifests: &HashMap<u64, CheckpointManifest>,
    ) -> Result<InspectableCheckpointManifest, String> {
        let bytes = self
            .store
            .get(location)
            .await
            .map_err(|err| err.to_string())?
            .bytes()
            .await
            .map_err(|err| err.to_string())?;
        let manifest =
            serde_json::from_slice::<CheckpointManifest>(&bytes).map_err(|err| err.to_string())?;
        manifest.validate().map_err(|err| err.to_string())?;

        let body_key = manifest.object_key();
        if manifest_key != body_key {
            return Err(CheckpointPublishError::ManifestKeyMismatch {
                object_key: manifest_key,
                body_key,
            }
            .to_string());
        }
        if manifest.checkpoint_version != checkpoint_version {
            return Err(format!(
                "checkpoint manifest key version {checkpoint_version} does not match body version {}",
                manifest.checkpoint_version
            ));
        }
        if let Some(parent_checkpoint) = manifest.parent_checkpoint {
            let parent = lineage_manifests.get(&parent_checkpoint).ok_or_else(|| {
                CheckpointPublishError::MissingParentManifest {
                    checkpoint_version: manifest.checkpoint_version,
                    parent_checkpoint,
                }
                .to_string()
            })?;
            Self::validate_child_input_progress(&manifest, parent)
                .map_err(|err| err.to_string())?;
        }
        let payload_status = match self.validate_state_objects_exist(&manifest).await {
            Ok(()) => self
                .validate_output_objects_exist(&manifest)
                .await
                .map_err(|err| err.to_string()),
            Err(err) => Err(err.to_string()),
        };

        let lifecycle_status = self
            .read_checkpoint_lifecycle_record(checkpoint_version)
            .await
            .ok()
            .and_then(|record| {
                (record.manifest_digest == manifest_digest(&bytes)).then_some(record.status)
            });
        let retention_record = self
            .read_checkpoint_retention_record(checkpoint_version)
            .await
            .ok()
            .filter(|record| record.manifest_digest == manifest_digest(&bytes));
        let retention_record = match retention_record {
            Some(record) if self.retention_record_matches_gc_run(&record).await => Some(record),
            _ => None,
        };
        let gc_transition_records = self
            .list_checkpoint_gc_transition_records(&manifest, &bytes, retention_record.as_ref())
            .await
            .unwrap_or_default();
        let recovery_transition_records = self
            .list_checkpoint_recovery_transition_records(&manifest, &bytes)
            .await
            .map_err(|err| err.to_string())?;

        Ok(InspectableCheckpointManifest {
            manifest,
            lifecycle_status,
            gc_transition_records,
            retention_record,
            recovery_transition_records,
            payload_status,
        })
    }

    async fn list_checkpoint_gc_transition_records(
        &self,
        manifest: &CheckpointManifest,
        manifest_bytes: &[u8],
        retention_record: Option<&CheckpointRetentionRecordV1>,
    ) -> Result<Vec<CheckpointGcTransitionRecordV1>, CheckpointPublishError> {
        let Some(retention_record) = retention_record else {
            return Ok(Vec::new());
        };
        let prefix = checkpoint_gc_transition_prefix(manifest.checkpoint_version)?;
        let objects = self
            .store
            .list(Some(&Path::from(prefix.as_str())))
            .try_collect::<Vec<_>>()
            .await?;
        let expected_digest = manifest_digest(manifest_bytes);
        let expected_manifest_key = manifest.object_key();
        let gc_run_key = ObjectKey::garbage_collection_run(&retention_record.gc_run_id)?;
        let gc_run_bytes = self
            .store
            .get(&Path::from(gc_run_key.as_str()))
            .await?
            .bytes()
            .await?;
        let gc_run_digest = checkpoint_admin_record_digest("velorix.gc-run.v1", &gc_run_bytes);
        let retention_record_key =
            ObjectKey::checkpoint_retention_record(retention_record.checkpoint_version);
        let retention_record_bytes = self
            .store
            .get(&Path::from(retention_record_key.as_str()))
            .await?
            .bytes()
            .await?;
        let retention_record_digest = checkpoint_admin_record_digest(
            "velorix.checkpoint-retention.v1",
            &retention_record_bytes,
        );
        let mut records = Vec::new();

        for object in objects {
            let Ok(object_key) = ObjectKey::parse(object.location.to_string()) else {
                continue;
            };
            let bytes = self.store.get(&object.location).await?.bytes().await?;
            let Ok(record) = serde_json::from_slice::<CheckpointGcTransitionRecordV1>(&bytes)
            else {
                continue;
            };
            if self
                .validate_gc_transition_record(&object_key, &record)
                .is_err()
            {
                continue;
            }
            if record.manifest_key == expected_manifest_key
                && record.manifest_digest == expected_digest
                && record.gc_run_key == gc_run_key
                && record.gc_run_digest == gc_run_digest
                && record.retention_record_key == retention_record_key
                && record.retention_record_digest == retention_record_digest
                && gc_transition_matches_retention_record(&record, retention_record)
            {
                records.push(record);
            }
        }

        records.sort_by(|left, right| left.transition_id.cmp(&right.transition_id));
        Ok(records)
    }

    async fn list_checkpoint_recovery_transition_records(
        &self,
        manifest: &CheckpointManifest,
        manifest_bytes: &[u8],
    ) -> Result<Vec<CheckpointRecoveryTransitionRecordV1>, CheckpointPublishError> {
        let prefix = checkpoint_recovery_transition_prefix(manifest.checkpoint_version)?;
        let objects = self
            .store
            .list(Some(&Path::from(prefix.as_str())))
            .try_collect::<Vec<_>>()
            .await?;
        let expected_digest = manifest_digest(manifest_bytes);
        let expected_manifest_key = manifest.object_key();
        let mut records = Vec::new();

        for object in objects {
            let Ok(object_key) = ObjectKey::parse(object.location.to_string()) else {
                continue;
            };
            let bytes = self.store.get(&object.location).await?.bytes().await?;
            let Ok(record) = serde_json::from_slice::<CheckpointRecoveryTransitionRecordV1>(&bytes)
            else {
                continue;
            };
            if self
                .validate_recovery_transition_record(&object_key, &record)
                .is_err()
            {
                continue;
            }
            if record.manifest_key == expected_manifest_key
                && record.manifest_digest == expected_digest
            {
                records.push(record);
            }
        }

        records.sort_by(|left, right| left.transition_id.cmp(&right.transition_id));
        Ok(records)
    }

    fn validate_lifecycle_record(
        &self,
        object_key: &ObjectKey,
        record: &CheckpointLifecycleRecord,
    ) -> Result<(), CheckpointPublishError> {
        if !record.validate_schema() {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "unsupported checkpoint lifecycle schema version {}",
                    record.schema_version
                )),
            ));
        }
        if *object_key != ObjectKey::checkpoint_lifecycle_record(record.checkpoint_version) {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint lifecycle key `{object_key}` does not match body checkpoint {}",
                    record.checkpoint_version
                )),
            ));
        }
        if record.manifest_key != ObjectKey::checkpoint_manifest(record.checkpoint_version) {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint lifecycle manifest key `{}` does not match checkpoint {}",
                    record.manifest_key, record.checkpoint_version
                )),
            ));
        }

        Ok(())
    }

    fn validate_recovery_transition_record(
        &self,
        object_key: &ObjectKey,
        record: &CheckpointRecoveryTransitionRecordV1,
    ) -> Result<(), CheckpointPublishError> {
        if !record.validate_schema() {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "unsupported checkpoint recovery transition schema version {}",
                    record.schema_version
                )),
            ));
        }
        if *object_key
            != ObjectKey::checkpoint_recovery_transition_record(
                record.checkpoint_version,
                &record.transition_id,
            )?
        {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint recovery transition key `{object_key}` does not match body checkpoint {} transition {}",
                    record.checkpoint_version, record.transition_id
                )),
            ));
        }
        if record.manifest_key != ObjectKey::checkpoint_manifest(record.checkpoint_version) {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint recovery transition manifest key `{}` does not match checkpoint {}",
                    record.manifest_key, record.checkpoint_version
                )),
            ));
        }

        Ok(())
    }

    fn validate_gc_transition_record(
        &self,
        object_key: &ObjectKey,
        record: &CheckpointGcTransitionRecordV1,
    ) -> Result<(), CheckpointPublishError> {
        if !record.validate_schema() {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "unsupported checkpoint GC transition schema version {}",
                    record.schema_version
                )),
            ));
        }
        if *object_key
            != ObjectKey::checkpoint_gc_transition_record(
                record.checkpoint_version,
                &record.transition_id,
            )?
        {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint GC transition key `{object_key}` does not match body checkpoint {} transition {}",
                    record.checkpoint_version, record.transition_id
                )),
            ));
        }
        if record.manifest_key != ObjectKey::checkpoint_manifest(record.checkpoint_version) {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint GC transition manifest key `{}` does not match checkpoint {}",
                    record.manifest_key, record.checkpoint_version
                )),
            ));
        }
        if record.gc_run_key != ObjectKey::garbage_collection_run(&record.gc_run_id)? {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint GC transition run key `{}` does not match run id {}",
                    record.gc_run_key, record.gc_run_id
                )),
            ));
        }
        if record.retention_record_key
            != ObjectKey::checkpoint_retention_record(record.checkpoint_version)
        {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint GC transition retention key `{}` does not match checkpoint {}",
                    record.retention_record_key, record.checkpoint_version
                )),
            ));
        }
        if record.emitter.is_empty() || record.emitter.contains('/') {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint GC transition emitter `{}` is invalid",
                    record.emitter
                )),
            ));
        }

        Ok(())
    }

    async fn retention_record_matches_gc_run(&self, record: &CheckpointRetentionRecordV1) -> bool {
        let Ok(run) = self
            .read_garbage_collection_run_evidence(&record.gc_run_id)
            .await
        else {
            return false;
        };
        let deleted_keys = run
            .report
            .deleted
            .iter()
            .map(|candidate| candidate.object_key.clone())
            .collect::<HashSet<_>>();

        let Ok(expected_records) = self
            .retention_records_for_deleted_keys(
                &run.run_id,
                run.policy,
                &run.plan.retained_manifest_versions,
                &deleted_keys,
            )
            .await
        else {
            return false;
        };

        expected_records
            .iter()
            .find(|expected| expected.checkpoint_version == record.checkpoint_version)
            .is_some_and(|expected| same_retention_causal_fields(expected, record))
    }

    fn validate_retention_record(
        &self,
        object_key: &ObjectKey,
        record: &CheckpointRetentionRecordV1,
    ) -> Result<(), CheckpointPublishError> {
        if !record.validate_schema() {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "unsupported checkpoint retention schema version {}",
                    record.schema_version
                )),
            ));
        }
        if *object_key != ObjectKey::checkpoint_retention_record(record.checkpoint_version) {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint retention key `{object_key}` does not match body checkpoint {}",
                    record.checkpoint_version
                )),
            ));
        }
        if record.manifest_key != ObjectKey::checkpoint_manifest(record.checkpoint_version) {
            return Err(CheckpointPublishError::ObjectKey(
                ObjectKeyError::InvalidExternalKey(format!(
                    "checkpoint retention manifest key `{}` does not match checkpoint {}",
                    record.manifest_key, record.checkpoint_version
                )),
            ));
        }
        ObjectKey::garbage_collection_run(&record.gc_run_id)?;

        Ok(())
    }

    async fn latest_manifest_from_candidate_marker(
        &self,
    ) -> Result<Option<CheckpointManifest>, CheckpointPublishError> {
        match self.try_latest_manifest_from_candidate_marker().await {
            Ok(manifest) => Ok(manifest),
            Err(CheckpointPublishError::ObjectStore(err)) => Err(err.into()),
            Err(CheckpointPublishError::MissingStateObject(err)) => {
                Err(CheckpointPublishError::MissingStateObject(err))
            }
            Err(CheckpointPublishError::MissingOutputObject(err)) => {
                Err(CheckpointPublishError::MissingOutputObject(err))
            }
            Err(_) => Ok(None),
        }
    }

    async fn try_latest_manifest_from_candidate_marker(
        &self,
    ) -> Result<Option<CheckpointManifest>, CheckpointPublishError> {
        let marker_key = ObjectKey::checkpoint_latest_candidate_marker();
        let marker_path = Path::from(marker_key.as_str());
        let marker_bytes = match self.store.get(&marker_path).await {
            Ok(result) => result.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let marker = serde_json::from_slice::<LatestCandidateMarker>(&marker_bytes)?;
        if !marker.validate_schema() {
            return Ok(None);
        }

        let expected_manifest_key = ObjectKey::checkpoint_manifest(marker.checkpoint_version);
        if marker.manifest_key != expected_manifest_key {
            return Ok(None);
        }

        let manifest_path = Path::from(marker.manifest_key.as_str());
        let manifest_bytes = self.store.get(&manifest_path).await?.bytes().await?;
        if marker.manifest_digest != manifest_digest(&manifest_bytes) {
            return Ok(None);
        }

        let manifest = serde_json::from_slice::<CheckpointManifest>(&manifest_bytes)?;
        manifest.validate()?;
        let body_key = manifest.object_key();
        if marker.manifest_key != body_key {
            return Ok(None);
        }
        if marker.validated_parent_checkpoint != manifest.parent_checkpoint {
            return Ok(None);
        }

        self.validate_parent_manifest_visible(&manifest).await?;
        self.validate_state_objects_exist(&manifest).await?;
        self.validate_output_objects_exist(&manifest).await?;
        if self
            .future_checkpoint_manifest_exists(&marker.manifest_key)
            .await?
        {
            return Ok(None);
        }

        Ok(Some(manifest))
    }

    async fn future_checkpoint_manifest_exists(
        &self,
        manifest_key: &ObjectKey,
    ) -> Result<bool, CheckpointPublishError> {
        let mut future_objects = self.store.list_with_offset(
            Some(&Path::from(CHECKPOINT_PREFIX)),
            &Path::from(manifest_key.as_str()),
        );

        Ok(future_objects.try_next().await?.is_some())
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

    async fn validate_manifest_production_owner_claims_current(
        &self,
        manifest: &CheckpointManifest,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<(), CheckpointPublishError> {
        let mut checked = HashSet::new();

        for input_range in &manifest.input_ranges {
            if checked.insert((input_range.stream_id.as_str(), input_range.partition_id)) {
                self.validate_production_owner_claim_current(
                    &input_range.stream_id,
                    input_range.partition_id,
                    owner_claim,
                )
                .await?;
            }
        }

        for output_object in &manifest.output_objects {
            if checked.insert((output_object.stream_id.as_str(), output_object.partition_id)) {
                self.validate_production_owner_claim_current(
                    &output_object.stream_id,
                    output_object.partition_id,
                    owner_claim,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn validate_production_owner_claim_current(
        &self,
        stream_id: &str,
        partition_id: u32,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<(), CheckpointPublishError> {
        self.validate_matching_ownership_epoch_record_visible(stream_id, partition_id, owner_claim)
            .await?;
        self.validate_owner_claim_current([partition_id], owner_claim)
            .await?;
        self.validate_no_newer_visible_ownership_epoch_record(stream_id, partition_id, owner_claim)
            .await
    }

    async fn validate_matching_ownership_epoch_record_visible(
        &self,
        stream_id: &str,
        partition_id: u32,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<(), CheckpointPublishError> {
        let object_key =
            ObjectKey::ownership_epoch_record(stream_id, partition_id, owner_claim.owner_epoch)?;
        let record = match self.read_ownership_epoch_record_object(&object_key).await {
            Ok(record) => record,
            Err(CheckpointPublishError::ObjectStore(object_store::Error::NotFound { .. })) => {
                return Err(CheckpointPublishError::MissingOwnershipEpochRecord(
                    object_key,
                ));
            }
            Err(err) => return Err(err),
        };

        record.validate_owner_claim(&object_key, &owner_claim.owner_id, owner_claim.owner_epoch)?;

        Ok(())
    }

    async fn validate_no_newer_visible_ownership_epoch_record(
        &self,
        stream_id: &str,
        partition_id: u32,
        owner_claim: &PartitionOwnerClaim,
    ) -> Result<(), CheckpointPublishError> {
        for record in self
            .list_ownership_epoch_records(stream_id, partition_id)
            .await?
        {
            let current = PartitionOwnerClaim {
                owner_id: record.owner_id,
                owner_epoch: record.owner_epoch,
            };
            if Self::is_current_claim_newer_or_conflicting(&current, owner_claim) {
                return Err(CheckpointPublishError::StaleOwnerClaim {
                    partition_id,
                    current,
                    attempted: owner_claim.clone(),
                });
            }
        }

        Ok(())
    }

    async fn list_ownership_epoch_records(
        &self,
        stream_id: &str,
        partition_id: u32,
    ) -> Result<Vec<OwnershipEpochRecord>, CheckpointPublishError> {
        let prefix_key = ObjectKey::ownership_epoch_record(stream_id, partition_id, 0)?;
        let prefix = prefix_key
            .as_str()
            .strip_suffix("epoch=00000000000000000000.claim")
            .ok_or_else(|| ObjectKeyError::InvalidExternalKey(prefix_key.to_string()))?;
        let objects = self
            .store
            .list(Some(&Path::from(prefix)))
            .try_collect::<Vec<_>>()
            .await?;

        let mut records = Vec::with_capacity(objects.len());
        for object in objects {
            let object_key = ObjectKey::parse(object.location.to_string())?;
            let (_, key_parts) = ObjectKey::parse_ownership_epoch_record(object_key.as_str())?;
            if key_parts.stream_id != stream_id || key_parts.partition_id != partition_id {
                continue;
            }

            records.push(self.read_ownership_epoch_record_object(&object_key).await?);
        }

        Ok(records)
    }

    async fn read_ownership_epoch_record_object(
        &self,
        object_key: &ObjectKey,
    ) -> Result<OwnershipEpochRecord, CheckpointPublishError> {
        let path = Path::from(object_key.as_str());
        let bytes = self.store.get(&path).await?.bytes().await?;
        let record = serde_json::from_slice::<OwnershipEpochRecord>(&bytes)?;
        record.validate_object_key(object_key)?;

        Ok(record)
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

fn garbage_collection_candidate_keys(plan: &GarbageCollectionPlan) -> Vec<ObjectKey> {
    plan.candidates
        .iter()
        .map(|candidate| candidate.object_key.clone())
        .collect()
}

fn same_retention_causal_fields(
    left: &CheckpointRetentionRecordV1,
    right: &CheckpointRetentionRecordV1,
) -> bool {
    left.schema_version == right.schema_version
        && left.checkpoint_version == right.checkpoint_version
        && left.manifest_key == right.manifest_key
        && left.manifest_digest == right.manifest_digest
        && left.gc_run_id == right.gc_run_id
        && left.policy == right.policy
        && left.retained_manifest_versions == right.retained_manifest_versions
        && left.deleted_candidate_keys == right.deleted_candidate_keys
}

fn validate_checkpoint_publisher_authoritative_namespaces(
    capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
) -> Result<(), AuthoritativeObjectStoreCapabilityError> {
    for namespace in [
        AuthoritativeNamespace::Checkpoint,
        AuthoritativeNamespace::CheckpointIndex,
        AuthoritativeNamespace::CheckpointLifecycle,
        AuthoritativeNamespace::CheckpointRetention,
        AuthoritativeNamespace::CheckpointGcTransition,
        AuthoritativeNamespace::CheckpointRecovery,
        AuthoritativeNamespace::GcRuns,
    ] {
        capabilities.validate_namespace(namespace)?;
    }

    Ok(())
}

fn same_gc_transition_causal_fields(
    left: &CheckpointGcTransitionRecordV1,
    right: &CheckpointGcTransitionRecordV1,
) -> bool {
    left.schema_version == right.schema_version
        && left.checkpoint_version == right.checkpoint_version
        && left.transition_id == right.transition_id
        && left.manifest_key == right.manifest_key
        && left.manifest_digest == right.manifest_digest
        && left.transition == right.transition
        && left.gc_run_id == right.gc_run_id
        && left.gc_run_key == right.gc_run_key
        && left.gc_run_digest == right.gc_run_digest
        && left.retention_record_key == right.retention_record_key
        && left.retention_record_digest == right.retention_record_digest
        && left.retained_manifest_versions == right.retained_manifest_versions
        && left.released_payload_keys == right.released_payload_keys
        && left.emitter == right.emitter
}

fn gc_transition_matches_retention_record(
    transition: &CheckpointGcTransitionRecordV1,
    retention: &CheckpointRetentionRecordV1,
) -> bool {
    transition.checkpoint_version == retention.checkpoint_version
        && transition.manifest_key == retention.manifest_key
        && transition.manifest_digest == retention.manifest_digest
        && transition.gc_run_id == retention.gc_run_id
        && transition.retained_manifest_versions == retention.retained_manifest_versions
        && transition.released_payload_keys == retention.deleted_candidate_keys
}

fn checkpoint_gc_transition_id_for_gc_run(run_id: &str) -> Result<String, ObjectKeyError> {
    ObjectKey::garbage_collection_run(run_id)?;

    Ok(format!("gc-retired-{run_id}"))
}

fn checkpoint_admin_record_digest(label: &str, bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(label.as_bytes());
    hasher.update(b"\0");
    hasher.update(bytes);

    format!("sha256:{:x}", hasher.finalize())
}

fn retention_conflict_error(checkpoint_version: u64) -> CheckpointPublishError {
    CheckpointPublishError::ObjectKey(ObjectKeyError::InvalidExternalKey(format!(
        "checkpoint retention record already exists with different causal fields for checkpoint {checkpoint_version}"
    )))
}

fn checkpoint_version_from_manifest_key(object_key: &ObjectKey) -> Result<u64, ObjectKeyError> {
    let value = object_key.as_str();
    let Some(version) = value
        .strip_prefix("v1/checkpoints/")
        .and_then(|value| value.strip_suffix(".manifest"))
    else {
        return Err(ObjectKeyError::InvalidExternalKey(value.to_string()));
    };

    version
        .parse()
        .map_err(|_| ObjectKeyError::InvalidExternalKey(value.to_string()))
}

fn checkpoint_recovery_transition_prefix(
    checkpoint_version: u64,
) -> Result<String, ObjectKeyError> {
    let probe = ObjectKey::checkpoint_recovery_transition_record(checkpoint_version, "probe")?;
    let Some(prefix) = probe.as_str().strip_suffix("probe.transition.json") else {
        return Err(ObjectKeyError::InvalidExternalKey(probe.to_string()));
    };

    Ok(prefix.to_string())
}

fn checkpoint_gc_transition_prefix(checkpoint_version: u64) -> Result<String, ObjectKeyError> {
    let probe = ObjectKey::checkpoint_gc_transition_record(checkpoint_version, "probe")?;
    let Some(prefix) = probe.as_str().strip_suffix("probe.transition.json") else {
        return Err(ObjectKeyError::InvalidExternalKey(probe.to_string()));
    };

    Ok(prefix.to_string())
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
            ref_type: StateRefType::RawObject,
            slatedb: None,
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
        request: FencedOutputObjectWriteRequest,
    ) -> Result<Self, CheckpointPublishError> {
        let mut output = Self::new(
            request.stream_id,
            request.partition_id,
            request.checkpoint_version,
            request.start_offset_inclusive,
            request.end_offset_exclusive,
            request.object_id,
            request.bytes,
        )?;
        output.owner_claim = Some(request.owner_claim);

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
