use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use arrow::{
    array::{Array, Date32Array, Int64Array, TimestampNanosecondArray},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{lock::Mutex as AsyncMutex, TryStreamExt};
use object_store::{path::Path, ObjectStore, ObjectStoreExt, PutMode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use velorix_core::{
    relation::{
        validate_record_batch_matches_catalog, ArrowPhysicalTypeV1, RelationSchemaError,
        VelorixRelationCatalogV1,
    },
    standing_program::InputEventTimeWatermark,
};

use crate::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
        AuthoritativeObjectStoreCapabilityError, ObjectStoreCapabilityError,
        ObjectStoreCapabilityProfile,
    },
    ingest_envelope::{IngestEnvelope, IngestEnvelopeError},
    object_key::{ObjectKey, ObjectKeyError},
    relation_catalog_registry::{RelationCatalogRegistry, RelationCatalogRegistryError},
};

const INGEST_PREFIX: &str = "v1/ingest";
const INGEST_ADMISSION_RECORD_KIND_V1: &str = "ingest_range_admission_v1";
const INGEST_ADMISSION_EXPIRY_DECISION_RECORD_KIND_V1: &str =
    "ingest_admission_orphan_expiry_decision_v1";
const INGEST_ADMISSION_INDEX_TRANSITION_RECORD_KIND_V1: &str =
    "ingest_admission_index_transition_v1";
const INGEST_ADMISSION_INDEX_MAX_ADVANCES: usize = 10_000;

type AdmissionLockMap = HashMap<(String, u32), Arc<AsyncMutex<()>>>;

#[derive(Clone, Debug)]
pub struct IngestLog {
    store: Arc<dyn ObjectStore>,
    relation_catalog: Option<RelationCatalogRegistry>,
}

#[derive(Clone, Debug)]
struct DurableIngestAdmission {
    log: IngestLog,
    reserve_before_committed_overlap: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AdmissionSerializationGuard {
    stream_id: String,
    partition_id: u32,
    _private: (),
}

#[derive(Clone)]
pub struct IngestAdmissionCoordinator {
    log: IngestLog,
    durable_admission: DurableIngestAdmission,
    admission_locks: Arc<Mutex<AdmissionLockMap>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngestBatch {
    descriptor: IngestBatchDescriptor,
    payload: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngestBatchDescriptor {
    pub stream_id: String,
    pub partition_id: u32,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
    pub object_key: ObjectKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableIngestAdmissionRecordV1 {
    pub schema_version: u16,
    pub record_kind: String,
    pub stream_id: String,
    pub partition_id: u32,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_time_watermark: Option<InputEventTimeWatermark>,
    pub batch_key: ObjectKey,
    pub admission_record_key: ObjectKey,
    pub payload_digest: String,
    pub relation_id: String,
    pub relation_version: String,
    pub schema_fingerprint: String,
    pub admission_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_guard_binding: Option<IngestCommitGuardBindingV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestCommitGuardBindingV1 {
    pub schema_version: u16,
    pub binding_kind: String,
    pub subject: String,
    pub owner_id: String,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableIngestAdmissionExpiryDecisionRecordV1 {
    pub schema_version: u16,
    pub record_kind: String,
    pub stream_id: String,
    pub partition_id: u32,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
    pub batch_key: ObjectKey,
    pub admission_record_key: ObjectKey,
    pub observed_missing_batch_key: ObjectKey,
    pub expiry_decision_key: ObjectKey,
    pub admission_record_digest: String,
    pub expired_reason: String,
    pub operator_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RangeAdmissionIndexTransitionRecordV1 {
    schema_version: u16,
    record_kind: String,
    stream_id: String,
    partition_id: u32,
    previous_state_digest: String,
    next_state_digest: String,
    admitted: DurableIngestAdmissionRecordV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RangeAdmissionIndexStateDigestV1<'a> {
    schema_version: u16,
    record_kind: &'static str,
    stream_id: &'a str,
    partition_id: u32,
    active_admissions: &'a [DurableIngestAdmissionRecordV1],
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RangeAdmissionIndexState {
    indexed_admissions: Vec<DurableIngestAdmissionRecordV1>,
    active_admissions: Vec<DurableIngestAdmissionRecordV1>,
    indexed_expired_admission_keys: HashSet<ObjectKey>,
    state_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngestAdmissionReconstructionReport {
    pub active_admission_records: usize,
    pub expired_orphan_admission_records: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IngestAdmissionReconstruction {
    active_records: Vec<DurableIngestAdmissionRecordV1>,
    expired_orphan_admission_records: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayCheckpoint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relation_version: Option<String>,
    pub stream_id: String,
    pub partition_id: u32,
    pub end_offset_exclusive: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppendValidatedEnvelopeOutcome {
    Appended {
        descriptor: IngestBatchDescriptor,
    },
    Duplicate {
        descriptor: IngestBatchDescriptor,
    },
    Conflict {
        descriptor: IngestBatchDescriptor,
        object_key: ObjectKey,
        reason: &'static str,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReserveIngestRangeAdmissionOutcome {
    Reserved,
    Duplicate,
    Conflict {
        object_key: ObjectKey,
        reason: &'static str,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngestCommitGuardPhase {
    BeforeAdmission,
    BeforeCommit,
}

impl IngestCommitGuardPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BeforeAdmission => "before_admission",
            Self::BeforeCommit => "before_commit",
        }
    }
}

#[async_trait]
pub trait IngestCommitGuard: Send + Sync {
    async fn verify(
        &self,
        phase: IngestCommitGuardPhase,
        descriptor: &IngestBatchDescriptor,
    ) -> Result<(), String>;

    fn admission_binding(
        &self,
        _descriptor: &IngestBatchDescriptor,
    ) -> Option<IngestCommitGuardBindingV1> {
        None
    }
}

#[derive(Debug, Error)]
pub enum IngestLogError {
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    #[error("ingest batch object `{0}` already exists")]
    AlreadyExists(ObjectKey),
    #[error("malformed ingest batch key `{key}`: {source}")]
    MalformedIngestKey { key: String, source: ObjectKeyError },
    #[error("malformed durable ingest admission record `{key}`: {reason}")]
    MalformedIngestAdmissionRecord { key: String, reason: String },
    #[error("malformed durable ingest admission expiry decision `{key}`: {reason}")]
    MalformedIngestAdmissionExpiryDecision { key: String, reason: String },
    #[error("malformed durable ingest admission index transition `{key}`: {reason}")]
    MalformedIngestAdmissionIndexTransition { key: String, reason: String },
    #[error("ingest admission expiry decision `{expiry_decision_key}` already exists with a different body")]
    IngestAdmissionExpiryDecisionConflict { expiry_decision_key: ObjectKey },
    #[error(
        "cannot expire committed ingest admission `{admission_record_key}` for batch `{batch_key}`"
    )]
    CannotExpireCommittedIngestAdmission {
        admission_record_key: ObjectKey,
        batch_key: ObjectKey,
    },
    #[error(
        "admission serialization guard is for {guard_stream_id}/p={guard_partition_id}, not {stream_id}/p={partition_id}"
    )]
    AdmissionSerializationGuardMismatch {
        guard_stream_id: String,
        guard_partition_id: u32,
        stream_id: String,
        partition_id: u32,
    },
    #[error(
        "ingest commit guard rejected {stream_id}/p={partition_id} {start_offset_inclusive}-{end_offset_exclusive} at {phase}: {reason}"
    )]
    IngestCommitGuardRejected {
        stream_id: String,
        partition_id: u32,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
        phase: &'static str,
        reason: String,
    },
    #[error(
        "overlapping committed ingest ranges for {stream_id}/p={partition_id}: `{previous}` overlaps `{current}`"
    )]
    OverlappingCommittedRange {
        stream_id: String,
        partition_id: u32,
        previous: ObjectKey,
        current: ObjectKey,
    },
    #[error(
        "checkpoint boundary {checkpoint_end_offset_exclusive} falls inside committed batch `{object_key}`"
    )]
    CheckpointInsideBatch {
        checkpoint_end_offset_exclusive: u64,
        object_key: ObjectKey,
    },
    #[error(
        "checkpoint boundary {checkpoint_end_offset_exclusive} falls inside admitted range `{admission_record_key}`"
    )]
    CheckpointInsideAdmittedRange {
        checkpoint_end_offset_exclusive: u64,
        admission_record_key: ObjectKey,
    },
    #[error("committed ingest batch `{batch_key}` has no matching admission record")]
    MissingIngestAdmissionRecord { batch_key: ObjectKey },
    #[error(
        "admission record `{admission_record_key}` does not match ingest batch `{batch_key}` for {field}: expected {expected}, found {actual}"
    )]
    IngestAdmissionMismatch {
        admission_record_key: ObjectKey,
        batch_key: ObjectKey,
        field: &'static str,
        expected: String,
        actual: String,
    },
    #[error("duplicate replay checkpoint for {stream_id}/p={partition_id}")]
    DuplicateReplayCheckpoint {
        stream_id: String,
        partition_id: u32,
    },
    #[error(transparent)]
    RelationCatalogRegistry(#[from] RelationCatalogRegistryError),
    #[error(transparent)]
    ObjectStoreCapability(#[from] ObjectStoreCapabilityError),
    #[error(transparent)]
    AuthoritativeObjectStoreCapabilities(#[from] AuthoritativeObjectStoreCapabilityError),
    #[error(transparent)]
    RelationSchema(#[from] RelationSchemaError),
    #[error(
        "ingest envelope relation catalog mismatch for {field}: expected {expected}, found {actual}"
    )]
    RelationCatalogMismatch {
        field: &'static str,
        expected: String,
        actual: String,
    },
    #[error(transparent)]
    IngestEnvelope(#[from] IngestEnvelopeError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl AdmissionSerializationGuard {
    fn process_local(stream_id: String, partition_id: u32) -> Self {
        Self {
            stream_id,
            partition_id,
            _private: (),
        }
    }

    fn validate(&self, descriptor: &IngestBatchDescriptor) -> Result<(), IngestLogError> {
        if self.stream_id == descriptor.stream_id && self.partition_id == descriptor.partition_id {
            return Ok(());
        }

        Err(IngestLogError::AdmissionSerializationGuardMismatch {
            guard_stream_id: self.stream_id.clone(),
            guard_partition_id: self.partition_id,
            stream_id: descriptor.stream_id.clone(),
            partition_id: descriptor.partition_id,
        })
    }
}

impl DurableIngestAdmission {
    fn new(log: IngestLog, reserve_before_committed_overlap: bool) -> Self {
        Self {
            log,
            reserve_before_committed_overlap,
        }
    }

    /// Reserves the durable range-admission index, records materialized
    /// admission evidence, then appends the canonical ingest object. The
    /// supplied guard keeps in-process callers from doing redundant work for the
    /// same partition while the object-store index remains the cross-process
    /// admission fence.
    async fn admit_validated_batch_with_commit_guard(
        &self,
        batch: IngestBatch,
        guard: &AdmissionSerializationGuard,
        commit_guard: Option<&dyn IngestCommitGuard>,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        let descriptor = batch.descriptor();
        guard.validate(&descriptor)?;
        verify_ingest_commit_guard(
            commit_guard,
            IngestCommitGuardPhase::BeforeAdmission,
            &descriptor,
        )
        .await?;

        if self.reserve_before_committed_overlap {
            return self
                .admit_validated_batch_indexed_before_committed_fallback(
                    batch,
                    descriptor,
                    commit_guard,
                )
                .await;
        }

        if let Some(conflict) = self.committed_overlap_conflict(&descriptor).await? {
            return Ok(conflict);
        }
        self.reserve_then_append_validated_batch(batch, descriptor, commit_guard)
            .await
    }

    async fn admit_validated_batch_indexed_before_committed_fallback(
        &self,
        batch: IngestBatch,
        descriptor: IngestBatchDescriptor,
        commit_guard: Option<&dyn IngestCommitGuard>,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        if let Some(existing) = self.log.existing_batch_conflict(&batch).await? {
            return Ok(existing);
        }

        if let Some(conflict) = self
            .indexed_or_committed_overlap_conflict(&descriptor)
            .await?
        {
            return Ok(conflict);
        }

        match self
            .reserve_then_materialize_admission(&batch, commit_guard)
            .await?
        {
            AdmissionReservationOutcome::Admitted | AdmissionReservationOutcome::Duplicate => {}
            AdmissionReservationOutcome::Conflict { object_key, reason } => {
                return Ok(AppendValidatedEnvelopeOutcome::Conflict {
                    descriptor,
                    object_key,
                    reason,
                });
            }
        }

        if let Some(conflict) = self
            .indexed_or_committed_overlap_conflict(&descriptor)
            .await?
        {
            return Ok(conflict);
        }

        verify_ingest_commit_guard(
            commit_guard,
            IngestCommitGuardPhase::BeforeCommit,
            &descriptor,
        )
        .await?;
        self.log.append_validated_batch(batch).await
    }

    async fn reserve_then_append_validated_batch(
        &self,
        batch: IngestBatch,
        descriptor: IngestBatchDescriptor,
        commit_guard: Option<&dyn IngestCommitGuard>,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        if let Some(existing) = self.log.existing_batch_conflict(&batch).await? {
            return Ok(existing);
        }

        match self
            .reserve_then_materialize_admission(&batch, commit_guard)
            .await?
        {
            AdmissionReservationOutcome::Admitted | AdmissionReservationOutcome::Duplicate => {}
            AdmissionReservationOutcome::Conflict { object_key, reason } => {
                return Ok(AppendValidatedEnvelopeOutcome::Conflict {
                    descriptor,
                    object_key,
                    reason,
                });
            }
        }

        verify_ingest_commit_guard(
            commit_guard,
            IngestCommitGuardPhase::BeforeCommit,
            &descriptor,
        )
        .await?;
        self.log.append_validated_batch(batch).await
    }

    async fn reserve_then_materialize_admission_record(
        &self,
        record: &DurableIngestAdmissionRecordV1,
    ) -> Result<AdmissionReservationOutcome, IngestLogError> {
        if let Some(conflict) = self.expired_materialized_admission_conflict(record).await? {
            return Ok(conflict);
        }
        let mut duplicate = false;
        match self.reserve_range_admission_index(record).await? {
            AdmissionReservationOutcome::Admitted => {}
            AdmissionReservationOutcome::Duplicate => duplicate = true,
            conflict @ AdmissionReservationOutcome::Conflict { .. } => return Ok(conflict),
        }
        match self.materialize_admission_record(record).await? {
            AdmissionReservationOutcome::Admitted => {}
            AdmissionReservationOutcome::Duplicate => duplicate = true,
            conflict @ AdmissionReservationOutcome::Conflict { .. } => return Ok(conflict),
        }

        if duplicate {
            Ok(AdmissionReservationOutcome::Duplicate)
        } else {
            Ok(AdmissionReservationOutcome::Admitted)
        }
    }

    async fn materialize_external_admission_then_append_validated_batch(
        &self,
        batch: IngestBatch,
        descriptor: IngestBatchDescriptor,
        commit_guard: Option<&dyn IngestCommitGuard>,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        if let Some(existing) = self.log.existing_batch_conflict(&batch).await? {
            return Ok(existing);
        }

        match self
            .materialize_admission_record(&admission_record_for_batch(&batch)?)
            .await?
        {
            AdmissionReservationOutcome::Admitted | AdmissionReservationOutcome::Duplicate => {}
            AdmissionReservationOutcome::Conflict { object_key, reason } => {
                return Ok(AppendValidatedEnvelopeOutcome::Conflict {
                    descriptor,
                    object_key,
                    reason,
                });
            }
        }

        verify_ingest_commit_guard(
            commit_guard,
            IngestCommitGuardPhase::BeforeCommit,
            &descriptor,
        )
        .await?;
        self.log.append_validated_batch(batch).await
    }

    async fn reserve_then_materialize_admission(
        &self,
        batch: &IngestBatch,
        commit_guard: Option<&dyn IngestCommitGuard>,
    ) -> Result<AdmissionReservationOutcome, IngestLogError> {
        let record = admission_record_for_batch_with_commit_guard(batch, commit_guard)?;
        self.reserve_then_materialize_admission_record(&record)
            .await
    }

    async fn expired_materialized_admission_conflict(
        &self,
        record: &DurableIngestAdmissionRecordV1,
    ) -> Result<Option<AdmissionReservationOutcome>, IngestLogError> {
        let expiry_decisions = self.log.list_admission_expiry_decisions().await?;
        let Some((existing_bytes, existing)) = self
            .log
            .list_admission_record_bodies()
            .await?
            .into_iter()
            .find(|(_, existing)| existing.admission_record_key == record.admission_record_key)
        else {
            return Ok(None);
        };

        if expiry_decisions
            .iter()
            .any(|decision| decision.expires_admission(&existing_bytes, &existing))
            && !self.log.object_exists(&existing.batch_key).await?
        {
            return Ok(Some(AdmissionReservationOutcome::Conflict {
                object_key: existing.batch_key,
                reason: "admission_expired",
            }));
        }

        Ok(None)
    }

    async fn committed_overlap_conflict(
        &self,
        descriptor: &IngestBatchDescriptor,
    ) -> Result<Option<AppendValidatedEnvelopeOutcome>, IngestLogError> {
        for committed in self.log.list_committed().await? {
            if committed.object_key == descriptor.object_key {
                continue;
            }
            if ranges_overlap(&committed, descriptor) {
                return Ok(Some(AppendValidatedEnvelopeOutcome::Conflict {
                    descriptor: descriptor.clone(),
                    object_key: committed.object_key,
                    reason: "range_overlap_committed",
                }));
            }
        }

        Ok(None)
    }

    async fn indexed_or_committed_overlap_conflict(
        &self,
        descriptor: &IngestBatchDescriptor,
    ) -> Result<Option<AppendValidatedEnvelopeOutcome>, IngestLogError> {
        let Some(committed_conflict) = self.committed_overlap_conflict(descriptor).await? else {
            return Ok(None);
        };

        let state = self
            .log
            .load_range_admission_index_state_for_partition(
                &descriptor.stream_id,
                descriptor.partition_id,
                false,
            )
            .await?;
        if let AdmissionReservationOutcome::Conflict { object_key, reason } = admission_conflict(
            &state.active_admissions,
            &index_probe_record_for_descriptor(descriptor)?,
        )? {
            return Ok(Some(AppendValidatedEnvelopeOutcome::Conflict {
                descriptor: descriptor.clone(),
                object_key,
                reason,
            }));
        }

        Ok(Some(committed_conflict))
    }

    async fn reserve_range_admission_index(
        &self,
        record: &DurableIngestAdmissionRecordV1,
    ) -> Result<AdmissionReservationOutcome, IngestLogError> {
        for _ in 0..INGEST_ADMISSION_INDEX_MAX_ADVANCES {
            let state = self.log.load_range_admission_index_state(record).await?;
            match admission_conflict(&state.active_admissions, record)? {
                AdmissionReservationOutcome::Admitted => {}
                outcome => return Ok(outcome),
            }

            let transition = range_admission_index_transition(record, &state)?;
            let transition_key = range_admission_index_transition_key(
                &transition.stream_id,
                transition.partition_id,
                &transition.previous_state_digest,
            )?;
            let bytes = Bytes::from(serde_json::to_vec(&transition)?);
            match self
                .log
                .store
                .put_opts(
                    &Path::from(transition_key.as_str()),
                    bytes.into(),
                    PutMode::Create.into(),
                )
                .await
            {
                Ok(_) => return Ok(AdmissionReservationOutcome::Admitted),
                Err(object_store::Error::AlreadyExists { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }

        Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
            key: range_admission_index_partition_prefix(&record.stream_id, record.partition_id),
            reason: format!(
                "admission index exceeded {INGEST_ADMISSION_INDEX_MAX_ADVANCES} advances"
            ),
        })
    }

    async fn materialize_admission_record(
        &self,
        record: &DurableIngestAdmissionRecordV1,
    ) -> Result<AdmissionReservationOutcome, IngestLogError> {
        let bytes = Bytes::from(serde_json::to_vec(record)?);
        match self
            .log
            .store
            .put_opts(
                &Path::from(record.admission_record_key.as_str()),
                bytes.clone().into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => Ok(AdmissionReservationOutcome::Admitted),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing_bytes = self
                    .log
                    .store
                    .get(&Path::from(record.admission_record_key.as_str()))
                    .await?
                    .bytes()
                    .await?;
                let existing: DurableIngestAdmissionRecordV1 =
                    serde_json::from_slice(&existing_bytes)?;
                existing.validate_key()?;
                if self
                    .log
                    .admission_has_matching_expiry(&existing_bytes, &existing)
                    .await?
                    && !self.log.object_exists(&existing.batch_key).await?
                {
                    return Ok(AdmissionReservationOutcome::Conflict {
                        object_key: existing.batch_key,
                        reason: "admission_expired",
                    });
                }

                if &existing == record && existing_bytes == bytes {
                    Ok(AdmissionReservationOutcome::Duplicate)
                } else if &existing == record {
                    Err(IngestLogError::MalformedIngestAdmissionRecord {
                        key: record.admission_record_key.to_string(),
                        reason: "materialized admission bytes do not match indexed transition"
                            .to_string(),
                    })
                } else {
                    Ok(AdmissionReservationOutcome::Conflict {
                        object_key: existing.batch_key,
                        reason: "same_range_different_digest_reserved",
                    })
                }
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl DurableIngestAdmission {
    #[expect(
        clippy::too_many_arguments,
        reason = "Expiry decisions mirror the durable admission identity plus operator audit fields."
    )]
    async fn expire_orphan_admission(
        &self,
        stream_id: &str,
        partition_id: u32,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
        decision_id: &str,
        expired_reason: &str,
        operator_id: &str,
    ) -> Result<DurableIngestAdmissionExpiryDecisionRecordV1, IngestLogError> {
        let admission_record_key = ObjectKey::ingest_admission_record(
            stream_id,
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
        )?;
        let batch_key = ObjectKey::ingest_batch(
            stream_id,
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
        )?;
        let Some((admission_bytes, admission)) = self
            .log
            .list_admission_record_bodies()
            .await?
            .into_iter()
            .find(|(_, record)| record.admission_record_key == admission_record_key)
        else {
            return Err(IngestLogError::MissingIngestAdmissionRecord { batch_key });
        };

        if self.log.object_exists(&admission.batch_key).await? {
            return Err(IngestLogError::CannotExpireCommittedIngestAdmission {
                admission_record_key: admission.admission_record_key,
                batch_key: admission.batch_key,
            });
        }

        let decision = expiry_decision_record_for_admission(
            &admission_bytes,
            &admission,
            decision_id,
            expired_reason,
            operator_id,
        )?;
        let bytes = Bytes::from(serde_json::to_vec(&decision)?);
        match self
            .log
            .store
            .put_opts(
                &Path::from(decision.expiry_decision_key.as_str()),
                bytes.into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => Ok(decision),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self
                    .log
                    .store
                    .get(&Path::from(decision.expiry_decision_key.as_str()))
                    .await?
                    .bytes()
                    .await?;
                let existing: DurableIngestAdmissionExpiryDecisionRecordV1 =
                    serde_json::from_slice(&existing)?;
                existing.validate_key()?;
                if existing == decision {
                    Ok(existing)
                } else {
                    Err(IngestLogError::IngestAdmissionExpiryDecisionConflict {
                        expiry_decision_key: decision.expiry_decision_key,
                    })
                }
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl DurableIngestAdmissionRecordV1 {
    #[expect(
        clippy::too_many_arguments,
        reason = "Admission records are constructed from explicit durable identity and catalog fields."
    )]
    pub fn for_external_admission(
        stream_id: impl Into<String>,
        partition_id: u32,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
        payload_digest: impl Into<String>,
        relation_id: impl Into<String>,
        relation_version: impl Into<String>,
        schema_fingerprint: impl Into<String>,
    ) -> Result<Self, IngestLogError> {
        let stream_id = stream_id.into();
        let batch_key = ObjectKey::ingest_batch(
            &stream_id,
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
        )?;
        let admission_record_key = ObjectKey::ingest_admission_record(
            &stream_id,
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
        )?;
        let record = Self {
            schema_version: 1,
            record_kind: INGEST_ADMISSION_RECORD_KIND_V1.to_string(),
            stream_id,
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
            event_time_watermark: None,
            batch_key,
            admission_record_key,
            payload_digest: payload_digest.into(),
            relation_id: relation_id.into(),
            relation_version: relation_version.into(),
            schema_fingerprint: schema_fingerprint.into(),
            admission_mode: "process_local_serialized".to_string(),
            commit_guard_binding: None,
        };
        record.validate_key()?;

        Ok(record)
    }

    fn descriptor(&self) -> Result<IngestBatchDescriptor, IngestLogError> {
        self.validate_key()?;

        Ok(IngestBatchDescriptor {
            stream_id: self.stream_id.clone(),
            partition_id: self.partition_id,
            start_offset_inclusive: self.start_offset_inclusive,
            end_offset_exclusive: self.end_offset_exclusive,
            object_key: self.batch_key.clone(),
        })
    }

    fn validate_key(&self) -> Result<(), IngestLogError> {
        let expected_batch_key = ObjectKey::ingest_batch(
            &self.stream_id,
            self.partition_id,
            self.start_offset_inclusive,
            self.end_offset_exclusive,
        )?;
        let expected_admission_record_key = ObjectKey::ingest_admission_record(
            &self.stream_id,
            self.partition_id,
            self.start_offset_inclusive,
            self.end_offset_exclusive,
        )?;

        if self.schema_version != 1 {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: self.admission_record_key.to_string(),
                reason: format!("unsupported schema_version {}", self.schema_version),
            });
        }
        if self.record_kind != INGEST_ADMISSION_RECORD_KIND_V1 {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: self.admission_record_key.to_string(),
                reason: format!("unsupported record_kind `{}`", self.record_kind),
            });
        }
        if self.batch_key != expected_batch_key {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: self.admission_record_key.to_string(),
                reason: format!("batch_key `{}` does not match record range", self.batch_key),
            });
        }
        if self.admission_record_key != expected_admission_record_key {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: self.admission_record_key.to_string(),
                reason: format!(
                    "admission_record_key does not match expected `{expected_admission_record_key}`"
                ),
            });
        }
        if !is_sha256_digest(&self.payload_digest) {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: self.admission_record_key.to_string(),
                reason: "payload_digest must be sha256:<64 hex chars>".to_string(),
            });
        }
        if self.relation_id.trim().is_empty() {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: self.admission_record_key.to_string(),
                reason: "relation_id must be nonempty".to_string(),
            });
        }
        if self.relation_version.trim().is_empty() {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: self.admission_record_key.to_string(),
                reason: "relation_version must be nonempty".to_string(),
            });
        }
        if !is_sha256_digest(&self.schema_fingerprint) {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: self.admission_record_key.to_string(),
                reason: "schema_fingerprint must be sha256:<64 hex chars>".to_string(),
            });
        }
        if self.admission_mode != "process_local_serialized" {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: self.admission_record_key.to_string(),
                reason: format!("unsupported admission_mode `{}`", self.admission_mode),
            });
        }
        if let Some(binding) = &self.commit_guard_binding {
            binding.validate(&self.admission_record_key)?;
        }

        Ok(())
    }
}

impl IngestCommitGuardBindingV1 {
    pub fn new(
        binding_kind: impl Into<String>,
        subject: impl Into<String>,
        owner_id: impl Into<String>,
        owner_epoch: u64,
    ) -> Self {
        Self {
            schema_version: 1,
            binding_kind: binding_kind.into(),
            subject: subject.into(),
            owner_id: owner_id.into(),
            owner_epoch,
        }
    }

    fn validate(&self, admission_record_key: &ObjectKey) -> Result<(), IngestLogError> {
        if self.schema_version != 1 {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: admission_record_key.to_string(),
                reason: format!(
                    "commit_guard_binding has unsupported schema_version {}",
                    self.schema_version
                ),
            });
        }
        if self.binding_kind.trim().is_empty() {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: admission_record_key.to_string(),
                reason: "commit_guard_binding.binding_kind must be nonempty".to_string(),
            });
        }
        if self.subject.trim().is_empty() {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: admission_record_key.to_string(),
                reason: "commit_guard_binding.subject must be nonempty".to_string(),
            });
        }
        if self.owner_id.trim().is_empty() {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: admission_record_key.to_string(),
                reason: "commit_guard_binding.owner_id must be nonempty".to_string(),
            });
        }
        if self.owner_epoch == 0 {
            return Err(IngestLogError::MalformedIngestAdmissionRecord {
                key: admission_record_key.to_string(),
                reason: "commit_guard_binding.owner_epoch must be greater than zero".to_string(),
            });
        }

        Ok(())
    }
}

impl DurableIngestAdmissionExpiryDecisionRecordV1 {
    fn validate_key(&self) -> Result<(), IngestLogError> {
        let expected_batch_key = ObjectKey::ingest_batch(
            &self.stream_id,
            self.partition_id,
            self.start_offset_inclusive,
            self.end_offset_exclusive,
        )?;
        let expected_admission_record_key = ObjectKey::ingest_admission_record(
            &self.stream_id,
            self.partition_id,
            self.start_offset_inclusive,
            self.end_offset_exclusive,
        )?;
        let expected_expiry_prefix = format!(
            "v1/ingest-admission/{}/p={:010}/ranges/{:020}-{:020}/expiry-decisions/",
            self.stream_id,
            self.partition_id,
            self.start_offset_inclusive,
            self.end_offset_exclusive
        );

        if self.schema_version != 1 {
            return Err(IngestLogError::MalformedIngestAdmissionExpiryDecision {
                key: self.expiry_decision_key.to_string(),
                reason: format!("unsupported schema_version {}", self.schema_version),
            });
        }
        if self.record_kind != INGEST_ADMISSION_EXPIRY_DECISION_RECORD_KIND_V1 {
            return Err(IngestLogError::MalformedIngestAdmissionExpiryDecision {
                key: self.expiry_decision_key.to_string(),
                reason: format!("unsupported record_kind `{}`", self.record_kind),
            });
        }
        if self.batch_key != expected_batch_key {
            return Err(IngestLogError::MalformedIngestAdmissionExpiryDecision {
                key: self.expiry_decision_key.to_string(),
                reason: format!(
                    "batch_key `{}` does not match decision range",
                    self.batch_key
                ),
            });
        }
        if self.admission_record_key != expected_admission_record_key {
            return Err(IngestLogError::MalformedIngestAdmissionExpiryDecision {
                key: self.expiry_decision_key.to_string(),
                reason: format!(
                    "admission_record_key does not match expected `{expected_admission_record_key}`"
                ),
            });
        }
        if self.observed_missing_batch_key != expected_batch_key {
            return Err(IngestLogError::MalformedIngestAdmissionExpiryDecision {
                key: self.expiry_decision_key.to_string(),
                reason: format!(
                    "observed_missing_batch_key does not match expected `{expected_batch_key}`"
                ),
            });
        }
        if !self
            .expiry_decision_key
            .as_str()
            .starts_with(&expected_expiry_prefix)
        {
            return Err(IngestLogError::MalformedIngestAdmissionExpiryDecision {
                key: self.expiry_decision_key.to_string(),
                reason: "expiry_decision_key does not match decision range".to_string(),
            });
        }
        if !is_sha256_digest(&self.admission_record_digest) {
            return Err(IngestLogError::MalformedIngestAdmissionExpiryDecision {
                key: self.expiry_decision_key.to_string(),
                reason: "admission_record_digest must be sha256:<64 hex chars>".to_string(),
            });
        }
        if self.expired_reason.trim().is_empty() {
            return Err(IngestLogError::MalformedIngestAdmissionExpiryDecision {
                key: self.expiry_decision_key.to_string(),
                reason: "expired_reason must be nonempty".to_string(),
            });
        }
        if self.operator_id.trim().is_empty() {
            return Err(IngestLogError::MalformedIngestAdmissionExpiryDecision {
                key: self.expiry_decision_key.to_string(),
                reason: "operator_id must be nonempty".to_string(),
            });
        }

        Ok(())
    }

    fn expires_admission(
        &self,
        admission_bytes: &Bytes,
        admission: &DurableIngestAdmissionRecordV1,
    ) -> bool {
        self.stream_id == admission.stream_id
            && self.partition_id == admission.partition_id
            && self.start_offset_inclusive == admission.start_offset_inclusive
            && self.end_offset_exclusive == admission.end_offset_exclusive
            && self.batch_key == admission.batch_key
            && self.observed_missing_batch_key == admission.batch_key
            && self.admission_record_key == admission.admission_record_key
            && self.admission_record_digest == digest_bytes(admission_bytes)
    }
}

impl RangeAdmissionIndexTransitionRecordV1 {
    fn validate_key(&self, key: &str) -> Result<(), IngestLogError> {
        self.admitted.validate_key()?;

        if self.schema_version != 1 {
            return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                key: key.to_string(),
                reason: format!("unsupported schema_version {}", self.schema_version),
            });
        }
        if self.record_kind != INGEST_ADMISSION_INDEX_TRANSITION_RECORD_KIND_V1 {
            return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                key: key.to_string(),
                reason: format!("unsupported record_kind `{}`", self.record_kind),
            });
        }
        if self.stream_id != self.admitted.stream_id {
            return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                key: key.to_string(),
                reason: "transition stream_id does not match admitted record".to_string(),
            });
        }
        if self.partition_id != self.admitted.partition_id {
            return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                key: key.to_string(),
                reason: "transition partition_id does not match admitted record".to_string(),
            });
        }
        if !is_sha256_digest(&self.previous_state_digest) {
            return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                key: key.to_string(),
                reason: "previous_state_digest must be sha256:<64 hex chars>".to_string(),
            });
        }
        if !is_sha256_digest(&self.next_state_digest) {
            return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                key: key.to_string(),
                reason: "next_state_digest must be sha256:<64 hex chars>".to_string(),
            });
        }

        let expected_key = range_admission_index_transition_key(
            &self.stream_id,
            self.partition_id,
            &self.previous_state_digest,
        )?;
        if key != expected_key {
            return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                key: key.to_string(),
                reason: format!("stored path does not match expected `{expected_key}`"),
            });
        }

        Ok(())
    }

    fn apply_to_state(&self, state: &mut RangeAdmissionIndexState) -> Result<(), IngestLogError> {
        let key = range_admission_index_transition_key(
            &self.stream_id,
            self.partition_id,
            &self.previous_state_digest,
        )?;
        self.validate_key(&key)?;
        if self.previous_state_digest != state.state_digest {
            return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                key,
                reason: format!(
                    "previous_state_digest does not match current head `{}`",
                    state.state_digest
                ),
            });
        }
        let expired = state
            .indexed_expired_admission_keys
            .contains(&self.admitted.admission_record_key);
        if !expired
            && !matches!(
                admission_conflict(&state.active_admissions, &self.admitted)?,
                AdmissionReservationOutcome::Admitted
            )
        {
            return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                key,
                reason: "transition admits an overlapping active range".to_string(),
            });
        }

        state.indexed_admissions.push(self.admitted.clone());
        sort_admission_records(&mut state.indexed_admissions);
        let next_digest = range_admission_index_state_digest(
            &self.stream_id,
            self.partition_id,
            &state.indexed_admissions,
        )?;
        if self.next_state_digest != next_digest {
            return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                key,
                reason: format!("next_state_digest does not match computed digest `{next_digest}`"),
            });
        }
        if !expired {
            state.active_admissions.push(self.admitted.clone());
            sort_admission_records(&mut state.active_admissions);
        }
        state.state_digest = next_digest;

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AdmissionReservationOutcome {
    Admitted,
    Duplicate,
    Conflict {
        object_key: ObjectKey,
        reason: &'static str,
    },
}

async fn verify_ingest_commit_guard(
    guard: Option<&dyn IngestCommitGuard>,
    phase: IngestCommitGuardPhase,
    descriptor: &IngestBatchDescriptor,
) -> Result<(), IngestLogError> {
    let Some(guard) = guard else {
        return Ok(());
    };
    guard.verify(phase, descriptor).await.map_err(|reason| {
        IngestLogError::IngestCommitGuardRejected {
            stream_id: descriptor.stream_id.clone(),
            partition_id: descriptor.partition_id,
            start_offset_inclusive: descriptor.start_offset_inclusive,
            end_offset_exclusive: descriptor.end_offset_exclusive,
            phase: phase.as_str(),
            reason,
        }
    })
}

impl IngestAdmissionCoordinator {
    pub fn new(log: IngestLog) -> Self {
        Self {
            durable_admission: DurableIngestAdmission::new(log.clone(), false),
            log,
            admission_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn new_object_store_meta_authority(store: Arc<dyn ObjectStore>) -> Self {
        Self::new(IngestLog::new(store))
    }

    /// Constructs an ingest admission coordinator from the shared startup
    /// capability evidence required for committed ingest objects, durable
    /// admission records, and relation-catalog reads before catalog-aware appends.
    pub fn new_checked(
        store: Arc<dyn ObjectStore>,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, AuthoritativeObjectStoreCapabilityError> {
        capabilities.validate_namespace(AuthoritativeNamespace::IngestAdmission)?;
        capabilities.validate_namespace(AuthoritativeNamespace::IngestAdmissionIndex)?;
        let log = IngestLog::new_catalog_checked(store, capabilities)?;

        Ok(Self {
            durable_admission: DurableIngestAdmission::new(log.clone(), true),
            log,
            admission_locks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Catalog-aware admission with a durable range-admission index.
    ///
    /// This serializes redundant range checks for each stream/partition inside
    /// this coordinator instance, reserves a create-only transition in the
    /// object-store admission index, and records Velorix-owned materialized
    /// admission evidence before appending.
    pub async fn append_catalog_validated_envelope(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        self.append_catalog_validated_envelope_with_optional_commit_guard(payload, None)
            .await
    }

    pub async fn append_catalog_validated_envelope_with_commit_guard(
        &self,
        payload: Bytes,
        commit_guard: &dyn IngestCommitGuard,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        self.append_catalog_validated_envelope_with_optional_commit_guard(
            payload,
            Some(commit_guard),
        )
        .await
    }

    async fn append_catalog_validated_envelope_with_optional_commit_guard(
        &self,
        payload: Bytes,
        commit_guard: Option<&dyn IngestCommitGuard>,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        let batch = self.log.catalog_validated_batch(payload).await?;
        let descriptor = batch.descriptor();
        let admission_lock = self.admission_lock(&descriptor);
        let _guard = admission_lock.lock().await;
        let serialization_guard = AdmissionSerializationGuard::process_local(
            descriptor.stream_id.clone(),
            descriptor.partition_id,
        );

        self.durable_admission
            .admit_validated_batch_with_commit_guard(batch, &serialization_guard, commit_guard)
            .await
    }

    /// Appends a catalog-validated envelope after a separate authoritative
    /// admission service has already reserved the range.
    pub async fn append_catalog_validated_envelope_after_external_admission(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        let batch = self.log.catalog_validated_batch(payload).await?;
        let descriptor = batch.descriptor();

        self.durable_admission
            .materialize_external_admission_then_append_validated_batch(batch, descriptor, None)
            .await
    }

    /// Appends an already catalog-validated envelope after a separate
    /// authoritative admission service has reserved the range.
    ///
    /// Metadata-backed API ingest uses this after validating the request
    /// against the metadata relation catalog and reserving the range through
    /// the metadata service. It intentionally does not re-read the object-store
    /// relation catalog materialization, because that record is a cache/evidence
    /// copy in metadata-backed mode rather than the ingest authority.
    pub async fn append_validated_envelope_after_external_admission(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        let batch = IngestBatch::from_validated_envelope(payload)?;
        let descriptor = batch.descriptor();

        self.durable_admission
            .materialize_external_admission_then_append_validated_batch(batch, descriptor, None)
            .await
    }

    pub async fn reserve_external_ingest_range_admission(
        &self,
        record: DurableIngestAdmissionRecordV1,
    ) -> Result<ReserveIngestRangeAdmissionOutcome, IngestLogError> {
        let descriptor = record.descriptor()?;
        let admission_lock = self.admission_lock(&descriptor);
        let _guard = admission_lock.lock().await;

        if let Some(conflict) = self
            .durable_admission
            .committed_overlap_conflict(&descriptor)
            .await?
        {
            let AppendValidatedEnvelopeOutcome::Conflict {
                object_key, reason, ..
            } = conflict
            else {
                unreachable!("committed_overlap_conflict only returns conflicts")
            };
            return Ok(ReserveIngestRangeAdmissionOutcome::Conflict { object_key, reason });
        }

        match self
            .durable_admission
            .reserve_then_materialize_admission_record(&record)
            .await?
        {
            AdmissionReservationOutcome::Admitted => {
                Ok(ReserveIngestRangeAdmissionOutcome::Reserved)
            }
            AdmissionReservationOutcome::Duplicate => {
                Ok(ReserveIngestRangeAdmissionOutcome::Duplicate)
            }
            AdmissionReservationOutcome::Conflict { object_key, reason } => {
                Ok(ReserveIngestRangeAdmissionOutcome::Conflict { object_key, reason })
            }
        }
    }

    pub async fn list_committed(&self) -> Result<Vec<IngestBatchDescriptor>, IngestLogError> {
        self.log.list_committed().await
    }

    /// Reconstructs the durable admission namespace before exposing the
    /// coordinator to production writers.
    ///
    /// This uses the same active-admission view that rejects overlapping
    /// writes, including digest-bound orphan expiry decisions, and fails closed
    /// on malformed or unexpected objects under `v1/ingest-admission`.
    pub async fn reconstruct_active_admissions(
        &self,
    ) -> Result<IngestAdmissionReconstructionReport, IngestLogError> {
        let reconstruction = self.log.reconstruct_admission_namespace().await?;
        let indexed_active_records = self
            .log
            .reconstruct_range_admission_index_active_records()
            .await?;
        let mut active_records_by_key = reconstruction
            .active_records
            .iter()
            .map(|record| (record.admission_record_key.clone(), record.clone()))
            .collect::<HashMap<_, _>>();
        for record in indexed_active_records {
            active_records_by_key.insert(record.admission_record_key.clone(), record);
        }

        Ok(IngestAdmissionReconstructionReport {
            active_admission_records: active_records_by_key.len(),
            expired_orphan_admission_records: reconstruction.expired_orphan_admission_records,
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Coordinator expiry API keeps the durable admission identity explicit at call sites."
    )]
    pub async fn expire_orphan_admission(
        &self,
        stream_id: &str,
        partition_id: u32,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
        decision_id: &str,
        expired_reason: &str,
        operator_id: &str,
    ) -> Result<DurableIngestAdmissionExpiryDecisionRecordV1, IngestLogError> {
        self.durable_admission
            .expire_orphan_admission(
                stream_id,
                partition_id,
                start_offset_inclusive,
                end_offset_exclusive,
                decision_id,
                expired_reason,
                operator_id,
            )
            .await
    }

    fn admission_lock(&self, descriptor: &IngestBatchDescriptor) -> Arc<AsyncMutex<()>> {
        let key = (descriptor.stream_id.clone(), descriptor.partition_id);
        let mut locks = self
            .admission_locks
            .lock()
            .expect("ingest admission lock map poisoned");
        Arc::clone(
            locks
                .entry(key)
                .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
        )
    }
}

impl IngestLog {
    /// Constructs an ingest log without object-store capability validation.
    /// Production/durable callers should use [`Self::new_checked`].
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            relation_catalog: None,
        }
    }

    /// Constructs an ingest log after validating the supplied object-store
    /// profile has the capabilities required by Velorix durability.
    pub fn new_checked(
        store: Arc<dyn ObjectStore>,
        profile: &ObjectStoreCapabilityProfile,
    ) -> Result<Self, ObjectStoreCapabilityError> {
        profile.validate_for_velorix_durability()?;

        Ok(Self::new(store))
    }

    /// Constructs an ingest log from shared startup capability evidence required
    /// for committed ingest writes and catalog-aware relation-catalog reads.
    pub fn new_catalog_checked(
        store: Arc<dyn ObjectStore>,
        capabilities: &AuthoritativeObjectStoreCapabilitiesV1,
    ) -> Result<Self, AuthoritativeObjectStoreCapabilityError> {
        capabilities.validate_namespace(AuthoritativeNamespace::Ingest)?;
        let relation_catalog = RelationCatalogRegistry::new_checked(
            Arc::clone(&store),
            capabilities.validate_namespace(AuthoritativeNamespace::RelationCatalog)?,
        )
        .map_err(
            |source| AuthoritativeObjectStoreCapabilityError::NamespaceProfile {
                namespace: AuthoritativeNamespace::RelationCatalog,
                source,
            },
        )?;

        Ok(Self {
            store,
            relation_catalog: Some(relation_catalog),
        })
    }

    /// Reconstructs the active durable admission records from Velorix-owned
    /// admission and expiry records.
    ///
    /// The reconstruction includes committed records for replay provenance,
    /// retains unexpired reservations, ignores only digest-bound expired
    /// orphans, and fails closed on malformed or unexpected admission namespace
    /// objects.
    pub async fn reconstruct_active_admissions(
        &self,
    ) -> Result<Vec<DurableIngestAdmissionRecordV1>, IngestLogError> {
        self.reconstruct_admission_namespace()
            .await
            .map(|reconstruction| reconstruction.active_records)
    }

    pub async fn append(&self, batch: &IngestBatch) -> Result<(), IngestLogError> {
        let path = Path::from(batch.object_key().as_str());
        let result = self
            .store
            .put_opts(&path, batch.payload.clone().into(), PutMode::Create.into())
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                Err(IngestLogError::AlreadyExists(batch.object_key().clone()))
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Validates an Arrow IPC ingest envelope and appends it as a create-only
    /// durable ingest object without consulting the relation catalog.
    ///
    /// This remains for bootstrap/dev compatibility. Production ingest callers
    /// should use [`Self::append_catalog_validated_envelope`] so relation
    /// identity, schema fingerprint, and Arrow batch schema are checked before
    /// durable append.
    pub async fn append_validated_envelope(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        let batch = IngestBatch::from_validated_envelope(payload)?;
        self.append_validated_batch(batch).await
    }

    /// Validates and appends an Arrow IPC ingest envelope in envelope-only
    /// single-writer admission mode.
    ///
    /// This rejects overlaps visible in already committed ranges before
    /// writing, but it does not consult the relation catalog and is not a
    /// distributed multi-writer range admission guarantee. Production
    /// single-writer callers should use
    /// [`Self::append_catalog_validated_envelope_single_writer`].
    pub async fn append_validated_envelope_single_writer(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        let batch = IngestBatch::from_validated_envelope(payload)?;
        self.append_validated_batch_single_writer(batch).await
    }

    /// Validates an Arrow IPC ingest envelope against the persisted relation
    /// catalog before durable append. Production ingest callers should prefer
    /// this entrypoint so relation identity, schema fingerprint, and Arrow
    /// batch schema are checked before `v1/ingest` is mutated.
    pub async fn append_catalog_validated_envelope(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        let batch = self.catalog_validated_batch(payload).await?;
        self.append_validated_batch(batch).await
    }

    /// Catalog-aware single-writer admission. This keeps the same single-writer
    /// overlap limits as [`Self::append_validated_envelope_single_writer`] and
    /// adds relation catalog validation before writing.
    pub async fn append_catalog_validated_envelope_single_writer(
        &self,
        payload: Bytes,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        let batch = self.catalog_validated_batch(payload).await?;
        self.append_validated_batch_single_writer(batch).await
    }

    async fn catalog_validated_batch(&self, payload: Bytes) -> Result<IngestBatch, IngestLogError> {
        let envelope = IngestEnvelope::decode(payload.clone())?;
        let header = envelope.header();
        let catalog = match &self.relation_catalog {
            Some(registry) => {
                registry
                    .read(&header.relation_id, &header.relation_version)
                    .await?
            }
            None => {
                RelationCatalogRegistry::new(Arc::clone(&self.store))
                    .read(&header.relation_id, &header.relation_version)
                    .await?
            }
        };

        if header.schema_fingerprint != catalog.schema_fingerprint.as_str() {
            return Err(IngestLogError::RelationCatalogMismatch {
                field: "schema_fingerprint",
                expected: catalog.schema_fingerprint.as_str().to_string(),
                actual: header.schema_fingerprint.clone(),
            });
        }

        let batches = envelope.record_batches()?;
        validate_event_time_watermark_against_catalog_and_batches(header, &catalog, &batches)?;
        for batch in batches {
            validate_record_batch_matches_catalog(&catalog, &batch)?;
        }

        IngestBatch::from_validated_envelope(payload)
    }

    async fn append_validated_batch_single_writer(
        &self,
        batch: IngestBatch,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        let descriptor = batch.descriptor();
        if let Some(committed) = self
            .list_committed()
            .await?
            .into_iter()
            .find(|committed| ranges_overlap(committed, &descriptor))
        {
            return Ok(AppendValidatedEnvelopeOutcome::Conflict {
                descriptor,
                object_key: committed.object_key,
                reason: "range_overlap_committed",
            });
        }

        self.append_validated_batch(batch).await
    }

    async fn append_validated_batch(
        &self,
        batch: IngestBatch,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        let descriptor = batch.descriptor();

        match self.append(&batch).await {
            Ok(()) => Ok(AppendValidatedEnvelopeOutcome::Appended { descriptor }),
            Err(IngestLogError::AlreadyExists(object_key)) => {
                let existing = self
                    .store
                    .get(&Path::from(object_key.as_str()))
                    .await?
                    .bytes()
                    .await?;
                let existing = IngestEnvelope::decode(existing)?;
                let incoming = IngestEnvelope::decode(batch.payload.clone())?;
                if existing.header().payload_digest == incoming.header().payload_digest {
                    Ok(AppendValidatedEnvelopeOutcome::Duplicate { descriptor })
                } else {
                    Ok(AppendValidatedEnvelopeOutcome::Conflict {
                        descriptor,
                        reason: "same_key_different_digest",
                        object_key,
                    })
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn existing_batch_conflict(
        &self,
        batch: &IngestBatch,
    ) -> Result<Option<AppendValidatedEnvelopeOutcome>, IngestLogError> {
        match self
            .store
            .get(&Path::from(batch.object_key().as_str()))
            .await
        {
            Ok(existing) => {
                let existing = IngestEnvelope::decode(existing.bytes().await?)?;
                let incoming = IngestEnvelope::decode(batch.payload.clone())?;
                let descriptor = batch.descriptor();
                if existing.header().payload_digest == incoming.header().payload_digest {
                    Ok(Some(AppendValidatedEnvelopeOutcome::Duplicate {
                        descriptor,
                    }))
                } else {
                    Ok(Some(AppendValidatedEnvelopeOutcome::Conflict {
                        descriptor,
                        reason: "same_key_different_digest",
                        object_key: batch.object_key().clone(),
                    }))
                }
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn list_committed(&self) -> Result<Vec<IngestBatchDescriptor>, IngestLogError> {
        let mut objects = self
            .store
            .list(Some(&Path::from(INGEST_PREFIX)))
            .try_collect::<Vec<_>>()
            .await?;

        objects.sort_by(|left, right| left.location.cmp(&right.location));

        let descriptors = objects
            .into_iter()
            .map(|object| parse_ingest_descriptor(object.location.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;

        validate_non_overlapping_ranges(&descriptors)?;

        Ok(descriptors)
    }

    async fn list_admission_records(
        &self,
    ) -> Result<Vec<DurableIngestAdmissionRecordV1>, IngestLogError> {
        self.list_admission_record_bodies()
            .await
            .map(|records| records.into_iter().map(|(_, record)| record).collect())
    }

    async fn list_admission_record_bodies(
        &self,
    ) -> Result<Vec<(Bytes, DurableIngestAdmissionRecordV1)>, IngestLogError> {
        let mut objects = self
            .store
            .list(Some(&Path::from("v1/ingest-admission")))
            .try_collect::<Vec<_>>()
            .await?;

        objects.sort_by(|left, right| left.location.cmp(&right.location));

        let mut records = Vec::with_capacity(objects.len());
        for object in objects {
            if object.location.as_ref().ends_with(".expiry.json") {
                ObjectKey::parse(object.location.to_string()).map_err(|error| {
                    IngestLogError::MalformedIngestAdmissionExpiryDecision {
                        key: object.location.to_string(),
                        reason: error.to_string(),
                    }
                })?;
                continue;
            }
            if !object.location.as_ref().ends_with(".admission.json") {
                return Err(IngestLogError::MalformedIngestAdmissionRecord {
                    key: object.location.to_string(),
                    reason: "unexpected object under v1/ingest-admission".to_string(),
                });
            }
            ObjectKey::parse(object.location.to_string()).map_err(|error| {
                IngestLogError::MalformedIngestAdmissionRecord {
                    key: object.location.to_string(),
                    reason: error.to_string(),
                }
            })?;
            let bytes = self.store.get(&object.location).await?.bytes().await?;
            let record: DurableIngestAdmissionRecordV1 = serde_json::from_slice(&bytes)?;
            record.validate_key()?;
            if object.location != Path::from(record.admission_record_key.as_str()) {
                return Err(IngestLogError::MalformedIngestAdmissionRecord {
                    key: object.location.to_string(),
                    reason: format!(
                        "stored path does not match body admission_record_key `{}`",
                        record.admission_record_key
                    ),
                });
            }
            records.push((bytes, record));
        }

        Ok(records)
    }

    async fn list_admission_expiry_decisions(
        &self,
    ) -> Result<Vec<DurableIngestAdmissionExpiryDecisionRecordV1>, IngestLogError> {
        let mut objects = self
            .store
            .list(Some(&Path::from("v1/ingest-admission")))
            .try_collect::<Vec<_>>()
            .await?;

        objects.sort_by(|left, right| left.location.cmp(&right.location));

        let mut records = Vec::with_capacity(objects.len());
        for object in objects {
            if object.location.as_ref().ends_with(".admission.json") {
                ObjectKey::parse(object.location.to_string()).map_err(|error| {
                    IngestLogError::MalformedIngestAdmissionRecord {
                        key: object.location.to_string(),
                        reason: error.to_string(),
                    }
                })?;
                continue;
            }
            if !object.location.as_ref().ends_with(".expiry.json") {
                return Err(IngestLogError::MalformedIngestAdmissionExpiryDecision {
                    key: object.location.to_string(),
                    reason: "unexpected object under v1/ingest-admission".to_string(),
                });
            }
            ObjectKey::parse(object.location.to_string()).map_err(|error| {
                IngestLogError::MalformedIngestAdmissionExpiryDecision {
                    key: object.location.to_string(),
                    reason: error.to_string(),
                }
            })?;
            let bytes = self.store.get(&object.location).await?.bytes().await?;
            let record: DurableIngestAdmissionExpiryDecisionRecordV1 =
                serde_json::from_slice(&bytes)?;
            record.validate_key()?;
            if object.location != Path::from(record.expiry_decision_key.as_str()) {
                return Err(IngestLogError::MalformedIngestAdmissionExpiryDecision {
                    key: object.location.to_string(),
                    reason: format!(
                        "stored path does not match body expiry_decision_key `{}`",
                        record.expiry_decision_key
                    ),
                });
            }
            records.push(record);
        }

        Ok(records)
    }

    async fn list_active_admission_records(
        &self,
    ) -> Result<Vec<DurableIngestAdmissionRecordV1>, IngestLogError> {
        self.reconstruct_admission_namespace()
            .await
            .map(|reconstruction| reconstruction.active_records)
    }

    async fn list_range_admission_index_transitions(
        &self,
        stream_id: &str,
        partition_id: u32,
        require_materialized_admission: bool,
    ) -> Result<Vec<RangeAdmissionIndexTransitionRecordV1>, IngestLogError> {
        let prefix = range_admission_index_partition_prefix(stream_id, partition_id);
        let mut objects = self
            .store
            .list(Some(&Path::from(prefix.as_str())))
            .try_collect::<Vec<_>>()
            .await?;

        objects.sort_by(|left, right| left.location.cmp(&right.location));

        let mut transitions = Vec::with_capacity(objects.len());
        for object in objects {
            let key = object.location.to_string();
            if !key.ends_with(".transition.json") {
                return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                    key,
                    reason: "unexpected object under v1/ingest-admission-index".to_string(),
                });
            }

            let bytes = self.store.get(&object.location).await?.bytes().await?;
            let transition: RangeAdmissionIndexTransitionRecordV1 = serde_json::from_slice(&bytes)?;
            transition.validate_key(&key)?;
            self.validate_index_transition_materialized_admission(
                &transition,
                require_materialized_admission,
            )
            .await?;
            transitions.push(transition);
        }

        Ok(transitions)
    }

    async fn validate_index_transition_materialized_admission(
        &self,
        transition: &RangeAdmissionIndexTransitionRecordV1,
        require_materialized_admission: bool,
    ) -> Result<(), IngestLogError> {
        let path = Path::from(transition.admitted.admission_record_key.as_str());
        let bytes = match self.store.get(&path).await {
            Ok(result) => result.bytes().await?,
            Err(object_store::Error::NotFound { .. }) if !require_materialized_admission => {
                return Ok(());
            }
            Err(object_store::Error::NotFound { .. }) => {
                return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                    key: range_admission_index_transition_key(
                        &transition.stream_id,
                        transition.partition_id,
                        &transition.previous_state_digest,
                    )?,
                    reason: "indexed transition is missing materialized admission".to_string(),
                });
            }
            Err(error) => return Err(error.into()),
        };
        let materialized: DurableIngestAdmissionRecordV1 = serde_json::from_slice(&bytes)?;
        materialized.validate_key()?;
        let expected_bytes = Bytes::from(serde_json::to_vec(&transition.admitted)?);

        if materialized != transition.admitted || bytes != expected_bytes {
            return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                key: range_admission_index_transition_key(
                    &transition.stream_id,
                    transition.partition_id,
                    &transition.previous_state_digest,
                )?,
                reason: "materialized admission does not match indexed transition".to_string(),
            });
        }

        Ok(())
    }

    async fn load_range_admission_index_state(
        &self,
        record: &DurableIngestAdmissionRecordV1,
    ) -> Result<RangeAdmissionIndexState, IngestLogError> {
        record.validate_key()?;
        self.load_range_admission_index_state_for_partition(
            &record.stream_id,
            record.partition_id,
            false,
        )
        .await
    }

    async fn load_range_admission_index_state_for_partition(
        &self,
        stream_id: &str,
        partition_id: u32,
        require_materialized_admission: bool,
    ) -> Result<RangeAdmissionIndexState, IngestLogError> {
        let transitions = self
            .list_range_admission_index_transitions(
                stream_id,
                partition_id,
                require_materialized_admission,
            )
            .await?;
        let indexed_expired_admission_keys = self
            .list_expired_orphan_admission_keys(stream_id, partition_id)
            .await?;
        let mut indexed_admissions = self
            .list_admission_records()
            .await?
            .into_iter()
            .filter(|admission| {
                admission.stream_id == stream_id && admission.partition_id == partition_id
            })
            .collect::<Vec<_>>();
        sort_admission_records(&mut indexed_admissions);
        let mut active_admissions = self
            .list_active_admission_records()
            .await?
            .into_iter()
            .filter(|active| active.stream_id == stream_id && active.partition_id == partition_id)
            .collect::<Vec<_>>();
        sort_admission_records(&mut active_admissions);
        let mut state = RangeAdmissionIndexState {
            state_digest: range_admission_index_state_digest(
                stream_id,
                partition_id,
                &indexed_admissions,
            )?,
            indexed_admissions,
            active_admissions,
            indexed_expired_admission_keys,
        };

        let mut transitions_by_previous: HashMap<
            String,
            Vec<RangeAdmissionIndexTransitionRecordV1>,
        > = HashMap::with_capacity(transitions.len());
        for transition in transitions {
            transitions_by_previous
                .entry(transition.previous_state_digest.clone())
                .or_default()
                .push(transition);
        }

        while let Some(mut transitions) = transitions_by_previous.remove(&state.state_digest) {
            if transitions.len() > 1 {
                return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
                    key: range_admission_index_partition_prefix(stream_id, partition_id),
                    reason: "multiple transitions share previous_state_digest".to_string(),
                });
            }
            let transition = transitions.pop().expect("transition bucket is nonempty");
            transition.apply_to_state(&mut state)?;
        }

        Ok(state)
    }

    async fn reconstruct_range_admission_index_active_records(
        &self,
    ) -> Result<Vec<DurableIngestAdmissionRecordV1>, IngestLogError> {
        let mut records_by_key = HashMap::new();
        for (stream_id, partition_id) in self.list_range_admission_index_partitions().await? {
            let state = self
                .load_range_admission_index_state_for_partition(&stream_id, partition_id, true)
                .await?;
            for record in state.active_admissions {
                records_by_key.insert(record.admission_record_key.clone(), record);
            }
        }

        let mut records = records_by_key.into_values().collect::<Vec<_>>();
        sort_admission_records(&mut records);
        Ok(records)
    }

    async fn list_range_admission_index_partitions(
        &self,
    ) -> Result<Vec<(String, u32)>, IngestLogError> {
        let mut objects = self
            .store
            .list(Some(&Path::from("v1/ingest-admission-index")))
            .try_collect::<Vec<_>>()
            .await?;
        objects.sort_by(|left, right| left.location.cmp(&right.location));

        let mut partitions = HashSet::new();
        for object in objects {
            partitions.insert(parse_range_admission_index_partition(
                object.location.as_ref(),
            )?);
        }

        let mut partitions = partitions.into_iter().collect::<Vec<_>>();
        partitions.sort();
        Ok(partitions)
    }

    async fn list_expired_orphan_admission_keys(
        &self,
        stream_id: &str,
        partition_id: u32,
    ) -> Result<HashSet<ObjectKey>, IngestLogError> {
        let expiry_decisions = self.list_admission_expiry_decisions().await?;
        let mut expired = HashSet::new();

        for (admission_bytes, admission) in self.list_admission_record_bodies().await? {
            if admission.stream_id != stream_id || admission.partition_id != partition_id {
                continue;
            }
            if self.object_exists(&admission.batch_key).await? {
                continue;
            }
            if expiry_decisions
                .iter()
                .any(|decision| decision.expires_admission(&admission_bytes, &admission))
            {
                expired.insert(admission.admission_record_key);
            }
        }

        Ok(expired)
    }

    async fn reconstruct_admission_namespace(
        &self,
    ) -> Result<IngestAdmissionReconstruction, IngestLogError> {
        let committed_by_key = self
            .list_committed()
            .await?
            .into_iter()
            .map(|descriptor| (descriptor.object_key.clone(), descriptor))
            .collect::<HashMap<_, _>>();
        let expiry_decisions = self.list_admission_expiry_decisions().await?;
        let mut active_records = Vec::new();
        let mut expired_orphan_admission_records = 0;

        for (admission_bytes, admission) in self.list_admission_record_bodies().await? {
            let committed = committed_by_key.get(&admission.batch_key);
            let expired = expiry_decisions
                .iter()
                .any(|decision| decision.expires_admission(&admission_bytes, &admission));

            if let Some(descriptor) = committed {
                self.validate_committed_admission(&admission, descriptor)
                    .await?;
                active_records.push(admission);
            } else if !expired {
                active_records.push(admission);
            } else {
                expired_orphan_admission_records += 1;
            }
        }

        Ok(IngestAdmissionReconstruction {
            active_records,
            expired_orphan_admission_records,
        })
    }

    async fn admission_has_matching_expiry(
        &self,
        admission_bytes: &Bytes,
        admission: &DurableIngestAdmissionRecordV1,
    ) -> Result<bool, IngestLogError> {
        Ok(self
            .list_admission_expiry_decisions()
            .await?
            .iter()
            .any(|decision| decision.expires_admission(admission_bytes, admission)))
    }

    async fn validate_committed_admission(
        &self,
        admission: &DurableIngestAdmissionRecordV1,
        descriptor: &IngestBatchDescriptor,
    ) -> Result<(), IngestLogError> {
        let bytes = self
            .store
            .get(&Path::from(descriptor.object_key.as_str()))
            .await?
            .bytes()
            .await?;
        let envelope = IngestEnvelope::decode(bytes)?;
        envelope.validate_descriptor(descriptor)?;
        validate_admission_matches_replayed_batch(admission, descriptor, &envelope)
    }

    async fn object_exists(&self, object_key: &ObjectKey) -> Result<bool, IngestLogError> {
        match self.store.get(&Path::from(object_key.as_str())).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Replays committed batches without validating payload bytes.
    ///
    /// This remains for bootstrap/local compatibility while runtime replay is
    /// still migrating off the pre-envelope JSON path. Production envelope
    /// callers should use [`Self::replay_validated_envelopes_from`] so corrupt
    /// or mismatched committed objects fail closed before replay.
    pub async fn replay_from(
        &self,
        checkpoints: &[ReplayCheckpoint],
    ) -> Result<Vec<IngestBatch>, IngestLogError> {
        let checkpoint_offsets = validate_checkpoints(checkpoints)?;

        let mut batches = Vec::new();
        for descriptor in self.list_committed().await? {
            let checkpoint_end = checkpoint_offsets
                .get(&(descriptor.stream_id.clone(), descriptor.partition_id))
                .copied()
                .unwrap_or(0);

            if descriptor.end_offset_exclusive <= checkpoint_end {
                continue;
            }

            if descriptor.start_offset_inclusive < checkpoint_end
                && checkpoint_end < descriptor.end_offset_exclusive
            {
                return Err(IngestLogError::CheckpointInsideBatch {
                    checkpoint_end_offset_exclusive: checkpoint_end,
                    object_key: descriptor.object_key,
                });
            }

            let bytes = self
                .store
                .get(&Path::from(descriptor.object_key.as_str()))
                .await?
                .bytes()
                .await?;
            batches.push(IngestBatch::from_descriptor(descriptor, bytes));
        }

        Ok(batches)
    }

    /// Replays committed batches only after validating each object body as a
    /// V1 Arrow IPC ingest envelope and matching its authoritative header
    /// against the deterministic object key descriptor.
    pub async fn replay_validated_envelopes_from(
        &self,
        checkpoints: &[ReplayCheckpoint],
    ) -> Result<Vec<IngestBatch>, IngestLogError> {
        let checkpoint_offsets = validate_checkpoints(checkpoints)?;

        let mut batches = Vec::new();
        for descriptor in self.list_committed().await? {
            let checkpoint_end = checkpoint_offsets
                .get(&(descriptor.stream_id.clone(), descriptor.partition_id))
                .copied()
                .unwrap_or(0);

            if descriptor.end_offset_exclusive <= checkpoint_end {
                continue;
            }

            if descriptor.start_offset_inclusive < checkpoint_end
                && checkpoint_end < descriptor.end_offset_exclusive
            {
                return Err(IngestLogError::CheckpointInsideBatch {
                    checkpoint_end_offset_exclusive: checkpoint_end,
                    object_key: descriptor.object_key,
                });
            }

            let bytes = self
                .store
                .get(&Path::from(descriptor.object_key.as_str()))
                .await?
                .bytes()
                .await?;
            let envelope = IngestEnvelope::decode(bytes.clone())?;
            envelope.validate_descriptor(&descriptor)?;
            batches.push(IngestBatch::from_descriptor(descriptor, bytes));
        }

        Ok(batches)
    }

    /// Replays committed V1 Arrow IPC ingest envelopes only when each replayed
    /// batch is backed by matching durable admission evidence. This is the
    /// checked production recovery path; bootstrap/local recovery remains on
    /// the non-admitted replay APIs above while old data is migrated.
    pub async fn replay_admitted_validated_envelopes_from(
        &self,
        checkpoints: &[ReplayCheckpoint],
    ) -> Result<Vec<IngestBatch>, IngestLogError> {
        let checkpoint_offsets = validate_checkpoints(checkpoints)?;
        let committed = self.list_committed().await?;
        let committed_by_key = committed
            .iter()
            .map(|descriptor| (descriptor.object_key.clone(), descriptor.clone()))
            .collect::<HashMap<_, _>>();
        let mut replay_admissions = HashMap::new();

        for admission in self.list_admission_records().await? {
            let admission_descriptor = admission.descriptor()?;
            if !committed_by_key.contains_key(&admission.batch_key) {
                continue;
            }

            if !admission_in_replay_window(
                &admission_descriptor,
                &admission.admission_record_key,
                &checkpoint_offsets,
            )? {
                continue;
            }

            replay_admissions.insert(admission.batch_key.clone(), admission);
        }

        let mut batches = Vec::new();
        for descriptor in committed {
            if !batch_in_replay_window(&descriptor, &checkpoint_offsets)? {
                continue;
            }

            let bytes = self
                .store
                .get(&Path::from(descriptor.object_key.as_str()))
                .await?
                .bytes()
                .await?;
            let envelope = IngestEnvelope::decode(bytes.clone())?;
            envelope.validate_descriptor(&descriptor)?;
            let Some(admission) = replay_admissions.remove(&descriptor.object_key) else {
                return Err(IngestLogError::MissingIngestAdmissionRecord {
                    batch_key: descriptor.object_key,
                });
            };
            validate_admission_matches_replayed_batch(&admission, &descriptor, &envelope)?;
            batches.push(IngestBatch::from_descriptor(descriptor, bytes));
        }

        Ok(batches)
    }
}

fn ranges_overlap(previous: &IngestBatchDescriptor, current: &IngestBatchDescriptor) -> bool {
    previous.stream_id == current.stream_id
        && previous.partition_id == current.partition_id
        && previous.start_offset_inclusive < current.end_offset_exclusive
        && current.start_offset_inclusive < previous.end_offset_exclusive
        && previous.object_key != current.object_key
}

fn admission_conflict(
    active_admissions: &[DurableIngestAdmissionRecordV1],
    record: &DurableIngestAdmissionRecordV1,
) -> Result<AdmissionReservationOutcome, IngestLogError> {
    let descriptor = record.descriptor()?;
    for candidate in active_admissions {
        let candidate_descriptor = candidate.descriptor()?;
        if candidate.admission_record_key == record.admission_record_key {
            if candidate == record {
                return Ok(AdmissionReservationOutcome::Duplicate);
            }

            return Ok(AdmissionReservationOutcome::Conflict {
                object_key: candidate.batch_key.clone(),
                reason: "same_range_different_digest_reserved",
            });
        }

        if candidate_descriptor.stream_id == descriptor.stream_id
            && candidate_descriptor.partition_id == descriptor.partition_id
            && candidate_descriptor.start_offset_inclusive < descriptor.end_offset_exclusive
            && descriptor.start_offset_inclusive < candidate_descriptor.end_offset_exclusive
        {
            return Ok(AdmissionReservationOutcome::Conflict {
                object_key: candidate.batch_key.clone(),
                reason: "range_overlap_reserved",
            });
        }
    }

    Ok(AdmissionReservationOutcome::Admitted)
}

fn index_probe_record_for_descriptor(
    descriptor: &IngestBatchDescriptor,
) -> Result<DurableIngestAdmissionRecordV1, IngestLogError> {
    let record = DurableIngestAdmissionRecordV1 {
        schema_version: 1,
        record_kind: INGEST_ADMISSION_RECORD_KIND_V1.to_string(),
        stream_id: descriptor.stream_id.clone(),
        partition_id: descriptor.partition_id,
        start_offset_inclusive: descriptor.start_offset_inclusive,
        end_offset_exclusive: descriptor.end_offset_exclusive,
        event_time_watermark: None,
        batch_key: descriptor.object_key.clone(),
        admission_record_key: ObjectKey::ingest_admission_record(
            &descriptor.stream_id,
            descriptor.partition_id,
            descriptor.start_offset_inclusive,
            descriptor.end_offset_exclusive,
        )?,
        payload_digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
            .to_string(),
        relation_id: "index_probe".to_string(),
        relation_version: "index_probe".to_string(),
        schema_fingerprint:
            "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        admission_mode: "process_local_serialized".to_string(),
        commit_guard_binding: None,
    };
    record.validate_key()?;
    Ok(record)
}

fn range_admission_index_transition(
    record: &DurableIngestAdmissionRecordV1,
    state: &RangeAdmissionIndexState,
) -> Result<RangeAdmissionIndexTransitionRecordV1, IngestLogError> {
    let mut indexed_admissions = state.indexed_admissions.clone();
    indexed_admissions.push(record.clone());
    sort_admission_records(&mut indexed_admissions);
    let next_state_digest = range_admission_index_state_digest(
        &record.stream_id,
        record.partition_id,
        &indexed_admissions,
    )?;

    Ok(RangeAdmissionIndexTransitionRecordV1 {
        schema_version: 1,
        record_kind: INGEST_ADMISSION_INDEX_TRANSITION_RECORD_KIND_V1.to_string(),
        stream_id: record.stream_id.clone(),
        partition_id: record.partition_id,
        previous_state_digest: state.state_digest.clone(),
        next_state_digest,
        admitted: record.clone(),
    })
}

fn range_admission_index_state_digest(
    stream_id: &str,
    partition_id: u32,
    active_admissions: &[DurableIngestAdmissionRecordV1],
) -> Result<String, IngestLogError> {
    let state = RangeAdmissionIndexStateDigestV1 {
        schema_version: 1,
        record_kind: "ingest_admission_index_state_v1",
        stream_id,
        partition_id,
        active_admissions,
    };
    Ok(digest_bytes(&Bytes::from(serde_json::to_vec(&state)?)))
}

fn range_admission_index_partition_prefix(stream_id: &str, partition_id: u32) -> String {
    format!("v1/ingest-admission-index/{stream_id}/p={partition_id:010}/advances")
}

fn parse_range_admission_index_partition(key: &str) -> Result<(String, u32), IngestLogError> {
    let segments = key.split('/').collect::<Vec<_>>();
    let ["v1", "ingest-admission-index", stream_id, partition, "advances", transition_file] =
        segments.as_slice()
    else {
        return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
            key: key.to_string(),
            reason: "unexpected object under v1/ingest-admission-index".to_string(),
        });
    };
    if stream_id.is_empty() {
        return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
            key: key.to_string(),
            reason: "stream_id must be nonempty".to_string(),
        });
    }
    let Some(partition) = partition.strip_prefix("p=") else {
        return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
            key: key.to_string(),
            reason: "partition segment must start with p=".to_string(),
        });
    };
    if partition.len() != 10 || !partition.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
            key: key.to_string(),
            reason: "partition segment must be p=<10 digits>".to_string(),
        });
    }
    if transition_file
        .strip_suffix(".transition.json")
        .filter(|digest| digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .is_none()
    {
        return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
            key: key.to_string(),
            reason: "transition file must be <64 hex chars>.transition.json".to_string(),
        });
    }

    Ok((
        (*stream_id).to_string(),
        partition.parse::<u32>().map_err(|error| {
            IngestLogError::MalformedIngestAdmissionIndexTransition {
                key: key.to_string(),
                reason: format!("invalid partition id: {error}"),
            }
        })?,
    ))
}

fn range_admission_index_transition_key(
    stream_id: &str,
    partition_id: u32,
    previous_state_digest: &str,
) -> Result<String, IngestLogError> {
    let Some(previous_state_digest_hex) = previous_state_digest.strip_prefix("sha256:") else {
        return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
            key: range_admission_index_partition_prefix(stream_id, partition_id),
            reason: "previous_state_digest must be sha256:<64 hex chars>".to_string(),
        });
    };
    if previous_state_digest_hex.len() != 64
        || !previous_state_digest_hex
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(IngestLogError::MalformedIngestAdmissionIndexTransition {
            key: range_admission_index_partition_prefix(stream_id, partition_id),
            reason: "previous_state_digest must be sha256:<64 hex chars>".to_string(),
        });
    }

    Ok(format!(
        "{}/{previous_state_digest_hex}.transition.json",
        range_admission_index_partition_prefix(stream_id, partition_id)
    ))
}

fn sort_admission_records(records: &mut [DurableIngestAdmissionRecordV1]) {
    records.sort_by(|left, right| {
        (
            &left.stream_id,
            left.partition_id,
            left.start_offset_inclusive,
            left.end_offset_exclusive,
            &left.admission_record_key,
        )
            .cmp(&(
                &right.stream_id,
                right.partition_id,
                right.start_offset_inclusive,
                right.end_offset_exclusive,
                &right.admission_record_key,
            ))
    });
}

fn admission_record_for_batch(
    batch: &IngestBatch,
) -> Result<DurableIngestAdmissionRecordV1, IngestLogError> {
    admission_record_for_batch_with_commit_guard(batch, None)
}

fn admission_record_for_batch_with_commit_guard(
    batch: &IngestBatch,
    commit_guard: Option<&dyn IngestCommitGuard>,
) -> Result<DurableIngestAdmissionRecordV1, IngestLogError> {
    let envelope = IngestEnvelope::decode(batch.payload.clone())?;
    let header = envelope.header();
    let descriptor = batch.descriptor();
    let commit_guard_binding = commit_guard.and_then(|guard| guard.admission_binding(&descriptor));
    let admission_record_key = ObjectKey::ingest_admission_record(
        &descriptor.stream_id,
        descriptor.partition_id,
        descriptor.start_offset_inclusive,
        descriptor.end_offset_exclusive,
    )?;

    let record = DurableIngestAdmissionRecordV1 {
        schema_version: 1,
        record_kind: INGEST_ADMISSION_RECORD_KIND_V1.to_string(),
        stream_id: descriptor.stream_id,
        partition_id: descriptor.partition_id,
        start_offset_inclusive: descriptor.start_offset_inclusive,
        end_offset_exclusive: descriptor.end_offset_exclusive,
        event_time_watermark: header.event_time_watermark.clone(),
        batch_key: descriptor.object_key,
        admission_record_key,
        payload_digest: header.payload_digest.clone(),
        relation_id: header.relation_id.clone(),
        relation_version: header.relation_version.clone(),
        schema_fingerprint: header.schema_fingerprint.clone(),
        admission_mode: "process_local_serialized".to_string(),
        commit_guard_binding,
    };
    record.validate_key()?;

    Ok(record)
}

fn expiry_decision_record_for_admission(
    admission_record_bytes: &Bytes,
    admission: &DurableIngestAdmissionRecordV1,
    decision_id: &str,
    expired_reason: &str,
    operator_id: &str,
) -> Result<DurableIngestAdmissionExpiryDecisionRecordV1, IngestLogError> {
    let expiry_decision_key = ObjectKey::ingest_admission_orphan_expiry_decision(
        &admission.stream_id,
        admission.partition_id,
        admission.start_offset_inclusive,
        admission.end_offset_exclusive,
        decision_id,
    )?;
    let record = DurableIngestAdmissionExpiryDecisionRecordV1 {
        schema_version: 1,
        record_kind: INGEST_ADMISSION_EXPIRY_DECISION_RECORD_KIND_V1.to_string(),
        stream_id: admission.stream_id.clone(),
        partition_id: admission.partition_id,
        start_offset_inclusive: admission.start_offset_inclusive,
        end_offset_exclusive: admission.end_offset_exclusive,
        batch_key: admission.batch_key.clone(),
        admission_record_key: admission.admission_record_key.clone(),
        observed_missing_batch_key: admission.batch_key.clone(),
        expiry_decision_key,
        admission_record_digest: digest_bytes(admission_record_bytes),
        expired_reason: expired_reason.to_string(),
        operator_id: operator_id.to_string(),
    };
    record.validate_key()?;

    Ok(record)
}

fn batch_in_replay_window(
    descriptor: &IngestBatchDescriptor,
    checkpoint_offsets: &HashMap<(String, u32), u64>,
) -> Result<bool, IngestLogError> {
    let checkpoint_end = checkpoint_offsets
        .get(&(descriptor.stream_id.clone(), descriptor.partition_id))
        .copied()
        .unwrap_or(0);

    if descriptor.end_offset_exclusive <= checkpoint_end {
        return Ok(false);
    }

    if descriptor.start_offset_inclusive < checkpoint_end
        && checkpoint_end < descriptor.end_offset_exclusive
    {
        return Err(IngestLogError::CheckpointInsideBatch {
            checkpoint_end_offset_exclusive: checkpoint_end,
            object_key: descriptor.object_key.clone(),
        });
    }

    Ok(true)
}

fn validate_event_time_watermark_against_catalog_and_batches(
    header: &crate::ingest_envelope::IngestEnvelopeHeader,
    catalog: &VelorixRelationCatalogV1,
    batches: &[RecordBatch],
) -> Result<(), IngestLogError> {
    let Some(watermark) = &header.event_time_watermark else {
        return Ok(());
    };
    let Some(event_time_column_id) = &catalog.relation_schema.event_time_column_id else {
        return Err(IngestLogError::RelationCatalogMismatch {
            field: "event_time_watermark.event_time_column_id",
            expected: "declared relation_schema.event_time_column_id".to_string(),
            actual: watermark.event_time_column_id.clone(),
        });
    };
    if watermark.event_time_column_id != *event_time_column_id {
        return Err(IngestLogError::RelationCatalogMismatch {
            field: "event_time_watermark.event_time_column_id",
            expected: event_time_column_id.clone(),
            actual: watermark.event_time_column_id.clone(),
        });
    }
    let Some(column) = catalog
        .relation_schema
        .columns
        .iter()
        .find(|column| column.column_id == *event_time_column_id)
    else {
        return Err(IngestLogError::RelationCatalogMismatch {
            field: "event_time_watermark.event_time_column_id",
            expected: "catalog column".to_string(),
            actual: watermark.event_time_column_id.clone(),
        });
    };
    let actual_max = match &column.physical_arrow_type {
        ArrowPhysicalTypeV1::Int64 => max_int64_column(batches, &column.name),
        ArrowPhysicalTypeV1::Date32 => max_date32_column(batches, &column.name).map(i64::from),
        ArrowPhysicalTypeV1::TimestampNanosecond { .. } => {
            max_timestamp_column(batches, &column.name)
        }
        other => {
            return Err(IngestLogError::RelationCatalogMismatch {
                field: "event_time_watermark.event_time_column_type",
                expected: "Int64, Date32, or TimestampNanosecond".to_string(),
                actual: format!("{other:?}"),
            });
        }
    }?;
    if watermark.max_observed_event_time_ns < actual_max {
        return Err(IngestLogError::RelationCatalogMismatch {
            field: "event_time_watermark.max_observed_event_time_ns",
            expected: actual_max.to_string(),
            actual: watermark.max_observed_event_time_ns.to_string(),
        });
    }
    Ok(())
}

fn max_int64_column(batches: &[RecordBatch], name: &str) -> Result<i64, IngestLogError> {
    let mut max_value = None;
    for batch in batches {
        let array = batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| IngestLogError::RelationCatalogMismatch {
                field: "event_time_watermark.event_time_column",
                expected: "Int64".to_string(),
                actual: name.to_string(),
            })?;
        for row in 0..array.len() {
            if !array.is_null(row) {
                max_value = Some(max_value.map_or(array.value(row), |current: i64| {
                    current.max(array.value(row))
                }));
            }
        }
    }
    max_value.ok_or_else(|| IngestLogError::RelationCatalogMismatch {
        field: "event_time_watermark.event_time_column",
        expected: "at least one non-null event-time value".to_string(),
        actual: name.to_string(),
    })
}

fn max_date32_column(batches: &[RecordBatch], name: &str) -> Result<i32, IngestLogError> {
    let mut max_value = None;
    for batch in batches {
        let array = batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Date32Array>())
            .ok_or_else(|| IngestLogError::RelationCatalogMismatch {
                field: "event_time_watermark.event_time_column",
                expected: "Date32".to_string(),
                actual: name.to_string(),
            })?;
        for row in 0..array.len() {
            if !array.is_null(row) {
                max_value = Some(max_value.map_or(array.value(row), |current: i32| {
                    current.max(array.value(row))
                }));
            }
        }
    }
    max_value.ok_or_else(|| IngestLogError::RelationCatalogMismatch {
        field: "event_time_watermark.event_time_column",
        expected: "at least one non-null event-time value".to_string(),
        actual: name.to_string(),
    })
}

fn max_timestamp_column(batches: &[RecordBatch], name: &str) -> Result<i64, IngestLogError> {
    let mut max_value = None;
    for batch in batches {
        let array = batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<TimestampNanosecondArray>())
            .ok_or_else(|| IngestLogError::RelationCatalogMismatch {
                field: "event_time_watermark.event_time_column",
                expected: "TimestampNanosecond".to_string(),
                actual: name.to_string(),
            })?;
        for row in 0..array.len() {
            if !array.is_null(row) {
                max_value = Some(max_value.map_or(array.value(row), |current: i64| {
                    current.max(array.value(row))
                }));
            }
        }
    }
    max_value.ok_or_else(|| IngestLogError::RelationCatalogMismatch {
        field: "event_time_watermark.event_time_column",
        expected: "at least one non-null event-time value".to_string(),
        actual: name.to_string(),
    })
}

fn admission_in_replay_window(
    descriptor: &IngestBatchDescriptor,
    admission_record_key: &ObjectKey,
    checkpoint_offsets: &HashMap<(String, u32), u64>,
) -> Result<bool, IngestLogError> {
    let checkpoint_end = checkpoint_offsets
        .get(&(descriptor.stream_id.clone(), descriptor.partition_id))
        .copied()
        .unwrap_or(0);

    if descriptor.end_offset_exclusive <= checkpoint_end {
        return Ok(false);
    }

    if descriptor.start_offset_inclusive < checkpoint_end
        && checkpoint_end < descriptor.end_offset_exclusive
    {
        return Err(IngestLogError::CheckpointInsideAdmittedRange {
            checkpoint_end_offset_exclusive: checkpoint_end,
            admission_record_key: admission_record_key.clone(),
        });
    }

    Ok(true)
}

fn validate_admission_matches_replayed_batch(
    admission: &DurableIngestAdmissionRecordV1,
    descriptor: &IngestBatchDescriptor,
    envelope: &IngestEnvelope,
) -> Result<(), IngestLogError> {
    let header = envelope.header();
    validate_admission_field(
        admission,
        descriptor,
        "batch_key",
        descriptor.object_key.as_str(),
        admission.batch_key.as_str(),
    )?;
    validate_admission_field(
        admission,
        descriptor,
        "stream_id",
        &descriptor.stream_id,
        &admission.stream_id,
    )?;
    validate_admission_field(
        admission,
        descriptor,
        "partition_id",
        &descriptor.partition_id.to_string(),
        &admission.partition_id.to_string(),
    )?;
    validate_admission_field(
        admission,
        descriptor,
        "start_offset_inclusive",
        &descriptor.start_offset_inclusive.to_string(),
        &admission.start_offset_inclusive.to_string(),
    )?;
    validate_admission_field(
        admission,
        descriptor,
        "end_offset_exclusive",
        &descriptor.end_offset_exclusive.to_string(),
        &admission.end_offset_exclusive.to_string(),
    )?;
    validate_admission_field(
        admission,
        descriptor,
        "payload_digest",
        &header.payload_digest,
        &admission.payload_digest,
    )?;
    validate_admission_field(
        admission,
        descriptor,
        "relation_id",
        &header.relation_id,
        &admission.relation_id,
    )?;
    validate_admission_field(
        admission,
        descriptor,
        "relation_version",
        &header.relation_version,
        &admission.relation_version,
    )?;
    validate_admission_field(
        admission,
        descriptor,
        "schema_fingerprint",
        &header.schema_fingerprint,
        &admission.schema_fingerprint,
    )?;
    let expected_event_time_watermark =
        serde_json::to_string(&header.event_time_watermark).map_err(IngestLogError::Json)?;
    let actual_event_time_watermark =
        serde_json::to_string(&admission.event_time_watermark).map_err(IngestLogError::Json)?;
    validate_admission_field(
        admission,
        descriptor,
        "event_time_watermark",
        &expected_event_time_watermark,
        &actual_event_time_watermark,
    )?;
    validate_admission_field(
        admission,
        descriptor,
        "admission_mode",
        "process_local_serialized",
        &admission.admission_mode,
    )?;

    Ok(())
}

fn validate_admission_field(
    admission: &DurableIngestAdmissionRecordV1,
    descriptor: &IngestBatchDescriptor,
    field: &'static str,
    expected: &str,
    actual: &str,
) -> Result<(), IngestLogError> {
    if expected == actual {
        return Ok(());
    }

    Err(IngestLogError::IngestAdmissionMismatch {
        admission_record_key: admission.admission_record_key.clone(),
        batch_key: descriptor.object_key.clone(),
        field,
        expected: expected.to_string(),
        actual: actual.to_string(),
    })
}

fn is_sha256_digest(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };

    hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn digest_bytes(bytes: &Bytes) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

impl IngestBatch {
    /// Constructs an ingest batch from unchecked opaque bytes.
    ///
    /// This remains for bootstrap/local compatibility while runtime replay
    /// still supports the pre-envelope JSON path. Production durable ingest
    /// callers should use [`Self::from_validated_envelope`] instead.
    pub fn new_bootstrap_unchecked(
        stream_id: impl Into<String>,
        partition_id: u32,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
        payload: Bytes,
    ) -> Result<Self, IngestLogError> {
        let stream_id = stream_id.into();
        let object_key = ObjectKey::ingest_batch(
            &stream_id,
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
        )?;
        let descriptor = IngestBatchDescriptor {
            stream_id,
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
            object_key,
        };

        Ok(Self {
            descriptor,
            payload,
        })
    }

    /// Constructs an appendable ingest batch from a validated V1 Arrow IPC
    /// envelope. Production durable ingest callers should use this boundary.
    ///
    /// [`Self::new`] remains available only for bootstrap/local compatibility
    /// paths that still need to pass unchecked opaque bytes through the storage
    /// log while runtime replay is being rewritten.
    pub fn from_validated_envelope(payload: Bytes) -> Result<Self, IngestLogError> {
        let envelope = IngestEnvelope::decode(payload.clone())?;
        let header = envelope.header();
        let object_key = ObjectKey::ingest_batch(
            &header.stream_id,
            header.partition_id,
            header.start_offset_inclusive,
            header.end_offset_exclusive,
        )?;
        let descriptor = IngestBatchDescriptor {
            stream_id: header.stream_id.clone(),
            partition_id: header.partition_id,
            start_offset_inclusive: header.start_offset_inclusive,
            end_offset_exclusive: header.end_offset_exclusive,
            object_key,
        };

        envelope.validate_descriptor(&descriptor)?;

        Ok(Self {
            descriptor,
            payload,
        })
    }

    pub fn descriptor(&self) -> IngestBatchDescriptor {
        self.descriptor.clone()
    }

    pub fn object_key(&self) -> &ObjectKey {
        &self.descriptor.object_key
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    fn from_descriptor(descriptor: IngestBatchDescriptor, payload: Bytes) -> Self {
        Self {
            descriptor,
            payload,
        }
    }
}

impl ReplayCheckpoint {
    pub fn new(stream_id: impl Into<String>, partition_id: u32, end_offset_exclusive: u64) -> Self {
        Self {
            relation_id: None,
            relation_version: None,
            stream_id: stream_id.into(),
            partition_id,
            end_offset_exclusive,
        }
    }

    pub fn for_relation(
        relation_id: impl Into<String>,
        relation_version: impl Into<String>,
        stream_id: impl Into<String>,
        partition_id: u32,
        end_offset_exclusive: u64,
    ) -> Self {
        Self {
            relation_id: Some(relation_id.into()),
            relation_version: Some(relation_version.into()),
            stream_id: stream_id.into(),
            partition_id,
            end_offset_exclusive,
        }
    }
}

fn parse_ingest_descriptor(value: &str) -> Result<IngestBatchDescriptor, IngestLogError> {
    let (object_key, parts) =
        ObjectKey::parse_ingest_batch(value.to_string()).map_err(|source| {
            IngestLogError::MalformedIngestKey {
                key: value.to_string(),
                source,
            }
        })?;

    Ok(IngestBatchDescriptor {
        stream_id: parts.stream_id,
        partition_id: parts.partition_id,
        start_offset_inclusive: parts.start_offset_inclusive,
        end_offset_exclusive: parts.end_offset_exclusive,
        object_key,
    })
}

fn validate_non_overlapping_ranges(
    descriptors: &[IngestBatchDescriptor],
) -> Result<(), IngestLogError> {
    for pair in descriptors.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];

        if previous.stream_id == current.stream_id
            && previous.partition_id == current.partition_id
            && current.start_offset_inclusive < previous.end_offset_exclusive
        {
            return Err(IngestLogError::OverlappingCommittedRange {
                stream_id: current.stream_id.clone(),
                partition_id: current.partition_id,
                previous: previous.object_key.clone(),
                current: current.object_key.clone(),
            });
        }
    }

    Ok(())
}

fn validate_checkpoints(
    checkpoints: &[ReplayCheckpoint],
) -> Result<HashMap<(String, u32), u64>, IngestLogError> {
    let mut checkpoint_offsets: HashMap<(String, u32), u64> = HashMap::new();
    let mut seen_checkpoints = HashSet::new();

    for checkpoint in checkpoints {
        let replay_key = (
            checkpoint.stream_id.clone(),
            checkpoint.partition_id,
            checkpoint.relation_id.clone(),
            checkpoint.relation_version.clone(),
        );
        if !seen_checkpoints.insert(replay_key) {
            return Err(IngestLogError::DuplicateReplayCheckpoint {
                stream_id: checkpoint.stream_id.clone(),
                partition_id: checkpoint.partition_id,
            });
        }
        checkpoint_offsets
            .entry((checkpoint.stream_id.clone(), checkpoint.partition_id))
            .and_modify(|checkpoint_end| {
                *checkpoint_end = (*checkpoint_end).min(checkpoint.end_offset_exclusive);
            })
            .or_insert(checkpoint.end_offset_exclusive);
    }

    Ok(checkpoint_offsets)
}
