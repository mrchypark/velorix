use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use futures::{lock::Mutex as AsyncMutex, TryStreamExt};
use object_store::{path::Path, ObjectStore, PutMode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use velorix_core::relation::{validate_record_batch_matches_catalog, RelationSchemaError};

use crate::{
    capability::{ObjectStoreCapabilityError, ObjectStoreCapabilityProfile},
    ingest_envelope::{IngestEnvelope, IngestEnvelopeError},
    object_key::{ObjectKey, ObjectKeyError},
    relation_catalog_registry::{RelationCatalogRegistry, RelationCatalogRegistryError},
};

const INGEST_PREFIX: &str = "v1/ingest";
const INGEST_ADMISSION_RECORD_KIND_V1: &str = "ingest_range_admission_v1";

#[derive(Clone, Debug)]
pub struct IngestLog {
    store: Arc<dyn ObjectStore>,
}

#[derive(Clone, Debug)]
struct DurableIngestAdmission {
    log: IngestLog,
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
    admission_locks: Arc<Mutex<HashMap<(String, u32), Arc<AsyncMutex<()>>>>>,
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
    pub batch_key: ObjectKey,
    pub admission_record_key: ObjectKey,
    pub payload_digest: String,
    pub relation_id: String,
    pub relation_version: String,
    pub schema_fingerprint: String,
    pub admission_mode: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayCheckpoint {
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
    #[error("duplicate replay checkpoint for {stream_id}/p={partition_id}")]
    DuplicateReplayCheckpoint {
        stream_id: String,
        partition_id: u32,
    },
    #[error(transparent)]
    RelationCatalogRegistry(#[from] RelationCatalogRegistryError),
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
    fn new(log: IngestLog) -> Self {
        Self { log }
    }

    /// Records durable serialized admission evidence, then appends the
    /// canonical ingest object. The supplied guard is the serialization
    /// boundary; this facade deliberately does not claim that object-store range
    /// records alone can reject arbitrary concurrent overlaps.
    async fn admit_validated_batch(
        &self,
        batch: IngestBatch,
        guard: &AdmissionSerializationGuard,
    ) -> Result<AppendValidatedEnvelopeOutcome, IngestLogError> {
        let descriptor = batch.descriptor();
        guard.validate(&descriptor)?;

        for committed in self.log.list_committed().await? {
            if committed.object_key == descriptor.object_key {
                continue;
            }
            if ranges_overlap(&committed, &descriptor) {
                return Ok(AppendValidatedEnvelopeOutcome::Conflict {
                    descriptor,
                    object_key: committed.object_key,
                    reason: "range_overlap_committed",
                });
            }
        }

        if let Some(existing) = self.log.existing_batch_conflict(&batch).await? {
            return Ok(existing);
        }

        let record = admission_record_for_batch(&batch)?;
        match self.reserve_admission_record(&record).await? {
            AdmissionReservationOutcome::Admitted | AdmissionReservationOutcome::Duplicate => {}
            AdmissionReservationOutcome::Conflict { object_key, reason } => {
                return Ok(AppendValidatedEnvelopeOutcome::Conflict {
                    descriptor,
                    object_key,
                    reason,
                });
            }
        }

        self.log.append_validated_batch(batch).await
    }

    async fn reserve_admission_record(
        &self,
        record: &DurableIngestAdmissionRecordV1,
    ) -> Result<AdmissionReservationOutcome, IngestLogError> {
        let descriptor = record.descriptor()?;
        let existing = self.list_admission_records().await?;

        for candidate in &existing {
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

            if ranges_overlap(&candidate_descriptor, &descriptor) {
                return Ok(AdmissionReservationOutcome::Conflict {
                    object_key: candidate.batch_key.clone(),
                    reason: "range_overlap_reserved",
                });
            }
        }

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
                let existing = self
                    .log
                    .store
                    .get(&Path::from(record.admission_record_key.as_str()))
                    .await?
                    .bytes()
                    .await?;
                let existing: DurableIngestAdmissionRecordV1 = serde_json::from_slice(&existing)?;
                existing.validate_key()?;
                if &existing == record {
                    Ok(AdmissionReservationOutcome::Duplicate)
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

    async fn list_admission_records(
        &self,
    ) -> Result<Vec<DurableIngestAdmissionRecordV1>, IngestLogError> {
        let mut objects = self
            .log
            .store
            .list(Some(&Path::from("v1/ingest-admission")))
            .try_collect::<Vec<_>>()
            .await?;

        objects.sort_by(|left, right| left.location.cmp(&right.location));

        let mut records = Vec::with_capacity(objects.len());
        for object in objects {
            let bytes = self.log.store.get(&object.location).await?.bytes().await?;
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
            records.push(record);
        }

        Ok(records)
    }
}

impl DurableIngestAdmissionRecordV1 {
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

impl IngestAdmissionCoordinator {
    pub fn new(log: IngestLog) -> Self {
        Self {
            durable_admission: DurableIngestAdmission::new(log.clone()),
            log,
            admission_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Catalog-aware process-local coordinated admission with durable
    /// serialized admission evidence.
    ///
    /// This serializes range checks for each stream/partition inside this
    /// coordinator instance and records a Velorix-owned admission record before
    /// appending. It is useful storage plumbing for a deployed coordinator, but
    /// it is not itself a distributed admission index across processes or pods.
    pub async fn append_catalog_validated_envelope(
        &self,
        payload: Bytes,
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
            .admit_validated_batch(batch, &serialization_guard)
            .await
    }

    pub async fn list_committed(&self) -> Result<Vec<IngestBatchDescriptor>, IngestLogError> {
        self.log.list_committed().await
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
        Self { store }
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
        let catalog = RelationCatalogRegistry::new(Arc::clone(&self.store))
            .read(&header.relation_id, &header.relation_version)
            .await?;

        if header.schema_fingerprint != catalog.schema_fingerprint.as_str() {
            return Err(IngestLogError::RelationCatalogMismatch {
                field: "schema_fingerprint",
                expected: catalog.schema_fingerprint.as_str().to_string(),
                actual: header.schema_fingerprint.clone(),
            });
        }

        for batch in envelope.record_batches()? {
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
}

fn ranges_overlap(previous: &IngestBatchDescriptor, current: &IngestBatchDescriptor) -> bool {
    previous.stream_id == current.stream_id
        && previous.partition_id == current.partition_id
        && previous.start_offset_inclusive < current.end_offset_exclusive
        && current.start_offset_inclusive < previous.end_offset_exclusive
        && previous.object_key != current.object_key
}

fn admission_record_for_batch(
    batch: &IngestBatch,
) -> Result<DurableIngestAdmissionRecordV1, IngestLogError> {
    let envelope = IngestEnvelope::decode(batch.payload.clone())?;
    let header = envelope.header();
    let descriptor = batch.descriptor();
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
        batch_key: descriptor.object_key,
        admission_record_key,
        payload_digest: header.payload_digest.clone(),
        relation_id: header.relation_id.clone(),
        relation_version: header.relation_version.clone(),
        schema_fingerprint: header.schema_fingerprint.clone(),
        admission_mode: "process_local_serialized".to_string(),
    };
    record.validate_key()?;

    Ok(record)
}

fn is_sha256_digest(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };

    hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
}

impl IngestBatch {
    /// Constructs an ingest batch from unchecked opaque bytes.
    ///
    /// This remains for bootstrap/local compatibility while runtime replay
    /// still supports the pre-envelope JSON path. Production durable ingest
    /// callers should use [`Self::from_validated_envelope`] instead.
    pub fn new(
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
    let mut checkpoint_offsets = HashMap::new();

    for checkpoint in checkpoints {
        let key = (checkpoint.stream_id.clone(), checkpoint.partition_id);
        if checkpoint_offsets
            .insert(key.clone(), checkpoint.end_offset_exclusive)
            .is_some()
        {
            return Err(IngestLogError::DuplicateReplayCheckpoint {
                stream_id: key.0,
                partition_id: key.1,
            });
        }
    }

    Ok(checkpoint_offsets)
}
