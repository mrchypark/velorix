use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode};
use thiserror::Error;

use crate::{
    capability::{ObjectStoreCapabilityError, ObjectStoreCapabilityProfile},
    object_key::{ObjectKey, ObjectKeyError},
};

const INGEST_PREFIX: &str = "v1/ingest";

#[derive(Clone, Debug)]
pub struct IngestLog {
    store: Arc<dyn ObjectStore>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayCheckpoint {
    pub stream_id: String,
    pub partition_id: u32,
    pub end_offset_exclusive: u64,
}

#[derive(Debug, Error)]
pub enum IngestLogError {
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    #[error("ingest batch object `{0}` already exists")]
    AlreadyExists(ObjectKey),
    #[error("malformed ingest batch key `{key}`: {source}")]
    MalformedIngestKey { key: String, source: ObjectKeyError },
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
    ObjectStore(#[from] object_store::Error),
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
}

impl IngestBatch {
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
