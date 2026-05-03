use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode};
use thiserror::Error;

use crate::object_key::{ObjectKey, ObjectKeyError};

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
    #[error("malformed ingest batch key `{0}`")]
    MalformedIngestKeyShape(String),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

impl IngestLog {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
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

        objects
            .into_iter()
            .map(|object| parse_ingest_descriptor(object.location.as_ref()))
            .collect()
    }

    pub async fn replay_from(
        &self,
        checkpoints: &[ReplayCheckpoint],
    ) -> Result<Vec<IngestBatch>, IngestLogError> {
        let checkpoint_offsets = checkpoints
            .iter()
            .map(|checkpoint| {
                (
                    (checkpoint.stream_id.as_str(), checkpoint.partition_id),
                    checkpoint.end_offset_exclusive,
                )
            })
            .collect::<HashMap<_, _>>();

        let mut batches = Vec::new();
        for descriptor in self.list_committed().await? {
            let checkpoint_end = checkpoint_offsets
                .get(&(descriptor.stream_id.as_str(), descriptor.partition_id))
                .copied()
                .unwrap_or(0);

            if descriptor.end_offset_exclusive <= checkpoint_end {
                continue;
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
    let object_key = ObjectKey::parse(value.to_string()).map_err(|source| {
        IngestLogError::MalformedIngestKey {
            key: value.to_string(),
            source,
        }
    })?;

    let ["v1", "ingest", stream_id, partition, range] = value.split('/').collect::<Vec<_>>()[..]
    else {
        return Err(IngestLogError::MalformedIngestKeyShape(value.to_string()));
    };
    let partition_id = partition
        .strip_prefix("p=")
        .ok_or_else(|| IngestLogError::MalformedIngestKeyShape(value.to_string()))?
        .parse()
        .map_err(|_| IngestLogError::MalformedIngestKeyShape(value.to_string()))?;
    let range = range
        .strip_suffix(".batch")
        .ok_or_else(|| IngestLogError::MalformedIngestKeyShape(value.to_string()))?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| IngestLogError::MalformedIngestKeyShape(value.to_string()))?;
    let start_offset_inclusive = start
        .parse()
        .map_err(|_| IngestLogError::MalformedIngestKeyShape(value.to_string()))?;
    let end_offset_exclusive = end
        .parse()
        .map_err(|_| IngestLogError::MalformedIngestKeyShape(value.to_string()))?;

    Ok(IngestBatchDescriptor {
        stream_id: stream_id.to_string(),
        partition_id,
        start_offset_inclusive,
        end_offset_exclusive,
        object_key,
    })
}
