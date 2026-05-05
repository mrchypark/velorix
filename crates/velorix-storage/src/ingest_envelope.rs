use std::io::Cursor;

use arrow::{
    datatypes::DataType,
    error::ArrowError,
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use velorix_core::relation::validate_schema_fingerprint;

use crate::log::IngestBatchDescriptor;

pub const INGEST_ENVELOPE_MAGIC: &[u8] = b"VLXINGEST\x00\x01";

const HEADER_LEN_BYTES: usize = 4;
const SCHEMA_VERSION_V1: u32 = 1;
const FORMAT_ARROW_IPC_DELTA_BATCH_V1: &str = "ArrowIpcDeltaBatchV1";
const COMPRESSION_NONE: &str = "none";
const PAYLOAD_DIGEST_DOMAIN: &[u8] = b"velorix-ingest-envelope-v1\0";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestEnvelopeHeader {
    pub schema_version: u32,
    pub format: String,
    pub stream_id: String,
    pub partition_id: u32,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
    pub relation_id: String,
    pub relation_version: String,
    pub schema_fingerprint: String,
    pub payload_digest: String,
    pub compression: String,
}

#[derive(Clone, Debug)]
pub struct IngestEnvelope {
    header: IngestEnvelopeHeader,
    payload: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngestEnvelopeEncodeRequest {
    pub relation_id: String,
    pub relation_version: String,
    pub schema_fingerprint: String,
    pub stream_id: String,
    pub partition_id: u32,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
}

#[derive(Debug, Error)]
pub enum IngestEnvelopeError {
    #[error("malformed ingest envelope: {reason}")]
    MalformedEnvelope { reason: String },
    #[error("unsupported ingest envelope schema version {found}")]
    UnsupportedSchemaVersion { found: u32 },
    #[error("unsupported ingest envelope format `{format}`")]
    UnsupportedFormat { format: String },
    #[error("unsupported ingest envelope compression `{compression}`")]
    UnsupportedCompression { compression: String },
    #[error("ingest envelope payload digest mismatch: expected {expected}, actual {actual}")]
    DigestMismatch { expected: String, actual: String },
    #[error("malformed Arrow IPC ingest payload: {source}")]
    MalformedArrowIpc { source: ArrowError },
    #[error("ingest envelope schema is missing required `weight` column")]
    MissingWeightColumn,
    #[error("ingest envelope `weight` column must be Int64, found {data_type:?}")]
    InvalidWeightColumn { data_type: DataType },
    #[error("ingest envelope descriptor mismatch for {field}: expected {expected}, found {found}")]
    DescriptorMismatch {
        field: &'static str,
        expected: String,
        found: String,
    },
}

impl IngestEnvelope {
    /// Encodes a V1 ingest envelope as:
    ///
    /// ```text
    /// magic bytes | little-endian u32 JSON header length | JSON header | Arrow IPC stream bytes
    /// ```
    ///
    /// The payload digest covers a domain-separated canonical header without
    /// `payload_digest` plus the stored body bytes, so replay fails closed on
    /// header-only mutations as well as body mutations.
    pub fn encode_batches(
        request: IngestEnvelopeEncodeRequest,
        batches: &[RecordBatch],
    ) -> Result<Bytes, IngestEnvelopeError> {
        let schema = batches.first().map(RecordBatch::schema).ok_or_else(|| {
            IngestEnvelopeError::MalformedEnvelope {
                reason: "at least one Arrow record batch is required".to_string(),
            }
        })?;

        validate_weight_column(schema.as_ref())?;

        for batch in batches {
            if batch.schema() != schema {
                return Err(IngestEnvelopeError::MalformedEnvelope {
                    reason: "all Arrow record batches in an ingest envelope must share a schema"
                        .to_string(),
                });
            }
        }

        if request.start_offset_inclusive >= request.end_offset_exclusive {
            return Err(IngestEnvelopeError::MalformedEnvelope {
                reason: format!(
                    "offset range must be nonempty: start={}, end={}",
                    request.start_offset_inclusive, request.end_offset_exclusive
                ),
            });
        }

        let header_without_digest = IngestEnvelopeHeaderWithoutDigest {
            schema_version: SCHEMA_VERSION_V1,
            format: FORMAT_ARROW_IPC_DELTA_BATCH_V1.to_string(),
            stream_id: request.stream_id,
            partition_id: request.partition_id,
            start_offset_inclusive: request.start_offset_inclusive,
            end_offset_exclusive: request.end_offset_exclusive,
            relation_id: request.relation_id,
            relation_version: request.relation_version,
            schema_fingerprint: request.schema_fingerprint,
            compression: COMPRESSION_NONE.to_string(),
        };
        validate_header_without_digest(&header_without_digest)?;

        let payload = encode_arrow_ipc_stream(schema.as_ref(), batches)?;
        let payload_digest = payload_digest(&header_without_digest, &payload)?;
        let header = header_without_digest.with_payload_digest(payload_digest);

        encode_envelope(header, &payload)
    }

    pub fn decode(bytes: Bytes) -> Result<Self, IngestEnvelopeError> {
        let (header, payload) = split_envelope(bytes)?;
        let header_without_digest = IngestEnvelopeHeaderWithoutDigest::from(&header);

        let actual_digest = payload_digest(&header_without_digest, &payload)?;
        if header.payload_digest != actual_digest {
            return Err(IngestEnvelopeError::DigestMismatch {
                expected: header.payload_digest,
                actual: actual_digest,
            });
        }

        validate_header_without_digest(&header_without_digest)?;

        let (schema, batch_count) = validate_arrow_ipc_stream(&payload)?;
        if batch_count == 0 {
            return Err(IngestEnvelopeError::MalformedArrowIpc {
                source: ArrowError::ParseError(
                    "Arrow IPC payload did not contain a record batch".to_string(),
                ),
            });
        }
        validate_weight_column(schema.as_ref())?;

        Ok(Self { header, payload })
    }

    pub fn header(&self) -> &IngestEnvelopeHeader {
        &self.header
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn record_batches(&self) -> Result<Vec<RecordBatch>, IngestEnvelopeError> {
        decode_arrow_ipc_stream(&self.payload)
    }

    pub fn validate_descriptor(
        &self,
        descriptor: &IngestBatchDescriptor,
    ) -> Result<(), IngestEnvelopeError> {
        if self.header.stream_id != descriptor.stream_id {
            return Err(IngestEnvelopeError::DescriptorMismatch {
                field: "stream_id",
                expected: descriptor.stream_id.clone(),
                found: self.header.stream_id.clone(),
            });
        }

        if self.header.partition_id != descriptor.partition_id {
            return Err(IngestEnvelopeError::DescriptorMismatch {
                field: "partition_id",
                expected: descriptor.partition_id.to_string(),
                found: self.header.partition_id.to_string(),
            });
        }

        if self.header.start_offset_inclusive != descriptor.start_offset_inclusive
            || self.header.end_offset_exclusive != descriptor.end_offset_exclusive
        {
            return Err(IngestEnvelopeError::DescriptorMismatch {
                field: "offset_range",
                expected: format!(
                    "{}-{}",
                    descriptor.start_offset_inclusive, descriptor.end_offset_exclusive
                ),
                found: format!(
                    "{}-{}",
                    self.header.start_offset_inclusive, self.header.end_offset_exclusive
                ),
            });
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
struct IngestEnvelopeHeaderWithoutDigest {
    schema_version: u32,
    format: String,
    stream_id: String,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    relation_id: String,
    relation_version: String,
    schema_fingerprint: String,
    compression: String,
}

impl IngestEnvelopeHeaderWithoutDigest {
    fn with_payload_digest(self, payload_digest: String) -> IngestEnvelopeHeader {
        IngestEnvelopeHeader {
            schema_version: self.schema_version,
            format: self.format,
            stream_id: self.stream_id,
            partition_id: self.partition_id,
            start_offset_inclusive: self.start_offset_inclusive,
            end_offset_exclusive: self.end_offset_exclusive,
            relation_id: self.relation_id,
            relation_version: self.relation_version,
            schema_fingerprint: self.schema_fingerprint,
            payload_digest,
            compression: self.compression,
        }
    }
}

impl From<&IngestEnvelopeHeader> for IngestEnvelopeHeaderWithoutDigest {
    fn from(header: &IngestEnvelopeHeader) -> Self {
        Self {
            schema_version: header.schema_version,
            format: header.format.clone(),
            stream_id: header.stream_id.clone(),
            partition_id: header.partition_id,
            start_offset_inclusive: header.start_offset_inclusive,
            end_offset_exclusive: header.end_offset_exclusive,
            relation_id: header.relation_id.clone(),
            relation_version: header.relation_version.clone(),
            schema_fingerprint: header.schema_fingerprint.clone(),
            compression: header.compression.clone(),
        }
    }
}

fn encode_envelope(
    header: IngestEnvelopeHeader,
    payload: &[u8],
) -> Result<Bytes, IngestEnvelopeError> {
    let header_bytes =
        serde_json::to_vec(&header).map_err(|source| IngestEnvelopeError::MalformedEnvelope {
            reason: format!("could not encode JSON header: {source}"),
        })?;
    let header_len =
        u32::try_from(header_bytes.len()).map_err(|_| IngestEnvelopeError::MalformedEnvelope {
            reason: "JSON header exceeds u32 length limit".to_string(),
        })?;

    let mut bytes = Vec::with_capacity(
        INGEST_ENVELOPE_MAGIC.len() + HEADER_LEN_BYTES + header_bytes.len() + payload.len(),
    );
    bytes.extend_from_slice(INGEST_ENVELOPE_MAGIC);
    bytes.extend_from_slice(&header_len.to_le_bytes());
    bytes.extend_from_slice(&header_bytes);
    bytes.extend_from_slice(payload);

    Ok(Bytes::from(bytes))
}

fn split_envelope(bytes: Bytes) -> Result<(IngestEnvelopeHeader, Bytes), IngestEnvelopeError> {
    let min_len = INGEST_ENVELOPE_MAGIC.len() + HEADER_LEN_BYTES;
    if bytes.len() < min_len {
        return Err(IngestEnvelopeError::MalformedEnvelope {
            reason: "envelope is shorter than magic and header length".to_string(),
        });
    }

    if &bytes[..INGEST_ENVELOPE_MAGIC.len()] != INGEST_ENVELOPE_MAGIC {
        return Err(IngestEnvelopeError::MalformedEnvelope {
            reason: "invalid envelope magic".to_string(),
        });
    }

    let header_len_start = INGEST_ENVELOPE_MAGIC.len();
    let header_len_end = header_len_start + HEADER_LEN_BYTES;
    let header_len =
        u32::from_le_bytes(bytes[header_len_start..header_len_end].try_into().unwrap()) as usize;
    let header_start = header_len_end;
    let header_end = header_start.checked_add(header_len).ok_or_else(|| {
        IngestEnvelopeError::MalformedEnvelope {
            reason: "header length overflowed".to_string(),
        }
    })?;

    if header_end > bytes.len() {
        return Err(IngestEnvelopeError::MalformedEnvelope {
            reason: "header length exceeds envelope length".to_string(),
        });
    }

    let header = serde_json::from_slice::<IngestEnvelopeHeader>(&bytes[header_start..header_end])
        .map_err(|source| IngestEnvelopeError::MalformedEnvelope {
        reason: format!("invalid JSON header: {source}"),
    })?;
    let payload = bytes.slice(header_end..);

    Ok((header, payload))
}

fn validate_header_without_digest(
    header: &IngestEnvelopeHeaderWithoutDigest,
) -> Result<(), IngestEnvelopeError> {
    if header.schema_version != SCHEMA_VERSION_V1 {
        return Err(IngestEnvelopeError::UnsupportedSchemaVersion {
            found: header.schema_version,
        });
    }

    if header.format != FORMAT_ARROW_IPC_DELTA_BATCH_V1 {
        return Err(IngestEnvelopeError::UnsupportedFormat {
            format: header.format.clone(),
        });
    }

    if header.compression != COMPRESSION_NONE {
        return Err(IngestEnvelopeError::UnsupportedCompression {
            compression: header.compression.clone(),
        });
    }

    if header.relation_id.trim().is_empty() {
        return Err(IngestEnvelopeError::MalformedEnvelope {
            reason: "relation_id must be nonempty".to_string(),
        });
    }
    if header.relation_version.trim().is_empty() {
        return Err(IngestEnvelopeError::MalformedEnvelope {
            reason: "relation_version must be nonempty".to_string(),
        });
    }
    validate_schema_fingerprint("schema_fingerprint", &header.schema_fingerprint).map_err(
        |source| IngestEnvelopeError::MalformedEnvelope {
            reason: source.to_string(),
        },
    )?;

    if header.start_offset_inclusive >= header.end_offset_exclusive {
        return Err(IngestEnvelopeError::MalformedEnvelope {
            reason: format!(
                "offset range must be nonempty: start={}, end={}",
                header.start_offset_inclusive, header.end_offset_exclusive
            ),
        });
    }

    Ok(())
}

fn payload_digest(
    header: &IngestEnvelopeHeaderWithoutDigest,
    payload: &[u8],
) -> Result<String, IngestEnvelopeError> {
    let canonical_header = serde_json::json!({
        "schema_version": header.schema_version,
        "format": header.format,
        "stream_id": header.stream_id,
        "partition_id": header.partition_id,
        "start_offset_inclusive": header.start_offset_inclusive,
        "end_offset_exclusive": header.end_offset_exclusive,
        "relation_id": header.relation_id,
        "relation_version": header.relation_version,
        "schema_fingerprint": header.schema_fingerprint,
        "compression": header.compression,
    });
    let canonical_header = serde_json::to_vec(&canonical_header).map_err(|source| {
        IngestEnvelopeError::MalformedEnvelope {
            reason: format!("could not encode canonical digest header: {source}"),
        }
    })?;
    let mut hasher = Sha256::new();
    hasher.update(PAYLOAD_DIGEST_DOMAIN);
    hasher.update(canonical_header);
    hasher.update([0]);
    hasher.update(payload);

    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn encode_arrow_ipc_stream(
    schema: &arrow::datatypes::Schema,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, IngestEnvelopeError> {
    let mut payload = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, schema)
            .map_err(|source| IngestEnvelopeError::MalformedArrowIpc { source })?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|source| IngestEnvelopeError::MalformedArrowIpc { source })?;
        }
        writer
            .finish()
            .map_err(|source| IngestEnvelopeError::MalformedArrowIpc { source })?;
    }

    Ok(payload)
}

fn decode_arrow_ipc_stream(payload: &Bytes) -> Result<Vec<RecordBatch>, IngestEnvelopeError> {
    let reader = StreamReader::try_new(Cursor::new(payload.clone()), None)
        .map_err(|source| IngestEnvelopeError::MalformedArrowIpc { source })?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| IngestEnvelopeError::MalformedArrowIpc { source })
}

fn validate_arrow_ipc_stream(
    payload: &Bytes,
) -> Result<(arrow::datatypes::SchemaRef, usize), IngestEnvelopeError> {
    let reader = StreamReader::try_new(Cursor::new(payload.clone()), None)
        .map_err(|source| IngestEnvelopeError::MalformedArrowIpc { source })?;
    let schema = reader.schema();
    let mut batch_count = 0;

    for batch in reader {
        batch.map_err(|source| IngestEnvelopeError::MalformedArrowIpc { source })?;
        batch_count += 1;
    }

    Ok((schema, batch_count))
}

fn validate_weight_column(schema: &arrow::datatypes::Schema) -> Result<(), IngestEnvelopeError> {
    let field = schema
        .field_with_name("weight")
        .map_err(|_| IngestEnvelopeError::MissingWeightColumn)?;
    if field.data_type() != &DataType::Int64 {
        return Err(IngestEnvelopeError::InvalidWeightColumn {
            data_type: field.data_type().clone(),
        });
    }

    Ok(())
}
