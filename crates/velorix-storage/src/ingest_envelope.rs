use std::{fmt::Write as _, io::Cursor};

use arrow::{
    datatypes::{DataType, Field, FieldRef, IntervalUnit, SchemaRef, TimeUnit, UnionMode},
    error::ArrowError,
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::log::IngestBatchDescriptor;

pub const INGEST_ENVELOPE_MAGIC: &[u8] = b"VLXINGEST\x00\x01";

const HEADER_LEN_BYTES: usize = 4;
const SCHEMA_VERSION_V1: u32 = 1;
const FORMAT_ARROW_IPC_DELTA_BATCH_V1: &str = "ArrowIpcDeltaBatchV1";
const COMPRESSION_NONE: &str = "none";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestEnvelopeHeader {
    pub schema_version: u32,
    pub format: String,
    pub stream_id: String,
    pub partition_id: u32,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
    pub schema_fingerprint: String,
    pub payload_digest: String,
    pub compression: String,
}

#[derive(Clone, Debug)]
pub struct IngestEnvelope {
    header: IngestEnvelopeHeader,
    payload: Bytes,
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
    /// The JSON header is authoritative for stream, partition, offset range,
    /// schema fingerprint, digest, format, version, and compression. The
    /// payload digest covers only the Arrow IPC stream bytes. Schema
    /// fingerprinting includes field order, names, Arrow data types, and
    /// nullability; field/schema metadata is intentionally ignored for this
    /// first storage boundary.
    pub fn encode_batches(
        stream_id: impl Into<String>,
        partition_id: u32,
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
        batches: &[RecordBatch],
    ) -> Result<Bytes, IngestEnvelopeError> {
        let schema = batches.first().map(RecordBatch::schema).ok_or_else(|| {
            IngestEnvelopeError::MalformedEnvelope {
                reason: "at least one Arrow record batch is required".to_string(),
            }
        })?;

        validate_weight_column(&schema)?;

        for batch in batches {
            if batch.schema() != schema {
                return Err(IngestEnvelopeError::MalformedEnvelope {
                    reason: "all Arrow record batches in an ingest envelope must share a schema"
                        .to_string(),
                });
            }
        }

        if start_offset_inclusive >= end_offset_exclusive {
            return Err(IngestEnvelopeError::MalformedEnvelope {
                reason: format!(
                    "offset range must be nonempty: start={start_offset_inclusive}, end={end_offset_exclusive}"
                ),
            });
        }

        let payload = encode_arrow_ipc_stream(&schema, batches)?;
        let header = IngestEnvelopeHeader {
            schema_version: SCHEMA_VERSION_V1,
            format: FORMAT_ARROW_IPC_DELTA_BATCH_V1.to_string(),
            stream_id: stream_id.into(),
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
            schema_fingerprint: schema_fingerprint(&schema),
            payload_digest: sha256_digest(&payload),
            compression: COMPRESSION_NONE.to_string(),
        };

        encode_envelope(header, &payload)
    }

    pub fn decode(bytes: Bytes) -> Result<Self, IngestEnvelopeError> {
        let (header, payload) = split_envelope(bytes)?;

        validate_header(&header)?;

        let actual_digest = sha256_digest(&payload);
        if header.payload_digest != actual_digest {
            return Err(IngestEnvelopeError::DigestMismatch {
                expected: header.payload_digest,
                actual: actual_digest,
            });
        }

        let (schema, batch_count) = validate_arrow_ipc_stream(&payload)?;
        if batch_count == 0 {
            return Err(IngestEnvelopeError::MalformedArrowIpc {
                source: ArrowError::ParseError(
                    "Arrow IPC payload did not contain a record batch".to_string(),
                ),
            });
        }
        validate_weight_column(&schema)?;

        let actual_fingerprint = schema_fingerprint(&schema);
        if header.schema_fingerprint != actual_fingerprint {
            return Err(IngestEnvelopeError::DescriptorMismatch {
                field: "schema_fingerprint",
                expected: header.schema_fingerprint,
                found: actual_fingerprint,
            });
        }

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

    let mut bytes =
        Vec::with_capacity(INGEST_ENVELOPE_MAGIC.len() + HEADER_LEN_BYTES + header_bytes.len());
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

fn validate_header(header: &IngestEnvelopeHeader) -> Result<(), IngestEnvelopeError> {
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

    if start_not_before_end(header.start_offset_inclusive, header.end_offset_exclusive) {
        return Err(IngestEnvelopeError::MalformedEnvelope {
            reason: format!(
                "offset range must be nonempty: start={}, end={}",
                header.start_offset_inclusive, header.end_offset_exclusive
            ),
        });
    }

    Ok(())
}

fn start_not_before_end(start_offset_inclusive: u64, end_offset_exclusive: u64) -> bool {
    start_offset_inclusive >= end_offset_exclusive
}

fn encode_arrow_ipc_stream(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, IngestEnvelopeError> {
    let mut payload = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, schema.as_ref())
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

fn validate_arrow_ipc_stream(payload: &Bytes) -> Result<(SchemaRef, usize), IngestEnvelopeError> {
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

fn validate_weight_column(schema: &SchemaRef) -> Result<(), IngestEnvelopeError> {
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

/// Returns the deterministic V1 fingerprint for an Arrow schema.
///
/// The canonical input includes field order, field names, Arrow data types,
/// and nullability. Schema and field metadata are ignored.
pub fn schema_fingerprint(schema: &SchemaRef) -> String {
    let mut canonical = String::new();

    canonical.push_str("velorix-arrow-schema-v1;");
    for field in schema.fields() {
        encode_field(field.as_ref(), &mut canonical);
    }

    sha256_digest(canonical.as_bytes())
}

fn encode_field(field: &Field, out: &mut String) {
    out.push_str("field(");
    encode_string(field.name(), out);
    out.push_str(",nullable=");
    out.push_str(if field.is_nullable() { "1" } else { "0" });
    out.push_str(",type=");
    encode_data_type(field.data_type(), out);
    out.push(')');
}

fn encode_fields(fields: &[FieldRef], out: &mut String) {
    let _ = write!(out, "{}[", fields.len());
    for field in fields {
        encode_field(field.as_ref(), out);
        out.push(';');
    }
    out.push(']');
}

fn encode_data_type(data_type: &DataType, out: &mut String) {
    match data_type {
        DataType::Null => out.push_str("Null"),
        DataType::Boolean => out.push_str("Boolean"),
        DataType::Int8 => out.push_str("Int8"),
        DataType::Int16 => out.push_str("Int16"),
        DataType::Int32 => out.push_str("Int32"),
        DataType::Int64 => out.push_str("Int64"),
        DataType::UInt8 => out.push_str("UInt8"),
        DataType::UInt16 => out.push_str("UInt16"),
        DataType::UInt32 => out.push_str("UInt32"),
        DataType::UInt64 => out.push_str("UInt64"),
        DataType::Float16 => out.push_str("Float16"),
        DataType::Float32 => out.push_str("Float32"),
        DataType::Float64 => out.push_str("Float64"),
        DataType::Timestamp(unit, timezone) => {
            out.push_str("Timestamp(");
            encode_time_unit(*unit, out);
            out.push(',');
            match timezone {
                Some(timezone) => encode_string(timezone.as_ref(), out),
                None => out.push_str("none"),
            }
            out.push(')');
        }
        DataType::Date32 => out.push_str("Date32"),
        DataType::Date64 => out.push_str("Date64"),
        DataType::Time32(unit) => {
            out.push_str("Time32(");
            encode_time_unit(*unit, out);
            out.push(')');
        }
        DataType::Time64(unit) => {
            out.push_str("Time64(");
            encode_time_unit(*unit, out);
            out.push(')');
        }
        DataType::Duration(unit) => {
            out.push_str("Duration(");
            encode_time_unit(*unit, out);
            out.push(')');
        }
        DataType::Interval(unit) => {
            out.push_str("Interval(");
            encode_interval_unit(*unit, out);
            out.push(')');
        }
        DataType::Binary => out.push_str("Binary"),
        DataType::FixedSizeBinary(size) => {
            let _ = write!(out, "FixedSizeBinary({size})");
        }
        DataType::LargeBinary => out.push_str("LargeBinary"),
        DataType::BinaryView => out.push_str("BinaryView"),
        DataType::Utf8 => out.push_str("Utf8"),
        DataType::LargeUtf8 => out.push_str("LargeUtf8"),
        DataType::Utf8View => out.push_str("Utf8View"),
        DataType::List(field) => {
            out.push_str("List(");
            encode_field(field.as_ref(), out);
            out.push(')');
        }
        DataType::ListView(field) => {
            out.push_str("ListView(");
            encode_field(field.as_ref(), out);
            out.push(')');
        }
        DataType::FixedSizeList(field, size) => {
            let _ = write!(out, "FixedSizeList({size},");
            encode_field(field.as_ref(), out);
            out.push(')');
        }
        DataType::LargeList(field) => {
            out.push_str("LargeList(");
            encode_field(field.as_ref(), out);
            out.push(')');
        }
        DataType::LargeListView(field) => {
            out.push_str("LargeListView(");
            encode_field(field.as_ref(), out);
            out.push(')');
        }
        DataType::Struct(fields) => {
            out.push_str("Struct(");
            encode_fields(fields.as_ref(), out);
            out.push(')');
        }
        DataType::Union(fields, mode) => {
            out.push_str("Union(");
            encode_union_mode(*mode, out);
            out.push(',');
            let _ = write!(out, "{}[", fields.len());
            for (type_id, field) in fields.iter() {
                let _ = write!(out, "{type_id}:");
                encode_field(field.as_ref(), out);
                out.push(';');
            }
            out.push_str("])");
        }
        DataType::Dictionary(key_type, value_type) => {
            out.push_str("Dictionary(");
            encode_data_type(key_type, out);
            out.push(',');
            encode_data_type(value_type, out);
            out.push(')');
        }
        DataType::Decimal32(precision, scale) => {
            let _ = write!(out, "Decimal32({precision},{scale})");
        }
        DataType::Decimal64(precision, scale) => {
            let _ = write!(out, "Decimal64({precision},{scale})");
        }
        DataType::Decimal128(precision, scale) => {
            let _ = write!(out, "Decimal128({precision},{scale})");
        }
        DataType::Decimal256(precision, scale) => {
            let _ = write!(out, "Decimal256({precision},{scale})");
        }
        DataType::Map(field, sorted) => {
            out.push_str("Map(sorted=");
            out.push_str(if *sorted { "1" } else { "0" });
            out.push(',');
            encode_field(field.as_ref(), out);
            out.push(')');
        }
        DataType::RunEndEncoded(run_ends, values) => {
            out.push_str("RunEndEncoded(");
            encode_field(run_ends.as_ref(), out);
            out.push(',');
            encode_field(values.as_ref(), out);
            out.push(')');
        }
    }
}

fn encode_string(value: &str, out: &mut String) {
    let _ = write!(out, "{}:", value.len());
    out.push_str(value);
}

fn encode_time_unit(unit: TimeUnit, out: &mut String) {
    out.push_str(match unit {
        TimeUnit::Second => "Second",
        TimeUnit::Millisecond => "Millisecond",
        TimeUnit::Microsecond => "Microsecond",
        TimeUnit::Nanosecond => "Nanosecond",
    });
}

fn encode_interval_unit(unit: IntervalUnit, out: &mut String) {
    out.push_str(match unit {
        IntervalUnit::YearMonth => "YearMonth",
        IntervalUnit::DayTime => "DayTime",
        IntervalUnit::MonthDayNano => "MonthDayNano",
    });
}

fn encode_union_mode(mode: UnionMode, out: &mut String) {
    out.push_str(match mode {
        UnionMode::Sparse => "Sparse",
        UnionMode::Dense => "Dense",
    });
}

fn sha256_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    format!("sha256:{hex}")
}
