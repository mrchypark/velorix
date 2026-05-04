use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};
use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use velorix_storage::{
    ingest_envelope::{
        schema_fingerprint, IngestEnvelope, IngestEnvelopeError, INGEST_ENVELOPE_MAGIC,
    },
    log::{IngestBatch, IngestBatchDescriptor, IngestLog, IngestLogError},
    object_key::ObjectKey,
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

fn ingest_descriptor(
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
) -> IngestBatchDescriptor {
    IngestBatchDescriptor {
        stream_id: stream_id.to_string(),
        partition_id,
        start_offset_inclusive,
        end_offset_exclusive,
        object_key: ObjectKey::ingest_batch(
            stream_id,
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
        )
        .unwrap(),
    }
}

fn valid_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("account_id", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("weight", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["acct-1", "acct-2"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, -1])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn batch_without_weight() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "account_id",
        DataType::Utf8,
        false,
    )]));

    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["acct-1"])) as ArrayRef],
    )
    .unwrap()
}

fn batch_with_unsigned_weight() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "weight",
        DataType::UInt64,
        false,
    )]));

    RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow::array::UInt64Array::from(vec![1])) as ArrayRef],
    )
    .unwrap()
}

fn envelope_bytes() -> Bytes {
    IngestEnvelope::encode_batches("orders", 7, 10, 12, &[valid_batch()]).unwrap()
}

async fn put_raw_ingest_object(
    store: &Arc<dyn ObjectStore>,
    descriptor: &IngestBatchDescriptor,
    payload: Bytes,
) {
    store
        .put(&Path::from(descriptor.object_key.as_str()), payload.into())
        .await
        .unwrap();
}

fn raw_arrow_ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut payload = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, batch.schema().as_ref()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }

    payload
}

fn sha256_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    format!("sha256:{hex}")
}

fn raw_envelope_with_payload(payload: &[u8]) -> Bytes {
    let header = serde_json::json!({
        "schema_version": 1,
        "format": "ArrowIpcDeltaBatchV1",
        "stream_id": "orders",
        "partition_id": 7,
        "start_offset_inclusive": 10,
        "end_offset_exclusive": 12,
        "schema_fingerprint": "sha256:placeholder",
        "payload_digest": sha256_digest(payload),
        "compression": "none"
    });
    let header = serde_json::to_vec(&header).unwrap();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(INGEST_ENVELOPE_MAGIC);
    bytes.extend_from_slice(&(header.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(payload);

    Bytes::from(bytes)
}

fn mutate_header(bytes: &Bytes, mutate: impl FnOnce(&mut serde_json::Map<String, Value>)) -> Bytes {
    let header_len_start = INGEST_ENVELOPE_MAGIC.len();
    let header_len_end = header_len_start + 4;
    let header_len =
        u32::from_le_bytes(bytes[header_len_start..header_len_end].try_into().unwrap()) as usize;
    let header_start = header_len_end;
    let header_end = header_start + header_len;
    let mut header = serde_json::from_slice::<Value>(&bytes[header_start..header_end])
        .unwrap()
        .as_object()
        .unwrap()
        .clone();
    mutate(&mut header);

    let new_header = serde_json::to_vec(&Value::Object(header)).unwrap();
    let mut mutated = Vec::new();
    mutated.extend_from_slice(INGEST_ENVELOPE_MAGIC);
    mutated.extend_from_slice(&(new_header.len() as u32).to_le_bytes());
    mutated.extend_from_slice(&new_header);
    mutated.extend_from_slice(&bytes[header_end..]);
    Bytes::from(mutated)
}

#[test]
fn ingest_envelope_round_trips_arrow_ipc_and_validates_matching_descriptor() {
    let bytes = envelope_bytes();
    let envelope = IngestEnvelope::decode(bytes).unwrap();

    assert_eq!(envelope.header().schema_version, 1);
    assert_eq!(envelope.header().format, "ArrowIpcDeltaBatchV1");
    assert_eq!(envelope.header().compression, "none");
    assert_eq!(envelope.header().stream_id, "orders");
    assert_eq!(envelope.header().partition_id, 7);
    assert_eq!(envelope.header().start_offset_inclusive, 10);
    assert_eq!(envelope.header().end_offset_exclusive, 12);
    let batches = envelope.record_batches().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);

    envelope
        .validate_descriptor(&ingest_descriptor("orders", 7, 10, 12))
        .unwrap();
}

#[test]
fn ingest_envelope_rejects_key_stream_mismatch() {
    let envelope = IngestEnvelope::decode(envelope_bytes()).unwrap();
    let err = envelope
        .validate_descriptor(&ingest_descriptor("payments", 7, 10, 12))
        .unwrap_err();

    assert!(matches!(
        err,
        IngestEnvelopeError::DescriptorMismatch { field, .. } if field == "stream_id"
    ));
}

#[test]
fn ingest_envelope_rejects_key_partition_mismatch() {
    let envelope = IngestEnvelope::decode(envelope_bytes()).unwrap();
    let err = envelope
        .validate_descriptor(&ingest_descriptor("orders", 8, 10, 12))
        .unwrap_err();

    assert!(matches!(
        err,
        IngestEnvelopeError::DescriptorMismatch { field, .. } if field == "partition_id"
    ));
}

#[test]
fn ingest_envelope_rejects_key_range_mismatch() {
    let envelope = IngestEnvelope::decode(envelope_bytes()).unwrap();
    let err = envelope
        .validate_descriptor(&ingest_descriptor("orders", 7, 11, 13))
        .unwrap_err();

    assert!(matches!(
        err,
        IngestEnvelopeError::DescriptorMismatch { field, .. }
            if field == "offset_range"
    ));
}

#[test]
fn ingest_envelope_rejects_unsupported_schema_version() {
    let bytes = mutate_header(&envelope_bytes(), |header| {
        header.insert("schema_version".to_string(), Value::from(2));
    });
    let err = IngestEnvelope::decode(bytes).unwrap_err();

    assert!(matches!(
        err,
        IngestEnvelopeError::UnsupportedSchemaVersion { found: 2 }
    ));
}

#[test]
fn ingest_envelope_rejects_unsupported_format() {
    let bytes = mutate_header(&envelope_bytes(), |header| {
        header.insert("format".to_string(), Value::from("JsonDeltaBatchV1"));
    });
    let err = IngestEnvelope::decode(bytes).unwrap_err();

    assert!(matches!(
        err,
        IngestEnvelopeError::UnsupportedFormat { format } if format == "JsonDeltaBatchV1"
    ));
}

#[test]
fn ingest_envelope_rejects_unsupported_compression() {
    let bytes = mutate_header(&envelope_bytes(), |header| {
        header.insert("compression".to_string(), Value::from("zstd"));
    });
    let err = IngestEnvelope::decode(bytes).unwrap_err();

    assert!(matches!(
        err,
        IngestEnvelopeError::UnsupportedCompression { compression } if compression == "zstd"
    ));
}

#[test]
fn ingest_envelope_rejects_digest_mismatch() {
    let bytes = mutate_header(&envelope_bytes(), |header| {
        header.insert(
            "payload_digest".to_string(),
            Value::from("sha256:0000000000000000000000000000000000000000000000000000000000000000"),
        );
    });
    let err = IngestEnvelope::decode(bytes).unwrap_err();

    assert!(matches!(err, IngestEnvelopeError::DigestMismatch { .. }));
}

#[test]
fn ingest_envelope_schema_fingerprint_ignores_metadata() {
    let mut metadata = HashMap::new();
    metadata.insert("source".to_string(), "ignored".to_string());
    let schema_with_metadata = Arc::new(Schema::new_with_metadata(
        vec![Field::new("weight", DataType::Int64, false).with_metadata(metadata.clone())],
        metadata,
    ));
    let schema_without_metadata = Arc::new(Schema::new(vec![Field::new(
        "weight",
        DataType::Int64,
        false,
    )]));

    assert_eq!(
        schema_fingerprint(&schema_with_metadata),
        schema_fingerprint(&schema_without_metadata)
    );
}

#[test]
fn ingest_envelope_schema_fingerprint_changes_for_field_order_name_type_and_nullability() {
    let baseline = Arc::new(Schema::new(vec![
        Field::new("account_id", DataType::Utf8, false),
        Field::new("weight", DataType::Int64, false),
    ]));
    let reordered = Arc::new(Schema::new(vec![
        Field::new("weight", DataType::Int64, false),
        Field::new("account_id", DataType::Utf8, false),
    ]));
    let renamed = Arc::new(Schema::new(vec![
        Field::new("account", DataType::Utf8, false),
        Field::new("weight", DataType::Int64, false),
    ]));
    let retyped = Arc::new(Schema::new(vec![
        Field::new("account_id", DataType::LargeUtf8, false),
        Field::new("weight", DataType::Int64, false),
    ]));
    let nullable = Arc::new(Schema::new(vec![
        Field::new("account_id", DataType::Utf8, true),
        Field::new("weight", DataType::Int64, false),
    ]));

    let baseline = schema_fingerprint(&baseline);
    assert_ne!(baseline, schema_fingerprint(&reordered));
    assert_ne!(baseline, schema_fingerprint(&renamed));
    assert_ne!(baseline, schema_fingerprint(&retyped));
    assert_ne!(baseline, schema_fingerprint(&nullable));
}

#[test]
fn ingest_envelope_rejects_missing_weight_column() {
    let err =
        IngestEnvelope::encode_batches("orders", 0, 0, 1, &[batch_without_weight()]).unwrap_err();

    assert!(matches!(err, IngestEnvelopeError::MissingWeightColumn));
}

#[test]
fn ingest_envelope_rejects_decoded_payload_missing_weight_column() {
    let payload = raw_arrow_ipc(&batch_without_weight());
    let err = IngestEnvelope::decode(raw_envelope_with_payload(&payload)).unwrap_err();

    assert!(matches!(err, IngestEnvelopeError::MissingWeightColumn));
}

#[test]
fn ingest_envelope_rejects_non_int64_weight_column() {
    let err = IngestEnvelope::encode_batches("orders", 0, 0, 1, &[batch_with_unsigned_weight()])
        .unwrap_err();

    assert!(matches!(
        err,
        IngestEnvelopeError::InvalidWeightColumn { data_type } if data_type == DataType::UInt64
    ));
}

#[test]
fn ingest_envelope_rejects_decoded_payload_with_non_int64_weight_column() {
    let payload = raw_arrow_ipc(&batch_with_unsigned_weight());
    let err = IngestEnvelope::decode(raw_envelope_with_payload(&payload)).unwrap_err();

    assert!(matches!(
        err,
        IngestEnvelopeError::InvalidWeightColumn { data_type } if data_type == DataType::UInt64
    ));
}

#[test]
fn ingest_envelope_rejects_malformed_bytes() {
    let err = IngestEnvelope::decode(Bytes::from_static(b"not an envelope")).unwrap_err();

    assert!(matches!(err, IngestEnvelopeError::MalformedEnvelope { .. }));
}

#[test]
fn ingest_envelope_rejects_malformed_arrow_ipc_payload() {
    let err =
        IngestEnvelope::decode(raw_envelope_with_payload(b"not arrow ipc bytes")).unwrap_err();

    assert!(matches!(err, IngestEnvelopeError::MalformedArrowIpc { .. }));
}

#[tokio::test]
async fn validated_envelope_batch_appends_and_replays_envelope_bytes_unchanged() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);
    let bytes = envelope_bytes();
    let batch = IngestBatch::from_validated_envelope(bytes.clone()).unwrap();

    assert_eq!(batch.payload(), &bytes);
    assert_eq!(batch.descriptor(), ingest_descriptor("orders", 7, 10, 12));

    log.append(&batch).await.unwrap();
    let replayed = log.replay_from(&[]).await.unwrap();

    assert_eq!(replayed, vec![batch]);
    assert_eq!(replayed[0].payload(), &bytes);
}

#[tokio::test]
async fn append_validated_envelope_writes_create_only_batch_and_rejects_duplicate() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);
    let bytes = envelope_bytes();

    let descriptor = log.append_validated_envelope(bytes.clone()).await.unwrap();
    assert_eq!(descriptor, ingest_descriptor("orders", 7, 10, 12));

    let replayed = log.replay_validated_envelopes_from(&[]).await.unwrap();
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].descriptor(), descriptor);
    assert_eq!(replayed[0].payload(), &bytes);

    let err = log.append_validated_envelope(bytes).await.unwrap_err();
    assert!(matches!(err, IngestLogError::AlreadyExists(_)));
}

#[tokio::test]
async fn validated_replay_rejects_digest_mismatch_under_valid_ingest_key() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(Arc::clone(&store));
    let descriptor = ingest_descriptor("orders", 7, 10, 12);
    let bytes = mutate_header(&envelope_bytes(), |header| {
        header.insert(
            "payload_digest".to_string(),
            Value::from("sha256:0000000000000000000000000000000000000000000000000000000000000000"),
        );
    });
    put_raw_ingest_object(&store, &descriptor, bytes).await;

    let err = log.replay_validated_envelopes_from(&[]).await.unwrap_err();

    assert!(matches!(
        err,
        IngestLogError::IngestEnvelope(IngestEnvelopeError::DigestMismatch { .. })
    ));
}

#[tokio::test]
async fn validated_replay_rejects_header_key_stream_mismatch_under_valid_ingest_key() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(Arc::clone(&store));
    let descriptor = ingest_descriptor("payments", 7, 10, 12);
    put_raw_ingest_object(&store, &descriptor, envelope_bytes()).await;

    let err = log.replay_validated_envelopes_from(&[]).await.unwrap_err();

    assert!(matches!(
        err,
        IngestLogError::IngestEnvelope(IngestEnvelopeError::DescriptorMismatch {
            field: "stream_id",
            ..
        })
    ));
}

#[tokio::test]
async fn validated_replay_rejects_header_key_range_mismatch_under_valid_ingest_key() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(Arc::clone(&store));
    let descriptor = ingest_descriptor("orders", 7, 10, 13);
    put_raw_ingest_object(&store, &descriptor, envelope_bytes()).await;

    let err = log.replay_validated_envelopes_from(&[]).await.unwrap_err();

    assert!(matches!(
        err,
        IngestLogError::IngestEnvelope(IngestEnvelopeError::DescriptorMismatch {
            field: "offset_range",
            ..
        })
    ));
}

#[tokio::test]
async fn validated_replay_rejects_raw_non_envelope_bytes_under_valid_ingest_key() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(Arc::clone(&store));
    let descriptor = ingest_descriptor("orders", 7, 10, 12);
    put_raw_ingest_object(&store, &descriptor, Bytes::from_static(b"not an envelope")).await;

    let err = log.replay_validated_envelopes_from(&[]).await.unwrap_err();

    assert!(matches!(
        err,
        IngestLogError::IngestEnvelope(IngestEnvelopeError::MalformedEnvelope { .. })
    ));
}
