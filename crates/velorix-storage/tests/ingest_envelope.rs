use std::sync::Arc;

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
        IngestEnvelope, IngestEnvelopeEncodeRequest, IngestEnvelopeError, INGEST_ENVELOPE_MAGIC,
    },
    log::{
        AppendValidatedEnvelopeOutcome, IngestBatch, IngestBatchDescriptor, IngestLog,
        IngestLogError,
    },
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
    envelope_bytes_for("orders", 7, 10, 12)
}

fn envelope_bytes_for(
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
) -> Bytes {
    IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "orders_relation".to_string(),
            relation_version: "2026-05-05".to_string(),
            schema_fingerprint:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            stream_id: stream_id.to_string(),
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
        },
        &[valid_batch()],
    )
    .unwrap()
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
    let request = IngestEnvelopeEncodeRequest {
        relation_id: "orders_relation".to_string(),
        relation_version: "2026-05-05".to_string(),
        schema_fingerprint:
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        stream_id: "orders".to_string(),
        partition_id: 7,
        start_offset_inclusive: 10,
        end_offset_exclusive: 12,
    };
    let header = serde_json::json!({
        "schema_version": 1,
        "format": "ArrowIpcDeltaBatchV1",
        "stream_id": request.stream_id.as_str(),
        "partition_id": request.partition_id,
        "start_offset_inclusive": request.start_offset_inclusive,
        "end_offset_exclusive": request.end_offset_exclusive,
        "relation_id": request.relation_id.as_str(),
        "relation_version": request.relation_version.as_str(),
        "schema_fingerprint": request.schema_fingerprint.as_str(),
        "payload_digest": sha256_digest_for_envelope_header(&request, "none", payload),
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

fn sha256_digest_for_envelope_header(
    request: &IngestEnvelopeEncodeRequest,
    compression: &str,
    payload: &[u8],
) -> String {
    let header_without_digest = serde_json::json!({
        "schema_version": 1,
        "format": "ArrowIpcDeltaBatchV1",
        "stream_id": request.stream_id.as_str(),
        "partition_id": request.partition_id,
        "start_offset_inclusive": request.start_offset_inclusive,
        "end_offset_exclusive": request.end_offset_exclusive,
        "relation_id": request.relation_id.as_str(),
        "relation_version": request.relation_version.as_str(),
        "schema_fingerprint": request.schema_fingerprint.as_str(),
        "compression": compression
    });
    let canonical_header = serde_json::to_vec(&header_without_digest).unwrap();
    let mut digest_input = Vec::new();
    digest_input.extend_from_slice(b"velorix-ingest-envelope-v1\0");
    digest_input.extend_from_slice(&canonical_header);
    digest_input.push(0);
    digest_input.extend_from_slice(payload);

    sha256_digest(&digest_input)
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
    assert_eq!(envelope.header().relation_id, "orders_relation");
    assert_eq!(envelope.header().relation_version, "2026-05-05");
    assert_eq!(
        envelope.header().schema_fingerprint,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
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
fn ingest_envelope_rejects_schema_version_header_mutation() {
    let bytes = mutate_header(&envelope_bytes(), |header| {
        header.insert("schema_version".to_string(), Value::from(2));
    });
    let err = IngestEnvelope::decode(bytes).unwrap_err();

    assert!(matches!(err, IngestEnvelopeError::DigestMismatch { .. }));
}

#[test]
fn ingest_envelope_rejects_format_header_mutation() {
    let bytes = mutate_header(&envelope_bytes(), |header| {
        header.insert("format".to_string(), Value::from("JsonDeltaBatchV1"));
    });
    let err = IngestEnvelope::decode(bytes).unwrap_err();

    assert!(matches!(err, IngestEnvelopeError::DigestMismatch { .. }));
}

#[test]
fn ingest_envelope_rejects_compression_header_mutation() {
    let bytes = mutate_header(&envelope_bytes(), |header| {
        header.insert("compression".to_string(), Value::from("zstd"));
    });
    let err = IngestEnvelope::decode(bytes).unwrap_err();

    assert!(matches!(err, IngestEnvelopeError::DigestMismatch { .. }));
}

#[test]
fn ingest_envelope_rejects_missing_relation_identity() {
    let err = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "".to_string(),
            relation_version: "2026-05-05".to_string(),
            schema_fingerprint:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 1,
        },
        &[valid_batch()],
    )
    .unwrap_err();

    assert!(matches!(err, IngestEnvelopeError::MalformedEnvelope { .. }));

    let err = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "orders_relation".to_string(),
            relation_version: " ".to_string(),
            schema_fingerprint:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 1,
        },
        &[valid_batch()],
    )
    .unwrap_err();

    assert!(matches!(err, IngestEnvelopeError::MalformedEnvelope { .. }));
}

#[test]
fn ingest_envelope_rejects_malformed_relation_schema_fingerprint() {
    let err = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "orders_relation".to_string(),
            relation_version: "2026-05-05".to_string(),
            schema_fingerprint: "sha256:not-a-v1-relation-fingerprint".to_string(),
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 1,
        },
        &[valid_batch()],
    )
    .unwrap_err();

    assert!(matches!(err, IngestEnvelopeError::MalformedEnvelope { .. }));
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
fn ingest_envelope_accepts_supplied_relation_schema_fingerprint_without_arrow_derivation() {
    let supplied = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let bytes = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "orders_relation".to_string(),
            relation_version: "2026-05-05".to_string(),
            schema_fingerprint: supplied.to_string(),
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 1,
        },
        &[valid_batch()],
    )
    .unwrap();

    let envelope = IngestEnvelope::decode(bytes).unwrap();

    assert_eq!(envelope.header().schema_fingerprint, supplied);
}

#[test]
fn ingest_envelope_digest_covers_canonical_header_without_payload_digest() {
    for (field, value) in [
        ("relation_id", Value::from("other_relation")),
        ("relation_version", Value::from("2026-05-06")),
        ("start_offset_inclusive", Value::from(9)),
        ("end_offset_exclusive", Value::from(13)),
        ("compression", Value::from("zstd")),
        (
            "schema_fingerprint",
            Value::from("sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        ),
    ] {
        let bytes = mutate_header(&envelope_bytes(), |header| {
            header.insert(field.to_string(), value);
        });
        let err = IngestEnvelope::decode(bytes).unwrap_err();

        assert!(
            matches!(err, IngestEnvelopeError::DigestMismatch { .. }),
            "expected digest mismatch for {field}, got {err:?}"
        );
    }
}

#[test]
fn ingest_envelope_rejects_missing_weight_column() {
    let err = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "orders_relation".to_string(),
            relation_version: "2026-05-05".to_string(),
            schema_fingerprint:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 1,
        },
        &[batch_without_weight()],
    )
    .unwrap_err();

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
    let err = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "orders_relation".to_string(),
            relation_version: "2026-05-05".to_string(),
            schema_fingerprint:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 1,
        },
        &[batch_with_unsigned_weight()],
    )
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
async fn append_validated_envelope_writes_batch_and_admits_same_digest_retry() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);
    let bytes = envelope_bytes();

    let outcome = log.append_validated_envelope(bytes.clone()).await.unwrap();
    let AppendValidatedEnvelopeOutcome::Appended { descriptor } = outcome else {
        panic!("expected appended outcome, got {outcome:?}");
    };
    assert_eq!(descriptor, ingest_descriptor("orders", 7, 10, 12));

    let replayed = log.replay_validated_envelopes_from(&[]).await.unwrap();
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].descriptor(), descriptor);
    assert_eq!(replayed[0].payload(), &bytes);

    let outcome = log.append_validated_envelope(bytes).await.unwrap();
    assert_eq!(
        outcome,
        AppendValidatedEnvelopeOutcome::Duplicate { descriptor }
    );
}

#[tokio::test]
async fn append_validated_envelope_reports_same_key_different_digest_conflict() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);
    let bytes = envelope_bytes();
    log.append_validated_envelope(bytes).await.unwrap();

    let conflicting = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: "orders_relation".to_string(),
            relation_version: "2026-05-05".to_string(),
            schema_fingerprint:
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_string(),
            stream_id: "orders".to_string(),
            partition_id: 7,
            start_offset_inclusive: 10,
            end_offset_exclusive: 12,
        },
        &[valid_batch()],
    )
    .unwrap();

    let outcome = log.append_validated_envelope(conflicting).await.unwrap();

    assert_eq!(
        outcome,
        AppendValidatedEnvelopeOutcome::Conflict {
            descriptor: ingest_descriptor("orders", 7, 10, 12),
            object_key: ObjectKey::ingest_batch("orders", 7, 10, 12).unwrap(),
            reason: "same_key_different_digest",
        }
    );
}

#[tokio::test]
async fn append_validated_envelope_single_writer_reports_committed_range_overlap_conflict() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);

    let first = envelope_bytes_for("orders", 7, 10, 20);
    let overlapping = envelope_bytes_for("orders", 7, 15, 25);

    let outcome = log
        .append_validated_envelope_single_writer(first)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        AppendValidatedEnvelopeOutcome::Appended { .. }
    ));

    let outcome = log
        .append_validated_envelope_single_writer(overlapping)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        AppendValidatedEnvelopeOutcome::Conflict {
            descriptor: ingest_descriptor("orders", 7, 15, 25),
            object_key: ObjectKey::ingest_batch("orders", 7, 10, 20).unwrap(),
            reason: "range_overlap_committed",
        }
    );
}

#[tokio::test]
async fn append_validated_envelope_single_writer_allows_adjacent_ranges() {
    let (_temp_dir, store) = temp_store();
    let log = IngestLog::new(store);

    let first = envelope_bytes_for("orders", 7, 10, 20);
    let adjacent = envelope_bytes_for("orders", 7, 20, 25);

    log.append_validated_envelope_single_writer(first)
        .await
        .unwrap();
    let outcome = log
        .append_validated_envelope_single_writer(adjacent)
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        AppendValidatedEnvelopeOutcome::Appended {
            descriptor
        } if descriptor == ingest_descriptor("orders", 7, 20, 25)
    ));
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
