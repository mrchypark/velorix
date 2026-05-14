use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};
use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore, PutMode};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::sync::Barrier;
use velorix_core::relation::{
    ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
    FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1, RelationOperationV1,
    RelationSchemaError, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
    VelorixRelationCatalogV1, VelorixRelationSchemaV1, RELATION_SCHEMA_VERSION_V1,
};
use velorix_storage::{
    ingest_envelope::{
        IngestEnvelope, IngestEnvelopeEncodeRequest, IngestEnvelopeError, INGEST_ENVELOPE_MAGIC,
    },
    log::{
        AppendValidatedEnvelopeOutcome, DurableIngestAdmissionExpiryDecisionRecordV1,
        DurableIngestAdmissionRecordV1, IngestAdmissionCoordinator, IngestBatch,
        IngestBatchDescriptor, IngestLog, IngestLogError, ReplayCheckpoint,
    },
    object_key::ObjectKey,
    relation_catalog_registry::{RelationCatalogRegistry, RelationCatalogRegistryError},
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

fn ranges_overlap_for_test(left: &IngestBatchDescriptor, right: &IngestBatchDescriptor) -> bool {
    left.stream_id == right.stream_id
        && left.partition_id == right.partition_id
        && left.start_offset_inclusive < right.end_offset_exclusive
        && right.start_offset_inclusive < left.end_offset_exclusive
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

fn valid_batch_with_different_payload() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("account_id", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("weight", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["acct-3", "acct-4"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![30, 40])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, -1])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn batch_with_wrong_value_column() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("account_id", DataType::Utf8, false),
        Field::new("total", DataType::Int64, false),
        Field::new("weight", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["acct-1"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
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

fn catalog_envelope_bytes(catalog: &VelorixRelationCatalogV1) -> Bytes {
    catalog_envelope_bytes_with_batches(catalog, &[valid_batch()])
}

fn catalog_envelope_bytes_with_batches(
    catalog: &VelorixRelationCatalogV1,
    batches: &[RecordBatch],
) -> Bytes {
    IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: catalog.relation_schema.relation_id.clone(),
            relation_version: catalog.relation_schema.relation_version.clone(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: "orders".to_string(),
            partition_id: 7,
            start_offset_inclusive: 10,
            end_offset_exclusive: 12,
        },
        batches,
    )
    .unwrap()
}

fn catalog_envelope_bytes_for(
    catalog: &VelorixRelationCatalogV1,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
) -> Bytes {
    IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: catalog.relation_schema.relation_id.clone(),
            relation_version: catalog.relation_schema.relation_version.clone(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: "orders".to_string(),
            partition_id: 7,
            start_offset_inclusive,
            end_offset_exclusive,
        },
        &[valid_batch()],
    )
    .unwrap()
}

fn durable_admission_record_for_payload(payload: Bytes) -> DurableIngestAdmissionRecordV1 {
    let batch = IngestBatch::from_validated_envelope(payload.clone()).unwrap();
    let envelope = IngestEnvelope::decode(payload).unwrap();
    let descriptor = batch.descriptor();

    DurableIngestAdmissionRecordV1 {
        schema_version: 1,
        record_kind: "ingest_range_admission_v1".to_string(),
        stream_id: descriptor.stream_id.clone(),
        partition_id: descriptor.partition_id,
        start_offset_inclusive: descriptor.start_offset_inclusive,
        end_offset_exclusive: descriptor.end_offset_exclusive,
        batch_key: descriptor.object_key.clone(),
        admission_record_key: ObjectKey::ingest_admission_record(
            &descriptor.stream_id,
            descriptor.partition_id,
            descriptor.start_offset_inclusive,
            descriptor.end_offset_exclusive,
        )
        .unwrap(),
        payload_digest: envelope.header().payload_digest.clone(),
        relation_id: envelope.header().relation_id.clone(),
        relation_version: envelope.header().relation_version.clone(),
        schema_fingerprint: envelope.header().schema_fingerprint.clone(),
        admission_mode: "process_local_serialized".to_string(),
    }
}

async fn put_durable_admission_record(
    store: &Arc<dyn ObjectStore>,
    record: &DurableIngestAdmissionRecordV1,
) {
    store
        .put_opts(
            &Path::from(record.admission_record_key.as_str()),
            Bytes::from(serde_json::to_vec(record).unwrap()).into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
}

fn catalog_envelope_bytes_with_fingerprint(
    catalog: &VelorixRelationCatalogV1,
    schema_fingerprint: &str,
) -> Bytes {
    IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: catalog.relation_schema.relation_id.clone(),
            relation_version: catalog.relation_schema.relation_version.clone(),
            schema_fingerprint: schema_fingerprint.to_string(),
            stream_id: "orders".to_string(),
            partition_id: 7,
            start_offset_inclusive: 10,
            end_offset_exclusive: 12,
        },
        &[valid_batch()],
    )
    .unwrap()
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

fn orders_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "orders_relation".to_string(),
        relation_name: "orders".to_string(),
        relation_version: "2026-05-05".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "amount".to_string(),
                name: "amount".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "weight".to_string(),
                name: "weight".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["account_id".to_string()],
        weight_column_id: "weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "orders".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "orders_relation".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: "incremental-adapter-orders-sum-count-v1".to_string(),
        },
    }
}

async fn create_orders_relation_catalog(store: &Arc<dyn ObjectStore>) -> VelorixRelationCatalogV1 {
    let catalog = orders_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(store))
        .create(&catalog)
        .await
        .unwrap();

    catalog
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
async fn append_catalog_validated_envelope_writes_batch_after_catalog_match() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let log = IngestLog::new(Arc::clone(&store));
    let bytes = catalog_envelope_bytes(&catalog);

    let outcome = log
        .append_catalog_validated_envelope(bytes.clone())
        .await
        .unwrap();

    let AppendValidatedEnvelopeOutcome::Appended { descriptor } = outcome else {
        panic!("expected appended outcome, got {outcome:?}");
    };
    assert_eq!(descriptor, ingest_descriptor("orders", 7, 10, 12));

    let replayed = log.replay_validated_envelopes_from(&[]).await.unwrap();
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].payload(), &bytes);
}

#[tokio::test]
async fn append_catalog_validated_envelope_admits_same_digest_retry_after_catalog_match() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let log = IngestLog::new(Arc::clone(&store));
    let bytes = catalog_envelope_bytes(&catalog);

    let outcome = log
        .append_catalog_validated_envelope(bytes.clone())
        .await
        .unwrap();
    let AppendValidatedEnvelopeOutcome::Appended { descriptor } = outcome else {
        panic!("expected appended outcome, got {outcome:?}");
    };

    let outcome = log.append_catalog_validated_envelope(bytes).await.unwrap();

    assert_eq!(
        outcome,
        AppendValidatedEnvelopeOutcome::Duplicate { descriptor }
    );
}

#[tokio::test]
async fn append_catalog_validated_envelope_rejects_missing_relation_catalog_before_commit() {
    let (_temp_dir, store) = temp_store();
    let catalog = orders_relation_catalog();
    let log = IngestLog::new(Arc::clone(&store));

    let error = log
        .append_catalog_validated_envelope(catalog_envelope_bytes(&catalog))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        IngestLogError::RelationCatalogRegistry(RelationCatalogRegistryError::ObjectStore(
            object_store::Error::NotFound { .. }
        ))
    ));
    assert!(log.list_committed().await.unwrap().is_empty());
}

#[tokio::test]
async fn append_catalog_validated_envelope_rejects_schema_fingerprint_mismatch_before_commit() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let log = IngestLog::new(Arc::clone(&store));
    let bytes = catalog_envelope_bytes_with_fingerprint(
        &catalog,
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );

    let error = log
        .append_catalog_validated_envelope(bytes)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        IngestLogError::RelationCatalogMismatch {
            field: "schema_fingerprint",
            expected,
            actual,
        } if expected == catalog.schema_fingerprint.as_str()
            && actual == "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    ));
    assert!(log.list_committed().await.unwrap().is_empty());
}

#[tokio::test]
async fn append_catalog_validated_envelope_rejects_batch_schema_mismatch_before_commit() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let log = IngestLog::new(Arc::clone(&store));
    let bytes = catalog_envelope_bytes_with_batches(&catalog, &[batch_with_wrong_value_column()]);

    let error = log
        .append_catalog_validated_envelope(bytes)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        IngestLogError::RelationSchema(RelationSchemaError::InvalidRelationSchema {
            field: "batch_schema"
        })
    ));
    assert!(log.list_committed().await.unwrap().is_empty());
}

#[tokio::test]
async fn append_catalog_validated_envelope_single_writer_rejects_committed_overlap_after_catalog_match(
) {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let log = IngestLog::new(Arc::clone(&store));

    log.append_catalog_validated_envelope_single_writer(catalog_envelope_bytes_for(
        &catalog, 10, 20,
    ))
    .await
    .unwrap();
    let outcome = log
        .append_catalog_validated_envelope_single_writer(catalog_envelope_bytes_for(
            &catalog, 15, 25,
        ))
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
async fn process_local_coordinated_catalog_admission_rejects_one_concurrent_overlap() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let coordinator = Arc::new(IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(
        &store,
    ))));
    let first = catalog_envelope_bytes_for(&catalog, 0, 100);
    let overlapping = catalog_envelope_bytes_for(&catalog, 50, 150);
    let start = Arc::new(Barrier::new(3));

    let first_task = tokio::spawn({
        let coordinator = Arc::clone(&coordinator);
        let start = Arc::clone(&start);
        async move {
            start.wait().await;
            coordinator.append_catalog_validated_envelope(first).await
        }
    });
    let overlapping_task = tokio::spawn({
        let coordinator = Arc::clone(&coordinator);
        let start = Arc::clone(&start);
        async move {
            start.wait().await;
            coordinator
                .append_catalog_validated_envelope(overlapping)
                .await
        }
    });

    start.wait().await;
    let outcomes = vec![
        first_task.await.unwrap().unwrap(),
        overlapping_task.await.unwrap().unwrap(),
    ];

    let appended = outcomes
        .iter()
        .find_map(|outcome| match outcome {
            AppendValidatedEnvelopeOutcome::Appended { descriptor } => Some(descriptor),
            _ => None,
        })
        .expect("one overlapping concurrent admission should append");
    let conflict = outcomes
        .iter()
        .find_map(|outcome| match outcome {
            AppendValidatedEnvelopeOutcome::Conflict {
                descriptor,
                object_key,
                reason,
            } => Some((descriptor, object_key, reason)),
            _ => None,
        })
        .expect("one overlapping concurrent admission should conflict");

    assert_eq!(conflict.1, &appended.object_key);
    assert_eq!(*conflict.2, "range_overlap_committed");
    assert!(ranges_overlap_for_test(appended, conflict.0));
    assert_eq!(coordinator.list_committed().await.unwrap().len(), 1);
}

#[tokio::test]
async fn process_local_coordinated_catalog_admission_allows_adjacent_ranges() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));

    let first = coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 0, 100))
        .await
        .unwrap();
    let adjacent = coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 100, 150))
        .await
        .unwrap();

    assert!(matches!(
        first,
        AppendValidatedEnvelopeOutcome::Appended {
            descriptor
        } if descriptor == ingest_descriptor("orders", 7, 0, 100)
    ));
    assert!(matches!(
        adjacent,
        AppendValidatedEnvelopeOutcome::Appended {
            descriptor
        } if descriptor == ingest_descriptor("orders", 7, 100, 150)
    ));
    assert_eq!(coordinator.list_committed().await.unwrap().len(), 2);
}

#[tokio::test]
async fn process_local_coordinated_catalog_admission_keeps_same_digest_retry_idempotent() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let bytes = catalog_envelope_bytes_for(&catalog, 0, 100);

    let first = coordinator
        .append_catalog_validated_envelope(bytes.clone())
        .await
        .unwrap();
    let AppendValidatedEnvelopeOutcome::Appended { descriptor } = first else {
        panic!("expected appended outcome, got {first:?}");
    };
    let retry = coordinator
        .append_catalog_validated_envelope(bytes)
        .await
        .unwrap();

    assert_eq!(
        retry,
        AppendValidatedEnvelopeOutcome::Duplicate { descriptor }
    );
}

#[tokio::test]
async fn durable_serialized_catalog_admission_rejects_overlap_reserved_by_separate_coordinator() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let reserved_payload = catalog_envelope_bytes_for(&catalog, 0, 100);
    let reserved_batch = IngestBatch::from_validated_envelope(reserved_payload.clone()).unwrap();
    let reserved_envelope = IngestEnvelope::decode(reserved_payload).unwrap();
    let reserved_descriptor = reserved_batch.descriptor();
    let reservation = DurableIngestAdmissionRecordV1 {
        schema_version: 1,
        record_kind: "ingest_range_admission_v1".to_string(),
        stream_id: reserved_descriptor.stream_id.clone(),
        partition_id: reserved_descriptor.partition_id,
        start_offset_inclusive: reserved_descriptor.start_offset_inclusive,
        end_offset_exclusive: reserved_descriptor.end_offset_exclusive,
        batch_key: reserved_descriptor.object_key.clone(),
        admission_record_key: ObjectKey::ingest_admission_record(
            &reserved_descriptor.stream_id,
            reserved_descriptor.partition_id,
            reserved_descriptor.start_offset_inclusive,
            reserved_descriptor.end_offset_exclusive,
        )
        .unwrap(),
        payload_digest: reserved_envelope.header().payload_digest.clone(),
        relation_id: reserved_envelope.header().relation_id.clone(),
        relation_version: reserved_envelope.header().relation_version.clone(),
        schema_fingerprint: reserved_envelope.header().schema_fingerprint.clone(),
        admission_mode: "process_local_serialized".to_string(),
    };

    store
        .put_opts(
            &Path::from(reservation.admission_record_key.as_str()),
            Bytes::from(serde_json::to_vec(&reservation).unwrap()).into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();

    let separate_coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let outcome = separate_coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 50, 150))
        .await
        .unwrap();

    assert_eq!(
        outcome,
        AppendValidatedEnvelopeOutcome::Conflict {
            descriptor: ingest_descriptor("orders", 7, 50, 150),
            object_key: reserved_descriptor.object_key,
            reason: "range_overlap_reserved",
        }
    );
    assert!(separate_coordinator
        .list_committed()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn durable_serialized_catalog_admission_fails_closed_on_record_body_key_mismatch() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let reserved_payload = catalog_envelope_bytes_for(&catalog, 0, 100);
    let reserved_batch = IngestBatch::from_validated_envelope(reserved_payload.clone()).unwrap();
    let reserved_envelope = IngestEnvelope::decode(reserved_payload).unwrap();
    let reserved_descriptor = reserved_batch.descriptor();
    let reservation = DurableIngestAdmissionRecordV1 {
        schema_version: 1,
        record_kind: "ingest_range_admission_v1".to_string(),
        stream_id: reserved_descriptor.stream_id.clone(),
        partition_id: reserved_descriptor.partition_id,
        start_offset_inclusive: reserved_descriptor.start_offset_inclusive,
        end_offset_exclusive: reserved_descriptor.end_offset_exclusive,
        batch_key: reserved_descriptor.object_key.clone(),
        admission_record_key: ObjectKey::ingest_admission_record(
            &reserved_descriptor.stream_id,
            reserved_descriptor.partition_id,
            reserved_descriptor.start_offset_inclusive,
            reserved_descriptor.end_offset_exclusive,
        )
        .unwrap(),
        payload_digest: reserved_envelope.header().payload_digest.clone(),
        relation_id: reserved_envelope.header().relation_id.clone(),
        relation_version: reserved_envelope.header().relation_version.clone(),
        schema_fingerprint: reserved_envelope.header().schema_fingerprint.clone(),
        admission_mode: "process_local_serialized".to_string(),
    };
    let wrong_path = ObjectKey::ingest_admission_record(
        &reserved_descriptor.stream_id,
        reserved_descriptor.partition_id,
        0,
        101,
    )
    .unwrap();

    store
        .put_opts(
            &Path::from(wrong_path.as_str()),
            Bytes::from(serde_json::to_vec(&reservation).unwrap()).into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();

    let separate_coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let error = separate_coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 100, 150))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        IngestLogError::MalformedIngestAdmissionRecord { key, reason }
            if key == wrong_path.as_str()
                && reason.contains("stored path does not match body admission_record_key")
    ));
}

#[tokio::test]
async fn durable_serialized_catalog_admission_does_not_reserve_same_key_different_digest() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let first = catalog_envelope_bytes_for(&catalog, 0, 100);
    let conflicting = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: catalog.relation_schema.relation_id.clone(),
            relation_version: catalog.relation_schema.relation_version.clone(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: "orders".to_string(),
            partition_id: 7,
            start_offset_inclusive: 0,
            end_offset_exclusive: 100,
        },
        &[valid_batch_with_different_payload()],
    )
    .unwrap();

    coordinator
        .append_catalog_validated_envelope(first)
        .await
        .unwrap();
    let outcome = coordinator
        .append_catalog_validated_envelope(conflicting)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        AppendValidatedEnvelopeOutcome::Conflict {
            descriptor: ingest_descriptor("orders", 7, 0, 100),
            object_key: ObjectKey::ingest_batch("orders", 7, 0, 100).unwrap(),
            reason: "same_key_different_digest",
        }
    );

    let reservation_key = ObjectKey::ingest_admission_record("orders", 7, 0, 100).unwrap();
    let reservation = store
        .get(&Path::from(reservation_key.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let reservation: DurableIngestAdmissionRecordV1 = serde_json::from_slice(&reservation).unwrap();
    assert_eq!(
        reservation.schema_fingerprint,
        catalog.schema_fingerprint.as_str()
    );
}

#[tokio::test]
async fn durable_orphan_expiry_decision_allows_restarted_coordinator_to_admit_range() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let orphan_payload = catalog_envelope_bytes_for(&catalog, 0, 100);
    let orphan_admission = durable_admission_record_for_payload(orphan_payload);
    put_durable_admission_record(&store, &orphan_admission).await;

    let coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let decision = coordinator
        .expire_orphan_admission(
            "orders",
            7,
            0,
            100,
            "repair-0001",
            "batch_append_failed_after_admission",
            "operator-1",
        )
        .await
        .unwrap();
    store
        .get(&Path::from(decision.expiry_decision_key.as_str()))
        .await
        .unwrap();

    let restarted = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let outcome = restarted
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 50, 150))
        .await
        .unwrap();

    assert_eq!(
        outcome,
        AppendValidatedEnvelopeOutcome::Appended {
            descriptor: ingest_descriptor("orders", 7, 50, 150),
        }
    );
    assert_eq!(restarted.list_committed().await.unwrap().len(), 1);
}

#[tokio::test]
async fn durable_orphan_expiry_decision_rejects_stale_original_retry() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let orphan_payload = catalog_envelope_bytes_for(&catalog, 0, 100);
    let orphan_admission = durable_admission_record_for_payload(orphan_payload.clone());
    put_durable_admission_record(&store, &orphan_admission).await;

    let coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    coordinator
        .expire_orphan_admission(
            "orders",
            7,
            0,
            100,
            "repair-0001",
            "batch_append_failed_after_admission",
            "operator-1",
        )
        .await
        .unwrap();

    let outcome = coordinator
        .append_catalog_validated_envelope(orphan_payload)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        AppendValidatedEnvelopeOutcome::Conflict {
            descriptor: ingest_descriptor("orders", 7, 0, 100),
            object_key: ObjectKey::ingest_batch("orders", 7, 0, 100).unwrap(),
            reason: "admission_expired",
        }
    );
    assert!(coordinator.list_committed().await.unwrap().is_empty());
}

#[tokio::test]
async fn durable_orphan_expiry_decision_uses_stored_admission_bytes_for_retry_rejection() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let orphan_payload = catalog_envelope_bytes_for(&catalog, 0, 100);
    let orphan_admission = durable_admission_record_for_payload(orphan_payload.clone());
    store
        .put_opts(
            &Path::from(orphan_admission.admission_record_key.as_str()),
            Bytes::from(serde_json::to_vec_pretty(&orphan_admission).unwrap()).into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();

    let coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    coordinator
        .expire_orphan_admission(
            "orders",
            7,
            0,
            100,
            "repair-0001",
            "batch_append_failed_after_admission",
            "operator-1",
        )
        .await
        .unwrap();

    let outcome = coordinator
        .append_catalog_validated_envelope(orphan_payload)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        AppendValidatedEnvelopeOutcome::Conflict {
            descriptor: ingest_descriptor("orders", 7, 0, 100),
            object_key: ObjectKey::ingest_batch("orders", 7, 0, 100).unwrap(),
            reason: "admission_expired",
        }
    );
    assert!(coordinator.list_committed().await.unwrap().is_empty());
}

#[tokio::test]
async fn durable_orphan_expiry_decision_digest_mismatch_keeps_reservation_active() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let orphan_payload = catalog_envelope_bytes_for(&catalog, 0, 100);
    let orphan_admission = durable_admission_record_for_payload(orphan_payload);
    put_durable_admission_record(&store, &orphan_admission).await;
    let decision = DurableIngestAdmissionExpiryDecisionRecordV1 {
        schema_version: 1,
        record_kind: "ingest_admission_orphan_expiry_decision_v1".to_string(),
        stream_id: "orders".to_string(),
        partition_id: 7,
        start_offset_inclusive: 0,
        end_offset_exclusive: 100,
        batch_key: ObjectKey::ingest_batch("orders", 7, 0, 100).unwrap(),
        admission_record_key: ObjectKey::ingest_admission_record("orders", 7, 0, 100).unwrap(),
        observed_missing_batch_key: ObjectKey::ingest_batch("orders", 7, 0, 100).unwrap(),
        expiry_decision_key: ObjectKey::ingest_admission_orphan_expiry_decision(
            "orders",
            7,
            0,
            100,
            "repair-0001",
        )
        .unwrap(),
        admission_record_digest:
            "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        expired_reason: "batch_append_failed_after_admission".to_string(),
        operator_id: "operator-1".to_string(),
    };
    store
        .put_opts(
            &Path::from(decision.expiry_decision_key.as_str()),
            Bytes::from(serde_json::to_vec(&decision).unwrap()).into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();

    let restarted = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let outcome = restarted
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 50, 150))
        .await
        .unwrap();

    assert_eq!(
        outcome,
        AppendValidatedEnvelopeOutcome::Conflict {
            descriptor: ingest_descriptor("orders", 7, 50, 150),
            object_key: ObjectKey::ingest_batch("orders", 7, 0, 100).unwrap(),
            reason: "range_overlap_reserved",
        }
    );
    assert!(restarted.list_committed().await.unwrap().is_empty());
}

#[tokio::test]
async fn durable_admission_reconstruction_fails_closed_on_unknown_admission_namespace_object() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let unknown = Path::from(
        "v1/ingest-admission/orders/p=0000000007/ranges/00000000000000000000-00000000000000000100/notes.txt",
    );
    store
        .put_opts(
            &unknown,
            Bytes::from_static(b"unknown admission namespace object").into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();

    let coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let err = coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 50, 150))
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        IngestLogError::MalformedIngestAdmissionExpiryDecision { key, reason }
            if key == unknown.as_ref()
                && reason == "unexpected object under v1/ingest-admission"
    ));
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

#[tokio::test]
async fn admitted_replay_rejects_batch_without_admission_in_replay_window() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let log = IngestLog::new(Arc::clone(&store));
    let payload = catalog_envelope_bytes_for(&catalog, 10, 12);
    log.append_validated_envelope(payload).await.unwrap();

    let err = log
        .replay_admitted_validated_envelopes_from(&[])
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        IngestLogError::MissingIngestAdmissionRecord { batch_key }
            if batch_key == ObjectKey::ingest_batch("orders", 7, 10, 12).unwrap()
    ));
}

#[tokio::test]
async fn admitted_replay_ignores_orphan_admission_without_batch_in_replay_window() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let log = IngestLog::new(Arc::clone(&store));
    let payload = catalog_envelope_bytes_for(&catalog, 10, 12);
    let admission = durable_admission_record_for_payload(payload);
    put_durable_admission_record(&store, &admission).await;

    let replayed = log
        .replay_admitted_validated_envelopes_from(&[])
        .await
        .unwrap();

    assert!(replayed.is_empty());
}

#[tokio::test]
async fn admitted_replay_rejects_admission_digest_mismatch() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let log = IngestLog::new(Arc::clone(&store));
    let payload = catalog_envelope_bytes_for(&catalog, 10, 12);
    let mut admission = durable_admission_record_for_payload(payload.clone());
    admission.payload_digest =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_string();
    put_durable_admission_record(&store, &admission).await;
    log.append_validated_envelope(payload).await.unwrap();

    let err = log
        .replay_admitted_validated_envelopes_from(&[])
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        IngestLogError::IngestAdmissionMismatch {
            field: "payload_digest",
            ..
        }
    ));
}

#[tokio::test]
async fn admitted_replay_rejects_checkpoint_inside_admitted_range() {
    let (_temp_dir, store) = temp_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let log = IngestLog::new(Arc::clone(&store));
    let payload = catalog_envelope_bytes_for(&catalog, 10, 20);
    let admission = durable_admission_record_for_payload(payload.clone());
    put_durable_admission_record(&store, &admission).await;
    log.append_validated_envelope(payload).await.unwrap();

    let err = log
        .replay_admitted_validated_envelopes_from(&[ReplayCheckpoint::new("orders", 7, 15)])
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        IngestLogError::CheckpointInsideAdmittedRange {
            checkpoint_end_offset_exclusive: 15,
            admission_record_key,
        } if admission_record_key == admission.admission_record_key
    ));
}
