use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
        StringDictionaryBuilder, TimestampNanosecondArray,
    },
    datatypes::{DataType, Field, Int32Type, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore, PutMode};
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::EngineCheckpoint,
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        DictionaryKeyTypeV1, FelderaRelationBindingV1, IncrementalAdapterBindingV1,
        RelationColumnV1, RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1,
        VelorixLogicalTypeV1, VelorixRelationCatalogV1, VelorixRelationSchemaV1,
        CATALOG_ROW_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID,
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
    },
};
#[cfg(feature = "dbsp-runtime")]
use velorix_runtime::recovery::IncrementalEngineBackend;
use velorix_runtime::recovery::{
    orders_sum_count_relation_catalog, RecoveredRuntime, RecoveryError,
    ORDERS_SUM_COUNT_ADAPTER_ID, ORDERS_SUM_COUNT_OWNER, ORDERS_SUM_COUNT_RELATION_ID,
    ORDERS_SUM_COUNT_RELATION_VERSION,
};
use velorix_storage::{
    capability::{
        probe_authoritative_object_store_capabilities, AuthoritativeNamespace,
        AuthoritativeObjectStoreCapabilitiesV1, AuthoritativeObjectStoreCapabilityError,
        ObjectStoreCapabilityProfile,
    },
    checkpoint_index::{CheckpointRecoveryMode, CheckpointRecoveryTransitionRecordV1},
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::{IngestAdmissionCoordinator, IngestLog, IngestLogError},
    manifest::{CheckpointManifest, InputRange, StateObjectRef},
    object_key::ObjectKey,
    relation_catalog_registry::{RelationCatalogRegistry, RelationCatalogRegistryError},
    state::{CheckpointPublisher, StateObjectWrite},
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

fn capabilities_missing(
    namespace: AuthoritativeNamespace,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    let profile = ObjectStoreCapabilityProfile::local_development();
    let mut profiles = AuthoritativeNamespace::all()
        .into_iter()
        .map(|namespace| (namespace, profile.clone()))
        .collect::<BTreeMap<_, _>>();
    profiles.remove(&namespace);
    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}

#[test]
fn default_orders_relation_catalog_keeps_legacy_adapter_id_for_durable_key_compatibility() {
    let catalog = orders_sum_count_relation_catalog().unwrap();

    assert_eq!(
        catalog.relation_schema.relation_id,
        ORDERS_SUM_COUNT_RELATION_ID
    );
    assert_eq!(
        catalog.relation_schema.relation_version,
        ORDERS_SUM_COUNT_RELATION_VERSION
    );
    assert_eq!(
        catalog.incremental_adapter.adapter_id,
        ORDERS_SUM_COUNT_ADAPTER_ID
    );
}

async fn probed_capabilities(store: &dyn ObjectStore) -> AuthoritativeObjectStoreCapabilitiesV1 {
    probe_authoritative_object_store_capabilities(store, "local-recovery-test", "v1/probes")
        .await
        .unwrap()
}

fn input_delta(account: &str, amount: i64, weight: i64) -> DeltaRecord {
    DeltaRecord::new(
        DeltaKey::from_json(json!(account)),
        DeltaValue::from_json(json!(amount)),
        weight,
    )
}

fn input_batch(records: impl IntoIterator<Item = DeltaRecord>) -> DeltaBatch {
    DeltaBatch::from_records(records)
}

fn ingest_record_batch(input: &DeltaBatch) -> RecordBatch {
    let keys = input
        .records()
        .iter()
        .map(|record| record.key.as_json().as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    let values = input
        .records()
        .iter()
        .map(|record| record.value.as_json().as_i64().unwrap())
        .collect::<Vec<_>>();
    let weights = input
        .records()
        .iter()
        .map(|record| record.weight)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
            Arc::new(Int64Array::from(weights)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn ingest_int64_key_record_batch(input: &DeltaBatch) -> RecordBatch {
    let keys = input
        .records()
        .iter()
        .map(|record| record.key.as_json().as_i64().unwrap())
        .collect::<Vec<_>>();
    let values = input
        .records()
        .iter()
        .map(|record| record.value.as_json().as_i64().unwrap())
        .collect::<Vec<_>>();
    let weights = input
        .records()
        .iter()
        .map(|record| record.weight)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
            Arc::new(Int64Array::from(weights)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn ingest_row_key_record_batch(
    account_ids: &[i64],
    currencies: &[&str],
    amounts: &[i64],
    weights: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Int64, false),
            Field::new("currency_code", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(account_ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(currencies.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(amounts.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(weights.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn ingest_boolean_key_record_batch(input: &DeltaBatch) -> RecordBatch {
    let keys = input
        .records()
        .iter()
        .map(|record| record.key.as_json().as_bool().unwrap())
        .collect::<Vec<_>>();
    let values = input
        .records()
        .iter()
        .map(|record| record.value.as_json().as_i64().unwrap())
        .collect::<Vec<_>>();
    let weights = input
        .records()
        .iter()
        .map(|record| record.weight)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Boolean, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(BooleanArray::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
            Arc::new(Int64Array::from(weights)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn ingest_decimal_key_record_batch(
    account_ids: &[i128],
    amounts: &[i64],
    weights: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Decimal128(38, 2), false),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(
                Decimal128Array::from(account_ids.to_vec())
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(Int64Array::from(amounts.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(weights.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn ingest_decimal_value_record_batch(
    account_ids: &[&str],
    amounts: &[i128],
    weights: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("amount", DataType::Decimal128(38, 2), false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(account_ids.to_vec())) as ArrayRef,
            Arc::new(
                Decimal128Array::from(amounts.to_vec())
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(Int64Array::from(weights.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn ingest_date32_key_record_batch(input: &DeltaBatch) -> RecordBatch {
    let keys = input
        .records()
        .iter()
        .map(|record| {
            i32::try_from(record.key.as_json().as_i64().unwrap()).expect("test Date32 key fits i32")
        })
        .collect::<Vec<_>>();
    let values = input
        .records()
        .iter()
        .map(|record| record.value.as_json().as_i64().unwrap())
        .collect::<Vec<_>>();
    let weights = input
        .records()
        .iter()
        .map(|record| record.weight)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("business_date", DataType::Date32, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Date32Array::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
            Arc::new(Int64Array::from(weights)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn ingest_timestamp_key_record_batch(input: &DeltaBatch) -> RecordBatch {
    let keys = input
        .records()
        .iter()
        .map(|record| record.key.as_json().as_i64().unwrap())
        .collect::<Vec<_>>();
    let values = input
        .records()
        .iter()
        .map(|record| record.value.as_json().as_i64().unwrap())
        .collect::<Vec<_>>();
    let weights = input
        .records()
        .iter()
        .map(|record| record.weight)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "observed_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(TimestampNanosecondArray::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
            Arc::new(Int64Array::from(weights)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn ingest_dictionary_utf8_key_record_batch(input: &DeltaBatch) -> RecordBatch {
    let mut key_builder = StringDictionaryBuilder::<Int32Type>::new();
    for record in input.records() {
        key_builder
            .append(record.key.as_json().as_str().unwrap())
            .unwrap();
    }
    let values = input
        .records()
        .iter()
        .map(|record| record.value.as_json().as_i64().unwrap())
        .collect::<Vec<_>>();
    let weights = input
        .records()
        .iter()
        .map(|record| record.weight)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "account_id",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(key_builder.finish()) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
            Arc::new(Int64Array::from(weights)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn ingest_json_utf8_key_record_batch(input: &DeltaBatch) -> RecordBatch {
    let keys = input
        .records()
        .iter()
        .map(|record| record.key.as_json().to_string())
        .collect::<Vec<_>>();
    let values = input
        .records()
        .iter()
        .map(|record| record.value.as_json().as_i64().unwrap())
        .collect::<Vec<_>>();
    let weights = input
        .records()
        .iter()
        .map(|record| record.weight)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_key", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(keys)) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
            Arc::new(Int64Array::from(weights)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn ingest_envelope_bytes(
    relation_version: &str,
    schema_fingerprint: &str,
    input: &DeltaBatch,
) -> Bytes {
    ingest_envelope_bytes_with_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
            relation_version: relation_version.to_string(),
            schema_fingerprint: schema_fingerprint.to_string(),
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: input.records().len() as u64,
        },
        &[ingest_record_batch(input)],
    )
}

fn ingest_envelope_bytes_with_batches(
    request: IngestEnvelopeEncodeRequest,
    batches: &[RecordBatch],
) -> Bytes {
    IngestEnvelope::encode_batches(request, batches).unwrap()
}

fn local_ingest_coordinator(store: Arc<dyn ObjectStore>) -> IngestAdmissionCoordinator {
    IngestAdmissionCoordinator::new(IngestLog::new(store))
}

async fn append_ingest_envelope(ingest_coordinator: &IngestAdmissionCoordinator, bytes: Bytes) {
    ingest_coordinator
        .append_catalog_validated_envelope(bytes)
        .await
        .unwrap();
}

async fn put_unadmitted_ingest_envelope(store: &Arc<dyn ObjectStore>, bytes: Bytes) -> ObjectKey {
    let envelope = IngestEnvelope::decode(bytes.clone()).unwrap();
    let header = envelope.header();
    let object_key = ObjectKey::ingest_batch(
        &header.stream_id,
        header.partition_id,
        header.start_offset_inclusive,
        header.end_offset_exclusive,
    )
    .unwrap();
    envelope
        .validate_descriptor(&velorix_storage::log::IngestBatchDescriptor {
            stream_id: header.stream_id.clone(),
            partition_id: header.partition_id,
            start_offset_inclusive: header.start_offset_inclusive,
            end_offset_exclusive: header.end_offset_exclusive,
            object_key: object_key.clone(),
        })
        .unwrap();

    store
        .put_opts(
            &Path::from(object_key.as_str()),
            bytes.into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();

    object_key
}

fn int64_account_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "accounts".to_string(),
        relation_name: "accounts".to_string(),
        relation_version: "2026-05-13.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
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
            name: "accounts".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "accounts".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn account_currency_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "account_currencies".to_string(),
        relation_name: "account_currencies".to_string(),
        relation_version: "2026-05-14.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "currency".to_string(),
                name: "currency_code".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "amount".to_string(),
                name: "amount".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "weight".to_string(),
                name: "weight".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 3,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["account_id".to_string(), "currency".to_string()],
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
            name: "account_currencies".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "account_currencies".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_ROW_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn boolean_account_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "boolean_accounts".to_string(),
        relation_name: "boolean_accounts".to_string(),
        relation_version: "2026-05-14.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Bool,
                physical_arrow_type: ArrowPhysicalTypeV1::Boolean,
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
            name: "boolean_accounts".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "boolean_accounts".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn decimal_account_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "decimal_accounts".to_string(),
        relation_name: "decimal_accounts".to_string(),
        relation_version: "2026-05-14.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Decimal {
                    precision: 38,
                    scale: 2,
                },
                physical_arrow_type: ArrowPhysicalTypeV1::Decimal128 {
                    precision: 38,
                    scale: 2,
                },
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
            name: "decimal_accounts".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "decimal_accounts".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn decimal_value_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "decimal_value_accounts".to_string(),
        relation_name: "decimal_value_accounts".to_string(),
        relation_version: "2026-05-15.v1".to_string(),
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
                logical_type: VelorixLogicalTypeV1::Decimal {
                    precision: 38,
                    scale: 2,
                },
                physical_arrow_type: ArrowPhysicalTypeV1::Decimal128 {
                    precision: 38,
                    scale: 2,
                },
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
            name: "decimal_value_accounts".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "decimal_value_accounts".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn unsupported_adapter_relation_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = orders_sum_count_relation_catalog().unwrap();
    catalog.incremental_adapter.adapter_id = "incremental-adapter-future-row-shaped-v1".to_string();
    catalog
}

fn multiple_value_relation_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = orders_sum_count_relation_catalog().unwrap();
    let mut extra_value = catalog.relation_schema.columns[1].clone();
    extra_value.column_id = "fee".to_string();
    extra_value.name = "fee".to_string();
    extra_value.ordinal = 3;
    catalog.relation_schema.columns.push(extra_value);
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.feldera_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    catalog
}

fn scalar_adapter_multi_key_relation_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = orders_sum_count_relation_catalog().unwrap();
    let mut extra_key = catalog.relation_schema.columns[0].clone();
    extra_key.column_id = "store_id".to_string();
    extra_key.name = "store_id".to_string();
    extra_key.ordinal = 3;
    catalog.relation_schema.columns.push(extra_key);
    catalog
        .relation_schema
        .primary_key_column_ids
        .push("store_id".to_string());
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.feldera_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    catalog
}

fn date32_daily_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "daily_balances".to_string(),
        relation_name: "daily_balances".to_string(),
        relation_version: "2026-05-13.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "business_date".to_string(),
                name: "business_date".to_string(),
                logical_type: VelorixLogicalTypeV1::Date,
                physical_arrow_type: ArrowPhysicalTypeV1::Date32,
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
        primary_key_column_ids: vec!["business_date".to_string()],
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
            name: "daily_balances".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "daily_balances".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn timestamped_observation_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "timestamped_observations".to_string(),
        relation_name: "timestamped_observations".to_string(),
        relation_version: "2026-05-13.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "observed_at".to_string(),
                name: "observed_at".to_string(),
                logical_type: VelorixLogicalTypeV1::Timestamp { timezone: None },
                physical_arrow_type: ArrowPhysicalTypeV1::TimestampNanosecond { timezone: None },
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
        primary_key_column_ids: vec!["observed_at".to_string()],
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
            name: "timestamped_observations".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "timestamped_observations".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn dictionary_account_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "dictionary_accounts".to_string(),
        relation_name: "dictionary_accounts".to_string(),
        relation_version: "2026-05-14.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_id".to_string(),
                name: "account_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::DictionaryUtf8 {
                    key_type: DictionaryKeyTypeV1::Int32,
                    ordered: false,
                },
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
            name: "dictionary_accounts".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "dictionary_accounts".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn json_account_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "json_accounts".to_string(),
        relation_name: "json_accounts".to_string(),
        relation_version: "2026-05-14.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "account_key".to_string(),
                name: "account_key".to_string(),
                logical_type: VelorixLogicalTypeV1::Json,
                physical_arrow_type: ArrowPhysicalTypeV1::JsonUtf8,
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
        primary_key_column_ids: vec!["account_key".to_string()],
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
            name: "json_accounts".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "json_accounts".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn checkpoint_input_range(end_offset_exclusive: u64) -> InputRange {
    InputRange {
        stream_id: "orders".to_string(),
        partition_id: 0,
        start_offset_inclusive: 0,
        end_offset_exclusive,
    }
}

fn selected_checkpoint_manifest(
    checkpoint_version: u64,
    input_end: u64,
    state_ref: StateObjectRef,
) -> CheckpointManifest {
    CheckpointManifest {
        schema_version: 1,
        checkpoint_version,
        input_ranges: vec![checkpoint_input_range(input_end)],
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint: None,
        created_at: "2026-05-04T00:00:00Z".to_string(),
    }
}

async fn write_checkpoint_state(
    publisher: &CheckpointPublisher,
    checkpoint_version: u64,
    logical_epoch: u64,
    state: &DeltaBatch,
) -> StateObjectRef {
    let checkpoint = EngineCheckpoint::new(logical_epoch, state.clone());
    let state = StateObjectWrite::new(
        ORDERS_SUM_COUNT_OWNER,
        0,
        checkpoint_version,
        format!("state-{checkpoint_version}"),
        Bytes::from(serde_json::to_vec(&checkpoint.to_payload()).unwrap()),
    )
    .unwrap();

    publisher.write_state_object(&state).await.unwrap()
}

fn aggregate_state(account: &str, sum: i64, count: i64) -> DeltaBatch {
    input_batch([DeltaRecord::new(
        DeltaKey::from_json(json!(account)),
        DeltaValue::from_json(json!({ "count": count, "sum": sum })),
        1,
    )])
}

fn aggregate_decimal_state(account: &str, sum: &str, count: i64) -> DeltaBatch {
    input_batch([DeltaRecord::new(
        DeltaKey::from_json(json!(account)),
        DeltaValue::from_json(json!({ "count": count, "sum": sum })),
        1,
    )])
}

#[tokio::test]
async fn catalog_backed_recovery_reads_catalog_record_and_replays_catalog_aware_ingest() {
    let (_temp_dir, store) = temp_store();
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([input_delta("account-a", 4, 1)]);
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes(
            ORDERS_SUM_COUNT_RELATION_VERSION,
            catalog.schema_fingerprint.as_str(),
            &input,
        ),
    )
    .await;

    let recovered = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
    )
    .await
    .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 1);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("account-a")),
            DeltaValue::from_json(json!({"count": 1, "sum": 4})),
            1,
        )]
    );
}

#[cfg(feature = "dbsp-runtime")]
#[tokio::test]
async fn catalog_backed_recovery_can_replay_with_dbsp_backend() {
    let (_temp_dir, store) = temp_store();
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([
        input_delta("account-a", 4, 1),
        input_delta("account-a", 6, 1),
    ]);
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes(
            ORDERS_SUM_COUNT_RELATION_VERSION,
            catalog.schema_fingerprint.as_str(),
            &input,
        ),
    )
    .await;

    let recovered =
        RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record_using_engine_backend(
            Arc::clone(&store),
            ORDERS_SUM_COUNT_OWNER,
            ORDERS_SUM_COUNT_RELATION_ID,
            ORDERS_SUM_COUNT_RELATION_VERSION,
            IncrementalEngineBackend::Dbsp,
        )
        .await
        .unwrap();

    assert_eq!(recovered.engine_backend(), IncrementalEngineBackend::Dbsp);
    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 1);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("account-a")),
            DeltaValue::from_json(json!({"count": 2, "sum": 10})),
            1,
        )]
    );
}

#[cfg(feature = "dbsp-runtime")]
#[tokio::test]
async fn catalog_backed_recovery_uses_dbsp_backend_by_default() {
    let (_temp_dir, store) = temp_store();
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([input_delta("account-a", 4, 1)]);
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes(
            ORDERS_SUM_COUNT_RELATION_VERSION,
            catalog.schema_fingerprint.as_str(),
            &input,
        ),
    )
    .await;

    let recovered = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
    )
    .await
    .unwrap();

    assert_eq!(recovered.engine_backend(), IncrementalEngineBackend::Dbsp);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("account-a")),
            DeltaValue::from_json(json!({"count": 1, "sum": 4})),
            1,
        )]
    );
}

#[cfg(feature = "dbsp-runtime")]
#[tokio::test]
async fn catalog_backed_recovery_uses_dbsp_backend_for_generic_catalog_keys() {
    let (_temp_dir, store) = temp_store();
    let catalog = int64_account_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([
        DeltaRecord::new(
            DeltaKey::from_json(json!(1001)),
            DeltaValue::from_json(json!(4)),
            1,
        ),
        DeltaRecord::new(
            DeltaKey::from_json(json!(1001)),
            DeltaValue::from_json(json!(6)),
            1,
        ),
    ]);
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "accounts".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: input.records().len() as u64,
            },
            &[ingest_int64_key_record_batch(&input)],
        ),
    )
    .await;

    let recovered = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
    )
    .await
    .unwrap();

    assert_eq!(recovered.engine_backend(), IncrementalEngineBackend::Dbsp);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!(1001)),
            DeltaValue::from_json(json!({"count": 2, "sum": 10})),
            1,
        )]
    );
}

#[tokio::test]
async fn catalog_backed_recovery_replays_int64_primary_key_relation() {
    let (_temp_dir, store) = temp_store();
    let catalog = int64_account_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([
        DeltaRecord::new(
            DeltaKey::from_json(json!(1001)),
            DeltaValue::from_json(json!(4)),
            1,
        ),
        DeltaRecord::new(
            DeltaKey::from_json(json!(1002)),
            DeltaValue::from_json(json!(7)),
            1,
        ),
    ]);
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "accounts".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: input.records().len() as u64,
            },
            &[ingest_int64_key_record_batch(&input)],
        ),
    )
    .await;

    let recovered = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
    )
    .await
    .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 1);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!(1001)),
                DeltaValue::from_json(json!({"count": 1, "sum": 4})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!(1002)),
                DeltaValue::from_json(json!({"count": 1, "sum": 7})),
                1,
            ),
        ]
    );
}

#[tokio::test]
async fn catalog_backed_recovery_replays_row_key_relation() {
    let (_temp_dir, store) = temp_store();
    let catalog = account_currency_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "account-currencies".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: 2,
            },
            &[ingest_row_key_record_batch(
                &[1001, 1001],
                &["USD", "EUR"],
                &[4, 7],
                &[1, 1],
            )],
        ),
    )
    .await;

    let recovered = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
    )
    .await
    .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 1);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!({
                    "account_id": 1001,
                    "currency": "EUR"
                })),
                DeltaValue::from_json(json!({"count": 1, "sum": 7})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!({
                    "account_id": 1001,
                    "currency": "USD"
                })),
                DeltaValue::from_json(json!({"count": 1, "sum": 4})),
                1,
            ),
        ]
    );
}

#[tokio::test]
async fn catalog_backed_recovery_replays_boolean_primary_key_relation() {
    let (_temp_dir, store) = temp_store();
    let catalog = boolean_account_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([
        DeltaRecord::new(
            DeltaKey::from_json(json!(true)),
            DeltaValue::from_json(json!(4)),
            1,
        ),
        DeltaRecord::new(
            DeltaKey::from_json(json!(false)),
            DeltaValue::from_json(json!(7)),
            1,
        ),
    ]);
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "boolean-accounts".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: input.records().len() as u64,
            },
            &[ingest_boolean_key_record_batch(&input)],
        ),
    )
    .await;

    let recovered = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
    )
    .await
    .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 1);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!(false)),
                DeltaValue::from_json(json!({"count": 1, "sum": 7})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!(true)),
                DeltaValue::from_json(json!({"count": 1, "sum": 4})),
                1,
            ),
        ]
    );
}

#[tokio::test]
async fn catalog_backed_recovery_replays_decimal128_primary_key_relation() {
    let (_temp_dir, store) = temp_store();
    let catalog = decimal_account_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([
        DeltaRecord::new(
            DeltaKey::from_json(json!("1234567890123456789012345678901234.56")),
            DeltaValue::from_json(json!(4)),
            1,
        ),
        DeltaRecord::new(
            DeltaKey::from_json(json!("-1.00")),
            DeltaValue::from_json(json!(7)),
            1,
        ),
    ]);
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "decimal-accounts".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: input.records().len() as u64,
            },
            &[ingest_decimal_key_record_batch(
                &[
                    123_456_789_012_345_678_901_234_567_890_123_456_i128,
                    -100_i128,
                ],
                &[4, 7],
                &[1, 1],
            )],
        ),
    )
    .await;

    let recovered = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
    )
    .await
    .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 1);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!("-1.00")),
                DeltaValue::from_json(json!({"count": 1, "sum": 7})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("1234567890123456789012345678901234.56")),
                DeltaValue::from_json(json!({"count": 1, "sum": 4})),
                1,
            ),
        ]
    );
}

#[tokio::test]
async fn catalog_backed_recovery_replays_decimal128_value_relation_exactly() {
    let (_temp_dir, store) = temp_store();
    let catalog = decimal_value_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "decimal-value-accounts".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: 3,
            },
            &[ingest_decimal_value_record_batch(
                &["account-a", "account-a", "account-a"],
                &[10, 20, 10],
                &[1, 1, -1],
            )],
        ),
    )
    .await;

    let recovered = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
    )
    .await
    .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 1);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("account-a")),
            DeltaValue::from_json(json!({"count": 1, "sum": "0.20"})),
            1,
        )]
    );
}

#[tokio::test]
async fn catalog_backed_recovery_replays_date32_primary_key_relation() {
    let (_temp_dir, store) = temp_store();
    let catalog = date32_daily_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([
        DeltaRecord::new(
            DeltaKey::from_json(json!(20_586)),
            DeltaValue::from_json(json!(4)),
            1,
        ),
        DeltaRecord::new(
            DeltaKey::from_json(json!(20_587)),
            DeltaValue::from_json(json!(7)),
            1,
        ),
    ]);
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "daily-balances".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: input.records().len() as u64,
            },
            &[ingest_date32_key_record_batch(&input)],
        ),
    )
    .await;

    let recovered = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
    )
    .await
    .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 1);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!(20_586)),
                DeltaValue::from_json(json!({"count": 1, "sum": 4})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!(20_587)),
                DeltaValue::from_json(json!({"count": 1, "sum": 7})),
                1,
            ),
        ]
    );
}

#[tokio::test]
async fn catalog_backed_recovery_replays_timestamp_nanosecond_primary_key_relation() {
    let (_temp_dir, store) = temp_store();
    let catalog = timestamped_observation_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([
        DeltaRecord::new(
            DeltaKey::from_json(json!(1_769_289_600_000_000_000_i64)),
            DeltaValue::from_json(json!(4)),
            1,
        ),
        DeltaRecord::new(
            DeltaKey::from_json(json!(1_769_293_200_000_000_000_i64)),
            DeltaValue::from_json(json!(7)),
            1,
        ),
    ]);
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "timestamped-observations".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: input.records().len() as u64,
            },
            &[ingest_timestamp_key_record_batch(&input)],
        ),
    )
    .await;

    let recovered = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
    )
    .await
    .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 1);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!(1_769_289_600_000_000_000_i64)),
                DeltaValue::from_json(json!({"count": 1, "sum": 4})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!(1_769_293_200_000_000_000_i64)),
                DeltaValue::from_json(json!({"count": 1, "sum": 7})),
                1,
            ),
        ]
    );
}

#[tokio::test]
async fn catalog_backed_recovery_replays_dictionary_utf8_primary_key_relation() {
    let (_temp_dir, store) = temp_store();
    let catalog = dictionary_account_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([
        DeltaRecord::new(
            DeltaKey::from_json(json!("account-a")),
            DeltaValue::from_json(json!(4)),
            1,
        ),
        DeltaRecord::new(
            DeltaKey::from_json(json!("account-b")),
            DeltaValue::from_json(json!(7)),
            1,
        ),
    ]);
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "dictionary-accounts".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: input.records().len() as u64,
            },
            &[ingest_dictionary_utf8_key_record_batch(&input)],
        ),
    )
    .await;

    let recovered = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
    )
    .await
    .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 1);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!("account-a")),
                DeltaValue::from_json(json!({"count": 1, "sum": 4})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!("account-b")),
                DeltaValue::from_json(json!({"count": 1, "sum": 7})),
                1,
            ),
        ]
    );
}

#[tokio::test]
async fn catalog_backed_recovery_replays_json_utf8_primary_key_relation() {
    let (_temp_dir, store) = temp_store();
    let catalog = json_account_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([
        DeltaRecord::new(
            DeltaKey::from_json(json!({"tenant": "a", "account": 1001})),
            DeltaValue::from_json(json!(4)),
            1,
        ),
        DeltaRecord::new(
            DeltaKey::from_json(json!({"tenant": "b", "account": 1002})),
            DeltaValue::from_json(json!(7)),
            1,
        ),
    ]);
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "json-accounts".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: input.records().len() as u64,
            },
            &[ingest_json_utf8_key_record_batch(&input)],
        ),
    )
    .await;

    let recovered = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
    )
    .await
    .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 1);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![
            DeltaRecord::new(
                DeltaKey::from_json(json!({"tenant": "a", "account": 1001})),
                DeltaValue::from_json(json!({"count": 1, "sum": 4})),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(json!({"tenant": "b", "account": 1002})),
                DeltaValue::from_json(json!({"count": 1, "sum": 7})),
                1,
            ),
        ]
    );
}

#[tokio::test]
async fn checked_catalog_backed_recovery_requires_complete_authoritative_capabilities() {
    let (_temp_dir, store) = temp_store();
    let capabilities = capabilities_missing(AuthoritativeNamespace::Ingest);

    let error = RecoveredRuntime::recover_with_owner_and_relation_catalog_record_checked(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &capabilities,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::AuthoritativeObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace {
                namespace: AuthoritativeNamespace::Ingest
            }
        )
    ));
}

#[tokio::test]
async fn checked_catalog_backed_recovery_requires_ingest_admission_capability() {
    let (_temp_dir, store) = temp_store();
    let capabilities = capabilities_missing(AuthoritativeNamespace::IngestAdmission);

    let error = RecoveredRuntime::recover_with_owner_and_relation_catalog_record_checked(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &capabilities,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::AuthoritativeObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace {
                namespace: AuthoritativeNamespace::IngestAdmission
            }
        )
    ));
}

#[tokio::test]
async fn checked_catalog_backed_recovery_rejects_replayed_ingest_without_admission_record() {
    let (_temp_dir, store) = temp_store();
    let capabilities = probed_capabilities(store.as_ref()).await;
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([input_delta("account-a", 4, 1)]);
    let batch_key = put_unadmitted_ingest_envelope(
        &store,
        ingest_envelope_bytes(
            ORDERS_SUM_COUNT_RELATION_VERSION,
            catalog.schema_fingerprint.as_str(),
            &input,
        ),
    )
    .await;

    let error = RecoveredRuntime::recover_with_owner_and_relation_catalog_record_checked(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &capabilities,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::Ingest(IngestLogError::MissingIngestAdmissionRecord { batch_key: actual })
            if actual == batch_key
    ));
}

#[tokio::test]
async fn checked_catalog_backed_recovery_reads_catalog_with_valid_capabilities() {
    let (_temp_dir, store) = temp_store();
    let capabilities = probed_capabilities(store.as_ref()).await;
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([input_delta("account-a", 4, 1)]);
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes(
            ORDERS_SUM_COUNT_RELATION_VERSION,
            catalog.schema_fingerprint.as_str(),
            &input,
        ),
    )
    .await;

    let recovered = RecoveredRuntime::recover_with_owner_and_relation_catalog_record_checked(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &capabilities,
    )
    .await
    .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 1);
}

#[tokio::test]
async fn checked_catalog_backed_recovery_rejects_batch_without_admission() {
    let (_temp_dir, store) = temp_store();
    let capabilities = probed_capabilities(store.as_ref()).await;
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([input_delta("account-a", 4, 1)]);
    IngestLog::new(Arc::clone(&store))
        .append_validated_envelope(ingest_envelope_bytes(
            ORDERS_SUM_COUNT_RELATION_VERSION,
            catalog.schema_fingerprint.as_str(),
            &input,
        ))
        .await
        .unwrap();

    let error = RecoveredRuntime::recover_with_owner_and_relation_catalog_record_checked(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &capabilities,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::Ingest(IngestLogError::MissingIngestAdmissionRecord { .. })
    ));
}

#[tokio::test]
async fn checked_selected_checkpoint_recovery_rejects_post_checkpoint_ingest_without_admission_record(
) {
    let (_temp_dir, store) = temp_store();
    let capabilities = probed_capabilities(store.as_ref()).await;
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let checkpoint_version = 0;
    let checkpoint_state = aggregate_state("account-a", 4, 1);
    let replay_input = input_batch([input_delta("account-a", 3, 1)]);
    let batch_key = put_unadmitted_ingest_envelope(
        &store,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
                relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "orders".to_string(),
                partition_id: 0,
                start_offset_inclusive: 1,
                end_offset_exclusive: 2,
            },
            &[ingest_record_batch(&replay_input)],
        ),
    )
    .await;
    let state_ref =
        write_checkpoint_state(&publisher, checkpoint_version, 1, &checkpoint_state).await;
    publisher
        .publish_manifest(&selected_checkpoint_manifest(
            checkpoint_version,
            1,
            state_ref,
        ))
        .await
        .unwrap();

    let error = RecoveredRuntime::recover_from_published_checkpoint_version_checked(
        Arc::clone(&store),
        checkpoint_version,
        &capabilities,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::Ingest(IngestLogError::MissingIngestAdmissionRecord { batch_key: actual })
            if actual == batch_key
    ));
}

#[tokio::test]
async fn checked_selected_checkpoint_recovery_allows_pre_admission_batch_when_checkpoint_covers_it()
{
    let (_temp_dir, store) = temp_store();
    let capabilities = probed_capabilities(store.as_ref()).await;
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let checkpoint_version = 0;
    let checkpoint_state = aggregate_state("account-a", 4, 1);
    let checkpointed_input = input_batch([input_delta("account-z", 99, 1)]);
    put_unadmitted_ingest_envelope(
        &store,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
                relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "orders".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: 1,
            },
            &[ingest_record_batch(&checkpointed_input)],
        ),
    )
    .await;
    let state_ref =
        write_checkpoint_state(&publisher, checkpoint_version, 1, &checkpoint_state).await;
    publisher
        .publish_manifest(&selected_checkpoint_manifest(
            checkpoint_version,
            1,
            state_ref,
        ))
        .await
        .unwrap();

    let recovered = RecoveredRuntime::recover_from_published_checkpoint_version_checked(
        Arc::clone(&store),
        checkpoint_version,
        &capabilities,
    )
    .await
    .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 0);
    assert_eq!(recovered.logical_epoch(), 1);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("account-a")),
            DeltaValue::from_json(json!({"count": 1, "sum": 4})),
            1,
        )]
    );
}

#[tokio::test]
async fn checked_selected_checkpoint_recovery_requires_checkpoint_capability() {
    let (_temp_dir, store) = temp_store();
    let capabilities = capabilities_missing(AuthoritativeNamespace::Checkpoint);

    let error =
        RecoveredRuntime::recover_from_published_checkpoint_version_with_owner_and_relation_catalog_checked(
            Arc::clone(&store),
            0,
            ORDERS_SUM_COUNT_OWNER,
            orders_sum_count_relation_catalog().unwrap(),
            &capabilities,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::AuthoritativeObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace {
                namespace: AuthoritativeNamespace::Checkpoint
            }
        )
    ));
}

#[tokio::test]
async fn checked_selected_checkpoint_recovery_requires_ingest_capability() {
    let (_temp_dir, store) = temp_store();
    let capabilities = capabilities_missing(AuthoritativeNamespace::Ingest);

    let error =
        RecoveredRuntime::recover_from_published_checkpoint_version_with_owner_and_relation_catalog_record_checked(
            Arc::clone(&store),
            0,
            ORDERS_SUM_COUNT_OWNER,
            ORDERS_SUM_COUNT_RELATION_ID,
            ORDERS_SUM_COUNT_RELATION_VERSION,
            &capabilities,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::AuthoritativeObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace {
                namespace: AuthoritativeNamespace::Ingest
            }
        )
    ));
}

#[tokio::test]
async fn checked_selected_checkpoint_recovery_requires_checkpoint_recovery_capability() {
    let (_temp_dir, store) = temp_store();
    let capabilities = capabilities_missing(AuthoritativeNamespace::CheckpointRecovery);

    let error =
        RecoveredRuntime::recover_from_published_checkpoint_version_with_owner_and_relation_catalog_record_checked(
            Arc::clone(&store),
            0,
            ORDERS_SUM_COUNT_OWNER,
            ORDERS_SUM_COUNT_RELATION_ID,
            ORDERS_SUM_COUNT_RELATION_VERSION,
            &capabilities,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::AuthoritativeObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace {
                namespace: AuthoritativeNamespace::CheckpointRecovery
            }
        )
    ));
}

#[tokio::test]
async fn checked_selected_checkpoint_recovery_replays_catalog_aware_ingest() {
    let (_temp_dir, store) = temp_store();
    let capabilities = probed_capabilities(store.as_ref()).await;
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();

    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let checkpoint_version = 0;
    let checkpoint_state = aggregate_state("account-a", 4, 1);
    let replay_input = input_batch([input_delta("account-a", 3, 1)]);

    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
                relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "orders".to_string(),
                partition_id: 0,
                start_offset_inclusive: 1,
                end_offset_exclusive: 2,
            },
            &[ingest_record_batch(&replay_input)],
        ),
    )
    .await;
    let state_ref =
        write_checkpoint_state(&publisher, checkpoint_version, 1, &checkpoint_state).await;
    publisher
        .publish_manifest(&selected_checkpoint_manifest(
            checkpoint_version,
            1,
            state_ref,
        ))
        .await
        .unwrap();

    let recovered = RecoveredRuntime::recover_from_published_checkpoint_version_checked(
        Arc::clone(&store),
        checkpoint_version,
        &capabilities,
    )
    .await
    .unwrap();

    assert_eq!(
        recovered.latest_checkpoint_version(),
        Some(checkpoint_version)
    );
    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 2);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("account-a")),
            DeltaValue::from_json(json!({"count": 2, "sum": 7})),
            1,
        )]
    );

    let transition = single_recovery_transition_record(store.as_ref(), checkpoint_version).await;
    assert_eq!(transition.checkpoint_version, checkpoint_version);
    assert_eq!(
        transition.manifest_key,
        ObjectKey::checkpoint_manifest(checkpoint_version)
    );
    assert_eq!(
        transition.recovery_mode,
        CheckpointRecoveryMode::SelectedCheckpoint
    );
    assert_eq!(transition.replay_checkpoint_count, 1);
    assert_eq!(transition.replayed_batch_count, 1);
    assert_eq!(
        publisher
            .read_checkpoint_recovery_transition_record(
                checkpoint_version,
                transition.transition_id.as_str()
            )
            .await
            .unwrap(),
        transition
    );
}

#[tokio::test]
async fn selected_checkpoint_recovery_hydrates_decimal128_value_state() {
    let (_temp_dir, store) = temp_store();
    let catalog = decimal_value_relation_catalog();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let checkpoint_version = 0;
    let checkpoint_state = aggregate_decimal_state("account-a", "0.10", 1);

    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: catalog.relation_schema.relation_id.clone(),
                relation_version: catalog.relation_schema.relation_version.clone(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "orders".to_string(),
                partition_id: 0,
                start_offset_inclusive: 1,
                end_offset_exclusive: 2,
            },
            &[ingest_decimal_value_record_batch(
                &["account-a"],
                &[20],
                &[1],
            )],
        ),
    )
    .await;
    let state_ref =
        write_checkpoint_state(&publisher, checkpoint_version, 1, &checkpoint_state).await;
    publisher
        .publish_manifest(&selected_checkpoint_manifest(
            checkpoint_version,
            1,
            state_ref,
        ))
        .await
        .unwrap();

    let recovered =
        RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version_with_owner_and_relation_catalog(
            Arc::clone(&store),
            checkpoint_version,
            ORDERS_SUM_COUNT_OWNER,
            catalog,
        )
        .await
        .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 2);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("account-a")),
            DeltaValue::from_json(json!({"count": 2, "sum": "0.30"})),
            1,
        )]
    );
}

#[tokio::test]
async fn selected_checkpoint_recovery_rejects_unsupported_adapter_before_checkpoint_hydration() {
    let (_temp_dir, store) = temp_store();
    let catalog = unsupported_adapter_relation_catalog();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let checkpoint_version = 0;
    let checkpoint_state = aggregate_state("account-a", 4, 1);
    let state_ref =
        write_checkpoint_state(&publisher, checkpoint_version, 1, &checkpoint_state).await;
    publisher
        .publish_manifest(&selected_checkpoint_manifest(
            checkpoint_version,
            1,
            state_ref,
        ))
        .await
        .unwrap();

    let error =
        RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version_with_owner_and_relation_catalog(
            Arc::clone(&store),
            checkpoint_version,
            ORDERS_SUM_COUNT_OWNER,
            catalog,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::UnsupportedIncrementalAdapter { adapter_id }
            if adapter_id == "incremental-adapter-future-row-shaped-v1"
    ));
}

#[tokio::test]
async fn selected_checkpoint_recovery_rejects_multiple_value_columns_before_checkpoint_hydration() {
    let (_temp_dir, store) = temp_store();
    let catalog = multiple_value_relation_catalog();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let checkpoint_version = 0;
    let checkpoint_state = aggregate_state("account-a", 4, 1);
    let state_ref =
        write_checkpoint_state(&publisher, checkpoint_version, 1, &checkpoint_state).await;
    publisher
        .publish_manifest(&selected_checkpoint_manifest(
            checkpoint_version,
            1,
            state_ref,
        ))
        .await
        .unwrap();

    let error =
        RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version_with_owner_and_relation_catalog(
            Arc::clone(&store),
            checkpoint_version,
            ORDERS_SUM_COUNT_OWNER,
            catalog,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::MalformedPrototypeArrowIngest { reason }
            if reason == "prototype adapter supports exactly one value column"
    ));
}

#[tokio::test]
async fn selected_checkpoint_recovery_rejects_scalar_adapter_multi_key_before_checkpoint_hydration()
{
    let (_temp_dir, store) = temp_store();
    let catalog = scalar_adapter_multi_key_relation_catalog();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let checkpoint_version = 0;
    let checkpoint_state = aggregate_state("account-a", 4, 1);
    let state_ref =
        write_checkpoint_state(&publisher, checkpoint_version, 1, &checkpoint_state).await;
    publisher
        .publish_manifest(&selected_checkpoint_manifest(
            checkpoint_version,
            1,
            state_ref,
        ))
        .await
        .unwrap();

    let error =
        RecoveredRuntime::recover_bootstrap_from_published_checkpoint_version_with_owner_and_relation_catalog(
            Arc::clone(&store),
            checkpoint_version,
            ORDERS_SUM_COUNT_OWNER,
            catalog,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::MalformedPrototypeArrowIngest { reason }
            if reason == "prototype adapter supports exactly one primary key column"
    ));
}

#[tokio::test]
async fn checked_selected_checkpoint_recovery_with_slatedb_state_replays_catalog_aware_ingest() {
    let (_temp_dir, store) = temp_store();
    let capabilities = probed_capabilities(store.as_ref()).await;
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();

    let ingest_coordinator = local_ingest_coordinator(Arc::clone(&store));
    let publisher =
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap();
    let checkpoint_version = 0;
    let checkpoint_state = aggregate_state("account-a", 4, 1);
    let replay_input = input_batch([input_delta("account-a", 3, 1)]);

    append_ingest_envelope(
        &ingest_coordinator,
        ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
                relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "orders".to_string(),
                partition_id: 0,
                start_offset_inclusive: 1,
                end_offset_exclusive: 2,
            },
            &[ingest_record_batch(&replay_input)],
        ),
    )
    .await;
    let state_ref =
        write_checkpoint_state(&publisher, checkpoint_version, 1, &checkpoint_state).await;
    publisher
        .publish_manifest(&selected_checkpoint_manifest(
            checkpoint_version,
            1,
            state_ref,
        ))
        .await
        .unwrap();

    let recovered =
        RecoveredRuntime::recover_from_published_checkpoint_version_with_slatedb_state_store_checked(
            Arc::clone(&store),
            "v1/slatedb/state",
            checkpoint_version,
            &capabilities,
        )
        .await
        .unwrap();

    assert_eq!(
        recovered.latest_checkpoint_version(),
        Some(checkpoint_version)
    );
    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 2);
    assert_eq!(
        recovered.materialized_state().net_rows().unwrap(),
        vec![DeltaRecord::new(
            DeltaKey::from_json(json!("account-a")),
            DeltaValue::from_json(json!({"count": 2, "sum": 7})),
            1,
        )]
    );

    let transition = single_recovery_transition_record(store.as_ref(), checkpoint_version).await;
    assert_eq!(
        transition.recovery_mode,
        CheckpointRecoveryMode::SelectedCheckpoint
    );
    assert_eq!(transition.replay_checkpoint_count, 1);
    assert_eq!(transition.replayed_batch_count, 1);
}

#[tokio::test]
async fn checked_selected_checkpoint_recovery_with_slatedb_state_requires_checkpoint_capability() {
    let (_temp_dir, store) = temp_store();
    let capabilities = capabilities_missing(AuthoritativeNamespace::Checkpoint);

    let error =
        RecoveredRuntime::recover_from_published_checkpoint_version_with_slatedb_state_store_checked(
            Arc::clone(&store),
            "v1/slatedb/state",
            0,
            &capabilities,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::AuthoritativeObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace {
                namespace: AuthoritativeNamespace::Checkpoint
            }
        )
    ));
}

#[tokio::test]
async fn checked_selected_checkpoint_recovery_with_slatedb_state_requires_state_capability() {
    let (_temp_dir, store) = temp_store();
    let capabilities = capabilities_missing(AuthoritativeNamespace::State);

    let error =
        RecoveredRuntime::recover_from_published_checkpoint_version_with_slatedb_state_store_checked(
            Arc::clone(&store),
            "v1/slatedb/state",
            0,
            &capabilities,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::AuthoritativeObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace {
                namespace: AuthoritativeNamespace::State
            }
        )
    ));
}

#[tokio::test]
async fn catalog_backed_recovery_fails_closed_when_catalog_record_is_missing() {
    let (_temp_dir, store) = temp_store();

    let error = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::RelationCatalogRegistry(RelationCatalogRegistryError::ObjectStore(
            object_store::Error::NotFound { .. }
        ))
    ));
}

#[tokio::test]
async fn catalog_backed_recovery_rejects_replayed_ingest_relation_drift() {
    let (_temp_dir, store) = temp_store();
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([input_delta("account-a", 4, 1)]);
    IngestLog::new(Arc::clone(&store))
        // Intentional bootstrap append: this fixture needs durable relation drift.
        .append_validated_envelope(ingest_envelope_bytes(
            "2026-05-06.v1",
            catalog.schema_fingerprint.as_str(),
            &input,
        ))
        .await
        .unwrap();

    let error = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::IngestRelationMismatch {
            field: "relation_version",
            ..
        }
    ));
}

#[tokio::test]
async fn catalog_backed_recovery_reports_malformed_ingest_when_batch_schema_differs_from_catalog() {
    let (_temp_dir, store) = temp_store();
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let wrong_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key_json", DataType::Utf8, false),
            Field::new("value_json", DataType::Utf8, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["account-a"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["4"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ],
    )
    .unwrap();
    IngestLog::new(Arc::clone(&store))
        // Intentional bootstrap append: this fixture needs a malformed batch
        // schema that catalog-aware append would reject before commit.
        .append_validated_envelope(ingest_envelope_bytes_with_batches(
            IngestEnvelopeEncodeRequest {
                relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
                relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: "orders".to_string(),
                partition_id: 0,
                start_offset_inclusive: 0,
                end_offset_exclusive: 1,
            },
            &[wrong_batch],
        ))
        .await
        .unwrap();

    let error = RecoveredRuntime::recover_bootstrap_with_owner_and_relation_catalog_record(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::MalformedPrototypeArrowIngest { reason }
            if reason.contains("schema")
    ));
}

async fn single_recovery_transition_record(
    store: &dyn ObjectStore,
    checkpoint_version: u64,
) -> CheckpointRecoveryTransitionRecordV1 {
    let prefix = format!("v1/checkpoint-recovery/{checkpoint_version:020}/transitions");
    let objects = store
        .list(Some(&Path::from(prefix)))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(objects.len(), 1);

    let bytes = store
        .get(&objects[0].location)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
