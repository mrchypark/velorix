use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, StringArray,
        StringDictionaryBuilder, Time64NanosecondArray, TimestampNanosecondArray, UInt64Array,
    },
    datatypes::{DataType, Field, Int16Type, Int32Type, Int64Type, Int8Type, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use velorix_core::delta::{DeltaKey, DeltaRecord, DeltaValue};
use velorix_core::relation::{
    arrow_record_batches_to_key_value_delta_batch,
    arrow_record_batches_to_orders_sum_count_delta_batch,
    arrow_record_batches_to_single_key_sum_count_delta_batch, datafusion_schema_from_catalog,
    orders_sum_count_relation_catalog, validate_record_batch_matches_catalog, ArrowPhysicalTypeV1,
    DataFusionRegistrationModeV1, DataFusionRegistrationV1, DictionaryKeyTypeV1,
    IncrementalAdapterBindingV1, IncrementalInputAdapterError, IncrementalRelationBindingV1,
    RelationColumnV1, RelationOperationV1, RelationSchemaError, RelationSemanticRoleV1,
    SchemaFingerprintV1, VelorixLogicalTypeV1, VelorixRelationCatalogV1, VelorixRelationSchemaV1,
    VelorixRelationSourceV1, CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID,
    CATALOG_ROW_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID,
    CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, ORDERS_SUM_COUNT_ADAPTER_ID,
    ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID, ORDERS_SUM_COUNT_RELATION_ID,
    ORDERS_SUM_COUNT_RELATION_VERSION, RELATION_SCHEMA_VERSION_V1,
};

const ORDERS_RELATION_SCHEMA_FINGERPRINT: &str =
    "sha256:0dc18e09a12b5b2ad4aced2e5e96c0ed49775b418e53478fc5456aa0e2c554e6";

fn orders_relation_schema() -> VelorixRelationSchemaV1 {
    VelorixRelationSchemaV1 {
        relation_id: "orders".to_string(),
        relation_name: "orders".to_string(),
        relation_version: "2026-05-05.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "order_id".to_string(),
                name: "order_id".to_string(),
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
                    precision: 18,
                    scale: 2,
                },
                physical_arrow_type: ArrowPhysicalTypeV1::Decimal128 {
                    precision: 18,
                    scale: 2,
                },
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "event_ts".to_string(),
                name: "event_ts".to_string(),
                logical_type: VelorixLogicalTypeV1::Timestamp {
                    timezone: Some("UTC".to_string()),
                },
                physical_arrow_type: ArrowPhysicalTypeV1::TimestampNanosecond {
                    timezone: Some("UTC".to_string()),
                },
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::EventTime,
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
        primary_key_column_ids: vec!["order_id".to_string()],
        weight_column_id: "weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: Some("event_ts".to_string()),
    }
}

#[test]
fn relation_schema_fingerprint_is_stable_for_semantically_identical_values() {
    let first = orders_relation_schema();
    let second = orders_relation_schema();

    assert_eq!(
        SchemaFingerprintV1::for_relation_schema(&first)
            .unwrap()
            .as_str(),
        ORDERS_RELATION_SCHEMA_FINGERPRINT
    );
    assert_eq!(
        SchemaFingerprintV1::for_relation_schema(&first)
            .unwrap()
            .as_str(),
        SchemaFingerprintV1::for_relation_schema(&second)
            .unwrap()
            .as_str()
    );
}

#[test]
fn relation_schema_fingerprint_changes_when_contract_fields_change() {
    let baseline = SchemaFingerprintV1::for_relation_schema(&orders_relation_schema()).unwrap();

    let mut version_changed = orders_relation_schema();
    version_changed.relation_version = "2026-05-06.v1".to_string();
    assert_ne!(
        baseline,
        SchemaFingerprintV1::for_relation_schema(&version_changed).unwrap()
    );

    let mut logical_type_changed = orders_relation_schema();
    logical_type_changed.columns[1].logical_type = VelorixLogicalTypeV1::Float64;
    logical_type_changed.columns[1].physical_arrow_type = ArrowPhysicalTypeV1::Float64;
    assert_ne!(
        baseline,
        SchemaFingerprintV1::for_relation_schema(&logical_type_changed).unwrap()
    );

    let mut weight_column_changed = orders_relation_schema();
    weight_column_changed.weight_column_id = "amount".to_string();
    assert_ne!(
        baseline,
        SchemaFingerprintV1::for_relation_schema(&weight_column_changed).unwrap()
    );
}

#[test]
fn relation_schema_fingerprint_rejects_invalid_relation_identity() {
    let mut missing_relation_id = orders_relation_schema();
    missing_relation_id.relation_id = " ".to_string();

    let error = SchemaFingerprintV1::for_relation_schema(&missing_relation_id).unwrap_err();

    assert!(matches!(
        error,
        RelationSchemaError::MissingIdentityField {
            field: "relation_id"
        }
    ));
}

#[test]
fn relation_schema_validation_rejects_logical_physical_type_mismatch() {
    let mut schema = orders_relation_schema();
    schema.columns[1].logical_type = VelorixLogicalTypeV1::Float64;

    let error = schema.validate().unwrap_err();

    assert!(matches!(
        error,
        RelationSchemaError::InvalidRelationSchema {
            field: "logical_physical_type"
        }
    ));
}

#[test]
fn relation_schema_validation_rejects_decimal128_precision_outside_arrow_bound() {
    let mut schema = orders_relation_schema();
    schema.columns[1].logical_type = VelorixLogicalTypeV1::Decimal {
        precision: 39,
        scale: 2,
    };
    schema.columns[1].physical_arrow_type = ArrowPhysicalTypeV1::Decimal128 {
        precision: 39,
        scale: 2,
    };

    let error = schema.validate().unwrap_err();

    assert!(matches!(
        error,
        RelationSchemaError::InvalidRelationSchema { field: "decimal" }
    ));
}

#[test]
fn relation_catalog_validation_requires_cataloged_schema_fingerprint() {
    let schema = orders_relation_schema();
    let fingerprint = SchemaFingerprintV1::for_relation_schema(&schema).unwrap();
    let mut catalog = VelorixRelationCatalogV1 {
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema: schema,
        schema_fingerprint: SchemaFingerprintV1::new(format!("sha256:{}", "0".repeat(64))),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "orders".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "orders".to_string(),
            schema_fingerprint: fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: "incremental-adapter-orders-v1".to_string(),
        },
    };

    let error = catalog.validate().unwrap_err();

    assert!(matches!(
        error,
        RelationSchemaError::SchemaFingerprintMismatch { field: "catalog" }
    ));

    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.validate().unwrap();

    catalog.incremental_relation.relation_id = "customers".to_string();
    let error = catalog.validate().unwrap_err();
    assert!(matches!(
        error,
        RelationSchemaError::RelationIdentityMismatch {
            field: "incremental_relation.relation_id"
        }
    ));
}

#[test]
fn orders_sum_count_default_catalog_is_a_core_relation_contract() {
    let catalog = orders_sum_count_relation_catalog().unwrap();

    catalog.validate().unwrap();
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

#[test]
fn datafusion_schema_from_catalog_accepts_dictionary_utf8_primary_key_types() {
    for (key_type, expected_key_data_type) in [
        (DictionaryKeyTypeV1::Int8, DataType::Int8),
        (DictionaryKeyTypeV1::Int16, DataType::Int16),
        (DictionaryKeyTypeV1::Int32, DataType::Int32),
        (DictionaryKeyTypeV1::Int64, DataType::Int64),
    ] {
        let catalog = dictionary_customer_balance_relation_catalog(key_type);

        let schema = datafusion_schema_from_catalog(&catalog).unwrap();

        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Dictionary(Box::new(expected_key_data_type), Box::new(DataType::Utf8))
        );
    }
}

#[test]
fn datafusion_schema_from_catalog_accepts_boolean_primary_key() {
    let catalog = boolean_account_balance_relation_catalog();

    let schema = datafusion_schema_from_catalog(&catalog).unwrap();

    assert_eq!(schema.field(0).data_type(), &DataType::Boolean);
}

#[test]
fn datafusion_registration_rejects_batch_schema_that_differs_from_catalog() {
    let catalog = orders_relation_catalog();
    let wrong_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key_json", DataType::Utf8, false),
            Field::new("value_json", DataType::Utf8, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["\"order-a\""])) as ArrayRef,
            Arc::new(StringArray::from(vec!["42"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ],
    )
    .unwrap();

    let error = validate_record_batch_matches_catalog(&catalog, &wrong_batch).unwrap_err();

    assert!(matches!(
        error,
        RelationSchemaError::InvalidRelationSchema {
            field: "batch_schema"
        }
    ));
}

#[test]
fn datafusion_schema_from_catalog_rejects_stale_catalog_fingerprint() {
    let mut catalog = orders_relation_catalog();
    catalog.schema_fingerprint = SchemaFingerprintV1::new(format!("sha256:{}", "0".repeat(64)));

    let error = datafusion_schema_from_catalog(&catalog).unwrap_err();

    assert!(matches!(
        error,
        RelationSchemaError::SchemaFingerprintMismatch { field: "catalog" }
    ));
}

#[test]
fn relation_catalog_rejects_reserved_datafusion_registration_name() {
    for name in [
        "input",
        "information_schema",
        "datafusion",
        "orders.v1",
        "\"orders\"",
        "1orders",
    ] {
        let mut catalog = orders_relation_catalog();
        catalog.datafusion_registration.name = name.to_string();

        let error = catalog.validate().unwrap_err();

        assert!(
            matches!(
                error,
                RelationSchemaError::InvalidRelationSchema {
                    field: "datafusion_registration.name"
                }
            ),
            "accepted invalid DataFusion registration name: {name}"
        );
    }
}

#[test]
fn catalog_incremental_input_accepts_catalog_arrow_fixture() {
    let mut catalog = orders_relation_catalog();
    catalog.incremental_adapter.adapter_id = ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string();
    let batch = orders_input_batch(&["order-a", "order-b"], &[42, 7], &[1, -1]);

    let delta = arrow_record_batches_to_orders_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("order-a")),
                DeltaValue::from_json(serde_json::json!(42)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("order-b")),
                DeltaValue::from_json(serde_json::json!(7)),
                -1,
            ),
        ]
    );
}

#[test]
fn generic_catalog_incremental_input_uses_catalog_roles_for_non_orders_columns() {
    let catalog = customer_balance_relation_catalog();
    let batch = customer_balance_input_batch(&["customer-a", "customer-b"], &[500, 125], &[1, -1]);

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("customer-a")),
                DeltaValue::from_json(serde_json::json!(500)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("customer-b")),
                DeltaValue::from_json(serde_json::json!(125)),
                -1,
            ),
        ]
    );
}

#[test]
fn key_value_incremental_input_uses_explicit_value_column_without_value_semantic_role() {
    let mut catalog = customer_balance_relation_catalog();
    for column in &mut catalog.relation_schema.columns {
        if column.column_id == "customer_key" || column.column_id == "balance_cents" {
            column.semantic_role = RelationSemanticRoleV1::Metadata;
        }
    }
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    let batch = customer_balance_input_batch(&["customer-a", "customer-b"], &[500, 125], &[1, -1]);

    let delta = arrow_record_batches_to_key_value_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &["customer_key".to_string()],
        "balance_cents",
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("customer-a")),
                DeltaValue::from_json(serde_json::json!(500)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("customer-b")),
                DeltaValue::from_json(serde_json::json!(125)),
                -1,
            ),
        ]
    );
}

#[test]
fn generic_catalog_incremental_input_accepts_int64_primary_key() {
    let catalog = account_balance_relation_catalog();
    let batch = account_balance_input_batch(&[1001, 1002], &[500, 125], &[1, -1]);

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(1001)),
                DeltaValue::from_json(serde_json::json!(500)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(1002)),
                DeltaValue::from_json(serde_json::json!(125)),
                -1,
            ),
        ]
    );
}

#[test]
fn generic_catalog_incremental_input_accepts_float64_primary_key() {
    let catalog = float_key_account_balance_relation_catalog();
    let batch = float_key_account_balance_input_batch(&[1001.5, 1002.25], &[500, 125], &[1, -1]);

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        "account_balances",
        "2026-05-13.v1",
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(1001.5)),
                DeltaValue::from_json(serde_json::json!(500)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(1002.25)),
                DeltaValue::from_json(serde_json::json!(125)),
                -1,
            ),
        ]
    );
}

#[test]
fn generic_catalog_incremental_input_rejects_non_finite_float64_primary_key() {
    let catalog = float_key_account_balance_relation_catalog();
    let batch = float_key_account_balance_input_batch(&[f64::INFINITY], &[500], &[1]);

    let error = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        "account_balances",
        "2026-05-13.v1",
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap_err();

    assert!(
        error.to_string().contains("finite"),
        "unexpected error: {error}"
    );
}

#[test]
fn generic_catalog_incremental_input_normalizes_negative_zero_float64_primary_key() {
    let catalog = float_key_account_balance_relation_catalog();
    let batch = float_key_account_balance_input_batch(&[-0.0, 0.0], &[500, 500], &[1, -1]);

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        "account_balances",
        "2026-05-13.v1",
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(delta.records()[0].key, delta.records()[1].key);
    assert!(delta.net_rows().unwrap().is_empty());
}

#[test]
fn generic_catalog_incremental_input_accepts_float64_value_column() {
    let catalog = float_account_balance_relation_catalog();
    let batch = float_account_balance_input_batch(&[1001, 1002], &[50.25, -12.5], &[1, -1]);

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(1001)),
                DeltaValue::from_json(serde_json::json!(50.25)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(1002)),
                DeltaValue::from_json(serde_json::json!(-12.5)),
                -1,
            ),
        ]
    );
}

#[test]
fn generic_catalog_incremental_input_rejects_non_finite_float64_value_column() {
    let catalog = float_account_balance_relation_catalog();
    let batch = float_account_balance_input_batch(&[1001], &[f64::NAN], &[1]);

    let error = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        "account_balances",
        "2026-05-13.v1",
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap_err();

    assert!(
        error.to_string().contains("finite"),
        "unexpected error: {error}"
    );
}

#[test]
fn generic_catalog_incremental_input_accepts_date32_primary_key() {
    let catalog = daily_balance_relation_catalog();
    let batch = daily_balance_input_batch(&[20_586, 20_587], &[500, 125], &[1, -1]);

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(20_586)),
                DeltaValue::from_json(serde_json::json!(500)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(20_587)),
                DeltaValue::from_json(serde_json::json!(125)),
                -1,
            ),
        ]
    );
}

#[test]
fn generic_catalog_incremental_input_accepts_timestamp_nanosecond_primary_key() {
    let catalog = timestamped_balance_relation_catalog();
    let batch = timestamped_balance_input_batch(
        &[1_769_289_600_000_000_000, 1_769_293_200_000_000_000],
        &[500, 125],
        &[1, -1],
    );

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(1_769_289_600_000_000_000_i64)),
                DeltaValue::from_json(serde_json::json!(500)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(1_769_293_200_000_000_000_i64)),
                DeltaValue::from_json(serde_json::json!(125)),
                -1,
            ),
        ]
    );
}

#[test]
fn generic_catalog_incremental_input_accepts_dictionary_utf8_primary_key() {
    for key_type in [
        DictionaryKeyTypeV1::Int8,
        DictionaryKeyTypeV1::Int16,
        DictionaryKeyTypeV1::Int32,
        DictionaryKeyTypeV1::Int64,
    ] {
        let catalog = dictionary_customer_balance_relation_catalog(key_type.clone());
        let batch = dictionary_customer_balance_input_batch(
            &key_type,
            &["customer-a", "customer-b"],
            &[500, 125],
            &[1, -1],
        );

        let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
            &catalog,
            catalog.relation_schema.relation_id.as_str(),
            catalog.relation_schema.relation_version.as_str(),
            catalog.schema_fingerprint.as_str(),
            &[batch],
        )
        .unwrap();

        assert_eq!(
            delta.records(),
            &[
                DeltaRecord::new(
                    DeltaKey::from_json(serde_json::json!("customer-a")),
                    DeltaValue::from_json(serde_json::json!(500)),
                    1,
                ),
                DeltaRecord::new(
                    DeltaKey::from_json(serde_json::json!("customer-b")),
                    DeltaValue::from_json(serde_json::json!(125)),
                    -1,
                ),
            ]
        );
    }
}

#[test]
fn generic_catalog_incremental_input_accepts_boolean_primary_key() {
    let catalog = boolean_account_balance_relation_catalog();
    let batch = boolean_account_balance_input_batch(&[true, false], &[500, 125], &[1, -1]);

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(true)),
                DeltaValue::from_json(serde_json::json!(500)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(false)),
                DeltaValue::from_json(serde_json::json!(125)),
                -1,
            ),
        ]
    );
}

#[test]
fn generic_catalog_incremental_input_accepts_json_utf8_primary_key() {
    let catalog = json_account_balance_relation_catalog();
    let batch = json_account_balance_input_batch(
        &[
            r#"{"tenant":"a","account":1001}"#,
            r#"{"tenant":"b","account":1002}"#,
        ],
        &[500, 125],
        &[1, -1],
    );

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!({"tenant": "a", "account": 1001})),
                DeltaValue::from_json(serde_json::json!(500)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!({"tenant": "b", "account": 1002})),
                DeltaValue::from_json(serde_json::json!(125)),
                -1,
            ),
        ]
    );
}

#[test]
fn generic_catalog_incremental_input_rejects_invalid_json_utf8_primary_key() {
    let catalog = json_account_balance_relation_catalog();
    let batch = json_account_balance_input_batch(&[r#"{"tenant":"a""#], &[500], &[1]);

    let error = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        IncrementalInputAdapterError::MalformedArrowInput { reason }
            if reason.starts_with("JsonUtf8 key column contains invalid JSON")
    ));
}

#[test]
fn generic_catalog_incremental_input_rejects_dictionary_utf8_null_value() {
    let catalog = dictionary_customer_balance_relation_catalog(DictionaryKeyTypeV1::Int8);
    let key_values = StringArray::from(vec![Some("customer-a"), None]);
    let customer_key_array = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0_i8, 1_i8]),
        Arc::new(key_values),
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
            Field::new(
                "customer_key",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
            ),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![500, 125])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, -1])) as ArrayRef,
            Arc::new(customer_key_array) as ArrayRef,
        ],
    )
    .unwrap();

    let error = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        IncrementalInputAdapterError::MalformedArrowInput { reason }
            if reason == "prototype ingest columns must be non-null"
    ));
}

#[test]
fn generic_catalog_incremental_input_rejects_dictionary_utf8_null_key() {
    let mut catalog = dictionary_customer_balance_relation_catalog(DictionaryKeyTypeV1::Int8);
    catalog.relation_schema.columns[2].nullable = true;
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    let mut key_builder = StringDictionaryBuilder::<Int8Type>::new();
    key_builder.append("customer-a").unwrap();
    key_builder.append_null();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
            Field::new(
                "customer_key",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![500, 125])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, -1])) as ArrayRef,
            Arc::new(key_builder.finish()) as ArrayRef,
        ],
    )
    .unwrap();

    let error = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        IncrementalInputAdapterError::MalformedArrowInput { reason }
            if reason == "prototype ingest columns must be non-null"
    ));
}

#[test]
fn generic_catalog_incremental_input_accepts_decimal128_primary_key_as_canonical_string() {
    let catalog = decimal_key_account_balance_relation_catalog();
    let batch = decimal_key_account_balance_input_batch(
        &[
            123_456_789_012_345_678_901_234_567_890_123_456_i128,
            -100_i128,
            0_i128,
            12_i128,
            -12_i128,
            123_400_i128,
        ],
        &[500, 125, 0, 12, -12, 123],
        &[1, -1, 1, 1, 1, 1],
    );

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("1234567890123456789012345678901234.56")),
                DeltaValue::from_json(serde_json::json!(500)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("-1.00")),
                DeltaValue::from_json(serde_json::json!(125)),
                -1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("0.00")),
                DeltaValue::from_json(serde_json::json!(0)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("0.12")),
                DeltaValue::from_json(serde_json::json!(12)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("-0.12")),
                DeltaValue::from_json(serde_json::json!(-12)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("1234.00")),
                DeltaValue::from_json(serde_json::json!(123)),
                1,
            ),
        ]
    );
}

#[test]
fn generic_catalog_incremental_input_preserves_decimal128_scale_zero_as_string_integer() {
    let mut catalog = decimal_key_account_balance_relation_catalog();
    catalog.relation_schema.columns[0].logical_type = VelorixLogicalTypeV1::Decimal {
        precision: 38,
        scale: 0,
    };
    catalog.relation_schema.columns[0].physical_arrow_type = ArrowPhysicalTypeV1::Decimal128 {
        precision: 38,
        scale: 0,
    };
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Decimal128(38, 0), false),
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(
                Decimal128Array::from(vec![123_i128, -123_i128])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(Int64Array::from(vec![500, 125])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, -1])) as ArrayRef,
        ],
    )
    .unwrap();

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("123")),
                DeltaValue::from_json(serde_json::json!(500)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!("-123")),
                DeltaValue::from_json(serde_json::json!(125)),
                -1,
            ),
        ]
    );
}

#[test]
fn generic_catalog_incremental_input_accepts_decimal128_value_column_as_canonical_string() {
    let catalog = decimal_value_account_balance_relation_catalog();
    let batch = decimal_value_account_balance_input_batch(
        &[1001, 1002],
        &[
            123_456_789_012_345_678_901_234_567_890_123_456_i128,
            -100_i128,
        ],
        &[1, -1],
    );

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(1001)),
                DeltaValue::from_json(serde_json::json!("1234567890123456789012345678901234.56")),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!(1002)),
                DeltaValue::from_json(serde_json::json!("-1.00")),
                -1,
            ),
        ]
    );
}

#[test]
fn row_key_catalog_incremental_input_accepts_multi_column_key_as_column_id_object() {
    let catalog = account_balance_by_currency_relation_catalog();
    let batch = account_balance_by_currency_input_batch(
        &[1001, 1001],
        &["USD", "EUR"],
        &[500, 125],
        &[1, -1],
    );

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!({
                    "account_id": 1001,
                    "currency": "USD"
                })),
                DeltaValue::from_json(serde_json::json!(500)),
                1,
            ),
            DeltaRecord::new(
                DeltaKey::from_json(serde_json::json!({
                    "account_id": 1001,
                    "currency": "EUR"
                })),
                DeltaValue::from_json(serde_json::json!(125)),
                -1,
            ),
        ]
    );
}

#[test]
fn row_key_catalog_incremental_input_accepts_expanded_scalar_primary_key_types() {
    let catalog = expanded_scalar_key_relation_catalog();
    let batch = expanded_scalar_key_input_batch();

    let delta = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap();

    assert_eq!(
        delta.records(),
        &[DeltaRecord::new(
            DeltaKey::from_json(serde_json::json!({
                "i8_key": -8,
                "i16_key": -32000,
                "i32_key": -123456,
                "u64_key": 9_000_000_000_u64,
                "binary_key": "0x0a0bff",
                "time_key": 3_723_004_005_006_i64,
                "char_key": "ABCD"
            })),
            DeltaValue::from_json(serde_json::json!(42)),
            1,
        )]
    );
}

#[test]
fn incremental_input_rejects_nested_primary_key_types_until_key_serialization_exists() {
    let catalog = nested_key_relation_catalog();
    let batch = nested_key_input_batch();

    let error = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        IncrementalInputAdapterError::MalformedArrowInput { reason }
            if reason == "`scores` column uses a nested Arrow type that is not supported as an incremental key"
    ));
}

#[test]
fn single_key_catalog_incremental_input_still_rejects_multi_column_key() {
    let mut catalog = account_balance_by_currency_relation_catalog();
    catalog.incremental_adapter.adapter_id =
        CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string();
    let batch = account_balance_by_currency_input_batch(&[1001], &["USD"], &[500], &[1]);

    let error = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        IncrementalInputAdapterError::MalformedArrowInput { reason }
            if reason == "prototype adapter supports exactly one primary key column"
    ));
}

#[test]
fn generic_catalog_incremental_input_rejects_decimal128_precision_scale_mismatch() {
    let catalog = decimal_key_account_balance_relation_catalog();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Decimal128(38, 3), false),
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(
                Decimal128Array::from(vec![100100])
                    .with_precision_and_scale(38, 3)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(Int64Array::from(vec![500])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ],
    )
    .unwrap();

    let error = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        IncrementalInputAdapterError::MalformedArrowInput { reason }
            if reason == "relation batch schema does not match catalog"
    ));
}

#[test]
fn catalog_incremental_input_rejects_unsupported_adapter() {
    let catalog = orders_relation_catalog();
    let batch = orders_input_batch(&["order-a"], &[42], &[1]);

    let error = arrow_record_batches_to_orders_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        IncrementalInputAdapterError::UnsupportedIncrementalAdapter { .. }
    ));
}

#[test]
fn generic_relation_admission_accepts_multi_payload_columns_without_value_role() {
    let catalog = generic_activity_relation_catalog();

    let spec = catalog.validate_ingest_adapter_scope().unwrap();

    assert_eq!(
        spec,
        velorix_core::relation::SupportedIncrementalAdapterSpec::Generic
    );
}

#[test]
fn generic_relation_stays_out_of_legacy_incremental_delta_adapter() {
    let catalog = generic_activity_relation_catalog();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("user_id", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
            Field::new("delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["e1"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["u1"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![7])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ],
    )
    .unwrap();

    let error = arrow_record_batches_to_single_key_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        IncrementalInputAdapterError::UnsupportedIncrementalAdapter { adapter_id }
            if adapter_id == CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID
    ));
}

#[test]
fn catalog_incremental_input_rejects_catalog_identity_mismatch() {
    let mut catalog = orders_relation_catalog();
    catalog.incremental_adapter.adapter_id = ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string();
    let batch = orders_input_batch(&["order-a"], &[42], &[1]);

    let error = arrow_record_batches_to_orders_sum_count_delta_batch(
        &catalog,
        "customers",
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        IncrementalInputAdapterError::IngestRelationMismatch {
            field: "relation_id",
            ..
        }
    ));
}

#[test]
fn catalog_incremental_input_rejects_multiple_value_columns() {
    let mut catalog = customer_balance_relation_catalog();
    catalog.relation_schema.columns.push(RelationColumnV1 {
        column_id: "pending_cents".to_string(),
        name: "pending_cents".to_string(),
        logical_type: VelorixLogicalTypeV1::Int64,
        physical_arrow_type: ArrowPhysicalTypeV1::Int64,
        nullable: false,
        ordinal: 3,
        semantic_role: RelationSemanticRoleV1::Value,
    });
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("customer_key", DataType::Utf8, false),
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
            Field::new("pending_cents", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["customer-a"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![500])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![25])) as ArrayRef,
        ],
    )
    .unwrap();

    let error = arrow_record_batches_to_orders_sum_count_delta_batch(
        &catalog,
        catalog.relation_schema.relation_id.as_str(),
        catalog.relation_schema.relation_version.as_str(),
        catalog.schema_fingerprint.as_str(),
        &[batch],
    )
    .unwrap_err();

    assert!(matches!(
        error,
        IncrementalInputAdapterError::MalformedArrowInput { reason }
            if reason == "prototype adapter supports exactly one value column"
    ));
}

fn orders_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "orders".to_string(),
        relation_name: "orders".to_string(),
        relation_version: "2026-05-05.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "order_id".to_string(),
                name: "order_id".to_string(),
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
        primary_key_column_ids: vec!["order_id".to_string()],
        weight_column_id: "weight".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "orders".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "orders".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: "incremental-adapter-orders-v1".to_string(),
        },
    }
}

fn account_balance_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "account_balances".to_string(),
        relation_name: "account_balances".to_string(),
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
                column_id: "balance_cents".to_string(),
                name: "balance_cents".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "row_delta".to_string(),
                name: "row_delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["account_id".to_string()],
        weight_column_id: "row_delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "account_balances".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "account_balances".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn generic_activity_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "activity_events".to_string(),
        relation_name: "activity_events".to_string(),
        relation_version: "2026-06-11.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "event_id".to_string(),
                name: "event_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "user_id".to_string(),
                name: "user_id".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Metadata,
            },
            RelationColumnV1 {
                column_id: "score".to_string(),
                name: "score".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Metadata,
            },
            RelationColumnV1 {
                column_id: "delta".to_string(),
                name: "delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 3,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["event_id".to_string()],
        weight_column_id: "delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "activity_events".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "activity_events".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn account_balance_by_currency_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "account_balances_by_currency".to_string(),
        relation_name: "account_balances_by_currency".to_string(),
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
                column_id: "currency".to_string(),
                name: "currency_code".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
            RelationColumnV1 {
                column_id: "balance_cents".to_string(),
                name: "balance_cents".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "row_delta".to_string(),
                name: "row_delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 3,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["account_id".to_string(), "currency".to_string()],
        weight_column_id: "row_delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "account_balances_by_currency".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "account_balances_by_currency".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_ROW_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn expanded_scalar_key_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "expanded_scalar_keys".to_string(),
        relation_name: "expanded_scalar_keys".to_string(),
        relation_version: "2026-06-09.v1".to_string(),
        columns: vec![
            relation_column(
                "i8_key",
                VelorixLogicalTypeV1::Int8,
                ArrowPhysicalTypeV1::Int8,
                RelationSemanticRoleV1::PrimaryKey,
                0,
            ),
            relation_column(
                "i16_key",
                VelorixLogicalTypeV1::Int16,
                ArrowPhysicalTypeV1::Int16,
                RelationSemanticRoleV1::PrimaryKey,
                1,
            ),
            relation_column(
                "i32_key",
                VelorixLogicalTypeV1::Int32,
                ArrowPhysicalTypeV1::Int32,
                RelationSemanticRoleV1::PrimaryKey,
                2,
            ),
            relation_column(
                "u64_key",
                VelorixLogicalTypeV1::UInt64,
                ArrowPhysicalTypeV1::UInt64,
                RelationSemanticRoleV1::PrimaryKey,
                3,
            ),
            relation_column(
                "binary_key",
                VelorixLogicalTypeV1::Varbinary,
                ArrowPhysicalTypeV1::Binary,
                RelationSemanticRoleV1::PrimaryKey,
                4,
            ),
            relation_column(
                "time_key",
                VelorixLogicalTypeV1::Time,
                ArrowPhysicalTypeV1::Time64Nanosecond,
                RelationSemanticRoleV1::PrimaryKey,
                5,
            ),
            relation_column(
                "char_key",
                VelorixLogicalTypeV1::Char { length: Some(4) },
                ArrowPhysicalTypeV1::Utf8,
                RelationSemanticRoleV1::PrimaryKey,
                6,
            ),
            relation_column(
                "amount",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                RelationSemanticRoleV1::Value,
                7,
            ),
            relation_column(
                "row_delta",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                RelationSemanticRoleV1::Weight,
                8,
            ),
        ],
        primary_key_column_ids: vec![
            "i8_key".to_string(),
            "i16_key".to_string(),
            "i32_key".to_string(),
            "u64_key".to_string(),
            "binary_key".to_string(),
            "time_key".to_string(),
            "char_key".to_string(),
        ],
        weight_column_id: "row_delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "expanded_scalar_keys".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "expanded_scalar_keys".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_ROW_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn expanded_scalar_key_input_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("i8_key", DataType::Int8, false),
            Field::new("i16_key", DataType::Int16, false),
            Field::new("i32_key", DataType::Int32, false),
            Field::new("u64_key", DataType::UInt64, false),
            Field::new("binary_key", DataType::Binary, false),
            Field::new("time_key", DataType::Time64(TimeUnit::Nanosecond), false),
            Field::new("char_key", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int8Array::from(vec![-8])) as ArrayRef,
            Arc::new(Int16Array::from(vec![-32000])) as ArrayRef,
            Arc::new(Int32Array::from(vec![-123456])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![9_000_000_000_u64])) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values([&[0x0a, 0x0b, 0xff][..]])) as ArrayRef,
            Arc::new(Time64NanosecondArray::from(vec![3_723_004_005_006_i64])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ABCD"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![42])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn nested_key_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "nested_key_scores".to_string(),
        relation_name: "nested_key_scores".to_string(),
        relation_version: "2026-06-10.v1".to_string(),
        columns: vec![
            relation_column(
                "scores",
                VelorixLogicalTypeV1::Array {
                    element_type: Box::new(VelorixLogicalTypeV1::Int64),
                },
                ArrowPhysicalTypeV1::List {
                    element_type: Box::new(ArrowPhysicalTypeV1::Int64),
                },
                RelationSemanticRoleV1::PrimaryKey,
                0,
            ),
            relation_column(
                "amount",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                RelationSemanticRoleV1::Value,
                1,
            ),
            relation_column(
                "row_delta",
                VelorixLogicalTypeV1::Int64,
                ArrowPhysicalTypeV1::Int64,
                RelationSemanticRoleV1::Weight,
                2,
            ),
        ],
        primary_key_column_ids: vec!["scores".to_string()],
        weight_column_id: "row_delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "nested_key_scores".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "nested_key_scores".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn nested_key_input_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "scores",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
            Field::new("amount", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(10), Some(20)]),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![42])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn relation_column(
    name: &str,
    logical_type: VelorixLogicalTypeV1,
    physical_arrow_type: ArrowPhysicalTypeV1,
    semantic_role: RelationSemanticRoleV1,
    ordinal: u32,
) -> RelationColumnV1 {
    RelationColumnV1 {
        column_id: name.to_string(),
        name: name.to_string(),
        logical_type,
        physical_arrow_type,
        nullable: false,
        ordinal,
        semantic_role,
    }
}

fn boolean_account_balance_relation_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = account_balance_relation_catalog();
    catalog.relation_schema.columns[0].logical_type = VelorixLogicalTypeV1::Bool;
    catalog.relation_schema.columns[0].physical_arrow_type = ArrowPhysicalTypeV1::Boolean;
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    catalog
}

fn float_account_balance_relation_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = account_balance_relation_catalog();
    catalog.relation_schema.columns[1].logical_type = VelorixLogicalTypeV1::Float64;
    catalog.relation_schema.columns[1].physical_arrow_type = ArrowPhysicalTypeV1::Float64;
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    catalog
}

fn float_key_account_balance_relation_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = account_balance_relation_catalog();
    catalog.relation_schema.columns[0].logical_type = VelorixLogicalTypeV1::Float64;
    catalog.relation_schema.columns[0].physical_arrow_type = ArrowPhysicalTypeV1::Float64;
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    catalog
}

fn decimal_key_account_balance_relation_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = account_balance_relation_catalog();
    catalog.relation_schema.columns[0].logical_type = VelorixLogicalTypeV1::Decimal {
        precision: 38,
        scale: 2,
    };
    catalog.relation_schema.columns[0].physical_arrow_type = ArrowPhysicalTypeV1::Decimal128 {
        precision: 38,
        scale: 2,
    };
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    catalog
}

fn decimal_value_account_balance_relation_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = account_balance_relation_catalog();
    catalog.relation_schema.columns[1].logical_type = VelorixLogicalTypeV1::Decimal {
        precision: 38,
        scale: 2,
    };
    catalog.relation_schema.columns[1].physical_arrow_type = ArrowPhysicalTypeV1::Decimal128 {
        precision: 38,
        scale: 2,
    };
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    catalog
}

fn json_account_balance_relation_catalog() -> VelorixRelationCatalogV1 {
    let mut catalog = account_balance_relation_catalog();
    catalog.relation_schema.columns[0].logical_type = VelorixLogicalTypeV1::Json;
    catalog.relation_schema.columns[0].physical_arrow_type = ArrowPhysicalTypeV1::JsonUtf8;
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    catalog
}

fn daily_balance_relation_catalog() -> VelorixRelationCatalogV1 {
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
                column_id: "balance_cents".to_string(),
                name: "balance_cents".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "row_delta".to_string(),
                name: "row_delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["business_date".to_string()],
        weight_column_id: "row_delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "daily_balances".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "daily_balances".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn timestamped_balance_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "timestamped_balances".to_string(),
        relation_name: "timestamped_balances".to_string(),
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
                column_id: "balance_cents".to_string(),
                name: "balance_cents".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "row_delta".to_string(),
                name: "row_delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
        ],
        primary_key_column_ids: vec!["observed_at".to_string()],
        weight_column_id: "row_delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "timestamped_balances".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "timestamped_balances".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn customer_balance_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "customer_balances".to_string(),
        relation_name: "customer_balances".to_string(),
        relation_version: "2026-05-13.v1".to_string(),
        columns: vec![
            RelationColumnV1 {
                column_id: "balance_cents".to_string(),
                name: "balance_cents".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 0,
                semantic_role: RelationSemanticRoleV1::Value,
            },
            RelationColumnV1 {
                column_id: "row_delta".to_string(),
                name: "row_delta".to_string(),
                logical_type: VelorixLogicalTypeV1::Int64,
                physical_arrow_type: ArrowPhysicalTypeV1::Int64,
                nullable: false,
                ordinal: 1,
                semantic_role: RelationSemanticRoleV1::Weight,
            },
            RelationColumnV1 {
                column_id: "customer_key".to_string(),
                name: "customer_key".to_string(),
                logical_type: VelorixLogicalTypeV1::Utf8,
                physical_arrow_type: ArrowPhysicalTypeV1::Utf8,
                nullable: false,
                ordinal: 2,
                semantic_role: RelationSemanticRoleV1::PrimaryKey,
            },
        ],
        primary_key_column_ids: vec!["customer_key".to_string()],
        weight_column_id: "row_delta".to_string(),
        allowed_operations: vec![RelationOperationV1::Insert, RelationOperationV1::Delete],
        event_time_column_id: None,
    };
    let schema_fingerprint = SchemaFingerprintV1::for_relation_schema(&relation_schema).unwrap();

    VelorixRelationCatalogV1 {
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "customer_balances".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "customer_balances".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

fn dictionary_customer_balance_relation_catalog(
    key_type: DictionaryKeyTypeV1,
) -> VelorixRelationCatalogV1 {
    let mut catalog = customer_balance_relation_catalog();
    catalog.relation_schema.columns[2].physical_arrow_type = ArrowPhysicalTypeV1::DictionaryUtf8 {
        key_type,
        ordered: false,
    };
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.incremental_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    catalog
}

fn orders_input_batch(order_ids: &[&str], amounts: &[i64], weights: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(order_ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(amounts.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(weights.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn account_balance_input_batch(
    account_ids: &[i64],
    balance_cents: &[i64],
    row_deltas: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Int64, false),
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(account_ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(balance_cents.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(row_deltas.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn account_balance_by_currency_input_batch(
    account_ids: &[i64],
    currencies: &[&str],
    balance_cents: &[i64],
    row_deltas: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Int64, false),
            Field::new("currency_code", DataType::Utf8, false),
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(account_ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(currencies.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(balance_cents.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(row_deltas.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn float_account_balance_input_batch(
    account_ids: &[i64],
    balances: &[f64],
    row_deltas: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Int64, false),
            Field::new("balance_cents", DataType::Float64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(account_ids.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(balances.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(row_deltas.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn float_key_account_balance_input_batch(
    account_ids: &[f64],
    balance_cents: &[i64],
    row_deltas: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Float64, false),
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Float64Array::from(account_ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(balance_cents.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(row_deltas.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn decimal_key_account_balance_input_batch(
    account_ids: &[i128],
    balance_cents: &[i64],
    row_deltas: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Decimal128(38, 2), false),
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(
                Decimal128Array::from(account_ids.to_vec())
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(Int64Array::from(balance_cents.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(row_deltas.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn decimal_value_account_balance_input_batch(
    account_ids: &[i64],
    balances: &[i128],
    row_deltas: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Int64, false),
            Field::new("balance_cents", DataType::Decimal128(38, 2), false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(account_ids.to_vec())) as ArrayRef,
            Arc::new(
                Decimal128Array::from(balances.to_vec())
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(Int64Array::from(row_deltas.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn boolean_account_balance_input_batch(
    account_ids: &[bool],
    balance_cents: &[i64],
    row_deltas: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Boolean, false),
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(BooleanArray::from(account_ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(balance_cents.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(row_deltas.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn json_account_balance_input_batch(
    account_ids: &[&str],
    balance_cents: &[i64],
    row_deltas: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Utf8, false),
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(account_ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(balance_cents.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(row_deltas.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn daily_balance_input_batch(
    business_dates: &[i32],
    balance_cents: &[i64],
    row_deltas: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("business_date", DataType::Date32, false),
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Date32Array::from(business_dates.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(balance_cents.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(row_deltas.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn timestamped_balance_input_batch(
    observed_at: &[i64],
    balance_cents: &[i64],
    row_deltas: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "observed_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(TimestampNanosecondArray::from(observed_at.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(balance_cents.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(row_deltas.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn customer_balance_input_batch(
    customer_keys: &[&str],
    balance_cents: &[i64],
    row_deltas: &[i64],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
            Field::new("customer_key", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(balance_cents.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(row_deltas.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(customer_keys.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn dictionary_customer_balance_input_batch(
    key_type: &DictionaryKeyTypeV1,
    customer_keys: &[&str],
    balance_cents: &[i64],
    row_deltas: &[i64],
) -> RecordBatch {
    macro_rules! dictionary_array {
        ($arrow_key_type:ty) => {{
            let mut builder = StringDictionaryBuilder::<$arrow_key_type>::new();
            for key in customer_keys {
                builder.append(key).unwrap();
            }
            Arc::new(builder.finish()) as ArrayRef
        }};
    }

    let (key_data_type, customer_key_array) = match key_type {
        DictionaryKeyTypeV1::Int8 => (DataType::Int8, dictionary_array!(Int8Type)),
        DictionaryKeyTypeV1::Int16 => (DataType::Int16, dictionary_array!(Int16Type)),
        DictionaryKeyTypeV1::Int32 => (DataType::Int32, dictionary_array!(Int32Type)),
        DictionaryKeyTypeV1::Int64 => (DataType::Int64, dictionary_array!(Int64Type)),
    };

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
            Field::new(
                "customer_key",
                DataType::Dictionary(Box::new(key_data_type), Box::new(DataType::Utf8)),
                false,
            ),
        ])),
        vec![
            Arc::new(Int64Array::from(balance_cents.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(row_deltas.to_vec())) as ArrayRef,
            customer_key_array,
        ],
    )
    .unwrap()
}
