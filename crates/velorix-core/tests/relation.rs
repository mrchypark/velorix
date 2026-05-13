use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanArray, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::prelude::SessionContext;
use velorix_core::delta::{DeltaKey, DeltaRecord, DeltaValue};
use velorix_core::relation::{
    arrow_record_batches_to_orders_sum_count_delta_batch,
    arrow_record_batches_to_single_key_sum_count_delta_batch, datafusion_schema_from_catalog,
    register_datafusion_catalog_batches, validate_record_batch_matches_catalog,
    ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
    FelderaRelationBindingV1, IncrementalAdapterBindingV1, IncrementalInputAdapterError,
    RelationColumnV1, RelationOperationV1, RelationSchemaError, RelationSemanticRoleV1,
    SchemaFingerprintV1, VelorixLogicalTypeV1, VelorixRelationCatalogV1, VelorixRelationSchemaV1,
    CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID, ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID,
    RELATION_SCHEMA_VERSION_V1,
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
fn relation_catalog_validation_requires_cataloged_schema_fingerprint() {
    let schema = orders_relation_schema();
    let fingerprint = SchemaFingerprintV1::for_relation_schema(&schema).unwrap();
    let mut catalog = VelorixRelationCatalogV1 {
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema: schema,
        schema_fingerprint: SchemaFingerprintV1::new(format!("sha256:{}", "0".repeat(64))),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "orders".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
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

    catalog.feldera_relation.relation_id = "customers".to_string();
    let error = catalog.validate().unwrap_err();
    assert!(matches!(
        error,
        RelationSchemaError::RelationIdentityMismatch {
            field: "feldera_relation.relation_id"
        }
    ));
}

#[tokio::test]
async fn datafusion_registration_from_catalog_exposes_typed_columns() {
    let catalog = orders_relation_catalog();
    let batch = orders_input_batch(&["order-a"], &[42], &[1]);
    let context = SessionContext::new();

    register_datafusion_catalog_batches(&context, &catalog, vec![batch]).unwrap();

    let output = context
        .sql("select order_id, amount, weight from orders")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        output[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        vec!["order_id", "amount", "weight"]
    );
    assert_eq!(output[0].schema().field(1).data_type(), &DataType::Int64);

    let error = context
        .sql("select key_json, value_json from orders")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("key_json"));
}

#[test]
fn datafusion_registration_rejects_unsupported_view_mode() {
    let mut catalog = orders_relation_catalog();
    catalog.datafusion_registration.mode = DataFusionRegistrationModeV1::View;
    let batch = orders_input_batch(&["order-a"], &[42], &[1]);
    let context = SessionContext::new();

    let error = register_datafusion_catalog_batches(&context, &catalog, vec![batch]).unwrap_err();

    assert!(
        error.to_string().contains("datafusion_registration.mode"),
        "unexpected error: {error}"
    );
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
fn generic_catalog_incremental_input_rejects_unsupported_primary_key_type() {
    let mut catalog = account_balance_relation_catalog();
    catalog.relation_schema.columns[0].logical_type = VelorixLogicalTypeV1::Bool;
    catalog.relation_schema.columns[0].physical_arrow_type = ArrowPhysicalTypeV1::Boolean;
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.feldera_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("account_id", DataType::Boolean, false),
            Field::new("balance_cents", DataType::Int64, false),
            Field::new("row_delta", DataType::Int64, false),
        ])),
        vec![
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
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
            if reason == "prototype adapter key column `account_id` must be Utf8 or Int64"
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
    catalog.feldera_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
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
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "orders".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
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
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "account_balances".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "account_balances".to_string(),
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
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "customer_balances".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "customer_balances".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_SINGLE_KEY_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
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
