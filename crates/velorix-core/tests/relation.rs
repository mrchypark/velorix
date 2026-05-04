use velorix_core::relation::{
    ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
    FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1, RelationOperationV1,
    RelationSchemaError, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
    VelorixRelationCatalogV1, VelorixRelationSchemaV1, RELATION_SCHEMA_VERSION_V1,
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
