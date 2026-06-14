use std::sync::Arc;

use object_store::{local::LocalFileSystem, ObjectStore};
use tempfile::TempDir;
use velorix_core::relation::{
    ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
    FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1, RelationOperationV1,
    RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1, VelorixRelationCatalogV1,
    VelorixRelationSchemaV1, CATALOG_FELDERA_GENERIC_INCREMENTAL_ADAPTER_ID,
    ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID, RELATION_SCHEMA_VERSION_V1,
};
use velorix_storage::{
    capability::{ObjectStoreCapabilityProfile, RequiredObjectStoreCapability},
    relation_catalog_registry::{
        CreateRelationCatalogOutcome, RelationCatalogRegistry, RelationCatalogRegistryError,
    },
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

fn weak_profile() -> ObjectStoreCapabilityProfile {
    ObjectStoreCapabilityProfile {
        backend_name: "weak-relation-catalog-store".to_string(),
        conditional_create: false,
        conditional_update: true,
        atomic_visibility: true,
        list_after_write: true,
        read_after_write: true,
    }
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
            adapter_id: ORDERS_SUM_COUNT_INCREMENTAL_ADAPTER_ID.to_string(),
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
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "activity_events".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "activity_events".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: CATALOG_FELDERA_GENERIC_INCREMENTAL_ADAPTER_ID.to_string(),
        },
    }
}

#[tokio::test]
async fn relation_catalog_registry_creates_and_reads_valid_catalog_record() {
    let (_temp_dir, store) = temp_store();
    let registry = RelationCatalogRegistry::new(store);
    let catalog = orders_relation_catalog();

    let outcome = registry.create(&catalog).await.unwrap();
    let read_back = registry
        .read(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .await
        .unwrap();

    assert_eq!(outcome, CreateRelationCatalogOutcome::Created);
    assert_eq!(read_back, catalog);
    assert_eq!(
        registry
            .object_key(
                &catalog.relation_schema.relation_id,
                &catalog.relation_schema.relation_version
            )
            .unwrap()
            .as_str(),
        "v1/relations/orders/versions/2026-05-05.v1.relation.json"
    );
}

#[tokio::test]
async fn relation_catalog_registry_treats_duplicate_same_record_as_idempotent() {
    let (_temp_dir, store) = temp_store();
    let registry = RelationCatalogRegistry::new(store);
    let catalog = orders_relation_catalog();

    registry.create(&catalog).await.unwrap();
    let duplicate = registry.create(&catalog).await.unwrap();

    assert_eq!(duplicate, CreateRelationCatalogOutcome::Duplicate);
}

#[tokio::test]
async fn relation_catalog_registry_accepts_generic_feldera_relation_without_value_shape() {
    let (_temp_dir, store) = temp_store();
    let registry = RelationCatalogRegistry::new(store);
    let catalog = generic_activity_relation_catalog();

    let outcome = registry.create(&catalog).await.unwrap();
    let read_back = registry
        .read(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .await
        .unwrap();

    assert_eq!(outcome, CreateRelationCatalogOutcome::Created);
    assert_eq!(read_back, catalog);
}

#[tokio::test]
async fn relation_catalog_registry_rejects_same_key_with_different_body() {
    let (_temp_dir, store) = temp_store();
    let registry = RelationCatalogRegistry::new(store);
    let catalog = orders_relation_catalog();
    let mut changed_body = catalog.clone();
    changed_body.datafusion_registration.name = "orders_alias".to_string();

    registry.create(&catalog).await.unwrap();
    let error = registry.create(&changed_body).await.unwrap_err();

    assert!(matches!(
        error,
        RelationCatalogRegistryError::RecordConflict { .. }
    ));
}

#[tokio::test]
async fn relation_catalog_registry_rejects_unsupported_adapter_on_create() {
    let (_temp_dir, store) = temp_store();
    let registry = RelationCatalogRegistry::new(store);
    let mut catalog = orders_relation_catalog();
    catalog.incremental_adapter.adapter_id = "incremental-adapter-future-row-shaped-v1".to_string();

    let error = registry.create(&catalog).await.unwrap_err();

    assert!(matches!(
        error,
        RelationCatalogRegistryError::Validation(
            velorix_core::relation::RelationSchemaError::InvalidRelationSchema {
                field: "incremental_adapter.adapter_id"
            }
        )
    ));
}

#[tokio::test]
async fn relation_catalog_registry_rejects_multi_value_adapter_shape_on_create() {
    let (_temp_dir, store) = temp_store();
    let registry = RelationCatalogRegistry::new(store);
    let mut catalog = orders_relation_catalog();
    let mut fee_column = catalog.relation_schema.columns[1].clone();
    fee_column.column_id = "fee".to_string();
    fee_column.name = "fee".to_string();
    fee_column.ordinal = 3;
    catalog.relation_schema.columns.push(fee_column);
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.feldera_relation.schema_fingerprint = catalog.schema_fingerprint.clone();

    let error = registry.create(&catalog).await.unwrap_err();

    assert!(matches!(
        error,
        RelationCatalogRegistryError::Validation(
            velorix_core::relation::RelationSchemaError::InvalidRelationSchema {
                field: "incremental_adapter.value_columns"
            }
        )
    ));
}

#[tokio::test]
async fn relation_catalog_registry_rejects_unsupported_adapter_on_read() {
    let (_temp_dir, store) = temp_store();
    let registry = RelationCatalogRegistry::new(Arc::clone(&store));
    let mut catalog = orders_relation_catalog();
    catalog.incremental_adapter.adapter_id = "incremental-adapter-future-row-shaped-v1".to_string();
    let path = registry
        .object_key(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .unwrap();

    object_store::ObjectStore::put(
        &*store,
        &object_store::path::Path::from(path.as_str()),
        serde_json::to_vec(&catalog).unwrap().into(),
    )
    .await
    .unwrap();
    let error = registry
        .read(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RelationCatalogRegistryError::Validation(
            velorix_core::relation::RelationSchemaError::InvalidRelationSchema {
                field: "incremental_adapter.adapter_id"
            }
        )
    ));
}

#[tokio::test]
async fn relation_catalog_registry_rejects_multi_value_adapter_shape_on_read() {
    let (_temp_dir, store) = temp_store();
    let registry = RelationCatalogRegistry::new(Arc::clone(&store));
    let mut catalog = orders_relation_catalog();
    let mut fee_column = catalog.relation_schema.columns[1].clone();
    fee_column.column_id = "fee".to_string();
    fee_column.name = "fee".to_string();
    fee_column.ordinal = 3;
    catalog.relation_schema.columns.push(fee_column);
    catalog.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&catalog.relation_schema).unwrap();
    catalog.feldera_relation.schema_fingerprint = catalog.schema_fingerprint.clone();
    let path = registry
        .object_key(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .unwrap();

    object_store::ObjectStore::put(
        &*store,
        &object_store::path::Path::from(path.as_str()),
        serde_json::to_vec(&catalog).unwrap().into(),
    )
    .await
    .unwrap();
    let error = registry
        .read(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RelationCatalogRegistryError::Validation(
            velorix_core::relation::RelationSchemaError::InvalidRelationSchema {
                field: "incremental_adapter.value_columns"
            }
        )
    ));
}

#[tokio::test]
async fn relation_catalog_registry_rejects_unknown_or_malformed_stored_fields_on_read() {
    let (_temp_dir, store) = temp_store();
    let registry = RelationCatalogRegistry::new(Arc::clone(&store));
    let catalog = orders_relation_catalog();
    let path = registry
        .object_key(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .unwrap();

    object_store::ObjectStore::put(
        &*store,
        &object_store::path::Path::from(path.as_str()),
        br#"{"schema_version":1,"unexpected":true}"#.as_slice().into(),
    )
    .await
    .unwrap();

    let error = registry
        .read(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .await
        .unwrap_err();

    assert!(matches!(error, RelationCatalogRegistryError::Serde(_)));

    let mut malformed = catalog.clone();
    malformed.datafusion_registration.name.clear();
    object_store::ObjectStore::put(
        &*store,
        &object_store::path::Path::from(path.as_str()),
        serde_json::to_vec(&malformed).unwrap().into(),
    )
    .await
    .unwrap();

    let error = registry
        .read(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RelationCatalogRegistryError::Validation(
            velorix_core::relation::RelationSchemaError::MissingIdentityField {
                field: "datafusion_registration.name"
            }
        )
    ));
}

#[tokio::test]
async fn relation_catalog_registry_rejects_stored_body_identity_mismatch_on_read() {
    let (_temp_dir, store) = temp_store();
    let registry = RelationCatalogRegistry::new(Arc::clone(&store));
    let catalog = orders_relation_catalog();
    let mut wrong_body = catalog.clone();
    wrong_body.relation_schema.relation_id = "customers".to_string();
    wrong_body.feldera_relation.relation_id = "customers".to_string();
    wrong_body.schema_fingerprint =
        SchemaFingerprintV1::for_relation_schema(&wrong_body.relation_schema).unwrap();
    wrong_body.feldera_relation.schema_fingerprint = wrong_body.schema_fingerprint.clone();
    let path = registry
        .object_key(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .unwrap();

    object_store::ObjectStore::put(
        &*store,
        &object_store::path::Path::from(path.as_str()),
        serde_json::to_vec(&wrong_body).unwrap().into(),
    )
    .await
    .unwrap();

    let error = registry
        .read(
            &catalog.relation_schema.relation_id,
            &catalog.relation_schema.relation_version,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RelationCatalogRegistryError::RecordIdentityMismatch { .. }
    ));
}

#[test]
fn relation_catalog_registry_checked_constructor_requires_durable_store_capabilities() {
    let (_temp_dir, store) = temp_store();

    let error = RelationCatalogRegistry::new_checked(store, &weak_profile()).unwrap_err();

    assert_eq!(
        error.required_capability(),
        RequiredObjectStoreCapability::ConditionalCreate
    );
}
