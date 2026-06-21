#![cfg(feature = "legacy-source-scan-surfaces")]

use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    query::QueryPolicy,
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1, RELATION_SCHEMA_VERSION_V1,
    },
};
use velorix_runtime::persisted_query::{PersistedQueryError, PersistedQueryStore};
use velorix_storage::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
        ObjectStoreCapabilityProfile,
    },
    object_key::ObjectKey,
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

#[tokio::test]
async fn persisted_query_store_creates_and_reads_spec_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let policy = QueryPolicy {
        max_sql_bytes: Some(128),
        max_output_rows: Some(10),
        ..QueryPolicy::default()
    };

    let created = catalog
        .create(
            "orders-active",
            "select key_json, value_json, weight from input where weight > 0",
            policy,
        )
        .await
        .unwrap();
    let read = catalog.get("orders-active").await.unwrap();

    assert_eq!(created, read);
    assert_eq!(read.schema_version, 1);
    assert_eq!(read.query_id, "orders-active");
    assert_eq!(read.policy, policy);
}

#[tokio::test]
async fn persisted_query_store_rejects_duplicate_ids_using_create_semantics() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));

    catalog
        .create(
            "orders-active",
            "select key_json, value_json, weight from input",
            QueryPolicy::default(),
        )
        .await
        .unwrap();

    let error = catalog
        .create(
            "orders-active",
            "select key_json from input",
            QueryPolicy::default(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, PersistedQueryError::ObjectStore(_)));
}

#[tokio::test]
async fn persisted_query_store_does_not_write_catalog_object_when_sql_is_invalid() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));

    let error = catalog
        .create(
            "broken-query",
            "select missing_column from input",
            QueryPolicy::default(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, PersistedQueryError::Query(_)));
    let key = ObjectKey::persisted_query("broken-query").unwrap();
    let path = Path::from(key.as_str());
    assert!(matches!(
        store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn unchecked_persisted_query_store_rejects_production_relation_create_before_validation() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let relation_catalog = orders_relation_catalog();

    let error = catalog
        .create_for_production_relation(
            "orders-production",
            "select account_id, value, weight from orders",
            QueryPolicy::default(),
            &relation_catalog,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        PersistedQueryError::MissingProductionAuthorityEvidence
    ));
}

#[tokio::test]
async fn persisted_query_store_creates_production_relation_query_against_catalog_table() {
    let (_temp_dir, store) = temp_store();
    let catalog =
        PersistedQueryStore::new_checked(Arc::clone(&store), &local_capabilities()).unwrap();
    let relation_catalog = orders_relation_catalog();

    let created = catalog
        .create_for_production_relation(
            "orders-production",
            "select account_id, value, weight from orders where weight > 0",
            QueryPolicy::default(),
            &relation_catalog,
        )
        .await
        .unwrap();

    assert_eq!(created.query_id, "orders-production");
    assert_eq!(
        created.sql,
        "select account_id, value, weight from orders where weight > 0"
    );
}

#[tokio::test]
async fn persisted_query_store_rejects_input_query_for_production_relation_before_writing() {
    let (_temp_dir, store) = temp_store();
    let catalog =
        PersistedQueryStore::new_checked(Arc::clone(&store), &local_capabilities()).unwrap();
    let relation_catalog = orders_relation_catalog();

    let error = catalog
        .create_for_production_relation(
            "bootstrap-input-query",
            "select key_json from input",
            QueryPolicy::default(),
            &relation_catalog,
        )
        .await
        .unwrap_err();

    assert!(matches!(error, PersistedQueryError::Query(_)));
    let key = ObjectKey::persisted_query("bootstrap-input-query").unwrap();
    assert!(matches!(
        store.head(&Path::from(key.as_str())).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn persisted_query_store_rejects_unsupported_schema_version_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));

    write_catalog_object(
        Arc::clone(&store),
        "orders-active",
        json!({
            "schema_version": 2,
            "query_id": "orders-active",
            "sql": "select key_json, value_json, weight from input",
            "policy": QueryPolicy::default(),
        }),
    )
    .await;

    let error = catalog.get("orders-active").await.unwrap_err();

    assert!(matches!(
        error,
        PersistedQueryError::UnsupportedSchemaVersion { schema_version: 2 }
    ));
}

#[tokio::test]
async fn persisted_query_store_rejects_mismatched_query_id_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));

    write_catalog_object(
        Arc::clone(&store),
        "orders-active",
        json!({
            "schema_version": 1,
            "query_id": "other-query",
            "sql": "select key_json, value_json, weight from input",
            "policy": QueryPolicy::default(),
        }),
    )
    .await;

    let error = catalog.get("orders-active").await.unwrap_err();

    assert!(matches!(
        error,
        PersistedQueryError::QueryIdMismatch {
            expected,
            actual,
        } if expected == "orders-active" && actual == "other-query"
    ));
}

#[tokio::test]
async fn persisted_query_store_rejects_malformed_json_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let key = ObjectKey::persisted_query("orders-active").unwrap();

    store
        .put(
            &Path::from(key.as_str()),
            Bytes::from_static(br#"{"schema_version":"#).into(),
        )
        .await
        .unwrap();

    let error = catalog.get("orders-active").await.unwrap_err();

    assert!(matches!(error, PersistedQueryError::Json(_)));
}

#[tokio::test]
async fn persisted_query_store_rejects_unknown_spec_field_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));

    write_catalog_object(
        Arc::clone(&store),
        "orders-active",
        json!({
            "schema_version": 1,
            "query_id": "orders-active",
            "sql": "select key_json, value_json, weight from input",
            "policy": QueryPolicy::default(),
            "unexpected": true,
        }),
    )
    .await;

    let error = catalog.get("orders-active").await.unwrap_err();

    assert!(matches!(error, PersistedQueryError::Json(_)));
}

#[tokio::test]
async fn persisted_query_store_rejects_unknown_policy_field_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));

    write_catalog_object(
        Arc::clone(&store),
        "orders-active",
        json!({
            "schema_version": 1,
            "query_id": "orders-active",
            "sql": "select key_json, value_json, weight from input",
            "policy": {
                "max_sql_bytes": null,
                "max_output_rows": null,
                "batch_size": null,
                "target_partitions": null,
                "unexpected": true,
            },
        }),
    )
    .await;

    let error = catalog.get("orders-active").await.unwrap_err();

    assert!(matches!(error, PersistedQueryError::Json(_)));
}

fn local_capabilities() -> AuthoritativeObjectStoreCapabilitiesV1 {
    let profile = ObjectStoreCapabilityProfile::local_development();
    AuthoritativeObjectStoreCapabilitiesV1::new(
        AuthoritativeNamespace::all()
            .into_iter()
            .map(|namespace| (namespace, profile.clone()))
            .collect::<BTreeMap<_, _>>(),
    )
}

async fn write_catalog_object(
    store: Arc<dyn ObjectStore>,
    query_id: &str,
    catalog: serde_json::Value,
) {
    let key = ObjectKey::persisted_query(query_id).unwrap();
    store
        .put(
            &Path::from(key.as_str()),
            Bytes::from(serde_json::to_vec(&catalog).unwrap()).into(),
        )
        .await
        .unwrap();
}

fn orders_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "orders".to_string(),
        relation_name: "orders".to_string(),
        relation_version: "2026-05-05.v1".to_string(),
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
                column_id: "value".to_string(),
                name: "value".to_string(),
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
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "orders".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: "incremental-adapter-orders-v1".to_string(),
        },
    }
}
