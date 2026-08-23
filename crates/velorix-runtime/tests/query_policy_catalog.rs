use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore, ObjectStoreExt};
use serde_json::json;
use tempfile::TempDir;
use velorix_core::query::{QueryExecutionPolicyV1, QueryPolicyError};
use velorix_runtime::query_policy_catalog::{
    QueryPolicyCatalogError, QueryPolicyCatalogStore, QUERY_POLICY_CATALOG_SCHEMA_VERSION,
};
use velorix_storage::capability::{
    AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
    AuthoritativeObjectStoreCapabilityError, ObjectStoreCapabilityProfile,
    RequiredObjectStoreCapability,
};

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
}

#[tokio::test]
async fn query_policy_catalog_checked_constructor_rejects_missing_query_policy_namespace() {
    let (_temp_dir, store) = temp_store();

    let error = QueryPolicyCatalogStore::new_checked(
        Arc::clone(&store),
        &capabilities_missing(AuthoritativeNamespace::QueryPolicy),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        QueryPolicyCatalogError::ObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace {
                namespace: AuthoritativeNamespace::QueryPolicy
            }
        )
    ));
}

#[tokio::test]
async fn query_policy_catalog_checked_constructor_rejects_weak_query_policy_namespace() {
    let (_temp_dir, store) = temp_store();

    let error = QueryPolicyCatalogStore::new_checked(
        Arc::clone(&store),
        &capabilities_with_weak_namespace(AuthoritativeNamespace::QueryPolicy),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        QueryPolicyCatalogError::ObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::NamespaceProfile {
                namespace: AuthoritativeNamespace::QueryPolicy,
                source,
            }
        ) if source.required_capability() == RequiredObjectStoreCapability::ConditionalCreate
    ));
}

#[tokio::test]
async fn query_policy_catalog_creates_and_reads_policy_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = QueryPolicyCatalogStore::new(Arc::clone(&store));
    let policy = QueryExecutionPolicyV1 {
        max_output_rows: Some(10),
        ..QueryExecutionPolicyV1::default()
    };

    let created = catalog
        .create("tenant-a", "standard", policy)
        .await
        .unwrap();
    let read = catalog.get("tenant-a", "standard").await.unwrap();

    assert_eq!(created, read);
    assert_eq!(read.schema_version, QUERY_POLICY_CATALOG_SCHEMA_VERSION);
    assert_eq!(read.tenant_id, "tenant-a");
    assert_eq!(read.query_policy_id, "standard");
    assert_eq!(read.policy, policy);
}

#[tokio::test]
async fn query_policy_catalog_rejects_duplicate_policy_ids_using_create_semantics() {
    let (_temp_dir, store) = temp_store();
    let catalog = QueryPolicyCatalogStore::new(Arc::clone(&store));

    catalog
        .create("tenant-a", "standard", QueryExecutionPolicyV1::default())
        .await
        .unwrap();

    let error = catalog
        .create(
            "tenant-a",
            "standard",
            QueryExecutionPolicyV1 {
                max_output_rows: Some(10),
                ..QueryExecutionPolicyV1::default()
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(error, QueryPolicyCatalogError::ObjectStore(_)));
}

#[tokio::test]
async fn query_policy_catalog_rejects_unknown_fields_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = QueryPolicyCatalogStore::new(Arc::clone(&store));

    write_policy_catalog_object(
        Arc::clone(&store),
        "tenant-a",
        "standard",
        json!({
            "schema_version": 1,
            "tenant_id": "tenant-a",
            "query_policy_id": "standard",
            "policy": {},
            "unexpected": true,
        }),
    )
    .await;

    let error = catalog.get("tenant-a", "standard").await.unwrap_err();

    assert!(matches!(error, QueryPolicyCatalogError::Json(_)));
}

#[tokio::test]
async fn query_policy_catalog_rejects_malformed_policy_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = QueryPolicyCatalogStore::new(Arc::clone(&store));

    write_policy_catalog_object(
        Arc::clone(&store),
        "tenant-a",
        "standard",
        json!({
            "schema_version": 1,
            "tenant_id": "tenant-a",
            "query_policy_id": "standard",
            "policy": {
                "unexpected": true
            },
        }),
    )
    .await;

    let error = catalog.get("tenant-a", "standard").await.unwrap_err();

    assert!(matches!(error, QueryPolicyCatalogError::Json(_)));
}

#[tokio::test]
async fn query_policy_catalog_rejects_tenant_id_mismatch_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = QueryPolicyCatalogStore::new(Arc::clone(&store));

    write_policy_catalog_object(
        Arc::clone(&store),
        "tenant-a",
        "standard",
        json!({
            "schema_version": 1,
            "tenant_id": "tenant-b",
            "query_policy_id": "standard",
            "policy": {},
        }),
    )
    .await;

    let error = catalog.get("tenant-a", "standard").await.unwrap_err();

    assert!(matches!(
        error,
        QueryPolicyCatalogError::TenantIdMismatch {
            expected,
            actual,
        } if expected == "tenant-a" && actual == "tenant-b"
    ));
}

#[tokio::test]
async fn query_policy_catalog_rejects_policy_id_mismatch_from_object_storage() {
    let (_temp_dir, store) = temp_store();
    let catalog = QueryPolicyCatalogStore::new(Arc::clone(&store));

    write_policy_catalog_object(
        Arc::clone(&store),
        "tenant-a",
        "standard",
        json!({
            "schema_version": 1,
            "tenant_id": "tenant-a",
            "query_policy_id": "other",
            "policy": {},
        }),
    )
    .await;

    let error = catalog.get("tenant-a", "standard").await.unwrap_err();

    assert!(matches!(
        error,
        QueryPolicyCatalogError::QueryPolicyIdMismatch {
            expected,
            actual,
        } if expected == "standard" && actual == "other"
    ));
}

#[tokio::test]
async fn query_policy_catalog_rejects_invalid_zero_policy_on_create_and_get() {
    let (_temp_dir, store) = temp_store();
    let catalog = QueryPolicyCatalogStore::new(Arc::clone(&store));
    let invalid_policy = QueryExecutionPolicyV1 {
        planning_timeout_ms: Some(0),
        ..QueryExecutionPolicyV1::default()
    };

    let create_error = catalog
        .create("tenant-a", "standard", invalid_policy)
        .await
        .unwrap_err();
    assert!(matches!(
        create_error,
        QueryPolicyCatalogError::Policy(QueryPolicyError::InvalidZeroTimeout {
            field: "planning_timeout_ms"
        })
    ));

    write_policy_catalog_object(
        Arc::clone(&store),
        "tenant-a",
        "standard",
        json!({
            "schema_version": 1,
            "tenant_id": "tenant-a",
            "query_policy_id": "standard",
            "policy": {
                "planning_timeout_ms": 0
            },
        }),
    )
    .await;

    let get_error = catalog.get("tenant-a", "standard").await.unwrap_err();
    assert!(matches!(
        get_error,
        QueryPolicyCatalogError::Policy(QueryPolicyError::InvalidZeroTimeout {
            field: "planning_timeout_ms"
        })
    ));
}

#[tokio::test]
async fn query_policy_catalog_generic_create_and_get_allow_default_policy() {
    let (_temp_dir, store) = temp_store();
    let catalog = QueryPolicyCatalogStore::new(Arc::clone(&store));

    catalog
        .create("tenant-a", "bootstrap", QueryExecutionPolicyV1::default())
        .await
        .unwrap();

    let read = catalog.get("tenant-a", "bootstrap").await.unwrap();

    assert_eq!(read.policy, QueryExecutionPolicyV1::default());
}

#[tokio::test]
async fn unchecked_query_policy_catalog_rejects_production_create_before_policy_validation() {
    let (_temp_dir, store) = temp_store();
    let catalog = QueryPolicyCatalogStore::new(Arc::clone(&store));

    let error = catalog
        .create_for_production_table_scan(
            "tenant-a",
            "production",
            fully_bounded_production_policy(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        QueryPolicyCatalogError::MissingProductionAuthorityEvidence
    ));
}

#[tokio::test]
async fn query_policy_catalog_production_create_and_get_reject_default_policy() {
    let (_temp_dir, store) = temp_store();
    let catalog =
        QueryPolicyCatalogStore::new_checked(Arc::clone(&store), &all_namespace_capabilities())
            .unwrap();

    // Default policy now has all fields set, so it should be accepted in production mode
    catalog
        .create_for_production_table_scan(
            "tenant-a",
            "production",
            QueryExecutionPolicyV1::default(),
        )
        .await
        .unwrap();

    catalog
        .create("tenant-a", "bootstrap", QueryExecutionPolicyV1::default())
        .await
        .unwrap();

    // Default policy now has all fields set, so get_for_production_table_scan should succeed
    let _record = catalog
        .get_for_production_table_scan("tenant-a", "bootstrap")
        .await
        .unwrap();
}

#[tokio::test]
async fn query_policy_catalog_production_create_and_get_accept_fully_bounded_policy() {
    let (_temp_dir, store) = temp_store();
    let catalog =
        QueryPolicyCatalogStore::new_checked(Arc::clone(&store), &all_namespace_capabilities())
            .unwrap();
    let policy = fully_bounded_production_policy();

    let created = catalog
        .create_for_production_table_scan("tenant-a", "production", policy)
        .await
        .unwrap();
    let read = catalog
        .get_for_production_table_scan("tenant-a", "production")
        .await
        .unwrap();

    assert_eq!(created, read);
    assert_eq!(read.policy, policy);
}

#[tokio::test]
async fn query_policy_catalog_rejects_unsafe_path_segments_without_writing() {
    let (_temp_dir, store) = temp_store();
    let catalog = QueryPolicyCatalogStore::new(Arc::clone(&store));

    for (tenant_id, query_policy_id) in [
        ("", "standard"),
        (".", "standard"),
        ("..", "standard"),
        ("/tenant-a", "standard"),
        ("tenant/a", "standard"),
        ("tenant-a", ""),
        ("tenant-a", "."),
        ("tenant-a", ".."),
        ("tenant-a", "/standard"),
        ("tenant-a", "standard/base"),
    ] {
        assert!(
            catalog
                .create(
                    tenant_id,
                    query_policy_id,
                    QueryExecutionPolicyV1::default(),
                )
                .await
                .is_err(),
            "accepted invalid policy identity: {tenant_id}/{query_policy_id}"
        );
    }
}

fn fully_bounded_production_policy() -> QueryExecutionPolicyV1 {
    QueryExecutionPolicyV1 {
        max_sql_bytes: Some(16 * 1024),
        planning_timeout_ms: Some(1_000),
        execution_timeout_ms: Some(10_000),
        max_output_rows: Some(1_000),
        max_output_bytes: Some(1_000_000),
        max_scan_files: Some(100),
        max_scan_bytes: Some(128 * 1024 * 1024),
        max_object_requests: Some(1_000),
        max_concurrent_queries: Some(4),
        memory_limit_bytes: Some(512 * 1024 * 1024),
        spill_limit_bytes: Some(1024 * 1024 * 1024),
        ..QueryExecutionPolicyV1::default()
    }
}

async fn write_policy_catalog_object(
    store: Arc<dyn ObjectStore>,
    tenant_id: &str,
    query_policy_id: &str,
    catalog: serde_json::Value,
) {
    store
        .put(
            &Path::from(format!(
                "v1/query-policy/{tenant_id}/{query_policy_id}.json"
            )),
            Bytes::from(serde_json::to_vec(&catalog).unwrap()).into(),
        )
        .await
        .unwrap();
}

fn all_namespace_capabilities() -> AuthoritativeObjectStoreCapabilitiesV1 {
    let profile = ObjectStoreCapabilityProfile::local_development();
    AuthoritativeObjectStoreCapabilitiesV1::new(
        AuthoritativeNamespace::all()
            .into_iter()
            .map(|namespace| (namespace, profile.clone()))
            .collect::<BTreeMap<_, _>>(),
    )
}

fn capabilities_missing(
    namespace: AuthoritativeNamespace,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    let mut profiles = all_namespace_capabilities().profiles;
    profiles.remove(&namespace);
    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}

fn capabilities_with_weak_namespace(
    namespace: AuthoritativeNamespace,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    let mut profile = ObjectStoreCapabilityProfile::local_development();
    profile.conditional_create = false;
    let mut profiles = all_namespace_capabilities().profiles;
    profiles.insert(namespace, profile);
    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}
