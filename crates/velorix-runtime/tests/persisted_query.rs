use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::EngineCheckpoint,
    operator::KeyedSumCountAggregate,
    query::{QueryError, QueryPolicy, QueryPolicyError},
    relation::{
        ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
        FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1,
        RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
        VelorixRelationCatalogV1, VelorixRelationSchemaV1, RELATION_SCHEMA_VERSION_V1,
    },
};
use velorix_runtime::persisted_query::{
    query_persisted_recovered_materialized_view,
    query_persisted_recovered_materialized_view_with_limiter,
    query_production_persisted_recovered_materialized_view,
    query_production_persisted_recovered_materialized_view_with_limiter, PersistedQueryError,
    PersistedQueryStore,
};
use velorix_runtime::query::{QueryExecutionLimiter, RuntimeQueryError};
use velorix_runtime::recovery::{
    orders_sum_count_relation_catalog, RecoveryError, ORDERS_SUM_COUNT_RELATION_ID,
    ORDERS_SUM_COUNT_RELATION_VERSION,
};
use velorix_storage::{
    capability::{
        probe_authoritative_object_store_capabilities, AuthoritativeNamespace,
        AuthoritativeObjectStoreCapabilitiesV1, AuthoritativeObjectStoreCapabilityError,
        ObjectStoreCapabilityProfile,
    },
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::{IngestAdmissionCoordinator, IngestLog},
    manifest::{CheckpointManifest, InputRange, StateObjectRef},
    object_key::ObjectKey,
    relation_catalog_registry::{RelationCatalogRegistry, RelationCatalogRegistryError},
    state::{CheckpointPublishError, CheckpointPublisher, StateObjectWrite},
};

const RECOVERY_OWNER: &str = "orders_sum_count";

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
async fn persisted_query_store_creates_production_relation_query_against_catalog_table() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
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
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
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

#[tokio::test]
async fn persisted_recovered_query_execution_uses_stored_sql_and_policy() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let ingest_coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let checkpoint_input = batch([
        input_delta("account-a", 10, 1),
        input_delta("account-a", 5, 1),
    ]);
    let replay_input = batch([
        input_delta("account-a", 3, 1),
        input_delta("account-b", 7, 1),
    ]);

    append_ingest_envelope(
        Arc::clone(&store),
        &ingest_coordinator,
        "orders",
        0,
        0,
        2,
        &checkpoint_input,
    )
    .await;
    append_ingest_envelope(
        Arc::clone(&store),
        &ingest_coordinator,
        "orders",
        0,
        2,
        4,
        &replay_input,
    )
    .await;

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&checkpoint_input).unwrap();
    let state_ref = write_checkpoint_state(
        &publisher,
        "state-persisted-query",
        2,
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest(2, state_ref))
        .await
        .unwrap();

    catalog
        .create(
            "account-a-only",
            "select key_json, value_json, weight from input where key_json = '\"account-a\"'",
            QueryPolicy {
                max_output_rows: Some(1),
                ..QueryPolicy::default()
            },
        )
        .await
        .unwrap();

    let output = query_persisted_recovered_materialized_view(Arc::clone(&store), "account-a-only")
        .await
        .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(string_value(&output[0], 0, 0), "\"account-a\"");
    assert_eq!(string_value(&output[0], 1, 0), "{\"count\":3,\"sum\":18}");
    assert_eq!(int64_value(&output[0], 2, 0), 1);
}

#[tokio::test]
async fn persisted_recovered_query_execution_applies_stored_policy() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let ingest_coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let checkpoint_input = batch([input_delta("account-a", 10, 1)]);
    let replay_input = batch([input_delta("account-b", 7, 1)]);

    append_ingest_envelope(
        Arc::clone(&store),
        &ingest_coordinator,
        "orders",
        0,
        0,
        1,
        &checkpoint_input,
    )
    .await;
    append_ingest_envelope(
        Arc::clone(&store),
        &ingest_coordinator,
        "orders",
        0,
        1,
        2,
        &replay_input,
    )
    .await;

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&checkpoint_input).unwrap();
    let state_ref = write_checkpoint_state(
        &publisher,
        "state-persisted-query-policy",
        1,
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest(1, state_ref))
        .await
        .unwrap();

    catalog
        .create(
            "too-many-rows",
            "select key_json, value_json, weight from input order by key_json",
            QueryPolicy {
                max_output_rows: Some(1),
                ..QueryPolicy::default()
            },
        )
        .await
        .unwrap();

    let error = query_persisted_recovered_materialized_view(Arc::clone(&store), "too-many-rows")
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        PersistedQueryError::RuntimeQuery(velorix_runtime::query::RuntimeQueryError::Query(
            QueryError::Policy(QueryPolicyError::OutputRowsExceeded {
                observed_rows: 2,
                max_rows: 1
            })
        ))
    ));
}

#[tokio::test]
async fn persisted_recovered_query_requires_shared_limiter_when_stored_policy_sets_concurrency() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));

    catalog
        .create(
            "limited-recovered-query",
            "select key_json, value_json, weight from input",
            QueryPolicy {
                max_concurrent_queries: Some(1),
                ..QueryPolicy::default()
            },
        )
        .await
        .unwrap();

    let error =
        query_persisted_recovered_materialized_view(Arc::clone(&store), "limited-recovered-query")
            .await
            .unwrap_err();

    assert!(matches!(
        error,
        PersistedQueryError::RuntimeQuery(RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ConcurrencyLimiterRequired {
                max_concurrent_queries: 1
            }
        )))
    ));
}

#[tokio::test]
async fn persisted_recovered_query_accepts_matching_shared_limiter_when_policy_sets_concurrency() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let ingest_coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let checkpoint_input = batch([input_delta("account-a", 10, 1)]);
    let replay_input = batch([input_delta("account-a", 3, 1)]);

    append_ingest_envelope(
        Arc::clone(&store),
        &ingest_coordinator,
        "orders",
        0,
        0,
        1,
        &checkpoint_input,
    )
    .await;
    append_ingest_envelope(
        Arc::clone(&store),
        &ingest_coordinator,
        "orders",
        0,
        1,
        2,
        &replay_input,
    )
    .await;

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&checkpoint_input).unwrap();
    let state_ref = write_checkpoint_state(
        &publisher,
        "state-persisted-query-concurrency",
        1,
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest(1, state_ref))
        .await
        .unwrap();

    let policy = QueryPolicy {
        max_concurrent_queries: Some(1),
        ..QueryPolicy::default()
    };
    catalog
        .create(
            "limited-recovered-query",
            "select key_json, value_json, weight from input",
            policy,
        )
        .await
        .unwrap();

    let output = query_persisted_recovered_materialized_view_with_limiter(
        Arc::clone(&store),
        "limited-recovered-query",
        QueryExecutionLimiter::from_policy(policy),
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(string_value(&output[0], 0, 0), "\"account-a\"");
    assert_eq!(string_value(&output[0], 1, 0), "{\"count\":2,\"sum\":13}");
    assert_eq!(int64_value(&output[0], 2, 0), 1);
}

#[tokio::test]
async fn production_persisted_recovered_query_reads_slatedb_checkpoint_with_relation_catalog() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let ingest_coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let publisher =
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap();

    let checkpoint_input = batch([input_delta("account-a", 10, 1)]);
    let replay_input = batch([input_delta("account-b", 7, 1)]);

    append_ingest_envelope(
        Arc::clone(&store),
        &ingest_coordinator,
        "orders",
        0,
        0,
        1,
        &checkpoint_input,
    )
    .await;
    append_ingest_envelope(
        Arc::clone(&store),
        &ingest_coordinator,
        "orders",
        0,
        1,
        2,
        &replay_input,
    )
    .await;

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&checkpoint_input).unwrap();
    let state_ref = write_checkpoint_state(
        &publisher,
        "state-production-persisted-query",
        1,
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest(1, state_ref))
        .await
        .unwrap();

    catalog
        .create(
            "production-persisted-recovered",
            "select key_json, value_json, weight from input order by key_json",
            QueryPolicy::default(),
        )
        .await
        .unwrap();
    let capabilities = probed_persisted_query_capabilities(store.as_ref()).await;

    let output = query_production_persisted_recovered_materialized_view_with_limiter(
        Arc::clone(&store),
        "production-persisted-recovered",
        "v1/slatedb/state",
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &capabilities,
        None,
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 2);
    assert_eq!(string_value(&output[0], 0, 0), "\"account-a\"");
    assert_eq!(string_value(&output[0], 1, 0), "{\"count\":1,\"sum\":10}");
    assert_eq!(string_value(&output[0], 0, 1), "\"account-b\"");
    assert_eq!(string_value(&output[0], 1, 1), "{\"count\":1,\"sum\":7}");
}

#[tokio::test]
async fn production_persisted_recovered_query_fails_closed_when_relation_catalog_is_missing() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));

    catalog
        .create(
            "production-missing-catalog",
            "select key_json, value_json, weight from input",
            QueryPolicy::default(),
        )
        .await
        .unwrap();

    let error = query_production_persisted_recovered_materialized_view_with_limiter(
        Arc::clone(&store),
        "production-missing-catalog",
        "v1/slatedb/state",
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &local_capabilities(),
        None,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedQueryError::RuntimeQuery(RuntimeQueryError::Recovery(
            RecoveryError::RelationCatalogRegistry(RelationCatalogRegistryError::ObjectStore(
                object_store::Error::NotFound { .. }
            ))
        ))
    ));
}

#[tokio::test]
async fn production_persisted_recovered_query_validates_stored_sql_before_recovery() {
    let (_temp_dir, store) = temp_store();

    write_catalog_object(
        Arc::clone(&store),
        "production-invalid-recovered-query",
        json!({
            "schema_version": 1,
            "query_id": "production-invalid-recovered-query",
            "sql": "select missing_column from input",
            "policy": QueryPolicy::default(),
        }),
    )
    .await;

    let error = query_production_persisted_recovered_materialized_view_with_limiter(
        Arc::clone(&store),
        "production-invalid-recovered-query",
        "v1/slatedb/state",
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &local_capabilities(),
        None,
    )
    .await
    .unwrap_err();

    assert!(matches!(error, PersistedQueryError::Query(_)));
}

#[tokio::test]
async fn production_persisted_recovered_query_uses_checked_catalog_before_catalog_read() {
    let (_temp_dir, store) = temp_store();
    let key = ObjectKey::persisted_query("malformed-production-query").unwrap();
    store
        .put(
            &Path::from(key.as_str()),
            Bytes::from_static(br#"{"schema_version":"#).into(),
        )
        .await
        .unwrap();

    let missing_error = query_production_persisted_recovered_materialized_view_with_limiter(
        Arc::clone(&store),
        "malformed-production-query",
        "v1/slatedb/state",
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &capabilities_missing(AuthoritativeNamespace::Queries),
        None,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        missing_error,
        PersistedQueryError::ObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace {
                namespace: AuthoritativeNamespace::Queries
            }
        )
    ));

    let weak_error = query_production_persisted_recovered_materialized_view_with_limiter(
        Arc::clone(&store),
        "missing-production-query",
        "v1/slatedb/state",
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &capabilities_with_weak_namespace(AuthoritativeNamespace::Queries),
        None,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        weak_error,
        PersistedQueryError::ObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::NamespaceProfile {
                namespace: AuthoritativeNamespace::Queries,
                ..
            }
        )
    ));
}

#[tokio::test]
async fn production_persisted_recovered_query_fails_closed_for_raw_state_checkpoint() {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&orders_sum_count_relation_catalog().unwrap())
        .await
        .unwrap();
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let checkpoint_input = batch([input_delta("account-a", 10, 1)]);
    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&checkpoint_input).unwrap();
    let state_ref = write_checkpoint_state(
        &publisher,
        "state-raw-production-persisted-query",
        1,
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest(1, state_ref))
        .await
        .unwrap();
    catalog
        .create(
            "production-raw-state",
            "select key_json, value_json, weight from input",
            QueryPolicy::default(),
        )
        .await
        .unwrap();

    let error = query_production_persisted_recovered_materialized_view_with_limiter(
        Arc::clone(&store),
        "production-raw-state",
        "v1/slatedb/state",
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &local_capabilities(),
        None,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedQueryError::RuntimeQuery(RuntimeQueryError::Recovery(RecoveryError::Checkpoint(
            CheckpointPublishError::MissingStateObject(_)
        )))
    ));
}

#[tokio::test]
async fn production_persisted_recovered_query_requires_shared_limiter_when_policy_sets_concurrency()
{
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));

    catalog
        .create(
            "production-limited-recovered-query",
            "select key_json, value_json, weight from input",
            QueryPolicy {
                max_concurrent_queries: Some(1),
                ..QueryPolicy::default()
            },
        )
        .await
        .unwrap();

    let error = query_production_persisted_recovered_materialized_view(
        Arc::clone(&store),
        "production-limited-recovered-query",
        "v1/slatedb/state",
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &local_capabilities(),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        PersistedQueryError::RuntimeQuery(RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ConcurrencyLimiterRequired {
                max_concurrent_queries: 1
            }
        )))
    ));
}

#[tokio::test]
async fn production_persisted_recovered_query_accepts_matching_shared_limiter_when_policy_sets_concurrency(
) {
    let (_temp_dir, store) = temp_store();
    let catalog = PersistedQueryStore::new(Arc::clone(&store));
    let ingest_coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let publisher =
        CheckpointPublisher::with_slatedb_state_store(Arc::clone(&store), "v1/slatedb/state")
            .await
            .unwrap();

    let checkpoint_input = batch([input_delta("account-a", 10, 1)]);
    append_ingest_envelope(
        Arc::clone(&store),
        &ingest_coordinator,
        "orders",
        0,
        0,
        1,
        &checkpoint_input,
    )
    .await;

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&checkpoint_input).unwrap();
    let state_ref = write_checkpoint_state(
        &publisher,
        "state-production-persisted-query-concurrency",
        1,
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest(1, state_ref))
        .await
        .unwrap();

    let policy = QueryPolicy {
        max_concurrent_queries: Some(1),
        ..QueryPolicy::default()
    };
    catalog
        .create(
            "production-limited-recovered-query",
            "select key_json, value_json, weight from input",
            policy,
        )
        .await
        .unwrap();
    let capabilities = probed_persisted_query_capabilities(store.as_ref()).await;

    let output = query_production_persisted_recovered_materialized_view_with_limiter(
        Arc::clone(&store),
        "production-limited-recovered-query",
        "v1/slatedb/state",
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &capabilities,
        QueryExecutionLimiter::from_policy(policy),
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(string_value(&output[0], 0, 0), "\"account-a\"");
    assert_eq!(string_value(&output[0], 1, 0), "{\"count\":1,\"sum\":10}");
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

fn capabilities_missing(
    namespace: AuthoritativeNamespace,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    let mut profiles = local_capabilities().profiles;
    profiles.remove(&namespace);
    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}

fn capabilities_with_weak_namespace(
    namespace: AuthoritativeNamespace,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    let mut profile = ObjectStoreCapabilityProfile::local_development();
    profile.conditional_create = false;
    let mut profiles = local_capabilities().profiles;
    profiles.insert(namespace, profile);
    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}

async fn probed_persisted_query_capabilities(
    store: &dyn ObjectStore,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    probe_authoritative_object_store_capabilities(
        store,
        "local-persisted-query-test",
        "v1/persisted-query-capability-probes",
    )
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

fn batch(records: impl IntoIterator<Item = DeltaRecord>) -> DeltaBatch {
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

async fn append_ingest_envelope(
    store: Arc<dyn ObjectStore>,
    ingest_coordinator: &IngestAdmissionCoordinator,
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    input: &DeltaBatch,
) {
    let registry = RelationCatalogRegistry::new(store);
    registry
        .create(&orders_sum_count_relation_catalog().unwrap())
        .await
        .unwrap();
    let catalog = registry
        .read(
            ORDERS_SUM_COUNT_RELATION_ID,
            ORDERS_SUM_COUNT_RELATION_VERSION,
        )
        .await
        .unwrap();
    let bytes = IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
            relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: stream_id.to_string(),
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
        },
        &[ingest_record_batch(input)],
    )
    .unwrap();

    ingest_coordinator
        .append_catalog_validated_envelope(bytes)
        .await
        .unwrap();
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

fn input_range(end_offset_exclusive: u64) -> InputRange {
    InputRange {
        stream_id: "orders".to_string(),
        partition_id: 0,
        start_offset_inclusive: 0,
        end_offset_exclusive,
    }
}

fn manifest(input_end: u64, state_ref: StateObjectRef) -> CheckpointManifest {
    CheckpointManifest {
        schema_version: 1,
        checkpoint_version: 0,
        input_ranges: vec![input_range(input_end)],
        state_objects: vec![state_ref],
        output_objects: vec![],
        parent_checkpoint: None,
        created_at: "2026-05-04T00:00:00Z".to_string(),
    }
}

async fn write_checkpoint_state(
    publisher: &CheckpointPublisher,
    object_id: &str,
    logical_epoch: u64,
    state: &DeltaBatch,
) -> StateObjectRef {
    let checkpoint = EngineCheckpoint::new(logical_epoch, state.clone());
    let state = StateObjectWrite::new(
        RECOVERY_OWNER,
        0,
        0,
        object_id,
        Bytes::from(serde_json::to_vec(&checkpoint.to_payload()).unwrap()),
    )
    .unwrap();

    publisher.write_state_object(&state).await.unwrap()
}

fn string_value(batch: &arrow::record_batch::RecordBatch, column: usize, row: usize) -> &str {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(row)
}

fn int64_value(batch: &arrow::record_batch::RecordBatch, column: usize, row: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(row)
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
        feldera_relation: FelderaRelationBindingV1 {
            relation_id: "orders".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: "incremental-adapter-orders-v1".to_string(),
        },
    }
}
