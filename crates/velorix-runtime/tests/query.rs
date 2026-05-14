use std::{collections::BTreeMap, error::Error, num::NonZeroUsize, sync::Arc};

use arrow::array::{ArrayRef, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::{
    error::DataFusionError,
    object_store::{
        memory::InMemory as DataFusionInMemory, path::Path as DataFusionPath,
        ObjectStore as DataFusionObjectStore, ObjectStoreExt as DataFusionObjectStoreExt,
    },
};
use object_store::{local::LocalFileSystem, ObjectStore};
use parquet::arrow::ArrowWriter;
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::EngineCheckpoint,
    feldera_artifact::catalog_input_relation_schema,
    operator::KeyedSumCountAggregate,
    query::{QueryError, QueryPolicy, QueryPolicyError},
    relation::{
        datafusion_schema_from_catalog, DATAFUSION_RELATION_ID_METADATA_KEY,
        DATAFUSION_RELATION_VERSION_METADATA_KEY, DATAFUSION_SCHEMA_FINGERPRINT_METADATA_KEY,
    },
};
use velorix_runtime::{
    query::{
        query_object_backed_input_with_policy, query_object_backed_input_with_policy_and_metrics,
        query_production_recovered_materialized_view_with_policy_and_limiter,
        query_recovered_materialized_view, query_recovered_materialized_view_with_policy,
        RuntimeQueryError,
    },
    recovery::{
        orders_sum_count_relation_catalog, RecoveredRuntime, RecoveryError,
        ORDERS_SUM_COUNT_RELATION_ID, ORDERS_SUM_COUNT_RELATION_VERSION,
    },
};
use velorix_storage::{
    capability::{
        probe_authoritative_object_store_capabilities, AuthoritativeNamespace,
        AuthoritativeObjectStoreCapabilitiesV1, AuthoritativeObjectStoreCapabilityError,
        ObjectStoreCapabilityProfile,
    },
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::{IngestAdmissionCoordinator, IngestBatch, IngestLog, IngestLogError},
    manifest::{CheckpointManifest, InputRange, StateObjectRef},
    relation_catalog_registry::{RelationCatalogRegistry, RelationCatalogRegistryError},
    state::{CheckpointPublishError, CheckpointPublisher, StateObjectWrite},
};

const RECOVERY_OWNER: &str = "orders_sum_count";

fn temp_store() -> (TempDir, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();

    (temp_dir, Arc::new(store))
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
    let profile = ObjectStoreCapabilityProfile::local_development();
    let mut profiles = AuthoritativeNamespace::all()
        .into_iter()
        .map(|namespace| (namespace, profile.clone()))
        .collect::<BTreeMap<_, _>>();
    profiles.remove(&namespace);
    AuthoritativeObjectStoreCapabilitiesV1::new(profiles)
}

async fn probed_query_capabilities(
    store: &dyn ObjectStore,
) -> AuthoritativeObjectStoreCapabilitiesV1 {
    probe_authoritative_object_store_capabilities(
        store,
        "local-query-test",
        "v1/query-capability-probes",
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

fn batch_bytes(batch: &DeltaBatch) -> Bytes {
    Bytes::from(serde_json::to_vec(batch).unwrap())
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

fn ingest_envelope_bytes(
    stream_id: &str,
    partition_id: u32,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
    input: &DeltaBatch,
) -> Bytes {
    let catalog = orders_sum_count_relation_catalog().unwrap();
    ingest_envelope_bytes_with_relation(
        IngestEnvelopeEncodeRequest {
            relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
            relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: stream_id.to_string(),
            partition_id,
            start_offset_inclusive,
            end_offset_exclusive,
        },
        input,
    )
}

fn ingest_envelope_bytes_with_relation(
    request: IngestEnvelopeEncodeRequest,
    input: &DeltaBatch,
) -> Bytes {
    IngestEnvelope::encode_batches(request, &[ingest_record_batch(input)]).unwrap()
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

    ingest_coordinator
        .append_catalog_validated_envelope(ingest_envelope_bytes_with_relation(
            IngestEnvelopeEncodeRequest {
                relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
                relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
                schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
                stream_id: stream_id.to_string(),
                partition_id,
                start_offset_inclusive,
                end_offset_exclusive,
            },
            input,
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn catalog_validated_ingest_append_fails_closed_when_relation_catalog_is_missing() {
    let (_temp_dir, store) = temp_store();
    let ingest_coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let input = batch([input_delta("account-a", 4, 1)]);

    let error = ingest_coordinator
        .append_catalog_validated_envelope(ingest_envelope_bytes("orders", 0, 0, 1, &input))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        IngestLogError::RelationCatalogRegistry(RelationCatalogRegistryError::ObjectStore(
            object_store::Error::NotFound { .. }
        ))
    ));
}

fn parquet_input_batch(keys: &[&str], values: &[&str], weights: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key_json", DataType::Utf8, false),
            Field::new("value_json", DataType::Utf8, false),
            Field::new("weight", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(keys.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(weights.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn parquet_bytes(batch: &RecordBatch) -> Bytes {
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }
    Bytes::from(bytes)
}

async fn put_parquet_input(
    store: &Arc<dyn DataFusionObjectStore>,
    path: &str,
    batch: &RecordBatch,
) {
    store
        .put(&DataFusionPath::from(path), parquet_bytes(batch).into())
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

#[tokio::test]
async fn query_recovered_materialized_view_reads_checkpointed_state_and_replayed_ingest() {
    let (_temp_dir, store) = temp_store();
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
    let state_ref =
        write_checkpoint_state(&publisher, "state-query", 2, &checkpointed_view.state()).await;
    publisher
        .publish_manifest(&manifest(2, state_ref))
        .await
        .unwrap();

    let output = query_recovered_materialized_view(
        Arc::clone(&store),
        "select key_json, value_json, weight from input order by key_json",
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 2);
    assert_eq!(string_value(&output[0], 0, 0), "\"account-a\"");
    assert_eq!(string_value(&output[0], 1, 0), "{\"count\":3,\"sum\":18}");
    assert_eq!(int64_value(&output[0], 2, 0), 1);
    assert_eq!(string_value(&output[0], 0, 1), "\"account-b\"");
    assert_eq!(string_value(&output[0], 1, 1), "{\"count\":1,\"sum\":7}");
    assert_eq!(int64_value(&output[0], 2, 1), 1);
}

#[tokio::test]
async fn query_recovered_materialized_view_with_policy_applies_row_limit_to_recovered_state() {
    let (_temp_dir, store) = temp_store();
    let ingest_coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let checkpoint_input = batch([
        input_delta("account-a", 10, 1),
        input_delta("account-a", 5, 1),
    ]);
    let replay_input = batch([input_delta("account-b", 7, 1)]);

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
        3,
        &replay_input,
    )
    .await;

    let mut checkpointed_view = KeyedSumCountAggregate::new();
    checkpointed_view.apply(&checkpoint_input).unwrap();
    let state_ref = write_checkpoint_state(
        &publisher,
        "state-query-policy",
        2,
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest(2, state_ref))
        .await
        .unwrap();

    let error = query_recovered_materialized_view_with_policy(
        Arc::clone(&store),
        "select key_json, value_json, weight from input order by key_json",
        QueryPolicy {
            max_output_rows: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(QueryPolicyError::OutputRowsExceeded {
            observed_rows: 2,
            max_rows: 1
        }))
    ));
}

#[tokio::test]
async fn query_recovered_materialized_view_with_policy_applies_byte_limit_under_row_limit() {
    let (_temp_dir, store) = temp_store();
    let ingest_coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));

    let checkpoint_input = batch([input_delta("account-with-wide-output", 10, 1)]);

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
        "state-query-byte-policy",
        1,
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest(1, state_ref))
        .await
        .unwrap();

    let error = query_recovered_materialized_view_with_policy(
        Arc::clone(&store),
        "select key_json, value_json, weight from input",
        QueryPolicy {
            max_output_rows: Some(10),
            max_output_bytes: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(QueryPolicyError::OutputBytesExceeded {
            observed_bytes,
            max_bytes: 1,
        })) if observed_bytes > 1
    ));
}

#[tokio::test]
async fn query_production_recovered_materialized_view_reads_slatedb_checkpoint_with_catalog_record()
{
    let (_temp_dir, store) = temp_store();
    let capabilities = probed_query_capabilities(store.as_ref()).await;
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
        "state-production-query",
        1,
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest(1, state_ref))
        .await
        .unwrap();

    let output = query_production_recovered_materialized_view_with_policy_and_limiter(
        Arc::clone(&store),
        "v1/slatedb/state",
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &capabilities,
        "select key_json, value_json, weight from input order by key_json",
        QueryPolicy::default(),
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
async fn query_production_recovered_materialized_view_fails_closed_when_relation_catalog_is_missing(
) {
    let (_temp_dir, store) = temp_store();

    let error = query_production_recovered_materialized_view_with_policy_and_limiter(
        Arc::clone(&store),
        "v1/slatedb/state",
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &local_capabilities(),
        "select key_json, value_json, weight from input",
        QueryPolicy::default(),
        None,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Recovery(RecoveryError::RelationCatalogRegistry(
            RelationCatalogRegistryError::ObjectStore(object_store::Error::NotFound { .. })
        ))
    ));
}

#[tokio::test]
async fn query_production_recovered_materialized_view_validates_sql_before_recovery() {
    let (_temp_dir, store) = temp_store();

    let error = query_production_recovered_materialized_view_with_policy_and_limiter(
        Arc::clone(&store),
        "v1/slatedb/state",
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &local_capabilities(),
        "select missing_column from input",
        QueryPolicy::default(),
        None,
    )
    .await
    .unwrap_err();

    assert!(
        matches!(
            error,
            RuntimeQueryError::Query(QueryError::DataFusion(ref datafusion_error))
                if datafusion_error.to_string().contains("missing_column")
        ),
        "expected SQL planning to reject missing_column before recovery, got {error:?}"
    );
}

#[tokio::test]
async fn query_production_recovered_materialized_view_fails_closed_when_capability_profile_is_missing(
) {
    let (_temp_dir, store) = temp_store();
    let capabilities = capabilities_missing(AuthoritativeNamespace::Ingest);

    let error = query_production_recovered_materialized_view_with_policy_and_limiter(
        Arc::clone(&store),
        "v1/slatedb/state",
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &capabilities,
        "select key_json, value_json, weight from input",
        QueryPolicy::default(),
        None,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Recovery(RecoveryError::AuthoritativeObjectStoreCapabilities(
            AuthoritativeObjectStoreCapabilityError::MissingNamespace {
                namespace: AuthoritativeNamespace::Ingest
            }
        ))
    ));
}

#[tokio::test]
async fn query_production_recovered_materialized_view_fails_closed_for_raw_state_checkpoint() {
    let (_temp_dir, store) = temp_store();
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
        "state-raw-production-query",
        1,
        &checkpointed_view.state(),
    )
    .await;
    publisher
        .publish_manifest(&manifest(1, state_ref))
        .await
        .unwrap();

    let error = query_production_recovered_materialized_view_with_policy_and_limiter(
        Arc::clone(&store),
        "v1/slatedb/state",
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &local_capabilities(),
        "select key_json, value_json, weight from input",
        QueryPolicy::default(),
        None,
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Recovery(RecoveryError::Checkpoint(
            CheckpointPublishError::MissingStateObject(_)
        ))
    ));
}

#[tokio::test]
async fn recovery_rejects_json_bytes_under_valid_v1_ingest_key() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let input = batch([input_delta("account-a", 4, 1)]);

    ingest_log
        .append(&IngestBatch::new("orders", 0, 0, 1, batch_bytes(&input)).unwrap())
        .await
        .unwrap();

    let error = RecoveredRuntime::recover(Arc::clone(&store))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::Ingest(IngestLogError::IngestEnvelope(_))
    ));
}

#[tokio::test]
async fn recovery_rejects_ingest_envelope_with_wrong_relation_version() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let input = batch([input_delta("account-a", 4, 1)]);
    let catalog = orders_sum_count_relation_catalog().unwrap();
    let bytes = ingest_envelope_bytes_with_relation(
        IngestEnvelopeEncodeRequest {
            relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
            relation_version: "2026-05-06.v1".to_string(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 1,
        },
        &input,
    );

    // Intentional bootstrap append: this fixture needs durable relation drift.
    ingest_log.append_validated_envelope(bytes).await.unwrap();

    let error = RecoveredRuntime::recover(Arc::clone(&store))
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
async fn recovery_rejects_ingest_envelope_with_wrong_schema_fingerprint() {
    let (_temp_dir, store) = temp_store();
    let ingest_log = IngestLog::new(Arc::clone(&store));
    let input = batch([input_delta("account-a", 4, 1)]);
    let bytes = ingest_envelope_bytes_with_relation(
        IngestEnvelopeEncodeRequest {
            relation_id: ORDERS_SUM_COUNT_RELATION_ID.to_string(),
            relation_version: ORDERS_SUM_COUNT_RELATION_VERSION.to_string(),
            schema_fingerprint:
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_string(),
            stream_id: "orders".to_string(),
            partition_id: 0,
            start_offset_inclusive: 0,
            end_offset_exclusive: 1,
        },
        &input,
    );

    // Intentional bootstrap append: this fixture needs durable schema drift.
    ingest_log.append_validated_envelope(bytes).await.unwrap();

    let error = RecoveredRuntime::recover(Arc::clone(&store))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::IngestRelationMismatch {
            field: "schema_fingerprint",
            ..
        }
    ));
}

#[tokio::test]
async fn arrow_ingest_datafusion_and_feldera_use_the_same_catalog_identity() {
    let (_temp_dir, store) = temp_store();
    let ingest_coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let input = batch([input_delta("account-a", 4, 1)]);
    let catalog = orders_sum_count_relation_catalog().unwrap();
    let bytes = ingest_envelope_bytes("orders", 0, 0, 1, &input);
    let envelope = IngestEnvelope::decode(bytes.clone()).unwrap();

    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    ingest_coordinator
        .append_catalog_validated_envelope(bytes)
        .await
        .unwrap();
    let recovered = RecoveredRuntime::recover(Arc::clone(&store)).await.unwrap();
    let datafusion_schema = datafusion_schema_from_catalog(&catalog).unwrap();
    let feldera_schema = catalog_input_relation_schema(&catalog).unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(
        envelope.header().relation_id,
        catalog.relation_schema.relation_id
    );
    assert_eq!(
        envelope.header().relation_version,
        catalog.relation_schema.relation_version
    );
    assert_eq!(
        envelope.header().schema_fingerprint,
        catalog.schema_fingerprint.as_str()
    );
    assert_eq!(
        datafusion_schema.metadata()[DATAFUSION_RELATION_ID_METADATA_KEY],
        catalog.relation_schema.relation_id
    );
    assert_eq!(
        datafusion_schema.metadata()[DATAFUSION_RELATION_VERSION_METADATA_KEY],
        catalog.relation_schema.relation_version
    );
    assert_eq!(
        datafusion_schema.metadata()[DATAFUSION_SCHEMA_FINGERPRINT_METADATA_KEY],
        catalog.schema_fingerprint.as_str()
    );
    assert_eq!(
        feldera_schema.relation_id,
        catalog.relation_schema.relation_id
    );
    assert_eq!(
        feldera_schema.relation_version,
        catalog.relation_schema.relation_version
    );
    assert_eq!(
        feldera_schema.schema_fingerprint,
        catalog.schema_fingerprint.as_str()
    );
}

#[tokio::test]
async fn query_recovered_materialized_view_propagates_datafusion_errors() {
    let (_temp_dir, store) = temp_store();
    let ingest_coordinator = IngestAdmissionCoordinator::new(IngestLog::new(Arc::clone(&store)));
    let input = batch([input_delta("account-a", 4, 1)]);

    append_ingest_envelope(
        Arc::clone(&store),
        &ingest_coordinator,
        "orders",
        0,
        0,
        1,
        &input,
    )
    .await;

    let error =
        query_recovered_materialized_view(Arc::clone(&store), "select missing_column from input")
            .await
            .unwrap_err();

    assert!(error.to_string().contains("missing_column"));
}

#[tokio::test]
async fn query_object_backed_input_scans_parquet_objects_without_materializing_delta_batch() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(
            &["\"account-a\"", "\"account-a\"", "\"account-b\""],
            &["10", "5", "7"],
            &[1, 1, -1],
        ),
    )
    .await;

    let output = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, sum(cast(value_json as int)) as total_value, sum(weight) as total_weight \
         from input where weight > 0 group by key_json order by key_json",
        QueryPolicy::default(),
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(string_value(&output[0], 0, 0), "\"account-a\"");
    assert_eq!(int64_value(&output[0], 1, 0), 15);
    assert_eq!(int64_value(&output[0], 2, 0), 2);
}

#[tokio::test]
async fn query_object_backed_input_reports_object_request_metrics_when_requested() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\"", "\"account-b\""], &["10", "7"], &[1, 1]),
    )
    .await;

    let output = query_object_backed_input_with_policy_and_metrics(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, value_json, weight from input order by key_json",
        QueryPolicy {
            max_scan_files: Some(1),
            max_output_rows: Some(4),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(output.batches.len(), 1);
    assert_eq!(output.batches[0].num_rows(), 2);
    assert!(output.object_requests.list_count > 0);
    assert!(
        output.object_requests.get_count + output.object_requests.range_read_count > 0,
        "expected DataFusion to read the Parquet object"
    );
    assert!(output.object_requests.bytes_read > 0);
}

#[tokio::test]
async fn query_object_backed_input_applies_policy_row_limit() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\"", "\"account-b\""], &["10", "7"], &[1, 1]),
    )
    .await;

    let error = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, value_json, weight from input order by key_json",
        QueryPolicy {
            max_output_rows: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(QueryPolicyError::OutputRowsExceeded {
            observed_rows: 2,
            max_rows: 1
        }))
    ));
}

#[tokio::test]
async fn query_object_backed_input_applies_byte_limit_under_row_limit() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-with-wide-output\""], &["10"], &[1]),
    )
    .await;

    let error = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, value_json, weight from input",
        QueryPolicy {
            max_output_rows: Some(10),
            max_output_bytes: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(QueryPolicyError::OutputBytesExceeded {
            observed_bytes,
            max_bytes: 1,
        })) if observed_bytes > 1
    ));
}

#[tokio::test]
async fn query_object_backed_input_with_memory_and_spill_policy_still_runs() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\"", "\"account-b\""], &["10", "7"], &[1, 1]),
    )
    .await;

    let output = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, value_json, weight from input order by key_json",
        QueryPolicy {
            memory_limit_bytes: Some(64 * 1024 * 1024),
            spill_limit_bytes: Some(32 * 1024 * 1024),
            batch_size: Some(NonZeroUsize::new(2).unwrap()),
            target_partitions: Some(NonZeroUsize::new(1).unwrap()),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 2);
    assert_eq!(string_value(&output[0], 0, 0), "\"account-a\"");
    assert_eq!(string_value(&output[0], 0, 1), "\"account-b\"");
}

#[tokio::test]
async fn query_object_backed_input_returns_datafusion_memory_exhausted_when_grouped_aggregate_exceeds_memory_policy(
) {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\"", "\"account-b\""], &["10", "7"], &[1, 1]),
    )
    .await;

    let error = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, sum(cast(value_json as int)) as total_value \
         from input group by key_json",
        QueryPolicy {
            memory_limit_bytes: Some(1),
            batch_size: Some(NonZeroUsize::new(2).unwrap()),
            target_partitions: Some(NonZeroUsize::new(1).unwrap()),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(
        matches!(
            error,
            RuntimeQueryError::Query(QueryError::DataFusion(ref error))
                if datafusion_error_has_resources_exhausted_root(error)
        ),
        "expected DataFusion ResourcesExhausted, got {error:?}"
    );
}

#[tokio::test]
async fn query_object_backed_input_rejects_scan_above_byte_limit() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\""], &["10"], &[1]),
    )
    .await;

    let error = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, value_json, weight from input",
        QueryPolicy {
            max_scan_bytes: Some(1),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(QueryPolicyError::ScanBytesExceeded {
            observed_bytes,
            max_bytes: 1,
        })) if observed_bytes > 1
    ));
}

#[tokio::test]
async fn query_object_backed_input_debits_preflight_and_runtime_against_one_object_request_budget()
{
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\"", "\"account-b\""], &["10", "7"], &[1, 1]),
    )
    .await;

    let error = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, value_json, weight from input order by key_json",
        QueryPolicy {
            max_object_requests: Some(3),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ObjectRequestsExceeded {
                observed_requests,
                max_requests: 3,
            }
        )) if observed_requests > 3
    ));
}

#[tokio::test]
async fn query_object_backed_input_limit_one_still_accounts_file_access_requests() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\""], &["10"], &[1]),
    )
    .await;

    let error = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, value_json, weight from input limit 1",
        QueryPolicy {
            max_object_requests: Some(2),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ObjectRequestsExceeded {
                observed_requests,
                max_requests: 2,
            }
        )) if observed_requests > 2
    ));
}

#[tokio::test]
async fn query_object_backed_input_rejects_object_requests_above_limit_before_limit_sql_scan() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());
    put_parquet_input(
        &store,
        "input/part-000.parquet",
        &parquet_input_batch(&["\"account-a\""], &["10"], &[1]),
    )
    .await;
    put_parquet_input(
        &store,
        "input/part-001.parquet",
        &parquet_input_batch(&["\"account-b\""], &["7"], &[1]),
    )
    .await;

    let error = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/input/",
        "select key_json, value_json, weight from input limit 1",
        QueryPolicy {
            max_object_requests: Some(2),
            ..QueryPolicy::default()
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::Policy(
            QueryPolicyError::ObjectRequestsExceeded {
                observed_requests: 3,
                max_requests: 2,
            }
        ))
    ));
}

#[tokio::test]
async fn query_object_backed_input_propagates_datafusion_scan_errors() {
    let store: Arc<dyn DataFusionObjectStore> = Arc::new(DataFusionInMemory::new());

    let error = query_object_backed_input_with_policy(
        Arc::clone(&store),
        "memory://velorix/missing/",
        "select key_json, value_json, weight from input",
        QueryPolicy::default(),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        RuntimeQueryError::Query(QueryError::DataFusion(_))
    ));
}

fn datafusion_error_has_resources_exhausted_root(error: &DataFusionError) -> bool {
    if matches!(error, DataFusionError::ResourcesExhausted(_)) {
        return true;
    }

    let mut source = error.source();
    while let Some(error) = source {
        if let Some(error) = error.downcast_ref::<DataFusionError>() {
            return datafusion_error_has_resources_exhausted_root(error);
        }
        source = error.source();
    }

    false
}

fn string_value(batch: &arrow::record_batch::RecordBatch, column: usize, row: usize) -> String {
    let column = batch.column(column);
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        return array.value(row).to_string();
    }
    if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() {
        return array.value(row).to_string();
    }

    panic!(
        "expected string-compatible column, got {:?}",
        column.data_type()
    );
}

fn int64_value(batch: &arrow::record_batch::RecordBatch, column: usize, row: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(row)
}
