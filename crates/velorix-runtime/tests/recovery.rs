use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use object_store::{local::LocalFileSystem, ObjectStore};
use serde_json::json;
use tempfile::TempDir;
use velorix_core::{
    delta::{DeltaBatch, DeltaKey, DeltaRecord, DeltaValue},
    engine::EngineCheckpoint,
};
use velorix_runtime::recovery::{
    orders_sum_count_relation_catalog, RecoveredRuntime, RecoveryError, ORDERS_SUM_COUNT_OWNER,
    ORDERS_SUM_COUNT_RELATION_ID, ORDERS_SUM_COUNT_RELATION_VERSION,
};
use velorix_storage::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
        AuthoritativeObjectStoreCapabilityError, ObjectStoreCapabilityProfile,
    },
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::IngestLog,
    manifest::{CheckpointManifest, InputRange, StateObjectRef},
    relation_catalog_registry::{RelationCatalogRegistry, RelationCatalogRegistryError},
    state::{CheckpointPublisher, StateObjectWrite},
};

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

#[tokio::test]
async fn catalog_backed_recovery_reads_catalog_record_and_replays_catalog_aware_ingest() {
    let (_temp_dir, store) = temp_store();
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([input_delta("account-a", 4, 1)]);
    IngestLog::new(Arc::clone(&store))
        .append_catalog_validated_envelope(ingest_envelope_bytes(
            ORDERS_SUM_COUNT_RELATION_VERSION,
            catalog.schema_fingerprint.as_str(),
            &input,
        ))
        .await
        .unwrap();

    let recovered = RecoveredRuntime::recover_with_owner_and_relation_catalog_record(
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
async fn checked_catalog_backed_recovery_reads_catalog_with_valid_capabilities() {
    let (_temp_dir, store) = temp_store();
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();
    let input = input_batch([input_delta("account-a", 4, 1)]);
    IngestLog::new(Arc::clone(&store))
        .append_catalog_validated_envelope(ingest_envelope_bytes(
            ORDERS_SUM_COUNT_RELATION_VERSION,
            catalog.schema_fingerprint.as_str(),
            &input,
        ))
        .await
        .unwrap();

    let recovered = RecoveredRuntime::recover_with_owner_and_relation_catalog_record_checked(
        Arc::clone(&store),
        ORDERS_SUM_COUNT_OWNER,
        ORDERS_SUM_COUNT_RELATION_ID,
        ORDERS_SUM_COUNT_RELATION_VERSION,
        &local_capabilities(),
    )
    .await
    .unwrap();

    assert_eq!(recovered.replayed_batch_count(), 1);
    assert_eq!(recovered.logical_epoch(), 1);
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
async fn checked_selected_checkpoint_recovery_replays_catalog_aware_ingest() {
    let (_temp_dir, store) = temp_store();
    let catalog = orders_sum_count_relation_catalog().unwrap();
    RelationCatalogRegistry::new(Arc::clone(&store))
        .create(&catalog)
        .await
        .unwrap();

    let ingest_log = IngestLog::new(Arc::clone(&store));
    let publisher = CheckpointPublisher::new(Arc::clone(&store));
    let checkpoint_version = 0;
    let checkpoint_state = aggregate_state("account-a", 4, 1);
    let replay_input = input_batch([input_delta("account-a", 3, 1)]);

    ingest_log
        .append_catalog_validated_envelope(ingest_envelope_bytes_with_batches(
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
        ))
        .await
        .unwrap();
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
        &local_capabilities(),
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
}

#[tokio::test]
async fn catalog_backed_recovery_fails_closed_when_catalog_record_is_missing() {
    let (_temp_dir, store) = temp_store();

    let error = RecoveredRuntime::recover_with_owner_and_relation_catalog_record(
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

    let error = RecoveredRuntime::recover_with_owner_and_relation_catalog_record(
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

    let error = RecoveredRuntime::recover_with_owner_and_relation_catalog_record(
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
