use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use object_store::{local::LocalFileSystem, ObjectStore};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use velorix_core::relation::{
    ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
    FelderaRelationBindingV1, IncrementalAdapterBindingV1, RelationColumnV1, RelationOperationV1,
    RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1, VelorixRelationCatalogV1,
    VelorixRelationSchemaV1, RELATION_SCHEMA_VERSION_V1,
};
use velorix_storage::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
        ObjectStoreCapabilityProfile,
    },
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::{AppendValidatedEnvelopeOutcome, IngestAdmissionCoordinator, IngestLog},
    object_key::ObjectKey,
    relation_catalog_registry::RelationCatalogRegistry,
};

const CHILD_MODE_ENV: &str = "VELORIX_STORAGE_MULTI_PROCESS_INGEST_CHILD";
const AUTHORITY_ROOT_ENV: &str = "VELORIX_STORAGE_MULTI_PROCESS_AUTHORITY_ROOT";
const START_MARKER_ENV: &str = "VELORIX_STORAGE_MULTI_PROCESS_START_MARKER";
const READY_FILE_ENV: &str = "VELORIX_STORAGE_MULTI_PROCESS_READY_FILE";
const OUTCOME_FILE_ENV: &str = "VELORIX_STORAGE_MULTI_PROCESS_OUTCOME_FILE";
const START_OFFSET_ENV: &str = "VELORIX_STORAGE_MULTI_PROCESS_START_OFFSET";
const END_OFFSET_ENV: &str = "VELORIX_STORAGE_MULTI_PROCESS_END_OFFSET";
const POST_START_DELAY_MS_ENV: &str = "VELORIX_STORAGE_MULTI_PROCESS_POST_START_DELAY_MS";

#[tokio::test]
async fn local_filesystem_serialized_multi_process_admission_rejects_one_overlapping_range() {
    let (_temp_dir, authority_root, store) = temp_authority_store();
    create_orders_relation_catalog(&store).await;
    let scratch = tempfile::tempdir().unwrap();
    let start_marker = scratch.path().join("start-overlap");

    let mut first = spawn_child(
        &authority_root,
        &start_marker,
        &scratch.path().join("first.ready"),
        &scratch.path().join("first.json"),
        0,
        100,
        0,
    );
    let mut overlapping = spawn_child(
        &authority_root,
        &start_marker,
        &scratch.path().join("overlapping.ready"),
        &scratch.path().join("overlapping.json"),
        50,
        150,
        // This proves a second OS process observes durable admission evidence
        // from the first process; it is not a simultaneous range-race proof.
        250,
    );

    release_children(
        &start_marker,
        &[
            scratch.path().join("first.ready"),
            scratch.path().join("overlapping.ready"),
        ],
    );
    assert_child_success(&mut first);
    assert_child_success(&mut overlapping);

    let outcomes = read_outcomes(&[
        scratch.path().join("first.json"),
        scratch.path().join("overlapping.json"),
    ]);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.kind == "appended")
            .count(),
        1,
        "{outcomes:?}"
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.kind == "conflict")
            .count(),
        1,
        "{outcomes:?}"
    );
    assert!(outcomes.iter().all(|outcome| outcome.error.is_none()));

    let appended = outcomes
        .iter()
        .find(|outcome| outcome.kind == "appended")
        .unwrap();
    let conflict = outcomes
        .iter()
        .find(|outcome| outcome.kind == "conflict")
        .unwrap();
    assert_eq!(conflict.conflict_object_key, appended.object_key);
    assert!(matches!(
        conflict.reason.as_deref(),
        Some("range_overlap_reserved" | "range_overlap_committed")
    ));

    let capabilities = complete_capabilities();
    let committed = IngestLog::new_catalog_checked(store, &capabilities)
        .unwrap()
        .list_committed()
        .await
        .unwrap();
    assert_eq!(committed.len(), 1);
}

#[tokio::test]
async fn local_filesystem_multi_process_admission_allows_adjacent_ranges() {
    let (_temp_dir, authority_root, store) = temp_authority_store();
    create_orders_relation_catalog(&store).await;
    let scratch = tempfile::tempdir().unwrap();
    let start_marker = scratch.path().join("start-adjacent");

    let mut first = spawn_child(
        &authority_root,
        &start_marker,
        &scratch.path().join("first.ready"),
        &scratch.path().join("first.json"),
        0,
        100,
        0,
    );
    let mut adjacent = spawn_child(
        &authority_root,
        &start_marker,
        &scratch.path().join("adjacent.ready"),
        &scratch.path().join("adjacent.json"),
        100,
        150,
        0,
    );

    release_children(
        &start_marker,
        &[
            scratch.path().join("first.ready"),
            scratch.path().join("adjacent.ready"),
        ],
    );
    assert_child_success(&mut first);
    assert_child_success(&mut adjacent);

    let outcomes = read_outcomes(&[
        scratch.path().join("first.json"),
        scratch.path().join("adjacent.json"),
    ]);
    assert!(outcomes
        .iter()
        .all(|outcome| outcome.kind == "appended" && outcome.error.is_none()),);

    let capabilities = complete_capabilities();
    let committed = IngestLog::new_catalog_checked(store, &capabilities)
        .unwrap()
        .list_committed()
        .await
        .unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(
        committed
            .iter()
            .map(|descriptor| descriptor.object_key.to_string())
            .collect::<Vec<_>>(),
        vec![
            ObjectKey::ingest_batch("orders", 7, 0, 100)
                .unwrap()
                .to_string(),
            ObjectKey::ingest_batch("orders", 7, 100, 150)
                .unwrap()
                .to_string(),
        ]
    );
}

#[test]
fn multi_process_ingest_admission_child() {
    if env::var_os(CHILD_MODE_ENV).is_none() {
        return;
    }

    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(run_child());
}

async fn run_child() {
    let authority_root = path_from_env(AUTHORITY_ROOT_ENV);
    let start_marker = path_from_env(START_MARKER_ENV);
    let ready_file = path_from_env(READY_FILE_ENV);
    let outcome_file = path_from_env(OUTCOME_FILE_ENV);
    let start_offset = integer_from_env(START_OFFSET_ENV);
    let end_offset = integer_from_env(END_OFFSET_ENV);
    let post_start_delay_ms = integer_from_env(POST_START_DELAY_MS_ENV);

    fs::write(&ready_file, b"ready").unwrap();
    wait_for_file(&start_marker);
    if post_start_delay_ms > 0 {
        thread::sleep(Duration::from_millis(post_start_delay_ms));
    }

    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&authority_root).unwrap());
    let capabilities = complete_capabilities();
    let coordinator = IngestAdmissionCoordinator::new_checked(store, &capabilities).unwrap();
    let payload = catalog_envelope_bytes_for(&orders_relation_catalog(), start_offset, end_offset);
    let outcome = match coordinator.append_catalog_validated_envelope(payload).await {
        Ok(AppendValidatedEnvelopeOutcome::Appended { descriptor }) => {
            ChildOutcome::for_descriptor("appended", descriptor.object_key.to_string())
        }
        Ok(AppendValidatedEnvelopeOutcome::Duplicate { descriptor }) => {
            ChildOutcome::for_descriptor("duplicate", descriptor.object_key.to_string())
        }
        Ok(AppendValidatedEnvelopeOutcome::Conflict {
            descriptor,
            object_key,
            reason,
        }) => ChildOutcome {
            kind: "conflict".to_string(),
            object_key: Some(descriptor.object_key.to_string()),
            conflict_object_key: Some(object_key.to_string()),
            reason: Some(reason.to_string()),
            error: None,
        },
        Err(error) => ChildOutcome {
            kind: "error".to_string(),
            object_key: None,
            conflict_object_key: None,
            reason: None,
            error: Some(error.to_string()),
        },
    };

    fs::write(outcome_file, serde_json::to_vec(&outcome).unwrap()).unwrap();
}

#[derive(Debug, Serialize, Deserialize)]
struct ChildOutcome {
    kind: String,
    object_key: Option<String>,
    conflict_object_key: Option<String>,
    reason: Option<String>,
    error: Option<String>,
}

impl ChildOutcome {
    fn for_descriptor(kind: &str, object_key: String) -> Self {
        Self {
            kind: kind.to_string(),
            object_key: Some(object_key),
            conflict_object_key: None,
            reason: None,
            error: None,
        }
    }
}

fn spawn_child(
    authority_root: &Path,
    start_marker: &Path,
    ready_file: &Path,
    outcome_file: &Path,
    start_offset: u64,
    end_offset: u64,
    post_start_delay_ms: u64,
) -> Child {
    Command::new(env::current_exe().unwrap())
        .arg("--exact")
        .arg("multi_process_ingest_admission_child")
        .arg("--nocapture")
        .env(CHILD_MODE_ENV, "1")
        .env(AUTHORITY_ROOT_ENV, authority_root)
        .env(START_MARKER_ENV, start_marker)
        .env(READY_FILE_ENV, ready_file)
        .env(OUTCOME_FILE_ENV, outcome_file)
        .env(START_OFFSET_ENV, start_offset.to_string())
        .env(END_OFFSET_ENV, end_offset.to_string())
        .env(POST_START_DELAY_MS_ENV, post_start_delay_ms.to_string())
        .spawn()
        .unwrap()
}

fn release_children(start_marker: &Path, ready_files: &[PathBuf]) {
    for ready_file in ready_files {
        wait_for_file(ready_file);
    }
    fs::write(start_marker, b"start").unwrap();
}

fn read_outcomes(outcome_files: &[PathBuf]) -> Vec<ChildOutcome> {
    outcome_files
        .iter()
        .map(|path| serde_json::from_slice(&fs::read(path).unwrap()).unwrap())
        .collect()
}

fn assert_child_success(child: &mut Child) {
    let status = child.wait().unwrap();
    assert!(status.success(), "child exited with {status}");
}

fn wait_for_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn path_from_env(name: &str) -> PathBuf {
    env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("{name} is required"))
}

fn integer_from_env(name: &str) -> u64 {
    env::var(name)
        .unwrap_or_else(|_| panic!("{name} is required"))
        .parse()
        .unwrap_or_else(|error| panic!("{name} must be an integer: {error}"))
}

fn temp_authority_store() -> (TempDir, PathBuf, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let authority_root = temp_dir.path().to_path_buf();
    let store = LocalFileSystem::new_with_prefix(&authority_root).unwrap();

    (temp_dir, authority_root, Arc::new(store))
}

async fn create_orders_relation_catalog(store: &Arc<dyn ObjectStore>) {
    let capabilities = complete_capabilities();
    RelationCatalogRegistry::new_checked(
        Arc::clone(store),
        capabilities
            .validate_namespace(AuthoritativeNamespace::RelationCatalog)
            .unwrap(),
    )
    .unwrap()
    .create(&orders_relation_catalog())
    .await
    .unwrap();
}

fn complete_capabilities() -> AuthoritativeObjectStoreCapabilitiesV1 {
    AuthoritativeObjectStoreCapabilitiesV1::new(
        AuthoritativeNamespace::all()
            .into_iter()
            .map(|namespace| {
                (
                    namespace,
                    ObjectStoreCapabilityProfile {
                        backend_name: format!("local-multiprocess-{namespace}"),
                        conditional_create: true,
                        atomic_visibility: true,
                        list_after_write: true,
                        read_after_write: true,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>(),
    )
}

fn catalog_envelope_bytes_for(
    catalog: &VelorixRelationCatalogV1,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
) -> Bytes {
    IngestEnvelope::encode_batches(
        IngestEnvelopeEncodeRequest {
            relation_id: catalog.relation_schema.relation_id.clone(),
            relation_version: catalog.relation_schema.relation_version.clone(),
            schema_fingerprint: catalog.schema_fingerprint.as_str().to_string(),
            stream_id: "orders".to_string(),
            partition_id: 7,
            start_offset_inclusive,
            end_offset_exclusive,
        },
        &[valid_batch()],
    )
    .unwrap()
}

fn valid_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("account_id", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("weight", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["acct-1", "acct-2"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, -1])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn orders_relation_catalog() -> VelorixRelationCatalogV1 {
    let relation_schema = VelorixRelationSchemaV1 {
        relation_id: "orders_relation".to_string(),
        relation_name: "orders".to_string(),
        relation_version: "2026-05-05".to_string(),
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
            relation_id: "orders_relation".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: "incremental-adapter-orders-sum-count-v1".to_string(),
        },
    }
}
