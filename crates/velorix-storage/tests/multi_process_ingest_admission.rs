#[cfg(feature = "s3-compat-tests")]
use std::time::{SystemTime, UNIX_EPOCH};
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
use futures::TryStreamExt;
#[cfg(feature = "s3-compat-tests")]
use object_store::{aws::AmazonS3Builder, prefix::PrefixStore};
use object_store::{
    local::LocalFileSystem, path::Path as ObjectStorePath, ObjectStore, ObjectStoreExt, PutMode,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::TempDir;
use velorix_core::relation::{
    ArrowPhysicalTypeV1, DataFusionRegistrationModeV1, DataFusionRegistrationV1,
    IncrementalAdapterBindingV1, IncrementalRelationBindingV1, RelationColumnV1,
    RelationOperationV1, RelationSemanticRoleV1, SchemaFingerprintV1, VelorixLogicalTypeV1,
    VelorixRelationCatalogV1, VelorixRelationSchemaV1, VelorixRelationSourceV1,
    RELATION_SCHEMA_VERSION_V1,
};
use velorix_storage::{
    capability::{
        AuthoritativeNamespace, AuthoritativeObjectStoreCapabilitiesV1,
        ObjectStoreCapabilityProfile,
    },
    ingest_envelope::{IngestEnvelope, IngestEnvelopeEncodeRequest},
    log::{
        AppendValidatedEnvelopeOutcome, DurableIngestAdmissionRecordV1, IngestAdmissionCoordinator,
        IngestBatch, IngestLog,
    },
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
const STORE_BACKEND_ENV: &str = "VELORIX_STORAGE_MULTI_PROCESS_STORE_BACKEND";
#[cfg(feature = "s3-compat-tests")]
const S3_AUTHORITY_PREFIX_ENV: &str = "VELORIX_STORAGE_MULTI_PROCESS_S3_AUTHORITY_PREFIX";

#[tokio::test]
async fn local_filesystem_simultaneous_multi_process_admission_rejects_one_overlapping_range() {
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
        0,
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
    assert_eq!(conflict.reason.as_deref(), Some("range_overlap_reserved"));

    let capabilities = complete_capabilities();
    assert_eq!(index_transition_bodies(&store).await.len(), 1);
    let committed = IngestLog::new_catalog_checked(Arc::clone(&store), &capabilities)
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
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.kind == "appended" && outcome.error.is_none()),
        "{outcomes:?}"
    );

    let capabilities = complete_capabilities();
    let committed = IngestLog::new_catalog_checked(Arc::clone(&store), &capabilities)
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
    assert_chained_index_transitions(
        &index_transition_bodies(&store).await,
        &[(0, 100), (100, 150)],
    );
}

#[tokio::test]
async fn local_filesystem_reconstruction_fails_closed_when_index_transition_lacks_materialized_admission(
) {
    let (_temp_dir, authority_root, store) = temp_authority_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let capabilities = complete_capabilities();
    let coordinator =
        IngestAdmissionCoordinator::new_checked(Arc::clone(&store), &capabilities).unwrap();

    coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 0, 100))
        .await
        .unwrap();
    fs::remove_file(
        authority_root.join(
            ObjectKey::ingest_batch("orders", 7, 0, 100)
                .unwrap()
                .as_str(),
        ),
    )
    .unwrap();
    fs::remove_file(
        authority_root.join(
            ObjectKey::ingest_admission_record("orders", 7, 0, 100)
                .unwrap()
                .as_str(),
        ),
    )
    .unwrap();

    let error = coordinator
        .reconstruct_active_admissions()
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("indexed transition is missing materialized admission"),
        "{error}"
    );
}

#[tokio::test]
async fn local_filesystem_indexed_admission_reports_reserved_overlap_before_committed_fallback() {
    let (_temp_dir, _authority_root, store) = temp_authority_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let capabilities = complete_capabilities();
    let coordinator =
        IngestAdmissionCoordinator::new_checked(Arc::clone(&store), &capabilities).unwrap();

    coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 0, 100))
        .await
        .unwrap();
    let outcome = coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 50, 150))
        .await
        .unwrap();

    let AppendValidatedEnvelopeOutcome::Conflict { reason, .. } = outcome else {
        panic!("expected conflict, got {outcome:?}");
    };
    assert_eq!(reason, "range_overlap_reserved");
}

#[tokio::test]
async fn local_filesystem_checked_overlap_with_legacy_committed_does_not_poison_adjacent_append() {
    let (_temp_dir, _authority_root, store) = temp_authority_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let capabilities = complete_capabilities();
    let legacy_log = IngestLog::new_catalog_checked(Arc::clone(&store), &capabilities).unwrap();
    legacy_log
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 0, 100))
        .await
        .unwrap();

    let coordinator =
        IngestAdmissionCoordinator::new_checked(Arc::clone(&store), &capabilities).unwrap();
    let overlap = coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 50, 150))
        .await
        .unwrap();
    assert!(matches!(
        overlap,
        AppendValidatedEnvelopeOutcome::Conflict {
            reason: "range_overlap_committed",
            ..
        }
    ));
    assert!(index_transition_bodies(&store).await.is_empty());

    let adjacent = coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 100, 150))
        .await
        .unwrap();
    assert!(matches!(
        adjacent,
        AppendValidatedEnvelopeOutcome::Appended { descriptor }
            if descriptor.object_key == ObjectKey::ingest_batch("orders", 7, 100, 150).unwrap()
    ));
    assert_chained_index_transitions(&index_transition_bodies(&store).await, &[(100, 150)]);
}

#[tokio::test]
async fn local_filesystem_expiring_legacy_orphan_preserves_index_history_digest_for_later_append() {
    let (_temp_dir, _authority_root, store) = temp_authority_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let capabilities = complete_capabilities();
    put_legacy_materialized_admission(&store, &catalog, 0, 100).await;

    let coordinator =
        IngestAdmissionCoordinator::new_checked(Arc::clone(&store), &capabilities).unwrap();
    let adjacent = coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 100, 150))
        .await
        .unwrap();
    assert!(matches!(
        adjacent,
        AppendValidatedEnvelopeOutcome::Appended { descriptor }
            if descriptor.object_key == ObjectKey::ingest_batch("orders", 7, 100, 150).unwrap()
    ));

    coordinator
        .expire_orphan_admission(
            "orders",
            7,
            0,
            100,
            "legacy-orphan-expiry",
            "legacy_orphan_expired",
            "multi-process-test",
        )
        .await
        .unwrap();
    coordinator.reconstruct_active_admissions().await.unwrap();

    let next = coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 150, 200))
        .await
        .unwrap();
    assert!(matches!(
        next,
        AppendValidatedEnvelopeOutcome::Appended { descriptor }
            if descriptor.object_key == ObjectKey::ingest_batch("orders", 7, 150, 200).unwrap()
    ));
    assert_chained_index_transitions(
        &index_transition_bodies(&store).await,
        &[(100, 150), (150, 200)],
    );
}

#[tokio::test]
async fn local_filesystem_stale_retry_of_expired_indexed_orphan_does_not_write_transition() {
    let (_temp_dir, authority_root, store) = temp_authority_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let capabilities = complete_capabilities();
    let coordinator =
        IngestAdmissionCoordinator::new_checked(Arc::clone(&store), &capabilities).unwrap();

    coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 0, 100))
        .await
        .unwrap();
    fs::remove_file(
        authority_root.join(
            ObjectKey::ingest_batch("orders", 7, 0, 100)
                .unwrap()
                .as_str(),
        ),
    )
    .unwrap();
    coordinator
        .expire_orphan_admission(
            "orders",
            7,
            0,
            100,
            "indexed-orphan-expiry",
            "indexed_orphan_expired",
            "multi-process-test",
        )
        .await
        .unwrap();
    let transitions_before = index_transition_bodies(&store).await;
    assert_eq!(transitions_before.len(), 1);

    let stale_retry = coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 0, 100))
        .await
        .unwrap();

    assert!(matches!(
        stale_retry,
        AppendValidatedEnvelopeOutcome::Conflict {
            reason: "admission_expired",
            ..
        }
    ));
    assert_eq!(index_transition_bodies(&store).await, transitions_before);
}

#[tokio::test]
async fn local_filesystem_reconstruction_ignores_unreachable_index_transition_when_head_chain_is_valid(
) {
    let (_temp_dir, _authority_root, store) = temp_authority_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let capabilities = complete_capabilities();
    let coordinator =
        IngestAdmissionCoordinator::new_checked(Arc::clone(&store), &capabilities).unwrap();

    coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 0, 100))
        .await
        .unwrap();
    let mut stale_transition = index_transition_bodies(&store)
        .await
        .into_iter()
        .next()
        .unwrap();
    let stale_previous_digest_hex = "1".repeat(64);
    stale_transition["previous_state_digest"] =
        Value::String(format!("sha256:{stale_previous_digest_hex}"));
    stale_transition["next_state_digest"] = Value::String(format!("sha256:{}", "2".repeat(64)));
    store
        .put_opts(
            &ObjectStorePath::from(format!(
                "v1/ingest-admission-index/orders/p=0000000007/advances/{stale_previous_digest_hex}.transition.json"
            )),
            Bytes::from(serde_json::to_vec(&stale_transition).unwrap()).into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();

    let report = coordinator.reconstruct_active_admissions().await.unwrap();
    assert_eq!(report.active_admission_records, 1);

    let adjacent = coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 100, 150))
        .await
        .unwrap();
    assert!(matches!(
        adjacent,
        AppendValidatedEnvelopeOutcome::Appended { descriptor }
            if descriptor.object_key == ObjectKey::ingest_batch("orders", 7, 100, 150).unwrap()
    ));
}

#[tokio::test]
async fn local_filesystem_reconstruction_fails_closed_when_materialized_admission_differs_from_index(
) {
    let (_temp_dir, authority_root, store) = temp_authority_store();
    let catalog = create_orders_relation_catalog(&store).await;
    let capabilities = complete_capabilities();
    let coordinator =
        IngestAdmissionCoordinator::new_checked(Arc::clone(&store), &capabilities).unwrap();

    coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 0, 100))
        .await
        .unwrap();
    let admission_path = authority_root.join(
        ObjectKey::ingest_admission_record("orders", 7, 0, 100)
            .unwrap()
            .as_str(),
    );
    let materialized: Value = serde_json::from_slice(&fs::read(&admission_path).unwrap()).unwrap();
    fs::write(
        &admission_path,
        serde_json::to_vec_pretty(&materialized).unwrap(),
    )
    .unwrap();

    let error = coordinator
        .reconstruct_active_admissions()
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("materialized admission does not match indexed transition"),
        "{error}"
    );
}

#[cfg(feature = "s3-compat-tests")]
#[tokio::test]
async fn s3_compatible_multi_process_admission_rejects_overlap_and_allows_adjacent_ranges() {
    let Some(config) = live_config() else {
        println!(
            "skipping S3-compatible multi-process ingest admission harness; set VELORIX_S3_COMPAT=1 and configure AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, and VELORIX_S3_BUCKET to enable"
        );
        return;
    };

    let overlap_config = config.scenario("overlap");
    let overlap_store = live_authority_store(&overlap_config).unwrap();
    create_orders_relation_catalog(&overlap_store).await;
    let overlap_outcomes = run_s3_children(
        &overlap_config,
        "s3-overlap",
        &[(0, 100, 0, "first"), (50, 150, 0, "overlapping")],
    );
    assert_eq!(
        overlap_outcomes
            .iter()
            .filter(|outcome| outcome.kind == "appended")
            .count(),
        1,
        "{overlap_outcomes:?}"
    );
    assert_eq!(
        overlap_outcomes
            .iter()
            .filter(|outcome| outcome.kind == "conflict")
            .count(),
        1,
        "{overlap_outcomes:?}"
    );
    assert!(overlap_outcomes
        .iter()
        .all(|outcome| outcome.error.is_none()));

    let appended = overlap_outcomes
        .iter()
        .find(|outcome| outcome.kind == "appended")
        .unwrap();
    let conflict = overlap_outcomes
        .iter()
        .find(|outcome| outcome.kind == "conflict")
        .unwrap();
    assert_eq!(conflict.conflict_object_key, appended.object_key);
    assert_eq!(conflict.reason.as_deref(), Some("range_overlap_reserved"));

    let capabilities = complete_capabilities();
    assert_eq!(index_transition_bodies(&overlap_store).await.len(), 1);
    let committed = IngestLog::new_catalog_checked(Arc::clone(&overlap_store), &capabilities)
        .unwrap()
        .list_committed()
        .await
        .unwrap();
    assert_eq!(committed.len(), 1);

    let adjacent_config = config.scenario("adjacent");
    let adjacent_store = live_authority_store(&adjacent_config).unwrap();
    create_orders_relation_catalog(&adjacent_store).await;
    let adjacent_outcomes = run_s3_children(
        &adjacent_config,
        "s3-adjacent",
        &[(0, 100, 0, "first"), (100, 150, 0, "adjacent")],
    );
    assert!(adjacent_outcomes
        .iter()
        .all(|outcome| outcome.kind == "appended" && outcome.error.is_none()),);

    assert_chained_index_transitions(
        &index_transition_bodies(&adjacent_store).await,
        &[(0, 100), (100, 150)],
    );
    let committed = IngestLog::new_catalog_checked(adjacent_store, &capabilities)
        .unwrap()
        .list_committed()
        .await
        .unwrap();
    assert_eq!(committed.len(), 2);
}

#[cfg(feature = "s3-compat-tests")]
#[tokio::test]
async fn s3_compatible_indexed_admission_orphan_expiry_survives_restart_and_blocks_stale_retry() {
    let Some(config) = live_config() else {
        println!(
            "skipping S3-compatible indexed ingest admission crash/restart harness; set VELORIX_S3_COMPAT=1 and configure AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, and VELORIX_S3_BUCKET to enable"
        );
        return;
    };

    let config = config.scenario("indexed-crash-restart");
    let store = live_authority_store(&config).unwrap();
    let catalog = create_orders_relation_catalog(&store).await;
    let capabilities = complete_capabilities();
    let coordinator =
        IngestAdmissionCoordinator::new_checked(Arc::clone(&store), &capabilities).unwrap();

    let appended = coordinator
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 0, 100))
        .await
        .unwrap();
    let AppendValidatedEnvelopeOutcome::Appended { descriptor } = appended else {
        panic!("expected initial append, got {appended:?}");
    };

    store
        .delete(&ObjectStorePath::from(descriptor.object_key.as_str()))
        .await
        .unwrap();

    let restarted =
        IngestAdmissionCoordinator::new_checked(Arc::clone(&store), &capabilities).unwrap();
    let report = restarted.reconstruct_active_admissions().await.unwrap();
    assert_eq!(report.active_admission_records, 1);
    assert_eq!(report.expired_orphan_admission_records, 0);

    let decision = restarted
        .expire_orphan_admission(
            "orders",
            7,
            0,
            100,
            "s3-indexed-crash-restart-expiry",
            "batch_append_failed_after_admission",
            "s3-compatible-ingest-admission-test",
        )
        .await
        .unwrap();
    store
        .get(&ObjectStorePath::from(
            decision.expiry_decision_key.as_str(),
        ))
        .await
        .unwrap();

    let restarted_after_expiry =
        IngestAdmissionCoordinator::new_checked(Arc::clone(&store), &capabilities).unwrap();
    let report = restarted_after_expiry
        .reconstruct_active_admissions()
        .await
        .unwrap();
    assert_eq!(report.active_admission_records, 0);
    assert_eq!(report.expired_orphan_admission_records, 1);
    let transitions_before_stale_retry = index_transition_bodies(&store).await;
    assert_eq!(transitions_before_stale_retry.len(), 1);

    let stale_retry = restarted_after_expiry
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 0, 100))
        .await
        .unwrap();
    assert!(matches!(
        stale_retry,
        AppendValidatedEnvelopeOutcome::Conflict {
            reason: "admission_expired",
            ..
        }
    ));
    assert_eq!(
        index_transition_bodies(&store).await,
        transitions_before_stale_retry
    );

    let adjacent = restarted_after_expiry
        .append_catalog_validated_envelope(catalog_envelope_bytes_for(&catalog, 100, 150))
        .await
        .unwrap();
    assert!(matches!(
        adjacent,
        AppendValidatedEnvelopeOutcome::Appended { descriptor }
            if descriptor.object_key == ObjectKey::ingest_batch("orders", 7, 100, 150).unwrap()
    ));
    assert_chained_index_transitions(
        &index_transition_bodies(&store).await,
        &[(0, 100), (100, 150)],
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
    let start_marker = path_from_env(START_MARKER_ENV);
    let ready_file = path_from_env(READY_FILE_ENV);
    let outcome_file = path_from_env(OUTCOME_FILE_ENV);
    let start_offset = integer_from_env(START_OFFSET_ENV);
    let end_offset = integer_from_env(END_OFFSET_ENV);
    let post_start_delay_ms = integer_from_env(POST_START_DELAY_MS_ENV);

    let store = child_authority_store();
    let capabilities = complete_capabilities();
    let coordinator = IngestAdmissionCoordinator::new_checked(store, &capabilities).unwrap();
    let payload = catalog_envelope_bytes_for(&orders_relation_catalog(), start_offset, end_offset);
    fs::write(&ready_file, b"ready").unwrap();
    wait_for_file(&start_marker);
    if post_start_delay_ms > 0 {
        thread::sleep(Duration::from_millis(post_start_delay_ms));
    }

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
        .env(STORE_BACKEND_ENV, "local")
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

#[cfg(feature = "s3-compat-tests")]
fn spawn_s3_child(
    config: &LiveConfig,
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
        .env(STORE_BACKEND_ENV, "s3")
        .env(S3_AUTHORITY_PREFIX_ENV, &config.run_prefix)
        .env(START_MARKER_ENV, start_marker)
        .env(READY_FILE_ENV, ready_file)
        .env(OUTCOME_FILE_ENV, outcome_file)
        .env(START_OFFSET_ENV, start_offset.to_string())
        .env(END_OFFSET_ENV, end_offset.to_string())
        .env(POST_START_DELAY_MS_ENV, post_start_delay_ms.to_string())
        .spawn()
        .unwrap()
}

#[cfg(feature = "s3-compat-tests")]
fn run_s3_children(
    config: &LiveConfig,
    marker_name: &str,
    ranges: &[(u64, u64, u64, &str)],
) -> Vec<ChildOutcome> {
    let scratch = tempfile::tempdir().unwrap();
    let start_marker = scratch.path().join(marker_name);
    let mut children = Vec::new();
    let mut ready_files = Vec::new();
    let mut outcome_files = Vec::new();

    for (start_offset, end_offset, post_start_delay_ms, name) in ranges {
        let ready_file = scratch.path().join(format!("{name}.ready"));
        let outcome_file = scratch.path().join(format!("{name}.json"));
        children.push(spawn_s3_child(
            config,
            &start_marker,
            &ready_file,
            &outcome_file,
            *start_offset,
            *end_offset,
            *post_start_delay_ms,
        ));
        ready_files.push(ready_file);
        outcome_files.push(outcome_file);
    }

    release_children(&start_marker, &ready_files);
    for child in &mut children {
        assert_child_success(child);
    }

    read_outcomes(&outcome_files)
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

async fn index_transition_bodies(store: &Arc<dyn ObjectStore>) -> Vec<Value> {
    let mut objects = store
        .list(Some(&ObjectStorePath::from("v1/ingest-admission-index")))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    objects.sort_by(|left, right| left.location.cmp(&right.location));

    let mut transitions = Vec::new();
    for object in objects {
        if !object.location.as_ref().ends_with(".transition.json") {
            continue;
        }
        let bytes = store
            .get(&object.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        transitions.push(serde_json::from_slice(&bytes).unwrap());
    }

    transitions
}

async fn put_legacy_materialized_admission(
    store: &Arc<dyn ObjectStore>,
    catalog: &VelorixRelationCatalogV1,
    start_offset_inclusive: u64,
    end_offset_exclusive: u64,
) {
    let payload = catalog_envelope_bytes_for(catalog, start_offset_inclusive, end_offset_exclusive);
    let batch = IngestBatch::from_validated_envelope(payload.clone()).unwrap();
    let descriptor = batch.descriptor();
    let envelope = IngestEnvelope::decode(payload).unwrap();
    let record = DurableIngestAdmissionRecordV1 {
        schema_version: 1,
        record_kind: "ingest_range_admission_v1".to_string(),
        stream_id: descriptor.stream_id.clone(),
        partition_id: descriptor.partition_id,
        start_offset_inclusive: descriptor.start_offset_inclusive,
        end_offset_exclusive: descriptor.end_offset_exclusive,
        event_time_watermark: None,
        batch_key: descriptor.object_key,
        admission_record_key: ObjectKey::ingest_admission_record(
            &descriptor.stream_id,
            descriptor.partition_id,
            descriptor.start_offset_inclusive,
            descriptor.end_offset_exclusive,
        )
        .unwrap(),
        payload_digest: envelope.header().payload_digest.clone(),
        relation_id: envelope.header().relation_id.clone(),
        relation_version: envelope.header().relation_version.clone(),
        schema_fingerprint: envelope.header().schema_fingerprint.clone(),
        admission_mode: "process_local_serialized".to_string(),
        commit_guard_binding: None,
    };
    store
        .put_opts(
            &ObjectStorePath::from(record.admission_record_key.as_str()),
            Bytes::from(serde_json::to_vec(&record).unwrap()).into(),
            PutMode::Create.into(),
        )
        .await
        .unwrap();
}

fn assert_chained_index_transitions(transitions: &[Value], expected_ranges: &[(u64, u64)]) {
    assert_eq!(transitions.len(), expected_ranges.len(), "{transitions:?}");
    let observed_ranges = transitions
        .iter()
        .map(|transition| {
            (
                transition["admitted"]["start_offset_inclusive"]
                    .as_u64()
                    .unwrap(),
                transition["admitted"]["end_offset_exclusive"]
                    .as_u64()
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    for expected_range in expected_ranges {
        assert!(
            observed_ranges.contains(expected_range),
            "missing {expected_range:?} in {observed_ranges:?}"
        );
    }

    let previous_digests = transitions
        .iter()
        .map(|transition| transition["previous_state_digest"].as_str().unwrap())
        .collect::<Vec<_>>();
    let next_digests = transitions
        .iter()
        .map(|transition| transition["next_state_digest"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(
        previous_digests
            .iter()
            .filter(|previous| !next_digests.contains(previous))
            .count()
            == 1,
        "expected one chain genesis: previous={previous_digests:?} next={next_digests:?}"
    );
    assert!(
        next_digests
            .iter()
            .filter(|next| !previous_digests.contains(next))
            .count()
            == 1,
        "expected one chain head: previous={previous_digests:?} next={next_digests:?}"
    );
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

fn child_authority_store() -> Arc<dyn ObjectStore> {
    match env::var(STORE_BACKEND_ENV).as_deref() {
        Ok("local") | Err(_) => {
            let authority_root = path_from_env(AUTHORITY_ROOT_ENV);
            Arc::new(LocalFileSystem::new_with_prefix(&authority_root).unwrap())
        }
        #[cfg(feature = "s3-compat-tests")]
        Ok("s3") => {
            let config = live_config_with_prefix(required_env(S3_AUTHORITY_PREFIX_ENV)).unwrap();
            live_authority_store(&config).unwrap()
        }
        Ok(other) => panic!("unsupported {STORE_BACKEND_ENV}: {other}"),
    }
}

fn temp_authority_store() -> (TempDir, PathBuf, Arc<dyn ObjectStore>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let authority_root = temp_dir.path().to_path_buf();
    let store = LocalFileSystem::new_with_prefix(&authority_root).unwrap();

    (temp_dir, authority_root, Arc::new(store))
}

#[cfg(feature = "s3-compat-tests")]
fn live_authority_store(config: &LiveConfig) -> object_store::Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_endpoint(config.endpoint.clone())
        .with_access_key_id(config.access_key_id.clone())
        .with_secret_access_key(config.secret_access_key.clone())
        .with_region(config.region.clone())
        .with_bucket_name(config.bucket.clone())
        .with_allow_http(config.allow_http)
        .build()?;

    Ok(Arc::new(PrefixStore::new(
        store,
        ObjectStorePath::from(config.run_prefix.clone()),
    )))
}

#[cfg(feature = "s3-compat-tests")]
#[derive(Clone)]
struct LiveConfig {
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    region: String,
    bucket: String,
    allow_http: bool,
    run_prefix: String,
}

#[cfg(feature = "s3-compat-tests")]
impl LiveConfig {
    fn scenario(&self, name: &str) -> Self {
        let mut config = self.clone();
        config.run_prefix = join_prefixes(&self.run_prefix, name);
        config
    }
}

#[cfg(feature = "s3-compat-tests")]
fn live_config() -> Option<LiveConfig> {
    if env::var("VELORIX_S3_COMPAT").ok().as_deref() != Some("1") {
        return None;
    }

    let required = [
        "AWS_ENDPOINT_URL",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_REGION",
        "VELORIX_S3_BUCKET",
    ];
    let missing = required
        .iter()
        .copied()
        .filter(|name| env::var(name).is_err())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        println!(
            "skipping S3-compatible multi-process ingest admission harness; missing {}",
            missing.join(", ")
        );
        return None;
    }

    let prefix = env::var("VELORIX_S3_PREFIX").unwrap_or_default();
    let run_prefix = join_prefixes(&prefix, &unique_run_prefix());
    Some(live_config_with_prefix(run_prefix).unwrap())
}

#[cfg(feature = "s3-compat-tests")]
fn live_config_with_prefix(run_prefix: String) -> Option<LiveConfig> {
    let endpoint = required_env("AWS_ENDPOINT_URL");
    let allow_http = endpoint.starts_with("http://");

    Some(LiveConfig {
        endpoint,
        access_key_id: required_env("AWS_ACCESS_KEY_ID"),
        secret_access_key: required_env("AWS_SECRET_ACCESS_KEY"),
        region: required_env("AWS_REGION"),
        bucket: required_env("VELORIX_S3_BUCKET"),
        allow_http,
        run_prefix,
    })
}

#[cfg(feature = "s3-compat-tests")]
fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} is required when VELORIX_S3_COMPAT=1"))
}

#[cfg(feature = "s3-compat-tests")]
fn unique_run_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after Unix epoch")
        .as_nanos();

    format!(
        "velorix-s3-compat-multi-process/{}-{nanos}",
        std::process::id()
    )
}

#[cfg(feature = "s3-compat-tests")]
fn join_prefixes(base: &str, run: &str) -> String {
    match base.trim_matches('/') {
        "" => run.to_string(),
        base => format!("{base}/{}", run.trim_matches('/')),
    }
}

async fn create_orders_relation_catalog(store: &Arc<dyn ObjectStore>) -> VelorixRelationCatalogV1 {
    let capabilities = complete_capabilities();
    let catalog = orders_relation_catalog();
    RelationCatalogRegistry::new_checked(
        Arc::clone(store),
        capabilities
            .validate_namespace(AuthoritativeNamespace::RelationCatalog)
            .unwrap(),
    )
    .unwrap()
    .create(&catalog)
    .await
    .unwrap();
    catalog
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
                        conditional_update: true,
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
            event_time_watermark: None,
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
        relation_source: VelorixRelationSourceV1::SourceRelation,
        schema_version: RELATION_SCHEMA_VERSION_V1,
        relation_schema,
        schema_fingerprint: schema_fingerprint.clone(),
        datafusion_registration: DataFusionRegistrationV1 {
            name: "orders".to_string(),
            mode: DataFusionRegistrationModeV1::Table,
        },
        incremental_relation: IncrementalRelationBindingV1 {
            relation_id: "orders_relation".to_string(),
            schema_fingerprint,
        },
        incremental_adapter: IncrementalAdapterBindingV1 {
            adapter_id: "incremental-adapter-orders-sum-count-v1".to_string(),
        },
    }
}
