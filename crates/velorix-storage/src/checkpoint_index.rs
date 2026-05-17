use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{gc::GarbageCollectionPolicy, manifest::CheckpointManifest, object_key::ObjectKey};

pub const LATEST_CANDIDATE_SCHEMA_VERSION: u16 = 1;
pub const CHECKPOINT_LIFECYCLE_SCHEMA_VERSION: u16 = 1;
pub const CHECKPOINT_GC_TRANSITION_SCHEMA_VERSION: u16 = 1;
pub const CHECKPOINT_RETENTION_SCHEMA_VERSION: u16 = 1;
pub const CHECKPOINT_RECOVERY_TRANSITION_SCHEMA_VERSION: u16 = 1;

static RECOVERY_TRANSITION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatestCandidateMarker {
    pub schema_version: u16,
    pub checkpoint_version: u64,
    pub manifest_key: ObjectKey,
    pub manifest_digest: String,
    pub validated_parent_checkpoint: Option<u64>,
    pub updated_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointLifecycleRecord {
    pub schema_version: u16,
    pub checkpoint_version: u64,
    pub manifest_key: ObjectKey,
    pub manifest_digest: String,
    pub status: CheckpointLifecycleStatus,
    pub status_updated_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRetentionRecordV1 {
    pub schema_version: u16,
    pub checkpoint_version: u64,
    pub manifest_key: ObjectKey,
    pub manifest_digest: String,
    pub gc_run_id: String,
    pub policy: GarbageCollectionPolicy,
    pub retained_manifest_versions: Vec<u64>,
    pub deleted_candidate_keys: Vec<ObjectKey>,
    pub retained_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointGcTransitionRecordV1 {
    pub schema_version: u16,
    pub checkpoint_version: u64,
    pub transition_id: String,
    pub manifest_key: ObjectKey,
    pub manifest_digest: String,
    pub transition: CheckpointGcTransition,
    pub gc_run_id: String,
    pub gc_run_key: ObjectKey,
    pub gc_run_digest: String,
    pub retention_record_key: ObjectKey,
    pub retention_record_digest: String,
    pub retained_manifest_versions: Vec<u64>,
    pub released_payload_keys: Vec<ObjectKey>,
    pub created_at: String,
    pub emitter: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRecoveryTransitionRecordV1 {
    pub schema_version: u16,
    pub checkpoint_version: u64,
    pub transition_id: String,
    pub manifest_key: ObjectKey,
    pub manifest_digest: String,
    pub recovery_mode: CheckpointRecoveryMode,
    pub replay_checkpoint_count: usize,
    pub replayed_batch_count: usize,
    pub recovered_at: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointLifecycleStatus {
    Published,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointGcTransition {
    PayloadReleased,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointRecoveryMode {
    LatestCandidate,
    SelectedCheckpoint,
    SlateDbLatest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointAdminInspection {
    pub latest_valid_checkpoint: Option<u64>,
    pub manifests: Vec<CheckpointManifestInspection>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointManifestInspection {
    pub checkpoint_version: u64,
    pub manifest_key: ObjectKey,
    pub lifecycle_status: Option<CheckpointLifecycleStatus>,
    pub gc_transition_records: Vec<CheckpointGcTransitionRecordV1>,
    pub retention_record: Option<CheckpointRetentionRecordV1>,
    pub recovery_transition_records: Vec<CheckpointRecoveryTransitionRecordV1>,
    pub status: CheckpointManifestInspectionStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointManifestInspectionStatus {
    Valid,
    Invalid { reason: String },
}

impl LatestCandidateMarker {
    pub fn for_manifest(
        manifest: &CheckpointManifest,
        manifest_bytes: &[u8],
        updated_at: String,
    ) -> Self {
        Self {
            schema_version: LATEST_CANDIDATE_SCHEMA_VERSION,
            checkpoint_version: manifest.checkpoint_version,
            manifest_key: manifest.object_key(),
            manifest_digest: manifest_digest(manifest_bytes),
            validated_parent_checkpoint: manifest.parent_checkpoint,
            updated_at,
        }
    }

    pub fn validate_schema(&self) -> bool {
        self.schema_version == LATEST_CANDIDATE_SCHEMA_VERSION
    }
}

impl CheckpointLifecycleRecord {
    pub fn published(
        manifest: &CheckpointManifest,
        manifest_bytes: &[u8],
        status_updated_at: String,
    ) -> Self {
        Self {
            schema_version: CHECKPOINT_LIFECYCLE_SCHEMA_VERSION,
            checkpoint_version: manifest.checkpoint_version,
            manifest_key: manifest.object_key(),
            manifest_digest: manifest_digest(manifest_bytes),
            status: CheckpointLifecycleStatus::Published,
            status_updated_at,
        }
    }

    pub fn validate_schema(&self) -> bool {
        self.schema_version == CHECKPOINT_LIFECYCLE_SCHEMA_VERSION
    }
}

impl CheckpointRetentionRecordV1 {
    pub fn for_manifest(
        manifest: &CheckpointManifest,
        manifest_bytes: &[u8],
        gc_run_id: String,
        policy: GarbageCollectionPolicy,
        retained_manifest_versions: Vec<u64>,
        deleted_candidate_keys: Vec<ObjectKey>,
        retained_at: String,
    ) -> Self {
        Self {
            schema_version: CHECKPOINT_RETENTION_SCHEMA_VERSION,
            checkpoint_version: manifest.checkpoint_version,
            manifest_key: manifest.object_key(),
            manifest_digest: manifest_digest(manifest_bytes),
            gc_run_id,
            policy,
            retained_manifest_versions,
            deleted_candidate_keys,
            retained_at,
        }
    }

    pub fn validate_schema(&self) -> bool {
        self.schema_version == CHECKPOINT_RETENTION_SCHEMA_VERSION
    }
}

impl CheckpointGcTransitionRecordV1 {
    pub fn payload_released_from_retention_record(
        retention: &CheckpointRetentionRecordV1,
        transition_id: String,
        gc_run_digest: String,
        retention_record_digest: String,
        created_at: String,
        emitter: String,
    ) -> Self {
        Self {
            schema_version: CHECKPOINT_GC_TRANSITION_SCHEMA_VERSION,
            checkpoint_version: retention.checkpoint_version,
            transition_id,
            manifest_key: retention.manifest_key.clone(),
            manifest_digest: retention.manifest_digest.clone(),
            transition: CheckpointGcTransition::PayloadReleased,
            gc_run_id: retention.gc_run_id.clone(),
            gc_run_key: ObjectKey::garbage_collection_run(&retention.gc_run_id)
                .expect("validated retention records have valid GC run ids"),
            gc_run_digest,
            retention_record_key: ObjectKey::checkpoint_retention_record(
                retention.checkpoint_version,
            ),
            retention_record_digest,
            retained_manifest_versions: retention.retained_manifest_versions.clone(),
            released_payload_keys: retention.deleted_candidate_keys.clone(),
            created_at,
            emitter,
        }
    }

    pub fn validate_schema(&self) -> bool {
        self.schema_version == CHECKPOINT_GC_TRANSITION_SCHEMA_VERSION
    }
}

impl CheckpointRecoveryTransitionRecordV1 {
    pub fn for_manifest(
        manifest: &CheckpointManifest,
        manifest_bytes: &[u8],
        transition_id: String,
        recovery_mode: CheckpointRecoveryMode,
        replay_checkpoint_count: usize,
        replayed_batch_count: usize,
        recovered_at: String,
    ) -> Self {
        Self {
            schema_version: CHECKPOINT_RECOVERY_TRANSITION_SCHEMA_VERSION,
            checkpoint_version: manifest.checkpoint_version,
            transition_id,
            manifest_key: manifest.object_key(),
            manifest_digest: manifest_digest(manifest_bytes),
            recovery_mode,
            replay_checkpoint_count,
            replayed_batch_count,
            recovered_at,
        }
    }

    pub fn validate_schema(&self) -> bool {
        self.schema_version == CHECKPOINT_RECOVERY_TRANSITION_SCHEMA_VERSION
    }
}

impl CheckpointManifestInspectionStatus {
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Valid => None,
            Self::Invalid { reason } => Some(reason),
        }
    }
}

pub fn manifest_body_digest(manifest_bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"velorix.checkpoint-manifest.v1\0");
    hasher.update(manifest_bytes);

    format!("sha256:{:x}", hasher.finalize())
}

pub(crate) fn manifest_digest(manifest_bytes: &[u8]) -> String {
    manifest_body_digest(manifest_bytes)
}

pub(crate) fn marker_updated_at_now() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    format!("unix:{}.{:09}", duration.as_secs(), duration.subsec_nanos())
}

pub fn recovery_transition_id_now() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let sequence = RECOVERY_TRANSITION_SEQUENCE.fetch_add(1, Ordering::Relaxed);

    format!(
        "recovery-{}-{:09}-{sequence:016}",
        duration.as_secs(),
        duration.subsec_nanos()
    )
}
