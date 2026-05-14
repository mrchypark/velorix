use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{gc::GarbageCollectionPolicy, manifest::CheckpointManifest, object_key::ObjectKey};

pub const LATEST_CANDIDATE_SCHEMA_VERSION: u16 = 1;
pub const CHECKPOINT_LIFECYCLE_SCHEMA_VERSION: u16 = 1;
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
