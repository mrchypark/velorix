use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{manifest::CheckpointManifest, object_key::ObjectKey};

pub const LATEST_CANDIDATE_SCHEMA_VERSION: u16 = 1;
pub const CHECKPOINT_LIFECYCLE_SCHEMA_VERSION: u16 = 1;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointLifecycleStatus {
    Published,
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

impl CheckpointManifestInspectionStatus {
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Valid => None,
            Self::Invalid { reason } => Some(reason),
        }
    }
}

pub(crate) fn manifest_digest(manifest_bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"velorix.checkpoint-manifest.v1\0");
    hasher.update(manifest_bytes);

    format!("sha256:{:x}", hasher.finalize())
}

pub(crate) fn marker_updated_at_now() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    format!("unix:{}.{:09}", duration.as_secs(), duration.subsec_nanos())
}
