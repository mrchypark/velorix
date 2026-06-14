use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::engine::LogicalEpoch;
use crate::feldera_artifact::RelationSchema;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StandingProgramIdentity {
    pub tenant_id: String,
    pub program_id: String,
    pub view_ids: Vec<String>,
    pub sql_hash: String,
    pub input_catalog_hash: String,
    pub output_schema_hash: String,
    pub compiler_identity: String,
    pub runtime_packages: Vec<FelderaRuntimePackageIdentity>,
    pub package_feature_set: Vec<String>,
    pub dbsp_runtime_compatibility: String,
    pub checkpoint_codec_identity: String,
    pub native_code_policy: NativeCodePolicy,
}

impl StandingProgramIdentity {
    pub fn validate(&self) -> Result<(), StandingProgramRuntimeError> {
        require_non_empty("tenant_id", &self.tenant_id)?;
        require_non_empty("program_id", &self.program_id)?;
        require_sha256("sql_hash", &self.sql_hash)?;
        require_sha256("input_catalog_hash", &self.input_catalog_hash)?;
        require_sha256("output_schema_hash", &self.output_schema_hash)?;
        require_non_empty("compiler_identity", &self.compiler_identity)?;
        require_non_empty(
            "dbsp_runtime_compatibility",
            &self.dbsp_runtime_compatibility,
        )?;
        require_non_empty("checkpoint_codec_identity", &self.checkpoint_codec_identity)?;
        if self.view_ids.is_empty() {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field: "view_ids" });
        }
        if self.runtime_packages.is_empty() {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "runtime_packages",
            });
        }
        for package in &self.runtime_packages {
            package.validate()?;
        }
        if let NativeCodePolicy::NativeCodeOrExternalDependenciesPresent { reason } =
            &self.native_code_policy
        {
            return Err(StandingProgramRuntimeError::UnsupportedNativeCodePolicy {
                reason: reason.clone(),
            });
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FelderaRuntimePackageIdentity {
    pub name: String,
    pub version: String,
}

impl FelderaRuntimePackageIdentity {
    fn validate(&self) -> Result<(), StandingProgramRuntimeError> {
        require_non_empty("runtime_packages.name", &self.name)?;
        require_non_empty("runtime_packages.version", &self.version)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum NativeCodePolicy {
    DisabledNoExternalDependencies,
    NativeCodeOrExternalDependenciesPresent { reason: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EpochIdempotencyKey(String);

impl EpochIdempotencyKey {
    pub fn new(value: impl Into<String>) -> Result<Self, StandingProgramRuntimeError> {
        let value = value.into();
        require_non_empty("idempotency_key", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct RelationInputBatch {
    pub relation_id: String,
    pub relation_version: String,
    pub schema_fingerprint: String,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
    pub batches: Vec<RecordBatch>,
}

#[derive(Clone, Debug)]
pub struct ViewOutputBatch {
    pub view_id: String,
    pub schema_fingerprint: String,
    pub batches: Vec<RecordBatch>,
}

#[derive(Clone, Debug)]
pub struct EpochCommit {
    pub logical_epoch: LogicalEpoch,
    pub idempotency_key: EpochIdempotencyKey,
    pub input_frontiers: Vec<RelationFrontier>,
    pub output_batches: Vec<ViewOutputBatch>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationFrontier {
    pub relation_id: String,
    pub relation_version: String,
    pub committed_offset_exclusive: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ViewFrontier {
    pub view_id: String,
    pub committed_epoch: LogicalEpoch,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurableStateRoot {
    pub object_key: String,
    pub content_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCheckpoint {
    pub identity: StandingProgramIdentity,
    pub logical_epoch: LogicalEpoch,
    pub input_frontiers: Vec<RelationFrontier>,
    pub output_frontiers: Vec<ViewFrontier>,
    pub checkpoint_codec_identity: String,
    pub state_root: DurableStateRoot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_payload: Option<RuntimeCheckpointStatePayload>,
    pub output_manifest_refs: Vec<String>,
    pub owner_epoch: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCheckpointStatePayload {
    pub codec_identity: String,
    pub payload: String,
}

impl RuntimeCheckpoint {
    pub fn validate_identity(
        &self,
        expected: &StandingProgramIdentity,
    ) -> Result<(), StandingProgramRuntimeError> {
        self.identity.validate()?;
        if &self.identity != expected {
            return Err(StandingProgramRuntimeError::ProgramIdentityMismatch {
                expected_program_id: expected.program_id.clone(),
                actual_program_id: self.identity.program_id.clone(),
            });
        }
        if self.checkpoint_codec_identity != expected.checkpoint_codec_identity {
            return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                expected: expected.checkpoint_codec_identity.clone(),
                actual: self.checkpoint_codec_identity.clone(),
            });
        }
        if let Some(payload) = &self.state_payload {
            if payload.codec_identity != expected.checkpoint_codec_identity {
                return Err(StandingProgramRuntimeError::CheckpointCodecMismatch {
                    expected: expected.checkpoint_codec_identity.clone(),
                    actual: payload.codec_identity.clone(),
                });
            }
            let actual = sha256_hex(payload.payload.as_bytes());
            if actual != self.state_root.content_hash {
                return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                    field: "state_payload.content_hash",
                });
            }
        }
        require_non_empty("state_root.object_key", &self.state_root.object_key)?;
        require_sha256("state_root.content_hash", &self.state_root.content_hash)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedViewId {
    pub tenant_id: String,
    pub program_id: String,
    pub view_id: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotPageRequest {
    pub committed_epoch: Option<LogicalEpoch>,
    pub page_token: Option<String>,
    pub max_rows: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct MaterializedViewPage {
    pub view: ScopedViewId,
    pub logical_epoch: LogicalEpoch,
    pub schema_fingerprint: String,
    pub batches: Vec<RecordBatch>,
    pub next_page_token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct MaterializedViewSqlPage {
    pub view: ScopedViewId,
    pub logical_epoch: LogicalEpoch,
    pub rows: Vec<Value>,
    pub next_page_token: Option<String>,
}

pub trait StandingProgramRuntime {
    fn program_identity(&self) -> &StandingProgramIdentity;

    fn input_schemas(&self) -> Vec<RelationSchema>;

    fn output_schemas(&self) -> Vec<RelationSchema>;

    fn logical_epoch(&self) -> LogicalEpoch;

    fn apply_changes(
        &mut self,
        logical_epoch: LogicalEpoch,
        idempotency_key: EpochIdempotencyKey,
        input_changes: Vec<RelationInputBatch>,
    ) -> Result<EpochCommit, StandingProgramRuntimeError>;

    fn materialized_view_page(
        &self,
        view: ScopedViewId,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewPage, StandingProgramRuntimeError>;

    fn materialized_view_sql_page(
        &self,
        view: ScopedViewId,
        sql: String,
        page: SnapshotPageRequest,
    ) -> Result<MaterializedViewSqlPage, StandingProgramRuntimeError> {
        let _ = (view, sql, page);
        Err(StandingProgramRuntimeError::ExternalRuntime {
            reason: "standing runtime does not support SQL pushdown queries".to_string(),
        })
    }

    fn checkpoint(&self) -> Result<RuntimeCheckpoint, StandingProgramRuntimeError>;

    fn restore(checkpoint: RuntimeCheckpoint) -> Result<Self, StandingProgramRuntimeError>
    where
        Self: Sized;
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum StandingProgramRuntimeError {
    #[error("invalid standing program identity field: {field}")]
    InvalidProgramIdentity { field: &'static str },
    #[error("unsupported native code policy: {reason}")]
    UnsupportedNativeCodePolicy { reason: String },
    #[error("standing program identity mismatch: expected={expected_program_id}, actual={actual_program_id}")]
    ProgramIdentityMismatch {
        expected_program_id: String,
        actual_program_id: String,
    },
    #[error("standing program checkpoint codec mismatch: expected={expected}, actual={actual}")]
    CheckpointCodecMismatch { expected: String, actual: String },
    #[error("logical epoch must increase monotonically: current={current}, attempted={attempted}")]
    NonMonotonicLogicalEpoch {
        current: LogicalEpoch,
        attempted: LogicalEpoch,
    },
    #[error("idempotency key `{idempotency_key}` was already applied at epoch {first_epoch}, cannot apply at epoch {attempted_epoch}")]
    IdempotencyKeyConflict {
        idempotency_key: String,
        first_epoch: LogicalEpoch,
        attempted_epoch: LogicalEpoch,
    },
    #[error("committed epoch {requested} is unavailable; current materialized epoch is {current}")]
    UnavailableCommittedEpoch {
        requested: LogicalEpoch,
        current: LogicalEpoch,
    },
    #[error("unknown standing program view `{view_id}`")]
    UnknownView { view_id: String },
    #[error("external standing runtime error: {reason}")]
    ExternalRuntime { reason: String },
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), StandingProgramRuntimeError> {
    if value.trim().is_empty() {
        Err(StandingProgramRuntimeError::InvalidProgramIdentity { field })
    } else {
        Ok(())
    }
}

fn require_sha256(field: &'static str, value: &str) -> Result<(), StandingProgramRuntimeError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field });
    };
    if hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(StandingProgramRuntimeError::InvalidProgramIdentity { field })
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}
