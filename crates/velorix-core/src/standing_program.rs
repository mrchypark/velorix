use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::delta::DeltaBatch;
use crate::engine::LogicalEpoch;
use crate::view_contract::RelationSchema;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StandingProgramIdentity {
    pub tenant_id: String,
    pub program_id: String,
    pub view_ids: Vec<String>,
    pub sql_hash: String,
    pub input_catalog_hash: String,
    pub output_schema_hash: String,
    pub planner_identity: String,
    pub builtin_runtime_identities: Vec<BuiltinRuntimeIdentity>,
    pub runtime_capabilities: Vec<String>,
    pub runtime_compatibility: String,
    pub checkpoint_codec_identity: String,
    pub native_code_policy: NativeCodePolicy,
    /// Canonical digest of the dependency edge binding set.
    /// Empty for views with no view-to-view dependencies.
    /// Computed over sorted ViewDependencyEdgeBindingV1 entries.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dependency_binding_digest: String,
    /// Tenant from authenticated request scope (not "default").
    /// Used for cross-tenant isolation in dependency graphs.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub authenticated_tenant_id: String,
}

impl StandingProgramIdentity {
    pub fn validate(&self) -> Result<(), StandingProgramRuntimeError> {
        require_non_empty("tenant_id", &self.tenant_id)?;
        require_non_empty("program_id", &self.program_id)?;
        require_sha256("sql_hash", &self.sql_hash)?;
        require_sha256("input_catalog_hash", &self.input_catalog_hash)?;
        require_sha256("output_schema_hash", &self.output_schema_hash)?;
        require_non_empty("planner_identity", &self.planner_identity)?;
        require_non_empty("runtime_compatibility", &self.runtime_compatibility)?;
        require_non_empty("checkpoint_codec_identity", &self.checkpoint_codec_identity)?;
        if self.view_ids.is_empty() {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity { field: "view_ids" });
        }
        if self.builtin_runtime_identities.is_empty() {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "builtin_runtime_identities",
            });
        }
        for runtime in &self.builtin_runtime_identities {
            runtime.validate()?;
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
pub struct BuiltinRuntimeIdentity {
    pub name: String,
    pub version: String,
}

impl BuiltinRuntimeIdentity {
    fn validate(&self) -> Result<(), StandingProgramRuntimeError> {
        require_non_empty("builtin_runtime_identities.name", &self.name)?;
        require_non_empty("builtin_runtime_identities.version", &self.version)?;
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
    pub const MAX_BYTES: usize = 256;

    pub fn new(value: impl Into<String>) -> Result<Self, StandingProgramRuntimeError> {
        let value = value.into();
        require_non_empty("idempotency_key", &value)?;
        if value.len() > Self::MAX_BYTES {
            return Err(StandingProgramRuntimeError::InvalidProgramIdentity {
                field: "idempotency_key",
            });
        }
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
    pub stream_id: String,
    pub partition_id: u32,
    pub schema_fingerprint: String,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
    pub event_time_watermark: Option<InputEventTimeWatermark>,
    pub batches: Vec<RecordBatch>,
}

/// Tagged input for the StandingProgramRuntime.
///
/// Distinguishes between physical source batches and upstream materialized
/// view deltas. View deltas preserve signed bag weights and are NOT converted
/// to source offsets.
#[derive(Clone, Debug)]
pub enum StandingInputChangeV1 {
    /// Input from a physical ingest source.
    Source(RelationInputBatch),
    /// Input from an upstream materialized view output.
    View(ViewInputDeltaV1),
}

/// A signed delta batch from an upstream materialized view.
///
/// Contains the edge binding digest, producer cursor, and the actual delta.
/// The controller must verify the authority chain before passing this to runtime.
#[derive(Clone, Debug)]
pub struct ViewInputDeltaV1 {
    /// Digest of the dependency edge binding this delta came from.
    pub edge_binding_digest: String,
    /// Producer cursor at the time of this delta.
    pub producer_cursor: CausalViewCursorV1,
    /// Reference to the producer commit object.
    pub commit_ref: String,
    /// The signed delta batch from the producer.
    pub delta: DeltaBatch,
}

impl ViewInputDeltaV1 {
    /// Validates the view input delta structure.
    pub fn validate(&self) -> Result<(), StandingProgramRuntimeError> {
        require_non_empty("edge_binding_digest", &self.edge_binding_digest)?;
        self.producer_cursor.validate()?;
        require_non_empty("commit_ref", &self.commit_ref)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ViewOutputBatch {
    pub view_id: String,
    pub schema_fingerprint: String,
    pub batches: Vec<RecordBatch>,
}

#[derive(Clone, Debug)]
pub struct ViewOutputDelta {
    pub view_id: String,
    pub schema_fingerprint: String,
    pub delta: DeltaBatch,
}

#[derive(Clone, Debug)]
pub struct EpochCommit {
    pub logical_epoch: LogicalEpoch,
    pub idempotency_key: EpochIdempotencyKey,
    pub input_frontiers: Vec<RelationFrontier>,
    pub input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    pub output_deltas: Vec<ViewOutputDelta>,
    pub output_batches: Vec<ViewOutputBatch>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationFrontier {
    pub relation_id: String,
    pub relation_version: String,
    #[serde(default)]
    pub stream_id: String,
    #[serde(default)]
    pub partition_id: u32,
    pub committed_offset_exclusive: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputEventTimeWatermark {
    pub stream_id: String,
    pub partition_id: u32,
    pub event_time_column_id: String,
    pub max_observed_event_time_ns: i64,
    pub watermark_ns: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputEventTimeFrontier {
    pub relation_id: String,
    pub relation_version: String,
    pub schema_fingerprint: String,
    pub stream_id: String,
    pub partition_id: u32,
    pub event_time_column_id: String,
    pub max_observed_event_time_ns: i64,
    pub watermark_ns: i64,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_event_time_frontiers: Vec<InputEventTimeFrontier>,
    pub output_frontiers: Vec<ViewFrontier>,
    pub checkpoint_codec_identity: String,
    pub state_root: DurableStateRoot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_payload: Option<RuntimeCheckpointStatePayload>,
    pub output_manifest_refs: Vec<String>,
    pub owner_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_coverage: Option<RuntimeCheckpointInputCoverageV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causal_cut: Option<CausalCutV1>,
}

pub const RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1: u32 = 1;
pub const CAUSAL_CUT_SCHEMA_VERSION_V1: u32 = 1;
const CAUSAL_CUT_DIGEST_DOMAIN_V1: &str = "velorix-causal-cut-v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCheckpointInputCoverageV1 {
    pub schema_version: u32,
    pub view_generation: u64,
    pub plan_hash: String,
    pub input_catalog_epoch: u64,
    pub relations: Vec<RuntimeCheckpointRelationCoverageV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCheckpointRelationCoverageV1 {
    pub relation_id: String,
    pub relation_version: String,
    pub relation_generation: u64,
    pub schema_fingerprint: String,
    pub partitions: Vec<RuntimeCheckpointPartitionCoverageV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCheckpointPartitionCoverageV1 {
    pub stream_id: String,
    pub stream_generation: u64,
    pub partition_id: u32,
    pub partition_generation: u64,
    pub covered_from_offset_inclusive: u64,
    pub processed_offset_exclusive: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CausalCutV1 {
    pub schema_version: u32,
    pub direct_source_catalog_epoch: u64,
    pub direct_source_frontiers: Vec<RuntimeCheckpointRelationCoverageV1>,
    pub direct_view_cursors: Vec<CausalViewCursorV1>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CausalViewCursorV1 {
    pub input_edge: String,
    pub producer_tenant_id: String,
    pub producer_program_id: String,
    pub producer_view_id: String,
    pub producer_generation: u64,
    pub output_stream: String,
    pub output_epoch: LogicalEpoch,
    pub commit_digest: String,
}

impl CausalViewCursorV1 {
    pub fn validate(&self) -> Result<(), StandingProgramRuntimeError> {
        require_non_empty("causal_cut.input_edge", &self.input_edge)?;
        require_non_empty("causal_cut.producer_tenant_id", &self.producer_tenant_id)?;
        require_non_empty("causal_cut.producer_program_id", &self.producer_program_id)?;
        require_non_empty("causal_cut.producer_view_id", &self.producer_view_id)?;
        require_non_empty("causal_cut.output_stream", &self.output_stream)?;
        require_sha256("causal_cut.commit_digest", &self.commit_digest)?;
        if self.producer_generation == 0 {
            return Err(invalid_causal_cut());
        }
        Ok(())
    }
}

impl CausalCutV1 {
    pub fn from_input_coverage(
        coverage: &RuntimeCheckpointInputCoverageV1,
        direct_view_cursors: Vec<CausalViewCursorV1>,
    ) -> Result<Self, StandingProgramRuntimeError> {
        let coverage = coverage.clone().canonicalized()?;
        Self {
            schema_version: CAUSAL_CUT_SCHEMA_VERSION_V1,
            direct_source_catalog_epoch: coverage.input_catalog_epoch,
            direct_source_frontiers: coverage.relations,
            direct_view_cursors,
        }
        .canonicalized()
    }

    pub fn canonicalized(mut self) -> Result<Self, StandingProgramRuntimeError> {
        if self.schema_version != CAUSAL_CUT_SCHEMA_VERSION_V1
            || (self.direct_source_frontiers.is_empty() && self.direct_view_cursors.is_empty())
        {
            return Err(invalid_causal_cut());
        }
        let source_coverage = RuntimeCheckpointInputCoverageV1 {
            schema_version: RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1,
            view_generation: 1,
            plan_hash: "causal-cut-source-validation".to_string(),
            input_catalog_epoch: self.direct_source_catalog_epoch,
            relations: self.direct_source_frontiers,
        }
        .canonicalized()?;
        self.direct_source_frontiers = source_coverage.relations;
        self.direct_view_cursors.sort();
        let mut previous_edge = None;
        for cursor in &self.direct_view_cursors {
            cursor.validate()?;
            if previous_edge == Some(cursor.input_edge.as_str()) {
                return Err(invalid_causal_cut());
            }
            previous_edge = Some(cursor.input_edge.as_str());
        }
        Ok(self)
    }

    /// Validates that the cursor set exactly matches the admitted edge set.
    ///
    /// For dependency-capable checkpoints, this ensures:
    /// 1. Every admitted edge has a corresponding cursor
    /// 2. No extra cursors exist beyond the admitted edges
    /// 3. Each cursor passes full authority chain validation
    ///
    /// `admitted_edge_ids` is the set of input_edge_id values from the
    /// admitted dependency graph edges. For source-only checkpoints, pass
    /// an empty set.
    pub fn validate_cursors_match_edges(
        &self,
        admitted_edge_ids: &[String],
    ) -> Result<(), StandingProgramRuntimeError> {
        // For source-only checkpoints, cursors must be empty
        if admitted_edge_ids.is_empty() {
            if !self.direct_view_cursors.is_empty() {
                return Err(StandingProgramRuntimeError::InvalidCausalCut {
                    reason: "source-only checkpoint has view cursors".to_string(),
                });
            }
            return Ok(());
        }

        // For dependency-capable checkpoints, cursors must match edges exactly
        let cursor_edges: Vec<&str> = self.direct_view_cursors.iter()
            .map(|c| c.input_edge.as_str())
            .collect();

        // Check every admitted edge has a cursor
        for edge_id in admitted_edge_ids {
            if !cursor_edges.contains(&edge_id.as_str()) {
                return Err(StandingProgramRuntimeError::InvalidCausalCut {
                    reason: format!("missing cursor for admitted edge: {}", edge_id),
                });
            }
        }

        // Check no extra cursors exist
        for cursor in &self.direct_view_cursors {
            if !admitted_edge_ids.contains(&cursor.input_edge) {
                return Err(StandingProgramRuntimeError::InvalidCausalCut {
                    reason: format!("extra cursor for non-admitted edge: {}", cursor.input_edge),
                });
            }
        }

        // Validate each cursor
        for cursor in &self.direct_view_cursors {
            cursor.validate()?;
        }

        Ok(())
    }

    /// Checks if this causal cut has any view dependencies.
    pub fn has_view_dependencies(&self) -> bool {
        !self.direct_view_cursors.is_empty()
    }

    /// Returns the set of edge IDs that have cursors.
    pub fn cursor_edge_ids(&self) -> Vec<String> {
        self.direct_view_cursors.iter()
            .map(|c| c.input_edge.clone())
            .collect()
    }

    pub fn stable_digest(&self) -> Result<String, StandingProgramRuntimeError> {
        #[derive(Serialize)]
        struct DigestEnvelope<'a> {
            domain: &'static str,
            causal_cut: &'a CausalCutV1,
        }

        let canonical = self.clone().canonicalized()?;
        let bytes = serde_json::to_vec(&DigestEnvelope {
            domain: CAUSAL_CUT_DIGEST_DOMAIN_V1,
            causal_cut: &canonical,
        })
        .map_err(|_| invalid_causal_cut())?;
        Ok(sha256_hex(&bytes))
    }
}

impl RuntimeCheckpointInputCoverageV1 {
    pub fn canonicalized(mut self) -> Result<Self, StandingProgramRuntimeError> {
        if self.schema_version != RUNTIME_CHECKPOINT_INPUT_COVERAGE_SCHEMA_VERSION_V1
            || self.view_generation == 0
        {
            return Err(invalid_input_coverage());
        }
        require_non_empty("input_coverage.plan_hash", &self.plan_hash)?;
        self.relations.sort_by(|left, right| {
            (
                &left.relation_id,
                &left.relation_version,
                left.relation_generation,
            )
                .cmp(&(
                    &right.relation_id,
                    &right.relation_version,
                    right.relation_generation,
                ))
        });
        let mut previous_relation = None;
        for relation in &mut self.relations {
            require_non_empty("input_coverage.relation_id", &relation.relation_id)?;
            require_non_empty(
                "input_coverage.relation_version",
                &relation.relation_version,
            )?;
            require_non_empty(
                "input_coverage.schema_fingerprint",
                &relation.schema_fingerprint,
            )?;
            if relation.relation_generation == 0 {
                return Err(invalid_input_coverage());
            }
            relation.partitions.sort();
            let mut previous_partition = None;
            for partition in &relation.partitions {
                require_non_empty("input_coverage.stream_id", &partition.stream_id)?;
                if partition.stream_generation == 0 || partition.partition_generation == 0 {
                    return Err(invalid_input_coverage());
                }
                if partition.covered_from_offset_inclusive > partition.processed_offset_exclusive {
                    return Err(invalid_input_coverage());
                }
                let identity = (
                    partition.stream_id.as_str(),
                    partition.stream_generation,
                    partition.partition_id,
                    partition.partition_generation,
                );
                if previous_partition == Some(identity) {
                    return Err(invalid_input_coverage());
                }
                previous_partition = Some(identity);
            }
            let identity = (
                relation.relation_id.as_str(),
                relation.relation_version.as_str(),
                relation.relation_generation,
            );
            if previous_relation == Some(identity) {
                return Err(invalid_input_coverage());
            }
            previous_relation = Some(identity);
        }
        Ok(self)
    }

    pub fn stable_hash(&self) -> Result<String, StandingProgramRuntimeError> {
        let canonical = self.clone().canonicalized()?;
        let bytes = serde_json::to_vec(&canonical).map_err(|_| invalid_input_coverage())?;
        Ok(sha256_hex(&bytes))
    }
}

fn invalid_input_coverage() -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "input_coverage",
    }
}

fn invalid_causal_cut() -> StandingProgramRuntimeError {
    StandingProgramRuntimeError::InvalidProgramIdentity {
        field: "causal_cut",
    }
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
        if let Some(coverage) = &self.input_coverage {
            coverage.stable_hash()?;
        }
        if let Some(causal_cut) = &self.causal_cut {
            let canonical_cut = causal_cut.clone().canonicalized()?;
            causal_cut.stable_digest()?;
            if let Some(coverage) = &self.input_coverage {
                let expected = CausalCutV1::from_input_coverage(
                    coverage,
                    canonical_cut.direct_view_cursors.clone(),
                )?;
                if canonical_cut != expected {
                    return Err(invalid_causal_cut());
                }
            }
        }
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
    #[error("invalid causal cut: {reason}")]
    InvalidCausalCut { reason: String },
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
