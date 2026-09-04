//! Metadata service contracts for Velorix control-plane state.

#![forbid(unsafe_code)]

use std::collections::{BTreeSet, HashMap};
#[cfg(feature = "hiqlite-backend")]
use std::future::Future;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use object_store::ObjectStore;
#[cfg(feature = "hiqlite-backend")]
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::RwLock;
use tonic::{metadata::MetadataValue, transport::Channel, Request, Response, Status};
use velorix_core::relation::{RelationSchemaError, VelorixRelationCatalogV1};
use velorix_core::standing_program::RuntimeCheckpointInputCoverageV1;
use velorix_storage::{
    log::{
        DurableIngestAdmissionRecordV1, IngestAdmissionCoordinator,
        ReserveIngestRangeAdmissionOutcome,
    },
    object_key::ObjectKey,
    relation_catalog_registry::{
        CreateRelationCatalogOutcome, RelationCatalogRegistry, RelationCatalogRegistryError,
    },
};

pub mod proto {
    tonic::include_proto!("velorix.meta.v1");
}

mod source_cut;
mod view_bootstrap;

pub use source_cut::{
    CaptureIngestSourceCutRequest, IngestSourceCutV1, IngestSourcePartitionCutV1,
    IngestSourceRelationCutV1, IngestSourceRelationIdentityV1, INGEST_SOURCE_CUT_SCHEMA_VERSION_V1,
    INGEST_SOURCE_IDENTITY_GENERATION_V1,
};
pub use view_bootstrap::{
    BeginViewBootstrapOutcome, BeginViewBootstrapRequest, BeginViewDependencyEdgeV1,
    FixViewBootstrapActivationCutOutcome, FixViewBootstrapActivationCutRequest,
    PromoteViewBootstrapOutcome, PromoteViewBootstrapRequest, ViewBootstrapControlV1,
    ViewBootstrapLifecycleV1, INITIAL_VIEW_BOOTSTRAP_GENERATION,
    VIEW_BOOTSTRAP_CONTROL_SCHEMA_VERSION_V1,
};

pub const STANDING_RUNTIME_FENCING_CAPABILITY_SCHEMA_VERSION: u32 = 2;
pub const STANDING_RUNTIME_OWNER_SCOPE_KIND_TENANT_PROGRAM_VIEW: &str = "tenant_program_view";
pub const STANDING_RUNTIME_BACKEND_TIME_SOURCE_RAFT_REPLICATED: &str =
    "raft_replicated_authority_time";
pub const STANDING_RUNTIME_BACKEND_TIME_SOURCE_PROCESS_CLOCK: &str = "process_clock";
pub const STANDING_RUNTIME_BACKEND_TIME_SOURCE_UNAVAILABLE: &str = "unavailable";
pub const STANDING_RUNTIME_LEASE_AUTHORITY_KIND_NONE: &str = "none";
pub const STANDING_RUNTIME_LEASE_AUTHORITY_KIND_PROCESS_LOCAL: &str = "process_local";
pub const STANDING_RUNTIME_LEASE_AUTHORITY_KIND_HIQLITE_RAFT_SERIALIZED: &str =
    "hiqlite_raft_serialized";
pub const STANDING_RUNTIME_LEASE_AUTHORITY_KIND_RAFT_REPLICATED_TIME: &str = "raft_replicated_time";
pub const STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_UNAVAILABLE: &str = "unavailable";
pub const STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_PROCESS_CLOCK_TTL: &str = "process_clock_ttl";
pub const STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_OPERATION_DRIVEN_LOGICAL: &str =
    "operation_driven_logical";
pub const STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_BACKEND_WALL_CLOCK_TTL: &str =
    "backend_wall_clock_ttl";
pub const STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX: &str = "standing-runtime-output-manifest:";
pub const STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX: &str = "standing-runtime-output-delta:";
pub const STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX: &str = "standing-runtime-output-commit:";
pub const MAX_STANDING_RUNTIME_OWNER_TTL_MS: u64 = 300_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoreRelationCatalogOutcome {
    Created,
    Duplicate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReserveIngestRangeOutcome {
    Reserved,
    Duplicate,
    Conflict,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitIngestRangeOutcome {
    Committed,
    Duplicate,
    Conflict,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetaStoreCapabilities {
    pub standing_runtime_fencing: StandingRuntimeFencingCapability,
    #[serde(default)]
    pub partition_authority: PartitionAuthorityCapability,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StandingRuntimeFencingCapability {
    pub capability_schema_version: u32,
    pub backend_name: String,
    pub owner_scope_kind: String,
    pub linearizable_owner_lease: bool,
    pub durable_monotonic_owner_epoch: bool,
    pub authoritative_backend_time: bool,
    pub owner_validated_checkpoint_publish: bool,
    pub publish_checks_owner_and_latest_atomically: bool,
    pub publish_rejects_expired_owner: bool,
    pub latest_read_linearizable: bool,
    pub publish_rejects_scope_mismatch: bool,
    pub max_owner_ttl_ms: u64,
    pub control_plane_auth_enforced: bool,
    pub production_multi_writer_safe: bool,
    pub backend_time_source_kind: String,
    pub backend_time_blocked_reason: String,
    pub lease_authority_kind: String,
    pub lease_expiry_semantics: String,
    pub bounded_wall_clock_failover: bool,
    pub failover_time_bound_ms: u64,
    pub multi_writer_fencing_safe: bool,
    pub production_bounded_failover_safe: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestRangeReservation {
    pub stream_id: String,
    pub partition_id: u32,
    pub start_offset_inclusive: u64,
    pub end_offset_exclusive: u64,
    pub batch_key: String,
    pub payload_digest: String,
    pub relation_id: String,
    pub relation_version: String,
    pub schema_fingerprint: String,
    pub writer_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StandingRuntimeCheckpointPointer {
    pub tenant_id: String,
    pub program_id: String,
    pub view_id: String,
    pub checkpoint_key: String,
    pub logical_epoch: u64,
    pub content_hash: String,
    #[serde(default)]
    pub manifest_hash: String,
    #[serde(default)]
    pub output_manifest_refs: Vec<String>,
    #[serde(default)]
    pub bootstrap_generation: u64,
    #[serde(default)]
    pub plan_hash: String,
    #[serde(default)]
    pub coverage_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_coverage: Option<RuntimeCheckpointInputCoverageV1>,
    #[serde(default)]
    pub previous_checkpoint_key: String,
    #[serde(default)]
    pub previous_manifest_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StandingRuntimeOwnerClaim {
    pub tenant_id: String,
    pub program_id: String,
    pub view_id: String,
    pub owner_id: String,
    pub owner_epoch: u64,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StandingRuntimeOwnerToken {
    pub tenant_id: String,
    pub program_id: String,
    pub view_id: String,
    pub owner_id: String,
    pub owner_epoch: u64,
}

/// Identifies the independent authority domain for one input partition.
///
/// This is intentionally not a standing-runtime owner scope: a runtime owner
/// cannot be used as authority to publish a partition checkpoint.
#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionAuthorityKey {
    pub namespace: String,
    pub view_id: String,
    pub stream_id: String,
    pub partition_id: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionAuthorityToken {
    pub key: PartitionAuthorityKey,
    pub owner_id: String,
    pub owner_epoch: u64,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcquirePartitionAuthorityRequest {
    pub key: PartitionAuthorityKey,
    pub owner_id: String,
    /// Required only when renewing an unexpired authority token. The backend
    /// rejects a same-owner renewal unless its exact current epoch is supplied.
    pub current_token: Option<PartitionAuthorityToken>,
    pub ttl_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcquirePartitionAuthorityOutcome {
    Acquired(PartitionAuthorityToken),
    Renewed(PartitionAuthorityToken),
    Conflict(PartitionAuthorityToken),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionCheckpointPointer {
    pub key: PartitionAuthorityKey,
    pub checkpoint_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishPartitionCheckpointPointerRequest {
    pub expected_previous: Option<PartitionCheckpointPointer>,
    pub candidate: PartitionCheckpointPointer,
    pub authority: PartitionAuthorityToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublishPartitionCheckpointPointerOutcome {
    Published,
    Duplicate,
    Conflict,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionAuthorityCapability {
    pub backend_name: String,
    pub partition_scoped_authority: bool,
    pub backend_owned_time: bool,
    pub fenced_checkpoint_pointer_publish: bool,
    pub durable_across_restart: bool,
    pub production_safe: bool,
}

impl Default for PartitionAuthorityCapability {
    fn default() -> Self {
        Self::unsupported("unwired")
    }
}

impl PartitionAuthorityCapability {
    fn unsupported(backend_name: &str) -> Self {
        Self {
            backend_name: backend_name.to_string(),
            partition_scoped_authority: false,
            backend_owned_time: false,
            fenced_checkpoint_pointer_publish: false,
            durable_across_restart: false,
            production_safe: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcquireStandingRuntimeOwnerRequest {
    pub tenant_id: String,
    pub program_id: String,
    pub view_id: String,
    pub owner_id: String,
    pub ttl_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcquireStandingRuntimeOwnerOutcome {
    Acquired(StandingRuntimeOwnerClaim),
    Renewed(StandingRuntimeOwnerClaim),
    Conflict(StandingRuntimeOwnerClaim),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishStandingRuntimeCheckpointRequest {
    pub expected_previous: Option<StandingRuntimeCheckpointPointer>,
    pub candidate: StandingRuntimeCheckpointPointer,
    pub owner: StandingRuntimeOwnerToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublishStandingRuntimeCheckpointOutcome {
    Published,
    Duplicate,
    Conflict,
}

#[derive(Debug, Error)]
pub enum MetaStoreError {
    #[error(transparent)]
    RelationSchema(#[from] RelationSchemaError),
    #[error("relation catalog conflict for {relation_id}/{relation_version}")]
    RelationCatalogConflict {
        relation_id: String,
        relation_version: String,
    },
    #[error("relation catalog not found for {relation_id}/{relation_version}")]
    RelationCatalogNotFound {
        relation_id: String,
        relation_version: String,
    },
    #[error(
        "ingest range must be nonempty: start={start_offset_inclusive}, end={end_offset_exclusive}"
    )]
    EmptyIngestRange {
        start_offset_inclusive: u64,
        end_offset_exclusive: u64,
    },
    #[error("metadata field `{field}` must be nonempty")]
    EmptyField { field: &'static str },
    #[error("metadata bearer token is invalid: {reason}")]
    InvalidBearerToken { reason: &'static str },
    #[error("metadata duration field `{field}` must be greater than zero")]
    InvalidDuration { field: &'static str },
    #[error("metadata integer field `{field}` is out of range: {value}")]
    IntegerOutOfRange { field: &'static str, value: u64 },
    #[error("metadata timestamp overflow")]
    TimestampOverflow,
    #[error("partition authority epoch cannot advance beyond i64::MAX")]
    AuthorityEpochOverflow,
    #[error("metadata serialization error: {0}")]
    Serialization(String),
    #[error("standing runtime checkpoint pointer scope mismatch")]
    StandingRuntimeCheckpointScopeMismatch,
    #[error("standing runtime owner token does not match the current unexpired owner")]
    StandingRuntimeOwnerMismatch,
    #[error("partition checkpoint pointer scope does not match its authority key")]
    PartitionCheckpointScopeMismatch,
    #[error("partition authority token scope does not match the requested authority")]
    PartitionAuthorityTokenScopeMismatch,
    #[error(
        "partition authority token is invalid or does not match the current unexpired authority"
    )]
    PartitionAuthorityInvalidToken,
    #[error("duplicate source-cut relation {relation_id}/{relation_version}")]
    DuplicateSourceCutRelation {
        relation_id: String,
        relation_version: String,
    },
    #[error("overlapping source-cut ranges for {stream_id}/p={partition_id}")]
    OverlappingSourceCutRange {
        stream_id: String,
        partition_id: u32,
    },
    #[error("metadata capability `{0}` is not supported by this backend")]
    UnsupportedCapability(&'static str),
    #[error("remote metadata service error: {0}")]
    Remote(String),
    #[error("remote metadata service returned unexpected outcome `{0}`")]
    UnexpectedOutcome(String),
    #[error("object-store metadata store error: {0}")]
    Oss(String),
    #[error("standing runtime checkpoint logical epoch must increase: previous={previous}, candidate={candidate}")]
    NonMonotonicCheckpointEpoch { previous: u64, candidate: u64 },
    #[error("hiqlite metadata store error: {0}")]
    Hiqlite(String),
}

#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    async fn read_meta_store_capabilities(&self) -> Result<MetaStoreCapabilities, MetaStoreError>;

    async fn store_relation_catalog(
        &self,
        catalog: VelorixRelationCatalogV1,
    ) -> Result<StoreRelationCatalogOutcome, MetaStoreError>;

    async fn read_relation_catalog(
        &self,
        relation_id: &str,
        relation_version: &str,
    ) -> Result<VelorixRelationCatalogV1, MetaStoreError>;

    async fn reserve_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError>;

    async fn commit_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<CommitIngestRangeOutcome, MetaStoreError> {
        reservation.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "committed_ingest_source_cut",
        ))
    }

    /// Atomically commit multiple ingest ranges. Either all ranges are
    /// committed or none are. This ensures epoch-level atomicity for
    /// multi-batch ingest epochs.
    ///
    /// Implementations MUST guarantee all-or-nothing semantics. The default
    /// returns UnsupportedCapability because a sequential fallback does NOT
    /// provide atomicity — callers must not rely on it.
    async fn commit_ingest_ranges(
        &self,
        _reservations: Vec<IngestRangeReservation>,
    ) -> Result<Vec<CommitIngestRangeOutcome>, MetaStoreError> {
        Err(MetaStoreError::UnsupportedCapability(
            "commit_ingest_ranges_atomic",
        ))
    }

    async fn capture_ingest_source_cut(
        &self,
        request: CaptureIngestSourceCutRequest,
    ) -> Result<IngestSourceCutV1, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "committed_ingest_source_cut",
        ))
    }

    async fn begin_view_bootstrap(
        &self,
        request: BeginViewBootstrapRequest,
    ) -> Result<BeginViewBootstrapOutcome, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "authoritative_view_bootstrap",
        ))
    }

    async fn read_view_bootstrap(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<ViewBootstrapControlV1>, MetaStoreError> {
        require_non_empty("tenant_id", tenant_id)?;
        require_non_empty("program_id", program_id)?;
        require_non_empty("view_id", view_id)?;
        Err(MetaStoreError::UnsupportedCapability(
            "authoritative_view_bootstrap",
        ))
    }

    async fn fix_view_bootstrap_activation_cut(
        &self,
        request: FixViewBootstrapActivationCutRequest,
    ) -> Result<FixViewBootstrapActivationCutOutcome, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "authoritative_view_bootstrap_activation",
        ))
    }

    async fn promote_view_bootstrap(
        &self,
        request: PromoteViewBootstrapRequest,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "authoritative_view_bootstrap_activation",
        ))
    }

    async fn acquire_standing_runtime_owner(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError>;

    async fn read_standing_runtime_owner(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeOwnerClaim>, MetaStoreError>;

    async fn publish_standing_runtime_checkpoint(
        &self,
        request: PublishStandingRuntimeCheckpointRequest,
    ) -> Result<PublishStandingRuntimeCheckpointOutcome, MetaStoreError>;

    async fn read_standing_runtime_checkpoint(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeCheckpointPointer>, MetaStoreError>;

    /// Returns support details for the separate partition-authority contract.
    /// Backends must opt in; treating an unimplemented backend as authoritative
    /// would permit an unsafe fallback.
    async fn read_partition_authority_capability(
        &self,
    ) -> Result<PartitionAuthorityCapability, MetaStoreError> {
        Err(MetaStoreError::UnsupportedCapability("partition_authority"))
    }

    async fn acquire_partition_authority(
        &self,
        request: AcquirePartitionAuthorityRequest,
    ) -> Result<AcquirePartitionAuthorityOutcome, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability("partition_authority"))
    }

    async fn read_partition_authority(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Option<PartitionAuthorityToken>, MetaStoreError> {
        // This is an advisory liveness observation. It may be stale as soon as
        // returned; any future checkpoint publication must revalidate token
        // and expiry inside its own Raft transaction.
        key.validate()?;
        Err(MetaStoreError::UnsupportedCapability("partition_authority"))
    }

    async fn publish_partition_checkpoint_pointer(
        &self,
        request: PublishPartitionCheckpointPointerRequest,
    ) -> Result<PublishPartitionCheckpointPointerOutcome, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability("partition_authority"))
    }

    async fn read_partition_checkpoint_pointer(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Option<PartitionCheckpointPointer>, MetaStoreError> {
        key.validate()?;
        Err(MetaStoreError::UnsupportedCapability("partition_authority"))
    }

    /// Current view-on-view dependency graph revision for a tenant.
    ///
    /// Required: a default `Ok(0)` here silently turns every missing
    /// forwarding override into a "no graph tracking" claim, which corrupts
    /// the admission-time revision CAS for view-on-view chains. Every concrete
    /// store and every forwarding wrapper must implement this explicitly.
    async fn read_view_dependency_graph_revision(
        &self,
        tenant_id: &str,
    ) -> Result<u64, MetaStoreError>;
}

#[async_trait]
impl<T> MetaStore for Arc<T>
where
    T: MetaStore + ?Sized,
{
    async fn read_meta_store_capabilities(&self) -> Result<MetaStoreCapabilities, MetaStoreError> {
        (**self).read_meta_store_capabilities().await
    }

    async fn store_relation_catalog(
        &self,
        catalog: VelorixRelationCatalogV1,
    ) -> Result<StoreRelationCatalogOutcome, MetaStoreError> {
        (**self).store_relation_catalog(catalog).await
    }

    async fn read_relation_catalog(
        &self,
        relation_id: &str,
        relation_version: &str,
    ) -> Result<VelorixRelationCatalogV1, MetaStoreError> {
        (**self)
            .read_relation_catalog(relation_id, relation_version)
            .await
    }

    async fn fix_view_bootstrap_activation_cut(
        &self,
        request: FixViewBootstrapActivationCutRequest,
    ) -> Result<FixViewBootstrapActivationCutOutcome, MetaStoreError> {
        (**self).fix_view_bootstrap_activation_cut(request).await
    }

    async fn promote_view_bootstrap(
        &self,
        request: PromoteViewBootstrapRequest,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        (**self).promote_view_bootstrap(request).await
    }

    async fn reserve_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        (**self).reserve_ingest_range(reservation).await
    }

    async fn commit_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<CommitIngestRangeOutcome, MetaStoreError> {
        (**self).commit_ingest_range(reservation).await
    }

    async fn commit_ingest_ranges(
        &self,
        reservations: Vec<IngestRangeReservation>,
    ) -> Result<Vec<CommitIngestRangeOutcome>, MetaStoreError> {
        (**self).commit_ingest_ranges(reservations).await
    }

    async fn capture_ingest_source_cut(
        &self,
        request: CaptureIngestSourceCutRequest,
    ) -> Result<IngestSourceCutV1, MetaStoreError> {
        (**self).capture_ingest_source_cut(request).await
    }

    async fn begin_view_bootstrap(
        &self,
        request: BeginViewBootstrapRequest,
    ) -> Result<BeginViewBootstrapOutcome, MetaStoreError> {
        (**self).begin_view_bootstrap(request).await
    }

    async fn read_view_bootstrap(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<ViewBootstrapControlV1>, MetaStoreError> {
        (**self)
            .read_view_bootstrap(tenant_id, program_id, view_id)
            .await
    }

    async fn read_view_dependency_graph_revision(
        &self,
        tenant_id: &str,
    ) -> Result<u64, MetaStoreError> {
        (**self)
            .read_view_dependency_graph_revision(tenant_id)
            .await
    }

    async fn acquire_standing_runtime_owner(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        (**self).acquire_standing_runtime_owner(request).await
    }

    async fn read_standing_runtime_owner(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeOwnerClaim>, MetaStoreError> {
        (**self)
            .read_standing_runtime_owner(tenant_id, program_id, view_id)
            .await
    }

    async fn publish_standing_runtime_checkpoint(
        &self,
        request: PublishStandingRuntimeCheckpointRequest,
    ) -> Result<PublishStandingRuntimeCheckpointOutcome, MetaStoreError> {
        (**self).publish_standing_runtime_checkpoint(request).await
    }

    async fn read_standing_runtime_checkpoint(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeCheckpointPointer>, MetaStoreError> {
        (**self)
            .read_standing_runtime_checkpoint(tenant_id, program_id, view_id)
            .await
    }

    async fn read_partition_authority_capability(
        &self,
    ) -> Result<PartitionAuthorityCapability, MetaStoreError> {
        (**self).read_partition_authority_capability().await
    }

    async fn acquire_partition_authority(
        &self,
        request: AcquirePartitionAuthorityRequest,
    ) -> Result<AcquirePartitionAuthorityOutcome, MetaStoreError> {
        (**self).acquire_partition_authority(request).await
    }

    async fn read_partition_authority(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Option<PartitionAuthorityToken>, MetaStoreError> {
        (**self).read_partition_authority(key).await
    }

    async fn publish_partition_checkpoint_pointer(
        &self,
        request: PublishPartitionCheckpointPointerRequest,
    ) -> Result<PublishPartitionCheckpointPointerOutcome, MetaStoreError> {
        (**self).publish_partition_checkpoint_pointer(request).await
    }

    async fn read_partition_checkpoint_pointer(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Option<PartitionCheckpointPointer>, MetaStoreError> {
        (**self).read_partition_checkpoint_pointer(key).await
    }
}

#[derive(Clone, Default)]
pub struct InMemoryMetaStore {
    inner: Arc<RwLock<InMemoryMetaState>>,
}

#[derive(Default)]
struct InMemoryMetaState {
    relation_catalogs: HashMap<(String, String), VelorixRelationCatalogV1>,
    ingest_reservations: HashMap<(String, u32), Vec<IngestRangeReservation>>,
    committed_ingest_batch_keys: BTreeSet<String>,
    ingest_catalog_epoch: u64,
    view_bootstraps: HashMap<(String, String, String), ViewBootstrapControlV1>,
    view_dependency_graph_revisions: HashMap<String, u64>,
    standing_runtime_owners: HashMap<(String, String, String), StandingRuntimeOwnerClaim>,
    standing_runtime_checkpoints:
        HashMap<(String, String, String), StandingRuntimeCheckpointPointer>,
    partition_authority_now_unix_ms: u64,
    partition_authorities: HashMap<PartitionAuthorityKey, PartitionAuthorityToken>,
    partition_checkpoint_pointers: HashMap<PartitionAuthorityKey, PartitionCheckpointPointer>,
}

impl InMemoryMetaStore {
    /// Controls the in-memory backend clock for deterministic authority tests.
    /// Production callers never provide time through the authority API.
    pub async fn set_partition_authority_clock_for_test(&self, now_unix_ms: u64) {
        self.inner.write().await.partition_authority_now_unix_ms = now_unix_ms;
    }
}

#[async_trait]
impl MetaStore for InMemoryMetaStore {
    async fn read_meta_store_capabilities(&self) -> Result<MetaStoreCapabilities, MetaStoreError> {
        Ok(MetaStoreCapabilities {
            standing_runtime_fencing: standing_runtime_fencing_capability(
                StandingRuntimeFencingCapabilityInput {
                    backend_name: "in-memory",
                    linearizable_owner_lease: true,
                    durable_monotonic_owner_epoch: false,
                    authoritative_backend_time: false,
                    backend_time_source_kind: STANDING_RUNTIME_BACKEND_TIME_SOURCE_PROCESS_CLOCK,
                    backend_time_blocked_reason: "in_memory_process_clock_not_backend_authority",
                    lease_authority_kind: STANDING_RUNTIME_LEASE_AUTHORITY_KIND_PROCESS_LOCAL,
                    lease_expiry_semantics:
                        STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_PROCESS_CLOCK_TTL,
                    bounded_wall_clock_failover: false,
                    owner_validated_checkpoint_publish: true,
                    publish_checks_owner_and_latest_atomically: true,
                    publish_rejects_expired_owner: true,
                    latest_read_linearizable: true,
                    publish_rejects_scope_mismatch: true,
                    control_plane_auth_enforced: false,
                },
            ),
            partition_authority: in_memory_partition_authority_capability(),
        })
    }

    async fn store_relation_catalog(
        &self,
        catalog: VelorixRelationCatalogV1,
    ) -> Result<StoreRelationCatalogOutcome, MetaStoreError> {
        catalog.validate_supported_incremental_adapter_scope()?;
        let key = relation_catalog_key(&catalog);
        let mut guard = self.inner.write().await;

        match guard.relation_catalogs.get(&key) {
            Some(existing) if existing == &catalog => Ok(StoreRelationCatalogOutcome::Duplicate),
            Some(_) => Err(MetaStoreError::RelationCatalogConflict {
                relation_id: key.0,
                relation_version: key.1,
            }),
            None => {
                guard.relation_catalogs.insert(key, catalog);
                Ok(StoreRelationCatalogOutcome::Created)
            }
        }
    }

    async fn read_relation_catalog(
        &self,
        relation_id: &str,
        relation_version: &str,
    ) -> Result<VelorixRelationCatalogV1, MetaStoreError> {
        require_non_empty("relation_id", relation_id)?;
        require_non_empty("relation_version", relation_version)?;

        let guard = self.inner.read().await;
        guard
            .relation_catalogs
            .get(&(relation_id.to_string(), relation_version.to_string()))
            .cloned()
            .ok_or_else(|| MetaStoreError::RelationCatalogNotFound {
                relation_id: relation_id.to_string(),
                relation_version: relation_version.to_string(),
            })
    }

    async fn reserve_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        reservation.validate()?;
        let key = (reservation.stream_id.clone(), reservation.partition_id);
        let mut guard = self.inner.write().await;
        let below_sealed_base = guard.view_bootstraps.values().any(|control| {
            control.bootstrap_cut.relations.iter().any(|relation| {
                relation.relation.relation_id == reservation.relation_id
                    && relation.relation.relation_version == reservation.relation_version
                    && relation.relation.schema_fingerprint == reservation.schema_fingerprint
                    && relation.partitions.iter().any(|partition| {
                        partition.stream_id == reservation.stream_id
                            && partition.partition_id == reservation.partition_id
                            && reservation.start_offset_inclusive < partition.base_offset_inclusive
                    })
            })
        });
        if below_sealed_base {
            return Ok(ReserveIngestRangeOutcome::Conflict);
        }
        let reservations = guard.ingest_reservations.entry(key).or_default();

        if reservations.iter().any(|existing| existing == &reservation) {
            return Ok(ReserveIngestRangeOutcome::Duplicate);
        }
        if reservations
            .iter()
            .any(|existing| existing.overlaps(&reservation))
        {
            return Ok(ReserveIngestRangeOutcome::Conflict);
        }

        reservations.push(reservation);
        reservations.sort_by_key(|entry| entry.start_offset_inclusive);
        guard.ingest_catalog_epoch = guard
            .ingest_catalog_epoch
            .checked_add(1)
            .ok_or(MetaStoreError::TimestampOverflow)?;
        Ok(ReserveIngestRangeOutcome::Reserved)
    }

    async fn commit_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<CommitIngestRangeOutcome, MetaStoreError> {
        reservation.validate()?;
        let key = (reservation.stream_id.clone(), reservation.partition_id);
        let mut guard = self.inner.write().await;
        if !guard
            .ingest_reservations
            .get(&key)
            .is_some_and(|reservations| reservations.contains(&reservation))
        {
            return Ok(CommitIngestRangeOutcome::Conflict);
        }
        if !guard
            .committed_ingest_batch_keys
            .insert(reservation.batch_key)
        {
            return Ok(CommitIngestRangeOutcome::Duplicate);
        }
        Ok(CommitIngestRangeOutcome::Committed)
    }

    async fn commit_ingest_ranges(
        &self,
        reservations: Vec<IngestRangeReservation>,
    ) -> Result<Vec<CommitIngestRangeOutcome>, MetaStoreError> {
        for r in &reservations {
            r.validate()?;
        }
        let mut guard = self.inner.write().await;
        // Validate all reservations can commit before committing any
        for reservation in &reservations {
            let key = (reservation.stream_id.clone(), reservation.partition_id);
            if !guard
                .ingest_reservations
                .get(&key)
                .is_some_and(|reservations| reservations.contains(reservation))
            {
                return Ok(reservations
                    .iter()
                    .map(|_| CommitIngestRangeOutcome::Conflict)
                    .collect());
            }
            if !guard
                .committed_ingest_batch_keys
                .contains(&reservation.batch_key)
            {
                // Check for duplicates within this batch
                let dup_count = reservations
                    .iter()
                    .filter(|r| r.batch_key == reservation.batch_key)
                    .count();
                if dup_count > 1 {
                    return Ok(reservations
                        .iter()
                        .map(|_| CommitIngestRangeOutcome::Duplicate)
                        .collect());
                }
            }
        }
        // All validations passed — commit atomically under single lock
        let mut results = Vec::with_capacity(reservations.len());
        for reservation in &reservations {
            if guard
                .committed_ingest_batch_keys
                .insert(reservation.batch_key.clone())
            {
                results.push(CommitIngestRangeOutcome::Committed);
            } else {
                results.push(CommitIngestRangeOutcome::Duplicate);
            }
        }
        Ok(results)
    }

    async fn capture_ingest_source_cut(
        &self,
        request: CaptureIngestSourceCutRequest,
    ) -> Result<IngestSourceCutV1, MetaStoreError> {
        request.validate()?;
        let guard = self.inner.read().await;
        source_cut::build_ingest_source_cut(
            &request,
            guard.ingest_catalog_epoch,
            guard.ingest_reservations.values().flatten().cloned(),
            &guard.committed_ingest_batch_keys,
        )
    }

    async fn begin_view_bootstrap(
        &self,
        request: BeginViewBootstrapRequest,
    ) -> Result<BeginViewBootstrapOutcome, MetaStoreError> {
        request.validate()?;
        let key = (
            request.tenant_id.clone(),
            request.program_id.clone(),
            request.view_id.clone(),
        );
        let mut guard = self.inner.write().await;
        if let Some(existing) = guard.view_bootstraps.get(&key) {
            return if request.matches(existing) {
                Ok(BeginViewBootstrapOutcome::Duplicate(existing.clone()))
            } else {
                Ok(BeginViewBootstrapOutcome::Conflict)
            };
        }
        // View-on-view admissions bump the tenant graph revision atomically
        // with the bootstrap record; a stale expected revision means another
        // admission moved the graph after this request's cycle check.
        if !request.view_inputs.is_empty() {
            let revision = guard
                .view_dependency_graph_revisions
                .get(&request.tenant_id)
                .copied()
                .unwrap_or(0);
            if revision != request.expected_graph_revision {
                return Ok(BeginViewBootstrapOutcome::Conflict);
            }
            guard
                .view_dependency_graph_revisions
                .insert(request.tenant_id.clone(), revision + 1);
        }
        let cut_request = CaptureIngestSourceCutRequest {
            relations: request.relations.clone(),
        };
        let cut = source_cut::build_ingest_source_cut(
            &cut_request,
            guard.ingest_catalog_epoch,
            guard.ingest_reservations.values().flatten().cloned(),
            &guard.committed_ingest_batch_keys,
        )?;
        let control = view_bootstrap::bootstrap_control(request, cut);
        guard.view_bootstraps.insert(key, control.clone());
        Ok(BeginViewBootstrapOutcome::Created(control))
    }

    async fn read_view_dependency_graph_revision(
        &self,
        tenant_id: &str,
    ) -> Result<u64, MetaStoreError> {
        require_non_empty("tenant_id", tenant_id)?;
        Ok(self
            .inner
            .read()
            .await
            .view_dependency_graph_revisions
            .get(tenant_id)
            .copied()
            .unwrap_or(0))
    }

    async fn read_view_bootstrap(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<ViewBootstrapControlV1>, MetaStoreError> {
        require_non_empty("tenant_id", tenant_id)?;
        require_non_empty("program_id", program_id)?;
        require_non_empty("view_id", view_id)?;
        Ok(self
            .inner
            .read()
            .await
            .view_bootstraps
            .get(&(
                tenant_id.to_string(),
                program_id.to_string(),
                view_id.to_string(),
            ))
            .cloned())
    }

    async fn fix_view_bootstrap_activation_cut(
        &self,
        request: FixViewBootstrapActivationCutRequest,
    ) -> Result<FixViewBootstrapActivationCutOutcome, MetaStoreError> {
        request.validate()?;
        let key = (
            request.tenant_id.clone(),
            request.program_id.clone(),
            request.view_id.clone(),
        );
        let mut guard = self.inner.write().await;
        validate_current_standing_runtime_owner(
            guard.standing_runtime_owners.get(&key),
            &request.owner,
            unix_time_ms()?,
        )?;
        let Some(mut control) = guard.view_bootstraps.get(&key).cloned() else {
            return Ok(FixViewBootstrapActivationCutOutcome::Conflict);
        };
        if control.bootstrap_generation != request.bootstrap_generation
            || control.plan_hash != request.plan_hash
            || request.owner.tenant_id != request.tenant_id
            || request.owner.program_id != request.program_id
            || request.owner.view_id != request.view_id
        {
            return Ok(FixViewBootstrapActivationCutOutcome::Conflict);
        }
        if control.activation_cut.is_some() {
            return Ok(FixViewBootstrapActivationCutOutcome::Duplicate(control));
        }
        if control.lifecycle != ViewBootstrapLifecycleV1::Bootstrapping {
            return Ok(FixViewBootstrapActivationCutOutcome::Conflict);
        }
        let Some(checkpoint) = guard.standing_runtime_checkpoints.get(&key) else {
            return Ok(FixViewBootstrapActivationCutOutcome::Conflict);
        };
        if checkpoint.bootstrap_generation != control.bootstrap_generation
            || checkpoint.plan_hash != control.plan_hash
            || !view_bootstrap::checkpoint_covers_source_cut(checkpoint, &control.bootstrap_cut)
        {
            return Ok(FixViewBootstrapActivationCutOutcome::Conflict);
        }
        let cut_request = CaptureIngestSourceCutRequest {
            relations: control
                .bootstrap_cut
                .relations
                .iter()
                .map(|relation| relation.relation.clone())
                .collect(),
        };
        let activation_cut = source_cut::build_ingest_source_cut(
            &cut_request,
            guard.ingest_catalog_epoch,
            guard.ingest_reservations.values().flatten().cloned(),
            &guard.committed_ingest_batch_keys,
        )?;
        if !view_bootstrap::source_cut_covers(&activation_cut, &control.bootstrap_cut) {
            return Ok(FixViewBootstrapActivationCutOutcome::Conflict);
        }
        control.activation_cut = Some(activation_cut);
        guard.view_bootstraps.insert(key, control.clone());
        Ok(FixViewBootstrapActivationCutOutcome::Fixed(control))
    }

    async fn promote_view_bootstrap(
        &self,
        request: PromoteViewBootstrapRequest,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        request.validate()?;
        let key = (
            request.tenant_id.clone(),
            request.program_id.clone(),
            request.view_id.clone(),
        );
        let mut guard = self.inner.write().await;
        validate_current_standing_runtime_owner(
            guard.standing_runtime_owners.get(&key),
            &request.owner,
            unix_time_ms()?,
        )?;
        let Some(mut control) = guard.view_bootstraps.get(&key).cloned() else {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        };
        if control.bootstrap_generation != request.bootstrap_generation
            || control.plan_hash != request.plan_hash
            || request.owner.tenant_id != request.tenant_id
            || request.owner.program_id != request.program_id
            || request.owner.view_id != request.view_id
        {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        }
        if control.lifecycle == ViewBootstrapLifecycleV1::Active {
            return if control.active_checkpoint.as_ref() == Some(&request.checkpoint) {
                Ok(PromoteViewBootstrapOutcome::Duplicate(control))
            } else {
                Ok(PromoteViewBootstrapOutcome::Conflict)
            };
        }
        let Some(activation_cut) = control.activation_cut.as_ref() else {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        };
        if guard.standing_runtime_checkpoints.get(&key) != Some(&request.checkpoint)
            || request.checkpoint.bootstrap_generation != control.bootstrap_generation
            || request.checkpoint.plan_hash != control.plan_hash
            || !view_bootstrap::checkpoint_covers_source_cut(&request.checkpoint, activation_cut)
        {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        }
        control.lifecycle = ViewBootstrapLifecycleV1::Active;
        control.active_checkpoint = Some(request.checkpoint);
        guard.view_bootstraps.insert(key, control.clone());
        Ok(PromoteViewBootstrapOutcome::Promoted(control))
    }

    async fn acquire_standing_runtime_owner(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        request.validate()?;
        let now = unix_time_ms()?;
        let expires_at_unix_ms = now
            .checked_add(request.ttl_ms)
            .ok_or(MetaStoreError::TimestampOverflow)?;
        let key = standing_runtime_owner_scope_key(
            &request.tenant_id,
            &request.program_id,
            &request.view_id,
        );
        let mut guard = self.inner.write().await;
        let current = guard.standing_runtime_owners.get(&key).cloned();
        match current {
            Some(current)
                if current.expires_at_unix_ms > now && current.owner_id != request.owner_id =>
            {
                Ok(AcquireStandingRuntimeOwnerOutcome::Conflict(current))
            }
            Some(current) if current.expires_at_unix_ms > now => {
                let claim = StandingRuntimeOwnerClaim {
                    tenant_id: request.tenant_id,
                    program_id: request.program_id,
                    view_id: request.view_id,
                    owner_id: request.owner_id,
                    owner_epoch: current.owner_epoch,
                    expires_at_unix_ms,
                };
                guard.standing_runtime_owners.insert(key, claim.clone());
                Ok(AcquireStandingRuntimeOwnerOutcome::Renewed(claim))
            }
            Some(current) => {
                let owner_epoch = current
                    .owner_epoch
                    .checked_add(1)
                    .ok_or(MetaStoreError::AuthorityEpochOverflow)?;
                let claim = StandingRuntimeOwnerClaim {
                    tenant_id: request.tenant_id,
                    program_id: request.program_id,
                    view_id: request.view_id,
                    owner_id: request.owner_id,
                    owner_epoch,
                    expires_at_unix_ms,
                };
                guard.standing_runtime_owners.insert(key, claim.clone());
                Ok(AcquireStandingRuntimeOwnerOutcome::Acquired(claim))
            }
            None => {
                let claim = StandingRuntimeOwnerClaim {
                    tenant_id: request.tenant_id,
                    program_id: request.program_id,
                    view_id: request.view_id,
                    owner_id: request.owner_id,
                    owner_epoch: 1,
                    expires_at_unix_ms,
                };
                guard.standing_runtime_owners.insert(key, claim.clone());
                Ok(AcquireStandingRuntimeOwnerOutcome::Acquired(claim))
            }
        }
    }

    async fn read_standing_runtime_owner(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeOwnerClaim>, MetaStoreError> {
        validate_standing_runtime_scope(tenant_id, program_id, view_id)?;
        let now = unix_time_ms()?;
        let guard = self.inner.read().await;
        Ok(guard
            .standing_runtime_owners
            .get(&standing_runtime_owner_scope_key(
                tenant_id, program_id, view_id,
            ))
            .filter(|claim| claim.expires_at_unix_ms > now)
            .cloned())
    }

    async fn publish_standing_runtime_checkpoint(
        &self,
        request: PublishStandingRuntimeCheckpointRequest,
    ) -> Result<PublishStandingRuntimeCheckpointOutcome, MetaStoreError> {
        request.validate()?;
        let key = standing_runtime_checkpoint_scope_key(&request.candidate);
        let mut guard = self.inner.write().await;
        validate_current_standing_runtime_owner(
            guard.standing_runtime_owners.get(&key),
            &request.owner,
            unix_time_ms()?,
        )?;
        let current = guard.standing_runtime_checkpoints.get(&key);

        if current == Some(&request.candidate) {
            return Ok(PublishStandingRuntimeCheckpointOutcome::Duplicate);
        }
        if current != request.expected_previous.as_ref() {
            return Ok(PublishStandingRuntimeCheckpointOutcome::Conflict);
        }

        guard
            .standing_runtime_checkpoints
            .insert(key, request.candidate);
        Ok(PublishStandingRuntimeCheckpointOutcome::Published)
    }

    async fn read_standing_runtime_checkpoint(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeCheckpointPointer>, MetaStoreError> {
        validate_standing_runtime_scope(tenant_id, program_id, view_id)?;
        let guard = self.inner.read().await;
        Ok(guard
            .standing_runtime_checkpoints
            .get(&(
                tenant_id.to_string(),
                program_id.to_string(),
                view_id.to_string(),
            ))
            .cloned())
    }

    async fn read_partition_authority_capability(
        &self,
    ) -> Result<PartitionAuthorityCapability, MetaStoreError> {
        Ok(in_memory_partition_authority_capability())
    }

    async fn acquire_partition_authority(
        &self,
        request: AcquirePartitionAuthorityRequest,
    ) -> Result<AcquirePartitionAuthorityOutcome, MetaStoreError> {
        request.validate()?;
        let mut guard = self.inner.write().await;
        let now = guard.partition_authority_now_unix_ms;
        let expires_at_unix_ms = now
            .checked_add(request.ttl_ms)
            .ok_or(MetaStoreError::TimestampOverflow)?;
        match guard.partition_authorities.get(&request.key).cloned() {
            Some(current) if current.expires_at_unix_ms > now => {
                if current.owner_id == request.owner_id
                    && request.current_token.as_ref() == Some(&current)
                {
                    let renewed = PartitionAuthorityToken {
                        expires_at_unix_ms,
                        ..current
                    };
                    guard
                        .partition_authorities
                        .insert(request.key, renewed.clone());
                    Ok(AcquirePartitionAuthorityOutcome::Renewed(renewed))
                } else {
                    Ok(AcquirePartitionAuthorityOutcome::Conflict(current))
                }
            }
            Some(current) => {
                let owner_epoch = current
                    .owner_epoch
                    .checked_add(1)
                    .ok_or(MetaStoreError::TimestampOverflow)?;
                let token = PartitionAuthorityToken {
                    key: request.key.clone(),
                    owner_id: request.owner_id,
                    owner_epoch,
                    expires_at_unix_ms,
                };
                guard
                    .partition_authorities
                    .insert(request.key, token.clone());
                Ok(AcquirePartitionAuthorityOutcome::Acquired(token))
            }
            None => {
                let token = PartitionAuthorityToken {
                    key: request.key.clone(),
                    owner_id: request.owner_id,
                    owner_epoch: 1,
                    expires_at_unix_ms,
                };
                guard
                    .partition_authorities
                    .insert(request.key, token.clone());
                Ok(AcquirePartitionAuthorityOutcome::Acquired(token))
            }
        }
    }

    async fn read_partition_authority(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Option<PartitionAuthorityToken>, MetaStoreError> {
        key.validate()?;
        let guard = self.inner.read().await;
        Ok(guard
            .partition_authorities
            .get(key)
            .filter(|token| token.expires_at_unix_ms > guard.partition_authority_now_unix_ms)
            .cloned())
    }

    async fn publish_partition_checkpoint_pointer(
        &self,
        request: PublishPartitionCheckpointPointerRequest,
    ) -> Result<PublishPartitionCheckpointPointerOutcome, MetaStoreError> {
        request.validate()?;
        let mut guard = self.inner.write().await;
        let now = guard.partition_authority_now_unix_ms;
        let current_authority = guard.partition_authorities.get(&request.candidate.key);
        validate_current_partition_authority(current_authority, &request.authority, now)?;
        let current = guard
            .partition_checkpoint_pointers
            .get(&request.candidate.key);
        if current == Some(&request.candidate) {
            return Ok(PublishPartitionCheckpointPointerOutcome::Duplicate);
        }
        if current != request.expected_previous.as_ref() {
            return Ok(PublishPartitionCheckpointPointerOutcome::Conflict);
        }
        guard
            .partition_checkpoint_pointers
            .insert(request.candidate.key.clone(), request.candidate);
        Ok(PublishPartitionCheckpointPointerOutcome::Published)
    }

    async fn read_partition_checkpoint_pointer(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Option<PartitionCheckpointPointer>, MetaStoreError> {
        key.validate()?;
        Ok(self
            .inner
            .read()
            .await
            .partition_checkpoint_pointers
            .get(key)
            .cloned())
    }
}

#[derive(Clone)]
pub struct OssMetaStore {
    relation_catalogs: RelationCatalogRegistry,
    ingest_admission: IngestAdmissionCoordinator,
}

impl OssMetaStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            relation_catalogs: RelationCatalogRegistry::new(Arc::clone(&store)),
            ingest_admission: IngestAdmissionCoordinator::new_object_store_meta_authority(store),
        }
    }
}

#[async_trait]
impl MetaStore for OssMetaStore {
    async fn read_meta_store_capabilities(&self) -> Result<MetaStoreCapabilities, MetaStoreError> {
        Ok(MetaStoreCapabilities {
            standing_runtime_fencing: standing_runtime_fencing_capability(
                StandingRuntimeFencingCapabilityInput {
                    backend_name: "oss",
                    linearizable_owner_lease: false,
                    durable_monotonic_owner_epoch: false,
                    authoritative_backend_time: false,
                    backend_time_source_kind: STANDING_RUNTIME_BACKEND_TIME_SOURCE_UNAVAILABLE,
                    backend_time_blocked_reason:
                        "oss_backend_has_no_standing_runtime_lease_authority",
                    lease_authority_kind: STANDING_RUNTIME_LEASE_AUTHORITY_KIND_NONE,
                    lease_expiry_semantics: STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_UNAVAILABLE,
                    bounded_wall_clock_failover: false,
                    owner_validated_checkpoint_publish: false,
                    publish_checks_owner_and_latest_atomically: false,
                    publish_rejects_expired_owner: false,
                    latest_read_linearizable: false,
                    publish_rejects_scope_mismatch: false,
                    control_plane_auth_enforced: false,
                },
            ),
            partition_authority: PartitionAuthorityCapability::unsupported("oss"),
        })
    }

    async fn store_relation_catalog(
        &self,
        catalog: VelorixRelationCatalogV1,
    ) -> Result<StoreRelationCatalogOutcome, MetaStoreError> {
        match self
            .relation_catalogs
            .create(&catalog)
            .await
            .map_err(|error| oss_store_catalog_error(error, &catalog))?
        {
            CreateRelationCatalogOutcome::Created => Ok(StoreRelationCatalogOutcome::Created),
            CreateRelationCatalogOutcome::Duplicate => Ok(StoreRelationCatalogOutcome::Duplicate),
        }
    }

    async fn read_relation_catalog(
        &self,
        relation_id: &str,
        relation_version: &str,
    ) -> Result<VelorixRelationCatalogV1, MetaStoreError> {
        self.relation_catalogs
            .read(relation_id, relation_version)
            .await
            .map_err(|error| oss_read_catalog_error(error, relation_id, relation_version))
    }

    async fn reserve_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        reservation.validate()?;
        let record = DurableIngestAdmissionRecordV1::for_external_admission(
            reservation.stream_id,
            reservation.partition_id,
            reservation.start_offset_inclusive,
            reservation.end_offset_exclusive,
            reservation.payload_digest,
            reservation.relation_id,
            reservation.relation_version,
            reservation.schema_fingerprint,
        )
        .map_err(|error| MetaStoreError::Oss(error.to_string()))?;
        if reservation.batch_key != record.batch_key.as_str() {
            return Err(MetaStoreError::Oss(format!(
                "reservation batch_key `{}` does not match expected `{}`",
                reservation.batch_key, record.batch_key
            )));
        }

        match self
            .ingest_admission
            .reserve_external_ingest_range_admission(record)
            .await
            .map_err(|error| MetaStoreError::Oss(error.to_string()))?
        {
            ReserveIngestRangeAdmissionOutcome::Reserved => Ok(ReserveIngestRangeOutcome::Reserved),
            ReserveIngestRangeAdmissionOutcome::Duplicate => {
                Ok(ReserveIngestRangeOutcome::Duplicate)
            }
            ReserveIngestRangeAdmissionOutcome::Conflict { .. } => {
                Ok(ReserveIngestRangeOutcome::Conflict)
            }
        }
    }

    async fn commit_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<CommitIngestRangeOutcome, MetaStoreError> {
        reservation.validate()?;
        let committed = self
            .ingest_admission
            .list_committed()
            .await
            .map_err(|error| MetaStoreError::Oss(error.to_string()))?;
        if committed.iter().any(|entry| {
            entry.stream_id == reservation.stream_id
                && entry.partition_id == reservation.partition_id
                && entry.start_offset_inclusive == reservation.start_offset_inclusive
                && entry.end_offset_exclusive == reservation.end_offset_exclusive
                && entry.object_key.as_str() == reservation.batch_key
        }) {
            Ok(CommitIngestRangeOutcome::Duplicate)
        } else {
            Ok(CommitIngestRangeOutcome::Conflict)
        }
    }

    async fn capture_ingest_source_cut(
        &self,
        request: CaptureIngestSourceCutRequest,
    ) -> Result<IngestSourceCutV1, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "committed_ingest_source_cut",
        ))
    }

    async fn acquire_standing_runtime_owner(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "linearizable_standing_runtime_owner_lease",
        ))
    }

    async fn read_standing_runtime_owner(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeOwnerClaim>, MetaStoreError> {
        validate_standing_runtime_scope(tenant_id, program_id, view_id)?;
        Err(MetaStoreError::UnsupportedCapability(
            "linearizable_standing_runtime_owner_lease",
        ))
    }

    async fn publish_standing_runtime_checkpoint(
        &self,
        request: PublishStandingRuntimeCheckpointRequest,
    ) -> Result<PublishStandingRuntimeCheckpointOutcome, MetaStoreError> {
        request.validate()?;
        Err(MetaStoreError::UnsupportedCapability(
            "linearizable_standing_runtime_checkpoint_publish",
        ))
    }

    async fn read_standing_runtime_checkpoint(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeCheckpointPointer>, MetaStoreError> {
        validate_standing_runtime_scope(tenant_id, program_id, view_id)?;
        Err(MetaStoreError::UnsupportedCapability(
            "linearizable_standing_runtime_checkpoint_publish",
        ))
    }

    async fn read_view_dependency_graph_revision(
        &self,
        tenant_id: &str,
    ) -> Result<u64, MetaStoreError> {
        require_non_empty("tenant_id", tenant_id)?;
        Err(MetaStoreError::UnsupportedCapability(
            "authoritative_view_bootstrap",
        ))
    }
}

impl IngestRangeReservation {
    fn validate(&self) -> Result<(), MetaStoreError> {
        require_non_empty("stream_id", &self.stream_id)?;
        require_non_empty("batch_key", &self.batch_key)?;
        require_non_empty("payload_digest", &self.payload_digest)?;
        require_non_empty("relation_id", &self.relation_id)?;
        require_non_empty("relation_version", &self.relation_version)?;
        require_non_empty("schema_fingerprint", &self.schema_fingerprint)?;
        if self.start_offset_inclusive >= self.end_offset_exclusive {
            return Err(MetaStoreError::EmptyIngestRange {
                start_offset_inclusive: self.start_offset_inclusive,
                end_offset_exclusive: self.end_offset_exclusive,
            });
        }

        Ok(())
    }

    fn overlaps(&self, other: &Self) -> bool {
        self.start_offset_inclusive < other.end_offset_exclusive
            && other.start_offset_inclusive < self.end_offset_exclusive
    }
}

impl AcquireStandingRuntimeOwnerRequest {
    fn validate(&self) -> Result<(), MetaStoreError> {
        validate_standing_runtime_scope(&self.tenant_id, &self.program_id, &self.view_id)?;
        require_non_empty("owner_id", &self.owner_id)?;
        if self.ttl_ms == 0 {
            return Err(MetaStoreError::InvalidDuration { field: "ttl_ms" });
        }
        if self.ttl_ms > MAX_STANDING_RUNTIME_OWNER_TTL_MS {
            return Err(MetaStoreError::IntegerOutOfRange {
                field: "ttl_ms",
                value: self.ttl_ms,
            });
        }
        Ok(())
    }
}

impl PublishStandingRuntimeCheckpointRequest {
    fn validate(&self) -> Result<(), MetaStoreError> {
        self.candidate.validate()?;
        self.owner.validate()?;
        if standing_runtime_owner_scope_key(
            &self.owner.tenant_id,
            &self.owner.program_id,
            &self.owner.view_id,
        ) != standing_runtime_checkpoint_scope_key(&self.candidate)
        {
            return Err(MetaStoreError::StandingRuntimeCheckpointScopeMismatch);
        }
        if let Some(expected) = &self.expected_previous {
            expected.validate()?;
            if standing_runtime_checkpoint_scope_key(expected)
                != standing_runtime_checkpoint_scope_key(&self.candidate)
            {
                return Err(MetaStoreError::StandingRuntimeCheckpointScopeMismatch);
            }
            if &self.candidate == expected {
                return Ok(());
            }
            if self.candidate.logical_epoch <= expected.logical_epoch {
                return Err(MetaStoreError::NonMonotonicCheckpointEpoch {
                    previous: expected.logical_epoch,
                    candidate: self.candidate.logical_epoch,
                });
            }
            if self.candidate.previous_checkpoint_key != expected.checkpoint_key
                || self.candidate.previous_manifest_hash != expected.manifest_hash
            {
                return Err(MetaStoreError::Serialization(
                    "standing runtime checkpoint predecessor commitment mismatch".to_string(),
                ));
            }
            if expected.bootstrap_generation != 0
                && (self.candidate.bootstrap_generation != expected.bootstrap_generation
                    || self.candidate.plan_hash != expected.plan_hash)
            {
                return Err(MetaStoreError::Serialization(
                    "standing runtime checkpoint generation or plan changed within one pointer lineage"
                        .to_string(),
                ));
            }
        } else if !self.candidate.previous_checkpoint_key.is_empty()
            || !self.candidate.previous_manifest_hash.is_empty()
        {
            return Err(MetaStoreError::Serialization(
                "initial standing runtime checkpoint has a predecessor commitment".to_string(),
            ));
        }
        Ok(())
    }
}

impl StandingRuntimeOwnerClaim {
    fn token(&self) -> StandingRuntimeOwnerToken {
        StandingRuntimeOwnerToken {
            tenant_id: self.tenant_id.clone(),
            program_id: self.program_id.clone(),
            view_id: self.view_id.clone(),
            owner_id: self.owner_id.clone(),
            owner_epoch: self.owner_epoch,
        }
    }
}

impl StandingRuntimeOwnerToken {
    fn validate(&self) -> Result<(), MetaStoreError> {
        validate_standing_runtime_scope(&self.tenant_id, &self.program_id, &self.view_id)?;
        require_non_empty("owner_id", &self.owner_id)?;
        if self.owner_epoch == 0 {
            return Err(MetaStoreError::IntegerOutOfRange {
                field: "owner_epoch",
                value: self.owner_epoch,
            });
        }
        Ok(())
    }
}

impl PartitionAuthorityKey {
    fn validate(&self) -> Result<(), MetaStoreError> {
        require_non_empty("namespace", &self.namespace)?;
        require_non_empty("view_id", &self.view_id)?;
        require_non_empty("stream_id", &self.stream_id)?;
        Ok(())
    }
}

impl PartitionAuthorityToken {
    fn validate(&self) -> Result<(), MetaStoreError> {
        self.key.validate()?;
        if self.owner_id.is_empty() || self.owner_epoch == 0 {
            return Err(MetaStoreError::PartitionAuthorityInvalidToken);
        }
        Ok(())
    }
}

impl AcquirePartitionAuthorityRequest {
    fn validate(&self) -> Result<(), MetaStoreError> {
        self.key.validate()?;
        require_non_empty("owner_id", &self.owner_id)?;
        if self.ttl_ms == 0 {
            return Err(MetaStoreError::InvalidDuration { field: "ttl_ms" });
        }
        if let Some(token) = &self.current_token {
            token.validate()?;
            if token.key != self.key {
                return Err(MetaStoreError::PartitionAuthorityTokenScopeMismatch);
            }
            if token.owner_id != self.owner_id {
                return Err(MetaStoreError::PartitionAuthorityInvalidToken);
            }
        }
        Ok(())
    }
}

impl PartitionCheckpointPointer {
    fn validate(&self) -> Result<(), MetaStoreError> {
        self.key.validate()?;
        require_non_empty("checkpoint_key", &self.checkpoint_key)
    }
}

impl PublishPartitionCheckpointPointerRequest {
    fn validate(&self) -> Result<(), MetaStoreError> {
        self.candidate.validate()?;
        self.authority.validate()?;
        if self.authority.key != self.candidate.key {
            return Err(MetaStoreError::PartitionCheckpointScopeMismatch);
        }
        if let Some(expected) = &self.expected_previous {
            expected.validate()?;
            if expected.key != self.candidate.key {
                return Err(MetaStoreError::PartitionCheckpointScopeMismatch);
            }
        }
        Ok(())
    }
}

impl StandingRuntimeCheckpointPointer {
    fn validate(&self) -> Result<(), MetaStoreError> {
        validate_standing_runtime_scope(&self.tenant_id, &self.program_id, &self.view_id)?;
        require_non_empty("checkpoint_key", &self.checkpoint_key)?;
        require_non_empty("content_hash", &self.content_hash)?;
        require_non_empty("manifest_hash", &self.manifest_hash)?;
        if self.previous_checkpoint_key.is_empty() != self.previous_manifest_hash.is_empty() {
            return Err(MetaStoreError::Serialization(
                "standing runtime checkpoint has a partial predecessor commitment".to_string(),
            ));
        }
        if !self.previous_manifest_hash.is_empty()
            && !self.previous_manifest_hash.starts_with("sha256:")
        {
            return Err(MetaStoreError::Serialization(
                "standing runtime checkpoint predecessor manifest hash is invalid".to_string(),
            ));
        }
        if !self.previous_checkpoint_key.is_empty() {
            let (_, previous_parts) =
                ObjectKey::parse_standing_runtime_checkpoint(self.previous_checkpoint_key.clone())
                    .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
            if previous_parts.tenant_id != self.tenant_id
                || previous_parts.program_id != self.program_id
                || previous_parts.view_id != self.view_id
                || previous_parts.logical_epoch >= self.logical_epoch
            {
                return Err(MetaStoreError::Serialization(
                    "standing runtime checkpoint predecessor scope or epoch mismatch".to_string(),
                ));
            }
        }
        match &self.input_coverage {
            Some(coverage) => {
                if self.bootstrap_generation == 0
                    || self.bootstrap_generation != coverage.view_generation
                    || self.plan_hash != coverage.plan_hash
                    || self.coverage_hash
                        != coverage
                            .stable_hash()
                            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?
                {
                    return Err(MetaStoreError::Serialization(
                        "standing runtime checkpoint coverage commitment mismatch".to_string(),
                    ));
                }
            }
            None => {
                if self.bootstrap_generation != 0
                    || !self.plan_hash.is_empty()
                    || !self.coverage_hash.is_empty()
                {
                    return Err(MetaStoreError::Serialization(
                        "standing runtime checkpoint has partial coverage commitment".to_string(),
                    ));
                }
            }
        }
        let (_, parts) = ObjectKey::parse_standing_runtime_checkpoint(self.checkpoint_key.clone())
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        if parts.tenant_id != self.tenant_id
            || parts.program_id != self.program_id
            || parts.view_id != self.view_id
            || parts.logical_epoch != self.logical_epoch
            || parts.content_hash != self.content_hash
        {
            return Err(MetaStoreError::Serialization(format!(
                "standing runtime checkpoint pointer key/body mismatch for `{}/{}/{}`",
                self.tenant_id, self.program_id, self.view_id
            )));
        }
        let mut seen_output_manifest_refs = BTreeSet::new();
        for output_manifest_ref in &self.output_manifest_refs {
            require_non_empty("output_manifest_refs", output_manifest_ref)?;
            if !seen_output_manifest_refs.insert(output_manifest_ref) {
                return Err(MetaStoreError::Serialization(format!(
                    "duplicate standing runtime output manifest ref `{output_manifest_ref}`"
                )));
            }
            if let Some(output_manifest_key) =
                output_manifest_ref.strip_prefix(STANDING_RUNTIME_OUTPUT_MANIFEST_REF_PREFIX)
            {
                let (_, output_parts) = ObjectKey::parse_standing_runtime_output_manifest(
                    output_manifest_key.to_string(),
                )
                .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
                if output_parts.tenant_id != self.tenant_id
                    || output_parts.program_id != self.program_id
                    || output_parts.view_id != self.view_id
                    || output_parts.logical_epoch != self.logical_epoch
                {
                    return Err(MetaStoreError::Serialization(format!(
                        "standing runtime output manifest ref scope mismatch for `{}/{}/{}`",
                        self.tenant_id, self.program_id, self.view_id
                    )));
                }
            } else if let Some(output_delta_key) = output_manifest_ref
                .strip_prefix(STANDING_RUNTIME_OUTPUT_DELTA_REF_PREFIX)
                .or_else(|| {
                    output_manifest_ref.strip_prefix(STANDING_RUNTIME_OUTPUT_COMMIT_REF_PREFIX)
                })
            {
                let (_, output_parts) =
                    ObjectKey::parse_standing_runtime_output_delta(output_delta_key.to_string())
                        .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
                if output_parts.tenant_id != self.tenant_id
                    || output_parts.program_id != self.program_id
                    || output_parts.view_id != self.view_id
                    || output_parts.logical_epoch != self.logical_epoch
                {
                    return Err(MetaStoreError::Serialization(format!(
                        "standing runtime output delta ref scope mismatch for `{}/{}/{}`",
                        self.tenant_id, self.program_id, self.view_id
                    )));
                }
            } else {
                return Err(MetaStoreError::Serialization(format!(
                    "standing runtime output ref uses unsupported prefix: `{output_manifest_ref}`"
                )));
            }
        }
        Ok(())
    }
}

fn validate_standing_runtime_scope(
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
) -> Result<(), MetaStoreError> {
    require_non_empty("tenant_id", tenant_id)?;
    require_non_empty("program_id", program_id)?;
    require_non_empty("view_id", view_id)?;
    Ok(())
}

fn standing_runtime_checkpoint_scope_key(
    pointer: &StandingRuntimeCheckpointPointer,
) -> (String, String, String) {
    (
        pointer.tenant_id.clone(),
        pointer.program_id.clone(),
        pointer.view_id.clone(),
    )
}

fn standing_runtime_owner_scope_key(
    tenant_id: &str,
    program_id: &str,
    view_id: &str,
) -> (String, String, String) {
    (
        tenant_id.to_string(),
        program_id.to_string(),
        view_id.to_string(),
    )
}

fn validate_current_standing_runtime_owner(
    current: Option<&StandingRuntimeOwnerClaim>,
    owner: &StandingRuntimeOwnerToken,
    now_unix_ms: u64,
) -> Result<(), MetaStoreError> {
    let Some(current) = current else {
        return Err(MetaStoreError::StandingRuntimeOwnerMismatch);
    };
    if current.expires_at_unix_ms <= now_unix_ms || current.token() != *owner {
        return Err(MetaStoreError::StandingRuntimeOwnerMismatch);
    }
    Ok(())
}

fn validate_current_partition_authority(
    current: Option<&PartitionAuthorityToken>,
    authority: &PartitionAuthorityToken,
    now_unix_ms: u64,
) -> Result<(), MetaStoreError> {
    let Some(current) = current else {
        return Err(MetaStoreError::PartitionAuthorityInvalidToken);
    };
    if current.expires_at_unix_ms <= now_unix_ms || current != authority {
        return Err(MetaStoreError::PartitionAuthorityInvalidToken);
    }
    Ok(())
}

struct StandingRuntimeFencingCapabilityInput {
    backend_name: &'static str,
    linearizable_owner_lease: bool,
    durable_monotonic_owner_epoch: bool,
    authoritative_backend_time: bool,
    backend_time_source_kind: &'static str,
    backend_time_blocked_reason: &'static str,
    lease_authority_kind: &'static str,
    lease_expiry_semantics: &'static str,
    bounded_wall_clock_failover: bool,
    owner_validated_checkpoint_publish: bool,
    publish_checks_owner_and_latest_atomically: bool,
    publish_rejects_expired_owner: bool,
    latest_read_linearizable: bool,
    publish_rejects_scope_mismatch: bool,
    control_plane_auth_enforced: bool,
}

fn in_memory_partition_authority_capability() -> PartitionAuthorityCapability {
    PartitionAuthorityCapability {
        backend_name: "in-memory".to_string(),
        partition_scoped_authority: true,
        backend_owned_time: true,
        fenced_checkpoint_pointer_publish: true,
        durable_across_restart: false,
        production_safe: false,
    }
}

fn standing_runtime_fencing_capability(
    input: StandingRuntimeFencingCapabilityInput,
) -> StandingRuntimeFencingCapability {
    let multi_writer_fencing_safe = input.linearizable_owner_lease
        && input.durable_monotonic_owner_epoch
        && input.owner_validated_checkpoint_publish
        && input.publish_checks_owner_and_latest_atomically
        && input.publish_rejects_expired_owner
        && input.latest_read_linearizable
        && input.publish_rejects_scope_mismatch
        && input.control_plane_auth_enforced;
    let production_bounded_failover_safe = multi_writer_fencing_safe
        && input.authoritative_backend_time
        && input.bounded_wall_clock_failover;
    let production_multi_writer_safe = production_bounded_failover_safe;
    StandingRuntimeFencingCapability {
        capability_schema_version: STANDING_RUNTIME_FENCING_CAPABILITY_SCHEMA_VERSION,
        backend_name: input.backend_name.to_string(),
        owner_scope_kind: STANDING_RUNTIME_OWNER_SCOPE_KIND_TENANT_PROGRAM_VIEW.to_string(),
        linearizable_owner_lease: input.linearizable_owner_lease,
        durable_monotonic_owner_epoch: input.durable_monotonic_owner_epoch,
        authoritative_backend_time: input.authoritative_backend_time,
        owner_validated_checkpoint_publish: input.owner_validated_checkpoint_publish,
        publish_checks_owner_and_latest_atomically: input
            .publish_checks_owner_and_latest_atomically,
        publish_rejects_expired_owner: input.publish_rejects_expired_owner,
        latest_read_linearizable: input.latest_read_linearizable,
        publish_rejects_scope_mismatch: input.publish_rejects_scope_mismatch,
        max_owner_ttl_ms: MAX_STANDING_RUNTIME_OWNER_TTL_MS,
        control_plane_auth_enforced: input.control_plane_auth_enforced,
        production_multi_writer_safe,
        backend_time_source_kind: input.backend_time_source_kind.to_string(),
        backend_time_blocked_reason: input.backend_time_blocked_reason.to_string(),
        lease_authority_kind: input.lease_authority_kind.to_string(),
        lease_expiry_semantics: input.lease_expiry_semantics.to_string(),
        bounded_wall_clock_failover: input.bounded_wall_clock_failover,
        failover_time_bound_ms: if input.bounded_wall_clock_failover {
            MAX_STANDING_RUNTIME_OWNER_TTL_MS
        } else {
            0
        },
        multi_writer_fencing_safe,
        production_bounded_failover_safe,
    }
}

fn apply_control_plane_auth_to_capability(
    capability: &mut StandingRuntimeFencingCapability,
    control_plane_auth_enforced: bool,
) {
    capability.control_plane_auth_enforced = control_plane_auth_enforced;
    capability.multi_writer_fencing_safe = capability.linearizable_owner_lease
        && capability.durable_monotonic_owner_epoch
        && capability.owner_validated_checkpoint_publish
        && capability.publish_checks_owner_and_latest_atomically
        && capability.publish_rejects_expired_owner
        && capability.latest_read_linearizable
        && capability.publish_rejects_scope_mismatch
        && capability.control_plane_auth_enforced;
    capability.production_bounded_failover_safe = capability.multi_writer_fencing_safe
        && capability.authoritative_backend_time
        && capability.bounded_wall_clock_failover;
    capability.production_multi_writer_safe = capability.production_bounded_failover_safe;
}

#[cfg(feature = "hiqlite-backend")]
fn hiqlite_standing_runtime_fencing_capability(
    control_plane_auth_enforced: bool,
) -> StandingRuntimeFencingCapability {
    // Hiqlite owner expiry and checkpoint publish consume the Raft-serialized
    // Unix timestamp inside the same Raft write transaction.
    standing_runtime_fencing_capability(StandingRuntimeFencingCapabilityInput {
        backend_name: "hiqlite",
        linearizable_owner_lease: true,
        durable_monotonic_owner_epoch: true,
        authoritative_backend_time: true,
        backend_time_source_kind: STANDING_RUNTIME_BACKEND_TIME_SOURCE_RAFT_REPLICATED,
        backend_time_blocked_reason: "",
        lease_authority_kind: STANDING_RUNTIME_LEASE_AUTHORITY_KIND_RAFT_REPLICATED_TIME,
        lease_expiry_semantics: STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_BACKEND_WALL_CLOCK_TTL,
        bounded_wall_clock_failover: true,
        owner_validated_checkpoint_publish: true,
        publish_checks_owner_and_latest_atomically: true,
        publish_rejects_expired_owner: true,
        latest_read_linearizable: true,
        publish_rejects_scope_mismatch: true,
        control_plane_auth_enforced,
    })
}

fn unix_time_ms() -> Result<u64, MetaStoreError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| MetaStoreError::TimestampOverflow)?;
    u64::try_from(duration.as_millis()).map_err(|_| MetaStoreError::TimestampOverflow)
}

fn relation_catalog_key(catalog: &VelorixRelationCatalogV1) -> (String, String) {
    (
        catalog.relation_schema.relation_id.clone(),
        catalog.relation_schema.relation_version.clone(),
    )
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), MetaStoreError> {
    if value.is_empty() {
        Err(MetaStoreError::EmptyField { field })
    } else {
        Ok(())
    }
}

pub fn validate_bearer_token(value: &str) -> Result<(), MetaStoreError> {
    require_non_empty("bearer_token", value)?;
    if value.trim() != value {
        return Err(MetaStoreError::InvalidBearerToken {
            reason: "leading or trailing whitespace is not allowed",
        });
    }
    if !value.is_ascii() {
        return Err(MetaStoreError::InvalidBearerToken {
            reason: "only ASCII bearer tokens are supported",
        });
    }
    if value.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(MetaStoreError::InvalidBearerToken {
            reason: "whitespace is not allowed",
        });
    }
    if value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(MetaStoreError::InvalidBearerToken {
            reason: "control characters are not allowed",
        });
    }
    Ok(())
}

fn oss_store_catalog_error(
    error: RelationCatalogRegistryError,
    catalog: &VelorixRelationCatalogV1,
) -> MetaStoreError {
    match error {
        RelationCatalogRegistryError::RecordConflict { .. } => {
            let (relation_id, relation_version) = relation_catalog_key(catalog);
            MetaStoreError::RelationCatalogConflict {
                relation_id,
                relation_version,
            }
        }
        other => MetaStoreError::Oss(other.to_string()),
    }
}

fn oss_read_catalog_error(
    error: RelationCatalogRegistryError,
    relation_id: &str,
    relation_version: &str,
) -> MetaStoreError {
    match error {
        RelationCatalogRegistryError::ObjectStore(object_store::Error::NotFound { .. }) => {
            MetaStoreError::RelationCatalogNotFound {
                relation_id: relation_id.to_string(),
                relation_version: relation_version.to_string(),
            }
        }
        other => MetaStoreError::Oss(other.to_string()),
    }
}

#[derive(Clone)]
pub struct MetaGrpcService<S> {
    store: S,
    expected_bearer_token: Option<String>,
}

impl<S> MetaGrpcService<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            expected_bearer_token: None,
        }
    }

    pub fn with_bearer_token(
        store: S,
        bearer_token: impl Into<String>,
    ) -> Result<Self, MetaStoreError> {
        let bearer_token = bearer_token.into();
        validate_bearer_token(&bearer_token)?;
        Ok(Self {
            store,
            expected_bearer_token: Some(bearer_token),
        })
    }

    fn control_plane_auth_enforced(&self) -> bool {
        self.expected_bearer_token.is_some()
    }

    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let Some(expected) = &self.expected_bearer_token else {
            return Ok(());
        };
        let expected = format!("Bearer {expected}");
        match request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
        {
            Some(actual) if actual == expected => Ok(()),
            _ => Err(Status::unauthenticated(
                "valid authorization bearer token is required",
            )),
        }
    }
}

#[tonic::async_trait]
impl<S> proto::velorix_meta_server::VelorixMeta for MetaGrpcService<S>
where
    S: MetaStore,
{
    async fn read_meta_store_capabilities(
        &self,
        request: Request<proto::ReadMetaStoreCapabilitiesRequest>,
    ) -> Result<Response<proto::ReadMetaStoreCapabilitiesResponse>, Status> {
        self.authorize(&request)?;
        let mut capabilities = self
            .store
            .read_meta_store_capabilities()
            .await
            .map_err(meta_status)?;
        apply_control_plane_auth_to_capability(
            &mut capabilities.standing_runtime_fencing,
            self.control_plane_auth_enforced(),
        );

        Ok(Response::new(proto::ReadMetaStoreCapabilitiesResponse {
            standing_runtime_fencing: Some(standing_runtime_fencing_capability_to_proto(
                capabilities.standing_runtime_fencing,
            )),
            partition_authority: Some(partition_authority_capability_to_proto(
                capabilities.partition_authority,
            )),
        }))
    }

    async fn commit_ingest_range(
        &self,
        request: Request<proto::ReserveIngestRangeRequest>,
    ) -> Result<Response<proto::CommitIngestRangeResponse>, Status> {
        self.authorize(&request)?;
        let outcome = self
            .store
            .commit_ingest_range(ingest_range_reservation_from_proto(request.into_inner()))
            .await
            .map_err(meta_status)?;
        Ok(Response::new(proto::CommitIngestRangeResponse {
            outcome: commit_ingest_range_outcome(&outcome).to_string(),
        }))
    }

    async fn capture_ingest_source_cut(
        &self,
        request: Request<proto::CaptureIngestSourceCutRequest>,
    ) -> Result<Response<proto::CaptureIngestSourceCutResponse>, Status> {
        self.authorize(&request)?;
        let request = serde_json::from_slice(&request.into_inner().request_json)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let source_cut = self
            .store
            .capture_ingest_source_cut(request)
            .await
            .map_err(meta_status)?;
        let source_cut_json =
            serde_json::to_vec(&source_cut).map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(proto::CaptureIngestSourceCutResponse {
            source_cut_json,
        }))
    }

    async fn begin_view_bootstrap(
        &self,
        request: Request<proto::BeginViewBootstrapRequest>,
    ) -> Result<Response<proto::BeginViewBootstrapResponse>, Status> {
        self.authorize(&request)?;
        let request = serde_json::from_slice(&request.into_inner().request_json)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .store
            .begin_view_bootstrap(request)
            .await
            .map_err(meta_status)?;
        let (outcome, control) = match outcome {
            BeginViewBootstrapOutcome::Created(control) => ("created", Some(control)),
            BeginViewBootstrapOutcome::Duplicate(control) => ("duplicate", Some(control)),
            BeginViewBootstrapOutcome::Conflict => ("conflict", None),
        };
        let control_json = control
            .map(|control| serde_json::to_vec(&control))
            .transpose()
            .map_err(|error| Status::internal(error.to_string()))?
            .unwrap_or_default();
        Ok(Response::new(proto::BeginViewBootstrapResponse {
            outcome: outcome.to_string(),
            control_json,
        }))
    }

    async fn read_view_bootstrap(
        &self,
        request: Request<proto::ReadViewBootstrapRequest>,
    ) -> Result<Response<proto::ReadViewBootstrapResponse>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        let control = self
            .store
            .read_view_bootstrap(&request.tenant_id, &request.program_id, &request.view_id)
            .await
            .map_err(meta_status)?;
        let found = control.is_some();
        let control_json = control
            .map(|control| serde_json::to_vec(&control))
            .transpose()
            .map_err(|error| Status::internal(error.to_string()))?
            .unwrap_or_default();
        Ok(Response::new(proto::ReadViewBootstrapResponse {
            found,
            control_json,
        }))
    }

    async fn fix_view_bootstrap_activation_cut(
        &self,
        request: Request<proto::FixViewBootstrapActivationCutRequest>,
    ) -> Result<Response<proto::FixViewBootstrapActivationCutResponse>, Status> {
        self.authorize(&request)?;
        let request = serde_json::from_slice(&request.into_inner().request_json)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .store
            .fix_view_bootstrap_activation_cut(request)
            .await
            .map_err(meta_status)?;
        let (outcome, control) = match outcome {
            FixViewBootstrapActivationCutOutcome::Fixed(control) => ("fixed", Some(control)),
            FixViewBootstrapActivationCutOutcome::Duplicate(control) => {
                ("duplicate", Some(control))
            }
            FixViewBootstrapActivationCutOutcome::Conflict => ("conflict", None),
        };
        Ok(Response::new(
            proto::FixViewBootstrapActivationCutResponse {
                outcome: outcome.to_string(),
                control_json: control
                    .map(|control| serde_json::to_vec(&control))
                    .transpose()
                    .map_err(|error| Status::internal(error.to_string()))?
                    .unwrap_or_default(),
            },
        ))
    }

    async fn promote_view_bootstrap(
        &self,
        request: Request<proto::PromoteViewBootstrapRequest>,
    ) -> Result<Response<proto::PromoteViewBootstrapResponse>, Status> {
        self.authorize(&request)?;
        let request = serde_json::from_slice(&request.into_inner().request_json)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .store
            .promote_view_bootstrap(request)
            .await
            .map_err(meta_status)?;
        let (outcome, control) = match outcome {
            PromoteViewBootstrapOutcome::Promoted(control) => ("promoted", Some(control)),
            PromoteViewBootstrapOutcome::Duplicate(control) => ("duplicate", Some(control)),
            PromoteViewBootstrapOutcome::Conflict => ("conflict", None),
        };
        Ok(Response::new(proto::PromoteViewBootstrapResponse {
            outcome: outcome.to_string(),
            control_json: control
                .map(|control| serde_json::to_vec(&control))
                .transpose()
                .map_err(|error| Status::internal(error.to_string()))?
                .unwrap_or_default(),
        }))
    }

    async fn store_relation_catalog(
        &self,
        request: Request<proto::StoreRelationCatalogRequest>,
    ) -> Result<Response<proto::StoreRelationCatalogResponse>, Status> {
        self.authorize(&request)?;
        let catalog =
            serde_json::from_slice::<VelorixRelationCatalogV1>(&request.into_inner().catalog_json)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .store
            .store_relation_catalog(catalog)
            .await
            .map_err(meta_status)?;

        Ok(Response::new(proto::StoreRelationCatalogResponse {
            outcome: store_relation_catalog_outcome(&outcome).to_string(),
        }))
    }

    async fn read_relation_catalog(
        &self,
        request: Request<proto::ReadRelationCatalogRequest>,
    ) -> Result<Response<proto::ReadRelationCatalogResponse>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        let catalog = self
            .store
            .read_relation_catalog(&request.relation_id, &request.relation_version)
            .await
            .map_err(meta_status)?;
        let catalog_json =
            serde_json::to_vec(&catalog).map_err(|error| Status::internal(error.to_string()))?;

        Ok(Response::new(proto::ReadRelationCatalogResponse {
            catalog_json,
        }))
    }

    async fn reserve_ingest_range(
        &self,
        request: Request<proto::ReserveIngestRangeRequest>,
    ) -> Result<Response<proto::ReserveIngestRangeResponse>, Status> {
        self.authorize(&request)?;
        let outcome = self
            .store
            .reserve_ingest_range(ingest_range_reservation_from_proto(request.into_inner()))
            .await
            .map_err(meta_status)?;

        Ok(Response::new(proto::ReserveIngestRangeResponse {
            outcome: reserve_ingest_range_outcome(&outcome).to_string(),
        }))
    }

    async fn acquire_standing_runtime_owner(
        &self,
        request: Request<proto::AcquireStandingRuntimeOwnerRequest>,
    ) -> Result<Response<proto::AcquireStandingRuntimeOwnerResponse>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        let outcome = self
            .store
            .acquire_standing_runtime_owner(AcquireStandingRuntimeOwnerRequest {
                tenant_id: request.tenant_id,
                program_id: request.program_id,
                view_id: request.view_id,
                owner_id: request.owner_id,
                ttl_ms: request.ttl_ms,
            })
            .await
            .map_err(meta_status)?;

        Ok(Response::new(proto::AcquireStandingRuntimeOwnerResponse {
            outcome: acquire_standing_runtime_owner_outcome(&outcome).to_string(),
            claim: Some(acquire_standing_runtime_owner_claim(outcome)),
        }))
    }

    async fn read_standing_runtime_owner(
        &self,
        request: Request<proto::ReadStandingRuntimeOwnerRequest>,
    ) -> Result<Response<proto::ReadStandingRuntimeOwnerResponse>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        let claim = self
            .store
            .read_standing_runtime_owner(&request.tenant_id, &request.program_id, &request.view_id)
            .await
            .map_err(meta_status)?;

        Ok(Response::new(proto::ReadStandingRuntimeOwnerResponse {
            found: claim.is_some(),
            claim: claim.map(standing_runtime_owner_claim_to_proto),
        }))
    }

    async fn publish_standing_runtime_checkpoint(
        &self,
        request: Request<proto::PublishStandingRuntimeCheckpointRequest>,
    ) -> Result<Response<proto::PublishStandingRuntimeCheckpointResponse>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        let candidate = request
            .candidate
            .ok_or_else(|| Status::invalid_argument("candidate checkpoint pointer is required"))?;
        let owner = request
            .owner
            .ok_or_else(|| Status::invalid_argument("standing runtime owner token is required"))?;
        let expected_previous = request
            .expected_previous
            .map(standing_runtime_checkpoint_pointer_from_proto)
            .transpose()
            .map_err(meta_status)?;
        let candidate =
            standing_runtime_checkpoint_pointer_from_proto(candidate).map_err(meta_status)?;
        let outcome = self
            .store
            .publish_standing_runtime_checkpoint(PublishStandingRuntimeCheckpointRequest {
                expected_previous,
                candidate,
                owner: standing_runtime_owner_token_from_proto(owner),
            })
            .await
            .map_err(meta_status)?;

        Ok(Response::new(
            proto::PublishStandingRuntimeCheckpointResponse {
                outcome: publish_standing_runtime_checkpoint_outcome(&outcome).to_string(),
            },
        ))
    }

    async fn read_standing_runtime_checkpoint(
        &self,
        request: Request<proto::ReadStandingRuntimeCheckpointRequest>,
    ) -> Result<Response<proto::ReadStandingRuntimeCheckpointResponse>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        let pointer = self
            .store
            .read_standing_runtime_checkpoint(
                &request.tenant_id,
                &request.program_id,
                &request.view_id,
            )
            .await
            .map_err(meta_status)?;

        Ok(Response::new(
            proto::ReadStandingRuntimeCheckpointResponse {
                found: pointer.is_some(),
                pointer: pointer.map(standing_runtime_checkpoint_pointer_to_proto),
            },
        ))
    }

    async fn read_view_dependency_graph_revision(
        &self,
        request: Request<proto::ReadViewDependencyGraphRevisionRequest>,
    ) -> Result<Response<proto::ReadViewDependencyGraphRevisionResponse>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        let revision = self
            .store
            .read_view_dependency_graph_revision(&request.tenant_id)
            .await
            .map_err(meta_status)?;
        Ok(Response::new(
            proto::ReadViewDependencyGraphRevisionResponse { revision },
        ))
    }

    async fn read_partition_authority_capability(
        &self,
        request: Request<proto::ReadPartitionAuthorityCapabilityRequest>,
    ) -> Result<Response<proto::ReadPartitionAuthorityCapabilityResponse>, Status> {
        self.authorize(&request)?;
        let capability = self
            .store
            .read_partition_authority_capability()
            .await
            .map_err(partition_authority_status)?;
        Ok(Response::new(
            proto::ReadPartitionAuthorityCapabilityResponse {
                capability: Some(partition_authority_capability_to_proto(capability)),
            },
        ))
    }

    async fn acquire_partition_authority(
        &self,
        request: Request<proto::AcquirePartitionAuthorityRequest>,
    ) -> Result<Response<proto::AcquirePartitionAuthorityResponse>, Status> {
        self.authorize(&request)?;
        let request = acquire_partition_authority_request_from_proto(request.into_inner())
            .map_err(partition_authority_status)?;
        let outcome = self
            .store
            .acquire_partition_authority(request)
            .await
            .map_err(partition_authority_status)?;
        Ok(Response::new(proto::AcquirePartitionAuthorityResponse {
            outcome: acquire_partition_authority_outcome(&outcome).to_string(),
            token: Some(partition_authority_token_to_proto(
                acquire_partition_authority_token(outcome),
            )),
        }))
    }

    async fn read_partition_authority(
        &self,
        request: Request<proto::ReadPartitionAuthorityRequest>,
    ) -> Result<Response<proto::ReadPartitionAuthorityResponse>, Status> {
        self.authorize(&request)?;
        let key = request
            .into_inner()
            .key
            .ok_or_else(|| Status::invalid_argument("partition authority key is required"))
            .and_then(|key| {
                partition_authority_key_from_proto(key).map_err(partition_authority_status)
            })?;
        let token = self
            .store
            .read_partition_authority(&key)
            .await
            .map_err(partition_authority_status)?;
        Ok(Response::new(proto::ReadPartitionAuthorityResponse {
            found: token.is_some(),
            token: token.map(partition_authority_token_to_proto),
        }))
    }

    async fn publish_partition_checkpoint_pointer(
        &self,
        request: Request<proto::PublishPartitionCheckpointPointerRequest>,
    ) -> Result<Response<proto::PublishPartitionCheckpointPointerResponse>, Status> {
        self.authorize(&request)?;
        let request = publish_partition_checkpoint_pointer_request_from_proto(request.into_inner())
            .map_err(partition_authority_status)?;
        let outcome = self
            .store
            .publish_partition_checkpoint_pointer(request)
            .await
            .map_err(partition_authority_status)?;
        Ok(Response::new(
            proto::PublishPartitionCheckpointPointerResponse {
                outcome: publish_partition_checkpoint_pointer_outcome(&outcome).to_string(),
            },
        ))
    }

    async fn read_partition_checkpoint_pointer(
        &self,
        request: Request<proto::ReadPartitionCheckpointPointerRequest>,
    ) -> Result<Response<proto::ReadPartitionCheckpointPointerResponse>, Status> {
        self.authorize(&request)?;
        let key = request
            .into_inner()
            .key
            .ok_or_else(|| Status::invalid_argument("partition checkpoint key is required"))
            .and_then(|key| {
                partition_authority_key_from_proto(key).map_err(partition_authority_status)
            })?;
        let pointer = self
            .store
            .read_partition_checkpoint_pointer(&key)
            .await
            .map_err(partition_authority_status)?;
        Ok(Response::new(
            proto::ReadPartitionCheckpointPointerResponse {
                found: pointer.is_some(),
                pointer: pointer.map(partition_checkpoint_pointer_to_proto),
            },
        ))
    }
}

fn store_relation_catalog_outcome(outcome: &StoreRelationCatalogOutcome) -> &'static str {
    match outcome {
        StoreRelationCatalogOutcome::Created => "created",
        StoreRelationCatalogOutcome::Duplicate => "duplicate",
    }
}

fn reserve_ingest_range_outcome(outcome: &ReserveIngestRangeOutcome) -> &'static str {
    match outcome {
        ReserveIngestRangeOutcome::Reserved => "reserved",
        ReserveIngestRangeOutcome::Duplicate => "duplicate",
        ReserveIngestRangeOutcome::Conflict => "conflict",
    }
}

fn commit_ingest_range_outcome(outcome: &CommitIngestRangeOutcome) -> &'static str {
    match outcome {
        CommitIngestRangeOutcome::Committed => "committed",
        CommitIngestRangeOutcome::Duplicate => "duplicate",
        CommitIngestRangeOutcome::Conflict => "conflict",
    }
}

fn ingest_range_reservation_from_proto(
    request: proto::ReserveIngestRangeRequest,
) -> IngestRangeReservation {
    IngestRangeReservation {
        stream_id: request.stream_id,
        partition_id: request.partition_id,
        start_offset_inclusive: request.start_offset_inclusive,
        end_offset_exclusive: request.end_offset_exclusive,
        batch_key: request.batch_key,
        payload_digest: request.payload_digest,
        relation_id: request.relation_id,
        relation_version: request.relation_version,
        schema_fingerprint: request.schema_fingerprint,
        writer_epoch: request.writer_epoch,
    }
}

fn ingest_range_reservation_to_proto(
    reservation: IngestRangeReservation,
) -> proto::ReserveIngestRangeRequest {
    proto::ReserveIngestRangeRequest {
        stream_id: reservation.stream_id,
        partition_id: reservation.partition_id,
        start_offset_inclusive: reservation.start_offset_inclusive,
        end_offset_exclusive: reservation.end_offset_exclusive,
        batch_key: reservation.batch_key,
        payload_digest: reservation.payload_digest,
        relation_id: reservation.relation_id,
        relation_version: reservation.relation_version,
        schema_fingerprint: reservation.schema_fingerprint,
        writer_epoch: reservation.writer_epoch,
    }
}

fn standing_runtime_fencing_capability_to_proto(
    capability: StandingRuntimeFencingCapability,
) -> proto::StandingRuntimeFencingCapability {
    proto::StandingRuntimeFencingCapability {
        capability_schema_version: capability.capability_schema_version,
        backend_name: capability.backend_name,
        owner_scope_kind: capability.owner_scope_kind,
        linearizable_owner_lease: capability.linearizable_owner_lease,
        durable_monotonic_owner_epoch: capability.durable_monotonic_owner_epoch,
        authoritative_backend_time: capability.authoritative_backend_time,
        owner_validated_checkpoint_publish: capability.owner_validated_checkpoint_publish,
        publish_checks_owner_and_latest_atomically: capability
            .publish_checks_owner_and_latest_atomically,
        publish_rejects_expired_owner: capability.publish_rejects_expired_owner,
        latest_read_linearizable: capability.latest_read_linearizable,
        publish_rejects_scope_mismatch: capability.publish_rejects_scope_mismatch,
        max_owner_ttl_ms: capability.max_owner_ttl_ms,
        control_plane_auth_enforced: capability.control_plane_auth_enforced,
        production_multi_writer_safe: capability.production_multi_writer_safe,
        backend_time_source_kind: capability.backend_time_source_kind,
        backend_time_blocked_reason: capability.backend_time_blocked_reason,
        lease_authority_kind: capability.lease_authority_kind,
        lease_expiry_semantics: capability.lease_expiry_semantics,
        bounded_wall_clock_failover: capability.bounded_wall_clock_failover,
        failover_time_bound_ms: capability.failover_time_bound_ms,
        multi_writer_fencing_safe: capability.multi_writer_fencing_safe,
        production_bounded_failover_safe: capability.production_bounded_failover_safe,
    }
}

fn standing_runtime_fencing_capability_from_proto(
    capability: proto::StandingRuntimeFencingCapability,
) -> StandingRuntimeFencingCapability {
    StandingRuntimeFencingCapability {
        capability_schema_version: capability.capability_schema_version,
        backend_name: capability.backend_name,
        owner_scope_kind: capability.owner_scope_kind,
        linearizable_owner_lease: capability.linearizable_owner_lease,
        durable_monotonic_owner_epoch: capability.durable_monotonic_owner_epoch,
        authoritative_backend_time: capability.authoritative_backend_time,
        owner_validated_checkpoint_publish: capability.owner_validated_checkpoint_publish,
        publish_checks_owner_and_latest_atomically: capability
            .publish_checks_owner_and_latest_atomically,
        publish_rejects_expired_owner: capability.publish_rejects_expired_owner,
        latest_read_linearizable: capability.latest_read_linearizable,
        publish_rejects_scope_mismatch: capability.publish_rejects_scope_mismatch,
        max_owner_ttl_ms: capability.max_owner_ttl_ms,
        control_plane_auth_enforced: capability.control_plane_auth_enforced,
        production_multi_writer_safe: capability.production_multi_writer_safe,
        backend_time_source_kind: capability.backend_time_source_kind,
        backend_time_blocked_reason: capability.backend_time_blocked_reason,
        lease_authority_kind: capability.lease_authority_kind,
        lease_expiry_semantics: capability.lease_expiry_semantics,
        bounded_wall_clock_failover: capability.bounded_wall_clock_failover,
        failover_time_bound_ms: capability.failover_time_bound_ms,
        multi_writer_fencing_safe: capability.multi_writer_fencing_safe,
        production_bounded_failover_safe: capability.production_bounded_failover_safe,
    }
}

fn partition_authority_capability_to_proto(
    capability: PartitionAuthorityCapability,
) -> proto::PartitionAuthorityCapability {
    proto::PartitionAuthorityCapability {
        backend_name: capability.backend_name,
        partition_scoped_authority: capability.partition_scoped_authority,
        backend_owned_time: capability.backend_owned_time,
        fenced_checkpoint_pointer_publish: capability.fenced_checkpoint_pointer_publish,
        durable_across_restart: capability.durable_across_restart,
        production_safe: capability.production_safe,
    }
}

fn partition_authority_capability_from_proto(
    capability: proto::PartitionAuthorityCapability,
) -> PartitionAuthorityCapability {
    let production_safe = capability.production_safe
        && capability.partition_scoped_authority
        && capability.backend_owned_time
        && capability.fenced_checkpoint_pointer_publish
        && capability.durable_across_restart;
    PartitionAuthorityCapability {
        backend_name: capability.backend_name,
        partition_scoped_authority: capability.partition_scoped_authority,
        backend_owned_time: capability.backend_owned_time,
        fenced_checkpoint_pointer_publish: capability.fenced_checkpoint_pointer_publish,
        durable_across_restart: capability.durable_across_restart,
        production_safe,
    }
}

fn partition_authority_key_to_proto(key: PartitionAuthorityKey) -> proto::PartitionAuthorityKey {
    proto::PartitionAuthorityKey {
        namespace: key.namespace,
        view_id: key.view_id,
        stream_id: key.stream_id,
        partition_id: key.partition_id,
    }
}

fn partition_authority_key_from_proto(
    key: proto::PartitionAuthorityKey,
) -> Result<PartitionAuthorityKey, MetaStoreError> {
    let key = PartitionAuthorityKey {
        namespace: key.namespace,
        view_id: key.view_id,
        stream_id: key.stream_id,
        partition_id: key.partition_id,
    };
    key.validate()?;
    Ok(key)
}

fn partition_authority_token_to_proto(
    token: PartitionAuthorityToken,
) -> proto::PartitionAuthorityToken {
    proto::PartitionAuthorityToken {
        key: Some(partition_authority_key_to_proto(token.key)),
        owner_id: token.owner_id,
        owner_epoch: token.owner_epoch,
        expires_at_unix_ms: token.expires_at_unix_ms,
    }
}

fn partition_authority_token_from_proto(
    token: proto::PartitionAuthorityToken,
) -> Result<PartitionAuthorityToken, MetaStoreError> {
    let key = token
        .key
        .ok_or_else(|| {
            MetaStoreError::Serialization("partition authority token key is required".into())
        })
        .and_then(partition_authority_key_from_proto)?;
    let token = PartitionAuthorityToken {
        key,
        owner_id: token.owner_id,
        owner_epoch: token.owner_epoch,
        expires_at_unix_ms: token.expires_at_unix_ms,
    };
    token.validate()?;
    Ok(token)
}

fn partition_checkpoint_pointer_to_proto(
    pointer: PartitionCheckpointPointer,
) -> proto::PartitionCheckpointPointer {
    proto::PartitionCheckpointPointer {
        key: Some(partition_authority_key_to_proto(pointer.key)),
        checkpoint_key: pointer.checkpoint_key,
    }
}

fn partition_checkpoint_pointer_from_proto(
    pointer: proto::PartitionCheckpointPointer,
) -> Result<PartitionCheckpointPointer, MetaStoreError> {
    let key = pointer
        .key
        .ok_or_else(|| MetaStoreError::Serialization("partition checkpoint key is required".into()))
        .and_then(partition_authority_key_from_proto)?;
    let pointer = PartitionCheckpointPointer {
        key,
        checkpoint_key: pointer.checkpoint_key,
    };
    pointer.validate()?;
    Ok(pointer)
}

fn acquire_partition_authority_request_from_proto(
    request: proto::AcquirePartitionAuthorityRequest,
) -> Result<AcquirePartitionAuthorityRequest, MetaStoreError> {
    let key = request
        .key
        .ok_or_else(|| MetaStoreError::Serialization("partition authority key is required".into()))
        .and_then(partition_authority_key_from_proto)?;
    let request = AcquirePartitionAuthorityRequest {
        key,
        owner_id: request.owner_id,
        current_token: request
            .current_token
            .map(partition_authority_token_from_proto)
            .transpose()?,
        ttl_ms: request.ttl_ms,
    };
    request.validate()?;
    Ok(request)
}

fn acquire_partition_authority_outcome(outcome: &AcquirePartitionAuthorityOutcome) -> &'static str {
    match outcome {
        AcquirePartitionAuthorityOutcome::Acquired(_) => "acquired",
        AcquirePartitionAuthorityOutcome::Renewed(_) => "renewed",
        AcquirePartitionAuthorityOutcome::Conflict(_) => "conflict",
    }
}

fn acquire_partition_authority_token(
    outcome: AcquirePartitionAuthorityOutcome,
) -> PartitionAuthorityToken {
    match outcome {
        AcquirePartitionAuthorityOutcome::Acquired(token)
        | AcquirePartitionAuthorityOutcome::Renewed(token)
        | AcquirePartitionAuthorityOutcome::Conflict(token) => token,
    }
}

fn publish_partition_checkpoint_pointer_request_from_proto(
    request: proto::PublishPartitionCheckpointPointerRequest,
) -> Result<PublishPartitionCheckpointPointerRequest, MetaStoreError> {
    let candidate = request
        .candidate
        .ok_or_else(|| {
            MetaStoreError::Serialization(
                "candidate partition checkpoint pointer is required".into(),
            )
        })
        .and_then(partition_checkpoint_pointer_from_proto)?;
    let authority = request
        .authority
        .ok_or_else(|| {
            MetaStoreError::Serialization("partition authority token is required".into())
        })
        .and_then(partition_authority_token_from_proto)?;
    let request = PublishPartitionCheckpointPointerRequest {
        expected_previous: request
            .expected_previous
            .map(partition_checkpoint_pointer_from_proto)
            .transpose()?,
        candidate,
        authority,
    };
    request.validate()?;
    Ok(request)
}

fn publish_partition_checkpoint_pointer_outcome(
    outcome: &PublishPartitionCheckpointPointerOutcome,
) -> &'static str {
    match outcome {
        PublishPartitionCheckpointPointerOutcome::Published => "published",
        PublishPartitionCheckpointPointerOutcome::Duplicate => "duplicate",
        PublishPartitionCheckpointPointerOutcome::Conflict => "conflict",
    }
}

fn acquire_standing_runtime_owner_outcome(
    outcome: &AcquireStandingRuntimeOwnerOutcome,
) -> &'static str {
    match outcome {
        AcquireStandingRuntimeOwnerOutcome::Acquired(_) => "acquired",
        AcquireStandingRuntimeOwnerOutcome::Renewed(_) => "renewed",
        AcquireStandingRuntimeOwnerOutcome::Conflict(_) => "conflict",
    }
}

fn acquire_standing_runtime_owner_claim(
    outcome: AcquireStandingRuntimeOwnerOutcome,
) -> proto::StandingRuntimeOwnerClaim {
    match outcome {
        AcquireStandingRuntimeOwnerOutcome::Acquired(claim)
        | AcquireStandingRuntimeOwnerOutcome::Renewed(claim)
        | AcquireStandingRuntimeOwnerOutcome::Conflict(claim) => {
            standing_runtime_owner_claim_to_proto(claim)
        }
    }
}

fn publish_standing_runtime_checkpoint_outcome(
    outcome: &PublishStandingRuntimeCheckpointOutcome,
) -> &'static str {
    match outcome {
        PublishStandingRuntimeCheckpointOutcome::Published => "published",
        PublishStandingRuntimeCheckpointOutcome::Duplicate => "duplicate",
        PublishStandingRuntimeCheckpointOutcome::Conflict => "conflict",
    }
}

fn standing_runtime_owner_claim_to_proto(
    claim: StandingRuntimeOwnerClaim,
) -> proto::StandingRuntimeOwnerClaim {
    proto::StandingRuntimeOwnerClaim {
        tenant_id: claim.tenant_id,
        program_id: claim.program_id,
        view_id: claim.view_id,
        owner_id: claim.owner_id,
        owner_epoch: claim.owner_epoch,
        expires_at_unix_ms: claim.expires_at_unix_ms,
    }
}

fn standing_runtime_owner_claim_from_proto(
    claim: proto::StandingRuntimeOwnerClaim,
) -> StandingRuntimeOwnerClaim {
    StandingRuntimeOwnerClaim {
        tenant_id: claim.tenant_id,
        program_id: claim.program_id,
        view_id: claim.view_id,
        owner_id: claim.owner_id,
        owner_epoch: claim.owner_epoch,
        expires_at_unix_ms: claim.expires_at_unix_ms,
    }
}

fn standing_runtime_owner_token_to_proto(
    token: StandingRuntimeOwnerToken,
) -> proto::StandingRuntimeOwnerToken {
    proto::StandingRuntimeOwnerToken {
        tenant_id: token.tenant_id,
        program_id: token.program_id,
        view_id: token.view_id,
        owner_id: token.owner_id,
        owner_epoch: token.owner_epoch,
    }
}

fn standing_runtime_owner_token_from_proto(
    token: proto::StandingRuntimeOwnerToken,
) -> StandingRuntimeOwnerToken {
    StandingRuntimeOwnerToken {
        tenant_id: token.tenant_id,
        program_id: token.program_id,
        view_id: token.view_id,
        owner_id: token.owner_id,
        owner_epoch: token.owner_epoch,
    }
}

fn standing_runtime_checkpoint_pointer_from_proto(
    pointer: proto::StandingRuntimeCheckpointPointer,
) -> Result<StandingRuntimeCheckpointPointer, MetaStoreError> {
    let input_coverage = if pointer.input_coverage_json.is_empty() {
        None
    } else {
        Some(
            serde_json::from_slice(&pointer.input_coverage_json)
                .map_err(|error| MetaStoreError::Serialization(error.to_string()))?,
        )
    };
    Ok(StandingRuntimeCheckpointPointer {
        tenant_id: pointer.tenant_id,
        program_id: pointer.program_id,
        view_id: pointer.view_id,
        checkpoint_key: pointer.checkpoint_key,
        logical_epoch: pointer.logical_epoch,
        content_hash: pointer.content_hash,
        manifest_hash: pointer.manifest_hash,
        output_manifest_refs: pointer.output_manifest_refs,
        bootstrap_generation: pointer.bootstrap_generation,
        plan_hash: pointer.plan_hash,
        coverage_hash: pointer.coverage_hash,
        input_coverage,
        previous_checkpoint_key: pointer.previous_checkpoint_key,
        previous_manifest_hash: pointer.previous_manifest_hash,
    })
}

fn standing_runtime_checkpoint_pointer_to_proto(
    pointer: StandingRuntimeCheckpointPointer,
) -> proto::StandingRuntimeCheckpointPointer {
    proto::StandingRuntimeCheckpointPointer {
        tenant_id: pointer.tenant_id,
        program_id: pointer.program_id,
        view_id: pointer.view_id,
        checkpoint_key: pointer.checkpoint_key,
        logical_epoch: pointer.logical_epoch,
        content_hash: pointer.content_hash,
        manifest_hash: pointer.manifest_hash,
        output_manifest_refs: pointer.output_manifest_refs,
        bootstrap_generation: pointer.bootstrap_generation,
        plan_hash: pointer.plan_hash,
        coverage_hash: pointer.coverage_hash,
        input_coverage_json: pointer
            .input_coverage
            .and_then(|coverage| serde_json::to_vec(&coverage).ok())
            .unwrap_or_default(),
        previous_checkpoint_key: pointer.previous_checkpoint_key,
        previous_manifest_hash: pointer.previous_manifest_hash,
    }
}

fn meta_status(error: MetaStoreError) -> Status {
    match error {
        MetaStoreError::RelationCatalogNotFound { .. } => Status::not_found(error.to_string()),
        MetaStoreError::RelationCatalogConflict { .. } => Status::already_exists(error.to_string()),
        MetaStoreError::RelationSchema(_)
        | MetaStoreError::EmptyIngestRange { .. }
        | MetaStoreError::EmptyField { .. }
        | MetaStoreError::InvalidBearerToken { .. }
        | MetaStoreError::InvalidDuration { .. }
        | MetaStoreError::IntegerOutOfRange { .. }
        | MetaStoreError::TimestampOverflow
        | MetaStoreError::AuthorityEpochOverflow
        | MetaStoreError::Serialization(_)
        | MetaStoreError::NonMonotonicCheckpointEpoch { .. }
        | MetaStoreError::StandingRuntimeCheckpointScopeMismatch
        | MetaStoreError::StandingRuntimeOwnerMismatch
        | MetaStoreError::PartitionCheckpointScopeMismatch
        | MetaStoreError::PartitionAuthorityTokenScopeMismatch
        | MetaStoreError::PartitionAuthorityInvalidToken
        | MetaStoreError::DuplicateSourceCutRelation { .. }
        | MetaStoreError::OverlappingSourceCutRange { .. }
        | MetaStoreError::UnexpectedOutcome(_) => Status::invalid_argument(error.to_string()),
        MetaStoreError::UnsupportedCapability(_) => Status::failed_precondition(error.to_string()),
        MetaStoreError::Remote(_) | MetaStoreError::Oss(_) | MetaStoreError::Hiqlite(_) => {
            Status::unavailable(error.to_string())
        }
    }
}

fn partition_authority_status(error: MetaStoreError) -> Status {
    match error {
        MetaStoreError::EmptyField { .. }
        | MetaStoreError::InvalidDuration { .. }
        | MetaStoreError::IntegerOutOfRange { .. }
        | MetaStoreError::Serialization(_)
        | MetaStoreError::PartitionCheckpointScopeMismatch
        | MetaStoreError::PartitionAuthorityTokenScopeMismatch => {
            Status::invalid_argument(error.to_string())
        }
        MetaStoreError::PartitionAuthorityInvalidToken => {
            Status::failed_precondition(error.to_string())
        }
        MetaStoreError::UnsupportedCapability(_) => Status::unimplemented(error.to_string()),
        MetaStoreError::AuthorityEpochOverflow => Status::aborted(error.to_string()),
        MetaStoreError::RelationSchema(_)
        | MetaStoreError::RelationCatalogConflict { .. }
        | MetaStoreError::RelationCatalogNotFound { .. }
        | MetaStoreError::EmptyIngestRange { .. }
        | MetaStoreError::InvalidBearerToken { .. }
        | MetaStoreError::TimestampOverflow
        | MetaStoreError::StandingRuntimeCheckpointScopeMismatch
        | MetaStoreError::StandingRuntimeOwnerMismatch
        | MetaStoreError::DuplicateSourceCutRelation { .. }
        | MetaStoreError::OverlappingSourceCutRange { .. }
        | MetaStoreError::NonMonotonicCheckpointEpoch { .. }
        | MetaStoreError::Remote(_)
        | MetaStoreError::Oss(_)
        | MetaStoreError::Hiqlite(_)
        | MetaStoreError::UnexpectedOutcome(_) => Status::internal(error.to_string()),
    }
}

fn partition_authority_remote_error(error: tonic::Status) -> MetaStoreError {
    match error.code() {
        tonic::Code::Unimplemented => MetaStoreError::UnsupportedCapability("partition_authority"),
        tonic::Code::FailedPrecondition => MetaStoreError::PartitionAuthorityInvalidToken,
        tonic::Code::Aborted => MetaStoreError::UnexpectedOutcome(error.message().to_string()),
        _ => MetaStoreError::Remote(error.to_string()),
    }
}

#[derive(Clone)]
pub struct GrpcMetaStore {
    client: proto::velorix_meta_client::VelorixMetaClient<Channel>,
    bearer_token: Option<MetadataValue<tonic::metadata::Ascii>>,
}

#[cfg(feature = "hiqlite-backend")]
#[derive(Clone)]
pub struct HiqliteMetaStore {
    client: hiqlite::Client,
}

#[cfg(feature = "hiqlite-backend")]
impl HiqliteMetaStore {
    pub async fn new(client: hiqlite::Client) -> Result<Self, MetaStoreError> {
        let store = Self { client };
        store.initialize_schema().await?;
        Ok(store)
    }

    pub async fn connect_remote(
        nodes: Vec<String>,
        api_secret: String,
        with_proxy: bool,
    ) -> Result<Self, MetaStoreError> {
        let client = hiqlite::Client::remote(nodes, false, false, api_secret, with_proxy)
            .await
            .map_err(hiqlite_error)?;
        Self::new(client).await
    }

    async fn initialize_schema(&self) -> Result<(), MetaStoreError> {
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_relation_catalogs (
                    relation_id TEXT NOT NULL,
                    relation_version TEXT NOT NULL,
                    catalog_json BLOB NOT NULL,
                    PRIMARY KEY (relation_id, relation_version)
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_ingest_reservations (
                    stream_id TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    start_offset_inclusive INTEGER NOT NULL,
                    end_offset_exclusive INTEGER NOT NULL,
                    batch_key TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    relation_id TEXT NOT NULL,
                    relation_version TEXT NOT NULL,
                    schema_fingerprint TEXT NOT NULL,
                    writer_epoch INTEGER NOT NULL,
                    committed INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (
                        stream_id,
                        partition_id,
                        start_offset_inclusive,
                        end_offset_exclusive
                    )
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        if !self
            .hiqlite_table_has_column(
                "PRAGMA table_info(velorix_ingest_reservations)",
                "committed",
            )
            .await?
        {
            self.client
                .execute(
                    "ALTER TABLE velorix_ingest_reservations
                        ADD COLUMN committed INTEGER NOT NULL DEFAULT 0",
                    vec![],
                )
                .await
                .map_err(hiqlite_error)?;
        }
        self.client
            .execute(
                "CREATE INDEX IF NOT EXISTS velorix_ingest_reservations_range_idx
                    ON velorix_ingest_reservations (
                        stream_id,
                        partition_id,
                        start_offset_inclusive,
                        end_offset_exclusive
                    )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_view_bootstrap_controls (
                    tenant_id TEXT NOT NULL,
                    program_id TEXT NOT NULL,
                    view_id TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    bootstrap_generation INTEGER NOT NULL,
                    plan_hash TEXT NOT NULL,
                    view_spec_json BLOB NOT NULL,
                    lifecycle TEXT NOT NULL,
                    input_catalog_epoch INTEGER NOT NULL,
                    activation_cut_json TEXT NOT NULL DEFAULT '',
                    active_checkpoint_json TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (tenant_id, program_id, view_id)
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        for (column, definition) in [
            ("activation_cut_json", "TEXT NOT NULL DEFAULT ''"),
            ("active_checkpoint_json", "TEXT NOT NULL DEFAULT ''"),
        ] {
            if !self
                .hiqlite_table_has_column(
                    "PRAGMA table_info(velorix_view_bootstrap_controls)",
                    column,
                )
                .await?
            {
                self.client
                    .execute(
                        format!(
                            "ALTER TABLE velorix_view_bootstrap_controls ADD COLUMN {column} {definition}"
                        ),
                        vec![],
                    )
                    .await
                    .map_err(hiqlite_error)?;
            }
        }
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_view_bootstrap_inputs (
                    tenant_id TEXT NOT NULL,
                    program_id TEXT NOT NULL,
                    view_id TEXT NOT NULL,
                    bootstrap_generation INTEGER NOT NULL,
                    relation_ordinal INTEGER NOT NULL,
                    relation_id TEXT NOT NULL,
                    relation_version TEXT NOT NULL,
                    schema_fingerprint TEXT NOT NULL,
                    PRIMARY KEY (
                        tenant_id,
                        program_id,
                        view_id,
                        bootstrap_generation,
                        relation_ordinal
                    )
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_view_bootstrap_view_inputs (
                    tenant_id TEXT NOT NULL,
                    program_id TEXT NOT NULL,
                    view_id TEXT NOT NULL,
                    bootstrap_generation INTEGER NOT NULL,
                    edge_ordinal INTEGER NOT NULL,
                    edge_json TEXT NOT NULL,
                    PRIMARY KEY (
                        tenant_id,
                        program_id,
                        view_id,
                        bootstrap_generation,
                        edge_ordinal
                    )
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_view_dependency_graph_heads (
                    tenant_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    PRIMARY KEY (tenant_id)
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_view_bootstrap_reservations (
                    tenant_id TEXT NOT NULL,
                    program_id TEXT NOT NULL,
                    view_id TEXT NOT NULL,
                    bootstrap_generation INTEGER NOT NULL,
                    admission_epoch INTEGER NOT NULL,
                    stream_id TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    start_offset_inclusive INTEGER NOT NULL,
                    end_offset_exclusive INTEGER NOT NULL,
                    batch_key TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    relation_id TEXT NOT NULL,
                    relation_version TEXT NOT NULL,
                    schema_fingerprint TEXT NOT NULL,
                    writer_epoch INTEGER NOT NULL,
                    committed INTEGER NOT NULL,
                    PRIMARY KEY (
                        tenant_id,
                        program_id,
                        view_id,
                        bootstrap_generation,
                        admission_epoch
                    )
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_standing_runtime_checkpoints (
                    tenant_id TEXT NOT NULL,
                    program_id TEXT NOT NULL,
                    view_id TEXT NOT NULL,
                    checkpoint_key TEXT NOT NULL,
                    logical_epoch INTEGER NOT NULL,
                    content_hash TEXT NOT NULL,
                    manifest_hash TEXT NOT NULL DEFAULT '',
                    output_manifest_refs_json TEXT NOT NULL DEFAULT '[]',
                    bootstrap_generation INTEGER NOT NULL DEFAULT 0,
                    plan_hash TEXT NOT NULL DEFAULT '',
                    coverage_hash TEXT NOT NULL DEFAULT '',
                    input_coverage_json TEXT NOT NULL DEFAULT '',
                    previous_checkpoint_key TEXT NOT NULL DEFAULT '',
                    previous_manifest_hash TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (tenant_id, program_id, view_id)
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        if !self
            .hiqlite_table_has_column(
                "PRAGMA table_info(velorix_standing_runtime_checkpoints)",
                "manifest_hash",
            )
            .await?
        {
            self.client
                .execute(
                    "ALTER TABLE velorix_standing_runtime_checkpoints
                        ADD COLUMN manifest_hash TEXT NOT NULL DEFAULT ''",
                    vec![],
                )
                .await
                .map_err(hiqlite_error)?;
        }
        for (column, definition) in [
            ("bootstrap_generation", "INTEGER NOT NULL DEFAULT 0"),
            ("plan_hash", "TEXT NOT NULL DEFAULT ''"),
            ("coverage_hash", "TEXT NOT NULL DEFAULT ''"),
            ("input_coverage_json", "TEXT NOT NULL DEFAULT ''"),
            ("previous_checkpoint_key", "TEXT NOT NULL DEFAULT ''"),
            ("previous_manifest_hash", "TEXT NOT NULL DEFAULT ''"),
        ] {
            if !self
                .hiqlite_table_has_column(
                    "PRAGMA table_info(velorix_standing_runtime_checkpoints)",
                    column,
                )
                .await?
            {
                self.client
                    .execute(
                        format!(
                            "ALTER TABLE velorix_standing_runtime_checkpoints ADD COLUMN {column} {definition}"
                        ),
                        vec![],
                    )
                    .await
                    .map_err(hiqlite_error)?;
            }
        }
        if !self
            .hiqlite_table_has_column(
                "PRAGMA table_info(velorix_standing_runtime_checkpoints)",
                "output_manifest_refs_json",
            )
            .await?
        {
            self.client
                .execute(
                    "ALTER TABLE velorix_standing_runtime_checkpoints
                        ADD COLUMN output_manifest_refs_json TEXT NOT NULL DEFAULT '[]'",
                    vec![],
                )
                .await
                .map_err(hiqlite_error)?;
        }
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_standing_runtime_owners (
                    tenant_id TEXT NOT NULL,
                    program_id TEXT NOT NULL,
                    view_id TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    owner_epoch INTEGER NOT NULL,
                    expires_at_unix_ms INTEGER NOT NULL,
                    expires_at_authority_tick INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (tenant_id, program_id, view_id)
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        if !self
            .hiqlite_table_has_column(
                "PRAGMA table_info(velorix_standing_runtime_owners)",
                "expires_at_authority_tick",
            )
            .await?
        {
            self.client
                .execute(
                    "ALTER TABLE velorix_standing_runtime_owners
                        ADD COLUMN expires_at_authority_tick INTEGER NOT NULL DEFAULT 0",
                    vec![],
                )
                .await
                .map_err(hiqlite_error)?;
        }
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_standing_runtime_authority_clocks (
                    tenant_id TEXT NOT NULL,
                    program_id TEXT NOT NULL,
                    view_id TEXT NOT NULL,
                    tick INTEGER NOT NULL CHECK (tick >= 0),
                    PRIMARY KEY (tenant_id, program_id, view_id)
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_partition_authorities (
                    namespace TEXT NOT NULL,
                    view_id TEXT NOT NULL,
                    stream_id TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    owner_id TEXT NOT NULL,
                    owner_epoch INTEGER NOT NULL,
                    expires_at_unix_ms INTEGER NOT NULL,
                    last_request_id TEXT NOT NULL,
                    last_outcome TEXT NOT NULL,
                    PRIMARY KEY (namespace, view_id, stream_id, partition_id)
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_partition_authority_requests (
                    request_id TEXT NOT NULL PRIMARY KEY,
                    request_digest TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    view_id TEXT NOT NULL,
                    stream_id TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    owner_id TEXT NOT NULL,
                    owner_epoch INTEGER NOT NULL DEFAULT 0,
                    expires_at_unix_ms INTEGER NOT NULL DEFAULT 0
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        for (table, column, definition) in [
            (
                "velorix_partition_authorities",
                "last_request_id",
                "TEXT NOT NULL DEFAULT ''",
            ),
            (
                "velorix_partition_authorities",
                "last_outcome",
                "TEXT NOT NULL DEFAULT ''",
            ),
            (
                "velorix_partition_authority_requests",
                "request_digest",
                "TEXT NOT NULL DEFAULT ''",
            ),
            (
                "velorix_partition_authority_requests",
                "outcome",
                "TEXT NOT NULL DEFAULT 'pending'",
            ),
            (
                "velorix_partition_authority_requests",
                "namespace",
                "TEXT NOT NULL DEFAULT ''",
            ),
            (
                "velorix_partition_authority_requests",
                "view_id",
                "TEXT NOT NULL DEFAULT ''",
            ),
            (
                "velorix_partition_authority_requests",
                "stream_id",
                "TEXT NOT NULL DEFAULT ''",
            ),
            (
                "velorix_partition_authority_requests",
                "partition_id",
                "INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "velorix_partition_authority_requests",
                "owner_id",
                "TEXT NOT NULL DEFAULT ''",
            ),
            (
                "velorix_partition_authority_requests",
                "owner_epoch",
                "INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "velorix_partition_authority_requests",
                "expires_at_unix_ms",
                "INTEGER NOT NULL DEFAULT 0",
            ),
        ] {
            let pragma_sql = match table {
                "velorix_partition_authorities" => {
                    "PRAGMA table_info(velorix_partition_authorities)"
                }
                "velorix_partition_authority_requests" => {
                    "PRAGMA table_info(velorix_partition_authority_requests)"
                }
                _ => unreachable!("partition authority migration table is fixed"),
            };
            if !self.hiqlite_table_has_column(pragma_sql, column).await? {
                self.client
                    .execute(
                        format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
                        vec![],
                    )
                    .await
                    .map_err(hiqlite_error)?;
            }
        }
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_partition_checkpoint_pointers (
                    namespace TEXT NOT NULL, view_id TEXT NOT NULL, stream_id TEXT NOT NULL,
                    partition_id INTEGER NOT NULL, checkpoint_key TEXT NOT NULL,
                    last_request_id TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (namespace, view_id, stream_id, partition_id)
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        self.client
            .execute(
                "CREATE TABLE IF NOT EXISTS velorix_partition_checkpoint_requests (
                    request_id TEXT NOT NULL PRIMARY KEY, request_digest TEXT NOT NULL,
                    outcome TEXT NOT NULL, namespace TEXT NOT NULL, view_id TEXT NOT NULL,
                    stream_id TEXT NOT NULL, partition_id INTEGER NOT NULL,
                    checkpoint_key TEXT NOT NULL DEFAULT ''
                )",
                vec![],
            )
            .await
            .map_err(hiqlite_error)?;
        for (table, column, definition) in [
            (
                "velorix_partition_checkpoint_pointers",
                "last_request_id",
                "TEXT NOT NULL DEFAULT ''",
            ),
            (
                "velorix_partition_checkpoint_requests",
                "request_digest",
                "TEXT NOT NULL DEFAULT ''",
            ),
            (
                "velorix_partition_checkpoint_requests",
                "outcome",
                "TEXT NOT NULL DEFAULT 'pending'",
            ),
            (
                "velorix_partition_checkpoint_requests",
                "namespace",
                "TEXT NOT NULL DEFAULT ''",
            ),
            (
                "velorix_partition_checkpoint_requests",
                "view_id",
                "TEXT NOT NULL DEFAULT ''",
            ),
            (
                "velorix_partition_checkpoint_requests",
                "stream_id",
                "TEXT NOT NULL DEFAULT ''",
            ),
            (
                "velorix_partition_checkpoint_requests",
                "partition_id",
                "INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "velorix_partition_checkpoint_requests",
                "checkpoint_key",
                "TEXT NOT NULL DEFAULT ''",
            ),
        ] {
            let pragma_sql = match table {
                "velorix_partition_checkpoint_pointers" => {
                    "PRAGMA table_info(velorix_partition_checkpoint_pointers)"
                }
                "velorix_partition_checkpoint_requests" => {
                    "PRAGMA table_info(velorix_partition_checkpoint_requests)"
                }
                _ => unreachable!("partition checkpoint migration table is fixed"),
            };
            if !self.hiqlite_table_has_column(pragma_sql, column).await? {
                self.client
                    .execute(
                        format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
                        vec![],
                    )
                    .await
                    .map_err(hiqlite_error)?;
            }
        }
        Ok(())
    }

    async fn with_schema_repair<T, F, Fut>(&self, mut operation: F) -> Result<T, MetaStoreError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, MetaStoreError>>,
    {
        match operation().await {
            Err(error) if hiqlite_meta_error_is_missing_table(&error) => {
                self.initialize_schema().await?;
                operation().await
            }
            result => result,
        }
    }

    #[cfg(feature = "hiqlite-backend")]
    async fn hiqlite_table_has_column(
        &self,
        pragma_sql: &'static str,
        column_name: &str,
    ) -> Result<bool, MetaStoreError> {
        let rows = self
            .client
            .query_map::<TableColumnRow, _>(pragma_sql, vec![])
            .await
            .map_err(hiqlite_error)?;
        Ok(rows.iter().any(|row| row.name == column_name))
    }

    #[cfg(feature = "hiqlite-backend")]
    async fn read_standing_runtime_owner_record(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeOwnerClaim>, MetaStoreError> {
        validate_standing_runtime_scope(tenant_id, program_id, view_id)?;
        let rows = self
            .with_schema_repair(|| async {
                self.client
                    .query_consistent_map::<StandingRuntimeOwnerClaimRow, _>(
                        "SELECT
                            owner.tenant_id,
                            owner.program_id,
                            owner.view_id,
                            owner_id,
                            owner_epoch,
                            expires_at_unix_ms
                        FROM velorix_standing_runtime_owners owner
                        WHERE owner.tenant_id = $1
                          AND owner.program_id = $2
                          AND owner.view_id = $3",
                        vec![
                            hiqlite::Param::from(tenant_id.to_string()),
                            hiqlite::Param::from(program_id.to_string()),
                            hiqlite::Param::from(view_id.to_string()),
                        ],
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        rows.into_iter()
            .next()
            .map(StandingRuntimeOwnerClaimRow::into_claim)
            .transpose()
    }

    async fn read_view_bootstrap_record(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<ViewBootstrapControlV1>, MetaStoreError> {
        validate_standing_runtime_scope(tenant_id, program_id, view_id)?;
        let params = || {
            vec![
                hiqlite::Param::from(tenant_id.to_string()),
                hiqlite::Param::from(program_id.to_string()),
                hiqlite::Param::from(view_id.to_string()),
            ]
        };
        let mut controls = self
            .with_schema_repair(|| async {
                self.client
                    .query_consistent_map::<ViewBootstrapControlRow, _>(
                        "SELECT tenant_id, program_id, view_id, schema_version,
                            bootstrap_generation, plan_hash, view_spec_json, lifecycle,
                            input_catalog_epoch, activation_cut_json, active_checkpoint_json
                        FROM velorix_view_bootstrap_controls
                        WHERE tenant_id = $1 AND program_id = $2 AND view_id = $3",
                        params(),
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        let Some(control) = controls.pop() else {
            return Ok(None);
        };
        let generation = u64::try_from(control.bootstrap_generation).map_err(|_| {
            MetaStoreError::Serialization(format!(
                "view bootstrap generation is negative: {}",
                control.bootstrap_generation
            ))
        })?;
        let schema_version = u32::try_from(control.schema_version).map_err(|_| {
            MetaStoreError::Serialization(format!(
                "view bootstrap schema version is invalid: {}",
                control.schema_version
            ))
        })?;
        if schema_version != VIEW_BOOTSTRAP_CONTROL_SCHEMA_VERSION_V1 {
            return Err(MetaStoreError::Serialization(format!(
                "unsupported view bootstrap schema version: {schema_version}"
            )));
        }
        let input_catalog_epoch = u64::try_from(control.input_catalog_epoch).map_err(|_| {
            MetaStoreError::Serialization(format!(
                "view bootstrap input catalog epoch is negative: {}",
                control.input_catalog_epoch
            ))
        })?;
        let lifecycle = match control.lifecycle.as_str() {
            "bootstrapping" => ViewBootstrapLifecycleV1::Bootstrapping,
            "active" => ViewBootstrapLifecycleV1::Active,
            other => {
                return Err(MetaStoreError::Serialization(format!(
                    "unsupported view bootstrap lifecycle: {other}"
                )))
            }
        };
        let inputs = self
            .client
            .query_consistent_map::<ViewBootstrapInputRow, _>(
                "SELECT relation_id, relation_version, schema_fingerprint
                FROM velorix_view_bootstrap_inputs
                WHERE tenant_id = $1 AND program_id = $2 AND view_id = $3
                  AND bootstrap_generation = $4
                ORDER BY relation_ordinal",
                vec![
                    hiqlite::Param::from(tenant_id.to_string()),
                    hiqlite::Param::from(program_id.to_string()),
                    hiqlite::Param::from(view_id.to_string()),
                    hiqlite::Param::from(control.bootstrap_generation),
                ],
            )
            .await
            .map_err(hiqlite_error)?;
        let relations = inputs
            .into_iter()
            .map(|input| IngestSourceRelationIdentityV1 {
                relation_id: input.relation_id,
                relation_version: input.relation_version,
                relation_generation: INGEST_SOURCE_IDENTITY_GENERATION_V1,
                schema_fingerprint: input.schema_fingerprint,
            })
            .collect::<Vec<_>>();
        let view_input_rows = self
            .client
            .query_consistent_map::<ViewBootstrapViewInputRow, _>(
                "SELECT CAST(edge_json AS BLOB) AS edge_json
                FROM velorix_view_bootstrap_view_inputs
                WHERE tenant_id = $1 AND program_id = $2 AND view_id = $3
                  AND bootstrap_generation = $4
                ORDER BY edge_ordinal",
                vec![
                    hiqlite::Param::from(tenant_id.to_string()),
                    hiqlite::Param::from(program_id.to_string()),
                    hiqlite::Param::from(view_id.to_string()),
                    hiqlite::Param::from(control.bootstrap_generation),
                ],
            )
            .await
            .map_err(hiqlite_error)?;
        let view_inputs = view_input_rows
            .into_iter()
            .map(|row| {
                serde_json::from_slice(&row.edge_json).map_err(|error| {
                    MetaStoreError::Serialization(format!(
                        "could not deserialize view bootstrap view input: {error}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let snapshots = self
            .client
            .query_consistent_map::<SourceCutReservationRow, _>(
                "SELECT admission_epoch, stream_id, partition_id,
                    start_offset_inclusive, end_offset_exclusive, batch_key,
                    payload_digest, relation_id, relation_version, schema_fingerprint,
                    writer_epoch, committed
                FROM velorix_view_bootstrap_reservations
                WHERE tenant_id = $1 AND program_id = $2 AND view_id = $3
                  AND bootstrap_generation = $4
                ORDER BY admission_epoch",
                vec![
                    hiqlite::Param::from(tenant_id.to_string()),
                    hiqlite::Param::from(program_id.to_string()),
                    hiqlite::Param::from(view_id.to_string()),
                    hiqlite::Param::from(control.bootstrap_generation),
                ],
            )
            .await
            .map_err(hiqlite_error)?;
        let committed_batch_keys = snapshots
            .iter()
            .filter(|row| row.committed)
            .map(|row| row.reservation.batch_key.clone())
            .collect::<BTreeSet<_>>();
        let bootstrap_cut = source_cut::build_ingest_source_cut(
            &CaptureIngestSourceCutRequest { relations },
            input_catalog_epoch,
            snapshots
                .into_iter()
                .map(|row| row.reservation.into_reservation()),
            &committed_batch_keys,
        )?;
        let activation_cut = if control.activation_cut_json.is_empty() {
            None
        } else {
            Some(
                serde_json::from_str(&control.activation_cut_json)
                    .map_err(|error| MetaStoreError::Serialization(error.to_string()))?,
            )
        };
        let active_checkpoint = if control.active_checkpoint_json.is_empty() {
            None
        } else {
            Some(
                serde_json::from_str(&control.active_checkpoint_json)
                    .map_err(|error| MetaStoreError::Serialization(error.to_string()))?,
            )
        };
        Ok(Some(ViewBootstrapControlV1 {
            schema_version,
            tenant_id: control.tenant_id,
            program_id: control.program_id,
            view_id: control.view_id,
            bootstrap_generation: generation,
            plan_hash: control.plan_hash,
            view_spec_json: control.view_spec_json,
            lifecycle,
            bootstrap_cut,
            activation_cut,
            active_checkpoint,
            view_inputs,
        }))
    }

    async fn read_partition_authority_record(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Option<PartitionAuthorityToken>, MetaStoreError> {
        let partition_id = i64::from(key.partition_id);
        let rows = self
            .with_schema_repair(|| async {
                self.client
                    .query_consistent_map::<PartitionAuthorityRow, _>(
                        "SELECT namespace, view_id, stream_id, partition_id, owner_id,
                                owner_epoch, expires_at_unix_ms
                         FROM velorix_partition_authorities
                         WHERE namespace = $1 AND view_id = $2 AND stream_id = $3
                           AND partition_id = $4",
                        vec![
                            hiqlite::Param::from(key.namespace.clone()),
                            hiqlite::Param::from(key.view_id.clone()),
                            hiqlite::Param::from(key.stream_id.clone()),
                            hiqlite::Param::from(partition_id),
                        ],
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        rows.into_iter()
            .next()
            .map(PartitionAuthorityRow::into_token)
            .transpose()
    }

    async fn read_partition_authority_request(
        &self,
        request_id: &str,
        request_digest: &str,
    ) -> Result<PartitionAuthorityRequestRow, MetaStoreError> {
        let rows = self
            .with_schema_repair(|| async {
                self.client
                    .query_consistent_map::<PartitionAuthorityRequestRow, _>(
                        "SELECT request_digest, outcome, namespace, view_id, stream_id,
                                partition_id, owner_id, owner_epoch, expires_at_unix_ms
                         FROM velorix_partition_authority_requests
                         WHERE request_id = $1 AND request_digest = $2",
                        vec![
                            hiqlite::Param::from(request_id.to_string()),
                            hiqlite::Param::from(request_digest.to_string()),
                        ],
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        rows.into_iter().next().ok_or_else(|| {
            MetaStoreError::Serialization("partition authority request status disappeared".into())
        })
    }
}

#[cfg(feature = "hiqlite-backend")]
#[async_trait]
impl MetaStore for HiqliteMetaStore {
    async fn read_meta_store_capabilities(&self) -> Result<MetaStoreCapabilities, MetaStoreError> {
        Ok(MetaStoreCapabilities {
            standing_runtime_fencing: hiqlite_standing_runtime_fencing_capability(false),
            partition_authority: PartitionAuthorityCapability::unsupported("hiqlite"),
        })
    }

    async fn store_relation_catalog(
        &self,
        catalog: VelorixRelationCatalogV1,
    ) -> Result<StoreRelationCatalogOutcome, MetaStoreError> {
        catalog.validate_supported_incremental_adapter_scope()?;
        let relation_id = catalog.relation_schema.relation_id.clone();
        let relation_version = catalog.relation_schema.relation_version.clone();
        let catalog_json = serde_json::to_vec(&catalog)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let inserted = self
            .with_schema_repair(|| async {
                self.client
                    .execute(
                        "INSERT OR IGNORE INTO velorix_relation_catalogs
                            (relation_id, relation_version, catalog_json)
                            VALUES ($1, $2, $3)",
                        vec![
                            hiqlite::Param::from(relation_id.clone()),
                            hiqlite::Param::from(relation_version.clone()),
                            hiqlite::Param::from(catalog_json.clone()),
                        ],
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        if inserted == 1 {
            return Ok(StoreRelationCatalogOutcome::Created);
        }

        let existing = self
            .read_relation_catalog(&relation_id, &relation_version)
            .await?;
        if existing == catalog {
            Ok(StoreRelationCatalogOutcome::Duplicate)
        } else {
            Err(MetaStoreError::RelationCatalogConflict {
                relation_id,
                relation_version,
            })
        }
    }

    async fn read_relation_catalog(
        &self,
        relation_id: &str,
        relation_version: &str,
    ) -> Result<VelorixRelationCatalogV1, MetaStoreError> {
        require_non_empty("relation_id", relation_id)?;
        require_non_empty("relation_version", relation_version)?;
        let rows = self
            .with_schema_repair(|| async {
                self.client
                    .query_map::<CatalogJsonRow, _>(
                        "SELECT catalog_json FROM velorix_relation_catalogs
                            WHERE relation_id = $1 AND relation_version = $2",
                        vec![
                            hiqlite::Param::from(relation_id.to_string()),
                            hiqlite::Param::from(relation_version.to_string()),
                        ],
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        let Some(row) = rows.into_iter().next() else {
            return Err(MetaStoreError::RelationCatalogNotFound {
                relation_id: relation_id.to_string(),
                relation_version: relation_version.to_string(),
            });
        };

        serde_json::from_slice(&row.catalog_json)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))
    }

    async fn reserve_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        reservation.validate()?;
        let start = i64_from_u64("start_offset_inclusive", reservation.start_offset_inclusive)?;
        let end = i64_from_u64("end_offset_exclusive", reservation.end_offset_exclusive)?;
        let writer_epoch = i64_from_u64("writer_epoch", reservation.writer_epoch)?;
        let inserted = self
            .with_schema_repair(|| async {
                self.client
                    .execute(
                        "INSERT INTO velorix_ingest_reservations (
                            stream_id,
                            partition_id,
                            start_offset_inclusive,
                            end_offset_exclusive,
                            batch_key,
                            payload_digest,
                            relation_id,
                            relation_version,
                            schema_fingerprint,
                            writer_epoch
                        )
                        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                        WHERE NOT EXISTS (
                            SELECT 1 FROM velorix_ingest_reservations
                            WHERE stream_id = $1
                              AND partition_id = $2
                              AND start_offset_inclusive < $4
                              AND $3 < end_offset_exclusive
                        )
                          AND NOT EXISTS (
                            SELECT 1
                            FROM velorix_view_bootstrap_reservations sealed
                            WHERE sealed.stream_id = $1
                              AND sealed.partition_id = $2
                              AND sealed.relation_id = $7
                              AND sealed.relation_version = $8
                              AND sealed.schema_fingerprint = $9
                            GROUP BY sealed.tenant_id, sealed.program_id, sealed.view_id,
                                sealed.bootstrap_generation
                            HAVING $3 < MIN(sealed.start_offset_inclusive)
                        )",
                        vec![
                            hiqlite::Param::from(reservation.stream_id.clone()),
                            hiqlite::Param::from(reservation.partition_id),
                            hiqlite::Param::from(start),
                            hiqlite::Param::from(end),
                            hiqlite::Param::from(reservation.batch_key.clone()),
                            hiqlite::Param::from(reservation.payload_digest.clone()),
                            hiqlite::Param::from(reservation.relation_id.clone()),
                            hiqlite::Param::from(reservation.relation_version.clone()),
                            hiqlite::Param::from(reservation.schema_fingerprint.clone()),
                            hiqlite::Param::from(writer_epoch),
                        ],
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        if inserted == 1 {
            return Ok(ReserveIngestRangeOutcome::Reserved);
        }

        let exact_rows = self
            .with_schema_repair(|| async {
                self.client
                    .query_map::<IngestReservationRow, _>(
                        "SELECT
                            stream_id,
                            partition_id,
                            start_offset_inclusive,
                            end_offset_exclusive,
                            batch_key,
                            payload_digest,
                            relation_id,
                            relation_version,
                            schema_fingerprint,
                            writer_epoch
                        FROM velorix_ingest_reservations
                        WHERE stream_id = $1
                          AND partition_id = $2
                          AND start_offset_inclusive = $3
                          AND end_offset_exclusive = $4",
                        vec![
                            hiqlite::Param::from(reservation.stream_id.clone()),
                            hiqlite::Param::from(reservation.partition_id),
                            hiqlite::Param::from(start),
                            hiqlite::Param::from(end),
                        ],
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        if exact_rows
            .into_iter()
            .any(|existing| existing.into_reservation() == reservation)
        {
            Ok(ReserveIngestRangeOutcome::Duplicate)
        } else {
            Ok(ReserveIngestRangeOutcome::Conflict)
        }
    }

    async fn commit_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<CommitIngestRangeOutcome, MetaStoreError> {
        reservation.validate()?;
        let start = i64_from_u64("start_offset_inclusive", reservation.start_offset_inclusive)?;
        let end = i64_from_u64("end_offset_exclusive", reservation.end_offset_exclusive)?;
        let writer_epoch = i64_from_u64("writer_epoch", reservation.writer_epoch)?;
        let changed = self
            .with_schema_repair(|| async {
                self.client
                    .execute(
                        "UPDATE velorix_ingest_reservations
                        SET committed = 1
                        WHERE stream_id = $1
                          AND partition_id = $2
                          AND start_offset_inclusive = $3
                          AND end_offset_exclusive = $4
                          AND batch_key = $5
                          AND payload_digest = $6
                          AND relation_id = $7
                          AND relation_version = $8
                          AND schema_fingerprint = $9
                          AND writer_epoch = $10
                          AND committed = 0",
                        vec![
                            hiqlite::Param::from(reservation.stream_id.clone()),
                            hiqlite::Param::from(reservation.partition_id),
                            hiqlite::Param::from(start),
                            hiqlite::Param::from(end),
                            hiqlite::Param::from(reservation.batch_key.clone()),
                            hiqlite::Param::from(reservation.payload_digest.clone()),
                            hiqlite::Param::from(reservation.relation_id.clone()),
                            hiqlite::Param::from(reservation.relation_version.clone()),
                            hiqlite::Param::from(reservation.schema_fingerprint.clone()),
                            hiqlite::Param::from(writer_epoch),
                        ],
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        if changed == 1 {
            return Ok(CommitIngestRangeOutcome::Committed);
        }

        let rows = self
            .with_schema_repair(|| async {
                self.client
                    .query_consistent_map::<SourceCutReservationRow, _>(
                        "SELECT
                            rowid AS admission_epoch,
                            stream_id,
                            partition_id,
                            start_offset_inclusive,
                            end_offset_exclusive,
                            batch_key,
                            payload_digest,
                            relation_id,
                            relation_version,
                            schema_fingerprint,
                            writer_epoch,
                            committed
                        FROM velorix_ingest_reservations
                        WHERE stream_id = $1
                          AND partition_id = $2
                          AND start_offset_inclusive = $3
                          AND end_offset_exclusive = $4",
                        vec![
                            hiqlite::Param::from(reservation.stream_id.clone()),
                            hiqlite::Param::from(reservation.partition_id),
                            hiqlite::Param::from(start),
                            hiqlite::Param::from(end),
                        ],
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        if rows
            .into_iter()
            .any(|row| row.committed && row.reservation.into_reservation() == reservation)
        {
            Ok(CommitIngestRangeOutcome::Duplicate)
        } else {
            Ok(CommitIngestRangeOutcome::Conflict)
        }
    }

    async fn capture_ingest_source_cut(
        &self,
        request: CaptureIngestSourceCutRequest,
    ) -> Result<IngestSourceCutV1, MetaStoreError> {
        request.validate()?;
        let rows = self
            .with_schema_repair(|| async {
                self.client
                    .query_consistent_map::<SourceCutReservationRow, _>(
                        "SELECT
                            rowid AS admission_epoch,
                            stream_id,
                            partition_id,
                            start_offset_inclusive,
                            end_offset_exclusive,
                            batch_key,
                            payload_digest,
                            relation_id,
                            relation_version,
                            schema_fingerprint,
                            writer_epoch,
                            committed
                        FROM velorix_ingest_reservations
                        ORDER BY rowid",
                        vec![],
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        let input_catalog_epoch = rows
            .iter()
            .map(|row| row.admission_epoch)
            .max()
            .unwrap_or_default();
        let input_catalog_epoch = u64::try_from(input_catalog_epoch).map_err(|_| {
            MetaStoreError::Serialization(format!(
                "ingest admission catalog epoch is negative: {input_catalog_epoch}"
            ))
        })?;
        let committed_batch_keys = rows
            .iter()
            .filter(|row| row.committed)
            .map(|row| row.reservation.batch_key.clone())
            .collect();
        source_cut::build_ingest_source_cut(
            &request,
            input_catalog_epoch,
            rows.into_iter()
                .map(|row| row.reservation.into_reservation()),
            &committed_batch_keys,
        )
    }

    async fn begin_view_bootstrap(
        &self,
        request: BeginViewBootstrapRequest,
    ) -> Result<BeginViewBootstrapOutcome, MetaStoreError> {
        request.validate()?;
        let generation = i64_from_u64("bootstrap_generation", INITIAL_VIEW_BOOTSTRAP_GENERATION)?;
        let schema_version = i64::from(VIEW_BOOTSTRAP_CONTROL_SCHEMA_VERSION_V1);
        let expected_graph_revision = if request.view_inputs.is_empty() {
            None
        } else {
            Some(i64_from_u64(
                "expected_graph_revision",
                request.expected_graph_revision,
            )?)
        };
        let control_statement = if let Some(expected_graph_revision) = expected_graph_revision {
            (
                "INSERT OR IGNORE INTO velorix_view_bootstrap_controls (
                    tenant_id, program_id, view_id, schema_version,
                    bootstrap_generation, plan_hash, view_spec_json, lifecycle,
                    input_catalog_epoch
                ) SELECT
                    $1, $2, $3, $4, $5, $6, $7, 'bootstrapping',
                    COALESCE((SELECT MAX(rowid) FROM velorix_ingest_reservations), 0)
                WHERE ($8 = 0 AND NOT EXISTS (
                    SELECT 1 FROM velorix_view_dependency_graph_heads
                    WHERE tenant_id = $1
                )) OR EXISTS (
                    SELECT 1 FROM velorix_view_dependency_graph_heads
                    WHERE tenant_id = $1 AND revision = $8
                )",
                vec![
                    hiqlite::Param::from(request.tenant_id.clone()),
                    hiqlite::Param::from(request.program_id.clone()),
                    hiqlite::Param::from(request.view_id.clone()),
                    hiqlite::Param::from(schema_version),
                    hiqlite::Param::from(generation),
                    hiqlite::Param::from(request.plan_hash.clone()),
                    hiqlite::Param::from(request.view_spec_json.clone()),
                    hiqlite::Param::from(expected_graph_revision),
                ],
            )
        } else {
            (
                "INSERT OR IGNORE INTO velorix_view_bootstrap_controls (
                tenant_id, program_id, view_id, schema_version,
                bootstrap_generation, plan_hash, view_spec_json, lifecycle,
                input_catalog_epoch
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, 'bootstrapping',
                COALESCE((SELECT MAX(rowid) FROM velorix_ingest_reservations), 0)
            )",
                vec![
                    hiqlite::Param::from(request.tenant_id.clone()),
                    hiqlite::Param::from(request.program_id.clone()),
                    hiqlite::Param::from(request.view_id.clone()),
                    hiqlite::Param::from(schema_version),
                    hiqlite::Param::from(generation),
                    hiqlite::Param::from(request.plan_hash.clone()),
                    hiqlite::Param::from(request.view_spec_json.clone()),
                ],
            )
        };
        let mut statements: Vec<(&'static str, Vec<hiqlite::Param>)> = vec![control_statement];
        if let Some(expected_graph_revision) = expected_graph_revision {
            // Keep the graph CAS immediately after the control insert: `changes()`
            // is then true only for a newly created control, never a duplicate.
            statements.push((
                "INSERT INTO velorix_view_dependency_graph_heads (tenant_id, revision)
                 SELECT $1, CASE WHEN $2 = 0 THEN 1 ELSE $2 END
                 WHERE changes() = 1
                 ON CONFLICT (tenant_id) DO UPDATE SET revision = revision + 1
                 WHERE revision = $2",
                vec![
                    hiqlite::Param::from(request.tenant_id.clone()),
                    hiqlite::Param::from(expected_graph_revision),
                ],
            ));
        }
        for (ordinal, relation) in request.relations.iter().enumerate() {
            let ordinal = i64::try_from(ordinal).map_err(|_| {
                MetaStoreError::Serialization(
                    "view bootstrap relation ordinal exceeds i64".to_string(),
                )
            })?;
            statements.push((
                "INSERT INTO velorix_view_bootstrap_inputs (
                    tenant_id, program_id, view_id, bootstrap_generation,
                    relation_ordinal, relation_id, relation_version, schema_fingerprint
                )
                SELECT $1, $2, $3, $4, $5, $6, $7, $8
                WHERE changes() = 1",
                vec![
                    hiqlite::Param::from(request.tenant_id.clone()),
                    hiqlite::Param::from(request.program_id.clone()),
                    hiqlite::Param::from(request.view_id.clone()),
                    hiqlite::Param::from(generation),
                    hiqlite::Param::from(ordinal),
                    hiqlite::Param::from(relation.relation_id.clone()),
                    hiqlite::Param::from(relation.relation_version.clone()),
                    hiqlite::Param::from(relation.schema_fingerprint.clone()),
                ],
            ));
        }
        for (ordinal, edge) in request.view_inputs.iter().enumerate() {
            let ordinal = i64::try_from(ordinal).map_err(|_| {
                MetaStoreError::Serialization(
                    "view bootstrap view-input ordinal exceeds i64".to_string(),
                )
            })?;
            let edge_json = serde_json::to_vec(edge).map_err(|error| {
                MetaStoreError::Serialization(format!(
                    "could not serialize view bootstrap view input: {error}"
                ))
            })?;
            statements.push((
                "INSERT INTO velorix_view_bootstrap_view_inputs (
                    tenant_id, program_id, view_id, bootstrap_generation,
                    edge_ordinal, edge_json
                )
                SELECT $1, $2, $3, $4, $5, $6
                WHERE changes() = 1",
                vec![
                    hiqlite::Param::from(request.tenant_id.clone()),
                    hiqlite::Param::from(request.program_id.clone()),
                    hiqlite::Param::from(request.view_id.clone()),
                    hiqlite::Param::from(generation),
                    hiqlite::Param::from(ordinal),
                    hiqlite::Param::from(edge_json),
                ],
            ));
        }
        statements.push((
            "INSERT INTO velorix_view_bootstrap_reservations (
                tenant_id, program_id, view_id, bootstrap_generation,
                admission_epoch, stream_id, partition_id, start_offset_inclusive,
                end_offset_exclusive, batch_key, payload_digest, relation_id,
                relation_version, schema_fingerprint, writer_epoch, committed
            )
            SELECT $1, $2, $3, $4, reservations.rowid,
                reservations.stream_id, reservations.partition_id,
                reservations.start_offset_inclusive, reservations.end_offset_exclusive,
                reservations.batch_key, reservations.payload_digest,
                reservations.relation_id, reservations.relation_version,
                reservations.schema_fingerprint, reservations.writer_epoch,
                reservations.committed
            FROM velorix_ingest_reservations reservations
            JOIN velorix_view_bootstrap_inputs inputs
              ON inputs.tenant_id = $1
             AND inputs.program_id = $2
             AND inputs.view_id = $3
             AND inputs.bootstrap_generation = $4
             AND inputs.relation_id = reservations.relation_id
             AND inputs.relation_version = reservations.relation_version
             AND inputs.schema_fingerprint = reservations.schema_fingerprint
            WHERE changes() = 1
              AND reservations.rowid <= (
                SELECT input_catalog_epoch
                FROM velorix_view_bootstrap_controls
                WHERE tenant_id = $1 AND program_id = $2 AND view_id = $3
              )",
            vec![
                hiqlite::Param::from(request.tenant_id.clone()),
                hiqlite::Param::from(request.program_id.clone()),
                hiqlite::Param::from(request.view_id.clone()),
                hiqlite::Param::from(generation),
            ],
        ));
        let results = self
            .with_schema_repair(|| {
                let statements = statements.clone();
                async { self.client.txn(statements).await.map_err(hiqlite_error) }
            })
            .await?;
        let created = hiqlite_txn_changed_rows(&results, 0)? == 1;
        if created
            && expected_graph_revision.is_some()
            && hiqlite_txn_changed_rows(&results, 1)? != 1
        {
            return Ok(BeginViewBootstrapOutcome::Conflict);
        }
        let Some(control) = self
            .read_view_bootstrap_record(&request.tenant_id, &request.program_id, &request.view_id)
            .await?
        else {
            return Ok(BeginViewBootstrapOutcome::Conflict);
        };
        if !request.matches(&control) {
            return Ok(BeginViewBootstrapOutcome::Conflict);
        }
        if created {
            Ok(BeginViewBootstrapOutcome::Created(control))
        } else {
            Ok(BeginViewBootstrapOutcome::Duplicate(control))
        }
    }

    async fn read_view_bootstrap(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<ViewBootstrapControlV1>, MetaStoreError> {
        self.read_view_bootstrap_record(tenant_id, program_id, view_id)
            .await
    }

    async fn fix_view_bootstrap_activation_cut(
        &self,
        request: FixViewBootstrapActivationCutRequest,
    ) -> Result<FixViewBootstrapActivationCutOutcome, MetaStoreError> {
        request.validate()?;
        if request.owner.tenant_id != request.tenant_id
            || request.owner.program_id != request.program_id
            || request.owner.view_id != request.view_id
        {
            return Ok(FixViewBootstrapActivationCutOutcome::Conflict);
        }
        let Some(control) = self
            .read_view_bootstrap_record(&request.tenant_id, &request.program_id, &request.view_id)
            .await?
        else {
            return Ok(FixViewBootstrapActivationCutOutcome::Conflict);
        };
        if control.bootstrap_generation != request.bootstrap_generation
            || control.plan_hash != request.plan_hash
        {
            return Ok(FixViewBootstrapActivationCutOutcome::Conflict);
        }
        if control.activation_cut.is_some() {
            return Ok(FixViewBootstrapActivationCutOutcome::Duplicate(control));
        }
        let Some(checkpoint) = self
            .read_standing_runtime_checkpoint(
                &request.tenant_id,
                &request.program_id,
                &request.view_id,
            )
            .await?
        else {
            return Ok(FixViewBootstrapActivationCutOutcome::Conflict);
        };
        if checkpoint.bootstrap_generation != control.bootstrap_generation
            || checkpoint.plan_hash != control.plan_hash
            || !view_bootstrap::checkpoint_covers_source_cut(&checkpoint, &control.bootstrap_cut)
        {
            return Ok(FixViewBootstrapActivationCutOutcome::Conflict);
        }
        let activation_cut = self
            .capture_ingest_source_cut(CaptureIngestSourceCutRequest {
                relations: control
                    .bootstrap_cut
                    .relations
                    .iter()
                    .map(|relation| relation.relation.clone())
                    .collect(),
            })
            .await?;
        if !view_bootstrap::source_cut_covers(&activation_cut, &control.bootstrap_cut) {
            return Ok(FixViewBootstrapActivationCutOutcome::Conflict);
        }
        let activation_cut_json = serde_json::to_string(&activation_cut)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let owner_epoch = i64_from_u64("owner_epoch", request.owner.owner_epoch)?;
        let generation = i64_from_u64("bootstrap_generation", request.bootstrap_generation)?;
        let checkpoint_epoch = i64_from_u64("logical_epoch", checkpoint.logical_epoch)?;
        let txn = self
            .with_schema_repair(|| async {
                self.client
                    .txn_with_raft_serialized_timestamp([
                        (
                            "SELECT 1 AS authorized FROM velorix_standing_runtime_owners owner
                             WHERE owner.tenant_id = $1 AND owner.program_id = $2
                               AND owner.view_id = $3 AND owner.owner_id = $4
                               AND owner.owner_epoch = $5 AND owner.expires_at_unix_ms > $6",
                            vec![
                                hiqlite::Param::from(request.owner.tenant_id.clone()),
                                hiqlite::Param::from(request.owner.program_id.clone()),
                                hiqlite::Param::from(request.owner.view_id.clone()),
                                hiqlite::Param::from(request.owner.owner_id.clone()),
                                hiqlite::Param::from(owner_epoch),
                                hiqlite::Param::raft_serialized_unix_ms(),
                            ],
                        ),
                        (
                            "UPDATE velorix_view_bootstrap_controls SET activation_cut_json = $1
                             WHERE tenant_id = $2 AND program_id = $3 AND view_id = $4
                               AND bootstrap_generation = $5 AND plan_hash = $6
                               AND lifecycle = 'bootstrapping' AND activation_cut_json = ''
                               AND EXISTS (
                                 SELECT 1 FROM velorix_standing_runtime_checkpoints checkpoint
                                  WHERE checkpoint.tenant_id = $2 AND checkpoint.program_id = $3
                                    AND checkpoint.view_id = $4 AND checkpoint.checkpoint_key = $7
                                    AND checkpoint.logical_epoch = $8 AND checkpoint.content_hash = $9
                                    AND checkpoint.manifest_hash = $10
                                    AND checkpoint.bootstrap_generation = $5
                                    AND checkpoint.plan_hash = $6 AND checkpoint.coverage_hash = $11
                               ) AND $12 = 1",
                            vec![
                                hiqlite::Param::from(activation_cut_json.clone()),
                                hiqlite::Param::from(request.tenant_id.clone()),
                                hiqlite::Param::from(request.program_id.clone()),
                                hiqlite::Param::from(request.view_id.clone()),
                                hiqlite::Param::from(generation),
                                hiqlite::Param::from(request.plan_hash.clone()),
                                hiqlite::Param::from(checkpoint.checkpoint_key.clone()),
                                hiqlite::Param::from(checkpoint_epoch),
                                hiqlite::Param::from(checkpoint.content_hash.clone()),
                                hiqlite::Param::from(checkpoint.manifest_hash.clone()),
                                hiqlite::Param::from(checkpoint.coverage_hash.clone()),
                                hiqlite::Param::StmtOutputIndexed(0, 0),
                            ],
                        ),
                    ])
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        let raft_timestamp = txn.timestamp;
        let changed = hiqlite_txn_changed_rows_or_zero_for_missing_stmt_output(txn.result, 1)?;
        let current = self
            .read_view_bootstrap_record(&request.tenant_id, &request.program_id, &request.view_id)
            .await?
            .ok_or_else(|| MetaStoreError::Serialization("view bootstrap disappeared".into()))?;
        if changed == 1 {
            return Ok(FixViewBootstrapActivationCutOutcome::Fixed(current));
        }
        validate_current_standing_runtime_owner(
            self.read_standing_runtime_owner_record(
                &request.owner.tenant_id,
                &request.owner.program_id,
                &request.owner.view_id,
            )
            .await?
            .as_ref(),
            &request.owner,
            u64::try_from(raft_timestamp.unix_ms).map_err(|_| MetaStoreError::TimestampOverflow)?,
        )?;
        if current.activation_cut.is_some() {
            Ok(FixViewBootstrapActivationCutOutcome::Duplicate(current))
        } else {
            Ok(FixViewBootstrapActivationCutOutcome::Conflict)
        }
    }

    async fn promote_view_bootstrap(
        &self,
        request: PromoteViewBootstrapRequest,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        request.validate()?;
        if request.owner.tenant_id != request.tenant_id
            || request.owner.program_id != request.program_id
            || request.owner.view_id != request.view_id
        {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        }
        let Some(control) = self
            .read_view_bootstrap_record(&request.tenant_id, &request.program_id, &request.view_id)
            .await?
        else {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        };
        if control.bootstrap_generation != request.bootstrap_generation
            || control.plan_hash != request.plan_hash
        {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        }
        if control.lifecycle == ViewBootstrapLifecycleV1::Active {
            return if control.active_checkpoint.as_ref() == Some(&request.checkpoint) {
                Ok(PromoteViewBootstrapOutcome::Duplicate(control))
            } else {
                Ok(PromoteViewBootstrapOutcome::Conflict)
            };
        }
        let Some(activation_cut) = control.activation_cut.as_ref() else {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        };
        let Some(authoritative_checkpoint) = self
            .read_standing_runtime_checkpoint(
                &request.tenant_id,
                &request.program_id,
                &request.view_id,
            )
            .await?
        else {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        };
        if authoritative_checkpoint != request.checkpoint
            || authoritative_checkpoint.bootstrap_generation != request.bootstrap_generation
            || authoritative_checkpoint.plan_hash != request.plan_hash
            || !view_bootstrap::checkpoint_covers_source_cut(
                &authoritative_checkpoint,
                activation_cut,
            )
        {
            return Ok(PromoteViewBootstrapOutcome::Conflict);
        }
        let checkpoint_json = serde_json::to_string(&authoritative_checkpoint)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let activation_cut_json = serde_json::to_string(activation_cut)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let owner_epoch = i64_from_u64("owner_epoch", request.owner.owner_epoch)?;
        let generation = i64_from_u64("bootstrap_generation", request.bootstrap_generation)?;
        let checkpoint_epoch =
            i64_from_u64("logical_epoch", authoritative_checkpoint.logical_epoch)?;
        let txn = self
            .with_schema_repair(|| async {
                self.client
                    .txn_with_raft_serialized_timestamp([
                        (
                            "SELECT 1 AS authorized FROM velorix_standing_runtime_owners owner
                             WHERE owner.tenant_id = $1 AND owner.program_id = $2
                               AND owner.view_id = $3 AND owner.owner_id = $4
                               AND owner.owner_epoch = $5 AND owner.expires_at_unix_ms > $6",
                            vec![
                                hiqlite::Param::from(request.owner.tenant_id.clone()),
                                hiqlite::Param::from(request.owner.program_id.clone()),
                                hiqlite::Param::from(request.owner.view_id.clone()),
                                hiqlite::Param::from(request.owner.owner_id.clone()),
                                hiqlite::Param::from(owner_epoch),
                                hiqlite::Param::raft_serialized_unix_ms(),
                            ],
                        ),
                        (
                            "UPDATE velorix_view_bootstrap_controls
                             SET lifecycle = 'active', active_checkpoint_json = $1
                             WHERE tenant_id = $2 AND program_id = $3 AND view_id = $4
                               AND bootstrap_generation = $5 AND plan_hash = $6
                               AND lifecycle = 'bootstrapping' AND activation_cut_json = $7
                               AND EXISTS (
                                 SELECT 1 FROM velorix_standing_runtime_checkpoints checkpoint
                                  WHERE checkpoint.tenant_id = $2 AND checkpoint.program_id = $3
                                    AND checkpoint.view_id = $4 AND checkpoint.checkpoint_key = $8
                                    AND checkpoint.logical_epoch = $9 AND checkpoint.content_hash = $10
                                    AND checkpoint.manifest_hash = $11
                                    AND checkpoint.bootstrap_generation = $5
                                    AND checkpoint.plan_hash = $6 AND checkpoint.coverage_hash = $12
                               ) AND $13 = 1",
                            vec![
                                hiqlite::Param::from(checkpoint_json.clone()),
                                hiqlite::Param::from(request.tenant_id.clone()),
                                hiqlite::Param::from(request.program_id.clone()),
                                hiqlite::Param::from(request.view_id.clone()),
                                hiqlite::Param::from(generation),
                                hiqlite::Param::from(request.plan_hash.clone()),
                                hiqlite::Param::from(activation_cut_json.clone()),
                                hiqlite::Param::from(authoritative_checkpoint.checkpoint_key.clone()),
                                hiqlite::Param::from(checkpoint_epoch),
                                hiqlite::Param::from(authoritative_checkpoint.content_hash.clone()),
                                hiqlite::Param::from(authoritative_checkpoint.manifest_hash.clone()),
                                hiqlite::Param::from(authoritative_checkpoint.coverage_hash.clone()),
                                hiqlite::Param::StmtOutputIndexed(0, 0),
                            ],
                        ),
                    ])
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        let raft_timestamp = txn.timestamp;
        let changed = hiqlite_txn_changed_rows_or_zero_for_missing_stmt_output(txn.result, 1)?;
        let current = self
            .read_view_bootstrap_record(&request.tenant_id, &request.program_id, &request.view_id)
            .await?
            .ok_or_else(|| MetaStoreError::Serialization("view bootstrap disappeared".into()))?;
        if changed == 1 {
            return Ok(PromoteViewBootstrapOutcome::Promoted(current));
        }
        validate_current_standing_runtime_owner(
            self.read_standing_runtime_owner_record(
                &request.owner.tenant_id,
                &request.owner.program_id,
                &request.owner.view_id,
            )
            .await?
            .as_ref(),
            &request.owner,
            u64::try_from(raft_timestamp.unix_ms).map_err(|_| MetaStoreError::TimestampOverflow)?,
        )?;
        if current.lifecycle == ViewBootstrapLifecycleV1::Active
            && current.active_checkpoint.as_ref() == Some(&request.checkpoint)
        {
            Ok(PromoteViewBootstrapOutcome::Duplicate(current))
        } else {
            Ok(PromoteViewBootstrapOutcome::Conflict)
        }
    }

    async fn acquire_standing_runtime_owner(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        request.validate()?;
        let active_before = self
            .read_standing_runtime_owner_record(
                &request.tenant_id,
                &request.program_id,
                &request.view_id,
            )
            .await?;
        let ttl_ms = i64_from_u64("ttl_ms", request.ttl_ms)?;
        let txn = self
            .with_schema_repair(|| async {
                self.client
                    .txn_with_raft_serialized_timestamp([(
                        "INSERT INTO velorix_standing_runtime_owners (
                        tenant_id,
                        program_id,
                        view_id,
                        owner_id,
                        owner_epoch,
                        expires_at_unix_ms,
                        expires_at_authority_tick
                    )
                    VALUES (
                        $1,
                        $2,
                        $3,
                        $4,
                        1,
                        $5 + $6,
                        0
                    )
                    ON CONFLICT(tenant_id, program_id, view_id) DO UPDATE SET
                        owner_id = excluded.owner_id,
                        owner_epoch = CASE
                            WHEN velorix_standing_runtime_owners.expires_at_unix_ms > $5
                             AND velorix_standing_runtime_owners.owner_id = excluded.owner_id
                            THEN velorix_standing_runtime_owners.owner_epoch
                            ELSE velorix_standing_runtime_owners.owner_epoch + 1
                        END,
                        expires_at_unix_ms = excluded.expires_at_unix_ms,
                        expires_at_authority_tick = excluded.expires_at_authority_tick
                    WHERE velorix_standing_runtime_owners.owner_id = excluded.owner_id
                       OR velorix_standing_runtime_owners.expires_at_unix_ms <= $5",
                        vec![
                            hiqlite::Param::from(request.tenant_id.clone()),
                            hiqlite::Param::from(request.program_id.clone()),
                            hiqlite::Param::from(request.view_id.clone()),
                            hiqlite::Param::from(request.owner_id.clone()),
                            hiqlite::Param::raft_serialized_unix_ms(),
                            hiqlite::Param::from(ttl_ms),
                        ],
                    )])
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        let results = txn.result.map_err(hiqlite_error)?;
        let raft_timestamp = txn.timestamp;
        let changed = hiqlite_txn_changed_rows(&results, 0)?;
        if changed == 0 {
            let current = self
                .read_standing_runtime_owner_record(
                    &request.tenant_id,
                    &request.program_id,
                    &request.view_id,
                )
                .await?
                .ok_or(MetaStoreError::StandingRuntimeOwnerMismatch)?;
            return Ok(AcquireStandingRuntimeOwnerOutcome::Conflict(current));
        }

        let claim = self
            .read_standing_runtime_owner_record(
                &request.tenant_id,
                &request.program_id,
                &request.view_id,
            )
            .await?
            .ok_or(MetaStoreError::StandingRuntimeOwnerMismatch)?;
        if active_before.as_ref().is_some_and(|current| {
            current.owner_id == request.owner_id
                && current.expires_at_unix_ms > raft_timestamp.unix_ms as u64
        }) {
            Ok(AcquireStandingRuntimeOwnerOutcome::Renewed(claim))
        } else {
            Ok(AcquireStandingRuntimeOwnerOutcome::Acquired(claim))
        }
    }

    async fn read_standing_runtime_owner(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeOwnerClaim>, MetaStoreError> {
        validate_standing_runtime_scope(tenant_id, program_id, view_id)?;
        let txn = self
            .with_schema_repair(|| async {
                self.client
                    .txn_with_raft_serialized_timestamp([(
                        "UPDATE velorix_standing_runtime_owners
                    SET expires_at_unix_ms = expires_at_unix_ms
                    WHERE 0",
                        vec![],
                    )])
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        txn.result.map_err(hiqlite_error)?;
        let now =
            u64::try_from(txn.timestamp.unix_ms).map_err(|_| MetaStoreError::TimestampOverflow)?;
        Ok(self
            .read_standing_runtime_owner_record(tenant_id, program_id, view_id)
            .await?
            .filter(|claim| claim.expires_at_unix_ms > now))
    }

    async fn read_partition_authority_capability(
        &self,
    ) -> Result<PartitionAuthorityCapability, MetaStoreError> {
        Ok(PartitionAuthorityCapability {
            backend_name: "hiqlite".to_string(),
            partition_scoped_authority: true,
            backend_owned_time: true,
            fenced_checkpoint_pointer_publish: true,
            durable_across_restart: true,
            production_safe: true,
        })
    }

    async fn acquire_partition_authority(
        &self,
        request: AcquirePartitionAuthorityRequest,
    ) -> Result<AcquirePartitionAuthorityOutcome, MetaStoreError> {
        request.validate()?;
        let (request_id, request_digest) = partition_authority_request_identity(&request);
        let partition_id = i64::from(request.key.partition_id);
        let ttl_ms = i64_from_u64("ttl_ms", request.ttl_ms)?;
        let (token_epoch, token_expiry) = request
            .current_token
            .as_ref()
            .map(|token| {
                Ok::<_, MetaStoreError>((
                    i64_from_u64("current_token.owner_epoch", token.owner_epoch)?,
                    i64_from_u64("current_token.expires_at_unix_ms", token.expires_at_unix_ms)?,
                ))
            })
            .transpose()?
            .unwrap_or((0, 0));
        self.with_schema_repair(|| async {
            self.client.txn_with_raft_serialized_timestamp([
                ("INSERT OR IGNORE INTO velorix_partition_authority_requests (request_id, request_digest, outcome, namespace, view_id, stream_id, partition_id, owner_id) VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7)", vec![hiqlite::Param::from(request_id.clone()), hiqlite::Param::from(request_digest.clone()), hiqlite::Param::from(request.key.namespace.clone()), hiqlite::Param::from(request.key.view_id.clone()), hiqlite::Param::from(request.key.stream_id.clone()), hiqlite::Param::from(partition_id), hiqlite::Param::from(request.owner_id.clone())]),
                ("INSERT INTO velorix_partition_authorities (namespace, view_id, stream_id, partition_id, owner_id, owner_epoch, expires_at_unix_ms, last_request_id, last_outcome) SELECT $1, $2, $3, $4, $5, 1, $6 + $7, $8, 'acquired' WHERE EXISTS (SELECT 1 FROM velorix_partition_authority_requests WHERE request_id = $8 AND request_digest = $9 AND outcome = 'pending') ON CONFLICT(namespace, view_id, stream_id, partition_id) DO UPDATE SET owner_id = excluded.owner_id, owner_epoch = velorix_partition_authorities.owner_epoch + 1, expires_at_unix_ms = excluded.expires_at_unix_ms, last_request_id = excluded.last_request_id, last_outcome = excluded.last_outcome WHERE velorix_partition_authorities.expires_at_unix_ms <= $6 AND velorix_partition_authorities.owner_epoch < 9223372036854775807", vec![hiqlite::Param::from(request.key.namespace.clone()), hiqlite::Param::from(request.key.view_id.clone()), hiqlite::Param::from(request.key.stream_id.clone()), hiqlite::Param::from(partition_id), hiqlite::Param::from(request.owner_id.clone()), hiqlite::Param::raft_serialized_unix_ms(), hiqlite::Param::from(ttl_ms), hiqlite::Param::from(request_id.clone()), hiqlite::Param::from(request_digest.clone())]),
                ("UPDATE velorix_partition_authorities SET expires_at_unix_ms = $1 + $2, last_request_id = $3, last_outcome = 'renewed' WHERE namespace = $4 AND view_id = $5 AND stream_id = $6 AND partition_id = $7 AND owner_id = $8 AND owner_epoch = $9 AND expires_at_unix_ms = $10 AND expires_at_unix_ms > $1 AND EXISTS (SELECT 1 FROM velorix_partition_authority_requests WHERE request_id = $3 AND request_digest = $11 AND outcome = 'pending')", vec![hiqlite::Param::raft_serialized_unix_ms(), hiqlite::Param::from(ttl_ms), hiqlite::Param::from(request_id.clone()), hiqlite::Param::from(request.key.namespace.clone()), hiqlite::Param::from(request.key.view_id.clone()), hiqlite::Param::from(request.key.stream_id.clone()), hiqlite::Param::from(partition_id), hiqlite::Param::from(request.owner_id.clone()), hiqlite::Param::from(token_epoch), hiqlite::Param::from(token_expiry), hiqlite::Param::from(request_digest.clone())]),
                (
                    "UPDATE velorix_partition_authority_requests SET outcome = CASE WHEN EXISTS (SELECT 1 FROM velorix_partition_authorities WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4 AND owner_epoch = 9223372036854775807 AND expires_at_unix_ms <= $5) THEN 'epoch_overflow' ELSE COALESCE((SELECT last_outcome FROM velorix_partition_authorities WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4 AND last_request_id = $6), 'conflict') END, owner_epoch = COALESCE((SELECT owner_epoch FROM velorix_partition_authorities WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4), 0), owner_id = COALESCE((SELECT owner_id FROM velorix_partition_authorities WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4), owner_id), expires_at_unix_ms = COALESCE((SELECT expires_at_unix_ms FROM velorix_partition_authorities WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4), 0) WHERE request_id = $6 AND request_digest = $7 AND outcome = 'pending'",
                    vec![hiqlite::Param::from(request.key.namespace.clone()), hiqlite::Param::from(request.key.view_id.clone()), hiqlite::Param::from(request.key.stream_id.clone()), hiqlite::Param::from(partition_id), hiqlite::Param::raft_serialized_unix_ms(), hiqlite::Param::from(request_id.clone()), hiqlite::Param::from(request_digest.clone())],
                ),
            ]).await.map(|_| ()).map_err(hiqlite_error)
        }).await?;
        self.read_partition_authority_request(&request_id, &request_digest)
            .await?
            .into_outcome()
    }

    async fn read_partition_authority(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Option<PartitionAuthorityToken>, MetaStoreError> {
        key.validate()?;
        let txn = self.with_schema_repair(|| async { self.client.txn_with_raft_serialized_timestamp([("UPDATE velorix_partition_authorities SET expires_at_unix_ms = expires_at_unix_ms WHERE 0", vec![])]).await.map_err(hiqlite_error) }).await?;
        txn.result.map_err(hiqlite_error)?;
        let now =
            u64::try_from(txn.timestamp.unix_ms).map_err(|_| MetaStoreError::TimestampOverflow)?;
        Ok(self
            .read_partition_authority_record(key)
            .await?
            .filter(|token| token.expires_at_unix_ms > now))
    }

    async fn publish_partition_checkpoint_pointer(
        &self,
        request: PublishPartitionCheckpointPointerRequest,
    ) -> Result<PublishPartitionCheckpointPointerOutcome, MetaStoreError> {
        request.validate()?;
        // A changed durable token is never eligible for a replayed duplicate.
        // The transaction below remains the authoritative live-expiry check.
        if self
            .read_partition_authority_record(&request.authority.key)
            .await?
            .as_ref()
            != Some(&request.authority)
        {
            return Err(MetaStoreError::PartitionAuthorityInvalidToken);
        }
        let (request_id, request_digest) = partition_checkpoint_request_identity(&request);
        let key = &request.candidate.key;
        let partition_id = i64::from(key.partition_id);
        let authority_epoch = i64_from_u64("authority.owner_epoch", request.authority.owner_epoch)?;
        let authority_expiry = i64_from_u64(
            "authority.expires_at_unix_ms",
            request.authority.expires_at_unix_ms,
        )?;
        let mut statements = vec![
            (
                "INSERT OR IGNORE INTO velorix_partition_checkpoint_requests (request_id, request_digest, outcome, namespace, view_id, stream_id, partition_id) VALUES ($1, $2, 'pending', $3, $4, $5, $6)",
                vec![hiqlite::Param::from(request_id.clone()), hiqlite::Param::from(request_digest.clone()), hiqlite::Param::from(key.namespace.clone()), hiqlite::Param::from(key.view_id.clone()), hiqlite::Param::from(key.stream_id.clone()), hiqlite::Param::from(partition_id)],
            ),
            (
                "SELECT 1 AS authorized FROM velorix_partition_authorities WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4 AND owner_id = $5 AND owner_epoch = $6 AND expires_at_unix_ms = $7 AND expires_at_unix_ms > $8",
                vec![hiqlite::Param::from(key.namespace.clone()), hiqlite::Param::from(key.view_id.clone()), hiqlite::Param::from(key.stream_id.clone()), hiqlite::Param::from(partition_id), hiqlite::Param::from(request.authority.owner_id.clone()), hiqlite::Param::from(authority_epoch), hiqlite::Param::from(authority_expiry), hiqlite::Param::raft_serialized_unix_ms()],
            ),
        ];
        if let Some(expected) = &request.expected_previous {
            statements.push((
                "UPDATE velorix_partition_checkpoint_pointers SET checkpoint_key = $1, last_request_id = $2 WHERE namespace = $3 AND view_id = $4 AND stream_id = $5 AND partition_id = $6 AND checkpoint_key = $7 AND checkpoint_key <> $1 AND $8 = 1 AND EXISTS (SELECT 1 FROM velorix_partition_checkpoint_requests WHERE request_id = $2 AND request_digest = $9 AND outcome = 'pending')",
                vec![hiqlite::Param::from(request.candidate.checkpoint_key.clone()), hiqlite::Param::from(request_id.clone()), hiqlite::Param::from(key.namespace.clone()), hiqlite::Param::from(key.view_id.clone()), hiqlite::Param::from(key.stream_id.clone()), hiqlite::Param::from(partition_id), hiqlite::Param::from(expected.checkpoint_key.clone()), hiqlite::Param::StmtOutputIndexed(1, 0), hiqlite::Param::from(request_digest.clone())],
            ));
        } else {
            statements.push((
                "INSERT OR IGNORE INTO velorix_partition_checkpoint_pointers (namespace, view_id, stream_id, partition_id, checkpoint_key, last_request_id) SELECT $1, $2, $3, $4, $5, $6 WHERE $7 = 1 AND EXISTS (SELECT 1 FROM velorix_partition_checkpoint_requests WHERE request_id = $6 AND request_digest = $8 AND outcome = 'pending')",
                vec![hiqlite::Param::from(key.namespace.clone()), hiqlite::Param::from(key.view_id.clone()), hiqlite::Param::from(key.stream_id.clone()), hiqlite::Param::from(partition_id), hiqlite::Param::from(request.candidate.checkpoint_key.clone()), hiqlite::Param::from(request_id.clone()), hiqlite::Param::StmtOutputIndexed(1, 0), hiqlite::Param::from(request_digest.clone())],
            ));
        }
        statements.push((
            "UPDATE velorix_partition_checkpoint_requests SET outcome = CASE WHEN NOT EXISTS (SELECT 1 FROM velorix_partition_authorities WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4 AND owner_id = $5 AND owner_epoch = $6 AND expires_at_unix_ms = $7 AND expires_at_unix_ms > $8) THEN 'invalid_authority' WHEN EXISTS (SELECT 1 FROM velorix_partition_checkpoint_pointers WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4 AND last_request_id = $9) THEN 'published' WHEN EXISTS (SELECT 1 FROM velorix_partition_checkpoint_pointers WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4 AND checkpoint_key = $10) THEN 'duplicate' ELSE 'conflict' END, checkpoint_key = COALESCE((SELECT checkpoint_key FROM velorix_partition_checkpoint_pointers WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4), '') WHERE request_id = $9 AND request_digest = $11 AND outcome = 'pending'",
            vec![hiqlite::Param::from(key.namespace.clone()), hiqlite::Param::from(key.view_id.clone()), hiqlite::Param::from(key.stream_id.clone()), hiqlite::Param::from(partition_id), hiqlite::Param::from(request.authority.owner_id.clone()), hiqlite::Param::from(authority_epoch), hiqlite::Param::from(authority_expiry), hiqlite::Param::raft_serialized_unix_ms(), hiqlite::Param::from(request_id.clone()), hiqlite::Param::from(request.candidate.checkpoint_key.clone()), hiqlite::Param::from(request_digest.clone())],
        ));
        self.with_schema_repair(|| async {
            self.client
                .txn_with_raft_serialized_timestamp(statements.clone())
                .await
                .map(|_| ())
                .map_err(hiqlite_error)
        })
        .await?;
        let rows = self.client.query_consistent_map::<PartitionCheckpointRequestRow, _>("SELECT outcome FROM velorix_partition_checkpoint_requests WHERE request_id = $1 AND request_digest = $2", vec![hiqlite::Param::from(request_id), hiqlite::Param::from(request_digest)]).await.map_err(hiqlite_error)?;
        match rows.first().map(|row| row.outcome.as_str()) {
            Some("published") => Ok(PublishPartitionCheckpointPointerOutcome::Published),
            Some("duplicate") => Ok(PublishPartitionCheckpointPointerOutcome::Duplicate),
            Some("conflict") => Ok(PublishPartitionCheckpointPointerOutcome::Conflict),
            Some("invalid_authority") => Err(MetaStoreError::PartitionAuthorityInvalidToken),
            Some(other) => Err(MetaStoreError::Serialization(format!(
                "invalid partition checkpoint request outcome: {other}"
            ))),
            None => Err(MetaStoreError::Serialization(
                "partition checkpoint request status disappeared".into(),
            )),
        }
    }

    async fn read_partition_checkpoint_pointer(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Option<PartitionCheckpointPointer>, MetaStoreError> {
        key.validate()?;
        let rows = self
            .with_schema_repair(|| async {
                self.client
                    .query_consistent_map::<PartitionCheckpointPointerRow, _>(
                        "SELECT namespace, view_id, stream_id, partition_id, checkpoint_key FROM velorix_partition_checkpoint_pointers WHERE namespace = $1 AND view_id = $2 AND stream_id = $3 AND partition_id = $4",
                        vec![hiqlite::Param::from(key.namespace.clone()), hiqlite::Param::from(key.view_id.clone()), hiqlite::Param::from(key.stream_id.clone()), hiqlite::Param::from(i64::from(key.partition_id))],
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        rows.into_iter()
            .next()
            .map(PartitionCheckpointPointerRow::into_pointer)
            .transpose()
    }

    async fn publish_standing_runtime_checkpoint(
        &self,
        request: PublishStandingRuntimeCheckpointRequest,
    ) -> Result<PublishStandingRuntimeCheckpointOutcome, MetaStoreError> {
        request.validate()?;
        let candidate_epoch = i64_from_u64("logical_epoch", request.candidate.logical_epoch)?;
        let candidate_output_manifest_refs_json =
            standing_runtime_output_manifest_refs_json(&request.candidate.output_manifest_refs)?;
        let candidate_bootstrap_generation = i64_from_u64(
            "bootstrap_generation",
            request.candidate.bootstrap_generation,
        )?;
        let candidate_input_coverage_json = request
            .candidate
            .input_coverage
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?
            .unwrap_or_default();
        let owner_epoch = i64_from_u64("owner_epoch", request.owner.owner_epoch)?;
        let (changed, raft_timestamp) = if let Some(expected) = &request.expected_previous {
            let expected_epoch =
                i64_from_u64("expected_previous.logical_epoch", expected.logical_epoch)?;
            let txn = self
                .with_schema_repair(|| async {
                    self.client
                        .txn_with_raft_serialized_timestamp([
                            (
                                "SELECT 1 AS authorized
                            FROM velorix_standing_runtime_owners owner
                            WHERE owner.tenant_id = $1
                              AND owner.program_id = $2
                              AND owner.view_id = $3
                              AND owner.owner_id = $4
                              AND owner.owner_epoch = $5
                              AND owner.expires_at_unix_ms > $6
                              AND $7 = $1
                              AND $8 = $2
                              AND $9 = $3",
                                vec![
                                    hiqlite::Param::from(request.owner.tenant_id.clone()),
                                    hiqlite::Param::from(request.owner.program_id.clone()),
                                    hiqlite::Param::from(request.owner.view_id.clone()),
                                    hiqlite::Param::from(request.owner.owner_id.clone()),
                                    hiqlite::Param::from(owner_epoch),
                                    hiqlite::Param::raft_serialized_unix_ms(),
                                    hiqlite::Param::from(request.candidate.tenant_id.clone()),
                                    hiqlite::Param::from(request.candidate.program_id.clone()),
                                    hiqlite::Param::from(request.candidate.view_id.clone()),
                                ],
                            ),
                            (
                                "UPDATE velorix_standing_runtime_checkpoints
                            SET checkpoint_key = $1,
                                logical_epoch = $2,
                                content_hash = $3,
                                manifest_hash = $4,
                                output_manifest_refs_json = $5,
                                bootstrap_generation = $6,
                                plan_hash = $7,
                                coverage_hash = $8,
                                input_coverage_json = $9,
                                previous_checkpoint_key = $10,
                                previous_manifest_hash = $11
                            WHERE tenant_id = $12
                              AND program_id = $13
                              AND view_id = $14
                              AND checkpoint_key = $15
                              AND logical_epoch = $16
                              AND content_hash = $17
                              AND manifest_hash = $18
                              AND $19 = 1",
                                vec![
                                    hiqlite::Param::from(request.candidate.checkpoint_key.clone()),
                                    hiqlite::Param::from(candidate_epoch),
                                    hiqlite::Param::from(request.candidate.content_hash.clone()),
                                    hiqlite::Param::from(request.candidate.manifest_hash.clone()),
                                    hiqlite::Param::from(
                                        candidate_output_manifest_refs_json.clone(),
                                    ),
                                    hiqlite::Param::from(candidate_bootstrap_generation),
                                    hiqlite::Param::from(request.candidate.plan_hash.clone()),
                                    hiqlite::Param::from(request.candidate.coverage_hash.clone()),
                                    hiqlite::Param::from(candidate_input_coverage_json.clone()),
                                    hiqlite::Param::from(
                                        request.candidate.previous_checkpoint_key.clone(),
                                    ),
                                    hiqlite::Param::from(
                                        request.candidate.previous_manifest_hash.clone(),
                                    ),
                                    hiqlite::Param::from(request.candidate.tenant_id.clone()),
                                    hiqlite::Param::from(request.candidate.program_id.clone()),
                                    hiqlite::Param::from(request.candidate.view_id.clone()),
                                    hiqlite::Param::from(expected.checkpoint_key.clone()),
                                    hiqlite::Param::from(expected_epoch),
                                    hiqlite::Param::from(expected.content_hash.clone()),
                                    hiqlite::Param::from(expected.manifest_hash.clone()),
                                    hiqlite::Param::StmtOutputIndexed(0, 0),
                                ],
                            ),
                        ])
                        .await
                        .map_err(hiqlite_error)
                })
                .await?;
            (
                hiqlite_txn_changed_rows_or_zero_for_missing_stmt_output(txn.result, 1)?,
                txn.timestamp,
            )
        } else {
            let txn = self
                .with_schema_repair(|| async {
                    self.client
                        .txn_with_raft_serialized_timestamp([
                            (
                                "SELECT 1 AS authorized
                            FROM velorix_standing_runtime_owners owner
                            WHERE owner.tenant_id = $1
                              AND owner.program_id = $2
                              AND owner.view_id = $3
                              AND owner.owner_id = $4
                              AND owner.owner_epoch = $5
                              AND owner.expires_at_unix_ms > $6
                              AND $7 = $1
                              AND $8 = $2
                              AND $9 = $3",
                                vec![
                                    hiqlite::Param::from(request.owner.tenant_id.clone()),
                                    hiqlite::Param::from(request.owner.program_id.clone()),
                                    hiqlite::Param::from(request.owner.view_id.clone()),
                                    hiqlite::Param::from(request.owner.owner_id.clone()),
                                    hiqlite::Param::from(owner_epoch),
                                    hiqlite::Param::raft_serialized_unix_ms(),
                                    hiqlite::Param::from(request.candidate.tenant_id.clone()),
                                    hiqlite::Param::from(request.candidate.program_id.clone()),
                                    hiqlite::Param::from(request.candidate.view_id.clone()),
                                ],
                            ),
                            (
                                "INSERT OR IGNORE INTO velorix_standing_runtime_checkpoints (
                            tenant_id,
                            program_id,
                            view_id,
                            checkpoint_key,
                            logical_epoch,
                            content_hash,
                            manifest_hash,
                            output_manifest_refs_json,
                            bootstrap_generation,
                            plan_hash,
                            coverage_hash,
                            input_coverage_json,
                            previous_checkpoint_key,
                            previous_manifest_hash
                        )
                        SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
                        WHERE $15 = 1",
                                vec![
                                    hiqlite::Param::from(request.candidate.tenant_id.clone()),
                                    hiqlite::Param::from(request.candidate.program_id.clone()),
                                    hiqlite::Param::from(request.candidate.view_id.clone()),
                                    hiqlite::Param::from(request.candidate.checkpoint_key.clone()),
                                    hiqlite::Param::from(candidate_epoch),
                                    hiqlite::Param::from(request.candidate.content_hash.clone()),
                                    hiqlite::Param::from(request.candidate.manifest_hash.clone()),
                                    hiqlite::Param::from(
                                        candidate_output_manifest_refs_json.clone(),
                                    ),
                                    hiqlite::Param::from(candidate_bootstrap_generation),
                                    hiqlite::Param::from(request.candidate.plan_hash.clone()),
                                    hiqlite::Param::from(request.candidate.coverage_hash.clone()),
                                    hiqlite::Param::from(candidate_input_coverage_json.clone()),
                                    hiqlite::Param::from(
                                        request.candidate.previous_checkpoint_key.clone(),
                                    ),
                                    hiqlite::Param::from(
                                        request.candidate.previous_manifest_hash.clone(),
                                    ),
                                    hiqlite::Param::StmtOutputIndexed(0, 0),
                                ],
                            ),
                        ])
                        .await
                        .map_err(hiqlite_error)
                })
                .await?;
            (
                hiqlite_txn_changed_rows_or_zero_for_missing_stmt_output(txn.result, 1)?,
                txn.timestamp,
            )
        };
        if changed == 1 {
            return Ok(PublishStandingRuntimeCheckpointOutcome::Published);
        }

        validate_current_standing_runtime_owner(
            self.read_standing_runtime_owner_record(
                &request.owner.tenant_id,
                &request.owner.program_id,
                &request.owner.view_id,
            )
            .await?
            .as_ref(),
            &request.owner,
            u64::try_from(raft_timestamp.unix_ms).map_err(|_| MetaStoreError::TimestampOverflow)?,
        )?;

        match self
            .read_standing_runtime_checkpoint(
                &request.candidate.tenant_id,
                &request.candidate.program_id,
                &request.candidate.view_id,
            )
            .await?
        {
            Some(current) if current == request.candidate => {
                Ok(PublishStandingRuntimeCheckpointOutcome::Duplicate)
            }
            _ => Ok(PublishStandingRuntimeCheckpointOutcome::Conflict),
        }
    }

    async fn read_standing_runtime_checkpoint(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeCheckpointPointer>, MetaStoreError> {
        validate_standing_runtime_scope(tenant_id, program_id, view_id)?;
        let rows = self
            .with_schema_repair(|| async {
                self.client
                    .query_consistent_map::<StandingRuntimeCheckpointPointerRow, _>(
                        "SELECT
                            tenant_id,
                            program_id,
                            view_id,
                            checkpoint_key,
                            logical_epoch,
                            content_hash,
                            manifest_hash,
                            output_manifest_refs_json,
                            bootstrap_generation,
                            plan_hash,
                            coverage_hash,
                            input_coverage_json,
                            previous_checkpoint_key,
                            previous_manifest_hash
                        FROM velorix_standing_runtime_checkpoints
                        WHERE tenant_id = $1
                          AND program_id = $2
                          AND view_id = $3",
                        vec![
                            hiqlite::Param::from(tenant_id.to_string()),
                            hiqlite::Param::from(program_id.to_string()),
                            hiqlite::Param::from(view_id.to_string()),
                        ],
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        rows.into_iter()
            .next()
            .map(StandingRuntimeCheckpointPointerRow::into_pointer)
            .transpose()
    }

    async fn read_view_dependency_graph_revision(
        &self,
        tenant_id: &str,
    ) -> Result<u64, MetaStoreError> {
        require_non_empty("tenant_id", tenant_id)?;
        let rows = self
            .with_schema_repair(|| async {
                self.client
                    .query_consistent_map::<GraphHeadRow, _>(
                        "SELECT revision FROM velorix_view_dependency_graph_heads
                         WHERE tenant_id = $1",
                        vec![hiqlite::Param::from(tenant_id.to_string())],
                    )
                    .await
                    .map_err(hiqlite_error)
            })
            .await?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(0);
        };
        let revision = row.revision;
        u64::try_from(revision)
            .map_err(|_| MetaStoreError::Serialization("graph revision is negative".to_string()))
    }
}

#[cfg(feature = "hiqlite-backend")]
struct GraphHeadRow {
    revision: i64,
}

#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for GraphHeadRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            revision: row.get("revision"),
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
struct CatalogJsonRow {
    catalog_json: Vec<u8>,
}

#[cfg(feature = "hiqlite-backend")]
struct TableColumnRow {
    name: String,
}

#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for TableColumnRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            name: row.get("name"),
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for CatalogJsonRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            catalog_json: row.get("catalog_json"),
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
struct IngestReservationRow {
    stream_id: String,
    partition_id: i64,
    start_offset_inclusive: i64,
    end_offset_exclusive: i64,
    batch_key: String,
    payload_digest: String,
    relation_id: String,
    relation_version: String,
    schema_fingerprint: String,
    writer_epoch: i64,
}

#[cfg(feature = "hiqlite-backend")]
struct SourceCutReservationRow {
    admission_epoch: i64,
    reservation: IngestReservationRow,
    committed: bool,
}

#[cfg(feature = "hiqlite-backend")]
struct ViewBootstrapControlRow {
    tenant_id: String,
    program_id: String,
    view_id: String,
    schema_version: i64,
    bootstrap_generation: i64,
    plan_hash: String,
    view_spec_json: Vec<u8>,
    lifecycle: String,
    input_catalog_epoch: i64,
    activation_cut_json: String,
    active_checkpoint_json: String,
}

#[cfg(feature = "hiqlite-backend")]
struct ViewBootstrapInputRow {
    relation_id: String,
    relation_version: String,
    schema_fingerprint: String,
}

#[cfg(feature = "hiqlite-backend")]
struct StandingRuntimeCheckpointPointerRow {
    tenant_id: String,
    program_id: String,
    view_id: String,
    checkpoint_key: String,
    logical_epoch: i64,
    content_hash: String,
    manifest_hash: String,
    output_manifest_refs_json: String,
    bootstrap_generation: i64,
    plan_hash: String,
    coverage_hash: String,
    input_coverage_json: String,
    previous_checkpoint_key: String,
    previous_manifest_hash: String,
}

#[cfg(feature = "hiqlite-backend")]
struct StandingRuntimeOwnerClaimRow {
    tenant_id: String,
    program_id: String,
    view_id: String,
    owner_id: String,
    owner_epoch: i64,
    expires_at_unix_ms: i64,
}

#[cfg(feature = "hiqlite-backend")]
struct PartitionAuthorityRow {
    namespace: String,
    view_id: String,
    stream_id: String,
    partition_id: i64,
    owner_id: String,
    owner_epoch: i64,
    expires_at_unix_ms: i64,
}

#[cfg(feature = "hiqlite-backend")]
impl PartitionAuthorityRow {
    fn into_token(self) -> Result<PartitionAuthorityToken, MetaStoreError> {
        Ok(PartitionAuthorityToken {
            key: PartitionAuthorityKey {
                namespace: self.namespace,
                view_id: self.view_id,
                stream_id: self.stream_id,
                partition_id: u32::try_from(self.partition_id).map_err(|_| {
                    MetaStoreError::Serialization(
                        "partition authority partition_id is invalid".into(),
                    )
                })?,
            },
            owner_id: self.owner_id,
            owner_epoch: u64::try_from(self.owner_epoch).map_err(|_| {
                MetaStoreError::Serialization("partition authority owner_epoch is invalid".into())
            })?,
            expires_at_unix_ms: u64::try_from(self.expires_at_unix_ms).map_err(|_| {
                MetaStoreError::Serialization("partition authority expiry is invalid".into())
            })?,
        })
    }
}

#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for PartitionAuthorityRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            namespace: row.get("namespace"),
            view_id: row.get("view_id"),
            stream_id: row.get("stream_id"),
            partition_id: row.get("partition_id"),
            owner_id: row.get("owner_id"),
            owner_epoch: row.get("owner_epoch"),
            expires_at_unix_ms: row.get("expires_at_unix_ms"),
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
struct PartitionAuthorityRequestRow {
    request_digest: String,
    outcome: String,
    namespace: String,
    view_id: String,
    stream_id: String,
    partition_id: i64,
    owner_id: String,
    owner_epoch: i64,
    expires_at_unix_ms: i64,
}

#[cfg(feature = "hiqlite-backend")]
struct PartitionCheckpointRequestRow {
    outcome: String,
}
#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for PartitionCheckpointRequestRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            outcome: row.get("outcome"),
        }
    }
}
#[cfg(feature = "hiqlite-backend")]
struct PartitionCheckpointPointerRow {
    namespace: String,
    view_id: String,
    stream_id: String,
    partition_id: i64,
    checkpoint_key: String,
}
#[cfg(feature = "hiqlite-backend")]
impl PartitionCheckpointPointerRow {
    fn into_pointer(self) -> Result<PartitionCheckpointPointer, MetaStoreError> {
        Ok(PartitionCheckpointPointer {
            key: PartitionAuthorityKey {
                namespace: self.namespace,
                view_id: self.view_id,
                stream_id: self.stream_id,
                partition_id: u32::try_from(self.partition_id).map_err(|_| {
                    MetaStoreError::Serialization(
                        "partition checkpoint partition_id is invalid".into(),
                    )
                })?,
            },
            checkpoint_key: self.checkpoint_key,
        })
    }
}
#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for PartitionCheckpointPointerRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            namespace: row.get("namespace"),
            view_id: row.get("view_id"),
            stream_id: row.get("stream_id"),
            partition_id: row.get("partition_id"),
            checkpoint_key: row.get("checkpoint_key"),
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
impl PartitionAuthorityRequestRow {
    fn into_outcome(self) -> Result<AcquirePartitionAuthorityOutcome, MetaStoreError> {
        let _ = self.request_digest;
        let token = PartitionAuthorityRow {
            namespace: self.namespace,
            view_id: self.view_id,
            stream_id: self.stream_id,
            partition_id: self.partition_id,
            owner_id: self.owner_id,
            owner_epoch: self.owner_epoch,
            expires_at_unix_ms: self.expires_at_unix_ms,
        }
        .into_token()?;
        match self.outcome.as_str() {
            "acquired" => Ok(AcquirePartitionAuthorityOutcome::Acquired(token)),
            "renewed" => Ok(AcquirePartitionAuthorityOutcome::Renewed(token)),
            "conflict" => Ok(AcquirePartitionAuthorityOutcome::Conflict(token)),
            "epoch_overflow" => Err(MetaStoreError::AuthorityEpochOverflow),
            other => Err(MetaStoreError::Serialization(format!(
                "invalid partition authority request outcome: {other}"
            ))),
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for PartitionAuthorityRequestRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            request_digest: row.get("request_digest"),
            outcome: row.get("outcome"),
            namespace: row.get("namespace"),
            view_id: row.get("view_id"),
            stream_id: row.get("stream_id"),
            partition_id: row.get("partition_id"),
            owner_id: row.get("owner_id"),
            owner_epoch: row.get("owner_epoch"),
            expires_at_unix_ms: row.get("expires_at_unix_ms"),
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
impl StandingRuntimeOwnerClaimRow {
    fn into_claim(self) -> Result<StandingRuntimeOwnerClaim, MetaStoreError> {
        let owner_epoch = u64::try_from(self.owner_epoch).map_err(|_| {
            MetaStoreError::Serialization(format!(
                "standing runtime owner_epoch is negative: {}",
                self.owner_epoch
            ))
        })?;
        let expires_at_unix_ms = u64::try_from(self.expires_at_unix_ms).map_err(|_| {
            MetaStoreError::Serialization(format!(
                "standing runtime owner expires_at_unix_ms is negative: {}",
                self.expires_at_unix_ms
            ))
        })?;
        Ok(StandingRuntimeOwnerClaim {
            tenant_id: self.tenant_id,
            program_id: self.program_id,
            view_id: self.view_id,
            owner_id: self.owner_id,
            owner_epoch,
            expires_at_unix_ms,
        })
    }
}

#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for StandingRuntimeOwnerClaimRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            tenant_id: row.get("tenant_id"),
            program_id: row.get("program_id"),
            view_id: row.get("view_id"),
            owner_id: row.get("owner_id"),
            owner_epoch: row.get("owner_epoch"),
            expires_at_unix_ms: row.get("expires_at_unix_ms"),
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
impl StandingRuntimeCheckpointPointerRow {
    fn into_pointer(self) -> Result<StandingRuntimeCheckpointPointer, MetaStoreError> {
        let logical_epoch = u64::try_from(self.logical_epoch).map_err(|_| {
            MetaStoreError::Serialization(format!(
                "standing runtime checkpoint logical_epoch is negative: {}",
                self.logical_epoch
            ))
        })?;
        let pointer = StandingRuntimeCheckpointPointer {
            tenant_id: self.tenant_id,
            program_id: self.program_id,
            view_id: self.view_id,
            checkpoint_key: self.checkpoint_key,
            logical_epoch,
            content_hash: self.content_hash,
            manifest_hash: self.manifest_hash,
            output_manifest_refs: standing_runtime_output_manifest_refs_from_json(
                &self.output_manifest_refs_json,
            )?,
            bootstrap_generation: u64::try_from(self.bootstrap_generation).map_err(|_| {
                MetaStoreError::Serialization(format!(
                    "standing runtime checkpoint bootstrap_generation is negative: {}",
                    self.bootstrap_generation
                ))
            })?,
            plan_hash: self.plan_hash,
            coverage_hash: self.coverage_hash,
            input_coverage: if self.input_coverage_json.is_empty() {
                None
            } else {
                Some(
                    serde_json::from_str(&self.input_coverage_json)
                        .map_err(|error| MetaStoreError::Serialization(error.to_string()))?,
                )
            },
            previous_checkpoint_key: self.previous_checkpoint_key,
            previous_manifest_hash: self.previous_manifest_hash,
        };
        pointer.validate()?;
        Ok(pointer)
    }
}

#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for StandingRuntimeCheckpointPointerRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            tenant_id: row.get("tenant_id"),
            program_id: row.get("program_id"),
            view_id: row.get("view_id"),
            checkpoint_key: row.get("checkpoint_key"),
            logical_epoch: row.get("logical_epoch"),
            content_hash: row.get("content_hash"),
            manifest_hash: row.get("manifest_hash"),
            output_manifest_refs_json: row.get("output_manifest_refs_json"),
            bootstrap_generation: row.get("bootstrap_generation"),
            plan_hash: row.get("plan_hash"),
            coverage_hash: row.get("coverage_hash"),
            input_coverage_json: row.get("input_coverage_json"),
            previous_checkpoint_key: row.get("previous_checkpoint_key"),
            previous_manifest_hash: row.get("previous_manifest_hash"),
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
fn standing_runtime_output_manifest_refs_json(refs: &[String]) -> Result<String, MetaStoreError> {
    serde_json::to_string(refs).map_err(|source| MetaStoreError::Serialization(source.to_string()))
}

#[cfg(feature = "hiqlite-backend")]
fn partition_authority_request_identity(
    request: &AcquirePartitionAuthorityRequest,
) -> (String, String) {
    // Length-prefixing gives a canonical unambiguous encoding without exposing
    // a request-id field in the public contract.
    let mut canonical = String::new();
    for value in [
        &request.key.namespace,
        &request.key.view_id,
        &request.key.stream_id,
        &request.owner_id,
    ] {
        canonical.push_str(&value.len().to_string());
        canonical.push(':');
        canonical.push_str(value);
        canonical.push('|');
    }
    canonical.push_str(&request.key.partition_id.to_string());
    canonical.push('|');
    canonical.push_str(&request.ttl_ms.to_string());
    canonical.push('|');
    match &request.current_token {
        Some(token) => {
            canonical.push_str("some|");
            canonical.push_str(&token.owner_epoch.to_string());
            canonical.push('|');
            canonical.push_str(&token.expires_at_unix_ms.to_string());
        }
        None => canonical.push_str("none"),
    }
    let digest = Sha256::digest(canonical.as_bytes());
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let request_id = format!("sha256:{hex}");
    (request_id.clone(), request_id)
}

#[cfg(feature = "hiqlite-backend")]
fn partition_checkpoint_request_identity(
    request: &PublishPartitionCheckpointPointerRequest,
) -> (String, String) {
    let mut canonical = String::from("publish_partition_checkpoint|");
    for value in [
        &request.candidate.key.namespace,
        &request.candidate.key.view_id,
        &request.candidate.key.stream_id,
        &request.authority.owner_id,
        &request.candidate.checkpoint_key,
    ] {
        canonical.push_str(&format!("{}:{value}|", value.len()));
    }
    canonical.push_str(&format!(
        "{}|{}|{}|",
        request.candidate.key.partition_id,
        request.authority.owner_epoch,
        request.authority.expires_at_unix_ms
    ));
    match &request.expected_previous {
        Some(pointer) => canonical.push_str(&format!(
            "some:{}:{}",
            pointer.key.partition_id, pointer.checkpoint_key
        )),
        None => canonical.push_str("none"),
    }
    let hex = Sha256::digest(canonical.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let id = format!("sha256:{hex}");
    (id.clone(), id)
}

#[cfg(feature = "hiqlite-backend")]
fn standing_runtime_output_manifest_refs_from_json(
    value: &str,
) -> Result<Vec<String>, MetaStoreError> {
    serde_json::from_str(value).map_err(|source| MetaStoreError::Serialization(source.to_string()))
}

#[cfg(feature = "hiqlite-backend")]
impl IngestReservationRow {
    fn into_reservation(self) -> IngestRangeReservation {
        IngestRangeReservation {
            stream_id: self.stream_id,
            partition_id: self.partition_id as u32,
            start_offset_inclusive: self.start_offset_inclusive as u64,
            end_offset_exclusive: self.end_offset_exclusive as u64,
            batch_key: self.batch_key,
            payload_digest: self.payload_digest,
            relation_id: self.relation_id,
            relation_version: self.relation_version,
            schema_fingerprint: self.schema_fingerprint,
            writer_epoch: self.writer_epoch as u64,
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for IngestReservationRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            stream_id: row.get("stream_id"),
            partition_id: row.get("partition_id"),
            start_offset_inclusive: row.get("start_offset_inclusive"),
            end_offset_exclusive: row.get("end_offset_exclusive"),
            batch_key: row.get("batch_key"),
            payload_digest: row.get("payload_digest"),
            relation_id: row.get("relation_id"),
            relation_version: row.get("relation_version"),
            schema_fingerprint: row.get("schema_fingerprint"),
            writer_epoch: row.get("writer_epoch"),
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for SourceCutReservationRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        let committed: i64 = row.get("committed");
        Self {
            admission_epoch: row.get("admission_epoch"),
            reservation: IngestReservationRow::from(&mut *row),
            committed: committed != 0,
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for ViewBootstrapControlRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            tenant_id: row.get("tenant_id"),
            program_id: row.get("program_id"),
            view_id: row.get("view_id"),
            schema_version: row.get("schema_version"),
            bootstrap_generation: row.get("bootstrap_generation"),
            plan_hash: row.get("plan_hash"),
            view_spec_json: row.get("view_spec_json"),
            lifecycle: row.get("lifecycle"),
            input_catalog_epoch: row.get("input_catalog_epoch"),
            activation_cut_json: row.get("activation_cut_json"),
            active_checkpoint_json: row.get("active_checkpoint_json"),
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for ViewBootstrapInputRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            relation_id: row.get("relation_id"),
            relation_version: row.get("relation_version"),
            schema_fingerprint: row.get("schema_fingerprint"),
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
struct ViewBootstrapViewInputRow {
    edge_json: Vec<u8>,
}

#[cfg(feature = "hiqlite-backend")]
impl From<&mut hiqlite::Row<'_>> for ViewBootstrapViewInputRow {
    fn from(row: &mut hiqlite::Row<'_>) -> Self {
        Self {
            edge_json: row.get("edge_json"),
        }
    }
}

#[cfg(feature = "hiqlite-backend")]
fn hiqlite_error(error: hiqlite::Error) -> MetaStoreError {
    MetaStoreError::Hiqlite(error.to_string())
}

#[cfg(feature = "hiqlite-backend")]
fn hiqlite_meta_error_is_missing_table(error: &MetaStoreError) -> bool {
    matches!(
        error,
        MetaStoreError::Hiqlite(message)
            if message.contains("no such table: velorix_")
                || message.contains("no such table: main.velorix_")
    )
}

#[cfg(feature = "hiqlite-backend")]
fn i64_from_u64(field: &'static str, value: u64) -> Result<i64, MetaStoreError> {
    i64::try_from(value).map_err(|_| MetaStoreError::IntegerOutOfRange { field, value })
}

#[cfg(feature = "hiqlite-backend")]
fn hiqlite_txn_changed_rows(
    results: &[Result<usize, hiqlite::Error>],
    changed_result_index: usize,
) -> Result<usize, MetaStoreError> {
    let mut changed = None;
    for (index, result) in results.iter().enumerate() {
        let rows = result
            .as_ref()
            .map_err(|e| MetaStoreError::Hiqlite(e.to_string()))?;
        if index == changed_result_index {
            changed = Some(*rows);
        }
    }
    changed.ok_or_else(|| {
        MetaStoreError::UnexpectedOutcome(format!(
            "hiqlite transaction did not return result index {changed_result_index}"
        ))
    })
}

#[cfg(feature = "hiqlite-backend")]
fn hiqlite_txn_changed_rows_or_zero_for_missing_stmt_output(
    result: Result<Vec<Result<usize, hiqlite::Error>>, hiqlite::Error>,
    changed_result_index: usize,
) -> Result<usize, MetaStoreError> {
    match result {
        Ok(results) => hiqlite_txn_changed_rows(&results, changed_result_index),
        Err(error) if hiqlite_error_is_missing_stmt_output(&error) => Ok(0),
        Err(error) => Err(hiqlite_error(error)),
    }
}

#[cfg(feature = "hiqlite-backend")]
fn hiqlite_error_is_missing_stmt_output(error: &hiqlite::Error) -> bool {
    matches!(
        error,
        hiqlite::Error::QueryParams(message)
            if message.contains("does not have observable row output")
    )
}

impl GrpcMetaStore {
    pub async fn connect(endpoint: impl AsRef<str>) -> Result<Self, MetaStoreError> {
        let client =
            proto::velorix_meta_client::VelorixMetaClient::connect(endpoint.as_ref().to_string())
                .await
                .map_err(|error| MetaStoreError::Remote(error.to_string()))?;

        Ok(Self {
            client,
            bearer_token: None,
        })
    }

    pub async fn connect_with_bearer_token(
        endpoint: impl AsRef<str>,
        bearer_token: impl Into<String>,
    ) -> Result<Self, MetaStoreError> {
        let mut store = Self::connect(endpoint).await?;
        store.set_bearer_token(bearer_token)?;
        Ok(store)
    }

    pub fn set_bearer_token(
        &mut self,
        bearer_token: impl Into<String>,
    ) -> Result<(), MetaStoreError> {
        let bearer_token = bearer_token.into();
        validate_bearer_token(&bearer_token)?;
        let value = format!("Bearer {bearer_token}").parse().map_err(|error| {
            MetaStoreError::Serialization(format!("invalid bearer token: {error}"))
        })?;
        self.bearer_token = Some(value);
        Ok(())
    }

    fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        if let Some(token) = &self.bearer_token {
            request
                .metadata_mut()
                .insert("authorization", token.clone());
        }
        request
    }

    fn client(&self) -> proto::velorix_meta_client::VelorixMetaClient<Channel> {
        self.client.clone()
    }
}

#[async_trait]
impl MetaStore for GrpcMetaStore {
    async fn read_meta_store_capabilities(&self) -> Result<MetaStoreCapabilities, MetaStoreError> {
        let response = self
            .client()
            .read_meta_store_capabilities(self.request(proto::ReadMetaStoreCapabilitiesRequest {}))
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        let standing_runtime_fencing = response
            .standing_runtime_fencing
            .ok_or_else(|| {
                MetaStoreError::UnexpectedOutcome(
                    "missing standing runtime fencing capability".to_string(),
                )
            })
            .map(standing_runtime_fencing_capability_from_proto)?;

        Ok(MetaStoreCapabilities {
            standing_runtime_fencing,
            partition_authority: response
                .partition_authority
                .map(partition_authority_capability_from_proto)
                .unwrap_or_else(|| PartitionAuthorityCapability::unsupported("grpc")),
        })
    }

    async fn store_relation_catalog(
        &self,
        catalog: VelorixRelationCatalogV1,
    ) -> Result<StoreRelationCatalogOutcome, MetaStoreError> {
        let catalog_json = serde_json::to_vec(&catalog)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let response = self
            .client()
            .store_relation_catalog(
                self.request(proto::StoreRelationCatalogRequest { catalog_json }),
            )
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();

        match response.outcome.as_str() {
            "created" => Ok(StoreRelationCatalogOutcome::Created),
            "duplicate" => Ok(StoreRelationCatalogOutcome::Duplicate),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn read_relation_catalog(
        &self,
        relation_id: &str,
        relation_version: &str,
    ) -> Result<VelorixRelationCatalogV1, MetaStoreError> {
        let response = self
            .client()
            .read_relation_catalog(self.request(proto::ReadRelationCatalogRequest {
                relation_id: relation_id.to_string(),
                relation_version: relation_version.to_string(),
            }))
            .await
            .map_err(|error| match error.code() {
                tonic::Code::NotFound => MetaStoreError::RelationCatalogNotFound {
                    relation_id: relation_id.to_string(),
                    relation_version: relation_version.to_string(),
                },
                _ => MetaStoreError::Remote(error.to_string()),
            })?
            .into_inner();

        serde_json::from_slice(&response.catalog_json)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))
    }

    async fn reserve_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<ReserveIngestRangeOutcome, MetaStoreError> {
        let response = self
            .client()
            .reserve_ingest_range(self.request(ingest_range_reservation_to_proto(reservation)))
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();

        match response.outcome.as_str() {
            "reserved" => Ok(ReserveIngestRangeOutcome::Reserved),
            "duplicate" => Ok(ReserveIngestRangeOutcome::Duplicate),
            "conflict" => Ok(ReserveIngestRangeOutcome::Conflict),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn commit_ingest_range(
        &self,
        reservation: IngestRangeReservation,
    ) -> Result<CommitIngestRangeOutcome, MetaStoreError> {
        let response = self
            .client()
            .commit_ingest_range(self.request(ingest_range_reservation_to_proto(reservation)))
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        match response.outcome.as_str() {
            "committed" => Ok(CommitIngestRangeOutcome::Committed),
            "duplicate" => Ok(CommitIngestRangeOutcome::Duplicate),
            "conflict" => Ok(CommitIngestRangeOutcome::Conflict),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn capture_ingest_source_cut(
        &self,
        request: CaptureIngestSourceCutRequest,
    ) -> Result<IngestSourceCutV1, MetaStoreError> {
        let request_json = serde_json::to_vec(&request)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let response = self
            .client()
            .capture_ingest_source_cut(
                self.request(proto::CaptureIngestSourceCutRequest { request_json }),
            )
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        serde_json::from_slice(&response.source_cut_json)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))
    }

    async fn begin_view_bootstrap(
        &self,
        request: BeginViewBootstrapRequest,
    ) -> Result<BeginViewBootstrapOutcome, MetaStoreError> {
        let request_json = serde_json::to_vec(&request)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let response = self
            .client()
            .begin_view_bootstrap(self.request(proto::BeginViewBootstrapRequest { request_json }))
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        match response.outcome.as_str() {
            "created" | "duplicate" => {
                let control = serde_json::from_slice(&response.control_json)
                    .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
                if response.outcome == "created" {
                    Ok(BeginViewBootstrapOutcome::Created(control))
                } else {
                    Ok(BeginViewBootstrapOutcome::Duplicate(control))
                }
            }
            "conflict" => Ok(BeginViewBootstrapOutcome::Conflict),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn read_view_bootstrap(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<ViewBootstrapControlV1>, MetaStoreError> {
        let response = self
            .client()
            .read_view_bootstrap(self.request(proto::ReadViewBootstrapRequest {
                tenant_id: tenant_id.to_string(),
                program_id: program_id.to_string(),
                view_id: view_id.to_string(),
            }))
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        if !response.found {
            return Ok(None);
        }
        serde_json::from_slice(&response.control_json)
            .map(Some)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))
    }

    async fn fix_view_bootstrap_activation_cut(
        &self,
        request: FixViewBootstrapActivationCutRequest,
    ) -> Result<FixViewBootstrapActivationCutOutcome, MetaStoreError> {
        let request_json = serde_json::to_vec(&request)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let response = self
            .client()
            .fix_view_bootstrap_activation_cut(
                self.request(proto::FixViewBootstrapActivationCutRequest { request_json }),
            )
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        match response.outcome.as_str() {
            "fixed" | "duplicate" => {
                let control = serde_json::from_slice(&response.control_json)
                    .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
                if response.outcome == "fixed" {
                    Ok(FixViewBootstrapActivationCutOutcome::Fixed(control))
                } else {
                    Ok(FixViewBootstrapActivationCutOutcome::Duplicate(control))
                }
            }
            "conflict" => Ok(FixViewBootstrapActivationCutOutcome::Conflict),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn promote_view_bootstrap(
        &self,
        request: PromoteViewBootstrapRequest,
    ) -> Result<PromoteViewBootstrapOutcome, MetaStoreError> {
        let request_json = serde_json::to_vec(&request)
            .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
        let response = self
            .client()
            .promote_view_bootstrap(
                self.request(proto::PromoteViewBootstrapRequest { request_json }),
            )
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        match response.outcome.as_str() {
            "promoted" | "duplicate" => {
                let control = serde_json::from_slice(&response.control_json)
                    .map_err(|error| MetaStoreError::Serialization(error.to_string()))?;
                if response.outcome == "promoted" {
                    Ok(PromoteViewBootstrapOutcome::Promoted(control))
                } else {
                    Ok(PromoteViewBootstrapOutcome::Duplicate(control))
                }
            }
            "conflict" => Ok(PromoteViewBootstrapOutcome::Conflict),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn acquire_standing_runtime_owner(
        &self,
        request: AcquireStandingRuntimeOwnerRequest,
    ) -> Result<AcquireStandingRuntimeOwnerOutcome, MetaStoreError> {
        let response = self
            .client()
            .acquire_standing_runtime_owner(self.request(
                proto::AcquireStandingRuntimeOwnerRequest {
                    tenant_id: request.tenant_id,
                    program_id: request.program_id,
                    view_id: request.view_id,
                    owner_id: request.owner_id,
                    ttl_ms: request.ttl_ms,
                },
            ))
            .await
            .map_err(|error| match error.code() {
                tonic::Code::FailedPrecondition => MetaStoreError::UnsupportedCapability(
                    "linearizable_standing_runtime_owner_lease",
                ),
                _ => MetaStoreError::Remote(error.to_string()),
            })?
            .into_inner();
        let claim = response
            .claim
            .ok_or_else(|| MetaStoreError::UnexpectedOutcome("missing owner claim".to_string()))
            .map(standing_runtime_owner_claim_from_proto)?;
        match response.outcome.as_str() {
            "acquired" => Ok(AcquireStandingRuntimeOwnerOutcome::Acquired(claim)),
            "renewed" => Ok(AcquireStandingRuntimeOwnerOutcome::Renewed(claim)),
            "conflict" => Ok(AcquireStandingRuntimeOwnerOutcome::Conflict(claim)),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn read_standing_runtime_owner(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeOwnerClaim>, MetaStoreError> {
        let response = self
            .client()
            .read_standing_runtime_owner(self.request(proto::ReadStandingRuntimeOwnerRequest {
                tenant_id: tenant_id.to_string(),
                program_id: program_id.to_string(),
                view_id: view_id.to_string(),
            }))
            .await
            .map_err(|error| match error.code() {
                tonic::Code::FailedPrecondition => MetaStoreError::UnsupportedCapability(
                    "linearizable_standing_runtime_owner_lease",
                ),
                _ => MetaStoreError::Remote(error.to_string()),
            })?
            .into_inner();
        if !response.found {
            return Ok(None);
        }
        let claim = response
            .claim
            .ok_or_else(|| MetaStoreError::UnexpectedOutcome("missing owner claim".to_string()))?;
        Ok(Some(standing_runtime_owner_claim_from_proto(claim)))
    }

    async fn publish_standing_runtime_checkpoint(
        &self,
        request: PublishStandingRuntimeCheckpointRequest,
    ) -> Result<PublishStandingRuntimeCheckpointOutcome, MetaStoreError> {
        let response = self
            .client()
            .publish_standing_runtime_checkpoint(
                self.request(proto::PublishStandingRuntimeCheckpointRequest {
                    expected_previous: request
                        .expected_previous
                        .map(standing_runtime_checkpoint_pointer_to_proto),
                    candidate: Some(standing_runtime_checkpoint_pointer_to_proto(
                        request.candidate,
                    )),
                    owner: Some(standing_runtime_owner_token_to_proto(request.owner)),
                }),
            )
            .await
            .map_err(|error| match error.code() {
                tonic::Code::FailedPrecondition => MetaStoreError::UnsupportedCapability(
                    "linearizable_standing_runtime_checkpoint_publish",
                ),
                _ => MetaStoreError::Remote(error.to_string()),
            })?
            .into_inner();

        match response.outcome.as_str() {
            "published" => Ok(PublishStandingRuntimeCheckpointOutcome::Published),
            "duplicate" => Ok(PublishStandingRuntimeCheckpointOutcome::Duplicate),
            "conflict" => Ok(PublishStandingRuntimeCheckpointOutcome::Conflict),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn read_standing_runtime_checkpoint(
        &self,
        tenant_id: &str,
        program_id: &str,
        view_id: &str,
    ) -> Result<Option<StandingRuntimeCheckpointPointer>, MetaStoreError> {
        let response = self
            .client()
            .read_standing_runtime_checkpoint(self.request(
                proto::ReadStandingRuntimeCheckpointRequest {
                    tenant_id: tenant_id.to_string(),
                    program_id: program_id.to_string(),
                    view_id: view_id.to_string(),
                },
            ))
            .await
            .map_err(|error| match error.code() {
                tonic::Code::FailedPrecondition => MetaStoreError::UnsupportedCapability(
                    "linearizable_standing_runtime_checkpoint_publish",
                ),
                _ => MetaStoreError::Remote(error.to_string()),
            })?
            .into_inner();
        if !response.found {
            return Ok(None);
        }
        let pointer = response
            .pointer
            .ok_or_else(|| MetaStoreError::UnexpectedOutcome("missing pointer".to_string()))?;
        Ok(Some(standing_runtime_checkpoint_pointer_from_proto(
            pointer,
        )?))
    }

    async fn read_partition_authority_capability(
        &self,
    ) -> Result<PartitionAuthorityCapability, MetaStoreError> {
        let response = self
            .client()
            .read_partition_authority_capability(
                self.request(proto::ReadPartitionAuthorityCapabilityRequest {}),
            )
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        response
            .capability
            .map(partition_authority_capability_from_proto)
            .ok_or_else(|| {
                MetaStoreError::UnexpectedOutcome("missing partition authority capability".into())
            })
    }

    async fn acquire_partition_authority(
        &self,
        request: AcquirePartitionAuthorityRequest,
    ) -> Result<AcquirePartitionAuthorityOutcome, MetaStoreError> {
        request.validate()?;
        let response = self
            .client()
            .acquire_partition_authority(
                self.request(proto::AcquirePartitionAuthorityRequest {
                    key: Some(partition_authority_key_to_proto(request.key)),
                    owner_id: request.owner_id,
                    current_token: request
                        .current_token
                        .map(partition_authority_token_to_proto),
                    ttl_ms: request.ttl_ms,
                }),
            )
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        let token = response
            .token
            .ok_or_else(|| {
                MetaStoreError::UnexpectedOutcome("missing partition authority token".into())
            })
            .and_then(partition_authority_token_from_proto)?;
        match response.outcome.as_str() {
            "acquired" => Ok(AcquirePartitionAuthorityOutcome::Acquired(token)),
            "renewed" => Ok(AcquirePartitionAuthorityOutcome::Renewed(token)),
            "conflict" => Ok(AcquirePartitionAuthorityOutcome::Conflict(token)),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn read_partition_authority(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Option<PartitionAuthorityToken>, MetaStoreError> {
        key.validate()?;
        let response = self
            .client()
            .read_partition_authority(self.request(proto::ReadPartitionAuthorityRequest {
                key: Some(partition_authority_key_to_proto(key.clone())),
            }))
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        match (response.found, response.token) {
            (true, Some(token)) => partition_authority_token_from_proto(token).map(Some),
            (false, None) => Ok(None),
            (true, None) => Err(MetaStoreError::UnexpectedOutcome(
                "missing partition authority token".into(),
            )),
            (false, Some(_)) => Err(MetaStoreError::UnexpectedOutcome(
                "partition authority response has a token without found".into(),
            )),
        }
    }

    async fn publish_partition_checkpoint_pointer(
        &self,
        request: PublishPartitionCheckpointPointerRequest,
    ) -> Result<PublishPartitionCheckpointPointerOutcome, MetaStoreError> {
        request.validate()?;
        let response = self
            .client()
            .publish_partition_checkpoint_pointer(
                self.request(proto::PublishPartitionCheckpointPointerRequest {
                    expected_previous: request
                        .expected_previous
                        .map(partition_checkpoint_pointer_to_proto),
                    candidate: Some(partition_checkpoint_pointer_to_proto(request.candidate)),
                    authority: Some(partition_authority_token_to_proto(request.authority)),
                }),
            )
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        match response.outcome.as_str() {
            "published" => Ok(PublishPartitionCheckpointPointerOutcome::Published),
            "duplicate" => Ok(PublishPartitionCheckpointPointerOutcome::Duplicate),
            "conflict" => Ok(PublishPartitionCheckpointPointerOutcome::Conflict),
            other => Err(MetaStoreError::UnexpectedOutcome(other.to_string())),
        }
    }

    async fn read_partition_checkpoint_pointer(
        &self,
        key: &PartitionAuthorityKey,
    ) -> Result<Option<PartitionCheckpointPointer>, MetaStoreError> {
        key.validate()?;
        let response = self
            .client()
            .read_partition_checkpoint_pointer(self.request(
                proto::ReadPartitionCheckpointPointerRequest {
                    key: Some(partition_authority_key_to_proto(key.clone())),
                },
            ))
            .await
            .map_err(partition_authority_remote_error)?
            .into_inner();
        match (response.found, response.pointer) {
            (true, Some(pointer)) => partition_checkpoint_pointer_from_proto(pointer).map(Some),
            (false, None) => Ok(None),
            (true, None) => Err(MetaStoreError::UnexpectedOutcome(
                "missing partition checkpoint pointer".into(),
            )),
            (false, Some(_)) => Err(MetaStoreError::UnexpectedOutcome(
                "partition checkpoint response has a pointer without found".into(),
            )),
        }
    }

    async fn read_view_dependency_graph_revision(
        &self,
        tenant_id: &str,
    ) -> Result<u64, MetaStoreError> {
        require_non_empty("tenant_id", tenant_id)?;
        let response = self
            .client()
            .read_view_dependency_graph_revision(self.request(
                proto::ReadViewDependencyGraphRevisionRequest {
                    tenant_id: tenant_id.to_string(),
                },
            ))
            .await
            .map_err(|error| MetaStoreError::Remote(error.to_string()))?
            .into_inner();
        Ok(response.revision)
    }
}

#[cfg(all(test, feature = "hiqlite-backend"))]
mod hiqlite_capability_tests {
    use super::*;

    #[test]
    fn hiqlite_capability_is_production_safe_with_raft_replicated_authority_time() {
        let capability = hiqlite_standing_runtime_fencing_capability(true);

        assert_eq!(capability.backend_name, "hiqlite");
        assert!(capability.linearizable_owner_lease);
        assert!(capability.durable_monotonic_owner_epoch);
        assert!(capability.authoritative_backend_time);
        assert_eq!(
            capability.backend_time_source_kind,
            STANDING_RUNTIME_BACKEND_TIME_SOURCE_RAFT_REPLICATED
        );
        assert_eq!(capability.backend_time_blocked_reason, "");
        assert_eq!(
            capability.lease_authority_kind,
            STANDING_RUNTIME_LEASE_AUTHORITY_KIND_RAFT_REPLICATED_TIME
        );
        assert_eq!(
            capability.lease_expiry_semantics,
            STANDING_RUNTIME_LEASE_EXPIRY_SEMANTICS_BACKEND_WALL_CLOCK_TTL
        );
        assert!(capability.owner_validated_checkpoint_publish);
        assert!(capability.publish_checks_owner_and_latest_atomically);
        assert!(capability.publish_rejects_expired_owner);
        assert!(capability.latest_read_linearizable);
        assert!(capability.publish_rejects_scope_mismatch);
        assert!(capability.control_plane_auth_enforced);
        assert!(capability.multi_writer_fencing_safe);
        assert!(capability.bounded_wall_clock_failover);
        assert_eq!(
            capability.failover_time_bound_ms,
            MAX_STANDING_RUNTIME_OWNER_TTL_MS
        );
        assert!(capability.production_bounded_failover_safe);
        assert!(capability.production_multi_writer_safe);
    }

    #[test]
    fn hiqlite_authority_time_transaction_satisfies_backend_time_gate() {
        let source = include_str!("lib.rs");
        let capability = hiqlite_standing_runtime_fencing_capability(true);

        assert!(source.contains(".txn_with_raft_serialized_timestamp(["));
        assert!(source.contains("hiqlite::Param::raft_serialized_unix_ms()"));
        assert!(source.contains("owner.expires_at_unix_ms > $"));
        assert!(capability.authoritative_backend_time);
        assert_eq!(
            capability.backend_time_source_kind,
            STANDING_RUNTIME_BACKEND_TIME_SOURCE_RAFT_REPLICATED
        );
        assert!(capability.multi_writer_fencing_safe);
        assert!(capability.bounded_wall_clock_failover);
        assert!(capability.production_bounded_failover_safe);
        assert!(capability.production_multi_writer_safe);
    }

    #[test]
    fn hiqlite_write_path_does_not_use_nondeterministic_sql_time_functions() {
        fn contains_sql_function_call(source: &str, function_call: &str) -> bool {
            source.match_indices(function_call).any(|(index, _)| {
                source[..index].chars().next_back().is_none_or(|previous| {
                    previous.is_ascii_whitespace() || "=(,+-*/".contains(previous)
                })
            })
        }

        let source = include_str!("lib.rs").to_ascii_lowercase();
        for forbidden in [
            concat!("now", "("),
            concat!("strftime", "("),
            concat!("unixepoch", "("),
            concat!("datetime", "("),
            concat!("time", "("),
            concat!("date", "("),
            concat!("julianday", "("),
            concat!("random", "("),
            concat!("randomblob", "("),
        ] {
            assert!(
                !contains_sql_function_call(&source, forbidden),
                "Hiqlite Raft write path must not use nondeterministic SQL function `{forbidden}`"
            );
        }
    }

    #[test]
    fn hiqlite_standing_runtime_reads_use_linearizable_consistent_queries() {
        let source = include_str!("lib.rs");
        for row_type in [
            "StandingRuntimeOwnerClaimRow",
            "StandingRuntimeCheckpointPointerRow",
        ] {
            let query = ["query_consistent_map::<", row_type].concat();
            assert!(
                source.contains(&query),
                "Hiqlite standing runtime fencing reads must use linearizable consistent queries: {query}"
            );
        }
    }

    #[test]
    fn hiqlite_schema_loss_paths_retry_after_idempotent_schema_repair() {
        let source = include_str!("lib.rs");
        let hiqlite_impl = source
            .split("impl MetaStore for HiqliteMetaStore")
            .nth(1)
            .expect("Hiqlite MetaStore impl should be present");

        assert!(
            source.contains("async fn with_schema_repair")
                && source.contains("hiqlite_meta_error_is_missing_table")
                && source.contains("self.initialize_schema().await?"),
            "HiqliteMetaStore must repair schema after an empty no-PVC DB is re-created"
        );
        for required_call in [
            "store_relation_catalog",
            "read_relation_catalog",
            "reserve_ingest_range",
            "acquire_standing_runtime_owner",
            "read_standing_runtime_owner",
            "publish_standing_runtime_checkpoint",
            "read_standing_runtime_checkpoint",
        ] {
            let method = hiqlite_impl
                .split(&format!("async fn {required_call}"))
                .nth(1)
                .and_then(|tail| tail.split("async fn ").next())
                .expect("required Hiqlite method should exist");
            assert!(
                method.contains(".with_schema_repair("),
                "Hiqlite method {required_call} must retry once after missing-table schema repair"
            );
        }
    }

    #[test]
    fn hiqlite_owner_read_filters_expiry_with_authority_time_not_process_time() {
        let source = include_str!("lib.rs");
        let hiqlite_impl = source
            .split("impl MetaStore for HiqliteMetaStore")
            .nth(1)
            .and_then(|tail| {
                tail.split("async fn publish_standing_runtime_checkpoint")
                    .next()
            })
            .expect("Hiqlite MetaStore impl should include owner read before publish");
        let read_impl = hiqlite_impl
            .split("async fn read_standing_runtime_owner")
            .nth(1)
            .expect("Hiqlite owner read impl should be present in source");

        assert!(
            read_impl.contains(".txn_with_raft_serialized_timestamp([")
                && read_impl.contains("txn.timestamp.unix_ms")
                && read_impl.contains(".filter(|claim| claim.expires_at_unix_ms > now)"),
            "Hiqlite owner read must evaluate lease expiry against a Raft-serialized authority timestamp"
        );
        assert!(
            !read_impl.contains("unix_time_ms()?"),
            "Hiqlite owner read must not filter lease expiry with the Velorix process clock"
        );
    }

    #[test]
    fn publish_request_validation_rejects_owner_scope_mismatch_before_sql() {
        let request = PublishStandingRuntimeCheckpointRequest {
            expected_previous: None,
            candidate: StandingRuntimeCheckpointPointer {
                tenant_id: "default".to_string(),
                program_id: "program".to_string(),
                view_id: "view".to_string(),
                checkpoint_key:
                    "v1/standing-runtime-checkpoints/default/program/view/epochs/00000000000000000001/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.checkpoint.json"
                        .to_string(),
                logical_epoch: 1,
                content_hash:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                manifest_hash:
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_string(),
                output_manifest_refs: Vec::new(),
                bootstrap_generation: 0,
                plan_hash: String::new(),
                coverage_hash: String::new(),
                input_coverage: None,
                previous_checkpoint_key: String::new(),
                previous_manifest_hash: String::new(),
            },
            owner: StandingRuntimeOwnerToken {
                tenant_id: "other".to_string(),
                program_id: "program".to_string(),
                view_id: "view".to_string(),
                owner_id: "owner-a".to_string(),
                owner_epoch: 1,
            },
        };

        assert!(matches!(
            request.validate(),
            Err(MetaStoreError::StandingRuntimeCheckpointScopeMismatch)
        ));
    }

    #[test]
    fn hiqlite_standing_runtime_sql_predicates_use_authority_time_param_not_process_time() {
        let source = include_str!("lib.rs");
        let hiqlite_impl = source
            .split("impl MetaStore for HiqliteMetaStore")
            .nth(1)
            .and_then(|tail| tail.split("struct CatalogJsonRow").next())
            .expect("Hiqlite MetaStore impl should be present in source");

        let acquire_impl = hiqlite_impl
            .split("async fn acquire_standing_runtime_owner")
            .nth(1)
            .and_then(|tail| tail.split("async fn read_standing_runtime_owner").next())
            .expect("Hiqlite acquire owner impl should be present in source");
        assert!(
            acquire_impl.contains(".txn_with_raft_serialized_timestamp([")
                && acquire_impl.contains("hiqlite::Param::raft_serialized_unix_ms()")
                && acquire_impl.contains("expires_at_unix_ms > $5"),
            "Hiqlite owner acquire must compare lease expiry to the Raft-serialized authority Unix timestamp"
        );
        assert!(
            !acquire_impl.contains("unix_time_ms()?"),
            "Hiqlite owner acquire must not derive safety predicates from the Velorix process clock"
        );

        let publish_impl = hiqlite_impl
            .split("async fn publish_standing_runtime_checkpoint")
            .nth(1)
            .and_then(|tail| {
                tail.split("async fn read_standing_runtime_checkpoint")
                    .next()
            })
            .expect("Hiqlite checkpoint publish impl should be present in source");
        assert!(
            publish_impl
                .matches(".txn_with_raft_serialized_timestamp([")
                .count()
                >= 2
                && publish_impl
                    .matches("hiqlite::Param::raft_serialized_unix_ms()")
                    .count()
                    >= 2
                && publish_impl.contains("owner.expires_at_unix_ms > $6")
                && publish_impl.matches("hiqlite::Param::StmtOutputIndexed(0, 0)").count() >= 2,
            "Hiqlite checkpoint publish update and insert paths must both compare owner expiry to the Raft-serialized authority Unix timestamp"
        );
        assert!(
            !publish_impl.contains("unix_time_ms()?"),
            "Hiqlite checkpoint publish must not derive safety predicates from the Velorix process clock"
        );
    }

    #[test]
    fn hiqlite_owner_read_sql_qualifies_joined_owner_columns() {
        let source = include_str!("lib.rs");
        let owner_read_impl = source
            .split("async fn read_standing_runtime_owner_record")
            .nth(1)
            .and_then(|tail| tail.split("async fn capabilities").next())
            .expect("Hiqlite owner read impl should be present in source");

        for column in ["tenant_id", "program_id", "view_id"] {
            let qualified_select = ["owner.", column, ","].concat();
            assert!(
                owner_read_impl.contains(&qualified_select),
                "Hiqlite owner read SELECT must qualify {column} because the authority clock join has the same column"
            );
        }
    }

    #[test]
    fn hiqlite_publish_does_not_mutate_logical_authority_clock() {
        let source = include_str!("lib.rs");
        let hiqlite_impl = source
            .split("impl MetaStore for HiqliteMetaStore")
            .nth(1)
            .and_then(|tail| tail.split("struct CatalogJsonRow").next())
            .expect("Hiqlite MetaStore impl should be present in source");
        let publish_impl = hiqlite_impl
            .split("async fn publish_standing_runtime_checkpoint")
            .nth(1)
            .and_then(|tail| {
                tail.split("async fn read_standing_runtime_checkpoint")
                    .next()
            })
            .expect("Hiqlite checkpoint publish impl should be present in source");
        assert!(
            !publish_impl.contains("UPDATE velorix_standing_runtime_authority_clocks"),
            "Hiqlite publish must use the Raft-serialized authority time parameter, not the old logical clock table"
        );
    }

    #[test]
    fn hiqlite_publish_validates_owner_inside_checkpoint_mutation() {
        let source = include_str!("lib.rs");
        let hiqlite_impl = source
            .split("impl MetaStore for HiqliteMetaStore")
            .nth(1)
            .and_then(|tail| {
                tail.split("async fn read_standing_runtime_checkpoint")
                    .next()
            })
            .expect("Hiqlite MetaStore impl should include publish before checkpoint read");
        let publish_impl = hiqlite_impl
            .split("async fn publish_standing_runtime_checkpoint")
            .nth(1)
            .expect("Hiqlite checkpoint publish impl should be present in source");
        let owner_validation = publish_impl
            .find("SELECT 1 AS authorized")
            .expect("publish must validate current owner token in SQL");
        let checkpoint_mutation = publish_impl
            .find("velorix_standing_runtime_checkpoints")
            .expect("publish should mutate checkpoint rows");

        assert!(
            owner_validation < checkpoint_mutation
                && publish_impl.contains("hiqlite::Param::StmtOutputIndexed(0, 0)"),
            "Hiqlite publish must validate owner token and authority-time expiry inside the same transaction and gate the checkpoint mutation on that validation"
        );
    }

    #[test]
    fn hiqlite_acquire_advances_authority_clock_before_conflict_decision() {
        let source = include_str!("lib.rs");
        let hiqlite_impl = source
            .split("impl MetaStore for HiqliteMetaStore")
            .nth(1)
            .and_then(|tail| tail.split("async fn read_standing_runtime_owner").next())
            .expect("Hiqlite MetaStore impl should include acquire before owner read");
        let acquire_impl = hiqlite_impl
            .split("async fn acquire_standing_runtime_owner")
            .nth(1)
            .expect("Hiqlite owner acquire impl should be present in source");
        let txn_start = acquire_impl
            .find(".txn_with_raft_serialized_timestamp([")
            .expect("Hiqlite owner acquire should use a Raft-serialized timestamp transaction");
        let conflict_return = acquire_impl.find("AcquireStandingRuntimeOwnerOutcome::Conflict");

        assert!(
            conflict_return.is_none_or(|index| index > txn_start),
            "Hiqlite owner acquire must run the Raft-serialized timestamp transaction before deciding a different owner is still active"
        );
    }

    #[test]
    fn hiqlite_acquire_binds_authority_unix_time_before_ttl() {
        let source = include_str!("lib.rs");
        let hiqlite_impl = source
            .split("impl MetaStore for HiqliteMetaStore")
            .nth(1)
            .and_then(|tail| tail.split("async fn read_standing_runtime_owner").next())
            .expect("Hiqlite MetaStore impl should include acquire before owner read");
        let acquire_impl = hiqlite_impl
            .split("async fn acquire_standing_runtime_owner")
            .nth(1)
            .expect("Hiqlite owner acquire impl should be present in source");

        assert!(
            acquire_impl.contains(
                "1,\n                        $5 + $6,\n                        0"
            ),
            "Hiqlite binds SQL parameters by occurrence order; owner acquire must bind raft_serialized_unix_ms before ttl_ms"
        );
        let expires_param = acquire_impl
            .find("hiqlite::Param::raft_serialized_unix_ms()")
            .expect("acquire should bind raft_serialized_unix_ms");
        let ttl_param = acquire_impl
            .find("hiqlite::Param::from(ttl_ms)")
            .expect("acquire should bind ttl_ms");
        assert!(
            expires_param < ttl_param,
            "Hiqlite owner acquire params must provide raft_serialized_unix_ms before ttl_ms"
        );
    }

    #[test]
    fn hiqlite_publish_update_binds_parameters_in_first_appearance_order() {
        let source = include_str!("lib.rs");
        let hiqlite_impl = source
            .split("impl MetaStore for HiqliteMetaStore")
            .nth(1)
            .and_then(|tail| {
                tail.split("async fn read_standing_runtime_checkpoint")
                    .next()
            })
            .expect("Hiqlite MetaStore impl should include publish before checkpoint read");
        let publish_impl = hiqlite_impl
            .split("async fn publish_standing_runtime_checkpoint")
            .nth(1)
            .expect("Hiqlite checkpoint publish impl should be present in source");
        let update_impl = publish_impl
            .split("UPDATE velorix_standing_runtime_checkpoints")
            .nth(1)
            .and_then(|tail| tail.split("INSERT OR IGNORE INTO").next())
            .expect("Hiqlite checkpoint publish update statement should be present");

        assert!(
            update_impl.contains(
                "SET checkpoint_key = $1,\n                                logical_epoch = $2,\n                                content_hash = $3"
            ),
            "Hiqlite binds SQL parameters by first appearance; publish update SET params must be $1..$3"
        );
        let candidate_key = update_impl
            .find("hiqlite::Param::from(request.candidate.checkpoint_key.clone())")
            .expect("publish update should bind candidate checkpoint_key");
        let candidate_epoch_param = update_impl
            .find("hiqlite::Param::from(candidate_epoch)")
            .expect("publish update should bind candidate logical_epoch");
        let candidate_hash = update_impl
            .find("hiqlite::Param::from(request.candidate.content_hash.clone())")
            .expect("publish update should bind candidate content_hash");
        let candidate_tenant = update_impl
            .find("hiqlite::Param::from(request.candidate.tenant_id.clone())")
            .expect("publish update should bind candidate tenant_id");
        assert!(
            candidate_key < candidate_epoch_param
                && candidate_epoch_param < candidate_hash
                && candidate_hash < candidate_tenant,
            "publish update params must bind candidate values before scope values to match SQL first appearance"
        );
    }

    #[test]
    fn hiqlite_publish_sql_enforces_owner_scope_equality_inside_mutation() {
        let source = include_str!("lib.rs");
        let hiqlite_impl = source
            .split("impl MetaStore for HiqliteMetaStore")
            .nth(1)
            .and_then(|tail| {
                tail.split("async fn read_standing_runtime_checkpoint")
                    .next()
            })
            .expect("Hiqlite MetaStore impl should include publish before checkpoint read");
        let publish_impl = hiqlite_impl
            .split("async fn publish_standing_runtime_checkpoint")
            .nth(1)
            .expect("Hiqlite checkpoint publish impl should be present in source");

        assert!(
            publish_impl.matches("AND $7 = $1").count() >= 2
                && publish_impl.matches("AND $8 = $2").count() >= 2
                && publish_impl.matches("AND $9 = $3").count() >= 2
                && publish_impl.matches("hiqlite::Param::StmtOutputIndexed(0, 0)").count() >= 2,
            "Hiqlite checkpoint publish must enforce owner scope equals candidate scope in the authorization statement and gate both checkpoint mutations on that statement"
        );
        for owner_scope_param in [
            "hiqlite::Param::from(request.owner.tenant_id.clone())",
            "hiqlite::Param::from(request.owner.program_id.clone())",
            "hiqlite::Param::from(request.owner.view_id.clone())",
        ] {
            assert!(
                publish_impl.contains(owner_scope_param),
                "Hiqlite checkpoint publish must bind owner scope values for SQL-level scope equality predicates: {owner_scope_param}"
            );
        }
    }

    #[test]
    fn hiqlite_required_backend_time_must_not_be_derived_from_metrics_or_dlock() {
        let source = include_str!("lib.rs");
        let hiqlite_impl = source
            .split("impl MetaStore for HiqliteMetaStore")
            .nth(1)
            .and_then(|tail| tail.split("struct CatalogJsonRow").next())
            .expect("Hiqlite MetaStore impl should be present in source");

        for forbidden in ["metrics_db", "RaftMetrics", ".metrics()", ".lock("] {
            assert!(
                !hiqlite_impl.contains(forbidden),
                "Hiqlite standing-runtime backend-time safety must not derive lease expiry from {forbidden}"
            );
        }
        let capability = hiqlite_standing_runtime_fencing_capability(true);
        assert_eq!(
            capability.backend_time_source_kind,
            STANDING_RUNTIME_BACKEND_TIME_SOURCE_RAFT_REPLICATED,
            "Hiqlite capability must advertise raft_replicated_authority_time only when the write path uses authority-time transactions"
        );
    }
}
